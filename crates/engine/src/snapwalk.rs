//! Plan 32 §0.2: snapshot chunk occurrences per chain, by tree diff.
//!
//! A snapshot is a retained whole-filesystem root plus the directory it
//! froze ([`SnapshotRoot`]). What it keeps alive is the multiset of
//! chunk objects — data chunks and spilled chunk lists — of every file
//! reachable under that directory in that root: its **occurrences**.
//! Chunk GC used to compute that set by walking every snapshot's whole
//! subtree, every round. Automatic policies take hundreds of snapshots
//! of one directory, nearly all of them alike, so that walk was
//! O(Σ subtree size) for an answer that is O(one subtree + what
//! changed).
//!
//! The snapshots of one directory form a **chain** (keyed by the
//! directory's `ino`, ordered by `(seq, created_unix_ms)`).
//! [`ChainWalk::first`] walks the oldest one in full;
//! [`ChainWalk::step`] turns each consecutive pair into occurrence
//! [`Deltas`] from `Tree::diff`, whose cost tracks the difference
//! between the two roots and not their size. GC protects
//! `first ∪ ⋃ additions(step)`; the space-accounting index (plan 32
//! §6.2) applies the deltas to per-chunk occurrence counts, which is why
//! a delta carries its plaintext size and why the counts are exact
//! rather than "at least".
//!
//! ## Exact, file by file
//!
//! A file `f` contributes `links_R(f) × chunks_R(f)` to root `R`, where
//! `links_R(f)` is the number of its names whose parent directory is in
//! the subtree (hardlinks weigh once per in-subtree name, exactly as the
//! old readdir walk visited them). `step` finds every file whose term
//! *can* differ and recomputes that term in both roots; everything else
//! cancels. A term can differ only if:
//!
//! - `chunks(f)` changed — its `0x01` record changed (write, truncate,
//!   create, delete, inode reuse). A record change that leaves the
//!   manifest bytes alone (chmod, utimes) recomputes to a zero delta, and
//!   an atime-only change never reaches the tree at all (§P6 keeps atime
//!   node-local), so it is not even a diff key;
//! - one of its names changed — a `0x04 ino | parent | name` key. `0x02`
//!   is written in the same commit as its mirror `0x04` and is not
//!   consulted: its value also changes on every size change (the attr
//!   copy), and reading it would buy nothing `0x04` does not say;
//! - a directory above one of its names **crossed the subtree boundary**.
//!   A directory has one name, so it crossed exactly when its own `0x04`
//!   changed and its membership differs between the roots (take the
//!   lowest directory on the file's ancestor chain whose `0x04` changed:
//!   everything below it is identical in both roots, so membership can
//!   only differ if that directory's does). The crossing directory is
//!   walked in the root where it is a member, and every file found is
//!   recomputed like the others. That is the one cost proportional to a
//!   moved subtree rather than to the diff; it is rare, and a directory
//!   created or deleted wholesale costs the same either way.
//!
//! Recomputing a term (rather than mapping each key to a `±` by kind)
//! is what makes renames, overwrites, hardlinks leaving and re-entering,
//! and reverts exact without a case each: both roots are read
//! independently, so nothing has to be inferred from the shape of the
//! change.
//!
//! **Membership** ([`in_subtree`]) is the `0x04` ancestor walk: `x` is in
//! the subtree of `dir` at `R` when `x == dir` or one of `x`'s parents
//! is. The answers are cached per `(root, dir)`; roots are immutable, so
//! the cache never needs invalidating.
//!
//! **Never less than the full walk.** Anything `step` cannot vouch for —
//! the chain's directory missing from a root, a directory with more than
//! one name, an ancestor chain that does not terminate — falls back to
//! two full walks and their difference, which is exact by definition,
//! and says so at `debug` ([`Deltas::fell_back`]).
//!
//! ## Spilled chunk lists
//!
//! A spilled list is content-addressed and therefore immutable: it is
//! fetched once per [`ChainWalk`] (one GC pass), and kept across passes
//! in a small process-wide LRU bounded by encoded bytes. The node-local
//! chunk cache is deliberately not involved — it is a data cache with
//! its own eviction story, and a GC pass must not churn it.

use crate::mtree_read::Resolver;
use crate::snapshot::{SnapshotRoot, TreeAccess};
use anyhow::{bail, Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest, SparseChunks};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_mtree::keys::{self, Key};
use constellation_mtree::record::{DentryRecord, InodeRecord, Kind};
use constellation_mtree::{NodeHash, NodeStore, Tree};
use constellation_store_s3::{ChunkStore, NodeCache};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// How deep an ancestor walk may go before the tree is presumed
/// malformed (a `0x04` cycle) and the step falls back to full walks.
const MAX_DEPTH: usize = 4096;

/// The process-wide spilled-list cache's budget, in encoded bytes.
const SPILL_CACHE_BYTES: u64 = 256 << 20;

// ------------------------------------------------------------- results

/// One chunk object's occurrences in a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Occurrence {
    /// How many times the subtree references it: once per in-subtree
    /// link of every file naming it, once per chunk index naming it.
    pub count: u64,
    /// Plaintext bytes per occurrence: `min(chunk_size, file_len −
    /// offset)` for a data chunk, the encoded length for a spilled list.
    /// Content-addressed, so the same for every occurrence.
    pub size_bytes: u64,
}

/// The multiset of chunk objects a snapshot keeps alive.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Occurrences {
    entries: HashMap<ChunkHash, Occurrence>,
}

impl Occurrences {
    fn add(&mut self, hash: ChunkHash, size_bytes: u64, count: u64) {
        let entry = self.entries.entry(hash).or_insert(Occurrence {
            count: 0,
            size_bytes,
        });
        entry.count += count;
    }

    /// How many times `hash` occurs (0 when it does not).
    pub fn count(&self, hash: &ChunkHash) -> u64 {
        self.entries.get(hash).map_or(0, |entry| entry.count)
    }

    pub fn get(&self, hash: &ChunkHash) -> Option<&Occurrence> {
        self.entries.get(hash)
    }

    /// Distinct chunk objects.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ChunkHash, &Occurrence)> {
        self.entries.iter()
    }

    pub fn hashes(&self) -> impl Iterator<Item = &ChunkHash> {
        self.entries.keys()
    }

    /// Advance to the next snapshot of the chain. Refuses a delta that
    /// would take a count below zero: deltas are exact, so that is a
    /// bug (or deltas applied to the wrong base), never a rounding.
    pub fn apply(&mut self, deltas: &Deltas) -> Result<()> {
        for delta in deltas.iter() {
            let have = self.count(&delta.hash) as i64;
            let now = have + delta.delta;
            if now < 0 {
                bail!(
                    "chunk {} would occur {now} times after a delta of {}",
                    delta.hash.to_hex(),
                    delta.delta
                );
            }
            if now == 0 {
                self.entries.remove(&delta.hash);
            } else {
                self.entries.insert(
                    delta.hash,
                    Occurrence {
                        count: now as u64,
                        size_bytes: delta.size_bytes,
                    },
                );
            }
        }
        Ok(())
    }

    /// `self − base`, as deltas: what [`ChainWalk::step`] computes from
    /// a diff, computed from two full walks instead.
    pub fn minus(&self, base: &Occurrences) -> Deltas {
        let mut deltas = Deltas::default();
        for (hash, occurrence) in &self.entries {
            deltas.add(*hash, occurrence.size_bytes, occurrence.count as i64);
        }
        for (hash, occurrence) in &base.entries {
            deltas.add(*hash, occurrence.size_bytes, -(occurrence.count as i64));
        }
        deltas.prune();
        deltas
    }
}

/// One chunk object's change in occurrence count between two snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delta {
    pub hash: ChunkHash,
    /// Positive: occurrences gained; negative: lost. Never zero.
    pub delta: i64,
    /// Plaintext bytes per occurrence (see [`Occurrence::size_bytes`]).
    pub size_bytes: u64,
}

/// Occurrence changes from one snapshot of a chain to the next, in hash
/// order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Deltas {
    entries: BTreeMap<ChunkHash, (i64, u64)>,
    /// Set when the step could not be computed from the diff and was
    /// computed from two full walks instead. The deltas are just as
    /// exact; only the cost differs.
    pub fell_back: bool,
}

impl Deltas {
    fn add(&mut self, hash: ChunkHash, size_bytes: u64, delta: i64) {
        let entry = self.entries.entry(hash).or_insert((0, size_bytes));
        entry.0 += delta;
    }

    fn prune(&mut self) {
        self.entries.retain(|_, (delta, _)| *delta != 0);
    }

    pub fn iter(&self) -> impl Iterator<Item = Delta> + '_ {
        self.entries
            .iter()
            .map(|(hash, (delta, size_bytes))| Delta {
                hash: *hash,
                delta: *delta,
                size_bytes: *size_bytes,
            })
    }

    /// The chunk objects that gained occurrences: what GC adds to a
    /// chain's protected set.
    pub fn additions(&self) -> impl Iterator<Item = &ChunkHash> {
        self.entries
            .iter()
            .filter(|(_, (delta, _))| *delta > 0)
            .map(|(hash, _)| hash)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ----------------------------------------------------------- membership

/// [`in_subtree`]'s answers, per `(root, dir)`. Roots are immutable, so
/// an answer never goes stale; drop the value to bound it.
#[derive(Debug, Default)]
pub struct Membership {
    answers: HashMap<(NodeHash, Ino), HashMap<Ino, bool>>,
}

/// Why a step could not be computed from the diff.
#[derive(Debug)]
struct Unclear(String);

impl std::fmt::Display for Unclear {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unclear {}

/// The `(parent, name)` links pointing at `ino` in `root`, from `0x04`.
fn parents_of<S: NodeStore>(tree: &Tree<S>, root: &NodeHash, ino: Ino) -> Result<Vec<Ino>> {
    let range = keys::names_of(ino);
    let mut parents = Vec::new();
    for (key, _) in tree.range(root, range.start(), range.prefix(), usize::MAX)? {
        let Key::RDentry { parent_ino, .. } = Key::parse(&key)? else {
            bail!("a non-reverse-dentry key in inode {ino}'s name range");
        };
        parents.push(parent_ino);
    }
    Ok(parents)
}

/// Whether directory `ino` is `dir` or below it in `root`, iteratively up
/// its single-parent chain, caching every directory on the way.
fn dir_in_subtree<S: NodeStore>(
    tree: &Tree<S>,
    root: &NodeHash,
    ino: Ino,
    dir: Ino,
    answers: &mut HashMap<Ino, bool>,
) -> Result<bool> {
    let mut path = Vec::new();
    let mut cursor = ino;
    let answer = loop {
        if cursor == dir {
            break true;
        }
        if let Some(&known) = answers.get(&cursor) {
            break known;
        }
        if path.len() >= MAX_DEPTH {
            return Err(Unclear(format!("inode {ino}'s ancestor chain does not terminate")).into());
        }
        path.push(cursor);
        match parents_of(tree, root, cursor)?.as_slice() {
            [] => break false,
            [parent] => cursor = *parent,
            _ => {
                return Err(Unclear(format!("directory {cursor} has more than one name")).into());
            }
        }
    };
    for step in path {
        answers.insert(step, answer);
    }
    Ok(answer)
}

/// Whether `ino` (any kind) is in the subtree of directory `dir` at
/// `root`: `ino == dir`, or one of its `0x04` parents is a directory in
/// the subtree. A file outside the subtree may still have *a* link in
/// it; this is true then. Plan 32 §3.4's skip-empty check uses it to ask
/// whether a diff key maps into a policy root.
pub fn in_subtree<S: NodeStore>(
    tree: &Tree<S>,
    root: &NodeHash,
    ino: Ino,
    dir: Ino,
    cache: &mut Membership,
) -> Result<bool> {
    let answers = cache.answers.entry((*root, dir)).or_default();
    if ino == dir {
        return Ok(true);
    }
    if let Some(&known) = answers.get(&ino) {
        return Ok(known);
    }
    let mut answer = false;
    for parent in parents_of(tree, root, ino)? {
        if dir_in_subtree(tree, root, parent, dir, answers)? {
            answer = true;
            break;
        }
    }
    answers.insert(ino, answer);
    Ok(answer)
}

/// How many of `ino`'s names have a parent in the subtree.
fn links_in_subtree<S: NodeStore>(
    tree: &Tree<S>,
    root: &NodeHash,
    ino: Ino,
    dir: Ino,
    answers: &mut HashMap<Ino, bool>,
) -> Result<u64> {
    let mut links = 0;
    for parent in parents_of(tree, root, ino)? {
        if dir_in_subtree(tree, root, parent, dir, answers)? {
            links += 1;
        }
    }
    Ok(links)
}

// -------------------------------------------------------- spilled lists

/// A decoded spilled chunk list and its encoded length.
struct SpilledList {
    chunks: SparseChunks,
    encoded_len: u64,
}

#[derive(Default)]
struct SpillLru {
    entries: HashMap<ChunkHash, (Arc<SpilledList>, u64)>,
    order: BTreeMap<u64, ChunkHash>,
    bytes: u64,
    tick: u64,
}

impl SpillLru {
    fn get(&mut self, hash: &ChunkHash) -> Option<Arc<SpilledList>> {
        let (list, tick) = self.entries.get_mut(hash)?;
        self.order.remove(tick);
        self.tick += 1;
        *tick = self.tick;
        self.order.insert(self.tick, *hash);
        Some(list.clone())
    }

    fn insert(&mut self, hash: ChunkHash, list: Arc<SpilledList>) {
        if self.entries.contains_key(&hash) || list.encoded_len > SPILL_CACHE_BYTES {
            return;
        }
        self.tick += 1;
        self.bytes += list.encoded_len;
        self.order.insert(self.tick, hash);
        self.entries.insert(hash, (list, self.tick));
        while self.bytes > SPILL_CACHE_BYTES {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some((list, _)) = self.entries.remove(&oldest) {
                self.bytes -= list.encoded_len;
            }
        }
    }
}

fn process_spills() -> &'static Mutex<SpillLru> {
    static SPILLS: OnceLock<Mutex<SpillLru>> = OnceLock::new();
    SPILLS.get_or_init(Default::default)
}

// -------------------------------------------------------------- walker

/// Snapshot occurrences of one bucket's chains. One per GC pass: it
/// owns the pass's spilled-list map and membership answers.
#[derive(Clone)]
pub struct ChainWalk {
    inner: Arc<Inner>,
}

struct Inner {
    tree: TreeAccess,
    chunks: Arc<ChunkStore>,
    spills: Mutex<HashMap<ChunkHash, Arc<SpilledList>>>,
    membership: Mutex<Membership>,
    /// Spilled lists this walker actually fetched from the store.
    fetches: AtomicU64,
}

/// One root, read synchronously on a blocking thread.
struct Side<'a> {
    tree: &'a Tree<Arc<NodeCache>>,
    root: NodeHash,
}

impl ChainWalk {
    pub fn new(tree: TreeAccess, chunks: Arc<ChunkStore>) -> ChainWalk {
        ChainWalk {
            inner: Arc::new(Inner {
                tree,
                chunks,
                spills: Mutex::new(HashMap::new()),
                membership: Mutex::new(Membership::default()),
                fetches: AtomicU64::new(0),
            }),
        }
    }

    /// Spilled chunk lists fetched from the store by this walker.
    pub fn spill_fetches(&self) -> u64 {
        self.inner.fetches.load(Ordering::Relaxed)
    }

    /// The occurrences of a chain's first snapshot, by a full walk of
    /// its directory. The multiset version of
    /// [`crate::snapshot::snapshot_chunk_refs`].
    pub async fn first(&self, root: SnapshotRoot) -> Result<Occurrences> {
        let inner = self.inner.clone();
        blocking(move |handle| {
            let tree = inner.tree.tree()?;
            let side = Side {
                tree: &tree,
                root: root.root,
            };
            inner.walk(&side, root.ino, handle)
        })
        .await
    }

    /// The occurrence deltas from `prev` to `next`, two snapshots of the
    /// same chain (`prev.ino == next.ino`; asserted). See the module
    /// docs for how keys map into the subtree.
    pub async fn step(&self, prev: SnapshotRoot, next: SnapshotRoot) -> Result<Deltas> {
        assert_eq!(
            prev.ino, next.ino,
            "ChainWalk::step across two chains ({} and {})",
            prev.ino, next.ino
        );
        let inner = self.inner.clone();
        blocking(move |handle| {
            let tree = inner.tree.tree()?;
            let before = Side {
                tree: &tree,
                root: prev.root,
            };
            let after = Side {
                tree: &tree,
                root: next.root,
            };
            match inner.step(&before, &after, prev.ino, handle) {
                Ok(deltas) => Ok(deltas),
                Err(error) => match error.downcast::<Unclear>() {
                    Ok(unclear) => {
                        tracing::debug!(
                            dir = prev.ino,
                            prev = prev.seq,
                            next = next.seq,
                            reason = %unclear,
                            "snapwalk: step falls back to two full walks"
                        );
                        let mut deltas = inner
                            .walk(&after, next.ino, handle)?
                            .minus(&inner.walk(&before, prev.ino, handle)?);
                        deltas.fell_back = true;
                        Ok(deltas)
                    }
                    Err(error) => Err(error),
                },
            }
        })
        .await
    }

    /// [`in_subtree`] against this walker's cache, off the runtime.
    pub async fn in_subtree(&self, root: NodeHash, ino: Ino, dir: Ino) -> Result<bool> {
        let inner = self.inner.clone();
        blocking(move |_| {
            let tree = inner.tree.tree()?;
            let mut membership = inner.membership.lock().expect("membership lock");
            in_subtree(&tree, &root, ino, dir, &mut membership)
        })
        .await
    }

    /// Add every chunk object any snapshot of one chain keeps alive to
    /// `protected`: `first(chain[0]) ∪ ⋃ additions(step_k)`. `chain` is
    /// one directory's snapshots in `(seq, created_unix_ms)` order.
    pub async fn protect_chain(
        &self,
        chain: &[SnapshotRoot],
        protected: &mut std::collections::HashSet<ChunkHash>,
    ) -> Result<()> {
        let Some(first) = chain.first() else {
            return Ok(());
        };
        protected.extend(self.first(*first).await?.hashes().copied());
        for pair in chain.windows(2) {
            if pair[0] == pair[1] {
                continue;
            }
            protected.extend(self.step(pair[0], pair[1]).await?.additions().copied());
        }
        Ok(())
    }
}

/// Run a synchronous tree read on a blocking thread (the node cache
/// bridges to async I/O with `block_in_place`).
async fn blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&tokio::runtime::Handle) -> Result<T> + Send + 'static,
{
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || f(&handle))
        .await
        .context("snapwalk tree read task")?
}

impl TreeAccess {
    fn tree(&self) -> Result<Tree<Arc<NodeCache>>> {
        Ok(Tree::with_config(self.nodes.clone(), self.config)?)
    }
}

impl Inner {
    /// A spilled chunk list, at most one GET per walker.
    fn spilled(
        &self,
        hash: &ChunkHash,
        handle: &tokio::runtime::Handle,
    ) -> Result<Arc<SpilledList>> {
        if let Some(list) = self.spills.lock().expect("spill lock").get(hash) {
            return Ok(list.clone());
        }
        let cached = process_spills().lock().expect("spill lru lock").get(hash);
        let list = match cached {
            Some(list) => list,
            None => {
                let bytes =
                    tokio::task::block_in_place(|| handle.block_on(self.chunks.get_chunk(hash)))
                        .with_context(|| format!("spilled chunk list {}", hash.to_hex()))?;
                self.fetches.fetch_add(1, Ordering::Relaxed);
                let list = Arc::new(SpilledList {
                    chunks: decode_chunk_list(&bytes)?,
                    encoded_len: bytes.len() as u64,
                });
                process_spills()
                    .lock()
                    .expect("spill lru lock")
                    .insert(*hash, list.clone());
                list
            }
        };
        self.spills
            .lock()
            .expect("spill lock")
            .insert(*hash, list.clone());
        Ok(list)
    }

    /// A file's occurrences, `count` times each.
    fn add_manifest(
        &self,
        bytes: &[u8],
        count: i64,
        handle: &tokio::runtime::Handle,
        out: &mut impl FnMut(ChunkHash, u64, i64),
    ) -> Result<()> {
        let manifest = Manifest::decode(bytes)?;
        let chunk_size = manifest.layout.chunk_size as u64;
        let size_at = |index: u64| {
            manifest
                .file_len
                .saturating_sub(index.saturating_mul(chunk_size))
                .min(chunk_size)
        };
        match &manifest.chunks {
            ChunkInfo::Inline(chunks) => {
                for (index, hash) in chunks {
                    out(*hash, size_at(*index), count);
                }
            }
            ChunkInfo::Spilled(spill) => {
                let list = self.spilled(spill, handle)?;
                out(*spill, list.encoded_len, count);
                for (index, hash) in &list.chunks {
                    out(*hash, size_at(*index), count);
                }
            }
        }
        Ok(())
    }

    /// A file's manifest bytes in one root; `None` for anything that is
    /// not a file with content.
    fn manifest_of(
        &self,
        side: &Side<'_>,
        ino: Ino,
        handle: &tokio::runtime::Handle,
    ) -> Result<Option<Vec<u8>>> {
        let Some(value) = side.tree.get(&side.root, &keys::inode(ino))? else {
            return Ok(None);
        };
        let record = InodeRecord::decode(&value).with_context(|| format!("inode {ino}"))?;
        if record.attrs.kind != Kind::File {
            return Ok(None);
        }
        let resolver = Resolver {
            blobs: &self.tree.blobs,
            handle,
        };
        record
            .manifest
            .as_ref()
            .map(|payload| resolver.payload(payload))
            .transpose()
    }

    /// The full walk: every dentry under `dir`, files counted once per
    /// name (the old `snapshot_chunk_refs` visit order and multiplicity).
    fn walk(
        &self,
        side: &Side<'_>,
        dir: Ino,
        handle: &tokio::runtime::Handle,
    ) -> Result<Occurrences> {
        let mut occurrences = Occurrences::default();
        let mut linked: HashMap<Ino, Option<Vec<u8>>> = HashMap::new();
        let mut stack = vec![dir];
        while let Some(parent) = stack.pop() {
            for (child, kind, nlink) in dentries(side, parent)? {
                match kind {
                    Kind::Dir => stack.push(child),
                    Kind::File => {
                        // A hardlinked file is read once, counted per name.
                        let bytes = if nlink > 1 {
                            match linked.get(&child) {
                                Some(bytes) => bytes.clone(),
                                None => {
                                    let bytes = self.manifest_of(side, child, handle)?;
                                    linked.insert(child, bytes.clone());
                                    bytes
                                }
                            }
                        } else {
                            self.manifest_of(side, child, handle)?
                        };
                        if let Some(bytes) = bytes {
                            self.add_manifest(&bytes, 1, handle, &mut |hash, size, count| {
                                occurrences.add(hash, size, count as u64)
                            })?;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(occurrences)
    }

    /// Every file reachable under `dir` in one root.
    fn files_under(&self, side: &Side<'_>, dir: Ino, out: &mut BTreeSet<Ino>) -> Result<()> {
        let mut stack = vec![dir];
        while let Some(parent) = stack.pop() {
            for (child, kind, _) in dentries(side, parent)? {
                match kind {
                    Kind::Dir => stack.push(child),
                    Kind::File => {
                        out.insert(child);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn step(
        &self,
        before: &Side<'_>,
        after: &Side<'_>,
        dir: Ino,
        handle: &tokio::runtime::Handle,
    ) -> Result<Deltas> {
        let mut deltas = Deltas::default();
        if before.root == after.root {
            return Ok(deltas);
        }
        for side in [before, after] {
            if kind_of(side, dir)? != Some(Kind::Dir) {
                return Err(Unclear(format!(
                    "the chain's directory {dir} is not a directory in every root"
                ))
                .into());
            }
        }
        let mut membership = self.membership.lock().expect("membership lock");
        let mut answers_before = membership
            .answers
            .remove(&(before.root, dir))
            .unwrap_or_default();
        let mut answers_after = membership
            .answers
            .remove(&(after.root, dir))
            .unwrap_or_default();
        let result = self.step_with(
            before,
            after,
            dir,
            handle,
            &mut answers_before,
            &mut answers_after,
            &mut deltas,
        );
        membership
            .answers
            .insert((before.root, dir), answers_before);
        membership.answers.insert((after.root, dir), answers_after);
        result?;
        deltas.prune();
        Ok(deltas)
    }

    #[allow(clippy::too_many_arguments)]
    fn step_with(
        &self,
        before: &Side<'_>,
        after: &Side<'_>,
        dir: Ino,
        handle: &tokio::runtime::Handle,
        answers_before: &mut HashMap<Ino, bool>,
        answers_after: &mut HashMap<Ino, bool>,
        deltas: &mut Deltas,
    ) -> Result<()> {
        // Inodes whose term may differ, and inodes whose names changed.
        let mut files = BTreeSet::new();
        let mut renamed = BTreeSet::new();
        for (key, _) in before.tree.diff(&before.root, &after.root)? {
            match Key::parse(&key)? {
                Key::Inode { ino } => {
                    files.insert(ino);
                }
                Key::RDentry { ino, .. } => {
                    files.insert(ino);
                    renamed.insert(ino);
                }
                // `0x02` mirrors `0x04` (module docs); xattrs and
                // subsystem rows hold no chunks.
                Key::Dentry { .. } | Key::Xattr { .. } | Key::Subsystem { .. } => {}
            }
        }
        // A directory whose name changed and whose membership differs
        // crossed the boundary: everything below it in the root where it
        // is a member is a candidate.
        for &ino in &renamed {
            let was = kind_of(before, ino)? == Some(Kind::Dir)
                && dir_in_subtree(before.tree, &before.root, ino, dir, answers_before)?;
            let is = kind_of(after, ino)? == Some(Kind::Dir)
                && dir_in_subtree(after.tree, &after.root, ino, dir, answers_after)?;
            match (was, is) {
                (true, false) => self.files_under(before, ino, &mut files)?,
                (false, true) => self.files_under(after, ino, &mut files)?,
                _ => {}
            }
        }
        let mut add = |hash, size, count| deltas.add(hash, size, count);
        for ino in files {
            let links_before = match kind_of(before, ino)? {
                Some(Kind::File) => {
                    links_in_subtree(before.tree, &before.root, ino, dir, answers_before)?
                }
                _ => 0,
            };
            let links_after = match kind_of(after, ino)? {
                Some(Kind::File) => {
                    links_in_subtree(after.tree, &after.root, ino, dir, answers_after)?
                }
                _ => 0,
            };
            if links_before == 0 && links_after == 0 {
                continue;
            }
            let manifest_before = match links_before {
                0 => None,
                _ => self.manifest_of(before, ino, handle)?,
            };
            let manifest_after = match links_after {
                0 => None,
                _ => self.manifest_of(after, ino, handle)?,
            };
            if links_before == links_after && manifest_before == manifest_after {
                continue;
            }
            if let Some(bytes) = &manifest_before {
                self.add_manifest(bytes, -(links_before as i64), handle, &mut add)?;
            }
            if let Some(bytes) = &manifest_after {
                self.add_manifest(bytes, links_after as i64, handle, &mut add)?;
            }
        }
        Ok(())
    }
}

/// An inode's kind in one root, `None` when it has no record there.
fn kind_of(side: &Side<'_>, ino: Ino) -> Result<Option<Kind>> {
    match side.tree.get(&side.root, &keys::inode(ino))? {
        Some(value) => Ok(Some(
            InodeRecord::decode(&value)
                .with_context(|| format!("inode {ino}"))?
                .attrs
                .kind,
        )),
        None => Ok(None),
    }
}

/// A directory's children as `(ino, kind, nlink)`, from the dentry
/// attr copies (one range scan, no inode reads).
fn dentries(side: &Side<'_>, dir: Ino) -> Result<Vec<(Ino, Kind, u32)>> {
    let range = keys::dentries_of(dir);
    let mut out = Vec::new();
    for (key, value) in side
        .tree
        .range(&side.root, range.start(), range.prefix(), usize::MAX)?
    {
        if !matches!(Key::parse(&key)?, Key::Dentry { .. }) {
            bail!("a non-dentry key inside directory {dir}'s range");
        }
        let record = DentryRecord::decode(&value)?;
        out.push((record.ino, record.attrs.kind, record.attrs.nlink));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gc::SnapWalkMode;
    use crate::snapshot::{test_manager, SnapshotManager, SnapshotOptions};
    use constellation_fs_core::types::ROOT_INO;
    use constellation_fs_core::InodeKind;
    use constellation_meta::{Meta, MetaStore};
    use constellation_store_s3::CompressionSetting;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::collections::HashSet;

    const CS: u32 = 4096;
    /// Chunk lists longer than this spill (small, so tests spill often).
    const INLINE_MAX: usize = 3;

    fn h(tag: &str) -> ChunkHash {
        ChunkHash::of(tag.as_bytes())
    }

    struct Fixture {
        meta: Arc<Meta>,
        store: Arc<dyn ObjectStore>,
        chunks: Arc<ChunkStore>,
        manager: SnapshotManager,
        walk: ChainWalk,
        _nodes: tempfile::TempDir,
        segment: u64,
        taken: u64,
    }

    impl Fixture {
        fn new() -> Fixture {
            let meta = Arc::new(crate::mtree_publish::test_meta());
            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let chunks = Arc::new(ChunkStore::new(store.clone()));
            let (manager, nodes) = test_manager(meta.clone(), chunks.clone(), CS);
            let walk = ChainWalk::new(manager.tree().unwrap().clone(), chunks.clone());
            Fixture {
                meta,
                store,
                chunks,
                manager,
                walk,
                _nodes: nodes,
                segment: 0,
                taken: 0,
            }
        }

        fn mkdir(&self, parent: Ino, name: &str) -> Ino {
            self.meta.mkdir(parent, name, 0o755, 1, 1).unwrap().ino
        }

        /// Write `ino` as full chunks `tags[i]` at index `i` (`""` is a
        /// hole), spilling the list when it is long.
        async fn write(&self, ino: Ino, tags: &[&str]) {
            let file_len = tags.len() as u64 * CS as u64;
            let sparse: SparseChunks = tags
                .iter()
                .enumerate()
                .filter(|(_, tag)| !tag.is_empty())
                .map(|(index, tag)| (index as u64, h(tag)))
                .collect();
            let (manifest, blob) =
                Manifest::from_sparse_chunks(CS, file_len, sparse, INLINE_MAX, ChunkHash::of);
            if let (ChunkInfo::Spilled(hash), Some(blob)) = (&manifest.chunks, blob) {
                self.chunks
                    .put_chunk(hash, &blob, CompressionSetting::RAW)
                    .await
                    .unwrap();
            }
            self.meta
                .set_manifest(ino, &manifest.encode(), file_len)
                .unwrap();
        }

        async fn file(&self, parent: Ino, name: &str, tags: &[&str]) -> Ino {
            let ino = self.meta.create(parent, name, 0o644, 1, 1).unwrap().ino;
            self.write(ino, tags).await;
            ino
        }

        /// Take a snapshot of `path` and return its root.
        async fn snap(&mut self, path: &str) -> SnapshotRoot {
            self.segment += 1;
            let rows = self.meta.take_journal(usize::MAX).unwrap();
            let seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
            self.meta.ack_journal_rows_at(&seqs, self.segment).unwrap();
            self.taken += 1;
            let (_, row) = self
                .manager
                .create_with(
                    path,
                    &format!("s{}", self.taken),
                    &SnapshotOptions::default(),
                )
                .await
                .unwrap();
            SnapshotRoot::parse(&row.root_hash).unwrap()
        }

        /// `step(prev, next)`, checked against the two full walks.
        async fn step(&self, prev: SnapshotRoot, next: SnapshotRoot) -> Deltas {
            let deltas = self.walk.step(prev, next).await.unwrap();
            assert!(!deltas.fell_back, "a well-formed history fell back");
            let mut occurrences = self.walk.first(prev).await.unwrap();
            occurrences.apply(&deltas).unwrap();
            assert_eq!(occurrences, self.walk.first(next).await.unwrap());
            deltas
        }

        /// The occurrences of the live tree under `dir`, straight from
        /// the replica: an implementation independent of the tree reader.
        async fn brute(&self, dir: Ino) -> Occurrences {
            let mut out = Occurrences::default();
            let mut stack = vec![dir];
            while let Some(parent) = stack.pop() {
                for entry in self.meta.readdir(parent).unwrap() {
                    match entry.kind {
                        InodeKind::Dir => stack.push(entry.ino),
                        InodeKind::File => {
                            let Some(bytes) = self.meta.manifest(entry.ino).unwrap() else {
                                continue;
                            };
                            let manifest = Manifest::decode(&bytes).unwrap();
                            let cs = manifest.layout.chunk_size as u64;
                            let size = |i: u64| (manifest.file_len - i * cs).min(cs);
                            let list = match &manifest.chunks {
                                ChunkInfo::Inline(list) => list.clone(),
                                ChunkInfo::Spilled(spill) => {
                                    let blob = self.chunks.get_chunk(spill).await.unwrap();
                                    out.add(*spill, blob.len() as u64, 1);
                                    decode_chunk_list(&blob).unwrap()
                                }
                            };
                            for (index, hash) in list {
                                out.add(hash, size(index), 1);
                            }
                        }
                        _ => {}
                    }
                }
            }
            out
        }
    }

    /// The deltas as `(tag, delta)`, resolved against `tags`.
    fn named(deltas: &Deltas, tags: &[&str]) -> Vec<(String, i64)> {
        let mut out: Vec<(String, i64)> = deltas
            .iter()
            .map(|delta| {
                let name = tags
                    .iter()
                    .find(|tag| h(tag) == delta.hash)
                    .map(|tag| tag.to_string())
                    .unwrap_or_else(|| "<spill>".into());
                (name, delta.delta)
            })
            .collect();
        out.sort();
        out
    }

    fn pairs(expected: &[(&str, i64)]) -> Vec<(String, i64)> {
        let mut out: Vec<(String, i64)> = expected
            .iter()
            .map(|(tag, delta)| (tag.to_string(), *delta))
            .collect();
        out.sort();
        out
    }

    const TAGS: &[&str] = &["a", "b", "c", "d", "e", "f", "x", "y", "z"];

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_truncate_delete_and_revert() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let file = fx.file(vol, "f", &["a"]).await;
        let s0 = fx.snap("/vol").await;
        assert_eq!(fx.walk.first(s0).await.unwrap(), fx.brute(vol).await);

        // Write: one chunk appended.
        fx.write(file, &["a", "b"]).await;
        let s1 = fx.snap("/vol").await;
        let deltas = fx.step(s0, s1).await;
        assert_eq!(named(&deltas, TAGS), pairs(&[("b", 1)]));
        assert_eq!(deltas.iter().next().unwrap().size_bytes, CS as u64);

        // Truncate back to one chunk.
        fx.write(file, &["a"]).await;
        let s2 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s1, s2).await, TAGS), pairs(&[("b", -1)]));

        // Overwrite, then revert to the old content: the chunk comes back.
        fx.write(file, &["c"]).await;
        let s3 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s2, s3).await, TAGS),
            pairs(&[("a", -1), ("c", 1)])
        );
        fx.write(file, &["a"]).await;
        let s4 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s3, s4).await, TAGS),
            pairs(&[("a", 1), ("c", -1)])
        );

        // Delete.
        fx.meta.unlink(vol, "f").unwrap();
        let s5 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s4, s5).await, TAGS), pairs(&[("a", -1)]));
        assert!(fx.walk.first(s5).await.unwrap().is_empty());

        // Two identical files: the chunk occurs twice; deleting one is -1.
        fx.file(vol, "g", &["d"]).await;
        fx.file(vol, "h", &["d"]).await;
        let s6 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s5, s6).await, TAGS), pairs(&[("d", 2)]));
        fx.meta.unlink(vol, "g").unwrap();
        let s7 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s6, s7).await, TAGS), pairs(&[("d", -1)]));
    }

    /// A sparse file: holes have no occurrences, and the last chunk's size
    /// is the file's tail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sizes_are_sparse_aware() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let file = fx.meta.create(vol, "sparse", 0o644, 1, 1).unwrap().ino;
        let s0 = fx.snap("/vol").await;
        let len = 3 * CS as u64 + 100;
        let sparse: SparseChunks = [(0, h("a")), (3, h("b"))].into_iter().collect();
        let (manifest, _) = Manifest::from_sparse_chunks(CS, len, sparse, 8, ChunkHash::of);
        fx.meta.set_manifest(file, &manifest.encode(), len).unwrap();
        let s1 = fx.snap("/vol").await;
        let deltas = fx.step(s0, s1).await;
        let sizes: BTreeMap<ChunkHash, (i64, u64)> = deltas
            .iter()
            .map(|delta| (delta.hash, (delta.delta, delta.size_bytes)))
            .collect();
        assert_eq!(sizes[&h("a")], (1, CS as u64));
        assert_eq!(sizes[&h("b")], (1, 100));
        assert_eq!(sizes.len(), 2);
    }

    /// Atime never reaches the tree, and a chmod changes the record but not
    /// the manifest: neither is a delta.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn atime_and_attribute_only_changes_have_no_delta() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let file = fx.file(vol, "f", &["a", "b"]).await;
        let s0 = fx.snap("/vol").await;
        let later = i64::MAX / 4;
        fx.meta.apply_atime(&[(file, later, later)]).unwrap();
        let s1 = fx.snap("/vol").await;
        assert!(fx.step(s0, s1).await.is_empty());
        fx.meta
            .setattr(file, Some(0o600), None, None, None, None, Some(12345))
            .unwrap();
        let s2 = fx.snap("/vol").await;
        assert!(fx.step(s1, s2).await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hardlinks_weigh_once_per_name_in_the_subtree() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let out = fx.mkdir(ROOT_INO, "out");
        let sub = fx.mkdir(vol, "sub");
        let file = fx.file(out, "f", &["x"]).await;
        let s0 = fx.snap("/vol").await;
        assert!(fx.walk.first(s0).await.unwrap().is_empty());

        // A link into the subtree, then a second one.
        fx.meta.link(file, vol, "in1").unwrap();
        let s1 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s0, s1).await, TAGS), pairs(&[("x", 1)]));
        fx.meta.link(file, sub, "in2").unwrap();
        let s2 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s1, s2).await, TAGS), pairs(&[("x", 1)]));
        assert_eq!(fx.walk.first(s2).await.unwrap().count(&h("x")), 2);

        // Writing through any name changes every in-subtree name's share.
        fx.write(file, &["y"]).await;
        let s3 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s2, s3).await, TAGS),
            pairs(&[("x", -2), ("y", 2)])
        );

        // The outside name goes: nothing changes inside.
        fx.meta.unlink(out, "f").unwrap();
        let s4 = fx.snap("/vol").await;
        assert!(fx.step(s3, s4).await.is_empty());

        // A link leaves the subtree by rename, and one is unlinked.
        fx.meta.rename(sub, "in2", out, "back").unwrap();
        let s5 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s4, s5).await, TAGS), pairs(&[("y", -1)]));
        fx.meta.unlink(vol, "in1").unwrap();
        let s6 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s5, s6).await, TAGS), pairs(&[("y", -1)]));
        assert_eq!(fx.walk.first(s6).await.unwrap(), fx.brute(vol).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn renames_within_and_across_the_boundary() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let out = fx.mkdir(ROOT_INO, "out");
        let sub = fx.mkdir(vol, "sub");
        fx.file(vol, "a", &["a"]).await;
        fx.file(out, "b", &["b"]).await;
        let s0 = fx.snap("/vol").await;

        // Within: no delta.
        fx.meta.rename(vol, "a", sub, "a2").unwrap();
        let s1 = fx.snap("/vol").await;
        assert!(fx.step(s0, s1).await.is_empty());

        // A file in, then out.
        fx.meta.rename(out, "b", sub, "b").unwrap();
        let s2 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s1, s2).await, TAGS), pairs(&[("b", 1)]));
        fx.meta.rename(sub, "b", out, "b").unwrap();
        let s3 = fx.snap("/vol").await;
        assert_eq!(named(&fx.step(s2, s3).await, TAGS), pairs(&[("b", -1)]));

        // A rename over an existing file replaces its content.
        fx.file(vol, "victim", &["c"]).await;
        let s4 = fx.snap("/vol").await;
        fx.meta.rename(out, "b", vol, "victim").unwrap();
        let s5 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s4, s5).await, TAGS),
            pairs(&[("b", 1), ("c", -1)])
        );
    }

    /// A directory crossing the boundary carries everything below it, its
    /// own files and nested directories and hardlinks alike, both ways.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_directory_crossing_the_boundary_carries_its_subtree() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let out = fx.mkdir(ROOT_INO, "out");
        let moving = fx.mkdir(out, "moving");
        let deep = fx.mkdir(moving, "deep");
        let deeper = fx.mkdir(deep, "deeper");
        fx.file(moving, "m", &["a", "b"]).await;
        let linked = fx.file(deeper, "l", &["c"]).await;
        fx.meta.link(linked, deep, "l2").unwrap();
        fx.file(vol, "stay", &["d"]).await;
        let s0 = fx.snap("/vol").await;

        fx.meta.rename(out, "moving", vol, "moved").unwrap();
        let s1 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s0, s1).await, TAGS),
            pairs(&[("a", 1), ("b", 1), ("c", 2)])
        );

        // Changes inside the moved tree are ordinary changes now.
        fx.write(linked, &["e"]).await;
        let s2 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s1, s2).await, TAGS),
            pairs(&[("c", -2), ("e", 2)])
        );

        // And out again, from one level down.
        fx.meta.rename(vol, "moved", out, "gone").unwrap();
        let s3 = fx.snap("/vol").await;
        assert_eq!(
            named(&fx.step(s2, s3).await, TAGS),
            pairs(&[("a", -1), ("b", -1), ("e", -2)])
        );
        assert_eq!(fx.walk.first(s3).await.unwrap(), fx.brute(vol).await);

        // Moving a directory between two outside places is nothing.
        let elsewhere = fx.mkdir(ROOT_INO, "elsewhere");
        fx.meta.rename(out, "gone", elsewhere, "gone").unwrap();
        let s4 = fx.snap("/vol").await;
        assert!(fx.step(s3, s4).await.is_empty());
    }

    /// A chain on a nested directory sees only its own subtree, and the
    /// chain above it sees the nested changes too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_nested_subtree_is_its_own_chain() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let inner = fx.mkdir(vol, "inner");
        let outer_file = fx.file(vol, "o", &["a"]).await;
        let inner_file = fx.file(inner, "i", &["b"]).await;
        let outer0 = fx.snap("/vol").await;
        let inner0 = fx.snap("/vol/inner").await;
        assert_eq!(fx.walk.first(outer0).await.unwrap().len(), 2);
        assert_eq!(fx.walk.first(inner0).await.unwrap().len(), 1);

        fx.write(outer_file, &["c"]).await;
        fx.write(inner_file, &["d"]).await;
        let outer1 = fx.snap("/vol").await;
        let inner1 = fx.snap("/vol/inner").await;
        assert_eq!(
            named(&fx.step(inner0, inner1).await, TAGS),
            pairs(&[("b", -1), ("d", 1)])
        );
        assert_eq!(
            named(&fx.step(outer0, outer1).await, TAGS),
            pairs(&[("a", -1), ("b", -1), ("c", 1), ("d", 1)])
        );

        // A file moving from the outer chain into the inner one is +1 for
        // the inner chain and nothing for the outer.
        fx.meta.rename(vol, "o", inner, "o").unwrap();
        let outer2 = fx.snap("/vol").await;
        let inner2 = fx.snap("/vol/inner").await;
        assert_eq!(
            named(&fx.step(inner1, inner2).await, TAGS),
            pairs(&[("c", 1)])
        );
        assert!(fx.step(outer1, outer2).await.is_empty());
        // Membership agrees.
        assert!(fx
            .walk
            .in_subtree(inner2.root, outer_file, inner)
            .await
            .unwrap());
        assert!(!fx
            .walk
            .in_subtree(inner1.root, outer_file, inner)
            .await
            .unwrap());
        assert!(fx.walk.in_subtree(outer2.root, inner, vol).await.unwrap());
        assert!(!fx.walk.in_subtree(outer2.root, vol, inner).await.unwrap());
    }

    /// A spilled chunk list is an occurrence of its own, at its encoded
    /// length, and is fetched once per walker however often it is read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spilled_manifests() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let file = fx.file(vol, "big", &["a", "b", "c", "d", "e"]).await;
        let s0 = fx.snap("/vol").await;
        fx.write(file, &["a", "b", "c", "d", "f"]).await;
        let s1 = fx.snap("/vol").await;
        let deltas = fx.step(s0, s1).await;
        let named = named(&deltas, TAGS);
        assert_eq!(
            named,
            pairs(&[("<spill>", -1), ("<spill>", 1), ("e", -1), ("f", 1)])
        );
        let first = fx.walk.first(s0).await.unwrap();
        assert_eq!(first.len(), 6, "five chunks and the list");
        let spill = first
            .iter()
            .find(|(hash, _)| !TAGS.iter().any(|tag| h(tag) == **hash))
            .map(|(hash, occurrence)| (*hash, occurrence.size_bytes))
            .unwrap();
        let blob = fx.chunks.get_chunk(&spill.0).await.unwrap();
        assert_eq!(spill.1, blob.len() as u64);
        // Fresh walker: each list GET once, whatever the number of reads
        // (the process LRU may already hold them, so at most once).
        let walk = ChainWalk::new(fx.manager.tree().unwrap().clone(), fx.chunks.clone());
        for _ in 0..3 {
            walk.first(s0).await.unwrap();
            walk.first(s1).await.unwrap();
            walk.step(s0, s1).await.unwrap();
        }
        assert!(walk.spill_fetches() <= 2, "{}", walk.spill_fetches());
    }

    /// The chain's directory replaced by another at the same path is a
    /// different chain; the same ino missing from a root falls back to
    /// full walks rather than guessing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unclear_step_falls_back_to_full_walks() {
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        fx.file(vol, "f", &["a"]).await;
        let s0 = fx.snap("/vol").await;
        fx.meta.unlink(vol, "f").unwrap();
        fx.meta.rmdir(ROOT_INO, "vol").unwrap();
        let other = fx.mkdir(ROOT_INO, "other");
        fx.file(other, "g", &["b"]).await;
        let s1 = fx.snap("/other").await;
        // Same ino, but `vol` is gone in `s1`: pretend it is a chain.
        let gone = SnapshotRoot { ino: vol, ..s1 };
        let deltas = fx.walk.step(s0, gone).await.unwrap();
        assert!(deltas.fell_back);
        assert_eq!(named(&deltas, TAGS), pairs(&[("a", -1)]));
    }

    #[test]
    #[should_panic(expected = "across two chains")]
    fn a_step_across_chains_is_a_bug() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let fx = Fixture::new();
            let root = |ino| SnapshotRoot {
                seq: 1,
                root: NodeHash([0; 32]),
                ino,
            };
            let _ = fx.walk.step(root(1), root(2)).await;
        });
    }

    // ---------------------------------------------------- randomized

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }

        fn pick<T: Clone>(&mut self, items: &[T]) -> Option<T> {
            (!items.is_empty()).then(|| items[self.below(items.len())].clone())
        }
    }

    /// Every `(parent, name, ino, kind)` reachable from the root.
    fn entries(meta: &Meta) -> Vec<(Ino, String, Ino, InodeKind)> {
        let mut out = Vec::new();
        let mut stack = vec![ROOT_INO];
        while let Some(parent) = stack.pop() {
            for entry in meta.readdir(parent).unwrap() {
                if entry.kind == InodeKind::Dir {
                    stack.push(entry.ino);
                }
                out.push((parent, entry.name, entry.ino, entry.kind));
            }
        }
        out
    }

    struct Chain {
        path: &'static str,
        ino: Ino,
        /// `(snapshot name, root)`, oldest first; deleted ones removed.
        snaps: Vec<(String, SnapshotRoot)>,
        latest: Option<SnapshotRoot>,
        occurrences: Occurrences,
    }

    async fn gc_sets(fx: &Fixture) -> (HashSet<ChunkHash>, HashSet<ChunkHash>) {
        let diff = crate::gc::snapshot_roots_with(
            SnapWalkMode::Diff,
            &fx.chunks,
            fx.store.clone(),
            &fx.meta,
        )
        .await
        .unwrap();
        let full = crate::gc::snapshot_roots_with(
            SnapWalkMode::Full,
            &fx.chunks,
            fx.store.clone(),
            &fx.meta,
        )
        .await
        .unwrap();
        (diff, full)
    }

    async fn random_history(seed: u64) {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let delete_from_middle = seed % 2 == 1;
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let inner = fx.mkdir(vol, "inner");
        fx.mkdir(ROOT_INO, "out");
        let pool: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
        let mut chains = vec![
            Chain {
                path: "/vol",
                ino: vol,
                snaps: Vec::new(),
                latest: None,
                occurrences: Occurrences::default(),
            },
            Chain {
                path: "/vol/inner",
                ino: inner,
                snaps: Vec::new(),
                latest: None,
                occurrences: Occurrences::default(),
            },
        ];
        let mut deleted = 0;
        let mut names = 0u64;
        for _ in 0..60 {
            let all = entries(&fx.meta);
            let dirs: Vec<Ino> = std::iter::once(ROOT_INO)
                .chain(all.iter().filter(|e| e.3 == InodeKind::Dir).map(|e| e.2))
                .collect();
            let files: Vec<_> = all
                .iter()
                .filter(|e| e.3 == InodeKind::File)
                .cloned()
                .collect();
            // Movable directories: not the chains' own, not the root.
            let movable: Vec<_> = all
                .iter()
                .filter(|e| e.3 == InodeKind::Dir && e.2 != vol && e.2 != inner)
                .cloned()
                .collect();
            names += 1;
            let name = format!("n{names}");
            let tags = |rng: &mut Rng| -> Vec<String> {
                let n = 1 + rng.below(5);
                (0..n)
                    .map(|_| match rng.below(6) {
                        0 => String::new(),
                        _ => pool[rng.below(pool.len())].clone(),
                    })
                    .collect()
            };
            match rng.below(12) {
                0 | 1 => {
                    let parent = rng.pick(&dirs).unwrap();
                    let tags = tags(&mut rng);
                    let tags: Vec<&str> = tags.iter().map(String::as_str).collect();
                    fx.file(parent, &name, &tags).await;
                }
                2 | 3 => {
                    if let Some(file) = rng.pick(&files) {
                        let tags = tags(&mut rng);
                        let tags: Vec<&str> = tags.iter().map(String::as_str).collect();
                        fx.write(file.2, &tags).await;
                    }
                }
                4 => {
                    // Truncate to a whole-chunk prefix.
                    if let Some(file) = rng.pick(&files) {
                        if let Some(bytes) = fx.meta.manifest(file.2).unwrap() {
                            let manifest = Manifest::decode(&bytes).unwrap();
                            let keep = rng.below(3) as u64;
                            let list = match manifest.chunks {
                                ChunkInfo::Inline(list) => list,
                                ChunkInfo::Spilled(spill) => {
                                    decode_chunk_list(&fx.chunks.get_chunk(&spill).await.unwrap())
                                        .unwrap()
                                }
                            };
                            let tags: Vec<&str> = (0..keep)
                                .map(|i| {
                                    list.get(&i)
                                        .and_then(|hash| pool.iter().find(|t| h(t) == *hash))
                                        .map_or("", String::as_str)
                                })
                                .collect();
                            fx.write(file.2, &tags).await;
                        }
                    }
                }
                5 => {
                    if let Some(file) = rng.pick(&files) {
                        fx.meta.unlink(file.0, &file.1).unwrap();
                    }
                }
                6 => {
                    if let Some(file) = rng.pick(&files) {
                        let parent = rng.pick(&dirs).unwrap();
                        fx.meta.link(file.2, parent, &name).unwrap();
                    }
                }
                7 => {
                    // Rename a file anywhere, sometimes over another file.
                    if let Some(file) = rng.pick(&files) {
                        let (parent, target) = match rng.below(3) {
                            0 => match rng.pick(&files) {
                                Some(victim) => (victim.0, victim.1),
                                None => (rng.pick(&dirs).unwrap(), name.clone()),
                            },
                            _ => (rng.pick(&dirs).unwrap(), name.clone()),
                        };
                        let _ = fx.meta.rename(file.0, &file.1, parent, &target);
                    }
                }
                8 => {
                    // Rename a directory anywhere (a cycle is refused).
                    if let Some(dir) = rng.pick(&movable) {
                        let parent = rng.pick(&dirs).unwrap();
                        let _ = fx.meta.rename(dir.0, &dir.1, parent, &name);
                    }
                }
                9 => {
                    let parent = rng.pick(&dirs).unwrap();
                    fx.mkdir(parent, &name);
                }
                10 => {
                    if let Some(dir) = rng.pick(&movable) {
                        let _ = fx.meta.rmdir(dir.0, &dir.1);
                    }
                }
                _ => {
                    if let Some(file) = rng.pick(&files) {
                        let at = 1_000_000_000_000 + names as i64;
                        fx.meta.apply_atime(&[(file.2, at, at)]).unwrap();
                    }
                }
            }

            if rng.below(3) == 0 {
                let chain = &mut chains[rng.below(2)];
                let root = fx.snap(chain.path).await;
                let name = format!("s{}", fx.taken);
                match chain.latest {
                    None => chain.occurrences = fx.walk.first(root).await.unwrap(),
                    Some(prev) => {
                        let deltas = fx.walk.step(prev, root).await.unwrap();
                        assert!(!deltas.fell_back, "seed {seed}: a step fell back");
                        chain.occurrences.apply(&deltas).unwrap();
                    }
                }
                assert_eq!(
                    chain.occurrences,
                    fx.brute(chain.ino).await,
                    "seed {seed}: first + Σ steps differs from the snapshot's own multiset"
                );
                chain.latest = Some(root);
                chain.snaps.push((name, root));

                let (diff, full) = gc_sets(&fx).await;
                assert!(
                    diff.is_superset(&full),
                    "seed {seed}: diff protects less than full"
                );
                if deleted == 0 {
                    assert_eq!(diff, full, "seed {seed}: diff != full with no deletion");
                }
            }

            // Delete a snapshot from the middle of a chain.
            if delete_from_middle && rng.below(8) == 0 {
                let chain = &mut chains[rng.below(2)];
                if chain.snaps.len() >= 3 {
                    let at = 1 + rng.below(chain.snaps.len() - 2);
                    let (name, _) = chain.snaps.remove(at);
                    fx.manager.delete(chain.path, &name, false).await.unwrap();
                    deleted += 1;
                    let (diff, full) = gc_sets(&fx).await;
                    assert!(
                        diff.is_superset(&full),
                        "seed {seed}: diff protects less than full after a deletion"
                    );
                }
            }
        }
        // The surviving chains, re-walked from scratch, agree too.
        for chain in &chains {
            let roots: Vec<SnapshotRoot> = chain.snaps.iter().map(|(_, root)| *root).collect();
            let mut protected = HashSet::new();
            fx.walk.protect_chain(&roots, &mut protected).await.unwrap();
            let mut union = HashSet::new();
            for root in &roots {
                union.extend(fx.walk.first(*root).await.unwrap().hashes().copied());
            }
            assert_eq!(protected, union, "seed {seed}: {}", chain.path);
        }
        if delete_from_middle {
            assert!(deleted > 0 || chains.iter().all(|c| c.snaps.len() < 3));
        }
    }

    /// Plan 32 §0.2 / §11: seeded random histories. After every snapshot,
    /// `first + Σ steps` is the snapshot's own multiset, and GC's `diff`
    /// set covers `full`'s (equal when nothing was deleted mid-chain).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn random_histories_agree_with_the_brute_force() {
        for seed in 1..=16 {
            random_history(seed).await;
        }
    }

    /// Plan 32 §0.2's measurement (report only): GC's snapshot mark over
    /// 50 snapshots of a 10k-file tree, `full` versus `diff`. Run with
    /// `cargo test --release -p constellation-engine --lib
    /// snapwalk::tests::measure -- --ignored --nocapture`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "a measurement, not a check"]
    async fn measure_gc_mark_full_versus_diff() {
        const DIRS: usize = 100;
        const PER_DIR: usize = 100;
        const SNAPSHOTS: usize = 50;
        const CHANGES: usize = 20;
        let mut fx = Fixture::new();
        let vol = fx.mkdir(ROOT_INO, "vol");
        let mut files = Vec::new();
        for d in 0..DIRS {
            let dir = fx.mkdir(vol, &format!("d{d}"));
            for f in 0..PER_DIR {
                let tag = format!("{d}/{f}");
                files.push(
                    fx.file(dir, &format!("f{f}"), &[tag.as_str(), "shared"])
                        .await,
                );
            }
        }
        let mut rng = Rng(0x5eed);
        let started = std::time::Instant::now();
        for s in 0..SNAPSHOTS {
            for c in 0..CHANGES {
                let file = files[rng.below(files.len())];
                let tag = format!("s{s}c{c}");
                fx.write(file, &[tag.as_str(), "shared"]).await;
            }
            fx.snap("/vol").await;
        }
        eprintln!(
            "built {} files, {SNAPSHOTS} snapshots in {:?}",
            files.len(),
            started.elapsed()
        );
        for mode in [
            SnapWalkMode::Full,
            SnapWalkMode::Diff,
            SnapWalkMode::Full,
            SnapWalkMode::Diff,
        ] {
            let started = std::time::Instant::now();
            let set = crate::gc::snapshot_roots_with(mode, &fx.chunks, fx.store.clone(), &fx.meta)
                .await
                .unwrap();
            eprintln!(
                "MEASURE {mode:?}: {} protected chunks in {:?}",
                set.len(),
                started.elapsed()
            );
        }
    }
}
