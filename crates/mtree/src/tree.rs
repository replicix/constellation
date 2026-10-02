//! The tree itself: bulk build, point read, ordered cursor, incremental
//! [`Tree::apply`], structural [`Tree::diff`], and three-way
//! [`Tree::merge`].
//!
//! ## Why `apply` is written the way it is
//!
//! A commit must cost O(keys changed), not O(tree), or the whole design
//! collapses back into the checkpoint it replaces. A batch of sorted
//! edits is therefore grouped into *runs*: for each run the affected
//! node is loaded, the edits inside its key range are spliced in, and
//! the result is re-chunked. Because the boundary function is
//! context-free ([`crate::node::is_boundary`]), a re-chunked run that
//! ends on a boundary key re-synchronizes with the untouched remainder
//! of its level immediately — that is the point where the old and new
//! chunkings provably agree again — so a scattered batch pays for the
//! leaves it actually hits and for nothing in between. Only when an
//! edit *deletes* a boundary key, or when a node was sealed by the
//! entry clamp rather than by a boundary key, does the run have to
//! absorb its right neighbour, and the same loop handles that cascade.
//!
//! Each level is rewritten in turn, and the level above receives one
//! deletion per consumed node and one insertion per produced node. So
//! the whole commit is one uniform algorithm from the leaves to the
//! root, and the root is reached by rewriting the level the old root
//! occupied and then growing or collapsing until a single node remains.
//! The collapse matters for canonicality: a bulk build stops as soon as
//! a level holds one node, so a root with a single child is *not* the
//! canonical encoding of that key set and has to be shed.
//!
//! ## What the cursor is for, and why it is lazy
//!
//! [`Cursor`] iterates leaf entries in key order, which is what a
//! `readdir`, a `listxattr` and a range scan are. Its unusual property
//! is that advancing past the end of a node leaves the stack *short*
//! instead of immediately descending into the next subtree. That is
//! what lets [`Tree::diff`] compare the child hashes two cursors
//! already hold and step over an identical subtree without reading a
//! single one of its nodes — the mechanism behind §14.6's measurement
//! that a one-key diff of a 35.8M-key tree costs 20 node reads.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::config::{Config, LeafAgg};
use crate::error::MtreeError;
use crate::hash::NodeHash;
use crate::node::{self, Agg, Entry, NodeRef, Value};
use crate::store::NodeStore;

/// One key mutation: `Some` upserts, `None` deletes.
///
/// A tuple rather than a struct because callers build these by the
/// hundred thousand from a `BTreeMap` drain and `(key, value)` is
/// already that shape.
pub type Edit = (Vec<u8>, Option<Vec<u8>>);

/// An owned key/value pair, as a bulk build takes them and a range
/// scan returns them.
pub type Pair = (Vec<u8>, Vec<u8>);

/// The entry under a [`Cursor`], borrowed from the node that holds it.
pub type CursorEntry<'a> = (&'a [u8], &'a [u8]);

/// What one level of an `apply` hands upward: the edits the level above
/// must absorb, and every node this level produced.
type LevelRewrite = (BTreeMap<Vec<u8>, Option<Value>>, Vec<Entry>);

/// Sealed nodes, the unterminated tail, and whether the last seal was
/// forced by the entry clamp rather than by a boundary key.
type Chunked = (Vec<Entry>, Vec<Entry>, bool);

/// What [`Tree::diff`] found at a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    /// Present in `b`, absent from `a`.
    Added,
    /// Present in `a`, absent from `b`.
    Removed,
    /// Present in both with different values.
    Modified,
}

/// The outcome of a three-way [`Tree::merge`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Merged {
    Root(NodeHash),
    /// Exactly the keys both branches touched. Not a superset: the
    /// caller resolves these by POSIX rules (§12's errno matrix) and a
    /// spurious conflict would turn a legal concurrent operation into a
    /// spurious failure.
    Conflicts(Vec<Vec<u8>>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LevelStats {
    pub nodes: u64,
    pub entries: u64,
    pub bytes: u64,
}

/// Structural census, indexed by level (0 = leaves).
#[derive(Clone, Debug, Default)]
pub struct Census {
    pub levels: Vec<LevelStats>,
    /// Entries per leaf node — the distribution §14.1 reports, and what
    /// the entry clamp is measured against.
    pub leaf_entries: Vec<usize>,
}

/// A canonical Merkle map from opaque byte keys to opaque byte values.
///
/// Holds no tree state: a root is a [`NodeHash`] a caller keeps, so one
/// `Tree` serves every root in the store — snapshot, clone, branch,
/// history — and reading an old root is not a different code path.
pub struct Tree<S> {
    store: S,
    config: Config,
}

impl<S: NodeStore> Tree<S> {
    /// Default configuration: plain blake3, the §14 entry clamps, and
    /// no leaf-aggregate projection (so aggregates report key counts
    /// only).
    pub fn new(store: S) -> Tree<S> {
        Tree {
            store,
            config: Config::default(),
        }
    }

    pub fn with_config(store: S, config: Config) -> Result<Tree<S>, MtreeError> {
        config.validate()?;
        Ok(Tree { store, config })
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    fn leaf_agg(&self) -> LeafAgg {
        self.config.leaf_agg
    }

    fn load(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        self.store.get(hash)
    }

    /// The root of the empty map. A real node, not a sentinel, so that
    /// "every root names a node that exists" holds without exceptions
    /// and an empty filesystem needs no special case in the commit
    /// chain or in GC.
    pub fn empty(&self) -> Result<NodeHash, MtreeError> {
        let bytes = node::encode(0, &[]);
        let hash = self.config.hasher.hash(&bytes);
        self.store.put(hash, 0, bytes)?;
        Ok(hash)
    }

    // ------------------------------------------------------------ build

    /// Bulk build from a strictly ascending, deduplicated stream.
    ///
    /// Streams: it holds at most one pending run per level, so the
    /// memory cost is O(depth × MAX_ENTRIES) regardless of key count.
    pub fn build<I>(&self, sorted: I) -> Result<NodeHash, MtreeError>
    where
        I: IntoIterator<Item = Pair>,
    {
        let mut builder = Builder::new(self);
        let mut previous: Option<Vec<u8>> = None;
        for (key, value) in sorted {
            if previous.as_deref().is_some_and(|prev| prev >= &key[..]) {
                return Err(MtreeError::Unsorted);
            }
            previous = Some(key.clone());
            builder.push(0, Entry::leaf(key, value))?;
        }
        builder.finish()
    }

    // ------------------------------------------------------------- read

    pub fn get(&self, root: &NodeHash, key: &[u8]) -> Result<Option<Vec<u8>>, MtreeError> {
        let mut buf = self.load(root)?;
        loop {
            let next = {
                let node = NodeRef::new(&buf)?;
                if node.is_leaf() {
                    return match node.search(key)? {
                        Ok(i) => Ok(Some(node.leaf_value(i)?.to_vec())),
                        Err(_) => Ok(None),
                    };
                }
                if node.count() == 0 {
                    return Ok(None);
                }
                node.child(node.descend(key)?)?.0
            };
            buf = self.load(&next)?;
        }
    }

    pub fn contains(&self, root: &NodeHash, key: &[u8]) -> Result<bool, MtreeError> {
        Ok(self.get(root, key)?.is_some())
    }

    /// The whole tree's aggregate (§P7's `statfs` path): one node read,
    /// because the root already sums its children.
    pub fn aggregate(&self, root: &NodeHash) -> Result<Agg, MtreeError> {
        let buf = self.load(root)?;
        NodeRef::new(&buf)?.aggregate(self.leaf_agg())
    }

    /// A cursor positioned at the first key.
    pub fn cursor(&self, root: &NodeHash) -> Result<Cursor<'_, S>, MtreeError> {
        Cursor::open(self, *root, None)
    }

    /// A cursor positioned at `from`, or at the first key after it when
    /// `from` is absent from the tree. That is what makes a `readdir`
    /// page resumable from the previous page's last key without
    /// storing anything but the key.
    pub fn cursor_at(&self, root: &NodeHash, from: &[u8]) -> Result<Cursor<'_, S>, MtreeError> {
        Cursor::open(self, *root, Some(from))
    }

    /// Up to `limit` entries with `prefix`, starting at `from` (which
    /// need not be in the tree). `from` equal to `prefix` starts at the
    /// beginning of the range.
    pub fn range(
        &self,
        root: &NodeHash,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<Pair>, MtreeError> {
        let mut cursor = self.cursor_at(root, from)?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some((key, value)) = cursor.entry()? else {
                break;
            };
            if !key.starts_with(prefix) {
                break;
            }
            out.push((key.to_vec(), value.to_vec()));
            cursor.next()?;
        }
        Ok(out)
    }

    // ------------------------------------------------------------ write

    /// Apply a strictly ascending, deduplicated batch of edits and
    /// return the new root.
    ///
    /// Cost is O(edits + nodes they touch), not O(tree). An empty batch
    /// returns `root` unchanged without writing anything.
    pub fn apply(&self, root: &NodeHash, edits: &[Edit]) -> Result<NodeHash, MtreeError> {
        if edits.is_empty() {
            return Ok(*root);
        }
        if edits.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err(MtreeError::Unsorted);
        }
        let root_level = {
            let buf = self.load(root)?;
            NodeRef::new(&buf)?.level()
        };
        let mut level_edits: Vec<(Vec<u8>, Option<Value>)> = edits
            .iter()
            .map(|(key, value)| (key.clone(), value.clone().map(Value::Leaf)))
            .collect();
        let mut level = 0u8;
        loop {
            let (parent_edits, produced) = self.rewrite_level(root, level, &level_edits)?;
            if level == root_level {
                return self.finish_top(level, produced);
            }
            level_edits = parent_edits.into_iter().collect();
            if level_edits.is_empty() {
                return Ok(*root);
            }
            level += 1;
        }
    }

    /// Rewrite the affected nodes of one level. Returns the edits the
    /// level above must absorb (one removal per consumed node, one
    /// insertion per produced node) and, in order, every node produced.
    fn rewrite_level(
        &self,
        root: &NodeHash,
        level: u8,
        edits: &[(Vec<u8>, Option<Value>)],
    ) -> Result<LevelRewrite, MtreeError> {
        let mut parent: BTreeMap<Vec<u8>, Option<Value>> = BTreeMap::new();
        let mut produced: Vec<Entry> = Vec::new();
        let mut i = 0usize;
        while i < edits.len() {
            let mut cursor = LevelCursor::seek(self, *root, level, &edits[i].0)?;
            let mut buf = cursor.entries()?;
            parent.insert(cursor.first_key()?, None);
            loop {
                let limit = cursor.next_first_key()?;
                while i < edits.len()
                    && limit
                        .as_ref()
                        .map(|bound| edits[i].0 < *bound)
                        .unwrap_or(true)
                {
                    splice(&mut buf, &edits[i]);
                    i += 1;
                }
                let (sealed, remainder, sealed_by_clamp) = self.chunk(level, &buf)?;
                for entry in sealed {
                    parent.insert(entry.key.clone(), Some(entry.value.clone()));
                    produced.push(entry);
                }
                if remainder.is_empty() && !sealed_by_clamp {
                    break;
                }
                if cursor.advance()? {
                    parent.insert(cursor.first_key()?, None);
                    buf = remainder;
                    buf.extend(cursor.entries()?);
                    continue;
                }
                if !remainder.is_empty() {
                    let entry = self.seal(level, &remainder)?;
                    parent.insert(entry.key.clone(), Some(entry.value.clone()));
                    produced.push(entry);
                }
                break;
            }
        }
        Ok((parent, produced))
    }

    /// Seal every terminated prefix of `buf`, returning the sealed
    /// nodes, the unterminated tail, and whether the last seal was
    /// forced by the entry clamp rather than by a boundary key — in
    /// which case the caller may *not* assume it has re-synchronized
    /// with the untouched right neighbour.
    fn chunk(&self, level: u8, buf: &[Entry]) -> Result<Chunked, MtreeError> {
        let mut sealed = Vec::new();
        let mut start = 0usize;
        let mut by_clamp = false;
        for j in 0..buf.len() {
            let run = j + 1 - start;
            let boundary =
                run >= self.config.min_entries && self.config.is_boundary(&buf[j].key, level);
            if boundary || run >= self.config.max_entries {
                sealed.push(self.seal(level, &buf[start..=j])?);
                start = j + 1;
                by_clamp = !boundary;
            }
        }
        Ok((
            sealed,
            buf[start..].to_vec(),
            by_clamp && start == buf.len(),
        ))
    }

    /// Encode, hash, store, and return the interior entry that points
    /// at the result.
    fn seal(&self, level: u8, entries: &[Entry]) -> Result<Entry, MtreeError> {
        let first = entries
            .first()
            .ok_or(MtreeError::Malformed("sealed an empty node"))?;
        let mut agg = Agg::EMPTY;
        if level == 0 {
            let leaf_agg = self.leaf_agg();
            for entry in entries {
                if let Value::Leaf(value) = &entry.value {
                    agg.merge(&leaf_agg(&entry.key, value));
                }
            }
            // The key count is the tree's to state, not the
            // projection's: it is the one aggregate field that is a
            // structural fact.
            agg.keys = entries.len() as u64;
        } else {
            for entry in entries {
                if let Value::Child { agg: child, .. } = &entry.value {
                    agg.merge(child);
                }
            }
        }
        let key = first.key.clone();
        let bytes = node::encode(level, entries);
        let hash = self.config.hasher.hash(&bytes);
        self.store.put(hash, level, bytes)?;
        Ok(Entry::child(key, hash, agg))
    }

    /// Turn the rewritten top level into a root, growing or collapsing
    /// levels so the result is byte-identical to a bulk build.
    fn finish_top(&self, level: u8, produced: Vec<Entry>) -> Result<NodeHash, MtreeError> {
        if produced.is_empty() {
            return self.empty();
        }
        if produced.len() == 1 {
            let hash = produced[0]
                .child_hash()
                .ok_or(MtreeError::Malformed("produced a leaf entry above level 0"))?;
            return self.collapse(hash);
        }
        let mut entries = produced;
        let mut level = level + 1;
        loop {
            let (mut sealed, remainder, _) = self.chunk(level, &entries)?;
            if !remainder.is_empty() {
                sealed.push(self.seal(level, &remainder)?);
            }
            if sealed.len() == 1 {
                let hash = sealed[0]
                    .child_hash()
                    .ok_or(MtreeError::Malformed("produced a leaf entry above level 0"))?;
                return self.collapse(hash);
            }
            entries = sealed;
            level += 1;
        }
    }

    /// Shed interior nodes with a single child: a bulk build stops as
    /// soon as a level holds one node, so those are not canonical.
    fn collapse(&self, mut hash: NodeHash) -> Result<NodeHash, MtreeError> {
        loop {
            let buf = self.load(&hash)?;
            let node = NodeRef::new(&buf)?;
            if node.is_leaf() || node.count() != 1 {
                return Ok(hash);
            }
            hash = node.child(0)?.0;
        }
    }

    // -------------------------------------------------------------- diff

    /// Every key that differs between two roots, in key order.
    ///
    /// Equal subtrees are skipped by hash, so the cost tracks the
    /// difference and not the state: §14.6 measured a one-key diff of a
    /// 35.8M-key tree at 20 node reads, and the per-change constant
    /// *falls* as the change set grows.
    pub fn diff(
        &self,
        a: &NodeHash,
        b: &NodeHash,
    ) -> Result<Vec<(Vec<u8>, ChangeKind)>, MtreeError> {
        let mut out = Vec::new();
        self.diff_each(a, b, |key, kind| {
            out.push((key.to_vec(), kind));
            true
        })?;
        Ok(out)
    }

    /// [`Self::diff`] as a visit: `visit` sees each differing key in key
    /// order and returns `false` to stop early, which costs nothing past
    /// the keys seen (a bounded "did anything here change" check need
    /// not materialize a large diff). `Ok(true)` when the diff ran to its
    /// end, `Ok(false)` when `visit` stopped it.
    pub fn diff_each(
        &self,
        a: &NodeHash,
        b: &NodeHash,
        mut visit: impl FnMut(&[u8], ChangeKind) -> bool,
    ) -> Result<bool, MtreeError> {
        if a == b {
            return Ok(true);
        }
        let mut ca = self.cursor(a)?;
        let mut cb = self.cursor(b)?;
        loop {
            if try_skip(&mut ca, &mut cb)? {
                continue;
            }
            ca.materialize()?;
            cb.materialize()?;
            // Decide with both entries borrowed, act with neither, so
            // that comparing two equal keys costs no allocation.
            let step = {
                match (ca.peek()?, cb.peek()?) {
                    (None, None) => Step::Done,
                    (Some(_), None) => Step::TakeA(ChangeKind::Removed),
                    (None, Some(_)) => Step::TakeB(ChangeKind::Added),
                    (Some((ka, va)), Some((kb, vb))) => match ka.cmp(kb) {
                        std::cmp::Ordering::Less => Step::TakeA(ChangeKind::Removed),
                        std::cmp::Ordering::Greater => Step::TakeB(ChangeKind::Added),
                        std::cmp::Ordering::Equal => Step::TakeBoth(va != vb),
                    },
                }
            };
            let go_on = match step {
                Step::Done => return Ok(true),
                Step::TakeA(kind) => {
                    let go_on = match ca.peek()? {
                        Some((key, _)) => visit(key, kind),
                        None => true,
                    };
                    ca.next()?;
                    go_on
                }
                Step::TakeB(kind) => {
                    let go_on = match cb.peek()? {
                        Some((key, _)) => visit(key, kind),
                        None => true,
                    };
                    cb.next()?;
                    go_on
                }
                Step::TakeBoth(modified) => {
                    let go_on = match (modified, ca.peek()?) {
                        (true, Some((key, _))) => visit(key, ChangeKind::Modified),
                        _ => true,
                    };
                    ca.next()?;
                    cb.next()?;
                    go_on
                }
            };
            if !go_on {
                return Ok(false);
            }
        }
    }

    /// The delta from `base` to `other` as edits applicable to any
    /// root — the three-way merge's building block, and what S5 ships
    /// as a change journal (§12's `diff(any root, any root)`).
    pub fn delta(&self, base: &NodeHash, other: &NodeHash) -> Result<Vec<Edit>, MtreeError> {
        let mut out = Vec::new();
        for (key, kind) in self.diff(base, other)? {
            match kind {
                ChangeKind::Removed => out.push((key, None)),
                ChangeKind::Added | ChangeKind::Modified => {
                    let value = self.get(other, &key)?.ok_or(MtreeError::Malformed(
                        "a key the diff reported as present is absent",
                    ))?;
                    out.push((key, Some(value)));
                }
            }
        }
        Ok(out)
    }

    /// Three-way merge.
    ///
    /// [`Merged::Root`] when the two branches touched disjoint key
    /// sets — deterministic, and the same hash whichever way round the
    /// branches are passed, because the result is a function of the
    /// merged key set alone. [`Merged::Conflicts`] otherwise, carrying
    /// exactly the keys both branches touched.
    pub fn merge(&self, base: &NodeHash, a: &NodeHash, b: &NodeHash) -> Result<Merged, MtreeError> {
        let delta_a = self.delta(base, a)?;
        let delta_b = self.delta(base, b)?;
        let touched_by_a: BTreeSet<&Vec<u8>> = delta_a.iter().map(|(key, _)| key).collect();
        let conflicts: Vec<Vec<u8>> = delta_b
            .iter()
            .filter(|(key, _)| touched_by_a.contains(key))
            .map(|(key, _)| key.clone())
            .collect();
        if !conflicts.is_empty() {
            return Ok(Merged::Conflicts(conflicts));
        }
        Ok(Merged::Root(self.apply(a, &delta_b)?))
    }

    // ------------------------------------------------------------ walks

    /// Every node hash reachable from `roots` — the mark half of §P10.
    ///
    /// Terminates on shared subtrees, so marking N commits costs
    /// O(their differences).
    pub fn reachable(&self, roots: &[NodeHash]) -> Result<BTreeSet<NodeHash>, MtreeError> {
        let mut seen = BTreeSet::new();
        let mut stack: Vec<NodeHash> = roots.to_vec();
        while let Some(hash) = stack.pop() {
            if !seen.insert(hash) {
                continue;
            }
            let buf = self.load(&hash)?;
            let node = NodeRef::new(&buf)?;
            if node.is_leaf() {
                continue;
            }
            for i in 0..node.count() {
                let child = node.child(i)?.0;
                if !seen.contains(&child) {
                    stack.push(child);
                }
            }
        }
        Ok(seen)
    }

    /// Node census by level — §14.1's table, and how a test asserts
    /// that the entry clamp was honoured.
    pub fn census(&self, root: &NodeHash) -> Result<Census, MtreeError> {
        let mut census = Census::default();
        let mut stack = vec![*root];
        while let Some(hash) = stack.pop() {
            let buf = self.load(&hash)?;
            let node = NodeRef::new(&buf)?;
            let level = node.level() as usize;
            if census.levels.len() <= level {
                census.levels.resize(level + 1, LevelStats::default());
            }
            census.levels[level].nodes += 1;
            census.levels[level].entries += node.count() as u64;
            census.levels[level].bytes += buf.len() as u64;
            if node.is_leaf() {
                census.leaf_entries.push(node.count());
            } else {
                for i in 0..node.count() {
                    stack.push(node.child(i)?.0);
                }
            }
        }
        Ok(census)
    }
}

enum Step {
    Done,
    TakeA(ChangeKind),
    TakeB(ChangeKind),
    /// Same key on both sides; the flag says whether the values differ.
    TakeBoth(bool),
}

fn splice(buf: &mut Vec<Entry>, edit: &(Vec<u8>, Option<Value>)) {
    match buf.binary_search_by(|entry| entry.key.as_slice().cmp(&edit.0)) {
        Ok(i) => match &edit.1 {
            Some(value) => buf[i].value = value.clone(),
            None => {
                buf.remove(i);
            }
        },
        Err(i) => {
            if let Some(value) = &edit.1 {
                buf.insert(
                    i,
                    Entry {
                        key: edit.0.clone(),
                        value: value.clone(),
                    },
                );
            }
        }
    }
}

// ------------------------------------------------------------- builder

/// Bulk build: one pending run per level, sealed as boundary keys and
/// the entry clamp dictate. `emitted` per level is what tells `finish`
/// whether a level ever produced more than the run it is holding, which
/// is how the build knows it has reached the root.
struct Builder<'a, S> {
    tree: &'a Tree<S>,
    pending: Vec<Vec<Entry>>,
    emitted: Vec<u64>,
}

impl<'a, S: NodeStore> Builder<'a, S> {
    fn new(tree: &'a Tree<S>) -> Builder<'a, S> {
        Builder {
            tree,
            pending: vec![Vec::new()],
            emitted: vec![0],
        }
    }

    fn ensure(&mut self, level: usize) {
        while self.pending.len() <= level {
            self.pending.push(Vec::new());
            self.emitted.push(0);
        }
    }

    fn push(&mut self, level: usize, entry: Entry) -> Result<(), MtreeError> {
        self.ensure(level);
        let config = self.tree.config();
        let boundary = config.is_boundary(&entry.key, level as u8);
        self.pending[level].push(entry);
        let run = self.pending[level].len();
        if (boundary && run >= config.min_entries) || run >= config.max_entries {
            self.seal(level)?;
        }
        Ok(())
    }

    fn seal(&mut self, level: usize) -> Result<(), MtreeError> {
        let entries = std::mem::take(&mut self.pending[level]);
        if entries.is_empty() {
            return Ok(());
        }
        self.emitted[level] += 1;
        let up = self.tree.seal(level as u8, &entries)?;
        self.push(level + 1, up)
    }

    fn finish(mut self) -> Result<NodeHash, MtreeError> {
        let mut level = 0usize;
        loop {
            self.ensure(level);
            if self.emitted[level] == 0 {
                let entries = std::mem::take(&mut self.pending[level]);
                if entries.is_empty() {
                    return self.tree.empty();
                }
                let hash = self
                    .tree
                    .seal(level as u8, &entries)?
                    .child_hash()
                    .ok_or(MtreeError::Malformed("sealed entry is not a child"))?;
                return self.tree.collapse(hash);
            }
            self.seal(level)?;
            level += 1;
        }
    }
}

// ------------------------------------------------------------- cursors

#[derive(Clone)]
struct Frame {
    buf: Arc<[u8]>,
    idx: usize,
}

/// An ordered leaf cursor: `next` and `seek`, plus the ancestor frames
/// [`Tree::diff`] needs in order to skip identical subtrees.
pub struct Cursor<'a, S> {
    tree: &'a Tree<S>,
    root: NodeHash,
    stack: Vec<Frame>,
}

impl<'a, S: NodeStore> Cursor<'a, S> {
    fn open(tree: &'a Tree<S>, root: NodeHash, key: Option<&[u8]>) -> Result<Self, MtreeError> {
        let mut cursor = Cursor {
            tree,
            root,
            stack: Vec::new(),
        };
        cursor.descend(root, key)?;
        cursor.settle()?;
        Ok(cursor)
    }

    /// Reposition at `key`, or at the first key after it.
    pub fn seek(&mut self, key: &[u8]) -> Result<(), MtreeError> {
        self.stack.clear();
        let root = self.root;
        self.descend(root, Some(key))?;
        self.settle()
    }

    /// The entry under the cursor, or `None` at the end of the tree.
    ///
    /// Takes `&mut self` because reaching a leaf may have to read
    /// nodes: the cursor is deliberately left un-descended after an
    /// advance so that `diff` can skip whole subtrees. The borrow ends
    /// at the last use of the returned slices, so the usual
    /// `entry()` / `next()` loop needs no clone.
    pub fn entry(&mut self) -> Result<Option<CursorEntry<'_>>, MtreeError> {
        self.materialize()?;
        self.peek()
    }

    /// Advance one entry.
    ///
    /// Not `Iterator::next`: advancing can fail (a node may be missing
    /// or malformed) and the item borrows from the cursor's own frame
    /// stack, neither of which `Iterator` can express. The name is kept
    /// because it is the one every caller reaches for.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<(), MtreeError> {
        self.materialize()?;
        if let Some(frame) = self.stack.last_mut() {
            frame.idx += 1;
        }
        self.settle()
    }

    /// The entry under the cursor without descending: `None` both at
    /// the end of the tree and when the cursor is parked on an
    /// interior frame.
    fn peek(&self) -> Result<Option<CursorEntry<'_>>, MtreeError> {
        let Some(frame) = self.stack.last() else {
            return Ok(None);
        };
        let node = NodeRef::new(&frame.buf)?;
        if !node.is_leaf() || frame.idx >= node.count() {
            return Ok(None);
        }
        Ok(Some((node.key(frame.idx)?, node.leaf_value(frame.idx)?)))
    }

    fn descend(&mut self, mut hash: NodeHash, key: Option<&[u8]>) -> Result<(), MtreeError> {
        loop {
            let buf = self.tree.load(&hash)?;
            let next = {
                let node = NodeRef::new(&buf)?;
                let idx = if node.count() == 0 {
                    0
                } else {
                    match key {
                        None => 0,
                        Some(key) if node.is_leaf() => match node.search(key)? {
                            Ok(i) => i,
                            Err(i) => i,
                        },
                        Some(key) => node.descend(key)?,
                    }
                };
                let next = if node.is_leaf() || node.count() == 0 {
                    None
                } else {
                    Some(node.child(idx.min(node.count() - 1))?.0)
                };
                self.stack.push(Frame {
                    buf: Arc::clone(&buf),
                    idx,
                });
                next
            };
            match next {
                Some(child) => hash = child,
                None => return Ok(()),
            }
        }
    }

    /// After an advance the index may sit past the end of its node;
    /// walk up and over until it points at something. Deliberately
    /// does *not* descend — leaving the stack short is what lets
    /// `diff` step over an identical subtree without reading one of
    /// its nodes.
    fn settle(&mut self) -> Result<(), MtreeError> {
        loop {
            let Some(frame) = self.stack.last() else {
                return Ok(());
            };
            if frame.idx < NodeRef::new(&frame.buf)?.count() {
                return Ok(());
            }
            self.stack.pop();
            match self.stack.last_mut() {
                Some(parent) => parent.idx += 1,
                None => return Ok(()),
            }
        }
    }

    /// Descend from a lazily-left interior frame to the leaf.
    fn materialize(&mut self) -> Result<(), MtreeError> {
        loop {
            let Some(frame) = self.stack.last() else {
                return Ok(());
            };
            let node = NodeRef::new(&frame.buf)?;
            if frame.idx >= node.count() {
                self.settle()?;
                if self.stack.is_empty() {
                    return Ok(());
                }
                continue;
            }
            if node.is_leaf() {
                return Ok(());
            }
            let child = node.child(frame.idx)?.0;
            let buf = self.tree.load(&child)?;
            self.stack.push(Frame { buf, idx: 0 });
        }
    }

    fn depth(&self) -> usize {
        self.stack.len()
    }

    /// Advance past the whole subtree rooted at stack position `at`.
    fn skip_at(&mut self, at: usize) -> Result<(), MtreeError> {
        self.stack.truncate(at + 1);
        if let Some(frame) = self.stack.last_mut() {
            frame.idx += 1;
        }
        self.settle()
    }
}

/// If both cursors sit at the start of an identical subtree, step both
/// over it — by comparing the *child hashes* recorded in interior nodes
/// the cursors already hold, so an identical subtree costs zero reads.
/// This is what makes `diff` O(difference) rather than O(state).
fn try_skip<S: NodeStore>(
    a: &mut Cursor<'_, S>,
    b: &mut Cursor<'_, S>,
) -> Result<bool, MtreeError> {
    if a.stack.is_empty() || b.stack.is_empty() {
        return Ok(false);
    }
    let depth = a.depth().min(b.depth());
    for level in 0..depth {
        // Only sound at the *start* of a subtree: if either cursor is
        // already part-way into one, the subtrees are equal but the
        // positions are not.
        if a.stack[level + 1..].iter().any(|f| f.idx != 0)
            || b.stack[level + 1..].iter().any(|f| f.idx != 0)
        {
            continue;
        }
        let (na, nb) = (
            NodeRef::new(&a.stack[level].buf)?,
            NodeRef::new(&b.stack[level].buf)?,
        );
        if na.is_leaf() || nb.is_leaf() {
            continue;
        }
        if a.stack[level].idx >= na.count() || b.stack[level].idx >= nb.count() {
            continue;
        }
        if na.child(a.stack[level].idx)?.0 != nb.child(b.stack[level].idx)?.0 {
            continue;
        }
        a.skip_at(level)?;
        b.skip_at(level)?;
        return Ok(true);
    }
    Ok(false)
}

/// A cursor pinned to one level: `apply` walks the node sequence of a
/// level, not of the leaves.
struct LevelCursor<'a, S> {
    tree: &'a Tree<S>,
    stack: Vec<Frame>,
    level: u8,
}

impl<'a, S: NodeStore> LevelCursor<'a, S> {
    fn seek(
        tree: &'a Tree<S>,
        root: NodeHash,
        level: u8,
        key: &[u8],
    ) -> Result<LevelCursor<'a, S>, MtreeError> {
        let mut stack = Vec::new();
        let mut hash = root;
        loop {
            let buf = tree.load(&hash)?;
            let next = {
                let node = NodeRef::new(&buf)?;
                let at_target = node.level() == level;
                let idx = if at_target || node.count() == 0 {
                    0
                } else {
                    node.descend(key)?
                };
                let next = if at_target || node.count() == 0 {
                    None
                } else {
                    Some(node.child(idx)?.0)
                };
                stack.push(Frame {
                    buf: Arc::clone(&buf),
                    idx,
                });
                next
            };
            match next {
                Some(child) => hash = child,
                None => break,
            }
        }
        Ok(LevelCursor { tree, stack, level })
    }

    fn current(&self) -> Result<NodeRef<'_>, MtreeError> {
        let frame = self
            .stack
            .last()
            .ok_or(MtreeError::Malformed("level cursor has no frame"))?;
        NodeRef::new(&frame.buf)
    }

    fn entries(&self) -> Result<Vec<Entry>, MtreeError> {
        self.current()?.entries()
    }

    fn first_key(&self) -> Result<Vec<u8>, MtreeError> {
        let node = self.current()?;
        if node.count() == 0 {
            Ok(Vec::new())
        } else {
            Ok(node.key(0)?.to_vec())
        }
    }

    /// The first key of the next node at this level — the exclusive
    /// upper bound of the current node's key range, `None` at the end
    /// of the level. First keys propagate upward unchanged, so an
    /// ancestor's next entry key *is* that bound and finding it costs
    /// no node reads.
    fn next_first_key(&self) -> Result<Option<Vec<u8>>, MtreeError> {
        for i in (0..self.stack.len().saturating_sub(1)).rev() {
            let node = NodeRef::new(&self.stack[i].buf)?;
            if self.stack[i].idx + 1 < node.count() {
                return Ok(Some(node.key(self.stack[i].idx + 1)?.to_vec()));
            }
        }
        Ok(None)
    }

    /// Move to the next node at this level; `false` at the end.
    fn advance(&mut self) -> Result<bool, MtreeError> {
        loop {
            if self.stack.len() < 2 {
                return Ok(false);
            }
            let last = self.stack.len() - 1;
            self.stack.pop();
            let parent = &mut self.stack[last - 1];
            parent.idx += 1;
            let node = NodeRef::new(&parent.buf)?;
            if parent.idx < node.count() {
                let mut hash = node.child(parent.idx)?.0;
                loop {
                    let buf = self.tree.load(&hash)?;
                    let child = {
                        let node = NodeRef::new(&buf)?;
                        if node.level() == self.level {
                            None
                        } else {
                            Some(node.child(0)?.0)
                        }
                    };
                    self.stack.push(Frame { buf, idx: 0 });
                    match child {
                        Some(child) => hash = child,
                        None => return Ok(true),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryNodeStore;

    fn key(i: u64) -> Vec<u8> {
        let mut out = vec![0x01];
        out.extend_from_slice(&i.to_be_bytes());
        out
    }

    fn pair(i: u64) -> (Vec<u8>, Vec<u8>) {
        (key(i), format!("value-of-{i}").into_bytes())
    }

    fn tree() -> Tree<MemoryNodeStore> {
        Tree::new(MemoryNodeStore::new())
    }

    #[test]
    fn build_get_and_scan() {
        let tree = tree();
        let root = tree.build((0..20_000).map(pair)).unwrap();
        for i in [0u64, 1, 999, 10_000, 19_999] {
            assert_eq!(tree.get(&root, &key(i)).unwrap(), Some(pair(i).1));
        }
        assert_eq!(tree.get(&root, &key(20_000)).unwrap(), None);
        assert_eq!(tree.aggregate(&root).unwrap().keys, 20_000);

        let mut cursor = tree.cursor_at(&root, &key(1_000)).unwrap();
        for i in 1_000..1_100 {
            let (k, v) = cursor.entry().unwrap().unwrap();
            assert_eq!(k, key(i).as_slice());
            assert_eq!(v, pair(i).1.as_slice());
            cursor.next().unwrap();
        }
        // A seek to an absent key lands on the next one.
        cursor.seek(&[0x01]).unwrap();
        assert_eq!(cursor.entry().unwrap().unwrap().0, key(0).as_slice());
    }

    #[test]
    fn an_empty_tree_is_a_real_node() {
        let tree = tree();
        let root = tree.empty().unwrap();
        assert_eq!(tree.build(Vec::new()).unwrap(), root);
        assert_eq!(tree.get(&root, b"anything").unwrap(), None);
        assert!(tree.cursor(&root).unwrap().entry().unwrap().is_none());
        assert_eq!(tree.aggregate(&root).unwrap(), Agg::EMPTY);
        assert!(tree.diff(&root, &root).unwrap().is_empty());
    }

    #[test]
    fn unsorted_input_is_refused_rather_than_reshaping_the_tree() {
        let tree = tree();
        assert!(matches!(
            tree.build(vec![pair(2), pair(1)]),
            Err(MtreeError::Unsorted)
        ));
        assert!(matches!(
            tree.build(vec![pair(1), pair(1)]),
            Err(MtreeError::Unsorted)
        ));
        let root = tree.empty().unwrap();
        assert!(matches!(
            tree.apply(&root, &[(key(2), None), (key(1), None)]),
            Err(MtreeError::Unsorted)
        ));
    }

    #[test]
    fn range_stops_at_the_prefix() {
        let tree = tree();
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500)
            .map(|i| (key(i), b"in".to_vec()))
            .chain((0..50).map(|i| {
                let mut k = vec![0x02];
                k.extend_from_slice(&(i as u64).to_be_bytes());
                (k, b"out".to_vec())
            }))
            .collect();
        entries.sort();
        let root = tree.build(entries).unwrap();
        let page = tree.range(&root, &[0x01], &[0x01], 1_000).unwrap();
        assert_eq!(page.len(), 500);
        assert!(page.iter().all(|(_, v)| v == b"in".as_slice()));
        assert_eq!(tree.range(&root, &[0x01], &[0x01], 7).unwrap().len(), 7);
        // Resume from the previous page's last key.
        let resumed = tree.range(&root, &page[499].0, &[0x01], 10).unwrap();
        assert_eq!(resumed.len(), 1);
    }

    #[test]
    fn apply_of_nothing_writes_nothing() {
        let tree = tree();
        let root = tree.build((0..1_000).map(pair)).unwrap();
        let writes = tree.store().writes();
        assert_eq!(tree.apply(&root, &[]).unwrap(), root);
        assert_eq!(tree.store().writes(), writes);
    }

    #[test]
    fn a_value_change_does_not_reshape_the_tree() {
        let tree = tree();
        let root = tree.build((0..20_000).map(pair)).unwrap();
        let before = tree.census(&root).unwrap();
        let edits: Vec<Edit> = (0..2_000)
            .map(|i| (key(i * 7), Some(vec![b'x'; 200])))
            .collect();
        let after_root = tree.apply(&root, &edits).unwrap();
        let after = tree.census(&after_root).unwrap();
        // Same partition of the key space: the boundary function never
        // looked at a value.
        assert_eq!(before.leaf_entries.len(), after.leaf_entries.len());
        let mut a = before.leaf_entries.clone();
        let mut b = after.leaf_entries.clone();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b);
    }

    #[test]
    fn reachable_terminates_on_shared_subtrees() {
        let tree = tree();
        let root = tree.build((0..20_000).map(pair)).unwrap();
        let all = tree.reachable(&[root]).unwrap();
        let next = tree
            .apply(&root, &[(key(5), Some(b"changed".to_vec()))])
            .unwrap();
        let both = tree.reachable(&[root, next]).unwrap();
        // One root path is rewritten; the rest is shared.
        let added = both.len() - all.len();
        assert!(added <= tree.census(&root).unwrap().levels.len(), "{added}");
        assert!(both.is_superset(&all));
    }

    #[test]
    fn a_missing_node_surfaces_as_an_error() {
        let tree = tree();
        let root = tree.build((0..1_000).map(pair)).unwrap();
        assert!(matches!(
            tree.get(&NodeHash([0xaa; 32]), &key(1)),
            Err(MtreeError::MissingNode(_))
        ));
        assert!(tree.get(&root, &key(1)).is_ok());
    }
}
