//! The prolly tree itself: bulk build, point read, ordered scan,
//! incremental commit (`apply`), structural diff, and three-way merge.
//!
//! `apply` is the measurement that matters for §0.2, so it is written to
//! do the minimum the structure allows rather than the minimum that is
//! easy. A batch of sorted mutations is grouped into *runs*: for each run
//! the affected node is loaded, the mutations inside its key range are
//! spliced in, and the result is re-chunked. Because the boundary
//! function is context-free (`node::is_boundary`), a re-chunked run that
//! ends on a boundary key re-synchronizes with the untouched remainder of
//! the level immediately — so a scattered batch pays for the leaves it
//! actually hits and nothing in between. Only when an edit *deletes* a
//! boundary key does the run absorb its right neighbour, and that
//! cascade is handled by the same loop.
//!
//! Each level is rewritten in turn, the level above receiving one
//! deletion per consumed node and one insertion per produced node. That
//! makes the whole commit one uniform algorithm from leaves to root.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::node::{self, Agg, Ent, Entry, Hash, NodeRef};
use crate::store::Store;

pub struct Tree<'a> {
    pub store: &'a Store,
}

/// A key mutation: `Some` upserts, `None` deletes.
pub type Mut = (Vec<u8>, Option<Vec<u8>>);

#[derive(Clone, Debug, Default)]
pub struct LevelStats {
    pub nodes: u64,
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Change {
    Added,
    Removed,
    Modified,
}

impl<'a> Tree<'a> {
    pub fn new(store: &'a Store) -> Tree<'a> {
        Tree { store }
    }

    pub fn empty(&self) -> Hash {
        self.store.put(0, node::encode(0, &[]))
    }

    // ---------------------------------------------------------------- build

    /// Bulk build from an already-sorted, deduplicated entry stream.
    pub fn build_sorted<I: Iterator<Item = (Vec<u8>, Vec<u8>)>>(&self, entries: I) -> Hash {
        let mut b = Builder::new(self.store);
        for (k, v) in entries {
            b.push(0, (k, Ent::Leaf(v)));
        }
        b.finish()
    }

    // ----------------------------------------------------------------- read

    pub fn get(&self, root: &Hash, key: &[u8]) -> Option<Vec<u8>> {
        let mut buf = self.store.get(root);
        loop {
            let n = NodeRef::new(&buf);
            if n.level == 0 {
                return n.search(key).ok().map(|i| n.leaf_val(i).to_vec());
            }
            if n.count == 0 {
                return None;
            }
            let h = n.child(n.descend(key)).0;
            buf = self.store.get(&h);
        }
    }

    /// Aggregates for the whole tree — the §P7 `statfs` path.
    pub fn agg(&self, root: &Hash) -> Agg {
        let buf = self.store.get(root);
        NodeRef::new(&buf).agg()
    }

    pub fn cursor(&self, root: &Hash, from: &[u8]) -> Cursor<'a> {
        Cursor::seek(self.store, *root, from)
    }

    /// Number of keys with `prefix`, and the bytes scanned — a readdir /
    /// listxattr range scan.
    pub fn scan_prefix(&self, root: &Hash, prefix: &[u8], limit: usize) -> (usize, u64) {
        let mut c = self.cursor(root, prefix);
        let (mut n, mut bytes) = (0usize, 0u64);
        while let Some((k, v)) = c.entry() {
            if !k.starts_with(prefix) || n >= limit {
                break;
            }
            bytes += (k.len() + v.len()) as u64;
            n += 1;
            c.next();
        }
        (n, bytes)
    }

    pub fn collect_prefix(&self, root: &Hash, prefix: &[u8], limit: usize) -> Vec<Vec<u8>> {
        self.collect_from(root, prefix, prefix, limit)
    }

    /// One `readdir` page: start at `from` (the previous page's last key,
    /// which *is* the cursor) and stop at the end of `prefix`.
    pub fn collect_from(
        &self,
        root: &Hash,
        from: &[u8],
        prefix: &[u8],
        limit: usize,
    ) -> Vec<Vec<u8>> {
        let mut c = self.cursor(root, from);
        let mut out = Vec::new();
        while let Some((k, _)) = c.entry() {
            if !k.starts_with(prefix) || out.len() >= limit {
                break;
            }
            out.push(k.to_vec());
            c.next();
        }
        out
    }

    /// A range scan that keeps the values. READDIRPLUS needs them: with
    /// the §P6 attr copy that is the whole operation, without it the
    /// values are the `(ino, kind)` pairs the second pass looks up.
    pub fn collect_entries_prefix(
        &self,
        root: &Hash,
        prefix: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut c = self.cursor(root, prefix);
        let mut out = Vec::new();
        while let Some((k, v)) = c.entry() {
            if !k.starts_with(prefix) || out.len() >= limit {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
            c.next();
        }
        out
    }

    // ---------------------------------------------------------------- write

    /// Commit `muts` (sorted, deduplicated) onto `root`, returning the new
    /// root. Nodes written are visible in `store.counters`.
    pub fn apply(&self, root: &Hash, muts: &[Mut]) -> Hash {
        if muts.is_empty() {
            return *root;
        }
        let root_level = NodeRef::new(&self.store.get(root)).level;
        let mut level_muts: Vec<(Vec<u8>, Option<Ent>)> = muts
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().map(Ent::Leaf)))
            .collect();
        let mut level = 0u8;
        loop {
            let (parent, produced) = self.rewrite_level(root, level, &level_muts);
            if level == root_level {
                return self.finish_top(level, produced);
            }
            level_muts = parent.into_iter().collect();
            if level_muts.is_empty() {
                return *root;
            }
            level += 1;
        }
    }

    /// Rewrite the affected nodes of one level. Returns the mutations the
    /// level above must absorb and, in order, every node produced.
    fn rewrite_level(
        &self,
        root: &Hash,
        level: u8,
        muts: &[(Vec<u8>, Option<Ent>)],
    ) -> (BTreeMap<Vec<u8>, Option<Ent>>, Vec<Entry>) {
        let mut parent: BTreeMap<Vec<u8>, Option<Ent>> = BTreeMap::new();
        let mut produced: Vec<Entry> = Vec::new();
        let mut i = 0usize;
        while i < muts.len() {
            let mut cur = LevelCursor::seek(self.store, *root, level, &muts[i].0);
            let mut buf = cur.entries();
            parent.insert(cur.first_key(), None);
            loop {
                let limit = cur.peek_next_first_key();
                while i < muts.len() && limit.as_ref().is_none_or(|l| muts[i].0 < *l) {
                    splice(&mut buf, &muts[i]);
                    i += 1;
                }
                let (sealed, remainder, clamped) = self.chunk(level, &buf);
                for e in sealed {
                    parent.insert(e.0.clone(), Some(e.1.clone()));
                    produced.push(e);
                }
                // A run re-synchronizes with the untouched right side only
                // when it ended on a boundary key: that is the point where
                // the old and new chunkings provably agree again.
                if remainder.is_empty() && !clamped {
                    break;
                }
                if cur.advance() {
                    parent.insert(cur.first_key(), None);
                    buf = remainder;
                    buf.extend(cur.entries());
                    continue;
                }
                if !remainder.is_empty() {
                    let e = self.seal(level, &remainder);
                    parent.insert(e.0.clone(), Some(e.1.clone()));
                    produced.push(e);
                }
                break;
            }
        }
        (parent, produced)
    }

    /// Seal every terminated prefix of `buf`, returning the sealed nodes,
    /// the unterminated tail, and whether the last seal was forced by the
    /// entry clamp rather than by a boundary key (in which case the caller
    /// may not assume it has re-synchronized).
    fn chunk(&self, level: u8, buf: &[Entry]) -> (Vec<Entry>, Vec<Entry>, bool) {
        let mut out = Vec::new();
        let mut start = 0usize;
        let max = node::max_entries();
        let mut clamped = false;
        for j in 0..buf.len() {
            let boundary = node::is_boundary(&buf[j].0, level);
            if boundary || (max > 0 && j + 1 - start >= max) {
                out.push(self.seal(level, &buf[start..=j]));
                start = j + 1;
                clamped = !boundary;
            }
        }
        (out, buf[start..].to_vec(), clamped && start == buf.len())
    }

    fn seal(&self, level: u8, ents: &[Entry]) -> Entry {
        let mut agg = Agg::default();
        if level == 0 {
            for (k, e) in ents {
                if let Ent::Leaf(v) = e {
                    agg.merge(&crate::keys::leaf_agg(k, v));
                }
            }
            agg.keys = ents.len() as u64;
        } else {
            for (_, e) in ents {
                if let Ent::Child(_, a) = e {
                    agg.merge(a);
                }
            }
        }
        let bytes = node::encode(level, ents);
        let h = self.store.put(level, bytes);
        (ents[0].0.clone(), Ent::Child(h, agg))
    }

    /// Turn the rewritten top level into a root, growing or collapsing
    /// levels so the result is byte-identical to a bulk build.
    fn finish_top(&self, level: u8, produced: Vec<Entry>) -> Hash {
        if produced.is_empty() {
            return self.empty();
        }
        if produced.len() == 1 {
            let Ent::Child(h, _) = produced[0].1 else {
                unreachable!()
            };
            return self.collapse(h);
        }
        let mut entries = produced;
        let mut l = level + 1;
        loop {
            let (mut sealed, remainder, _) = self.chunk(l, &entries);
            if !remainder.is_empty() {
                sealed.push(self.seal(l, &remainder));
            }
            if sealed.len() == 1 {
                let Ent::Child(h, _) = sealed[0].1 else {
                    unreachable!()
                };
                return self.collapse(h);
            }
            entries = sealed;
            l += 1;
        }
    }

    /// A bulk build stops as soon as a level holds one node, so a root
    /// with a single child is not canonical.
    fn collapse(&self, mut h: Hash) -> Hash {
        loop {
            let buf = self.store.get(&h);
            let n = NodeRef::new(&buf);
            if n.level == 0 || n.count != 1 {
                return h;
            }
            h = n.child(0).0;
        }
    }

    // ----------------------------------------------------------------- diff

    /// Every key that differs between two roots, with the number of nodes
    /// the walk had to read. Equal subtrees are skipped by hash, so the
    /// cost is O(difference) rather than O(state).
    pub fn diff(&self, a: &Hash, b: &Hash) -> (Vec<(Vec<u8>, Change)>, u64) {
        let mut out = Vec::new();
        if a == b {
            return (out, 0);
        }
        let before = self
            .store
            .counters
            .gets
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut ca = Cursor::first(self.store, *a);
        let mut cb = Cursor::first(self.store, *b);
        loop {
            if try_skip(&mut ca, &mut cb) {
                continue;
            }
            match (ca.entry(), cb.entry()) {
                (None, None) => break,
                (Some((ka, _)), None) => {
                    out.push((ka.to_vec(), Change::Removed));
                    ca.next();
                }
                (None, Some((kb, _))) => {
                    out.push((kb.to_vec(), Change::Added));
                    cb.next();
                }
                (Some((ka, va)), Some((kb, vb))) => match ka.cmp(kb) {
                    std::cmp::Ordering::Less => {
                        out.push((ka.to_vec(), Change::Removed));
                        ca.next();
                    }
                    std::cmp::Ordering::Greater => {
                        out.push((kb.to_vec(), Change::Added));
                        cb.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if va != vb {
                            out.push((ka.to_vec(), Change::Modified));
                        }
                        ca.next();
                        cb.next();
                    }
                },
            }
        }
        let after = self
            .store
            .counters
            .gets
            .load(std::sync::atomic::Ordering::Relaxed);
        (out, after - before)
    }

    /// The delta from `base` to `other`, as mutations that can be applied
    /// to any root (the three-way merge's building block).
    pub fn delta(&self, base: &Hash, other: &Hash) -> Vec<Mut> {
        let (changes, _) = self.diff(base, other);
        changes
            .into_iter()
            .map(|(k, kind)| match kind {
                Change::Removed => (k, None),
                _ => {
                    let v = self.get(other, &k).expect("changed key must exist");
                    (k, Some(v))
                }
            })
            .collect()
    }

    /// Three-way merge. `Ok(root)` when the two branches touched disjoint
    /// keys; `Err(conflicts)` is the exact overlapping key set.
    pub fn merge(&self, base: &Hash, a: &Hash, b: &Hash) -> Result<Hash, Vec<Vec<u8>>> {
        let da = self.delta(base, a);
        let db = self.delta(base, b);
        let sa: HashSet<&Vec<u8>> = da.iter().map(|(k, _)| k).collect();
        let conflicts: Vec<Vec<u8>> = db
            .iter()
            .filter(|(k, _)| sa.contains(k))
            .map(|(k, _)| k.clone())
            .collect();
        if !conflicts.is_empty() {
            return Err(conflicts);
        }
        Ok(self.apply(a, &db))
    }

    /// Every node hash reachable from `roots` — the mark half of §P10.
    pub fn reachable(&self, roots: &[Hash]) -> HashSet<Hash> {
        self.reachable_par(roots, 1)
    }

    /// Parallel mark over immutable nodes: each frontier level is walked
    /// with `threads` workers, then the next frontier is deduped. No
    /// latch on the hot path beyond the store's own read locks.
    pub fn reachable_par(&self, roots: &[Hash], threads: usize) -> HashSet<Hash> {
        use rayon::prelude::*;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads.max(1))
            .build()
            .expect("rayon pool");
        pool.install(|| {
            let mut seen: HashSet<Hash> = HashSet::new();
            let mut frontier: Vec<Hash> = roots.to_vec();
            while !frontier.is_empty() {
                for h in &frontier {
                    seen.insert(*h);
                }
                let children: Vec<Hash> = frontier
                    .par_iter()
                    .flat_map_iter(|h| {
                        let buf = self.store.get(h);
                        let n = NodeRef::new(&buf);
                        if n.level == 0 {
                            Vec::new()
                        } else {
                            (0..n.count).map(|i| n.child(i).0).collect()
                        }
                    })
                    .collect();
                frontier.clear();
                for h in children {
                    if !seen.contains(&h) {
                        frontier.push(h);
                    }
                }
                frontier.sort_unstable();
                frontier.dedup();
            }
            seen
        })
    }

    /// Node census by level: `(nodes, entries, encoded bytes)`.
    pub fn level_stats(&self, root: &Hash) -> (Vec<LevelStats>, Vec<usize>) {
        let mut levels: Vec<LevelStats> = Vec::new();
        let mut leaf_entry_counts: Vec<usize> = Vec::new();
        let mut stack = vec![*root];
        while let Some(h) = stack.pop() {
            let buf = self.store.get(&h);
            let n = NodeRef::new(&buf);
            let l = n.level as usize;
            if levels.len() <= l {
                levels.resize(l + 1, LevelStats::default());
            }
            levels[l].nodes += 1;
            levels[l].entries += n.count as u64;
            levels[l].bytes += buf.len() as u64;
            if n.level == 0 {
                leaf_entry_counts.push(n.count);
            } else {
                for i in 0..n.count {
                    stack.push(n.child(i).0);
                }
            }
        }
        (levels, leaf_entry_counts)
    }
}

fn splice(buf: &mut Vec<Entry>, m: &(Vec<u8>, Option<Ent>)) {
    match buf.binary_search_by(|(k, _)| k.as_slice().cmp(&m.0)) {
        Ok(i) => match &m.1 {
            Some(e) => buf[i].1 = e.clone(),
            None => {
                buf.remove(i);
            }
        },
        Err(i) => {
            if let Some(e) = &m.1 {
                buf.insert(i, (m.0.clone(), e.clone()));
            }
        }
    }
}

// -------------------------------------------------------------- builder

struct Builder<'a> {
    store: &'a Store,
    pending: Vec<Vec<Entry>>,
    emitted: Vec<u64>,
}

impl<'a> Builder<'a> {
    fn new(store: &'a Store) -> Builder<'a> {
        Builder {
            store,
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

    fn push(&mut self, level: usize, e: Entry) {
        self.ensure(level);
        let boundary = node::is_boundary(&e.0, level as u8);
        self.pending[level].push(e);
        let max = node::max_entries();
        if boundary || (max > 0 && self.pending[level].len() >= max) {
            self.seal(level);
        }
    }

    fn seal(&mut self, level: usize) {
        let ents = std::mem::take(&mut self.pending[level]);
        if ents.is_empty() {
            return;
        }
        self.emitted[level] += 1;
        let up = Tree::new(self.store).seal(level as u8, &ents);
        self.push(level + 1, up);
    }

    fn finish(mut self) -> Hash {
        let tree = Tree::new(self.store);
        let mut level = 0usize;
        loop {
            self.ensure(level);
            if self.emitted[level] == 0 {
                let ents = std::mem::take(&mut self.pending[level]);
                if ents.is_empty() {
                    return tree.empty();
                }
                let Ent::Child(h, _) = tree.seal(level as u8, &ents).1 else {
                    unreachable!()
                };
                return tree.collapse(h);
            }
            self.seal(level);
            level += 1;
        }
    }
}

// --------------------------------------------------------------- cursors

#[derive(Clone)]
struct Frame {
    buf: Arc<Vec<u8>>,
    idx: usize,
}

/// A leaf-level cursor: ordered iteration plus the ancestor hashes a diff
/// needs in order to skip identical subtrees.
#[derive(Clone)]
pub struct Cursor<'a> {
    store: &'a Store,
    stack: Vec<Frame>,
}

impl<'a> Cursor<'a> {
    pub fn first(store: &'a Store, root: Hash) -> Cursor<'a> {
        let mut c = Cursor {
            store,
            stack: Vec::new(),
        };
        c.push_descend(root, None);
        c.settle();
        c
    }

    pub fn seek(store: &'a Store, root: Hash, key: &[u8]) -> Cursor<'a> {
        let mut c = Cursor {
            store,
            stack: Vec::new(),
        };
        c.push_descend(root, Some(key));
        c.settle();
        c
    }

    fn push_descend(&mut self, mut h: Hash, key: Option<&[u8]>) {
        loop {
            let buf = self.store.get(&h);
            let n = NodeRef::new(&buf);
            let idx = match key {
                _ if n.count == 0 => 0,
                None => 0,
                Some(k) if n.level == 0 => match n.search(k) {
                    Ok(i) => i,
                    Err(i) => i,
                },
                Some(k) => n.descend(k),
            };
            let level = n.level;
            let count = n.count;
            self.stack.push(Frame { buf, idx });
            if level == 0 || count == 0 {
                return;
            }
            let f = self.stack.last().unwrap();
            h = NodeRef::new(&f.buf).child(f.idx.min(count - 1)).0;
        }
    }

    /// After an advance the index may sit past the end of its node; walk
    /// up and over until it points at something. Deliberately does *not*
    /// descend: leaving the stack short is what lets `diff` step over an
    /// identical subtree without reading a single one of its nodes.
    fn settle(&mut self) {
        loop {
            let Some(f) = self.stack.last() else { return };
            if f.idx < NodeRef::new(&f.buf).count {
                return;
            }
            self.stack.pop();
            match self.stack.last_mut() {
                Some(p) => p.idx += 1,
                None => return,
            }
        }
    }

    /// Descend from a lazily-left interior frame to the leaf.
    fn materialize(&mut self) {
        loop {
            let Some(f) = self.stack.last() else { return };
            let n = NodeRef::new(&f.buf);
            if f.idx >= n.count {
                self.settle();
                if self.stack.is_empty() {
                    return;
                }
                continue;
            }
            if n.level == 0 {
                return;
            }
            let h = n.child(f.idx).0;
            let buf = self.store.get(&h);
            self.stack.push(Frame { buf, idx: 0 });
        }
    }

    /// The entry under the cursor. The slices stay valid until the next
    /// cursor mutation (the frame holding them is alive until then).
    pub fn entry<'b>(&mut self) -> Option<(&'b [u8], &'b [u8])> {
        self.materialize();
        let f = self.stack.last()?;
        let n = NodeRef::new(&f.buf);
        if n.level != 0 || f.idx >= n.count {
            return None;
        }
        let (k, v) = (n.key(f.idx), n.leaf_val(f.idx));
        Some(unsafe {
            (
                std::slice::from_raw_parts(k.as_ptr(), k.len()),
                std::slice::from_raw_parts(v.as_ptr(), v.len()),
            )
        })
    }

    pub fn next(&mut self) {
        self.materialize();
        if let Some(f) = self.stack.last_mut() {
            f.idx += 1;
        }
        self.settle();
    }

    fn depth(&self) -> usize {
        self.stack.len()
    }

    /// Advance past the whole subtree rooted at stack position `l`.
    fn skip_at(&mut self, l: usize) {
        self.stack.truncate(l + 1);
        if let Some(f) = self.stack.last_mut() {
            f.idx += 1;
        }
        self.settle();
    }
}

/// If both cursors sit at the start of an identical subtree, step both
/// over it — by comparing the *child hashes* recorded in the interior
/// nodes already in hand, so an identical subtree costs zero reads. This
/// is what makes `diff` O(difference) rather than O(state).
fn try_skip(a: &mut Cursor<'_>, b: &mut Cursor<'_>) -> bool {
    if a.stack.is_empty() || b.stack.is_empty() {
        return false;
    }
    let n = a.depth().min(b.depth());
    for l in 0..n {
        if a.stack[l + 1..].iter().any(|f| f.idx != 0)
            || b.stack[l + 1..].iter().any(|f| f.idx != 0)
        {
            continue;
        }
        let (na, nb) = (NodeRef::new(&a.stack[l].buf), NodeRef::new(&b.stack[l].buf));
        if na.level == 0 || nb.level == 0 {
            continue;
        }
        if a.stack[l].idx >= na.count || b.stack[l].idx >= nb.count {
            continue;
        }
        if na.child(a.stack[l].idx).0 != nb.child(b.stack[l].idx).0 {
            continue;
        }
        a.skip_at(l);
        b.skip_at(l);
        return true;
    }
    false
}

/// A cursor pinned to one level: `apply` walks the node sequence of a
/// level, not of the leaves.
struct LevelCursor<'a> {
    store: &'a Store,
    stack: Vec<Frame>,
    level: u8,
}

impl<'a> LevelCursor<'a> {
    fn seek(store: &'a Store, root: Hash, level: u8, key: &[u8]) -> LevelCursor<'a> {
        let mut stack = Vec::new();
        let mut h = root;
        loop {
            let buf = store.get(&h);
            let n = NodeRef::new(&buf);
            let at_target = n.level == level;
            let idx = if at_target || n.count == 0 {
                0
            } else {
                n.descend(key)
            };
            let next = if at_target || n.count == 0 {
                None
            } else {
                Some(n.child(idx).0)
            };
            stack.push(Frame { buf, idx });
            match next {
                Some(c) => h = c,
                None => break,
            }
        }
        LevelCursor {
            store,
            stack,
            level,
        }
    }

    fn cur(&self) -> NodeRef<'_> {
        NodeRef::new(&self.stack.last().unwrap().buf)
    }

    fn entries(&self) -> Vec<Entry> {
        self.cur().entries()
    }

    fn first_key(&self) -> Vec<u8> {
        let n = self.cur();
        if n.count == 0 {
            Vec::new()
        } else {
            n.key(0).to_vec()
        }
    }

    /// The first key of the next node at this level — the exclusive upper
    /// bound of the current node's key range. `None` at the end of the
    /// level. First keys propagate upward unchanged, so an ancestor's
    /// next entry key *is* that bound.
    fn peek_next_first_key(&self) -> Option<Vec<u8>> {
        for i in (0..self.stack.len().saturating_sub(1)).rev() {
            let n = NodeRef::new(&self.stack[i].buf);
            if self.stack[i].idx + 1 < n.count {
                return Some(n.key(self.stack[i].idx + 1).to_vec());
            }
        }
        None
    }

    fn advance(&mut self) -> bool {
        loop {
            if self.stack.len() < 2 {
                return false;
            }
            let last = self.stack.len() - 1;
            self.stack.pop();
            let p = &mut self.stack[last - 1];
            p.idx += 1;
            let n = NodeRef::new(&p.buf);
            if p.idx < n.count {
                let mut h = n.child(p.idx).0;
                loop {
                    let buf = self.store.get(&h);
                    let nn = NodeRef::new(&buf);
                    let at = nn.level == self.level;
                    let child = if at { None } else { Some(nn.child(0).0) };
                    self.stack.push(Frame { buf, idx: 0 });
                    match child {
                        Some(c) => h = c,
                        None => return true,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::*;

    /// The chunking parameters are global, so the tests that depend on
    /// them take turns.
    fn lock() -> std::sync::MutexGuard<'static, ()> {
        static L: std::sync::Mutex<()> = std::sync::Mutex::new(());
        L.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn kv(i: u64) -> (Vec<u8>, Vec<u8>) {
        (
            crate::keys::inode_key(i),
            format!("value-of-{i}").into_bytes(),
        )
    }

    fn build(store: &Store, n: u64) -> Hash {
        Tree::new(store).build_sorted((0..n).map(kv))
    }

    #[test]
    fn build_get_scan() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let root = build(&s, 50_000);
        for i in [0u64, 1, 999, 25_000, 49_999] {
            assert_eq!(t.get(&root, &kv(i).0), Some(kv(i).1));
        }
        assert_eq!(t.get(&root, &crate::keys::inode_key(50_000)), None);
        let mut c = t.cursor(&root, &kv(1000).0);
        for i in 1000..1100 {
            assert_eq!(c.entry().unwrap().0, kv(i).0.as_slice());
            c.next();
        }
        assert_eq!(t.agg(&root).keys, 50_000);
    }

    /// The property the whole design rests on (§P1): shape is a function
    /// of the key set, never of insertion order or history.
    #[test]
    fn insertion_order_does_not_change_the_root() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let mut keys: Vec<u64> = (0..20_000).collect();
        let bulk = t.build_sorted(keys.iter().map(|i| kv(*i)));
        let mut rng = SmallRng::seed_from_u64(7);
        for round in 0..8 {
            keys.shuffle(&mut rng);
            let mut root = t.empty();
            for chunk in keys.chunks(1 + round * 500) {
                let mut muts: Vec<Mut> = chunk
                    .iter()
                    .map(|i| {
                        let (k, v) = kv(*i);
                        (k, Some(v))
                    })
                    .collect();
                muts.sort();
                root = t.apply(&root, &muts);
            }
            assert_eq!(root, bulk, "round {round}");
        }
    }

    #[test]
    fn delete_then_reinsert_returns_the_original_hash() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let root = build(&s, 30_000);
        let mut rng = SmallRng::seed_from_u64(11);
        let mut victims: Vec<u64> = (0..30_000).choose_multiple(&mut rng, 3_000);
        victims.sort();
        let del: Vec<Mut> = victims.iter().map(|i| (kv(*i).0, None)).collect();
        let after = t.apply(&root, &del);
        assert_ne!(after, root);
        let ins: Vec<Mut> = victims
            .iter()
            .map(|i| {
                let (k, v) = kv(*i);
                (k, Some(v))
            })
            .collect();
        assert_eq!(t.apply(&after, &ins), root);
    }

    #[test]
    fn apply_matches_bulk_build_under_random_edits() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let mut rng = SmallRng::seed_from_u64(3);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = (0..40_000).map(kv).collect();
        let mut root = t.build_sorted(model.iter().map(|(k, v)| (k.clone(), v.clone())));
        for round in 0..12 {
            let mut muts: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
            for _ in 0..500 {
                let i: u64 = rng.random_range(0..60_000);
                let (k, v) = kv(i);
                if rng.random_bool(0.35) {
                    muts.insert(k, None);
                } else {
                    let mut v = v;
                    v.extend_from_slice(b"-edited");
                    muts.insert(k, Some(v));
                }
            }
            for (k, v) in &muts {
                match v {
                    Some(v) => model.insert(k.clone(), v.clone()),
                    None => model.remove(k),
                };
            }
            let m: Vec<Mut> = muts.into_iter().collect();
            root = t.apply(&root, &m);
            let bulk = t.build_sorted(model.iter().map(|(k, v)| (k.clone(), v.clone())));
            assert_eq!(root, bulk, "round {round}");
        }
        assert_eq!(t.agg(&root).keys, model.len() as u64);
    }

    /// The entry clamp must not weaken canonicality: a node's start is
    /// context-free, so "seal after MAX entries from the start" is still
    /// a pure function of the key set.
    #[test]
    fn entry_clamp_keeps_the_tree_canonical() {
        let _g = lock();
        for max in [0usize, 32, 128] {
            node::MAX_ENTRIES.store(max, std::sync::atomic::Ordering::Relaxed);
            let s = Store::memory();
            let t = Tree::new(&s);
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = (0..20_000).map(kv).collect();
            let bulk = t.build_sorted(model.iter().map(|(k, v)| (k.clone(), v.clone())));
            let mut rng = SmallRng::seed_from_u64(max as u64 + 1);
            let mut keys: Vec<u64> = (0..20_000).collect();
            keys.shuffle(&mut rng);
            let mut root = t.empty();
            for chunk in keys.chunks(777) {
                let mut muts: Vec<Mut> = chunk
                    .iter()
                    .map(|i| {
                        let (k, v) = kv(*i);
                        (k, Some(v))
                    })
                    .collect();
                muts.sort();
                root = t.apply(&root, &muts);
            }
            assert_eq!(root, bulk, "max={max}");
            // And under deletes, which are what force a run to absorb its
            // right neighbour when the clamp is on.
            let mut victims: Vec<u64> = (0..20_000).choose_multiple(&mut rng, 2_000);
            victims.sort();
            let del: Vec<Mut> = victims.iter().map(|i| (kv(*i).0, None)).collect();
            root = t.apply(&root, &del);
            for i in &victims {
                model.remove(&kv(*i).0);
            }
            let bulk2 = t.build_sorted(model.iter().map(|(k, v)| (k.clone(), v.clone())));
            assert_eq!(root, bulk2, "max={max} after deletes");
            if max > 0 {
                let (levels, counts) = t.level_stats(&root);
                assert!(counts.iter().all(|c| *c <= max), "clamp not honoured");
                assert!(levels.len() >= 2);
            }
            node::MAX_ENTRIES.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn diff_reports_exactly_the_changed_keys() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let root = build(&s, 40_000);
        let muts: Vec<Mut> = vec![
            (kv(10).0, Some(b"new".to_vec())),
            (kv(20_000).0, None),
            (crate::keys::inode_key(1_000_000), Some(b"added".to_vec())),
        ];
        let b = t.apply(&root, &muts);
        let (changes, nodes) = t.diff(&root, &b);
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].1, Change::Modified);
        assert_eq!(changes[1].1, Change::Removed);
        assert_eq!(changes[2].1, Change::Added);
        // O(difference): a 3-key diff of a 40k-key tree must not walk it.
        assert!(nodes < 60, "diff read {nodes} nodes");
        assert!(t.diff(&root, &root).0.is_empty());
    }

    #[test]
    fn disjoint_branches_merge_to_one_hash() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let base = build(&s, 20_000);
        let a = t.apply(
            &base,
            &(0..500)
                .map(|i| (kv(i * 2).0, Some(b"a".to_vec())))
                .collect::<Vec<_>>(),
        );
        let b = t.apply(
            &base,
            &(0..500)
                .map(|i| (kv(i * 2 + 1).0, Some(b"b".to_vec())))
                .collect::<Vec<_>>(),
        );
        let ab = t.merge(&base, &a, &b).expect("disjoint");
        let ba = t.merge(&base, &b, &a).expect("disjoint");
        assert_eq!(ab, ba);
        assert_eq!(t.get(&ab, &kv(0).0), Some(b"a".to_vec()));
        assert_eq!(t.get(&ab, &kv(1).0), Some(b"b".to_vec()));
    }

    #[test]
    fn overlapping_branches_report_the_conflict_set() {
        let _g = lock();
        let s = Store::memory();
        let t = Tree::new(&s);
        let base = build(&s, 5_000);
        let a = t.apply(&base, &[(kv(7).0, Some(b"a".to_vec()))]);
        let b = t.apply(&base, &[(kv(7).0, Some(b"b".to_vec()))]);
        assert_eq!(t.merge(&base, &a, &b), Err(vec![kv(7).0]));
    }
}
