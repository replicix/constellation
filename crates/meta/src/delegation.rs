//! Plan 30 §M11: the replicated delegation table, its row in `ns`, and
//! ownership resolution by an ancestor walk. (The per-stream pending
//! part of a position is `session::Streams`.)
//!
//! Nothing here does IO or touches the store. The namespace is reached
//! through the [`Namespace`] trait, which phase 2 implements for `Meta`
//! over the `0x02` (dentry) and `0x04` (reverse dentry) key ranges; the
//! model (`crates/model/src/delegation.rs`) is the specification of the
//! rules these helpers serve, and the "Plan 30 M11 — phase 1" section of
//! `docs/plans/v1/PROGRESS.md` the design they fit into.
//!
//! # Ownership
//!
//! - A dentry key `0x02 | parent | name` belongs to the delegation that
//!   *contains* `parent`: the innermost live delegation on `parent` or an
//!   ancestor of it, found by walking `parent_of` towards the root.
//! - An inode's own keys (`0x01`, `0x03`, `0x04`) belong to the
//!   delegation containing its primary link's parent
//!   (`Namespace::primary_parent`).
//! - Keys under no delegation belong to the root sequencer.
//! - An op whose keys resolve to more than one owner (two delegations, or
//!   a delegation and the root) is *cross-subtree* and goes to the root,
//!   which recalls the delegations involved before executing it.
//!
//! With an empty table (a single node, or a cluster with no delegation)
//! every resolution is one `is_empty` check: M11 is a strict no-op there.
//! With delegations, a resolution costs one parent lookup per ancestor
//! walked (the directory depth at worst) per distinct directory in the
//! op's keys — two or three walks for a rename, one for everything else.

use crate::error::MetaError;
use crate::replay::TouchSet;
use crate::store::{ns, Meta};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use fjall::{Readable, SingleWriterWriteTx};
use std::collections::BTreeMap;

pub type NodeId = u64;
/// A delegation generation: minted by the root, never reused for a
/// directory (a re-delegation is a new generation).
pub type Gen = u64;

/// The log records that maintain the table (phase 2 adds them to
/// `LogRecord`; they touch no inode or dentry, like `Completed`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegationRecord {
    /// `dir` (its `range`: the whole subtree, or one hash range of its
    /// own names) is sequenced by `node` under generation `gen` from
    /// this record on.
    Delegate {
        dir: Ino,
        node: NodeId,
        gen: Gen,
        designated: bool,
        range: Range,
    },
    /// Generation `gen` ended: every record of its stream that is not
    /// before this one in the log is void.
    Recall { dir: Ino, gen: Gen },
}

/// Plan 30 §M12: a slice of a directory's name space (GIGA+). `bits ==
/// 0` is the whole directory *and its subtree* (M11's delegation);
/// `bits > 0` is the names of the directory itself whose hash's top
/// `bits` bits are `idx` — a range covers no subdirectory's contents
/// (those resolve past the split directory, to its ancestors).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Range {
    pub bits: u8,
    pub idx: u32,
}

impl Range {
    pub const WHOLE: Range = Range { bits: 0, idx: 0 };

    pub fn is_whole(&self) -> bool {
        self.bits == 0
    }

    /// The range of `bits` bits that `name` falls in.
    pub fn of(bits: u8, name: &str) -> Range {
        if bits == 0 {
            return Range::WHOLE;
        }
        Range {
            bits,
            idx: (name_hash(name) >> (64 - u32::from(bits))) as u32,
        }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.is_whole() || Range::of(self.bits, name).idx == self.idx
    }

    /// `idx/2^bits` (`""` for the whole directory), for status output.
    pub fn label(&self) -> String {
        if self.is_whole() {
            String::new()
        } else {
            format!("{}/{}", self.idx, 1u64 << self.bits)
        }
    }
}

/// The name hash the ranges split on: FNV-1a 64, a fixed function
/// every node computes alike (a range is part of the log).
pub fn name_hash(name: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // Mix the low bits into the top ones the ranges look at.
    h ^= h >> 32;
    h = h.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    h ^ (h >> 29)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delegation {
    pub dir: Ino,
    pub node: NodeId,
    pub gen: Gen,
    /// Phase 2b: an offline designation — recalled only by `online`.
    pub designated: bool,
    /// Plan 30 §M12: the part of `dir` this generation owns.
    pub range: Range,
}

/// What owns a directory (plan 30 §M12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirOwner {
    /// The root sequencer.
    Root,
    /// One delegation of the directory or an ancestor.
    Whole(Delegation),
    /// The directory's own names are split into ranges, each its own
    /// generation (names in a range nobody holds are the root's).
    Split(Vec<Delegation>),
}

/// The live delegations, derived from the log prefix (plus, on the root,
/// its unshipped journal). Delegations never overlap: the root writes a
/// `Delegate` only for a directory no live delegation contains, and only
/// after every delegation under it was recalled; a directory is either
/// wholly delegated or split into ranges (one split depth at a time).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegationTable {
    by_dir: BTreeMap<Ino, Vec<Delegation>>,
    /// The highest generation any `Delegate` record ever named (phase
    /// 2b: a successor root allocates above it — a generation number is
    /// a stream's identity, never reused across roots).
    max_gen: Gen,
}

type Row = (Ino, NodeId, Gen, bool, u8, u32);

impl DelegationTable {
    pub fn apply(&mut self, rec: DelegationRecord) {
        match rec {
            DelegationRecord::Delegate {
                dir,
                node,
                gen,
                designated,
                range,
            } => {
                self.max_gen = self.max_gen.max(gen);
                let d = Delegation {
                    dir,
                    node,
                    gen,
                    designated,
                    range,
                };
                let v = self.by_dir.entry(dir).or_default();
                if range.is_whole() {
                    v.clear();
                } else {
                    v.retain(|e| !e.range.is_whole() && e.range != range);
                }
                v.push(d);
            }
            DelegationRecord::Recall { dir, gen } => {
                if let Some(v) = self.by_dir.get_mut(&dir) {
                    v.retain(|e| e.gen != gen);
                    if v.is_empty() {
                        self.by_dir.remove(&dir);
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.by_dir.is_empty()
    }

    /// The highest generation ever delegated (0: none).
    pub fn max_gen(&self) -> Gen {
        self.max_gen
    }

    /// The `0x30 | Delegation` row's value: postcard of the live rows,
    /// sorted by directory.
    pub fn encode(&self) -> Vec<u8> {
        let rows: Vec<Row> = self
            .iter()
            .map(|d| {
                (
                    d.dir,
                    d.node,
                    d.gen,
                    d.designated,
                    d.range.bits,
                    d.range.idx,
                )
            })
            .collect();
        postcard::to_allocvec(&(rows, self.max_gen)).unwrap_or_default()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        let (rows, max_gen): (Vec<Row>, Gen) = postcard::from_bytes(bytes)
            .map_err(|e| MetaError::Invalid(format!("delegation row: {e}")))?;
        let mut t = DelegationTable {
            max_gen,
            ..DelegationTable::default()
        };
        for (dir, node, gen, designated, bits, idx) in rows {
            t.apply(DelegationRecord::Delegate {
                dir,
                node,
                gen,
                designated,
                range: Range { bits, idx },
            });
        }
        Ok(t)
    }

    /// The whole-directory delegation of `dir`, if any (a split
    /// directory has none: see [`Self::ranges_of`]).
    pub fn get(&self, dir: Ino) -> Option<Delegation> {
        self.by_dir
            .get(&dir)
            .and_then(|v| v.iter().find(|d| d.range.is_whole()).copied())
    }

    /// Every live delegation of `dir` itself: one whole, or the ranges.
    pub fn ranges_of(&self, dir: Ino) -> Vec<Delegation> {
        self.by_dir.get(&dir).cloned().unwrap_or_default()
    }

    /// Whether `dir`'s own names are split into ranges.
    pub fn is_split(&self, dir: Ino) -> bool {
        self.by_dir
            .get(&dir)
            .is_some_and(|v| v.iter().any(|d| !d.range.is_whole()))
    }

    pub fn iter(&self) -> impl Iterator<Item = Delegation> + '_ {
        self.by_dir.values().flatten().copied()
    }

    /// Who owns `dir` as a whole: the innermost whole delegation on it
    /// or an ancestor (a split ancestor's ranges cover only its own
    /// names, so the walk passes it), the ranges when `dir` itself is
    /// split, else the root.
    pub fn owner_of_dir(&self, ns: &dyn Namespace, dir: Ino) -> DirOwner {
        if self.by_dir.is_empty() {
            return DirOwner::Root;
        }
        let mut d = dir;
        let mut first = true;
        // Bounded by the tree's depth; the guard only stops a corrupt
        // parent chain from looping.
        for _ in 0..MAX_WALK {
            if let Some(v) = self.by_dir.get(&d) {
                if let Some(w) = v.iter().find(|e| e.range.is_whole()) {
                    return DirOwner::Whole(*w);
                }
                if first {
                    return DirOwner::Split(v.clone());
                }
            }
            first = false;
            match ns.parent_of(d) {
                Some(p) => d = p,
                None => return DirOwner::Root,
            }
        }
        DirOwner::Root
    }

    /// The whole delegation containing `dir` (`None`: the root, or
    /// `dir` is split — its names have owners of their own).
    pub fn covering(&self, ns: &dyn Namespace, dir: Ino) -> Option<Delegation> {
        match self.owner_of_dir(ns, dir) {
            DirOwner::Whole(d) => Some(d),
            _ => None,
        }
    }

    /// Who owns the name `name` in `parent`.
    pub fn owner_of_dentry(
        &self,
        ns: &dyn Namespace,
        parent: Ino,
        name: &str,
    ) -> Option<Delegation> {
        match self.owner_of_dir(ns, parent) {
            DirOwner::Root => None,
            DirOwner::Whole(d) => Some(d),
            DirOwner::Split(v) => v.into_iter().find(|d| d.range.contains(name)),
        }
    }

    /// Who executes an op touching `keys`.
    pub fn resolve(&self, ns: &dyn Namespace, keys: &TouchSet) -> Ownership {
        if self.by_dir.is_empty() {
            return Ownership::Root;
        }
        let mut owners: Vec<Option<Delegation>> = Vec::new();
        let mut note = |o: Option<Delegation>| {
            if !owners.contains(&o) {
                owners.push(o);
            }
        };
        for (parent, name) in keys.dentries.iter() {
            note(self.owner_of_dentry(ns, *parent, name));
        }
        // An exclusive hold on a directory involves everything of it
        // (every range of a split one); a file's is its parent's — and,
        // in a split parent, its name's range (M12 round 2: a publish or
        // setattr of a file the root wrote into a range resolved to the
        // root *and* the range, so every one recalled the range: harness
        // `shared-dir-multi-writer`, the root's log PUTs up by a third
        // and a file's content unpublished for the run). A directory
        // inside a split parent stays the root's: its subtree is not the
        // range's, so an op that removes or moves it recalls the range.
        for ino in keys.inos.iter() {
            if !self.by_dir.contains_key(ino) {
                if let Some((p, name, false)) = ns.primary_dentry(*ino) {
                    if self.is_split(p) {
                        note(self.owner_of_dentry(ns, p, &name));
                        continue;
                    }
                }
            }
            match self.owner_of_dir(ns, *ino) {
                DirOwner::Root => note(None),
                DirOwner::Whole(d) => note(Some(d)),
                DirOwner::Split(v) => {
                    for d in v {
                        note(Some(d));
                    }
                }
            }
        }
        // A shared hold on a split directory decides nothing (the name
        // does); on a whole one it is that delegation's.
        for dir in keys.shared.iter() {
            match self.owner_of_dir(ns, *dir) {
                DirOwner::Root => note(None),
                DirOwner::Whole(d) => note(Some(d)),
                DirOwner::Split(_) => {}
            }
        }
        match owners.as_slice() {
            [] | [None] => Ownership::Root,
            [Some(d)] => Ownership::Delegated(*d),
            _ => Ownership::CrossSubtree {
                involved: owners.iter().flatten().copied().collect(),
                root_owned: owners.contains(&None),
            },
        }
    }
}

const MAX_WALK: usize = 4096;

/// Plan 30 §M12: a root fast-path execution admitted by
/// [`Meta::root_fast_path`]; dropped once the op is journaled (or
/// refused). Never held across a wait.
pub struct RootAdmission<'a>(#[allow(dead_code)] std::sync::RwLockReadGuard<'a, ()>);

/// Where an op executes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ownership {
    /// Every key is root-owned (or the table is empty).
    Root,
    /// Every key is under this one delegation.
    Delegated(Delegation),
    /// Keys under more than one owner: the root recalls `involved` and
    /// executes.
    CrossSubtree {
        involved: Vec<Delegation>,
        root_owned: bool,
    },
}

/// The namespace shape ownership resolution needs.
pub trait Namespace {
    /// The parent directory of `dir`; `None` at the root directory.
    fn parent_of(&self, dir: Ino) -> Option<Ino>;
    /// The parent of `ino`'s primary link (the first `0x04` entry);
    /// `None` for the root directory or an unlinked inode.
    fn primary_parent(&self, ino: Ino) -> Option<Ino>;
    /// M12 round 2: `ino`'s primary link as `(parent, name, is_dir)`;
    /// `None` for the root directory or an unlinked inode. A file whose
    /// name is in a split directory belongs to that name's range.
    fn primary_dentry(&self, ino: Ino) -> Option<(Ino, String, bool)>;
}

// ------------------------------------------------------ the row in `ns`

/// The key of the table's row.
pub fn table_key() -> Vec<u8> {
    keys::subsystem(keys::Subsystem::Delegation, b"")
}

pub(crate) fn read_table_tx(r: &impl Readable, meta: &Meta) -> Result<DelegationTable, MetaError> {
    match r.get(&meta.ns, table_key())? {
        Some(v) => DelegationTable::decode(&v),
        None => Ok(DelegationTable::default()),
    }
}

pub(crate) fn write_table_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    table: &DelegationTable,
) -> Result<(), MetaError> {
    // Plan 30 §M12: the fast path's cheap pre-check. Set before the
    // commit, so a reader that sees it clear is one the commit had not
    // reached; a grant's transaction holds `deleg_gate` exclusively.
    meta.deleg_any
        .store(!table.is_empty(), std::sync::atomic::Ordering::Release);
    if table.is_empty() && table.max_gen() == 0 {
        ns::ns_remove(tx, &meta.ns, dirty, table_key())
    } else {
        ns::ns_insert(tx, &meta.ns, dirty, table_key(), table.encode())
    }
}

impl Meta {
    /// The live delegation table as this replica knows it: the fold of
    /// every `Delegate`/`Recall` record it applied (its journal
    /// included, on the root). One point read of a small key.
    pub fn delegation_table(&self) -> DelegationTable {
        let r = self.db.read_tx();
        read_table_tx(&r, self).unwrap_or_default()
    }

    /// Plan 30 §M12: whether the table names any live delegation, as
    /// of the last table write here (an atomic: the FUSE fast path's
    /// pre-check).
    pub fn delegations_any(&self) -> bool {
        self.deleg_any.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Plan 30 §M12: the lock the root's FUSE fast path holds shared
    /// across its ownership check and execution, and a `Delegate`/
    /// `Recall` transaction holds exclusively (see `Meta::deleg_gate`).
    pub fn delegation_gate(&self) -> &std::sync::RwLock<()> {
        &self.deleg_gate
    }

    /// Plan 30 §M12: the root's fast path admission — `Some` (the
    /// delegation gate held shared until dropped) when no live delegation
    /// owns any key of `op`, so the root may execute it locally; `None`
    /// when one does (the op goes through the core: a forward to its
    /// delegate, or a recall). The check and the execution happen under
    /// the same shared hold, and a grant's transaction takes the gate
    /// exclusively, so the execution is either journaled before the
    /// grant's record or refused here. Costs one atomic load while the
    /// table is empty. The FUSE fast path and the simulation's model of
    /// it both go through here.
    pub fn root_fast_path(&self, op: &crate::mutate::MutateOp) -> Option<RootAdmission<'_>> {
        let guard = self.deleg_gate.read().unwrap_or_else(|e| e.into_inner());
        if self.delegations_any() {
            let keys = TouchSet::from_op_in(op, &|p, n| {
                crate::MetaStore::lookup(self, p, n)
                    .ok()
                    .flatten()
                    .map(|a| a.ino)
            });
            if !matches!(self.resolve_ownership(&keys), Ownership::Root) {
                return None;
            }
        }
        Some(RootAdmission(guard))
    }

    /// Who executes an op touching `keys` (see the module doc).
    pub fn resolve_ownership(&self, keys: &TouchSet) -> Ownership {
        let table = self.delegation_table();
        if table.is_empty() {
            return Ownership::Root;
        }
        table.resolve(self, keys)
    }

    /// Mint the next delegation generation (the root's persisted
    /// counter; never reused, a successor root continues above the
    /// highest generation in the log).
    pub fn next_delegation_gen(&self, at_least: u64) -> Result<u64, MetaError> {
        let mut tx = self.db.write_tx();
        let cur = crate::store::kv_get_tx(&tx, &self.local, KV_NEXT_DELEG_GEN)?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(1);
        let gen = cur.max(at_least);
        crate::store::kv_set_tx(
            &mut tx,
            &self.local,
            KV_NEXT_DELEG_GEN,
            &(gen + 1).to_string(),
        );
        tx.commit()?;
        Ok(gen)
    }
}

/// `local` kv: the root's next delegation generation.
pub(crate) const KV_NEXT_DELEG_GEN: &str = "next_deleg_gen";

impl Namespace for Meta {
    fn parent_of(&self, dir: Ino) -> Option<Ino> {
        if dir == ROOT_INO {
            return None;
        }
        self.primary_parent(dir)
    }

    fn primary_parent(&self, ino: Ino) -> Option<Ino> {
        self.primary_dentry(ino).map(|(p, _, _)| p)
    }

    fn primary_dentry(&self, ino: Ino) -> Option<(Ino, String, bool)> {
        if ino == ROOT_INO {
            return None;
        }
        let r = self.db.read_tx();
        let range = keys::names_of(ino);
        let guard = r.range(&self.ns, ns::key_range_bounds(&range)).next()?;
        let (k, _) = guard.into_inner().ok()?;
        let (parent, name) = match keys::Key::parse(&k).ok()? {
            keys::Key::RDentry {
                parent_ino, name, ..
            } => (parent_ino, String::from_utf8_lossy(name).into_owned()),
            _ => return None,
        };
        let is_dir = ns::get_inode_record(&r, &self.ns, ino)
            .ok()
            .flatten()
            .is_some_and(|rec| rec.attrs.kind == constellation_mtree::record::Kind::Dir);
        Some((parent, name, is_dir))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// `/` (1), `/D1` (2), `/D2` (3), `/D1/D3` (4); file 10 under D3,
    /// file 11 under `/`.
    struct Tree {
        parent: HashMap<Ino, Ino>,
        /// Files: `ino -> (parent, name)`.
        files: HashMap<Ino, (Ino, String)>,
    }

    impl Tree {
        fn new() -> Self {
            let parent = [(2, 1), (3, 1), (4, 2), (10, 4), (11, 1)]
                .into_iter()
                .collect();
            Tree {
                parent,
                files: HashMap::new(),
            }
        }
    }

    impl Namespace for Tree {
        fn parent_of(&self, dir: Ino) -> Option<Ino> {
            self.parent.get(&dir).copied()
        }
        fn primary_parent(&self, ino: Ino) -> Option<Ino> {
            self.parent.get(&ino).copied()
        }
        fn primary_dentry(&self, ino: Ino) -> Option<(Ino, String, bool)> {
            // Every inode of the test tree is a directory named by its
            // number; files are added by `resolve_file_in_split_parent`.
            self.files
                .get(&ino)
                .map(|(p, n)| (*p, n.clone(), false))
                .or_else(|| self.parent.get(&ino).map(|p| (*p, ino.to_string(), true)))
        }
    }

    fn table() -> DelegationTable {
        let mut t = DelegationTable::default();
        t.apply(DelegationRecord::Delegate {
            dir: 2,
            node: 7,
            gen: 1,
            designated: false,
            range: Range::WHOLE,
        });
        t.apply(DelegationRecord::Delegate {
            dir: 3,
            node: 8,
            gen: 2,
            designated: false,
            range: Range::WHOLE,
        });
        t
    }

    fn dentry(parent: Ino, name: &str) -> TouchSet {
        let mut k = TouchSet::default();
        k.dentries.insert((parent, name.to_string()));
        k
    }

    #[test]
    fn the_walk_finds_the_innermost_containing_delegation() {
        let ns = Tree::new();
        let t = table();
        assert_eq!(t.covering(&ns, 4).map(|d| d.gen), Some(1));
        assert_eq!(t.covering(&ns, 2).map(|d| d.gen), Some(1));
        assert_eq!(t.covering(&ns, 1), None);
        // A nested delegation is innermost.
        let mut t2 = t.clone();
        t2.apply(DelegationRecord::Delegate {
            dir: 4,
            node: 9,
            gen: 3,
            designated: false,
            range: Range::WHOLE,
        });
        assert_eq!(t2.covering(&ns, 4).map(|d| d.gen), Some(3));
    }

    #[test]
    fn keys_resolve_to_root_one_delegation_or_cross_subtree() {
        let ns = Tree::new();
        let t = table();
        assert_eq!(t.resolve(&ns, &dentry(1, "x")), Ownership::Root);
        assert!(matches!(
            t.resolve(&ns, &dentry(4, "x")),
            Ownership::Delegated(Delegation { gen: 1, .. })
        ));
        // An inode key follows its primary link's parent.
        let mut inode = TouchSet::default();
        inode.inos.insert(10);
        assert!(matches!(
            t.resolve(&ns, &inode),
            Ownership::Delegated(Delegation { gen: 1, .. })
        ));
        // A rename inside one subtree stays with it; across two, or into
        // the root directory, it is cross-subtree.
        let mut inside = dentry(2, "a");
        inside.dentries.insert((4, "b".into()));
        assert!(matches!(t.resolve(&ns, &inside), Ownership::Delegated(_)));
        let mut across = dentry(2, "a");
        across.dentries.insert((3, "b".into()));
        match t.resolve(&ns, &across) {
            Ownership::CrossSubtree {
                involved,
                root_owned,
            } => {
                assert_eq!(involved.len(), 2);
                assert!(!root_owned);
            }
            o => panic!("{o:?}"),
        }
        let mut up = dentry(2, "a");
        up.dentries.insert((1, "b".into()));
        match t.resolve(&ns, &up) {
            Ownership::CrossSubtree {
                involved,
                root_owned,
            } => {
                assert_eq!(involved.len(), 1);
                assert!(root_owned);
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn a_recall_ends_only_its_generation_and_an_empty_table_is_free() {
        let ns = Tree::new();
        let mut t = table();
        t.apply(DelegationRecord::Recall { dir: 2, gen: 0 });
        assert!(t.get(2).is_some(), "a stale recall changes nothing");
        t.apply(DelegationRecord::Recall { dir: 2, gen: 1 });
        assert!(t.get(2).is_none());
        assert_eq!(t.resolve(&ns, &dentry(4, "x")), Ownership::Root);
        let empty = DelegationTable::default();
        assert_eq!(empty.resolve(&ns, &dentry(4, "x")), Ownership::Root);
    }

    /// M12 round 2: a file whose name is in a split directory belongs to
    /// its name's range (a setattr or manifest commit of it goes to the
    /// range's delegate); a directory inside a split parent stays the
    /// root's, so an exclusive hold on it involves the root and the
    /// range.
    #[test]
    fn a_file_in_a_split_directory_resolves_to_its_range() {
        let mut ns = Tree::new();
        // Directory 11 (under the root) is split two ways between nodes
        // 7 and 8; file 20 is "alpha" in it, directory 12 is "beta" in it.
        let mut t = DelegationTable::default();
        t.apply(DelegationRecord::Delegate {
            dir: 11,
            node: 7,
            gen: 1,
            designated: false,
            range: Range { bits: 1, idx: 0 },
        });
        t.apply(DelegationRecord::Delegate {
            dir: 11,
            node: 8,
            gen: 2,
            designated: false,
            range: Range { bits: 1, idx: 1 },
        });
        ns.files.insert(20, (11, "alpha".into()));
        ns.parent.insert(12, 11);
        let owner = t
            .owner_of_dentry(&ns, 11, "alpha")
            .expect("a range owns alpha");
        let mut file = TouchSet::default();
        file.inos.insert(20);
        assert_eq!(t.resolve(&ns, &file), Ownership::Delegated(owner));
        // The directory's own exclusive hold: the root's subtree and the
        // range of its name.
        let mut dir = TouchSet::default();
        dir.inos.insert(12);
        dir.dentries.insert((11, "12".into()));
        match t.resolve(&ns, &dir) {
            Ownership::CrossSubtree { root_owned, .. } => assert!(root_owned),
            other => panic!("a directory in a split parent resolved to {other:?}"),
        }
    }
}
