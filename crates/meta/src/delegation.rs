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
    /// `dir` and everything under it is sequenced by `node` under
    /// generation `gen` from this record on.
    Delegate {
        dir: Ino,
        node: NodeId,
        gen: Gen,
        designated: bool,
    },
    /// Generation `gen` ended: every record of its stream that is not
    /// before this one in the log is void.
    Recall { dir: Ino, gen: Gen },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delegation {
    pub dir: Ino,
    pub node: NodeId,
    pub gen: Gen,
    /// Phase 2b: an offline designation — recalled only by `online`.
    pub designated: bool,
}

/// The live delegations, derived from the log prefix (plus, on the root,
/// its unshipped journal). Delegations never overlap: the root writes a
/// `Delegate` only for a directory no live delegation contains, and only
/// after every delegation under it was recalled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegationTable {
    by_dir: BTreeMap<Ino, Delegation>,
    /// The highest generation any `Delegate` record ever named (phase
    /// 2b: a successor root allocates above it — a generation number is
    /// a stream's identity, never reused across roots).
    max_gen: Gen,
}

impl DelegationTable {
    pub fn apply(&mut self, rec: DelegationRecord) {
        match rec {
            DelegationRecord::Delegate {
                dir,
                node,
                gen,
                designated,
            } => {
                self.max_gen = self.max_gen.max(gen);
                self.by_dir.insert(
                    dir,
                    Delegation {
                        dir,
                        node,
                        gen,
                        designated,
                    },
                );
            }
            DelegationRecord::Recall { dir, gen } => {
                if self.by_dir.get(&dir).is_some_and(|d| d.gen == gen) {
                    self.by_dir.remove(&dir);
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
        let rows: Vec<(Ino, NodeId, Gen, bool)> = self
            .iter()
            .map(|d| (d.dir, d.node, d.gen, d.designated))
            .collect();
        postcard::to_allocvec(&(rows, self.max_gen)).unwrap_or_default()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        type Rows = Vec<(Ino, NodeId, Gen, bool)>;
        let (rows, max_gen): (Rows, Gen) = match postcard::from_bytes(bytes) {
            Ok(v) => v,
            // Phase 2a rows carried no counter.
            Err(_) => {
                let rows: Rows = postcard::from_bytes(bytes)
                    .map_err(|e| MetaError::Invalid(format!("delegation row: {e}")))?;
                let max = rows.iter().map(|r| r.2).max().unwrap_or(0);
                (rows, max)
            }
        };
        let mut t = DelegationTable {
            max_gen,
            ..DelegationTable::default()
        };
        for (dir, node, gen, designated) in rows {
            t.by_dir.insert(
                dir,
                Delegation {
                    dir,
                    node,
                    gen,
                    designated,
                },
            );
        }
        Ok(t)
    }

    pub fn get(&self, dir: Ino) -> Option<Delegation> {
        self.by_dir.get(&dir).copied()
    }

    pub fn iter(&self) -> impl Iterator<Item = Delegation> + '_ {
        self.by_dir.values().copied()
    }

    /// The delegation containing `dir`: the innermost live one on `dir`
    /// or an ancestor. `None` is the root sequencer.
    pub fn owner_of_dir(&self, ns: &dyn Namespace, mut dir: Ino) -> Option<Delegation> {
        if self.by_dir.is_empty() {
            return None;
        }
        // Bounded by the tree's depth; the guard only stops a corrupt
        // parent chain from looping.
        for _ in 0..MAX_WALK {
            if let Some(d) = self.by_dir.get(&dir) {
                return Some(*d);
            }
            dir = ns.parent_of(dir)?;
        }
        None
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
        for (parent, _) in keys.dentries.iter() {
            note(self.owner_of_dir(ns, *parent));
        }
        for ino in keys.inos.iter() {
            let dir = ns.primary_parent(*ino).unwrap_or(*ino);
            note(self.owner_of_dir(ns, dir));
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
        if ino == ROOT_INO {
            return None;
        }
        let r = self.db.read_tx();
        let range = keys::names_of(ino);
        let guard = r.range(&self.ns, ns::key_range_bounds(&range)).next()?;
        let (k, _) = guard.into_inner().ok()?;
        match keys::Key::parse(&k).ok()? {
            keys::Key::RDentry { parent_ino, .. } => Some(parent_ino),
            _ => None,
        }
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
    }

    impl Tree {
        fn new() -> Self {
            let parent = [(2, 1), (3, 1), (4, 2), (10, 4), (11, 1)]
                .into_iter()
                .collect();
            Tree { parent }
        }
    }

    impl Namespace for Tree {
        fn parent_of(&self, dir: Ino) -> Option<Ino> {
            self.parent.get(&dir).copied()
        }
        fn primary_parent(&self, ino: Ino) -> Option<Ino> {
            self.parent.get(&ino).copied()
        }
    }

    fn table() -> DelegationTable {
        let mut t = DelegationTable::default();
        t.apply(DelegationRecord::Delegate {
            dir: 2,
            node: 7,
            gen: 1,
            designated: false,
        });
        t.apply(DelegationRecord::Delegate {
            dir: 3,
            node: 8,
            gen: 2,
            designated: false,
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
        assert_eq!(t.owner_of_dir(&ns, 4).map(|d| d.gen), Some(1));
        assert_eq!(t.owner_of_dir(&ns, 2).map(|d| d.gen), Some(1));
        assert_eq!(t.owner_of_dir(&ns, 1), None);
        // A nested delegation is innermost.
        let mut t2 = t.clone();
        t2.apply(DelegationRecord::Delegate {
            dir: 4,
            node: 9,
            gen: 3,
            designated: false,
        });
        assert_eq!(t2.owner_of_dir(&ns, 4).map(|d| d.gen), Some(3));
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
}
