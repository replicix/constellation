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
use crate::record::{SubtreeGrant, SubtreeKind};
use crate::replay::TouchSet;
use crate::store::{ns, Meta};
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use fjall::{Readable, SingleWriterWriteTx};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type NodeId = u64;
/// A delegation generation: minted by the root tenure that grants it, in its
/// epoch's block (`epoch << 32 | n`, ADR-43), never reused for a directory
/// (a re-delegation is a new generation) and never minted twice by two
/// tenures.
pub type Gen = u64;

/// The log records that maintain the table (they touch no inode or
/// dentry, like `Completed`): the fold of [`LogRecord::DelegateNode`]
/// (a generation opens, or its set grows), `SubtreeReleased`,
/// `StreamReleased` and `DelegateNodeEnded` (plan 46 M1.2 step 8a, §5.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DelegationRecord {
    /// `node` holds the subtrees `dirs` under generation `gen` from this
    /// record on: the generation's first record opens its stream, a later
    /// one grows the live stream's set.
    DelegateNode {
        node: NodeId,
        gen: Gen,
        dirs: Vec<SubtreeGrant>,
    },
    /// The holder released the subtree `dir`; the rest of the set stays.
    SubtreeReleased { node: NodeId, gen: Gen, dir: Ino },
    /// The holder released every subtree (the generation lives on, with an
    /// empty set, until the root ends it).
    StreamReleased { node: NodeId, gen: Gen },
    /// Generation `gen` ended: every record of its stream that is not
    /// before this one in the log is void.
    DelegateNodeEnded { node: NodeId, gen: Gen },
}

/// One live subtree grant, as the walk finds it: the subtree `dir`, the
/// stream `(node, gen)` that sequences it, and what kind of grant it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delegation {
    pub dir: Ino,
    pub node: NodeId,
    pub gen: Gen,
    /// Phase 2b: a `Designated` subtree is an offline designation:
    /// recalled only by `online`.
    pub kind: SubtreeKind,
}

impl Delegation {
    /// An offline designation (never recalled by TTL or placement).
    pub fn designated(&self) -> bool {
        self.kind == SubtreeKind::Designated
    }

    /// The stream that sequences the subtree.
    pub fn stream(&self) -> (NodeId, Gen) {
        (self.node, self.gen)
    }
}

/// The live delegations, derived from the log prefix (plus, on the root,
/// its unshipped journal): one [`NodeStream`] per `(node, gen)` with the
/// subtree set folded from its records, and a `by_dir` index for the
/// ownership walk. A node has one stream per generation, so a table keyed
/// by node holds as many entries as the node has live generations (one, from
/// step 9 on). Subtrees never overlap in one directory: the root writes a
/// grant only for a directory no live grant contains, and only after every
/// grant under it was recalled (plan 46 D5 deleted the hash-range splits).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DelegationTable {
    /// The live streams and the subtrees each holds.
    streams: BTreeMap<(NodeId, Gen), BTreeMap<Ino, SubtreeKind>>,
    /// The walk's index: a subtree's directory to its stream and kind.
    by_dir: BTreeMap<Ino, (NodeId, Gen, SubtreeKind)>,
    /// The highest generation any `DelegateNode` record ever named. A number
    /// is minted by one tenure in its epoch's block (ADR-43), so no two
    /// tenures mint the same one and a successor's numbers are above it
    /// anyway; it floors the next mint (`Meta::next_delegation_gen`) and
    /// seeds the restart's session (`SessionState::seed_generations`).
    max_gen: Gen,
}

/// A live stream's subtrees (the persisted row of one stream).
type Row = (NodeId, Gen, Vec<(Ino, SubtreeKind)>);

/// One node stream as the table folded it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStream {
    pub node: NodeId,
    pub gen: Gen,
    /// The subtrees it holds now, in directory order.
    pub subtrees: Vec<(Ino, SubtreeKind)>,
}

impl DelegationTable {
    pub fn apply(&mut self, rec: DelegationRecord) {
        match rec {
            DelegationRecord::DelegateNode { node, gen, dirs } => {
                self.max_gen = self.max_gen.max(gen);
                self.streams.entry((node, gen)).or_default();
                for g in dirs {
                    // A directory has one holder: a grant of it replaces
                    // whatever stream held it (the fold of a recall the log
                    // lacked, as before).
                    if let Some((n, gn, _)) = self.by_dir.insert(g.dir, (node, gen, g.kind)) {
                        if (n, gn) != (node, gen) {
                            if let Some(set) = self.streams.get_mut(&(n, gn)) {
                                set.remove(&g.dir);
                            }
                        }
                    }
                    if let Some(set) = self.streams.get_mut(&(node, gen)) {
                        set.insert(g.dir, g.kind);
                    }
                }
            }
            DelegationRecord::SubtreeReleased { node, gen, dir } => {
                if self
                    .by_dir
                    .get(&dir)
                    .is_some_and(|e| (e.0, e.1) == (node, gen))
                {
                    self.by_dir.remove(&dir);
                    if let Some(set) = self.streams.get_mut(&(node, gen)) {
                        set.remove(&dir);
                    }
                }
            }
            DelegationRecord::StreamReleased { node, gen } => {
                if let Some(set) = self.streams.get_mut(&(node, gen)) {
                    for dir in std::mem::take(set).into_keys() {
                        self.by_dir.remove(&dir);
                    }
                }
            }
            // A generation number is unique in the log (ADR-43: minted in
            // one tenure's epoch block), so the end names its stream by
            // `gen`; `node` is the record's redundancy for its readers.
            DelegationRecord::DelegateNodeEnded { node: _, gen } => {
                let Some(key) = self.streams.keys().find(|(_, g)| *g == gen).copied() else {
                    return;
                };
                if let Some(set) = self.streams.remove(&key) {
                    for dir in set.into_keys() {
                        self.by_dir.remove(&dir);
                    }
                }
            }
        }
    }

    /// Whether no stream is live (a stream that released everything is
    /// live until the root ends it).
    pub fn is_empty(&self) -> bool {
        self.streams.is_empty()
    }

    /// The highest generation ever delegated (0: none).
    pub fn max_gen(&self) -> Gen {
        self.max_gen
    }

    /// The `0x30 | Delegation` row's value: postcard of the live streams,
    /// sorted by `(node, gen)`.
    pub fn encode(&self) -> Vec<u8> {
        let rows: Vec<Row> = self
            .streams
            .iter()
            .map(|(&(n, g), set)| (n, g, set.iter().map(|(&d, &k)| (d, k)).collect()))
            .collect();
        postcard::to_allocvec(&(rows, self.max_gen)).unwrap_or_default()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        let (rows, max_gen): (Vec<Row>, Gen) = crate::record::from_bytes_exact(bytes)
            .map_err(|e| MetaError::Invalid(format!("delegation row: {e}")))?;
        let mut t = DelegationTable {
            max_gen,
            ..DelegationTable::default()
        };
        for (node, gen, dirs) in rows {
            t.apply(DelegationRecord::DelegateNode {
                node,
                gen,
                dirs: dirs
                    .into_iter()
                    .map(|(dir, kind)| SubtreeGrant { dir, kind })
                    .collect(),
            });
        }
        Ok(t)
    }

    /// The subtree grant on `dir` itself, if any.
    pub fn get(&self, dir: Ino) -> Option<Delegation> {
        self.by_dir.get(&dir).map(|&(node, gen, kind)| Delegation {
            dir,
            node,
            gen,
            kind,
        })
    }

    /// Every live subtree grant, by directory.
    pub fn iter(&self) -> impl Iterator<Item = Delegation> + '_ {
        self.by_dir
            .iter()
            .map(|(&dir, &(node, gen, kind))| Delegation {
                dir,
                node,
                gen,
                kind,
            })
    }

    /// Every live stream with the subtrees it holds, by `(node, gen)`.
    pub fn streams(&self) -> impl Iterator<Item = NodeStream> + '_ {
        self.streams.iter().map(|(&(node, gen), set)| NodeStream {
            node,
            gen,
            subtrees: set.iter().map(|(&d, &k)| (d, k)).collect(),
        })
    }

    /// The live stream `(node, gen)`: its subtrees (`None`: not live).
    pub fn stream(&self, node: NodeId, gen: Gen) -> Option<NodeStream> {
        self.streams.get(&(node, gen)).map(|set| NodeStream {
            node,
            gen,
            subtrees: set.iter().map(|(&d, &k)| (d, k)).collect(),
        })
    }

    /// The node that holds generation `gen` (`None`: not live).
    pub fn node_of_gen(&self, gen: Gen) -> Option<NodeId> {
        self.streams
            .keys()
            .find(|(_, g)| *g == gen)
            .map(|&(n, _)| n)
    }

    /// The delegation containing `dir`: the innermost one on it or an
    /// ancestor (`None`: the root).
    pub fn covering(&self, ns: &dyn Namespace, dir: Ino) -> Option<Delegation> {
        if self.by_dir.is_empty() {
            return None;
        }
        let mut d = dir;
        // Bounded by the tree's depth; the guard only stops a corrupt
        // parent chain from looping.
        for _ in 0..MAX_WALK {
            if let Some(w) = self.get(d) {
                return Some(w);
            }
            d = ns.parent_of(d)?;
        }
        None
    }

    /// The stream `(node, gen)` that sequences `dir` (`None`: the root).
    pub fn owner_of_dir(&self, ns: &dyn Namespace, dir: Ino) -> Option<(NodeId, Gen)> {
        self.covering(ns, dir).map(|d| d.stream())
    }

    /// The stream that sequences the dentry `parent/_`: the one holding
    /// `parent`'s containing subtree (`None`: the root).
    pub fn owner_of_dentry(&self, ns: &dyn Namespace, parent: Ino) -> Option<(NodeId, Gen)> {
        self.owner_of_dir(ns, parent)
    }

    /// Who executes an op touching `keys`.
    pub fn resolve(&self, ns: &dyn Namespace, keys: &TouchSet) -> Ownership {
        if self.by_dir.is_empty() {
            return Ownership::Root;
        }
        // Owners are streams: two subtrees of one node's stream are one
        // owner (a rename between them is an ordinary op of the stream).
        let mut owners: Vec<Option<Delegation>> = Vec::new();
        let mut note = |o: Option<Delegation>| {
            if !owners
                .iter()
                .any(|x| x.map(|d| d.stream()) == o.map(|d| d.stream()))
            {
                owners.push(o);
            }
        };
        for (parent, _) in keys.dentries.iter() {
            note(self.covering(ns, *parent));
        }
        // An exclusive hold on a directory or a file involves the delegation
        // containing it (a file's is its parent's); a shared hold on a
        // directory is that delegation's.
        for ino in keys.inos.iter().chain(keys.shared.iter()) {
            note(self.covering(ns, *ino));
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

/// Plan 43 C2: the configuration of one delegation generation as this
/// replica's table records it: the latest [`LogRecord::DelegateNodeGroup`] applied
/// (`version` 0: none, the group is the delegate and the root), and every
/// backup any record of the generation ever named (`named`, sorted: the
/// seal's best-effort set at the generation's end, C2e). A separate key per
/// generation beside the table's row, so the table's persisted form does
/// not change; written in the same funnel (dirty-tracked, captured, rolled
/// back with the table) and deleted with the generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGroupRow {
    pub version: u64,
    pub backups: Vec<NodeId>,
    pub named: Vec<NodeId>,
}

/// `0x30 | Delegation | "group/" | gen` (big endian).
pub fn group_key(gen: Gen) -> Vec<u8> {
    let mut id = b"group/".to_vec();
    id.extend_from_slice(&gen.to_be_bytes());
    keys::subsystem(keys::Subsystem::Delegation, &id)
}

pub(crate) fn read_group_tx(
    r: &impl Readable,
    meta: &Meta,
    gen: Gen,
) -> Result<Option<NodeGroupRow>, MetaError> {
    match r.get(&meta.ns, group_key(gen))? {
        Some(v) => Ok(Some(crate::record::from_bytes_exact(&v).map_err(|e| {
            MetaError::Invalid(format!("delegation group row: {e}"))
        })?)),
        None => Ok(None),
    }
}

/// Apply a `DelegateNodeGroup` record: a version above the row's replaces it; a
/// replayed or stale one is ignored. `true`: the row changed.
pub(crate) fn apply_group_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    gen: Gen,
    version: u64,
    backups: &[NodeId],
) -> Result<bool, MetaError> {
    let old = read_group_tx(&*tx, meta, gen)?.unwrap_or_default();
    if version <= old.version {
        return Ok(false);
    }
    let mut named = old.named;
    named.extend_from_slice(backups);
    named.sort_unstable();
    named.dedup();
    let row = NodeGroupRow {
        version,
        backups: backups.to_vec(),
        named,
    };
    let bytes = postcard::to_allocvec(&row)
        .map_err(|e| MetaError::Invalid(format!("delegation group row: {e}")))?;
    ns::ns_insert(tx, &meta.ns, dirty, group_key(gen), bytes)?;
    #[cfg(any(test, feature = "test-util"))]
    {
        let mut top = meta.deleg_group_top.lock().unwrap();
        let t = top.entry(gen).or_default();
        *t = (*t).max(version);
        let next = crate::store::journal::peek_next_seq(&*tx, &meta.local)?;
        meta.deleg_group_history
            .lock()
            .unwrap()
            .entry(gen)
            .or_default()
            .push((version, next));
    }
    Ok(true)
}

/// The generation ended: its group row goes with its table row.
pub(crate) fn remove_group_tx(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: ns::Dirty<'_>,
    gen: Gen,
) -> Result<(), MetaError> {
    if tx.get(&meta.ns, group_key(gen))?.is_some() {
        ns::ns_remove(tx, &meta.ns, dirty, group_key(gen))?;
    }
    Ok(())
}

impl Meta {
    /// The live delegation table as this replica knows it: the fold of
    /// every `DelegateNode`/`SubtreeReleased`/`StreamReleased`/
    /// `DelegateNodeEnded` record it applied (its journal
    /// included, on the root). One point read of a small key.
    pub fn delegation_table(&self) -> DelegationTable {
        let r = self.db.read_tx();
        read_table_tx(&r, self).unwrap_or_default()
    }

    /// Plan 43 C2: generation `gen`'s configuration as this replica's table
    /// records it (`None`: no `DelegateNodeGroup` record applied, version 0).
    pub fn deleg_group(&self, gen: Gen) -> Result<Option<NodeGroupRow>, MetaError> {
        #[cfg(any(test, feature = "test-util"))]
        crate::store::backup::injected_read_fault()?;
        let r = self.db.read_tx();
        read_group_tx(&r, self, gen)
    }

    /// Simulation oracle: the highest `DelegateNodeGroup` version of `gen` this
    /// replica ever applied (0: none), kept after the generation ends.
    #[cfg(any(test, feature = "test-util"))]
    pub fn deleg_group_top(&self, gen: Gen) -> u64 {
        self.deleg_group_top
            .lock()
            .unwrap()
            .get(&gen)
            .copied()
            .unwrap_or(0)
    }

    /// Simulation oracle: the `DelegateNodeGroup` version of `gen` in force at the
    /// journal position of the row whose first seq is `first`: the latest
    /// version this replica applied while the journal's next seq was at or
    /// below `first`; `Some(0)` when every recorded version was applied after
    /// that position (the row was journaled while no record was applied: "no
    /// group"). `None`: no version of `gen` was recorded on this replica (none
    /// applied yet, or a replica rebuilt from the log, whose records went
    /// into the rebuilt copy's history).
    #[cfg(any(test, feature = "test-util"))]
    pub fn deleg_group_at(&self, gen: Gen, first: u64) -> Option<u64> {
        let history = self.deleg_group_history.lock().unwrap();
        let applied = history.get(&gen)?;
        Some(
            applied
                .iter()
                .rev()
                .find(|(_, next)| *next <= first)
                .map_or(0, |(v, _)| *v),
        )
    }

    /// Plan 30 §M12: whether the table names any live delegation, as
    /// of the last table write here (an atomic: the FUSE fast path's
    /// pre-check).
    pub fn delegations_any(&self) -> bool {
        self.deleg_any.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Plan 30 §M12: the lock the root's FUSE fast path holds shared
    /// across its ownership check and execution, and a `DelegateNode`/
    /// `DelegateNodeEnded` transaction holds exclusively (see `Meta::deleg_gate`).
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
            let keys = TouchSet::from_op_in_names(
                op,
                &|p, n| {
                    crate::MetaStore::lookup(self, p, n)
                        .ok()
                        .flatten()
                        .map(|a| a.ino)
                },
                &|i| self.names_of_ino(i),
            );
            if !matches!(self.resolve_ownership(&keys), Ownership::Root) {
                return None;
            }
        }
        // Plan 46 M0.2 review: not ahead of a replay-queue entry on its
        // keys (a dropped row's placeholder, or an acknowledged op queued
        // behind one): the core parks the op behind the queue.
        if self.queue_overlaps_op(op) {
            return None;
        }
        Some(RootAdmission(guard))
    }

    /// Plan 30 §M8 under §M11: whether a live delegation owns what a
    /// strict read of `ino` (or of the entry `name` in it) reads, so the
    /// root's replica is not its index: the delegate acknowledges its own
    /// writes once its group holds them, before the root applies its
    /// stream. The keys are the core's (`Core::read_keys`). Costs one
    /// atomic load while the table is empty.
    pub fn read_delegated(&self, ino: Ino, name: Option<&str>) -> bool {
        if !self.delegations_any() {
            return false;
        }
        let mut keys = TouchSet::default();
        match name {
            Some(n) => {
                keys.dentries.insert((ino, n.to_string()));
            }
            None => {
                keys.inos.insert(ino);
            }
        }
        !matches!(self.resolve_ownership(&keys), Ownership::Root)
    }

    /// Who executes an op touching `keys` (see the module doc).
    pub fn resolve_ownership(&self, keys: &TouchSet) -> Ownership {
        let table = self.delegation_table();
        if table.is_empty() {
            return Ownership::Root;
        }
        table.resolve(self, keys)
    }

    /// Mint the next delegation generation number (ADR-43): `epoch << 32 | n`,
    /// `epoch` the lease epoch of the tenure that grants it (the caller's
    /// held S3 lease, one holder per epoch), `n` this node's persisted
    /// counter. Two tenures therefore never mint the same number, in either
    /// direction, even when one of them was deposed without knowing it and
    /// both see the same log: a number names its grant for good, whatever
    /// a node kept of a grant the log never carried.
    ///
    /// A root mints only in its own epoch's block. `at_least` is the floor
    /// the caller knows of (the log's highest number plus one): it covers a
    /// counter this store lost (a wiped state directory re-adopting its
    /// epoch), and a number it, or the persisted counter, names outside the
    /// block — from any source — is refused rather than minted: this
    /// replica's table already names a later tenure's grant, so the caller
    /// is deposed. The counter is on stable storage before the number is
    /// returned: one epoch spans a restart of its holder (a node
    /// re-claiming its own lease keeps its epoch) and a number reached the
    /// stream could otherwise be minted again.
    pub fn next_delegation_gen(&self, epoch: u64, at_least: u64) -> Result<u64, MetaError> {
        if epoch == 0 || epoch > GEN_MAX_EPOCH {
            return Err(MetaError::Invalid(format!(
                "epoch {epoch} cannot mint a generation number"
            )));
        }
        let base = epoch << GEN_EPOCH_SHIFT;
        let top = base | u64::from(u32::MAX);
        let mut tx = self.db.write_tx();
        let cur = crate::store::kv_get_tx(&tx, &self.local, KV_NEXT_DELEG_GEN)?
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let gen = cur.max(at_least).max(base + 1);
        if gen_epoch(gen) != epoch {
            return Err(MetaError::Invalid(format!(
                "generation {gen} is outside epoch {epoch}'s block: not minting"
            )));
        }
        if gen >= top {
            return Err(MetaError::Invalid(format!(
                "the generation numbers of epoch {epoch} are exhausted"
            )));
        }
        crate::store::kv_set_tx(
            &mut tx,
            &self.local,
            KV_NEXT_DELEG_GEN,
            &(gen + 1).to_string(),
        );
        tx.commit()?;
        self.sync()?;
        #[cfg(any(test, feature = "test-util"))]
        mint_hook::fire(gen);
        Ok(gen)
    }
}

/// Tests only (`test-util`): a hook run on the calling thread with every
/// generation number [`Meta::next_delegation_gen`] mints, after its counter
/// is synced: the simulation's uniqueness oracle ("no generation number is
/// minted twice", ADR-43) records them per run (the sim runs a seed on one
/// thread).
#[cfg(any(test, feature = "test-util"))]
pub mod mint_hook {
    use std::cell::RefCell;

    type Hook = Box<dyn FnMut(u64)>;

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    /// Install (or, `None`, remove) this thread's hook.
    pub fn set(hook: Option<Hook>) {
        HOOK.with(|h| *h.borrow_mut() = hook);
    }

    pub(crate) fn fire(gen: u64) {
        let hook = HOOK.with(|h| h.borrow_mut().take());
        if let Some(mut hook) = hook {
            hook(gen);
            HOOK.with(|h| *h.borrow_mut() = Some(hook));
        }
    }
}

/// A generation number's epoch block (ADR-43): `gen = epoch << GEN_EPOCH_SHIFT | n`,
/// `n` the minting node's counter within the tenure.
pub const GEN_EPOCH_SHIFT: u32 = 32;

/// The highest lease epoch that can mint a generation number. The number
/// then stays below `2^61`, so every id space built on `BASE + gen` stays
/// inside its own range and below `2^63`: the delegate backup tail's
/// `1 << 62` (`store::backup`, past the member copies' `2 << 48`), the
/// parked-wait ids' `1 << 62` and the delegates' read-delegation epochs'
/// `1 << 48`. Lease epochs grow by one per acquisition: unreachable.
pub const GEN_MAX_EPOCH: u64 = (1 << 29) - 1;

/// The epoch of the tenure that minted generation number `gen`.
pub fn gen_epoch(gen: Gen) -> u64 {
    gen >> GEN_EPOCH_SHIFT
}

/// A generation number for a log line (ADR-43): `epoch:n` (a plain `n` for
/// an old-mint number, epoch 0), the minting
/// tenure's epoch and its counter within the tenure, where the raw `u64`
/// (`epoch << 32 | n`) reads as noise. `GenLabel::of` takes the number or a
/// reference to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenLabel(pub Gen);

impl GenLabel {
    pub fn of(gen: impl std::borrow::Borrow<Gen>) -> Self {
        GenLabel(*gen.borrow())
    }
}

impl std::fmt::Display for GenLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if gen_epoch(self.0) == 0 {
            // An old-mint number is the plain counter, as the CLI shows it.
            return write!(f, "{}", self.0);
        }
        write!(
            f,
            "{}:{}",
            gen_epoch(self.0),
            self.0 & ((1 << GEN_EPOCH_SHIFT) - 1)
        )
    }
}

/// `local` kv: the root's next delegation generation.
pub(crate) const KV_NEXT_DELEG_GEN: &str = "next_deleg_gen";

impl Meta {
    /// Every name of `ino` as `(parent, name)`, in key order (the first is
    /// its primary one: [`Namespace::primary_parent`]) — or nothing while
    /// no delegation exists, when every key is the root's and a name that
    /// leaves cannot move an inode between owners
    /// ([`TouchSet::from_op_in_names`]).
    pub fn names_of_ino(&self, ino: Ino) -> Vec<(Ino, String)> {
        if ino == ROOT_INO || !self.delegations_any() {
            return Vec::new();
        }
        let r = self.db.read_tx();
        let range = keys::names_of(ino);
        let mut out = Vec::new();
        for guard in r.range(&self.ns, ns::key_range_bounds(&range)) {
            let Ok((k, _)) = guard.into_inner() else {
                break;
            };
            if let Ok(keys::Key::RDentry {
                parent_ino, name, ..
            }) = keys::Key::parse(&k)
            {
                out.push((parent_ino, String::from_utf8_lossy(name).into_owned()));
            }
        }
        out
    }
}

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

    /// A table row of an older shape (a rows-of-dirs layout) is refused,
    /// not decoded shifted.
    #[test]
    fn old_shape_table_row_is_refused() {
        let new = DelegationTable::default();
        assert!(DelegationTable::decode(&new.encode()).is_ok());
        let old_rows: Vec<(Ino, NodeId, Gen, bool, u8, u32)> =
            vec![(7, 2, 5, false, 0, 0), (9, 3, 6, true, 0, 0)];
        let old = postcard::to_allocvec(&(old_rows, 6u64)).unwrap();
        assert!(DelegationTable::decode(&old).is_err());
        let mut trailing = new.encode();
        trailing.push(0);
        assert!(DelegationTable::decode(&trailing).is_err());
    }

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

    fn grant(node: NodeId, gen: Gen, dir: Ino) -> DelegationRecord {
        DelegationRecord::DelegateNode {
            node,
            gen,
            dirs: vec![SubtreeGrant {
                dir,
                kind: SubtreeKind::Placed,
            }],
        }
    }

    fn table() -> DelegationTable {
        let mut t = DelegationTable::default();
        t.apply(grant(7, 1, 2));
        t.apply(grant(8, 2, 3));
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
        t2.apply(grant(9, 3, 4));
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
    fn an_end_ends_only_its_generation_and_an_empty_table_is_free() {
        let ns = Tree::new();
        let mut t = table();
        t.apply(DelegationRecord::DelegateNodeEnded { node: 7, gen: 0 });
        assert!(t.get(2).is_some(), "a stale end changes nothing");
        t.apply(DelegationRecord::DelegateNodeEnded { node: 7, gen: 1 });
        assert!(t.get(2).is_none());
        assert_eq!(t.resolve(&ns, &dentry(4, "x")), Ownership::Root);
        let empty = DelegationTable::default();
        assert_eq!(empty.resolve(&ns, &dentry(4, "x")), Ownership::Root);
    }

    fn grow(node: NodeId, gen: Gen, dirs: &[(Ino, SubtreeKind)]) -> DelegationRecord {
        DelegationRecord::DelegateNode {
            node,
            gen,
            dirs: dirs
                .iter()
                .map(|&(dir, kind)| SubtreeGrant { dir, kind })
                .collect(),
        }
    }

    /// M1.2 step 8a: a node holds subtrees `/D1` (2) and `/D2` (3) in one
    /// stream. A grow adds a subtree to the live stream (same gen), a
    /// partial release shrinks it, an end removes it; the walk resolves a
    /// dentry under either subtree to the one stream, and an op between the
    /// two subtrees is the stream's own (not cross-subtree).
    #[test]
    fn the_table_folds_a_grow_a_partial_release_and_an_end() {
        use SubtreeKind::{Designated, Placed, Sticky};
        let ns = Tree::new();
        let mut t = DelegationTable::default();
        t.apply(grow(7, 5, &[(2, Placed)]));
        assert_eq!(t.stream(7, 5).unwrap().subtrees, vec![(2, Placed)]);
        // The grow: same node, same gen, one more subtree.
        t.apply(grow(7, 5, &[(3, Designated)]));
        assert_eq!(
            t.stream(7, 5).unwrap().subtrees,
            vec![(2, Placed), (3, Designated)]
        );
        assert_eq!(t.streams().count(), 1, "one stream, not two");
        assert_eq!(t.max_gen(), 5);
        // The walk: a dentry under either subtree is the one stream's.
        let under_d3 = t.covering(&ns, 4).unwrap();
        assert_eq!((under_d3.node, under_d3.gen, under_d3.dir), (7, 5, 2));
        assert!(!under_d3.designated());
        let under_d2 = t.covering(&ns, 3).unwrap();
        assert_eq!((under_d2.node, under_d2.gen, under_d2.dir), (7, 5, 3));
        assert!(under_d2.designated());
        assert_eq!(t.owner_of_dir(&ns, 4), Some((7, 5)));
        assert_eq!(t.owner_of_dentry(&ns, 3), Some((7, 5)));
        assert!(matches!(
            t.resolve(&ns, &dentry(4, "x")),
            Ownership::Delegated(Delegation {
                node: 7,
                gen: 5,
                ..
            })
        ));
        // A rename between the two subtrees is one owner: no recall.
        let mut between = dentry(4, "a");
        between.dentries.insert((3, "b".into()));
        assert!(matches!(
            t.resolve(&ns, &between),
            Ownership::Delegated(Delegation {
                node: 7,
                gen: 5,
                ..
            })
        ));
        // Into the root directory it is still cross-subtree.
        let mut up = dentry(4, "a");
        up.dentries.insert((1, "b".into()));
        assert!(matches!(
            t.resolve(&ns, &up),
            Ownership::CrossSubtree {
                root_owned: true,
                ..
            }
        ));
        // A release by another stream, or of a subtree the stream does not
        // hold, changes nothing.
        t.apply(DelegationRecord::SubtreeReleased {
            node: 8,
            gen: 5,
            dir: 2,
        });
        t.apply(DelegationRecord::SubtreeReleased {
            node: 7,
            gen: 5,
            dir: 9,
        });
        assert_eq!(t.stream(7, 5).unwrap().subtrees.len(), 2);
        // The partial release: only `/D1` leaves; the walk finds the root
        // there and the stream under `/D2`.
        t.apply(DelegationRecord::SubtreeReleased {
            node: 7,
            gen: 5,
            dir: 2,
        });
        assert_eq!(t.stream(7, 5).unwrap().subtrees, vec![(3, Designated)]);
        assert_eq!(t.resolve(&ns, &dentry(4, "x")), Ownership::Root);
        assert!(matches!(
            t.resolve(&ns, &dentry(3, "x")),
            Ownership::Delegated(Delegation {
                node: 7,
                gen: 5,
                ..
            })
        ));
        // A release of everything leaves the stream live, with no subtree,
        // until the root ends it.
        t.apply(DelegationRecord::StreamReleased { node: 7, gen: 5 });
        assert!(t.stream(7, 5).unwrap().subtrees.is_empty());
        assert!(!t.is_empty());
        assert_eq!(t.resolve(&ns, &dentry(3, "x")), Ownership::Root);
        t.apply(grow(7, 5, &[(2, Placed)]));
        assert!(t.get(2).is_some(), "a stream grows again after a release");
        // The end removes the stream and its subtrees; the number stays
        // minted.
        t.apply(DelegationRecord::DelegateNodeEnded { node: 7, gen: 5 });
        assert!(t.is_empty());
        assert!(t.get(2).is_none());
        assert_eq!(t.max_gen(), 5);
        // The persisted row round-trips a multi-subtree stream.
        let mut t = DelegationTable::default();
        t.apply(grow(7, 5, &[(2, Placed), (3, Designated), (4, Sticky)]));
        t.apply(grow(8, 6, &[]));
        let sticky = t.get(4).unwrap();
        assert_eq!(sticky.kind, Sticky);
        assert!(!sticky.designated());
        assert_eq!(DelegationTable::decode(&t.encode()).unwrap(), t);
    }
}
