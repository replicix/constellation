//! Plan 46 M1.0: one stream per delegate node, the commit rule per
//! configuration — the `NodeStream` model.
//!
//! # Why a model of its own
//!
//! [`crate::delegation`] (plan 30 M11, plan 43 C2) models a delegation per
//! directory and generation, with the clock discipline of placed grants
//! (honour and outwait under bounded drift). Plan 46 stage 1 changes the
//! *structure*: a delegation is a grant to a **node** covering a set of
//! subtrees, the node has **one stream** `(gen, idx)`, one group and one
//! cut, and the commit rule is a property of the stream's **configuration**
//! (`RowCommit::{Group { config_version }, Final}`), never decided per row.
//! This module is that structure, built up chunk by chunk (the work
//! breakdown is in the plan's M1.0 notes); the clock margins it does not
//! re-prove are `delegation`'s, unchanged by stage 1 for placed grants.
//!
//! # What is modeled (chunk 1: the skeleton)
//!
//! - **Namespace.** [`crate::delegation`]'s keys (`(dir, name)`, one
//!   presence bit each) and its `eval`. Directory 0 is `/`; a grant covers
//!   directories `1..` (children of `/`), as a bitmask.
//! - **Streams.** Stream 0 ([`ROOT`]) is the root's journal: its own rows
//!   and, verbatim with their origin `(stream, idx)`, the rows it appended
//!   from delegate streams. Stream `k + 1` is the stream of
//!   [`NodeStreamModel::grants`]`[k]`, a `DelegateNode { node, gen, dirs }`
//!   grant with one [`GrantKind`] and one recorded backup set
//!   (`DelegateNodeGroup`).
//! - **Configurations.** One per stream in chunk 1 (version 0); every row
//!   carries the version it was journaled under, and the rule is looked up
//!   from it ([`NodeStreamModel::rule`]), never from mutable state:
//!   * the root: committed when every listed backup holds the row, or
//!     with none listed (and peers present, so not a singleton) once the
//!     archive carries it;
//!   * a placed grant with a recorded backup: every backup holds it;
//!   * a placed grant with none (the group `{delegate, root}`): the root's
//!     appended copy, **committed by the root's own configuration**
//!     (ADR-41's rule with its lone-journal branch removed: no root with a
//!     delegate is lone);
//!   * a sticky grant or a designation: `Final` (the holder's disk);
//!   * any row, once the archive carries it (its appended copy shipped).
//! - **Replication** is monotone prefix copying (`held`, `shipped`, the
//!   root's appended cursor): streams are ordered and resent until
//!   acknowledged (L3), so a lost or reordered message is only a delay,
//!   which an interleaving already covers. A member acknowledges what it
//!   holds atomically with holding it; the root's report of its committed
//!   copy to a delegate (`DelegateStreamAck.committed`) is a step of its
//!   own ([`Action::RootAck`]) because what the root counts is a rule under
//!   test.
//! - **Clients.** One outstanding op per node, scripted. An op's keys
//!   resolve to one stream from the (static, in chunk 1) grant table; the
//!   op runs at that stream's sequencer, locally or forwarded. Ops whose
//!   keys span streams (cross-subtree: recall by request) are chunk 4's.
//!
//! Faults, reconfiguration, release, recall, TTL ends, sticky release and
//! abandon, the singleton and the first peer's join come in later chunks.
//!
//! # Properties (chunk 1)
//!
//! - `ack_implies_committed_by_config` (I1, plan 46 §5.9): at the moment a
//!   sequencer emits a definitive answer, the row is committed by the
//!   configuration in force at its journal position, judged from the
//!   stores (`held`, `shipped`, the root's journal), never from the
//!   answerer's knowledge. The executor decides from what it was told
//!   ([`Stream::root_committed`]), so the oracle and the gate are
//!   separate code.
//!
//! # Knobs (each a planted bug the checker must find)
//!
//! - `lone_delegate`: a placed delegate answers from its own disk (the
//!   deleted `Lone` class of a delegate with peers).
//! - `root_journal_counts`: the root reports its appended copy of a
//!   delegate's row as committed once it is in its journal (ADR-41's
//!   lone-journal branch, `u64::MAX`), not once its own configuration
//!   committed it.

use crate::delegation::{dir_of, eval, Ret, Rid, View, WriteOp};
use stateright::{Model, Property};

/// The root's journal's stream id.
pub const ROOT: u8 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GrantKind {
    /// Placed by the root; ends by the holder's release or the TTL.
    Placed,
    /// Non-stealable; ended only by the holder's release or the operator.
    Sticky,
    /// An offline designation; released only by `online`.
    Designated,
}

/// A `DelegateNode` grant with its `DelegateNodeGroup` (chunk 1: static).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeGrant {
    pub node: u8,
    /// Bitmask over directories `1..` (bit `d` covers directory `d`).
    pub dirs: u8,
    pub kind: GrantKind,
    /// Bitmask over nodes: the stream's recorded backups.
    pub backups: u8,
}

/// A configuration's commit rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Rule {
    /// Every node in `members` holds the row; `via_root`: the root's
    /// appended copy, committed by the root's configuration, counts as the
    /// second copy; neither (no member, not via the root): the archive.
    Group { members: u8, via_root: bool },
    /// The holder's disk: a non-stealable configuration.
    Final,
}

/// A journaled row. In the root's journal an appended row keeps its
/// origin `(stream, idx)` verbatim; the root's own rows have `stream ==
/// ROOT` and `idx` their position in the root's journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Row {
    pub stream: u8,
    pub idx: u8,
    pub rid: Rid,
    pub op: WriteOp,
    pub ret: Ret,
    /// The configuration version the row was journaled under.
    pub version: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Stream {
    /// At the sequencer: the root's journal (stream 0) or the delegate's.
    pub rows: Vec<Row>,
    /// Per node: the prefix of `rows` it holds as a member copy.
    pub held: Vec<u8>,
    /// Delegate streams: the prefix of `rows` the root reported committed
    /// in its copy (`DelegateStreamAck.committed`).
    pub root_committed: u8,
}

/// An answer parked at its executor until the row is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Pending {
    pub executor: u8,
    pub stream: u8,
    pub idx: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Msg {
    Fwd { to: u8, rid: Rid, op: WriteOp },
    Reply { rid: Rid, ret: Ret },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The script index of the next op to start.
    pub next: u8,
    pub outstanding: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Violation {
    pub property: &'static str,
    pub rid: Rid,
    pub stream: u8,
    pub idx: u8,
}

/// A delegate answered through the root's committed copy.
pub const SAW_VIA_ROOT: u8 = 1;
/// A delegate answered through its recorded backup.
pub const SAW_DELEG_BACKUP: u8 = 2;
/// A non-stealable stream answered (`Final`).
pub const SAW_FINAL: u8 = 4;
/// An answer went to another node (a forward).
pub const SAW_FORWARD: u8 = 8;
/// The root answered its own row through its backups, before the archive.
pub const SAW_ROOT_GROUP: u8 = 16;
/// The root answered its own row once the archive carried it.
pub const SAW_ARCHIVE: u8 = 32;
/// An answer whose row (or its appended copy) the archive did not carry.
pub const SAW_BEFORE_ARCHIVE: u8 = 64;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub nodes: Vec<Node>,
    pub streams: Vec<Stream>,
    /// The prefix of the root's journal the archive carries.
    pub shipped: u8,
    /// The namespace as the sequencers validated it. Ownership is static
    /// in chunk 1 and each key has one sequencer, so one view is every
    /// sequencer's view of its own keys.
    pub view: View,
    pub pending: Vec<Pending>,
    pub net: Vec<Msg>,
    /// Definitive answers the clients received.
    pub answered: Vec<(Rid, Ret)>,
    pub violation: Option<Violation>,
    pub saw: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// Node `i`'s client starts its next op.
    Start(u8),
    /// Deliver `net[k]`.
    Deliver(usize),
    /// `member` copies the next row of `stream`.
    Replicate { stream: u8, member: u8 },
    /// The root appends the next row of delegate stream `g`, verbatim.
    Append(u8),
    /// The root ships its journal to the archive.
    Ship,
    /// The root tells delegate stream `g` how far its copy is committed.
    RootAck(u8),
    /// The executor of `pending[k]` answers it.
    Answer(usize),
}

#[derive(Clone, Debug)]
pub struct NodeStreamModel {
    /// Node 0 is the root (the lease holder).
    pub n: u8,
    pub scripts: Vec<Vec<WriteOp>>,
    pub initial_present: View,
    /// Bitmask over nodes: the lease's listed backups.
    pub root_backups: u8,
    /// Stream `k + 1` is `grants[k]`.
    pub grants: Vec<NodeGrant>,
    pub lone_delegate: bool,
    pub root_journal_counts: bool,
}

impl NodeStreamModel {
    /// The design: node 0 roots, no grant, no backup, the rules on.
    pub fn design(scripts: Vec<Vec<WriteOp>>) -> Self {
        assert!(
            scripts.len() >= 2,
            "a root with no peer is the singleton (a later chunk)"
        );
        NodeStreamModel {
            n: scripts.len() as u8,
            scripts,
            initial_present: 0,
            root_backups: 0,
            grants: Vec::new(),
            lone_delegate: false,
            root_journal_counts: false,
        }
    }

    pub fn with_root_backups(mut self, backups: u8) -> Self {
        assert_eq!(backups & 1, 0, "the root is not its own backup");
        self.root_backups = backups;
        self
    }

    pub fn with_grant(mut self, g: NodeGrant) -> Self {
        // Chunk 1: delegates are other nodes (the root's own sticky grants
        // are a later chunk), on disjoint subtree sets, never `/`.
        assert!(g.node != 0 && g.node < self.n);
        assert_eq!(g.dirs & 1, 0, "never a grant on `/`");
        assert_eq!(g.backups & (1 << g.node), 0);
        assert!(self
            .grants
            .iter()
            .all(|o| o.dirs & g.dirs == 0 && o.node != g.node));
        if g.kind != GrantKind::Placed {
            assert_eq!(g.backups, 0, "a non-stealable stream has no group");
        }
        self.grants.push(g);
        self
    }

    pub fn with_initial(mut self, present: View) -> Self {
        self.initial_present = present;
        self
    }

    fn n(&self) -> usize {
        self.n as usize
    }

    /// The sequencer of a stream.
    pub fn executor(&self, stream: u8) -> u8 {
        if stream == ROOT {
            0
        } else {
            self.grants[stream as usize - 1].node
        }
    }

    /// The commit rule of `stream`'s configuration `version` (chunk 1: one
    /// version per stream). No lone delegate: a placed grant with no
    /// recorded backup has the root as its second member.
    pub fn rule(&self, stream: u8, _version: u8) -> Rule {
        if stream == ROOT {
            return Rule::Group {
                members: self.root_backups,
                via_root: false,
            };
        }
        let g = self.grants[stream as usize - 1];
        match g.kind {
            GrantKind::Placed => Rule::Group {
                members: g.backups,
                via_root: g.backups == 0,
            },
            GrantKind::Sticky | GrantKind::Designated => Rule::Final,
        }
    }

    /// The stream holding `dir`.
    fn stream_of_dir(&self, dir: u8) -> u8 {
        if dir == 0 {
            return ROOT;
        }
        self.grants
            .iter()
            .position(|g| g.dirs & (1 << dir) != 0)
            .map_or(ROOT, |k| k as u8 + 1)
    }

    /// The stream every key of `op` belongs to, or `None` when they span
    /// streams (cross-subtree: chunk 4).
    pub fn stream_of(&self, op: WriteOp) -> Option<u8> {
        let mut streams = op.keys().into_iter().map(|k| self.stream_of_dir(dir_of(k)));
        let first = streams.next()?;
        streams.all(|s| s == first).then_some(first)
    }

    fn members(&self, stream: u8) -> u8 {
        match self.rule(stream, 0) {
            Rule::Group { members, .. } => members,
            Rule::Final => 0,
        }
    }
}

fn bits(mask: u8) -> impl Iterator<Item = usize> {
    (0..8).filter(move |b| mask & (1 << b) != 0)
}

/// The root journal's position of the row `(stream, idx)`, if appended.
fn root_pos(s: &State, stream: u8, idx: u8) -> Option<usize> {
    s.streams[ROOT as usize]
        .rows
        .iter()
        .position(|r| r.stream == stream && r.idx == idx)
}

/// Oracle: the root's journal row at `p` is committed by the root's
/// configuration (the version it was journaled under), from the stores.
fn root_pos_committed(m: &NodeStreamModel, s: &State, p: usize) -> bool {
    if s.shipped as usize > p {
        return true;
    }
    let row = s.streams[ROOT as usize].rows[p];
    // An appended row is journaled by the root under the root's
    // configuration in force at its position; chunk 1 has one.
    let version = if row.stream == ROOT { row.version } else { 0 };
    match m.rule(ROOT, version) {
        Rule::Group { members, .. } => {
            members != 0 && bits(members).all(|b| s.streams[ROOT as usize].held[b] as usize > p)
        }
        Rule::Final => true,
    }
}

/// Oracle (I1): row `idx` of `stream` is committed by the configuration in
/// force at its journal position, judged from the stores alone.
pub fn committed(m: &NodeStreamModel, s: &State, stream: u8, idx: u8) -> bool {
    if stream == ROOT {
        return root_pos_committed(m, s, idx as usize);
    }
    let rp = root_pos(s, stream, idx);
    if rp.is_some_and(|p| s.shipped as usize > p) {
        return true;
    }
    let row = s.streams[stream as usize].rows[idx as usize];
    match m.rule(stream, row.version) {
        Rule::Final => true,
        Rule::Group { members, via_root } => {
            let held = bits(members).all(|b| s.streams[stream as usize].held[b] > idx);
            let root = !via_root || rp.is_some_and(|p| root_pos_committed(m, s, p));
            (members != 0 || via_root) && held && root
        }
    }
}

/// What the root reports as the committed prefix of its copy of stream `g`
/// (`DelegateStreamAck.committed`), from its own state.
fn root_reported(m: &NodeStreamModel, s: &State, g: u8) -> u8 {
    let mut k = 0u8;
    while let Some(p) = root_pos(s, g, k) {
        if !(m.root_journal_counts || root_pos_committed(m, s, p)) {
            break;
        }
        k += 1;
    }
    k
}

/// The executor's gate: may it answer row `idx` of `stream` now, from what
/// it knows (its members' acknowledgements, the root's report, the
/// archive's cursor for the root)?
fn may_answer(m: &NodeStreamModel, s: &State, stream: u8, idx: u8) -> bool {
    if stream == ROOT {
        // The root knows its backups' acknowledgements and its shipping.
        return root_pos_committed(m, s, idx as usize);
    }
    let st = &s.streams[stream as usize];
    let row = st.rows[idx as usize];
    match m.rule(stream, row.version) {
        Rule::Final => true,
        Rule::Group { .. } if m.lone_delegate => true,
        Rule::Group { members, via_root } => {
            bits(members).all(|b| st.held[b] > idx) && (!via_root || st.root_committed > idx)
        }
    }
}

fn execute(m: &NodeStreamModel, s: &mut State, stream: u8, rid: Rid, op: WriteOp) {
    let (ret, view) = eval(s.view, op);
    s.view = view;
    let st = &mut s.streams[stream as usize];
    let idx = st.rows.len() as u8;
    st.rows.push(Row {
        stream,
        idx,
        rid,
        op,
        ret,
        version: 0,
    });
    s.pending.push(Pending {
        executor: m.executor(stream),
        stream,
        idx,
    });
}

impl Model for NodeStreamModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        vec![State {
            nodes: vec![
                Node {
                    next: 0,
                    outstanding: false,
                };
                n
            ],
            streams: (0..=self.grants.len())
                .map(|_| Stream {
                    rows: Vec::new(),
                    held: vec![0; n],
                    root_committed: 0,
                })
                .collect(),
            shipped: 0,
            view: self.initial_present,
            pending: Vec::new(),
            net: Vec::new(),
            answered: Vec::new(),
            violation: None,
            saw: 0,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        // A violation is a terminal state for the search.
        if s.violation.is_some() {
            return;
        }
        for i in 0..self.n() {
            let node = s.nodes[i];
            if !node.outstanding {
                if let Some(op) = self.scripts[i].get(node.next as usize) {
                    if self.stream_of(*op).is_some() {
                        actions.push(Action::Start(i as u8));
                    }
                }
            }
        }
        for k in 0..s.net.len() {
            actions.push(Action::Deliver(k));
        }
        for (stream, st) in s.streams.iter().enumerate() {
            let stream = stream as u8;
            for b in bits(self.members(stream)) {
                if (st.held[b] as usize) < st.rows.len() {
                    actions.push(Action::Replicate {
                        stream,
                        member: b as u8,
                    });
                }
            }
            if stream != ROOT {
                let appended = s.streams[ROOT as usize]
                    .rows
                    .iter()
                    .filter(|r| r.stream == stream)
                    .count();
                if appended < st.rows.len() {
                    actions.push(Action::Append(stream));
                }
                let via_root = matches!(self.rule(stream, 0), Rule::Group { via_root: true, .. });
                if via_root && root_reported(self, s, stream) > st.root_committed {
                    actions.push(Action::RootAck(stream));
                }
            }
        }
        if (s.shipped as usize) < s.streams[ROOT as usize].rows.len() {
            actions.push(Action::Ship);
        }
        for (k, p) in s.pending.iter().enumerate() {
            if may_answer(self, s, p.stream, p.idx) {
                actions.push(Action::Answer(k));
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Start(i) => {
                let node = &mut s.nodes[i as usize];
                let op = self.scripts[i as usize][node.next as usize];
                let rid = Rid {
                    node: i,
                    seq: node.next,
                };
                node.next += 1;
                node.outstanding = true;
                let stream = self.stream_of(op)?;
                let to = self.executor(stream);
                if to == i {
                    execute(self, &mut s, stream, rid, op);
                } else {
                    s.net.push(Msg::Fwd { to, rid, op });
                }
            }
            Action::Deliver(k) => match s.net.remove(k) {
                Msg::Fwd { rid, op, .. } => {
                    let stream = self.stream_of(op)?;
                    execute(self, &mut s, stream, rid, op);
                }
                Msg::Reply { rid, ret } => {
                    s.nodes[rid.node as usize].outstanding = false;
                    s.answered.push((rid, ret));
                }
            },
            Action::Replicate { stream, member } => {
                s.streams[stream as usize].held[member as usize] += 1;
            }
            Action::Append(g) => {
                let appended = s.streams[ROOT as usize]
                    .rows
                    .iter()
                    .filter(|r| r.stream == g)
                    .count();
                let row = s.streams[g as usize].rows[appended];
                s.streams[ROOT as usize].rows.push(row);
            }
            Action::Ship => {
                s.shipped = s.streams[ROOT as usize].rows.len() as u8;
            }
            Action::RootAck(g) => {
                s.streams[g as usize].root_committed = root_reported(self, &s, g);
            }
            Action::Answer(k) => {
                let p = s.pending.remove(k);
                let row = s.streams[p.stream as usize].rows[p.idx as usize];
                // The oracle, at emission, from the stores.
                if !committed(self, &s, p.stream, p.idx) {
                    s.violation = Some(Violation {
                        property: "ack_implies_committed_by_config",
                        rid: row.rid,
                        stream: p.stream,
                        idx: p.idx,
                    });
                }
                let in_archive = if p.stream == ROOT {
                    (s.shipped as usize) > p.idx as usize
                } else {
                    root_pos(&s, p.stream, p.idx).is_some_and(|q| s.shipped as usize > q)
                };
                if !in_archive {
                    s.saw |= SAW_BEFORE_ARCHIVE;
                }
                s.saw |= match self.rule(p.stream, row.version) {
                    Rule::Final => SAW_FINAL,
                    Rule::Group { via_root: true, .. } => SAW_VIA_ROOT,
                    Rule::Group { .. } if p.stream != ROOT => SAW_DELEG_BACKUP,
                    Rule::Group { .. } if in_archive => SAW_ARCHIVE,
                    Rule::Group { .. } => SAW_ROOT_GROUP,
                };
                if row.rid.node == p.executor {
                    s.nodes[row.rid.node as usize].outstanding = false;
                    s.answered.push((row.rid, row.ret));
                } else {
                    s.saw |= SAW_FORWARD;
                    s.net.push(Msg::Reply {
                        rid: row.rid,
                        ret: row.ret,
                    });
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("ack_implies_committed_by_config", |_, s: &State| {
                s.violation.is_none()
            }),
            Property::sometimes("all_answered", |m: &NodeStreamModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| !n.outstanding && n.next as usize == sc.len())
            }),
            Property::sometimes("forward_answered", |_, s: &State| s.saw & SAW_FORWARD != 0),
        ];
        // Reachable only with a group that is not the archive: a root
        // backup, a delegate's backup, or a non-stealable stream. With
        // neither, a placed delegate's second copy (the root's) commits
        // only by the archive (the root's own rule).
        if self.root_backups != 0
            || self
                .grants
                .iter()
                .any(|g| g.backups != 0 || g.kind != GrantKind::Placed)
        {
            props.push(Property::sometimes(
                "answered_before_archive",
                |_, s: &State| s.saw & SAW_BEFORE_ARCHIVE != 0,
            ));
        }
        if self.root_backups != 0 {
            props.push(Property::sometimes("root_group_answer", |_, s: &State| {
                s.saw & SAW_ROOT_GROUP != 0
            }));
        }
        let has = |f: fn(&NodeGrant) -> bool| self.grants.iter().any(f);
        if has(|g| g.kind == GrantKind::Placed && g.backups == 0) {
            props.push(Property::sometimes(
                "delegate_answer_via_root",
                |_, s: &State| s.saw & SAW_VIA_ROOT != 0,
            ));
        }
        if has(|g| g.kind == GrantKind::Placed && g.backups != 0) {
            props.push(Property::sometimes(
                "delegate_answer_via_backup",
                |_, s: &State| s.saw & SAW_DELEG_BACKUP != 0,
            ));
        }
        if has(|g| g.kind != GrantKind::Placed) {
            props.push(Property::sometimes("final_answer", |_, s: &State| {
                s.saw & SAW_FINAL != 0
            }));
        }
        props
    }
}
