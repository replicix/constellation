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
//! # What is modeled
//!
//! ## Chunk 1: the skeleton
//!
//! - **Namespace.** [`crate::delegation`]'s keys (`(dir, name)`, one
//!   presence bit each) and its `eval`. Directory 0 is `/`; a grant covers
//!   directories `1..` (children of `/`), as a bitmask.
//! - **Streams.** Stream 0 ([`ROOT`]) is the root's journal: its own rows
//!   and, verbatim with their origin `(stream, idx)`, the rows it appended
//!   from delegate streams. Stream `k + 1` is the stream of
//!   [`NodeStreamModel::grants`]`[k]`, a `DelegateNode { node, gen, dirs }`
//!   grant with one [`GrantKind`] and a recorded backup set
//!   (`DelegateNodeGroup`). The grants are static: their records are in
//!   the archive before the first step (Q4's premise, honoured once
//!   committed; issuing and ending grants are later chunks).
//! - **Configurations.** Each stream has a list of [`Cfg`]s; a row carries
//!   the version (index) it was journaled under, and the rule is looked up
//!   from the configuration, never from mutable state:
//!   * the root: committed when every listed backup holds the row, or with
//!     none listed (and peers present, so not a singleton) once the
//!     archive carries it;
//!   * a placed grant with recorded backups: every backup holds it;
//!   * a placed grant with none (the group `{delegate, root}`): the root's
//!     appended copy, **committed by the root's own configuration**
//!     (ADR-41's rule with its lone-journal branch removed: the maintainer's
//!     decision of 2026-11-05);
//!   * a sticky grant or a designation: `Final` (the holder's disk);
//!   * any row, once the archive carries it (its appended copy shipped).
//!
//!   Rows commit in stream order: a row is committed when it and every
//!   earlier row of its stream satisfy their own rules (which matters once
//!   a stream has two configurations).
//! - **Replication** is monotone prefix copying (`held`, `shipped`, the
//!   root's appended cursor): streams are ordered and resent until
//!   acknowledged (L3), so a lost or reordered message is only a delay,
//!   which an interleaving already covers. A member acknowledges what it
//!   holds atomically with holding it; the root's report of its committed
//!   copy to a delegate (`DelegateStreamAck.committed`) is a step of its
//!   own ([`Action::RootAck`]) because what the root counts is a rule under
//!   test.
//! - **Clients.** One outstanding op per node, scripted. An op's keys
//!   resolve to one stream from the grant table; the op runs at that
//!   stream's sequencer, locally or forwarded. Ops whose keys span streams
//!   (cross-subtree: recall by request) are chunk 4's.
//!
//! ## Chunk 2: crashes and root takeover
//!
//! - **Crash** ([`Action::Crash`], at most [`NodeStreamModel::max_crashes`]):
//!   a fail-stop of a node that is not the root; its disk is kept (its
//!   copies still count for the definitional oracle) but it takes no step,
//!   receives nothing and ships nothing. The root's failure is a deposition.
//! - **Deposition** ([`Action::Depose`], at most
//!   [`NodeStreamModel::max_depositions`]): the root pauses and a claimant
//!   takes the lease (abstract TTL: the claim is enabled only once the
//!   paused root has stopped honouring, `delegation`'s argument). The
//!   claim needs the seal: with backups listed, the claimant is a live
//!   backup and adopts its held prefix of the journal (at least the
//!   archive); with none, any live node that holds no grant (a delegate
//!   taking the root is chunk 6's) adopts the archive alone. The deposed
//!   root never resumes as root (its PUTs are fenced), so its parked
//!   answers die with it; the new epoch's configuration is the lease's
//!   (written by the claim's conditional PUT, so it needs no record), its
//!   backups the old ones less the claimant and the deposed root. The new
//!   root rebuilds its keys from the adopted journal and **re-streams**
//!   each delegate stream from the delegate's rows (the appended cursor is
//!   what its journal carries).
//! - **Seal of a crashed delegate's stream**: the root appends a crashed
//!   delegate's rows from a live member of a configuration it knows (the
//!   grant's, or one whose record it has appended).
//! - **Retry by rid** ([`Action::Retry`]): a requester whose op went to a
//!   node that is down resends it, same rid, to the stream's sequencer in
//!   force. **Completed-dedup**: a sequencer that finds the rid in its
//!   stream parks an answer for that row instead of executing again.
//!
//! ## Chunk 3: reconfiguration under old ∪ new
//!
//! - [`Action::Reconfig`]: a planned change of one stream's backups
//!   ([`NodeStreamModel::reconfigs`], at most one per stream): a delegate's
//!   `DelegateNodeGroup` version change, or the root lease's configuration
//!   change. The sequencer journals a **record** row; rows from the record
//!   on carry the new version. The record commits under **old ∪ new**
//!   (both rules), every later row under the new rule, and, with commit in
//!   stream order, no row after the record is committed before the record
//!   is. **Add-then-record**: an added backup copies the stream as a
//!   learner and the record is journaled only once it holds all of it.
//!   **Remove-when-covered**: a removed backup still has to hold the
//!   record (old ∪ new); a dead one is covered by the archive.
//! - [`Action::TogglePeer`]: the environment's "peer within budget" flag
//!   flips; under the design it changes no row (a change of group is a
//!   reconfiguration).
//!
//! # Properties
//!
//! - `ack_implies_committed_by_config` (I1, plan 46 §5.9): at the moment a
//!   sequencer emits a definitive answer, the row is committed by the
//!   configuration in force at its journal position, judged from the
//!   stores (`held`, `shipped`, the root's journal, the configuration
//!   list), never from the answerer's knowledge or the row's own version
//!   stamp. The executor decides from what it was told
//!   ([`Stream::root_committed`], its members' acknowledgements) and the
//!   row's version, so the oracle and the gate are separate code.
//! - `acked_never_lost` (chunk 2, I1's consequence): every answered row is
//!   in the root's journal (live by construction: the root fails only by a
//!   deposition with a successor), or held by a live node from which the
//!   root will append it (the delegate, or a member the root knows of).
//! - `exactly_once` (chunk 2, I2(a)): no rid has two rows.
//! - `row_version_matches_config` (chunk 3, M1.1's oracle in model form):
//!   every row's version is the configuration in force at its position.
//!
//! # Knobs (each a planted bug the checker must find)
//!
//! - `lone_delegate`: a placed delegate answers from its own disk (the
//!   deleted `Lone` class of a delegate with peers).
//! - `root_journal_counts`: the root reports its appended copy of a
//!   delegate's row as committed once it is in its journal (ADR-41's
//!   lone-journal branch, `u64::MAX`), not once its own configuration
//!   committed it.
//! - `no_dedup`: a sequencer executes a retried rid again (I2(a)).
//! - `per_row_class`: a delegate stamps each row with a class from the
//!   mutable "peer within budget" flag at journal time (no peer: `Lone`,
//!   answered from disk) instead of the configuration's version.
//! - `ack_by_latest_config`: a delegate gates a row on the latest
//!   configuration's rule at answer time, not the rule of the row's version.
//! - `check_i1 = false` turns the I1 oracle off, so that a knob's harm must
//!   be shown by `acked_never_lost`, not by I1's definition.

use crate::delegation::{dir_of, eval, Ret, Rid, View, WriteOp};
use stateright::{Model, Property};

/// The root's journal's stream id.
pub const ROOT: u8 = 0;
/// `Rid::node` of a configuration record (records answer no requester).
pub const RECORD_NODE: u8 = 0xFF;
/// The `per_row_class` knob's stamp: a row classed `Lone` at journal time.
pub const LONE: u8 = 0xFE;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GrantKind {
    /// Placed by the root; ends by the holder's release or the TTL.
    Placed,
    /// Non-stealable; ended only by the holder's release or the operator.
    Sticky,
    /// An offline designation; released only by `online`.
    Designated,
}

/// A `DelegateNode` grant with its initial `DelegateNodeGroup`.
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

/// One configuration of a stream: in force for its rows from `start` on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cfg {
    pub start: u8,
    /// Bitmask over nodes: the listed backups.
    pub backups: u8,
    /// Begun by a record row at `start` (which commits under old ∪ new);
    /// `false` for the initial configuration and a lease claim's.
    pub record: bool,
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
    /// The configuration version the row was journaled under (an index
    /// into its stream's [`Stream::cfgs`]), or [`LONE`] (a knob).
    pub version: u8,
    /// A configuration record (no op, no requester).
    pub record: bool,
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
    /// Delegate streams: the prefix of `rows` the root reported archived
    /// (`DelegateStreamAck`'s shipped cursor): committed under any rule,
    /// which is how a dead member is covered (remove-when-covered).
    pub root_archived: u8,
    /// The configurations, by version; sorted by `start`.
    pub cfgs: Vec<Cfg>,
    /// The planned reconfiguration ran.
    pub reconfigured: bool,
}

/// An answer parked at its executor until the row is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pending {
    pub executor: u8,
    pub stream: u8,
    pub idx: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Fwd { to: u8, rid: Rid, op: WriteOp },
    Reply { rid: Rid, ret: Ret },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The script index of the next op to start.
    pub next: u8,
    pub outstanding: bool,
    /// Where the outstanding op was last sent (the sequencer).
    pub sent_to: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Violation {
    pub property: &'static str,
    pub rid: Rid,
    pub stream: u8,
    pub idx: u8,
}

/// A delegate answered through the root's committed copy.
pub const SAW_VIA_ROOT: u16 = 1;
/// A delegate answered through its recorded backup.
pub const SAW_DELEG_BACKUP: u16 = 2;
/// A non-stealable stream answered (`Final`).
pub const SAW_FINAL: u16 = 4;
/// An answer went to another node (a forward).
pub const SAW_FORWARD: u16 = 8;
/// The root answered its own row through its backups, before the archive.
pub const SAW_ROOT_GROUP: u16 = 16;
/// The root answered its own row once the archive carried it.
pub const SAW_ARCHIVE: u16 = 32;
/// An answer whose row (or its appended copy) the archive did not carry.
pub const SAW_BEFORE_ARCHIVE: u16 = 64;
/// A retried rid found in the sequencer's stream (completed-dedup).
pub const SAW_DEDUP: u16 = 128;
/// The root appended a crashed delegate's row from a member's copy.
pub const SAW_SEAL: u16 = 256;
/// A root that took the lease by a claim answered.
pub const SAW_TAKEOVER_ANSWER: u16 = 512;
/// A row journaled under a recorded (reconfigured) configuration answered.
pub const SAW_RECONFIG_ANSWER: u16 = 1024;
/// A node crashed after an answer.
pub const SAW_CRASH_AFTER_ACK: u16 = 2048;
/// A requester resent its op by rid.
pub const SAW_RETRY: u16 = 4096;
/// A delegate answered a row one of whose rule's members is down (the
/// archive covered it).
pub const SAW_COVERED: u16 = 8192;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub nodes: Vec<Node>,
    pub streams: Vec<Stream>,
    /// The prefix of the root's journal the archive carries.
    pub shipped: u8,
    /// The namespace as the sequencers validated it. Each key has one
    /// sequencer (ownership is static), so one view is every sequencer's
    /// view of its own keys.
    pub view: View,
    pub pending: Vec<Pending>,
    pub net: Vec<Msg>,
    /// Definitive answers emitted by a sequencer (each rid once). `net`,
    /// `pending` and `acked` are kept sorted: their order is not state.
    pub acked: Vec<(Rid, Ret)>,
    pub violation: Option<Violation>,
    pub saw: u16,
    /// The lease holder.
    pub root: u8,
    /// Bitmask over nodes: crashed or deposed (never return).
    pub down: u8,
    pub crashes: u8,
    pub depositions: u8,
    /// The environment's "a peer is within the RTT budget" flag.
    pub peer_in_budget: bool,
}

impl State {
    fn live(&self, n: u8) -> bool {
        self.down & (1 << n) == 0
    }
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
    /// The root tells delegate stream `g` how far its copy is committed and
    /// how far the archive carries it.
    RootAck(u8),
    /// The executor of `pending[k]` answers it.
    Answer(usize),
    /// Node `i` fails (fail-stop, disk kept).
    Crash(u8),
    /// The root pauses and `claimant` takes the lease (seal takeover).
    Depose { claimant: u8 },
    /// Node `i` resends its outstanding op by rid.
    Retry(u8),
    /// The planned reconfiguration of `stream` is journaled.
    Reconfig(u8),
    /// The "peer within budget" flag goes down.
    TogglePeer,
}

#[derive(Clone, Debug)]
pub struct NodeStreamModel {
    /// Node 0 is the initial root (the lease holder).
    pub n: u8,
    pub scripts: Vec<Vec<WriteOp>>,
    pub initial_present: View,
    /// Bitmask over nodes: the lease's listed backups.
    pub root_backups: u8,
    /// Stream `k + 1` is `grants[k]`.
    pub grants: Vec<NodeGrant>,
    pub max_crashes: u8,
    pub max_depositions: u8,
    /// Planned reconfigurations: `(stream, new backups)`, one per stream.
    pub reconfigs: Vec<(u8, u8)>,
    /// The environment may flip the "peer within budget" flag (once).
    pub peer_toggle: bool,
    /// The I1 oracle at answer time (off only to show a knob's harm).
    pub check_i1: bool,
    pub lone_delegate: bool,
    pub root_journal_counts: bool,
    pub no_dedup: bool,
    pub per_row_class: bool,
    pub ack_by_latest_config: bool,
}

impl NodeStreamModel {
    /// The design: node 0 roots, no grant, no backup, no fault, the rules on.
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
            max_crashes: 0,
            max_depositions: 0,
            reconfigs: Vec::new(),
            peer_toggle: false,
            check_i1: true,
            lone_delegate: false,
            root_journal_counts: false,
            no_dedup: false,
            per_row_class: false,
            ack_by_latest_config: false,
        }
    }

    pub fn with_root_backups(mut self, backups: u8) -> Self {
        assert_eq!(backups & 1, 0, "the root is not its own backup");
        self.root_backups = backups;
        self
    }

    pub fn with_grant(mut self, g: NodeGrant) -> Self {
        // Delegates are other nodes (the root's own sticky grants are a
        // later chunk), on disjoint subtree sets, never `/`.
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

    pub fn with_faults(mut self, crashes: u8, depositions: u8) -> Self {
        self.max_crashes = crashes;
        self.max_depositions = depositions;
        self
    }

    /// Plan one reconfiguration of `stream` to `backups`.
    pub fn with_reconfig(mut self, stream: u8, backups: u8) -> Self {
        assert!(self.reconfigs.iter().all(|r| r.0 != stream));
        if stream == ROOT {
            assert_eq!(backups & 1, 0);
        } else {
            let g = self.grants[stream as usize - 1];
            assert_eq!(g.kind, GrantKind::Placed);
            assert_eq!(backups & (1 << g.node), 0);
        }
        self.reconfigs.push((stream, backups));
        self
    }

    pub fn with_peer_toggle(mut self) -> Self {
        self.peer_toggle = true;
        self
    }

    fn n(&self) -> usize {
        self.n as usize
    }

    fn holds_grant(&self, node: u8) -> bool {
        self.grants.iter().any(|g| g.node == node)
    }

    /// The sequencer of a stream.
    pub fn executor(&self, s: &State, stream: u8) -> u8 {
        if stream == ROOT {
            s.root
        } else {
            self.grants[stream as usize - 1].node
        }
    }

    /// The commit rule of a configuration of `stream` with `backups`. No
    /// lone delegate: a placed grant with no recorded backup has the root
    /// as its second member.
    pub fn rule(&self, stream: u8, backups: u8) -> Rule {
        if stream == ROOT {
            return Rule::Group {
                members: backups,
                via_root: false,
            };
        }
        match self.grants[stream as usize - 1].kind {
            GrantKind::Placed => Rule::Group {
                members: backups,
                via_root: backups == 0,
            },
            GrantKind::Sticky | GrantKind::Designated => Rule::Final,
        }
    }

    /// The rules row `idx` must satisfy under version `v` of `cfgs`: a
    /// record commits under old ∪ new.
    fn rules(&self, cfgs: &[Cfg], stream: u8, v: usize, idx: u8) -> [Option<Rule>; 2] {
        let c = cfgs[v];
        let new = self.rule(stream, c.backups);
        if c.record && c.start == idx && v > 0 {
            [Some(new), Some(self.rule(stream, cfgs[v - 1].backups))]
        } else {
            [Some(new), None]
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

    /// Bitmask over keys: those the root sequences.
    fn root_keys(&self) -> View {
        (0..View::BITS as u8)
            .filter(|&k| self.stream_of_dir(dir_of(k)) == ROOT)
            .fold(0, |v, k| v | (1 << k))
    }

    /// The planned backups not yet in `stream`'s configuration (learners).
    fn learners(&self, s: &State, stream: u8) -> u8 {
        let st = &s.streams[stream as usize];
        if st.reconfigured {
            return 0;
        }
        self.reconfigs
            .iter()
            .find(|r| r.0 == stream)
            .map_or(0, |r| r.1 & !st.cfgs.last().unwrap().backups)
    }
}

fn bits(mask: u8) -> impl Iterator<Item = usize> {
    (0..8).filter(move |b| mask & (1 << b) != 0)
}

/// The version in force at position `pos` of a stream.
fn cfg_at(cfgs: &[Cfg], pos: u8) -> usize {
    cfgs.iter().rposition(|c| c.start <= pos).unwrap()
}

/// The root journal's position of the row `(stream, idx)`, if appended.
fn root_pos(s: &State, stream: u8, idx: u8) -> Option<usize> {
    s.streams[ROOT as usize]
        .rows
        .iter()
        .position(|r| r.stream == stream && r.idx == idx)
}

fn appended(s: &State, g: u8) -> usize {
    s.streams[ROOT as usize]
        .rows
        .iter()
        .filter(|r| r.stream == g)
        .count()
}

/// The root's position `p` satisfies the rules of the root configuration
/// in force there (the archive aside).
fn root_rules_ok(m: &NodeStreamModel, s: &State, p: usize) -> bool {
    let st = &s.streams[ROOT as usize];
    let v = cfg_at(&st.cfgs, p as u8);
    m.rules(&st.cfgs, ROOT, v, p as u8)
        .into_iter()
        .flatten()
        .all(|r| match r {
            Rule::Group { members, .. } => {
                members != 0 && bits(members).all(|b| st.held[b] as usize > p)
            }
            Rule::Final => true,
        })
}

/// The committed prefix of the root's journal, from the stores. The root
/// knows its backups' acknowledgements and its shipping, so this is also
/// its own gate.
fn root_committed_prefix(m: &NodeStreamModel, s: &State) -> usize {
    let len = s.streams[ROOT as usize].rows.len();
    let mut k = s.shipped as usize;
    while k < len && root_rules_ok(m, s, k) {
        k += 1;
    }
    k
}

/// Oracle: the root's journal row at `p` is committed by the root's
/// configuration in force at its position.
fn root_pos_committed(m: &NodeStreamModel, s: &State, p: usize) -> bool {
    p < root_committed_prefix(m, s)
}

/// Oracle: the committed prefix of delegate stream `g`, by the
/// configuration in force at each position (not the row's stamp).
fn deleg_committed_prefix(m: &NodeStreamModel, s: &State, g: u8) -> usize {
    let st = &s.streams[g as usize];
    let rcp = root_committed_prefix(m, s);
    let mut k = 0u8;
    // The archived prefix (appended in order).
    while root_pos(s, g, k).is_some_and(|p| p < s.shipped as usize) {
        k += 1;
    }
    while (k as usize) < st.rows.len() {
        let v = cfg_at(&st.cfgs, k);
        let ok = m
            .rules(&st.cfgs, g, v, k)
            .into_iter()
            .flatten()
            .all(|r| match r {
                Rule::Final => true,
                Rule::Group { members, via_root } => {
                    (members != 0 || via_root)
                        && bits(members).all(|b| st.held[b] > k)
                        && (!via_root || root_pos(s, g, k).is_some_and(|p| p < rcp))
                }
            });
        if !ok {
            break;
        }
        k += 1;
    }
    k as usize
}

/// Oracle (I1): row `idx` of `stream` is committed by the configuration in
/// force at its journal position, judged from the stores alone.
pub fn committed(m: &NodeStreamModel, s: &State, stream: u8, idx: u8) -> bool {
    if stream == ROOT {
        root_pos_committed(m, s, idx as usize)
    } else {
        (idx as usize) < deleg_committed_prefix(m, s, stream)
    }
}

/// What the root reports as the committed prefix of its copy of stream `g`
/// (`DelegateStreamAck.committed`), from its own state.
fn root_reported(m: &NodeStreamModel, s: &State, g: u8) -> u8 {
    let rcp = root_committed_prefix(m, s);
    let mut k = 0u8;
    while let Some(p) = root_pos(s, g, k) {
        if !(m.root_journal_counts || p < rcp) {
            break;
        }
        k += 1;
    }
    k
}

/// The prefix of delegate stream `g` the archive carries.
fn root_archived(s: &State, g: u8) -> u8 {
    let mut k = 0u8;
    while root_pos(s, g, k).is_some_and(|p| p < s.shipped as usize) {
        k += 1;
    }
    k
}

/// The delegate's gate: the prefix of its stream it may answer, from what
/// it knows (its members' acknowledgements, the root's report) and each
/// row's version stamp.
fn deleg_gate_prefix(m: &NodeStreamModel, s: &State, g: u8) -> usize {
    let st = &s.streams[g as usize];
    let mut k = 0u8;
    while (k as usize) < st.rows.len() {
        let row = st.rows[k as usize];
        let ok = if m.lone_delegate || row.version == LONE || st.root_archived > k {
            true
        } else {
            let mut rules = if m.ack_by_latest_config {
                m.rules(&st.cfgs, g, st.cfgs.len() - 1, k)
            } else {
                m.rules(&st.cfgs, g, row.version as usize, k)
            };
            if m.ack_by_latest_config {
                rules[1] = None;
            }
            rules.into_iter().flatten().all(|r| match r {
                Rule::Final => true,
                Rule::Group { members, via_root } => {
                    bits(members).all(|b| st.held[b] > k) && (!via_root || st.root_committed > k)
                }
            })
        };
        if !ok {
            break;
        }
        k += 1;
    }
    k as usize
}

/// May the executor answer row `idx` of `stream` now?
fn may_answer(m: &NodeStreamModel, s: &State, stream: u8, idx: u8) -> bool {
    if stream == ROOT {
        root_pos_committed(m, s, idx as usize)
    } else {
        (idx as usize) < deleg_gate_prefix(m, s, stream)
    }
}

/// The prefix of delegate stream `g` the root can append: the delegate's
/// stream while it lives, else (the seal) the longest copy held by a live
/// member of a configuration the root knows (the grant's, or one whose
/// record it has appended).
fn available(s: &State, g: u8, exec: u8) -> usize {
    let st = &s.streams[g as usize];
    if s.live(exec) {
        return st.rows.len();
    }
    let app = appended(s, g);
    st.cfgs
        .iter()
        .enumerate()
        .filter(|(v, c)| *v == 0 || (c.start as usize) < app)
        .flat_map(|(_, c)| bits(c.backups))
        .filter(|&b| s.live(b as u8))
        .map(|b| st.held[b] as usize)
        .max()
        .unwrap_or(0)
}

/// Every answered row is in the root's journal or held by a live node from
/// which the root will append it, with the answered result.
pub fn acked_never_lost(m: &NodeStreamModel, s: &State) -> bool {
    s.acked.iter().all(|&(rid, ret)| {
        let root = &s.streams[ROOT as usize];
        if let Some(r) = root
            .rows
            .iter()
            .find(|r| r.stream == ROOT && !r.record && r.rid == rid)
        {
            return r.ret == ret;
        }
        for g in 1..s.streams.len() {
            let st = &s.streams[g];
            if let Some(idx) = st.rows.iter().position(|r| !r.record && r.rid == rid) {
                return st.rows[idx].ret == ret
                    && (root_pos(s, g as u8, idx as u8).is_some()
                        || idx < available(s, g as u8, m.executor(s, g as u8)));
            }
        }
        false
    })
}

/// I2(a): no rid has two rows (the root's own, and every delegate's).
pub fn exactly_once(s: &State) -> bool {
    let mut rids: Vec<Rid> = Vec::new();
    let root = s.streams[ROOT as usize]
        .rows
        .iter()
        .filter(|r| r.stream == ROOT);
    let delegs = s.streams[1..].iter().flat_map(|st| st.rows.iter());
    for r in root.chain(delegs).filter(|r| !r.record) {
        if rids.contains(&r.rid) {
            return false;
        }
        rids.push(r.rid);
    }
    true
}

/// M1.1's oracle: every row's version is the configuration in force at its
/// position in its own stream.
pub fn row_version_matches_config(s: &State) -> bool {
    s.streams.iter().enumerate().all(|(k, st)| {
        st.rows
            .iter()
            .enumerate()
            .filter(|(_, r)| k != ROOT as usize || r.stream == ROOT)
            .all(|(p, r)| r.version as usize == cfg_at(&st.cfgs, p as u8))
    })
}

fn execute(m: &NodeStreamModel, s: &mut State, stream: u8, rid: Rid, op: WriteOp) {
    let (ret, view) = eval(s.view, op);
    s.view = view;
    let executor = m.executor(s, stream);
    let lone = m.per_row_class && stream != ROOT && !s.peer_in_budget;
    let st = &mut s.streams[stream as usize];
    let idx = st.rows.len() as u8;
    st.rows.push(Row {
        stream,
        idx,
        rid,
        op,
        ret,
        version: if lone {
            LONE
        } else {
            (st.cfgs.len() - 1) as u8
        },
        record: false,
    });
    s.pending.push(Pending {
        executor,
        stream,
        idx,
    });
}

/// The sequencer of `stream` receives `rid`: completed-dedup, else execute.
fn receive(m: &NodeStreamModel, s: &mut State, stream: u8, rid: Rid, op: WriteOp) {
    if !m.no_dedup {
        let found = s.streams[stream as usize]
            .rows
            .iter()
            .position(|r| !r.record && r.rid == rid && r.stream == stream);
        if let Some(idx) = found {
            let idx = idx as u8;
            s.saw |= SAW_DEDUP;
            if !s.pending.iter().any(|p| p.stream == stream && p.idx == idx) {
                s.pending.push(Pending {
                    executor: m.executor(s, stream),
                    stream,
                    idx,
                });
            }
            return;
        }
    }
    execute(m, s, stream, rid, op);
}

/// Node `i` sends `rid` to the sequencer of its stream.
fn submit(m: &NodeStreamModel, s: &mut State, i: u8, rid: Rid, op: WriteOp) -> Option<()> {
    let stream = m.stream_of(op)?;
    let to = m.executor(s, stream);
    s.nodes[i as usize].sent_to = to;
    if to == i {
        receive(m, s, stream, rid, op);
    } else {
        s.net.push(Msg::Fwd { to, rid, op });
    }
    Some(())
}

impl Model for NodeStreamModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        let mut streams = vec![self.root_backups];
        streams.extend(self.grants.iter().map(|g| g.backups));
        vec![State {
            nodes: vec![
                Node {
                    next: 0,
                    outstanding: false,
                    sent_to: 0,
                };
                n
            ],
            streams: streams
                .into_iter()
                .map(|backups| Stream {
                    rows: Vec::new(),
                    held: vec![0; n],
                    root_committed: 0,
                    root_archived: 0,
                    cfgs: vec![Cfg {
                        start: 0,
                        backups,
                        record: false,
                    }],
                    reconfigured: false,
                })
                .collect(),
            shipped: 0,
            view: self.initial_present,
            pending: Vec::new(),
            net: Vec::new(),
            acked: Vec::new(),
            violation: None,
            saw: 0,
            root: 0,
            down: 0,
            crashes: 0,
            depositions: 0,
            peer_in_budget: true,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        // A violation is a terminal state for the search.
        if s.violation.is_some() {
            return;
        }
        for i in 0..self.n() as u8 {
            let node = s.nodes[i as usize];
            if !s.live(i) {
                continue;
            }
            if !node.outstanding {
                if let Some(op) = self.scripts[i as usize].get(node.next as usize) {
                    if self.stream_of(*op).is_some() {
                        actions.push(Action::Start(i));
                    }
                }
            } else if !s.live(node.sent_to) {
                let op = self.scripts[i as usize][node.next as usize - 1];
                let stream = self.stream_of(op).unwrap();
                if s.live(self.executor(s, stream)) {
                    actions.push(Action::Retry(i));
                }
            }
        }
        for k in 0..s.net.len() {
            actions.push(Action::Deliver(k));
        }
        for (stream, st) in s.streams.iter().enumerate() {
            let stream = stream as u8;
            let exec = self.executor(s, stream);
            if s.live(exec) {
                let learners = self.learners(s, stream);
                for b in 0..self.n() {
                    let k = st.held[b];
                    if b as u8 == exec || !s.live(b as u8) || k as usize >= st.rows.len() {
                        continue;
                    }
                    let v = cfg_at(&st.cfgs, k);
                    let member = learners & (1 << b) != 0
                        || self
                            .rules(&st.cfgs, stream, v, k)
                            .into_iter()
                            .flatten()
                            .any(|r| matches!(r, Rule::Group { members, .. } if members & (1 << b) != 0));
                    if member {
                        actions.push(Action::Replicate {
                            stream,
                            member: b as u8,
                        });
                    }
                }
                // Add-then-record: every added backup holds the stream.
                if !st.reconfigured
                    && self.reconfigs.iter().any(|r| r.0 == stream)
                    && bits(learners).all(|b| st.held[b] as usize == st.rows.len())
                {
                    actions.push(Action::Reconfig(stream));
                }
            }
            if stream != ROOT {
                if appended(s, stream) < available(s, stream, exec) {
                    actions.push(Action::Append(stream));
                }
                let via_root = st.cfgs.iter().any(|c| {
                    matches!(
                        self.rule(stream, c.backups),
                        Rule::Group { via_root: true, .. }
                    )
                });
                let reported = via_root && root_reported(self, s, stream) > st.root_committed;
                if s.live(exec) && (reported || root_archived(s, stream) > st.root_archived) {
                    actions.push(Action::RootAck(stream));
                }
            }
        }
        if (s.shipped as usize) < s.streams[ROOT as usize].rows.len() {
            actions.push(Action::Ship);
        }
        for (k, p) in s.pending.iter().enumerate() {
            if s.live(p.executor) && may_answer(self, s, p.stream, p.idx) {
                actions.push(Action::Answer(k));
            }
        }
        if s.crashes < self.max_crashes {
            for i in 0..self.n() as u8 {
                if i != s.root && s.live(i) {
                    actions.push(Action::Crash(i));
                }
            }
        }
        if s.depositions < self.max_depositions {
            let backups = s.streams[ROOT as usize].cfgs.last().unwrap().backups;
            for c in 0..self.n() as u8 {
                let eligible = if backups != 0 {
                    backups & (1 << c) != 0
                } else {
                    c != s.root && !self.holds_grant(c)
                };
                if eligible && s.live(c) {
                    actions.push(Action::Depose { claimant: c });
                }
            }
        }
        if self.peer_toggle && s.peer_in_budget {
            actions.push(Action::TogglePeer);
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
                submit(self, &mut s, i, rid, op)?;
            }
            Action::Retry(i) => {
                let seq = s.nodes[i as usize].next - 1;
                let op = self.scripts[i as usize][seq as usize];
                s.saw |= SAW_RETRY;
                submit(self, &mut s, i, Rid { node: i, seq }, op)?;
            }
            Action::Deliver(k) => match s.net.remove(k) {
                Msg::Fwd { to, rid, op } => {
                    let stream = self.stream_of(op)?;
                    // A message to a node that is down, or no longer the
                    // sequencer, is lost; the requester retries.
                    if s.live(to) && self.executor(&s, stream) == to {
                        receive(self, &mut s, stream, rid, op);
                    }
                }
                Msg::Reply { rid, .. } => {
                    // A duplicate (a retry's second answer) is dropped.
                    let node = &mut s.nodes[rid.node as usize];
                    if last.live(rid.node) && node.outstanding && node.next == rid.seq + 1 {
                        node.outstanding = false;
                    }
                }
            },
            Action::Replicate { stream, member } => {
                s.streams[stream as usize].held[member as usize] += 1;
            }
            Action::Append(g) => {
                let k = appended(&s, g);
                let row = s.streams[g as usize].rows[k];
                if !s.live(self.executor(&s, g)) {
                    s.saw |= SAW_SEAL;
                }
                s.streams[ROOT as usize].rows.push(row);
            }
            Action::Ship => {
                s.shipped = s.streams[ROOT as usize].rows.len() as u8;
            }
            Action::RootAck(g) => {
                let committed = root_reported(self, &s, g);
                let archived = root_archived(&s, g);
                let st = &mut s.streams[g as usize];
                st.root_committed = st.root_committed.max(committed);
                st.root_archived = archived;
            }
            Action::Answer(k) => {
                let p = s.pending.remove(k);
                let row = s.streams[p.stream as usize].rows[p.idx as usize];
                // The oracle, at emission, from the stores.
                if self.check_i1 && !committed(self, &s, p.stream, p.idx) {
                    s.violation = Some(Violation {
                        property: "ack_implies_committed_by_config",
                        rid: row.rid,
                        stream: p.stream,
                        idx: p.idx,
                    });
                }
                if !s.acked.contains(&(row.rid, row.ret)) {
                    s.acked.push((row.rid, row.ret));
                }
                let in_archive = if p.stream == ROOT {
                    (s.shipped as usize) > p.idx as usize
                } else {
                    root_pos(&s, p.stream, p.idx).is_some_and(|q| s.shipped as usize > q)
                };
                if !in_archive {
                    s.saw |= SAW_BEFORE_ARCHIVE;
                }
                if p.stream == ROOT && s.depositions > 0 {
                    s.saw |= SAW_TAKEOVER_ANSWER;
                }
                let st = &s.streams[p.stream as usize];
                let (backups, recorded) = if row.version == LONE {
                    (0, false)
                } else {
                    let v = row.version as usize;
                    let rec = st.cfgs[..=v].iter().any(|c| c.record);
                    (st.cfgs[v].backups, rec)
                };
                let dead = p.stream != ROOT
                    && row.version != LONE
                    && self
                        .rules(&st.cfgs, p.stream, row.version as usize, p.idx)
                        .into_iter()
                        .flatten()
                        .any(|r| matches!(r, Rule::Group { members, .. } if members & s.down != 0));
                if recorded {
                    s.saw |= SAW_RECONFIG_ANSWER;
                }
                if dead {
                    s.saw |= SAW_COVERED;
                }
                s.saw |= match self.rule(p.stream, backups) {
                    Rule::Final => SAW_FINAL,
                    Rule::Group { via_root: true, .. } => SAW_VIA_ROOT,
                    Rule::Group { .. } if p.stream != ROOT => SAW_DELEG_BACKUP,
                    Rule::Group { .. } if in_archive => SAW_ARCHIVE,
                    Rule::Group { .. } => SAW_ROOT_GROUP,
                };
                if row.rid.node == p.executor {
                    s.nodes[row.rid.node as usize].outstanding = false;
                } else {
                    s.saw |= SAW_FORWARD;
                    s.net.push(Msg::Reply {
                        rid: row.rid,
                        ret: row.ret,
                    });
                }
            }
            Action::Crash(i) => {
                s.down |= 1 << i;
                s.crashes += 1;
                s.pending.retain(|p| p.executor != i);
                if !s.acked.is_empty() {
                    s.saw |= SAW_CRASH_AFTER_ACK;
                }
            }
            Action::Depose { claimant } => {
                let old = s.root;
                s.down |= 1 << old;
                s.depositions += 1;
                let shipped = s.shipped;
                let st = &mut s.streams[ROOT as usize];
                let backups = st.cfgs.last().unwrap().backups;
                // The seal: a backup's held prefix, or the archive alone.
                let len = if backups != 0 {
                    shipped.max(st.held[claimant as usize])
                } else {
                    shipped
                };
                st.rows.truncate(len as usize);
                for h in st.held.iter_mut() {
                    *h = (*h).min(len);
                }
                st.held[claimant as usize] = len;
                let keep = st
                    .cfgs
                    .iter()
                    .skip(1)
                    .position(|c| c.start >= len)
                    .map_or(st.cfgs.len(), |k| k + 1);
                st.cfgs.truncate(keep);
                st.cfgs.push(Cfg {
                    start: len,
                    backups: backups & !(1 << claimant) & !(1 << old),
                    record: false,
                });
                s.root = claimant;
                // The deposed root's parked answers die with its tenure.
                s.pending.retain(|p| p.stream != ROOT);
                // The new root's keys are its journal's.
                let mask = self.root_keys();
                let mut v = self.initial_present;
                for r in s.streams[ROOT as usize]
                    .rows
                    .iter()
                    .filter(|r| r.stream == ROOT && !r.record)
                {
                    v = eval(v, r.op).1;
                }
                s.view = (s.view & !mask) | (v & mask);
            }
            Action::Reconfig(stream) => {
                let new = self.reconfigs.iter().find(|r| r.0 == stream)?.1;
                let st = &mut s.streams[stream as usize];
                let idx = st.rows.len() as u8;
                st.cfgs.push(Cfg {
                    start: idx,
                    backups: new,
                    record: true,
                });
                let v = (st.cfgs.len() - 1) as u8;
                st.rows.push(Row {
                    stream,
                    idx,
                    rid: Rid {
                        node: RECORD_NODE,
                        seq: v,
                    },
                    op: WriteOp::Create(0),
                    ret: Ret::Ok,
                    version: v,
                    record: true,
                });
                st.reconfigured = true;
            }
            Action::TogglePeer => {
                s.peer_in_budget = false;
            }
        }
        s.net.sort_unstable();
        s.pending.sort_unstable();
        s.acked.sort_unstable();
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("ack_implies_committed_by_config", |_, s: &State| {
                s.violation.is_none()
            }),
            Property::always("acked_never_lost", |m: &NodeStreamModel, s: &State| {
                acked_never_lost(m, s)
            }),
            Property::always("exactly_once", |_, s: &State| exactly_once(s)),
            Property::always("row_version_matches_config", |_, s: &State| {
                row_version_matches_config(s)
            }),
            Property::sometimes("all_answered", |m: &NodeStreamModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| !n.outstanding && n.next as usize == sc.len())
            }),
            Property::sometimes("forward_answered", |_, s: &State| s.saw & SAW_FORWARD != 0),
        ];
        let reconf = |stream: u8| self.reconfigs.iter().find(|r| r.0 == stream).map(|r| r.1);
        // Reachable only with a group that is not the archive: a root
        // backup, a delegate's backup, or a non-stealable stream. With
        // neither, a placed delegate's second copy (the root's) commits
        // only by the archive (the root's own rule).
        if self.root_backups != 0
            || reconf(ROOT).is_some_and(|b| b != 0)
            || self.grants.iter().enumerate().any(|(k, g)| {
                g.backups != 0
                    || g.kind != GrantKind::Placed
                    || reconf(k as u8 + 1).is_some_and(|b| b != 0)
            })
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
        if self.max_crashes > 0 {
            props.push(Property::sometimes("crash_after_ack", |_, s: &State| {
                s.saw & SAW_CRASH_AFTER_ACK != 0
            }));
        }
        if self.max_crashes > 0 && has(|g| g.kind == GrantKind::Placed && g.backups != 0) {
            props.push(Property::sometimes("seal_append", |_, s: &State| {
                s.saw & SAW_SEAL != 0
            }));
        }
        if self.max_depositions > 0 {
            props.push(Property::sometimes("takeover_answer", |_, s: &State| {
                s.saw & SAW_TAKEOVER_ANSWER != 0
            }));
            props.push(Property::sometimes("retry_by_rid", |_, s: &State| {
                s.saw & SAW_RETRY != 0
            }));
            if !self.no_dedup {
                props.push(Property::sometimes("completed_dedup", |_, s: &State| {
                    s.saw & SAW_DEDUP != 0
                }));
            }
        }
        let removes = self
            .reconfigs
            .iter()
            .any(|&(g, to)| g != ROOT && self.grants[g as usize - 1].backups & !to != 0);
        if self.max_crashes > 0 && removes {
            props.push(Property::sometimes(
                "dead_member_covered",
                |_, s: &State| s.saw & SAW_COVERED != 0,
            ));
        }
        if !self.reconfigs.is_empty() {
            props.push(Property::sometimes("reconfig_answer", |_, s: &State| {
                s.saw & SAW_RECONFIG_ANSWER != 0
            }));
        }
        props
    }
}
