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
//!   (`DelegateNodeGroup`). The initial grants' records are in the archive
//!   before the first step (Q4's premise, honoured once committed); chunk 4
//!   adds growth and release, chunk 6 the end.
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
//!   stream's sequencer, locally or forwarded.
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
//!   archive); with none, any live node that holds no grant (or, with
//!   [`NodeStreamModel::delegate_may_claim`], a delegate: chunk 6) adopts
//!   the archive alone. The deposed root never resumes as root (its PUTs
//!   are fenced), so its parked answers die with it; the new epoch's
//!   configuration is the lease's (written by the claim's conditional PUT,
//!   so it needs no record), its backups the old ones less the claimant and
//!   the deposed root. The new root rebuilds its keys from the adopted
//!   journal and **re-streams** each delegate stream from the delegate's
//!   rows (the appended cursor is what its journal carries).
//! - **Seal of a crashed delegate's stream**: the root appends a crashed
//!   delegate's rows from a live member of a configuration it knows (the
//!   grant's, or one whose record it has appended).
//! - **Retry by rid** ([`Action::Retry`]): a requester whose op went to a
//!   node that is down (or a delegate that stopped honouring) resends it,
//!   same rid, to the stream's sequencer in force. **Completed-dedup**: a
//!   sequencer that finds the rid in its stream (the root: anywhere in its
//!   journal, an appended row included) parks an answer for that row
//!   instead of executing again.
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
//! ## Chunk 4: subtree sets, release, recall as a request
//!
//! - **Ownership is the root's table**, folded from the root's journal
//!   ([`RowKind::Grow`] records, appended [`RowKind::Released`] rows,
//!   [`RowKind::Ended`] records) over the initial grants. Requesters route
//!   by it; a sequencer checks its own view when the op arrives and answers
//!   **`NotOwner`** when it no longer holds a key (a forward in flight
//!   across a release, a root table not yet showing a release), and the
//!   requester re-resolves ([`Msg::NotOwner`]). A delegate's own view is
//!   its grant, less what its own stream released, plus what a `Grow`
//!   record it has learned committed added ([`Action::Learn`]).
//! - **Cross-subtree ops at the root.** An op whose keys span node streams
//!   goes to the root, which parks it ([`Wait`]) and **requests** the
//!   release (ADR-44's recall-on-demand as a request): the delegate answers
//!   with `SubtreeReleased { dir, at }`, a row of **its own stream**
//!   ([`Action::Release`]; `at` is the last index that touched `dir`). The
//!   root sequences `dir` once its journal shows the release, i.e. after
//!   it appended the stream through the release row and so through `at`
//!   ([`Action::Exec`]). The request is abstracted to the release action's
//!   guard: it is enabled while the root has a parked op that needs the
//!   directory. An op between two subtrees of **one** node is an ordinary
//!   op of that node's stream: no request, no release.
//! - **`StreamReleased { through }`** ([`Action::ReleaseAll`],
//!   [`NodeStreamModel::stream_releases`]): the holder releases every
//!   subtree it holds at once (a voluntary release), as one row.
//! - **Growth** ([`Action::Grow`], [`NodeStreamModel::grows`]): the root
//!   journals a `DelegateNode` record adding a directory to a node's set
//!   and stops sequencing it at once; the delegate honours it once the
//!   record is committed and it has learned it (its applied prefix of the
//!   root's committed journal).
//! - The root executes against its journal (the fold of its own and its
//!   appended rows), a delegate against its keys; the shared [`State::view`]
//!   carries each key's latest sequenced value for the next owner.
//!
//! ## Chunk 5: per-key linearizability and the causal cut
//!
//! - **History** ([`NodeStreamModel::history`]): each requester's invoke
//!   and return per key (a `Move`'s two halves), checked against a boolean
//!   register (`delegation`'s [`KeySpec`]).
//! - **`deps` per node stream** ([`NodeStreamModel::causal`]): a requester's
//!   `observed` vector has one entry per stream (the root's journal and
//!   each node stream): `idx + 1` of the latest row it was answered from.
//!   An op carries it as `deps`; the root executes only once its journal
//!   carries every delegate stream through `deps`, a delegate only once
//!   its applied prefix of the root's journal does. A reader on a replica
//!   (the archive, a root backup's copy, a delegate's replica) is the
//!   `marker_order` probe ([`NodeStreamModel::probe`]).
//! - **Replicas** ([`NodeStreamModel::replicas`]): a delegate's replica is
//!   the root's committed journal streamed to it ([`Action::Learn`]) with
//!   its own unordered rows applied on top, and the archive's durable apply
//!   ([`Action::Durable`]), which skips what the replica already holds (the
//!   segment path skips own rows as own and streamed rows as confirmed).
//!
//! ## Chunk 6: the end of a placed grant, C3's verbatim re-ship, L1, L2b
//!
//! - **TTL end** ([`Action::Stop`], [`Action::End`],
//!   [`NodeStreamModel::ttl_ends`]): a placed holder stops honouring
//!   (abstract TTL: unreachable, it answers nothing more, its parked answers
//!   die); then the root seals the stream from the recorded members' copies
//!   in one step (the verbatim tail) and journals `DelegateNodeEnded {
//!   through }` at its cursor. A stream whose configuration names backups
//!   ends only once one of them is live to seal (plan 43 C2e; with none the
//!   end waits for the group, and the operator's `abandon-group` is not
//!   modeled). The directories return to the root; rows
//!   past the cut are void (the delegate strands them; their requesters
//!   retry, and the root dedups a rid it finds appended).
//! - **A delegate taking the root** ([`NodeStreamModel::delegate_may_claim`]):
//!   at its claim it re-appends, verbatim, its own rows the adopted journal
//!   lacks, then ends its generation at the end of its stream; its parked
//!   answers then wait on the root's rule at the appended positions.
//! - **Member copies** stay until the log carries the rows: a later root
//!   that finds the predecessor's stream unended (the delegate-turned-root
//!   was deposed before shipping) seals it from them, verbatim (L2b).
//! - **L1**: a delegate's mark ([`Stream::mark`]) orders its own rows the
//!   stream installs at their log place; it is never cleared by the stream
//!   (only the log's durable apply retires the rows).
//! - **Shadow replay** ([`NodeStreamModel::shadow_replay`],
//!   [`Action::Rescue`]): a requester whose answered row turned void
//!   replays it by rid (the product's rescue); unreachable in the design.
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
//! - `exactly_once` (chunk 2, I2(a)): no rid has two live rows (a void row
//!   past a cut does not count).
//! - `row_version_matches_config` (chunk 3, M1.1's oracle in model form):
//!   every row's version is the configuration in force at its position.
//! - `recall_safety` (chunk 4): a delegate executes in a directory only
//!   while it holds it by the stores: granted initially or by a committed
//!   `Grow` record, not released in its stream, its grant not ended.
//! - `released_subtree_sequenced_after_cut` (chunk 4): every root row in a
//!   directory a node stream held comes after that stream's release (or
//!   end) of it in the root's journal and after every row of that stream
//!   below the release that touched it.
//! - `two_subtrees_one_order` (chunk 4): a release is journaled only for an
//!   op whose keys span node streams (an op between two subtrees of one
//!   node never recalls).
//! - `journal_valid` (chunk 4): every row of the root's journal returns at
//!   its position what it returned when executed (the verbatim append's
//!   premise).
//! - `per_key_linearizable`, `causal_cut`, `marker_order` (chunk 5);
//!   `converged_at_quiescence` (chunks 5, 6: every live delegate's replica
//!   is the log once nothing moves).
//! - `acked_at_or_below_cut` (chunk 6, M1.2's oracle): no answered row of
//!   an ended stream lies past its cut.
//! - `verbatim_identity` (chunk 6, I2(c)): every answered row has one
//!   identity, its original origin `(stream, idx)`, wherever it is.
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
//! - `root_executes_before_at` (chunk 4): the root takes a directory as
//!   released once the release row is in the delegate's stream (its reply),
//!   before appending through `at`.
//! - `execute_after_release` (chunk 4): a delegate ignores its own release
//!   when it checks ownership (a stale forward executes).
//! - `per_subtree_streams` (chunk 4): an op between two subtrees of one node
//!   is routed as cross-subtree (recall by request).
//! - `honour_uncommitted_grant` (chunk 4): a delegate honours a `Grow`
//!   record once it is in the root's journal, not once committed.
//! - `deps_ignored` (chunk 5): sequencers execute without waiting for
//!   `deps`.
//! - `end_before_seal` (chunk 6): the TTL end cuts at the root's cursor
//!   without sealing from the recorded members first.
//! - `no_reappend` (chunk 6): a delegate taking the root ends its
//!   generation at the adopted cursor without re-appending its own rows.
//! - `drop_member_copy` (chunk 6, L2b): the members retire their copies of
//!   a stream when its end is journaled, before the log carries it.
//! - `l1_mark_cleared_by_stream` (chunk 6, L1): the streamed install of the
//!   stream's end or of the delegate's release clears the mark.

use crate::delegation::{dir_of, eval, force, HEvt, Key, KeyOp, KeySpec, Ret, Rid, View, WriteOp};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stateright::{Model, Property};

/// The root's journal's stream id.
pub const ROOT: u8 = 0;
/// `Rid::node` of a record row (records answer no requester).
pub const RECORD_NODE: u8 = 0xFF;
/// The `per_row_class` knob's stamp: a row classed `Lone` at journal time.
pub const LONE: u8 = 0xFE;
/// Streams: the root's journal and at most two node streams.
pub const MAXS: usize = 3;
/// Directories (bits of a directory mask).
const DIRS: usize = 8;

/// A causal position: per stream, `idx + 1` of the latest row (the root's
/// journal: its position).
pub type Deps = [u8; MAXS];

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

/// What a row is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RowKind {
    /// A requester's op.
    Op,
    /// A configuration record (`DelegateNodeGroup`, the lease's).
    Cfg,
    /// A node stream's `SubtreeReleased { dir, at }` (one bit) or
    /// `StreamReleased` (every subtree it held): `at` is one past the last
    /// index of the stream that touched `dirs`.
    Released { dirs: u8, at: u8 },
    /// The root's `DelegateNode` record growing `stream`'s set.
    Grow { stream: u8, dirs: u8 },
    /// The root's `DelegateNodeEnded { through }` for `stream`.
    Ended { stream: u8, through: u8 },
}

/// A journaled row. In the root's journal an appended row keeps its
/// origin `(stream, idx)` verbatim; the root's own rows (and records) have
/// `stream == ROOT` and `idx` their position in the root's journal.
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
    pub kind: RowKind,
    /// The requester's causal position (chunk 5; zero otherwise).
    pub deps: Deps,
}

impl Row {
    /// A record (no op, no requester).
    pub fn record(&self) -> bool {
        self.kind != RowKind::Op
    }
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
    /// Delegate streams: the holder stopped honouring (TTL, or it took the
    /// root and ended its generation): it executes and answers nothing.
    pub stopped: bool,
    /// Delegate streams: the prefix of the root's journal the holder has
    /// installed (streamed, committed rows).
    pub applied: u8,
    /// The L1 mark: own rows below it are ordered at their log place.
    pub mark: u8,
    /// The prefix of the root's journal the holder applied from the archive.
    pub durable: u8,
    /// The holder's replica (chunk 5's readers, the convergence check).
    pub rview: View,
}

/// An answer parked at its executor until the row is committed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pending {
    pub executor: u8,
    pub stream: u8,
    pub idx: u8,
}

/// An op a sequencer received and has not executed: a cross-subtree op at
/// the root waiting for a release, or an op waiting for its `deps`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Wait {
    pub node: u8,
    pub rid: Rid,
    pub op: WriteOp,
    pub deps: Deps,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Fwd {
        to: u8,
        rid: Rid,
        op: WriteOp,
        deps: Deps,
    },
    /// The answer, with the answered row's origin.
    Reply {
        rid: Rid,
        ret: Ret,
        stream: u8,
        idx: u8,
    },
    /// The receiver does not hold a key of the op: re-resolve.
    NotOwner { rid: Rid },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The script index of the next op to start.
    pub next: u8,
    pub outstanding: bool,
    /// Where the outstanding op was last sent (the sequencer).
    pub sent_to: u8,
    /// Per stream, `idx + 1` of the latest row it was answered from
    /// (chunk 5; zero otherwise).
    pub observed: Deps,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Violation {
    pub property: &'static str,
    pub rid: Rid,
    pub stream: u8,
    pub idx: u8,
}

/// A delegate answered through the root's committed copy.
pub const SAW_VIA_ROOT: u32 = 1;
/// A delegate answered through its recorded backup.
pub const SAW_DELEG_BACKUP: u32 = 1 << 1;
/// A non-stealable stream answered (`Final`).
pub const SAW_FINAL: u32 = 1 << 2;
/// An answer went to another node (a forward).
pub const SAW_FORWARD: u32 = 1 << 3;
/// The root answered its own row through its backups, before the archive.
pub const SAW_ROOT_GROUP: u32 = 1 << 4;
/// The root answered its own row once the archive carried it.
pub const SAW_ARCHIVE: u32 = 1 << 5;
/// An answer whose row (or its appended copy) the archive did not carry.
pub const SAW_BEFORE_ARCHIVE: u32 = 1 << 6;
/// A retried rid found in the sequencer's stream (completed-dedup).
pub const SAW_DEDUP: u32 = 1 << 7;
/// The root appended a crashed delegate's row from a member's copy.
pub const SAW_SEAL: u32 = 1 << 8;
/// A root that took the lease by a claim answered.
pub const SAW_TAKEOVER_ANSWER: u32 = 1 << 9;
/// A row journaled under a recorded (reconfigured) configuration answered.
pub const SAW_RECONFIG_ANSWER: u32 = 1 << 10;
/// A node crashed after an answer.
pub const SAW_CRASH_AFTER_ACK: u32 = 1 << 11;
/// A requester resent its op by rid.
pub const SAW_RETRY: u32 = 1 << 12;
/// A delegate answered a row one of whose rule's members is down (the
/// archive covered it).
pub const SAW_COVERED: u32 = 1 << 13;
/// The root executed in a directory a node stream had released.
pub const SAW_RELEASED_EXEC: u32 = 1 << 14;
/// A delegate executed an op between two of its subtrees.
pub const SAW_TWO_SUBTREES: u32 = 1 << 15;
/// A sequencer answered `NotOwner`.
pub const SAW_NOT_OWNER: u32 = 1 << 16;
/// A delegate executed in a directory it was granted by a `Grow` record.
pub const SAW_GROWN: u32 = 1 << 17;
/// A holder released every subtree (`StreamReleased`).
pub const SAW_STREAM_RELEASED: u32 = 1 << 18;
/// A sequencer parked an op for its `deps`.
pub const SAW_DEPS_WAIT: u32 = 1 << 19;
/// The model reached quiescence with replicas compared.
pub const SAW_QUIESCENT: u32 = 1 << 20;
/// The root ended a placed grant (TTL).
pub const SAW_TTL_END: u32 = 1 << 21;
/// A delegate took the root and re-appended its own rows.
pub const SAW_DELEG_CLAIM: u32 = 1 << 22;
/// A stream's end sealed rows from a member's copy.
pub const SAW_MEMBER_SEAL: u32 = 1 << 23;
/// L1: a delegate installed a root row on a key its own ordered row wrote.
pub const SAW_L1: u32 = 1 << 24;
/// The root answered a retried rid from a delegate's row it appended.
pub const SAW_APPENDED_DEDUP: u32 = 1 << 25;
/// A reader saw both the marker and the data on a replica.
pub const SAW_MARKER_AND_DATA: u32 = 1 << 26;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub nodes: Vec<Node>,
    pub streams: Vec<Stream>,
    /// The prefix of the root's journal the archive carries.
    pub shipped: u8,
    /// Each key's latest sequenced value (its sequencer's view; the next
    /// owner's starting point).
    pub view: View,
    pub pending: Vec<Pending>,
    pub waiting: Vec<Wait>,
    pub net: Vec<Msg>,
    /// Definitive answers emitted by a sequencer, with the answered row's
    /// origin `(stream, idx)`. `net`, `pending`, `waiting` and `acked` are
    /// kept sorted: their order is not state.
    pub acked: Vec<(Rid, Ret, u8, u8)>,
    pub violation: Option<Violation>,
    pub saw: u32,
    /// The lease holder.
    pub root: u8,
    /// Bitmask over nodes: crashed or deposed (never return).
    pub down: u8,
    pub crashes: u8,
    pub depositions: u8,
    /// The environment's "a peer is within the RTT budget" flag.
    pub peer_in_budget: bool,
    /// Chunk 5: the requesters' per-key history.
    pub history: Vec<HEvt>,
    /// A shadow replay ran (at most one).
    pub rescued: bool,
    /// The lease's `inherited_tail` (C1-3): a lone tenure that adopted a
    /// tail past the archive names the old group until the log carries it,
    /// so a claim with the archive alone waits for `shipped` to reach it.
    /// Tracked with two depositions or more.
    pub inherited: u8,
}

impl State {
    fn live(&self, n: u8) -> bool {
        self.down & (1 << n) == 0
    }

    fn violate(&mut self, property: &'static str, rid: Rid, stream: u8, idx: u8) {
        if self.violation.is_none() {
            self.violation = Some(Violation {
                property,
                rid,
                stream,
                idx,
            });
        }
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
    /// The sequencer of `waiting[k]` executes it, or answers `NotOwner`.
    Exec(usize),
    /// The holder of `stream` answers the root's request with
    /// `SubtreeReleased` for `dirs`.
    Release { stream: u8, dirs: u8 },
    /// The holder of stream `g` releases every subtree (`StreamReleased`).
    ReleaseAll(u8),
    /// The root journals the planned growth `grows[k]`.
    Grow(u8),
    /// The holder of stream `g` stops honouring (abstract TTL).
    Stop(u8),
    /// The root seals stream `g` and journals its end.
    End(u8),
    /// The holder of stream `g` installs the next committed root row.
    Learn(u8),
    /// The holder of stream `g` applies the archive.
    Durable(u8),
    /// Node `i` replays an answered op whose row turned void, by rid.
    Rescue(u8),
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
    /// Planned `DelegateNode` growth: `(stream, dirs)`, each once.
    pub grows: Vec<(u8, u8)>,
    /// Streams whose holder may release every subtree voluntarily.
    pub stream_releases: Vec<u8>,
    /// Streams whose (placed) holder may stop honouring: the TTL end.
    pub ttl_ends: Vec<u8>,
    /// A delegate may claim the lease (chunk 6).
    pub delegate_may_claim: bool,
    /// Delegates keep replicas (chunk 5's readers, L1, convergence).
    pub replicas: bool,
    /// Requesters carry `deps` (chunk 5).
    pub causal: bool,
    /// Record the per-key history (chunk 5).
    pub history: bool,
    /// A reader's `(marker, data)` probe on every replica (chunk 5).
    pub probe: Option<(Key, Key)>,
    /// A requester replays an answered op whose row turned void (chunk 6).
    pub shadow_replay: bool,
    /// The I1 oracle at answer time (off only to show a knob's harm).
    pub check_i1: bool,
    pub lone_delegate: bool,
    pub root_journal_counts: bool,
    pub no_dedup: bool,
    pub per_row_class: bool,
    pub ack_by_latest_config: bool,
    pub root_executes_before_at: bool,
    pub execute_after_release: bool,
    pub per_subtree_streams: bool,
    pub honour_uncommitted_grant: bool,
    pub deps_ignored: bool,
    pub end_before_seal: bool,
    pub no_reappend: bool,
    pub drop_member_copy: bool,
    pub l1_mark_cleared_by_stream: bool,
}

/// Where a received op stands at a sequencer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Run,
    /// Waiting: for a release (`false`) or for its `deps` (`true`).
    Blocked(bool),
    Bounce,
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
            grows: Vec::new(),
            stream_releases: Vec::new(),
            ttl_ends: Vec::new(),
            delegate_may_claim: false,
            replicas: false,
            causal: false,
            history: false,
            probe: None,
            shadow_replay: false,
            check_i1: true,
            lone_delegate: false,
            root_journal_counts: false,
            no_dedup: false,
            per_row_class: false,
            ack_by_latest_config: false,
            root_executes_before_at: false,
            execute_after_release: false,
            per_subtree_streams: false,
            honour_uncommitted_grant: false,
            deps_ignored: false,
            end_before_seal: false,
            no_reappend: false,
            drop_member_copy: false,
            l1_mark_cleared_by_stream: false,
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
        assert!(self.grants.len() + 1 < MAXS);
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

    /// Plan the growth of `stream`'s set by `dirs` (directories no grant
    /// holds).
    pub fn with_grow(mut self, stream: u8, dirs: u8) -> Self {
        assert!(stream != ROOT && dirs & 1 == 0);
        assert!(self.grants.iter().all(|g| g.dirs & dirs == 0));
        self.grows.push((stream, dirs));
        self
    }

    pub fn with_stream_release(mut self, stream: u8) -> Self {
        self.stream_releases.push(stream);
        self
    }

    pub fn with_ttl_end(mut self, stream: u8) -> Self {
        assert_eq!(self.grants[stream as usize - 1].kind, GrantKind::Placed);
        self.ttl_ends.push(stream);
        self
    }

    fn n(&self) -> usize {
        self.n as usize
    }

    fn holds_grant(&self, node: u8) -> bool {
        self.grants.iter().any(|g| g.node == node)
    }

    /// The node stream of `node`'s grant.
    fn stream_of_node(&self, node: u8) -> Option<u8> {
        self.grants
            .iter()
            .position(|g| g.node == node)
            .map(|k| k as u8 + 1)
    }

    /// The installed prefix of the root's journal matters (deps, growth,
    /// replicas).
    fn learns(&self) -> bool {
        self.replicas || self.causal || !self.grows.is_empty()
    }

    /// A stream's end may be journaled (TTL, or a delegate's claim).
    fn ends(&self, g: u8) -> bool {
        self.ttl_ends.contains(&g)
            || (self.delegate_may_claim && self.grants[g as usize - 1].kind == GrantKind::Placed)
    }

    /// Every directory a node stream ever holds (granted or grown).
    fn delegated_dirs(&self) -> u8 {
        self.grants.iter().fold(0, |m, g| m | g.dirs) | self.grows.iter().fold(0, |m, g| m | g.1)
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

    /// `op`'s keys span node streams by the initial grants.
    fn spans(&self, op: WriteOp) -> bool {
        let mut st = bits(op_dirs(op)).map(|d| {
            self.grants
                .iter()
                .position(|g| g.dirs & (1 << d) != 0)
                .map_or(ROOT, |k| k as u8 + 1)
        });
        let first = st.next().unwrap();
        !st.all(|x| x == first)
    }

    /// `node` is a delegate that stopped honouring (unreachable).
    fn stopped_node(&self, s: &State, node: u8) -> bool {
        node != s.root
            && self
                .stream_of_node(node)
                .is_some_and(|g| s.streams[g as usize].stopped)
    }
}

fn bits(mask: u8) -> impl Iterator<Item = usize> {
    (0..8).filter(move |b| mask & (1 << b) != 0)
}

/// Bitmask over directories: those `op`'s keys are in.
fn op_dirs(op: WriteOp) -> u8 {
    op.keys().into_iter().fold(0, |m, k| m | (1 << dir_of(k)))
}

/// Bitmask over keys: `op`'s keys.
fn op_keys(op: WriteOp) -> View {
    op.keys().into_iter().fold(0, |m, k| m | (1 << k))
}

/// A logged row's effect (replay applies a successful write).
fn apply_row(v: View, r: &Row) -> View {
    if r.kind == RowKind::Op && r.ret == Ret::Ok {
        force(v, r.op)
    } else {
        v
    }
}

fn fold_rows(v: View, rows: &[Row]) -> View {
    rows.iter().fold(v, apply_row)
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
    count_in(&s.streams[ROOT as usize].rows, g)
}

/// The rows of stream `h` among `rows` (a prefix of the root's journal).
fn count_in(rows: &[Row], h: u8) -> usize {
    rows.iter().filter(|r| r.stream == h).count()
}

/// The cut of stream `g`'s end, if the root's journal records one.
fn ended(s: &State, g: u8) -> Option<u8> {
    s.streams[ROOT as usize]
        .rows
        .iter()
        .find_map(|r| match r.kind {
            RowKind::Ended { stream, through } if stream == g => Some(through),
            _ => None,
        })
}

/// Row `idx` of node stream `g` lies past the cut of the stream's end.
fn void_row(s: &State, g: u8, idx: u8) -> bool {
    ended(s, g).is_some_and(|t| idx >= t)
}

/// The root's table: per directory, the stream that holds it, folded from
/// the root's journal (the first `upto` rows) over the initial grants.
/// `early`: count releases journaled in node streams but not yet appended
/// (the `root_executes_before_at` knob).
fn table_upto(m: &NodeStreamModel, s: &State, upto: usize, early: bool) -> [u8; DIRS] {
    let mut t = [ROOT; DIRS];
    for (k, g) in m.grants.iter().enumerate() {
        for d in bits(g.dirs) {
            t[d] = k as u8 + 1;
        }
    }
    for r in &s.streams[ROOT as usize].rows[..upto] {
        match r.kind {
            RowKind::Grow { stream, dirs } => {
                for d in bits(dirs) {
                    t[d] = stream;
                }
            }
            RowKind::Ended { stream, .. } => {
                for x in t.iter_mut().filter(|x| **x == stream) {
                    *x = ROOT;
                }
            }
            RowKind::Released { dirs, .. } if r.stream != ROOT => {
                for d in bits(dirs) {
                    if t[d] == r.stream {
                        t[d] = ROOT;
                    }
                }
            }
            _ => {}
        }
    }
    if early {
        for g in 1..s.streams.len() {
            for r in &s.streams[g].rows {
                if let RowKind::Released { dirs, .. } = r.kind {
                    for d in bits(dirs) {
                        if t[d] == g as u8 {
                            t[d] = ROOT;
                        }
                    }
                }
            }
        }
    }
    t
}

/// The root's table as the root uses it.
fn table(m: &NodeStreamModel, s: &State) -> [u8; DIRS] {
    table_upto(
        m,
        s,
        s.streams[ROOT as usize].rows.len(),
        m.root_executes_before_at,
    )
}

/// The stream `op` is routed to by the root's table: the one stream of all
/// its keys, else the root (a cross-subtree op).
fn route(m: &NodeStreamModel, s: &State, op: WriteOp) -> u8 {
    let t = table(m, s);
    let dirs = op_dirs(op);
    let mut streams = bits(dirs).map(|d| t[d]);
    let first = streams.next().unwrap();
    let one = streams.all(|x| x == first);
    if one && !(m.per_subtree_streams && first != ROOT && dirs.count_ones() > 1) {
        first
    } else {
        ROOT
    }
}

/// Bitmask over keys the root holds by its table (its rebuild after a
/// claim).
fn root_keys(m: &NodeStreamModel, s: &State) -> View {
    let t = table_upto(m, s, s.streams[ROOT as usize].rows.len(), false);
    (0..View::BITS as u8)
        .filter(|&k| t[dir_of(k) as usize] == ROOT)
        .fold(0, |v, k| v | (1 << k))
}

/// The delegate's gate: it holds `dir` by its own view.
fn deleg_owns(m: &NodeStreamModel, s: &State, g: u8, dir: usize) -> bool {
    let st = &s.streams[g as usize];
    if st.stopped {
        return false;
    }
    let initial = m.grants[g as usize - 1].dirs & (1 << dir) != 0;
    let grown = s.streams[ROOT as usize]
        .rows
        .iter()
        .enumerate()
        .any(|(p, r)| {
            matches!(r.kind, RowKind::Grow { stream, dirs } if stream == g && dirs & (1 << dir) != 0)
                && (m.honour_uncommitted_grant || p < st.applied as usize)
        });
    let released = !m.execute_after_release
        && st
            .rows
            .iter()
            .any(|r| matches!(r.kind, RowKind::Released { dirs, .. } if dirs & (1 << dir) != 0));
    (initial || grown) && !released
}

/// Oracle (`recall_safety`): stream `g` may hold `dir` by the stores.
fn deleg_may_hold(m: &NodeStreamModel, s: &State, g: u8, dir: usize) -> bool {
    let rcp = root_committed_prefix(m, s);
    let initial = m.grants[g as usize - 1].dirs & (1 << dir) != 0;
    let grown = s.streams[ROOT as usize].rows[..rcp].iter().any(
        |r| matches!(r.kind, RowKind::Grow { stream, dirs } if stream == g && dirs & (1 << dir) != 0),
    );
    let released = s.streams[g as usize]
        .rows
        .iter()
        .any(|r| matches!(r.kind, RowKind::Released { dirs, .. } if dirs & (1 << dir) != 0));
    (initial || grown) && !released && ended(s, g).is_none()
}

/// The root's journal carries every node stream through `deps`.
fn deps_ok_root(m: &NodeStreamModel, s: &State, deps: &Deps) -> bool {
    let rows = &s.streams[ROOT as usize].rows;
    m.deps_ignored
        || (deps[0] as usize <= rows.len()
            && (1..s.streams.len()).all(|h| count_in(rows, h as u8) >= deps[h] as usize))
}

/// Delegate `g`'s installed prefix of the root's journal carries `deps`.
fn deps_ok_deleg(m: &NodeStreamModel, s: &State, g: u8, deps: &Deps) -> bool {
    let applied = s.streams[g as usize].applied as usize;
    let rows = &s.streams[ROOT as usize].rows[..applied];
    m.deps_ignored
        || (deps[0] as usize <= applied
            && (1..s.streams.len())
                .filter(|&h| h != g as usize)
                .all(|h| count_in(rows, h as u8) >= deps[h] as usize))
}

/// Where `op` stands at sequencer candidate `node`.
fn status(m: &NodeStreamModel, s: &State, node: u8, op: WriteOp, deps: &Deps) -> Status {
    let dirs = op_dirs(op);
    if node == s.root {
        let t = table(m, s);
        if bits(dirs).all(|d| t[d] == ROOT) {
            if deps_ok_root(m, s, deps) {
                Status::Run
            } else {
                Status::Blocked(true)
            }
        } else if route(m, s, op) == ROOT {
            Status::Blocked(false)
        } else {
            Status::Bounce
        }
    } else if let Some(g) = m
        .stream_of_node(node)
        .filter(|&g| !s.streams[g as usize].stopped)
    {
        if bits(dirs).all(|d| deleg_owns(m, s, g, d)) {
            if deps_ok_deleg(m, s, g, deps) {
                Status::Run
            } else {
                Status::Blocked(true)
            }
        } else {
            Status::Bounce
        }
    } else {
        Status::Bounce
    }
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
/// stream while it lives and honours, else (the seal) the longest copy
/// held by a live member of a configuration the root knows (the grant's,
/// or one whose record it has appended).
fn available(s: &State, g: u8, exec: u8) -> usize {
    let st = &s.streams[g as usize];
    if s.live(exec) && !st.stopped {
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
        .max(app)
}

/// Every answered row is in the root's journal or held by a live node from
/// which the root will append it, with the answered result.
pub fn acked_never_lost(m: &NodeStreamModel, s: &State) -> bool {
    s.acked.iter().all(|&(rid, ret, _, _)| {
        let root = &s.streams[ROOT as usize];
        if let Some(r) = root
            .rows
            .iter()
            .find(|r| r.stream == ROOT && !r.record() && r.rid == rid)
        {
            return r.ret == ret;
        }
        for g in 1..s.streams.len() {
            let st = &s.streams[g];
            if let Some(idx) = st.rows.iter().position(|r| !r.record() && r.rid == rid) {
                // A stopped holder is unreachable, not gone: its disk keeps
                // the row (the end waits for a member, C2e).
                let exec = m.executor(s, g as u8);
                return st.rows[idx].ret == ret
                    && (root_pos(s, g as u8, idx as u8).is_some()
                        || s.live(exec)
                        || idx < available(s, g as u8, exec));
            }
        }
        false
    })
}

/// I2(a): no rid has two live rows (the root's own, and every delegate's
/// not past its stream's cut).
pub fn exactly_once(s: &State) -> bool {
    let mut rids: Vec<Rid> = Vec::new();
    let root = s.streams[ROOT as usize]
        .rows
        .iter()
        .filter(|r| r.stream == ROOT);
    let delegs = s.streams[1..].iter().enumerate().flat_map(|(k, st)| {
        st.rows
            .iter()
            .filter(move |r| !void_row(s, k as u8 + 1, r.idx))
    });
    for r in root.chain(delegs).filter(|r| !r.record()) {
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

/// Every row of the root's journal returns at its position what it
/// returned when executed.
pub fn journal_valid(m: &NodeStreamModel, s: &State) -> bool {
    let mut v = m.initial_present;
    for r in &s.streams[ROOT as usize].rows {
        if r.kind == RowKind::Op {
            let (ret, next) = eval(v, r.op);
            if ret != r.ret {
                return false;
            }
            v = next;
        }
    }
    true
}

/// Every root row in a directory a node stream held comes after that
/// stream's release (or end) of it, and after every row of the stream
/// below the release (through `at`) that touched it.
pub fn released_subtree_sequenced_after_cut(m: &NodeStreamModel, s: &State) -> bool {
    let rows = &s.streams[ROOT as usize].rows;
    for (p, r) in rows.iter().enumerate() {
        if r.stream != ROOT || r.kind != RowKind::Op {
            continue;
        }
        let dirs = op_dirs(r.op);
        if dirs & m.delegated_dirs() == 0 {
            continue;
        }
        let t = table_upto(m, s, p, false);
        if bits(dirs).any(|d| t[d] != ROOT) {
            return false;
        }
        for g in 1..s.streams.len() {
            let gs = g as u8;
            let st = &s.streams[g];
            for d in bits(dirs) {
                let held = m.grants[g - 1].dirs & (1 << d) != 0
                    || rows[..p].iter().any(|x| {
                        matches!(x.kind, RowKind::Grow { stream, dirs } if stream == gs && dirs & (1 << d) != 0)
                    });
                if !held {
                    continue;
                }
                let below = st
                    .rows
                    .iter()
                    .position(|x| matches!(x.kind, RowKind::Released { dirs, .. } if dirs & (1 << d) != 0))
                    .unwrap_or(st.rows.len());
                let behind = st.rows[..below].iter().any(|x| {
                    x.kind == RowKind::Op
                        && op_dirs(x.op) & (1 << d) != 0
                        && !void_row(s, gs, x.idx)
                        && root_pos(s, gs, x.idx).is_none_or(|q| q > p)
                });
                if behind {
                    return false;
                }
            }
        }
    }
    true
}

/// M1.2's oracle: no answered row of an ended stream lies past its cut.
pub fn acked_at_or_below_cut(s: &State) -> bool {
    s.acked
        .iter()
        .all(|&(_, _, g, idx)| g == ROOT || !void_row(s, g, idx))
}

/// I2(c): every answered rid has one identity, its original origin,
/// wherever a live row of it is (the root's own rows, its appended copies,
/// the node streams below their cuts).
pub fn verbatim_identity(s: &State) -> bool {
    s.acked.iter().all(|&(rid, _, so, io)| {
        let same = |st: u8, idx: u8| st == so && idx == io;
        s.acked
            .iter()
            .all(|&(r2, _, s2, i2)| r2 != rid || same(s2, i2))
            && s.streams[ROOT as usize]
                .rows
                .iter()
                .all(|r| r.record() || r.rid != rid || same(r.stream, r.idx))
            && s.streams[1..].iter().enumerate().all(|(k, st)| {
                let g = k as u8 + 1;
                st.rows
                    .iter()
                    .all(|r| r.record() || r.rid != rid || void_row(s, g, r.idx) || same(g, r.idx))
            })
    })
}

/// The causal cut: every row of the root's journal comes after its `deps`
/// (so every prefix of it, the archive and each backup's copy, is closed),
/// and every live delegate's replica carries the `deps` of its own rows.
pub fn causal_cut(m: &NodeStreamModel, s: &State) -> bool {
    let rows = &s.streams[ROOT as usize].rows;
    let closed = |r: &Row, prefix: &[Row], p: usize, own: u8| {
        (0..s.streams.len()).all(|h| {
            let need = r.deps[h] as usize;
            if h == ROOT as usize {
                need <= p
            } else if h == own as usize {
                need <= r.idx as usize
            } else {
                count_in(prefix, h as u8) >= need
            }
        })
    };
    for (p, r) in rows.iter().enumerate() {
        if r.kind == RowKind::Op && !closed(r, &rows[..p], p, r.stream) {
            return false;
        }
    }
    for g in 1..s.streams.len() {
        let gs = g as u8;
        let node = m.grants[g - 1].node;
        let st = &s.streams[g];
        if !s.live(node) || node == s.root {
            continue;
        }
        let applied = st.applied as usize;
        for r in st.rows.iter().filter(|r| r.kind == RowKind::Op) {
            if !void_row(s, gs, r.idx) && !closed(r, &rows[..applied], applied, gs) {
                return false;
            }
        }
    }
    true
}

/// The replicas a reader may read: the root's journal and each of its
/// prefixes held (the archive, the backups' copies), and each live
/// delegate's replica.
fn replica_views(m: &NodeStreamModel, s: &State) -> Vec<View> {
    let root = &s.streams[ROOT as usize];
    let mut out = vec![
        fold_rows(m.initial_present, &root.rows),
        fold_rows(m.initial_present, &root.rows[..s.shipped as usize]),
    ];
    for b in 0..m.n() {
        if s.live(b as u8) && root.held[b] > 0 {
            let h = (root.held[b] as usize).min(root.rows.len());
            out.push(fold_rows(m.initial_present, &root.rows[..h]));
        }
    }
    if m.replicas {
        for (k, g) in m.grants.iter().enumerate() {
            if s.live(g.node) && g.node != s.root {
                out.push(s.streams[k + 1].rview);
            }
        }
    }
    out
}

/// The probe's data write was answered `Ok`.
fn data_acked(m: &NodeStreamModel, s: &State, data: Key) -> bool {
    s.acked.iter().any(|&(rid, ret, _, _)| {
        ret == Ret::Ok
            && m.scripts[rid.node as usize].get(rid.seq as usize) == Some(&WriteOp::Create(data))
    })
}

/// A reader never sees the marker without the data once the data's write
/// was answered (the requester writes the marker after it).
pub fn marker_order(m: &NodeStreamModel, s: &State) -> bool {
    let Some((marker, data)) = m.probe else {
        return true;
    };
    let bit = |v: View, k: Key| v & (1 << k) != 0;
    !data_acked(m, s, data)
        || replica_views(m, s)
            .into_iter()
            .all(|v| !bit(v, marker) || bit(v, data))
}

/// Nothing moves: no message, no parked op or answer, every script done,
/// every stream appended (or ended), the journal shipped, and every live
/// delegate's replica caught up.
fn quiescent(m: &NodeStreamModel, s: &State) -> bool {
    let root_len = s.streams[ROOT as usize].rows.len();
    s.net.is_empty()
        && s.waiting.is_empty()
        && s.pending.is_empty()
        && s.nodes.iter().enumerate().all(|(i, n)| {
            !s.live(i as u8) || (!n.outstanding && n.next as usize == m.scripts[i].len())
        })
        && (1..s.streams.len()).all(|g| {
            let gs = g as u8;
            let exec = m.executor(s, gs);
            let st = &s.streams[g];
            ended(s, gs).is_some()
                || (!st.stopped && s.live(exec) && appended(s, gs) == st.rows.len())
        })
        && s.shipped as usize == root_len
        && m.grants.iter().enumerate().all(|(k, g)| {
            let st = &s.streams[k + 1];
            !s.live(g.node)
                || g.node == s.root
                || (st.applied as usize == root_len && st.durable == s.shipped)
        })
}

/// At quiescence every live delegate's replica is the log.
pub fn converged_at_quiescence(m: &NodeStreamModel, s: &State) -> bool {
    if !quiescent(m, s) {
        return true;
    }
    let log = fold_rows(m.initial_present, &s.streams[ROOT as usize].rows);
    m.grants
        .iter()
        .enumerate()
        .all(|(k, g)| !s.live(g.node) || g.node == s.root || s.streams[k + 1].rview == log)
}

/// Per key, the history is linearizable against a boolean register.
pub fn per_key_linearizable(m: &NodeStreamModel, s: &State) -> bool {
    let skipped: Vec<(Key, Rid)> = s
        .history
        .iter()
        .filter_map(|e| match e {
            HEvt::Return {
                ret: Ret::Skipped,
                rid,
                key,
                ..
            } => Some((*key, *rid)),
            _ => None,
        })
        .collect();
    let mut keys: Vec<Key> = s
        .history
        .iter()
        .map(|e| match e {
            HEvt::Invoke { key, .. } | HEvt::Return { key, .. } => *key,
        })
        .collect();
    keys.sort();
    keys.dedup();
    for k in keys {
        let mut tester = LinearizabilityTester::<usize, KeySpec>::new(KeySpec {
            present: m.initial_present & (1 << k) != 0,
        });
        for evt in &s.history {
            let r = match evt {
                HEvt::Invoke { key, .. } | HEvt::Return { key, .. } if *key != k => continue,
                HEvt::Invoke { rid, .. } if skipped.contains(&(k, *rid)) => continue,
                HEvt::Return {
                    ret: Ret::Skipped, ..
                } => continue,
                HEvt::Invoke { node, op, .. } => tester.on_invoke(*node as usize, *op),
                HEvt::Return { node, ret, .. } => tester.on_return(*node as usize, *ret),
            };
            if r.is_err() {
                return false;
            }
        }
        if !tester.is_consistent() {
            return false;
        }
    }
    true
}

fn invoke(s: &mut State, node: u8, rid: Rid, op: WriteOp) {
    let mut push = |key: Key, op: KeyOp| {
        s.history.push(HEvt::Invoke { node, key, op, rid });
    };
    match op {
        WriteOp::Create(k) => push(k, KeyOp::Create),
        WriteOp::Unlink(k) => push(k, KeyOp::Unlink),
        WriteOp::Move(a, b) => {
            push(a, KeyOp::Unlink);
            push(b, KeyOp::Put);
        }
    }
}

fn returned(s: &mut State, node: u8, rid: Rid, op: WriteOp, ret: Ret) {
    match op {
        WriteOp::Create(k) | WriteOp::Unlink(k) => {
            s.history.push(HEvt::Return {
                node,
                key: k,
                ret,
                rid,
            });
        }
        WriteOp::Move(a, b) => {
            s.history.push(HEvt::Return {
                node,
                key: a,
                ret,
                rid,
            });
            s.history.push(HEvt::Return {
                node,
                key: b,
                ret: if ret == Ret::Ok {
                    Ret::Ok
                } else {
                    Ret::Skipped
                },
                rid,
            });
        }
    }
}

/// Requester `i` receives the answer to `rid` from the row `(stream, idx)`.
fn requester_answered(m: &NodeStreamModel, s: &mut State, rid: Rid, ret: Ret, stream: u8, idx: u8) {
    let i = rid.node as usize;
    let live = s.live(rid.node);
    let node = &mut s.nodes[i];
    if !(live && node.outstanding && node.next == rid.seq + 1) {
        // A duplicate (a retry's second answer, a replay's) is dropped.
        return;
    }
    node.outstanding = false;
    if m.causal {
        let o = &mut node.observed[stream as usize];
        *o = (*o).max(idx + 1);
    }
    if m.history {
        let op = m.scripts[i][rid.seq as usize];
        returned(s, rid.node, rid, op, ret);
    }
}

/// Delegate `g`'s replica: its installed prefix of the root's journal, then
/// its own rows not ordered by the mark, not retired by the durable apply
/// and not past an installed cut.
fn recompute_rview(m: &NodeStreamModel, s: &mut State, g: u8) {
    let root = &s.streams[ROOT as usize].rows;
    let st = &s.streams[g as usize];
    let applied = &root[..st.applied as usize];
    let durable = &root[..st.durable as usize];
    let cut = applied.iter().find_map(|r| match r.kind {
        RowKind::Ended { stream, through } if stream == g => Some(through),
        _ => None,
    });
    let mut v = fold_rows(m.initial_present, applied);
    for r in &st.rows {
        let local = r.idx >= st.mark
            && cut.is_none_or(|t| r.idx < t)
            && !durable.iter().any(|x| x.stream == g && x.idx == r.idx);
        if local {
            v = apply_row(v, r);
        }
    }
    s.streams[g as usize].rview = v;
}

/// The root journals stream `g`'s end at its cursor.
fn end_stream(m: &NodeStreamModel, s: &mut State, g: u8) {
    let through = appended(s, g) as u8;
    let root = &mut s.streams[ROOT as usize];
    let pos = root.rows.len() as u8;
    let version = (root.cfgs.len() - 1) as u8;
    root.rows.push(Row {
        stream: ROOT,
        idx: pos,
        rid: Rid {
            node: RECORD_NODE,
            seq: pos,
        },
        op: WriteOp::Create(0),
        ret: Ret::Ok,
        version,
        kind: RowKind::Ended { stream: g, through },
        deps: [0; MAXS],
    });
    if m.drop_member_copy {
        for h in s.streams[g as usize].held.iter_mut() {
            *h = 0;
        }
    }
}

fn execute(m: &NodeStreamModel, s: &mut State, at: u8, rid: Rid, op: WriteOp, deps: Deps) {
    let stream = if at == s.root {
        ROOT
    } else {
        m.stream_of_node(at).unwrap()
    };
    let dirs = op_dirs(op);
    let idx = s.streams[stream as usize].rows.len() as u8;
    if stream != ROOT {
        if bits(dirs).any(|d| !deleg_may_hold(m, s, stream, d)) {
            s.violate("recall_safety", rid, stream, idx);
        }
        if dirs.count_ones() > 1 {
            s.saw |= SAW_TWO_SUBTREES;
        }
        let g = m.grants[stream as usize - 1];
        if dirs & !g.dirs != 0 {
            s.saw |= SAW_GROWN;
        }
    } else if dirs & m.delegated_dirs() != 0 {
        s.saw |= SAW_RELEASED_EXEC;
    }
    // The root executes against its journal, a delegate against its keys.
    let base = if stream == ROOT {
        fold_rows(m.initial_present, &s.streams[ROOT as usize].rows)
    } else {
        s.view
    };
    let (ret, after) = eval(base, op);
    let mask = op_keys(op);
    s.view = (s.view & !mask) | (after & mask);
    let executor = at;
    let lone = m.per_row_class && stream != ROOT && !s.peer_in_budget;
    let st = &mut s.streams[stream as usize];
    let row = Row {
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
        kind: RowKind::Op,
        deps,
    };
    st.rows.push(row);
    if m.replicas && stream != ROOT {
        st.rview = apply_row(st.rview, &row);
    }
    s.pending.push(Pending {
        executor,
        stream,
        idx,
    });
}

/// Sequencer candidate `at` receives `rid`: completed-dedup, else execute,
/// park, or answer `NotOwner`.
fn receive(m: &NodeStreamModel, s: &mut State, at: u8, rid: Rid, op: WriteOp, deps: Deps) {
    let mine = if at == s.root {
        Some(ROOT)
    } else {
        m.stream_of_node(at)
            .filter(|&g| !s.streams[g as usize].stopped)
    };
    if let (Some(stream), false) = (mine, m.no_dedup) {
        let found = s.streams[stream as usize]
            .rows
            .iter()
            .position(|r| !r.record() && r.rid == rid);
        if let Some(idx) = found {
            let idx = idx as u8;
            s.saw |= SAW_DEDUP;
            if stream == ROOT && s.streams[ROOT as usize].rows[idx as usize].stream != ROOT {
                s.saw |= SAW_APPENDED_DEDUP;
            }
            if !s.pending.iter().any(|p| p.stream == stream && p.idx == idx) {
                s.pending.push(Pending {
                    executor: at,
                    stream,
                    idx,
                });
            }
            return;
        }
    }
    if s.waiting.iter().any(|w| w.node == at && w.rid == rid) {
        return;
    }
    match status(m, s, at, op, &deps) {
        Status::Run => execute(m, s, at, rid, op, deps),
        Status::Blocked(for_deps) => {
            if for_deps {
                s.saw |= SAW_DEPS_WAIT;
            }
            s.waiting.push(Wait {
                node: at,
                rid,
                op,
                deps,
            });
        }
        Status::Bounce => {
            s.saw |= SAW_NOT_OWNER;
            s.net.push(Msg::NotOwner { rid });
        }
    }
}

/// Node `i` sends `rid` to the sequencer of its stream by the root's table.
fn submit(m: &NodeStreamModel, s: &mut State, i: u8, rid: Rid, op: WriteOp) {
    let stream = route(m, s, op);
    let to = m.executor(s, stream);
    let deps = s.nodes[i as usize].observed;
    s.nodes[i as usize].sent_to = to;
    if to == i {
        receive(m, s, i, rid, op, deps);
    } else {
        s.net.push(Msg::Fwd { to, rid, op, deps });
    }
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
                    observed: [0; MAXS],
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
                    stopped: false,
                    applied: 0,
                    mark: 0,
                    durable: 0,
                    rview: if self.replicas {
                        self.initial_present
                    } else {
                        0
                    },
                })
                .collect(),
            shipped: 0,
            view: self.initial_present,
            pending: Vec::new(),
            waiting: Vec::new(),
            net: Vec::new(),
            acked: Vec::new(),
            violation: None,
            saw: 0,
            root: 0,
            down: 0,
            crashes: 0,
            depositions: 0,
            peer_in_budget: true,
            history: Vec::new(),
            rescued: false,
            inherited: 0,
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
                if (node.next as usize) < self.scripts[i as usize].len() {
                    actions.push(Action::Start(i));
                }
            } else if !s.live(node.sent_to) || self.stopped_node(s, node.sent_to) {
                let op = self.scripts[i as usize][node.next as usize - 1];
                let to = self.executor(s, route(self, s, op));
                if s.live(to) && !self.stopped_node(s, to) {
                    actions.push(Action::Retry(i));
                }
            }
        }
        for k in 0..s.net.len() {
            actions.push(Action::Deliver(k));
        }
        for (k, w) in s.waiting.iter().enumerate() {
            if s.live(w.node)
                && !matches!(status(self, s, w.node, w.op, &w.deps), Status::Blocked(_))
            {
                actions.push(Action::Exec(k));
            }
        }
        let rcp = root_committed_prefix(self, s);
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
                    && !st.stopped
                    && self.reconfigs.iter().any(|r| r.0 == stream)
                    && bits(learners).all(|b| st.held[b] as usize == st.rows.len())
                {
                    actions.push(Action::Reconfig(stream));
                }
            }
            if stream == ROOT {
                continue;
            }
            let end = ended(s, stream);
            if end.is_none() && appended(s, stream) < available(s, stream, exec) {
                actions.push(Action::Append(stream));
            }
            let via_root = st.cfgs.iter().any(|c| {
                matches!(
                    self.rule(stream, c.backups),
                    Rule::Group { via_root: true, .. }
                )
            });
            let reported = via_root && root_reported(self, s, stream) > st.root_committed;
            if s.live(exec)
                && !st.stopped
                && (reported || root_archived(s, stream) > st.root_archived)
            {
                actions.push(Action::RootAck(stream));
            }
            let serving = s.live(exec) && !st.stopped && exec != s.root;
            if serving {
                // The root's request: a parked op needs a directory this
                // stream holds.
                let t = table(self, s);
                let mut asked = 0u8;
                for w in s.waiting.iter().filter(|w| w.node == s.root) {
                    for d in bits(op_dirs(w.op)) {
                        if t[d] == stream && deleg_owns(self, s, stream, d) {
                            asked |= 1 << d;
                        }
                    }
                }
                for d in bits(asked) {
                    actions.push(Action::Release {
                        stream,
                        dirs: 1 << d,
                    });
                }
                let owned = (1..DIRS).any(|d| deleg_owns(self, s, stream, d));
                if self.stream_releases.contains(&stream)
                    && owned
                    && !st
                        .rows
                        .iter()
                        .any(|r| matches!(r.kind, RowKind::Released { .. }))
                {
                    actions.push(Action::ReleaseAll(stream));
                }
                if self.ttl_ends.contains(&stream) {
                    actions.push(Action::Stop(stream));
                }
            }
            // C2e: a stream whose configuration names backups ends only
            // after one of them seals; with none live it waits for the
            // group (the operator's `abandon-group`, not modeled).
            let group = st.cfgs.last().unwrap().backups;
            let sealable = group == 0 || bits(group).any(|b| s.live(b as u8));
            if end.is_none() && self.ends(stream) && (st.stopped || !s.live(exec)) && sealable {
                actions.push(Action::End(stream));
            }
            if s.live(exec) && exec != s.root {
                if self.learns() && (st.applied as usize) < rcp {
                    actions.push(Action::Learn(stream));
                }
                if self.replicas && st.durable < s.shipped {
                    actions.push(Action::Durable(stream));
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
                    // A lone tenure's inherited tail is in the archive.
                    c != s.root
                        && (self.delegate_may_claim || !self.holds_grant(c))
                        && s.shipped >= s.inherited
                };
                if eligible && s.live(c) {
                    actions.push(Action::Depose { claimant: c });
                }
            }
        }
        if self.peer_toggle && s.peer_in_budget {
            actions.push(Action::TogglePeer);
        }
        if !self.grows.is_empty() {
            let t = table(self, s);
            for (k, &(g, dirs)) in self.grows.iter().enumerate() {
                let done = s.streams[ROOT as usize].rows.iter().any(
                    |r| matches!(r.kind, RowKind::Grow { stream, dirs: d } if stream == g && d == dirs),
                );
                if !done && ended(s, g).is_none() && bits(dirs).all(|d| t[d] == ROOT) {
                    actions.push(Action::Grow(k as u8));
                }
            }
        }
        if self.shadow_replay && !s.rescued {
            for &(rid, _, g, idx) in &s.acked {
                let i = rid.node;
                if g != ROOT
                    && s.live(i)
                    && !s.nodes[i as usize].outstanding
                    && void_row(s, g, idx)
                    && !s.streams[ROOT as usize]
                        .rows
                        .iter()
                        .any(|r| !r.record() && r.rid == rid)
                {
                    actions.push(Action::Rescue(i));
                }
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
                if self.history {
                    invoke(&mut s, i, rid, op);
                }
                submit(self, &mut s, i, rid, op);
            }
            Action::Retry(i) => {
                let seq = s.nodes[i as usize].next - 1;
                let op = self.scripts[i as usize][seq as usize];
                s.saw |= SAW_RETRY;
                submit(self, &mut s, i, Rid { node: i, seq }, op);
            }
            Action::Rescue(i) => {
                let &(rid, ..) = s.acked.iter().find(|&&(rid, _, g, idx)| {
                    rid.node == i && g != ROOT && void_row(&s, g, idx)
                })?;
                let op = self.scripts[i as usize][rid.seq as usize];
                s.rescued = true;
                submit(self, &mut s, i, rid, op);
            }
            Action::Deliver(k) => match s.net.remove(k) {
                Msg::Fwd { to, rid, op, deps } => {
                    // A message to a node that is down, or that stopped
                    // honouring (unreachable), is lost; the requester
                    // retries.
                    if s.live(to) && !self.stopped_node(&s, to) {
                        receive(self, &mut s, to, rid, op, deps);
                    }
                }
                Msg::Reply {
                    rid,
                    ret,
                    stream,
                    idx,
                } => requester_answered(self, &mut s, rid, ret, stream, idx),
                Msg::NotOwner { rid } => {
                    let node = s.nodes[rid.node as usize];
                    if s.live(rid.node) && node.outstanding && node.next == rid.seq + 1 {
                        let op = self.scripts[rid.node as usize][rid.seq as usize];
                        submit(self, &mut s, rid.node, rid, op);
                    }
                }
            },
            Action::Exec(k) => {
                let w = s.waiting.remove(k);
                match status(self, &s, w.node, w.op, &w.deps) {
                    Status::Run => execute(self, &mut s, w.node, w.rid, w.op, w.deps),
                    Status::Bounce => {
                        s.saw |= SAW_NOT_OWNER;
                        s.net.push(Msg::NotOwner { rid: w.rid });
                    }
                    Status::Blocked(_) => return None,
                }
            }
            Action::Release { stream, dirs } => {
                // Justified only by an op whose keys span node streams.
                let t = table_upto(self, &s, s.streams[ROOT as usize].rows.len(), false);
                let spans = |op: WriteOp| {
                    let mut st = bits(op_dirs(op)).map(|d| t[d]);
                    let first = st.next().unwrap();
                    !st.all(|x| x == first)
                };
                let justified = s
                    .waiting
                    .iter()
                    .any(|w| w.node == s.root && op_dirs(w.op) & dirs != 0 && spans(w.op));
                let idx = s.streams[stream as usize].rows.len() as u8;
                if !justified {
                    s.violate(
                        "two_subtrees_one_order",
                        Rid {
                            node: RECORD_NODE,
                            seq: idx,
                        },
                        stream,
                        idx,
                    );
                }
                push_release(&mut s, stream, dirs);
            }
            Action::ReleaseAll(g) => {
                let dirs = (1..DIRS)
                    .filter(|&d| deleg_owns(self, &s, g, d))
                    .fold(0u8, |m, d| m | (1 << d));
                s.saw |= SAW_STREAM_RELEASED;
                push_release(&mut s, g, dirs);
            }
            Action::Grow(k) => {
                let (g, dirs) = self.grows[k as usize];
                let root = &mut s.streams[ROOT as usize];
                let pos = root.rows.len() as u8;
                let version = (root.cfgs.len() - 1) as u8;
                root.rows.push(Row {
                    stream: ROOT,
                    idx: pos,
                    rid: Rid {
                        node: RECORD_NODE,
                        seq: pos,
                    },
                    op: WriteOp::Create(0),
                    ret: Ret::Ok,
                    version,
                    kind: RowKind::Grow { stream: g, dirs },
                    deps: [0; MAXS],
                });
            }
            Action::Stop(g) => {
                let node = self.executor(&s, g);
                s.streams[g as usize].stopped = true;
                s.pending.retain(|p| p.executor != node);
                s.waiting.retain(|w| w.node != node);
            }
            Action::End(g) => {
                if !self.end_before_seal {
                    let exec = self.executor(&s, g);
                    let avail = available(&s, g, exec);
                    // The verbatim tail, in one step.
                    while appended(&s, g) < avail {
                        let row = s.streams[g as usize].rows[appended(&s, g)];
                        s.streams[ROOT as usize].rows.push(row);
                        s.saw |= SAW_MEMBER_SEAL;
                    }
                }
                end_stream(self, &mut s, g);
                s.saw |= SAW_TTL_END;
            }
            Action::Learn(g) => {
                let row = s.streams[ROOT as usize].rows[s.streams[g as usize].applied as usize];
                let own_keys = {
                    let st = &s.streams[g as usize];
                    let root = &s.streams[ROOT as usize].rows[..st.applied as usize];
                    st.rows
                        .iter()
                        .filter(|r| {
                            r.kind == RowKind::Op
                                && root.iter().any(|x| x.stream == g && x.idx == r.idx)
                        })
                        .fold(0, |m, r| m | op_keys(r.op))
                };
                let st = &mut s.streams[g as usize];
                st.applied += 1;
                if row.stream == g {
                    st.mark = st.mark.max(row.idx + 1);
                }
                let end_or_release = matches!(row.kind, RowKind::Ended { stream, .. } if stream == g)
                    || (row.stream == g && matches!(row.kind, RowKind::Released { .. }));
                if self.l1_mark_cleared_by_stream && end_or_release {
                    st.mark = 0;
                }
                if row.stream == ROOT && row.kind == RowKind::Op && op_keys(row.op) & own_keys != 0
                {
                    s.saw |= SAW_L1;
                }
                if self.replicas {
                    recompute_rview(self, &mut s, g);
                }
            }
            Action::Durable(g) => {
                let shipped = s.shipped;
                let own = count_in(&s.streams[ROOT as usize].rows[..shipped as usize], g) as u8;
                let st = &mut s.streams[g as usize];
                // The segment path skips what the replica holds: own rows as
                // own, streamed rows as confirmed; only new rows apply, in
                // log order.
                let fresh = shipped > st.applied;
                if fresh {
                    st.applied = shipped;
                    st.mark = st.mark.max(own);
                }
                st.durable = shipped;
                if fresh {
                    recompute_rview(self, &mut s, g);
                }
            }
            Action::Replicate { stream, member } => {
                s.streams[stream as usize].held[member as usize] += 1;
            }
            Action::Append(g) => {
                let k = appended(&s, g);
                let row = s.streams[g as usize].rows[k];
                let exec = self.executor(&s, g);
                if !s.live(exec) || s.streams[g as usize].stopped {
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
                    s.violate("ack_implies_committed_by_config", row.rid, p.stream, p.idx);
                }
                let entry = (row.rid, row.ret, row.stream, row.idx);
                if !s.acked.contains(&entry) {
                    s.acked.push(entry);
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
                let (backups, recorded) = if p.stream == ROOT {
                    let v = cfg_at(&st.cfgs, p.idx);
                    (st.cfgs[v].backups, st.cfgs[..=v].iter().any(|c| c.record))
                } else if row.version == LONE {
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
                    requester_answered(self, &mut s, row.rid, row.ret, row.stream, row.idx);
                } else {
                    s.saw |= SAW_FORWARD;
                    s.net.push(Msg::Reply {
                        rid: row.rid,
                        ret: row.ret,
                        stream: row.stream,
                        idx: row.idx,
                    });
                }
            }
            Action::Crash(i) => {
                s.down |= 1 << i;
                s.crashes += 1;
                s.pending.retain(|p| p.executor != i);
                s.waiting.retain(|w| w.node != i);
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
                let nb = backups & !(1 << claimant) & !(1 << old);
                st.cfgs.push(Cfg {
                    start: len,
                    backups: nb,
                    record: false,
                });
                s.root = claimant;
                s.inherited = if self.max_depositions >= 2 && nb == 0 {
                    len
                } else {
                    0
                };
                // The deposed root's parked answers and ops die with its
                // tenure.
                s.pending.retain(|p| p.stream != ROOT);
                s.waiting.retain(|w| w.node != old);
                // A delegate taking the root re-appends its own rows the
                // adopted journal lacks, verbatim, then ends its generation.
                if let Some(g) = self.stream_of_node(claimant) {
                    if ended(&s, g).is_none() {
                        if !self.no_reappend {
                            let k = appended(&s, g);
                            let tail = s.streams[g as usize].rows[k..].to_vec();
                            if !tail.is_empty() {
                                s.saw |= SAW_DELEG_CLAIM;
                            }
                            s.streams[ROOT as usize].rows.extend(tail);
                        }
                        end_stream(self, &mut s, g);
                        s.streams[g as usize].stopped = true;
                        let moved: Vec<Pending> = s
                            .pending
                            .iter()
                            .filter(|p| p.executor == claimant && p.stream == g)
                            .copied()
                            .collect();
                        s.pending
                            .retain(|p| !(p.executor == claimant && p.stream == g));
                        for p in moved {
                            if let Some(q) = root_pos(&s, g, p.idx) {
                                s.pending.push(Pending {
                                    executor: claimant,
                                    stream: ROOT,
                                    idx: q as u8,
                                });
                            }
                        }
                    }
                }
                // Installed prefixes never pass the adopted journal.
                let root_len = s.streams[ROOT as usize].rows.len() as u8;
                for g in 1..s.streams.len() {
                    let st = &mut s.streams[g];
                    if st.applied > root_len || st.durable > root_len {
                        st.applied = st.applied.min(root_len);
                        st.durable = st.durable.min(root_len);
                        if self.replicas {
                            recompute_rview(self, &mut s, g as u8);
                        }
                    }
                }
                // The new root's keys are its journal's.
                let mask = root_keys(self, &s);
                let v = fold_rows(self.initial_present, &s.streams[ROOT as usize].rows);
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
                    kind: RowKind::Cfg,
                    deps: [0; MAXS],
                });
                st.reconfigured = true;
            }
            Action::TogglePeer => {
                s.peer_in_budget = false;
            }
        }
        if self.replicas && quiescent(self, &s) {
            s.saw |= SAW_QUIESCENT;
        }
        if let Some((marker, data)) = self.probe {
            let bit = |v: View, k: Key| v & (1 << k) != 0;
            if replica_views(self, &s)
                .into_iter()
                .any(|v| bit(v, marker) && bit(v, data))
            {
                s.saw |= SAW_MARKER_AND_DATA;
            }
        }
        s.net.sort_unstable();
        s.pending.sort_unstable();
        s.waiting.sort_unstable();
        s.acked.sort_unstable();
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("ack_implies_committed_by_config", |_, s: &State| {
                s.violation
                    .is_none_or(|v| v.property != "ack_implies_committed_by_config")
            }),
            Property::always("acked_never_lost", |m: &NodeStreamModel, s: &State| {
                acked_never_lost(m, s)
            }),
            Property::always("exactly_once", |_, s: &State| exactly_once(s)),
            Property::always("row_version_matches_config", |_, s: &State| {
                row_version_matches_config(s)
            }),
            Property::always("recall_safety", |_, s: &State| {
                s.violation.is_none_or(|v| v.property != "recall_safety")
            }),
            Property::always(
                "released_subtree_sequenced_after_cut",
                |m: &NodeStreamModel, s: &State| released_subtree_sequenced_after_cut(m, s),
            ),
            Property::always("two_subtrees_one_order", |_, s: &State| {
                s.violation
                    .is_none_or(|v| v.property != "two_subtrees_one_order")
            }),
            Property::always("journal_valid", |m: &NodeStreamModel, s: &State| {
                journal_valid(m, s)
            }),
            Property::always("acked_at_or_below_cut", |_, s: &State| {
                acked_at_or_below_cut(s)
            }),
            Property::always("verbatim_identity", |_, s: &State| verbatim_identity(s)),
            Property::sometimes("all_answered", |m: &NodeStreamModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| !n.outstanding && n.next as usize == sc.len())
            }),
            Property::sometimes("forward_answered", |_, s: &State| s.saw & SAW_FORWARD != 0),
        ];
        if self.history {
            props.push(Property::always(
                "per_key_linearizable",
                |m: &NodeStreamModel, s: &State| per_key_linearizable(m, s),
            ));
        }
        if self.causal {
            props.push(Property::always(
                "causal_cut",
                |m: &NodeStreamModel, s: &State| causal_cut(m, s),
            ));
            props.push(Property::sometimes("deps_waited", |_, s: &State| {
                s.saw & SAW_DEPS_WAIT != 0
            }));
        }
        if self.probe.is_some() {
            props.push(Property::always(
                "marker_order",
                |m: &NodeStreamModel, s: &State| marker_order(m, s),
            ));
            props.push(Property::sometimes(
                "marker_and_data_read",
                |_, s: &State| s.saw & SAW_MARKER_AND_DATA != 0,
            ));
        }
        if self.replicas {
            props.push(Property::always(
                "converged_at_quiescence",
                |m: &NodeStreamModel, s: &State| converged_at_quiescence(m, s),
            ));
            props.push(Property::sometimes("quiescent", |_, s: &State| {
                s.saw & SAW_QUIESCENT != 0
            }));
        }
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
        // Chunk 4: a cross-subtree op (one whose keys span node streams).
        let cross = self.scripts.iter().flatten().any(|&op| self.spans(op));
        if cross {
            props.push(Property::sometimes(
                "released_then_root_executes",
                |_, s: &State| s.saw & SAW_RELEASED_EXEC != 0,
            ));
        }
        // A delegate's own op in a directory a cross-subtree op releases:
        // a stale view meets the release (`NotOwner`), and the root's row
        // follows the delegate's own on a key (L1's order).
        let cross_dirs = self
            .scripts
            .iter()
            .flatten()
            .filter(|&&op| self.spans(op))
            .fold(0, |m, &op| m | op_dirs(op));
        let own = |op: WriteOp, i: usize| {
            !self.spans(op)
                && op_dirs(op) & cross_dirs & self.delegated_dirs() != 0
                && self.holds_grant(i as u8)
        };
        let stale = self
            .scripts
            .iter()
            .enumerate()
            .any(|(i, sc)| sc.iter().any(|&op| own(op, i)));
        if stale {
            props.push(Property::sometimes(
                "not_owner_reresolved",
                |_, s: &State| s.saw & SAW_NOT_OWNER != 0,
            ));
        }
        let two = self.scripts.iter().flatten().any(|&op| {
            op_dirs(op).count_ones() > 1 && self.grants.iter().any(|g| op_dirs(op) & !g.dirs == 0)
        });
        if two {
            props.push(Property::sometimes(
                "two_subtrees_at_delegate",
                |_, s: &State| s.saw & SAW_TWO_SUBTREES != 0,
            ));
        }
        if !self.grows.is_empty() {
            props.push(Property::sometimes(
                "grown_subtree_executed",
                |_, s: &State| s.saw & SAW_GROWN != 0,
            ));
        }
        if !self.stream_releases.is_empty() {
            props.push(Property::sometimes("stream_released", |_, s: &State| {
                s.saw & SAW_STREAM_RELEASED != 0
            }));
        }
        if !self.ttl_ends.is_empty() {
            props.push(Property::sometimes("ttl_end", |_, s: &State| {
                s.saw & SAW_TTL_END != 0
            }));
            props.push(Property::sometimes(
                "appended_row_deduped",
                |_, s: &State| s.saw & SAW_APPENDED_DEDUP != 0,
            ));
        }
        let ends_later =
            !self.ttl_ends.is_empty() || (self.delegate_may_claim && self.max_depositions >= 2);
        if ends_later && has(|g| g.backups != 0) && !self.end_before_seal && !self.drop_member_copy
        {
            props.push(Property::sometimes("member_copy_sealed", |_, s: &State| {
                s.saw & SAW_MEMBER_SEAL != 0
            }));
        }
        if self.delegate_may_claim && self.max_depositions > 0 && !self.no_reappend {
            props.push(Property::sometimes(
                "delegate_reappended",
                |_, s: &State| s.saw & SAW_DELEG_CLAIM != 0,
            ));
        }
        if self.replicas && stale {
            props.push(Property::sometimes(
                "l1_own_row_then_root_row",
                |_, s: &State| s.saw & SAW_L1 != 0,
            ));
        }
        props
    }
}

/// Stream `g`'s holder journals the release of `dirs`.
fn push_release(s: &mut State, g: u8, dirs: u8) {
    let st = &mut s.streams[g as usize];
    let idx = st.rows.len() as u8;
    let at = st
        .rows
        .iter()
        .filter(|r| r.kind == RowKind::Op && op_dirs(r.op) & dirs != 0)
        .map(|r| r.idx + 1)
        .max()
        .unwrap_or(0);
    let version = (st.cfgs.len() - 1) as u8;
    st.rows.push(Row {
        stream: g,
        idx,
        rid: Rid {
            node: RECORD_NODE,
            seq: idx,
        },
        op: WriteOp::Create(0),
        ret: Ret::Ok,
        version,
        kind: RowKind::Released { dirs, at },
        deps: [0; MAXS],
    });
}
