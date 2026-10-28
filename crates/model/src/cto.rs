//! Plan 30 §M8: `cto=strict` — ReadIndex, read delegations with recall,
//! and the close-to-open property, over clocks with bounded drift.
//!
//! # Why a model of its own
//!
//! The authority model ([`crate::protocol`]) already checks everything
//! the lease, the log, forwarding, stranding and positions do. What M8
//! adds is orthogonal to all of that and needs something that model
//! deliberately leaves out: **clocks**. A read delegation is a
//! time-bounded promise, and whether it is safe depends on how two
//! nodes' clocks can disagree. Adding per-node clocks, delegation tables
//! and recalls to the authority model would multiply a state space that
//! already needs 30M states for its M6 configurations, so this module is
//! a second, focused `stateright::Model` over the same abstractions:
//!
//! - the durable log, with the takeover's epoch marker (an entry of the
//!   new epoch) and one file `X` whose every write is a new *version*;
//! - each node's replica as an applied prefix of that log;
//! - the lease register (holder, epoch, expiry in the holder's clock,
//!   released), renewals and takeovers, with exactly the lease's margin
//!   discipline (`LeaseView::usable`: a holder uses its lease only while
//!   `local < expires − lease_margin`; anyone may take over once its own
//!   `local ≥ expires`);
//! - a global real time `t` (a bounded [`Action::Tick`]) and, per node, a
//!   clock with a wall offset `|off[i]| ≤ max_offset` chosen
//!   nondeterministically at start, optionally stepping within that bound
//!   ([`Action::Jump`]: an NTP step), and, plan 43 C7a, an oscillator rate
//!   (`rate_period`, `max_rate`) read through the honour, wait and wall
//!   readings of [`crate::clk`] (below; with `clock_split` off and no rate
//!   it is the one clock `local = t + off[i]` the model had);
//! - a lossy, reordering network (an unreachable delegate is a dropped
//!   recall or recall ack).
//!
//! What it abstracts away, each justified by the authority model: a
//! write is durable at execution (the L2 window of unshipped acks is M9's,
//! and M6's `Tentative` rule already exempts it from every session
//! property); only the current epoch's holder may append (the log's
//! fencing); stranding and replay are not modeled (they are M3's and the
//! authority model's).
//!
//! # The protocol (what `crates/authority/src/core/readindex.rs` does)
//!
//! - **ReadIndex.** A strict read on a node that is not a usable holder
//!   asks the register's holder for its position. A usable holder
//!   answers with its applied position (it is authoritative); the reader
//!   waits until its own replica reaches it, then reads.
//! - **Grant.** With the answer the holder may grant a read delegation:
//!   `ttl = min(deleg_ttl, lease_expires − lease_margin − now)` (the grant
//!   never outlives the lease that backs it, in the holder's clock), and
//!   it records the grant as live until `now + ttl + seq_margin`.
//! - **Honour.** The reader measures from when it *sent* the request:
//!   it serves reads locally under the delegation only while `local <
//!   sent + ttl − deleg_margin`, and always waits for the grant's
//!   position first.
//! - **Recall.** A holder that executed a write touching `X` withholds
//!   the write's acknowledgement (the close's completion) until every
//!   grant on `X` held by a node other than the writer is recalled (a
//!   `Recall` answered by `RecallAck`; the delegate drops the delegation
//!   before acking) or has expired by the holder's clock.
//! - **Overtaken grants.** A recall can overtake the reply carrying the
//!   grant it recalls (grant, then a write executes and recalls, and the
//!   network reorders). The delegate counts the recalls it receives; a
//!   reply's grant is installed only if none arrived since the request
//!   was sent (the reply's *position* still serves that one read, which
//!   started before the recalled write could complete).
//! - **Release.** A holder releases its lease (which lets another node
//!   take over at once) only when its table holds no live grant.
//! - **Epoch change.** A new holder starts with an empty table; old
//!   delegations are dead by then because every grant was capped by the
//!   old lease. A delegate that tails a marker of a higher epoch than its
//!   delegation's drops it (an optimization; safety does not need it).
//!
//! # Model action → code
//!
//! | Model | Code |
//! |---|---|
//! | `ReadLocal` (bounded) | `engine/src/view/ops.rs` `lookup`/`open`/`readdir` → `View::strict_read`, bounded arm: M6's `session_wait` only |
//! | `ReadLocal` (strict, the usable holder) | `strict_read` → `LeaseView::reads_locally` and `!Meta::read_delegated` (a key a live delegation owns goes to the core, which routes it to the delegate: `Core::deleg_read_owner`) |
//! | `ReadLocal` (strict, no live holder) | `Core::on_read_holder_learned` (lease claimable) → `read_tail` → `JobReq::TailToHead` → `ReadAnswer::Tailed` |
//! | `ReadUnderDelegation` / `FinishRead` | `strict_read` → `ReadDelegations::valid` → `Meta::session_wait_at(keys, grant position)` |
//! | `SendReadIndex` / `ReadIndexTimeout` | `strict_read` → `SyncRequest::ReadIndex` → `Control::ReadIndex` → `Core::on_read_index_control` → `PeerMsg::ReadIndex`; `Timer::ReadIndexTimeout` / `ReadIndexDeadline` |
//! | `Deliver(ReadIndex)` | `Core::on_read_index`: `LeaseState::usable` + not fenced, `maybe_grant` (lease cap, `note_grant_horizon`, `ReadDelegations::grant` *before* the position), per-key position |
//! | `Deliver(ReadIndexReply)` | `Core::on_read_index_reply` → `ReadDelegations::install(.., gen_at_send)` (until `sent + ttl − margin`) |
//! | `execute_write` recall set | `holder.rs` `on_mutate_request` → `recall_needed(.., Some(requester))` → `park_reply`; `client.rs` `execute_local` → `park_finish`; FUSE fast path `recall_after_local_write` → `Control::Recall`; inbox `inbox_recall_first` |
//! | `Deliver(Recall)` | `Core::on_delegation_recall` → `ReadDelegations::recall` (drop, bump the generation) → `PeerMsg::DelegationRecalled` |
//! | `Deliver(RecallAck)` / `Expire` | `Core::on_delegation_recalled` / `Timer::GrantExpiry` → `grant_done` → `complete_ready` (the parked reply, finish, control, release, inbox repoll) |
//! | `Release` (gated) | `jobs.rs` `issue_release` → `park_release_for_recalls` (`Phase::RecallBeforeRelease`) |
//! | `Takeover` (empty table) | a new tenure: every older grant was capped by its lease (`maybe_grant`) |
//! | `apply_next` voiding | `Replica::apply_segment` → `ReadDelegations::void_below_epoch` |
//! | `Jump` / offsets | clock error; the sim's `SimConfig::clock_skew_ms` |
//!
//! Not modeled, argued in PROGRESS.md (plan 30 M8): the restart
//! quarantine (a holder that restarts inside its own lease re-adopts the
//! same epoch, so it waits its persisted grant horizon out before any
//! acknowledgement — the model's `Takeover` already starts a fresh table
//! only at a *new* epoch) and the lone-node kernel-cache latch (a FUSE
//! concern).
//!
//! # The property
//!
//! `close_to_open` (always): a read that *starts* after another node's
//! close *completed* returns that close's version or a newer one. The
//! ghost `closed` is the highest version whose close has completed; a
//! read records it when it starts ([`NodeOp::ReadStart`]) and compares
//! at its return. A node's own writes are read-your-writes (M6), so the
//! scripts here always read on a different node than they write.
//!
//! # Plan 43 C7a: the readings
//!
//! Each node reads three clocks of its one oscillator ([`crate::clk`]): WALL
//! (what a step moves, what crosses nodes), HONOUR (a holder's rights; never
//! under-counts; follows a forward step once, ignores a backward one) and WAIT
//! (a waiter's outwaits and the liveness timers; never over-counts; ignores
//! every step), the last two scaled by the design's `rate_budget`. With
//! `clock_split` off every check is on the wall reading (the design before
//! C7-H1). Each former `local(i)` is now one of them:
//!
//! | Reading | Where (the code's) |
//! |---|---|
//! | HONOUR | a node's lease use (`usable`: `expires_h`, `LeaseView::usable`), its probe freshness (`last_probe`, `liveness_fresh`), a delegate's `Deleg.until` and the `sent` it stamps (`ReadIndex`), the cap of a grant by the lease, the holder's beats' end |
//! | WAIT | the sequencer's grant records and recall-by-expiry (`Grant.until`), the floor (`ack_floor`, `FloorPassed`), a release's live-grant test, a beat's `sent` and arrival (the lag) and the lease end it names (`expires_w`, the code's `holder_wait_ms`), the cap's `X` |
//! | WALL | a claimant's compare with the register's expiry under `claim_rule: Wall` (`claimable_by`, `unexpired`: a stamp of another node's clock; under `Observed` (plan 43 C7b) the claimant waits a TTL of its WAIT reading from its first read of the register's key instead, and a fast takeover is the permit alone), the expiry written to the register (`Reg::expires`) |
//!
//! `Jump(i, d)` replaces `off[i]` (a wall step); under the split it moves
//! WALL alone. [`CtoModel::step_node`] and `step_dir` restrict which node
//! steps and which way (a backward step of the delegate, a forward one of
//! the holder). `beat_span` is the code's span filter on the cap (`M·period
//! / (4·budget)` ticks): only beats sent within it of `X` count.
//!
//! # The drift margin
//!
//! *Offsets* (cross-node compares: the claimant against the register's
//! expiry, plan 43 C7b replaces them): let every clock be within `D` of real
//! time. A duration measured on one clock is then off by at most `2D` (the
//! offset can move by that much between the two readings). *Rate* (plan 43
//! C7a): the budget makes each local window exact (WAIT never over-counts,
//! HONOUR never under-counts, for an oscillator within it, up to a tick of
//! the staircase's rounding), and the margin is slack. The offsets'
//! arguments follow.
//! - *Recall override.* The delegate honours until real time `≤ s + ttl −
//!   deleg_margin + 2D` (`s` = when it sent the request); the holder
//!   overrides from real time `≥ g + ttl + seq_margin − 2D` (`g ≥ s` =
//!   when it granted). Safe when `deleg_margin + seq_margin > 4D`.
//! - *Epoch change.* The grant ends by `E − lease_margin` in the holder's
//!   clock (`E` = the lease's expiry), so the delegate honours until real
//!   time `≤ E − lease_margin − deleg_margin + 3D`; a new holder takes
//!   over at real time `≥ E − D`. Safe when `lease_margin + deleg_margin
//!   > 4D`.
//! - The lease itself needs `lease_margin > 2D` (holder stops at `E − M +
//!   D`, a taker starts at `E − D`).
//!
//! With all three margins equal to the lease's `M` (what the code does:
//! `Config::expiry_margin_ms` for all of them), every condition is the
//! lease's own `M > 2D`: read delegations tolerate exactly the clock
//! disagreement the lease already assumes. The tests show the margin is
//! load-bearing under drift and unnecessary without it (the delegate's
//! measurement starts before the holder's, so honest clocks need no
//! margin at all), and that the cap, the recall before a release, the
//! recall before an ack and the overtaken-grant rule each are
//! (`tests/cto.rs`).
//!
//! # Plan 30 §M9: a takeover before the lease expires
//!
//! A sealed backup, or any peer of an `ack=s3` holder, takes the lease
//! over while it is still live (`CtoModel::fast_takeover`, on a register
//! marked `fast`), which breaks the epoch-change argument above: the old
//! holder's grants are capped by a lease that has *not* ended, and the
//! old holder keeps answering strict reads (positions and grants) until
//! it notices. Two rules restore it, both in `Reg`'s clock discipline:
//! - **Liveness probe** (`probe_freshness`): a holder of a `fast`
//!   register answers a ReadIndex only if it probed the register (an S3
//!   request that would reveal a deposition: `Probe(h)`) within
//!   `takeover_window` by its clock; a probe that finds another holder
//!   deposes it at once. So every answer it gives was sent within
//!   `takeover_window + 2D` of the last moment before the successor's
//!   marker existed (the code: `note_s3_liveness`, and the immediate
//!   deposition on applying a higher epoch).
//! - **Acknowledgement floor** (`horizon_wait`): a successor of an
//!   unexpired lease whose tenure served strict reads (`Reg::marked`,
//!   the `granted_delegations` flag set by a CAS before the first answer)
//!   acknowledges no write until its clock reaches
//!   `marker time + takeover_window + deleg_ttl + 2·seq_margin`
//!   (`FloorPassed`; never cut short by the old expiry, a stamp of the
//!   old holder's clock). The delegate honours a grant made at `G` until
//!   `G + ttl − deleg_margin`; with `G < marker + takeover_window + 2D`
//!   the floor covers it iff `seq_margin + deleg_margin > 4D` — the
//!   lease's `M > 2D` again. A tenure that never served a strict read
//!   (`marked == false`) costs its successor nothing.
//!
//! # Plan 43 C7-F F-a: the floor capped by the old holder's beats
//!
//! With `beat_cap` the old holder (any node holding its lease, while its
//! clock is below the expiry: its beats stop there) may send a beat
//! (`Beat(h, b)`) to a node `b`, which records what it says on its own
//! clock: the smallest `local(b) − local(h)` over the beats (the arrival
//! minus the send: the code's `at − sent`, never the credit, which is an
//! instant of the holder's clock), the highest expiry they named and the
//! highest guard (the holder's own floor, if it has one: the code's
//! `Beat::guard_ms`; the model's holder has no takeover gate, so the
//! unknown guard never arises here). A takeover of the holder whose
//! beats `b` heard (same holder and epoch) ends its floor at
//! `min(bound, X + lag + seq_margin)`, `X = max(old expiry, named,
//! guard)`; none if that has passed. The code takes only beats sent
//! within `M/(4ρ)` of `X` (250·M at the defaults), so that the rate error
//! `2ρ·(X − sent)` stays under `M/2`: a beat sent before the renewal that
//! set `X` measures an interval the holder's wait clock under-counts.
//! `beat_span` is that filter; without it and with rates (`tests/cto.rs`,
//! `beat_cap_rate_*`) the cap ends the floor inside the holder's window. A
//! beat is delivered at once here: a later delivery makes the lag larger,
//! hence the floor longer.
//! `beat_cap_from_named_expiry` is the naive variant (`X` = what the
//! beats named, ignoring the lease the takeover replaced: a holder that
//! renews after its last beat is then outlived), and must be found
//! violating.
//!
//! `tests/cto.rs` shows each rule is load-bearing (a fast takeover
//! without the floor, and with the floor but without the probe, both
//! violate close-to-open) and that with both the design is clean,
//! including under drift within the margin.

use stateright::{Model, Property};

/// "No node".
pub const NONE: u8 = u8::MAX;

/// One step of a node's client script.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// Write a new version of `X` and close it.
    Write,
    /// Open `X` and read it.
    Read,
}

#[derive(Clone, Debug)]
pub struct CtoModel {
    /// Per-node client scripts (the node count is `scripts.len()`).
    pub scripts: Vec<Vec<Op>>,
    /// `cto=strict`; `false` is bounded mode (reads are local).
    pub strict: bool,
    /// Grant read delegations with ReadIndex answers.
    pub delegations: bool,
    /// Recall before acking a write (`false` is the naive design).
    pub recall: bool,
    pub seq_margin: i16,
    pub deleg_margin: i16,
    pub lease_margin: i16,
    pub lease_ttl: i16,
    pub deleg_ttl: i16,
    /// Cap a grant at the backing lease's usable end.
    pub cap_by_lease: bool,
    /// A release waits until no grant is live.
    pub recall_before_release: bool,
    /// A delegate drops its delegation on tailing a higher epoch's marker.
    pub void_on_epoch: bool,
    /// A grant whose reply a recall overtook is not installed.
    pub recall_gen_check: bool,
    /// `D`: every clock is within this many ticks of real time.
    pub max_offset: i16,
    /// How many clock steps (within `±D`) may happen.
    pub max_jumps: u8,
    pub max_drops: u8,
    pub max_tick: u8,
    pub max_epoch: u8,
    pub allow_renew: bool,
    pub allow_release: bool,
    /// Initial holder (epoch 1, fresh lease at t = 0), or `NONE`.
    pub initial_holder: u8,
    /// Every initial offset combination (`true`), or `init_offsets`.
    pub explore_offsets: bool,
    /// The initial offsets when not exploring them (default all zero).
    pub init_offsets: Option<Vec<i16>>,
    // ---- plan 30 §M9 ----
    /// The register is a `fast` tenure (a sealed backup / `ack=s3`): a
    /// takeover may happen before the lease expires.
    pub fast_takeover: bool,
    /// The successor waits the predecessor's strict-read horizon out
    /// before acknowledging writes.
    pub horizon_wait: bool,
    /// A `fast` holder answers strict reads only with a fresh probe.
    pub probe_freshness: bool,
    /// The takeover detection window (`backup_takeover_ms`), in ticks.
    pub takeover_window: i16,
    // ---- plan 43 C7-F F-a0 ----
    /// A new tenure's register is marked at its first CAS (strict mounts:
    /// `Config::strict_mounts`). Off: only a tenure that serves a strict
    /// read marks it, before its first answer.
    pub mark_at_takeover: bool,
    /// A takeover that waits out its predecessor's grants (a fast one of a
    /// marked tenure: the floor above) writes its register marked, so
    /// its own successor waits too (`LeaseState::granted_lease`'s
    /// `inherits_duty`). Off, a tenure sealed inside its floor before it
    /// served a strict read leaves its successor no floor.
    pub inherit_mark: bool,
    // ---- plan 43 C7-F F-a ----
    /// A successor's floor is capped by what the old holder's beats say
    /// of its lease.
    pub beat_cap: bool,
    /// The naive cap: `X` is only what the beats named. Must violate.
    pub beat_cap_from_named_expiry: bool,
    // ---- plan 43 C7a: rate and the honour/wait/wall split ----
    /// A node's oscillator gains or loses `rate[i]` ticks every
    /// `rate_period` real ticks (`rate[i]` in `[-max_rate, max_rate]`,
    /// chosen at start; every combination when `explore_rates`). 0: no
    /// rate error.
    pub rate_period: i16,
    pub max_rate: i16,
    pub explore_rates: bool,
    pub init_rates: Option<Vec<i16>>,
    /// The design's rate budget, in ticks per `rate_period` (the code's
    /// rho): the wait reading runs slow, and the honour reading fast, by it.
    pub rate_budget: i16,
    /// Plan 43 C7-H1's split: a holder judges its rights on its honour
    /// reading, a waiter on its wait reading, cross-node values on the wall
    /// reading. `false`: every check on the wall reading (the design before
    /// C7-H1).
    pub clock_split: bool,
    /// An undisciplined host: the wall follows the oscillator too.
    pub wall_follows_rate: bool,
    /// Which node steps, and which way (plan 43 C7a): `Jump` is offered only
    /// for `step_node` (`None`: any node) and only backwards (`step_dir <
    /// 0`) or forwards (`step_dir > 0`) of the node's present offset (`0`:
    /// either).
    pub step_node: Option<u8>,
    pub step_dir: i8,
    /// The span filter of the beat cap (the code's `beat_cap_span_ms`): only
    /// beats sent at most this many ticks before the end `X` the cap counts
    /// to are used. `None`: no filter.
    pub beat_span: Option<i16>,
    // ---- plan 43 C7b ----
    /// How a node decides the register's lease has run out
    /// ([`crate::clk::ClaimRule`]): `Wall` (its wall reading against the
    /// holder's wall expiry) or `Observed` (one TTL of its own wait reading
    /// after its first read of the register's current renewal key). Under
    /// `Observed` a fast takeover is judged by the permit alone.
    pub claim_rule: crate::clk::ClaimRule,
}

impl CtoModel {
    /// Strict mode with delegations, recall, lease-capped grants and the
    /// lease's margin on every side: the design.
    pub fn strict(scripts: Vec<Vec<Op>>) -> Self {
        CtoModel {
            scripts,
            strict: true,
            delegations: true,
            recall: true,
            seq_margin: 1,
            deleg_margin: 1,
            lease_margin: 1,
            lease_ttl: 6,
            deleg_ttl: 3,
            cap_by_lease: true,
            recall_before_release: true,
            void_on_epoch: true,
            recall_gen_check: true,
            max_offset: 0,
            max_jumps: 0,
            max_drops: 1,
            max_tick: 9,
            max_epoch: 2,
            allow_renew: false,
            allow_release: false,
            initial_holder: 0,
            explore_offsets: false,
            init_offsets: None,
            fast_takeover: false,
            horizon_wait: true,
            probe_freshness: true,
            takeover_window: 2,
            mark_at_takeover: true,
            inherit_mark: true,
            beat_cap: false,
            beat_cap_from_named_expiry: false,
            rate_period: 0,
            max_rate: 0,
            explore_rates: false,
            init_rates: None,
            rate_budget: 0,
            clock_split: false,
            wall_follows_rate: false,
            step_node: None,
            step_dir: 0,
            beat_span: None,
            claim_rule: crate::clk::ClaimRule::Wall,
        }
    }

    /// Plan 30 §M9: strict mode on a `fast` tenure (takeovers before
    /// expiry), with both rules on.
    pub fn strict_fast(scripts: Vec<Vec<Op>>) -> Self {
        CtoModel {
            fast_takeover: true,
            ..Self::strict(scripts)
        }
    }

    pub fn bounded(scripts: Vec<Vec<Op>>) -> Self {
        CtoModel {
            strict: false,
            delegations: false,
            ..Self::strict(scripts)
        }
    }

    fn n(&self) -> usize {
        self.scripts.len()
    }

    fn spec(&self) -> crate::clk::ClockSpec {
        crate::clk::ClockSpec {
            rate_period: self.rate_period,
            rate_budget: self.rate_budget,
            clock_split: self.clock_split,
            wall_follows_rate: self.wall_follows_rate,
        }
    }
}

/// The lease object in the bucket. `expires` is in the writer's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    /// The lease's end on the writer's wall clock (what a claimant compares
    /// its own wall reading with).
    pub expires: i16,
    /// Plan 43 C7a: the same end on the holder's wait clock (the lease
    /// body's `holder_wait_ms`: what the beat cap counts to).
    pub expires_w: i16,
    pub released: bool,
    /// Plan 30 §M9: this tenure has served a strict read (the
    /// `granted_delegations` flag, set by one CAS before the first).
    pub marked: bool,
}

impl Reg {
    /// The renewal key (`ClaimRule::Observed`).
    pub fn key(&self) -> crate::clk::RegKey {
        (self.holder, self.epoch, self.expires_w, self.released)
    }
}

/// Holder side: a grant it must recall or outwait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Grant {
    pub to: u8,
    /// Live until the holder's clock reaches this (`g + ttl + seq_margin`).
    pub until: i16,
    pub id: u8,
}

/// Delegate side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Deleg {
    /// Honoured while the delegate's clock is below this.
    pub until: i16,
    /// The position the reader must have applied (log length).
    pub pos: u8,
    pub epoch: u8,
}

/// A write whose acknowledgement waits for recalls.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Held {
    pub writer: u8,
    pub version: u8,
    pub waiting: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeOp {
    Idle,
    /// A write in flight (forwarded, or held here for recalls).
    Writing,
    /// A read began; `must` is the ghost: the highest version whose close
    /// had completed.
    ReadStart {
        must: u8,
    },
    ReadIndexSent {
        must: u8,
        sent: i16,
        gen: u8,
    },
    /// Waiting for the replica to reach `pos`.
    ReadWait {
        must: u8,
        pos: u8,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The lease this node believes it holds: `(epoch, its end on this
    /// node's honour clock, its end on its wait clock)`. (With no split
    /// the two are the one end of the one clock.)
    pub lease: Option<(u8, i16, i16)>,
    pub applied: u8,
    pub pc: u8,
    pub op: NodeOp,
    pub deleg: Option<Deleg>,
    pub grants: Vec<Grant>,
    pub held: Vec<Held>,
    /// Delegate side: bumped by every recall this node receives. A
    /// ReadIndex records it when sent, and its reply's grant is installed
    /// only if no recall arrived meanwhile — a recall can overtake the
    /// reply that carries the very grant it recalls.
    pub recall_gen: u8,
    /// Plan 30 §M9: the holder's clock at its last probe of the register
    /// (a takeover or renewal counts).
    pub last_probe: i16,
    /// Plan 30 §M9: the successor's acknowledgement floor (its clock),
    /// and the writes held until it passes.
    pub ack_floor: Option<i16>,
    pub floor_held: Vec<(u8, u8)>,
    /// Plan 43 C7-F F-a: what this node heard of one holder's beats.
    pub heard: Option<Heard>,
}

/// What a node heard of the beats of `holder` at `epoch` (`beat_cap`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Heard {
    pub holder: u8,
    pub epoch: u8,
    /// The smallest `arrival − send` over the beats, each on its own
    /// clock (the heard node's minus the holder's).
    pub lag: i16,
    /// The highest expiry named, and the highest guard named (0: none).
    pub named: i16,
    pub guard: i16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Forward {
        from: u8,
        to: u8,
    },
    /// `version == 0`: not the holder; retry.
    ForwardReply {
        to: u8,
        version: u8,
    },
    ReadIndex {
        from: u8,
        to: u8,
    },
    /// `ok == false`: not a usable holder. `grant`: `(ttl, epoch)`.
    ReadIndexReply {
        to: u8,
        ok: bool,
        pos: u8,
        grant: Option<(i16, u8)>,
    },
    Recall {
        to: u8,
        from: u8,
        id: u8,
    },
    RecallAck {
        to: u8,
        from: u8,
        id: u8,
    },
}

/// Ghost bits for the `sometimes` (non-vacuity) properties.
pub const SAW_DELEG_READ: u16 = 1;
pub const SAW_RECALL_ACK: u16 = 2;
pub const SAW_RECALL_EXPIRED: u16 = 4;
pub const SAW_TAKEOVER: u16 = 8;
pub const SAW_READINDEX_READ: u16 = 16;
pub const SAW_FAST_TAKEOVER: u16 = 32;
pub const SAW_FLOOR_WAIT: u16 = 64;
pub const SAW_FLOOR_CAPPED: u16 = 128;
/// A floor was armed with a cap computed from heard beats, whether or not
/// it ended the floor before the bound (the far-ahead successor's cap is
/// later than the bound: it stands).
pub const SAW_CAP_COMPUTED: u16 = 256;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: u8,
    /// Each node's clock (plan 43 C7a: wall offset, initial offset, rate,
    /// honour jump).
    pub clk: Vec<Clk>,
    pub jumps: u8,
    pub drops: u8,
    pub reg: Reg,
    /// 0 = a write of `X`; `e > 0` = the epoch-`e` takeover marker.
    pub log: Vec<u8>,
    pub nodes: Vec<Node>,
    pub net: Vec<Msg>,
    /// Ghost: the highest version whose close completed.
    pub closed: u8,
    /// A read returned less than its `must`: `(node, must, got)`.
    pub violation: Option<(u8, u8, u8)>,
    pub next_id: u8,
    pub saw: u16,
    /// Plan 43 C7a (`beat_span`): the beats each node heard, `(heard by,
    /// holder, epoch, sent, lag)`, on the Pareto frontier (a later beat
    /// with a lag at most an earlier one's leaves the earlier one out: it
    /// is in every window the earlier one is). Empty without a span.
    pub beat_lags: Vec<BeatLag>,
    /// Plan 43 C7b (`ClaimRule::Observed`): each node's first read of the
    /// register's current renewal key (all `None` under `Wall`).
    pub seen: Vec<crate::clk::Seen>,
}

pub use crate::clk::Clk;

/// A beat the backup heard: `(heard by, holder, epoch, sent, lag)`.
pub type BeatLag = (u8, u8, u8, i16, i16);

impl State {
    fn wall(&self, cfg: &CtoModel, i: usize) -> i16 {
        cfg.spec().wall(self.t, &self.clk[i])
    }

    /// A holder's rights are judged on this reading.
    fn honour(&self, cfg: &CtoModel, i: usize) -> i16 {
        cfg.spec().honour(self.t, &self.clk[i])
    }

    /// A waiter's outwaits and the liveness timers are on this one.
    fn wait(&self, cfg: &CtoModel, i: usize) -> i16 {
        cfg.spec().wait(self.t, &self.clk[i])
    }

    /// Whether node `i` may treat the register's lease as run out: never
    /// held, released, or (by the claim rule) expired on `i`'s reading.
    fn claimable_by(&self, cfg: &CtoModel, i: usize) -> bool {
        if self.reg.holder == NONE || self.reg.released {
            return true;
        }
        match cfg.claim_rule {
            crate::clk::ClaimRule::Wall => self.wall(cfg, i) >= self.reg.expires,
            crate::clk::ClaimRule::Observed => self.seen[i].is_some_and(|(key, since)| {
                key == self.reg.key() && self.wait(cfg, i) - since >= cfg.lease_ttl
            }),
        }
    }

    /// The version a replica at prefix `n` holds (writes in the prefix).
    fn version_at(&self, n: u8) -> u8 {
        self.log[..n as usize].iter().filter(|e| **e == 0).count() as u8
    }

    fn versions(&self) -> u8 {
        self.version_at(self.log.len() as u8)
    }

    fn usable(&self, cfg: &CtoModel, i: usize) -> Option<u8> {
        let (epoch, expires_h, _) = self.nodes[i].lease?;
        (self.honour(cfg, i) < expires_h - cfg.lease_margin).then_some(epoch)
    }

    /// A usable holder whose epoch is the register's: may execute and
    /// answer (a stale-epoch holder is fenced by the log). Answering a
    /// ReadIndex needs only `usable` — that is exactly the check the
    /// lease discipline must make safe, so the model does not add the
    /// register comparison there.
    fn may_execute(&self, cfg: &CtoModel, i: usize) -> bool {
        self.usable(cfg, i)
            .is_some_and(|e| e == self.reg.epoch && self.reg.holder == i as u8)
    }

    fn send(&mut self, m: Msg) {
        self.net.push(m);
        self.net.sort();
    }

    fn fresh_id(&mut self) -> u8 {
        self.next_id += 1;
        self.next_id
    }

    fn finish_op(&mut self, i: usize) {
        self.nodes[i].pc += 1;
        self.nodes[i].op = NodeOp::Idle;
    }

    fn check_read(&mut self, i: usize, must: u8, got: u8) {
        if got < must && self.violation.is_none() {
            self.violation = Some((i as u8, must, got));
        }
    }

    /// The write's close completed (the writer got its ack).
    fn close_completed(&mut self, writer: usize, version: u8) {
        self.closed = self.closed.max(version);
        self.finish_op(writer);
    }

    /// Execute a write at holder `h` for `writer`; returns whether it is
    /// acked now (else held for recalls).
    fn execute_write(&mut self, cfg: &CtoModel, h: usize, writer: usize) {
        self.log.push(0);
        let len = self.log.len() as u8;
        self.nodes[h].applied = len;
        let version = self.versions();
        // The sequencer's table and its floor are on its wait clock.
        let now = self.wait(cfg, h);
        self.nodes[h].grants.retain(|g| g.until > now);
        let waiting: Vec<u8> = if cfg.recall {
            self.nodes[h]
                .grants
                .iter()
                .filter(|g| g.to != writer as u8)
                .map(|g| g.id)
                .collect()
        } else {
            Vec::new()
        };
        if waiting.is_empty() {
            // Plan 30 §M9: the successor's floor holds the acknowledgement.
            if self.nodes[h].ack_floor.is_some_and(|f| now < f) {
                self.nodes[h].floor_held.push((writer as u8, version));
                return;
            }
            self.ack_write(h, writer, version);
            return;
        }
        for g in self.nodes[h].grants.clone() {
            if waiting.contains(&g.id) {
                self.send(Msg::Recall {
                    to: g.to,
                    from: h as u8,
                    id: g.id,
                });
            }
        }
        self.nodes[h].held.push(Held {
            writer: writer as u8,
            version,
            waiting,
        });
    }

    fn ack_write(&mut self, h: usize, writer: usize, version: u8) {
        if writer == h {
            self.close_completed(writer, version);
        } else {
            self.send(Msg::ForwardReply {
                to: writer as u8,
                version,
            });
        }
    }

    /// Grant `id` is gone (acked or expired): release held writes.
    fn grant_done(&mut self, cfg: &CtoModel, h: usize, id: u8) {
        self.nodes[h].grants.retain(|g| g.id != id);
        let mut done = Vec::new();
        for held in self.nodes[h].held.iter_mut() {
            held.waiting.retain(|w| *w != id);
            if held.waiting.is_empty() {
                done.push((held.writer, held.version));
            }
        }
        self.nodes[h].held.retain(|x| !x.waiting.is_empty());
        let now = self.wait(cfg, h);
        for (writer, version) in done {
            if self.nodes[h].ack_floor.is_some_and(|f| now < f) {
                self.nodes[h].floor_held.push((writer, version));
            } else {
                self.ack_write(h, writer as usize, version);
            }
        }
    }

    fn apply_next(&mut self, cfg: &CtoModel, i: usize) {
        let entry = self.log[self.nodes[i].applied as usize];
        self.nodes[i].applied += 1;
        if entry > 0 && cfg.void_on_epoch {
            if let Some(d) = self.nodes[i].deleg {
                if entry > d.epoch {
                    self.nodes[i].deleg = None;
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Jump(u8, i16),
    Drop(usize),
    Tail(u8),
    Renew(u8),
    Takeover(u8),
    Release(u8),
    /// Start node `i`'s next script op.
    Start(u8),
    /// Bounded read, a holder's local read, or the S3 path (no live
    /// holder: tail to head, then read).
    ReadLocal(u8),
    /// A strict read under a valid delegation: wait for its position.
    ReadUnderDelegation(u8),
    SendReadIndex(u8),
    /// The reader gives up on an unanswered ReadIndex and starts over.
    ReadIndexTimeout(u8),
    FinishRead(u8),
    /// A write whose forward got `NotHolder` (or whose writer holds no
    /// usable lease) retries against the register's holder.
    RetryWrite(u8),
    /// Holder `h` drops its expired grants (by its clock).
    Expire(u8),
    Deliver(usize),
    /// Plan 30 §M9: holder `h` probes the register (an S3 request): a
    /// foreign holder there deposes it; otherwise its liveness is fresh.
    Probe(u8),
    /// Plan 30 §M9: successor `h`'s acknowledgement floor passed (by its
    /// clock): the writes held for it are acknowledged.
    FloorPassed(u8),
    /// Plan 43 C7-F F-a: holder `h` beats to `b` (`beat_cap`).
    Beat(u8, u8),
    /// Plan 43 C7b: node `i` reads the register; the first read showing its
    /// current renewal key starts the node's observation of it
    /// (`ClaimRule::Observed`).
    Observe(u8),
}

impl Model for CtoModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        let rates =
            crate::clk::initial_rates(n, self.max_rate, self.explore_rates, &self.init_rates);
        let mut offs: Vec<Vec<i16>> = vec![self.init_offsets.clone().unwrap_or(vec![0; n])];
        if self.explore_offsets && self.max_offset > 0 {
            offs = vec![vec![]];
            for _ in 0..n {
                let mut next = Vec::new();
                for o in &offs {
                    for d in -self.max_offset..=self.max_offset {
                        let mut v = o.clone();
                        v.push(d);
                        next.push(v);
                    }
                }
                offs = next;
            }
        }
        offs.into_iter()
            .flat_map(|off| rates.iter().map(move |r| (off.clone(), r.clone())))
            .map(|(off, rate)| {
                let mut nodes: Vec<Node> = (0..n)
                    .map(|_| Node {
                        lease: None,
                        applied: 0,
                        pc: 0,
                        op: NodeOp::Idle,
                        deleg: None,
                        grants: Vec::new(),
                        held: Vec::new(),
                        recall_gen: 0,
                        last_probe: 0,
                        ack_floor: None,
                        floor_held: Vec::new(),
                        heard: None,
                    })
                    .collect();
                let mut reg = Reg {
                    holder: NONE,
                    epoch: 0,
                    expires: 0,
                    expires_w: 0,
                    released: false,
                    marked: false,
                };
                let mut log = Vec::new();
                if self.initial_holder != NONE {
                    let h = self.initial_holder as usize;
                    let expires = off[h] + self.lease_ttl;
                    reg = Reg {
                        holder: h as u8,
                        epoch: 1,
                        expires,
                        expires_w: expires,
                        released: false,
                        // A tenure with strict mounts serves strict reads
                        // from the start (the code marks the lease at
                        // acquisition when this node has `cto=strict`).
                        marked: self.strict && self.mark_at_takeover,
                    };
                    log.push(1);
                    for node in nodes.iter_mut() {
                        node.applied = 1;
                    }
                    nodes[h].lease = Some((1, expires, expires));
                    nodes[h].last_probe = off[h];
                }
                State {
                    t: 0,
                    clk: crate::clk::initial_clocks(&off, &rate),
                    jumps: 0,
                    drops: 0,
                    reg,
                    log,
                    nodes,
                    net: Vec::new(),
                    closed: 0,
                    violation: None,
                    next_id: 0,
                    saw: 0,
                    beat_lags: Vec::new(),
                    seen: vec![None; n],
                }
            })
            .collect()
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.violation.is_some() {
            return;
        }
        let n = self.n();
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        if s.jumps < self.max_jumps {
            for i in 0..n {
                if self.step_node.is_some_and(|n| n as usize != i) {
                    continue;
                }
                for d in -self.max_offset..=self.max_offset {
                    let dir_ok = match self.step_dir {
                        0 => true,
                        x if x < 0 => d < s.clk[i].off,
                        _ => d > s.clk[i].off,
                    };
                    if d != s.clk[i].off && dir_ok {
                        actions.push(Action::Jump(i as u8, d));
                    }
                }
            }
        }
        if s.drops < self.max_drops {
            for k in 0..s.net.len() {
                if k == 0 || s.net[k] != s.net[k - 1] {
                    actions.push(Action::Drop(k));
                }
            }
        }
        for k in 0..s.net.len() {
            if k == 0 || s.net[k] != s.net[k - 1] {
                actions.push(Action::Deliver(k));
            }
        }
        for i in 0..n {
            let node = &s.nodes[i];
            let iu = i as u8;
            if (node.applied as usize) < s.log.len() {
                actions.push(Action::Tail(iu));
            }
            if self.allow_renew && s.may_execute(self, i) && !s.reg.released {
                actions.push(Action::Renew(iu));
            }
            // (A claimant under `Wall` compares its wall reading with the
            // register's expiry: a value of another node's clock. Under
            // `Observed` it waits a TTL of its own wait reading from its
            // first read of the register's current key, and claims on that
            // read's tag.)
            let claimable = s.claimable_by(self, i);
            // Plan 30 §M9: on a `fast` tenure the detection of the
            // holder's silence is nondeterministic (safety must hold for
            // any timing), so a live lease is claimable by a peer at any
            // moment.
            let fast = self.fast_takeover && s.reg.holder != NONE && s.reg.holder != iu;
            // Takeover is offered only to a node with a write to do: that
            // is what makes a node acquire.
            let wants = node.op == NodeOp::Writing
                || (node.op == NodeOp::Idle
                    && self.scripts[i].get(node.pc as usize) == Some(&Op::Write));
            if (claimable || fast) && wants && s.reg.epoch < self.max_epoch {
                actions.push(Action::Takeover(iu));
            }
            // A read of the register that shows a key this node has not
            // seen (it reads when it has a reason: a write to do, a strict
            // read with no usable lease or delegation).
            if self.claim_rule == crate::clk::ClaimRule::Observed
                && (wants || matches!(node.op, NodeOp::ReadStart { .. }))
                && s.seen[i].map(|(k, _)| k) != Some(s.reg.key())
                && s.reg.holder != NONE
                && !s.reg.released
            {
                actions.push(Action::Observe(iu));
            }
            if node.lease.is_some() && self.fast_takeover {
                actions.push(Action::Probe(iu));
            }
            // Plan 43 C7-F F-a: the holder's beats stop at its expiry.
            if self.beat_cap {
                if let Some((_, expires_h, _)) = node.lease {
                    if s.honour(self, i) < expires_h {
                        for b in 0..n {
                            if b != i {
                                actions.push(Action::Beat(iu, b as u8));
                            }
                        }
                    }
                }
            }
            if node
                .ack_floor
                .is_some_and(|f| s.wait(self, i) >= f && !node.floor_held.is_empty())
            {
                actions.push(Action::FloorPassed(iu));
            }
            if self.allow_release && s.may_execute(self, i) && node.held.is_empty() {
                let now = s.wait(self, i);
                let live = node.grants.iter().any(|g| g.until > now);
                if !(self.recall_before_release && live) {
                    actions.push(Action::Release(iu));
                }
            }
            if !node.grants.is_empty() {
                let now = s.wait(self, i);
                if node.grants.iter().any(|g| g.until <= now) {
                    actions.push(Action::Expire(iu));
                }
            }
            // Plan 30 §M9: on a `fast` tenure a holder serves even its own
            // strict reads locally only with a fresh probe (a deposed
            // holder that has not noticed would read stale state).
            let fresh = !self.fast_takeover
                || !self.probe_freshness
                || s.honour(self, i) - node.last_probe < self.takeover_window;
            match node.op {
                NodeOp::Idle => {
                    if (node.pc as usize) < self.scripts[i].len() {
                        actions.push(Action::Start(iu));
                    }
                }
                NodeOp::Writing => {
                    let in_flight = s.net.iter().any(|m| {
                        matches!(m, Msg::Forward { from, .. } if *from == iu)
                            || matches!(m, Msg::ForwardReply { to, .. } if *to == iu)
                    }) || s
                        .nodes
                        .iter()
                        .any(|x| x.held.iter().any(|h| h.writer == iu));
                    if !in_flight {
                        actions.push(Action::RetryWrite(iu));
                    }
                }
                NodeOp::ReadStart { .. } => {
                    // Bounded, or the usable holder: its own replica.
                    if !self.strict {
                        actions.push(Action::ReadLocal(iu));
                    } else if s.usable(self, i).is_some() {
                        if fresh {
                            actions.push(Action::ReadLocal(iu));
                        }
                        // Else: `Probe(i)` first (offered above).
                    } else if self.delegations
                        && node.deleg.is_some_and(|d| s.honour(self, i) < d.until)
                    {
                        actions.push(Action::ReadUnderDelegation(iu));
                    } else if s.claimable_by(self, i) {
                        actions.push(Action::ReadLocal(iu));
                    } else {
                        actions.push(Action::SendReadIndex(iu));
                    }
                }
                NodeOp::ReadIndexSent { .. } => {
                    // One read in flight per node, and a retry only once
                    // nothing of the last attempt is in flight: no stale
                    // reply can reach a later attempt.
                    let pending = s.net.iter().any(|m| {
                        matches!(m, Msg::ReadIndex { from, .. } if *from == iu)
                            || matches!(m, Msg::ReadIndexReply { to, .. } if *to == iu)
                    });
                    if !pending {
                        actions.push(Action::ReadIndexTimeout(iu));
                    }
                }
                NodeOp::ReadWait { pos, .. } => {
                    if node.applied >= pos {
                        actions.push(Action::FinishRead(iu));
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Tick => {
                s.t += 1;
                {
                    let (t, spec) = (s.t, self.spec());
                    spec.follow_wall(t, &mut s.clk);
                }
            }
            Action::Jump(i, d) => {
                s.clk[i as usize].off = d;
                s.jumps += 1;
                {
                    let (t, spec) = (s.t, self.spec());
                    spec.follow_wall(t, &mut s.clk);
                }
            }
            Action::Drop(k) => {
                s.net.remove(k);
                s.drops += 1;
            }
            Action::Tail(i) => s.apply_next(self, i as usize),
            Action::Renew(i) => {
                let i = i as usize;
                // Three stamps of one instant: the register's (wall), the
                // holder's own (honour: what it honours the lease until;
                // wait: what its beats name).
                s.reg.expires = s.wall(self, i) + self.lease_ttl;
                s.reg.expires_w = s.wait(self, i) + self.lease_ttl;
                let epoch = s.reg.epoch;
                s.nodes[i].lease = Some((
                    epoch,
                    s.honour(self, i) + self.lease_ttl,
                    s.wait(self, i) + self.lease_ttl,
                ));
                s.nodes[i].last_probe = s.honour(self, i);
            }
            Action::Observe(i) => {
                let i = i as usize;
                let key = s.reg.key();
                if s.seen[i].map(|(k, _)| k) != Some(key) {
                    s.seen[i] = Some((key, s.wait(self, i)));
                }
            }
            Action::Takeover(i) => {
                let i = i as usize;
                let epoch = s.reg.epoch + 1;
                // The claimant's wall (the register's stamp, the compare
                // with the old expiry), honour (its own lease window, its
                // probe) and wait (its floor) readings.
                let (now_wall, now_honour, now_wait) =
                    (s.wall(self, i), s.honour(self, i), s.wait(self, i));
                let expires = now_wall + self.lease_ttl;
                let prev = s.reg;
                // Plan 30 §M9: an unexpired predecessor that served strict
                // reads: hold every acknowledgement until its horizon.
                // The permit decides, not the clock: a fast takeover is a
                // fast one whatever the taker's clock says of the old
                // expiry (a stamp of the old holder's clock: plan 43
                // C7-H2, the code's `acquire_won`). With `fast_takeover`
                // every takeover of a live holder counts as permitted: a
                // superset of the code's permit (a seal, `ack=s3` after
                // silence), so this model explores no takeover that waits
                // no floor; the code's no-floor TTL path is the sim's.
                let permitted = self.fast_takeover && prev.holder != NONE && prev.holder != i as u8;
                // Under `Observed` a claim that is not permitted was proved
                // expired by the observation: no floor (the old expiry is a
                // stamp of the old holder's clock and is not compared).
                let unexpired = prev.holder != NONE
                    && !prev.released
                    && (permitted
                        || (self.claim_rule == crate::clk::ClaimRule::Wall
                            && now_wall < prev.expires));
                // Plan 43 C7-F F-a0: a tenure that inherits a floor is
                // marked from its first CAS.
                let inherits = self.inherit_mark && self.horizon_wait && unexpired && prev.marked;
                s.reg = Reg {
                    holder: i as u8,
                    epoch,
                    expires,
                    expires_w: now_wait + self.lease_ttl,
                    released: false,
                    marked: (self.strict && self.mark_at_takeover) || inherits,
                };
                // Tail to head, then the marker.
                while (s.nodes[i].applied as usize) < s.log.len() {
                    s.apply_next(self, i);
                }
                s.log.push(epoch);
                s.nodes[i].applied = s.log.len() as u8;
                s.nodes[i].lease = Some((
                    epoch,
                    now_honour + self.lease_ttl,
                    now_wait + self.lease_ttl,
                ));
                s.nodes[i].last_probe = now_honour;
                // A new tenure's table is empty.
                s.nodes[i].grants.clear();
                s.nodes[i].held.clear();
                s.saw |= SAW_TAKEOVER;
                if unexpired {
                    s.saw |= SAW_FAST_TAKEOVER;
                }
                s.nodes[i].ack_floor = None;
                if self.horizon_wait && unexpired && prev.marked {
                    // The floor is a wait on the successor's wait clock.
                    let bound =
                        now_wait + self.takeover_window + self.deleg_ttl + 2 * self.seq_margin;
                    // Not `min(prev.expires, bound)`: the old expiry is the
                    // old holder's clock (the code's floor is the bound).
                    //
                    // Plan 43 C7-F F-a: capped by what the old holder's
                    // beats say of its lease, on this node's clock (the
                    // code's `Core::beat_cap`): none if that has passed.
                    let cap = if self.beat_cap {
                        s.nodes[i]
                            .heard
                            .filter(|h| h.holder == prev.holder && h.epoch == prev.epoch)
                            .and_then(|h| {
                                // `X`, the holder's wait-clock end, all of
                                // one clock: the lease the takeover
                                // replaced, what the beats named, the guard.
                                let x = if self.beat_cap_from_named_expiry {
                                    h.named
                                } else {
                                    prev.expires_w.max(h.named).max(h.guard)
                                };
                                // The beats' lag, over those sent within
                                // `beat_span` of `X` when there is a span:
                                // on the frontier the earliest in the
                                // window has the least lag.
                                let lag = match self.beat_span {
                                    None => Some(h.lag),
                                    Some(span) => s
                                        .beat_lags
                                        .iter()
                                        .filter(|(by, holder, epoch, sent, _)| {
                                            *by == i as u8
                                                && *holder == prev.holder
                                                && *epoch == prev.epoch
                                                && (0..=span).contains(&(x - sent))
                                        })
                                        .map(|(.., lag)| *lag)
                                        .min(),
                                };
                                lag.map(|lag| x + lag + self.seq_margin)
                            })
                    } else {
                        None
                    };
                    let end = cap.map_or(bound, |c| bound.min(c));
                    if cap.is_some() {
                        s.saw |= SAW_CAP_COMPUTED;
                    }
                    if end < bound {
                        s.saw |= SAW_FLOOR_CAPPED;
                    }
                    if end > now_wait {
                        s.nodes[i].ack_floor = Some(end);
                        s.saw |= SAW_FLOOR_WAIT;
                    }
                }
            }
            Action::Beat(h, b) => {
                let (h, b) = (h as usize, b as usize);
                // The beat is sent on the holder's wait clock and arrives
                // on the backup's: `at - sent`, the code's lag; it names
                // the holder's wait-clock end and its floor (also wait).
                let (epoch, _, expires) = s.nodes[h].lease?;
                let (sent, at) = (s.wait(self, h), s.wait(self, b));
                let lag = at - sent;
                let guard = s.nodes[h].ack_floor.unwrap_or(0);
                if self.beat_span.is_some() {
                    // The frontier of this holder's beats at `b` (a new
                    // epoch starts the book over; a beat whose lag is at
                    // most an earlier one's leaves that one out).
                    let lags = &mut s.beat_lags;
                    lags.retain(|(by, hh, e, _, l)| {
                        (*by, *hh) != (b as u8, h as u8) || (*e == epoch && *l < lag)
                    });
                    lags.push((b as u8, h as u8, epoch, sent, lag));
                }
                s.nodes[b].heard = Some(match s.nodes[b].heard {
                    Some(o) if o.holder == h as u8 && o.epoch == epoch => Heard {
                        lag: o.lag.min(lag),
                        named: o.named.max(expires),
                        guard: o.guard.max(guard),
                        ..o
                    },
                    _ => Heard {
                        holder: h as u8,
                        epoch,
                        lag,
                        named: expires,
                        guard,
                    },
                });
            }
            Action::Probe(i) => {
                let i = i as usize;
                let (mine, ..) = s.nodes[i].lease?;
                if s.reg.holder != i as u8 || s.reg.epoch != mine {
                    // Deposed: stop everything at once.
                    s.nodes[i].lease = None;
                    s.nodes[i].grants.clear();
                    s.nodes[i].held.clear();
                } else {
                    s.nodes[i].last_probe = s.honour(self, i);
                }
            }
            Action::FloorPassed(i) => {
                let i = i as usize;
                let held = std::mem::take(&mut s.nodes[i].floor_held);
                s.nodes[i].ack_floor = None;
                for (writer, version) in held {
                    s.ack_write(i, writer as usize, version);
                }
            }
            Action::Release(i) => {
                let i = i as usize;
                s.reg.released = true;
                s.nodes[i].lease = None;
                s.nodes[i].grants.clear();
            }
            Action::Start(i) => {
                let i = i as usize;
                let op = self.scripts[i][s.nodes[i].pc as usize];
                match op {
                    Op::Read => {
                        s.nodes[i].op = NodeOp::ReadStart { must: s.closed };
                    }
                    Op::Write => {
                        s.nodes[i].op = NodeOp::Writing;
                        start_write(self, &mut s, i);
                    }
                }
            }
            Action::RetryWrite(i) => start_write(self, &mut s, i as usize),
            Action::ReadLocal(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                if self.strict && s.usable(self, i).is_none() {
                    // No live holder: the S3 path tails to head.
                    while (s.nodes[i].applied as usize) < s.log.len() {
                        s.apply_next(self, i);
                    }
                }
                let got = s.version_at(s.nodes[i].applied);
                s.check_read(i, must, got);
                s.finish_op(i);
            }
            Action::ReadUnderDelegation(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                let d = s.nodes[i].deleg.expect("valid delegation");
                s.nodes[i].op = NodeOp::ReadWait { must, pos: d.pos };
                s.saw |= SAW_DELEG_READ;
            }
            Action::SendReadIndex(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                let sent = s.honour(self, i);
                let to = s.reg.holder;
                s.send(Msg::ReadIndex { from: i as u8, to });
                let gen = s.nodes[i].recall_gen;
                s.nodes[i].op = NodeOp::ReadIndexSent { must, sent, gen };
            }
            Action::ReadIndexTimeout(i) => {
                let i = i as usize;
                let NodeOp::ReadIndexSent { must, .. } = s.nodes[i].op else {
                    return None;
                };
                s.nodes[i].op = NodeOp::ReadStart { must };
            }
            Action::FinishRead(i) => {
                let i = i as usize;
                let NodeOp::ReadWait { must, .. } = s.nodes[i].op else {
                    return None;
                };
                let got = s.version_at(s.nodes[i].applied);
                s.check_read(i, must, got);
                s.finish_op(i);
            }
            Action::Expire(h) => {
                let h = h as usize;
                let now = s.wait(self, h);
                let expired: Vec<u8> = s.nodes[h]
                    .grants
                    .iter()
                    .filter(|g| g.until <= now)
                    .map(|g| g.id)
                    .collect();
                for id in expired {
                    if s.nodes[h].held.iter().any(|x| x.waiting.contains(&id)) {
                        s.saw |= SAW_RECALL_EXPIRED;
                    }
                    s.grant_done(self, h, id);
                }
            }
            Action::Deliver(k) => {
                let m = s.net.remove(k);
                deliver(self, &mut s, m);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("close_to_open", |_, s: &State| s.violation.is_none()),
            Property::sometimes("all_ops_done", |m: &CtoModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| n.pc as usize == sc.len())
            }),
            Property::sometimes("read_under_delegation", |_, s: &State| {
                s.saw & SAW_DELEG_READ != 0
            }),
            Property::sometimes("recall_acked", |_, s: &State| s.saw & SAW_RECALL_ACK != 0),
            Property::sometimes("takeover", |_, s: &State| s.saw & SAW_TAKEOVER != 0),
            Property::sometimes("fast_takeover", |_, s: &State| {
                s.saw & SAW_FAST_TAKEOVER != 0
            }),
            Property::sometimes("floor_waited", |_, s: &State| s.saw & SAW_FLOOR_WAIT != 0),
            Property::sometimes("floor_capped", |_, s: &State| s.saw & SAW_FLOOR_CAPPED != 0),
            Property::sometimes("cap_computed", |_, s: &State| s.saw & SAW_CAP_COMPUTED != 0),
        ]
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick
    }
}

/// Start (or retry) node `i`'s write: execute here if it holds, else
/// forward to the register's holder. With no usable holder anywhere the
/// write waits (a `Takeover` makes progress).
fn start_write(cfg: &CtoModel, s: &mut State, i: usize) {
    if s.may_execute(cfg, i) {
        s.execute_write(cfg, i, i);
        return;
    }
    let h = s.reg.holder;
    if h != NONE && h as usize != i && !s.reg.released {
        s.send(Msg::Forward {
            from: i as u8,
            to: h,
        });
    }
}

fn deliver(cfg: &CtoModel, s: &mut State, m: Msg) {
    match m {
        Msg::Forward { from, to } => {
            let h = to as usize;
            if s.may_execute(cfg, h) {
                s.execute_write(cfg, h, from as usize);
            } else {
                s.send(Msg::ForwardReply {
                    to: from,
                    version: 0,
                });
            }
        }
        Msg::ForwardReply { to, version } => {
            let w = to as usize;
            if s.nodes[w].op != NodeOp::Writing {
                return;
            }
            if version > 0 {
                // M6's read-your-writes: the writer's replica has its own
                // write when the close returns (a shadow on an applied
                // base, or `AwaitingLog` until the log carries it) —
                // modeled as catching up to the write's log entry. That
                // is what lets the sequencer leave the writer's own
                // delegation alone.
                let index = s
                    .log
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| **e == 0)
                    .nth(version as usize - 1)
                    .map(|(i, _)| i as u8 + 1)
                    .expect("the version is in the log");
                while s.nodes[w].applied < index {
                    s.apply_next(cfg, w);
                }
                s.close_completed(w, version);
            }
            // version 0: `RetryWrite` is enabled (nothing in flight).
        }
        Msg::ReadIndex { from, to } => {
            let h = to as usize;
            // The holder's rights (its probe, the lease it caps the grant
            // by) are on its honour clock; the table it records the grant
            // in is on its wait clock.
            let now = s.honour(cfg, h);
            let now_wait = s.wait(cfg, h);
            // Plan 30 §M9: on a `fast` tenure, only with a fresh probe.
            let stale = cfg.fast_takeover
                && cfg.probe_freshness
                && now - s.nodes[h].last_probe >= cfg.takeover_window;
            let usable = s.usable(cfg, h).filter(|_| !stale);
            let Some(epoch) = usable else {
                s.send(Msg::ReadIndexReply {
                    to: from,
                    ok: false,
                    pos: 0,
                    grant: None,
                });
                return;
            };
            // The tenure serves strict reads: say so in the register
            // first (one CAS; a stale holder's CAS fails and deposes it —
            // here the register comparison stands in for the CAS).
            if s.reg.holder == h as u8 && s.reg.epoch == epoch {
                s.reg.marked = true;
            } else {
                s.nodes[h].lease = None;
                s.nodes[h].grants.clear();
                s.nodes[h].held.clear();
                s.send(Msg::ReadIndexReply {
                    to: from,
                    ok: false,
                    pos: 0,
                    grant: None,
                });
                return;
            }
            let pos = s.nodes[h].applied;
            let mut grant = None;
            if cfg.delegations {
                let mut ttl = cfg.deleg_ttl;
                if cfg.cap_by_lease {
                    let (_, expires_h, _) = s.nodes[h].lease.expect("usable");
                    ttl = ttl.min(expires_h - cfg.lease_margin - now);
                }
                if ttl > 0 {
                    let id = s.fresh_id();
                    s.nodes[h].grants.push(Grant {
                        to: from,
                        until: now_wait + ttl + cfg.seq_margin,
                        id,
                    });
                    s.nodes[h].grants.sort();
                    grant = Some((ttl, epoch));
                }
            }
            s.send(Msg::ReadIndexReply {
                to: from,
                ok: true,
                pos,
                grant,
            });
        }
        Msg::ReadIndexReply { to, ok, pos, grant } => {
            let r = to as usize;
            let NodeOp::ReadIndexSent { must, sent, gen } = s.nodes[r].op else {
                return;
            };
            if !ok {
                s.nodes[r].op = NodeOp::ReadStart { must };
                return;
            }
            // The position stands either way: this read started before
            // any close the racing recall is for could complete.
            let raced = cfg.recall_gen_check && s.nodes[r].recall_gen != gen;
            if let (Some((ttl, epoch)), false) = (grant, raced) {
                s.nodes[r].deleg = Some(Deleg {
                    until: sent + ttl - cfg.deleg_margin,
                    pos,
                    epoch,
                });
            }
            s.nodes[r].op = NodeOp::ReadWait { must, pos };
            s.saw |= SAW_READINDEX_READ;
        }
        Msg::Recall { to, from, id } => {
            // The delegate stops honouring before it acks.
            s.nodes[to as usize].deleg = None;
            s.nodes[to as usize].recall_gen = s.nodes[to as usize].recall_gen.saturating_add(1);
            s.send(Msg::RecallAck {
                to: from,
                from: to,
                id,
            });
        }
        Msg::RecallAck { to, id, .. } => {
            let h = to as usize;
            if s.nodes[h].grants.iter().any(|g| g.id == id) {
                if s.nodes[h].held.iter().any(|x| x.waiting.contains(&id)) {
                    s.saw |= SAW_RECALL_ACK;
                }
                s.grant_done(cfg, h, id);
            }
        }
    }
}
