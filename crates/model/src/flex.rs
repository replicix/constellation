//! Plan 30 §M10: continuation epochs with flexible quorums — the
//! `FlexEpochs` variant.
//!
//! # Why a model of its own
//!
//! The authority model ([`crate::protocol`]) has no continuation epochs
//! at all (its simplification 14), no clocks, and a state space that is
//! already large. What M10 changes is *who may claim authority*: an S3
//! lease holder, a lease taker, and an epoch that can now form with up
//! to `f` roster nodes missing. Whether that is safe depends on
//! time-bounded promises read across clocks that disagree. So, like
//! M8's `cto` and M9's `backup`, this is a focused `stateright::Model`
//! over the same abstractions: the lease register (holder, epoch,
//! expiry in the holder's clock, a CAS version), the log with its epoch
//! markers, per-node clocks with bounded drift, the `heartbeat/<node>`
//! promise objects, and the epoch records.
//!
//! # The protocol under test (the full rule)
//!
//! `f = epoch_slack`, `N` = the write-eligible roster.
//!
//! - **Promises.** A node with `f > 0` publishes `heartbeat/<node> =
//!   {no_epoch_until}` ("I will not join a continuation epoch before
//!   this instant, in my clock"). It persists the value locally *before*
//!   issuing the PUT ([`Action::Issue`]); the PUT lands later
//!   ([`Action::Land`]), possibly after a crash.
//! - **Joining.** A node joins an epoch only once its own last *issued*
//!   (= persisted) promise has expired in its own clock.
//! - **Silence.** A member publishes no promise while its epoch is open
//!   (until it has learned the epoch closed, [`Action::Learn`]).
//! - **Formation** ([`Action::Form`]): at least `N − f` members, none in
//!   an open epoch, each past its own promise. The epoch holds a lease
//!   only if a member holds it *usably* (its clock is before `expires −
//!   M`) at formation — and, plan 30 §M9, only if the lease's
//!   acknowledgement policy cannot be taken over from outside the epoch:
//!   `Local`, or `Backup` with every listed backup a member; never `S3`.
//!   Formation converts that member's S3 lease into the epoch hold; the
//!   epoch holder then acknowledges locally (what M9's `ack_policy()`
//!   does under `epoch_hold`).
//! - **TTL takeover** ([`Action::ReadHb`] then [`Action::Takeover`]): a
//!   node that is not in an open epoch, whose clock is at or past the
//!   register's expiry, may CAS the register over only after reading the
//!   heartbeat objects *after that expiry* and finding at least `f`
//!   *other* nodes with unexpired promises (`no_epoch_until` later than
//!   its clock at the read). The read and the CAS are separate steps;
//!   the CAS names the register version the read was taken against.
//!   `compare_to_expiry` is the simpler form phase 2 implements: count a
//!   promise when it outlasts the lease's recorded expiry, read at any
//!   time (it is implied by the form above, and clean on its own).
//!   `on_demand` is phase 2's cadence: a promise is issued only after some
//!   node observed the expiry of the lease as it last read it (possibly
//!   stale), [`Action::Observe`].
//! - **Claim resolution.** Of several members that believe they hold the
//!   lease, the highest lease epoch's claim wins, and none is taken
//!   below an epoch some member knows exists (its own tenure, a marker it
//!   tailed, the successor of an epoch it sealed): `resolve_by_epoch`.
//! - **Close** ([`Action::Close`]): the epoch holder, S3 back, re-claims
//!   its own register (unchanged since formation — the CAS names the
//!   formation-time version) without a promise check, ships its journal
//!   and becomes an ordinary S3 holder. Members learn the close and only
//!   then resume promises.
//! - **The hold as a group** (plan 46 §5.7, M1.0 chunk C10;
//!   `check_hold_answers`, knob `hold_answers_uncommitted`). The hold's
//!   configuration is {owner, one member}. The owner executes an op into
//!   its journal at once but answers it only when a member holds the row:
//!   the member copies the owner's stream-ahead ([`Action::StreamAhead`],
//!   persisted) and the owner receives the `StreamAheadAck`
//!   ([`Action::AnswerHold`]). A one-member epoch's hold is `Final` (the
//!   owner's disk; nobody else is in the epoch). `Close` flushes the
//!   journal into the log, where the archive's rule answers it. Today's
//!   behaviour (the knob) answers on the owner's disk in any epoch
//!   (`NonStealable` to its members).
//! - **Retire** ([`Action::Retire`], plan 46 M1.4 step 4, `max_retires`): the
//!   operator retires a hold's owner. Its journal and node are gone, the
//!   members abandon the epoch, the carrier's lease is fenced. A member
//!   keeps the rows it acknowledged (`Node::kept`, committed) and ships them
//!   with its own next claim of the lease; a one-member hold's `Final` rows
//!   went with its owner (the operator's loss). Knob `retire_drops_acked`:
//!   the member drops them (found by `hold_answered_rows_survive_retire`).
//! - **Everything else is today's.** Renewal by CAS; a takeover ships its
//!   epoch marker and tails to head; a holder that tails a higher marker
//!   is deposed; a member of an open epoch does no S3 acquisition of any
//!   kind (TTL, seal, `ack=s3`).
//! - **M9 fast takeovers** (`fast_takeover`): under `Backup(b)` the
//!   listed backup may seal and take over at any moment, skipping the
//!   TTL ([`Action::Seal`]); under `S3` any node may ([`Action::FastS3`]).
//!   Neither is gated by promises (it cannot be: see
//!   `docs/plans/v1/PROGRESS.md`, "Plan 30 M10 — phase 1"); the
//!   formation-side claim rule is what keeps them apart from epochs.
//!
//! - **Plan 43 C7b** (`promise_rule`, `lease_claim`): the rules above compare
//!   values of other nodes' clocks (a promise's `no_epoch_until` with the
//!   lease's `expires`, a taker's clock with the holder's expiry), safe
//!   only while every offset is below the margin. `PromiseRule::Anchored`
//!   has a promise name the register version it was made for ([`Action::ObserveKey`]
//!   then [`Action::Issue`], only once the promiser's own observation of
//!   that version has lasted a lease TTL on its clock) and the taker count
//!   a promise iff it names the version it claims; `ClaimRule::Observed`
//!   has the taker wait a TTL of its own clock from its first read of the
//!   register's current version. Together no clock is compared across
//!   nodes: the evidence is that a node was in no open epoch at an instant at
//!   least a TTL after the key was written, and no epoch formed after that
//!   can carry the key's lease, which has ended. (The S3 path only; the
//!   P2P request-anchored one gives the same evidence.)
//!
//! Each rule is a knob, so the tests can show what each one prevents.
//!
//! # Faults and nondeterminism
//!
//! Partitions are not a separate mechanism: every S3 or P2P step is an
//! action that may simply never fire, so "A and B lose S3", "C is
//! offline" and "C is back with S3 but cut from A and B" are all
//! schedules. Crashes are fail-stop with the persisted state (promise,
//! epoch membership, the epoch holder's journal) intact on
//! [`Action::Restart`]; an S3 lease belief is not kept across a restart
//! (the real restart re-adopts through the takeover gate). Clocks:
//! `local(i) = t + off[i]`, `|off[i]| ≤ D`, chosen at start (M8's
//! discipline); the lease is usable while `local < expires − M` and
//! claimable at `local ≥ expires`.
//!
//! # Properties
//!
//! - `single_authority` (always): at most one live node *could
//!   acknowledge a write now*. An epoch holder always can; an S3 holder
//!   can while its lease is usable in its own clock, and further under
//!   `Backup(b)` while `b` is alive and has not sealed its epoch (M9's
//!   write-all ack), under `S3` while no newer marker is in the log (the
//!   slot fence). This is "never two concurrent authorities": an epoch
//!   and an S3 holder, two epochs, or two S3 holders.
//! - `linearizable` (always): every operation executes atomically at an
//!   authority (an S3 holder ships it into the log on the spot; an epoch
//!   holder journals it), so the history is totally ordered in real time
//!   and is linearizable iff replaying it through
//!   [`crate::namespace::NamespaceSpec`] reproduces every return.
//! - `hold_answer_committed_or_final` (plan 46 C10, opt-in): a row the hold
//!   answered while its epoch is open is held by a member other than the
//!   owner, or the epoch has one member. (Definitional at the answer, like
//!   `ack_implies_committed_by_config`: the model's owner keeps its disk
//!   across a crash, so the loss the rule prevents, the owner's death with
//!   the epoch journal, is the sim's `epoch-member-dies-with-chunk`.)
//! - `converged_at_quiescence` (always): once no epoch is open, every
//!   acknowledged operation is in the log (an epoch whose close finds the
//!   register moved has lost its journal to a conflict).
//! - `sometimes` witnesses, so the clean runs are not vacuous: an epoch
//!   formed with a roster node missing and holding the lease; a TTL
//!   takeover that passed the promise check; an epoch flushed with its
//!   journal; every operation done; the fast takeovers when enabled.
//!
//! # Model action → code (phase 2 targets; see PROGRESS.md)
//!
//! | Model | Code |
//! |---|---|
//! | `Issue` / `Land` | `core/promise.rs` `promise_now`: `Replica::issue_promise` (`Meta::promise_issue`, refused while `Meta::promise_join_begin` holds the gate), then `S3Op::HeartbeatPut` (`HeartbeatStore::put`); on a `PeerMsg::PromiseRequest`, or on an observed expiry while P2P is down (`on_demand`) |
//! | `Form` | `crates/cli/src/epoch.rs` `EpochManager::maybe_propose` / `handle_propose` (`component_quorum`, `Meta::promise_join_begin`, acks with `EpochClaimView::claim`, `resolve_epoch_claims`) → `Control::Epoch { carrier, stale_below }` → `Core::on_epoch_state` (`carries_mine` + `epoch_may_carry`, or `deposed` when stale) |
//! | `ReadHb` / `Takeover` | `core/jobs.rs` `acquire_classified` → `Plan::Claim { takeover: true }` → `Phase::PromiseCheck` (`core/promise.rs`: `S3Op::HeartbeatRead` plus `PeerMsg::PromiseRequest`s, `effective_slack`, `takeover_check` against the lease's `expires_unix_ms` — `compare_to_expiry`) → `acquire_cas` |
//! | `Seal` / `FastS3` | M9: `core/backup.rs` takeover permit → `LeaseState::classify` (`permit_allows`) |
//! | `Close` / `Learn` | `core/jobs.rs` `TailThen::EpochProbe` (only a probed round closes) → `Action::EpochClose` → re-acquisition, exempt when the lease object is exactly the carried one (`promise_check_needed`) → `Action::EpochFlushed`; the hold owner stays silent until its journal is in (`flush_pending`) |
//! | `Renew` / `Tail` / `Op` | today's renewal, tail (deposition on a higher marker), and execution |
//!
//! # Simplifications
//!
//! 1. One partition (one lease), one name, and a global op budget; an op
//!    runs on the authority's own node (invocation and return in one
//!    step), which is all a split brain needs to show up as two `Ok`
//!    creates.
//! 2. The P2P handoff inside an epoch is modelled by transfer numbers
//!    (`Msg`, `HoldRec`): a node that holds gives as transfer `n + 1`
//!    (persisting `Given` first), the giver re-offers `n` to its recipient
//!    when asked and nudges it every round, and a node takes only an `n`
//!    above the highest it ever took or gave, inside its own open epoch.
//!    It moves the hold between members of the same epoch, never outside
//!    it. The `FlexEpochs` knobs switch each guard off in turn.
//! 3. Only epochs that claim the lease are formed: an epoch holding
//!    nothing only silences its members (fewer promises, fewer
//!    acquisitions), which can only make takeovers rarer.
//! 4. A heartbeat read is a snapshot of every object at one instant; a
//!    real LIST + GETs reads them one by one, each after the expiry,
//!    which the per-node argument in PROGRESS.md covers.
//! 5. One heartbeat PUT in flight per node; out-of-order landing of two
//!    PUTs can only show an *older* (smaller) promise.

use crate::namespace::{self, NamespaceSpec, NsOp, NsRet};
use stateright::semantics::SequentialSpec;
use stateright::{Model, Property};

/// A time every clock has passed: expired promises and leases are
/// normalized to it, so states that differ only in how long ago
/// something expired are one state.
pub const PAST: i16 = -100;
/// A time no clock reaches within the tick horizon: values beyond it are
/// normalized to it.
pub const FAR: i16 = 100;

/// Plan 43 C7b: what a promise (`heartbeat/<node>`) says and how a taker
/// counts it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PromiseRule {
    /// The design before C7b: `no_epoch_until`, an instant on the promiser's
    /// wall; a taker counts a promise that outlasts the lease's expiry (or
    /// is unexpired on its own clock): a compare of another node's clock,
    /// safe only while every offset is below the margin.
    #[default]
    Expiry,
    /// Plan 43 C7b commit 5: a promise names the lease key it was made for,
    /// and is made only once the promiser's own observation of that key has
    /// lasted a lease TTL on its wait reading (`Action::ObserveKey`, then
    /// `Action::Issue`); a taker counts a promise iff it names the key it is
    /// claiming. No clock is compared across nodes: the evidence is "this
    /// node was in no open epoch at an instant at least a TTL after the key
    /// was written", and no epoch formed after that instant can carry the
    /// key's lease (it has ended).
    Anchored,
}

/// The lease's acknowledgement policy (plan 30 §M9).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Policy {
    Local,
    /// One listed backup.
    Backup(u8),
    S3,
}

#[derive(Clone, Debug)]
pub struct FlexEpochs {
    /// Write-eligible roster size `N`; node 0 holds at start.
    pub nodes: u8,
    /// `f`.
    pub epoch_slack: u8,
    /// Formation quorum; the rule is `N − f`.
    pub min_members: u8,
    pub lease_ttl: i16,
    pub promise_ttl: i16,
    /// The lease margin `M` (usable while `local < expires − M`).
    pub margin: i16,
    /// Clock drift bound `D`: every `|off[i]| ≤ D`.
    pub drift: i16,
    /// Only the extreme offsets `±D` (every pairwise disagreement is
    /// then `0` or `2D`), for a CI-sized drift run; the exhaustive one
    /// takes every offset in `[−D, D]`.
    pub extreme_offsets: bool,
    pub max_tick: i16,
    /// Node 0's lease at start expires this long after its clock's 0
    /// (it renewed `lease_ttl − initial_expiry` ago).
    pub initial_expiry: i16,
    /// Every node's promise at start runs out this long after its
    /// clock's 0 (it published `promise_ttl − initial_promise` ago).
    pub initial_promise: i16,
    pub max_crashes: u8,
    pub restart: bool,
    /// A heartbeat PUT may stay in flight across other steps (and a
    /// crash). Off: [`Action::Issue`] lands at once — a smaller model for
    /// the drift runs; with `persist_before_publish` the in-flight value
    /// never exceeds the persisted one, so it cannot matter there.
    pub inflight_puts: bool,
    pub max_epochs: u8,
    /// Bound on register epochs (takeovers).
    pub max_reg_epoch: u8,
    /// The operations executed, in order, by whichever authority runs
    /// them.
    pub ops: Vec<NsOp>,
    /// The initial register's policy.
    pub policy: Policy,
    /// M9's seal-based / `ack=s3` fast takeovers.
    pub fast_takeover: bool,
    // ---- the rules, each a knob ----
    pub takeover_promise_check: bool,
    pub join_after_own_promise: bool,
    pub persist_before_publish: bool,
    pub read_after_expiry: bool,
    /// Count a promise when it outlasts the lease's recorded expiry
    /// (`no_epoch_until > expires`), instead of when it is unexpired in
    /// the taker's clock at the read. Safe with the read taken at any
    /// time (see PROGRESS.md, "Plan 30 M10 — phase 1").
    pub compare_to_expiry: bool,
    /// Plan 30 §M10 phase 2's cadence: a node publishes a promise only
    /// once a lease expiry has been observed — by itself, or by a would-be
    /// taker that asks it: some node's clock is past the expiry *as that
    /// node last read the register* ([`Action::Observe`]), which may be
    /// stale (the lease renewed since). Off: at any time (a steady
    /// refresh).
    pub on_demand: bool,
    pub silent_in_epoch: bool,
    /// Formation claims only `Local`, or `Backup` with the backup a
    /// member (M9 interaction).
    pub claim_rule: bool,
    /// When several members believe they hold the lease, the epoch takes
    /// the highest lease epoch's claim, and none below an epoch any member
    /// knows of (the others are stale).
    pub resolve_by_epoch: bool,
    /// Plan 43 C0: check `hold_owner_never_deposed_except_by_retire` (Q3).
    /// Off by default so a counterexample search that stops at the first
    /// failed property (`HasDiscoveries::AnyFailures`) still ends on the
    /// property a test is about.
    pub check_hold_owner: bool,
    /// Plan 46 M1.0 C10 (§5.7): check `hold_answer_committed_or_final`.
    /// Off by default for the same reason as `check_hold_owner`; off, the
    /// hold answers on the owner's disk as before (no `StreamAhead`
    /// interleavings, so the older runs' state counts are unchanged).
    pub check_hold_answers: bool,
    /// Plan 46 M1.0 C10 knob: the hold answers a row before one member has
    /// it (today's `NonStealable` answer to members); the design waits for
    /// a [`Action::StreamAhead`] and answers only then, except in a
    /// one-member epoch, whose hold is `Final`.
    pub hold_answers_uncommitted: bool,
    /// Plan 46 M1.4 step 4 (Q4): the operator may retire a hold's owner
    /// ([`Action::Retire`]) this many times (0: no retire, the older runs'
    /// state space). The owner and its epoch journal are gone, its members
    /// abandon the epoch, and the lease it carried is fenced.
    pub max_retires: u8,
    /// Plan 46 M1.4 step 4 knob: a member drops the rows it acknowledged
    /// when the owner is retired (today's abandon: the stream-ahead rows were
    /// speculation, stranded with the epoch). The design: the member keeps
    /// them as committed, and ships them when it claims the lease.
    pub retire_drops_acked: bool,
    // ---- plan 43 C7b ----
    /// What a promise says and how a taker counts it ([`PromiseRule`]).
    pub promise_rule: PromiseRule,
    /// How a taker decides the register's lease has run out
    /// ([`crate::clk::ClaimRule`]): its clock against the holder's expiry
    /// (`Wall`), or one TTL after its first read of the register's current
    /// version (`Observed`; a node's clock then only measures a duration).
    /// (Not `claim_rule`, which is the formation's.)
    pub lease_claim: crate::clk::ClaimRule,
    // ---- FX-5: the hold moves between members (transfer numbers) ----
    /// Bound on transfer numbers: a hold at `n` gives only while
    /// `n < max_transfers`. 0: no transfer actions (the older runs' state
    /// space).
    pub max_transfers: u8,
    /// Asks not prompted by a nudge (ops waiting), in the whole run
    /// (`None`: unbounded). `Some(1)`: one acquisition asks, its ops give
    /// up, and from then on members ask only when nudged (the idle
    /// recipient, M2).
    pub free_asks: Option<u8>,
    /// A message may be delivered and stay in flight (a duplicate).
    pub dup_msgs: bool,
    /// Knob: the giver never re-offers (today's code).
    pub no_reoffer: bool,
    /// Knob: the giver never nudges an idle recipient.
    pub no_nudge: bool,
    /// Knob: the taker drops `n > highest` (takes any offer while it holds
    /// nothing).
    pub reoffer_ignores_held: bool,
    /// Knob: the giver re-offers to any member that asks, not only `to`.
    pub reoffer_to_anyone: bool,
    /// Knob: the give's record is in memory only (persisted after the
    /// reply, lost by a crash before that).
    pub record_not_persisted: bool,
    /// Knob: the taker does not check that its epoch is open and the
    /// sender a member.
    pub take_after_close: bool,
    /// Knob: spec v1's guard: an original transfer answer is taken
    /// whenever it answers the live ask; a re-offer only by a node with no
    /// record of the key (it never held or gave).
    pub v1_guard: bool,
}

impl FlexEpochs {
    /// The full rule: `N` nodes, slack `f`, honest clocks.
    pub fn new(nodes: u8, epoch_slack: u8) -> Self {
        FlexEpochs {
            nodes,
            epoch_slack,
            min_members: nodes - epoch_slack,
            lease_ttl: 8,
            promise_ttl: 2,
            margin: 0,
            drift: 0,
            extreme_offsets: false,
            max_tick: 5,
            initial_expiry: 3,
            initial_promise: 2,
            max_crashes: 1,
            restart: true,
            inflight_puts: true,
            max_epochs: 1,
            max_reg_epoch: 2,
            ops: vec![NsOp::CreateExcl(0), NsOp::CreateExcl(0)],
            policy: Policy::Local,
            fast_takeover: false,
            takeover_promise_check: true,
            join_after_own_promise: true,
            persist_before_publish: true,
            read_after_expiry: true,
            compare_to_expiry: false,
            on_demand: false,
            silent_in_epoch: true,
            claim_rule: true,
            resolve_by_epoch: true,
            check_hold_owner: false,
            check_hold_answers: false,
            hold_answers_uncommitted: false,
            max_retires: 0,
            retire_drops_acked: false,
            promise_rule: PromiseRule::Expiry,
            lease_claim: crate::clk::ClaimRule::Wall,
            max_transfers: 0,
            free_asks: None,
            dup_msgs: false,
            no_reoffer: false,
            no_nudge: false,
            reoffer_ignores_held: false,
            reoffer_to_anyone: false,
            record_not_persisted: false,
            take_after_close: false,
            v1_guard: false,
        }
    }

    /// Clocks within `drift` of real time, with margin `margin`.
    pub fn with_drift(mut self, drift: i16, margin: i16) -> Self {
        self.drift = drift;
        self.margin = margin;
        self
    }

    /// Plan 30 §M10's validation: the promise TTL is at most a quarter
    /// of the lease TTL (a liveness rule: promises must run out well
    /// before the leases an epoch wants to carry do).
    pub fn ttl_rule_holds(&self) -> bool {
        self.promise_ttl * 4 <= self.lease_ttl
    }

    /// Plan 46 C10: the hold's answers wait for a member. Only when the
    /// property is checked: the earlier flex runs (which never looked at
    /// answers) keep today's state space, and the knob is today's answer.
    fn hold_waits(&self) -> bool {
        self.check_hold_answers && !self.hold_answers_uncommitted
    }

    fn f(&self) -> u8 {
        self.epoch_slack
    }

    fn promises(&self) -> bool {
        self.epoch_slack > 0
    }
}

/// A node's view of the lease it holds via S3.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LeaseView {
    pub epoch: u8,
    pub expires: i16,
    pub version: u8,
    pub policy: Policy,
}

/// The lease object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    /// In the holder's clock.
    pub expires: i16,
    pub version: u8,
    pub policy: Policy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Entry {
    Marker(u8),
    /// An op (its history index).
    Op(u8),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub alive: bool,
    pub lease: Option<LeaseView>,
    pub applied: u8,
    /// The promise this node knows it may have published (persisted).
    pub promise: i16,
    /// A heartbeat PUT in flight.
    pub pending: Option<i16>,
    /// Membership (persisted): an index into `State::epochs`.
    pub epoch: Option<u8>,
    /// A heartbeat read that found `f` unexpired promises, against this
    /// register version (volatile).
    pub snap: Option<u8>,
    /// M9: the highest register epoch this node sealed as a backup.
    pub sealed: u8,
    /// The highest lease epoch this node knows exists (persisted): its
    /// own tenures, the markers it tailed, the successor of an epoch it
    /// sealed.
    pub known: u8,
    /// An epoch holder's journal (history indices; persisted).
    pub journal: Vec<u8>,
    /// Plan 46 C10: a member's copy of the owner's stream-ahead (history
    /// indices; persisted); the owner's `StreamAheadAck` source. Dropped
    /// when the member leaves the epoch.
    pub copy: Vec<u8>,
    /// Plan 46 M1.4 step 4: the rows a member acknowledged and kept when the
    /// owner was retired (history indices; persisted): committed, and
    /// shipped into the log by this node's next claim of the lease.
    pub kept: Vec<u8>,
    /// Plan 46 M1.4 step 4: retired by the operator: never restarts.
    pub retired: bool,
    /// `on_demand`: the register's expiry as this node last read it.
    pub seen: i16,
    /// Plan 43 C7b (`Observed` claims, `Anchored` promises): the register
    /// version this node's observation is of, and this node's clock at the
    /// first read that showed it.
    pub obs: Option<(u8, i16)>,
    /// A heartbeat PUT in flight names this register version (`Anchored`).
    pub pending_key: Option<u8>,
    /// FX-5: this node's persisted record of the hold of an epoch (by
    /// index; the model's key of the carried lease). `Held` is the hold:
    /// a node acknowledges under an epoch hold iff its record says it
    /// holds it (persisted before it acts, M4).
    pub rec: Option<(u8, HoldRec)>,
    /// `record_not_persisted` only: the record as this process believes
    /// it, ahead of the persisted one; dropped by a crash.
    pub mem_rec: Option<(u8, HoldRec)>,
    /// FX-5: the member this node's live epoch acquisition asked
    /// (volatile: the phase `LeaseRequest { epoch_mode }`).
    pub ask: Option<u8>,
}

/// FX-5: a node's persisted record of one carried key's hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HoldRec {
    /// Holds it, taken as transfer `n` (0: adopted at formation).
    Held { n: u8 },
    /// Gave it as transfer `n` to `to`.
    Given { n: u8, to: u8 },
    /// Let it go without a transfer (the close, a retire).
    Ended { n: u8 },
}

impl HoldRec {
    pub fn n(self) -> u8 {
        match self {
            HoldRec::Held { n } | HoldRec::Given { n, .. } | HoldRec::Ended { n } => n,
        }
    }
}

/// FX-5: a P2P message of the hold transfer, in flight.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    /// An epoch `LeaseRequest`.
    Request { from: u8, to: u8 },
    /// A `LeaseHandoff` carrying transfer `n` of epoch `key`'s hold
    /// (`re`: a re-offer).
    Offer {
        from: u8,
        to: u8,
        key: u8,
        n: u8,
        re: bool,
    },
    /// The giver's nudge (carries `n` as a filter only).
    Nudge { from: u8, to: u8, key: u8, n: u8 },
}

impl Msg {
    fn to(self) -> u8 {
        match self {
            Msg::Request { to, .. } | Msg::Offer { to, .. } | Msg::Nudge { to, .. } => to,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EpochRec {
    pub members: u8,
    /// The member whose lease the epoch carries (its hold at formation).
    pub carrier: u8,
    /// The register version and epoch the holder's lease had.
    pub version: u8,
    pub reg_epoch: u8,
    pub closed: bool,
}

pub const SAW_MISSING_EPOCH: u32 = 1;
pub const SAW_CHECKED_TAKEOVER: u32 = 2;
pub const SAW_FLUSH: u32 = 4;
pub const SAW_SEAL: u32 = 8;
pub const SAW_FAST_S3: u32 = 16;
pub const SAW_CONFLICT: u32 = 32;
/// Plan 43 C0: an epoch's hold owner lost its epoch journal because the
/// register moved under it (`Action::Close`'s strand branch).
pub const SAW_HOLD_OWNER_DEPOSED: u32 = 64;
/// Plan 46 C10: a hold answered a row after a member's `StreamAheadAck`.
pub const SAW_HOLD_MEMBER_ANSWER: u32 = 128;
/// Plan 46 C10: a one-member epoch's hold answered (`Final`).
pub const SAW_HOLD_FINAL_ANSWER: u32 = 256;
/// Plan 46 M1.4 step 4: the operator retired a hold's owner.
pub const SAW_RETIRE: u32 = 512;
/// Plan 46 M1.4 step 4: a retired one-member hold's owner took rows answered
/// `Final` (its disk alone) with it: the operator's loss, by the rule.
pub const SAW_RETIRE_FINAL_LOSS: u32 = 1024;
/// Plan 46 M1.4 step 4: a claim shipped rows a member kept at a retire.
pub const SAW_RETIRE_CARRIED: u32 = 2048;
/// FX-5: a transfer's first answer was taken.
pub const SAW_TRANSFER_TAKEN: u32 = 4096;
/// FX-5: a re-offer was taken.
pub const SAW_REOFFER_TAKEN: u32 = 8192;
/// FX-5: a node that took the hold over P2P flushed it at the close.
pub const SAW_TRANSFEREE_FLUSH: u32 = 16384;
/// FX-5: a nudge started an acquisition.
pub const SAW_NUDGE_ASK: u32 = 32768;
/// FX-5: a transfer of number 3 was taken (the G, R, Q cycle: a former
/// giver takes the hold back above everything it took or gave).
pub const SAW_THIRD_TRANSFER: u32 = 65536;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: i16,
    pub off: Vec<i16>,
    pub reg: Reg,
    pub log: Vec<Entry>,
    /// `heartbeat/<node>`: `no_epoch_until` (in the publisher's clock).
    pub hb: Vec<i16>,
    /// Plan 43 C7b (`Anchored`): the register version each heartbeat names.
    pub hb_key: Vec<Option<u8>>,
    pub nodes: Vec<Node>,
    pub epochs: Vec<EpochRec>,
    pub crashes: u8,
    /// Executed ops: `(op, return)`, in real-time order.
    pub history: Vec<(NsOp, NsRet)>,
    /// Plan 46 C10: bitmask of the history indices the client was answered
    /// (executed ops are answered at once outside a hold).
    pub answered: u16,
    pub saw: u32,
    /// Plan 46 M1.4 step 4: owners retired so far.
    pub retires: u8,
    /// Rows (history indices) a retire took with its owner that no client
    /// was told were committed (never answered; or answered `Final` by a
    /// one-member hold: the operator's loss): out of the history for the
    /// linearizability and convergence properties.
    pub lost: u16,
    /// Rows a retire dropped although the client was answered and a member
    /// held them (only with the `retire_drops_acked` knob).
    pub dropped: u16,
    /// FX-5: transfer messages in flight (a set: sorted, deduplicated).
    pub msgs: Vec<Msg>,
    /// FX-5: asks made without a nudge (bounded by `free_asks`).
    pub asks: u8,
}

impl State {
    pub fn local(&self, i: usize) -> i16 {
        self.t + self.off[i]
    }

    fn log_dir(&self, upto: usize) -> u8 {
        self.log[..upto].iter().fold(0, |d, e| match e {
            Entry::Op(k) => namespace::force_apply(d, self.history[*k as usize].0),
            Entry::Marker(_) => d,
        })
    }

    fn open_epoch_of(&self, i: usize) -> Option<&EpochRec> {
        self.nodes[i].epoch.map(|k| &self.epochs[k as usize])
    }

    /// FX-5: the node's record as its process sees it (the persisted one,
    /// or under `record_not_persisted` the one ahead of it).
    pub fn eff_rec(&self, i: usize) -> Option<(u8, HoldRec)> {
        self.nodes[i].mem_rec.or(self.nodes[i].rec)
    }

    /// The node holds an epoch hold: its own record says so (M4).
    pub fn epoch_holder(&self, i: usize) -> bool {
        matches!(self.eff_rec(i), Some((_, HoldRec::Held { .. })))
    }

    /// FX-5: the node that holds epoch `idx`'s hold (the first, if the
    /// property is broken).
    pub fn hold_owner(&self, idx: u8) -> Option<usize> {
        (0..self.nodes.len())
            .find(|j| matches!(self.eff_rec(*j), Some((k, HoldRec::Held { .. })) if k == idx))
    }

    /// FX-5: the node is a member of epoch `idx` and has not learned it
    /// ended (persisted membership).
    fn in_open_epoch(&self, i: usize, idx: u8) -> bool {
        self.nodes[i].epoch == Some(idx) && !self.epochs[idx as usize].closed
    }

    fn member(&self, idx: u8, j: u8) -> bool {
        self.epochs[idx as usize].members & (1 << j) != 0
    }

    /// FX-5: the highest transfer number of key `idx` this node took or
    /// gave (0 without a record of the key).
    fn highest(&self, i: usize, idx: u8) -> u8 {
        match self.eff_rec(i) {
            Some((k, r)) if k == idx => r.n(),
            _ => 0,
        }
    }

    /// FX-5 give guard: `g` holds at `n`, in its open epoch, journal
    /// empty, the asker a member. The transfer number to send.
    pub fn give_guard(&self, m: &FlexEpochs, g: usize, r: u8) -> Option<(u8, u8)> {
        let Some((idx, HoldRec::Held { n })) = self.eff_rec(g) else {
            return None;
        };
        (self.in_open_epoch(g, idx)
            && self.member(idx, r)
            && r as usize != g
            && self.nodes[g].journal.is_empty()
            && n < m.max_transfers)
            .then_some((idx, n + 1))
    }

    /// FX-5 re-offer guard: `g`'s record says it gave transfer `n` to
    /// `r` and it holds nothing since, in its open epoch, `r` a member.
    pub fn reoffer_guard(&self, m: &FlexEpochs, g: usize, r: u8) -> Option<(u8, u8)> {
        if m.no_reoffer {
            return None;
        }
        let Some((idx, HoldRec::Given { n, to })) = self.eff_rec(g) else {
            return None;
        };
        ((to == r || m.reoffer_to_anyone)
            && r as usize != g
            && self.in_open_epoch(g, idx)
            && self.member(idx, r)
            && self.nodes[g].journal.is_empty())
        .then_some((idx, n))
    }

    /// FX-5 nudge guard: `g`'s record names an outstanding recipient.
    pub fn nudge_guard(&self, m: &FlexEpochs, g: usize) -> Option<(u8, u8, u8)> {
        if m.no_nudge {
            return None;
        }
        let Some((idx, HoldRec::Given { n, to })) = self.eff_rec(g) else {
            return None;
        };
        (self.in_open_epoch(g, idx) && self.member(idx, to)).then_some((idx, to, n))
    }

    /// FX-5 take guard (M1, M3), over the node's record and membership:
    /// its epoch open, the sender a member, transfer `n` above its
    /// highest of the key, and it holds nothing.
    pub fn take_guard(&self, m: &FlexEpochs, r: usize, from: u8, key: u8, n: u8, re: bool) -> bool {
        if self.epoch_holder(r) {
            return false;
        }
        if !m.take_after_close && !(self.in_open_epoch(r, key) && self.member(key, from)) {
            return false;
        }
        if m.v1_guard {
            let never = !matches!(self.eff_rec(r), Some((k, _)) if k == key);
            return !re || never;
        }
        m.reoffer_ignores_held || n > self.highest(r, key)
    }

    /// FX-5 `hold_recoverable`: every open epoch's hold is held (a
    /// non-retired node's record says so: a crashed holder restores it),
    /// or some non-retired giver's record lets it nudge, re-offer and have
    /// the re-offer taken (the guards the actions use, over records and
    /// membership only).
    pub fn hold_recoverable(&self, m: &FlexEpochs) -> bool {
        (0..self.epochs.len()).all(|idx| {
            let idx = idx as u8;
            if self.epochs[idx as usize].closed {
                return true;
            }
            let live = |j: usize| !self.nodes[j].retired;
            let held = (0..self.nodes.len()).any(|j| {
                live(j) && matches!(self.eff_rec(j), Some((k, HoldRec::Held { .. })) if k == idx)
            });
            held || (0..self.nodes.len()).any(|g| {
                live(g)
                    && self.nudge_guard(m, g).is_some_and(|(k, r, n)| {
                        k == idx
                            && live(r as usize)
                            && self.reoffer_guard(m, g, r) == Some((k, n))
                            && self.take_guard(m, r as usize, g as u8, k, n, true)
                    })
            })
        })
    }

    fn send(&mut self, msg: Msg) {
        if let Err(at) = self.msgs.binary_search(&msg) {
            self.msgs.insert(at, msg);
        }
    }

    /// Plan 46 C10: the one-member epoch of node `i`'s hold is `Final`.
    fn hold_final(&self, i: usize) -> bool {
        self.open_epoch_of(i)
            .is_some_and(|r| r.members.count_ones() == 1)
    }

    /// Plan 46 C10: rows of the hold owner `i`'s journal a member holds.
    fn member_held(&self, i: usize) -> u16 {
        let Some(idx) = self.nodes[i].epoch else {
            return 0;
        };
        let mask = self.epochs[idx as usize].members;
        let mut held = 0u16;
        for j in 0..self.nodes.len() {
            if j != i && mask & (1 << j) != 0 && self.nodes[j].epoch == Some(idx) {
                for k in &self.nodes[j].copy {
                    held |= 1 << k;
                }
            }
        }
        held
    }

    /// Plan 46 C10: some journal row of owner `i` is unanswered and held
    /// by a member.
    fn hold_answerable(&self, i: usize) -> bool {
        let held = self.member_held(i);
        self.nodes[i]
            .journal
            .iter()
            .any(|k| held & (1 << k) != 0 && self.answered & (1 << k) == 0)
    }

    fn s3_usable(&self, m: &FlexEpochs, i: usize) -> Option<LeaseView> {
        let l = self.nodes[i].lease?;
        (self.local(i) < l.expires - m.margin).then_some(l)
    }

    /// Could node `i` acknowledge a write now?
    pub fn can_ack(&self, m: &FlexEpochs, i: usize) -> bool {
        let n = &self.nodes[i];
        if !n.alive {
            return false;
        }
        // FX-5 (M4): the node's own record of the hold is its authority.
        if self.epoch_holder(i) {
            return true;
        }
        if n.epoch.is_some() {
            // A member that holds nothing has no authority.
            return false;
        }
        let Some(l) = self.s3_usable(m, i) else {
            return false;
        };
        match l.policy {
            Policy::Local => true,
            Policy::Backup(b) => {
                let b = &self.nodes[b as usize];
                b.alive && b.sealed < l.epoch
            }
            Policy::S3 => n.applied as usize == self.log.len(),
        }
    }

    pub fn authorities(&self, m: &FlexEpochs) -> Vec<u8> {
        (0..self.nodes.len())
            .filter(|i| self.can_ack(m, *i))
            .map(|i| i as u8)
            .collect()
    }

    /// Other roster nodes whose published promise is unexpired in `i`'s
    /// clock now (or, `compare_to_expiry`, outlasts the lease's expiry).
    fn unexpired_others(&self, m: &FlexEpochs, i: usize) -> u8 {
        if m.promise_rule == PromiseRule::Anchored {
            // Counted iff it names the key being claimed.
            return (0..self.nodes.len())
                .filter(|j| *j != i && self.hb_key[*j] == Some(self.reg.version))
                .count() as u8;
        }
        let now = if m.compare_to_expiry {
            self.reg.expires
        } else {
            self.local(i)
        };
        (0..self.nodes.len())
            .filter(|j| *j != i && self.hb[*j] > now)
            .count() as u8
    }

    /// Whether node `i` may treat the register's lease as run out.
    fn claimable_by(&self, m: &FlexEpochs, i: usize) -> bool {
        match m.lease_claim {
            crate::clk::ClaimRule::Wall => self.local(i) >= self.reg.expires,
            crate::clk::ClaimRule::Observed => self.nodes[i].obs.is_some_and(|(v, since)| {
                v == self.reg.version && self.local(i) - since >= m.lease_ttl
            }),
        }
    }

    /// Node `i`'s observation of the register's current version is a lease
    /// TTL old (its own clock measuring a duration).
    fn observation_passed(&self, m: &FlexEpochs, i: usize) -> bool {
        self.nodes[i]
            .obs
            .is_some_and(|(v, since)| v == self.reg.version && self.local(i) - since >= m.lease_ttl)
    }

    fn takeover(&mut self, m: &FlexEpochs, i: usize) {
        let epoch = self.reg.epoch + 1;
        let expires = self.local(i) + m.lease_ttl;
        let version = self.reg.version + 1;
        self.reg = Reg {
            holder: i as u8,
            epoch,
            expires,
            version,
            policy: Policy::Local,
        };
        self.log.push(Entry::Marker(epoch));
        self.nodes[i].applied = self.log.len() as u8;
        self.nodes[i].lease = Some(LeaseView {
            epoch,
            expires,
            version,
            policy: Policy::Local,
        });
        self.nodes[i].snap = None;
        self.nodes[i].known = epoch;
        // Plan 46 M1.4 step 4: the rows this node kept at the owner's retire
        // are committed (the owner answered them once it held them): the
        // claim ships them behind its marker, verbatim.
        // (Any member's: the claim fetches the group's tail from one member
        // that has it, or the rid is replayed; the model orders them first.)
        let mut kept: Vec<u8> = Vec::new();
        for n in self.nodes.iter_mut() {
            for k in std::mem::take(&mut n.kept) {
                if !kept.contains(&k) {
                    kept.push(k);
                }
            }
        }
        kept.sort_unstable();
        for k in kept {
            self.saw |= SAW_RETIRE_CARRIED;
            if !self.log.contains(&Entry::Op(k)) {
                self.log.push(Entry::Op(k));
            }
            self.answered |= 1 << k;
        }
        self.nodes[i].applied = self.log.len() as u8;
    }

    /// Collapse what no longer matters: expired values to [`PAST`], a
    /// heartbeat read against an old register version, the applied
    /// position of a node that neither holds nor needs to (it tails to
    /// head before it could act).
    fn normalize(&mut self, m: &FlexEpochs) {
        let floor = self.t - m.drift;
        let ceil = m.max_tick + m.drift;
        let squash = |v: &mut i16| {
            if *v <= floor {
                *v = PAST;
            } else if *v > ceil {
                *v = FAR;
            }
        };
        for v in self.hb.iter_mut() {
            squash(v);
        }
        squash(&mut self.reg.expires);
        let len = self.log.len() as u8;
        for i in 0..self.nodes.len() {
            let local = self.local(i);
            let holder = self.epoch_holder(i);
            let version = self.reg.version;
            let n = &mut self.nodes[i];
            if m.on_demand {
                // Only `local ≥ seen` matters, and once true stays true.
                if n.seen <= local {
                    n.seen = PAST;
                } else if n.seen > ceil {
                    n.seen = FAR;
                }
            } else {
                n.seen = 0;
            }
            if let Some((_, since)) = n.obs.as_mut() {
                // Only `local - since >= ttl` matters, and once true stays
                // true for this version.
                if local - *since >= m.lease_ttl {
                    *since = PAST;
                }
            }
            if n.promise <= local {
                n.promise = PAST;
            } else if n.promise > ceil {
                n.promise = FAR;
            }
            if let Some(u) = n.pending.as_mut() {
                if *u > ceil {
                    *u = FAR;
                }
            }
            if n.snap.is_some_and(|v| v != version) {
                n.snap = None;
            }
            if let Some(l) = n.lease.as_mut() {
                if l.expires - m.margin <= floor {
                    l.expires = PAST;
                } else if l.expires - m.margin > ceil {
                    l.expires = FAR;
                }
            } else if !holder {
                n.applied = len;
            }
        }
        // Register versions are compared for equality only: rank them.
        let mut seen: Vec<u8> = vec![self.reg.version];
        for n in &self.nodes {
            seen.extend(n.lease.map(|l| l.version));
            seen.extend(n.snap);
            seen.extend(n.obs.map(|(v, _)| v));
            seen.extend(n.pending_key);
        }
        seen.extend(self.hb_key.iter().flatten());
        seen.extend(self.epochs.iter().map(|r| r.version));
        seen.sort_unstable();
        seen.dedup();
        let rank = |v: u8| seen.iter().position(|x| *x == v).unwrap() as u8;
        self.reg.version = rank(self.reg.version);
        for n in self.nodes.iter_mut() {
            if let Some(l) = n.lease.as_mut() {
                l.version = rank(l.version);
            }
            n.snap = n.snap.map(rank);
            n.obs = n.obs.map(|(v, since)| (rank(v), since));
            n.pending_key = n.pending_key.map(rank);
        }
        for k in self.hb_key.iter_mut() {
            *k = k.map(rank);
        }
        for r in self.epochs.iter_mut() {
            r.version = rank(r.version);
        }
    }

    fn all_ops_in_log(&self) -> bool {
        (0..self.history.len()).all(|k| {
            self.log.contains(&Entry::Op(k as u8))
                // After a retire: a row the owner never answered went with it
                // (its client retries), and one a member kept waits in its
                // store for the next claim.
                || (self.lost | self.dropped) & (1 << k) != 0
                || self.kept_by_a_node(k as u8)
        })
    }

    fn kept_by_a_node(&self, k: u8) -> bool {
        self.nodes.iter().any(|n| n.kept.contains(&k))
    }

    /// Plan 46 M1.4 step 4: every row the client was answered is in the
    /// log, in an open epoch's owner journal, or kept by a member (shipped
    /// by its claim). Not after a deposed owner's lost journal
    /// (`SAW_HOLD_OWNER_DEPOSED`) or a retired `Final` hold's (the rule).
    fn answered_rows_held(&self) -> bool {
        if self.saw & SAW_HOLD_OWNER_DEPOSED != 0 {
            return true;
        }
        if self.dropped != 0 {
            return false;
        }
        (0..self.history.len()).all(|k| {
            let bit = 1u16 << k;
            self.answered & bit == 0
                || self.lost & bit != 0
                || self.log.contains(&Entry::Op(k as u8))
                || self.kept_by_a_node(k as u8)
                || self
                    .nodes
                    .iter()
                    .enumerate()
                    .any(|(i, n)| self.epoch_holder(i) && n.journal.contains(&(k as u8)))
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    /// Persist (with the rule) and issue a heartbeat promise PUT.
    Issue(u8),
    /// The in-flight heartbeat PUT lands.
    Land(u8),
    Renew(u8),
    /// Read every heartbeat object (a snapshot) for a TTL takeover.
    ReadHb(u8),
    /// TTL takeover of an expired lease.
    Takeover(u8),
    /// M9: the listed backup seals and takes over (no TTL).
    Seal(u8),
    /// M9: `ack=s3` — anyone takes over (no TTL).
    FastS3(u8),
    /// Form an epoch with this member bitmask.
    Form(u8),
    /// The epoch holder flushes and closes.
    Close(u8),
    /// A member learns its epoch closed.
    Learn(u8),
    /// Execute the next op at this authority.
    Op(u8),
    /// Plan 46 C10: this member copies the hold owner's stream-ahead (its
    /// journal) and persists it (`StreamAheadAck`'s subject).
    StreamAhead(u8),
    /// Plan 46 C10: this epoch owner receives a member's `StreamAheadAck`
    /// and answers every row of its journal that a member holds.
    AnswerHold(u8),
    /// Plan 46 M1.4 step 4 (Q4): the operator retires this hold owner (admin
    /// `leave`): its epoch journal is gone with it, its members abandon the
    /// epoch, the lease it carried is fenced (claimable at once).
    Retire(u8),
    /// `on_demand`: read the register (its expiry).
    Observe(u8),
    /// Plan 43 C7b: read the register; the first read showing its current
    /// version starts this node's observation of it (`Observed` claims,
    /// `Anchored` promises).
    ObserveKey(u8),
    /// Tail to head.
    Tail(u8),
    Crash(u8),
    Restart(u8),
    /// FX-5: this member's epoch acquisition asks that member.
    Ask(u8, u8),
    /// FX-5: the acquisition's request times out (its phase ends).
    AskTimeout(u8),
    /// FX-5: a giver whose record names an outstanding recipient nudges it
    /// (its ordinary round).
    Nudge(u8),
    /// FX-5: deliver `msgs[k]`; `true`: it stays in flight (a duplicate).
    Deliver(u8, bool),
}

impl FlexEpochs {
    /// FX-5: the transfer actions of live node `i`.
    fn transfer_actions(&self, s: &State, i: usize, actions: &mut Vec<Action>) {
        let iu = i as u8;
        let node = &s.nodes[i];
        if node.ask.is_some() {
            actions.push(Action::AskTimeout(iu));
        }
        if let Some(idx) = node.epoch {
            if self.free_asks.is_none_or(|k| s.asks < k)
                && node.ask.is_none()
                && !s.epoch_holder(i)
                && s.in_open_epoch(i, idx)
            {
                for g in 0..self.nodes {
                    if g != iu && s.member(idx, g) {
                        actions.push(Action::Ask(iu, g));
                    }
                }
            }
        }
        if let Some((key, r, n)) = s.nudge_guard(self, i) {
            let msg = Msg::Nudge {
                from: iu,
                to: r,
                key,
                n,
            };
            if s.msgs.binary_search(&msg).is_err() {
                actions.push(Action::Nudge(iu));
            }
        }
    }
}

impl Model for FlexEpochs {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.nodes as usize;
        let d = self.drift;
        let span = (2 * d + 1) as usize;
        let mut out = Vec::new();
        for combo in 0..span.pow(n as u32) {
            let mut c = combo;
            let off: Vec<i16> = (0..n)
                .map(|_| {
                    let o = (c % span) as i16 - d;
                    c /= span;
                    o
                })
                .collect();
            // Shifting every clock by the same amount is a relabeling of
            // real time: keep one representative (the slowest clock at
            // `−D`).
            if off.iter().min() != Some(&-d) {
                continue;
            }
            if self.extreme_offsets && off.iter().any(|o| o.abs() != d) {
                continue;
            }
            let expires = off[0] + self.initial_expiry;
            let reg = Reg {
                holder: 0,
                epoch: 1,
                expires,
                version: 1,
                policy: self.policy,
            };
            let hb_key: Vec<Option<u8>> = vec![None; n];
            let hb: Vec<i16> = (0..n)
                .map(|i| {
                    if self.promises() {
                        off[i] + self.initial_promise
                    } else {
                        PAST
                    }
                })
                .collect();
            let nodes = (0..n)
                .map(|i| Node {
                    alive: true,
                    lease: (i == 0).then_some(LeaseView {
                        epoch: 1,
                        expires,
                        version: 1,
                        policy: self.policy,
                    }),
                    applied: 1,
                    promise: hb[i],
                    pending: None,
                    epoch: None,
                    snap: None,
                    sealed: 0,
                    known: 1,
                    journal: Vec::new(),
                    copy: Vec::new(),
                    kept: Vec::new(),
                    retired: false,
                    seen: expires,
                    obs: None,
                    pending_key: None,
                    rec: None,
                    mem_rec: None,
                    ask: None,
                })
                .collect();
            out.push(State {
                t: 0,
                off,
                reg,
                log: vec![Entry::Marker(1)],
                hb,
                hb_key,
                nodes,
                epochs: Vec::new(),
                crashes: 0,
                history: Vec::new(),
                answered: 0,
                saw: 0,
                retires: 0,
                lost: 0,
                dropped: 0,
                msgs: Vec::new(),
                asks: 0,
            });
        }
        out
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        let n = self.nodes as usize;
        for i in 0..n {
            let node = &s.nodes[i];
            let iu = i as u8;
            if node.pending.is_some() {
                // The PUT lands whether or not its issuer still lives.
                actions.push(Action::Land(iu));
            }
            if !node.alive {
                if self.restart && !node.retired {
                    actions.push(Action::Restart(iu));
                }
                continue;
            }
            if s.crashes < self.max_crashes {
                actions.push(Action::Crash(iu));
            }
            if node.lease.is_some() && (node.applied as usize) < s.log.len() {
                actions.push(Action::Tail(iu));
            }
            let in_epoch = node.epoch.is_some();
            // Promises.
            let observed = !self.on_demand
                || (0..n).any(|j| s.nodes[j].alive && s.local(j) >= s.nodes[j].seen);
            if self.on_demand && node.seen != s.reg.expires {
                actions.push(Action::Observe(iu));
            }
            // `Anchored`: only for a key this node has observed for a lease TTL
            // (its own clock measuring a duration), the register's present
            // one, and not its own tenure's.
            let anchored_ok = self.promise_rule == PromiseRule::Expiry
                || (s.reg.holder != iu && s.observation_passed(self, i));
            if self.promises()
                && observed
                && anchored_ok
                && node.pending.is_none()
                && (!in_epoch || !self.silent_in_epoch)
                && s.local(i) + self.promise_ttl > node.promise
            {
                actions.push(Action::Issue(iu));
            }
            // A node reads the register when it has a reason: a takeover to
            // consider, or a promise to anchor.
            if (self.lease_claim == crate::clk::ClaimRule::Observed
                || self.promise_rule == PromiseRule::Anchored)
                && !in_epoch
                && s.reg.holder != iu
                && node.obs.map(|(v, _)| v) != Some(s.reg.version)
            {
                actions.push(Action::ObserveKey(iu));
            }
            // Ops.
            if s.history.len() < self.ops.len() && s.can_ack(self, i) {
                let fenced = !s.epoch_holder(i) && (node.applied as usize) < s.log.len();
                if !fenced {
                    actions.push(Action::Op(iu));
                }
            }
            if self.max_transfers > 0 {
                self.transfer_actions(s, i, actions);
            }
            if let Some(r) = s.open_epoch_of(i) {
                let idx = node.epoch.unwrap_or(0);
                let holds = s.epoch_holder(i);
                if r.closed && !holds {
                    actions.push(Action::Learn(iu));
                }
                if holds && !r.closed {
                    actions.push(Action::Close(iu));
                    if self.hold_waits() && s.hold_answerable(i) {
                        actions.push(Action::AnswerHold(iu));
                    }
                }
                // The operator retires the epoch's carrier (FX-5 decision:
                // the carrier's retire is the one the members abandon on),
                // while it holds the hold or nobody does.
                if r.carrier == iu
                    && !r.closed
                    && s.retires < self.max_retires
                    && (holds || s.hold_owner(idx).is_none())
                {
                    actions.push(Action::Retire(iu));
                }
                let owner = s.hold_owner(idx);
                if self.hold_waits()
                    && !holds
                    && !r.closed
                    && owner.is_some_and(|h| s.nodes[h].alive && node.copy != s.nodes[h].journal)
                {
                    actions.push(Action::StreamAhead(iu));
                }
                // A member does no S3 acquisition of any kind.
                continue;
            }
            // Renewal: the register still names this tenure.
            if let Some(l) = node.lease {
                if s.reg.holder == iu
                    && s.reg.version == l.version
                    && s.local(i) + self.lease_ttl > s.reg.expires
                {
                    actions.push(Action::Renew(iu));
                }
            }
            if s.reg.holder != iu && s.reg.epoch < self.max_reg_epoch {
                let expired = s.claimable_by(self, i);
                if self.promises() && self.takeover_promise_check {
                    let may_read = expired || !self.read_after_expiry;
                    if may_read
                        && node.snap != Some(s.reg.version)
                        && s.unexpired_others(self, i) >= self.f()
                    {
                        actions.push(Action::ReadHb(iu));
                    }
                    if expired && node.snap == Some(s.reg.version) {
                        actions.push(Action::Takeover(iu));
                    }
                } else if expired {
                    actions.push(Action::Takeover(iu));
                }
                if self.fast_takeover {
                    match s.reg.policy {
                        Policy::Backup(b) if b == iu => actions.push(Action::Seal(iu)),
                        Policy::S3 => actions.push(Action::FastS3(iu)),
                        _ => {}
                    }
                }
            }
        }
        // FX-5: deliveries (any order; a dead recipient's wait: a pause).
        for (k, msg) in s.msgs.iter().enumerate() {
            if s.nodes[msg.to() as usize].alive {
                actions.push(Action::Deliver(k as u8, false));
                if self.dup_msgs {
                    actions.push(Action::Deliver(k as u8, true));
                }
            }
        }
        // Formation.
        if (s.epochs.len() as u8) < self.max_epochs {
            for mask in 1u8..(1 << n) {
                if self.formable(s, mask).is_some() {
                    actions.push(Action::Form(mask));
                }
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Tick => s.t += 1,
            Action::Issue(i) => {
                let i = i as usize;
                let u = s.local(i) + self.promise_ttl;
                if self.persist_before_publish {
                    s.nodes[i].promise = u;
                }
                s.nodes[i].pending = Some(u);
                s.nodes[i].pending_key = (self.promise_rule == PromiseRule::Anchored)
                    .then(|| s.nodes[i].obs.map(|(v, _)| v))
                    .flatten();
                if !self.inflight_puts {
                    return self.next_state(&s, Action::Land(i as u8));
                }
            }
            Action::Land(i) => {
                let i = i as usize;
                let u = s.nodes[i].pending.take()?;
                s.hb[i] = u;
                s.hb_key[i] = s.nodes[i].pending_key.take();
                if !self.persist_before_publish && s.nodes[i].alive {
                    // Persist after the PUT acknowledged.
                    s.nodes[i].promise = s.nodes[i].promise.max(u);
                }
            }
            Action::Renew(i) => {
                let i = i as usize;
                let expires = s.local(i) + self.lease_ttl;
                s.reg.expires = expires;
                s.reg.version += 1;
                let l = s.nodes[i].lease.as_mut()?;
                l.expires = expires;
                l.version = s.reg.version;
            }
            Action::ReadHb(i) => {
                s.nodes[i as usize].snap = Some(s.reg.version);
            }
            Action::Takeover(i) => {
                let i = i as usize;
                if self.promises() && self.takeover_promise_check {
                    s.saw |= SAW_CHECKED_TAKEOVER;
                }
                s.takeover(self, i);
            }
            Action::Seal(b) => {
                let b = b as usize;
                s.nodes[b].sealed = s.reg.epoch;
                s.takeover(self, b);
                s.saw |= SAW_SEAL;
            }
            Action::FastS3(i) => {
                s.takeover(self, i as usize);
                s.saw |= SAW_FAST_S3;
            }
            Action::Form(mask) => {
                let h = self.formable(&s, mask)?;
                let l = s.nodes[h].lease?;
                let idx = s.epochs.len() as u8;
                s.epochs.push(EpochRec {
                    members: mask,
                    carrier: h as u8,
                    version: l.version,
                    reg_epoch: l.epoch,
                    closed: false,
                });
                for j in 0..self.nodes as usize {
                    if mask & (1 << j) != 0 {
                        s.nodes[j].epoch = Some(idx);
                        s.nodes[j].lease = None;
                        s.nodes[j].snap = None;
                    }
                }
                // The carrier's hold at transfer 0 (persisted at the
                // adoption; a restart re-adopts it).
                s.nodes[h].rec = Some((idx, HoldRec::Held { n: 0 }));
                if mask.count_ones() < self.nodes as u32 {
                    s.saw |= SAW_MISSING_EPOCH;
                }
            }
            Action::Close(h) => {
                let h = h as usize;
                let idx = s.nodes[h].epoch? as usize;
                let rec = s.epochs[idx].clone();
                let journal = std::mem::take(&mut s.nodes[h].journal);
                s.epochs[idx].closed = true;
                s.nodes[h].epoch = None;
                let n = s.highest(h, idx as u8);
                s.nodes[h].rec = Some((idx as u8, HoldRec::Ended { n }));
                s.nodes[h].mem_rec = None;
                if h != rec.carrier as usize
                    && s.reg.version == rec.version
                    && s.reg.holder == rec.carrier
                {
                    // FX-5: a transferee's flush CASes the carried object
                    // (exempt from the promise check: it is exactly the
                    // carried one) and takes it over at the next epoch.
                    s.saw |= SAW_TRANSFEREE_FLUSH;
                    let epoch = s.reg.epoch + 1;
                    s.log.push(Entry::Marker(epoch));
                    for k in journal {
                        s.log.push(Entry::Op(k));
                        s.answered |= 1 << k;
                        s.saw |= SAW_FLUSH;
                    }
                    let expires = s.local(h) + self.lease_ttl;
                    s.reg = Reg {
                        holder: h as u8,
                        epoch,
                        expires,
                        version: s.reg.version + 1,
                        policy: Policy::Local,
                    };
                    s.nodes[h].applied = s.log.len() as u8;
                    s.nodes[h].known = epoch;
                    s.nodes[h].lease = Some(LeaseView {
                        epoch,
                        expires,
                        version: s.reg.version,
                        policy: Policy::Local,
                    });
                } else if s.reg.version == rec.version && s.reg.holder == h as u8 {
                    // Re-claim the own register (no promise check: it
                    // names this node) and ship the journal.
                    s.nodes[h].applied = s.log.len() as u8;
                    if !journal.is_empty() {
                        s.saw |= SAW_FLUSH;
                    }
                    for k in journal {
                        s.log.push(Entry::Op(k));
                        s.answered |= 1 << k;
                    }
                    let expires = s.local(h) + self.lease_ttl;
                    s.reg.expires = expires;
                    s.reg.version += 1;
                    s.reg.policy = Policy::Local;
                    s.nodes[h].applied = s.log.len() as u8;
                    s.nodes[h].lease = Some(LeaseView {
                        epoch: s.reg.epoch,
                        expires,
                        version: s.reg.version,
                        policy: Policy::Local,
                    });
                } else {
                    // The register moved under the epoch: its journal is
                    // a conflicting branch (replayed by rid at best).
                    s.saw |= SAW_CONFLICT | SAW_HOLD_OWNER_DEPOSED;
                }
            }
            Action::Retire(h) => {
                let h = h as usize;
                let idx = s.nodes[h].epoch? as usize;
                let rec = s.epochs[idx].clone();
                let final_hold = s.hold_final(h);
                let mut survivors = 0u16;
                for j in 0..self.nodes as usize {
                    if j != h && s.nodes[j].epoch == Some(idx as u8) && !self.retire_drops_acked {
                        for k in &s.nodes[j].copy {
                            survivors |= 1 << k;
                        }
                    }
                }
                for k in s.nodes[h].journal.clone() {
                    let bit = 1u16 << k;
                    if survivors & bit != 0 {
                        continue;
                    }
                    if s.answered & bit == 0 {
                        s.lost |= bit;
                    } else if final_hold {
                        // `Final`: the owner's disk is the only copy, by the rule.
                        s.lost |= bit;
                        s.saw |= SAW_RETIRE_FINAL_LOSS;
                    } else {
                        s.dropped |= bit;
                    }
                }
                s.epochs[idx].closed = true;
                for j in 0..self.nodes as usize {
                    if s.nodes[j].epoch != Some(idx as u8) {
                        continue;
                    }
                    s.nodes[j].epoch = None;
                    if j == h {
                        let n = s.highest(j, idx as u8);
                        s.nodes[j].rec = Some((idx as u8, HoldRec::Ended { n }));
                        s.nodes[j].mem_rec = None;
                        s.nodes[j].ask = None;
                        s.nodes[j].journal.clear();
                        s.nodes[j].alive = false;
                        s.nodes[j].retired = true;
                        s.nodes[j].lease = None;
                        s.nodes[j].snap = None;
                        s.nodes[j].pending = None;
                    } else if !self.retire_drops_acked {
                        let copy = std::mem::take(&mut s.nodes[j].copy);
                        for k in copy {
                            if !s.nodes[j].kept.contains(&k) {
                                s.nodes[j].kept.push(k);
                            }
                        }
                    }
                    s.nodes[j].copy.clear();
                }
                if s.reg.version == rec.version && s.reg.holder == h as u8 {
                    // The fence: the carrier's lease is claimable now.
                    s.reg.expires = PAST;
                    s.reg.version += 1;
                }
                s.retires += 1;
                s.saw |= SAW_RETIRE;
            }
            Action::Observe(i) => {
                let e = s.reg.expires;
                s.nodes[i as usize].seen = e;
            }
            Action::ObserveKey(i) => {
                let i = i as usize;
                let version = s.reg.version;
                if s.nodes[i].obs.map(|(v, _)| v) != Some(version) {
                    s.nodes[i].obs = Some((version, s.local(i)));
                }
            }
            Action::Learn(m) => {
                s.nodes[m as usize].epoch = None;
                s.nodes[m as usize].copy.clear();
            }
            Action::Op(i) => {
                let i = i as usize;
                let k = s.history.len();
                let op = self.ops[k];
                if s.epoch_holder(i) {
                    let mut dir = s.log_dir(s.nodes[i].applied as usize);
                    for j in &s.nodes[i].journal {
                        dir = namespace::force_apply(dir, s.history[*j as usize].0);
                    }
                    let (ret, _) = namespace::eval(dir, op);
                    s.history.push((op, ret));
                    s.nodes[i].journal.push(k as u8);
                    // Plan 46 C10: a one-member hold is `Final` (answered
                    // from the owner's disk); otherwise the answer waits
                    // for a member's `StreamAheadAck`, unless the knob.
                    if s.hold_final(i) {
                        s.answered |= 1 << k;
                        s.saw |= SAW_HOLD_FINAL_ANSWER;
                    } else if !self.hold_waits() {
                        s.answered |= 1 << k;
                    }
                } else {
                    let dir = s.log_dir(s.log.len());
                    let (ret, _) = namespace::eval(dir, op);
                    s.history.push((op, ret));
                    s.log.push(Entry::Op(k as u8));
                    s.nodes[i].applied = s.log.len() as u8;
                    s.answered |= 1 << k;
                }
            }
            Action::StreamAhead(m) => {
                let m = m as usize;
                let h = s.hold_owner(s.nodes[m].epoch?)?;
                s.nodes[m].copy = s.nodes[h].journal.clone();
            }
            Action::AnswerHold(h) => {
                let h = h as usize;
                let held = s.member_held(h);
                let before = s.answered;
                for k in s.nodes[h].journal.clone() {
                    if held & (1 << k) != 0 {
                        s.answered |= 1 << k;
                    }
                }
                if s.answered == before {
                    return None;
                }
                s.saw |= SAW_HOLD_MEMBER_ANSWER;
            }
            Action::Tail(i) => {
                let i = i as usize;
                while (s.nodes[i].applied as usize) < s.log.len() {
                    let e = s.log[s.nodes[i].applied as usize];
                    s.nodes[i].applied += 1;
                    if let Entry::Marker(e) = e {
                        s.nodes[i].known = s.nodes[i].known.max(e);
                        if s.nodes[i].lease.is_some_and(|l| l.epoch < e) {
                            s.nodes[i].lease = None;
                        }
                    }
                }
            }
            Action::Crash(i) => {
                let i = i as usize;
                s.nodes[i].alive = false;
                s.nodes[i].lease = None;
                s.nodes[i].snap = None;
                // An observation is volatile: a restarted process is a cold
                // claimant.
                s.nodes[i].obs = None;
                // FX-5: the acquisition and an unpersisted record go too.
                s.nodes[i].ask = None;
                s.nodes[i].mem_rec = None;
                s.crashes += 1;
            }
            Action::Ask(r, g) => {
                if self.free_asks.is_some() {
                    s.asks += 1;
                }
                s.nodes[r as usize].ask = Some(g);
                s.send(Msg::Request { from: r, to: g });
            }
            Action::AskTimeout(r) => {
                s.nodes[r as usize].ask = None;
            }
            Action::Nudge(g) => {
                let (key, to, n) = s.nudge_guard(self, g as usize)?;
                s.send(Msg::Nudge {
                    from: g,
                    to,
                    key,
                    n,
                });
            }
            Action::Deliver(k, keep) => {
                let msg = *s.msgs.get(k as usize)?;
                if !keep {
                    s.msgs.remove(k as usize);
                }
                self.deliver(&mut s, msg);
            }
            Action::Restart(i) => {
                s.nodes[i as usize].alive = true;
            }
        }
        s.normalize(self);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("single_authority", |m: &FlexEpochs, s: &State| {
                s.authorities(m).len() <= 1
            }),
            Property::always("linearizable", |_, s: &State| {
                let mut spec = NamespaceSpec::default();
                s.history
                    .iter()
                    .enumerate()
                    .filter(|(k, _)| (s.lost | s.dropped) & (1 << k) == 0)
                    .all(|(_, (op, ret))| spec.invoke(op) == *ret)
            }),
            Property::always("converged_at_quiescence", |_, s: &State| {
                s.epochs.iter().any(|r| !r.closed) || s.all_ops_in_log()
            }),
            Property::sometimes("all_ops_done", |m: &FlexEpochs, s: &State| {
                s.history.len() == m.ops.len()
            }),
            Property::sometimes("epoch_flushed", |_, s: &State| s.saw & SAW_FLUSH != 0),
        ];
        // Plan 43 C0 (§11 Q3, decided): an epoch's hold owner is not
        // stealable. Its epoch journal is dropped only by an operator's
        // retire ([`Action::Retire`], plan 46 M1.4 step 4; off unless
        // `max_retires` > 0), or when its epoch carried no lease (every
        // epoch is formed over a lease: `Form` converts a member's lease):
        // the register never moves under a hold owner. A retire is not a
        // deposition (it sets no `SAW_HOLD_OWNER_DEPOSED`); what it loses is
        // judged by `hold_answered_rows_survive_retire`.
        if self.check_hold_owner {
            props.push(Property::always(
                "hold_owner_never_deposed_except_by_retire",
                |_, s: &State| s.saw & SAW_HOLD_OWNER_DEPOSED == 0,
            ));
        }
        // Plan 46 M1.0 C10 (§5.7): the hold is a group {owner, one member}.
        // A row the hold answered while its epoch is open is committed:
        // a member (other than the owner) holds it (`StreamAheadAck`), or
        // the epoch has one member and the hold is `Final`. (Once the
        // epoch closes the journal is in the log, the archive's rule.)
        if self.check_hold_answers {
            props.push(Property::always(
                "hold_answer_committed_or_final",
                |_, s: &State| {
                    (0..s.nodes.len()).all(|i| {
                        if !s.epoch_holder(i) || s.hold_final(i) {
                            return true;
                        }
                        let held = s.member_held(i);
                        s.nodes[i]
                            .journal
                            .iter()
                            .all(|k| s.answered & (1 << k) == 0 || held & (1 << k) != 0)
                    })
                },
            ));
            props.push(Property::sometimes(
                "hold_answered_by_member",
                |_, s: &State| s.saw & SAW_HOLD_MEMBER_ANSWER != 0,
            ));
            if self.min_members == 1 {
                props.push(Property::sometimes("hold_final_answer", |_, s: &State| {
                    s.saw & SAW_HOLD_FINAL_ANSWER != 0
                }));
            }
        }
        // Plan 46 M1.4 step 4 (Q4): the operator's retire of a hold's owner
        // takes its epoch journal, but not what a member acknowledged: the
        // member keeps it as committed and ships it when it claims.
        if self.max_retires > 0 {
            props.push(Property::always(
                "hold_answered_rows_survive_retire",
                |_, s: &State| s.answered_rows_held(),
            ));
            props.push(Property::sometimes("retire_happens", |_, s: &State| {
                s.saw & SAW_RETIRE != 0
            }));
            props.push(Property::sometimes(
                "retired_rows_carried_by_a_claim",
                |_, s: &State| s.saw & SAW_RETIRE_CARRIED != 0,
            ));
        }
        // FX-5: a hold in flight is recoverable from persisted state alone.
        if self.max_transfers > 0 {
            props.push(Property::always(
                "hold_recoverable",
                |m: &FlexEpochs, s: &State| s.hold_recoverable(m),
            ));
            props.push(Property::sometimes("transfer_taken", |_, s: &State| {
                s.saw & SAW_TRANSFER_TAKEN != 0
            }));
            props.push(Property::sometimes("reoffer_taken", |_, s: &State| {
                s.saw & SAW_REOFFER_TAKEN != 0
            }));
            props.push(Property::sometimes("transferee_flush", |_, s: &State| {
                s.saw & SAW_TRANSFEREE_FLUSH != 0
            }));
            props.push(Property::sometimes("nudge_ask", |_, s: &State| {
                s.saw & SAW_NUDGE_ASK != 0
            }));
            if self.max_transfers >= 3 {
                props.push(Property::sometimes("third_transfer", |_, s: &State| {
                    s.saw & SAW_THIRD_TRANSFER != 0
                }));
            }
        }
        if self.min_members < self.nodes {
            props.push(Property::sometimes(
                "epoch_with_missing_node",
                |_, s: &State| s.saw & SAW_MISSING_EPOCH != 0,
            ));
        }
        if self.promises() && self.takeover_promise_check {
            props.push(Property::sometimes("checked_takeover", |_, s: &State| {
                s.saw & SAW_CHECKED_TAKEOVER != 0
            }));
        }
        if self.fast_takeover {
            match self.policy {
                Policy::Backup(_) => props
                    .push(Property::sometimes("seal_takeover", |_, s: &State| {
                        s.saw & SAW_SEAL != 0
                    })),
                Policy::S3 => props
                    .push(Property::sometimes("fast_s3_takeover", |_, s: &State| {
                        s.saw & SAW_FAST_S3 != 0
                    })),
                Policy::Local => {}
            }
        }
        props
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick
    }
}

impl FlexEpochs {
    /// FX-5: `msg` reaches its (live) recipient.
    fn deliver(&self, s: &mut State, msg: Msg) {
        match msg {
            Msg::Request { from: r, to: g } => {
                let gu = g as usize;
                if let Some((key, n)) = s.give_guard(self, gu, r) {
                    // The give: the record is durable before the answer
                    // leaves (unless the knob), the hold let go with it.
                    let given = Some((key, HoldRec::Given { n, to: r }));
                    if self.record_not_persisted {
                        s.nodes[gu].mem_rec = given;
                    } else {
                        s.nodes[gu].rec = given;
                    }
                    s.send(Msg::Offer {
                        from: g,
                        to: r,
                        key,
                        n,
                        re: false,
                    });
                } else if let Some((key, n)) = s.reoffer_guard(self, gu, r) {
                    s.send(Msg::Offer {
                        from: g,
                        to: r,
                        key,
                        n,
                        re: true,
                    });
                }
                // Otherwise a decline: the asker's phase ends by its
                // timeout (`AskTimeout`), the same in the model.
            }
            Msg::Offer {
                from: g,
                to: r,
                key,
                n,
                re,
            } => {
                let ru = r as usize;
                if s.nodes[ru].ask != Some(g) {
                    // No live phase: the answer is dropped (today's 797).
                    return;
                }
                s.nodes[ru].ask = None;
                if s.take_guard(self, ru, g, key, n, re) {
                    // Persisted before the hold is used (durable before act).
                    s.nodes[ru].rec = Some((key, HoldRec::Held { n }));
                    s.nodes[ru].mem_rec = None;
                    s.saw |= if re {
                        SAW_REOFFER_TAKEN
                    } else {
                        SAW_TRANSFER_TAKEN
                    };
                    if n == 3 {
                        s.saw |= SAW_THIRD_TRANSFER;
                    }
                }
            }
            Msg::Nudge {
                from: g,
                to: r,
                key,
                n,
            } => {
                let ru = r as usize;
                // Stateless: an idle member of the open epoch, past nothing,
                // starts an acquisition aimed at the giver.
                if s.nodes[ru].ask.is_none()
                    && !s.epoch_holder(ru)
                    && s.in_open_epoch(ru, key)
                    && n > s.highest(ru, key)
                {
                    s.nodes[ru].ask = Some(g);
                    s.send(Msg::Request { from: r, to: g });
                    s.saw |= SAW_NUDGE_ASK;
                }
            }
        }
    }

    /// Whether `mask` may form an epoch now; the member whose lease it
    /// would hold.
    fn formable(&self, s: &State, mask: u8) -> Option<usize> {
        if (mask.count_ones() as u8) < self.min_members {
            return None;
        }
        let members: Vec<usize> = (0..self.nodes as usize)
            .filter(|j| mask & (1 << j) != 0)
            .collect();
        for &j in &members {
            let n = &s.nodes[j];
            if !n.alive || n.epoch.is_some() {
                return None;
            }
            if self.promises() && self.join_after_own_promise && s.local(j) < n.promise {
                return None;
            }
        }
        let known = members.iter().map(|j| s.nodes[*j].known).max()?;
        let claims = members
            .into_iter()
            .filter_map(|h| s.s3_usable(self, h).map(|l| (l, h)));
        let (l, h) = if self.resolve_by_epoch {
            // The claim at the highest lease epoch wins, and only if no
            // member knows of a later epoch (a tenure it took, a marker it
            // tailed, an epoch it sealed): a lower claim is a holder that
            // was taken over and has not learned it yet.
            let (l, h) = claims.max_by_key(|(l, _)| l.epoch)?;
            if l.epoch < known {
                return None;
            }
            (l, h)
        } else {
            claims.into_iter().next()?
        };
        let claimable = !self.claim_rule
            || match l.policy {
                Policy::Local => true,
                Policy::Backup(b) => mask & (1 << b) != 0,
                Policy::S3 => false,
            };
        claimable.then_some(h)
    }
}
