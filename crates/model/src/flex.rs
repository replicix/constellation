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
//! 2. No P2P handoff inside an epoch: it moves the hold between members
//!    of the same epoch, never outside it.
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
    /// `on_demand`: the register's expiry as this node last read it.
    pub seen: i16,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EpochRec {
    pub members: u8,
    pub holder: u8,
    /// The register version and epoch the holder's lease had.
    pub version: u8,
    pub reg_epoch: u8,
    pub closed: bool,
}

pub const SAW_MISSING_EPOCH: u8 = 1;
pub const SAW_CHECKED_TAKEOVER: u8 = 2;
pub const SAW_FLUSH: u8 = 4;
pub const SAW_SEAL: u8 = 8;
pub const SAW_FAST_S3: u8 = 16;
pub const SAW_CONFLICT: u8 = 32;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: i16,
    pub off: Vec<i16>,
    pub reg: Reg,
    pub log: Vec<Entry>,
    /// `heartbeat/<node>`: `no_epoch_until` (in the publisher's clock).
    pub hb: Vec<i16>,
    pub nodes: Vec<Node>,
    pub epochs: Vec<EpochRec>,
    pub crashes: u8,
    /// Executed ops: `(op, return)`, in real-time order.
    pub history: Vec<(NsOp, NsRet)>,
    pub saw: u8,
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

    /// The node is the holder of an epoch that has not closed.
    fn epoch_holder(&self, i: usize) -> bool {
        self.open_epoch_of(i)
            .is_some_and(|r| r.holder == i as u8 && !r.closed)
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
        if n.epoch.is_some() {
            // A member: only the holder of an open epoch has authority.
            return self.epoch_holder(i);
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
        let now = if m.compare_to_expiry {
            self.reg.expires
        } else {
            self.local(i)
        };
        (0..self.nodes.len())
            .filter(|j| *j != i && self.hb[*j] > now)
            .count() as u8
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
        }
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
        }
        for r in self.epochs.iter_mut() {
            r.version = rank(r.version);
        }
    }

    fn all_ops_in_log(&self) -> bool {
        (0..self.history.len()).all(|k| self.log.contains(&Entry::Op(k as u8)))
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
    /// `on_demand`: read the register (its expiry).
    Observe(u8),
    /// Tail to head.
    Tail(u8),
    Crash(u8),
    Restart(u8),
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
                    seen: expires,
                })
                .collect();
            out.push(State {
                t: 0,
                off,
                reg,
                log: vec![Entry::Marker(1)],
                hb,
                nodes,
                epochs: Vec::new(),
                crashes: 0,
                history: Vec::new(),
                saw: 0,
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
                if self.restart {
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
            if self.promises()
                && observed
                && node.pending.is_none()
                && (!in_epoch || !self.silent_in_epoch)
                && s.local(i) + self.promise_ttl > node.promise
            {
                actions.push(Action::Issue(iu));
            }
            // Ops.
            if s.history.len() < self.ops.len() && s.can_ack(self, i) {
                let fenced = !s.epoch_holder(i) && (node.applied as usize) < s.log.len();
                if !fenced {
                    actions.push(Action::Op(iu));
                }
            }
            if let Some(r) = s.open_epoch_of(i) {
                if r.closed && r.holder != iu {
                    actions.push(Action::Learn(iu));
                }
                if r.holder == iu && !r.closed {
                    actions.push(Action::Close(iu));
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
                let expired = s.local(i) >= s.reg.expires;
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
                if !self.inflight_puts {
                    return self.next_state(&s, Action::Land(i as u8));
                }
            }
            Action::Land(i) => {
                let i = i as usize;
                let u = s.nodes[i].pending.take()?;
                s.hb[i] = u;
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
                    holder: h as u8,
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
                if s.reg.version == rec.version && s.reg.holder == h as u8 {
                    // Re-claim the own register (no promise check: it
                    // names this node) and ship the journal.
                    s.nodes[h].applied = s.log.len() as u8;
                    if !journal.is_empty() {
                        s.saw |= SAW_FLUSH;
                    }
                    for k in journal {
                        s.log.push(Entry::Op(k));
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
                    s.saw |= SAW_CONFLICT;
                }
            }
            Action::Observe(i) => {
                let e = s.reg.expires;
                s.nodes[i as usize].seen = e;
            }
            Action::Learn(m) => {
                s.nodes[m as usize].epoch = None;
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
                } else {
                    let dir = s.log_dir(s.log.len());
                    let (ret, _) = namespace::eval(dir, op);
                    s.history.push((op, ret));
                    s.log.push(Entry::Op(k as u8));
                    s.nodes[i].applied = s.log.len() as u8;
                }
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
                s.crashes += 1;
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
                s.history.iter().all(|(op, ret)| spec.invoke(op) == *ret)
            }),
            Property::always("converged_at_quiescence", |_, s: &State| {
                s.epochs.iter().any(|r| !r.closed) || s.all_ops_in_log()
            }),
            Property::sometimes("all_ops_done", |m: &FlexEpochs, s: &State| {
                s.history.len() == m.ops.len()
            }),
            Property::sometimes("epoch_flushed", |_, s: &State| s.saw & SAW_FLUSH != 0),
        ];
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
