//! Plan 30 §M14: strict mode — cross-node `flock`/`fcntl` as leased,
//! recallable, per-node file-lock grants at the owning sequencer.
//!
//! The tables live in `Meta` (`constellation_meta::locks`, shared with
//! the FUSE threads); this module is the protocol around them, on both
//! sides of every exchange. `crates/model/src/locks.rs` is the model of
//! it; its module doc has the argument, and its action → code table
//! names the functions here.
//!
//! # Owner side (the root holder, or an M11 delegate for its subtree)
//!
//! - [`Core::on_lock_request`]: route (`NotOwner` for another owner's
//!   subtree), the M9 liveness and marking guards (a tenure that grants
//!   locks is one its fast successor waits out), the grace (M9's
//!   acknowledgement floor, the restart quarantine, or the subtree grace
//!   after an outwaited delegate), then grant, or recall the conflicting
//!   grants and park the request (a waiter, FIFO per inode).
//! - A recalled node releases (`LockReleased`) or is outwaited by the
//!   grant's expiry (`Timer::LockGrantExpiry`); either serves the
//!   waiters. A waiter whose request was answered `Waiting` (so the RPC
//!   returns within `recall_hold_ms`) gets its grant pushed
//!   (`LockGranted`) or re-attaches with its next request.
//! - [`Core::on_lock_renew`]: extend, or *reclaim* an unknown grant
//!   during a grace period, else `Lost`. The acknowledgement carries the
//!   recalled flag, so a lost recall is repaired by the next renewal.
//! - The table follows the subtree: handed to a delegate with its first
//!   granting `DelegRenewed`, back with `DelegRecalled`. The root keeps
//!   its copy of what it handed ([`LockState::handed`]): every granting
//!   renewal re-sends the live copies (the reply can be lost), and a
//!   generation that ends without handing one back — its recall
//!   overtook the reply — leaves it reinstated in the root's table,
//!   where the subtree's next delegation takes it along. A delegate
//!   outwaited by TTL also leaves a grace on its subtree. An
//!   asynchronous mirror rides to the backups (`LockMirror`); a fast
//!   successor installs it, restamped, next to the floor.
//!
//! # Node side
//!
//! - [`Core::on_lock_control`]: the FUSE thread's request for a grant
//!   (`Control::Lock`), answered `Granted { position }` (the FUSE thread
//!   then session-waits and invalidates the kernel's cache of the file,
//!   which is what makes writes under the previous holder's lock visible
//!   under this one), `WouldBlock`, or `Unavailable` (no P2P path: the
//!   inbox is not a lock path, see PROGRESS.md).
//! - Renewals ride `Timer::LockRenewTick` while any grant is held,
//!   grouped by owner; a lapsed grant is never renewed (the FUSE thread
//!   fences I/O under it: `EIO`).
//! - A recall of a grant with local locks under it is honoured when the
//!   last one leaves (`Control::LockIdle`); one without waits for the
//!   flush of the file's dirty data (`Action::LockFlush` →
//!   `Event::LockFlushed`) before `LockReleased`.

use super::{Core, Timer};
use crate::action::{Action, ControlOk, LockAnswer, LockTestAnswer, S3Op};
use crate::event::{LockOutcome, LockRenewEntry, LockRenewResult, LockTestOutcome, PeerMsg};
use crate::ids::{Ms, NodeId, OpId, TimerId};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::locks::{Grant, GrantId, HeldGrant, Installed, LockMode};
use constellation_meta::Position;
use std::collections::BTreeMap;

/// A client-side lock request in progress.
#[derive(Debug)]
struct LockOp {
    ino: Ino,
    mode: LockMode,
    blocking: bool,
    /// The peer request in flight, if any.
    req: Option<OpId>,
    owner: NodeId,
    /// When the current request was sent (the grant's lifetime counts
    /// from here).
    sent_at: Ms,
    attempts: u32,
    since: Ms,
    /// Waiting in this node's own waiter list (the owner is here).
    local_wait: bool,
    /// The lease was re-read once because the cached holder could not be
    /// reached (see `lock_route_op`).
    reread: bool,
}

/// How often a release held back for tagged mutations in flight looks
/// again (plan 30 §M14 phase 2). Only a fallback: the FUSE thread that
/// ends the last of them wakes the release (`Control::LockReleaseWake`).
const RELEASE_WAIT_POLL_MS: i64 = 250;

/// How many grants in a row a peer may leave unused (outwaited) before
/// the owner takes it for unreachable ([`LockState::unreachable`]). One
/// can be a race (the push found no op: it had just been granted over
/// its own request), two in a row are not.
const UNUSED_GRANTS_UNREACHABLE: u32 = 2;

/// How many `LockState::done_reqs` entries are kept.
const DONE_REQS_KEPT: usize = 64;

/// Owner side: a blocking request waiting for recalls.
#[derive(Debug)]
struct Waiter {
    id: u64,
    node: NodeId,
    ino: Ino,
    mode: LockMode,
    /// The RPC to answer, or none (answered `Waiting`; the grant is
    /// pushed, or the next request re-attaches).
    req: Option<OpId>,
    /// The requester's clock when it sent its request (echoed with a
    /// push: the grant's window counts from there, never later).
    sent: Ms,
    /// This owner's clock when that request (the park, or the latest
    /// re-send) arrived: a grant served to a remote waiter is live from
    /// here, not from when it is served. The requester's window counts
    /// from `sent`, which was before, so the owner's record still
    /// outlasts it by `2 × margin` — and a requester that died while
    /// parked costs at most `ttl + margin` from its last message, never
    /// that plus however long it sat in the queue (the EC2 lock run,
    /// PROGRESS.md "Fix: M14 follow-ups").
    recv: Ms,
    /// This node's own request (its `LockOp`).
    op: Option<OpId>,
    held_timer: Option<TimerId>,
    since: Ms,
}

#[derive(Debug)]
struct Recalling {
    ino: Ino,
    req: Option<OpId>,
    timer: TimerId,
}

#[derive(Debug)]
struct RenewInFlight {
    entries: Vec<(Ino, GrantId)>,
    sent: Ms,
    timer: TimerId,
    /// The owner it went to.
    to: NodeId,
}

/// A renewal whose answer `on_lock_renew_timeout` stopped waiting for:
/// the owner it went to and this node's clock at the send. The timeout
/// lets the next tick renew again (a lost request is re-sent at once);
/// it says nothing about the answer to this one, which an owner whose
/// requests queue behind seconds of other work gives late. Thrown away,
/// as they were, every renewal answered later than the timeout (500 ms)
/// was wasted: under `stress-ng-fs-nodes` an owner's answers all came
/// late, no renewal ever counted, and the grant lapsed under the
/// holder's writes (`EIO`) although the owner had renewed it each time.
/// A late grant is honoured from its own send, as one in time would be.
#[derive(Debug, Clone, Copy)]
struct LateRenew {
    to: NodeId,
    sent: Ms,
}

/// How long a late renewal answer is still used ([`LateRenew`]).
const LATE_RENEW_MS: i64 = 60_000;

#[derive(Debug, Default)]
pub(crate) struct LockState {
    // ---- node side ----
    ops: BTreeMap<OpId, LockOp>,
    by_req: BTreeMap<OpId, OpId>,
    /// `getlk` probes in flight (control op → peer request).
    tests: BTreeMap<OpId, OpId>,
    test_by_req: BTreeMap<OpId, OpId>,
    renews: BTreeMap<OpId, RenewInFlight>,
    late_renews: BTreeMap<OpId, LateRenew>,
    renew_timer: Option<TimerId>,
    /// When `renew_timer` fires.
    renew_at: Option<Ms>,
    /// Delegate: generations whose renewal a lock renewal asked for
    /// (too little of the delegation left to extend the grant by much).
    deleg_renew_wanted: std::collections::BTreeSet<u64>,
    /// A lease read to relearn the owner for renewals is in flight.
    relearning: bool,
    /// Node side, per generation this node's table still delegates: the
    /// node its delegate named the owner (`NotOwner { root }`) once a
    /// recall had taken the subtree's grants back to that root
    /// (`DelegationState::handed_back`). Requests and renewals for the
    /// subtree go there until the `Recall` record reaches this table —
    /// which under an S3 cut is only after it (the root ends the
    /// generation in its journal at once, a continuation epoch's
    /// recall-all). Sent to the stale delegate meanwhile, they were
    /// answered `NotOwner { 0 }` for the whole cut, and the grants the
    /// root renews lapsed under their holders' I/O
    /// (`locks-blips-tight-delegated`, chunk delegate-fenced-io). Dropped
    /// when the named node answers `NotOwner` itself, or once the table
    /// no longer has the generation.
    deleg_moved: BTreeMap<u64, NodeId>,
    /// Flush attempts per inode with a release in flight.
    flushing: BTreeMap<Ino, u32>,
    /// Phase 2: inodes whose release waits for tagged mutations
    /// (`on_lock_flushed`; counted once per wait), with the timer that
    /// looks again.
    release_waiting: BTreeMap<Ino, (GrantId, crate::ids::TimerId)>,
    /// Node side: requests answered by a push while their RPC was still
    /// in flight, by that RPC's id → the inode: the RPC's own reply
    /// (the owner re-affirmed the grant under a *new* id) then installs
    /// that id instead of being dropped, so this node holds the id the
    /// owner recalls. Bounded (`DONE_REQS_KEPT`).
    done_reqs: BTreeMap<OpId, Ino>,
    // ---- owner side ----
    waiters: Vec<Waiter>,
    next_waiter: u64,
    /// Owner side: the queue position (`Waiter::since`) of the last
    /// waiter served per `(node, inode)`. A grant that goes unused — the
    /// push found no op, the reply lapsed on arrival, the requester's
    /// routing changed — is outwaited, and the node asks again: it then
    /// re-parks *at its old position*, not behind everyone who asked
    /// meanwhile (EC2 campaign 8: a committer waiting 16–28 s while the
    /// other took turn after turn). Kept `4 × ttl`.
    served: BTreeMap<(NodeId, Ino), Ms>,
    /// Owner side: per peer, how many grants served to it from the queue
    /// in a row went unused and were outwaited (no renewal, release or
    /// recall acknowledgement from it in between). At
    /// [`UNUSED_GRANTS_UNREACHABLE`] the peer is taken for unreachable.
    unused_grants: BTreeMap<NodeId, u32>,
    /// Owner side: grants served to remote waiters from the queue that
    /// their node has not used yet (renewed or released); one outwaited
    /// while still here went unused. (`served` cannot tell: a re-send of
    /// the node parks it again and takes that entry.)
    unproven: std::collections::BTreeSet<GrantId>,
    /// Owner side: peers this owner cannot reach — a recall to one failed
    /// at the transport with no connection left, or grants pushed to it
    /// kept going unused. The link can be one-way: such a peer's own
    /// requests (and their replies, on its connection) still arrive while
    /// everything this owner sends on its own — a `LockGranted` push, a
    /// `LockRecall` — is lost (`git-under-flock-causal`: a restarted
    /// reader this owner could not dial; it was pushed the turn lock,
    /// recalled, outwaited and pushed it again, for 35 minutes, while
    /// both committers waited behind it). Its waiter is granted only over
    /// a request of its own, which it is told to re-send at once; the
    /// queue waits for that request briefly, and passes the waiter over
    /// once it is silent for longer. Its kept queue position (`served`)
    /// goes.
    /// Cleared by a recall it acknowledges (this owner reached it), or by
    /// a new incarnation.
    unreachable: std::collections::BTreeSet<NodeId>,
    /// Owner side: the newest incarnation each peer's lock requests
    /// carried. A higher one drops what the previous incarnation left
    /// queued here; a request from a lower one (delayed in flight from a
    /// process that is gone) is ignored.
    incarnations: BTreeMap<NodeId, u32>,
    /// Owner side: while waiters are parked, they are re-served on a
    /// tick — a refusal for stale liveness, an unmarked lease or a grace
    /// period has no event of its own that ends it.
    waiter_tick: Option<TimerId>,
    recalls: BTreeMap<GrantId, Recalling>,
    recall_by_req: BTreeMap<OpId, GrantId>,
    /// Subtree grace after an outwaited delegate: `(dir, until)`.
    pub(crate) grace: Vec<(Ino, Ms)>,
    pub(crate) mirror_dirty: bool,
    mirror_ver: u64,
    /// This node as a backup: the holder's last mirror.
    bk_mirror: Option<(u64, Vec<Grant>)>,
    /// Root: grants to hand to a delegate with its first renewal.
    handoff: BTreeMap<u64, Vec<Grant>>,
    /// Root: the grants handed to a generation, as this root recorded
    /// them (its clock), until they would have expired here. The handoff
    /// rides a renewal *reply*, which the generation's recall can
    /// overtake (the delegate answers the recall with nothing, then
    /// drops what arrives for a generation it no longer serves) or which
    /// can be lost (the delegate renews again and serves without them).
    /// Either way the holders still honour the root's windows, so:
    /// every granting renewal re-sends the live ones (the delegate
    /// installs an id once), and a generation that ends without handing
    /// one back leaves it reinstated here — where the next delegation of
    /// the subtree takes it along. A subtree grace here instead covered
    /// only this root's own grants: the next delegate of the subtree
    /// granted over them (sim `locks-delegated` seed 196102, two
    /// exclusive holders).
    handed: BTreeMap<u64, Vec<Grant>>,
    /// Delegate: the moved grant ids a generation installed (a re-sent
    /// handoff installs each once: a grant released or replaced here
    /// since is not revived).
    moved_seen: BTreeMap<u64, std::collections::BTreeSet<GrantId>>,
    /// Owner side, per inode: what the holders that released their
    /// grants on it had seen or been acknowledged (`LockReleased`'s
    /// position, joined). Every later grant on the inode carries it, so a
    /// lock orders *all* of the previous holder's writes before the next
    /// holder's reads, not only the locked file's: an application that
    /// keeps several files consistent under one lock (git's objects and
    /// refs under an `flock` turn file, EC2 campaign 4 B-1) otherwise
    /// read the refs the previous holder had replaced — up to a ship
    /// interval stale — and committed on top of them, losing the other
    /// node's commits. In memory, and moved with the table: a subtree's
    /// floor rides its delegation's granting renewals and its recall
    /// answer, the join of all of them the backup mirror, and every new
    /// tenure floors everything with its own position first (see
    /// [`LockState::dir_floors`]).
    floors: BTreeMap<Ino, Position>,
    /// Floors for *every* inode under a directory (`ROOT_INO`: all):
    /// what an owner change leaves where the per-inode floors were lost
    /// or summarized — a subtree handed to or back from a delegate, an
    /// outwaited delegate's subtree, the predecessor's mirrored floors
    /// after a fast takeover, and this tenure's own floor after any
    /// takeover. Joined into every later grant under the directory. Few
    /// entries (past [`DIR_FLOORS_CAP`] they fold into the root's).
    dir_floors: Vec<(Ino, Position)>,
    /// Every floor noted here, joined (what the backup mirror carries).
    floor_all: Position,
    /// Root: the floor of each generation's subtree at its move, re-sent
    /// with every granting renewal (a join: resending is harmless), and
    /// raised by a release that reaches this root after the move.
    handed_floor: BTreeMap<u64, Position>,
    /// Root: subtrees of outwaited generations whose floor is noted at
    /// the next event (with this root's position then).
    pending_dir_floors: Vec<Ino>,
    /// A new tenure (start, or after the lease was gone): its first grant
    /// notes this node's whole position as a floor on everything first —
    /// the predecessor's floors are gone with it, and what it knew is in
    /// the log this tenure tailed (and, fast, its backup tail).
    tenure_floor_due: bool,
    /// Backup: the floor that came with the last mirror.
    bk_floor: Position,
    /// A new tenure: the inherited live generations whose delegate has
    /// not renewed with this root yet (`None`: not collected yet). Until
    /// every one has, this root makes no new grant: the floors the
    /// previous tenure held may name their streams past what this root
    /// was re-streamed, and a renewal carries the delegate's head.
    tenure_waiting: Option<std::collections::BTreeSet<u64>>,
    /// The heads those renewals carried, for the tenure floor.
    tenure_heads: Vec<(u64, u64)>,
    /// Generations this tenure delegated before `tenure_waiting` was
    /// collected (it is collected at the tenure's first grant, not when
    /// the tenure begins). They are not inherited: their streams start
    /// in this tenure, so no floor of a previous one can name them, and
    /// waiting for their delegate's renewal only held the root's own
    /// first grant hostage to a delegate's liveness
    /// (`lock-grant-dead-generation`: an uncontended `flock` on a root
    /// file parked behind the renewal of a delegation made a moment
    /// before; the renewal was lost, the grant waited for the delegation
    /// to be reclaimed, and by then the requester's link was down —
    /// 451 s).
    tenure_minted: std::collections::BTreeSet<u64>,
    /// Owner side, per inode: the end of the latest grant record on it
    /// that expired here unreleased (outwaited, on this clock). Its
    /// holder's release position is lost: what it was acknowledged under
    /// the grant may sit in a delegate's stream (or the root's journal)
    /// that this owner's own position does not name — a partitioned
    /// holder that is itself the delegate of the files it wrote (sim
    /// `locks-unlinked-delegated-dbackup-random` seed 276). No grant on
    /// the inode until a *cut* as of that time is noted as its floor
    /// ([`Core::lock_barrier_ready`]). The holder's fencing token is
    /// refused everywhere from its window's end, which is `2 × margin −
    /// (transit + the holder's skew)` before the record's — positive by
    /// the margin rule (`margin ≥ skew + transit`) — so a cut taken at or
    /// after the record's end covers every row executed under the grant.
    ///
    /// With it, the holder whose record it was while it is only one node's
    /// (`None`: several, or unknown — carried from a delegate). That node
    /// itself does not wait on it: what it was acknowledged under the
    /// grant is in its own position (a live holder whose renewal came late
    /// re-asks). Its *exclusive* grant ends the barrier: nobody is granted
    /// past it before its release, which carries that position, or its
    /// outwait, which sets a later barrier. A shared one does not: another
    /// node's shared grant does not wait for its release (sim
    /// `locks-unlinked-delegated-partition` seed 390 read the older turn
    /// beside it).
    barriers: BTreeMap<Ino, (i64, Option<NodeId>)>,
    /// The same for every inode under a directory (`ROOT_INO`: all): an
    /// outwaited delegation's subtree (its holders' releases went with
    /// it), a barrier carried with a subtree move, the overflow of
    /// [`LockState::barriers`].
    dir_barriers: Vec<(Ino, i64)>,
    /// Root: per live generation, the latest stream head its delegate's
    /// renewals reported and when the delegate sent it, on its clock
    /// (`DelegRenew::stream_head_at`): what a cut takes for the stream;
    /// and when the latest renewal arrived here (whether a designee is
    /// online, [`Core::lock_cut_here`]).
    heads: BTreeMap<u64, (i64, u64, Ms)>,
    /// Delegate: the latest cut a granting renewal answer carried, as
    /// `(root, as of, position)` (that root's clock; see
    /// [`Core::lock_cut_here`]). A cut from another root replaces it
    /// whatever its time: the clocks differ, and a new root's behind the
    /// old one's would settle nothing until it passed the old value.
    pub(crate) cut: Option<(NodeId, i64, Position)>,
    /// The next `lock_on_lease_gone` is a continuation epoch's close
    /// that this node's next acquisition may continue: the grant table
    /// stays, for that acquisition to keep or drop
    /// (`Core::epoch_tenure_resumed`).
    keep_grants: bool,
    /// Owner side: the restart horizon (`Meta::note_lock_grant_horizon`)
    /// as far as it is known durable. A grant or renewal to a peer is
    /// answered only once its window is covered; the write runs off the
    /// core (`Action::PersistLockHorizon`), so a slow disk delays the
    /// answers waiting for it, not every other event (`git-under-flock-b2b`
    /// seed 7: a 7.6 s sync blocked the core 12 s, and another node's
    /// grant lapsed meanwhile).
    horizon_durable: i64,
    /// The write in flight, if any (one at a time: later needs coalesce
    /// into the next one, which carries the highest).
    horizon_writing: Option<i64>,
    /// The highest horizon asked for (rounded up), written next.
    horizon_want: i64,
    /// What the answer being built needs durable (`lock_need_horizon`),
    /// taken by `lock_answer`.
    horizon_need: i64,
    /// Answers waiting for the horizon: `(needed, to, message)`, in the
    /// order they were made.
    horizon_held: Vec<(i64, NodeId, PeerMsg)>,
}

/// How many subtree floors an owner keeps before folding them into one
/// on the whole namespace (conservative: a larger floor only waits
/// longer).
const DIR_FLOORS_CAP: usize = 64;

/// How many inodes' release positions an owner remembers
/// ([`LockState::floors`]); past it the oldest-inode entries go, which
/// can only make a later grant carry less than it could.
const FLOORS_CAP: usize = 4096;

impl Core {
    /// Owner side: parked waiters right now (tests).
    #[cfg(test)]
    pub(crate) fn lock_waiters(&self) -> usize {
        self.lk.waiters.len()
    }

    /// Owner side: peers taken for unreachable right now (tests).
    #[cfg(test)]
    pub(crate) fn lock_unreachable(&self) -> usize {
        self.lk.unreachable.len()
    }
}

impl LockState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("lk_done_reqs", self.done_reqs.len()),
            ("lk_served", self.served.len()),
            ("lk_unused_grants", self.unused_grants.len()),
            ("lk_unproven", self.unproven.len()),
            ("lk_unreachable", self.unreachable.len()),
            ("lk_incarnations", self.incarnations.len()),
            ("lk_ops", self.ops.len()),
            ("lk_waiters", self.waiters.len()),
            ("lk_recalls", self.recalls.len()),
            ("lk_renews", self.renews.len()),
            ("lk_late_renews", self.late_renews.len()),
            ("lk_horizon_held", self.horizon_held.len()),
            ("lk_barriers", self.barriers.len()),
            ("lk_dir_barriers", self.dir_barriers.len()),
            ("lk_heads", self.heads.len()),
        ]
    }
}

/// Plan 30 §M14's view for `status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockView {
    pub requests_in_flight: usize,
    pub waiters: usize,
    pub recalls_in_flight: usize,
}

/// Where a lock request for `ino` goes.
enum Route {
    /// This node owns it: the usable end of its authority (its clock),
    /// and the generation (0: the root).
    Me {
        cap_ms: i64,
        gen: u64,
    },
    Node(NodeId),
    Unknown,
}

/// What serving a request produced.
enum Served {
    Outcome(LockOutcome),
    Parked,
}

impl Core {
    pub fn lock_view(&self) -> LockView {
        LockView {
            requests_in_flight: self.lk.ops.len(),
            waiters: self.lk.waiters.len(),
            recalls_in_flight: self.lk.recalls.len(),
        }
    }

    pub(crate) fn lock_ttl_ms(&self) -> i64 {
        self.cfg.lock_ttl_ms as i64
    }

    pub(crate) fn lock_margin_ms(&self) -> i64 {
        self.cfg.expiry_margin_ms as i64
    }

    /// The shortest grant worth making: its holder honours it for
    /// `ttl − margin` and renews half-way through that, so under
    /// `2 × margin` there is barely time for one renewal round trip.
    fn lock_min_grant_ms(&self) -> i64 {
        (2 * self.lock_margin_ms()).min(self.lock_ttl_ms())
    }

    /// A restamp for a grant that changed owner (a move, a reclaim, a
    /// mirror installed): as if renewed now, which is never sooner than
    /// what its holder measured.
    fn restamp(&self, now: Ms) -> i64 {
        now.0 + self.lock_ttl_ms() + self.lock_margin_ms()
    }

    // ------------------------------------------------------------ routing

    fn lock_route(&self, now: Ms, ino: Ino, replica: &dyn Replica) -> Route {
        self.lock_route_for(now, ino, replica, false)
    }

    /// The route as answered to `from`'s request (`renewal`: see
    /// `lock_route_for`). A non-owner never names the requester itself
    /// as the owner: that is a cached holder at least as stale as the
    /// requester's own (which named this node), and two non-holders that
    /// each cached the other sent every request back and forth until a
    /// lease expired (650acc8's review; `locks-blips-tight` with in-doubt
    /// lease PUTs, seed 400925: 56 s after the last holder released).
    /// `NotOwner { 0 }` sends it to the lease instead.
    fn lock_route_answering(
        &self,
        now: Ms,
        ino: Ino,
        replica: &dyn Replica,
        from: NodeId,
        renewal: bool,
    ) -> Route {
        match self.lock_route_for(now, ino, replica, renewal) {
            Route::Node(n) if n == from => Route::Unknown,
            route => route,
        }
    }

    /// `renewal`: a held lease whose takeover gate is still pending (the
    /// view fenced for M9's floor) still serves renewals and reclaims —
    /// that is what the floor is for; only new grants wait (the
    /// harness's lock-failover found the successor answering `NotOwner`
    /// until the locker's grant lapsed).
    fn lock_route_for(&self, now: Ms, ino: Ino, replica: &dyn Replica, renewal: bool) -> Route {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if d.node == self.cfg.node_id {
                    return match self.deleg_mine_until(d.gen) {
                        Some(until) if now < until => Route::Me {
                            cap_ms: until.0 - self.lock_margin_ms() - now.0,
                            gen: d.gen,
                        },
                        // Recalled: the subtree's grants went back to the
                        // root that recalled it, which serves them from
                        // its own table (its journal ended the generation
                        // before this table hears of it).
                        _ => match self.deleg_handed_back_to(d.gen) {
                            Some(root) => Route::Node(root),
                            // Not installed or renewed yet: a request here
                            // would be answered `NotOwner{me}`; wait a tick.
                            None => Route::Unknown,
                        },
                    };
                }
                if let Some(&to) = self.lk.deleg_moved.get(&d.gen) {
                    return Route::Node(to);
                }
                return Route::Node(d.node);
            }
        }
        if self.lease.usable(now, &self.cfg) && (renewal || !self.lease.fenced()) {
            return Route::Me {
                cap_ms: self.lock_cap_ms(now),
                gen: 0,
            };
        }
        // An epoch's close let the lease go locally and kept its grants,
        // and S3 was cut again before the re-claim landed: the lease still
        // stands as this node's (`epoch_reclaim_expires`), so the kept
        // grants are renewed under it, capped at its expiry as a held
        // lease's are; new grants wait for the re-claim
        // (`lock_op_while_resuming`). Answered `NotOwner { 0 }`, they
        // lapsed under their holders' I/O whenever the next epoch could
        // not form: its other members were still in the last one, waiting
        // for this very re-claim (`locks-blips-tight` seed 99102).
        if renewal {
            if let Some(expires) = self.epoch_reclaim_expires(now) {
                return Route::Me {
                    cap_ms: expires - self.lock_margin_ms() - now.0,
                    gen: 0,
                };
            }
        }
        match self
            .lease
            .cached_holder
            .filter(|h| *h != 0 && *h != self.cfg.node_id)
        {
            Some(h) => Route::Node(h),
            None => Route::Unknown,
        }
    }

    /// `from` answered a request or renewal for `ino` with `NotOwner {
    /// owner }`. From the delegate this node's table names for it, an
    /// owner elsewhere is the root its recall handed the subtree back to:
    /// the subtree goes there (`LockState::deleg_moved`). From the node
    /// that redirect named, the redirect is stale (that node's table
    /// still delegates the subtree, or it is not the root any more): the
    /// subtree goes to its delegate again, which names the owner anew.
    fn lock_note_not_owner(
        &mut self,
        from: NodeId,
        ino: Ino,
        owner: NodeId,
        replica: &dyn Replica,
    ) {
        if !self.cfg.delegation || replica.delegation_table().is_empty() {
            return;
        }
        let keys = Self::read_keys(ino, None);
        let constellation_meta::delegation::Ownership::Delegated(d) =
            replica.resolve_ownership(&keys)
        else {
            return;
        };
        let me = self.cfg.node_id;
        if d.node == me {
            return;
        }
        if from == d.node {
            if owner != 0 && owner != from && owner != me {
                self.lk.deleg_moved.insert(d.gen, owner);
            }
        } else if self.lk.deleg_moved.get(&d.gen) == Some(&from) {
            self.lk.deleg_moved.remove(&d.gen);
        }
    }

    /// The root lease is this node's again in a moment, with the grant
    /// table it has: an epoch's close let it go and its re-claim is
    /// pending (`epoch_reclaim_pending`), or it holds the lease and the
    /// gate of the acquisition that took it is still closed (any
    /// acquisition's: a takeover's, the re-claim's, a first one's). It
    /// answers from that table once the lease is usable: a request it
    /// conflicts with would block now, any other waits for it. A refusal
    /// grants nothing, so a table that is not the whole truth yet (a
    /// takeover's, whose predecessor's grants are still being learned)
    /// only refuses early what would have waited.
    fn lock_owner_resuming(&self, now: Ms) -> bool {
        self.epoch_reclaim_pending(now)
            || (self.lease.usable(now, &self.cfg) && self.lease.fenced() && !self.lease.releasing)
    }

    /// The root's grants never outlive its lease's usable end (under a
    /// continuation epoch the lease has no expiry; the ttl stands).
    fn lock_cap_ms(&self, now: Ms) -> i64 {
        let Some((lease, _)) = &self.lease.held else {
            return 0;
        };
        if self.lease.epoch_held() {
            return self.lock_ttl_ms();
        }
        lease.expires_unix_ms - self.lock_margin_ms() - now.0
    }

    /// The inode is under a subtree grace (an outwaited delegate), or is
    /// unlinked while one lasts: a generation that ends without handing
    /// its grants back (outwaited, sealed, drained from its backup) may
    /// have granted an inode its rows then took out of the subtree, and
    /// such an inode is under no directory any more. Which subtree it
    /// left is not recorded, so any grace covers every unlinked inode
    /// (reclaims admitted, as under any grace).
    fn lock_in_grace(&mut self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        use constellation_fs_core::types::ROOT_INO;
        self.lk.grace.retain(|(_, until)| *until > now);
        if self.lk.grace.is_empty() {
            return false;
        }
        let dirs: Vec<Ino> = self.lk.grace.iter().map(|(d, _)| *d).collect();
        dirs.iter()
            .any(|d| *d == ROOT_INO || replica.is_under(ino, *d))
            || (ino != ROOT_INO && !replica.is_under(ino, ROOT_INO))
    }

    /// Whether new grants are refused now (reclaims still accepted).
    fn lock_grace_active(&mut self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        Self::lock_quarantine_until(replica) > now.0 || self.lock_in_grace(now, ino, replica)
    }

    /// The whole-namespace grace: the restart or takeover quarantine of
    /// read delegations, or of lock grants (kept apart, see
    /// `LockTables::set_quarantine`), whichever ends later.
    fn lock_quarantine_until(replica: &dyn Replica) -> i64 {
        replica
            .read_delegations()
            .quarantine_until()
            .max(replica.locks().quarantine_until())
    }

    // ------------------------------------------------------------ owner side

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        sent: Ms,
        incarnation: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_foreign(now, replica, out);
        if !self.lock_note_incarnation(now, from, incarnation, replica, out) {
            // Answered all the same: the RPC waits for its reply on the
            // serving side (its entry in the driver's reply table, the
            // P2P task holding the stream). The process that sent it is
            // gone; `Waiting` asks nothing of a live one.
            let outcome = LockOutcome::Waiting {
                retry_ms: self.lock_resume_retry_ms(),
            };
            self.lock_answer(from, PeerMsg::LockReply { req, outcome }, out);
            return;
        }
        // A re-sent request of a parked node: re-attach (and answer at
        // once if its grant is ready). Two ops of one node on one inode
        // are two waiters (matched by mode; sim seed 93007).
        if let Some(i) = self
            .lk
            .waiters
            .iter()
            .position(|w| w.node == from && w.ino == ino && w.mode == mode && w.op.is_none())
        {
            let hold = self.cfg.recall_hold_ms;
            let wid = self.lk.waiters[i].id;
            let timer = self.set_timer(now.plus(hold), Timer::LockHeldReply(wid), out);
            let w = &mut self.lk.waiters[i];
            w.sent = sent;
            w.recv = now;
            w.req = Some(req);
            let old = w.held_timer.replace(timer);
            if let Some(old) = old {
                self.cancel_timer(old, out);
            }
            self.lock_serve_waiters(now, ino, replica, out);
            return;
        }
        match self.lock_serve(
            now,
            from,
            ino,
            mode,
            blocking,
            Some(req),
            None,
            sent,
            replica,
            out,
        ) {
            Served::Outcome(outcome) => {
                self.lock_answer(from, PeerMsg::LockReply { req, outcome }, out)
            }
            Served::Parked => {}
        }
    }

    /// A lock request from `from`'s `incarnation`: `false` if it comes
    /// from an older incarnation than one already seen (a request delayed
    /// in flight from a process that is gone: not served, only answered).
    /// A newer one drops what the previous incarnation left queued here —
    /// its parked waiters, their kept queue positions — and serves the
    /// waiters behind them. A dropped waiter's held request is answered
    /// `Waiting`, as every removed waiter's is (`lock_on_lease_gone`). Its grants stay until released or
    /// outwaited: that process is gone, but nothing here proves its I/O
    /// is over.
    ///
    /// The new incarnation starts out unreachable from here
    /// (`LockState::unreachable`) until it acknowledges a recall. Its
    /// requests reach this owner over its own fresh connection, but this
    /// owner's link to it takes seconds to come back after a restart, and
    /// once never did (PROGRESS.md "lock-recall-unreachable"): the
    /// first grant pushed to it was lost, and every waiter behind it
    /// waited out `ttl + margin` (`git-under-flock-causal`: 20 s after 3
    /// of 8 reader restarts, once the 35-minute livelock was gone).
    fn lock_note_incarnation(
        &mut self,
        now: Ms,
        from: NodeId,
        incarnation: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if from == self.cfg.node_id {
            return true;
        }
        let seen = self.lk.incarnations.get(&from).copied();
        match seen {
            Some(i) if incarnation < i => {
                self.stats.lock_stale_incarnation_requests += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    from,
                    incarnation,
                    seen = i,
                    "ignored a lock request from an earlier incarnation"
                );
                return false;
            }
            Some(i) if incarnation == i => return true,
            _ => {}
        }
        self.lk.incarnations.insert(from, incarnation);
        let Some(previous) = seen else {
            return true;
        };
        let mut inos = Vec::new();
        let mut dropped = 0u64;
        let mut i = 0;
        while i < self.lk.waiters.len() {
            let w = &self.lk.waiters[i];
            if w.node == from && w.op.is_none() {
                let w = self.lk.waiters.remove(i);
                if let Some(t) = w.held_timer {
                    self.cancel_timer(t, out);
                }
                if let Some(req) = w.req {
                    let outcome = LockOutcome::Waiting {
                        retry_ms: self.lock_resume_retry_ms(),
                    };
                    self.lock_answer(from, PeerMsg::LockReply { req, outcome }, out);
                }
                if !inos.contains(&w.ino) {
                    inos.push(w.ino);
                }
                dropped += 1;
            } else {
                i += 1;
            }
        }
        self.lk.served.retain(|(n, _), _| *n != from);
        self.lk.unused_grants.remove(&from);
        self.lock_peer_unreachable(from, "it restarted: this owner has not reached it yet");
        self.stats.lock_incarnation_waiters_dropped += dropped;
        tracing::info!(
            node = self.cfg.node_id,
            from,
            incarnation,
            previous,
            dropped,
            "a node asked for a lock under a new incarnation; dropped what its previous one left \
             queued"
        );
        for ino in inos {
            self.lock_serve_waiters(now, ino, replica, out);
        }
        true
    }

    /// `node` cannot be reached from here (`LockState::unreachable`).
    fn lock_peer_unreachable(&mut self, node: NodeId, why: &'static str) {
        if node == self.cfg.node_id || !self.lk.unreachable.insert(node) {
            return;
        }
        self.stats.lock_peers_unreachable += 1;
        // Its kept positions go: it is passed over until a request of its
        // own arrives, and then it queues as any new request does.
        self.lk.served.retain(|(n, _), _| *n != node);
        tracing::info!(
            node = self.cfg.node_id,
            peer = node,
            why,
            "a lock waiter's node is unreachable from this owner; it is granted only over its \
             own requests until it acknowledges a recall"
        );
    }

    /// `node` used a grant, or this owner reached it.
    fn lock_peer_heard(&mut self, node: NodeId, reached: bool) {
        self.lk.unused_grants.remove(&node);
        if reached && self.lk.unreachable.remove(&node) {
            tracing::info!(
                node = self.cfg.node_id,
                peer = node,
                "a lock waiter's node is reachable from this owner again"
            );
        }
    }

    /// Note that the answer being built needs the restart horizon durable
    /// up to `need`; `target` (≥ `need`, rounded up) is what to write.
    fn lock_need_horizon(&mut self, need: i64, target: i64) {
        self.lk.horizon_need = self.lk.horizon_need.max(need);
        self.lk.horizon_want = self.lk.horizon_want.max(target.max(need));
    }

    /// Send an owner's answer to a peer: at once if the horizon it needs
    /// (`lock_need_horizon`) is durable, else once the write lands
    /// (`on_lock_horizon_persisted`). Answers to one peer keep their
    /// order: one made after a held one waits behind it. Only the
    /// answers pass through here: a direct send (a `LockRecall`) can
    /// overtake a held grant, and so reach the holder before the grant
    /// it recalls — covered on the holder's side by `note_pending_recall`
    /// and `tombstone_unheld`.
    fn lock_answer(&mut self, to: NodeId, msg: PeerMsg, out: &mut Vec<Action>) {
        let need = std::mem::take(&mut self.lk.horizon_need);
        let behind = self.lk.horizon_held.iter().any(|(_, n, _)| *n == to);
        if need <= self.lk.horizon_durable && !behind {
            out.push(Action::Send { to, msg });
            return;
        }
        self.stats.lock_horizon_held += 1;
        self.lk.horizon_held.push((need, to, msg));
        self.lock_horizon_write(out);
    }

    fn lock_horizon_write(&mut self, out: &mut Vec<Action>) {
        if self.lk.horizon_writing.is_some() || self.lk.horizon_want <= self.lk.horizon_durable {
            return;
        }
        let until = self.lk.horizon_want;
        self.lk.horizon_writing = Some(until);
        self.stats.lock_horizon_writes += 1;
        out.push(Action::PersistLockHorizon { until });
    }

    /// `Action::PersistLockHorizon` finished: `durable` is the horizon
    /// now on disk (`None`: the write failed). The answers it covers go
    /// out in order; on a failure the ones it should have covered are
    /// refused instead — as a failed synchronous write refused them —
    /// a grant answered `Busy` (and dropped here), a renewal
    /// `NotOwner { 0 }` (the holder retries).
    pub(crate) fn on_lock_horizon_persisted(
        &mut self,
        now: Ms,
        until: i64,
        durable: Option<i64>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.lk.horizon_writing == Some(until) {
            self.lk.horizon_writing = None;
        }
        let failed = match durable {
            Some(d) => {
                self.lk.horizon_durable = self.lk.horizon_durable.max(d);
                false
            }
            None => {
                self.stats.lock_horizon_failed += 1;
                // Asked again only by a later answer.
                self.lk.horizon_want = self.lk.horizon_durable;
                true
            }
        };
        let durable = self.lk.horizon_durable;
        let held = std::mem::take(&mut self.lk.horizon_held);
        let mut keep: Vec<(i64, NodeId, PeerMsg)> = Vec::new();
        let mut dropped = Vec::new();
        for (need, to, mut msg) in held {
            if failed {
                // Everything held goes now, in order: what the write
                // should have covered refused.
                if need > durable && !Self::lock_refuse_answer(&mut msg, &mut dropped) {
                    continue;
                }
                out.push(Action::Send { to, msg });
            } else if need <= durable && !keep.iter().any(|(_, n, _)| *n == to) {
                out.push(Action::Send { to, msg });
            } else {
                keep.push((need, to, msg));
            }
        }
        self.lk.horizon_held = keep;
        for id in dropped {
            if let Some(g) = replica.locks().get(id) {
                self.lock_grant_done(now, id, g.ino, replica, out);
            }
        }
        self.lock_horizon_write(out);
    }

    /// A held answer whose horizon could not be persisted: its grants
    /// refused (their ids into `dropped`), its renewals `NotOwner { 0 }`.
    /// `false`: nothing to send (a push; the waiter asks again).
    fn lock_refuse_answer(msg: &mut PeerMsg, dropped: &mut Vec<GrantId>) -> bool {
        match msg {
            PeerMsg::LockReply { outcome, .. } => {
                if let LockOutcome::Granted { id, .. } = outcome {
                    dropped.push(*id);
                    *outcome = LockOutcome::Busy;
                }
                true
            }
            PeerMsg::LockGranted { outcome, .. } => {
                if let LockOutcome::Granted { id, .. } = outcome {
                    dropped.push(*id);
                }
                false
            }
            PeerMsg::LockRenewed { results, .. } => {
                for (_, _, r) in results.iter_mut() {
                    if matches!(r, LockRenewResult::Ok { .. }) {
                        *r = LockRenewResult::NotOwner { owner: 0 };
                    }
                }
                true
            }
            _ => true,
        }
    }

    /// Grant, or recall and park. `req`/`op`: the waiter's identity if it
    /// parks. Never called for a re-attach.
    #[allow(clippy::too_many_arguments)]
    fn lock_serve(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Served {
        let (cap_ms, gen) = match self.lock_route_answering(now, ino, replica, from, false) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return Served::Outcome(LockOutcome::NotOwner { owner: n }),
            Route::Unknown if self.lock_owner_resuming(now) => {
                // This node is the owner again in a moment (an epoch's
                // close re-claims the lease it let go, or the gate of the
                // acquisition is still pending), and its grant table is
                // the one it will answer from: a conflict is `EAGAIN`
                // now, anything else waits — `Waiting`, which the
                // requester retries without spending its attempts.
                // `NotOwner` sent the requester to the lease, which names
                // this node: its retries ran out and a non-blocking lock
                // failed `ENOLCK` (`stress-ng-fs-faults`'s blips).
                return Served::Outcome(
                    self.lock_conflict_refusal(now, from, ino, mode, blocking, replica)
                        .unwrap_or(LockOutcome::Waiting {
                            retry_ms: self.lock_resume_retry_ms(),
                        }),
                );
            }
            Route::Unknown => return Served::Outcome(LockOutcome::NotOwner { owner: 0 }),
        };
        // Plan 30 §M9: a tenure that may be taken over before its lease
        // expires grants only with fresh S3 liveness, and only once the
        // lease says it grants (its successor then waits the horizon
        // out). A delegate's authority is its renewed grant; the root's
        // is the lease.
        let root_owned = self.lock_route_is_root(now, ino, replica);
        if root_owned
            && (!self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out))
        {
            // A refusal grants nothing: a non-blocking request that a
            // live grant conflicts with is refused even before this
            // tenure may grant (a fresh successor whose lease is not
            // marked yet: the harness's lock-failover contender got
            // `Busy`, then `ENOLCK`, while the locker held its grant).
            return Served::Outcome(
                self.lock_conflict_refusal(now, from, ino, mode, blocking, replica)
                    .unwrap_or(LockOutcome::Busy),
            );
        }
        if self.lock_grace_active(now, ino, replica) {
            self.stats.lock_grace_refusals += 1;
            return if blocking {
                self.lock_park(now, from, ino, mode, req, op, sent, out);
                Served::Parked
            } else {
                Served::Outcome(LockOutcome::WouldBlock)
            };
        }
        let conflicting =
            match self.lock_try_grant(now, now, from, ino, mode, cap_ms, gen, out, replica) {
                Ok(outcome) => return Served::Outcome(outcome),
                Err(conflicting) => conflicting,
            };
        for g in conflicting {
            self.lock_recall(now, g, replica, out);
        }
        if blocking {
            self.lock_park(now, from, ino, mode, req, op, sent, out);
            Served::Parked
        } else {
            self.stats.lock_would_block += 1;
            Served::Outcome(LockOutcome::WouldBlock)
        }
    }

    /// A non-blocking request that a live grant conflicts with, asked of
    /// an owner that may not grant yet: `WouldBlock` now (a refusal
    /// grants nothing). `None`: it waits.
    #[allow(clippy::too_many_arguments)]
    fn lock_conflict_refusal(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        replica: &dyn Replica,
    ) -> Option<LockOutcome> {
        if blocking
            || replica
                .locks()
                .conflicting(ino, from, mode, now.0)
                .is_empty()
        {
            return None;
        }
        self.stats.lock_would_block += 1;
        Some(LockOutcome::WouldBlock)
    }

    /// How soon a request waiting for this node to own the root again
    /// (`lock_owner_resuming`) asks again.
    fn lock_resume_retry_ms(&self) -> u64 {
        (self.cfg.forward_backoff_ms / 4).max(10)
    }

    /// How long the queue waits for an unreachable waiter's next request
    /// (`lock_serve_waiters`): two of its request cycles (the request
    /// held `recall_hold_ms`, then re-sent after `lock_resume_retry_ms`).
    fn lock_unreachable_wait_ms(&self) -> i64 {
        2 * (self.cfg.recall_hold_ms + self.lock_resume_retry_ms()) as i64
    }

    /// Grant `mode` to `from` — re-affirming or upgrading *in place* a
    /// live grant it already holds (an owner never downgrades: the
    /// node's local locks may be under the stronger mode; the sim found
    /// a shared request in flight behind an exclusive one) — or the
    /// conflicting grants to recall. `base`: when the request being
    /// answered arrived (`now` for one answered on arrival; see
    /// [`Waiter::recv`]).
    #[allow(clippy::too_many_arguments)]
    fn lock_try_grant(
        &mut self,
        now: Ms,
        base: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        cap_ms: i64,
        gen: u64,
        out: &mut Vec<Action>,
        replica: &dyn Replica,
    ) -> Result<LockOutcome, Vec<Grant>> {
        // A new root tenure grants nothing until its floor is known
        // (parked, or `WouldBlock`; reclaims go on).
        if gen == 0 && !self.lock_tenure_floor_ready(replica) {
            self.stats.lock_tenure_waits += 1;
            return Err(Vec::new());
        }
        let own = replica.locks().own_grant(ino, from, now.0);
        if own.is_some_and(|g| g.recalled) {
            // Recalled: the node gives it up once its local locks are
            // gone; nothing is granted meanwhile (a re-affirmation kept
            // the id, and the release then dropped a grant with locks
            // under it — the harness's SQLite run found this). The
            // request parks (or would block) until the release.
            return Err(Vec::new());
        }
        let mode = own.map_or(mode, |g| g.mode.max(mode));
        let reaffirm = own.is_some_and(|g| g.mode == mode);
        let conflicting = if reaffirm {
            Vec::new()
        } else {
            replica.locks().conflicting(ino, from, mode, now.0)
        };
        self.lock_take_outwaited(replica);
        if !conflicting.is_empty() {
            return Err(conflicting);
        }
        // A holder of this inode was outwaited: what it did under its
        // grant must be under this one's floor first (parked, or
        // `WouldBlock`). A re-affirmation gives the node nothing new.
        if !reaffirm && !self.lock_barrier_ready(now, from, ino, gen, replica, out) {
            return Err(Vec::new());
        }
        let ttl = self.lock_ttl_ms().min(cap_ms);
        if ttl < self.lock_min_grant_ms() {
            // Too little authority left to grant anything the holder
            // could keep (a delegate late in its delegation's window): the
            // request waits for the delegation's renewal (parked, or
            // `WouldBlock`), which is asked for now. A grant this short
            // lapsed at its holder before its first renewal.
            if gen != 0 {
                self.stats.lock_short_authority_waits += 1;
                self.deleg_renew_now(now, gen, out);
                return Err(Vec::new());
            }
            if ttl <= 0 {
                return Ok(LockOutcome::Busy);
            }
        }
        let until = base.0.min(now.0) + ttl + self.lock_margin_ms();
        // The horizon is what this node's restart waits out, for grants
        // honoured elsewhere. A grant to itself dies with the process (its
        // held table and local locks are in memory, and a handover is
        // refused under a cluster lock), so it must not keep the restarted
        // node from granting: a lone node's remount answered every
        // non-blocking lock `EAGAIN` for a lock TTL
        // (`transport-lock-wait-budget`). The answer waits for it
        // (`lock_answer`).
        if from != self.cfg.node_id {
            self.lock_need_horizon(until, until);
        }
        // Every grant is a new id, an own grant re-asked for included:
        // the node may have dropped (lapsed, released) the id the owner
        // still holds, and it refuses to reinstall a released id (sim
        // seed 90017 looped on that). The old id dies with its recall
        // bookkeeping; the node replaces it under its local locks.
        let id = {
            {
                if let Some(old) = own {
                    if let Some(r) = self.lk.recalls.remove(&old.id) {
                        self.cancel_timer(r.timer, out);
                        if let Some(req) = r.req {
                            self.lk.recall_by_req.remove(&req);
                        }
                    }
                }
                replica
                    .locks()
                    .grant(self.cfg.node_id, from, ino, mode, until, gen)
            }
        };
        // A barrier its own outwaited record left ends with this grant
        // when it is exclusive (see `LockState::barriers`).
        if mode == LockMode::Exclusive
            && self
                .lk
                .barriers
                .get(&ino)
                .is_some_and(|(_, holder)| *holder == Some(from))
        {
            self.lk.barriers.remove(&ino);
        }
        self.stats.lock_grants += 1;
        self.lk.mirror_dirty = true;
        let position = self.lock_grant_position(from, ino, replica);
        tracing::debug!(
            node = self.cfg.node_id,
            to = from,
            ino,
            ?mode,
            ttl,
            upgraded = own.is_some(),
            "granted a lock"
        );
        Ok(LockOutcome::Granted {
            id,
            mode,
            ttl_ms: ttl as u64,
            position,
        })
    }

    fn lock_route_is_root(&self, now: Ms, ino: Ino, replica: &dyn Replica) -> bool {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(_) =
                replica.resolve_ownership(&keys)
            {
                return false;
            }
        }
        let _ = now;
        true
    }

    /// The position a grant carries: everything acknowledged on `ino`
    /// before it (as a ReadIndex answer would say).
    fn lock_position(&self, ino: Ino, replica: &dyn Replica) -> Position {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if d.node == self.cfg.node_id {
                    let mut s = constellation_meta::Streams::NONE;
                    let idx = replica.stream_applied(d.gen);
                    if idx > 0 {
                        s.raise(d.gen, idx);
                    }
                    return Position {
                        seq: replica.applied_seq().unwrap_or(0),
                        pending: None,
                        streams: s,
                    };
                }
            }
        }
        let epoch = self.lease.epoch().unwrap_or(0);
        let pending = if replica.unshipped_touches_read(ino, false, None) {
            replica.journal_position(epoch)
        } else {
            None
        };
        Position {
            seq: self.ship.head_seq,
            pending,
            streams: Default::default(),
        }
    }

    /// [`Self::lock_position`] joined with what the previous holders of
    /// `ino` released at ([`LockState::floors`]): the position a grant
    /// to `to` carries. A root holder's grant to itself leaves out its own
    /// tenure's journal: its replica holds that already, and the session
    /// counts only shipped journal as reached, so its reads would wait
    /// for the next ship.
    fn lock_grant_position(&self, to: NodeId, ino: Ino, replica: &dyn Replica) -> Position {
        let own = self.lock_position(ino, replica);
        let mut p = match self.lk.floors.get(&ino) {
            Some(floor) => constellation_meta::locks::floor_join(&own, floor),
            None => own,
        };
        for (dir, floor) in &self.lk.dir_floors {
            if *dir == constellation_fs_core::types::ROOT_INO || replica.is_under(ino, *dir) {
                p = constellation_meta::locks::floor_join(&p, floor);
            }
        }
        if to == self.cfg.node_id && self.lease.held.is_some() {
            let epoch = self.lease.epoch().unwrap_or(0);
            if p.pending.is_some_and(|j| j.epoch == epoch) {
                p.pending = None;
            }
        }
        p
    }

    /// Everything this node's clients were acknowledged, as a sequencer
    /// included: its session frontier, plus — as the root holder — its
    /// whole unshipped journal (its own clients' writes live there and
    /// raise no frontier). A delegate's own executions are in the
    /// frontier already (its stream index).
    fn lock_release_floor(&self, replica: &dyn Replica) -> Position {
        let mut p = replica.frontier();
        // Its own executions as a delegate: the session frontier can
        // trail them (it records what replies and streams brought here,
        // and a delegate's own client ops reach it later — sim
        // `locks-delegated-writes` seed 200981: a turn written under the
        // lock at its generation's index 5 released at `(gen, 4)`).
        for gen in self.dl.mine.keys() {
            let idx = replica.delegate_idx(*gen);
            if idx > 0 {
                let mut own = Position::ZERO;
                if own.streams.raise(*gen, idx) {
                    p = constellation_meta::locks::floor_join(&p, &own);
                }
            }
        }
        if self.lease.epoch().is_some() && self.lease.held.is_some() {
            let epoch = self.lease.epoch().unwrap_or(0);
            p = constellation_meta::locks::floor_join(
                &p,
                &Position {
                    seq: self.ship.head_seq,
                    pending: replica.journal_position(epoch),
                    streams: Default::default(),
                },
            );
        }
        p
    }

    /// Remember that a holder released `ino` at `position`.
    fn lock_note_floor(&mut self, ino: Ino, position: &Position) {
        if *position == Position::ZERO {
            return;
        }
        let e = self.lk.floors.entry(ino).or_insert(Position::ZERO);
        *e = constellation_meta::locks::floor_join(e, position);
        while self.lk.floors.len() > FLOORS_CAP {
            self.lk.floors.pop_first();
        }
        self.lk.floor_all = constellation_meta::locks::floor_join(&self.lk.floor_all, position);
        self.lk.mirror_dirty = true;
    }

    /// A new root tenure's first grants wait for its floor (see
    /// [`LockState::tenure_waiting`]); `true` once it is noted. Only the
    /// root's own grants (`gen == 0`) ask.
    fn lock_tenure_floor_ready(&mut self, replica: &dyn Replica) -> bool {
        if !self.lk.tenure_floor_due {
            return true;
        }
        let me = self.cfg.node_id;
        if self.lk.tenure_waiting.is_none() {
            self.lk.tenure_waiting = Some(
                self.dl
                    .gens
                    .iter()
                    .filter(|(gen, g)| {
                        !g.ended && g.node != me && !self.lk.tenure_minted.contains(*gen)
                    })
                    .map(|(gen, _)| *gen)
                    .collect(),
            );
        }
        let ended: Vec<u64> = self
            .lk
            .tenure_waiting
            .iter()
            .flatten()
            .copied()
            .filter(|gen| self.dl.gens.get(gen).is_none_or(|g| g.ended))
            .collect();
        if let Some(w) = self.lk.tenure_waiting.as_mut() {
            for gen in ended {
                w.remove(&gen);
            }
        }
        if self
            .lk
            .tenure_waiting
            .as_ref()
            .is_some_and(|w| !w.is_empty())
        {
            return false;
        }
        self.lk.tenure_floor_due = false;
        self.lk.tenure_waiting = None;
        self.lk.tenure_minted.clear();
        let mut floor = self.lock_release_floor(replica);
        for (gen, head) in std::mem::take(&mut self.lk.tenure_heads) {
            let mut p = Position::ZERO;
            if p.streams.raise(gen, head) {
                floor = constellation_meta::locks::floor_join(&floor, &p);
            }
        }
        self.lock_note_dir_floor(constellation_fs_core::types::ROOT_INO, &floor);
        true
    }

    /// Root: a delegate renewed `gen` with its stream `head`, executed
    /// by `at` on its clock; the renewal arrived `now`.
    pub(crate) fn lock_note_delegate_head(
        &mut self,
        now: Ms,
        from: NodeId,
        gen: u64,
        head: u64,
        at: i64,
    ) {
        if !self.dl.gens.get(&gen).is_some_and(|g| g.node == from) {
            return;
        }
        if let Some(w) = self.lk.tenure_waiting.as_mut() {
            if w.remove(&gen) && head > 0 {
                self.lk.tenure_heads.push((gen, head));
            }
        }
        let e = self.lk.heads.entry(gen).or_insert((at, head, now));
        e.2 = e.2.max(now);
        if at >= e.0 {
            *e = (at, e.1.max(head), e.2);
        }
    }

    /// Owner side: the grant records that expired here unreleased since
    /// the last look become barriers on their inodes (see
    /// [`LockState::barriers`]). This node's own grants are left out:
    /// what its clients were acknowledged is in its release floor, which
    /// every grant here joins anyway.
    fn lock_take_outwaited(&mut self, replica: &dyn Replica) {
        for g in replica.locks().take_outwaited() {
            if g.node != self.cfg.node_id {
                self.lock_barrier(g.ino, g.until_ms, Some(g.node));
            }
        }
    }

    /// No grant on `ino` before a cut as of `at` is its floor; `holder`:
    /// whose record it was, if known.
    fn lock_barrier(&mut self, ino: Ino, at: i64, holder: Option<NodeId>) {
        let e = self.lk.barriers.entry(ino).or_insert((i64::MIN, holder));
        if e.1 != holder {
            e.1 = None;
        }
        if e.0 >= at {
            return;
        }
        e.0 = at;
        self.stats.lock_outwait_barriers += 1;
        if self.lk.barriers.len() > FLOORS_CAP {
            let all = self
                .lk
                .barriers
                .values()
                .map(|(a, _)| *a)
                .max()
                .unwrap_or(at);
            self.lk.barriers.clear();
            self.lock_dir_barrier(constellation_fs_core::types::ROOT_INO, all);
        }
    }

    /// No grant under `dir` before a cut as of `at` is its floor.
    fn lock_dir_barrier(&mut self, dir: Ino, at: i64) {
        if at <= 0 {
            return;
        }
        match self.lk.dir_barriers.iter_mut().find(|(d, _)| *d == dir) {
            Some((_, a)) => *a = (*a).max(at),
            None => self.lk.dir_barriers.push((dir, at)),
        }
        if self.lk.dir_barriers.len() > DIR_FLOORS_CAP {
            let all = self
                .lk
                .dir_barriers
                .drain(..)
                .map(|(_, a)| a)
                .max()
                .unwrap_or(at);
            self.lk
                .dir_barriers
                .push((constellation_fs_core::types::ROOT_INO, all));
        }
    }

    /// The latest barrier that applies to `ino` for a grant to `to`
    /// (`None`: none). An inode under no directory (unlinked) is under
    /// every directory barrier, as under every subtree grace
    /// (`lock_in_grace`): which subtree it left is not recorded. One left
    /// by `to`'s own record only does not apply (see
    /// [`LockState::barriers`]).
    fn lock_barrier_for(&self, ino: Ino, to: NodeId, replica: &dyn Replica) -> Option<i64> {
        use constellation_fs_core::types::ROOT_INO;
        let mut at = self
            .lk
            .barriers
            .get(&ino)
            .filter(|(_, holder)| *holder != Some(to))
            .map(|(a, _)| *a);
        if self.lk.dir_barriers.is_empty() {
            return at;
        }
        let orphan = !replica.is_under(ino, ROOT_INO);
        for (dir, a) in &self.lk.dir_barriers {
            if *dir == ROOT_INO || orphan || replica.is_under(ino, *dir) {
                at = Some(at.map_or(*a, |b| b.max(*a)));
            }
        }
        at
    }

    /// The latest barrier on or under `dir` (0: none) — what a subtree
    /// move carries.
    fn lock_barrier_under(&self, dir: Ino, replica: &dyn Replica) -> i64 {
        let mut at = 0;
        for (ino, (a, _)) in &self.lk.barriers {
            if replica.is_under(*ino, dir) {
                at = at.max(*a);
            }
        }
        for (d, a) in &self.lk.dir_barriers {
            if *d == constellation_fs_core::types::ROOT_INO
                || replica.is_under(dir, *d)
                || replica.is_under(*d, dir)
            {
                at = at.max(*a);
            }
        }
        at
    }

    /// Root: a cut of everything acknowledged anywhere, as `(as of,
    /// position)`: this node's release floor now (its log, its journal,
    /// its own generations), joined with the head every other live
    /// generation's delegate last renewed with. It is as of the earliest
    /// of those renewals' sends (and of now): a row executed before that,
    /// anywhere, is under the position. A live generation never heard
    /// from makes it as of nothing (`i64::MIN`). An ended generation
    /// counts no more: what this root appended of it is in its own
    /// position, and the rest was never appended — tentative, its
    /// acknowledgements rolled back (replayed by rid, where an outwaited
    /// holder's fencing token refuses them). A designation counts as any
    /// generation while it is online — renewed (or, never heard from,
    /// granted here) within one delegation TTL. An offline one adds the
    /// head it last renewed with but no longer holds the cut back: its
    /// designee writes while isolated and is never reclaimed (DESIGN.md
    /// §5.2), so waiting for it would stop every grant after an outwait
    /// for as long as it is away. What it executed after its last renewal
    /// is then not under the cut (`cluster-locks.md`, "After an outwait").
    /// A cut names at most `STREAMS_CAP` streams, as every floor does
    /// (`floor_join` keeps the newest): `Stats::lock_cut_truncated`.
    pub(crate) fn lock_cut_here(&mut self, now: Ms, replica: &dyn Replica) -> (i64, Position) {
        let me = self.cfg.node_id;
        let online_ms = self.cfg.delegation_ttl_ms as i64;
        let gens = &self.dl.gens;
        self.lk
            .heads
            .retain(|gen, _| gens.get(gen).is_some_and(|g| !g.ended));
        let mut at = now.0;
        let mut pos = self.lock_release_floor(replica);
        for (gen, g) in &self.dl.gens {
            if g.ended || g.node == me {
                continue;
            }
            let designated = g.kind == super::delegate::DelegKind::Designated;
            match self.lk.heads.get(gen) {
                Some((sent, head, heard)) => {
                    if !designated || now.0 - heard.0 < online_ms {
                        at = at.min(*sent);
                    }
                    let mut p = Position::ZERO;
                    if *head > 0 && p.streams.raise(*gen, *head) {
                        pos = constellation_meta::locks::floor_join(&pos, &p);
                    }
                }
                None if designated && now.0 - g.granted.0 >= online_ms => {}
                None => at = i64::MIN,
            }
        }
        let truncated = self.dl.gens.iter().any(|(gen, g)| {
            !g.ended
                && g.node != me
                && self.lk.heads.get(gen).is_some_and(|(_, head, _)| {
                    *head > 0 && pos.streams.get(*gen).is_none_or(|i| i < *head)
                })
        });
        if truncated {
            self.stats.lock_cut_truncated += 1;
        }
        (at, pos)
    }

    /// Every barrier a cut as of `at` settles becomes a floor: `pos` on
    /// its inode or directory — and, at a root, on the floor it re-sends
    /// with every granting renewal of a generation whose subtree has the
    /// inode or overlaps the directory (as a release that reaches the
    /// root after the move raises it). `true` if any did.
    fn lock_settle_barriers(&mut self, at: i64, pos: &Position, replica: &dyn Replica) -> bool {
        let inos: Vec<Ino> = self
            .lk
            .barriers
            .iter()
            .filter(|(_, (b, _))| *b <= at)
            .map(|(ino, _)| *ino)
            .collect();
        let dirs: Vec<Ino> = self
            .lk
            .dir_barriers
            .iter()
            .filter(|(_, b)| *b <= at)
            .map(|(d, _)| *d)
            .collect();
        for ino in &inos {
            self.lk.barriers.remove(ino);
            self.lock_note_floor(*ino, pos);
        }
        self.lk.dir_barriers.retain(|(_, b)| *b > at);
        for dir in &dirs {
            self.lock_note_dir_floor(*dir, pos);
        }
        if (!inos.is_empty() || !dirs.is_empty()) && !self.lk.handed_floor.is_empty() {
            let gens: Vec<u64> = self
                .lk
                .handed_floor
                .keys()
                .copied()
                .filter(|gen| {
                    self.dl.gens.get(gen).is_some_and(|g| {
                        inos.iter().any(|ino| replica.is_under(*ino, g.dir))
                            || dirs.iter().any(|d| {
                                *d == constellation_fs_core::types::ROOT_INO
                                    || replica.is_under(g.dir, *d)
                                    || replica.is_under(*d, g.dir)
                            })
                    })
                })
                .collect();
            for gen in gens {
                if let Some(f) = self.lk.handed_floor.get_mut(&gen) {
                    *f = constellation_meta::locks::floor_join(f, pos);
                }
            }
        }
        !inos.is_empty() || !dirs.is_empty()
    }

    /// Whether `ino` may be granted as far as outwait barriers go: none
    /// applies, or a cut as of the latest one is noted as its floor now.
    /// The root takes its own cut; a delegate the latest one its root
    /// sent, joined with its own release floor, and asks for a fresher
    /// one (a renewal) when it is too old.
    fn lock_barrier_ready(
        &mut self,
        now: Ms,
        to: NodeId,
        ino: Ino,
        gen: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if self.lock_barrier_for(ino, to, replica).is_none() {
            return true;
        }
        if self.lease.held.is_some() {
            let (at, pos) = self.lock_cut_here(now, replica);
            self.lock_settle_barriers(at, &pos, replica);
        } else if let Some((_, at, cut)) = self.lk.cut {
            let pos =
                constellation_meta::locks::floor_join(&cut, &self.lock_release_floor(replica));
            self.lock_settle_barriers(at, &pos, replica);
        }
        if self.lock_barrier_for(ino, to, replica).is_none() {
            return true;
        }
        self.stats.lock_barrier_waits += 1;
        if gen != 0 && self.lease.held.is_none() {
            self.deleg_renew_now(now, gen, out);
        }
        false
    }

    /// Delegate: a granting renewal answer from `root` carried its cut,
    /// as `(as of, position)`, and the latest barrier left under the
    /// subtree.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn lock_take_cut(
        &mut self,
        now: Ms,
        root: NodeId,
        gen: u64,
        (cut_at, cut): (i64, Position),
        barrier: i64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if barrier > 0 {
            if let Some(dir) = self.dl.mine.get(&gen).map(|d| d.dir) {
                self.lock_dir_barrier(dir, barrier);
            }
        }
        if self
            .lk
            .cut
            .is_none_or(|(from, at, _)| from != root || cut_at > at)
        {
            self.lk.cut = Some((root, cut_at, cut));
        }
        let Some((_, at, cut)) = self.lk.cut else {
            return;
        };
        if self.lk.barriers.is_empty() && self.lk.dir_barriers.is_empty() {
            return;
        }
        let pos = constellation_meta::locks::floor_join(&cut, &self.lock_release_floor(replica));
        if self.lock_settle_barriers(at, &pos, replica) {
            self.lock_reserve_all(now, replica, out);
        }
    }

    /// Root: what a granting renewal of `gen` carries for the barriers —
    /// the cut as of now and the latest barrier under the subtree (the
    /// delegate settles it with the cut when it covers it). This root's
    /// own barriers that the cut covers settle here too.
    pub(crate) fn lock_cut_for_generation(
        &mut self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
    ) -> (i64, Position, i64) {
        let (at, pos) = self.lock_cut_here(now, replica);
        let barrier = match self.dl.gens.get(&gen).map(|g| g.dir) {
            Some(dir) => self.lock_barrier_under(dir, replica),
            None => 0,
        };
        if !self.lk.barriers.is_empty() || !self.lk.dir_barriers.is_empty() {
            self.lock_settle_barriers(at, &pos, replica);
        }
        (at, pos, barrier)
    }

    /// Delegate: every renewal sent in this event carries this node's
    /// executed stream head of its generation.
    fn lock_fill_renew_heads(&self, replica: &dyn Replica, out: &mut [Action]) {
        for a in out.iter_mut() {
            if let Action::Send {
                msg:
                    PeerMsg::DelegRenew {
                        gen, stream_head, ..
                    },
                ..
            } = a
            {
                *stream_head = replica.delegate_idx(*gen);
            }
        }
    }

    /// Remember that every inode under `dir` may have been released at
    /// `position` (see [`LockState::dir_floors`]).
    fn lock_note_dir_floor(&mut self, dir: Ino, position: &Position) {
        use constellation_meta::locks::floor_join;
        if *position == Position::ZERO {
            return;
        }
        match self.lk.dir_floors.iter_mut().find(|(d, _)| *d == dir) {
            Some((_, p)) => *p = floor_join(p, position),
            None => self.lk.dir_floors.push((dir, *position)),
        }
        if self.lk.dir_floors.len() > DIR_FLOORS_CAP {
            let all = self
                .lk
                .dir_floors
                .drain(..)
                .fold(Position::ZERO, |a, (_, p)| floor_join(&a, &p));
            self.lk
                .dir_floors
                .push((constellation_fs_core::types::ROOT_INO, all));
        }
        self.lk.floor_all = floor_join(&self.lk.floor_all, position);
        self.stats.lock_dir_floors += 1;
        self.lk.mirror_dirty = true;
    }

    /// The floor of the subtree under `dir`: its inodes' floors (those
    /// `under` accepts) and the directory floors over or under it.
    fn lock_subtree_floor(
        &self,
        dir: Ino,
        under: impl Fn(Ino) -> bool,
        replica: &dyn Replica,
    ) -> Position {
        use constellation_meta::locks::floor_join;
        let mut p = Position::ZERO;
        for (ino, f) in &self.lk.floors {
            if under(*ino) {
                p = floor_join(&p, f);
            }
        }
        for (d, f) in &self.lk.dir_floors {
            if *d == constellation_fs_core::types::ROOT_INO
                || replica.is_under(dir, *d)
                || replica.is_under(*d, dir)
            {
                p = floor_join(&p, f);
            }
        }
        p
    }

    fn lock_recall(&mut self, now: Ms, g: Grant, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lk.recalls.contains_key(&g.id) {
            return;
        }
        let timer = self.set_timer(Ms(g.until_ms), Timer::LockGrantExpiry(g.id), out);
        let req = if g.node == self.cfg.node_id {
            None
        } else {
            Some(self.op_id())
        };
        self.lk.recalls.insert(
            g.id,
            Recalling {
                ino: g.ino,
                req,
                timer,
            },
        );
        if let Some(req) = req {
            self.lk.recall_by_req.insert(req, g.id);
        }
        self.stats.lock_recalls_sent += 1;
        replica.locks().mark_recalled(g.id);
        tracing::debug!(
            node = self.cfg.node_id,
            holder = g.node,
            ino = g.ino,
            "recalling a lock grant"
        );
        match req {
            Some(req) => out.push(Action::Send {
                to: g.node,
                msg: PeerMsg::LockRecall {
                    req,
                    ino: g.ino,
                    grant: g.id,
                },
            }),
            // This node's own grant (the owner as a lock user): the same
            // path as a peer's recall, in place.
            None => self.lock_recall_here(now, g.ino, g.id, replica, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_park(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_waiters_parked += 1;
        self.lk.next_waiter += 1;
        let id = self.lk.next_waiter;
        // A node served before whose grant went unused keeps its place
        // in the queue (see `LockState::served`); its request never
        // stopped waiting from its point of view.
        let keep = 4 * self.lock_ttl_ms();
        self.lk.served.retain(|_, s| now.since(*s) < keep);
        let since = match self.lk.served.remove(&(from, ino)) {
            Some(s) => {
                self.stats.lock_requeued_in_place += 1;
                s
            }
            None => now,
        };
        self.lock_arm_waiter_tick(now, out);
        let held_timer = req.map(|_| {
            self.set_timer(
                now.plus(self.cfg.recall_hold_ms),
                Timer::LockHeldReply(id),
                out,
            )
        });
        self.lk.waiters.push(Waiter {
            id,
            node: from,
            ino,
            mode,
            req,
            sent,
            recv: now,
            op,
            held_timer,
            since,
        });
        if let Some(op) = op {
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.local_wait = true;
            }
        }
    }

    fn lock_arm_waiter_tick(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.lk.waiter_tick.is_some() {
            return;
        }
        let at = now.plus(self.cfg.recall_hold_ms.max(50));
        let t = self.set_timer(at, Timer::LockWaiterTick, out);
        self.lk.waiter_tick = Some(t);
    }

    pub(crate) fn on_lock_waiter_tick(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.waiter_tick = None;
        if self.lk.waiters.is_empty() {
            return;
        }
        self.lock_reserve_all(now, replica, out);
        if !self.lk.waiters.is_empty() {
            self.lock_arm_waiter_tick(now, out);
        }
    }

    /// A waiter's RPC has been held long enough: answer `Waiting` (the
    /// requester re-sends; the grant is pushed if it comes first).
    pub(crate) fn on_lock_held_reply_timer(&mut self, wid: u64, out: &mut Vec<Action>) {
        // Re-sent well inside the window a grant served from the re-send
        // would have (`ttl - margin` from its arrival): a waiter silent
        // for that long is skipped (`lock_serve_waiters`).
        let retry_ms = (self
            .cfg
            .lock_ttl_ms
            .saturating_sub(self.cfg.expiry_margin_ms)
            / 2)
        .max(50);
        // A waiter this owner cannot push to is granted only over a
        // request of its own: it asks again at once, so one is held here
        // nearly all the time.
        let short = self.lock_resume_retry_ms();
        let unreachable = &self.lk.unreachable;
        let Some(w) = self.lk.waiters.iter_mut().find(|w| w.id == wid) else {
            return;
        };
        let retry_ms = if unreachable.contains(&w.node) {
            short
        } else {
            retry_ms
        };
        w.held_timer = None;
        if let Some(req) = w.req.take() {
            self.stats.lock_waiting_replies += 1;
            out.push(Action::Send {
                to: w.node,
                msg: PeerMsg::LockReply {
                    req,
                    outcome: LockOutcome::Waiting { retry_ms },
                },
            });
        }
    }

    /// Something on `ino` changed hands: serve its waiters in order.
    fn lock_serve_waiters(
        &mut self,
        now: Ms,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // In the order they asked (`since`: a re-park keeps the old one),
        // whatever order they sit in the list.
        let mut idx: Vec<usize> = (0..self.lk.waiters.len())
            .filter(|i| self.lk.waiters[*i].ino == ino)
            .collect();
        idx.sort_by_key(|i| (self.lk.waiters[*i].since, self.lk.waiters[*i].id));
        let ttl = self.lock_ttl_ms();
        let margin = self.lock_margin_ms();
        let mut done = Vec::new();
        let mut gone = Vec::new();
        for i in idx {
            let (node, mode, req, op, sent, recv) = {
                let w = &self.lk.waiters[i];
                (w.node, w.mode, w.req, w.op, w.sent, w.recv)
            };
            // A remote waiter not heard from within its window (a live
            // one re-sends every `(ttl - margin) / 2`) would find a grant lapsed on
            // arrival and ask again anyway — or it is gone (killed while
            // parked): granting it would make everyone behind it wait out
            // `ttl + margin` for nothing, once per such waiter. Skipped,
            // not granted; a re-send re-attaches it in place, and one
            // silent for long is dropped (it re-parks if it ever asks).
            if op.is_none() && req.is_none() {
                if now.0 >= recv.0 + 4 * ttl {
                    gone.push(i);
                    continue;
                }
                if now.0 >= recv.0 + ttl - margin {
                    continue;
                }
                // A push would be lost; its next request carries its
                // grant. It was told to ask again at once, so the queue
                // waits for that request, briefly: passing it over then
                // and there let the waiter behind it take the turn
                // whenever the lock freed between two of its requests.
                // Silent for longer (its answers are lost too, or it is
                // gone), it is passed over, not pushed to: everyone
                // behind it waited out a lost push's grant otherwise,
                // once per push.
                if self.lk.unreachable.contains(&node) {
                    if now.0 < recv.0 + self.lock_unreachable_wait_ms() {
                        break;
                    }
                    self.stats.lock_unreachable_passed_over += 1;
                    continue;
                }
            }
            let base = if op.is_some() { now } else { recv };
            match self.lock_serve_again(now, base, node, ino, mode, replica, out) {
                Some(outcome) => {
                    done.push(i);
                    // A remote waiter's grant may go unused (see
                    // `LockState::served`); a local op installs its grant
                    // in this very event.
                    if let (None, LockOutcome::Granted { id, .. }) = (op, &outcome) {
                        let since = self.lk.waiters[i].since;
                        self.lk.served.insert((node, ino), since);
                        let locks = replica.locks();
                        self.lk.unproven.retain(|g| locks.get(*g).is_some());
                        self.lk.unproven.insert(*id);
                    }
                    self.lock_deliver(now, node, ino, req, op, sent, outcome, replica, out);
                }
                None => {
                    // Still conflicting (or in grace): stays parked.
                    // Nobody behind it is served either: a waiter that
                    // cannot be granted (its holder's recall is out) holds
                    // the queue, or the ones behind it would overtake it.
                    break;
                }
            }
        }
        if !gone.is_empty() {
            self.stats.lock_waiters_dropped += gone.len() as u64;
            tracing::debug!(
                node = self.cfg.node_id,
                ino,
                n = gone.len(),
                "dropped lock waiters silent for 4 × ttl"
            );
        }
        done.extend(gone);
        done.sort_unstable();
        for i in done.into_iter().rev() {
            let w = self.lk.waiters.remove(i);
            if let Some(t) = w.held_timer {
                self.cancel_timer(t, out);
            }
            let waited = now.since(w.since).max(0) as u64;
            self.stats.lock_wait_ms_total += waited;
        }
    }

    /// Try a parked request again: `Some` once it can be answered (a
    /// grant, or a refusal that ends the wait).
    #[allow(clippy::too_many_arguments)]
    fn lock_serve_again(
        &mut self,
        now: Ms,
        base: Ms,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<LockOutcome> {
        let (cap_ms, gen) = match self.lock_route_answering(now, ino, replica, from, false) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return Some(LockOutcome::NotOwner { owner: n }),
            Route::Unknown => return Some(LockOutcome::NotOwner { owner: 0 }),
        };
        let root_owned = self.lock_route_is_root(now, ino, replica);
        if root_owned
            && (!self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out))
        {
            return None;
        }
        if self.lock_grace_active(now, ino, replica) {
            return None;
        }
        match self.lock_try_grant(now, base, from, ino, mode, cap_ms, gen, out, replica) {
            Ok(outcome) => Some(outcome),
            Err(conflicting) => {
                for g in conflicting {
                    self.lock_recall(now, g, replica, out);
                }
                None
            }
        }
    }

    /// Answer a (formerly) parked request: over its RPC, by a push, or
    /// to this node's own op.
    #[allow(clippy::too_many_arguments)]
    fn lock_deliver(
        &mut self,
        now: Ms,
        node: NodeId,
        ino: Ino,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: Ms,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(op) = op {
            // A local waiter: the owner's clock is this node's, so the
            // grant's window starts now (sim seed 90017: stamped from a
            // request made seconds ago, it arrived lapsed).
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.sent_at = now;
            }
            let me = self.cfg.node_id;
            self.lock_op_outcome(now, op, me, outcome, replica, out);
            return;
        }
        match req {
            Some(req) => self.lock_answer(node, PeerMsg::LockReply { req, outcome }, out),
            None => self.lock_answer(node, PeerMsg::LockGranted { ino, sent, outcome }, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_released(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        grant: GrantId,
        position: Position,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Whether or not the grant is still ours to forget (it may have
        // been outwaited meanwhile), what its holder did under it must be
        // visible under the next grant.
        self.lock_note_floor(ino, &position);
        // A release that reaches this root after the inode's subtree was
        // delegated (it was routed by the releaser's older view): the
        // floor follows the subtree with the next granting renewal.
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if let Some(f) = self.lk.handed_floor.get_mut(&d.gen) {
                    *f = constellation_meta::locks::floor_join(f, &position);
                }
            }
        }
        // Released: the grant was used, so its holder's next request is
        // a new one (no queue position to keep; `LockState::served`).
        self.lk.served.remove(&(from, ino));
        self.lock_peer_heard(from, false);
        if replica
            .locks()
            .get(grant)
            .is_some_and(|g| g.node == from && g.ino == ino)
        {
            self.stats.lock_recalls_released += 1;
            self.lock_grant_done(now, grant, ino, replica, out);
            return;
        }
        // The node released an id this owner has since replaced: a
        // re-sent request re-affirmed its grant under a new id while the
        // old one was on its way there, and the node may hold nothing on
        // the inode any more. Or it may hold the new id: its *next*
        // request, sent after this release, overtook it, and the
        // re-affirmation answered it, so the new id has a lock under it
        // there (`locks-blips-tight-in-doubt` seed 1383: ended here, the
        // owner granted itself the inode beside it). So the new id is
        // recalled: a node that holds it releases it once its locks are
        // gone, one that cannot come to hold it answers the recall with
        // its release (`lock_recall_here`) — the waiter is served one
        // round trip later, not outwaited (`ttl + margin`). This node's
        // own release is never overtaken by its own next request: it
        // ends the grant at once.
        if let Some(g) = replica.locks().own_grant(ino, from, now.0) {
            if g.id.node == grant.node && g.id.seq > grant.seq {
                self.stats.lock_released_superseded += 1;
                if from == self.cfg.node_id {
                    self.stats.lock_recalls_released += 1;
                    self.lock_grant_done(now, g.id, ino, replica, out);
                } else {
                    self.lock_recall(now, g, replica, out);
                }
            }
        }
    }

    pub(crate) fn on_lock_recalled_ack(&mut self, from: NodeId, req: OpId) {
        // The recall arrived; the release (or the expiry) follows. The
        // node is reachable from here.
        if let Some(id) = self.lk.recall_by_req.remove(&req) {
            self.lock_peer_heard(from, true);
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.req = None;
            }
        }
    }

    pub(crate) fn on_lock_grant_expiry(
        &mut self,
        now: Ms,
        id: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(g) = replica.locks().get(id) else {
            // Replaced or released meanwhile: whatever waited on it is
            // served now (the sim found a waiter stranded here).
            if let Some(r) = self.lk.recalls.remove(&id) {
                if let Some(req) = r.req {
                    self.lk.recall_by_req.remove(&req);
                }
                self.lock_serve_waiters(now, r.ino, replica, out);
            }
            return;
        };
        if g.until_ms > now.0 {
            // Renewed since: wait again.
            let timer = self.set_timer(Ms(g.until_ms), Timer::LockGrantExpiry(id), out);
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.timer = timer;
            }
            return;
        }
        self.stats.lock_recalls_expired += 1;
        // A grant served from the queue that its node never renewed nor
        // released: it went unused (the push was lost, or found no op).
        if self.lk.unproven.remove(&id) {
            let n = self.lk.unused_grants.entry(g.node).or_insert(0);
            *n += 1;
            if *n >= UNUSED_GRANTS_UNREACHABLE {
                self.lock_peer_unreachable(g.node, "grants pushed to it went unused");
            }
        }
        // The holder never said what it did under the grant. What it was
        // acknowledged is in some sequencer's journal: this one's, at
        // least, is covered by what this node has now.
        let floor = self.lock_release_floor(replica);
        self.lock_note_floor(g.ino, &floor);
        // But not what it was acknowledged elsewhere: a barrier.
        if g.node != self.cfg.node_id {
            self.lock_barrier(g.ino, g.until_ms, Some(g.node));
        }
        tracing::info!(
            node = self.cfg.node_id,
            holder = g.node,
            ino = g.ino,
            "a lock grant's recall went unanswered; outwaited it (TTL + margin)"
        );
        self.lock_grant_done(now, id, g.ino, replica, out);
    }

    fn lock_grant_done(
        &mut self,
        now: Ms,
        id: GrantId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.locks().forget(id);
        self.lk.unproven.remove(&id);
        self.lk.mirror_dirty = true;
        if let Some(r) = self.lk.recalls.remove(&id) {
            self.cancel_timer(r.timer, out);
            if let Some(req) = r.req {
                self.lk.recall_by_req.remove(&req);
            }
        }
        self.lock_serve_waiters(now, ino, replica, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_renew(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        entries: Vec<LockRenewEntry>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut results = Vec::with_capacity(entries.len());
        for e in entries {
            let r = self.lock_renew_one(now, from, e.ino, e.grant, e.mode, replica);
            results.push((e.ino, e.grant, r));
        }
        self.lock_answer(from, PeerMsg::LockRenewed { req, results }, out);
        for gen in std::mem::take(&mut self.lk.deleg_renew_wanted) {
            self.deleg_renew_now(now, gen, out);
        }
    }

    fn lock_renew_one(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        id: GrantId,
        mode: LockMode,
        replica: &dyn Replica,
    ) -> LockRenewResult {
        // Renewed: the grant is in use (`LockState::served`).
        self.lk.served.remove(&(from, ino));
        self.lock_peer_heard(from, false);
        let (cap_ms, gen) = match self.lock_route_answering(now, ino, replica, from, true) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return LockRenewResult::NotOwner { owner: n },
            Route::Unknown => return LockRenewResult::NotOwner { owner: 0 },
        };
        let ttl = self.lock_ttl_ms().min(cap_ms);
        if ttl <= 0 {
            return LockRenewResult::NotOwner { owner: 0 };
        }
        if gen != 0 && ttl < self.lock_min_grant_ms() {
            // A delegate short of authority: renew the delegation now
            // (the caller does), so the next renewal gives more.
            self.lk.deleg_renew_wanted.insert(gen);
        }
        let until = now.0 + ttl + self.lock_margin_ms();
        // The grant renewed: that id, or this node's newer grant here (a
        // reply that never arrived replaced it: renew that one and say so
        // — sim seed 90013 fenced a healthy node otherwise).
        let target = match replica.locks().get(id) {
            Some(g) if g.node == from => Some(g.id),
            _ => replica.locks().own_grant(ino, from, now.0).map(|g| g.id),
        };
        if let Some(target) = target {
            // A renewal moves the end of the window a peer honours, so the
            // restart horizon moves with it, persisted before the answer as
            // a grant's is: a sequencer that restarted inside its lease
            // waited only for its last *grant*'s window, then regranted
            // while the holder still honoured its renewed one
            // (lock-fence-token review, `git-under-flock-faults` at 2 s
            // TTL). The answer waits for the write (`lock_answer`); one
            // that fails answers it `NotOwner { 0 }` and the holder
            // retries. Only for a grant the renewal extends (an unknown id
            // must not lengthen the next restart's quarantine), and rounded
            // up by a quarter TTL: a durable write per quarter TTL, not per
            // second, while peers hold grants. A node's own grants die with
            // its process and leave no horizon (`lock_try_grant`).
            // The renewal is applied to the table before its horizon is
            // durable; a failed write only makes the owner wait longer
            // (it refuses the answer, the table keeps the later expiry).
            self.lk.unproven.remove(&target);
            if let Some((mode, recalled)) = replica.locks().extend(target, from, until, now.0) {
                if from != self.cfg.node_id {
                    self.lock_need_horizon(until, until + ttl / 4);
                }
                self.stats.lock_renewals_served += 1;
                return LockRenewResult::Ok {
                    ttl_ms: ttl as u64,
                    recalled,
                    id: target,
                    mode,
                };
            }
        }
        // Unknown: a reclaim during a grace period, if nothing conflicts.
        let grace = self.lock_grace_active(now, ino, replica);
        let conflicting = replica.locks().conflicting(ino, from, mode, now.0);
        if grace && conflicting.is_empty() {
            if from != self.cfg.node_id {
                self.lock_need_horizon(until, until);
            }
            // Its holder's renewal: confirmed from now on.
            replica.locks().install(Grant {
                id,
                node: from,
                ino,
                mode,
                until_ms: until,
                recalled: false,
                gen,
                confirmed_ms: now.0,
            });
            self.stats.lock_reclaimed += 1;
            self.lk.mirror_dirty = true;
            tracing::info!(node = self.cfg.node_id, from, ino, "reclaimed a lock grant");
            return LockRenewResult::Ok {
                ttl_ms: ttl as u64,
                recalled: false,
                id,
                mode,
            };
        }
        LockRenewResult::Lost
    }

    // ------------------------------------------------------------ getlk

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_test(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let outcome = match self.lock_route_answering(now, ino, replica, from, false) {
            Route::Me { .. } => match replica.locks().first_conflicting(ino, from, mode, now.0) {
                Some(g) => LockTestOutcome::Held {
                    node: g.node,
                    mode: g.mode,
                },
                None => LockTestOutcome::Free,
            },
            Route::Node(n) => LockTestOutcome::NotOwner { owner: n },
            Route::Unknown => LockTestOutcome::NotOwner { owner: 0 },
        };
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LockTestReply { req, outcome },
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_test_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let answer =
                    match replica
                        .locks()
                        .first_conflicting(ino, self.cfg.node_id, mode, now.0)
                    {
                        Some(g) => LockTestAnswer::Held {
                            node: g.node,
                            mode: g.mode,
                        },
                        None => LockTestAnswer::Free,
                    };
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::LockTest(answer)),
                });
            }
            Route::Node(n) if self.cfg.p2p && self.reaches(now, n) => {
                let req = self.op_id();
                self.lk.tests.insert(op, req);
                self.lk.test_by_req.insert(req, op);
                self.set_timer(
                    now.plus(self.cfg.forward_timeout_ms),
                    Timer::LockTestTimeout(req),
                    out,
                );
                out.push(Action::Send {
                    to: n,
                    msg: PeerMsg::LockTest { req, ino, mode },
                });
            }
            _ => out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::LockTest(LockTestAnswer::Free)),
            }),
        }
    }

    pub(crate) fn on_lock_test_reply(
        &mut self,
        req: OpId,
        outcome: LockTestOutcome,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.test_by_req.remove(&req) else {
            return;
        };
        if self.lk.tests.remove(&op).is_none() {
            return;
        }
        let answer = match outcome {
            LockTestOutcome::Held { node, mode } => LockTestAnswer::Held { node, mode },
            // Best effort: `getlk` is advisory.
            LockTestOutcome::Free | LockTestOutcome::NotOwner { .. } => LockTestAnswer::Free,
        };
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::LockTest(answer)),
        });
    }

    pub(crate) fn on_lock_test_timeout(&mut self, req: OpId, out: &mut Vec<Action>) {
        self.on_lock_test_reply(req, LockTestOutcome::Free, out);
    }

    // ------------------------------------------------------------ node side

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_requests += 1;
        self.lk.ops.insert(
            op,
            LockOp {
                ino,
                mode,
                blocking,
                req: None,
                owner: 0,
                sent_at: now,
                attempts: 0,
                since: now,
                local_wait: false,
                reread: false,
            },
        );
        self.lock_route_op(now, op, replica, out);
    }

    fn lock_route_op(&mut self, now: Ms, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        let (ino, mode, blocking) = (o.ino, o.mode, o.blocking);
        // A grant this node holds that is recalled or being released:
        // no request leaves until it is gone (a reply for it could cross
        // its own `LockReleased` and reinstall a forgotten grant — sim
        // seeds 90000/90004). A non-blocking request while local locks
        // still pin the recalled grant would block: say so.
        if let Some(h) = replica.locks().held(ino) {
            if h.until_ms <= now.0 {
                // Lapsed: it is fenced already and cannot be renewed;
                // drop it (the owner outwaits it) and ask afresh.
                replica.locks().drop_held(ino, h.id);
            } else if h.recalled || h.releasing {
                if !blocking && !replica.locks().local_idle(ino) {
                    self.stats.lock_would_block += 1;
                    self.lk.ops.remove(&op);
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Lock(LockAnswer::WouldBlock)),
                    });
                    return;
                }
                self.set_timer(now.plus(10), Timer::LockRetry(op), out);
                return;
            }
        }
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let me = self.cfg.node_id;
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.sent_at = now;
                    o.owner = me;
                }
                match self.lock_serve(
                    now,
                    me,
                    ino,
                    mode,
                    blocking,
                    None,
                    Some(op),
                    now,
                    replica,
                    out,
                ) {
                    Served::Outcome(outcome) => {
                        self.lock_op_outcome(now, op, me, outcome, replica, out)
                    }
                    Served::Parked => {}
                }
            }
            Route::Node(n) => {
                if !self.cfg.p2p || !self.reaches(now, n) {
                    if !self.lock_reread_holder(op, out) {
                        self.lock_op_unreachable(now, op, out);
                    }
                    return;
                }
                let req = self.op_id();
                let Some(o) = self.lk.ops.get_mut(&op) else {
                    return;
                };
                o.req = Some(req);
                o.owner = n;
                o.sent_at = now;
                o.attempts += 1;
                self.lk.by_req.insert(req, op);
                self.set_timer(
                    now.plus(self.cfg.forward_timeout_ms),
                    Timer::LockRequestTimeout(req),
                    out,
                );
                out.push(Action::Send {
                    to: n,
                    msg: PeerMsg::LockRequest {
                        req,
                        ino,
                        mode,
                        blocking,
                        sent: now,
                        incarnation: self.cfg.incarnation,
                    },
                });
            }
            Route::Unknown => {
                if self.lock_op_while_resuming(now, op, replica, out) {
                    return;
                }
                if !self.cfg.p2p {
                    self.lock_op_unreachable(now, op, out);
                    return;
                }
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.attempts += 1;
                }
                self.issue_s3(S3Op::LeaseGet, super::S3For::LockHolder(op), out);
            }
        }
    }

    /// No P2P path to the owner: a non-blocking request fails
    /// (`ENOLCK`); a blocking one keeps trying.
    fn lock_op_unreachable(&mut self, now: Ms, op: OpId, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if o.blocking && self.cfg.p2p {
            self.lock_retry(now, op, out);
            return;
        }
        self.stats.lock_unavailable += 1;
        self.lk.ops.remove(&op);
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Lock(LockAnswer::Unavailable)),
        });
    }

    /// The holder this node asked (or would ask) cannot be reached: it
    /// may be stale — it died, and a successor took the lease before
    /// gossip said so (the harness's lock-failover: a non-blocking `flock`
    /// answered `ENOLCK` while the backup already served). Re-read the
    /// lease, once per request, as for an unknown holder, before a
    /// non-blocking request gives up. `false`: read already (or no P2P).
    fn lock_reread_holder(&mut self, op: OpId, out: &mut Vec<Action>) -> bool {
        if !self.cfg.p2p {
            return false;
        }
        let Some(o) = self.lk.ops.get_mut(&op).filter(|o| !o.reread) else {
            return false;
        };
        o.reread = true;
        o.attempts += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            ino = o.ino,
            "lock holder unreachable: re-reading the lease before giving up"
        );
        self.issue_s3(S3Op::LeaseGet, super::S3For::LockHolder(op), out);
        true
    }

    fn lock_retry(&mut self, now: Ms, op: OpId, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if !o.blocking && o.attempts >= self.cfg.forward_retries {
            // Out of attempts against the holder this node knew (its
            // requests timed out before the link was declared dead).
            if self.lock_reread_holder(op, out) {
                return;
            }
            self.stats.lock_unavailable += 1;
            self.lk.ops.remove(&op);
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lock(LockAnswer::Unavailable)),
            });
            return;
        }
        let delay = (self.cfg.forward_backoff_ms / 4).max(10) * u64::from(o.attempts.clamp(1, 8));
        self.set_timer(now.plus(delay), Timer::LockRetry(op), out);
    }

    pub(crate) fn on_lock_retry(
        &mut self,
        now: Ms,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self
            .lk
            .ops
            .get(&op)
            .is_some_and(|o| o.req.is_none() && !o.local_wait)
        {
            return;
        }
        self.lock_route_op(now, op, replica, out);
    }

    pub(crate) fn on_lock_holder_learned(
        &mut self,
        now: Ms,
        op: OpId,
        result: crate::event::S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.lk.ops.contains_key(&op) {
            return;
        }
        if let crate::event::S3Result::LeaseGet(Ok(Some((lease, _)))) = &result {
            self.lease.note_object(now, lease);
            if lease.holder != 0 && !lease.is_claimable(now.0) && lease.holder != self.cfg.node_id {
                self.lease.cached_holder = Some(lease.holder);
                self.lock_route_op(now, op, replica, out);
                return;
            }
        }
        // This node is the owner again in a moment (an epoch's close let
        // the lease go and its re-claim is pending, S3 maybe cut again
        // before it landed; or its own acquisition's gate is pending).
        if self.lock_op_while_resuming(now, op, replica, out) {
            return;
        }
        if matches!(&result, crate::event::S3Result::LeaseGet(Err(_))) {
            if let Some(owner) = self.lock_owner_without_s3(now) {
                self.lease.cached_holder = Some(owner);
                self.lock_route_op(now, op, replica, out);
                return;
            }
        }
        // Nobody usable holds it: a lock needs a sequencer as a write
        // does — acquire (the sim found blocked lockers with nobody
        // taking the lease). Non-blocking: unavailable after the retries.
        let claimable = matches!(&result, crate::event::S3Result::LeaseGet(Ok(l))
            if l.as_ref().is_none_or(|(l, _)| l.holder == 0 || l.is_claimable(now.0)));
        if claimable && !self.lease.lost {
            self.enqueue_job(
                now,
                super::jobs::JobReq::Acquire {
                    reason: "lock",
                    ask_handoff: self.cfg.p2p,
                },
                replica,
                out,
            );
        }
        self.lock_retry(now, op, out);
    }

    /// A local request while this node is about to own the root again
    /// (`lock_owner_resuming`): its own grant table answers a conflict
    /// (`WouldBlock` for a non-blocking request), anything else waits for
    /// the lease — the re-claim queued if it is one — without spending
    /// the request's attempts, up to the bound an op without its
    /// sequencer has (`s3_less_deadline_ms`). Routed as before, the
    /// request asked the lease, which names this node (or S3 was cut
    /// again, or P2P is off), and a non-blocking lock failed `ENOLCK`
    /// once its retries ran out (`stress-ng-fs-faults`). `false`: not
    /// resuming, or past the bound — route it as any other.
    fn lock_op_while_resuming(
        &mut self,
        now: Ms,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.lock_owner_resuming(now) {
            return false;
        }
        let Some(o) = self.lk.ops.get(&op) else {
            return true;
        };
        if now.0 - o.since.0 >= self.cfg.s3_less_deadline_ms as i64 {
            return false;
        }
        let (ino, mode, blocking) = (o.ino, o.mode, o.blocking);
        let me = self.cfg.node_id;
        if let Some(outcome) = self.lock_conflict_refusal(now, me, ino, mode, blocking, replica) {
            self.lock_op_outcome(now, op, me, outcome, replica, out);
            return true;
        }
        if self.epoch_reclaim_pending(now) {
            self.enqueue_job(
                now,
                super::jobs::JobReq::Acquire {
                    reason: "lock",
                    ask_handoff: self.cfg.p2p,
                },
                replica,
                out,
            );
        }
        let retry = self.lock_resume_retry_ms();
        self.set_timer(now.plus(retry), Timer::LockRetry(op), out);
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            self.lock_late_reply(now, from, req, outcome, replica);
            return;
        };
        if !self.lk.ops.get(&op).is_some_and(|o| o.req == Some(req)) {
            return;
        }
        self.note_p2p_result(now, from, true);
        if let Some(o) = self.lk.ops.get_mut(&op) {
            o.req = None;
        }
        self.lock_op_outcome(now, op, from, outcome, replica, out);
    }

    /// The reply to a request a push already answered: the owner
    /// re-affirmed the grant under a new id (`lock_try_grant` mints one
    /// per answer). Installed over the id held here — merged, the local
    /// locks and the recalled flag kept (`LockTables::install_held`) —
    /// so the id this node holds is the one the owner will recall and
    /// expects released. Dropped, the owner's recall found no holder
    /// here and the grant was outwaited.
    fn lock_late_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        outcome: LockOutcome,
        replica: &dyn Replica,
    ) {
        let Some(ino) = self.lk.done_reqs.remove(&req) else {
            return;
        };
        let LockOutcome::Granted {
            id,
            mode,
            ttl_ms,
            position,
        } = outcome
        else {
            return;
        };
        let Some(cur) = replica.locks().held(ino) else {
            return;
        };
        if cur.id.node != id.node || cur.id.seq > id.seq || cur.owner != from {
            return;
        }
        let margin = self.lock_margin_ms();
        let ttl = ttl_ms as i64;
        if cur.id == id {
            // The owner's recall of the new id came first and adopted it
            // (`LockTables::recall_held`): the reply extends that grant
            // like a renewal answer, its local locks and flags kept.
            // Dropped, the grant kept the window of the push it replaced,
            // counted from the waiter's first request, and lapsed under
            // the I/O it was finishing before its release (sim
            // `locks-blips-tight-long-lease` seed 3270: fenced 236 ms
            // after the push's install).
            replica
                .locks()
                .renewed(ino, id, id, mode, now.0, ttl, margin, false);
            self.stats.lock_late_replies_installed += 1;
            return;
        }
        let held = HeldGrant {
            id,
            mode,
            until_ms: now.0 + ttl - margin,
            renew_at_ms: constellation_meta::locks::renew_point(now.0, ttl, margin),
            owner: from,
            recalled: false,
            position,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: now.0,
        };
        if matches!(
            replica.locks().install_held(ino, held),
            Installed::Ok { .. }
        ) {
            self.stats.lock_late_replies_installed += 1;
        }
    }

    /// A grant pushed to a waiter whose request was answered `Waiting`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_granted_push(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        sent: Ms,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // The op is matched by inode and owner; the window counts from
        // the echoed send (this clock, the request the push answers) or
        // the op's current send, whichever is earlier — a push queued
        // behind a pause answers an older attempt (sim seed 191008).
        let granted_mode = match &outcome {
            LockOutcome::Granted { mode, .. } => Some(*mode),
            _ => None,
        };
        // By inode and mode; the op asked at `from` first, else any op
        // for the inode: the owner this op last asked may not be the
        // owner that serves the queue (a delegation made or recalled
        // meanwhile, a `NotOwner` redirect in flight). A push for an
        // inode nobody here waits for is the only one dropped — the
        // grant is then outwaited by its owner, so dropping one that
        // *is* wanted costs everyone a window (EC2 campaign 8).
        let candidates = || {
            self.lk
                .ops
                .iter()
                .filter(|(_, o)| o.ino == ino && granted_mode.is_none_or(|m| m.covers(o.mode)))
        };
        let op = candidates()
            .find(|(_, o)| o.owner == from)
            .or_else(|| candidates().next())
            .map(|(op, _)| *op);
        let Some(op) = op else {
            // No op waits for it: the grant is unknown here; the owner's
            // recall will find no holder and outwait it, or its next
            // request replaces it.
            return;
        };
        if let Some(o) = self.lk.ops.get_mut(&op) {
            if o.owner != from {
                self.stats.lock_pushes_from_other_owner += 1;
                o.owner = from;
            }
            if let Some(req) = o.req.take() {
                self.lk.by_req.remove(&req);
                // Its reply, if the owner answers it too, installs the
                // id the owner then tracks (`on_lock_reply`).
                self.lk.done_reqs.insert(req, ino);
                while self.lk.done_reqs.len() > DONE_REQS_KEPT {
                    self.lk.done_reqs.pop_first();
                }
            }
            if sent < o.sent_at {
                o.sent_at = sent;
            }
        }
        self.lock_op_outcome(now, op, from, outcome, replica, out);
    }

    fn lock_op_outcome(
        &mut self,
        now: Ms,
        op: OpId,
        from: NodeId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let margin = self.lock_margin_ms();
        let Some(o) = self.lk.ops.get_mut(&op) else {
            return;
        };
        o.local_wait = false;
        match outcome {
            LockOutcome::Granted {
                id,
                mode,
                ttl_ms,
                position,
            } => {
                let ttl = ttl_ms as i64;
                if o.sent_at.0 + ttl - margin <= now.0 + margin / 4 {
                    // Lapsed on arrival (a request held longer than the
                    // window), or all but: ask again rather than install
                    // a fence. A grant whose window ends before a renewal
                    // could come back lapses under the I/O it lets in
                    // (the window counts from the request's send, and a
                    // waiter served from the queue near the end of its
                    // re-send interval under a delegate's capped ttl got
                    // 5–20 ms of it: `locks-delegated-writes`, chunk
                    // delegate-fenced-io). The owner answers the request
                    // again at once (it re-affirms the grant this node
                    // holds there), from its fresh send.
                    self.lock_retry(now, op, out);
                    return;
                }
                let held = HeldGrant {
                    id,
                    mode,
                    until_ms: o.sent_at.0 + ttl - margin,
                    renew_at_ms: constellation_meta::locks::renew_point(o.sent_at.0, ttl, margin),
                    owner: from,
                    recalled: false,
                    position,
                    renewing: None,
                    releasing: false,
                    first_use: false,
                    idle_since_ms: None,
                    installed_ms: now.0,
                };
                let ino = o.ino;
                let waited = now.since(o.since).max(0) as u64;
                let recalled = match replica.locks().install_held(ino, held) {
                    Installed::Ok { recalled } => recalled,
                    Installed::Released => {
                        // This node released that id already (the reply
                        // crossed the release): ask again.
                        self.stats.lock_released_replies += 1;
                        self.lock_retry(now, op, out);
                        return;
                    }
                };
                self.lk.ops.remove(&op);
                self.stats.lock_grant_ms_total += waited;
                let b = (64 - waited.max(1).leading_zeros()).min(13) as usize;
                self.stats.lock_grant_ms[b] += 1;
                if held.renew_at_ms <= now.0 {
                    // Served late in its window (a push to a request
                    // parked a while): renew now, not at the next tick,
                    // which may come after the window.
                    if let Some(t) = self.lk.renew_timer.take() {
                        self.cancel_timer(t, out);
                    }
                    let t = self.set_timer(now.plus(1), Timer::LockRenewTick, out);
                    self.lk.renew_timer = Some(t);
                    self.lk.renew_at = Some(now.plus(1));
                } else {
                    self.lock_arm_renew_tick(now, replica, out);
                }
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Lock(LockAnswer::Granted { position })),
                });
                if recalled {
                    // Recalled before it arrived: the FUSE thread takes
                    // its local lock first (the grant covers it until the
                    // last local lock leaves), then `LockIdle` releases.
                    self.stats.lock_granted_recalled += 1;
                }
            }
            LockOutcome::Waiting { retry_ms } => {
                // A non-blocking request is told to wait only by an owner
                // about to own the root again (`lock_serve`): within the
                // bound an op without its sequencer has; past it, as
                // `Busy`.
                let late = !o.blocking && now.0 - o.since.0 >= self.cfg.s3_less_deadline_ms as i64;
                if late {
                    self.lock_retry(now, op, out);
                } else {
                    self.set_timer(now.plus(retry_ms), Timer::LockRetry(op), out);
                }
            }
            LockOutcome::WouldBlock => {
                self.lk.ops.remove(&op);
                out.push(Action::ControlDone {
                    op,
                    result: Ok(ControlOk::Lock(LockAnswer::WouldBlock)),
                });
            }
            LockOutcome::NotOwner { owner } => {
                let ino = o.ino;
                self.lock_note_not_owner(from, ino, owner, replica);
                if owner != 0 && owner != from && owner != self.cfg.node_id {
                    let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                    if !names_delegate {
                        self.lease.cached_holder = Some(owner);
                    }
                } else if from != self.cfg.node_id
                    && (owner == 0 || self.lease.cached_holder == Some(from))
                {
                    // The node asked as the owner names no other one (or
                    // names this node, or itself): what this node cached
                    // is stale; the next attempt reads the lease rather
                    // than ask it again.
                    self.lease.cached_holder = None;
                }
                self.lock_retry(now, op, out);
            }
            LockOutcome::Busy => self.lock_retry(now, op, out),
        }
    }

    pub(crate) fn on_lock_request_timeout(&mut self, now: Ms, req: OpId, out: &mut Vec<Action>) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            return;
        };
        if let Some(o) = self.lk.ops.get_mut(&op) {
            if o.req == Some(req) {
                o.req = None;
                self.lock_retry(now, op, out);
            }
        }
    }

    /// A lock request, test, recall or renewal failed at the transport.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request_failed(
        &mut self,
        now: Ms,
        req: OpId,
        to: NodeId,
        outage: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if let Some(id) = self.lk.recall_by_req.remove(&req) {
            // The grant's expiry answers an undelivered recall. With no
            // connection left to the holder, a push would not arrive
            // either.
            if let Some(r) = self.lk.recalls.get_mut(&id) {
                r.req = None;
            }
            if outage {
                self.lock_peer_unreachable(to, "a recall could not be delivered");
            }
            return true;
        }
        if let Some(op) = self.lk.by_req.remove(&req) {
            self.note_p2p_result(now, to, !outage);
            if let Some(o) = self.lk.ops.get_mut(&op) {
                if o.req == Some(req) {
                    o.req = None;
                }
                // Twice unanswered: the holder this node remembers may be
                // gone; the next attempt reads the lease.
                if o.attempts >= 2 && self.lease.cached_holder == Some(to) {
                    self.lease.cached_holder = None;
                }
            }
            self.lock_retry(now, op, out);
            return true;
        }
        if self.lk.test_by_req.contains_key(&req) {
            self.on_lock_test_timeout(req, out);
            return true;
        }
        if let Some(r) = self.lk.renews.remove(&req) {
            tracing::debug!(
                node = self.cfg.node_id,
                to,
                outage,
                "lock renewal failed at the transport"
            );
            self.cancel_timer(r.timer, out);
            self.note_p2p_result(now, to, !outage);
            for (ino, id) in r.entries {
                replica.locks().renewal_failed(ino, id);
            }
            self.lock_relearn_owner(out);
            return true;
        }
        false
    }

    // ------------------------------------------------------------ renewals

    /// Arm the renewal tick: at the ttl/4 cadence, or sooner when a held
    /// grant's renewal point comes first. A delegate's grants can be
    /// short (capped by what is left of its delegation), with a window
    /// under the cadence: a tick at the cadence alone came after it
    /// closed, and the grant lapsed under the application's lock.
    fn lock_arm_renew_tick(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let mut at = now.plus((self.cfg.lock_ttl_ms / 4).max(50));
        if let Some(first) = replica.locks().next_renewal_ms(now.0) {
            at = at.min(Ms(first.max(now.0 + 1)));
        }
        at = at.max(self.lock_relearn_floor(now));
        if let Some((t, armed)) = self.lk.renew_timer.zip(self.lk.renew_at) {
            if armed <= at {
                return;
            }
            self.cancel_timer(t, out);
        }
        let t = self.set_timer(at, Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
        self.lk.renew_at = Some(at);
    }

    pub(crate) fn on_lock_renew_tick(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.renew_timer = None;
        self.lk.renew_at = None;
        let mut relearn = false;
        if !self.lk.deleg_moved.is_empty() {
            let table = replica.delegation_table();
            self.lk
                .deleg_moved
                .retain(|gen, _| table.iter().any(|e| e.gen == *gen));
        }
        // A cache nobody locked for a while is given back (its renewals
        // would otherwise go on forever).
        let idle = replica
            .locks()
            .idle_before(now.0 - self.cfg.lock_cache_idle_ms as i64);
        for (ino, _) in idle {
            self.stats.lock_idle_released += 1;
            self.lock_release_begin(now, ino, replica, out);
        }
        // A recalled grant whose first local lock never came (the
        // requester gave up, or is gone) stops being pinned by it: it is
        // released below, not renewed for ever under the waiters.
        for ino in replica.locks().expire_first_use(now.0) {
            tracing::info!(
                node = self.cfg.node_id,
                ino,
                "a recalled lock grant was never used by its requester; releasing it"
            );
        }
        // A recalled grant with no local lock under it is released, not
        // renewed; a lapsed one is dropped (sim seed 90010: a grant that
        // answered an op it did not cover was neither used nor released).
        for (ino, h) in replica.locks().held_all() {
            if h.until_ms <= now.0 {
                replica.locks().drop_held(ino, h.id);
            } else if h.recalled && !h.releasing && replica.locks().local_idle(ino) {
                self.lock_release_begin(now, ino, replica, out);
            }
        }
        let due = replica.locks().due_renewals(now.0);
        if !due.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                due = due.len(),
                cached_holder = ?self.lease.cached_holder,
                "lock renewals due"
            );
        }
        let mut by_owner: BTreeMap<NodeId, Vec<(Ino, HeldGrant)>> = BTreeMap::new();
        for (ino, h) in due {
            // Routed as a peer's renewal is (`lock_renew_one`): a lease
            // whose gate is pending serves renewals. Its own lockers'
            // found no owner and spun until their grants lapsed under
            // their I/O (a re-claim whose marker S3 cut again).
            match self.lock_route_for(now, ino, replica, true) {
                Route::Me { .. } => {
                    let me = self.cfg.node_id;
                    let r = self.lock_renew_one(now, me, ino, h.id, h.mode, replica);
                    self.lock_apply_renew_result(now, me, ino, h.id, now, r, replica, out);
                    for gen in std::mem::take(&mut self.lk.deleg_renew_wanted) {
                        self.deleg_renew_now(now, gen, out);
                    }
                }
                Route::Node(n) if self.cfg.p2p && self.reaches(now, n) => {
                    by_owner.entry(n).or_default().push((ino, h));
                }
                route => {
                    // The owner is unreachable or unknown (a failover: the
                    // holder this node last heard of is gone): learn the
                    // current one from the lease and renew there (the
                    // harness's lock-failover found renewals sent to a
                    // dead holder until the grant was outwaited).
                    tracing::debug!(
                        node = self.cfg.node_id,
                        ino,
                        unreachable = matches!(route, Route::Node(_)),
                        "lock renewal: no reachable owner; relearning"
                    );
                    replica.locks().renewal_failed(ino, h.id);
                    relearn = true;
                }
            }
        }
        if relearn {
            self.lock_relearn_owner(out);
        }
        for (to, entries) in by_owner {
            let req = self.op_id();
            let timer = self.set_timer(
                now.plus(self.cfg.forward_timeout_ms),
                Timer::LockRenewTimeout(req),
                out,
            );
            self.lk.renews.insert(
                req,
                RenewInFlight {
                    entries: entries.iter().map(|(i, h)| (*i, h.id)).collect(),
                    sent: now,
                    timer,
                    to,
                },
            );
            tracing::debug!(
                node = self.cfg.node_id,
                to,
                n = entries.len(),
                "sending lock renewals"
            );
            out.push(Action::Send {
                to,
                msg: PeerMsg::LockRenew {
                    req,
                    entries: entries
                        .into_iter()
                        .map(|(ino, h)| LockRenewEntry {
                            ino,
                            grant: h.id,
                            mode: h.mode,
                        })
                        .collect(),
                },
            });
        }
        if replica.locks().held_count() > 0 || !self.lk.renews.is_empty() {
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Re-read the lease to learn the current holder for renewals (once
    /// at a time). `reaches` is no guide here: a dead holder's link stays
    /// "connected" until the transport notices, and a renewal that fails
    /// must not wait for that (the harness's lock-failover outwaited a
    /// live locker this way).
    /// The earliest the renewal tick fires again while the lease read for
    /// the owner (`lock_relearn_owner`) is in flight. The grants that
    /// found no owner keep their renewal points (past), and the tick armed
    /// from them fired every millisecond for as long as the read took — a
    /// whole S3 cut long. Not later than the retry cadence of a lock
    /// request: an owner learned meanwhile otherwise (a push, a hint, a
    /// lock reply) is used at the next tick; the read's answer re-ticks at
    /// once.
    fn lock_relearn_floor(&self, now: Ms) -> Ms {
        if self.lk.relearning {
            now.plus(self.lock_resume_retry_ms())
        } else {
            now
        }
    }

    /// The owner to ask when the lease could not be read (S3 cut): the
    /// active continuation epoch's carrier, or the holder of the last
    /// lease object seen while that object has not expired here. Only a
    /// node to *ask*: it answers from its own table, and `NotOwner`
    /// clears it again (`cached_holder`). With no owner known, a cut
    /// lasting longer than a grant's window lapsed it under its holder's
    /// I/O although the root renewing it was reachable over P2P (a
    /// holder whose stale delegate answered `NotOwner { 0 }` and whose
    /// table no longer named a delegate: `locks-blips-tight-delegated`
    /// seed 157, chunk delegate-fenced-io).
    pub(crate) fn lock_owner_without_s3(&self, now: Ms) -> Option<NodeId> {
        let me = self.cfg.node_id;
        let carrier = self
            .pr
            .carried
            .filter(|_| self.epoch.active)
            .map(|c| c.node);
        let seen = self
            .lease
            .last_seen
            .as_ref()
            .filter(|l| !l.is_claimable(now.0))
            .map(|l| l.holder);
        carrier.or(seen).filter(|n| *n != 0 && *n != me)
    }

    fn lock_relearn_owner(&mut self, out: &mut Vec<Action>) {
        if self.lk.relearning {
            return;
        }
        self.lk.relearning = true;
        self.issue_s3(S3Op::LeaseGet, super::S3For::LockRenewHolder, out);
    }

    /// The lease read a renewal tick asked for: the holder to renew with.
    pub(crate) fn on_lock_renew_holder(
        &mut self,
        now: Ms,
        result: crate::event::S3Result,
        out: &mut Vec<Action>,
    ) {
        self.lk.relearning = false;
        tracing::debug!(
            node = self.cfg.node_id,
            ok = matches!(&result, crate::event::S3Result::LeaseGet(Ok(Some(_)))),
            "lock renewal: lease read"
        );
        if let crate::event::S3Result::LeaseGet(Ok(Some((lease, _)))) = &result {
            self.lease.note_object(now, lease);
            if lease.holder != 0 && !lease.is_claimable(now.0) && lease.holder != self.cfg.node_id {
                self.lease.cached_holder = Some(lease.holder);
            }
        } else if matches!(&result, crate::event::S3Result::LeaseGet(Err(_))) {
            if let Some(owner) = self.lock_owner_without_s3(now) {
                self.lease.cached_holder = Some(owner);
            }
        }
        // Renew at once with what was learned.
        if let Some(t) = self.lk.renew_timer.take() {
            self.cancel_timer(t, out);
        }
        let t = self.set_timer(now.plus(1), Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
        self.lk.renew_at = Some(now.plus(1));
    }

    pub(crate) fn on_lock_renewed(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.lk.renews.remove(&req) else {
            self.lock_late_renewed(now, from, req, results, replica, out);
            return;
        };
        self.cancel_timer(r.timer, out);
        self.note_p2p_result(now, from, true);
        let sent = r.sent;
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            ?results,
            "lock renewals answered"
        );
        for (ino, id, result) in results {
            self.lock_apply_renew_result(now, from, ino, id, sent, result, replica, out);
        }
        // The answer may carry a shorter window than the tick allows for.
        if replica.locks().held_count() > 0 {
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_apply_renew_result(
        &mut self,
        now: Ms,
        from: NodeId,
        ino: Ino,
        id: GrantId,
        sent: Ms,
        result: LockRenewResult,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match result {
            LockRenewResult::Ok {
                ttl_ms,
                recalled,
                id: now_id,
                mode,
            } => {
                replica.locks().renewed(
                    ino,
                    id,
                    now_id,
                    mode,
                    sent.0,
                    ttl_ms as i64,
                    self.lock_margin_ms(),
                    recalled,
                );
                self.stats.lock_renewals += 1;
                if recalled {
                    self.lock_release_begin(now, ino, replica, out);
                }
            }
            LockRenewResult::Lost => {
                if replica.locks().lost(ino, id) {
                    self.stats.lock_lost += 1;
                    tracing::warn!(
                        node = self.cfg.node_id,
                        ino,
                        "lock grant lost (the owner does not know it); I/O under local locks is fenced"
                    );
                }
            }
            LockRenewResult::NotOwner { owner } => {
                let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                if owner != 0 && owner != self.cfg.node_id && !names_delegate {
                    self.lease.cached_holder = Some(owner);
                }
                self.lock_note_not_owner(from, ino, owner, replica);
                replica.locks().renewal_failed(ino, id);
            }
        }
    }

    pub(crate) fn on_lock_renew_timeout(
        &mut self,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(r) = self.lk.renews.remove(&req) {
            self.cancel_timer(r.timer, out);
            for (ino, id) in r.entries {
                replica.locks().renewal_failed(ino, id);
            }
            let sent = r.sent;
            self.lk
                .late_renews
                .retain(|_, l| sent.since(l.sent) < LATE_RENEW_MS);
            self.lk
                .late_renews
                .insert(req, LateRenew { to: r.to, sent });
            self.lock_relearn_owner(out);
        }
    }

    /// The answer to a renewal [`LateRenew`] stopped waiting for: only
    /// its granted renewals count — honoured from that renewal's send
    /// (`LockTable::renewed` never shortens a grant). A `Lost` or
    /// `NotOwner` late is left to the renewal sent since: an owner that
    /// gave the inode away after answering this one would have the grant
    /// dropped that its successor renewed meanwhile.
    fn lock_late_renewed(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Only from the owner it went to (another node's answer to the
        // same id leaves it for that one).
        let late = match self.lk.late_renews.entry(req) {
            std::collections::btree_map::Entry::Occupied(e) if e.get().to == from => e.remove(),
            _ => return,
        };
        self.note_p2p_result(now, from, true);
        let granted: Vec<_> = results
            .into_iter()
            .filter(|(_, _, r)| matches!(r, LockRenewResult::Ok { .. }))
            .collect();
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            late_ms = now.since(late.sent),
            granted = granted.len(),
            "late lock renewals answered"
        );
        for (ino, id, result) in granted {
            self.lock_apply_renew_result(now, from, ino, id, late.sent, result, replica, out);
        }
        if replica.locks().held_count() > 0 {
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    // ------------------------------------------------------------ recalls (node)

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_recall(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::LockRecalled { req },
        });
        // A newer id of the grant this node released last on the inode,
        // with nothing held or installed there since and no op here
        // waiting on it: the id that superseded the released one at the
        // owner (`on_lock_released`). Nothing ran under it here, so it
        // is released now rather than outwaited — and tombstoned: a push
        // of it may still be in flight, and a push installs for any
        // later op on the inode (sim `locks-failover` seed 5297: the
        // recall overtook the push, a new op took the late push, two
        // exclusive holders); refused, that op asks again. A grant this
        // node held and lost (lapsed) is not: what ran under it may still
        // be in flight, and its owner waits that out. With an op waiting, the
        // grant may be its answer, in flight: the reply installs it
        // recalled, as below.
        if replica.locks().held(ino).is_none()
            && replica
                .locks()
                .released_last(ino)
                .is_some_and(|r| r.node == grant.node && r.seq < grant.seq)
            && !self.lk.ops.values().any(|o| o.ino == ino)
        {
            self.stats.lock_recalls_unheld_released += 1;
            replica.locks().tombstone_unheld(grant);
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LockReleased {
                    ino,
                    grant,
                    position: Position::ZERO,
                },
            });
            return;
        }
        self.lock_recall_here(now, ino, grant, replica, out);
    }

    fn lock_recall_here(
        &mut self,
        now: Ms,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match replica.locks().recall_held(ino, grant) {
            Some(false) => self.lock_release_begin(now, ino, replica, out),
            Some(true) => {
                // Local locks under it: released when the last one
                // leaves (`Control::LockIdle`).
            }
            None => {
                // Not held (yet): the reply that brings it installs it
                // recalled (the model's rule; answering "released" here
                // livelocks two contenders).
                replica.locks().note_pending_recall(ino, grant);
            }
        }
    }

    /// The last local lock under a recalled grant left.
    pub(crate) fn on_lock_idle_control(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lock_release_begin(now, ino, replica, out);
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Done),
        });
    }

    /// Phase 2: the end of the latest token window naming `grant` among
    /// this node's own queued replays (stranded ops not resolved yet), if
    /// one is still open at `now`.
    fn replay_blocks_release(grant: GrantId, now: Ms, replica: &dyn Replica) -> Option<i64> {
        let queued = replica.pending_replays().ok()?;
        queued
            .iter()
            .filter(|q| q.refused.is_none() && !q.foreign)
            .flat_map(|q| q.lock_tag.0.iter())
            .filter(|t| t.grant == grant && t.until_ms > now.0)
            .map(|t| t.until_ms)
            .max()
    }

    /// Phase 2: the last tagged mutation in flight under `ino`'s recalled
    /// grant was answered (`LockTables::set_release_wake`): a release
    /// waiting for it looks again now, not at its next poll.
    pub(crate) fn on_lock_release_wake(
        &mut self,
        now: Ms,
        op: OpId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(&(grant, _)) = self.lk.release_waiting.get(&ino) {
            self.on_lock_flushed(now, ino, grant, true, replica, out);
        }
        out.push(Action::ControlDone {
            op,
            result: Ok(ControlOk::Done),
        });
    }

    /// Start releasing `ino`'s grant: flush its dirty data first.
    fn lock_release_begin(
        &mut self,
        now: Ms,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(h) = replica.locks().begin_release(ino) else {
            return;
        };
        self.lk.flushing.insert(ino, 0);
        let _ = now;
        out.push(Action::LockFlush { ino, grant: h.id });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_flushed(
        &mut self,
        now: Ms,
        ino: Ino,
        grant: GrantId,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !ok {
            let attempts = self.lk.flushing.entry(ino).or_insert(0);
            *attempts += 1;
            if *attempts < 5 {
                out.push(Action::LockFlush { ino, grant });
                return;
            }
            tracing::warn!(
                node = self.cfg.node_id,
                ino,
                "releasing a recalled lock grant without a successful flush (its expiry would drop it anyway)"
            );
        }
        // Plan 30 §M14 phase 2, release ordering: no release while a
        // mutation tagged with this grant is in flight, or one left in
        // doubt could still execute (until its token's window is over,
        // when every executor refuses it). Released earlier, such an op
        // could land after the next holder's writes at a sequencer that
        // only checks the window. The grant is not renewed meanwhile
        // (recalled, nothing pins it): if the wait outlasts its window it
        // lapses here, and its owner outwaits it.
        if replica.locks().held(ino).is_some_and(|h| h.id == grant) {
            // A stranded op tagged with the grant, queued for replay by
            // rid after a holder change, is in flight too: a release now
            // would let it land, at an executor that only checks the
            // window, after the next holder's writes. Waited for until it
            // is resolved or its token's window is over.
            let blocked = replica.locks().release_blocked(grant, now.0).or_else(|| {
                Self::replay_blocks_release(grant, now, replica)
                    .map(|until| Some(until.min(now.0 + RELEASE_WAIT_POLL_MS)))
            });
            if let Some(until) = blocked {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ino,
                    ?grant,
                    ?until,
                    "a recalled grant's release waits for mutations tagged with it"
                );
                let at = until.unwrap_or(now.0 + RELEASE_WAIT_POLL_MS);
                let timer = self.set_timer(
                    Ms(at.max(now.0 + 1)),
                    Timer::LockReleaseWait(ino, grant),
                    out,
                );
                match self.lk.release_waiting.insert(ino, (grant, timer)) {
                    None => replica.locks().with_stats(|s| s.release_waits += 1),
                    Some((_, old)) => self.cancel_timer(old, out),
                }
                return;
            }
        }
        if let Some((_, timer)) = self.lk.release_waiting.remove(&ino) {
            self.cancel_timer(timer, out);
        }
        self.lk.flushing.remove(&ino);
        if !replica.locks().end_release(ino, grant) {
            // Gone already, or a local lock (or the grant's first use)
            // appeared meanwhile: it stays, recalled; the last unlock
            // releases it again (sim seed 90106).
            return;
        }
        self.stats.lock_released += 1;
        let position = self.lock_release_floor(replica);
        match self.lock_route(now, ino, replica) {
            Route::Me { .. } => {
                let me = self.cfg.node_id;
                self.on_lock_released(now, me, ino, grant, position, replica, out);
            }
            Route::Node(n) => out.push(Action::Send {
                to: n,
                msg: PeerMsg::LockReleased {
                    ino,
                    grant,
                    position,
                },
            }),
            Route::Unknown if self.lock_owner_resuming(now) => {
                // An epoch's close kept this node's grant table for the
                // re-claim (`lock_keep_grants_at_close`): the grant is in
                // it, and it is this node's to end. A release that waited
                // through the close for tagged mutations (above) was
                // dropped here, and the grant outwaited (`ttl + margin`)
                // before its waiter was served.
                let me = self.cfg.node_id;
                self.on_lock_released(now, me, ino, grant, position, replica, out);
            }
            Route::Unknown => {
                // The owner (whoever it is) outwaits it.
            }
        }
    }

    // ------------------------------------------------------------ mirror

    pub(crate) fn on_lock_mirror(
        &mut self,
        from: NodeId,
        ver: u64,
        grants: Vec<Grant>,
        floor: Position,
    ) {
        if self.root_node() != Some(from) {
            return;
        }
        if self.lk.bk_mirror.as_ref().is_none_or(|(v, _)| ver > *v) {
            self.lk.bk_mirror = Some((ver, grants));
            // Floors only grow: joined across mirrors.
            self.lk.bk_floor = constellation_meta::locks::floor_join(&self.lk.bk_floor, &floor);
        }
    }

    /// A fast takeover (this node was the backup): the mirror is this
    /// tenure's table, restamped; M9's floor covers what it misses.
    pub(crate) fn lock_install_mirror(&mut self, now: Ms, replica: &dyn Replica) {
        let floor = std::mem::take(&mut self.lk.bk_floor);
        self.lock_note_dir_floor(constellation_fs_core::types::ROOT_INO, &floor);
        let Some((_, grants)) = self.lk.bk_mirror.take() else {
            return;
        };
        let until = self.restamp(now);
        let n = grants.len();
        for g in grants {
            // Unconfirmed: the mirror is asynchronous, and the previous
            // holder may have ended a grant it still lists (released, or
            // outwaited, and granted to another node since).
            replica.locks().install(Grant {
                until_ms: until,
                recalled: false,
                confirmed_ms: Grant::UNCONFIRMED,
                ..g
            });
        }
        if n > 0 {
            tracing::info!(
                node = self.cfg.node_id,
                grants = n,
                "installed the lock mirror"
            );
        }
        self.lk.mirror_dirty = true;
    }

    // ------------------------------------------------------------ delegation moves

    /// Root: a delegation was granted; its subtree's grants go with it
    /// (handed over with the delegate's first renewal).
    pub(crate) fn lock_on_delegated(&mut self, gen: u64, replica: &dyn Replica) {
        if self.lk.tenure_floor_due && self.lk.tenure_waiting.is_none() {
            self.lk.tenure_minted.insert(gen);
        }
        let moved = replica.locks().take_where(|ino| {
            let keys = Self::read_keys(ino, None);
            matches!(
                replica.resolve_ownership(&keys),
                constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
            )
        });
        if !moved.is_empty() {
            self.lk.mirror_dirty = true;
            self.lk.handoff.entry(gen).or_default().extend(moved);
        }
        // The subtree's lock floor goes with it (with every granting
        // renewal: a join, so a resend is harmless).
        if let Some(dir) = self.dl.gens.get(&gen).map(|g| g.dir) {
            let floor = self.lock_subtree_floor(
                dir,
                |ino| {
                    let keys = Self::read_keys(ino, None);
                    matches!(
                        replica.resolve_ownership(&keys),
                        constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
                    )
                },
                replica,
            );
            self.lk.handed_floor.insert(gen, floor);
        }
    }

    /// Delegate: the live grants of generation `gen` on inodes that are
    /// no longer in any delegated subtree here — unlinked under their
    /// lock (`stress-ng`'s lock stressors unlink their files while they
    /// hold them). By location such an inode is the root's now, and its
    /// holder renews there; the root, which never had the grant, answered
    /// `Lost` (overload-cascade-2: the holder's writes discarded, `EIO`)
    /// — or, adopting it at that renewal instead, could have granted the
    /// inode to another node first (routed to the root by location too)
    /// and then adopted the first holder's grant once that one released:
    /// two exclusive holders. So the grants go to the root with the batch
    /// that carries the unlink ([`Core::lock_install_leaving`]): the root
    /// keeps routing the inode here until it applies that row, and has
    /// the grants from the same step on. This node no longer serves the
    /// inode (it routes it to the root), so nothing changes them
    /// meanwhile but a lapse.
    pub(crate) fn deleg_leaving_grants(
        &self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
    ) -> Vec<Grant> {
        if replica.locks().grants_len() == 0 {
            return Vec::new();
        }
        replica
            .locks()
            .grants_snapshot()
            .into_iter()
            .filter(|g| {
                g.gen == gen
                    && g.until_ms > now.0
                    && matches!(
                        replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                        constellation_meta::delegation::Ownership::Root
                    )
            })
            .collect()
    }

    /// Delegate: the outwait barriers that go to the root with a batch of
    /// `gen`, on the inodes no longer in its subtree (unlinked): those on
    /// the inode itself — an expired, unreleased record still in the
    /// table counts, as nothing may have dropped it yet — joined with the
    /// barriers on the subtree, which no longer cover an inode outside
    /// it. Without them the root granted an unlinked inode whose last
    /// holder this delegate had outwaited with only its own position
    /// (sim `locks-unlinked-delegated-partition` seed 4067).
    pub(crate) fn deleg_leaving_barriers(
        &mut self,
        now: Ms,
        gen: u64,
        leaving: &[Grant],
        replica: &dyn Replica,
    ) -> Vec<(Ino, i64)> {
        self.lock_take_outwaited(replica);
        let me = self.cfg.node_id;
        for g in replica.locks().expired(now.0) {
            if g.gen == gen && g.node != me {
                self.lock_barrier(g.ino, g.until_ms, Some(g.node));
            }
        }
        let root_owned = |ino: Ino| {
            matches!(
                replica.resolve_ownership(&Self::read_keys(ino, None)),
                constellation_meta::delegation::Ownership::Root
            )
        };
        let mut inos: Vec<Ino> = self
            .lk
            .barriers
            .keys()
            .copied()
            .filter(|ino| root_owned(*ino))
            .collect();
        inos.extend(leaving.iter().map(|g| g.ino));
        inos.sort_unstable();
        inos.dedup();
        if inos.is_empty() {
            return Vec::new();
        }
        let subtree = match self.dl.mine.get(&gen).map(|d| d.dir) {
            Some(dir) => self.lock_barrier_under(dir, replica),
            None => 0,
        };
        inos.into_iter()
            .filter_map(|ino| {
                let at = self
                    .lk
                    .barriers
                    .get(&ino)
                    .map_or(0, |(a, _)| *a)
                    .max(subtree);
                (at > 0).then_some((ino, at))
            })
            .collect()
    }

    /// Root: a batch of `gen`, applied through its last row, carried
    /// `barriers` on inodes it took out of the subtree
    /// ([`Core::deleg_leaving_barriers`]; whose records they were is not
    /// said): kept where the inode is this
    /// table's now.
    pub(crate) fn lock_install_leaving_barriers(
        &mut self,
        barriers: Vec<(Ino, i64)>,
        replica: &dyn Replica,
    ) {
        for (ino, at) in barriers {
            if matches!(
                replica.resolve_ownership(&Self::read_keys(ino, None)),
                constellation_meta::delegation::Ownership::Root
            ) {
                self.lock_barrier(ino, at, None);
            }
        }
    }

    /// Delegate: the root acknowledged the batch that carried grants out
    /// of the subtree ([`Core::deleg_leaving_grants`]): they are the
    /// root's, and leave this table (a recall must not hand them back
    /// over the root's own record, which may have ended them since).
    pub(crate) fn deleg_drop_left(&mut self, gen: u64, replica: &dyn Replica) {
        let Some(d) = self.dl.mine.get_mut(&gen) else {
            return;
        };
        let through = d.streamed_through;
        let Some((_, inos)) = d.leaving.take_if(|(last, _)| *last <= through) else {
            return;
        };
        for ino in &inos {
            self.lk.barriers.remove(ino);
        }
        let gone = replica.locks().take_where(|ino| inos.contains(&ino));
        if !gone.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                n = gone.len(),
                "lock grants on inodes that left the subtree are the root's now"
            );
        }
    }

    /// Root: a delegate's batch of `gen`, applied through its last row,
    /// carried `leaving` ([`Core::deleg_leaving_grants`]) — installed
    /// restamped, as a move is, where the inode is this table's now (or
    /// still the sending generation's: a later row the delegate has not
    /// streamed yet takes it out, and the delegate no longer serves it).
    /// A grant this table has already (its own copy, put back by
    /// [`Core::lock_take_back_left`]) is extended to the restamped end:
    /// the delegate renewed it past what this root recorded. A grant this
    /// table ended (the batch re-sent after its holder released it here)
    /// or one next to a conflicting grant stays out.
    pub(crate) fn lock_install_leaving(
        &mut self,
        now: Ms,
        gen: u64,
        leaving: Vec<Grant>,
        replica: &dyn Replica,
    ) {
        let until = self.restamp(now);
        let mut n = 0;
        for g in leaving {
            let here = match replica.resolve_ownership(&Self::read_keys(g.ino, None)) {
                constellation_meta::delegation::Ownership::Root => true,
                constellation_meta::delegation::Ownership::Delegated(d) => d.gen == gen,
                _ => false,
            };
            if !here || replica.locks().was_ended(g.id) {
                continue;
            }
            if let Some(e) = replica.locks().get(g.id) {
                if e.node == g.node && e.ino == g.ino && e.until_ms < until {
                    replica.locks().install(Grant {
                        until_ms: until,
                        ..e
                    });
                }
                continue;
            }
            let g = Grant {
                until_ms: until,
                gen: 0,
                ..g
            };
            if replica.locks().install_if_consistent(g, now.0) {
                n += 1;
            }
        }
        if n > 0 {
            self.stats.lock_moved += n;
            self.stats.lock_leaving_installed += n;
            self.lk.mirror_dirty = true;
            tracing::info!(
                node = self.cfg.node_id,
                gen,
                n,
                "installed a delegate's lock grants on inodes that left its subtree"
            );
        }
    }

    /// Root: rows of generation `gen` were applied here; grants this root
    /// moved to the generation itself — waiting for its first renewal
    /// ([`LockState::handoff`]) or handed and not handed back
    /// ([`LockState::handed`]) — on an inode those rows took out of the
    /// subtree (unlinked under the lock) come back to this table as it
    /// recorded them, unconfirmed (the delegate may have ended one). The
    /// delegate cannot carry them ([`Core::deleg_leaving_grants`]) if they
    /// never reached it (sim `locks-unlinked-delegated` seed 292: a
    /// recall and re-delegation put the grant in the new generation's
    /// handoff, the delegate executed the unlink before its first
    /// renewal, and the root granted the unlinked inode over it); one it
    /// did install and renew comes with its batch and extends this copy
    /// ([`Core::lock_install_leaving`]). Nor are they handed to the
    /// delegate after this: it no longer serves the inode.
    pub(crate) fn lock_take_back_left(&mut self, now: Ms, gen: u64, replica: &dyn Replica) {
        let left = |g: &Grant| {
            matches!(
                replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                constellation_meta::delegation::Ownership::Root
            )
        };
        let mut back = Vec::new();
        for moved in [self.lk.handoff.get_mut(&gen), self.lk.handed.get_mut(&gen)]
            .into_iter()
            .flatten()
        {
            moved.retain(|g| {
                if left(g) {
                    back.push(*g);
                    false
                } else {
                    true
                }
            });
        }
        let mut n = 0;
        for g in back {
            let g = Grant {
                gen: 0,
                confirmed_ms: Grant::UNCONFIRMED,
                ..g
            };
            if g.until_ms > now.0
                && !replica.locks().was_ended(g.id)
                && replica.locks().install_if_consistent(g, now.0)
            {
                n += 1;
            }
        }
        if n > 0 {
            self.stats.lock_reinstated += n;
            self.lk.mirror_dirty = true;
            tracing::info!(
                node = self.cfg.node_id,
                gen,
                n,
                "grants moved to a delegation came back with their inodes, which left its subtree"
            );
        }
    }

    /// Root: the lock floor a granting renewal of `gen` carries.
    pub(crate) fn lock_floor_for_generation(&self, gen: u64) -> Position {
        self.lk.handed_floor.get(&gen).copied().unwrap_or_default()
    }

    /// Delegate: a granting renewal carried the subtree's floor.
    pub(crate) fn lock_take_floor(&mut self, gen: u64, floor: &Position) {
        if *floor == Position::ZERO {
            return;
        }
        let Some(dir) = self.dl.mine.get(&gen).map(|d| d.dir) else {
            return;
        };
        self.lock_note_dir_floor(dir, floor);
    }

    /// Delegate: the generation is recalled — its grants and its
    /// subtree's floor go back with the answer.
    pub(crate) fn lock_hand_back(
        &mut self,
        gen: u64,
        replica: &dyn Replica,
    ) -> constellation_meta::locks::LockHandback {
        let dir = self.dl.mine.get(&gen).map(|d| d.dir);
        let grants = self.lock_take_generation(gen, replica);
        let (floor, barrier) = match dir {
            Some(dir) => (
                self.lock_subtree_floor(dir, |ino| replica.is_under(ino, dir), replica),
                self.lock_barrier_under(dir, replica),
            ),
            None => (Position::ZERO, 0),
        };
        constellation_meta::locks::LockHandback {
            grants,
            floor,
            barrier,
        }
    }

    /// Root: what to hand a delegate with a granting renewal — what is
    /// waiting for its first one, and again every grant handed to it
    /// before that this root would still consider live (see
    /// [`LockState::handed`]; the delegate installs each id once).
    pub(crate) fn lock_take_handoff(&mut self, now: Ms, gen: u64) -> Vec<Grant> {
        let pending = self.lk.handoff.remove(&gen).unwrap_or_default();
        let handed = self.lk.handed.entry(gen).or_default();
        handed.extend(pending);
        handed.retain(|g| g.until_ms > now.0);
        let grants = handed.clone();
        if grants.is_empty() {
            self.lk.handed.remove(&gen);
        }
        grants
    }

    /// Root: generation `gen` ended. What was waiting for its first
    /// renewal, and what was handed to it and not handed back, returns to
    /// this table as this root recorded it (the holders measure their
    /// windows against the grants it made or renewed; nobody renewed a
    /// grant the delegate never installed, and one it did install and
    /// renew came back with its drained answer — or was capped by its
    /// tenure, which an outwait waited out). A grant here of the same node
    /// on the same inode, or a conflicting one, is newer and stays (see
    /// `LockTables::install_if_consistent`).
    pub(crate) fn lock_on_generation_ended(&mut self, now: Ms, gen: u64, replica: &dyn Replica) {
        let mut back = self.lk.handoff.remove(&gen).unwrap_or_default();
        back.extend(self.lk.handed.remove(&gen).unwrap_or_default());
        let mut n = 0;
        for g in back {
            // This root's copy, unconfirmed: the delegate may have ended
            // the grant (and granted another) before the generation ended.
            let g = Grant {
                gen: 0,
                confirmed_ms: Grant::UNCONFIRMED,
                ..g
            };
            if g.until_ms > now.0 && replica.locks().install_if_consistent(g, now.0) {
                n += 1;
            }
        }
        if n > 0 {
            self.stats.lock_reinstated += n;
            self.lk.mirror_dirty = true;
            tracing::info!(
                node = self.cfg.node_id,
                gen,
                n,
                "a delegation ended without handing back grants this root handed it; reinstated"
            );
        }
    }

    /// Root: a delegate's drained answer handed `grants` back — installed
    /// restamped; the root's copies of them are done. An answer that
    /// comes once the generation's window is outwaited (past `until`,
    /// sealing, or ended) here brings no grants back.
    pub(crate) fn lock_install_returned(
        &mut self,
        now: Ms,
        gen: u64,
        back: constellation_meta::locks::LockHandback,
        replica: &dyn Replica,
    ) {
        let constellation_meta::locks::LockHandback {
            grants,
            floor,
            barrier,
        } = back;
        let live = match self.dl.gens.get(&gen) {
            Some(g) => {
                // Past its window the delegation's grants have lapsed at
                // their holders and the outwait's grace stands (or is
                // about to: an expiry retry, `!root_usable`); a seal
                // pushed `until` on but began after the outwait.
                let outwaited =
                    g.ended || now >= g.until || g.recall == super::delegate::RecallPhase::Sealing;
                let dir = g.dir;
                self.lock_note_dir_floor(dir, &floor);
                self.lock_dir_barrier(dir, barrier);
                !outwaited
            }
            None => false,
        };
        if !live {
            // Whatever outwaited or ended the generation accounted for its grants: an
            // outwait or a seal waited out the delegation's window, which
            // caps every grant it made or renewed (and left a grace on
            // the subtree); a drained answer already handed them back; a
            // lost lease took this tenure's table with it. A late answer
            // (the delegate paused past its window, then answered the
            // recall it found queued) carries copies nobody honours any
            // more. Restamped, they would come back to life beside the
            // subtree's next generation and keep its grants out at an
            // unlink (sim `locks-unlinked-delegated-partition` seed 7455:
            // the root then upgraded the revived copy for its old holder
            // beside the delegate's live exclusive grant).
            if !grants.is_empty() {
                self.stats.lock_returned_after_end += grants.len() as u64;
                tracing::debug!(
                    node = self.cfg.node_id,
                    gen,
                    n = grants.len(),
                    "a recall answer after its generation ended; its grants dropped"
                );
            }
            return;
        }
        if let Some(handed) = self.lk.handed.get_mut(&gen) {
            handed.retain(|h| !grants.iter().any(|g| g.node == h.node && g.ino == h.ino));
        }
        // A grant on an inode that left the subtree is this table's since
        // the row that took it out (it came with that row, or was this
        // root's own copy, `lock_take_back_left`): the delegate's copy may
        // be older than what this root made of it since (sim
        // `locks-unlinked-delegated` seed 6475: the root upgraded it to
        // exclusive, the recall answer put the delegate's shared copy
        // back over it, and the root granted another node shared).
        let (left, grants): (Vec<Grant>, Vec<Grant>) = grants.into_iter().partition(|g| {
            matches!(
                replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                constellation_meta::delegation::Ownership::Root
            )
        });
        if !left.is_empty() {
            self.lock_install_leaving(now, gen, left, replica);
        }
        if !grants.is_empty() {
            self.lock_install_moved(now, 0, grants, replica);
        }
    }

    /// Delegate: grants that came with a renewal or, root: with a recall
    /// answer — installed restamped.
    pub(crate) fn lock_install_moved(
        &mut self,
        now: Ms,
        gen: u64,
        grants: Vec<Grant>,
        replica: &dyn Replica,
    ) {
        if gen != 0 && self.deleg_mine_until(gen).is_none() {
            // A handoff for a generation this node no longer serves (its
            // recall overtook the renewal reply carrying it, and was
            // answered without it): the root reinstates what it handed
            // when the generation ends. Installed here, the grants would
            // be dropped at that end anyway.
            if !grants.is_empty() {
                tracing::debug!(
                    node = self.cfg.node_id,
                    gen,
                    n = grants.len(),
                    "a lock handoff for a generation not served here; left to the root"
                );
            }
            return;
        }
        let until = self.restamp(now);
        let mut n = 0;
        for g in grants {
            // Tagged with the generation the move names (0: the root's
            // table) — never resolved from the delegation table, which
            // may not say yet (a handoff arrives with the renewal that
            // installs the generation; sim seed 196252 tagged it 0 and
            // lost it at the next recall).
            let g = Grant {
                until_ms: until,
                gen,
                ..g
            };
            if gen == 0 {
                replica.locks().install(g);
            } else {
                // A re-sent handoff: each id once, and never over a
                // newer grant of the same node, or next to a conflicting
                // one, here.
                if !self.lk.moved_seen.entry(gen).or_default().insert(g.id) {
                    continue;
                }
                if !replica.locks().install_if_consistent(g, now.0) {
                    continue;
                }
            }
            self.stats.lock_moved += 1;
            n += 1;
        }
        if n > 0 {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                n,
                "installed moved lock grants"
            );
        }
        self.lk.mirror_dirty = true;
    }

    /// Delegate: the generation is being recalled (or ended): its
    /// subtree's grants leave this table (handed back, or dropped).
    pub(crate) fn lock_take_generation(&mut self, gen: u64, replica: &dyn Replica) -> Vec<Grant> {
        // By tag, not by the table: a generation the log has already
        // ended resolves to nobody (sim seed 96046 left its grants
        // behind).
        self.lk.moved_seen.remove(&gen);
        let mut taken = replica.locks().take_by_gen(gen);
        let by_tag = taken.len();
        taken.extend(replica.locks().take_where(|ino| {
            let keys = Self::read_keys(ino, None);
            matches!(
                replica.resolve_ownership(&keys),
                constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
            )
        }));
        if !taken.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                by_tag,
                by_table = taken.len() - by_tag,
                "took a generation's lock grants"
            );
        }
        taken
    }

    /// The table changed hands for some inodes (a move, a lease change):
    /// every waiter is tried again, which answers `NotOwner` to those
    /// whose owner is elsewhere now.
    pub(crate) fn lock_reserve_all(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut inos: Vec<Ino> = self.lk.waiters.iter().map(|w| w.ino).collect();
        inos.sort_unstable();
        inos.dedup();
        for ino in inos {
            self.lock_serve_waiters(now, ino, replica, out);
        }
    }

    /// Root: a generation ended by TTL (an outwaited delegate): the
    /// grants moved to it may still be honoured with the windows this
    /// root gave them — a grace on the subtree until they lapse or
    /// reclaim.
    pub(crate) fn lock_on_generation_outwaited(&mut self, now: Ms, dir: Ino) {
        let until = Ms(self.restamp(now));
        self.lk.grace.push((dir, until));
        // Its holders' releases are lost with it: what they could have
        // seen is at most what this root has once it appended the
        // delegate's stream — noted at the next event.
        self.lk.pending_dir_floors.push(dir);
        // Nor did they say what they were acknowledged in other
        // generations' streams: a barrier on the subtree.
        self.lock_dir_barrier(dir, now.0);
        self.stats.lock_grace_periods += 1;
        // What was not handed over yet stays the generation's until it
        // ends: [`Core::lock_on_generation_ended`] returns it to this
        // table as recorded, live ones only, before a re-delegation of
        // the subtree takes the table's grants along. Returned here at
        // the next event instead, it came back restamped — lapsed grants
        // revived — after the same step had re-delegated the subtree, so
        // it sat in this table on inodes the new generation serves; such
        // a record of a holder kept that holder's grant out when it came
        // back with an unlink (`lock_install_leaving`), and this root
        // granted the inode over it (sim `locks-unlinked-delegated-
        // dbackup-random` seed 5681).
    }

    /// Root: what is left of a grace here (a subtree grace overlapping
    /// generation `gen`'s directory, the whole-namespace grace after a
    /// released takeover, the restart or takeover quarantine) — carried
    /// with the generation's granting renewals. The grants such a grace
    /// protects are ones this root cannot hand over (it never knew them),
    /// so a delegate that starts serving the subtree inside it must
    /// honour it too; left here, the new delegate granted over them (the
    /// sim's `locks-released-delegated`).
    pub(crate) fn lock_grace_for_generation(
        &mut self,
        now: Ms,
        gen: u64,
        replica: &dyn Replica,
    ) -> u64 {
        let Some(dir) = self.dl.gens.get(&gen).map(|g| g.dir) else {
            return 0;
        };
        self.lk.grace.retain(|(_, until)| *until > now);
        let mut until = Self::lock_quarantine_until(replica);
        for (g, u) in &self.lk.grace {
            if *g == constellation_fs_core::types::ROOT_INO
                || replica.is_under(dir, *g)
                || replica.is_under(*g, dir)
            {
                until = until.max(u.0);
            }
        }
        (until - now.0).max(0) as u64
    }

    /// Delegate: generation `gen`'s first granting renewal carried the
    /// root's remaining grace on the subtree: no new grant there until
    /// it passes (measured from the receipt, which is after the root's
    /// send, plus the margin), reclaims accepted meanwhile.
    pub(crate) fn lock_take_grace(&mut self, now: Ms, gen: u64, grace_ms: u64) {
        if grace_ms == 0 {
            return;
        }
        let Some(dir) = self.dl.mine.get(&gen).map(|d| d.dir) else {
            return;
        };
        let until = Ms(now.0 + grace_ms as i64 + self.lock_margin_ms());
        self.lk.grace.push((dir, until));
        self.stats.lock_grace_periods += 1;
        self.stats.lock_graces_inherited += 1;
        tracing::info!(
            node = self.cfg.node_id,
            gen,
            dir,
            grace_ms,
            "a delegation starts inside the root's lock grace; no new grants on the subtree until it passes"
        );
    }

    /// A takeover of a lease its holder *released* (not expired): its
    /// grants were capped by a lease still live, so they may be honoured
    /// for up to `ttl` more — a grace on everything, reclaims admitted
    /// (the sim found a successor granting at once). The whole-namespace
    /// quarantine, not this tenure's `grace`: the grants are not this
    /// tenure's, so they do not end with it. Kept per tenure, the grace
    /// went with the successor's own release a moment later (an epoch's
    /// flush), and its re-claim of the lease it had released — no
    /// takeover — granted over the predecessor's live exclusive grant
    /// (`locks-blips-tight` seed 2723).
    /// The predecessor's grants end unreleased, as an outwait does, and
    /// their holders may write into delegates' streams until then: a
    /// barrier on everything as of the quarantine's end (the tenure's
    /// floor takes the inherited delegates' *first* renewals, which may
    /// be older than those writes).
    pub(crate) fn lock_on_released_takeover(&mut self, now: Ms, replica: &dyn Replica) {
        let until = self.restamp(now);
        replica.locks().set_quarantine(until);
        self.lock_dir_barrier(constellation_fs_core::types::ROOT_INO, until);
        self.stats.lock_grace_periods += 1;
    }

    /// The tenure ends and its grant table is dropped (`lock_on_lease_gone`
    /// without a close that keeps it): grants still live there may be
    /// honoured by their holders until they lapse, and so may grants a
    /// grace protects (an outwaited delegate's). A successor that takes
    /// the released lease over waits them out (`lock_on_released_takeover`);
    /// this node re-claiming its own released lease is no takeover, so it
    /// waits them out here: no new grant until the last one has lapsed.
    /// Also on a deposal or an expiry, where no re-claim follows: harmless
    /// there, and conservative — the quarantine is node-wide, so a
    /// subtree's grace widens to the whole namespace until it lapses.
    fn lock_quarantine_dropped_tenure(&mut self, now: Ms, replica: &dyn Replica) {
        let grants = replica
            .locks()
            .grants_snapshot()
            .into_iter()
            .map(|g| g.until_ms)
            .max()
            .unwrap_or(0);
        let grace = self.lk.grace.iter().map(|(_, u)| u.0).max().unwrap_or(0);
        let until = grants.max(grace);
        if until > now.0 {
            replica.locks().set_quarantine(until);
            // Those grants end unreleased (see `lock_on_released_takeover`).
            self.lock_dir_barrier(constellation_fs_core::types::ROOT_INO, until);
            self.stats.lock_grace_periods += 1;
        }
    }

    pub(crate) fn locks_start(&mut self, now: Ms, replica: &dyn Replica) {
        self.lk.tenure_floor_due = true;
        replica
            .locks()
            .seed_ids(u64::from(self.cfg.incarnation) << 40);
        // Phase 2: an ended grant's id is refused for as long as a token
        // naming it could still be within its window somewhere.
        replica
            .locks()
            .set_token_memory_ms(self.lock_ttl_ms() + 2 * self.lock_margin_ms());
        if !self.cfg.locks {
            return;
        }
        if let Some(until) = replica.load_lock_quarantine(now.0) {
            tracing::info!(
                node = self.cfg.node_id,
                wait_ms = until - now.0,
                "lock grants made before this restart may still be honoured; \
                 no new lock grant until they have expired (parked requests are \
                 served by the waiter tick then)"
            );
            // They all end unreleased, as an outwait does, and their
            // holders may write into delegates' streams until then: a
            // barrier on everything as of that end.
            self.lock_dir_barrier(constellation_fs_core::types::ROOT_INO, until);
        }
    }

    /// A continuation epoch closes and this node's next acquisition may
    /// continue the tenure it lets go: the next `lock_on_lease_gone`
    /// keeps the grant table (see `Core::epoch_close_release`).
    pub(crate) fn lock_keep_grants_at_close(&mut self) {
        self.lk.keep_grants = true;
    }

    /// The lease is gone (deposed, released, an epoch closed): this
    /// tenure's grants are void (capped by the lease), its waiters retry
    /// elsewhere. An epoch's close keeps the grants when the lease's
    /// re-claim may continue the tenure (`lock_keep_grants_at_close`).
    pub(crate) fn lock_on_lease_gone(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Kept at an epoch's close (`Core::epoch_close_release`). Else
        // the tenure is over: nothing continues it any more.
        let kept = std::mem::take(&mut self.lk.keep_grants);
        if !kept {
            self.lock_quarantine_dropped_tenure(now, replica);
            self.pr.closed_tenure.clear();
            self.pr.closed_lease = None;
            let n = replica.locks().clear_grants();
            if n > 0 {
                tracing::info!(
                    node = self.cfg.node_id,
                    grants = n,
                    "lease gone: lock grants dropped"
                );
            }
        }
        for (_, r) in std::mem::take(&mut self.lk.recalls) {
            self.cancel_timer(r.timer, out);
        }
        self.lk.recall_by_req.clear();
        self.lk.handoff.clear();
        self.lk.handed.clear();
        self.lk.handed_floor.clear();
        self.lk.grace.clear();
        self.lk.unused_grants.clear();
        self.lk.unproven.clear();
        self.lk.unreachable.clear();
        // The floors stay (positions are the cluster's, not the
        // tenure's); a next tenure here floors everything again first.
        self.lk.tenure_floor_due = true;
        self.lk.tenure_waiting = None;
        self.lk.tenure_heads.clear();
        self.lk.heads.clear();
        self.lk.tenure_minted.clear();
        // A tenure the close keeps is this node's again in a moment
        // (`lock_owner_resuming`): its waiters ask here again. `NotOwner`
        // made a peer forget this node as the holder, and with S3 cut
        // again it could not learn it back: its own grant's renewals
        // found no owner until the grant lapsed under its I/O.
        let outcome = if kept {
            LockOutcome::Waiting {
                retry_ms: self.lock_resume_retry_ms(),
            }
        } else {
            LockOutcome::NotOwner { owner: 0 }
        };
        let waiters = std::mem::take(&mut self.lk.waiters);
        for w in waiters {
            if let Some(t) = w.held_timer {
                self.cancel_timer(t, out);
            }
            self.lock_deliver(
                now,
                w.node,
                w.ino,
                w.req,
                w.op,
                w.sent,
                outcome.clone(),
                replica,
                out,
            );
        }
    }

    // ------------------------------------------------------------ after every event

    pub(crate) fn locks_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lock_fill_renew_heads(replica, out);
        let dirs = std::mem::take(&mut self.lk.pending_dir_floors);
        if !dirs.is_empty() {
            let floor = self.lock_release_floor(replica);
            for dir in dirs {
                self.lock_note_dir_floor(dir, &floor);
            }
        }
        // An expired record dropped on the way (a conflicting request's
        // outwait, a token check) changed the table too.
        if replica.locks().take_expired_dropped() {
            self.lk.mirror_dirty = true;
        }
        self.lock_take_outwaited(replica);
        if self.lk.mirror_dirty {
            self.lk.mirror_dirty = false;
            let backups = self.lease.backups();
            if !backups.is_empty() && self.root_usable(now) {
                self.lk.mirror_ver += 1;
                let grants = replica.locks().grants_snapshot();
                let floor = self.lk.floor_all;
                for b in backups {
                    out.push(Action::Send {
                        to: *b,
                        msg: PeerMsg::LockMirror {
                            ver: self.lk.mirror_ver,
                            grants: grants.clone(),
                            floor,
                        },
                    });
                }
            }
        }
    }
}
