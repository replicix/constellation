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
//!   grants and park the request (a waiter, FIFO per inode). A
//!   non-blocking request that nothing conflicts with but which this
//!   owner may not grant yet (the grace, a new tenure's floor, an
//!   outwaited holder's barrier, a delegate's short authority) is
//!   answered `Waiting { grace: true }`, and its requester answers its
//!   engine `LockAnswer::Wait` (plan 43 F-b): the engine retries it,
//!   bounded, on a thread of its own. A known conflict still answers
//!   `WouldBlock`. The same for an owner that may not grant at all yet
//!   (no fresh S3 liveness, a lease not marked as granting: it answered
//!   `Busy`, `ENOLCK` after three attempts). A timer at each grace's end
//!   (`Timer::LockGraceEnd`) serves the parked waiters it held back.
//! - A recalled node releases (`LockReleased`) or is outwaited by the
//!   grant's expiry (`Timer::LockGrantExpiry`); either serves the
//!   waiters. A waiter whose request was answered `Waiting` (so the RPC
//!   returns within `recall_hold_ms`) gets its grant pushed
//!   (`LockGranted`) or re-attaches with its next request.
//!   A grant is pushed only from a request recent enough to reach its
//!   holder before its first renewal is due (half of `ttl - margin` of the
//!   granted ttl); a waiter whose turn comes with an older one is asked to
//!   ask again (a pushed `Waiting { retry_ms: 0 }`), its turn held for two
//!   request cycles (`Waiter::ask`, `lock_reserved_against`), and granted
//!   over the request that comes (`lock_try_grant`).
//! - [`Core::on_lock_renew`]: extend, or *reclaim* an unknown grant
//!   during a grace period, else `Lost`. The acknowledgement carries the
//!   recalled flag, so a lost recall is repaired by the next renewal.
//!   A **late** renewal (`LockRenewEntry::late`, plan 43 C7-F F-c: the
//!   holder's window ended while the grant pins local locks or writes) is
//!   never a reclaim: it is admitted only from a record the table can
//!   prove continuous since the grant was made (`LockTables::admit_late`),
//!   else `Lost`.
//! - The old root's **final mirror** (plan 43 C7-F F-c,
//!   [`Core::lock_final_step`]): when its lease is no longer usable it
//!   sends its backups its whole table, names the lease body, and adds
//!   nothing under that lease from then on; a fast successor whose
//!   takeover replaced exactly that body installs the records settled and
//!   its lapsed memory ([`Core::lock_install_mirror`]); any other mirror
//!   only unsettled.
//! - The table follows the subtree: handed to a delegate with its first
//!   granting `DelegRenewed`, back with `DelegRecalled`. The root keeps
//!   its copy of what it handed ([`LockState::handed`]): every granting
//!   renewal re-sends the live copies (the reply can be lost), and a
//!   generation that ends without handing one back — its recall
//!   overtook the reply — leaves it reinstated in the root's table,
//!   where the subtree's next delegation takes it along. A grant that
//!   moves does not get a fresh lock TTL at its receiver, which a
//!   restamp (`now + ttl + margin`) did: it travels with what is left of
//!   its record, a duration the sender measures as the grant leaves its
//!   table, and the receiver records `min(restamp, now + left + margin)`
//!   ([`Core::lock_install_moved`]). Its holder may be gone (a push that
//!   lapsed or found no op, a holder that lapsed under the delegate's short
//!   authority or released at the old owner after the move), and then
//!   whoever waits behind it waited a whole lock TTL again at every move
//!   (21 s at the production TTL, sim `locks-delegated-production-ttl`
//!   seed 15, and 694 of 2000 seeds of its heavy variant). The leaving
//!   grants stay restamped. A delegate outwaited by TTL also leaves a
//!   grace on its subtree. An
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
//!   grouped by owner. A lapsed grant is not renewed as one; if it pins
//!   local locks or writes it is *lapsing* and asked for back with late
//!   renewals every `lock_poll_ms` until the owner admits or refuses it or
//!   `lock_owner_wait_max_ms` passes (the FUSE thread parks the I/O
//!   meanwhile); one that pins nothing is dropped (the FUSE thread fences
//!   I/O under it: `EIO`).
//! - A recall of a grant with local locks under it is honoured when the
//!   last one leaves (`Control::LockIdle`); one without waits for the
//!   flush of the file's dirty data (`Action::LockFlush` →
//!   `Event::LockFlushed`) before `LockReleased`.

use super::backup::{FreshHold, FreshVerdict};
use super::{Core, Timer};
use crate::action::{Action, ControlOk, LockAnswer, LockTestAnswer, S3Op};
use crate::event::{
    LeaseKey, LockOutcome, LockRenewEntry, LockRenewResult, LockTestOutcome, PeerMsg,
};
use crate::ids::{HonourMs, Ms, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::Ino;
use constellation_meta::locks::{Grant, GrantId, GrantWire, HeldGrant, Installed, LockMode};
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
    /// from here), on this node's honour clock (plan 43 C7-H1).
    sent_at: HonourMs,
    attempts: u32,
    /// When this node's op was created (the lock call reached the core):
    /// the S3-less deadline and the grant-time statistics count from here.
    created: WaitMs,
    /// Where the application's wait began (`created`, or earlier by the
    /// `waited_ms` the call carried): the place in the owner's queue and
    /// the `waited_ms` sent on, nothing else. On the wait clock (plan 43
    /// C7-H1), like the owner's queue it joins.
    since: WaitMs,
    /// Waiting in this node's own waiter list (the owner is here).
    local_wait: bool,
    /// The lease was re-read once because the cached holder could not be
    /// reached (see `lock_route_op`).
    reread: bool,
    /// The node the delegation table routed this request to answered
    /// `NotOwner` with nowhere new to go (`Core::lock_delegated_dead_end`):
    /// the next attempt reads the lease instead, once.
    ask_lease: bool,
}

/// Why `lock_try_grant` did not grant.
enum Blocked {
    /// Live grants of other nodes conflict: to recall.
    Conflict(Vec<Grant>),
    /// The requester's own grant is recalled and being given up.
    Recalled,
    /// Nothing conflicts, but this owner may not grant yet.
    NotYet(NotYet),
    /// Grantable, but the waiter's last request is too old for a pushed
    /// grant to reach it before its first renewal is due: it is asked to
    /// re-send (`Waiter::ask`; the "ask again" rule, `lock_try_grant`).
    AskAgain,
}

/// What a request that nothing conflicts with is waiting for (plan 43
/// F-b: a non-blocking request waits for it too).
#[derive(Clone, Copy, Debug)]
enum NotYet {
    /// A new root tenure's floor is not known yet.
    TenureFloor,
    /// An outwaited holder's barrier is not settled yet.
    Barrier,
    /// A delegate with too little authority left to grant anything.
    ShortAuthority,
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
    /// The requester's honour clock when it sent its request (echoed with
    /// a push: the grant's window counts from there, never later). Opaque
    /// here: the owner never reads it as its own time, only echoes it
    /// (and compares it with a `LockCancel`'s `upto`, a reading of the
    /// same requester clock).
    sent: HonourMs,
    /// This owner's wait clock when that request (the park, or the latest
    /// re-send) arrived: a grant served to a remote waiter is live from
    /// here, not from when it is served. The requester's window counts
    /// from `sent`, which was before, so the owner's record still
    /// outlasts it by `2 × margin` — and a requester that died while
    /// parked costs at most `ttl + margin` from its last message, never
    /// that plus however long it sat in the queue (the EC2 lock run,
    /// PROGRESS.md "Fix: M14 follow-ups").
    recv: WaitMs,
    /// This node's own request (its `LockOp`).
    op: Option<OpId>,
    held_timer: Option<TimerId>,
    since: WaitMs,
    /// This waiter's turn has come but its last request is too old for a
    /// pushed grant; it was asked to re-send (see [`Ask`]). `None` at
    /// park and whenever its request re-attaches.
    ask: Option<Ask>,
}

/// An "ask again" sent to a parked remote waiter (the "ask again" rule,
/// `lock_try_grant`): its turn is held for `Core::lock_ask_again_wait_ms`
/// (nothing that conflicts is granted to anyone else,
/// `lock_reserved_against`), then it is passed over until its next request.
#[derive(Debug, Clone, Copy)]
struct Ask {
    /// When the ask was sent (this owner's clock).
    at: WaitMs,
    /// The hold ran out unanswered: passed over, never asked or held
    /// again until a request of the waiter's own re-attaches.
    passed: bool,
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
    sent: HonourMs,
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
    sent: HonourMs,
}

/// How long a late renewal answer is still used ([`LateRenew`]).
const LATE_RENEW_MS: i64 = 60_000;

/// How long a lease read made for lock requests is shared with the ones
/// that need the same answer (wait clock). A request told its owner cannot
/// be reached re-reads the lease before it gives up, once per request;
/// requests re-issued against a dead link (the engine's waits ask again
/// every 50 ms to a second) used to read it once each: hundreds of waiters
/// on a node, hundreds of S3 reads a second. Short: the lease names who
/// holds the root now, and a successor's takeover is learned from the
/// next read after it.
const LOCK_HOLDER_READ_TTL_MS: i64 = 250;

/// What a lease read told the lock requests that waited for it
/// ([`Core::lock_read_holder`]).
///
/// Plan 43 C7b: it keeps no ETag, so it cannot be observed
/// (`LeaseState::observe` takes the tag): the read was observed once, when
/// it was handled (`Core::on_s3`), and a request that shares it or takes it
/// up to [`LOCK_HOLDER_READ_TTL_MS`] later is no sighting of the lease at
/// its own `now`. Whether the lease may be claimed is the observation's
/// say (`LeaseState::claimable_observed`), never the body's.
#[derive(Debug, Clone)]
enum HolderRead {
    Lease(constellation_store_s3::Lease),
    Empty,
    Failed,
}

impl HolderRead {
    fn of(result: &crate::event::S3Result) -> Self {
        match result {
            crate::event::S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                HolderRead::Lease(lease.clone())
            }
            crate::event::S3Result::LeaseGet(Ok(None)) => HolderRead::Empty,
            _ => HolderRead::Failed,
        }
    }
}

/// The lease read in flight for lock requests: its S3 op and the requests
/// it answers.
#[derive(Debug)]
struct HolderFlight {
    s3: OpId,
    waiting: Vec<OpId>,
}

/// The last lease read made for lock requests: when it was answered, what
/// it said, and the holder this node took the lease to name then (a gossip
/// that moved it since ends the sharing).
#[derive(Debug)]
struct HolderDone {
    at: WaitMs,
    read: HolderRead,
    holder: Option<NodeId>,
}

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
    /// When `renew_timer` fires (honour clock).
    renew_at: Option<HonourMs>,
    /// Delegate: generations whose renewal a lock renewal asked for
    /// (too little of the delegation left to extend the grant by much).
    deleg_renew_wanted: std::collections::BTreeSet<u64>,
    /// Root: a lock renewal it answered was capped by its lease below a
    /// grant worth holding (`Core::lease_renew_for_grants`).
    lease_renew_wanted: bool,
    /// The release CAS last issued lets the lease go unmarked
    /// (`issue_release`): read while that CAS is out or in doubt
    /// ([`Core::release_unmarked_in_flight`]).
    pub(crate) release_unmarked: bool,
    /// A lease read to relearn the owner for renewals is in flight.
    relearning: bool,
    /// An owner answered a renewal `NotOwner` naming itself (a successor
    /// whose predecessor's lock table is not installed yet: "ask again"):
    /// the renewal tick fires no earlier than this
    /// ([`Core::lock_relearn_floor`]).
    ask_again_at: Option<HonourMs>,
    /// Plan 43 C7-F F-b N3: the lease read shared by the lock requests
    /// that need to know who holds the root (`Core::lock_read_holder`):
    /// the one in flight with the requests it will answer, and the last
    /// one's result for [`LOCK_HOLDER_READ_TTL_MS`].
    holder_flight: Option<HolderFlight>,
    holder_done: Option<HolderDone>,
    /// Applications of a shared result on the stack (bounded recursion).
    holder_depth: u8,
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
    /// owner recalls. Bounded (`DONE_REQS_KEPT`). With the inode, when
    /// that RPC was sent: the reply's window counts from it.
    done_reqs: BTreeMap<OpId, (Ino, HonourMs)>,
    /// Plan 43 C7-H1 (R8): the end, on this node's wait clock, of each
    /// token window of its own queued replays (by grant and wall end), for
    /// the release that waits for them (`replay_blocks_release`).
    replay_pins: BTreeMap<(GrantId, i64), WaitMs>,
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
    served: BTreeMap<(NodeId, Ino), WaitMs>,
    /// Owner side: per peer, how many grants served to it from the queue
    /// (and asks to ask again) in a row went unused and were outwaited,
    /// or unanswered (no renewal, release, recall acknowledgement or
    /// request from it in between). At
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
    /// Owner side: the timer at the earliest end of a grace still to
    /// come, and when it fires (`lock_arm_grace_end`).
    grace_end_timer: Option<(TimerId, WaitMs)>,
    recalls: BTreeMap<GrantId, Recalling>,
    recall_by_req: BTreeMap<OpId, GrantId>,
    /// Subtree grace after an outwaited delegate: `(dir, until)`, on this
    /// node's wait clock.
    pub(crate) grace: Vec<(Ino, WaitMs)>,
    pub(crate) mirror_dirty: bool,
    mirror_ver: u64,
    /// This node as a backup: the holder's last mirror, and the lease it
    /// names when it is that lease's *final* one (plan 43 C7-F F-c).
    bk_mirror: Option<(u64, Vec<GrantWire>, Option<LeaseKey>)>,
    /// Old root, plan 43 C7-F F-c: the lease (`lock_lease_key`) whose
    /// final mirror this node sent: while it is the lease held, nothing is
    /// added to the table (`lock_final_blocks`).
    final_for: Option<LeaseKey>,
    /// The timer that brings the final mirror about (the lease's usable
    /// end), or re-sends it, and when it fires (honour clock).
    pub(crate) final_timer: Option<(TimerId, HonourMs)>,
    /// Successor, plan 43 C7-F F-c: the lease body this node's fast
    /// takeover replaced (`Core::note_fast_takeover`), which the mirror's
    /// final key must equal for its records to be trusted.
    pub(crate) takeover_prev: Option<LeaseKey>,
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
    /// Root, plan 43 C7-F follow-up (the review's H1): the grants a
    /// delegate's recall answer handed back, per generation, by `(grant,
    /// recall)`. A duplicate of the answer (the peer layer re-sent it, a
    /// retried stream) is the same message: installed again, a record the
    /// root released or superseded since would come back, and one it kept
    /// would be restamped (later), and F-c admits a late renewal from a
    /// record. Each pair installs once; dropped with the generation.
    returned: BTreeMap<u64, std::collections::BTreeSet<(GrantId, OpId)>>,
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
    heads: BTreeMap<u64, (i64, u64, WaitMs)>,
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
    ///
    /// Plan 43 C7a: the horizons here are on this node's **wait** clock,
    /// where the owner waits its grants out, and so is the write
    /// (`Action::PersistLockHorizon { until }`, the event's `durable`): what
    /// crosses a restart is the boot stamp the store makes of it, never
    /// seen here, so each comparison is wait with wait.
    horizon_durable: WaitMs,
    /// The write in flight, if any (one at a time: later needs coalesce
    /// into the next one, which carries the highest): the horizon asked
    /// for (the event names it).
    horizon_writing: Option<WaitMs>,
    /// The highest horizon asked for (rounded up), written next.
    horizon_want: WaitMs,
    /// What the answer being built needs durable (`lock_need_horizon`),
    /// taken by `lock_answer`.
    horizon_need: WaitMs,
    /// Answers waiting for the horizon or (plan 41 L0) for the tenure's
    /// liveness, in the order they were made.
    horizon_held: Vec<HeldAnswer>,
    /// Plan 41 L0: the answer being built extends a window while the
    /// tenure's S3 liveness is stale or the tenure is not marked
    /// (`Core::renew_hold`); taken by `lock_answer`, or by the renewal
    /// tick for this node's own grants.
    fresh_need: Option<FreshHold>,
    /// Plan 41 L0: this node's own renewals at itself (`Route::Me`)
    /// decided while stale: `(hold, ino, grant, result)`, applied as
    /// renewed from the decision once the tenure is proven, or failed.
    own_held: Vec<(FreshHold, Ino, GrantId, LockRenewResult)>,
}

/// An owner's answer to a peer held back: until the restart horizon is
/// durable through `need`, and (plan 41 L0) until `fresh`'s tenure is
/// proven live.
#[derive(Debug)]
pub(crate) struct HeldAnswer {
    need: WaitMs,
    fresh: Option<FreshHold>,
    to: NodeId,
    msg: PeerMsg,
    /// When it was held (the debug line of an answer that waited only for
    /// the order of the answers to its peer says for how long).
    since: WaitMs,
    /// What it waits for is nothing but an earlier held answer to the same
    /// peer (its own window is durable and its tenure proven): counted in
    /// `lock_order_holds` when it goes.
    order_only: bool,
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

    /// Owner side: how many grants served to `node` in a row went unused
    /// and were outwaited (tests).
    #[cfg(test)]
    pub(crate) fn lock_unused_grants(&self, node: NodeId) -> u32 {
        self.lk.unused_grants.get(&node).copied().unwrap_or(0)
    }

    /// Owner side: `id` was served from the queue and not seen used since
    /// (tests).
    #[cfg(test)]
    pub(crate) fn lock_is_unproven(&self, id: GrantId) -> bool {
        self.lk.unproven.contains(&id)
    }

    /// Owner side: peers taken for unreachable right now (tests).
    #[cfg(test)]
    pub(crate) fn lock_unreachable(&self) -> usize {
        self.lk.unreachable.len()
    }

    /// Owner side: the barrier on directory `dir` itself (tests).
    #[cfg(test)]
    pub(crate) fn lock_dir_barrier_at(&self, dir: Ino) -> Option<i64> {
        self.lk
            .dir_barriers
            .iter()
            .find(|(d, _)| *d == dir)
            .map(|(_, a)| *a)
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
            ("lk_own_held", self.own_held.len()),
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

/// What trying a parked request again produced (`lock_serve_again`).
enum Again {
    /// A grant, or a refusal that ends the wait.
    Answer(LockOutcome),
    /// Still conflicting (its recalls are sent), recalled, not yet
    /// grantable, or in a grace: it stays parked, and so does the queue.
    Hold,
    /// Grantable, but the waiter's request is too old for a push
    /// (`Blocked::AskAgain`).
    Ask,
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
    pub(crate) fn lock_min_grant_ms(&self) -> i64 {
        (2 * self.lock_margin_ms()).min(self.lock_ttl_ms())
    }

    /// A restamp for a grant that changed owner (a move, a reclaim, a
    /// mirror installed): as if renewed now, which is never sooner than
    /// what its holder measured. On this node's wait clock, where an
    /// owner's records run (plan 43 C7-H1).
    fn restamp(&self, now: Now) -> WaitMs {
        now.wait
            .plus((self.lock_ttl_ms() + self.lock_margin_ms()).max(0) as u64)
    }

    /// The record of a moved grant that carried what was left of its
    /// own (`left_ms`: the sender's measure, a duration, at its send):
    /// from this receipt, plus the margin for this clock — each hop
    /// measures one more duration on one more clock — and never later
    /// than a restamp.
    ///
    /// Why it never ends before a window a holder honours: the sender's
    /// record already ended no sooner (the invariant 11ff444 restored:
    /// an owner's record is at least `ttl + margin` from the request it
    /// answered, and a re-grant in place keeps the later end); the
    /// sender measured `left` in the step that took the grant out of its
    /// table (`lock_on_delegated`, `lock_hand_back`) and extends nothing
    /// after (it renews and reclaims no grant it does not have, and
    /// routes the inode elsewhere), so only this receiver can extend a
    /// holder's window from there; this receipt is after that
    /// measure in real time; and the margin pays for the second clock.
    /// A restamp ends no sooner than any such window either (from a send
    /// before now), so the minimum of the two is as safe as each, and is
    /// never longer than a restamp (what a move gave before). Model:
    /// `MoveStamp::CarryAll` (`crates/model/src/locks.rs`).
    fn carried_until(&self, now: Now, left_ms: u64) -> WaitMs {
        WaitMs(
            now.wait
                .0
                .saturating_add(left_ms.min(i64::MAX as u64) as i64)
                .saturating_add(self.lock_margin_ms()),
        )
        .min(self.restamp(now))
    }

    /// What is left of `g`'s record on the sender's clock at `now`, in
    /// ms: the duration a moved grant carries. An expired record not yet
    /// dropped carries 0 (it arrives as `now + margin`, and its expiry
    /// there is an outwait like any other).
    fn lock_left(now: WaitMs, g: &Grant) -> u64 {
        g.until.since(now).max(0) as u64
    }

    // ------------------------------------------------------------ routing

    fn lock_route(&self, now: Now, ino: Ino, replica: &dyn Replica) -> Route {
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
        now: Now,
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

    /// This node's own table delegates `ino` to this node, under a
    /// generation it has not handed back to a root (a recall's
    /// `deleg_handed_back_to`, which routes the subtree's renewals to that
    /// root). A `NotOwner { 0 }` for it from the root this node caches is
    /// then that root declining to name the requester
    /// (`lock_route_answering`): its table has re-delegated the subtree to
    /// this very node, whose own generation serves the inode once it is
    /// renewed — not a root that found itself deposed, so the cached root
    /// stays. (A handed-back generation's root that answers so may well
    /// be deposed: the cached root is cleared as before.) Cleared, the
    /// delegate lost its root (`root_node`), started a new stream round,
    /// and dropped its new generation's first renewal answer as stale
    /// (`on_deleg_renewed`): every lock renewal in the subtree was
    /// answered `NotOwner { 0 }` until that renewal timed out (sim
    /// `locks-delegated-production-ttl-heavy` seed 1434: a cross-subtree
    /// rename re-delegated `d1` to its delegate, 45 such answers in
    /// 0.46 s, five I/Os fenced).
    fn lock_delegated_here(&self, ino: Ino, replica: &dyn Replica) -> bool {
        if !self.cfg.delegation || replica.delegation_table().is_empty() {
            return false;
        }
        let keys = Self::read_keys(ino, None);
        matches!(
            replica.resolve_ownership(&keys),
            constellation_meta::delegation::Ownership::Delegated(d)
                if d.node == self.cfg.node_id && self.deleg_handed_back_to(d.gen).is_none()
        )
    }

    /// `renewal`: a held lease whose takeover gate is still pending (the
    /// view fenced for M9's floor) still serves renewals and reclaims —
    /// that is what the floor is for; only new grants wait (the
    /// harness's lock-failover found the successor answering `NotOwner`
    /// until the locker's grant lapsed).
    fn lock_route_for(&self, now: Now, ino: Ino, replica: &dyn Replica, renewal: bool) -> Route {
        if self.cfg.delegation && !replica.delegation_table().is_empty() {
            let keys = Self::read_keys(ino, None);
            if let constellation_meta::delegation::Ownership::Delegated(d) =
                replica.resolve_ownership(&keys)
            {
                if d.node == self.cfg.node_id {
                    return match self.deleg_mine_until(d.gen) {
                        Some(until) if now.honour < until => Route::Me {
                            cap_ms: until.since(now.honour) - self.lock_margin_ms(),
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
        if self.lease.usable(now, &self.cfg)
            && (renewal || !self.lease.fenced())
            && !self.lock_final_blocks()
        {
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
                    cap_ms: expires.since(now.honour) - self.lock_margin_ms(),
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

    /// `from`, asked for `ino` by the route this node's delegation table
    /// gives (the subtree's delegate, the root a recall redirected it to,
    /// or, at the delegate, the root it handed the generation back to),
    /// answered `NotOwner` with nowhere new to go: it names nobody, or
    /// this node, or it is the redirect's root and names the delegate
    /// again. That route is followed whatever the cached holder says, so
    /// clearing the cache (650acc8's "`NotOwner { 0 }` sends it to the
    /// lease") reached no lease read: the next attempt reads the lease
    /// instead (`LockOp::ask_lease`), and takes it if nobody holds it.
    ///
    /// Without it nobody took a free lease while only lock waiters needed
    /// it (`locks-unlinked-delegated-blips` seed 230: an epoch's flush
    /// released the lease of the root that had just recalled `d1`, before
    /// the recall's end reached the table; its delegate sent the subtree's
    /// requests to that root, which sent them back, and the third node
    /// went round the two; 55 s, until a mutation took the lease).
    fn lock_delegated_dead_end(
        &self,
        from: NodeId,
        ino: Ino,
        owner: NodeId,
        replica: &dyn Replica,
    ) -> bool {
        if from == self.cfg.node_id || !self.cfg.delegation || replica.delegation_table().is_empty()
        {
            return false;
        }
        let keys = Self::read_keys(ino, None);
        let constellation_meta::delegation::Ownership::Delegated(d) =
            replica.resolve_ownership(&keys)
        else {
            return false;
        };
        owner == 0 || owner == self.cfg.node_id || self.lk.deleg_moved.get(&d.gen) == Some(&from)
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
    fn lock_owner_resuming(&self, now: Now) -> bool {
        self.epoch_reclaim_pending(now)
            || (self.lease.usable(now, &self.cfg) && self.lease.fenced() && !self.lease.releasing)
    }

    /// The root's grants never outlive its lease's usable end (under a
    /// continuation epoch the lease has no expiry; the ttl stands).
    pub(crate) fn lock_cap_ms(&self, now: Now) -> i64 {
        if self.lease.held.is_none() {
            return 0;
        }
        if self.lease.epoch_held() {
            let ttl = self.lock_ttl_ms();
            self.ack.note_hold_grant(now.wait.plus(ttl.max(0) as u64));
            return ttl;
        }
        // Plan 43 C7-H1 (R6): the lease's end on the honour clock, not its
        // wall expiry (a backward wall step must not lengthen a grant past
        // what this holder honours of its lease); no honour end: no cap.
        self.lease
            .honour_left_ms(now, &self.cfg)
            .map_or(0, |left| left - self.lock_margin_ms())
    }

    /// The latest end of a subtree grace on this tenure (an outwaited
    /// delegate's, a delegation that started inside the root's), on the
    /// wait clock; 0 none: what the holder's beat names as its guard
    /// ([`Core::beat_guard_wait_ms`]).
    pub(crate) fn lock_grace_until(&self) -> WaitMs {
        self.lk
            .grace
            .iter()
            .map(|(_, u)| *u)
            .max()
            .unwrap_or(WaitMs(0))
    }

    /// The inode is under a subtree grace (an outwaited delegate), or is
    /// unlinked while one lasts: a generation that ends without handing
    /// its grants back (outwaited, sealed, drained from its backup) may
    /// have granted an inode its rows then took out of the subtree, and
    /// such an inode is under no directory any more. Which subtree it
    /// left is not recorded, so any grace covers every unlinked inode
    /// (reclaims admitted, as under any grace).
    fn lock_in_grace(&mut self, now: Now, ino: Ino, replica: &dyn Replica) -> bool {
        use constellation_fs_core::types::ROOT_INO;
        self.lk.grace.retain(|(_, until)| *until > now.wait);
        if self.lk.grace.is_empty() {
            return false;
        }
        let dirs: Vec<Ino> = self.lk.grace.iter().map(|(d, _)| *d).collect();
        dirs.iter()
            .any(|d| *d == ROOT_INO || replica.is_under(ino, *d))
            || (ino != ROOT_INO && !replica.is_under(ino, ROOT_INO))
    }

    /// Whether new grants are refused now (reclaims still accepted).
    fn lock_grace_active(&mut self, now: Now, ino: Ino, replica: &dyn Replica) -> bool {
        Self::lock_quarantine_until(replica) > now.wait || self.lock_in_grace(now, ino, replica)
    }

    /// The whole-namespace grace: the restart or takeover quarantine of
    /// read delegations, or of lock grants (kept apart, see
    /// `LockTables::set_quarantine`), whichever ends later.
    fn lock_quarantine_until(replica: &dyn Replica) -> WaitMs {
        replica
            .read_delegations()
            .quarantine_until()
            .max(replica.locks().quarantine_until())
    }

    // ------------------------------------------------------------ owner side

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        sent: HonourMs,
        incarnation: u32,
        waited_ms: u64,
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
                grace: false,
            };
            self.lock_answer(now, from, PeerMsg::LockReply { req, outcome }, out);
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
            let timer = self.set_timer_wait(now.wait.plus(hold), Timer::LockHeldReply(wid), out);
            let queued = self.lock_waited_since(now.wait, waited_ms);
            let w = &mut self.lk.waiters[i];
            w.sent = sent;
            w.recv = now.wait;
            w.req = Some(req);
            // A client that lost a round keeps its place (`waited_ms`).
            w.since = w.since.min(queued);
            let old = w.held_timer.replace(timer);
            // A re-send is the answer to an ask, and a live node's: what
            // the unanswered asks and unused grants counted against it is
            // over, also when it comes after the turn was passed over (a
            // slow step, a late answer: it proves the node alive all the
            // same). Then served over this request (`base = recv`: a grant
            // answering an RPC is never stale).
            let answered = w.ask.take().is_some();
            if let Some(old) = old {
                self.cancel_timer(old, out);
            }
            if answered {
                self.lock_peer_heard(from, false);
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
            waited_ms,
            replica,
            out,
        ) {
            Served::Outcome(outcome) => {
                self.lock_answer(now, from, PeerMsg::LockReply { req, outcome }, out)
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
        now: Now,
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
                        grace: false,
                    };
                    self.lock_answer(now, from, PeerMsg::LockReply { req, outcome }, out);
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
    fn lock_need_horizon(&mut self, need: WaitMs, target: WaitMs) {
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
    ///
    /// Plan 41 L0: an answer that extended a window while the tenure's
    /// liveness was stale (`fresh_need`) waits, the same way, for the
    /// lease read that proves the tenure (`renew_after_event`), and is
    /// refused (`NotOwner { 0 }`) if it shows a deposition. Holding
    /// rather than refusing at once: a refusal makes the holder re-read
    /// the lease and send again, a round trip to S3 either way.
    fn lock_answer(&mut self, now: Now, to: NodeId, msg: PeerMsg, out: &mut Vec<Action>) {
        let need = std::mem::take(&mut self.lk.horizon_need);
        let fresh = self.lk.fresh_need.take();
        // An answer that grants and extends nothing (a refusal, a request
        // to wait, a redirection) has no window to hold back, and none
        // that an earlier held answer to the same peer needs to precede:
        // it goes at once. Queued behind a held renewal (which a stale
        // owner holds for a lease read, S3 being away) it reached its
        // requester after the request's timeout, three times in a row:
        // `ENOLCK` for a lock nothing held (plan 43 F-b, Q18c).
        let refusal = matches!(
            &msg,
            PeerMsg::LockReply {
                outcome: LockOutcome::Waiting { .. }
                    | LockOutcome::WouldBlock
                    | LockOutcome::NotOwner { .. }
                    | LockOutcome::Busy,
                ..
            }
        ) && need == WaitMs(0)
            && fresh.is_none();
        let oldest_held = self
            .lk
            .horizon_held
            .iter()
            .filter(|h| h.to == to)
            .map(|h| h.since)
            .min();
        let behind = !refusal && oldest_held.is_some();
        if refusal {
            if let Some(since) = oldest_held {
                // What the order rule would have queued behind a held
                // answer (plan 43 F-b review F5): counted, so that a run
                // shows what the exception saved.
                self.stats.lock_order_bypassed += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    to,
                    held_ms = now.wait.0 - since.0,
                    "an answer that grants nothing goes ahead of a held answer to the same peer"
                );
            }
        }
        if (need <= self.lk.horizon_durable && fresh.is_none() && !behind) || refusal {
            out.push(Action::Send { to, msg });
            return;
        }
        let order_only = behind && need <= self.lk.horizon_durable && fresh.is_none();
        if fresh.is_none() || need > self.lk.horizon_durable {
            self.stats.lock_horizon_held += 1;
        }
        if let Some(f) = fresh {
            self.stats.renew_held += 1;
            self.renew_kick(&[f], out);
            // A holder whose renewal waits re-sends it at every tick (its
            // 500 ms timeout): the answer to a re-send supersedes a held
            // one to the same peer that renews nothing more, so it is not
            // queued twice (one per renewal round, not per re-send).
            if let PeerMsg::LockRenewed { results, .. } = &msg {
                let renews: Vec<GrantId> = results.iter().map(|(_, g, _)| *g).collect();
                self.lk.horizon_held.retain(|h| {
                    !(h.to == to
                        && h.fresh.is_some()
                        && matches!(&h.msg, PeerMsg::LockRenewed { results, .. }
                            if results.iter().all(|(_, g, _)| renews.contains(g))))
                });
            }
        }
        self.lk.horizon_held.push(HeldAnswer {
            need,
            fresh,
            to,
            msg,
            since: now.wait,
            order_only,
        });
        self.lock_horizon_write(out);
    }

    /// Plan 43 C7a (commit 4): a takeover floor's lock part ends at `until`:
    /// the restart horizon is written for it (off the core, as for a grant;
    /// nothing waits for it, the floor itself holds the grants back).
    pub(crate) fn lock_persist_horizon(&mut self, until: WaitMs, out: &mut Vec<Action>) {
        self.lk.horizon_want = self.lk.horizon_want.max(until);
        self.lock_horizon_write(out);
    }

    /// The restart horizon known durable (this process's wait clock).
    pub(crate) fn lock_horizon_durable(&self) -> WaitMs {
        self.lk.horizon_durable
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
        now: Now,
        until: WaitMs,
        durable: Option<WaitMs>,
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
        self.floor_mark_covered(replica);
        self.lock_flush_held(now, failed, replica, out);
    }

    /// Send the held answers that may go now, in order per peer.
    /// `horizon_failed`: the horizon write failed, and what it should
    /// have covered is refused (as a failed synchronous write refused
    /// it) — a grant answered `Busy` (and dropped here), a renewal
    /// `NotOwner { 0 }` (the holder retries). Plan 41 L0: an answer held
    /// for liveness goes once its tenure is proven, and is refused the
    /// same way once that tenure is gone (`Core::renew_verdict`).
    fn lock_flush_held(
        &mut self,
        now: Now,
        horizon_failed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let durable = self.lk.horizon_durable;
        let held = std::mem::take(&mut self.lk.horizon_held);
        let mut keep: Vec<HeldAnswer> = Vec::new();
        let mut dropped = Vec::new();
        for mut h in held {
            if let Some(f) = h.fresh {
                match self.renew_verdict(now, f) {
                    FreshVerdict::Ready => h.fresh = None,
                    FreshVerdict::Wait => {}
                    FreshVerdict::Refuse => {
                        self.stats.renew_refused += 1;
                        if !Self::lock_refuse_answer(&mut h.msg, &mut dropped) {
                            continue;
                        }
                        // Refused, it extends nothing: no horizon to wait for.
                        h.fresh = None;
                        h.need = WaitMs(i64::MIN);
                    }
                }
            }
            if horizon_failed && h.need > durable {
                if !Self::lock_refuse_answer(&mut h.msg, &mut dropped) {
                    continue;
                }
                h.need = WaitMs(i64::MIN);
                h.fresh = None;
            }
            if h.need <= durable && h.fresh.is_none() && !keep.iter().any(|k| k.to == h.to) {
                if h.order_only || h.since != now.wait {
                    // Held for the order of the answers to its peer alone
                    // (or released later than it was held): how long.
                    if h.order_only {
                        self.stats.lock_order_holds += 1;
                    }
                    tracing::debug!(
                        node = self.cfg.node_id,
                        to = h.to,
                        held_ms = now.wait.0 - h.since.0,
                        order_only = h.order_only,
                        "a held lock answer goes"
                    );
                }
                out.push(Action::Send {
                    to: h.to,
                    msg: h.msg,
                });
            } else {
                keep.push(h);
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

    /// Plan 41 L0, after every event: the renewal answers held for the
    /// tenure's liveness (a peer's, this node's own, a delegate's) go
    /// once it is proven, or are refused once it is gone; those still
    /// waiting keep a lease read in flight (`Core::renew_kick`).
    ///
    /// The hold has no timer of its own: it is settled by the events that
    /// can settle it — the lease read's result (or any other S3 result,
    /// the marking CAS's included), a deposition — and its lock-TTL bound
    /// is checked at the next event. A fast tenure never goes long without
    /// one: its backup bookkeeping runs every 50 ms (`HOUSEKEEPING_MS`,
    /// the backup tick) and an `ack=s3` holder's stream heartbeat is a
    /// timer too.
    pub(crate) fn renew_after_event(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.lk.horizon_held.iter().any(|h| h.fresh.is_some()) {
            self.lock_flush_held(now, false, replica, out);
        }
        if !self.lk.own_held.is_empty() {
            let me = self.cfg.node_id;
            let held = std::mem::take(&mut self.lk.own_held);
            let before = held.len();
            let mut keep = Vec::new();
            for (f, ino, id, r) in held {
                match self.renew_verdict(now, f) {
                    FreshVerdict::Ready => {
                        // Honoured from the decision, as a peer's from its
                        // renewal's send.
                        self.lock_apply_renew_result(now, me, ino, id, f.at, r, replica, out);
                    }
                    FreshVerdict::Wait => keep.push((f, ino, id, r)),
                    FreshVerdict::Refuse => {
                        self.stats.renew_refused += 1;
                        replica.locks().renewal_failed(ino, id);
                    }
                }
            }
            let settled = keep.len() < before;
            self.lk.own_held = keep;
            // A failed one is due again; an applied one has its next point.
            if settled && replica.locks().held_count() > 0 {
                self.lock_arm_renew_tick(now, replica, out);
            }
        }
        if !self.ack.renew_held_deleg.is_empty() {
            self.deleg_flush_held(now, out);
        }
        let waiting: Vec<FreshHold> = self
            .lk
            .horizon_held
            .iter()
            .filter_map(|h| h.fresh)
            .chain(self.lk.own_held.iter().map(|(f, ..)| *f))
            .chain(self.ack.renew_held_deleg.iter().map(|(f, ..)| *f))
            .collect();
        if !waiting.is_empty() {
            self.renew_kick(&waiting, out);
        }
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
        now: Now,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: HonourMs,
        waited_ms: u64,
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
                            grace: false,
                        }),
                );
            }
            Route::Unknown
                if from != self.cfg.node_id
                    && (self.readopting()
                        || self.readopt_own_lease_for_forward(now, replica, out)) =>
            {
                // A restarted root the lease object still names re-adopts
                // it, as for a forward (EC2 campaign 8 A-1), and the
                // requester waits: `NotOwner { 0 }` sent it to the lease,
                // which names this node, until the lease expired (60 s).
                // Nothing is judged from a table that is not yet the
                // owner's.
                self.stats.readopt_for_forward += 1;
                return Served::Outcome(LockOutcome::Waiting {
                    retry_ms: self.lock_resume_retry_ms(),
                    grace: false,
                });
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
            // With nothing in conflict a non-blocking request waits for
            // the tenure to be able to grant (plan 43 F-b, Q18c), as for
            // a grace: it got `Busy`, three attempts and `ENOLCK`.
            return Served::Outcome(
                self.lock_conflict_refusal(now, from, ino, mode, blocking, replica)
                    .unwrap_or_else(|| {
                        if blocking {
                            LockOutcome::Busy
                        } else {
                            self.stats.lock_stale_owner_waits += 1;
                            LockOutcome::Waiting {
                                retry_ms: self.lock_resume_retry_ms(),
                                grace: true,
                            }
                        }
                    }),
            );
        }
        if self.lock_grace_active(now, ino, replica) {
            self.stats.lock_grace_refusals += 1;
            if blocking {
                self.lock_park(now, from, ino, mode, req, op, sent, waited_ms, out);
                return Served::Parked;
            }
            // Nothing granted in a grace, and a live grant that conflicts
            // is refused all the same; with none the request waits for
            // the grace to end (plan 43 F-b), as NLM and NFSv4 clients do
            // for a server in its grace period.
            return Served::Outcome(
                match self.lock_conflict_refusal(now, from, ino, mode, false, replica) {
                    Some(refused) => refused,
                    None => {
                        self.stats.lock_grace_waits += 1;
                        LockOutcome::Waiting {
                            retry_ms: self.lock_grace_retry_ms(now, ino, replica),
                            grace: true,
                        }
                    }
                },
            );
        }
        let blocked = match self.lock_try_grant(
            now, now.wait, from, ino, mode, cap_ms, gen, false, out, replica,
        ) {
            Ok(outcome) => return Served::Outcome(outcome),
            Err(blocked) => blocked,
        };
        // Only a grant to be pushed is ever asked again (`lock_try_grant`).
        debug_assert!(!matches!(blocked, Blocked::AskAgain));
        let conflicting = match blocked {
            Blocked::Conflict(conflicting) => conflicting,
            // Behind the tenure floor `lock_try_grant` has not looked at
            // the table yet: a live conflict, or the requester's own grant
            // under recall, is refused at once all the same (a refusal
            // grants nothing; the barrier and the short authority come
            // after these checks, so it is no change for them).
            Blocked::NotYet(_)
                if !blocking && self.lock_known_refusal(now, from, ino, mode, replica) =>
            {
                self.stats.lock_would_block += 1;
                return Served::Outcome(LockOutcome::WouldBlock);
            }
            Blocked::NotYet(why) if !blocking => {
                self.stats.lock_not_yet_waits += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    to = from,
                    ino,
                    ?why,
                    "a non-blocking lock request waits: nothing conflicts, the owner may not grant yet"
                );
                return Served::Outcome(LockOutcome::Waiting {
                    retry_ms: self.lock_resume_retry_ms(),
                    grace: true,
                });
            }
            Blocked::Recalled | Blocked::NotYet(_) | Blocked::AskAgain => Vec::new(),
        };
        for g in conflicting {
            self.lock_recall(now, g, replica, out);
        }
        if blocking {
            self.lock_park(now, from, ino, mode, req, op, sent, waited_ms, out);
            Served::Parked
        } else {
            self.stats.lock_would_block += 1;
            Served::Outcome(LockOutcome::WouldBlock)
        }
    }

    /// How soon a non-blocking request refused for a grace asks again:
    /// the grace's remainder (a difference on this owner's clock), at
    /// most a second, so that it is routed afresh promptly should the
    /// ownership move meanwhile.
    fn lock_grace_retry_ms(&mut self, now: Now, ino: Ino, replica: &dyn Replica) -> u64 {
        let left = self.lock_grace_end(now, ino, replica).since(now.wait);
        (left + 1).clamp(10, 1_000) as u64
    }

    /// When the grace on `ino` ends: the whole-namespace quarantine or
    /// the latest subtree grace covering it (see `lock_in_grace`).
    fn lock_grace_end(&mut self, now: Now, ino: Ino, replica: &dyn Replica) -> WaitMs {
        use constellation_fs_core::types::ROOT_INO;
        self.lk.grace.retain(|(_, until)| *until > now.wait);
        let unlinked = ino != ROOT_INO && !replica.is_under(ino, ROOT_INO);
        let mut end = Self::lock_quarantine_until(replica);
        for (d, until) in &self.lk.grace {
            if unlinked || *d == ROOT_INO || replica.is_under(ino, *d) {
                end = end.max(*until);
            }
        }
        end
    }

    /// Keep one timer at the earliest end of a grace still to come
    /// (`Timer::LockGraceEnd`): the parked waiters are served the moment
    /// a grace ends, not at their next re-attach. Called wherever a
    /// grace is set, and by what parks a waiter.
    pub(crate) fn lock_arm_grace_end(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.grace.retain(|(_, until)| *until > now.wait);
        let quarantine = Self::lock_quarantine_until(replica);
        let next = self
            .lk
            .grace
            .iter()
            .map(|(_, until)| *until)
            .chain((quarantine > now.wait).then_some(quarantine))
            .min();
        let Some(next) = next else {
            return;
        };
        if let Some((id, at)) = self.lk.grace_end_timer {
            if at <= next {
                return;
            }
            self.cancel_timer(id, out);
        }
        let id = self.set_timer_wait(next, Timer::LockGraceEnd, out);
        self.lk.grace_end_timer = Some((id, next));
    }

    /// A grace ended: serve the waiters it held back, and look at the
    /// next end.
    pub(crate) fn on_lock_grace_end(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lk.grace_end_timer = None;
        let parked = self.lk.waiters.len();
        self.lock_reserve_all(now, replica, out);
        if self.lk.waiters.len() < parked {
            self.stats.lock_grace_end_serves += 1;
        }
        // (The next end is armed after this event: `locks_after_event`.)
    }

    /// A non-blocking request that a live grant conflicts with, asked of
    /// an owner that may not grant yet: `WouldBlock` now (a refusal
    /// grants nothing). `None`: it waits.
    #[allow(clippy::too_many_arguments)]
    fn lock_conflict_refusal(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        replica: &dyn Replica,
    ) -> Option<LockOutcome> {
        if blocking
            || replica
                .locks()
                .conflicting(ino, from, mode, now.wait)
                .is_empty()
        {
            return None;
        }
        self.stats.lock_would_block += 1;
        Some(LockOutcome::WouldBlock)
    }

    /// Whether a non-blocking request is refused for what the table knows
    /// whatever this owner may grant yet: the requester's own grant is
    /// under recall, or a live grant of another node conflicts
    /// (`lock_try_grant`'s own tests, without its effects).
    fn lock_known_refusal(
        &self,
        now: Now,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
    ) -> bool {
        let own = replica.locks().own_grant(ino, from, now.wait);
        if own.is_some_and(|g| g.recalled) {
            return true;
        }
        let mode = own.map_or(mode, |g| g.mode.max(mode));
        let reaffirm = own.is_some_and(|g| g.mode == mode);
        !reaffirm
            && !replica
                .locks()
                .conflicting(ino, from, mode, now.wait)
                .is_empty()
    }

    /// How soon a request waiting for this node to own the root again
    /// (`lock_owner_resuming`) asks again.
    pub(crate) fn lock_resume_retry_ms(&self) -> u64 {
        self.cfg.lock_poll_ms()
    }

    /// How long the queue waits for an unreachable waiter's next request
    /// (`lock_serve_waiters`): two of its request cycles (the request
    /// held `recall_hold_ms`, then re-sent after `lock_resume_retry_ms`).
    fn lock_unreachable_wait_ms(&self) -> i64 {
        2 * (self.cfg.recall_hold_ms + self.lock_resume_retry_ms()) as i64
    }

    /// How long a waiter's turn is held for the request it was asked to
    /// re-send (`Waiter::ask`): the same two request cycles as an
    /// unreachable waiter's (`lock_unreachable_wait_ms`) — a live node's
    /// re-send comes well inside them.
    ///
    /// Longer for a node far away: four of its round trips more (as the
    /// core measures them, `PeerLink::rtt_ms`), at most three times the
    /// base — the request cycle of a node across a WAN is not a LAN's.
    fn lock_ask_again_wait_ms(&self, node: NodeId) -> i64 {
        let base = self.lock_unreachable_wait_ms();
        let rtt = self
            .links
            .get(&node)
            .and_then(|l| l.rtt_ms)
            .unwrap_or(0)
            .min(i64::MAX as u64 / 8) as i64;
        (base + 4 * rtt).min(3 * base)
    }

    /// Whether a waiter of another node that was asked to ask again is
    /// still held a turn that a grant of `mode` to `from` would take
    /// (`Waiter::ask`, `lock_try_grant`).
    fn lock_reserved_against(&self, now: WaitMs, ino: Ino, from: NodeId, mode: LockMode) -> bool {
        self.lk.waiters.iter().any(|w| {
            w.ino == ino
                && w.node != from
                && w.mode.conflicts(mode)
                && w.ask.is_some_and(|a| {
                    !a.passed && now.0 < a.at.0 + self.lock_ask_again_wait_ms(w.node)
                })
        })
    }

    /// Grant `mode` to `from` — re-affirming or upgrading *in place* a
    /// live grant it already holds (an owner never downgrades: the
    /// node's local locks may be under the stronger mode; the sim found
    /// a shared request in flight behind an exclusive one) — or the
    /// conflicting grants to recall. `base`: when the request being
    /// answered arrived, on the wait clock (`now.wait` for one answered on
    /// arrival; see [`Waiter::recv`]). `push`: the grant, if made, is pushed to a
    /// waiter whose RPC is gone (`LockGranted`); see the "ask again" rule
    /// below.
    #[allow(clippy::too_many_arguments)]
    fn lock_try_grant(
        &mut self,
        now: Now,
        base: WaitMs,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        cap_ms: i64,
        gen: u64,
        push: bool,
        out: &mut Vec<Action>,
        replica: &dyn Replica,
    ) -> Result<LockOutcome, Blocked> {
        // A new root tenure grants nothing until its floor is known
        // (parked, or `Waiting` for a non-blocking request; reclaims go
        // on).
        if gen == 0 && !self.lock_tenure_floor_ready(replica) {
            self.stats.lock_tenure_waits += 1;
            return Err(Blocked::NotYet(NotYet::TenureFloor));
        }
        let own = replica.locks().own_grant(ino, from, now.wait);
        if own.is_some_and(|g| g.recalled) {
            // Recalled: the node gives it up once its local locks are
            // gone; nothing is granted meanwhile (a re-affirmation kept
            // the id, and the release then dropped a grant with locks
            // under it — the harness's SQLite run found this). The
            // request parks (or would block) until the release.
            return Err(Blocked::Recalled);
        }
        let mode = own.map_or(mode, |g| g.mode.max(mode));
        let reaffirm = own.is_some_and(|g| g.mode == mode);
        let conflicting = if reaffirm {
            Vec::new()
        } else {
            replica.locks().conflicting(ino, from, mode, now.wait)
        };
        self.lock_take_outwaited(now, replica);
        if !conflicting.is_empty() {
            return Err(Blocked::Conflict(conflicting));
        }
        // A waiter asked to ask again (below) has its turn held: nothing
        // that conflicts with it goes to another node meanwhile, or the
        // asked waiter is overtaken by whoever comes in the round trip
        // (the delegate's own clients and every new request, again and
        // again). A refusal grants nothing and recalls nothing: a blocking
        // request parks behind it, a non-blocking one is refused (the lock
        // is promised and in flight: a known conflict). The asked node's
        // own requests are never held back by it.
        if !reaffirm && self.lock_reserved_against(now.wait, ino, from, mode) {
            self.stats.lock_reserved_waits += 1;
            return Err(Blocked::Conflict(Vec::new()));
        }
        // A holder of this inode was outwaited: what it did under its
        // grant must be under this one's floor first (parked, or
        // `WouldBlock`). A re-affirmation gives the node nothing new.
        if !reaffirm && !self.lock_barrier_ready(now, from, ino, gen, replica, out) {
            return Err(Blocked::NotYet(NotYet::Barrier));
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
                return Err(Blocked::NotYet(NotYet::ShortAuthority));
            }
            // The root, short of lease: what it grants now is renewed
            // under the lease it renews at once.
            self.lease_renew_for_grants(now, out);
            if ttl <= 0 {
                return Ok(LockOutcome::Busy);
            }
        }
        // The "ask again" rule. A grant pushed to a waiter counts from the
        // request it answers, and its holder renews it at `(ttl - margin)
        // / 2` into that window (`renew_point`), so it must reach the
        // holder before then: from a request no older than that. A
        // waiter re-sends every `(lock_ttl - margin) / 2` (9.5 s at the
        // defaults) and a delegate (or a lease-capped root) grants for what
        // is left of its authority, about 2 to 3 s: a push served from a
        // request seconds old arrived lapsed (the holder drops those: see
        // `lock_op_outcome`) and asked again behind the next waiter's
        // recall, or the owner's record of it was expired when made (the
        // next waiter of the pass granted over it) — 14 of 300 seeds of the
        // sim's delegated production-TTL config never got their lock in
        // 60 s, and waits ran to 9.5 and 21 s. So a waiter whose request is older
        // than that is not granted: it is asked to send it again, its turn
        // held (`lock_serve_waiters`, `lock_reserved_against`), and
        // granted over the fresh RPC, whose grant counts from a request
        // that has just arrived.
        //
        // Safety (it only withholds): nothing is minted, replaced or
        // extended here, so mutual exclusion, and the rule that a node's
        // record never ends sooner than the window of the grant it
        // replaces (below, `until`), are those of the grants made after it.
        if push && now.wait.since(base) > (ttl - self.lock_margin_ms()) / 2 {
            return Err(Blocked::AskAgain);
        }
        // Never sooner than the record it replaces (`LockTables::grant`
        // keeps one per node and inode): that grant is live, and its
        // holder honours it — under its old id, or under this one, which
        // keeps the later of the two windows (`LockTables::install_held`,
        // as a renewal's answer does). Counted from this request alone (a
        // waiter's last arrival, older than the renewal or the grant the
        // record holds), the record ended inside the holder's window: the
        // owner outwaited it and granted the lock to another node while
        // the holder was still in its section (sim
        // `double-seal-inside-the-grace` seed 4796: two exclusive holders,
        // a second waiter of the node served after the first in one pass,
        // from a request 2.4 s older).
        let until = base
            .min(now.wait)
            .plus((ttl + self.lock_margin_ms()).max(0) as u64)
            .max(own.map_or(WaitMs(i64::MIN), |g| g.until));
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
                // A mint is *settled* (`Grant::settled`, plan 43 C7-F F-c):
                // made only where no grace, floor or barrier holds the
                // inode, so that every grant before it was issued before
                // it. (The callers check the grace first; the tenure floor
                // and the barrier above.)
                debug_assert!(
                    !self.lock_grace_active(now, ino, replica),
                    "a lock grant minted inside a grace"
                );
                let new = replica
                    .locks()
                    .grant(self.cfg.node_id, from, ino, mode, until, gen);
                // A live record of the node replaced: the same grant under a
                // new id (a late renewal of the old one is the new one's).
                if let Some(old) = own {
                    replica.locks().note_continues(&old, new);
                }
                new
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
        // `minter`/`seq` name the grant as the fencing token's
        // `tagged op executed` lines do (a load campaign's token-order
        // check pairs them).
        tracing::debug!(
            node = self.cfg.node_id,
            to = from,
            ino,
            ?mode,
            ttl,
            upgraded = own.is_some(),
            minter = id.node,
            seq = id.seq,
            "granted a lock"
        );
        Ok(LockOutcome::Granted {
            id,
            mode,
            ttl_ms: ttl as u64,
            position,
        })
    }

    fn lock_route_is_root(&self, now: Now, ino: Ino, replica: &dyn Replica) -> bool {
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
        // F1: as `lock_release_floor`.
        replica.settle_tagged_writes();
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
        // F1: what this node executed includes a tagged write that passed
        // its check at the write and has not committed yet; under a grant
        // that has ended here since (the one this floor follows, maybe),
        // it would land after the floor was taken. Wait for it first.
        replica.settle_tagged_writes();
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
        now: Now,
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
        let e = self.lk.heads.entry(gen).or_insert((at, head, now.wait));
        e.2 = e.2.max(now.wait);
        if at >= e.0 {
            *e = (at, e.1.max(head), e.2);
        }
    }

    /// Owner side: the grant records that expired here unreleased since
    /// the last look become barriers on their inodes (see
    /// [`LockState::barriers`]). This node's own grants need none (unless
    /// a mutation under one may still land elsewhere,
    /// `lock_own_outwait_barrier`): what its clients were acknowledged is
    /// in its release floor, which becomes the inode's floor here, as at
    /// an expiry timer's outwait (`on_lock_grant_expiry`). A grant's own
    /// position names this node's unshipped journal only where it touched
    /// the lock file, not the files the lock guards: an own grant dropped
    /// here unreleased, after a stall of this node's core past its window
    /// (sim `locks-stall-step`, `locks-writer-hold`), was granted on with
    /// a floor that lacked the turn its holder wrote to another file.
    fn lock_take_outwaited(&mut self, now: Now, replica: &dyn Replica) {
        for g in replica.locks().take_outwaited() {
            if g.node != self.cfg.node_id {
                // A barrier is a wall value (row 9, C7b): the record's end
                // mapped from the wait clock when the barrier is made.
                self.lock_barrier(g.ino, now.wall_of_wait(g.until).0, Some(g.node));
            } else {
                let floor = self.lock_release_floor(replica);
                self.lock_note_floor(g.ino, &floor);
                self.lock_own_outwait_barrier(now, &g, replica);
            }
        }
    }

    /// F1: this node's own grant ended unreleased (outwaited here) while
    /// a mutation of this node tagged with it may still land at an
    /// executor — one in flight, or one that ended in doubt
    /// (`LockTables::tagged_unsettled`). Its release floor does not have
    /// it, so the next grant to another node waits for a cut, as after
    /// another holder's outwait (its own next grant does not).
    fn lock_own_outwait_barrier(&mut self, now: Now, g: &Grant, replica: &dyn Replica) {
        Self::lock_resolve_doubts(g.id, replica);
        if replica.locks().tagged_unsettled(g.id) {
            self.lock_barrier(g.ino, now.wall_of_wait(g.until).0, Some(g.node));
        }
    }

    /// No grant on `ino` before a cut as of `at` is its floor; `holder`:
    /// whose record it was, if known.
    pub(super) fn lock_barrier(&mut self, ino: Ino, at: i64, holder: Option<NodeId>) {
        // (Plan 41 R2: a barrier is lock content an answer off the core
        // would leave behind; the generation is unsettled in this step.)
        self.dw.unsettle_all();
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
    pub(super) fn lock_dir_barrier(&mut self, dir: Ino, at: i64) {
        if at <= 0 {
            return;
        }
        self.dw.unsettle_all();
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
    pub(crate) fn lock_cut_here(&mut self, now: Now, replica: &dyn Replica) -> (i64, Position) {
        let me = self.cfg.node_id;
        let online_ms = self.cfg.delegation_ttl_ms as i64;
        let gens = &self.dl.gens;
        self.lk
            .heads
            .retain(|gen, _| gens.get(gen).is_some_and(|g| !g.ended));
        let mut at = now.wall.0;
        let mut pos = self.lock_release_floor(replica);
        for (gen, g) in &self.dl.gens {
            if g.ended || g.node == me {
                continue;
            }
            let designated = g.kind == super::delegate::DelegKind::Designated;
            match self.lk.heads.get(gen) {
                Some((sent, head, heard)) => {
                    if !designated || now.wait.since(*heard) < online_ms {
                        at = at.min(*sent);
                    }
                    let mut p = Position::ZERO;
                    if *head > 0 && p.streams.raise(*gen, *head) {
                        pos = constellation_meta::locks::floor_join(&pos, &p);
                    }
                }
                None if designated && now.wait.since(g.granted) >= online_ms => {}
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
                    self.dw.unsettle(gen);
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
        now: Now,
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
        now: Now,
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
        now: Now,
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
        // F1: the head is reported as of the renewal's stamp (this step's
        // `now`, taken before it): a tagged write that passed its check
        // before then and is still in its transaction must be under it.
        if out.iter().any(|a| {
            matches!(
                a,
                Action::Send {
                    msg: PeerMsg::DelegRenew { .. },
                    ..
                }
            )
        }) {
            replica.settle_tagged_writes();
        }
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

    fn lock_recall(&mut self, now: Now, g: Grant, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lk.recalls.contains_key(&g.id) {
            return;
        }
        let timer = self.set_timer_wait(g.until, Timer::LockGrantExpiry(g.id), out);
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

    /// When an application that has waited `waited_ms` began (by this
    /// owner's clock: a duration crosses nodes, never a time), at most as
    /// far back as a waiter is kept.
    ///
    /// Plan 43 C7-H1: a queue key, a liveness stamp (`Waiter::since`,
    /// `LockOp::since`, `served`): not a holder's right, so not on the honour
    /// clock; on the wait clock, which no wall step reorders the queue by.
    /// The `LockCancel` and `LockRequest` stamps (`sent`, `upto`) are the
    /// requester's honour readings, only compared with each other.
    fn lock_waited_since(&self, now: WaitMs, waited_ms: u64) -> WaitMs {
        let cap = 4 * self.lock_ttl_ms();
        WaitMs(now.0 - (waited_ms.min(cap.max(0) as u64) as i64))
    }

    #[allow(clippy::too_many_arguments)]
    fn lock_park(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: HonourMs,
        waited_ms: u64,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_waiters_parked += 1;
        self.lk.next_waiter += 1;
        let id = self.lk.next_waiter;
        // A node served before whose grant went unused keeps its place
        // in the queue (see `LockState::served`); its request never
        // stopped waiting from its point of view.
        let keep = 4 * self.lock_ttl_ms();
        self.lk.served.retain(|_, s| now.wait.since(*s) < keep);
        // ... and so does a client whose application has waited already
        // (`waited_ms`: it lost a round, or this request is a re-send of
        // its call): queued from when the wait began, as the sim's clients
        // and the engine's lock calls tell it.
        let queued = self.lock_waited_since(now.wait, waited_ms);
        let since = match self.lk.served.remove(&(from, ino)) {
            Some(s) => {
                self.stats.lock_requeued_in_place += 1;
                s.min(queued)
            }
            None => queued,
        };
        self.lock_arm_waiter_tick(now, out);
        let held_timer = req.map(|_| {
            self.set_timer_wait(
                now.wait.plus(self.cfg.recall_hold_ms),
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
            recv: now.wait,
            op,
            held_timer,
            since,
            ask: None,
        });
        if let Some(op) = op {
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.local_wait = true;
            }
        }
    }

    fn lock_arm_waiter_tick(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.lk.waiter_tick.is_some() {
            return;
        }
        let at = now.wait.plus(self.cfg.recall_hold_ms.max(50));
        let t = self.set_timer_wait(at, Timer::LockWaiterTick, out);
        self.lk.waiter_tick = Some(t);
    }

    pub(crate) fn on_lock_waiter_tick(
        &mut self,
        now: Now,
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
                    outcome: LockOutcome::Waiting {
                        retry_ms,
                        grace: false,
                    },
                },
            });
        }
    }

    /// Something on `ino` changed hands: serve its waiters in order.
    fn lock_serve_waiters(
        &mut self,
        now: Now,
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
                if now.wait.since(recv) >= 4 * ttl {
                    gone.push(i);
                    continue;
                }
                if now.wait.since(recv) >= ttl - margin {
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
                    if now.wait.since(recv) < self.lock_unreachable_wait_ms() {
                        break;
                    }
                    self.stats.lock_unreachable_passed_over += 1;
                    continue;
                }
            }
            let base = if op.is_some() { now.wait } else { recv };
            // A grant pushed (the waiter's RPC is gone), not one answering
            // a request or a local op's: the only one that can arrive old.
            let push = op.is_none() && req.is_none();
            match self.lock_serve_again(now, base, node, ino, mode, push, replica, out) {
                Again::Answer(outcome) => {
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
                Again::Hold => {
                    // Still conflicting (or in grace): stays parked.
                    // Nobody behind it is served either: a waiter that
                    // cannot be granted (its holder's recall is out) holds
                    // the queue, or the ones behind it would overtake it.
                    break;
                }
                Again::Ask => {
                    // Its turn, with a request too old for a push: asked
                    // to send another (`lock_try_grant`), and the queue
                    // waits for it, as for an unreachable waiter's: two
                    // of its request cycles. Unanswered, it is passed
                    // over (the ones behind it are served), counted as a
                    // grant left unused, and never asked or held again
                    // until a request of its own arrives.
                    let wait = self.lock_ask_again_wait_ms(node);
                    match self.lk.waiters[i].ask {
                        None => {
                            self.stats.lock_asked_again += 1;
                            self.lk.waiters[i].ask = Some(Ask {
                                at: now.wait,
                                passed: false,
                            });
                            tracing::debug!(
                                node = self.cfg.node_id,
                                to = node,
                                ino,
                                age_ms = now.wait.since(recv),
                                "asked a lock waiter to ask again: its last request is too old \
                                 for a pushed grant"
                            );
                            // It grants and extends nothing: no horizon to
                            // wait for, as for a recall.
                            out.push(Action::Send {
                                to: node,
                                msg: PeerMsg::LockGranted {
                                    ino,
                                    sent,
                                    outcome: LockOutcome::Waiting {
                                        retry_ms: 0,
                                        grace: false,
                                    },
                                },
                            });
                            self.lock_arm_waiter_tick(now, out);
                            break;
                        }
                        Some(a) if !a.passed && now.wait.0 < a.at.0 + wait => break,
                        Some(a) if !a.passed => {
                            self.lk.waiters[i].ask = Some(Ask { passed: true, ..a });
                            self.stats.lock_ask_again_passed_over += 1;
                            tracing::debug!(
                                node = self.cfg.node_id,
                                to = node,
                                ino,
                                "a lock waiter did not ask again: passed over until its next request"
                            );
                            let n = self.lk.unused_grants.entry(node).or_insert(0);
                            *n += 1;
                            if *n >= UNUSED_GRANTS_UNREACHABLE {
                                self.lock_peer_unreachable(node, "asked to ask again and did not");
                            }
                            continue;
                        }
                        Some(_) => continue,
                    }
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
            let waited = now.wait.since(w.since).max(0) as u64;
            self.stats.lock_wait_ms_total += waited;
        }
    }

    /// Try a parked request again: [`Again::Answer`] once it can be
    /// answered (a grant, or a refusal that ends the wait).
    #[allow(clippy::too_many_arguments)]
    fn lock_serve_again(
        &mut self,
        now: Now,
        base: WaitMs,
        from: NodeId,
        ino: Ino,
        mode: LockMode,
        push: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Again {
        let (cap_ms, gen) = match self.lock_route_answering(now, ino, replica, from, false) {
            Route::Me { cap_ms, gen } => (cap_ms, gen),
            Route::Node(n) => return Again::Answer(LockOutcome::NotOwner { owner: n }),
            Route::Unknown => return Again::Answer(LockOutcome::NotOwner { owner: 0 }),
        };
        let root_owned = self.lock_route_is_root(now, ino, replica);
        if root_owned
            && (!self.strict_answer_allowed(now, out) || !self.ensure_granting_marked(out))
        {
            return Again::Hold;
        }
        if self.lock_grace_active(now, ino, replica) {
            return Again::Hold;
        }
        match self.lock_try_grant(now, base, from, ino, mode, cap_ms, gen, push, out, replica) {
            Ok(outcome) => Again::Answer(outcome),
            Err(Blocked::Conflict(conflicting)) => {
                for g in conflicting {
                    self.lock_recall(now, g, replica, out);
                }
                Again::Hold
            }
            Err(Blocked::Recalled | Blocked::NotYet(_)) => Again::Hold,
            Err(Blocked::AskAgain) => Again::Ask,
        }
    }

    /// Answer a (formerly) parked request: over its RPC, by a push, or
    /// to this node's own op.
    #[allow(clippy::too_many_arguments)]
    fn lock_deliver(
        &mut self,
        now: Now,
        node: NodeId,
        ino: Ino,
        req: Option<OpId>,
        op: Option<OpId>,
        sent: HonourMs,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(op) = op {
            // A local waiter: the owner's clock is this node's, so the
            // grant's window starts now (sim seed 90017: stamped from a
            // request made seconds ago, it arrived lapsed). On the honour
            // clock, the holder's (plan 43 C7-H1).
            if let Some(o) = self.lk.ops.get_mut(&op) {
                o.sent_at = now.honour;
            }
            let me = self.cfg.node_id;
            self.lock_op_outcome(now, op, me, outcome, replica, out);
            return;
        }
        match req {
            Some(req) => self.lock_answer(now, node, PeerMsg::LockReply { req, outcome }, out),
            None => self.lock_answer(now, node, PeerMsg::LockGranted { ino, sent, outcome }, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_released(
        &mut self,
        now: Now,
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
                    self.dw.unsettle(d.gen);
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
        if let Some(g) = replica.locks().own_grant(ino, from, now.wait) {
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
        now: Now,
        id: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Plan 41 L1: is the record expired, and gone if it is, in one step
        // of the table, on the table's own clock. Another writer extends a
        // record (the lock lane), so a read here and a forget below would
        // forget a window renewed in between; and what this step goes on to
        // do (floors, barriers) is decided on the record this call took out.
        let g = match replica.locks().forget_outwaited_if_expired(id, now) {
            Some(Ok(g)) => g,
            Some(Err(until)) => {
                // Renewed since: wait again.
                let timer = self.set_timer_wait(until, Timer::LockGrantExpiry(id), out);
                if let Some(r) = self.lk.recalls.get_mut(&id) {
                    r.timer = timer;
                }
                return;
            }
            None => {
                // Replaced or released meanwhile: whatever waited on it is
                // served now (the sim found a waiter stranded here).
                if let Some(r) = self.lk.recalls.remove(&id) {
                    if let Some(req) = r.req {
                        self.lk.recall_by_req.remove(&req);
                    }
                    self.lock_serve_waiters(now, r.ino, replica, out);
                }
                return;
            }
        };
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
            self.lock_barrier(g.ino, now.wall_of_wait(g.until).0, Some(g.node));
        } else {
            self.lock_own_outwait_barrier(now, &g, replica);
        }
        tracing::info!(
            node = self.cfg.node_id,
            holder = g.node,
            ino = g.ino,
            "a lock grant's recall went unanswered; outwaited it (TTL + margin)"
        );
        self.lock_grant_ended(now, id, g.ino, replica, out);
    }

    /// The grant was released or replaced: gone from the table, and what
    /// waited on it is served.
    fn lock_grant_done(
        &mut self,
        now: Now,
        id: GrantId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        replica.locks().forget(id);
        self.lock_grant_ended(now, id, ino, replica, out);
    }

    /// The rest of a grant's end, after its record left the table (by
    /// [`LockTables::forget`] for a release, by
    /// [`LockTables::forget_outwaited_if_expired`] for an outwait, which
    /// remembers a settled record as lapsed, plan 43 C7-F F-c).
    fn lock_grant_ended(
        &mut self,
        now: Now,
        id: GrantId,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
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
        now: Now,
        from: NodeId,
        req: OpId,
        entries: Vec<LockRenewEntry>,
        sent: (u64, i64),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut results = Vec::with_capacity(entries.len());
        for e in entries {
            let r = self.lock_renew_one(now, from, e.ino, e.grant, e.mode, e.late, sent, replica);
            results.push((e.ino, e.grant, r));
        }
        self.lock_answer(now, from, PeerMsg::LockRenewed { req, results }, out);
        self.lock_renew_authority(now, out);
    }

    /// Renewals answered short of authority ask for more of it: a
    /// delegate's renewal of its delegation, the root's of its lease.
    fn lock_renew_authority(&mut self, now: Now, out: &mut Vec<Action>) {
        for gen in std::mem::take(&mut self.lk.deleg_renew_wanted) {
            self.deleg_renew_now(now, gen, out);
        }
        if std::mem::take(&mut self.lk.lease_renew_wanted) {
            self.lease_renew_for_grants(now, out);
        }
    }

    /// `late`: plan 43 C7-F F-c, a renewal of a grant whose window its holder
    /// no longer honours. Admitted only from a record of that id that is
    /// *settled* here (present, live or expired and not dropped yet) or its
    /// lapsed memory ([`constellation_meta::locks::LockTables::admit_late`]):
    /// the proof that nothing conflicting was granted since. Never the
    /// grace's reclaim (the model's counterexample: an old owner that had
    /// outwaited the grant and granted another node, a successor that knew
    /// neither), never the newer-own-grant fallback (a grant minted
    /// afresh after the old one lapsed does not continue it), and looked up
    /// before anything calls `conflicting`, which drops the expired records.
    #[allow(clippy::too_many_arguments)]
    fn lock_renew_one(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        id: GrantId,
        mode: LockMode,
        late: bool,
        sent: (u64, i64),
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
        // A release that let the lease go unmarked told its taker that
        // this tenure leaves no grant behind, and the taker grants at once
        // (`acquire_won`): from the mark on, the root extends nothing. Its
        // table can still take a record while the CAS is out — a reclaim
        // under a read delegation's quarantine, which the mark does not
        // count, or the grants a delegate's recall answer hands back — and
        // a renewal served from it gave its holder a window past the
        // release, beside the taker's grants. The holder relearns the
        // owner.
        if gen == 0 && self.release_unmarked_in_flight() {
            return LockRenewResult::NotOwner { owner: 0 };
        }
        // Plan 41 L0: the root extends a window only as far as a fresh
        // owner may promise one. A tenure that can be taken over before
        // its lease expires (a backup's seal, `ack=s3`) answers with fresh
        // S3 liveness and a marked lease, as it grants (`lock_serve`):
        // its successor's grace (`note_marker_landed`) covers what it
        // answered within `backup_takeover_ms` of its last proof, and
        // nothing later. Checking the lease's usability alone, a root
        // sealed by its backups kept renewing — its own grants at the
        // tick and its peers' — up to `lock_ttl` past that grace while
        // its lease ran (the model's
        // `fast_takeover_renewals_without_the_probe_violate`). Stale, the
        // extension is decided now and its answer held behind a lease
        // read (`lock_answer`, `renew_after_event`), refused if the read
        // shows a deposition. A delegate's authority is its delegation,
        // which the root renewed under the same rule (`on_deleg_renew`).
        let hold = if gen == 0 { self.renew_hold(now) } else { None };
        if gen != 0 && ttl < self.lock_min_grant_ms() {
            // A delegate short of authority: renew the delegation now
            // (the caller does), so the next renewal gives more.
            self.lk.deleg_renew_wanted.insert(gen);
        } else if gen == 0 && ttl < self.lock_min_grant_ms() {
            // The root, short of lease: renew it now (the caller does).
            self.lk.lease_renew_wanted = true;
        }
        let until = now.wait.plus((ttl + self.lock_margin_ms()).max(0) as u64);
        if late {
            if self.lock_final_blocks() {
                // The final mirror of this lease went: nothing is added
                // under it, and a late admission is an add.
                self.stats.lock_late_refused += 1;
                return LockRenewResult::NotOwner { owner: 0 };
            }
            let waiter = self
                .lk
                .waiters
                .iter()
                .any(|w| w.ino == ino && w.node != from && w.mode.conflicts(mode));
            return match replica
                .locks()
                .admit_late(id, from, mode, until, waiter, now, sent)
            {
                Some(a) => {
                    if from != self.cfg.node_id {
                        self.lock_need_horizon(until, until.plus((ttl / 4).max(0) as u64));
                    }
                    if hold.is_some() {
                        self.lk.fresh_need = hold;
                    }
                    self.lk.unproven.remove(&a.id);
                    self.lk.mirror_dirty = true;
                    self.stats.lock_late_admitted += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        from,
                        ino,
                        from_memory = a.from_memory,
                        "admitted a late lock renewal: nothing conflicting was granted since"
                    );
                    LockRenewResult::Ok {
                        ttl_ms: ttl as u64,
                        recalled: a.recalled,
                        id: a.id,
                        mode: a.mode,
                    }
                }
                None if gen == 0 && self.lock_mirror_pending() => self.lock_ask_again(),
                None => {
                    self.stats.lock_late_refused += 1;
                    tracing::info!(
                        node = self.cfg.node_id,
                        from,
                        ino,
                        "refused a late lock renewal: the grant's continuity is not proven"
                    );
                    LockRenewResult::Lost
                }
            };
        }
        // The grant renewed: that id, or this node's newer grant here (a
        // reply that never arrived replaced it: renew that one and say so
        // — sim seed 90013 fenced a healthy node otherwise). Only a record
        // whose mode covers the one the holder honours: the holder keeps
        // the stronger of its own and the answer's (`LockTables::renewed`),
        // so a weaker record renewed in its place left the holder
        // exclusive where this table knew it shared, and the table granted
        // a shared lock beside it (sim `locks-failover-backup-renew` seeds
        // 203170 and 203580: a fast successor's mirror, which the old root
        // last reached before the holder's shared grant became an
        // exclusive one, renewed the shared one). A weaker record is not
        // the grant: reclaimed during a grace like an unknown one (the
        // install replaces it), else lost. That holds for a weaker record
        // under the holder's own id too, without looking further for
        // another record of the node: a table holds one per node and
        // inode (`LockTables::install` replaces it).
        let target = match replica.locks().get(id) {
            Some(g) if g.node == from => Some(g),
            _ => replica.locks().own_grant(ino, from, now.wait),
        }
        .filter(|g| g.mode >= mode)
        .map(|g| g.id);
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
            if let Some((mode, recalled)) = replica.locks().extend(target, from, until, sent) {
                if from != self.cfg.node_id {
                    self.lock_need_horizon(until, until.plus((ttl / 4).max(0) as u64));
                }
                if hold.is_some() {
                    self.lk.fresh_need = hold;
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
        if gen == 0 && self.lock_mirror_pending() {
            return self.lock_ask_again();
        }
        // Unknown: a reclaim during a grace period, if nothing conflicts.
        let grace = self.lock_grace_active(now, ino, replica);
        let conflicting = replica.locks().conflicting(ino, from, mode, now.wait);
        if grace && conflicting.is_empty() {
            if from != self.cfg.node_id {
                self.lock_need_horizon(until, until);
            }
            if hold.is_some() {
                self.lk.fresh_need = hold;
            }
            // Its holder's renewal: confirmed from now on.
            // Not settled: nothing proves what was granted between the
            // grant it names and now (`Grant::settled`).
            replica.locks().install(Grant {
                id,
                node: from,
                ino,
                mode,
                until,
                recalled: false,
                gen,
                confirmed: Some(sent),
                settled: false,
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
        now: Now,
        from: NodeId,
        req: OpId,
        ino: Ino,
        mode: LockMode,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let outcome = match self.lock_route_answering(now, ino, replica, from, false) {
            Route::Me { .. } => {
                match replica.locks().first_conflicting(ino, from, mode, now.wait) {
                    Some(g) => LockTestOutcome::Held {
                        node: g.node,
                        mode: g.mode,
                    },
                    None => LockTestOutcome::Free,
                }
            }
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
        now: Now,
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
                        .first_conflicting(ino, self.cfg.node_id, mode, now.wait)
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
                self.set_timer_wait(
                    now.wait.plus(self.cfg.forward_timeout_ms),
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
        now: Now,
        op: OpId,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
        waited_ms: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_requests += 1;
        let since = self.lock_waited_since(now.wait, waited_ms);
        self.lk.ops.insert(
            op,
            LockOp {
                ino,
                mode,
                blocking,
                req: None,
                owner: 0,
                sent_at: now.honour,
                attempts: 0,
                created: now.wait,
                since,
                local_wait: false,
                reread: false,
                ask_lease: false,
            },
        );
        self.lock_route_op(now, op, replica, out);
    }

    /// Tests: the request is gone (answered elsewhere, or its engine gave
    /// up): what a read made for it still serves.
    #[cfg(test)]
    pub(crate) fn forget_lock_op(&mut self, op: OpId) {
        self.lk.ops.remove(&op);
    }

    fn lock_route_op(&mut self, now: Now, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        let (ino, mode, blocking) = (o.ino, o.mode, o.blocking);
        let waited_ms = now.wait.since(o.since).max(0) as u64;
        // A grant this node holds that is recalled or being released:
        // no request leaves until it is gone (a reply for it could cross
        // its own `LockReleased` and reinstall a forgotten grant — sim
        // seeds 90000/90004). A non-blocking request while local locks
        // still pin the recalled grant would block: say so.
        if let Some(h) = replica.locks().held(ino) {
            if h.until <= now.honour && replica.locks().lapse_pending(ino, now) {
                // Plan 43 C7-F F-c: lapsing — its late renewal is asked
                // for; a fresh grant would replace it and fence its owners.
                // Look again shortly.
                self.set_timer_wait(now.wait.plus(10), Timer::LockRetry(op), out);
                return;
            } else if h.until <= now.honour {
                // Lapsed: it is fenced already and cannot be renewed;
                // drop it (the owner outwaits it) and ask afresh. Plan 41
                // L1: dropped only if its window is over *now*, in one step
                // of the table: a window renewed since the look above (the
                // lock lane's) stays, and so does a grant that began
                // lapsing in between (another thread's look), whose park
                // the renewal tick resolves. Either way: look again.
                if !replica.locks().drop_held_if_lapsed(ino, h.id, now) {
                    self.set_timer_wait(now.wait.plus(10), Timer::LockRetry(op), out);
                    return;
                }
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
                self.set_timer_wait(now.wait.plus(10), Timer::LockRetry(op), out);
                return;
            }
        }
        let mut route = self.lock_route(now, ino, replica);
        // The table's route led to a dead end last time: ask the lease, as
        // for an unknown owner (`lock_delegated_dead_end`).
        if let (Route::Node(_), Some(o)) = (&route, self.lk.ops.get_mut(&op)) {
            if std::mem::take(&mut o.ask_lease) {
                route = Route::Unknown;
            }
        }
        match route {
            Route::Me { .. } => {
                let me = self.cfg.node_id;
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.sent_at = now.honour;
                    o.owner = me;
                }
                // Two readings of one step (plan 43 C7-H1): the honour
                // clock for the requester's stamp this node echoes, the
                // owner's record on its own (wall until the wait-side
                // commit).
                match self.lock_serve(
                    now,
                    me,
                    ino,
                    mode,
                    blocking,
                    None,
                    Some(op),
                    now.honour,
                    waited_ms,
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
                    if !self.lock_reread_holder(now, op, replica, out) {
                        self.lock_op_unreachable(now, op, replica, out);
                    }
                    return;
                }
                let req = self.op_id();
                let Some(o) = self.lk.ops.get_mut(&op) else {
                    return;
                };
                o.req = Some(req);
                o.owner = n;
                o.sent_at = now.honour;
                o.attempts += 1;
                self.lk.by_req.insert(req, op);
                self.set_timer_wait(
                    now.wait.plus(self.cfg.forward_timeout_ms),
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
                        sent: now.honour,
                        incarnation: self.cfg.incarnation,
                        waited_ms,
                    },
                });
            }
            Route::Unknown => {
                if self.lock_op_while_resuming(now, op, replica, out) {
                    return;
                }
                if !self.cfg.p2p {
                    self.lock_op_unreachable(now, op, replica, out);
                    return;
                }
                if let Some(o) = self.lk.ops.get_mut(&op) {
                    o.attempts += 1;
                }
                self.lock_read_holder(now, op, replica, out);
            }
        }
    }

    /// No P2P path to the owner (or none that works: its link is dead
    /// and the lease, read again, still names it): a blocking request
    /// keeps trying; a non-blocking one over P2P is handed back to its
    /// engine to wait (plan 43 F-b, the maintainer's decision of
    /// 2026-10-19: a stale owner is a failover in progress, and a lock
    /// request in a failover costs latency, not `ENOLCK`); the engine asks
    /// again as a fresh request (fresh attempts, a fresh lease read,
    /// routed to the successor as soon as the lease names it), up to its
    /// bound. Without P2P there is no path at all: `ENOLCK`, as documented.
    fn lock_op_unreachable(
        &mut self,
        now: Now,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if o.blocking && self.cfg.p2p {
            self.lock_retry(now, op, replica, out);
            return;
        }
        self.lock_give_up(op, out);
    }

    /// A non-blocking request out of attempts, its lease read spent: told
    /// to wait if there is a P2P path to wait for, else `Unavailable`
    /// ([`Self::lock_op_unreachable`]).
    fn lock_give_up(&mut self, op: OpId, out: &mut Vec<Action>) {
        self.lk.ops.remove(&op);
        if self.cfg.p2p {
            self.stats.lock_unreachable_waits += 1;
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lock(LockAnswer::Wait {
                    retry_ms: self.lock_resume_retry_ms(),
                })),
            });
        } else {
            self.stats.lock_unavailable += 1;
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lock(LockAnswer::Unavailable)),
            });
        }
    }

    /// The holder this node asked (or would ask) cannot be reached: it
    /// may be stale — it died, and a successor took the lease before
    /// gossip said so (the harness's lock-failover: a non-blocking `flock`
    /// answered `ENOLCK` while the backup already served). Re-read the
    /// lease, once per request, as for an unknown holder, before a
    /// non-blocking request gives up. `false`: read already (or no P2P).
    fn lock_reread_holder(
        &mut self,
        now: Now,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
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
        self.lock_read_holder(now, op, replica, out);
        true
    }

    /// Plan 43 C7-F F-b N3: read the lease for `op` — who holds the root —
    /// *shared*: one read in flight for every request that needs it, whose
    /// result they all take, and a result answered less than
    /// [`LOCK_HOLDER_READ_TTL_MS`] ago, taken as it stands unless gossip has
    /// moved the holder since. The read made for each request, one per
    /// request, is what a node with hundreds of requests waiting for a dead
    /// owner paid per second.
    fn lock_read_holder(
        &mut self,
        now: Now,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(f) = self
            .lk
            .holder_flight
            .as_mut()
            .filter(|f| self.s3.contains_key(&f.s3))
        {
            if !f.waiting.contains(&op) {
                f.waiting.push(op);
            }
            self.stats.lock_holder_read_shared += 1;
            return;
        }
        self.lk.holder_flight = None;
        let fresh = self.lk.holder_done.as_ref().is_some_and(|d| {
            now.wait.0 - d.at.0 < LOCK_HOLDER_READ_TTL_MS
                && d.holder == self.lease.cached_holder
                && d.at.0 <= now.wait.0
        });
        if fresh && self.lk.holder_depth < 2 {
            let read = self.lk.holder_done.as_ref().expect("fresh").read.clone();
            self.stats.lock_holder_read_cached += 1;
            self.lk.holder_depth += 1;
            // (No sighting: `HolderRead` carries no tag to observe.)
            self.lock_holder_applied(now, op, &read, replica, out);
            self.lk.holder_depth -= 1;
            return;
        }
        self.stats.lock_holder_reads += 1;
        let s3 = self.issue_s3(S3Op::LeaseGet, super::S3For::LockHolder(op), out);
        self.lk.holder_flight = Some(HolderFlight {
            s3,
            waiting: vec![op],
        });
    }

    /// The lease observation's key and stamps (plan 43 C7b), which a
    /// reused lease read must leave as they are.
    #[cfg(test)]
    pub(crate) fn lease_obs_stamp(
        &self,
    ) -> Option<(constellation_store_s3::RenewalKey, WaitMs, WaitMs)> {
        self.lease.obs.as_ref().map(|o| (o.key, o.since, o.read_at))
    }

    fn lock_retry(&mut self, now: Now, op: OpId, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(o) = self.lk.ops.get(&op) else {
            return;
        };
        if !o.blocking && o.attempts >= self.cfg.forward_retries {
            // Out of attempts against the holder this node knew (its
            // requests timed out before the link was declared dead).
            if self.lock_reread_holder(now, op, replica, out) {
                return;
            }
            self.lock_give_up(op, out);
            return;
        }
        let delay = (self.cfg.forward_backoff_ms / 4).max(10) * u64::from(o.attempts.clamp(1, 8));
        self.set_timer_wait(now.wait.plus(delay), Timer::LockRetry(op), out);
    }

    pub(crate) fn on_lock_retry(
        &mut self,
        now: Now,
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

    /// The shared lease read ([`Self::lock_read_holder`]) came back: kept
    /// for [`LOCK_HOLDER_READ_TTL_MS`], and applied to every request that
    /// waited for it (the one it was issued for may be gone: the others
    /// still wait).
    pub(crate) fn on_lock_holder_learned(
        &mut self,
        now: Now,
        op: OpId,
        result: crate::event::S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let read = HolderRead::of(&result);
        if let HolderRead::Lease(lease) = &read {
            self.lease.note_object(lease);
        }
        let mut waiting = match self.lk.holder_flight.take() {
            Some(f) => f.waiting,
            None => Vec::new(),
        };
        if !waiting.contains(&op) {
            waiting.push(op);
        }
        self.lk.holder_done = Some(HolderDone {
            at: now.wait,
            read: read.clone(),
            holder: self.lease.cached_holder,
        });
        for w in waiting {
            self.lock_holder_applied(now, w, &read, replica, out);
        }
    }

    /// What `op` does with what the lease says (read for it, or shared).
    fn lock_holder_applied(
        &mut self,
        now: Now,
        op: OpId,
        read: &HolderRead,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.lk.ops.contains_key(&op) {
            return;
        }
        if let HolderRead::Lease(lease) = read {
            if lease.holder != 0
                && !self.lease.claimable_observed(now, &self.cfg)
                && lease.holder != self.cfg.node_id
            {
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
        if matches!(read, HolderRead::Failed) {
            if let Some(owner) = self.lock_owner_without_s3(now) {
                self.lease.cached_holder = Some(owner);
                self.lock_route_op(now, op, replica, out);
                return;
            }
        }
        // Nobody usable holds it: a lock needs a sequencer as a write
        // does — acquire (the sim found blocked lockers with nobody
        // taking the lease). Non-blocking: unavailable after the retries.
        let claimable = match read {
            HolderRead::Lease(l) => l.holder == 0 || self.lease.claimable_observed(now, &self.cfg),
            HolderRead::Empty => true,
            HolderRead::Failed => false,
        };
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
        self.lock_retry(now, op, replica, out);
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
        now: Now,
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
        if now.wait.0 - o.created.0 >= self.cfg.s3_less_deadline_ms as i64 {
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
        self.set_timer_wait(now.wait.plus(retry), Timer::LockRetry(op), out);
        true
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_reply(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            self.lock_late_reply(now, from, req, outcome, replica, out);
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
        now: Now,
        from: NodeId,
        req: OpId,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((ino, sent)) = self.lk.done_reqs.remove(&req) else {
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
        // The window counts from the RPC's send, as every grant's does
        // (`lock_op_outcome`), not from the reply's arrival: one handled
        // late (a stalled step, a reply slow in transit) was honoured
        // past the owner's record of it — the owner outwaited it and
        // granted another node while this one still did I/O under it
        // (sim `locks-stall-step` seed 1934: two exclusive holders).
        if sent.0 + ttl - margin <= now.honour.0 {
            return;
        }
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
                .renewed(ino, id, id, mode, sent, ttl, margin, false, from, now);
            self.stats.lock_late_replies_installed += 1;
            // The grant has a new renewal point and no renewal in flight
            // (`renewed` clears it): the tick must come by then. Armed at
            // the cadence while the renewal was out, it came after the
            // window and the grant lapsed under the application's I/O (sim
            // `locks-delegated-production-ttl-heavy` seed 3850: a grant
            // capped at 2.1 s, the tick 5 s out).
            self.lock_arm_renew_tick(now, replica, out);
            return;
        }
        let held = HeldGrant {
            id,
            mode,
            until: HonourMs(sent.0 + ttl - margin),
            renew_at: constellation_meta::locks::renew_point(sent, ttl, margin),
            owner: from,
            recalled: false,
            position,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: now.honour,
            lapsing: None,
            written: false,
            route: Some(from),
        };
        if matches!(
            replica.locks().install_held(ino, held),
            Installed::Ok { .. }
        ) {
            self.stats.lock_late_replies_installed += 1;
            // A new grant, with its own renewal point (see above).
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    /// A grant pushed to a waiter whose request was answered `Waiting`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_granted_push(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        sent: HonourMs,
        outcome: LockOutcome,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let LockOutcome::Waiting { .. } = outcome {
            // The owner's ask to ask again (`lock_try_grant`): it is this
            // node's turn, but its last request is too old for a grant to
            // be pushed. Every op of this node waiting on the inode with
            // no request in flight sends one now, as its retry timer
            // would (ops of one mode re-attach the same waiter). This
            // grants nothing and installs nothing, and it leaves an op
            // with a request in flight alone: that request is answered by
            // its own reply, which a generic push would have moved to
            // `done_reqs`, where the grant it brings is dropped and
            // outwaited. A timer of the op that fires later re-sends once
            // more at worst (`on_lock_retry`). A pushed `Waiting` was
            // never sent before: pushes carried `Granted` or `NotOwner`.
            let ops: Vec<OpId> = self
                .lk
                .ops
                .iter()
                .filter(|(_, o)| o.ino == ino && o.req.is_none() && !o.local_wait)
                .map(|(op, _)| *op)
                .collect();
            if ops.is_empty() && !self.lk.ops.values().any(|o| o.ino == ino) {
                // No op of this node waits for the inode at all: its
                // waiter there is a ghost (an op another op's grant
                // covered, or that left): say so, so that the owner does
                // not hold its queue for it.
                self.lock_send_cancel(now.honour, from, ino, true, true, out);
                return;
            }
            for op in ops {
                self.stats.lock_asks_answered += 1;
                self.lock_route_op(now, op, replica, out);
            }
            return;
        }
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
                self.lk.done_reqs.insert(req, (ino, o.sent_at));
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

    /// A grant was installed that is not recalled: answer every other op
    /// of this node on the inode that it covers now, at its position,
    /// instead of leaving each to its own request or its re-send (9.5 s at
    /// the production lock ttl: a push wakes the one op it is matched to).
    /// An op's request in flight is moved to `done_reqs`, as a push moves
    /// it, so its reply installs over the held id. The waiters these ops
    /// left at their owners are cancelled (`LockCancel`) unless an op of
    /// the mode remains. Not for a grant that arrives recalled (its first
    /// use admits one local lock; the others would only ask again), nor
    /// for the ops queued in this node's own table.
    fn lock_wake_covered(
        &mut self,
        now: Now,
        ino: Ino,
        position: &Position,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(held) = replica.locks().held(ino) else {
            return;
        };
        let ids: Vec<OpId> = self
            .lk
            .ops
            .iter()
            .filter(|(_, o)| o.ino == ino && !o.local_wait && held.mode.covers(o.mode))
            .map(|(op, _)| *op)
            .collect();
        if ids.is_empty() {
            return;
        }
        let me = self.cfg.node_id;
        let mut woken: BTreeMap<NodeId, (bool, bool)> = BTreeMap::new();
        for op in ids {
            let Some(o) = self.lk.ops.remove(&op) else {
                continue;
            };
            if let Some(req) = o.req {
                self.lk.by_req.remove(&req);
                self.lk.done_reqs.insert(req, (ino, o.sent_at));
                while self.lk.done_reqs.len() > DONE_REQS_KEPT {
                    self.lk.done_reqs.pop_first();
                }
            }
            if o.owner != 0 && o.owner != me {
                let e = woken.entry(o.owner).or_default();
                match o.mode {
                    LockMode::Exclusive => e.0 = true,
                    LockMode::Shared => e.1 = true,
                }
            }
            self.stats.lock_covered_woken += 1;
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lock(LockAnswer::Granted {
                    position: *position,
                })),
            });
        }
        for (owner, (exclusive, shared)) in woken {
            // Only the modes no op of this node on the inode still waits
            // for.
            let left = |m: LockMode| {
                self.lk
                    .ops
                    .values()
                    .any(|o| o.ino == ino && o.mode == m && !o.local_wait)
            };
            let exclusive = exclusive && !left(LockMode::Exclusive);
            let shared = shared && !left(LockMode::Shared);
            if exclusive || shared {
                self.lock_send_cancel(now.honour, owner, ino, exclusive, shared, out);
            }
        }
    }

    /// A grant arrived recalled: its first use admits one local lock, and
    /// the other ops of this node on the inode are not answered with it.
    /// The waiter the owner kept for them was the one its answer just
    /// served (ops of one mode share a waiter), so none is parked there any
    /// more and their own retry is a re-send away (9.5 s at the production
    /// lock ttl): they ask again now, from where their wait began
    /// (`waited_ms`), which keeps their place in the queue.
    fn lock_resend_idle(
        &mut self,
        now: Now,
        ino: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let ops: Vec<OpId> = self
            .lk
            .ops
            .iter()
            .filter(|(_, o)| o.ino == ino && o.req.is_none() && !o.local_wait)
            .map(|(op, _)| *op)
            .collect();
        for op in ops {
            self.lock_route_op(now, op, replica, out);
        }
    }

    /// Tell `owner` that no op of this node waits for `ino` in the modes
    /// named (`LockCancel`): its waiters of this node there that were last
    /// sent before now are dropped (`now`: the requester's honour clock,
    /// the clock of `LockRequest::sent`).
    fn lock_send_cancel(
        &mut self,
        now: HonourMs,
        owner: NodeId,
        ino: Ino,
        exclusive: bool,
        shared: bool,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_cancels_sent += 1;
        out.push(Action::Send {
            to: owner,
            msg: PeerMsg::LockCancel {
                ino,
                exclusive,
                shared,
                upto: now,
            },
        });
    }

    /// Owner side: `from` no longer waits for `ino` in the modes named:
    /// drop its parked waiters there (those it last sent before `upto`).
    /// They would only be asked in vain, a turn of the queue each. A held
    /// request is answered `Waiting`, as every removed waiter's is.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_cancel(
        &mut self,
        now: Now,
        from: NodeId,
        ino: Ino,
        exclusive: bool,
        shared: bool,
        upto: HonourMs,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut dropped = 0u64;
        let mut i = 0;
        while i < self.lk.waiters.len() {
            let w = &self.lk.waiters[i];
            let mode_hit = match w.mode {
                LockMode::Exclusive => exclusive,
                LockMode::Shared => shared,
            };
            if w.node == from && w.ino == ino && w.op.is_none() && mode_hit && w.sent < upto {
                let w = self.lk.waiters.remove(i);
                if let Some(t) = w.held_timer {
                    self.cancel_timer(t, out);
                }
                if let Some(req) = w.req {
                    let outcome = LockOutcome::Waiting {
                        retry_ms: self.lock_resume_retry_ms(),
                        grace: false,
                    };
                    self.lock_answer(now, from, PeerMsg::LockReply { req, outcome }, out);
                }
                dropped += 1;
            } else {
                i += 1;
            }
        }
        if dropped > 0 {
            self.stats.lock_cancelled += dropped;
            self.lk.served.remove(&(from, ino));
            tracing::debug!(
                node = self.cfg.node_id,
                from,
                ino,
                dropped,
                "a node cancelled its waiters: nothing there waits for the lock any more"
            );
            self.lock_serve_waiters(now, ino, replica, out);
        }
    }

    fn lock_op_outcome(
        &mut self,
        now: Now,
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
                if o.sent_at.0 + ttl - margin <= now.honour.0 + margin / 4 {
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
                    self.lock_retry(now, op, replica, out);
                    return;
                }
                let held = HeldGrant {
                    id,
                    mode,
                    until: HonourMs(o.sent_at.0 + ttl - margin),
                    renew_at: constellation_meta::locks::renew_point(o.sent_at, ttl, margin),
                    owner: from,
                    recalled: false,
                    position,
                    renewing: None,
                    releasing: false,
                    first_use: false,
                    idle_since: None,
                    installed: now.honour,
                    lapsing: None,
                    written: false,
                    route: Some(from),
                };
                let ino = o.ino;
                let waited = now.wait.since(o.created).max(0) as u64;
                let recalled = match replica.locks().install_held(ino, held) {
                    Installed::Ok { recalled } => recalled,
                    Installed::Released => {
                        // This node released that id already (the reply
                        // crossed the release): ask again.
                        self.stats.lock_released_replies += 1;
                        self.lock_retry(now, op, replica, out);
                        return;
                    }
                };
                self.lk.ops.remove(&op);
                self.stats.lock_grant_ms_total += waited;
                let b = (64 - waited.max(1).leading_zeros()).min(13) as usize;
                self.stats.lock_grant_ms[b] += 1;
                if held.renew_at <= now.honour {
                    // Served late in its window (a push to a request
                    // parked a while): renew now, not at the next tick,
                    // which may come after the window.
                    if let Some(t) = self.lk.renew_timer.take() {
                        self.cancel_timer(t, out);
                    }
                    let t = self.set_timer_honour(now.honour.plus(1), Timer::LockRenewTick, out);
                    self.lk.renew_timer = Some(t);
                    self.lk.renew_at = Some(now.honour.plus(1));
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
                    self.lock_resend_idle(now, ino, replica, out);
                } else {
                    self.lock_wake_covered(now, ino, &position, replica, out);
                }
            }
            LockOutcome::Waiting {
                retry_ms,
                grace: true,
            } => {
                // The owner may not grant yet, and nothing conflicts
                // (plan 43 F-b). A non-blocking request is handed back
                // to the engine to wait on a thread of its own, up to
                // its bound, without spending an attempt; a blocking one
                // (the owner parks those: a race) asks again.
                if o.blocking {
                    self.set_timer_wait(now.wait.plus(retry_ms), Timer::LockRetry(op), out);
                } else {
                    self.lk.ops.remove(&op);
                    out.push(Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Lock(LockAnswer::Wait { retry_ms })),
                    });
                }
            }
            LockOutcome::Waiting {
                retry_ms,
                grace: false,
            } => {
                // A non-blocking request is told to wait this way only by
                // an owner about to own the root again (`lock_serve`):
                // within the bound an op without its sequencer has; past
                // it, as `Busy`.
                let late =
                    !o.blocking && now.wait.0 - o.created.0 >= self.cfg.s3_less_deadline_ms as i64;
                if late {
                    self.lock_retry(now, op, replica, out);
                } else {
                    self.set_timer_wait(now.wait.plus(retry_ms), Timer::LockRetry(op), out);
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
                if self.lock_delegated_dead_end(from, ino, owner, replica) {
                    if let Some(o) = self.lk.ops.get_mut(&op) {
                        o.ask_lease = true;
                    }
                }
                self.lock_note_not_owner(from, ino, owner, replica);
                if owner != 0 && owner != from && owner != self.cfg.node_id {
                    let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                    if !names_delegate {
                        self.lease.cached_holder = Some(owner);
                    }
                } else if from != self.cfg.node_id
                    && (owner == 0 || self.lease.cached_holder == Some(from))
                    && !(owner == 0 && self.lock_delegated_here(ino, replica))
                {
                    // The node asked as the owner names no other one (or
                    // names this node, or itself): what this node cached
                    // is stale; the next attempt reads the lease rather
                    // than ask it again. (Not when this node's table
                    // delegates the inode to this node: see
                    // `lock_delegated_here`.)
                    self.lease.cached_holder = None;
                }
                self.lock_retry(now, op, replica, out);
            }
            LockOutcome::Busy => self.lock_retry(now, op, replica, out),
        }
    }

    pub(crate) fn on_lock_request_timeout(
        &mut self,
        now: Now,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(op) = self.lk.by_req.remove(&req) else {
            return;
        };
        if let Some(o) = self.lk.ops.get_mut(&op) {
            if o.req == Some(req) {
                o.req = None;
                self.lock_retry(now, op, replica, out);
            }
        }
    }

    /// A lock request, test, recall or renewal failed at the transport.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lock_request_failed(
        &mut self,
        now: Now,
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
            self.lock_retry(now, op, replica, out);
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
    fn lock_arm_renew_tick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        // The whole tick is on the honour clock (plan 43 C7-H1): its
        // cadence, the grants' renewal points (honour stamps) and the
        // relearn floor are durations and stamps of this holder's own
        // windows.
        let mut at = now.honour.plus((self.cfg.lock_ttl_ms / 4).max(50));
        if let Some(first) = replica.locks().next_renewal(now) {
            at = at.min(first.max(now.honour.plus(1)));
        }
        // Plan 43 C7-F F-c: a lapsing grant asks for its late renewal again
        // until it is answered or gives up, at the owner-poll cadence.
        if replica.locks().lapsing_any() {
            at = at.min(now.honour.plus(self.cfg.lock_poll_ms().clamp(1, 1_000)));
        }
        at = at.max(self.lock_relearn_floor(now));
        if let Some((t, armed)) = self.lk.renew_timer.zip(self.lk.renew_at) {
            if armed <= at {
                return;
            }
            self.cancel_timer(t, out);
        }
        let t = self.set_timer_honour(at, Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
        self.lk.renew_at = Some(at);
    }

    pub(crate) fn on_lock_renew_tick(
        &mut self,
        now: Now,
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
            .idle_before(now.wait.minus(self.cfg.lock_cache_idle_ms));
        for (ino, _) in idle {
            self.stats.lock_idle_released += 1;
            self.lock_release_begin(now, ino, replica, out);
        }
        // A recalled grant whose first local lock never came (the
        // requester gave up, or is gone) stops being pinned by it: it is
        // released below, not renewed for ever under the waiters.
        for ino in replica.locks().expire_first_use(now) {
            tracing::info!(
                node = self.cfg.node_id,
                ino,
                "a recalled lock grant was never used by its requester; releasing it"
            );
        }
        // A recalled grant with no local lock under it is released, not
        // renewed; a lapsed one is dropped (sim seed 90010: a grant that
        // answered an op it did not cover was neither used nor released) —
        // unless it pins local locks or writes: plan 43 C7-F F-c, it is
        // then *lapsing*, its I/O waits and a late renewal is asked for
        // (below), until the owner admits or refuses it or the park's bound
        // passes (`LockTables::lapse_tick`).
        let lapse = replica.locks().lapse_tick(now);
        self.stats.lock_lapse_parks_gave_up += lapse.gave_up as u64;
        for (ino, h) in replica.locks().held_all() {
            if h.until > now.honour && h.recalled && !h.releasing && replica.locks().local_idle(ino)
            {
                self.lock_release_begin(now, ino, replica, out);
            }
        }
        let mut due: Vec<(Ino, HeldGrant, bool)> = replica
            .locks()
            .due_renewals(now)
            .into_iter()
            .map(|(ino, h)| (ino, h, false))
            .collect();
        due.extend(lapse.late.into_iter().map(|(ino, h)| (ino, h, true)));
        if !due.is_empty() {
            tracing::debug!(
                node = self.cfg.node_id,
                due = due.len(),
                cached_holder = ?self.lease.cached_holder,
                "lock renewals due"
            );
        }
        let mut by_owner: BTreeMap<NodeId, Vec<(Ino, HeldGrant, bool)>> = BTreeMap::new();
        for (ino, h, late) in due {
            // Routed as a peer's renewal is (`lock_renew_one`): a lease
            // whose gate is pending serves renewals. Its own lockers'
            // found no owner and spun until their grants lapsed under
            // their I/O (a re-claim whose marker S3 cut again).
            match self.lock_route_for(now, ino, replica, true) {
                Route::Me { .. } => {
                    let me = self.cfg.node_id;
                    let r = self.lock_renew_one(
                        now,
                        me,
                        ino,
                        h.id,
                        h.mode,
                        late,
                        (self.cfg.clock_id, now.honour.0),
                        replica,
                    );
                    if let Some(f) = self.lk.fresh_need.take() {
                        // Plan 41 L0: the tenure's liveness is stale; the
                        // grant stays `renewing` (not due again) until the
                        // lease read proves the tenure (applied as renewed
                        // now) or shows it gone (failed, and due again).
                        self.stats.renew_held += 1;
                        self.lk.own_held.push((f, ino, h.id, r));
                        self.renew_kick(&[f], out);
                    } else {
                        // Two readings of one step (plan 43 C7-H1): the
                        // honour clock for the window this node honours,
                        // the wall for the owner's record in
                        // `lock_renew_one`.
                        self.lock_apply_renew_result(
                            now, me, ino, h.id, now.honour, r, replica, out,
                        );
                    }
                    self.lock_renew_authority(now, out);
                }
                Route::Node(n) if self.cfg.p2p && self.reaches(now, n) => {
                    by_owner.entry(n).or_default().push((ino, h, late));
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
            let timer = self.set_timer_honour(
                now.honour.plus(self.cfg.forward_timeout_ms),
                Timer::LockRenewTimeout(req),
                out,
            );
            self.lk.renews.insert(
                req,
                RenewInFlight {
                    entries: entries.iter().map(|(i, h, _)| (*i, h.id)).collect(),
                    sent: now.honour,
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
                        .map(|(ino, h, late)| LockRenewEntry {
                            ino,
                            grant: h.id,
                            mode: h.mode,
                            late,
                        })
                        .collect(),
                    sent: now.honour,
                    clock: self.cfg.clock_id,
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
    /// So, too, while the predecessor's lock table is not installed yet
    /// (`lock_mirror_pending`): its own grants' renewals are answered
    /// `NotOwner` naming this node until then.
    /// And after an owner asked to be asked again (`NotOwner` naming
    /// itself, `ask_again_at`): the grant is due again at once, and the
    /// tick re-armed for it sent the renewal back to that owner every
    /// millisecond until its mirror was in.
    fn lock_relearn_floor(&self, now: Now) -> HonourMs {
        let floor = if self.lk.relearning || self.lock_mirror_pending() {
            now.honour.plus(self.lock_resume_retry_ms())
        } else {
            now.honour
        };
        self.lk.ask_again_at.map_or(floor, |at| floor.max(at))
    }

    /// The answer to a renewal of a grant this root does not know while the
    /// predecessor's lock table is not in it yet (`lock_mirror_pending`):
    /// not lost, perhaps only not installed. Asked again here (`NotOwner`
    /// naming this node: the holder asks again after a retry,
    /// `ask_again_at`; this node's own tick waits behind
    /// `lock_relearn_floor`). Answered `Lost`, a locker whose renewal
    /// reached the successor between its claim and its mirror's install
    /// was fenced (sim `locks-failover-stepped`, once a lane renewal's
    /// timeout made the lockers read the lease again at once). Only for an
    /// unknown grant: a grant this table knows is renewed as before (a
    /// lease whose gate is pending serves renewals: `on_lock_renew_tick`),
    /// and a late renewal it can admit is admitted. The gate clears the
    /// tail in its first step after the marker lands, whether or not a
    /// mirror came, so this lasts as long as the marker's write (and the
    /// tail's apply); a holder whose window ends meanwhile lapses and asks
    /// late, as from an owner that does not answer, until its park's bound.
    fn lock_ask_again(&self) -> LockRenewResult {
        LockRenewResult::NotOwner {
            owner: self.cfg.node_id,
        }
    }

    /// A successor that backed its predecessor installs the predecessor's
    /// lock table (its last mirror) with the backup tail, in its gate's step
    /// after the epoch marker lands (`lock_install_mirror`): until then the
    /// grants the mirror lists are not in this table.
    fn lock_mirror_pending(&self) -> bool {
        self.lease
            .gate
            .as_ref()
            .is_some_and(|g| g.backup_tail_epoch.is_some())
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
    pub(crate) fn lock_owner_without_s3(&self, now: Now) -> Option<NodeId> {
        let me = self.cfg.node_id;
        let carrier = self
            .pr
            .carried
            .filter(|_| self.epoch.active)
            .map(|c| c.node);
        let seen = self
            .lease
            .live_holder_seen(now, &self.cfg)
            .map(|(holder, _)| holder);
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
        now: Now,
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
            self.lease.note_object(lease);
            if lease.holder != 0
                && !self.lease.claimable_observed(now, &self.cfg)
                && lease.holder != self.cfg.node_id
            {
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
        let t = self.set_timer_honour(now.honour.plus(1), Timer::LockRenewTick, out);
        self.lk.renew_timer = Some(t);
        self.lk.renew_at = Some(now.honour.plus(1));
    }

    pub(crate) fn on_lock_renewed(
        &mut self,
        now: Now,
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
        now: Now,
        from: NodeId,
        ino: Ino,
        id: GrantId,
        sent: HonourMs,
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
                    sent,
                    ttl_ms as i64,
                    self.lock_margin_ms(),
                    recalled,
                    from,
                    now,
                );
                self.stats.lock_renewals += 1;
                if recalled {
                    self.lock_release_begin(now, ino, replica, out);
                }
            }
            LockRenewResult::Lost => {
                if replica.locks().lost(ino, id, now) {
                    self.stats.lock_lost += 1;
                    tracing::warn!(
                        node = self.cfg.node_id,
                        ino,
                        "lock grant lost (the owner does not know it); I/O under local locks is fenced"
                    );
                } else {
                    // The answer is for an id this node no longer holds (a
                    // recall made it adopt a newer one): the grant held now
                    // is asked about again (a lapsing one, by its late
                    // renewal at the next tick).
                    replica.locks().renewal_failed(ino, id);
                }
            }
            LockRenewResult::NotOwner { owner } => {
                let names_delegate = replica.delegation_table().iter().any(|e| e.node == owner);
                if owner != 0 && owner != self.cfg.node_id && !names_delegate {
                    self.lease.cached_holder = Some(owner);
                } else if owner == 0
                    && from != self.cfg.node_id
                    && self.lease.cached_holder == Some(from)
                    && !self.lock_delegated_here(ino, replica)
                {
                    // The owner this node cached names no other one (a
                    // root that found itself deposed, plan 41 L0): the next
                    // renewal reads the lease, as a request's retry does,
                    // rather than ask it again at once — the grant is due
                    // again, and the tick re-armed for it fired every
                    // millisecond at the old root.
                    self.lease.cached_holder = None;
                }
                if owner == from && from != self.cfg.node_id {
                    // "Ask again" (`lock_renew_one`: a successor whose
                    // predecessor's lock table is not installed yet): after
                    // a retry, not at once.
                    self.lk.ask_again_at = Some(now.honour.plus(self.lock_resume_retry_ms()));
                }
                self.lock_note_not_owner(from, ino, owner, replica);
                replica.locks().clear_route(ino, id, from);
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
        now: Now,
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
            late_ms = now.honour.since(late.sent),
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
        now: Now,
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
        now: Now,
        ino: Ino,
        grant: GrantId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match replica.locks().recall_held(ino, grant) {
            Some(false) => self.lock_release_begin(now, ino, replica, out),
            Some(true) => {
                // Local locks under it: released when the last one
                // leaves (`Control::LockIdle`), renewed until then. An
                // adopted newer id has no renewal in flight any more
                // (`LockTables::recall_held`): the tick is armed for it.
                self.lock_arm_renew_tick(now, replica, out);
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
        now: Now,
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

    /// Phase 2: the end, on this node's **wait** clock, of the latest
    /// token window naming `grant` among this node's own queued replays
    /// (stranded ops not resolved yet), if one is still open at `now`.
    ///
    /// The wait for a window to run out must not be shortened by a forward
    /// wall step of this node, so the window's wall end is mapped onto the
    /// wait clock once, when the op is queued ([`Self::lock_note_replay_pins`],
    /// after each strand), not at each check (a conversion at the check is
    /// the wall comparison again). A queue this process did not strand (a
    /// restart) has no twin: it is taken when first seen here, which is the
    /// wall comparison of that moment (plan 43 Appendix B row 14, C7a's
    /// boot-id form).
    fn replay_blocks_release(
        &mut self,
        grant: GrantId,
        now: Now,
        replica: &dyn Replica,
    ) -> Option<WaitMs> {
        self.lock_note_replay_pins(now, replica);
        self.lk
            .replay_pins
            .iter()
            .filter(|((g, _), until)| *g == grant && **until > now.wait)
            .map(|(_, until)| *until)
            .max()
    }

    /// Record, for every token of this node's own queued replays (stranded
    /// ops not resolved yet) not recorded yet, the end of its window on
    /// this node's wait clock (`replay_blocks_release`, plan 43 C7-H1 R8);
    /// forget the windows that are over, and those of replays no longer
    /// queued (resolved).
    pub(crate) fn lock_note_replay_pins(&mut self, now: Now, replica: &dyn Replica) {
        let Ok(queued) = replica.pending_replays() else {
            self.lk.replay_pins.clear();
            return;
        };
        // Per token: its identity (grant, wall stamp) and where its window
        // ends on this node's wait clock. A token this process minted ends
        // by its honour end (plan 43 C7b, row 6: both readings of this
        // clock); one a previous incarnation minted (another clock id; the
        // queue is persisted) can only be placed by the wall stamp it was
        // given, this node's own, as before (row 14).
        let queued: std::collections::BTreeMap<(GrantId, i64), WaitMs> = queued
            .iter()
            .filter(|q| q.refused.is_none() && !q.foreign)
            .flat_map(|q| q.lock_tag.0.iter())
            .map(|t| {
                let until = if t.clock == self.cfg.clock_id {
                    now.wait_of_honour(HonourMs(t.until_h))
                } else {
                    now.wait_of_wall(Ms(t.until_ms))
                };
                ((t.grant, t.until_ms), until)
            })
            .collect();
        self.lk
            .replay_pins
            .retain(|key, until| queued.contains_key(key) && *until > now.wait);
        for (key, until) in queued {
            if until > now.wait {
                self.lk.replay_pins.entry(key).or_insert(until);
            }
        }
    }

    /// F1: the ops that ended in doubt under `grant` whose outcome this
    /// replica knows now — their `Completed` (or refusal) came with the
    /// log: executed (so committed) or refused, either way not still
    /// landing — are doubts no more; a grant with none left is released
    /// as usual. One without a rid stays a doubt.
    fn lock_resolve_doubts(grant: GrantId, replica: &dyn Replica) {
        let Some((_, rids)) = replica.locks().doubt_of(grant) else {
            return;
        };
        for rid in rids {
            if matches!(replica.completed_outcome(rid), Ok(Some(_))) {
                replica.locks().resolve_doubt(grant, rid);
            }
        }
    }

    /// F1: one of this node's own stranded ops tagged with `grant` is
    /// still queued for its replay, unresolved, its token's window over (a
    /// release waited for that): it may be executing at its new sequencer
    /// already, as an op in doubt may.
    fn replay_unresolved(grant: GrantId, replica: &dyn Replica) -> bool {
        replica.pending_replays().is_ok_and(|queued| {
            queued
                .iter()
                .filter(|q| q.refused.is_none() && !q.foreign)
                .any(|q| q.lock_tag.0.iter().any(|t| t.grant == grant))
        })
    }

    /// Phase 2: the last tagged mutation in flight under `ino`'s recalled
    /// grant was answered (`LockTables::set_release_wake`): a release
    /// waiting for it looks again now, not at its next poll.
    pub(crate) fn on_lock_release_wake(
        &mut self,
        now: Now,
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
        now: Now,
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
        now: Now,
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
            let blocked = replica.locks().release_blocked(grant, now).or_else(|| {
                self.replay_blocks_release(grant, now, replica)
                    .map(|until| Some(until.min(now.wait.plus(RELEASE_WAIT_POLL_MS as u64))))
            });
            if let Some(until) = blocked {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ino,
                    ?grant,
                    ?until,
                    "a recalled grant's release waits for mutations tagged with it"
                );
                let at = until.unwrap_or(now.wait.plus(RELEASE_WAIT_POLL_MS as u64));
                let timer = self.set_timer_wait(
                    at.max(now.wait.plus(1)),
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
        // F1: a mutation tagged with the grant ended in doubt (or was
        // stranded and queued for replay). Its token's window is over now,
        // so no executor *checks* it any more — but one that checked it
        // before may still be committing it (a slow writer ahead of it),
        // and a release would let the owner grant the lock at once, with a
        // floor that lacks the write. Not released, then: dropped here and
        // left to the owner's outwait, whose barrier waits for a cut that
        // every executor settles its tagged writes for (its own record,
        // when this node is the owner: `lock_own_outwait_barrier`).
        Self::lock_resolve_doubts(grant, replica);
        let doubted = replica.locks().was_doubted(grant) || Self::replay_unresolved(grant, replica);
        let ended = if doubted {
            replica.locks().drop_unreleased(ino, grant)
        } else {
            replica.locks().end_release(ino, grant)
        };
        if !ended {
            // Gone already, or a local lock (or the grant's first use)
            // appeared meanwhile: it stays, recalled; the last unlock
            // releases it again (sim seed 90106).
            return;
        }
        if doubted {
            self.stats.lock_doubt_lapses += 1;
            replica.locks().with_stats(|s| s.doubt_lapses += 1);
            tracing::info!(
                node = self.cfg.node_id,
                ino,
                ?grant,
                "a recalled lock grant is left to its owner's outwait: a mutation tagged with it ended in doubt"
            );
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
        grants: Vec<GrantWire>,
        floor: Position,
        final_for: Option<LeaseKey>,
    ) {
        if self.root_node() != Some(from) {
            return;
        }
        if self.lk.bk_mirror.as_ref().is_none_or(|(v, _, _)| ver > *v) {
            self.lk.bk_mirror = Some((ver, grants, final_for));
            // Floors only grow: joined across mirrors.
            self.lk.bk_floor = constellation_meta::locks::floor_join(&self.lk.bk_floor, &floor);
        }
    }

    /// A fast takeover (this node was the backup): the mirror is this
    /// tenure's table, restamped; M9's floor covers what it misses.
    ///
    /// Plan 43 C7-F F-c: a mirror is *trusted* only if it is the old root's
    /// **final** one — it names the very lease body this takeover replaced
    /// (`LockState::takeover_prev`), so the old root added nothing under
    /// that lease after it (`lock_final_step`). Its records then keep their
    /// `settled` flag and the ones it had expired become this table's lapsed
    /// memory (a late renewal of one is admitted, until a conflicting grant
    /// supersedes it). Any other mirror may have missed a later grant the
    /// old root made after outwaiting a record it lists: its records are
    /// installed unsettled, its lapsed ones dropped, and a late renewal of
    /// either is refused. A mirror that arrives after the takeover is
    /// ignored (the sender is no longer the root).
    pub(crate) fn lock_install_mirror(&mut self, now: Now, replica: &dyn Replica) {
        let floor = std::mem::take(&mut self.lk.bk_floor);
        self.lock_note_dir_floor(constellation_fs_core::types::ROOT_INO, &floor);
        let prev = self.lk.takeover_prev.take();
        let Some((_, grants, final_for)) = self.lk.bk_mirror.take() else {
            return;
        };
        let trusted =
            (final_for.is_some() && final_for == prev) || self.cfg.lock_trust_nonfinal_mirror;
        if final_for.is_some() {
            if trusted {
                self.stats.lock_mirror_final_installed += 1;
            } else {
                self.stats.lock_mirror_final_mismatch += 1;
            }
        }
        let until = self.restamp(now);
        let n = grants.len();
        let mut lapsed = Vec::new();
        for g in grants {
            // Unconfirmed: the mirror is asynchronous, and the previous
            // holder may have ended a grant it still lists (released, or
            // outwaited, and granted to another node since).
            // The mirror's own end (the previous holder's wall stamp) is
            // never read: the record is restamped on this node's wait clock.
            let grant = Grant {
                recalled: false,
                confirmed: Grant::UNCONFIRMED,
                settled: trusted && g.settled,
                ..g.into_grant(until)
            };
            if g.lapsed {
                // An expired record is no grant: nobody is blocked by it.
                if trusted {
                    lapsed.push(grant);
                }
                continue;
            }
            replica.locks().install(grant);
        }
        // After the live ones, which supersede what they conflict with.
        for g in lapsed {
            replica.locks().install_lapsed(g);
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
        // The predicate reads the store (an uncached read can take as long
        // as a stalled device): it runs outside the tables' mutex, which
        // every FUSE fence check and every renewal takes
        // (`LockTables::take_where_outside`, plan 41 L1b).
        let moved = replica.locks().take_where_outside(|ino| {
            let keys = Self::read_keys(ino, None);
            matches!(
                replica.resolve_ownership(&keys),
                constellation_meta::delegation::Ownership::Delegated(d) if d.gen == gen
            )
        });
        if !moved.is_empty() {
            self.lk.mirror_dirty = true;
            self.lk.handoff.entry(gen).or_default().extend(moved);
            self.dw.unsettle(gen);
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
            // (Unsettled only if it moved: the same floor again, as every
            // granting renewal re-computes it, leaves nothing behind.)
            if self.lk.handed_floor.insert(gen, floor) != Some(floor) {
                self.dw.unsettle(gen);
            }
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
        now: Now,
        gen: u64,
        replica: &dyn Replica,
    ) -> Vec<GrantWire> {
        if replica.locks().grants_len() == 0 {
            return Vec::new();
        }
        replica
            .locks()
            .grants_snapshot()
            .into_iter()
            .filter(|g| {
                g.gen == gen
                    && g.until > now.wait
                    && matches!(
                        replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                        constellation_meta::delegation::Ownership::Root
                    )
            })
            .map(|g| g.to_wire(now))
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
        now: Now,
        gen: u64,
        leaving: &[GrantWire],
        replica: &dyn Replica,
    ) -> Vec<(Ino, i64)> {
        self.lock_take_outwaited(now, replica);
        let me = self.cfg.node_id;
        for g in replica.locks().expired(now.wait) {
            if g.gen == gen && g.node != me {
                // A barrier is a wall value (row 9, C7b).
                self.lock_barrier(g.ino, now.wall_of_wait(g.until).0, Some(g.node));
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
        now: Now,
        gen: u64,
        leaving: Vec<GrantWire>,
        replica: &dyn Replica,
    ) {
        if self.lock_final_blocks() {
            return;
        }
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
                if e.node == g.node && e.ino == g.ino && e.until < until {
                    // Never sooner than the record's end *now* (plan 41
                    // L1): `e` was read above, and the owner's own record
                    // of this node may have been extended since.
                    replica.locks().install_keeping_later(Grant { until, ..e });
                }
                continue;
            }
            let g = Grant {
                gen: 0,
                ..g.into_grant(until)
            };
            if replica.locks().install_if_consistent(g, now.wait) {
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
    pub(crate) fn lock_take_back_left(&mut self, now: Now, gen: u64, replica: &dyn Replica) {
        if self.lock_final_blocks() {
            return;
        }
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
                confirmed: Grant::UNCONFIRMED,
                settled: false,
                ..g
            };
            if g.until > now.wait
                && !replica.locks().was_ended(g.id)
                && replica.locks().install_if_consistent(g, now.wait)
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

    /// Plan 41 L2: the certificate the lock lane judges by, as of this step
    /// ([`crate::lane::LockLaneView`]). Every excluded state is one whose
    /// core rule differs from "extend a live record up to the lease cap".
    pub fn lock_lane_view(&self, now: Now, replica: &dyn Replica) -> crate::lane::LockLaneView {
        let me = self.cfg.node_id;
        let margin = self.lock_margin_ms();
        let honour_end = self.lease.honour_until(&self.cfg);
        let root_ok = self.cfg.locks
            && !self.lease.lost
            && !self.lease.fenced()
            && !self.lease.epoch_held()
            && self.lease.usable(now, &self.cfg)
            && !self.epoch.open
            && !self.epoch.active
            && !self.dl.epoch_active
            && !self.deleg_known()
            && !replica.delegations_any()
            && !self.lock_owner_resuming(now)
            && !self.release_unmarked_in_flight()
            && !self.lock_final_blocks()
            && honour_end.is_some();
        crate::lane::LockLaneView {
            me,
            root_ok,
            cap_until: honour_end.map_or(HonourMs(0), |h| h.minus(margin.max(0) as u64)),
            fast: self.may_be_taken_over_early(),
            last_fresh: self.last_s3_fresh(),
            window_ms: self.cfg.backup_takeover_ms as i64,
            marked: self.tenure_marked(),
            owner: self
                .lease
                .cached_holder
                .filter(|h| *h != 0 && *h != me)
                .unwrap_or(0),
            clock: self.cfg.clock_id,
            lock_ttl_ms: self.lock_ttl_ms(),
            margin_ms: margin,
            forward_timeout_ms: self.cfg.forward_timeout_ms,
        }
    }

    /// Publish [`Core::lock_lane_view`] for the lane: the end of `handle`, so
    /// before the driver dispatches the step's actions (a release's CAS, an
    /// epoch's recall go out only after the certificate that forbids
    /// renewing has been published).
    pub(crate) fn lock_publish_lane(&self, now: Now, replica: &dyn Replica) {
        self.lane_cell.publish(if self.cfg.locks {
            self.lock_lane_view(now, replica)
        } else {
            Default::default()
        });
    }

    /// Plan 41 L2: the lane extended `renewed` of this node's own grants.
    /// What the core's own renewal does besides the extension: the mirror
    /// carries the longer record, and a window short of what a grant needs
    /// asks the lease (or the delegation) to be renewed now
    /// ([`Core::lock_renew_authority`]).
    pub(crate) fn on_lane_own(
        &mut self,
        now: Now,
        renewed: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.stats.lock_lane_own += renewed as u64;
        self.lk.mirror_dirty = true;
        self.lock_renew_authority(now, out);
        if replica.locks().held_count() > 0 {
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    /// Plan 41 L4: the owner `from` answered the lane's renewal sent at
    /// `sent`: the lane applied every `Ok` (monotone) already; each result is
    /// applied here as the core applies its own renewal's — idempotent for
    /// `Ok`; `Lost`, `NotOwner` and a recalled grant's release are the core's
    /// to decide.
    pub(crate) fn on_lane_renewed(
        &mut self,
        now: Now,
        from: NodeId,
        sent: HonourMs,
        results: Vec<(Ino, GrantId, LockRenewResult)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.note_p2p_result(now, from, true);
        for (ino, id, result) in results {
            self.stats.lock_lane_results += 1;
            self.lock_apply_renew_result(now, from, ino, id, sent, result, replica, out);
        }
        if replica.locks().held_count() > 0 {
            self.lock_arm_renew_tick(now, replica, out);
        }
    }

    /// The lane's renewal to `to` went unanswered: its grants are due again
    /// and no longer routed to `to` (`RenewLane`'s `Failed`). As after the
    /// core's own renewal's timeout (`on_lock_renew_timeout`), the owner is
    /// read again from the lease; the next certificate names the holder
    /// learned, where the lane's next renewal goes.
    pub(crate) fn on_lane_unanswered(
        &mut self,
        to: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if replica.locks().held_count() == 0 {
            return;
        }
        tracing::debug!(
            node = self.cfg.node_id,
            to,
            "lock lane renewal unanswered; relearning the owner"
        );
        self.lock_relearn_owner(out);
    }

    /// Root, plan 41 R2: the lock side has no content pending for a granting
    /// answer to `gen` beyond what the last one carried — no handoff waiting
    /// for the first renewal, no outwait barrier a cut must settle at the
    /// delegate. (The floor is compared by the caller against what it
    /// last sent.) An answer without lock content (`PeerMsg::DelegRenewedOffCore`)
    /// leaves nothing behind then.
    ///
    /// A barrier counts only if it applies under the generation's subtree
    /// `dir` (what a renewal's answer would carry, `lock_barrier_under`): one
    /// elsewhere in the tree does not take the lane away from this delegate.
    pub(crate) fn lock_deleg_quiet(&self, gen: u64, dir: Ino, replica: &dyn Replica) -> bool {
        !self.lk.handoff.contains_key(&gen) && self.lock_barrier_under(dir, replica) == 0
    }

    /// A floor raised under `gen`'s subtree since the last answer (tests).
    #[cfg(test)]
    pub(crate) fn lock_set_handed_floor_for_tests(&mut self, gen: u64, floor: Position) {
        self.lk.handed_floor.insert(gen, floor);
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
    ///
    /// Every grant goes back with what is left of its record
    /// (`LockHandback::carry`, measured here, in the step that takes it out
    /// of the table and stops the generation: nothing here renews or
    /// reclaims it after); the root records it from the receipt
    /// (`Core::carried_until`). The grants that left the subtree are
    /// restamped by the root.
    pub(crate) fn lock_hand_back(
        &mut self,
        now: Now,
        gen: u64,
        replica: &dyn Replica,
    ) -> constellation_meta::locks::LockHandback {
        let dir = self.dl.mine.get(&gen).map(|d| d.dir);
        let grants = self.lock_take_generation(gen, replica);
        // What is left of each record is a duration on this node's wait
        // clock (plan 43 C7-H1): the wire carries no absolute.
        let carry: Vec<(GrantId, u64)> = grants
            .iter()
            .map(|g| (g.id, Self::lock_left(now.wait, g)))
            .collect();
        let grants = grants.iter().map(|g| g.to_wire(now)).collect();
        let (floor, barrier) = match dir {
            Some(dir) => (
                self.lock_subtree_floor(dir, |ino| replica.is_under(ino, dir), replica),
                self.lock_barrier_under(dir, replica),
            ),
            None => (Position::ZERO, 0),
        };
        constellation_meta::locks::LockHandback {
            grants,
            carry,
            floor,
            barrier,
        }
    }

    /// Root: what to hand a delegate with a granting renewal — what is
    /// waiting for its first one, and again every grant handed to it
    /// before that this root would still consider live (see
    /// [`LockState::handed`]; the delegate installs each id once).
    ///
    /// Every copy comes with what is left of its record, measured now (it
    /// shrinks at every re-send: the receipt is later each time, which only
    /// lengthens the delegate's record in real time). The copy itself is
    /// frozen: the subtree's route is the delegate's, so nothing here
    /// renews it.
    pub(crate) fn lock_take_handoff(
        &mut self,
        now: Now,
        gen: u64,
    ) -> (Vec<GrantWire>, Vec<(GrantId, u64)>) {
        let pending = self.lk.handoff.remove(&gen).unwrap_or_default();
        let handed = self.lk.handed.entry(gen).or_default();
        handed.extend(pending);
        handed.retain(|g| g.until > now.wait);
        let grants = handed.clone();
        if grants.is_empty() {
            self.lk.handed.remove(&gen);
        }
        let carry = grants
            .iter()
            .map(|g| (g.id, Self::lock_left(now.wait, g)))
            .collect();
        (grants.iter().map(|g| g.to_wire(now)).collect(), carry)
    }

    /// Root: the core forgot ended generation `gen`
    /// (`Core::deleg_forget_ended`): the floor its granting renewals
    /// carried goes with it.
    pub(crate) fn lock_forget_generation(&mut self, gen: u64) {
        self.lk.handed_floor.remove(&gen);
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
    pub(crate) fn lock_on_generation_ended(&mut self, now: Now, gen: u64, replica: &dyn Replica) {
        self.lk.returned.remove(&gen);
        if self.lock_final_blocks() {
            // Plan 43 C7-F F-c: nothing is added under a lease whose final
            // mirror went (the copies are not a grant of this table).
            self.lk.handoff.remove(&gen);
            self.lk.handed.remove(&gen);
            return;
        }
        let mut back = self.lk.handoff.remove(&gen).unwrap_or_default();
        back.extend(self.lk.handed.remove(&gen).unwrap_or_default());
        let mut n = 0;
        for g in back {
            // This root's copy, unconfirmed: the delegate may have ended
            // the grant (and granted another) before the generation ended.
            let g = Grant {
                gen: 0,
                confirmed: Grant::UNCONFIRMED,
                settled: false,
                ..g
            };
            if g.until > now.wait && replica.locks().install_if_consistent(g, now.wait) {
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
        now: Now,
        req: OpId,
        gen: u64,
        back: constellation_meta::locks::LockHandback,
        replica: &dyn Replica,
    ) {
        let constellation_meta::locks::LockHandback {
            grants,
            carry,
            floor,
            barrier,
        } = back;
        let live = match self.dl.gens.get(&gen) {
            Some(g) => {
                // Past its window the delegation's grants have lapsed at
                // their holders and the outwait's grace stands (or is
                // about to: an expiry retry, `!root_usable`); a seal
                // pushed `until` on but began after the outwait.
                let outwaited = g.ended
                    || now.wait >= self.gen_until(gen)
                    || g.recall == super::delegate::RecallPhase::Sealing;
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
        // Each (grant, recall) once: a duplicate of the answer brings back
        // nothing the first did not (a record released or superseded since
        // stays gone), and extends none (it would be restamped later).
        let seen = self.lk.returned.entry(gen).or_default();
        let before = grants.len();
        let grants: Vec<GrantWire> = grants
            .into_iter()
            .filter(|g| seen.insert((g.id, req)))
            .collect();
        if grants.len() < before {
            self.stats.lock_returned_duplicates += (before - grants.len()) as u64;
            tracing::debug!(
                node = self.cfg.node_id,
                gen,
                n = before - grants.len(),
                "a duplicate recall answer; its grants dropped"
            );
        }
        if self.dl.release_wait.is_some() {
            // Back while a release waits for this recall
            // (`release_recall_waits`): before, dropped with the lease.
            self.stats.lock_returned_in_release += grants.len() as u64;
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
        let (left, grants): (Vec<GrantWire>, Vec<GrantWire>) = grants.into_iter().partition(|g| {
            matches!(
                replica.resolve_ownership(&Self::read_keys(g.ino, None)),
                constellation_meta::delegation::Ownership::Root
            )
        });
        if !left.is_empty() {
            self.lock_install_leaving(now, gen, left, replica);
        }
        if !grants.is_empty() {
            self.lock_install_moved(now, 0, grants, &carry, replica);
        }
    }

    /// Delegate: grants that came with a renewal or, root: with a recall
    /// answer — installed from this receipt with what was left of their
    /// record (`carry`: ids with the sender's measure, a duration;
    /// `Core::carried_until`), and restamped when the sender listed none
    /// (a message from a sender without the measure, an undecodable one, the
    /// leaving grants).
    ///
    /// Invariants (tests: `a_carried_*`, `a_moved_twice_*`):
    /// - the record never ends before a window a holder may honour for
    ///   the grant (`Core::carried_until`);
    /// - at the root (`gen == 0`) it is never below the record it
    ///   replaces: a reclaim answered in a grace before the move arrived
    ///   put a later end there (`install_keeping_later`);
    /// - never later than a restamp.
    pub(crate) fn lock_install_moved(
        &mut self,
        now: Now,
        gen: u64,
        grants: Vec<GrantWire>,
        carry: &[(GrantId, u64)],
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
        if gen == 0 && self.lock_final_blocks() {
            return;
        }
        let restamp = self.restamp(now);
        let mut n = 0;
        // Plan 43 C7-F F-c: the batch that starts a generation's service
        // here is the move itself (its grants keep what they were); a later
        // one may be a re-send installed after this delegate served: not
        // settled (`Grant::settled`).
        let first = gen == 0 || self.lk.moved_seen.get(&gen).is_none_or(|s| s.is_empty());
        for g in grants {
            let left = carry.iter().find(|(id, _)| *id == g.id).map(|(_, l)| *l);
            let until = left.map_or(restamp, |l| self.carried_until(now, l));
            // Tagged with the generation the move names (0: the root's
            // table) — never resolved from the delegation table, which
            // may not say yet (a handoff arrives with the renewal that
            // installs the generation; sim seed 196252 tagged it 0 and
            // lost it at the next recall).
            let mut g = Grant {
                gen,
                ..g.into_grant(until)
            };
            g.settled &= first;
            if gen == 0 {
                if left.is_some() {
                    replica.locks().install_keeping_later(g);
                } else {
                    replica.locks().install(g);
                }
            } else {
                // A re-sent handoff: each id once, and never over a
                // newer grant of the same node, or next to a conflicting
                // one, here.
                if !self.lk.moved_seen.entry(gen).or_default().insert(g.id) {
                    continue;
                }
                if !replica.locks().install_if_consistent(g, now.wait) {
                    continue;
                }
            }
            self.stats.lock_moved += 1;
            n += 1;
            if let Some(left) = left {
                self.stats.lock_moved_carried += 1;
                tracing::debug!(
                    node = self.cfg.node_id,
                    gen,
                    holder = g.node,
                    ino = g.ino,
                    left,
                    until = until.0,
                    "a moved lock grant carried what was left of its record"
                );
            }
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
        taken.extend(replica.locks().take_where_outside(|ino| {
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
        now: Now,
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
    pub(crate) fn lock_on_generation_outwaited(&mut self, now: Now, dir: Ino) {
        let until = self.restamp(now);
        self.lk.grace.push((dir, until));
        // Its holders' releases are lost with it: what they could have
        // seen is at most what this root has once it appended the
        // delegate's stream — noted at the next event.
        self.lk.pending_dir_floors.push(dir);
        // Nor did they say what they were acknowledged in other
        // generations' streams: a barrier on the subtree.
        self.lock_dir_barrier(dir, now.wall.0);
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
        now: Now,
        gen: u64,
        replica: &dyn Replica,
    ) -> u64 {
        let Some(dir) = self.dl.gens.get(&gen).map(|g| g.dir) else {
            return 0;
        };
        self.lk.grace.retain(|(_, until)| *until > now.wait);
        let mut until = Self::lock_quarantine_until(replica);
        for (g, u) in &self.lk.grace {
            if *g == constellation_fs_core::types::ROOT_INO
                || replica.is_under(dir, *g)
                || replica.is_under(*g, dir)
            {
                until = until.max(*u);
            }
        }
        until.since(now.wait).max(0) as u64
    }

    /// Delegate: generation `gen`'s first granting renewal carried the
    /// root's remaining grace on the subtree: no new grant there until
    /// it passes (measured from the receipt, which is after the root's
    /// send, plus the margin), reclaims accepted meanwhile.
    pub(crate) fn lock_take_grace(&mut self, now: Now, gen: u64, grace_ms: u64) {
        if grace_ms == 0 {
            return;
        }
        let Some(dir) = self.dl.mine.get(&gen).map(|d| d.dir) else {
            return;
        };
        let until = now
            .wait
            .plus(grace_ms.saturating_add(self.lock_margin_ms().max(0) as u64));
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
    /// (the sim found a successor granting at once). Only when the
    /// release says it left any: its mark (`acquire_won`). The
    /// whole-namespace quarantine, not this tenure's `grace`: the grants
    /// are not this tenure's, so they do not end with it. Kept per
    /// tenure, the grace went with the successor's own release a moment
    /// later (an epoch's flush), and its re-claim of the lease it had
    /// released — no takeover — granted over the predecessor's live
    /// exclusive grant (`locks-blips-tight` seed 2723).
    /// The predecessor's grants end unreleased, as an outwait does, and
    /// their holders may write into delegates' streams until then: a
    /// barrier on everything as of the quarantine's end (the tenure's
    /// floor takes the inherited delegates' *first* renewals, which may
    /// be older than those writes).
    ///
    /// Plan 43 C7b: `seen` is this node's first sight (wait clock) of the
    /// release (`LeaseState::released_since`; `now.wait` when it has not
    /// watched it, a cold taker). The grace ends at `seen + lock_ttl + 2M`:
    /// the releaser's grants were issued before its release CAS and each
    /// ends by true `T(release send) + lock_ttl`, and `seen` is after the
    /// release landed. A taker that saw the release long ago waits for no
    /// grant (no quarantine); a cold one waits the whole grace.
    ///
    /// The barrier is set either way: the writes it covers were made before
    /// `until` whether `until` is past or not, and a floor taken from an
    /// inherited delegate's first renewal may be older than them. A past
    /// barrier costs nothing once every live generation's head is past it.
    pub(crate) fn lock_on_released_takeover(
        &mut self,
        now: Now,
        seen: WaitMs,
        replica: &dyn Replica,
    ) {
        let until = seen.plus(self.lock_released_grace_ms());
        // The barrier is a wall value (row 9, C7b): the record's end mapped
        // from this node's wait clock by the time left on it.
        self.lock_dir_barrier(
            constellation_fs_core::types::ROOT_INO,
            now.wall_of_wait(until).0,
        );
        if until <= now.wait {
            return;
        }
        replica.locks().set_quarantine(until);
        self.stats.lock_grace_periods += 1;
    }

    /// How long after its first sight of a release the taker of a released
    /// lease waits for the releaser's lock grants: the lock TTL and two
    /// margins ([`Core::lock_on_released_takeover`]).
    pub(crate) fn lock_released_grace_ms(&self) -> u64 {
        (self.lock_ttl_ms() + 2 * self.lock_margin_ms()).max(0) as u64
    }

    /// Plan 43 §11.2 (`locks-released-takeover`): the grace of a released
    /// takeover as the restart horizon, persisted before the claim CAS goes
    /// out. The grace itself is in memory (`LockTables::set_quarantine`),
    /// and a taker that stopped inside it, or between the CAS and
    /// `acquire_won`, re-adopted its own lease at its restart, which is no
    /// takeover: it granted over a grant the releaser's holder still
    /// honoured (the default clock mix's reboots of the taker). Written
    /// before the CAS so no point after it is without the grace; from now,
    /// not from the first sight of the release: the releaser's grants end by
    /// the release plus the lock TTL, in true time, and the claim is after
    /// the release. `locks_start` loads it as any restart horizon: the
    /// quarantine and the barrier on everything, to its end on the same
    /// boot, the recorded span from the restart on another. Durable on the
    /// core, as the takeover floor's claim mark is (`acquire_cas`): a
    /// claim of a released lease that left grants is rare, and each pays
    /// one write. `false`: not persisted, and the CAS must not go out.
    pub(crate) fn lock_persist_released_grace(&self, now: Now, replica: &dyn Replica) -> bool {
        replica.note_lock_grant_horizon(now.wait.plus(self.lock_released_grace_ms()), now)
    }

    /// Plan 43 C7-F F-a0: this tenure may have lock grants that are still
    /// honoured, or waits out some of an earlier tenure's: a lease released
    /// now says so (`granted_delegations`, with any grant record left in
    /// the table: `issue_release`), because the node that takes a
    /// released lease so marked over grants no lock for a grace
    /// ([`Core::lock_on_released_takeover`]; one that takes an unmarked
    /// one over grants at once: `acquire_won`) and is then a tenure its own
    /// successor must wait out too (`acquire_cas`'s `inherits_duty`). A
    /// tenure with nothing of the kind costs its successor's successor
    /// nothing, so a handoff that served nothing (a start-up's) does not
    /// make the next seal pay a floor.
    pub(crate) fn lock_duty_live(&self, now: Now, replica: &dyn Replica) -> bool {
        self.cfg.locks
            && (replica.locks().quarantine_until() > now.wait
                || replica
                    .locks()
                    .grants_snapshot()
                    .iter()
                    .any(|g| g.until > now.wait)
                // Grants moved out of the table toward a delegate
                // (`lock_on_delegated`) are honoured all the same.
                || self
                    .lk
                    .handoff
                    .values()
                    .chain(self.lk.handed.values())
                    .flatten()
                    .any(|g| g.until > now.wait)
                || self.lk.grace.iter().any(|(_, u)| *u > now.wait))
    }

    /// A lock answer (a grant, a renewal, a reclaim) is held for the
    /// tenure's liveness, its mark or a horizon write.
    pub(crate) fn lock_answers_held(&self) -> bool {
        !self.lk.horizon_held.is_empty() || !self.lk.own_held.is_empty()
    }

    /// What a release leaves behind, exactly: [`Core::lock_duty_live`], or
    /// any grant record at all, an expired one too (a renewal is still
    /// served while a release is under way, or by a tenure whose record
    /// the table still holds, and extends a known record, expired or not:
    /// `LockTables::extend`). The one definition of the release's mark
    /// (`issue_release`) and of the lock part of [`Core::tenure_duty_live`],
    /// so the two cannot drift.
    pub(crate) fn lock_duty_standing(&self, now: Now, replica: &dyn Replica) -> bool {
        self.lock_duty_live(now, replica) || (self.cfg.locks && replica.locks().grants_len() > 0)
    }

    /// Plan 43 C7-F F-a0 L3: this tenure waits out, or answered, something
    /// a sealer of it must wait out too: lock grants (live, moved toward a
    /// delegate, or a record a renewal can revive), a subtree grace or a
    /// quarantine, a takeover floor still to be armed or running, read
    /// grants or a read quarantine, and what it granted under a
    /// continuation epoch's hold. The lease says so
    /// (`Lease::granted_delegations`) from the moment the tenure can be
    /// sealed (`Core::fast_tenure`): its claim CAS under `ack=s3`, or the
    /// reconfiguration that lists its first backup. A sealer arms its
    /// takeover floor only for a lease so marked (`note_fast_takeover`),
    /// and a claim CAS writes an unmarked lease unless it inherits a
    /// predecessor's duty (`acquire_cas`): a node that re-claims its own
    /// released lease, a restart's quarantine and the reclaims a `Local`
    /// tenure admits (nothing mirrors them to a backup listed later) all
    /// leave a duty no claim CAS marked. So do the delegation renewals a
    /// `Local` tenure answers (`renew_hold` holds nothing there): a live
    /// write delegation's delegate may serve strict reads (and lock grants)
    /// whatever this tenure served, which the floor's `deleg` term waits
    /// out (`note_marker_landed`); counted as that term counts it, by a
    /// non-empty delegation table.
    pub(crate) fn tenure_duty_live(&self, now: Now, replica: &dyn Replica) -> bool {
        self.lock_duty_standing(now, replica)
            || !replica.delegation_table().is_empty()
            || self.ack.pending_floor
            || self.in_takeover_floor(now)
            || replica.read_delegations().quarantine_until() > now.wait
            || replica.read_delegations().has_live_grant(now.wait)
            || WaitMs(
                self.ack
                    .hold_grant_until
                    .load(std::sync::atomic::Ordering::Relaxed),
            ) > now.wait
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
    fn lock_quarantine_dropped_tenure(&mut self, now: Now, replica: &dyn Replica) {
        let grants = replica
            .locks()
            .grants_snapshot()
            .into_iter()
            .map(|g| g.until)
            .max()
            .unwrap_or(WaitMs(0));
        let grace = self.lock_grace_until();
        let until = grants.max(grace);
        if until > now.wait {
            replica.locks().set_quarantine(until);
            // Those grants end unreleased (see `lock_on_released_takeover`).
            self.lock_dir_barrier(
                constellation_fs_core::types::ROOT_INO,
                now.wall_of_wait(until).0,
            );
            self.stats.lock_grace_periods += 1;
        }
    }

    pub(crate) fn locks_start(&mut self, now: Now, replica: &dyn Replica) {
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
        // Plan 43 C7-F F-c: how long a lapsing grant parks its I/O.
        replica
            .locks()
            .set_lapse_park_ms(self.cfg.lock_owner_wait_max_ms() as i64);
        if let Some(until) = replica.load_lock_quarantine(now, self.cfg.lock_horizon_span_ms()) {
            tracing::info!(
                node = self.cfg.node_id,
                wait_ms = until.since(now.wait),
                "lock grants made before this restart may still be honoured; \
                 no new lock grant until they have expired (parked requests are \
                 served by the waiter tick then)"
            );
            // They all end unreleased, as an outwait does, and their
            // holders may write into delegates' streams until then: a
            // barrier on everything as of that end.
            self.lock_dir_barrier(
                constellation_fs_core::types::ROOT_INO,
                now.wall_of_wait(until).0,
            );
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
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Kept at an epoch's close (`Core::epoch_close_release`). Else
        // the tenure is over: nothing continues it any more.
        let kept = std::mem::take(&mut self.lk.keep_grants);
        if !kept {
            self.end_takeover_floor();
            // The floor of a tenure that is over is not the next one's (the
            // quarantines it set run on; the claim mark names an epoch no
            // longer ours).
            // A claim of a newer epoch that is out (or in doubt) is not the
            // tenure that ended: its mark stays, as the floor it may owe
            // (`acquire_cas`) is not armed yet.
            let claiming = self
                .ack
                .floor_mark
                .is_some_and(|e| self.claim_unsettled_at(e));
            if !claiming {
                self.ack.floor_mark_locks = None;
                if self.ack.floor_mark.take().is_some() {
                    replica.clear_takeover_floor_pending();
                }
            }
            if std::mem::take(&mut self.ack.floor_persisted) {
                replica.clear_takeover_floor();
            }
            self.lock_quarantine_dropped_tenure(now, replica);
            self.pr.closed_tenure.clear();
            self.pr.closed_honour.clear();
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
        self.lk.returned.clear();
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
                grace: false,
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

    // ------------------------------------------------------------ the final mirror

    /// The lease body this node holds as its own (`LeaseKey`): what the
    /// final mirror names and a fast takeover's replaced body is compared
    /// with.
    pub(crate) fn lock_lease_key(&self) -> Option<LeaseKey> {
        let (l, _) = self.lease.held.as_ref()?;
        (l.holder == self.cfg.node_id
            && l.holder_clock == self.cfg.clock_id
            && l.holder_wait_ms != 0)
            .then_some((l.holder, l.holder_clock, l.holder_wait_ms, l.epoch))
    }

    /// The final mirror of the lease this node holds went: while it is the
    /// lease held, nothing is added to this table (enforced, not derived:
    /// a mint, a reclaim, a late admission, a move-in, an upgrade). A
    /// renewal of the lease (another body, another key) lifts it.
    pub(crate) fn lock_final_blocks(&self) -> bool {
        self.lk.final_for.is_some() && self.lk.final_for == self.lock_lease_key()
    }

    /// Plan 43 C7-F F-c, the old root's last word. When this node can no
    /// longer add a record under its current lease — the lease not usable
    /// on the honour clock, and no continuation epoch's re-claim route
    /// (`lock_route_for` answers `Route::Me` for nothing) — it sends its
    /// backups its whole table as the **final** mirror of that lease:
    /// every record present (those expired on its wait clock marked
    /// `lapsed`) and its lapsed memory, each with its `settled` flag, and
    /// adds nothing under the lease from then on. The asynchronous mirror
    /// cannot say whether a later one was missed; this one can: a
    /// successor whose takeover replaced exactly this lease body knows the
    /// table below is all this node ever granted under it
    /// (`lock_install_mirror`). Sent at the usable end (a timer armed for
    /// it) and re-sent at each backup heartbeat until the lease ends, to
    /// survive a lost message (the same version: idempotent there).
    pub(crate) fn lock_final_step(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
        from_timer: bool,
    ) {
        if !self.cfg.locks {
            return;
        }
        let Some(key) = self.lock_lease_key() else {
            return;
        };
        // Runs after every event: nothing is allocated until a mirror goes.
        if self.lease.backups().is_empty() || self.lease.epoch_held() {
            return;
        }
        let adds_open =
            self.lease.usable(now, &self.cfg) || self.epoch_reclaim_expires(now).is_some();
        if adds_open {
            // Wake at the usable end.
            if let Some(end) = self.lease.honour_until(&self.cfg) {
                let at = end.minus(self.cfg.expiry_margin_ms);
                if self.lk.final_timer.is_none_or(|(_, armed)| armed != at) {
                    if let Some((t, _)) = self.lk.final_timer.take() {
                        self.cancel_timer(t, out);
                    }
                    let t =
                        self.set_timer_honour(at.max(now.honour.plus(1)), Timer::LockFinal, out);
                    self.lk.final_timer = Some((t, at));
                }
            }
            return;
        }
        let first = self.lk.final_for != Some(key);
        if first {
            self.lk.final_for = Some(key);
            self.lk.mirror_ver += 1;
            self.stats.lock_mirror_final_sent += 1;
        }
        // Re-sent each heartbeat until the lease's own end.
        let left = self.lease.honour_left_ms(now, &self.cfg).unwrap_or(0);
        if self.lk.final_timer.is_none() && left > 0 {
            let at = now.honour.plus(self.cfg.backup_heartbeat_ms.max(1));
            let t = self.set_timer_honour(at, Timer::LockFinal, out);
            self.lk.final_timer = Some((t, at));
        }
        if !first && !from_timer {
            return;
        }
        let grants = replica.locks().final_snapshot(now);
        let floor = self.lk.floor_all;
        for b in self.lease.backups().to_vec() {
            out.push(Action::Send {
                to: b,
                msg: PeerMsg::LockMirror {
                    ver: self.lk.mirror_ver,
                    grants: grants.clone(),
                    floor,
                    final_for: Some(key),
                },
            });
        }
    }

    // ------------------------------------------------------------ after every event

    pub(crate) fn locks_after_event(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lock_fill_renew_heads(replica, out);
        // Every grace is set by some event (a takeover's marker, a
        // restart, an outwaited delegate, a released takeover): the
        // timer at its end is armed right after it.
        if self.cfg.locks {
            self.lock_arm_grace_end(now, replica, out);
        }
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
        self.lock_take_outwaited(now, replica);
        self.lock_final_step(now, replica, out, false);
        if self.lk.mirror_dirty {
            self.lk.mirror_dirty = false;
            let backups = self.lease.backups();
            if !backups.is_empty() && self.root_usable(now) && !self.lock_final_blocks() {
                self.lk.mirror_ver += 1;
                let grants: Vec<GrantWire> = replica
                    .locks()
                    .grants_snapshot()
                    .iter()
                    .map(|g| g.to_wire(now))
                    .collect();
                let floor = self.lk.floor_all;
                for b in backups {
                    out.push(Action::Send {
                        to: *b,
                        msg: PeerMsg::LockMirror {
                            ver: self.lk.mirror_ver,
                            grants: grants.clone(),
                            floor,
                            final_for: None,
                        },
                    });
                }
            }
        }
    }
}
