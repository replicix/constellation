//! Plan 30 §M9: the synchronous backup, seal-based failover, `ack=s3`,
//! and pre-S3 streaming — the holder's acknowledgement gate, the backup's
//! side, and the fast takeover.
//!
//! # What an acknowledgement means
//!
//! A holder answers a mutation (accepted *or refused*: a refusal was
//! evaluated against the same unshipped state) only once the journal
//! position it was evaluated at is *durable* under the lease's
//! `ack_policy` (`constellation_store_s3::AckPolicy`):
//! - `Local`: at once (today; plan 30 §3's Layer A).
//! - `Backup`: once every backup listed in the *committed* lease object
//!   has persisted the journal through that position (Layer B). The
//!   holder streams whole journal transactions to each backup, up to
//!   `Config::backup_max_inflight` (8) appends in flight per backup
//!   (pipelined; the backup acknowledges what it holds contiguously), so
//!   a LAN backup adds about one round trip to an acknowledgement and
//!   every row journaled meanwhile rides the next append (the group
//!   commit). An append is committed to the backup's store before the
//!   acknowledgement but not fsynced (fjall `PersistMode::Buffer`): it
//!   survives any failure of the holder, and a crash of the backup's
//!   process, but not a simultaneous power loss of the holder and every
//!   backup (the durability contract in
//!   `docs/reference/features/durability-and-failover.md`). The seal,
//!   which safety rests on, *is* synced (`Meta::backup_seal`).
//! - `S3`: once the segment carrying the position landed (Layer C).
//!
//! The wait is a parked continuation (plan 30 §M8's park, extended with a
//! durability condition): the op has executed, its rows ship like any
//! other, only the reply waits. The FUSE fast path writes its row outside
//! the core and then waits by the row's own class (plan 43 C1-2): the class
//! the core published with the gate when the row was journaled
//! (`Core::root_ack_class`, stored with the journal row as
//! `constellation_meta::RowClass`), so what the row is tagged with and what
//! its answer waits for agree; a row whose answer was released because the
//! gate went off (the group is gone and no peer is eligible, or an epoch
//! hold took over), not because the row is committed, is re-tagged `Lone`
//! first (`Core::complete_ready`, the fast path's `DurableWait::Ungated`):
//! "gated implies committed at the answer". Every other local mutation goes
//! through the core and parks the same way — which is also what lets a
//! deposition turn a waiting op into an in-doubt retry by rid instead of
//! an acknowledgement nobody can honour. And the holder's own reads wait
//! for durability of the unshipped rows they would observe
//! (`Meta::durability_pending`), so no client of any node ever observes
//! an effect that a failover could roll back: the sim's
//! `observed_tentative` is a hard failure under these policies.
//!
//! # Safety without failure detection
//!
//! Every timeout here is liveness only. What makes a failover safe:
//! - **The seal.** A backup that decides to take over first persists
//!   "epoch e sealed" and from then on answers every epoch-`e` append
//!   with `sealed`. The old holder needs a write-all ack for anything it
//!   acknowledges, so after the seal it can acknowledge nothing — whether
//!   it is dead, slow, or merely partitioned from this backup.
//! - **The committed set.** An acknowledgement needs the backups in the
//!   lease *object*; adding a backup streams to it first and CASes it
//!   in only once it has caught up; removing one is a CAS the holder
//!   waits for before acknowledging anything further (the parked acks
//!   need the removed backup and the survivors — the log, if none
//!   survives — until the CAS lands, and only the survivors after). A
//!   backup that is not (or no longer) in the object never takes over:
//!   it reads the object before its CAS.
//!   Plan 43 C1-1 makes both halves true of what is counted, not only of
//!   what is asked:
//!   - A backup counts only from the acknowledgements of appends that
//!     carried a configuration version listing it
//!     (`BackupPeer::counted`, `Core::backed_through`): a candidate that
//!     is not listed yet discards its tail on any lease read that does
//!     not list it, so what it acknowledged before its listing landed
//!     proves nothing about what it will keep. A backup whose hold was
//!     created by an append says so (`PeerMsg::BackupAck::fresh`) and the
//!     holder restarts its count from that hold.
//!   - While a removal CAS is out or in doubt, an acknowledgement rests
//!     on the staying set too (empty: the log), whatever the held version
//!     says: the version S3 may hold is the one that lists only them
//!     (`Core::ack_constraint_sets`).
//! - **The tail fetch (plan 43 C1-3: the TTL path).** A claim of a lease
//!   by TTL used to carry nothing: the lease's group (the holder and the
//!   listed backups) may have acknowledged rows nobody shipped, which the
//!   claimant's log lacks. Since write-all makes every member hold every
//!   committed row, the claimant of a lease that lists backups (or names a
//!   tail its tenure re-ships: below) first asks the group for one
//!   member's tail (`PeerMsg::SealTail`, `core::seal_tail`). A member
//!   persists and syncs a seal of the epoch before it answers, so its
//!   group commits nothing more of it: a *backup* seals its role and
//!   answers its contiguous tail; a *holder* deposes itself (nothing of
//!   the epoch is acknowledged again, its unshipped rows are kept as a
//!   member copy) and answers the same rows. The claim is conditioned, as
//!   every claim is, on the lease version it read: a holder that renewed
//!   meanwhile fails it, and the attempt starts over. A claimant that is
//!   itself a listed backup holding the tail seals first and claims with
//!   it. **If no member answers the claim waits** (G4: no claim without the
//!   tail, `status.lease.waiting_for_group`) until one does, or until the
//!   operator abandons the group (`constellation repair abandon-group`,
//!   `LogRecord::GroupAbandoned` in the claimant's marker). A holder that
//!   sealed itself never revives its epoch: it re-claims its own expired
//!   lease at the next epoch with its member copy as the tail (D7).
//! - **The inherited tail (D2).** A takeover that re-ships a predecessor's
//!   tail journals the committed rows as its own, and until their segments
//!   are in the log they rest on its disk and in the old group's copies.
//!   The lease it writes says so (`Lease::inherited_tail`: the epoch, the
//!   members, `through`) until the first renewal or reconfiguration CAS
//!   after its journal shipped through them; a claimant of such a lease
//!   fetches from the listed backups (which caught up with the whole
//!   unshipped journal before they were listed) or from the holder and the
//!   old group, and the old members keep their copies until a lease reading
//!   no longer names them.
//! - **The log-slot CAS.** The taker's epoch marker is a create-if-absent
//!   at the next sequence; any segment the old holder ships after it
//!   collides, and what it shipped before it the taker adopted in its
//!   tail to head. Under `S3` this is the only fence needed: every
//!   acknowledged record is in a slot below the marker.
//! - **Reconfiguration races.** A backup's takeover CAS and the holder's
//!   removal CAS are on the same object version; exactly one wins.
//!   Holder wins: the backup re-reads, finds itself unlisted, discards
//!   its tail. Backup wins: the holder's re-read shows a new holder — a
//!   deposition, with the stranding and replay by rid of plan 30 §M3b.
//!
//! # The delegation horizon across a fast failover
//!
//! Plan 30 §M8 caps a read delegation by the granting holder's lease
//! expiry; a takeover *before* that expiry (a seal, or `ack=s3`) breaks
//! the cap. Two rules close it, and both reduce to the lease's own
//! `M > 2D` (clocks within `D` of real time, margin `M`):
//! - A holder under a non-`Local` policy answers strict reads (positions
//!   and grants) only while an S3 request it *sent* within the last
//!   `backup_takeover_ms` succeeded without revealing a deposition
//!   (`note_s3_liveness`); otherwise it answers `Busy` and probes. And a
//!   holder that applies a segment of a higher epoch deposes itself at
//!   once. So every strict answer the old holder gives is sent within
//!   `backup_takeover_ms + 2D` of the last moment before the successor's
//!   marker existed.
//! - A successor of a lease it took over by permit (a seal, or `ack=s3`:
//!   before the expiry, or past it by the successor's clock alone) whose
//!   tenure served strict reads (`Lease::granted_delegations`, set by one
//!   CAS before the first answer) acknowledges no mutation until
//!   `marker time + backup_takeover_ms + read_delegation_ttl + 2M`
//!   (plus the reclaim horizon when write delegations were live) —
//!   through the M8 quarantine every acknowledgement already waits on.
//!   The predecessor's lease expiry is no shortcut: it is a stamp of the
//!   predecessor's wall clock, and a successor whose clock runs ahead
//!   reads it as past (or too near) while the predecessor's grants are
//!   still honoured. The bound alone counts only the successor's own
//!   elapsed time, so it needs a bounded clock *rate*, not a bounded
//!   offset (`W + ttl + 2M ≥ (W + ttl − M)(1 + ρ)`). A tenure that never
//!   served a strict read costs its successor nothing.
//!
//! ## The floor ends earlier when the predecessor's own beats say its lease did
//!
//! The bound counts from the marker whatever the predecessor did; it is
//! the right answer for a holder that died or was cut off mid-lease, but
//! for one cut from S3 only, which went on beating until its lease ran
//! out, the successor (which seals at that expiry) waited the whole bound
//! on top of the lease. [`Core::beat_cap`] takes that back. Notation: H
//! the predecessor, S the successor, `E_h` the expiry of the lease H holds
//! at a decision (H's **wait** clock, plan 43 C7-H1: the lease object
//! carries it, `Lease::holder_wait_ms`, with the id of that clock,
//! `Lease::holder_clock`), `M` the expiry margin.
//! 1. *Everything S waits out is capped by H's lease.* Strict answers only
//!    while the lease is `usable` (expiry − now > M); read delegations
//!    (`maybe_grant`), lock grants and renewals (`lock_cap_ms`), write
//!    delegations and their renewals (`grant_cap_ms`) end at the expiry
//!    less M, their holders honour them from their own send less another
//!    M, and what a delegate grants is capped by its own `until − M`. So
//!    (with `2ρ·ttl ≤ 2M`) nothing H answered is honoured, in real time,
//!    after H's wait clock reads `E_h`. Its wait clock never over-counts, so
//!    it reaches `E_h` no earlier in real time than the grants' holders,
//!    whose clocks did not step with H's wall clock, reach their ends
//!    (plan 43 C7-H1, R7: the honour clock would count a forward wall step
//!    of H as elapsed time they never saw). Exception: under a continuation epoch's
//!    hold the lease has no expiry, and `lock_cap_ms` and `grant_cap_ms`
//!    cap a grant by its ttl alone; the grants kept across the re-claim
//!    (`epoch_tenure_resumed`) can then outlast the re-claimed lease (a
//!    lock or delegation ttl longer than the lease ttl: legal), which a fast
//!    seal of that tenure would cap. They are in the guard
//!    (`hold_grant_until`, step 4), so `X` covers them once a beat sent
//!    after the grant is heard (a holder that dies within one beat interval
//!    of its last hold-era grant, after the re-claim, is the residual). The
//!    hold itself is never taken over fast (`epoch_may_carry`).
//! 2. *`E_h ≤ X`.* S's CAS replaced version T, so no later write of H
//!    landed; H adopts only versions its own PUT or read returned, all at
//!    or before T, and along that chain the expiry only grows (renewals
//!    write `now + ttl`, everything else copies it). So `E_h ≤
//!    T.expires`; `X` also takes the highest expiry any beat named and the
//!    highest guard. Exceptions: a restart of H within its epoch with a
//!    shorter TTL (its earlier incarnation's grants are behind its restart
//!    quarantine, which the guard names; and the wait-clock end of that
//!    incarnation is on another clock, kept apart by clock id). (The cap's
//!    own dependence on the holder's clock is step 3's.)
//!    Counting from the *last renewal S observed* instead would be unsafe:
//!    H's other proofs of S3 liveness (segment runs and puts, a lease
//!    read) are invisible to S, and H answers within `W` of any of them.
//! 3. *The beat links the clocks.* A beat says H's wait clock read `sent`
//!    no later than S's clock read `at` (its arrival stamp, on S's wait
//!    clock: `sent` < real send < real arrival ≤ `at`). Each node's wait
//!    clock advances at most real time (P3: the rate budget `ρ` is in the
//!    readings, per node), which makes the cap exact for a beat sent *after*
//!    the renewal that set `X`: H's windows end at most `ttl` real ms after
//!    that renewal, the real interval renewal → `sent` is at least
//!    `sent − renew` on H's wait clock, so they end at most `X − sent` real
//!    ms after `sent`, and S's wait clock cannot read `at + (X − sent) + M`
//!    sooner. For a beat sent *before* that renewal `X − sent` is
//!    `(renew − sent) + ttl`, and H's wait clock may have under-counted that
//!    past interval `sent → renew` by up to `2ρ·(renew − sent)` (it
//!    advances at `(1 − ρ)/(1 + ρ)` of real at the slowest): the cap can be
//!    early by that much, so such a beat is used only within `M/(4ρ)` of
//!    `X` (`2ρ·span ≤ M/2`:
//!    [`Core::beat_cap_span_ms`]; `250·M` at the defaults, no limit at
//!    ρ = 0).
//!    A difference on H's clock plus a stamp of S's: rate-only, no offset,
//!    and **no wall step of H in either direction**: the wait clock ignores
//!    both, so `sent`, the lease's end (`Lease::holder_wait_ms`, written at
//!    each renewal) and the guard stay consistent however H's wall clock
//!    moved between its last renewal and the seal. Every term is of one
//!    holder clock: S keeps `lags`, `named_max` and `guard_max` per clock id (a restarted H is
//!    a new process, a new clock; at most two ids), takes the latest
//!    `X_c + min lag_c` over them, and puts the replaced lease's own wait-clock
//!    end in `X` of the id that wrote it (`holder_clock`), never its wall
//!    expiry (a stamp of another clock). No beat of that clock: no cap. (A
//!    holder that is suspended or paused between its last renewal and the
//!    seal leaves a later cap: its wait clock does not count the pause. The
//!    honour clock does, and a forward wall step of H counts on it too,
//!    which is why neither is used here.)
//! 4. *The guard* is what H itself waits out and its lease does not cap:
//!    its own takeover floor (which can outlast its lease), a released
//!    takeover's lock grace, restart quarantines, subtree graces
//!    ([`Core::beat_guard_wait_ms`], a reading of H's wait clock as it is); `i64::MAX` while its takeover gate
//!    or a floor still to be armed is pending (the marker may be ahead,
//!    and H's predecessor's proofs extend until then): no cap. The guard
//!    is the *maximum* over the beats of the epoch, so one `i64::MAX` beat
//!    disables the cap for the whole epoch (conservative; a finite guard
//!    may rise afterwards only by what protects a lease-capped right: a
//!    subtree grace, or the hold-era grants above).
//! 5. The cap is `X + min(at − sent) + M`; the floor is `min(bound, cap)`,
//!    none if the cap has passed when the marker lands. Each is sufficient
//!    alone (the bound by the argument above, the cap by 1–4).
//!
//! Never the beat's *credit*, `min(at, sent + M)`: `sent + M` is an instant
//! of H's clock, and a successor whose clock runs ahead of H's by more than
//! M would end its floor that much early (the sim's
//! `fast-takeover-skewed-successor` is the control; since plan 43 C7-H1 the
//! seal watch counts the plain arrival too, `sent` being a reading of H's
//! wait clock, opaque here). And never the lease's expiry compared with S's
//! clock. No beat, a beat of
//! another holder or epoch, or none sent within `M/(4ρ)` of `X`: the bound.
//! The beats are in memory (plan 43 Appendix B row 27): a successor that
//! restarted has no cap and waits the bound.
//!
//! ## The floor survives a restart (plan 43 C7a, commit 4; Appendix B row 14)
//!
//! The floor counts from the marker and lived in memory, so a successor that
//! stopped inside it re-adopted its own lease (M4's gate, which lands a new
//! marker) with no floor and answered, granted and acknowledged inside the
//! predecessor's window. Two writes close it:
//! - *At arming* (`note_marker_landed`): the floor's three ends are persisted
//!   as boot stamps (`meta::store::horizon`), the read-grant horizon (until
//!   which acknowledgements wait: what `read_start` loads as the restart
//!   quarantine), the lock-grant horizon (`Action::PersistLockHorizon`, off
//!   the core, as every lock grant's) and the floor itself
//!   (`takeover_floor_horizon_ms`, which `floor_start` loads, so that the
//!   lease is not released, handed off or flushed inside a floor the node
//!   came back into). A restart on the same boot reads the time left on the
//!   raw clock, another boot waits the span written.
//! - *Before the claim CAS* (`acquire_cas`): the window the keys cannot
//!   cover is between the CAS (the lease object now names this node) and the
//!   marker. A takeover of a tenure that granted records the epoch it writes
//!   (`takeover_floor_pending`, durable), and removes it once the three ends
//!   above are durable, or when the won CAS armed no floor (a takeover by
//!   TTL). A restart that re-adopts its own lease at the recorded epoch arms
//!   the floor as the takeover would (`note_floor_restart`, counted from the
//!   re-adoption's own marker: later than the claim's, so never short of it;
//!   no beat cap, the predecessor's beats are gone; the gate is a fast
//!   takeover's meanwhile).
//!
//! The mark names the epoch only and the floor's bound is recomputed when the
//! marker lands, because it depends on what the gate adopts (the write
//! delegations of the log) which neither the CAS nor the restart knows yet.
//! An upgrade handover that gave up waiting for the gate (`GATE_WAIT`) or
//! stopped inside the floor starts the new image through the same
//! `Core::start` and re-adoption, so it needs nothing of its own.
//!
//! # Continuation epochs (plan 30 §M10's claim rule, enforced here)
//!
//! A fast takeover cannot be gated by an epoch's promises (it happens
//! before the lease expires), so it is kept away from epochs on the
//! formation side: an epoch carries this node's lease only if nobody
//! outside it can take that lease over (`epoch_may_carry`: `Local`, or
//! `Backup` with every listed backup a member; never `S3`), and a member
//! backup runs no seal watch while its epoch is open (`arm_backup_watch`,
//! `backup_watch_after_epoch`). The M10 model shows both halves
//! (`m9_*_against_an_unguarded_epoch_splits_brain` vs
//! `m9_*_with_the_claim_rule_is_clean`).
//!
//! # Backup selection (plan 30 §2.4: never assume a LAN)
//!
//! Candidates are write-eligible peers whose measured RTT is within
//! `backup_rtt_budget_ms` and whose link has been up for
//! `backup_stable_ms`; the one connected the longest is preferred; at
//! most `backups_max`. No candidate: `backups = []`, `Local`, today's
//! behaviour — a WAN-only cluster never pays a synchronous round trip.
//! One node: nothing here ever runs (no peers, no backups, no messages).

use super::lease::LeaseState;
use super::{Core, S3For, Timer};
use crate::action::{Action, AnswerSource, S3Op};
use crate::event::{PeerMsg, S3Result};
use crate::ids::{Epoch, HonourMs, Ms, NodeId, Now, OpId, Seq, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_meta::{BackupRole, BackupTx, MutateOutcome, Rid};
use constellation_store_s3::{AckPolicy, Lease, LeaseTag};
use std::collections::{BTreeMap, BTreeSet};

/// How many times a reconfiguration CAS is retried against a re-read
/// object before the holder gives up on it (and re-plans later).
const RECONFIG_ATTEMPTS: u32 = 3;
/// How often the holder's backup bookkeeping (selection, timeouts,
/// promotion) runs between ticks.
const HOUSEKEEPING_MS: i64 = 50;

/// Plan 43 C7-F F-a0 (b): the least time between two attempts to clear an
/// inherited mark (or two reads that settle one): a failed CAS is not
/// retried on every event.
const CLEAR_RETRY_MS: i64 = 1_000;
/// The longest a holder's [`PeerMsg::BackupHold`] holds this node's seal
/// watch, whatever it asks.
pub const BACKUP_HOLD_MAX_MS: u64 = 30_000;

/// How long a candidate that timed out `strikes` times in a row is not
/// brought up again: the reconfiguration rate limit doubled per strike
/// (6 s, 12 s, 24 s, ... at the 3 s default), at most a minute. A
/// candidate that makes it into the set starts over.
pub(crate) fn candidate_backoff_ms(reconfig_min_ms: u64, strikes: u32) -> u64 {
    reconfig_min_ms
        .max(1)
        .saturating_mul(1u64 << strikes.min(16))
        .min(CANDIDATE_BACKOFF_MAX_MS)
}

/// The longest [`candidate_backoff_ms`].
const CANDIDATE_BACKOFF_MAX_MS: u64 = 60_000;

/// The liveness rule of a tenure that may be taken over before its lease
/// expires (the module doc): fresh at `at_ms` only within `window_ms`
/// (`backup_takeover_ms`) of the send of the latest S3 request that
/// proved the tenure (`last_s3_fresh`); always fresh otherwise. One
/// definition for the core ([`Core::renew_fresh`]: strict answers, new
/// lock grants, renewals) and the FUSE side (`LeaseView::reads_locally`).
///
/// Plan 43 C7-H1: both stamps are on the holder's **honour** clock (the
/// send of the proving request is the holder's own stamp bounding its own
/// right to answer), so a backward wall step cannot keep a stale tenure
/// "fresh" for the step's length.
pub fn liveness_fresh(
    fast_tenure: bool,
    at: HonourMs,
    last_s3_fresh: HonourMs,
    window_ms: i64,
) -> bool {
    !fast_tenure || at.since(last_s3_fresh) < window_ms
}

/// Plan 43 C7-H1 (R7): the reading a holder stamps its heartbeat's `sent`
/// with: its **wait** clock, the clock the beat's lease end
/// (`Lease::holder_wait_ms`) and guard are on. The successor that seals this
/// holder reads the time left on the lease as `end - sent`, so that
/// difference must never under-estimate real time left: the wait clock
/// never over-counts (P3) and ignores a wall step in both directions. The
/// honour clock would count a forward step of the holder as elapsed time
/// that its grantees' clocks (which did not step) never saw, and the cap
/// would be early by the step for every grant made before it.
pub fn beat_sent_ms(now: Now) -> i64 {
    now.wait.0
}

/// Plan 41 L0: an owner's answer that extends a window (a lock grant's
/// renewal, a reclaim, a delegation's renewal) decided at `at` in lease
/// epoch `epoch` while the tenure's liveness was stale or the tenure was
/// not marked as granting: held until [`Core::renew_verdict`] says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FreshHold {
    /// The decision's reading on the honour clock (plan 43 C7-H1).
    pub at: HonourMs,
    pub epoch: Epoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreshVerdict {
    Ready,
    Wait,
    Refuse,
}

/// A candidate's strike count after it timed out at `now`, its previous
/// strikes and the end of their backoff being `prev`: one more — unless
/// it has had a quiet [`CANDIDATE_BACKOFF_MAX_MS`] since that backoff
/// ended (not timed out again in that time), when it starts over at 1. A
/// node that timed out once an hour ago is not still on its second
/// strike.
pub(crate) fn candidate_strikes_after(prev: Option<(u32, WaitMs)>, now: Now) -> u32 {
    match prev {
        Some((strikes, until)) if now.wait.since(until) < CANDIDATE_BACKOFF_MAX_MS as i64 => {
            strikes.saturating_add(1)
        }
        _ => 1,
    }
}

/// One backup (committed, or a candidate being brought up) as the holder
/// tracks it.
#[derive(Debug, Clone)]
pub(crate) struct BackupPeer {
    /// The journal seq it holds through (its highest cumulative ack).
    /// Catch-up (`backup_promote`, the stream's resend point) is judged on
    /// this; what an acknowledgement may rest on is [`Self::counted`].
    pub acked: u64,
    /// Plan 43 C1-1: the held `config_version` at which this node became a
    /// committed backup in this bring-up (`reconcile_peers_with_lease`);
    /// `None` while it is a candidate or not committed.
    pub listed_at: Option<u64>,
    /// Plan 43 C1-1: the highest cumulative ack of an append that carried
    /// a `config_version >= listed_at`: what an acknowledgement given to a
    /// client may count on this backup (`Core::backed_through`). An ack
    /// given at an older version (as a candidate, or before a restart of
    /// the holder re-listed it) says nothing about the tail the backup
    /// will keep once it is listed: it discards its tail on a lease read
    /// that does not list it.
    pub counted: u64,
    /// The journal seq the appends in flight reach; the next batch
    /// starts past it (pipelining, `Config::backup_max_inflight`).
    pub sent_through: u64,
    /// The appends in flight, oldest first.
    pub inflight: std::collections::VecDeque<Inflight>,
    pub last_sent: WaitMs,
    /// When it last acknowledged something new (or was added), on the
    /// wait clock (as are `alive_at` and `short_since`: they are compared
    /// with the heartbeat answers' stamps and the node's own lapses).
    pub last_progress: WaitMs,
    /// Whether the last append was a resync (the backup's `acked` did not
    /// match ours: it restarted, or we did).
    pub committed: bool,
    /// This bring-up of the backup (`PeerMsg::BackupAppend::candidacy`).
    pub candidacy: u64,
    /// When it was last known alive (`Event::BackupAlive`; its bring-up
    /// counts).
    pub alive_at: WaitMs,
    /// It answered an append short since its last progress: it is
    /// processing appends but stuck, whether alive or not.
    pub short_since_progress: bool,
    /// When it first answered short since its last progress, moved on by
    /// the gaps in its heartbeat answers since (`on_backup_alive`): how
    /// long it has been answering short while answering at all.
    pub short_since: WaitMs,
    /// An append to it failed at the transport (`Event::PeerFailed`)
    /// since its last answer: alive or not (its heartbeat answers may
    /// come on the liveness lane), it is unreachable on the data lane,
    /// and gets the ordinary acknowledgement timeout, not the slow-core
    /// grace.
    pub unreachable: bool,
    /// It has answered the holder's heartbeat (`Event::BackupAlive`) in
    /// this bring-up: a backup known to answer may answer a window late.
    pub answers_beats: bool,
}

/// One pipelined append: its request, send time and the rows it carries.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Inflight {
    pub req: OpId,
    pub sent: WaitMs,
    pub last: u64,
    /// The `config_version` the append carried (plan 43 C1-1): its ack
    /// counts for a backup only if this lists it (`BackupPeer::counted`).
    pub config_version: u64,
}

/// Whether a tenure under this lease can be taken over before it expires
/// (`Core::fast_tenure`, for a lease that is not held yet).
fn lease_fast(lease: &Lease) -> bool {
    lease.ack_policy != AckPolicy::Local || !lease.backups.is_empty()
}

/// A backup-set reconfiguration CAS in flight.
#[derive(Debug, Clone)]
pub(crate) struct Reconfig {
    lease: Lease,
    attempts: u32,
    /// Plan 43 C7-F F-a0 L3: the lease marks itself at this
    /// reconfiguration (the tenure becomes sealable with a duty live): a
    /// retry against a fresher object marks too, whatever the duty is by
    /// then.
    marks: bool,
    /// The version the CAS is conditioned on (`LeaseState::reconfig_doubt`).
    tag: LeaseTag,
    /// The node being added or removed (for the counters).
    add: Option<NodeId>,
    remove: Option<NodeId>,
}

/// The holder's side: backups, the durability watermarks, the
/// reconfiguration in flight.
#[derive(Debug, Default)]
pub(crate) struct AckState {
    /// Plan 43 C1-3 (D2): the last journal seq of the predecessor's tail this
    /// tenure journaled as its own (`apply_backup_tail`; at the gate's end
    /// for a lease that says it inherits a tail and this node re-adopted).
    /// The lease's `inherited_tail` is cleared by the first renewal or
    /// reconfiguration CAS after the journal is shipped through it.
    pub(crate) inherited_last: Option<u64>,
    pub peers: BTreeMap<NodeId, BackupPeer>,
    /// Candidates that answered `sealed` for the epoch held: never asked
    /// again for it (EC2 follow-up (e)).
    pub(crate) refused_epoch: BTreeMap<NodeId, Epoch>,
    /// A peer streamed to before it is added to the lease.
    pub candidate: Option<NodeId>,
    pub(crate) reconfig: Option<Reconfig>,
    /// A reconfiguration the job slot's lease CAS made us postpone.
    pub(crate) reconfig_wanted: Option<Vec<NodeId>>,
    /// M9 `S3`: the shipped-through journal seq and the shipped rows
    /// above it (M4/M7 ship out of order).
    pub shipped_through: u64,
    pub shipped_rows: BTreeSet<u64>,
    pub tick_timer: Option<TimerId>,
    last_reconfig: WaitMs,
    /// The last reconfiguration attempt (a failed CAS is not retried on
    /// every event).
    last_reconfig_try: WaitMs,
    /// Send times of outstanding S3 requests, and the send time of the
    /// latest one that succeeded without revealing a deposition.
    /// On the honour clock (plan 43 C7-H1): the holder's stamp bounding
    /// its own right to answer.
    pub s3_sent: BTreeMap<OpId, HonourMs>,
    pub last_s3_fresh: HonourMs,
    /// EWMA of this node's S3 request round trips (ms, 0 until the
    /// first): what a handoff's successor needs to claim the lease
    /// scales with it (`Core::begin_handoff_pause`).
    pub s3_rtt_ms: u64,
    /// The `MarkGranting` CAS in flight.
    marking: Option<OpId>,
    /// Plan 43 C7-F F-a0 (b): the lease's mark is one the tenure inherited
    /// (its claim CAS, the mark `acquire_won` or the reconfiguration wrote
    /// for a duty it stood on), and none of it is for something this
    /// tenure itself granted or answered (`mark_own`): the mark may be
    /// cleared once nothing it stands on is live
    /// (`Core::maybe_clear_inherited_mark`). Both are reset with the tenure.
    pub(crate) mark_inherited: bool,
    /// The tenure itself marked (a grant or an answer's gate:
    /// `ensure_granting_marked`), or its lease was marked at its claim by
    /// `strict_mounts` or the carry of its own earlier lease: permanent.
    pub(crate) mark_own: bool,
    /// The `ClearMark` CAS (or the re-read that settles it) in flight.
    pub(crate) unmarking: Option<OpId>,
    /// A `ClearMark` CAS whose outcome is not known (in doubt, or its
    /// re-read failed): the lease is read before anything is answered,
    /// granted or marked, whatever the in-memory mark says.
    pub(crate) unmark_unsettled: bool,
    /// The last attempt to clear the mark (a failed one is not retried on
    /// every event).
    last_clear_try: WaitMs,
    /// Plan 41 L0: the lease read that renewal answers held for stale
    /// liveness wait for (`Core::renew_probe`), and when it was sent.
    renew_probe: Option<(OpId, HonourMs)>,
    /// A held renewal answer decided at or before this is refused (the
    /// lease read sent then failed: nothing proves the tenure since).
    renew_refuse_through: HonourMs,
    /// Delegation renewal answers held for liveness (`on_deleg_renew`),
    /// in the order they were made.
    pub(crate) renew_held_deleg: Vec<(FreshHold, NodeId, PeerMsg)>,
    /// What the replica's durability gate was last told.
    reported: Option<(bool, u64, bool, constellation_meta::RowClass)>,
    /// A fast takeover of a tenure that served strict reads is waiting for
    /// its marker, where the successor's floor starts (see the module
    /// doc); `false` after the floor was set or when none is due.
    pub(crate) pending_floor: bool,
    /// Plan 43 C7-F F-a: the cap the predecessor's beats put on the
    /// pending floor (`Core::beat_cap`, an instant of this node's wait
    /// clock); `None`: no usable beats, or the predecessor's guard was
    /// unknown.
    pending_cap: Option<WaitMs>,
    /// Plan 43 C7-F F-a (review F1): the latest end, on this node's wait
    /// clock, of a lock or
    /// delegation grant this node answered under a continuation epoch's
    /// hold (`lock_cap_ms`, `grant_cap_ms`: capped by the ttl alone, the
    /// lease having no expiry then). `epoch_tenure_resumed` keeps those
    /// grants across the re-claim, whose lease may end before them when a
    /// ttl is longer than the lease's: the beat's guard names it, so a
    /// fast successor's cap never ends before them. Recorded by the cap
    /// functions on every query under a hold (an upper bound: a query may
    /// not end in a grant).
    pub(crate) hold_grant_until: std::sync::atomic::AtomicI64,
    /// Where this tenure's takeover floor ends (the later of its
    /// acknowledgement floor and its lock grace, `note_marker_landed`),
    /// on the wait clock (plan 43 C7-H1: a successor waits it out, and a
    /// forward wall step must not end it early); 0 for a tenure with
    /// none: the lease is not idle-released before it (`in_takeover_floor`).
    floor_until: WaitMs,
    /// Plan 43 C7a (commit 4): the epoch the persisted claim mark names
    /// (`Replica::set_takeover_floor_pending`): a fast claim CAS for it went
    /// out (or the mark was read at start) and the floor it is to arm is
    /// not yet durable. A restart that re-adopts its own lease at this epoch
    /// arms the floor again, counted from its own marker (`acquire_won`).
    pub(crate) floor_mark: Option<Epoch>,
    /// Set once the floor the mark stands for is armed and its ends written
    /// on the core are durable (`persist_floor`): the lock-grant horizon the
    /// mark still waits for (`on_lock_horizon_persisted` removes the mark
    /// once it is durable; `WaitMs(i64::MIN)`: none). `None`: the mark is not
    /// covered yet (the claim's CAS or marker is out, or a write failed),
    /// and no completed lock-horizon write removes it.
    pub(crate) floor_mark_locks: Option<WaitMs>,
    /// A takeover floor of this node is persisted
    /// (`Replica::note_takeover_floor`, or loaded at start): removed with
    /// the tenure (`lock_on_lease_gone`).
    pub(crate) floor_persisted: bool,
    /// The journal seq the last stream-ahead covered, for every
    /// subscriber.
    streamed_through: u64,
    /// Subscribers streamed further than `streamed_through`: the
    /// forwarder of a manifest whose chunks are still uploading on it
    /// gets that transaction (it has the bytes) before anyone else may
    /// (`Replica::releasable_prefix`). Dropped once the common cursor
    /// catches up.
    ahead_of: BTreeMap<NodeId, u64>,
    /// The stream-ahead hold-off timer, and whether rows became durable
    /// while it ran (sent when it fires).
    stream_ahead_timer: Option<TimerId>,
    stream_ahead_pending: bool,
    /// The last run of the periodic backup bookkeeping (selection,
    /// timeouts, promotion): a few times per second is plenty, and the
    /// per-event path stays the append and the release.
    last_housekeeping: WaitMs,
    /// The tick fired since the last bookkeeping run.
    tick_fired: bool,
    /// Whether a peer in budget is eligible as a backup right now (the
    /// last selection pass saw one); `None` until this tenure's first
    /// selection pass ran, which gates like `Some(true)` (see
    /// `durable_jseq`).
    pub eligible: Option<bool>,
    /// When each node was last dropped or removed (not re-added within
    /// `backup_reconfig_min_ms`).
    last_dropped: BTreeMap<NodeId, WaitMs>,
    /// Candidates that timed out lately: how many times in a row, and
    /// until when they are not brought up again ([`candidate_backoff_ms`];
    /// the count decays, [`candidate_strikes_after`]).
    candidate_strikes: BTreeMap<NodeId, (u32, WaitMs)>,
    /// The highest journal seq any acknowledgement of this tenure may
    /// have rested on (the durable seq's high-water mark). A backup is
    /// removed from the lease only once everything up to here is in the
    /// log or on every backup that stays: an acknowledgement given on
    /// the strength of the removed backup must not become one that rests
    /// on this node's disk alone (the model's `acking_before_the_removal
    /// _cas_lands` counterexample's sibling: removing a backup that holds
    /// acknowledged, unshipped rows).
    acked_hwm: u64,
    /// The last candidacy handed out (`BackupPeer::candidacy`).
    last_candidacy: u64,
}

impl AckState {
    /// A grant made under an epoch hold ends by `until` (`hold_grant_until`).
    pub(crate) fn note_hold_grant(&self, until: WaitMs) {
        self.hold_grant_until
            .fetch_max(until.0, std::sync::atomic::Ordering::Relaxed);
    }
}

/// This node as a backup, and as a pre-S3 stream subscriber.
#[derive(Debug, Default)]
pub(crate) struct BackupState {
    pub role: Option<BackupRole>,
    /// The journal seq held through *contiguously* for `role`'s epoch.
    pub acked: u64,
    /// Transactions persisted ahead of a gap (the holder pipelines
    /// appends, and requests may overtake each other): `first -> last`,
    /// folded into `acked` as the gap closes.
    pub(crate) held: BTreeMap<u64, u64>,
    pub last_heard: WaitMs,
    /// The highest epoch sealed (persisted by the replica too).
    pub sealed: Epoch,
    pub watch_timer: Option<TimerId>,
    takeover_get: Option<OpId>,
    /// When the silence watch issued `takeover_get`: the holder heard
    /// after this is alive, and the epoch is not sealed.
    takeover_read_at: WaitMs,
    /// `takeover_get` was issued because the holder's lease expiry as
    /// this node knew it had passed, not for silence: the object decides
    /// (`on_takeover_get`) — expired, sealed; renewed, a window again.
    /// (A read for silence that finds the lease unexpired judges the
    /// silence again in this node's attentive time, the same way.)
    takeover_read_for_expiry: bool,
    /// Subscriber: the next journal seq a `StreamAhead` may install for
    /// `(epoch, jseq)` — contiguity with what the log and earlier batches
    /// gave us.
    pub(crate) ahead_next: Option<(Epoch, u64)>,
    /// Subscriber: `StreamAhead` batches that arrived before the log
    /// segment they follow (`base`) — the holder streams them right after
    /// shipping it, on another QUIC stream, so they often overtake it.
    /// Kept (oldest first, a few) and installed once the segment is
    /// applied ([`Core::retry_stream_ahead`]) instead of dropped, which
    /// sent every forward whose reply waited for them to the log.
    ahead_waiting: std::collections::VecDeque<(NodeId, Epoch, Seq, Vec<BackupTx>)>,
    /// Subscriber: `StreamAhead` transactions that did not follow what
    /// this replica holds — a batch arrived while a job had the cursor,
    /// or after an earlier one was lost — kept until the log closes the
    /// gap ([`Core::retry_stream_ahead`]). The holder streams each
    /// transaction once: dropping them left the cursor behind the
    /// stream for as long as the writer kept writing (every later batch
    /// was "not contiguous" too), so this node saw every write only
    /// through S3 — one segment PUT and the ship pacing, 0.4-5 s at
    /// 300 ms per S3 request (EC2 campaign 6, visibility-s3-latency).
    ahead_gapped: std::collections::VecDeque<(NodeId, Epoch, Seq, Vec<BackupTx>)>,
    /// `ack=s3` fast takeover: since when the known holder has been
    /// silent on P2P.
    holder_silent_since: Option<WaitMs>,
    /// Plan 43 C7b: when `ack=s3`'s silence watch last read the lease to
    /// refresh a stale view (`watch_s3_holder`).
    s3_reread_at: Option<WaitMs>,
    /// Plan 43 C7b: when that watch last read the lease for want of any
    /// observation of it (at most once per holder renewal cadence).
    s3_cold_read_at: Option<WaitMs>,
    /// EC2 follow-up 3b: this node restarted holding a backup role and
    /// has not yet had a P2P link to its holder since: its downtime (and
    /// the redial) is not the holder's silence.
    restarted: bool,
    /// The lease read in flight is a restarted backup's probe (not
    /// sealed first): it seals and takes over only an expired lease.
    restart_probe: bool,
    /// The holder's latest candidacy for `role` this node has had an
    /// append of (0: none since this process started): a dismissal
    /// naming another one is stale.
    candidacy: u64,
    /// What this node has heard of each holder's off-core heartbeat
    /// (`Event::HolderAlive`), as a backup or under `ack=s3`
    /// ([`HolderBeats`]).
    pub(crate) beats: BTreeMap<NodeId, HolderBeats>,
    /// The silence measured by the latest seal watch that read the lease
    /// to seal, and by the latest that sealed ([`SealRecord`]).
    watch_read: Option<SealRecord>,
    last_seal: Option<SealRecord>,
    /// This node's own recent lapses (`Event::OwnLag`), `(from, until)`,
    /// oldest first, merged where they overlap: no silence counts before
    /// the latest one ended ([`Core::attentive_since`]).
    own_lapses: std::collections::VecDeque<(WaitMs, WaitMs)>,
    /// Plan 43 C1-3: the claim's attempt to fetch the group's sealed tail
    /// in flight, the timer id that ends it, the tail a claim carries
    /// (fetched, a self-sealed holder's own, until the lease is won), the
    /// wait across attempts (`waiting_for_group`), the epoch whose group
    /// the operator abandoned (to be claimed without), the abandoned epoch
    /// the claim in flight records in its marker, and the epochs this node
    /// keeps a member copy of.
    pub(crate) tf: Option<super::seal_tail::TailFetch>,
    pub(crate) tf_round: Option<OpId>,
    pub(crate) fetched: Option<super::seal_tail::FetchedTail>,
    pub(crate) group_wait: Option<super::seal_tail::GroupWait>,
    pub(crate) abandon: Option<Epoch>,
    pub(crate) abandoned_claim: Option<Epoch>,
    pub(crate) member_epochs: Vec<Epoch>,
}

/// The most own lapses remembered (older ones than
/// [`LATE_BEATS_MEMORY_MS`] go first).
const OWN_LAPSES_MAX: usize = 64;

/// How long a beat that came late lengthens the silence window
/// ([`Core::silence_window`]).
const LATE_BEATS_MEMORY_MS: i64 = 60_000;
/// The most late beats remembered per holder.
const LATE_BEATS_MAX: usize = 16;

/// What a seal watch measured when it read the lease to seal (`status.ack`
/// reports the latest seal's): the holder and epoch, the plain silence,
/// the silence it counted (not across its own lapses), the window it
/// waited, and when.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SealRecord {
    pub holder: NodeId,
    pub epoch: Epoch,
    pub silent_ms: i64,
    pub attentive_ms: i64,
    pub window_ms: i64,
    pub at: Ms,
}

/// How far a silence watch's start may move for this node's own lapses
/// ([`Core::lag_discount`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Reach {
    /// This much past the holder's last word, plus the lapses' length:
    /// a reach in the node's own running time.
    PlusLapses(i64),
    /// This much past it, lapses included: a reach in wall time.
    Flat(i64),
}

/// What a silence watch watches: `(holder, epoch, seal)` — `seal`: the
/// backup's seal watch (else `ack=s3`'s holder watch).
pub(crate) type Watched = (NodeId, Epoch, bool);

/// One holder's off-core heartbeat as this node hears it.
#[derive(Debug, Clone, Default)]
pub(crate) struct HolderBeats {
    pub(crate) epoch: Epoch,
    /// The latest arrival of a beat at `epoch` (or of an append, handed
    /// over by the driver as an unnumbered one), on this node's wait
    /// clock.
    heard_at: WaitMs,
    /// The latest numbered beat: `(seq, sent, arrival, clock)`; `sent` a
    /// reading of the holder's wait clock `clock` (plan 43 C7-H1).
    pub(crate) last: Option<(u64, i64, WaitMs, u64)>,
    /// Plan 43 C7b (row 24), the seal watch's evidence of where the holder's
    /// lease ends: every arrival of the holder's at `epoch` (a numbered beat,
    /// an arrival the driver handed over, an append) says, as readings of the
    /// holder's own wait clock, when it was sent and where the lease it holds
    /// ends, per clock id of the holder (at most [`BEAT_CLOCKS_KEPT`], oldest
    /// first) like `clocks`. [`Core::known_end`] places the end on this node's
    /// clock as `named end + the least (arrival − sent) lately`.
    ends: Vec<(u64, EndAgg)>,
    /// The holder clock id whose samples came last (0: none): a restarted
    /// holder is a new process with a new clock, and the earlier one's end
    /// is not this one's.
    end_clock: u64,
    /// Plan 43 C7-F F-a / C7-H1: what the numbered beats of `epoch` say of
    /// where the holder's lease ends on *this* node's clock, for the cap of
    /// a fast takeover's floor ([`Core::beat_cap`]), per clock id of the
    /// holder (at most [`BEAT_CLOCKS_KEPT`], oldest first): a restarted
    /// holder is a new process with a new wait clock, and values of two
    /// clocks must never be mixed.
    pub(crate) clocks: Vec<(u64, BeatClockAgg)>,
    /// A beat at this epoch named the guard `i64::MAX`: the holder's
    /// takeover gate was pending, what it waited out was not known, which
    /// gives no cap for the whole epoch.
    pub(crate) guard_unknown: bool,
    /// Beats that came late lately, `(arrival, ms late)`, oldest first:
    /// later than one interval after the previous one, as sent (the
    /// holder's process was held up) or as heard (held up on the way, or
    /// by this node's own runtime), by at least the lag threshold.
    late: std::collections::VecDeque<(WaitMs, i64)>,
}

impl HolderBeats {
    /// An arrival at `at` (this node's wait clock) of the holder's, sent at
    /// `sent` and naming the end `expires` of the lease it holds, both
    /// readings of its wait clock `clock` ([`HolderBeats::ends`]). An
    /// arrival naming none (0, or `i64::MAX`: its takeover gate is pending)
    /// says nothing of the end.
    fn end_sample(&mut self, at: WaitMs, clock: u64, sent: i64, expires: i64) {
        if clock == 0 || sent <= 0 || expires <= 0 || expires == i64::MAX {
            return;
        }
        let i = match self.ends.iter().position(|(c, _)| *c == clock) {
            Some(i) => i,
            None => {
                if self.ends.len() >= BEAT_CLOCKS_KEPT {
                    self.ends.remove(0);
                }
                self.ends.push((clock, EndAgg::default()));
                // A clock not seen before is a restarted holder's: its
                // samples stand for the end from now on.
                self.end_clock = clock;
                self.ends.len() - 1
            }
        };
        let agg = &mut self.ends[i].1;
        if agg.lags.len() >= BEAT_LAGS_KEPT {
            agg.lags.pop_front();
        }
        agg.lags.push_back((sent, at.0.saturating_sub(sent)));
        agg.named_max = agg.named_max.max(expires);
    }

    /// The terms of the holder clock `clock`, started afresh when it is
    /// new; past [`BEAT_CLOCKS_KEPT`] clocks the oldest goes.
    fn clock_agg(&mut self, clock: u64) -> &mut BeatClockAgg {
        let i = match self.clocks.iter().position(|(c, _)| *c == clock) {
            Some(i) => i,
            None => {
                if self.clocks.len() >= BEAT_CLOCKS_KEPT {
                    self.clocks.remove(0);
                }
                self.clocks.push((clock, BeatClockAgg::default()));
                self.clocks.len() - 1
            }
        };
        &mut self.clocks[i].1
    }
}

/// What the numbered beats of one holder clock (one process of the holder)
/// say, for [`Core::beat_cap`].
#[derive(Debug, Clone, Default)]
pub(crate) struct BeatClockAgg {
    /// `(sent, at − sent)` of the latest [`BEAT_LAGS_KEPT`] beats: `sent` a
    /// reading of the holder's wait clock, `at` the arrival as this node
    /// stamped it (never a credit), so `at − sent` places that clock on
    /// this node's: a difference on the holder's clock (rate only, no
    /// offset, no wall step: its wait clock ignores them in both
    /// directions, P4) plus a stamp of this node's.
    pub(crate) lags: std::collections::VecDeque<(i64, i64)>,
    /// The highest lease end (wait) and the highest finite guard those
    /// beats named, on that clock.
    pub(crate) named_max: i64,
    pub(crate) guard_max: i64,
}

/// What the arrivals of one holder clock say of where its lease ends
/// ([`HolderBeats::ends`]).
#[derive(Debug, Clone, Default)]
pub(crate) struct EndAgg {
    /// `(sent, arrival − sent)` of the latest [`BEAT_LAGS_KEPT`] arrivals:
    /// `sent` a reading of the holder's wait clock, `arrival` the stamp on
    /// this node's. The least difference is the arrival stamped least late:
    /// an arrival this node stamped late (its runtime was held up, the
    /// driver stamps a backlog when it gets to it) lengthens its own
    /// difference and no other's.
    lags: std::collections::VecDeque<(i64, i64)>,
    /// The highest lease end any arrival named, on the holder's wait clock.
    named_max: i64,
}

/// How many holder clock ids a [`HolderBeats`] keeps terms for.
const BEAT_CLOCKS_KEPT: usize = 2;

/// How many beats' `(sent, at − sent)` a [`HolderBeats`] keeps for the cap
/// of a fast takeover's floor (about 19 s of beats at the default
/// interval; the cap needs only the recent ones).
const BEAT_LAGS_KEPT: usize = 64;

/// How many early `StreamAhead` batches a subscriber keeps.
const AHEAD_WAITING_MAX: usize = 32;

/// Plan 43 §5.1: what an answer waits for — the commit point of the
/// sequencer that gives it. C0 names it (the simulation's I1 oracle
/// classifies every answer by it); C1/C2 make the core decide it at
/// journal time (`Core::ack_class`). Nothing in the core uses it yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AckClass {
    /// An applied, unfenced log segment carries the outcome.
    Archive,
    /// The root group: the holder's journal and every backup the lease
    /// object lists hold the transaction.
    RootGroup,
    /// The delegation's group: the delegate's journal and each recorded
    /// backup's delegate tail, or the root's appended row `(gen, idx)`.
    DelegGroup { gen: u64 },
    /// A designee or an epoch's hold owner (§11 Q3): not stealable.
    NonStealable,
    /// A lone sequencer answering its own node's client (Layer A).
    Lone,
}

impl AckClass {
    /// Q12's rule: the classes whose answer waited for a group or the
    /// archive, so what it says is committed. Not `NonStealable` and not
    /// `Lone`. (`RowClass::gated`, the stored class's.)
    pub fn gated(&self) -> bool {
        !matches!(self, AckClass::NonStealable | AckClass::Lone)
    }

    /// The class's names in counter order (`AckClass::index`).
    pub const NAMES: [&'static str; 5] = [
        "archive",
        "root-group",
        "deleg-group",
        "non-stealable",
        "lone",
    ];

    /// The class's place in `Stats::ack_classes`.
    pub fn index(&self) -> usize {
        match self {
            AckClass::Archive => 0,
            AckClass::RootGroup => 1,
            AckClass::DelegGroup { .. } => 2,
            AckClass::NonStealable => 3,
            AckClass::Lone => 4,
        }
    }

    /// The class's name in reports and counters.
    pub fn name(&self) -> &'static str {
        match self {
            AckClass::Archive => "archive",
            AckClass::RootGroup => "root-group",
            AckClass::DelegGroup { .. } => "deleg-group",
            AckClass::NonStealable => "non-stealable",
            AckClass::Lone => "lone",
        }
    }
}

impl From<AckClass> for constellation_meta::RowClass {
    /// Plan 43 C1-2: the class stored with a journal row.
    fn from(class: AckClass) -> Self {
        match class {
            AckClass::Archive => constellation_meta::RowClass::Archive,
            AckClass::RootGroup => constellation_meta::RowClass::RootGroup,
            AckClass::DelegGroup { .. } => constellation_meta::RowClass::DelegGroup,
            AckClass::NonStealable => constellation_meta::RowClass::NonStealable,
            AckClass::Lone => constellation_meta::RowClass::Lone,
        }
    }
}

/// Whether a holder's reply is a definitive answer (the outcome of the op,
/// accepted or refused) as opposed to a redirect, a retry or a hold.
pub(crate) fn is_answer(outcome: &MutateOutcome) -> bool {
    matches!(
        outcome,
        MutateOutcome::Accepted { .. }
            | MutateOutcome::Errno(_)
            | MutateOutcome::Exists { .. }
            | MutateOutcome::Conflict { .. }
            | MutateOutcome::LockLapsed
    )
}

/// Plan 30 §M9's view for `status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AckView {
    /// The policy of the lease this node holds (`"local"`, `"backup"`,
    /// `"s3"`; `"-"` when it holds none).
    pub policy: &'static str,
    /// The committed backups, and the candidate being brought up.
    pub backups: Vec<NodeId>,
    pub candidate: Option<NodeId>,
    pub config_version: u64,
    /// The durable journal seq (min over the committed backups' acks, or
    /// the shipped-through seq under `S3`), and the journal tip.
    pub durable: u64,
    pub parked_acks: usize,
    /// This node backs `holder` at `epoch` (0/0: nobody) through `acked`;
    /// the highest epoch it sealed.
    pub backing_holder: NodeId,
    pub backing_epoch: Epoch,
    pub backing_acked: u64,
    pub sealed_epoch: Epoch,
    /// The latest seal this node made: what its watch measured.
    pub last_seal: Option<SealRecord>,
    pub reconfig_in_flight: bool,
    /// Plan 43 C2a: the definitive answers this node gave as a sequencer
    /// by the class they waited for (`Stats::ack_classes`, named by
    /// `AckClass::NAMES`).
    pub classes: Vec<(&'static str, u64)>,
    /// Plan 43 C1-3: a claim of the lease waits for the tail of an old
    /// group (`status.lease.waiting_for_group`), and the member copies this
    /// node keeps of epochs it sealed.
    pub waiting_for_group: Option<super::seal_tail::WaitingForGroup>,
    pub member_copies: usize,
}

impl Core {
    /// The backups the holder's off-core heartbeat goes to: the committed
    /// ones and the candidate, each with its candidacy (0: none known —
    /// such a node is never dismissed).
    ///
    /// A member of the group the held lease names as the tail of its own
    /// epoch (a restart re-adoption, `own_group_tail`) is listed with
    /// candidacy 0 while the lease names it: its copy of the rows the log
    /// does not carry yet predates this incarnation's candidacies, and a
    /// dismissal (a bring-up that timed out, a removal) would wipe it. It
    /// lets go of the copy when a lease read stops naming it.
    pub fn backup_candidacies(&self) -> Vec<(NodeId, u64)> {
        let named = |n: NodeId| {
            self.lease.held.as_ref().is_some_and(|(l, _)| {
                l.inherited_tail
                    .as_ref()
                    .is_some_and(|t| t.epoch == l.epoch && t.members.contains(&n))
            })
        };
        self.lease
            .backups()
            .iter()
            .copied()
            .chain(self.ack.candidate)
            .map(|n| {
                let candidacy = if named(n) {
                    0
                } else {
                    self.ack.peers.get(&n).map_or(0, |p| p.candidacy)
                };
                (n, candidacy)
            })
            .collect()
    }

    /// Who the holder's off-core heartbeat goes to, with candidacies:
    /// under `ack=s3`, the subscribers of its log stream (they read its
    /// silence, `watch_s3_holder`; candidacy 0, never dismissed);
    /// otherwise its committed backups and the candidate
    /// ([`Self::backup_candidacies`]).
    pub fn liveness_targets(&self) -> Vec<(NodeId, u64)> {
        if self.lease.ack_policy() == AckPolicy::S3 {
            return self
                .stream_subscribers()
                .into_iter()
                .map(|n| (n, 0))
                .collect();
        }
        self.backup_candidacies()
    }

    pub(crate) fn backup_view(&self) -> AckView {
        let policy = match self.lease.ack_policy() {
            _ if self.lease.held.is_none() => "-",
            AckPolicy::Local => "local",
            AckPolicy::Backup => "backup",
            AckPolicy::S3 => "s3",
        };
        AckView {
            policy,
            backups: self.lease.backups().to_vec(),
            candidate: self.ack.candidate,
            config_version: self
                .lease
                .held
                .as_ref()
                .map(|(l, _)| l.config_version)
                .unwrap_or(0),
            durable: self.durable_jseq(),
            parked_acks: self.parked_durable_count(),
            backing_holder: self.bk.role.map(|r| r.holder).unwrap_or(0),
            backing_epoch: self.bk.role.map(|r| r.epoch).unwrap_or(0),
            backing_acked: self.bk.acked,
            sealed_epoch: self.bk.sealed,
            last_seal: self.bk.last_seal,
            reconfig_in_flight: self.ack.reconfig.is_some(),
            classes: AckClass::NAMES
                .iter()
                .copied()
                .zip(self.stats.ack_classes)
                .collect(),
            waiting_for_group: self.waiting_for_group(self.clk),
            member_copies: self.bk.member_epochs.len(),
        }
    }

    // ------------------------------------------------------------ start

    /// At start: a persisted backup role (this node backed someone
    /// before it restarted) and a persisted seal are honoured.
    pub(crate) fn backup_start(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.bk.sealed = replica.backup_sealed_epoch();
        self.bk.member_epochs = replica.member_copy_epochs();
        if let Some(role) = replica.backup_role() {
            self.bk.role = Some(role);
            self.bk.acked = replica.backup_acked(role.epoch);
            self.bk.last_heard = now.wait;
            self.bk.restarted = true;
            self.lease.cached_holder = Some(role.holder);
            self.arm_backup_watch(now, out);
        }
    }

    // ------------------------------------------------------- durability

    /// Plan 30 §M9: whether this holder's tenure may be taken over
    /// before its lease expires (a non-`Local` policy or a listed
    /// backup), so its own strict reads need fresh S3 liveness
    /// (`LeaseView::reads_locally`). The liveness itself is the send time
    /// of the latest S3 request that succeeded without revealing a
    /// deposition (`last_s3_fresh`).
    pub fn fast_tenure(&self) -> bool {
        self.lease.held.is_some()
            && (self.lease.ack_policy() != AckPolicy::Local || !self.lease.backups().is_empty())
    }

    /// Whether this tenure may be taken over before its lease expires as
    /// far as this node knows: it is fast, or a reconfiguration CAS whose
    /// outcome is not known (in doubt, or still out) may have listed a
    /// backup that seals it (`doubted_backups`, plan 43 C7-F F-a0). Its
    /// strict reads, new grants and renewals need fresh S3 liveness
    /// (`renew_fresh`, and the FUSE side's `LeaseView::reads_locally`).
    pub fn may_be_taken_over_early(&self) -> bool {
        self.fast_tenure() || self.doubted_backups().next().is_some()
    }

    pub fn last_s3_fresh(&self) -> HonourMs {
        self.ack.last_s3_fresh
    }

    /// Whether acknowledgements are gated by durability right now: the
    /// FUSE fast path stays closed and every local mutation goes through
    /// the core (`LeaseView::ack_gated`).
    pub fn ack_gated(&self) -> bool {
        self.lease.held.is_some() && !self.lease.lost && self.durable_jseq() != u64::MAX
    }

    /// The durable journal seq under the current policy: `u64::MAX` when
    /// nothing gates acknowledgements.
    pub(crate) fn durable_jseq(&self) -> u64 {
        if self.lease.epoch_held() {
            // Plan 30 §M10 (found by `node-leave` on M9's round 2): a
            // continuation epoch's hold acknowledges locally — nothing
            // ships and nothing is backed during an epoch, and the claim
            // rule carried the lease in only with every backup a member
            // (which takes nothing over while the epoch is open). Gating
            // here on the held lease's backups, a candidate or the
            // shipped watermark parked every acknowledgement for the
            // epoch's lifetime: the FUSE caller timed out, retried, and
            // met its own first attempt (`EEXIST`).
            return u64::MAX;
        }
        match self.lease.ack_policy() {
            AckPolicy::S3 => self.ack.shipped_through,
            AckPolicy::Local | AckPolicy::Backup => {
                // Plan 43 C7-F F-a0: a reconfiguration whose outcome is not
                // known (in doubt, or the first listing still out) may have
                // listed backups this node does not hold listed
                // (`ack_constraint_sets`). One of them that seals this
                // tenure takes over with its tail and the log, nothing
                // else: an acknowledgement rests on each of them as on a
                // committed backup — and on the held version's rule too,
                // for the CAS may not have landed (a candidate the lease
                // does not list voids its tail when the holder is gone).
                // Under a held `Local` lease this ends acknowledging on
                // this disk alone until the lease is next adopted from S3
                // (a `LeasePut` timeout, then the candidate cut off and
                // nobody else in budget: the listed candidate sealed and
                // took over without what was acknowledged meanwhile).
                //
                // Plan 43 C1-1: every such version is a constraint of its
                // own, an empty one included (a removal of the last backup
                // is the log: nothing on any backup's disk is what the
                // version leaves), each counted on its own set. A version
                // that may be in S3 is a configuration a reconfigurer may
                // read, and a row acknowledged now must rest, whatever
                // version S3 holds, on the group that version names.
                let held = self.durable_jseq_held();
                self.ack_constraint_sets().fold(held, |d, set| {
                    d.min(self.backed_through(set.iter().copied()))
                })
            }
        }
    }

    /// `durable_jseq` under the held version's `Local` or `Backup` policy.
    fn durable_jseq_held(&self) -> u64 {
        let committed = self.lease.backups();
        if committed.is_empty() {
            // While a backup could be had — a candidate is being brought
            // up, or a peer in budget is eligible — nothing is acknowledged
            // under `Local`: such an acknowledgement would rest on nobody's
            // disk but ours, and the candidate could not take over (it is
            // not listed). The wait ends with the CAS that lists it, or
            // once no peer is in budget after all (today's behaviour).
            //
            // Not yet known counts as could-be (backup-crash-slow seed
            // 603322): the ops queued behind an acquisition run in the
            // acquisition's own event, before the tenure's first selection
            // pass, and were acknowledged at once on this disk alone.
            let unassessed = self.ack.eligible.is_none() && self.backups_possible();
            return if self.ack.candidate.is_some() || self.ack.eligible == Some(true) || unassessed
            {
                self.ack.shipped_through
            } else {
                u64::MAX
            };
        }
        self.backed_through(committed.iter().copied())
    }

    /// The journal seq every one of `backups` holds, or the log does
    /// (the empty set is the log alone).
    ///
    /// Plan 43 C1-1: what a backup holds *for this purpose* is
    /// `BackupPeer::counted`, its acknowledgements of appends that carried
    /// a configuration version listing it, never the running maximum of
    /// every acknowledgement (`acked`). Why that is enough: a backup
    /// acknowledges an append carrying `v` only after it recorded `v` in
    /// its role (`on_backup_append`); from then on it discards its tail
    /// only on a lease read that does not list it (`on_takeover_get`'s
    /// catch-all) at a version `>= v` (an older read is stale and ignored:
    /// S3 holds a version `>= v` by then, the holder sent `v` only after
    /// its CAS landed), which lists it unless this holder removed it
    /// (while a removal is out or in doubt only the staying set counts:
    /// `ack_constraint_sets`). Before, the acknowledgements a
    /// candidate gave while it was not yet listed were counted from the
    /// moment its listing landed, though a candidate that read the lease
    /// while the listing was in flight had discarded that tail
    /// (`long-backup` seeds 892 and 472: an acknowledged row on no disk
    /// but the holder's, and the TTL successor never saw it).
    fn backed_through(&self, backups: impl Iterator<Item = NodeId>) -> u64 {
        backups
            .map(|b| self.ack.peers.get(&b).map(|p| p.counted).unwrap_or(0))
            .min()
            .unwrap_or(0)
            .max(self.ack.shipped_through)
    }

    /// Plan 43 C1-1: the backup sets, besides the held version's, that an
    /// acknowledgement must rest on: the set of every reconfiguration in
    /// doubt (an answer lost: it may have landed) and the set of the one
    /// out, when it lists a first backup or removes one, as long as it
    /// is conditioned on the version held. Each may be what S3 holds when
    /// the holder dies; none may be empty-handed:
    /// - a listing out or in doubt: its backups may seal this tenure
    ///   (plan 43 C7-F F-a0);
    /// - a removal out or in doubt: the backup it removes is still in the
    ///   held set, so a row acknowledged on it alone, between the check
    ///   that judged the removal safe and the landing, would rest after
    ///   the landing on the holder alone under a version that lists
    ///   nobody, which a TTL claimant never fetches from. The staying
    ///   set (empty: the log) carries it.
    fn ack_constraint_sets(&self) -> impl Iterator<Item = &[NodeId]> + '_ {
        let doubt = self
            .lease
            .reconfig_doubt
            .iter()
            .flat_map(|d| d.sets.iter().map(Vec::as_slice));
        let out = self.lease.held.iter().filter_map(|(held, tag)| {
            self.ack
                .reconfig
                .as_ref()
                .filter(|r| r.tag == *tag && (r.remove.is_some() || held.backups.is_empty()))
                .map(|r| r.lease.backups.as_slice())
        });
        doubt.chain(out)
    }

    /// Whether journal seq `jseq` is durable now: on every committed
    /// backup, or shipped (a row in the log is durable under any policy).
    pub(crate) fn durable_covers(&self, jseq: u64) -> bool {
        jseq <= self.durable_jseq() || self.ack.shipped_rows.contains(&jseq)
    }

    /// The journal seq an acknowledgement evaluated at `position` must
    /// wait for, or `None` when it may be given now.
    ///
    /// The gate is exactly `durable_jseq`'s: nothing waits only when it
    /// says nothing gates (a continuation epoch; `Local` with no backup
    /// to be had). In particular `Local` with an *eligible* peer but no
    /// candidate yet — the moment after a removal CAS emptied the set,
    /// before the next selection names one — gates too (backup-crash
    /// seed 606255: an acknowledgement given then rested on the holder's
    /// disk alone; the holder died, its successor never saw the row, and
    /// the acknowledged create came back as a conflict copy).
    pub(crate) fn ack_need(&self, position: &constellation_meta::Position) -> Option<u64> {
        if self.durable_jseq() == u64::MAX {
            return None;
        }
        let jseq = position.pending.map(|p| p.jseq)?;
        (!self.durable_covers(jseq)).then_some(jseq)
    }

    /// Plan 43 §5.1: the class of an answer this node gives as the root
    /// sequencer, by what the answer waits for *today* (C2a records it; C2c
    /// and C2f change what some of them wait for): a continuation epoch's
    /// hold is non-stealable; under `ack=s3` the archive; with a listed (or
    /// doubted) backup the root group; while a backup is being had (a
    /// candidate, an eligible or not yet assessed peer) the archive; and
    /// otherwise nothing but this node's disk — `Lone`, whoever is
    /// answered. The same decision as [`Self::ack_need`] (`durable_jseq`),
    /// named.
    pub fn root_ack_class(&self) -> AckClass {
        if self.lease.epoch_held() {
            AckClass::NonStealable
        } else if self.lease.ack_policy() == AckPolicy::S3 {
            AckClass::Archive
        } else if !self.lease.backups().is_empty() || self.doubted_backups().next().is_some() {
            AckClass::RootGroup
        } else if self.durable_jseq() != u64::MAX {
            AckClass::Archive
        } else {
            AckClass::Lone
        }
    }

    /// Plan 43 C1-2: the class a journal row this node writes now is tagged
    /// with, for the scope of the journaling call (the guard): what an answer
    /// to the row's own client waits for ([`Self::root_ack_class`]). A
    /// forward executed here is journaled as an own op until C2 gives it the
    /// archive. The class is fixed at the journaling; only a row answered
    /// with the gate off is re-tagged `Lone` (`complete_ready`).
    pub(crate) fn journal_class(&self) -> constellation_meta::PendingClassGuard {
        constellation_meta::PendingClass::set(self.root_ack_class().into())
    }

    /// Plan 43 C1-2: the class of an answer to a rid this node already
    /// journaled (a retry's dedup, an in-doubt op's resolution): the class
    /// of the row itself, not what an answer would wait for now — a row
    /// journaled `Lone` (the gate was off) answered `RootGroup` because a
    /// backup has been listed since would tell its requester it is
    /// committed, and a deposition would then treat it as not. A row that
    /// is no longer in the journal is in the log (`Archive`) or, failing
    /// that, answered as the root's now.
    ///
    /// `Err`: the row had to be re-tagged `Lone` and the store failed the
    /// write; the answer may claim neither class, and the caller answers
    /// `EIO` ([`Self::eio_unless_recorded`]; the C1-2/C1-3 review).
    pub(crate) fn journaled_answer_class(
        &self,
        replica: &dyn Replica,
        rid: Rid,
    ) -> Result<AckClass, constellation_meta::MetaError> {
        Ok(match replica.journal_class_of_rid(rid) {
            // A gated row nothing gates any more (its group is gone, the
            // lease lists no backup): answered on this disk alone, so it
            // is a lone node's answer and the row is re-tagged `Lone`, as
            // a parked one released with the gate off is (D4).
            Some((first, class))
                if class.gated()
                    && self.durable_jseq() == u64::MAX
                    && !self.ack.shipped_rows.contains(&first) =>
            {
                replica
                    .reclass_journal_tx(first, constellation_meta::RowClass::Lone)
                    .inspect_err(|error| {
                        tracing::warn!(node = self.cfg.node_id, ?rid, %error, "could not re-tag a row answered with the gate off: EIO");
                    })?;
                AckClass::Lone
            }
            Some((_, constellation_meta::RowClass::Lone)) => AckClass::Lone,
            Some((_, constellation_meta::RowClass::Archive)) => AckClass::Archive,
            Some((_, constellation_meta::RowClass::RootGroup)) => AckClass::RootGroup,
            Some((_, constellation_meta::RowClass::NonStealable)) => AckClass::NonStealable,
            Some((_, constellation_meta::RowClass::DelegGroup)) | None => self.root_ack_class(),
        })
    }

    /// The answer to give when its class is `class`: `outcome` under it,
    /// or — the store failed the `Lone` re-tag ([`Self::journaled_answer_class`])
    /// — `EIO`, sourced as this node's uncommitted answer.
    pub(crate) fn eio_unless_recorded<T>(
        outcome: MutateOutcome,
        class: Result<T, constellation_meta::MetaError>,
        uncommitted: T,
    ) -> (MutateOutcome, T) {
        match class {
            Ok(class) => (outcome, class),
            Err(_) => (
                MutateOutcome::Errno(constellation_types::Code::Io),
                uncommitted,
            ),
        }
    }

    /// Plan 43 C1-2: the class of the answer to an op this thread executed
    /// just now (`execute_local`): that of the row it journaled; an op that
    /// journaled nothing was answered from what the node had journaled
    /// before ([`Self::journaled_answer_class`]).
    pub(crate) fn executed_answer_class(
        &self,
        replica: &dyn Replica,
        rid: Rid,
    ) -> Result<AckClass, constellation_meta::MetaError> {
        Ok(
            match constellation_meta::take_last_journaled().map(|r| r.class) {
                Some(constellation_meta::RowClass::DelegGroup) => self.root_ack_class(),
                Some(class) => match class {
                    constellation_meta::RowClass::Lone => AckClass::Lone,
                    constellation_meta::RowClass::Archive => AckClass::Archive,
                    constellation_meta::RowClass::RootGroup => AckClass::RootGroup,
                    _ => AckClass::NonStealable,
                },
                None => return self.journaled_answer_class(replica, rid),
            },
        )
    }

    /// Plan 43 C2a: the source to name for an outcome this node reads off
    /// its own `completed` row (a dedup, a retry that finds the rid done):
    /// the archive when an applied segment carries the row, otherwise the
    /// class its journal row waits for as the root's.
    /// `Err`: as [`Self::journaled_answer_class`]'s.
    pub(crate) fn completed_source(
        &self,
        replica: &dyn Replica,
        rid: Rid,
    ) -> Result<AnswerSource, constellation_meta::MetaError> {
        Ok(AnswerSource::Sequenced(if replica.outcome_from_log(rid) {
            AckClass::Archive
        } else {
            self.journaled_answer_class(replica, rid)?
        }))
    }

    /// Send a message to a peer; a `MutateReply` carrying a definitive
    /// outcome is counted under `class` (`Stats::ack_classes`).
    pub(crate) fn send_answer(
        &mut self,
        to: NodeId,
        msg: PeerMsg,
        class: AckClass,
        out: &mut Vec<Action>,
    ) {
        self.send_answer_committed(to, msg, class, class.gated(), out)
    }

    /// [`Self::send_answer`], with whether the answer says it is committed
    /// given: a gated answer released with the gate off says it is not.
    pub(crate) fn send_answer_committed(
        &mut self,
        to: NodeId,
        mut msg: PeerMsg,
        class: AckClass,
        says_committed: bool,
        out: &mut Vec<Action>,
    ) {
        if let PeerMsg::MutateReply {
            outcome, committed, ..
        } = &mut msg
        {
            if is_answer(outcome) {
                self.stats.ack_classes[class.index()] += 1;
                // Plan 43 C1-2: a gated answer is a commit; the requester's
                // shadow of it is not speculation (C1-4).
                *committed = says_committed;
            }
        }
        out.push(Action::Send { to, msg });
    }

    /// `ship_landed`: journal rows `seqs` are in the log, shipped through
    /// `through`.
    pub(crate) fn note_shipped(&mut self, seqs: &[u64], through: u64) {
        if through > self.ack.shipped_through {
            self.ack.shipped_through = through;
        }
        for s in seqs {
            if *s > self.ack.shipped_through {
                self.ack.shipped_rows.insert(*s);
            }
        }
        let floor = self.ack.shipped_through;
        self.ack.shipped_rows.retain(|s| *s > floor);
    }

    /// Tell the replica's session gate where durability stands (the
    /// holder's own reads wait for it), when it changed.
    fn report_durable(&mut self, replica: &dyn Replica) {
        let gate = self.lease.held.is_some() && self.durable_jseq() != u64::MAX;
        let jseq = if gate { self.durable_jseq() } else { u64::MAX };
        let lost = self.lease.lost;
        // Plan 43 C1-2: the class of the answers this node gives its own
        // clients, published with the gate: the FUSE fast path journals
        // its rows with it and waits by it.
        let class: constellation_meta::RowClass = self.root_ack_class().into();
        if self.ack.reported != Some((gate, jseq, lost, class)) {
            self.ack.reported = Some((gate, jseq, lost, class));
            replica.set_durable(gate, jseq, lost, class);
        }
    }

    /// A successful S3 result proves liveness as of the request's send
    /// time — unless it revealed a deposition (a foreign lease object).
    pub(crate) fn note_s3_liveness(&mut self, now: Now, op: OpId, result: &S3Result) {
        let Some(sent) = self.ack.s3_sent.remove(&op) else {
            return;
        };
        let rtt = now.honour.since(sent).max(0) as u64;
        self.ack.s3_rtt_ms = if self.ack.s3_rtt_ms == 0 {
            rtt
        } else {
            (self.ack.s3_rtt_ms * 3 + rtt) / 4
        };
        let fresh = match result {
            S3Result::SegmentRun(Ok(_))
            | S3Result::SegmentPut(Ok(()))
            | S3Result::LeasePut(Ok(_)) => true,
            S3Result::LeaseGet(Ok(Some((lease, _)))) => {
                lease.holder == self.cfg.node_id && Some(lease.epoch) == self.lease.epoch()
            }
            _ => false,
        };
        if fresh && sent > self.ack.last_s3_fresh {
            self.ack.last_s3_fresh = sent;
        }
    }

    /// Whether this holder may answer a strict read now (see the module
    /// doc): under `Local` always (the lease cap is the argument); else
    /// only with fresh S3 liveness ([`Core::renew_fresh`]). `false`
    /// starts a probe.
    pub(crate) fn strict_answer_allowed(&mut self, now: Now, out: &mut Vec<Action>) -> bool {
        if self.renew_fresh(now.honour) {
            return true;
        }
        self.stats.stale_liveness_refusals += 1;
        self.ship.probe_now = true;
        self.nudge(now, out);
        false
    }

    /// The liveness rule of the module doc, side-effect free: a tenure
    /// that cannot be taken over before its lease expires (`Local`, no
    /// backup and none listed by a reconfiguration in doubt or out, or a
    /// continuation epoch's hold) is always fresh; any other
    /// only within `backup_takeover_ms` of the send of an S3 request that
    /// succeeded without revealing a deposition (`last_s3_fresh`). What a
    /// strict read, a new lock grant and (plan 41 L0) a renewal of a lock
    /// grant or a delegation by the root all need. `now` may be the time
    /// an answer was *decided*: a liveness proof sent after it covers it
    /// too (the successor's marker came after the request that proved
    /// the tenure, so after the decision).
    pub(crate) fn renew_fresh(&self, now: HonourMs) -> bool {
        liveness_fresh(
            self.may_be_taken_over_early(),
            now,
            self.ack.last_s3_fresh,
            self.cfg.backup_takeover_ms as i64,
        )
    }

    /// The lease says this tenure grants (`Lease::granted_delegations`),
    /// or nobody can take it over before it expires: its successor waits
    /// out the grants and renewals it answered (`note_marker_landed`'s
    /// floor applies only to a marked tenure).
    ///
    /// Plan 43 C7-F F-a0 L3: not while a reconfiguration CAS is out that
    /// makes the tenure sealable and does not mark it (its duty, if any,
    /// was not live when it was issued): what the tenure answers now
    /// (a reclaim under a grace that began since, a delegate's renewal)
    /// would stand in a lease a sealer may replace as it lands. Held
    /// until it settles; then the tenure is fast and marks itself
    /// (`renew_kick`), or is `Local` still. Nor while such a CAS's outcome
    /// is not known (`reconfig_in_doubt`).
    ///
    /// Plan 43 C7-F F-a0 (b): nor while the mark is being cleared
    /// (`Core::maybe_clear_inherited_mark`), or its clearing is not known
    /// to have failed: a lease that is unmarked in S3 and marked here would
    /// let an answer stand in a version nobody waits out.
    pub(crate) fn tenure_marked(&self) -> bool {
        if self.ack.unmarking.is_some() || self.ack.unmark_unsettled {
            return false;
        }
        if self
            .ack
            .reconfig
            .as_ref()
            .is_some_and(|r| lease_fast(&r.lease) && !r.lease.granted_delegations)
            || self.reconfig_in_doubt()
        {
            return false;
        }
        !self.fast_tenure()
            || self
                .lease
                .held
                .as_ref()
                .is_some_and(|(l, _)| l.granted_delegations)
    }

    /// Plan 43 C7-F F-a0 L3: a reconfiguration CAS that would have made
    /// the tenure sealable unmarked may have landed (its outcome is not
    /// known) and no lease version has been adopted from S3 since
    /// (`LeaseState::reconfig_doubt`): to a sealer the tenure may be
    /// sealable and unmarked while this node still holds it `Local`.
    pub(crate) fn reconfig_in_doubt(&self) -> bool {
        self.lease.held.is_some()
            && self
                .lease
                .reconfig_doubt
                .as_ref()
                .is_some_and(|d| d.unmarked_sealable)
    }

    /// Plan 43 C7-F F-a0 (the ack side of `reconfig_in_doubt`): the nodes
    /// a reconfiguration CAS whose outcome is unknown may have listed in
    /// S3 as this tenure's backups (`LeaseState::reconfig_doubt`). Any of
    /// them may seal this tenure and take over with its own tail.
    ///
    /// A reconfiguration still out that lists the first backup counts
    /// too: it may have landed already, and its answer may take up to the
    /// S3 client's retry budget (30 s). The candidate it lists may be
    /// dropped meanwhile (cut off: `backup_timeouts`), and with nobody
    /// else in budget the held `Local` version's rule would acknowledge on
    /// this disk alone while S3 may list that candidate. (A
    /// reconfiguration out from a version that lists backups already gates
    /// on those: a second one's add window is C6's.)
    fn doubted_backups(&self) -> impl Iterator<Item = NodeId> + '_ {
        let none: &[NodeId] = &[];
        let (doubt, out) = match &self.lease.held {
            None => (none, none),
            Some((held, tag)) => (
                self.lease
                    .reconfig_doubt
                    .as_ref()
                    .map_or(none, |d| d.backups.as_slice()),
                match &self.ack.reconfig {
                    Some(r) if held.backups.is_empty() && r.tag == *tag => {
                        r.lease.backups.as_slice()
                    }
                    _ => none,
                },
            ),
        };
        doubt.iter().chain(out).copied()
    }

    /// Plan 41 L0: an owner extending a window it promised (a lock grant
    /// or a delegation, as the root) may answer now: fresh liveness and
    /// a marked tenure, as a new grant needs (`lock_serve`). `Some`: the
    /// answer is held ([`Core::renew_verdict`]) under this tag.
    pub(crate) fn renew_hold(&self, now: Now) -> Option<FreshHold> {
        if self.renew_fresh(now.honour) && self.tenure_marked() {
            return None;
        }
        Some(FreshHold {
            at: now.honour,
            epoch: self.lease.epoch().unwrap_or(0),
        })
    }

    /// What becomes of an answer held under `h`: `Ready` once the tenure
    /// it was decided in is proven live since (or cannot be taken over
    /// any more) and is marked; `Refuse` once that tenure is gone, the
    /// lease read that should have proven it failed, or it waited a lock
    /// TTL (no holder still counts on it); `Wait` otherwise.
    pub(crate) fn renew_verdict(&self, now: Now, h: FreshHold) -> FreshVerdict {
        if self.lease.lost || self.lease.epoch() != Some(h.epoch) {
            return FreshVerdict::Refuse;
        }
        if self.renew_fresh(h.at) && self.tenure_marked() {
            return FreshVerdict::Ready;
        }
        if h.at <= self.ack.renew_refuse_through
            || now.honour.since(h.at) >= self.cfg.lock_ttl_ms as i64
        {
            return FreshVerdict::Refuse;
        }
        FreshVerdict::Wait
    }

    /// Answers wait under `holds`: read the lease (one read at a time)
    /// for those whose liveness is stale, and mark the tenure if it is
    /// not. Nothing for a tenure that cannot be taken over early.
    pub(crate) fn renew_kick(&mut self, holds: &[FreshHold], out: &mut Vec<Action>) {
        // (A reconfiguration in doubt may have made the tenure fast: the
        // probe proves its liveness as a fast one's; unmarked, the mark CAS
        // settles it, landing on the version held or conflicting with the
        // one that replaced it.)
        if !self.may_be_taken_over_early() || self.lease.lost {
            return;
        }
        if self.ack.renew_probe.is_none() && holds.iter().any(|h| !self.renew_fresh(h.at)) {
            let sent = self.clk.honour;
            let op = self.issue_s3(S3Op::LeaseGet, S3For::RenewProbe, out);
            self.ack.renew_probe = Some((op, sent));
            self.stats.renew_probes += 1;
        }
        if !self.tenure_marked() {
            self.ensure_granting_marked(out);
        }
    }

    /// The lease read [`Core::renew_kick`] asked for: another holder (or
    /// none) is a deposition, found here rather than at the next lease
    /// CAS; a failed read refuses what it should have proven. Its
    /// liveness, if any, was noted already (`note_s3_liveness`); the held
    /// answers go (or are refused) after the event (`renew_after_event`).
    pub(crate) fn on_renew_probe(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((_, sent)) = self.ack.renew_probe.take() else {
            return;
        };
        let mine = self
            .lease
            .held
            .as_ref()
            .filter(|_| !self.lease.lost && !self.lease.epoch_held())
            .map(|(l, _)| l.epoch);
        match result {
            S3Result::LeaseGet(Ok(Some((cur, _)))) => {
                // (An epoch below this tenure's is a read older than the
                // acquisition that made it: it proves nothing either way.)
                if let Some(mine) = mine.filter(|m| cur.epoch >= *m) {
                    if cur.holder != self.cfg.node_id || cur.epoch != mine {
                        self.deposed(now, cur.holder, cur.epoch, mine, replica, out);
                    }
                }
            }
            S3Result::LeaseGet(Ok(None)) => {
                if let Some(mine) = mine {
                    self.deposed(now, 0, 0, mine, replica, out);
                }
            }
            _ => {
                self.ack.renew_refuse_through = self.ack.renew_refuse_through.max(sent);
            }
        }
    }

    /// Plan 30 §M9: a tenure that serves strict reads says so in the
    /// lease first (one CAS per tenure). `true` when it already does.
    ///
    /// Plan 43 C7-F F-a0 (b): a mark made for what the tenure answers
    /// itself is its own for good (`mark_own`): never cleared.
    pub(crate) fn ensure_granting_marked(&mut self, out: &mut Vec<Action>) -> bool {
        self.ack.mark_own = true;
        self.ensure_marked(out)
    }

    /// Plan 43 C7-F F-a0 L3: the mark for a duty the tenure stands on
    /// (inherited from a predecessor, left by its own earlier tenure, or
    /// admitted into a `Local` one), not for something it grants itself.
    pub(crate) fn ensure_marked_inherited(&mut self, out: &mut Vec<Action>) -> bool {
        self.ack.mark_inherited = true;
        self.ensure_marked(out)
    }

    fn ensure_marked(&mut self, out: &mut Vec<Action>) -> bool {
        let Some((lease, tag)) = self.lease.held.clone() else {
            return false;
        };
        // The mark is being cleared (or the clear's outcome is not known):
        // the in-memory mark says nothing about the object, and nothing is
        // granted or answered until the CAS is settled.
        if self.ack.unmarking.is_some() || self.ack.unmark_unsettled {
            return false;
        }
        if lease.granted_delegations {
            return true;
        }
        // A reconfiguration CAS out replaces the version this mark would
        // replace (the lease CAS gate below is the job slot's only): the
        // mark follows once it settles (`renew_after_event`, the next
        // grant). The same while the mark is being cleared (a mark CAS
        // before its outcome is known could undo it, or be undone).
        if self.ack.reconfig.is_none()
            && self.ack.marking.is_none()
            && self.ack.unmarking.is_none()
            && !self.ack.unmark_unsettled
            && !self.lease_cas_in_flight()
        {
            let op = self.issue_s3(
                S3Op::LeaseSwap {
                    lease: lease.with_granted_delegations(),
                    tag,
                },
                S3For::MarkGranting,
                out,
            );
            self.ack.marking = Some(op);
        }
        false
    }

    pub(crate) fn on_mark_granting_put(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.ack.marking = None;
        match result {
            S3Result::LeasePut(Ok(tag)) => {
                if let Some((lease, _)) = self.lease.held.clone() {
                    self.lease
                        .renewed(now, lease.with_granted_delegations(), tag);
                }
            }
            S3Result::LeasePut(Err(crate::event::CasFailure::Conflict)) => {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::MarkGrantingReread, out);
                self.ack.marking = Some(op);
            }
            _ => {}
        }
        let _ = replica;
    }

    pub(crate) fn on_mark_granting_reread(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.ack.marking = None;
        let Some((mine, _)) = self.lease.held.clone() else {
            return;
        };
        match result {
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
            {
                self.lease.renewed(now, cur, tag);
                self.reconcile_peers_with_lease();
                // Retried by the next strict read.
            }
            S3Result::LeaseGet(Ok(Some((cur, _)))) => {
                self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out);
            }
            S3Result::LeaseGet(Ok(None)) => {
                self.deposed(now, 0, 0, mine.epoch, replica, out);
            }
            _ => {}
        }
    }

    // ------------------------------------------------ clearing a mark

    /// Plan 43 C7-F F-a0 (b): a mark this tenure inherited (not one it made
    /// for its own grants and answers: `mark_own`) is cleared by CAS once
    /// nothing it stands on is live, so a seal of the tenure costs nothing
    /// even if it never granted, and a chain of seals stops paying a floor
    /// each. Only when all of it holds: `strict_mounts` marks for good; the
    /// held lease is marked, is not being released, lost or gated and is no
    /// continuation epoch's hold; no floor is pending or running and no
    /// duty is live ([`Core::tenure_duty_live`]: no grant record at all,
    /// live or expired, no grace or quarantine, no read grant); no
    /// delegation is out (its generations' grants and the read grants a
    /// delegate gave); no answer is held; and no lease CAS or lease read of
    /// the tenure's is out.
    ///
    /// Safe because nothing is live at the decision and nothing is answered
    /// while the CAS is out (`tenure_marked`, `ensure_marked`: renewals,
    /// reclaims, grants and strict reads wait). Once it lands the tenure is
    /// a fresh unmarked fast tenure, which any grant or renewal first marks
    /// again (`ensure_granting_marked`, `renew_kick`) — the state a taker of
    /// an unmarked released lease already starts from. A seal racing the
    /// clear only over-waits (it replaces the marked version), or replaces
    /// the unmarked one and finds nothing live. A clear that fails or whose
    /// outcome is not known is settled by a lease read before the tenure
    /// answers anything (`unmark_unsettled`): the in-memory mark is never
    /// true while S3 may hold the unmarked version.
    pub(crate) fn maybe_clear_inherited_mark(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((lease, tag)) = self.lease.held.clone().filter(|_| !self.lease.lost) else {
            self.ack.unmarking = None;
            self.ack.unmark_unsettled = false;
            return;
        };
        if self.ack.unmark_unsettled {
            // The lease is read again, whatever else is in flight: nothing
            // is answered until it says what the mark is.
            if self.ack.unmarking.is_none()
                && now.wait.since(self.ack.last_clear_try) >= CLEAR_RETRY_MS
            {
                self.ack.last_clear_try = now.wait;
                let op = self.issue_s3(S3Op::LeaseGet, S3For::ClearMarkReread, out);
                self.ack.unmarking = Some(op);
            }
            return;
        }
        if !self.ack.mark_inherited
            || self.ack.mark_own
            || self.ack.unmarking.is_some()
            || !lease.granted_delegations
            || self.cfg.strict_mounts
            || now.wait.since(self.ack.last_clear_try) < CLEAR_RETRY_MS
        {
            return;
        }
        if self.lease.releasing
            || self.lease.epoch_held()
            || self.lease.gate.is_some()
            || self.ack.pending_floor
            || self.ack.floor_until > now.wait
            || self.ack.marking.is_some()
            || self.ack.reconfig.is_some()
            || self.ack.renew_probe.is_some()
            || self.lock_answers_held()
            || !self.ack.renew_held_deleg.is_empty()
            || self.lease_cas_in_flight()
            || self.tenure_duty_live(now, replica)
            || !replica.delegation_table().is_empty()
        {
            return;
        }
        self.ack.last_clear_try = now.wait;
        tracing::info!(
            node = self.cfg.node_id,
            epoch = lease.epoch,
            "nothing the inherited mark stands on is live any more: clearing it"
        );
        let op = self.issue_s3(
            S3Op::LeaseSwap {
                lease: lease.without_granted_delegations(),
                tag,
            },
            S3For::ClearMark,
            out,
        );
        self.ack.unmarking = Some(op);
        self.stats.mark_clears += 1;
    }

    pub(crate) fn on_clear_mark_put(
        &mut self,
        now: Now,
        op: OpId,
        result: S3Result,
        out: &mut Vec<Action>,
    ) {
        if self.ack.unmarking != Some(op) {
            return;
        }
        self.ack.unmarking = None;
        match result {
            S3Result::LeasePut(Ok(tag)) => {
                if let Some((lease, _)) = self.lease.held.clone() {
                    self.lease
                        .renewed(now, lease.without_granted_delegations(), tag);
                }
                self.ack.mark_inherited = false;
            }
            // A conflict, a failure, an answer lost: the object may be
            // either version. The lease is read before anything else.
            _ => {
                self.ack.unmark_unsettled = true;
                let op = self.issue_s3(S3Op::LeaseGet, S3For::ClearMarkReread, out);
                self.ack.unmarking = Some(op);
            }
        }
    }

    pub(crate) fn on_clear_mark_reread(
        &mut self,
        now: Now,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.ack.unmarking != Some(op) {
            return;
        }
        self.ack.unmarking = None;
        let Some((mine, _)) = self.lease.held.clone() else {
            self.ack.unmark_unsettled = false;
            return;
        };
        match result {
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
            {
                // Whichever mark it has: the clear landed or it did not.
                if !cur.granted_delegations {
                    self.ack.mark_inherited = false;
                }
                self.lease.renewed(now, cur, tag);
                self.ack.unmark_unsettled = false;
                self.reconcile_peers_with_lease();
            }
            S3Result::LeaseGet(Ok(Some((cur, _)))) => {
                self.ack.unmark_unsettled = false;
                self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out);
            }
            S3Result::LeaseGet(Ok(None)) => {
                self.ack.unmark_unsettled = false;
                self.deposed(now, 0, 0, mine.epoch, replica, out);
            }
            // Still unknown: read again at the next housekeeping.
            _ => {}
        }
    }

    // --------------------------------------------------------- the floor

    /// `acquire_won`: a seal or an `ack=s3` fast takeover (the lease had
    /// not expired, or this node's clock cannot tell). Once the marker
    /// lands, the successor's acknowledgements wait out the predecessor's
    /// strict-read horizon — unless that tenure never served one. The
    /// predecessor's lease expiry plays no part in the wait on its own
    /// (`note_marker_landed`); `cap` is the end of that lease as the
    /// predecessor's own heartbeat places it on this node's clock
    /// ([`Core::beat_cap`]), which ends the wait earlier when it is.
    pub(crate) fn note_fast_takeover(&mut self, prev: &Lease, cap: Option<WaitMs>) {
        // Plan 43 C7-F F-c: the lease body replaced, for the old root's
        // final lock mirror to be recognised (`lock_install_mirror`).
        self.lk.takeover_prev = Some((
            prev.holder,
            prev.holder_clock,
            prev.holder_wait_ms,
            prev.epoch,
        ));
        // Plan 41 L0: a predecessor's generations are outwaited the
        // takeover window longer (`DelegationState::inherit_slack_ms`).
        self.dl.inherit_slack_ms = self.cfg.backup_takeover_ms;
        self.ack.pending_floor = prev.granted_delegations;
        self.ack.pending_cap = cap.filter(|_| prev.granted_delegations);
    }

    /// Plan 43 C7a (commit 4): this node re-adopted its own lease at the
    /// epoch of a fast claim whose floor was not durable when it stopped
    /// (`AckState::floor_mark`): it owes the predecessor's floor still. Armed
    /// as `note_fast_takeover` arms it, but counted from this tenure's marker
    /// (later than the claim's, so never short of it) and with no beat cap
    /// (the predecessor's beats are not known any more).
    pub(crate) fn note_floor_restart(&mut self) {
        self.dl.inherit_slack_ms = self.cfg.backup_takeover_ms;
        self.ack.pending_floor = true;
        self.ack.pending_cap = None;
    }

    /// At start: the previous incarnation's persisted takeover floor is this
    /// one's (a restart inside the floor is inside it still: no release, no
    /// handoff, no flush before it ends; the horizons it set load as the
    /// quarantines of `read_start` and `locks_start`), and the claim mark a
    /// fast takeover left is read for `acquire_won`.
    pub(crate) fn floor_start(&mut self, now: Now, replica: &dyn Replica) {
        self.ack.floor_mark = replica.takeover_floor_pending();
        if let Some(until) = replica.load_takeover_floor(now, self.cfg.floor_horizon_span_ms()) {
            tracing::info!(
                node = self.cfg.node_id,
                wait_ms = until.since(now.wait),
                "a takeover floor of the previous incarnation is still running; \
                 the lease is not released before it ends"
            );
            self.ack.floor_until = self.ack.floor_until.max(until);
            self.ack.floor_persisted = true;
        }
    }

    /// The floor armed at `now` ends at `floor_end`; its read and lock
    /// parts at `until` and `locks_until` (all on the wait clock): persist
    /// them as the horizons a restart waits out (the read and lock keys, as
    /// every grant's horizon is) and as the floor itself (the third key), so
    /// that a successor that stops inside the floor does not come back
    /// without it. The claim mark of `acquire_cas` goes once all three are
    /// durable; one that is not (a write failed; the lock horizon is
    /// written off the core) leaves it, and a restart then arms the floor
    /// again.
    fn persist_floor(
        &mut self,
        now: Now,
        (until, locks_until, floor_end): (WaitMs, WaitMs, WaitMs),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let mut durable = true;
        // Whatever `read_delegations` says: `note_marker_landed` quarantines
        // the acknowledgements until `until` in any case, and a restart
        // must wait it out as the process that armed it would have.
        if until > now.wait {
            durable &= replica.note_grant_horizon(until, now);
        }
        if replica.note_takeover_floor(floor_end, now) {
            self.ack.floor_persisted = true;
        } else {
            durable = false;
        }
        let locks_need = if self.cfg.locks && locks_until > now.wait {
            self.lock_persist_horizon(locks_until, out);
            locks_until
        } else {
            WaitMs(i64::MIN)
        };
        if durable {
            self.ack.floor_mark_locks = Some(locks_need);
            self.floor_mark_covered(replica);
        }
    }

    /// The claim mark goes once everything it stands for is durable: the
    /// floor armed and written (`persist_floor`), and the lock horizon it
    /// needs. Called for every completed lock-horizon write, which before
    /// that (the claim's CAS or marker out, a write of the floor failed)
    /// covers nothing of the floor: a grant's of this node as a delegate's
    /// lock owner, or of the previous tenure.
    pub(crate) fn floor_mark_covered(&mut self, replica: &dyn Replica) {
        let Some(locks_until) = self.ack.floor_mark_locks else {
            return;
        };
        if self.lock_horizon_durable() < locks_until {
            return;
        }
        self.ack.floor_mark_locks = None;
        if self.ack.floor_mark.take().is_some() {
            replica.clear_takeover_floor_pending();
        }
    }

    /// How far before `X` (the end a beat cap counts from) a beat may have
    /// been sent to be used for the cap, ms (plan 43 C7a): `M·1e6/(4ρ)` for
    /// the node's rate budget `ρ` in ppm, so that the error `2ρ·(X − sent)`
    /// of a beat sent before the renewal that set `X` stays under `M/2`
    /// (the module doc, step 3). 250·M at the default budget; no limit when
    /// the budget is 0.
    pub(crate) fn beat_cap_span_ms(&self) -> i64 {
        let rho = self.cfg.clock_rate_budget_ppm as i64;
        if rho == 0 {
            return i64::MAX;
        }
        let margin = self.cfg.expiry_margin_ms as i128;
        (margin * 1_000_000 / (4 * rho as i128)).min(i64::MAX as i128) as i64
    }

    /// Plan 43 C7-F F-a: where the lease of `prev`'s tenure ends on this
    /// node's wait clock, as that tenure's beats at `prev.epoch` placed it,
    /// plus one expiry margin (see the module doc, "The delegation horizon
    /// across a fast failover"): `X + min(at − sent) + M`, with `X` the
    /// latest of the replaced lease's end (`Lease::holder_wait_ms`), the
    /// ends the beats named and the guards they named, all on the holder's
    /// **wait** clock, over the beats sent within [`Core::beat_cap_span_ms`]
    /// of `X` (so the rate error `2ρ·(X − sent)` of a beat sent before the
    /// renewal that set `X` stays under `M/2`). `None`: no beat of
    /// that tenure heard, none that recent, or one named an unknown guard
    /// (the holder's own takeover gate was pending: what it waited out was
    /// not known yet).
    ///
    /// `at` is the arrival as this node stamped it. The seal watch counts
    /// the same plain arrival (never a beat's *credit*, `min(at, sent +
    /// M)`, an instant of the holder's clock: a holder whose clock is
    /// behind this node's by more than M would end the floor that much
    /// early, the sim's `fast-takeover-skewed-successor`).
    pub(crate) fn beat_cap(&self, prev: &Lease) -> Option<WaitMs> {
        let b = self
            .bk
            .beats
            .get(&prev.holder)
            .filter(|b| b.epoch == prev.epoch)?;
        // No wait-clock end of that tenure's holder (a body that predates
        // its clock id, or a fenced one), or no beat of the clock that wrote
        // it: `prev.holder_wait_ms` cannot be placed on any clock heard.
        if b.guard_unknown || prev.holder_clock == 0 || prev.holder_wait_ms == 0 {
            return None;
        }
        let margin = self.cfg.expiry_margin_ms as i64;
        let span = self.beat_cap_span_ms();
        let mut cap: Option<i64> = None;
        let mut named_clock_placed = false;
        for (clock, agg) in &b.clocks {
            let own = *clock == prev.holder_clock;
            let x = agg
                .named_max
                .max(agg.guard_max)
                .max(if own { prev.holder_wait_ms } else { 0 });
            let lag = agg
                .lags
                .iter()
                // A beat sent after `x` cannot be (they stop at the lease's
                // end): not evidence; left out, which only lengthens the
                // floor.
                .filter(|(sent, _)| (0..=span).contains(&x.saturating_sub(*sent)))
                .map(|(_, lag)| *lag)
                .min();
            match lag {
                Some(lag) => {
                    named_clock_placed |= own;
                    let end = x.saturating_add(lag);
                    cap = Some(cap.map_or(end, |c| c.max(end)));
                }
                // The clock that wrote the lease must be placed; an earlier
                // incarnation's terms only add to the cap (the maximum).
                None if own => return None,
                None => {}
            }
        }
        if !named_clock_placed {
            return None;
        }
        Some(WaitMs(cap?.saturating_add(margin)))
    }

    /// Plan 43 C7-F F-a: what this node, as the holder, waits out that its
    /// lease does not cap, on its own **wait** clock (where it waits it
    /// out; plan 43 C7-H1, R7: the beat names it as it is, never mapped onto
    /// another clock), for its beats to name
    /// ([`constellation_net::Beat::guard_ms`]; 0: none): its takeover
    /// floor (it can outlast the lease: the lock floor of a 20 s lease),
    /// its read and lock quarantines (a restart's, a released takeover's),
    /// its subtree graces and the grants it made under a continuation
    /// epoch's hold (`hold_grant_until`). `i64::MAX` while its takeover gate or a
    /// floor still to be armed is pending: the marker may be ahead of it,
    /// and its own predecessor's proofs extend until then, so the floor
    /// is not known yet and a successor must not cap its own.
    pub fn beat_guard_wait_ms(&self, replica: &dyn Replica) -> i64 {
        let (base, protecting) = self.beat_guard_parts(replica);
        base.max(protecting)
    }

    /// The guard in two parts, `(base, protecting)`: the base (its own
    /// floor, the read and lock quarantines; `i64::MAX` while the gate or a
    /// floor due is pending) is set when the tenure takes over and is
    /// meant not to rise once published; `protecting` (the subtree graces
    /// and the grants made under an epoch hold) may rise later, by what
    /// protects a lease-capped right: a delegate outwaited after the beat
    /// last named the guard granted under its own `until`, which the root's
    /// lease caps. Both are readings of this node's wait clock, which no
    /// wall step moves, so the base does not rise with a step either; the
    /// simulator asserts it never rises.
    pub fn beat_guard_parts(&self, replica: &dyn Replica) -> (i64, i64) {
        if self.lease.gate.is_some() || self.ack.pending_floor {
            return (i64::MAX, 0);
        }
        let base = self
            .ack
            .floor_until
            .max(replica.read_delegations().quarantine_until())
            .max(replica.locks().quarantine_until())
            .0;
        let protecting = self
            .ack
            .hold_grant_until
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(self.lock_grace_until().0);
        (base, protecting)
    }

    /// This tenure is inside its takeover floor (`note_marker_landed`).
    /// The floor stands for grants of the predecessor's tenure that only
    /// this node's tables know to wait for, and the node that takes a
    /// released lease over starts without it (`fast_prev` is false for a
    /// released lease, and `lock_on_released_takeover` covers this node's
    /// own grants only): the lease is not released to another node
    /// meanwhile, by idle release, by a peer's handoff request or by a
    /// continuation epoch's flush. (An operator's flush or stop waits for
    /// it too, parked: `service_pending_flushes`; one that gives up on a
    /// stuck gate releases nothing and leaves the lease to lapse.)
    pub(crate) fn in_takeover_floor(&self, now: Now) -> bool {
        now.wait < self.ack.floor_until
    }

    /// Where this tenure's takeover floor ends, on this node's wait clock
    /// (0: none): the engine's upgrade handover waits for it before it
    /// detaches the views, which keep serving meanwhile.
    pub fn takeover_floor_until(&self) -> WaitMs {
        self.ack.floor_until
    }

    /// A fast takeover's gate is pending (its marker, tail or quarantine
    /// not done): a release now would hand the lease on without a floor.
    pub fn fast_gate_pending(&self) -> bool {
        self.lease.gate.as_ref().is_some_and(|g| g.fast_prev)
    }

    /// The tenure ended (`lock_on_lease_gone`): a floor not yet armed is
    /// not the next tenure's, and the guard on releasing ends with it (the
    /// tables' quarantines run on).
    pub(crate) fn end_takeover_floor(&mut self) {
        self.ack.pending_floor = false;
        self.ack.pending_cap = None;
        self.ack.floor_until = WaitMs(0);
        // The marks belong to the tenure that ended.
        self.ack.mark_inherited = false;
        self.ack.mark_own = false;
        self.ack.unmarking = None;
        self.ack.unmark_unsettled = false;
    }

    /// The takeover's marker landed at `now`.
    pub(crate) fn note_marker_landed(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // The predecessor's expiry is a stamp of its own clock: compared
        // with this node's it is offset-dependent (a successor ahead by
        // more than the remaining lease would read it as past and wait
        // for nothing), so the floor is the bound, which counts only this
        // node's own elapsed time — or, if earlier, the cap its own beats
        // put on its lease (`beat_cap`: a difference on the holder's clock
        // added to the beat's arrival stamp on this node's).
        if !std::mem::take(&mut self.ack.pending_floor) {
            return;
        }
        let cap = self.ack.pending_cap.take();
        // Plan 30 §M11 phase 2b: a tenure with live write delegations
        // has grants (and, under them, read grants a delegate gave)
        // that its successor cannot see renewed; one constant covers
        // them all.
        let deleg = if replica.delegation_table().is_empty() {
            0
        } else {
            self.reclaim_horizon_ms()
        };
        // Plan 43 C7-H1: the floor is a wait on this node's wait clock (a
        // forward wall step must not end it early); the cap is on it too.
        let bound = |ttl: u64| {
            now.wait
                .plus(self.cfg.backup_takeover_ms + ttl + deleg + 2 * self.cfg.expiry_margin_ms)
        };
        let capped = |bound: WaitMs| cap.map_or(bound, |c| bound.min(c));
        // Plan 30 §M14: lock grants are capped like read delegations and
        // marked the same way, but only new lock grants wait for them —
        // a lock grant still honoured elsewhere does not make an
        // acknowledgement stale — so their (longer) ttl is a quarantine
        // of the lock table's own.
        let locks_bound = bound(self.cfg.read_delegation_ttl_ms.max(self.cfg.lock_ttl_ms));
        let locks_until = capped(locks_bound);
        let bound_until = bound(self.cfg.read_delegation_ttl_ms);
        let until = capped(bound_until);
        let floor_bound = if self.cfg.locks {
            locks_bound.max(bound_until)
        } else {
            bound_until
        };
        let floor_end = if self.cfg.locks {
            locks_until.max(until)
        } else {
            until
        };
        if floor_end <= now.wait {
            // The predecessor's lease, as its beats placed it, ended
            // before this marker landed: everything it granted is
            // lapsed, and no floor remains.
            self.ack.floor_until = WaitMs(0);
            self.stats.ack_floor_capped_out += 1;
            // Nothing to persist and nothing owed: the claim's mark goes.
            self.ack.floor_mark_locks = None;
            if self.ack.floor_mark.take().is_some() {
                replica.clear_takeover_floor_pending();
            }
            tracing::info!(
                node = self.cfg.node_id,
                bound_ms = floor_bound.since(now.wait),
                cap_ms = floor_end.since(now.wait),
                "took a lease over by seal or ack=s3 from a tenure that served strict \
                 reads or locks; its lease, as its beats placed it, has ended: no floor"
            );
            return;
        }
        if self.cfg.locks && locks_until > now.wait {
            replica.locks().set_quarantine(locks_until);
        }
        self.ack.floor_until = floor_end;
        self.stats.ack_floor_waits += 1;
        let capped_by = floor_bound.since(floor_end);
        if capped_by > 0 {
            self.stats.ack_floor_capped += 1;
            self.stats.ack_floor_cap_saved_ms += capped_by as u64;
        }
        tracing::info!(
            node = self.cfg.node_id,
            wait_ms = until.max(now.wait).since(now.wait),
            lock_wait_ms = locks_until.max(now.wait).since(now.wait),
            bound_ms = bound_until.since(now.wait),
            capped = capped_by > 0,
            "took a lease over by seal or ack=s3 from a tenure that served strict \
             reads or locks; acknowledging no mutation until its delegation \
             horizon has passed"
        );
        if until > now.wait {
            replica.read_delegations().set_quarantine(until);
            self.arm_grant_quarantine(until, out);
        }
        self.persist_floor(now, (until, locks_until, floor_end), replica, out);
    }

    // ------------------------------------------------------ holder side

    /// After every event: keep the backup set in step with the lease, the
    /// links and the acknowledgements; stream; reconfigure; and, as a
    /// non-holder, watch an `ack=s3` holder for silence.
    pub(crate) fn backup_after_event(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.stopped {
            return;
        }
        let holding = self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.epoch_held();
        if !holding
            && self.lease.holds_unexpired(now, &self.cfg)
            && self.lease.ack_policy() != AckPolicy::S3
            && self.cfg.p2p
            && !self.ack.peers.is_empty()
        {
            // EC2 follow-up 3a: the lease is held but inside its expiry
            // margin — a renewal still in flight on a slow S3. Nothing new
            // is admitted, but the backups keep hearing from this holder
            // (heartbeats never wait on S3): a silent holder is sealed by
            // its backup after `backup_takeover_ms`, and a live one sealed
            // that way fails its forwarded writes until it reconfigures.
            // Past the lease's own expiry this stops, as before.
            self.backup_stream(now, replica, out);
            self.report_durable(replica);
            self.complete_ready(now, replica, out);
            return;
        }
        if !holding {
            if !self.ack.peers.is_empty() || self.ack.candidate.is_some() {
                self.ack.peers.clear();
                self.ack.candidate = None;
                self.ack.reconfig = None;
                self.ack.reconfig_wanted = None;
            }
            if self.lease.held.is_none() {
                // (Assessed again by the next tenure's acquisition; a
                // held lease whose gate is still closed keeps it.)
                self.ack.eligible = None;
            }
            if self.ack.shipped_through != 0 || !self.ack.shipped_rows.is_empty() {
                self.ack.shipped_through = 0;
                self.ack.shipped_rows.clear();
            }
            if self.epoch_streams_ahead() {
                // A continuation epoch's hold owner: its journal is
                // acknowledged on its disk alone (`durable_jseq`) and
                // streams ahead to the members as it grows.
                if replica.journal_tip() > self.ack.streamed_through {
                    self.stream_ahead_soon(now, replica, out);
                }
            } else {
                self.ack.streamed_through = 0;
                self.ack.ahead_of.clear();
                self.ack.stream_ahead_pending = false;
            }
            self.ack.acked_hwm = 0;
            self.report_durable(replica);
            self.watch_s3_holder(now, replica, out);
            // What this node acknowledged as holder and has since shipped
            // is durable though its lease lapsed (a deposition aborts the
            // rest: `ack_abort_parked`).
            self.complete_ready(now, replica, out);
            return;
        }
        self.reconcile_peers_with_lease();
        let durable = self.durable_jseq();
        if durable != u64::MAX && durable > self.ack.acked_hwm {
            self.ack.acked_hwm = durable;
        }
        if self.lease.ack_policy() != AckPolicy::S3 && self.cfg.p2p {
            // The append goes out on every event (a row journaled, an
            // ack that freed a slot); the bookkeeping runs on the tick
            // and a few times a second in between.
            let housekeeping = self.ack.tick_fired
                || now.wait.since(self.ack.last_housekeeping) >= HOUSEKEEPING_MS;
            if housekeeping {
                self.ack.tick_fired = false;
                self.ack.last_housekeeping = now.wait;
                self.backup_select(now, replica, out);
            }
            self.backup_stream(now, replica, out);
            if housekeeping {
                self.backup_timeouts(now, replica, out);
                self.backup_promote(now, replica, out);
            }
        }
        // Plan 43 C7-F F-a0 (b): an inherited mark nothing stands on any
        // more is cleared (a few flag reads unless the tenure has one).
        self.maybe_clear_inherited_mark(now, replica, out);
        if let Some(backups) = self.ack.reconfig_wanted.take() {
            self.issue_reconfig(now, backups, replica, out);
        }
        if self.ack.reconfig_wanted.is_some() {
            self.arm_backup_tick(now, out);
        }
        self.report_durable(replica);
        self.complete_ready(now, replica, out);
    }

    /// The committed set is the lease's: drop peers it no longer lists
    /// (a restart re-adopted the lease with `backups = []`; a reread
    /// showed a reconfiguration landed).
    fn reconcile_peers_with_lease(&mut self) {
        let committed: Vec<NodeId> = self.lease.backups().to_vec();
        let held_version = self.lease.held.as_ref().map(|(l, _)| l.config_version);
        for (n, p) in self.ack.peers.iter_mut() {
            p.committed = committed.contains(n);
            // Plan 43 C1-1: listed from this held version on. The first
            // time the held lease lists the peer in this bring-up, what it
            // acknowledged before counted for nothing (it was a
            // candidate, or an earlier tenure of this holder's process
            // listed it): counting restarts, from the appends sent at a
            // version that lists it (`on_backup_ack`). The version is the
            // held one when this runs, which is the one that listed it or
            // a later one: never earlier, so never an append that did not
            // list it. An append is due at once (`last_sent`): the backup
            // may be caught up and idle, and the acknowledgements parked
            // behind the listing would wait out a heartbeat interval.
            if !p.committed {
                p.listed_at = None;
                p.counted = 0;
            } else if p.listed_at.is_none() {
                p.listed_at = held_version;
                p.counted = 0;
                p.last_sent = WaitMs(0);
            }
        }
        // A candidate that made it in starts over.
        self.ack
            .candidate_strikes
            .retain(|n, _| !committed.contains(n));
        let candidate = self.ack.candidate;
        self.ack
            .peers
            .retain(|n, p| p.committed || Some(*n) == candidate);
    }

    /// Whether this configuration ever selects a backup (the selection
    /// pass runs at all): P2P on, a budget and a set size.
    fn backups_possible(&self) -> bool {
        self.cfg.p2p && self.cfg.backup_rtt_budget_ms > 0 && self.cfg.backups_max > 0
    }

    /// Eligible peers, best first: in the roster, connected, within the
    /// RTT budget, stable, not us, not already a backup.
    pub(crate) fn backup_candidates(&self, now: Now) -> Vec<NodeId> {
        if self.cfg.backup_rtt_budget_ms == 0 || self.cfg.backups_max == 0 {
            return Vec::new();
        }
        let mut eligible: Vec<(WaitMs, NodeId)> = self
            .links
            .values()
            .filter(|l| l.node != self.cfg.node_id && l.connected)
            .filter(|l| self.roster.contains(&l.node))
            .filter(|l| !self.lease.backups().contains(&l.node))
            .filter(|l| {
                l.rtt_ms
                    .is_some_and(|rtt| rtt <= self.cfg.backup_rtt_budget_ms)
            })
            .filter(|l| {
                l.since
                    .is_some_and(|since| now.wait.since(since) >= self.cfg.backup_stable_ms as i64)
            })
            .map(|l| (l.since.unwrap_or(now.wait), l.node))
            .collect();
        eligible.sort();
        eligible.into_iter().map(|(_, n)| n).collect()
    }

    fn backup_select(&mut self, now: Now, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        // EC2 follow-up (e): a node that sealed the epoch held can never
        // back it: neither a candidate nor a reason to hold
        // acknowledgements back waiting for one.
        let epoch = self.lease.epoch();
        self.ack.refused_epoch.retain(|_, e| Some(*e) == epoch);
        let candidates: Vec<NodeId> = self
            .backup_candidates(now)
            .into_iter()
            .filter(|n| !self.ack.refused_epoch.contains_key(n))
            .collect();
        self.ack.eligible = Some(!candidates.is_empty());
        if self.ack.candidate.is_some()
            || self.ack.reconfig.is_some()
            || self.lease.backups().len() >= self.cfg.backups_max
        {
            return;
        }
        // The rate limit is for churn (the same node in and out); a set
        // that is empty is filled at once — acknowledgements wait for it.
        if !self.lease.backups().is_empty()
            && now.wait.since(self.ack.last_reconfig) < self.cfg.backup_reconfig_min_ms as i64
        {
            return;
        }
        // A candidate that timed out lately is backed off: brought up
        // again, it starts from zero and likely times out the same way
        // (`stress-ng-fs-nodes`: the same loaded node re-selected every
        // ~13 s for minutes, each time re-streaming the journal). Another
        // peer goes first meanwhile.
        let Some(n) = candidates.into_iter().find(|n| {
            self.ack
                .last_dropped
                .get(n)
                .is_none_or(|at| now.wait.since(*at) >= self.cfg.backup_reconfig_min_ms as i64)
                && self
                    .ack
                    .candidate_strikes
                    .get(n)
                    .is_none_or(|(_, until)| now.wait >= *until)
        }) else {
            return;
        };
        tracing::info!(
            node = self.cfg.node_id,
            candidate = n,
            "bringing up a backup"
        );
        self.ack.candidate = Some(n);
        // Unique per holder across its restarts too (time-based), so a
        // dismissal of an earlier bring-up never matches this one.
        let candidacy = (now.wall.0.max(0) as u64).max(self.ack.last_candidacy + 1);
        self.ack.last_candidacy = candidacy;
        self.ack.peers.insert(
            n,
            BackupPeer {
                acked: 0,
                listed_at: None,
                counted: 0,
                sent_through: 0,
                inflight: std::collections::VecDeque::new(),
                last_sent: WaitMs(0),
                last_progress: now.wait,
                committed: false,
                candidacy,
                alive_at: now.wait,
                short_since_progress: false,
                short_since: now.wait,
                unreachable: false,
                answers_beats: false,
            },
        );
    }

    /// Send the next append to every backup with a free pipeline slot:
    /// the rows past what it was last sent, or a heartbeat when it is
    /// caught up and idle. Appends are acknowledged cumulatively by
    /// journal position, so several may be in flight; rows journaled
    /// while every slot is taken ride the next one (the group commit).
    fn backup_stream(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some((lease, _)) = self.lease.held.clone() else {
            return;
        };
        if self.ack.peers.is_empty() {
            // Nobody to stream to (a lone node, `ack=s3`): no journal
            // read per event.
            return;
        }
        let tip = replica.journal_tip();
        let heartbeat = self.cfg.backup_heartbeat_ms as i64;
        let max_inflight = self.cfg.backup_max_inflight.max(1);
        let through = self.ack.shipped_through;
        let nodes: Vec<NodeId> = self
            .ack
            .peers
            .iter()
            .filter(|(_, p)| p.inflight.len() < max_inflight)
            .filter(|(_, p)| {
                p.sent_through.max(p.acked).max(through) < tip
                    || (p.inflight.is_empty() && now.wait.since(p.last_sent) >= heartbeat)
            })
            .map(|(n, _)| *n)
            .collect();
        if nodes.is_empty() {
            self.arm_backup_tick(now, out);
            return;
        }
        for n in nodes {
            let p = &self.ack.peers[&n];
            // Rows through `shipped_through` are in the log (and trimmed
            // from the journal): a backup needs nothing below them, so
            // the batch starts past whichever is highest. Rows missing
            // above that are rolled-back transactions (their heads stay,
            // their rows are gone): skipped by the reader, and nobody
            // needs them.
            let start = p.sent_through.max(p.acked).max(through) + 1;
            let mut txs = if start <= tip {
                replica.journal_txs_from(start, self.cfg.backup_batch_rows)
            } else {
                Vec::new()
            };
            // Round 2: rows gone from the journal between what was
            // streamed and the tip (shipped out of order behind a
            // held-back transaction) go as a hole, so the backup's hold
            // reaches the tip and it counts as caught up (the reader
            // fills the holes below its last transaction itself).
            let streamed: usize = txs.iter().map(|t| t.records.len()).sum();
            let last_live = txs.last().map(|t| t.last).unwrap_or(start - 1);
            if start <= tip && streamed < self.cfg.backup_batch_rows && last_live < tip {
                txs.push(BackupTx {
                    first: last_live + 1,
                    last: tip,
                    records: Vec::new(),
                    origin: (0, 0),
                });
            }
            let last = txs.last().map(|t| t.last);
            let req = self.op_id();
            let p = self.ack.peers.get_mut(&n).expect("present");
            let candidacy = p.candidacy;
            if let Some(last) = last {
                p.sent_through = p.sent_through.max(last);
            }
            p.inflight.push_back(Inflight {
                req,
                sent: now.wait,
                last: last.unwrap_or(start - 1),
                config_version: lease.config_version,
            });
            p.last_sent = now.wait;
            self.stats.backup_appends += 1;
            tracing::trace!(
                target: "constellation_authority::ack_wait",
                node = self.cfg.node_id,
                backup = n,
                from = start,
                tip,
                txs = txs.len(),
                inflight = p.inflight.len(),
                "backup append sent"
            );
            out.push(Action::Send {
                to: n,
                msg: PeerMsg::BackupAppend {
                    req,
                    epoch: lease.epoch,
                    holder: self.cfg.node_id,
                    config_version: lease.config_version,
                    candidacy,
                    from: start,
                    txs,
                    through,
                    // The holder's readings of its own wait clock now, as a beat
                    // carries them (`publish_alive_targets`): when this was
                    // built and where the lease it holds ends (0: unknown), and
                    // the clock's id. The backup places the end on its own
                    // clock by the least lag of what it receives (plan 43 C7b,
                    // row 24).
                    sent_ms: now.wait.0,
                    expires_ms: self.lease.wait_until(&self.cfg).map_or(0, |end| end.0),
                    clock: self.cfg.clock_id,
                },
            });
        }
        self.arm_backup_tick(now, out);
    }

    fn arm_backup_tick(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.ack.tick_timer.is_some()
            || (self.ack.peers.is_empty() && self.ack.reconfig_wanted.is_none())
        {
            return;
        }
        let id = self.set_timer_wait(
            now.wait.plus(self.cfg.backup_heartbeat_ms.max(1)),
            Timer::BackupTick,
            out,
        );
        self.ack.tick_timer = Some(id);
    }

    pub(crate) fn backup_tick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        // Everything runs in `backup_after_event`, which follows.
        self.ack.tick_fired = true;
        let _ = (now, replica, out);
    }

    /// A backup that makes no progress for `backup_ack_timeout_ms` is
    /// removed (a committed one by a lease CAS) or dropped (a candidate)
    /// — unless it is known alive and not answering short: a loaded node
    /// whose authority core takes seconds over a step acknowledges
    /// seconds late, and dropping it for that (then bringing it, or the
    /// other loaded peer, up again from scratch) left the holder with no
    /// backup for most of `stress-ng-fs-nodes`: every acknowledgement
    /// waited for S3. Such a backup is removed after
    /// `backup_slow_max_ms` without progress; a dead or cut-off one stops
    /// answering the heartbeat and goes after `backup_ack_timeout_ms` as
    /// before.
    fn backup_timeouts(&mut self, now: Now, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        let timeout = self.cfg.backup_ack_timeout_ms as i64;
        let slow_max = (self.cfg.backup_slow_max_ms as i64).max(timeout);
        let tip_behind = |p: &BackupPeer| !p.inflight.is_empty();
        // Under host load (`Config::liveness_lag_aware`): the holder's own
        // lapses are not its backups' silence (its core did not take their
        // answers meanwhile: the same discount as a seal watch's), and a
        // backup's answers to the heartbeat may be a window late — its own
        // runtime held up, its P2P handler cannot answer, and it cannot
        // say so until it runs again. Each such lapse used to drop a
        // healthy backup at the ack timeout: a reconfiguration CAS, then a
        // re-sync from zero. A dead or cut-off one that had answered goes
        // a window later than before (one that never answered, at the
        // timeout); one whose appends fail at the transport, at once.
        let aware = self.cfg.liveness_lag_aware;
        let grace = if aware {
            self.cfg.backup_takeover_ms as i64
        } else {
            0
        };
        let t = self.cfg.backup_takeover_ms.max(1) as i64;
        let since = |s: &Self, at: WaitMs| {
            if aware {
                s.after_own_lapses(at, t, true)
            } else {
                at
            }
        };
        let short_for = if aware { timeout } else { 0 };
        // How long it has been answering short, not counting the gap it is
        // in now if it has stopped answering (its gaps before were taken
        // off as the answers resumed, `on_backup_alive`).
        let interval = self.cfg.backup_heartbeat_ms as i64;
        let short_answering = |p: &BackupPeer| {
            let quiet = now.wait.since(p.alive_at.max(p.short_since)) - interval;
            now.wait.since(p.short_since) - if aware { quiet.max(0) } else { 0 }
        };
        let stale: Vec<NodeId> = self
            .ack
            .peers
            .iter()
            .filter(|(_, p)| {
                let silent = now.wait.since(since(self, p.last_progress));
                let unheard = now.wait.since(since(self, p.alive_at));
                tip_behind(p)
                    && silent >= timeout
                    && (unheard >= timeout + if p.answers_beats { grace } else { 0 }
                        // Short for a while of its answering time, not once:
                        // a backup that answered short and then lapsed (its
                        // runtime held up: no answers at all) answers its
                        // backlog in a burst, one append overtaking another,
                        // and is not stuck — the silence rule judges the
                        // lapse.
                        || (p.short_since_progress && short_answering(p) >= short_for)
                        || p.unreachable
                        || silent >= slow_max)
            })
            .map(|(n, _)| *n)
            .collect();
        for n in stale {
            self.stats.backup_ack_timeouts += 1;
            if let Some(p) = self.ack.peers.get(&n) {
                tracing::debug!(
                    node = self.cfg.node_id,
                    backup = n,
                    silent_ms = now.wait.since(since(self, p.last_progress)),
                    unheard_ms = now.wait.since(since(self, p.alive_at)),
                    short = p.short_since_progress,
                    unreachable = p.unreachable,
                    "backup acknowledgement timeout"
                );
            }
            if self.ack.candidate == Some(n) {
                tracing::info!(
                    node = self.cfg.node_id,
                    candidate = n,
                    "backup candidate timed out"
                );
                self.ack.candidate = None;
                self.ack.peers.remove(&n);
                self.ack.last_dropped.insert(n, now.wait);
                let strikes =
                    candidate_strikes_after(self.ack.candidate_strikes.get(&n).copied(), now);
                let until = now.wait.plus(candidate_backoff_ms(
                    self.cfg.backup_reconfig_min_ms,
                    strikes,
                ));
                // Entries that have decayed act as absent ones: gone
                // (a node that left the cluster keeps none).
                self.ack
                    .candidate_strikes
                    .retain(|_, (_, u)| now.wait.since(*u) < CANDIDATE_BACKOFF_MAX_MS as i64);
                self.ack.candidate_strikes.insert(n, (strikes, until));
                continue;
            }
            self.drop_backup(now, n, "no acknowledgement progress");
        }
        // A committed backup whose link is down is removed too (the
        // timeout would find it; this is sooner).
        let down: Vec<NodeId> = self
            .lease
            .backups()
            .iter()
            .copied()
            .filter(|n| self.links.get(n).is_some_and(|l| !l.connected))
            .collect();
        for n in down {
            self.drop_backup(now, n, "link down");
        }
    }

    /// Reconfigure `n` out of the committed set.
    fn drop_backup(&mut self, now: Now, n: NodeId, why: &'static str) {
        if !self.lease.backups().contains(&n) {
            return;
        }
        if self
            .ack
            .reconfig
            .as_ref()
            .is_some_and(|r| r.remove == Some(n))
        {
            return;
        }
        tracing::warn!(
            node = self.cfg.node_id,
            backup = n,
            why,
            "removing a backup"
        );
        let backups: Vec<NodeId> = self
            .lease
            .backups()
            .iter()
            .copied()
            .filter(|b| *b != n)
            .collect();
        self.ack.reconfig_wanted = Some(backups);
        self.ack.last_dropped.insert(n, now.wait);
    }

    /// A candidate that has caught up joins the lease.
    fn backup_promote(&mut self, now: Now, replica: &dyn Replica, _out: &mut Vec<Action>) {
        let Some(n) = self.ack.candidate else {
            return;
        };
        if self.ack.reconfig.is_some() || self.ack.reconfig_wanted.is_some() {
            return;
        }
        let tip = replica.journal_tip();
        let caught_up = self
            .ack
            .peers
            .get(&n)
            .is_some_and(|p| p.acked >= tip && p.inflight.is_empty());
        if !caught_up {
            return;
        }
        let mut backups = self.lease.backups().to_vec();
        backups.push(n);
        self.ack.reconfig_wanted = Some(backups);
        let _ = now;
    }

    pub(crate) fn issue_reconfig(
        &mut self,
        now: Now,
        backups: Vec<NodeId>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.ack.reconfig.is_some() {
            return;
        }
        if self.lease_cas_in_flight()
            || self.ack.unmarking.is_some()
            || self.ack.unmark_unsettled
            || now.wait.since(self.ack.last_reconfig_try)
                < (self.cfg.backup_heartbeat_ms as i64).max(1)
        {
            self.ack.reconfig_wanted = Some(backups);
            return;
        }
        let Some((lease, tag)) = self.lease.held.clone() else {
            return;
        };
        if lease.backups.iter().any(|b| !backups.contains(b)) && !self.removal_safe(&backups) {
            // Not yet: a ship round (or the staying backups' acks) must
            // first cover what was acknowledged. Retried on the tick.
            self.ack.reconfig_wanted = Some(backups);
            return;
        }
        self.ack.last_reconfig_try = now.wait;
        let policy = if backups.is_empty() {
            AckPolicy::Local
        } else {
            AckPolicy::Backup
        };
        let mut new = lease.reconfigured(backups.clone(), policy);
        self.clear_covered_tail(&mut new);
        // Plan 43 C7-F F-a0 L3: the tenure becomes sealable here (a lease
        // that lists a backup, or says `ack=s3`): a duty it stands on now
        // is in this version, which a sealer replaces
        // (`Core::tenure_duty_live`).
        let marks = lease_fast(&new) && self.tenure_duty_live(now, replica);
        if marks {
            new.granted_delegations = true;
        }
        let add = backups.iter().find(|b| !lease.backups.contains(b)).copied();
        let remove = lease.backups.iter().find(|b| !backups.contains(b)).copied();
        self.issue_s3(
            S3Op::LeaseSwap {
                lease: new.clone(),
                tag: tag.clone(),
            },
            S3For::Reconfig,
            out,
        );
        self.stats.reconfig_cas += 1;
        self.ack.reconfig = Some(Reconfig {
            lease: new,
            attempts: 1,
            marks,
            tag,
            add,
            remove,
        });
    }

    /// Plan 43 C1-3 (D2): `lease`, a renewal or a reconfiguration of the one
    /// held, no longer names the predecessor's tail once the rows this tenure
    /// re-shipped are in the log (the gate done, the journal shipped through
    /// the last of them). A configuration that has not yet made its
    /// predecessor's tail durable is not one a claimant may skip.
    pub(crate) fn clear_covered_tail(&mut self, lease: &mut Lease) {
        if lease.inherited_tail.is_some()
            && self.lease.gate.is_none()
            && self
                .ack
                .inherited_last
                .is_some_and(|last| self.ack.shipped_through >= last)
        {
            lease.inherited_tail = None;
            self.stats.inherited_tails_cleared += 1;
        }
    }

    /// Whether every journal seq an acknowledgement may have rested on is
    /// in the log, or held by each backup of the set that `stays`.
    fn removal_safe(&self, stays: &[NodeId]) -> bool {
        let hwm = self.ack.acked_hwm;
        let shipped =
            (self.ack.shipped_through + 1..=hwm).all(|s| self.ack.shipped_rows.contains(&s));
        if shipped {
            return true;
        }
        !stays.is_empty()
            && stays
                .iter()
                .all(|b| self.ack.peers.get(b).is_some_and(|p| p.counted >= hwm))
    }

    pub(crate) fn on_reconfig_put(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.ack.reconfig.take() else {
            return;
        };
        match result {
            S3Result::LeasePut(Ok(tag)) => {
                tracing::info!(
                    node = self.cfg.node_id,
                    backups = ?r.lease.backups,
                    policy = ?r.lease.ack_policy,
                    config_version = r.lease.config_version,
                    "backup set reconfigured"
                );
                // A mark this reconfiguration wrote for a duty the tenure
                // stood on is inherited (its own, if the lease was marked
                // already, stays its own).
                let was_marked = self
                    .lease
                    .held
                    .as_ref()
                    .is_some_and(|(l, _)| l.granted_delegations);
                if r.marks && !was_marked {
                    self.ack.mark_inherited = true;
                }
                self.lease.renewed(now, r.lease, tag);
                // The catch-all (L3): a duty that arose with the CAS out
                // and the tenure now sealable but unmarked (a duty that
                // no answer path gates: the gates hold the answers).
                if self.fast_tenure() && self.tenure_duty_live(now, replica) {
                    self.ensure_marked_inherited(out);
                }
                self.ack.last_reconfig = now.wait;
                if let Some(n) = r.add {
                    self.stats.backups_added += 1;
                    if self.ack.candidate == Some(n) {
                        self.ack.candidate = None;
                    }
                }
                if let Some(n) = r.remove {
                    self.stats.backups_removed += 1;
                    self.ack.peers.remove(&n);
                }
                self.reconcile_peers_with_lease();
            }
            S3Result::LeasePut(Err(crate::event::CasFailure::Conflict)) => {
                self.issue_s3(S3Op::LeaseGet, S3For::ReconfigReread, out);
                self.ack.reconfig = Some(r);
            }
            _ => {
                // Plan 43 C7-F F-a0 L3: an answer lost may hide a CAS that
                // landed: S3 may hold this version while this node holds
                // the one it replaced, until the lease is next adopted from
                // S3. Sealable and unmarked, nothing is answered unheld
                // meanwhile (`reconfig_in_doubt`); and whatever it lists
                // may seal this tenure, so acknowledgements rest on those
                // nodes too (`durable_jseq`).
                if self.lease.held.as_ref().is_some_and(|(_, t)| *t == r.tag) {
                    let d = self.lease.reconfig_doubt.get_or_insert_default();
                    d.unmarked_sealable |= lease_fast(&r.lease) && !r.lease.granted_delegations;
                    for b in &r.lease.backups {
                        if !d.backups.contains(b) {
                            d.backups.push(*b);
                        }
                    }
                    // Plan 43 C1-1: the set itself too, an empty one
                    // included: acknowledgements rest on each version in
                    // doubt as a whole (`ack_constraint_sets`).
                    if !d.sets.contains(&r.lease.backups) {
                        d.sets.push(r.lease.backups.clone());
                    }
                }
                tracing::warn!(
                    node = self.cfg.node_id,
                    "backup reconfiguration CAS failed; retrying later"
                );
            }
        }
        self.report_durable(replica);
        self.complete_ready(now, replica, out);
    }

    pub(crate) fn on_reconfig_reread(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.ack.reconfig.take() else {
            return;
        };
        let Some((mine, _)) = self.lease.held.clone() else {
            return;
        };
        match result {
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
            {
                // Lost the CAS to our own renewal or a `wanted_by` edit:
                // retry against the fresh object.
                self.lease.renewed(now, cur.clone(), tag.clone());
                if r.attempts < RECONFIG_ATTEMPTS {
                    let mut new = cur.reconfigured(r.lease.backups.clone(), r.lease.ack_policy);
                    let marks =
                        lease_fast(&new) && (r.marks || self.tenure_duty_live(now, replica));
                    if marks {
                        new.granted_delegations = true;
                    }
                    self.issue_s3(
                        S3Op::LeaseSwap {
                            lease: new.clone(),
                            tag: tag.clone(),
                        },
                        S3For::Reconfig,
                        out,
                    );
                    self.stats.reconfig_cas += 1;
                    self.ack.reconfig = Some(Reconfig {
                        lease: new,
                        attempts: r.attempts + 1,
                        marks,
                        tag,
                        ..r
                    });
                } else {
                    self.reconcile_peers_with_lease();
                }
            }
            S3Result::LeaseGet(Ok(Some((cur, _)))) => {
                self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out);
            }
            S3Result::LeaseGet(Ok(None)) => {
                self.deposed(now, 0, 0, mine.epoch, replica, out);
            }
            _ => {}
        }
    }

    /// A backup answered an append.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_backup_ack(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        epoch: Epoch,
        acked: u64,
        sealed: bool,
        fresh: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(p) = self.ack.peers.get_mut(&from) else {
            return;
        };
        let Some(pos) = p.inflight.iter().position(|i| i.req == req) else {
            return;
        };
        let entry = p.inflight.remove(pos).expect("present");
        if Some(epoch) != self.lease.epoch() {
            return;
        }
        self.stats.backup_acks += 1;
        if sealed {
            // It will never acknowledge this epoch again: out of the
            // set before anything further is acknowledged (or it is
            // taking over, and our CAS loses).
            tracing::warn!(
                node = self.cfg.node_id,
                backup = from,
                "a backup sealed our epoch"
            );
            if self.ack.candidate == Some(from) {
                self.ack.candidate = None;
                self.ack.peers.remove(&from);
                // EC2 follow-up (e): a candidate that sealed this epoch (a
                // seal persisted from an earlier tenure of it) refuses it
                // for good. Not asked again this tenure: before, the next
                // housekeeping tick (~100 ms) brought it up again, forever.
                self.ack.refused_epoch.insert(from, epoch);
            } else {
                self.ack.refused_epoch.insert(from, epoch);
                self.drop_backup(now, from, "sealed");
            }
            return;
        }
        let before = self.durable_jseq();
        let p = self.ack.peers.get_mut(&from).expect("present");
        p.unreachable = false;
        // Progress is the hold advancing, or an append answered in full
        // (a heartbeat's included): a backup that keeps answering short
        // for the same rows is stuck, and the timeout rule drops it
        // rather than the two of them resending forever (round 2).
        if acked > p.acked || acked >= entry.last {
            p.last_progress = now.wait;
            p.short_since_progress = false;
        } else if !p.short_since_progress {
            p.short_since_progress = true;
            p.short_since = now.wait;
        }
        tracing::trace!(
            node = self.cfg.node_id,
            backup = from,
            rtt_ms = now.wait.since(entry.sent),
            acked,
            inflight = p.inflight.len(),
            "backup append acknowledged"
        );
        let listed_by_append = p.listed_at.is_some_and(|v| entry.config_version >= v);
        if fresh {
            // Plan 43 C1-1: the backup's hold for this `(holder, epoch)`
            // was created by this very append: whatever it held before
            // was discarded (a lease read that did not list it, a restart
            // that lost its role), so what it holds is exactly `acked`
            // and nothing we counted earlier is still there. Before, the
            // running maximum kept the discarded `acked`; the stream
            // resumed from it, never filled the backup's gap, and the
            // backup was dropped at the ack timeout instead of caught up.
            // The appends still in flight were sent from the old hold;
            // their acks are ignored (not in flight) and the stream
            // resumes from what the backup holds.
            p.acked = acked;
            p.sent_through = acked;
            p.inflight.clear();
            p.counted = if listed_by_append { acked } else { 0 };
        } else {
            // Acks are cumulative (the backup's contiguous hold), and may
            // arrive out of order.
            p.acked = p.acked.max(acked);
            // What an acknowledgement to a client may rest on: only an
            // append that carried a version listing the backup
            // (`Core::backed_through`).
            if listed_by_append {
                p.counted = p.counted.max(acked);
            }
        }
        if acked < entry.last && pos == 0 {
            // The oldest append in flight came back short: rows below
            // it never landed (an earlier request was lost, or answered
            // before them). Resend from what the backup holds — once:
            // the later appends in flight would come back short too,
            // and each would resend the same rows again (their acks
            // are ignored, as any unknown request's is); the inserts
            // are idempotent.
            p.sent_through = p.acked;
            p.inflight.clear();
        }
        p.sent_through = p.sent_through.max(p.acked);
        while p.inflight.front().is_some_and(|i| i.last <= p.acked) {
            p.inflight.pop_front();
        }
        let after = self.durable_jseq();
        if after > before && self.lease.ack_policy() == AckPolicy::Backup {
            self.stream_ahead_soon(now, replica, out);
        }
        // `backup_after_event` streams the next batch and releases the
        // parked acknowledgements.
    }

    /// `Event::BackupAlive`: `from` answered the holder's heartbeat sent
    /// at `at` (this node's wait clock).
    pub(crate) fn on_backup_alive(&mut self, from: NodeId, at: WaitMs) {
        let interval = self.cfg.backup_heartbeat_ms as i64;
        let aware = self.cfg.liveness_lag_aware;
        if let Some(p) = self.ack.peers.get_mut(&from) {
            // A gap in its answers since it began answering short is time
            // it was not answering at all: not counted as answering short.
            let gap = at.since(p.alive_at.max(p.short_since)) - interval;
            if aware && p.short_since_progress && gap > 0 {
                p.short_since = p.short_since.plus(gap as u64);
            }
            p.alive_at = p.alive_at.max(at);
            p.answers_beats = true;
        }
    }

    /// `Event::PeerFailed` for an append: the ack will not come; the
    /// timeout rule decides the rest.
    pub(crate) fn on_backup_request_failed(&mut self, _now: WaitMs, req: OpId, to: NodeId) -> bool {
        let Some(p) = self.ack.peers.get_mut(&to) else {
            return false;
        };
        let Some(pos) = p.inflight.iter().position(|i| i.req == req) else {
            return false;
        };
        p.inflight.remove(pos);
        // Its heartbeat may still be answered over the liveness lane, a
        // connection of its own: that says the backup is alive, not that
        // it can take appends (a wedged data connection beside a healthy
        // liveness one). Until it answers an append again it is held to
        // the ordinary timeout — writes would otherwise park for the
        // slow-core grace (10 s) behind a backup they cannot reach.
        p.unreachable = true;
        // Resend from what the backup holds (the timeout counts from
        // `last_progress`); the inserts are idempotent.
        p.sent_through = p.acked;
        p.last_sent = WaitMs(0);
        true
    }

    /// Rows became durable: stream them ahead now, unless a send went
    /// out within the hold-off — then once it elapses, all at once. One
    /// request per subscriber per hold-off, not per acknowledgement.
    fn stream_ahead_soon(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.ack.stream_ahead_timer.is_some() {
            self.ack.stream_ahead_pending = true;
            return;
        }
        let from = self.ack.streamed_through;
        let to = if self.epoch_streams_ahead() {
            replica.journal_tip()
        } else {
            self.durable_jseq()
        };
        self.stream_ahead(now, from, to, replica, out);
        let id = self.set_timer_wait(
            now.wait.plus(self.cfg.stream_ahead_holdoff_ms.max(1)),
            Timer::StreamAhead,
            out,
        );
        self.ack.stream_ahead_timer = Some(id);
    }

    pub(crate) fn on_stream_ahead_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.ack.stream_ahead_timer = None;
        if !std::mem::take(&mut self.ack.stream_ahead_pending) {
            return;
        }
        let backed = self.lease.held.is_some() && self.lease.ack_policy() == AckPolicy::Backup;
        if !backed && !self.epoch_streams_ahead() {
            return;
        }
        self.stream_ahead_soon(now, replica, out);
    }

    /// Plan 30 §M10 × §M9: this node owns an active continuation epoch's
    /// hold, and streams its journal ahead to the members following its
    /// log stream (see `stream.rs`'s module comment).
    pub(crate) fn epoch_streams_ahead(&self) -> bool {
        self.cfg.pre_s3_streaming
            && self.lease.epoch_held()
            && !self.lease.lost
            && self.epoch.active
            && !self.epoch.frozen
    }

    /// Chunk close-stall-metered (`Core::own_chunks_for`): whether the
    /// pre-S3 stream carries this node's journal through seq `through` to
    /// subscriber `to` without waiting for S3 — what [`Self::stream_ahead`]
    /// would release to it from its cursor once the rows are durable. It
    /// stops before a transaction naming a chunk pending here that `to`
    /// does not have (this node's own write-back, another node's forwarded
    /// close), and then only the segment brings the rest: the forwarder's
    /// upload is on that path. A window longer than one stream batch reads
    /// as "does not reach" (a forwarder's upload costs less than a stall).
    pub(crate) fn stream_reaches(&self, to: NodeId, through: u64, replica: &dyn Replica) -> bool {
        if self.epoch_streams_ahead() {
            return true;
        }
        let cursor = self
            .ack
            .ahead_of
            .get(&to)
            .copied()
            .unwrap_or(self.ack.streamed_through);
        if through <= cursor {
            return true;
        }
        let rows = self.cfg.backup_batch_rows.max(1);
        let txs: Vec<BackupTx> = replica
            .journal_txs_from(cursor + 1, rows)
            .into_iter()
            .filter(|t| t.last <= through)
            .collect();
        txs.last().is_some_and(|t| t.last >= through)
            && replica.releasable_prefix(&txs, Some(to)) >= txs.len()
    }

    /// Stream the epoch journal again from its start (a member
    /// subscribed): the next stream-ahead pass resends everything.
    pub(crate) fn restream_ahead(&mut self) {
        self.ack.streamed_through = 0;
        self.ack.ahead_of.clear();
    }

    /// The durable range `(from, to]` just became backup-acked: stream it
    /// to the log-stream subscribers as speculation (plan 30 §M9).
    ///
    /// A manifest whose chunks are not in S3 yet — a write-back close
    /// here, or a non-owner's `back` close forwarded with its chunks still
    /// uploading (`meta::store::remote`) — stops the stream: a subscriber
    /// that installed it would find its bytes nowhere (S3 lacks them and
    /// dirty chunks are never served), and the stream must stay in
    /// journal order. It and what follows reach subscribers with the
    /// segment (whose ship plan waits for the chunks) or with a later
    /// stream once they are up. The one exception is the forwarder
    /// itself: it has the bytes, and its own close waits for its
    /// transaction to arrive when the holder's unshipped journal touched
    /// the same file (plan 30 §M5's `base`), so it is streamed past such
    /// a transaction of its own ([`AckState::ahead_of`]).
    pub(crate) fn stream_ahead(
        &mut self,
        now: Now,
        from: u64,
        to: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.pre_s3_streaming || to == u64::MAX || to <= self.ack.streamed_through {
            return;
        }
        let start = from.max(self.ack.streamed_through) + 1;
        // Every subscriber, the backups included: a backup holds these
        // rows in its tail, but not in its namespace — without the
        // stream-ahead its own clients' forwards and reads waited for S3
        // (the OVH run's finding 4: the backup's small-file writes cost
        // an S3 round trip each, the other non-owners' did not).
        let subscribers: Vec<NodeId> = self.stream_subscribers();
        if subscribers.is_empty() {
            self.ack.streamed_through = to;
            self.ack.ahead_of.clear();
            return;
        }
        let Some(epoch) = self.lease.epoch() else {
            return;
        };
        let window = |from: u64| -> Vec<BackupTx> {
            replica
                .journal_txs_from(from, self.cfg.backup_batch_rows)
                .into_iter()
                .filter(|t| t.last <= to)
                .collect()
        };
        let txs = window(start);
        // In an active epoch every pending chunk is on a member (the hold
        // owner's own writes, or a member's forwarded close): the members
        // serve them to each other (`coop`), and nothing could upload
        // them before the close anyway, so the S3 gate would stop the
        // stream at the first file write for the whole epoch.
        let in_epoch = self.epoch_streams_ahead();
        let releasable = |txs: &[BackupTx], n: Option<NodeId>| {
            if in_epoch {
                txs.len()
            } else {
                replica.releasable_prefix(txs, n)
            }
        };
        let common = releasable(&txs, None).min(txs.len());
        if common < txs.len() {
            tracing::debug!(
                node = self.cfg.node_id,
                first = txs[common].first,
                held = txs.len() - common,
                "stream-ahead stops before a transaction naming chunks not in S3 yet"
            );
        }
        let through = if common > 0 {
            txs[common - 1].last
        } else {
            self.ack.streamed_through
        };
        let base = self.ship.head_seq;
        let mut sent = 0u64;
        for n in subscribers {
            let cursor = self
                .ack
                .ahead_of
                .get(&n)
                .copied()
                .unwrap_or(self.ack.streamed_through);
            // A subscriber already ahead continues from its own cursor.
            let own;
            let (list, upto) = if cursor > self.ack.streamed_through {
                own = window(cursor + 1);
                let upto = releasable(&own, Some(n)).min(own.len());
                (&own, upto)
            } else {
                let upto = releasable(&txs, Some(n)).max(common).min(txs.len());
                (&txs, upto)
            };
            let batch: Vec<BackupTx> = list[..upto]
                .iter()
                .filter(|t| t.first > cursor)
                .cloned()
                .collect();
            let reached = batch.last().map_or(cursor, |t| t.last);
            if reached > through {
                self.ack.ahead_of.insert(n, reached);
            }
            if batch.is_empty() {
                continue;
            }
            sent = sent.max(batch.len() as u64);
            out.push(Action::Send {
                to: n,
                msg: PeerMsg::StreamAhead {
                    epoch,
                    base,
                    txs: batch,
                },
            });
        }
        self.ack.streamed_through = through;
        self.ack.ahead_of.retain(|_, c| *c > through);
        self.stats.streamed_ahead += sent;
        if in_epoch {
            self.stats.epoch_streamed_ahead += sent;
        }
        let _ = now;
    }

    // ------------------------------------------------- parked ack abort

    /// The lease is gone (deposed, released): whatever waited for
    /// durability is answered as "not acknowledged" — a forwarded reply
    /// as `Busy` (the requester retries the same rid), a local op as in
    /// doubt (retried by rid through the new holder), a control as an
    /// error. The rows themselves are stranded and replayed by plan 30
    /// §M3b's recovery; nothing is lost and nothing runs twice.
    pub(crate) fn ack_abort_parked(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let aborted = self.abort_durable_parks(now, replica, out);
        if aborted > 0 {
            self.stats.acks_aborted += aborted;
        }
    }

    // --------------------------------------------------- the backup side

    /// The holder streams to this node.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_backup_append(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        (epoch, holder, config_version, candidacy): (Epoch, NodeId, u64, u64),
        from_jseq: u64,
        txs: Vec<BackupTx>,
        (through, (clock, sent, expires)): (u64, (u64, i64, i64)),
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        tracing::trace!(
            target: "constellation_authority::ack_wait",
            node = self.cfg.node_id,
            from,
            epoch,
            from_jseq,
            txs = txs.len(),
            "backup append received"
        );
        self.note_foreign(now, replica, out);
        let reply = |acked: u64, sealed: bool, fresh: bool, out: &mut Vec<Action>| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::BackupAck {
                    req,
                    epoch,
                    acked,
                    sealed,
                    fresh,
                },
            });
        };
        // A sealed epoch, or one this node itself holds or has seen
        // superseded, is never acknowledged.
        let superseded = self.bk.sealed >= epoch
            || self
                .lease
                .epoch()
                .is_some_and(|mine| mine >= epoch && self.lease.held.is_some())
            || self.ship.max_epoch > epoch;
        if superseded || from != holder {
            reply(0, true, false, out);
            return;
        }
        // Plan 43 C1-1: this append created the hold (no role for this
        // `(holder, epoch)` before it).
        let mut fresh = false;
        match self.bk.role {
            Some(role) if role.epoch == epoch && role.holder == holder => {
                // Plan 43 C1-1 (review): the highest version this node
                // acknowledged an append at. Appends overtake each other,
                // and one sent before a listing landed may arrive after
                // one sent after it; lowered by it, the role's version no
                // longer told a stale lease read (`on_takeover_get`) from
                // the listing's.
                if config_version > role.config_version {
                    let role = BackupRole {
                        config_version,
                        ..role
                    };
                    replica.set_backup_role(role);
                    self.bk.role = Some(role);
                }
                self.bk.candidacy = self.bk.candidacy.max(candidacy);
            }
            Some(role) if role.epoch > epoch => {
                reply(0, true, false, out);
                return;
            }
            _ => {
                fresh = true;
                let role = BackupRole {
                    holder,
                    epoch,
                    config_version,
                };
                replica.set_backup_role(role);
                self.bk.role = Some(role);
                self.bk.acked = replica.backup_acked(epoch);
                self.bk.held.clear();
                self.bk.candidacy = candidacy;
            }
        }
        self.bk.last_heard = now.wait;
        self.bk.restarted = false;
        self.lease.cached_holder = Some(holder);
        self.bk.holder_silent_since = None;
        // Where the lease ends, as this append says it (plan 43 C7b, row
        // 24): the holder's readings when it built it, placed on this node's
        // clock by the least lag of its arrivals (`known_end`).
        self.note_named_end(holder, epoch, now.wait, (clock, sent, expires));
        // What is acknowledged is held *contiguously* (or is in the log:
        // rows through the holder's shipped-through seq, `through`, are
        // durable in S3, so a batch may start past them). The holder
        // pipelines appends and requests can overtake each other, so a
        // batch ahead of a gap is persisted and remembered, and folded
        // into `acked` once the gap closes; the reply always names the
        // contiguous hold. (A batch below `acked` is a retransmission:
        // idempotent inserts, and `acked` never regresses.)
        if !txs.is_empty() && replica.backup_append(epoch, &txs) {
            self.stats.backup_persisted += txs.len() as u64;
            for t in &txs {
                if t.last > self.bk.acked {
                    self.bk.held.insert(t.first, t.last);
                }
            }
        }
        if from_jseq - 1 <= through {
            self.bk.acked = self.bk.acked.max(from_jseq - 1);
        }
        while let Some(last) = self.bk.held.remove(&(self.bk.acked + 1)) {
            self.bk.acked = self.bk.acked.max(last);
        }
        let floor = self.bk.acked;
        self.bk.held.retain(|_, last| *last > floor);
        reply(self.bk.acked, false, fresh, out);
        // Rows below the holder's shipped-through are in the log
        // (trimmed exactly by the segments we apply; this is the cheap
        // bound meanwhile).
        if through > 0 {
            replica.backup_trim(epoch, through, &[]);
        }
        self.arm_backup_watch(now, out);
    }

    /// Plan 37 §8: the holder this node backs is about to be replaced by
    /// a successor on its own state dir — the same node, which re-adopts
    /// the same lease at the same epoch once it has started, and is
    /// silent until then. Sealing the epoch meanwhile costs the successor
    /// its backup for nothing (a seal, a reconfiguration, durability acks
    /// without a backup until one is brought up again), so the seal watch
    /// counts the silence from `for_ms` from now (at most
    /// [`BACKUP_HOLD_MAX_MS`]); the successor's first append ends the hold
    /// (`on_backup_append` sets `last_heard` to its arrival). Liveness
    /// only, like every timeout here: a hold delays a seal and never
    /// acknowledges anything, so a holder that never comes back is still
    /// sealed and taken over, `for_ms` later.
    pub(crate) fn on_backup_hold(&mut self, now: Now, from: NodeId, epoch: Epoch, for_ms: u64) {
        let ours = self
            .bk
            .role
            .is_some_and(|role| role.holder == from && role.epoch == epoch);
        if !ours || self.bk.sealed >= epoch {
            tracing::debug!(
                node = self.cfg.node_id,
                from,
                epoch,
                role = ?self.bk.role,
                "a backup hold from a holder this node does not back at that epoch: ignored"
            );
            return;
        }
        let until = now.wait.plus(for_ms.min(BACKUP_HOLD_MAX_MS));
        self.bk.last_heard = self.bk.last_heard.max(until);
        self.stats.backup_holds += 1;
        tracing::info!(
            node = self.cfg.node_id,
            holder = from,
            epoch,
            hold_ms = for_ms.min(BACKUP_HOLD_MAX_MS),
            "the holder is being replaced on its own state dir: holding the seal watch"
        );
    }

    /// Plan 30 §M10's claim rule (enforced since M9): whether a
    /// continuation epoch of `members` may carry this node's lease. Its
    /// acknowledgement policy must be one nobody outside the epoch can
    /// take over from: `Local`, or `Backup` with every listed backup a
    /// member (an outside backup could seal and take the lease over
    /// with no TTL and no promise check — the M10 model's
    /// `m9_seal_takeover_against_an_unguarded_epoch_splits_brain`).
    /// Never `S3`: any node may take an `ack=s3` lease over on silence,
    /// and an epoch acknowledges locally, which breaks `ack=s3`'s
    /// durability promise even with nobody outside. And not while a
    /// reconfiguration CAS's outcome is unknown (it could list a backup
    /// this node believes removed).
    /// A backup-set reconfiguration CAS is in flight or wanted.
    pub(crate) fn reconfig_busy(&self) -> bool {
        self.ack.reconfig.is_some() || self.ack.reconfig_wanted.is_some()
    }

    pub(crate) fn epoch_may_carry(&mut self, members: &[NodeId]) -> bool {
        let Some((lease, _)) = self.lease.held.clone() else {
            return false;
        };
        if self.lease.lost {
            return false;
        }
        let ok = super::promise::lease_may_carry(&lease, members, self.reconfig_busy());
        if !ok {
            self.stats.epoch_carry_refused += 1;
            tracing::warn!(
                node = self.cfg.node_id,
                policy = ?lease.ack_policy,
                backups = ?lease.backups,
                ?members,
                "continuation epoch cannot carry this lease (plan 30 M10 claim rule); \
                 writes wait for S3"
            );
        }
        ok
    }

    /// The holder's off-core heartbeat: as good as an append for the seal
    /// watch (silence counts from `at`, when it arrived, not from when
    /// this core got to it). A holder whose authority core is slow — a
    /// busy or overloaded node takes seconds over a step — sends no
    /// appends meanwhile, and its backups used to take that for death and
    /// seal it ~1.5 s in; the heartbeat comes from a task of its driver
    /// that keeps beating while the core is responsive (no step longer
    /// than `CONSTELLATION_HOLDER_STALL_MS`) and stops at the lease's
    /// expiry, so a hung core or a dead process still gets sealed.
    ///
    /// `listed: false` is the holder saying it dropped this node's
    /// `candidacy` from its backups (a candidate that timed out, a
    /// committed backup reconfigured out): the role is dropped and the
    /// tail discarded, as a takeover attempt would after reading the
    /// lease, instead of sealing a live holder first. It applies only to
    /// the candidacy this node is backing under (the latest one it had an
    /// append of): a dismissal is repeated for a while, and one that
    /// arrives after the node was brought up again — the holder counting
    /// its acknowledgements from the new candidacy — must not wipe the
    /// tail those acknowledgements stand for. A dismissal that comes
    /// before the new candidacy's first append is harmless: the holder
    /// counts that candidacy's acknowledgements from zero.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_holder_alive(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        candidacy: u64,
        listed: bool,
        at: WaitMs,
        beat: (u64, u64, i64, i64, i64),
        replica: &dyn Replica,
    ) {
        // Plan 43 C7-H1: the seal watch counts the arrival as this node
        // stamped it. A numbered beat used to be credited no later than its
        // send plus the expiry margin (`min(at, sent + M)`), which compared
        // the holder's `sent` with this node's `at`; `sent` is now a reading
        // of the holder's wait clock, opaque here (`note_beat` keeps it per
        // clock id), so the credit is gone. What it bought was at most one
        // margin of silence window for a beat held up in a retransmit queue.
        if listed {
            self.note_beat(from, epoch, at, beat);
        }
        let Some(role) = self.bk.role else {
            return;
        };
        if role.holder != from || role.epoch != epoch || self.bk.sealed >= epoch {
            return;
        }
        if listed {
            self.bk.last_heard = self.bk.last_heard.max(at);
            self.bk.restarted = false;
            return;
        }
        if candidacy == 0 || candidacy != self.bk.candidacy || self.bk.takeover_get.is_some() {
            tracing::debug!(
                node = self.cfg.node_id,
                holder = from,
                epoch,
                candidacy,
                backing = self.bk.candidacy,
                "a dismissal of another candidacy: ignored"
            );
            return;
        }
        tracing::info!(
            node = self.cfg.node_id,
            holder = from,
            epoch,
            candidacy,
            "the holder dropped this backup; discarding the backup tail"
        );
        self.stats.backup_dismissals += 1;
        self.bk.candidacy = 0;
        self.backup_role_ends(replica);
    }

    /// A beat of `from`'s at `epoch`, heard at `at` (`seq` 0: an arrival the
    /// driver handed over, not a numbered beat; it carries the stamps of the
    /// latest beat or append it stands for, if any, and nothing to tell its
    /// lateness by): its arrival, and whether it came late
    /// ([`HolderBeats::late`]).
    /// A numbered beat also records what it says of the holder's lease on
    /// this node's clock (`lags`, with the raw `at`: see
    /// [`Core::beat_cap`]) and the guard it names.
    fn note_beat(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        at: WaitMs,
        (seq, clock, sent, holder_expires, guard): (u64, u64, i64, i64, i64),
    ) {
        let interval = self.cfg.backup_heartbeat_ms as i64;
        let threshold = self.lag_threshold_ms();
        let own_lapsed = match self.bk.beats.get(&from).and_then(|b| b.last) {
            Some((_, _, last_at, _)) => self.own_lapsed_between(last_at, at),
            None => 0,
        };
        self.bk
            .beats
            .retain(|n, b| *n == from || at.since(b.heard_at) < LATE_BEATS_MEMORY_MS);
        let Some(b) = self.beats_at(from, epoch, at) else {
            return;
        };
        b.end_sample(at, clock, sent, holder_expires);
        if seq == 0 {
            return;
        }
        if sent != 0 {
            if guard == i64::MAX {
                b.guard_unknown = true;
            }
            let agg = b.clock_agg(clock);
            if agg.lags.len() >= BEAT_LAGS_KEPT {
                agg.lags.pop_front();
            }
            agg.lags.push_back((sent, at.0.saturating_sub(sent)));
            agg.named_max = agg.named_max.max(holder_expires);
            if guard != i64::MAX {
                agg.guard_max = agg.guard_max.max(guard);
            }
        }
        match b.last {
            Some((last_seq, last_sent, last_at, last_clock))
                if clock == last_clock && seq > last_seq =>
            {
                // Late against its own schedule: `steps` intervals after
                // the previous one, as sent and as heard (a lost beat is
                // not lateness; a stalled host makes the next one late).
                let steps = (seq - last_seq) as i64;
                // Not what this node's own lapses held up: those are
                // discounted once, by `attentive_since`.
                let heard_late = at.since(last_at) - steps * interval - own_lapsed;
                // Two `sent` of one holder clock (its wait clock: a wall step
                // of the holder moves it neither way).
                let sent_late = sent - last_sent - steps * interval;
                let late = heard_late.max(sent_late);
                if late >= threshold {
                    if b.late.len() >= LATE_BEATS_MAX {
                        b.late.pop_front();
                    }
                    b.late.push_back((at, late));
                }
                b.last = Some((seq, sent, at.max(last_at), clock));
            }
            // A restarted holder (a new process: a new clock id) numbers
            // from 1 again.
            Some((_, _, _, last_clock)) if clock != last_clock => {
                b.last = Some((seq, sent, at, clock));
            }
            // Overtaken by a later beat (they go out concurrently).
            Some(_) => {}
            None => b.last = Some((seq, sent, at, clock)),
        }
    }

    /// `from`'s [`HolderBeats`] at `epoch`, heard at `at`: a new tenure
    /// numbers on (the same beat task), or from 1 (a restarted holder), so
    /// the entry starts afresh, though its late beats still say how the
    /// host has been doing; `None`: `from` has since been heard at a later
    /// epoch.
    fn beats_at(&mut self, from: NodeId, epoch: Epoch, at: WaitMs) -> Option<&mut HolderBeats> {
        let b = self.bk.beats.entry(from).or_default();
        if epoch < b.epoch {
            return None;
        }
        if epoch > b.epoch {
            b.epoch = epoch;
            b.heard_at = at;
            b.last = None;
            b.ends.clear();
            b.end_clock = 0;
            b.clocks.clear();
            b.guard_unknown = false;
        }
        b.heard_at = b.heard_at.max(at);
        Some(b)
    }

    /// An append of `from`'s at `epoch`, arrived at `at`, carries the holder's
    /// readings `(clock, sent, expires)` when it built it, as a beat does:
    /// recorded for [`Self::known_end`]. Appends reach a backup whose
    /// heartbeat lane is broken, and a tenure that sent at least one has
    /// named its end. No other epoch's. Through `beats_at` it also moves
    /// `HolderBeats::heard_at` to the core's `now`, which the `ack=s3` holder
    /// watch reads: by at most one core lag, within the epoch.
    fn note_named_end(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        at: WaitMs,
        (clock, sent, expires): (u64, i64, i64),
    ) {
        if clock == 0 || sent <= 0 || expires <= 0 {
            return;
        }
        if let Some(b) = self.beats_at(from, epoch, at) {
            b.end_sample(at, clock, sent, expires);
        }
    }

    /// How much of `(from, to)` this node spent in its own lapses.
    fn own_lapsed_between(&self, from: WaitMs, to: WaitMs) -> i64 {
        self.bk
            .own_lapses
            .iter()
            .map(|&(a, b)| b.min(to).since(a.max(from)).max(0))
            .sum()
    }

    /// `Event::OwnLag`.
    pub(crate) fn on_own_lag(&mut self, from: WaitMs, until: WaitMs) {
        if until <= from {
            return;
        }
        let lapses = &mut self.bk.own_lapses;
        match lapses.back_mut() {
            // The same lapse, grown, or one that overlaps the last.
            Some((_, end)) if from <= *end => *end = (*end).max(until),
            _ => lapses.push_back((from, until)),
        }
        while lapses.len() > OWN_LAPSES_MAX
            || lapses
                .front()
                .is_some_and(|(_, end)| until.since(*end) >= LATE_BEATS_MEMORY_MS)
        {
            lapses.pop_front();
        }
    }

    /// The lapse that counts as lag: an eighth of the takeover window
    /// (187 ms at the default), well above a healthy runtime's timer
    /// jitter and a LAN's, well below a window.
    fn lag_threshold_ms(&self) -> i64 {
        (self.cfg.backup_takeover_ms as i64 / 8).max(1)
    }

    /// How far past a holder's last word a watch on `holder` at `epoch`
    /// may move the start of its silence for this node's own lapses
    /// ([`Self::attentive_since`]; `None`: no discount, plain silence, and
    /// no late-beat window either):
    ///
    /// - none within the expiry margin of the end of the holder's lease,
    ///   as its own beats and appends last placed it on this node's wait
    ///   clock — or, before either has, as this node's last read of the
    ///   lease object did ([`Self::known_end`]; an end a renewal has not
    ///   moved yet, so the discount may end early, never late).
    ///   Its beats stop at the end, so silence there may be the lease
    ///   ending, not a stall — and a backup must seal and claim promptly
    ///   then: nobody else may claim a `Backup` lease for
    ///   `backup_claim_grace_ms` (2T) past its expiry, time meant for a
    ///   listed backup, whose tail holds what the holder acknowledged but
    ///   did not ship (long-backup seed 802943). The margin ends the
    ///   discount for *when the watch reads*; whether an unexpired lease
    ///   that still lists this node is sealed keeps it
    ///   ([`Self::on_takeover_get`]: the holder's silence in this node's
    ///   attentive time, 2026-10-16 `snapsched`);
    /// - a window (T) plus the lapses' length while the lease, so known,
    ///   runs;
    /// - with no expiry known at all (no beat or append heard, no read naming this
    ///   tenure): for the seal watch a flat half window, lapses included —
    ///   the read then comes at most 1.5T after the holder's last word in
    ///   wall time, whatever the lapse, inside the 2T grace past an expiry
    ///   it could not know of; for `ack=s3`'s watch, which has no such
    ///   grace to keep (every acknowledged record is in S3), a window plus
    ///   the lapses.
    fn lag_discount(&self, (holder, epoch, seal): Watched, now: Now) -> Option<Reach> {
        if !self.cfg.liveness_lag_aware {
            return None;
        }
        let t = self.cfg.backup_takeover_ms.max(1) as i64;
        let margin = self.cfg.expiry_margin_ms as i64;
        match self.known_end(holder, epoch, now) {
            Some(end) => (end.since(now.wait) > margin).then_some(Reach::PlusLapses(t)),
            None if seal => Some(Reach::Flat(t / 2)),
            None => Some(Reach::PlusLapses(t)),
        }
    }

    /// Where `holder`'s lease at `epoch` ends on this node's **wait** clock,
    /// as this node knows it (plan 43 C7b, row 24): the latest of what the
    /// holder's own words placed and, for a lease this node read whose
    /// renewal they had not named (a renewal it did not hear: its beats lost,
    /// or the holder's other word), the first sight of that key plus the
    /// holder's TTL ([`LeaseState::observe`]; the holder's window for it ends
    /// one TTL after it was written, which was before this node first saw it).
    ///
    /// The holder's words are readings of its own wait clock, `X` the
    /// highest end named and `sent` when each arrival left it. `X` is placed
    /// on this node's clock as `X + the least (arrival − sent)` over the
    /// recent arrivals, the way a successor places the end of the floor it
    /// waits out ([`Self::beat_cap`]; rate only, no offset): an arrival this
    /// node stamped late, because its runtime was held up and the driver
    /// stamps the backlog when it gets to it, lengthens its own difference
    /// and no other's, so the end is not pushed past the 2T claim grace
    /// that a listed backup's seal must beat by a lapse of its own. Each term
    /// is no earlier in true time than the holder's own end (the holder's wait
    /// clock never over-counts, and an arrival follows its send): a seal by
    /// it is never earlier than the holder's, and the silence trigger is the
    /// other way in.
    /// Every append carries the same readings, so a backup with a broken
    /// heartbeat lane that never read the lease during the tenure still knows
    /// it, once the holder has sent it one append; `None` is a tenure that
    /// sent this node no append, no beat (and none read): it has nothing
    /// unshipped to lose, a holder lists a backup only to append to it.
    pub(crate) fn known_end(&self, holder: NodeId, epoch: Epoch, now: Now) -> Option<WaitMs> {
        let beats = self.bk.beats.get(&holder).filter(|b| b.epoch == epoch);
        let latest = beats.and_then(|b| {
            b.ends
                .iter()
                .find(|(c, _)| *c == b.end_clock)
                .map(|(c, agg)| (*c, agg))
        });
        let span = self.beat_cap_span_ms();
        let placed = latest.and_then(|(_, agg)| {
            let x = agg.named_max;
            agg.lags
                .iter()
                // The rate error of a sample sent long before `X` (see
                // `beat_cap`): left out beyond the span.
                .filter(|(sent, _)| (0..=span).contains(&x.saturating_sub(*sent)))
                .map(|(_, lag)| *lag)
                .min()
                .map(|lag| WaitMs(x.saturating_add(lag)))
        });
        let read = self
            .lease
            .obs
            .as_ref()
            .filter(|o| o.body.holder == holder && o.body.epoch == epoch && !o.body.released)
            .filter(|o| {
                // The holder's words had named this very renewal (or a
                // later one): their placing is the better one.
                !latest.is_some_and(|(clock, agg)| {
                    o.body.holder_clock == clock && o.body.holder_wait_ms <= agg.named_max
                })
            })
            .map(|o| {
                let by_observation = o.since.plus(LeaseState::ttl_of(&o.body, &self.cfg));
                if placed.is_some() {
                    return by_observation;
                }
                // Cold: this node has not heard a word of the holder's own
                // since it started (a reboot, an upgrade; or since the
                // holder's last beat is older than its samples), so its wait
                // clock says nothing of how long ago the holder renewed, and
                // a node that watched all along claims the lease one TTL and
                // 2T after that renewal (`backup-lag-at-expiry-beats-lost`
                // seed 318, `backup-crash` 410): a cold backup that waited a
                // TTL from its first read would lose the tail it holds. The
                // one thing that knows how long it was away is the wall
                // clock, so the object's wall expiry is read as a hint, the
                // earlier of the two: an early read is harmless and a seal
                // is safe, only a seal of a live holder when this node's
                // clock is ahead of the holder's, once per start; never a
                // claim, which is judged on observations alone. Heard once
                // (a beat, an append), the holder's own words place the end
                // and the hint is gone.
                by_observation.min(now.wait_of_wall(Ms(o.body.expires_unix_ms)))
            });
        placed.max(read)
    }

    /// Whether a watch discounts lag at all ([`Self::lag_discount`]).
    fn lag_aware(&self, watched: Watched, now: Now) -> bool {
        self.lag_discount(watched, now).is_some()
    }

    /// The silence a watch waits for before it acts on `holder`
    /// (`backup_takeover_ms`, T): T, plus twice the latest of its beats
    /// that came late within the last minute ([`HolderBeats::late`]), at
    /// most 4T. A holder whose host stalled it (or its beats) by `x`
    /// lately is likely to stall that long again, and silence of `x` is
    /// then not a death. Liveness only, like the window itself: the seal
    /// and the log-slot CAS fence at any window (ADR-12), so a longer one
    /// can only delay a failover — by at most 3T, only within a minute of
    /// a late beat, and never past the lease's expiry margin
    /// ([`Self::lag_aware`]).
    pub(crate) fn silence_window(&self, watched: Watched, now: Now) -> i64 {
        let t = self.cfg.backup_takeover_ms.max(1) as i64;
        if !self.lag_aware(watched, now) {
            return t;
        }
        let holder = watched.0;
        let late = self.bk.beats.get(&holder).map_or(0, |b| {
            b.late
                .iter()
                .filter(|(at, _)| now.wait.since(*at) < LATE_BEATS_MEMORY_MS)
                .map(|(_, ms)| *ms)
                .max()
                .unwrap_or(0)
        });
        t + (2 * late).min(3 * t)
    }

    /// Since when a watch whose holder was last heard at `last` counts
    /// silence: not before this node's own latest lapse ended
    /// (`Event::OwnLag`) — beats that reached it during the lapse are
    /// stamped when its runtime gets to them, possibly after the watch
    /// looked — but at most one window plus the length of its lapses
    /// since `last` after `last`, so a node that lags again and again
    /// still acts on a dead holder: within a window plus one of its own
    /// attentive time (the time it was not held up) of the silence.
    pub(crate) fn attentive_since(&self, watched: Watched, last: WaitMs, now: Now) -> WaitMs {
        match self.lag_discount(watched, now) {
            None => last,
            Some(Reach::PlusLapses(beyond)) => self.after_own_lapses(last, beyond, true),
            Some(Reach::Flat(reach)) => self.after_own_lapses(last, reach, false),
        }
    }

    /// `last`, moved past this node's own lapses since it (to the latest
    /// one's end), but by no more than `beyond` — plus the lapses' own
    /// length when `plus_lapses` (a reach in this node's running time;
    /// otherwise in wall time): [`Self::attentive_since`]'s rule, and the
    /// holder's for how long its backups have been silent
    /// (`backup_timeouts`, a window beyond, plus the lapses).
    fn after_own_lapses(&self, last: WaitMs, beyond: i64, plus_lapses: bool) -> WaitMs {
        let (mut latest, mut lapsed) = (last, 0i64);
        for &(from, until) in &self.bk.own_lapses {
            if until > last {
                latest = latest.max(until);
                lapsed += until.since(from.max(last));
            }
        }
        let reach = beyond + if plus_lapses { lapsed } else { 0 };
        latest.min(last.plus(reach.max(0) as u64))
    }

    /// Rule (b): the epoch closed; a member backup resumes its watch with
    /// the holder given a full silence window (its re-acquisition after
    /// the close keeps the epoch, and a stale `last_heard` would seal it
    /// at once).
    pub(crate) fn backup_watch_after_epoch(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.bk.role.is_none() {
            return;
        }
        self.bk.last_heard = now.wait;
        self.arm_backup_watch(now, out);
    }

    fn arm_backup_watch(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.bk.watch_timer.is_some() || !self.cfg.p2p || self.epoch.open {
            // Rule (b): no seal watch while a continuation epoch is
            // open (re-armed by `backup_watch_after_epoch`).
            return;
        }
        let mut due = now.wait.plus(self.cfg.backup_takeover_ms.max(1));
        if let Some(role) = self.bk.role {
            due = self.watch_due_by_end(role.holder, role.epoch, now, due);
        }
        let id = self.set_timer_wait(due, Timer::BackupWatch, out);
        self.bk.watch_timer = Some(id);
    }

    /// `due`, but no later than the end of `holder`'s lease at `epoch` as
    /// this node knows it ([`Self::known_end`]), if that is still ahead: the
    /// watch looks at the end itself, whatever it has credited since
    /// (`on_backup_watch`).
    fn watch_due_by_end(&self, holder: NodeId, epoch: Epoch, now: Now, due: WaitMs) -> WaitMs {
        match self.known_end(holder, epoch, now) {
            Some(end) if end > now.wait => due.min(end),
            _ => due,
        }
    }

    /// The watch fired: silence past `backup_takeover_ms` reads the lease,
    /// and the epoch is sealed and taken over only if it still lists this
    /// node (`on_takeover_get`). A holder that removed this node from its
    /// backups stops appending to it once the removal lands, which is
    /// silence too; sealed then, its epoch refused this node for good, so
    /// the holder never brought it back and ran without a backup: a later
    /// death of the holder was a TTL takeover (up to a minute), not a seal
    /// (1.5 s). Every `daemon --upgrade` or K5 handoff of a backup did
    /// this: the holder dropped it across the restart gap ("no
    /// acknowledgement progress"), and the resumed node sealed 1.5 s after
    /// the removal. Read first, a removed backup sees itself unlisted,
    /// gives its role up unsealed and is invited back as a candidate. The
    /// read costs no failover time: it was issued right after the seal
    /// before, and the seal is a local write. What the read finds decides
    /// (`on_takeover_get`): an expired lease that lists this node is
    /// sealed; an unexpired one is sealed only if the holder's silence,
    /// measured in this node's attentive time (its own lapses
    /// discounted), is still a window or more — also when the watch,
    /// inside the lease's expiry margin, counted plain silence to read.
    pub(crate) fn on_backup_watch(&mut self, now: Now, out: &mut Vec<Action>) {
        let Some(role) = self.bk.role else {
            return;
        };
        if self.lease.held.is_some()
            || self.bk.takeover_get.is_some()
            || self.epoch.open
            // Plan 43 C1-3: a claim that fetched a group's tail holds it as
            // a sealed backup's until its gate applies it; the seal watch
            // must not read the lease and discard it as "not listed".
            || self.bk.fetched.is_some()
            || self.bk.tf.is_some()
        {
            // (`epoch.open`: rule (b) — a member of an open epoch never
            // seals; `backup_watch_after_epoch` resumes the watch.)
            return;
        }
        // EC2 follow-up 3b: a restarted backup has heard nothing because
        // it was down, and redials its holder only now. Silence counts
        // from the moment a link to the holder is up; until then the
        // holder's own word is its lease: read it (not sealed first), and
        // seal and take over only if it expired.
        if self.bk.restarted && self.bk.sealed < role.epoch {
            if self.links.get(&role.holder).is_some_and(|l| l.connected) {
                self.bk.restarted = false;
                self.bk.last_heard = self.bk.last_heard.max(now.wait);
            } else {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
                self.bk.takeover_get = Some(op);
                self.bk.restart_probe = true;
                return;
            }
        }
        // Silence as this node could have heard it (not across its own
        // lapse), against a window that is longer while the holder's
        // beats have lately come late. Plain silence sealed a live holder
        // `silent_ms=1501` in on a busy host (`snapsched-write-overhead`):
        // its beats were held up, and nothing told the backup so.
        let watched = (role.holder, role.epoch, true);
        let window = self.silence_window(watched, now);
        let since = self.attentive_since(watched, self.bk.last_heard, now);
        let silent = now.wait.since(since);
        // Past the end of the holder's lease as this node knows it on its
        // own wait clock (its beats and appends placed it, or this node
        // read it: `known_end`), its silence is not the question; the
        // lease ending is. What this node
        // credited since may be the backlog of its own lapse — the daemon
        // stamps beats and appends as its runtime gets to them and hands
        // the stamps over before this timer — and a window counted from
        // that credit missed the 2T claim grace past the expiry. So the
        // lease is read at once, and the object decides
        // (`on_takeover_get`): expired and still listing this node,
        // sealed; unexpired, the holder's silence judged in this node's
        // attentive time (its own lapses discounted, though the watch
        // counted plain silence inside the expiry margin), and a holder
        // heard within a window of that has a window again, no later than
        // the object's end; moved on, the role ends. Known that way, the
        // end is a placing of the holder's own duration, so a seal by it
        // comes no earlier than the holder's own end in real time (plan 43
        // C7b, row 24: no wall value of another node is compared).
        let expired = self
            .known_end(role.holder, role.epoch, now)
            .is_some_and(|end| now.wait >= end);
        if silent < window && !expired {
            // Looked at again no later than where the discount ends (the
            // lease's expiry margin, `lag_aware`): from there the plain
            // window applies, and a backup that waited out a long one
            // past the expiry would miss its claim grace.
            let mut due = since.plus(window as u64);
            if let Some(end) = self.known_end(role.holder, role.epoch, now) {
                let aware_until = end.minus(self.cfg.expiry_margin_ms);
                if aware_until > now.wait {
                    due = due.min(aware_until);
                }
            }
            let due = self.watch_due_by_end(role.holder, role.epoch, now, due);
            let id = self.set_timer_wait(due, Timer::BackupWatch, out);
            self.bk.watch_timer = Some(id);
            return;
        }
        tracing::info!(
            node = self.cfg.node_id,
            holder = role.holder,
            epoch = role.epoch,
            silent_ms = now.wait.since(self.bk.last_heard),
            attentive_ms = silent,
            window_ms = window,
            at_expiry = silent < window,
            "holder silent (or its lease's expiry as this node knew it passed): reading the \
             lease before sealing its epoch"
        );
        self.bk.watch_read = Some(SealRecord {
            holder: role.holder,
            epoch: role.epoch,
            silent_ms: now.wait.since(self.bk.last_heard),
            attentive_ms: silent,
            window_ms: window,
            at: now.wall,
        });
        let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
        self.bk.takeover_get = Some(op);
        self.bk.takeover_read_at = now.wait;
        self.bk.takeover_read_for_expiry = silent < window;
    }

    pub(crate) fn on_takeover_get(
        &mut self,
        now: Now,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.bk.takeover_get = None;
        let probe = std::mem::take(&mut self.bk.restart_probe);
        let for_expiry = std::mem::take(&mut self.bk.takeover_read_for_expiry);
        let S3Result::LeaseGet(Ok(object)) = result else {
            self.arm_backup_watch(now, out);
            return;
        };
        let Some(role) = self.bk.role else {
            return;
        };
        match object {
            Some((lease, _))
                if lease.holder == role.holder
                    && lease.epoch == role.epoch
                    && !lease.released
                    && lease.backups.contains(&self.cfg.node_id) =>
            {
                // Whether the lease this read shows has run out, on this node's
                // own clocks (plan 43 C7b, row 24): the end its beats and
                // appends placed, or, for a renewal it had not heard, one TTL
                // from its first sight of it (`known_end`; the dispatch
                // observed this read just before): a renewal the beats did
                // not name is alive until its TTL has run on this node's
                // wait clock, never by a wall compare with the holder's.
                let ended = self
                    .known_end(role.holder, role.epoch, now)
                    .is_some_and(|end| now.wait >= end);
                if probe && !ended {
                    // A restarted backup's probe (3b): the holder is
                    // renewing — alive; wait for its link. An expired
                    // lease is a holder gone: seal and take over now.
                    self.lease.note_object(&lease);
                    self.arm_backup_watch(now, out);
                    return;
                }
                if !ended {
                    // The object is unexpired and lists this node. Whether
                    // the holder is silent is judged in this node's
                    // attentive time (`after_own_lapses`: its own lapses
                    // discounted, by a window plus their length at most),
                    // whatever the watch counted: inside the expiry margin,
                    // or past an expiry it knew of (a lower bound: beats its
                    // core had not got to yet named a newer one), the watch
                    // counts plain silence and reads — so that a listed
                    // backup seals and claims inside the 2T grace past the
                    // expiry, and so that the object decides. Heard within a
                    // window of attentive time (or read for the expiry it
                    // knew of, which the object moved), the holder is alive:
                    // a window again, no later than the object's expiry
                    // (`watch_due_by_end`), where the watch reads again
                    // and an expired object seals. 2026-10-16 `snapsched`
                    // under host load: the holder's core held up 6.7 s (no
                    // renewal, its 10 s lease inside the margin), the
                    // backup's runtime 2.9 s with its liveness lane broken
                    // meanwhile: plain silence `silent_ms=3226
                    // attentive_ms=3226` sealed the live holder.
                    let t = self.cfg.backup_takeover_ms.max(1) as i64;
                    let since = if self.cfg.liveness_lag_aware {
                        self.after_own_lapses(self.bk.last_heard, t, true)
                    } else {
                        self.bk.last_heard
                    };
                    let window = self.silence_window((role.holder, role.epoch, true), now);
                    if for_expiry || now.wait.since(since) < window {
                        // (The lease read is in `known_end` through the
                        // observation: a renewal unheard is alive for its TTL
                        // from the first sight of it.)
                        self.lease.note_object(&lease);
                        self.arm_backup_watch(now, out);
                        return;
                    }
                }
                if !probe && self.bk.last_heard >= self.bk.takeover_read_at && !ended {
                    // The holder spoke while the lease was read: alive.
                    // Not once the lease it holds has expired: a holder
                    // stops acting a margin before that, so what reached
                    // this node meanwhile was sent before — the backlog
                    // of this node's own lapse, stamped only as its
                    // runtime got to it. Waiting a window for silence
                    // again then missed the 2T claim grace past the
                    // expiry, and another node took the lease over from
                    // a log without this node's tail.
                    self.lease.note_object(&lease);
                    self.arm_backup_watch(now, out);
                    return;
                }
                if self.epoch.open {
                    // A continuation epoch opened during the read: a
                    // member never seals then (`backup_watch_after_epoch`
                    // re-arms the watch).
                    self.arm_backup_watch(now, out);
                    return;
                }
                if self.bk.sealed < role.epoch {
                    if !replica.backup_seal(role.epoch) {
                        // Could not persist the seal: try again shortly;
                        // never take over unsealed.
                        self.arm_backup_watch(now, out);
                        return;
                    }
                    self.bk.sealed = role.epoch;
                    self.stats.seals += 1;
                    self.bk.last_seal = Some(
                        self.bk
                            .watch_read
                            .filter(|r| r.holder == role.holder && r.epoch == role.epoch)
                            .unwrap_or(SealRecord {
                                holder: role.holder,
                                epoch: role.epoch,
                                at: now.wall,
                                ..Default::default()
                            }),
                    );
                    tracing::warn!(
                        node = self.cfg.node_id,
                        holder = role.holder,
                        epoch = role.epoch,
                        restarted = probe,
                        "holder silent (or its lease expired) and this node still listed: sealed \
                         its epoch to take over"
                    );
                }
                self.lease.note_object(&lease);
                self.lease.takeover_permit = Some((role.epoch, role.holder));
                self.permit_interrupts_handoff_wait(out);
                self.enqueue_job(
                    now,
                    super::jobs::JobReq::Acquire {
                        reason: "backup-takeover",
                        ask_handoff: false,
                    },
                    replica,
                    out,
                );
                // If the acquisition loses (the holder reconfigured us
                // out, or is alive and renewed), the watch re-reads.
                self.arm_backup_watch(now, out);
            }
            Some((lease, _))
                if lease.holder == self.cfg.node_id && lease.epoch == role.epoch + 1 =>
            {
                // Our own takeover CAS landed although its request failed
                // (applied, then timed out): the tail is ours to re-ship;
                // finish the acquisition (backup-crash-slow seed 603631
                // discarded it here).
                self.lease.note_object(&lease);
                self.enqueue_job(
                    now,
                    super::jobs::JobReq::Acquire {
                        reason: "backup-takeover",
                        ask_handoff: false,
                    },
                    replica,
                    out,
                );
                self.arm_backup_watch(now, out);
            }
            Some((lease, _))
                if lease.holder == role.holder
                    && lease.epoch == role.epoch
                    && lease.config_version < role.config_version =>
            {
                // Plan 43 C1-1 (review): a version older than one this node
                // acknowledged an append at. The read was answered by S3
                // before the holder's later CAS landed and reached us after
                // that append: S3 holds a version at least the role's
                // (versions only grow, and the holder sends one only once
                // its CAS landed), so what it says about this node's
                // listing is stale. The holder counts what this node
                // acknowledged under the role's version
                // (`BackupPeer::counted`): discarding the tail on this
                // read left rows acknowledged to clients on no backup's
                // disk. The watch reads again.
                tracing::debug!(
                    node = self.cfg.node_id,
                    holder = lease.holder,
                    epoch = lease.epoch,
                    read = lease.config_version,
                    role = role.config_version,
                    "a lease read older than the backup role's version; ignored"
                );
                self.arm_backup_watch(now, out);
            }
            Some((lease, _))
                if !lease.released
                    && lease.inherited_tail.as_ref().is_some_and(|t| {
                        t.epoch == role.epoch && t.members.contains(&self.cfg.node_id)
                    }) =>
            {
                // Not listed, but the lease names this node's group as the
                // holder of a tail nobody shipped yet (C1-3 D2; a holder's
                // restart re-adoption writes a configuration that lists no
                // backup, `Core::own_group_tail`): a TTL claimant fetches it
                // from here. Kept, unsealed, until the lease stops naming it.
                self.lease.note_object(&lease);
                self.arm_backup_watch(now, out);
            }
            Some((lease, _)) => {
                // Not listed any more, or the lease moved on: our tail is
                // void (a listed successor re-shipped what mattered). Not
                // sealed: a live holder that removed this node may invite
                // it back as a candidate in the same epoch.
                self.lease.note_object(&lease);
                tracing::info!(
                    node = self.cfg.node_id,
                    holder = lease.holder,
                    epoch = lease.epoch,
                    "no longer a listed backup; discarding the backup tail"
                );
                self.bk.role = None;
                self.bk.acked = 0;
                self.bk.held.clear();
                replica.backup_clear();
            }
            None => {
                self.bk.role = None;
                self.bk.acked = 0;
                self.bk.held.clear();
                replica.backup_clear();
            }
        }
    }

    /// This node backs nobody any more; its tail is void.
    pub(crate) fn backup_role_ends(&mut self, replica: &dyn Replica) {
        self.bk.role = None;
        self.bk.acked = 0;
        self.bk.held.clear();
        replica.backup_clear();
    }

    /// The takeover gate (after the marker and the strand): apply what is
    /// left of the predecessor's tail into this node's own journal, in
    /// journal order, skipping transactions the log already completed.
    /// `Err` keeps the gate pending (retried by every round).
    pub(crate) fn apply_backup_tail(
        &mut self,
        prev_epoch: Epoch,
        replica: &dyn Replica,
    ) -> Result<(), String> {
        let tail = replica.backup_tail(prev_epoch);
        let mut applied = 0u64;
        for tx in &tail {
            if tx.records.is_empty() {
                // A hole: rows the holder had shipped (or rolled back)
                // before it streamed past them; nothing to re-apply.
                replica.backup_trim(prev_epoch, tx.last, &[]);
                continue;
            }
            let rid = tx.records.iter().find_map(|r| match r {
                constellation_meta::LogRecord::Completed { rid } => Some(*rid),
                _ => None,
            });
            // A refusal's transaction is done once its rid has an outcome
            // here too (the log carried the refusal already).
            let refused = tx.records.iter().find_map(|r| match r {
                constellation_meta::LogRecord::Refused { rid, .. } => Some(*rid),
                _ => None,
            });
            let done = rid
                .is_some_and(|rid| replica.completed_position(rid).ok().flatten().is_some())
                || refused
                    .is_some_and(|rid| replica.completed_outcome(rid).ok().flatten().is_some());
            tracing::debug!(
                node = self.cfg.node_id,
                first = tx.first,
                last = tx.last,
                origin = ?tx.origin,
                ?rid,
                ?refused,
                done,
                "backup tail transaction"
            );
            if !done {
                // Under the row's delegation origin: a root's append of a
                // delegate's stream row stays one (delegated-holder-cut
                // seed 1719).
                replica
                    .apply_backup_tx_journaled(&tx.records, rid, tx.origin)
                    .map_err(|e| format!("re-applying the backup tail: {e}"))?;
                applied += 1;
            }
            replica.backup_trim(prev_epoch, tx.last, &[]);
        }
        if !tail.is_empty() {
            tracing::info!(
                node = self.cfg.node_id,
                prev_epoch,
                held = tail.len(),
                applied,
                "re-shipping the predecessor's backup tail under the new epoch"
            );
        }
        self.stats.backup_tail_applied += applied;
        self.stats.backup_takeovers += 1;
        // Plan 43 C1-3 (D2): the lease names this tail until the journal is
        // shipped through what it now holds.
        if self
            .lease
            .held
            .as_ref()
            .is_some_and(|(l, _)| l.inherited_tail.is_some())
        {
            self.ack.inherited_last = Some(replica.journal_tip());
        }
        self.bk.role = None;
        self.bk.acked = 0;
        replica.backup_clear();
        Ok(())
    }

    /// A segment of `epoch` applied here: trim the backup tail by its
    /// rows, and re-sync the pre-S3 stream cursor.
    pub(crate) fn backup_note_segment(
        &mut self,
        epoch: Epoch,
        through: u64,
        rows: &[u64],
        replica: &dyn Replica,
    ) {
        if self.bk.role.is_some_and(|r| r.epoch <= epoch) {
            replica.backup_trim(epoch, through, rows);
            if self.bk.role.is_some_and(|r| r.epoch < epoch) {
                // A newer tenure: whoever holds now re-shipped or
                // stranded what our holder left; our tail is void.
                self.bk.role = None;
                self.bk.acked = 0;
                self.bk.held.clear();
                replica.backup_clear();
            }
        }
        // The next streamed transaction must follow the log — or what the
        // stream already installed past it: a segment through `through`
        // that arrives after the stream gave us later transactions of the
        // same epoch must not pull the cursor back (the next batch, which
        // continues from those, was then dropped as not contiguous, and
        // every forward waiting for it waited for the log instead).
        self.bk.ahead_next = match self.bk.ahead_next {
            Some((e, next)) if e == epoch && next > through + 1 => Some((e, next)),
            _ => Some((epoch, through + 1)),
        };
    }

    // -------------------------------------------- pre-S3 stream subscriber

    /// The holder's backups hold `txs`: install them ahead of the log.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_stream_ahead(
        &mut self,
        now: Now,
        from: NodeId,
        epoch: Epoch,
        base: Seq,
        txs: Vec<BackupTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.cfg.pre_s3_streaming
            && self.lease.held.is_none()
            && !self.lease.epoch_held()
            && self.lease.cached_holder == Some(from)
            && epoch == self.ship.max_epoch
            && !self.cursor_free()
        {
            // A job moves the cursor: follow the log first, then these.
            self.bk.ahead_next = None;
            self.park_gapped(from, epoch, base, txs);
            return;
        }
        if !self.cfg.pre_s3_streaming
            || self.lease.held.is_some()
            || self.lease.epoch_held()
            || self.lease.cached_holder != Some(from)
            || epoch != self.ship.max_epoch
        {
            tracing::trace!(
                node = self.cfg.node_id,
                from,
                epoch,
                max_epoch = self.ship.max_epoch,
                cursor_free = self.cursor_free(),
                txs = txs.len(),
                "stream-ahead batch dropped"
            );
            self.stats.streamed_dropped += txs.len() as u64;
            self.bk.ahead_next = None;
            return;
        }
        if self.ship.head_seq < base {
            // Ahead of the segment it follows: wait for that segment.
            if self.bk.ahead_waiting.len() >= AHEAD_WAITING_MAX {
                if let Some((_, _, _, old)) = self.bk.ahead_waiting.pop_front() {
                    self.stats.streamed_dropped += old.len() as u64;
                    self.bk.ahead_next = None;
                }
            }
            self.bk.ahead_waiting.push_back((from, epoch, base, txs));
            return;
        }
        let mut completed: Vec<(constellation_meta::Rid, constellation_meta::KeySet)> = Vec::new();
        let mut txs = txs.into_iter();
        while let Some(tx) = txs.next() {
            let Some((e, next)) = self.bk.ahead_next else {
                // The cursor is lost until the next segment re-syncs it.
                let rest: Vec<BackupTx> = std::iter::once(tx).chain(txs).collect();
                self.park_gapped(from, epoch, base, rest);
                break;
            };
            if e != epoch || tx.first < next {
                // Already in the log (or superseded): nothing to do.
                if tx.first < next && e == epoch {
                    continue;
                }
                self.stats.streamed_dropped += 1;
                self.bk.ahead_next = None;
                continue;
            }
            // Fix "capture under an epoch hold": what the replica holds
            // already (as streamed speculation still outstanding) is not
            // installed again. The cursor is in memory: a subscriber that
            // restarted derives it from the log's `through`, below the
            // journal it had streamed before — and a holder whose flush
            // deferred part of its epoch journal (a member's chunk) keeps
            // those rows unshipped and re-streams them to a (re)subscriber
            // (flex-backup seed 1230: node 1 re-applied the epoch journal
            // it held on top of itself). The replica is the truth.
            let held_tip = replica.streamed_tip(epoch).ok().flatten();
            if held_tip.is_some_and(|tip| tx.first <= tip) {
                self.stats.streamed_held_already += 1;
                self.bk.ahead_next = Some((epoch, tx.last + 1));
                replica.note_streamed(constellation_meta::JournalPos {
                    epoch,
                    jseq: tx.last,
                });
                continue;
            }
            if tx.first != next {
                tracing::trace!(
                    node = self.cfg.node_id,
                    first = tx.first,
                    next,
                    base,
                    head = self.ship.head_seq,
                    "stream-ahead transaction parked: not contiguous"
                );
                // What lies between reaches this node in a segment; these
                // follow it.
                let rest: Vec<BackupTx> = std::iter::once(tx).chain(txs).collect();
                self.park_gapped(from, epoch, base, rest);
                break;
            }
            if tx.records.is_empty() {
                // A hole (round 2): rows the holder shipped and dropped
                // before streaming past them — in the log at or below
                // `base`, which this node has applied. The cursor steps
                // over them.
                self.bk.ahead_next = Some((epoch, tx.last + 1));
                replica.note_streamed(constellation_meta::JournalPos {
                    epoch,
                    jseq: tx.last,
                });
                continue;
            }
            match replica.install_streamed(epoch, from, tx.first, tx.last, &tx.records) {
                Ok(()) => {
                    replica.note_foreign_executed(&tx.records);
                    tracing::debug!(
                        node = self.cfg.node_id,
                        from,
                        epoch,
                        first = tx.first,
                        last = tx.last,
                        base,
                        records = ?tx.records,
                        "streamed transaction installed ahead of the log"
                    );
                    self.stats.streamed_installed += 1;
                    if self.epoch.open {
                        self.stats.epoch_streamed_installed += 1;
                    }
                    let keys = constellation_meta::KeySet::from_records(&tx.records);
                    for rec in &tx.records {
                        if let constellation_meta::LogRecord::Completed { rid } = rec {
                            if rid.node == self.cfg.node_id {
                                completed.push((*rid, keys.clone()));
                            }
                        }
                    }
                    replica.note_covering(
                        constellation_meta::KeySet::from_records(&tx.records),
                        constellation_meta::Position {
                            seq: self.ship.head_seq,
                            pending: Some(constellation_meta::JournalPos {
                                epoch,
                                jseq: tx.last,
                            }),
                            streams: Default::default(),
                        },
                    );
                    self.bk.ahead_next = Some((epoch, tx.last + 1));
                    // The holder's journal is here through `tx.last`, in
                    // its order: a dependency on it (a forward's `deps`,
                    // the root's own writes into a delegated directory
                    // above all) is reached without waiting for S3.
                    replica.note_streamed(constellation_meta::JournalPos {
                        epoch,
                        jseq: tx.last,
                    });
                }
                Err(error) => {
                    tracing::debug!(node = self.cfg.node_id, %error, "streamed transaction not installed");
                    self.stats.streamed_dropped += 1;
                    self.bk.ahead_next = None;
                }
            }
        }
        // This node's own ops among them that wait for the log have their
        // effect here now.
        if !completed.is_empty() {
            self.answer_awaiting_streamed(now, &completed, replica, out);
        }
    }

    /// A segment was applied: install the early `StreamAhead` batches it
    /// was the base of (see [`BackupState::ahead_waiting`]), in arrival
    /// order; those still ahead of the log keep waiting.
    pub(crate) fn retry_stream_ahead(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        while self
            .bk
            .ahead_waiting
            .front()
            .is_some_and(|(_, _, base, _)| *base <= self.ship.head_seq)
        {
            let (from, epoch, base, txs) = self.bk.ahead_waiting.pop_front().expect("front");
            self.on_stream_ahead(now, from, epoch, base, txs, replica, out);
        }
        // The log may have closed the gap before parked transactions (or
        // re-synced a lost cursor): install what now follows it; what the
        // log already holds is skipped, what is still ahead of a gap
        // waits again.
        for (from, epoch, base, txs) in std::mem::take(&mut self.bk.ahead_gapped) {
            self.on_stream_ahead(now, from, epoch, base, txs, replica, out);
        }
    }

    /// Keep `txs` for [`Core::retry_stream_ahead`] (the oldest go once
    /// [`AHEAD_WAITING_MAX`] batches wait).
    fn park_gapped(&mut self, from: NodeId, epoch: Epoch, base: Seq, txs: Vec<BackupTx>) {
        if txs.is_empty() {
            return;
        }
        if self.bk.ahead_gapped.len() >= AHEAD_WAITING_MAX {
            if let Some((_, _, _, old)) = self.bk.ahead_gapped.pop_front() {
                self.stats.streamed_dropped += old.len() as u64;
            }
        }
        self.bk.ahead_gapped.push_back((from, epoch, base, txs));
    }

    // ------------------------------------------- `ack=s3` fast takeover

    /// A non-holder under `ack_policy = S3`: the known holder silent on
    /// P2P for `backup_takeover_ms` is a takeover trigger (the lease's
    /// renewal time is checked at classification).
    fn watch_s3_holder(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if !self.cfg.fast_takeover || !self.cfg.p2p || self.lease.lost || self.epoch.open {
            return;
        }
        let Some(seen) = self.lease.last_seen.clone() else {
            return;
        };
        if seen.ack_policy != AckPolicy::S3 || seen.holder == 0 || seen.holder == self.cfg.node_id {
            self.bk.holder_silent_since = None;
            return;
        }
        // Plan 43 C7b: judged on this node's observation. Without one (the
        // key not watched yet) a read first: the S3 permit's window and the
        // lease's TTL both count from it.
        // Throttled to the holder's renewal cadence (half its TTL): a read
        // that finds no object (which clears the observation) or fails
        // must not be repeated at every tick.
        if self.lease.obs.is_none() {
            let ttl = if seen.ttl_ms > 0 {
                seen.ttl_ms
            } else {
                self.cfg.ttl_ms
            };
            let cadence = (ttl / 2) as i64;
            if self
                .bk
                .s3_cold_read_at
                .is_none_or(|t| now.wait.since(t) >= cadence)
            {
                self.bk.s3_cold_read_at = Some(now.wait);
                self.issue_s3(S3Op::LeaseGet, S3For::PromiseWatch, out);
            }
            return;
        }
        if self.lease.claimable_observed(now, &self.cfg) {
            return;
        }
        // Silence is measured on the holder's log stream when this node
        // subscribes to it (the holder heartbeats it every third of the
        // takeover time under `S3`): a frozen or dead holder stops
        // framing at once, while its QUIC connection and gossip
        // membership linger for seconds. Without a subscription, the
        // link's `connected` flag is all there is (slower, never wrong
        // for safety: the log slot fences either way).
        // On a live subscription the holder's off-core heartbeat counts
        // like a frame (it beats its subscribers): the frames are the
        // core's (its timer), and a holder whose step took 1.5 s — a busy
        // host — was taken over by the first subscriber to look. Silence
        // counts as for the seal watch: not across this node's own lapse,
        // against a window that is longer while the holder's beats have
        // lately come late. Without a live subscription the link decides,
        // as before (a beat heard while subscribed earlier says nothing
        // about now).
        let heard = self
            .bk
            .beats
            .get(&seen.holder)
            .filter(|b| b.epoch == seen.epoch)
            .map(|b| b.heard_at);
        let who = (seen.holder, seen.epoch, false);
        let window = self.silence_window(who, now);
        let silent = match self.stream_last_frame_from(seen.holder) {
            Some(frame) => {
                let last = heard.map_or(frame, |h| h.max(frame));
                now.wait.since(self.attentive_since(who, last, now)) >= window
            }
            None => {
                let up = self.links.get(&seen.holder).is_some_and(|l| l.connected)
                    || self.stream_live_from(seen.holder);
                if up {
                    self.bk.holder_silent_since = None;
                    return;
                }
                let since = *self.bk.holder_silent_since.get_or_insert(now.wait);
                now.wait.since(self.attentive_since(who, since, now)) >= window
            }
        };
        if !silent {
            return;
        }
        if self.lease.takeover_permit == Some((seen.epoch, seen.holder)) {
            return;
        }
        // Plan 43 C7b: silence of the holder in the lease this node last
        // read is worth a permit only if that read is fresh: a lease read
        // that is a silence window old may be one the holder has renewed
        // since (a later epoch even), and a permit for it would claim
        // nothing and count a takeover of a live holder. Read again first,
        // at most once per half window.
        let fresh = self
            .lease
            .obs
            .as_ref()
            .is_some_and(|o| now.wait.since(o.read_at) <= window);
        if !fresh {
            if self
                .bk
                .s3_reread_at
                .is_none_or(|t| now.wait.since(t) >= window / 2)
            {
                self.bk.s3_reread_at = Some(now.wait);
                self.issue_s3(S3Op::LeaseGet, S3For::PromiseWatch, out);
            }
            return;
        }
        self.stats.s3_fast_takeovers += 1;
        tracing::warn!(
            node = self.cfg.node_id,
            holder = seen.holder,
            epoch = seen.epoch,
            "ack=s3 holder silent: taking its lease over (the log-slot CAS fences it)"
        );
        self.lease.takeover_permit = Some((seen.epoch, seen.holder));
        self.permit_interrupts_handoff_wait(out);
        self.enqueue_job(
            now,
            super::jobs::JobReq::Acquire {
                reason: "s3-fast-takeover",
                ask_handoff: false,
            },
            replica,
            out,
        );
    }
}
