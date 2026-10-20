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
//! other, only the reply waits. Under a non-`Local` policy the FUSE fast
//! path is closed (`LeaseView::admit`), so every local mutation goes
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
//!   still need the removed backup until the CAS lands, and need only
//!   the survivors after). A backup that is not (or no longer) in the
//!   object never takes over: it reads the object before its CAS.
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
//! at a decision (H's **honour** clock, plan 43 C7-H1: the lease object
//! carries it, `Lease::holder_expires_ms`, with the id of that clock,
//! `Lease::holder_clock`), `M` the expiry margin.
//! 1. *Everything S waits out is capped by H's lease.* Strict answers only
//!    while the lease is `usable` (expiry − now > M); read delegations
//!    (`maybe_grant`), lock grants and renewals (`lock_cap_ms`), write
//!    delegations and their renewals (`grant_cap_ms`) end at the expiry
//!    less M, their holders honour them from their own send less another
//!    M, and what a delegate grants is capped by its own `until − M`. So
//!    (with `2ρ·ttl ≤ 2M`) nothing H answered is honoured, in real time,
//!    after H's clock reads `E_h` — unless H's wall clock stepped forward
//!    after it last wrote `E_h`: its honour clock then counts the step, the
//!    beats' `sent` jump by it, and a grant made before the step outlives
//!    the cap by the step until H's next renewal (C7-H1's open item R7,
//!    closed in its commit 3 by stamping the beat and the body's end on the
//!    wait clock). Exception: under a continuation epoch's
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
//!    quarantine, which the guard names; and the honour end of that
//!    incarnation is on another clock, kept apart by clock id). (The cap's
//!    own dependence on the holder's clock is step 3's.)
//!    Counting from the *last renewal S observed* instead would be unsafe:
//!    H's other proofs of S3 liveness (segment runs and puts, a lease
//!    read) are invisible to S, and H answers within `W` of any of them.
//! 3. *The beat links the clocks.* A beat says H's honour clock read `sent`
//!    no later than S's clock read `at` (its arrival stamp: `sent` < real
//!    send < real arrival ≤ `at`). H's honour clock reaches `X` at most
//!    `(X − sent)(1 + ρ)/(1 − ρ)` real ms after `sent` (it never
//!    under-counts, so no more), S's runs at least `1 − ρ` per real ms, so
//!    S's clock reading `at + (X − sent) + M` is after H's clock passed `X`
//!    as long as `2ρ(X − sent) < M`: beats sent within `250·M` of `X` only
//!    (`2ρ·250·M = M/2` at ρ = 1 000 ppm). A difference on H's clock plus a
//!    stamp of S's: rate-only, no offset, and **no wall step of H**: the
//!    honour clock ignores a backward step (the differences are unchanged),
//!    and `sent`, the lease's end (written after the step) and the guard all
//!    move together by a forward one. Every term is of one holder clock: S
//!    keeps `lags`, `named_max` and `guard_max` per clock id (a restarted H is
//!    a new process, a new clock; at most two ids), takes the latest
//!    `X_c + min lag_c` over them, and puts the replaced lease's own honour
//!    end in `X` of the id that wrote it (`holder_clock`), never its wall
//!    expiry (a stamp of another clock). No beat of that clock: no cap. Open
//!    item (plan 43 C7-H1): a forward step of H *between its last renewal
//!    and the seal* moves `sent` by the step but not that lease's honour
//!    end, so grants made before the step are capped by `X − sent`, early
//!    by the step; the guard, mapped from the wall clock until the wait
//!    commit, has the same property. Not detected here.
//! 4. *The guard* is what H itself waits out and its lease does not cap:
//!    its own takeover floor (which can outlast its lease), a released
//!    takeover's lock grace, restart quarantines, subtree graces
//!    ([`Core::beat_guard_unix_ms`]); `i64::MAX` while its takeover gate
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
//! honour clock, opaque here). And never the lease's expiry compared with S's
//! clock. No beat, a beat of
//! another holder or epoch, or none sent within `250·M` of `X`: the bound.
//! The floor and the beats are in memory (plan 43 Appendix B rows 14 and
//! 27): a successor that restarted waits the bound.
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

use super::{Core, S3For, Timer};
use crate::action::{Action, S3Op};
use crate::event::{PeerMsg, S3Result};
use crate::ids::{Epoch, HonourMs, Ms, NodeId, Now, OpId, Seq, TimerId};
use crate::replica::Replica;
use constellation_meta::{BackupRole, BackupTx};
use constellation_store_s3::{AckPolicy, Lease};
use std::collections::{BTreeMap, BTreeSet};

/// How many times a reconfiguration CAS is retried against a re-read
/// object before the holder gives up on it (and re-plans later).
const RECONFIG_ATTEMPTS: u32 = 3;
/// How often the holder's backup bookkeeping (selection, timeouts,
/// promotion) runs between ticks.
const HOUSEKEEPING_MS: i64 = 50;
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
pub(crate) fn candidate_strikes_after(prev: Option<(u32, Ms)>, now: Now) -> u32 {
    match prev {
        Some((strikes, until)) if now.wall.since(until) < CANDIDATE_BACKOFF_MAX_MS as i64 => {
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
    pub acked: u64,
    /// The journal seq the appends in flight reach; the next batch
    /// starts past it (pipelining, `Config::backup_max_inflight`).
    pub sent_through: u64,
    /// The appends in flight, oldest first.
    pub inflight: std::collections::VecDeque<Inflight>,
    pub last_sent: Ms,
    /// When it last acknowledged something new (or was added).
    pub last_progress: Ms,
    /// Whether the last append was a resync (the backup's `acked` did not
    /// match ours: it restarted, or we did).
    pub committed: bool,
    /// This bring-up of the backup (`PeerMsg::BackupAppend::candidacy`).
    pub candidacy: u64,
    /// When it was last known alive (`Event::BackupAlive`; its bring-up
    /// counts).
    pub alive_at: Ms,
    /// It answered an append short since its last progress: it is
    /// processing appends but stuck, whether alive or not.
    pub short_since_progress: bool,
    /// When it first answered short since its last progress, moved on by
    /// the gaps in its heartbeat answers since (`on_backup_alive`): how
    /// long it has been answering short while answering at all.
    pub short_since: Ms,
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
    pub sent: Ms,
    pub last: u64,
}

/// A backup-set reconfiguration CAS in flight.
#[derive(Debug, Clone)]
pub(crate) struct Reconfig {
    lease: Lease,
    attempts: u32,
    /// The node being added or removed (for the counters).
    add: Option<NodeId>,
    remove: Option<NodeId>,
}

/// The holder's side: backups, the durability watermarks, the
/// reconfiguration in flight.
#[derive(Debug, Default)]
pub(crate) struct AckState {
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
    last_reconfig: Ms,
    /// The last reconfiguration attempt (a failed CAS is not retried on
    /// every event).
    last_reconfig_try: Ms,
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
    reported: Option<(bool, u64, bool)>,
    /// A fast takeover of a tenure that served strict reads is waiting for
    /// its marker, where the successor's floor starts (see the module
    /// doc); `false` after the floor was set or when none is due.
    pub(crate) pending_floor: bool,
    /// Plan 43 C7-F F-a: the cap the predecessor's beats put on the
    /// pending floor (`Core::beat_cap`, an instant of this node's clock);
    /// `None`: no usable beats, or the predecessor's guard was unknown.
    pending_cap: Option<Ms>,
    /// Plan 43 C7-F F-a (review F1): the latest end, unix ms, of a lock or
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
    /// 0 for a tenure with none: the lease is not idle-released before it
    /// (`in_takeover_floor`).
    floor_until: i64,
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
    last_housekeeping: Ms,
    /// The tick fired since the last bookkeeping run.
    tick_fired: bool,
    /// Whether a peer in budget is eligible as a backup right now (the
    /// last selection pass saw one); `None` until this tenure's first
    /// selection pass ran, which gates like `Some(true)` (see
    /// `durable_jseq`).
    pub eligible: Option<bool>,
    /// When each node was last dropped or removed (not re-added within
    /// `backup_reconfig_min_ms`).
    last_dropped: BTreeMap<NodeId, Ms>,
    /// Candidates that timed out lately: how many times in a row, and
    /// until when they are not brought up again ([`candidate_backoff_ms`];
    /// the count decays, [`candidate_strikes_after`]).
    candidate_strikes: BTreeMap<NodeId, (u32, Ms)>,
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
    pub(crate) fn note_hold_grant(&self, until: i64) {
        self.hold_grant_until
            .fetch_max(until, std::sync::atomic::Ordering::Relaxed);
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
    held: BTreeMap<u64, u64>,
    pub last_heard: Ms,
    /// The highest epoch sealed (persisted by the replica too).
    pub sealed: Epoch,
    pub watch_timer: Option<TimerId>,
    takeover_get: Option<OpId>,
    /// When the silence watch issued `takeover_get`: the holder heard
    /// after this is alive, and the epoch is not sealed.
    takeover_read_at: Ms,
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
    holder_silent_since: Option<Ms>,
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
    own_lapses: std::collections::VecDeque<(Ms, Ms)>,
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
    /// over by the driver as an unnumbered one).
    heard_at: Ms,
    /// The latest numbered beat: `(seq, sent, arrival, clock)`; `sent` a
    /// reading of the holder's honour clock `clock` (plan 43 C7-H1).
    pub(crate) last: Option<(u64, i64, Ms, u64)>,
    /// The latest expiry of the holder's lease its beats at `epoch` named,
    /// on the lease body's **wall** clock (0: none heard): they stop there.
    /// The seal watch's `known_expiry` (plan 43 Appendix B row 24).
    pub(crate) expires: Ms,
    /// Plan 43 C7-F F-a / C7-H1: what the numbered beats of `epoch` say of
    /// where the holder's lease ends on *this* node's clock, for the cap of
    /// a fast takeover's floor ([`Core::beat_cap`]), per clock id of the
    /// holder (at most [`BEAT_CLOCKS_KEPT`], oldest first): a restarted
    /// holder is a new process with a new honour clock, and values of two
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
    late: std::collections::VecDeque<(Ms, i64)>,
}

impl HolderBeats {
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
    /// reading of the holder's honour clock, `at` the arrival as this node
    /// stamped it (never a credit), so `at − sent` places that clock on
    /// this node's: a difference on the holder's clock (rate only, no
    /// offset, no wall step: its honour clock follows a forward step in
    /// `sent` and in every value it names alike, P4) plus a stamp of this
    /// node's.
    pub(crate) lags: std::collections::VecDeque<(i64, i64)>,
    /// The highest lease end (honour) and the highest finite guard those
    /// beats named, on that clock.
    pub(crate) named_max: i64,
    pub(crate) guard_max: i64,
}

/// How many holder clock ids a [`HolderBeats`] keeps terms for.
const BEAT_CLOCKS_KEPT: usize = 2;

/// How many beats' `(sent, at − sent)` a [`HolderBeats`] keeps for the cap
/// of a fast takeover's floor (about 19 s of beats at the default
/// interval; the cap needs only the recent ones).
const BEAT_LAGS_KEPT: usize = 64;

/// How many expiry margins before the instant a cap counts to a beat may
/// have been sent to be used for it (`2ρ·250·M ≤ M/2` at ρ = 1 000 ppm).
const BEAT_CAP_SPAN_MARGINS: i64 = 250;

/// How many early `StreamAhead` batches a subscriber keeps.
const AHEAD_WAITING_MAX: usize = 32;

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
}

impl Core {
    /// The backups the holder's off-core heartbeat goes to: the committed
    /// ones and the candidate, each with its candidacy (0: none known —
    /// such a node is never dismissed).
    pub fn backup_candidacies(&self) -> Vec<(NodeId, u64)> {
        self.lease
            .backups()
            .iter()
            .copied()
            .chain(self.ack.candidate)
            .map(|n| (n, self.ack.peers.get(&n).map_or(0, |p| p.candidacy)))
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
        }
    }

    // ------------------------------------------------------------ start

    /// At start: a persisted backup role (this node backed someone
    /// before it restarted) and a persisted seal are honoured.
    pub(crate) fn backup_start(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.bk.sealed = replica.backup_sealed_epoch();
        if let Some(role) = replica.backup_role() {
            self.bk.role = Some(role);
            self.bk.acked = replica.backup_acked(role.epoch);
            self.bk.last_heard = now.wall;
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
                let committed = self.lease.backups();
                if committed.is_empty() {
                    // While a backup could be had — a candidate is being
                    // brought up, or a peer in budget is eligible —
                    // nothing is acknowledged under `Local`: such an
                    // acknowledgement would rest on nobody's disk but
                    // ours, and the candidate could not take over (it is
                    // not listed). The wait ends with the CAS that lists
                    // it, or once no peer is in budget after all (today's
                    // behaviour).
                    //
                    // Not yet known counts as could-be (backup-crash-slow
                    // seed 603322): the ops queued behind an acquisition
                    // run in the acquisition's own event, before the
                    // tenure's first selection pass, and were
                    // acknowledged at once on this disk alone.
                    let unassessed = self.ack.eligible.is_none() && self.backups_possible();
                    return if self.ack.candidate.is_some()
                        || self.ack.eligible == Some(true)
                        || unassessed
                    {
                        self.ack.shipped_through
                    } else {
                        u64::MAX
                    };
                }
                committed
                    .iter()
                    .map(|b| self.ack.peers.get(b).map(|p| p.acked).unwrap_or(0))
                    .min()
                    .unwrap_or(0)
                    .max(self.ack.shipped_through)
            }
        }
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
        if self.ack.reported != Some((gate, jseq, lost)) {
            self.ack.reported = Some((gate, jseq, lost));
            replica.set_durable(gate, jseq, lost);
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
    /// backup, or a continuation epoch's hold) is always fresh; any other
    /// only within `backup_takeover_ms` of the send of an S3 request that
    /// succeeded without revealing a deposition (`last_s3_fresh`). What a
    /// strict read, a new lock grant and (plan 41 L0) a renewal of a lock
    /// grant or a delegation by the root all need. `now` may be the time
    /// an answer was *decided*: a liveness proof sent after it covers it
    /// too (the successor's marker came after the request that proved
    /// the tenure, so after the decision).
    pub(crate) fn renew_fresh(&self, now: HonourMs) -> bool {
        liveness_fresh(
            self.fast_tenure(),
            now,
            self.ack.last_s3_fresh,
            self.cfg.backup_takeover_ms as i64,
        )
    }

    /// The lease says this tenure grants (`Lease::granted_delegations`),
    /// or nobody can take it over before it expires: its successor waits
    /// out the grants and renewals it answered (`note_marker_landed`'s
    /// floor applies only to a marked tenure).
    pub(crate) fn tenure_marked(&self) -> bool {
        !self.fast_tenure()
            || self
                .lease
                .held
                .as_ref()
                .is_some_and(|(l, _)| l.granted_delegations)
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
        if !self.fast_tenure() || self.lease.lost {
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
    pub(crate) fn ensure_granting_marked(&mut self, out: &mut Vec<Action>) -> bool {
        let Some((lease, tag)) = self.lease.held.clone() else {
            return false;
        };
        if lease.granted_delegations {
            return true;
        }
        if self.ack.marking.is_none() && !self.lease_cas_in_flight() {
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

    // --------------------------------------------------------- the floor

    /// `acquire_won`: a seal or an `ack=s3` fast takeover (the lease had
    /// not expired, or this node's clock cannot tell). Once the marker
    /// lands, the successor's acknowledgements wait out the predecessor's
    /// strict-read horizon — unless that tenure never served one. The
    /// predecessor's lease expiry plays no part in the wait on its own
    /// (`note_marker_landed`); `cap` is the end of that lease as the
    /// predecessor's own heartbeat places it on this node's clock
    /// ([`Core::beat_cap`]), which ends the wait earlier when it is.
    pub(crate) fn note_fast_takeover(&mut self, prev: &Lease, cap: Option<Ms>) {
        // Plan 41 L0: a predecessor's generations are outwaited the
        // takeover window longer (`DelegationState::inherit_slack_ms`).
        self.dl.inherit_slack_ms = self.cfg.backup_takeover_ms;
        self.ack.pending_floor = prev.granted_delegations;
        self.ack.pending_cap = cap.filter(|_| prev.granted_delegations);
    }

    /// Plan 43 C7-F F-a: where the lease of `prev`'s tenure ends on this
    /// node's clock, as that tenure's beats at `prev.epoch` placed it, plus
    /// one expiry margin (see the module doc, "The delegation horizon
    /// across a fast failover"): `X + min(at − sent) + M`, with `X` the
    /// latest of the replaced lease's expiry, the expiries the beats named
    /// and the guards they named, over the beats sent within
    /// `250·M` of `X` (so the rate error `2ρ·(X − sent)` stays under
    /// `M/2`). `None`: no beat of that tenure heard, none that recent, or
    /// one named an unknown guard (the holder's own takeover gate was
    /// pending: what it waited out was not known yet).
    ///
    /// `at` is the arrival as this node stamped it. The seal watch counts
    /// a beat's *credit*, `min(at, sent + M)`, which is an instant of the
    /// holder's clock: used here, a holder whose clock is behind this
    /// node's by more than M ends the floor that much early (the sim's
    /// `fast-takeover-skewed-successor`).
    pub(crate) fn beat_cap(&self, prev: &Lease) -> Option<Ms> {
        let b = self
            .bk
            .beats
            .get(&prev.holder)
            .filter(|b| b.epoch == prev.epoch)?;
        // No honour end of that tenure's holder (a body that predates its
        // clock id, or a fenced one), or no beat of the clock that wrote it:
        // `prev.holder_expires_ms` cannot be placed on any clock heard.
        if b.guard_unknown || prev.holder_clock == 0 || prev.holder_expires_ms == 0 {
            return None;
        }
        let margin = self.cfg.expiry_margin_ms as i64;
        let span = BEAT_CAP_SPAN_MARGINS.saturating_mul(margin);
        let mut cap: Option<i64> = None;
        let mut named_clock_placed = false;
        for (clock, agg) in &b.clocks {
            let own = *clock == prev.holder_clock;
            let x =
                agg.named_max
                    .max(agg.guard_max)
                    .max(if own { prev.holder_expires_ms } else { 0 });
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
        Some(Ms(cap?.saturating_add(margin)))
    }

    /// Plan 43 C7-F F-a: what this node, as the holder, waits out that its
    /// lease does not cap, on its own clock, for its beats to name
    /// ([`constellation_net::Beat::guard_unix_ms`]; 0: none): its takeover
    /// floor (it can outlast the lease: the lock floor of a 20 s lease),
    /// its read and lock quarantines (a restart's, a released takeover's),
    /// its subtree graces and the grants it made under a continuation
    /// epoch's hold (`hold_grant_until`). `i64::MAX` while its takeover gate or a
    /// floor still to be armed is pending: the marker may be ahead of it,
    /// and its own predecessor's proofs extend until then, so the floor
    /// is not known yet and a successor must not cap its own.
    pub fn beat_guard_unix_ms(&self, replica: &dyn Replica) -> i64 {
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
    /// lease caps. The simulator asserts the base never rises.
    pub fn beat_guard_parts(&self, replica: &dyn Replica) -> (i64, i64) {
        if self.lease.gate.is_some() || self.ack.pending_floor {
            return (i64::MAX, 0);
        }
        let base = self
            .ack
            .floor_until
            .max(replica.read_delegations().quarantine_until())
            .max(replica.locks().quarantine_until());
        let protecting = self
            .ack
            .hold_grant_until
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(self.lock_grace_until_ms());
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
        now.wall.0 < self.ack.floor_until
    }

    /// Where this tenure's takeover floor ends, in unix ms (0: none): the
    /// engine's upgrade handover waits for it before it detaches the views,
    /// which keep serving meanwhile.
    pub fn takeover_floor_until_ms(&self) -> i64 {
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
        self.ack.floor_until = 0;
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
        let cap = self.ack.pending_cap.take().map(|c| c.0);
        // Plan 30 §M11 phase 2b: a tenure with live write delegations
        // has grants (and, under them, read grants a delegate gave)
        // that its successor cannot see renewed; one constant covers
        // them all.
        let deleg = if replica.delegation_table().is_empty() {
            0
        } else {
            self.reclaim_horizon_ms() as i64
        };
        let bound = |ttl: u64| {
            now.wall.0
                + self.cfg.backup_takeover_ms as i64
                + ttl as i64
                + deleg
                + 2 * self.cfg.expiry_margin_ms as i64
        };
        let capped = |bound: i64| cap.map_or(bound, |c| bound.min(c));
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
        if floor_end <= now.wall.0 {
            // The predecessor's lease, as its beats placed it, ended
            // before this marker landed: everything it granted is
            // lapsed, and no floor remains.
            self.ack.floor_until = 0;
            self.stats.ack_floor_capped_out += 1;
            tracing::info!(
                node = self.cfg.node_id,
                bound_ms = floor_bound - now.wall.0,
                cap_ms = floor_end - now.wall.0,
                "took a lease over by seal or ack=s3 from a tenure that served strict \
                 reads or locks; its lease, as its beats placed it, has ended: no floor"
            );
            return;
        }
        if self.cfg.locks && locks_until > now.wall.0 {
            replica.locks().set_quarantine(locks_until);
        }
        self.ack.floor_until = floor_end;
        self.stats.ack_floor_waits += 1;
        let capped_by = floor_bound - floor_end;
        if capped_by > 0 {
            self.stats.ack_floor_capped += 1;
            self.stats.ack_floor_cap_saved_ms += capped_by as u64;
        }
        tracing::info!(
            node = self.cfg.node_id,
            wait_ms = until.max(now.wall.0) - now.wall.0,
            lock_wait_ms = locks_until.max(now.wall.0) - now.wall.0,
            bound_ms = bound_until - now.wall.0,
            capped = capped_by > 0,
            "took a lease over by seal or ack=s3 from a tenure that served strict \
             reads or locks; acknowledging no mutation until its delegation \
             horizon has passed"
        );
        if until > now.wall.0 {
            replica.read_delegations().set_quarantine(until);
            self.arm_grant_quarantine(Ms(until), out);
        }
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
                || now.wall.since(self.ack.last_housekeeping) >= HOUSEKEEPING_MS;
            if housekeeping {
                self.ack.tick_fired = false;
                self.ack.last_housekeeping = now.wall;
                self.backup_select(now, replica, out);
            }
            self.backup_stream(now, replica, out);
            if housekeeping {
                self.backup_timeouts(now, replica, out);
                self.backup_promote(now, replica, out);
            }
        }
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
        for (n, p) in self.ack.peers.iter_mut() {
            p.committed = committed.contains(n);
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
        let mut eligible: Vec<(Ms, NodeId)> = self
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
                    .is_some_and(|since| now.wall.since(since) >= self.cfg.backup_stable_ms as i64)
            })
            .map(|l| (l.since.unwrap_or(now.wall), l.node))
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
            && now.wall.since(self.ack.last_reconfig) < self.cfg.backup_reconfig_min_ms as i64
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
                .is_none_or(|at| now.wall.since(*at) >= self.cfg.backup_reconfig_min_ms as i64)
                && self
                    .ack
                    .candidate_strikes
                    .get(n)
                    .is_none_or(|(_, until)| now.wall >= *until)
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
                sent_through: 0,
                inflight: std::collections::VecDeque::new(),
                last_sent: Ms(0),
                last_progress: now.wall,
                committed: false,
                candidacy,
                alive_at: now.wall,
                short_since_progress: false,
                short_since: now.wall,
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
                    || (p.inflight.is_empty() && now.wall.since(p.last_sent) >= heartbeat)
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
                sent: now.wall,
                last: last.unwrap_or(start - 1),
            });
            p.last_sent = now.wall;
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
                    // The lease as the holder holds it now: the beats'
                    // source too (`publish_alive_targets`).
                    expires_unix_ms: lease.expires_unix_ms,
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
        let id = self.set_timer_wall(
            now.wall.plus(self.cfg.backup_heartbeat_ms.max(1)),
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
        let since = |s: &Self, at: Ms| {
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
            let quiet = now.wall.since(p.alive_at.max(p.short_since)) - interval;
            now.wall.since(p.short_since) - if aware { quiet.max(0) } else { 0 }
        };
        let stale: Vec<NodeId> = self
            .ack
            .peers
            .iter()
            .filter(|(_, p)| {
                let silent = now.wall.since(since(self, p.last_progress));
                let unheard = now.wall.since(since(self, p.alive_at));
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
                    silent_ms = now.wall.since(since(self, p.last_progress)),
                    unheard_ms = now.wall.since(since(self, p.alive_at)),
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
                self.ack.last_dropped.insert(n, now.wall);
                let strikes =
                    candidate_strikes_after(self.ack.candidate_strikes.get(&n).copied(), now);
                let until = now.wall.plus(candidate_backoff_ms(
                    self.cfg.backup_reconfig_min_ms,
                    strikes,
                ));
                // Entries that have decayed act as absent ones: gone
                // (a node that left the cluster keeps none).
                self.ack
                    .candidate_strikes
                    .retain(|_, (_, u)| now.wall.since(*u) < CANDIDATE_BACKOFF_MAX_MS as i64);
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
        self.ack.last_dropped.insert(n, now.wall);
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

    fn issue_reconfig(
        &mut self,
        now: Now,
        backups: Vec<NodeId>,
        _replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.ack.reconfig.is_some() {
            return;
        }
        if self.lease_cas_in_flight()
            || now.wall.since(self.ack.last_reconfig_try)
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
        self.ack.last_reconfig_try = now.wall;
        let policy = if backups.is_empty() {
            AckPolicy::Local
        } else {
            AckPolicy::Backup
        };
        let new = lease.reconfigured(backups.clone(), policy);
        let add = backups.iter().find(|b| !lease.backups.contains(b)).copied();
        let remove = lease.backups.iter().find(|b| !backups.contains(b)).copied();
        self.issue_s3(
            S3Op::LeaseSwap {
                lease: new.clone(),
                tag,
            },
            S3For::Reconfig,
            out,
        );
        self.stats.reconfig_cas += 1;
        self.ack.reconfig = Some(Reconfig {
            lease: new,
            attempts: 1,
            add,
            remove,
        });
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
                .all(|b| self.ack.peers.get(b).is_some_and(|p| p.acked >= hwm))
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
                self.lease.renewed(now, r.lease, tag);
                self.ack.last_reconfig = now.wall;
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
                    let new = cur.reconfigured(r.lease.backups.clone(), r.lease.ack_policy);
                    self.issue_s3(
                        S3Op::LeaseSwap {
                            lease: new.clone(),
                            tag,
                        },
                        S3For::Reconfig,
                        out,
                    );
                    self.stats.reconfig_cas += 1;
                    self.ack.reconfig = Some(Reconfig {
                        lease: new,
                        attempts: r.attempts + 1,
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
            p.last_progress = now.wall;
            p.short_since_progress = false;
        } else if !p.short_since_progress {
            p.short_since_progress = true;
            p.short_since = now.wall;
        }
        tracing::trace!(
            node = self.cfg.node_id,
            backup = from,
            rtt_ms = now.wall.since(entry.sent),
            acked,
            inflight = p.inflight.len(),
            "backup append acknowledged"
        );
        // Acks are cumulative (the backup's contiguous hold), and may
        // arrive out of order.
        p.acked = p.acked.max(acked);
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
    /// at `at`.
    pub(crate) fn on_backup_alive(&mut self, from: NodeId, at: Ms) {
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
    pub(crate) fn on_backup_request_failed(&mut self, _now: Ms, req: OpId, to: NodeId) -> bool {
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
        p.last_sent = Ms(0);
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
        let id = self.set_timer_wall(
            now.wall.plus(self.cfg.stream_ahead_holdoff_ms.max(1)),
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
        (through, expires): (u64, Ms),
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
        let reply = |acked: u64, sealed: bool, out: &mut Vec<Action>| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::BackupAck {
                    req,
                    epoch,
                    acked,
                    sealed,
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
            reply(0, true, out);
            return;
        }
        match self.bk.role {
            Some(role) if role.epoch == epoch && role.holder == holder => {
                if role.config_version != config_version {
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
                reply(0, true, out);
                return;
            }
            _ => {
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
        self.bk.last_heard = now.wall;
        self.bk.restarted = false;
        self.lease.cached_holder = Some(holder);
        self.bk.holder_silent_since = None;
        self.note_named_expiry(holder, epoch, now.wall, expires);
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
        reply(self.bk.acked, false, out);
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
        let until = now.wall.plus(for_ms.min(BACKUP_HOLD_MAX_MS));
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
        (at, expires): (Ms, Ms),
        beat: (u64, u64, i64, i64, i64),
        replica: &dyn Replica,
    ) {
        // Plan 43 C7-H1: the seal watch counts the arrival as this node
        // stamped it. A numbered beat used to be credited no later than its
        // send plus the expiry margin (`min(at, sent + M)`), which compared
        // the holder's `sent` with this node's `at`; `sent` is now a reading
        // of the holder's honour clock, opaque here (`note_beat` keeps it per
        // clock id), so the credit is gone. What it bought was at most one
        // margin of silence window for a beat held up in a retransmit queue.
        if listed {
            self.note_beat(from, epoch, at, expires, beat);
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

    /// A beat of `from`'s at `epoch`, heard at `at` and credited as heard
    /// at `credit` (`seq` 0: an arrival the driver handed over, not a
    /// numbered beat; it carries the expiry the beat named, if it was
    /// one, and nothing to tell its lateness by): its arrival, the lease
    /// expiry it names, and whether it came late ([`HolderBeats::late`]).
    /// A numbered beat also records what it says of the holder's lease on
    /// this node's clock (`lags`, with the raw `at`: see
    /// [`Core::beat_cap`]) and the guard it names.
    fn note_beat(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        at: Ms,
        expires: Ms,
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
        // The expiry a beat names (the lease body's wall expiry), numbered
        // or an arrival the driver handed over (the peer service notes it
        // with the stamp, so it is as fresh as the stamp however far behind
        // this node's steps are).
        if expires.0 != 0 {
            b.expires = b.expires.max(expires);
        }
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
                // Two `sent` of one holder clock. A forward wall step of the
                // holder moves its honour clock once, which shows as one
                // late beat: liveness only (plan 43 C7-H1).
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
    fn beats_at(&mut self, from: NodeId, epoch: Epoch, at: Ms) -> Option<&mut HolderBeats> {
        let b = self.bk.beats.entry(from).or_default();
        if epoch < b.epoch {
            return None;
        }
        if epoch > b.epoch {
            b.epoch = epoch;
            b.heard_at = at;
            b.last = None;
            b.expires = Ms(0);
            b.clocks.clear();
            b.guard_unknown = false;
        }
        b.heard_at = b.heard_at.max(at);
        Some(b)
    }

    /// An append of `from`'s at `epoch`, arrived at `at`, names the
    /// expiry of the lease it holds (`expires`; 0: unknown): recorded
    /// where a beat's is, for [`Self::known_expiry`]. Appends reach a
    /// backup whose heartbeat lane is broken, and a tenure that sent at
    /// least one has named its expiry. Never lowered, and no other
    /// epoch's: an epoch's expiry only moves later, so each value is a
    /// lower bound, and a lower bound can only make the watch read the
    /// lease early (which seals nothing: `on_takeover_get` seals only an
    /// expired object that lists this node). Through `beats_at` it also
    /// moves `HolderBeats::heard_at` to the core's `now`, which the
    /// `ack=s3` holder watch reads: by at most one core lag, within the epoch.
    fn note_named_expiry(&mut self, from: NodeId, epoch: Epoch, at: Ms, expires: Ms) {
        if expires.0 <= 0 {
            return;
        }
        if let Some(b) = self.beats_at(from, epoch, at) {
            b.expires = b.expires.max(expires);
        }
    }

    /// How much of `(from, to)` this node spent in its own lapses.
    fn own_lapsed_between(&self, from: Ms, to: Ms) -> i64 {
        self.bk
            .own_lapses
            .iter()
            .map(|&(a, b)| b.min(to).since(a.max(from)).max(0))
            .sum()
    }

    /// `Event::OwnLag`.
    pub(crate) fn on_own_lag(&mut self, from: Ms, until: Ms) {
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
    /// - none within the expiry margin of the holder's lease, as its own
    ///   beats and appends last named the expiry — or, before either has,
    ///   as this node's last read of the lease object did (a lower
    ///   bound: the holder renews past it, so the discount ends early,
    ///   never late).
    ///   Its beats stop at the expiry, so silence there may be the lease
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
        match self.known_expiry(holder, epoch) {
            Some(expires) => (expires.since(now.wall) > margin).then_some(Reach::PlusLapses(t)),
            None if seal => Some(Reach::Flat(t / 2)),
            None => Some(Reach::PlusLapses(t)),
        }
    }

    /// The expiry of `holder`'s lease at `epoch` as its beats and appends
    /// last named it (or as a lease read past it showed it renewed).
    fn beat_named_expiry(&self, holder: NodeId, epoch: Epoch) -> Option<Ms> {
        self.bk
            .beats
            .get(&holder)
            .filter(|b| b.epoch == epoch && b.expires.0 != 0)
            .map(|b| b.expires)
    }

    /// The expiry of `holder`'s lease at `epoch` as this node knows it:
    /// the latest of what its beats and its appends named and what this
    /// node last read (each a lower bound: the holder may have renewed
    /// since). Every `BackupAppend` carries the expiry the holder holds,
    /// so a backup with a broken heartbeat lane that never read the lease
    /// during the tenure still knows it, once the holder has sent it one
    /// append; `None` is a tenure that sent this node no append, no beat
    /// (and none read): it has nothing unshipped to lose, a holder lists
    /// a backup only to append to it.
    pub(crate) fn known_expiry(&self, holder: NodeId, epoch: Epoch) -> Option<Ms> {
        let read = self
            .lease
            .last_seen
            .as_ref()
            .filter(|l| l.holder == holder && l.epoch == epoch)
            .map(|l| Ms(l.expires_unix_ms));
        self.beat_named_expiry(holder, epoch).max(read)
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
                .filter(|(at, _)| now.wall.since(*at) < LATE_BEATS_MEMORY_MS)
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
    pub(crate) fn attentive_since(&self, watched: Watched, last: Ms, now: Now) -> Ms {
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
    fn after_own_lapses(&self, last: Ms, beyond: i64, plus_lapses: bool) -> Ms {
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
        self.bk.last_heard = now.wall;
        self.arm_backup_watch(now, out);
    }

    fn arm_backup_watch(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.bk.watch_timer.is_some() || !self.cfg.p2p || self.epoch.open {
            // Rule (b): no seal watch while a continuation epoch is
            // open (re-armed by `backup_watch_after_epoch`).
            return;
        }
        let mut due = now.wall.plus(self.cfg.backup_takeover_ms.max(1));
        if let Some(role) = self.bk.role {
            due = self.watch_due_by_expiry(role.holder, role.epoch, now, due);
        }
        let id = self.set_timer_wall(due, Timer::BackupWatch, out);
        self.bk.watch_timer = Some(id);
    }

    /// `due`, but no later than the expiry of `holder`'s lease at `epoch`
    /// as this node knows it, if that is still ahead: the watch looks at
    /// the expiry itself, whatever it has credited since
    /// (`on_backup_watch`).
    fn watch_due_by_expiry(&self, holder: NodeId, epoch: Epoch, now: Now, due: Ms) -> Ms {
        match self.known_expiry(holder, epoch) {
            Some(expires) if expires > now.wall => due.min(expires),
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
        if self.lease.held.is_some() || self.bk.takeover_get.is_some() || self.epoch.open {
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
                self.bk.last_heard = self.bk.last_heard.max(now.wall);
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
        let silent = now.wall.since(since);
        // Past the expiry of the holder's lease as this node knows it (its
        // beats named it, or this node read it), its silence is not the
        // question; the lease ending is. What this node
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
        // the object's expiry; moved on, the role ends. Known that way,
        // the expiry is a lower bound, so this reads early at worst, never
        // seals early.
        let expired = self
            .known_expiry(role.holder, role.epoch)
            .is_some_and(|expires| now.wall >= expires);
        if silent < window && !expired {
            // Looked at again no later than where the discount ends (the
            // lease's expiry margin, `lag_aware`): from there the plain
            // window applies, and a backup that waited out a long one
            // past the expiry would miss its claim grace.
            let mut due = since.plus(window as u64);
            if let Some(expires) = self.known_expiry(role.holder, role.epoch) {
                let aware_until = Ms(expires.0 - self.cfg.expiry_margin_ms as i64);
                if aware_until > now.wall {
                    due = due.min(aware_until);
                }
            }
            let due = self.watch_due_by_expiry(role.holder, role.epoch, now, due);
            let id = self.set_timer_wall(due, Timer::BackupWatch, out);
            self.bk.watch_timer = Some(id);
            return;
        }
        tracing::info!(
            node = self.cfg.node_id,
            holder = role.holder,
            epoch = role.epoch,
            silent_ms = now.wall.since(self.bk.last_heard),
            attentive_ms = silent,
            window_ms = window,
            at_expiry = silent < window,
            "holder silent (or its lease's expiry as this node knew it passed): reading the \
             lease before sealing its epoch"
        );
        self.bk.watch_read = Some(SealRecord {
            holder: role.holder,
            epoch: role.epoch,
            silent_ms: now.wall.since(self.bk.last_heard),
            attentive_ms: silent,
            window_ms: window,
            at: now.wall,
        });
        let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
        self.bk.takeover_get = Some(op);
        self.bk.takeover_read_at = now.wall;
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
                if probe && !lease.is_expired(now.wall.0) {
                    // A restarted backup's probe (3b): the holder is
                    // renewing — alive; wait for its link. An expired
                    // lease is a holder gone: seal and take over now.
                    self.lease.note_object(now, &lease);
                    self.arm_backup_watch(now, out);
                    return;
                }
                if !lease.is_expired(now.wall.0) {
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
                    // (`watch_due_by_expiry`), where the watch reads again
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
                    if for_expiry || now.wall.since(since) < window {
                        if let Some(b) = self.bk.beats.get_mut(&role.holder) {
                            if b.epoch == role.epoch {
                                b.expires = b.expires.max(Ms(lease.expires_unix_ms));
                            }
                        }
                        self.lease.note_object(now, &lease);
                        self.arm_backup_watch(now, out);
                        return;
                    }
                }
                if !probe
                    && self.bk.last_heard >= self.bk.takeover_read_at
                    && !lease.is_expired(now.wall.0)
                {
                    // The holder spoke while the lease was read: alive.
                    // Not once the lease it holds has expired: a holder
                    // stops acting a margin before that, so what reached
                    // this node meanwhile was sent before — the backlog
                    // of this node's own lapse, stamped only as its
                    // runtime got to it. Waiting a window for silence
                    // again then missed the 2T claim grace past the
                    // expiry, and another node took the lease over from
                    // a log without this node's tail.
                    self.lease.note_object(now, &lease);
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
                self.lease.note_object(now, &lease);
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
                self.lease.note_object(now, &lease);
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
            Some((lease, _)) => {
                // Not listed any more, or the lease moved on: our tail is
                // void (a listed successor re-shipped what mattered). Not
                // sealed: a live holder that removed this node may invite
                // it back as a candidate in the same epoch.
                self.lease.note_object(now, &lease);
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
        if seen.is_claimable(now.wall.0) {
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
                now.wall.since(self.attentive_since(who, last, now)) >= window
            }
            None => {
                let up = self.links.get(&seen.holder).is_some_and(|l| l.connected)
                    || self.stream_live_from(seen.holder);
                if up {
                    self.bk.holder_silent_since = None;
                    return;
                }
                let since = *self.bk.holder_silent_since.get_or_insert(now.wall);
                now.wall.since(self.attentive_since(who, since, now)) >= window
            }
        };
        if !silent {
            return;
        }
        if self.lease.takeover_permit == Some((seen.epoch, seen.holder)) {
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
