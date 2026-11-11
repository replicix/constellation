//! Plan 30 §M9: the synchronous backup, seal-based failover, `ack=s3`,
//! and pre-S3 streaming — the holder's acknowledgement gate, the backup's
//! side, and the fast takeover.
//!
//! # What an acknowledgement means
//!
//! A holder answers a mutation (accepted *or refused*: a refusal was
//! evaluated against the same unshipped state) only once the journal
//! position it was evaluated at is *durable* under the lease's
//! `ack_policy` (`replicix_objcas::AckPolicy`):
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
//! (`Core::root_commit`, stored with the journal row as
//! `constellation_meta::RowCommit`), so what the row is tagged with and what
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
//!   operator abandons the group (`constellation repair abandon`,
//!   `LogRecord::GroupAbandoned` in the claimant's marker). A holder that
//!   sealed itself never revives its epoch: it re-claims its own lease at
//!   the next epoch with its member copy as the tail, at once when the lease
//!   has moved off every key a sealer read (that sealer's CAS cannot land),
//!   otherwise once it expired (D7).
//! - **The inherited tail (D2).** A takeover that re-ships a predecessor's
//!   tail journals the committed rows as its own, and until their segments
//!   are in the log they rest on its disk and in the old group's copies.
//!   The lease it writes says so (`Lease::inherited_tail`: the epoch, the
//!   members, `through`) until the first renewal or reconfiguration CAS
//!   after its journal shipped through them; a claimant of such a lease
//!   fetches from the listed backups (which caught up with the whole
//!   unshipped journal before they were listed) or from the holder and the
//!   old group, and the old members keep their copies — as *member copies*
//!   (plan 43 C1-4, `constellation_meta::MemberTx`) — until the log carries
//!   the rows (below), not until a lease stops naming them.
//! - **The log-slot CAS.** The taker's epoch marker is a create-if-absent
//!   at the next sequence; any segment the old holder ships after it
//!   collides, and what it shipped before it the taker adopted in its
//!   tail to head. Under `S3` this is the only fence needed: every
//!   acknowledged record is in a slot below the marker.
//! - **Reconfiguration races.** A backup's takeover CAS and the holder's
//!   removal CAS are on the same object version; exactly one wins.
//!   Holder wins: the backup re-reads, finds itself unlisted in the same
//!   epoch, discards its tail (safe: the holder counts only what a backup
//!   acknowledged under a version that lists it, C1-1). Backup wins: the
//!   holder's re-read shows a new holder — a deposition, which since plan 43
//!   C1-4 queues nothing it was told is committed for replay: those rows
//!   are rolled back and kept as the member copy of their epoch (plan 30
//!   §M3b's stranding and replay by rid remain for the rows of a lone
//!   node, and for an operator's `abandon`). A backup that reads the
//!   lease moved on to a *later epoch* keeps its tail the same way.
//!
//! # Why a backup-acknowledged row is committed (plan 43 C1)
//!
//! - **S1.** A gated answer leaves when every backup of every
//!   configuration that may be in S3 holds the row: the held version's set
//!   and every out or doubted version's set (empty: the log), each counted
//!   only from acknowledgements given under a version that lists it
//!   (C1-1). A backup that acknowledged under `v` keeps its tail until it
//!   reads a version above `v` without it in the same epoch — a removal by
//!   this holder, during which only the staying set counts. So at the
//!   answer the lease object's group holds the row, whatever version S3 has.
//! - **S2 (seal path).** The sealer is listed in the version it CASes over;
//!   write-all means its contiguous tail holds every committed unshipped
//!   row; its persisted seal stops further commits; the marker's slot CAS
//!   fences later segments; it re-ships the prefix (`TailFollowsThrough`'s
//!   `through`) before its gate opens, each transaction led by its identity
//!   (`LogRecord::TailOf`).
//! - **S3 (TTL path).** A claimant of a lease whose group may hold
//!   committed rows obtains one member's sealed tail before its CAS;
//!   the seal stops further commits of the epoch; the CAS is conditioned on
//!   the version whose group it fetched from. No member: the claim waits
//!   (Q1) or the operator abandons.
//! - **S4 (inherited tail).** Rows a successor re-ships are committed by the
//!   old group but, until the log carries them, held by the new holder and
//!   the old group's copies only. The lease names that group until then, and
//!   every member keeps its copy until the log carries the rows' identities
//!   (never because of a marker alone: a marker that announces no tail of an
//!   epoch drops only the rows nobody acknowledged); a later claimant
//!   fetches from the new group's backups or from the old group.
//! - **S5 (deposed holder).** Gated rows go to the member copy, never to the
//!   replay queue; the client of an aborted parked answer retries by rid and
//!   dedups against the re-ship (committed) or executes once (uncommitted:
//!   the member copy drops it at a `TailFollowsThrough` past `through`).
//! - **S6 (requesters and subscribers).** A shadow or hint answered as
//!   committed (`MutateReply::committed`) and a transaction streamed as
//!   committed (`PeerMsg::StreamAhead::committed`) are not stranded by a
//!   takeover of their epoch: the successor re-ships the row with its rid
//!   and identity, so the entry retires instead of rolling back; the
//!   successor's own entries of the tenure are rolled back, not queued, by
//!   its takeover gate (the tail it re-applies carries the rows). Only an
//!   operator's abandon of the group strands a committed entry
//!   (`speculation.committed_rolled_back`).
//! - **S7 (the class is truthful).** Every row answered without a commit is
//!   `Lone`-class at its answer (C1-2's re-tag), so "gated implies committed
//!   at the answer" holds and only `Lone` rows enter the lone branch.
//!
//! What stays on clocks is liveness (the claim grace orders the sealer
//! before others; the fetch makes losing the race harmless); what is
//! availability is G4 (a claim waits while no member answers).
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
pub use constellation_meta::GroupOn;
use constellation_meta::{BackupRole, BackupTx, MutateOutcome, Rid};
use replicix_objcas::{AckPolicy, Lease, LeaseTag};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    /// It has answered the holder in this bring-up, the heartbeat
    /// (`Event::BackupAlive`) or an append: a backup known to answer may
    /// answer a window late.
    pub answered: bool,
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

/// Plan 46 M1.1 step 5: the commit-relevant part of the lease a tenure
/// holds: what changes the rule its rows are committed by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CfgSeen {
    pub(crate) epoch: Epoch,
    /// The listed backups (none unless the policy is `Backup`).
    pub(crate) backups: Vec<NodeId>,
    pub(crate) singleton: bool,
}

/// A closed configuration of the tenure ([`AckState::spans`]): its rule and
/// the journal position it ended at (the tip when the change that
/// replaced it landed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CfgSpan {
    pub(crate) backups: Vec<NodeId>,
    pub(crate) singleton: bool,
    pub(crate) end: u64,
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
    /// `LeaseState::held_gen` when the re-read of this CAS's loss was sent.
    read_gen: u64,
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
    /// Plan 46 M1.1 step 5: the configurations of this tenure that are
    /// closed (a landed change of the backup set or of the singleton flag
    /// replaced them) and whose rows the journal still holds uncommitted
    /// under their own rule: a row keeps the rule of the configuration it
    /// was journaled in, and a change commits under old and new (the
    /// change's position, the journal's tip when it landed, must be
    /// committed by the old rule before the new rule answers anything past
    /// it). Oldest first; one is dropped once its rule has covered its end
    /// ([`Core::cfg_span_cap`]).
    pub(crate) spans: Vec<CfgSpan>,
    /// The configuration of the held lease as of the last
    /// [`Core::sync_cfg_spans`].
    pub(crate) cfg_seen: Option<CfgSeen>,
    /// Plan 46 M1.4 step 6a (ADR-53): where this tenure started: the held
    /// lease's `config_version` and the journal's tip when it was first seen
    /// (a first tenure of the process, a re-take after the lease was let go,
    /// a lease of another epoch). A gated row at or below the tip,
    /// journaled under an older version, is an earlier tenure's: its group's
    /// acks do not survive the gap, so the archive alone commits it
    /// ([`Core::unsettled_root_row`]).
    pub(crate) tenure_start: Option<(u64, u64)>,
    /// The journal's tip at the event in which the singleton test turned
    /// against a lease that still says singleton: rows past it are not
    /// answered alone (the archive rule), though the lease's bump has not
    /// landed; the singleton configuration ends here.
    pub(crate) flip_tip: Option<u64>,
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
    pub(crate) marking: Option<OpId>,
    /// `LeaseState::held_gen` when the re-read of `marking`'s loss was sent.
    marking_gen: u64,
    /// The body that CAS carries: its answer adopts this one, never one
    /// built from what is held by then (a renewal may have been adopted
    /// since: `LeaseState::advances_held`).
    marking_sent: Option<Lease>,
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
    /// The body the `ClearMark` CAS carries (see `marking_sent`).
    unmark_sent: Option<Lease>,
    /// `LeaseState::held_gen` when the re-read of `unmarking` was sent.
    unmark_gen: u64,
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
    reported: Option<(bool, u64, bool, constellation_meta::RowCommit, bool)>,
    /// A fast takeover of a tenure that served strict reads is waiting for
    /// its marker, where the successor's floor starts (see the module
    /// doc); `false` after the floor was set or when none is due.
    pub(crate) pending_floor: bool,
    /// Plan 43 C7-F F-a: the cap the predecessor's beats put on the
    /// pending floor (`Core::beat_cap`, an instant of this node's wait
    /// clock); `None`: no usable beats, or the predecessor's guard was
    /// unknown.
    pub(crate) pending_cap: Option<WaitMs>,
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
    /// The claim mark could not be read at start (`floor_start`): the
    /// re-adoption of this node's own lease owes the floor as if the mark
    /// named its epoch. Gone once a floor was armed for it, or the mark
    /// was written again.
    pub(crate) floor_mark_unreadable: bool,
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
    /// Plan 46 M1.4 step 2: the timer that resends the hold's unacknowledged
    /// batches ([`Core::on_hold_resend_timer`]).
    pub(crate) hold_resend_timer: Option<TimerId>,
    /// Plan 46 M1.4 step 2: an epoch hold was in force at the last event.
    held_before: bool,
    /// The journal tip when the last hold ended, until the archive carries
    /// it: a row at or below it was journaled before or in the hold, and
    /// answers by the archive or its group, never as a lone node's
    /// ([`Core::park_durable`]); the hold's own acks are forgotten.
    /// Memory only, on purpose: it guards the answers parked (and the
    /// retries answered by `settled_outcome`) in this process, and a restart
    /// drops every parked answer and client entry and ends the tenure, so
    /// the hold's rows are the startup recovery's (stranded, replayed by rid
    /// through the next lease, plan 30 M3b), never answered from here.
    hold_end_tip: Option<u64>,
    /// Plan 46 M1.4 step 1, hold owner: the highest journal seq each epoch
    /// member acknowledged holding (`PeerMsg::StreamAheadAck`), for the
    /// continuation epoch `hold_epoch`. The hold's commit rule reads it
    /// (`durable_jseq_cfg`, step 2).
    pub(crate) hold_acked: BTreeMap<NodeId, u64>,
    /// ADR-51: the members that refused this hold's batches because they
    /// sealed its stream (`PeerMsg::StreamAheadRefused`), by epoch.
    pub(crate) hold_sealed: BTreeSet<(Epoch, NodeId)>,
    hold_epoch: Option<Epoch>,
    /// Batches of the hold streamed to a member and not yet acknowledged:
    /// `(last seq, sent at)`, oldest first (bounded; the wait statistic only).
    pub(crate) hold_sent: BTreeMap<NodeId, VecDeque<(u64, WaitMs)>>,
    /// The highest journal seq streamed to each member in this hold, and the
    /// members whose lost batches are being resent: a resend carries one
    /// batch, and its ack sends the next until the member's ack reaches what
    /// was streamed (a gap wider than a batch; no segment closes it while
    /// the epoch is open).
    pub(crate) hold_sent_hi: BTreeMap<NodeId, u64>,
    pub(crate) hold_resending: std::collections::BTreeSet<NodeId>,
    /// The last run of the periodic backup bookkeeping (selection,
    /// timeouts, promotion): a few times per second is plenty, and the
    /// per-event path stays the append and the release.
    last_housekeeping: WaitMs,
    /// The tick fired since the last bookkeeping run.
    tick_fired: bool,
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
    /// The lease renewal keys the claimants that sealed this node's own
    /// epoch (`sealed`) read, as their `SealTail`s named them (memory only:
    /// a restart forgets them, and the TTL rule of D7 then applies). A
    /// claimant's CAS is conditioned on the key it read, so a lease that
    /// moved off every one of them can no longer be claimed by a sealer
    /// (`LeaseState::sealer_doomed`).
    pub sealer_keys: Vec<replicix_objcas::RenewalKey>,
    /// The wait-clock instant the last such seal was answered.
    pub sealer_at: WaitMs,
    /// The persisted seal could not be read at start: `sealed` is not
    /// known, and nothing is acknowledged or answered until a read succeeds
    /// ([`Core::backup_sealed_known`]; plan 43 C2e review).
    pub(crate) sealed_unread: bool,
    /// ADR-51 (K4, keyed on the tenure): the tenures `(holder, lease epoch)`
    /// this node sealed for a claimant's kept fetch (persisted,
    /// `Replica::seal_tenures`). The holder's own stream of that epoch and
    /// the hold streams of an epoch carrying the tenure install nothing and
    /// acknowledge nothing ([`Core::sealed_hold_stream`]); a successor's own
    /// tenure never matches.
    pub(crate) sealed_tenures: BTreeSet<(NodeId, Epoch)>,
    /// The persisted role could not be read at start (ADR-43): not "no
    /// role"; nothing is answered or claimed until a read succeeds
    /// ([`Core::backup_role_known`]).
    pub(crate) role_unread: bool,
    /// The last `seal_tail_hold` could not read the store: its answer is
    /// none at all, not a refusal.
    pub(crate) seal_unreadable: bool,
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
    ahead_waiting: std::collections::VecDeque<(NodeId, Epoch, Seq, Vec<BackupTx>, bool)>,
    /// Subscriber: `StreamAhead` transactions that did not follow what
    /// this replica holds — a batch arrived while a job had the cursor,
    /// or after an earlier one was lost — kept until the log closes the
    /// gap ([`Core::retry_stream_ahead`]). The holder streams each
    /// transaction once: dropping them left the cursor behind the
    /// stream for as long as the writer kept writing (every later batch
    /// was "not contiguous" too), so this node saw every write only
    /// through S3 — one segment PUT and the ship pacing, 0.4-5 s at
    /// 300 ms per S3 request (EC2 campaign 6, visibility-s3-latency).
    ahead_gapped: std::collections::VecDeque<(NodeId, Epoch, Seq, Vec<BackupTx>, bool)>,
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
    /// ADR-51: the kept fetch of the running claim, the union it completed
    /// (awaiting the log read to head and the merge), whether the union is
    /// installed (the claim's promise re-check does not collect it again),
    /// when this node first saw the lease the fetch is for, and the
    /// operator's `--force` for a divergence.
    pub(crate) kept: Option<super::kept::KeptFetch>,
    pub(crate) kept_union: Option<super::kept::KeptUnion>,
    pub(crate) kept_ready: Option<replicix_objcas::RenewalKey>,
    pub(crate) kept_seen: Option<(Epoch, WaitMs)>,
    /// ADR-51 (S3): the claim waits for a roster read issued after
    /// `kept_seen` (`Action::RefreshRoster` is out): the lease it claims and
    /// the timer that ends the wait. [`Core::kept_roster_arrived`] resumes
    /// the phase when the read is installed.
    pub(crate) kept_roster_wait: Option<(replicix_objcas::Lease, TimerId)>,
    pub(crate) kept_force: bool,
    pub(crate) kept_force_wanted: Option<Epoch>,
    /// The `through_known` of an abandon this claim made after partial
    /// answers (what its union carries of the abandoned epoch).
    pub(crate) abandoned_through: u64,
    pub(crate) gate_abandoned_through: u64,
    pub(crate) fetched: Vec<super::seal_tail::FetchedTail>,
    pub(crate) group_wait: Option<super::seal_tail::GroupWait>,
    pub(crate) abandon: Option<Epoch>,
    pub(crate) abandoned_claim: Option<Epoch>,
    /// Plan 43 C1-4: the abandoned epoch of the claim whose gate is open
    /// (its strand rolls back that group's committed entries too).
    pub(crate) gate_abandoned: Option<Epoch>,
    /// ADR-43: the open gate re-adopts this node's own lease, which names
    /// an inherited tail: its marker never carries the no-tail record.
    pub(crate) gate_names_tail: bool,
    /// Plan 43 C1-4: the extent of the tail the open gate re-ships (its
    /// marker's `TailFollowsThrough.through`).
    pub(crate) tail_through: u64,
    /// ADR-51: the tails the takeover gate re-ships, ascending by epoch, each
    /// with the contiguous hold it reaches; and whether they are a kept
    /// fetch's union (its marker then names every epoch from the first to the
    /// one below the claim's, `tail_through` being the first's).
    pub(crate) gate_tails: Vec<(Epoch, u64)>,
    pub(crate) gate_tails_kept: bool,
    /// With `gate_tails_kept`: the lowest tenure epoch of the fetch (the
    /// marker names every epoch from it).
    pub(crate) gate_tails_from: Epoch,
    /// ADR-51: the claim must land above every epoch a hold journaled under
    /// (the streams the members sealed with rows): the claim's epoch is at
    /// least this plus one.
    pub(crate) kept_floor: Epoch,
    /// ADR-43 (orphan grants): what the open gate's marker proves the log
    /// will never grant, with the gate's epoch: worked out at the gate's
    /// first strand, from the marker's records as they were shipped (a later
    /// pass of the gate has cleared the tail it re-applied, which would read
    /// as a marker that carried none).
    pub(crate) gate_orphan_proof: Option<(Epoch, constellation_meta::OrphanProof)>,
    /// Plan 43 C3 (a delegate that takes the root over): this node's own
    /// rows as the delegate of the generations the open gate strands, as
    /// its journal held them, until the gate re-appends them after the
    /// tail (`Core::reappend_own_delegate_rows`), each with its `deps`, and
    /// with the gate's epoch (a later gate captures its own).
    #[allow(clippy::type_complexity)]
    pub(crate) gate_own_rows: Option<(
        Epoch,
        Vec<(
            constellation_meta::BackupTx,
            Option<constellation_meta::Position>,
        )>,
    )>,
    pub(crate) member_epochs: Vec<Epoch>,
    /// Plan 46 M1.2 step 5: when the ERROR of a diverged tail was last
    /// logged and how many passes it has stood for since (it repeats every
    /// round of the closed gate, `Core::reship_failed`).
    pub(crate) tail_diverged_logged: Option<(WaitMs, u64)>,
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

/// How many unacknowledged hold batches per member the owner times.
const HOLD_SENT_MAX: usize = 256;

/// How many early `StreamAhead` batches a subscriber keeps.
const AHEAD_WAITING_MAX: usize = 32;

/// Plan 46 M1.1 step 7: the names of `Stats::ack_classes`, in counter order
/// (`status.ack.classes`): the definitive answers this node gave, by what
/// their row's configuration made them wait for. `group` counts the
/// configurations that have members, `archive_waits` those that have none
/// (the archive carries the row): the two are disjoint.
pub const ACK_COUNTERS: [&str; 4] = ["group", "final", "singleton", "archive_waits"];

/// The counter's name of an answer under `commit` (`ACK_COUNTERS`).
pub fn commit_name(commit: constellation_meta::RowCommit) -> &'static str {
    ACK_COUNTERS[ack_counter(commit)]
}

/// The place in `Stats::ack_classes` of an answer under `commit`.
pub fn ack_counter(commit: constellation_meta::RowCommit) -> usize {
    use constellation_meta::RowCommit;
    match commit {
        RowCommit::Group {
            on: GroupOn::Archive,
            ..
        } => 3,
        RowCommit::Group { .. } => 0,
        RowCommit::Final => 1,
        RowCommit::Singleton { .. } => 2,
    }
}

/// Plan 43 C2 (§2.1): who an answer goes to — plan 43 §5.1's two columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Asker {
    /// The answering node's own client.
    Own,
    /// Another node (a forward's requester).
    Remote,
}

/// Plan 46 M1.1 step 2: the commit rule of a sequencer's configuration
/// (§5.1), looked up once ([`Core::rule_of`]); the rule stored with a row
/// and named by its answer is the same one (`RowCommit::from`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitRule {
    /// Committed once every member of the configuration holds the row, or
    /// the archive carries it when none is listed.
    Group {
        /// The configuration's version: the lease's `config_version`, or
        /// the generation's `DelegateNodeGroup` version.
        version: u64,
        /// How many nodes besides the holder the rule waits for.
        members: usize,
        on: GroupOn,
    },
    /// A non-stealable configuration (a designation, an epoch hold): the
    /// holder's disk has the row.
    Final,
    /// A sequencer no peer could back (the roster holds no peer within the
    /// budget): answered from its own disk, to its own node's client.
    Singleton {
        /// The configuration's version, as for `Group`.
        version: u64,
    },
}

/// The sequencer a [`CommitRule`] is looked up for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleStream {
    /// This node's root lease.
    Root,
    /// This node's delegate generation `gen`.
    Delegate(u64),
}

impl From<CommitRule> for constellation_meta::RowCommit {
    /// Plan 46 M1.1 step 3: the rule stored with a journal row, in the
    /// version of the configuration it was looked up in.
    fn from(rule: CommitRule) -> Self {
        match rule {
            CommitRule::Group { version, on, .. } => {
                constellation_meta::RowCommit::group(version, on)
            }
            CommitRule::Final => constellation_meta::RowCommit::Final,
            CommitRule::Singleton { version } => constellation_meta::RowCommit::Singleton {
                config_version: version,
            },
        }
    }
}

/// Whether a holder's reply is a definitive answer (the outcome of the op,
/// accepted or refused) as opposed to a redirect, a retry or a hold.
/// The path of an answer that reached the I1 emission gate
/// ([`Core::send_answer`], [`Core::finish`]), for the held counters.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EmissionPath {
    Finish,
    DelegExec,
    HolderReply,
    Designation,
    Release,
}

/// Why the emission gate holds an answer ([`Core::emission_hold_of`]); the
/// journal seq it waits on.
#[derive(Debug, Clone, Copy)]
pub(crate) enum EmissionHold {
    /// It rests on a delegation row its group or the log does not hold.
    Unsettled(u64),
    /// ADR-52: a singleton's answer, and a fresh clock read finds the lease
    /// unusable: it waits for the archive to carry its row or a usable
    /// lease.
    StaleLease(u64),
}

pub(crate) fn is_answer(outcome: &MutateOutcome) -> bool {
    matches!(
        outcome,
        MutateOutcome::Accepted { .. }
            | MutateOutcome::Errno(_)
            | MutateOutcome::Exists { .. }
            | MutateOutcome::Conflict { .. }
            | MutateOutcome::LockLapsed
            | MutateOutcome::Abandoned
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
    /// `ACK_COUNTERS`).
    pub classes: Vec<(&'static str, u64)>,
    /// Plan 43 C1-3: a claim of the lease waits for the tail of an old
    /// group (`status.lease.waiting_for_group`), and the member copies this
    /// node keeps of epochs it sealed.
    pub waiting_for_group: Option<super::seal_tail::WaitingForGroup>,
    pub member_copies: usize,
    /// The persisted seal could not be read since the start: `sealed_epoch`
    /// is not known, and nothing is acknowledged or answered as a backup
    /// until it reads (plan 43 C2e review).
    pub sealed_unread: bool,
    /// Plan 43 C2 (D9): this node answers its own clients as a lone
    /// sequencer — the root lease's (`Core::root_lone`), or a delegation's
    /// it holds (the class it last published for the generation's own
    /// clients is `Lone`).
    pub lone: bool,
    /// Plan 43 C2 (§5.3): shadows and hints installed for an answer not
    /// said to be committed (`Stats::ungated_shadows`).
    pub ungated_shadows: u64,
    /// ... and for a non-stealable sequencer's answers, uncommitted by
    /// design (`Stats::ungated_shadows_non_stealable`).
    pub ungated_shadows_non_stealable: u64,
    /// Plan 43 C2 (§5.3 item 5): this node's client ops settled at their
    /// deadline — settles sent, answered `Abandoned`, answered with the
    /// op's late outcome, settled here (never left this node), and the
    /// unsettled `EIO`s (no sequencer answered within the window)
    /// (`Stats::settles_sent` and the rest).
    pub settles_sent: u64,
    pub settled_abandoned: u64,
    pub settled_late_success: u64,
    pub settled_local: u64,
    pub client_unsettled_eio: u64,
    /// Plan 46 M0.2 (R5): placeholders of dropped rows (`repair
    /// drop-held`) whose requester never retried, settled by the bound
    /// (`Stats::await_retry_settled`).
    pub await_retry_settled: u64,
    /// Inbox batches halted behind the replay queue (`Stats::inbox_halted`).
    pub inbox_halted: u64,
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
            classes: ACK_COUNTERS
                .iter()
                .copied()
                .zip(self.stats.ack_classes)
                .collect(),
            waiting_for_group: self.waiting_for_group(self.clk),
            member_copies: self.bk.member_epochs.len(),
            sealed_unread: self.bk.sealed_unread,
            lone: self.root_lone() || self.dl.reported.values().any(|r| r.commit.is_singleton()),
            ungated_shadows: self.stats.ungated_shadows,
            ungated_shadows_non_stealable: self.stats.ungated_shadows_non_stealable,
            settles_sent: self.stats.settles_sent,
            settled_abandoned: self.stats.settled_abandoned,
            settled_late_success: self.stats.settled_late_success,
            settled_local: self.stats.settled_local,
            client_unsettled_eio: self.stats.client_unsettled_eio,
            await_retry_settled: self.stats.await_retry_settled,
            inbox_halted: self.stats.inbox_halted,
        }
    }

    // ------------------------------------------------------------ start

    /// Whether the persisted backup role is known: read again if its read
    /// failed at start (`false`: still unknown, no append is answered and no
    /// claim is made on it).
    pub(crate) fn backup_role_known(&mut self, replica: &dyn Replica) -> bool {
        if !self.bk.role_unread {
            return true;
        }
        match replica.backup_role() {
            Ok(role) => {
                self.bk.role_unread = false;
                if let Some(role) = role {
                    self.bk.role = Some(role);
                    self.bk.acked = replica.backup_acked(role.epoch).unwrap_or(0);
                    self.bk.restarted = true;
                    self.lease.name_holder(role.holder);
                }
                true
            }
            Err(_) => false,
        }
    }

    /// Whether the highest sealed epoch is known: read again if its read
    /// failed at start (`false`: still unknown, answer nothing).
    pub(crate) fn backup_sealed_known(&mut self, replica: &dyn Replica) -> bool {
        if !self.bk.sealed_unread {
            return true;
        }
        match (replica.backup_sealed_epoch(), replica.sealed_tenures()) {
            (Some(sealed), Some(streams)) => {
                self.bk.sealed = self.bk.sealed.max(sealed);
                self.bk.sealed_tenures.extend(streams);
                self.bk.sealed_unread = false;
                true
            }
            _ => {
                // Every append asks again: warned at the 1st, 2nd, 4th, 8th...
                // failed read, not at each.
                self.stats.backup_seal_unread_reads += 1;
                let n = self.stats.backup_seal_unread_reads;
                if n.is_power_of_two() {
                    tracing::warn!(
                        node = self.cfg.node_id,
                        failed_reads = n,
                        "the persisted backup seal still cannot be read: answering nothing as a backup"
                    );
                }
                false
            }
        }
    }

    /// At start: a persisted backup role (this node backed someone
    /// before it restarted) and a persisted seal are honoured.
    pub(crate) fn backup_start(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        match (replica.backup_sealed_epoch(), replica.sealed_tenures()) {
            (Some(sealed), Some(streams)) => {
                self.bk.sealed = sealed;
                self.bk.sealed_tenures = streams.into_iter().collect();
            }
            _ => {
                self.bk.sealed_unread = true;
                tracing::warn!(
                    node = self.cfg.node_id,
                    "the persisted backup seal could not be read: acknowledging and answering nothing \
                     as a backup until it reads"
                );
            }
        }
        self.bk.member_epochs = replica.member_copy_epochs();
        let role = match replica.backup_role() {
            Ok(role) => role,
            Err(error) => {
                // Not "no role": a claim would re-ship no tail and its
                // marker would say so (ADR-43). Nothing is answered or
                // claimed until a read succeeds (`backup_role_known`).
                self.bk.role_unread = true;
                tracing::warn!(
                    node = self.cfg.node_id,
                    %error,
                    "the persisted backup role could not be read: acknowledging and claiming nothing until it reads"
                );
                None
            }
        };
        if let Some(role) = role {
            self.bk.role = Some(role);
            // (Unreadable: from 0, which only acknowledges less.)
            self.bk.acked = replica.backup_acked(role.epoch).unwrap_or(0);
            self.bk.last_heard = now.wait;
            self.bk.restarted = true;
            self.lease.name_holder(role.holder);
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
        let held = self.durable_jseq_cfg();
        if self.ack.spans.is_empty() || self.lease.epoch_held() {
            return held;
        }
        // Plan 46 M1.1 step 5: a row keeps the rule of the configuration it
        // was journaled in, and a change commits under old and new: nothing
        // past a closed configuration's end is acknowledged before that
        // configuration's own rule has committed its end.
        self.ack
            .spans
            .iter()
            .filter_map(|s| self.cfg_span_cap(s))
            .fold(held, u64::min)
    }

    /// The journal seq through which a closed configuration's own rule has
    /// committed, while it has not committed its end (`None`: it has; a
    /// singleton's rule never gates).
    fn cfg_span_cap(&self, span: &CfgSpan) -> Option<u64> {
        if span.singleton {
            return None;
        }
        let through = self.backed_through(span.backups.iter().copied());
        (through < span.end).then_some(through)
    }

    /// The configuration the held lease says rows are committed by, or
    /// `None` while the node holds none (or an epoch's hold: its rule is
    /// the hold's).
    fn lease_cfg(&self) -> Option<CfgSeen> {
        if self.lease.lost || self.lease.epoch_held() {
            return None;
        }
        let (lease, _) = self.lease.held.as_ref()?;
        let backed = lease.ack_policy == AckPolicy::Backup;
        Some(CfgSeen {
            epoch: lease.epoch,
            backups: if backed {
                lease.backups.clone()
            } else {
                Vec::new()
            },
            singleton: lease.singleton && lease.ack_policy == AckPolicy::Local,
        })
    }

    /// Plan 46 M1.1 step 5: notice a change of the held lease's
    /// configuration (a landed reconfiguration or singleton bump, an
    /// adopted re-read of one) and close the configuration it replaced at
    /// the journal's tip: rows before keep the old rule, the change commits
    /// under old and new. Run where a lease lands and at the start of every
    /// event, before a row is journaled under the new configuration.
    pub(crate) fn sync_cfg_spans(&mut self, replica: &dyn Replica) {
        let cur = self.lease_cfg();
        match (cur, self.ack.cfg_seen.take()) {
            (None, _) => {
                self.ack.spans.clear();
                self.ack.tenure_start = None;
            }
            (Some(c), Some(seen)) if seen.epoch == c.epoch => {
                if seen != c {
                    let tip = replica.journal_tip();
                    if let Some(flip) = self.ack.flip_tip.take().filter(|f| *f < tip) {
                        // A singleton's configuration until the test turned,
                        // the archive's from there to the landing.
                        if seen.singleton {
                            self.ack.spans.push(CfgSpan {
                                backups: Vec::new(),
                                singleton: true,
                                end: flip,
                            });
                            self.ack.spans.push(CfgSpan {
                                backups: Vec::new(),
                                singleton: false,
                                end: tip,
                            });
                        } else {
                            self.ack.spans.push(CfgSpan {
                                backups: seen.backups,
                                singleton: false,
                                end: tip,
                            });
                        }
                    } else {
                        self.ack.spans.push(CfgSpan {
                            backups: seen.backups,
                            singleton: seen.singleton,
                            end: tip,
                        });
                    }
                    self.stats.cfg_spans_closed += 1;
                }
                self.ack.flip_tip = None;
                self.ack.cfg_seen = Some(c);
            }
            (Some(c), _) => {
                self.ack.spans.clear();
                self.ack.flip_tip = None;
                // A tenure starts: the lease was not held before (the
                // process's first, or let go and taken again) or is of
                // another epoch. Whatever the journal holds from before is
                // an earlier tenure's.
                self.ack.tenure_start = self
                    .lease
                    .held
                    .as_ref()
                    .map(|(l, _)| (l.config_version, replica.journal_tip()));
                self.ack.cfg_seen = Some(c);
            }
        }
        self.note_singleton_flip(replica);
        if !self.ack.spans.is_empty() {
            let spans = std::mem::take(&mut self.ack.spans);
            self.ack.spans = spans
                .into_iter()
                .filter(|s| self.cfg_span_cap(s).is_some())
                .collect();
        }
    }

    /// Plan 46 M1.4 step 2: this node owns the hold of a continuation epoch
    /// with at least one member besides itself, and answers by that group's
    /// rule ([`CommitRule::Group`] on [`GroupOn::Hold`]). A one-member
    /// epoch's hold is `Final`; `Config::hold_answers_uncommitted` keeps
    /// today's answer from the owner's disk (the structural fail-before).
    pub(crate) fn hold_group(&self) -> bool {
        self.lease.epoch_held()
            && !self.cfg.hold_answers_uncommitted
            && self.pr.epoch_members.len() >= 2
    }

    /// Plan 46 M1.4 step 4 (the sim's oracle): the members of the epoch this
    /// node holds now, with those that acknowledged part of its journal
    /// (`hold_acked`): `(epoch, members)`. `None` outside a hold.
    pub fn hold_roster(&self) -> Option<(Epoch, Vec<NodeId>)> {
        if !self.lease.epoch_held() {
            return None;
        }
        let epoch = self.lease.epoch()?;
        let mut m = self.pr.epoch_members.clone();
        if self.ack.hold_epoch == Some(epoch) {
            m.extend(self.ack.hold_acked.keys().copied());
        }
        m.sort_unstable();
        m.dedup();
        Some((epoch, m))
    }

    /// The highest journal seq any member of the epoch has acknowledged
    /// holding (`hold_acked`), of the epoch this node owns now.
    fn hold_acked_through(&self) -> u64 {
        if self.ack.hold_epoch != self.lease.epoch() {
            return 0;
        }
        self.ack.hold_acked.values().copied().max().unwrap_or(0)
    }

    fn durable_jseq_cfg(&self) -> u64 {
        if self.hold_group() {
            // Plan 46 M1.4 step 2: an epoch hold with two or more members
            // is a group of the owner and one member: a row is committed
            // once a member's store holds it (`StreamAheadAck`), the highest
            // `hold_acked` of any member.
            return self.hold_acked_through();
        }
        if self.lease.epoch_held() {
            // Plan 30 §M10 (found by `node-leave` on M9's round 2): a
            // continuation epoch's hold acknowledges locally — nothing
            // ships and nothing is backed during an epoch, and the claim
            // rule carried the lease in only with every backup a member
            // (which takes nothing over while the epoch is open). Gating
            // here on the held lease's backups, a candidate or the
            // shipped watermark parked every acknowledgement for the
            // epoch's lifetime: the FUSE caller timed out, retried, and
            // met its own first attempt (`EEXIST`). What is left of this
            // branch is a one-member epoch's hold (`Final`: no member to
            // wait for) and the knob `hold_answers_uncommitted`.
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
    /// Plan 46 M1.1: the journal seq through which this tenure's
    /// configuration has committed its rows, for a copy another sequencer
    /// counts on (a delegate's row this root appended,
    /// `DelegateStreamAck.committed`): [`Self::durable_jseq`], except that a
    /// lone root's journal (`u64::MAX` outside an epoch hold) commits
    /// nothing beyond the archive (`shipped_through`). An epoch hold's
    /// journal counts by the member-acked seq (plan 46 M1.4 step 3): a
    /// row is committed once a member's store holds it
    /// ([`Self::hold_acked_through`]), as the hold's own answers are.
    pub(crate) fn config_committed_jseq(&self) -> u64 {
        if self.hold_group() {
            return self.hold_acked_through();
        }
        if self.lease.epoch_held() {
            // A `Final` hold (a one-member epoch, or the knob
            // `hold_answers_uncommitted`): its journal is the answer's own
            // commit, and there is no member to count by.
            return u64::MAX;
        }
        match self.durable_jseq() {
            u64::MAX => self.ack.shipped_through,
            d => d,
        }
    }

    /// Plan 46 M1.2: this root's configuration commits by the archive alone
    /// (no backup is listed, outside an epoch hold): a delegate's rows it
    /// appended commit at its next ship, not as an acknowledgement comes in
    /// (`DelegateStreamAck.archive_bound`). A root with a live delegate
    /// generation is no singleton (ADR-50, S10-I4), so this is the archive
    /// wait of a root with peers and no backup, not only a singleton's.
    pub(crate) fn config_archive_bound(&self) -> bool {
        self.lease.backups().is_empty() && !self.lease.epoch_held()
    }

    /// Plan 46 M1.1 step 5: the lease this node holds records a singleton's
    /// configuration (no peer within the budget, no backup).
    fn held_singleton(&self) -> bool {
        self.lease
            .held
            .as_ref()
            .is_some_and(|(l, _)| l.singleton && l.ack_policy == AckPolicy::Local)
    }

    /// What `status` shows of the roster object, with the core's own
    /// verdict ([`Self::singleton_now`]) as its `singleton`.
    pub fn roster_view(&self) -> super::roster::RosterView {
        let mut view = self.roster.view(self.cfg.node_id);
        view.singleton = self.singleton_now();
        view
    }

    /// The singleton test (plan 46 D3, ADR-50): this node answers its own
    /// clients from its disk alone only if it is no peer's lease holder to
    /// be: no candidate is being brought up, its roster object says no
    /// other node could write ([`Roster::singleton`]: a complete read lists
    /// nobody else, nothing newer is incomplete, no lapse is newer than the
    /// read, nobody unlisted has messaged or linked), and no other node
    /// holds a live generation of this root's (S10-I4: such a node is a
    /// peer whatever the roster says, and it takes over with its tail).
    /// Judged as of the current event from the inputs the core received,
    /// never from a cache: it is what triggers a bump of the lease's
    /// configuration, what the claim stamps, and one of the two conditions
    /// of answering alone.
    pub(crate) fn singleton_now(&self) -> bool {
        self.ack.candidate.is_none()
            && self.roster.singleton(self.cfg.node_id)
            && !self
                .dl
                .gens
                .values()
                .any(|g| g.node != self.cfg.node_id && !g.ended)
    }

    /// Plan 46 M1.1 step 5 (C8 conditions 1 and 3), ADR-50: a message from
    /// `node` is a singleton-test event, whatever its kind: a node that
    /// writes to this one is a peer, linked, whatever the roster read in
    /// hand says. Called at the one door every peer message passes, before
    /// any row of the message is journaled; the lease's bump follows in the
    /// same event.
    pub(crate) fn note_heard(&mut self, now: Now, node: NodeId, replica: &dyn Replica) {
        if node != self.cfg.node_id && node != 0 && self.roster.note_heard(node, now.wait) {
            self.singleton_maintain(replica);
        }
    }

    /// The event in which the singleton test turns against a lease that
    /// says singleton ends the singleton configuration in the journal: the
    /// tip is noted (once) for [`Self::sync_cfg_spans`].
    fn note_singleton_flip(&mut self, replica: &dyn Replica) {
        if self.ack.flip_tip.is_none()
            && !self.lease.lost
            && !self.lease.epoch_held()
            && self.held_singleton()
            && self.lease.backups().is_empty()
            && !self.singleton_now()
        {
            self.ack.flip_tip = Some(replica.journal_tip());
        }
    }

    /// Plan 46 M1.1 step 5: the singleton test flipped against the lease's
    /// configuration - ask for the bump (a reconfiguration CAS of the same
    /// backup set that moves the flag), in the event that flipped it.
    fn singleton_maintain(&mut self, replica: &dyn Replica) {
        self.note_singleton_flip(replica);
        let Some((lease, _)) = self.lease.held.as_ref() else {
            return;
        };
        if self.lease.lost
            || self.lease.epoch_held()
            || lease.ack_policy != AckPolicy::Local
            || !lease.backups.is_empty()
            || self.ack.reconfig.is_some()
            || self.ack.reconfig_wanted.is_some()
        {
            return;
        }
        if self.singleton_now() != lease.singleton {
            self.ack.reconfig_wanted = Some(Vec::new());
        }
    }

    fn durable_jseq_held(&self) -> u64 {
        let committed = self.lease.backups();
        if committed.is_empty() {
            // Nothing is acknowledged on this disk alone unless the node is
            // a singleton (ADR-50): its roster object lists no
            // write-eligible node but itself and nothing contradicts it
            // (`singleton_now`: no incomplete read, no lapse newer than the
            // read, no unlisted node that messaged or linked, no live
            // foreign generation, no candidate). Such an acknowledgement
            // would rest on nobody's disk but ours, and a peer that takes
            // the lease over would not have it. A node with any
            // write-eligible peer, near, far, unlinked, partitioned or
            // unmounted, waits for the archive (`shipped_through`) or its
            // group instead. Unknown never means alone: before the first
            // complete read, after a read that could not be completed and
            // across the node's own lapse it is no singleton.
            //
            // Plan 46 M1.1 step 5: and the configuration the lease records
            // must be a singleton's. The test above is the event's; the
            // rule changes only at the lease's bump, which commits under
            // old and new (`sync_cfg_spans`): a peer that left does not make
            // the next row lone before the bump has landed.
            return if self.singleton_now() && self.held_singleton() {
                u64::MAX
            } else {
                self.ack.shipped_through
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

    /// Plan 43 C2 (§5.1): journal seq `jseq` is in the log — at or below the
    /// shipped watermark (the core's, or the replica's `shipped`), or one of
    /// the rows shipped past it. What an `Archive` answer waits for.
    pub(crate) fn archive_covers(&self, jseq: u64, shipped: u64) -> bool {
        jseq <= self.ack.shipped_through.max(shipped) || self.ack.shipped_rows.contains(&jseq)
    }

    /// Plan 43 C2 (§5.2): an answer resting on journal seq `jseq` may leave
    /// now. The archive carries it, or this node holds a continuation
    /// epoch with no member to wait for (never wait for S3 there: S3 is what
    /// the epoch is without; with members the hold is a group, plan 46 M1.4,
    /// and the row waits for a member's `StreamAheadAck`), or the gate is on and the
    /// group holds it. With the gate off the root is lone: its own client's
    /// answer leaves (as `Lone`), another node's (`remote`) waits for the
    /// archive — the lone shortcut is never taken for a remote, whatever
    /// class the row was parked under (a `RootGroup` row whose backup set
    /// emptied while it waited included).
    pub(crate) fn park_durable(&self, jseq: u64, remote: bool, shipped: u64) -> bool {
        if self.archive_covers(jseq, shipped) || (self.lease.epoch_held() && !self.hold_group()) {
            return true;
        }
        if self.left_by_hold(jseq) {
            // Plan 46 M1.4 step 2: journaled before or in a hold that has
            // ended, and its acks are forgotten: the archive, or the group
            // of the configuration in force, commits it. Never a lone
            // node's answer on a row the hold left uncommitted: a lone root
            // has no group, and what a closed configuration still owes
            // (`durable_jseq`) is not a second copy of it.
            return !self.root_lone()
                && self.durable_jseq() != u64::MAX
                && self.durable_covers(jseq);
        }
        if self.durable_jseq() != u64::MAX {
            return self.durable_covers(jseq);
        }
        !remote
    }

    /// [`Self::park_durable`] for a parked answer: the journal seq `jseq` is
    /// durable *and*, when the answer rests on a delegation row (`gen != 0`),
    /// that row is committed by its group or the log
    /// ([`Self::completed_row_settled`]). `park_durable` knows a journal seq
    /// only, and a lone root's own client shortcut (`!remote`) would
    /// release at once an answer resting on a delegate's row this disk
    /// alone holds (the I1 class of `Core::answer_need_of`).
    pub(crate) fn park_released(
        &self,
        what: &super::readindex::ParkedWhat,
        jseq: u64,
        shipped: u64,
        replica: &dyn Replica,
    ) -> bool {
        if !self.park_durable(jseq, what.to_remote(self.cfg.node_id), shipped) {
            return false;
        }
        match what {
            super::readindex::ParkedWhat::Reply { rid, .. }
            | super::readindex::ParkedWhat::Finish { rid, .. } => {
                self.completed_row_settled(*rid, replica)
            }
            _ => true,
        }
    }

    /// Plan 43 C2 (§5.1): the journal seq an answer of class `class` to
    /// `asker`, evaluated at `position`, must wait for, or `None` when it may
    /// leave now: [`Self::ack_need`] for this node's own client, and for
    /// another node the same except that a lone root's answer waits for the
    /// archive ([`Self::park_durable`]). A non-stealable answer never waits.
    pub(crate) fn answer_need(
        &self,
        position: &constellation_meta::Position,
        class: constellation_meta::RowCommit,
        asker: Asker,
        shipped: u64,
    ) -> Option<u64> {
        if class.is_final() {
            return None;
        }
        let jseq = position.pending.map(|p| p.jseq)?;
        (!self.park_durable(jseq, asker == Asker::Remote, shipped)).then_some(jseq)
    }

    /// Plan 43 C2 (§2.1): this node holds the root lease, usably, as a lone
    /// sequencer: not lost, no continuation epoch, not under `ack=s3`, and
    /// nothing gates its answers (`durable_jseq` is `u64::MAX`: no listed or
    /// doubted backup, and the node is a singleton). Its
    /// own node's clients are answered from its disk (`Lone`), every other
    /// node from the archive.
    ///
    /// Lone means the node is a singleton ([`Self::singleton_now`], ADR-50):
    /// its roster lists no write-eligible node but itself, judged as of the
    /// current event. A node with any write-eligible peer, however far or
    /// unlinked, answers under a group or the archive.
    pub(crate) fn root_lone(&self) -> bool {
        self.lease.held.is_some()
            && !self.lease.lost
            && !self.lease.epoch_held()
            && self.lease.ack_policy() != AckPolicy::S3
            // The configuration's own rule (plan 46 M1.1 step 5): a row
            // journaled under the singleton's configuration is a singleton
            // row from the moment the lease says so, though what a closed
            // configuration still owes keeps it from being answered
            // (`durable_jseq`: the answer waits, the stamp does not
            // change).
            && self.durable_jseq_cfg() == u64::MAX
    }

    /// Plan 46 M1.1 step 2: the one lookup of the commit rule of `stream`'s
    /// configuration, for an answer to `asker`: from the configuration (the
    /// lease, or the generation's group), and for the singleton from the
    /// event's own assessment ([`Self::root_lone`]),
    /// never from what an earlier row was given.
    pub fn rule_of(&self, stream: RuleStream, asker: Asker) -> CommitRule {
        match stream {
            RuleStream::Root => self.root_rule(asker),
            RuleStream::Delegate(gen) => self.deleg_rule(gen, asker),
        }
    }

    /// Plan 46 M1.4 step 2: journal seq `jseq` was journaled before or in a
    /// hold that has ended and the archive has not carried it yet: its
    /// answer waits for the archive or the group ([`Self::park_durable`]),
    /// and the row in this node's journal is no outcome of the log.
    pub(crate) fn left_by_hold(&self, jseq: u64) -> bool {
        !self.lease.epoch_held() && self.ack.hold_end_tip.is_some_and(|t| jseq <= t)
    }

    /// The `config_version` of the lease this node holds; while it holds none
    /// (a claim not yet adopted, a lease lost), of the one it last held (0:
    /// never).
    pub(crate) fn root_config_version(&self) -> u64 {
        self.lease
            .held
            .as_ref()
            .map_or(self.lease.last_config_version, |(lease, _)| {
                lease.config_version
            })
    }

    /// Plan 46 M1.1 step 2: the commit rule of this node's root
    /// configuration, for an answer to `asker`, looked up once. A
    /// continuation epoch's hold with a member is the group of the owner and
    /// one member (plan 46 M1.4), without one `Final`; under `ack=s3` the group has no
    /// member and the archive carries it; with a listed (or doubted) backup
    /// the group is the root's; while the node is no singleton (a candidate, or any write-eligible peer,
    /// ADR-50) the archive; and otherwise nothing
    /// but this node's disk: the singleton (a node whose roster lists no write-eligible peer), to its own node's client. A
    /// lone root answers another node's forward from the archive: a remote
    /// is never answered on the lone root's disk alone (§5.1's table, the
    /// `Remote` column).
    pub fn root_rule(&self, asker: Asker) -> CommitRule {
        let version = self.root_config_version();
        let archive = CommitRule::Group {
            version,
            members: 0,
            on: GroupOn::Archive,
        };
        if self.hold_group() {
            // Plan 46 M1.4 step 2 (Q1): the epoch is the configuration, so
            // its number is the version; one member's copy commits a row.
            CommitRule::Group {
                version: self.lease.epoch().unwrap_or(version),
                members: 1,
                on: GroupOn::Hold,
            }
        } else if self.lease.epoch_held() {
            CommitRule::Final
        } else if self.lease.ack_policy() == AckPolicy::S3 {
            archive
        } else if !self.lease.backups().is_empty() || self.doubted_backups().next().is_some() {
            CommitRule::Group {
                version,
                members: self.lease.backups().len() + self.doubted_backups().count(),
                on: GroupOn::Root,
            }
        } else if !self.root_lone() {
            // (Plan 43 C2f review, D5: nothing gating but the node not lone —
            // the lease released, lost, not held — is no singleton answer: a
            // fast-path row journaled now waits, and finds the gate off.)
            archive
        } else if asker == Asker::Remote {
            archive
        } else {
            CommitRule::Singleton { version }
        }
    }

    /// Plan 46 M1.1 step 7: the rule of an answer this node gives as the
    /// root sequencer to `asker`, as the commit a row and an answer carry
    /// ([`Self::root_rule`]). The same decision as [`Self::ack_need`]
    /// (`durable_jseq`), named.
    pub fn root_commit(&self, asker: Asker) -> constellation_meta::RowCommit {
        self.root_rule(asker).into()
    }

    /// Plan 43 C1-2: the class a journal row this node writes now is tagged
    /// with, for the scope of the journaling call (the guard): what an answer
    /// to the row's own client waits for ([`Self::root_commit`]). The
    /// class is fixed at the journaling; only a row answered with the gate
    /// off is re-tagged `Lone` (`complete_ready`). Another node's op:
    /// [`Self::journal_class_for`].
    pub(crate) fn journal_class(&self) -> constellation_meta::PendingClassGuard {
        self.journal_class_for(Asker::Own)
    }

    /// Plan 43 C2 (§2.1): [`Self::journal_class`] for a row whose answer goes
    /// to `asker`: a lone root journals another node's op `Archive`, so that
    /// at its deposition the row goes to the member copy, never to the
    /// replay queue (the requester's retry by rid is the op's one copy, H1).
    pub(crate) fn journal_class_for(&self, asker: Asker) -> constellation_meta::PendingClassGuard {
        constellation_meta::PendingClass::set(self.root_rule(asker).into())
    }

    /// Who an answer to `node`'s request goes to.
    pub(crate) fn asker_of(&self, node: NodeId) -> Asker {
        if node == self.cfg.node_id {
            Asker::Own
        } else {
            Asker::Remote
        }
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
    ) -> Result<constellation_meta::RowCommit, constellation_meta::MetaError> {
        Ok(match replica.journal_class_of_rid(rid) {
            // Plan 46 M1.1 step 5: a row keeps the rule it was journaled
            // under. A gated row whose group is gone is not re-tagged
            // `Lone`: the configuration that replaced it commits under old
            // and new ([`Self::sync_cfg_spans`]), so the row is answered
            // when its own rule has committed it (`durable_jseq`).
            Some((_, class)) if class.is_singleton() => class,
            Some((_, class)) if class.is_archive() => class,
            Some((
                _,
                class @ constellation_meta::RowCommit::Group {
                    config_version,
                    on: GroupOn::Root,
                },
            )) => {
                // A group row of the configuration in force that lists no
                // member (the successor's journal holds the predecessor's
                // tail as rows of its own version, plan 46 M1.1 step 4's
                // note): no group stands behind it now, and the answer is
                // named for what it rests on, this disk.
                if self.lease.held.is_some()
                    && self.held_singleton()
                    && config_version == self.root_config_version()
                    && self.lease.backups().is_empty()
                    && self.doubted_backups().next().is_none()
                {
                    constellation_meta::RowCommit::Singleton { config_version }
                } else {
                    class
                }
            }
            // Plan 43 C2f review: a row of a continuation epoch's hold is
            // non-stealable only while the hold lasts. After it, this node's
            // own client is answered as the node is now: on its disk alone
            // only if it is lone (the row re-tagged `Lone`, so its strand
            // follows the answer), from the archive otherwise (`flex-crash`
            // seed 39's shape, for the own client).
            Some((first, class))
                if (class.is_final() || class.is_hold()) && !self.lease.epoch_held() =>
            {
                // Plan 46 M1.4 step 2 fix: a row the ended hold left
                // uncommitted waits for the archive or the group
                // (`park_durable`), lone or not; its label is the wait's.
                if self.root_lone() && !self.left_by_hold(first) {
                    replica
                        .reclass_journal_tx(
                            first,
                            constellation_meta::RowCommit::Singleton {
                                config_version: self.root_config_version(),
                            },
                        )
                        .inspect_err(|error| {
                            tracing::warn!(node = self.cfg.node_id, ?rid, %error, "could not re-tag a hold's row answered alone: EIO");
                        })?;
                    constellation_meta::RowCommit::Singleton {
                        config_version: self.root_config_version(),
                    }
                } else {
                    self.ended_hold_label(first, class, false)
                }
            }
            Some((_, class)) if class.is_final() || class.is_hold() => class,
            Some((
                _,
                constellation_meta::RowCommit::Group {
                    on: GroupOn::Delegate,
                    ..
                },
            ))
            | None => self.root_commit(Asker::Own),
            // (The variants the guarded arms above cover: no wildcard, so a
            // new one fails to compile here.)
            Some((
                _,
                constellation_meta::RowCommit::Singleton { .. }
                | constellation_meta::RowCommit::Final
                | constellation_meta::RowCommit::Group {
                    on: GroupOn::Archive | GroupOn::Hold,
                    ..
                },
            )) => self.root_commit(Asker::Own),
        })
    }

    /// The label of an answer on the row `first` of an ended hold (tagged
    /// `class`, `Hold` or `Final`), which the row's tag is not changed to
    /// (a deposition keeps the hold's rows; an archive tag would let it drop
    /// the only copy of an answered row): the rule that commits the answer
    /// as [`Self::park_durable`] decides it now. The group of the
    /// configuration in force already holds the row: the row's own class,
    /// which that group stands behind. Otherwise the answer waits for the
    /// archive, labelled so; if the group comes to hold the row while it
    /// waits, it leaves with the row's class ([`Self::released_label`]).
    /// `remote`: the answer goes to another node.
    pub(crate) fn ended_hold_label(
        &self,
        first: u64,
        class: constellation_meta::RowCommit,
        remote: bool,
    ) -> constellation_meta::RowCommit {
        if !self.archive_covers(first, 0)
            && self.durable_jseq() != u64::MAX
            && self.park_durable(first, remote, 0)
        {
            class
        } else {
            constellation_meta::RowCommit::as_archive(class, self.root_config_version())
        }
    }

    /// The label of a parked answer labelled `class` as it leaves: the
    /// archive-bound label of a row an ended hold left
    /// ([`Self::ended_hold_label`]) stays only while the archive is what
    /// carries the row; released by the group instead, the answer names
    /// the row's tag (no tag is changed, and a label never says archive
    /// for an answer the archive did not release).
    pub(crate) fn released_label(
        &self,
        replica: &dyn Replica,
        rid: Rid,
        class: constellation_meta::RowCommit,
        shipped: u64,
    ) -> constellation_meta::RowCommit {
        if !class.is_archive() {
            return class;
        }
        match replica.journal_class_of_rid(rid) {
            Some((first, tag))
                if (tag.is_final() || tag.is_hold()) && !self.archive_covers(first, shipped) =>
            {
                tag
            }
            _ => class,
        }
    }

    /// Plan 43 C2 (§5.1): [`Self::journaled_answer_class`] for an answer to
    /// `asker`. Another node is never answered on this disk alone: a gated
    /// row keeps its class with the gate off (its answer waits for the
    /// archive, [`Self::park_durable`], instead of a `Lone` re-tag), and a
    /// `Lone` row — another node's op journaled while nothing gated, before
    /// C2 — is answered from the archive, re-tagged `Archive` first when the
    /// op is another node's (so its strand follows the answer: a member
    /// copy, not a replay). A row of this node's own op answered to another
    /// node (a relay) keeps its `Lone` tag: its own client was answered on
    /// it.
    /// `Err`: the store failed the re-tag ([`Self::eio_unless_recorded`]).
    pub(crate) fn journaled_answer_class_for(
        &self,
        replica: &dyn Replica,
        rid: Rid,
        asker: Asker,
    ) -> Result<constellation_meta::RowCommit, constellation_meta::MetaError> {
        if asker == Asker::Own {
            return self.journaled_answer_class(replica, rid);
        }
        // Plan 43 C2f review (§5.2, Q3): in a continuation epoch's hold the
        // answer leaves at once (never a wait for S3 there) and the hold
        // vouches for it: non-stealable, whatever class the row was
        // journaled with before the hold, and the row re-tagged so (what a
        // deposition or `repair drop-held` does with it follows the answer).
        if self.hold_group() {
            // Plan 46 M1.4 step 2: the answer waits for a member's copy
            // (`park_durable`); the row is the hold group's, tagged so (what
            // a deposition does with it follows the answer). A row of an
            // earlier configuration answered here is re-tagged to the hold's.
            let want: constellation_meta::RowCommit = self.root_rule(asker).into();
            if let Some((first, class)) = replica.journal_class_of_rid(rid) {
                if class != want {
                    replica.reclass_journal_tx(first, want).inspect_err(|error| {
                        tracing::warn!(node = self.cfg.node_id, ?rid, %error, "could not re-tag a row answered in a hold: EIO");
                    })?;
                }
            }
            return Ok(want);
        }
        if self.lease.epoch_held() {
            if let Some((first, class)) = replica.journal_class_of_rid(rid) {
                if !class.is_final() {
                    replica
                        .reclass_journal_tx(first, constellation_meta::RowCommit::FINAL)
                        .inspect_err(|error| {
                            tracing::warn!(node = self.cfg.node_id, ?rid, %error, "could not re-tag a row answered in a hold: EIO");
                        })?;
                }
            }
            return Ok(constellation_meta::RowCommit::Final);
        }
        Ok(match replica.journal_class_of_rid(rid) {
            Some((first, class)) if class.is_singleton() => {
                if rid.node != self.cfg.node_id {
                    replica
                        .reclass_journal_tx(first, constellation_meta::RowCommit::ARCHIVE)
                        .inspect_err(|error| {
                            tracing::warn!(node = self.cfg.node_id, ?rid, %error, "could not re-tag a row answered from the archive: EIO");
                        })?;
                }
                class.as_archive(0)
            }
            Some((_, class)) if class.is_archive() => class,
            Some((
                _,
                class @ constellation_meta::RowCommit::Group {
                    on: GroupOn::Root, ..
                },
            )) => class,
            // A row of a continuation epoch's hold is non-stealable while
            // the hold lasts; once it ended, the row is one more unshipped
            // row of this tenure, and another node's answer waits for the
            // log (`flex-crash` seed 39: a rename journaled in the hold and
            // held back behind a dead member's chunk, answered at a retry
            // after the hold as the hold's).
            Some((_, class)) if class.is_final() && self.lease.epoch_held() => class,
            Some((first, class)) if class.is_final() || class.is_hold() => {
                self.ended_hold_label(first, class, true)
            }
            Some((
                _,
                constellation_meta::RowCommit::Group {
                    on: GroupOn::Delegate,
                    ..
                },
            ))
            | None => self.root_commit(asker),
            // (The variants the guarded arms above cover: no wildcard.)
            Some((
                _,
                constellation_meta::RowCommit::Singleton { .. }
                | constellation_meta::RowCommit::Final
                | constellation_meta::RowCommit::Group {
                    on: GroupOn::Archive | GroupOn::Hold,
                    ..
                },
            )) => self.root_commit(asker),
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
    ) -> Result<constellation_meta::RowCommit, constellation_meta::MetaError> {
        Ok(
            match constellation_meta::take_last_journaled().map(|r| r.class) {
                Some(constellation_meta::RowCommit::Group {
                    on: GroupOn::Delegate,
                    ..
                }) => self.root_commit(Asker::Own),
                Some(class) => class,
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
        let settled = matches!(
            replica.completed_outcome(rid),
            Ok(Some(constellation_meta::CompletedOutcome::Refused { code }))
                if code == super::client::ABANDONED_CODE
                    || code == constellation_meta::DROPPED_CODE
        );
        Ok(AnswerSource::Sequenced(if replica.outcome_from_log(rid) {
            constellation_meta::RowCommit::ARCHIVE
        } else if settled {
            // A settle's refusal is answered as another node's would be,
            // never re-tagged `Lone` (`Core::settle_refusal`).
            self.journaled_answer_class_for(replica, rid, Asker::Remote)?
        } else {
            self.journaled_answer_class(replica, rid)?
        }))
    }

    /// The emission gate's test (I1, and ADR-52): why `rid`'s answer of
    /// class `class` may not leave now, `None` when it may. Every answer
    /// path decides this itself ([`Self::answer_need_of`]); this is asked
    /// again where the answer leaves, so that no path can emit it
    /// unsettled or on a lease it no longer holds.
    ///
    /// First ADR-52: an answer of class `Singleton` (committed by this
    /// node's disk alone under a time-bounded lease) leaves only if a clock
    /// read taken here finds the lease usable with the margin; the step
    /// that produced it may have read its `now` long ago (a stalled
    /// process, a slow store write) and the lease been taken over since.
    /// The read is taken only for a `Singleton`: nothing else pays it. Then
    /// [`Self::unsettled_hold_of`].
    pub(crate) fn emission_hold_of(
        &self,
        rid: Rid,
        class: constellation_meta::RowCommit,
        replica: &dyn Replica,
    ) -> Option<EmissionHold> {
        // (`lone_delegate`, the negative control of the retired lone
        // delegate, answers on no root lease at all by design.)
        if class.is_singleton()
            && !self.lease.epoch_held()
            && !self.cfg.lone_delegate
            && !self.cfg.stale_lease_hold_off
            && !self.lease.usable(self.fresh_now(), &self.cfg)
        {
            // What it holds is the rid's own journaled row: an answer with
            // none (a refusal nothing executed, an `EIO`) commits nothing
            // and leaves as it is. A row the log carries is committed
            // whatever the lease says.
            if let Some((seq, _)) = replica.journal_class_of_rid(rid) {
                if !self.archive_covers(seq, replica.journal_acked_seq().unwrap_or(0)) {
                    return Some(EmissionHold::StaleLease(seq));
                }
            }
        }
        self.unsettled_hold_of(rid, replica)
            .map(EmissionHold::Unsettled)
    }

    /// The I1 emission gate's delegation half: the journal seq to park
    /// `rid`'s answer on when it rests on a delegation row (`gen != 0`) its
    /// group or the log does not hold ([`Self::unsettled_delegate_row`]),
    /// `None` when it may leave.
    pub(crate) fn unsettled_hold_of(&self, rid: Rid, replica: &dyn Replica) -> Option<u64> {
        // Plan 46 M1.4 step 6a (ADR-53): and a row of this node's own root
        // journal that an earlier tenure left, which only the archive
        // commits. Decided by the row's own state, not by how it came here.
        if let Some(seq) = self.unsettled_root_row(rid, replica) {
            return Some(seq);
        }
        // Rows of generations exist only on a node with a delegation (as the
        // delegate, or the root that appended a delegate's rows): the rest
        // pays nothing.
        if self.cfg.dedup_deleg_rows_unawaited {
            return None;
        }
        if self.dl.mine.is_empty() && replica.holder_append_tip() == 0 {
            // The cheap skip must agree with the real test: a former
            // delegate after `drop_delegate_state` may still hold a gen != 0
            // row above `log_stream_idx`.
            debug_assert!(
                self.unsettled_delegate_row(rid, replica).is_none(),
                "emission pre-check skipped an unsettled delegation row ({rid:?})"
            );
            return None;
        }
        self.unsettled_delegate_row(rid, replica)?;
        Some(
            replica
                .journal_class_of_rid(rid)
                .map(|(first, _)| first)
                .unwrap_or(0),
        )
    }

    /// Plan 46 M1.4 step 6a (ADR-53), the root-row twin of
    /// [`Self::unsettled_delegate_row`]: the journal seq of `rid`'s row in
    /// this node's journal when the row is a gated group of the root
    /// (`Archive` or `Root`) journaled before this tenure began, under an
    /// older configuration, and the archive has not carried it. Those groups'
    /// acks do not survive the tenure gap (a restart, a lease let go and
    /// taken again), so the archive alone commits them. `None`: no such row,
    /// or it may leave. Decided by the row's own state and the tenure's
    /// start, never by a flag set on the way in: every answer path asks it
    /// ([`Self::answer_need_of`], [`Self::completed_row_settled`], the
    /// release, and the emission gate), and only the rows of earlier
    /// tenures wait: the rows of this one are answered by this tenure's rule.
    pub(crate) fn unsettled_root_row(&self, rid: Rid, replica: &dyn Replica) -> Option<u64> {
        if self.cfg.resumed_span_dropped {
            return None;
        }
        let (version, tip) = self.ack.tenure_start?;
        let shipped = replica.journal_acked_seq().unwrap_or(0);
        if self.ack.shipped_through.max(shipped) >= tip {
            return None;
        }
        let (first, class) = replica.journal_class_of_rid(rid)?;
        let constellation_meta::RowCommit::Group {
            config_version,
            on: GroupOn::Archive | GroupOn::Root,
        } = class
        else {
            return None;
        };
        (first <= tip && config_version < version && !self.archive_covers(first, shipped))
            .then_some(first)
    }

    /// [`Self::unsettled_root_row`] as the paths that decide early ask it
    /// (`Config::root_row_emission_only` blinds them, to test the backstop).
    pub(crate) fn unsettled_root_row_early(&self, rid: Rid, replica: &dyn Replica) -> Option<u64> {
        if self.cfg.root_row_emission_only {
            return None;
        }
        self.unsettled_root_row(rid, replica)
    }

    /// Count an answer the emission gate held for a lease a fresh clock read
    /// finds unusable (`Stats::answers_held_stale_lease`). An expected
    /// runtime outcome of a stalled step, unlike
    /// [`Self::note_held_at_emission`]: no assertion.
    pub(crate) fn note_held_stale_lease(&mut self, path: EmissionPath, rid: Rid) {
        self.stats.answers_held_stale_lease += 1;
        match path {
            EmissionPath::Finish => self.stats.held_stale_lease_finish += 1,
            EmissionPath::DelegExec => self.stats.held_stale_lease_deleg_exec += 1,
            EmissionPath::HolderReply => self.stats.held_stale_lease_holder_reply += 1,
            EmissionPath::Designation => self.stats.held_stale_lease_designation += 1,
            EmissionPath::Release => self.stats.held_stale_lease_release += 1,
        }
        tracing::warn!(
            node = self.cfg.node_id,
            ?rid,
            ?path,
            "a singleton answer reached the point it leaves on a lease no longer usable: held"
        );
    }

    /// Count an answer the emission gate held (`Stats::answers_held_at_emission`).
    pub(crate) fn note_held_at_emission(
        &mut self,
        path: EmissionPath,
        rid: Rid,
        replica: &dyn Replica,
    ) {
        self.stats.answers_held_at_emission += 1;
        if self.unsettled_root_row(rid, replica).is_some() {
            self.stats.held_at_emission_root_row += 1;
        }
        match path {
            EmissionPath::Finish => self.stats.held_at_emission_finish += 1,
            EmissionPath::DelegExec => self.stats.held_at_emission_deleg_exec += 1,
            EmissionPath::HolderReply => self.stats.held_at_emission_holder_reply += 1,
            EmissionPath::Designation => self.stats.held_at_emission_designation += 1,
            EmissionPath::Release => self.stats.held_at_emission_release += 1,
        }
        tracing::warn!(
            node = self.cfg.node_id,
            ?rid,
            ?path,
            "an answer resting on an unsettled delegation row reached the point it leaves: held"
        );
        debug_assert!(
            self.emission_hold_expected || self.cfg.root_row_emission_only,
            "an answer on an unsettled delegation row reached emission ({path:?}, {rid:?})"
        );
    }

    /// Send a message to a peer; a `MutateReply` carrying a definitive
    /// outcome is counted under `class` (`Stats::ack_classes`).
    ///
    /// The emission gate (I1, acked implies committed): an answer for `rid`
    /// resting on a delegation row (`gen != 0`) its group or the log does
    /// not hold is parked, not sent, whatever path brought it here
    /// ([`Self::emission_hold_of`]); the release asks the row again
    /// ([`Self::park_released`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_answer(
        &mut self,
        now: Now,
        rid: Rid,
        path: EmissionPath,
        to: NodeId,
        msg: PeerMsg,
        class: constellation_meta::RowCommit,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.send_answer_committed(now, rid, path, to, msg, class, class.gated(), replica, out)
    }

    /// [`Self::send_answer`], with whether the answer says it is committed
    /// given: a gated answer released with the gate off says it is not.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn send_answer_committed(
        &mut self,
        now: Now,
        rid: Rid,
        path: EmissionPath,
        to: NodeId,
        mut msg: PeerMsg,
        class: constellation_meta::RowCommit,
        says_committed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let PeerMsg::MutateReply {
            req,
            outcome,
            base,
            position,
            gen,
            committed,
            ..
        } = &mut msg
        {
            if is_answer(outcome) {
                let hold = self.emission_hold_of(rid, class, replica);
                if let Some(EmissionHold::StaleLease(seq)) = hold {
                    self.note_held_stale_lease(path, rid);
                    let id = self.park_stale_reply(
                        now,
                        seq,
                        to,
                        *req,
                        rid,
                        outcome.clone(),
                        *base,
                        *position,
                        *gen,
                        class,
                        out,
                    );
                    self.keep_parked_completed(rid, replica);
                    if self.lease.lost {
                        // Deposed already: nothing else would release or
                        // abort it; the requester retries by rid.
                        self.abort_park_busy(now, id, replica, out);
                    }
                    return;
                }
                if let Some(EmissionHold::Unsettled(seq)) = hold {
                    self.note_held_at_emission(path, rid, replica);
                    self.park_reply(
                        now,
                        Default::default(),
                        Some(seq),
                        to,
                        *req,
                        rid,
                        outcome.clone(),
                        *base,
                        *position,
                        *gen,
                        class,
                        out,
                    );
                    self.keep_parked_completed(rid, replica);
                    return;
                }
                self.stats.ack_classes[ack_counter(class)] += 1;
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
        let class: constellation_meta::RowCommit = self.root_rule(Asker::Own).into();
        // Plan 43 C2 (§5.4, D5): with the gate off, the fast path answers its
        // own clients on this disk only where this node is lone or holds an
        // epoch; the gate off for any other reason (a release, a handoff) is
        // in doubt there.
        //
        // Plan 46 M1.1 step 5 (C8 condition 2): a singleton's row is the
        // lease's stamp from the bump on, but it is answered alone only when
        // no closed configuration still owes its end (`durable_jseq`: the
        // spans' caps): until the archive carries the journal through the
        // bump, a Singleton row waits like a gated one.
        let lone_ok = (self.root_lone() && self.durable_jseq() == u64::MAX)
            || (self.lease.epoch_held() && !self.hold_group());
        if self.ack.reported != Some((gate, jseq, lost, class, lone_ok)) {
            self.ack.reported = Some((gate, jseq, lost, class, lone_ok));
            replica.set_durable(gate, jseq, lost, class, lone_ok);
        }
    }

    /// Plan 43 C2f review (D5): the replica stops being the holder's
    /// (`set_holder_epoch(0)`, at a release, a handoff, a deposition, an
    /// epoch's close, the mount). First the FUSE fast path is told that this
    /// node may not answer its own clients alone: a write the engine's lease
    /// mirror still admits in the window, journaled under the class published
    /// before, is in doubt (`DurableWait::Unexpected`), never answered as a
    /// lone node's.
    pub(crate) fn clear_holder_epoch(&mut self, replica: &dyn Replica) {
        let lost = self.lease.lost;
        // The archive's rule at the configuration the fast path's rows have
        // been journaled under until now (the version last published, else
        // the lease's): a row journaled in this window still belongs to the
        // tenure whose configuration it is, not to version 0.
        let version = self
            .ack
            .reported
            .and_then(|(_, _, _, published, _)| published.config_version())
            .unwrap_or_else(|| self.root_config_version());
        let class = constellation_meta::RowCommit::group(version, GroupOn::Archive);
        self.ack.reported = Some((false, u64::MAX, lost, class, false));
        replica.set_durable(false, u64::MAX, lost, class, false);
        replica.set_holder_epoch(0);
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

    /// Plan 41 D3: an off-core lane read the lease object for the tenure of
    /// `epoch` while this core was stuck (`Event::RootProbed`), the request
    /// sent at `sent`. Judged as the core judges a lease read of its own:
    /// the tenure seen live is proven live as of `sent`
    /// (`note_s3_liveness`), one seen taken over is deposed here (the finding
    /// of [`Core::on_renew_probe`]); a read for a tenure this core no longer
    /// holds, an older body or a failed read say nothing about the one it
    /// does.
    pub(crate) fn on_root_probed(
        &mut self,
        now: Now,
        epoch: Epoch,
        sent: HonourMs,
        read: crate::root_probe::LeaseRead,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        use crate::root_probe::{judge, LeaseRead, Verdict};
        self.stats.root_probes += 1;
        let mine = self
            .lease
            .held
            .as_ref()
            .filter(|_| !self.lease.lost && !self.lease.epoch_held())
            .map(|(l, _)| l.epoch);
        if mine != Some(epoch) {
            return;
        }
        match judge(read, self.cfg.node_id, epoch) {
            Verdict::Live => {
                self.stats.root_probes_live += 1;
                if sent > self.ack.last_s3_fresh {
                    self.ack.last_s3_fresh = sent;
                }
            }
            Verdict::Deposed => {
                self.stats.root_probes_deposed += 1;
                let (holder, at) = match read {
                    LeaseRead::Held { holder, epoch, .. } => (holder, epoch),
                    _ => (0, 0),
                };
                self.deposed(now, holder, at, epoch, replica, out);
            }
            Verdict::Unknown => {}
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
            let sent = lease.with_granted_delegations();
            let op = self.issue_s3(
                S3Op::LeaseSwap {
                    lease: sent.clone(),
                    tag,
                },
                S3For::MarkGranting,
                out,
            );
            self.ack.marking = Some(op);
            self.ack.marking_sent = Some(sent);
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
                // The body that was sent, and only while it is the newer: a
                // renewal may have been adopted since (its CAS lost to this
                // one and re-read it), which already holds the mark.
                if let Some(sent) = self.ack.marking_sent.take() {
                    if self.lease.advances_held(&sent) {
                        self.lease.renewed(now, sent, tag);
                    }
                }
            }
            S3Result::LeasePut(Err(crate::event::CasFailure::Conflict)) => {
                self.issue_mark_reread(out);
            }
            _ => {}
        }
        let _ = replica;
    }

    /// Read the lease to settle a `MarkGranting` that lost (`marking` is the
    /// read).
    fn issue_mark_reread(&mut self, out: &mut Vec<Action>) {
        let op = self.issue_s3(S3Op::LeaseGet, S3For::MarkGrantingReread, out);
        self.ack.marking = Some(op);
        self.ack.marking_gen = self.lease.held_gen();
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
            // Served before a CAS of ours that landed (a renewal beside the
            // mark): it says nothing of the object now. Read again
            // (`LeaseState::older_read`).
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if cur.holder == self.cfg.node_id
                    && cur.epoch == mine.epoch
                    && self.lease.older_read(now, &cur, &tag, self.ack.marking_gen) =>
            {
                self.issue_mark_reread(out);
            }
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
                self.issue_clear_reread(out);
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
        let sent = lease.without_granted_delegations();
        let op = self.issue_s3(
            S3Op::LeaseSwap {
                lease: sent.clone(),
                tag,
            },
            S3For::ClearMark,
            out,
        );
        self.ack.unmarking = Some(op);
        self.ack.unmark_sent = Some(sent);
        self.stats.mark_clears += 1;
    }

    /// Read the lease to settle a `ClearMark` (`unmarking` is the read).
    fn issue_clear_reread(&mut self, out: &mut Vec<Action>) {
        let op = self.issue_s3(S3Op::LeaseGet, S3For::ClearMarkReread, out);
        self.ack.unmarking = Some(op);
        self.ack.unmark_gen = self.lease.held_gen();
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
                if let Some(sent) = self.ack.unmark_sent.take() {
                    if self.lease.advances_held(&sent) {
                        self.lease.renewed(now, sent, tag);
                    }
                }
                self.ack.mark_inherited = false;
            }
            // A conflict, a failure, an answer lost: the object may be
            // either version. The lease is read before anything else.
            _ => {
                self.ack.unmark_unsettled = true;
                self.issue_clear_reread(out);
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
        let older = matches!(
            &result,
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if self.lease.older_read(now, cur, tag, self.ack.unmark_gen)
        );
        let Some((mine, _)) = self.lease.held.clone() else {
            self.ack.unmark_unsettled = false;
            return;
        };
        match result {
            // Served before a CAS of ours that landed: it cannot tell
            // whether the clear did. Read again (`LeaseState::older_read`).
            S3Result::LeaseGet(Ok(Some(_))) if older => {
                self.issue_clear_reread(out);
            }
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
        use constellation_meta::readdeleg::FloorMark;
        let mark = replica.takeover_floor_pending();
        self.ack.floor_mark = match mark {
            FloorMark::Epoch(e) => Some(e),
            FloorMark::None | FloorMark::Unreadable => None,
        };
        // A mark that cannot be read may be a claim's whose floor is owed:
        // the re-adoption of this node's own lease arms it (fail closed).
        self.ack.floor_mark_unreadable = mark == FloorMark::Unreadable;
        if let Some(until) = replica.load_takeover_floor(now, self.dl_t().floor_span as i64) {
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
        let delegations = !replica.delegation_table().is_empty();
        let dl = self.dl_t();
        // Plan 43 C7-H1: the floor is a wait on this node's wait clock (a
        // forward wall step must not end it early); the cap is on it too.
        let bound = |locks: bool| now.wait.plus(dl.ack_floor(locks, delegations));
        let capped = |bound: WaitMs| cap.map_or(bound, |c| bound.min(c));
        // Plan 30 §M14: lock grants are capped like read delegations and
        // marked the same way, but only new lock grants wait for them —
        // a lock grant still honoured elsewhere does not make an
        // acknowledgement stale — so their (longer) ttl is a quarantine
        // of the lock table's own.
        let locks_bound = bound(true);
        let locks_until = capped(locks_bound);
        let bound_until = bound(false);
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
        if std::mem::replace(&mut self.ack.held_before, self.lease.epoch_held())
            && !self.lease.epoch_held()
        {
            self.ack.hold_end_tip = Some(replica.journal_tip());
        }
        if self
            .ack
            .hold_end_tip
            .is_some_and(|t| t <= self.ack.shipped_through)
        {
            self.ack.hold_end_tip = None;
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
            if !self.lease.epoch_held() {
                self.ack.hold_acked.clear();
                self.ack.hold_epoch = None;
                self.ack.hold_sent.clear();
                self.ack.hold_sent_hi.clear();
                self.ack.hold_resending.clear();
                if let Some(id) = self.ack.hold_resend_timer.take() {
                    self.cancel_timer(id, out);
                }
            } else {
                self.arm_hold_resend(now, out);
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
        self.sync_cfg_spans(replica);
        self.singleton_maintain(replica);
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
        self.backup_peers_in_budget(now)
            .filter(|n| !self.lease.backups().contains(n))
            .collect()
    }

    /// The peers that could back this node now, listed ones included:
    /// linked, listed by the roster in hand as write-eligible (a node linked
    /// after the read is a peer for the singleton test,
    /// [`Roster::link_is_peer`], but never a candidate until a read lists
    /// it: it may be read-only, or a retired node's process), within the RTT
    /// budget and up for the stability window, oldest link first
    /// ([`Self::backup_candidates`] leaves the committed ones out).
    fn backup_peers_in_budget(&self, now: Now) -> impl Iterator<Item = NodeId> {
        let mut eligible: Vec<(WaitMs, NodeId)> = if self.cfg.backup_rtt_budget_ms == 0
            || self.cfg.backups_max == 0
        {
            Vec::new()
        } else {
            self.roster
                .links()
                .values()
                .filter(|l| l.node != self.cfg.node_id && l.connected && self.roster.lists(l.node))
                .filter(|l| {
                    l.rtt_ms
                        .is_some_and(|rtt| rtt <= self.cfg.backup_rtt_budget_ms)
                })
                .filter(|l| {
                    l.since.is_some_and(|since| {
                        now.wait.since(since) >= self.cfg.backup_stable_ms as i64
                    })
                })
                .map(|l| (l.since.unwrap_or(now.wait), l.node))
                .collect()
        };
        eligible.sort();
        eligible.into_iter().map(|(_, n)| n)
    }

    /// Whether a backup could be had from a peer linked within the RTT
    /// budget ([`Roster::link_is_peer`]: a roster node, or a node linked
    /// after the roster read in hand), listed or not. One that sealed the
    /// epoch counts too (it is no candidate, `backup_select`, but it takes
    /// over with its tail). Unknown counts as could-be: before the first
    /// directory snapshot, across this node's own lapse, after a registry
    /// read that could not be completed, and while a node that is not on
    /// the roster has messaged.
    ///
    /// This is no longer the root's lone test: that is
    /// [`Self::singleton_now`], which a peer out of budget or unlinked does
    /// not satisfy. It remains the delegate's (`deleg_rule`), whose peers
    /// are those of the whole filesystem.
    ///
    /// Stability is a matter of time, not of an event: a link that is not
    /// yet up for the stability window, or whose RTT is not yet measured,
    /// counts (candidate selection, [`Self::backup_candidates`], asks more).
    pub(crate) fn backup_could_be_had(&self) -> bool {
        if !self.backups_possible() {
            return false;
        }
        // I1 class 2: lone needs a directory snapshot taken after this
        // node's last lapse was handed (plan 46 D3: no node with peers
        // answers from its disk alone). The links it holds were given
        // before it stopped; newer snapshots were already queued behind the
        // event that resumed it (backup+commit seeds 578, 1942;
        // backup-crash-slow 1416: a root paused for seconds answered on a
        // directory without the peers that came up meanwhile). Knowledge
        // older than the node's own lapse is not knowledge at answer time.
        // Ordered by counters (`Event::Peers`'s `lapses`), not the clock:
        // the wait clock does not count a suspend, and the assessment
        // still never changes with the clock alone. Before the first
        // snapshot nothing is known either.
        //
        // The newest roster read may have been partial: who the peers are
        // is unknown, and unknown counts as peers.
        if self.roster.peers_unknown() {
            return true;
        }
        // Plan 46 M1.1 step 5 (C8 condition 3): a node that asked this one
        // for something is a linked peer until the directory says
        // otherwise.
        // (Only the root's: a delegate's requesters are no peer of the root's
        // configuration.)
        if self.lease.held.is_some() && self.roster.has_heard() {
            return true;
        }
        self.roster.links().values().any(|l| {
            self.roster.link_is_peer(self.cfg.node_id, l)
                && l.rtt_ms
                    .is_none_or(|rtt| rtt <= self.cfg.backup_rtt_budget_ms)
        })
    }

    fn backup_select(&mut self, now: Now, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        // EC2 follow-up (e): a node that sealed the epoch held can never
        // back it: not a candidate. (It still counts for whether a backup
        // could be had: the acknowledgements wait for the archive, not for
        // it, and never leave on this disk alone while it is linked.)
        let epoch = self.lease.epoch();
        self.ack.refused_epoch.retain(|_, e| Some(*e) == epoch);
        let candidates: Vec<NodeId> = self
            .backup_candidates(now)
            .into_iter()
            .filter(|n| !self.ack.refused_epoch.contains_key(n))
            .collect();
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
                answered: false,
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
                    && (unheard >= timeout + if p.answered { grace } else { 0 }
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
            .filter(|n| self.roster.links().get(n).is_some_and(|l| !l.connected))
            .collect();
        for n in down {
            self.drop_backup(now, n, "link down");
        }
        // One that a complete read shows is no longer write-eligible (it
        // left, or is read-only) is no backup either, though its link is up
        // and it keeps answering (a retired node's process, a read-only
        // mount): nothing else removes it. (One whose link is down goes by
        // the rule above.)
        if self.roster.has_complete_read() {
            let ineligible: Vec<NodeId> = self
                .lease
                .backups()
                .iter()
                .copied()
                .filter(|n| !self.roster.lists(*n))
                .filter(|n| self.roster.links().get(n).is_some_and(|l| l.connected))
                .collect();
            for n in ineligible {
                self.drop_backup(now, n, "not write-eligible");
            }
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
        // Plan 46 M1.1 step 5: the singleton flag follows the event's test
        // (a set with a backup is a peer's configuration). A change that
        // moves neither the set nor the flag is no change.
        new.singleton = backups.is_empty() && policy == AckPolicy::Local && self.singleton_now();
        if new.backups == lease.backups
            && new.ack_policy == lease.ack_policy
            && new.singleton == lease.singleton
        {
            return;
        }
        if new.backups == lease.backups && new.ack_policy == lease.ack_policy {
            self.stats.singleton_bumps += 1;
        }
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
            read_gen: 0,
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
                self.sync_cfg_spans(replica);
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
                self.ack.reconfig = Some(Reconfig {
                    read_gen: self.lease.held_gen(),
                    ..r
                });
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
        let older = matches!(
            &result,
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if self.lease.older_read(now, cur, tag, r.read_gen)
        );
        let Some((mine, _)) = self.lease.held.clone() else {
            return;
        };
        match result {
            // Served before a CAS of ours that landed: read again
            // (`LeaseState::older_read`).
            S3Result::LeaseGet(Ok(Some(_))) if older => {
                self.issue_s3(S3Op::LeaseGet, S3For::ReconfigReread, out);
                self.ack.reconfig = Some(Reconfig {
                    read_gen: self.lease.held_gen(),
                    ..r
                });
            }
            S3Result::LeaseGet(Ok(Some((cur, tag))))
                if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
            {
                // Lost the CAS to our own renewal or a `wanted_by` edit:
                // retry against the fresh object.
                self.lease.renewed(now, cur.clone(), tag.clone());
                self.sync_cfg_spans(replica);
                if self.lease_cas_in_flight() || self.ack.unmarking.is_some() {
                    // A CAS of the lease's own is out (a renewal started
                    // beside this reconfiguration, which gates on the same):
                    // two on one version would lose one to the other. The
                    // set is asked for again when it settles
                    // (`issue_reconfig` holds it back as it does a first
                    // attempt).
                    self.ack
                        .reconfig_wanted
                        .get_or_insert(r.lease.backups.clone());
                    self.reconcile_peers_with_lease();
                } else if r.attempts < RECONFIG_ATTEMPTS {
                    let mut new = cur.reconfigured(r.lease.backups.clone(), r.lease.ack_policy);
                    new.singleton = r.lease.singleton;
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
        // An answer as good as the heartbeat's. A backup listed moments
        // ago has acknowledged its catch-up but may not have answered a
        // heartbeat yet (it answers them once listed, at the beat
        // cadence): without this, a lag of its runtime in that first
        // interval dropped it at the plain timeout
        // (`backup-lag-keeps-its-backup` seed 259: listed 82 ms before a
        // 0.6 s lag).
        p.answered = true;
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
        if after > before {
            // Rows became committed: an op awaiting the log whose outcome
            // is a delegate's row this node appended (`holder_append_
            // uncommitted`) is answerable now, as it is when a segment lands
            // (`ship_landed`); nothing else re-checks it before the next
            // ship or batch.
            self.answer_awaiting_log(now, replica, out);
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
            p.answered = true;
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
        let mut subscribers: Vec<NodeId> = self.stream_subscribers();
        // Plan 46 M1.4 step 1: the hold streams to every member of its
        // epoch, subscribed or not (a member answers it with the ack the
        // group's rule will wait for).
        if self.epoch_streams_ahead() {
            for m in self.pr.epoch_members.clone() {
                if m != self.cfg.node_id && !subscribers.contains(&m) {
                    subscribers.push(m);
                }
            }
        }
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
        let base = self.ship.head_seq;
        let hold_group = in_epoch && self.hold_group();
        let members = self.pr.epoch_members.clone();
        // A subscriber outside a hold group's epoch gets only the rows the
        // group has acknowledged (`hold_acked_through`), as committed: an
        // unacknowledged row streamed to it stayed an ungated copy, which
        // a later epoch's marker (a takeover of this hold) strands into a
        // replay of a row the group went on to acknowledge (I5, flex-tight
        // 1267). Its copy of an acknowledged row is gated, and a takeover
        // keeps it (S6).
        let gate_cap = hold_group.then(|| self.hold_acked_through());
        let gated = |n: NodeId| gate_cap.is_some() && !members.contains(&n);
        let mut through = if common > 0 {
            txs[common - 1].last
        } else {
            self.ack.streamed_through
        };
        if let Some(cap) = gate_cap {
            if subscribers.iter().any(|n| gated(*n)) {
                // The common cursor stays where the gated subscribers are;
                // the members ahead of it continue from their own cursors.
                let limit = txs[..common]
                    .iter()
                    .take_while(|t| t.last <= cap)
                    .last()
                    .map_or(self.ack.streamed_through, |t| t.last);
                through = through.min(limit);
            }
        }
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
            let upto = match gate_cap {
                Some(cap) if gated(n) => {
                    upto.min(list.iter().take_while(|t| t.last <= cap).count())
                }
                _ => upto,
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
            // Simulation: every Nth batch carrying a grant goes out as
            // uncommitted, whatever the group holds.
            let mut knob_uncommitted = false;
            if self.cfg.stream_ahead_uncommit_grant_every > 0
                && batch.iter().any(|t| {
                    t.records
                        .iter()
                        .any(|r| matches!(r, constellation_meta::LogRecord::DelegateNode { .. }))
                })
            {
                self.stats.stream_grant_batches += 1;
                if self
                    .stats
                    .stream_grant_batches
                    .is_multiple_of(self.cfg.stream_ahead_uncommit_grant_every)
                {
                    self.stats.stream_grants_uncommitted += 1;
                    knob_uncommitted = true;
                }
            }
            if in_epoch && self.ack.hold_epoch != Some(epoch) {
                self.ack.hold_acked.clear();
                self.ack.hold_sent.clear();
                self.ack.hold_sent_hi.clear();
                self.ack.hold_resending.clear();
                self.ack.hold_epoch = Some(epoch);
            }
            // Only a member acknowledges (and is resent to).
            if in_epoch && members.contains(&n) {
                let hi = self.ack.hold_sent_hi.entry(n).or_insert(0);
                *hi = (*hi).max(reached);
                let q = self.ack.hold_sent.entry(n).or_default();
                q.push_back((reached, now.wait));
                while q.len() > HOLD_SENT_MAX {
                    q.pop_front();
                }
            }
            out.push(Action::Send {
                to: n,
                msg: PeerMsg::StreamAhead {
                    epoch,
                    base,
                    // Committed: held by the group, and every row of the
                    // root's own tenure in it was journaled under a class
                    // that waits for a group or the archive; a lone node's
                    // rows streamed after the gate went off are not.
                    // Plan 46 M1.4 step 4: a hold's batch to a member of its
                    // epoch is committed: the member acknowledges it
                    // (`StreamAheadAck`) once it is durable there, and the
                    // owner's answers wait for that ack (the hold's group),
                    // so the member keeps what it acknowledged and ships it
                    // if it claims the lease. A subscriber outside the
                    // epoch is sent only rows the group acknowledged
                    // (`gate_cap`), so its copy is the group's too.
                    committed: !knob_uncommitted
                        && if in_epoch {
                            hold_group
                        } else {
                            batch.iter().all(|t| {
                                t.records.is_empty()
                                    || t.origin != (0, 0)
                                    || replica.journal_tx_class(t.first).is_some_and(|c| c.gated())
                            })
                        },
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
    }

    /// Plan 46 M1.4 step 2: arm the resend timer for the oldest batch no
    /// member has acknowledged yet (once per batch; the timer re-arms itself
    /// while anything is unacknowledged).
    fn arm_hold_resend(&mut self, now: Now, out: &mut Vec<Action>) {
        if self.ack.hold_resend_timer.is_some() || !self.hold_group() {
            return;
        }
        let Some(oldest) = self
            .ack
            .hold_sent
            .values()
            .filter_map(|q| q.front().map(|&(_, at)| at))
            .min()
        else {
            return;
        };
        let at = oldest.plus(self.cfg.hold_resend_ms.max(1));
        let at = if at > now.wait {
            at
        } else {
            now.wait.plus(self.cfg.hold_resend_ms.max(1))
        };
        let id = self.set_timer_wait(at, Timer::HoldResend, out);
        self.ack.hold_resend_timer = Some(id);
    }

    /// Plan 46 M1.4 step 2: a member that has not acknowledged a batch for
    /// `hold_resend_ms` gets the journal again from what it acknowledged
    /// (one batch per member and round; the member installs it idempotently
    /// and answers with its ack). Without it a batch lost in transit parked
    /// every answer of the hold until the client's deadline: the owner
    /// streams each row once, and the member that never held it never acks.
    pub(crate) fn on_hold_resend_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.ack.hold_resend_timer = None;
        if !self.hold_group() || !self.epoch_streams_ahead() {
            return;
        }
        let due = self.cfg.hold_resend_ms.max(1) as i64;
        let members: Vec<NodeId> = self
            .ack
            .hold_sent
            .iter()
            .filter(|(_, q)| q.front().is_some_and(|&(_, at)| now.wait.since(at) >= due))
            .map(|(n, _)| *n)
            .collect();
        for n in members {
            self.resend_hold_batch(n, now, replica, out);
        }
        self.arm_hold_resend(now, out);
    }

    /// One batch of the journal after what member `n` acknowledged, sent
    /// again (and the member marked as being resent to).
    fn resend_hold_batch(
        &mut self,
        n: NodeId,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(epoch) = self.lease.epoch() else {
            return;
        };
        let tip = replica.journal_tip();
        let acked = self.ack.hold_acked.get(&n).copied().unwrap_or(0).min(tip);
        let txs: Vec<BackupTx> = replica
            .journal_txs_from(acked + 1, self.cfg.backup_batch_rows)
            .into_iter()
            .filter(|t| t.last <= tip)
            .collect();
        let q = self.ack.hold_sent.entry(n).or_default();
        q.clear();
        let Some(last) = txs.last().map(|t| t.last) else {
            self.ack.hold_resending.remove(&n);
            return;
        };
        q.push_back((last, now.wait));
        // The episode runs to the journal's tip as it stands (the rows
        // the answers wait for), whatever was streamed before.
        let hi = self.ack.hold_sent_hi.entry(n).or_insert(0);
        *hi = (*hi).max(last);
        if self.ack.hold_resending.insert(n) {
            *hi = (*hi).max(tip);
        }
        self.stats.hold_resends += 1;
        // Simulation: the knob uncommits the resent batches that carry a
        // grant too (the same count as the first sends).
        let mut knob_uncommitted = false;
        if self.cfg.stream_ahead_uncommit_grant_every > 0
            && txs.iter().any(|t| {
                t.records
                    .iter()
                    .any(|r| matches!(r, constellation_meta::LogRecord::DelegateNode { .. }))
            })
        {
            self.stats.stream_grant_batches += 1;
            if self
                .stats
                .stream_grant_batches
                .is_multiple_of(self.cfg.stream_ahead_uncommit_grant_every)
            {
                self.stats.stream_grants_uncommitted += 1;
                knob_uncommitted = true;
            }
        }
        out.push(Action::Send {
            to: n,
            msg: PeerMsg::StreamAhead {
                epoch,
                base: self.ship.head_seq,
                txs,
                committed: !knob_uncommitted && self.hold_group(),
            },
        });
    }

    /// Plan 46 M1.4 step 1, hold owner: a member holds this node's epoch
    /// journal through `through`. Recorded (the highest per member) only
    /// while this node owns the hold of `epoch` and `from` is another
    /// member of it; anything else is ignored. Nothing answers on it yet.
    pub(crate) fn on_stream_ahead_ack(
        &mut self,
        now: Now,
        from: NodeId,
        epoch: Epoch,
        through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.lease.epoch_held()
            || self.lease.epoch() != Some(epoch)
            || from == self.cfg.node_id
            || !self.pr.epoch_members.contains(&from)
        {
            self.stats.hold_acks_ignored += 1;
            return;
        }
        if self.ack.hold_epoch != Some(epoch) {
            self.ack.hold_acked.clear();
            self.ack.hold_sent.clear();
            self.ack.hold_sent_hi.clear();
            self.ack.hold_resending.clear();
            self.ack.hold_epoch = Some(epoch);
        }
        // A member cannot hold more than the journal has.
        let through = through.min(replica.journal_tip());
        let acked = self.ack.hold_acked.entry(from).or_insert(0);
        *acked = (*acked).max(through);
        self.stats.hold_acks += 1;
        if let Some(q) = self.ack.hold_sent.get_mut(&from) {
            while let Some(&(last, sent)) = q.front() {
                if last > through {
                    break;
                }
                self.stats.hold_ack_wait_ms += (now.wait.0 - sent.0).max(0) as u64;
                q.pop_front();
            }
        }
        // The group acknowledged more: the subscribers outside the epoch
        // are sent what it holds now (`stream_ahead`'s `gate_cap`).
        if self.hold_group()
            && self.epoch_streams_ahead()
            && self
                .stream_subscribers()
                .iter()
                .any(|n| !self.pr.epoch_members.contains(n))
        {
            let tip = replica.journal_tip();
            self.stream_ahead(now, 0, tip, replica, out);
        }
        // A resend carries one batch: while the member's ack is short of
        // what was streamed to it, its ack of the resend asks for the next.
        if self.ack.hold_resending.contains(&from) {
            let hi = self.ack.hold_sent_hi.get(&from).copied().unwrap_or(0);
            let acked = self.ack.hold_acked.get(&from).copied().unwrap_or(0);
            if acked >= hi {
                self.ack.hold_resending.remove(&from);
            } else if self.ack.hold_sent.get(&from).is_none_or(|q| q.is_empty()) {
                self.resend_hold_batch(from, now, replica, out);
                self.arm_hold_resend(now, out);
            }
        }
    }

    /// ADR-51 (K4 per stream), hold owner: a member sealed this hold's stream
    /// for a claimant and refuses its batches. That member is sealed for this
    /// hold; when no member of the epoch can acknowledge any more, the hold
    /// ends as deposed (the answers it parked abort, the clients retry on the
    /// successor, and the epoch closes by the members' rule). The rows it
    /// answered before the seal are in the sealing member's answer; the rows
    /// after were never acknowledged, so never answered.
    pub(crate) fn on_stream_ahead_refused(
        &mut self,
        now: Now,
        from: NodeId,
        epoch: Epoch,
        sealed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.cfg.node_id;
        if !sealed
            || !self.lease.epoch_held()
            || self.lease.epoch() != Some(epoch)
            || from == me
            || !self.pr.epoch_members.contains(&from)
        {
            self.stats.hold_acks_ignored += 1;
            return;
        }
        self.ack.hold_sealed.insert((epoch, from));
        // A member can still acknowledge when it did not seal and its link
        // is up (a link this node has no record of counts as up): one that
        // is down cannot, and with the others sealed the hold would stall.
        let open = self.pr.epoch_members.iter().any(|n| {
            *n != me
                && !self.ack.hold_sealed.contains(&(epoch, *n))
                && self.roster.links().get(n).is_none_or(|l| l.connected)
        });
        if open {
            return;
        }
        self.stats.hold_ended_sealed += 1;
        tracing::warn!(
            node = me,
            epoch,
            "no other member of the epoch can acknowledge this hold (each sealed its stream for a \
             claimant, or is unreachable): the hold ends as deposed"
        );
        // As a claimant's seal of this node's own epoch does (`seal_tail_hold`,
        // case 1): the epoch is sealed here, the journal is kept as the member
        // copy of it, and the node claims the lease again at the next epoch,
        // from that copy. A hold that formed after the claimant's seals (a
        // restarted owner re-adopting its tenure) is thereby the tenure's
        // end, not a stalled epoch no claimant can enter.
        let cv = self
            .lease
            .held
            .as_ref()
            .map_or(self.lease.last_config_version, |(l, _)| l.config_version);
        let first = replica.journal_acked_seq().unwrap_or(0) + 1;
        if replica.backup_seal(epoch) {
            self.bk.sealed = self.bk.sealed.max(epoch);
            if replica.member_copy_put_journal(epoch, cv, first).is_some()
                && !self.bk.member_epochs.contains(&epoch)
            {
                self.bk.member_epochs.push(epoch);
            }
        }
        let carried_mine = self.pr.carried.is_some_and(|c| c.node == me);
        self.deposed(now, 0, epoch + 1, epoch, replica, out);
        self.end_epoch_hold(replica, false);
        // The members wait for the carried lease to move: this node, its
        // owner, owes that move (it closes the epoch once S3 is back).
        self.owe_given_up_move(carried_mine, replica);
        self.nudge(now, out);
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
        // The seal unknown (its read failed at start): no answer, the holder
        // sends again (plan 43 C2e review).
        if !self.backup_sealed_known(replica) || !self.backup_role_known(replica) {
            return;
        }
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
                    if replica.set_backup_role(role).is_err() {
                        // (Not durable: no answer, the holder sends again.)
                        return;
                    }
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
                if replica.set_backup_role(role).is_err() {
                    return;
                }
                self.bk.role = Some(role);
                // (Unreadable: from 0, which only acknowledges less.)
                self.bk.acked = replica.backup_acked(epoch).unwrap_or(0);
                self.bk.held.clear();
                self.bk.candidacy = candidacy;
            }
        }
        self.bk.last_heard = now.wait;
        self.bk.restarted = false;
        self.lease.name_holder(holder);
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
        self.dl_t().lag_threshold as i64
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
        // (Only lapses at or past the lag threshold are recorded: positive.)
        self.dl_t().silence_window(late as u64) as i64
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
            || !self.bk.fetched.is_empty()
            || self.bk.tf.is_some()
            || self.bk.kept.is_some()
            || self.bk.kept_union.is_some()
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
            if self
                .roster
                .links()
                .get(&role.holder)
                .is_some_and(|l| l.connected)
            {
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
        if let Some((lease, _)) = &object {
            if (lease.epoch, lease.config_version) < (role.epoch, role.config_version) {
                // Plan 43 C1-1 (review), I1 class 1: a read older than the
                // role. The role's `(epoch, config_version)` comes from an
                // append, and a holder sends an epoch only after its
                // acquisition CAS landed and a version only after its
                // reconfiguration CAS landed; S3's pair only grows. So the
                // read was answered before the append was sent (a slow
                // reply, a read issued before a new holder listed this
                // node), and what it says about this node's listing is
                // stale: the holder counts what this node acknowledged
                // under the role's pair (`BackupPeer::counted`), and
                // discarding the tail on such a read left rows acknowledged
                // to clients on no backup's disk (backup+commit seeds 1373,
                // 1472 with slow lease reads: an older epoch's read took
                // the "no longer listed" arm below). Not noted either: the
                // observation keeps the newer body (`observe_body`). The
                // watch reads again.
                tracing::debug!(
                    node = self.cfg.node_id,
                    holder = lease.holder,
                    epoch = lease.epoch,
                    read = lease.config_version,
                    role_epoch = role.epoch,
                    role = role.config_version,
                    "a lease read older than the backup role; ignored"
                );
                self.arm_backup_watch(now, out);
                return;
            }
        }
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
                self.lease.note_object(&lease);
                // Plan 43 C1-4 (S4): the lease moved on to a later epoch —
                // a successor took this holder's group over. What this node
                // held of the old epoch may be the last copy of rows the
                // group committed that the successor has not shipped yet
                // (the successor's lease names this group until it has): it
                // is kept as a member copy of the epoch, served to a
                // claimant that fetches it, and dropped when the log
                // carries it. (A tail this node held through a gap is not
                // the group's: only the contiguous hold.)
                if lease.epoch > role.epoch && !self.keep_tail_as_member_copy(role, replica) {
                    self.arm_backup_watch(now, out);
                    return;
                }
                // Not listed any more in the same epoch (a removal, or a
                // candidate that was not listed yet): our tail is void —
                // the holder counts only what a listed backup acknowledged
                // (plan 43 C1-1). Not sealed: a live holder that removed
                // this node may invite it back as a candidate in the same
                // epoch.
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

    /// Plan 43 C1-4: move the contiguous tail this node holds for `role`'s
    /// epoch into the member copy of that epoch (persisted and synced), so
    /// that clearing the backup tail loses nothing a later claimant may need.
    /// `false`: could not persist it; the caller keeps the tail and tries
    /// again.
    pub(crate) fn keep_tail_as_member_copy(
        &mut self,
        role: BackupRole,
        replica: &dyn Replica,
    ) -> bool {
        let Some(acked) = replica.backup_acked(role.epoch) else {
            return false;
        };
        // (Not what the log carries already: rows of the epoch this replica
        // applied through — a tail that outlived a crash before its trim.
        // Nor a delegate's row the root appended, `origin` of a generation:
        // the delegate re-streams it to the next root, whose append names no
        // `TailOf` (a successor's re-ship of it does) — as in the successor's
        // own copy, `apply_backup_tail`. Kept, it outlived every segment that
        // carried it: delegated-holder-cut seed 170. A subscriber's committed
        // stream entry of it retires by its rid then.)
        let shipped = replica.applied_jseq_of(role.epoch);
        let Some(tail) = replica.backup_tail(role.epoch) else {
            return false;
        };
        let txs: Vec<BackupTx> = tail
            .into_iter()
            .filter(|t| {
                t.last <= acked && t.last > shipped && !t.records.is_empty() && t.origin == (0, 0)
            })
            .collect();
        if txs.is_empty() {
            return true;
        }
        if !replica.member_copy_put(role.epoch, role.config_version, &txs) {
            return false;
        }
        self.stats.tails_kept_as_member_copy += 1;
        true
    }

    /// This node backs nobody any more; its tail is void.
    pub(crate) fn backup_role_ends(&mut self, replica: &dyn Replica) {
        self.bk.role = None;
        self.bk.acked = 0;
        self.bk.held.clear();
        replica.backup_clear();
    }

    /// What the takeover's re-ship takes of the predecessor's sealed tail of
    /// `prev_epoch` ([`constellation_meta::TailArgs`]).
    ///
    /// Plan 43 C1-4 (D6): the re-ship is the sealed tail's contiguous
    /// prefix (`bk.tail_through`). Rows past a gap were never acknowledged
    /// (the holder counts contiguous acknowledgements); their clients retry
    /// by rid.
    ///
    /// Plan 46 M1.1 step 4: the adopted rows are journaled in this tenure's
    /// journal, so they carry the configuration in force here: the lease
    /// this node holds now (the claim's version, which the gate runs under).
    /// The predecessor's group committed them (the sealer holds the tail,
    /// write-all), which is what the `Group` rule says and nothing in this
    /// tenure waits for; the version only names the configuration of the
    /// journal they are rows of. (The predecessor's own version per row is
    /// not recorded on a backup: `BackupTx` carries none, and the member
    /// copy keeps one version for the whole tail.) Not the version-0
    /// placeholder.
    ///
    /// NOT the commit configuration of these rows (plan 46 M1.1 step 5):
    /// the version stamped here is the successor's lease version, the
    /// configuration of the journal the rows sit in, while the group that
    /// committed each row is the predecessor's, at a version this stamp does
    /// not name. The old ∪ new reconfiguration rule and the replay must not
    /// read these versions as the rows' commit configurations.
    pub(crate) fn tail_args(&self, prev_epoch: Epoch) -> constellation_meta::TailArgs {
        self.tail_args_through(prev_epoch, self.bk.tail_through)
    }

    /// [`Self::tail_args`] for a tail that reaches `through`.
    fn tail_args_through(&self, prev_epoch: Epoch, through: u64) -> constellation_meta::TailArgs {
        constellation_meta::TailArgs {
            epoch: prev_epoch,
            through,
            config_version: self.bk.role.map_or(0, |r| r.config_version),
            class: Some(constellation_meta::RowCommit::group(
                self.root_config_version(),
                GroupOn::Root,
            )),
        }
    }

    /// The tails the gate re-ships, ascending by epoch (ADR-51: a kept
    /// fetch's union has one per epoch with rows; any other claim one).
    pub(crate) fn gate_tail_args(
        &self,
        gate: &super::lease::PendingGate,
    ) -> Vec<constellation_meta::TailArgs> {
        if self.bk.gate_tails.is_empty() {
            return gate
                .backup_tail_epoch
                .map(|e| self.tail_args(e))
                .into_iter()
                .collect();
        }
        self.bk
            .gate_tails
            .iter()
            .map(|(e, through)| self.tail_args_through(*e, *through))
            .collect()
    }

    /// The takeover's re-ship of the predecessor's tail committed
    /// (`Meta::reship_at_takeover`, one store transaction with the strand;
    /// it also moved the tail into a member copy and cleared it): the
    /// core's half of it.
    pub(crate) fn tail_reshipped(
        &mut self,
        args: constellation_meta::TailArgs,
        report: &constellation_meta::ReshipReport,
        replica: &dyn Replica,
    ) {
        self.stats.tail_beyond_gap_dropped += report.tail_beyond_gap as u64;
        self.stats.tail_done_skipped += report.tail_done as u64;
        self.stats.tail_same_row += report.tail_same_row as u64;
        self.stats.reship_sync_failed += u64::from(report.sync_failed);
        self.stats.reship_duration_ms_max = self.stats.reship_duration_ms_max.max(report.hold_ms);
        // A row of the tail whose rid the log already carried (the holder's
        // upload landed while its backup still listed the row: `backup-crash`
        // seed 1004) is skipped, never re-journaled; counted (`reship.
        // tail_rid_done`), logged by the store, and failed by the sim under
        // strict identity.
        tracing::info!(
            node = self.cfg.node_id,
            prev_epoch = args.epoch,
            applied = report.tail_applied,
            beyond_gap = report.tail_beyond_gap,
            done = report.tail_done,
            "re-shipped the predecessor's backup tail under the new epoch"
        );
        self.stats.backup_tail_applied += report.tail_applied as u64;
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
        self.bk.tail_through = 0;
        self.bk.gate_tails.clear();
        self.bk.gate_tails_kept = false;
        self.bk.gate_tails_from = 0;
    }

    /// The tail alone, as before the strand joined it (the core tests that
    /// drive a re-ship without a gate).
    #[cfg(test)]
    pub(crate) fn apply_backup_tail(
        &mut self,
        prev_epoch: Epoch,
        replica: &dyn Replica,
    ) -> Result<(), String> {
        let args = self.tail_args(prev_epoch);
        let report = replica
            .reship_at_takeover(None, Some(args))
            .map_err(|e| e.to_string())?;
        self.tail_reshipped(args, &report, replica);
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
        if let Some(role) = self.bk.role.filter(|r| r.epoch <= epoch) {
            if role.epoch < epoch {
                // A newer tenure: whoever holds now re-ships what our
                // holder left, from its own copy or ours. Plan 43 C1-4: ours
                // is kept as a member copy of the old epoch until the log
                // carries it (the successor may yet be replaced before it
                // ships); the backup tail itself is void. (Before the trim
                // below, which drops every older epoch of the tail.)
                if self.keep_tail_as_member_copy(role, replica) {
                    self.bk.role = None;
                    self.bk.acked = 0;
                    self.bk.held.clear();
                    replica.backup_clear();
                }
            } else {
                replica.backup_trim(epoch, through, rows);
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

    /// ADR-51 (K4, keyed on the tenure): whether a `StreamAhead` batch of
    /// stream `(epoch, from)` is a hold stream over a tenure this node sealed
    /// for a claimant's kept fetch. Two cases: the sealed holder's own stream
    /// of the sealed epoch (`from == holder && epoch == lease epoch`), and
    /// any hold batch received in an open epoch whose carried lease is a
    /// sealed tenure that has not moved (the hold of a transferee journals at
    /// the next epoch, so the stream key cannot tell it from a successor's
    /// own tenure; the carried lease can). A successor's tenure carries
    /// nothing of the sealed one and is never refused.
    pub(crate) fn sealed_hold_stream(&self, epoch: Epoch, from: NodeId) -> bool {
        if self.bk.sealed_tenures.contains(&(from, epoch)) {
            return true;
        }
        if !self.epoch.open || from == self.cfg.node_id {
            return false;
        }
        let Some(c) = self.pr.carried else {
            return false;
        };
        if !self.bk.sealed_tenures.contains(&(c.node, c.epoch)) {
            return false;
        }
        let moved = self
            .lease
            .last_seen
            .as_ref()
            .is_some_and(|l| l.released || l.holder != c.node || l.epoch != c.epoch);
        !moved
    }

    /// The holder's backups hold `txs`: install them ahead of the log.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_stream_ahead(
        &mut self,
        now: Now,
        from: NodeId,
        epoch: Epoch,
        base: Seq,
        txs: Vec<BackupTx>,
        committed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // ADR-51 (K4, keyed on the tenure): a hold stream of a tenure this
        // node sealed for a claimant's kept fetch is final here. What it
        // answered is the set the claim carries, so nothing more of it is
        // installed or acknowledged; the hold owner is told, and ends its
        // hold when no member can acknowledge. A successor's own tenure (the
        // sealed holder's re-claim, or a member's claim, at a later epoch) is
        // not a hold over the sealed tenure and is never refused.
        if self.sealed_hold_stream(epoch, from) {
            self.stats.stream_ahead_sealed_refused += 1;
            self.stats.streamed_dropped += txs.len() as u64;
            if matches!(self.bk.ahead_next, Some((e, _)) if e == epoch) {
                self.bk.ahead_next = None;
            }
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::StreamAheadRefused {
                    epoch,
                    sealed: true,
                },
            });
            return;
        }
        if self.cfg.pre_s3_streaming
            && self.lease.held.is_none()
            && !self.lease.epoch_held()
            && self.lease.cached_holder == Some(from)
            && epoch == self.ship.max_epoch
            && !self.cursor_free()
        {
            // A job moves the cursor: follow the log first, then these.
            self.bk.ahead_next = None;
            self.park_gapped(from, epoch, base, txs, committed);
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
                if let Some((_, _, _, old, _)) = self.bk.ahead_waiting.pop_front() {
                    self.stats.streamed_dropped += old.len() as u64;
                    self.bk.ahead_next = None;
                }
            }
            self.bk
                .ahead_waiting
                .push_back((from, epoch, base, txs, committed));
            return;
        }
        // Plan 46 M1.4 step 2: in an open epoch the cursor is the store's,
        // not a segment's: no segment will re-sync it while the hold lasts, and
        // the owner's answers wait for this node's ack. A cursor lost to a
        // dropped batch or a restart (or left at an older epoch) restarts
        // after what the store holds of this epoch; the batch must still
        // follow it (anything else is parked as before).
        if self.epoch.open && self.bk.ahead_next.is_none_or(|(e, _)| e != epoch) {
            let lo = replica
                .streamed_tip(epoch)
                .ok()
                .flatten()
                .map_or(1, |t| t + 1);
            // The rows before the batch's first are in the log (a segment
            // carries a prefix of an epoch's journal): the batch continues it.
            let next = match txs.first() {
                Some(t) if t.first > lo && replica.row_in_log(epoch, t.first - 1) => t.first,
                _ => lo,
            };
            self.bk.ahead_next = Some((epoch, next));
        }
        let mut completed: Vec<(constellation_meta::Rid, constellation_meta::KeySet)> = Vec::new();
        // Plan 46 M1.4 step 1: the highest journal seq this call left
        // durable here (installed by a committed store transaction, or held
        // already); an open epoch's member acknowledges it to the owner.
        let mut held_through: Option<u64> = None;
        let mut txs = txs.into_iter();
        while let Some(tx) = txs.next() {
            let Some((e, next)) = self.bk.ahead_next else {
                // The cursor is lost until the next segment re-syncs it.
                let rest: Vec<BackupTx> = std::iter::once(tx).chain(txs).collect();
                self.park_gapped(from, epoch, base, rest, committed);
                break;
            };
            if e != epoch || tx.first < next {
                // Already in the log (or superseded): nothing to do.
                if tx.first < next && e == epoch {
                    // Plan 46 M1.4 step 2 fix: the cursor is past these rows
                    // (an installed batch whose ack was lost: the owner
                    // sends them again from what it saw acknowledged). They
                    // are in the store, so the ack goes out again, or the
                    // owner resends them forever.
                    held_through = held_through.max(Some(tx.last.min(next - 1)));
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
                held_through = held_through.max(Some(tx.last.min(held_tip.unwrap_or(0))));
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
                self.park_gapped(from, epoch, base, rest, committed);
                break;
            }
            if tx.records.is_empty() {
                // A hole (round 2): rows the holder shipped and dropped
                // before streaming past them — in the log at or below
                // `base`, which this node has applied. The cursor steps
                // over them.
                held_through = held_through.max(Some(tx.last));
                self.bk.ahead_next = Some((epoch, tx.last + 1));
                replica.note_streamed(constellation_meta::JournalPos {
                    epoch,
                    jseq: tx.last,
                });
                continue;
            }
            // The log carries it already (a segment named the row a moment
            // before the stream delivered it): nothing to install, and a
            // committed entry installed now would wait for a segment that went
            // by. The cursor steps over it.
            if replica.row_in_log(epoch, tx.last) {
                held_through = held_through.max(Some(tx.last));
                self.bk.ahead_next = Some((epoch, tx.last + 1));
                replica.note_streamed(constellation_meta::JournalPos {
                    epoch,
                    jseq: tx.last,
                });
                continue;
            }
            // A delegate's row the root appended (`origin` of a generation) is
            // committed as the root's own rows are: its group held it when
            // the holder streamed it, and the successor re-ships it under
            // its identity (`TailOf`, `apply_backup_tail`); without a tail the
            // delegate re-streams it to the next root. Stranded with the
            // tenure, it was rolled back here and queued as a `Records`
            // replay of an acknowledged row (placement-hot seed 675: I5).
            match replica.install_streamed(
                epoch,
                from,
                tx.first,
                tx.last,
                &tx.records,
                committed,
                tx.origin,
            ) {
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
                    held_through = held_through.max(Some(tx.last));
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
        // The batch is in this node's store: tell the hold owner how far
        // (the group's rule waits for it; step 2).
        if let (true, Some(through)) = (self.epoch.open, held_through) {
            // ADR-51 (review SHOULD 1): the ack covers every copy through
            // `through`, and the owner answers on it. A copy this member
            // streamed earlier as speculation (before the hold formed; the
            // hold does not send it again) is the group's committed copy
            // from here on: promoted before the ack leaves, or it would be
            // dropped as speculation after the owner's retirement.
            let promoted = if committed {
                match replica.promote_streamed(epoch, from, through) {
                    Ok(n) => {
                        self.stats.streamed_promoted += n as u64;
                        true
                    }
                    Err(error) => {
                        // Unpromoted, the copy is not the group's: no ack.
                        tracing::warn!(node = self.cfg.node_id, %error, "streamed copy not promoted");
                        false
                    }
                }
            } else {
                true
            };
            if promoted {
                out.push(Action::Send {
                    to: from,
                    msg: PeerMsg::StreamAheadAck { epoch, through },
                });
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
            .is_some_and(|(_, _, base, _, _)| *base <= self.ship.head_seq)
        {
            let (from, epoch, base, txs, committed) =
                self.bk.ahead_waiting.pop_front().expect("front");
            self.on_stream_ahead(now, from, epoch, base, txs, committed, replica, out);
        }
        // The log may have closed the gap before parked transactions (or
        // re-synced a lost cursor): install what now follows it; what the
        // log already holds is skipped, what is still ahead of a gap
        // waits again.
        for (from, epoch, base, txs, committed) in std::mem::take(&mut self.bk.ahead_gapped) {
            self.on_stream_ahead(now, from, epoch, base, txs, committed, replica, out);
        }
    }

    /// Keep `txs` for [`Core::retry_stream_ahead`] (the oldest go once
    /// [`AHEAD_WAITING_MAX`] batches wait).
    fn park_gapped(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        base: Seq,
        txs: Vec<BackupTx>,
        committed: bool,
    ) {
        if txs.is_empty() {
            return;
        }
        if self.bk.ahead_gapped.len() >= AHEAD_WAITING_MAX {
            if let Some((_, _, _, old, _)) = self.bk.ahead_gapped.pop_front() {
                self.stats.streamed_dropped += old.len() as u64;
            }
        }
        self.bk
            .ahead_gapped
            .push_back((from, epoch, base, txs, committed));
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
                let up = self
                    .roster
                    .links()
                    .get(&seen.holder)
                    .is_some_and(|l| l.connected)
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
