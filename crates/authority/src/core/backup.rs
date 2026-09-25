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
//! - A successor of an *unexpired* lease whose tenure served strict
//!   reads (`Lease::granted_delegations`, set by one CAS before the first
//!   answer) acknowledges no mutation until
//!   `min(old expiry, marker time + backup_takeover_ms + read_delegation_ttl + 2M)`
//!   — through the M8 quarantine every acknowledgement already waits on.
//!   A tenure that never served a strict read costs its successor nothing.
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
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
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
    pub s3_sent: BTreeMap<OpId, Ms>,
    pub last_s3_fresh: Ms,
    /// The `MarkGranting` CAS in flight.
    marking: Option<OpId>,
    /// What the replica's durability gate was last told.
    reported: Option<(bool, u64, bool)>,
    /// The tenure's marker time, for the successor's floor (see the
    /// module doc); `None` after the floor was set or when none is due.
    pending_floor: Option<(i64, Ms)>,
    /// The journal seq the last stream-ahead covered.
    streamed_through: u64,
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
    /// The highest journal seq any acknowledgement of this tenure may
    /// have rested on (the durable seq's high-water mark). A backup is
    /// removed from the lease only once everything up to here is in the
    /// log or on every backup that stays: an acknowledgement given on
    /// the strength of the removed backup must not become one that rests
    /// on this node's disk alone (the model's `acking_before_the_removal
    /// _cas_lands` counterexample's sibling: removing a backup that holds
    /// acknowledged, unshipped rows).
    acked_hwm: u64,
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
    /// Subscriber: the next journal seq a `StreamAhead` may install for
    /// `(epoch, jseq)` — contiguity with what the log and earlier batches
    /// gave us.
    ahead_next: Option<(Epoch, u64)>,
    /// `ack=s3` fast takeover: since when the known holder has been
    /// silent on P2P.
    holder_silent_since: Option<Ms>,
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
    pub reconfig_in_flight: bool,
}

impl Core {
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
            reconfig_in_flight: self.ack.reconfig.is_some(),
        }
    }

    // ------------------------------------------------------------ start

    /// At start: a persisted backup role (this node backed someone
    /// before it restarted) and a persisted seal are honoured.
    pub(crate) fn backup_start(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.bk.sealed = replica.backup_sealed_epoch();
        if let Some(role) = replica.backup_role() {
            self.bk.role = Some(role);
            self.bk.acked = replica.backup_acked(role.epoch);
            self.bk.last_heard = now;
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

    pub fn last_s3_fresh(&self) -> Ms {
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
    pub(crate) fn note_s3_liveness(&mut self, op: OpId, result: &S3Result) {
        let Some(sent) = self.ack.s3_sent.remove(&op) else {
            return;
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
    /// only with fresh S3 liveness. `false` starts a probe.
    pub(crate) fn strict_answer_allowed(&mut self, now: Ms, out: &mut Vec<Action>) -> bool {
        if self.lease.ack_policy() == AckPolicy::Local && self.lease.backups().is_empty() {
            return true;
        }
        if now.since(self.ack.last_s3_fresh) < self.cfg.backup_takeover_ms as i64 {
            return true;
        }
        self.stats.stale_liveness_refusals += 1;
        self.ship.probe_now = true;
        self.nudge(now, out);
        false
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
        now: Ms,
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
        now: Ms,
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

    /// `acquire_won`: the lease taken over had not expired (a seal or an
    /// `ack=s3` fast takeover). Once the marker lands, the successor's
    /// acknowledgements wait out the predecessor's strict-read horizon —
    /// unless that tenure never served one.
    pub(crate) fn note_fast_takeover(&mut self, prev: &Lease) {
        self.ack.pending_floor = prev
            .granted_delegations
            .then_some((prev.expires_unix_ms, Ms(0)));
    }

    /// The takeover's marker landed at `now`.
    pub(crate) fn note_marker_landed(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((prev_expires, _)) = self.ack.pending_floor.take() else {
            return;
        };
        // Plan 30 §M11 phase 2b: a tenure with live write delegations
        // has grants (and, under them, read grants a delegate gave)
        // that its successor cannot see renewed; one constant covers
        // them all.
        let deleg = if replica.delegation_table().is_empty() {
            0
        } else {
            self.reclaim_horizon_ms() as i64
        };
        // Plan 30 §M14: lock grants are capped like read delegations and
        // marked the same way; the longer ttl covers both.
        let ttl = self.cfg.read_delegation_ttl_ms.max(self.cfg.lock_ttl_ms) as i64;
        let bound = now.0
            + self.cfg.backup_takeover_ms as i64
            + ttl
            + deleg
            + 2 * self.cfg.expiry_margin_ms as i64;
        let until = prev_expires.min(bound);
        if until <= now.0 {
            return;
        }
        self.stats.ack_floor_waits += 1;
        tracing::info!(
            node = self.cfg.node_id,
            wait_ms = until - now.0,
            "took an unexpired lease over from a tenure that served strict reads; \
             acknowledging no mutation until its delegation horizon has passed"
        );
        replica.read_delegations().set_quarantine(until);
        self.arm_grant_quarantine(Ms(until), out);
    }

    // ------------------------------------------------------ holder side

    /// After every event: keep the backup set in step with the lease, the
    /// links and the acknowledgements; stream; reconfigure; and, as a
    /// non-holder, watch an `ack=s3` holder for silence.
    pub(crate) fn backup_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.stopped {
            return;
        }
        let holding = self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.epoch_held();
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
            self.ack.streamed_through = 0;
            self.ack.stream_ahead_pending = false;
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
            let housekeeping =
                self.ack.tick_fired || now.since(self.ack.last_housekeeping) >= HOUSEKEEPING_MS;
            if housekeeping {
                self.ack.tick_fired = false;
                self.ack.last_housekeeping = now;
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
    pub(crate) fn backup_candidates(&self, now: Ms) -> Vec<NodeId> {
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
                    .is_some_and(|since| now.since(since) >= self.cfg.backup_stable_ms as i64)
            })
            .map(|l| (l.since.unwrap_or(now), l.node))
            .collect();
        eligible.sort();
        eligible.into_iter().map(|(_, n)| n).collect()
    }

    fn backup_select(&mut self, now: Ms, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        let candidates = self.backup_candidates(now);
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
            && now.since(self.ack.last_reconfig) < self.cfg.backup_reconfig_min_ms as i64
        {
            return;
        }
        let Some(n) = candidates.into_iter().find(|n| {
            self.ack
                .last_dropped
                .get(n)
                .is_none_or(|at| now.since(*at) >= self.cfg.backup_reconfig_min_ms as i64)
        }) else {
            return;
        };
        tracing::info!(
            node = self.cfg.node_id,
            candidate = n,
            "bringing up a backup"
        );
        self.ack.candidate = Some(n);
        self.ack.peers.insert(
            n,
            BackupPeer {
                acked: 0,
                sent_through: 0,
                inflight: std::collections::VecDeque::new(),
                last_sent: Ms(0),
                last_progress: now,
                committed: false,
            },
        );
    }

    /// Send the next append to every backup with a free pipeline slot:
    /// the rows past what it was last sent, or a heartbeat when it is
    /// caught up and idle. Appends are acknowledged cumulatively by
    /// journal position, so several may be in flight; rows journaled
    /// while every slot is taken ride the next one (the group commit).
    fn backup_stream(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
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
                    || (p.inflight.is_empty() && now.since(p.last_sent) >= heartbeat)
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
                });
            }
            let last = txs.last().map(|t| t.last);
            let req = self.op_id();
            let p = self.ack.peers.get_mut(&n).expect("present");
            if let Some(last) = last {
                p.sent_through = p.sent_through.max(last);
            }
            p.inflight.push_back(Inflight {
                req,
                sent: now,
                last: last.unwrap_or(start - 1),
            });
            p.last_sent = now;
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
                    from: start,
                    txs,
                    through,
                },
            });
        }
        self.arm_backup_tick(now, out);
    }

    fn arm_backup_tick(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.ack.tick_timer.is_some()
            || (self.ack.peers.is_empty() && self.ack.reconfig_wanted.is_none())
        {
            return;
        }
        let id = self.set_timer(
            now.plus(self.cfg.backup_heartbeat_ms.max(1)),
            Timer::BackupTick,
            out,
        );
        self.ack.tick_timer = Some(id);
    }

    pub(crate) fn backup_tick(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        // Everything runs in `backup_after_event`, which follows.
        self.ack.tick_fired = true;
        let _ = (now, replica, out);
    }

    /// A backup that makes no progress for `backup_ack_timeout_ms` is
    /// removed (a committed one by a lease CAS) or dropped (a candidate).
    fn backup_timeouts(&mut self, now: Ms, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        let timeout = self.cfg.backup_ack_timeout_ms as i64;
        let tip_behind = |p: &BackupPeer| !p.inflight.is_empty();
        let stale: Vec<NodeId> = self
            .ack
            .peers
            .iter()
            .filter(|(_, p)| tip_behind(p) && now.since(p.last_progress) >= timeout)
            .map(|(n, _)| *n)
            .collect();
        for n in stale {
            self.stats.backup_ack_timeouts += 1;
            if self.ack.candidate == Some(n) {
                tracing::info!(
                    node = self.cfg.node_id,
                    candidate = n,
                    "backup candidate timed out"
                );
                self.ack.candidate = None;
                self.ack.peers.remove(&n);
                self.ack.last_dropped.insert(n, now);
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
    fn drop_backup(&mut self, now: Ms, n: NodeId, why: &'static str) {
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
        self.ack.last_dropped.insert(n, now);
    }

    /// A candidate that has caught up joins the lease.
    fn backup_promote(&mut self, now: Ms, replica: &dyn Replica, _out: &mut Vec<Action>) {
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
        now: Ms,
        backups: Vec<NodeId>,
        _replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.ack.reconfig.is_some() {
            return;
        }
        if self.lease_cas_in_flight()
            || now.since(self.ack.last_reconfig_try) < (self.cfg.backup_heartbeat_ms as i64).max(1)
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
        self.ack.last_reconfig_try = now;
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
        now: Ms,
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
                self.ack.last_reconfig = now;
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
        now: Ms,
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
        now: Ms,
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
            } else {
                self.drop_backup(now, from, "sealed");
            }
            return;
        }
        let before = self.durable_jseq();
        let p = self.ack.peers.get_mut(&from).expect("present");
        // Progress is the hold advancing, or an append answered in full
        // (a heartbeat's included): a backup that keeps answering short
        // for the same rows is stuck, and the timeout rule drops it
        // rather than the two of them resending forever (round 2).
        if acked > p.acked || acked >= entry.last {
            p.last_progress = now;
        }
        tracing::trace!(
            node = self.cfg.node_id,
            backup = from,
            rtt_ms = now.since(entry.sent),
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
        // Resend from what the backup holds (the timeout counts from
        // `last_progress`); the inserts are idempotent.
        p.sent_through = p.acked;
        p.last_sent = Ms(0);
        true
    }

    /// Rows became durable: stream them ahead now, unless a send went
    /// out within the hold-off — then once it elapses, all at once. One
    /// request per subscriber per hold-off, not per acknowledgement.
    fn stream_ahead_soon(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.ack.stream_ahead_timer.is_some() {
            self.ack.stream_ahead_pending = true;
            return;
        }
        let from = self.ack.streamed_through;
        let to = self.durable_jseq();
        self.stream_ahead(now, from, to, replica, out);
        let id = self.set_timer(
            now.plus(self.cfg.stream_ahead_holdoff_ms.max(1)),
            Timer::StreamAhead,
            out,
        );
        self.ack.stream_ahead_timer = Some(id);
    }

    pub(crate) fn on_stream_ahead_timer(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.ack.stream_ahead_timer = None;
        if !std::mem::take(&mut self.ack.stream_ahead_pending) {
            return;
        }
        if self.lease.held.is_none() || self.lease.ack_policy() != AckPolicy::Backup {
            return;
        }
        self.stream_ahead_soon(now, replica, out);
    }

    /// The durable range `(from, to]` just became backup-acked: stream it
    /// to the log-stream subscribers as speculation (plan 30 §M9).
    fn stream_ahead(
        &mut self,
        now: Ms,
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
            return;
        }
        let txs: Vec<BackupTx> = replica
            .journal_txs_from(start, self.cfg.backup_batch_rows)
            .into_iter()
            .filter(|t| t.last <= to)
            .collect();
        if txs.is_empty() {
            return;
        }
        self.ack.streamed_through = txs.last().map(|t| t.last).unwrap_or(to);
        let Some(epoch) = self.lease.epoch() else {
            return;
        };
        self.stats.streamed_ahead += txs.len() as u64;
        let base = self.ship.head_seq;
        for n in subscribers {
            out.push(Action::Send {
                to: n,
                msg: PeerMsg::StreamAhead {
                    epoch,
                    base,
                    txs: txs.clone(),
                },
            });
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
        now: Ms,
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
        now: Ms,
        from: NodeId,
        req: OpId,
        (epoch, holder, config_version): (Epoch, NodeId, u64),
        from_jseq: u64,
        txs: Vec<BackupTx>,
        through: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
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
            }
        }
        self.bk.last_heard = now;
        self.lease.cached_holder = Some(holder);
        self.bk.holder_silent_since = None;
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

    /// Rule (b): the epoch closed; a member backup resumes its watch with
    /// the holder given a full silence window (its re-acquisition after
    /// the close keeps the epoch, and a stale `last_heard` would seal it
    /// at once).
    pub(crate) fn backup_watch_after_epoch(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.bk.role.is_none() {
            return;
        }
        self.bk.last_heard = now;
        self.arm_backup_watch(now, out);
    }

    fn arm_backup_watch(&mut self, now: Ms, out: &mut Vec<Action>) {
        if self.bk.watch_timer.is_some() || !self.cfg.p2p || self.epoch.open {
            // Rule (b): no seal watch while a continuation epoch is
            // open (re-armed by `backup_watch_after_epoch`).
            return;
        }
        let id = self.set_timer(
            now.plus(self.cfg.backup_takeover_ms.max(1)),
            Timer::BackupWatch,
            out,
        );
        self.bk.watch_timer = Some(id);
    }

    /// The watch fired: silence past `backup_takeover_ms` seals the epoch
    /// and reads the lease to take it over.
    pub(crate) fn on_backup_watch(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(role) = self.bk.role else {
            return;
        };
        if self.lease.held.is_some() || self.bk.takeover_get.is_some() || self.epoch.open {
            // (`epoch.open`: rule (b) — a member of an open epoch never
            // seals; `backup_watch_after_epoch` resumes the watch.)
            return;
        }
        let silent = now.since(self.bk.last_heard);
        if silent < self.cfg.backup_takeover_ms as i64 {
            let id = self.set_timer(
                self.bk.last_heard.plus(self.cfg.backup_takeover_ms),
                Timer::BackupWatch,
                out,
            );
            self.bk.watch_timer = Some(id);
            return;
        }
        if self.bk.sealed < role.epoch {
            if !replica.backup_seal(role.epoch) {
                // Could not persist the seal: try again shortly; never
                // take over unsealed.
                self.arm_backup_watch(now, out);
                return;
            }
            self.bk.sealed = role.epoch;
            self.stats.seals += 1;
            tracing::warn!(
                node = self.cfg.node_id,
                holder = role.holder,
                epoch = role.epoch,
                silent_ms = silent,
                "holder silent: sealed its epoch; reading the lease to take over"
            );
        }
        let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
        self.bk.takeover_get = Some(op);
    }

    pub(crate) fn on_takeover_get(
        &mut self,
        now: Ms,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.bk.takeover_get = None;
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
                // void (a listed successor re-shipped what mattered).
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
            if !done {
                replica
                    .apply_records_journaled(&tx.records, rid)
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
        // The next streamed transaction must follow the log.
        self.bk.ahead_next = Some((epoch, through + 1));
    }

    // -------------------------------------------- pre-S3 stream subscriber

    /// The holder's backups hold `txs`: install them ahead of the log.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_stream_ahead(
        &mut self,
        now: Ms,
        from: NodeId,
        epoch: Epoch,
        base: Seq,
        txs: Vec<BackupTx>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self.cfg.pre_s3_streaming
            || self.lease.held.is_some()
            || self.lease.epoch_held()
            || self.lease.cached_holder != Some(from)
            || epoch != self.ship.max_epoch
            || self.ship.head_seq < base
            || !self.cursor_free()
        {
            self.stats.streamed_dropped += txs.len() as u64;
            self.bk.ahead_next = None;
            return;
        }
        let mut completed: Vec<(constellation_meta::Rid, constellation_meta::KeySet)> = Vec::new();
        for tx in txs {
            let Some((e, next)) = self.bk.ahead_next else {
                self.stats.streamed_dropped += 1;
                continue;
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
            if tx.first != next {
                self.stats.streamed_dropped += 1;
                self.bk.ahead_next = None;
                continue;
            }
            if tx.records.is_empty() {
                // A hole (round 2): rows the holder shipped and dropped
                // before streaming past them — in the log at or below
                // `base`, which this node has applied. The cursor steps
                // over them.
                self.bk.ahead_next = Some((epoch, tx.last + 1));
                continue;
            }
            match replica.install_streamed(epoch, tx.first, tx.last, &tx.records) {
                Ok(()) => {
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

    // ------------------------------------------- `ack=s3` fast takeover

    /// A non-holder under `ack_policy = S3`: the known holder silent on
    /// P2P for `backup_takeover_ms` is a takeover trigger (the lease's
    /// renewal time is checked at classification).
    fn watch_s3_holder(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
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
        if seen.is_claimable(now.0) {
            return;
        }
        // Silence is measured on the holder's log stream when this node
        // subscribes to it (the holder heartbeats it every third of the
        // takeover time under `S3`): a frozen or dead holder stops
        // framing at once, while its QUIC connection and gossip
        // membership linger for seconds. Without a subscription, the
        // link's `connected` flag is all there is (slower, never wrong
        // for safety: the log slot fences either way).
        let silent = match self.stream_last_frame_from(seen.holder) {
            Some(last) => now.since(last) >= self.cfg.backup_takeover_ms as i64,
            None => {
                let up = self.links.get(&seen.holder).is_some_and(|l| l.connected)
                    || self.stream_live_from(seen.holder);
                if up {
                    self.bk.holder_silent_since = None;
                    return;
                }
                let since = *self.bk.holder_silent_since.get_or_insert(now);
                now.since(since) >= self.cfg.backup_takeover_ms as i64
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
