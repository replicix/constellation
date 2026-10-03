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
/// The longest a holder's [`PeerMsg::BackupHold`] holds this node's seal
/// watch, whatever it asks.
pub const BACKUP_HOLD_MAX_MS: u64 = 30_000;

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
    pub s3_sent: BTreeMap<OpId, Ms>,
    pub last_s3_fresh: Ms,
    /// EWMA of this node's S3 request round trips (ms, 0 until the
    /// first): what a handoff's successor needs to claim the lease
    /// scales with it (`Core::begin_handoff_pause`).
    pub s3_rtt_ms: u64,
    /// The `MarkGranting` CAS in flight.
    marking: Option<OpId>,
    /// What the replica's durability gate was last told.
    reported: Option<(bool, u64, bool)>,
    /// The tenure's marker time, for the successor's floor (see the
    /// module doc); `None` after the floor was set or when none is due.
    pending_floor: Option<(i64, Ms)>,
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
}

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
    pub(crate) fn note_s3_liveness(&mut self, now: Ms, op: OpId, result: &S3Result) {
        let Some(sent) = self.ack.s3_sent.remove(&op) else {
            return;
        };
        let rtt = now.since(sent).max(0) as u64;
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
        let bound = |ttl: u64| {
            now.0
                + self.cfg.backup_takeover_ms as i64
                + ttl as i64
                + deleg
                + 2 * self.cfg.expiry_margin_ms as i64
        };
        // Plan 30 §M14: lock grants are capped like read delegations and
        // marked the same way, but only new lock grants wait for them —
        // a lock grant still honoured elsewhere does not make an
        // acknowledgement stale — so their (longer) ttl is a quarantine
        // of the lock table's own.
        let locks_until = prev_expires.min(bound(
            self.cfg.read_delegation_ttl_ms.max(self.cfg.lock_ttl_ms),
        ));
        if self.cfg.locks && locks_until > now.0 {
            replica.locks().set_quarantine(locks_until);
        }
        let until = prev_expires.min(bound(self.cfg.read_delegation_ttl_ms));
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
        // Unique per holder across its restarts too (time-based), so a
        // dismissal of an earlier bring-up never matches this one.
        let candidacy = (now.0.max(0) as u64).max(self.ack.last_candidacy + 1);
        self.ack.last_candidacy = candidacy;
        self.ack.peers.insert(
            n,
            BackupPeer {
                acked: 0,
                sent_through: 0,
                inflight: std::collections::VecDeque::new(),
                last_sent: Ms(0),
                last_progress: now,
                committed: false,
                candidacy,
                alive_at: now,
                short_since_progress: false,
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
                    candidacy,
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
    fn backup_timeouts(&mut self, now: Ms, _replica: &dyn Replica, _out: &mut Vec<Action>) {
        let timeout = self.cfg.backup_ack_timeout_ms as i64;
        let slow_max = (self.cfg.backup_slow_max_ms as i64).max(timeout);
        let tip_behind = |p: &BackupPeer| !p.inflight.is_empty();
        let stale: Vec<NodeId> = self
            .ack
            .peers
            .iter()
            .filter(|(_, p)| {
                let silent = now.since(p.last_progress);
                tip_behind(p)
                    && silent >= timeout
                    && (now.since(p.alive_at) >= timeout
                        || p.short_since_progress
                        || silent >= slow_max)
            })
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
        // Progress is the hold advancing, or an append answered in full
        // (a heartbeat's included): a backup that keeps answering short
        // for the same rows is stuck, and the timeout rule drops it
        // rather than the two of them resending forever (round 2).
        if acked > p.acked || acked >= entry.last {
            p.last_progress = now;
            p.short_since_progress = false;
        } else {
            p.short_since_progress = true;
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

    /// `Event::BackupAlive`: `from` answered the holder's heartbeat sent
    /// at `at`.
    pub(crate) fn on_backup_alive(&mut self, from: NodeId, at: Ms) {
        if let Some(p) = self.ack.peers.get_mut(&from) {
            p.alive_at = p.alive_at.max(at);
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
        let to = if self.epoch_streams_ahead() {
            replica.journal_tip()
        } else {
            self.durable_jseq()
        };
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
        (epoch, holder, config_version, candidacy): (Epoch, NodeId, u64, u64),
        from_jseq: u64,
        txs: Vec<BackupTx>,
        through: u64,
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
        self.bk.last_heard = now;
        self.bk.restarted = false;
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
    pub(crate) fn on_backup_hold(&mut self, now: Ms, from: NodeId, epoch: Epoch, for_ms: u64) {
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
        let until = now.plus(for_ms.min(BACKUP_HOLD_MAX_MS));
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
    pub(crate) fn on_holder_alive(
        &mut self,
        from: NodeId,
        epoch: Epoch,
        candidacy: u64,
        listed: bool,
        at: Ms,
        replica: &dyn Replica,
    ) {
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
    /// before, and the seal is a local write.
    pub(crate) fn on_backup_watch(&mut self, now: Ms, out: &mut Vec<Action>) {
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
                self.bk.last_heard = self.bk.last_heard.max(now);
            } else {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
                self.bk.takeover_get = Some(op);
                self.bk.restart_probe = true;
                return;
            }
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
        tracing::info!(
            node = self.cfg.node_id,
            holder = role.holder,
            epoch = role.epoch,
            silent_ms = silent,
            "holder silent: reading the lease before sealing its epoch"
        );
        let op = self.issue_s3(S3Op::LeaseGet, S3For::TakeoverGet, out);
        self.bk.takeover_get = Some(op);
        self.bk.takeover_read_at = now;
    }

    pub(crate) fn on_takeover_get(
        &mut self,
        now: Ms,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.bk.takeover_get = None;
        let probe = std::mem::take(&mut self.bk.restart_probe);
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
                if probe && !lease.is_expired(now.0) {
                    // A restarted backup's probe (3b): the holder is
                    // renewing — alive; wait for its link. An expired
                    // lease is a holder gone: seal and take over now.
                    self.lease.note_object(now, &lease);
                    self.arm_backup_watch(now, out);
                    return;
                }
                if !probe && self.bk.last_heard >= self.bk.takeover_read_at {
                    // The holder spoke while the lease was read: alive.
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
        now: Ms,
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
        now: Ms,
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
