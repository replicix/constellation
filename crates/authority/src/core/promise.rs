//! Plan 30 §M10: heartbeat promises and the TTL takeover's promise check
//! (flexible-quorum continuation epochs; the rule is checked by the
//! Stateright model `constellation_model::flex`).
//!
//! With `epoch_slack = f > 0` a continuation epoch may form with up to
//! `f` write-eligible nodes missing, and a missing node may still reach
//! S3. A *promise* is a node's persisted word that it joins no
//! continuation epoch before `no_epoch_until` (its own clock); an S3
//! takeover of an expired lease someone else held proceeds only when at
//! least `f` other roster nodes promise past the lease's recorded expiry
//! (`store_s3::heartbeat::takeover_check`). An epoch needs `N − f`
//! members, each past its own last issued promise, so a taker's `f`
//! promisers and an epoch's `N − f` members share a node, whose promise
//! would then have to be both expired and outlast the expiry of a lease
//! the epoch holder still used — two clocks further apart than drift
//! allows.
//!
//! # Cadence: on demand
//!
//! Nothing is published on a timer. A node promises when:
//! - a would-be taker asks for one over P2P ([`PeerMsg::PromiseRequest`]);
//! - with P2P unavailable (nobody can ask), it observes the lease expire
//!   unrenewed (a lease object it read, held by another node, not
//!   released, past its expiry in this node's clock, and not already
//!   covered by its last promise);
//! - its slack changed (the heartbeat advertises the slack it runs with,
//!   and a taker honours the largest one any roster node advertises).
//!
//! Every promise is persisted through the replica *before* its PUT
//! (`Replica::issue_promise`), and the replica refuses while a
//! continuation-epoch join holds the gate: a member publishes nothing.
//! With P2P unavailable a would-be taker cannot ask, so a node with
//! `f > 0` and no P2P link re-reads the lease every `promise_watch_ms`
//! (GETs, not PUTs) to notice the expiry itself.
//!
//! # The takeover check
//!
//! In the acquisition job (`jobs.rs`), a `Plan::Claim { takeover: true }`
//! on an *expired*, unreleased lease another node held — not M9's permit
//! paths, which the claim rule keeps apart from epochs — reads
//! `heartbeat/` and asks every roster peer for a promise, then counts the
//! heartbeats and the replies whose promise outlasts the lease's
//! `expires_unix_ms`. The heartbeat read always completes first: it is
//! where a larger advertised slack would show. Exempt: the flush
//! re-claim of exactly the lease object a continuation epoch this node
//! was a member of carried (the register unchanged since formation — no
//! other epoch can hold it, and the members stay silent until the flush,
//! so the check would deadlock).

use super::{Core, S3For, Timer};
use crate::action::{Action, S3Op};
use crate::event::{Carrier, PeerMsg, S3Result};
use crate::ids::{Epoch, Ms, NodeId, OpId, TimerId};
use crate::replica::Replica;
use constellation_store_s3::heartbeat::{
    effective_slack, takeover_check, Promise, PromiseConfig, PROMISE_TTL_LEASE_DIVISOR,
};
use constellation_store_s3::{AckPolicy, Lease, LeaseTag};
use std::collections::BTreeMap;

/// Whether a continuation epoch of `members` may carry `lease` (plan 30
/// §M10's claim rule, enforced since M9): an acknowledgement policy
/// nobody outside the epoch can take over from — `Local`, or `Backup`
/// with every listed backup a member and no reconfiguration in flight;
/// never `S3`.
pub fn lease_may_carry(lease: &Lease, members: &[NodeId], reconfig_busy: bool) -> bool {
    match lease.ack_policy {
        AckPolicy::Local => true,
        AckPolicy::Backup => lease.backups.iter().all(|b| members.contains(b)) && !reconfig_busy,
        AckPolicy::S3 => false,
    }
}

/// What this node would claim in a continuation-epoch proposal: the
/// driver's epoch coordinator reads it (it runs outside the core) when it
/// proposes or acks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochClaimView {
    /// The lease this node holds usably, as the object read.
    pub held: Option<Lease>,
    /// A backup-set reconfiguration CAS is in flight or wanted.
    pub reconfig_busy: bool,
    /// The highest lease epoch this node knows exists: its own tenure, a
    /// marker or segment it applied, the successor of an epoch it sealed.
    pub known: Epoch,
    /// The slack of the last heartbeat that landed (`None`: none yet). A
    /// node forms or joins an epoch under `f > 0` only once it has
    /// advertised `f`.
    pub advertised_slack: Option<u32>,
    /// This node holds a continuation-epoch hold now.
    pub epoch_held: bool,
}

impl EpochClaimView {
    /// The claim for an epoch of `members`: `(lease epoch, expires,
    /// may carry)`.
    pub fn claim(&self, members: &[NodeId]) -> Option<(Epoch, i64, bool)> {
        self.held.as_ref().map(|l| {
            (
                l.epoch,
                l.expires_unix_ms,
                lease_may_carry(l, members, self.reconfig_busy),
            )
        })
    }
}

/// One member's ack for [`resolve_epoch_claims`]: `(member, claim as
/// [`EpochClaimView::claim`] gives it, highest epoch it knows of)`.
pub type EpochAckClaim = (NodeId, Option<(Epoch, i64, bool)>, Epoch);

/// Plan 30 §M10's claim resolution over a proposal's acks
/// (`(member, claim, known)`, the claim as [`EpochClaimView::claim`]
/// gives it): the epoch carries the claim at the highest lease epoch,
/// provided no member knows a later epoch and the claim rule lets these
/// members carry it; every claim below `max(highest claim, highest
/// known)` is stale (its member was taken over and has not heard yet).
/// Returns `(carrier, stale_below)`.
pub fn resolve_epoch_claims(acks: &[EpochAckClaim]) -> (Option<Carrier>, Epoch) {
    let known = acks.iter().map(|(_, _, k)| *k).max().unwrap_or(0);
    let best = acks
        .iter()
        .filter_map(|(node, claim, _)| claim.map(|c| (*node, c)))
        .max_by_key(|(node, (epoch, _, _))| (*epoch, *node));
    let top = best.map(|(_, (epoch, _, _))| epoch).unwrap_or(0);
    let stale_below = known.max(top);
    let carrier = best.and_then(|(node, (epoch, expires_unix_ms, may_carry))| {
        (epoch >= known && may_carry).then_some(Carrier {
            node,
            epoch,
            expires_unix_ms,
        })
    });
    (carrier, stale_below)
}

#[derive(Debug, Default)]
pub(crate) struct PromiseState {
    /// The last promise issued (persisted), mirrored from the replica.
    pub issued: i64,
    /// The slack of the last heartbeat that landed.
    pub advertised: Option<u32>,
    /// A PUT advertising this slack is in flight.
    pub advertising: Option<u32>,
    /// The lease the current (or last) continuation epoch carried.
    pub carried: Option<Carrier>,
    /// Admin `leave --node-id` retired this node.
    pub retired: bool,
    /// The running takeover check.
    pub check: Option<Check>,
    pub watch_timer: Option<TimerId>,
    /// The epoch hold as last persisted, and the one found at start (a
    /// restarted hold owner re-adopts it when its epoch is reported
    /// open).
    pub persisted_hold: Option<Epoch>,
    pub restored_hold: Option<Epoch>,
    /// The carried lease (epoch, expiry) whose hold this node owned and
    /// let go — handed to a peer, or closed with the epoch (persisted):
    /// never re-adopted (flex-crash seeds 166, 2236). The flag: this node
    /// closed the epoch itself and owes the lease's re-claim.
    pub hold_ended: Option<(Epoch, i64, bool)>,
    /// This node closed its epoch while owning the hold, and its journal
    /// has not all reached the log yet: it is still the epoch's
    /// authority, and promises nothing.
    pub flush_pending: bool,
    /// The leases `(holder, epoch, expiry)` this node held when it last
    /// closed a continuation epoch — its own S3 lease, and the carried
    /// lease whose hold it owned — until it next acquires. The close let
    /// them go locally but kept the lock grants made under them
    /// (`epoch_close_release`); the acquisition that CASes exactly one
    /// of these objects continues that tenure, any other ends it
    /// (`epoch_tenure_resumed`). An epoch hold of one of them continues
    /// the tenure too, and keeps the list: the S3 object is still one of
    /// them. Holds the object a re-claim CAS in doubt may have written
    /// (`(me, e, X')` replacing `(me, e, X)`): it is this tenure's if it
    /// landed. Empty whenever the grants were dropped
    /// (`lock_on_lease_gone`).
    pub closed_tenure: Vec<(NodeId, Epoch, i64)>,
    /// The S3 lease object (and its tag) this node held at that close,
    /// while it is in `closed_tenure`: what it claims for a continuation
    /// epoch until the re-claim lands (`epoch_claim_view`), and holds
    /// again if one carries it (`on_epoch_state`).
    pub closed_lease: Option<(Lease, LeaseTag)>,
}

#[derive(Debug)]
pub(crate) struct Check {
    pub holder: NodeId,
    pub expires: i64,
    /// Outstanding promise requests → the peer asked.
    pub asked: BTreeMap<OpId, NodeId>,
    /// Promises received over P2P: node → until (and its slack).
    pub replies: BTreeMap<NodeId, (i64, u32)>,
    /// The heartbeat read's result (`None` while outstanding).
    pub heartbeats: Option<Result<Vec<(NodeId, Promise)>, String>>,
    pub timer: Option<TimerId>,
    pub timed_out: bool,
    /// Promise requests went out (at `f = 0` only once the heartbeats
    /// show a larger slack).
    pub asked_any: bool,
}

impl Core {
    pub(crate) fn promise_start(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.pr.issued = replica.promise_issued();
        self.pr.restored_hold = replica.epoch_hold_persisted();
        self.pr.persisted_hold = self.pr.restored_hold;
        self.pr.hold_ended = replica.epoch_hold_ended();
        self.advertise_slack(now, replica, out);
    }

    /// `Event::Slack`: `meta.json`'s `epoch_slack` changed.
    pub(crate) fn on_slack(
        &mut self,
        now: Ms,
        epoch_slack: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if epoch_slack != self.cfg.epoch_slack {
            tracing::info!(
                node = self.cfg.node_id,
                from = self.cfg.epoch_slack,
                to = epoch_slack,
                "epoch slack changed"
            );
            self.cfg.epoch_slack = epoch_slack;
        }
        self.clamp_promise_ttl();
        self.advertise_slack(now, replica, out);
    }

    /// Plan 30 §M10's promise-TTL rule (at most lease TTL / 4, positive)
    /// is checked at mount only when `epoch_slack > 0` then; a mount that
    /// started at `f = 0` with a longer `CONSTELLATION_PROMISE_TTL_S` and
    /// is raised at runtime (`fs set epoch-slack`) is checked here. The
    /// TTL is clamped rather than the slack refused: this node must run
    /// with the filesystem's slack (a taker honours the largest one
    /// advertised anyway), and a shorter promise is always safe — the
    /// promises already issued stay persisted and still bind the join.
    pub(crate) fn clamp_promise_ttl(&mut self) {
        if self.cfg.epoch_slack == 0 {
            return;
        }
        let promise = PromiseConfig::new(self.cfg.promise_ttl_ms);
        if let Err(why) = promise.validate(self.cfg.ttl_ms) {
            let clamped = (self.cfg.ttl_ms / PROMISE_TTL_LEASE_DIVISOR).max(1);
            tracing::error!(
                node = self.cfg.node_id,
                epoch_slack = self.cfg.epoch_slack,
                from_ms = self.cfg.promise_ttl_ms,
                to_ms = clamped,
                %why,
                "epoch slack raised at runtime with an invalid promise TTL \
                 (CONSTELLATION_PROMISE_TTL_S); clamping it to lease TTL / 4"
            );
            self.cfg.promise_ttl_ms = clamped;
        }
    }

    /// Plan 30 §M10: the heartbeat carries the slack this node runs with.
    /// Publish it when it differs from what last landed (f > 0, or a
    /// withdrawal after f dropped to 0); f = 0 on a node that never
    /// advertised writes nothing (today's cost).
    fn advertise_slack(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let f = self.cfg.epoch_slack;
        if self.pr.retired || self.pr.advertising.is_some() || self.pr.advertised == Some(f) {
            return;
        }
        if f == 0 && self.pr.advertised.is_none() && self.pr.issued == 0 {
            return;
        }
        // Same promise as last issued: only the slack changes. Nothing to
        // persist first (the promise is not extended).
        let promise = Promise::new(self.cfg.node_id, self.pr.issued, f, now.0);
        self.pr.advertising = Some(f);
        self.stats.promise_puts += 1;
        self.issue_s3(S3Op::HeartbeatPut { promise }, S3For::HeartbeatPut(f), out);
        let _ = replica;
    }

    pub(crate) fn on_heartbeat_put(&mut self, slack: u32, result: S3Result) {
        if self.pr.advertising == Some(slack) {
            self.pr.advertising = None;
        }
        match result {
            S3Result::HeartbeatPut(Ok(())) => self.pr.advertised = Some(slack),
            S3Result::HeartbeatPut(Err(e)) => {
                tracing::debug!(node = self.cfg.node_id, error = %e.0, "heartbeat PUT failed")
            }
            _ => {}
        }
    }

    /// Persist, then publish, a promise binding this node until
    /// `now + promise_ttl` (never shortening one already issued). `false`
    /// when refused: in an open epoch (or joining one), or retired.
    fn promise_now(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) -> bool {
        // An epoch's hold owner stays silent through its flush too (found
        // by the simulation, flex-crash seed 30002): it closed its epoch,
        // but until its journal is in the log it is still the epoch's
        // authority, and a promise from it would let a taker in first.
        if self.pr.retired || self.epoch.open || self.pr.flush_pending {
            return false;
        }
        let until = now.0 + self.cfg.promise_ttl_ms as i64;
        if until <= self.pr.issued {
            return true;
        }
        match replica.issue_promise(until) {
            Ok(true) => {}
            Ok(false) => return false,
            Err(e) => {
                tracing::warn!(node = self.cfg.node_id, error = %e, "persisting a promise failed");
                return false;
            }
        }
        self.pr.issued = until;
        self.stats.promise_puts += 1;
        let f = self.cfg.epoch_slack;
        let promise = Promise::new(self.cfg.node_id, until, f, now.0);
        self.issue_s3(S3Op::HeartbeatPut { promise }, S3For::HeartbeatPut(f), out);
        true
    }

    /// After every event: promise when the lease this node last read has
    /// expired unrenewed, and keep the idle watch armed while P2P is
    /// unavailable.
    pub(crate) fn promise_after_event(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // The hold's owner is persisted, whichever path moved it — but not
        // while a restarted owner's persisted hold waits to be re-adopted
        // (its epoch not reported yet): a second crash then would lose it.
        // A hold that goes away ends for good (`end_epoch_hold`): a
        // handoff, the close, a handoff job's release in a frozen epoch
        // (flex-crash seed 2236), a deposition.
        self.epoch_reclaim_settle(replica);
        let hold = self.lease.epoch_hold();
        if hold != self.pr.persisted_hold && self.pr.restored_hold.is_none() {
            if hold.is_none() {
                self.end_epoch_hold(replica, false);
            }
            if replica.persist_epoch_hold(hold).is_ok() {
                self.pr.persisted_hold = hold;
            }
        }
        if self.pr.flush_pending && replica.journal_len().unwrap_or(1) == 0 {
            self.pr.flush_pending = false;
        }
        if self.cfg.epoch_slack == 0 || self.pr.retired || self.epoch.open || self.pr.flush_pending
        {
            return;
        }
        // With P2P up a would-be taker asks (`PromiseRequest`); promising
        // on every lease object this node happens to read would cost a PUT
        // per read once its recorded expiry passes, renewed or not. With
        // P2P down nobody can ask: the idle watch below keeps `last_seen`
        // fresh, and an expiry it shows is a real one.
        let p2p_down = !self.cfg.p2p || !self.links.values().any(|l| l.connected);
        if let Some(seen) = self.lease.last_seen.as_ref().filter(|_| p2p_down) {
            if seen.holder != 0
                && seen.holder != self.cfg.node_id
                && !seen.released
                && seen.is_expired(now.0)
                && self.pr.issued <= seen.expires_unix_ms
            {
                self.promise_now(now, replica, out);
            }
        }
        if p2p_down && self.pr.watch_timer.is_none() && !self.lease.usable(now, &self.cfg) {
            let id = self.set_timer(
                now.plus(self.cfg.promise_watch_ms),
                Timer::PromiseWatch,
                out,
            );
            self.pr.watch_timer = Some(id);
        }
    }

    /// The idle watch: re-read the lease (its result lands in
    /// `last_seen`, and `promise_after_event` promises if it expired).
    pub(crate) fn on_promise_watch(&mut self, now: Ms, out: &mut Vec<Action>) {
        let p2p_down = !self.cfg.p2p || !self.links.values().any(|l| l.connected);
        if self.cfg.epoch_slack == 0
            || self.pr.retired
            || self.epoch.open
            || !p2p_down
            || self.lease.usable(now, &self.cfg)
        {
            return;
        }
        self.issue_s3(S3Op::LeaseGet, S3For::PromiseWatch, out);
    }

    pub(crate) fn on_promise_request(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        expires_unix_ms: i64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A node running with `f = 0` joins only epochs of the whole
        // roster (the taker included): its promise would add nothing, and
        // `f = 0` never touches `heartbeat/`.
        let until = if self.cfg.epoch_slack > 0 && self.promise_now(now, replica, out) {
            self.stats.promise_requests_answered += 1;
            Some(self.pr.issued)
        } else {
            self.stats.promise_requests_refused += 1;
            None
        };
        tracing::debug!(
            node = self.cfg.node_id,
            from,
            expires_unix_ms,
            ?until,
            "answered a promise request"
        );
        out.push(Action::Send {
            to: from,
            msg: PeerMsg::PromiseReply {
                req,
                until,
                epoch_slack: self.cfg.epoch_slack,
            },
        });
    }

    /// `Control::Retire`: never acquire, promise or acknowledge again.
    pub(crate) fn on_retire(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.pr.retired {
            return;
        }
        tracing::error!(
            node = self.cfg.node_id,
            "retired: this node takes no authority again"
        );
        self.pr.retired = true;
        if self.lease.held.is_some() || self.lease.epoch_held() {
            let mine = self.lease.epoch().unwrap_or(0);
            self.deposed(now, 0, mine.saturating_add(1), mine, replica, out);
        }
    }

    pub(crate) fn retired(&self) -> bool {
        self.pr.retired
    }

    // ---- the claim, for the driver's epoch coordinator ----

    pub fn epoch_claim_view(&self, now: Ms) -> EpochClaimView {
        let held = if self.lease.usable(now, &self.cfg) && !self.lease.epoch_held() {
            self.lease.held.as_ref().map(|(l, _)| l.clone())
        } else {
            // Between an epoch's close and its re-claim the lease stands
            // in S3, this node's, though let go locally: it claims it as
            // it would hold it. Claiming nothing formed an epoch that
            // carried no lease and refused every write (`EROFS`) for the
            // rest of an outage that began in that window, on the node
            // whose lease stood all along (`stress-ng-fs-faults`); the
            // activation adopts the hold (`on_epoch_state`).
            self.epoch_closed_claim(now).map(|(l, _)| l.clone())
        };
        let mut known = self.ship.max_epoch;
        if self.bk.sealed > 0 {
            known = known.max(self.bk.sealed + 1);
        }
        if let Some((l, _)) = &self.lease.held {
            known = known.max(l.epoch);
        }
        if let Some(l) = &held {
            known = known.max(l.epoch);
        }
        if self.lease.lost && self.lease.lost_floor != u64::MAX {
            known = known.max(self.lease.lost_floor);
        }
        EpochClaimView {
            held,
            reconfig_busy: self.reconfig_busy(),
            known,
            advertised_slack: self.pr.advertised,
            epoch_held: self.lease.epoch_held(),
        }
    }

    /// The lease an epoch's close let go here, while its re-claim is
    /// pending and it is usable as a held lease would be (unexpired past
    /// the margin): what this node claims for a continuation epoch
    /// meanwhile ([`Self::epoch_claim_view`]). Also while a re-claim CAS
    /// is in doubt (S3 cut again while it was in flight: the usual case),
    /// as a holder whose renewal is in doubt claims the lease it holds:
    /// if the CAS landed, the object is the later one, of this tenure
    /// too (`epoch_tenure_cas_in_doubt`), and the claim is the earlier
    /// expiry.
    pub(crate) fn epoch_closed_claim(&self, now: Ms) -> Option<&(Lease, LeaseTag)> {
        if !self.epoch_reclaim_pending(now) {
            return None;
        }
        let me = self.cfg.node_id;
        self.pr.closed_lease.as_ref().filter(|(l, _)| {
            let it = (l.holder, l.epoch, l.expires_unix_ms);
            l.holder == me
                && !l.released
                && l.expires_in_ms(now.0) > self.cfg.expiry_margin_ms as i64
                && self.pr.closed_tenure.contains(&it)
        })
    }

    /// A continuation epoch closed (S3 is back): the hold and the S3
    /// lease go locally (the next acquisition re-adopts the lease through
    /// the gate: the flush's re-claim, `epoch_reclaim_due`), the tenure's
    /// delegations with them. Its lock grants stay until that acquisition
    /// decides (`epoch_tenure_resumed`): a node that kept its lease
    /// through an S3 blip keeps its grants, where dropping them failed
    /// every lock holder's I/O with `EIO` (fenced) and its next lock with
    /// `ENOLCK` at every close (`stress-ng-fs-faults`). Safe because the
    /// grants are this lease's to answer for until somebody else holds
    /// it, and nobody can without a CAS on the very object the re-claim
    /// CASes: if the re-claim lands, nobody held it in between.
    pub(crate) fn epoch_close_release(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.cfg.node_id;
        // The tenure's objects so far: a close reported again (the
        // driver's report of a close this core made itself), or an epoch
        // hold that continued an earlier close's tenure (and a re-claim
        // in doubt meanwhile), adds to them. Empty unless grants are
        // kept, so nothing older than this tenure is in it.
        let mut tenure = std::mem::take(&mut self.pr.closed_tenure);
        let mut object = self.pr.closed_lease.take();
        let before = tenure.len();
        if self.lease.lost {
            tenure.clear();
        } else {
            if let Some((l, _)) = self
                .lease
                .held
                .as_ref()
                .filter(|(l, _)| l.holder == me && !l.released)
            {
                let it = (l.holder, l.epoch, l.expires_unix_ms);
                if !tenure.contains(&it) {
                    tenure.push(it);
                }
                object = self.lease.held.clone();
            }
            if self.lease.epoch_held() {
                if let Some(c) = self.pr.carried {
                    let c = (c.node, c.epoch, c.expires_unix_ms);
                    if !tenure.contains(&c) {
                        tenure.push(c);
                    }
                }
            }
        }
        self.lease.release_local();
        self.pr.closed_lease =
            object.filter(|(l, _)| tenure.contains(&(l.holder, l.epoch, l.expires_unix_ms)));
        let added = tenure.len() > before;
        self.pr.closed_tenure = tenure;
        if !self.pr.closed_tenure.is_empty() {
            let grants = replica.locks().grants_len();
            if grants > 0 && added {
                tracing::info!(
                    node = me,
                    grants,
                    "continuation epoch closed: lock grants kept for the lease's re-claim"
                );
            }
            self.lock_keep_grants_at_close();
        }
        self.deleg_on_lease_gone(now, replica, out);
        replica.set_holder_epoch(0);
    }

    /// This node holds again — by an acquisition whose CAS replaced
    /// `from` (`hold` false), or a hold of the carried lease `from` —
    /// after an epoch close kept its lock grants: they stand if `from`
    /// is one of the objects the close let go (the tenure continues), and
    /// are dropped otherwise. A hold keeps the list: the S3 object is
    /// still that one, for the re-claim after the hold's own close.
    pub(crate) fn epoch_tenure_resumed(
        &mut self,
        from: Option<(NodeId, Epoch, i64)>,
        hold: bool,
        replica: &dyn Replica,
    ) {
        if self.pr.closed_tenure.is_empty() {
            return;
        }
        if from.is_some_and(|f| self.pr.closed_tenure.contains(&f)) {
            tracing::debug!(
                node = self.cfg.node_id,
                grants = replica.locks().grants_len(),
                hold,
                "the lease a continuation epoch's close let go is held again: its lock grants stand"
            );
            if !hold {
                self.pr.closed_tenure.clear();
                self.pr.closed_lease = None;
            }
            return;
        }
        self.pr.closed_tenure.clear();
        self.pr.closed_lease = None;
        // Their holders honour them until they lapse (a peer's, and this
        // node's own lockers'): nothing is granted over them meanwhile,
        // should no other tenure have intervened after all.
        let until = replica
            .locks()
            .grants_snapshot()
            .iter()
            .map(|g| g.until_ms)
            .max();
        let n = replica.locks().clear_grants();
        if let Some(until) = until {
            replica.locks().set_quarantine(until);
        }
        if n > 0 {
            tracing::info!(
                node = self.cfg.node_id,
                grants = n,
                "the lease a continuation epoch's close let go was not re-claimed: lock grants dropped"
            );
        }
    }

    /// A lease CAS replacing `prev` failed in doubt: had it landed, `sent`
    /// is the S3 object now, written by this node over a lease of the
    /// tenure an epoch's close kept the grants of — the same tenure. The
    /// next acquisition then replaces `sent`, and must keep the grants
    /// (a cut that began while the re-claim CAS was in flight dropped
    /// them, and released the lease).
    pub(crate) fn epoch_tenure_cas_in_doubt(&mut self, prev: &Lease, sent: &Lease) {
        let p = (prev.holder, prev.epoch, prev.expires_unix_ms);
        let s = (sent.holder, sent.epoch, sent.expires_unix_ms);
        if !prev.released
            && !sent.released
            && sent.holder == self.cfg.node_id
            && self.pr.closed_tenure.contains(&p)
            && !self.pr.closed_tenure.contains(&s)
        {
            tracing::debug!(
                node = self.cfg.node_id,
                epoch = sent.epoch,
                "a lease CAS of a kept tenure is in doubt: its object is the tenure's too"
            );
            self.pr.closed_tenure.push(s);
        }
    }

    /// The lease object [`Self::epoch_closed_claim`] claims, when the
    /// activation's carrier is exactly it.
    pub(crate) fn epoch_closed_carried(&self, now: Ms) -> Option<(Lease, LeaseTag)> {
        let c = self.pr.carried?;
        self.epoch_closed_claim(now)
            .filter(|(l, _)| {
                (c.node, c.epoch, c.expires_unix_ms) == (l.holder, l.epoch, l.expires_unix_ms)
            })
            .cloned()
    }

    /// This node closed an epoch holding a lease that still stands (as
    /// far as it knows: unexpired past the margin) and has not acquired
    /// since: its re-claim is pending (meanwhile it claims that lease for
    /// a continuation epoch: [`Self::epoch_closed_claim`]).
    pub(crate) fn epoch_reclaim_pending(&self, now: Ms) -> bool {
        !self.lease.lost
            && self.lease.held.is_none()
            && !self.lease.epoch_held()
            && self
                .pr
                .closed_tenure
                .iter()
                .any(|(_, _, expires)| *expires - self.cfg.expiry_margin_ms as i64 > now.0)
    }

    /// This node lets its epoch hold go for good (a handoff, the close):
    /// record the carried lease so nothing adopts that hold again here.
    /// `closed`: it closed the epoch itself (S3 is back), and owes the
    /// carried lease's re-claim (`epoch_reclaim_due`). `false` if it
    /// could not be persisted.
    pub(crate) fn end_epoch_hold(&mut self, replica: &dyn Replica, closed: bool) -> bool {
        let Some(c) = self.pr.carried else {
            return true;
        };
        let ended = Some((c.epoch, c.expires_unix_ms, closed));
        let same = self
            .pr
            .hold_ended
            .is_some_and(|(e, x, _)| (e, x) == (c.epoch, c.expires_unix_ms));
        if same && !closed {
            return true;
        }
        if self.pr.hold_ended == ended {
            return true;
        }
        if let Err(error) = replica.persist_epoch_hold_ended(ended) {
            tracing::warn!(node = self.cfg.node_id, %error, "persisting the ended epoch hold failed");
            return false;
        }
        self.pr.hold_ended = ended;
        true
    }

    /// Whether the carried lease `hold_ended` names is `c`'s.
    pub(crate) fn hold_ended_is(&self, epoch: Epoch, expires_unix_ms: i64) -> bool {
        self.pr
            .hold_ended
            .is_some_and(|(e, x, _)| (e, x) == (epoch, expires_unix_ms))
    }

    /// This node closed its epoch as the hold owner and the carried lease
    /// — its own — still stands in S3 as far as it knows: it re-claims it.
    /// Members close only once the carried lease has moved (the rule in
    /// `jobs::epoch_carrier_checked`); a hold owner with nothing to flush
    /// used to leave it standing, expired, and every member waited for
    /// good (flex-crash seed 16755). The re-claim needs no promises (its
    /// own lease), and an idle holder releases it later as usual.
    pub(crate) fn epoch_reclaim_due(&self) -> bool {
        let Some((epoch, expires, true)) = self.pr.hold_ended else {
            return false;
        };
        let carried_mine = self.pr.carried.is_some_and(|c| {
            c.node == self.cfg.node_id && (c.epoch, c.expires_unix_ms) == (epoch, expires)
        });
        let moved = self.lease.last_seen.as_ref().is_some_and(|l| {
            l.released
                || l.holder != self.cfg.node_id
                || (l.epoch, l.expires_unix_ms) != (epoch, expires)
        });
        carried_mine && !moved && !self.lease.lost && !self.epoch.open && self.lease.held.is_none()
    }

    /// The re-claim `epoch_reclaim_due` asked for is done (or moot): the
    /// lease object is no longer the carried one.
    fn epoch_reclaim_settle(&mut self, replica: &dyn Replica) {
        let Some((epoch, expires, true)) = self.pr.hold_ended else {
            return;
        };
        let moved = self.lease.last_seen.as_ref().is_some_and(|l| {
            l.released
                || l.holder != self.cfg.node_id
                || (l.epoch, l.expires_unix_ms) != (epoch, expires)
        }) || self
            .lease
            .held
            .as_ref()
            .is_some_and(|(l, _)| (l.epoch, l.expires_unix_ms) != (epoch, expires));
        if moved
            && replica
                .persist_epoch_hold_ended(Some((epoch, expires, false)))
                .is_ok()
        {
            self.pr.hold_ended = Some((epoch, expires, false));
        }
    }

    /// Take the continuation epoch's hold at `epoch`, and capture under
    /// it. The hold owner's journal is speculation like any holder's
    /// (ADR-19): its transactions are captured with their before-images,
    /// so the ship plan after the close skips exactly a transaction that
    /// waits for a member's chunk (and its dependents) instead of
    /// everything journaled after it, the publisher substitutes them,
    /// and a deposed hold owner rolls back and replays by rid instead of
    /// rebuilding from the head commit. Before this, the hold set
    /// `Meta::holder_epoch` to 0 (since M5) and journaled uncaptured:
    /// one member away with the only copy of a chunk stalled the whole
    /// cluster's log at its pre-epoch head (`epoch-member-dies-with-chunk`).
    ///
    /// `epoch` is the epoch the flush will ship under, so nothing of the
    /// hold's journal is stranded by the flush's own acquisition gate
    /// (`strand_for_takeover(gate.epoch)`), and a member's position
    /// `(epoch, jseq)` is reached by the flush's segments and by nothing
    /// earlier: see [`Self::epoch_hold_epoch_for`].
    pub(crate) fn adopt_epoch_hold(&mut self, now: Ms, epoch: Epoch, replica: &dyn Replica) {
        let carried = self
            .pr
            .carried
            .map(|c| (c.node, c.epoch, c.expires_unix_ms));
        self.epoch_tenure_resumed(carried, true, replica);
        self.lease.adopt_epoch_hold(now, epoch);
        replica.set_holder_epoch(self.lease.epoch_hold().unwrap_or(0));
    }

    /// The epoch a hold this node takes over P2P journals under: the
    /// epoch its flush CAS grants once the epoch closes
    /// (`LeaseState::granted_lease`'s rule on the carried lease object,
    /// which nothing else can touch while the epoch is open — its
    /// members promise nothing, and a taker needs their promises). The
    /// carrier re-adopts its own lease at the same epoch; anyone else
    /// takes it over at the next one. A chain of transfers stays at
    /// that next epoch (the object is still the carrier's). Without the
    /// carrier known (no activation seen), the giver's epoch.
    pub(crate) fn epoch_hold_epoch_for(&self, giver_epoch: Epoch) -> Epoch {
        match self.pr.carried {
            Some(c) if c.node == self.cfg.node_id => c.epoch.max(1),
            Some(c) => c.epoch.max(1) + 1,
            None => giver_epoch.max(1),
        }
    }

    /// A restarted hold owner: its epoch is still open, so it re-adopts
    /// the hold it persisted (it alone journaled under it; the flush its
    /// members wait for is its to do).
    pub(crate) fn restore_epoch_hold(&mut self, now: Ms, replica: &dyn Replica) {
        let Some(epoch) = self.pr.restored_hold else {
            return;
        };
        if !self.epoch.open {
            self.pr.restored_hold = None;
            return;
        }
        if self.lease.epoch_held() || self.lease.lost {
            return;
        }
        self.pr.restored_hold = None;
        if self.pr.hold_ended.map(|(e, _, _)| e) == Some(epoch) {
            // Handed away before the crash (persisted before the reply):
            // the successor owns it.
            return;
        }
        tracing::info!(
            node = self.cfg.node_id,
            epoch,
            "re-adopting the persisted epoch hold"
        );
        self.skip_ship = true;
        self.adopt_epoch_hold(now, epoch, replica);
    }

    /// The activation's resolution says this node's claim is stale.
    pub(crate) fn epoch_claim_stale(&self, stale_below: Epoch) -> bool {
        match &self.lease.held {
            Some((l, _)) if !self.lease.lost => l.epoch < stale_below,
            _ => false,
        }
    }

    /// The activation named this node's lease, exactly as held, as the
    /// one the epoch carries.
    pub(crate) fn carries_mine(&self) -> bool {
        match (&self.lease.held, self.pr.carried) {
            (Some((l, _)), Some(c)) => {
                c.node == self.cfg.node_id
                    && c.node == l.holder
                    && c.epoch == l.epoch
                    && c.expires_unix_ms == l.expires_unix_ms
            }
            _ => false,
        }
    }

    // ---- the takeover check ----

    /// Whether claiming `prev` needs the promise check.
    pub(crate) fn promise_check_needed(&mut self, now: Ms, prev: &Lease) -> bool {
        if !self.cfg.takeover_promise_check {
            return false;
        }
        if prev.holder == 0 || prev.holder == self.cfg.node_id || prev.released {
            return false;
        }
        if !prev.is_expired(now.0) {
            // M9's permit paths (a sealed backup, `ack=s3`): the claim
            // rule keeps them apart from epochs.
            return false;
        }
        if self.pr.carried.is_some_and(|c| {
            c.node == prev.holder
                && c.epoch == prev.epoch
                && c.expires_unix_ms == prev.expires_unix_ms
        }) {
            self.stats.promise_flush_exempt += 1;
            return false;
        }
        true
    }

    /// Begin the check for `prev` (the job is in `Phase::PromiseCheck`).
    pub(crate) fn promise_check_begin(&mut self, now: Ms, prev: &Lease, out: &mut Vec<Action>) {
        self.stats.promise_checks += 1;
        tracing::debug!(
            node = self.cfg.node_id,
            prev_holder = prev.holder,
            prev_epoch = prev.epoch,
            prev_expires = prev.expires_unix_ms,
            carried = ?self.pr.carried,
            "takeover needs the promise check"
        );
        let mut check = Check {
            holder: prev.holder,
            expires: prev.expires_unix_ms,
            asked: BTreeMap::new(),
            replies: BTreeMap::new(),
            heartbeats: None,
            timer: None,
            timed_out: false,
            asked_any: false,
        };
        self.issue_s3(S3Op::HeartbeatRead, S3For::Heartbeats, out);
        // At `f = 0` only the heartbeat read (a larger advertised slack
        // would show there); the requests go out only if it does.
        if self.cfg.epoch_slack > 0 {
            self.ask_for_promises(&mut check, out);
        }
        check.timer =
            Some(self.set_timer(now.plus(self.cfg.promise_wait_ms), Timer::PromiseWait, out));
        tracing::debug!(
            node = self.cfg.node_id,
            holder = prev.holder,
            epoch = prev.epoch,
            asked = check.asked.len(),
            "takeover: promise check"
        );
        self.pr.check = Some(check);
    }

    fn ask_for_promises(&mut self, check: &mut Check, out: &mut Vec<Action>) {
        check.asked_any = true;
        if !self.cfg.p2p {
            return;
        }
        let peers: Vec<NodeId> = self
            .roster
            .iter()
            .copied()
            .filter(|n| *n != self.cfg.node_id)
            .collect();
        for peer in peers {
            let req = self.op_id();
            check.asked.insert(req, peer);
            self.stats.promise_requests_sent += 1;
            out.push(Action::Send {
                to: peer,
                msg: PeerMsg::PromiseRequest {
                    req,
                    expires_unix_ms: check.expires,
                },
            });
        }
    }

    pub(crate) fn on_heartbeats(
        &mut self,
        now: Ms,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(check) = self.pr.check.as_mut() else {
            return;
        };
        check.heartbeats = Some(match result {
            S3Result::Heartbeats(Ok(v)) => Ok(v),
            S3Result::Heartbeats(Err(e)) => Err(e.0),
            _ => Err("unexpected result".into()),
        });
        self.promise_check_evaluate(now, replica, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_promise_reply(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        until: Option<i64>,
        epoch_slack: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(check) = self.pr.check.as_mut() else {
            return;
        };
        let Some(peer) = check.asked.remove(&req) else {
            return;
        };
        let _ = from;
        if let Some(until) = until {
            check.replies.insert(peer, (until, epoch_slack));
        } else {
            // A refusal still tells the slack the peer runs with.
            check.replies.insert(peer, (0, epoch_slack));
        }
        self.promise_check_evaluate(now, replica, out);
    }

    pub(crate) fn on_promise_request_failed(
        &mut self,
        now: Ms,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(check) = self.pr.check.as_mut() else {
            return false;
        };
        if check.asked.remove(&req).is_none() {
            return false;
        }
        self.promise_check_evaluate(now, replica, out);
        true
    }

    pub(crate) fn on_promise_wait(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(check) = self.pr.check.as_mut() else {
            return;
        };
        check.timer = None;
        check.timed_out = true;
        self.promise_check_evaluate(now, replica, out);
    }

    /// Decide once the heartbeats are read: allowed as soon as enough
    /// promises outlast the expiry; refused once nothing else can arrive
    /// (every request answered or failed, or the wait ran out).
    fn promise_check_evaluate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(check) = self.pr.check.as_ref() else {
            return;
        };
        let Some(heartbeats) = check.heartbeats.as_ref() else {
            return;
        };
        let hb = match heartbeats {
            Ok(v) => v.clone(),
            Err(e) => {
                tracing::debug!(node = self.cfg.node_id, error = %e, "heartbeat read failed");
                return self.promise_check_finish(now, false, replica, out);
            }
        };
        // Replies count like heartbeats (both are persisted promises).
        let mut all = hb;
        for (node, (until, slack)) in &check.replies {
            all.push((*node, Promise::new(*node, *until, *slack, 0)));
        }
        let slack = effective_slack(self.cfg.epoch_slack, &self.roster, &all);
        let verdict = takeover_check(
            slack,
            self.cfg.node_id,
            &self.roster,
            check.holder,
            check.expires,
            &all,
        );
        if !verdict.allows() && !check.asked_any {
            // `f = 0` here, but a roster node still advertises more.
            let mut check = self.pr.check.take().expect("checked above");
            self.ask_for_promises(&mut check, out);
            self.pr.check = Some(check);
            return;
        }
        let check = self.pr.check.as_ref().expect("checked above");
        let settled = check.asked.is_empty() || check.timed_out;
        if verdict.allows() {
            tracing::info!(
                node = self.cfg.node_id,
                slack,
                ?verdict,
                "takeover: promise check passed"
            );
            self.promise_check_finish(now, true, replica, out);
        } else if settled {
            tracing::warn!(
                node = self.cfg.node_id,
                slack,
                ?verdict,
                "takeover refused: too few promises outlast the lease (a continuation \
                 epoch may hold it)"
            );
            self.stats.takeovers_refused_promises += 1;
            self.promise_check_finish(now, false, replica, out);
        }
    }

    fn promise_check_finish(
        &mut self,
        now: Ms,
        allowed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(check) = self.pr.check.take() {
            if let Some(id) = check.timer {
                self.cancel_timer(id, out);
            }
        }
        self.promise_check_resolved(now, allowed, replica, out);
    }

    /// The acquisition job left the check (it finished another way).
    pub(crate) fn promise_check_abandon(&mut self, out: &mut Vec<Action>) {
        if let Some(check) = self.pr.check.take() {
            if let Some(id) = check.timer {
                self.cancel_timer(id, out);
            }
        }
    }
}
