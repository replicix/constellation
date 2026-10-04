//! Continuation-epoch coordinator for the mount daemon (DESIGN.md §5.3).
//!
//! Wraps [`constellation_net::EpochMachine`] with SQLite persistence and
//! the P2P propose/ack/activate exchange. FUSE threads only read the
//! three atomics (`active`, `frozen`, `blocks_takeover`).
//!
//! Plan 30 §M10 (flexible quorums): with `epoch_slack = f`, an epoch
//! needs `N − f` members of the write-eligible roster
//! ([`constellation_net::component_quorum`]; all of it at `f = 0`), and a
//! node proposes or joins only once its own last issued heartbeat promise
//! has expired — checked and gated in one replica transaction
//! (`Meta::promise_join_begin`), so no promise can be issued between the
//! check and the join. Each ack carries the member's lease claim and the
//! highest epoch it knows of; the proposer resolves them
//! ([`constellation_authority::resolve_epoch_claims`]) into the lease the epoch
//! carries and the epoch below which a member's claim is stale, and the
//! activation carries both to every member's core. With `f > 0` a node
//! forms or joins only after a heartbeat advertising `f` has landed
//! (a taker honours the largest slack advertised). When the carrier's
//! node is retired (admin `leave --node-id`, which fences its lease), the
//! members that do not own the hold abandon the epoch: the flush it was
//! waiting for will never come.

use anyhow::Result;
use constellation_authority::EpochClaimView;
use constellation_meta::Meta;
use constellation_net::{
    component_quorum, EpochActivation, EpochCarrier, EpochClaim, EpochMachine, EpochPromise,
    Payload,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// `local` key of the current (or last) epoch's resolution.
const CARRIER_KEY: &str = "epoch_carrier";

pub struct EpochManager {
    node_id: u64,
    meta: Arc<Meta>,
    peers: constellation_net::Peers,
    machine: Mutex<EpochMachine>,
    roster: Mutex<Vec<u64>>,
    s3_failure_since: Mutex<Option<std::time::Instant>>,
    /// EC2 follow-up 3c: at this node's last proposal attempt a live
    /// member said its S3 works (see `maybe_propose`); cleared by this
    /// node's next successful round or a proposal attempt that found none.
    peers_reach_s3: std::sync::atomic::AtomicBool,
    /// Proposals made (`status.epoch.proposals`).
    proposals: std::sync::atomic::AtomicU64,
    /// How long S3 must have been failing before a proposal (the sync
    /// interval at least: one full round's worth of failures, not a
    /// blip inside one).
    propose_grace: Mutex<std::time::Duration>,
    pub active: Arc<AtomicBool>,
    pub frozen: Arc<AtomicBool>,
    /// The FUSE write gate (`EROFS`): the epoch is frozen, or it is active
    /// but carries no lease — nobody in it holds authority, so no write
    /// can execute until S3 returns and the epoch closes (the
    /// `epoch-member-lost` fix: such writes used to wait out the acquire
    /// deadline and answer `EIO`).
    pub writes_refused: Arc<AtomicBool>,
    /// The members of the active (or frozen) epoch, else empty: they
    /// serve one another the chunks their epoch writes named, which
    /// nothing can upload before the close (`Coop::set_epoch_members`).
    pub members_open: Arc<Mutex<Vec<u64>>>,
    pub blocks_takeover: Arc<AtomicBool>,
    flushing: AtomicBool,
    /// Plan 30 §M10: `f`.
    slack: AtomicU32,
    /// The core's claim, mirrored by the driver after every step.
    claim: Mutex<EpochClaimView>,
    /// The current (or last) epoch's `(carrier, stale_below)`.
    carrier: Mutex<(Option<EpochCarrier>, u64)>,
    /// No new proposal before this (a declined one backs off).
    propose_after: Mutex<Option<std::time::Instant>>,
}

/// How long a proposer waits after a declined proposal before the next.
const PROPOSE_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

/// After a live member answered that it reaches S3 (`PingS3`): the next
/// proposal attempt waits this long (every attempt makes each member probe
/// S3; a bucket outage beginning meanwhile costs at most this much).
const REACHES_S3_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a proposer waits for each member's answer. A member probes
/// S3 before it answers (`handle_propose_checked`, bounded by the
/// daemon's 300 ms probe), so the P2P default (500 ms) would leave too
/// little room: a member that accepted just after the proposer gave up
/// would keep a promise nobody activates.
const PROPOSE_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

impl EpochManager {
    pub fn new(node_id: u64, meta: Arc<Meta>, peers: constellation_net::Peers) -> Self {
        let loaded = meta
            .load_open_epoch()
            .ok()
            .flatten()
            .map(|(id, members, base, at, state)| {
                let mut p = EpochPromise::new(id, members, base, at);
                p.state = match state.as_str() {
                    "active" => constellation_net::EpochState::Active,
                    "frozen" => constellation_net::EpochState::Frozen,
                    "closed" => constellation_net::EpochState::Closed,
                    _ => constellation_net::EpochState::Promised,
                };
                p
            });
        let machine = loaded.map(EpochMachine::from_promise).unwrap_or_default();
        let carrier = meta
            .kv_get(CARRIER_KEY)
            .ok()
            .flatten()
            .and_then(|v| decode_carrier(&v))
            .unwrap_or((None, 0));
        let mgr = Self {
            node_id,
            meta,
            peers,
            machine: Mutex::new(machine),
            roster: Mutex::new(Vec::new()),
            s3_failure_since: Mutex::new(None),
            peers_reach_s3: std::sync::atomic::AtomicBool::new(false),
            proposals: std::sync::atomic::AtomicU64::new(0),
            propose_grace: Mutex::new(std::time::Duration::from_millis(500)),
            active: Arc::new(AtomicBool::new(false)),
            frozen: Arc::new(AtomicBool::new(false)),
            writes_refused: Arc::new(AtomicBool::new(false)),
            members_open: Arc::new(Mutex::new(Vec::new())),
            blocks_takeover: Arc::new(AtomicBool::new(false)),
            flushing: AtomicBool::new(false),
            slack: AtomicU32::new(0),
            claim: Mutex::new(EpochClaimView::default()),
            carrier: Mutex::new(carrier),
            propose_after: Mutex::new(None),
        };
        mgr.sync_flags();
        mgr
    }

    fn sync_flags(&self) {
        let m = self.machine.lock().unwrap();
        self.active.store(m.is_active(), Ordering::Relaxed);
        self.frozen.store(m.is_frozen(), Ordering::Relaxed);
        let carrierless = m.is_active() && self.carrier.lock().unwrap().0.is_none();
        self.writes_refused
            .store(m.is_frozen() || carrierless, Ordering::Relaxed);
        *self.members_open.lock().unwrap() = match m.current() {
            Some(p) if m.is_active() || m.is_frozen() => p.members.clone(),
            _ => Vec::new(),
        };
        self.blocks_takeover
            .store(m.blocks_s3_takeover(), Ordering::Relaxed);
        if let Some(p) = m.current() {
            let state = match p.state {
                constellation_net::EpochState::Promised => "promised",
                constellation_net::EpochState::Active => "active",
                constellation_net::EpochState::Frozen => "frozen",
                constellation_net::EpochState::Closed => "closed",
            };
            let _ = self
                .meta
                .persist_epoch(&p.epoch_id, &p.members, &p.base, p.promised_at, state);
        }
        if !m.is_open() {
            // Plan 30 §M10: in no open epoch, promises may be issued again.
            let _ = self.meta.promise_join_end();
        }
    }

    /// Plan 30 §M10: `meta.json`'s `epoch_slack`.
    pub fn set_slack(&self, epoch_slack: u32) {
        self.slack.store(epoch_slack, Ordering::Relaxed);
    }

    pub fn slack(&self) -> u32 {
        self.slack.load(Ordering::Relaxed)
    }

    /// The driver mirrors the core's claim here after every step.
    pub fn set_claim_view(&self, view: EpochClaimView) {
        *self.claim.lock().unwrap() = view;
    }

    /// The current (or last) epoch's carried lease and stale floor.
    pub fn carrier(&self) -> (Option<EpochCarrier>, u64) {
        *self.carrier.lock().unwrap()
    }

    /// Called with the machine lock held, by the activation the carrier
    /// belongs to (lock order: machine, then carrier, as `sync_flags`).
    fn set_carrier(&self, carrier: Option<EpochCarrier>, stale_below: u64) {
        *self.carrier.lock().unwrap() = (carrier, stale_below);
        let _ = self
            .meta
            .kv_set(CARRIER_KEY, &encode_carrier(carrier, stale_below));
    }

    /// This node's ack for an epoch of `members`: `(claim, known)`.
    fn my_claim(&self, members: &[u64]) -> (Option<EpochClaim>, u64) {
        let view = self.claim.lock().unwrap();
        tracing::debug!(node = self.node_id, ?view, "epoch claim");
        let claim = view
            .claim(members)
            .map(|(epoch, expires_unix_ms, may_carry)| EpochClaim {
                epoch,
                expires_unix_ms,
                may_carry,
            });
        (claim, view.known)
    }

    /// Whether this node may take part in an epoch under its own slack:
    /// `members` is at least `N − f` of the roster and contains this
    /// node, and with `f > 0` a heartbeat advertising `f` has landed.
    fn quorum_ok(&self, members: &[u64]) -> bool {
        let roster = self.roster();
        let f = self.slack();
        if roster.is_empty() || !members.contains(&self.node_id) {
            return false;
        }
        if !members.iter().all(|m| roster.contains(m)) {
            return false;
        }
        let Some(quorum) = constellation_store_s3::heartbeat::epoch_quorum(roster.len(), f) else {
            return false;
        };
        if members.len() < quorum {
            return false;
        }
        f == 0 || self.claim.lock().unwrap().advertised_slack == Some(f)
    }

    pub fn status(&self) -> constellation_control::proto::types::EpochStatus {
        let m = self.machine.lock().unwrap();
        let (active, epoch_id, members) = m.status();
        constellation_control::proto::types::EpochStatus {
            active,
            epoch_id,
            members,
            epoch_slack: self.slack(),
            carrier: self.carrier().0.map(|c| c.node),
            promise_until_ms: self.meta.promise_issued().unwrap_or(0),
            own_s3_outage: self.peers_reach_s3(),
            proposals: self.proposals.load(Ordering::Relaxed),
            ..Default::default()
        }
    }

    pub fn writes_ok(&self) -> bool {
        self.active.load(Ordering::Relaxed) && !self.frozen.load(Ordering::Relaxed)
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Relaxed)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn is_open(&self) -> bool {
        self.blocks_takeover.load(Ordering::Relaxed)
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }

    pub fn finish_flushing(&self) {
        self.flushing.store(false, Ordering::Relaxed);
    }

    /// Whether this node's own last S3 round failed (and none has
    /// succeeded since): it is in the outage itself, and joins a proposal
    /// without probing.
    pub fn s3_failing(&self) -> bool {
        self.s3_failure_since.lock().unwrap().is_some()
    }

    pub fn note_s3_success(&self) {
        *self.s3_failure_since.lock().unwrap() = None;
        self.peers_reach_s3.store(false, Ordering::Relaxed);
    }

    /// EC2 follow-up 3c: a live member answered that its S3 works, so this
    /// node's S3 failure is its own, not a bucket outage (for `status`
    /// and tests).
    pub fn peers_reach_s3(&self) -> bool {
        self.peers_reach_s3.load(Ordering::Relaxed)
    }

    /// See `propose_grace`: never below 500 ms.
    pub fn set_propose_grace(&self, grace: std::time::Duration) {
        *self.propose_grace.lock().unwrap() = grace.max(std::time::Duration::from_millis(500));
    }

    pub fn set_roster(&self, ids: Vec<u64>) {
        self.abandon_if_carrier_retired(&ids);
        *self.roster.lock().unwrap() = ids;
    }

    pub fn roster(&self) -> Vec<u64> {
        self.roster.lock().unwrap().clone()
    }

    /// Persist a promise (BEFORE any ack is sent) then reply. Plan 30
    /// §M10: only under this node's own quorum rule, and only once its
    /// own last issued heartbeat promise has expired (the join gate).
    /// (The daemon calls [`Self::handle_propose_checked`] with its S3
    /// probe; this is the tests' form, S3 unreachable.)
    #[cfg(test)]
    pub fn handle_propose(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        proposer: u64,
        proposer_slack: u32,
    ) -> Payload {
        self.handle_propose_checked(epoch_id, members, base, proposer, proposer_slack, false)
    }

    /// [`Self::handle_propose`] with the member's own S3 reachability
    /// (probed by the caller just before): a member that reaches S3
    /// declines. A continuation epoch is for a bucket outage; a proposer
    /// cut from S3 on its own is not one. Joining would be harmful, not
    /// just pointless: a member of an open epoch runs no seal watch and
    /// no S3 acquisition (M10's rule (b), which keeps fast takeovers away
    /// from epochs), so if the proposer — typically the holder, whose
    /// lease the epoch carries — then died, this member would stay
    /// frozen (`EROFS`) with S3 reachable, until the holder came back.
    /// Declined, the proposer keeps acknowledging through its M9 backup
    /// (if any) until its lease runs out, and a dead holder is replaced by
    /// a seal (M9) or the TTL takeover (M10's promise check). Declining
    /// is always safe: an epoch that does not form claims nothing. The
    /// simulation's epoch coordinator has always formed epochs among the
    /// S3-cut nodes only (`tests/sim/epochs.rs`).
    pub fn handle_propose_checked(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        proposer: u64,
        proposer_slack: u32,
        member_reaches_s3: bool,
    ) -> Payload {
        let refuse = |why: &str| {
            tracing::info!(
                epoch_id,
                proposer,
                proposer_slack,
                why,
                "declined an epoch proposal"
            );
            Payload::EpochAck {
                epoch_id: epoch_id.clone(),
                member: self.node_id,
                accepted: false,
                claim: None,
                known: 0,
            }
        };
        // The proposer applied the quorum rule to its roster. Re-check it
        // here only when the proposer runs with a larger slack than this
        // node does (a change of `f` in flight): then this node's stricter
        // rule decides. Otherwise the proposer's view — as fresh as ours,
        // or fresher (a leave this node has not read yet) — stands, as it
        // always did.
        if member_reaches_s3 {
            return refuse("this member reaches S3: not a bucket outage");
        }
        let own = self.slack();
        if proposer_slack > own && !self.quorum_ok(&members) {
            return refuse("not a quorum under this node's slack");
        }
        if !members.contains(&self.node_id) {
            return refuse("not a member");
        }
        if own > 0 && self.claim.lock().unwrap().advertised_slack != Some(own) {
            return refuse("this node's slack is not advertised yet");
        }
        match self.meta.promise_join_begin(now_ms()) {
            Ok(true) => {}
            Ok(false) => return refuse("this node's own promise has not expired"),
            Err(_) => return refuse("join gate failed"),
        }
        let base: BTreeMap<String, u64> = base.into_iter().collect();
        let p = EpochPromise::new(epoch_id.clone(), members.clone(), base, now_ms());
        // Chunk epoch-liveness-gap: the open epoch's carrier, closed (it
        // proposes) and cut from S3 again before its re-claim landed,
        // proposes a fresh epoch, and this member (not the hold's owner)
        // takes it in place of the open one
        // (`EpochMachine::persist_promise_proposed`). The carrier and the
        // hold are read under the machine lock, which `handle_activate`
        // also holds while it sets the carrier with the activation: the
        // carrier read is always the open epoch's own, never the previous
        // epoch's beside a just-activated one.
        let (accepted, replaced) = {
            let mut m = self.machine.lock().unwrap();
            let carrier = self.carrier().0.map(|c| c.node);
            let holds = self.claim.lock().unwrap().epoch_held;
            let open = m
                .current()
                .filter(|_| m.is_open())
                .map(|c| c.epoch_id.clone());
            let accepted = m
                .persist_promise_proposed(p, proposer, carrier, holds)
                .is_ok();
            (accepted, open.filter(|id| accepted && *id != epoch_id))
        };
        self.sync_flags();
        if let Some(replaced) = replaced {
            // `sync_flags` persisted the new epoch; the replaced one's row
            // would otherwise stay open on disk and be loaded again at a
            // restart once the new one closes. Closed after the new one is
            // written: a crash in between loads the new one (the later
            // promise).
            let _ = self.meta.set_epoch_state(&replaced, "closed");
            tracing::info!(
                epoch_id,
                replaced,
                proposer,
                "joined a proposal in place of the open epoch"
            );
        }
        let (claim, known) = self.my_claim(&members);
        Payload::EpochAck {
            epoch_id,
            member: self.node_id,
            accepted,
            claim: accepted.then_some(claim).flatten(),
            known,
        }
    }

    pub fn handle_activate(&self, activation: EpochActivation) {
        // The carrier is set under the machine lock that activates: a
        // proposal handled meanwhile (`handle_propose_checked`) sees either
        // the promised epoch or the active one with its own carrier.
        {
            let mut m = self.machine.lock().unwrap();
            if m.activate(&activation.epoch_id).is_ok() {
                self.set_carrier(activation.carrier, activation.stale_below);
            }
        }
        self.sync_flags();
        tracing::info!(
            epoch_id = activation.epoch_id,
            carrier = ?activation.carrier,
            stale_below = activation.stale_below,
            "continuation epoch activated"
        );
    }

    pub async fn check_liveness(&self) {
        if !self.is_open() {
            return;
        }
        let members = self
            .machine
            .lock()
            .unwrap()
            .current()
            .map(|p| p.members.clone())
            .unwrap_or_default();
        let mut live = vec![self.node_id];
        for id in members.iter().copied().filter(|id| *id != self.node_id) {
            if self.peers.ping_node(id).await {
                live.push(id);
            }
        }
        self.machine.lock().unwrap().note_live_members(&live);
        self.sync_flags();
        if self.is_frozen() {
            tracing::error!(
                missing = ?members.iter().filter(|m| !live.contains(m)).collect::<Vec<_>>(),
                "continuation epoch frozen: lost contact with a member (EROFS)"
            );
        }
    }

    /// If S3 is down and the live component covers the roster, propose.
    pub async fn maybe_propose(&self, base: BTreeMap<String, u64>) -> Result<bool> {
        if self.is_open() {
            return Ok(self.is_active());
        }
        if self
            .propose_after
            .lock()
            .unwrap()
            .is_some_and(|t| std::time::Instant::now() < t)
        {
            return Ok(false);
        }
        {
            let now = std::time::Instant::now();
            let mut since = self.s3_failure_since.lock().unwrap();
            match *since {
                None => {
                    *since = Some(now);
                    return Ok(false);
                }
                Some(first) if now.duration_since(first) < *self.propose_grace.lock().unwrap() => {
                    return Ok(false);
                }
                Some(_) => {}
            }
        }
        let roster = self.roster();
        if roster.is_empty() {
            return Ok(false);
        }
        // A single-node roster covers itself even with P2P disabled.
        // Multi-node needs successful current pings; cached status is
        // deliberately insufficient for this safety decision.
        let mut live = vec![self.node_id];
        if roster.len() > 1 {
            // Plan 30 §M10: in parallel — a missing node's ping runs to its
            // timeout, and with `f > 0` the epoch must still form while
            // the holder's lease is usable.
            let others: Vec<u64> = roster
                .iter()
                .copied()
                .filter(|id| *id != self.node_id)
                .collect();
            let answers =
                futures::future::join_all(others.iter().map(|id| self.peers.ping_node_s3(*id)))
                    .await;
            // EC2 follow-up 3c: a live member whose S3 probe just
            // succeeded (`PingS3`) says this is not a bucket outage — only this node lost S3.
            // Such a proposal is declined anyway (a member that reaches S3
            // declines), and meanwhile this node's promise holds its epoch
            // open: no S3 acquisition, rounds that only probe — its
            // closes stalled. So no proposal this time (each attempt asks
            // again: in a real bucket outage the members' probes fail,
            // within `EPOCH_MEMBER_S3_PROBE`, and the epoch forms).
            if let Some(reaching) = others
                .iter()
                .zip(&answers)
                .find(|(_, a)| **a == Some(true))
                .map(|(id, _)| *id)
            {
                self.peers_reach_s3.store(true, Ordering::Relaxed);
                // Each attempt costs every member an S3 probe: ask again
                // in a second, not every failed round.
                *self.propose_after.lock().unwrap() =
                    Some(std::time::Instant::now() + REACHES_S3_BACKOFF);
                tracing::info!(
                    member = reaching,
                    "not proposing a continuation epoch: a live member reaches S3 (only this node's S3 is away)"
                );
                return Ok(false);
            }
            self.peers_reach_s3.store(false, Ordering::Relaxed);
            live.extend(
                others
                    .iter()
                    .zip(answers)
                    .filter(|(_, up)| up.is_some())
                    .map(|(id, _)| *id),
            );
        }
        let f = self.slack();
        let Some(members) = component_quorum(self.node_id, &live, &roster, f) else {
            return Ok(false);
        };
        if !self.quorum_ok(&members) {
            return Ok(false);
        }
        if !self
            .meta
            .promise_join_begin(now_ms())
            .map_err(|e| anyhow::anyhow!("{e}"))?
        {
            tracing::debug!("not proposing an epoch: this node's promise has not expired");
            return Ok(false);
        }
        self.proposals.fetch_add(1, Ordering::Relaxed);
        let epoch_id = format!("{}-{}", self.node_id, now_ms());
        let p = EpochPromise::new(epoch_id.clone(), members.clone(), base.clone(), now_ms());
        {
            let mut m = self.machine.lock().unwrap();
            m.persist_promise(p).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        self.sync_flags();
        let base_vec: Vec<(String, u64)> = base.into_iter().collect();
        let (claim, known) = self.my_claim(&members);
        let mut acks = vec![(self.node_id, claim, known)];
        for id in members.iter().copied().filter(|id| *id != self.node_id) {
            let payload = Payload::EpochPropose {
                epoch_id: epoch_id.clone(),
                members: members.clone(),
                base: base_vec.clone(),
                proposer: self.node_id,
                epoch_slack: f,
            };
            match self
                .peers
                .request_to_node_timeout(id, &payload, PROPOSE_REQUEST_TIMEOUT)
                .await
            {
                Ok(Payload::EpochAck {
                    accepted: true,
                    member,
                    claim,
                    known,
                    ..
                }) if member == id => acks.push((member, claim, known)),
                other => {
                    tracing::warn!(peer = id, ?other, "epoch propose not acked");
                    self.abandon_own_proposal(&epoch_id);
                    // The members that acked — and the one whose answer
                    // was lost or late — may hold a promise nobody will
                    // activate: tell every member (best effort; one that
                    // misses it keeps its promise, as before this message
                    // existed).
                    let abort = Payload::EpochAbort {
                        epoch_id: epoch_id.clone(),
                        proposer: self.node_id,
                    };
                    for member in members.iter().filter(|m| **m != self.node_id) {
                        let _ = self
                            .peers
                            .request_to_node_timeout(*member, &abort, PROPOSE_REQUEST_TIMEOUT)
                            .await;
                    }
                    return Ok(false);
                }
            }
        }
        let resolved: Vec<_> = acks
            .iter()
            .map(|(node, claim, known)| {
                (
                    *node,
                    claim.map(|c| (c.epoch, c.expires_unix_ms, c.may_carry)),
                    *known,
                )
            })
            .collect();
        let (carrier, stale_below) = constellation_authority::resolve_epoch_claims(&resolved);
        let carrier = carrier.map(|c| EpochCarrier {
            node: c.node,
            epoch: c.epoch,
            expires_unix_ms: c.expires_unix_ms,
        });
        {
            let mut m = self.machine.lock().unwrap();
            m.activate(&epoch_id).map_err(|e| anyhow::anyhow!("{e}"))?;
            self.set_carrier(carrier, stale_below);
        }
        self.sync_flags();
        let activation = EpochActivation {
            epoch_id: epoch_id.clone(),
            members: members.clone(),
            base: base_vec,
            carrier,
            stale_below,
        };
        self.peers.announce_epoch_activate(&activation).await;
        tracing::info!(
            epoch_id,
            ?members,
            slack = f,
            ?acks,
            ?carrier,
            stale_below,
            "continuation epoch active"
        );
        Ok(true)
    }

    /// A proposal this node made was declined (or not answered): drop
    /// its own promise, which nobody can have activated — only the
    /// proposer activates, and only once every member acked. Before,
    /// the proposer stayed `Promised` for good (`is_open`: it never
    /// proposed again, and a member of an open epoch runs no S3
    /// acquisition), which the member S3 rule (`handle_propose_checked`)
    /// would make routine. Proposals then back off for
    /// [`PROPOSE_BACKOFF`], so a member that keeps declining is not asked
    /// (and does not probe S3) every round. Members that acked before
    /// the decline keep their `Promised` state, as before this change.
    fn abandon_own_proposal(&self, epoch_id: &str) {
        {
            let mut m = self.machine.lock().unwrap();
            if m.current().is_some_and(|p| p.epoch_id == epoch_id) && !m.is_active() {
                m.close();
            } else {
                return;
            }
        }
        let _ = self.meta.set_epoch_state(epoch_id, "closed");
        let _ = self.meta.promise_join_end();
        *self.propose_after.lock().unwrap() = Some(std::time::Instant::now() + PROPOSE_BACKOFF);
        self.sync_flags();
    }

    /// A proposer abandoned `epoch_id` (`Payload::EpochAbort`): drop this
    /// member's promise for it — if it is still only `Promised`. The
    /// proposer activates only after every member acked, and it sends
    /// the abort only after closing its own promise for good, so no
    /// activation of this id exists or will; an `Active` (or already
    /// replaced) epoch is left alone. Returns whether it dropped one.
    pub fn handle_abort(&self, epoch_id: &str, proposer: u64) -> bool {
        {
            let mut m = self.machine.lock().unwrap();
            let promised = m.current().is_some_and(|p| {
                p.epoch_id == epoch_id && p.state == constellation_net::EpochState::Promised
            });
            if !promised {
                return false;
            }
            m.close();
        }
        let _ = self.meta.set_epoch_state(epoch_id, "closed");
        let _ = self.meta.promise_join_end();
        self.sync_flags();
        tracing::info!(
            epoch_id,
            proposer,
            "epoch proposal aborted by its proposer; promise dropped"
        );
        true
    }

    /// Plan 30 §M10: the epoch's carrier was retired (admin `leave
    /// --node-id`, which fenced its lease): a member that does not own
    /// the hold abandons the epoch — the flush it waits for will never
    /// come, and the fence makes the carried lease unclaimable by anyone
    /// still holding it. The hold's owner (a handoff successor) keeps its
    /// epoch until its own flush, whose re-claim CAS the fence fails.
    /// Returns whether it abandoned.
    pub fn abandon_if_carrier_retired(&self, roster: &[u64]) -> bool {
        if roster.is_empty() || !self.is_open() {
            return false;
        }
        let (carrier, _) = self.carrier();
        let Some(carrier) = carrier else {
            return false;
        };
        if roster.contains(&carrier.node) || carrier.node == self.node_id {
            return false;
        }
        if self.claim.lock().unwrap().epoch_held {
            return false;
        }
        let id = {
            self.machine
                .lock()
                .unwrap()
                .current()
                .map(|p| p.epoch_id.clone())
        };
        if let Some(id) = id {
            self.machine.lock().unwrap().close();
            let _ = self.meta.set_epoch_state(&id, "closed");
        }
        self.sync_flags();
        tracing::warn!(
            carrier = carrier.node,
            "continuation epoch abandoned: its carrier was retired (admin leave); \
             its unflushed journal is lost"
        );
        true
    }

    pub fn close(&self) {
        // Read before the `if let`: a lock guard created in the
        // scrutinee lives through the whole body and would deadlock the
        // second lock below.
        let id = {
            self.machine
                .lock()
                .unwrap()
                .current()
                .map(|p| p.epoch_id.clone())
        };
        if let Some(id) = id {
            self.machine.lock().unwrap().close();
            let _ = self.meta.set_epoch_state(&id, "closed");
        }
        self.flushing.store(true, Ordering::Relaxed);
        self.sync_flags();
        tracing::info!("continuation epoch closed");
    }
}

/// Refresh the epoch coordinator's write-eligible roster from the
/// registry; the roster read on success.
///
/// Fail-closed where it matters: a registry that answers but cannot be
/// fully parsed clears the roster (an empty roster never satisfies a
/// quorum, so no epoch activates on a registry we could not fully read).
/// Plan 30 §M10: an S3 *outage* (the read fails at the transport) keeps
/// the last complete roster instead — clearing it there made every
/// continuation epoch race the next 5 s refresh after the outage began,
/// since the outage is exactly when an epoch forms. The kept roster is
/// as stale as it would be between two refreshes; a node enrolled during
/// the outage is plan 30 §M10's known gap (see PROGRESS.md).
pub async fn refresh_roster(
    epochs: &EpochManager,
    store: Arc<dyn object_store::ObjectStore>,
) -> Option<Vec<u64>> {
    let roster = constellation_store_s3::write_eligible_roster(store).await;
    let unreachable = roster
        .as_ref()
        .is_err_and(|e| !matches!(e, constellation_store_s3::StoreError::Registry(_)));
    apply_roster(epochs, roster, unreachable)
}

/// Install a roster read: a complete one is the roster; an incomplete
/// registry (a record that vanished or does not parse) empties it
/// (continuation epochs stay unavailable: fail closed); an unreachable
/// one (`unreachable`) keeps the last complete roster.
pub fn apply_roster(
    epochs: &EpochManager,
    roster: Result<Vec<u64>, constellation_store_s3::StoreError>,
    unreachable: bool,
) -> Option<Vec<u64>> {
    match roster {
        Err(e) if unreachable => {
            tracing::warn!(
                error = %e,
                "registry unreachable; keeping the last complete write-eligible roster"
            );
            None
        }
        Ok(roster) => {
            epochs.set_roster(roster.clone());
            Some(roster)
        }
        Err(e @ constellation_store_s3::StoreError::Registry(_)) => {
            tracing::error!(
                error = %e,
                "cannot determine the write-eligible roster; continuation epochs stay unavailable"
            );
            epochs.set_roster(Vec::new());
            None
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "registry unreachable; keeping the last complete write-eligible roster"
            );
            None
        }
    }
}

fn encode_carrier(carrier: Option<EpochCarrier>, stale_below: u64) -> String {
    match carrier {
        Some(c) => format!("{}:{}:{}:{stale_below}", c.node, c.epoch, c.expires_unix_ms),
        None => format!("-:-:-:{stale_below}"),
    }
}

fn decode_carrier(v: &str) -> Option<(Option<EpochCarrier>, u64)> {
    let parts: Vec<&str> = v.split(':').collect();
    let [node, epoch, expires, stale] = parts.as_slice() else {
        return None;
    };
    let stale_below = stale.parse().ok()?;
    if *node == "-" {
        return Some((None, stale_below));
    }
    Some((
        Some(EpochCarrier {
            node: node.parse().ok()?,
            epoch: epoch.parse().ok()?,
            expires_unix_ms: expires.parse().ok()?,
        }),
        stale_below,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(node: u64, slack: u32) -> (EpochManager, Arc<Meta>) {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let m = EpochManager::new(node, meta.clone(), constellation_net::Peers::disabled());
        m.set_roster(vec![1, 2, 3]);
        m.set_slack(slack);
        m.set_claim_view(EpochClaimView {
            advertised_slack: (slack > 0).then_some(slack),
            known: 1,
            ..Default::default()
        });
        (m, meta)
    }

    fn accepted(p: &Payload) -> bool {
        matches!(p, Payload::EpochAck { accepted: true, .. })
    }

    /// Plan 30 §M10: a member acks an epoch of `N − f` under its own
    /// slack, only once its own promise has expired, and holds the join
    /// gate (no promise is issued) while the epoch is open.
    #[test]
    fn a_member_joins_a_flexible_quorum_only_past_its_own_promise() {
        // A proposer running a larger slack than this node's f = 0: two
        // of three is not a quorum here.
        let (m, _) = manager(1, 0);
        assert!(!accepted(&m.handle_propose(
            "e".into(),
            vec![1, 2],
            vec![],
            2,
            1
        )));
        // f = 1 but this node's promise is still out.
        let (m, meta) = manager(1, 1);
        assert!(meta.promise_issue(now_ms() + 60_000).unwrap());
        assert!(!accepted(&m.handle_propose(
            "e".into(),
            vec![1, 2],
            vec![],
            2,
            1
        )));
        // Expired: joins, and no promise can be issued while open.
        let (m, meta) = manager(1, 1);
        assert!(meta.promise_issue(now_ms() - 1).unwrap());
        assert!(accepted(&m.handle_propose(
            "e".into(),
            vec![1, 2],
            vec![],
            2,
            1
        )));
        assert!(m.is_open());
        assert!(!meta.promise_issue(now_ms() + 60_000).unwrap(), "gated");
        // Not advertised yet: refused.
        let (m, _) = manager(1, 1);
        m.set_claim_view(EpochClaimView::default());
        assert!(!accepted(&m.handle_propose(
            "e".into(),
            vec![1, 2],
            vec![],
            2,
            1
        )));
    }

    /// Plan 30 §M10: the members of an epoch whose carrier was retired
    /// (admin leave fenced its lease) abandon it — unless they own the
    /// hold, which flushes (its re-claim fails on the fence) — and may
    /// promise again.
    #[test]
    fn members_abandon_an_epoch_whose_carrier_was_retired() {
        let (m, meta) = manager(1, 1);
        assert!(accepted(&m.handle_propose(
            "e".into(),
            vec![1, 2],
            vec![],
            2,
            1
        )));
        m.handle_activate(EpochActivation {
            epoch_id: "e".into(),
            members: vec![1, 2],
            base: vec![],
            carrier: Some(EpochCarrier {
                node: 2,
                epoch: 1,
                expires_unix_ms: 5,
            }),
            stale_below: 1,
        });
        assert!(m.is_active());
        assert_eq!(m.carrier().0.map(|c| c.node), Some(2));
        // The carrier stays in the roster: nothing happens.
        m.set_roster(vec![1, 2, 3]);
        assert!(m.is_open());
        // A hold owner does not abandon.
        m.set_claim_view(EpochClaimView {
            epoch_held: true,
            ..Default::default()
        });
        m.set_roster(vec![1, 3]);
        assert!(m.is_open());
        m.set_claim_view(EpochClaimView::default());
        m.set_roster(vec![1, 3]);
        assert!(!m.is_open(), "abandoned");
        assert!(
            meta.promise_issue(now_ms() + 1_000).unwrap(),
            "promises again"
        );
        // The carrier survives a restart of the coordinator.
        let again = EpochManager::new(1, meta, constellation_net::Peers::disabled());
        assert_eq!(again.carrier().0.map(|c| c.node), Some(2));
    }

    /// A member that reaches S3 declines a proposal and persists no
    /// promise; the same member cut from S3 joins.
    #[test]
    fn a_member_that_reaches_s3_declines_an_epoch() {
        let (mgr, _meta) = manager(2, 0);
        mgr.set_roster(vec![1, 2]);
        let declined = mgr.handle_propose_checked("1-1".into(), vec![1, 2], vec![], 1, 0, true);
        assert!(!accepted(&declined), "{declined:?}");
        assert!(!mgr.is_open(), "no promise persisted");
        let joined = mgr.handle_propose_checked("1-2".into(), vec![1, 2], vec![], 1, 0, false);
        assert!(accepted(&joined), "{joined:?}");
        assert!(mgr.is_open());
    }

    /// A member that acked a proposal the proposer then abandoned drops
    /// its promise on the abort; an active epoch, or another proposal's
    /// promise, is not touched.
    #[test]
    fn an_abort_drops_only_the_named_promised_epoch() {
        let (mgr, _meta) = manager(2, 0);
        mgr.set_roster(vec![1, 2, 3]);
        let ack = mgr.handle_propose_checked("1-7".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(accepted(&ack), "{ack:?}");
        assert!(mgr.is_open());
        assert!(!mgr.handle_abort("1-6", 1), "another id");
        assert!(mgr.is_open());
        assert!(mgr.handle_abort("1-7", 1));
        assert!(!mgr.is_open(), "the promise is dropped");
        // Active: an abort (which no proposer sends after activating) is
        // ignored.
        let ack = mgr.handle_propose_checked("1-8".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(accepted(&ack), "{ack:?}");
        mgr.handle_activate(constellation_net::EpochActivation {
            epoch_id: "1-8".into(),
            members: vec![1, 2, 3],
            base: vec![],
            carrier: None,
            stale_below: 0,
        });
        assert!(!mgr.handle_abort("1-8", 1));
        assert!(mgr.is_active());
    }

    /// `epoch-member-lost`: an active epoch that carries no lease has no
    /// authority anywhere; its writes are refused at once (`EROFS`), as a
    /// frozen epoch's are, instead of waiting out the acquire deadline.
    /// One carrying a lease writes.
    #[test]
    fn an_epoch_carrying_no_lease_refuses_writes() {
        for (id, carrier) in [
            ("1-1", None),
            (
                "1-2",
                Some(EpochCarrier {
                    node: 1,
                    epoch: 1,
                    expires_unix_ms: 1,
                }),
            ),
        ] {
            let (mgr, _meta) = manager(2, 0);
            mgr.set_roster(vec![1, 2]);
            let ack = mgr.handle_propose_checked(id.into(), vec![1, 2], vec![], 1, 0, false);
            assert!(accepted(&ack), "{ack:?}");
            assert!(!mgr.writes_refused.load(Ordering::Relaxed), "promised only");
            mgr.handle_activate(constellation_net::EpochActivation {
                epoch_id: id.into(),
                members: vec![1, 2],
                base: vec![],
                carrier,
                stale_below: 0,
            });
            assert!(mgr.is_active());
            assert_eq!(
                mgr.writes_refused.load(Ordering::Relaxed),
                carrier.is_none(),
                "{carrier:?}"
            );
        }
    }

    /// Chunk epoch-liveness-gap: a member of an active epoch carrying
    /// node 1's lease takes node 1's fresh proposal in place of it (node 1
    /// closed that epoch and was cut from S3 again before its re-claim
    /// landed), and stays open throughout (no promise in between). Another
    /// proposer is refused, and so is node 1 at a member owning the hold.
    #[test]
    fn a_member_takes_the_carriers_fresh_epoch_in_place_of_the_open_one() {
        let carried = |node| {
            Some(EpochCarrier {
                node,
                epoch: 1,
                expires_unix_ms: 5,
            })
        };
        let activated = |carrier| {
            let (mgr, meta) = manager(2, 0);
            let ack = mgr.handle_propose_checked("1-1".into(), vec![1, 2, 3], vec![], 1, 0, false);
            assert!(accepted(&ack), "{ack:?}");
            mgr.handle_activate(EpochActivation {
                epoch_id: "1-1".into(),
                members: vec![1, 2, 3],
                base: vec![],
                carrier,
                stale_below: 1,
            });
            assert!(mgr.is_active());
            (mgr, meta)
        };
        let (mgr, meta) = activated(carried(1));
        let ack = mgr.handle_propose_checked("3-2".into(), vec![1, 2, 3], vec![], 3, 0, false);
        assert!(!accepted(&ack), "another proposer: {ack:?}");
        assert!(mgr.is_active());
        let ack = mgr.handle_propose_checked("1-2".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(accepted(&ack), "{ack:?}");
        assert!(mgr.is_open() && !mgr.is_active(), "promised to the new one");
        assert_eq!(mgr.status().epoch_id.as_deref(), Some("1-2"));
        assert!(
            !meta.promise_issue(now_ms() + 60_000).unwrap(),
            "the join gate stayed held"
        );
        mgr.handle_activate(EpochActivation {
            epoch_id: "1-2".into(),
            members: vec![1, 2, 3],
            base: vec![],
            carrier: carried(1),
            stale_below: 1,
        });
        assert!(mgr.is_active());
        // The hold's owner (handed the hold by node 1 in the epoch) never
        // takes it: node 1 cannot have closed while it holds, and a second
        // hold would stand beside its own.
        let (mgr, _meta) = activated(carried(1));
        mgr.set_claim_view(EpochClaimView {
            epoch_held: true,
            known: 1,
            ..Default::default()
        });
        let ack = mgr.handle_propose_checked("1-2".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(!accepted(&ack), "the hold owner: {ack:?}");
        assert_eq!(mgr.status().epoch_id.as_deref(), Some("1-1"));
        // An epoch carrying no lease (or another node's) is not node 1's.
        for carrier in [None, carried(3)] {
            let (mgr, _meta) = activated(carrier);
            let ack = mgr.handle_propose_checked("1-2".into(), vec![1, 2, 3], vec![], 1, 0, false);
            assert!(!accepted(&ack), "carrier {carrier:?}: {ack:?}");
        }
    }

    /// Review of chunk epoch-liveness-gap: the carrier aborts its fresh
    /// proposal (another member refused it) after this member took it in
    /// place of the open epoch. The member closes without the carried
    /// lease having moved, and is then any closed node: the join gate is
    /// released (a promise may be issued) and another proposer's epoch is
    /// taken. Safe: the carrier closed the old epoch, so no hold of it
    /// remains, and its closed claim ends at the closed lease's expiry.
    #[test]
    fn an_abort_after_a_replacement_closes_the_member() {
        let carried = Some(EpochCarrier {
            node: 1,
            epoch: 1,
            expires_unix_ms: 5,
        });
        let (mgr, meta) = manager(2, 0);
        let ack = mgr.handle_propose_checked("1-1".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(accepted(&ack), "{ack:?}");
        mgr.handle_activate(EpochActivation {
            epoch_id: "1-1".into(),
            members: vec![1, 2, 3],
            base: vec![],
            carrier: carried,
            stale_below: 1,
        });
        let ack = mgr.handle_propose_checked("1-2".into(), vec![1, 2, 3], vec![], 1, 0, false);
        assert!(accepted(&ack), "replaced: {ack:?}");
        assert!(!mgr.handle_abort("1-1", 1), "the replaced epoch's id");
        assert!(mgr.is_open());
        assert!(mgr.handle_abort("1-2", 1));
        assert!(!mgr.is_open() && !mgr.is_active() && !mgr.is_frozen());
        assert_eq!(mgr.status().epoch_id, None);
        // Neither epoch is open on disk: a restart stays closed (the
        // replaced epoch's row was closed at the replacement).
        assert!(meta.load_open_epoch().unwrap().is_none());
        assert!(
            !EpochManager::new(2, meta.clone(), constellation_net::Peers::disabled()).is_open()
        );
        // The last activated epoch's carrier is kept (as at any close).
        assert_eq!(mgr.carrier(), (carried, 1));
        assert!(!mgr.writes_refused.load(Ordering::Relaxed));
        assert!(mgr.members_open.lock().unwrap().is_empty());
        // The join gate is released: a promise is issued (one already
        // expired, so the next join is not held back by it).
        assert!(meta.promise_issue(now_ms() - 1).unwrap());
        // Closed: another proposer's epoch is taken like any node's.
        let ack = mgr.handle_propose_checked("3-3".into(), vec![1, 2, 3], vec![], 3, 0, false);
        assert!(accepted(&ack), "{ack:?}");
        assert_eq!(mgr.status().epoch_id.as_deref(), Some("3-3"));
    }

    /// Review of chunk epoch-liveness-gap: an activation and a proposal
    /// handled at once. The member closed node 3's epoch and promised node
    /// 1's, whose activation carries node 1's lease; node 3 proposes
    /// meanwhile. Node 3 is not the carrier of any epoch the member is
    /// active in, so it is refused whichever runs first: the carrier is
    /// set under the lock that activates, never read stale beside it.
    #[test]
    fn a_proposal_racing_an_activation_never_sees_the_previous_carrier() {
        let carried = |node| {
            Some(EpochCarrier {
                node,
                epoch: 1,
                expires_unix_ms: 5,
            })
        };
        for round in 0..200 {
            let (mgr, _meta) = manager(2, 0);
            let ack = mgr.handle_propose_checked("3-1".into(), vec![1, 2, 3], vec![], 3, 0, false);
            assert!(accepted(&ack), "{ack:?}");
            mgr.handle_activate(EpochActivation {
                epoch_id: "3-1".into(),
                members: vec![1, 2, 3],
                base: vec![],
                carrier: carried(3),
                stale_below: 1,
            });
            mgr.close();
            let ack = mgr.handle_propose_checked("1-5".into(), vec![1, 2, 3], vec![], 1, 0, false);
            assert!(accepted(&ack), "{ack:?}");
            let gate = std::sync::Barrier::new(2);
            let ack = std::thread::scope(|s| {
                s.spawn(|| {
                    gate.wait();
                    mgr.handle_activate(EpochActivation {
                        epoch_id: "1-5".into(),
                        members: vec![1, 2, 3],
                        base: vec![],
                        carrier: carried(1),
                        stale_below: 1,
                    });
                });
                let proposal = s.spawn(|| {
                    gate.wait();
                    // A larger id than the promised one: only the carrier
                    // rule could take it.
                    mgr.handle_propose_checked("3-9".into(), vec![1, 2, 3], vec![], 3, 0, false)
                });
                proposal.join().unwrap()
            });
            assert!(!accepted(&ack), "round {round}: {ack:?}");
            assert!(mgr.is_active());
            assert_eq!(mgr.status().epoch_id.as_deref(), Some("1-5"));
            assert_eq!(mgr.carrier().0.map(|c| c.node), Some(1));
        }
    }

    #[test]
    fn carrier_roundtrips_through_the_kv_encoding() {
        let c = EpochCarrier {
            node: 3,
            epoch: 7,
            expires_unix_ms: -5,
        };
        assert_eq!(
            decode_carrier(&encode_carrier(Some(c), 9)),
            Some((Some(c), 9))
        );
        assert_eq!(decode_carrier(&encode_carrier(None, 2)), Some((None, 2)));
        assert_eq!(decode_carrier("garbage"), None);
    }
}
