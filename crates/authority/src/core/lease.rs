//! The lease view and keeper state, as pure data with pure decisions
//! (`cli::lease::LeaseView` + `LeaseKeeper` minus their S3 calls, which
//! the job machine in `jobs.rs` issues as actions).
//!
//! Time is an input everywhere: expiry, the dwell and grace clocks and the
//! handoff pause compare against the `now` the core was handed, never a
//! wall clock. `Lease` objects are built here with that same `now`, so the
//! simulation's paused clock governs expiry end to end.

use super::Config;
use crate::ids::{Epoch, HonourMs, NodeId, Now, WaitMs};
use constellation_store_s3::lease::LEASE_VERSION;
use constellation_store_s3::{AckPolicy, Lease, LeaseTag};

/// Plan 30 §M3b: a won lease whose takeover gate has not completed. While
/// pending the view is closed to every new mutation and nothing ships.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingGate {
    pub epoch: Epoch,
    /// The lease came from another node (or is re-adopted after a restart,
    /// M4): strand what the epoch supersedes before replaying.
    pub takeover: bool,
    /// The epoch marker is durable (or none is needed).
    pub marker_shipped: bool,
    /// M13: every older epoch's inbox batch has been executed and deleted
    /// (or the inbox is off).
    pub drained: bool,
    /// Plan 30 §M9: this tenure took its lease over by permit (a sealed
    /// backup's or an `ack=s3` fast takeover), whatever this node's clock
    /// says of the old expiry (plan 43 C7-H2: that is a stamp of another
    /// node's clock and is not kept).
    pub fast_prev: bool,
    /// Plan 30 §M9: this node backed the predecessor at this epoch and
    /// must apply what is left of its backup tail before its view opens
    /// (`None` once applied, or when it backed nobody).
    pub backup_tail_epoch: Option<Epoch>,
    /// Plan 30 §M9: the gate's journal work is done — the marker landed,
    /// the backup tail and the stranded replays are journaled — and only
    /// the read-delegation quarantine or the kernel drain keeps the view
    /// closed. What is journaled ships meanwhile (`ship_epoch`): the
    /// re-shipped tail is what makes the predecessor's acknowledgements
    /// durable again, and a successor that crashed during a multi-second
    /// horizon wait lost it (backup-strict seed 1328).
    pub shippable: bool,
}

/// What the lease object allows this node to do (`LeaseKeeper::classify`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    /// Already ours and still valid.
    Held,
    /// No lease object: create-if-absent.
    Create,
    /// Claimable. `takeover`: the CAS must be preceded by a tail to head
    /// and followed by the gate (a genuine change of hands, or M4's
    /// re-adoption of our own live lease after a restart). `marker`: the
    /// previous holder did not release, so the new tenure opens with an
    /// epoch-marker segment.
    Claim {
        prev: Lease,
        tag: LeaseTag,
        takeover: bool,
        marker: bool,
    },
    /// A live foreign holder.
    Busy {
        holder: NodeId,
        epoch: Epoch,
        prev: Lease,
        tag: LeaseTag,
    },
    /// Deposed, or takeover impossible in single-writer mode: nothing to
    /// do but wait.
    Refused(&'static str),
}

#[derive(Debug, Clone, Default)]
pub struct LeaseState {
    /// The lease we believe we hold, with the tag to swap it.
    pub held: Option<(Lease, LeaseTag)>,
    pub held_since: Option<WaitMs>,
    pub wanted: Vec<NodeId>,
    pub wanted_since: Option<WaitMs>,
    /// Deposed: never ship under the old epoch again until recovered.
    pub lost: bool,
    /// After a deposition, the epoch below which own unshipped work is
    /// stranded (`u64::MAX` when unknown: a persisted deposition).
    pub lost_floor: Epoch,
    /// `Core::begin_handoff_pause`: new local mutations closed until here (wait clock).
    pub pause_until: WaitMs,
    /// A release/handoff is in its final flush + CAS section.
    pub releasing: bool,
    pub gate: Option<PendingGate>,
    pub last_write: WaitMs,
    /// `ForwardState::cached_holder` (from `NotHolder` redirects and
    /// lease reads).
    pub cached_holder: Option<NodeId>,
    /// The last lease object read from S3, for `AcquireProgress`-style
    /// progress detection and the cached holder, and when it was read.
    pub last_seen: Option<Lease>,
    /// Continuation-epoch local authority (DESIGN.md §5.3): no S3 lease
    /// object, writes journal locally, nothing ships until the epoch
    /// closes.
    epoch_hold: Option<Epoch>,
    /// Plan 30 §M9: this node may take over the live lease `(epoch,
    /// holder)` before it expires — it sealed that epoch as a listed
    /// backup, or the tenure's `ack_policy` is `S3` and the holder has
    /// gone silent. Checked against the lease object at classification
    /// (the object is the truth; this is only the permit to try).
    pub takeover_permit: Option<(Epoch, NodeId)>,
    /// Plan 30 §M9: a takeover CAS whose outcome is unknown (the request
    /// failed: it may have applied — S3's "applied, then timed out"):
    /// the epoch it claimed and the lease it replaced. A later
    /// acquisition that finds this node's own lease at that epoch is the
    /// same takeover landing, and the replaced lease stays its
    /// predecessor (backup-crash-slow seed 603631: re-adopted as a plain
    /// own-lease claim, the tenure skipped the sealed backup's tail).
    pub ambiguous_claim: Option<(Epoch, Lease)>,
    /// Plan 31 C8: a forward-only (or suspended) node gives the lease back
    /// once idle, whether or not anyone asked for it (`wants_handoff`).
    pub release_when_idle: bool,
    /// Plan 43 C7-F F-a0 L3: a reconfiguration CAS that would have made the
    /// tenure sealable without marking it failed with its outcome unknown
    /// (an answer lost: it may have landed) while this version was held: S3
    /// may hold the sealable one, so the tenure counts as unmarked
    /// (`Core::reconfig_in_doubt`) until a version is next adopted from S3
    /// (`renewed`: a renewal, a re-read, a mark CAS; `adopt`).
    pub reconfig_doubt: bool,
}

impl LeaseState {
    pub fn epoch(&self) -> Option<Epoch> {
        self.epoch_hold
            .or_else(|| self.held.as_ref().map(|(l, _)| l.epoch))
    }

    /// The continuation-epoch hold's epoch, if one is in force.
    pub fn epoch_hold(&self) -> Option<Epoch> {
        self.epoch_hold
    }

    /// Continuation-epoch local authority is in force.
    pub fn epoch_held(&self) -> bool {
        self.epoch_hold.is_some()
    }

    /// `LeaseKeeper::adopt_epoch_hold`: epoch-local authority without an
    /// S3 CAS.
    pub fn adopt_epoch_hold(&mut self, now: Now, epoch: Epoch) {
        self.lost = false;
        self.lost_floor = 0;
        self.epoch_hold = Some(epoch.max(1));
        self.cached_holder = None;
        self.last_write = now.wait;
    }

    /// `LeaseKeeper::release_local`: the epoch closed; whatever S3 lease
    /// this node had is forgotten locally (re-adopted through the gate
    /// by the next acquisition, M4).
    pub fn release_local(&mut self) {
        self.epoch_hold = None;
        self.held = None;
        self.held_since = None;
        self.gate = None;
        self.releasing = false;
    }

    /// Plan 43 C7-H1 (R6): the end of this node's own lease on its
    /// **honour** clock, from the body it holds (`Lease::holder_expires_ms`).
    /// `Some` only for a body this process wrote (`holder == node_id`,
    /// `holder_clock == cfg.clock_id`, an end set): the anchor travels in the
    /// body, so every adoption of one needs no bookkeeping. A body of an
    /// earlier incarnation of this node, or another node's, carries another
    /// clock's reading and means nothing here: `None` is "not usable" and
    /// "renew now" until this node's next own CAS writes fresh fields (a
    /// restarted holder re-adopts through a claim CAS, which does). Every
    /// decision on the lease's own window (`usable`, `holds_unexpired`,
    /// `renew_due`, the grant and lock caps) reads this, never
    /// `expires_unix_ms`: a backward wall step must not extend what the
    /// holder honours, and a forward one ends it with the honour clock.
    pub fn honour_until(&self, cfg: &Config) -> Option<HonourMs> {
        let (lease, _) = self.held.as_ref()?;
        honour_end_of(lease, cfg)
    }

    /// Plan 43 C7-H1 (R7): the end of this node's own lease on its **wait**
    /// clock, from the body it holds (`Lease::holder_wait_ms`; the same
    /// conditions as [`Self::honour_until`]): what its heartbeats name, so
    /// that a successor that seals it places the lease's end by a
    /// difference on a clock no wall step moves. Never used to judge what
    /// this holder honours (that is the honour end).
    pub fn wait_until(&self, cfg: &Config) -> Option<WaitMs> {
        let (lease, _) = self.held.as_ref()?;
        wait_end_of(lease, cfg)
    }

    /// What is left of the held lease on the honour clock (`None`: no
    /// honour end, see [`Self::honour_until`]; negative: lapsed).
    pub fn honour_left_ms(&self, now: Now, cfg: &Config) -> Option<i64> {
        self.honour_until(cfg).map(|h| h.since(now.honour))
    }

    /// `LeaseView::usable`: held, not lost, and enough margin left.
    pub fn usable(&self, now: Now, cfg: &Config) -> bool {
        if self.lost {
            return false;
        }
        if self.epoch_hold.is_some() {
            return true;
        }
        self.honour_left_ms(now, cfg)
            .is_some_and(|left| left > cfg.expiry_margin_ms as i64)
    }

    /// EC2 follow-up 3a: this node holds the lease and it has not
    /// expired — though it may be inside its expiry margin (a renewal in
    /// flight on a slow S3), where nothing new is admitted. Its backups
    /// keep hearing from it meanwhile.
    pub fn holds_unexpired(&self, now: Now, cfg: &Config) -> bool {
        !self.lost
            && self.epoch_hold.is_none()
            && self.honour_left_ms(now, cfg).is_some_and(|left| left > 0)
    }

    /// `LeaseView::fenced`: a release in its final section, or the
    /// takeover gate pending. A peer's forwarded op is answered `Busy`.
    pub fn fenced(&self) -> bool {
        self.releasing || self.gate.is_some()
    }

    /// `LeaseView::open_for_new_mutation`.
    pub fn open_for_new_mutation(&self, now: Now, cfg: &Config) -> bool {
        if self.fenced() {
            return false;
        }
        if self.epoch_hold.is_some() {
            return self.usable(now, cfg);
        }
        now.wait >= self.pause_until && self.usable(now, cfg)
    }

    /// `LeaseView::new_mutation_epoch`: the epoch a new local mutation
    /// admitted right now executes under.
    pub fn new_mutation_epoch(&self, now: Now, cfg: &Config) -> Option<Epoch> {
        self.open_for_new_mutation(now, cfg)
            .then(|| self.epoch())
            .flatten()
    }

    /// `LeaseKeeper::ship_epoch`: the epoch to stamp into a segment, or
    /// none when this node may not ship (no lease, expired, deposed, gate
    /// pending). Deliberately blind to the handoff pause and the releasing
    /// flag: the drain those exist for must still ship.
    pub fn ship_epoch(&self, now: Now, cfg: &Config) -> Option<Epoch> {
        if self.lost || self.gate.is_some() {
            return None;
        }
        self.usable(now, cfg).then(|| self.epoch()).flatten()
    }

    /// Plan 30 §M9: `ship_epoch` for the shipper alone — also while a
    /// gate whose journal work is done waits out the read-delegation
    /// quarantine (`PendingGate::shippable`). Nothing else (forwards,
    /// handoffs, the inbox, backups, streams) opens before the gate.
    pub fn journal_ship_epoch(&self, now: Now, cfg: &Config) -> Option<Epoch> {
        if self.lost || self.gate.as_ref().is_some_and(|g| !g.shippable) {
            return None;
        }
        self.usable(now, cfg).then(|| self.epoch()).flatten()
    }

    pub fn is_paused_for_handoff(&self, now: Now) -> bool {
        self.pause_until > now.wait
    }

    pub fn touch(&mut self, now: Now) {
        self.last_write = now.wait;
    }

    /// `LeaseKeeper::wants_handoff`: dwell and wanted timers alone justify
    /// handing the lease back.
    ///
    /// Plan 31 C8: a forward-only node needs nobody to ask — it hands the
    /// lease back once it has held it for the dwell and written nothing
    /// for as long (`release_when_idle`).
    pub fn wants_handoff(&self, now: Now, cfg: &Config) -> bool {
        let dwelt = self
            .held_since
            .is_some_and(|since| now.wait.since(since) >= cfg.dwell_ms as i64);
        if self.epoch_hold.is_some() || self.held.is_none() || !dwelt {
            return false;
        }
        if self.release_when_idle && now.wait.since(self.last_write) >= cfg.dwell_ms as i64 {
            return true;
        }
        !self.wanted.is_empty()
            && (now.wait.since(self.last_write) >= cfg.idle_release_ms as i64
                || self
                    .wanted_since
                    .is_some_and(|since| now.wait.since(since) >= cfg.wanted_grace_ms as i64))
    }

    /// `LeaseKeeper::idle_release_due`.
    pub fn idle_release_due(&self, now: Now, cfg: &Config, backlog: u64, held_back: u64) -> bool {
        backlog == 0
            && !(cfg.keep_lease_while_held && held_back > 0)
            && self.wants_handoff(now, cfg)
    }

    /// Plan 30 §M9: whether the permit lets this node claim `prev` now.
    /// A sealed backup may claim the epoch it sealed while it is still
    /// listed; under `ack_policy = S3` any peer may claim once the holder
    /// has not renewed for `backup_takeover_ms` (a renewal within that
    /// window is proof of S3 liveness, and a merely P2P-partitioned
    /// holder keeps its lease).
    fn permit_allows(&self, now: Now, cfg: &Config, prev: &Lease, sealed_epoch: Epoch) -> bool {
        let Some((epoch, holder)) = self.takeover_permit else {
            return false;
        };
        if prev.epoch != epoch || prev.holder != holder || holder == cfg.node_id {
            return false;
        }
        match prev.ack_policy {
            AckPolicy::Backup => prev.backups.contains(&cfg.node_id) && sealed_epoch >= prev.epoch,
            AckPolicy::S3 => {
                let renewed_at = prev.expires_unix_ms - cfg.ttl_ms as i64;
                now.wall.0 - renewed_at >= cfg.backup_takeover_ms as i64
            }
            AckPolicy::Local => false,
        }
    }

    /// `LeaseKeeper::classify` over an already-read lease object.
    /// `sealed_epoch`: the highest epoch this node sealed as a backup
    /// (plan 30 §M9), 0 when none.
    pub fn classify(
        &self,
        now: Now,
        cfg: &Config,
        object: Option<(Lease, LeaseTag)>,
        sealed_epoch: Epoch,
    ) -> Plan {
        if self.lost {
            return Plan::Refused("deposed; refusing to reacquire before recovery");
        }
        let Some((prev, tag)) = object else {
            return Plan::Create;
        };
        if prev.retired.contains(&cfg.node_id) {
            // Plan 30 §M10: an admin `leave --node-id` fenced this lease
            // against this node; it never holds it again.
            return Plan::Refused("retired by an admin leave (the lease is fenced against us)");
        }
        if prev.holder == cfg.node_id && !prev.released {
            if self.held.is_some() && !prev.is_expired(now.wall.0) {
                return Plan::Held;
            }
            if self.held.is_none() {
                // Our own lease that we do not track: a restart, or a CAS
                // whose reply was lost. M4: gated like a takeover (the
                // tail to head, the epoch marker, the strand and replay),
                // **whatever the wall clock says of its expiry** (plan 43
                // C7a: plan 30's rule had no expiry condition; the port
                // narrowed it to a lease the wall read as live, and a
                // forward step of the wall, or a restart after the expiry,
                // re-claimed without the gate).
                return Plan::Claim {
                    prev,
                    tag,
                    takeover: true,
                    marker: true,
                };
            }
        }
        let permitted = self.permit_allows(now, cfg, &prev, sealed_epoch);
        if !prev.is_claimable(now.wall.0) && !permitted {
            return Plan::Busy {
                holder: prev.holder,
                epoch: prev.epoch,
                prev,
                tag,
            };
        }
        // Plan 30 §M9: the listed backups of a `Backup` lease hold the
        // acknowledged rows its holder has not shipped; only a backup's
        // claim re-ships them (its sealed tail). Anyone else waits
        // `backup_claim_grace_ms` past the expiry, which is time for a
        // backup to hear the lapsed holder fall silent, seal and claim
        // (long-backup seed 802943: the holder, cut from S3, could not
        // renew; a non-backup claimed at the expiry while the backup
        // still held an acknowledged create, which came back later only
        // as the deposed holder's replay — after ops invoked once it had
        // been acknowledged). If no backup claims, the lease is anyone's
        // after the grace (a double fault: the tail is lost).
        if prev.ack_policy == AckPolicy::Backup
            && !prev.released
            && prev.holder != 0
            && prev.holder != cfg.node_id
            && !prev.backups.is_empty()
            && !prev.backups.contains(&cfg.node_id)
            && !permitted
            && !prev.is_expired(now.wall.0 - backup_claim_grace_ms(cfg))
        {
            return Plan::Busy {
                holder: prev.holder,
                epoch: prev.epoch,
                prev,
                tag,
            };
        }
        let takeover = prev.holder != 0 && prev.holder != cfg.node_id;
        if takeover && cfg.single_writer {
            return Plan::Refused("no If-Match support: takeover cannot be made safe");
        }
        let marker = takeover && !prev.released;
        Plan::Claim {
            prev,
            tag,
            takeover,
            marker,
        }
    }

    /// The lease object a `Plan::Create`/`Plan::Claim` CAS writes
    /// (`LeaseKeeper::commit_cas`'s epoch rule: same holder re-adopting
    /// keeps the epoch, a real handover bumps it). Plan 30 §M9: a new
    /// tenure starts with no backups (`Local`, or `S3` when this mount
    /// asks for `ack=s3`) and a `config_version` above the predecessor's.
    /// Re-adopting its own lease, the holder keeps the granting mark
    /// (`ensure_granting_marked`): the same tenure goes on, its grants
    /// with it, and its successor still waits their horizon out. Dropped,
    /// a re-claim that S3 cut again could not mark the lease, and every
    /// lock request was answered `Busy` (then `ENOLCK`) until S3 returned.
    ///
    /// Plan 43 C7-F F-a0: `inherits_duty` marks the lease from its first
    /// CAS too. A tenure that waits out its predecessor's grants (a
    /// takeover inside a floor, or a lock grace after a released lease) is
    /// one its own successor must wait out: it may be sealed before it
    /// grants anything itself, and a lease still unmarked then would arm no
    /// floor at that successor (`note_fast_takeover`), leaving the grants
    /// this tenure's floor protected open to conflicting ones.
    pub fn granted_lease(
        &self,
        now: Now,
        cfg: &Config,
        prev: Option<&Lease>,
        inherits_duty: bool,
    ) -> Lease {
        let own = prev.filter(|p| p.holder == cfg.node_id && !p.released);
        let epoch = match prev {
            None => 1,
            Some(prev) if own.is_some() => prev.epoch.max(1),
            Some(prev) => prev.epoch + 1,
        };
        Lease {
            v: LEASE_VERSION,
            partition: cfg.partition.clone(),
            holder: cfg.node_id,
            epoch,
            // Plan 43 C7-H1 (R5): the wall end for everyone else, the
            // honour end for this node alone, both from the send's reading.
            expires_unix_ms: now.wall.plus(cfg.ttl_ms).0,
            holder_expires_ms: now.honour.plus(cfg.ttl_ms).0,
            holder_clock: cfg.clock_id,
            holder_wait_ms: now.wait.plus(cfg.ttl_ms).0,
            released: false,
            wanted_by: Vec::new(),
            backups: Vec::new(),
            config_version: prev.map(|p| p.config_version + 1).unwrap_or(1),
            ack_policy: if cfg.ack_s3 {
                AckPolicy::S3
            } else {
                AckPolicy::Local
            },
            granted_delegations: cfg.strict_mounts
                || inherits_duty
                || own.is_some_and(|p| p.granted_delegations),
            retired: prev.map(|p| p.retired.clone()).unwrap_or_default(),
        }
    }

    /// Plan 30 §M9: the acknowledgement policy of the lease this node
    /// holds (`Local` when it holds none, or holds through a continuation
    /// epoch: nothing ships, nothing is backed).
    pub fn ack_policy(&self) -> AckPolicy {
        match &self.held {
            Some((lease, _)) if !self.lost && self.epoch_hold.is_none() => lease.ack_policy,
            _ => AckPolicy::Local,
        }
    }

    /// Plan 30 §M9: the committed backup set (empty unless holding under
    /// `Backup`).
    pub fn backups(&self) -> &[NodeId] {
        match &self.held {
            // Plan 30 §M10: a continuation epoch's hold backs nothing (it
            // acknowledges locally; see `ack_policy`).
            Some((lease, _))
                if !self.lost
                    && self.epoch_hold.is_none()
                    && lease.ack_policy == AckPolicy::Backup =>
            {
                &lease.backups
            }
            _ => &[],
        }
    }

    pub fn renewed_lease(&self, now: Now, cfg: &Config, mine: &Lease) -> Lease {
        Lease {
            expires_unix_ms: now.wall.plus(cfg.ttl_ms).0,
            holder_expires_ms: now.honour.plus(cfg.ttl_ms).0,
            holder_clock: cfg.clock_id,
            holder_wait_ms: now.wait.plus(cfg.ttl_ms).0,
            released: false,
            ..mine.clone()
        }
    }

    /// A requester's inbox batch said it wants the lease (M13, M5): the
    /// same as finding it in `wanted_by` at a renewal, learned at the
    /// next poll instead of the next half-TTL.
    pub fn note_wanted(&mut self, now: Now, node: NodeId) {
        if node == 0 || self.held.is_none() {
            return;
        }
        if !self.wanted.contains(&node) {
            self.wanted.push(node);
            self.wanted.sort_unstable();
        }
        if self.wanted_since.is_none() {
            self.wanted_since = Some(now.wait);
        }
    }

    /// `LeaseKeeper::open_won`: take ownership of a won lease.
    pub fn adopt(&mut self, now: Now, lease: Lease, tag: LeaseTag, gate: Option<PendingGate>) {
        self.gate = gate;
        self.wanted.clear();
        self.wanted_since = None;
        self.held_since = Some(now.wait);
        self.last_write = now.wait;
        self.last_seen = Some(lease.clone());
        self.cached_holder = Some(lease.holder);
        self.held = Some((lease, tag));
        self.reconfig_doubt = false;
    }

    /// `LeaseKeeper::apply_renew`'s `Renewed` arm.
    pub fn renewed(&mut self, now: Now, lease: Lease, tag: LeaseTag) {
        // Union with what inbox batches already told us (M5): a renewal
        // object that predates a batch's flag must not forget it.
        let mut wanted = lease.wanted_by.clone();
        for w in &self.wanted {
            if !wanted.contains(w) {
                wanted.push(*w);
            }
        }
        wanted.sort_unstable();
        self.wanted = wanted;
        if self.wanted.is_empty() {
            self.wanted_since = None;
        } else if self.wanted_since.is_none() {
            self.wanted_since = Some(now.wait);
        }
        self.last_seen = Some(lease.clone());
        self.held = Some((lease, tag));
        self.reconfig_doubt = false;
    }

    /// `LeaseKeeper::mark_lost`.
    pub fn mark_lost(&mut self, holder: NodeId, epoch: Epoch, my_epoch: Epoch) {
        self.held = None;
        self.epoch_hold = None;
        self.gate = None;
        self.releasing = false;
        self.lost = true;
        self.cached_holder = (holder != 0).then_some(holder);
        self.lost_floor = epoch.max(my_epoch.saturating_add(1));
    }

    pub fn force_lost(&mut self) {
        self.held = None;
        self.epoch_hold = None;
        self.gate = None;
        self.releasing = false;
        self.lost = true;
        self.lost_floor = u64::MAX;
    }

    pub fn clear_lost(&mut self) {
        self.lost = false;
        self.lost_floor = 0;
    }

    /// `LeaseKeeper::release`'s success arm.
    pub fn released(&mut self) {
        self.wanted.clear();
        self.wanted_since = None;
        self.held_since = None;
        self.held = None;
        self.gate = None;
        self.releasing = false;
        self.epoch_hold = None;
    }

    /// Remember what a lease read said about who holds it.
    pub fn note_object(&mut self, lease: &Lease) {
        if lease.holder != 0 {
            self.cached_holder = Some(lease.holder);
        }
        self.last_seen = Some(lease.clone());
    }
}

/// Plan 43 C7-H1 (R6): `lease`'s end on this node's honour clock when this
/// process wrote it (see [`LeaseState::honour_until`]); `None` for any other
/// body (another holder, another incarnation, a fenced or foreign one).
pub(crate) fn honour_end_of(lease: &Lease, cfg: &Config) -> Option<HonourMs> {
    (lease.holder == cfg.node_id
        && lease.holder_clock == cfg.clock_id
        && lease.holder_expires_ms != 0)
        .then_some(HonourMs(lease.holder_expires_ms))
}

/// Plan 43 C7-H1 (R7): `lease`'s end on this node's wait clock when this
/// process wrote it (see [`LeaseState::wait_until`]).
pub(crate) fn wait_end_of(lease: &Lease, cfg: &Config) -> Option<WaitMs> {
    (lease.holder == cfg.node_id && lease.holder_clock == cfg.clock_id && lease.holder_wait_ms != 0)
        .then_some(WaitMs(lease.holder_wait_ms))
}

/// Plan 30 §M9: how long past a `Backup` lease's expiry a node that is not
/// one of its listed backups waits before claiming it (see
/// `LeaseState::classify`): twice the backups' silence window.
pub fn backup_claim_grace_ms(cfg: &Config) -> i64 {
    2 * cfg.backup_takeover_ms as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::Ms;

    fn cfg() -> Config {
        let mut c = Config::defaults(7, 1);
        c.ttl_ms = 10_000;
        c
    }

    fn tag() -> LeaseTag {
        // Built through the store so the private field stays private: a
        // create on an in-memory store yields a tag.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = std::sync::Arc::new(object_store::memory::InMemory::new());
            let leases = constellation_store_s3::LeaseStore::new(
                store,
                "p0",
                constellation_store_s3::LeaseMode::Cas,
            );
            leases
                .try_create(&Lease::granted("p0", 1, 1, 1000))
                .await
                .unwrap()
        })
    }

    #[test]
    fn classify_mirrors_the_keeper() {
        let cfg = cfg();
        let st = LeaseState::default();
        let now = Now::same(1_000_000);
        assert_eq!(st.classify(now, &cfg, None, 0), Plan::Create);
        // A live foreign holder is busy.
        let mut foreign = st.granted_lease(now, &cfg, None, false);
        foreign.holder = 3;
        match st.classify(now, &cfg, Some((foreign.clone(), tag())), 0) {
            Plan::Busy { holder: 3, .. } => {}
            other => panic!("{other:?}"),
        }
        // Expired and unreleased: a takeover with a marker.
        let expired = Lease {
            expires_unix_ms: now.wall.0 - 1,
            ..foreign.clone()
        };
        match st.classify(now, &cfg, Some((expired, tag())), 0) {
            Plan::Claim {
                takeover: true,
                marker: true,
                ..
            } => {}
            other => panic!("{other:?}"),
        }
        // Released: a takeover without a marker.
        let released = foreign.released();
        match st.classify(now, &cfg, Some((released, tag())), 0) {
            Plan::Claim {
                takeover: true,
                marker: false,
                ..
            } => {}
            other => panic!("{other:?}"),
        }
        // Our own live lease we do not track: re-adopted through the gate
        // (M4); tracked: held.
        let mine = st.granted_lease(now, &cfg, None, false);
        match st.classify(now, &cfg, Some((mine.clone(), tag())), 0) {
            Plan::Claim {
                takeover: true,
                marker: true,
                ..
            } => {}
            other => panic!("{other:?}"),
        }
        let mut holding = st.clone();
        holding.adopt(now, mine.clone(), tag(), None);
        assert_eq!(
            holding.classify(now, &cfg, Some((mine, tag())), 0),
            Plan::Held
        );
        // Deposed: refused until recovered.
        let mut lost = st.clone();
        lost.mark_lost(3, 5, 4);
        assert!(matches!(
            lost.classify(now, &cfg, None, 0),
            Plan::Refused(_)
        ));
        assert_eq!(lost.lost_floor, 5);
    }

    /// Plan 43 C7a (M4's own-lease arm): an own unreleased lease this node
    /// does not track is re-adopted through the gate (tail to head, epoch
    /// marker, strand and replay) **whatever its wall clock says of the
    /// lease's expiry**: plan 30's rule had no expiry condition, the sans-IO
    /// port narrowed it to a lease the wall read as live, and a forward step
    /// of the wall (or a restart after the expiry) re-claimed it without the
    /// gate. A tracked lease is `Held` while live, as before; one the wall
    /// reads as expired while tracked takes the generic path, as before.
    /// Fails before: `takeover: false, marker: false` for the expired one.
    #[test]
    fn a_restarted_holder_readopts_its_own_lease_through_the_gate_whatever_its_wall_clock_says() {
        let cfg = cfg();
        let st = LeaseState::default();
        let t0 = Now::same(1_000_000);
        let mine = st.granted_lease(t0, &cfg, None, false);
        for step in [-3_600_000i64, 0, 5_000, cfg.ttl_ms as i64 + 1, 3_600_000] {
            // The wall now: `step` from the grant (past `expires_unix_ms`
            // for the larger ones); the three clocks otherwise agree.
            let now = Now::same(t0.wall.0 + step);
            match st.classify(now, &cfg, Some((mine.clone(), tag())), 0) {
                Plan::Claim {
                    takeover: true,
                    marker: true,
                    ..
                } => {}
                other => panic!("step {step}: {other:?}"),
            }
        }
        // Released: nothing to be gated by (a takeover without a marker for a
        // foreign release; an own released lease is the generic claim).
        let released = mine.clone().released();
        let now = Now::same(t0.wall.0 + 3_600_000);
        assert!(matches!(
            st.classify(now, &cfg, Some((released, tag())), 0),
            Plan::Claim { marker: false, .. }
        ));
        // Tracked: held while live; once the wall reads it as expired the
        // generic path (a plain claim) is what it always was.
        let mut holding = st.clone();
        holding.adopt(t0, mine.clone(), tag(), None);
        assert_eq!(
            holding.classify(t0, &cfg, Some((mine.clone(), tag())), 0),
            Plan::Held
        );
        let late = Now::same(t0.wall.0 + cfg.ttl_ms as i64 + 1);
        assert!(matches!(
            holding.classify(late, &cfg, Some((mine, tag())), 0),
            Plan::Claim {
                takeover: false,
                marker: false,
                ..
            }
        ));
    }

    #[test]
    fn the_view_gates_match_lease_view() {
        let cfg = cfg();
        let now = Now::same(5_000_000);
        let mut st = LeaseState::default();
        assert_eq!(st.new_mutation_epoch(now, &cfg), None);
        let lease = st.granted_lease(now, &cfg, None, false);
        st.adopt(
            now,
            lease,
            tag(),
            Some(PendingGate {
                epoch: 1,
                takeover: true,
                marker_shipped: false,
                drained: true,
                fast_prev: false,
                backup_tail_epoch: None,
                shippable: false,
            }),
        );
        // Gate pending: closed to everything, nothing ships.
        assert!(st.fenced());
        assert_eq!(st.new_mutation_epoch(now, &cfg), None);
        assert_eq!(st.ship_epoch(now, &cfg), None);
        st.gate = None;
        assert_eq!(st.new_mutation_epoch(now, &cfg), Some(1));
        assert_eq!(st.ship_epoch(now, &cfg), Some(1));
        // The handoff pause closes new mutations but not shipping.
        st.pause_until = now.wait.plus(cfg.handoff_pause_ms);
        assert_eq!(st.new_mutation_epoch(now, &cfg), None);
        assert_eq!(st.ship_epoch(now, &cfg), Some(1));
        let later = now.plus(cfg.handoff_pause_ms);
        assert_eq!(st.new_mutation_epoch(later, &cfg), Some(1));
        // Releasing closes new mutations, and shipping still drains.
        st.releasing = true;
        assert_eq!(st.new_mutation_epoch(later, &cfg), None);
        assert_eq!(st.ship_epoch(later, &cfg), Some(1));
        st.releasing = false;
        // Past the margin: unusable.
        let near_expiry = Now::same(st.held.as_ref().unwrap().0.expires_unix_ms - 500);
        assert_eq!(st.ship_epoch(near_expiry, &cfg), None);
    }

    #[test]
    fn a_forward_only_holder_releases_once_idle_unasked() {
        let mut cfg = cfg();
        cfg.dwell_ms = 1_000;
        cfg.idle_release_ms = 30_000;
        let t0 = Now::same(1_000_000);
        let mut st = LeaseState {
            release_when_idle: true,
            ..LeaseState::default()
        };
        let lease = st.granted_lease(t0, &cfg, None, false);
        st.adopt(t0, lease, tag(), None);
        assert!(!st.wants_handoff(t0.plus(900), &cfg), "dwell not met");
        st.touch(t0.plus(800));
        assert!(!st.wants_handoff(t0.plus(1_500), &cfg), "written just now");
        assert!(st.wants_handoff(t0.plus(1_900), &cfg), "idle for the dwell");
        assert!(!st.idle_release_due(t0.plus(1_900), &cfg, 1, 0), "backlog");
        assert!(st.idle_release_due(t0.plus(1_900), &cfg, 0, 0));
        st.release_when_idle = false;
        assert!(
            !st.wants_handoff(t0.plus(9_000), &cfg),
            "hold: nobody asked"
        );
    }

    /// Plan 43 C7-H1 (4/4): the idle release counts on the wait clock. A
    /// forward wall step does not make a lease that was just written look
    /// idle (a release the workload then pays for), and a backward one does
    /// not hold an idle lease past its dwell. Fails on the parent: the
    /// stamps were wall.
    #[test]
    fn a_forward_step_does_not_release_an_idle_lease_early() {
        let mut cfg = cfg();
        cfg.dwell_ms = 1_000;
        cfg.idle_release_ms = 30_000;
        let t0 = Now::same(1_000_000);
        let mut st = LeaseState {
            release_when_idle: true,
            ..LeaseState::default()
        };
        let lease = st.granted_lease(t0, &cfg, None, false);
        st.adopt(t0, lease, tag(), None);
        st.touch(t0.plus(800));
        // 700 ms later on every clock but the wall clock, which stepped
        // 60 s forward (and the honour clock with it).
        let stepped = |wall_step: i64, honour_step: i64, ms: u64| Now {
            wall: Ms(t0.wall.0 + wall_step + ms as i64),
            honour: HonourMs(t0.honour.0 + honour_step + ms as i64),
            wait: WaitMs(t0.wait.0 + ms as i64),
            boot: t0.boot,
        };
        assert!(
            !st.wants_handoff(stepped(60_000, 60_000, 1_500), &cfg),
            "written 700 ms ago: a forward step is not idle time"
        );
        // A backward step: the wall clock reads 60 s behind, the idle time
        // is the elapsed time.
        assert!(
            st.wants_handoff(stepped(-60_000, 0, 1_900), &cfg),
            "idle for the dwell, whatever the wall clock says"
        );
    }

    #[test]
    fn handoff_wants_dwell_then_idle_or_grace() {
        let mut cfg = cfg();
        cfg.dwell_ms = 1_000;
        cfg.idle_release_ms = 5_000;
        cfg.wanted_grace_ms = 2_000;
        let t0 = Now::same(1_000_000);
        let mut st = LeaseState::default();
        let lease = st.granted_lease(t0, &cfg, None, false);
        st.adopt(t0, lease.clone(), tag(), None);
        assert!(!st.wants_handoff(t0.plus(9_000), &cfg), "nobody waiting");
        st.renewed(t0.plus(500), lease.wanting(9), tag());
        assert!(!st.wants_handoff(t0.plus(900), &cfg), "dwell not met");
        assert!(
            !st.wants_handoff(t0.plus(1_500), &cfg),
            "neither idle nor grace"
        );
        assert!(st.wants_handoff(t0.plus(2_600), &cfg), "grace elapsed");
        st.touch(t0.plus(2_600));
        assert!(
            st.wants_handoff(t0.plus(2_700), &cfg),
            "grace still elapsed"
        );
        assert!(!st.idle_release_due(t0.plus(2_700), &cfg, 3, 0), "backlog");
        assert!(st.idle_release_due(t0.plus(2_700), &cfg, 0, 0));
        assert!(
            !st.idle_release_due(t0.plus(2_700), &cfg, 0, 1),
            "M4 held records keep it"
        );
    }
}
