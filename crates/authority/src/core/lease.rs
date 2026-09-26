//! The lease view and keeper state, as pure data with pure decisions
//! (`cli::lease::LeaseView` + `LeaseKeeper` minus their S3 calls, which
//! the job machine in `jobs.rs` issues as actions).
//!
//! Time is an input everywhere: expiry, the dwell and grace clocks and the
//! handoff pause compare against the `now` the core was handed, never a
//! wall clock. `Lease` objects are built here with that same `now`, so the
//! simulation's paused clock governs expiry end to end.

use super::Config;
use crate::ids::{Epoch, Ms, NodeId};
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
    /// Plan 30 §M9: the lease this one took over had not expired (a
    /// sealed backup's or an `ack=s3` fast takeover): its expiry and
    /// whether that tenure served strict reads, for the successor's
    /// acknowledgement floor (see `core::backup`).
    pub fast_prev: Option<(i64, bool)>,
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
    pub held_since: Option<Ms>,
    pub wanted: Vec<NodeId>,
    pub wanted_since: Option<Ms>,
    /// Deposed: never ship under the old epoch again until recovered.
    pub lost: bool,
    /// After a deposition, the epoch below which own unshipped work is
    /// stranded (`u64::MAX` when unknown: a persisted deposition).
    pub lost_floor: Epoch,
    /// `begin_handoff_pause`: new local mutations closed until here.
    pub pause_until: Ms,
    /// A release/handoff is in its final flush + CAS section.
    pub releasing: bool,
    pub gate: Option<PendingGate>,
    pub last_write: Ms,
    /// `ForwardState::cached_holder` (from `NotHolder` redirects and
    /// lease reads).
    pub cached_holder: Option<NodeId>,
    /// The last lease object read from S3, for `AcquireProgress`-style
    /// progress detection and the cached holder, and when it was read.
    pub last_seen: Option<Lease>,
    pub last_seen_at: Option<Ms>,
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
    pub fn adopt_epoch_hold(&mut self, now: Ms, epoch: Epoch) {
        self.lost = false;
        self.lost_floor = 0;
        self.epoch_hold = Some(epoch.max(1));
        self.cached_holder = None;
        self.last_write = now;
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

    /// `LeaseView::usable`: held, not lost, and enough margin left.
    pub fn usable(&self, now: Ms, cfg: &Config) -> bool {
        if self.lost {
            return false;
        }
        if self.epoch_hold.is_some() {
            return true;
        }
        match &self.held {
            Some((lease, _)) => {
                lease.expires_in_ms(now.0) > cfg.expiry_margin_ms as i64
                    && lease.holder == cfg.node_id
            }
            None => false,
        }
    }

    /// EC2 follow-up 3a: this node holds the lease and it has not
    /// expired — though it may be inside its expiry margin (a renewal in
    /// flight on a slow S3), where nothing new is admitted. Its backups
    /// keep hearing from it meanwhile.
    pub fn holds_unexpired(&self, now: Ms, cfg: &Config) -> bool {
        !self.lost
            && self.epoch_hold.is_none()
            && self.held.as_ref().is_some_and(|(lease, _)| {
                lease.holder == cfg.node_id && lease.expires_in_ms(now.0) > 0
            })
    }

    /// `LeaseView::fenced`: a release in its final section, or the
    /// takeover gate pending. A peer's forwarded op is answered `Busy`.
    pub fn fenced(&self) -> bool {
        self.releasing || self.gate.is_some()
    }

    /// `LeaseView::open_for_new_mutation`.
    pub fn open_for_new_mutation(&self, now: Ms, cfg: &Config) -> bool {
        if self.fenced() {
            return false;
        }
        if self.epoch_hold.is_some() {
            return self.usable(now, cfg);
        }
        now >= self.pause_until && self.usable(now, cfg)
    }

    /// `LeaseView::new_mutation_epoch`: the epoch a new local mutation
    /// admitted right now executes under.
    pub fn new_mutation_epoch(&self, now: Ms, cfg: &Config) -> Option<Epoch> {
        self.open_for_new_mutation(now, cfg)
            .then(|| self.epoch())
            .flatten()
    }

    /// `LeaseKeeper::ship_epoch`: the epoch to stamp into a segment, or
    /// none when this node may not ship (no lease, expired, deposed, gate
    /// pending). Deliberately blind to the handoff pause and the releasing
    /// flag: the drain those exist for must still ship.
    pub fn ship_epoch(&self, now: Ms, cfg: &Config) -> Option<Epoch> {
        if self.lost || self.gate.is_some() {
            return None;
        }
        self.usable(now, cfg).then(|| self.epoch()).flatten()
    }

    /// Plan 30 §M9: `ship_epoch` for the shipper alone — also while a
    /// gate whose journal work is done waits out the read-delegation
    /// quarantine (`PendingGate::shippable`). Nothing else (forwards,
    /// handoffs, the inbox, backups, streams) opens before the gate.
    pub fn journal_ship_epoch(&self, now: Ms, cfg: &Config) -> Option<Epoch> {
        if self.lost || self.gate.as_ref().is_some_and(|g| !g.shippable) {
            return None;
        }
        self.usable(now, cfg).then(|| self.epoch()).flatten()
    }

    pub fn is_paused_for_handoff(&self, now: Ms) -> bool {
        self.pause_until > now
    }

    pub fn begin_handoff_pause(&mut self, now: Ms, cfg: &Config) {
        self.pause_until = now.plus(cfg.handoff_pause_ms);
    }

    pub fn touch(&mut self, now: Ms) {
        self.last_write = now;
    }

    /// `LeaseKeeper::wants_handoff`: dwell and wanted timers alone justify
    /// handing the lease back.
    pub fn wants_handoff(&self, now: Ms, cfg: &Config) -> bool {
        self.epoch_hold.is_none()
            && self.held.is_some()
            && !self.wanted.is_empty()
            && self
                .held_since
                .is_some_and(|since| now.since(since) >= cfg.dwell_ms as i64)
            && (now.since(self.last_write) >= cfg.idle_release_ms as i64
                || self
                    .wanted_since
                    .is_some_and(|since| now.since(since) >= cfg.wanted_grace_ms as i64))
    }

    /// `LeaseKeeper::idle_release_due`.
    pub fn idle_release_due(&self, now: Ms, cfg: &Config, backlog: u64, held_back: u64) -> bool {
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
    fn permit_allows(&self, now: Ms, cfg: &Config, prev: &Lease, sealed_epoch: Epoch) -> bool {
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
                now.0 - renewed_at >= cfg.backup_takeover_ms as i64
            }
            AckPolicy::Local => false,
        }
    }

    /// `LeaseKeeper::classify` over an already-read lease object.
    /// `sealed_epoch`: the highest epoch this node sealed as a backup
    /// (plan 30 §M9), 0 when none.
    pub fn classify(
        &self,
        now: Ms,
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
        if prev.holder == cfg.node_id && !prev.released && !prev.is_expired(now.0) {
            if self.held.is_some() {
                return Plan::Held;
            }
            // Our own live lease that we do not track: a restart, or a CAS
            // whose reply was lost. M4: gated like a takeover.
            return Plan::Claim {
                prev,
                tag,
                takeover: true,
                marker: true,
            };
        }
        let permitted = self.permit_allows(now, cfg, &prev, sealed_epoch);
        if !prev.is_claimable(now.0) && !permitted {
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
            && !prev.is_expired(now.0 - backup_claim_grace_ms(cfg))
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
    pub fn granted_lease(&self, now: Ms, cfg: &Config, prev: Option<&Lease>) -> Lease {
        let epoch = match prev {
            None => 1,
            Some(prev) if prev.holder == cfg.node_id && !prev.released => prev.epoch.max(1),
            Some(prev) => prev.epoch + 1,
        };
        Lease {
            v: LEASE_VERSION,
            partition: cfg.partition.clone(),
            holder: cfg.node_id,
            epoch,
            expires_unix_ms: now.plus(cfg.ttl_ms).0,
            released: false,
            wanted_by: Vec::new(),
            backups: Vec::new(),
            config_version: prev.map(|p| p.config_version + 1).unwrap_or(1),
            ack_policy: if cfg.ack_s3 {
                AckPolicy::S3
            } else {
                AckPolicy::Local
            },
            granted_delegations: cfg.strict_mounts,
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

    pub fn renewed_lease(&self, now: Ms, cfg: &Config, mine: &Lease) -> Lease {
        Lease {
            expires_unix_ms: now.plus(cfg.ttl_ms).0,
            released: false,
            ..mine.clone()
        }
    }

    /// A requester's inbox batch said it wants the lease (M13, M5): the
    /// same as finding it in `wanted_by` at a renewal, learned at the
    /// next poll instead of the next half-TTL.
    pub fn note_wanted(&mut self, now: Ms, node: NodeId) {
        if node == 0 || self.held.is_none() {
            return;
        }
        if !self.wanted.contains(&node) {
            self.wanted.push(node);
            self.wanted.sort_unstable();
        }
        if self.wanted_since.is_none() {
            self.wanted_since = Some(now);
        }
    }

    /// `LeaseKeeper::open_won`: take ownership of a won lease.
    pub fn adopt(&mut self, now: Ms, lease: Lease, tag: LeaseTag, gate: Option<PendingGate>) {
        self.gate = gate;
        self.wanted.clear();
        self.wanted_since = None;
        self.held_since = Some(now);
        self.last_write = now;
        self.last_seen = Some(lease.clone());
        self.cached_holder = Some(lease.holder);
        self.held = Some((lease, tag));
    }

    /// `LeaseKeeper::apply_renew`'s `Renewed` arm.
    pub fn renewed(&mut self, now: Ms, lease: Lease, tag: LeaseTag) {
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
            self.wanted_since = Some(now);
        }
        self.last_seen = Some(lease.clone());
        self.held = Some((lease, tag));
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
    pub fn note_object(&mut self, now: Ms, lease: &Lease) {
        if lease.holder != 0 {
            self.cached_holder = Some(lease.holder);
        }
        self.last_seen = Some(lease.clone());
        self.last_seen_at = Some(now);
    }
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
        let now = Ms(1_000_000);
        assert_eq!(st.classify(now, &cfg, None, 0), Plan::Create);
        // A live foreign holder is busy.
        let mut foreign = st.granted_lease(now, &cfg, None);
        foreign.holder = 3;
        match st.classify(now, &cfg, Some((foreign.clone(), tag())), 0) {
            Plan::Busy { holder: 3, .. } => {}
            other => panic!("{other:?}"),
        }
        // Expired and unreleased: a takeover with a marker.
        let expired = Lease {
            expires_unix_ms: now.0 - 1,
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
        let mine = st.granted_lease(now, &cfg, None);
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

    #[test]
    fn the_view_gates_match_lease_view() {
        let cfg = cfg();
        let now = Ms(5_000_000);
        let mut st = LeaseState::default();
        assert_eq!(st.new_mutation_epoch(now, &cfg), None);
        let lease = st.granted_lease(now, &cfg, None);
        st.adopt(
            now,
            lease,
            tag(),
            Some(PendingGate {
                epoch: 1,
                takeover: true,
                marker_shipped: false,
                drained: true,
                fast_prev: None,
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
        st.begin_handoff_pause(now, &cfg);
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
        let near_expiry = Ms(st.held.as_ref().unwrap().0.expires_unix_ms - 500);
        assert_eq!(st.ship_epoch(near_expiry, &cfg), None);
    }

    #[test]
    fn handoff_wants_dwell_then_idle_or_grace() {
        let mut cfg = cfg();
        cfg.dwell_ms = 1_000;
        cfg.idle_release_ms = 5_000;
        cfg.wanted_grace_ms = 2_000;
        let t0 = Ms(1_000_000);
        let mut st = LeaseState::default();
        let lease = st.granted_lease(t0, &cfg, None);
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
