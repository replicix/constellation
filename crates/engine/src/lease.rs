//! The FUSE-facing lease view (plan 30 M5 phase 2).
//!
//! Every lease *decision* — classify, CAS, renew, deposition, dwell,
//! grace, the handoff pause, the releasing flag, the takeover gate —
//! lives in `constellation_authority::core::lease::LeaseState`, owned by
//! the core and driven by `crate::authority_driver`. What stays here is
//! the lock-free snapshot the FUSE threads read on every mutating op:
//! the driver mirrors the core's state into it after every event
//! ([`LeaseView::mirror`]), and the FUSE fast path admits a local write
//! through it ([`LeaseView::admit`]) exactly as before.
//!
//! # The releasing flag, with the core
//!
//! A release or handoff's final section — the flush of the journal and
//! the release CAS — is a job in the core's slot; nothing the core
//! executes can interleave with it. The FUSE fast path is the one writer
//! outside the core, so the flag is still what fences it: the core sets
//! `releasing` when the section begins, the driver mirrors it here
//! *before* dispatching the section's IO, `admit` refuses new writes
//! while it is up, and the driver waits for the writes admitted before it
//! ([`LeaseView::wait_quiescent`]) and re-reads the journal before the
//! release CAS — a write that landed after the flush fails the release
//! (the next round ships it) rather than being stranded behind it.

use constellation_authority::core::LeaseState;
use constellation_authority::{Config, HonourMs, Now, WaitMs};
use constellation_platform::clock::protocol;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

pub use constellation_store_s3::lease::lease_ttl_ms;

/// Bound on [`LeaseView::wait_quiescent`]: a FUSE mutation that stays
/// admitted this long is stuck in the metadata store, not in flight.
const QUIESCE_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// Default for `CONSTELLATION_LEASE_IDLE_RELEASE_MS`.
pub const DEFAULT_IDLE_RELEASE_MS: u64 = 30_000;
/// A lease handed over cannot be handed back before this (plan 26).
pub const LEASE_MIN_DWELL_MS: u64 = 5_000;
/// A registered waiter is answered within this, busy holder or not.
pub const LEASE_WANTED_GRACE_MS: u64 = 5_000;
/// How long a handoff closes this node's own fast path (plan 29 M3c).
pub const HANDOFF_PAUSE_MS: i64 = 2_000;
/// A lease is usable only with this much left before expiry.
const EXPIRY_MARGIN_MS: i64 = 1_000;

/// [`EXPIRY_MARGIN_MS`], clamped to a quarter of the configured TTL (a
/// fixed 1 s margin is wider than a 200 ms TTL). Read once: this is on
/// every gated FUSE mutation.
pub fn expiry_margin_ms() -> i64 {
    static MARGIN: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MARGIN.get_or_init(|| EXPIRY_MARGIN_MS.min(lease_ttl_ms() as i64 / 4))
}

pub fn idle_release_ms() -> u64 {
    std::env::var("CONSTELLATION_LEASE_IDLE_RELEASE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_IDLE_RELEASE_MS)
}

/// Lock-free lease snapshot shared with the FUSE threads and the
/// control API.
#[derive(Debug, Default)]
pub struct LeaseView {
    /// The end of this node's own lease on its **honour** clock
    /// (`ProtocolClock::honour`; plan 43 C7-H1, from the body's
    /// `holder_expires_ms`: `LeaseState::honour_until`); 0 = not held, or
    /// no honour end (a body of an earlier incarnation: not usable until
    /// the next own CAS). Every judgement of it reads the same clock.
    valid_until_honour: AtomicI64,
    holder: AtomicU64,
    epoch: AtomicU64,
    lost: AtomicBool,
    /// Wait-clock ms of the last gated mutation, for idle release.
    last_write_ms: AtomicI64,
    /// Gated local mutations since start (`status.inbox.local_ops`).
    touches: AtomicU64,
    /// Continuation-epoch local authority (no S3 lease object).
    epoch_held: AtomicBool,
    /// Unix ms until which the handoff pause has closed the ordinary
    /// (non-epoch) fast path; 0 or past means open.
    handoff_pause_until_ms: AtomicI64,
    /// A release/handoff's final flush + CAS is in progress.
    releasing: AtomicBool,
    /// This node won the lease but its takeover gate has not completed.
    gate_pending: AtomicBool,
    /// Mutations admitted by [`Self::admit`] and still executing.
    inflight: AtomicU32,
    /// Plan 30 §M9: acknowledgements are gated by durability (a backup,
    /// `ack=s3`, or a backup being brought up): a fast-path mutation is
    /// acknowledged only once the core's durable watermark covers its
    /// journal row.
    ack_gated: AtomicBool,
    /// Plan 30 §M9: the tenure may be taken over before its lease
    /// expires, and the send time (honour clock) of the latest S3 request
    /// that proved this node still holds; a strict read on this node is
    /// local only while that is within `fresh_window_ms`.
    fast_tenure: AtomicBool,
    last_s3_fresh_honour: AtomicI64,
    fresh_window_ms: AtomicI64,
    /// Plan 43 C7-H2: where the takeover floor ends (this node's wait clock;
    /// 0: none) and
    /// whether a fast takeover's gate is pending: releasing the lease now
    /// would hand it on without them (`release_hold`).
    floor_until_wait: AtomicI64,
    fast_gate: AtomicBool,
}

/// Why releasing the lease now would be held back (`LeaseView::release_hold`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseHold {
    /// A fast takeover's gate is pending; it ends when the gate's work
    /// does (bounded by the caller).
    Gate,
    /// The takeover floor ends in this long (on its own: never given up
    /// on).
    Floor(std::time::Duration),
}

/// A new mutation admitted through [`LeaseView::admit`]: counted in
/// flight until dropped, so a release that raised its flag after the
/// admission waits for this mutation's journal row before its CAS.
pub struct AdmitGuard<'a>(&'a LeaseView);

impl Drop for AdmitGuard<'_> {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::SeqCst);
    }
}

impl LeaseView {
    /// Copy the core's lease state in (the driver, after every event).
    /// `ack_gated`: plan 30 §M9's durability gate is in force.
    pub fn mirror(&self, lease: &LeaseState, now: Now, cfg: &Config, ack_gated: bool) {
        self.ack_gated.store(ack_gated, Ordering::Relaxed);
        self.fresh_window_ms
            .store(cfg.backup_takeover_ms as i64, Ordering::Relaxed);
        self.lost.store(lease.lost, Ordering::Relaxed);
        self.gate_pending
            .store(lease.gate.is_some(), Ordering::SeqCst);
        self.handoff_pause_until_ms
            .store(lease.pause_until.0, Ordering::Relaxed);
        if lease.epoch_held() {
            self.holder.store(cfg.node_id, Ordering::Relaxed);
            self.epoch
                .store(lease.epoch().unwrap_or(1), Ordering::Relaxed);
            self.valid_until_honour
                .store(now.honour.plus(365 * 24 * 3600 * 1000).0, Ordering::Relaxed);
            self.epoch_held.store(true, Ordering::Release);
        } else {
            self.epoch_held.store(false, Ordering::Relaxed);
            match &lease.held {
                Some((held, _)) if !lease.lost => {
                    self.holder.store(held.holder, Ordering::Relaxed);
                    self.epoch.store(held.epoch, Ordering::Relaxed);
                    self.valid_until_honour.store(
                        lease.honour_until(cfg).map_or(0, |h| h.0),
                        Ordering::Release,
                    );
                }
                _ => {
                    self.valid_until_honour.store(0, Ordering::Relaxed);
                    if let Some(holder) = lease.cached_holder {
                        self.holder.store(holder, Ordering::Relaxed);
                    }
                    if let Some(seen) = &lease.last_seen {
                        self.epoch.store(seen.epoch, Ordering::Relaxed);
                    }
                }
            }
        }
        // Last: the flag the fast path checks first.
        self.releasing.store(lease.releasing, Ordering::SeqCst);
    }

    /// Usable right now, with enough margin to finish an op. Blind to the
    /// handoff pause (that is [`Self::open_for_new_mutation`]'s).
    pub fn usable(&self) -> bool {
        self.usable_at(protocol().honour())
    }

    /// [`Self::usable`] at an honour reading already taken (one reading per
    /// request, plan 43 C7-H1 §12).
    pub fn usable_at(&self, honour: HonourMs) -> bool {
        if self.lost.load(Ordering::Relaxed) {
            return false;
        }
        if self.epoch_held.load(Ordering::Relaxed) {
            return true;
        }
        self.valid_until_honour.load(Ordering::Relaxed) - honour.0 > expiry_margin_ms()
    }

    /// Plan 30 §M8: this node is the sequencer for reads — a usable
    /// lease, no takeover gate, no release in progress — so its replica
    /// is authoritative and a `cto=strict` read needs no ReadIndex. The
    /// handoff pause does not matter (it closes this node's own new
    /// writes, not its authority). Plan 30 §M9: on a tenure that may be
    /// taken over before its lease expires, only with fresh S3 liveness;
    /// otherwise the read goes through the core, which probes first.
    pub fn reads_locally(&self) -> bool {
        self.reads_locally_at(protocol().honour())
    }

    /// [`Self::reads_locally`] at an honour reading already taken: one
    /// reading for the lease and the liveness check, both judged on the
    /// honour clock (plan 43 C7-H1).
    pub fn reads_locally_at(&self, honour: HonourMs) -> bool {
        if self.releasing.load(Ordering::SeqCst)
            || self.gate_pending.load(Ordering::SeqCst)
            || !self.usable_at(honour)
        {
            return false;
        }
        // The core's rule (`Core::renew_fresh`), one definition.
        constellation_authority::core::liveness_fresh(
            self.fast_tenure.load(Ordering::Relaxed),
            honour,
            HonourMs(self.last_s3_fresh_honour.load(Ordering::Relaxed)),
            self.fresh_window_ms.load(Ordering::Relaxed),
        )
    }

    /// Plan 43 C7-H2: the driver mirrors what holds a release back.
    ///
    /// `floor_until`: the takeover floor's end on this node's wait clock
    /// (plan 43 C7-H1: a forward wall step must not release early).
    pub fn set_release_hold(&self, floor_until: WaitMs, fast_gate: bool) {
        self.floor_until_wait
            .store(floor_until.0, Ordering::Relaxed);
        self.fast_gate.store(fast_gate, Ordering::Relaxed);
    }

    /// What would hold back a release of the lease now, if anything: a
    /// fast takeover's pending gate, or the takeover floor.
    pub fn release_hold(&self) -> Option<ReleaseHold> {
        if self.fast_gate.load(Ordering::Relaxed) {
            return Some(ReleaseHold::Gate);
        }
        let left = self.floor_until_wait.load(Ordering::Relaxed) - crate::locks::now().wait.0;
        (left > 0).then(|| ReleaseHold::Floor(std::time::Duration::from_millis(left as u64)))
    }

    /// Plan 30 §M9: the driver mirrors the core's liveness bookkeeping
    /// (`last_s3_fresh` on the honour clock).
    pub fn set_liveness(&self, fast_tenure: bool, last_s3_fresh: HonourMs) {
        self.fast_tenure.store(fast_tenure, Ordering::Relaxed);
        self.last_s3_fresh_honour
            .store(last_s3_fresh.0, Ordering::Relaxed);
    }

    /// As [`Self::usable`], but also closed by the releasing flag, a
    /// pending takeover gate and the handoff pause.
    pub fn open_for_new_mutation(&self) -> bool {
        self.open_for_new_mutation_at(protocol().now())
    }

    /// [`Self::open_for_new_mutation`] at a reading already taken: the
    /// lease on its honour clock, the handoff pause on the wait clock
    /// (liveness: a forward step must not end the pause early).
    pub fn open_for_new_mutation_at(&self, now: Now) -> bool {
        if self.releasing.load(Ordering::SeqCst) || self.gate_pending.load(Ordering::SeqCst) {
            return false;
        }
        if self.epoch_held.load(Ordering::Relaxed) {
            return self.usable_at(now.honour);
        }
        if self.handoff_pause_until_ms.load(Ordering::Relaxed) > now.wait.0 {
            return false;
        }
        self.usable_at(now.honour)
    }

    /// Admit one new mutation, atomically with respect to a release (see
    /// the module doc): `None` when [`Self::open_for_new_mutation`] is
    /// closed. Hold the guard across the mutation's metadata write, and
    /// no longer.
    pub fn admit(&self) -> Option<AdmitGuard<'_>> {
        self.admit_at(protocol().now())
    }

    /// [`Self::admit`] at the request's one reading (`View`'s fast path
    /// takes a single `Now` for everything it decides at admission).
    pub fn admit_at(&self, now: Now) -> Option<AdmitGuard<'_>> {
        // Plan 30 §M9: under a durability gate the fast path still
        // executes here; its acknowledgement then waits for the core's
        // durable watermark (`View::ack_when_durable`).
        self.inflight.fetch_add(1, Ordering::SeqCst);
        if self.open_for_new_mutation_at(now) {
            Some(AdmitGuard(self))
        } else {
            self.inflight.fetch_sub(1, Ordering::SeqCst);
            None
        }
    }

    /// Wait for every mutation admitted before the releasing flag went
    /// up to finish (bounded by [`QUIESCE_MAX`]). The driver calls this
    /// before a release CAS.
    pub async fn wait_quiescent(&self) {
        let deadline = Instant::now() + QUIESCE_MAX;
        while self.inflight.load(Ordering::SeqCst) > 0 {
            if Instant::now() >= deadline {
                tracing::warn!(
                    inflight = self.inflight.load(Ordering::SeqCst),
                    "a release waited {QUIESCE_MAX:?} for admitted mutations to finish; proceeding"
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Plan 30 §M9: whether a fast-path acknowledgement waits for
    /// durability.
    pub fn ack_gated(&self) -> bool {
        self.ack_gated.load(Ordering::Relaxed)
    }

    /// Whether the takeover gate is still pending (`status`).
    pub fn gate_pending(&self) -> bool {
        self.gate_pending.load(Ordering::SeqCst)
    }

    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    /// Record write activity; keeps the idle-release timer from firing
    /// under a running workload.
    pub fn touch(&self, now: WaitMs) {
        self.last_write_ms.store(now.0, Ordering::Relaxed);
        self.touches.fetch_add(1, Ordering::Relaxed);
    }

    /// Gated local mutations since start (see `touches`).
    pub fn touches(&self) -> u64 {
        self.touches.load(Ordering::Relaxed)
    }

    /// Wait-clock ms of the last fast-path write (0 if none).
    pub fn last_write_ms(&self) -> i64 {
        self.last_write_ms.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> constellation_control::proto::types::LeaseStatus {
        // On the honour clock, the one the lease is judged on (plan 43
        // C7-H1): what is left is what this node will honour.
        let now = protocol().honour().0;
        let until = self.valid_until_honour.load(Ordering::Relaxed);
        constellation_control::proto::types::LeaseStatus {
            held: until > now,
            holder: self.holder.load(Ordering::Relaxed),
            epoch: self.epoch.load(Ordering::Relaxed),
            expires_in_ms: if until > 0 { until - now } else { 0 },
            lost: self.lost.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_authority::{Ms, Now};
    use constellation_store_s3::lease::now_unix_ms;
    use constellation_store_s3::{Lease, LeaseMode, LeaseStore};

    fn tag() -> constellation_store_s3::LeaseTag {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = std::sync::Arc::new(object_store::memory::InMemory::new());
            LeaseStore::new(store, "p0", LeaseMode::Cas)
                .try_create(&Lease::granted("p0", 1, 1, 1000))
                .await
                .unwrap()
        })
    }

    /// Plan 43 C7-H1 (2/4): the view judges the lease the core mirrored, and
    /// the liveness proof a strict read needs, on the honour clock: a
    /// backward wall step of 3 s, a hundred milliseconds before either ends,
    /// does not give it 3 s more (`step_test_wall` moves this thread's test
    /// clock as a wall step does). Fails on the parent: both were wall
    /// values compared with the stepped-back wall clock.
    #[test]
    fn usable_ends_at_the_honour_end_after_a_backward_step() {
        let cfg = Config::defaults(7, 1);
        let start = crate::locks::now();
        let mut state = LeaseState::default();
        let lease = state.granted_lease(start, &cfg, None, false);
        state.adopt(start, lease, tag(), None);
        let view = LeaseView::default();
        view.mirror(&state, start, &cfg, false);
        let margin = expiry_margin_ms();
        let ttl = cfg.ttl_ms as i64;
        crate::locks::advance_test_clock(ttl - margin - 100);
        crate::locks::step_test_wall(-3_000);
        let at = crate::locks::now();
        assert!(view.usable_at(at.honour));
        crate::locks::advance_test_clock(101);
        let at = crate::locks::now();
        assert!(
            !view.usable_at(at.honour),
            "usable past its honour end because the wall clock stepped back"
        );
        // What the parent judged: the wall expiry against the wall clock.
        let wall_expiry = start.wall.0 + ttl;
        assert!(wall_expiry - at.wall.0 > margin, "the old rule says usable");
        // The liveness proof: fresh within the window of its send.
        let view = LeaseView::default();
        let renewed = crate::locks::now();
        let mut state = LeaseState::default();
        let lease = state.granted_lease(renewed, &cfg, None, false);
        state.adopt(renewed, lease, tag(), None);
        view.mirror(&state, renewed, &cfg, false);
        let sent = renewed.honour;
        view.set_liveness(true, sent);
        let w = cfg.backup_takeover_ms as i64;
        let waited = (crate::locks::now().honour.since(sent) + w - 100) as u64;
        crate::locks::advance_test_clock(waited as i64);
        crate::locks::step_test_wall(-3_000);
        assert!(
            view.reads_locally_at(crate::locks::now().honour),
            "inside the window"
        );
        crate::locks::advance_test_clock(200);
        assert!(
            !view.reads_locally_at(crate::locks::now().honour),
            "fresh past its window because the wall clock stepped back"
        );
    }

    /// Plan 43 C7-H2: what holds back a release, as the upgrade's handover
    /// waits on it: nothing, a fast takeover's pending gate, or the floor
    /// with its remainder (the gate first: it has no end of its own).
    #[test]
    fn the_release_hold_names_the_gate_or_the_floors_remainder() {
        use std::time::Duration;
        let view = LeaseView::default();
        assert_eq!(view.release_hold(), None);
        let wait = |d: i64| WaitMs(crate::locks::now().wait.0 + d);
        view.set_release_hold(wait(-1), false);
        assert_eq!(view.release_hold(), None, "a floor in the past holds");
        view.set_release_hold(wait(5_000), false);
        match view.release_hold() {
            Some(ReleaseHold::Floor(left)) => {
                assert!(left > Duration::from_millis(4_000) && left <= Duration::from_secs(5))
            }
            other => panic!("{other:?}"),
        }
        view.set_release_hold(wait(5_000), true);
        assert_eq!(view.release_hold(), Some(ReleaseHold::Gate));
        view.set_release_hold(WaitMs(0), false);
        assert_eq!(view.release_hold(), None);
    }

    /// Plan 43 C7-H1: the floor's remainder is on the wait clock, which a
    /// forward wall step does not move: the handover that waits for the floor
    /// is not released early by one. Fails on the parent (a wall deadline
    /// read against the stepped wall clock: the remainder drops by the step).
    #[test]
    fn release_hold_is_not_shortened_by_a_forward_step() {
        use std::time::Duration;
        let view = LeaseView::default();
        let wait = crate::locks::now().wait;
        view.set_release_hold(WaitMs(wait.0 + 5_000), false);
        crate::locks::advance_test_clock(1_000);
        crate::locks::step_test_wall(4_500);
        match view.release_hold() {
            Some(ReleaseHold::Floor(left)) => assert!(
                left > Duration::from_millis(3_900) && left <= Duration::from_millis(4_000),
                "the floor's remainder moved with the wall step: {left:?}"
            ),
            other => panic!("the floor ended with the wall step: {other:?}"),
        }
        crate::locks::step_test_wall(-30_000);
        match view.release_hold() {
            Some(ReleaseHold::Floor(left)) => assert!(left <= Duration::from_millis(4_000)),
            other => panic!("a backward step ended or lengthened the floor: {other:?}"),
        }
    }

    /// The mirror follows the core's state: held opens the fast path;
    /// the releasing flag, a pending gate and the handoff pause close it
    /// (a pause leaves `usable` alone, so shipping still drains); a
    /// continuation-epoch hold opens it without an S3 lease.
    #[test]
    fn the_mirror_tracks_the_core_gates() {
        let cfg = Config::defaults(7, 1);
        let now = Ms(now_unix_ms());
        let view = LeaseView::default();
        let mut state = LeaseState::default();
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(!view.usable());
        assert!(view.admit().is_none());

        let lease = state.granted_lease(Now::same(now.0), &cfg, None, false);
        state.adopt(Now::same(now.0), lease, tag(), None);
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(view.usable());
        assert!(view.open_for_new_mutation());
        assert_eq!(view.status().holder, 7);

        state.releasing = true;
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(view.usable());
        assert!(view.admit().is_none(), "releasing closes new mutations");
        state.releasing = false;

        state.pause_until = WaitMs(now.0 + cfg.handoff_pause_ms as i64);
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(view.usable());
        assert!(!view.open_for_new_mutation(), "paused for a handoff");
        state.pause_until = WaitMs(0);

        state.gate = Some(constellation_authority::core::PendingGate {
            epoch: 1,
            takeover: true,
            marker_shipped: false,
            drained: true,
            fast_prev: false,
            backup_tail_epoch: None,
            shippable: false,
        });
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(view.gate_pending());
        assert!(view.admit().is_none());
        state.gate = None;

        state.released();
        state.adopt_epoch_hold(Now::same(now.0), 3);
        view.mirror(&state, Now::same(now.0), &cfg, false);
        assert!(view.usable() && view.open_for_new_mutation());
        assert_eq!(view.status().epoch, 3);
    }

    /// An admitted mutation is counted until its guard drops, and the
    /// quiescence wait returns once none is in flight.
    #[test]
    fn admitted_mutations_are_waited_for() {
        let cfg = Config::defaults(7, 1);
        let now = Ms(now_unix_ms());
        let view = std::sync::Arc::new(LeaseView::default());
        let mut state = LeaseState::default();
        let lease = state.granted_lease(Now::same(now.0), &cfg, None, false);
        state.adopt(Now::same(now.0), lease, tag(), None);
        view.mirror(&state, Now::same(now.0), &cfg, false);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let guard = view.admit().expect("open");
            assert_eq!(view.inflight.load(Ordering::SeqCst), 1);
            let waiter = {
                let view = view.clone();
                tokio::spawn(async move {
                    view.wait_quiescent().await;
                })
            };
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            assert!(!waiter.is_finished());
            drop(guard);
            tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
                .await
                .expect("quiescent")
                .unwrap();
        });
    }
}

/// Plan 30 §M11: the delegations this node holds, mirrored from the
/// core after every event for the FUSE fast path's sibling check
/// (`view::View::mutate_op_rebasable_with_rid`): a write whose keys fall
/// under one of them, while its grant is honoured, executes on the FUSE
/// thread as the delegate (`Meta::delegate_execute`) with no channel
/// round trip, and the core streams it from the journal.
#[derive(Debug, Default)]
pub struct DelegateView {
    /// `(dir, gen, honoured until, stopped)`: `until` on this node's honour
    /// clock (plan 43 C7-H1).
    entries: std::sync::Mutex<Vec<(u64, u64, i64, bool)>>,
    /// Ops the FUSE fast path executed here as the delegate (the core's
    /// `deleg_executed` counts the ones that went through it).
    executed: std::sync::atomic::AtomicU64,
    /// Generations the driver stopped ahead of the core (a recall came
    /// in): the fast path admits nothing under them from that instant,
    /// so the `through` the core answers the recall with is final.
    stopped: std::sync::Mutex<std::collections::HashSet<u64>>,
    /// Fast-path executions admitted and not yet journaled.
    in_flight: std::sync::atomic::AtomicU64,
    /// Phase 2b: the delegate acknowledges only after its backup (or the
    /// root's segment): its writes go through the core and park there.
    gated: std::sync::atomic::AtomicBool,
    /// Plan 30 §M12: the keys of the ops the root's fast path executed
    /// since the driver last drained them, for the placement's
    /// histogram (`Core::place_note_local`).
    fast_path_notes: std::sync::Mutex<Vec<constellation_meta::TouchSet>>,
    /// M16: how many writes the root's fast path executed since the
    /// driver last drained this, for the root lease's placement
    /// (`crate::placement::Placement::note_ops`), exact even when
    /// `fast_path_notes` drops its oldest entries.
    fast_path_ops: std::sync::atomic::AtomicU64,
    /// Plan 30 §M12: ops the root's fast path sent through the core
    /// because a live delegation owned their keys (`status`).
    routed: std::sync::atomic::AtomicU64,
    /// Ops the delegate's fast path sent through the core because its
    /// generation ended at this node's takeover gate while the op waited
    /// for the store's writer lock (`Meta::begin_local`).
    fenced: std::sync::atomic::AtomicU64,
}

/// A fast-path execution admitted under a grant; dropped once the op is
/// journaled (or refused), so a recall can wait for every admitted op.
pub struct DelegateAdmission<'a> {
    pub gen: u64,
    view: &'a DelegateView,
}

impl Drop for DelegateAdmission<'_> {
    fn drop(&mut self) {
        self.view
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

impl DelegateView {
    /// A recall of `gen` arrived: admit nothing more under it. The
    /// caller waits for `in_flight` to drain before the core answers.
    pub fn stop(&self, gen: u64) {
        self.stopped.lock().unwrap().insert(gen);
        let mut g = self.entries.lock().unwrap();
        for e in g.iter_mut() {
            if e.1 == gen {
                e.3 = true;
            }
        }
    }

    /// F1/F9: whether this node still honours its delegation `gen` now
    /// (an admission's re-check once its op holds the store's writer
    /// lock): live, not past its window. A stop (a recall) waits for the
    /// admissions in flight, so it is not looked at.
    pub fn honours(&self, gen: u64) -> bool {
        // The clock the lock and fence decisions read (a unit test's
        // `advance_test_clock` included), as the check at the write does:
        // read afresh, F1 is "judged at the write".
        let now = crate::locks::now().honour.0;
        self.entries
            .lock()
            .unwrap()
            .iter()
            .any(|(_, g, until, _)| *g == gen && now < *until)
    }

    /// Fast-path executions admitted and not yet journaled.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Phase 2b: close (or open) the fast path for delegated writes.
    pub fn set_gated(&self, gated: bool) {
        self.gated
            .store(gated, std::sync::atomic::Ordering::Relaxed);
    }

    /// The fast path executed one op as the delegate.
    pub fn note_executed(&self) {
        self.executed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Ops the fast path executed as the delegate so far.
    pub fn executed(&self) -> u64 {
        self.executed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Plan 30 §M12: the root's fast path executed an op with `keys`.
    pub fn note_fast_path(&self, keys: constellation_meta::TouchSet) {
        self.fast_path_ops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut g = self.fast_path_notes.lock().unwrap();
        // Bounded: the driver drains after every event; a long stall
        // keeps the latest instead of growing without bound.
        if g.len() >= 4096 {
            g.remove(0);
        }
        g.push(keys);
    }

    /// The notes since the last drain.
    pub fn take_fast_path_notes(&self) -> Vec<constellation_meta::TouchSet> {
        std::mem::take(&mut *self.fast_path_notes.lock().unwrap())
    }

    /// The fast-path write count since the last drain.
    pub fn take_fast_path_ops(&self) -> u64 {
        self.fast_path_ops
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Plan 30 §M12: the root's fast path sent an op through the core
    /// (a live delegation owned its keys).
    pub fn note_routed(&self) {
        self.routed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn routed(&self) -> u64 {
        self.routed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The delegate's fast path was refused at the write under a
    /// generation its own takeover gate ended, and sent the op through the
    /// core.
    pub fn note_fenced(&self) {
        self.fenced
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn fenced(&self) -> u64 {
        self.fenced.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn mirror(&self, view: &constellation_authority::core::DelegView) {
        let mut stopped = self.stopped.lock().unwrap();
        let mut g = self.entries.lock().unwrap();
        g.clear();
        for (dir, gen, until, is_stopped, _, _, _) in &view.mine {
            g.push((*dir, *gen, *until, *is_stopped || stopped.contains(gen)));
        }
        // A generation the core dropped is over: forget the stop.
        stopped.retain(|gen| view.mine.iter().any(|m| m.1 == *gen));
    }

    /// The generation this node executes `keys` under right now, if any:
    /// the keys' owner is one of this node's live, honoured delegations.
    /// The admission is in flight until dropped.
    pub fn admit_for(
        &self,
        meta: &constellation_meta::Meta,
        keys: &constellation_meta::TouchSet,
        now: Now,
    ) -> Option<DelegateAdmission<'_>> {
        let g = self.entries.lock().unwrap();
        if g.is_empty() || self.gated.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        drop(g);
        let constellation_meta::delegation::Ownership::Delegated(d) = meta.resolve_ownership(keys)
        else {
            return None;
        };
        let now = now.honour.0;
        // Counted before the check under the same lock a `stop` takes:
        // once `stop` returns, no new admission of that generation
        // exists and every earlier one is counted.
        let g = self.entries.lock().unwrap();
        let gen = g
            .iter()
            .find(|(_, gen, until, stopped)| *gen == d.gen && !*stopped && now < *until)
            .map(|(_, gen, _, _)| *gen)?;
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        drop(g);
        Some(DelegateAdmission { gen, view: self })
    }
}
