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
use constellation_authority::{Config, Ms};
use constellation_store_s3::lease::now_unix_ms;
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
    /// Unix ms until which this node holds the lease; 0 = not held.
    valid_until_ms: AtomicI64,
    holder: AtomicU64,
    epoch: AtomicU64,
    lost: AtomicBool,
    /// Unix ms of the last gated mutation, for idle release.
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
    /// expires, and the send time (unix ms) of the latest S3 request that
    /// proved this node still holds; a strict read on this node is local
    /// only while that is within `fresh_window_ms`.
    fast_tenure: AtomicBool,
    last_s3_fresh_ms: AtomicI64,
    fresh_window_ms: AtomicI64,
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
    pub fn mirror(&self, lease: &LeaseState, now: Ms, cfg: &Config, ack_gated: bool) {
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
            self.valid_until_ms
                .store(now.0 + 365 * 24 * 3600 * 1000, Ordering::Relaxed);
            self.epoch_held.store(true, Ordering::Release);
        } else {
            self.epoch_held.store(false, Ordering::Relaxed);
            match &lease.held {
                Some((held, _)) if !lease.lost => {
                    self.holder.store(held.holder, Ordering::Relaxed);
                    self.epoch.store(held.epoch, Ordering::Relaxed);
                    self.valid_until_ms
                        .store(held.expires_unix_ms, Ordering::Release);
                }
                _ => {
                    self.valid_until_ms.store(0, Ordering::Relaxed);
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
        if self.lost.load(Ordering::Relaxed) {
            return false;
        }
        if self.epoch_held.load(Ordering::Relaxed) {
            return true;
        }
        self.valid_until_ms.load(Ordering::Relaxed) - now_unix_ms() > expiry_margin_ms()
    }

    /// Plan 30 §M8: this node is the sequencer for reads — a usable
    /// lease, no takeover gate, no release in progress — so its replica
    /// is authoritative and a `cto=strict` read needs no ReadIndex. The
    /// handoff pause does not matter (it closes this node's own new
    /// writes, not its authority). Plan 30 §M9: on a tenure that may be
    /// taken over before its lease expires, only with fresh S3 liveness;
    /// otherwise the read goes through the core, which probes first.
    pub fn reads_locally(&self) -> bool {
        if self.releasing.load(Ordering::SeqCst)
            || self.gate_pending.load(Ordering::SeqCst)
            || !self.usable()
        {
            return false;
        }
        if !self.fast_tenure.load(Ordering::Relaxed) {
            return true;
        }
        now_unix_ms() - self.last_s3_fresh_ms.load(Ordering::Relaxed)
            < self.fresh_window_ms.load(Ordering::Relaxed)
    }

    /// Plan 30 §M9: the driver mirrors the core's liveness bookkeeping.
    pub fn set_liveness(&self, fast_tenure: bool, last_s3_fresh_ms: i64) {
        self.fast_tenure.store(fast_tenure, Ordering::Relaxed);
        self.last_s3_fresh_ms
            .store(last_s3_fresh_ms, Ordering::Relaxed);
    }

    /// As [`Self::usable`], but also closed by the releasing flag, a
    /// pending takeover gate and the handoff pause.
    pub fn open_for_new_mutation(&self) -> bool {
        if self.releasing.load(Ordering::SeqCst) || self.gate_pending.load(Ordering::SeqCst) {
            return false;
        }
        if self.epoch_held.load(Ordering::Relaxed) {
            return self.usable();
        }
        if self.handoff_pause_until_ms.load(Ordering::Relaxed) > now_unix_ms() {
            return false;
        }
        self.usable()
    }

    /// Admit one new mutation, atomically with respect to a release (see
    /// the module doc): `None` when [`Self::open_for_new_mutation`] is
    /// closed. Hold the guard across the mutation's metadata write, and
    /// no longer.
    pub fn admit(&self) -> Option<AdmitGuard<'_>> {
        // Plan 30 §M9: under a durability gate the fast path still
        // executes here; its acknowledgement then waits for the core's
        // durable watermark (`ConstellationFs::ack_when_durable`).
        self.inflight.fetch_add(1, Ordering::SeqCst);
        if self.open_for_new_mutation() {
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
    pub fn touch(&self) {
        self.last_write_ms.store(now_unix_ms(), Ordering::Relaxed);
        self.touches.fetch_add(1, Ordering::Relaxed);
    }

    /// Gated local mutations since start (see `touches`).
    pub fn touches(&self) -> u64 {
        self.touches.load(Ordering::Relaxed)
    }

    /// Unix ms of the last fast-path write (0 if none).
    pub fn last_write_ms(&self) -> i64 {
        self.last_write_ms.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> constellation_api::LeaseStatus {
        let now = now_unix_ms();
        let until = self.valid_until_ms.load(Ordering::Relaxed);
        constellation_api::LeaseStatus {
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
        view.mirror(&state, now, &cfg, false);
        assert!(!view.usable());
        assert!(view.admit().is_none());

        let lease = state.granted_lease(now, &cfg, None);
        state.adopt(now, lease, tag(), None);
        view.mirror(&state, now, &cfg, false);
        assert!(view.usable());
        assert!(view.open_for_new_mutation());
        assert_eq!(view.status().holder, 7);

        state.releasing = true;
        view.mirror(&state, now, &cfg, false);
        assert!(view.usable());
        assert!(view.admit().is_none(), "releasing closes new mutations");
        state.releasing = false;

        state.begin_handoff_pause(now, &cfg);
        view.mirror(&state, now, &cfg, false);
        assert!(view.usable());
        assert!(!view.open_for_new_mutation(), "paused for a handoff");
        state.pause_until = Ms(0);

        state.gate = Some(constellation_authority::core::PendingGate {
            epoch: 1,
            takeover: true,
            marker_shipped: false,
            drained: true,
            fast_prev: None,
            backup_tail_epoch: None,
            shippable: false,
        });
        view.mirror(&state, now, &cfg, false);
        assert!(view.gate_pending());
        assert!(view.admit().is_none());
        state.gate = None;

        state.released();
        state.adopt_epoch_hold(now, 3);
        view.mirror(&state, now, &cfg, false);
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
        let lease = state.granted_lease(now, &cfg, None);
        state.adopt(now, lease, tag(), None);
        view.mirror(&state, now, &cfg, false);
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
/// (`fusefs::mutate_op_rebasable_with_rid`): a write whose keys fall
/// under one of them, while its grant is honoured, executes on the FUSE
/// thread as the delegate (`Meta::delegate_execute`) with no channel
/// round trip, and the core streams it from the journal.
#[derive(Debug, Default)]
pub struct DelegateView {
    /// `(dir, gen, honoured until unix ms, stopped)`.
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
    /// Plan 30 §M12: ops the root's fast path sent through the core
    /// because a live delegation owned their keys (`status`).
    routed: std::sync::atomic::AtomicU64,
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

    /// Plan 30 §M12: the root's fast path sent an op through the core
    /// (a live delegation owned its keys).
    pub fn note_routed(&self) {
        self.routed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn routed(&self) -> u64 {
        self.routed.load(std::sync::atomic::Ordering::Relaxed)
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
        let now = now_unix_ms();
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
