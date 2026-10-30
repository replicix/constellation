//! Plan 41 R1: the root's delegation windows, in one table.
//!
//! A generation's *window* is how long the root outwaits its delegate
//! (`until`, on the root's **wait** clock): a granting renewal sets it to
//! `receipt + ttl + extra`, the root reclaims at it. The core used to own
//! it (`GenState::until`), and every extension and every decision on it was
//! a step of the core. With a second party that may extend it while a core
//! step is running (the off-core answerer of plan 41 R2, which answers a
//! delegation's renewal while the root's core is stuck), the window lives
//! here instead, under one mutex, and the core's decisions on it are single
//! critical sections that read the table's own clock inside the guard.
//!
//! # The two rules
//!
//! 1. **Extensions are monotone and commute**: [`DelegWindows::extend_core`]
//!    (and R2's answerer) are a `max`; two of them in either order leave the
//!    same end.
//! 2. **Whatever ends or shortens a window, or after which the core would
//!    grant no more, first *closes* the slot, inside the core's step**, at
//!    the transition (not at the end of the step, not when a certificate is
//!    next published): a recall begins, a seal is sent, the generation ends,
//!    the lease is gone, an epoch opens. A closed slot is extended by
//!    nobody but the core, and only by a decision the core makes then (the
//!    core extends only while it would grant, which reopens the slot). The
//!    expiry decision is [`DelegWindows::close_if_expired`]: it reads the
//!    clock *inside* the guard and closes in the same critical section, so a
//!    window extended after the core's step took its `now` (a step that
//!    waited in the store between its clock read and its writes) is
//!    re-armed, never sealed or reclaimed.
//!
//! The delegate honours `sent + ttl − margin` on its own clock from its own
//! request, the root's window is `receipt + ttl + extra` with
//! `receipt ≥ sent` and `extra ≥ margin`: whoever sets the window, the
//! delegate stops before the root reclaims (`Core::on_deleg_renew` has the
//! argument).
//!
//! R1 is the table and the core's use of it. R2 adds the off-core answerer's
//! side: the lease certificate the core publishes after every step
//! ([`RootLease`]) and [`DelegWindows::extend_off_core`], which decides from
//! it and from the generation's slot in one critical section, so a
//! certificate that revokes (a release, an epoch, a deposition) and an
//! extension are ordered by the same mutex.

use crate::ids::{HonourMs, NodeId, Now, WaitMs};
use crate::root_probe::RootProbe;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// The table's clock (the daemon's protocol clock, a simulation's node
/// clock): read inside the guard by the decisions that must not act on a
/// step's older `now`.
pub type WindowsClock = Arc<dyn Fn() -> Now + Send + Sync>;

/// One generation's window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// The delegate the generation was granted to (a renewal of another
    /// node never extends it).
    pub node: NodeId,
    /// The root outwaits the delegate once its wait clock reaches this.
    pub until: WaitMs,
    /// What the renewals alone gave `until` (the rest is the delegate's
    /// stream, `Core::deleg_live_by_stream`): a statistic.
    pub renewed_until: WaitMs,
    /// The core would grant a renewal of this generation now (no recall,
    /// seal or end begun, no epoch, the lease held), and the generation is
    /// one an off-core party may extend at all (`eligible`).
    pub open: bool,
    /// Not a designation: only a delegation the root reclaims by the clock
    /// is ever opened to a party other than the core.
    pub eligible: bool,
    /// The lock side has nothing to hand this generation that an answer
    /// without lock content would leave behind: a granting answer was sent
    /// (the delegate has the first handoff, the grace and the floor), and
    /// no handoff, barrier or raised floor is pending since
    /// (`Core::deleg_settled`). Set by the core when it publishes
    /// ([`DelegWindows::publish`]).
    pub settled: bool,
}

/// What the off-core answerer needs to know of the root's lease and
/// tenure, published by the core after every step (R2; [`Core::deleg_root_lease`]):
/// the state at the end of the last finished step, never newer. Every field
/// is either time-bounded (judged against the answerer's own reading of the
/// honour clock when it answers) or a fact that can only become false
/// inside a running step, where the argument in `deleg_lane::answer_root`
/// covers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootLease {
    /// The lease epoch the certificate is of.
    pub epoch: u64,
    /// The end of the lease on this node's **honour** clock; the cap of a
    /// grant is this less the margin.
    pub honour_until: HonourMs,
    /// `expiry_margin_ms`.
    pub margin_ms: u64,
    /// `delegation_ttl_ms`, and what the root adds to a grant's end
    /// (`reclaim_horizon_ms − delegation_ttl_ms`).
    pub delegation_ttl_ms: u64,
    pub extra_ms: u64,
    /// The tenure may be taken over before its lease expires
    /// (`may_be_taken_over_early`), the send of the latest S3 request that
    /// proved it live (honour clock), the freshness window, and whether the
    /// lease is marked (`tenure_marked`): the freshness rule of
    /// `renew_hold`.
    pub fast: bool,
    pub last_s3_fresh: HonourMs,
    pub window_ms: i64,
    pub marked: bool,
}

/// Why the answerer did not extend a window ([`DelegWindows::extend_off_core`]):
/// the request is queued to the core as before and answered there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The driver is not stuck (the common case: the renewal is the core's).
    NotStuck,
    /// The driver has been stuck past the bound: hung, not slow.
    Hung,
    /// No certificate: this node is not a usable root now (no lease, a
    /// release or takeover gate, an epoch).
    Lease,
    /// No such generation in the table.
    Generation,
    /// Not the generation's delegate.
    Node,
    /// The slot is closed (a recall, a seal or an end began), or the
    /// generation is a designation.
    Closed,
    /// The lock side has something an answer without lock content would
    /// leave behind (`Window::settled`).
    Unsettled,
    /// A tenure that may be taken over early without fresh S3 liveness.
    Stale,
    /// A tenure that may be taken over early whose lease is not marked.
    Unmarked,
    /// Nothing is left of the lease below its margin: the cap is zero.
    Cap,
    /// An off-core read of the lease object showed the tenure over (plan 41
    /// D3, [`crate::root_probe`]): nothing more is answered for its epoch.
    Deposed,
}

impl Refusal {
    pub const COUNT: usize = 11;
    pub const ALL: [Refusal; Refusal::COUNT] = [
        Refusal::NotStuck,
        Refusal::Hung,
        Refusal::Lease,
        Refusal::Generation,
        Refusal::Node,
        Refusal::Closed,
        Refusal::Unsettled,
        Refusal::Stale,
        Refusal::Unmarked,
        Refusal::Cap,
        Refusal::Deposed,
    ];

    pub fn index(self) -> usize {
        self as usize
    }

    pub fn label(self) -> &'static str {
        match self {
            Refusal::NotStuck => "not_stuck",
            Refusal::Hung => "hung",
            Refusal::Lease => "lease",
            Refusal::Generation => "generation",
            Refusal::Node => "node",
            Refusal::Closed => "closed",
            Refusal::Unsettled => "unsettled",
            Refusal::Stale => "stale_liveness",
            Refusal::Unmarked => "unmarked",
            Refusal::Cap => "cap",
            Refusal::Deposed => "deposed",
        }
    }
}

/// The decision of the answerer (pure): extend `w` to the returned `until`
/// with a grant of the returned `ttl_ms`, or why not. `now` is the
/// answerer's own reading, taken when the request arrived; `lease` is the
/// core's certificate, which holds at least as far as the argument in
/// `deleg_lane::answer_root` says. `proof` is the send of the latest off-core
/// read of the lease object that showed this tenure live (plan 41 D3,
/// [`RootProbe::proof`]): the freshness rule takes the later of it and the
/// core's own.
pub fn decide(
    lease: &RootLease,
    w: &Window,
    from: NodeId,
    now: Now,
    proof: Option<HonourMs>,
) -> Result<(u64, WaitMs), Refusal> {
    if w.node != from {
        return Err(Refusal::Node);
    }
    if !w.open || !w.eligible {
        return Err(Refusal::Closed);
    }
    if !w.settled {
        return Err(Refusal::Unsettled);
    }
    // The core's freshness rule (`Core::renew_hold`): a tenure that may be
    // taken over before its lease expires answers only within the freshness
    // window of a proof of liveness, and only if its lease is marked.
    if !lease.marked {
        return Err(Refusal::Unmarked);
    }
    // The cap (`Core::grant_cap_ms`): a grant never outlives the lease's
    // usable end, which is also `root_usable`'s time bound.
    let cap = lease
        .honour_until
        .since(now.honour)
        .saturating_sub(lease.margin_ms as i64);
    if cap <= 0 {
        return Err(Refusal::Cap);
    }
    // (Last: the one refusal an off-core read of the lease can lift, so a
    // read is asked for only when nothing else stands in the way.)
    let fresh = proof.map_or(lease.last_s3_fresh, |p| lease.last_s3_fresh.max(p));
    if !crate::core::liveness_fresh(lease.fast, now.honour, fresh, lease.window_ms) {
        return Err(Refusal::Stale);
    }
    let ttl = lease.delegation_ttl_ms.min(cap as u64);
    Ok((ttl, now.wait.plus(ttl + lease.extra_ms)))
}

/// A test hook run once before the expiry decision takes the guard.
#[cfg(test)]
type CloseHook = Box<dyn FnOnce(&DelegWindows) + Send>;

#[derive(Default)]
struct Inner {
    slots: HashMap<u64, Window>,
    clock: Option<WindowsClock>,
    /// The lease facts the answerer needs, published by the core after
    /// every step ([`DelegWindows::publish`]); `None`: answer nothing.
    lease: Option<RootLease>,
    /// The off-core proof of liveness the answerer takes besides the core's
    /// own (plan 41 D3); `None`: the core's alone.
    probe: Option<Arc<RootProbe>>,
    /// Test hook: run once just before [`DelegWindows::close_if_expired`]
    /// takes the guard (an extension landing between a step's earlier read
    /// of the window and its decision).
    #[cfg(test)]
    before_close: Option<CloseHook>,
}

/// The root's delegation windows, per generation. See the module doc.
#[derive(Default)]
pub struct DelegWindows {
    inner: Mutex<Inner>,
    stats: WindowStats,
    /// Non-vacuity knobs of the simulation (never set by the daemon): skip
    /// the close a recall makes, and the close the expiry decision makes —
    /// the code without R1's rule 2.
    skip_close_on_recall: std::sync::atomic::AtomicBool,
    skip_close_on_expiry: std::sync::atomic::AtomicBool,
}

/// What the answerer did (`status`: `delegation.root_answers_off_core`,
/// `root_answers_deferred`).
#[derive(Default, Debug)]
pub struct WindowStats {
    /// Renewals extended off the core ([`DelegWindows::extend_off_core`]).
    pub answered_off_core: AtomicU64,
    /// Renewals the answerer declined while the driver was stuck, by reason
    /// (indexed by [`Refusal::index`]).
    pub deferred: [AtomicU64; Refusal::COUNT],
}

impl std::fmt::Debug for DelegWindows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.lock();
        f.debug_struct("DelegWindows")
            .field("slots", &g.slots)
            .finish_non_exhaustive()
    }
}

impl DelegWindows {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The clock the guard-side decisions read ([`WindowsClock`]). Unset (a
    /// core unit test driving a step with an explicit `now`), they read the
    /// step's own `now`.
    pub fn set_clock(&self, clock: WindowsClock) {
        self.lock().clock = Some(clock);
    }

    /// The off-core proof of liveness the answerer consults (plan 41 D3).
    pub fn set_probe(&self, probe: Arc<RootProbe>) {
        self.lock().probe = Some(probe);
    }

    /// The proof set by [`Self::set_probe`].
    pub fn probe(&self) -> Option<Arc<RootProbe>> {
        self.lock().probe.clone()
    }

    fn now_or(g: &Inner, fallback: Now) -> Now {
        match &g.clock {
            Some(clock) => clock(),
            None => fallback,
        }
    }

    /// A new generation: its window, and whether it may ever be opened to a
    /// party other than the core (`eligible`). Open from the start when so.
    pub fn insert(
        &self,
        gen: u64,
        node: NodeId,
        until: WaitMs,
        renewed_until: WaitMs,
        eligible: bool,
    ) {
        self.lock().slots.insert(
            gen,
            Window {
                node,
                until,
                renewed_until,
                open: eligible,
                eligible,
                settled: false,
            },
        );
    }

    /// The lease is gone: no generation is this node's. Every slot goes,
    /// and the certificate with them.
    pub fn clear(&self) {
        let mut g = self.lock();
        g.slots.clear();
        g.lease = None;
    }

    pub fn stats(&self) -> &WindowStats {
        &self.stats
    }

    /// How many generations have a slot (`container_sizes`).
    pub fn len(&self) -> usize {
        self.lock().slots.len()
    }

    /// Whether no generation has a slot.
    pub fn is_empty(&self) -> bool {
        self.lock().slots.is_empty()
    }

    /// Forget generation `gen`'s slot (the core forgot the generation:
    /// `Core::deleg_forget_ended`). The answerer then refuses it as one it
    /// does not know, as it refused it closed.
    pub fn remove(&self, gen: u64) {
        self.lock().slots.remove(&gen);
    }

    pub fn get(&self, gen: u64) -> Option<Window> {
        self.lock().slots.get(&gen).copied()
    }

    /// The generation's window end (`None`: no such generation).
    pub fn until(&self, gen: u64) -> Option<WaitMs> {
        self.lock().slots.get(&gen).map(|w| w.until)
    }

    pub fn renewed_until(&self, gen: u64) -> Option<WaitMs> {
        self.lock().slots.get(&gen).map(|w| w.renewed_until)
    }

    /// Close the slot: nothing but the core extends it from here (rule 2).
    pub fn close(&self, gen: u64) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.open = false;
        }
    }

    /// A recall begins: [`Self::close`], unless the simulation's non-vacuity
    /// knob says to skip it ([`Self::skip_closes_for_tests`]).
    pub fn close_for_recall(&self, gen: u64) {
        if !self.skip_close_on_recall.load(Ordering::Relaxed) {
            self.close(gen);
        }
    }

    /// The simulation's non-vacuity knobs: a build without the close a recall
    /// makes (`recall`) and without the one the expiry decision makes
    /// (`expiry`). Never set by the daemon.
    #[doc(hidden)]
    pub fn skip_closes_for_tests(&self, recall: bool, expiry: bool) {
        self.skip_close_on_recall.store(recall, Ordering::Relaxed);
        self.skip_close_on_expiry.store(expiry, Ordering::Relaxed);
    }

    /// Close every slot (the lease is released or lost, an epoch opened).
    pub fn close_all(&self) {
        for w in self.lock().slots.values_mut() {
            w.open = false;
        }
    }

    /// The core's own state says a renewal of `gen` would be granted again
    /// (a seal that was not answered went back to the phase before it).
    pub fn reopen(&self, gen: u64) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.open = w.eligible;
        }
    }

    /// The core grants a renewal (or counts the delegate's stream as a sign
    /// of life): the window is at least `until`. A `max`. The slot is open
    /// again: the core extends only where it would grant. `renewed`: the
    /// renewals' own end moves too.
    pub fn extend_core(&self, gen: u64, until: WaitMs, renewed: bool) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.until = w.until.max(until);
            if renewed {
                w.renewed_until = w.renewed_until.max(until);
            }
            w.open = w.eligible;
        }
    }

    /// The core holds a renewal back and outwaits for it: the window is at
    /// least `until` (a `max`), and the slot is left as it is. A closed
    /// slot (a recall, a seal, an epoch or a lost lease closed it, rule 2)
    /// stays closed: the core grants nothing from here, and the answerer
    /// must not either.
    pub fn extend_until(&self, gen: u64, until: WaitMs) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.until = w.until.max(until);
        }
    }

    /// The expiry decision, one critical section on the table's own clock:
    /// `Err(until)`: the window has not run out (it was extended since the
    /// timer was set, or since the step took its `now`): re-arm for `until`
    /// and decide nothing. `Ok`: it has, and the slot is closed in the same
    /// section — nothing but the core extends it from here, so what the core
    /// does next (a seal, a reclaim) acts on a window no one else can
    /// lengthen.
    pub fn close_if_expired(&self, gen: u64, fallback: Now) -> Result<(), WaitMs> {
        #[cfg(test)]
        {
            let hook = self.lock().before_close.take();
            if let Some(hook) = hook {
                hook(self);
            }
        }
        let mut g = self.lock();
        let now = Self::now_or(&g, fallback).wait;
        let Some(w) = g.slots.get_mut(&gen) else {
            return Ok(());
        };
        if now < w.until {
            return Err(w.until);
        }
        if !self.skip_close_on_expiry.load(Ordering::Relaxed) {
            w.open = false;
        }
        Ok(())
    }

    /// The seal's window (`now + 2 × forward_timeout`): only on a closed
    /// slot; `false` (nothing set) if it is open.
    pub fn set_until_closed(&self, gen: u64, until: WaitMs) -> bool {
        match self.lock().slots.get_mut(&gen) {
            Some(w) if !w.open => {
                w.until = until;
                true
            }
            _ => false,
        }
    }

    /// The core publishes what it knows after a step: the lease certificate
    /// (`None`: nothing may be answered) and the generations whose lock side
    /// is settled ([`Window::settled`]; one not named is not). Replaces the
    /// previous publication.
    pub fn publish(&self, lease: Option<RootLease>, settled: &[u64]) {
        let mut g = self.lock();
        g.lease = lease;
        for (gen, w) in g.slots.iter_mut() {
            w.settled = settled.contains(gen);
        }
    }

    /// A lock-side change within a step (a handoff queued, a floor raised, a
    /// barrier noted) leaves generation `gen` unsettled at once, not at the
    /// next publication: an answer without lock content between the change
    /// and the end of the step would leave it behind.
    pub fn unsettle(&self, gen: u64) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.settled = false;
        }
    }

    /// [`Self::unsettle`] for every generation (a barrier whose subtree is
    /// not known here).
    pub fn unsettle_all(&self) {
        for w in self.lock().slots.values_mut() {
            w.settled = false;
        }
    }

    /// Count a renewal the answerer declined before it reached the table.
    pub fn note_deferred(&self, why: Refusal) {
        self.stats.deferred[why.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// The certificate last published (tests, status).
    pub fn lease_cert(&self) -> Option<RootLease> {
        self.lock().lease
    }

    /// The off-core answerer's extension: one critical section that decides
    /// from the published certificate and the generation's slot at `now`
    /// ([`decide`]) and, if every rule holds, extends the window (a `max`,
    /// commuting with the core's own). `Ok(ttl_ms)`: the window is now at
    /// least `now.wait + ttl + extra` and the reply carries `ttl_ms`; the
    /// delegate honours from its own send, which is no later than `now`.
    pub fn extend_off_core(&self, gen: u64, from: NodeId, now: Now) -> Result<u64, Refusal> {
        let result = {
            let mut g = self.lock();
            let Inner {
                slots,
                lease,
                probe,
                ..
            } = &mut *g;
            match (lease.as_ref(), slots.get_mut(&gen)) {
                (None, _) => Err(Refusal::Lease),
                (_, None) => Err(Refusal::Generation),
                (Some(lease), Some(w)) => {
                    // (The probe's own lock is taken inside the table's, and
                    // never the other way round.)
                    let (deposed, proof) = probe.as_ref().map_or((false, None), |p| {
                        (p.deposed(lease.epoch), p.proof(lease.epoch))
                    });
                    if deposed {
                        Err(Refusal::Deposed)
                    } else {
                        decide(lease, w, from, now, proof).map(|(ttl, until)| {
                            w.until = w.until.max(until);
                            w.renewed_until = w.renewed_until.max(until);
                            ttl
                        })
                    }
                }
            }
        };
        match result {
            Ok(_) => self.stats.answered_off_core.fetch_add(1, Ordering::Relaxed),
            Err(r) => self.stats.deferred[r.index()].fetch_add(1, Ordering::Relaxed),
        };
        result
    }

    /// Set a generation's window outright (a core unit test that forces an
    /// expiry or a long window).
    #[cfg(test)]
    pub(crate) fn set_until_for_tests(&self, gen: u64, until: WaitMs) {
        if let Some(w) = self.lock().slots.get_mut(&gen) {
            w.until = until;
            w.renewed_until = until;
        }
    }

    /// Run `hook` once, just before the next [`Self::close_if_expired`]
    /// takes the guard (tests).
    #[cfg(test)]
    pub(crate) fn before_close_for_tests(&self, hook: Box<dyn FnOnce(&DelegWindows) + Send>) {
        self.lock().before_close = Some(hook);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn windows() -> Arc<DelegWindows> {
        let w = DelegWindows::new();
        w.insert(7, 2, WaitMs(10_000), WaitMs(10_000), true);
        w
    }

    #[test]
    fn extensions_are_a_max_and_commute() {
        let a = windows();
        a.extend_core(7, WaitMs(12_000), true);
        a.extend_core(7, WaitMs(11_000), true);
        let b = windows();
        b.extend_core(7, WaitMs(11_000), true);
        b.extend_core(7, WaitMs(12_000), true);
        assert_eq!(a.get(7), b.get(7));
        assert_eq!(a.until(7), Some(WaitMs(12_000)));
        // The stream's sign of life moves `until`, not the renewals' end.
        a.extend_core(7, WaitMs(15_000), false);
        assert_eq!(a.until(7), Some(WaitMs(15_000)));
        assert_eq!(a.renewed_until(7), Some(WaitMs(12_000)));
    }

    #[test]
    fn a_held_renewals_extension_leaves_a_closed_slot_closed() {
        let w = windows();
        w.extend_until(7, WaitMs(12_000));
        assert_eq!(w.until(7), Some(WaitMs(12_000)));
        assert!(w.get(7).unwrap().open);
        w.close(7);
        w.extend_until(7, WaitMs(11_000));
        w.extend_until(7, WaitMs(14_000));
        assert_eq!(w.until(7), Some(WaitMs(14_000)));
        assert!(!w.get(7).unwrap().open);
    }

    #[test]
    fn the_expiry_decision_reads_the_window_inside_the_guard() {
        let w = windows();
        // The step took its `now` (10 000: the window has run out), then
        // waited; an extension landed before the decision.
        w.before_close_for_tests(Box::new(|w| w.extend_core(7, WaitMs(13_000), true)));
        assert_eq!(
            w.close_if_expired(7, Now::same(10_000)),
            Err(WaitMs(13_000))
        );
        assert!(w.get(7).unwrap().open, "an unexpired window stays open");
        // Run out: closed in the same section.
        assert_eq!(w.close_if_expired(7, Now::same(13_000)), Ok(()));
        assert!(!w.get(7).unwrap().open);
    }

    #[test]
    fn the_clock_inside_the_guard_is_the_tables_own() {
        let w = windows();
        // The step's `now` is old (9 000: not run out); the table's clock
        // says the window has run out.
        w.set_clock(Arc::new(|| Now::same(10_200)));
        assert_eq!(w.close_if_expired(7, Now::same(9_000)), Ok(()));
        assert!(!w.get(7).unwrap().open);
    }

    /// The fail-before of R1: the sequence the core had before the window
    /// table (read `until`, decide, write the seal's window) acts on a
    /// stale read of a window that was extended meanwhile; the primitive
    /// does not.
    #[test]
    fn the_old_read_then_seal_sequence_loses_an_extension_the_primitive_keeps() {
        // Old: read, (an extension lands), decide on the read, write.
        let w = windows();
        let read = w.until(7).unwrap();
        w.extend_core(7, WaitMs(13_000), true);
        assert!(Now::same(10_500).wait >= read, "decided on the stale read");
        w.close(7);
        // The seal's window replaces the renewed one: shorter than what the
        // delegate honours from the extension.
        assert!(w.set_until_closed(7, WaitMs(11_000)));
        assert_eq!(
            w.until(7),
            Some(WaitMs(11_000)),
            "the renewed window was cut"
        );
        // New: decision and close are one section.
        let w = windows();
        w.before_close_for_tests(Box::new(|w| w.extend_core(7, WaitMs(13_000), true)));
        assert_eq!(
            w.close_if_expired(7, Now::same(10_500)),
            Err(WaitMs(13_000))
        );
        assert!(
            !w.set_until_closed(7, WaitMs(11_000)),
            "an open slot is never shortened"
        );
        assert_eq!(w.until(7), Some(WaitMs(13_000)));
    }

    #[test]
    fn a_closed_slot_is_reopened_only_by_the_core() {
        let w = windows();
        w.close(7);
        assert!(!w.get(7).unwrap().open);
        w.extend_core(7, WaitMs(11_000), true);
        assert!(
            w.get(7).unwrap().open,
            "the core extends only where it grants"
        );
        w.close_all();
        assert!(!w.get(7).unwrap().open);
        w.reopen(7);
        assert!(w.get(7).unwrap().open);
        // A designation is never opened.
        w.insert(8, 3, WaitMs(5), WaitMs(5), false);
        w.reopen(8);
        w.extend_core(8, WaitMs(9), true);
        assert!(!w.get(8).unwrap().open);
        w.clear();
        assert!(w.get(7).is_none());
    }

    fn lease() -> RootLease {
        RootLease {
            epoch: 3,
            honour_until: HonourMs(60_000),
            margin_ms: 1_000,
            delegation_ttl_ms: 5_000,
            extra_ms: 1_000,
            fast: false,
            last_s3_fresh: HonourMs(0),
            window_ms: 1_500,
            marked: true,
        }
    }

    fn window() -> Window {
        Window {
            node: 2,
            until: WaitMs(10_000),
            renewed_until: WaitMs(10_000),
            open: true,
            eligible: true,
            settled: true,
        }
    }

    #[test]
    fn the_answerer_grants_the_ttl_and_never_past_the_cap() {
        let now = Now::same(20_000);
        // Far from the lease's end: the full ttl, window to `now + ttl + extra`.
        assert_eq!(
            decide(&lease(), &window(), 2, now, None),
            Ok((5_000, WaitMs(26_000)))
        );
        // The lease ends at 24 s with a 1 s margin: the cap is 3 s.
        let mut l = lease();
        l.honour_until = HonourMs(24_000);
        assert_eq!(
            decide(&l, &window(), 2, now, None),
            Ok((3_000, WaitMs(24_000)))
        );
        // A grant never outlives the cap, by construction: the delegate
        // stops at `sent + ttl - margin`, before the lease's end less margin.
        for left in [1_001i64, 1_500, 4_999, 5_000, 6_000, 30_000] {
            let mut l = lease();
            l.honour_until = HonourMs(20_000 + left + 1_000);
            let (ttl, _) = decide(&l, &window(), 2, now, None).unwrap();
            assert!(ttl as i64 <= left, "ttl {ttl} past the cap {left}");
        }
        // Nothing left below the margin: no answer.
        l.honour_until = HonourMs(21_000);
        assert_eq!(decide(&l, &window(), 2, now, None), Err(Refusal::Cap));
        l.honour_until = HonourMs(20_500);
        assert_eq!(decide(&l, &window(), 2, now, None), Err(Refusal::Cap));
    }

    #[test]
    fn the_answerer_refuses_each_state_the_core_would_not_grant_in() {
        let now = Now::same(20_000);
        let l = lease();
        assert!(decide(&l, &window(), 2, now, None).is_ok());
        // Another node's renewal.
        assert_eq!(decide(&l, &window(), 9, now, None), Err(Refusal::Node));
        // A recall, seal or end closed the slot; a designation never opens.
        let mut w = window();
        w.open = false;
        assert_eq!(decide(&l, &w, 2, now, None), Err(Refusal::Closed));
        let mut w = window();
        w.eligible = false;
        assert_eq!(decide(&l, &w, 2, now, None), Err(Refusal::Closed));
        // The lock side has something to hand over.
        let mut w = window();
        w.settled = false;
        assert_eq!(decide(&l, &w, 2, now, None), Err(Refusal::Unsettled));
        // A tenure that may be taken over early needs a fresh proof of
        // liveness within the window, and a marked lease.
        let mut fast = lease();
        fast.fast = true;
        fast.last_s3_fresh = HonourMs(18_500);
        assert_eq!(decide(&fast, &window(), 2, now, None), Err(Refusal::Stale));
        fast.last_s3_fresh = HonourMs(18_501);
        assert!(decide(&fast, &window(), 2, now, None).is_ok());
        fast.marked = false;
        assert_eq!(
            decide(&fast, &window(), 2, now, None),
            Err(Refusal::Unmarked)
        );
        // (A `Local` tenure that cannot be taken over early needs neither
        // freshness nor a proof; the cap is its bound.)
        let mut local = lease();
        local.last_s3_fresh = HonourMs(0);
        assert!(decide(&local, &window(), 2, now, None).is_ok());
    }

    #[test]
    fn the_certificate_and_the_slot_are_one_critical_section() {
        let w = windows();
        let now = Now::same(20_000);
        // No certificate: nothing is answered.
        assert_eq!(w.extend_off_core(7, 2, now), Err(Refusal::Lease));
        w.publish(Some(lease()), &[7]);
        assert_eq!(w.extend_off_core(7, 2, now), Ok(5_000));
        assert_eq!(w.until(7), Some(WaitMs(26_000)));
        assert_eq!(w.extend_off_core(8, 2, now), Err(Refusal::Generation));
        // A certificate that revokes (a release began) stops it at once;
        // the window stays what it was.
        w.publish(None, &[7]);
        assert_eq!(w.extend_off_core(7, 2, now), Err(Refusal::Lease));
        assert_eq!(w.until(7), Some(WaitMs(26_000)));
        // A generation the core did not name as settled is not answered.
        w.publish(Some(lease()), &[]);
        assert_eq!(w.extend_off_core(7, 2, now), Err(Refusal::Unsettled));
        // The extension is a max: a shorter one later leaves it.
        w.publish(Some(lease()), &[7]);
        assert_eq!(w.extend_off_core(7, 2, Now::same(19_000)), Ok(5_000));
        assert_eq!(w.until(7), Some(WaitMs(26_000)));
        assert_eq!(
            w.stats().answered_off_core.load(Ordering::Relaxed),
            2,
            "two answers"
        );
        assert_eq!(
            w.stats().deferred[Refusal::Unsettled.index()].load(Ordering::Relaxed),
            1
        );
        w.clear();
        assert!(w.lease_cert().is_none());
    }

    /// Plan 41 D3: an off-core read that proved the tenure live lifts the
    /// refusal for stale liveness, dated at its send, for its epoch only,
    /// within the freshness window; and nothing else.
    #[test]
    fn a_proof_from_the_probe_lifts_only_the_stale_refusal() {
        let mut fast = lease();
        fast.fast = true;
        fast.last_s3_fresh = HonourMs(10_000);
        let now = Now::same(20_000);
        assert_eq!(decide(&fast, &window(), 2, now, None), Err(Refusal::Stale));
        // Sent 400 ms ago: fresh. At the window's edge: not (the rule is
        // the core's, unchanged).
        assert!(decide(&fast, &window(), 2, now, Some(HonourMs(18_600))).is_ok());
        assert_eq!(
            decide(&fast, &window(), 2, now, Some(HonourMs(18_500))),
            Err(Refusal::Stale)
        );
        // A proof older than the core's own changes nothing.
        fast.last_s3_fresh = HonourMs(19_900);
        assert!(decide(&fast, &window(), 2, now, Some(HonourMs(1_000))).is_ok());
        // Nothing else is lifted: an unmarked lease, a closed slot, a lease
        // with nothing left below the margin.
        let proof = Some(HonourMs(19_950));
        let mut l = fast;
        l.marked = false;
        assert_eq!(decide(&l, &window(), 2, now, proof), Err(Refusal::Unmarked));
        let mut w = window();
        w.open = false;
        assert_eq!(decide(&fast, &w, 2, now, proof), Err(Refusal::Closed));
        let mut l = fast;
        l.honour_until = HonourMs(20_500);
        assert_eq!(decide(&l, &window(), 2, now, proof), Err(Refusal::Cap));
    }

    /// Plan 41 D3: the table consults the probe inside its guard, for the
    /// certificate's epoch: a read that showed another holder ends the
    /// epoch's answers whatever the proof it replaced would have allowed.
    #[test]
    fn the_table_answers_on_the_probes_proof_and_stops_when_a_read_shows_a_successor() {
        use crate::root_probe::{RootProbe, Verdict, Want};
        let w = windows();
        let probe = RootProbe::new();
        w.set_probe(probe.clone());
        let mut fast = lease();
        fast.fast = true;
        fast.last_s3_fresh = HonourMs(0);
        w.publish(Some(fast), &[7]);
        let now = Now::same(20_000);
        assert_eq!(w.extend_off_core(7, 2, now), Err(Refusal::Stale));
        let Want::Start(t) = probe.want(now.honour, fast.epoch, 1_500, || Box::new(|| {})) else {
            panic!("a read is asked for");
        };
        let _ = probe.finish(t, Verdict::Live);
        assert_eq!(w.extend_off_core(7, 2, Now::same(20_400)), Ok(5_000));
        // The proof is for the epoch it was taken for.
        let mut next = fast;
        next.epoch += 1;
        w.publish(Some(next), &[7]);
        assert_eq!(
            w.extend_off_core(7, 2, Now::same(20_500)),
            Err(Refusal::Stale)
        );
        w.publish(Some(fast), &[7]);
        assert!(w.extend_off_core(7, 2, Now::same(20_500)).is_ok());
        // A later read shows the successor: the earlier proof is withdrawn
        // at once, though its window has not run out.
        let Want::Start(t2) = probe.want(HonourMs(21_600), fast.epoch, 1_500, || Box::new(|| {}))
        else {
            panic!("the next round reads again");
        };
        let _ = probe.finish(t2, Verdict::Deposed);
        assert_eq!(
            w.extend_off_core(7, 2, Now::same(21_700)),
            Err(Refusal::Deposed)
        );
        assert_eq!(
            w.stats().deferred[Refusal::Deposed.index()].load(Ordering::Relaxed),
            1
        );
        // Without the probe wired in, the table is the core's alone.
        let bare = windows();
        bare.publish(Some(fast), &[7]);
        assert_eq!(bare.extend_off_core(7, 2, now), Err(Refusal::Stale));
    }

    /// The fail-before of the recall's close: without it (the code before
    /// R1's rule 2) a renewal answered after the recall began extends the
    /// window the recall outwaits.
    #[test]
    fn a_closed_slot_is_not_extended_and_an_open_one_is() {
        let w = windows();
        w.publish(Some(lease()), &[7]);
        let now = Now::same(20_000);
        w.close_for_recall(7);
        assert_eq!(w.extend_off_core(7, 2, now), Err(Refusal::Closed));
        assert_eq!(w.until(7), Some(WaitMs(10_000)));
        // The knob: the recall's close skipped.
        let w = windows();
        w.skip_closes_for_tests(true, false);
        w.publish(Some(lease()), &[7]);
        w.close_for_recall(7);
        assert_eq!(
            w.extend_off_core(7, 2, now),
            Ok(5_000),
            "extended past the recall"
        );
    }
}
