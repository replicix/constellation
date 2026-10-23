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
//! This is R1: the table and the core's use of it, no behaviour change.

use crate::ids::{NodeId, Now, WaitMs};
use std::collections::HashMap;
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
}

/// A test hook run once before the expiry decision takes the guard.
#[cfg(test)]
type CloseHook = Box<dyn FnOnce(&DelegWindows) + Send>;

#[derive(Default)]
struct Inner {
    slots: HashMap<u64, Window>,
    clock: Option<WindowsClock>,
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
            },
        );
    }

    /// The lease is gone: no generation is this node's. Every slot goes.
    pub fn clear(&self) {
        self.lock().slots.clear();
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
        w.open = false;
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
}
