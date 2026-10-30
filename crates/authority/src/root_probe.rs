//! Plan 41 D3: the root's off-core confirmation that it still holds its lease.
//!
//! A tenure that can be taken over before its lease expires (it lists a
//! backup, or acks at `ack=s3`) answers lock renewals and delegation renewals
//! only within `backup_takeover_ms` of the send of an S3 request that proved
//! it live and did not reveal a deposition (`Core::renew_fresh`, plan 41 L0).
//! The core makes such a request often enough while it runs; a core stuck in
//! one step (a priority store write behind a slow journal write) makes none,
//! and the off-core lanes (the delegation answerer of R2, the lock lane's
//! own renewals) found their proof stale after `backup_takeover_ms` and
//! answered nothing more for the rest of the stall: a root with a backup got
//! nothing from them (`KNOWN-GAP root core stuck`).
//!
//! This module is the lane's own proof. While the core is stuck (and not
//! hung), a lane that is refused only for stale liveness asks [`RootProbe`]
//! for one read of the lease object; the driver performs it off the core
//! (the engine's P2P service, the simulation's bus tap) and hands back what it
//! read. A read that names this node and the certificate's epoch, not
//! released, is a proof of liveness **as of its send** ([`Ticket::sent`], the
//! honour reading taken before the request left), exactly what
//! `note_s3_liveness` records for a `LeaseGet` the core made itself: the rule
//! it feeds is the unchanged [`crate::core::liveness_fresh`].
//!
//! # Why a read is a proof, and what it is not
//!
//! A successor takes this root over only by a lease CAS (a TTL claim, a
//! backup's seal, an `ack=s3` takeover), and quarantines what the root may
//! have answered from its marker on: `marker + backup_takeover_ms + ...`
//! (`Core::note_marker_landed`). A read served at `s' >= s` that still shows
//! this node at the certificate's epoch precedes that CAS, so the marker is
//! after `s`, and an answer decided at `t < s + backup_takeover_ms` ends no
//! later than the quarantine does: the argument of L0, unchanged, with the
//! read's send standing where the core's request's send stood.
//!
//! - *Latency.* The proof is dated at the **send**, never at the answer: a
//!   read that took longer than `backup_takeover_ms` proves nothing for the
//!   decision it was meant to justify, and the lane refuses ([`liveness_fresh`]
//!   judges `at - sent`, with `at` the decision's own reading).
//! - *A takeover racing the read.* A CAS that lands before the read is served
//!   shows in it (another holder, or another epoch): [`Verdict::Deposed`], no
//!   proof, and every later answer of the epoch is refused ([`RootProbe::deposed`]),
//!   whatever an earlier proof would have allowed. One that lands after it is
//!   covered by the quarantine above.
//! - *Failed, slow or unreadable.* No proof; nothing is granted, refused or
//!   dropped (the lane declines, and the core answers in full when it runs).
//! - *Another tenure.* A proof names the epoch it was taken for and counts
//!   only for a certificate of that epoch.
//! - *The certificate.* A proof is an input to the freshness rule and nothing
//!   else: a certificate the core withdrew (a release, an epoch, a gate) is
//!   not revived by it, and the marked-tenure, cap and settled rules are
//!   untouched.
//! - *Judged as the core judges.* The read is compared on holder and epoch
//!   only; no expiry stamp is compared with a wall clock (plan 43 C7: the
//!   lease is judged by observation, `released` is the only body bit that
//!   ends a tenure here).
//!
//! # Bounds
//!
//! One read in flight; one new read at most per freshness window
//! (`backup_takeover_ms`, the *round*: a read serves every renewal of every
//! lane that arrives inside its window, and none is made without a renewal
//! that was refused for staleness); none once a read showed the epoch
//! deposed; none unless the core has been stuck for the lanes' own
//! threshold and not past the hung bound (the caller's gate, D5/D6).

use crate::ids::{HonourMs, NodeId};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// What to run when a read ends (whatever its outcome): the lane that asked
/// retries its decision then.
pub type Waiter = Box<dyn FnOnce() + Send>;

/// What a read of the lease object returned, as far as the lanes judge it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseRead {
    /// The request failed, timed out or the body did not parse.
    Failed,
    /// No lease object.
    Absent,
    /// The object as read.
    Held {
        holder: NodeId,
        epoch: u64,
        released: bool,
    },
}

impl LeaseRead {
    /// The read of a lease object that parsed.
    pub fn of(lease: &constellation_store_s3::Lease) -> LeaseRead {
        LeaseRead::Held {
            holder: lease.holder,
            epoch: lease.epoch,
            released: lease.released,
        }
    }
}

/// What a read says about the tenure of `epoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// This node, at that epoch, not released: live as of the read's send.
    Live,
    /// Another holder, another epoch at or above it, or no lease: the tenure
    /// is over (the core finds out when it runs, `Event::RootProbed`).
    Deposed,
    /// Proves nothing either way: a failed read, a body older than the
    /// tenure, a release (which the core made and knows).
    Unknown,
}

/// Judge `read` for the tenure of `me` at `epoch` the way the core judges a
/// lease read (`Core::note_s3_liveness`, `Core::on_renew_probe`): holder and
/// epoch, nothing compared with a clock.
pub fn judge(read: LeaseRead, me: NodeId, epoch: u64) -> Verdict {
    match read {
        LeaseRead::Failed => Verdict::Unknown,
        LeaseRead::Absent => Verdict::Deposed,
        // (An epoch below this tenure's is a read older than the
        // acquisition that made it: it proves nothing either way.)
        LeaseRead::Held { epoch: e, .. } if e < epoch => Verdict::Unknown,
        LeaseRead::Held { released: true, .. } => Verdict::Unknown,
        LeaseRead::Held {
            holder, epoch: e, ..
        } if holder == me && e == epoch => Verdict::Live,
        LeaseRead::Held { .. } => Verdict::Deposed,
    }
}

/// A read the caller must perform and report ([`RootProbe::finish`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ticket {
    seq: u64,
    /// The tenure the read is for.
    pub epoch: u64,
    /// The honour reading taken before the request leaves: the proof, if
    /// the read succeeds, is dated here.
    pub sent: HonourMs,
}

impl Ticket {
    /// The same read, dated `sent`: the proof taken at the answer instead of
    /// the send. The simulation's non-vacuity switch for the dating
    /// (`ProbeMutant::DatedAtReturn`) and the unit tests'; the daemon never
    /// calls it.
    #[doc(hidden)]
    pub fn dated(self, sent: HonourMs) -> Ticket {
        Ticket { sent, ..self }
    }
}

/// Why no read was started for a renewal that was refused for stale liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoProbe {
    /// A read showed the epoch deposed.
    Deposed,
    /// A read was sent less than a freshness window ago and did not prove
    /// the tenure (it failed, or is not back): the round has had its read.
    Spaced,
}

/// The answer of [`RootProbe::want`].
#[derive(Debug, PartialEq, Eq)]
pub enum Want {
    /// Perform the read for this ticket, then [`RootProbe::finish`] it. The
    /// waiter registered is run when it ends.
    Start(Ticket),
    /// A read is in flight; the waiter registered is run when it ends.
    Joined,
    /// No read; the waiter was dropped unused.
    No(NoProbe),
}

#[derive(Default)]
struct Inner {
    /// The latest read that showed a tenure live: its epoch and send.
    proof: Option<(u64, HonourMs)>,
    /// The read in flight.
    inflight: Option<Ticket>,
    /// The renewals waiting for a read, by the read's ticket: a read that
    /// ends wakes its own and no others (an abandoned read's late end must not
    /// wake the waiters of the newer read, which would decide before it is
    /// back).
    waiters: Vec<(u64, Waiter)>,
    /// The send and epoch of the latest read started (the round gate).
    last_sent: Option<(u64, HonourMs)>,
    /// The latest epoch a read showed deposed.
    deposed: Option<u64>,
    next: u64,
}

/// What the probe did (`status`).
#[derive(Debug, Default)]
pub struct ProbeStats {
    /// Reads started.
    pub started: AtomicU64,
    /// Renewals that joined a read in flight.
    pub joined: AtomicU64,
    /// Renewals refused a read: spaced or deposed.
    pub withheld: AtomicU64,
    /// Reads by verdict.
    pub live: AtomicU64,
    pub deposed: AtomicU64,
    pub unknown: AtomicU64,
}

/// The lanes' shared proof of liveness (one per node). See the module doc.
#[derive(Default)]
pub struct RootProbe {
    inner: Mutex<Inner>,
    stats: ProbeStats,
}

impl std::fmt::Debug for RootProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.lock();
        f.debug_struct("RootProbe")
            .field("proof", &g.proof)
            .field("inflight", &g.inflight)
            .field("deposed", &g.deposed)
            .finish_non_exhaustive()
    }
}

impl RootProbe {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn stats(&self) -> &ProbeStats {
        &self.stats
    }

    /// The send of the latest read that proved the tenure of `epoch` live
    /// (honour clock), if it did: what the lanes' freshness rule takes
    /// besides the core's own `last_s3_fresh`.
    pub fn proof(&self, epoch: u64) -> Option<HonourMs> {
        self.lock()
            .proof
            .filter(|(e, _)| *e == epoch)
            .map(|(_, sent)| sent)
    }

    /// Whether a read showed the tenure of `epoch` deposed: the lanes answer
    /// nothing more for it.
    pub fn deposed(&self, epoch: u64) -> bool {
        self.lock().deposed == Some(epoch)
    }

    /// A renewal of the tenure of `epoch` was refused for stale liveness at
    /// `now` (honour clock): ask for a read. `window_ms` is the freshness
    /// window (`backup_takeover_ms`), also the round: at most one read is
    /// started per window. `waiter` (built only when it is used) runs when the
    /// read ends, whatever it found.
    pub fn want(
        &self,
        now: HonourMs,
        epoch: u64,
        window_ms: i64,
        waiter: impl FnOnce() -> Waiter,
    ) -> Want {
        let mut g = self.lock();
        if g.deposed == Some(epoch) {
            self.stats.withheld.fetch_add(1, Ordering::Relaxed);
            return Want::No(NoProbe::Deposed);
        }
        if let Some(t) = g.inflight {
            if t.epoch == epoch && now.since(t.sent) < window_ms {
                g.waiters.push((t.seq, waiter()));
                self.stats.joined.fetch_add(1, Ordering::Relaxed);
                return Want::Joined;
            }
            // A read that has been out longer than the window it could still
            // justify (a request that hangs): abandoned. Its waiters have
            // their own bound; its late end still counts (`finish`).
            g.inflight = None;
        }
        if let Some((e, sent)) = g.last_sent {
            if e == epoch && now.since(sent) < window_ms {
                self.stats.withheld.fetch_add(1, Ordering::Relaxed);
                return Want::No(NoProbe::Spaced);
            }
        }
        g.next += 1;
        let ticket = Ticket {
            seq: g.next,
            epoch,
            sent: now,
        };
        g.inflight = Some(ticket);
        g.last_sent = Some((epoch, now));
        g.waiters.push((ticket.seq, waiter()));
        self.stats.started.fetch_add(1, Ordering::Relaxed);
        Want::Start(ticket)
    }

    /// The read for `ticket` ended with `verdict`. Returns the waiters of
    /// that read to run (outside any lock of the caller's): those of a newer
    /// read are not woken by it. A late end of an abandoned read still records
    /// what it proved: the proof is dated at its own send.
    #[must_use]
    pub fn finish(&self, ticket: Ticket, verdict: Verdict) -> Vec<Waiter> {
        let mut g = self.lock();
        match verdict {
            Verdict::Live => {
                self.stats.live.fetch_add(1, Ordering::Relaxed);
                let newer = g
                    .proof
                    .is_none_or(|(e, s)| e != ticket.epoch || ticket.sent > s);
                if newer {
                    g.proof = Some((ticket.epoch, ticket.sent));
                }
            }
            Verdict::Deposed => {
                self.stats.deposed.fetch_add(1, Ordering::Relaxed);
                g.deposed = Some(ticket.epoch);
                if g.proof.is_some_and(|(e, _)| e == ticket.epoch) {
                    g.proof = None;
                }
            }
            Verdict::Unknown => {
                self.stats.unknown.fetch_add(1, Ordering::Relaxed);
            }
        }
        if g.inflight.is_some_and(|t| t.seq == ticket.seq) {
            g.inflight = None;
        }
        let (mine, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut g.waiters)
            .into_iter()
            .partition(|(seq, _)| *seq == ticket.seq);
        g.waiters = rest;
        mine.into_iter().map(|(_, w)| w).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    const W: i64 = 1_500;

    fn counter() -> (Arc<AtomicUsize>, impl Fn() -> Waiter) {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        (n, move || {
            let n = n2.clone();
            Box::new(move || {
                n.fetch_add(1, Ordering::SeqCst);
            }) as Waiter
        })
    }

    fn start(p: &RootProbe, now: i64, epoch: u64, w: impl FnOnce() -> Waiter) -> Ticket {
        match p.want(HonourMs(now), epoch, W, w) {
            Want::Start(t) => t,
            other => panic!("expected a read at {now}: {other:?}"),
        }
    }

    #[test]
    fn a_read_is_judged_on_holder_and_epoch_only() {
        let held = |holder, epoch, released| LeaseRead::Held {
            holder,
            epoch,
            released,
        };
        assert_eq!(judge(held(3, 7, false), 3, 7), Verdict::Live);
        // Another holder, at the epoch or above it: deposed.
        assert_eq!(judge(held(4, 7, false), 3, 7), Verdict::Deposed);
        assert_eq!(judge(held(4, 8, false), 3, 7), Verdict::Deposed);
        // The same node at a later epoch is a later tenure, not this one.
        assert_eq!(judge(held(3, 8, false), 3, 7), Verdict::Deposed);
        // An older body proves nothing either way.
        assert_eq!(judge(held(4, 6, false), 3, 7), Verdict::Unknown);
        assert_eq!(judge(held(3, 6, false), 3, 7), Verdict::Unknown);
        // A released lease is a tenure that ended: nothing is renewed on it.
        assert_eq!(judge(held(3, 7, true), 3, 7), Verdict::Unknown);
        assert_eq!(judge(LeaseRead::Absent, 3, 7), Verdict::Deposed);
        assert_eq!(judge(LeaseRead::Failed, 3, 7), Verdict::Unknown);
    }

    #[test]
    fn a_live_read_is_a_proof_dated_at_its_send_for_its_epoch() {
        let p = RootProbe::new();
        let (n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        assert_eq!(t.sent, HonourMs(10_000));
        assert_eq!(p.proof(7), None, "nothing proved before the read is back");
        for run in p.finish(t, Verdict::Live) {
            run();
        }
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert_eq!(p.proof(7), Some(HonourMs(10_000)));
        assert_eq!(p.proof(8), None, "another tenure's epoch");
        assert_eq!(p.proof(6), None);
    }

    #[test]
    fn one_read_in_flight_and_the_renewals_that_arrive_meanwhile_join_it() {
        let p = RootProbe::new();
        let (n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        assert_eq!(p.want(HonourMs(10_100), 7, W, &w), Want::Joined);
        assert_eq!(p.want(HonourMs(10_400), 7, W, &w), Want::Joined);
        assert_eq!(p.stats().started.load(Ordering::Relaxed), 1);
        for run in p.finish(t, Verdict::Live) {
            run();
        }
        assert_eq!(n.load(Ordering::SeqCst), 3, "all three are told");
    }

    #[test]
    fn at_most_one_read_per_window_whatever_the_last_one_found() {
        let p = RootProbe::new();
        let (_n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        let _ = p.finish(t, Verdict::Unknown);
        // The round has had its read.
        assert_eq!(
            p.want(HonourMs(10_000 + W - 1), 7, W, &w),
            Want::No(NoProbe::Spaced)
        );
        // The next round may ask again.
        let t2 = start(&p, 10_000 + W, 7, &w);
        let _ = p.finish(t2, Verdict::Live);
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + W)));
    }

    #[test]
    fn a_read_that_hangs_is_abandoned_after_its_window_and_its_late_proof_still_counts() {
        let p = RootProbe::new();
        let (_n, w) = counter();
        let slow = start(&p, 10_000, 7, &w);
        // Past the window the read could justify: a new one may start.
        let fresh = start(&p, 10_000 + W, 7, &w);
        let _ = p.finish(fresh, Verdict::Live);
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + W)));
        // The slow one ends at last: its proof is dated at its own, earlier
        // send and never shortens the newer.
        let _ = p.finish(slow, Verdict::Live);
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + W)));
    }

    /// A read abandoned for hanging past its window ends late, after a newer
    /// read started: it wakes the renewals that waited for it, and not those
    /// waiting for the newer read, which would decide before that read is
    /// back (and decline for want of the proof it brings). Fail-before: the
    /// probe took every waiter at any read's end.
    #[test]
    fn a_late_abandoned_read_wakes_only_its_own_waiters() {
        let p = RootProbe::new();
        let (old_n, old_w) = counter();
        let (new_n, new_w) = counter();
        let slow = start(&p, 10_000, 7, &old_w);
        assert_eq!(p.want(HonourMs(10_200), 7, W, &old_w), Want::Joined);
        // Past the window the slow read could justify: a new read starts, and
        // a renewal joins it.
        let fresh = start(&p, 10_000 + W, 7, &new_w);
        assert_eq!(
            p.want(HonourMs(10_000 + W + 100), 7, W, &new_w),
            Want::Joined
        );
        for run in p.finish(slow, Verdict::Unknown) {
            run();
        }
        assert_eq!(old_n.load(Ordering::SeqCst), 2, "its own two are told");
        assert_eq!(new_n.load(Ordering::SeqCst), 0, "the newer read's are not");
        assert_eq!(p.proof(7), None);
        // The newer read, back at last, tells its own and records its proof.
        for run in p.finish(fresh, Verdict::Live) {
            run();
        }
        assert_eq!(new_n.load(Ordering::SeqCst), 2);
        assert_eq!(old_n.load(Ordering::SeqCst), 2, "none twice");
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + W)));
    }

    #[test]
    fn a_read_that_shows_a_successor_ends_the_epochs_answers_and_its_reads() {
        let p = RootProbe::new();
        let (_n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        let _ = p.finish(t, Verdict::Live);
        let t2 = start(&p, 10_000 + W, 7, &w);
        let _ = p.finish(t2, Verdict::Deposed);
        assert!(p.deposed(7));
        assert_eq!(p.proof(7), None, "an earlier proof is withdrawn at once");
        assert_eq!(
            p.want(HonourMs(20_000), 7, W, &w),
            Want::No(NoProbe::Deposed)
        );
        // A later tenure is another epoch.
        assert!(!p.deposed(8));
        let t3 = start(&p, 20_000, 8, &w);
        let _ = p.finish(t3, Verdict::Live);
        assert_eq!(p.proof(8), Some(HonourMs(20_000)));
    }

    #[test]
    fn a_read_for_another_epoch_is_not_joined_and_a_live_one_does_not_shorten_a_proof() {
        let p = RootProbe::new();
        let (_n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        let _ = p.finish(t, Verdict::Live);
        let older = Ticket {
            seq: 99,
            epoch: 7,
            sent: HonourMs(9_000),
        };
        let _ = p.finish(older, Verdict::Live);
        assert_eq!(p.proof(7), Some(HonourMs(10_000)));
        let t8 = start(&p, 10_100, 8, &w);
        assert_eq!(t8.epoch, 8);
    }
}
