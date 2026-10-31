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
//! One new read at most per *stagger* ([`PROBE_READS_PER_WINDOW`] per
//! freshness window, `backup_takeover_ms`): a renewal refused for staleness
//! joins the newest read in flight when that read left less than a stagger
//! ago, and starts the next one otherwise; and while renewals wait for a read
//! in flight, the next read follows a stagger after the last one started
//! ([`RootProbe::follow_up`]), whether or not another renewal arrives. So at
//! most that many reads are out at once (a read out for a whole window is
//! abandoned), and none is made but for a renewal that was refused for
//! staleness and waits (a window at most after it asked: a renewal whose read
//! proved nothing waits for the next one only if that one was sent within a
//! window of its ask). None once a read showed the epoch deposed; none unless
//! the core has been stuck for the lanes' own threshold and not past the hung
//! bound (the caller's gate, D5/D6, which every renewal that waits passed).
//!
//! *Why staggered.* A proof is good for the window from its read's send, so a
//! read is worth something only if it is back within the window, with what is
//! left of it. When the lease GET's round trip nears the window (a slow S3
//! against a 1.5 s takeover window), a read misses it often, and one read per
//! window (the rule before) left a delegate two or three attempts between its
//! first renewal and its grant's end: two misses in a row lapsed a grant that
//! the root still held (`delegated-core-stall-root-backup` under
//! `AUTHORITY_SIM_LEASE_READS=0:0:800:1000`, 249 of 2,000 seeds). Reads that
//! overlap give the round several independent chances, at most one GET per
//! stagger, and only while the core is stuck and a renewal waits.
//!
use crate::ids::{HonourMs, NodeId};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// How many reads a freshness window may start (see the module doc's
/// *Bounds*): a new read starts at most once per `window / this`.
pub const PROBE_READS_PER_WINDOW: i64 = 4;

/// The least time between two reads' sends for a freshness window of
/// `window_ms` ([`PROBE_READS_PER_WINDOW`]).
pub fn probe_stagger_ms(window_ms: i64) -> i64 {
    (window_ms / PROBE_READS_PER_WINDOW).max(1)
}

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
    /// A read was sent less than a stagger ago ([`probe_stagger_ms`]) and
    /// did not prove the tenure (it failed): the next read waits for the
    /// stagger.
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
    /// The reads in flight, oldest first (at most [`PROBE_READS_PER_WINDOW`]
    /// younger than a window).
    inflight: Vec<Ticket>,
    /// The renewals waiting for a read, by the read's ticket: a read that
    /// ends wakes its own, and if it proved the tenure live (or deposed)
    /// those of the older reads too (whose proofs could only be older); never
    /// those of a newer read, which would decide before that read is back (an
    /// abandoned read's late end, whose proof is stale by then). One that
    /// proved nothing passes its own to the next newer read still out, if
    /// that one was sent within a window of the renewal's ask (the second
    /// field): a renewal rides the reads of one window at most.
    waiters: Vec<(u64, HonourMs, Waiter)>,
    /// The send and epoch of the latest read started (the stagger gate).
    last_sent: Option<(u64, HonourMs)>,
    /// The latest epoch a read showed deposed.
    deposed: Option<u64>,
    /// The freshness window of the latest ask (`want`, `follow_up`): how
    /// far a renewal rides the reads after its own (`finish`).
    window_ms: i64,
    next: u64,
}

/// What the probe did (`status`).
#[derive(Debug, Default)]
pub struct ProbeStats {
    /// Reads started (follow-ups included).
    pub started: AtomicU64,
    /// Of them, follow-ups ([`RootProbe::follow_up`]).
    pub followed: AtomicU64,
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
    /// window (`backup_takeover_ms`): a read is started at most once per
    /// stagger of it ([`probe_stagger_ms`]), and a renewal joins the newest
    /// read in flight younger than that. `waiter` (built only when it is
    /// used) runs when the read ends, whatever it found.
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
        // A read out longer than the window it could still justify (a
        // request that hangs) is abandoned: its waiters have their own bound,
        // and its late end still counts (`finish`).
        g.inflight
            .retain(|t| t.epoch != epoch || now.since(t.sent) < window_ms);
        g.window_ms = window_ms;
        let stagger = probe_stagger_ms(window_ms);
        if let Some(t) = g.inflight.iter().rev().find(|t| t.epoch == epoch).copied() {
            if now.since(t.sent) < stagger {
                g.waiters.push((t.seq, now, waiter()));
                self.stats.joined.fetch_add(1, Ordering::Relaxed);
                return Want::Joined;
            }
        }
        if let Some((e, sent)) = g.last_sent {
            if e == epoch && now.since(sent) < stagger {
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
        g.inflight.push(ticket);
        g.last_sent = Some((epoch, now));
        g.waiters.push((ticket.seq, now, waiter()));
        self.stats.started.fetch_add(1, Ordering::Relaxed);
        Want::Start(ticket)
    }

    /// A stagger after a read of the tenure of `epoch` was started: the next
    /// read, if renewals still wait for a read in flight (none has proved the
    /// tenure live since they asked: a live end wakes the older reads'
    /// waiters too), asked within the window, and none was started within the
    /// stagger. Without a
    /// waiter of its own: renewals that arrive within its stagger join it, and
    /// its live end wakes the older reads' waiters. The caller schedules it
    /// after every read it starts, so reads go on at one per stagger while
    /// renewals wait, not only as fast as the delegates' renewals arrive; and
    /// stop a window after the last renewal that asked (its read ends by then
    /// and takes its waiters with it). The caller's gate is judged again with
    /// `stuck_ms`, how long the driver has been stuck now: none once the core
    /// runs again (below the lanes' least stall,
    /// [`crate::deleg_lane::DELEG_LANE_STUCK_MIN_MS`]) or past `hung_ms`.
    pub fn follow_up(
        &self,
        now: HonourMs,
        epoch: u64,
        window_ms: i64,
        stuck_ms: i64,
        hung_ms: i64,
    ) -> Option<Ticket> {
        if stuck_ms < crate::deleg_lane::DELEG_LANE_STUCK_MIN_MS || stuck_ms > hung_ms {
            return None;
        }
        let mut g = self.lock();
        if g.deposed == Some(epoch) {
            return None;
        }
        g.inflight
            .retain(|t| t.epoch != epoch || now.since(t.sent) < window_ms);
        g.window_ms = window_ms;
        let Inner {
            inflight, waiters, ..
        } = &*g;
        let waiting = waiters.iter().any(|(seq, asked, _)| {
            now.since(*asked) < window_ms
                && inflight.iter().any(|t| t.seq == *seq && t.epoch == epoch)
        });
        if !waiting {
            return None;
        }
        if g.last_sent
            .is_some_and(|(e, sent)| e == epoch && now.since(sent) < probe_stagger_ms(window_ms))
        {
            return None;
        }
        g.next += 1;
        let ticket = Ticket {
            seq: g.next,
            epoch,
            sent: now,
        };
        g.inflight.push(ticket);
        g.last_sent = Some((epoch, now));
        self.stats.started.fetch_add(1, Ordering::Relaxed);
        self.stats.followed.fetch_add(1, Ordering::Relaxed);
        Some(ticket)
    }

    /// The read for `ticket` ended with `verdict`. Returns the waiters to run
    /// (outside any lock of the caller's): that read's, and if it settled the
    /// question (live, or deposed) those of the older reads still out too;
    /// those of a newer read are not woken by it. A read that proved nothing
    /// while a newer read of the tenure is out hands its waiters to the next
    /// such read, those it may carry (asked within a window of that read's
    /// send). A late end of an abandoned read still records what it proved:
    /// the proof is dated at its own send.
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
        g.inflight.retain(|t| t.seq != ticket.seq);
        // A read that proved nothing hands its renewals to the next read of
        // the tenure still out, if a newer one is: they are decided when that
        // one ends, or a later one proves the tenure live first (and so on
        // while reads follow), not refused at once for a miss that a read
        // already on its way may make up for.
        let next = (verdict == Verdict::Unknown)
            .then(|| {
                g.inflight
                    .iter()
                    .filter(|t| t.epoch == ticket.epoch && t.seq > ticket.seq)
                    .min_by_key(|t| t.seq)
                    .copied()
            })
            .flatten();
        let window = g.window_ms;
        let settled = verdict != Verdict::Unknown;
        let mut woken = Vec::new();
        for (seq, asked, w) in std::mem::take(&mut g.waiters) {
            if seq == ticket.seq {
                match next {
                    Some(t) if t.sent.since(asked) < window => g.waiters.push((t.seq, asked, w)),
                    _ => woken.push(w),
                }
            } else if settled && seq < ticket.seq {
                woken.push(w);
            } else {
                g.waiters.push((seq, asked, w));
            }
        }
        woken
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    const W: i64 = 1_500;
    /// A driver stuck for a while, and the hung bound (`follow_up`'s gate).
    const STUCK: i64 = 2_000;
    const HUNG: i64 = 15_000;

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
    fn the_renewals_that_arrive_within_a_stagger_of_a_read_join_it() {
        let p = RootProbe::new();
        let (n, w) = counter();
        let t = start(&p, 10_000, 7, &w);
        assert_eq!(p.want(HonourMs(10_100), 7, W, &w), Want::Joined);
        assert_eq!(
            p.want(HonourMs(10_000 + probe_stagger_ms(W) - 1), 7, W, &w),
            Want::Joined
        );
        assert_eq!(p.stats().started.load(Ordering::Relaxed), 1);
        for run in p.finish(t, Verdict::Live) {
            run();
        }
        assert_eq!(n.load(Ordering::SeqCst), 3, "all three are told");
    }

    /// The fix of `delegated-core-stall-root-backup` under slow lease reads:
    /// a round whose reads may each miss the window (a GET's round trip near
    /// `backup_takeover_ms`) gets [`PROBE_READS_PER_WINDOW`] overlapping
    /// reads, one per stagger, not one. Fail-before: one read per window, the
    /// renewals of the round joined it or were refused (`Spaced`), and two
    /// misses in a row lapsed a grant.
    #[test]
    fn a_round_starts_one_read_per_stagger_and_they_overlap() {
        let p = RootProbe::new();
        let (_n, w) = counter();
        let s = probe_stagger_ms(W);
        let mut out = vec![start(&p, 10_000, 7, &w)];
        // A renewal every 100 ms, none back yet: one new read per stagger.
        for at in (10_100..10_000 + W).step_by(100) {
            match p.want(HonourMs(at), 7, W, &w) {
                Want::Start(t) => out.push(t),
                Want::Joined => {}
                other => panic!("at {at}: {other:?}"),
            }
        }
        assert_eq!(out.len() as i64, PROBE_READS_PER_WINDOW);
        for (i, t) in out.iter().enumerate() {
            assert!(t.sent.0 >= 10_000 + s * i as i64, "{t:?}");
        }
        // A read that failed leaves the next one to the stagger.
        let q = RootProbe::new();
        let t = start(&q, 10_000, 7, &w);
        let _ = q.finish(t, Verdict::Unknown);
        assert_eq!(
            q.want(HonourMs(10_000 + s - 1), 7, W, &w),
            Want::No(NoProbe::Spaced)
        );
        let t2 = start(&q, 10_000 + s, 7, &w);
        let _ = q.finish(t2, Verdict::Live);
        assert_eq!(q.proof(7), Some(HonourMs(10_000 + s)));
    }

    /// Overlapping reads: one that proves the tenure live wakes its own
    /// waiters and the older reads' (whose proofs could only be older), not
    /// the newer reads'; one that proves nothing hands its own to the next
    /// newer read still out, and wakes none.
    #[test]
    fn a_live_read_wakes_its_own_and_the_older_reads_waiters() {
        let p = RootProbe::new();
        let s = probe_stagger_ms(W);
        let (a_n, a_w) = counter();
        let (b_n, b_w) = counter();
        let (c_n, c_w) = counter();
        let (d_n, d_w) = counter();
        let a = start(&p, 10_000, 7, &a_w);
        let b = start(&p, 10_000 + s, 7, &b_w);
        let c = start(&p, 10_000 + 2 * s, 7, &c_w);
        let d = start(&p, 10_000 + 3 * s, 7, &d_w);
        assert!(p.finish(b, Verdict::Unknown).is_empty(), "handed to c");
        for run in p.finish(c, Verdict::Live) {
            run();
        }
        assert_eq!(a_n.load(Ordering::SeqCst), 1, "the older read's waiter");
        assert_eq!(b_n.load(Ordering::SeqCst), 1, "the one handed to it");
        assert_eq!(c_n.load(Ordering::SeqCst), 1);
        assert_eq!(d_n.load(Ordering::SeqCst), 0, "not the newer read's");
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + 2 * s)));
        // The older read's late end tells no one twice and keeps the newer
        // proof.
        assert!(p.finish(a, Verdict::Live).is_empty());
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + 2 * s)));
        for run in p.finish(d, Verdict::Live) {
            run();
        }
        assert_eq!(d_n.load(Ordering::SeqCst), 1);
        assert_eq!(
            (a_n.load(Ordering::SeqCst), b_n.load(Ordering::SeqCst)),
            (1, 1),
            "none twice"
        );
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + 3 * s)));
    }

    /// While a renewal waits for a read in flight, the next read follows a
    /// stagger after the last one started, whether or not another renewal
    /// arrives; the renewal rides it when its own read misses, and is told
    /// when it proves the tenure. Not within the stagger, not once nothing
    /// waits, not for a deposed epoch. Fail-before: reads came only as fast as
    /// renewals arrived (the lane's, a few hundred ms apart), each renewal
    /// was refused at its own read's miss, and four or five misses in a row
    /// lapsed a grant (`delegated-core-stall-root-backup` seeds 232, 1219,
    /// 1581 and 1762 under slow lease reads, with the stagger alone).
    #[test]
    fn a_waiting_renewal_has_a_read_follow_every_stagger() {
        let p = RootProbe::new();
        let s = probe_stagger_ms(W);
        let (n, w) = counter();
        let first = start(&p, 10_000, 7, &w);
        assert_eq!(
            p.follow_up(HonourMs(10_000 + s - 1), 7, W, STUCK, HUNG),
            None
        );
        let second = p
            .follow_up(HonourMs(10_000 + s), 7, W, STUCK, HUNG)
            .expect("a follow-up");
        assert_eq!(second.sent, HonourMs(10_000 + s));
        assert_eq!(p.stats().followed.load(Ordering::Relaxed), 1);
        assert_eq!(
            p.follow_up(HonourMs(10_000 + s + 1), 7, W, STUCK, HUNG),
            None,
            "spaced"
        );
        // The first misses: its renewal waits for the follow-up, which
        // proves the tenure and tells it.
        assert!(p.finish(first, Verdict::Unknown).is_empty());
        assert!(
            p.follow_up(HonourMs(10_000 + 2 * s), 7, W, STUCK, HUNG)
                .is_some(),
            "it still waits: the reads go on"
        );
        for run in p.finish(second, Verdict::Live) {
            run();
        }
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + s)));
        // Nothing waits for a read in flight any more: no follow-up.
        assert_eq!(
            p.follow_up(HonourMs(10_000 + 4 * s), 7, W, STUCK, HUNG),
            None
        );
        // A miss with no newer read out tells its renewal (refused).
        let r = RootProbe::new();
        let (m, w2) = counter();
        let t = start(&r, 10_000, 7, &w2);
        for run in r.finish(t, Verdict::Unknown) {
            run();
        }
        assert_eq!(m.load(Ordering::SeqCst), 1);
        assert_eq!(r.follow_up(HonourMs(10_000 + s), 7, W, STUCK, HUNG), None);
        // Reads that keep failing (S3 out of reach) carry a renewal for a
        // window from its ask, no longer: then the follow-ups stop.
        let f = RootProbe::new();
        let (k, w3) = counter();
        let mut t = start(&f, 10_000, 7, &w3);
        let mut at = 10_000;
        loop {
            at += s;
            let Some(next) = f.follow_up(HonourMs(at), 7, W, STUCK, HUNG) else {
                break;
            };
            for run in f.finish(t, Verdict::Unknown) {
                run();
            }
            t = next;
        }
        assert!(at <= 10_000 + W, "followed past a window: {at}");
        for run in f.finish(t, Verdict::Unknown) {
            run();
        }
        assert_eq!(k.load(Ordering::SeqCst), 1, "told once, refused");
        // Nor once the core runs again, or past the hung bound, while a
        // renewal still waits.
        let g = RootProbe::new();
        let (_gn, gw) = counter();
        let t = start(&g, 10_000, 7, &gw);
        assert_eq!(g.follow_up(HonourMs(10_000 + s), 7, W, 0, HUNG), None);
        assert_eq!(
            g.follow_up(HonourMs(10_000 + s), 7, W, HUNG + 1, HUNG),
            None
        );
        assert!(g
            .follow_up(HonourMs(10_000 + s), 7, W, STUCK, HUNG)
            .is_some());
        let _ = g.finish(t, Verdict::Unknown);
        // A deposed epoch has no follow-up either.
        let q = RootProbe::new();
        let t = start(&q, 10_000, 7, &w);
        let t2 = q
            .follow_up(HonourMs(10_000 + s), 7, W, STUCK, HUNG)
            .unwrap();
        let _ = q.finish(t2, Verdict::Deposed);
        assert_eq!(
            q.follow_up(HonourMs(10_000 + 2 * s), 7, W, STUCK, HUNG),
            None
        );
        let _ = q.finish(t, Verdict::Unknown);
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
    /// read started: whatever it found, it wakes no renewal waiting for the
    /// newer read, which would decide before that read is back (and decline
    /// for want of the proof it brings). Proving nothing, it hands its own to
    /// the newer read, those that asked within a window of that read's send
    /// (the others are told); proving the tenure live (a stale proof by now),
    /// it wakes its own. Fail-before: the probe took every waiter at any read's
    /// end.
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
        // The renewal that asked a whole window before the newer read was
        // sent is told (refused); the later one rides on.
        for run in p.finish(slow, Verdict::Unknown) {
            run();
        }
        assert_eq!(old_n.load(Ordering::SeqCst), 1);
        assert_eq!(
            new_n.load(Ordering::SeqCst),
            0,
            "the newer read's are not told"
        );
        assert_eq!(p.proof(7), None);
        // The newer read, back at last, tells the other three and records
        // its proof.
        for run in p.finish(fresh, Verdict::Live) {
            run();
        }
        assert_eq!(new_n.load(Ordering::SeqCst), 2);
        assert_eq!(old_n.load(Ordering::SeqCst), 2, "none twice");
        assert_eq!(p.proof(7), Some(HonourMs(10_000 + W)));
        // A late live end: its own alone.
        let q = RootProbe::new();
        let (o, ow) = counter();
        let (f, fw) = counter();
        let slow = start(&q, 10_000, 7, &ow);
        let fresh = start(&q, 10_000 + W, 7, &fw);
        for run in q.finish(slow, Verdict::Live) {
            run();
        }
        assert_eq!((o.load(Ordering::SeqCst), f.load(Ordering::SeqCst)), (1, 0));
        let _ = q.finish(fresh, Verdict::Unknown);
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
