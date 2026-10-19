//! Which delegation renewals the off-core lane sends (sans-IO).
//!
//! A delegate's renewal is a step of its authority core: the `DelegRenew`
//! timer, then a send. A core stuck in one step for longer than the
//! renewal's slack — a priority metadata write waiting for the store's
//! writer lock behind a FUSE thread's slow journal write, a blocking log
//! write — sends nothing, and the root reclaims a delegation whose node is
//! alive, not dead (`docs/reference/features/delegations.md`, "Grants,
//! renewal and expiry"). While the driver is stuck, a thread beside it (the
//! holder-heartbeat thread, `authority_driver::holder_alive_loop`) sends the
//! renewals the core would have, from the certificates the core publishes
//! after every step ([`DelegRenewCert`]), and registers each send with the
//! core first ([`crate::Event::DelegRenewSentOffCore`]). The lane only sends:
//! the core counts the answer as a late answer, honoured from the lane's
//! send, and decides everything else
//! ([`crate::Core::on_deleg_renew_sent_off_core`] has the integrity argument).
//! This module decides *which* renewals are due.
//!
//! The lane's renewal is its own message, [`crate::PeerMsg::DelegRenewOffCore`]:
//! it carries neither a backup nor a stream head, and the root extends the
//! grant and learns nothing about the delegate's stream or backup from it.

use crate::core::DelegRenewCert;
use crate::ids::Ms;
use std::collections::HashMap;

/// Request ids of the lane's renewals carry this bit; the core's are
/// counted from 1, so the two never collide.
pub const LANE_REQ_BIT: u64 = 1 << 62;

/// The driver must have been stuck this long before the lane acts on a
/// generation whose renewal period is long enough: a core that is merely
/// busy sends its own renewals. A shorter period (the root capped the
/// grant, so the core renews at half of what it was given) shortens it, see
/// [`stuck_threshold_ms`].
pub const DELEG_LANE_STUCK_MS: i64 = 500;

/// The least a driver is stuck before the lane acts at all: the heartbeat
/// thread ticks every few hundred ms, and an ordinary step is shorter.
pub const DELEG_LANE_STUCK_MIN_MS: i64 = 100;

/// How long the driver must have been stuck before the lane renews a
/// generation renewed every `period_ms`: [`DELEG_LANE_STUCK_MS`], or a
/// quarter of the period if that is less (not below
/// [`DELEG_LANE_STUCK_MIN_MS`]). The core would have renewed by one period
/// after the last renewal; the lane steps in after a quarter of one, so a
/// short grant is not given up on for most of its length first.
pub fn stuck_threshold_ms(period_ms: u64) -> i64 {
    ((period_ms / 4) as i64).clamp(DELEG_LANE_STUCK_MIN_MS, DELEG_LANE_STUCK_MS)
}

/// The shortest period the lane follows a capped grant down to: the
/// granularity of the heartbeat tick and of a freeze, and a bound on the
/// renewals one stall can cost the root (a stall is at most the hung bound,
/// 15 s: about 60 per generation).
pub const DELEG_LANE_PERIOD_MIN_MS: i64 = 250;

/// The lane's period for a generation whose latest known grant (an answer
/// that reached the node's P2P service, whoever it was to: the core's request
/// or the lane's) was `granted_ttl_ms` long: the certificate's, or a quarter
/// of the grant if that is shorter, not below [`DELEG_LANE_PERIOD_MIN_MS`]. The
/// root caps a grant by what is left of its own lease, and the core renews at
/// half of the grant it was given; the lane does not see the core's
/// bookkeeping, only the answers, and a quarter (it cannot know whether lock
/// grants are out, which make the core renew at a quarter) keeps it inside a
/// capped grant. `next_due` follows: it is a period after the same send.
pub fn with_granted_ttl(c: &DelegRenewCert, granted_ttl_ms: u64) -> DelegRenewCert {
    let period = c
        .period_ms
        .min((granted_ttl_ms / 4).max(DELEG_LANE_PERIOD_MIN_MS as u64));
    let mut c = c.clone();
    c.next_due = Ms(c.next_due.0 - c.period_ms as i64 + period as i64);
    c.period_ms = period;
    c
}

/// A stretch of this process's own time in which the heartbeat thread, the
/// lane's thread (which logs nothing and does no I/O), did not run: the whole
/// process was frozen (stopped, starved of CPU, every thread blocked in the
/// kernel), and nothing in it could renew a grant, the lane included.
/// Shorter ones say nothing about a grant (one period is seconds) and are not
/// handed to the core.
pub const FREEZE_MIN_MS: i64 = 250;

/// How soon after a renewal fell due a freeze may have begun and still be
/// what lost the grant: the lane reacts after [`DELEG_LANE_STUCK_MS`] at
/// most, on a tick of the heartbeat (300 ms), and the answer comes back over
/// a round trip. A freeze that began later left the lane time to renew.
pub const FREEZE_REACTION_MS: i64 = 1_000;

/// A grant is lost at the root's reclaim, `granted + ttl + margin`; a freeze
/// that ended this long before the delegate's lapse point left a round trip
/// to renew in.
pub const FREEZE_RTT_MS: i64 = 100;

/// The freezes of the heartbeat thread that the core should hear of, as
/// `(from, until)` in unix ms: the last one the thread saw end (`done`:
/// [`pack_freeze`], 0 for none), and the one in progress, which the thread
/// cannot report before it runs again (its next tick was due at `beat_due`
/// and `now` is past it by [`FREEZE_MIN_MS`]: read by the driver as it wakes,
/// before the thread has run). Oldest first.
pub fn freezes(done: u64, beat_due: i64, now: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let (from, until) = unpack_freeze(done);
    if until - from >= FREEZE_MIN_MS {
        out.push((from, until));
    }
    if beat_due > 0 && now - beat_due >= FREEZE_MIN_MS {
        out.push((beat_due, now));
    }
    out
}

/// The bits of [`pack_freeze`]'s length (about 70 minutes, saturating).
const FREEZE_LEN_BITS: u32 = 22;
/// The most unix ms [`pack_freeze`] holds: 42 bits, to the year 2109 (a
/// later start saturates to this, a freeze that is then misplaced, not a
/// corrupted neighbour).
const FREEZE_FROM_MAX: i64 = (1 << (64 - FREEZE_LEN_BITS)) - 1;

/// `(from, length)` in one word, for one atomic: no reader sees a length with
/// another freeze's start. 42 bits of unix ms, 22 of length.
pub fn pack_freeze(from_ms: i64, len_ms: i64) -> u64 {
    ((from_ms.clamp(0, FREEZE_FROM_MAX) as u64) << FREEZE_LEN_BITS)
        | (len_ms.clamp(0, (1 << FREEZE_LEN_BITS) - 1) as u64)
}

/// The `(from, until)` of [`pack_freeze`] (`(0, 0)` for 0).
pub fn unpack_freeze(packed: u64) -> (i64, i64) {
    let from = (packed >> FREEZE_LEN_BITS) as i64;
    (from, from + (packed & ((1 << FREEZE_LEN_BITS) - 1)) as i64)
}

/// Whether a freeze of the heartbeat thread `(from, until)` was the whole
/// process's: the shared runtime's own lag probe (`Event::OwnLag`'s source:
/// a task of the daemon's runtime that ticks every 50 ms) missed the same
/// interval, `runtime` being its latest lapse `(from, until)` (`(0, 0)`:
/// none), to within [`FREEZE_CORROBORATION_MS`] at each end. A heartbeat
/// thread held up alone (blocked in a log line, a mutex) while the runtime
/// ran on is not a freeze of the process: the lane's thread failed, or the
/// log writer did, and the loss is not excused.
pub fn whole_process(freeze: (i64, i64), runtime: (i64, i64)) -> bool {
    runtime.0 > 0
        && runtime.0 <= freeze.0 + FREEZE_CORROBORATION_MS
        && runtime.1 >= freeze.1 - FREEZE_CORROBORATION_MS
}

/// The slack at each end of [`whole_process`]: the probe's tick (50 ms) and
/// the heartbeat's own tick (300 ms) are not aligned.
pub const FREEZE_CORROBORATION_MS: i64 = 500;

/// The length of the freeze, if any, that alone explains a grant lost at
/// `lapse_point` whose renewal fell due at `due`: one that began by
/// `due + FREEZE_REACTION_MS` and ended no earlier than `lapse_point -
/// FREEZE_RTT_MS`, so that no renewal could be sent and answered in
/// between. A freeze far from the loss, one that began after the lane had
/// its chance, and one that ended in time to renew in explain nothing.
pub fn freeze_explains(freezes: &[(Ms, Ms)], due: Ms, lapse_point: Ms) -> Option<i64> {
    freezes
        .iter()
        .filter(|(from, until)| {
            from.0 <= due.0 + FREEZE_REACTION_MS && until.0 >= lapse_point.0 - FREEZE_RTT_MS
        })
        .map(|(from, until)| until.since(*from))
        .max()
}

/// The certificates due at `now`, with the driver stuck for `stuck_ms`.
///
/// Nothing unless the driver has been stuck for [`stuck_threshold_ms`] of
/// the certificate's period and no longer than `hung_ms`: a driver that is
/// not stuck renews itself, and one stuck past `hung_ms` (the holder
/// heartbeat's bound, `CONSTELLATION_HOLDER_STALL_MS`) is hung, not slow —
/// the lane stops, and the root reclaims the delegation `ttl + margin`
/// after the last renewal, as it always did. A certificate is due once a
/// period has passed since the latest renewal known to have gone out: the
/// core's answered one (`next_due`), and the latest the core dispatched or
/// the lane sent for the same generation and round (`sent`).
pub fn due(
    certs: &[DelegRenewCert],
    now: Ms,
    stuck_ms: i64,
    hung_ms: i64,
    sent: &HashMap<(u64, u64), Ms>,
) -> Vec<DelegRenewCert> {
    if stuck_ms < DELEG_LANE_STUCK_MIN_MS || stuck_ms > hung_ms {
        return Vec::new();
    }
    certs
        .iter()
        .filter(|c| {
            let lane = sent
                .get(&(c.gen, c.round))
                .map_or(Ms(0), |s| s.plus(c.period_ms));
            stuck_ms >= stuck_threshold_ms(c.period_ms) && now >= c.next_due.max(lane)
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert(gen: u64, round: u64, next_due: i64, period_ms: u64) -> DelegRenewCert {
        DelegRenewCert {
            gen,
            root: 1,
            round,
            next_due: Ms(next_due),
            period_ms,
        }
    }

    #[test]
    fn nothing_while_the_driver_progresses_or_is_hung() {
        let certs = [cert(1, 7, 1_000, 2_500)];
        let none = HashMap::new();
        assert!(due(&certs, Ms(9_000), 0, 15_000, &none).is_empty());
        assert!(due(&certs, Ms(9_000), DELEG_LANE_STUCK_MS - 1, 15_000, &none).is_empty());
        assert_eq!(
            due(&certs, Ms(9_000), DELEG_LANE_STUCK_MS, 15_000, &none).len(),
            1
        );
        assert_eq!(due(&certs, Ms(9_000), 15_000, 15_000, &none).len(), 1);
        assert!(due(&certs, Ms(9_000), 15_001, 15_000, &none).is_empty());
    }

    #[test]
    fn a_short_period_shortens_the_wait() {
        assert_eq!(stuck_threshold_ms(10_000), 500);
        assert_eq!(stuck_threshold_ms(2_000), 500);
        assert_eq!(stuck_threshold_ms(1_200), 300);
        assert_eq!(stuck_threshold_ms(540), 135);
        assert_eq!(stuck_threshold_ms(10), DELEG_LANE_STUCK_MIN_MS);
        let certs = [cert(1, 7, 1_000, 540)];
        let none = HashMap::new();
        assert!(due(&certs, Ms(9_000), 134, 15_000, &none).is_empty());
        assert_eq!(due(&certs, Ms(9_000), 135, 15_000, &none).len(), 1);
        // Never below the floor, whatever the period.
        assert!(due(
            &certs,
            Ms(9_000),
            DELEG_LANE_STUCK_MIN_MS - 1,
            15_000,
            &none
        )
        .is_empty());
    }

    #[test]
    fn due_a_period_after_the_cores_latest_renewal() {
        let certs = [cert(1, 7, 4_000, 2_500)];
        let none = HashMap::new();
        assert!(due(&certs, Ms(3_999), 2_000, 15_000, &none).is_empty());
        assert_eq!(due(&certs, Ms(4_000), 2_000, 15_000, &none).len(), 1);
    }

    #[test]
    fn a_dispatched_or_sent_renewal_postpones_the_next() {
        let certs = [cert(1, 7, 4_000, 2_500)];
        let mut sent = HashMap::new();
        // The core's renewal, decided but not dispatched, is not in the
        // certificate: the lane renews. Once it is dispatched (here at
        // 4_100, the driver notes it), the next is a period after.
        assert_eq!(due(&certs, Ms(4_050), 3_000, 15_000, &sent).len(), 1);
        sent.insert((1, 7), Ms(4_100));
        assert!(due(&certs, Ms(6_599), 3_000, 15_000, &sent).is_empty());
        assert_eq!(due(&certs, Ms(6_600), 3_000, 15_000, &sent).len(), 1);
        // The book is by generation and round: another round's send says
        // nothing about this one's.
        let mut other = HashMap::new();
        other.insert((1, 8), Ms(4_100));
        other.insert((2, 7), Ms(4_100));
        assert_eq!(due(&certs, Ms(4_200), 3_000, 15_000, &other).len(), 1);
    }

    #[test]
    fn each_generation_on_its_own_clock() {
        let certs = [cert(1, 7, 4_000, 2_500), cert(2, 9, 8_000, 5_000)];
        let none = HashMap::new();
        let due_now = due(&certs, Ms(5_000), 2_000, 15_000, &none);
        assert_eq!(due_now.len(), 1);
        assert_eq!(due_now[0].gen, 1);
        assert_eq!(due(&certs, Ms(8_000), 2_000, 15_000, &none).len(), 2);
    }

    #[test]
    fn a_capped_grant_shortens_the_period_to_a_quarter() {
        let c = cert(1, 7, 4_000, 2_500);
        // The last send was at 1_500; a grant of 1_072 ms: period 268.
        let d = with_granted_ttl(&c, 1_072);
        assert_eq!((d.period_ms, d.next_due), (268, Ms(1_500 + 268)));
        // A full grant leaves the certificate's period (10 s / 4 > 2.5 s).
        assert_eq!(with_granted_ttl(&c, 10_000), c);
        // Never below the floor.
        let d = with_granted_ttl(&c, 40);
        assert_eq!(d.period_ms, DELEG_LANE_PERIOD_MIN_MS as u64);
        let d = with_granted_ttl(&c, 900);
        assert_eq!(
            d.period_ms, DELEG_LANE_PERIOD_MIN_MS as u64,
            "900 / 4 = 225 is below the floor"
        );
    }

    #[test]
    fn freezes_are_the_completed_one_and_the_one_in_progress() {
        let done = pack_freeze(1_000_000, 5_000);
        assert_eq!(unpack_freeze(done), (1_000_000, 1_005_000));
        assert!(freezes(0, 0, 2_000_000).is_empty());
        assert!(freezes(0, 2_000_000, 2_000_249).is_empty());
        assert_eq!(
            freezes(0, 2_000_000, 2_000_250),
            vec![(2_000_000, 2_000_250)]
        );
        assert_eq!(
            freezes(done, 2_000_000, 2_006_000),
            vec![(1_000_000, 1_005_000), (2_000_000, 2_006_000)]
        );
        // A completed one shorter than the minimum is not handed over.
        assert!(freezes(pack_freeze(1_000_000, 100), 0, 2_000_000).is_empty());
        // Packing saturates the length, never wraps it into the start.
        assert_eq!(
            unpack_freeze(pack_freeze(1_790_000_000_000, 1 << 30)).0,
            1_790_000_000_000
        );
        // The start, at the boundary of the bits it has (42, the year 2109):
        // the last value round-trips, the one after it saturates to the last
        // (and does not wrap to the epoch or into the length).
        let last = (1i64 << 42) - 1;
        assert_eq!(
            unpack_freeze(pack_freeze(last, 5_000)),
            (last, last + 5_000)
        );
        assert_eq!(unpack_freeze(pack_freeze(last + 1, 5_000)).0, last);
        assert_eq!(unpack_freeze(pack_freeze(1 << 50, 7)), (last, last + 7));
        // A start of 2039-09-07 (2^41 ms), where the first encoding overflowed.
        assert_eq!(
            unpack_freeze(pack_freeze(1 << 41, 1_234)),
            (1 << 41, (1 << 41) + 1_234)
        );
        // 0 is "none".
        assert_eq!(unpack_freeze(0), (0, 0));
    }

    #[test]
    fn only_a_freeze_in_the_window_of_the_loss_explains_it() {
        // Renewal due at 2_500, lapse point 6_000 (a 5 s grant, 500 ms margin).
        let (due, lapse) = (Ms(2_500), Ms(6_000));
        let ms = |a: i64, b: i64| (Ms(a), Ms(b));
        // Covering due -> lapse: explains it, by its length.
        assert_eq!(
            freeze_explains(&[ms(2_400, 6_100)], due, lapse),
            Some(3_700)
        );
        // Began just inside the reaction time, ended at the lapse point.
        assert_eq!(
            freeze_explains(&[ms(3_500, 6_000)], due, lapse),
            Some(2_500)
        );
        // Far from the loss (long before the renewal fell due): nothing.
        assert_eq!(freeze_explains(&[ms(-20_000, -12_000)], due, lapse), None);
        // Right after a renewal, thawed with time to renew in (the grant
        // survives, there is no lapse to explain; and if one comes anyway,
        // this freeze does not account for it).
        assert_eq!(freeze_explains(&[ms(100, 4_000)], due, lapse), None);
        // Began after the lane had its chance (due + 1 s): not what lost it.
        assert_eq!(freeze_explains(&[ms(3_501, 6_500)], due, lapse), None);
        // The longest of several that qualify.
        assert_eq!(
            freeze_explains(
                &[ms(-9_000, -1_000), ms(2_400, 6_100), ms(3_000, 6_000)],
                due,
                lapse
            ),
            Some(3_700)
        );
    }

    #[test]
    fn a_freeze_is_the_whole_processs_only_if_the_runtime_probe_missed_it_too() {
        // The heartbeat thread missed 1_000_300 .. 1_006_000.
        let f = (1_000_300, 1_006_000);
        // The shared runtime's lag probe missed about the same (it ticks
        // every 50 ms from the last tick: a little earlier).
        assert!(whole_process(f, (1_000_250, 1_006_000)));
        assert!(whole_process(f, (1_000_250, 1_005_700)));
        // The runtime ran on (no lapse), or only a short one: the heartbeat
        // thread was held up alone.
        assert!(!whole_process(f, (0, 0)));
        assert!(!whole_process(f, (1_003_000, 1_003_400)));
        // A lapse of the runtime long before or after is not this freeze.
        assert!(!whole_process(f, (900_000, 960_000)));
        assert!(!whole_process(f, (1_010_000, 1_020_000)));
    }
}
