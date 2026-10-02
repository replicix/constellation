//! `policy check` (plan 32 Step 5): what an expression means, before
//! anyone writes it onto a directory.
//!
//! The control method `snapshot.policy.check` and the CLI's local
//! (daemonless) `constellation snapshot policy check` both come here, so
//! the two cannot disagree on a warning or a count. Nothing here is
//! retention: the bound is [`SnapPolicy::steady_state_bound`], the
//! simulated figure is [`retention::simulate`](super::retention::simulate)'s,
//! and this module only decides *how far* to simulate and which facts
//! deserve a warning.
//!
//! ## How far to simulate
//!
//! The figure people want next to the upper bound is the count a policy
//! **settles at**, so without an explicit horizon the simulation runs for
//! [`settle_horizon_ms`]: the longest finite window plus one interval of
//! its tier — by then every finite tier has dropped its first
//! representative, and the count only wobbles with the calendar (a
//! 31-day month holds one more daily than a 30-day one). A policy with a
//! `*` tier never settles; it is simulated for at least a year, and the
//! figure is "how many after a year".
//!
//! A settled dense policy (`5m:… 1mo:1y`) needs ~110 000 synthetic
//! snapshots, past [`retention::MAX_SYNTHETIC`](super::retention::MAX_SYNTHETIC),
//! the cap that keeps the web UI's *timeline* (every snapshot, returned)
//! small. `check` returns one number, not the timeline, so it runs under
//! the larger [`CHECK_MAX_SYNTHETIC`]; a horizon past even that comes
//! back `truncated`, and says so.
//!
//! ## What bounds the work
//!
//! Creations are not the cost: every expiry pass looks at every live
//! candidate, so a policy that keeps most of what it creates (`1m:1y`
//! keeps 525 600, `1m:*` everything) costs the square of its creations
//! — a minute of CPU at the creation cap alone, from a Viewer method the
//! web UI calls as the user types. [`check_limits`] therefore also stops
//! the simulation once too many of the policy's snapshots are alive, and
//! after [`MAX_WORK`] evaluations in all. "Too many" is a little over the
//! policy's own bound when that bound is affordable (at most
//! [`CHECK_AFFORDABLE_BOUND`]: `10s:1d`'s 8640 settles in a fraction of
//! a second), and [`CHECK_MAX_LIVE`] otherwise (`1m:1y`, `1m:*`): past
//! that the count is already twice the bound warning, so "at least this
//! many, truncated" says everything the number would. The plan's densest
//! example peaks near 400 and never gets close.

use super::policy::{Keep, SnapPolicy};
use super::retention::{simulate_capped, SimLimits, SnapFacts, MAX_WORK};
use crate::policy_lex::{parse_duration, PolicyError};

/// `policy check` (and, in M2, `policy set`) warns on a steady-state
/// bound above this many snapshots (plan 32 Step 5).
pub const BOUND_WARNING: u64 = 2000;

/// The synthetic-snapshot cap of [`check`]'s simulation: a settled
/// 5-minute policy with a yearly tier, with room to spare. See the
/// module docs.
pub const CHECK_MAX_SYNTHETIC: usize = 131_072;

/// [`check`]'s cap on the policy's live snapshots during the simulation
/// of a policy whose bound is not affordable. See the module docs.
pub const CHECK_MAX_LIVE: usize = 4096;

/// The largest steady-state bound [`check`] simulates all the way to:
/// settling it costs about its square over two evaluations, which is
/// [`MAX_WORK`]. See the module docs.
pub const CHECK_AFFORDABLE_BOUND: u64 = 16_384;

/// What [`check`]'s simulation of `policy` may do. See the module docs.
pub fn check_limits(policy: &SnapPolicy) -> SimLimits {
    let max_live = match policy.steady_state_bound() {
        // The bound is nominal: a 31-day month or a 25-hour day holds a
        // little more, so leave a quarter on top before calling it
        // runaway.
        Some(bound) if bound <= CHECK_AFFORDABLE_BOUND => {
            (bound + bound / 4 + 2).max(CHECK_MAX_LIVE as u64) as usize
        }
        _ => CHECK_MAX_LIVE,
    };
    SimLimits {
        max_synthetic: CHECK_MAX_SYNTHETIC,
        max_live,
        max_work: MAX_WORK,
    }
}

/// The shortest horizon of a policy with a `*` tier.
const FOREVER_HORIZON_MS: i64 = 366 * 86_400_000;

/// The warnings `policy check` and `policy set` print for a valid
/// policy, in a fixed order.
pub fn warnings(policy: &SnapPolicy) -> Vec<String> {
    let mut out = Vec::new();
    if policy.has_subminute() {
        out.push(
            "sub-minute tier: seconds intervals are test-only (they exist so the harness \
             can run a real schedule in minutes)"
                .to_string(),
        );
    }
    if let Some(bound) = policy.steady_state_bound() {
        if bound > BOUND_WARNING {
            out.push(format!(
                "steady-state bound is {bound} snapshots (over {BOUND_WARNING}); \
                 consider a coarser finest tier or shorter windows"
            ));
        }
    }
    out
}

/// How long to simulate `policy` for its count to settle. See the
/// module docs.
pub fn settle_horizon_ms(policy: &SnapPolicy) -> i64 {
    let mut horizon: i64 = 0;
    let mut forever = false;
    for t in &policy.tiers {
        match t.keep {
            Keep::Forever => forever = true,
            keep => {
                let secs = keep
                    .nominal_duration()
                    .unwrap_or(0)
                    .saturating_add(t.every.nominal_duration());
                horizon = horizon.max(i64::try_from(secs.saturating_mul(1000)).unwrap_or(i64::MAX));
            }
        }
    }
    if forever {
        horizon = horizon.max(FOREVER_HORIZON_MS);
    }
    horizon
}

/// A simulation horizon as the CLI spells it (`--simulate 30d`): plan
/// 22's durations (`s m h d w y`, `m` is minutes), plus `mo` for a
/// nominal 30-day month. Milliseconds.
pub fn parse_horizon(s: &str) -> Result<i64, PolicyError> {
    let t = s.trim();
    let secs = if let Some(n) = t.strip_suffix("mo") {
        let n: u64 = n
            .parse()
            .map_err(|_| PolicyError::at(0, format!("invalid duration `{t}`")))?;
        n.saturating_mul(30 * 86_400)
    } else {
        parse_duration(t, 0)?.as_secs()
    };
    if secs == 0 {
        return Err(PolicyError::at(0, "the horizon must be longer than zero"));
    }
    Ok(i64::try_from(secs.saturating_mul(1000)).unwrap_or(i64::MAX))
}

/// What `policy check` says about one valid policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckReport {
    /// The canonical form ([`SnapPolicy`]'s `Display`).
    pub canonical: String,
    pub warnings: Vec<String>,
    /// [`SnapPolicy::steady_state_bound`]; `None` with a `*` tier.
    pub steady_state_bound: Option<u64>,
    /// The horizon simulated, ms.
    pub horizon_ms: i64,
    /// The policy's own snapshots alive at the end of the simulation
    /// (its candidates: manual, held and foreign snapshots in `existing`
    /// are not counted).
    pub simulated_count: u64,
    /// How far the simulation got, ms after `now`: `horizon_ms`'s last
    /// creation, or where it stopped when `truncated`.
    pub reached_ms: i64,
    /// The simulation hit one of [`check_limits`] before the horizon;
    /// `simulated_count` is the count at `reached_ms`.
    pub truncated: bool,
    /// Live candidates evaluated ([`SimLimits::max_work`]'s unit).
    pub work: u64,
}

/// Check `policy` for the root `policy_ino`: warnings, the bound, and a
/// simulation from `now_ms` over `existing` (empty for an expression
/// checked on its own) for `horizon_ms`, or [`settle_horizon_ms`].
pub fn check(
    policy: &SnapPolicy,
    policy_ino: u64,
    existing: &[SnapFacts],
    now_ms: i64,
    horizon_ms: Option<i64>,
) -> CheckReport {
    let horizon_ms = horizon_ms.unwrap_or_else(|| settle_horizon_ms(policy));
    let timeline = simulate_capped(
        policy,
        policy_ino,
        existing,
        now_ms,
        horizon_ms,
        check_limits(policy),
    );
    let last = timeline.counts.last();
    CheckReport {
        canonical: policy.to_string(),
        warnings: warnings(policy),
        steady_state_bound: policy.steady_state_bound(),
        horizon_ms,
        simulated_count: last.map_or(0, |c| u64::from(c.candidates)),
        reached_ms: last.map_or(0, |c| c.at_unix_ms.saturating_sub(now_ms).max(0)),
        truncated: timeline.truncated,
        work: timeline.work,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapsched::retention::Origin;

    fn policy(src: &str) -> SnapPolicy {
        SnapPolicy::parse(src).unwrap()
    }

    /// 2026-10-01T00:00:00Z: a Thursday, mid-year, no DST edge in UTC.
    const NOW: i64 = 1_790_812_800_000;

    #[test]
    fn warns_on_subminute_tiers_and_large_bounds_only() {
        assert!(warnings(&policy("1h:1d 1d:7d")).is_empty());
        let sub = warnings(&policy("10s:1h 1h:1d"));
        assert_eq!(sub.len(), 1);
        assert!(sub[0].contains("test-only"), "{sub:?}");
        // 1m:2d = 2880 > 2000.
        let big = warnings(&policy("1m:2d"));
        assert_eq!(big.len(), 1);
        assert!(big[0].contains("2880"), "{big:?}");
        // Just over the threshold warns (5m:1w = 2016), just under does
        // not (5m:6d = 1728).
        assert_eq!(warnings(&policy("5m:1w")).len(), 1);
        assert!(warnings(&policy("5m:6d")).is_empty());
        assert_eq!(warnings(&policy("10s:1d")).len(), 2, "both");
        // A `*` tier has no bound, so nothing to warn about.
        assert!(warnings(&policy("1d:14d 1mo:*")).is_empty());
    }

    #[test]
    fn settle_horizon_covers_the_longest_window_and_a_year_for_forever() {
        assert_eq!(
            settle_horizon_ms(&policy("1h:1d 1d:7d")),
            8 * 86_400_000,
            "7d + 1d"
        );
        assert_eq!(
            settle_horizon_ms(&policy("1d:14d 1mo:*")),
            FOREVER_HORIZON_MS
        );
        // 1y + 1mo, nominal.
        assert_eq!(
            settle_horizon_ms(&policy("5m:1d 1mo:1y")),
            (365 + 30) * 86_400_000
        );
    }

    #[test]
    fn simulated_count_settles_below_the_bound() {
        let p = policy("1h:1d 1d:7d");
        let r = check(&p, 7, &[], NOW, None);
        assert_eq!(r.canonical, "1h:1d 1d:7d");
        assert_eq!(r.steady_state_bound, Some(31));
        assert!(!r.truncated);
        // Union semantics: midnight's hourly is also the daily, so a
        // settled count sits below the bound.
        assert!(
            r.simulated_count < 31 && r.simulated_count >= 24,
            "{}",
            r.simulated_count
        );
    }

    #[test]
    fn a_dense_yearly_policy_settles_without_truncation() {
        let p = policy("5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest");
        let r = check(&p, 7, &[], NOW, None);
        assert!(!r.truncated, "{r:?}");
        assert!(r.simulated_count <= 510, "{r:?}");
        assert!(r.simulated_count > 288, "{r:?}");
        // Settled, with room under the work cap that stops `1m:*`.
        assert!(r.work < MAX_WORK / 2, "{r:?}");
        assert!(r.reached_ms <= r.horizon_ms && r.reached_ms > r.horizon_ms - 300_000);
    }

    /// The 32-M1c review's minute-long check: a policy that keeps most
    /// of what it creates costs the square of its creations, so the
    /// simulation stops on the live cap instead — in work, not just in
    /// creations.
    #[test]
    fn a_policy_that_keeps_everything_truncates_on_bounded_work() {
        // Summing 1..=CHECK_MAX_LIVE over the passes, plus the first.
        let live_cap_work = (CHECK_MAX_LIVE * (CHECK_MAX_LIVE + 1) / 2) as u64;
        for expr in ["1m:*", "1m:1y"] {
            let r = check(&policy(expr), 7, &[], NOW, None);
            assert!(r.truncated, "{expr}: {r:?}");
            assert!(r.work <= live_cap_work, "{expr}: {} evaluations", r.work);
            assert_eq!(r.simulated_count, CHECK_MAX_LIVE as u64, "{expr}");
            // It says how far it got, which is not the horizon.
            assert_eq!(r.reached_ms, (CHECK_MAX_LIVE as i64 - 1) * 60_000, "{expr}");
            assert!(r.reached_ms < r.horizon_ms, "{expr}");
        }
        // A bound over the live cap but affordable settles, untruncated.
        let r = check(&policy("1m:3d"), 7, &[], NOW, None);
        assert!(!r.truncated, "{r:?}");
        assert_eq!(r.simulated_count, 4320, "{r:?}");
    }

    #[test]
    fn only_the_policys_own_snapshots_are_counted() {
        let manual = SnapFacts {
            id: "m".into(),
            created_unix_ms: NOW - 1000,
            origin: Origin::Manual,
            policy_ino: 0,
            held: false,
            held_by: None,
        };
        let p = policy("1d:7d");
        let alone = check(&p, 7, &[], NOW, Some(86_400_000));
        let with_manual = check(&p, 7, &[manual], NOW, Some(86_400_000));
        assert_eq!(alone.simulated_count, with_manual.simulated_count);
    }

    #[test]
    fn horizons_parse_with_months_and_refuse_zero() {
        assert_eq!(parse_horizon("30d").unwrap(), 30 * 86_400_000);
        assert_eq!(parse_horizon("2mo").unwrap(), 60 * 86_400_000);
        assert_eq!(parse_horizon("90m").unwrap(), 90 * 60_000, "m is minutes");
        assert!(parse_horizon("0d").is_err());
        assert!(parse_horizon("soon").is_err());
        assert!(parse_horizon("xmo").is_err());
    }
}
