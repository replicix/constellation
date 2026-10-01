//! The snapshot-schedule expression language (plan 32, Step 1).
//!
//! An operator declares, once, on a directory: "every 5 minutes, keep
//! them a day; hourly for a week; daily for a month; monthly for a
//! year", and the cluster does that forever with nobody watching:
//!
//! ```text
//! setfattr -n user.constellation.snapshots \
//!          -v '5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest' /mnt/proj
//! ```
//!
//! The grammar, in full:
//!
//! ```text
//! policy   := clause ((";" | WS) clause)*
//! clause   := tier | key "=" value | "paused"
//! tier     := interval ":" keep
//! interval := <n>"s"   (n divides 60, n >= 10)   test-only: 10s 12s 15s 20s 30s
//!           | <n>"m"   (n divides 60)            1m 5m 10m 15m 20m 30m …
//!           | <n>"h"   (n divides 24)            1h 2h 3h 4h 6h 8h 12h
//!           | "1d" | "1w" | "1mo" | "1y"
//! keep     := <n>("min"|"m"|"h"|"d"|"w"|"mo"|"y") | "*"
//! ```
//!
//! and the settings, in the order the canonical form prints them:
//! `tz=`, `day-start=`, `week-start=`, `last=`, `skip-empty=`,
//! `budget=`, then the `paused` flag.
//!
//! # Purity
//!
//! Like [`crate::prune::policy`], this parser is *pure and total*: same
//! bytes in, same [`SnapPolicy`] out, on every node and every run — no
//! environment, no locale, no clock, no filesystem. That is not a style
//! preference. One node evaluates retention for the whole cluster, and a
//! node that parsed the policy differently would delete *different
//! snapshots*. The one external table consulted is the IANA tzdb
//! **bundled into the binary** by `jiff`, never the host's
//! `/usr/share/zoneinfo`, so `tz=Europe/Budapest` is valid or invalid
//! identically on every node and inside a static musl build.
//!
//! # Why minute-versus-month is a parse error
//!
//! Units follow plan 22: `m` is minutes and `mo` is months. This is a
//! delete-your-data feature and minute-versus-month is the most likely
//! misreading, so the two shapes where a reader could plausibly mean
//! either are refused rather than guessed:
//!
//! * `1M` anywhere — the error names both readings;
//! * a bare-`m` *keep* in a policy that also has a `1mo`-or-coarser tier
//!   (`5m:6m 1mo:1y`: did `6m` mean six minutes, or six months?). Write
//!   `6mo`, or the explicit `6min`.
//!
//! A bare-`m` keep shorter than its own bucket (`1d:6m`, the commonest
//! way to *mean* "daily, keep six months") is refused by the
//! `keep >= every` rule instead, which names both readings as well — no
//! reading of `1d:6m` is both legal and what the writer asked for.
//!
//! The canonical form always prints a minute keep as `6min`, so printed
//! policies never contain the ambiguous spelling and always re-parse.

use std::fmt;
use std::sync::OnceLock;

use crate::policy_lex::{fmt_size, parse_int, parse_size, PolicyError};

// --- the calendar interval -------------------------------------------

/// How often a snapshot is taken, and therefore the bucket width
/// retention aligns in. Deliberately *only* the set that divides the
/// calendar: a bucket that did not tile its parent unit would put a
/// daily boundary in the middle of a bucket, and the representative of
/// that bucket would flip as the day rolled over (plan 32, L4).
///
/// [`Ord`] is finest-first, so `tiers[0]` is the policy's cadence and
/// `tiers.last()` its coarsest retention tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Interval {
    /// Sub-minute, **test-only** (the harness runs a real schedule in
    /// minutes). Part of the pure grammar because validity may not
    /// depend on the environment; `snapshot policy set` warns on it.
    /// `n` divides 60 and is at least 10.
    Seconds(u32),
    /// `n` divides 60. `60m` normalizes to [`Interval::Hours(1)`].
    Minutes(u32),
    /// `n` divides 24. `24h` normalizes to [`Interval::Day`].
    Hours(u32),
    Day,
    Week,
    Month,
    Year,
}

impl Interval {
    /// A nominal length in seconds: a month is 30 days and a year is 365
    /// (plan 22's convention). Good for *bounds* and for ordering, never
    /// for aligning a bucket — that is calendar arithmetic in the
    /// policy's timezone, and lives in retention (Step 2).
    pub fn nominal_duration(&self) -> u64 {
        match self {
            Interval::Seconds(n) => u64::from(*n),
            Interval::Minutes(n) => u64::from(*n) * 60,
            Interval::Hours(n) => u64::from(*n) * 3_600,
            Interval::Day => 86_400,
            Interval::Week => 604_800,
            Interval::Month => 30 * 86_400,
            Interval::Year => 365 * 86_400,
        }
    }

    /// Whether boundaries of this interval need the policy's timezone and
    /// `day-start` (1h and coarser), as opposed to being the same in
    /// every zone.
    pub fn is_calendar_aligned(&self) -> bool {
        !matches!(self, Interval::Seconds(_) | Interval::Minutes(_))
    }

    /// Discriminant rank, so the ordering is total even for a value
    /// hand-built outside [`SnapPolicy::parse`] (which normalizes, and
    /// where nominal lengths are therefore already unique).
    fn rank(&self) -> u8 {
        match self {
            Interval::Seconds(_) => 0,
            Interval::Minutes(_) => 1,
            Interval::Hours(_) => 2,
            Interval::Day => 3,
            Interval::Week => 4,
            Interval::Month => 5,
            Interval::Year => 6,
        }
    }

    /// The longest a single bucket of this interval can actually be.
    /// Only calendar units differ from the nominal figure: a month is up
    /// to 31 days, a year up to 366. Used by the `keep >= every` check,
    /// which must hold in every month of every year.
    fn max_realization(&self) -> u64 {
        match self {
            Interval::Month => 31 * 86_400,
            Interval::Year => 366 * 86_400,
            other => other.nominal_duration(),
        }
    }

    /// `Some(months)` if this is a calendar-month-based unit.
    fn months(&self) -> Option<u32> {
        match self {
            Interval::Month => Some(1),
            Interval::Year => Some(12),
            _ => None,
        }
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.nominal_duration()
            .cmp(&other.nominal_duration())
            .then_with(|| self.rank().cmp(&other.rank()))
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Interval::Seconds(n) => write!(f, "{n}s"),
            Interval::Minutes(n) => write!(f, "{n}m"),
            Interval::Hours(n) => write!(f, "{n}h"),
            Interval::Day => f.write_str("1d"),
            Interval::Week => f.write_str("1w"),
            Interval::Month => f.write_str("1mo"),
            Interval::Year => f.write_str("1y"),
        }
    }
}

// --- the retention window --------------------------------------------

/// How long a tier keeps its snapshots. Months and years stay *symbolic*
/// rather than collapsing to seconds, because retention subtracts them
/// with calendar arithmetic ("one month before 31 March") and 30-day
/// months would drift.
///
/// The minute variant prints as `6min`, never `6m`: see the module docs
/// on the minute/month ambiguity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keep {
    Minutes(u32),
    Hours(u32),
    Days(u32),
    Weeks(u32),
    Months(u32),
    Years(u32),
    /// `*`: this tier's representatives are kept forever.
    Forever,
}

impl Keep {
    /// A nominal length in seconds (month = 30 d, year = 365 d), or
    /// `None` for [`Keep::Forever`]. For bounds and display only.
    pub fn nominal_duration(&self) -> Option<u64> {
        Some(match self {
            Keep::Minutes(n) => u64::from(*n) * 60,
            Keep::Hours(n) => u64::from(*n) * 3_600,
            Keep::Days(n) => u64::from(*n) * 86_400,
            Keep::Weeks(n) => u64::from(*n) * 604_800,
            Keep::Months(n) => u64::from(*n) * 30 * 86_400,
            Keep::Years(n) => u64::from(*n) * 365 * 86_400,
            Keep::Forever => return None,
        })
    }

    /// `Some(months)` if this window is expressed in calendar months.
    fn months(&self) -> Option<u32> {
        match self {
            Keep::Months(n) => Some(*n),
            Keep::Years(n) => Some(n.saturating_mul(12)),
            _ => None,
        }
    }
}

impl fmt::Display for Keep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Keep::Minutes(n) => write!(f, "{n}min"),
            Keep::Hours(n) => write!(f, "{n}h"),
            Keep::Days(n) => write!(f, "{n}d"),
            Keep::Weeks(n) => write!(f, "{n}w"),
            Keep::Months(n) => write!(f, "{n}mo"),
            Keep::Years(n) => write!(f, "{n}y"),
            Keep::Forever => f.write_str("*"),
        }
    }
}

// --- a tier ----------------------------------------------------------

/// One tier: "take one every `every`, keep them for `keep`".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tier {
    pub every: Interval,
    pub keep: Keep,
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.every, self.keep)
    }
}

/// Whether `keep` is at least as long as one `every` bucket, compared so
/// that the answer holds in every month of every year:
///
/// * both sides fixed-length (`s m h d w`) — exact nominal seconds, so
///   `1d:1d` and `1d:24h` are accepted;
/// * both sides calendar (`mo y`) — compare month counts, so `1mo:1y`
///   is fine and `1y:1mo` is not;
/// * a calendar *keep* against a fixed `every` — a month is at least 28
///   days, which is longer than the coarsest fixed interval (`1w`), so
///   `1d:1mo` is fine;
/// * a fixed *keep* against a calendar `every` — the keep must cover the
///   **longest** such bucket, so `1mo:30d` is rejected (February is
///   fine, but March is 31 days) and `1mo:31d` is accepted.
fn keep_covers_every(every: Interval, keep: Keep) -> bool {
    if keep == Keep::Forever {
        return true;
    }
    match (every.months(), keep.months()) {
        (Some(e), Some(k)) => k >= e,
        (Some(_), None) => keep.nominal_duration().unwrap_or(0) >= every.max_realization(),
        (None, Some(_)) => every.nominal_duration() <= 28 * 86_400,
        (None, None) => keep.nominal_duration().unwrap_or(0) >= every.nominal_duration(),
    }
}

// --- settings --------------------------------------------------------

/// Which day a `1w` bucket starts on. ISO weeks (Monday) by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum WeekStart {
    #[default]
    Mon,
    Sun,
}

impl fmt::Display for WeekStart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WeekStart::Mon => f.write_str("mon"),
            WeekStart::Sun => f.write_str("sun"),
        }
    }
}

pub const DEFAULT_TZ: &str = "UTC";
pub const DEFAULT_DAY_START: (u8, u8) = (0, 0);
pub const DEFAULT_LAST: u32 = 1;
pub const DEFAULT_SKIP_EMPTY: bool = true;

/// A parsed snapshot schedule: one stream of snapshots, with the tiers
/// acting purely as a retention filter over it (plan 32, L1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapPolicy {
    /// At least one, unique by `every`, sorted finest first.
    pub tiers: Vec<Tier>,
    /// An IANA timezone name in its canonical tzdb spelling. Bucket
    /// boundaries of 1h and coarser are aligned in it.
    pub tz: String,
    /// `(hour, minute)` at which day/week/month/year buckets start.
    /// Always a whole hour; see [`SnapPolicy::parse`].
    pub day_start: (u8, u8),
    pub week_start: WeekStart,
    /// Always keep the `last` newest candidates. At least 1: the newest
    /// snapshot is never auto-deleted (L3).
    pub last: u32,
    /// Skip creation when nothing under the root changed (L8).
    pub skip_empty: bool,
    /// Step 8's soft cap on this policy's snapshot-only bytes. Parsed
    /// here; nothing consumes it yet.
    pub budget: Option<u64>,
    /// The maintenance switch: stops creation *and* expiry, keeps
    /// everything. Better than removing the policy, which would trip the
    /// grace period of Step 4.3.
    pub paused: bool,
}

impl SnapPolicy {
    /// The policy's cadence: the finest tier's interval. `None` only for
    /// a tierless value built outside [`SnapPolicy::parse`].
    pub fn finest(&self) -> Option<Interval> {
        self.tiers.first().map(|t| t.every)
    }

    /// The coarsest tier's interval, the one Step 8's budget sheds last.
    pub fn coarsest(&self) -> Option<Interval> {
        self.tiers.last().map(|t| t.every)
    }

    /// Whether any tier is sub-minute, i.e. test-only. The CLI warns on
    /// it and the scheduler needs a faster tick.
    pub fn has_subminute(&self) -> bool {
        self.tiers
            .iter()
            .any(|t| matches!(t.every, Interval::Seconds(_)))
    }

    /// The plan's steady-state upper bound on how many snapshots this
    /// policy retains: the sum of `keep / every` over the tiers, floored
    /// at `last`. `None` when a tier keeps forever.
    ///
    /// It is an *upper* bound and union semantics (L2) make the real
    /// figure smaller — the midnight snapshot is the representative of
    /// the 5m, 1h and 1d buckets at once, and is counted three times
    /// here. `policy check` prints this next to a simulated figure.
    ///
    /// `None` also for a zero-width interval, which only a value
    /// hand-built outside [`SnapPolicy::parse`] can have (the parser
    /// refuses `0m`): a cadence of zero takes infinitely many
    /// snapshots, so there is no bound to print.
    pub fn steady_state_bound(&self) -> Option<u64> {
        let mut sum = 0u64;
        for t in &self.tiers {
            let keep = t.keep.nominal_duration()?;
            sum = sum.saturating_add(keep.checked_div(t.every.nominal_duration())?);
        }
        Some(sum.max(u64::from(self.last)))
    }

    /// Parse a snapshot-schedule expression. Pure — no environment, no
    /// clock; the only table consulted is the bundled IANA tzdb.
    ///
    /// Every failure carries the byte offset of the token at fault, for
    /// the caret rendering of [`PolicyError::render`].
    pub fn parse(src: &str) -> Result<SnapPolicy, PolicyError> {
        let clauses = split_clauses(src);
        if clauses.is_empty() {
            return Err(PolicyError::at(0, "empty policy"));
        }

        let mut parsed: Vec<ParsedTier> = Vec::new();
        // A bare-`m` keep: legal on its own, ambiguous beside a monthly
        // tier, which may only be known once every clause is read.
        let mut bare_minute_keep: Option<(u32, usize)> = None;
        let mut tz: Option<String> = None;
        let mut day_start: Option<((u8, u8), usize)> = None;
        let mut week_start: Option<WeekStart> = None;
        let mut last: Option<u32> = None;
        let mut skip_empty: Option<bool> = None;
        let mut budget: Option<u64> = None;
        let mut paused = false;

        for clause in &clauses {
            let text = clause.text;
            let base = clause.start;
            let eq = text.find('=');
            let colon = text.find(':');

            // A setting is `key=value`; `day-start=02:00` has both an `=`
            // and a `:`, so the first separator decides.
            if let Some(eq) = eq.filter(|e| colon.is_none_or(|c| *e < c)) {
                let key = &text[..eq];
                let val = &text[eq + 1..];
                let key_off = base;
                let val_off = base + eq + 1;
                if val.is_empty() {
                    return Err(PolicyError::at(
                        val_off,
                        format!("setting `{key}` has no value"),
                    ));
                }
                match key {
                    "tz" => {
                        dup(tz.is_some(), key_off, "tz")?;
                        tz = Some(canonical_tz(val, val_off)?);
                    }
                    "day-start" => {
                        dup(day_start.is_some(), key_off, "day-start")?;
                        day_start = Some((parse_hhmm(val, val_off)?, val_off));
                    }
                    "week-start" => {
                        dup(week_start.is_some(), key_off, "week-start")?;
                        week_start = Some(match val {
                            "mon" => WeekStart::Mon,
                            "sun" => WeekStart::Sun,
                            other => {
                                return Err(PolicyError::at(
                                    val_off,
                                    format!("`week-start` must be `mon` or `sun`, not `{other}`"),
                                ))
                            }
                        });
                    }
                    "last" => {
                        dup(last.is_some(), key_off, "last")?;
                        let n = parse_int(val, val_off)?;
                        if n == 0 {
                            return Err(PolicyError::at(
                                val_off,
                                "`last` must be at least 1: the newest snapshot is never \
                                 deleted automatically",
                            ));
                        }
                        last = Some(n.min(u64::from(u32::MAX)) as u32);
                    }
                    "skip-empty" => {
                        dup(skip_empty.is_some(), key_off, "skip-empty")?;
                        skip_empty = Some(match val {
                            "yes" => true,
                            "no" => false,
                            other => {
                                return Err(PolicyError::at(
                                    val_off,
                                    format!("`skip-empty` must be `yes` or `no`, not `{other}`"),
                                ))
                            }
                        });
                    }
                    "budget" => {
                        dup(budget.is_some(), key_off, "budget")?;
                        let b = parse_size(val, val_off)?;
                        if b == 0 {
                            return Err(PolicyError::at(val_off, "`budget` cannot be 0"));
                        }
                        budget = Some(b);
                    }
                    other => {
                        return Err(PolicyError::at(
                            key_off,
                            format!(
                                "unknown setting `{other}` (tz, day-start, week-start, last, \
                                 skip-empty, budget)"
                            ),
                        ));
                    }
                }
                continue;
            }

            // A tier is `interval ":" keep`.
            if let Some(colon) = colon {
                let every_src = &text[..colon];
                let keep_src = &text[colon + 1..];
                let keep_off = base + colon + 1;
                if every_src.is_empty() {
                    return Err(PolicyError::at(
                        base,
                        "tier has no interval; write `<every>:<keep>`, e.g. `5m:1d`",
                    ));
                }
                if keep_src.is_empty() {
                    return Err(PolicyError::at(
                        keep_off,
                        format!("tier `{text}` has no keep window; e.g. `{every_src}:1d`"),
                    ));
                }
                let every = parse_interval(every_src, base)?;
                let (keep, bare_m) = parse_keep(keep_src, keep_off)?;
                if let Some(prev) = parsed.iter().find(|p| p.tier.every == every) {
                    return Err(PolicyError::at(
                        base,
                        format!(
                            "duplicate `{every}` tier (already given as `{}`)",
                            prev.tier
                        ),
                    ));
                }
                if bare_m {
                    // Report the first such keep, so the caret lands on
                    // the earliest ambiguity in the expression.
                    let n = match keep {
                        Keep::Minutes(n) => n,
                        _ => 0,
                    };
                    bare_minute_keep = bare_minute_keep.or(Some((n, keep_off)));
                }
                parsed.push(ParsedTier {
                    tier: Tier { every, keep },
                    keep_off,
                    bare_minute: bare_m,
                });
                continue;
            }

            // A flag, or a mistake shaped like one.
            if text == "paused" {
                if paused {
                    return Err(PolicyError::at(base, "duplicate `paused`"));
                }
                paused = true;
                continue;
            }
            if matches!(
                text,
                "tz" | "day-start" | "week-start" | "last" | "skip-empty" | "budget"
            ) {
                return Err(PolicyError::at(
                    base,
                    format!("setting `{text}` needs a value, e.g. `{text}=…`"),
                ));
            }
            if parse_interval(text, base).is_ok() {
                return Err(PolicyError::at(
                    base,
                    format!("`{text}` has no keep window; write `{text}:<keep>`, e.g. `{text}:1d`"),
                ));
            }
            return Err(PolicyError::at(
                base,
                format!("unknown clause `{text}` (a tier `5m:1d`, a setting `k=v`, or `paused`)"),
            ));
        }

        if parsed.is_empty() {
            return Err(PolicyError::at(
                0,
                "policy has no tier; write at least one `<every>:<keep>`, e.g. `1h:1d 1d:30d`",
            ));
        }

        // Minute-versus-month, now that every tier is known.
        let has_monthly = parsed.iter().any(|p| p.tier.every >= Interval::Month);
        if let (Some((n, off)), true) = (bare_minute_keep, has_monthly) {
            return Err(PolicyError::at(
                off,
                format!(
                    "`{n}m` as a keep is ambiguous in a policy with a monthly tier: write \
                     `{n}mo` for {n} months, or `{n}min` for {n} minutes"
                ),
            ));
        }

        // `keep >= every`, calendar-aware. A bare-`m` keep gets the
        // both-readings text here too: `1d:6m` is the commonest way to
        // *mean* "daily, keep six months", and it reaches this refusal
        // (six minutes is shorter than a day) rather than the
        // ambiguity one above, which only fires beside a monthly tier.
        if let Some(bad) = parsed
            .iter()
            .find(|p| !keep_covers_every(p.tier.every, p.tier.keep))
        {
            let mut msg = format!(
                "keep `{}` is shorter than one `{}` bucket, so the tier would keep nothing",
                bad.tier.keep, bad.tier.every
            );
            if bad.bare_minute {
                if let Keep::Minutes(n) = bad.tier.keep {
                    msg = format!(
                        "keep `{n}m` is shorter than one `{}` bucket, so the tier would keep \
                         nothing — and `{n}m` is ambiguous: write `{n}mo` for {n} months, or \
                         `{n}min` for {n} minutes",
                        bad.tier.every
                    );
                }
            }
            return Err(PolicyError::at(bad.keep_off, msg));
        }

        let mut tiers: Vec<Tier> = parsed.iter().map(|p| p.tier).collect();
        tiers.sort_by_key(|t| t.every);

        // `day-start` shifts the day/week/month/year boundary, so it has
        // to fall on a boundary of the finest *hourly* tier; otherwise
        // that tier's buckets would straddle the daily boundary (L4).
        // With no hourly tier nothing finer than a day is aligned in the
        // timezone, and any whole hour will do.
        if let Some(((h, m), off)) = day_start {
            if m != 0 {
                return Err(PolicyError::at(
                    off,
                    format!(
                        "`day-start` must be on a whole hour, not `{h:02}:{m:02}`: day, week, \
                         month and year buckets start on an hour boundary"
                    ),
                ));
            }
            let finest_hourly = tiers
                .iter()
                .filter_map(|t| match t.every {
                    Interval::Hours(n) => Some(n),
                    _ => None,
                })
                .min();
            if let Some(n) = finest_hourly {
                if u32::from(h) % n != 0 {
                    return Err(PolicyError::at(
                        off,
                        format!(
                            "`day-start={h:02}:{m:02}` is not a multiple of the finest hourly \
                             tier `{n}h`; use a multiple of {n}h"
                        ),
                    ));
                }
            }
        }

        Ok(SnapPolicy {
            tiers,
            tz: tz.unwrap_or_else(|| DEFAULT_TZ.to_string()),
            day_start: day_start.map(|(v, _)| v).unwrap_or(DEFAULT_DAY_START),
            week_start: week_start.unwrap_or_default(),
            last: last.unwrap_or(DEFAULT_LAST),
            skip_empty: skip_empty.unwrap_or(DEFAULT_SKIP_EMPTY),
            budget,
            paused,
        })
    }
}

impl fmt::Display for SnapPolicy {
    /// The canonical form: tiers finest first, then the settings in table
    /// order with defaults omitted, then `paused`. `parse(p.to_string())
    /// == p` for every parsed policy.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tiers: Vec<String> = self.tiers.iter().map(Tier::to_string).collect();
        f.write_str(&tiers.join(" "))?;
        let mut settings: Vec<String> = Vec::new();
        if self.tz != DEFAULT_TZ {
            settings.push(format!("tz={}", self.tz));
        }
        if self.day_start != DEFAULT_DAY_START {
            settings.push(format!(
                "day-start={:02}:{:02}",
                self.day_start.0, self.day_start.1
            ));
        }
        if self.week_start != WeekStart::default() {
            settings.push(format!("week-start={}", self.week_start));
        }
        if self.last != DEFAULT_LAST {
            settings.push(format!("last={}", self.last));
        }
        if self.skip_empty != DEFAULT_SKIP_EMPTY {
            settings.push("skip-empty=no".to_string());
        }
        if let Some(b) = self.budget {
            settings.push(format!("budget={}", fmt_size(b)));
        }
        if self.paused {
            settings.push("paused".to_string());
        }
        for s in settings {
            write!(f, "; {s}")?;
        }
        Ok(())
    }
}

// --- parsing internals -----------------------------------------------

/// A tier plus the offset its keep window starts at, which the
/// `keep >= every` refusal points the caret at.
struct ParsedTier {
    tier: Tier,
    keep_off: usize,
    /// The keep was written with the ambiguous bare `m` unit, which
    /// both the minute-versus-month refusal and the `keep >= every`
    /// refusal name both readings of.
    bare_minute: bool,
}

struct Clause<'a> {
    text: &'a str,
    start: usize,
}

/// Split on `;` and on whitespace, both separators. Runs of separators
/// collapse, so the canonical form's `"; "` is one separator and a
/// trailing `;` is not an empty clause.
fn split_clauses(src: &str) -> Vec<Clause<'_>> {
    let mut out = Vec::new();
    let mut cur: Option<usize> = None;
    for (i, c) in src.char_indices() {
        if c == ';' || c.is_whitespace() {
            if let Some(s) = cur.take() {
                out.push(Clause {
                    text: &src[s..i],
                    start: s,
                });
            }
        } else if cur.is_none() {
            cur = Some(i);
        }
    }
    if let Some(s) = cur {
        out.push(Clause {
            text: &src[s..],
            start: s,
        });
    }
    out
}

fn dup(seen: bool, off: usize, key: &str) -> Result<(), PolicyError> {
    if seen {
        return Err(PolicyError::at(off, format!("duplicate setting `{key}`")));
    }
    Ok(())
}

/// Split `<digits><unit>`, with the unit possibly empty.
fn split_number(s: &str) -> (&str, &str) {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    (&s[..end], &s[end..])
}

fn parse_count(digits: &str, whole: &str, off: usize, what: &str) -> Result<u32, PolicyError> {
    if digits.is_empty() {
        return Err(PolicyError::at(
            off,
            format!("{what} `{whole}` must start with a number"),
        ));
    }
    digits
        .parse::<u32>()
        .map_err(|_| PolicyError::at(off, format!("{what} `{whole}` is out of range")))
}

/// The ambiguity refusal, used for `1M` in either position.
fn ambiguous_capital_m(n: u32, whole: &str, off: usize) -> PolicyError {
    PolicyError::at(
        off,
        format!(
            "`{whole}` is ambiguous: write `{n}mo` for {n} months, or `{n}m` for {n} minutes \
             (units are plan 22's: `m` is minutes, `mo` is months)"
        ),
    )
}

fn parse_interval(s: &str, off: usize) -> Result<Interval, PolicyError> {
    let (digits, unit) = split_number(s);
    let n = parse_count(digits, s, off, "interval")?;
    if unit == "M" {
        return Err(ambiguous_capital_m(n, s, off));
    }
    if n == 0 {
        return Err(PolicyError::at(off, format!("interval `{s}` cannot be 0")));
    }
    match unit {
        "s" => {
            if n == 60 {
                return Ok(Interval::Minutes(1));
            }
            if n < 10 || !60u32.is_multiple_of(n) {
                return Err(PolicyError::at(
                    off,
                    format!(
                        "`{s}` is not a usable sub-minute interval; use 10s, 12s, 15s, 20s or \
                         30s (test-only)"
                    ),
                ));
            }
            Ok(Interval::Seconds(n))
        }
        "m" | "min" => {
            if n == 60 {
                return Ok(Interval::Hours(1));
            }
            if !60u32.is_multiple_of(n) {
                return Err(PolicyError::at(
                    off,
                    format!(
                        "`{s}` does not divide an hour; use a divisor of 60 (1m, 2m, 3m, 4m, 5m, \
                         6m, 10m, 12m, 15m, 20m or 30m)"
                    ),
                ));
            }
            Ok(Interval::Minutes(n))
        }
        "h" => {
            if n == 24 {
                return Ok(Interval::Day);
            }
            if !24u32.is_multiple_of(n) {
                return Err(PolicyError::at(
                    off,
                    format!("`{s}` does not divide a day; use 1h, 2h, 3h, 4h, 6h, 8h or 12h"),
                ));
            }
            Ok(Interval::Hours(n))
        }
        "d" | "w" | "mo" | "y" => {
            if n != 1 {
                return Err(PolicyError::at(
                    off,
                    format!(
                        "only `1{unit}` is a valid interval; a {n}-{unit} bucket does not tile \
                         the calendar"
                    ),
                ));
            }
            Ok(match unit {
                "d" => Interval::Day,
                "w" => Interval::Week,
                "mo" => Interval::Month,
                _ => Interval::Year,
            })
        }
        other => Err(PolicyError::at(
            off,
            format!("unknown interval unit `{other}` in `{s}` (use s, m, h, d, w, mo or y)"),
        )),
    }
}

/// Returns the window plus whether it was written with the bare `m`
/// unit, which the caller refuses beside a monthly tier.
fn parse_keep(s: &str, off: usize) -> Result<(Keep, bool), PolicyError> {
    if s == "*" {
        return Ok((Keep::Forever, false));
    }
    let (digits, unit) = split_number(s);
    let n = parse_count(digits, s, off, "keep")?;
    if unit == "M" {
        return Err(ambiguous_capital_m(n, s, off));
    }
    if n == 0 {
        return Err(PolicyError::at(
            off,
            format!("keep `{s}` cannot be 0; use `*` to keep forever"),
        ));
    }
    let keep = match unit {
        "min" => return Ok((Keep::Minutes(n), false)),
        "m" => return Ok((Keep::Minutes(n), true)),
        "h" => Keep::Hours(n),
        "d" => Keep::Days(n),
        "w" => Keep::Weeks(n),
        "mo" => Keep::Months(n),
        "y" => Keep::Years(n),
        "s" => {
            return Err(PolicyError::at(
                off,
                format!(
                    "`{s}` is not a keep window; keep units are min, h, d, w, mo and y (a \
                     sub-minute window would keep nothing)"
                ),
            ))
        }
        other => {
            return Err(PolicyError::at(
                off,
                format!(
                    "unknown keep unit `{other}` in `{s}` (use min, h, d, w, mo, y, or `*` for \
                     forever)"
                ),
            ))
        }
    };
    Ok((keep, false))
}

fn parse_hhmm(s: &str, off: usize) -> Result<(u8, u8), PolicyError> {
    let Some((h, m)) = s.split_once(':') else {
        return Err(PolicyError::at(
            off,
            format!("`day-start` must be `HH:MM`, got `{s}`"),
        ));
    };
    if m.contains(':') || h.is_empty() || m.is_empty() {
        return Err(PolicyError::at(
            off,
            format!("`day-start` must be `HH:MM`, got `{s}`"),
        ));
    }
    let hour = parse_int(h, off)?;
    let minute = parse_int(m, off)?;
    if hour > 23 || minute > 59 {
        return Err(PolicyError::at(
            off,
            format!("`day-start={s}` is not a time of day"),
        ));
    }
    Ok((hour as u8, minute as u8))
}

/// Validate an IANA name against the **bundled** tzdb and return its
/// canonical spelling, so `tz=europe/budapest` and `tz=Europe/Budapest`
/// are the same policy and the canonical form is stable.
///
/// Only the compiled-in database is consulted (jiff with default
/// features off plus `tzdb-bundle-always`): a `tz=` name must be valid
/// or invalid identically on every node, and a static musl build has no
/// `/usr/share/zoneinfo` to read. `Etc/Unknown`, the tzdb's sentinel for
/// "no zone", resolves but has no IANA name, and is refused.
fn canonical_tz(name: &str, off: usize) -> Result<String, PolicyError> {
    static DB: OnceLock<jiff::tz::TimeZoneDatabase> = OnceLock::new();
    let db = DB.get_or_init(jiff::tz::TimeZoneDatabase::bundled);
    match db.get(name) {
        Ok(tz) => match tz.iana_name() {
            Some(canon) => Ok(canon.to_string()),
            None => Err(PolicyError::at(
                off,
                format!("`{name}` is not a usable timezone; name a real IANA zone"),
            )),
        },
        Err(_) => Err(PolicyError::at(
            off,
            format!(
                "unknown timezone `{name}`; use an IANA name such as `UTC` or \
                 `Europe/Budapest` (checked against the bundled tzdb)"
            ),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn p(s: &str) -> SnapPolicy {
        SnapPolicy::parse(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
    }

    fn err(s: &str) -> PolicyError {
        SnapPolicy::parse(s).expect_err(&format!("expected an error for {s:?}"))
    }

    /// Every policy printed in the plan's examples table (Step 1), plus
    /// the canonical form quoted in "Canonical form".
    const EXAMPLES: &[&str] = &[
        "1h:1d 1d:7d",
        "15m:1d 1h:2d 1d:30d 1mo:1y",
        "5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest",
        "1d:14d 1mo:*; day-start=02:00",
        "1d:7d; last=3; skip-empty=no",
        "5m:1d 1h:7d 1d:30d 1mo:1y; tz=Europe/Budapest",
    ];

    #[test]
    fn plan_examples_parse_and_print_canonically() {
        for ex in EXAMPLES {
            let parsed = p(ex);
            assert_eq!(&parsed.to_string(), ex, "canonical form of {ex:?}");
            assert_eq!(p(&parsed.to_string()), parsed, "round-trip of {ex:?}");
        }
    }

    #[test]
    fn plan_example_steady_state_bounds() {
        // The figures in the plan's examples table.
        assert_eq!(p("1h:1d 1d:7d").steady_state_bound(), Some(31));
        assert_eq!(
            p("15m:1d 1h:2d 1d:30d 1mo:1y").steady_state_bound(),
            Some(96 + 48 + 30 + 12)
        );
        assert_eq!(
            p("5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y; tz=Europe/Budapest").steady_state_bound(),
            Some(288 + 168 + 30 + 12 + 12)
        );
        // A `*` tier is unbounded: "14 + months elapsed".
        assert_eq!(
            p("1d:14d 1mo:*; day-start=02:00").steady_state_bound(),
            None
        );
        assert_eq!(
            p("1d:7d; last=3; skip-empty=no").steady_state_bound(),
            Some(7)
        );
        // `last` is a floor on the bound, not a summand.
        assert_eq!(p("1d:7d; last=100").steady_state_bound(), Some(100));
    }

    #[test]
    fn defaults_are_the_tables() {
        let pol = p("1h:1d");
        assert_eq!(pol.tz, "UTC");
        assert_eq!(pol.day_start, (0, 0));
        assert_eq!(pol.week_start, WeekStart::Mon);
        assert_eq!(pol.last, 1);
        assert!(pol.skip_empty);
        assert_eq!(pol.budget, None);
        assert!(!pol.paused);
        // Defaults are omitted from the canonical form.
        assert_eq!(pol.to_string(), "1h:1d");
        assert_eq!(
            p("1h:1d; tz=UTC; day-start=00:00; week-start=mon; last=1; skip-empty=yes").to_string(),
            "1h:1d"
        );
    }

    #[test]
    fn settings_print_in_table_order() {
        let src = "last=5 skip-empty=no 1h:1d week-start=sun budget=500G day-start=02:00 \
                   tz=Asia/Kolkata paused";
        assert_eq!(
            p(src).to_string(),
            "1h:1d; tz=Asia/Kolkata; day-start=02:00; week-start=sun; last=5; \
             skip-empty=no; budget=500G; paused"
        );
        assert_eq!(p(&p(src).to_string()), p(src));
    }

    #[test]
    fn clause_separators_are_semicolon_or_whitespace() {
        let want = p("5m:1d 1h:7d; tz=UTC");
        for src in [
            "5m:1d 1h:7d",
            "5m:1d;1h:7d",
            "5m:1d; 1h:7d",
            "  5m:1d \t 1h:7d  ",
            "1h:7d 5m:1d",
            "5m:1d;;;1h:7d;",
        ] {
            assert_eq!(p(src), want, "{src:?}");
        }
    }

    #[test]
    fn tiers_sort_finest_first_and_normalize() {
        let pol = p("1y:* 1mo:1y 1w:12w 1d:30d 1h:7d 5m:1d 30s:10min");
        assert_eq!(
            pol.to_string(),
            "30s:10min 5m:1d 1h:7d 1d:30d 1w:12w 1mo:1y 1y:*"
        );
        assert_eq!(pol.finest(), Some(Interval::Seconds(30)));
        assert_eq!(pol.coarsest(), Some(Interval::Year));
        assert!(pol.has_subminute());
        assert!(!p("5m:1d").has_subminute());
        // `60s`, `60m` and `24h` normalize to the coarser spelling, which
        // is what makes "no duplicate intervals" airtight.
        assert_eq!(p("60s:1h").tiers[0].every, Interval::Minutes(1));
        assert_eq!(p("60m:1d").tiers[0].every, Interval::Hours(1));
        assert_eq!(p("24h:7d").tiers[0].every, Interval::Day);
        assert!(err("60m:1d 1h:2d").msg.contains("duplicate"));
    }

    #[test]
    fn interval_order_is_finest_first() {
        let mut v = vec![
            Interval::Year,
            Interval::Minutes(1),
            Interval::Day,
            Interval::Seconds(10),
            Interval::Month,
            Interval::Hours(12),
            Interval::Week,
            Interval::Minutes(30),
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                Interval::Seconds(10),
                Interval::Minutes(1),
                Interval::Minutes(30),
                Interval::Hours(12),
                Interval::Day,
                Interval::Week,
                Interval::Month,
                Interval::Year,
            ]
        );
    }

    #[test]
    fn timezones_come_from_the_bundled_tzdb_and_canonicalize() {
        assert_eq!(p("1d:30d; tz=Europe/Budapest").tz, "Europe/Budapest");
        // Case-insensitive lookup, canonical spelling stored, so the
        // policy value is stable and the canonical form re-parses.
        let pol = p("1d:30d; tz=europe/BUDAPEST");
        assert_eq!(pol.tz, "Europe/Budapest");
        assert_eq!(pol.to_string(), "1d:30d; tz=Europe/Budapest");
        assert_eq!(p("1d:30d; tz=utc").to_string(), "1d:30d");
        for zone in ["UTC", "Etc/UTC", "Asia/Kolkata", "Pacific/Chatham"] {
            let _ = p(&format!("1d:30d; tz={zone}"));
        }
        // The tzdb's "no zone" sentinel is not a schedule timezone.
        assert!(err("1d:30d; tz=Etc/Unknown").msg.contains("not a usable"));
        // Never the host's zoneinfo, and never a local-time escape hatch.
        assert!(err("1d:30d; tz=localtime").msg.contains("unknown timezone"));
    }

    #[test]
    fn keep_forever_and_calendar_keeps() {
        assert_eq!(p("1mo:*").tiers[0].keep, Keep::Forever);
        assert_eq!(p("1d:1mo").tiers[0].keep, Keep::Months(1));
        assert_eq!(p("1d:2y").tiers[0].keep, Keep::Years(2));
        // Days are not promoted to weeks: the canonical form keeps the
        // unit written, which is why `1h:7d` stays `1h:7d`.
        assert_eq!(p("1h:7d").to_string(), "1h:7d");
        assert_eq!(p("1h:1w").to_string(), "1h:1w");
        // A minute keep always prints unambiguously.
        assert_eq!(p("1m:90m").to_string(), "1m:90min");
        assert_eq!(p("1m:90min").to_string(), "1m:90min");
    }

    #[test]
    fn keep_ge_every_is_calendar_aware() {
        // A month can be 31 days, so a 30-day window may not cover it.
        assert!(err("1mo:30d").msg.contains("shorter than one `1mo`"));
        assert!(SnapPolicy::parse("1mo:31d").is_ok());
        assert!(SnapPolicy::parse("1mo:5w").is_ok());
        // A year can be 366 days.
        assert!(SnapPolicy::parse("1y:365d").is_err());
        assert!(SnapPolicy::parse("1y:366d").is_ok());
        // A calendar keep always covers a fixed-length bucket.
        assert!(SnapPolicy::parse("1d:1mo").is_ok());
        assert!(SnapPolicy::parse("1w:1mo").is_ok());
        // Calendar against calendar compares month counts.
        assert!(SnapPolicy::parse("1mo:1y").is_ok());
        assert!(SnapPolicy::parse("1y:1mo").is_err());
        assert!(SnapPolicy::parse("1mo:1mo").is_ok());
        // Fixed against fixed is exact and nominal, so equality passes.
        assert!(SnapPolicy::parse("1d:1d").is_ok());
        assert!(SnapPolicy::parse("1d:24h").is_ok());
        assert!(SnapPolicy::parse("1h:1h").is_ok());
        assert!(SnapPolicy::parse("10s:1min").is_ok());
    }

    #[test]
    fn day_start_must_land_on_the_finest_hourly_tier() {
        // No hourly tier: any whole hour.
        assert_eq!(p("1d:14d 1mo:*; day-start=02:00").day_start, (2, 0));
        assert_eq!(p("5m:1d 1d:30d; day-start=07:00").day_start, (7, 0));
        assert!(err("5m:1d 1d:30d; day-start=07:30")
            .msg
            .contains("whole hour"));
        // With a 1h tier every whole hour is a multiple.
        assert!(SnapPolicy::parse("1h:7d 1d:30d; day-start=02:00").is_ok());
        // With a coarser hourly tier it must be a multiple of it.
        assert!(err("6h:7d 1d:30d; day-start=02:00")
            .msg
            .contains("multiple of the finest hourly tier `6h`"));
        assert!(SnapPolicy::parse("6h:7d 1d:30d; day-start=06:00").is_ok());
        assert!(SnapPolicy::parse("2h:7d 1d:30d; day-start=02:00").is_ok());
        assert!(err("2h:7d 1d:30d; day-start=03:00").msg.contains("2h"));
        // A one-or-two digit hour parses; the canonical form pads.
        assert_eq!(
            p("1d:30d; day-start=2:00").to_string(),
            "1d:30d; day-start=02:00"
        );
        assert!(err("1d:30d; day-start=24:00").msg.contains("time of day"));
        assert!(err("1d:30d; day-start=0200").msg.contains("HH:MM"));
    }

    #[test]
    fn paused_is_a_flag_and_prints_last() {
        let pol = p("paused 1d:30d");
        assert!(pol.paused);
        assert_eq!(pol.to_string(), "1d:30d; paused");
        assert!(err("1d:30d paused paused")
            .msg
            .contains("duplicate `paused`"));
    }

    #[test]
    fn budget_is_parsed_but_unused() {
        assert_eq!(
            p("1d:30d; budget=500G").budget,
            Some(500 * 1024 * 1024 * 1024)
        );
        assert_eq!(p("1d:30d; budget=500G").to_string(), "1d:30d; budget=500G");
        assert!(err("1d:30d; budget=0").msg.contains("cannot be 0"));
        assert!(err("1d:30d; budget=5X").msg.contains("size unit"));
    }

    // --- the Step 11 rejection list, each with its byte offset ---

    /// Assert the error offset and that the caret lands on `token`.
    fn at(src: &str, want_off: usize, token: &str) -> PolicyError {
        let e = err(src);
        assert_eq!(
            e.offset, want_off,
            "offset for {src:?}: {} (msg: {})",
            e.offset, e.msg
        );
        assert!(
            src[e.offset..].starts_with(token),
            "caret for {src:?} at {} should point at {token:?}, got {:?} ({})",
            e.offset,
            &src[e.offset..],
            e.msg
        );
        e
    }

    #[test]
    fn rejects_interval_that_does_not_divide_an_hour() {
        let e = at("7m:1d", 0, "7m");
        assert!(e.msg.contains("does not divide an hour"), "{}", e.msg);
        assert!(e.msg.contains("5m"), "{}", e.msg);
    }

    #[test]
    fn rejects_interval_that_does_not_divide_a_day() {
        let e = at("5h:1d", 0, "5h");
        assert!(e.msg.contains("does not divide a day"), "{}", e.msg);
    }

    #[test]
    fn rejects_capital_m_with_both_readings() {
        let e = at("1M:1y", 0, "1M");
        assert!(e.msg.contains("1mo") && e.msg.contains("1m"), "{}", e.msg);
        // Also in a keep position.
        let e = at("1d:6M", 3, "6M");
        assert!(e.msg.contains("6mo") && e.msg.contains("6m"), "{}", e.msg);
    }

    #[test]
    fn rejects_bare_minute_keep_beside_a_monthly_tier() {
        let e = at("5m:6m 1mo:1y", 3, "6m");
        assert!(e.msg.contains("6mo") && e.msg.contains("6min"), "{}", e.msg);
        // A yearly tier makes months just as plausible.
        assert!(SnapPolicy::parse("5m:6m 1y:*").is_err());
        // With no monthly tier a bare-`m` keep is unambiguous.
        assert_eq!(p("5m:6m").to_string(), "5m:6min");
        // And `min` is the escape hatch that works everywhere.
        assert_eq!(p("5m:6min 1mo:1y").to_string(), "5m:6min 1mo:1y");
    }

    #[test]
    fn rejects_keep_shorter_than_every() {
        let e = at("1h:30m", 3, "30m");
        assert!(e.msg.contains("shorter than one `1h`"), "{}", e.msg);
    }

    /// `1d:6m` is how an operator writes "daily, keep six months". It is
    /// not caught by the ambiguity rule (there is no monthly tier), so
    /// the `keep >= every` refusal has to name both readings itself.
    #[test]
    fn keep_shorter_than_every_names_both_readings_for_a_bare_minute_keep() {
        for (src, off, tok, n) in [
            ("1d:6m", 3, "6m", "6"),
            ("1w:6m", 3, "6m", "6"),
            ("12h:600m", 4, "600m", "600"),
        ] {
            let e = at(src, off, tok);
            assert!(e.msg.contains("shorter than one"), "{src}: {}", e.msg);
            assert!(
                e.msg.contains(&format!("`{n}mo` for {n} months")),
                "{src}: {}",
                e.msg
            );
            assert!(
                e.msg.contains(&format!("`{n}min` for {n} minutes")),
                "{src}: {}",
                e.msg
            );
        }
        // The explicit spellings are not ambiguous and keep the plain
        // message: `6min` really is shorter than a day, `6mo` is not.
        let e = err("1d:6min");
        assert!(e.msg.contains("shorter than one `1d`"), "{}", e.msg);
        assert!(!e.msg.contains("ambiguous"), "{}", e.msg);
        assert_eq!(p("1d:6mo").to_string(), "1d:6mo");
    }

    /// `steady_state_bound` divides by the interval's nominal length. A
    /// hand-built zero interval (the parser refuses `0m`) used to panic.
    #[test]
    fn steady_state_bound_survives_a_hand_built_zero_interval() {
        let mut policy = p("1d:7d");
        policy.tiers = vec![Tier {
            every: Interval::Minutes(0),
            keep: Keep::Days(1),
        }];
        assert_eq!(policy.steady_state_bound(), None);
        // A zero interval after a well-formed tier, with a finite keep,
        // so the division (not `*`'s early `None`) is what refuses it.
        policy.tiers = vec![
            Tier {
                every: Interval::Day,
                keep: Keep::Days(7),
            },
            Tier {
                every: Interval::Seconds(0),
                keep: Keep::Hours(1),
            },
        ];
        assert_eq!(policy.steady_state_bound(), None);
        policy.tiers.truncate(1);
        assert_eq!(policy.steady_state_bound(), Some(7));
        // The parser itself can never build one.
        assert!(SnapPolicy::parse("0m:1h").is_err());
        assert!(SnapPolicy::parse("0s:1h").is_err());
    }

    #[test]
    fn rejects_duplicate_interval() {
        let e = at("1h:1d 1h:2d", 6, "1h:2d");
        assert!(e.msg.contains("duplicate `1h` tier"), "{}", e.msg);
    }

    #[test]
    fn rejects_last_zero() {
        let e = at("1d:7d; last=0", 12, "0");
        assert!(e.msg.contains("at least 1"), "{}", e.msg);
    }

    #[test]
    fn rejects_unknown_timezone() {
        let e = at("1d:7d; tz=Mars/Phobos", 10, "Mars/Phobos");
        assert!(e.msg.contains("unknown timezone"), "{}", e.msg);
    }

    #[test]
    fn rejects_day_start_off_the_hourly_grid() {
        let e = at("1h:1d; day-start=02:30", 17, "02:30");
        assert!(e.msg.contains("whole hour"), "{}", e.msg);
    }

    #[test]
    fn paused_plus_unknown_key_reports_the_key() {
        let e = at("1d:7d; paused; bogus=1", 15, "bogus");
        assert!(e.msg.contains("unknown setting `bogus`"), "{}", e.msg);
        // Order within the expression does not matter.
        let e = at("bogus=1; paused; 1d:7d", 0, "bogus");
        assert!(e.msg.contains("unknown setting `bogus`"), "{}", e.msg);
    }

    #[test]
    fn rejects_empty_policy() {
        let e = err("");
        assert_eq!(e.offset, 0);
        assert!(e.msg.contains("empty policy"), "{}", e.msg);
        for blank in ["   ", ";", " ; ; "] {
            let e = err(blank);
            assert_eq!(e.offset, 0, "{blank:?}");
            assert!(e.msg.contains("empty policy"), "{}", e.msg);
        }
        // Settings without a tier are not a policy either.
        let e = err("tz=UTC");
        assert_eq!(e.offset, 0);
        assert!(e.msg.contains("no tier"), "{}", e.msg);
        let e = err("paused");
        assert!(e.msg.contains("no tier"), "{}", e.msg);
    }

    #[test]
    fn other_rejections_point_at_their_token() {
        assert!(at("1d:7d; 1x:3d", 7, "1x")
            .msg
            .contains("unknown interval unit"));
        assert!(at("1d:7d; 1h:3x", 10, "3x")
            .msg
            .contains("unknown keep unit"));
        assert!(at("1d:7d; 1h:30s", 10, "30s")
            .msg
            .contains("not a keep window"));
        assert!(at("1d:7d; 3d:30d", 7, "3d").msg.contains("does not tile"));
        assert!(at("1d:7d; 5s:1h", 7, "5s").msg.contains("sub-minute"));
        assert!(at("1d:7d; 7s:1h", 7, "7s").msg.contains("sub-minute"));
        assert!(at("1d:7d; 0m:1h", 7, "0m").msg.contains("cannot be 0"));
        assert!(at("1d:7d; 1h:0d", 10, "0d").msg.contains("cannot be 0"));
        assert!(at("1d:7d; :3d", 7, ":3d").msg.contains("no interval"));
        assert!(at("1d:7d; 1h:", 10, "").msg.contains("no keep window"));
        assert!(at("1d:7d; tz=", 10, "").msg.contains("no value"));
        assert!(at("1d:7d; tz", 7, "tz").msg.contains("needs a value"));
        assert!(at("1d:7d; 1h", 7, "1h").msg.contains("no keep window"));
        assert!(at("1d:7d; nonsense", 7, "nonsense")
            .msg
            .contains("unknown clause"));
        assert!(at("1d:7d; tz=UTC; tz=CET", 15, "tz")
            .msg
            .contains("duplicate setting"));
        assert!(at("1d:7d; week-start=tue", 18, "tue").msg.contains("mon"));
        assert!(at("1d:7d; skip-empty=maybe", 18, "maybe")
            .msg
            .contains("yes"));
        assert!(at("1d:7d; last=x", 12, "x").msg.contains("integer"));
        // A count that does not fit a u32 is a range error, not a panic.
        assert!(at("1d:99999999999d", 3, "99999999999d")
            .msg
            .contains("out of range"));
    }

    #[test]
    fn caret_rendering_matches_the_prune_convention() {
        let src = "1h:1d 7m:1d";
        let e = err(src);
        assert_eq!(e.render(src), "1h:1d 7m:1d\n      ^ ".to_string() + &e.msg);
    }

    // --- properties ---

    /// Every interval the grammar accepts, in canonical form.
    fn all_intervals() -> Vec<Interval> {
        let mut v: Vec<Interval> = Vec::new();
        for n in [10u32, 12, 15, 20, 30] {
            v.push(Interval::Seconds(n));
        }
        for n in [1u32, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30] {
            v.push(Interval::Minutes(n));
        }
        for n in [1u32, 2, 3, 4, 6, 8, 12] {
            v.push(Interval::Hours(n));
        }
        v.extend([
            Interval::Day,
            Interval::Week,
            Interval::Month,
            Interval::Year,
        ]);
        v
    }

    fn all_keeps() -> Vec<Keep> {
        let mut v = vec![Keep::Forever];
        for n in [1u32, 2, 7, 30, 90, 366] {
            v.push(Keep::Minutes(n));
            v.push(Keep::Hours(n));
            v.push(Keep::Days(n));
            v.push(Keep::Weeks(n));
            v.push(Keep::Months(n));
            v.push(Keep::Years(n));
        }
        v
    }

    /// A generated policy: a non-empty subset of distinct intervals, each
    /// with a keep that covers it, plus settings consistent with the
    /// `day-start` rule.
    fn arb_policy() -> impl Strategy<Value = SnapPolicy> {
        let intervals = all_intervals();
        let keeps = all_keeps();
        (
            proptest::collection::vec(0..intervals.len(), 1..6),
            proptest::collection::vec(0..keeps.len(), 1..6),
            prop::option::of(prop::sample::select(vec![
                "UTC",
                "Europe/Budapest",
                "Asia/Kolkata",
                "Pacific/Chatham",
                "Etc/UTC",
            ])),
            prop::option::of(0u8..24),
            any::<bool>(),
            1u32..1000,
            any::<bool>(),
            prop::option::of(1u64..(1 << 50)),
            any::<bool>(),
        )
            .prop_map(
                move |(ivs, ks, tz, hour, sunday, last, skip_empty, budget, paused)| {
                    let mut tiers: Vec<Tier> = Vec::new();
                    for (i, &iv) in ivs.iter().enumerate() {
                        let every = intervals[iv];
                        if tiers.iter().any(|t| t.every == every) {
                            continue;
                        }
                        // Pick the first generated keep that covers this
                        // interval, falling back to `*`.
                        let keep = (0..keeps.len())
                            .map(|j| keeps[(ks[i % ks.len()] + j) % keeps.len()])
                            .find(|&k| keep_covers_every(every, k))
                            .unwrap_or(Keep::Forever);
                        tiers.push(Tier { every, keep });
                    }
                    tiers.sort_by_key(|t| t.every);
                    let finest_hourly = tiers
                        .iter()
                        .filter_map(|t| match t.every {
                            Interval::Hours(n) => Some(n),
                            _ => None,
                        })
                        .min();
                    let day_start = hour.map(|h| {
                        let h = match finest_hourly {
                            Some(n) => (u32::from(h) / n * n) as u8,
                            None => h,
                        };
                        (h, 0u8)
                    });
                    SnapPolicy {
                        tiers,
                        tz: tz.unwrap_or(DEFAULT_TZ).to_string(),
                        day_start: day_start.unwrap_or(DEFAULT_DAY_START),
                        week_start: if sunday {
                            WeekStart::Sun
                        } else {
                            WeekStart::Mon
                        },
                        last,
                        skip_empty,
                        budget,
                        paused,
                    }
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// The round-trip the plan calls for: the canonical form of a
        /// policy parses back to that exact policy.
        #[test]
        fn canonical_form_round_trips(pol in arb_policy()) {
            let printed = pol.to_string();
            let reparsed = SnapPolicy::parse(&printed)
                .map_err(|e| TestCaseError::fail(format!("{printed:?}: {e}")))?;
            prop_assert_eq!(&reparsed, &pol, "via {:?}", printed);
            prop_assert_eq!(reparsed.to_string(), printed);
        }

        /// `parse(s).map(to_string)` is idempotent over arbitrary input:
        /// printing a parsed policy and parsing it again yields the same
        /// policy and the same text.
        #[test]
        fn printing_is_idempotent_over_arbitrary_text(src in policy_ish()) {
            if let Ok(pol) = SnapPolicy::parse(&src) {
                let once = pol.to_string();
                let again = SnapPolicy::parse(&once)
                    .map_err(|e| TestCaseError::fail(format!("{once:?}: {e}")))?;
                prop_assert_eq!(&again, &pol, "src {:?} printed {:?}", src, once);
                prop_assert_eq!(again.to_string(), once);
            }
        }

        /// `parse` is total: it never panics, however mangled the input.
        #[test]
        fn parse_never_panics(src in ".{0,64}") {
            let _ = SnapPolicy::parse(&src);
        }
    }

    /// Policy-shaped text: real fragments, mangled fragments and soup,
    /// so the idempotency property sees plenty of inputs that parse.
    fn policy_ish() -> impl Strategy<Value = String> {
        let frag = prop_oneof![
            prop::sample::select(vec![
                "5m:1d",
                "1h:7d",
                "1d:30d",
                "1mo:1y",
                "1w:12w",
                "1y:*",
                "30s:10min",
                "1m:1h",
                "12h:7d",
                "7m:1d",
                "5h:1d",
                "1M:1y",
                "1h:30m",
                "6m",
                "0m:1h",
                "1h:",
                "tz=Europe/Budapest",
                "tz=utc",
                "tz=Nowhere",
                "day-start=02:00",
                "day-start=02:30",
                "week-start=sun",
                "week-start=mon",
                "last=3",
                "last=0",
                "skip-empty=no",
                "skip-empty=yes",
                "budget=500G",
                "paused",
                "bogus=1",
                "",
            ])
            .prop_map(String::from),
            "[0-9]{0,3}(s|m|min|h|d|w|mo|y|M)?(:([0-9]{0,3}(m|min|h|d|w|mo|y)?|\\*))?"
                .prop_map(|s| s),
            "[0-9a-z:=;*/ -]{0,12}".prop_map(|s| s),
        ];
        proptest::collection::vec(frag, 0..6).prop_map(|parts| parts.join(" "))
    }

    #[test]
    fn fuzz_corpus_never_panics() {
        // The same deterministic byte soup the prune parser uses, so
        // `cargo test` covers totality even without `cargo fuzz`.
        let mut state: u64 = 0x9e3779b97f4a7c15;
        let alphabet = b"0123456789smhdwoyM:;=* -paused_tzlkbgintx.";
        for _ in 0..20_000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = (state >> 24) as usize % 24;
            let mut s = String::new();
            let mut st = state;
            for _ in 0..len {
                st = st.wrapping_mul(6364136223846793005).wrapping_add(1);
                s.push(alphabet[(st >> 33) as usize % alphabet.len()] as char);
            }
            if let Ok(pol) = SnapPolicy::parse(&s) {
                let printed = pol.to_string();
                assert_eq!(
                    SnapPolicy::parse(&printed).as_ref(),
                    Ok(&pol),
                    "canonical form {printed:?} of {s:?} must re-parse"
                );
            }
        }
    }
}
