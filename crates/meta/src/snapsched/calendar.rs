//! Where a bucket starts (plan 32, Step 2).
//!
//! Retention never asks "how old is this snapshot". It asks "which
//! bucket does it fall in", and keeps the oldest snapshot of each
//! bucket inside a window (L3–L5). Everything calendar-shaped about
//! that question lives here, as three pure integer functions over Unix
//! milliseconds: [`bucket_start`], [`next_bucket_start`] and
//! [`subtract_keep`] (with its mirror [`add_keep`], which the
//! `expires_at` forecast needs).
//!
//! No clock, no environment, no locale. The only table consulted is the
//! IANA tzdb **bundled into the binary** by `jiff` — the same rule the
//! parser follows, for the same reason: one node evaluates retention for
//! the whole cluster, and a node that placed bucket boundaries
//! differently would delete different snapshots.
//!
//! # The two alignment classes
//!
//! * **Sub-hour** ([`Interval::Seconds`], [`Interval::Minutes`]) aligns
//!   on the **absolute clock**: a 5-minute bucket starts when the Unix
//!   epoch has run a whole multiple of 5 minutes, in every zone at
//!   once. `day_start` and `week_start` do not apply.
//! * **One hour and coarser** aligns in the policy's **timezone**.
//!   An hour bucket starts at local `HH:00`; day, week, month and year
//!   buckets start at `day_start` on the local date (ISO weeks, Monday,
//!   unless `week_start=sun`).
//!
//! The split is the plan's (L4: an explicit timezone for everything
//! calendar-shaped) and it is what makes a daily bucket mean "a day in
//! Budapest" rather than "a day in UTC". Its visible consequence is in
//! zones whose offset is not a whole hour:
//!
//! * `Asia/Kolkata` is `+05:30`, so a 1h bucket starts at local `HH:00`
//!   = absolute `HH:30`, while a 30m bucket starts at absolute `HH:00`
//!   and `HH:30`. The two grids still nest: 30 is a multiple of 30.
//!   A 20m bucket, however, starts at absolute `HH:00`/`:20`/`:40`,
//!   i.e. local `HH:30`/`:50`/`HH+1:10` — it straddles the hour.
//! * `Pacific/Chatham` is `+12:45`. There a 5m or 15m bucket nests
//!   inside the hour and a 10m, 20m or 30m one does not.
//!
//! The rule, stated once: a sub-hour interval `n` tiles the hourly grid
//! exactly when the zone's offset-past-the-hour is a multiple of `n`.
//! Whole-hour zones (most of the world, and the default `UTC`) always
//! nest. This is documented rather than prevented: preventing it would
//! make a policy's validity depend on its timezone's current offset,
//! which the tzdb is free to change.
//!
//! # Buckets are identified by their label
//!
//! A DST transition makes "the local hour" and "the local date" behave
//! differently, and the plan asks for both behaviours:
//!
//! * An **hour** bucket is identified by its local hour *and the offset
//!   it happened at*. In `Europe/Budapest` the 25-hour October day has
//!   **25** hour buckets (local 02:00 occurs twice, as two distinct
//!   buckets) and the 23-hour March day has **23** (local 02:00 does not
//!   occur at all).
//! * A **day**, week, month or year bucket is identified by its
//!   calendar date, which happens exactly once. Both of those Budapest
//!   days hold exactly **one** daily bucket, 25 and 23 hours long.
//!
//! # A `day_start` that does not exist
//!
//! `day-start=02:00` names a local time that a spring-forward day may
//! skip entirely. Such a boundary resolves by `jiff`'s **compatible**
//! disambiguation: the nonexistent local time is read with the offset
//! in force *before* the gap, which moves it **forward by the gap's
//! length**. For a whole-hour `day_start` inside a one-hour gap that is
//! the first instant after the gap (local 03:00 in `Europe/Budapest`),
//! but it is not "the first instant at or after it" in general: in
//! `Pacific/Chatham`, whose 2026-09-27 gap runs 02:45 → 03:45,
//! `day-start=03:00` resolves to local **04:00** +13:45, fifteen minutes
//! after the gap closes. Either way the boundary is one instant per
//! date, so the buckets still tile the line and that day still has
//! exactly one daily bucket, shorter by the gap. On the fall-back day,
//! where 02:00 happens twice, a day boundary takes the **earlier** of
//! the two, so the day is 25 hours long and there is still one of it.
//!
//! # Keep windows
//!
//! [`subtract_keep`] counts `min`/`h` as exact durations, `d`/`w` as
//! exact durations for a sub-day tier and as calendar days in the zone
//! for a `1d`-or-coarser one, and `mo`/`y` as calendar months. Each
//! window therefore holds a whole number of its own tier's buckets on
//! every day of the year: `5m:1d` is 288 buckets and `1d:7d` is 7, DST
//! or not.
//!
//! # Totality
//!
//! Every function here is total: timestamps outside the range `jiff` can
//! represent are clamped into it, and the (unreachable for a real
//! snapshot) cases where calendar arithmetic would overflow fall back to
//! absolute arithmetic on the interval's nominal length. A retention
//! verdict must never panic — it runs over whatever the replica holds.

use jiff::civil::{Date, DateTime, Weekday};
use jiff::tz::{AmbiguousOffset, Offset, TimeZone, TimeZoneDatabase};
use jiff::{Span, Timestamp};

use super::policy::{Interval, Keep, SnapPolicy, WeekStart};

/// Look up a timezone in the **bundled** tzdb only, the way
/// [`SnapPolicy::parse`] validates `tz=`.
///
/// `None` for a name the bundled database does not know, which a parsed
/// policy cannot carry (`parse` refuses it, and stores the canonical
/// spelling). [`crate::snapsched::retention::evaluate`] treats `None` as
/// "this policy cannot be evaluated", and keeps everything.
pub fn time_zone(name: &str) -> Option<TimeZone> {
    use std::sync::OnceLock;
    static DB: OnceLock<TimeZoneDatabase> = OnceLock::new();
    if name == "UTC" {
        // The default, and the one every test uses: skip the lookup.
        return Some(TimeZone::UTC);
    }
    let db = DB.get_or_init(TimeZoneDatabase::bundled);
    db.get(name).ok().filter(|tz| !tz.is_unknown())
}

/// The timezone a policy's buckets are aligned in, or `None` when its
/// `tz` is not in the bundled tzdb.
pub fn policy_time_zone(policy: &SnapPolicy) -> Option<TimeZone> {
    time_zone(&policy.tz)
}

/// The start of the `interval` bucket containing `t_unix_ms`, in Unix
/// milliseconds. Always `<= t_unix_ms`, and non-decreasing in
/// `t_unix_ms` (the property the representative-per-bucket scan relies
/// on).
///
/// `day_start` is `(hour, minute)` and `week_start` the weekday a `1w`
/// bucket begins on; both are ignored by sub-hour intervals. See the
/// module docs for the two alignment classes and for how a `day_start`
/// inside a DST gap resolves.
pub fn bucket_start(
    interval: Interval,
    t_unix_ms: i64,
    tz: &TimeZone,
    day_start: (u8, u8),
    week_start: WeekStart,
) -> i64 {
    match absolute_width_ms(interval) {
        Some(width) => floor_to(t_unix_ms, width),
        None => local_bucket_start(interval, t_unix_ms, tz, day_start, week_start),
    }
}

/// The start of the bucket *after* the one containing `t_unix_ms`.
/// Strictly greater than [`bucket_start`] of the same arguments, so
/// iterating it enumerates every bucket exactly once — including both
/// halves of a fold hour, and skipping the local hour a spring-forward
/// swallowed.
pub fn next_bucket_start(
    interval: Interval,
    t_unix_ms: i64,
    tz: &TimeZone,
    day_start: (u8, u8),
    week_start: WeekStart,
) -> i64 {
    if let Some(width) = absolute_width_ms(interval) {
        return floor_to(t_unix_ms, width).saturating_add(width);
    }
    let here = local_bucket_start(interval, t_unix_ms, tz, day_start, week_start);
    let Some(slot) = local_slot(interval, here, tz, day_start, week_start) else {
        return here.saturating_add(nominal_ms(interval));
    };
    // The next boundary is the smallest realized one past this bucket's
    // start — which can be the *other half* of a fold hour (the 25-hour
    // day's second local 02:00) rather than the next label.
    boundaries_near(interval, slot, tz, LABEL_WINDOW)
        .into_iter()
        .find(|ms| *ms > here)
        .unwrap_or_else(|| here.saturating_add(nominal_ms(interval)))
}

/// `t_unix_ms` minus one `keep` window, the lower edge of a tier `every:keep`'s
/// retention window. `None` for [`Keep::Forever`], which has no edge.
///
/// How a unit is subtracted depends on the tier's own interval, because
/// the window must hold a whole number of that tier's buckets:
///
/// * `min` and `h` are always **exact durations**.
/// * `d` and `w` are **exact durations for a sub-day tier** — `5m:1d`
///   must keep exactly 288 buckets and `1h:1d` exactly 24, which
///   24 × 3600 s does and "a calendar day" does not on a 23- or 25-hour
///   day — and **calendar days in `tz` for a `1d`-or-coarser tier**,
///   whose buckets are calendar days: `1d:7d` keeps 7 daily buckets on
///   either side of a DST change, where 7 × 24 h would reach one hour
///   into an eighth after spring-forward and keep it for a whole week.
/// * `mo` and `y` are **calendar months in `tz`** ("one month before
///   31 March" is 28 or 29 February, never "30 days ago"), clamping to
///   the end of a short month the way every calendar does.
///
/// Calendar shifts land on a local time that may not exist; it resolves
/// the way a `day_start` does (compatible disambiguation, module docs).
pub fn subtract_keep(t_unix_ms: i64, keep: Keep, every: Interval, tz: &TimeZone) -> Option<i64> {
    shift_keep(t_unix_ms, keep, every, tz, -1)
}

/// `t_unix_ms` plus one `keep` window, by the same rules as
/// [`subtract_keep`]. `None` for [`Keep::Forever`]. Used by the
/// `expires_at` forecast, never by the keep/expire decision.
pub fn add_keep(t_unix_ms: i64, keep: Keep, every: Interval, tz: &TimeZone) -> Option<i64> {
    shift_keep(t_unix_ms, keep, every, tz, 1)
}

/// [`bucket_start`] with the policy's own timezone, `day_start` and
/// `week_start`. The scheduler's "is this root due" check (Step 3.3)
/// and the simulator both want exactly this; nothing else should need to
/// re-derive a policy's grid.
///
/// Falls back to UTC when the policy's `tz` is not in the bundled tzdb,
/// which [`SnapPolicy::parse`] cannot produce.
pub fn policy_bucket_start(policy: &SnapPolicy, interval: Interval, t_unix_ms: i64) -> i64 {
    let tz = policy_time_zone(policy).unwrap_or(TimeZone::UTC);
    bucket_start(
        interval,
        t_unix_ms,
        &tz,
        policy.day_start,
        policy.week_start,
    )
}

/// [`next_bucket_start`] with the policy's own settings.
pub fn policy_next_bucket_start(policy: &SnapPolicy, interval: Interval, t_unix_ms: i64) -> i64 {
    let tz = policy_time_zone(policy).unwrap_or(TimeZone::UTC);
    next_bucket_start(
        interval,
        t_unix_ms,
        &tz,
        policy.day_start,
        policy.week_start,
    )
}

// --- internals -------------------------------------------------------

/// `Some(width in ms)` for the intervals that align on the absolute
/// clock, `None` for the ones aligned in a timezone. A zero-width
/// interval (hand-built; the parser refuses `0m`) counts as 1 ms, so
/// `floor_to` never divides by zero.
fn absolute_width_ms(interval: Interval) -> Option<i64> {
    let secs = match interval {
        Interval::Seconds(n) => i64::from(n),
        Interval::Minutes(n) => i64::from(n) * 60,
        _ => return None,
    };
    Some((secs * 1_000).max(1))
}

/// The interval's nominal length in ms (month = 30 d, year = 365 d),
/// the fallback step when calendar arithmetic cannot be done at all.
fn nominal_ms(interval: Interval) -> i64 {
    (interval.nominal_duration() as i64)
        .saturating_mul(1_000)
        .max(1)
}

/// Floor to a multiple of `width`, correctly for pre-1970 timestamps.
fn floor_to(t: i64, width: i64) -> i64 {
    t.saturating_sub(t.rem_euclid(width))
}

/// Clamp into the range `jiff` can represent, so no conversion fails on
/// a corrupt or adversarial `created_unix_ms`.
fn clamp_ts(t_unix_ms: i64) -> Timestamp {
    match Timestamp::from_millisecond(t_unix_ms) {
        Ok(ts) => ts,
        Err(_) if t_unix_ms < 0 => Timestamp::MIN,
        Err(_) => Timestamp::MAX,
    }
}

/// The civil datetime of a bucket boundary: the slot *label*. Two
/// instants can share one label (a fold hour) and a label can have no
/// instant at all (a gap).
fn local_slot(
    interval: Interval,
    t_unix_ms: i64,
    tz: &TimeZone,
    day_start: (u8, u8),
    week_start: WeekStart,
) -> Option<DateTime> {
    let ts = clamp_ts(t_unix_ms);
    let local = tz.to_offset(ts).to_datetime(ts);
    slot_of(interval, local, day_start, week_start)
}

/// The slot label of the bucket containing the local datetime `local`.
fn slot_of(
    interval: Interval,
    local: DateTime,
    day_start: (u8, u8),
    week_start: WeekStart,
) -> Option<DateTime> {
    let (ds_h, ds_m) = (
        i8::try_from(day_start.0).ok()?,
        i8::try_from(day_start.1).ok()?,
    );
    match interval {
        // Hour buckets align on local midnight, never on `day_start`:
        // the parser already forces `day_start` onto a multiple of the
        // finest hourly tier, so the day boundary is always one of them.
        Interval::Hours(n) => {
            let n = i8::try_from(n.max(1)).unwrap_or(1);
            let hour = (local.hour() / n) * n;
            Some(local.date().at(hour, 0, 0, 0))
        }
        Interval::Day => Some(logical_date(local, day_start)?.at(ds_h, ds_m, 0, 0)),
        Interval::Week => {
            let date = logical_date(local, day_start)?;
            let target = match week_start {
                WeekStart::Mon => Weekday::Monday,
                WeekStart::Sun => Weekday::Sunday,
            };
            let back = i64::from(
                (date.weekday().to_monday_zero_offset() - target.to_monday_zero_offset())
                    .rem_euclid(7),
            );
            let date = date.checked_sub(Span::new().days(back)).ok()?;
            Some(date.at(ds_h, ds_m, 0, 0))
        }
        Interval::Month => Some(
            logical_date(local, day_start)?
                .first_of_month()
                .at(ds_h, ds_m, 0, 0),
        ),
        Interval::Year => Some(
            logical_date(local, day_start)?
                .first_of_year()
                .at(ds_h, ds_m, 0, 0),
        ),
        Interval::Seconds(_) | Interval::Minutes(_) => None,
    }
}

/// Which `day_start`-to-`day_start` day a local datetime belongs to.
/// Before `day_start` it is still yesterday's.
fn logical_date(local: DateTime, day_start: (u8, u8)) -> Option<Date> {
    let now = (local.hour() as i32, local.minute() as i32);
    let start = (i32::from(day_start.0), i32::from(day_start.1));
    if now < start {
        local.date().yesterday().ok()
    } else {
        Some(local.date())
    }
}

/// Move a slot label `by` whole intervals.
fn advance_slot(interval: Interval, slot: DateTime, by: i64) -> Option<DateTime> {
    let span = match interval {
        Interval::Hours(n) => Span::new().hours(by * i64::from(n.max(1))),
        Interval::Day => Span::new().days(by),
        Interval::Week => Span::new().days(by * 7),
        Interval::Month => Span::new().months(by),
        Interval::Year => Span::new().years(by),
        Interval::Seconds(_) | Interval::Minutes(_) => return None,
    };
    slot.checked_add(span).ok()
}

/// Every instant a slot label denotes, oldest first: one normally, two
/// in a fold hour, and for a gap the first instant past it.
///
/// Day and coarser labels collapse to a single instant (the earlier of a
/// fold), because a calendar date happens once — see the module docs.
fn slot_instants(interval: Interval, slot: DateTime, tz: &TimeZone) -> Vec<i64> {
    let at = |offset: Offset| {
        offset
            .to_timestamp(slot)
            .ok()
            .map(Timestamp::as_millisecond)
    };
    let folds_split = matches!(interval, Interval::Hours(_));
    match tz.to_ambiguous_timestamp(slot).offset() {
        AmbiguousOffset::Unambiguous { offset } => at(offset).into_iter().collect(),
        // A local time that does not exist starts where it resumes.
        AmbiguousOffset::Gap { before, .. } => at(before).into_iter().collect(),
        AmbiguousOffset::Fold { before, after } => {
            if folds_split {
                let mut out: Vec<i64> = [at(before), at(after)].into_iter().flatten().collect();
                out.sort_unstable();
                out.dedup();
                out
            } else {
                at(before).into_iter().collect()
            }
        }
    }
}

/// How many slot labels either side of the one `t` names are consulted
/// when a transition makes the obvious label the wrong one. Three
/// covers every shift the tzdb contains, including the zones that moved
/// a whole day across the date line (`Pacific/Apia`, 2011).
const LABEL_WINDOW: i64 = 3;

/// Every bucket boundary near `t`: the realized instants of the slot
/// labels within [`LABEL_WINDOW`] of `t`'s own, sorted and deduped.
fn boundaries_near(interval: Interval, slot: DateTime, tz: &TimeZone, window: i64) -> Vec<i64> {
    let mut out = Vec::with_capacity(8);
    for k in -window..=window {
        let Some(label) = advance_slot(interval, slot, k) else {
            continue;
        };
        out.extend(slot_instants(interval, label, tz));
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// The greatest bucket boundary at or before `t_unix_ms`.
///
/// The slot label derived from the offset *at* `t_unix_ms` is the right
/// one in every zone whose transitions land on the grid. Where they do
/// not — `Pacific/Chatham` moves by an hour at local 03:45, so the
/// instant just before local 03:00 STD carries the label 02:00, whose
/// only realization is an hour *earlier* — the boundary belongs to a
/// neighbouring label, and the wider window finds it. The guard is
/// cheap: a transition between the candidate and `t` changes the offset.
fn local_bucket_start(
    interval: Interval,
    t_unix_ms: i64,
    tz: &TimeZone,
    day_start: (u8, u8),
    week_start: WeekStart,
) -> i64 {
    let ts = clamp_ts(t_unix_ms);
    let offset_at_t = tz.to_offset(ts);
    let Some(slot) = slot_of(interval, offset_at_t.to_datetime(ts), day_start, week_start) else {
        return floor_to(t_unix_ms, nominal_ms(interval));
    };
    let pick = |window: i64| {
        boundaries_near(interval, slot, tz, window)
            .into_iter()
            .filter(|ms| *ms <= t_unix_ms)
            .max()
    };
    if let Some(b) = pick(0) {
        if tz.to_offset(clamp_ts(b)) == offset_at_t {
            // No transition between the boundary and `t`, so local time
            // ran uniformly and this label really is the latest one.
            return b;
        }
    }
    pick(LABEL_WINDOW).unwrap_or_else(|| floor_to(t_unix_ms, nominal_ms(interval)))
}

fn shift_keep(
    t_unix_ms: i64,
    keep: Keep,
    every: Interval,
    tz: &TimeZone,
    sign: i64,
) -> Option<i64> {
    // Day-or-coarser tiers count `d`/`w` in calendar days; see
    // `subtract_keep`.
    let calendar_days = matches!(
        every,
        Interval::Day | Interval::Week | Interval::Month | Interval::Year
    );
    let span = match keep {
        Keep::Forever => return None,
        Keep::Minutes(n) => return Some(shift_ms(t_unix_ms, sign, i64::from(n) * 60_000)),
        Keep::Hours(n) => return Some(shift_ms(t_unix_ms, sign, i64::from(n) * 3_600_000)),
        Keep::Days(n) if !calendar_days => {
            return Some(shift_ms(t_unix_ms, sign, i64::from(n) * 86_400_000));
        }
        Keep::Weeks(n) if !calendar_days => {
            return Some(shift_ms(t_unix_ms, sign, i64::from(n) * 604_800_000));
        }
        Keep::Days(n) => Span::new().try_days(sign * i64::from(n)),
        Keep::Weeks(n) => Span::new().try_days(sign * i64::from(n) * 7),
        Keep::Months(n) => Span::new().try_months(sign * i64::from(n)),
        Keep::Years(n) => Span::new().try_months(sign * i64::from(n) * 12),
    };
    let nominal = || {
        let secs = keep.nominal_duration().unwrap_or(0) as i64;
        shift_ms(t_unix_ms, sign, secs.saturating_mul(1_000))
    };
    let ts = clamp_ts(t_unix_ms);
    let offset = tz.to_offset(ts);
    let local = offset.to_datetime(ts);
    let Ok(shifted) = span.and_then(|span| local.checked_add(span)) else {
        return Some(nominal());
    };
    // Compatible disambiguation, as for a `day_start` in a gap: one
    // month before a nonexistent local time is the instant it resumes.
    match tz.to_zoned(shifted) {
        Ok(zoned) => Some(zoned.timestamp().as_millisecond()),
        Err(_) => Some(nominal()),
    }
}

fn shift_ms(t: i64, sign: i64, by: i64) -> i64 {
    if sign < 0 {
        t.saturating_sub(by)
    } else {
        t.saturating_add(by)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDAPEST: &str = "Europe/Budapest";
    const KOLKATA: &str = "Asia/Kolkata";
    const CHATHAM: &str = "Pacific/Chatham";

    fn tz(name: &str) -> TimeZone {
        time_zone(name).unwrap_or_else(|| panic!("tzdb has no {name}"))
    }

    /// A local civil datetime as Unix ms, by compatible disambiguation.
    fn at(name: &str, y: i16, m: i8, d: i8, h: i8, min: i8) -> i64 {
        tz(name)
            .to_zoned(Date::new(y, m, d).unwrap().at(h, min, 0, 0))
            .unwrap()
            .timestamp()
            .as_millisecond()
    }

    fn bs(interval: Interval, t: i64, name: &str) -> i64 {
        bucket_start(interval, t, &tz(name), (0, 0), WeekStart::Mon)
    }

    fn nbs(interval: Interval, t: i64, name: &str) -> i64 {
        next_bucket_start(interval, t, &tz(name), (0, 0), WeekStart::Mon)
    }

    /// How many `interval` buckets start inside `[from, to)`.
    fn count_buckets(
        interval: Interval,
        from: i64,
        to: i64,
        name: &str,
        day_start: (u8, u8),
    ) -> usize {
        let z = tz(name);
        let mut t = bucket_start(interval, from, &z, day_start, WeekStart::Mon);
        assert_eq!(t, from, "`from` must itself be a bucket start");
        let mut n = 0;
        while t < to {
            n += 1;
            let next = next_bucket_start(interval, t, &z, day_start, WeekStart::Mon);
            assert!(next > t, "next_bucket_start must advance");
            t = next;
        }
        assert_eq!(t, to, "buckets must tile [from, to)");
        n
    }

    #[test]
    fn sub_hour_buckets_align_on_the_absolute_clock() {
        // 1970-01-01T00:07:30Z.
        let t = 7 * 60_000 + 30_000;
        assert_eq!(bs(Interval::Minutes(5), t, "UTC"), 5 * 60_000);
        assert_eq!(bs(Interval::Minutes(5), t, KOLKATA), 5 * 60_000);
        assert_eq!(bs(Interval::Seconds(10), t, CHATHAM), 7 * 60_000 + 30_000);
        // Negative (pre-epoch) times floor downwards, not towards zero.
        assert_eq!(bs(Interval::Minutes(5), -1, "UTC"), -5 * 60_000);
        assert_eq!(nbs(Interval::Minutes(5), -1, "UTC"), 0);
    }

    /// The plan wants hourly buckets aligned in local time, so in a zone
    /// whose offset is not a whole hour they are not aligned on the
    /// absolute clock. Both halves of that statement are load-bearing.
    #[test]
    fn hourly_buckets_start_at_local_hh00_in_half_hour_zones() {
        // 2026-06-15T10:20 in Kolkata (+05:30) → the 10:00 bucket,
        // which is 04:30Z, not 05:00Z.
        let t = at(KOLKATA, 2026, 6, 15, 10, 20);
        let b = bs(Interval::Hours(1), t, KOLKATA);
        assert_eq!(b, at(KOLKATA, 2026, 6, 15, 10, 0));
        assert_eq!(b.rem_euclid(3_600_000), 30 * 60_000);
        assert_eq!(nbs(Interval::Hours(1), t, KOLKATA) - b, 3_600_000);
        // Chatham is +12:45: the hour boundary sits at absolute :15.
        let t = at(CHATHAM, 2026, 6, 15, 10, 20);
        let b = bs(Interval::Hours(1), t, CHATHAM);
        assert_eq!(b, at(CHATHAM, 2026, 6, 15, 10, 0));
        assert_eq!(b.rem_euclid(3_600_000), 15 * 60_000);
        // A 15m bucket nests inside Chatham's hour, a 20m one does not.
        assert_eq!(bs(Interval::Minutes(15), b, CHATHAM), b);
        assert_ne!(bs(Interval::Minutes(20), b, CHATHAM), b);
        // In Kolkata it is 30m that nests and 20m that does not.
        let b = bs(
            Interval::Hours(1),
            at(KOLKATA, 2026, 6, 15, 10, 20),
            KOLKATA,
        );
        assert_eq!(bs(Interval::Minutes(30), b, KOLKATA), b);
        assert_ne!(bs(Interval::Minutes(20), b, KOLKATA), b);
    }

    /// Europe/Budapest, 2026-03-29: 02:00 → 03:00. 23 hourly buckets,
    /// one daily.
    #[test]
    fn budapest_spring_forward_has_23_hourly_buckets_and_one_day() {
        let day = at(BUDAPEST, 2026, 3, 29, 0, 0);
        let next_day = at(BUDAPEST, 2026, 3, 30, 0, 0);
        assert_eq!(next_day - day, 23 * 3_600_000);
        assert_eq!(
            count_buckets(Interval::Hours(1), day, next_day, BUDAPEST, (0, 0)),
            23
        );
        assert_eq!(
            count_buckets(Interval::Day, day, next_day, BUDAPEST, (0, 0)),
            1
        );
        // The hour that swallowed 02:00 starts at the transition.
        let t = at(BUDAPEST, 2026, 3, 29, 3, 30);
        assert_eq!(bs(Interval::Hours(1), t, BUDAPEST), day + 2 * 3_600_000);
        // Everything in the day lands in the one daily bucket.
        assert_eq!(bs(Interval::Day, t, BUDAPEST), day);
    }

    /// Europe/Budapest, 2026-10-25: 03:00 → 02:00. 25 hourly buckets,
    /// one daily, and the two local 02:00 hours are distinct buckets.
    #[test]
    fn budapest_fall_back_has_25_hourly_buckets_and_one_day() {
        let day = at(BUDAPEST, 2026, 10, 25, 0, 0);
        let next_day = at(BUDAPEST, 2026, 10, 26, 0, 0);
        assert_eq!(next_day - day, 25 * 3_600_000);
        assert_eq!(
            count_buckets(Interval::Hours(1), day, next_day, BUDAPEST, (0, 0)),
            25
        );
        assert_eq!(
            count_buckets(Interval::Day, day, next_day, BUDAPEST, (0, 0)),
            1
        );
        let first_02 = day + 2 * 3_600_000;
        let second_02 = first_02 + 3_600_000;
        assert_eq!(
            bs(Interval::Hours(1), first_02 + 59 * 60_000, BUDAPEST),
            first_02
        );
        assert_eq!(bs(Interval::Hours(1), second_02, BUDAPEST), second_02);
        assert_eq!(nbs(Interval::Hours(1), first_02, BUDAPEST), second_02);
        assert_eq!(bs(Interval::Day, second_02, BUDAPEST), day);
    }

    /// `day-start=02:00` on the spring-forward day names an instant that
    /// does not exist. It resolves to the first instant after the gap,
    /// local 03:00 — and the day before it is 1 hour short, not broken.
    #[test]
    fn day_start_inside_a_dst_gap_resolves_forward() {
        let z = tz(BUDAPEST);
        let gap_day_start = at(BUDAPEST, 2026, 3, 29, 3, 0); // 02:00 does not exist
        let prev_day_start = at(BUDAPEST, 2026, 3, 28, 2, 0);
        let next_day_start = at(BUDAPEST, 2026, 3, 30, 2, 0);
        for probe in [gap_day_start, gap_day_start + 3_600_000, next_day_start - 1] {
            assert_eq!(
                bucket_start(Interval::Day, probe, &z, (2, 0), WeekStart::Mon),
                gap_day_start,
                "probe {probe}"
            );
        }
        assert_eq!(
            bucket_start(Interval::Day, gap_day_start - 1, &z, (2, 0), WeekStart::Mon),
            prev_day_start
        );
        assert_eq!(
            next_bucket_start(Interval::Day, gap_day_start, &z, (2, 0), WeekStart::Mon),
            next_day_start
        );
        // That "day" is 22 hours long, and it is still one day.
        assert_eq!(next_day_start - gap_day_start, 23 * 3_600_000);
        assert_eq!(gap_day_start - prev_day_start, 24 * 3_600_000);
        assert_eq!(
            count_buckets(
                Interval::Day,
                prev_day_start,
                next_day_start,
                BUDAPEST,
                (2, 0)
            ),
            2
        );
    }

    /// The same `day-start=02:00` on the fall-back day, where 02:00
    /// happens twice: the day starts at the earlier one, and is 25 hours
    /// long. One daily bucket, not two.
    #[test]
    fn day_start_inside_a_dst_fold_takes_the_earlier_instant() {
        let z = tz(BUDAPEST);
        let day = at(BUDAPEST, 2026, 10, 25, 2, 0);
        let next = at(BUDAPEST, 2026, 10, 26, 2, 0);
        assert_eq!(next - day, 25 * 3_600_000);
        // The repeated 02:00 hour does not open a second day.
        assert_eq!(
            bucket_start(Interval::Day, day + 3_600_000, &z, (2, 0), WeekStart::Mon),
            day
        );
        assert_eq!(count_buckets(Interval::Day, day, next, BUDAPEST, (2, 0)), 1);
    }

    #[test]
    fn february_29_and_month_ends() {
        // A leap day is an ordinary day bucket.
        let feb29 = at("UTC", 2024, 2, 29, 0, 0);
        assert_eq!(bs(Interval::Day, feb29 + 1, "UTC"), feb29);
        assert_eq!(
            nbs(Interval::Day, feb29, "UTC"),
            at("UTC", 2024, 3, 1, 0, 0)
        );
        // 1mo buckets on the 31st belong to that month, and the next
        // bucket is the 1st of the next month, never "30 days later".
        let mar31 = at("UTC", 2026, 3, 31, 23, 59);
        assert_eq!(
            bs(Interval::Month, mar31, "UTC"),
            at("UTC", 2026, 3, 1, 0, 0)
        );
        assert_eq!(
            nbs(Interval::Month, mar31, "UTC"),
            at("UTC", 2026, 4, 1, 0, 0)
        );
        // February 2024 has 29 day buckets, February 2026 has 28.
        assert_eq!(
            count_buckets(
                Interval::Day,
                at("UTC", 2024, 2, 1, 0, 0),
                at("UTC", 2024, 3, 1, 0, 0),
                "UTC",
                (0, 0)
            ),
            29
        );
        assert_eq!(
            count_buckets(
                Interval::Day,
                at("UTC", 2026, 2, 1, 0, 0),
                at("UTC", 2026, 3, 1, 0, 0),
                "UTC",
                (0, 0)
            ),
            28
        );
        // 12 month buckets per year bucket.
        assert_eq!(
            count_buckets(
                Interval::Month,
                at("UTC", 2026, 1, 1, 0, 0),
                at("UTC", 2027, 1, 1, 0, 0),
                "UTC",
                (0, 0)
            ),
            12
        );
    }

    /// `subtract_keep` is calendar arithmetic for months and years and
    /// exact seconds for everything else, and it clamps.
    #[test]
    fn keep_windows_subtract_by_calendar_for_months_and_exactly_otherwise() {
        let z = tz("UTC");
        let mar31 = at("UTC", 2026, 3, 31, 0, 0);
        assert_eq!(
            subtract_keep(mar31, Keep::Months(1), Interval::Day, &z),
            Some(at("UTC", 2026, 2, 28, 0, 0))
        );
        assert_eq!(
            subtract_keep(
                at("UTC", 2024, 3, 31, 0, 0),
                Keep::Months(1),
                Interval::Day,
                &z
            ),
            Some(at("UTC", 2024, 2, 29, 0, 0))
        );
        assert_eq!(
            subtract_keep(mar31, Keep::Years(1), Interval::Day, &z),
            Some(at("UTC", 2025, 3, 31, 0, 0))
        );
        assert_eq!(
            add_keep(
                at("UTC", 2026, 1, 31, 0, 0),
                Keep::Months(1),
                Interval::Day,
                &z
            ),
            Some(at("UTC", 2026, 2, 28, 0, 0))
        );
        assert_eq!(
            subtract_keep(mar31, Keep::Days(1), Interval::Hours(1), &z),
            Some(mar31 - 86_400_000)
        );
        assert_eq!(
            subtract_keep(mar31, Keep::Weeks(2), Interval::Hours(1), &z),
            Some(mar31 - 14 * 86_400_000)
        );
        assert_eq!(subtract_keep(mar31, Keep::Forever, Interval::Day, &z), None);
        assert_eq!(add_keep(mar31, Keep::Forever, Interval::Day, &z), None);
        // A calendar month across a DST transition keeps the local
        // wall-clock time, so it is not a whole number of hours.
        let bz = tz(BUDAPEST);
        let apr15 = at(BUDAPEST, 2026, 4, 15, 0, 0);
        assert_eq!(
            subtract_keep(apr15, Keep::Months(1), Interval::Day, &bz),
            Some(at(BUDAPEST, 2026, 3, 15, 0, 0))
        );
        assert_eq!(
            apr15 - at(BUDAPEST, 2026, 3, 15, 0, 0),
            31 * 86_400_000 - 3_600_000
        );
    }

    /// `5m:1d` must keep exactly 288 buckets (the plan's own figure), so
    /// a `d` keep is an exact duration even across a DST transition.
    #[test]
    fn a_day_keep_is_exactly_288_five_minute_buckets() {
        let z = tz(BUDAPEST);
        for probe in [
            at(BUDAPEST, 2026, 3, 29, 12, 0),
            at(BUDAPEST, 2026, 10, 25, 12, 0),
            at(BUDAPEST, 2026, 6, 15, 7, 35),
        ] {
            let anchor = bucket_start(Interval::Minutes(5), probe, &z, (0, 0), WeekStart::Mon);
            let edge = subtract_keep(anchor, Keep::Days(1), Interval::Minutes(5), &z).unwrap();
            let mut n = 0;
            let mut b = anchor;
            while b > edge {
                n += 1;
                b -= 5 * 60_000;
            }
            assert_eq!(n, 288, "probe {probe}");
        }
    }

    /// A `d`/`w` keep on a `1d`-or-coarser tier counts calendar days in
    /// the zone, so `1d:7d` is 7 daily buckets on both sides of a DST
    /// change. As an exact duration, the edge after spring-forward fell
    /// one hour *inside* the eighth-newest day, which then stayed for a
    /// whole week (32-M1b review).
    #[test]
    fn day_and_week_keeps_count_calendar_days_on_daily_tiers() {
        let z = tz(BUDAPEST);
        // Spring-forward is 2026-03-29; the anchor's bucket is 3 April.
        let anchor = at(BUDAPEST, 2026, 4, 3, 0, 0);
        let edge = subtract_keep(anchor, Keep::Days(7), Interval::Day, &z).unwrap();
        assert_eq!(edge, at(BUDAPEST, 2026, 3, 27, 0, 0));
        // Daily buckets strictly after the edge: exactly seven.
        let mut n = 0;
        let mut b = at(BUDAPEST, 2026, 3, 20, 0, 0);
        while b <= anchor {
            if b > edge {
                n += 1;
            }
            b = next_bucket_start(Interval::Day, b, &z, (0, 0), WeekStart::Mon);
        }
        assert_eq!(n, 7);
        // The exact-duration edge would have been an hour earlier.
        assert_eq!(edge - (anchor - 7 * 86_400_000), 3_600_000);
        // Weeks likewise, and the forecast mirror agrees.
        assert_eq!(
            subtract_keep(anchor, Keep::Weeks(1), Interval::Week, &z),
            Some(at(BUDAPEST, 2026, 3, 27, 0, 0))
        );
        assert_eq!(
            add_keep(edge, Keep::Days(7), Interval::Day, &z),
            Some(anchor)
        );
        // A sub-day tier still subtracts an exact 24 h.
        assert_eq!(
            subtract_keep(anchor, Keep::Days(7), Interval::Hours(1), &z),
            Some(anchor - 7 * 86_400_000)
        );
    }

    /// Compatible disambiguation moves a `day_start` in a gap forward
    /// by the gap's length, which for a gap not aligned to the hour is
    /// *not* the first instant after it (module docs).
    #[test]
    fn day_start_in_an_unaligned_gap_moves_forward_by_the_gap() {
        let z = tz(CHATHAM);
        // 2026-09-27 12:00 local, well after the 02:45 → 03:45 gap.
        let noon = at(CHATHAM, 2026, 9, 27, 12, 0);
        let b = bucket_start(Interval::Day, noon, &z, (3, 0), WeekStart::Mon);
        let local = z.to_offset(clamp_ts(b)).to_datetime(clamp_ts(b));
        assert_eq!(local, Date::new(2026, 9, 27).unwrap().at(4, 0, 0, 0));
        assert_eq!(z.to_offset(clamp_ts(b)).seconds(), 13 * 3600 + 45 * 60);
        // Still one daily bucket that date: the previous one began at
        // 03:00 +12:45 and this one ends at 03:00 +13:45 the next day.
        let prev = bucket_start(Interval::Day, b - 1, &z, (3, 0), WeekStart::Mon);
        assert_eq!(prev, at(CHATHAM, 2026, 9, 26, 3, 0));
        assert_eq!(
            next_bucket_start(Interval::Day, b, &z, (3, 0), WeekStart::Mon),
            at(CHATHAM, 2026, 9, 28, 3, 0)
        );
    }

    #[test]
    fn iso_weeks_span_the_year_boundary() {
        // 2026-01-01 is a Thursday; its ISO week starts Mon 2025-12-29.
        let newyear = at("UTC", 2026, 1, 1, 12, 0);
        assert_eq!(
            bs(Interval::Week, newyear, "UTC"),
            at("UTC", 2025, 12, 29, 0, 0)
        );
        assert_eq!(
            nbs(Interval::Week, newyear, "UTC"),
            at("UTC", 2026, 1, 5, 0, 0)
        );
        // Sunday weeks put the same instant in a week starting
        // 2025-12-28, and the year bucket is unaffected by either.
        let z = tz("UTC");
        assert_eq!(
            bucket_start(Interval::Week, newyear, &z, (0, 0), WeekStart::Sun),
            at("UTC", 2025, 12, 28, 0, 0)
        );
        assert_eq!(
            bs(Interval::Year, newyear, "UTC"),
            at("UTC", 2026, 1, 1, 0, 0)
        );
        // Monday itself starts its own week.
        let mon = at("UTC", 2026, 1, 5, 0, 0);
        assert_eq!(bs(Interval::Week, mon, "UTC"), mon);
        assert_eq!(
            bs(Interval::Week, mon - 1, "UTC"),
            at("UTC", 2025, 12, 29, 0, 0)
        );
    }

    /// A zone that shifts by 30 minutes, at 02:00: the local hour label
    /// 02:00 is a gap of half an hour, so the bucket that carries
    /// 02:45 starts at the transition.
    #[test]
    fn lord_howe_half_hour_transition_still_tiles_the_day() {
        let name = "Australia/Lord_Howe";
        let day = at(name, 2026, 10, 4, 0, 0);
        let next = at(name, 2026, 10, 5, 0, 0);
        assert_eq!(next - day, 24 * 3_600_000 - 30 * 60_000);
        // The day still tiles into hour buckets with no gap or overlap,
        // and holds exactly one daily bucket.
        let n = count_buckets(Interval::Hours(1), day, next, name, (0, 0));
        assert_eq!(n, 24);
        assert_eq!(count_buckets(Interval::Day, day, next, name, (0, 0)), 1);
    }

    /// Whatever the zone or interval, `bucket_start` is a floor and
    /// `next_bucket_start` its successor: the two tile the line.
    #[test]
    fn buckets_tile_the_line_in_every_zone() {
        let zones = ["UTC", BUDAPEST, KOLKATA, CHATHAM, "America/Santiago"];
        let intervals = [
            Interval::Seconds(15),
            Interval::Minutes(20),
            Interval::Hours(1),
            Interval::Hours(6),
            Interval::Day,
            Interval::Week,
            Interval::Month,
            Interval::Year,
        ];
        for name in zones {
            let z = tz(name);
            for interval in intervals {
                for day_start in [(0u8, 0u8), (2, 0)] {
                    let mut t = at(name, 2026, 3, 1, 0, 0);
                    let end = at(name, 2026, 11, 2, 0, 0);
                    let mut b = bucket_start(interval, t, &z, day_start, WeekStart::Mon);
                    assert!(b <= t);
                    while t < end {
                        let next = next_bucket_start(interval, t, &z, day_start, WeekStart::Mon);
                        assert!(next > t, "{name} {interval} {day_start:?} at {t}");
                        assert_eq!(
                            bucket_start(interval, next, &z, day_start, WeekStart::Mon),
                            next,
                            "{name} {interval} {day_start:?}: {next} is not its own bucket start"
                        );
                        assert_eq!(
                            bucket_start(interval, next - 1, &z, day_start, WeekStart::Mon),
                            b,
                            "{name} {interval} {day_start:?}: gap before {next}"
                        );
                        b = next;
                        t = next;
                    }
                }
            }
        }
    }

    /// Nothing here panics or returns nonsense for a timestamp far
    /// outside the representable range.
    #[test]
    fn extreme_timestamps_are_clamped_not_fatal() {
        let z = tz(BUDAPEST);
        for t in [i64::MIN, i64::MIN + 1, -1, 0, i64::MAX - 1, i64::MAX] {
            for interval in [
                Interval::Seconds(10),
                Interval::Minutes(5),
                Interval::Hours(1),
                Interval::Day,
                Interval::Week,
                Interval::Month,
                Interval::Year,
            ] {
                let b = bucket_start(interval, t, &z, (2, 0), WeekStart::Mon);
                assert!(b <= t, "{interval} at {t}: {b}");
                let n = next_bucket_start(interval, t, &z, (2, 0), WeekStart::Mon);
                assert!(n > b, "{interval} at {t}: {b} -> {n}");
                for keep in [
                    Keep::Minutes(1),
                    Keep::Days(30),
                    Keep::Months(6),
                    Keep::Years(2),
                ] {
                    assert!(subtract_keep(b, keep, interval, &z).is_some());
                    assert!(add_keep(b, keep, interval, &z).is_some());
                }
            }
        }
    }

    #[test]
    fn unknown_timezones_are_none_and_utc_is_free() {
        assert!(time_zone("Nowhere/Nothing").is_none());
        assert!(time_zone("Etc/Unknown").is_none());
        assert_eq!(time_zone("UTC").unwrap(), TimeZone::UTC);
        assert!(time_zone(BUDAPEST).is_some());
    }
}
