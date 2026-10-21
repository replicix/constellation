//! The persisted text form of a deadline (plan 43 C7a, Appendix B row 14).
//!
//! A horizon that must outlive the process (the latest end of any read
//! delegation this node granted, of any lock grant, of the last heartbeat
//! promise) is a KV string, written with the boot it was measured on
//! ([`constellation_types::clock::BootStamp`]):
//!
//! ```text
//! <wall_until_ms> <span_ms> <boot_id as 16 hex digits> <raw_until_ms>
//! ```
//!
//! - Token 0 is the value an older build wrote on its own (the deadline on
//!   the wall clock) and what status and logs show.
//! - Tokens 1 to 3 let a restart read the time left without the wall clock:
//!   on the same boot, from the raw clock; after a reboot, the whole `span`
//!   (how far ahead of the write the deadline was).
//!
//! The format is append-only: **readers ignore tokens they do not know,
//! writers write every token they know, future fields go at the end.** A
//! value with only token 0, or whose tokens 1 to 3 do not parse, carries no
//! boot: its span is the caller's `default_span_ms` (the configured maximum
//! lead of that horizon), which only costs a wait, never safety. A span
//! beyond [`MAX_SPAN_MS`] (and twice the default) is a corrupt value and is
//! clamped to it, so that it cannot block a node for ever. The bound is not
//! the default alone: a span the previous process wrote under a longer
//! configured TTL (or a longer obligation persisted through the same key)
//! is legitimate, and clamping it to this process's default would wait out
//! less than a peer may still honour.

use constellation_types::clock::{BootStamp, Ms};

/// The longest span a persisted horizon is trusted with when it exceeds
/// twice the caller's default (an hour: far beyond any configured lead, so
/// only a corrupt value is cut, and a corrupt value blocks for an hour at
/// most).
pub const MAX_SPAN_MS: i64 = 3_600_000;

/// `stamp` as the persisted value.
pub fn format_stamp(stamp: &BootStamp) -> String {
    format!(
        "{} {} {:016x} {}",
        stamp.wall.0, stamp.span_ms, stamp.boot_id, stamp.raw_until_ms
    )
}

/// The persisted value `v` as a stamp (see the module doc). Always a stamp:
/// a value that does not even start with a number is read as an older
/// build's would have been, with no boot and the default span, and logged.
pub fn parse_stamp(v: &str, default_span_ms: i64) -> BootStamp {
    let mut tokens = v.split_whitespace();
    let Some(wall) = tokens.next().and_then(|t| t.parse::<i64>().ok()) else {
        tracing::warn!(
            value = v,
            "a persisted horizon is not a number; waiting its default span"
        );
        return BootStamp {
            wall: Ms(0),
            span_ms: default_span_ms,
            boot_id: 0,
            raw_until_ms: 0,
        };
    };
    let boot = (|| {
        let span = tokens.next()?.parse::<i64>().ok().filter(|s| *s >= 0)?;
        let id = u64::from_str_radix(tokens.next()?, 16).ok()?;
        let raw = tokens.next()?.parse::<i64>().ok()?;
        Some((span, id, raw))
    })();
    let Some((span, boot_id, raw_until_ms)) = boot else {
        return BootStamp {
            wall: Ms(wall),
            span_ms: default_span_ms,
            boot_id: 0,
            raw_until_ms: 0,
        };
    };
    let cap = default_span_ms.saturating_mul(2).max(MAX_SPAN_MS);
    if span > default_span_ms.saturating_mul(2) {
        tracing::warn!(
            span_ms = span,
            default_span_ms,
            cap_ms = cap,
            "a persisted horizon's span is beyond twice its configured maximum lead \
             (a longer TTL before this restart, or a corrupt value); waiting it out \
             up to the cap"
        );
    }
    BootStamp {
        wall: Ms(wall),
        span_ms: span.min(cap),
        boot_id,
        raw_until_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expectation: the text form round-trips, token 0 stays the wall value
    /// an older build read, and the boot id is 16 hex digits.
    #[test]
    fn a_stamp_round_trips_as_text() {
        let s = BootStamp {
            wall: Ms(1_780_000_123_456),
            span_ms: 21_000,
            boot_id: 0xab,
            raw_until_ms: 99_000,
        };
        let text = format_stamp(&s);
        assert_eq!(text, "1780000123456 21000 00000000000000ab 99000");
        assert_eq!(parse_stamp(&text, 21_000), s);
        // What an older build does with it: reads token 0.
        assert_eq!(text.split(' ').next().unwrap().parse::<i64>(), Ok(s.wall.0));
    }

    /// Expectation (the persisted format is append-only): a value an older
    /// build wrote, a bare number, reads as no boot and the default span;
    /// trailing tokens a future build appends are ignored; an unparsable
    /// token 1 to 3 is no boot too; garbage is the default span, and never
    /// panics.
    #[test]
    fn old_values_extra_tokens_and_garbage_read_conservatively() {
        let none = |wall: i64| BootStamp {
            wall: Ms(wall),
            span_ms: 7_000,
            boot_id: 0,
            raw_until_ms: 0,
        };
        assert_eq!(parse_stamp("123", 7_000), none(123));
        assert_eq!(parse_stamp("  123\n", 7_000), none(123));
        let full = BootStamp {
            wall: Ms(123),
            span_ms: 5_000,
            boot_id: 0x00ab_cdef_0123_4567,
            raw_until_ms: 99_000,
        };
        assert_eq!(parse_stamp("123 5000 00abcdef01234567 99000", 7_000), full);
        assert_eq!(
            parse_stamp("123 5000 00abcdef01234567 99000 7 future tokens", 7_000),
            full
        );
        for bad in [
            "123 5000",
            "123 5000 00abcdef01234567",
            "123 x 00abcdef01234567 99000",
            "123 5000 zz 99000",
            "123 5000 00abcdef01234567 y",
            "123 -5 00abcdef01234567 99000",
        ] {
            assert_eq!(parse_stamp(bad, 7_000), none(123), "{bad:?}");
        }
        for garbage in ["", "soon", "-", "1.5 2 3 4"] {
            let s = parse_stamp(garbage, 7_000);
            assert_eq!((s.span_ms, s.boot_id), (7_000, 0), "{garbage:?}");
        }
    }

    /// Expectation: a span beyond the cap (a corrupt value) is clamped to
    /// it, the boot kept; one beyond twice the default but within the cap (a
    /// previous process that ran with a longer TTL) is kept whole, since a
    /// peer may still honour that much; a default beyond the cap raises it.
    #[test]
    fn a_corrupt_span_is_clamped() {
        let at_bound = parse_stamp("1 14000 0000000000000001 5", 7_000);
        assert_eq!(at_bound.span_ms, 14_000);
        let longer_ttl = parse_stamp("1 67000 0000000000000001 5", 7_000);
        assert_eq!(longer_ttl.span_ms, 67_000);
        let beyond = parse_stamp("1 9999999999 0000000000000001 5", 7_000);
        assert_eq!((beyond.span_ms, beyond.boot_id), (MAX_SPAN_MS, 1));
        let huge_default = parse_stamp("1 9999999999 0000000000000001 5", MAX_SPAN_MS);
        assert_eq!(huge_default.span_ms, 2 * MAX_SPAN_MS);
    }
}
