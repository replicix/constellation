//! Lexical helpers shared by the xattr policy languages: plan 22's prune
//! policies ([`crate::prune::policy`]) and plan 32's snapshot schedules
//! ([`crate::snapsched::policy`]).
//!
//! Both languages are hand-written, *pure* parsers over tiny closed
//! grammars: same bytes in, same policy out, on every node and every
//! run — no environment, no locale, no clock. Both report failures as a
//! [`PolicyError`] carrying a byte offset into the source expression, so
//! a CLI or the web UI can render a caret under the offending token.
//! This module exists so the two agree on the pieces where agreeing
//! matters: what `30d` means, what `500G` means, and how a figure is
//! printed back.
//!
//! Note that *duration units* are shared only where the two languages
//! share a meaning. `parse_duration` here is plan 22's: every unit has a
//! fixed second count. Plan 32 keeps months and years symbolic (a month
//! is not 30 days when you are aligning calendar buckets) and therefore
//! parses its own `keep` windows; it uses this module for sizes,
//! integers, offsets and the error type.

use std::fmt;
use std::time::Duration;

/// A parse/validation failure carrying the byte offset into the source
/// expression, so the CLI can render a caret under the offending token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyError {
    pub offset: usize,
    pub msg: String,
}

impl PolicyError {
    pub(crate) fn at(offset: usize, msg: impl Into<String>) -> Self {
        Self {
            offset,
            msg: msg.into(),
        }
    }

    /// Two-line rendering: the expression, then a caret under `offset`.
    pub fn render(&self, src: &str) -> String {
        let caret = self.offset.min(src.len());
        format!("{src}\n{}^ {}", " ".repeat(caret), self.msg)
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "at byte {}: {}", self.offset, self.msg)
    }
}

impl std::error::Error for PolicyError {}

/// Plan 22's duration grammar: `<n><s|m|h|d|w|y>`, every unit a fixed
/// number of seconds (`m` is minutes; a year is 365 days).
pub fn parse_duration(s: &str, off: usize) -> Result<Duration, PolicyError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PolicyError::at(off, "expected a duration"));
    }
    let bytes = s.as_bytes();
    let last = bytes[bytes.len() - 1];
    let mult: u64 = match last {
        b's' => 1,
        b'm' => 60,
        b'h' => 3600,
        b'd' => 86_400,
        b'w' => 604_800,
        b'y' => 31_536_000,
        b'0'..=b'9' => {
            return Err(PolicyError::at(
                off,
                format!("duration `{s}` has no unit (use s/m/h/d/w/y)"),
            ))
        }
        _ => {
            return Err(PolicyError::at(
                off,
                format!("unknown duration unit in `{s}`"),
            ))
        }
    };
    let num: u64 = s[..s.len() - 1]
        .parse()
        .map_err(|_| PolicyError::at(off, format!("invalid duration `{s}`")))?;
    Ok(Duration::from_secs(num.saturating_mul(mult)))
}

/// Binary size units: `<n>[K|M|G|T]`, a bare number meaning bytes.
pub(crate) fn parse_size(s: &str, off: usize) -> Result<u64, PolicyError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(PolicyError::at(off, "expected a size"));
    }
    let bytes = s.as_bytes();
    let last = bytes[bytes.len() - 1];
    let (mult, digits): (u64, &str) = match last {
        b'K' => (1024, &s[..s.len() - 1]),
        b'M' => (1024 * 1024, &s[..s.len() - 1]),
        b'G' => (1024 * 1024 * 1024, &s[..s.len() - 1]),
        b'T' => (1024 * 1024 * 1024 * 1024, &s[..s.len() - 1]),
        b'0'..=b'9' => (1, s),
        _ => {
            return Err(PolicyError::at(
                off,
                format!("unknown size unit in `{s}` (use K/M/G/T)"),
            ))
        }
    };
    let num: u64 = digits
        .parse()
        .map_err(|_| PolicyError::at(off, format!("invalid size `{s}`")))?;
    Ok(num.saturating_mul(mult))
}

pub(crate) fn parse_int(s: &str, off: usize) -> Result<u64, PolicyError> {
    s.trim()
        .parse()
        .map_err(|_| PolicyError::at(off, format!("expected an integer, got `{s}`")))
}

/// Bytes of leading whitespace, used to keep an error offset on the
/// first byte of a token rather than on the space before it.
pub(crate) fn leading_ws(s: &str) -> usize {
    s.len() - s.trim_start().len()
}

/// A setting or argument key: lowercase ASCII, digits and `-`.
pub(crate) fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// Largest exact duration unit.
pub(crate) fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    for (unit, sym) in [
        (31_536_000u64, 'y'),
        (604_800, 'w'),
        (86_400, 'd'),
        (3_600, 'h'),
        (60, 'm'),
        (1, 's'),
    ] {
        if secs >= unit && secs.is_multiple_of(unit) {
            return format!("{}{}", secs / unit, sym);
        }
    }
    format!("{secs}s")
}

/// Largest exact binary size unit.
pub(crate) fn fmt_size(b: u64) -> String {
    for (unit, sym) in [
        (1024u64 * 1024 * 1024 * 1024, 'T'),
        (1024 * 1024 * 1024, 'G'),
        (1024 * 1024, 'M'),
        (1024, 'K'),
    ] {
        if b >= unit && b.is_multiple_of(unit) {
            return format!("{}{}", b / unit, sym);
        }
    }
    b.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_units_are_plan_22s() {
        assert_eq!(parse_duration("90d", 0).unwrap().as_secs(), 90 * 86_400);
        assert_eq!(parse_duration("5m", 0).unwrap().as_secs(), 300);
        assert!(parse_duration("60", 0).is_err());
        assert!(parse_duration("1mo", 0).is_err());
    }

    #[test]
    fn sizes_are_binary() {
        assert_eq!(parse_size("1G", 0).unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("17", 0).unwrap(), 17);
        assert!(parse_size("1X", 0).is_err());
    }

    #[test]
    fn formatting_picks_the_largest_exact_unit() {
        assert_eq!(fmt_duration(Duration::from_secs(604_800)), "1w");
        assert_eq!(fmt_duration(Duration::from_secs(90)), "90s");
        assert_eq!(fmt_size(500 * 1024 * 1024 * 1024), "500G");
        assert_eq!(fmt_size(1025), "1025");
    }

    #[test]
    fn error_renders_a_caret() {
        let e = PolicyError::at(4, "boom");
        assert_eq!(e.render("abcdefg"), "abcdefg\n    ^ boom");
        // An out-of-range offset still renders.
        assert_eq!(PolicyError::at(99, "x").render("ab"), "ab\n  ^ x");
    }
}
