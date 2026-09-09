//! Prune-policy expression language (plan 22, Step 1).
//!
//! A directory carries its complete retention policy in the
//! `user.constellation.prune` xattr; there is no policy registry and no
//! name to resolve. This module is the hand-written parser over a tiny,
//! closed grammar plus the canonical `Display` form. It is *pure*: same
//! bytes in, same [`Policy`] out, on every node and every run — no
//! environment, no locale, no clock. That purity is what lets one node
//! prune for the whole cluster (a divergent parse would diverge the
//! namespace), so nothing here may consult runtime state. The one
//! set-time courtesy that *does* look at the mount (rejecting an
//! atime-driven rule when atime is off) lives in [`Policy::needs_atime`]
//! and is applied by the setxattr gate, never by `parse`.
//!
//! ```text
//! policy    := clause (";" clause)*
//! clause    := ident "(" args ")"      // a rule
//!            | ident                    // a flag: off, dry
//!            | "!"                       // the arming token
//!            | ident "=" value          // a setting: every=1h, max=5000
//! args      := (value | ident "=" value) ("," ...)*
//! ```

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
    fn at(offset: usize, msg: impl Into<String>) -> Self {
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

/// A watermark for `lru`: an absolute size (bytes) or a percentage.
/// "Of what" is settled elsewhere: a size is bytes of the marked
/// subtree, a percentage is of the filesystem quota (see [`Of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Watermark {
    Size(u64),
    Percent(u8),
}

impl fmt::Display for Watermark {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Watermark::Size(b) => write!(f, "{}", fmt_size(*b)),
            Watermark::Percent(p) => write!(f, "{p}%"),
        }
    }
}

/// What an `lru` watermark is measured against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Of {
    /// Whole-filesystem usage (against the quota for a percentage, or
    /// the raw byte figure for a size).
    Fs,
    /// The marked subtree's own recursive size.
    Subtree,
}

impl fmt::Display for Of {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Of::Fs => f.write_str("fs"),
            Of::Subtree => f.write_str("subtree"),
        }
    }
}

/// A retention rule. `age`/`keep` need no atime; `unused`/`lru` do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rule {
    /// mtime older than the duration.
    Age(Duration),
    /// atime older than the duration.
    Unused(Duration),
    /// Usage crossed `high`: evict coldest-by-atime until under `low`.
    Lru {
        high: Watermark,
        low: Watermark,
        of: Of,
    },
    /// Keep the newest `n` entries per directory, unlink the rest.
    Keep(u32),
}

impl Rule {
    /// Canonical ordering key so the internal representation is
    /// order-independent (the language says clause order is irrelevant).
    fn order(&self) -> u8 {
        match self {
            Rule::Age(_) => 0,
            Rule::Unused(_) => 1,
            Rule::Lru { .. } => 2,
            Rule::Keep(_) => 3,
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Rule::Age(_) => "age",
            Rule::Unused(_) => "unused",
            Rule::Lru { .. } => "lru",
            Rule::Keep(_) => "keep",
        }
    }

    /// Whether this rule requires the optional read-time atime feature.
    pub fn needs_atime(&self) -> bool {
        matches!(self, Rule::Unused(_) | Rule::Lru { .. })
    }
}

/// Per-rule filters (named args on any rule). All optional except
/// `min_age`, which floors every rule at 1h by default and can never be
/// zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub min_size: Option<u64>,
    pub max_size: Option<u64>,
    pub only: Option<String>,
    pub except: Vec<String>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub min_age: Duration,
}

pub const DEFAULT_MIN_AGE: Duration = Duration::from_secs(3600);

impl Default for Filter {
    fn default() -> Self {
        Self {
            min_size: None,
            max_size: None,
            only: None,
            except: Vec::new(),
            uid: None,
            gid: None,
            min_age: DEFAULT_MIN_AGE,
        }
    }
}

impl Filter {
    fn is_default(&self) -> bool {
        *self == Filter::default()
    }
}

/// A rule together with its filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleClause {
    pub rule: Rule,
    pub filter: Filter,
}

/// A fully parsed prune policy. `off` is exclusive of everything else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// Empty iff `off`.
    pub rules: Vec<RuleClause>,
    /// The subtree is exempt; cancels any inherited policy.
    pub off: bool,
    /// The policy is armed for real deletion (the `!` token). A policy
    /// is dry-run unless this is set.
    pub armed: bool,
    pub every: Duration,
    pub max: u64,
    pub rate: u32,
}

pub const DEFAULT_EVERY: Duration = Duration::from_secs(3600);
pub const DEFAULT_MAX: u64 = 10_000;
pub const DEFAULT_RATE: u32 = 50;

impl Policy {
    /// The exempting policy `off`.
    pub fn off() -> Self {
        Self {
            rules: Vec::new(),
            off: true,
            armed: false,
            every: DEFAULT_EVERY,
            max: DEFAULT_MAX,
            rate: DEFAULT_RATE,
        }
    }

    /// Whether any rule requires the optional read-time atime feature.
    /// The setxattr gate uses this to reject an atime-driven policy on a
    /// mount with atime off; `parse` never consults it.
    pub fn needs_atime(&self) -> bool {
        self.rules.iter().any(|c| c.rule.needs_atime())
    }

    /// Parse a policy expression. Pure — no environment, no clock.
    pub fn parse(src: &str) -> Result<Policy, PolicyError> {
        let mut rules: Vec<RuleClause> = Vec::new();
        let mut off = false;
        let mut armed = false;
        let mut dry = false;
        let mut every: Option<Duration> = None;
        let mut max: Option<u64> = None;
        let mut rate: Option<u32> = None;
        let mut clause_count = 0usize;

        for clause in split_top(src, ';') {
            let text = clause.text.trim();
            if text.is_empty() {
                return Err(PolicyError::at(clause.start, "empty clause"));
            }
            clause_count += 1;
            let base = clause.start + leading_ws(clause.text);

            if text == "!" {
                if armed {
                    return Err(PolicyError::at(base, "duplicate `!`"));
                }
                armed = true;
                continue;
            }

            // Rule: ident "(" args ")"
            if let Some(paren) = text.find('(') {
                let name = text[..paren].trim();
                let name_off = base + leading_ws(&text[..paren]);
                if !text.ends_with(')') {
                    return Err(PolicyError::at(
                        base + text.len(),
                        "missing `)` to close rule arguments",
                    ));
                }
                let args_str = &text[paren + 1..text.len() - 1];
                let args_off = base + paren + 1;
                let clause = parse_rule(name, name_off, args_str, args_off)?;
                if rules.iter().any(|c| c.rule.order() == clause.rule.order()) {
                    return Err(PolicyError::at(
                        name_off,
                        format!("duplicate `{}` rule", clause.rule.kind_name()),
                    ));
                }
                rules.push(clause);
                continue;
            }

            // Setting: ident "=" value
            if let Some(eq) = text.find('=') {
                let key = text[..eq].trim();
                let key_off = base + leading_ws(&text[..eq]);
                let val = text[eq + 1..].trim();
                let val_off = base + eq + 1 + leading_ws(&text[eq + 1..]);
                match key {
                    "every" => {
                        every = Some(parse_duration(val, val_off)?);
                    }
                    "max" => {
                        max = Some(parse_int(val, val_off)?);
                    }
                    "rate" => {
                        rate = Some(parse_rate(val, val_off)?);
                    }
                    other => {
                        return Err(PolicyError::at(
                            key_off,
                            format!("unknown setting `{other}`"),
                        ));
                    }
                }
                continue;
            }

            // Flag: bare ident
            match text {
                "off" => off = true,
                "dry" => dry = true,
                other => {
                    return Err(PolicyError::at(base, format!("unknown clause `{other}`")));
                }
            }
        }

        if clause_count == 0 {
            return Err(PolicyError::at(0, "empty policy"));
        }
        if armed && dry {
            return Err(PolicyError::at(0, "`dry` and `!` are contradictory"));
        }
        if off {
            // `off` must be the only clause.
            if clause_count != 1 || !rules.is_empty() || armed || dry {
                return Err(PolicyError::at(0, "`off` must be the only clause"));
            }
            return Ok(Policy::off());
        }
        if rules.is_empty() {
            return Err(PolicyError::at(0, "policy has no rule"));
        }

        rules.sort_by_key(|c| c.rule.order());
        Ok(Policy {
            rules,
            off: false,
            armed,
            every: every.unwrap_or(DEFAULT_EVERY),
            max: max.unwrap_or(DEFAULT_MAX),
            rate: rate.unwrap_or(DEFAULT_RATE),
        })
    }
}

impl fmt::Display for Policy {
    /// Canonical, normalized form. `parse(to_string(p)) == p`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.off {
            return f.write_str("off");
        }
        let mut parts: Vec<String> = Vec::new();
        for clause in &self.rules {
            parts.push(fmt_rule(clause));
        }
        if self.every != DEFAULT_EVERY {
            parts.push(format!("every={}", fmt_duration(self.every)));
        }
        if self.max != DEFAULT_MAX {
            parts.push(format!("max={}", self.max));
        }
        if self.rate != DEFAULT_RATE {
            parts.push(format!("rate={}/s", self.rate));
        }
        if self.armed {
            parts.push("!".to_string());
        }
        f.write_str(&parts.join("; "))
    }
}

fn fmt_rule(clause: &RuleClause) -> String {
    let mut args: Vec<String> = Vec::new();
    match &clause.rule {
        Rule::Age(d) => args.push(fmt_duration(*d)),
        Rule::Unused(d) => args.push(fmt_duration(*d)),
        Rule::Keep(n) => args.push(n.to_string()),
        Rule::Lru { high, low, of } => {
            args.push(format!("high={high}"));
            args.push(format!("low={low}"));
            // Emit `of` only when it is not the default for these units.
            if *of != default_of(*high, *low) {
                args.push(format!("of={of}"));
            }
        }
    }
    let f = &clause.filter;
    if let Some(v) = f.min_size {
        args.push(format!("min-size={}", fmt_size(v)));
    }
    if let Some(v) = f.max_size {
        args.push(format!("max-size={}", fmt_size(v)));
    }
    if let Some(g) = &f.only {
        args.push(format!("only='{g}'"));
    }
    for g in &f.except {
        args.push(format!("except='{g}'"));
    }
    if let Some(v) = f.uid {
        args.push(format!("uid={v}"));
    }
    if let Some(v) = f.gid {
        args.push(format!("gid={v}"));
    }
    if f.min_age != DEFAULT_MIN_AGE {
        args.push(format!("min-age={}", fmt_duration(f.min_age)));
    }
    let _ = f.is_default();
    format!("{}({})", clause.rule.kind_name(), args.join(", "))
}

/// Default `of=` for a watermark pair: `fs` if either is a percentage,
/// `subtree` otherwise.
fn default_of(high: Watermark, low: Watermark) -> Of {
    if matches!(high, Watermark::Percent(_)) || matches!(low, Watermark::Percent(_)) {
        Of::Fs
    } else {
        Of::Subtree
    }
}

// --- rule parsing ---

fn parse_rule(
    name: &str,
    name_off: usize,
    args_str: &str,
    args_off: usize,
) -> Result<RuleClause, PolicyError> {
    let args = split_args(args_str, args_off)?;
    let mut filter = Filter::default();

    // Split positional from named, applying filter keys as we go and
    // collecting rule-specific named args (high/low/of) for `lru`.
    let mut positional: Vec<&Arg> = Vec::new();
    let mut high: Option<(Watermark, usize)> = None;
    let mut low: Option<(Watermark, usize)> = None;
    let mut of: Option<Of> = None;

    for arg in &args {
        match &arg.key {
            None => positional.push(arg),
            Some(k) => match k.as_str() {
                "high" => high = Some((parse_watermark(&arg.val, arg.val_off)?, arg.val_off)),
                "low" => low = Some((parse_watermark(&arg.val, arg.val_off)?, arg.val_off)),
                "of" => {
                    of = Some(match arg.val.as_str() {
                        "fs" => Of::Fs,
                        "subtree" => Of::Subtree,
                        _ => {
                            return Err(PolicyError::at(
                                arg.val_off,
                                "`of` must be `fs` or `subtree`",
                            ))
                        }
                    })
                }
                "min-size" => filter.min_size = Some(parse_size(&arg.val, arg.val_off)?),
                "max-size" => filter.max_size = Some(parse_size(&arg.val, arg.val_off)?),
                "only" => filter.only = Some(parse_glob(&arg.val, arg.val_off)?),
                "except" => filter.except.push(parse_glob(&arg.val, arg.val_off)?),
                "uid" => filter.uid = Some(parse_int(&arg.val, arg.val_off)? as u32),
                "gid" => filter.gid = Some(parse_int(&arg.val, arg.val_off)? as u32),
                "min-age" => {
                    let d = parse_duration(&arg.val, arg.val_off)?;
                    if d.is_zero() {
                        return Err(PolicyError::at(arg.val_off, "`min-age` cannot be 0"));
                    }
                    filter.min_age = d;
                }
                other => {
                    return Err(PolicyError::at(
                        arg.key_off,
                        format!("unknown argument `{other}`"),
                    ))
                }
            },
        }
    }

    let rule = match name {
        "age" | "unused" => {
            let d = expect_one_duration(name, name_off, &positional)?;
            if name == "age" {
                Rule::Age(d)
            } else {
                Rule::Unused(d)
            }
        }
        "keep" => {
            if positional.len() != 1 {
                return Err(PolicyError::at(
                    name_off,
                    "`keep` takes exactly one count, e.g. keep(10)",
                ));
            }
            let n = parse_int(&positional[0].val, positional[0].val_off)?;
            Rule::Keep(n as u32)
        }
        "lru" => {
            if !positional.is_empty() {
                return Err(PolicyError::at(
                    positional[0].val_off,
                    "`lru` takes named arguments only, e.g. lru(high=85%, low=70%)",
                ));
            }
            let (high, _hoff) = high.ok_or_else(|| {
                PolicyError::at(
                    name_off,
                    "`lru` requires `high=`, e.g. lru(high=85%, low=70%)",
                )
            })?;
            let (low, loff) = low.ok_or_else(|| {
                PolicyError::at(
                    name_off,
                    "`lru` requires `low=`, e.g. lru(high=85%, low=70%)",
                )
            })?;
            let of = of.unwrap_or_else(|| default_of(high, low));
            // `of=subtree` with a percentage `high` has nothing to be a
            // percentage of.
            if of == Of::Subtree && matches!(high, Watermark::Percent(_)) {
                return Err(PolicyError::at(
                    name_off,
                    "`of=subtree` needs a size `high`, not a percentage",
                ));
            }
            // Same-unit `low >= high` is a static error; mixed units are
            // checked at run time (a size vs a percentage of a quota
            // that is not known at parse time).
            match (high, low) {
                (Watermark::Size(h), Watermark::Size(l)) if l >= h => {
                    return Err(PolicyError::at(loff, "`low` must be below `high`"));
                }
                (Watermark::Percent(h), Watermark::Percent(l)) if l >= h => {
                    return Err(PolicyError::at(loff, "`low` must be below `high`"));
                }
                _ => {}
            }
            if let Watermark::Percent(p) = low {
                if p >= 100 {
                    return Err(PolicyError::at(loff, "`low` percentage must be below 100%"));
                }
            }
            Rule::Lru { high, low, of }
        }
        other => return Err(PolicyError::at(name_off, format!("unknown rule `{other}`"))),
    };

    Ok(RuleClause { rule, filter })
}

fn expect_one_duration(
    name: &str,
    name_off: usize,
    positional: &[&Arg],
) -> Result<Duration, PolicyError> {
    if positional.len() != 1 {
        return Err(PolicyError::at(
            name_off,
            format!("`{name}` takes exactly one duration, e.g. {name}(30d)"),
        ));
    }
    parse_duration(&positional[0].val, positional[0].val_off)
}

// --- argument tokenization ---

struct Arg {
    key: Option<String>,
    key_off: usize,
    val: String,
    val_off: usize,
}

/// Split rule arguments on top-level commas (globs may be quoted, so a
/// comma inside quotes is not a separator), then split each on `=`.
fn split_args(s: &str, base: usize) -> Result<Vec<Arg>, PolicyError> {
    let mut out = Vec::new();
    if s.trim().is_empty() {
        return Ok(out);
    }
    for piece in split_top_quoted(s, ',', base)? {
        let text = piece.text.trim();
        let poff = piece.start + leading_ws(piece.text);
        if text.is_empty() {
            return Err(PolicyError::at(piece.start, "empty argument"));
        }
        // A named arg is `ident=value`; but a quoted glob or a `/s` rate
        // never appears here, and `=` inside a quoted glob is possible,
        // so only treat a leading unquoted `ident=` as a key.
        if let Some(eq) = unquoted_find(text, '=') {
            let key = text[..eq].trim();
            if is_ident(key) {
                out.push(Arg {
                    key: Some(key.to_string()),
                    key_off: poff + leading_ws(&text[..eq]),
                    val: text[eq + 1..].trim().to_string(),
                    val_off: poff + eq + 1 + leading_ws(&text[eq + 1..]),
                });
                continue;
            }
        }
        out.push(Arg {
            key: None,
            key_off: poff,
            val: text.to_string(),
            val_off: poff,
        });
    }
    Ok(out)
}

// --- value parsing ---

fn parse_watermark(s: &str, off: usize) -> Result<Watermark, PolicyError> {
    if let Some(stripped) = s.strip_suffix('%') {
        let p: u64 = stripped
            .trim()
            .parse()
            .map_err(|_| PolicyError::at(off, "invalid percentage"))?;
        if p == 0 || p > 100 {
            return Err(PolicyError::at(off, "percentage must be 1..=100"));
        }
        Ok(Watermark::Percent(p as u8))
    } else {
        Ok(Watermark::Size(parse_size(s, off)?))
    }
}

fn parse_duration(s: &str, off: usize) -> Result<Duration, PolicyError> {
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

fn parse_size(s: &str, off: usize) -> Result<u64, PolicyError> {
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

fn parse_int(s: &str, off: usize) -> Result<u64, PolicyError> {
    s.trim()
        .parse()
        .map_err(|_| PolicyError::at(off, format!("expected an integer, got `{s}`")))
}

fn parse_rate(s: &str, off: usize) -> Result<u32, PolicyError> {
    // `rate=<n>/s`; the `/s` suffix is optional but canonical.
    let s = s.trim();
    let digits = s.strip_suffix("/s").unwrap_or(s).trim();
    let n: u64 = digits
        .parse()
        .map_err(|_| PolicyError::at(off, format!("invalid rate `{s}` (use n/s)")))?;
    if n == 0 {
        return Err(PolicyError::at(off, "`rate` cannot be 0"));
    }
    Ok(n.min(u32::MAX as u64) as u32)
}

fn parse_glob(s: &str, off: usize) -> Result<String, PolicyError> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() >= 2
        && (bytes[0] == b'\'' || bytes[0] == b'"')
        && bytes[bytes.len() - 1] == bytes[0]
    {
        Ok(s[1..s.len() - 1].to_string())
    } else if bytes.first() == Some(&b'\'') || bytes.first() == Some(&b'"') {
        Err(PolicyError::at(off, "unterminated glob quote"))
    } else {
        // A bare (unquoted) glob is accepted too.
        Ok(s.to_string())
    }
}

// --- small lexical helpers ---

struct Span<'a> {
    text: &'a str,
    start: usize,
}

/// Split on `sep` at the top level (no nesting to track for `;`).
fn split_top(s: &str, sep: char) -> Vec<Span<'_>> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        if c == sep {
            out.push(Span {
                text: &s[start..i],
                start,
            });
            start = i + c.len_utf8();
        }
    }
    out.push(Span {
        text: &s[start..],
        start,
    });
    out
}

/// Split on `sep`, honouring single/double quotes so a separator inside
/// a quoted glob does not split.
fn split_top_quoted(s: &str, sep: char, base: usize) -> Result<Vec<Span<'_>>, PolicyError> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut quote: Option<char> = None;
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => {
                if c == '\'' || c == '"' {
                    quote = Some(c);
                } else if c == sep {
                    out.push(Span {
                        text: &s[start..i],
                        start: base + start,
                    });
                    start = i + c.len_utf8();
                }
            }
        }
    }
    if quote.is_some() {
        return Err(PolicyError::at(base + start, "unterminated glob quote"));
    }
    out.push(Span {
        text: &s[start..],
        start: base + start,
    });
    Ok(out)
}

/// Find `c` outside any quoted region, returning a byte offset.
fn unquoted_find(s: &str, c: char) -> Option<usize> {
    let mut quote: Option<char> = None;
    for (i, ch) in s.char_indices() {
        match quote {
            Some(q) => {
                if ch == q {
                    quote = None;
                }
            }
            None => {
                if ch == '\'' || ch == '"' {
                    quote = Some(ch);
                } else if ch == c {
                    return Some(i);
                }
            }
        }
    }
    None
}

fn leading_ws(s: &str) -> usize {
    s.len() - s.trim_start().len()
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

// --- canonical value formatting ---

/// Largest exact duration unit.
fn fmt_duration(d: Duration) -> String {
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
fn fmt_size(b: u64) -> String {
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

    fn p(s: &str) -> Policy {
        Policy::parse(s).unwrap_or_else(|e| panic!("parse {s:?}: {e}"))
    }

    #[test]
    fn examples_parse() {
        for ex in [
            "age(90d)",
            "unused(30d, except='*.keep', min-size=1M)",
            "lru(high=85%, low=70%)",
            "lru(high=500G, low=300G)",
            "age(180d); every=6h; max=50000",
            "keep(10, only='core.*')",
            "off",
        ] {
            let _ = p(ex);
        }
    }

    #[test]
    fn roundtrip_is_stable() {
        for ex in [
            "age(90d)",
            "unused(30d, min-size=1M, except='*.keep')",
            "lru(high=85%, low=70%)",
            "lru(high=500G, low=300G)",
            "lru(high=500G, low=70%)",
            "lru(high=500G, low=300G, of=fs)",
            "age(180d); every=6h; max=50000; rate=10/s",
            "keep(10, only='core.*')",
            "age(1y); !",
            "off",
        ] {
            let parsed = p(ex);
            let printed = parsed.to_string();
            let reparsed = p(&printed);
            assert_eq!(parsed, reparsed, "roundtrip {ex:?} via {printed:?}");
        }
    }

    #[test]
    fn clause_order_is_irrelevant() {
        assert_eq!(p("keep(10); age(90d)"), p("age(90d); keep(10)"));
    }

    #[test]
    fn arming_token() {
        assert!(!p("age(90d)").armed);
        assert!(p("age(90d); !").armed);
    }

    #[test]
    fn units_and_defaults() {
        assert_eq!(p("age(2h)").rules.len(), 1);
        let pol = p("age(90d)");
        assert_eq!(pol.every, DEFAULT_EVERY);
        assert_eq!(pol.max, DEFAULT_MAX);
        assert_eq!(pol.rate, DEFAULT_RATE);
        // Size units are binary.
        match &p("lru(high=1G, low=512M)").rules[0].rule {
            Rule::Lru {
                high: Watermark::Size(h),
                low: Watermark::Size(l),
                of: Of::Subtree,
            } => {
                assert_eq!(*h, 1024 * 1024 * 1024);
                assert_eq!(*l, 512 * 1024 * 1024);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn default_of_follows_units() {
        match p("lru(high=85%, low=70%)").rules[0].rule {
            Rule::Lru { of: Of::Fs, .. } => {}
            ref o => panic!("expected of=fs, got {o:?}"),
        }
        match p("lru(high=500G, low=300G)").rules[0].rule {
            Rule::Lru {
                of: Of::Subtree, ..
            } => {}
            ref o => panic!("expected of=subtree, got {o:?}"),
        }
    }

    fn err(s: &str) -> PolicyError {
        Policy::parse(s).expect_err(&format!("expected error for {s:?}"))
    }

    #[test]
    fn rejections_point_at_the_token() {
        // Bare positional numbers for lru.
        assert!(err("lru(70,80)").msg.contains("named arguments"));
        // Duration missing a unit.
        assert!(err("age(60)").msg.contains("no unit"));
        // off must stand alone.
        assert!(err("age(60d); off").msg.contains("only"));
        // dry and ! contradict.
        assert!(err("age(1d); dry; !").msg.contains("contradictory"));
        // unknown rule / arg.
        assert!(err("bogus(1d)").msg.contains("unknown rule"));
        assert!(err("age(1d, wat=3)").msg.contains("unknown argument"));
        // min-age cannot be zero.
        assert!(err("age(1d, min-age=0s)").msg.contains("min-age"));
        // lru missing a watermark.
        assert!(err("lru(high=80%)").msg.contains("low"));
        assert!(err("lru(low=80%)").msg.contains("high"));
        // low >= high, same unit.
        assert!(err("lru(high=70%, low=80%)").msg.contains("below"));
        assert!(err("lru(high=100G, low=200G)").msg.contains("below"));
        // of=subtree with a percentage high.
        assert!(err("lru(high=80%, low=70%, of=subtree)")
            .msg
            .contains("of=subtree"));
        // duplicate rule kind.
        assert!(err("age(1d); age(2d)").msg.contains("duplicate"));
        // unterminated glob.
        assert!(err("age(1d, only='foo)").msg.contains("glob"));
        // empty policy / clause.
        assert!(err("").msg.contains("empty"));
        assert!(err("age(1d);;").msg.contains("empty"));
    }

    #[test]
    fn caret_offset_lands_on_token() {
        let e = err("age(90d); bogus");
        // The offset should point at `bogus`.
        assert_eq!(&"age(90d); bogus"[e.offset..], "bogus");
    }

    #[test]
    fn needs_atime_flag() {
        assert!(!p("age(90d)").needs_atime());
        assert!(p("unused(30d)").needs_atime());
        assert!(p("lru(high=85%, low=70%)").needs_atime());
    }

    #[test]
    fn fuzz_never_panics() {
        // Deterministic pseudo-random byte soup: parse must always
        // terminate and never panic.
        let mut state: u64 = 0x9e3779b97f4a7c15;
        let alphabet = b"age(unsd)lrkp%,;=!'\"0123456789GMKThdwy -_.*?/";
        for _ in 0..20_000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = (state >> 24) as usize % 24;
            let mut s = String::new();
            let mut st = state;
            for _ in 0..len {
                st = st.wrapping_mul(6364136223846793005).wrapping_add(1);
                s.push(alphabet[(st >> 33) as usize % alphabet.len()] as char);
            }
            let _ = Policy::parse(&s);
        }
    }
}
