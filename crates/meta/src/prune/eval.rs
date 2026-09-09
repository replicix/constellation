//! Pure per-entry evaluation of a prune [`Policy`] (plan 22).
//!
//! Everything here is a function of the entry's replicated inode facts
//! and a caller-supplied `now_ns` — no I/O, no clock of its own — so the
//! singleton pruner reaches the same verdict on any node. `age`/`unused`
//! are deterministic and evaluated per entry; `keep` needs per-directory
//! context and `lru` needs a global heap, so those two only expose
//! candidacy here and the pruner does the ranking.

use super::policy::{Filter, Policy, Rule, Watermark};
use constellation_fs_core::InodeKind;

/// The replicated facts the evaluator needs about one directory entry.
#[derive(Debug, Clone)]
pub struct EntryFacts {
    pub name: String,
    pub kind: InodeKind,
    pub size: u64,
    pub uid: u32,
    pub gid: u32,
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub nlink: u32,
}

/// An entry proposed for pruning, tagged with which rule proposed it and
/// (for `lru`) its coldness key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Selected by a deterministic rule (`age`/`unused`).
    Prune,
    /// Not selected by any deterministic rule this pass.
    Keep,
}

/// An `lru` candidate: the coldness key (atime) plus the entry's size,
/// so the pruner's heap can rank victims and track freed bytes.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub atime_ns: i64,
    pub size: u64,
}

impl Policy {
    /// Deterministic verdict from `age`/`unused` (union). `keep` and
    /// `lru` are handled by the pruner and never contribute here.
    pub fn deterministic_verdict(&self, facts: &EntryFacts, now_ns: i64) -> Verdict {
        if !is_prunable_kind(facts.kind) {
            return Verdict::Keep;
        }
        for clause in &self.rules {
            let selects = match &clause.rule {
                Rule::Age(d) => older_than(facts.mtime_ns, now_ns, d.as_secs()),
                Rule::Unused(d) => older_than(facts.atime_ns, now_ns, d.as_secs()),
                // Non-deterministic rules do not contribute here.
                Rule::Lru { .. } | Rule::Keep(_) => false,
            };
            if selects && passes_filter(&clause.filter, facts, now_ns) {
                return Verdict::Prune;
            }
        }
        Verdict::Keep
    }

    /// If any `lru` rule matches this entry (filter + `nlink == 1`),
    /// return its candidacy. `lru` never selects a hardlinked entry:
    /// unlinking it frees no bytes, so it cannot move the watermark.
    pub fn lru_candidate(&self, facts: &EntryFacts, now_ns: i64) -> Option<Candidate> {
        if !is_prunable_kind(facts.kind) || facts.nlink > 1 {
            return None;
        }
        for clause in &self.rules {
            if matches!(clause.rule, Rule::Lru { .. })
                && passes_filter(&clause.filter, facts, now_ns)
            {
                return Some(Candidate {
                    atime_ns: facts.atime_ns,
                    size: facts.size,
                });
            }
        }
        None
    }

    /// If a `keep` rule applies to this entry (filter matches), return
    /// `(keep_n, mtime_ns)` so the pruner can rank per directory. `keep`
    /// prunes hardlinked names like `age` does (safe: data survives the
    /// remaining link).
    pub fn keep_candidate(&self, facts: &EntryFacts, now_ns: i64) -> Option<(u32, i64)> {
        if !is_prunable_kind(facts.kind) {
            return None;
        }
        for clause in &self.rules {
            if let Rule::Keep(n) = clause.rule {
                if passes_filter(&clause.filter, facts, now_ns) {
                    return Some((n, facts.mtime_ns));
                }
            }
        }
        None
    }

    /// Whether this policy has any `lru` rule (drives the whole-FS /
    /// subtree accounting the pruner sets up).
    pub fn has_lru(&self) -> bool {
        self.rules
            .iter()
            .any(|c| matches!(c.rule, Rule::Lru { .. }))
    }

    /// The single `lru` rule's `(high, low, of)`, if present.
    pub fn lru_watermarks(&self) -> Option<(Watermark, Watermark, super::policy::Of)> {
        self.rules.iter().find_map(|c| match c.rule {
            Rule::Lru { high, low, of } => Some((high, low, of)),
            _ => None,
        })
    }
}

/// Directories never get pruned (a tree's shape survives its contents),
/// nor do the synthetic snapshot/clone roots the caller filters upstream.
fn is_prunable_kind(kind: InodeKind) -> bool {
    kind != InodeKind::Dir
}

fn older_than(ts_ns: i64, now_ns: i64, secs: u64) -> bool {
    let threshold = (secs as i128) * 1_000_000_000;
    (now_ns as i128 - ts_ns as i128) >= threshold
}

/// Filter gate shared by every rule: the `min-age` floor, size bounds,
/// uid/gid, and `only`/`except` globs.
///
/// The floor is measured against `mtime` (content age), deliberately not
/// `ctime`: a pure metadata change (chmod/chown) must not reset a
/// retention timer, or an unrelated permission fix would resurrect a
/// file the policy had already aged out.
pub fn passes_filter(filter: &Filter, facts: &EntryFacts, now_ns: i64) -> bool {
    if !older_than(facts.mtime_ns, now_ns, filter.min_age.as_secs()) {
        return false;
    }
    if let Some(min) = filter.min_size {
        if facts.size < min {
            return false;
        }
    }
    if let Some(max) = filter.max_size {
        if facts.size > max {
            return false;
        }
    }
    if let Some(uid) = filter.uid {
        if facts.uid != uid {
            return false;
        }
    }
    if let Some(gid) = filter.gid {
        if facts.gid != gid {
            return false;
        }
    }
    if let Some(only) = &filter.only {
        if !glob_match(only, &facts.name) {
            return false;
        }
    }
    for except in &filter.except {
        if glob_match(except, &facts.name) {
            return false;
        }
    }
    true
}

/// Minimal shell-style glob: `*` (any run), `?` (one char). No character
/// classes — the filter names are simple by design.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    // Iterative backtracking matcher.
    let (mut pi, mut ni) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ni < n.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ni;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ni = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prune::policy::Policy;

    const S: i64 = 1_000_000_000;

    fn facts(name: &str, size: u64, atime_s: i64, mtime_s: i64) -> EntryFacts {
        EntryFacts {
            name: name.into(),
            kind: InodeKind::File,
            size,
            uid: 1000,
            gid: 1000,
            atime_ns: atime_s * S,
            mtime_ns: mtime_s * S,
            ctime_ns: mtime_s * S,
            nlink: 1,
        }
    }

    #[test]
    fn glob() {
        assert!(glob_match("*.keep", "core.keep"));
        assert!(glob_match("core.*", "core.1234"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("*.keep", "keep.txt"));
    }

    #[test]
    fn age_selects_by_mtime() {
        let pol = Policy::parse("age(30d)").unwrap();
        let now = 100 * 86_400 * S;
        // mtime 40 days ago -> older than 30d -> prune.
        let old = facts(
            "f",
            10,
            100 * 86_400 - 40 * 86_400,
            100 * 86_400 - 40 * 86_400,
        );
        assert_eq!(pol.deterministic_verdict(&old, now), Verdict::Prune);
        // mtime 10 days ago -> keep.
        let fresh = facts(
            "f",
            10,
            100 * 86_400 - 10 * 86_400,
            100 * 86_400 - 10 * 86_400,
        );
        assert_eq!(pol.deterministic_verdict(&fresh, now), Verdict::Keep);
    }

    #[test]
    fn min_age_floor_spares_young_entries() {
        // age(0s) would prune everything, but min-age floors at 1h.
        let pol = Policy::parse("age(1s)").unwrap();
        let now = 1_000_000 * S;
        // mtime 30 min ago: younger than the 1h floor -> keep.
        let young = facts("f", 1, 0, 1_000_000 - 1_800);
        assert_eq!(pol.deterministic_verdict(&young, now), Verdict::Keep);
        // mtime 2h ago: past the floor and past age(1s) -> prune.
        let old = facts("f", 1, 0, 1_000_000 - 7_200);
        assert_eq!(pol.deterministic_verdict(&old, now), Verdict::Prune);
    }

    #[test]
    fn except_glob_protects() {
        let pol = Policy::parse("age(1s, except='*.keep')").unwrap();
        let now = 1_000_000 * S;
        let keeper = facts("data.keep", 1, 0, 0);
        assert_eq!(pol.deterministic_verdict(&keeper, now), Verdict::Keep);
        let other = facts("data.tmp", 1, 0, 0);
        assert_eq!(pol.deterministic_verdict(&other, now), Verdict::Prune);
    }

    #[test]
    fn size_filter() {
        let pol = Policy::parse("age(1s, min-size=1M)").unwrap();
        let now = 1_000_000 * S;
        let small = facts("s", 1024, 0, 0);
        assert_eq!(pol.deterministic_verdict(&small, now), Verdict::Keep);
        let big = facts("b", 4 * 1024 * 1024, 0, 0);
        assert_eq!(pol.deterministic_verdict(&big, now), Verdict::Prune);
    }

    #[test]
    fn dirs_never_pruned() {
        let pol = Policy::parse("age(1s)").unwrap();
        let now = 1_000_000 * S;
        let mut d = facts("sub", 0, 0, 0);
        d.kind = InodeKind::Dir;
        assert_eq!(pol.deterministic_verdict(&d, now), Verdict::Keep);
    }

    #[test]
    fn lru_skips_hardlinks() {
        let pol = Policy::parse("lru(high=1G, low=512M)").unwrap();
        let now = 1_000_000 * S;
        let mut linked = facts("h", 4096, 0, 0);
        linked.nlink = 2;
        assert!(pol.lru_candidate(&linked, now).is_none());
        let single = facts("s", 4096, 0, 0);
        assert!(pol.lru_candidate(&single, now).is_some());
    }

    #[test]
    fn keep_candidate_reports_n_and_mtime() {
        let pol = Policy::parse("keep(3)").unwrap();
        let now = 1_000_000 * S;
        let f = facts("x", 1, 0, 500_000);
        assert_eq!(pol.keep_candidate(&f, now), Some((3, 500_000 * S)));
    }
}
