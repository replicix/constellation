//! Per-node session guarantees (plan 30 §M6): **read-your-writes** and
//! **monotonic reads**, checked over a recorded history.
//!
//! A chaos worker is one mount (one node) issuing one op at a time, so a
//! worker's ops in history order are that node's session. Cross-node
//! staleness is allowed (bounded close-to-open); these checkers only flag
//! a node going *backwards relative to itself*:
//!
//! - **read-your-writes**: after a worker's own write to a path returned
//!   `Ok`, its later observations of that path must not show a state
//!   strictly older than that write;
//! - **monotonic reads**: after a worker observed a path in some state,
//!   its later observations must not show a strictly older state.
//!
//! "Observation" is broader than a read: a `create`/`mkdir`/`link` that
//! fails with `EEXIST` observed the name present, an `unlink`/`rmdir`/
//! `rename`/`link` that fails with `ENOENT` observed it absent — plan 29
//! M6's `Exists` case (a refused create followed by a lookup that does
//! not find the name) is exactly a monotonic-reads violation here.
//!
//! # Black-box, conservative
//!
//! The checker sees no log positions, only the history. Each observation
//! is *explained* by the set of writes that could have produced what it
//! saw ([`explanations`]): the writes of that content (every op writes
//! unique content), or for "present" any write that creates the path,
//! for "absent" any removal plus the initial state. Writes with unknown
//! content (`append`, `write_at`, `truncate`, the target of a `rename` or
//! `link`) explain any content. Only writes that returned `Ok` or never
//! completed (in doubt) explain anything, and never one invoked after the
//! observation completed. A refusal was evaluated by the sequencer
//! against its current state, so for it an explanation that a
//! contradicting write certainly followed before the refused op was
//! issued is ruled out too (a read is served from a possibly stale
//! replica, so for it nothing is). An observation `O2` after `O1` in one session
//! is a violation only when **every** explanation of `O2` completed
//! before **any** explanation of `O1` was invoked (the history's event
//! indices are the coordinator's real-time order): `O2` then certainly
//! saw an older state. Ambiguous cases are never flagged, and an
//! observation nothing explains is skipped.
//!
//! # Enforced since M6 phase 2
//!
//! Reads wait on the node's `observed` watermark (plan 30 §M6), so
//! [`check_history_sessions`] fails on the first violation. Before phase 2
//! it only reported them: two saved soak histories showed real
//! monotonic-reads violations (the losers of a `mkdir` storm saw `EEXIST`
//! and then no directory; the losers of an `unlink` storm saw `ENOENT` and
//! then the file). `CONSTELLATION_CHAOS_ENFORCE_SESSIONS=0` reports only,
//! for histories recorded by an older build.

use crate::check::CheckFailure;
use crate::history::{EventKind, History};
use crate::op::{hash_bytes, Complete, Op, Outcome};
use std::collections::HashMap;

/// Plan 30 §M6 phase 2: reads wait on the `observed` watermark, so a
/// violation fails the check. Set `CONSTELLATION_CHAOS_ENFORCE_SESSIONS=0`
/// to only report (e.g. to check a history recorded before M6).
pub const ENFORCE_SESSION_GUARANTEES: bool = true;

/// `1` enforces the session checkers, `0` only reports them; unset follows
/// [`ENFORCE_SESSION_GUARANTEES`].
pub const ENFORCE_ENV: &str = "CONSTELLATION_CHAOS_ENFORCE_SESSIONS";

/// Whether session violations fail the check.
pub fn enforced() -> bool {
    match std::env::var(ENFORCE_ENV).as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        _ => ENFORCE_SESSION_GUARANTEES,
    }
}

/// What [`check_sessions`] found.
#[derive(Debug, Default)]
pub struct SessionReport {
    /// Observations that had at least one explanation (were judged).
    pub judged: usize,
    /// Observations nothing in the history explains (skipped).
    pub unexplained: usize,
    /// Every violation, in history order.
    pub violations: Vec<CheckFailure>,
}

/// Run both checkers; fail on the first violation only when `enforce`.
/// Otherwise violations are logged and the check passes.
pub fn check_history_sessions(
    history: &History,
    enforce: bool,
) -> Result<SessionReport, CheckFailure> {
    let report = check_sessions(history);
    if let Some(first) = report.violations.first() {
        if enforce {
            return Err(first.clone());
        }
        tracing::warn!(
            violations = report.violations.len(),
            first = %first,
            "session guarantees (read-your-writes, monotonic reads) violated, \
             reported only ({ENFORCE_ENV}=0)"
        );
    }
    Ok(report)
}

/// A write that may explain an observation.
#[derive(Debug, Clone)]
struct Cand {
    kind: CandKind,
    /// Invoke and complete event indices (`hi` is `i64::MAX` for an op
    /// that never completed).
    lo: i64,
    hi: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CandKind {
    /// Whole known content (`create`, `write_full`).
    Content(String),
    /// Unknown content (`append`, `write_at`, `truncate`, the target of a
    /// `rename`/`link`): explains any content, and presence.
    AnyContent,
    /// Presence without content (`mkdir`).
    Present,
    /// `unlink`, `rmdir`, the source of a `rename`.
    Removal,
}

/// What an op observed about a path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Fact {
    Content(String),
    Present,
    Absent,
}

/// How an observation was made. A refused mutation was evaluated by the
/// sequencer against the current state (the linearizability and
/// exactly-once checkers hold it to that), so an explanation that another
/// write had certainly overwritten before it was issued is ruled out. A
/// read is served from the node's replica, which may be stale, so every
/// explanation stays possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Via {
    Refusal,
    Read,
}

/// One completed (or in-doubt) op.
struct Done {
    worker: usize,
    op_id: u64,
    op: Op,
    lo: i64,
    hi: i64,
    complete: Option<Complete>,
}

fn done_ops(history: &History) -> Vec<Done> {
    let mut open: HashMap<u64, (usize, Op, i64)> = HashMap::new();
    let mut out = Vec::new();
    for ev in history.events() {
        match ev.kind {
            EventKind::Invoke => {
                if let Some(op) = &ev.op {
                    open.insert(ev.op_id, (ev.worker_id, op.clone(), ev.index as i64));
                }
            }
            EventKind::Ok | EventKind::Fail => {
                if let Some((worker, op, lo)) = open.remove(&ev.op_id) {
                    out.push(Done {
                        worker,
                        op_id: ev.op_id,
                        op,
                        lo,
                        hi: ev.index as i64,
                        complete: ev.complete.clone(),
                    });
                }
            }
            EventKind::Info => {}
        }
    }
    for (op_id, (worker, op, lo)) in open {
        out.push(Done {
            worker,
            op_id,
            op,
            lo,
            hi: i64::MAX,
            complete: None,
        });
    }
    out.sort_by_key(|d| d.lo);
    out
}

fn is_ok(d: &Done) -> bool {
    d.complete
        .as_ref()
        .is_some_and(|c| c.outcome == Outcome::Ok)
}

fn failed_with(d: &Done, name: &str) -> bool {
    d.complete
        .as_ref()
        .is_some_and(|c| c.outcome == Outcome::Fail && c.errno_name.as_deref() == Some(name))
}

/// The writes an op may have made, per path (`Ok` or in doubt only).
fn candidates(d: &Done) -> Vec<(String, CandKind)> {
    if !(is_ok(d) || d.complete.is_none()) {
        return Vec::new();
    }
    match &d.op {
        Op::Create { path, content } | Op::WriteFull { path, content } => {
            vec![(path.clone(), CandKind::Content(hash_bytes(content)))]
        }
        Op::Mkdir { path } => vec![(path.clone(), CandKind::Present)],
        Op::Unlink { path } | Op::Rmdir { path } => vec![(path.clone(), CandKind::Removal)],
        Op::Rename { from, to } => vec![
            (from.clone(), CandKind::Removal),
            (to.clone(), CandKind::AnyContent),
        ],
        Op::Link { to, .. } => vec![(to.clone(), CandKind::AnyContent)],
        Op::WriteAt { path, .. } | Op::Append { path, .. } | Op::Truncate { path, .. } => {
            vec![(path.clone(), CandKind::AnyContent)]
        }
        Op::Chmod { .. } | Op::Read { .. } | Op::ReadAt { .. } | Op::Stat { .. } => Vec::new(),
    }
}

/// What a completed op observed (other ops' effects it saw).
fn observations(d: &Done) -> Vec<(String, Fact, Via)> {
    let Some(c) = &d.complete else {
        return Vec::new();
    };
    let enoent = failed_with(d, "ENOENT");
    let eexist = failed_with(d, "EEXIST");
    match &d.op {
        Op::Read { path } if c.outcome == Outcome::Ok => match &c.value_hash {
            Some(h) => vec![(path.clone(), Fact::Content(h.clone()), Via::Read)],
            None => vec![(path.clone(), Fact::Present, Via::Read)],
        },
        Op::ReadAt { path, .. } | Op::Stat { path } if c.outcome == Outcome::Ok => {
            vec![(path.clone(), Fact::Present, Via::Read)]
        }
        Op::Read { path } | Op::ReadAt { path, .. } | Op::Stat { path } if enoent => {
            vec![(path.clone(), Fact::Absent, Via::Read)]
        }
        Op::Unlink { path } | Op::Rmdir { path } | Op::Chmod { path, .. } if enoent => {
            vec![(path.clone(), Fact::Absent, Via::Refusal)]
        }
        Op::Rename { from, .. } | Op::Link { from, .. } if enoent => {
            vec![(from.clone(), Fact::Absent, Via::Refusal)]
        }
        Op::Create { path, .. } | Op::Mkdir { path } if eexist => {
            vec![(path.clone(), Fact::Present, Via::Refusal)]
        }
        Op::Link { to, .. } if eexist => vec![(to.clone(), Fact::Present, Via::Refusal)],
        _ => Vec::new(),
    }
}

/// The own writes an op makes that its session must see afterwards
/// (`Ok` only). Writes that may be no-ops (`append` of nothing,
/// `truncate` to the current size, `write_at` of identical bytes) are
/// left out, so a later read showing the earlier content is not flagged.
fn own_writes(d: &Done) -> Vec<String> {
    if !is_ok(d) {
        return Vec::new();
    }
    match &d.op {
        Op::Create { path, .. }
        | Op::WriteFull { path, .. }
        | Op::Mkdir { path }
        | Op::Unlink { path }
        | Op::Rmdir { path } => vec![path.clone()],
        Op::Rename { from, to } => vec![from.clone(), to.clone()],
        Op::Link { to, .. } => vec![to.clone()],
        _ => Vec::new(),
    }
}

/// `(lo, hi)` of the explanations of `fact` at `path` for an observation
/// made between event indices `issued` and `done`: the earliest invoke and
/// the latest complete among them, or `None` when nothing explains it. The
/// initial state (`-1`) explains every absence. A write invoked after
/// `done` explains nothing; for a refusal (`Via::Refusal`), neither does
/// one that a write contradicting `fact` certainly followed before
/// `issued`.
fn explanations(
    by_path: &HashMap<&str, Vec<Cand>>,
    path: &str,
    fact: &Fact,
    via: Via,
    (issued, done): (i64, i64),
) -> Option<(i64, i64)> {
    let fits = |k: &CandKind| match (fact, k) {
        (Fact::Content(h), CandKind::Content(c)) => h == c,
        (Fact::Content(_), CandKind::AnyContent) => true,
        (Fact::Present, CandKind::Content(_) | CandKind::AnyContent | CandKind::Present) => true,
        (Fact::Absent, CandKind::Removal) => true,
        _ => false,
    };
    let writes = by_path.get(path).map(Vec::as_slice).unwrap_or(&[]);
    let overwritten = |hi: i64| {
        via == Via::Refusal
            && writes
                .iter()
                .any(|w| !fits(&w.kind) && hi < w.lo && w.hi < issued)
    };
    let initial = (matches!(fact, Fact::Absent) && !overwritten(-1)).then_some((-1, -1));
    writes
        .iter()
        .filter(|c| c.lo <= done && fits(&c.kind) && !overwritten(c.hi))
        .map(|c| (c.lo, c.hi))
        .chain(initial)
        .reduce(|(lo, hi), (l, h)| (lo.min(l), hi.max(h)))
}

/// Both checkers over `history`: every violation, never failing.
pub fn check_sessions(history: &History) -> SessionReport {
    let done = done_ops(history);
    let mut by_path: HashMap<&str, Vec<Cand>> = HashMap::new();
    let mut owned: Vec<Vec<(String, CandKind)>> = Vec::with_capacity(done.len());
    for d in &done {
        owned.push(candidates(d));
    }
    for (d, cands) in done.iter().zip(&owned) {
        for (path, kind) in cands {
            by_path.entry(path.as_str()).or_default().push(Cand {
                kind: kind.clone(),
                lo: d.lo,
                hi: d.hi,
            });
        }
    }

    // Per (worker, path): the floor an observation must not fall below —
    // the latest own write's invoke (read-your-writes) and the latest
    // observation's earliest explanation (monotonic reads), each with the
    // op that set it.
    #[derive(Default, Clone, Copy)]
    struct Floor {
        own: Option<(i64, u64)>,
        seen: Option<(i64, u64)>,
    }
    let mut floors: HashMap<(usize, String), Floor> = HashMap::new();
    let mut report = SessionReport::default();
    for d in &done {
        if d.complete.is_none() {
            continue;
        }
        for (path, fact, via) in observations(d) {
            let Some((lo, hi)) = explanations(&by_path, &path, &fact, via, (d.lo, d.hi)) else {
                report.unexplained += 1;
                continue;
            };
            report.judged += 1;
            let floor = floors.entry((d.worker, path.clone())).or_default();
            let violated = |f: Option<(i64, u64)>| f.filter(|(at, _)| hi < *at);
            if let Some((_, by)) = violated(floor.own) {
                report.violations.push(CheckFailure {
                    checker: "read_your_writes".into(),
                    message: format!(
                        "worker {} observed {path} as {fact:?} (every explanation completed \
                         before its own write, op {by}, was issued) in op {} {:?}",
                        d.worker, d.op_id, d.op
                    ),
                    op_ids: vec![by, d.op_id],
                });
            } else if let Some((_, by)) = violated(floor.seen) {
                report.violations.push(CheckFailure {
                    checker: "monotonic_reads".into(),
                    message: format!(
                        "worker {} observed {path} as {fact:?} in op {} {:?}, strictly older \
                         than what it observed in op {by}",
                        d.worker, d.op_id, d.op
                    ),
                    op_ids: vec![by, d.op_id],
                });
            }
            if floor.seen.is_none_or(|(at, _)| lo > at) {
                floor.seen = Some((lo, d.op_id));
            }
        }
        for path in own_writes(d) {
            let floor = floors.entry((d.worker, path)).or_default();
            if floor.own.is_none_or(|(at, _)| d.lo > at) {
                floor.own = Some((d.lo, d.op_id));
            }
        }
    }
    report
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn phase_2_enforces_by_default() {
        const { assert!(ENFORCE_SESSION_GUARANTEES) };
    }

    fn ok_c() -> Complete {
        Complete {
            outcome: Outcome::Ok,
            errno: None,
            errno_name: None,
            value_hash: None,
            bytes: None,
            size: None,
            mode: None,
            wall_ns: 1,
        }
    }

    fn fail_c(name: &str) -> Complete {
        Complete {
            outcome: Outcome::Fail,
            errno: Some(1),
            errno_name: Some(name.into()),
            ..ok_c()
        }
    }

    fn read_c(bytes: &[u8]) -> Complete {
        Complete {
            value_hash: Some(hash_bytes(bytes)),
            bytes: Some(bytes.to_vec()),
            ..ok_c()
        }
    }

    fn create(path: &str, content: &[u8]) -> Op {
        Op::Create {
            path: path.into(),
            content: content.to_vec(),
        }
    }

    fn write(path: &str, content: &[u8]) -> Op {
        Op::WriteFull {
            path: path.into(),
            content: content.to_vec(),
        }
    }

    fn read(path: &str) -> Op {
        Op::Read { path: path.into() }
    }

    /// Sequential ops `(worker, op, completion)`: each completes before
    /// the next is invoked.
    fn sequential(ops: Vec<(usize, Op, Complete)>) -> History {
        let mut h = History::new();
        for (i, (w, op, c)) in ops.into_iter().enumerate() {
            h.record_invoke(w, i as u64 + 1, op);
            h.record_complete(w, i as u64 + 1, c);
        }
        h
    }

    fn checkers(h: &History) -> Vec<String> {
        check_sessions(h)
            .violations
            .into_iter()
            .map(|v| v.checker)
            .collect()
    }

    #[test]
    fn own_overwrite_then_stale_read_violates_read_your_writes() {
        let h = sequential(vec![
            (0, create("f", b"old"), ok_c()),
            (1, write("f", b"new"), ok_c()),
            (1, read("f"), read_c(b"old")),
        ]);
        assert_eq!(checkers(&h), vec!["read_your_writes"]);
    }

    #[test]
    fn own_create_then_enoent_violates_read_your_writes() {
        let h = sequential(vec![
            (1, create("f", b"x"), ok_c()),
            (1, Op::Stat { path: "f".into() }, fail_c("ENOENT")),
        ]);
        assert_eq!(checkers(&h), vec!["read_your_writes"]);
    }

    #[test]
    fn own_unlink_then_old_content_violates_read_your_writes() {
        let h = sequential(vec![
            (0, create("f", b"x"), ok_c()),
            (1, Op::Unlink { path: "f".into() }, ok_c()),
            (1, read("f"), read_c(b"x")),
        ]);
        assert_eq!(checkers(&h), vec!["read_your_writes"]);
    }

    #[test]
    fn newer_then_older_read_violates_monotonic_reads() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (0, write("f", b"b"), ok_c()),
            (1, read("f"), read_c(b"b")),
            (1, read("f"), read_c(b"a")),
        ]);
        assert_eq!(checkers(&h), vec!["monotonic_reads"]);
    }

    /// Plan 29 M6's `Exists` anomaly: a refused create, then a lookup
    /// that does not find the name nobody removed.
    #[test]
    fn eexist_then_enoent_violates_monotonic_reads() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (1, create("f", b"b"), fail_c("EEXIST")),
            (1, Op::Stat { path: "f".into() }, fail_c("ENOENT")),
        ]);
        assert_eq!(checkers(&h), vec!["monotonic_reads"]);
    }

    #[test]
    fn enoent_refusal_then_stale_content_violates_monotonic_reads() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (0, Op::Unlink { path: "f".into() }, ok_c()),
            (1, Op::Unlink { path: "f".into() }, fail_c("ENOENT")),
            (1, read("f"), read_c(b"a")),
        ]);
        assert_eq!(checkers(&h), vec!["monotonic_reads"]);
    }

    /// Cross-node staleness is allowed: another worker's first read may
    /// be older than a write it never observed.
    #[test]
    fn another_nodes_stale_first_read_is_fine() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (0, write("f", b"b"), ok_c()),
            (1, read("f"), read_c(b"a")),
            (1, read("f"), read_c(b"b")),
        ]);
        assert!(checkers(&h).is_empty());
    }

    /// Overlapping writes may be ordered either way, so reading either
    /// value in either order is not a violation.
    #[test]
    fn concurrent_writes_are_ambiguous_and_not_flagged() {
        let mut h = History::new();
        h.record_invoke(0, 1, write("f", b"a"));
        h.record_invoke(1, 2, write("f", b"b"));
        h.record_complete(0, 1, ok_c());
        h.record_complete(1, 2, ok_c());
        h.record_invoke(2, 3, read("f"));
        h.record_complete(2, 3, read_c(b"b"));
        h.record_invoke(2, 4, read("f"));
        h.record_complete(2, 4, read_c(b"a"));
        h.record_invoke(0, 5, read("f"));
        h.record_complete(0, 5, read_c(b"b"));
        assert!(checkers(&h).is_empty());
    }

    /// An unlink between the observations explains the absence.
    #[test]
    fn removal_explains_absence_after_presence() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (1, create("f", b"b"), fail_c("EEXIST")),
            (0, Op::Unlink { path: "f".into() }, ok_c()),
            (1, Op::Stat { path: "f".into() }, fail_c("ENOENT")),
        ]);
        assert!(checkers(&h).is_empty());
    }

    /// An in-doubt write (invoked, never completed) may have landed any
    /// time after its invoke, so it explains later reads.
    #[test]
    fn in_doubt_write_explains_what_follows() {
        let mut h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (1, read("f"), read_c(b"a")),
        ]);
        h.record_invoke(0, 10, Op::Unlink { path: "f".into() });
        h.record_invoke(1, 11, Op::Stat { path: "f".into() });
        h.record_complete(1, 11, fail_c("ENOENT"));
        h.record_invoke(1, 12, read("f"));
        h.record_complete(1, 12, fail_c("ENOENT"));
        assert!(checkers(&h).is_empty());
    }

    /// A write invoked after an observation completed cannot explain it.
    #[test]
    fn a_later_write_does_not_explain_an_earlier_read() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (0, write("f", b"b"), ok_c()),
            (1, read("f"), read_c(b"b")),
            (1, read("f"), read_c(b"a")),
            (0, write("f", b"a2"), ok_c()),
        ]);
        assert_eq!(checkers(&h), vec!["monotonic_reads"]);
    }

    /// A value nothing wrote is skipped (counted), not judged.
    #[test]
    fn unexplained_values_are_skipped() {
        let h = sequential(vec![
            (0, create("f", b"a"), ok_c()),
            (0, read("f"), read_c(b"torn")),
        ]);
        let r = check_sessions(&h);
        assert!(r.violations.is_empty());
        assert_eq!((r.judged, r.unexplained), (0, 1));
    }

    #[test]
    fn not_enforced_reports_and_passes_enforced_fails() {
        let h = sequential(vec![
            (1, create("f", b"x"), ok_c()),
            (1, read("f"), fail_c("ENOENT")),
        ]);
        let r = check_history_sessions(&h, false).unwrap();
        assert_eq!(r.violations.len(), 1);
        let e = check_history_sessions(&h, true).unwrap_err();
        assert_eq!(e.checker, "read_your_writes");
    }

    /// Report on a saved history regardless of the other checkers
    /// (`chaos check` stops at the first failing one):
    /// `CHAOS_SESSION_HISTORY=<history.jsonl> cargo test -p
    /// constellation-chaos sessions::tests::report_saved_history --
    /// --ignored --nocapture`.
    #[test]
    #[ignore]
    fn report_saved_history() {
        let path = std::env::var("CHAOS_SESSION_HISTORY").expect("CHAOS_SESSION_HISTORY");
        let h = History::load_jsonl(std::path::Path::new(&path)).unwrap();
        let r = check_sessions(&h);
        println!(
            "{path}: {} judged, {} unexplained, {} violations",
            r.judged,
            r.unexplained,
            r.violations.len()
        );
        for v in r.violations.iter().take(10) {
            println!("  {v}");
        }
    }

    /// Wired into the chaos entry point: `check_history` fails on a
    /// violating history (enforced since M6 phase 2), and only reports it
    /// with `CONSTELLATION_CHAOS_ENFORCE_SESSIONS=0`.
    #[test]
    fn check_history_enforces_since_phase_2() {
        // An overwritten path (so M4's Elle checker, which needs
        // single-assignment paths and would call a stale read a
        // linearizability cycle, leaves it out) read newer, then older.
        let h = sequential(vec![
            (0, write("f", b"a"), ok_c()),
            (0, write("f", b"b"), ok_c()),
            (1, read("f"), read_c(b"b")),
            (1, read("f"), read_c(b"a")),
        ]);
        assert_eq!(checkers(&h), vec!["monotonic_reads"]);
        let result = crate::check::check_history(&h);
        if enforced() {
            assert_eq!(result.unwrap_err().checker, "monotonic_reads");
        } else {
            assert!(result.is_ok(), "{result:?}");
        }
    }
}
