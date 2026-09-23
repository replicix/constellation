//! Exactly-once checkers (plan 30 §M4 item 5): no operation takes effect
//! twice, and none reports failure after taking effect.
//!
//! Plan 30 §M2 gives every mutation a request id (rid) that its records
//! and a `Completed { rid }` record carry into the log together, and the
//! holder answers a retried rid from `recent`/`completed` instead of
//! executing it again. Two views check that from both ends:
//!
//! - **The log** ([`check_log_completions`]): every rid completes at most
//!   once in the shared log. A second `Completed { rid }` means the op's
//!   records were executed and shipped twice — bug A's shape, whatever
//!   path produced it (a timed-out forward re-executed on the lease path,
//!   a replay by rid after a deposition, a system op reusing a rid). The
//!   harness decodes the bucket's segments into [`LoggedCompletion`]s.
//! - **The client history** ([`check_history_exactly_once`]): what an
//!   application sees. Every op in the generated workloads carries unique
//!   content (or operates on a fresh name), so its effect is recognizable
//!   in the state the quiesce reads record:
//!   - a `create` that failed with `EEXIST` whose own content is what the
//!     file holds afterwards took effect and then reported failure (the
//!     exact symptom of bug A: executed by the holder, retried, refused
//!     by its own effect);
//!   - a storm over one fresh name (`mkdir`, `unlink`, `rename` of a
//!     seeded file) in which the effect is visible afterwards but *no*
//!     attempt reported success: one of them took effect and failed;
//!   - an `append` of unique bytes that the file contains more than once
//!     took effect twice; one that failed but whose bytes are there took
//!     effect and reported failure.

use crate::check::CheckFailure;
use crate::history::{EventKind, History};
use crate::op::{hash_bytes, Complete, Op, Outcome};
use std::collections::{BTreeMap, HashMap};

/// One `Completed { rid }` record found in the shared log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoggedCompletion {
    /// The segment's sequence number and the record's index in it.
    pub segment: u64,
    pub index: usize,
    /// The shipping node and its lease epoch.
    pub node: u64,
    pub epoch: u64,
    /// `(node, incarnation, seq)`.
    pub rid: (u64, u32, u64),
}

/// Every rid completes at most once in the log.
pub fn check_log_completions(entries: &[LoggedCompletion]) -> Result<(), CheckFailure> {
    let mut seen: HashMap<(u64, u32, u64), &LoggedCompletion> = HashMap::new();
    for e in entries {
        if let Some(first) = seen.insert(e.rid, e) {
            return Err(CheckFailure {
                checker: "exactly_once_log".into(),
                message: format!(
                    "rid {:?} completed twice in the log: segment {} #{} (node {}, epoch {}) \
                     and segment {} #{} (node {}, epoch {}) — its op took effect twice",
                    e.rid,
                    first.segment,
                    first.index,
                    first.node,
                    first.epoch,
                    e.segment,
                    e.index,
                    e.node,
                    e.epoch
                ),
                op_ids: vec![],
            });
        }
    }
    Ok(())
}

struct Done {
    op_id: u64,
    op: Op,
    complete: Complete,
    index: u64,
}

fn completed(history: &History) -> Vec<Done> {
    let mut invokes: HashMap<u64, Op> = HashMap::new();
    let mut out = Vec::new();
    for ev in history.events() {
        match ev.kind {
            EventKind::Invoke => {
                if let Some(op) = &ev.op {
                    invokes.insert(ev.op_id, op.clone());
                }
            }
            EventKind::Ok | EventKind::Fail => {
                if let (Some(op), Some(complete)) = (invokes.remove(&ev.op_id), ev.complete.clone())
                {
                    out.push(Done {
                        op_id: ev.op_id,
                        op,
                        complete,
                        index: ev.index,
                    });
                }
            }
            EventKind::Info => {}
        }
    }
    out
}

fn ok(d: &Done) -> bool {
    d.complete.outcome == Outcome::Ok
}

fn failed_with(d: &Done, name: &str) -> bool {
    d.complete.outcome == Outcome::Fail && d.complete.errno_name.as_deref() == Some(name)
}

/// The content an op wrote, if it writes whole content.
fn written(op: &Op) -> Option<(&str, &[u8])> {
    match op {
        Op::Create { path, content } | Op::WriteFull { path, content } => {
            Some((path.as_str(), content.as_slice()))
        }
        _ => None,
    }
}

/// See the module doc.
pub fn check_history_exactly_once(history: &History) -> Result<(), CheckFailure> {
    let ops = completed(history);

    // How many ops in the whole history wrote each content: only unique
    // content identifies one op's effect.
    let mut writers: HashMap<String, usize> = HashMap::new();
    for d in &ops {
        if let Some((_, content)) = written(&d.op) {
            *writers.entry(hash_bytes(content)).or_default() += 1;
        }
    }

    // Last successful read of each path: `(history index, content hash,
    // bytes)`, and every successful stat / ENOENT by path, in order.
    type ReadsByPath<'a> = HashMap<&'a str, Vec<(u64, String, Option<&'a [u8]>)>>;
    let mut reads: ReadsByPath = HashMap::new();
    let mut presence: HashMap<&str, Vec<(u64, bool)>> = HashMap::new();
    for d in &ops {
        match &d.op {
            Op::Read { path } if ok(d) => {
                if let Some(h) = &d.complete.value_hash {
                    reads.entry(path.as_str()).or_default().push((
                        d.index,
                        h.clone(),
                        d.complete.bytes.as_deref(),
                    ));
                }
                presence
                    .entry(path.as_str())
                    .or_default()
                    .push((d.index, true));
            }
            Op::Stat { path } if ok(d) => {
                presence
                    .entry(path.as_str())
                    .or_default()
                    .push((d.index, true));
            }
            Op::Read { path } | Op::Stat { path } if failed_with(d, "ENOENT") => {
                presence
                    .entry(path.as_str())
                    .or_default()
                    .push((d.index, false));
            }
            _ => {}
        }
    }
    let present_after = |path: &str, index: u64| -> Option<bool> {
        presence
            .get(path)?
            .iter()
            .rev()
            .find(|(i, _)| *i > index)
            .map(|(_, p)| *p)
    };

    // 1. A create refused with EEXIST whose unique content is in the file.
    for d in &ops {
        let Op::Create { path, content } = &d.op else {
            continue;
        };
        if !failed_with(d, "EEXIST") {
            continue;
        }
        let mine = hash_bytes(content);
        if writers.get(&mine) != Some(&1) {
            continue;
        }
        let later = reads
            .get(path.as_str())
            .and_then(|r| r.iter().rev().find(|(i, _, _)| *i > d.index));
        if let Some((_, seen, _)) = later {
            if *seen == mine {
                return Err(CheckFailure {
                    checker: "exactly_once".into(),
                    message: format!(
                        "create {path} reported EEXIST, yet the file holds that create's own \
                         content: it took effect and then reported failure"
                    ),
                    op_ids: vec![d.op_id],
                });
            }
        }
    }

    // 2. Storms over one name where the effect happened but nobody
    //    succeeded.
    type Group<'a> = BTreeMap<&'a str, Vec<&'a Done>>;
    let mut mkdirs: Group = BTreeMap::new();
    let mut unlinks: Group = BTreeMap::new();
    let mut renames: Group = BTreeMap::new();
    let mut placed: HashMap<&str, usize> = HashMap::new(); // ok placements per path
    for d in &ops {
        match &d.op {
            Op::Mkdir { path } => mkdirs.entry(path.as_str()).or_default().push(d),
            Op::Unlink { path } => unlinks.entry(path.as_str()).or_default().push(d),
            Op::Rename { from, to } => {
                renames.entry(from.as_str()).or_default().push(d);
                if ok(d) {
                    *placed.entry(to.as_str()).or_default() += 1;
                }
            }
            Op::Create { path, .. } | Op::Link { to: path, .. } if ok(d) => {
                *placed.entry(path.as_str()).or_default() += 1;
            }
            _ => {}
        }
    }
    for (path, group) in &mkdirs {
        if group.iter().any(|d| ok(d)) || placed.contains_key(path) {
            continue;
        }
        let first = group.iter().map(|d| d.index).min().unwrap_or(0);
        let last = group.iter().map(|d| d.index).max().unwrap_or(0);
        let existed_before = presence
            .get(path)
            .is_some_and(|p| p.iter().any(|(i, present)| *i < first && *present));
        if !existed_before
            && group.iter().all(|d| failed_with(d, "EEXIST"))
            && present_after(path, last) == Some(true)
        {
            return Err(CheckFailure {
                checker: "exactly_once".into(),
                message: format!(
                    "every mkdir {path} reported EEXIST, nothing else created it, and it exists \
                     afterwards: one of them took effect and reported failure"
                ),
                op_ids: group.iter().map(|d| d.op_id).collect(),
            });
        }
    }
    for (path, group) in &unlinks {
        if group.iter().any(|d| ok(d)) || group.len() < 2 {
            continue;
        }
        let moved_away = renames.get(path).is_some_and(|g| g.iter().any(|d| ok(d)));
        let last = group.iter().map(|d| d.index).max().unwrap_or(0);
        let first = group.iter().map(|d| d.index).min().unwrap_or(0);
        let existed_before = presence
            .get(path)
            .is_some_and(|p| p.iter().any(|(i, present)| *i < first && *present))
            || placed.contains_key(path);
        if !moved_away
            && existed_before
            && group.iter().all(|d| failed_with(d, "ENOENT"))
            && present_after(path, last) == Some(false)
        {
            return Err(CheckFailure {
                checker: "exactly_once".into(),
                message: format!(
                    "every unlink {path} reported ENOENT, the file existed and nothing else \
                     removed it, and it is gone: one of them took effect and reported failure"
                ),
                op_ids: group.iter().map(|d| d.op_id).collect(),
            });
        }
    }
    for (from, group) in &renames {
        let last = group.iter().map(|d| d.index).max().unwrap_or(0);
        for d in group {
            let Op::Rename { to, .. } = &d.op else {
                continue;
            };
            if ok(d) {
                continue;
            }
            // Only a destination nothing else could have produced.
            if placed.contains_key(to.as_str()) {
                continue;
            }
            if present_after(to, last) == Some(true) {
                return Err(CheckFailure {
                    checker: "exactly_once".into(),
                    message: format!(
                        "rename {from} -> {to} reported failure, yet {to} exists afterwards \
                         and no other op put anything there: it took effect and reported failure"
                    ),
                    op_ids: vec![d.op_id],
                });
            }
        }
    }

    // 3. Appends of unique bytes: never twice, never after a failure.
    let mut appends: BTreeMap<&str, Vec<&Done>> = BTreeMap::new();
    for d in &ops {
        if let Op::Append { path, .. } = &d.op {
            appends.entry(path.as_str()).or_default().push(d);
        }
    }
    for (path, group) in &appends {
        let last = group.iter().map(|d| d.index).max().unwrap_or(0);
        let Some((_, _, Some(bytes))) = reads
            .get(path)
            .and_then(|r| r.iter().rev().find(|(i, _, _)| *i > last))
        else {
            continue;
        };
        for d in group {
            let Op::Append { data, .. } = &d.op else {
                continue;
            };
            if data.len() < 16 {
                continue;
            }
            let count = bytes
                .windows(data.len())
                .filter(|w| *w == data.as_slice())
                .count();
            if count > 1 {
                return Err(CheckFailure {
                    checker: "exactly_once".into(),
                    message: format!("append to {path} took effect {count} times"),
                    op_ids: vec![d.op_id],
                });
            }
            if count == 1 && d.complete.outcome == Outcome::Fail {
                return Err(CheckFailure {
                    checker: "exactly_once".into(),
                    message: format!(
                        "append to {path} reported {} yet its bytes are in the file: it took \
                         effect and reported failure",
                        d.complete.errno_name.as_deref().unwrap_or("failure")
                    ),
                    op_ids: vec![d.op_id],
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

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

    fn history(ops: Vec<(Op, Complete)>) -> History {
        let mut h = History::new();
        for (i, (op, c)) in ops.into_iter().enumerate() {
            h.record_invoke(0, i as u64 + 1, op);
            h.record_complete(0, i as u64 + 1, c);
        }
        h
    }

    fn rid(seq: u64) -> (u64, u32, u64) {
        (3, 1, seq)
    }

    #[test]
    fn a_rid_completed_twice_in_the_log_is_caught() {
        let entry = |segment, seq| LoggedCompletion {
            segment,
            index: 0,
            node: 3,
            epoch: 1,
            rid: rid(seq),
        };
        check_log_completions(&[entry(1, 1), entry(1, 2), entry(2, 3)]).unwrap();
        let err = check_log_completions(&[entry(1, 1), entry(2, 2), entry(5, 1)]).unwrap_err();
        assert_eq!(err.checker, "exactly_once_log");
        assert!(err.message.contains("segment 1") && err.message.contains("segment 5"));
    }

    /// Bug A's symptom: the holder executed the create, the requester's
    /// retry was refused by it, and the application saw EEXIST although
    /// the file holds its content.
    #[test]
    fn a_create_that_failed_after_taking_effect_is_caught() {
        let mine = b"unique content of the losing create".to_vec();
        let h = history(vec![
            (
                Op::Create {
                    path: "d/f".into(),
                    content: mine.clone(),
                },
                fail_c("EEXIST"),
            ),
            (Op::Read { path: "d/f".into() }, read_c(&mine)),
        ]);
        let err = check_history_exactly_once(&h).unwrap_err();
        assert!(err.message.contains("took effect"), "{err}");

        // The same shape with a genuine winner is fine.
        let theirs = b"the winner's content".to_vec();
        let h = history(vec![
            (
                Op::Create {
                    path: "d/f".into(),
                    content: theirs.clone(),
                },
                ok_c(),
            ),
            (
                Op::Create {
                    path: "d/f".into(),
                    content: mine,
                },
                fail_c("EEXIST"),
            ),
            (Op::Read { path: "d/f".into() }, read_c(&theirs)),
        ]);
        check_history_exactly_once(&h).unwrap();
    }

    #[test]
    fn a_storm_with_an_effect_and_no_success_is_caught() {
        let h = history(vec![
            (Op::Mkdir { path: "d".into() }, fail_c("EEXIST")),
            (Op::Mkdir { path: "d".into() }, fail_c("EEXIST")),
            (Op::Stat { path: "d".into() }, ok_c()),
        ]);
        assert!(check_history_exactly_once(&h).is_err());

        let h = history(vec![
            (
                Op::Create {
                    path: "u".into(),
                    content: b"seed-content-long-enough".to_vec(),
                },
                ok_c(),
            ),
            (Op::Unlink { path: "u".into() }, fail_c("ENOENT")),
            (Op::Unlink { path: "u".into() }, fail_c("ENOENT")),
            (Op::Stat { path: "u".into() }, fail_c("ENOENT")),
        ]);
        assert!(check_history_exactly_once(&h).is_err());

        let h = history(vec![
            (
                Op::Rename {
                    from: "s".into(),
                    to: "t1".into(),
                },
                fail_c("ENOENT"),
            ),
            (Op::Stat { path: "t1".into() }, ok_c()),
        ]);
        assert!(check_history_exactly_once(&h).is_err());

        // One success: the ordinary storm outcome.
        let h = history(vec![
            (Op::Mkdir { path: "d".into() }, ok_c()),
            (Op::Mkdir { path: "d".into() }, fail_c("EEXIST")),
            (Op::Stat { path: "d".into() }, ok_c()),
        ]);
        check_history_exactly_once(&h).unwrap();
    }

    #[test]
    fn an_append_applied_twice_or_after_failing_is_caught() {
        let data = b"0123456789abcdef-unique-append".to_vec();
        let mut twice = data.clone();
        twice.extend_from_slice(&data);
        let h = history(vec![
            (
                Op::Append {
                    path: "a".into(),
                    data: data.clone(),
                },
                ok_c(),
            ),
            (Op::Read { path: "a".into() }, read_c(&twice)),
        ]);
        let err = check_history_exactly_once(&h).unwrap_err();
        assert!(err.message.contains("2 times"), "{err}");

        let h = history(vec![
            (
                Op::Append {
                    path: "a".into(),
                    data: data.clone(),
                },
                fail_c("EIO"),
            ),
            (Op::Read { path: "a".into() }, read_c(&data)),
        ]);
        assert!(check_history_exactly_once(&h).is_err());

        let h = history(vec![
            (
                Op::Append {
                    path: "a".into(),
                    data: data.clone(),
                },
                ok_c(),
            ),
            (Op::Read { path: "a".into() }, read_c(&data)),
        ]);
        check_history_exactly_once(&h).unwrap();
    }
}
