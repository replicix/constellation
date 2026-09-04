//! Pure history checkers for Tier-A conflict workloads.

use crate::history::{Event, EventKind, History};
use crate::op::{Op, Outcome};
use anyhow::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CheckFailure {
    pub checker: String,
    pub message: String,
    pub op_ids: Vec<u64>,
}

impl std::fmt::Display for CheckFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (ops {:?})",
            self.checker, self.message, self.op_ids
        )
    }
}

fn allowed_errno(name: &str) -> bool {
    matches!(
        name,
        "EEXIST"
            | "ENOENT"
            | "EISDIR"
            | "ENOTDIR"
            | "ENOTEMPTY"
            | "ESTALE"
            | "EACCES"
            | "EPERM"
            | "EBUSY"
            | "EINVAL"
    )
}

/// Pair invokes with their completes by op_id.
fn paired(history: &History) -> Vec<(Event, Event)> {
    let mut invokes: HashMap<u64, Event> = HashMap::new();
    let mut out = Vec::new();
    for ev in history.events() {
        match ev.kind {
            EventKind::Invoke => {
                invokes.insert(ev.op_id, ev.clone());
            }
            EventKind::Ok | EventKind::Fail => {
                if let Some(inv) = invokes.remove(&ev.op_id) {
                    out.push((inv, ev.clone()));
                }
            }
            EventKind::Info => {}
        }
    }
    out
}

pub fn check_history(history: &History) -> Result<(), CheckFailure> {
    check_unexpected_errno(history)?;
    check_exactly_one_winner(history)?;
    check_write_full_register(history)?;
    check_torn_writes(history)?;
    check_disjoint_writes(history)?;
    check_chmod_atomicity(history)?;
    check_convergence_reads(history)?;
    Ok(())
}

fn check_unexpected_errno(history: &History) -> Result<(), CheckFailure> {
    for (inv, comp) in paired(history) {
        let Some(c) = &comp.complete else { continue };
        if c.outcome != Outcome::Fail {
            continue;
        }
        let name = c.errno_name.as_deref().unwrap_or("OTHER");
        if !allowed_errno(name) {
            return Err(CheckFailure {
                checker: "unexpected_errno".into(),
                message: format!(
                    "worker {} op {:?} failed with {name}",
                    inv.worker_id,
                    inv.op.as_ref().map(|o| format!("{o:?}"))
                ),
                op_ids: vec![inv.op_id],
            });
        }
    }
    Ok(())
}

fn storm_groups(history: &History, pred: impl Fn(&Op) -> bool) -> HashMap<String, Vec<(Event, Event)>> {
    let mut groups: HashMap<String, Vec<(Event, Event)>> = HashMap::new();
    for (inv, comp) in paired(history) {
        let Some(op) = &inv.op else { continue };
        if !pred(op) {
            continue;
        }
        let key = match op {
            Op::Create { path, .. }
            | Op::Mkdir { path }
            | Op::Unlink { path }
            | Op::Rmdir { path } => path.clone(),
            Op::Rename { from, .. } => from.clone(),
            _ => continue,
        };
        groups.entry(key).or_default().push((inv, comp));
    }
    groups
}

fn check_exactly_one_winner(history: &History) -> Result<(), CheckFailure> {
    // Creates: at most one Ok per path among concurrent creates.
    for (path, pairs) in storm_groups(history, |op| matches!(op, Op::Create { .. })) {
        if pairs.len() < 2 {
            continue;
        }
        let oks: Vec<_> = pairs
            .iter()
            .filter(|(_, c)| c.kind == EventKind::Ok)
            .collect();
        if oks.len() > 1 {
            return Err(CheckFailure {
                checker: "exactly_one_winner".into(),
                message: format!("create {path}: {} Ok completions, expected ≤1", oks.len()),
                op_ids: oks.iter().map(|(i, _)| i.op_id).collect(),
            });
        }
        for (_, c) in &pairs {
            if c.kind == EventKind::Fail {
                let name = c
                    .complete
                    .as_ref()
                    .and_then(|x| x.errno_name.as_deref())
                    .unwrap_or("");
                if name != "EEXIST" && name != "EISDIR" {
                    // Other failures may be ok in races; only flag clearly wrong.
                    if name == "OTHER" {
                        return Err(CheckFailure {
                            checker: "exactly_one_winner".into(),
                            message: format!("create {path}: unexpected fail errno"),
                            op_ids: pairs.iter().map(|(i, _)| i.op_id).collect(),
                        });
                    }
                }
            }
        }
    }

    for (path, pairs) in storm_groups(history, |op| matches!(op, Op::Mkdir { .. })) {
        if pairs.len() < 2 {
            continue;
        }
        let oks = pairs.iter().filter(|(_, c)| c.kind == EventKind::Ok).count();
        if oks > 1 {
            return Err(CheckFailure {
                checker: "exactly_one_winner".into(),
                message: format!("mkdir {path}: {oks} Ok, expected ≤1"),
                op_ids: pairs.iter().map(|(i, _)| i.op_id).collect(),
            });
        }
    }

    for (path, pairs) in storm_groups(history, |op| matches!(op, Op::Unlink { .. })) {
        if pairs.len() < 2 {
            continue;
        }
        let oks = pairs.iter().filter(|(_, c)| c.kind == EventKind::Ok).count();
        if oks > 1 {
            return Err(CheckFailure {
                checker: "exactly_one_winner".into(),
                message: format!("unlink {path}: {oks} Ok, expected ≤1"),
                op_ids: pairs.iter().map(|(i, _)| i.op_id).collect(),
            });
        }
    }

    for (path, pairs) in storm_groups(history, |op| matches!(op, Op::Rmdir { .. })) {
        if pairs.len() < 2 {
            continue;
        }
        let oks = pairs.iter().filter(|(_, c)| c.kind == EventKind::Ok).count();
        if oks > 1 {
            return Err(CheckFailure {
                checker: "exactly_one_winner".into(),
                message: format!("rmdir {path}: {oks} Ok, expected ≤1"),
                op_ids: pairs.iter().map(|(i, _)| i.op_id).collect(),
            });
        }
    }

    for (src, pairs) in storm_groups(history, |op| matches!(op, Op::Rename { .. })) {
        if pairs.len() < 2 {
            continue;
        }
        let oks = pairs.iter().filter(|(_, c)| c.kind == EventKind::Ok).count();
        if oks > 1 {
            return Err(CheckFailure {
                checker: "exactly_one_winner".into(),
                message: format!("rename {src}: {oks} Ok, expected ≤1"),
                op_ids: pairs.iter().map(|(i, _)| i.op_id).collect(),
            });
        }
    }
    Ok(())
}

fn check_write_full_register(history: &History) -> Result<(), CheckFailure> {
    // Group WriteFull by path; collect successful write hashes; subsequent Reads must match one.
    // Skip paths that also saw Append (those use a different verify rule).
    let mut writes: HashMap<String, Vec<(u64, String, u64, u64)>> = HashMap::new();
    let mut appended: HashMap<String, bool> = HashMap::new();
    let mut reads: Vec<(String, String, u64, u64, u64)> = Vec::new();

    for (inv, comp) in paired(history) {
        let Some(op) = &inv.op else { continue };
        let Some(c) = &comp.complete else { continue };
        match op {
            Op::WriteFull { path, content } if c.outcome == Outcome::Ok => {
                let hash = c
                    .value_hash
                    .clone()
                    .unwrap_or_else(|| blake3::hash(content).to_hex().to_string());
                writes.entry(path.clone()).or_default().push((
                    inv.op_id,
                    hash,
                    inv.time_ns,
                    comp.time_ns,
                ));
            }
            Op::Append { path, .. } if c.outcome == Outcome::Ok => {
                appended.insert(path.clone(), true);
            }
            Op::Read { path } if c.outcome == Outcome::Ok => {
                if let Some(hash) = &c.value_hash {
                    reads.push((
                        path.clone(),
                        hash.clone(),
                        inv.time_ns,
                        comp.time_ns,
                        inv.op_id,
                    ));
                }
            }
            _ => {}
        }
    }

    for (path, hash, _r_inv, _r_comp, op_id) in reads {
        if appended.get(&path).copied().unwrap_or(false) {
            continue;
        }
        let Some(ws) = writes.get(&path) else {
            continue;
        };
        if ws.is_empty() {
            continue;
        }
        let any_write = ws.iter().any(|(_, wh, _, _)| wh == &hash);
        if !any_write {
            return Err(CheckFailure {
                checker: "register_linearizability".into(),
                message: format!(
                    "read of {path} got hash {hash} not in successful WriteFull set"
                ),
                op_ids: vec![op_id],
            });
        }
    }
    Ok(())
}

fn check_torn_writes(history: &History) -> Result<(), CheckFailure> {
    // For WriteAt at same path+offset, collect patches; ReadAt must match one full patch.
    let mut patches: HashMap<(String, u64), Vec<(u64, Vec<u8>)>> = HashMap::new();
    let mut readats: Vec<(String, u64, Vec<u8>, u64)> = Vec::new();

    for (inv, comp) in paired(history) {
        let Some(op) = &inv.op else { continue };
        let Some(c) = &comp.complete else { continue };
        match op {
            Op::WriteAt {
                path,
                offset,
                patch,
            } if c.outcome == Outcome::Ok => {
                patches
                    .entry((path.clone(), *offset))
                    .or_default()
                    .push((inv.op_id, patch.clone()));
            }
            Op::ReadAt {
                path,
                offset,
                len: _,
            } if c.outcome == Outcome::Ok => {
                if let Some(bytes) = &c.bytes {
                    readats.push((path.clone(), *offset, bytes.clone(), inv.op_id));
                }
            }
            _ => {}
        }
    }

    for (path, offset, bytes, op_id) in readats {
        let Some(ps) = patches.get(&(path.clone(), offset)) else {
            continue;
        };
        if ps.len() < 2 {
            continue;
        }
        let matches = ps.iter().any(|(_, p)| p == &bytes);
        if !matches {
            return Err(CheckFailure {
                checker: "torn_write".into(),
                message: format!(
                    "ReadAt {path}@{offset} does not equal any concurrent WriteAt patch (torn?)"
                ),
                op_ids: std::iter::once(op_id)
                    .chain(ps.iter().map(|(id, _)| *id))
                    .collect(),
            });
        }
    }
    Ok(())
}

fn check_disjoint_writes(history: &History) -> Result<(), CheckFailure> {
    // After a disjoint WriteAt round, each ReadAt for that offset should match the patch.
    // We approximate: for each successful WriteAt, if there is a later successful ReadAt
    // at same path+offset+len with matching length, bytes should equal patch when no
    // overlapping write to that span succeeded.
    let pairs = paired(history);
    for (inv, comp) in &pairs {
        let Some(Op::WriteAt {
            path,
            offset,
            patch,
        }) = &inv.op
        else {
            continue;
        };
        if comp.kind != EventKind::Ok {
            continue;
        }
        // Find ReadAt same path/offset after this write completed with no other WriteAt to span.
        for (rinv, rcomp) in &pairs {
            let Some(Op::ReadAt {
                path: rp,
                offset: ro,
                len,
            }) = &rinv.op
            else {
                continue;
            };
            if rp != path || *ro != *offset || *len != patch.len() as u64 {
                continue;
            }
            if rinv.time_ns < comp.time_ns {
                continue;
            }
            if rcomp.kind != EventKind::Ok {
                continue;
            }
            let Some(bytes) = rcomp.complete.as_ref().and_then(|c| c.bytes.as_ref()) else {
                continue;
            };
            // Conflicting overlap?
            let overlap = pairs.iter().any(|(wi, wc)| {
                if wi.op_id == inv.op_id || wc.kind != EventKind::Ok {
                    return false;
                }
                matches!(&wi.op, Some(Op::WriteAt { path: p, offset: o, patch: pat })
                    if p == path && ranges_overlap(*offset, patch.len() as u64, *o, pat.len() as u64))
            });
            if overlap {
                continue;
            }
            if bytes != patch {
                return Err(CheckFailure {
                    checker: "disjoint_write".into(),
                    message: format!("disjoint WriteAt {path}@{offset} not visible in later ReadAt"),
                    op_ids: vec![inv.op_id, rinv.op_id],
                });
            }
        }
    }
    Ok(())
}

fn ranges_overlap(a0: u64, alen: u64, b0: u64, blen: u64) -> bool {
    let a1 = a0 + alen;
    let b1 = b0 + blen;
    a0 < b1 && b0 < a1
}

fn check_chmod_atomicity(history: &History) -> Result<(), CheckFailure> {
    let mut chmods: HashMap<String, Vec<u32>> = HashMap::new();
    let mut stats: Vec<(String, Option<u32>, u64)> = Vec::new();

    for (inv, comp) in paired(history) {
        let Some(op) = &inv.op else { continue };
        let Some(c) = &comp.complete else { continue };
        match op {
            Op::Chmod { path, mode } if c.outcome == Outcome::Ok => {
                chmods.entry(path.clone()).or_default().push(*mode);
            }
            Op::Stat { path } if c.outcome == Outcome::Ok => {
                stats.push((path.clone(), c.mode, inv.op_id));
            }
            _ => {}
        }
    }

    for (path, mode, op_id) in stats {
        let Some(modes) = chmods.get(&path) else {
            continue;
        };
        if modes.len() < 2 {
            continue;
        }
        let Some(m) = mode else { continue };
        // Compare permission bits only (ignore file type bits).
        let m_perm = m & 0o7777;
        if !modes.iter().any(|x| (*x & 0o7777) == m_perm) {
            return Err(CheckFailure {
                checker: "attr_atomicity".into(),
                message: format!("stat mode {m:#o} on {path} not in concurrent chmod set {modes:?}"),
                op_ids: vec![op_id],
            });
        }
    }
    Ok(())
}

fn check_convergence_reads(history: &History) -> Result<(), CheckFailure> {
    // Info events mark quiesce groups: "quiesce:<tag>" followed by reads.
    // Simpler approach: consecutive successful Reads of same path with same
    // op tag window — group by path among reads that share close time after Info.
    let mut pending_tag: Option<String> = None;
    let mut group: HashMap<String, Vec<(usize, Option<String>, u64)>> = HashMap::new();
    // path -> (worker, hash, op_id)

    let flush =
        |tag: &str, group: &mut HashMap<String, Vec<(usize, Option<String>, u64)>>| -> Result<(), CheckFailure> {
            for (path, entries) in group.drain() {
                if entries.len() < 2 {
                    continue;
                }
                let hashes: Vec<_> = entries.iter().map(|(_, h, _)| h.clone()).collect();
                let first = &hashes[0];
                if hashes.iter().any(|h| h != first) {
                    return Err(CheckFailure {
                        checker: "convergence".into(),
                        message: format!(
                            "after {tag}, workers disagree on {path}: {hashes:?}"
                        ),
                        op_ids: entries.iter().map(|(_, _, id)| *id).collect(),
                    });
                }
            }
            Ok(())
        };

    for ev in history.events() {
        if ev.kind == EventKind::Info {
            if let Some(info) = &ev.info {
                if let Some(rest) = info.strip_prefix("quiesce_begin:") {
                    if let Some(prev) = pending_tag.take() {
                        flush(&prev, &mut group)?;
                    }
                    pending_tag = Some(rest.to_string());
                    group.clear();
                } else if let Some(rest) = info.strip_prefix("quiesce_end:") {
                    if pending_tag.as_deref() == Some(rest) {
                        flush(rest, &mut group)?;
                        pending_tag = None;
                    }
                }
            }
            continue;
        }
        if pending_tag.is_none() {
            continue;
        }
        if ev.kind != EventKind::Ok {
            continue;
        }
        // Look up invoke for this op_id to get the Read path.
    }

    // Second pass with pairs for quiesce windows.
    let mut windows: Vec<(String, Vec<(Event, Event)>)> = Vec::new();
    let mut cur: Option<(String, Vec<(Event, Event)>)> = None;
    let mut inv_map: HashMap<u64, Event> = HashMap::new();

    for ev in history.events() {
        match ev.kind {
            EventKind::Info => {
                if let Some(info) = &ev.info {
                    if let Some(rest) = info.strip_prefix("quiesce_begin:") {
                        if let Some(w) = cur.take() {
                            windows.push(w);
                        }
                        cur = Some((rest.to_string(), Vec::new()));
                    } else if let Some(rest) = info.strip_prefix("quiesce_end:") {
                        if let Some((tag, ops)) = cur.take() {
                            if tag == rest {
                                windows.push((tag, ops));
                            }
                        }
                    }
                }
            }
            EventKind::Invoke => {
                inv_map.insert(ev.op_id, ev.clone());
            }
            EventKind::Ok | EventKind::Fail => {
                if let (Some((_, ref mut ops)), Some(inv)) = (&mut cur, inv_map.remove(&ev.op_id))
                {
                    ops.push((inv, ev.clone()));
                }
            }
        }
    }

    for (tag, ops) in windows {
        let mut by_path: HashMap<String, Vec<(usize, Option<String>, u64)>> = HashMap::new();
        for (inv, comp) in ops {
            let Some(op) = &inv.op else { continue };
            if comp.kind != EventKind::Ok {
                // For unlink storms, ENOENT on Stat is expected convergence.
                if matches!(op, Op::Stat { .. }) && comp.kind == EventKind::Fail {
                    let name = comp
                        .complete
                        .as_ref()
                        .and_then(|c| c.errno_name.as_deref())
                        .unwrap_or("");
                    if let Op::Stat { path } = op {
                        by_path.entry(path.clone()).or_default().push((
                            inv.worker_id,
                            Some(format!("FAIL:{name}")),
                            inv.op_id,
                        ));
                    }
                }
                continue;
            }
            match op {
                Op::Read { path } | Op::Stat { path } => {
                    let hash = comp.complete.as_ref().and_then(|c| {
                        c.value_hash
                            .clone()
                            .or_else(|| c.mode.map(|m| format!("mode:{m}")))
                            .or_else(|| c.size.map(|s| format!("size:{s}")))
                    });
                    by_path
                        .entry(path.clone())
                        .or_default()
                        .push((inv.worker_id, hash, inv.op_id));
                }
                Op::ReadAt { path, offset, len } => {
                    let hash = comp.complete.as_ref().and_then(|c| c.value_hash.clone());
                    by_path
                        .entry(format!("read_at:{path}@{offset}+{len}"))
                        .or_default()
                        .push((inv.worker_id, hash, inv.op_id));
                }
                _ => {}
            }
        }
        for (path, entries) in by_path {
            if entries.len() < 2 {
                continue;
            }
            let first = &entries[0].1;
            if entries.iter().any(|(_, h, _)| h != first) {
                return Err(CheckFailure {
                    checker: "convergence".into(),
                    message: format!("after {tag}, workers disagree on {path}: {entries:?}"),
                    op_ids: entries.iter().map(|(_, _, id)| *id).collect(),
                });
            }
        }
    }
    Ok(())
}

/// Convenience for offline CLI.
pub fn check_file(path: &std::path::Path) -> Result<()> {
    let hist = History::load_jsonl(path)?;
    check_history(&hist).map_err(|e| anyhow::anyhow!("{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::Complete;

    #[test]
    fn create_double_ok_fails() {
        let mut h = History::new();
        let op = Op::Create {
            path: "a".into(),
            content: b"x".to_vec(),
        };
        h.record_invoke(0, 1, op.clone());
        h.record_complete(0, 1, Complete {
            outcome: Outcome::Ok,
            errno: None,
            errno_name: None,
            value_hash: None,
            bytes: None,
            size: None,
            mode: None,
            wall_ns: 1,
        });
        h.record_invoke(1, 2, op);
        h.record_complete(1, 2, Complete {
            outcome: Outcome::Ok,
            errno: None,
            errno_name: None,
            value_hash: None,
            bytes: None,
            size: None,
            mode: None,
            wall_ns: 1,
        });
        assert!(check_exactly_one_winner(&h).is_err());
    }
}
