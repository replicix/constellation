//! Plan 30 §M6: per-node read-your-writes and monotonic reads over a
//! simulated run's history, with the log as the witness of the version
//! order.
//!
//! The successes in log order (`Completed { rid }` positions) fix every
//! name's version sequence: after `k` successes, name `x` is present or
//! not. A session is one node incarnation (a restart is a new FUSE
//! session). Each observation the session makes constrains, per name,
//! how far along that sequence a later read of the name must be:
//! - its own write that returned `Ok` (not tentative) at log index `p`:
//!   at least `p + 1` (read-your-writes);
//! - a refusal (`EEXIST` — present, `ENOENT` — absent; its op never
//!   landed): at least the state its own `Refused` row follows, the number
//!   of successes logged before that row (the sequencer evaluated it
//!   there, and its reply carries that position). That holds in every
//!   scope and needs no real-time argument, however late the row was
//!   appended, when the row is where the refusal was evaluated: the
//!   refused name shows what the refusal says at that state. A refusal
//!   with no such row (none in an applied segment, another code than the
//!   client's return, or a state that does not show the refusal: an inbox
//!   or local answer is evaluated against the requester's replica and
//!   journaled later) falls back to a window: at least the earliest
//!   state, after the successes of its scope and its own node's that
//!   returned before it was invoked, that refuses the same way (`Scope`);
//! - a read: at least the earliest state at or after its own floor that
//!   shows what it returned (monotonic reads).
//!
//! The fallback window is lenient: a success of another scope that
//! really precedes the refusal in the sequencer's order does not bind it,
//! so a stale read can pass there (the row path has no such gap).
//!
//! A read invoked after an observation returned must be explained by a
//! state at or after that floor; if no such state shows what it returned,
//! that is a violation — read-your-writes when the binding floor came from
//! the session's own write, monotonic reads otherwise. Violations on a
//! name some tentative op (acked, then rolled back — plan 30 M13's rule)
//! touched are counted, not failed: nothing may be required to see such
//! an effect, or to keep seeing it. A read whose session wait timed out
//! answered degraded by design; its violations are counted separately.

use super::history::{
    dir_of, range_idx_of, witness_order, HistEvt, JournalOrder, NsOp, NsRet, ReadEvt,
};
use constellation_meta::Rid;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Default, Clone)]
pub struct SessionReport {
    pub reads: usize,
    /// Reads whose wait timed out (answered degraded).
    pub degraded: usize,
    /// Violations explained by a tentative op on the name.
    pub tentative: usize,
    /// Violations of reads that timed out.
    pub degraded_violations: usize,
    /// Refusals placed at their own `Refused` row, and refusals with no
    /// row in an applied segment, placed by the scoped window.
    pub refusals_placed: usize,
    pub refusals_windowed: usize,
    /// Of the windowed, those whose row's state does not show what the
    /// refusal says (the row is not where it was evaluated).
    pub refusals_row_disagrees: usize,
    /// Real violations: (checker, description).
    pub violations: Vec<(String, String)>,
}

fn names_of(op: &NsOp) -> Vec<&str> {
    match op {
        NsOp::Create(n) | NsOp::Unlink(n) | NsOp::Put(n) => vec![n.as_str()],
        NsOp::Rename(a, b) | NsOp::Exchange(a, b) | NsOp::Same(a, b) => {
            vec![a.as_str(), b.as_str()]
        }
        // The source name is only how the client found the inode.
        NsOp::Link(_, b) => vec![b.as_str()],
    }
}

/// Which successes bind a refusal that has no `Refused` row in the log
/// (the fallback window): the whole namespace with one sequencer; under
/// delegation, keys of different owners are ordered independently, so only
/// successes on a name of the same directory (of the same hash range, when
/// ranges are checked). That is the projection the linearizability check
/// uses, which loses nothing there because linearizability is local; for a
/// refusal's window it is only a lower bound on what the sequencer
/// ordered, since one sequencer can own several directories (the root's
/// own, a delegate's subtree, the ranges of an unsplit directory). A
/// node's own successes order before its later ops in any directory
/// (`check_session_order`), whatever the scope.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Scope {
    Global,
    Dir,
    Range(u8),
}

impl Scope {
    fn shares(self, a: &str, b: &str) -> bool {
        match self {
            Scope::Global => true,
            Scope::Dir => dir_of(a) == dir_of(b),
            Scope::Range(bits) => {
                dir_of(a) == dir_of(b) && range_idx_of(bits, a) == range_idx_of(bits, b)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Own,
    Observed,
    /// Set by observing another node's write: everything that node had
    /// been acknowledged before it issued that write (EC2 campaign 7,
    /// finding B-1: git's objects before the ref that names them).
    Causal,
}

#[allow(clippy::too_many_arguments)]
pub fn check_sessions(
    events: &[HistEvt],
    ticks: &[u64],
    reads: &[ReadEvt],
    tentative: &HashSet<Rid>,
    completed_at: &HashMap<Rid, (u64, usize)>,
    refused_at: &HashMap<Rid, ((u64, usize), NsRet)>,
    journal: &JournalOrder,
    refused_journal: &JournalOrder,
    scope: Scope,
) -> SessionReport {
    let mut report = SessionReport {
        reads: reads.len(),
        degraded: reads.iter().filter(|r| r.timed_out).count(),
        ..Default::default()
    };
    let mut invoke: HashMap<Rid, (u64, NsOp)> = HashMap::new();
    let mut ret: HashMap<Rid, (u64, NsRet)> = HashMap::new();
    for (e, t) in events.iter().zip(ticks) {
        match e {
            HistEvt::Invoke { rid, op, .. } => {
                invoke.insert(*rid, (*t, op.clone()));
            }
            HistEvt::Return { rid, ret: r, .. } => {
                ret.insert(*rid, (*t, *r));
            }
        }
    }
    // The successes in witness order (the log's, a tenure's run in
    // journal order: `witness_order`, as the linearizability and
    // session-order checks take it), and every name's presence after each.
    // A journaled refusal's row is ordered with them, by the same rule
    // (its journal seq inside its tenure's run): `row_k` is the number of
    // successes before it, the state it was evaluated at.
    let mut at: HashMap<Rid, (u64, usize)> = completed_at.clone();
    let mut merged: JournalOrder = journal.clone();
    for (rid, (p, _)) in refused_at {
        if invoke.contains_key(rid) && !completed_at.contains_key(rid) {
            at.insert(*rid, *p);
            if let Some(j) = refused_journal.get(rid) {
                merged.insert(*rid, *j);
            }
        }
    }
    let mut successes: Vec<Rid> = Vec::new();
    let mut row_k: HashMap<Rid, usize> = HashMap::new();
    for rid in witness_order(&at, &merged) {
        if completed_at.contains_key(&rid) {
            if invoke.contains_key(&rid) {
                successes.push(rid);
            }
        } else {
            row_k.insert(rid, successes.len());
        }
    }
    let index_of: HashMap<Rid, usize> = successes
        .iter()
        .enumerate()
        .map(|(i, rid)| (*rid, i))
        .collect();
    let mut names: Vec<String> = reads.iter().map(|r| r.name.clone()).collect();
    for (_, op) in invoke.values() {
        names.extend(names_of(op).into_iter().map(String::from));
    }
    names.sort();
    names.dedup();
    let mut presence: HashMap<String, Vec<bool>> = HashMap::new();
    // Per name and state, the success that last touched it: the op a
    // read of the name in that state observed.
    let mut touched: HashMap<String, Vec<Option<Rid>>> = HashMap::new();
    for n in &names {
        let mut v = vec![false];
        let mut by = vec![None];
        let mut cur = false;
        let mut last = None;
        for rid in &successes {
            let (_, op) = &invoke[rid];
            let hit = match op {
                NsOp::Create(x) | NsOp::Put(x) if x == n => {
                    cur = true;
                    true
                }
                NsOp::Unlink(x) if x == n => {
                    cur = false;
                    true
                }
                NsOp::Rename(a, b) => {
                    let mut hit = false;
                    if a == n {
                        cur = false;
                        hit = true;
                    }
                    if b == n {
                        cur = true;
                        hit = true;
                    }
                    hit
                }
                NsOp::Link(_, x) if x == n => {
                    cur = true;
                    true
                }
                // Both names were present, and each now names the other's
                // inode: a write to both.
                NsOp::Exchange(a, b) | NsOp::Same(a, b) if a == n || b == n => {
                    cur = true;
                    true
                }
                _ => false,
            };
            if hit {
                last = Some(*rid);
            }
            v.push(cur);
            by.push(last);
        }
        presence.insert(n.clone(), v);
        touched.insert(n.clone(), by);
    }
    let first_at_or_after = |name: &str, from: usize, want: bool| -> Option<usize> {
        let v = presence.get(name)?;
        (from..v.len()).find(|k| v[*k] == want)
    };
    // Names a tentative op touched, with the tick it was invoked at.
    let mut tentative_names: HashMap<String, u64> = HashMap::new();
    for rid in tentative {
        if let Some((t, op)) = invoke.get(rid) {
            for n in names_of(op) {
                let e = tentative_names.entry(n.to_string()).or_insert(*t);
                *e = (*e).min(*t);
            }
        }
    }
    let n_states = successes.len();
    // The latest state an observation of `name` that returned at `after`
    // can show: before every success invoked after it returned on a name
    // of its scope. Only those: under delegation another owner's success
    // invoked later can be logged before rows the observation saw (a
    // delegate's acknowledged rows the root appends late), and a cut there
    // drops the observation or its causal floors.
    let hi_after = |name: &str, after: u64| -> usize {
        index_of
            .iter()
            .filter(|(b, _)| {
                let (t, op) = &invoke[*b];
                *t > after && names_of(op).into_iter().any(|m| scope.shares(m, name))
            })
            .map(|(_, ib)| *ib)
            .min()
            .unwrap_or(n_states)
    };
    // Causal floors (EC2 campaign 7, finding B-1). A session that
    // observed `name` as `want` in some state of `lo..=hi` observed the
    // write that last set it there — when that is one op `b` across
    // every such state (else nothing is attributed). `b`'s node had, by
    // the time it issued `b`, been acknowledged every earlier op of its
    // own that returned before `b` was invoked: they precede `b` in the
    // log (a delegate waits for the requester's `deps`; the root parks
    // on the delegate streams they name), and whatever installs `b`
    // ahead of the log (a contiguous streamed prefix, the delegate's own
    // execution after the same wait, a refusal's position the reader
    // waits for) carries them too. So the observer's later reads of
    // their names must reflect them: one floor per name, at the index
    // after the latest such op. The observer's own writes are covered
    // by read-your-writes; tentative ops are exempt.
    let causal_floors = |name: &str,
                         lo: usize,
                         hi: usize,
                         want: bool,
                         observer: (u64, u32)|
     -> Vec<(String, usize, String)> {
        let (Some(p), Some(t)) = (presence.get(name), touched.get(name)) else {
            return Vec::new();
        };
        let hi = hi.min(p.len() - 1);
        let mut setter: Option<Rid> = None;
        for s in lo..=hi {
            if p[s] != want {
                continue;
            }
            match (setter, t[s]) {
                // The initial state: nothing was observed.
                (_, None) => return Vec::new(),
                (None, Some(b)) => setter = Some(b),
                (Some(a), Some(b)) if a != b => return Vec::new(),
                _ => {}
            }
        }
        let Some(b) = setter else {
            return Vec::new();
        };
        if tentative.contains(&b) || (b.node, b.incarnation) == observer {
            return Vec::new();
        }
        let Some((inv_b, op_b)) = invoke.get(&b) else {
            return Vec::new();
        };
        let mut floors: BTreeMap<String, usize> = BTreeMap::new();
        for (a, (ret_a, r_a)) in &ret {
            if *r_a != NsRet::Ok
                || (a.node, a.incarnation) != (b.node, b.incarnation)
                || ret_a >= inv_b
                || tentative.contains(a)
            {
                continue;
            }
            let Some(ia) = index_of.get(a) else {
                continue;
            };
            let (_, op_a) = &invoke[a];
            for n in names_of(op_a) {
                let e = floors.entry(n.to_string()).or_insert(0);
                *e = (*e).max(ia + 1);
            }
        }
        floors
            .into_iter()
            .map(|(n, f)| {
                let by = format!(
                    "causal: {name} seen as {want} was set by {op_b:?} rid {b:?}, and its node had been acknowledged {n} (log index {}) before issuing it",
                    f - 1
                );
                (n, f, by)
            })
            .collect()
    };

    // Per session: (return tick, name, floor, kind, what set it).
    type Session = (u64, u32);
    /// (return tick, name, floor, kind, what set it)
    type Obs = (u64, String, usize, Kind, String);
    let mut obs: BTreeMap<Session, Vec<Obs>> = BTreeMap::new();
    for (rid, (rt, r)) in &ret {
        let Some((it, op)) = invoke.get(rid) else {
            continue;
        };
        if tentative.contains(rid) {
            continue;
        }
        let session = (rid.node, rid.incarnation);
        match (r, index_of.get(rid)) {
            (NsRet::Ok, Some(p)) => {
                for n in names_of(op) {
                    obs.entry(session).or_default().push((
                        *rt,
                        n.to_string(),
                        p + 1,
                        Kind::Own,
                        format!("own {op:?} rid {rid:?}"),
                    ));
                }
            }
            (NsRet::Eexist | NsRet::Enoent, None) => {
                let (name, want) = match (op, r) {
                    (NsOp::Create(n), NsRet::Eexist) | (NsOp::Link(_, n), NsRet::Eexist) => {
                        (n.as_str(), true)
                    }
                    (NsOp::Unlink(n), NsRet::Enoent) | (NsOp::Rename(n, _), NsRet::Enoent) => {
                        (n.as_str(), false)
                    }
                    (NsOp::Rename(_, _), NsRet::Eexist) => continue,
                    _ => continue,
                };
                // The latest state the refusal can have been evaluated at:
                // before every success invoked after it returned.
                let hi = hi_after(name, *rt);
                // The row of this answer: its code is the client's return,
                // and the refused name shows what the refusal says at the
                // state the row follows. A row that fails either is not
                // where the refusal was evaluated (an inbox or local
                // answer is evaluated against the requester's replica and
                // journaled later), and the window decides.
                let row_k = refused_at
                    .get(rid)
                    .filter(|(_, code)| code == r)
                    .and_then(|_| row_k.get(rid).copied());
                let row_k = match row_k {
                    Some(k) if presence.get(name).is_some_and(|v| v[k] != want) => {
                        report.refusals_row_disagrees += 1;
                        None
                    }
                    k => k,
                };
                let placed = match row_k {
                    // A refusal the sequencer journaled: it evaluated it at
                    // the state its `Refused` row follows, the number of
                    // successes logged before that row, and the reply
                    // carries that position (cto-modes "Positions"). No
                    // real-time cut applies, not even `hi`: a delegate's
                    // row the root appended late can follow successes
                    // invoked after the refusal returned.
                    Some(k) => {
                        report.refusals_placed += 1;
                        Some((k, hi.max(k)))
                    }
                    // No usable `Refused` row (answered without
                    // journaling, journaled in a fenced segment, another
                    // code, or a state that does not show the refusal): the
                    // refusal's window, as the witnessed check places it,
                    // after the successes of its scope and its own node's
                    // that returned before it was invoked. Another owner's
                    // success that returned earlier may follow it in the
                    // log (`long-delegated-late-answers` 4125: a
                    // delegate's acknowledged `d2` rows appended after the
                    // root's refusal on `d1`). Lenient where a success of
                    // another scope really precedes it (see the module
                    // doc).
                    None => {
                        report.refusals_windowed += 1;
                        let lo = index_of
                            .iter()
                            .filter(|(b, _)| {
                                !tentative.contains(*b)
                                    && ret.get(*b).is_some_and(|(t, _)| t < it)
                                    && ((b.node, b.incarnation) == session
                                        || names_of(&invoke[*b].1)
                                            .into_iter()
                                            .any(|m| scope.shares(m, name)))
                            })
                            .map(|(_, ib)| ib + 1)
                            .max()
                            .unwrap_or(0);
                        first_at_or_after(name, lo, want)
                            .filter(|k| *k <= hi)
                            .map(|k| (k, hi))
                    }
                };
                if let Some((k, hi)) = placed {
                    let causal = causal_floors(name, k, hi, want, session);
                    let list = obs.entry(session).or_default();
                    list.push((
                        *rt,
                        name.to_string(),
                        k,
                        Kind::Observed,
                        format!("refusal {op:?} -> {r:?} rid {rid:?}"),
                    ));
                    for (n, f, by) in causal {
                        list.push((*rt, n, f, Kind::Causal, by));
                    }
                }
            }
            _ => {}
        }
    }
    // Reads, in return order per session, each checked against the
    // observations that returned before it was invoked, then added as one.
    let mut by_session: BTreeMap<Session, Vec<&ReadEvt>> = BTreeMap::new();
    for r in reads {
        by_session
            .entry((r.node, r.incarnation))
            .or_default()
            .push(r);
    }
    for (session, mut rs) in by_session {
        rs.sort_by_key(|r| r.ret);
        for r in rs {
            let list = obs.entry(session).or_default();
            let binding = list
                .iter()
                .filter(|(t, n, ..)| *t < r.inv && *n == r.name)
                .max_by_key(|(_, _, k, ..)| *k)
                .cloned();
            let floor = binding.as_ref().map(|b| b.2).unwrap_or(0);
            match first_at_or_after(&r.name, floor, r.present) {
                // A degraded read (its session wait timed out: the state
                // it was owed never arrived, plan 30 §M6) is answered from
                // whatever the replica has; it binds no later read — it
                // would otherwise be "explained" by a far-future state and
                // make the next, fresh read look non-monotonic.
                Some(_) if r.timed_out => {}
                Some(k) => {
                    // The latest state the read can have seen: before
                    // every success invoked after it returned.
                    let hi = hi_after(&r.name, r.ret);
                    let causal = causal_floors(&r.name, k, hi, r.present, session);
                    list.push((
                        r.ret,
                        r.name.clone(),
                        k,
                        Kind::Observed,
                        format!("read of {} by t{} -> {}", r.name, r.thread, r.present),
                    ));
                    for (n, f, by) in causal {
                        list.push((r.ret, n, f, Kind::Causal, by));
                    }
                }
                None => {
                    let tentative_name = tentative_names.get(&r.name).is_some_and(|t| *t < r.ret);
                    let Some((_, _, _, kind, by)) = binding else {
                        // No log state ever showed it: the read saw an
                        // effect that never landed.
                        if tentative_name {
                            report.tentative += 1;
                        } else {
                            report.violations.push((
                                "phantom_read".to_string(),
                                format!(
                                    "node {} t{} read {} as {} but no log state shows that",
                                    r.node, r.thread, r.name, r.present
                                ),
                            ));
                        }
                        continue;
                    };
                    let chain: Vec<String> = list
                        .iter()
                        .filter(|(_, n, ..)| *n == r.name)
                        .map(|(t, _, k, kind, by)| format!("[tick {t} index {k} {kind:?}: {by}]"))
                        .collect();
                    let what = format!(
                        "node {} (incarnation {}) t{} read {} as {} at tick {}, but it had \
                         already observed a later state (log index {floor}, set by {by}); \
                         observations of {}: {}",
                        r.node,
                        r.incarnation,
                        r.thread,
                        r.name,
                        if r.present { "present" } else { "absent" },
                        r.inv,
                        r.name,
                        chain.join(" "),
                    );
                    if tentative_name {
                        report.tentative += 1;
                    } else if r.timed_out {
                        report.degraded_violations += 1;
                    } else {
                        let checker = match kind {
                            Kind::Own => "read_your_writes",
                            Kind::Observed => "monotonic_reads",
                            Kind::Causal => "causal_order",
                        };
                        report.violations.push((checker.to_string(), what));
                    }
                }
            }
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(non_upper_case_globals)]
    static none_j: std::sync::LazyLock<JournalOrder> = std::sync::LazyLock::new(JournalOrder::new);

    fn rid(node: u64, seq: u64) -> Rid {
        Rid {
            node,
            incarnation: 1,
            seq,
        }
    }

    fn read(node: u64, name: &str, present: bool, inv: u64, ret: u64) -> ReadEvt {
        ReadEvt {
            node,
            incarnation: 1,
            thread: node * 16,
            name: name.into(),
            present,
            inv,
            ret,
            timed_out: false,
        }
    }

    /// Node 1 creates `a` (log 0); node 2's create is refused `EEXIST`;
    /// node 2 then reads `a` absent: a monotonic-reads violation. Node 1
    /// reading it absent after its own create: read-your-writes.
    #[test]
    fn finds_both_violations_and_accepts_a_clean_history() {
        let events = vec![
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 32,
                rid: rid(2, 1),
                op: NsOp::Create("a".into()),
            },
            HistEvt::Return {
                thread: 32,
                rid: rid(2, 1),
                ret: NsRet::Eexist,
            },
        ];
        let ticks = vec![0, 1, 2, 3];
        let log: HashMap<Rid, (u64, usize)> = [(rid(1, 1), (1, 0))].into_iter().collect();
        let none = HashSet::new();
        let bad = vec![read(2, "a", false, 4, 5), read(1, "a", false, 6, 7)];
        let r = check_sessions(
            &events,
            &ticks,
            &bad,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Global,
        );
        let mut kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        kinds.sort();
        assert_eq!(kinds, vec!["monotonic_reads", "read_your_writes"], "{r:?}");
        // Reading `a` absent *before* the refusal is fine (cross-node
        // staleness), and present afterwards.
        let good = vec![read(2, "a", false, 2, 2), read(2, "a", true, 4, 5)];
        let r = check_sessions(
            &events,
            &ticks,
            &good,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Global,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        // A tentative create exempts the name.
        let tentative: HashSet<Rid> = [rid(1, 1)].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &bad,
            &tentative,
            &HashMap::new(),
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Global,
        );
        assert!(r.violations.is_empty(), "{r:?}");
    }

    /// Campaign 7's B-1 shape: node 1 creates `d1/a` (an object), and
    /// once that returned, `d2/b` (the ref naming it) — different
    /// directories, possibly different owners. Node 2 that read `d2/b`
    /// present must read `d1/a` present afterwards; reading `d1/a`
    /// absent before it saw `d2/b` is plain staleness.
    #[test]
    fn observing_a_write_carries_what_its_node_wrote_before() {
        let events = vec![
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 1),
                op: NsOp::Create("d1/a".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 1),
                ret: NsRet::Ok,
            },
            HistEvt::Invoke {
                thread: 16,
                rid: rid(1, 2),
                op: NsOp::Create("d2/b".into()),
            },
            HistEvt::Return {
                thread: 16,
                rid: rid(1, 2),
                ret: NsRet::Ok,
            },
        ];
        let ticks = vec![0, 1, 2, 3];
        let log: HashMap<Rid, (u64, usize)> = [(rid(1, 1), (1, 0)), (rid(1, 2), (1, 1))]
            .into_iter()
            .collect();
        let none = HashSet::new();
        let bad = vec![read(2, "d2/b", true, 4, 5), read(2, "d1/a", false, 6, 7)];
        let r = check_sessions(
            &events,
            &ticks,
            &bad,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["causal_order"], "{r:?}");
        let good = vec![
            read(2, "d1/a", false, 2, 3),
            read(2, "d2/b", true, 4, 5),
            read(2, "d1/a", true, 6, 7),
        ];
        let r = check_sessions(
            &events,
            &ticks,
            &good,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        // The two creates in flight together: `d2/b` carries nothing.
        let overlapping = vec![
            events[0].clone(),
            events[2].clone(),
            events[1].clone(),
            events[3].clone(),
        ];
        let r = check_sessions(
            &overlapping,
            &ticks,
            &bad,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        // A tentative `d1/a` carries nothing either.
        let tentative: HashSet<Rid> = [rid(1, 1)].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &bad,
            &tentative,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
    }

    /// `long-delegated-late-answers` 4125 (slow lease reads): node 3's
    /// `create d2/x`, acknowledged by d2's delegate, returns before node 2
    /// unlinks `d1/f`; the root appends it after its own `create d1/f`
    /// (log 0, 1), and node 4's `unlink d1/f` lands last (log 2). Node 2's
    /// unlink is refused `ENOENT` at the root before the create (state 0),
    /// and its next read sees `d1/f` present (state 1): monotonic. Under
    /// delegation `d2/x` returned before the refusal was invoked but is
    /// logged after the refusal's row, so the real-time window must not
    /// place the refusal after it. A journaled refusal needs no window:
    /// its `Refused` row says where it was evaluated.
    fn another_owners_earlier_success(
        dir_of_x: &str,
        by: u64,
    ) -> (Vec<HistEvt>, HashMap<Rid, (u64, usize)>) {
        let ev = |thread: u64, rid: Rid, op: Option<NsOp>, ret: NsRet| match op {
            Some(op) => HistEvt::Invoke { thread, rid, op },
            None => HistEvt::Return { thread, rid, ret },
        };
        let x = format!("{dir_of_x}/x");
        let events = vec![
            ev(by * 16, rid(by, 1), Some(NsOp::Create(x)), NsRet::Ok),
            ev(by * 16, rid(by, 1), None, NsRet::Ok),
            ev(32, rid(2, 2), Some(NsOp::Unlink("d1/f".into())), NsRet::Ok),
            ev(16, rid(1, 1), Some(NsOp::Create("d1/f".into())), NsRet::Ok),
            ev(64, rid(4, 1), Some(NsOp::Unlink("d1/f".into())), NsRet::Ok),
            ev(16, rid(1, 1), None, NsRet::Ok),
            ev(32, rid(2, 2), None, NsRet::Enoent),
            ev(64, rid(4, 1), None, NsRet::Ok),
        ];
        let log: HashMap<Rid, (u64, usize)> = [
            (rid(1, 1), (1, 0)),
            (rid(by, 1), (2, 0)),
            (rid(4, 1), (3, 0)),
        ]
        .into_iter()
        .collect();
        (events, log)
    }

    /// The fallback window (no `Refused` row in the log): the same
    /// history without rows, per scope.
    #[test]
    fn an_unjournaled_refusal_is_not_placed_after_another_owners_earlier_success() {
        let ticks: Vec<u64> = (0..8).collect();
        let none = HashSet::new();
        // Node 2 reads `d1/f` present after its refusal returned (tick 6).
        let reads = vec![read(2, "d1/f", true, 8, 9)];
        let (events, log) = another_owners_earlier_success("d2", 3);
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        // One sequencer orders everything in real time: a violation.
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Global,
        );
        let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["monotonic_reads"], "{r:?}");
        // The earlier success in the refusal's own directory binds it.
        let (events, log) = another_owners_earlier_success("d1", 3);
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["monotonic_reads"], "{r:?}");
        // So does the refusing node's own earlier success, in any directory.
        let (events, log) = another_owners_earlier_success("d2", 2);
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(kinds, vec!["monotonic_reads"], "{r:?}");
    }

    /// A journaled refusal sits at its own `Refused` row, whatever the
    /// scope and the real-time order of other directories' successes.
    #[test]
    fn a_journaled_refusal_is_placed_at_its_row() {
        let ticks: Vec<u64> = (0..8).collect();
        let none = HashSet::new();
        let reads = vec![read(2, "d1/f", true, 8, 9)];
        let (events, log) = another_owners_earlier_success("d2", 3);
        let row = |at: (u64, usize)| -> HashMap<Rid, ((u64, usize), NsRet)> {
            [(rid(2, 2), (at, NsRet::Enoent))].into_iter().collect()
        };
        // (c) The 4125 shape: the row precedes the other owner's
        // late-appended success (and the create): state 0, then the read
        // sees `d1/f` present at state 1. Accepted in every scope.
        for scope in [Scope::Global, Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events,
                &ticks,
                &reads,
                &none,
                &log,
                &row((0, 0)),
                &none_j,
                &none_j,
                scope,
            );
            assert!(r.violations.is_empty(), "{scope:?}: {r:?}");
            assert_eq!((r.refusals_placed, r.refusals_windowed), (1, 0));
        }
        // (b) The same refusal whose row lands after `d2/x` and node 4's
        // unlink (state 3: absent): the read of `d1/f` present is stale,
        // in every scope; `d2/x` is another directory but sits before the
        // row.
        for scope in [Scope::Global, Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events,
                &ticks,
                &reads,
                &none,
                &log,
                &row((3, 1)),
                &none_j,
                &none_j,
                scope,
            );
            let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(kinds, vec!["monotonic_reads"], "{scope:?}: {r:?}");
        }
        // The row's rid is not the refusal's: the window decides, and
        // `Dir` accepts it (the documented leniency of the fallback).
        let other: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(9, 9), ((3, 1), NsRet::Enoent))].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &other,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        assert_eq!((r.refusals_placed, r.refusals_windowed), (0, 1));
    }

    /// The review's M1 shape (`lda`): `d1` and `d3` have one sequencer. Node
    /// 4's `create d1/f` (log 0) and `unlink d1/f` (log 1) are sequenced but
    /// have not returned; node 5's `create d3/y` (log 2) returns; node 2's
    /// `unlink d1/f` is then invoked, and the root refuses it `ENOENT` after
    /// log 2 (state 3). Node 2 reading `d1/f` present afterwards is stale.
    /// The scoped window alone misses it (`Dir` sees no success of `d1`
    /// returned: state 0, explained by state 1); the row finds it.
    #[test]
    fn a_refusal_row_after_another_directorys_success_binds_the_read() {
        let ev = |thread: u64, rid: Rid, op: Option<NsOp>, ret: NsRet| match op {
            Some(op) => HistEvt::Invoke { thread, rid, op },
            None => HistEvt::Return { thread, rid, ret },
        };
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d1/f".into())), NsRet::Ok),
            ev(65, rid(4, 2), Some(NsOp::Unlink("d1/f".into())), NsRet::Ok),
            ev(80, rid(5, 1), Some(NsOp::Create("d3/y".into())), NsRet::Ok),
            ev(80, rid(5, 1), None, NsRet::Ok),
            ev(32, rid(2, 1), Some(NsOp::Unlink("d1/f".into())), NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Enoent),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(65, rid(4, 2), None, NsRet::Ok),
        ];
        let ticks: Vec<u64> = (0..8).collect();
        let log: HashMap<Rid, (u64, usize)> = [
            (rid(4, 1), (1, 0)),
            (rid(4, 2), (1, 1)),
            (rid(5, 1), (1, 2)),
        ]
        .into_iter()
        .collect();
        let rows: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(2, 1), ((1, 3), NsRet::Enoent))].into_iter().collect();
        let none = HashSet::new();
        let reads = vec![read(2, "d1/f", true, 8, 9)];
        // Accepted on the window alone: the leniency the row closes.
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &HashMap::new(),
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        for scope in [Scope::Global, Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events, &ticks, &reads, &none, &log, &rows, &none_j, &none_j, scope,
            );
            let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(kinds, vec!["monotonic_reads"], "{scope:?}: {r:?}");
        }
        // Reading it absent is fine.
        let reads = vec![read(2, "d1/f", false, 8, 9)];
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
    }

    /// A delegate's refusal whose row the root appends late: node 4's
    /// `create d2/f` and `unlink d2/f` are acknowledged by the delegate of
    /// `d2` (ticks 0-3); node 2's `unlink d2/f` is refused `ENOENT` by it
    /// (invoked at tick 4, returned at 5); node 1's `create d1/a`, invoked
    /// at tick 6 and sequenced by the root, is appended first. Log: the
    /// create `d1/a`, the two `d2` ops, the refusal's row (state 3). The
    /// reply carries that position, so node 2 reading `d2/f` present is
    /// stale although a success was invoked after the refusal returned
    /// (`hi` is 0): the row is not cut by real time.
    #[test]
    fn a_late_row_is_not_cut_by_a_success_invoked_after_the_refusal_returned() {
        let ev = |thread: u64, rid: Rid, op: Option<NsOp>, ret: NsRet| match op {
            Some(op) => HistEvt::Invoke { thread, rid, op },
            None => HistEvt::Return { thread, rid, ret },
        };
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d2/f".into())), NsRet::Ok),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(64, rid(4, 2), Some(NsOp::Unlink("d2/f".into())), NsRet::Ok),
            ev(64, rid(4, 2), None, NsRet::Ok),
            ev(32, rid(2, 1), Some(NsOp::Unlink("d2/f".into())), NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Enoent),
            ev(16, rid(1, 1), Some(NsOp::Create("d1/a".into())), NsRet::Ok),
            ev(16, rid(1, 1), None, NsRet::Ok),
        ];
        let ticks: Vec<u64> = (0..8).collect();
        let log: HashMap<Rid, (u64, usize)> = [
            (rid(1, 1), (1, 0)),
            (rid(4, 1), (2, 0)),
            (rid(4, 2), (2, 1)),
        ]
        .into_iter()
        .collect();
        let rows = |at: (u64, usize)| -> HashMap<Rid, ((u64, usize), NsRet)> {
            [(rid(2, 1), (at, NsRet::Enoent))].into_iter().collect()
        };
        let none = HashSet::new();
        let present = vec![read(2, "d2/f", true, 8, 9)];
        let absent = vec![read(2, "d2/f", false, 8, 9)];
        for scope in [Scope::Global, Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events,
                &ticks,
                &present,
                &none,
                &log,
                &rows((2, 2)),
                &none_j,
                &none_j,
                scope,
            );
            let kinds: Vec<&str> = r.violations.iter().map(|(k, _)| k.as_str()).collect();
            assert_eq!(kinds, vec!["monotonic_reads"], "{scope:?}: {r:?}");
            let r = check_sessions(
                &events,
                &ticks,
                &absent,
                &none,
                &log,
                &rows((2, 2)),
                &none_j,
                &none_j,
                scope,
            );
            assert!(r.violations.is_empty(), "{scope:?}: {r:?}");
            // A row before the delegate's ops (state 1: absent) is placed
            // there, and the read of `present` (state 2) is monotonic.
            let r = check_sessions(
                &events,
                &ticks,
                &present,
                &none,
                &log,
                &rows((1, 5)),
                &none_j,
                &none_j,
                scope,
            );
            assert!(r.violations.is_empty(), "{scope:?}: {r:?}");
        }
    }

    /// The row must agree with the refusal to place it: an `ENOENT` row at
    /// a state where the name is present is not where the refusal was
    /// evaluated (an inbox answer journaled late), so the window decides
    /// and the disagreement is counted; a row whose code is not the
    /// client's return is not used either.
    #[test]
    fn a_row_that_disagrees_with_its_refusal_falls_back_to_the_window() {
        let ev = |thread: u64, rid: Rid, op: Option<NsOp>, ret: NsRet| match op {
            Some(op) => HistEvt::Invoke { thread, rid, op },
            None => HistEvt::Return { thread, rid, ret },
        };
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d2/f".into())), NsRet::Ok),
            ev(32, rid(2, 1), Some(NsOp::Unlink("d2/f".into())), NsRet::Ok),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Enoent),
        ];
        let ticks: Vec<u64> = (0..4).collect();
        let log: HashMap<Rid, (u64, usize)> = [(rid(4, 1), (1, 0))].into_iter().collect();
        let none = HashSet::new();
        // The refusal was evaluated before the create (state 0), the row
        // landed after it (state 1: present), the create overlapping it.
        // With the row it would bind a read of `d2/f` absent to state 1
        // or later (a false violation); the window places it at state 0.
        let reads = vec![read(2, "d2/f", false, 4, 5)];
        let rows: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(2, 1), ((1, 1), NsRet::Enoent))].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        assert_eq!(
            (
                r.refusals_placed,
                r.refusals_windowed,
                r.refusals_row_disagrees
            ),
            (0, 1, 1)
        );
        let reads: Vec<ReadEvt> = Vec::new();
        // At the state before the create the row agrees.
        let rows: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(2, 1), ((0, 0), NsRet::Enoent))].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        assert_eq!((r.refusals_placed, r.refusals_windowed), (1, 0));
        // An `EEXIST` row for a refusal the client saw as `ENOENT` is not
        // this answer's outcome: the window places it.
        let rows: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(2, 1), ((1, 1), NsRet::Eexist))].into_iter().collect();
        let r = check_sessions(
            &events,
            &ticks,
            &reads,
            &none,
            &log,
            &rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        assert_eq!((r.refusals_placed, r.refusals_windowed), (0, 1));
    }

    fn ev(thread: u64, rid: Rid, op: Option<NsOp>, ret: NsRet) -> HistEvt {
        match op {
            Some(op) => HistEvt::Invoke { thread, rid, op },
            None => HistEvt::Return { thread, rid, ret },
        }
    }

    fn kinds(r: &SessionReport) -> Vec<&str> {
        r.violations.iter().map(|(k, _)| k.as_str()).collect()
    }

    /// Open leniency 1 (cross-owner `hi`), a read. Node 4's `create d2/a`,
    /// then `create d2/b` (invoked after the first returned), acknowledged
    /// by `d2`'s delegate; node 2 reads `d2/b` present (tick 4-5); node 1's
    /// `create d1/z`, invoked at tick 6 and sequenced by the root, is
    /// logged before the root appends the delegate's two rows. Node 2 then
    /// reads `d2/a` absent: it saw `d2/b`, whose node had been
    /// acknowledged `d2/a` before issuing it (causal). With `hi` cut by the
    /// root's `d1/z` (index 0, below the state that shows `d2/b`) the read
    /// has no causal floors and the stale read passes; a cut by `d2`'s own
    /// successes only finds it.
    #[test]
    fn a_read_is_not_cut_by_another_directorys_later_success() {
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d2/a".into())), NsRet::Ok),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(64, rid(4, 2), Some(NsOp::Create("d2/b".into())), NsRet::Ok),
            ev(64, rid(4, 2), None, NsRet::Ok),
            ev(16, rid(1, 1), Some(NsOp::Create("d1/z".into())), NsRet::Ok),
            ev(16, rid(1, 1), None, NsRet::Ok),
        ];
        let ticks = vec![0, 1, 2, 3, 6, 7];
        let log: HashMap<Rid, (u64, usize)> = [
            (rid(1, 1), (1, 0)),
            (rid(4, 1), (2, 0)),
            (rid(4, 2), (2, 1)),
        ]
        .into_iter()
        .collect();
        let none = HashSet::new();
        let no_rows = HashMap::new();
        let bad = vec![read(2, "d2/b", true, 4, 5), read(2, "d2/a", false, 8, 9)];
        for scope in [Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events, &ticks, &bad, &none, &log, &no_rows, &none_j, &none_j, scope,
            );
            assert_eq!(kinds(&r), vec!["causal_order"], "{scope:?}: {r:?}");
        }
        // One sequencer cannot log `d1/z` (invoked after the read returned)
        // before what the read saw: `Global` keeps the plain cut.
        let r = check_sessions(
            &events,
            &ticks,
            &bad,
            &none,
            &log,
            &no_rows,
            &none_j,
            &none_j,
            Scope::Global,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        // Still accepted: `d2/a` present after, or absent before `d2/b`.
        let good = vec![
            read(2, "d2/a", false, 2, 3),
            read(2, "d2/b", true, 4, 5),
            read(2, "d2/a", true, 8, 9),
        ];
        for scope in [Scope::Global, Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events, &ticks, &good, &none, &log, &no_rows, &none_j, &none_j, scope,
            );
            assert!(r.violations.is_empty(), "{scope:?}: {r:?}");
        }
        // A later success of `d2` itself still cuts: node 5's `unlink d2/b`
        // invoked after the read and logged right after it.
        let mut events2 = events.clone();
        events2.push(ev(
            80,
            rid(5, 1),
            Some(NsOp::Unlink("d2/b".into())),
            NsRet::Ok,
        ));
        events2.push(ev(80, rid(5, 1), None, NsRet::Ok));
        let mut ticks2 = ticks.clone();
        ticks2.extend([10, 11]);
        let mut log2 = log.clone();
        log2.insert(rid(5, 1), (2, 2));
        let r = check_sessions(
            &events2,
            &ticks2,
            &bad,
            &none,
            &log2,
            &no_rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert_eq!(kinds(&r), vec!["causal_order"], "{r:?}");
    }

    /// Open leniency 1, the fallback window: an unjournaled `EEXIST` on
    /// `d2/f` after the delegate acknowledged `create d2/f` (state 2) is
    /// dropped when the root's `create d1/z`, invoked after the refusal
    /// returned, is logged first (`hi` 0 below k 2); node 2 then reading
    /// `d2/f` absent is stale.
    #[test]
    fn an_unjournaled_refusal_is_not_dropped_by_another_directorys_later_success() {
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d2/f".into())), NsRet::Ok),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(32, rid(2, 1), Some(NsOp::Create("d2/f".into())), NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Eexist),
            ev(16, rid(1, 1), Some(NsOp::Create("d1/z".into())), NsRet::Ok),
            ev(16, rid(1, 1), None, NsRet::Ok),
        ];
        let ticks: Vec<u64> = (0..6).collect();
        let log: HashMap<Rid, (u64, usize)> = [(rid(1, 1), (1, 0)), (rid(4, 1), (2, 0))]
            .into_iter()
            .collect();
        let none = HashSet::new();
        let no_rows = HashMap::new();
        for scope in [Scope::Dir, Scope::Range(1)] {
            let r = check_sessions(
                &events,
                &ticks,
                &[read(2, "d2/f", false, 6, 7)],
                &none,
                &log,
                &no_rows,
                &none_j,
                &none_j,
                scope,
            );
            assert_eq!(kinds(&r), vec!["monotonic_reads"], "{scope:?}: {r:?}");
            assert_eq!(r.refusals_windowed, 1);
            let r = check_sessions(
                &events,
                &ticks,
                &[read(2, "d2/f", true, 6, 7)],
                &none,
                &log,
                &no_rows,
                &none_j,
                &none_j,
                scope,
            );
            assert!(r.violations.is_empty(), "{scope:?}: {r:?}");
        }
    }

    /// Open leniency 2 (raw log order), write-back: node 2's `create
    /// d1/a` (journal 1), then its `link d0/s d2/x` (journal 2) held on a
    /// pending chunk; node 1's `create d3/y` (journal 3, invoked after node
    /// 3's read returned) ships ahead of it. Node 3 read `d2/x` present
    /// (the held row, streamed in journal order), so it must see `d1/a`
    /// afterwards. In raw log order `d3/y` (index 1) cuts below the state
    /// showing `d2/x` (3) and drops the causal floor; in witness order
    /// (`d1/a`, the link, `d3/y`) it is found. The run is one tenure's
    /// (epoch 1, node 9).
    #[test]
    fn a_held_row_is_ordered_as_its_tenure_journaled_it() {
        let events = vec![
            ev(32, rid(2, 1), Some(NsOp::Create("d1/a".into())), NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Ok),
            ev(
                32,
                rid(2, 2),
                Some(NsOp::Link("d0/s".into(), "d2/x".into())),
                NsRet::Ok,
            ),
            ev(32, rid(2, 2), None, NsRet::Ok),
            ev(16, rid(1, 1), Some(NsOp::Create("d3/y".into())), NsRet::Ok),
            ev(16, rid(1, 1), None, NsRet::Ok),
        ];
        let ticks = vec![0, 1, 2, 3, 6, 7];
        let log: HashMap<Rid, (u64, usize)> = [
            (rid(2, 1), (1, 0)),
            (rid(1, 1), (1, 1)),
            (rid(2, 2), (2, 0)),
        ]
        .into_iter()
        .collect();
        let journal: JournalOrder = [
            (rid(2, 1), (1, 9, 1)),
            (rid(2, 2), (1, 9, 2)),
            (rid(1, 1), (1, 9, 3)),
        ]
        .into_iter()
        .collect();
        let none = HashSet::new();
        let no_rows = HashMap::new();
        let bad = vec![read(3, "d2/x", true, 4, 5), read(3, "d1/a", false, 8, 9)];
        for scope in [Scope::Global, Scope::Dir] {
            let r = check_sessions(
                &events, &ticks, &bad, &none, &log, &no_rows, &journal, &none_j, scope,
            );
            assert_eq!(kinds(&r), vec!["causal_order"], "{scope:?}: {r:?}");
            // Raw log order (no journal rows) misses it under `Global`.
            if scope == Scope::Global {
                let r = check_sessions(
                    &events, &ticks, &bad, &none, &log, &no_rows, &none_j, &none_j, scope,
                );
                assert!(r.violations.is_empty(), "{r:?}");
            }
        }
        let good = vec![read(3, "d2/x", true, 4, 5), read(3, "d1/a", true, 8, 9)];
        let r = check_sessions(
            &events,
            &ticks,
            &good,
            &none,
            &log,
            &no_rows,
            &journal,
            &none_j,
            Scope::Global,
        );
        assert!(r.violations.is_empty(), "{r:?}");
    }

    /// A refusal row shipped ahead of a held row it was evaluated after:
    /// node 4's `unlink d2/x` (journal 2) is held, node 2's `unlink d2/x`
    /// refused `ENOENT` (journal 3) ships with the earlier rows. In raw log
    /// order the row sits where `d2/x` is present (it disagrees and the
    /// window places it); in journal order it is placed at its row, and a
    /// later read of `d2/x` present is stale.
    #[test]
    fn a_refusal_row_is_ordered_as_its_tenure_journaled_it() {
        let events = vec![
            ev(64, rid(4, 1), Some(NsOp::Create("d2/x".into())), NsRet::Ok),
            ev(64, rid(4, 1), None, NsRet::Ok),
            ev(64, rid(4, 2), Some(NsOp::Unlink("d2/x".into())), NsRet::Ok),
            ev(64, rid(4, 2), None, NsRet::Ok),
            ev(32, rid(2, 1), Some(NsOp::Unlink("d2/x".into())), NsRet::Ok),
            ev(32, rid(2, 1), None, NsRet::Enoent),
        ];
        let ticks: Vec<u64> = (0..6).collect();
        let log: HashMap<Rid, (u64, usize)> = [(rid(4, 1), (1, 0)), (rid(4, 2), (2, 0))]
            .into_iter()
            .collect();
        let journal: JournalOrder = [(rid(4, 1), (1, 9, 1)), (rid(4, 2), (1, 9, 2))]
            .into_iter()
            .collect();
        let rows: HashMap<Rid, ((u64, usize), NsRet)> =
            [(rid(2, 1), ((1, 1), NsRet::Enoent))].into_iter().collect();
        let rjournal: JournalOrder = [(rid(2, 1), (1, 9, 3))].into_iter().collect();
        let none = HashSet::new();
        let r = check_sessions(
            &events,
            &ticks,
            &[read(2, "d2/x", true, 6, 7)],
            &none,
            &log,
            &rows,
            &journal,
            &rjournal,
            Scope::Dir,
        );
        assert_eq!(kinds(&r), vec!["monotonic_reads"], "{r:?}");
        assert_eq!((r.refusals_placed, r.refusals_row_disagrees), (1, 0));
        // Raw order: the row disagrees, the window decides.
        let r = check_sessions(
            &events,
            &ticks,
            &[read(2, "d2/x", false, 6, 7)],
            &none,
            &log,
            &rows,
            &none_j,
            &none_j,
            Scope::Dir,
        );
        assert!(r.violations.is_empty(), "{r:?}");
        assert_eq!((r.refusals_placed, r.refusals_row_disagrees), (0, 1));
    }
}
