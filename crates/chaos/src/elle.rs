//! Elle-style dependency-cycle detection over rename and link histories
//! (plan 30 §M4 item 5).
//!
//! Elle (Kingsbury & Alvaro, VLDB 2020) infers a dependency graph between
//! the operations of a history — write-write, write-read and read-write
//! (anti-) dependencies from the values each operation observed, plus
//! real-time order — and reports any cycle as an anomaly: no serial
//! order can explain what the operations saw. This is the same idea for
//! a filesystem namespace, where each path is a register and a file's
//! identity (its unique content) is the value.
//!
//! # Single-assignment paths
//!
//! Elle needs *recoverable* versions: every read must name the write it
//! observed. The chaos generator makes that true for the paths this
//! checker looks at: each path receives a file at most once (a create, or
//! a rename or link *to* it) and loses it at most once (an unlink, or a
//! rename *from* it), and every file is created with unique content. So a
//! path's version order is fixed — absent, then the one file, then absent
//! again — and every observation maps to one version:
//!
//! | operation | reads | writes |
//! |---|---|---|
//! | create `p` (ok) | `p` = absent | `p` = its file |
//! | rename `a`→`b` (ok) | `a` = file | `b` = file, `a` = absent |
//! | link `a`→`b` (ok) | `a` = file | `b` = file |
//! | unlink `p` (ok) | `p` = file | `p` = absent |
//! | read / stat `p` (ok) | `p` = file | — |
//! | any of them failing `ENOENT` | the source = absent | — |
//! | create / link / mkdir failing `EEXIST` | the target = file | — |
//!
//! A path that sees any other write (overwrite, truncate, append, a
//! second file) is left out: its versions are not recoverable. Reads that
//! return content that is not the path's file are left to the other
//! checkers.
//!
//! # Edges
//!
//! For a path `k` whose file was placed by `W` and removed by `D`:
//! - `ww`: `W → D`;
//! - `wr`: `W → R` for every `R` that observed the file, and `D → R` for
//!   an `R` that observed the path absent *after* the file (see below);
//! - `rw`: `R → D` for every `R` that observed the file (it must precede
//!   the removal), and `R → W` for an `R` that observed the path absent
//!   *before* the file;
//! - `rt`: `A → B` when `A` completed before `B` was invoked (the usual
//!   frontier construction, so the graph stays linear in the history).
//!
//! An "absent" observation is ambiguous between before and after the
//! file. Under linearizability an observation that started after `W`
//! completed can only be the later one, and one that completed before
//! `D` was invoked only the earlier one; any other is skipped. A path
//! whose file is never removed has only the earlier absence, so a read
//! of "absent" after the placement completed forms `W →rt→ R →rw→ W`: a
//! lost write.
//!
//! Two ok removals of the same file are reported directly: each needed
//! the file present, so one of them took effect on a state it could not
//! have seen.
//!
//! A cycle is classified like Elle's: only `ww` edges is G0, `ww`/`wr`
//! is G1c, exactly one `rw` is G-single, more is G2; with `rt` edges the
//! class gets a `-realtime` suffix (a violation of linearizability rather
//! than of serializability alone).

use crate::check::CheckFailure;
use crate::history::{EventKind, History};
use crate::op::{Complete, Op, Outcome};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Synthetic graph nodes for quiesce barriers count down from here; real
/// operation ids count up from 1.
const BARRIER_BASE: u64 = u64::MAX;

/// Why one operation must precede another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EdgeKind {
    Ww,
    Wr,
    Rw,
    Rt,
}

impl EdgeKind {
    fn as_str(self) -> &'static str {
        match self {
            EdgeKind::Ww => "ww",
            EdgeKind::Wr => "wr",
            EdgeKind::Rw => "rw",
            EdgeKind::Rt => "rt",
        }
    }
}

/// One completed operation, as the checker sees it.
#[derive(Debug, Clone)]
struct Done {
    op_id: u64,
    op: Op,
    complete: Complete,
    /// The worker (one mount) that ran it.
    worker: usize,
    /// History index of the invoke and of the completion.
    invoked: u64,
    completed: u64,
}

fn completed_ops(history: &History) -> Vec<Done> {
    let mut invokes: HashMap<u64, (u64, usize, Op)> = HashMap::new();
    let mut out = Vec::new();
    for ev in history.events() {
        match ev.kind {
            EventKind::Invoke => {
                if let Some(op) = &ev.op {
                    invokes.insert(ev.op_id, (ev.index, ev.worker_id, op.clone()));
                }
            }
            EventKind::Ok | EventKind::Fail => {
                if let (Some((invoked, worker, op)), Some(complete)) =
                    (invokes.remove(&ev.op_id), ev.complete.clone())
                {
                    out.push(Done {
                        op_id: ev.op_id,
                        op,
                        complete,
                        worker,
                        invoked,
                        completed: ev.index,
                    });
                }
            }
            EventKind::Info => {}
        }
    }
    out
}

fn errno_is(c: &Complete, name: &str) -> bool {
    c.outcome == Outcome::Fail && c.errno_name.as_deref() == Some(name)
}

/// What an operation did to, or saw at, one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Touch {
    /// Put a file there (its identity is resolved later).
    Place,
    /// Removed the file there.
    Remove,
    /// Saw the path's file.
    SawFile,
    /// Saw the path absent.
    SawAbsent,
}

/// The per-path facts an operation contributes.
fn touches(d: &Done) -> Vec<(String, Touch)> {
    let ok = d.complete.outcome == Outcome::Ok;
    let enoent = errno_is(&d.complete, "ENOENT");
    let eexist = errno_is(&d.complete, "EEXIST");
    match &d.op {
        Op::Create { path, .. } | Op::Mkdir { path } if ok => vec![(path.clone(), Touch::Place)],
        Op::Create { path, .. } | Op::Mkdir { path } if eexist => {
            vec![(path.clone(), Touch::SawFile)]
        }
        Op::Rename { from, to } if ok => vec![
            (from.clone(), Touch::SawFile),
            (from.clone(), Touch::Remove),
            (to.clone(), Touch::Place),
        ],
        Op::Link { from, to } if ok => {
            vec![(from.clone(), Touch::SawFile), (to.clone(), Touch::Place)]
        }
        Op::Link { to, .. } if eexist => vec![(to.clone(), Touch::SawFile)],
        Op::Rename { from, .. } | Op::Link { from, .. } if enoent => {
            vec![(from.clone(), Touch::SawAbsent)]
        }
        Op::Unlink { path } | Op::Rmdir { path } if ok => vec![
            (path.clone(), Touch::SawFile),
            (path.clone(), Touch::Remove),
        ],
        Op::Unlink { path } | Op::Rmdir { path } if enoent => {
            vec![(path.clone(), Touch::SawAbsent)]
        }
        Op::Stat { path } | Op::Read { path } if ok => vec![(path.clone(), Touch::SawFile)],
        Op::Stat { path } | Op::Read { path } if enoent => {
            vec![(path.clone(), Touch::SawAbsent)]
        }
        _ => Vec::new(),
    }
}

/// Paths that saw a write this checker cannot attribute to one version.
fn mutable_paths(ops: &[Done]) -> HashSet<String> {
    let mut out = HashSet::new();
    for d in ops {
        if d.complete.outcome != Outcome::Ok {
            continue;
        }
        match &d.op {
            Op::WriteFull { path, .. }
            | Op::WriteAt { path, .. }
            | Op::Append { path, .. }
            | Op::Truncate { path, .. } => {
                out.insert(path.clone());
            }
            _ => {}
        }
    }
    out
}

/// A dependency graph over operation ids.
#[derive(Default)]
struct Graph {
    edges: BTreeMap<u64, BTreeMap<u64, BTreeSet<EdgeKind>>>,
}

impl Graph {
    fn add(&mut self, from: u64, to: u64, kind: EdgeKind) {
        if from != to {
            self.edges
                .entry(from)
                .or_default()
                .entry(to)
                .or_default()
                .insert(kind);
        }
    }

    fn successors(&self, n: u64) -> impl Iterator<Item = u64> + '_ {
        self.edges
            .get(&n)
            .into_iter()
            .flat_map(|m| m.keys().copied())
    }

    fn nodes(&self) -> BTreeSet<u64> {
        let mut out = BTreeSet::new();
        for (from, tos) in &self.edges {
            out.insert(*from);
            out.extend(tos.keys().copied());
        }
        out
    }

    fn kinds(&self, from: u64, to: u64) -> BTreeSet<EdgeKind> {
        self.edges
            .get(&from)
            .and_then(|m| m.get(&to))
            .cloned()
            .unwrap_or_default()
    }

    /// Strongly connected components with more than one node (Tarjan,
    /// iterative).
    fn cyclic_components(&self) -> Vec<Vec<u64>> {
        let nodes: Vec<u64> = self.nodes().into_iter().collect();
        let mut index: HashMap<u64, usize> = HashMap::new();
        let mut low: HashMap<u64, usize> = HashMap::new();
        let mut on_stack: HashSet<u64> = HashSet::new();
        let mut stack: Vec<u64> = Vec::new();
        let mut next = 0usize;
        let mut out = Vec::new();
        for &root in &nodes {
            if index.contains_key(&root) {
                continue;
            }
            // (node, successors not yet visited)
            let mut work: Vec<(u64, Vec<u64>)> = Vec::new();
            index.insert(root, next);
            low.insert(root, next);
            next += 1;
            stack.push(root);
            on_stack.insert(root);
            work.push((root, self.successors(root).collect()));
            while let Some((node, pending)) = work.last_mut() {
                let node = *node;
                if let Some(succ) = pending.pop() {
                    if let std::collections::hash_map::Entry::Vacant(e) = index.entry(succ) {
                        e.insert(next);
                        low.insert(succ, next);
                        next += 1;
                        stack.push(succ);
                        on_stack.insert(succ);
                        work.push((succ, self.successors(succ).collect()));
                    } else if on_stack.contains(&succ) {
                        let l = low[&node].min(index[&succ]);
                        low.insert(node, l);
                    }
                    continue;
                }
                work.pop();
                if let Some((parent, _)) = work.last() {
                    let l = low[parent].min(low[&node]);
                    low.insert(*parent, l);
                }
                if low[&node] == index[&node] {
                    let mut comp = Vec::new();
                    while let Some(top) = stack.pop() {
                        on_stack.remove(&top);
                        comp.push(top);
                        if top == node {
                            break;
                        }
                    }
                    if comp.len() > 1 {
                        comp.sort_unstable();
                        out.push(comp);
                    }
                }
            }
        }
        out
    }

    /// A shortest cycle through `start` inside `comp` (BFS).
    fn cycle_in(&self, comp: &[u64]) -> Vec<u64> {
        let members: HashSet<u64> = comp.iter().copied().collect();
        let start = comp[0];
        let mut prev: HashMap<u64, u64> = HashMap::new();
        let mut queue = std::collections::VecDeque::from([start]);
        let mut seen: HashSet<u64> = HashSet::from([start]);
        while let Some(n) = queue.pop_front() {
            for s in self.successors(n) {
                if !members.contains(&s) {
                    continue;
                }
                if s == start {
                    let mut path = vec![n];
                    let mut cur = n;
                    while cur != start {
                        cur = prev[&cur];
                        path.push(cur);
                    }
                    path.reverse();
                    return path;
                }
                if seen.insert(s) {
                    prev.insert(s, n);
                    queue.push_back(s);
                }
            }
        }
        comp.to_vec()
    }
}

/// Elle's name for a cycle with these edges.
fn classify(kinds: &[BTreeSet<EdgeKind>]) -> String {
    // Prefer the weakest explanation of each hop: a hop that is also a
    // `ww` or `wr` counts as that, not as `rw` or `rt`.
    let mut rw = 0;
    let mut rt = false;
    let mut wr = false;
    for k in kinds {
        if k.contains(&EdgeKind::Ww) {
            continue;
        }
        if k.contains(&EdgeKind::Wr) {
            wr = true;
            continue;
        }
        if k.contains(&EdgeKind::Rw) {
            rw += 1;
            continue;
        }
        rt = true;
    }
    let base = match (rw, wr) {
        (0, false) => "G0",
        (0, true) => "G1c",
        (1, _) => "G-single",
        _ => "G2",
    };
    if rt {
        format!("{base}-realtime")
    } else {
        base.to_string()
    }
}

/// See the module doc.
pub fn check_cycles(history: &History) -> Result<(), CheckFailure> {
    let ops = completed_ops(history);
    let mutable = mutable_paths(&ops);
    let by_id: HashMap<u64, &Done> = ops.iter().map(|d| (d.op_id, d)).collect();

    // Per path: who placed and removed its file, who saw what.
    #[derive(Default)]
    struct PathFacts {
        placers: Vec<u64>,
        removers: Vec<u64>,
        saw_file: Vec<u64>,
        saw_absent: Vec<u64>,
    }
    let mut facts: BTreeMap<String, PathFacts> = BTreeMap::new();
    for d in &ops {
        for (path, touch) in touches(d) {
            if mutable.contains(&path) {
                continue;
            }
            let f = facts.entry(path).or_default();
            match touch {
                Touch::Place => f.placers.push(d.op_id),
                Touch::Remove => f.removers.push(d.op_id),
                Touch::SawFile => f.saw_file.push(d.op_id),
                Touch::SawAbsent => f.saw_absent.push(d.op_id),
            }
        }
    }

    let mut graph = Graph::default();
    let mut involved: HashSet<u64> = HashSet::new();
    for (path, f) in &facts {
        // Single assignment only: exactly one file ever placed here.
        let [w] = f.placers.as_slice() else {
            continue;
        };
        let w = *w;
        if f.removers.len() > 1 {
            return Err(CheckFailure {
                checker: "elle".into(),
                message: format!(
                    "{path}: its one file was removed {} times (ops {:?}); every removal \
                     needed it present",
                    f.removers.len(),
                    f.removers
                ),
                op_ids: f.removers.clone(),
            });
        }
        let d = f.removers.first().copied();
        involved.insert(w);
        if let Some(d) = d {
            graph.add(w, d, EdgeKind::Ww);
            involved.insert(d);
        }
        for &r in &f.saw_file {
            if r == w {
                continue;
            }
            graph.add(w, r, EdgeKind::Wr);
            involved.insert(r);
            if let Some(d) = d {
                graph.add(r, d, EdgeKind::Rw);
            }
        }
        let wd = by_id[&w];
        for &r in &f.saw_absent {
            let rd = by_id[&r];
            let after_placed = rd.invoked > wd.completed;
            match d.map(|d| by_id[&d]) {
                None => {
                    // Only the earlier absence exists.
                    graph.add(r, w, EdgeKind::Rw);
                    involved.insert(r);
                }
                Some(dd) if after_placed => {
                    graph.add(dd.op_id, r, EdgeKind::Wr);
                    involved.insert(r);
                }
                Some(dd) if rd.completed < dd.invoked => {
                    graph.add(r, w, EdgeKind::Rw);
                    involved.insert(r);
                }
                Some(_) => {}
            }
        }
    }

    // Real-time order — as much of it as the filesystem promises today.
    // Writes are sequenced by the lease holder, but a read on another node
    // is only bounded-stale until plan 30 M6-M8 add sessions and read
    // barriers, so wall-clock order between two nodes' operations inside
    // one step is *not* an ordering constraint. What is: one worker's own
    // operations, in order (a worker is one mount: read-your-writes), and
    // everything before a quiesce barrier before everything after it (the
    // coordinator only records the verify reads once every node agrees).
    // Per-worker frontiers keep the graph linear in the history; each
    // barrier is one synthetic node.
    {
        enum Mark {
            Invoke(u64, usize),
            Complete(u64, usize),
            Barrier,
        }
        let mut marks: Vec<(u64, Mark)> = Vec::new();
        for d in &ops {
            if involved.contains(&d.op_id) {
                marks.push((d.invoked, Mark::Invoke(d.op_id, d.worker)));
                marks.push((d.completed, Mark::Complete(d.op_id, d.worker)));
            }
        }
        for ev in history.events() {
            if ev.kind == EventKind::Info
                && ev
                    .info
                    .as_deref()
                    .is_some_and(|i| i.starts_with("quiesce_begin:"))
            {
                marks.push((ev.index, Mark::Barrier));
            }
        }
        marks.sort_by_key(|(index, _)| *index);
        let mut frontiers: HashMap<usize, BTreeSet<u64>> = HashMap::new();
        let mut preds: HashMap<u64, BTreeSet<u64>> = HashMap::new();
        let mut since_barrier: Vec<u64> = Vec::new();
        let mut barrier: Option<u64> = None;
        let mut next_barrier = BARRIER_BASE;
        for (_, mark) in marks {
            match mark {
                Mark::Invoke(op, worker) => {
                    let frontier = frontiers.entry(worker).or_default();
                    for &f in frontier.iter() {
                        graph.add(f, op, EdgeKind::Rt);
                    }
                    preds.insert(op, frontier.clone());
                    if let Some(b) = barrier {
                        graph.add(b, op, EdgeKind::Rt);
                    }
                }
                Mark::Complete(op, worker) => {
                    let frontier = frontiers.entry(worker).or_default();
                    if let Some(p) = preds.remove(&op) {
                        for q in p {
                            frontier.remove(&q);
                        }
                    }
                    frontier.insert(op);
                    since_barrier.push(op);
                }
                Mark::Barrier => {
                    let b = next_barrier;
                    next_barrier -= 1;
                    for op in since_barrier.drain(..) {
                        graph.add(op, b, EdgeKind::Rt);
                    }
                    if let Some(prev) = barrier {
                        graph.add(prev, b, EdgeKind::Rt);
                    }
                    barrier = Some(b);
                }
            }
        }
    }

    if let Some(comp) = graph.cyclic_components().into_iter().next() {
        let cycle = graph.cycle_in(&comp);
        let mut kinds = Vec::new();
        let mut hops = Vec::new();
        for (i, &a) in cycle.iter().enumerate() {
            let b = cycle[(i + 1) % cycle.len()];
            let k = graph.kinds(a, b);
            let names: Vec<&str> = k.iter().map(|k| k.as_str()).collect();
            let what = match by_id.get(&a) {
                Some(d) => format!("{a} {:?}", d.op),
                None => "quiesce barrier".to_string(),
            };
            hops.push(format!("{what} -{}-> {b}", names.join("/")));
            kinds.push(k);
        }
        return Err(CheckFailure {
            checker: "elle".into(),
            message: format!(
                "{} dependency cycle over {} ops: {}",
                classify(&kinds),
                cycle.len(),
                hops.join("; ")
            ),
            op_ids: cycle
                .into_iter()
                .filter(|id| by_id.contains_key(id))
                .collect(),
        });
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::op::hash_bytes;
    use constellation_types::Code;

    fn ok() -> Complete {
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

    fn read_ok(content: &[u8]) -> Complete {
        Complete {
            value_hash: Some(hash_bytes(content)),
            ..ok()
        }
    }

    fn fail(name: &str) -> Complete {
        Complete {
            outcome: Outcome::Fail,
            errno: Some(if name == "ENOENT" {
                Code::NotFound.to_native()
            } else {
                Code::Exists.to_native()
            }),
            errno_name: Some(name.to_string()),
            ..ok()
        }
    }

    /// Builds a history one event at a time; `id` names each op.
    struct H(History);

    impl H {
        fn new() -> Self {
            H(History::new())
        }
        fn inv(&mut self, id: u64, op: Op) -> &mut Self {
            self.0.record_invoke(0, id, op);
            self
        }
        fn done(&mut self, id: u64, c: Complete) -> &mut Self {
            self.0.record_complete(0, id, c);
            self
        }
        /// Invoke and complete back to back.
        fn op(&mut self, id: u64, op: Op, c: Complete) -> &mut Self {
            self.inv(id, op).done(id, c)
        }
        /// The same, on another worker (another mount).
        fn op_on(&mut self, worker: usize, id: u64, op: Op, c: Complete) -> &mut Self {
            self.0.record_invoke(worker, id, op);
            self.0.record_complete(worker, id, c);
            self
        }
        fn barrier(&mut self) -> &mut Self {
            self.0.record_info("quiesce_begin:step");
            self
        }
    }

    fn create(p: &str, content: &[u8]) -> Op {
        Op::Create {
            path: p.into(),
            content: content.to_vec(),
        }
    }
    fn rename(a: &str, b: &str) -> Op {
        Op::Rename {
            from: a.into(),
            to: b.into(),
        }
    }
    fn link(a: &str, b: &str) -> Op {
        Op::Link {
            from: a.into(),
            to: b.into(),
        }
    }
    fn read(p: &str) -> Op {
        Op::Read { path: p.into() }
    }
    fn stat(p: &str) -> Op {
        Op::Stat { path: p.into() }
    }

    #[test]
    fn a_clean_move_and_link_history_has_no_cycle() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, rename("a", "b"), ok())
            .op(3, read("b"), read_ok(b"v"))
            .op(4, stat("a"), fail("ENOENT"))
            .op(5, link("b", "c"), ok())
            .op(6, read("c"), read_ok(b"v"));
        // A read overlapping the rename may see either side.
        h.op(7, create("x", b"w"), ok())
            .inv(8, rename("x", "y"))
            .inv(9, read("x"))
            .done(9, read_ok(b"w"))
            .done(8, ok());
        check_cycles(&h.0).unwrap();
    }

    /// A read that starts after a rename completed must not see the
    /// file at its old name.
    #[test]
    fn a_stale_read_after_a_rename_is_a_cycle() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, rename("a", "b"), ok())
            .op(3, read("a"), read_ok(b"v"));
        let err = check_cycles(&h.0).unwrap_err();
        assert_eq!(err.checker, "elle");
        assert!(err.message.contains("G-single-realtime"), "{err}");
    }

    /// Reads on another node are only bounded-stale inside a step (plan 30
    /// §1.2 L4), so a stale read there is not an anomaly — but once the
    /// quiesce barrier has passed, every node must reflect the rename.
    #[test]
    fn a_stale_read_on_another_node_counts_only_after_a_barrier() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, rename("a", "b"), ok())
            .op_on(1, 3, read("a"), read_ok(b"v"));
        check_cycles(&h.0).unwrap();

        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, rename("a", "b"), ok())
            .barrier()
            .op_on(1, 3, read("a"), read_ok(b"v"));
        let err = check_cycles(&h.0).unwrap_err();
        assert!(err.op_ids.contains(&3), "{err}");
        assert!(err.message.contains("quiesce barrier"), "{err}");
    }

    /// A read that completed before the rename was invoked cannot have
    /// seen the file at its new name.
    #[test]
    fn a_read_from_the_future_is_a_cycle() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, read("b"), read_ok(b"v"))
            .op(3, rename("a", "b"), ok());
        let err = check_cycles(&h.0).unwrap_err();
        assert!(err.message.contains("realtime"), "{err}");
        assert!(err.op_ids.contains(&2) && err.op_ids.contains(&3), "{err}");
    }

    /// A link that completed, then a stat of the new name that finds
    /// nothing (and nobody removed it): the link's write was lost.
    #[test]
    fn a_lost_link_is_a_cycle() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, link("a", "c"), ok())
            .op(3, stat("c"), fail("ENOENT"));
        let err = check_cycles(&h.0).unwrap_err();
        assert!(err.op_ids.contains(&2) && err.op_ids.contains(&3), "{err}");
    }

    /// A rename reporting ENOENT for a source nobody had moved yet, and
    /// then the source still being there: the rename saw a future
    /// absence.
    #[test]
    fn an_enoent_before_the_removal_is_a_cycle() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(2, rename("a", "z"), fail("ENOENT"))
            .op(3, stat("a"), ok());
        let err = check_cycles(&h.0).unwrap_err();
        assert!(err.op_ids.contains(&2), "{err}");
    }

    /// Two renames of the one file both succeeding: one of them moved a
    /// file that was already gone.
    #[test]
    fn a_double_move_is_reported() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .inv(2, rename("a", "b1"))
            .inv(3, rename("a", "b2"))
            .done(2, ok())
            .done(3, ok());
        let err = check_cycles(&h.0).unwrap_err();
        assert!(err.message.contains("removed 2 times"), "{err}");
    }

    /// Overwritten paths are not single-assignment and are ignored.
    #[test]
    fn mutable_paths_are_ignored() {
        let mut h = H::new();
        h.op(1, create("a", b"v"), ok())
            .op(
                2,
                Op::WriteFull {
                    path: "a".into(),
                    content: b"w".to_vec(),
                },
                ok(),
            )
            .op(3, rename("a", "b"), ok())
            .op(4, read("a"), read_ok(b"w"));
        check_cycles(&h.0).unwrap();
    }
}
