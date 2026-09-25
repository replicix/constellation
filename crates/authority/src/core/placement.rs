//! Plan 30 §M11 phase 2b: automatic placement (ADR-15 generalized).
//!
//! The root sees every op's origin node (the rid) and its directory. It
//! keeps a sliding window of ops per `(dir, node)`, rolled up to
//! ancestors at evaluation time, and every tick decides:
//!
//! - **Delegate** the *topmost* directory `D` such that one node `n`
//!   writes at least `placement_dominance_pct` of the ops under `D` in
//!   the window, the subtree saw at least `placement_min_ops` ops (a
//!   rate floor: a subtree whose forwards cost more than a recall would),
//!   `n` is not the root, is connected and write-eligible, `D` is not
//!   under (or over) a live delegation, and `D` is not cooling down. The
//!   walk goes from each dominated directory towards the root while the
//!   condition still holds, so one delegation covers a writer's whole
//!   working set rather than a swarm of leaves.
//! - **Recall** a placed generation when, for a whole dwell
//!   (`placement_dwell_ms`), the delegate's share of its subtree stayed
//!   below `placement_leave_pct` (hysteresis: enter at 70 %, leave at
//!   50 %) or the subtree's rate stayed below half the floor. A recall
//!   for a cross-subtree op is not a placement decision and does not
//!   reset the dwell. Manual delegations and designations are never
//!   recalled here.
//! - **Cool down** a recalled directory for `placement_cooldown_ms`
//!   before it can be placed again, so it never ping-pongs between two
//!   writers (Ceph's finding: delegate to the dominant writer, never
//!   split a subtree between writers — M12's hash-range split is the
//!   answer for a shared directory, and this module's per-directory
//!   counts by node are exactly what it needs to find one).
//!
//! Cost: one hash-map increment per op on the root, and per tick a walk
//! of the window's distinct directories to the root (the directory
//! depth each). Off (`placement: false`), nothing here runs.

use super::delegate::DelegKind;
use super::{Core, Timer};
use crate::action::Action;
use crate::ids::{Ms, NodeId};
use crate::replica::Replica;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Namespace;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// One tick's counts.
#[derive(Debug, Default)]
struct Bucket {
    ops: HashMap<(Ino, NodeId), u64>,
}

#[derive(Debug, Default)]
pub(crate) struct PlacementState {
    /// The window's older buckets, oldest first, and the current one.
    buckets: VecDeque<Bucket>,
    cur: Bucket,
    /// Directories recalled by placement: not placed again before this.
    cooldown: BTreeMap<Ino, Ms>,
    pub timer: Option<crate::ids::TimerId>,
    /// The last evaluation's top subtrees, `(dir, node, node_ops,
    /// subtree_ops)`, for `status` (and M12's split decision).
    pub top: Vec<(Ino, NodeId, u64, u64)>,
}

impl PlacementState {
    /// Count one op of `node` in `dir`.
    pub fn note(&mut self, dir: Ino, node: NodeId) {
        *self.cur.ops.entry((dir, node)).or_default() += 1;
    }

    /// Roll the current bucket into the window and drop what fell out.
    fn roll(&mut self, buckets: usize) {
        let cur = std::mem::take(&mut self.cur);
        self.buckets.push_back(cur);
        while self.buckets.len() > buckets.max(1) {
            self.buckets.pop_front();
        }
    }

    /// Per-directory counts over the window (the current bucket included).
    fn per_dir(&self) -> HashMap<Ino, HashMap<NodeId, u64>> {
        let mut out: HashMap<Ino, HashMap<NodeId, u64>> = HashMap::new();
        for b in self.buckets.iter().chain(std::iter::once(&self.cur)) {
            for ((dir, node), n) in &b.ops {
                *out.entry(*dir).or_default().entry(*node).or_default() += n;
            }
        }
        out
    }
}

/// The subtree counts: every directory's own ops plus its descendants',
/// by walking each counted directory up to the root.
fn subtree_counts(
    per_dir: &HashMap<Ino, HashMap<NodeId, u64>>,
    ns: &dyn Namespace,
) -> HashMap<Ino, HashMap<NodeId, u64>> {
    let mut out: HashMap<Ino, HashMap<NodeId, u64>> = HashMap::new();
    for (dir, by_node) in per_dir {
        let mut d = *dir;
        for _ in 0..4096 {
            let e = out.entry(d).or_default();
            for (node, n) in by_node {
                *e.entry(*node).or_default() += n;
            }
            match ns.parent_of(d) {
                Some(p) => d = p,
                None => break,
            }
        }
    }
    out
}

/// The dominant node of a subtree at `pct`, and the subtree's total.
fn dominant(by_node: &HashMap<NodeId, u64>, pct: u64) -> (Option<NodeId>, u64) {
    let total: u64 = by_node.values().sum();
    if total == 0 {
        return (None, 0);
    }
    let (node, n) = by_node
        .iter()
        .max_by_key(|(node, n)| (**n, std::cmp::Reverse(**node)))
        .map(|(node, n)| (*node, *n))
        .expect("non-empty");
    ((n * 100 >= total * pct).then_some(node), total)
}

impl Core {
    fn placement_tick_ms(&self) -> u64 {
        (self.cfg.placement_window_ms / 10).max(100)
    }

    /// Count `dirs` of one op from `node` (the root's own, a forwarded
    /// one, a delegate's streamed one).
    pub(crate) fn place_note(&mut self, node: NodeId, dirs: impl IntoIterator<Item = Ino>) {
        if !self.cfg.placement || !self.cfg.delegation {
            return;
        }
        for dir in dirs {
            self.pl.note(dir, node);
        }
    }

    /// The directories an op's keys fall in.
    pub(crate) fn dirs_of_keys(
        keys: &constellation_meta::TouchSet,
        replica: &dyn Replica,
    ) -> Vec<Ino> {
        let ns = replica.namespace();
        let mut dirs: Vec<Ino> = keys.dentries.iter().map(|(p, _)| *p).collect();
        for ino in &keys.inos {
            if let Some(p) = ns.primary_parent(*ino) {
                dirs.push(p);
            }
        }
        dirs.sort_unstable();
        dirs.dedup();
        dirs
    }

    /// Arm the placement tick while this node is the root.
    pub(crate) fn place_arm(&mut self, now: Ms, out: &mut Vec<Action>) {
        if !self.cfg.placement || !self.cfg.delegation || self.pl.timer.is_some() {
            return;
        }
        let t = self.set_timer(now.plus(self.placement_tick_ms()), Timer::Placement, out);
        self.pl.timer = Some(t);
    }

    /// The tick: roll the window and decide.
    pub(crate) fn on_placement_timer(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.pl.timer = None;
        let buckets = (self.cfg.placement_window_ms / self.placement_tick_ms()).max(1) as usize;
        self.pl.roll(buckets);
        if !self.root_usable(now) || self.dl.epoch_active || self.epoch.open {
            // Not the root any more: the counts are meaningless.
            self.pl.buckets.clear();
            return;
        }
        self.place_arm(now, out);
        self.place_evaluate(now, replica, out);
    }

    fn place_evaluate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.stats.place_evaluations += 1;
        let per_dir = self.pl.per_dir();
        if per_dir.is_empty() && self.dl.gens.values().all(|g| g.ended) {
            return;
        }
        let subtree = subtree_counts(&per_dir, replica.namespace());
        let min_ops = self.cfg.placement_min_ops;
        let leave = self.cfg.placement_leave_pct;
        let dominance = self.cfg.placement_dominance_pct;
        // Status: the busiest subtrees.
        let mut top: Vec<(Ino, NodeId, u64, u64)> = subtree
            .iter()
            .map(|(dir, by_node)| {
                let (node, n) = by_node
                    .iter()
                    .max_by_key(|(_, n)| **n)
                    .map(|(node, n)| (*node, *n))
                    .unwrap_or((0, 0));
                (*dir, node, n, by_node.values().sum())
            })
            .collect();
        top.sort_by_key(|(_, _, _, total)| std::cmp::Reverse(*total));
        top.truncate(16);
        self.pl.top = top;
        // 1. Recalls of placed generations (hysteresis over the dwell).
        let placed: Vec<(u64, Ino, NodeId)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.kind == DelegKind::Placed)
            .map(|(gen, g)| (*gen, g.dir, g.node))
            .collect();
        for (gen, dir, node) in placed {
            let (share_ok, total) = match subtree.get(&dir) {
                Some(by_node) => {
                    let total: u64 = by_node.values().sum();
                    let n = by_node.get(&node).copied().unwrap_or(0);
                    (total > 0 && n * 100 >= total * leave, total)
                }
                None => (false, 0),
            };
            let rate_ok = total * 2 >= min_ops;
            let g = self.dl.gens.get_mut(&gen).expect("present");
            if share_ok && rate_ok {
                g.below_since = None;
                continue;
            }
            let since = *g.below_since.get_or_insert(now);
            let dwell = self.cfg.placement_dwell_ms as i64;
            if now.since(since) >= dwell && now.since(g.granted) >= dwell {
                self.pl
                    .cooldown
                    .insert(dir, now.plus(self.cfg.placement_cooldown_ms));
                self.stats.place_recalled += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    dir,
                    delegate = node,
                    gen,
                    total,
                    "placement recalls a delegation (share or rate below the floor for the dwell)"
                );
                self.start_recall(now, gen, out);
            }
        }
        // 2. New placements: the topmost dominated directory per node.
        let table = replica.delegation_table();
        let ns = replica.namespace();
        let me = self.cfg.node_id;
        let mut chosen: BTreeMap<NodeId, (Ino, u64)> = BTreeMap::new();
        for (dir, by_node) in &subtree {
            // The root directory is the root sequencer's: never placed.
            if *dir == ROOT_INO {
                continue;
            }
            let (Some(node), total) = dominant(by_node, dominance) else {
                continue;
            };
            if total < min_ops || node == me {
                continue;
            }
            // Walk up while the parent is dominated by the same node.
            let mut top = *dir;
            let mut top_total = total;
            let mut d = *dir;
            for _ in 0..4096 {
                let Some(p) = ns.parent_of(d) else { break };
                if p == ROOT_INO {
                    break;
                }
                let Some(pb) = subtree.get(&p) else { break };
                match dominant(pb, dominance) {
                    (Some(n), t) if n == node && t >= min_ops => {
                        top = p;
                        top_total = t;
                        d = p;
                    }
                    _ => break,
                }
            }
            let e = chosen.entry(node).or_insert((top, top_total));
            if top_total > e.1 {
                *e = (top, top_total);
            }
        }
        for (node, (dir, total)) in chosen {
            if table.owner_of_dir(ns, dir).is_some() || table.iter().any(|d| d.node == node) {
                continue;
            }
            if self.pl.cooldown.get(&dir).is_some_and(|until| *until > now) {
                self.stats.place_skipped_cooldown += 1;
                continue;
            }
            let reachable =
                self.links.get(&node).is_some_and(|l| l.connected) && self.roster.contains(&node);
            if !reachable {
                self.stats.place_skipped_unreachable += 1;
                continue;
            }
            match self.delegate_dir(now, dir, node, DelegKind::Placed, replica, out) {
                Ok(gen) => {
                    tracing::info!(
                        node = me,
                        dir,
                        delegate = node,
                        gen,
                        total,
                        "placement delegated a dominated subtree"
                    );
                }
                Err(why) => {
                    tracing::debug!(
                        node = me,
                        dir,
                        delegate = node,
                        why,
                        "placement: not delegated"
                    )
                }
            }
        }
        self.pl.cooldown.retain(|_, until| *until > now);
    }
}
