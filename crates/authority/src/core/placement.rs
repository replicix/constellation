//! Plan 30 §M11 phase 2b: automatic placement (ADR-15 generalized);
//! §M12: hash-range splits of hot shared directories (GIGA+).
//!
//! The root sees every op's origin node (the rid) and its directory. It
//! keeps a sliding window of ops per `(dir, node)` — with, per name, a
//! 16-bucket histogram of the name's hash (the range a split would put
//! it in) — rolled up to ancestors at evaluation time, and every tick
//! decides:
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
//! - **Split** (M12) a directory `D` that is hot (at least the floor of
//!   ops in `D` itself) but has *no* dominant node while several nodes
//!   each write at least `placement_split_pct` of it: its own names are
//!   split into `2^bits` hash ranges (`bits` the smallest that gives
//!   every qualifying node a range), each range delegated as a
//!   generation of its own to the qualifying node that wrote most of it
//!   (the root's own ranges stay the root's). A subdirectory of a split
//!   directory is not covered by the ranges: its contents resolve past
//!   `D` to `D`'s ancestors. A cross-range rename in `D` is a
//!   cross-subtree op (both ranges recalled, then re-delegated).
//! - **Recall** a placed generation when, for a whole dwell
//!   (`placement_dwell_ms`), the delegate's share of its subtree stayed
//!   below `placement_leave_pct` (hysteresis: enter at 70 %, leave at
//!   50 %) or the subtree's rate stayed below half the floor; a range
//!   generation when the directory's rate stayed below half the floor
//!   or its delegate's share of the directory below half the split
//!   share. A directory whose every range was recalled is *merged*. A
//!   recall for a cross-subtree op is not a placement decision and does
//!   not reset the dwell. Manual delegations and designations are never
//!   recalled here.
//! - **Cool down** a recalled directory for `placement_cooldown_ms`
//!   before it can be placed or split again, so it never ping-pongs
//!   between two writers.
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
use constellation_meta::delegation::{name_hash, Namespace, Range};
use constellation_meta::TouchSet;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// The name-hash histogram's resolution: the top four bits, sixteen
/// buckets — a split is at most sixteen ways.
const BUCKET_BITS: u8 = 4;
const BUCKETS: usize = 1 << BUCKET_BITS;

/// The counts of one `(dir, node)`: ops in the directory, and the ones
/// on a name by the name's hash bucket.
#[derive(Debug, Default, Clone)]
struct Counts {
    total: u64,
    buckets: [u64; BUCKETS],
}

impl Counts {
    fn add(&mut self, other: &Counts) {
        self.total += other.total;
        for (a, b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
    }

    /// The ops on names in `range`.
    fn in_range(&self, range: Range) -> u64 {
        if range.is_whole() {
            return self.total;
        }
        let shift = BUCKET_BITS.saturating_sub(range.bits);
        self.buckets
            .iter()
            .enumerate()
            .filter(|(b, _)| (*b as u32) >> shift == range.idx)
            .map(|(_, n)| *n)
            .sum()
    }
}

/// One tick's counts.
#[derive(Debug, Default)]
struct Bucket {
    ops: HashMap<(Ino, NodeId), Counts>,
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

/// The hash bucket of a name (its range at the histogram's resolution).
pub fn bucket_of(name: &str) -> u8 {
    (name_hash(name) >> (64 - u32::from(BUCKET_BITS))) as u8
}

impl PlacementState {
    /// Count one op of `node` in `dir` (`bucket`: the name's, for an op
    /// on a name).
    pub fn note(&mut self, dir: Ino, node: NodeId, bucket: Option<u8>) {
        let c = self.cur.ops.entry((dir, node)).or_default();
        c.total += 1;
        if let Some(b) = bucket {
            c.buckets[usize::from(b) % BUCKETS] += 1;
        }
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
    fn per_dir(&self) -> HashMap<Ino, HashMap<NodeId, Counts>> {
        let mut out: HashMap<Ino, HashMap<NodeId, Counts>> = HashMap::new();
        for b in self.buckets.iter().chain(std::iter::once(&self.cur)) {
            for ((dir, node), c) in &b.ops {
                out.entry(*dir)
                    .or_default()
                    .entry(*node)
                    .or_default()
                    .add(c);
            }
        }
        out
    }
}

/// The subtree counts: every directory's own ops plus its descendants',
/// by walking each counted directory up to the root.
fn subtree_counts(
    per_dir: &HashMap<Ino, HashMap<NodeId, Counts>>,
    ns: &dyn Namespace,
) -> HashMap<Ino, HashMap<NodeId, u64>> {
    let mut out: HashMap<Ino, HashMap<NodeId, u64>> = HashMap::new();
    for (dir, by_node) in per_dir {
        let mut d = *dir;
        for _ in 0..4096 {
            let e = out.entry(d).or_default();
            for (node, c) in by_node {
                *e.entry(*node).or_default() += c.total;
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

    /// Count the `dirs` of one op from `node` (the root's own, a
    /// forwarded one, a delegate's streamed one), each with the name's
    /// hash bucket when the op is on a name.
    pub(crate) fn place_note(
        &mut self,
        node: NodeId,
        dirs: impl IntoIterator<Item = (Ino, Option<u8>)>,
    ) {
        if !self.cfg.placement || !self.cfg.delegation {
            return;
        }
        for (dir, bucket) in dirs {
            self.pl.note(dir, node, bucket);
        }
    }

    /// Plan 30 §M12: ops this node's FUSE fast path executed as the
    /// root (outside the core): counted like the core's own executions,
    /// or the root's share of a directory is invisible to the placement
    /// and a directory the root writes most is given away.
    pub fn place_note_local(&mut self, keys: &[TouchSet], replica: &dyn Replica) {
        if !self.cfg.placement || !self.cfg.delegation || keys.is_empty() {
            return;
        }
        let me = self.cfg.node_id;
        for k in keys {
            let dirs = Core::dirs_of_keys(k, replica);
            self.place_note(me, dirs);
        }
    }

    /// The directories an op's keys fall in, with the hash bucket of
    /// each name touched.
    pub(crate) fn dirs_of_keys(
        keys: &constellation_meta::TouchSet,
        replica: &dyn Replica,
    ) -> Vec<(Ino, Option<u8>)> {
        let ns = replica.namespace();
        let mut dirs: Vec<(Ino, Option<u8>)> = keys
            .dentries
            .iter()
            .map(|(p, n)| (*p, Some(bucket_of(n))))
            .collect();
        for ino in &keys.inos {
            if let Some(p) = ns.primary_parent(*ino) {
                if !dirs.iter().any(|(d, _)| *d == p) {
                    dirs.push((p, None));
                }
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
        let split_pct = self.cfg.placement_split_pct;
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
        let placed: Vec<(u64, Ino, NodeId, Range)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| !g.ended && g.kind == DelegKind::Placed)
            .map(|(gen, g)| (*gen, g.dir, g.node, g.range))
            .collect();
        for (gen, dir, node, range) in placed {
            let (share_ok, rate_ok, total) = if range.is_whole() {
                let (share_ok, total) = match subtree.get(&dir) {
                    Some(by_node) => {
                        let total: u64 = by_node.values().sum();
                        let n = by_node.get(&node).copied().unwrap_or(0);
                        (total > 0 && n * 100 >= total * leave, total)
                    }
                    None => (false, 0),
                };
                (share_ok, total * 2 >= min_ops, total)
            } else {
                // M12: the directory's own rate, and the delegate's share
                // of the directory (its range is a fixed slice of it).
                let (share_ok, total) = match per_dir.get(&dir) {
                    Some(by_node) => {
                        let total: u64 = by_node.values().map(|c| c.total).sum();
                        let n = by_node.get(&node).map(|c| c.total).unwrap_or(0);
                        (total > 0 && n * 100 * 2 >= total * split_pct, total)
                    }
                    None => (false, 0),
                };
                (share_ok, total * 2 >= min_ops, total)
            };
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
                if range.is_whole() {
                    self.stats.place_recalled += 1;
                } else {
                    self.stats.place_range_recalls += 1;
                }
                tracing::info!(
                    node = self.cfg.node_id,
                    dir,
                    delegate = node,
                    gen,
                    range = range.label(),
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
            if table.covering(ns, dir).is_some()
                || table.is_split(dir)
                || table
                    .iter()
                    .any(|d| d.node == node && d.range.is_whole() && !d.designated)
            {
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
            match self.delegate_dir(
                now,
                dir,
                node,
                DelegKind::Placed,
                Range::WHOLE,
                replica,
                out,
            ) {
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
        // 3. M12: splits of hot directories no node dominates.
        if split_pct > 0 {
            let mut hot: Vec<(Ino, &HashMap<NodeId, Counts>)> = per_dir
                .iter()
                .map(|(dir, by_node)| (*dir, by_node))
                .filter(|(dir, by_node)| {
                    let total: u64 = by_node.values().map(|c| c.total).sum();
                    total >= min_ops
                        && by_node.values().all(|c| c.total * 100 < total * dominance)
                        && !table.is_split(*dir)
                        && table.covering(ns, *dir).is_none()
                        && !self.pl.cooldown.get(dir).is_some_and(|until| *until > now)
                })
                .collect();
            hot.sort_by_key(|(dir, _)| *dir);
            for (dir, by_node) in hot {
                let total: u64 = by_node.values().map(|c| c.total).sum();
                let mut qualifiers: Vec<NodeId> = by_node
                    .iter()
                    .filter(|(_, c)| c.total * 100 >= total * split_pct)
                    .map(|(n, _)| *n)
                    .collect();
                qualifiers.sort_unstable();
                if qualifiers.len() < 2 {
                    continue;
                }
                let delegates: Vec<NodeId> = qualifiers
                    .iter()
                    .copied()
                    .filter(|n| {
                        *n != me
                            && self.links.get(n).is_some_and(|l| l.connected)
                            && self.roster.contains(n)
                    })
                    .collect();
                if delegates.is_empty() {
                    self.stats.place_skipped_unreachable += 1;
                    continue;
                }
                let bits = (qualifiers.len() as u32)
                    .next_power_of_two()
                    .trailing_zeros() as u8;
                let bits = bits.clamp(1, BUCKET_BITS);
                let k = 1u32 << bits;
                let quota = k.div_ceil(qualifiers.len() as u32);
                let mut given: HashMap<NodeId, u32> = HashMap::new();
                let mut plan: Vec<(Range, NodeId)> = Vec::new();
                for idx in 0..k {
                    let range = Range { bits, idx };
                    // The qualifier that wrote most of this range, within
                    // its quota (ties: the lower node id) — and only one
                    // that *dominates* it, like a subtree's writer (M12
                    // round 2: names hashed uniformly across the writers
                    // give no range a local writer; splitting such a
                    // directory only adds a hop to every op — the
                    // delegate's backup acknowledgement, the root's own
                    // ops forwarded — for parallel sequencing an unloaded
                    // root does not need: `3node-p2pon-shared-create`
                    // lost 20–40 % on a LAN. A range nobody dominates
                    // stays the root's.)
                    let range_total: u64 = by_node.values().map(|c| c.in_range(range)).sum();
                    let winner = qualifiers
                        .iter()
                        .copied()
                        .filter(|n| given.get(n).copied().unwrap_or(0) < quota)
                        .max_by_key(|n| {
                            (
                                by_node.get(n).map(|c| c.in_range(range)).unwrap_or(0),
                                std::cmp::Reverse(*n),
                            )
                        })
                        .unwrap_or(me);
                    let dominates = range_total > 0
                        && by_node.get(&winner).map(|c| c.in_range(range)).unwrap_or(0) * 100
                            >= range_total * dominance;
                    if !dominates {
                        continue;
                    }
                    *given.entry(winner).or_default() += 1;
                    if winner != me && delegates.contains(&winner) {
                        plan.push((range, winner));
                    }
                }
                if plan.is_empty() {
                    continue;
                }
                let mut delegated = 0;
                for (range, node) in plan {
                    match self.delegate_dir(now, dir, node, DelegKind::Placed, range, replica, out)
                    {
                        Ok(gen) => {
                            delegated += 1;
                            tracing::info!(
                                node = me,
                                dir,
                                delegate = node,
                                gen,
                                range = range.label(),
                                total,
                                "placement split a hot directory: range delegated"
                            );
                        }
                        Err(why) => {
                            tracing::debug!(
                                node = me,
                                dir,
                                delegate = node,
                                why,
                                "placement: range not delegated"
                            )
                        }
                    }
                }
                if delegated > 0 {
                    self.stats.place_splits += 1;
                }
            }
        }
        self.pl.cooldown.retain(|_, until| *until > now);
    }
}
