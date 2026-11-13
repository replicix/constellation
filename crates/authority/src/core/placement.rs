//! Plan 30 §M11 phase 2b: automatic placement (ADR-15 generalized);
//! (Plan 46 D5 deleted the hash-range splits of ADR-38.)
//!
//! The root sees every op's origin node (the rid) and its directory. It
//! keeps a sliding window of ops per `(dir, node)`, rolled up to ancestors at evaluation time, and every tick
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
//! - **Grant a far writer a sticky subtree** (plan 46 M1.3 step 7,
//!   ADR-54). A writer with no peer within the backup RTT budget (the
//!   root included) cannot be given a placed grant (its answers would
//!   wait for no group); it gets a *sticky* one, whose answers are final,
//!   once its link has measured far (`placement_far_rtt_ms`, every
//!   sample of `placement_far_dwell_ms`). Between the budget and that
//!   test lies the dead band: the node stays forwarded. At a budget of 0
//!   no group can ever exist, so every connected eligible writer is far.
//!   [`Core::place_kind_for`] picks the kind.
//! - **Recall** a placed generation (never a manual, sticky or
//!   designated one) for two reasons only. (1) The long dwell: for a
//!   whole dwell (`placement_dwell_ms`, 5 minutes) the delegate's share of
//!   its subtree stayed below `placement_leave_pct` (hysteresis: enter at
//!   70 %, leave at 50 %) or the subtree's rate stayed below half the
//!   floor. A recall for a cross-subtree op is not a placement decision
//!   and does not reset the dwell. (2) Conversion: the node is measured
//!   far and has no recorded backup; the recall carries no cooldown, so
//!   the next round grants it a sticky subtree. A sticky grant ends only
//!   by its holder (step 5's release) or an operator.
//! - **Cool down** a recalled directory for `placement_cooldown_ms`
//!   before it can be placed again, so it never ping-pongs
//!   between two writers.
//!
//! Cost: one hash-map increment per op on the root, and per tick a walk
//! of the window's distinct directories to the root (the directory
//! depth each). Off (`placement: false`), nothing here runs.

use super::delegate::DelegKind;
use super::{Core, Timer};
use crate::action::Action;
use crate::event::PeerLink;
use crate::ids::{NodeId, Now, WaitMs};
use crate::replica::Replica;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::delegation::Namespace;
use constellation_meta::TouchSet;
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
    pub(crate) cooldown: BTreeMap<Ino, WaitMs>,
    pub timer: Option<crate::ids::TimerId>,
    /// The last evaluation's top subtrees, `(dir, node, node_ops,
    /// subtree_ops)`, for `status`.
    pub top: Vec<(Ino, NodeId, u64, u64)>,
    /// When a candidate outside the roster last had it re-read.
    roster_refreshed: Option<WaitMs>,
    /// Plan 46 M1.3 step 7: the links whose every sample since `since` was
    /// at least the far round trip (see [`Core::note_rtt_samples`]).
    pub(crate) far: BTreeMap<NodeId, FarTrack>,
    /// Plan 46 M1.3 step 7 (D-e): a sticky subtree its holder released on
    /// its own, `dir -> (holder, until)`: the window still counts the
    /// holder's earlier ops, so the holder is not granted it again before
    /// the window has turned over. Other nodes are not held back.
    pub(crate) released: BTreeMap<Ino, (NodeId, WaitMs)>,
    /// Plan 46 M1.3 step 8 (ADR-56): when this root last changed hands of a
    /// directory (granted it, or saw its holder's release row): the tenure
    /// a handover request waits out.
    pub(crate) hands: BTreeMap<Ino, WaitMs>,
    /// ADR-56: the handovers the root asked for or is granting, by the
    /// directory handed over (one request in flight per directory).
    pub(crate) handover: BTreeMap<Ino, HandoverReq>,
}

/// ADR-56: a sticky subtree the root asked its holder to hand over to `to`.
#[derive(Debug, Clone)]
pub(crate) struct HandoverReq {
    /// The node that dominates the subtree.
    pub to: NodeId,
    /// The holder's stream.
    pub gen: u64,
    /// When the root asked (it drops the request
    /// [`Core::handover_patience_ms`] after).
    pub asked: WaitMs,
    /// `Some(until)`: the holder's release row is in the log and the grant
    /// to `to` is made, retried every tick until `until` (a read grant under
    /// the subtree still being recalled, the quarantine).
    pub granting: Option<WaitMs>,
}

/// Since when a link has measured far.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FarTrack {
    pub since: WaitMs,
}

/// Why [`Core::place_kind_for`] grants nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaceSkip {
    /// No group within budget, and not (robustly) far: forwarded.
    Far,
    /// A designated stream or a pending designation, or a sticky stream
    /// with the node near: nothing joins it.
    NonStealable,
    /// The root's own subtrees (step 9).
    Root,
}

/// How often a candidate the roster does not list has it re-read.
const ROSTER_REFRESH_MS: i64 = 1_000;

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
            for ((dir, node), c) in &b.ops {
                *out.entry(*dir).or_default().entry(*node).or_default() += c;
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
            for (node, c) in by_node {
                *e.entry(*node).or_default() += c;
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
    pub(crate) fn placement_tick_ms(&self) -> u64 {
        (self.cfg.placement_window_ms / 10).max(100)
    }

    /// Count the `dirs` of one op from `node` (the root's own, a
    /// forwarded one, a delegate's streamed one).
    pub(crate) fn place_note(&mut self, node: NodeId, dirs: impl IntoIterator<Item = Ino>) {
        if !self.cfg.placement || !self.cfg.delegation {
            return;
        }
        for dir in dirs {
            self.pl.note(dir, node);
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
    pub(crate) fn place_arm(&mut self, now: Now, out: &mut Vec<Action>) {
        if !self.cfg.placement || !self.cfg.delegation || self.pl.timer.is_some() {
            return;
        }
        let t = self.set_timer_wait(
            now.wait.plus(self.placement_tick_ms()),
            Timer::Placement,
            out,
        );
        self.pl.timer = Some(t);
    }

    /// The tick: roll the window and decide.
    pub(crate) fn on_placement_timer(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.pl.timer = None;
        let buckets = (self.cfg.placement_window_ms / self.placement_tick_ms()).max(1) as usize;
        self.pl.roll(buckets);
        if !self.root_usable(now) || self.dl.epoch_active || self.epoch.open {
            // Not the root any more: the counts are meaningless.
            self.pl.buckets.clear();
            self.pl.far.clear();
            self.handover_drop_all(out);
            self.pl.hands.clear();
            return;
        }
        self.place_arm(now, out);
        self.handover_tick(now, replica, out);
        self.place_evaluate(now, replica, out);
    }

    /// Whether `node` may be given a subtree: connected and in the
    /// write-eligible roster. A connected node the roster does not list
    /// has it re-read (at most once a second): the roster is read at
    /// mount and every 5 s after, so a peer that registered after this
    /// node mounted and writes at once was skipped as unreachable for up
    /// to 5 s while its forwards streamed in — long enough for the root
    /// lease placement to hand it the lease first (harness
    /// `auto-placement`, 2 in 11: `place_skipped_unreachable` 9, the
    /// lease on b at the 5 s evaluation, `d1` never delegated).
    fn place_eligible(&mut self, now: Now, node: NodeId, out: &mut Vec<Action>) -> bool {
        if self.is_leaving(now, node) {
            return false;
        }
        if !self.roster.links().get(&node).is_some_and(|l| l.connected) {
            return false;
        }
        if self.roster.lists(node) {
            return true;
        }
        if self
            .pl
            .roster_refreshed
            .is_none_or(|at| now.wait.since(at) >= ROSTER_REFRESH_MS)
        {
            self.pl.roster_refreshed = Some(now.wait);
            tracing::debug!(
                node = self.cfg.node_id,
                candidate = node,
                "placement: a connected writer outside the roster; re-reading it"
            );
            out.push(Action::RefreshRoster);
        }
        false
    }

    /// Plan 46 M1.2 step 13 (no lone delegate): whether a stealable grant
    /// (placed or manual) to `node` has a group within budget. A delegate's
    /// group is its recorded backup and the root. The root counts when its
    /// link to `node` is within `backup_rtt_budget_ms` by its best round
    /// trip of the last placement tick ([`Self::link_rtt_ms`]: one slow
    /// sample, from load or a rejoin, does not refuse a healthy link; a
    /// link slow throughout the window does), an unmeasured or
    /// unknown link counts: what is not known to be far is not refused,
    /// and so does a backup the node's live stream already records. A budget
    /// of 0 admits no peer (no backup is ever selected, and no link is
    /// "within" it, a 0 ms measurement included), so it refuses every node
    /// whatever the measured link: the outcome never depends on whether a
    /// round trip rounded to 0 ms. A node
    /// with neither would answer its own clients on its disk alone, which
    /// no committed-or-final answer may do.
    pub(crate) fn delegate_group_in_budget(&self, node: NodeId) -> bool {
        if self.cfg.lone_delegate {
            return true;
        }
        let budget = self.cfg.backup_rtt_budget_ms;
        if budget == 0 {
            return false;
        }
        if self
            .roster
            .links()
            .get(&node)
            .is_none_or(|l| self.link_rtt_ms(l).is_none_or(|rtt| rtt <= budget))
        {
            return true;
        }
        self.dl
            .gens
            .values()
            .any(|g| !g.ended && g.node == node && !g.group.backups.is_empty())
    }

    /// Records the round trips of a directory snapshot (see
    /// `Core::rtt_recent`): the window is the placement tick, the time a
    /// far link must hold before a placed grant is recalled.
    pub(crate) fn note_rtt_samples(&mut self, at: WaitMs, links: &[PeerLink]) {
        let window = self.placement_tick_ms() as i64;
        // Plan 46 M1.3 step 7 (D-b): the far state of a link. Only while the
        // placement tick runs (this node is the root): a new root measures
        // again before its first sticky grant. Load only inflates a round
        // trip, so one low sample (or a break) is proof of nearness and
        // ends the state.
        if self.pl.timer.is_some() {
            let far_rtt = self
                .cfg
                .placement_far_rtt_ms
                .max(self.cfg.backup_rtt_budget_ms + 1);
            self.pl
                .far
                .retain(|n, _| links.iter().any(|l| l.node == *n && l.connected));
            for l in links {
                match (l.connected, l.rtt_ms) {
                    (true, Some(r)) if r >= far_rtt => {
                        self.pl.far.entry(l.node).or_insert(FarTrack { since: at });
                    }
                    _ => {
                        self.pl.far.remove(&l.node);
                    }
                }
            }
        } else {
            self.pl.far.clear();
        }
        self.rtt_recent
            .retain(|n, _| links.iter().any(|l| l.node == *n && l.connected));
        for l in links {
            let (true, Some(rtt)) = (l.connected, l.rtt_ms) else {
                self.rtt_recent.remove(&l.node);
                continue;
            };
            let q = self.rtt_recent.entry(l.node).or_default();
            q.push_back((at, rtt));
            while q.len() > 1 && q.front().is_some_and(|(t, _)| at.since(*t) > window) {
                q.pop_front();
            }
        }
    }

    /// The round trip to a peer as admission judges it: the best of the
    /// samples of the last placement tick (the latest alone when the
    /// window holds no other). `None`: not measured.
    pub(crate) fn link_rtt_ms(&self, l: &PeerLink) -> Option<u64> {
        let latest = l.rtt_ms?;
        Some(
            self.rtt_recent
                .get(&l.node)
                .and_then(|q| q.iter().map(|(_, r)| *r).min())
                .map_or(latest, |m| m.min(latest)),
        )
    }

    /// `node` holds a designated stream, or a designation to it is pending
    /// (`DelegationState::designating`). (A sticky stream is not one: the
    /// placement grows it for a far node.)
    fn place_node_designated(&self, node: NodeId) -> bool {
        self.dl.designating.values().any(|n| *n == node)
            || self
                .dl
                .gens
                .values()
                .any(|g| !g.ended && g.node == node && g.designated())
    }

    /// Whether `node`'s link has measured far for a whole far dwell (every
    /// sample at least `far_rtt`), or, at a budget of 0 (no group can ever
    /// exist, so no writer is "near"), whether it is connected at all.
    pub(crate) fn place_far(&self, now: Now, node: NodeId) -> bool {
        if self.cfg.backup_rtt_budget_ms == 0 {
            return self.roster.links().get(&node).is_some_and(|l| l.connected);
        }
        self.pl
            .far
            .get(&node)
            .is_some_and(|f| now.wait.since(f.since) >= self.cfg.placement_far_dwell_ms as i64)
    }

    /// Plan 46 M1.3 step 7 (D-a): the kind of grant the placement makes
    /// `node` for `dir`, or why none (counted). The caller checks that the
    /// node is eligible (connected, in the roster, not leaving:
    /// `place_eligible`) first; at a budget of 0 `place_far` holds for any
    /// connected node.
    ///
    /// - `Placed` when the node has a group within budget and its live
    ///   stream (if any) is stealable;
    /// - `Sticky` when it has none, its link is far, and nothing it holds
    ///   or awaits is a designation (a sticky stream of its own is grown);
    /// - otherwise nothing: the dead band, a stealable stream of a far
    ///   node that conversion has yet to recall, a designated stream, or a
    ///   sticky stream with the node near now.
    pub(crate) fn place_kind_for(
        &mut self,
        now: Now,
        node: NodeId,
        dir: Ino,
        replica: &dyn Replica,
    ) -> Result<DelegKind, PlaceSkip> {
        if node == self.cfg.node_id {
            return self.place_kind_for_root(now, dir, replica);
        }
        let ns = replica.namespace();
        let live: Option<DelegKind> = self
            .dl
            .gens
            .values()
            .find(|g| !g.ended && g.node == node)
            .map(|g| g.kind());
        // A designated stream, a designation pending for the node, or one
        // pending on `dir`, a directory over it or one under it.
        let designating_here = self.dl.designating.keys().any(|k| {
            super::delegate::is_under(ns, *k, dir) || super::delegate::is_under(ns, dir, *k)
        });
        if live == Some(DelegKind::Designated)
            || self.place_node_designated(node)
            || designating_here
        {
            self.stats.place_skipped_designated += 1;
            return Err(PlaceSkip::NonStealable);
        }
        if self.delegate_group_in_budget(node) {
            if live == Some(DelegKind::Sticky) {
                self.stats.place_skipped_designated += 1;
                return Err(PlaceSkip::NonStealable);
            }
            return Ok(DelegKind::Placed);
        }
        // No group within budget: a final (sticky) grant, or forwarded.
        let stealable_live = live.is_some_and(|k| !k.non_stealable());
        if !self.cfg.placement_sticky || !self.place_far(now, node) || stealable_live {
            self.stats.place_skipped_far += 1;
            return Err(PlaceSkip::Far);
        }
        Ok(DelegKind::Sticky)
    }

    /// Plan 46 M1.3 step 9 (ADR-55): the kind of grant the root itself gets
    /// on `dir`: sticky, only while its own ops commit by the archive (a
    /// write-eligible peer, no group), never on `/`, and never beside a
    /// designation (a stream never mixes kinds) or inside the quarantine
    /// of a release it made itself (step 7 D-e).
    fn place_kind_for_root(
        &mut self,
        now: Now,
        dir: Ino,
        replica: &dyn Replica,
    ) -> Result<DelegKind, PlaceSkip> {
        let me = self.cfg.node_id;
        let skip = |core: &mut Self| {
            core.stats.place_skipped_root_own += 1;
            Err(PlaceSkip::Root)
        };
        if !self.cfg.placement_root_sticky
            || !self.root_commits_by_archive()
            || (dir == ROOT_INO && !self.cfg.root_sticky_on_slash)
        {
            return skip(self);
        }
        let ns = replica.namespace();
        let designating_here = self.dl.designating.keys().any(|k| {
            super::delegate::is_under(ns, *k, dir) || super::delegate::is_under(ns, dir, *k)
        });
        let live = self
            .dl
            .gens
            .values()
            .find(|g| !g.ended && g.node == me)
            .map(|g| g.kind());
        if designating_here
            || self.place_node_designated(me)
            || live.is_some_and(|k| k != DelegKind::Sticky)
        {
            return skip(self);
        }
        // A stream under recall or release is not grown.
        if self.dl.gens.values().any(|g| {
            !g.ended
                && g.node == me
                && (g.recall != crate::core::delegate::RecallPhase::None || !g.releasing.is_empty())
        }) {
            return skip(self);
        }
        if self.pl.released.iter().any(|(d, (n, until))| {
            *n == me
                && *until > now.wait
                && (super::delegate::is_under(ns, *d, dir)
                    || super::delegate::is_under(ns, dir, *d))
        }) {
            self.stats.place_quarantined += 1;
            return skip(self);
        }
        Ok(DelegKind::Sticky)
    }

    fn place_evaluate(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
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
        let placed: Vec<(u64, Vec<Ino>, NodeId)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended
                    && (g.kind() == DelegKind::Placed
                        || (self.cfg.placement_recalls_sticky && g.kind() == DelegKind::Sticky))
            })
            .map(|(gen, g)| (*gen, g.dir_list(), g.node))
            .collect();
        for (gen, dirs, node) in placed {
            // Plan 46 M1.3 step 7 (D-c): conversion. A placed grant to a node
            // that has no group within budget (it was granted while its link
            // was unmeasured, or its backup is gone) and whose link has
            // measured far for a whole far dwell would answer its clients
            // at the WAN's round trip or not at all. It is recalled, with
            // no cooldown, so that the next round grants it a sticky
            // subtree. With sticky placement off the grant is kept.
            let g = self.dl.gens.get(&gen).expect("present");
            if g.kind() == DelegKind::Placed
                && self.cfg.placement_sticky
                && g.recall == crate::core::delegate::RecallPhase::None
                && !self.delegate_group_in_budget(node)
                && self.place_far(now, node)
            {
                self.stats.place_recalled += 1;
                self.stats.place_converted += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    dirs = ?dirs,
                    delegate = node,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    "placement recalls a placed delegation to convert it (its link is measured far)"
                );
                self.start_recall(now, gen, out);
                continue;
            }
            // (A recall under way is neither counted nor started again.)
            if self.dl.gens[&gen].recall != crate::core::delegate::RecallPhase::None {
                continue;
            }
            // A stream is recalled whole (step 9; a partial release is step
            // 10): it stays while any of its subtrees keeps the delegate's
            // share and rate.
            let mut keep = false;
            let mut total_all = 0u64;
            for dir in &dirs {
                let (share_ok, total) = match subtree.get(dir) {
                    Some(by_node) => {
                        let total: u64 = by_node.values().sum();
                        let n = by_node.get(&node).copied().unwrap_or(0);
                        (total > 0 && n * 100 >= total * leave, total)
                    }
                    None => (false, 0),
                };
                total_all += total;
                keep |= share_ok && total * 2 >= min_ops;
            }
            let (share_ok, rate_ok, total) = (keep, true, total_all);
            let g = self.dl.gens.get_mut(&gen).expect("present");
            if share_ok && rate_ok {
                g.below_since = None;
                continue;
            }
            let since = *g.below_since.get_or_insert(now.wait);
            let dwell = self.cfg.placement_dwell_ms as i64;
            if now.wait.since(since) >= dwell
                && now
                    .wait
                    .since(g.grown_at.map_or(g.granted, |t| t.max(g.granted)))
                    >= dwell
            {
                for dir in &dirs {
                    self.pl
                        .cooldown
                        .insert(*dir, now.wait.plus(self.cfg.placement_cooldown_ms));
                }
                self.stats.place_recalled += 1;
                tracing::info!(
                    node = self.cfg.node_id,
                    dirs = ?dirs,
                    delegate = node,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    total,
                    "placement recalls a delegation (share or rate below the floor for the dwell)"
                );
                self.start_recall(now, gen, out);
            }
        }
        // 1b. Plan 46 M1.3 step 8 (ADR-56): a node that dominates a subtree a
        // sticky stream holds is handed it, by a request to the holder.
        self.place_handover_requests(now, &subtree, replica, out);
        // 1c. Plan 46 M1.3 step 9 (ADR-55): the root's own sticky stream is
        // released by the root as its holder, when the root's share or rate
        // in it has failed for a whole dwell. The decision is the holder's
        // (`StickyReleaseCmd`, step 5's drain and release row): no recall,
        // no `deleg_sticky_recalled_*`.
        let own: Vec<(u64, Vec<Ino>)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended
                    && g.node == self.cfg.node_id
                    && g.kind() == DelegKind::Sticky
                    && !g.dirs.is_empty()
                    && g.recall == crate::core::delegate::RecallPhase::None
                    && g.releasing.is_empty()
            })
            .map(|(gen, g)| (*gen, g.dir_list()))
            .collect();
        for (gen, dirs) in own {
            let me = self.cfg.node_id;
            let mut keep = false;
            for dir in &dirs {
                if let Some(by_node) = subtree.get(dir) {
                    let total: u64 = by_node.values().sum();
                    let n = by_node.get(&me).copied().unwrap_or(0);
                    keep |= total > 0 && n * 100 >= total * leave && total * 2 >= min_ops;
                }
            }
            let g = self.dl.gens.get_mut(&gen).expect("present");
            if keep {
                g.below_since = None;
                continue;
            }
            let since = *g.below_since.get_or_insert(now.wait);
            let dwell = self.cfg.placement_dwell_ms as i64;
            if now.wait.since(since) >= dwell
                && now
                    .wait
                    .since(g.grown_at.map_or(g.granted, |t| t.max(g.granted)))
                    >= dwell
            {
                g.below_since = None;
                tracing::info!(
                    node = me,
                    dirs = ?dirs,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    "the root releases its own sticky subtrees (its share or rate was below the floor for the dwell)"
                );
                self.own_sticky_release(now, gen, dirs.into_iter().collect(), replica, out);
            }
        }
        // 2. New placements: the topmost dominated directory per node.
        let table = replica.delegation_table();
        let ns = replica.namespace();
        let me = self.cfg.node_id;
        let mut chosen: BTreeMap<NodeId, (Ino, u64)> = BTreeMap::new();
        for (dir, by_node) in &subtree {
            // The root directory is the root sequencer's: never placed (the
            // fault `root_sticky_on_slash` lets the walk reach it).
            if *dir == ROOT_INO && !self.cfg.root_sticky_on_slash {
                continue;
            }
            let (Some(node), total) = dominant(by_node, dominance) else {
                continue;
            };
            if total < min_ops {
                continue;
            }
            // Walk up while the parent is dominated by the same node.
            let mut top = *dir;
            let mut top_total = total;
            let mut d = *dir;
            for _ in 0..4096 {
                let Some(p) = ns.parent_of(d) else { break };
                if p == ROOT_INO && !self.cfg.root_sticky_on_slash {
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
            if let Some(cover) = table.covering(ns, dir) {
                // Plan 46 M1.3 step 7 (D-g): a subtree a live sticky stream
                // of another node covers is dominated by this one: the
                // handover's input (step 8, `place_handover_requests` asks
                // the holder); counted here.
                if self
                    .dl
                    .gens
                    .get(&cover.gen)
                    .is_some_and(|g| !g.ended && g.kind() == DelegKind::Sticky && g.node != node)
                {
                    self.stats.place_sticky_contended += 1;
                }
                continue;
            }
            if self
                .pl
                .cooldown
                .get(&dir)
                .is_some_and(|until| *until > now.wait)
            {
                self.stats.place_skipped_cooldown += 1;
                continue;
            }
            // Plan 46 M1.3 step 7 (D-e): the holder released this subtree on
            // its own a window ago or less; its earlier ops still count.
            if self.pl.released.iter().any(|(d, (n, until))| {
                *n == node
                    && *until > now.wait
                    && (super::delegate::is_under(ns, *d, dir)
                        || super::delegate::is_under(ns, dir, *d))
            }) {
                self.stats.place_quarantined += 1;
                continue;
            }
            // (The root has no link to itself to be eligible by.)
            if node != me && !self.place_eligible(now, node, out) {
                self.stats.place_skipped_unreachable += 1;
                continue;
            }
            // The kind: placed for a node with a group within budget; sticky
            // for a robustly far one (plan 46 M1.2 step 13 forwarded it: no
            // lone delegate); none for the dead band, a designation, or a
            // node that holds designations only (a stream never mixes grant
            // kinds): skipped, not asked again every round.
            let Ok(kind) = self.place_kind_for(now, node, dir, replica) else {
                continue;
            };
            match self.delegate_dir_from(
                super::delegate::GrantSource::Placement,
                now,
                dir,
                node,
                kind,
                replica,
                out,
            ) {
                Ok(gen) => {
                    self.pl.hands.insert(dir, now.wait);
                    if node == me {
                        tracing::info!(
                            node = me,
                            dir,
                            gen = %constellation_meta::delegation::GenLabel::of(gen),
                            total,
                            "placement granted the root a sticky subtree it dominates"
                        );
                        continue;
                    }
                    if kind == DelegKind::Sticky {
                        self.stats.place_sticky += 1;
                        tracing::info!(
                            node = me,
                            dir,
                            delegate = node,
                            gen = %constellation_meta::delegation::GenLabel::of(gen),
                            total,
                            "placement granted a sticky subtree to a far writer"
                        );
                        continue;
                    }
                    tracing::info!(
                        node = me,
                        dir,
                        delegate = node,
                        gen = %constellation_meta::delegation::GenLabel::of(gen),
                        total,
                        "placement delegated a dominated subtree"
                    );
                }
                Err(why) => {
                    if kind == DelegKind::Sticky {
                        self.stats.place_grant_waits += 1;
                    }
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
        self.pl.cooldown.retain(|_, until| *until > now.wait);
        self.pl.released.retain(|_, (_, until)| *until > now.wait);
    }
}

impl Core {
    /// ADR-56 (3.1): for each subtree a sticky stream holds that another node
    /// dominates over the placement window, ask the holder to hand it over.
    /// The root never ends the grant itself (H-I1): a request, one per
    /// directory in flight, after the grant's tenure (H-I3), only for a node
    /// the subtree can be given to (H-I4). A grant waiting for the release
    /// row of a node that no longer dominates is dropped.
    fn place_handover_requests(
        &mut self,
        now: Now,
        subtree: &HashMap<Ino, HashMap<NodeId, u64>>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let (min_ops, dominance) = (self.cfg.placement_min_ops, self.cfg.placement_dominance_pct);
        let me = self.cfg.node_id;
        // A grant the holder's row has freed, whose requester stopped
        // dominating: the ordinary proposals decide.
        let stale: Vec<Ino> = self
            .pl
            .handover
            .iter()
            .filter(|(dir, r)| {
                r.granting.is_some()
                    && !subtree.get(dir).is_some_and(|by| {
                        matches!(dominant(by, dominance), (Some(n), t) if n == r.to && t >= min_ops)
                    })
            })
            .map(|(d, _)| *d)
            .collect();
        for dir in stale {
            self.pl.handover.remove(&dir);
            self.stats.place_handover_grant_dropped += 1;
        }
        if self.lease.gate.is_some() {
            return;
        }
        let tenure = self.cfg.handover_min_tenure_ms as i64;
        let streams: Vec<(u64, NodeId, Vec<Ino>)> = self
            .dl
            .gens
            .iter()
            .filter(|(_, g)| {
                !g.ended
                    && g.kind() == DelegKind::Sticky
                    && g.recall == crate::core::delegate::RecallPhase::None
                    && !g.waiting_for_group
                    && !g.prev_blocked
            })
            .map(|(gen, g)| (*gen, g.node, g.dir_list()))
            .collect();
        for (gen, holder, dirs) in streams {
            for dir in dirs {
                let Some(by_node) = subtree.get(&dir) else {
                    continue;
                };
                if dir == ROOT_INO || self.pl.handover.contains_key(&dir) {
                    continue;
                }
                let (Some(to), total) = dominant(by_node, dominance) else {
                    continue;
                };
                if to == holder || total < min_ops {
                    continue;
                }
                let Some(g) = self.dl.gens.get(&gen) else {
                    break;
                };
                let since = g
                    .granted
                    .max(g.grown_at.unwrap_or(g.granted))
                    .max(self.pl.hands.get(&dir).copied().unwrap_or(g.granted));
                if now.wait.since(since) < tenure && !self.cfg.handover_ignores_tenure {
                    self.stats.place_handover_tenure += 1;
                    continue;
                }
                // The root itself is always a node a subtree can go back to.
                if to != me
                    && !(self.place_eligible(now, to, out)
                        && self.place_kind_for(now, to, dir, replica).is_ok())
                {
                    self.stats.place_handover_ineligible += 1;
                    continue;
                }
                self.request_handover(now, gen, dir, to, replica, out);
            }
        }
    }

    /// ADR-56: each tick, the requests the root has waited too long for are
    /// dropped, and a grant that could not be made when the holder's row came
    /// is tried again (until it is given up).
    fn handover_tick(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let gens: std::collections::BTreeSet<u64> =
            self.pl.handover.values().map(|r| r.gen).collect();
        for gen in gens {
            let live = self.dl.gens.get(&gen).is_some_and(|g| !g.ended);
            if !live {
                // The stream is over (its TTL lapsed, its state dropped): the
                // requests with it (a grant waiting for a row that ended the
                // stream is still made).
                let before = self.pl.handover.len();
                self.pl
                    .handover
                    .retain(|_, r| r.gen != gen || r.granting.is_some());
                self.stats.place_handover_expired += (before - self.pl.handover.len()) as u64;
            } else {
                self.handover_expire(now, gen, out);
            }
        }
        // A change of hands older than the tenure changes nothing any more
        // (`since` falls back to the grant's own time).
        let tenure = self.cfg.handover_min_tenure_ms as i64;
        self.pl.hands.retain(|_, at| now.wait.since(*at) < tenure);
        let granting: Vec<(Ino, WaitMs)> = self
            .pl
            .handover
            .iter()
            .filter_map(|(d, r)| r.granting.map(|u| (*d, u)))
            .collect();
        for (dir, until) in granting {
            if now.wait >= until {
                self.pl.handover.remove(&dir);
                self.stats.place_handover_grant_dropped += 1;
                continue;
            }
            self.handover_grant(now, dir, replica, out);
        }
    }

    /// ADR-56 (3.3): the holder's release row for `dir` is in this log (its
    /// ops the root held have run): note the change of hands, and grant
    /// `dir` to the node that was asked for.
    pub(crate) fn handover_released(
        &mut self,
        now: Now,
        dir: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let until = now.wait.plus(self.cfg.placement_window_ms);
        let Some(r) = self.pl.handover.get_mut(&dir) else {
            return;
        };
        r.granting = Some(until);
        self.pl.hands.insert(dir, now.wait);
        self.handover_grant(now, dir, replica, out);
    }

    /// Grant `dir` to the node its handover was asked for, by the placement's
    /// kind rule: a far node a sticky grant, one with a group a placed grant;
    /// the root itself its own sticky grant (ADR-55) or, when it cannot hold
    /// one, the keys of the root. Refused by `delegate_dir` (read grants
    /// under `dir` being recalled, the quarantine): kept for the next tick.
    pub(crate) fn handover_grant(
        &mut self,
        now: Now,
        dir: Ino,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(r) = self.pl.handover.get(&dir).cloned() else {
            return;
        };
        let me = self.cfg.node_id;
        let ns = replica.namespace();
        // Granted by the placement's own proposal meanwhile: done.
        if replica
            .delegation_table()
            .covering(ns, dir)
            .is_some_and(|c| c.node == r.to && c.gen != r.gen)
        {
            self.pl.handover.remove(&dir);
            return;
        }
        let kind = if r.to == me {
            match self.place_kind_for(now, me, dir, replica) {
                Ok(k) => k,
                Err(_) => {
                    // A skip that passes (the root's own stream is releasing
                    // or being recalled, the quarantine of a release of its
                    // own) is waited out until the window closes; any other
                    // leaves the subtree on the root's keys.
                    if r.granting.is_some_and(|until| now.wait < until)
                        && self.root_skip_is_transient(now, dir, replica)
                    {
                        return;
                    }
                    self.pl.handover.remove(&dir);
                    self.stats.place_handover_root_keys += 1;
                    return;
                }
            }
        } else {
            if !self.place_eligible(now, r.to, out) {
                return;
            }
            match self.place_kind_for(now, r.to, dir, replica) {
                Ok(k) => k,
                Err(_) => return,
            }
        };
        match self.delegate_dir_from(
            super::delegate::GrantSource::Handover,
            now,
            dir,
            r.to,
            kind,
            replica,
            out,
        ) {
            Ok(gen) => {
                self.pl.handover.remove(&dir);
                self.pl.hands.insert(dir, now.wait);
                self.stats.place_handover_granted += 1;
                if kind == DelegKind::Sticky && r.to != me {
                    self.stats.place_sticky += 1;
                }
                tracing::info!(
                    node = me,
                    dir,
                    delegate = r.to,
                    gen = %constellation_meta::delegation::GenLabel::of(gen),
                    ?kind,
                    "a handed-over subtree is granted to the node that dominates it"
                );
            }
            Err(why) => tracing::debug!(
                node = me,
                dir,
                delegate = r.to,
                why,
                "a handed-over subtree is not granted yet"
            ),
        }
    }
}

impl Core {
    /// Whether `place_kind_for_root` refused `dir` for a reason that passes:
    /// the root's own stream is releasing a subtree or being recalled, or a
    /// release the root made itself is inside its quarantine on an ancestor
    /// or descendant of `dir` (ADR-56: a handover to the root waits these out).
    fn root_skip_is_transient(&self, now: Now, dir: Ino, replica: &dyn Replica) -> bool {
        let me = self.cfg.node_id;
        let ns = replica.namespace();
        self.dl.gens.values().any(|g| {
            !g.ended
                && g.node == me
                && (g.recall != crate::core::delegate::RecallPhase::None || !g.releasing.is_empty())
        }) || self.pl.released.iter().any(|(d, (n, until))| {
            *n == me
                && *until > now.wait
                && (super::delegate::is_under(ns, *d, dir)
                    || super::delegate::is_under(ns, dir, *d))
        })
    }
}
