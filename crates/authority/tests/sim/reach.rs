//! Plan 46 M0.3: the reachability history of a run, and the classifier of
//! unsettled `EIO`s that reads it.
//!
//! I4's exception, as the maintainer decided it (2026-11-01, "option A"): a
//! client may hear the unsettled `EIO` (in doubt, no sequencer answered its
//! settle) only if its requester was *isolated* — it could reach no
//! sequencer for the op (no root, no delegate of the op's keys, itself none
//! of them, and no way to acquire the lease) during its whole settle window,
//! the settle window being the span that ends at the `EIO` and is as long as
//! the core's `settle_window`. Every other unsettled `EIO` is a violation.
//!
//! The history is the sim's own network model, never a node's log: the bus
//! records each partition and each node's attach and detach, the nodes
//! record the spans in which their core handles nothing (`Pause`, a
//! suspend, a runtime lag, a stalled step), the bucket records the cuts of a
//! node's path to S3 and the lease bodies that land. From those, for any
//! span, [`Reach::classify`] decides whether a sequencer was reachable:
//!
//! - *a root*: the node the latest lease body names, when its own process
//!   holds that lease (a node restarted since holds nothing, whatever the
//!   body says; a deposed holder that has not noticed is not named), or a
//!   node that holds a continuation epoch; reachable when the requester's
//!   link to it is up and it is alive and not stopped;
//! - *a delegate*: the delegate of the op's keys (the caller reads the
//!   root's delegation table) while its grant is live (a grant that ran
//!   out, or a delegate restarted since, executes nothing), reachable the
//!   same way;
//! - *the requester itself*, when it is such a root or delegate;
//! - *the lease path*: the requester's path to S3 is up and the lease is
//!   acquirable (none was ever written, it was released, or it ran out).
//!
//! A reachability that lasts less than one round trip of the medium (twice
//! the slowest P2P delay; twice the slowest S3 request for the lease path)
//! is not one: no settle could have been sent, answered and heard in it.
//! Random message loss (`SimConfig::p2p_drop`) is not an isolation: the
//! link is up and the settle is retried.

use constellation_authority::NodeId;
use std::sync::Mutex;

/// One lease body that landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseSpan {
    pub at_ms: u64,
    pub holder: NodeId,
    /// When the body runs out (landing time plus the TTL), true ms.
    pub ends_ms: u64,
    pub released: bool,
    /// The lease lists backups (a `Backup` policy): a node that is none of
    /// them claims it only a claim grace past its end.
    pub backups: Vec<NodeId>,
}

/// What a node's core holds: the S3 lease, a continuation epoch, nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    No,
    Lease,
    Epoch,
}

/// A node's authority as its core published it after a step: what it holds
/// and the delegation grants it has, `(generation, ends_ms)` in true ms.
#[derive(Debug, Clone, PartialEq, Eq)]
struct View {
    at: u64,
    held: Held,
    grants: Vec<(u64, u64)>,
    /// What keeps its core from executing (`BUSY_*`), for the causes.
    busy: u8,
}

/// `View::busy` bits: the takeover gate is pending, a takeover floor is
/// running, the lease is lost, a job (an acquisition, a rebuild) runs.
pub const BUSY_GATE: u8 = 1;
pub const BUSY_FLOOR: u8 = 2;
pub const BUSY_LOST: u8 = 4;
pub const BUSY_JOB: u8 = 8;

fn busy_names(b: u8) -> String {
    let names: Vec<&str> = [
        (BUSY_GATE, "takeover gate"),
        (BUSY_FLOOR, "takeover floor"),
        (BUSY_LOST, "lease lost"),
        (BUSY_JOB, "job"),
    ]
    .into_iter()
    .filter(|(bit, _)| b & bit != 0)
    .map(|(_, n)| n)
    .collect();
    if names.is_empty() {
        "idle".into()
    } else {
        names.join("+")
    }
}

/// How a sequencer was reachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// The requester is the sequencer (the lease holder, or the delegate).
    Itself,
    /// The lease holder, over P2P.
    Root(NodeId),
    /// The delegate of the op's keys, over P2P.
    Delegate(NodeId),
    /// No holder to ask, but the lease could be taken (S3 reachable).
    LeasePath,
}

/// What the history says of one unsettled `EIO`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// No sequencer was reachable for long enough to settle, in the whole
    /// window (`why` names what cut the requester off).
    Isolated { why: String },
    /// A sequencer was reachable from `from_ms` to `to_ms`.
    Reachable {
        via: Via,
        from_ms: u64,
        to_ms: u64,
        /// What the sequencer's core was busy with at any time of the run
        /// (`idle`: nothing the history knows).
        busy: String,
    },
}

impl Verdict {
    pub fn isolated(&self) -> bool {
        matches!(self, Verdict::Isolated { .. })
    }
}

/// The question: could `requester` reach a sequencer in `[from_ms, to_ms]`?
#[derive(Debug, Clone, Copy)]
pub struct Question {
    pub requester: NodeId,
    pub from_ms: u64,
    pub to_ms: u64,
    /// The delegate of the op's keys and its generation, per the root's
    /// table (`None`: the keys are the root's, or cross subtrees and the
    /// root recalls).
    pub delegate: Option<(NodeId, u64)>,
}

#[derive(Default)]
struct Inner {
    /// `(at, a, b, cut)` with `a < b`: the pair's link goes down or up.
    links: Vec<(u64, NodeId, NodeId, bool)>,
    /// `(at, node, attached)`.
    alive: Vec<(u64, NodeId, bool)>,
    /// `(node, from, to)`: its core handles nothing.
    stopped: Vec<(NodeId, u64, u64)>,
    /// `(at, node, cut)`: its path to S3.
    s3: Vec<(u64, NodeId, bool)>,
    leases: Vec<LeaseSpan>,
    /// What each node's core said it was, step by step (`note_view`).
    views: std::collections::BTreeMap<NodeId, Vec<View>>,
    /// Round-trip floors (ms) of P2P and S3, the lease TTL, how long past
    /// its end a lease with backups is not a non-backup's to claim, and how
    /// much longer than a plain S3 request a lease read takes.
    min_p2p_ms: u64,
    min_s3_ms: u64,
    lease_ttl_ms: u64,
    claim_grace_ms: u64,
}

/// The run's reachability history (see the module doc).
#[derive(Default)]
pub struct Reach {
    inner: Mutex<Inner>,
    anchor: std::sync::OnceLock<tokio::time::Instant>,
}

impl Reach {
    /// The run's clock: true ms since the first reading (virtual under a
    /// paused runtime).
    pub fn now_ms(&self) -> u64 {
        let a = *self.anchor.get_or_init(tokio::time::Instant::now);
        tokio::time::Instant::now()
            .saturating_duration_since(a)
            .as_millis() as u64
    }

    /// The floors a reachability must last, and the lease TTL. Claiming a
    /// foreign lease takes a lease read and a CAS (`2 x s3_latency_hi_ms`
    /// each way, plus what the slow lease reads add: `lease_read_extra_ms`)
    /// after the claimant has watched the body unchanged for a TTL and, for
    /// a lease with backups it is not listed in, `claim_grace_ms` more.
    pub fn configure(
        &self,
        p2p_delay_hi_ms: u64,
        s3_latency_hi_ms: u64,
        lease_ttl_ms: u64,
        claim_grace_ms: u64,
        lease_read_extra_ms: u64,
    ) {
        let mut g = self.inner.lock().unwrap();
        g.min_p2p_ms = 2 * p2p_delay_hi_ms;
        g.min_s3_ms = 2 * s3_latency_hi_ms + lease_read_extra_ms;
        g.lease_ttl_ms = lease_ttl_ms;
        g.claim_grace_ms = claim_grace_ms;
    }

    pub fn partition(&self, a: NodeId, b: NodeId, cut: bool) {
        self.partition_at(self.now_ms(), a, b, cut);
    }

    pub fn partition_at(&self, at: u64, a: NodeId, b: NodeId, cut: bool) {
        let (a, b) = (a.min(b), a.max(b));
        self.inner.lock().unwrap().links.push((at, a, b, cut));
    }

    pub fn attached(&self, node: NodeId, up: bool) {
        self.attached_at(self.now_ms(), node, up);
    }

    pub fn attached_at(&self, at: u64, node: NodeId, up: bool) {
        self.inner.lock().unwrap().alive.push((at, node, up));
    }

    /// `node`'s core handles nothing from now for `for_ms`.
    pub fn stopped_for(&self, node: NodeId, for_ms: u64) {
        let now = self.now_ms();
        self.stopped_at(node, now, now + for_ms);
    }

    pub fn stopped_at(&self, node: NodeId, from: u64, to: u64) {
        self.inner.lock().unwrap().stopped.push((node, from, to));
    }

    pub fn s3_cut(&self, node: NodeId, cut: bool) {
        self.s3_cut_at(self.now_ms(), node, cut);
    }

    pub fn s3_cut_at(&self, at: u64, node: NodeId, cut: bool) {
        self.inner.lock().unwrap().s3.push((at, node, cut));
    }

    /// A lease body landed in the bucket.
    pub fn lease_landed(&self, holder: NodeId, released: bool, backups: Vec<NodeId>) {
        let at = self.now_ms();
        let ttl = self.inner.lock().unwrap().lease_ttl_ms;
        self.lease_landed_with(at, holder, at + ttl, released, backups);
    }

    pub fn lease_landed_at(&self, at: u64, holder: NodeId, ends: u64, released: bool) {
        self.lease_landed_with(at, holder, ends, released, Vec::new());
    }

    pub fn lease_landed_with(
        &self,
        at: u64,
        holder: NodeId,
        ends: u64,
        released: bool,
        backups: Vec<NodeId>,
    ) {
        self.inner.lock().unwrap().leases.push(LeaseSpan {
            at_ms: at,
            holder,
            ends_ms: ends,
            released,
            backups,
        });
    }

    /// `node`'s core published its authority after a step (kept when it
    /// changed, or when a grant's end moved by a quarter second).
    pub fn note_view(&self, node: NodeId, held: Held, grants: Vec<(u64, u64)>, busy: u8) {
        self.note_view_at(self.now_ms(), node, held, grants, busy);
    }

    pub fn note_view_at(
        &self,
        at: u64,
        node: NodeId,
        held: Held,
        grants: Vec<(u64, u64)>,
        busy: u8,
    ) {
        let mut g = self.inner.lock().unwrap();
        // (A view from before the node's last attach is another process's.)
        let since = attach_since(&g, at, node);
        let v = g.views.entry(node).or_default();
        if let Some(last) = v.last().filter(|l| l.at >= since) {
            let same = last.held == held
                && last.busy == busy
                && last.grants.len() == grants.len()
                && last
                    .grants
                    .iter()
                    .zip(&grants)
                    .all(|(a, b)| a.0 == b.0 && a.1.abs_diff(b.1) < 250);
            if same {
                return;
            }
        }
        v.push(View {
            at,
            held,
            grants,
            busy,
        });
    }

    /// The holder named by the latest lease body that had landed by `at`.
    pub fn holder_at(&self, at: u64) -> Option<NodeId> {
        let g = self.inner.lock().unwrap();
        lease_at(&g, at).map(|l| l.holder).filter(|h| *h != 0)
    }

    /// Classify `q` (see the module doc).
    pub fn classify(&self, q: Question) -> Verdict {
        let g = self.inner.lock().unwrap();
        classify(&g, q)
    }
}

fn lease_at(g: &Inner, at: u64) -> Option<LeaseSpan> {
    g.leases.iter().rev().find(|l| l.at_ms <= at).cloned()
}

fn attached(g: &Inner, at: u64, n: NodeId) -> bool {
    g.alive
        .iter()
        .rev()
        .find(|(t, node, _)| *node == n && *t <= at)
        .is_some_and(|(_, _, up)| *up)
}

/// When `n` was last attached, as of `at`.
fn attach_since(g: &Inner, at: u64, n: NodeId) -> u64 {
    g.alive
        .iter()
        .rev()
        .find(|(t, node, up)| *node == n && *t <= at && *up)
        .map_or(0, |(t, _, _)| *t)
}

/// The latest view `n` published by `at` since it was last attached.
fn view_at(g: &Inner, at: u64, n: NodeId) -> Option<&View> {
    let since = attach_since(g, at, n);
    g.views
        .get(&n)?
        .iter()
        .rev()
        .find(|v| v.at <= at)
        .filter(|v| v.at >= since)
}

fn held(g: &Inner, at: u64, n: NodeId) -> Held {
    view_at(g, at, n).map_or(Held::No, |v| v.held)
}

/// Whether `n` is a sequencer for everything not delegated at `at`: it
/// holds the lease the latest body names, or a continuation epoch.
fn root_in_force(g: &Inner, at: u64, n: NodeId) -> bool {
    match held(g, at, n) {
        Held::Epoch => true,
        Held::Lease => lease_at(g, at).is_some_and(|l| l.holder == n),
        Held::No => false,
    }
}

/// Whether `n` is the live delegate of generation `gen` at `at`.
fn delegate_in_force(g: &Inner, at: u64, n: NodeId, gen: u64) -> bool {
    view_at(g, at, n).is_some_and(|v| v.grants.iter().any(|(gn, ends)| *gn == gen && *ends > at))
}

fn running(g: &Inner, at: u64, n: NodeId) -> bool {
    attached(g, at, n)
        && !g
            .stopped
            .iter()
            .any(|(node, from, to)| *node == n && *from <= at && at < *to)
}

fn cut(g: &Inner, at: u64, a: NodeId, b: NodeId) -> bool {
    let (a, b) = (a.min(b), a.max(b));
    g.links
        .iter()
        .rev()
        .find(|(t, x, y, _)| *x == a && *y == b && *t <= at)
        .is_some_and(|(_, _, _, c)| *c)
}

fn peer_up(g: &Inner, at: u64, a: NodeId, b: NodeId) -> bool {
    a == b || (running(g, at, a) && running(g, at, b) && !cut(g, at, a, b))
}

fn s3_up(g: &Inner, at: u64, n: NodeId) -> bool {
    !g.s3
        .iter()
        .rev()
        .find(|(t, node, _)| *node == n && *t <= at)
        .is_some_and(|(_, _, c)| *c)
}

/// What `n`'s core was busy with at any time of `[from, to)`.
fn busy_over(g: &Inner, n: NodeId, from: u64, to: u64) -> u8 {
    let mut busy = view_at(g, from, n).map_or(0, |v| v.busy);
    for v in g.views.get(&n).into_iter().flatten() {
        if v.at > from && v.at < to && v.at >= attach_since(g, v.at, n) {
            busy |= v.busy;
        }
    }
    busy
}

/// The sequencer `q`'s requester could reach in the segment starting at
/// `at` (every state is constant until the next recorded change).
fn reach_at(g: &Inner, q: &Question, at: u64) -> Option<Via> {
    let me = q.requester;
    // The requester itself must be running to send anything.
    if !running(g, at, me) {
        return None;
    }
    let lease = lease_at(g, at);
    if root_in_force(g, at, me)
        || q.delegate
            .is_some_and(|(d, gen)| d == me && delegate_in_force(g, at, me, gen))
    {
        return Some(Via::Itself);
    }
    // The roots in force: the holder the body names and the epoch holders.
    let mut roots: Vec<NodeId> = g
        .views
        .keys()
        .copied()
        .filter(|n| held(g, at, *n) == Held::Epoch)
        .collect();
    if let Some(h) = lease.as_ref().map(|l| l.holder).filter(|h| *h != 0) {
        if root_in_force(g, at, h) && !roots.contains(&h) {
            roots.push(h);
        }
    }
    if let Some(r) = roots.into_iter().find(|r| peer_up(g, at, me, *r)) {
        return Some(Via::Root(r));
    }
    if let Some((d, gen)) = q.delegate {
        if delegate_in_force(g, at, d, gen) && peer_up(g, at, me, d) {
            return Some(Via::Delegate(d));
        }
    }
    let acquirable = match lease {
        None => true,
        Some(l) => {
            let grace = if l.backups.is_empty() || l.backups.contains(&me) {
                0
            } else {
                g.claim_grace_ms
            };
            l.holder == 0 || l.released || l.ends_ms + grace <= at
        }
    };
    (acquirable && s3_up(g, at, me)).then_some(Via::LeasePath)
}

fn classify(g: &Inner, q: Question) -> Verdict {
    // Every instant at which any state changes inside the window.
    let mut marks: Vec<u64> = vec![q.from_ms, q.to_ms];
    let inside = |t: u64| t > q.from_ms && t < q.to_ms;
    marks.extend(g.links.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(g.alive.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(g.s3.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(
        g.views
            .values()
            .flatten()
            .map(|v| v.at)
            .filter(|t| inside(*t)),
    );
    marks.extend(
        g.views
            .values()
            .flatten()
            .flat_map(|v| v.grants.iter().map(|(_, ends)| *ends))
            .filter(|t| inside(*t)),
    );
    marks.extend(
        g.leases
            .iter()
            .flat_map(|l| [l.at_ms, l.ends_ms, l.ends_ms + g.claim_grace_ms])
            .filter(|t| inside(*t)),
    );
    for (_, from, to) in &g.stopped {
        marks.extend([*from, *to].into_iter().filter(|t| inside(*t)));
    }
    marks.sort_unstable();
    marks.dedup();
    // The reachable runs: consecutive segments reaching a sequencer by the
    // same kind of way merge into one.
    let mut runs: Vec<(Via, u64, u64)> = Vec::new();
    for w in marks.windows(2) {
        let (s, e) = (w[0], w[1]);
        let Some(via) = reach_at(g, &q, s) else {
            continue;
        };
        match runs.last_mut() {
            Some((v, _, to)) if *to == s && *v == via => {
                *to = e;
            }
            _ => runs.push((via, s, e)),
        }
    }
    // An instantaneous window (or one with no segment) is judged at its
    // point.
    if marks.len() < 2 || q.from_ms == q.to_ms {
        if let Some(via) = reach_at(g, &q, q.to_ms) {
            runs.push((via, q.to_ms, q.to_ms));
        }
    }
    for (via, from, to) in runs {
        let floor = match via {
            Via::Itself => 0,
            Via::Root(_) | Via::Delegate(_) => g.min_p2p_ms,
            Via::LeasePath => g.min_s3_ms,
        };
        if to - from >= floor {
            let node = match via {
                Via::Itself => Some(q.requester),
                Via::Root(n) | Via::Delegate(n) => Some(n),
                Via::LeasePath => None,
            };
            let busy = node.map_or(0, |n| busy_over(g, n, from, to));
            return Verdict::Reachable {
                via,
                from_ms: from,
                to_ms: to,
                busy: busy_names(busy),
            };
        }
    }
    Verdict::Isolated {
        why: why_isolated(g, &q),
    }
}

/// What kept the requester from the sequencers at the window's end, in a
/// line (diagnostics only).
fn why_isolated(g: &Inner, q: &Question) -> String {
    let at = q.to_ms.saturating_sub(1);
    let me = q.requester;
    if !running(g, at, me) {
        return format!("requester {me} stopped or down");
    }
    let lease = lease_at(g, at);
    let state = |n: NodeId, role: &str| {
        let why = if !attached(g, at, n) {
            "down"
        } else if !running(g, at, n) {
            "stopped"
        } else if cut(g, at, me, n) {
            "cut"
        } else {
            "up"
        };
        format!("{role} {n} {why}")
    };
    let mut parts = Vec::new();
    match lease.as_ref().filter(|l| l.holder != 0) {
        Some(l) if root_in_force(g, at, l.holder) => parts.push(state(l.holder, "root")),
        Some(l) => parts.push(format!("root {} holds nothing", l.holder)),
        None => parts.push("no lease holder".into()),
    }
    if let Some((d, gen)) = q.delegate {
        if delegate_in_force(g, at, d, gen) {
            parts.push(state(d, "delegate"));
        } else {
            parts.push(format!("delegate {d} has no live grant"));
        }
    }
    if !s3_up(g, at, me) {
        parts.push("S3 cut".into());
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three nodes up from 0, a 10 ms P2P delay (floor 20 ms), a 40 ms S3
    /// (floor 80 ms), a 1 000 ms lease held by node 1 from t=0, renewed.
    fn world() -> Reach {
        let r = Reach::default();
        r.configure(10, 40, 1_000, 0, 0);
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(0, 1, 1_000, false);
        r.note_view_at(0, 1, Held::Lease, vec![], 0);
        r
    }

    fn q(requester: NodeId, delegate: Option<(NodeId, u64)>) -> Question {
        Question {
            requester,
            from_ms: 4_000,
            to_ms: 7_000,
            delegate,
        }
    }

    #[test]
    fn an_isolated_requester_is_isolated() {
        // Node 3 is cut from both others for the whole window, and from S3;
        // the lease node 1 holds is renewed throughout.
        let r = world();
        r.lease_landed_at(3_500, 1, 4_500, false);
        r.lease_landed_at(4_400, 1, 5_400, false);
        r.lease_landed_at(5_300, 1, 6_300, false);
        r.lease_landed_at(6_200, 1, 7_200, false);
        r.partition_at(3_000, 3, 1, true);
        r.partition_at(3_000, 3, 2, true);
        r.s3_cut_at(3_000, 3, true);
        let v = r.classify(q(3, None));
        assert!(v.isolated(), "{v:?}");
        // Its isolation is named.
        let Verdict::Isolated { why } = v else {
            unreachable!()
        };
        assert!(
            why.contains("root 1 cut") && why.contains("S3 cut"),
            "{why}"
        );
    }

    #[test]
    fn a_requester_that_reaches_the_root_is_not_isolated() {
        // Cut from node 2 but not from the root, node 1.
        let r = world();
        r.lease_landed_at(3_500, 1, 4_500, false);
        r.lease_landed_at(4_400, 1, 5_400, false);
        r.lease_landed_at(5_300, 1, 6_300, false);
        r.lease_landed_at(6_200, 1, 7_200, false);
        r.partition_at(3_000, 3, 2, true);
        let v = r.classify(q(3, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // And a delegate it reaches counts when the root is cut.
        r.partition_at(3_000, 3, 1, true);
        r.partition_at(3_000, 3, 2, false);
        // Node 2 is the delegate of generation 7 while its grant lasts.
        r.note_view_at(3_000, 2, Held::No, vec![(7, 9_000)], 0);
        let v = r.classify(q(3, Some((2, 7))));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Delegate(2),
                    ..
                }
            ),
            "{v:?}"
        );
        // A grant that ran out at 4 010 leaves 10 ms: under a round trip.
        let r2 = world();
        r2.lease_landed_at(3_500, 1, 9_000, false);
        r2.partition_at(3_000, 3, 1, true);
        r2.note_view_at(3_000, 2, Held::No, vec![(7, 4_010)], 0);
        let v = r2.classify(q(3, Some((2, 7))));
        assert!(v.isolated(), "{v:?}");
        // The delegate of another generation is none.
        assert!(r.classify(q(3, Some((2, 8)))).isolated());
        // The same node as the delegate of other keys is no sequencer here.
        assert!(r.classify(q(3, None)).isolated());
    }

    #[test]
    fn a_pause_of_the_sequencer_covering_the_window_isolates() {
        // The root, node 1, is paused across the whole window (and node 2
        // is no sequencer of the op): the requester's link is up and
        // nobody answers. A pause that ends inside the window with room
        // for a round trip does not isolate; one that leaves less does.
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 7_100);
        assert!(r.classify(q(3, None)).isolated());
        // (A pause that outlasts the lease frees the lease path: the
        // requester could take the lease over, so it is not isolated.)
        let r = world();
        r.lease_landed_at(3_500, 1, 5_000, false);
        r.stopped_at(1, 3_900, 7_100);
        assert!(matches!(
            r.classify(q(3, None)),
            Verdict::Reachable {
                via: Via::LeasePath,
                from_ms: 5_000,
                ..
            }
        ));
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 6_000);
        let v = r.classify(q(3, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 6_000,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 6_990);
        assert!(
            r.classify(q(3, None)).isolated(),
            "10 ms is under a round trip"
        );
    }

    #[test]
    fn a_dead_root_is_reachable_through_the_lease_only_once_it_runs_out() {
        let r = world();
        // Node 1 dies at 3 000; its last body, landed at 2 500, ends at
        // 3 500.
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        // The window [4 000, 7 000]: the body ran out at 3 500, so the
        // lease is acquirable throughout, and S3 is reachable.
        let v = r.classify(q(3, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    ..
                }
            ),
            "{v:?}"
        );
        // With S3 cut, nothing is reachable.
        r.s3_cut_at(3_100, 3, true);
        assert!(r.classify(q(3, None)).isolated());
        // A lease that has not run out blocks the lease path.
        let r = world();
        r.lease_landed_at(3_900, 1, 8_000, false);
        r.attached_at(3_950, 1, false);
        let v = r.classify(q(3, None));
        assert!(
            v.isolated(),
            "{v:?}: the lease runs to 8 000, after the window"
        );
    }

    #[test]
    fn a_requester_stopped_for_the_window_could_send_nothing() {
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(3, 3_000, 7_000);
        let v = r.classify(q(3, None));
        let Verdict::Isolated { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("requester 3 stopped"), "{why}");
    }

    #[test]
    fn the_sequencer_itself_is_never_isolated() {
        // A holder that heard EIO for its own op: it is the sequencer.
        let r = world();
        r.lease_landed_at(3_500, 3, 8_000, false);
        r.note_view_at(3_500, 3, Held::Lease, vec![], 0);
        r.partition_at(3_000, 3, 1, true);
        r.partition_at(3_000, 3, 2, true);
        let v = r.classify(q(3, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Itself,
                    ..
                }
            ),
            "{v:?}"
        );
    }

    #[test]
    fn a_holder_restarted_since_its_lease_holds_nothing() {
        // The body still names node 1 until 8 000, but node 1 restarted at
        // 3 100: nobody in force, and the lease path opens only when the
        // body runs out.
        let r = world();
        r.lease_landed_at(2_900, 1, 8_000, false);
        r.attached_at(3_000, 1, false);
        r.attached_at(3_100, 1, true);
        let v = r.classify(q(3, None));
        let Verdict::Isolated { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("root 1 holds nothing"), "{why}");
        // The restarted node, asked for its own op, is no sequencer either.
        let v = r.classify(q(1, None));
        assert!(v.isolated(), "{v:?}");
        // Once it holds a lease again (a new body, a new view), it is.
        r.lease_landed_at(5_000, 1, 11_000, false);
        r.note_view_at(5_000, 1, Held::Lease, vec![], 0);
        let v = r.classify(q(1, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Itself,
                    from_ms: 5_000,
                    ..
                }
            ),
            "{v:?}"
        );
    }

    #[test]
    fn a_reachable_sequencer_in_its_takeover_gate_is_named() {
        // The root holds the lease and answers nobody: its core is in the
        // takeover gate. It is reachable all the same (a violation), and
        // the verdict says what kept it from settling.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(3_500, 1, Held::Lease, vec![], BUSY_GATE | BUSY_FLOOR);
        let v = r.classify(q(3, None));
        let Verdict::Reachable { via, busy, .. } = v else {
            panic!("{v:?}")
        };
        assert_eq!(via, Via::Root(1));
        assert_eq!(busy, "takeover gate+takeover floor");
    }

    #[test]
    fn the_busy_flags_are_those_of_any_moment_of_the_run() {
        // The gate ends at 6 900; the run is [4 000, 7 000].
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(3_500, 1, Held::Lease, vec![], BUSY_GATE);
        r.note_view_at(6_900, 1, Held::Lease, vec![], 0);
        let v = r.classify(q(3, None));
        let Verdict::Reachable { busy, .. } = v else {
            panic!("{v:?}")
        };
        assert_eq!(busy, "takeover gate");
    }

    #[test]
    fn a_lease_with_backups_is_a_non_backups_to_claim_only_a_grace_past_its_end() {
        // The body ends at 5 000; node 3 is not among its backups, so it
        // may claim from 5 000 + 1 000 (the grace); node 2, a backup, from
        // 5 000.
        let r = Reach::default();
        r.configure(10, 40, 1_000, 1_000, 0);
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_with(3_000, 1, 5_000, false, vec![2]);
        r.note_view_at(3_000, 1, Held::Lease, vec![], 0);
        r.attached_at(3_500, 1, false);
        let v = r.classify(q(3, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    from_ms: 6_000,
                    ..
                }
            ),
            "{v:?}"
        );
        let v = r.classify(q(2, None));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    from_ms: 5_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // And what the slow lease reads add is a floor on the run.
        let r = Reach::default();
        r.configure(10, 40, 1_000, 0, 2_000);
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(3_000, 1, 5_000, false);
        r.attached_at(3_500, 1, false);
        // The lease is open from 5 000: 2 000 ms of the window, under the
        // 80 + 2 000 ms a claim takes.
        let v = r.classify(q(3, None));
        assert!(v.isolated(), "{v:?}");
    }
}
