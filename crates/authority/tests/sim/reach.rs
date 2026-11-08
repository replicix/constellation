//! Plan 46 M0.3: the reachability history of a run, and the classifier of
//! unsettled `EIO`s that reads it.
//!
//! I4's exception, as the maintainer decided it (2026-11-01, "option A"): a
//! client may hear the unsettled `EIO` (in doubt, no sequencer answered its
//! settle) only if its requester was *isolated* — it could reach no
//! sequencer for the op (no root, no delegate of the op's keys, itself none
//! of them, and no way to acquire the lease) during its whole settle window,
//! the settle window being the span that ends at the `EIO` and is as long as
//! the core's `settle_window`. Every other unsettled `EIO` is a violation
//! (the labels that are allowed are listed below).
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
//!   acquirable (none was ever written, it was released, or it ran out), or
//!   it is the requester's own, held when it restarted (re-adopted at once).
//!
//! Who owns the op's keys is decided at each instant of the window from the
//! generation history (the lease holder's delegation table as it changed:
//! install, recall, end), not from the table at the `EIO`. While a live
//! generation owns the keys the delegate is the only sequencer (the root
//! redirects or parks the op while the grant lives, so it does not count),
//! and every delegate the keys had during the window counts while its grant
//! lived.
//!
//! A generation that is *not live* does not leave the op without a sequencer:
//! its grant ran out, it was never installed, or the keys span generations
//! ([`Owner::Blocked`], the root recalls first). The root ends such a
//! generation (it outwaits the delegate's window, then recalls and records the
//! end), so from the *allowance* on (`gen_end_due`: the root's window for the
//! generation plus one recall round trip, counted from the constants of the
//! core's configuration) the root is the op's sequencer, if it is reachable,
//! can commit, and can reach a holder of the generation's tail (the delegate,
//! or a member of its group). That the generation outlives the allowance is the
//! product's lack of progress, never the requester's isolation. Keys across
//! generations whose grants live are recalled, not outwaited, while the root
//! reaches their delegates: the allowance is then one recall round trip plus
//! the recall's resend back-off (2 s) from the stretch in which it does.
//!
//! A sequencer counts only if it can commit: a root with S3 cut and no
//! backup reachable cannot commit a refusal.
//!
//! The lease path, as the product implements the claim: the claimant reads the
//! lease (the sim bucket records every lease GET), waits one TTL (and a
//! backup grace when the lease lists backups it is not among) of its own
//! observation of the body, reads it again (the claim is made on a read
//! handled after the TTL; that read's own transit, from the history, is part
//! of the claim), asks its peers for the promise check, writes the CAS, and,
//! for a lease with a group, fetches the group's tail from a running member it
//! can reach at any time up to the window's end (itself only while it holds a
//! tail of an epoch the claim asks for in place: rows of it in its journal, a
//! backup role of it, a member copy of it). If that claim cannot finish inside the window
//! by design and no root is in force, the requester had no sequencer *in time*.
//! Once the claim is done the claimant is the root, and ends the generations
//! it has to end for the op as any root does: only with a holder of each one's
//! tail it reaches (else `generation_tail_unreachable`).
//!
//! The allowed verdicts, each counted apart (`Report::unsettled_*`):
//!
//! - `isolated` ([`Verdict::Isolated`]): the requester could reach no
//!   sequencer in the window (a cut, a stopped node, a root that cannot
//!   commit, a live generation whose delegate it cannot reach);
//! - `no_sequencer_in_time` ([`Verdict::NoSequencerInTime`]): a sequencer
//!   exists only by design later than the window's end: the lease claim
//!   cannot finish inside it (a TTL of its own observation, the group's tail),
//!   or the root's outwait of a generation that does not live (its window and
//!   the recall) ends after it, or the lease body still names a holder that
//!   holds nothing (sealed by a claimant, deposed, restarted, dead) and is not
//!   claimable until it runs out;
//! - `generation_tail_unreachable` ([`Verdict::GenerationTailUnreachable`]):
//!   the root is reachable and the generation's allowance is over, but the
//!   generation has a group and no holder of its tail (the delegate, a member)
//!   can be reached from the root, so the root cannot end it (the strict wait
//!   for the group's tail).
//!
//! Every other unsettled `EIO` is a violation.
//!
//! A reachability that lasts less than the settle's retry interval plus one
//! round trip of the medium (the slowest P2P delay twice; twice the slowest S3
//! request for a lease read) is not one: no settle could have been sent,
//! answered and heard in it. Random message loss (`SimConfig::p2p_drop`) is
//! not an isolation: the link is up and the settle is retried.

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
    /// The members of the tail this tenure inherited and has not yet
    /// shipped (`Lease::inherited_tail`): a claimant fetches it from them too.
    pub inherited: Vec<NodeId>,
    /// The body's epoch and that of the tail it inherited (0: not known, as
    /// in the unit tests: every member is then taken to hold the tail).
    pub epoch: u64,
    pub inherited_epoch: u64,
}

/// What a node's core holds: the S3 lease, a continuation epoch, nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Held {
    No,
    Lease,
    Epoch,
}

/// A node's authority as its core published it after a step: what it holds
/// and the delegation grants it has, `(generation, ends_ms)` in true ms (0:
/// installed, never renewed).
#[derive(Debug, Clone, PartialEq, Eq)]
struct View {
    at: u64,
    held: Held,
    grants: Vec<(u64, u64)>,
    /// The group members this node knows for generations: the backups of the
    /// generations it is the delegate of and those its root table names,
    /// `(generation, member)`.
    members: Vec<(u64, NodeId)>,
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
    /// The only way to a sequencer was to claim the lease, and the claim, as
    /// the product implements it, could not finish inside the window (`why`
    /// says what it waited for).
    NoSequencerInTime { why: String },
    /// The root was reachable and could commit, and the allowance for ending
    /// the op's generation was over, but the generation has a group and no
    /// holder of its tail (the delegate, a member) could be reached from the
    /// root: it cannot end it (`why` names what it waited for).
    GenerationTailUnreachable { why: String },
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

    pub fn no_sequencer_in_time(&self) -> bool {
        matches!(self, Verdict::NoSequencerInTime { .. })
    }

    pub fn generation_tail_unreachable(&self) -> bool {
        matches!(self, Verdict::GenerationTailUnreachable { .. })
    }

    /// Allowed: the requester had no sequencer, or none in time, or the root
    /// could not end the op's generation for want of its tail.
    pub fn allowed(&self) -> bool {
        !matches!(self, Verdict::Reachable { .. })
    }
}

/// Who executes the op at an instant of the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    /// The root (no live generation owns the op's keys).
    Root,
    /// The delegate of generation `gen` (while its grant lives it is the
    /// only sequencer; once it does not, the root ends the generation).
    Delegate { node: NodeId, gen: u64 },
    /// Keys under more than one owner: the root recalls the `(delegate,
    /// generation)` pairs first; nobody executes the op until that is done.
    Blocked(Vec<(NodeId, u64)>),
}

/// The question: could `requester` reach a sequencer in `[from_ms, to_ms]`?
#[derive(Debug, Clone)]
pub struct Question {
    pub requester: NodeId,
    pub from_ms: u64,
    pub to_ms: u64,
    /// Who owns the op's keys from each instant on (sorted by time; the first
    /// entry covers the window's start).
    pub owners: Vec<(u64, Owner)>,
    /// When each generation first showed in a delegation table (ms): the
    /// start of the root's window of a generation its delegate never
    /// installed. One that is absent is taken to be born at the window's start.
    pub born: std::collections::BTreeMap<u64, u64>,
}

impl Question {
    /// A question whose keys are the root's all along.
    pub fn new(requester: NodeId, from_ms: u64, to_ms: u64) -> Self {
        Question {
            requester,
            from_ms,
            to_ms,
            owners: vec![(0, Owner::Root)],
            born: Default::default(),
        }
    }

    /// Generation `gen` first showed in the root's table at `at`.
    pub fn born_at(mut self, gen: u64, at: u64) -> Self {
        self.born.insert(gen, at);
        self
    }

    /// The keys are generation `gen`'s, delegated to `node`, all along.
    pub fn delegated(mut self, node: NodeId, gen: u64) -> Self {
        self.owners = vec![(0, Owner::Delegate { node, gen })];
        self
    }

    /// The owner at `at` and when that stretch of ownership began (clamped
    /// to the window's start).
    fn owner_at(&self, at: u64) -> (u64, Owner) {
        self.owners
            .iter()
            .rev()
            .find(|(t, _)| *t <= at)
            .or(self.owners.first())
            .map_or((self.from_ms, Owner::Root), |(t, o)| {
                ((*t).max(self.from_ms), o.clone())
            })
    }
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
    /// `(at, epochs)` per node, when they changed: the epochs whose tail the
    /// node could hand a claimant in place (`note_tails`).
    tails: std::collections::BTreeMap<NodeId, Vec<(u64, Vec<u64>)>>,
    /// Round-trip floors (ms) of P2P and S3, the lease TTL, how long past
    /// its end a lease with backups is not a non-backup's to claim, and how
    /// much longer than a plain S3 request a lease read takes.
    min_p2p_ms: u64,
    min_s3_ms: u64,
    lease_ttl_ms: u64,
    claim_grace_ms: u64,
    /// What a claim does after the TTL wait: the promise check (a P2P round
    /// trip) and the CAS (an S3 round trip).
    claim_tail_ms: u64,
    /// `(node, sent_ms, served_ms, observed_ms)`: every lease GET the bucket
    /// served (sent: when the node's handle issued it).
    lease_reads: Vec<(NodeId, u64, u64, u64)>,
    /// The fastest lease read the configuration allows (`s3_latency_lo_ms`,
    /// all of which lies before the bucket serves it, plus the slow reads'
    /// least delays): what the claim's fresh read costs when the history has
    /// none.
    min_lease_read_ms: u64,
    /// `(at, node)`: a node restarted with its journal (its rows survived).
    kept: Vec<(u64, NodeId)>,
    /// The slowest P2P one-way delay.
    p2p_hi_ms: u64,
    /// The lease's and grants' safety margin (`expiry_margin_ms`).
    margin_ms: u64,
    /// How long the root outwaits a grant it cannot see renewed: the grant's
    /// ttl plus the margin (`Core::reclaim_horizon_ms`).
    horizon_ms: u64,
    /// What a root adds to the window of a generation it inherits after a fast
    /// takeover (`DelegationState::inherit_slack_ms`: `backup_takeover_ms`;
    /// 0 after any other acquisition, see [`tenure_of`]).
    inherit_slack_ms: u64,
    /// One recall round trip: the root asks the generation's group for its
    /// tail (P2P there and back) and records the generation's end (an S3 put:
    /// the slowest request there and back).
    recall_ms: u64,
}

/// The constants of the sim's configuration the classifier derives its
/// floors and allowances from.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReachConfig {
    /// The slowest P2P one-way delay (`SimConfig::p2p_delay.1`).
    pub p2p_delay_hi_ms: u64,
    /// The slowest S3 request (`SimConfig::s3_latency.1`).
    pub s3_latency_hi_ms: u64,
    /// The lease's ttl (`Config::ttl_ms`).
    pub lease_ttl_ms: u64,
    /// How long past its end a lease with backups is not a non-backup's to
    /// claim (twice `Config::backup_takeover_ms`).
    pub claim_grace_ms: u64,
    /// What the slow lease reads add to an S3 request.
    pub lease_read_extra_ms: u64,
    /// The fastest S3 request (`SimConfig::s3_latency.0`).
    pub s3_latency_lo_ms: u64,
    /// The least the slow lease reads add to an S3 request.
    pub lease_read_extra_lo_ms: u64,
    /// The settle's retry interval (`Config::acquire_retry_min_ms`).
    pub settle_retry_ms: u64,
    /// A delegation grant's ttl (`Config::delegation_ttl_ms`).
    pub delegation_ttl_ms: u64,
    /// `Config::expiry_margin_ms`.
    pub expiry_margin_ms: u64,
    /// `Config::backup_takeover_ms`: a root's slack on the windows of the
    /// generations it inherits after a fast takeover.
    pub backup_takeover_ms: u64,
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

    /// The floors a reachability must last, the lease TTL and the allowances.
    /// A P2P reachability must last the settle's retry interval plus a round
    /// trip; claiming a foreign lease takes a lease read (`2 x s3_latency_hi_ms`
    /// plus what the slow lease reads add: `lease_read_extra_ms`), a TTL of the
    /// claimant's own observation and, for a lease with backups it is not
    /// listed in, `claim_grace_ms` more, then the promise check and the CAS.
    ///
    /// The root ends a generation that does not live (its grant ran out, it
    /// was never installed) by outwaiting its window and recalling it: the
    /// delegate honours a grant until `sent + ttl - margin`, the root until
    /// `receipt + ttl + margin` (`Core::reclaim_horizon_ms`), a one-way P2P
    /// delay after the request was sent; a generation never installed has the
    /// window of the root's own grant, `born + ttl + margin`; a root starts every
    /// window it inherits (a generation born before its tenure) anew, from its
    /// tenure's start, and a fast takeover makes it `backup_takeover_ms`
    /// longer. The recall is one round trip on top (`recall_ms`).
    pub fn configure(&self, c: ReachConfig) {
        let mut g = self.inner.lock().unwrap();
        g.min_p2p_ms = 2 * c.p2p_delay_hi_ms + c.settle_retry_ms;
        g.min_s3_ms = 2 * c.s3_latency_hi_ms + c.lease_read_extra_ms;
        g.min_lease_read_ms = c.s3_latency_lo_ms + c.lease_read_extra_lo_ms;
        g.lease_ttl_ms = c.lease_ttl_ms;
        g.claim_grace_ms = c.claim_grace_ms;
        g.claim_tail_ms = 2 * c.p2p_delay_hi_ms + 2 * c.s3_latency_hi_ms;
        g.p2p_hi_ms = c.p2p_delay_hi_ms;
        g.margin_ms = c.expiry_margin_ms;
        g.horizon_ms = c.delegation_ttl_ms + c.expiry_margin_ms;
        g.inherit_slack_ms = c.backup_takeover_ms;
        g.recall_ms = 2 * c.p2p_delay_hi_ms + 2 * c.s3_latency_hi_ms;
    }

    /// The bucket served `node` a lease GET at `served_ms`; the answer reached
    /// it at `observed_ms` (issued when it was served).
    pub fn lease_read(&self, node: NodeId, served_ms: u64, observed_ms: u64) {
        self.lease_read_sent(node, served_ms, served_ms, observed_ms);
    }

    /// `node` issued a lease GET at `sent_ms`, the bucket served it at
    /// `served_ms`, the answer reached it at `observed_ms`.
    pub fn lease_read_sent(&self, node: NodeId, sent_ms: u64, served_ms: u64, observed_ms: u64) {
        self.inner
            .lock()
            .unwrap()
            .lease_reads
            .push((node, sent_ms, served_ms, observed_ms));
    }

    /// `node` was restarted now with its journal (its synced rows survived).
    pub fn journal_kept(&self, node: NodeId) {
        let at = self.now_ms();
        self.journal_kept_at(at, node);
    }

    pub fn journal_kept_at(&self, at: u64, node: NodeId) {
        self.inner.lock().unwrap().kept.push((at, node));
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

    /// A lease body of `epoch` landed in the bucket (`inherited_epoch`: the
    /// epoch of the tail it inherited, 0 for none).
    #[allow(clippy::too_many_arguments)]
    pub fn lease_landed(
        &self,
        holder: NodeId,
        released: bool,
        backups: Vec<NodeId>,
        inherited: Vec<NodeId>,
        epoch: u64,
        inherited_epoch: u64,
    ) {
        let at = self.now_ms();
        let ends_ms = at + self.inner.lock().unwrap().lease_ttl_ms;
        self.lease_landed_span(LeaseSpan {
            at_ms: at,
            holder,
            ends_ms,
            released,
            backups,
            inherited,
            epoch,
            inherited_epoch,
        });
    }

    /// The highest epoch of a lease body that landed.
    pub fn max_epoch(&self) -> u64 {
        let g = self.inner.lock().unwrap();
        g.leases.iter().map(|l| l.epoch).max().unwrap_or(0)
    }

    /// `node`'s core could hand a claimant the tail of each of `epochs` in
    /// place now (`Core::seal_tail_hold`: rows it executed under the epoch
    /// and holds in its journal, a backup role of it, a member copy of it);
    /// kept when it changed.
    pub fn note_tails(&self, node: NodeId, epochs: Vec<u64>) {
        self.note_tails_at(self.now_ms(), node, epochs);
    }

    pub fn note_tails_at(&self, at: u64, node: NodeId, epochs: Vec<u64>) {
        let mut g = self.inner.lock().unwrap();
        let since = attach_since(&g, at, node);
        let v = g.tails.entry(node).or_default();
        if v.last().is_some_and(|(t, e)| *t >= since && *e == epochs) {
            return;
        }
        v.push((at, epochs));
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
        self.lease_landed_full(at, holder, ends, released, backups, Vec::new());
    }

    pub fn lease_landed_full(
        &self,
        at: u64,
        holder: NodeId,
        ends: u64,
        released: bool,
        backups: Vec<NodeId>,
        inherited: Vec<NodeId>,
    ) {
        self.lease_landed_span(LeaseSpan {
            at_ms: at,
            holder,
            ends_ms: ends,
            released,
            backups,
            inherited,
            epoch: 0,
            inherited_epoch: 0,
        });
    }

    pub fn lease_landed_span(&self, span: LeaseSpan) {
        self.inner.lock().unwrap().leases.push(span);
    }

    /// `node`'s core published its authority after a step (kept when it
    /// changed, or when a grant's end moved by a quarter second).
    pub fn note_view(
        &self,
        node: NodeId,
        held: Held,
        grants: Vec<(u64, u64)>,
        members: Vec<(u64, NodeId)>,
        busy: u8,
    ) {
        self.note_view_at(self.now_ms(), node, held, grants, members, busy);
    }

    pub fn note_view_at(
        &self,
        at: u64,
        node: NodeId,
        held: Held,
        grants: Vec<(u64, u64)>,
        members: Vec<(u64, NodeId)>,
        busy: u8,
    ) {
        let mut g = self.inner.lock().unwrap();
        // (A view from before the node's last attach is another process's.)
        let since = attach_since(&g, at, node);
        let v = g.views.entry(node).or_default();
        if let Some(last) = v.last().filter(|l| l.at >= since) {
            let same = last.held == held
                && last.busy == busy
                && last.members == members
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
            members,
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

/// Whether root `n` (in force at `at`) can commit what it settles: an epoch
/// can; a lease holder needs its S3 path, or a backup it can reach (the group
/// commits without S3). A lone root with S3 cut cannot commit even a refusal.
fn can_commit(g: &Inner, at: u64, n: NodeId) -> bool {
    match held(g, at, n) {
        Held::Epoch => true,
        Held::Lease => {
            s3_up(g, at, n)
                || lease_at(g, at)
                    .is_some_and(|l| l.backups.iter().any(|b| *b != n && peer_up(g, at, n, *b)))
        }
        Held::No => false,
    }
}

/// What the requester could reach in a segment.
enum Seg {
    /// A sequencer.
    Via(Via),
    /// The root could end the generation, but no holder of its tail can be
    /// reached from it (the group's strict wait).
    TailUnreachable(String),
    /// The root is reachable, but its allowance for ending the generation is
    /// not over (by design it has no sequencer for these keys yet).
    NotDue(String),
    /// Nothing.
    Nothing,
}

/// When the root is due to have ended a generation that does not live at `at`
/// (and so to execute the op): the root's window plus one recall round trip.
///
/// - A generation whose grant the delegate renewed: the grant ended at `end`
///   on the delegate (`sent + ttl - margin`); the root's window is
///   `receipt + ttl + margin`, so up to `end + 2 x margin` plus the one-way
///   delay of the renewal.
/// - Whatever was renewed or not, the root's own grant gave it a window of
///   `born + ttl + margin` (`reclaim_horizon_ms`): the only window of a
///   generation never installed, or never renewed (a grant whose end is 0 in
///   the views).
/// - A root whose tenure began after the generation was born inherited it and
///   started its window anew, `tenure + reclaim_horizon_ms` (plus the takeover
///   slack after a fast takeover: see [`tenure_of`]). A generation born in the
///   tenure was never inherited: its window is its own.
///
/// `since` is where an unknown birth is taken to be (the stretch's start).
fn gen_end_due(g: &Inner, q: &Question, node: NodeId, gen: u64, at: u64, since: u64) -> u64 {
    let mut installed: Option<u64> = None;
    let mut renewed_until: Option<u64> = None;
    for v in g
        .views
        .get(&node)
        .into_iter()
        .flatten()
        .filter(|v| v.at <= at)
    {
        for (_, ends) in v.grants.iter().filter(|(gn, _)| *gn == gen) {
            installed = Some(installed.map_or(v.at, |i| i.min(v.at)));
            if *ends > 0 {
                renewed_until = Some(renewed_until.map_or(*ends, |e| e.max(*ends)));
            }
        }
    }
    let born = q.born.get(&gen).copied().or(installed).unwrap_or(since);
    let by_grant = renewed_until.map_or(0, |end| end + 2 * g.margin_ms + g.p2p_hi_ms);
    let inherited = tenure_of(g, at).filter(|t| born < t.start).map_or(0, |t| {
        t.start + g.horizon_ms + if t.fast { g.inherit_slack_ms } else { 0 }
    });
    inherited.max((born + g.horizon_ms).max(by_grant)) + g.recall_ms
}

/// The longest the root's recall resend waits (`on_release_resend`,
/// delegate.rs: the delay doubles from a request's timeout up to 2 s).
const RECALL_RESEND_CAP_MS: u64 = 2_000;

/// When the root is due to have recalled keys across generations: it ends
/// every involved generation, so the last of their allowances. Each is
/// anchored on its own grant's end, birth or tenure, never on the window or
/// the stretch (a stretch's start only says since when the keys span them).
///
/// A generation whose grant lives and whose delegate the root reaches is not
/// outwaited: the root recalls it, and the delegate answers within a round
/// trip (L3 resends an unanswered recall, the delay doubling up to 2 s). So
/// its allowance is at most one recall round trip plus that back-off past
/// the start of the stretch (from `since` on) in which a root was in force,
/// could commit, was reached by the requester and reached every such
/// delegate (plan 46 M0.4 review, SHOULD-2). Only a delegate the root cannot
/// reach is outwaited for its grant's whole window.
fn blocked_due(g: &Inner, q: &Question, gens: &[(NodeId, u64)], at: u64, since: u64) -> u64 {
    let outwait = |node: NodeId, gen: u64| gen_end_due(g, q, node, gen, at, since);
    let plain = gens
        .iter()
        .map(|(node, gen)| outwait(*node, *gen))
        .max()
        .unwrap_or(0);
    let live: Vec<(NodeId, u64)> = gens
        .iter()
        .copied()
        .filter(|(node, gen)| delegate_in_force(g, at, *node, *gen))
        .collect();
    if live.is_empty() {
        return plain;
    }
    let me = q.requester;
    let capped = g
        .views
        .keys()
        .copied()
        .filter(|r| root_in_force(g, at, *r))
        .filter_map(|r| {
            let recalls = |t: u64| {
                root_in_force(g, t, r)
                    && can_commit(g, t, r)
                    && peer_up(g, t, me, r)
                    && live.iter().all(|(node, _)| peer_up(g, t, r, *node))
            };
            let anchor = stable_since(g, since, at, recalls)?;
            let recalled = anchor + g.recall_ms + RECALL_RESEND_CAP_MS;
            gens.iter()
                .map(|(node, gen)| {
                    let due = outwait(*node, *gen);
                    if live.contains(&(*node, *gen)) {
                        due.min(recalled)
                    } else {
                        due
                    }
                })
                .max()
        })
        .min();
    capped.map_or(plain, |c| c.min(plain))
}

/// Every instant in `(from, to]` at which the network, a node's liveness,
/// S3 path, view or the lease changed.
fn change_marks(g: &Inner, from: u64, to: u64) -> Vec<u64> {
    let inside = |t: &u64| *t > from && *t <= to;
    let mut marks: Vec<u64> = g.links.iter().map(|e| e.0).filter(inside).collect();
    marks.extend(g.alive.iter().map(|e| e.0).filter(inside));
    marks.extend(g.s3.iter().map(|e| e.0).filter(inside));
    marks.extend(g.views.values().flatten().map(|v| v.at).filter(inside));
    marks.extend(g.leases.iter().map(|l| l.at_ms).filter(inside));
    for (_, a, b) in &g.stopped {
        marks.extend([*a, *b].into_iter().filter(inside));
    }
    marks.sort_unstable();
    marks.dedup();
    marks
}

/// When the stretch ending at `at` in which `holds` held began, no earlier
/// than `floor`; `None` when it does not hold at `at`.
fn stable_since(g: &Inner, floor: u64, at: u64, holds: impl Fn(u64) -> bool) -> Option<u64> {
    if !holds(at) {
        return None;
    }
    for t in change_marks(g, floor, at).into_iter().rev() {
        if !holds(t - 1) {
            return Some(t);
        }
    }
    Some(floor)
}

/// The members of generation `gen`'s group (delegate `node`) as of `at`: the
/// latest version the views carry, never a union over the run (plan 46 M0.4
/// review, SHOULD-4a: a member a later version dropped holds no tail the
/// root waits for). The delegate's current view, when it still knows the
/// generation and is up, is the latest version (it sets the group);
/// otherwise the current views of the nodes up that know it (a root's
/// table); otherwise the last view that knew it. A view knows a generation it has a grant of or
/// names in its members (`(gen, 0)`: known, no group).
fn gen_members(g: &Inner, at: u64, node: NodeId, gen: u64) -> Vec<NodeId> {
    let knows = |v: &View| {
        v.grants.iter().any(|(gn, _)| *gn == gen) || v.members.iter().any(|(gn, _)| *gn == gen)
    };
    let of = |v: &View| -> Vec<NodeId> {
        v.members
            .iter()
            .filter(|(gn, m)| *gn == gen && *m != 0)
            .map(|(_, m)| *m)
            .collect()
    };
    let current = |n: NodeId| view_at(g, at, n).filter(|v| attached(g, at, n) && knows(v));
    let mut out: Vec<NodeId> = if let Some(v) = current(node) {
        of(v)
    } else {
        let current: Vec<&View> = g
            .views
            .keys()
            .filter_map(|n| current(*n))
            .filter(|v| knows(v))
            .collect();
        if !current.is_empty() {
            current.into_iter().flat_map(of).collect()
        } else {
            g.views
                .values()
                .flatten()
                .filter(|v| v.at <= at && knows(v))
                .max_by_key(|v| v.at)
                .map_or_else(Vec::new, of)
        }
    };
    out.sort_unstable();
    out.dedup();
    out
}

/// Whether root `r` can reach a holder of generation `gen`'s tail at `at`: the
/// delegate, or a member of its group. A generation without a group has no
/// tail elsewhere: the root ends it without one, so nothing can be missing.
fn tail_reachable(g: &Inner, at: u64, r: NodeId, node: NodeId, gen: u64) -> bool {
    let members = gen_members(g, at, node, gen);
    // Plan 46 M1.2 step 14: a delegate that was attached again since it
    // installed the grant (a restart: it may have lost the unsynced end of its
    // journal, and is not taken up again, `delegation_sync`) is not a holder of
    // the tail the root can end the generation by. A release it answers
    // rowless is sealed by the group (`Core::root_release_appended`), so the
    // root needs a member.
    let delegate_holds = delegate_attached_since_install(g, at, node, gen);
    members.is_empty()
        || delegate_holds
            .then_some(node)
            .into_iter()
            .chain(members)
            .any(|h| running(g, at, h) && peer_up(g, at, r, h))
}

/// Whether `node` has been attached continuously since it first listed a grant
/// of `gen` (no restart in between).
fn delegate_attached_since_install(g: &Inner, at: u64, node: NodeId, gen: u64) -> bool {
    let installed = g
        .views
        .get(&node)
        .into_iter()
        .flatten()
        .filter(|v| v.at <= at && v.grants.iter().any(|(gn, _)| *gn == gen))
        .map(|v| v.at)
        .min();
    installed.is_none_or(|t| attach_since(g, at, node) <= t)
}

/// The generations the root has to end before it executes `q`'s op at `at`
/// (each needs a holder of its tail the root reaches, [`root_path`]): the
/// one whose grant does not live, or every one of keys across generations.
/// None when the keys are the root's, or a live delegate's.
fn gens_to_end(g: &Inner, q: &Question, at: u64) -> Vec<(NodeId, u64)> {
    match q.owner_at(at).1 {
        Owner::Root => Vec::new(),
        Owner::Delegate { node, gen } if delegate_in_force(g, at, node, gen) => Vec::new(),
        Owner::Delegate { node, gen } => vec![(node, gen)],
        Owner::Blocked(gens) => gens,
    }
}

/// Whether the requester, once its claim of the lease is done at `done` and
/// it is the root, reaches a holder of the tail of every generation it has to
/// end for `q`'s op, at some instant up to `to`: the condition [`root_path`]
/// puts on a root in force (`tail`). The claimant inherits the generations
/// its predecessors left live (`Core::delegation_sync`, delegate.rs: "inherited
/// a delegation from a predecessor root") and ends each only with its tail:
/// a recorded backup it cannot reach for the seal leaves the generation
/// unended (`deleg_seal_backup`, delegate.rs: "the generation is not ended
/// without its tail"). Plan 46 M0.3 (hang item 1, seeds 214 and 1004 of
/// `delegated-backup-and-delegate-die`: the claimant became the root, then
/// waited for a dead group's tail as the roots of the allowed `EIO`s do).
fn claimant_reaches_tails(g: &Inner, q: &Question, done: u64, to: u64) -> Result<(), String> {
    let me = q.requester;
    let mut points = vec![done];
    points.extend(change_marks(g, done, to));
    points.extend(
        q.owners
            .iter()
            .map(|(t, _)| *t)
            .filter(|t| *t > done && *t <= to),
    );
    points.sort_unstable();
    points.dedup();
    let mut why: Option<String> = None;
    for t in points {
        let gens = gens_to_end(g, q, t);
        match gens
            .iter()
            .find(|(node, gen)| !tail_reachable(g, t, me, *node, *gen))
        {
            None => return Ok(()),
            Some((node, gen)) => {
                why.get_or_insert_with(|| {
                    format!(
                        "the claimant {me}, the root once its claim is done at {done} ms, reaches no holder of \
                         generation {gen}'s tail (delegate {node}, members {:?}) up to {to} ms",
                        gen_members(g, t, *node, *gen)
                    )
                });
            }
        }
    }
    Err(why.unwrap_or_default())
}

/// Whether `n` could hand a claimant the tail of one of `epochs` in place at
/// `at`, as its core last published it since it was last attached
/// ([`Reach::note_tails`]); `None` when that is not known (no epoch known, or
/// nothing published, as in the unit tests that set no epochs).
fn holds_tail_of(g: &Inner, at: u64, n: NodeId, epochs: &[u64]) -> Option<bool> {
    if epochs.is_empty() {
        return None;
    }
    let since = attach_since(g, at, n);
    let (_, held) = g
        .tails
        .get(&n)?
        .iter()
        .rev()
        .find(|(t, _)| *t <= at)
        .filter(|(t, _)| *t >= since)?;
    Some(held.iter().any(|e| epochs.contains(e)))
}

/// The sequencer `q`'s requester could reach in the segment starting at
/// `at` (every state is constant until the next recorded change).
fn reach_at(g: &Inner, q: &Question, at: u64) -> Seg {
    let me = q.requester;
    // The requester itself must be running to send anything.
    if !running(g, at, me) {
        return Seg::Nothing;
    }
    let (since, owner) = q.owner_at(at);
    match owner {
        Owner::Root => root_path(g, me, at, &[]),
        // The generation's delegate alone while its grant lives: the root
        // redirects or parks the op.
        Owner::Delegate { node, gen } => {
            if delegate_in_force(g, at, node, gen) {
                return if node == me {
                    Seg::Via(Via::Itself)
                } else if peer_up(g, at, me, node) {
                    Seg::Via(Via::Delegate(node))
                } else {
                    Seg::Nothing
                };
            }
            // A grant that ran out, or never was: the root ends the
            // generation once its window and the recall are over.
            let due = gen_end_due(g, q, node, gen, at, since);
            root_after(g, me, at, due, &[(node, gen)])
        }
        // Keys across generations: the root recalls them first.
        Owner::Blocked(gens) => {
            match root_after(g, me, at, blocked_due(g, q, &gens, at, since), &gens) {
                Seg::NotDue(why) => Seg::NotDue(format!("{why}, keys across generations")),
                s => s,
            }
        }
    }
}

/// The root's path to the op once it is `due` to have ended `gens` (and not
/// before: by design nobody executes the op then).
fn root_after(g: &Inner, me: NodeId, at: u64, due: u64, gens: &[(NodeId, u64)]) -> Seg {
    match root_path(g, me, at, gens) {
        Seg::Via(_) | Seg::TailUnreachable(_) if at < due => Seg::NotDue(format!(
            "the root ends the generation {gens:?} (its window and the recall) at {due} ms"
        )),
        s => s,
    }
}

/// The path to the op through the roots: the requester as one, a root it can
/// reach, the lease once claimable. Every generation of `gens` must have a
/// tail holder the root reaches.
fn root_path(g: &Inner, me: NodeId, at: u64, gens: &[(NodeId, u64)]) -> Seg {
    let tail = |r: NodeId| -> Seg {
        for (node, gen) in gens {
            if !tail_reachable(g, at, r, *node, *gen) {
                return Seg::TailUnreachable(format!(
                    "root {r} reaches no holder of generation {gen}'s tail (delegate {node}, members {:?})",
                    gen_members(g, at, *node, *gen)
                ));
            }
        }
        Seg::Via(if r == me { Via::Itself } else { Via::Root(r) })
    };
    let lease = lease_at(g, at);
    if root_in_force(g, at, me) && can_commit(g, at, me) {
        return tail(me);
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
    if let Some(r) = roots
        .into_iter()
        .find(|r| can_commit(g, at, *r) && peer_up(g, at, me, *r))
    {
        return tail(r);
    }
    // No root to ask: the lease, once claimable (the claim itself is judged
    // over the run, `claim_done`).
    let acquirable = match lease {
        None => true,
        Some(l) => {
            let grace = claim_grace_for(g, &l, me);
            l.holder == 0 || l.released || l.ends_ms + grace <= at || readopts_own(g, at, me, &l)
        }
    };
    if acquirable && s3_up(g, at, me) {
        Seg::Via(Via::LeasePath)
    } else {
        Seg::Nothing
    }
}

/// Whether `me` re-adopts its own unexpired lease `l` at once at `at`: the
/// body names it, it restarted since the body landed, and the process that
/// went down still held the lease (neither deposed nor sealed: a sealed epoch
/// is persisted and never revived). The product claims its own untracked
/// lease without the TTL wait (`LeaseState::classify`, lease.rs: "our own
/// lease that we do not track: a restart"), gated like a takeover (plan 46
/// M0.4 review, SHOULD-3).
fn readopts_own(g: &Inner, at: u64, me: NodeId, l: &LeaseSpan) -> bool {
    if l.holder != me || l.released {
        return false;
    }
    let attach = attach_since(g, at, me);
    attach > l.at_ms
        && g.views
            .get(&me)
            .into_iter()
            .flatten()
            .rev()
            .find(|v| v.at < attach)
            .is_some_and(|v| v.held == Held::Lease && v.busy & BUSY_LOST == 0)
}

/// How long past its end `l` is not `me`'s to claim: the backups' claim grace
/// of a lease with backups `me` is not among.
fn claim_grace_for(g: &Inner, l: &LeaseSpan, me: NodeId) -> u64 {
    if l.backups.is_empty() || l.backups.contains(&me) {
        0
    } else {
        g.claim_grace_ms
    }
}

/// When the tenure whose body landed at `at` began: the landing of the first of
/// the holder's consecutive bodies.
fn tenure_start(g: &Inner, at: u64) -> u64 {
    let Some(idx) = g.leases.iter().rposition(|l| l.at_ms <= at) else {
        return 0;
    };
    let holder = g.leases[idx].holder;
    g.leases[..=idx]
        .iter()
        .rev()
        .take_while(|l| l.holder == holder)
        .last()
        .map_or(0, |l| l.at_ms)
}

/// A root's tenure as the product's delegation state sees it.
struct Tenure {
    /// When it began: the landing of the body that acquired the lease.
    start: u64,
    /// The acquisition took an unexpired lease of another node over (a seal or
    /// an `ack=s3` takeover): the product adds the takeover slack to the
    /// windows it inherits.
    fast: bool,
}

/// The tenure the body in force at `at` belongs to. Unlike [`tenure_start`] it
/// starts anew at every acquisition, the same holder's included: a holder that
/// lost the lease and acquired it again (same epoch, `takeover=true`) dropped
/// its delegation state (`Core::deleg_on_lease_gone`, delegate.rs:7660-7708) and
/// inherits every generation again at its next sync, `now +
/// reclaim_horizon_ms + inherit_slack_ms` (`delegation_sync`,
/// delegate.rs:1232-1234). It is seen as a drop of the holder's view from
/// held to nothing, or a detach, between two of its bodies. (The product's
/// window runs from the sync, after the tenure's gate; the landing is never
/// later.)
fn tenure_of(g: &Inner, at: u64) -> Option<Tenure> {
    let mut idx = g.leases.iter().rposition(|l| l.at_ms <= at)?;
    let holder = g.leases[idx].holder;
    while idx > 0 {
        let (prev, cur) = (&g.leases[idx - 1], &g.leases[idx]);
        if prev.holder != holder || lost_between(g, holder, prev.at_ms, cur.at_ms) {
            break;
        }
        idx -= 1;
    }
    let first = &g.leases[idx];
    // The slack only after a takeover of another node's unexpired lease: the
    // product sets `inherit_slack_ms` in `note_fast_takeover` (backup.rs:2579),
    // called for a permitted takeover of a lease that is not released and not
    // its own (`fast_prev`, jobs.rs:3623-3625), and resets it to 0 at every
    // acquisition (jobs.rs:3626). A body of the same holder
    // is no such takeover.
    let fast = idx > 0 && {
        let p = &g.leases[idx - 1];
        p.holder != holder && p.holder != 0 && !p.released && p.ends_ms > first.at_ms
    };
    Some(Tenure {
        start: first.at_ms,
        fast,
    })
}

/// Whether `n` let go of what it held, or was detached, in `(from, to]`.
fn lost_between(g: &Inner, n: NodeId, from: u64, to: u64) -> bool {
    let detached = g
        .alive
        .iter()
        .any(|(t, node, up)| *node == n && !*up && *t > from && *t <= to);
    let mut last = Held::No;
    let mut dropped = false;
    for v in g.views.get(&n).into_iter().flatten() {
        if v.at > to {
            break;
        }
        if v.at > from && last != Held::No && v.held == Held::No {
            dropped = true;
        }
        last = v.held;
    }
    detached || dropped
}

/// Whether `n` was detached (crashed) at any moment of `(since, at]` and lost
/// what it had not synced: a node that restarted with its journal
/// (`keep_journal`) holds its synced rows again from its bootstrap on (counted
/// from the attach, the sim's bootstrap is not modelled apart).
fn went_down_between(g: &Inner, n: NodeId, since: u64, at: u64) -> bool {
    g.alive.iter().enumerate().any(|(i, (t, node, up))| {
        if *node != n || *up || *t <= since || *t > at {
            return false;
        }
        // The attach that ended this detach, if it came by `at`, and the
        // journal kept in the same up-period (the restart records it right
        // after the attach, in the same step): between that attach and the
        // node's next change of attachment.
        let back = g.alive[i + 1..]
            .iter()
            .enumerate()
            .find(|(_, (_, node, up))| *node == n && *up);
        let kept = back.is_some_and(|(j, (u, _, _))| {
            let next = g.alive[i + 1 + j + 1..]
                .iter()
                .find(|(_, node, _)| *node == n)
                .map_or(u64::MAX, |(t, _, _)| *t);
            *u <= at
                && g.kept
                    .iter()
                    .any(|(k, node)| *node == n && *k >= *u && *k < next && *k <= at)
        });
        !kept
    })
}

/// The group of the tenure whose body landed at `at`: every node any body of
/// the tenure (the consecutive bodies of one holder) listed as a backup or named
/// in the tail it inherited and had not yet shipped, and the holder (its own
/// disk holds its journal). Empty when the tenure never had either.
fn tenure_group(g: &Inner, at: u64) -> Vec<NodeId> {
    let Some(idx) = g.leases.iter().rposition(|l| l.at_ms <= at) else {
        return Vec::new();
    };
    let holder = g.leases[idx].holder;
    let mut members: std::collections::BTreeSet<NodeId> = std::collections::BTreeSet::new();
    for l in g.leases[..=idx].iter().rev() {
        if l.holder != holder {
            break;
        }
        members.extend(l.backups.iter().copied());
        members.extend(l.inherited.iter().copied());
    }
    if members.is_empty() {
        return Vec::new();
    }
    members.insert(holder);
    members.into_iter().collect()
}

/// When a claim of the lease that `me` starts in a run beginning at `run_from`
/// of a window beginning at `window_from` would be done, as the product
/// implements it (the module doc), or why it cannot be done by `until`.
fn claim_done(
    g: &Inner,
    me: NodeId,
    run_from: u64,
    window_from: u64,
    until: u64,
) -> Result<u64, String> {
    let lease = lease_at(g, run_from);
    let since_of = |landed: u64| -> u64 {
        // The claimant's first sight of the body: its first lease GET served
        // after the body landed, but never later than a read it would have
        // made at the window's start (a product delay before the first read
        // is not excused).
        let at_start = landed.max(window_from) + g.min_s3_ms;
        g.lease_reads
            .iter()
            .filter(|(n, _, served, _)| *n == me && *served >= landed)
            .map(|(_, _, _, observed)| *observed)
            .min()
            .map_or(at_start, |first| first.min(at_start))
    };
    let ready = match &lease {
        None => run_from + g.min_s3_ms,
        Some(l) if l.holder == 0 || l.released => since_of(l.at_ms).max(run_from),
        // Its own lease, re-adopted on the first read of its new process.
        Some(l) if readopts_own(g, run_from, me, l) => {
            since_of(l.at_ms.max(attach_since(g, run_from, me))).max(run_from)
        }
        Some(l) => since_of(l.at_ms) + g.lease_ttl_ms + claim_grace_for(g, l, me),
    };
    // A claim by TTL starts with a fresh read: the product claims by TTL only
    // on a read it *handles* at or after its observation's TTL has run
    // (`LeaseState::classify`, the observation rule: a read at `t1 >= t0 +
    // ttl`; `Plan::Busy::retry_at` issues that read), and the promise check and
    // the CAS follow its answer. Only that arm waits: no lease, a lease nobody
    // holds, a released one (and a fenced one, which the model does not tell
    // apart) go to the CAS after the FIRST read, which `ready` already counts,
    // and a lease of `me`'s own is claimed at once too when untracked (the
    // model cannot tell a tracked one, and charges nothing extra either way).
    // The fresh read's cost is taken from the history: the earliest lease read
    // of the claimant issued at or after the claim's start (the retry's read;
    // the history cannot tell it from another lease GET of the node, such as a
    // registration read, so the earliest answered one stands, and a read
    // issued before the start is not taken for it), less any delay of the
    // product's before it was issued (only the read's own transit is excused,
    // as a read issued at the start would have taken). With no such read, the
    // fastest read the configuration allows (plan 46 M0.3, the review of cause
    // 6: with slow lease reads, 0.8 to 1 s each, that read was the residual
    // the model left out). A claim made on a takeover permit (older behaviour:
    // the model charges the TTL and grace for it anyway) is not made worse.
    let start = ready.max(run_from);
    let waits_ttl = lease
        .as_ref()
        .is_some_and(|l| l.holder != 0 && !l.released && l.holder != me);
    let read_done = if waits_ttl {
        g.lease_reads
            .iter()
            .filter(|(n, sent, _, observed)| *n == me && *sent >= start && *observed >= start)
            .min_by_key(|(_, _, _, observed)| *observed)
            .map_or(start + g.min_lease_read_ms, |(_, sent, _, observed)| {
                *observed - (*sent - start).min(*observed - start)
            })
    } else {
        start
    };
    let done = read_done + g.claim_tail_ms;
    if done > until {
        return Err(format!(
            "its claim of the lease needs until {done} ms (a lease read, a TTL of its own observation, \
             a fresh read answered at {read_done} ms, the promise check and the CAS), after the window's \
             end {until}"
        ));
    }
    // A lease with a group: the claim carries the tail of the tenure it takes
    // over, which only the tenure's group (every member any of its bodies
    // listed, and the holder's own disk) holds; it waits until one of them
    // answers.
    if let Some(l) = lease.filter(|l| l.holder != 0 && !l.released) {
        let members = tenure_group(g, l.at_ms);
        // A member holds the tail only if it was up all along: a node that
        // crashed since the tenure began lost what it had not synced, and
        // answers only after its bootstrap.
        let mut since = tenure_start(g, l.at_ms);
        if !l.inherited.is_empty() && since > 0 {
            // (The tail a tenure inherited was the previous tenure's.)
            since = tenure_start(g, since.saturating_sub(1));
        }
        // The epochs whose tail the claim asks for (`LeaseState::classify`,
        // lease.rs, `Plan::FetchTail`: the lease's own, and the inherited
        // tail's), over the tenure's bodies.
        let mut asked: Vec<u64> = g
            .leases
            .iter()
            .filter(|b| b.at_ms <= l.at_ms && tenure_start(g, b.at_ms) == tenure_start(g, l.at_ms))
            .flat_map(|b| [b.epoch, b.inherited_epoch])
            .filter(|e| *e != 0)
            .collect();
        asked.sort_unstable();
        asked.dedup();
        // The claimant is a source of its own claim, asked in place: it hands
        // the tail over only if it holds rows of an asked epoch, a role or a
        // member copy of it (`begin_tail_fetch`, seal_tail.rs: "This node's
        // own copy ... asked in place", `seal_tail_hold`); one that holds
        // nothing waits for another member like everyone (plan 46 M0.3, hang
        // item 1, `delegated-backup-and-delegate-die` seed 435: the deposed
        // holder of the inherited epoch had shipped every row of it). Its own
        // lease is its own (as before).
        let me_holds = |at: u64| l.holder == me || holds_tail_of(g, at, me, &asked).unwrap_or(true);
        let reachable = |at: u64| {
            members.iter().any(|m| {
                !went_down_between(g, *m, since, at)
                    && if *m == me {
                        me_holds(at)
                    } else {
                        running(g, at, *m) && peer_up(g, at, me, *m)
                    }
            })
        };
        if !members.is_empty() {
            // Any stretch from the claim's start to the window's end in which
            // a member answers for a round trip serves the tail, not only the
            // claim's start and end (plan 46 M0.4 review, SHOULD-4b); a
            // member first reached after the claim was otherwise done
            // delays it by that round trip.
            let rtt = 2 * g.p2p_hi_ms;
            let mut points = vec![start];
            points.extend(change_marks(g, start, until));
            let mut fetched: Option<u64> = None;
            let mut run: Option<u64> = None;
            for (i, t) in points.iter().copied().enumerate() {
                if !reachable(t) {
                    run = None;
                    continue;
                }
                let begun = *run.get_or_insert(t);
                if begun < done {
                    // (Reached while the claim runs: fetched alongside it.)
                    fetched = Some(done);
                    break;
                }
                let end = points.get(i + 1).copied().unwrap_or(until);
                if end >= begun + rtt {
                    fetched = Some(begun + rtt);
                    break;
                }
            }
            match fetched {
                Some(d) if d <= until => return Ok(d),
                _ => {
                    let own = if members.contains(&me) && !me_holds(until) {
                        format!(" (the claimant itself holds no tail of epochs {asked:?})")
                    } else {
                        String::new()
                    };
                    return Err(format!(
                        "no member of the tenure's group {members:?} had been running since the tenure began and \
                         was reachable for its tail between the claim's start ({start} ms) and the window's end \
                         ({until} ms){own}"
                    ));
                }
            }
        }
    }
    Ok(done)
}

fn classify(g: &Inner, q: Question) -> Verdict {
    // Every instant at which any state changes inside the window.
    let mut marks: Vec<u64> = vec![q.from_ms, q.to_ms];
    let inside = |t: u64| t > q.from_ms && t < q.to_ms;
    marks.extend(g.links.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(g.alive.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(g.s3.iter().map(|e| e.0).filter(|t| inside(*t)));
    marks.extend(q.owners.iter().map(|e| e.0).filter(|t| inside(*t)));
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
    // The instants at which the root becomes due to have ended a generation
    // that does not live (each depends on the state at the mark before).
    let mut dues: Vec<u64> = Vec::new();
    for &m in &marks {
        let (since, owner) = q.owner_at(m);
        let due = match &owner {
            Owner::Root => continue,
            Owner::Delegate { node, gen } => gen_end_due(g, &q, *node, *gen, m, since),
            Owner::Blocked(gens) => blocked_due(g, &q, gens, m, since),
        };
        if inside(due) {
            dues.push(due);
        }
    }
    marks.extend(dues);
    marks.sort_unstable();
    marks.dedup();
    // The reachable runs: consecutive segments reaching a sequencer by the
    // same kind of way merge into one.
    let mut runs: Vec<(Via, u64, u64)> = Vec::new();
    let mut tail_blocked: Option<String> = None;
    let mut not_due: Option<String> = None;
    for w in marks.windows(2) {
        let (s, e) = (w[0], w[1]);
        let via = match reach_at(g, &q, s) {
            Seg::Via(via) => via,
            Seg::TailUnreachable(why) => {
                tail_blocked.get_or_insert(why);
                continue;
            }
            Seg::NotDue(why) => {
                not_due.get_or_insert(why);
                continue;
            }
            Seg::Nothing => continue,
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
        match reach_at(g, &q, q.to_ms) {
            Seg::Via(via) => runs.push((via, q.to_ms, q.to_ms)),
            Seg::TailUnreachable(why) => tail_blocked = Some(why),
            Seg::NotDue(why) => not_due = Some(why),
            Seg::Nothing => {}
        }
    }
    let mut claim_blocked: Option<String> = None;
    for (via, from, to) in runs {
        let floor = match via {
            Via::Itself => 0,
            Via::Root(_) | Via::Delegate(_) => g.min_p2p_ms,
            Via::LeasePath => 0,
        };
        if to - from < floor {
            continue;
        }
        let (to, node) = match via {
            Via::Itself => (to, Some(q.requester)),
            Via::Root(n) | Via::Delegate(n) => (to, Some(n)),
            Via::LeasePath => match claim_done(g, q.requester, from, q.from_ms, to) {
                Ok(done) => match claimant_reaches_tails(g, &q, done, to) {
                    Ok(()) => (done, None),
                    Err(why) => {
                        tail_blocked.get_or_insert(why);
                        continue;
                    }
                },
                Err(why) => {
                    claim_blocked.get_or_insert(why);
                    continue;
                }
            },
        };
        let busy = node.map_or(0, |n| busy_over(g, n, from, to));
        return Verdict::Reachable {
            via,
            from_ms: from,
            to_ms: to,
            busy: busy_names(busy),
        };
    }
    if let Some(why) = tail_blocked {
        return Verdict::GenerationTailUnreachable { why };
    }
    match claim_blocked
        .or(not_due)
        .or_else(|| dead_holders_lease(g, &q))
    {
        Some(why) => Verdict::NoSequencerInTime { why },
        None => Verdict::Isolated {
            why: why_isolated(g, &q),
        },
    }
}

/// The requester reaches S3 and nothing is cut from it, yet no sequencer
/// exists because the lease body still names a holder that holds nothing (a
/// holder a claimant sealed, deposed or restarted; a dead one): the lease is
/// not claimable until its body runs out, by design. That is the lease path
/// not being open in time, not the requester's isolation (plan 46 M0.3
/// re-check 2, `delegated-holder-cut` seed 41: a claimant sealed the holder,
/// then lost its CAS to the holder's own late renewal, and the sealed holder's
/// lease blocked every claim for a TTL).
fn dead_holders_lease(g: &Inner, q: &Question) -> Option<String> {
    let at = q.to_ms.saturating_sub(1);
    let me = q.requester;
    if !running(g, at, me) || !s3_up(g, at, me) {
        return None;
    }
    // Keys a live delegate owns are that delegate's: the requester's way to
    // it is what is missing, not the lease.
    if let Owner::Delegate { node, gen } = q.owner_at(at).1 {
        if delegate_in_force(g, at, node, gen) {
            return None;
        }
    }
    let l = lease_at(g, at).filter(|l| l.holder != 0 && !l.released)?;
    if attached(g, at, l.holder) && root_in_force(g, at, l.holder) || readopts_own(g, at, me, &l) {
        return None;
    }
    Some(format!(
        "the lease still names node {}, which holds nothing (sealed, deposed, restarted or down); \
         it is claimable from {} ms",
        l.holder,
        l.ends_ms + claim_grace_for(g, &l, me)
    ))
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
        } else if !can_commit(g, at, n) && role == "root" {
            "cannot commit"
        } else {
            "up"
        };
        format!("{role} {n} {why}")
    };
    let mut parts = Vec::new();
    let root_state = |parts: &mut Vec<String>| {
        match lease.as_ref().filter(|l| l.holder != 0) {
            Some(l) if root_in_force(g, at, l.holder) => parts.push(state(l.holder, "root")),
            Some(l) => parts.push(format!("root {} holds nothing", l.holder)),
            None => parts.push("no lease holder".into()),
        }
        if !s3_up(g, at, me) {
            parts.push("S3 cut".into());
        }
    };
    match q.owner_at(at).1 {
        Owner::Blocked(gens) => {
            parts.push(format!(
                "keys across generations {gens:?}: the recall is pending"
            ));
            root_state(&mut parts);
        }
        Owner::Delegate { node, gen } => {
            if delegate_in_force(g, at, node, gen) {
                parts.push(state(node, "delegate"));
                parts.push("the generation's root redirects".into());
            } else {
                parts.push(format!(
                    "delegate {node} has no live grant of generation {gen}"
                ));
                root_state(&mut parts);
            }
        }
        Owner::Root => root_state(&mut parts),
    }
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 10 ms P2P delay and a 100 ms settle retry (a P2P run must last
    /// 120 ms), a 40 ms S3 (a lease read 80 ms, a claim's tail and a recall
    /// round trip 100 ms), a 1 000 ms lease, 500 ms delegation grants and a
    /// 100 ms margin: the root ends a generation 600 ms after its grant (a
    /// grant ended at `e` is due at `e + 2 x 100 + 10 + 100 = e + 310`; one
    /// born at `b` and never installed at `b + 600 + 100 = b + 700`).
    fn cfg(claim_grace_ms: u64, lease_read_extra_ms: u64) -> ReachConfig {
        ReachConfig {
            p2p_delay_hi_ms: 10,
            s3_latency_hi_ms: 40,
            lease_ttl_ms: 1_000,
            claim_grace_ms,
            lease_read_extra_ms,
            s3_latency_lo_ms: 0,
            lease_read_extra_lo_ms: 0,
            settle_retry_ms: 100,
            delegation_ttl_ms: 500,
            expiry_margin_ms: 100,
            backup_takeover_ms: 0,
        }
    }

    /// Three nodes up from 0 and a 1 000 ms lease held by node 1 from t=0,
    /// renewed.
    fn world() -> Reach {
        let r = Reach::default();
        r.configure(cfg(0, 0));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(0, 1, 1_000, false);
        r.note_view_at(0, 1, Held::Lease, vec![], vec![], 0);
        r
    }

    /// The window [4 000, 7 000] of `requester`, the keys the root's.
    fn q(requester: NodeId) -> Question {
        Question::new(requester, 4_000, 7_000)
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
        let v = r.classify(q(3));
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
        let v = r.classify(q(3));
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
    }

    #[test]
    fn a_delegate_the_requester_reaches_is_a_sequencer_of_its_generation() {
        // The root is cut, node 2 is the delegate of generation 7 while its
        // grant lasts.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.partition_at(3_000, 3, 1, true);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 9_000)], vec![], 0);
        let v = r.classify(q(3).delegated(2, 7));
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
        r2.note_view_at(3_000, 2, Held::No, vec![(7, 4_010)], vec![], 0);
        assert!(r2.classify(q(3).delegated(2, 7)).isolated());
        // The delegate of another generation is none.
        assert!(r.classify(q(3).delegated(2, 8)).isolated());
        // The same node as the delegate of other keys is no sequencer here
        // (the keys are the root's, which is cut).
        assert!(r.classify(q(3)).isolated());
    }

    #[test]
    fn the_root_does_not_count_while_a_live_generation_owns_the_keys() {
        // The root, node 1, is reachable and holds the lease; generation 7,
        // node 2's, owns the op's keys and node 2 is stopped. The root
        // redirects or parks the op until the generation's end is recorded:
        // no sequencer, whatever the root's reachability.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 9_000)], vec![], 0);
        r.stopped_at(2, 3_900, 7_100);
        let v = r.classify(q(3).delegated(2, 7));
        let Verdict::Isolated { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("delegate 2 stopped"), "{why}");
        // The same op once the end of the generation is recorded (5 500):
        // the root counts from then on.
        let mut ended = q(3).delegated(2, 7);
        ended.owners.push((5_500, Owner::Root));
        let v = r.classify(ended);
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 5_500,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // The requester as the root itself: no sequencer of those keys either
        // while the generation lives.
        let r = world();
        r.lease_landed_at(3_500, 3, 9_000, false);
        r.note_view_at(3_500, 3, Held::Lease, vec![], vec![], 0);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 9_000)], vec![], 0);
        r.stopped_at(2, 3_900, 7_100);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(v.isolated(), "{v:?}");
        // Keys across generations: nobody until the generation's own
        // allowance is over (its grant ended at 4 000: 4 000 + 200 + 10, the
        // recall 100 more: 4 310); then the root, here the requester itself.
        let r = world();
        r.lease_landed_at(3_500, 3, 9_000, false);
        r.note_view_at(3_500, 3, Held::Lease, vec![], vec![], 0);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 4_000)], vec![], 0);
        let mut across = q(3);
        across.owners = vec![(0, Owner::Blocked(vec![(2, 7)]))];
        let v = r.classify(across.clone());
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Itself,
                    from_ms: 4_310,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        across.to_ms = 4_300;
        let v = r.classify(across);
        assert!(v.no_sequencer_in_time(), "{v:?}");
    }

    #[test]
    fn every_delegate_the_keys_had_during_the_window_counts() {
        // Node 2 held the keys until 5 000 (grant to 4 500), then node 1 took
        // generation 8 (cut from the requester): at the `EIO` the table names
        // node 1, which is unreachable, yet node 2 was reachable for 500 ms
        // inside the window.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 4_500)], vec![], 0);
        r.note_view_at(5_000, 1, Held::Lease, vec![(8, 9_000)], vec![], 0);
        r.partition_at(3_000, 3, 1, true);
        let mut moved = q(3).delegated(2, 7);
        moved
            .owners
            .push((5_000, Owner::Delegate { node: 1, gen: 8 }));
        let v = r.classify(moved);
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Delegate(2),
                    from_ms: 4_000,
                    to_ms: 4_500,
                    ..
                }
            ),
            "{v:?}"
        );
        // Judged by the last owner alone it would not have been.
        let last = q(3).delegated(1, 8);
        assert!(r.classify(last).isolated());
    }

    #[test]
    fn a_root_that_cannot_commit_is_no_sequencer() {
        // Node 1 holds the lease with its S3 cut and no backup: it cannot
        // commit even a refusal.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.s3_cut_at(3_000, 1, true);
        let v = r.classify(q(3));
        let Verdict::Isolated { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("cannot commit"), "{why}");
        // With a running backup it linked it commits through the group.
        let r = world();
        r.lease_landed_with(3_500, 1, 9_000, false, vec![2]);
        r.s3_cut_at(3_000, 1, true);
        let v = r.classify(q(3));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    ..
                }
            ),
            "{v:?}"
        );
        // And the requester as that lone root is not a sequencer either.
        let r = world();
        r.lease_landed_at(3_500, 3, 9_000, false);
        r.note_view_at(3_500, 3, Held::Lease, vec![], vec![], 0);
        r.s3_cut_at(3_000, 3, true);
        assert!(r.classify(q(3)).isolated());
    }

    #[test]
    fn a_pause_of_the_sequencer_covering_the_window_isolates() {
        // The root, node 1, is paused across the whole window: the requester's
        // link is up and nobody answers. A pause that ends inside the window
        // with room for a retry and a round trip does not isolate; one that
        // leaves less does.
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 7_100);
        assert!(r.classify(q(3)).isolated());
        // (A pause that outlasts the lease frees the lease path: the
        // requester could take the lease over, so it is not isolated.)
        let r = world();
        r.lease_landed_at(3_500, 1, 5_000, false);
        r.stopped_at(1, 3_900, 7_100);
        assert!(matches!(
            r.classify(q(3)),
            Verdict::Reachable {
                via: Via::LeasePath,
                from_ms: 5_000,
                ..
            }
        ));
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 6_000);
        let v = r.classify(q(3));
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
        // The floor is the retry interval plus a round trip (120 ms): a run
        // of 100 is none, one of 200 is.
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 6_900);
        assert!(r.classify(q(3)).isolated(), "100 ms is under a retry");
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(1, 3_900, 6_800);
        assert!(!r.classify(q(3)).isolated());
    }

    #[test]
    fn a_dead_root_is_reachable_through_the_lease_only_once_it_runs_out() {
        let r = world();
        // Node 1 dies at 3 000; its last body, landed at 2 500, ends at
        // 3 500.
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        // The window [4 000, 7 000]: the body ran out at 3 500, so the
        // lease is acquirable throughout, S3 is reachable and the claim (a
        // read at 4 000, a TTL, the promise check and the CAS: done by
        // 5 180) finishes inside the window.
        let v = r.classify(q(3));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 5_180,
                    ..
                }
            ),
            "{v:?}"
        );
        // With S3 cut, nothing is reachable.
        r.s3_cut_at(3_100, 3, true);
        assert!(r.classify(q(3)).isolated());
        // A lease that has not run out blocks the lease path: the dead
        // holder's body runs to 8 000, after the window (the lease path is
        // not open in time, `dead_holders_lease`).
        let r = world();
        r.lease_landed_at(3_900, 1, 8_000, false);
        r.attached_at(3_950, 1, false);
        let v = r.classify(q(3));
        assert!(
            v.no_sequencer_in_time(),
            "{v:?}: the lease runs to 8 000, after the window"
        );
    }

    #[test]
    fn a_claim_that_cannot_finish_inside_the_window_is_no_sequencer_in_time() {
        // The same dead root, a window of one second: the claim needs a TTL
        // of the claimant's own observation (read at the window's start) and
        // finishes at 5 180, after the window's end.
        let r = world();
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        let v = r.classify(Question::new(3, 4_000, 5_000));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("the window's end"), "{why}");
        // Its own earlier sight of the body (a read served at 2 600, answered
        // at 2 650) starts the TTL there: the claim is done by 3 750 and the
        // run begins at the window's start.
        r.lease_read(3, 2_600, 2_650);
        let v = r.classify(Question::new(3, 4_000, 5_000));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 4_100,
                    ..
                }
            ),
            "{v:?}"
        );
        // Another node's reads are not its own.
        let r = world();
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        r.lease_read(2, 2_600, 2_650);
        assert!(r
            .classify(Question::new(3, 4_000, 5_000))
            .no_sequencer_in_time());
    }

    #[test]
    fn a_lease_with_a_group_needs_a_member_for_its_tail() {
        // Node 1 held the lease with node 2 as its backup and died; node 2 is
        // down too: nobody holds the group's tail, so the claim cannot finish
        // by design (the strict wait of the generation's end).
        let r = world();
        r.lease_landed_with(2_500, 1, 3_500, false, vec![2]);
        r.attached_at(3_000, 1, false);
        r.attached_at(3_000, 2, false);
        let v = r.classify(q(3));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("no member"), "{why}");
        // With the member up and reachable the claim finishes.
        let r = world();
        r.lease_landed_with(2_500, 1, 3_500, false, vec![2]);
        r.attached_at(3_000, 1, false);
        let v = r.classify(q(3));
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
        // The member itself is cut from the claimant: still no tail.
        r.partition_at(3_000, 3, 2, true);
        assert!(r.classify(q(3)).no_sequencer_in_time());
        // The group of the tenure is every member any of its bodies listed: a
        // later body without backups (the backup was dropped) does not free
        // the claim from the tail the dropped backup may hold.
        let r = world();
        r.lease_landed_with(2_000, 1, 3_000, false, vec![2]);
        r.lease_landed_with(2_500, 1, 3_500, false, vec![]);
        r.attached_at(3_000, 1, false);
        r.attached_at(3_000, 2, false);
        let v = r.classify(q(3));
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // The tail a tenure inherited and has not shipped lives with the old
        // group too: node 2 took the lease from node 3 (a backup of 3's
        // tenure) with 3's tail in `inherited_tail`; both are down.
        let r = world();
        r.lease_landed_full(2_500, 2, 3_500, false, vec![], vec![3, 2]);
        r.attached_at(3_000, 2, false);
        r.attached_at(3_000, 3, false);
        let v = r.classify(q(1));
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // The tail of an inherited tenure was held since the tenure before it
        // began: node 3 (its holder) was down in the middle of that one.
        let r = world();
        r.lease_landed_with(100, 3, 6_100, false, vec![1]);
        r.lease_landed_full(2_500, 1, 8_500, false, vec![], vec![1, 3]);
        r.attached_at(1_500, 3, false);
        r.attached_at(3_800, 3, true);
        r.attached_at(3_900, 1, false);
        let v = r.classify(Question::new(2, 9_000, 12_000));
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // A member that crashed after the tenure began (and came back) is not
        // relied on for the tail: it lost what it had not synced.
        let r = world();
        r.lease_landed_with(2_000, 1, 3_500, false, vec![2]);
        r.attached_at(3_000, 1, false);
        r.attached_at(3_200, 2, false);
        r.attached_at(3_400, 2, true);
        let v = r.classify(q(3));
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // A tenure that never had a backup needs no tail.
        let r = world();
        r.lease_landed_with(2_500, 1, 3_500, false, vec![]);
        r.attached_at(3_000, 1, false);
        assert!(!r.classify(q(3)).no_sequencer_in_time());
    }

    #[test]
    fn a_requester_stopped_for_the_window_could_send_nothing() {
        let r = world();
        r.lease_landed_at(3_500, 1, 8_000, false);
        r.stopped_at(3, 3_000, 7_000);
        let v = r.classify(q(3));
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
        r.note_view_at(3_500, 3, Held::Lease, vec![], vec![], 0);
        r.partition_at(3_000, 3, 1, true);
        r.partition_at(3_000, 3, 2, true);
        let v = r.classify(q(3));
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
        let v = r.classify(q(3));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("node 1, which holds nothing"), "{why}");
        // The restarted node, asked for its own op, re-adopts its own lease at
        // once (SHOULD-3): a lease read from the window's start, the promise
        // check and the CAS, done by 4 180.
        let v = r.classify(q(1));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 4_180,
                    ..
                }
            ),
            "{v:?}"
        );
        // Once it holds a lease again (a new body, a new view), it is.
        r.lease_landed_at(5_000, 1, 11_000, false);
        r.note_view_at(5_000, 1, Held::Lease, vec![], vec![], 0);
        let v = r.classify(Question::new(1, 5_000, 7_000));
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
        r.note_view_at(
            3_500,
            1,
            Held::Lease,
            vec![],
            vec![],
            BUSY_GATE | BUSY_FLOOR,
        );
        let v = r.classify(q(3));
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
        r.note_view_at(3_500, 1, Held::Lease, vec![], vec![], BUSY_GATE);
        r.note_view_at(6_900, 1, Held::Lease, vec![], vec![], 0);
        let v = r.classify(q(3));
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
        r.configure(cfg(1_000, 0));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_with(3_000, 1, 5_000, false, vec![2]);
        r.note_view_at(3_000, 1, Held::Lease, vec![], vec![], 0);
        r.attached_at(3_500, 1, false);
        let v = r.classify(q(3));
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
        let v = r.classify(q(2));
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
        // And what the slow lease reads add is part of the claim: its read
        // takes 2 080 ms, the TTL wait ends after the window.
        let r = Reach::default();
        r.configure(cfg(0, 2_000));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(3_000, 1, 5_000, false);
        r.attached_at(3_500, 1, false);
        let v = r.classify(q(3));
        assert!(v.no_sequencer_in_time(), "{v:?}");
    }

    /// Generation 7 of node 2, whose grant ended at `ends`, the keys of the
    /// window [4 000, 7 000] of node 3; node 1 is the root, up and holding the
    /// lease (renewed). `members` are the generation's group.
    fn expired(ends: u64, members: Vec<(u64, NodeId)>) -> Reach {
        let r = world();
        r.attached_at(0, 4, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(2_000, 2, Held::No, vec![(7, ends)], members, 0);
        r
    }

    #[test]
    fn an_expired_grant_whose_delegate_the_root_reaches_makes_the_root_the_sequencer() {
        // The grant ended at 3 900: the root is due to have ended the
        // generation at 3 900 + 310 = 4 210, and is the op's sequencer from
        // then on.
        let r = expired(3_900, vec![]);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_210,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // Before its allowance is over the op has no sequencer by design:
        // allowed, not isolated.
        let v = r.classify(Question::new(3, 4_000, 4_150).delegated(2, 7));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("4210"), "{why}");
        // The allowance leaves less than a retry and a round trip (120 ms):
        // none either.
        let v = r.classify(Question::new(3, 4_000, 4_300).delegated(2, 7));
        assert!(v.allowed(), "{v:?}");
        // A grant that ended long before: the root counts from the window's
        // start.
        let r = expired(3_000, vec![]);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // A root the requester cannot reach is no sequencer: that is the
        // requester's isolation, and stays so.
        r.partition_at(3_000, 3, 1, true);
        let v = r.classify(q(3).delegated(2, 7));
        let Verdict::Isolated { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("no live grant"), "{why}");
        // So is a root that cannot commit.
        let r = expired(3_000, vec![]);
        r.s3_cut_at(3_000, 1, true);
        assert!(r.classify(q(3).delegated(2, 7)).isolated());
    }

    #[test]
    fn an_expired_grant_whose_tail_the_root_cannot_reach_is_allowed_under_its_own_label() {
        // The generation has a group (node 4); the delegate is cut from the
        // root and so is its backup: the root cannot end the generation.
        let r = expired(3_000, vec![(7, 4)]);
        r.partition_at(3_100, 1, 2, true);
        r.partition_at(3_100, 1, 4, true);
        let v = r.classify(q(3).delegated(2, 7));
        let Verdict::GenerationTailUnreachable { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("generation 7"), "{why}");
        assert!(!r.classify(q(3).delegated(2, 7)).isolated());
        // A member the root reaches holds the tail: the root counts.
        let r = expired(3_000, vec![(7, 4)]);
        r.partition_at(3_100, 1, 2, true);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    ..
                }
            ),
            "{v:?}"
        );
        // And the delegate itself does, whatever the members.
        let r = expired(3_000, vec![(7, 4)]);
        r.partition_at(3_100, 1, 4, true);
        assert!(matches!(
            r.classify(q(3).delegated(2, 7)),
            Verdict::Reachable { .. }
        ));
        // A generation without a group has its tail nowhere else: the root
        // ends it without one, so a delegate it cannot reach does not excuse
        // it.
        let r = expired(3_000, vec![]);
        r.partition_at(3_100, 1, 2, true);
        assert!(matches!(
            r.classify(q(3).delegated(2, 7)),
            Verdict::Reachable {
                via: Via::Root(1),
                ..
            }
        ));
        // A delegate that is down and a member that is down: unreachable too.
        let r = expired(3_000, vec![(7, 4)]);
        r.attached_at(3_100, 2, false);
        r.attached_at(3_100, 4, false);
        assert!(r
            .classify(q(3).delegated(2, 7))
            .generation_tail_unreachable());
    }

    #[test]
    fn a_lingering_grant_does_not_postpone_the_roots_allowance() {
        // The delegate keeps listing generation 7 after its grant ended at
        // 3 200 (its core publishes a view at each step): the end stays 3 200,
        // the root is due at 3 510 and counts from the window's start.
        let r = expired(3_200, vec![]);
        for at in [3_400, 3_800, 4_200, 5_000] {
            r.note_view_at(at, 2, Held::No, vec![(7, 3_200)], vec![], 0);
        }
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // A grant never renewed (end 0): the root's own window, from the
        // install (the first view that lists it) or the birth: 600 ms and a
        // recall (installed at 4 300: due 5 000).
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(4_300, 2, Held::No, vec![(7, 0)], vec![], 0);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 5_000,
                    ..
                }
            ),
            "{v:?}"
        );
        let v = r.classify(Question::new(3, 4_000, 4_900).delegated(2, 7));
        assert!(v.no_sequencer_in_time(), "{v:?}");
    }

    #[test]
    fn a_generation_never_installed_is_ended_by_the_root_after_its_own_window() {
        // Born at 4 500 in the root's table, never in the delegate's views:
        // the root's window is 600 ms and a recall: due at 5 200.
        let r = world();
        r.lease_landed_at(3_500, 1, 9_000, false);
        let v = r.classify(q(3).delegated(2, 7).born_at(7, 4_500));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 5_200,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // Inside the window it has no sequencer by design.
        let v = r.classify(
            Question::new(3, 4_000, 5_100)
                .delegated(2, 7)
                .born_at(7, 4_500),
        );
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // Born long before: the root counts from the window's start.
        let v = r.classify(q(3).delegated(2, 7).born_at(7, 1_000));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // The root cut from the requester: isolated.
        r.partition_at(3_000, 3, 1, true);
        let v = r.classify(q(3).delegated(2, 7).born_at(7, 1_000));
        assert!(v.isolated(), "{v:?}");
        // The root that cannot reach the group's tail (the delegate never
        // installed, its backup is down): the strict wait.
        let r = world();
        r.attached_at(0, 4, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(2_000, 1, Held::Lease, vec![], vec![(7, 4)], 0);
        r.attached_at(3_100, 4, false);
        r.partition_at(3_100, 1, 2, true);
        let v = r.classify(q(3).delegated(2, 7).born_at(7, 1_000));
        assert!(v.generation_tail_unreachable(), "{v:?}");
    }

    #[test]
    fn a_successor_root_starts_the_windows_of_the_generations_it_inherits_anew() {
        // Node 1 took the lease over at 3 900 (the tenure of node 3 before
        // it): the generation's grant ended at 3 000, but the successor
        // outwaits its window from its own takeover, 3 900 + 600 + 100 =
        // 4 600.
        let r = world();
        r.lease_landed_at(500, 3, 1_500, false);
        r.lease_landed_at(1_400, 3, 2_400, false);
        r.lease_landed_at(3_900, 1, 9_000, false);
        r.note_view_at(3_900, 1, Held::Lease, vec![], vec![], 0);
        r.note_view_at(2_000, 2, Held::No, vec![(7, 3_000)], vec![], 0);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_600,
                    ..
                }
            ),
            "{v:?}"
        );
    }

    /// Keys across generations 7 (delegate 2) and 8 (delegate 4), blocked
    /// since before the window.
    fn blocked() -> Question {
        let mut q = q(3).born_at(7, 500).born_at(8, 700);
        q.owners = vec![(0, Owner::Blocked(vec![(2, 7), (4, 8)]))];
        q
    }

    #[test]
    fn keys_across_generations_are_the_roots_once_every_generations_own_allowance_is_over() {
        // Both grants ended long before the window (2 000 and 2 200): the
        // allowances (grant end + 2 x margin + delay + recall) were over at
        // 2 310 and 2 510. The root is reachable and linked to the delegates:
        // the root's from the window's start, however long the keys have
        // been blocked.
        let r = world();
        r.attached_at(0, 4, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        r.note_view_at(1_000, 4, Held::No, vec![(8, 2_200)], vec![], 0);
        let v = r.classify(blocked());
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
        // A delegate cut from the root (no group, so its tail is nowhere
        // else): the root ends the generation without it, the decision taken
        // (`tail_reachable`).
        r.partition_at(3_100, 1, 2, true);
        let v = r.classify(blocked());
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // One generation still inside its allowance: the root waits for the
        // last, here 4 800 + 200 + 10 + 100 = 5 110.
        let r = world();
        r.attached_at(0, 4, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        r.note_view_at(1_000, 4, Held::No, vec![(8, 4_800)], vec![], 0);
        let v = r.classify(blocked());
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 5_110,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // Inside the window the keys have no sequencer yet, by design.
        let mut early = Question::new(3, 4_000, 5_000)
            .born_at(7, 500)
            .born_at(8, 700);
        early.owners = blocked().owners;
        let Verdict::NoSequencerInTime { why } = r.classify(early) else {
            panic!("not no_sequencer_in_time")
        };
        assert!(why.contains("keys across generations"), "{why}");
    }

    #[test]
    fn keys_across_generations_whose_tail_cannot_be_reached_are_not_the_roots() {
        // Generation 7 has a group (node 5, down) and its delegate is cut from
        // the root; its allowance was over long before the window: the root
        // cannot end it without its tail.
        let r = world();
        r.attached_at(0, 4, true);
        r.attached_at(0, 5, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 2_000)], vec![(7, 5)], 0);
        r.note_view_at(1_000, 4, Held::No, vec![(8, 2_200)], vec![], 0);
        r.attached_at(3_100, 5, false);
        r.partition_at(3_100, 1, 2, true);
        let v = r.classify(blocked());
        assert!(v.generation_tail_unreachable(), "{v:?}");
        // The same group with a member up: the root has its tail.
        r.attached_at(3_200, 5, true);
        let v = r.classify(blocked());
        assert!(matches!(v, Verdict::Reachable { .. }), "{v:?}");
    }

    #[test]
    fn a_stretch_that_began_in_the_window_does_not_move_the_allowance() {
        // The keys became blocked at 4 500 (another owner's before): the
        // generations' own allowances were over long before, so the root is
        // the sequencer from the stretch's start, not a horizon after it.
        let r = world();
        r.attached_at(0, 4, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        r.note_view_at(1_000, 4, Held::No, vec![(8, 2_200)], vec![], 0);
        let mut q = blocked();
        q.owners = vec![
            (0, Owner::Delegate { node: 2, gen: 7 }),
            (4_500, Owner::Blocked(vec![(2, 7), (4, 8)])),
        ];
        let v = r.classify(q);
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
    }

    /// `world` with a 300 ms takeover slack.
    fn world_with_slack() -> Reach {
        let r = Reach::default();
        r.configure(ReachConfig {
            backup_takeover_ms: 300,
            ..cfg(0, 0)
        });
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(0, 1, 1_000, false);
        r.note_view_at(0, 1, Held::Lease, vec![], vec![], 0);
        r
    }

    /// The window [2 000, 5 000] of node 3 on keys of generation 7 (delegate
    /// 2, its grant ended at 2 000), born at `born`.
    fn gen7(born: u64) -> Question {
        Question::new(3, 2_000, 5_000)
            .delegated(2, 7)
            .born_at(7, born)
    }

    #[test]
    fn the_takeover_slack_is_added_to_the_generations_born_before_a_fast_takeover_only() {
        // Node 3 held the lease until 2 400; node 1 took it over at 1 900
        // (a seal: the body had not run out). Generation 7 was born at
        // 1 000, its grant ended at 2 000. The successor's window:
        // 1 900 + 600 + 300 (slack) = 2 800, and the recall: 2 900.
        let r = world_with_slack();
        r.lease_landed_at(500, 3, 1_500, false);
        r.lease_landed_at(1_400, 3, 2_400, false);
        r.lease_landed_at(1_900, 1, 9_000, false);
        r.note_view_at(1_200, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        let from = |r: &Reach, born: u64| match r.classify(gen7(born)) {
            Verdict::Reachable {
                via: Via::Root(1),
                from_ms,
                ..
            } => from_ms,
            v => panic!("{v:?}"),
        };
        assert_eq!(from(&r, 1_000), 2_900);
        // A generation born in the tenure was never inherited: its window is
        // its own, 2 000 + 600 + 100 (the grant's: 2 000 + 210 is less).
        assert_eq!(from(&r, 2_000), 2_700);
        // A takeover of a lease that had run out (no seal, no permit) adds no
        // slack: the predecessor's last body ended at 1 500, before the
        // landing.
        let r = world_with_slack();
        r.lease_landed_at(500, 3, 1_500, false);
        r.lease_landed_at(1_900, 1, 9_000, false);
        r.note_view_at(1_200, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        assert_eq!(from(&r, 1_000), 2_600);
        // Nor a released lease's.
        let r = world_with_slack();
        r.lease_landed_at(500, 3, 2_400, true);
        r.lease_landed_at(1_900, 1, 9_000, false);
        r.note_view_at(1_200, 2, Held::No, vec![(7, 2_000)], vec![], 0);
        assert_eq!(from(&r, 1_000), 2_600);
    }

    #[test]
    fn a_holder_that_lost_the_lease_and_acquired_it_again_inherits_every_generation_anew() {
        // Node 1 held the lease since 0 (generation 7 born at 500, its grant
        // ended at 1 500), lost it (its view drops at 2 500) and acquired it
        // again at 3 000, the same epoch. It dropped its delegation state and
        // inherits the generation at the sync: 3 000 + 600, the recall:
        // 3 700, and not the 1 810 of the generation's own window. No slack:
        // the lease it took was its own.
        let r = world_with_slack();
        r.lease_landed_at(900, 1, 1_900, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 1_500)], vec![], 0);
        r.note_view_at(2_500, 1, Held::No, vec![], vec![], 0);
        r.lease_landed_at(3_000, 1, 9_000, false);
        r.note_view_at(3_050, 1, Held::Lease, vec![], vec![], 0);
        let v = r.classify(
            Question::new(3, 3_000, 6_000)
                .delegated(2, 7)
                .born_at(7, 500),
        );
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 3_700,
                    ..
                }
            ),
            "{v:?}"
        );
        // A crash and restart between the bodies is the same.
        let r = world_with_slack();
        r.lease_landed_at(900, 1, 1_900, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 1_500)], vec![], 0);
        r.attached_at(2_500, 1, false);
        r.attached_at(2_800, 1, true);
        r.lease_landed_at(3_000, 1, 9_000, false);
        r.note_view_at(3_050, 1, Held::Lease, vec![], vec![], 0);
        let v = r.classify(
            Question::new(3, 3_000, 6_000)
                .delegated(2, 7)
                .born_at(7, 500),
        );
        assert!(
            matches!(v, Verdict::Reachable { from_ms: 3_700, .. }),
            "{v:?}"
        );
        // A renewal (the holder never let go) is the same tenure: the
        // generation, born in it, keeps its own window.
        let r = world_with_slack();
        r.lease_landed_at(900, 1, 1_900, false);
        r.lease_landed_at(1_800, 1, 2_800, false);
        r.lease_landed_at(2_700, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 1_500)], vec![], 0);
        let v = r.classify(
            Question::new(3, 3_000, 6_000)
                .delegated(2, 7)
                .born_at(7, 500),
        );
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 3_000,
                    ..
                }
            ),
            "{v:?}"
        );
    }

    #[test]
    fn a_lease_that_names_a_holder_with_nothing_is_the_lease_path_not_open_in_time() {
        // Node 1 renewed the lease to 9 000, then a claimant sealed it: it
        // holds nothing from 3 000 on (its view drops), and the body keeps
        // naming it. Nobody can claim before it runs out: not isolation, the
        // lease path is not open in time.
        let r = world();
        r.lease_landed_at(2_500, 1, 9_000, false);
        r.note_view_at(3_000, 1, Held::No, vec![], vec![], 0);
        let v = r.classify(q(3));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(
            why.contains("holds nothing") && why.contains("9000 ms"),
            "{why}"
        );
        // The same holder dead: the same.
        let r = world();
        r.lease_landed_at(2_500, 1, 9_000, false);
        r.attached_at(3_000, 1, false);
        assert!(r.classify(q(3)).no_sequencer_in_time());
        // A holder that holds the lease, cut from the requester: isolation.
        let r = world();
        r.lease_landed_at(2_500, 1, 9_000, false);
        r.partition_at(3_000, 3, 1, true);
        assert!(r.classify(q(3)).isolated());
        // A requester with its path to S3 cut: isolation, whoever holds.
        let r = world();
        r.lease_landed_at(2_500, 1, 9_000, false);
        r.note_view_at(3_000, 1, Held::No, vec![], vec![], 0);
        r.s3_cut_at(3_000, 3, true);
        assert!(r.classify(q(3)).isolated());
        // Keys of a live delegate the requester cannot reach: the delegate is
        // the sequencer, the lease has nothing to do with it.
        let r = world();
        r.lease_landed_at(2_500, 1, 9_000, false);
        r.note_view_at(3_000, 1, Held::No, vec![], vec![], 0);
        r.note_view_at(3_000, 2, Held::No, vec![(7, 9_000)], vec![], 0);
        r.partition_at(3_000, 3, 2, true);
        assert!(r.classify(q(3).delegated(2, 7)).isolated());
    }

    #[test]
    fn a_late_first_lease_read_does_not_excuse_the_product_s_delay() {
        // The dead root's body ran out at 3 500; a window of two seconds. A
        // read at the window's start would finish the claim by 5 180; the
        // requester's first read was served at 5 450 (a product delay), which
        // taken as the start of the TTL would make the claim too late.
        let r = world();
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        r.lease_read(3, 5_450, 5_500);
        let v = r.classify(Question::new(3, 4_000, 6_000));
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
        // An earlier read still starts the TTL there (and counts).
        let r = world();
        r.lease_landed_at(2_500, 1, 3_500, false);
        r.attached_at(3_000, 1, false);
        r.lease_read(3, 2_600, 2_650);
        let v = r.classify(Question::new(3, 4_000, 5_000));
        assert!(matches!(v, Verdict::Reachable { .. }), "{v:?}");
    }

    #[test]
    fn a_claims_fresh_read_slowed_past_the_window_is_no_sequencer_in_time() {
        // Plan 46 M0.3, cause 6: the dead root's body ran out at 3 500 and
        // the requester's own read at 2 650 started its TTL, so the claim is
        // ready from 3 650; the window is [4 000, 5 000]. The product claims
        // only on a read it handles after that: the requester issued it at
        // the window's start and a slow lease read answered it at 4 950, so
        // the promise check and the CAS end at 5 050, past the window.
        let dead_root = || {
            let r = world();
            r.lease_landed_at(2_500, 1, 3_500, false);
            r.attached_at(3_000, 1, false);
            r.lease_read(3, 2_600, 2_650);
            r
        };
        let r = dead_root();
        r.lease_read_sent(3, 4_000, 4_010, 4_950);
        let v = r.classify(Question::new(3, 4_000, 5_000));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("a fresh read answered at 4950"), "{why}");
        // A fresh read that came back in time leaves the claim possible.
        let r = dead_root();
        r.lease_read_sent(3, 4_000, 4_010, 4_300);
        assert!(
            matches!(
                r.classify(Question::new(3, 4_000, 5_000)),
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 4_400,
                    ..
                }
            ),
            "{:?}",
            r.classify(Question::new(3, 4_000, 5_000))
        );
        // A read the product issued late is excused only its own transit:
        // issued at 4 700 and answered at 4 990, it counts as answered at
        // 4 290, and the claim was possible by 4 390.
        let r = dead_root();
        r.lease_read_sent(3, 4_700, 4_710, 4_990);
        assert!(
            matches!(
                r.classify(Question::new(3, 4_000, 5_000)),
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 4_390,
                    ..
                }
            ),
            "{:?}",
            r.classify(Question::new(3, 4_000, 5_000))
        );
        // Another node's slow read is not the requester's.
        let r = dead_root();
        r.lease_read_sent(2, 4_000, 4_010, 4_950);
        assert!(matches!(
            r.classify(Question::new(3, 4_000, 5_000)),
            Verdict::Reachable { .. }
        ));
    }

    /// The claim of node 3 in the window [4 000, 5 000] is possible, done at
    /// `to_ms`, by the lease path.
    fn assert_claim_done_at(r: &Reach, to_ms: u64) {
        let v = r.classify(Question::new(3, 4_000, 5_000));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: t,
                    ..
                } if t == to_ms
            ),
            "{v:?}"
        );
    }

    #[test]
    fn a_claim_without_a_ttl_wait_adds_no_fresh_read() {
        // Plan 46 M0.3, the review of cause 6: only a claim by TTL waits for a
        // second read. A released lease, a lease nobody holds and no lease at
        // all go to the CAS after the first read (counted in `ready`), so a
        // slow lease read answered at 4 950 must not excuse them: they stay
        // a violation (a reachable lease path), done a claim's tail
        // (100 ms) after the start.
        let slow = |r: &Reach| r.lease_read_sent(3, 4_000, 4_010, 4_950);
        // Released: ready from the window's start.
        let r = world();
        r.lease_landed_at(2_500, 1, 3_500, true);
        r.attached_at(3_000, 1, false);
        r.lease_read(3, 2_600, 2_650);
        slow(&r);
        assert_claim_done_at(&r, 4_100);
        // Held by nobody (holder 0).
        let r = world();
        r.lease_landed_at(2_500, 0, 3_500, false);
        r.lease_read(3, 2_600, 2_650);
        slow(&r);
        assert_claim_done_at(&r, 4_100);
        // No lease at all: the first read is estimated (80 ms, two S3 hops
        // at the hi bound) from the run's start; the slow read after it is
        // not a second one.
        let r = Reach::default();
        r.configure(cfg(0, 0));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        slow(&r);
        assert_claim_done_at(&r, 4_180);
        // The same with a first read that was recorded before that estimate
        // (it was answered at 4 050, earlier than the hi bound): the recorded
        // read is not mistaken for a second one either.
        let r = Reach::default();
        r.configure(cfg(0, 0));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_read_sent(3, 4_000, 4_010, 4_050);
        slow(&r);
        assert_claim_done_at(&r, 4_180);
    }

    #[test]
    fn a_claim_by_ttl_counts_only_a_read_issued_from_its_start() {
        let dead_root = || {
            let r = world();
            r.lease_landed_at(2_500, 1, 3_500, false);
            r.attached_at(3_000, 1, false);
            r.lease_read(3, 2_600, 2_650);
            r
        };
        // A read issued before the claim's start (3 900 < 4 000) and slow
        // enough to be answered at 4 500 is not the claim's fresh read: the
        // fallback (no extra delay here) stands.
        let r = dead_root();
        r.lease_read_sent(3, 3_900, 3_910, 4_500);
        assert_claim_done_at(&r, 4_100);
        // A later one, issued at 4 200, is: answered at 4 600 it was 400 ms
        // in flight after its issue 200 ms past the start, so it counts as answered at 4 400 and the claim is done at 4 500.
        let r = dead_root();
        r.lease_read_sent(3, 4_200, 4_210, 4_600);
        assert_claim_done_at(&r, 4_500);
    }

    #[test]
    fn a_fresh_read_without_history_costs_the_fastest_read_the_configuration_allows() {
        // 100 ms of S3 at least and slow reads that add 600 ms at least: the
        // claim's fresh read is done 700 ms after its start (4 000), the claim
        // at 4 800: inside a window ending at 5 000, past one ending at 4 750.
        let dead_root = || {
            let r = world();
            r.configure(ReachConfig {
                s3_latency_lo_ms: 100,
                lease_read_extra_lo_ms: 600,
                ..cfg(0, 0)
            });
            r.lease_landed_at(2_500, 1, 3_500, false);
            r.attached_at(3_000, 1, false);
            r.lease_read(3, 2_600, 2_650);
            r
        };
        assert_claim_done_at(&dead_root(), 4_800);
        let v = dead_root().classify(Question::new(3, 4_000, 4_750));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("a fresh read answered at 4700"), "{why}");
    }

    #[test]
    fn a_member_restarted_with_its_journal_holds_the_tail_again() {
        // Node 1 held the lease with node 2 as its backup and died; node 2
        // crashed at 3 200 and came back at 3 400. Restarted fresh it lost
        // what it had not synced; restarted with its journal it answers for
        // the tail from its bootstrap on.
        let fresh = world();
        fresh.lease_landed_with(2_000, 1, 3_500, false, vec![2]);
        fresh.attached_at(3_000, 1, false);
        fresh.attached_at(3_200, 2, false);
        fresh.attached_at(3_400, 2, true);
        assert!(fresh.classify(q(3)).no_sequencer_in_time());
        let kept = world();
        kept.lease_landed_with(2_000, 1, 3_500, false, vec![2]);
        kept.attached_at(3_000, 1, false);
        kept.attached_at(3_200, 2, false);
        kept.attached_at(3_400, 2, true);
        kept.journal_kept_at(3_400, 2);
        let v = kept.classify(q(3));
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
        // (The journal recorded a millisecond after the attach, in the same
        // up-period, counts as well; one from a later up-period does not.)
        let late = world();
        late.lease_landed_with(2_000, 1, 3_500, false, vec![2]);
        late.attached_at(3_000, 1, false);
        late.attached_at(3_200, 2, false);
        late.attached_at(3_400, 2, true);
        late.journal_kept_at(3_401, 2);
        assert!(matches!(
            late.classify(q(3)),
            Verdict::Reachable {
                via: Via::LeasePath,
                ..
            }
        ));
        let stale = world();
        stale.lease_landed_with(2_000, 1, 3_500, false, vec![2]);
        stale.attached_at(3_000, 1, false);
        stale.attached_at(3_200, 2, false);
        stale.attached_at(3_400, 2, true);
        stale.attached_at(3_450, 2, false);
        stale.attached_at(3_500, 2, true);
        stale.journal_kept_at(3_500, 2);
        assert!(stale.classify(q(3)).no_sequencer_in_time());
        // A second crash, restarted fresh, loses it again.
        kept.attached_at(3_600, 2, false);
        kept.attached_at(3_700, 2, true);
        assert!(kept.classify(q(3)).no_sequencer_in_time());
    }

    /// Plan 46 M0.4 review, SHOULD-2: keys across live generations whose
    /// delegates the root reaches are recalled within a round trip and the
    /// resend back-off, not outwaited for the grants' windows.
    #[test]
    fn keys_across_live_generations_the_root_reaches_are_due_after_one_recall() {
        let live = || {
            let r = world();
            r.attached_at(0, 4, true);
            r.lease_landed_at(3_500, 1, 9_000, false);
            r.note_view_at(1_000, 2, Held::No, vec![(7, 9_000)], vec![], 0);
            r.note_view_at(1_000, 4, Held::No, vec![(8, 9_000)], vec![], 0);
            r
        };
        // Both grants live past the window (their windows end at 9 310), the
        // root reaches both delegates and the requester reaches the root: the
        // recall is due at 4 000 + 100 + 2 000 = 6 100, and the root is the
        // sequencer from then on (a violation; it was "none in time").
        let v = live().classify(blocked());
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 6_100,
                    to_ms: 7_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // A delegate the root cannot reach is outwaited for its window: none
        // in time, allowed.
        let r = live();
        r.partition_at(3_100, 1, 4, true);
        let v = r.classify(blocked());
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // Reached again from 5 000: the recall is due at 7 100, after the
        // window.
        r.partition_at(5_000, 1, 4, false);
        let v = r.classify(blocked());
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // A requester cut from the root is isolated, as before.
        let r = live();
        r.partition_at(3_100, 3, 1, true);
        let v = r.classify(blocked());
        assert!(v.isolated(), "{v:?}");
    }

    /// Plan 46 M0.4 review, SHOULD-3: a holder restarted inside its lease
    /// re-adopts it at once; one that was deposed before it went down, or
    /// that cannot reach S3, does not.
    #[test]
    fn a_restarted_holder_re_adopts_its_own_unexpired_lease() {
        let restarted = |busy: u8| {
            let r = Reach::default();
            r.configure(cfg(0, 0));
            for n in 1..=3 {
                r.attached_at(0, n, true);
            }
            r.lease_landed_at(2_900, 1, 8_000, false);
            r.note_view_at(0, 1, Held::Lease, vec![], vec![], busy);
            r.attached_at(3_000, 1, false);
            r.attached_at(3_100, 1, true);
            r
        };
        // Its own op: the lease path, at once (it was "none in time": the
        // body ran to 8 000).
        let v = restarted(0).classify(q(1));
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
        // Another node still waits for the body to run out.
        assert!(restarted(0).classify(q(3)).no_sequencer_in_time());
        // Deposed before it went down: no re-adoption.
        let v = restarted(BUSY_LOST).classify(q(1));
        assert!(v.no_sequencer_in_time(), "{v:?}");
        // Cut from S3 and from every node: isolated.
        let r = restarted(0);
        r.s3_cut_at(3_100, 1, true);
        r.partition_at(3_100, 1, 2, true);
        r.partition_at(3_100, 1, 3, true);
        let v = r.classify(q(1));
        assert!(v.isolated(), "{v:?}");
    }

    /// Plan 46 M0.4 review, SHOULD-4a: a generation's group is its latest
    /// version, not every member any view ever named.
    #[test]
    fn a_member_the_group_dropped_holds_no_tail_the_root_waits_for() {
        let r = world();
        r.attached_at(0, 5, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 3_000)], vec![(7, 5)], 0);
        // The delegate dropped its backup.
        r.note_view_at(2_000, 2, Held::No, vec![(7, 3_000)], vec![], 0);
        r.attached_at(3_100, 5, false);
        r.partition_at(3_100, 1, 2, true);
        // The grant ended at 3 000 (due at 3 310): the root ends the
        // generation without a tail (it was "generation tail unreachable").
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::Root(1),
                    from_ms: 4_000,
                    ..
                }
            ),
            "{v:?}"
        );
        // The latest version still names node 5 (down): the tail is
        // unreachable, allowed.
        r.note_view_at(2_500, 2, Held::No, vec![(7, 3_000)], vec![(7, 5)], 0);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(v.generation_tail_unreachable(), "{v:?}");
        // A root table that names the generation without a group is a
        // version too, when the delegate is gone.
        let r = world();
        r.attached_at(0, 5, true);
        r.lease_landed_at(3_500, 1, 9_000, false);
        r.note_view_at(1_000, 2, Held::No, vec![(7, 3_000)], vec![(7, 5)], 0);
        r.note_view_at(1_000, 1, Held::Lease, vec![], vec![(7, 5)], 0);
        r.note_view_at(2_000, 1, Held::Lease, vec![], vec![(7, 0)], 0);
        r.attached_at(3_100, 2, false);
        r.attached_at(3_100, 5, false);
        let v = r.classify(q(3).delegated(2, 7));
        assert!(matches!(v, Verdict::Reachable { .. }), "{v:?}");
    }

    /// Plan 46 M0.4 review, SHOULD-4b: a member reached between the claim's
    /// start and the window's end serves the group's tail.
    #[test]
    fn a_member_reached_after_the_claims_start_serves_the_tail() {
        let r = world();
        r.lease_landed_with(2_500, 1, 3_500, false, vec![2]);
        r.attached_at(3_000, 1, false);
        r.partition_at(3_000, 3, 2, true);
        // Cut for the whole window: none in time.
        assert!(r.classify(q(3)).no_sequencer_in_time());
        // Healed at 5 500, after the claim's start (5 080) and its end
        // (5 180): the tail is fetched by 5 520 (it was "none in time").
        r.partition_at(5_500, 3, 2, false);
        let v = r.classify(q(3));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 5_520,
                    ..
                }
            ),
            "{v:?}"
        );
        // Healed too late for a round trip inside the window: none in time.
        let r = world();
        r.lease_landed_with(2_500, 1, 3_500, false, vec![2]);
        r.attached_at(3_000, 1, false);
        r.partition_at(3_000, 3, 2, true);
        r.partition_at(6_990, 3, 2, false);
        assert!(r.classify(q(3)).no_sequencer_in_time());
    }

    /// Plan 46 M0.3, hang item 1 (`delegated-backup-and-delegate-die` seeds
    /// 214 and 1004): a requester whose only way is the lease path becomes the
    /// root by its claim, and ends the op's generation, as any root, only with
    /// a holder of its tail it reaches.
    #[test]
    fn a_claimant_root_needs_a_holder_of_the_generations_tail() {
        // Node 1, the root, is down from 500 (its lease ran out at 1 000);
        // generation 7 (delegate 2, member 4) ran out at 1 000 and is due at
        // 1 310; delegate and member are down from 1 100. Node 3 claims (done
        // at 5 180) and reaches no holder of the tail.
        let world = || {
            let r = world();
            r.attached_at(0, 4, true);
            r.note_view_at(500, 2, Held::No, vec![(7, 1_000)], vec![(7, 4)], 0);
            r.attached_at(500, 1, false);
            r.attached_at(1_100, 2, false);
            r
        };
        let r = world();
        r.attached_at(1_100, 4, false);
        let v = r.classify(q(3).delegated(2, 7));
        let Verdict::GenerationTailUnreachable { why } = v else {
            panic!("{v:?}")
        };
        assert!(
            why.contains("claimant 3") && why.contains("generation 7"),
            "{why}"
        );
        // The member is up and reachable: the claimant ends the generation.
        let r = world();
        let v = r.classify(q(3).delegated(2, 7));
        assert!(
            matches!(
                v,
                Verdict::Reachable {
                    via: Via::LeasePath,
                    to_ms: 5_180,
                    ..
                }
            ),
            "{v:?}"
        );
        // Cut from the claimant until 6 000, after the claim is done: reached
        // inside the window, so the claimant ends it.
        let r = world();
        r.partition_at(1_000, 3, 4, true);
        r.partition_at(6_000, 3, 4, false);
        assert!(
            matches!(
                r.classify(q(3).delegated(2, 7)),
                Verdict::Reachable {
                    via: Via::LeasePath,
                    ..
                }
            ),
            "the tail was reachable from the claimant once root"
        );
        // Cut for the whole window: unreachable.
        let r = world();
        r.partition_at(1_000, 3, 4, true);
        assert!(r
            .classify(q(3).delegated(2, 7))
            .generation_tail_unreachable());
        // A generation without a group has no tail elsewhere: nothing is
        // missing, the lease path counts.
        let r = Reach::default();
        r.configure(cfg(0, 0));
        for n in 1..=3 {
            r.attached_at(0, n, true);
        }
        r.lease_landed_at(0, 1, 1_000, false);
        r.note_view_at(0, 1, Held::Lease, vec![], vec![], 0);
        r.note_view_at(500, 2, Held::No, vec![(7, 1_000)], vec![], 0);
        r.attached_at(500, 1, false);
        r.attached_at(1_100, 2, false);
        assert!(matches!(
            r.classify(q(3).delegated(2, 7)),
            Verdict::Reachable {
                via: Via::LeasePath,
                ..
            }
        ));
    }

    /// Plan 46 M0.3, hang item 1 (`delegated-backup-and-delegate-die` seed
    /// 435): a claimant that is a member of the group whose tail its claim
    /// needs holds that tail only if it has it in place (`seal_tail_hold`).
    #[test]
    fn a_claimant_member_holds_the_tail_only_with_a_copy_in_place() {
        // Node 2 took the lease over (epoch 2) with node 1's epoch-1 tail
        // (members 3 and 2) and died at 3 000; node 3 claims.
        let world = |tails: Option<Vec<u64>>| {
            let r = world();
            r.lease_landed_span(LeaseSpan {
                at_ms: 2_500,
                holder: 2,
                ends_ms: 3_500,
                released: false,
                backups: vec![],
                inherited: vec![2, 3],
                epoch: 2,
                inherited_epoch: 1,
            });
            r.attached_at(3_000, 2, false);
            if let Some(t) = tails {
                r.note_tails_at(0, 3, t);
            }
            r
        };
        // It holds nothing of epoch 1 or 2: no member answers.
        let v = world(Some(vec![])).classify(q(3));
        let Verdict::NoSequencerInTime { why } = v else {
            panic!("{v:?}")
        };
        assert!(why.contains("holds no tail"), "{why}");
        // A copy of another epoch is no copy of these.
        assert!(world(Some(vec![5])).classify(q(3)).no_sequencer_in_time());
        // A copy of the inherited epoch, or of the lease's own, is the tail.
        for held in [vec![1], vec![2]] {
            let v = world(Some(held)).classify(q(3));
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
        }
        // Not known (nothing published): taken to hold it, as before.
        assert!(matches!(
            world(None).classify(q(3)),
            Verdict::Reachable { .. }
        ));
        // A copy dropped before the window: none in time.
        let r = world(Some(vec![1]));
        r.note_tails_at(3_900, 3, vec![]);
        assert!(r.classify(q(3)).no_sequencer_in_time());
    }
}
