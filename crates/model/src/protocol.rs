//! The `stateright::Model` implementation itself: state, actions,
//! transitions and properties for the "Today" authority protocol (plan
//! 30 §M1). See the crate-level docs for the action → code mapping table
//! and the list of deliberate simplifications.

use crate::namespace::{eval, force_apply, DirState, Errno, NamespaceSpec, NsOp, NsRet, Record};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stateright::{Model, Property};

pub type NodeId = u8;
pub type Epoch = u8;
pub type Seq = u8;
pub type Tick = u8;
pub type MsgId = u32;

/// A shipped log segment: `seq -> (epoch, records)` in the crate doc's
/// terms. Corresponds to the envelope `shipper.rs::encode`/`decode`
/// produce and to one `LogStore::put_segment` slot.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Segment {
    pub node: NodeId,
    pub epoch: Epoch,
    pub records: Vec<Record>,
}

/// The S3 lease register (`leases/p0.json`, `store-s3::Lease`), modeled
/// with etag-CAS semantics collapsed into "claimable" logic rather than
/// an explicit ETag value: any node attempting `AcquireLease` when the
/// register is claimable is exactly the CAS race the real object's ETag
/// arbitrates, so nothing is lost by not carrying a literal tag.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseReg {
    pub holder: Option<NodeId>,
    pub epoch: Epoch,
    pub expires_at: Tick,
    pub released: bool,
}

/// Where a node's own in-flight client op currently stands. Exactly one
/// `Invoke`/`Return` pair is recorded per op regardless of how many of
/// these phases it passes through: the retries below are invisible
/// machinery inside a single FUSE syscall (`mutate_op_rebasable` calls
/// forward *once*, then falls back to lease acquisition once), matching
/// what a real caller observes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Phase {
    /// Sent to `holder` as `MutateRequest{corr}`, awaiting `MutateReply`.
    WaitingReply { corr: MsgId, holder: NodeId },
    /// Forward failed/timed out (or there was no holder to ask): must
    /// acquire the lease, then execute locally.
    NeedsLease,
    /// Asked the (still-believed-live) holder for a fast handoff.
    WaitingHandoff { corr: MsgId, holder: NodeId },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientOp {
    pub op: NsOp,
    pub phase: Phase,
}

/// What a forwarded mutation request resolves to
/// (`forward.rs::MutateOutcome`, reduced: `Busy` and `NotHolder` are the
/// same fallback for the caller — see `fusefs.rs`'s `Busy | NotHolder`
/// arm — so this model only has `NotHolder`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Outcome {
    Accepted(Record),
    Errno(Errno),
    NotHolder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MsgBody {
    MutateReq {
        from: NodeId,
        to: NodeId,
        corr: MsgId,
        op: NsOp,
    },
    MutateRep {
        to: NodeId,
        corr: MsgId,
        outcome: Outcome,
    },
    HandoffReq {
        from: NodeId,
        to: NodeId,
        corr: MsgId,
    },
    HandoffRep {
        to: NodeId,
        corr: MsgId,
        ok: bool,
    },
}

/// One P2P message in flight. `id` is the transport-level identity used
/// to target this exact message for delivery or loss (the lossy,
/// reordering network); `corr`, carried inside the body, is the logical
/// request id a reply is matched against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Envelope {
    pub id: MsgId,
    pub body: MsgBody,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub alive: bool,
    pub paused: bool,
    /// This node's cached belief that it holds authority, at this epoch
    /// (`LeaseView`/`LeaseKeeper::ship_epoch`). Only ever invalidated by
    /// `Renew` noticing a mismatch against the shared register — a live
    /// node trusts its cache between renewals, exactly like the FUSE fast
    /// path trusts `LeaseView::usable()` without going to S3.
    pub held_epoch: Option<Epoch>,
    /// Highest log seq this node has tailed (`Meta::applied_seq`).
    pub applied_seq: Seq,
    /// Unshipped local records, in order (the fjall journal).
    pub journal: Vec<Record>,
    /// At most one outstanding shadow row (`shadow_insert`), matching the
    /// one-client-op-per-node simplification below.
    pub shadow: Option<Record>,
    pub client_op: Option<ClientOp>,
    /// Ops this node's (single, serialized) FUSE-calling client will
    /// still issue, oldest first.
    pub pending_ops: Vec<NsOp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HistEvt {
    Invoke(NodeId, NsOp),
    Return(NodeId, NsRet),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub tick: Tick,
    pub lease: LeaseReg,
    /// Slots `1..=max_seq`; index 0 is unused padding so `Seq` can serve
    /// directly as an index.
    pub log: Vec<Option<Segment>>,
    /// The last-published commit: `(published_state, claimed applied
    /// position)` (`mtree_publish::Commit`).
    pub commit: Option<(DirState, Seq)>,
    pub nodes: Vec<Node>,
    pub network: Vec<Envelope>,
    pub history: Vec<HistEvt>,
    pub next_id: MsgId,
    pub crashes_used: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    ClientInvoke(NodeId),
    DeliverForwardRequest(MsgId),
    DeliverForwardReply(MsgId),
    ForwardTimeout(NodeId),
    RequestHandoff(NodeId),
    DeliverHandoffRequest(MsgId),
    DeliverHandoffReply(MsgId),
    Tail(NodeId),
    Ship(NodeId),
    Renew(NodeId),
    AcquireLease(NodeId),
    Publish(NodeId),
    Crash(NodeId),
    Restart(NodeId),
    Pause(NodeId),
    Resume(NodeId),
    DropMessage(MsgId),
}

/// Selects the modeled protocol variant. Only `Today` exists in M1; later
/// milestones (per plan 30 §4) add siblings here rather than replacing
/// it, each changing a specific, documented subset of the transitions
/// below:
/// - `ExactlyOnce` (M2) would add a `Rid`/`completed` table consulted at
///   the top of `DeliverForwardRequest` and checked before `AcquireLease`
///   re-executes a `NeedsLease` op — see the comments at both call sites.
/// - `Recovery` (M3) would replace "stranded journal never resolves"
///   (the `Renew` deposition arm, and `Crash`) with an explicit
///   reintegration action.
/// - `Positions`/`Backup`/`FlexEpochs`/`Delegation` (M6/M9/M10/M11) each
///   extend `State`/`Action` further; none are added speculatively here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    Today,
}

/// A model instance: which protocol variant, how many nodes, the bounds
/// that keep the state space finite, and the fixed client workload.
#[derive(Clone, Debug)]
pub struct AuthorityModel {
    pub protocol: Protocol,
    pub n_nodes: u8,
    pub max_tick: Tick,
    pub max_seq: Seq,
    pub lease_ttl: Tick,
    pub max_crashes: u8,
    pub allow_restart: bool,
    pub allow_pause: bool,
    pub allow_lossy: bool,
    pub initial_holder: Option<NodeId>,
    pub initial_expiry: Tick,
    pub workload: Vec<(NodeId, NsOp)>,
}

impl AuthorityModel {
    pub fn new(n_nodes: u8) -> Self {
        Self {
            protocol: Protocol::Today,
            n_nodes,
            max_tick: 4,
            max_seq: 3,
            lease_ttl: 2,
            max_crashes: 0,
            allow_restart: false,
            allow_pause: false,
            allow_lossy: true,
            initial_holder: None,
            initial_expiry: 0,
            workload: Vec::new(),
        }
    }

    pub fn with_initial_holder(mut self, id: NodeId, expiry: Tick) -> Self {
        self.initial_holder = Some(id);
        self.initial_expiry = expiry;
        self
    }

    pub fn with_max_tick(mut self, t: Tick) -> Self {
        self.max_tick = t;
        self
    }

    pub fn with_max_seq(mut self, s: Seq) -> Self {
        self.max_seq = s;
        self
    }

    pub fn with_lease_ttl(mut self, t: Tick) -> Self {
        self.lease_ttl = t;
        self
    }

    pub fn with_max_crashes(mut self, c: u8) -> Self {
        self.max_crashes = c;
        self
    }

    pub fn with_restart(mut self, b: bool) -> Self {
        self.allow_restart = b;
        self
    }

    pub fn with_pause(mut self, b: bool) -> Self {
        self.allow_pause = b;
        self
    }

    pub fn with_lossy(mut self, b: bool) -> Self {
        self.allow_lossy = b;
        self
    }

    pub fn with_op(mut self, node: NodeId, op: NsOp) -> Self {
        self.workload.push((node, op));
        self
    }
}

fn is_live(state: &State, id: NodeId) -> bool {
    let n = &state.nodes[id as usize];
    n.alive && !n.paused
}

/// Replay `log[1..=upto]` applying epoch fencing the way
/// `shipper.rs::apply_decoded_segment` does: a segment whose epoch is
/// lower than the highest epoch already applied is skipped (but still
/// counts toward that running maximum — a no-op in practice since it was
/// already below it). Returns the resulting directory state and the
/// running maximum epoch (the latter is what a node's own tailer would
/// have in `self.parts[part].max_epoch`).
fn log_state_and_epoch(log: &[Option<Segment>], upto: Seq) -> (DirState, Epoch) {
    let mut dir: DirState = 0;
    let mut max_epoch: Epoch = 0;
    for seq in 1..=upto {
        if let Some(seg) = &log[seq as usize] {
            let fenced = seg.epoch > 0 && seg.epoch < max_epoch;
            if !fenced {
                for r in &seg.records {
                    dir = force_apply(dir, *r);
                }
                max_epoch = max_epoch.max(seg.epoch);
            }
        }
    }
    (dir, max_epoch)
}

/// The highest occupied log slot. Always contiguous from 1 (see the
/// crate doc: every successful `put_segment` targets the writer's own
/// `applied_seq + 1`, and a collision is absorbed by tailing instead of
/// writing elsewhere), so this doubles as "how many slots exist".
fn log_head(log: &[Option<Segment>]) -> Seq {
    log.iter()
        .rposition(|s| s.is_some())
        .map(|i| i as Seq)
        .unwrap_or(0)
}

/// What a node's replica currently shows: the log-derived state at its
/// own `applied_seq`, overlaid with its pending shadow (a forwarded op's
/// reply, applied ahead of the log — `forward.rs::apply_accepted`) and
/// its own unshipped journal (a holder's local writes not yet shipped).
fn node_replica(state: &State, id: NodeId) -> DirState {
    let node = &state.nodes[id as usize];
    let (mut dir, _) = log_state_and_epoch(&state.log, node.applied_seq);
    if let Some(r) = node.shadow {
        dir = force_apply(dir, r);
    }
    for r in &node.journal {
        dir = force_apply(dir, *r);
    }
    dir
}

/// Apply exactly the next log slot to `id`'s tailer state
/// (`shipper.rs::apply_decoded_segment`): advance `applied_seq`, and, if
/// the segment was not fenced out, retire a matching shadow row
/// (`shadow_retire_matching`). Panics if there is no next slot to apply;
/// callers only invoke this when `log_head > applied_seq`.
fn tail_one(s: &mut State, id: NodeId) {
    let node = &s.nodes[id as usize];
    let (_, max_before) = log_state_and_epoch(&s.log, node.applied_seq);
    let target = node.applied_seq + 1;
    let seg = s.log[target as usize]
        .clone()
        .expect("caller checked log_head > applied_seq");
    let fenced = seg.epoch > 0 && seg.epoch < max_before;
    s.nodes[id as usize].applied_seq = target;
    if !fenced {
        if let Some(sh) = s.nodes[id as usize].shadow {
            if seg.records.contains(&sh) {
                s.nodes[id as usize].shadow = None;
            }
        }
    }
}

/// Attempt to ship `id`'s whole journal as one segment
/// (`shipper.rs::ship_part`'s create-if-absent `put_segment`). A
/// collision (someone else already filled the target slot — a deposed
/// holder racing a new one) is absorbed by tailing that slot instead,
/// exactly once, matching `Err(AlreadyExists) => { self.tail_part(...); }`.
/// Returns whether the journal ended up empty (shipped, or already was).
fn try_ship_once(s: &mut State, id: NodeId, max_seq: Seq) -> bool {
    let node = &s.nodes[id as usize];
    if node.journal.is_empty() {
        return true;
    }
    let target = node.applied_seq + 1;
    if target > max_seq {
        return false;
    }
    if s.log[target as usize].is_none() {
        let epoch = s.nodes[id as usize]
            .held_epoch
            .expect("Ship requires holding");
        let records = std::mem::take(&mut s.nodes[id as usize].journal);
        s.log[target as usize] = Some(Segment {
            node: id,
            epoch,
            records,
        });
        s.nodes[id as usize].applied_seq = target;
        true
    } else {
        tail_one(s, id);
        s.nodes[id as usize].journal.is_empty()
    }
}

/// A handoff's inline flush: keep absorbing collisions and re-attempting
/// the ship until the journal is empty or the log is full
/// (`node_runtime.rs`'s `HandOff` arm: `ship.sync_one` then `release()`).
fn flush_for_handoff(s: &mut State, id: NodeId, max_seq: Seq) -> bool {
    loop {
        if s.nodes[id as usize].journal.is_empty() {
            return true;
        }
        let before = s.nodes[id as usize].applied_seq;
        if !try_ship_once(s, id, max_seq) && s.nodes[id as usize].applied_seq == before {
            // Log is full and no progress was made: decline the handoff,
            // keeping the lease ("declining is always safe").
            return false;
        }
    }
}

/// Whether `id` may claim the lease right now, and if so whether it is a
/// genuine takeover requiring tail-to-head first
/// (`LeaseKeeper::classify`'s `Plan::Claim{needs_tail}` /
/// `TailedToHead`). `None` means not claimable at all (someone else holds
/// an unexpired, unreleased lease).
fn claim_kind(state: &State, id: NodeId) -> Option<bool> {
    match state.lease.holder {
        None => Some(false),
        Some(h) if h == id && !state.lease.released => Some(false),
        Some(_) => {
            if state.lease.released || state.lease.expires_at <= state.tick {
                Some(true)
            } else {
                None
            }
        }
    }
}

impl Model for AuthorityModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let mut nodes = Vec::new();
        for _ in 0..self.n_nodes {
            nodes.push(Node {
                alive: true,
                paused: false,
                held_epoch: None,
                applied_seq: 0,
                journal: Vec::new(),
                shadow: None,
                client_op: None,
                pending_ops: Vec::new(),
            });
        }
        for (n, op) in &self.workload {
            nodes[*n as usize].pending_ops.push(*op);
        }
        let lease = if let Some(h) = self.initial_holder {
            nodes[h as usize].held_epoch = Some(1);
            LeaseReg {
                holder: Some(h),
                epoch: 1,
                expires_at: self.initial_expiry,
                released: false,
            }
        } else {
            LeaseReg {
                holder: None,
                epoch: 0,
                expires_at: 0,
                released: false,
            }
        };
        vec![State {
            tick: 0,
            lease,
            log: vec![None; self.max_seq as usize + 1],
            commit: None,
            nodes,
            network: Vec::new(),
            history: Vec::new(),
            next_id: 0,
            crashes_used: 0,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.tick < self.max_tick {
            actions.push(Action::Tick);
        }
        for (i, node) in state.nodes.iter().enumerate() {
            let id = i as NodeId;
            if !node.alive {
                if self.allow_restart {
                    actions.push(Action::Restart(id));
                }
                continue;
            }
            if node.paused {
                actions.push(Action::Resume(id));
                continue;
            }
            if node.client_op.is_none() && !node.pending_ops.is_empty() {
                actions.push(Action::ClientInvoke(id));
            }
            if let Some(cop) = &node.client_op {
                match &cop.phase {
                    Phase::WaitingReply { .. } => actions.push(Action::ForwardTimeout(id)),
                    Phase::NeedsLease => {
                        match claim_kind(state, id) {
                            Some(needs_tail) => {
                                if !needs_tail || node.applied_seq == log_head(&state.log) {
                                    actions.push(Action::AcquireLease(id));
                                }
                            }
                            // Genuinely busy (someone else holds an
                            // unexpired, unreleased lease): the fast
                            // handoff path is only ever worth asking for
                            // here (`node_runtime.rs`: handoff is
                            // requested precisely when the plain acquire
                            // attempt came back `Busy`). Offering it
                            // whenever a holder merely *exists* — even
                            // one already released/expired and hence
                            // directly claimable — would let a declined
                            // or pointless handoff round be requested
                            // forever, since each round mints a fresh
                            // message id and so a fresh, never-before-
                            // seen state; gating on genuine business
                            // keeps the reachable state space finite.
                            None => {
                                if let Some(h) = state.lease.holder {
                                    if h != id {
                                        actions.push(Action::RequestHandoff(id));
                                    }
                                }
                            }
                        }
                    }
                    Phase::WaitingHandoff { .. } => {}
                }
            }
            if node.held_epoch.is_some()
                && !node.journal.is_empty()
                && node.applied_seq < self.max_seq
            {
                actions.push(Action::Ship(id));
            }
            if node.held_epoch.is_some() {
                actions.push(Action::Renew(id));
            }
            if log_head(&state.log) > node.applied_seq {
                actions.push(Action::Tail(id));
            }
            // `mtree_publish` runs on the same per-partition sync cycle
            // that ships first (`node_runtime.rs`'s round: `ship_all`,
            // then, on its own idle cadence, `publish`), so by the time
            // an honest, still-acting node publishes, its own journal
            // has already been drained. Gating on an empty journal here
            // approximates that ordering rather than treating `Publish`
            // as fully independent of it — without this, *any* node
            // (even a lone, never-forwarded-to holder) could publish the
            // instant after a local `execute_mutate`, before ever
            // shipping, which is a real defect (plan 30 §1.1: "the
            // holder side has the same shape") but not the one
            // `single_writer_is_clean` exists to rule out. A lingering
            // *shadow* (the actual bug B shape) is untouched by this
            // gate and still reaches `Publish`.
            if node.journal.is_empty() {
                actions.push(Action::Publish(id));
            }
            if self.max_crashes > state.crashes_used {
                actions.push(Action::Crash(id));
            }
            if self.allow_pause {
                actions.push(Action::Pause(id));
            }
        }
        for env in &state.network {
            let deliverable = match env.body {
                MsgBody::MutateReq { to, .. } => is_live(state, to),
                MsgBody::MutateRep { to, .. } => is_live(state, to),
                MsgBody::HandoffReq { to, .. } => is_live(state, to),
                MsgBody::HandoffRep { to, .. } => is_live(state, to),
            };
            if deliverable {
                let a = match env.body {
                    MsgBody::MutateReq { .. } => Action::DeliverForwardRequest(env.id),
                    MsgBody::MutateRep { .. } => Action::DeliverForwardReply(env.id),
                    MsgBody::HandoffReq { .. } => Action::DeliverHandoffRequest(env.id),
                    MsgBody::HandoffRep { .. } => Action::DeliverHandoffReply(env.id),
                };
                actions.push(a);
            }
            if self.allow_lossy {
                actions.push(Action::DropMessage(env.id));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut s = state.clone();
        match action {
            Action::Tick => {
                s.tick += 1;
            }
            Action::ClientInvoke(id) => {
                let op = s.nodes[id as usize].pending_ops.remove(0);
                s.history.push(HistEvt::Invoke(id, op));
                let held = s.nodes[id as usize].held_epoch;
                if held.is_some() {
                    // Fast path: `fusefs.rs::mutate_op_rebasable`'s
                    // `view.open_for_new_mutation()` branch — a
                    // synchronous local `execute_mutate`, no yield point.
                    let base = node_replica(&s, id);
                    let (ret, _) = eval(base, op);
                    if ret == NsRet::Ok {
                        s.nodes[id as usize].journal.push(op);
                    }
                    s.history.push(HistEvt::Return(id, ret));
                } else if let Some(h) = s.lease.holder {
                    // `forward.rs::request_mutate_with`: send once, block
                    // on the reply (modeled as entering `WaitingReply`).
                    let corr = s.next_id;
                    s.next_id += 1;
                    s.network.push(Envelope {
                        id: corr,
                        body: MsgBody::MutateReq {
                            from: id,
                            to: h,
                            corr,
                            op,
                        },
                    });
                    s.nodes[id as usize].client_op = Some(ClientOp {
                        op,
                        phase: Phase::WaitingReply { corr, holder: h },
                    });
                } else {
                    // No known holder to forward to at all: straight to
                    // lease acquisition.
                    s.nodes[id as usize].client_op = Some(ClientOp {
                        op,
                        phase: Phase::NeedsLease,
                    });
                }
            }
            Action::DeliverForwardRequest(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (from, to, op) = match env.body {
                    MsgBody::MutateReq { from, to, op, .. } => (from, to, op),
                    _ => return None,
                };
                let corr = match env.body {
                    MsgBody::MutateReq { corr, .. } => corr,
                    _ => return None,
                };
                // M2 `ExactlyOnce` would check a `completed`/in-flight-
                // outcome table here before calling `eval` again, so a
                // retried rid is answered from the recorded outcome
                // instead of being re-executed (`forward.rs::holder_execute`'s
                // "an executed rid is never executed again").
                let held = s.nodes[to as usize].held_epoch;
                let outcome = if held.is_some() {
                    let base = node_replica(&s, to);
                    let (ret, _) = eval(base, op);
                    match ret {
                        NsRet::Ok => {
                            s.nodes[to as usize].journal.push(op);
                            Outcome::Accepted(op)
                        }
                        NsRet::Err(e) => Outcome::Errno(e),
                    }
                } else {
                    Outcome::NotHolder
                };
                let rep_id = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: rep_id,
                    body: MsgBody::MutateRep {
                        to: from,
                        corr,
                        outcome,
                    },
                });
            }
            Action::DeliverForwardReply(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (to, corr, outcome) = match env.body {
                    MsgBody::MutateRep { to, corr, outcome } => (to, corr, outcome),
                    _ => return None,
                };
                let mut completed = None;
                if let Some(cop) = s.nodes[to as usize].client_op.clone() {
                    if let Phase::WaitingReply { corr: waiting, .. } = cop.phase {
                        if waiting == corr {
                            match outcome {
                                Outcome::Accepted(rec) => {
                                    // `forward.rs::apply_accepted`: shadow
                                    // insert + apply immediately, ahead of
                                    // the log.
                                    s.nodes[to as usize].shadow = Some(rec);
                                    completed = Some(NsRet::Ok);
                                }
                                Outcome::Errno(e) => completed = Some(NsRet::Err(e)),
                                Outcome::NotHolder => {
                                    s.nodes[to as usize].client_op.as_mut().unwrap().phase =
                                        Phase::NeedsLease;
                                }
                            }
                        }
                    }
                }
                if let Some(ret) = completed {
                    s.history.push(HistEvt::Return(to, ret));
                    s.nodes[to as usize].client_op = None;
                }
            }
            Action::ForwardTimeout(id) => {
                // `forward.rs::request_mutate_with`'s `tokio::time::timeout`
                // firing before the reply arrives: the pending `MutateReq`/
                // `MutateRep` in `s.network` is simply left to be delivered
                // (and ignored, since `client_op` has moved on) or dropped.
                if let Some(cop) = &mut s.nodes[id as usize].client_op {
                    if matches!(cop.phase, Phase::WaitingReply { .. }) {
                        cop.phase = Phase::NeedsLease;
                    }
                }
            }
            Action::RequestHandoff(id) => {
                let h = s.lease.holder?;
                let corr = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: corr,
                    body: MsgBody::HandoffReq {
                        from: id,
                        to: h,
                        corr,
                    },
                });
                if let Some(cop) = &mut s.nodes[id as usize].client_op {
                    cop.phase = Phase::WaitingHandoff { corr, holder: h };
                }
            }
            Action::DeliverHandoffRequest(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (from, to, corr) = match env.body {
                    MsgBody::HandoffReq { from, to, corr } => (from, to, corr),
                    _ => return None,
                };
                let ok = if s.nodes[to as usize].held_epoch.is_some() {
                    if flush_for_handoff(&mut s, to, self.max_seq) {
                        s.nodes[to as usize].held_epoch = None;
                        s.lease.released = true;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                let rep_id = s.next_id;
                s.next_id += 1;
                s.network.push(Envelope {
                    id: rep_id,
                    body: MsgBody::HandoffRep { to: from, corr, ok },
                });
            }
            Action::DeliverHandoffReply(mid) => {
                let idx = s.network.iter().position(|e| e.id == mid)?;
                let env = s.network.remove(idx);
                let (to, corr) = match env.body {
                    MsgBody::HandoffRep { to, corr, .. } => (to, corr),
                    _ => return None,
                };
                if let Some(cop) = &mut s.nodes[to as usize].client_op {
                    if let Phase::WaitingHandoff { corr: waiting, .. } = cop.phase {
                        if waiting == corr {
                            // Whether the handoff succeeded or was
                            // declined, the requester falls back to the
                            // ordinary CAS-based acquire
                            // (`shipper::acquire_lease_for`); a decline
                            // just means it may have to wait out the TTL.
                            cop.phase = Phase::NeedsLease;
                        }
                    }
                }
            }
            Action::Tail(id) => {
                tail_one(&mut s, id);
            }
            Action::Ship(id) => {
                try_ship_once(&mut s, id, self.max_seq);
            }
            Action::Renew(id) => {
                let epoch = s.nodes[id as usize]
                    .held_epoch
                    .expect("Renew requires holding");
                let still_ours =
                    s.lease.holder == Some(id) && s.lease.epoch == epoch && !s.lease.released;
                if still_ours {
                    s.lease.expires_at = s.tick + self.lease_ttl;
                } else {
                    // `diagnose_lost_renew` -> `mark_lost`: deposition is
                    // terminal, the journal is stranded.
                    s.nodes[id as usize].held_epoch = None;
                }
            }
            Action::AcquireLease(id) => {
                let new_epoch = match s.lease.holder {
                    None => 1,
                    Some(h) if h == id && !s.lease.released => s.lease.epoch.max(1),
                    _ => s.lease.epoch + 1,
                };
                s.lease = LeaseReg {
                    holder: Some(id),
                    epoch: new_epoch,
                    expires_at: s.tick + self.lease_ttl,
                    released: false,
                };
                s.nodes[id as usize].held_epoch = Some(new_epoch);
                // `mutate_op_rebasable`: `require_lease_for` succeeding
                // falls straight into `execute_mutate`, synchronously.
                // M2 `ExactlyOnce` would first check whether this rid was
                // already completed by a stale forward before re-running
                // `eval` here.
                if let Some(cop) = s.nodes[id as usize].client_op.clone() {
                    let base = node_replica(&s, id);
                    let (ret, _) = eval(base, cop.op);
                    if ret == NsRet::Ok {
                        s.nodes[id as usize].journal.push(cop.op);
                    }
                    s.history.push(HistEvt::Return(id, ret));
                    s.nodes[id as usize].client_op = None;
                }
            }
            Action::Publish(id) => {
                // `mtree_publish::TreePublisher::publish`: commits from
                // the current replica (dirty-key derived in the real
                // code; here, the full node_replica, which — faithfully —
                // includes any lingering shadow, since mtree_publish has
                // "no notion of shadows").
                let dir = node_replica(&s, id);
                let applied = s.nodes[id as usize].applied_seq;
                s.commit = Some((dir, applied));
            }
            Action::Crash(id) => {
                s.nodes[id as usize].alive = false;
                s.crashes_used += 1;
            }
            Action::Restart(id) => {
                // Durable journal/applied_seq/shadow survive; authority
                // does not (`bootstrap()` starts a fresh `LeaseKeeper`).
                s.nodes[id as usize].alive = true;
                s.nodes[id as usize].paused = false;
                s.nodes[id as usize].held_epoch = None;
            }
            Action::Pause(id) => {
                s.nodes[id as usize].paused = true;
            }
            Action::Resume(id) => {
                s.nodes[id as usize].paused = false;
            }
            Action::DropMessage(mid) => {
                s.network.retain(|e| e.id != mid);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("linearizable", prop_linearizable),
            Property::always("converged_at_quiescence", prop_converged_at_quiescence),
            Property::always("commits_are_log_prefixes", prop_commits_are_log_prefixes),
            Property::sometimes("progress", prop_progress),
        ]
    }

    /// A defensive backstop, not a load-bearing part of the model: every
    /// field the transitions above touch is already bounded by a config
    /// value (workload size, `max_tick`, `max_seq`, `max_crashes`), so
    /// the reachable state space is finite without this. It exists so
    /// that a future protocol variant with a genuine "keep retrying
    /// forever" edge (e.g. a handoff that always declines because the
    /// log is full) fails a bounded exploration loudly instead of
    /// hanging the checker.
    fn within_boundary(&self, state: &State) -> bool {
        state.next_id < 200 && state.history.len() < 64
    }
}

fn prop_linearizable(_m: &AuthorityModel, s: &State) -> bool {
    let mut tester = LinearizabilityTester::<NodeId, NamespaceSpec>::new(NamespaceSpec::default());
    for evt in &s.history {
        let r = match evt {
            HistEvt::Invoke(n, op) => tester.on_invoke(*n, *op),
            HistEvt::Return(n, ret) => tester.on_return(*n, *ret),
        };
        if r.is_err() {
            return false;
        }
    }
    tester.is_consistent()
}

fn no_client_op_in_flight(s: &State) -> bool {
    s.nodes.iter().all(|n| n.client_op.is_none())
}

/// "Every durable slot is applied on every live node" — but a live node
/// whose *own* journal still has unshipped records is not quiescent
/// either (it will ship them momentarily; that is ordinary operation,
/// not the stranding bug B is about). A crashed holder's stranded
/// journal is exactly what should be allowed to keep this false forever
/// — which is why the check is skipped for nodes that are not alive.
fn every_durable_slot_applied_everywhere(s: &State) -> bool {
    let head = log_head(&s.log);
    s.nodes
        .iter()
        .all(|n| !n.alive || (n.applied_seq == head && n.journal.is_empty()))
}

fn prop_converged_at_quiescence(_m: &AuthorityModel, s: &State) -> bool {
    if !no_client_op_in_flight(s) || !every_durable_slot_applied_everywhere(s) {
        return true;
    }
    let head = log_head(&s.log);
    let (log_dir, _) = log_state_and_epoch(&s.log, head);
    s.nodes
        .iter()
        .enumerate()
        .all(|(i, n)| !n.alive || node_replica(s, i as NodeId) == log_dir)
}

fn prop_commits_are_log_prefixes(_m: &AuthorityModel, s: &State) -> bool {
    match &s.commit {
        None => true,
        Some((published, applied)) => {
            let (derived, _) = log_state_and_epoch(&s.log, *applied);
            *published == derived
        }
    }
}

fn prop_progress(m: &AuthorityModel, s: &State) -> bool {
    let total_ops = m.workload.len();
    let completed = s
        .history
        .iter()
        .filter(|e| matches!(e, HistEvt::Return(..)))
        .count();
    completed == total_ops
        && s.nodes
            .iter()
            .all(|n| n.client_op.is_none() && n.pending_ops.is_empty())
}
