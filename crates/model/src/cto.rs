//! Plan 30 §M8: `cto=strict` — ReadIndex, read delegations with recall,
//! and the close-to-open property, over clocks with bounded drift.
//!
//! # Why a model of its own
//!
//! The authority model ([`crate::protocol`]) already checks everything
//! the lease, the log, forwarding, stranding and positions do. What M8
//! adds is orthogonal to all of that and needs something that model
//! deliberately leaves out: **clocks**. A read delegation is a
//! time-bounded promise, and whether it is safe depends on how two
//! nodes' clocks can disagree. Adding per-node clocks, delegation tables
//! and recalls to the authority model would multiply a state space that
//! already needs 30M states for its M6 configurations, so this module is
//! a second, focused `stateright::Model` over the same abstractions:
//!
//! - the durable log, with the takeover's epoch marker (an entry of the
//!   new epoch) and one file `X` whose every write is a new *version*;
//! - each node's replica as an applied prefix of that log;
//! - the lease register (holder, epoch, expiry in the holder's clock,
//!   released), renewals and takeovers, with exactly the lease's margin
//!   discipline (`LeaseView::usable`: a holder uses its lease only while
//!   `local < expires − lease_margin`; anyone may take over once its own
//!   `local ≥ expires`);
//! - a global real time `t` (a bounded [`Action::Tick`]) and, per node, a
//!   clock `local = t + off[i]` with `|off[i]| ≤ max_offset` chosen
//!   nondeterministically at start, and optionally stepping within that
//!   bound ([`Action::Jump`]: an NTP step, or a rate error accumulated
//!   over a delegation's lifetime);
//! - a lossy, reordering network (an unreachable delegate is a dropped
//!   recall or recall ack).
//!
//! What it abstracts away, each justified by the authority model: a
//! write is durable at execution (the L2 window of unshipped acks is M9's,
//! and M6's `Tentative` rule already exempts it from every session
//! property); only the current epoch's holder may append (the log's
//! fencing); stranding and replay are not modeled (they are M3's and the
//! authority model's).
//!
//! # The protocol (what `crates/authority/src/core/readindex.rs` does)
//!
//! - **ReadIndex.** A strict read on a node that is not a usable holder
//!   asks the register's holder for its position. A usable holder
//!   answers with its applied position (it is authoritative); the reader
//!   waits until its own replica reaches it, then reads.
//! - **Grant.** With the answer the holder may grant a read delegation:
//!   `ttl = min(deleg_ttl, lease_expires − lease_margin − now)` (the grant
//!   never outlives the lease that backs it, in the holder's clock), and
//!   it records the grant as live until `now + ttl + seq_margin`.
//! - **Honour.** The reader measures from when it *sent* the request:
//!   it serves reads locally under the delegation only while `local <
//!   sent + ttl − deleg_margin`, and always waits for the grant's
//!   position first.
//! - **Recall.** A holder that executed a write touching `X` withholds
//!   the write's acknowledgement (the close's completion) until every
//!   grant on `X` held by a node other than the writer is recalled (a
//!   `Recall` answered by `RecallAck`; the delegate drops the delegation
//!   before acking) or has expired by the holder's clock.
//! - **Overtaken grants.** A recall can overtake the reply carrying the
//!   grant it recalls (grant, then a write executes and recalls, and the
//!   network reorders). The delegate counts the recalls it receives; a
//!   reply's grant is installed only if none arrived since the request
//!   was sent (the reply's *position* still serves that one read, which
//!   started before the recalled write could complete).
//! - **Release.** A holder releases its lease (which lets another node
//!   take over at once) only when its table holds no live grant.
//! - **Epoch change.** A new holder starts with an empty table; old
//!   delegations are dead by then because every grant was capped by the
//!   old lease. A delegate that tails a marker of a higher epoch than its
//!   delegation's drops it (an optimization; safety does not need it).
//!
//! # Model action → code
//!
//! | Model | Code |
//! |---|---|
//! | `ReadLocal` (bounded) | `fusefs_ops.rs` `lookup`/`open`/`readdir` → `ConstellationFs::strict_read`, bounded arm: M6's `session_wait` only |
//! | `ReadLocal` (strict, the usable holder) | `strict_read` → `LeaseView::reads_locally` |
//! | `ReadLocal` (strict, no live holder) | `Core::on_read_holder_learned` (lease claimable) → `read_tail` → `JobReq::TailToHead` → `ReadAnswer::Tailed` |
//! | `ReadUnderDelegation` / `FinishRead` | `strict_read` → `ReadDelegations::valid` → `Meta::session_wait_at(keys, grant position)` |
//! | `SendReadIndex` / `ReadIndexTimeout` | `strict_read` → `SyncRequest::ReadIndex` → `Control::ReadIndex` → `Core::on_read_index_control` → `PeerMsg::ReadIndex`; `Timer::ReadIndexTimeout` / `ReadIndexDeadline` |
//! | `Deliver(ReadIndex)` | `Core::on_read_index`: `LeaseState::usable` + not fenced, `maybe_grant` (lease cap, `note_grant_horizon`, `ReadDelegations::grant` *before* the position), per-key position |
//! | `Deliver(ReadIndexReply)` | `Core::on_read_index_reply` → `ReadDelegations::install(.., gen_at_send)` (until `sent + ttl − margin`) |
//! | `execute_write` recall set | `holder.rs` `on_mutate_request` → `recall_needed(.., Some(requester))` → `park_reply`; `client.rs` `execute_local` → `park_finish`; FUSE fast path `recall_after_local_write` → `Control::Recall`; inbox `inbox_recall_first` |
//! | `Deliver(Recall)` | `Core::on_delegation_recall` → `ReadDelegations::recall` (drop, bump the generation) → `PeerMsg::DelegationRecalled` |
//! | `Deliver(RecallAck)` / `Expire` | `Core::on_delegation_recalled` / `Timer::GrantExpiry` → `grant_done` → `complete_ready` (the parked reply, finish, control, release, inbox repoll) |
//! | `Release` (gated) | `jobs.rs` `issue_release` → `park_release_for_recalls` (`Phase::RecallBeforeRelease`) |
//! | `Takeover` (empty table) | a new tenure: every older grant was capped by its lease (`maybe_grant`) |
//! | `apply_next` voiding | `Replica::apply_segment` → `ReadDelegations::void_below_epoch` |
//! | `Jump` / offsets | clock error; the sim's `SimConfig::clock_skew_ms` |
//!
//! Not modeled, argued in PROGRESS.md (plan 30 M8): the restart
//! quarantine (a holder that restarts inside its own lease re-adopts the
//! same epoch, so it waits its persisted grant horizon out before any
//! acknowledgement — the model's `Takeover` already starts a fresh table
//! only at a *new* epoch) and the lone-node kernel-cache latch (a FUSE
//! concern).
//!
//! # The property
//!
//! `close_to_open` (always): a read that *starts* after another node's
//! close *completed* returns that close's version or a newer one. The
//! ghost `closed` is the highest version whose close has completed; a
//! read records it when it starts ([`NodeOp::ReadStart`]) and compares
//! at its return. A node's own writes are read-your-writes (M6), so the
//! scripts here always read on a different node than they write.
//!
//! # The drift margin
//!
//! Let every clock be within `D` of real time. A duration measured on one
//! clock is then off by at most `2D` (the offset can move by that much
//! between the two readings).
//! - *Recall override.* The delegate honours until real time `≤ s + ttl −
//!   deleg_margin + 2D` (`s` = when it sent the request); the holder
//!   overrides from real time `≥ g + ttl + seq_margin − 2D` (`g ≥ s` =
//!   when it granted). Safe when `deleg_margin + seq_margin > 4D`.
//! - *Epoch change.* The grant ends by `E − lease_margin` in the holder's
//!   clock (`E` = the lease's expiry), so the delegate honours until real
//!   time `≤ E − lease_margin − deleg_margin + 3D`; a new holder takes
//!   over at real time `≥ E − D`. Safe when `lease_margin + deleg_margin
//!   > 4D`.
//! - The lease itself needs `lease_margin > 2D` (holder stops at `E − M +
//!   D`, a taker starts at `E − D`).
//!
//! With all three margins equal to the lease's `M` (what the code does:
//! `Config::expiry_margin_ms` for all of them), every condition is the
//! lease's own `M > 2D`: read delegations tolerate exactly the clock
//! disagreement the lease already assumes. The tests show the margin is
//! load-bearing under drift and unnecessary without it (the delegate's
//! measurement starts before the holder's, so honest clocks need no
//! margin at all), and that the cap, the recall before a release, the
//! recall before an ack and the overtaken-grant rule each are
//! (`tests/cto.rs`).

use stateright::{Model, Property};

/// "No node".
pub const NONE: u8 = u8::MAX;

/// One step of a node's client script.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    /// Write a new version of `X` and close it.
    Write,
    /// Open `X` and read it.
    Read,
}

#[derive(Clone, Debug)]
pub struct CtoModel {
    /// Per-node client scripts (the node count is `scripts.len()`).
    pub scripts: Vec<Vec<Op>>,
    /// `cto=strict`; `false` is bounded mode (reads are local).
    pub strict: bool,
    /// Grant read delegations with ReadIndex answers.
    pub delegations: bool,
    /// Recall before acking a write (`false` is the naive design).
    pub recall: bool,
    pub seq_margin: i16,
    pub deleg_margin: i16,
    pub lease_margin: i16,
    pub lease_ttl: i16,
    pub deleg_ttl: i16,
    /// Cap a grant at the backing lease's usable end.
    pub cap_by_lease: bool,
    /// A release waits until no grant is live.
    pub recall_before_release: bool,
    /// A delegate drops its delegation on tailing a higher epoch's marker.
    pub void_on_epoch: bool,
    /// A grant whose reply a recall overtook is not installed.
    pub recall_gen_check: bool,
    /// `D`: every clock is within this many ticks of real time.
    pub max_offset: i16,
    /// How many clock steps (within `±D`) may happen.
    pub max_jumps: u8,
    pub max_drops: u8,
    pub max_tick: u8,
    pub max_epoch: u8,
    pub allow_renew: bool,
    pub allow_release: bool,
    /// Initial holder (epoch 1, fresh lease at t = 0), or `NONE`.
    pub initial_holder: u8,
    /// Every initial offset combination (`true`), or `init_offsets`.
    pub explore_offsets: bool,
    /// The initial offsets when not exploring them (default all zero).
    pub init_offsets: Option<Vec<i16>>,
}

impl CtoModel {
    /// Strict mode with delegations, recall, lease-capped grants and the
    /// lease's margin on every side: the design.
    pub fn strict(scripts: Vec<Vec<Op>>) -> Self {
        CtoModel {
            scripts,
            strict: true,
            delegations: true,
            recall: true,
            seq_margin: 1,
            deleg_margin: 1,
            lease_margin: 1,
            lease_ttl: 6,
            deleg_ttl: 3,
            cap_by_lease: true,
            recall_before_release: true,
            void_on_epoch: true,
            recall_gen_check: true,
            max_offset: 0,
            max_jumps: 0,
            max_drops: 1,
            max_tick: 9,
            max_epoch: 2,
            allow_renew: false,
            allow_release: false,
            initial_holder: 0,
            explore_offsets: false,
            init_offsets: None,
        }
    }

    pub fn bounded(scripts: Vec<Vec<Op>>) -> Self {
        CtoModel {
            strict: false,
            delegations: false,
            ..Self::strict(scripts)
        }
    }

    fn n(&self) -> usize {
        self.scripts.len()
    }
}

/// The lease object in the bucket. `expires` is in the writer's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    pub expires: i16,
    pub released: bool,
}

/// Holder side: a grant it must recall or outwait.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Grant {
    pub to: u8,
    /// Live until the holder's clock reaches this (`g + ttl + seq_margin`).
    pub until: i16,
    pub id: u8,
}

/// Delegate side.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Deleg {
    /// Honoured while the delegate's clock is below this.
    pub until: i16,
    /// The position the reader must have applied (log length).
    pub pos: u8,
    pub epoch: u8,
}

/// A write whose acknowledgement waits for recalls.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Held {
    pub writer: u8,
    pub version: u8,
    pub waiting: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeOp {
    Idle,
    /// A write in flight (forwarded, or held here for recalls).
    Writing,
    /// A read began; `must` is the ghost: the highest version whose close
    /// had completed.
    ReadStart {
        must: u8,
    },
    ReadIndexSent {
        must: u8,
        sent: i16,
        gen: u8,
    },
    /// Waiting for the replica to reach `pos`.
    ReadWait {
        must: u8,
        pos: u8,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    /// The lease this node believes it holds: `(epoch, expires)`.
    pub lease: Option<(u8, i16)>,
    pub applied: u8,
    pub pc: u8,
    pub op: NodeOp,
    pub deleg: Option<Deleg>,
    pub grants: Vec<Grant>,
    pub held: Vec<Held>,
    /// Delegate side: bumped by every recall this node receives. A
    /// ReadIndex records it when sent, and its reply's grant is installed
    /// only if no recall arrived meanwhile — a recall can overtake the
    /// reply that carries the very grant it recalls.
    pub recall_gen: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Forward {
        from: u8,
        to: u8,
    },
    /// `version == 0`: not the holder; retry.
    ForwardReply {
        to: u8,
        version: u8,
    },
    ReadIndex {
        from: u8,
        to: u8,
    },
    /// `ok == false`: not a usable holder. `grant`: `(ttl, epoch)`.
    ReadIndexReply {
        to: u8,
        ok: bool,
        pos: u8,
        grant: Option<(i16, u8)>,
    },
    Recall {
        to: u8,
        from: u8,
        id: u8,
    },
    RecallAck {
        to: u8,
        from: u8,
        id: u8,
    },
}

/// Ghost bits for the `sometimes` (non-vacuity) properties.
pub const SAW_DELEG_READ: u8 = 1;
pub const SAW_RECALL_ACK: u8 = 2;
pub const SAW_RECALL_EXPIRED: u8 = 4;
pub const SAW_TAKEOVER: u8 = 8;
pub const SAW_READINDEX_READ: u8 = 16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: u8,
    pub off: Vec<i16>,
    pub jumps: u8,
    pub drops: u8,
    pub reg: Reg,
    /// 0 = a write of `X`; `e > 0` = the epoch-`e` takeover marker.
    pub log: Vec<u8>,
    pub nodes: Vec<Node>,
    pub net: Vec<Msg>,
    /// Ghost: the highest version whose close completed.
    pub closed: u8,
    /// A read returned less than its `must`: `(node, must, got)`.
    pub violation: Option<(u8, u8, u8)>,
    pub next_id: u8,
    pub saw: u8,
}

impl State {
    fn local(&self, i: usize) -> i16 {
        self.t as i16 + self.off[i]
    }

    /// The version a replica at prefix `n` holds (writes in the prefix).
    fn version_at(&self, n: u8) -> u8 {
        self.log[..n as usize].iter().filter(|e| **e == 0).count() as u8
    }

    fn versions(&self) -> u8 {
        self.version_at(self.log.len() as u8)
    }

    fn usable(&self, cfg: &CtoModel, i: usize) -> Option<u8> {
        let (epoch, expires) = self.nodes[i].lease?;
        (self.local(i) < expires - cfg.lease_margin).then_some(epoch)
    }

    /// A usable holder whose epoch is the register's: may execute and
    /// answer (a stale-epoch holder is fenced by the log). Answering a
    /// ReadIndex needs only `usable` — that is exactly the check the
    /// lease discipline must make safe, so the model does not add the
    /// register comparison there.
    fn may_execute(&self, cfg: &CtoModel, i: usize) -> bool {
        self.usable(cfg, i)
            .is_some_and(|e| e == self.reg.epoch && self.reg.holder == i as u8)
    }

    fn send(&mut self, m: Msg) {
        self.net.push(m);
        self.net.sort();
    }

    fn fresh_id(&mut self) -> u8 {
        self.next_id += 1;
        self.next_id
    }

    fn finish_op(&mut self, i: usize) {
        self.nodes[i].pc += 1;
        self.nodes[i].op = NodeOp::Idle;
    }

    fn check_read(&mut self, i: usize, must: u8, got: u8) {
        if got < must && self.violation.is_none() {
            self.violation = Some((i as u8, must, got));
        }
    }

    /// The write's close completed (the writer got its ack).
    fn close_completed(&mut self, writer: usize, version: u8) {
        self.closed = self.closed.max(version);
        self.finish_op(writer);
    }

    /// Execute a write at holder `h` for `writer`; returns whether it is
    /// acked now (else held for recalls).
    fn execute_write(&mut self, cfg: &CtoModel, h: usize, writer: usize) {
        self.log.push(0);
        let len = self.log.len() as u8;
        self.nodes[h].applied = len;
        let version = self.versions();
        let now = self.local(h);
        self.nodes[h].grants.retain(|g| g.until > now);
        let waiting: Vec<u8> = if cfg.recall {
            self.nodes[h]
                .grants
                .iter()
                .filter(|g| g.to != writer as u8)
                .map(|g| g.id)
                .collect()
        } else {
            Vec::new()
        };
        if waiting.is_empty() {
            self.ack_write(h, writer, version);
            return;
        }
        for g in self.nodes[h].grants.clone() {
            if waiting.contains(&g.id) {
                self.send(Msg::Recall {
                    to: g.to,
                    from: h as u8,
                    id: g.id,
                });
            }
        }
        self.nodes[h].held.push(Held {
            writer: writer as u8,
            version,
            waiting,
        });
    }

    fn ack_write(&mut self, h: usize, writer: usize, version: u8) {
        if writer == h {
            self.close_completed(writer, version);
        } else {
            self.send(Msg::ForwardReply {
                to: writer as u8,
                version,
            });
        }
    }

    /// Grant `id` is gone (acked or expired): release held writes.
    fn grant_done(&mut self, h: usize, id: u8) {
        self.nodes[h].grants.retain(|g| g.id != id);
        let mut done = Vec::new();
        for held in self.nodes[h].held.iter_mut() {
            held.waiting.retain(|w| *w != id);
            if held.waiting.is_empty() {
                done.push((held.writer, held.version));
            }
        }
        self.nodes[h].held.retain(|x| !x.waiting.is_empty());
        for (writer, version) in done {
            self.ack_write(h, writer as usize, version);
        }
    }

    fn apply_next(&mut self, cfg: &CtoModel, i: usize) {
        let entry = self.log[self.nodes[i].applied as usize];
        self.nodes[i].applied += 1;
        if entry > 0 && cfg.void_on_epoch {
            if let Some(d) = self.nodes[i].deleg {
                if entry > d.epoch {
                    self.nodes[i].deleg = None;
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Jump(u8, i16),
    Drop(usize),
    Tail(u8),
    Renew(u8),
    Takeover(u8),
    Release(u8),
    /// Start node `i`'s next script op.
    Start(u8),
    /// Bounded read, a holder's local read, or the S3 path (no live
    /// holder: tail to head, then read).
    ReadLocal(u8),
    /// A strict read under a valid delegation: wait for its position.
    ReadUnderDelegation(u8),
    SendReadIndex(u8),
    /// The reader gives up on an unanswered ReadIndex and starts over.
    ReadIndexTimeout(u8),
    FinishRead(u8),
    /// A write whose forward got `NotHolder` (or whose writer holds no
    /// usable lease) retries against the register's holder.
    RetryWrite(u8),
    /// Holder `h` drops its expired grants (by its clock).
    Expire(u8),
    Deliver(usize),
}

impl Model for CtoModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        let mut offs: Vec<Vec<i16>> = vec![self.init_offsets.clone().unwrap_or(vec![0; n])];
        if self.explore_offsets && self.max_offset > 0 {
            offs = vec![vec![]];
            for _ in 0..n {
                let mut next = Vec::new();
                for o in &offs {
                    for d in -self.max_offset..=self.max_offset {
                        let mut v = o.clone();
                        v.push(d);
                        next.push(v);
                    }
                }
                offs = next;
            }
        }
        offs.into_iter()
            .map(|off| {
                let mut nodes: Vec<Node> = (0..n)
                    .map(|_| Node {
                        lease: None,
                        applied: 0,
                        pc: 0,
                        op: NodeOp::Idle,
                        deleg: None,
                        grants: Vec::new(),
                        held: Vec::new(),
                        recall_gen: 0,
                    })
                    .collect();
                let mut reg = Reg {
                    holder: NONE,
                    epoch: 0,
                    expires: 0,
                    released: false,
                };
                let mut log = Vec::new();
                if self.initial_holder != NONE {
                    let h = self.initial_holder as usize;
                    let expires = off[h] + self.lease_ttl;
                    reg = Reg {
                        holder: h as u8,
                        epoch: 1,
                        expires,
                        released: false,
                    };
                    log.push(1);
                    for node in nodes.iter_mut() {
                        node.applied = 1;
                    }
                    nodes[h].lease = Some((1, expires));
                }
                State {
                    t: 0,
                    off,
                    jumps: 0,
                    drops: 0,
                    reg,
                    log,
                    nodes,
                    net: Vec::new(),
                    closed: 0,
                    violation: None,
                    next_id: 0,
                    saw: 0,
                }
            })
            .collect()
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.violation.is_some() {
            return;
        }
        let n = self.n();
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        if s.jumps < self.max_jumps {
            for i in 0..n {
                for d in -self.max_offset..=self.max_offset {
                    if d != s.off[i] {
                        actions.push(Action::Jump(i as u8, d));
                    }
                }
            }
        }
        if s.drops < self.max_drops {
            for k in 0..s.net.len() {
                if k == 0 || s.net[k] != s.net[k - 1] {
                    actions.push(Action::Drop(k));
                }
            }
        }
        for k in 0..s.net.len() {
            if k == 0 || s.net[k] != s.net[k - 1] {
                actions.push(Action::Deliver(k));
            }
        }
        for i in 0..n {
            let node = &s.nodes[i];
            let iu = i as u8;
            if (node.applied as usize) < s.log.len() {
                actions.push(Action::Tail(iu));
            }
            if self.allow_renew && s.may_execute(self, i) && !s.reg.released {
                actions.push(Action::Renew(iu));
            }
            let claimable = s.reg.holder == NONE || s.reg.released || s.local(i) >= s.reg.expires;
            // Takeover is offered only to a node with a write to do: that
            // is what makes a node acquire.
            let wants = node.op == NodeOp::Writing
                || (node.op == NodeOp::Idle
                    && self.scripts[i].get(node.pc as usize) == Some(&Op::Write));
            if claimable && wants && s.reg.epoch < self.max_epoch {
                actions.push(Action::Takeover(iu));
            }
            if self.allow_release && s.may_execute(self, i) && node.held.is_empty() {
                let now = s.local(i);
                let live = node.grants.iter().any(|g| g.until > now);
                if !(self.recall_before_release && live) {
                    actions.push(Action::Release(iu));
                }
            }
            if !node.grants.is_empty() {
                let now = s.local(i);
                if node.grants.iter().any(|g| g.until <= now) {
                    actions.push(Action::Expire(iu));
                }
            }
            match node.op {
                NodeOp::Idle => {
                    if (node.pc as usize) < self.scripts[i].len() {
                        actions.push(Action::Start(iu));
                    }
                }
                NodeOp::Writing => {
                    let in_flight = s.net.iter().any(|m| {
                        matches!(m, Msg::Forward { from, .. } if *from == iu)
                            || matches!(m, Msg::ForwardReply { to, .. } if *to == iu)
                    }) || s
                        .nodes
                        .iter()
                        .any(|x| x.held.iter().any(|h| h.writer == iu));
                    if !in_flight {
                        actions.push(Action::RetryWrite(iu));
                    }
                }
                NodeOp::ReadStart { .. } => {
                    // Bounded, or the usable holder: its own replica.
                    if !self.strict || s.usable(self, i).is_some() {
                        actions.push(Action::ReadLocal(iu));
                    } else if self.delegations && node.deleg.is_some_and(|d| s.local(i) < d.until) {
                        actions.push(Action::ReadUnderDelegation(iu));
                    } else if s.reg.holder == NONE || s.reg.released || s.local(i) >= s.reg.expires
                    {
                        actions.push(Action::ReadLocal(iu));
                    } else {
                        actions.push(Action::SendReadIndex(iu));
                    }
                }
                NodeOp::ReadIndexSent { .. } => {
                    // One read in flight per node, and a retry only once
                    // nothing of the last attempt is in flight: no stale
                    // reply can reach a later attempt.
                    let pending = s.net.iter().any(|m| {
                        matches!(m, Msg::ReadIndex { from, .. } if *from == iu)
                            || matches!(m, Msg::ReadIndexReply { to, .. } if *to == iu)
                    });
                    if !pending {
                        actions.push(Action::ReadIndexTimeout(iu));
                    }
                }
                NodeOp::ReadWait { pos, .. } => {
                    if node.applied >= pos {
                        actions.push(Action::FinishRead(iu));
                    }
                }
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Tick => s.t += 1,
            Action::Jump(i, d) => {
                s.off[i as usize] = d;
                s.jumps += 1;
            }
            Action::Drop(k) => {
                s.net.remove(k);
                s.drops += 1;
            }
            Action::Tail(i) => s.apply_next(self, i as usize),
            Action::Renew(i) => {
                let i = i as usize;
                let expires = s.local(i) + self.lease_ttl;
                s.reg.expires = expires;
                let epoch = s.reg.epoch;
                s.nodes[i].lease = Some((epoch, expires));
            }
            Action::Takeover(i) => {
                let i = i as usize;
                let epoch = s.reg.epoch + 1;
                let expires = s.local(i) + self.lease_ttl;
                s.reg = Reg {
                    holder: i as u8,
                    epoch,
                    expires,
                    released: false,
                };
                // Tail to head, then the marker.
                while (s.nodes[i].applied as usize) < s.log.len() {
                    s.apply_next(self, i);
                }
                s.log.push(epoch);
                s.nodes[i].applied = s.log.len() as u8;
                s.nodes[i].lease = Some((epoch, expires));
                // A new tenure's table is empty.
                s.nodes[i].grants.clear();
                s.nodes[i].held.clear();
                s.saw |= SAW_TAKEOVER;
            }
            Action::Release(i) => {
                let i = i as usize;
                s.reg.released = true;
                s.nodes[i].lease = None;
                s.nodes[i].grants.clear();
            }
            Action::Start(i) => {
                let i = i as usize;
                let op = self.scripts[i][s.nodes[i].pc as usize];
                match op {
                    Op::Read => {
                        s.nodes[i].op = NodeOp::ReadStart { must: s.closed };
                    }
                    Op::Write => {
                        s.nodes[i].op = NodeOp::Writing;
                        start_write(self, &mut s, i);
                    }
                }
            }
            Action::RetryWrite(i) => start_write(self, &mut s, i as usize),
            Action::ReadLocal(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                if self.strict && s.usable(self, i).is_none() {
                    // No live holder: the S3 path tails to head.
                    while (s.nodes[i].applied as usize) < s.log.len() {
                        s.apply_next(self, i);
                    }
                }
                let got = s.version_at(s.nodes[i].applied);
                s.check_read(i, must, got);
                s.finish_op(i);
            }
            Action::ReadUnderDelegation(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                let d = s.nodes[i].deleg.expect("valid delegation");
                s.nodes[i].op = NodeOp::ReadWait { must, pos: d.pos };
                s.saw |= SAW_DELEG_READ;
            }
            Action::SendReadIndex(i) => {
                let i = i as usize;
                let NodeOp::ReadStart { must } = s.nodes[i].op else {
                    return None;
                };
                let sent = s.local(i);
                let to = s.reg.holder;
                s.send(Msg::ReadIndex { from: i as u8, to });
                let gen = s.nodes[i].recall_gen;
                s.nodes[i].op = NodeOp::ReadIndexSent { must, sent, gen };
            }
            Action::ReadIndexTimeout(i) => {
                let i = i as usize;
                let NodeOp::ReadIndexSent { must, .. } = s.nodes[i].op else {
                    return None;
                };
                s.nodes[i].op = NodeOp::ReadStart { must };
            }
            Action::FinishRead(i) => {
                let i = i as usize;
                let NodeOp::ReadWait { must, .. } = s.nodes[i].op else {
                    return None;
                };
                let got = s.version_at(s.nodes[i].applied);
                s.check_read(i, must, got);
                s.finish_op(i);
            }
            Action::Expire(h) => {
                let h = h as usize;
                let now = s.local(h);
                let expired: Vec<u8> = s.nodes[h]
                    .grants
                    .iter()
                    .filter(|g| g.until <= now)
                    .map(|g| g.id)
                    .collect();
                for id in expired {
                    if s.nodes[h].held.iter().any(|x| x.waiting.contains(&id)) {
                        s.saw |= SAW_RECALL_EXPIRED;
                    }
                    s.grant_done(h, id);
                }
            }
            Action::Deliver(k) => {
                let m = s.net.remove(k);
                deliver(self, &mut s, m);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("close_to_open", |_, s: &State| s.violation.is_none()),
            Property::sometimes("all_ops_done", |m: &CtoModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| n.pc as usize == sc.len())
            }),
            Property::sometimes("read_under_delegation", |_, s: &State| {
                s.saw & SAW_DELEG_READ != 0
            }),
            Property::sometimes("recall_acked", |_, s: &State| s.saw & SAW_RECALL_ACK != 0),
        ]
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick
    }
}

/// Start (or retry) node `i`'s write: execute here if it holds, else
/// forward to the register's holder. With no usable holder anywhere the
/// write waits (a `Takeover` makes progress).
fn start_write(cfg: &CtoModel, s: &mut State, i: usize) {
    if s.may_execute(cfg, i) {
        s.execute_write(cfg, i, i);
        return;
    }
    let h = s.reg.holder;
    if h != NONE && h as usize != i && !s.reg.released {
        s.send(Msg::Forward {
            from: i as u8,
            to: h,
        });
    }
}

fn deliver(cfg: &CtoModel, s: &mut State, m: Msg) {
    match m {
        Msg::Forward { from, to } => {
            let h = to as usize;
            if s.may_execute(cfg, h) {
                s.execute_write(cfg, h, from as usize);
            } else {
                s.send(Msg::ForwardReply {
                    to: from,
                    version: 0,
                });
            }
        }
        Msg::ForwardReply { to, version } => {
            let w = to as usize;
            if s.nodes[w].op != NodeOp::Writing {
                return;
            }
            if version > 0 {
                // M6's read-your-writes: the writer's replica has its own
                // write when the close returns (a shadow on an applied
                // base, or `AwaitingLog` until the log carries it) —
                // modeled as catching up to the write's log entry. That
                // is what lets the sequencer leave the writer's own
                // delegation alone.
                let index = s
                    .log
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| **e == 0)
                    .nth(version as usize - 1)
                    .map(|(i, _)| i as u8 + 1)
                    .expect("the version is in the log");
                while s.nodes[w].applied < index {
                    s.apply_next(cfg, w);
                }
                s.close_completed(w, version);
            }
            // version 0: `RetryWrite` is enabled (nothing in flight).
        }
        Msg::ReadIndex { from, to } => {
            let h = to as usize;
            let Some(epoch) = s.usable(cfg, h) else {
                s.send(Msg::ReadIndexReply {
                    to: from,
                    ok: false,
                    pos: 0,
                    grant: None,
                });
                return;
            };
            let pos = s.nodes[h].applied;
            let now = s.local(h);
            let mut grant = None;
            if cfg.delegations {
                let mut ttl = cfg.deleg_ttl;
                if cfg.cap_by_lease {
                    let (_, expires) = s.nodes[h].lease.expect("usable");
                    ttl = ttl.min(expires - cfg.lease_margin - now);
                }
                if ttl > 0 {
                    let id = s.fresh_id();
                    s.nodes[h].grants.push(Grant {
                        to: from,
                        until: now + ttl + cfg.seq_margin,
                        id,
                    });
                    s.nodes[h].grants.sort();
                    grant = Some((ttl, epoch));
                }
            }
            s.send(Msg::ReadIndexReply {
                to: from,
                ok: true,
                pos,
                grant,
            });
        }
        Msg::ReadIndexReply { to, ok, pos, grant } => {
            let r = to as usize;
            let NodeOp::ReadIndexSent { must, sent, gen } = s.nodes[r].op else {
                return;
            };
            if !ok {
                s.nodes[r].op = NodeOp::ReadStart { must };
                return;
            }
            // The position stands either way: this read started before
            // any close the racing recall is for could complete.
            let raced = cfg.recall_gen_check && s.nodes[r].recall_gen != gen;
            if let (Some((ttl, epoch)), false) = (grant, raced) {
                s.nodes[r].deleg = Some(Deleg {
                    until: sent + ttl - cfg.deleg_margin,
                    pos,
                    epoch,
                });
            }
            s.nodes[r].op = NodeOp::ReadWait { must, pos };
            s.saw |= SAW_READINDEX_READ;
        }
        Msg::Recall { to, from, id } => {
            // The delegate stops honouring before it acks.
            s.nodes[to as usize].deleg = None;
            s.nodes[to as usize].recall_gen = s.nodes[to as usize].recall_gen.saturating_add(1);
            s.send(Msg::RecallAck {
                to: from,
                from: to,
                id,
            });
        }
        Msg::RecallAck { to, id, .. } => {
            let h = to as usize;
            if s.nodes[h].grants.iter().any(|g| g.id == id) {
                if s.nodes[h].held.iter().any(|x| x.waiting.contains(&id)) {
                    s.saw |= SAW_RECALL_ACK;
                }
                s.grant_done(h, id);
            }
        }
    }
}
