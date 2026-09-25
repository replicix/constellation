//! Plan 30 §M14: strict mode — cross-node `flock`/`fcntl` as leased,
//! recallable per-node file locks, over clocks with bounded drift.
//!
//! # What is modeled
//!
//! One file `X`. Its lock table lives at the *owning sequencer*: the root
//! (the lease register's holder) or, after a delegation move, a delegate.
//! The table is keyed by node: a node holds at most one *grant* on `X`,
//! in mode shared or exclusive, and resolves its applications' POSIX
//! and flock requests locally under that grant (the kernel-facing
//! owner/range table is a node-local matter and not modeled; what can
//! corrupt data across nodes is two nodes acting under conflicting
//! grants). The grant is a lease:
//!
//! - **Grant.** The owner grants `ttl = min(lock_ttl, usable end of its
//!   own authority − now)` (the cap: a root's lease, a delegate's
//!   delegation) and records the grant live until `now + ttl +
//!   seq_margin` in its clock. The requester honours it until `sent +
//!   ttl − node_margin`, measured from when it *sent* the request.
//! - **Renew.** A node holding a grant renews it (`Renew` → `RenewAck`,
//!   the same `sent`-based measurement) while it still honours it; a
//!   lapsed grant is lost, never renewed late. An owner that does not
//!   know the grant answers `Lost`, except during its *grace* (below),
//!   when it installs it: that is a reclaim. The acknowledgement carries
//!   the grant's recalled flag, so a lost recall is repaired by the next
//!   renewal.
//! - **Cached re-locks.** A node keeps its grant after its application
//!   unlocks; a later local lock in a covered mode is granted locally,
//!   with no message.
//! - **Recall.** A conflicting request makes the owner recall the
//!   conflicting grants (`Recall`) and park the request. A recalled node
//!   releases (`Released`) at once if no application holds a lock under
//!   the grant, else when the last one unlocks. An unreachable node is
//!   outwaited: the owner drops the grant at its `until` (`Expire`).
//! - **Fencing.** A node performs I/O under a lock only while its grant
//!   is honoured (`fencing`; the naive design lets the application go on
//!   until it unlocks).
//! - **Failover.** A TTL takeover starts an empty table (old grants were
//!   capped by the old lease, so they have expired at their holders). A
//!   *fast* takeover (a sealed backup or `ack=s3`, before expiry) starts
//!   from the backup's asynchronously mirrored table (`replicate`) and
//!   applies M9's acknowledgement floor as a **grace period**: no new
//!   grant until `now + takeover_window + lock_ttl + 2·seq_margin`
//!   (capped at the old expiry), while reclaims are accepted. The old
//!   root keeps answering until it probes the register, and answers only
//!   within `takeover_window` of a probe (M8's probe freshness).
//! - **Delegation moves.** `Delegate` hands the table to the delegate
//!   (restamped: every entry is live until `now + lock_ttl + seq_margin`
//!   in the receiver's clock, as if renewed at the move — conservative);
//!   a graceful `Recall` hands it back the same way; an unreachable
//!   delegate is outwaited by its delegation's TTL plus margin, after
//!   which the root's table for the subtree is empty *and under a grace
//!   period*: the delegate's own grants were capped by its tenure, but
//!   the grants moved to it at the delegation still carry the windows
//!   the root gave them until their first renewal there (the checker
//!   found this: `tests/locks.rs`, `an_outwaited_delegate_without_grace_violates`).
//!   `move_state = false` is the naive design that starts every move
//!   with an empty table.
//!
//! # The property
//!
//! `mutual_exclusion` (always): a node never performs I/O under a grant
//! after a conflicting grant to another node was issued. Grants carry a
//! global issue order (`id`); at `Io(i)` under grant `c` the ghost
//! `issued` is searched for a conflicting grant with `id > c` to another
//! node. That is the interval form of mutual exclusion: the later grant
//! opened an exclusion interval the earlier holder's I/O now lands in,
//! whether or not the later holder has acted yet, and whether or not it
//! has already unlocked (a lost update needs only an overlap). A reclaim
//! keeps its id; an upgrade is a new grant.
//!
//! # The drift margin (as in `cto`)
//!
//! Every clock is within `D` of real time, so a duration measured on one
//! clock is off by at most `2D`. A node honours until real time
//! `≤ s + ttl − node_margin + 2D`; the owner considers the grant gone
//! from `≥ g + ttl + seq_margin − 2D` with `g ≥ s`: safe when
//! `node_margin + seq_margin > 4D`. A restamp at a move or takeover is
//! the same inequality with `g` the move. The cap makes an epoch change the
//! lease's own argument (`lease_margin + node_margin > 4D`), and the
//! grace makes a fast takeover M8's floor argument. With every margin
//! equal to the lease's `M`, all of it is `M > 2D`.
//!
//! # Model action → code
//!
//! | Model | Code |
//! |---|---|
//! | `Start(Lock)` local hit | `fusefs_ops.rs` `setlk` → `Meta::locks().try_local` under a live `CacheGrant` |
//! | `SendLock`/`Deliver(LockReq)` | `SyncRequest::Lock` → `Control::Lock` → `PeerMsg::LockRequest` → `Core::on_lock_request` (`core/locks.rs`) |
//! | recall + park | `on_lock_request` → `recall_needed`-style `LockRecall` + `park_reply(ParkedWhat::LockGrant)` |
//! | `Deliver(Recall)` / `Released` | `Core::on_lock_recall` → `Action::LockFlush` → `Event::LockFlushed` → `PeerMsg::LockReleased` |
//! | `Expire` | `Timer::LockGrantExpiry` |
//! | `Renew`/`RenewAck`/`Lost` | `Timer::LockRenew` → `PeerMsg::LockRenew` → `LockRenewed { ok }` |
//! | `Io` fenced | `ConstellationFs::lock_fence(ino)` → `EIO` |
//! | `Takeover` grace | `note_marker_landed`'s floor, extended to lock grants; reclaims in `on_lock_renew` |
//! | `Delegate`/`Recall` state | `DelegRenewed { locks }` (first renewal) and `DelegRecalled { locks }` |
//! | `Mirror` | `BackupAppend { locks }` (a snapshot rides the next append) |

use stateright::{Model, Property};

/// "No node".
pub const NONE: u8 = u8::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Mode {
    Shared,
    Exclusive,
}

impl Mode {
    pub fn conflicts(self, other: Mode) -> bool {
        self == Mode::Exclusive || other == Mode::Exclusive
    }

    /// A grant in `self` covers a local lock in `m`.
    pub fn covers(self, m: Mode) -> bool {
        self == Mode::Exclusive || m == Mode::Shared
    }
}

/// One step of a node's application script.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Lock(Mode),
    /// Read or write `X` under the lock held.
    Io,
    Unlock,
}

/// Who owns `X`'s lock table (the replicated delegation table).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Owner {
    Root,
    Delegate(u8),
    /// A graceful recall in flight: nobody answers until it lands.
    Recalling(u8),
}

#[derive(Clone, Debug)]
pub struct LockModel {
    pub scripts: Vec<Vec<Op>>,
    /// A node stops I/O when its grant is no longer honoured.
    pub fencing: bool,
    /// Grants are capped by the owner's own authority.
    pub cap_by_lease: bool,
    /// A fast takeover's successor applies the grace period.
    pub grace: bool,
    /// A `fast` holder answers only within `takeover_window` of a probe.
    pub probe_freshness: bool,
    /// The root mirrors its table to the backup (asynchronously).
    pub replicate: bool,
    /// A delegation move carries the table.
    pub move_state: bool,
    pub seq_margin: i16,
    pub node_margin: i16,
    pub lease_margin: i16,
    pub lease_ttl: i16,
    pub lock_ttl: i16,
    pub deleg_ttl: i16,
    pub takeover_window: i16,
    /// `D`.
    pub max_offset: i16,
    pub init_offsets: Option<Vec<i16>>,
    pub max_jumps: u8,
    pub max_drops: u8,
    pub max_tick: u8,
    pub max_epoch: u8,
    pub max_renews: u8,
    pub max_moves: u8,
    pub initial_holder: u8,
    /// The node that may take the lease over before it expires (a sealed
    /// backup / any peer under `ack=s3`), or `NONE`.
    pub backup: u8,
    /// A TTL takeover is allowed (by any node with a lock to take).
    pub ttl_takeover: bool,
    /// The node that may be delegated `X`'s subtree, or `NONE`.
    pub delegate: u8,
    /// The root may end an unanswered recall by the delegation's TTL.
    pub recall_by_ttl: bool,
    /// The root may renew its lease.
    pub allow_renew: bool,
}

impl LockModel {
    /// The design: fencing, the cap, the grace, state moves.
    pub fn design(scripts: Vec<Vec<Op>>) -> Self {
        LockModel {
            scripts,
            fencing: true,
            cap_by_lease: true,
            grace: true,
            probe_freshness: true,
            replicate: true,
            move_state: true,
            seq_margin: 1,
            node_margin: 1,
            lease_margin: 1,
            lease_ttl: 8,
            lock_ttl: 3,
            deleg_ttl: 4,
            takeover_window: 1,
            max_offset: 0,
            init_offsets: None,
            max_jumps: 0,
            max_drops: 1,
            max_tick: 8,
            max_epoch: 1,
            max_renews: 1,
            max_moves: 0,
            initial_holder: 0,
            backup: NONE,
            ttl_takeover: false,
            delegate: NONE,
            recall_by_ttl: false,
            allow_renew: false,
        }
    }

    fn n(&self) -> usize {
        self.scripts.len()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    pub expires: i16,
}

/// Owner side: a node's grant on `X`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Grant {
    pub node: u8,
    pub mode: Mode,
    /// Live until the owner's clock reaches this.
    pub until: i16,
    pub id: u8,
    pub recalled: bool,
}

/// Owner side: a blocking request waiting for recalls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Park {
    pub node: u8,
    pub mode: Mode,
    pub sent: i16,
}

/// Node side: the grant it holds (cached across local unlocks).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cache {
    pub mode: Mode,
    pub until: i16,
    /// Renew from here (`sent + ttl/2`, as the code does).
    pub renew_at: i16,
    pub id: u8,
    pub recalled: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeOp {
    Idle,
    /// A lock request in flight (or waiting to be (re)sent).
    Locking {
        mode: Mode,
        sent: i16,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub lease: Option<(u8, i16)>,
    pub pc: u8,
    pub op: NodeOp,
    // ---- application / node side ----
    pub cache: Option<Cache>,
    /// The application's lock under the cache.
    pub held: Option<Mode>,
    pub renews: u8,
    /// Recalls of grants this node does not hold (yet): a recall can
    /// overtake the reply carrying the grant it recalls. The reply then
    /// installs the grant already recalled — the application gets its
    /// turn and the node releases at its unlock. Answering `Released`
    /// instead would let the owner grant the waiter, whose grant the
    /// re-sent request would recall before *its* reply lands: a livelock
    /// the checker found (see `tests/locks.rs`).
    pub pending_recalls: Vec<u8>,
    // ---- owner side (root or delegate) ----
    pub table: Vec<Grant>,
    pub parks: Vec<Park>,
    /// Grace: no new grant while the clock is below this.
    pub floor: Option<i16>,
    pub last_probe: i16,
    /// Delegate side: the delegation is honoured below this.
    pub dtenure: Option<i16>,
    /// Root side: the delegation it granted is live below this.
    pub dgen: Option<i16>,
    /// Backup side: the mirrored table and its version.
    pub mirror: (u8, Vec<Grant>),
    /// Root side: the mirror version counter.
    pub mirror_ver: u8,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    LockReq {
        from: u8,
        to: u8,
        mode: Mode,
        sent: i16,
    },
    /// `ok`: `(id, ttl)`; `None`: not the owner (retry).
    LockReply {
        to: u8,
        sent: i16,
        ok: Option<(u8, Mode, i16)>,
    },
    Recall {
        to: u8,
        from: u8,
        id: u8,
    },
    Released {
        to: u8,
        from: u8,
        id: u8,
    },
    Renew {
        from: u8,
        to: u8,
        id: u8,
        mode: Mode,
        sent: i16,
    },
    RenewAck {
        to: u8,
        id: u8,
        sent: i16,
        outcome: RenewOutcome,
    },
    /// Root → backup: the table as of version `ver`.
    Mirror {
        to: u8,
        ver: u8,
        table: Vec<Grant>,
    },
    DelegRecall {
        to: u8,
    },
    DelegRecalled {
        to: u8,
        table: Vec<Grant>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RenewOutcome {
    /// `(ttl, recalled)`: the heartbeat repairs a lost recall.
    Ok(i16, bool),
    Lost,
    NotOwner,
}

pub const SAW_LOCAL_RELOCK: u16 = 1;
pub const SAW_RECALL_RELEASED: u16 = 2;
pub const SAW_RECALL_EXPIRED: u16 = 4;
pub const SAW_RECLAIM: u16 = 8;
pub const SAW_MOVE: u16 = 16;
pub const SAW_FAST_TAKEOVER: u16 = 32;
pub const SAW_TTL_TAKEOVER: u16 = 64;
pub const SAW_FENCED: u16 = 128;
pub const SAW_RECALL_BY_TTL: u16 = 256;
pub const SAW_MIRROR_USED: u16 = 512;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: u8,
    pub off: Vec<i16>,
    pub jumps: u8,
    pub drops: u8,
    pub reg: Reg,
    pub owner: Owner,
    pub moves: u8,
    pub nodes: Vec<Node>,
    pub net: Vec<Msg>,
    pub next_id: u8,
    /// Ghost: every grant ever issued, `(id, node, mode)`.
    pub issued: Vec<(u8, u8, Mode)>,
    /// `(node, its grant id, the conflicting later grant id)`.
    pub violation: Option<(u8, u8, u8)>,
    pub saw: u16,
}

impl State {
    fn local(&self, i: usize) -> i16 {
        self.t as i16 + self.off[i]
    }

    fn usable_root(&self, cfg: &LockModel, i: usize) -> bool {
        let Some((epoch, expires)) = self.nodes[i].lease else {
            return false;
        };
        self.local(i) < expires - cfg.lease_margin
            && self.reg.holder == i as u8
            && self.reg.epoch == epoch
    }

    /// Node `i` believes it holds a usable root lease (its own clock, no
    /// look at the register: a lock grant is not a log append, so nothing
    /// but the lease discipline and the probe fences a deposed root).
    fn believes_root(&self, cfg: &LockModel, i: usize) -> bool {
        let Some((_, expires)) = self.nodes[i].lease else {
            return false;
        };
        self.local(i) < expires - cfg.lease_margin
    }

    /// The usable end of node `i`'s authority over `X`'s table, in its
    /// clock, if it believes it is the owner now.
    fn owner_until(&self, cfg: &LockModel, i: usize) -> Option<i16> {
        match self.owner {
            Owner::Root => {
                if !self.believes_root(cfg, i) {
                    return None;
                }
                if cfg.backup != NONE
                    && cfg.probe_freshness
                    && self.local(i) - self.nodes[i].last_probe >= cfg.takeover_window
                {
                    return None;
                }
                let (_, expires) = self.nodes[i].lease?;
                Some(expires - cfg.lease_margin)
            }
            Owner::Delegate(d) if d as usize == i => {
                let until = self.nodes[i].dtenure?;
                (self.local(i) < until).then_some(until)
            }
            _ => None,
        }
    }

    fn current_owner(&self) -> u8 {
        match self.owner {
            Owner::Root => self.reg.holder,
            Owner::Delegate(d) => d,
            Owner::Recalling(_) => NONE,
        }
    }

    /// Where node `i` sends lock traffic: a node that believes itself
    /// the root asks itself (it learns of a deposition only by probing);
    /// any other node reads the register (an S3 GET) and the delegation
    /// table.
    fn owner_for(&self, cfg: &LockModel, i: usize) -> u8 {
        if self.owner == Owner::Root && self.believes_root(cfg, i) {
            return i as u8;
        }
        self.current_owner()
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

    fn honoured(&self, i: usize) -> Option<Cache> {
        let c = self.nodes[i].cache?;
        (self.local(i) < c.until).then_some(c)
    }

    fn mirror_push(&mut self, cfg: &LockModel, h: usize) {
        if cfg.backup == NONE || !cfg.replicate || cfg.backup as usize == h {
            return;
        }
        self.nodes[h].mirror_ver = self.nodes[h].mirror_ver.wrapping_add(1);
        let ver = self.nodes[h].mirror_ver;
        let table = self.nodes[h].table.clone();
        self.send(Msg::Mirror {
            to: cfg.backup,
            ver,
            table,
        });
    }

    /// Issue a grant at owner `h` to `node`.
    fn grant(&mut self, cfg: &LockModel, h: usize, node: u8, mode: Mode, ttl: i16) -> u8 {
        let now = self.local(h);
        let id = self.fresh_id();
        self.nodes[h].table.retain(|g| g.node != node);
        self.nodes[h].table.push(Grant {
            node,
            mode,
            until: now + ttl + cfg.seq_margin,
            id,
            recalled: false,
        });
        self.nodes[h].table.sort();
        self.issued.push((id, node, mode));
        self.mirror_push(cfg, h);
        id
    }

    fn ttl_at(&self, cfg: &LockModel, h: usize, until: i16) -> i16 {
        let mut ttl = cfg.lock_ttl;
        if cfg.cap_by_lease {
            ttl = ttl.min(until - self.local(h));
        }
        ttl
    }

    /// Try to grant `mode` to `node` at `h`; else recall the conflicting
    /// grants and return `false`.
    fn try_grant(&mut self, cfg: &LockModel, h: usize, node: u8, mode: Mode, sent: i16) -> bool {
        let now = self.local(h);
        self.nodes[h].table.retain(|g| g.until > now);
        if self.nodes[h].floor.is_some_and(|f| now < f) {
            return false;
        }
        let Some(until) = self.owner_until(cfg, h) else {
            return false;
        };
        // A recalled grant of the requester's own: nothing until it is
        // released (the code's rule; re-affirming it lets the release
        // drop a grant with locks under it).
        if self.nodes[h]
            .table
            .iter()
            .any(|g| g.node == node && g.recalled)
        {
            return false;
        }
        let conflicting: Vec<Grant> = self.nodes[h]
            .table
            .iter()
            .filter(|g| g.node != node && g.mode.conflicts(mode))
            .copied()
            .collect();
        if conflicting.is_empty() {
            let ttl = self.ttl_at(cfg, h, until);
            if ttl <= 0 {
                return false;
            }
            let id = self.grant(cfg, h, node, mode, ttl);
            self.send(Msg::LockReply {
                to: node,
                sent,
                ok: Some((id, mode, ttl)),
            });
            return true;
        }
        for g in conflicting {
            if !g.recalled {
                self.send(Msg::Recall {
                    to: g.node,
                    from: h as u8,
                    id: g.id,
                });
                for e in self.nodes[h].table.iter_mut() {
                    if e.id == g.id {
                        e.recalled = true;
                    }
                }
            }
        }
        false
    }

    /// A grant left the table: serve the parks in order.
    fn grant_done(&mut self, cfg: &LockModel, h: usize, id: u8) {
        self.nodes[h].table.retain(|g| g.id != id);
        self.mirror_push(cfg, h);
        self.serve_parks(cfg, h);
    }

    fn serve_parks(&mut self, cfg: &LockModel, h: usize) {
        let parks = std::mem::take(&mut self.nodes[h].parks);
        let mut rest = Vec::new();
        for p in parks {
            if !self.try_grant(cfg, h, p.node, p.mode, p.sent) {
                rest.push(p);
            }
        }
        self.nodes[h].parks = rest;
    }

    /// Node `i`'s application releases: the cache is kept unless recalled.
    fn app_unlock(&mut self, cfg: &LockModel, i: usize) {
        self.nodes[i].held = None;
        if let Some(c) = self.nodes[i].cache {
            if c.recalled {
                self.release_cache(cfg, i);
            }
        }
    }

    fn release_cache(&mut self, cfg: &LockModel, i: usize) {
        let Some(c) = self.nodes[i].cache.take() else {
            return;
        };
        let to = self.owner_for(cfg, i);
        if to != NONE {
            self.send(Msg::Released {
                to,
                from: i as u8,
                id: c.id,
            });
        }
    }

    /// The owner `h` stops being one: its table and parks are gone.
    fn depose_owner(&mut self, h: usize) {
        self.nodes[h].table.clear();
        self.nodes[h].parks.clear();
        self.nodes[h].floor = None;
    }

    fn restamp(&self, cfg: &LockModel, h: usize, table: Vec<Grant>) -> Vec<Grant> {
        let now = self.local(h);
        table
            .into_iter()
            .map(|g| Grant {
                until: now + cfg.lock_ttl + cfg.seq_margin,
                ..g
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Jump(u8, i16),
    Drop(usize),
    Deliver(usize),
    RenewLease(u8),
    /// A TTL takeover, or a fast one by the backup.
    Takeover(u8),
    Probe(u8),
    Start(u8),
    /// (Re)send node `i`'s lock request to the current owner.
    SendLock(u8),
    /// Node `i` renews its cached grant.
    Renew(u8),
    /// Owner `h` drops its expired grants.
    Expire(u8),
    /// Owner `h`'s grace period ended: its parked requests are served.
    FloorPassed(u8),
    Delegate(u8),
    /// Root: graceful recall of the delegation.
    RecallDelegation,
    /// Root: the delegation's TTL and margin passed without an answer.
    RecallByTtl,
}

impl Model for LockModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.n();
        let off = self.init_offsets.clone().unwrap_or(vec![0; n]);
        let mut nodes: Vec<Node> = (0..n)
            .map(|_| Node {
                lease: None,
                pc: 0,
                op: NodeOp::Idle,
                cache: None,
                held: None,
                renews: 0,
                pending_recalls: Vec::new(),
                table: Vec::new(),
                parks: Vec::new(),
                floor: None,
                last_probe: 0,
                dtenure: None,
                dgen: None,
                mirror: (0, Vec::new()),
                mirror_ver: 0,
            })
            .collect();
        let h = self.initial_holder as usize;
        let expires = off[h] + self.lease_ttl;
        nodes[h].lease = Some((1, expires));
        nodes[h].last_probe = off[h];
        vec![State {
            t: 0,
            off,
            jumps: 0,
            drops: 0,
            reg: Reg {
                holder: h as u8,
                epoch: 1,
                expires,
            },
            owner: Owner::Root,
            moves: 0,
            nodes,
            net: Vec::new(),
            next_id: 0,
            issued: Vec::new(),
            violation: None,
            saw: 0,
        }]
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
        for k in 0..s.net.len() {
            if k == 0 || s.net[k] != s.net[k - 1] {
                if s.drops < self.max_drops {
                    actions.push(Action::Drop(k));
                }
                actions.push(Action::Deliver(k));
            }
        }
        let anyone_wants = (0..n).any(|i| {
            matches!(s.nodes[i].op, NodeOp::Locking { .. })
                || (s.nodes[i].op == NodeOp::Idle
                    && matches!(
                        self.scripts[i].get(s.nodes[i].pc as usize),
                        Some(Op::Lock(_))
                    ))
        });
        for i in 0..n {
            let node = &s.nodes[i];
            let iu = i as u8;
            let now = s.local(i);
            if s.usable_root(self, i) {
                if self.allow_renew {
                    actions.push(Action::RenewLease(iu));
                }
                if self.backup != NONE && now - node.last_probe >= self.takeover_window {
                    actions.push(Action::Probe(iu));
                }
                if self.delegate != NONE
                    && s.owner == Owner::Root
                    && s.moves < self.max_moves
                    && self.delegate != iu
                {
                    actions.push(Action::Delegate(self.delegate));
                }
                if let Owner::Delegate(_) = s.owner {
                    if s.moves < self.max_moves {
                        actions.push(Action::RecallDelegation);
                    }
                }
                if self.recall_by_ttl
                    && matches!(s.owner, Owner::Delegate(_) | Owner::Recalling(_))
                    && node.dgen.is_some_and(|u| now >= u)
                {
                    actions.push(Action::RecallByTtl);
                }
            } else if node.lease.is_some() && self.backup != NONE && s.reg.holder != iu {
                actions.push(Action::Probe(iu));
            }
            let ttl_claimable = self.ttl_takeover && now >= s.reg.expires;
            let fast = self.backup == iu && s.reg.holder != iu;
            if (ttl_claimable || fast) && anyone_wants && s.reg.epoch <= self.max_epoch {
                actions.push(Action::Takeover(iu));
            }
            if node.table.iter().any(|g| g.until <= now) {
                actions.push(Action::Expire(iu));
            }
            // A request to an owner that cannot answer yet would only be
            // retried after a timeout: the node waits for one (a state
            // the checker reaches through `Tick`/`Takeover`/`Deliver`).
            let owner_ready = {
                let o = s.owner_for(self, i);
                o != NONE && s.owner_until(self, o as usize).is_some()
            };
            if node.floor.is_some_and(|f| now >= f) {
                actions.push(Action::FloorPassed(iu));
            }
            // Only a grant still honoured is renewed (or reclaimed): a
            // lapsed one is lost, and the node fences itself until it
            // acquires afresh. (A late reclaim of a lapsed grant during a
            // successor's grace is the counterexample the checker found:
            // the old owner had expired it and granted another node.)
            if s.honoured(i).is_some_and(|c| now >= c.renew_at) && node.renews < self.max_renews {
                let in_flight = s.net.iter().any(|m| {
                    matches!(m, Msg::Renew { from, .. } if *from == iu)
                        || matches!(m, Msg::RenewAck { to, .. } if *to == iu)
                });
                if !in_flight && owner_ready {
                    actions.push(Action::Renew(iu));
                }
            }
            match node.op {
                NodeOp::Idle => {
                    if (node.pc as usize) < self.scripts[i].len() {
                        let op = self.scripts[i][node.pc as usize];
                        let ok = match op {
                            Op::Lock(_) => node.held.is_none(),
                            Op::Io => {
                                node.held.is_some() && (!self.fencing || s.honoured(i).is_some())
                            }
                            Op::Unlock => node.held.is_some(),
                        };
                        if ok {
                            actions.push(Action::Start(iu));
                        }
                    }
                }
                NodeOp::Locking { .. } => {
                    let in_flight =
                        s.net.iter().any(|m| {
                            matches!(m, Msg::LockReq { from, .. } if *from == iu)
                                || matches!(m, Msg::LockReply { to, .. } if *to == iu)
                        }) || s.nodes.iter().any(|x| x.parks.iter().any(|p| p.node == iu));
                    if !in_flight && owner_ready {
                        actions.push(Action::SendLock(iu));
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
            Action::Deliver(k) => {
                let m = s.net.remove(k);
                deliver(self, &mut s, m);
            }
            Action::RenewLease(i) => {
                let i = i as usize;
                let expires = s.local(i) + self.lease_ttl;
                s.reg.expires = expires;
                s.nodes[i].lease = Some((s.reg.epoch, expires));
                s.nodes[i].last_probe = s.local(i);
            }
            Action::Takeover(i) => {
                let i = i as usize;
                let now = s.local(i);
                let prev = s.reg;
                let fast = now < prev.expires;
                let epoch = prev.epoch + 1;
                let expires = now + self.lease_ttl;
                s.reg = Reg {
                    holder: i as u8,
                    epoch,
                    expires,
                };
                s.nodes[i].lease = Some((epoch, expires));
                s.nodes[i].last_probe = now;
                s.nodes[i].parks.clear();
                s.nodes[i].floor = None;
                // The delegation, if any, is inherited as is (M11): the
                // delegate keeps owning `X` under the new root.
                s.nodes[i].dgen = s.nodes[prev.holder as usize].dgen;
                s.nodes[prev.holder as usize].dgen = None;
                if fast {
                    s.saw |= SAW_FAST_TAKEOVER;
                    let mirrored = std::mem::take(&mut s.nodes[i].mirror).1;
                    if !mirrored.is_empty() {
                        s.saw |= SAW_MIRROR_USED;
                    }
                    s.nodes[i].table = s.restamp(self, i, mirrored);
                    if self.grace {
                        let bound =
                            now + self.takeover_window + self.lock_ttl + 2 * self.seq_margin;
                        let floor = prev.expires.min(bound);
                        if floor > now {
                            s.nodes[i].floor = Some(floor);
                        }
                    }
                } else {
                    s.saw |= SAW_TTL_TAKEOVER;
                    s.nodes[i].table.clear();
                }
            }
            Action::Probe(i) => {
                let i = i as usize;
                let (mine, _) = s.nodes[i].lease?;
                if s.reg.holder != i as u8 || s.reg.epoch != mine {
                    s.nodes[i].lease = None;
                    s.nodes[i].dgen = None;
                    s.depose_owner(i);
                } else {
                    s.nodes[i].last_probe = s.local(i);
                }
            }
            Action::Start(i) => {
                let i = i as usize;
                let op = self.scripts[i][s.nodes[i].pc as usize];
                match op {
                    Op::Lock(mode) => {
                        if let Some(c) = s.honoured(i) {
                            if c.mode.covers(mode) && !c.recalled {
                                s.nodes[i].held = Some(mode);
                                s.saw |= SAW_LOCAL_RELOCK;
                                s.finish_op(i);
                                return Some(s);
                            }
                        }
                        let sent = s.local(i);
                        s.nodes[i].op = NodeOp::Locking { mode, sent };
                        send_lock(self, &mut s, i);
                    }
                    Op::Io => {
                        let held = s.nodes[i].held?;
                        let c = s.nodes[i].cache;
                        let mine = c.map(|c| c.id).unwrap_or(0);
                        if let Some((later, _, _)) = s.issued.iter().find(|(id, node, m)| {
                            *id > mine && *node != i as u8 && m.conflicts(held)
                        }) {
                            s.violation = Some((i as u8, mine, *later));
                        }
                        s.finish_op(i);
                    }
                    Op::Unlock => {
                        s.app_unlock(self, i);
                        s.finish_op(i);
                    }
                }
            }
            Action::SendLock(i) => {
                let i = i as usize;
                let NodeOp::Locking { mode, .. } = s.nodes[i].op else {
                    return None;
                };
                let sent = s.local(i);
                s.nodes[i].op = NodeOp::Locking { mode, sent };
                send_lock(self, &mut s, i);
            }
            Action::Renew(i) => {
                let i = i as usize;
                let c = s.nodes[i].cache?;
                let to = s.owner_for(self, i);
                s.nodes[i].renews += 1;
                let sent = s.local(i);
                s.send(Msg::Renew {
                    from: i as u8,
                    to,
                    id: c.id,
                    mode: c.mode,
                    sent,
                });
            }
            Action::FloorPassed(h) => {
                let h = h as usize;
                s.nodes[h].floor = None;
                s.serve_parks(self, h);
            }
            Action::Expire(h) => {
                let h = h as usize;
                let now = s.local(h);
                let expired: Vec<Grant> = s.nodes[h]
                    .table
                    .iter()
                    .filter(|g| g.until <= now)
                    .copied()
                    .collect();
                for g in expired {
                    if g.recalled {
                        s.saw |= SAW_RECALL_EXPIRED;
                    }
                    s.grant_done(self, h, g.id);
                }
            }
            Action::Delegate(d) => {
                let d = d as usize;
                let r = s.reg.holder as usize;
                s.moves += 1;
                s.saw |= SAW_MOVE;
                let table = std::mem::take(&mut s.nodes[r].table);
                s.nodes[r].parks.clear();
                s.nodes[r].dgen = Some(s.local(r) + self.deleg_ttl + self.seq_margin);
                s.nodes[d].dtenure = Some(s.local(d) + self.deleg_ttl - self.node_margin);
                s.nodes[d].table = if self.move_state {
                    s.restamp(self, d, table)
                } else {
                    Vec::new()
                };
                s.owner = Owner::Delegate(d as u8);
            }
            Action::RecallDelegation => {
                let Owner::Delegate(d) = s.owner else {
                    return None;
                };
                s.moves += 1;
                s.owner = Owner::Recalling(d);
                s.send(Msg::DelegRecall { to: d });
            }
            Action::RecallByTtl => {
                let d = match s.owner {
                    Owner::Delegate(d) | Owner::Recalling(d) => d,
                    Owner::Root => return None,
                };
                let r = s.reg.holder as usize;
                s.saw |= SAW_RECALL_BY_TTL;
                s.nodes[r].dgen = None;
                s.nodes[d as usize].dtenure = None;
                s.nodes[d as usize].table.clear();
                s.nodes[d as usize].parks.clear();
                s.owner = Owner::Root;
                s.nodes[r].table.clear();
                // The delegate's own grants were capped by its tenure,
                // but the grants *moved* to it at the delegation carry
                // the windows this root gave them (up to `lock_ttl` from
                // the move, before their first renewal there): a grace
                // period, as after a fast takeover, lets them lapse or
                // reclaim.
                if self.grace {
                    s.nodes[r].floor = Some(s.local(r) + self.lock_ttl + self.seq_margin);
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("mutual_exclusion", |_, s: &State| s.violation.is_none()),
            Property::sometimes("all_ops_done", |m: &LockModel, s: &State| {
                s.nodes
                    .iter()
                    .zip(&m.scripts)
                    .all(|(n, sc)| n.pc as usize == sc.len())
            }),
            Property::sometimes("local_relock", |_, s: &State| s.saw & SAW_LOCAL_RELOCK != 0),
            Property::sometimes("recall_released", |_, s: &State| {
                s.saw & SAW_RECALL_RELEASED != 0
            }),
            Property::sometimes("recall_expired", |_, s: &State| {
                s.saw & SAW_RECALL_EXPIRED != 0
            }),
            Property::sometimes("reclaimed", |_, s: &State| s.saw & SAW_RECLAIM != 0),
            Property::sometimes("moved", |_, s: &State| s.saw & SAW_MOVE != 0),
            Property::sometimes("fast_takeover", |_, s: &State| {
                s.saw & SAW_FAST_TAKEOVER != 0
            }),
            Property::sometimes("ttl_takeover", |_, s: &State| s.saw & SAW_TTL_TAKEOVER != 0),
            Property::sometimes("fenced", |_, s: &State| {
                s.saw & SAW_FENCED != 0
                    || (0..s.nodes.len())
                        .any(|i| s.nodes[i].held.is_some() && s.honoured(i).is_none())
            }),
            Property::sometimes("recall_by_ttl", |_, s: &State| {
                s.saw & SAW_RECALL_BY_TTL != 0
            }),
            Property::sometimes("mirror_used", |_, s: &State| s.saw & SAW_MIRROR_USED != 0),
        ]
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick
    }
}

fn send_lock(cfg: &LockModel, s: &mut State, i: usize) {
    let NodeOp::Locking { mode, sent, .. } = s.nodes[i].op else {
        return;
    };
    let to = s.owner_for(cfg, i);
    if to == NONE {
        return;
    }
    s.send(Msg::LockReq {
        from: i as u8,
        to,
        mode,
        sent,
    });
}

fn deliver(cfg: &LockModel, s: &mut State, m: Msg) {
    match m {
        Msg::LockReq {
            from,
            to,
            mode,
            sent,
        } => {
            let h = to as usize;
            if s.owner_until(cfg, h).is_none() {
                s.send(Msg::LockReply {
                    to: from,
                    sent,
                    ok: None,
                });
                return;
            }
            // A re-sent request of a parked node refreshes its `sent`.
            s.nodes[h].parks.retain(|p| p.node != from);
            if !s.try_grant(cfg, h, from, mode, sent) {
                s.nodes[h].parks.push(Park {
                    node: from,
                    mode,
                    sent,
                });
            }
        }
        Msg::LockReply { to, sent, ok } => {
            let i = to as usize;
            let NodeOp::Locking { mode, sent: mine } = s.nodes[i].op else {
                return;
            };
            if sent != mine {
                // A stale reply to an earlier attempt.
                return;
            }
            let Some((id, gmode, ttl)) = ok else {
                // Not the owner: `SendLock` retries.
                return;
            };
            let recalled = s.nodes[i].pending_recalls.contains(&id);
            s.nodes[i].pending_recalls.clear();
            s.nodes[i].cache = Some(Cache {
                mode: gmode,
                until: sent + ttl - cfg.node_margin,
                renew_at: sent + ttl / 2,
                id,
                recalled,
            });
            s.nodes[i].held = Some(mode);
            s.finish_op(i);
        }
        Msg::Recall { to, from, id } => {
            let i = to as usize;
            match s.nodes[i].cache {
                Some(c) if c.id == id => {
                    if s.nodes[i].held.is_none() {
                        s.nodes[i].cache = None;
                        s.send(Msg::Released {
                            to: from,
                            from: to,
                            id,
                        });
                    } else {
                        s.nodes[i].cache = Some(Cache {
                            recalled: true,
                            ..c
                        });
                    }
                }
                _ => {
                    // Not a grant this node holds (yet): remember it (see
                    // `pending_recalls`). If no reply ever brings it, the
                    // owner outwaits it.
                    if !s.nodes[i].pending_recalls.contains(&id) {
                        s.nodes[i].pending_recalls.push(id);
                        s.nodes[i].pending_recalls.sort();
                    }
                }
            }
        }
        Msg::Released { to, id, .. } => {
            let h = to as usize;
            if s.nodes[h].table.iter().any(|g| g.id == id) {
                s.saw |= SAW_RECALL_RELEASED;
                s.grant_done(cfg, h, id);
            }
        }
        Msg::Renew {
            from,
            to,
            id,
            mode,
            sent,
        } => {
            let h = to as usize;
            let Some(until) = s.owner_until(cfg, h) else {
                s.send(Msg::RenewAck {
                    to: from,
                    id,
                    sent,
                    outcome: RenewOutcome::NotOwner,
                });
                return;
            };
            let now = s.local(h);
            s.nodes[h].table.retain(|g| g.until > now);
            let ttl = s.ttl_at(cfg, h, until);
            let known = s.nodes[h]
                .table
                .iter()
                .any(|g| g.id == id && g.node == from);
            let outcome = if known {
                let mut recalled = false;
                for g in s.nodes[h].table.iter_mut() {
                    if g.id == id {
                        g.until = g.until.max(now + ttl + cfg.seq_margin);
                        recalled = g.recalled;
                    }
                }
                RenewOutcome::Ok(ttl, recalled)
            } else if s.nodes[h].floor.is_some_and(|f| now < f)
                && !s.nodes[h]
                    .table
                    .iter()
                    .any(|g| g.node != from && g.mode.conflicts(mode))
                && ttl > 0
            {
                // A reclaim during the grace period.
                s.nodes[h].table.retain(|g| g.node != from);
                s.nodes[h].table.push(Grant {
                    node: from,
                    mode,
                    until: now + ttl + cfg.seq_margin,
                    id,
                    recalled: false,
                });
                s.nodes[h].table.sort();
                s.mirror_push(cfg, h);
                s.saw |= SAW_RECLAIM;
                RenewOutcome::Ok(ttl, false)
            } else {
                RenewOutcome::Lost
            };
            if ttl <= 0 && known {
                // Nothing to extend with: the node keeps what it has.
                return;
            }
            s.send(Msg::RenewAck {
                to: from,
                id,
                sent,
                outcome,
            });
        }
        Msg::RenewAck {
            to,
            id,
            sent,
            outcome,
        } => {
            let i = to as usize;
            let Some(c) = s.nodes[i].cache else {
                return;
            };
            if c.id != id {
                return;
            }
            match outcome {
                RenewOutcome::Ok(ttl, recalled) => {
                    s.nodes[i].cache = Some(Cache {
                        until: c.until.max(sent + ttl - cfg.node_margin),
                        renew_at: c.renew_at.max(sent + ttl / 2),
                        recalled: c.recalled || recalled,
                        ..c
                    });
                    if recalled && s.nodes[i].held.is_none() {
                        s.release_cache(cfg, i);
                    }
                }
                RenewOutcome::Lost => {
                    s.nodes[i].cache = None;
                    if s.nodes[i].held.is_some() {
                        s.saw |= SAW_FENCED;
                    }
                }
                RenewOutcome::NotOwner => {}
            }
        }
        Msg::Mirror { to, ver, table } => {
            let b = to as usize;
            if ver > s.nodes[b].mirror.0 {
                s.nodes[b].mirror = (ver, table);
            }
        }
        Msg::DelegRecall { to } => {
            let d = to as usize;
            let r = s.reg.holder;
            s.nodes[d].dtenure = None;
            s.nodes[d].parks.clear();
            let table = std::mem::take(&mut s.nodes[d].table);
            s.send(Msg::DelegRecalled {
                to: r,
                table: if cfg.move_state { table } else { Vec::new() },
            });
        }
        Msg::DelegRecalled { to, table } => {
            let r = to as usize;
            let Owner::Recalling(_) = s.owner else {
                return;
            };
            if !s.usable_root(cfg, r) {
                return;
            }
            s.nodes[r].dgen = None;
            s.nodes[r].table = s.restamp(cfg, r, table);
            s.owner = Owner::Root;
            s.mirror_push(cfg, r);
        }
    }
}
