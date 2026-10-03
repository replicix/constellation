//! Plan 30 §M9: the synchronous backup, seal-based failover and `ack=s3`
//! — "no acknowledged write is lost under any single failure", and the
//! acknowledgement order stays consistent with the log.
//!
//! # Why a model of its own
//!
//! The authority model ([`crate::protocol`]) checks the lease, the log,
//! forwarding, stranding, replay and positions, and treats a holder's
//! acknowledgement as immediate (the L2 window of plan 30 §1.2 is what
//! its `Tentative` rule exempts). M9 changes exactly that: *when* an
//! acknowledgement may be given, and who may take a live lease over.
//! This module is a second, focused `stateright::Model` over the same
//! abstractions — the register (lease object), the log with its epoch
//! markers and create-if-absent slots, a holder's journal, clients that
//! forward writes — plus what M9 adds: the register's `backups`,
//! `config_version` and `ack_policy`; a backup's persisted tail and seal;
//! the holder's append/ack round; the reconfiguration CAS; and a
//! takeover that may happen *before* the lease expires.
//!
//! What it leaves out, and why: requester-side replay by rid (M3, the
//! authority model's; here an acknowledged write that only the dead
//! holder had is simply *lost*, which is the failure the property must
//! catch), clocks (the takeover trigger is nondeterministic — safety must
//! not depend on when a backup decides the holder is dead), and reads
//! (`cto.rs` has the delegation horizon across a fast failover).
//!
//! # The protocol (what `crates/authority/src/core/backup.rs` does)
//!
//! - A client's write is forwarded to the register's holder, which
//!   *executes* it (appends it to its journal) and acknowledges it per
//!   the register's policy: `Local` at once; `Backup` once every backup
//!   *listed in the register* has acknowledged an append carrying it;
//!   `S3` once the slot carrying it landed.
//! - The holder ships its journal as slots (create-if-absent: a PUT from
//!   a holder whose view of the head is stale fails, and the tail it
//!   then reads reveals the marker that deposed it).
//! - A backup persists every append it receives into its tail and
//!   acknowledges it — unless it has *sealed* the epoch, in which case it
//!   answers `Sealed` and never acknowledges that epoch again.
//! - A backup may seal and take over at any moment (silence detection is
//!   liveness): it reads the register, and only if it is still listed
//!   for that epoch CASes the register to itself at `epoch + 1`, tails to
//!   head, appends its marker, and re-ships what is left of its tail
//!   (dropping what the log already has).
//! - Under `S3` any node may take a live lease over at any moment.
//! - The holder may remove a backup (a CAS bumping `config_version`);
//!   the acknowledgements that needed it wait for the CAS to land. It
//!   may add one: stream the journal to it first, CAS it in once it has
//!   caught up.
//! - A takeover after the lease expired (`Tick`s) is today's TTL path.
//!
//! # Model action → code
//!
//! | Model | Code |
//! |---|---|
//! | `Start(i)` / `Deliver(Forward)` | `holder.rs` `on_mutate_request` → `holder_execute`, then `ack_need` → `park_reply` |
//! | `Deliver(Append)` / `Deliver(AppendAck)` | `backup.rs` `on_backup_append` (persist, or `sealed`) / `on_backup_ack` → `durable_jseq` → `complete_ready` |
//! | `Ship(h)` | `jobs.rs` `issue_ship` → `ship_landed` → `note_shipped` (`S3` acks) |
//! | `Tail(i)` | `apply_incoming` (a higher epoch deposes the holder at once; a backup trims its tail) |
//! | `Seal(b)` / `Takeover(b)` | `on_backup_watch` (read the register) → `on_takeover_get` (listed? persist the seal; unlisted: give the role up, unsealed) → `Acquire` with the permit → marker → `apply_backup_tail` |
//! | `TakeoverS3(i)` | `watch_s3_holder` → the permit → `classify` |
//! | `Remove(h, b)` / `Add(h, b)` | `drop_backup` / `backup_promote` → `issue_reconfig` (a CAS on the object's version) |
//! | `Crash(i)` | fail-stop; the sim's `CrashHolder` |
//!
//! # Properties
//!
//! - `acked_never_lost` (always): every write acknowledged *durably* —
//!   while the register listed a backup, or under `S3` — is in the log,
//!   or in the journal (or stranded replay set) of a *live* node, or in
//!   the tail of a *live* node listed as a backup for the register's
//!   epoch. A live deposed holder's stranded journal counts: plan 30
//!   §M3b replays it by rid through the new holder. A write that only a
//!   dead node has is lost. With at most one crash this is the plan's "no
//!   acked op is lost under any single failure"; an acknowledgement given
//!   with no backup listed is today's Layer A and makes no such promise.
//! - `ack_order_is_log_order` (always): a write acknowledged before
//!   another was invoked precedes it in the log — the log is a
//!   linearization of the acknowledged writes.
//! - `sometimes` witnesses: a seal-based takeover, an `S3` fast takeover,
//!   a removal, a tail re-shipped, every write acknowledged and in the
//!   log (convergence).

use stateright::{Model, Property};

pub const NONE: u8 = u8::MAX;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Policy {
    Local,
    Backup,
    S3,
}

#[derive(Clone, Debug)]
pub struct BackupModel {
    /// Node count; node 0 holds at start.
    pub nodes: u8,
    /// Writes per node (each node's client issues them in order).
    pub writes_per_node: u8,
    pub policy: Policy,
    /// Nodes listed as backups in the initial register (`Backup` only).
    pub initial_backups: Vec<u8>,
    pub max_crashes: u8,
    pub max_reconfigs: u8,
    pub max_tick: u8,
    pub lease_ttl: u8,
    pub max_epoch: u8,
    // ---- the rules, each a knob so the checker shows what it prevents ----
    /// A backup takes over only while the register lists it.
    pub takeover_requires_listing: bool,
    /// The holder acknowledges nothing that needed a removed backup until
    /// the removal CAS landed.
    pub wait_for_reconfig: bool,
    /// `S3`: acknowledge on landing, not on issuing the PUT.
    pub ack_on_landing: bool,
    /// Allow takeovers of an unexpired lease at all (the fast path).
    pub fast_takeover: bool,
    /// Allow reconfiguration actions.
    pub reconfig: bool,
    /// Check every acknowledgement, not only the durable ones (to show
    /// what today's `Local` acknowledgement loses).
    pub check_all_acks: bool,
}

impl BackupModel {
    pub fn backup(nodes: u8, writes_per_node: u8) -> Self {
        BackupModel {
            nodes,
            writes_per_node,
            policy: Policy::Backup,
            initial_backups: vec![1],
            max_crashes: 1,
            max_reconfigs: 1,
            max_tick: 3,
            lease_ttl: 3,
            max_epoch: 3,
            takeover_requires_listing: true,
            wait_for_reconfig: true,
            ack_on_landing: true,
            fast_takeover: true,
            reconfig: true,
            check_all_acks: false,
        }
    }

    pub fn local(nodes: u8, writes_per_node: u8) -> Self {
        BackupModel {
            policy: Policy::Local,
            initial_backups: Vec::new(),
            reconfig: false,
            check_all_acks: true,
            ..Self::backup(nodes, writes_per_node)
        }
    }

    pub fn s3(nodes: u8, writes_per_node: u8) -> Self {
        BackupModel {
            policy: Policy::S3,
            initial_backups: Vec::new(),
            reconfig: false,
            ..Self::backup(nodes, writes_per_node)
        }
    }
}

/// The lease object.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Reg {
    pub holder: u8,
    pub epoch: u8,
    pub expires: u8,
    pub backups: Vec<u8>,
    pub config_version: u8,
    pub policy: Policy,
    /// The object's version (`If-Match`): every write to it bumps this,
    /// and a CAS names the version it read.
    pub version: u8,
}

/// One log slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Slot {
    Marker(u8),
    /// A write (its id) shipped under `epoch`.
    Write {
        id: u8,
        epoch: u8,
    },
}

/// A write's lifecycle on its client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WriteState {
    Pending,
    /// Forwarded (or executing locally); waiting for the ack.
    InFlight,
    Acked,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub alive: bool,
    /// The lease this node believes it holds: `(epoch, register version
    /// it last saw)`.
    pub lease: Option<u8>,
    pub applied: u8,
    /// Unshipped writes (ids), in journal order, with the acks each
    /// still needs: `(id, needs: backups not yet acked)`.
    pub journal: Vec<(u8, Vec<u8>)>,
    /// As a backup: `(holder, epoch)` backed, and the tail held.
    pub backing: Option<(u8, u8)>,
    pub tail: Vec<u8>,
    /// A deposed holder's stranded writes, kept for replay by rid (plan
    /// 30 §M3b); credited while this node is alive.
    pub replay: Vec<u8>,
    /// The highest epoch sealed.
    pub sealed: u8,
    /// This node's client's writes.
    pub writes: Vec<WriteState>,
    /// `S3`: writes whose PUT is in flight (acked on landing).
    pub putting: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Msg {
    Forward {
        from: u8,
        to: u8,
        id: u8,
    },
    /// `ok == false`: not the holder (retry).
    ForwardAck {
        to: u8,
        id: u8,
        ok: bool,
    },
    /// The holder's append: the write, the epoch, the config version.
    Append {
        from: u8,
        to: u8,
        id: u8,
        epoch: u8,
    },
    AppendAck {
        from: u8,
        to: u8,
        id: u8,
        epoch: u8,
        sealed: bool,
    },
}

pub const SAW_SEAL_TAKEOVER: u8 = 1;
pub const SAW_S3_TAKEOVER: u8 = 2;
pub const SAW_REMOVE: u8 = 4;
pub const SAW_TAIL_RESHIPPED: u8 = 8;
pub const SAW_ADD: u8 = 16;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    pub t: u8,
    pub reg: Reg,
    pub log: Vec<Slot>,
    pub nodes: Vec<Node>,
    pub net: Vec<Msg>,
    pub crashes: u8,
    pub reconfigs: u8,
    /// A removal the holder decided but has not CASed yet: `(holder,
    /// backup)`.
    pub pending_remove: Option<(u8, u8)>,
    /// Ghost: acknowledged write ids, in acknowledgement order, with the
    /// ack tick and whether the acknowledgement was durable (a backup
    /// listed, or `S3`); the invocation ticks of every write are in
    /// `invoked[id]`.
    pub acked: Vec<(u8, u8, bool)>,
    pub invoked: Vec<u8>,
    pub saw: u8,
    /// A rule was violated: `(what, write id)`.
    pub violation: Option<(&'static str, u8)>,
}

impl State {
    fn write_id(&self, node: u8, k: u8) -> u8 {
        node * 16 + k
    }

    fn in_log(&self, id: u8) -> bool {
        self.log
            .iter()
            .any(|s| matches!(s, Slot::Write { id: w, .. } if *w == id))
    }

    /// The node that may execute and ship: holds the register's epoch
    /// and is alive.
    fn is_holder(&self, i: usize) -> bool {
        self.nodes[i].alive
            && self.reg.holder == i as u8
            && self.nodes[i].lease == Some(self.reg.epoch)
    }

    fn send(&mut self, m: Msg) {
        self.net.push(m);
        self.net.sort();
    }

    fn client_of(&self, id: u8) -> (usize, usize) {
        ((id / 16) as usize, (id % 16) as usize)
    }

    fn ack(&mut self, id: u8) {
        let (n, k) = self.client_of(id);
        if self.nodes[n].writes[k] == WriteState::InFlight {
            self.nodes[n].writes[k] = WriteState::Acked;
            let t = self.t;
            let durable = self.reg.policy == Policy::S3 || !self.reg.backups.is_empty();
            self.acked.push((id, t, durable));
        }
    }

    fn check_lost(&mut self, all: bool) {
        for (id, _, durable) in self.acked.clone() {
            if (!durable && !all) || self.in_log(id) {
                continue;
            }
            let held_by_live = self.nodes.iter().any(|n| {
                n.alive && (n.journal.iter().any(|(w, _)| *w == id) || n.replay.contains(&id))
            });
            let held_by_backup = self.reg.backups.iter().any(|b| {
                let n = &self.nodes[*b as usize];
                n.alive
                    && n.backing == Some((self.reg.holder, self.reg.epoch))
                    && n.tail.contains(&id)
            });
            if !held_by_live && !held_by_backup && self.violation.is_none() {
                self.violation = Some(("acked write lost", id));
            }
        }
    }

    fn check_order(&mut self) {
        let pos = |s: &State, id: u8| {
            s.log
                .iter()
                .position(|x| matches!(x, Slot::Write { id: w, .. } if *w == id))
        };
        for (a, ta, _) in self.acked.clone() {
            for (b, _, _) in self.acked.clone() {
                if a == b {
                    continue;
                }
                let tb = self.invoked[b as usize];
                if ta < tb {
                    if let (Some(pa), Some(pb)) = (pos(self, a), pos(self, b)) {
                        if pa > pb && self.violation.is_none() {
                            self.violation = Some(("ack order violated", b));
                        }
                    }
                }
            }
        }
    }

    /// The holder appends `id` to its journal and starts its
    /// acknowledgement per policy.
    fn execute(&mut self, cfg: &BackupModel, h: usize, id: u8) {
        let epoch = self.reg.epoch;
        match self.reg.policy {
            Policy::Local => {
                self.nodes[h].journal.push((id, Vec::new()));
                self.ack(id);
            }
            Policy::Backup => {
                let needs = self.reg.backups.clone();
                self.nodes[h].journal.push((id, needs.clone()));
                for b in needs {
                    self.send(Msg::Append {
                        from: h as u8,
                        to: b,
                        id,
                        epoch,
                    });
                }
                self.try_ack_journal(cfg, h);
            }
            Policy::S3 => {
                self.nodes[h].journal.push((id, Vec::new()));
                if !cfg.ack_on_landing {
                    self.ack(id);
                }
            }
        }
    }

    /// Acknowledge every journaled write that needs no more backup acks
    /// (and, with `wait_for_reconfig`, none while a removal is pending —
    /// modeled by the removal being a single atomic CAS action here, so
    /// the wait is "the needs are re-derived from the register").
    fn try_ack_journal(&mut self, cfg: &BackupModel, h: usize) {
        if self.reg.policy != Policy::Backup {
            return;
        }
        let _ = cfg;
        let ready: Vec<u8> = self.nodes[h]
            .journal
            .iter()
            .filter(|(_, needs)| needs.is_empty())
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            self.ack(id);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Tick,
    Start(u8),
    Deliver(usize),
    /// The holder ships its journal head as the next slot.
    Ship(u8),
    Tail(u8),
    /// A backup seals the epoch it backs (its takeover decision; from
    /// here it answers every append of that epoch with `Sealed`).
    Seal(u8),
    /// A sealed, listed backup takes the lease over.
    Takeover(u8),
    /// `S3`: a peer takes the unexpired lease over.
    TakeoverS3(u8),
    /// Anyone takes an expired lease over.
    TakeoverExpired(u8),
    /// The holder decides to remove a backup (an ack timeout), then CASes
    /// the register. With `wait_for_reconfig` the acknowledgements that
    /// needed it wait for the CAS; without, they leave at the decision.
    RemoveDecide(u8, u8),
    RemoveCas(u8, u8),
    Add(u8, u8),
    Crash(u8),
}

impl Model for BackupModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        let n = self.nodes as usize;
        let nodes: Vec<Node> = (0..n)
            .map(|i| Node {
                alive: true,
                lease: (i == 0).then_some(1),
                applied: 1,
                journal: Vec::new(),
                backing: (self.policy == Policy::Backup
                    && self.initial_backups.contains(&(i as u8)))
                .then_some((0, 1)),
                tail: Vec::new(),
                replay: Vec::new(),
                sealed: 0,
                writes: vec![WriteState::Pending; self.writes_per_node as usize],
                putting: Vec::new(),
            })
            .collect();
        vec![State {
            t: 0,
            reg: Reg {
                holder: 0,
                epoch: 1,
                expires: self.lease_ttl,
                backups: if self.policy == Policy::Backup {
                    self.initial_backups.clone()
                } else {
                    Vec::new()
                },
                config_version: 1,
                policy: self.policy,
                version: 1,
            },
            log: vec![Slot::Marker(1)],
            nodes,
            net: Vec::new(),
            crashes: 0,
            reconfigs: 0,
            pending_remove: None,
            acked: Vec::new(),
            invoked: vec![0; 16 * n],
            saw: 0,
            violation: None,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.violation.is_some() {
            return;
        }
        if s.t < self.max_tick {
            actions.push(Action::Tick);
        }
        for k in 0..s.net.len() {
            if k == 0 || s.net[k] != s.net[k - 1] {
                actions.push(Action::Deliver(k));
            }
        }
        for i in 0..self.nodes as usize {
            let node = &s.nodes[i];
            let iu = i as u8;
            if !node.alive {
                continue;
            }
            if s.crashes < self.max_crashes {
                actions.push(Action::Crash(iu));
            }
            if node.writes.contains(&WriteState::Pending)
                && !node.writes.contains(&WriteState::InFlight)
            {
                actions.push(Action::Start(iu));
            }
            if (node.applied as usize) < s.log.len() {
                actions.push(Action::Tail(iu));
            }
            if s.is_holder(i) && !node.journal.is_empty() && node.applied as usize == s.log.len() {
                actions.push(Action::Ship(iu));
            }
            // Backups.
            if let Some((h, e)) = node.backing {
                if e == s.reg.epoch && h == s.reg.holder && s.reg.epoch < self.max_epoch {
                    if node.sealed < e {
                        actions.push(Action::Seal(iu));
                    }
                    if node.sealed >= e && self.fast_takeover {
                        actions.push(Action::Takeover(iu));
                    }
                }
            }
            // `S3` fast takeover by anyone.
            if s.reg.policy == Policy::S3
                && self.fast_takeover
                && s.reg.holder != iu
                && s.reg.epoch < self.max_epoch
            {
                actions.push(Action::TakeoverS3(iu));
            }
            // TTL takeover: a live holder renews (a round it always
            // runs), so an expired lease is a dead holder's.
            if s.t >= s.reg.expires
                && s.reg.holder != iu
                && s.reg.epoch < self.max_epoch
                && (s.reg.holder == NONE || !s.nodes[s.reg.holder as usize].alive)
            {
                actions.push(Action::TakeoverExpired(iu));
            }
            // Reconfiguration by the holder.
            if self.reconfig && s.is_holder(i) {
                match s.pending_remove {
                    Some((h, b)) if h == iu => {
                        // The removal CAS waits until nothing the holder
                        // acknowledged rests on the removed backup alone:
                        // every acknowledged write still in the journal
                        // is on a backup that stays.
                        let stays: Vec<u8> =
                            s.reg.backups.iter().copied().filter(|x| *x != b).collect();
                        let safe = node.journal.iter().all(|(id, _)| {
                            let (n, k) = s.client_of(*id);
                            s.nodes[n].writes[k] != WriteState::Acked
                                || (!stays.is_empty()
                                    && stays.iter().all(|x| s.nodes[*x as usize].tail.contains(id)))
                        });
                        if safe || !self.wait_for_reconfig {
                            actions.push(Action::RemoveCas(iu, b));
                        }
                    }
                    Some(_) => {}
                    None if s.reconfigs < self.max_reconfigs => {
                        for b in &s.reg.backups {
                            actions.push(Action::RemoveDecide(iu, *b));
                        }
                        for b in 0..self.nodes {
                            if b != iu
                                && !s.reg.backups.contains(&b)
                                && s.nodes[b as usize].alive
                                && s.nodes[b as usize].lease.is_none()
                            {
                                actions.push(Action::Add(iu, b));
                            }
                        }
                    }
                    None => {}
                }
            }
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut s = last.clone();
        match action {
            Action::Tick => s.t += 1,
            Action::Start(i) => {
                let i = i as usize;
                let k = s.nodes[i]
                    .writes
                    .iter()
                    .position(|w| *w == WriteState::Pending)?;
                let id = s.write_id(i as u8, k as u8);
                s.nodes[i].writes[k] = WriteState::InFlight;
                s.invoked[id as usize] = s.t;
                if s.is_holder(i) {
                    s.execute(self, i, id);
                } else if s.reg.holder != NONE {
                    let to = s.reg.holder;
                    s.send(Msg::Forward {
                        from: i as u8,
                        to,
                        id,
                    });
                } else {
                    return None;
                }
            }
            Action::Deliver(k) => {
                let m = s.net.remove(k);
                deliver(self, &mut s, m);
            }
            Action::Ship(h) => {
                let h = h as usize;
                let (id, _) = s.nodes[h].journal.remove(0);
                let epoch = s.reg.epoch;
                s.log.push(Slot::Write { id, epoch });
                s.nodes[h].applied = s.log.len() as u8;
                if s.reg.policy == Policy::S3 && self.ack_on_landing {
                    s.ack(id);
                }
                // Backups trim by the log when they tail; the holder's
                // journal no longer needs their acks for this write.
            }
            Action::Tail(i) => {
                let i = i as usize;
                let slot = s.log[s.nodes[i].applied as usize];
                s.nodes[i].applied += 1;
                match slot {
                    Slot::Marker(e) => {
                        if s.nodes[i].lease.is_some_and(|mine| mine < e) {
                            // Deposed: the journal is stranded and
                            // replayed by rid (plan 30 §M3b).
                            s.nodes[i].lease = None;
                            let stranded: Vec<u8> =
                                s.nodes[i].journal.drain(..).map(|(w, _)| w).collect();
                            s.nodes[i].replay.extend(stranded);
                            s.nodes[i].putting.clear();
                        }
                        if s.nodes[i].backing.is_some_and(|(_, be)| be < e) {
                            s.nodes[i].backing = None;
                            s.nodes[i].tail.clear();
                        }
                    }
                    Slot::Write { id, .. } => {
                        s.nodes[i].tail.retain(|w| *w != id);
                        s.nodes[i].replay.retain(|w| *w != id);
                    }
                }
            }
            Action::Seal(b) => {
                let b = b as usize;
                let (_, e) = s.nodes[b].backing?;
                s.nodes[b].sealed = e;
            }
            Action::Takeover(b) => {
                let b = b as usize;
                let (h, e) = s.nodes[b].backing?;
                if self.takeover_requires_listing && !s.reg.backups.contains(&(b as u8)) {
                    return None;
                }
                if s.reg.holder != h || s.reg.epoch != e {
                    return None;
                }
                takeover(self, &mut s, b);
                s.saw |= SAW_SEAL_TAKEOVER;
            }
            Action::TakeoverS3(i) => {
                takeover(self, &mut s, i as usize);
                s.saw |= SAW_S3_TAKEOVER;
            }
            Action::TakeoverExpired(i) => {
                takeover(self, &mut s, i as usize);
            }
            Action::RemoveDecide(h, b) => {
                let h = h as usize;
                s.pending_remove = Some((h as u8, b));
                if !self.wait_for_reconfig {
                    for (_, needs) in s.nodes[h].journal.iter_mut() {
                        needs.retain(|x| *x != b);
                    }
                    let ready: Vec<u8> = s.nodes[h]
                        .journal
                        .iter()
                        .filter(|(_, needs)| needs.is_empty())
                        .map(|(id, _)| *id)
                        .collect();
                    for id in ready {
                        s.ack(id);
                    }
                }
            }
            Action::RemoveCas(h, b) => {
                let h = h as usize;
                s.pending_remove = None;
                s.reg.backups.retain(|x| *x != b);
                s.reg.config_version += 1;
                s.reg.version += 1;
                s.reconfigs += 1;
                s.saw |= SAW_REMOVE;
                // The removed backup does not learn it is unlisted until
                // it reads the register: it keeps backing, and its
                // takeover attempt is what checks the listing
                // (`takeover_requires_listing`).
                for (_, needs) in s.nodes[h].journal.iter_mut() {
                    needs.retain(|x| *x != b);
                }
                s.try_ack_journal(self, h);
            }
            Action::Add(h, b) => {
                let h = h as usize;
                // Stream the journal first (the candidate holds it all),
                // then the CAS: modeled atomically as "the backup holds
                // every journaled write and is listed".
                let epoch = s.reg.epoch;
                let ids: Vec<u8> = s.nodes[h].journal.iter().map(|(id, _)| *id).collect();
                s.nodes[b as usize].backing = Some((h as u8, epoch));
                s.nodes[b as usize].tail = ids;
                s.reg.backups.push(b);
                s.reg.backups.sort();
                s.reg.config_version += 1;
                s.reg.version += 1;
                s.reconfigs += 1;
                s.saw |= SAW_ADD;
                // Writes journaled from now on need it; earlier ones were
                // already covered by the streamed tail.
            }
            Action::Crash(i) => {
                let i = i as usize;
                s.nodes[i].alive = false;
                s.nodes[i].journal.clear();
                s.nodes[i].replay.clear();
                s.nodes[i].putting.clear();
                s.crashes += 1;
                if s.pending_remove.is_some_and(|(h, _)| h as usize == i) {
                    s.pending_remove = None;
                }
                // Messages to and from it are lost.
                s.net.retain(|m| match m {
                    Msg::Forward { from, to, .. } => *from != i as u8 && *to != i as u8,
                    Msg::ForwardAck { to, .. } => *to != i as u8,
                    Msg::Append { from, to, .. } => *from != i as u8 && *to != i as u8,
                    Msg::AppendAck { from, to, .. } => *from != i as u8 && *to != i as u8,
                });
            }
        }
        s.check_lost(self.check_all_acks);
        s.check_order();
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("acked_never_lost", |_, s: &State| {
                !matches!(s.violation, Some(("acked write lost", _)))
            }),
            Property::always("ack_order_is_log_order", |_, s: &State| {
                !matches!(s.violation, Some(("ack order violated", _)))
            }),
            Property::sometimes("all_acked_and_in_log", |m: &BackupModel, s: &State| {
                s.nodes
                    .iter()
                    .all(|n| n.writes.iter().all(|w| *w == WriteState::Acked))
                    && (0..m.nodes as usize)
                        .all(|i| (0..m.writes_per_node).all(|k| s.in_log(s.write_id(i as u8, k))))
            }),
        ];
        match self.policy {
            Policy::Backup if self.fast_takeover => {
                props.push(Property::sometimes("seal_takeover", |_, s: &State| {
                    s.saw & SAW_SEAL_TAKEOVER != 0
                }));
                props.push(Property::sometimes("tail_reshipped", |_, s: &State| {
                    s.saw & SAW_TAIL_RESHIPPED != 0
                }));
            }
            Policy::S3 if self.fast_takeover => {
                props.push(Property::sometimes("s3_fast_takeover", |_, s: &State| {
                    s.saw & SAW_S3_TAKEOVER != 0
                }));
            }
            _ => {}
        }
        if self.reconfig {
            props.push(Property::sometimes("backup_removed", |_, s: &State| {
                s.saw & SAW_REMOVE != 0
            }));
        }
        props
    }

    fn within_boundary(&self, s: &State) -> bool {
        s.t <= self.max_tick
    }
}

/// Node `i` takes the register over: CAS to itself at `epoch + 1` (no
/// backups; the policy the new tenure asks for is the model's), tails to
/// head, appends its marker, and — as a sealed backup — re-ships what is
/// left of its tail.
fn takeover(cfg: &BackupModel, s: &mut State, i: usize) {
    let from_tail = s.nodes[i].backing == Some((s.reg.holder, s.reg.epoch));
    let epoch = s.reg.epoch + 1;
    s.reg = Reg {
        holder: i as u8,
        epoch,
        expires: s.t + cfg.lease_ttl,
        backups: Vec::new(),
        config_version: s.reg.config_version + 1,
        // The tenure's policy is the model's: `Backup` with no backups
        // yet acknowledges locally and counts nothing as durable until a
        // backup is added (the code's `Local` register policy with a
        // candidate in flight).
        policy: cfg.policy,
        version: s.reg.version + 1,
    };
    // Tail to head.
    while (s.nodes[i].applied as usize) < s.log.len() {
        let slot = s.log[s.nodes[i].applied as usize];
        s.nodes[i].applied += 1;
        if let Slot::Write { id, .. } = slot {
            s.nodes[i].tail.retain(|w| *w != id);
        }
    }
    s.log.push(Slot::Marker(epoch));
    s.nodes[i].applied = s.log.len() as u8;
    s.nodes[i].lease = Some(epoch);
    // The old journal of a node that was a deposed holder is stranded
    // (replayed by rid through itself, plan 30 §M3b's gate).
    let stranded: Vec<u8> = s.nodes[i].journal.drain(..).map(|(w, _)| w).collect();
    s.nodes[i].replay.extend(stranded);
    if from_tail {
        let tail = std::mem::take(&mut s.nodes[i].tail);
        for id in tail {
            if !s.in_log(id) {
                s.nodes[i].journal.push((id, Vec::new()));
                s.saw |= SAW_TAIL_RESHIPPED;
            }
        }
    }
    s.nodes[i].backing = None;
    s.nodes[i].tail.clear();
    // Under `Local`/`Backup`-with-no-backups the re-shipped writes were
    // already acknowledged; nothing new to acknowledge. Under `S3` they
    // ship as slots and ack on landing (already acked ones stay acked).
}

fn deliver(cfg: &BackupModel, s: &mut State, m: Msg) {
    match m {
        Msg::Forward { from, to, id } => {
            let h = to as usize;
            if !s.nodes[h].alive {
                return;
            }
            if s.is_holder(h) {
                s.execute(cfg, h, id);
            } else {
                s.send(Msg::ForwardAck {
                    to: from,
                    id,
                    ok: false,
                });
            }
        }
        Msg::ForwardAck { to, id, ok } => {
            let (n, k) = s.client_of(id);
            if !s.nodes[to as usize].alive {
                return;
            }
            if !ok && s.nodes[n].writes[k] == WriteState::InFlight {
                // Retry against the current holder.
                s.nodes[n].writes[k] = WriteState::Pending;
            }
        }
        Msg::Append {
            from,
            to,
            id,
            epoch,
        } => {
            let b = to as usize;
            if !s.nodes[b].alive {
                return;
            }
            let sealed =
                s.nodes[b].sealed >= epoch || s.nodes[b].lease.is_some_and(|mine| mine >= epoch);
            if !sealed
                && s.nodes[b].backing == Some((from, epoch))
                && !s.nodes[b].tail.contains(&id)
                && !s.in_log(id)
            {
                s.nodes[b].tail.push(id);
            }
            s.send(Msg::AppendAck {
                from: to,
                to: from,
                id,
                epoch,
                sealed: sealed || s.nodes[b].backing != Some((from, epoch)),
            });
        }
        Msg::AppendAck {
            from,
            to,
            id,
            epoch,
            sealed,
        } => {
            let h = to as usize;
            if !s.nodes[h].alive || s.nodes[h].lease != Some(epoch) {
                return;
            }
            if sealed {
                // Never again: the holder must reconfigure it out before
                // it acknowledges anything that needed it (`Remove`).
                return;
            }
            for (w, needs) in s.nodes[h].journal.iter_mut() {
                if *w == id {
                    needs.retain(|x| *x != from);
                }
            }
            s.try_ack_journal(cfg, h);
        }
    }
}
