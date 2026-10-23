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
//! - A takeover after the lease expired (`Tick`s) is today's TTL path: the
//!   claimant carries no tail (a claimant that is itself a backup carries
//!   its own). With [`BackupModel::tail_fetch`] (plan 43 C1-3) the claim of
//!   a lease with listed backups first fetches a live member's tail (and
//!   seals that member), or waits when no member is alive; with
//!   [`BackupModel::inherited_tail`] a takeover that re-ships a tail records
//!   the old group in the register until the log carries the rows, the
//!   group's members keep their copy until then, and a claim of such a
//!   lease with no listed backups fetches from those members.
//!
//! # Member copies (plan 43 C1-4, `commit_rule`)
//!
//! A row is *gated* when it is journaled with a group behind it
//! ([`Class::RootGroup`]: a backup listed; [`Class::Archive`]: `S3`) and
//! *lone* otherwise ([`Class::Lone`]: nothing but the holder's disk). A node
//! keeps `(epoch, id, class)` rows it holds as a member of the group of
//! `epoch` in [`Node::member`], outside its journal and tail:
//!
//! - a deposed holder (`Tail` of a higher epoch's marker, or its own claim)
//!   moves its gated journal rows there under its own epoch; only lone rows
//!   go to the replay set (spec §6.3);
//! - a backup that applies a higher epoch's marker keeps its tail there as a
//!   member copy of the old epoch (the real code does so when it reads a
//!   register that moved on); one that reads its own epoch's register
//!   *unlisted* discards it instead (`Unlist`);
//! - a claimant that re-ships a tail (seal takeover or fetch) also moves the
//!   rows it applies into its own member copy of the old epoch, and a
//!   `TakeoverFetch(i, m)` serves from `m`'s tail or from its member copy of
//!   the group's epoch.
//!
//! The drop rules (spec §6.4): (1) coverage — a slot that carries the row
//! (`Tail` of the write, or the shipper's own `Ship`) retires it, identified
//! by the write's id; (2) a marker that follows a tail
//! (`TailFollowsThrough { prev_epoch, through }`, [`State::follows`] records
//! the ids it re-ships) drops the member rows of `prev_epoch` that are not
//! among them; (4) a marker of a higher epoch that follows no tail for an
//! epoch drops only its [`Class::Archive`] rows — a [`Class::RootGroup`] row
//! may be another tenure's inherited tail and stays until coverage
//! ([`BackupModel::drop_member_at_tail_follows`] shows why rows must stay
//! until coverage). There is no abandon in this model (rule 3), and the
//! tail's contiguity (`through`) is the whole tail.
//!
//! # Model action → code
//!
//! | Model | Code |
//! |---|---|
//! | `Start(i)` / `Deliver(Forward)` | `holder.rs` `on_mutate_request` → `holder_execute`, then `ack_need` → `park_reply` |
//! | `Deliver(Append)` / `Deliver(AppendAck)` | `backup.rs` `on_backup_append` (persist, or `sealed`) / `on_backup_ack` → `durable_jseq` → `complete_ready` |
//! | `Ship(h)` | `jobs.rs` `issue_ship` → `ship_landed` → `note_shipped` (`S3` acks) |
//! | `Tail(i)` | `apply_incoming` (a higher epoch deposes the holder at once: gated rows to the member copy with `commit_rule`, else the replay set; a backup trims its tail, or moves it to a member copy; the drop rules of §6.4) |
//! | `Seal(b)` / `Takeover(b)` | `on_backup_watch` (read the register) → `on_takeover_get` (listed? persist the seal; unlisted: give the role up, unsealed) → `Acquire` with the permit → marker → `apply_backup_tail` |
//! | `TakeoverS3(i)` | `watch_s3_holder` → the permit → `classify` |
//! | `TakeoverFetch(i, m)` | `LeaseState::classify` `Plan::FetchTail` → `on_seal_tail` (the member seals and answers) → the claim CAS with `inherited_tail` (`Lease.inherited_tail`) |
//! | `Unlist(b)` | `on_takeover_get` with the register at the backup's own epoch and the backup unlisted: the tail is discarded (a newer epoch moves it to a member copy instead) |
//! | `Remove(h, b)` / `Add(h, b)` | `drop_backup` / `backup_promote` → `issue_reconfig` (a CAS on the object's version) |
//! | `Crash(i)` | fail-stop; the sim's `CrashHolder` |
//!
//! # Properties
//!
//! - `acked_never_lost` (always): every write acknowledged *durably* —
//!   while the register listed a backup, or under `S3` — is in the log,
//!   or in the journal (or stranded replay set) of a *live* node, or in
//!   the tail of a *live* node listed as a backup for the register's
//!   epoch, or in a member copy a claimant can reach (below). A live deposed holder's stranded journal counts: plan 30
//!   §M3b replays it by rid through the new holder. A write that only a
//!   dead node has is lost. With at most one crash this is the plan's "no
//!   acked op is lost under any single failure"; an acknowledgement given
//!   with no backup listed is today's Layer A and makes no such promise.
//! - `ack_order_is_log_order` (always): a write acknowledged before
//!   another was invoked precedes it in the log — the log is a
//!   linearization of the acknowledged writes.
//! - `acked_never_rolled_back` (always; plan 43 C0): a write acknowledged
//!   durably is never in a node's replay set — a deposed holder replays
//!   what it journaled by rid, and a write the backup acknowledged is
//!   committed, not speculation. **Today it is violated**
//!   (`today_a_deposed_holder_replays_committed_rows`): the holder's journal
//!   is stranded into the replay set whatever the acknowledgement meant.
//!   [`BackupModel::commit_rule`] (plan 43 C1-4) is the knob that fixes it:
//!   a deposed holder's gated rows go to its member copy, never to the
//!   replay set; `false` (today's protocol) in every constructor, and
//!   `assert_clean` in the tests checks the property with the knob on.
//! - A copy held by a member of a register's `inherited` group counts as
//!   holding an acknowledged write (plan 43 C1-3, D2): the lease names that
//!   group, so a claimant can fetch from it. This covers a backup's tail, a
//!   backup's or a deposed holder's member copy of that epoch. Without
//!   `inherited_tail` the copies of the old group are unreachable once the
//!   successor is gone, and `acked_never_lost` reports it; a member copy of
//!   a group the lease does not name (a plain TTL claim without
//!   `tail_fetch`) is unreachable too.
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

/// Plan 43 §5.1: the acknowledgement class a journal row was given at
/// journal time. `Lone` rows (no group existed) are the lone branch's:
/// they may be replayed. `RootGroup` (a listed backup holds it) and
/// `Archive` (`S3`) rows are *gated*: acknowledged only when committed, so a
/// deposed holder never replays them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Class {
    Lone,
    RootGroup,
    Archive,
}

/// One unshipped journal row.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Row {
    pub id: u8,
    /// The backups that have not acknowledged it yet.
    pub needs: Vec<u8>,
    pub class: Class,
}

#[derive(Clone, Debug)]
pub struct BackupModel {
    /// Node count; node 0 holds at start.
    pub nodes: u8,
    /// Writes per node (each node's client issues them in order).
    pub writes_per_node: u8,
    /// Plan 43 C1-3: only nodes `0..writers` issue writes (the others are
    /// holders-to-be and backups); `nodes` in every constructor.
    pub writers: u8,
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
    /// Plan 43 C1-4: a backup-acknowledged (gated) row is committed — a
    /// deposed holder keeps it as a member copy (served to a claimant that
    /// fetches the tail, dropped by the rules of spec §6.4), never in its
    /// replay queue; a backup that reads a newer epoch keeps its tail as a
    /// member copy of the old epoch. `false` is today's protocol (C0 records
    /// its counterexample).
    pub commit_rule: bool,
    /// Plan 43 C1-3: a TTL takeover of a lease whose group may hold
    /// acknowledged rows (listed backups, or with `inherited_tail` an
    /// inherited tail) first fetches a live member's tail and seals that
    /// member; with no live member the claim waits. `false` is today's
    /// protocol: the TTL claimant carries no tail.
    pub tail_fetch: bool,
    /// Plan 43 C1-3 (D2): a takeover that re-ships a predecessor's tail
    /// records the old group in the register until the log carries the
    /// rows; the group's members keep their copy of the tail (they do not
    /// drop it at a newer epoch's marker) until the log carries it; a TTL
    /// claim of such a lease with no listed backups fetches from those
    /// members. `false` is today's protocol.
    pub inherited_tail: bool,
    /// Plan 43 C1-4 (the drop rules' counterexample): a successor's
    /// `TailFollowsThrough` marker drops the whole member copy of
    /// `prev_epoch` at once, instead of only the rows past `through` (§6.4
    /// rule 2) and keeping the rest until the log carries them (rule 1): the
    /// rows it re-ships are then held by the successor alone. `false` is the
    /// design.
    pub drop_member_at_tail_follows: bool,
}

impl BackupModel {
    pub fn backup(nodes: u8, writes_per_node: u8) -> Self {
        BackupModel {
            nodes,
            writes_per_node,
            writers: nodes,
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
            commit_rule: false,
            tail_fetch: false,
            inherited_tail: false,
            drop_member_at_tail_follows: false,
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
    /// Plan 43 C1-3 (D2): the predecessor's group `(epoch, members)` whose
    /// tail this tenure re-ships, until the log carries the rows. A
    /// configuration that has not made its predecessor's tail durable is not
    /// one a claimant may skip.
    pub inherited: Option<(u8, Vec<u8>)>,
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
    /// Unshipped writes, in journal order, with the acks each still needs.
    pub journal: Vec<Row>,
    /// As a backup: `(holder, epoch)` backed, and the tail held.
    pub backing: Option<(u8, u8)>,
    pub tail: Vec<u8>,
    /// The member copy (plan 43 §5.6): `(epoch, id, class)` rows this node
    /// holds as a member of the group of `epoch` — the tail it backed, kept
    /// past a newer epoch's marker (`inherited_tail` or `commit_rule`), and,
    /// with `commit_rule`, a deposed holder's gated journal rows — until the
    /// log carries them or a drop rule retires them.
    pub member: Vec<(u8, u8, Class)>,
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

pub const SAW_SEAL_TAKEOVER: u16 = 1;
pub const SAW_S3_TAKEOVER: u16 = 2;
pub const SAW_REMOVE: u16 = 4;
pub const SAW_TAIL_RESHIPPED: u16 = 8;
pub const SAW_ADD: u16 = 16;
pub const SAW_TAIL_FETCHED: u16 = 32;
pub const SAW_INHERITED_FETCHED: u16 = 64;
pub const SAW_MEMBER_COPY: u16 = 128;
pub const SAW_MEMBER_FETCHED: u16 = 256;
pub const SAW_MEMBER_RETIRED: u16 = 512;

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
    pub saw: u16,
    /// Ghost (plan 43 C1-3): the rows the register's `inherited` tail
    /// re-shipped (the code's `InheritedTail.through`); the register's
    /// field is cleared once the log carries all of them.
    pub inherited_rows: Vec<u8>,
    /// Ghost (plan 43 C1-4): what a marker carries — `(marker epoch,
    /// prev_epoch, ids of the tail it re-ships)` for a marker that writes
    /// `TailFollowsThrough { prev_epoch, through }`; a marker with no entry
    /// carries no tail (a plain claim).
    pub follows: Vec<(u8, u8, Vec<u8>)>,
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
                n.alive && (n.journal.iter().any(|r| r.id == id) || n.replay.contains(&id))
            });
            let held_by_backup = self.reg.backups.iter().any(|b| {
                let n = &self.nodes[*b as usize];
                n.alive
                    && n.backing == Some((self.reg.holder, self.reg.epoch))
                    && n.tail.contains(&id)
            });
            // Plan 43 C1-3 (D2): the register's inherited group is named by the
            // lease, so a claimant can fetch its copy (a live member's tail
            // of that epoch, or its member copy of it: the copy it kept past
            // a newer marker, a deposed holder's gated rows). A member copy of
            // a group the lease does not name is unreachable.
            let held_by_inherited = self.reg.inherited.as_ref().is_some_and(|(ie, members)| {
                members.iter().any(|m| {
                    let n = &self.nodes[*m as usize];
                    n.alive
                        && ((n.backing.is_some_and(|(_, be)| be == *ie) && n.tail.contains(&id))
                            || n.member.iter().any(|(e, w, _)| e == ie && *w == id))
                })
            });
            if !held_by_live && !held_by_backup && !held_by_inherited && self.violation.is_none() {
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
                self.nodes[h].journal.push(Row {
                    id,
                    needs: Vec::new(),
                    class: Class::Lone,
                });
                self.ack(id);
            }
            Policy::Backup => {
                let needs = self.reg.backups.clone();
                // The class is decided at journal time: a listed backup
                // makes the row gated; with none it is the lone branch's.
                let class = if needs.is_empty() {
                    Class::Lone
                } else {
                    Class::RootGroup
                };
                self.nodes[h].journal.push(Row {
                    id,
                    needs: needs.clone(),
                    class,
                });
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
                self.nodes[h].journal.push(Row {
                    id,
                    needs: Vec::new(),
                    class: Class::Archive,
                });
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
            .filter(|r| r.needs.is_empty())
            .map(|r| r.id)
            .collect();
        for id in ready {
            self.ack(id);
        }
    }

    /// Node `i` stops sequencing for `epoch` with `journal` rows left: a
    /// gated row goes to its member copy (`commit_rule`), anything else to
    /// the replay set (plan 30 §M3b, the lone branch).
    fn strand(&mut self, cfg: &BackupModel, i: usize, epoch: u8) {
        let rows: Vec<Row> = std::mem::take(&mut self.nodes[i].journal);
        for r in rows {
            if cfg.commit_rule && r.class != Class::Lone {
                // Keyed by the journal's epoch: a re-shipped row that the
                // node also holds under its original epoch (the tail it
                // applied) is a second, distinct member copy.
                if !self.nodes[i]
                    .member
                    .iter()
                    .any(|(e, w, _)| *e == epoch && *w == r.id)
                {
                    self.nodes[i].member.push((epoch, r.id, r.class));
                    self.saw |= SAW_MEMBER_COPY;
                }
            } else {
                self.nodes[i].replay.push(r.id);
            }
        }
        self.nodes[i].member.sort();
    }

    /// Node `i` applies the next slot of the log (`Tail`).
    fn apply_slot(&mut self, cfg: &BackupModel, i: usize) {
        let slot = self.log[self.nodes[i].applied as usize];
        self.nodes[i].applied += 1;
        match slot {
            Slot::Marker(e) => {
                if let Some(mine) = self.nodes[i].lease.filter(|mine| *mine < e) {
                    // Deposed: the journal is stranded — replayed by rid
                    // (plan 30 §M3b), or with `commit_rule` its gated rows go
                    // to the member copy of `mine`.
                    self.nodes[i].lease = None;
                    self.strand(cfg, i, mine);
                    self.nodes[i].putting.clear();
                }
                if let Some((_, be)) = self.nodes[i].backing.filter(|(_, be)| *be < e) {
                    // Plan 43 C1-3 (`inherited_tail`) / C1-4: a member keeps
                    // its copy of what the log does not carry yet; a
                    // successor's marker is not coverage.
                    if cfg.inherited_tail || cfg.commit_rule {
                        let keep: Vec<u8> = self.nodes[i]
                            .tail
                            .iter()
                            .copied()
                            .filter(|id| !self.in_log(*id))
                            .collect();
                        for id in keep {
                            if !self.nodes[i].member.iter().any(|(_, w, _)| *w == id) {
                                self.nodes[i].member.push((be, id, Class::RootGroup));
                            }
                        }
                        self.nodes[i].member.sort();
                    }
                    self.nodes[i].backing = None;
                    self.nodes[i].tail.clear();
                }
                self.marker_drops(cfg, i, e);
            }
            Slot::Write { id, .. } => {
                self.nodes[i].tail.retain(|w| *w != id);
                let before = self.nodes[i].member.len();
                self.nodes[i].member.retain(|(_, w, _)| *w != id);
                if self.nodes[i].member.len() != before {
                    self.saw |= SAW_MEMBER_RETIRED;
                }
                self.nodes[i].replay.retain(|w| *w != id);
            }
        }
    }

    /// Node `i` applies the marker of epoch `e`: the drop rules of spec §6.4
    /// for its member copies (coverage is `Tail` of the written slot).
    fn marker_drops(&mut self, cfg: &BackupModel, i: usize, e: u8) {
        let follows = self.follows.iter().find(|(fe, _, _)| *fe == e).cloned();
        self.nodes[i].member.retain(|(me, id, class)| {
            if *me >= e {
                return true;
            }
            match &follows {
                // Rule 2: `TailFollowsThrough { prev_epoch, through }` drops
                // the rows the sealer's contiguous hold did not reach.
                Some((_, prev, rows)) if prev == me => {
                    !cfg.drop_member_at_tail_follows && rows.contains(id)
                }
                // Rule 4: a marker with no tail for the epoch drops the
                // never-acknowledged (Archive) rows only; a RootGroup row may
                // be another tenure's inherited tail and stays until covered.
                _ => *class == Class::RootGroup,
            }
        });
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
    /// Plan 43 C1-4 (`commit_rule`): a backup reads the register at its own
    /// epoch, finds itself unlisted (a removal) and discards its tail
    /// (spec §6.3: same epoch, unlisted → discard; a newer epoch moves the
    /// tail to a member copy instead, at `Tail`).
    Unlist(u8),
    /// `S3`: a peer takes the unexpired lease over.
    TakeoverS3(u8),
    /// Anyone takes an expired lease over.
    TakeoverExpired(u8),
    /// Plan 43 C1-3 (`tail_fetch`): anyone takes an expired lease over after
    /// fetching the tail of member `.1` of the lease's group (the member
    /// seals).
    TakeoverFetch(u8, u8),
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
                member: Vec::new(),
                replay: Vec::new(),
                sealed: 0,
                writes: vec![
                    WriteState::Pending;
                    if (i as u8) < self.writers {
                        self.writes_per_node as usize
                    } else {
                        0
                    }
                ],
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
                inherited: None,
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
            inherited_rows: Vec::new(),
            follows: Vec::new(),
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
                    if self.commit_rule && !s.reg.backups.contains(&iu) {
                        actions.push(Action::Unlist(iu));
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
                match self.fetch_group(s) {
                    None => actions.push(Action::TakeoverExpired(iu)),
                    // Plan 43 C1-3: the claim needs one member's tail; with
                    // no live member it waits (no action).
                    Some(g) => {
                        for m in g.candidates {
                            actions.push(Action::TakeoverFetch(iu, m));
                        }
                    }
                }
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
                        let safe = node.journal.iter().all(|r| {
                            let (n, k) = s.client_of(r.id);
                            s.nodes[n].writes[k] != WriteState::Acked
                                || (!stays.is_empty()
                                    && stays
                                        .iter()
                                        .all(|x| s.nodes[*x as usize].tail.contains(&r.id)))
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
                let id = s.nodes[h].journal.remove(0).id;
                let epoch = s.reg.epoch;
                s.log.push(Slot::Write { id, epoch });
                // The log carries the row: the shipper's own member copy of it
                // is covered (spec §6.4 rule 1, `ack_journal` for own segments).
                s.nodes[h].member.retain(|(_, w, _)| *w != id);
                s.nodes[h].applied = s.log.len() as u8;
                if s.reg.policy == Policy::S3 && self.ack_on_landing {
                    s.ack(id);
                }
                // Backups trim by the log when they tail; the holder's
                // journal no longer needs their acks for this write.
            }
            Action::Tail(i) => {
                s.apply_slot(self, i as usize);
            }
            Action::Seal(b) => {
                let b = b as usize;
                let (_, e) = s.nodes[b].backing?;
                s.nodes[b].sealed = e;
            }
            Action::Unlist(b) => {
                let b = b as usize;
                let (h, e) = s.nodes[b].backing?;
                if s.reg.holder != h || s.reg.epoch != e || s.reg.backups.contains(&(b as u8)) {
                    return None;
                }
                s.nodes[b].backing = None;
                s.nodes[b].tail.clear();
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
                if self.fetch_group(&s).is_some() {
                    return None;
                }
                takeover(self, &mut s, i as usize);
            }
            Action::TakeoverFetch(i, m) => {
                let g = self.fetch_group(&s)?;
                if !g.candidates.contains(&m) {
                    return None;
                }
                s.saw |= SAW_TAIL_FETCHED;
                if g.inherited {
                    s.saw |= SAW_INHERITED_FETCHED;
                }
                takeover_from(self, &mut s, i as usize, Some((m as usize, g)));
            }
            Action::RemoveDecide(h, b) => {
                let h = h as usize;
                s.pending_remove = Some((h as u8, b));
                if !self.wait_for_reconfig {
                    for r in s.nodes[h].journal.iter_mut() {
                        r.needs.retain(|x| *x != b);
                    }
                    let ready: Vec<u8> = s.nodes[h]
                        .journal
                        .iter()
                        .filter(|r| r.needs.is_empty())
                        .map(|r| r.id)
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
                for r in s.nodes[h].journal.iter_mut() {
                    r.needs.retain(|x| *x != b);
                }
                s.try_ack_journal(self, h);
            }
            Action::Add(h, b) => {
                let h = h as usize;
                // Stream the journal first (the candidate holds it all),
                // then the CAS: modeled atomically as "the backup holds
                // every journaled write and is listed".
                let epoch = s.reg.epoch;
                let ids: Vec<u8> = s.nodes[h].journal.iter().map(|r| r.id).collect();
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
                s.nodes[i].member.clear();
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
        // Plan 43 C1-3: the first renewal after the log carries the
        // re-shipped rows clears the inherited tail (`InheritedTail` is
        // cleared once `shipped_through` reaches it); modelled as it happens.
        if s.reg.inherited.is_some() && s.inherited_rows.iter().all(|id| s.in_log(*id)) {
            s.reg.inherited = None;
            s.reg.version += 1;
        }
        if s.reg.inherited.is_none() {
            s.inherited_rows.clear();
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
            // Plan 43 C0: a pure state predicate (the `violation` ghost is
            // first-wins). A durably acknowledged write is committed; it is
            // never a deposed holder's replay.
            Property::always("acked_never_rolled_back", |_, s: &State| {
                s.acked.iter().all(|(id, _, durable)| {
                    !durable || s.nodes.iter().all(|n| !n.replay.contains(id))
                })
            }),
            Property::sometimes("all_acked_and_in_log", |m: &BackupModel, s: &State| {
                s.nodes
                    .iter()
                    .all(|n| n.writes.iter().all(|w| *w == WriteState::Acked))
                    && (0..m.writers as usize)
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
        if self.tail_fetch && self.policy == Policy::Backup {
            props.push(Property::sometimes("tail_fetched", |_, s: &State| {
                s.saw & SAW_TAIL_FETCHED != 0
            }));
            if self.inherited_tail {
                props.push(Property::sometimes(
                    "inherited_tail_fetched",
                    |_, s: &State| s.saw & SAW_INHERITED_FETCHED != 0,
                ));
            }
        }
        if self.commit_rule && self.policy == Policy::Backup {
            props.push(Property::sometimes("member_copy_kept", |_, s: &State| {
                s.saw & SAW_MEMBER_COPY != 0
            }));
            props.push(Property::sometimes(
                "member_copy_retired",
                |_, s: &State| s.saw & SAW_MEMBER_RETIRED != 0,
            ));
            if self.tail_fetch && self.inherited_tail {
                props.push(Property::sometimes(
                    "member_copy_fetched",
                    |_, s: &State| s.saw & SAW_MEMBER_FETCHED != 0,
                ));
            }
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

/// The group a TTL claimant must fetch a tail from (plan 43 C1-3).
struct FetchGroup {
    /// The epoch whose tail is fetched.
    epoch: u8,
    /// The group's members (the old holder and its backups).
    members: Vec<u8>,
    /// The members alive and holding a tail (or a member copy) of `epoch`.
    candidates: Vec<u8>,
    /// The group is the register's inherited one, not its listed backups.
    inherited: bool,
}

impl BackupModel {
    /// The group an expired lease's claimant fetches from (`tail_fetch`): the
    /// holder and listed backups of a `Backup` lease with listed backups (a
    /// member that is alive and holds the role: the holder is dead on the
    /// TTL path), else the lease's inherited group (`inherited_tail`). `None`:
    /// no fetch (today's claim, or a lease whose group holds nothing).
    fn fetch_group(&self, s: &State) -> Option<FetchGroup> {
        if !self.tail_fetch || s.reg.policy != Policy::Backup {
            return None;
        }
        if !s.reg.backups.is_empty() {
            let mut members = s.reg.backups.clone();
            members.push(s.reg.holder);
            let candidates = s
                .reg
                .backups
                .iter()
                .copied()
                .filter(|b| {
                    let n = &s.nodes[*b as usize];
                    n.alive && n.backing == Some((s.reg.holder, s.reg.epoch))
                })
                .collect();
            return Some(FetchGroup {
                epoch: s.reg.epoch,
                members,
                candidates,
                inherited: false,
            });
        }
        let (ie, members) = s.reg.inherited.as_ref().filter(|_| self.inherited_tail)?;
        let candidates = members
            .iter()
            .copied()
            .filter(|m| {
                let n = &s.nodes[*m as usize];
                n.alive
                    && (n.backing.is_some_and(|(_, be)| be == *ie)
                        || n.member.iter().any(|(e, _, _)| e == ie))
            })
            .collect();
        Some(FetchGroup {
            epoch: *ie,
            members: members.clone(),
            candidates,
            inherited: true,
        })
    }
}

/// Node `i` takes the register over: CAS to itself at `epoch + 1` (no
/// backups; the policy the new tenure asks for is the model's), tails to
/// head, appends its marker, and — as a sealed backup — re-ships what is
/// left of its tail.
fn takeover(cfg: &BackupModel, s: &mut State, i: usize) {
    takeover_from(cfg, s, i, None);
}

/// [`takeover`], with the tail fetched from member `m` of group `g` (plan 43
/// C1-3, `tail_fetch`): the member seals (it never acknowledges that epoch
/// again) and its tail — or, with `commit_rule`, its member copy — is
/// re-shipped as a seal takeover's is.
fn takeover_from(cfg: &BackupModel, s: &mut State, i: usize, fetched: Option<(usize, FetchGroup)>) {
    let old = s.reg.clone();
    let from_tail = fetched.is_none() && s.nodes[i].backing == Some((old.holder, old.epoch));
    // The tail that is re-shipped, and the group it comes from.
    let mut reship: Vec<u8> = Vec::new();
    let mut group: Option<(u8, Vec<u8>)> = None;
    let mut from_member = false;
    if let Some((m, g)) = &fetched {
        let n = &s.nodes[*m];
        if n.backing.is_some_and(|(_, be)| be == g.epoch) {
            reship.extend(n.tail.iter().copied());
        }
        for (e, id, _) in &n.member {
            if *e == g.epoch && !reship.contains(id) {
                reship.push(*id);
                from_member = true;
            }
        }
        s.nodes[*m].sealed = s.nodes[*m].sealed.max(g.epoch);
        group = Some((g.epoch, g.members.clone()));
    } else if from_tail {
        reship.extend(s.nodes[i].tail.iter().copied());
        let mut members = old.backups.clone();
        members.push(old.holder);
        members.sort();
        group = Some((old.epoch, members));
    }
    let claimant_is_member = fetched.as_ref().is_some_and(|(m, _)| *m == i);
    let epoch = old.epoch + 1;
    s.reg = Reg {
        holder: i as u8,
        epoch,
        expires: s.t + cfg.lease_ttl,
        backups: Vec::new(),
        config_version: old.config_version + 1,
        // The tenure's policy is the model's: `Backup` with no backups
        // yet acknowledges locally and counts nothing as durable until a
        // backup is added (the code's `Local` register policy with a
        // candidate in flight).
        policy: cfg.policy,
        version: old.version + 1,
        inherited: None,
    };
    // Tail to head: the claimant applies every slot it has not seen, a
    // deposed holder's journal and a backup's tail included.
    while (s.nodes[i].applied as usize) < s.log.len() {
        s.apply_slot(cfg, i);
    }
    s.log.push(Slot::Marker(epoch));
    s.nodes[i].applied = s.log.len() as u8;
    // The marker names the tail it carries (`TailFollowsThrough`): the rows
    // of the group the sealer held.
    if let Some((pe, _)) = &group {
        s.follows.push((epoch, *pe, reship.clone()));
    }
    s.marker_drops(cfg, i, epoch);
    let stale = s.nodes[i].lease.unwrap_or(old.epoch);
    s.nodes[i].lease = Some(epoch);
    // The old journal of a node that was a deposed holder is stranded
    // (replayed by rid through itself, plan 30 §M3b's gate; with
    // `commit_rule` its gated rows are member copies).
    s.strand(cfg, i, stale);
    let mut reshipped: Vec<u8> = Vec::new();
    for id in reship {
        if !s.in_log(id) {
            s.nodes[i].journal.push(Row {
                id,
                needs: Vec::new(),
                class: Class::RootGroup,
            });
            reshipped.push(id);
            s.saw |= SAW_TAIL_RESHIPPED;
            // Plan 43 C1-4: the claimant moves the tail it applies into its
            // own member copy of the old epoch (`apply_backup_tail`), so a
            // successor's death before it ships loses no copy.
            if cfg.commit_rule {
                let pe = group.as_ref().map(|(pe, _)| *pe).unwrap_or(old.epoch);
                if !s.nodes[i].member.iter().any(|(_, w, _)| *w == id) {
                    s.nodes[i].member.push((pe, id, Class::RootGroup));
                }
            }
        }
    }
    if from_member {
        s.saw |= SAW_MEMBER_FETCHED;
    }
    s.nodes[i].member.sort();
    // Plan 43 C1-3 (D2): the claim that re-ships a tail names the group the
    // rows came from, so a later claimant can fetch them from it until the
    // log carries them; it replaces the predecessor's inherited tail (the
    // rows that one named are inside this tail).
    if cfg.inherited_tail && !reshipped.is_empty() {
        if let Some(g) = group {
            s.reg.inherited = Some(g);
            s.inherited_rows.extend(reshipped);
        }
    }
    s.nodes[i].backing = None;
    s.nodes[i].tail.clear();
    if claimant_is_member && !cfg.commit_rule {
        // The claimant's own copy became its journal.
        s.nodes[i].member.clear();
    }
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
            for r in s.nodes[h].journal.iter_mut() {
                if r.id == id {
                    r.needs.retain(|x| *x != from);
                }
            }
            s.try_ack_journal(cfg, h);
        }
    }
}
