//! Plan 30 §M6: positions on replies, a per-node `observed` watermark,
//! and the two session guarantees they buy — per-node **read-your-writes**
//! and **monotonic reads** — as model properties.
//!
//! # Reads
//!
//! A client step is a mutation or a read ([`Step`]). A read is a lookup
//! of one name or a readdir of the whole (two-name) directory, served
//! from the node's local replica (`protocol::node_replica`: its applied
//! log prefix, its shadows and its own unshipped journal). Reads are never
//! fed to the linearizability checker — a replica read is not
//! linearizable by design (bounded close-to-open) — and record no history
//! event; they are checked by the two session properties below instead.
//! `Action::Read` is offered when the node's client is idle and its next
//! step is a read, and under `Positions` only once [`read_allowed`] says
//! the session wait is over (the model waits without a bound; the real
//! wait is bounded by `CONSTELLATION_SESSION_WAIT_MS` and then answers
//! degraded, which is a liveness choice outside what the model checks).
//!
//! # What a read must contain (the properties)
//!
//! Session guarantees are defined the classical way, on *write sets*
//! (Terry et al., "Session guarantees for weakly consistent replicated
//! data"), per name: the view a read of name `x` returns is the set of
//! rids of the records touching `x` that its replica has applied or
//! speculates ([`view_mask`]). A value-level definition cannot see the
//! anomaly M5's simulation found (a shadow installed on a stale base is
//! value-correct for `create`/`unlink` of one name, and only diverges
//! with `rename`), and a set-level one can.
//!
//! - **`read_your_writes`**: every write of this node's current session
//!   that returned `Ok` on `x` is in the view. A write whose return M13
//!   round 3a rewrote to `Tentative`/`Conflicted` (a deposed holder's own
//!   acked-but-unshipped op) is exempt, exactly as `linearizable` exempts
//!   it: its return is no longer `Ok`. A `Restart` starts a new session
//!   (the FUSE session dies with the process), so earlier incarnations'
//!   writes are not this session's.
//! - **`monotonic_reads`**: every write the session has *observed* on `x`
//!   is in the view. A client observes the holder's state *of the op's
//!   name* through every reply it returns from — an `EEXIST`/`ENOENT`
//!   refusal and an `Accepted` alike — so the ghost field `Node::seen[x]`
//!   records the newest holder state ([`Obs`]: epoch, applied slot,
//!   journal length) any reply about `x` carried, and [`observed_mask`]
//!   reconstructs its write set on `x` from the log and the journals *at
//!   read time*. Its reads of its own replica
//!   need no record: a replica only moves backwards by rolling
//!   speculation back, which `read_your_writes` covers for the session's
//!   own writes and which is exempt for everyone else's (next point).
//!   The observation's unshipped part is **void** once its tenure is over
//!   without shipping it (the next slot holds another epoch's segment, or
//!   the register moved on): those effects were acknowledged by a holder
//!   deposed before they became durable, M13's `Tentative` case seen by a
//!   third party (M5's finding 6, the L2 window M9 closes), and nothing may
//!   be required to see them in between. Holder-state observations nest
//!   (a later tenure tailed every segment of an earlier one to head before
//!   taking over, and a tenure's journal only grows until it ships whole),
//!   so the newest observation subsumes every earlier one.
//!
//! Inbox-submitted ops (plan 30 §M13) return only once the requester has
//! applied the segment carrying their outcome, which also carries every
//! journal row the holder evaluated them against: the requester's own
//! replica already covers what it observed, so they record nothing and
//! raise no watermark.
//!
//! # The `Positions` rules
//!
//! - A reply carries the answering holder's position `at` ([`Obs`]); its
//!   [`Pos`] is "the slot the holder will ship its journal in, through
//!   this many rows" when the journal is non-empty, else "fully applied
//!   through the holder's applied slot". The real position is
//!   `(epoch, journal_seq)` compared against segments that carry the
//!   journal range they ship; in the model a tenure ships its whole
//!   journal as one segment at the next slot, so the slot plus a row
//!   count is the same order (and a slot taken by a higher epoch's
//!   marker is past every position of the tenure it ended — the void
//!   rule above).
//! - `base` (M5): an `Accepted` record is installed as a shadow only if
//!   the requester has applied the holder's applied slot and the holder's
//!   unshipped journal had not already touched the name; otherwise the
//!   client waits for the rid in the log (`Phase::AwaitingLog`), and a
//!   replay the same way (`ReplayEntry::awaiting`).
//! - `Node::observed` is the maximum `Pos` of the replies the client
//!   returned from *whose effects were not installed here* — refusals
//!   (plan 30 M6 phase 2, coordinator decision 2): an accepted op is
//!   installed (a shadow on an applied base, the applied log, or a queued
//!   replay that blocks reads of its name), so it never makes a read of
//!   another name wait (`touch a; ls; stat .` stays on the fast path).
//! - A read of `x` runs only when no queued replay touches `x`, and either
//!   the applied position has reached `observed` or the newest shadow on
//!   `x` is at least `observed` ("speculation covers the key").

use crate::namespace::{present, DirState, Name, NsOp, NsRet, N_NAMES};
use crate::protocol::{
    log_fold, AuthorityModel, Epoch, HistEvt, NodeId, Outcome, Phase, Protocol, Rid, Segment, Seq,
    State,
};

/// One step of a node's (single, serialized) client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Step {
    Mutate(NsOp),
    /// `lookup`/`getattr`/`open`/`readlink`/`getxattr` of one name.
    Lookup(Name),
    /// `readdir`/`listxattr`-style: every name at once.
    Readdir,
}

impl Step {
    /// The names a read step reads, as a `DirState`-style mask. Empty for
    /// a mutation (mutations are validated by the sequencer and never
    /// wait).
    pub fn names(self) -> DirState {
        match self {
            Step::Mutate(_) => 0,
            Step::Lookup(n) => 1 << n,
            Step::Readdir => (1 << N_NAMES) - 1,
        }
    }
}

/// A log position: through row `j` of the segment at slot `seq`, with
/// `j == u8::MAX` meaning the whole segment (see the module doc for how
/// this stands for the real `(epoch, journal_seq)`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pos {
    pub seq: Seq,
    pub j: u8,
}

impl Pos {
    /// Nothing observed: the position every replica starts at.
    pub const ZERO: Pos = Pos::applied(0);

    /// A replica that has applied every slot through `seq`.
    pub const fn applied(seq: Seq) -> Pos {
        Pos { seq, j: u8::MAX }
    }
}

/// What a reply observed: the answering holder's epoch, applied slot and
/// unshipped journal length right after it evaluated the op. Ordered
/// epoch-first (a later tenure's state subsumes an earlier one's durable
/// part, see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Obs {
    pub epoch: Epoch,
    pub applied: Seq,
    pub journal: u8,
}

impl Obs {
    pub const ZERO: Obs = Obs {
        epoch: 0,
        applied: 0,
        journal: 0,
    };

    /// The reply position this observation stands for.
    pub fn pos(self) -> Pos {
        if self.journal > 0 {
            Pos {
                seq: self.applied + 1,
                j: self.journal,
            }
        } else {
            Pos::applied(self.applied)
        }
    }
}

/// Bits of `State::session_violations`.
pub const RYW_VIOLATED: u8 = 1;
pub const MR_VIOLATED: u8 = 2;

/// The holder side (`holder_execute` answering a forward): the reply's
/// position, and `base` for an `Accepted` whose base is known (a fresh
/// execution whose name the unshipped journal had not touched before).
/// `NotHolder` observes nothing.
pub(crate) fn reply_position(
    s: &State,
    holder: NodeId,
    outcome: Outcome,
    base_known: bool,
) -> (Obs, Option<Seq>) {
    if outcome == Outcome::NotHolder {
        return (Obs::ZERO, None);
    }
    let node = &s.nodes[holder as usize];
    let at = Obs {
        epoch: node.held_epoch.unwrap_or(0),
        applied: node.applied_seq,
        journal: node.journal.len() as u8,
    };
    let base = (matches!(outcome, Outcome::Accepted(..)) && base_known).then_some(node.applied_seq);
    (at, base)
}

/// The requester side: may an `Accepted` record be installed ahead of the
/// log? Always, except under `Positions` when the base is unknown or not
/// yet applied here (`stale_base_shadows` restores the M3a behaviour).
pub(crate) fn base_ok(m: &AuthorityModel, s: &State, id: NodeId, base: Option<Seq>) -> bool {
    m.protocol != Protocol::Positions
        || m.stale_base_shadows
        || base.is_some_and(|b| s.nodes[id as usize].applied_seq >= b)
}

/// A client op returned from a reply carrying `at`: raise the watermark
/// (`Positions`) and record the observation (the session ghost).
pub(crate) fn observe_reply(
    m: &AuthorityModel,
    s: &mut State,
    id: NodeId,
    at: Obs,
    name: Name,
    installed: bool,
) {
    let node = &mut s.nodes[id as usize];
    if m.protocol == Protocol::Positions && !installed {
        node.observed = node.observed.max(at.pos());
    }
    if m.tracks_sessions {
        let seen = &mut node.seen[name as usize];
        *seen = (*seen).max(at);
    }
}

/// `id` applied `seg` (not fenced): an op awaiting its rid in the log is
/// answered, or retried by rid if `seg` proves the accepting tenure over;
/// an awaiting replay retires, or goes back to the queue. Both only exist
/// under `Positions`.
pub(crate) fn on_tailed(s: &mut State, id: NodeId, seg: &Segment) {
    let completes = |rid: Rid| seg.records.iter().any(|(r, _)| *r == Some(rid));
    if let Some(cop) = s.nodes[id as usize].client_op.clone() {
        if let Phase::AwaitingLog { epoch } = cop.phase {
            if completes(cop.rid) {
                s.history.push(HistEvt::Return(id, NsRet::Ok, cop.rid));
                s.nodes[id as usize].client_op = None;
            } else if seg.epoch > epoch {
                // In doubt, as a timed-out forward is: the lease path
                // resolves it by rid (dedup), or re-executes it.
                s.nodes[id as usize]
                    .client_op
                    .as_mut()
                    .expect("cloned above")
                    .phase = Phase::NeedsLease;
            }
        }
    }
    let node = &mut s.nodes[id as usize];
    node.replays
        .retain(|r| !(r.awaiting.is_some() && completes(r.rid)));
    for r in &mut node.replays {
        if r.awaiting.is_some_and(|e| seg.epoch > e) {
            r.awaiting = None;
        }
    }
}

/// `Positions`' session wait for a read of `names` at `id` (always true
/// for the other variants, and with `session_wait` off).
pub fn read_allowed(m: &AuthorityModel, s: &State, id: NodeId, names: DirState) -> bool {
    if m.protocol != Protocol::Positions || !m.session_wait {
        return true;
    }
    let node = &s.nodes[id as usize];
    // The client's own acknowledged write (or another op it is replaying)
    // is rolled back until the replay lands: nothing covers the name.
    if node.replays.iter().any(|r| present(names, r.op.name())) {
        return false;
    }
    if Pos::applied(node.applied_seq) >= node.observed {
        return true;
    }
    (0..N_NAMES).filter(|x| present(names, *x)).all(|x| {
        node.shadows
            .iter()
            .filter(|sh| sh.rec.name() == x)
            .map(|sh| sh.pos)
            .max()
            .is_some_and(|p| p >= node.observed)
    })
}

/// `Action::Read`: the read happens; check both session properties
/// against what it returned (the ghost state only).
pub(crate) fn read(m: &AuthorityModel, s: &mut State, id: NodeId, step: Step) {
    if !m.tracks_sessions {
        return;
    }
    let names = step.names();
    for x in (0..N_NAMES).filter(|x| present(names, *x)) {
        let view = view_mask(s, id, x);
        if own_writes_mask(s, id, x) & !view != 0 {
            s.session_violations |= RYW_VIOLATED;
        }
        if observed_mask(s, s.nodes[id as usize].seen[x as usize], x) & !view != 0 {
            s.session_violations |= MR_VIOLATED;
        }
    }
}

/// A rid as a bit (the model's rid space is tiny: at most 4 nodes, 4
/// incarnations and 4 ops per incarnation).
pub fn rid_bit(rid: Rid) -> u64 {
    assert!(
        (rid.node as usize) < crate::protocol::MAX_NODES && rid.incarnation < 4 && rid.seq < 4,
        "session properties support at most 4 incarnations and 4 mutations per incarnation per \
         node: {rid:?}"
    );
    1 << (rid.node as u32 * 16 + rid.incarnation as u32 * 4 + rid.seq as u32)
}

/// The write set a read of `x` at `id` returns: rids of the records on
/// `x` in its applied (non-fenced) log prefix, its shadows and its
/// unshipped journal.
pub fn view_mask(s: &State, id: NodeId, x: Name) -> u64 {
    let node = &s.nodes[id as usize];
    let (_, _, completions) = log_fold(&s.log, node.applied_seq);
    let logged = completions
        .iter()
        .filter(|(_, rec, _)| rec.name() == x)
        .map(|(rid, _, _)| rid_bit(*rid));
    let shadows = node
        .shadows
        .iter()
        .filter(|sh| sh.rec.name() == x)
        .map(|sh| rid_bit(sh.rid));
    let journal = node
        .journal
        .iter()
        .filter(|e| e.rec.name() == x)
        .filter_map(|e| e.rid.map(rid_bit));
    logged.chain(shadows).chain(journal).fold(0, |m, b| m | b)
}

/// This session's own acknowledged writes on `x`: history returns of
/// `Ok` for `id`'s ops on `x` in its current incarnation.
pub fn own_writes_mask(s: &State, id: NodeId, x: Name) -> u64 {
    let incarnation = s.nodes[id as usize].incarnation;
    let mut mask = 0;
    for evt in &s.history {
        let HistEvt::Return(n, NsRet::Ok, rid) = evt else {
            continue;
        };
        if *n != id || rid.incarnation != incarnation {
            continue;
        }
        let on_x = s.history.iter().any(|e| {
            matches!(e, HistEvt::Invoke(n2, op, r2) if *n2 == id && r2 == rid && op.name() == x)
        });
        if on_x {
            mask |= rid_bit(*rid);
        }
    }
    mask
}

/// The write set on `x` of the holder state `obs` describes, as far as it
/// is still owed to an observer (see the module doc): the holder's applied
/// prefix, plus its first `obs.journal` unshipped rows — found in the
/// segment its tenure shipped at the next slot, or still in its journal
/// while its tenure lasts, and void otherwise.
pub fn observed_mask(s: &State, obs: Obs, x: Name) -> u64 {
    if obs == Obs::ZERO {
        return 0;
    }
    let (_, _, completions) = log_fold(&s.log, obs.applied);
    let mut mask = completions
        .iter()
        .filter(|(_, rec, _)| rec.name() == x)
        .fold(0, |m, (rid, _, _)| m | rid_bit(*rid));
    if obs.journal == 0 {
        return mask;
    }
    let rows = obs.journal as usize;
    let next = s
        .log
        .get(obs.applied as usize + 1)
        .and_then(|slot| slot.as_ref());
    let unshipped: Vec<(Option<Rid>, NsOp)> = match next {
        Some(seg) if seg.epoch == obs.epoch => seg.records.iter().take(rows).copied().collect(),
        // Another tenure's segment took the slot: this one's unshipped
        // rows never shipped under it (void).
        Some(_) => Vec::new(),
        None if s.lease.epoch == obs.epoch => match s.lease.holder {
            Some(h) => s.nodes[h as usize]
                .journal
                .iter()
                .take(rows)
                .map(|e| (e.rid, e.rec))
                .collect(),
            None => Vec::new(),
        },
        // The register moved on with the rows unshipped (void).
        None => Vec::new(),
    };
    for (rid, rec) in unshipped {
        if let (Some(rid), true) = (rid, rec.name() == x) {
            mask |= rid_bit(rid);
        }
    }
    mask
}

pub(crate) fn prop_read_your_writes(_m: &AuthorityModel, s: &State) -> bool {
    s.session_violations & RYW_VIOLATED == 0
}

pub(crate) fn prop_monotonic_reads(_m: &AuthorityModel, s: &State) -> bool {
    s.session_violations & MR_VIOLATED == 0
}
