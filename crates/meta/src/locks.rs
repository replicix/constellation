//! Plan 30 §M14: cross-node `flock`/`fcntl` — the lock tables, in
//! memory, shared by the authority core (through its `Replica` port) and
//! the FUSE threads, like [`crate::readdeleg`].
//!
//! # Two levels
//!
//! **Grants** are the cross-node level: a time-bounded promise from the
//! *owning sequencer* of a file (the lease holder, or the M11 delegate of
//! the subtree) to one node — *no other node holds a conflicting grant
//! on this inode until you have stopped honouring this one*. A grant is
//! shared or exclusive and covers the whole file; the sequencer's table
//! is keyed by `(node, ino)` and never sees ranges or owners. A grant is
//! **cached**: the node keeps it after its last application unlocks, so
//! an uncontended re-lock by the same node costs no message, and the
//! sequencer *recalls* it when another node asks for a conflicting one.
//!
//! **Local locks** are the kernel-facing level: the POSIX (`fcntl`) and
//! `flock` locks of this node's processes, keyed by the kernel's
//! `lock_owner` with byte ranges, resolved on this node under the grant
//! it holds. fuser 0.18 delivers `flock` through `setlk` without the
//! `FUSE_LK_FLOCK` flag, so both kinds share one table and one owner
//! space (a process's `flock` owner is its open file, its `fcntl` owner
//! its file table: distinct ids). Two local owners conflict as POSIX
//! says; across nodes the grant's mode decides.
//!
//! # Time and safety
//!
//! Times are unix milliseconds on one of this node's clocks (plan 43
//! C7-H1, `constellation_types::clock`): a grant this node *holds*
//! ([`HeldGrant`]) is stamped and judged on its **honour** clock (never
//! under-counts: a backward wall step cannot extend it); a fencing token
//! that leaves the node names the window's end on the honour clock and
//! the time left (a duration), beside an absolute **wall** stamp minted from
//! the honour window at that moment (`Now::wall_of_honour`; plan 43 C7b,
//! row 6), and the holder's own wait for a token to run out is a
//! **wait**-clock copy of its honour end taken when the pin is made
//! ([`LockTables::tag_end`]). The sequencer's records ([`Grant`], the
//! restart quarantine, the memory of ended grants) are on its **wait**
//! clock (never over-counts; a wall step moves none of them) and cross a
//! node as a [`GrantWire`] whose end is informational wall. The sequencer
//! records a grant as live until `granted + ttl + margin`; the node
//! honours it until `sent + ttl − margin`, measured from when it *sent*
//! the request or renewal — the read delegations' discipline
//! (`crates/model/src/cto.rs`), and `crates/model/src/locks.rs` checks
//! it for locks: recall, expiry, fencing, failover and delegation moves.
//! Three rules the model found load-bearing live here:
//! - a recall for a grant this node does not hold *yet* is remembered
//!   ([`LockTables::note_pending_recall`]), and the reply that brings the
//!   grant installs it already recalled — answering "released" instead
//!   livelocks two contenders;
//! - a grant is renewed while it is honoured
//!   ([`LockTables::due_renewals`]); a lapsed one is never *reclaimed*
//!   (the owner cannot tell what it granted meanwhile) but, if it pins
//!   something, asked for back (below);
//! - I/O on a file this node holds local locks on is **fenced** once its
//!   grant lapsed ([`LockTables::fenced`]): `EIO` until a lock is taken
//!   again (NFSv4's rule) — unless the grant is *lapsing*, below;
//! - so is the **lock owner** itself, everywhere on this node
//!   ([`LockTables::fenced_owners`]): an application that guards other
//!   files with the lock (git creates, links and renames under one
//!   `flock`) gets `EIO` from every write and namespace op it issues
//!   until its local locks are gone — other processes are not touched;
//! - and what the owner already sent is refused where it lands: every
//!   mutation of a lock owner carries a **fencing token** ([`LockTag`]:
//!   its grants and the end of the window this node honours each for),
//!   and whichever sequencer executes it refuses it once that window is
//!   over on its own clock — unless the grant is live in its own table
//!   (the minter, or the delegate it moved to: exact) — or once it ended
//!   the grant ([`LockTables::check_tag`]); a grant is not released while
//!   a mutation tagged with it is in flight ([`LockTables::release_blocked`]);
//! - dirty data written under a grant that ended *without* its release's
//!   flush (lapsed, lost, or unlocked while fenced) is never published:
//!   the inode is **tainted**, and every point that would publish it
//!   (close, release, `fsync`, a recalled grant's flush, the next lock)
//!   asks [`LockTables::take_discard`] first and throws the data away.
//!   Each such discard is an **error event** on the inode
//!   ([`LockTables::note_discard`]), reported errseq-style (Linux 4.13's
//!   `errseq_t`): every open file description that was open when it
//!   happened sees `EIO` exactly once, at its next `fsync` or close, and
//!   descriptions opened afterwards never do. A description samples
//!   [`LockTables::error_seq`] when it opens and compares at each
//!   publication point (plan 39 §3.7; the view keeps the samples).
//!
//! # Plan 43 C7-F F-c: a lapsed grant that pins something is asked for back
//!
//! A held grant whose window ended while local locks are under it, or a
//! write that may not be published ([`HeldGrant::written`]), is **lapsing**
//! ([`HeldGrant::lapsing`], [`Inner::observe`]), not lapsed: it is not
//! honoured, but nothing is captured or tainted for it. The fences answer
//! [`Fence::Parked`] / [`Discard::Parked`] / [`LocalOutcome::Parked`]
//! (the caller waits for the verdict and asks again; the engine parks the
//! op on its wait pool) and [`LockTables::lapse_tick`] returns it for a
//! **late** renewal at the owner. The owner admits it only where it can
//! prove no conflicting grant was issued since the grant was made
//! ([`LockTables::admit_late`]: a [`Grant::settled`] record, or its memory
//! in [`Inner::lapsed`]; the model, `crates/model/src/locks.rs`, has the
//! argument and the variants that violate). Admitted
//! ([`LockTables::renewed`] makes the grant honoured again) the parked
//! I/O runs; refused ([`LockTables::lost`]) it is fenced as a lapse always
//! was, since the lapse; no verdict within the park's bound
//! ([`LockTables::set_lapse_park_ms`], the core's `lock_owner_wait_max_ms`)
//! the same (`lapse_park_gave_up`). A grant that pins nothing is dropped at
//! once, as before.
//!
//! The owner's side of it lives here too: [`Inner::lapsed`] remembers the
//! settled records [`Inner::drop_expired`] dropped, [`Inner::supersede`]
//! forgets them at every change that could let another node act on the
//! inode (the one place; a test per path), and [`LockTables::final_snapshot`]
//! and [`LockTables::install_lapsed`] carry them in an old root's final
//! mirror.
//!
//! # Cost when unused
//!
//! [`LockTables::fenced`] is on every read and write, and
//! [`LockTables::owner_fence_armed`] on every mutating op; with no local
//! lock anywhere each is one relaxed atomic load (two loads while locks
//! are held under grants that are still honoured).

use crate::session::Position;
use constellation_types::clock::{HonourMs, Now, WaitMs};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    pub fn conflicts(self, other: LockMode) -> bool {
        self == LockMode::Exclusive || other == LockMode::Exclusive
    }

    /// A grant in `self` covers a local lock needing `m`.
    pub fn covers(self, m: LockMode) -> bool {
        self == LockMode::Exclusive || m == LockMode::Shared
    }
}

/// When a holder renews a grant of `ttl_ms` it sent for at `sent_ms`:
/// half-way through the window it honours it for (`ttl − margin`), not at
/// `ttl/2`. A delegate's grants are capped by what is left of its own
/// delegation, so a ttl as short as `margin` plus a second is routine
/// there; renewing at `ttl/2` came after the window had closed for any
/// ttl under `2 × margin`, the grant lapsed while the application still
/// held its lock, and the owner outwaited it and granted the lock to
/// another node (the rounds harness scenario: two `git commit`s under one
/// `flock` at once, git's `index.lock` stall on EC2).
pub fn renew_point(sent: HonourMs, ttl_ms: i64, margin_ms: i64) -> HonourMs {
    HonourMs(sent.0 + (ttl_ms - margin_ms).max(0) / 2)
}

/// A grant's identity: minted by the sequencer that made it, kept
/// across reclaims and delegation moves.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct GrantId {
    pub node: u64,
    pub seq: u64,
}

/// Plan 30 §M14 phase 2, the **fencing token**: one grant a mutation was
/// issued under, as its issuing node (H) honoured it, and what H says of the
/// end of that window (plan 43 C7b, row 6, phase 1).
///
/// - `until_ms`: the end of the window on H's **wall** clock, minted from its
///   honour window at the moment the tag is built (`Now::wall_of_honour`),
///   so a backward wall step of H only shortens it. An executor compares it
///   with its own wall: sound while the offset between the two is below the
///   margin (the phase-1 absolute check, kept).
/// - `until_h` / `clock`: the same end on H's **honour** clock and the id of
///   that clock (`ProtocolClock::id`). They mean nothing on another node;
///   H judges its own tokens exactly with them, and a sequencer's
///   [`Grant::confirmed`] compares them with the renewal that confirmed the
///   grant (both H's readings of one clock).
/// - `remaining_ms`: how much of the window H had left when the op left it
///   (`until_h − honour`, a duration), which the executor adds to **its**
///   receipt of the op on its honour clock (`recv`, below): it honours the
///   token only until `recv + remaining_ms`. Receipt is after the send and
///   honour never over-counts, so the op runs at most the transit after the
///   window H honoured; the minter outwaits H's grant by `2 × margin` past
///   the end of that window, so the op never runs after a conflicting grant
///   if either the offset (the absolute stamp) or the one-way delay of the
///   op is below that — a rate bound and causality, no offset bound, in the
///   second case.
/// - `recv`: the executor's honour reading at the earliest point it has the
///   token (the P2P handler's receive; for a token minted here, the mint),
///   never serialized. `None` on a token that came by the S3 inbox or the
///   persisted replay queue, which have no transit bound: such a token is
///   judged exactly (the grant live and confirmed in this table) or
///   refused.
///
/// The minter records the grant live until `granted + ttl + margin` and
/// grants no conflicting lock before that, which is at least `2 × margin`
/// after the holder's window ended. The minter (or the delegate a grant
/// moved to) judges exactly: a grant live in its table passes whatever the
/// window, one it ended or outwaited is rejected at once. See [`LockTag`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LockToken {
    pub grant: GrantId,
    pub until_ms: i64,
    pub until_h: i64,
    pub clock: u64,
    pub remaining_ms: i64,
    #[serde(skip)]
    pub recv: Option<i64>,
}

impl LockToken {
    /// The token of `grant` for a window H honours until `until` on its
    /// honour clock `clock`, minted at `now`: also received here, now (the
    /// executor of a local op is H itself).
    pub fn minted(grant: GrantId, until: HonourMs, now: Now, clock: u64) -> LockToken {
        LockToken {
            grant,
            until_ms: now.wall_of_honour(until).0,
            until_h: until.0,
            clock,
            remaining_ms: until.since(now.honour).max(0),
            recv: Some(now.honour.0),
        }
    }
}

impl LockToken {
    /// A token whose window ends at `until_ms` on clocks that agree (wall
    /// and honour readings equal, one clock id, received at 0): for tests
    /// and tools that have no node behind them.
    pub fn window(grant: GrantId, until_ms: i64) -> LockToken {
        LockToken {
            grant,
            until_ms,
            until_h: until_ms,
            clock: 1,
            remaining_ms: until_ms,
            recv: Some(0),
        }
    }
}

/// One token on the wire: `(minter, seq, until_ms, until_h, clock,
/// remaining_ms)`.
pub type LockTokenWire = (u64, u64, i64, i64, u64, i64);

/// The grants a mutation was issued under (empty: none — the op of a
/// process that holds no cluster lock, which nothing checks).
///
/// A node tags a mutation when the process issuing it (or one of its
/// ancestors, as the owner fence matches them) holds local locks: one
/// [`LockToken`] per minting sequencer, the earliest window among that
/// minter's grants under the owner's locks. Every executor rejects the
/// op ([`LockTables::check_tag`]) once the token's absolute stamp has
/// passed on its own wall clock (`until_ms`) or its own honour clock has
/// passed its receipt plus the time left the holder named
/// (`recv + remaining_ms`): the minter records the grant live until
/// `granted + ttl + margin` and grants no conflicting lock before that,
/// which is at least `2 × margin` after the holder's window ended — so an
/// op the executor receives within that of the end of the holder's window
/// (the one-way delay of the op), or whose executor's wall clock is within
/// that of the holder's, never runs after a conflicting grant was made.
/// The minter (or the delegate a grant moved to) judges exactly: a
/// grant live in its table passes whatever the window, one it ended or
/// outwaited is rejected at once. A node takes the token when it sends
/// the op, from its current window ([`LockTables::refresh_tag`]). No
/// generation is carried: a `GrantId` is unique across a minter's
/// incarnations — that relies on [`LockTables::seed_ids`] seeding each
/// incarnation's counter above the last one's (the incarnation in the
/// high bits) — and keeps its id across delegation moves, and neither
/// check needs more.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LockTag(pub Vec<LockToken>);

impl LockTag {
    pub const NONE: LockTag = LockTag(Vec::new());

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The tag as it arrives at `at` (this node's honour reading): every
    /// token received now (a transport that does not go through the wire
    /// form's receipt stamp: the simulation's bus).
    pub fn received(&self, at: HonourMs) -> LockTag {
        LockTag(
            self.0
                .iter()
                .map(|t| LockToken {
                    recv: Some(at.0),
                    ..*t
                })
                .collect(),
        )
    }

    /// What the tokens say of the windows, without how they travelled: per
    /// token, the grant and the end of the holder's window on its honour
    /// clock. A token taken again for the same windows (the op is sent again,
    /// with a new receipt and less time left) is the same windows.
    pub fn windows(&self) -> Vec<(GrantId, i64)> {
        self.0.iter().map(|t| (t.grant, t.until_h)).collect()
    }

    /// The earliest window among the tokens (`i64::MAX` with none).
    pub fn until_ms(&self) -> i64 {
        self.0.iter().map(|t| t.until_ms).min().unwrap_or(i64::MAX)
    }

    /// The wire form, as the op leaves this node at honour reading `now`:
    /// a token received here (`recv`) has spent the time since in this
    /// node, which the receiver cannot see, so it is taken off the
    /// remainder. A token with no receipt (it came by the S3 inbox or the
    /// persisted replay queue, which have no transit bound) has no window to
    /// hand on: it leaves with no time left, so the next executor, which
    /// stamps its own receipt, judges it exactly or refuses it as this node
    /// would (plan 43 C7b, row 31: a stranded op replayed and forwarded to
    /// the new holder is not given its old remainder afresh).
    pub fn to_wire(&self, now: HonourMs) -> Vec<LockTokenWire> {
        self.0
            .iter()
            .map(|t| {
                let left = t
                    .recv
                    .map_or(0, |r| (t.remaining_ms - (now.0 - r).max(0)).max(0));
                (
                    t.grant.node,
                    t.grant.seq,
                    t.until_ms,
                    t.until_h,
                    t.clock,
                    left,
                )
            })
            .collect()
    }

    /// The tag as it arrived, received at honour reading `recv` (`None`: it
    /// came by a path with no transit bound, the S3 inbox, and is judged
    /// exactly or refused).
    pub fn from_wire(w: &[LockTokenWire], recv: Option<HonourMs>) -> LockTag {
        LockTag(
            w.iter()
                .map(
                    |&(node, seq, until_ms, until_h, clock, remaining_ms)| LockToken {
                        grant: GrantId { node, seq },
                        until_ms,
                        until_h,
                        clock,
                        remaining_ms,
                        recv: recv.map(|r| r.0),
                    },
                )
                .collect(),
        )
    }
}

/// [`LockTables::owner_tag`]'s refusal: one of the owner's local locks is
/// under no honoured, covering grant — the owner is fenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnerFenced;

/// A grant the sequencer made, as its table keeps it. On the wire (a
/// backup's mirror, a delegation move) it travels as a [`GrantWire`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Grant {
    pub id: GrantId,
    pub node: u64,
    pub ino: u64,
    pub mode: LockMode,
    /// Live until this node's **wait** clock reaches it (`granted + ttl +
    /// margin`; plan 43 C7-H1: the owner outwaits a grant on a clock a
    /// wall step cannot move).
    pub until: WaitMs,
    pub recalled: bool,
    /// The M11 generation it was granted (or installed) under; 0 for the
    /// root's table. A generation's grants leave with it.
    pub gen: u64,
    /// Phase 2: a token naming this grant that this does not confirm is
    /// judged by its window even while the grant is live here
    /// ([`LockTables::check_tag`]). [`Self::MINTED`] for a grant this table
    /// made or its holder renewed from the start; [`Self::UNCONFIRMED`] for
    /// a copy installed without its holder — a backup's mirror, a root's
    /// reinstated handoff copy — which may be a grant its last sequencer
    /// had already ended; the holder's next renewal sets it to that
    /// renewal's send, `(the holder's clock id, its honour reading when it
    /// sent)` ([`LockTables::extend`]), so only an op whose token's window
    /// (`until_h`, a reading of the same clock) ran past it passes whatever
    /// the window: the holder honoured the grant continuously into the
    /// renewal this table accepted, so no gap exists in which it could have
    /// been ended and granted anew. Another clock id (a restarted holder)
    /// replaces it. A move keeps it. (Plan 43 C7b, row 29: both readings
    /// are the holder's own, no offset between nodes is in the compare.)
    pub confirmed: Option<(u64, i64)>,
    /// Plan 43 C7-F F-c: no grant conflicting with this one was issued
    /// anywhere since it was issued, as long as it (or its memory in
    /// [`Inner::lapsed`]) stays in this table — what a late renewal of it
    /// may rest on ([`LockTables::admit_late`]). A mint is settled
    /// ([`LockTables::grant`]: it is made only when no grace, floor or
    /// barrier holds the inode, so every predecessor's grant was issued
    /// before it); a move keeps the wire's value (the sender gave the
    /// grant up as it sent it, the receiver serves it only after
    /// installing it); a reclaim in a grace, a copy installed without its
    /// holder (a reinstated handoff copy, a mirror that is not the old
    /// root's final one) is not: nothing proves what was granted between.
    pub settled: bool,
}

impl Grant {
    /// [`Grant::confirmed`] of a grant confirmed from the start (clock 0:
    /// any holder clock).
    pub const MINTED: Option<(u64, i64)> = Some((0, i64::MIN));
    /// [`Grant::confirmed`] of a copy no holder has renewed here yet.
    pub const UNCONFIRMED: Option<(u64, i64)> = None;

    /// Whether this grant confirms `t`: its window ran past the renewal
    /// that confirmed the grant here, on the holder's own clock.
    pub fn confirms(&self, t: &LockToken) -> bool {
        match self.confirmed {
            None => false,
            Some((0, _)) => true,
            Some((clock, sent)) => t.clock == clock && t.until_h > sent,
        }
    }

    /// A renewal sent by the holder at its honour reading `sent` on clock
    /// `clock` reached this table: confirmed from that send on (the
    /// earliest renewal of one clock stands; another clock's replaces). A
    /// clock id of 0 is no clock (an unset certificate or stamp): it
    /// confirms nothing, since `(0, _)` is [`Self::MINTED`]'s "any clock".
    pub fn confirm(&mut self, clock: u64, sent: i64) {
        if clock == 0 {
            return;
        }
        self.confirmed = match self.confirmed {
            Some((0, _)) => self.confirmed,
            Some((c, s)) if c == clock => Some((c, s.min(sent))),
            _ => Some((clock, sent)),
        };
    }

    /// This grant as it crosses a node: `until_ms` is the end on the
    /// sender's *wall* clock, mapped from the wait-clock record now
    /// (`Now::wall_of_wait`; Appendix B row 9, offset-bound). A receiver
    /// never reads it: it records its own end ([`GrantWire::into_grant`]).
    pub fn to_wire(&self, now: Now) -> GrantWire {
        GrantWire {
            id: self.id,
            node: self.node,
            ino: self.ino,
            mode: self.mode,
            until_ms: now.wall_of_wait(self.until).0,
            recalled: self.recalled,
            gen: self.gen,
            confirmed: self.confirmed,
            settled: self.settled,
            lapsed: false,
        }
    }
}

/// A [`Grant`] on the wire. The end is the sender's wall clock and is
/// informational: another node's wait-clock reading would mean nothing
/// here, so the receiver supplies its own end
/// ([`GrantWire::into_grant`]), from its receipt or from a carried
/// remainder (`Core::lock_install_moved`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantWire {
    pub id: GrantId,
    pub node: u64,
    pub ino: u64,
    pub mode: LockMode,
    /// The end on the sender's wall clock when it sent (see [`Grant::to_wire`]).
    pub until_ms: i64,
    pub recalled: bool,
    pub gen: u64,
    pub confirmed: Option<(u64, i64)>,
    /// Plan 43 C7-F F-c, appended: [`Grant::settled`].
    pub settled: bool,
    /// Plan 43 C7-F F-c, appended: the record had expired on its owner's
    /// wait clock when it was sent (a final mirror's expired records and
    /// its memory of lapsed ones: [`LockTables::final_snapshot`]); the
    /// receiver keeps it as a memory, not a live record.
    pub lapsed: bool,
}

impl GrantWire {
    /// The grant as the receiver records it, ending at `until` on the
    /// receiver's wait clock.
    pub fn into_grant(self, until: WaitMs) -> Grant {
        Grant {
            id: self.id,
            node: self.node,
            ino: self.ino,
            mode: self.mode,
            until,
            recalled: self.recalled,
            gen: self.gen,
            confirmed: self.confirmed,
            settled: self.settled,
        }
    }
}

/// What a delegate hands back to the root with its recall answer: the
/// subtree's grants and the *floor* its holders released at (every
/// position a later grant under the subtree must carry — see
/// `core::locks`'s `LockState::floors`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockHandback {
    pub grants: Vec<GrantWire>,
    /// What was left, in ms on the delegate's clock when it built this, of
    /// the record of each grant of `grants`: the root records it from the
    /// receipt, plus the margin, never later than a restamp
    /// (`Core::lock_install_moved`). A grant not listed is restamped.
    /// Durations only: no clock's absolute crosses nodes.
    pub carry: Vec<(GrantId, u64)>,
    pub floor: Position,
    /// The latest outwait barrier under the subtree that no cut has
    /// settled yet (the delegate's clock; 0: none): the root's grants
    /// there wait for a cut as of it, as the delegate's would have.
    pub barrier: i64,
}

/// Join two lock floors (positions: watermarks, so the join is what both
/// cover). Unlike [`Position::join`], a streams overflow does not drop
/// the other side's streams wholesale: the newest generations are kept
/// (an older generation's stream has long reached the log, which `seq`
/// covers).
pub fn floor_join(a: &Position, b: &Position) -> Position {
    let mut all: BTreeMap<u64, u64> = a.streams_wire().into_iter().collect();
    for (g, i) in b.streams_wire() {
        let e = all.entry(g).or_insert(0);
        *e = (*e).max(i);
    }
    let mut kept: Vec<(u64, u64)> = all.into_iter().rev().collect();
    kept.truncate(crate::session::STREAMS_CAP);
    Position {
        seq: a.seq.max(b.seq),
        pending: a.pending.max(b.pending),
        streams: Default::default(),
    }
    .with_streams_wire(&kept)
}

/// What installing a grant on the node side did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Installed {
    /// Installed (`recalled`: a recall for it had arrived already).
    Ok { recalled: bool },
    /// This node released that id already (the reply crossed its own
    /// release): not installed; ask again.
    Released,
}

/// What [`LockTables::admit_late`] answers when it admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LateAdmitted {
    /// The grant admitted: the one asked about, or the one that replaced it
    /// while it was live.
    pub id: GrantId,
    pub mode: LockMode,
    pub recalled: bool,
    /// The record had been dropped: it was reinstated from the memory.
    pub from_memory: bool,
}

/// A grant this node holds (the cache).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldGrant {
    pub id: GrantId,
    pub mode: LockMode,
    /// Honoured while this node's honour clock is below this.
    pub until: HonourMs,
    /// Renew from here (`sent + ttl/2`), on the same clock.
    pub renew_at: HonourMs,
    /// Who granted it (renewals go to the current owner, not here).
    pub owner: u64,
    pub recalled: bool,
    /// The position the replica must reach before reading under it.
    pub position: Position,
    /// A renewal is in flight (sent at this time, honour clock).
    pub renewing: Option<HonourMs>,
    /// A release (flush, then `LockReleased`) is in flight.
    pub releasing: bool,
    /// Arrived already recalled (the recall overtook the reply): the
    /// request it was granted for still gets its one local lock — the
    /// release follows that lock's unlock. Otherwise the two contenders
    /// would trade the grant without either application ever holding it.
    pub first_use: bool,
    /// Since when no local lock has been under it (`None`: one is, or it
    /// was never used): an idle cache is released after a while so its
    /// renewals stop.
    pub idle_since: Option<WaitMs>,
    /// When this node installed it (its honour clock): a recalled grant
    /// waiting for its first local lock is renewed only for
    /// [`LockTables::first_use_budget_ms`] from here.
    pub installed: HonourMs,
    /// Plan 43 C7-F F-c: the window ended while the grant pins something
    /// (local locks under it, or writes that may not be published): the
    /// grant is *lapsing*, not lapsed. It is not honoured, but nothing is
    /// captured or tainted for it: the I/O that would be fenced waits
    /// ([`Fence::Parked`]) while the owner is asked to admit a late
    /// renewal. See [`Lapsing`].
    pub lapsing: Option<Lapsing>,
    /// Plan 43 C7-F F-c: a write-like op ran on the inode under this grant
    /// (a hint that unpublished data may be pinned by it).
    pub written: bool,
    /// Plan 41 L1: the node that granted this grant or answered the last
    /// renewal of it ([`LockTables::renewed`]) — where the next one goes
    /// without the core routing it again (the lock lane's, at a remote
    /// owner). Cleared when that node answered `NotOwner`
    /// ([`LockTables::clear_route`]); `None`: not known, the core routes.
    pub route: Option<u64>,
}

/// A held grant whose window ended while it pins something (see
/// [`HeldGrant::lapsing`]): it resolves by a renewal that makes it honoured
/// again (resumed), by the owner refusing it (lost) or by `deadline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Lapsing {
    /// The end of the window it was honoured for (the lapse), honour
    /// clock: what a capture is stamped with if the park ends refused.
    pub since: HonourMs,
    /// When this node first noticed the lapse (wait clock): the park's
    /// length is measured from here.
    pub at: WaitMs,
    /// Past this (wait clock, a liveness bound) the park gives up and the
    /// lapse is handled as before F-c.
    pub deadline: WaitMs,
}

/// What [`Inner::observe`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Observed {
    /// A grant honoured now.
    Honoured,
    /// A lapsing one.
    Lapsing,
    /// None, or lapsed and not parked (the rule before F-c).
    Gone,
}

/// What [`LockTables::lapse_tick`] found.
#[derive(Debug, Default)]
pub struct LapseTick {
    /// Lapsing grants to ask the owner to admit late renewals of.
    pub late: Vec<(u64, HeldGrant)>,
    /// Lapsing grants that gave up at their deadline.
    pub gave_up: usize,
}

/// What a fence check answers (plan 43 C7-F F-c).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fence {
    /// Not fenced.
    Ok,
    /// Fenced by a grant that is lapsing: wait, then ask again.
    Parked,
    /// Fenced (`EIO`).
    Fenced,
}

/// What [`LockTables::take_discard`] answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discard {
    /// Publish (nothing to discard).
    Publish,
    /// Discard the dirty data; `fenced`: local locks are still under the
    /// lapsed grant, so the caller answers `EIO` even with nothing dirty.
    Discard { fenced: bool },
    /// The inode's grant is lapsing: wait for its resolution (plan 43
    /// C7-F F-c), then ask again.
    Parked,
}

/// A kernel-facing lock of one local process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalLock {
    pub owner: u64,
    /// The process that took it (its thread group id; 0: unknown).
    pub pid: u32,
    /// What tells that process from a later one given its pid (0:
    /// unknown; the platform's `Lineage::start`, opaque): the pid names it
    /// only with this, across pid reuse.
    pub pid_start: u64,
    pub write: bool,
    pub start: u64,
    /// Inclusive; `u64::MAX` for "to the end".
    pub end: u64,
}

impl LocalLock {
    fn overlaps(&self, start: u64, end: u64) -> bool {
        self.start <= end && start <= self.end
    }

    pub fn mode(&self) -> LockMode {
        if self.write {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        }
    }
}

/// Plan 43 C7b: the path a tagged op reached its executor's token check by
/// (a label for [`LockStats::tokens_judged_window`]'s counters only).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TokenPath {
    /// A forwarded op executed by the sequencer that received it over P2P.
    #[default]
    P2p,
    /// An op submitted on this node and executed here (it is its own
    /// executor).
    Local,
    /// An op drained from the inbox (an S3 batch).
    Inbox,
    /// A stranded op replayed from the persisted queue.
    Replay,
}

impl TokenPath {
    pub const COUNT: usize = 4;

    /// The slot of this path in the per-path counters.
    pub fn index(self) -> usize {
        match self {
            TokenPath::P2p => 0,
            TokenPath::Local => 1,
            TokenPath::Inbox => 2,
            TokenPath::Replay => 3,
        }
    }

    /// The names of the per-path counters, in slot order.
    pub const NAMES: [&'static str; 4] = ["p2p", "local", "inbox", "replay"];

    /// Run `f` with this path as the label of every token judged on this
    /// thread inside it. A funnel that does not label itself is `P2p`.
    pub fn scope<R>(self, f: impl FnOnce() -> R) -> R {
        let prev = TOKEN_PATH.with(|p| p.replace(self));
        let r = f();
        TOKEN_PATH.with(|p| p.set(prev));
        r
    }
}

thread_local! {
    static TOKEN_PATH: std::cell::Cell<TokenPath> = const { std::cell::Cell::new(TokenPath::P2p) };
}

/// Counters for `status.locks`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockStats {
    /// Local lock requests, and how they were answered.
    pub requests: u64,
    pub local_hits: u64,
    pub local_conflicts: u64,
    /// Phase 2: a new local turn waited for an earlier turn's close
    /// publication or mutation in doubt under the same grant
    /// ([`LocalOutcome::Predecessor`]): once
    /// per lock request (the engine's `ClusterLocks`), however often it
    /// looked again, whether it then took the lock or, non-blocking, was
    /// refused.
    pub predecessor_waits: u64,
    pub granted: u64,
    pub would_block: u64,
    pub unavailable: u64,
    /// Sum of round-trip times of grants that needed the sequencer (ms).
    pub grant_ms_total: u64,
    pub grant_ms: [u64; 14],
    pub renewals: u64,
    pub lost: u64,
    /// Recalls this node received, and how many found local holders.
    pub recalled: u64,
    pub recalled_busy: u64,
    pub released: u64,
    /// I/O refused because the grant lapsed.
    pub fenced_io: u64,
    /// Lock owners fenced on this node (their grant ended under their
    /// local lock), and the ops of theirs refused for it, on any file.
    pub owners_fenced: u64,
    pub owner_fenced_ops: u64,
    /// Recalled grants given up before their first local lock: the
    /// requester gave up, or the first-use budget ran out.
    pub first_use_abandoned: u64,
    /// Plan 30 §M14 phase 2: mutations this node issued tagged with the
    /// grants their owner held (the fencing token), and releases held
    /// back until such an op had been answered.
    pub tagged_ops: u64,
    pub release_waits: u64,
    /// Recalls that named a newer id of the same owner than the held
    /// one (adopted; see `recall_held`).
    pub recalled_superseded: u64,
    /// The grant's read wait (`locks::granted`): grants whose floor the
    /// replica had not reached when they arrived (the first read under
    /// the lock waited), the milliseconds those waits took, and the ones
    /// that gave up after the session budget — the read under the lock
    /// was then answered degraded, and the guarantee that the next
    /// holder reads what the previous one wrote did not hold for it
    /// (EC2 campaign 8 B-1: the counter that says whether it ever
    /// happened).
    pub grants_waited: u64,
    pub grant_wait_ms_total: u64,
    pub grants_degraded: u64,
    /// Plan 43 F-b: non-blocking requests that waited for an owner that
    /// could not grant yet (a grace, the tenure's floor, a barrier), the
    /// milliseconds those waits took in all, and the ones that gave up
    /// with `EAGAIN` after `lock_owner_wait_max_ms` (a campaign expects
    /// none).
    pub grace_waits: u64,
    pub grace_wait_ms_total: u64,
    pub grace_wait_gave_up: u64,
    /// Waits refused when they would have begun, answered `EAGAIN` as
    /// before F-b (a campaign expects none): the frontend could not hold
    /// one more waiting request (a ring queue's lock-wait budget was
    /// spent), or no `lock-wait` thread could be started.
    pub grace_wait_refused_ring: u64,
    pub grace_wait_refused_pool: u64,
    // ---- sequencer side ----
    pub grants_made: u64,
    pub recalls_sent: u64,
    pub recalls_released: u64,
    pub recalls_expired: u64,
    pub reclaimed: u64,
    pub waiters_parked: u64,
    pub grace_refusals: u64,
    /// Plan 30 §M14 phase 2: tagged mutations this executor rejected
    /// because a grant they named was no longer live (`EIO`, nothing
    /// journaled).
    pub token_rejections: u64,
    /// F1: of those, the ones only the check under the store's writer
    /// lock refused — the op had passed the check before it waited for
    /// the lock (a slow writer ahead of it, a stalled step).
    pub token_rejections_at_write: u64,
    /// Plan 43 C7b (evidence, no decision reads them): the tokens this
    /// executor judged, by the path the op took ([`TokenPath::index`]: P2P
    /// forward, local submit, inbox drain, replay queue) and by how: exactly
    /// (the grant's record is live here and the token passes its
    /// confirmation) or by the token's window alone (the grant is not here,
    /// or the record is not yet confirmed for the token). The window is the
    /// phase-1 check that needs an offset or a delay bound.
    pub tokens_judged_exact: [u64; 4],
    pub tokens_judged_window: [u64; 4],
    /// F1: positions this node reported as covering what it executed (a
    /// grant's floor, an outwait's cut, a delegate's stream head) that
    /// first waited for a tagged write still in its transaction under a
    /// grant no longer live here.
    pub settle_waits: u64,
    /// Node side: recalled grants left to their owner's outwait instead
    /// of released, because a mutation tagged with them had ended in
    /// doubt (it may still be committing at an executor).
    pub doubt_lapses: u64,
    // ---- plan 43 C7-F F-c ----
    /// Node side: grants whose window ended while they pinned something
    /// (they park the fenced I/O instead of fencing it), and how those
    /// ended: renewed late (resumed), refused by the owner (`Lost`),
    /// or past `lock_owner_wait_max_ms` with no answer (gave up). The
    /// longest park (ms, wait clock), and the ops that waited for one.
    pub lapse_parks: u64,
    pub lapse_parks_resumed: u64,
    pub lapse_parks_refused: u64,
    pub lapse_park_gave_up: u64,
    pub lapse_park_ms_max: u64,
    pub lapse_parked_ops: u64,
    /// A parked op the frontend could not hold (the ring's lock-wait budget
    /// was spent) or no `lock-wait` thread could be started for: answered
    /// `EIO` at once, as before F-c. A campaign expects none.
    pub lapse_park_refused_ring: u64,
    pub lapse_park_refused_pool: u64,
    /// Parked ops ended by their caller's signal (`EINTR`).
    pub lapse_park_interrupted: u64,
    /// Lock-wait follow-ups (W1, W2): ops the park gate did not hold (a
    /// lapse its estimate missed, or one that began while the op queued)
    /// that found a lapsing grant at a fence, and the ones among them handed
    /// to the wait pool then (`lapse_late_parks`: reads, which run on a
    /// ring's thread); the ones that waited on their own thread
    /// (`lapse_inline_waits`, and the time they took); those ended by their
    /// caller's signal (`EINTR`); and those refused the wait because the
    /// thread is a ring's (`EIO`, as before F-c). A campaign expects the
    /// last to stay 0.
    pub lapse_late_parks: u64,
    pub lapse_inline_waits: u64,
    pub lapse_inline_wait_ms_total: u64,
    pub lapse_inline_interrupted: u64,
    pub lapse_inline_refused_thread: u64,
    /// Sequencer side: late renewals admitted (the grant's continuity
    /// proven: a settled record, or its memory) and refused (`Lost`).
    pub late_admitted: u64,
    pub late_refused: u64,
}

/// `BTreeMap`s throughout: the renewal tick and the idle sweep iterate
/// these, and the simulation replays a seed only if that order is the
/// same in every process (a `HashMap` made seed 196252 fail one run in
/// five).
#[derive(Default)]
struct Inner {
    /// Sequencer side.
    grants: BTreeMap<GrantId, Grant>,
    /// Node side, by inode.
    held: BTreeMap<u64, HeldGrant>,
    local: BTreeMap<u64, Vec<LocalLock>>,
    pending_recalls: BTreeMap<u64, Vec<GrantId>>,
    /// Ids this node released or dropped (the last few hundred): a
    /// reply that crosses the release is refused.
    released: std::collections::VecDeque<GrantId>,
    /// Node side: per `(inode, minting owner)`, the newest grant sequence
    /// this node installed. An owner keeps one grant per node and inode,
    /// and every grant is a fresh id, so an older id from the same owner
    /// arriving later is one the owner already replaced (two waiters of
    /// this node on one inode answered in one pass: the second grant
    /// replaced the first in the owner's table, and the first one's push
    /// landed after the second was released — sim `locks-released-writes`
    /// seed 211029, two exclusive holders). Refused like a released id.
    newest: BTreeMap<(u64, u64), u64>,
    /// Node side: per inode, the grant this node released last, while it
    /// has held none there since (see [`LockTables::released_last`]).
    released_last: BTreeMap<u64, GrantId>,
    /// Node side: inodes whose dirty data may have been written under a
    /// grant that ended without the release's flush (see
    /// [`LockTables::take_discard`]).
    taint: std::collections::BTreeSet<u64>,
    /// Node side: per inode, the sequence number of its latest discard
    /// error event ([`LockTables::note_discard`]); absent: none since the
    /// last [`LockTables::forget_errors`].
    errors: BTreeMap<u64, u64>,
    /// Node side: lock owners whose grant ended under their local lock,
    /// by kernel lock owner (see [`LockTables::fenced_owners`]).
    fenced_owners: BTreeMap<u64, FencedOwner>,
    /// Sequencer side (phase 2): grants this table ended by a release or
    /// an outwait (an expired record dropped), kept until `TTL + 2 ×
    /// margin` past the end of their record (`ended_keep_ms`) — a token
    /// naming one is rejected at once ([`LockTables::check_tag`]). Moves
    /// to and from a delegate are not ends.
    ended: BTreeMap<GrantId, WaitMs>,
    /// Sequencer side: the expired records [`Inner::drop_expired`]
    /// dropped (outwaited: their holders never released them) since the
    /// sequencer last took them ([`LockTables::take_outwaited`]), at most
    /// [`ENDED_KEPT`] (the oldest go first).
    outwaited: Vec<Grant>,
    /// Sequencer side, plan 43 C7-F F-c: the settled records
    /// [`Inner::drop_expired`] dropped (an outwait, not a release), by
    /// inode — the memory a late renewal of one is admitted from
    /// ([`LockTables::admit_late`]). Removed (superseded) by every change
    /// that could let another node act on the inode ([`Inner::supersede`]),
    /// by the inode leaving this table and by its release. At most
    /// [`ENDED_KEPT`] in all, the oldest evicted first (an evicted entry
    /// only refuses what it would have admitted).
    lapsed: BTreeMap<u64, Vec<Grant>>,
    lapsed_order: std::collections::VecDeque<GrantId>,
    /// Sequencer side, plan 43 C7-F F-c: the grants this table replaced by a
    /// newer one of the same node *while the old one was live* (a
    /// re-affirmation: [`LockTables::note_continues`]), old id → new id. The
    /// record never lapsed, so nothing conflicting was granted between them:
    /// a late renewal of the old id is a renewal of the new one (the holder
    /// that adopted the new id by a recall may still ask under the old).
    /// Bounded like `lapsed`; an entry lost only refuses.
    continues: BTreeMap<GrantId, GrantId>,
    continues_order: std::collections::VecDeque<GrantId>,
    /// Node side (phase 2): per grant, the tagged ops in flight and how
    /// long one left in doubt may still execute somewhere — the grant is
    /// not released before both are over ([`LockTables::release_blocked`]).
    inflight: BTreeMap<GrantId, Inflight>,
    /// Node side, per inode: the held grant's current id and the older
    /// ids it went on from under its owner's newer ones (a re-affirmation
    /// installed over it, a recall that adopted the newer id, a renewal
    /// answered under it), newest first. The ops tagged with an old id
    /// are in flight, pinned or in doubt under that id's entries
    /// (`inflight`, `doubted`), and the grant they hold back is the
    /// current one: the release, the doubt lapse and the next turn's
    /// predecessor wait look at every id of its [`Inner::lineage`]. An
    /// entry is valid while its inode's held grant has the current id; it
    /// goes once that grant ends (pruned at the next renaming), never
    /// while it is held, so at most one per held grant and none evicted.
    lineages: BTreeMap<u64, (GrantId, Vec<GrantId>)>,
    /// Sequencer side (phase 2): how long past the end of its record an
    /// ended grant stays in `ended` ([`LockTables::set_token_memory_ms`]:
    /// `TTL + 2 × margin`, so an outwaited grant — whose record has
    /// already expired when it ends — is remembered too).
    ended_keep_ms: i64,
    /// Node side (phase 2): the grant last held on each inode, for a
    /// fence captured after the lapsed grant left `held`
    /// ([`FencedOwner::grant`]).
    last_held: BTreeMap<u64, GrantId>,
    /// Node side (phase 2): told when a grant's last tagged mutation in
    /// flight ended, so a release that waits for it goes on at once
    /// ([`LockTables::set_release_wake`]).
    release_wake: Option<ReleaseWake>,
    /// Executor side (F1, the token at the write): per grant, the tagged
    /// writes that passed [`LockTables::check_tag_at_write`] under the
    /// store's writer lock and have not finished their transaction yet
    /// ([`TagPins`]). A position this node reports as covering what was
    /// executed here ([`crate::Meta::settle_tagged_writes`]) waits for
    /// those whose grant ended here or whose token's window is over: how
    /// many are in, and the latest window among their tokens, as the
    /// token's absolute stamp (wall) and as the receipt plus the time left
    /// (this node's honour clock; `i64::MIN` for a token with no receipt,
    /// which is judged exactly and has no window: over as soon as its grant
    /// is not live here).
    pins: BTreeMap<GrantId, (u32, i64, i64)>,
    /// Node side: grants under which a tagged mutation of this node ended
    /// in doubt, and which ones (by rid, where known): such an op may have
    /// passed its check at an executor and still be committing there, so
    /// the grant is not released while one is unresolved, only left to
    /// its owner's outwait ([`LockTables::doubt_of`]). At most
    /// [`ENDED_KEPT`], the oldest evicted first.
    doubted: BTreeMap<GrantId, Doubt>,
    /// The executor's clock for [`LockTables::check_tag_at_write`] (unix
    /// ms); `None`: the system clock.
    clock: Option<LockClock>,
    stats: LockStats,
}

/// The clock a table reads for the write-time token check
/// ([`LockTables::set_clock`]): the daemon's protocol clock, or a
/// simulation's node clock (all three readings of one instant).
pub type LockClock = std::sync::Arc<dyn Fn() -> Now + Send + Sync>;

/// See [`LockTables::set_release_wake`]: called with the inode whose
/// held grant has no tagged mutation in flight any more.
pub type ReleaseWake = std::sync::Arc<dyn Fn(u64) + Send + Sync>;

/// See `Inner::inflight`.
#[derive(Clone, Copy, Debug, Default)]
struct Inflight {
    /// Tagged mutations in flight: the release waits for every one.
    active: u32,
    /// The publications of the grant's own file by a close (`FLUSH`,
    /// `RELEASE`) or by the release's flush ([`LockTables::publish_begin`]):
    /// what the earlier turn wrote, which its application already counts
    /// as done. A new turn on the node waits for these (and for pins)
    /// only (`LocalOutcome::Predecessor`); the release waits for them too.
    /// Not always among `active`: a close is tagged per minter with the
    /// grant whose window ends first, which may be another file's.
    publishing: u32,
    /// An op left in doubt may still execute somewhere until its token's
    /// window is over: on this node's **wait** clock, taken from the
    /// token's wall end when the pin is made ([`Inner::pin`]). The holder
    /// waits until its token's executors would refuse it, never less: a
    /// forward wall step must not end the wait early (plan 43 C7-H1, R8;
    /// the wall-to-wait crossing is offset-bound, Appendix B row 6).
    pinned_until: WaitMs,
}

impl Inflight {
    /// Nothing left to wait for at `now`: the entry can go.
    fn over(&self, now: Now) -> bool {
        self.active == 0 && self.publishing == 0 && self.pinned_until <= now.wait
    }
}

/// A lock owner fenced on this node: its grant lapsed (or was lost)
/// while it held a local lock under it, so another node may hold the
/// lock now. Every op it issues on this node, on any file, is refused
/// with `EIO` until its local locks are gone (unlock or close).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FencedOwner {
    /// The kernel's lock owner (`flock`: the open file; `fcntl`: the
    /// process's file table).
    pub owner: u64,
    /// The process that took the lock (0: unknown) and its identity
    /// across pid reuse (0: unknown; see [`LocalLock::pid_start`]).
    pub pid: u32,
    pub pid_start: u64,
    /// Since when (the end of the window it was honoured for, or when it
    /// was captured), on this node's honour clock.
    pub since: HonourMs,
    /// The grant whose end fenced it (the one its lock was under), when
    /// known: the fencing token named it on every op the owner issued
    /// under that lock.
    pub grant: Option<GrantId>,
}

const RELEASED_KEPT: usize = 512;

/// Past this many ended grants the oldest go (an end forgotten early only
/// leaves the token's window check, which a release cannot beat: the
/// releasing node waits for its tagged ops first).
const ENDED_KEPT: usize = 4096;
/// Past this many `(inode, owner)` pairs the oldest inodes' go (a lost
/// entry only admits what it would have refused before).
const NEWEST_KEPT: usize = 4096;

/// How long before a held grant's end the park gates start to look (see
/// [`LockTables::may_park`]).
pub const PARK_LOOKAHEAD_MS: i64 = 50;

/// How long before a held grant's end the frontend starts wrapping ops'
/// responders so that they can park ([`LockTables::may_park_est_ahead`]):
/// wider than [`PARK_LOOKAHEAD_MS`], so that an op the gate parks was
/// wrapped (registered for its caller's signal, counted for a detach). Out
/// of it an op is not wrapped at all: the wrap's two shared reference
/// counts on every op of a node holding a lock were the write path's cost
/// (review of F-c).
pub const WRAP_LOOKAHEAD_MS: i64 = 1_000;

impl Inner {
    /// Node side: the grant held on `ino` as `old` goes on as `new`
    /// (`lineages`). Called once `held` names `new`. Entries of grants no
    /// longer held go here, so the map holds the held grants' lineages
    /// alone.
    fn note_superseded(&mut self, ino: u64, old: GrantId, new: GrantId) {
        if old == new {
            return;
        }
        let olds = match self.lineages.remove(&ino) {
            Some((cur, mut olds)) if cur == old => {
                olds.insert(0, old);
                olds
            }
            _ => vec![old],
        };
        self.lineages.insert(ino, (new, olds));
        let held = &self.held;
        self.lineages
            .retain(|ino, (cur, _)| held.get(ino).is_some_and(|h| h.id == *cur));
    }

    /// The valid lineage entry of the held grant whose current id is `id`.
    fn lineage_entry(&self, id: GrantId) -> Option<(&u64, &Vec<GrantId>)> {
        self.lineages
            .iter()
            .find(|(ino, (cur, _))| *cur == id && self.held.get(ino).is_some_and(|h| h.id == id))
            .map(|(ino, (_, olds))| (ino, olds))
    }

    /// Node side: `id` and, when it is a held grant's current id, every
    /// older id of that grant (`lineages`), newest first.
    fn lineage(&self, id: GrantId) -> Vec<GrantId> {
        let mut ids = vec![id];
        if let Some((_, olds)) = self.lineage_entry(id) {
            ids.extend(olds.iter().copied());
        }
        ids
    }

    /// The current id of the held grant `id` is an id of (itself if none).
    fn current_id(&self, id: GrantId) -> GrantId {
        self.lineages
            .iter()
            .find(|(ino, (cur, olds))| {
                olds.contains(&id) && self.held.get(ino).is_some_and(|h| h.id == *cur)
            })
            .map_or(id, |(_, (cur, _))| *cur)
    }

    /// Node side: the in-flight state of `id`'s whole lineage, merged.
    fn inflight_of(&self, id: GrantId) -> Option<Inflight> {
        let mut out: Option<Inflight> = None;
        for g in self.lineage(id) {
            if let Some(e) = self.inflight.get(&g) {
                let o = out.get_or_insert_with(Inflight::default);
                o.active += e.active;
                o.publishing += e.publishing;
                o.pinned_until = o.pinned_until.max(e.pinned_until);
            }
        }
        out
    }

    fn tombstone(&mut self, id: GrantId) {
        if self.released.len() >= RELEASED_KEPT {
            self.released.pop_front();
        }
        self.released.push_back(id);
    }

    /// `ino`'s grant ended without the release's flush: whatever is
    /// dirty on it must not be published.
    fn taint(&mut self, ino: u64) {
        self.taint.insert(ino);
    }

    /// `ino`'s local locks are under no honoured grant: fence their
    /// owners everywhere (kept until each owner's locks are gone, even if
    /// a new grant arrives meanwhile — the lock was not held throughout).
    ///
    /// Only the owners whose own lock is uncovered: a shared lock under
    /// an honoured shared grant is still held when another owner's
    /// exclusive lock on the same file lost its cover (the exclusive
    /// grant lapsed and a shared one came for another request).
    fn capture(&mut self, ino: u64, now: HonourMs) {
        let Some(v) = self.local.get(&ino) else {
            return;
        };
        let cover = self
            .held
            .get(&ino)
            .filter(|h| h.until > now)
            .map(|h| h.mode);
        // Fenced since the grant's window ended (from then on every op of
        // the owner is refused, whenever this capture runs), or now when
        // there is no grant to tell.
        let since = self
            .held
            .get(&ino)
            .map(|h| h.until)
            .filter(|until| *until <= now)
            .unwrap_or(now);
        let grant = self
            .held
            .get(&ino)
            .map(|h| h.id)
            .or_else(|| self.last_held.get(&ino).copied());
        for l in v {
            if cover.is_some_and(|m| m.covers(l.mode())) {
                continue;
            }
            if let std::collections::btree_map::Entry::Vacant(e) = self.fenced_owners.entry(l.owner)
            {
                e.insert(FencedOwner {
                    owner: l.owner,
                    pid: l.pid,
                    pid_start: l.pid_start,
                    since,
                    grant,
                });
                self.stats.owners_fenced += 1;
            }
        }
    }

    /// Capture the owners of every inode that is fenced at `now` — except
    /// those under a lapsing grant (plan 43 C7-F F-c): they wait.
    fn capture_lapsed(&mut self, now: Now, park_ms: i64) {
        let fenced: Vec<u64> = self
            .local
            .keys()
            .copied()
            .filter(|ino| self.fenced_at(*ino, now.honour))
            .collect();
        for ino in fenced {
            if self.observe(ino, now, park_ms) == Observed::Lapsing {
                continue;
            }
            self.capture(ino, now.honour);
        }
    }

    /// Expired grant records leave the table, remembered as ended (an
    /// outwait: the next holder may be granted from now on). Whether any
    /// was dropped.
    fn drop_expired(&mut self, now: WaitMs) -> bool {
        if !self.grants.values().any(|e| e.until <= now) {
            return false;
        }
        let expired: Vec<Grant> = self
            .grants
            .values()
            .filter(|e| e.until <= now)
            .copied()
            .collect();
        for e in expired {
            self.grants.remove(&e.id);
            self.note_ended(e.id, e.until);
            if e.settled {
                self.remember_lapsed(e);
            }
            self.outwaited.push(e);
        }
        if self.outwaited.len() > ENDED_KEPT {
            let over = self.outwaited.len() - ENDED_KEPT;
            self.outwaited.drain(..over);
        }
        true
    }

    /// Remember a settled record that expired here (see [`Inner::lapsed`]).
    fn remember_lapsed(&mut self, e: Grant) {
        self.forget_lapsed(e.id);
        self.lapsed.entry(e.ino).or_default().push(e);
        self.lapsed_order.push_back(e.id);
        while self.lapsed_order.len() > ENDED_KEPT {
            if let Some(old) = self.lapsed_order.pop_front() {
                self.remove_lapsed(old);
            }
        }
    }

    fn remove_lapsed(&mut self, id: GrantId) -> Option<Grant> {
        let mut found = None;
        let mut empty = None;
        for (ino, v) in self.lapsed.iter_mut() {
            if let Some(i) = v.iter().position(|g| g.id == id) {
                found = Some(v.remove(i));
                if v.is_empty() {
                    empty = Some(*ino);
                }
                break;
            }
        }
        if let Some(ino) = empty {
            self.lapsed.remove(&ino);
        }
        found
    }

    /// `id`'s memory goes (it was released, admitted late, superseded).
    fn forget_lapsed(&mut self, id: GrantId) -> Option<Grant> {
        let g = self.remove_lapsed(id)?;
        if let Some(i) = self.lapsed_order.iter().position(|x| *x == id) {
            self.lapsed_order.remove(i);
        }
        Some(g)
    }

    /// A record of `node` in `mode` enters (or is strengthened in) this
    /// table on `ino`, any way at all: the lapsed memories it could let
    /// another node act against (a conflicting mode), and any of the same
    /// node's (it was granted afresh), are superseded. Every path that adds
    /// or strengthens a record calls this, so none forgets: the argument
    /// that a memory still here means nothing conflicting was granted
    /// since rests on it (`crates/model/src/locks.rs`, `lapsed`).
    fn supersede(&mut self, ino: u64, node: u64, mode: LockMode) {
        if self.lapsed.is_empty() {
            return;
        }
        let Some(v) = self.lapsed.get(&ino) else {
            return;
        };
        let gone: Vec<GrantId> = v
            .iter()
            .filter(|e| e.node == node || e.mode.conflicts(mode))
            .map(|e| e.id)
            .collect();
        for id in gone {
            self.forget_lapsed(id);
        }
    }

    /// The memory of every inode for which `under` holds leaves (the inode
    /// leaves this table: a delegation move, a generation's end).
    fn supersede_where(&mut self, under: impl Fn(&Grant) -> bool) {
        if self.lapsed.is_empty() {
            return;
        }
        let gone: Vec<GrantId> = self
            .lapsed
            .values()
            .flatten()
            .filter(|g| under(g))
            .map(|g| g.id)
            .collect();
        for id in gone {
            self.forget_lapsed(id);
        }
    }

    /// The table's clock now (see `LockTables::set_clock`), or
    /// `fallback` while none is set.
    fn now_or(&self, fallback: Now) -> Now {
        match &self.clock {
            Some(clock) => clock(),
            None => fallback,
        }
    }

    /// An op tagged with `until_h` (a token's window end, this node's
    /// honour clock: it minted the token) may still execute somewhere until
    /// then: `grant` is not released before that has passed *on this node's
    /// wait clock* (plan 43 C7-H1, R8; C7b, row 6: the honour end is mapped
    /// to wait now, both readings of this node; no wall value is in it).
    fn pin(&mut self, grant: GrantId, until_h: i64, now: Now) {
        let wait = now.wait_of_honour(HonourMs(until_h));
        let e = self.inflight.entry(grant).or_default();
        e.pinned_until = e.pinned_until.max(wait);
    }

    /// A mutation tagged with `grant` ended in doubt: `rid` when known
    /// (resolvable later by its outcome), else an unresolvable doubt —
    /// unless the grant already has a doubt noted (the op's scope ending
    /// after the doubt was noted with its rid).
    fn note_doubted(&mut self, grant: GrantId, since: WaitMs, rid: Option<crate::rid::Rid>) {
        let e = self.doubted.entry(grant).or_insert_with(|| Doubt {
            since,
            rids: std::collections::BTreeSet::new(),
            anon: false,
        });
        match rid {
            Some(r) => {
                e.rids.insert(r);
            }
            None if e.rids.is_empty() => e.anon = true,
            None => {}
        }
        while self.doubted.len() > ENDED_KEPT {
            let Some(oldest) = self
                .doubted
                .iter()
                .min_by_key(|(_, d)| d.since)
                .map(|(id, _)| *id)
            else {
                break;
            };
            self.doubted.remove(&oldest);
        }
    }

    /// `id` ended here with its record running to `until` (wait clock).
    fn note_ended(&mut self, id: GrantId, until: WaitMs) {
        let keep = WaitMs(until.0.saturating_add(self.ended_keep_ms));
        self.ended.insert(id, keep);
        while self.ended.len() > ENDED_KEPT {
            self.ended.pop_first();
        }
    }

    /// `owner` has no local lock left: its fence lifts.
    fn lift_if_unlocked(&mut self, owner: u64) {
        if self.fenced_owners.contains_key(&owner)
            && !self
                .local
                .values()
                .any(|v| v.iter().any(|l| l.owner == owner))
        {
            self.fenced_owners.remove(&owner);
        }
    }

    /// The first lapse among the grants under local locks of owners not
    /// fenced yet: before it [`Self::capture_lapsed`] has nothing to add
    /// (`i64::MIN`: a local lock has no covering grant; `i64::MAX`: no
    /// such lock). The owner fence's arming time is this, or `i64::MIN`
    /// while an owner is fenced already ([`LockTables::refence`]).
    fn next_lapse(&self) -> i64 {
        self.local
            .iter()
            .filter(|(_, v)| v.iter().any(|l| !self.fenced_owners.contains_key(&l.owner)))
            .map(|(ino, v)| {
                let needed = if v.iter().any(|l| l.write) {
                    LockMode::Exclusive
                } else {
                    LockMode::Shared
                };
                self.held
                    .get(ino)
                    .filter(|h| h.mode.covers(needed))
                    .map_or(i64::MIN, |h| h.until.0)
            })
            .min()
            .unwrap_or(i64::MAX)
    }

    /// Plan 43 C7-F F-c: the held grant on `ino` at `now`. A lapsed grant
    /// that pins something (local locks under it, or a write that may be
    /// unpublished) becomes *lapsing* (`park_ms > 0`) instead of lapsed; a
    /// lapsing one past its deadline gives up (as a lapse always did).
    fn observe(&mut self, ino: u64, now: Now, park_ms: i64) -> Observed {
        let Some(h) = self.held.get(&ino).copied() else {
            return Observed::Gone;
        };
        if h.until > now.honour {
            return Observed::Honoured;
        }
        if park_ms <= 0 {
            return Observed::Gone;
        }
        match h.lapsing {
            Some(l) if now.wait >= l.deadline => {
                self.give_up(ino, now);
                Observed::Gone
            }
            Some(_) => Observed::Lapsing,
            None => {
                let pinned = self.local.get(&ino).is_some_and(|v| !v.is_empty()) || h.written;
                if !pinned {
                    return Observed::Gone;
                }
                if let Some(h) = self.held.get_mut(&ino) {
                    h.lapsing = Some(Lapsing {
                        since: h.until,
                        at: now.wait,
                        deadline: WaitMs(now.wait.0.saturating_add(park_ms)),
                    });
                }
                self.stats.lapse_parks += 1;
                Observed::Lapsing
            }
        }
    }

    /// A lapsing grant's park ended without an answer in time: handled as
    /// a lapse was before F-c (dropped, tainted, its owners captured, since
    /// the end of its window).
    fn give_up(&mut self, ino: u64, now: Now) {
        let Some(h) = self.held.remove(&ino) else {
            return;
        };
        self.tombstone(h.id);
        self.taint(ino);
        self.capture(ino, h.until);
        self.stats.lapse_park_gave_up += 1;
        if let Some(l) = h.lapsing {
            self.stats.lapse_park_ms_max = self
                .stats
                .lapse_park_ms_max
                .max(now.wait.0.saturating_sub(l.at.0).max(0) as u64);
        }
    }

    /// The last local lock under a lapsing grant went and nothing else pins
    /// it (no write under it): it is dropped without a taint or a capture —
    /// there is nothing left for a late renewal to protect.
    fn quiet_drop_lapsing(&mut self, ino: u64) {
        let quiet = self
            .held
            .get(&ino)
            .is_some_and(|h| h.lapsing.is_some() && !h.written)
            && self.local.get(&ino).is_none_or(|v| v.is_empty());
        if quiet {
            if let Some(h) = self.held.remove(&ino) {
                self.tombstone(h.id);
            }
        }
    }

    /// Local locks on `ino` and no honoured grant *covering* them. An
    /// exclusive local lock under a shared grant is fenced too: its
    /// exclusive grant lapsed and another local owner's request brought a
    /// shared one, which other nodes may share (sim
    /// `locks-released-writes` seed 211727: the exclusive holder's next
    /// write went through under node 3's shared grant while node 1 held
    /// one too).
    fn fenced_at(&self, ino: u64, now: HonourMs) -> bool {
        let Some(v) = self.local.get(&ino).filter(|v| !v.is_empty()) else {
            return false;
        };
        let needed = if v.iter().any(|l| l.write) {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        };
        !self
            .held
            .get(&ino)
            .is_some_and(|h| h.until > now && h.mode.covers(needed))
    }
}

#[derive(Default)]
pub struct LockTables {
    inner: Mutex<Inner>,
    /// Inodes with at least one local lock: the fence's fast path.
    local_inos: AtomicUsize,
    /// Held grants plus tainted inodes: [`LockTables::take_discard`]'s
    /// fast path (with `local_inos`).
    tracked: AtomicUsize,
    next_seq: AtomicU64,
    /// Discard error events ever noted (the last one's sequence number):
    /// [`LockTables::error_seq`]'s fast path while it is 0.
    error_events: AtomicU64,
    /// The owner fence's fast path ([`LockTables::refence`]): no owner is
    /// fenced before this time. Only read while `local_inos` is non-zero,
    /// and refreshed with every change to `held` or `local`.
    fence_from_ms: AtomicI64,
    /// [`Inner::next_lapse`], kept with `fence_from_ms`: before it no
    /// owner not fenced yet can be.
    lapse_from_ms: AtomicI64,
    /// Lock grants made by an earlier tenure (a restart inside the lease,
    /// a fast takeover) may be honoured until this time: no new grant
    /// before it (reclaims are accepted).
    /// On this node's wait clock (plan 43 C7-H1).
    quarantine_ms: AtomicI64,
    /// How long after its install a recalled grant waiting for its first
    /// local lock is still renewed (0: [`FIRST_USE_BUDGET_MS`]).
    first_use_budget_ms: AtomicI64,
    /// Expired grant records were dropped (an outwait noticed by a
    /// conflicting request or a token check) since the sequencer last
    /// asked ([`LockTables::take_expired_dropped`]): its backup's mirror
    /// is out of date.
    expired_dropped: AtomicBool,
    /// The sum of `Inner::pins`: [`LockTables::unsettled_writes`]'s fast
    /// path while no tagged write is in a transaction here.
    pinned: AtomicUsize,
    /// Plan 43 C7-F F-c: how long a lapsing grant parks the I/O it fences
    /// before the lapse is handled as before (ms, wait clock; `lock_owner_wait_max_ms`,
    /// set by the core). 0: a lapse is never parked (the rule before).
    lapse_park_ms: AtomicI64,
    /// Held grants, local locks and taints, summed (kept with them,
    /// [`LockTables::refence`]): [`LockTables::active`].
    active_n: AtomicUsize,
    /// Held grants that are lapsing now (kept with `held`, [`LockTables::track`]):
    /// the engine's one relaxed load on every op while none is.
    lapsing_n: AtomicUsize,
    /// A lower bound of the earliest end (honour ms) of a held grant, kept
    /// with `held` ([`LockTables::track`], [`LockTables::install_held`], and
    /// recomputed by [`LockTables::may_park`] when it is passed): while no
    /// grant is lapsing and none ends within [`PARK_LOOKAHEAD_MS`] no op
    /// can find one lapsing, so the park gates do nothing — a clock reading
    /// and two relaxed loads on the hot path of a node that holds a lock,
    /// not a table lookup per fence. 0 (the default): not known, recomputed.
    next_end: AtomicI64,
    /// `honour − wait` as of the last full reading ([`LockTables::may_park`]):
    /// the hot path reads the wait clock alone (one host clock, not four) and
    /// estimates the honour reading from it ([`LockTables::may_park_est`]).
    honour_offset: AtomicI64,
    /// Tests only: [`LockTables::set_tag_check_at_write_for_tests`].
    no_check_at_write: AtomicBool,
    /// Tests only: [`LockTables::set_settle_for_tests`].
    no_settle: AtomicBool,
    /// Tests only: [`LockTables::set_gap_hook_for_tests`].
    #[cfg(any(test, feature = "test-util"))]
    gap_hook: Mutex<Option<GapHook>>,
}

/// See [`LockTables::set_gap_hook_for_tests`].
#[cfg(any(test, feature = "test-util"))]
pub type GapHook = std::sync::Arc<dyn Fn(&LockTables, &'static str) + Send + Sync>;

/// The tables' mutex guard. In debug and test builds it counts the guards
/// this thread holds ([`assert_no_guard`]): nothing under the tables' mutex
/// may touch the store (plan 41 L1), because the mutex is every FUSE fence
/// check's and, with the lane, every renewal's, and a store access can take
/// as long as a stalled device. In release builds it is the plain guard.
#[cfg(debug_assertions)]
struct Guard<'a>(std::sync::MutexGuard<'a, Inner>);

#[cfg(not(debug_assertions))]
type Guard<'a> = std::sync::MutexGuard<'a, Inner>;

#[cfg(debug_assertions)]
thread_local! {
    static GUARDS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[cfg(debug_assertions)]
impl std::ops::Deref for Guard<'_> {
    type Target = Inner;
    fn deref(&self) -> &Inner {
        &self.0
    }
}

#[cfg(debug_assertions)]
impl std::ops::DerefMut for Guard<'_> {
    fn deref_mut(&mut self) -> &mut Inner {
        &mut self.0
    }
}

#[cfg(debug_assertions)]
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        GUARDS.with(|n| n.set(n.get() - 1));
    }
}

/// Debug and test builds: panics if this thread holds a [`LockTables`]
/// guard. The store's transaction entry points ([`crate::store`]'s
/// `GatedDb::write_tx` and `read_tx`) call it with `what` naming the entry:
/// a store access under the tables' mutex is a bug (plan 41 L1b). Nothing in
/// release builds.
#[inline]
pub(crate) fn assert_no_guard(what: &str) {
    #[cfg(debug_assertions)]
    GUARDS.with(|n| {
        assert!(
            n.get() == 0,
            "{what} called while this thread holds the lock tables' mutex (plan 41 L1b: \
             nothing under it may touch the store)"
        )
    });
    #[cfg(not(debug_assertions))]
    let _ = what;
}

/// The host clock (µs) at a token check that let an op through, when the
/// evidence line ([`log_executed`]) is wanted at all — read inside the
/// table's critical section, so it stamps the check; the line is written
/// after the section.
fn executed_stamp() -> Option<i64> {
    tracing::enabled!(target: "constellation::token_exec", tracing::Level::DEBUG).then(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as i64)
    })
}

/// Evidence for a harness (off by default): when each grant last let an op
/// through here, on this node's clock — none may after another node got the
/// lock. In the message, not as fields: a harness parses it from logs that
/// style field names. `host_us` is the host clock at the check
/// ([`executed_stamp`]), so a harness can tell which clock step (plan 43
/// C7-H1) was in force for `at_ms` instead of guessing from the line's time.
fn log_executed(tag: &LockTag, now_ms: i64, host_us: i64) {
    for t in &tag.0 {
        tracing::debug!(
            target: "constellation::token_exec",
            "tagged op executed minter={} seq={} at_ms={now_ms} host_us={host_us}",
            t.grant.node,
            t.grant.seq,
        );
    }
}

/// See `Inner::doubted`.
#[derive(Clone, Debug)]
struct Doubt {
    /// Eviction order only (the wait clock).
    since: WaitMs,
    rids: std::collections::BTreeSet<crate::rid::Rid>,
    /// An op whose rid is unknown ended in doubt: never resolved.
    anon: bool,
}

/// The default first-use budget: the session wait (2 s), the kernel
/// invalidation wait (1 s) and a margin (1 s) — what may pass between a
/// grant's arrival and the local lock it was asked for.
pub const FIRST_USE_BUDGET_MS: i64 = 4_000;

/// What a local lock request needs from the cross-node level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalOutcome {
    /// Done under the grant held.
    Done,
    /// Another local owner holds a conflicting range (the lock returned
    /// to `getlk`; `EAGAIN`/wait for `setlk`).
    Conflict(LocalLock),
    /// No grant covers `mode` (none, lapsed, recalled, or shared for an
    /// exclusive request): ask the sequencer for one.
    NeedGrant(LockMode),
    /// Phase 2: the grant covers it, but no local lock is left on the
    /// inode and what an earlier turn already counts as done may still
    /// land at the minter, which honours the grant, after this turn's
    /// writes: a close's publication of the file still in flight
    /// ([`LockTables::publish_begin`]), or a mutation tagged with the grant
    /// that ended in doubt inside its window. Mutations still waiting for
    /// their answer do not count: the application has not seen them done,
    /// so they are concurrent with the unlock, and on a local file system
    /// too they may take effect after the next holder's. Wait and ask
    /// again (as for `Conflict`), or `EAGAIN`.
    Predecessor,
    /// Plan 43 C7-F F-c: the grant held is lapsing (its window ended while
    /// it pins something and the owner has not yet admitted or refused a
    /// late renewal of it): wait and ask again (a non-blocking request, as
    /// for a grace, from the wait pool).
    Parked,
}

impl LockTables {
    fn lock(&self) -> Guard<'_> {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        #[cfg(debug_assertions)]
        {
            GUARDS.with(|n| n.set(n.get() + 1));
            Guard(g)
        }
        #[cfg(not(debug_assertions))]
        g
    }

    /// After a change to `held` or `taint` (under the table lock).
    fn track(&self, g: &Inner) {
        self.next_end.store(
            g.held.values().map(|h| h.until.0).min().unwrap_or(i64::MAX),
            Ordering::Relaxed,
        );
        self.tracked
            .store(g.held.len() + g.taint.len(), Ordering::Relaxed);
        self.lapsing_n.store(
            g.held.values().filter(|h| h.lapsing.is_some()).count(),
            Ordering::Relaxed,
        );
        self.refence(g);
    }

    /// Plan 43 C7-F F-c: how long a lapsing grant parks (see the field).
    pub fn set_lapse_park_ms(&self, ms: i64) {
        self.lapse_park_ms.store(ms.max(0), Ordering::Relaxed);
    }

    fn park_ms(&self) -> i64 {
        self.lapse_park_ms.load(Ordering::Relaxed)
    }

    /// Whether this node holds a grant, a local lock or a taint (one
    /// relaxed load): the park gates' fast path (plan 43 C7-F F-c).
    pub fn active(&self) -> bool {
        self.active_n.load(Ordering::Relaxed) != 0
    }

    /// Plan 43 C7-F F-c: whether an op at `now` may find a held grant
    /// lapsing: one is already, or one ends within [`PARK_LOOKAHEAD_MS`]
    /// (the ops that wait for the verdict are wrapped and gated from there
    /// on; the window covers the time an op spends queued between the
    /// frontend's look and the engine's check, and an op that still slips
    /// through waits inline). Two relaxed loads while none is near; the
    /// earliest end is recomputed once it is passed (a renewal moved it).
    pub fn may_park(&self, now: Now) -> bool {
        self.honour_offset
            .store(now.honour.0 - now.wait.0, Ordering::Relaxed);
        if self.lapsing_n.load(Ordering::Relaxed) != 0 {
            return true;
        }
        if now.honour.0.saturating_add(PARK_LOOKAHEAD_MS) < self.next_end.load(Ordering::Relaxed) {
            return false;
        }
        let g = self.lock();
        let end = g.held.values().map(|h| h.until.0).min().unwrap_or(i64::MAX);
        self.next_end.store(end, Ordering::Relaxed);
        now.honour.0.saturating_add(PARK_LOOKAHEAD_MS) >= end
    }

    /// [`Self::may_park`] from the wait clock alone, which one host clock
    /// reading gives: the honour reading is estimated as the wait reading
    /// plus the offset of the last full reading. The two clocks tick
    /// together except for a forward step or a suspend, which the honour
    /// clock counts and the wait clock does not: the estimate then lags until
    /// the next full reading, so a grant may end before the estimate says
    /// (`true` is exact, `false` can be early by the jump): the op that
    /// slips through waits inline, and the renewal tick marks the grant
    /// lapsing within a poll interval, which opens the gates for good. The
    /// caller follows a `true` with the full [`Self::may_park`].
    pub fn may_park_est(&self, wait: WaitMs) -> bool {
        self.may_park_est_ahead(wait, PARK_LOOKAHEAD_MS)
    }

    /// [`Self::may_park_est`] with a lookahead of `ahead_ms`: the frontend's
    /// look ([`WRAP_LOOKAHEAD_MS`]), wider than the op's own gate so that an
    /// op the gate may park was wrapped to park.
    pub fn may_park_est_ahead(&self, wait: WaitMs, ahead_ms: i64) -> bool {
        if self.lapsing_n.load(Ordering::Relaxed) != 0 {
            return true;
        }
        wait.0
            .saturating_add(self.honour_offset.load(Ordering::Relaxed))
            .saturating_add(ahead_ms)
            >= self.next_end.load(Ordering::Relaxed)
    }

    /// Whether any held grant is lapsing (one relaxed load): the ops that
    /// may have to wait for one look no further while none is.
    pub fn lapsing_any(&self) -> bool {
        self.lapsing_n.load(Ordering::Relaxed) != 0
    }

    /// Plan 43 C7-F F-c: `ino`'s held grant is lapsing at `now` (a lapsed
    /// grant that pins something becomes so here).
    pub fn lapse_pending(&self, ino: u64, now: Now) -> bool {
        let mut g = self.lock();
        let lapsing = g.observe(ino, now, self.park_ms()) == Observed::Lapsing;
        self.track(&g);
        lapsing
    }

    /// Whether `ino`'s held grant is lapsing now.
    pub fn lapsing(&self, ino: u64) -> bool {
        self.lapsing_any()
            && self
                .lock()
                .held
                .get(&ino)
                .is_some_and(|h| h.lapsing.is_some())
    }

    /// After a change to `held`, `local` or the fenced owners.
    fn refence(&self, g: &Inner) {
        self.active_n.store(
            g.held.len() + g.taint.len() + g.local.len(),
            Ordering::Relaxed,
        );
        let next = g.next_lapse();
        self.lapse_from_ms.store(next, Ordering::Relaxed);
        let from = if g.fenced_owners.is_empty() {
            next
        } else {
            i64::MIN
        };
        self.fence_from_ms.store(from, Ordering::Relaxed);
    }

    /// Set the first-use budget (ms; see [`HeldGrant::installed_ms`]):
    /// the session budget plus the kernel invalidation wait plus the
    /// lease margin.
    pub fn set_first_use_budget_ms(&self, ms: i64) {
        self.first_use_budget_ms.store(ms.max(1), Ordering::Relaxed);
    }

    pub fn first_use_budget_ms(&self) -> i64 {
        match self.first_use_budget_ms.load(Ordering::Relaxed) {
            0 => FIRST_USE_BUDGET_MS,
            ms => ms,
        }
    }

    /// Lock grants an earlier tenure made may be live until `until` (this
    /// node's wait clock): grant nothing new before then (never lowered).
    pub fn set_quarantine(&self, until: WaitMs) {
        self.quarantine_ms.fetch_max(until.0, Ordering::Relaxed);
    }

    pub fn quarantine_until(&self) -> WaitMs {
        WaitMs(self.quarantine_ms.load(Ordering::Relaxed))
    }

    pub fn stats(&self) -> LockStats {
        self.lock().stats
    }

    pub fn with_stats(&self, f: impl FnOnce(&mut LockStats)) {
        f(&mut self.lock().stats);
    }

    // ------------------------------------------------------ sequencer

    /// Grant ids of a sequencer incarnation never collide with an earlier
    /// one's (a restarted sequencer must not re-mint an id a node still
    /// holds): seed the counter above `base`.
    pub fn seed_ids(&self, base: u64) {
        self.next_seq.fetch_max(base, Ordering::Relaxed);
    }

    /// `node`'s live grant on `ino`, if any.
    pub fn own_grant(&self, ino: u64, node: u64, now: WaitMs) -> Option<Grant> {
        self.lock()
            .grants
            .values()
            .find(|e| e.ino == ino && e.node == node && e.until > now)
            .copied()
    }

    /// Strengthen (never weaken) a grant in place, keeping its id and its
    /// recalled flag, and extend it.
    pub fn upgrade(&self, id: GrantId, mode: LockMode, until: WaitMs) -> bool {
        let mut g = self.lock();
        match g.grants.get_mut(&id) {
            Some(e) => {
                e.mode = e.mode.max(mode);
                e.until = e.until.max(until);
                let (ino, node, mode) = (e.ino, e.node, e.mode);
                g.supersede(ino, node, mode);
                true
            }
            None => false,
        }
    }

    /// Make a grant; `me` is this sequencer's id (the grant's minter),
    /// `gen` the generation it is made under (0: the root).
    ///
    /// The record of `node` on `ino` it replaces (a re-affirmation, or an
    /// expired one) is read here, in the same critical section: the new
    /// record never ends sooner than it (plan 41 L1). A caller that read
    /// the old record earlier and took the later end of the two itself
    /// replaced a record a second writer had extended since (the lock lane
    /// extends an owner's own records) with a shorter one, inside the
    /// window its holder honours.
    pub fn grant(
        &self,
        me: u64,
        node: u64,
        ino: u64,
        mode: LockMode,
        until: WaitMs,
        gen: u64,
    ) -> GrantId {
        let id = GrantId {
            node: me,
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed) + 1,
        };
        let mut g = self.lock();
        let mut until = until;
        g.grants.retain(|_, e| {
            if e.node == node && e.ino == ino {
                until = until.max(e.until);
                false
            } else {
                true
            }
        });
        g.grants.insert(
            id,
            Grant {
                id,
                node,
                ino,
                mode,
                until,
                recalled: false,
                gen,
                confirmed: Grant::MINTED,
                // A mint: made only where no grace, floor or barrier holds
                // the inode, so every grant before it was issued before it
                // (`Grant::settled`).
                settled: true,
            },
        );
        g.supersede(ino, node, mode);
        g.stats.grants_made += 1;
        id
    }

    /// Install a grant made elsewhere (a reclaim, a delegation move, a
    /// backup's mirror), keeping its id; a grant of the same node on the
    /// same inode is replaced.
    pub fn install(&self, grant: Grant) {
        let mut g = self.lock();
        g.grants
            .retain(|_, e| !(e.node == grant.node && e.ino == grant.ino));
        g.supersede(grant.ino, grant.node, grant.mode);
        g.grants.insert(grant.id, grant);
    }

    /// [`Self::install`] for a grant that moved carrying what was left of
    /// its record (a delegation's recall answer, `Core::lock_install_moved`):
    /// the record it replaces — the same node's on the same inode, live or
    /// expired — is never shortened, the later end of the two is kept (a
    /// reclaim this table answered in a grace before the move arrived
    /// extended its holder's window past what the carried record says).
    pub fn install_keeping_later(&self, grant: Grant) {
        let mut g = self.lock();
        let mut grant = grant;
        g.grants.retain(|_, e| {
            if e.node == grant.node && e.ino == grant.ino {
                grant.until = grant.until.max(e.until);
                false
            } else {
                true
            }
        });
        g.supersede(grant.ino, grant.node, grant.mode);
        g.grants.insert(grant.id, grant);
    }

    /// Install a grant made elsewhere unless this table already has a
    /// grant of the same node on the same inode, or a conflicting grant
    /// of another node there — for the root's copy of a delegation
    /// handoff, which is re-sent and may be reinstated after the table
    /// moved on. Either grant here is newer than the copy: an owner that
    /// knew the copy granted the node again (its holder replaced the old
    /// id) or granted the conflicting one only after the copy left its
    /// table (released, or expired at its holder). Reinstating it anyway
    /// would put two conflicting grants in one table, and a request of
    /// the copy's node would then be re-affirmed without a recall (sim
    /// `locks-delegated` seed 196004). `false`: not installed.
    ///
    /// A record that expired by `now` is no grant: it is dropped
    /// first, as [`Self::conflicting`] drops it before granting over it.
    /// Kept, it refused the copy while the owner already granted past it
    /// (sim `locks-unlinked-delegated-dbackup-random` seed 5681: the
    /// root's stale record of the same holder on an unlinked inode kept
    /// the grant that came with the unlink out, and the root granted the
    /// inode to another node while the holder was inside its section).
    pub fn install_if_consistent(&self, grant: Grant, now: WaitMs) -> bool {
        let mut g = self.lock();
        if g.drop_expired(now) {
            self.expired_dropped.store(true, Ordering::Relaxed);
        }
        if g.grants
            .values()
            .any(|e| e.ino == grant.ino && (e.node == grant.node || e.mode.conflicts(grant.mode)))
        {
            return false;
        }
        g.supersede(grant.ino, grant.node, grant.mode);
        g.grants.insert(grant.id, grant);
        true
    }

    /// Live grants on `ino` held by nodes other than `node` that conflict
    /// with `mode`. Expired grants are dropped on the way.
    pub fn conflicting(&self, ino: u64, node: u64, mode: LockMode, now: WaitMs) -> Vec<Grant> {
        let mut g = self.lock();
        if g.drop_expired(now) {
            self.expired_dropped.store(true, Ordering::Relaxed);
        }
        g.grants
            .values()
            .filter(|e| e.ino == ino && e.node != node && e.mode.conflicts(mode))
            .copied()
            .collect()
    }

    /// Any live grant on `ino` by another node that conflicts with
    /// `mode` (for `getlk`).
    pub fn first_conflicting(
        &self,
        ino: u64,
        node: u64,
        mode: LockMode,
        now: WaitMs,
    ) -> Option<Grant> {
        self.conflicting(ino, node, mode, now).into_iter().next()
    }

    pub fn get(&self, id: GrantId) -> Option<Grant> {
        let got = self.lock().grants.get(&id).copied();
        #[cfg(any(test, feature = "test-util"))]
        self.gap("get");
        got
    }

    /// Whether this table ended `id` (released, or outwaited) and still
    /// remembers so ([`Self::forget`]). A grant that moved out with a
    /// delegation ([`Self::take_where`]) did not end here.
    pub fn was_ended(&self, id: GrantId) -> bool {
        self.lock().ended.contains_key(&id)
    }

    /// Extend a known grant of `node` at its renewal (`now`): `(mode,
    /// recalled)`, or `None` when unknown. The record ends at `until`
    /// (wait clock); the renewal, sent by the holder at honour reading
    /// `sent` of its clock `clock` (`from`), confirms an installed copy from
    /// that send on ([`Grant::confirmed`]: the holder's own readings, so no
    /// offset between the nodes is in the compare with its tokens).
    pub fn extend(
        &self,
        id: GrantId,
        node: u64,
        until: WaitMs,
        from: (u64, i64),
    ) -> Option<(LockMode, bool)> {
        let mut g = self.lock();
        let e = g.grants.get_mut(&id).filter(|e| e.node == node)?;
        e.until = e.until.max(until);
        e.confirm(from.0, from.1);
        Some((e.mode, e.recalled))
    }

    /// [`Self::extend`] for a record that is still live: `None` unless
    /// `id` is a record of `node` that ends after *now* (this table's
    /// clock, `fallback` while none is set; read inside the critical
    /// section). A second writer's extension (the lock lane) must never
    /// revive what the core may already have expired and outwaited, so
    /// it extends only what is live at the instant it extends. Like
    /// [`Self::extend`], the renewal confirms an installed copy from its
    /// send `from` (the holder's clock id and honour reading: plan 43 C7b,
    /// row 29), never from this table's clock.
    pub fn extend_live(
        &self,
        id: GrantId,
        node: u64,
        until: WaitMs,
        fallback: Now,
        from: (u64, i64),
    ) -> Option<(LockMode, bool)> {
        let mut g = self.lock();
        let now = g.now_or(fallback);
        let e = g
            .grants
            .get_mut(&id)
            .filter(|e| e.node == node && e.until > now.wait)?;
        e.until = e.until.max(until);
        e.confirm(from.0, from.1);
        Some((e.mode, e.recalled))
    }

    pub fn mark_recalled(&self, id: GrantId) -> bool {
        let mut g = self.lock();
        match g.grants.get_mut(&id) {
            Some(e) => {
                let first = !e.recalled;
                e.recalled = true;
                first
            }
            None => false,
        }
    }

    /// The grant ended here (released, or outwaited): gone from the
    /// table, and remembered as ended until its record would have expired
    /// (a token naming it is rejected meanwhile, [`Self::check_tag`]).
    pub fn forget(&self, id: GrantId) -> Option<Grant> {
        let mut g = self.lock();
        let gone = g.grants.remove(&id);
        if let Some(e) = gone {
            g.note_ended(id, e.until);
        }
        // Released: nothing of it is left to admit late.
        g.forget_lapsed(id);
        gone
    }

    /// As [`Self::forget`] for a grant this table *outwaited* (its holder
    /// never released it: the recall's expiry): its record is gone, and a
    /// settled one stays in the lapsed memory a late renewal may be
    /// admitted from — until a conflicting grant, or its inode leaving
    /// this table, supersedes it.
    pub fn forget_outwaited(&self, id: GrantId) -> Option<Grant> {
        let mut g = self.lock();
        let gone = g.grants.remove(&id);
        if let Some(e) = gone {
            g.note_ended(id, e.until);
            if e.settled {
                g.remember_lapsed(e);
            }
        }
        gone
    }

    /// The expiry timer of grant `id` fired: if the record is still expired
    /// *now*, end it as [`Self::forget_outwaited`] does and return it
    /// (`Some(Ok)`); if it was extended meanwhile, leave it and say until
    /// when (`Some(Err(until))`: arm the timer again); `None`: no such
    /// record (released, replaced, or dropped as expired by a token check).
    ///
    /// One critical section, with `now` read inside it (this table's clock,
    /// `fallback` while none is set): a second writer extending the record
    /// (the lock lane, plan 41) either lands before it, and the record is
    /// kept, or after it, and finds no record to extend. Reading the record
    /// first and forgetting it by id later (what the core did) forgot a
    /// record extended in between.
    pub fn forget_outwaited_if_expired(
        &self,
        id: GrantId,
        fallback: Now,
    ) -> Option<Result<Grant, WaitMs>> {
        #[cfg(any(test, feature = "test-util"))]
        self.gap("forget_if_expired");
        let mut g = self.lock();
        let now = g.now_or(fallback);
        let e = *g.grants.get(&id)?;
        if e.until > now.wait {
            return Some(Err(e.until));
        }
        g.grants.remove(&id);
        g.note_ended(id, e.until);
        if e.settled {
            g.remember_lapsed(e);
        }
        Some(Ok(e))
    }

    /// `new` replaced `old` of the same node while `old` was live (see
    /// `Inner::continues`). Only a *settled* `old` continues: an unsettled
    /// one (a reclaim in a grace, a non-final mirror's record, a reinstated
    /// copy) proves nothing about what was granted between its issue and
    /// its arrival in this table, so a late renewal of its id must stay
    /// refused although the replacement is settled (review of F-c: a
    /// non-final mirror's record replaced by a mint for the same node, then
    /// the holder's late renewal of the old id admitted as the new one).
    pub fn note_continues(&self, old: &Grant, new: GrantId) {
        if !old.settled {
            return;
        }
        let old = old.id;
        let mut g = self.lock();
        g.continues.insert(old, new);
        g.continues_order.push_back(old);
        while g.continues_order.len() > ENDED_KEPT {
            if let Some(o) = g.continues_order.pop_front() {
                g.continues.remove(&o);
            }
        }
    }

    /// Plan 43 C7-F F-c: a **late** renewal of `id` by `node` (a holder
    /// whose window ended and that pins something asks to have the same
    /// grant back) in `mode`, at `now`: admitted when this table can prove
    /// that no grant conflicting with it was issued since it was made —
    /// the record is here, settled, live or expired and not dropped yet
    /// (looked up before anything drops the expired ones), or its memory
    /// is ([`Inner::lapsed`]: dropped by an outwait, and nothing
    /// conflicting granted on the inode since). `Admitted`: the record is
    /// live until `until` again, settled, confirmed from the late renewal's
    /// send `from` (the holder's clock id and honour reading: a token whose
    /// window ended before is refused as one past the lapse), and no
    /// longer remembered as ended; `recalled` is `waiter`. Anything else —
    /// an unknown grant, an unsettled record, a mode that does not cover,
    /// a memory superseded — is `Refused`, grace or no grace: an owner
    /// cannot tell what was granted meanwhile.
    #[allow(clippy::too_many_arguments)]
    pub fn admit_late(
        &self,
        id: GrantId,
        node: u64,
        mode: LockMode,
        until: WaitMs,
        waiter: bool,
        now: Now,
        from: (u64, i64),
    ) -> Option<LateAdmitted> {
        let mut g = self.lock();
        // An id this table replaced while it was live is its replacement.
        let mut id = id;
        if !g.grants.contains_key(&id) && !g.lapsed.values().flatten().any(|e| e.id == id) {
            for _ in 0..4 {
                let Some(next) = g.continues.get(&id).copied() else {
                    break;
                };
                id = next;
                if g.grants.contains_key(&id) {
                    break;
                }
            }
        }
        // Belt and braces: a live record of another node in a conflicting
        // mode beside the one admitted would mean a supersede path is
        // missing; never admit beside it.
        let beside = |g: &Inner, ino: u64, node: u64, mode: LockMode| {
            g.grants.values().any(|o| {
                o.ino == ino && o.until > now.wait && o.node != node && o.mode.conflicts(mode)
            })
        };
        if let Some(e) = g
            .grants
            .get(&id)
            .filter(|e| e.node == node && e.mode.covers(mode) && e.settled)
            .copied()
        {
            if beside(&g, e.ino, e.node, e.mode) {
                debug_assert!(false, "a settled record beside a conflicting live grant");
                g.stats.late_refused += 1;
                return None;
            }
            let e = g.grants.get_mut(&id).expect("present");
            e.until = e.until.max(until);
            e.confirmed = Some(from);
            let out = LateAdmitted {
                id,
                mode: e.mode,
                recalled: e.recalled,
                from_memory: false,
            };
            g.ended.remove(&id);
            g.stats.late_admitted += 1;
            return Some(out);
        }
        let known = g.lapsed.values().flatten().any(|e| e.id == id);
        if known {
            let ok = g
                .lapsed
                .values()
                .flatten()
                .find(|e| e.id == id)
                .is_some_and(|e| e.node == node && e.mode.covers(mode) && e.settled);
            let conflicting = g
                .lapsed
                .values()
                .flatten()
                .find(|e| e.id == id)
                .is_some_and(|e| beside(&g, e.ino, e.node, e.mode));
            if ok && !conflicting {
                let mut e = g.forget_lapsed(id).expect("remembered");
                e.until = until;
                e.recalled = waiter;
                e.confirmed = Some(from);
                e.settled = true;
                // The same node's older record on the inode is replaced.
                g.grants
                    .retain(|_, o| !(o.node == e.node && o.ino == e.ino));
                g.supersede(e.ino, e.node, e.mode);
                g.grants.insert(e.id, e);
                g.ended.remove(&id);
                g.stats.late_admitted += 1;
                return Some(LateAdmitted {
                    id,
                    mode: e.mode,
                    recalled: waiter,
                    from_memory: true,
                });
            }
            // A conflicting live record beside the memory means a supersede
            // path is missing: refuse (never admit beside it).
            debug_assert!(
                !(ok && conflicting),
                "a lapsed memory beside a conflicting live grant"
            );
        }
        g.stats.late_refused += 1;
        None
    }

    /// Plan 43 C7-F F-c, the old root's **final** mirror: every record
    /// present as of `now` (`lapsed` for those expired on this wait clock)
    /// and every lapsed memory (`lapsed`), each with its `settled` flag.
    /// The successor of a fast takeover that finds this the final mirror of
    /// the lease it replaced installs the live records and keeps the
    /// lapsed ones as its memory ([`Self::install_lapsed`]).
    pub fn final_snapshot(&self, now: Now) -> Vec<GrantWire> {
        let g = self.lock();
        let mut out: Vec<GrantWire> = g
            .grants
            .values()
            .map(|e| GrantWire {
                lapsed: e.until <= now.wait,
                ..e.to_wire(now)
            })
            .collect();
        out.extend(g.lapsed.values().flatten().map(|e| GrantWire {
            lapsed: true,
            ..e.to_wire(now)
        }));
        out
    }

    /// Plan 43 C7-F F-c: keep `grant` — a record the old root had expired
    /// when it sent its final mirror — as this table's memory of a lapsed
    /// grant (not live: it blocks nobody; a conflicting grant here
    /// supersedes it). Unsettled ones are not kept (they can be admitted by
    /// nothing).
    pub fn install_lapsed(&self, grant: Grant) {
        if !grant.settled {
            return;
        }
        let mut g = self.lock();
        // A live record of another node in a conflicting mode (or of the
        // same node) supersedes it at once.
        let beside = g
            .grants
            .values()
            .any(|o| o.ino == grant.ino && (o.node == grant.node || o.mode.conflicts(grant.mode)));
        if !beside {
            g.remember_lapsed(grant);
        }
    }

    /// How many lapsed memories are kept (tests, status).
    pub fn lapsed_len(&self) -> usize {
        self.lock().lapsed_order.len()
    }

    /// The lapsed memory of `id`, if any (tests).
    pub fn lapsed_get(&self, id: GrantId) -> Option<Grant> {
        self.lock()
            .lapsed
            .values()
            .flatten()
            .find(|e| e.id == id)
            .copied()
    }

    /// How long an ended grant is remembered past the end of its record:
    /// the sequencer's `TTL + 2 × margin`. An outwaited grant ends when
    /// its record has expired already; remembered only until then, its
    /// id was forgotten at the first prune (lock-fence-token review).
    pub fn set_token_memory_ms(&self, ms: i64) {
        self.lock().ended_keep_ms = ms.max(0);
    }

    /// Plan 30 §M14 phase 2, the fencing token at the executor: `Err` with
    /// the first token of `tag` naming a grant that is no longer live — its
    /// holder's window has passed here (the token's absolute stamp has
    /// passed on this node's wall clock, or this node's honour clock has
    /// passed the receipt plus the time the holder said it had left:
    /// plan 43 C7b, row 6), or this table ended the grant (released or
    /// outwaited). The caller then executes nothing
    /// (`MetaError::LockLapsed`). An empty tag costs nothing.
    ///
    /// A grant live in this table (the minter, or the delegate it moved
    /// to) is judged exactly: it passes whatever the token's window says,
    /// as no conflicting grant can exist while it is here — a holder that
    /// kept renewing it is not refused because its op was slower than
    /// the window it set out with (lock-fence-token review). One whose
    /// record has expired here is dead (an outwait), as is one this
    /// table ended. Except: a grant installed here from a copy (a mirror,
    /// a reinstated handoff) may be one its last sequencer had ended, so
    /// a token whose window ends before the holder's first renewal here
    /// confirmed it ([`Grant::confirmed`]) is judged by that window
    /// (lock-fence-token review, round 2: an inbox op drained by a new
    /// holder that installed a stale mirror). A token with no receipt (it
    /// came by the S3 inbox or the replay queue, which have no transit
    /// bound) has no window to be judged by here: it passes only exactly,
    /// and is refused otherwise (plan 43 C7b, row 31).
    pub fn check_tag(&self, tag: &LockTag, now: Now) -> Result<(), LockToken> {
        if tag.is_empty() {
            return Ok(());
        }
        let mut g = self.lock();
        let verdict = self.judge(&mut g, tag, now, true);
        let stamp = verdict.is_ok().then(executed_stamp).flatten();
        drop(g);
        if let Some(host_us) = stamp {
            log_executed(tag, now.wall.0, host_us);
        }
        verdict
    }

    /// F1, the token *at the write*: [`Self::check_tag`] on this table's
    /// clock ([`Self::set_clock`]) read now, called with the store's
    /// writer lock held, right before the op's transaction writes
    /// anything (`Meta::begin_local`). The check before the call
    /// (`mutate::execute_tagged`) ran on a clock read before the op waited
    /// for that lock — behind a slow writer, or in a core step that took
    /// its `now` before it waited — and judged a window that may have
    /// ended meanwhile, or a grant this table has ended since (and
    /// granted to another node, whose writes may be in already).
    ///
    /// Passed, the op's grants are *pinned* until its transaction is over
    /// ([`Self::unpin`]): a position this node reports as covering what it
    /// executed waits for a pinned write under a grant no longer live here
    /// ([`Self::unsettled_writes`]), so a grant ended while the write was
    /// in its transaction (or its commit was slow) is never followed by a
    /// next holder whose floor lacks it.
    ///
    /// `checked`: the clock the earlier check read, which this one
    /// reads instead only while no clock is set (unit tests of a core
    /// with an explicit `now`).
    pub fn check_tag_at_write(&self, tag: &LockTag, checked: Now) -> Result<(), LockToken> {
        if tag.is_empty() {
            return Ok(());
        }
        let mut g = self.lock();
        let now = g.now_or(checked);
        let now_ms = now.wall.0;
        // Nothing is logged under the guard (plan 41 L1b): the verdict and
        // the host stamp are taken inside it, the lines written after it.
        match self.judge(&mut g, tag, now, false) {
            Err(t) => {
                g.stats.token_rejections_at_write += 1;
                drop(g);
                tracing::info!(
                    target: "constellation::locks",
                    minter = t.grant.node,
                    seq = t.grant.seq,
                    until_ms = t.until_ms,
                    now_ms,
                    "refused a tagged write at the store: its grant ended while it waited for the writer lock"
                );
                Err(t)
            }
            Ok(()) => {
                for t in &tag.0 {
                    let e = g.pins.entry(t.grant).or_insert((0, i64::MIN, i64::MIN));
                    e.0 += 1;
                    e.1 = e.1.max(t.until_ms);
                    // A token with no receipt passed exactly and has no
                    // window of its own: once its grant is not live here, it
                    // is over (`unsettled_writes`), never "open" by the wall
                    // stamp alone.
                    e.2 = e.2.max(
                        t.recv
                            .map_or(i64::MIN, |r| r.saturating_add(t.remaining_ms)),
                    );
                }
                self.pinned.fetch_add(tag.0.len(), Ordering::SeqCst);
                let stamp = executed_stamp();
                drop(g);
                if let Some(host_us) = stamp {
                    log_executed(tag, now_ms, host_us);
                }
                Ok(())
            }
        }
    }

    /// The transaction a [`Self::check_tag_at_write`] that passed with
    /// `tag` pinned for is over (committed or abandoned).
    pub fn unpin(&self, tag: &LockTag) {
        if tag.is_empty() {
            return;
        }
        let mut g = self.lock();
        for t in &tag.0 {
            if let Some(e) = g.pins.get_mut(&t.grant) {
                e.0 -= 1;
                if e.0 == 0 {
                    g.pins.remove(&t.grant);
                }
            }
        }
        self.pinned.fetch_sub(tag.0.len(), Ordering::SeqCst);
    }

    /// F1: a tagged write is in its transaction here under a grant whose
    /// next holder may be granted already: one this table ended (released,
    /// outwaited) or whose record here has run out, or — a grant this
    /// table does not hold (another sequencer's, judged by its window) —
    /// one whose token's window is over on this clock. A position read now
    /// could miss it although its op passed every check, so whoever
    /// reports one as covering what this node executed first waits for the
    /// writer lock (`Meta::settle_tagged_writes`): such a write holds it
    /// from its check to its commit.
    ///
    /// A write under another sequencer's grant whose window is still open
    /// here is not waited for: the next grant can follow only an outwait,
    /// whose barrier is after that window (`2 × margin` past it), and a
    /// cut as of a time before the window's end settles no barrier after
    /// it. So a delegate's renewal does not wait for every tagged commit
    /// in flight — only, after a slow commit outlasted its window, for
    /// that one. One atomic load while no tagged write is in a
    /// transaction.
    pub fn unsettled_writes(&self) -> bool {
        if self.pinned.load(Ordering::SeqCst) == 0 {
            return false;
        }
        let g = self.lock();
        // One call, three readings (plan 43 C7-H1, C7b): the table's records
        // are on this node's wait clock; a token's window is over when
        // either of its bounds is: the minter's wall stamp has passed on
        // this node's wall clock, or this node's honour clock has passed
        // the receipt plus the time the holder said it had left.
        let now = g.now_or(Now::same(i64::MIN));
        g.pins
            .iter()
            .any(|(id, (_, until, until_h))| match g.grants.get(id) {
                Some(e) => e.until <= now.wait,
                None => {
                    g.ended.contains_key(id) || now.wall.0 >= *until || now.honour.0 >= *until_h
                }
            })
    }

    /// Tests only (the simulation's non-vacuity knobs for F1): `false`
    /// switches the token check at the write off for this table, which is
    /// the code before it — an op is judged only by the check
    /// `mutate::execute_tagged` runs before it waits for the writer lock.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_tag_check_at_write_for_tests(&self, on: bool) {
        self.no_check_at_write.store(!on, Ordering::Relaxed);
    }

    /// See [`Self::set_tag_check_at_write_for_tests`].
    pub(crate) fn checks_at_write(&self) -> bool {
        !self.no_check_at_write.load(Ordering::Relaxed)
    }

    /// Tests only: `false` makes `Meta::settle_tagged_writes` wait for
    /// nothing (the code before it).
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_settle_for_tests(&self, on: bool) {
        self.no_settle.store(!on, Ordering::Relaxed);
    }

    /// See [`Self::set_settle_for_tests`].
    pub(crate) fn settles(&self) -> bool {
        !self.no_settle.load(Ordering::Relaxed)
    }

    /// Tests only: `hook(tables, point)` runs, on the calling thread and with no
    /// table lock held, at the points where a two-step sequence of the core
    /// has a gap another writer can use — after [`Self::get`] and
    /// [`Self::held`] return, and before the atomic primitives
    /// ([`Self::forget_outwaited_if_expired`],
    /// [`Self::drop_held_if_lapsed`]) take the lock (`"get"`, `"held"`,
    /// `"forget_if_expired"`, `"drop_held_if_lapsed"`). A test that lets a
    /// second writer's extension land there fails the sequence that decides
    /// on the earlier read and passes the primitive that reads inside its
    /// critical section.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_gap_hook_for_tests(&self, hook: Option<GapHook>) {
        *self.gap_hook.lock().unwrap_or_else(|e| e.into_inner()) = hook;
    }

    #[cfg(any(test, feature = "test-util"))]
    fn gap(&self, point: &'static str) {
        let hook = self
            .gap_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook(self, point);
        }
    }

    /// The clock [`Self::check_tag_at_write`] reads: the daemon's
    /// protocol clock, which its other lock decisions read too; a
    /// simulation's node clock. Unset (unit tests driving a core with an
    /// explicit `now`), the write-time check reads the clock the check
    /// before it read.
    pub fn set_clock(&self, clock: LockClock) {
        self.lock().clock = Some(clock);
    }

    /// This table's clock now, if one is set ([`Self::set_clock`]).
    pub fn clock_now(&self) -> Option<Now> {
        self.lock().clock.as_ref().map(|c| c())
    }

    /// Node side: a mutation tagged with `grant` ended in doubt (`rid`:
    /// which, if known): it may pass its check at an executor and commit
    /// there after any wait this node could take, so the grant is not
    /// released while the doubt is unresolved; its owner outwaits it,
    /// which orders the next grant after whatever lands
    /// ([`Self::doubt_of`]).
    pub fn note_doubted(&self, grant: GrantId, now: WaitMs, rid: Option<crate::rid::Rid>) {
        let mut g = self.lock();
        g.note_doubted(grant, now, rid);
    }

    /// The doubt noted on `grant`, if any: whether one of its ops is
    /// unknown (no rid: never resolvable), and the rids of the others —
    /// each resolved once its outcome is known
    /// ([`Self::resolve_doubt`]).
    pub fn doubt_of(&self, grant: GrantId) -> Option<(bool, Vec<crate::rid::Rid>)> {
        let g = self.lock();
        let mut out: Option<(bool, Vec<crate::rid::Rid>)> = None;
        // The doubts of the grant's older ids are its own (`superseded`).
        for id in g.lineage(grant) {
            if let Some(d) = g.doubted.get(&id) {
                let o = out.get_or_insert_with(|| (false, Vec::new()));
                o.0 |= d.anon;
                o.1.extend(d.rids.iter().copied());
            }
        }
        out
    }

    /// The in-doubt op `rid` under `grant` has a definitive outcome (it
    /// executed — its commit is done — or it was refused): no longer a
    /// doubt. The grant is released normally once none is left.
    pub fn resolve_doubt(&self, grant: GrantId, rid: crate::rid::Rid) {
        let mut g = self.lock();
        for id in g.lineage(grant) {
            if let Some(d) = g.doubted.get_mut(&id) {
                d.rids.remove(&rid);
                if d.rids.is_empty() && !d.anon {
                    g.doubted.remove(&id);
                }
            }
        }
    }

    /// Whether any doubt is noted on `grant` (resolved or not).
    pub fn was_doubted(&self, grant: GrantId) -> bool {
        let g = self.lock();
        g.lineage(grant).iter().any(|id| g.doubted.contains_key(id))
    }

    /// Node side: `grant` and the older ids of the same held grant (it
    /// went on under the owner's newer id), newest first: the tokens a
    /// mutation of this node may carry for it.
    pub fn lineage(&self, grant: GrantId) -> Vec<GrantId> {
        self.lock().lineage(grant)
    }

    /// Whether a mutation this node issued under `grant` may still land
    /// at an executor: one is in flight (not answered yet), or one ended
    /// in doubt ([`Self::note_doubted`]). An owner that ends its own
    /// grant meanwhile has not got it in its release floor.
    pub fn tagged_unsettled(&self, grant: GrantId) -> bool {
        let g = self.lock();
        g.lineage(grant).iter().any(|id| g.doubted.contains_key(id))
            || g.inflight_of(grant)
                .is_some_and(|e| e.active > 0 || e.publishing > 0)
    }

    /// The token check itself (see [`Self::check_tag`]), under the
    /// table lock.
    ///
    /// `count`: record how each token was judged (`LockStats::tokens_judged_*`;
    /// the check at the write repeats the first check's judgement and does
    /// not count it again).
    fn judge(&self, g: &mut Inner, tag: &LockTag, now: Now, count: bool) -> Result<(), LockToken> {
        // One call, three readings (plan 43 C7-H1, C7b): a token's window
        // is judged by this node's wall clock against the minter's absolute
        // stamp (phase 1, offset-bound) and by this node's honour clock
        // against its receipt plus the time left the holder named
        // (rate-only); the table's records (and the memory of ended grants)
        // are on its wait clock.
        let now_ms = now.wall.0;
        if g.ended.len() > 256 {
            g.ended.retain(|_, keep| *keep > now.wait);
        }
        if g.drop_expired(now.wait) {
            self.expired_dropped.store(true, Ordering::Relaxed);
        }
        let exact =
            |g: &Inner, t: &LockToken| g.grants.get(&t.grant).is_some_and(|e| e.confirms(t));
        if count {
            let path = TOKEN_PATH.with(|p| p.get()).index();
            for t in &tag.0 {
                if exact(g, t) {
                    g.stats.tokens_judged_exact[path] += 1;
                } else {
                    g.stats.tokens_judged_window[path] += 1;
                }
            }
        }
        let dead = tag
            .0
            .iter()
            .find(|t| {
                if exact(g, t) {
                    return false;
                }
                if g.ended.contains_key(&t.grant) {
                    return true;
                }
                match t.recv {
                    Some(r) => {
                        now_ms >= t.until_ms || now.honour.0 >= r.saturating_add(t.remaining_ms)
                    }
                    // No receipt: no transit bound, so no window to judge
                    // by (the inbox, the replay queue): exact or refused.
                    None => true,
                }
            })
            .copied();
        match dead {
            Some(t) => {
                g.stats.token_rejections += 1;
                Err(t)
            }
            None => Ok(()),
        }
    }

    /// Grants whose `until` has passed (still in the table).
    pub fn expired(&self, now: WaitMs) -> Vec<Grant> {
        self.lock()
            .grants
            .values()
            .filter(|e| e.until <= now)
            .copied()
            .collect()
    }

    pub fn grants_snapshot(&self) -> Vec<Grant> {
        self.lock().grants.values().copied().collect()
    }

    /// Remove and return the grants of generation `gen`.
    pub fn take_by_gen(&self, gen: u64) -> Vec<Grant> {
        let mut g = self.lock();
        let taken: Vec<Grant> = g
            .grants
            .values()
            .filter(|e| e.gen == gen)
            .copied()
            .collect();
        for t in &taken {
            g.grants.remove(&t.id);
        }
        // The generation's inodes leave this table: so does the memory.
        g.supersede_where(|e| e.gen == gen);
        taken
    }

    /// Every grant record and every lapsed memory, as `(id, inode)`: the
    /// snapshot [`Self::take_ids`] works from.
    pub fn snapshot_inos(&self) -> Vec<(GrantId, u64)> {
        let g = self.lock();
        g.grants
            .values()
            .map(|e| (e.id, e.ino))
            .chain(g.lapsed.values().flatten().map(|e| (e.id, e.ino)))
            .collect()
    }

    /// Remove and return the grants among `ids` that are still here, and
    /// forget the lapsed memories among them (as [`Self::take_where`] does
    /// for the inodes it takes). A grant installed after the caller's
    /// snapshot is not in `ids` and stays: the core's next event handles it,
    /// as it would one installed a moment later.
    pub fn take_ids(&self, ids: &std::collections::HashSet<GrantId>) -> Vec<Grant> {
        if ids.is_empty() {
            return Vec::new();
        }
        let mut g = self.lock();
        let taken: Vec<Grant> = g
            .grants
            .values()
            .filter(|e| ids.contains(&e.id))
            .copied()
            .collect();
        for t in &taken {
            g.grants.remove(&t.id);
        }
        g.supersede_where(|e| ids.contains(&e.id));
        taken
    }

    /// [`Self::take_where`] for a predicate that is not store-free or not
    /// cheap (it reads the store: `resolve_ownership`): the predicate runs
    /// *outside* the table's mutex, once per inode of a snapshot
    /// ([`Self::snapshot_inos`]), and [`Self::take_ids`] takes what it
    /// chose. Nothing of the table is blocked for as long as the store
    /// takes (plan 41 L1b; on a stalled device an uncached read blocked
    /// every FUSE fence check and every renewal of the lane behind the
    /// mutex).
    pub fn take_where_outside(&self, under: impl Fn(u64) -> bool) -> Vec<Grant> {
        let snapshot = self.snapshot_inos();
        let mut verdict: std::collections::HashMap<u64, bool> = std::collections::HashMap::new();
        let ids: std::collections::HashSet<GrantId> = snapshot
            .into_iter()
            .filter(|(_, ino)| *verdict.entry(*ino).or_insert_with(|| under(*ino)))
            .map(|(id, _)| id)
            .collect();
        self.take_ids(&ids)
    }

    /// Remove and return the grants whose inode satisfies `under`.
    ///
    /// `under` runs under the table's mutex: it must be store-free and
    /// cheap. A predicate that reads the store goes through
    /// [`Self::take_where_outside`].
    pub fn take_where(&self, under: impl Fn(u64) -> bool) -> Vec<Grant> {
        let mut g = self.lock();
        let taken: Vec<Grant> = g
            .grants
            .values()
            .filter(|e| under(e.ino))
            .copied()
            .collect();
        for t in &taken {
            g.grants.remove(&t.id);
        }
        g.supersede_where(|e| under(e.ino));
        taken
    }

    /// The records expired and dropped since the last call, wherever the
    /// drop happened (a conflicting request, a token check): grants their
    /// holders never released here. What such a holder was acknowledged
    /// under the grant is in no release position, so the sequencer must
    /// learn it another way before it grants the inode again
    /// (`core::locks`, the outwait barrier).
    pub fn take_outwaited(&self) -> Vec<Grant> {
        std::mem::take(&mut self.lock().outwaited)
    }

    /// Whether expired records were dropped since the last call (the
    /// sequencer's mirror must be sent again).
    pub fn take_expired_dropped(&self) -> bool {
        self.expired_dropped.load(Ordering::Relaxed)
            && self.expired_dropped.swap(false, Ordering::Relaxed)
    }

    pub fn clear_grants(&self) -> usize {
        let mut g = self.lock();
        let n = g.grants.len();
        g.grants.clear();
        g.lapsed.clear();
        g.lapsed_order.clear();
        n
    }

    pub fn grants_len(&self) -> usize {
        self.lock().grants.len()
    }

    /// Grants still live at `now` (`grants_len` counts the expired ones
    /// too, until a `conflicting` call purges them).
    pub fn live_grants_len(&self, now: WaitMs) -> usize {
        self.lock()
            .grants
            .values()
            .filter(|g| g.until > now)
            .count()
    }

    /// Whether this owner's table has a grant made under generation `gen`.
    pub fn has_grants_of_gen(&self, gen: u64) -> bool {
        self.lock().grants.values().any(|g| g.gen == gen)
    }

    // ------------------------------------------------------ node: grants

    pub fn held(&self, ino: u64) -> Option<HeldGrant> {
        let got = self.lock().held.get(&ino).copied();
        #[cfg(any(test, feature = "test-util"))]
        self.gap("held");
        got
    }

    pub fn held_all(&self) -> Vec<(u64, HeldGrant)> {
        self.lock().held.iter().map(|(i, h)| (*i, *h)).collect()
    }

    pub fn held_count(&self) -> usize {
        self.lock().held.len()
    }

    /// A recall arrived for a grant on `ino` this node does not hold:
    /// remember it for the reply that brings it.
    pub fn note_pending_recall(&self, ino: u64, id: GrantId) {
        let mut g = self.lock();
        let v = g.pending_recalls.entry(ino).or_default();
        if !v.contains(&id) {
            v.push(id);
        }
    }

    /// Install a grant this node was given. The same grant (by id: the
    /// owner re-affirmed it) merges — the mode never weakens under local
    /// locks, the recalled flag stays; an id this node already released
    /// is refused.
    pub fn install_held(&self, ino: u64, mut held: HeldGrant) -> Installed {
        let mut g = self.lock();
        if g.released.contains(&held.id) {
            return Installed::Released;
        }
        let key = (ino, held.id.node);
        if g.newest.get(&key).is_some_and(|seq| *seq > held.id.seq) {
            g.tombstone(held.id);
            return Installed::Released;
        }
        g.newest.insert(key, held.id.seq);
        while g.newest.len() > NEWEST_KEPT {
            g.newest.pop_first();
        }
        g.released_last.remove(&ino);
        if g.pending_recalls
            .remove(&ino)
            .is_some_and(|ids| ids.contains(&held.id))
        {
            held.recalled = true;
        }
        // Local locks under no honoured grant (it lapsed, or was dropped,
        // before this one came): their owners stay fenced until their
        // locks are gone, whatever this grant covers.
        //
        // Plan 43 C7-F F-c: not for the very grant this node holds lapsing,
        // which the owner re-affirms (a push of the same id): it resumes
        // below if the window is open again. A *different* id replacing a
        // lapsing grant (a fresh grant made after the old one's record
        // ended, or a re-affirmation under a new id: this node cannot tell
        // them apart) is a refusal, as a lapse always was: captured,
        // tainted, from the end of the old window.
        let lapsing_old = g.held.get(&ino).copied().filter(|o| o.lapsing.is_some());
        match lapsing_old {
            Some(o) if o.id == held.id => {}
            Some(o) => {
                g.taint(ino);
                g.capture(ino, o.until);
                g.stats.lapse_parks_refused += 1;
            }
            None if g.fenced_at(ino, held.installed) => g.capture(ino, held.installed),
            None => {}
        }
        // Every grant gets the local lock it was asked for before a
        // recall can release it (sim seed 94033: an owner's own fresh
        // grant recalled and released in the same event, before its
        // FUSE thread ran). A lapsed unused grant is dropped by the
        // renewal tick.
        held.first_use = true;
        let mut continued = false;
        if let Some(old) = g.held.get(&ino).copied().filter(|o| o.id == held.id) {
            held.mode = old.mode.max(held.mode);
            held.until = old.until.max(held.until);
            held.renew_at = old.renew_at.max(held.renew_at);
            held.recalled |= old.recalled;
            held.releasing = old.releasing;
            held.idle_since = old.idle_since;
            held.written = old.written;
            held.lapsing = old.lapsing;
        } else if let Some(old) = g.held.get(&ino).copied().filter(|o| {
            o.id.node == held.id.node
                && o.id.seq < held.id.seq
                && o.owner == held.owner
                && held.mode.covers(o.mode)
        }) {
            // The same owner re-affirmed the grant under a newer id (a
            // request of this node it answered after pushing the old
            // one; an owner never weakens a live grant, so a weaker mode
            // is a fresh grant after a lapse, replaced below): the local
            // locks, the recalled flag and the release in flight
            // continue under the new id — it is the id the owner will
            // recall and expects released. Installed as a fresh, unused
            // grant it would sit `first_use` until a local lock came, and
            // its release would name an id the owner no longer had (the
            // grant was outwaited then).
            continued = true;
            held.until = old.until.max(held.until);
            held.recalled |= old.recalled;
            held.releasing = old.releasing;
            held.first_use = old.first_use;
            held.idle_since = old.idle_since;
            held.installed = old.installed;
            held.written = old.written && old.lapsing.is_none();
        }
        // Plan 43 C7-F F-c: the owner re-affirmed a lapsing grant (a push)
        // and its window is open again: it resumes.
        if held.lapsing.is_some() && held.until > held.installed {
            held.lapsing = None;
            g.stats.lapse_parks_resumed += 1;
        }
        let recalled = held.recalled;
        self.next_end.fetch_min(held.until.0, Ordering::Relaxed);
        g.last_held.insert(ino, held.id);
        while g.last_held.len() > NEWEST_KEPT {
            g.last_held.pop_first();
        }
        if let Some(old) = g.held.insert(ino, held) {
            if old.id != held.id {
                // Replaced: the old id is dead here. Either the same grant
                // under the owner's newer id (the continuing branch above:
                // a re-affirmation, an upgrade made in place), or a fresh
                // grant (after a lapse, from another owner), which ops
                // tagged with the old id do not hold back: its owner
                // outwaited the old record before minting it.
                g.tombstone(old.id);
                // The same grant under the owner's newer id (the branch
                // above): what is tagged with the old id holds back the
                // new one's release (sim `locks-writer-hold-delegated-doubt`
                // seeds 279, 344 and 1324 under slow lease reads: a write
                // in doubt under the old id, the new id released, and the
                // write landed inside the next holder's section).
                if continued {
                    g.note_superseded(ino, old.id, held.id);
                }
            }
        }
        g.stats.granted += 1;
        self.track(&g);
        Installed::Ok { recalled }
    }

    /// The release's flush completed: drop the grant — unless a local
    /// lock (or the grant's first use) appeared meanwhile, in which case
    /// it stays (recalled; the last unlock releases it again). `true`:
    /// dropped.
    pub fn end_release(&self, ino: u64, id: GrantId) -> bool {
        self.end_held(ino, id, true)
    }

    /// As [`Self::end_release`], for a grant this node gives up *without*
    /// a release (left to its owner's outwait: an op tagged with it may
    /// still land, `core::locks`'s doubt lapse). Tombstoned as a released
    /// one is — a reply or push of the id that crosses the drop must not
    /// reinstall what this node gave up — but not recorded as released
    /// last: that vouches nothing ran under the grant's successor here,
    /// and the owner will outwait this one, not see a release.
    pub fn drop_unreleased(&self, ino: u64, id: GrantId) -> bool {
        self.end_held(ino, id, false)
    }

    fn end_held(&self, ino: u64, id: GrantId, released: bool) -> bool {
        let mut g = self.lock();
        let pinned = g.local.get(&ino).is_some_and(|v| !v.is_empty());
        match g.held.get_mut(&ino) {
            Some(h) if h.id == id => {
                if pinned || h.first_use {
                    h.releasing = false;
                    return false;
                }
                g.held.remove(&ino);
                g.tombstone(id);
                if released {
                    g.released_last.insert(ino, id);
                    while g.released_last.len() > NEWEST_KEPT {
                        g.released_last.pop_first();
                    }
                } else {
                    g.released_last.remove(&ino);
                }
                self.track(&g);
                true
            }
            _ => false,
        }
    }

    /// The grant on `ino` this node released last (its flush done, no
    /// local lock under it), if it has held none there since: every
    /// other way a held grant ends (a lapse, a loss) follows an install,
    /// which clears this. `None` also when the entry was evicted (past
    /// `NEWEST_KEPT` inodes): what it vouches for is then unknown.
    pub fn released_last(&self, ino: u64) -> Option<GrantId> {
        self.lock().released_last.get(&ino).copied()
    }

    /// This node answered the recall of `id`, which it does not hold, with
    /// a release: refuse it from now on. A push of it may still be in
    /// flight, and a push installs for any later op on the inode.
    pub fn tombstone_unheld(&self, id: GrantId) {
        self.lock().tombstone(id);
    }

    /// The sequencer recalled `id` on `ino`: `Some(busy)` when this node
    /// holds it (`busy`: local locks under it), `None` when it does not.
    pub fn recall_held(&self, ino: u64, id: GrantId) -> Option<bool> {
        let mut g = self.lock();
        // The owner's newest id supersedes an older one of its own this
        // node still holds (it re-affirmed the grant under a new id for
        // a request that crossed the old id's push): the recall is for
        // this grant, which adopts the id — its release then names the
        // id the owner has. Unrecalled, the cached grant kept serving
        // local locks while the owner's waiters sat until it lapsed
        // (EC2 campaign 8: a turn taken under such a grant while the
        // other committer's request was parked).
        let h = g
            .held
            .get_mut(&ino)
            .filter(|h| h.id == id || (h.id.node == id.node && h.id.seq < id.seq))?;
        let old = h.id;
        h.id = id;
        h.recalled = true;
        if old != id {
            // A renewal of the old id still out answers for a dead id:
            // `renewed` and `renewal_failed` match it no more, and left
            // `renewing`, the grant was never due again — a recalled grant
            // still pinned by a local lock lapsed under it unless a late
            // reply happened to extend it. Due again at its renewal point
            // (an answer naming the adopted id still extends it; the
            // duplicate renewal is harmless).
            h.renewing = None;
            g.tombstone(old);
            g.note_superseded(ino, old, id);
            g.stats.recalled_superseded += 1;
            // The owner keeps one record per node and inode and mints a
            // fresh id per answer, so every id of its below the adopted
            // one is dead there. A reply carrying one that is still in
            // flight (the recall overtook it) must not install once the
            // adopted id is released: it was a second exclusive holder
            // beside the owner's next grantee (sim `locks-long-grants`
            // seed 32392 with the ask-again patch; reachable without it
            // whenever a recall overtakes the reply of an older in-place
            // grant).
            let n = g.newest.entry((ino, id.node)).or_insert(id.seq);
            if *n < id.seq {
                *n = id.seq;
            }
        }
        let busy = g.local.get(&ino).is_some_and(|v| !v.is_empty());
        g.stats.recalled += 1;
        if busy {
            g.stats.recalled_busy += 1;
        }
        Some(busy)
    }

    /// Whether `ino`'s grant is recalled and free of local locks (ready
    /// to release), marking the release in flight.
    pub fn begin_release(&self, ino: u64) -> Option<HeldGrant> {
        let mut g = self.lock();
        let busy = g.local.get(&ino).is_some_and(|v| !v.is_empty());
        let h = g.held.get_mut(&ino)?;
        if !h.recalled || busy || h.releasing || h.first_use || h.lapsing.is_some() {
            return None;
        }
        h.releasing = true;
        Some(*h)
    }

    /// The grant is gone without a release (it lapsed): what was written
    /// under it and not flushed must not be published (tainted).
    pub fn drop_held(&self, ino: u64, id: GrantId) -> bool {
        let mut g = self.lock();
        match g.held.get(&ino).copied() {
            Some(h) if h.id == id => {
                g.held.remove(&ino);
                g.tombstone(id);
                g.taint(ino);
                g.capture(ino, h.until);
                self.track(&g);
                true
            }
            _ => false,
        }
    }

    /// [`Self::drop_held`] for a window that is over *now*: one critical
    /// section, `now` read inside it (this table's clock, `fallback` while
    /// none is set). `true`: `id` is not held on `ino` any more (dropped
    /// here, or already gone or replaced); `false`: it is held still — its
    /// window was extended since the caller looked (a second writer's
    /// renewal, plan 41), or it is *lapsing* (parked, resolved by the renewal
    /// tick, [`Self::lapse_tick`]) — and nothing was changed: the caller
    /// looks again later. Dropping by id after reading the window (what the
    /// core did) dropped a window renewed in between.
    pub fn drop_held_if_lapsed(&self, ino: u64, id: GrantId, fallback: Now) -> bool {
        #[cfg(any(test, feature = "test-util"))]
        self.gap("drop_held_if_lapsed");
        let mut g = self.lock();
        let now = g.now_or(fallback);
        match g.held.get(&ino).copied() {
            Some(h) if h.id == id => {
                if h.until > now.honour || h.lapsing.is_some() {
                    return false;
                }
                g.held.remove(&ino);
                g.tombstone(id);
                g.taint(ino);
                g.capture(ino, h.until);
                self.track(&g);
                true
            }
            _ => true,
        }
    }

    pub fn drop_held_any(&self, ino: u64) -> Option<HeldGrant> {
        let mut g = self.lock();
        let h = g.held.remove(&ino);
        if let Some(h) = h {
            g.tombstone(h.id);
            g.taint(ino);
            g.capture(ino, h.until);
        }
        self.track(&g);
        h
    }

    /// Whether `h` is kept alive by renewals. A recalled grant is renewed
    /// only while something pins it — local locks under it, or the one
    /// local lock it was granted for and has not served yet
    /// (`first_use`): its release is what the owner waits for, and it
    /// cannot be released before that lock has come and gone. The FUSE
    /// thread takes that lock only once the grant's floor is reached (up
    /// to the session budget) and the kernel's cache dropped: unrenewed
    /// meanwhile, the grant was renewed only from that lock on, past its
    /// renewal point, and lapsed under the application's `flock` when
    /// the late renewal's answer did not come back in what was left of
    /// the window (`git-under-flock-b2b`: two committers in the turn at
    /// once).
    ///
    /// The first use is waited for only as long as it can take
    /// ([`Self::first_use_budget_ms`] from the install): a requester that
    /// gave up without taking the lock (its answer lost, a non-blocking
    /// request that lost the race) would otherwise keep the recalled grant
    /// renewed for ever, and the waiters behind it waiting for ever.
    fn renewable(h: &HeldGrant, pinned: bool, now: HonourMs, budget_ms: i64) -> bool {
        !h.recalled || pinned || (h.first_use && now < h.installed.plus(budget_ms.max(0) as u64))
    }

    /// The requester a grant was installed for gave up without taking its
    /// local lock: the grant no longer waits for that first use (a recall
    /// then releases it), and, free of local locks, it is idle from
    /// `now` (`CONSTELLATION_LOCK_CACHE_IDLE_MS` reclaims it — unused, it
    /// would otherwise be renewed until a recall came). `true` if the
    /// grant is recalled and free of local locks — the caller lets the
    /// core release it.
    pub fn abandon_first_use(&self, ino: u64, now: Now) -> bool {
        let mut g = self.lock();
        let idle = g.local.get(&ino).is_none_or(|v| v.is_empty());
        let Some(h) = g.held.get_mut(&ino).filter(|h| h.first_use) else {
            return false;
        };
        h.first_use = false;
        if idle && h.idle_since.is_none() {
            // Idle time is a liveness stamp (the cache's idle release): it
            // moves to the wait clock with the other housekeeping.
            h.idle_since = Some(now.wait);
        }
        let recalled = h.recalled;
        g.stats.first_use_abandoned += 1;
        recalled && idle
    }

    /// Recalled grants whose first local lock never came within the
    /// first-use budget: no longer pinned (the renewal tick releases
    /// them). Returns their inodes.
    pub fn expire_first_use(&self, now: Now) -> Vec<u64> {
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let Inner {
            held, local, stats, ..
        } = &mut *g;
        let mut out = Vec::new();
        for (ino, h) in held.iter_mut() {
            if h.recalled
                && h.first_use
                && now.honour >= h.installed.plus(budget.max(0) as u64)
                && local.get(ino).is_none_or(|v| v.is_empty())
            {
                h.first_use = false;
                stats.first_use_abandoned += 1;
                out.push(*ino);
            }
        }
        out
    }

    /// Grants to renew: honoured, past `renew_at`, no renewal in flight,
    /// [`Self::renewable`]. Marks them renewing at `now` (honour clock).
    pub fn due_renewals(&self, now: Now) -> Vec<(u64, HeldGrant)> {
        let now = now.honour;
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let mut due = Vec::new();
        let Inner { held, local, .. } = &mut *g;
        for (ino, h) in held.iter_mut() {
            let pinned = local.get(ino).is_some_and(|v| !v.is_empty());
            if h.until > now
                && now >= h.renew_at
                && h.renewing.is_none()
                && Self::renewable(h, pinned, now, budget)
            {
                h.renewing = Some(now);
                due.push((*ino, *h));
            }
        }
        due
    }

    /// [`Self::due_renewals`] for the lock lane (plan 41): a `renewing` mark
    /// older than `stale_ms` (honour clock) counts as none — the renewal it
    /// stood for is lost or still stuck behind a stalled core, and the lane
    /// asks again — and a lapsing grant is never returned (a late renewal
    /// is an *add*, the core's).
    ///
    /// `core_stuck`: the authority core is stuck inside a step. A mark that
    /// is not the lane's own (`lane_marked`) was then set by that step,
    /// which began renewing the grant and cannot finish (a lone node's own
    /// grant is marked by the core's renewal tick a moment before its
    /// commit stalls): it stands for nothing the lane should wait on, and
    /// the lane renews at once. A renewal done twice is harmless, every
    /// extension being a max; one waited for `stale_ms` is the lapse.
    pub fn due_renewals_lane(
        &self,
        now: Now,
        stale_ms: i64,
        core_stuck: bool,
        lane_marked: impl Fn(u64, GrantId) -> bool,
    ) -> Vec<(u64, HeldGrant)> {
        let now = now.honour;
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let mut due = Vec::new();
        let Inner { held, local, .. } = &mut *g;
        for (ino, h) in held.iter_mut() {
            let pinned = local.get(ino).is_some_and(|v| !v.is_empty());
            let marked = h.renewing.is_some_and(|t| now.since(t) < stale_ms)
                && !(core_stuck && !lane_marked(*ino, h.id));
            if h.until > now
                && now >= h.renew_at
                && !marked
                && h.lapsing.is_none()
                && Self::renewable(h, pinned, now, budget)
            {
                h.renewing = Some(now);
                due.push((*ino, *h));
            }
        }
        due
    }

    /// The earliest renewal point among the grants [`Self::due_renewals`]
    /// would renew (honoured, no renewal in flight, renewable); `None`
    /// when there is none. The renewal tick is armed from this alone: a
    /// grant that is not renewed (released, or recalled with nothing
    /// pinning it) has a renewal point in the past, and arming from it
    /// re-armed the tick every millisecond for as long as it stayed.
    pub fn next_renewal(&self, now: Now) -> Option<HonourMs> {
        let now = now.honour;
        let budget = self.first_use_budget_ms();
        let g = self.lock();
        g.held
            .iter()
            .filter(|(ino, h)| {
                let pinned = g.local.get(ino).is_some_and(|v| !v.is_empty());
                h.until > now && h.renewing.is_none() && Self::renewable(h, pinned, now, budget)
            })
            .map(|(_, h)| h.renew_at)
            .min()
    }

    /// A renewal of `id` sent at `sent` (honour clock) was answered: the owner holds
    /// `now_id` (the same, or a newer grant of this node whose reply was
    /// lost — adopted here, with its mode) in `mode`. Applied too when
    /// the grant held here already has `now_id`: a recall of the newer id
    /// moved it there while the renewal was out (`recall_held`), and the
    /// answer is that grant's extension. Dropped, the grant kept the
    /// window of the push that installed it, which a waiter's push sets
    /// from its first request (sim `locks-blips-tight` seeds 568, 1713,
    /// 1887, 2762: lapsed under I/O 350 ms after its install, the
    /// renewal of the old id answered `Ok` 10 ms after it). That answer
    /// also clears `renewing` when a renewal of `now_id` itself is out:
    /// harmless, the grant is at worst renewed once more (a duplicate
    /// renewal, whose answer extends it again).
    #[allow(clippy::too_many_arguments)]
    pub fn renewed(
        &self,
        ino: u64,
        id: GrantId,
        now_id: GrantId,
        mode: LockMode,
        sent: HonourMs,
        ttl_ms: i64,
        margin_ms: i64,
        recalled: bool,
        from: u64,
        now: Now,
    ) {
        let mut g = self.lock();
        if let Some(h) = g
            .held
            .get_mut(&ino)
            .filter(|h| h.id == id || h.id == now_id)
        {
            h.renewing = None;
            h.route = Some(from);
            h.until = h.until.max(HonourMs(sent.0 + ttl_ms - margin_ms));
            h.renew_at = h.renew_at.max(renew_point(sent, ttl_ms, margin_ms));
            h.recalled |= recalled;
            // Plan 43 C7-F F-c: a renewal (a late one, or a normal one that
            // was in flight at the lapse) that makes the grant honoured
            // again resumes it: nothing was lost.
            let resumed = match h.lapsing {
                Some(l) if h.until > now.honour => {
                    h.lapsing = None;
                    Some(l)
                }
                _ => None,
            };
            // The owner's record names the mode: a grant upgraded in place
            // under this very id (its answer lost or arrived lapsed, the id
            // adopted by a recall onto the weaker grant) is the stronger one
            // here too. Held weaker, the node and its owner disagreed on the
            // mode of one grant for as long as it was renewed, and an owner
            // converting its read lock waited for a grant it had (sim
            // `locks-upgrade` seed 719).
            h.mode = h.mode.max(mode);
            if h.id != now_id {
                // The owner's newer id of this grant (its answer to a
                // request of this node lost, or not installed): the third
                // way a held grant goes on under a newer id, and its ops
                // tagged with the old one hold the new one's release too.
                let old = h.id;
                h.id = now_id;
                g.tombstone(old);
                g.note_superseded(ino, old, now_id);
            }
            g.stats.renewals += 1;
            if let Some(l) = resumed {
                g.stats.lapse_parks_resumed += 1;
                g.stats.lapse_park_ms_max = g
                    .stats
                    .lapse_park_ms_max
                    .max(now.wait.0.saturating_sub(l.at.0).max(0) as u64);
            }
            self.track(&g);
        }
    }

    /// Renew this node's own grant `id` on `ino` (this node, `me`, is the
    /// root's owner and the holder both): the owner record and the held
    /// window in **one** critical section, with `now` read inside it (this
    /// table's clock, `fallback` while none is set). The lock lane's
    /// primitive (plan 41): a second writer to both, which never ends or
    /// shortens anything and never adds a record.
    ///
    /// Renews only a grant the core itself would renew now: the owner
    /// record is `me`'s, of the root (`gen == 0`), live on the wait clock,
    /// and its mode covers the held grant's (a weaker record is not the
    /// grant, `Core::lock_renew_one`); the held grant has this id, is
    /// honoured, [`Self::renewable`], not lapsing, not being released.
    /// `ttl = min(lock_ttl_ms, ttl_cap(now))` must be positive. The caller's
    /// `ttl_cap` runs under the table's mutex: it reads a mirrored fact and
    /// nothing slow. Then the owner record ends at `max(old, now.wait + ttl +
    /// margin)` ([`Self::extend`]) and the held window as
    /// [`Self::renewed`] extends it for a renewal sent at `now.honour` of
    /// this node's clock `clock` (its id, which confirms an installed copy
    /// from that send: [`Grant::confirmed`]).
    /// `true`: renewed; `false`: nothing changed.
    #[allow(clippy::too_many_arguments)]
    pub fn renew_own(
        &self,
        ino: u64,
        id: GrantId,
        me: u64,
        ttl_cap: impl FnOnce(Now) -> i64,
        lock_ttl_ms: i64,
        margin_ms: i64,
        fallback: Now,
        clock: u64,
    ) -> bool {
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let now = g.now_or(fallback);
        let pinned = g.local.get(&ino).is_some_and(|v| !v.is_empty());
        let Some(h) = g.held.get(&ino).copied().filter(|h| h.id == id) else {
            return false;
        };
        if h.until <= now.honour
            || h.releasing
            || h.lapsing.is_some()
            || !Self::renewable(&h, pinned, now.honour, budget)
        {
            return false;
        }
        let ok = g
            .grants
            .get(&id)
            .is_some_and(|e| e.node == me && e.gen == 0 && e.until > now.wait && e.mode >= h.mode);
        if !ok {
            return false;
        }
        let ttl = lock_ttl_ms.min(ttl_cap(now));
        if ttl <= 0 {
            return false;
        }
        let sent = now.honour;
        let end = now.wait.plus((ttl + margin_ms).max(0) as u64);
        let mut recalled = false;
        if let Some(e) = g.grants.get_mut(&id) {
            e.until = e.until.max(end);
            // Confirmed from this renewal's send on this node's own clock
            // (`clock`), as a peer's renewal confirms from its holder's
            // (plan 43 C7b, row 29).
            e.confirm(clock, sent.0);
            recalled = e.recalled;
        }
        if let Some(h) = g.held.get_mut(&ino) {
            h.renewing = None;
            h.route = Some(me);
            h.until = h.until.max(HonourMs(sent.0 + ttl - margin_ms));
            h.renew_at = h.renew_at.max(renew_point(sent, ttl, margin_ms));
            h.recalled |= recalled;
        }
        g.stats.renewals += 1;
        self.track(&g);
        true
    }

    /// `owner` answered a renewal of `id` on `ino` `NotOwner`: it is no
    /// longer where renewals go ([`HeldGrant::route`]).
    pub fn clear_route(&self, ino: u64, id: GrantId, owner: u64) {
        let mut g = self.lock();
        if let Some(h) = g
            .held
            .get_mut(&ino)
            .filter(|h| h.id == id && h.route == Some(owner))
        {
            h.route = None;
        }
    }

    /// A renewal was not answered, or answered "not the owner": try
    /// again at the next tick.
    pub fn renewal_failed(&self, ino: u64, id: GrantId) {
        let mut g = self.lock();
        // (A renewal of an id the owner has since replaced by a newer one of
        // its own, which a recall made this node adopt, fails for the grant
        // held now: its late renewal, if lapsing, is sent again.)
        if let Some(h) = g
            .held
            .get_mut(&ino)
            .filter(|h| h.id == id || (h.id.node == id.node && h.id.seq > id.seq))
        {
            h.renewing = None;
        }
    }

    /// The owner does not know the grant: it is lost.
    pub fn lost(&self, ino: u64, id: GrantId, now: Now) -> bool {
        let mut g = self.lock();
        let gone = matches!(g.held.get(&ino), Some(h) if h.id == id);
        if gone {
            let held = g.held.remove(&ino);
            let since = held.map_or(HonourMs(0), |h| h.until);
            g.tombstone(id);
            g.taint(ino);
            g.capture(ino, since);
            g.stats.lost += 1;
            if let Some(l) = held.and_then(|h| h.lapsing) {
                g.stats.lapse_parks_refused += 1;
                g.stats.lapse_park_ms_max = g
                    .stats
                    .lapse_park_ms_max
                    .max(now.wait.0.saturating_sub(l.at.0).max(0) as u64);
            }
            self.track(&g);
        }
        gone
    }

    /// Plan 43 C7-F F-c, the renewal tick's look at the lapsed grants: each
    /// grant whose window ended is dropped as before (nothing pins it) or
    /// becomes lapsing; a lapsing one past its deadline gives up; the
    /// others that have no renewal in flight are returned, marked renewing
    /// at `now`, for a **late** renewal to the owner. `gave_up`: how many
    /// gave up now.
    ///
    /// One critical section (plan 41 L1): each window is read inside it and
    /// ended or marked in it, so a second writer's extension (the lock lane)
    /// lands before the tick looks, and the grant is honoured, or after the
    /// tick has dropped or marked it, and the lane refuses a grant that is
    /// not honoured ([`Self::renew_own`]). `now` is the caller's reading,
    /// older than the table's when the step waited: that only makes fewer
    /// windows look over, never more.
    pub fn lapse_tick(&self, now: Now) -> LapseTick {
        // The estimate's offset follows the clocks at every tick: `honour −
        // wait` grows with a suspend, a forward step and the oscillator's
        // drift against the wall, and the park gates' full reading happens
        // only near a grant's end (review of F-c).
        self.honour_offset
            .store(now.honour.0 - now.wait.0, Ordering::Relaxed);
        let mut g = self.lock();
        let mut out = LapseTick::default();
        let lapsed: Vec<u64> = g
            .held
            .iter()
            .filter(|(_, h)| h.until <= now.honour)
            .map(|(i, _)| *i)
            .collect();
        for ino in lapsed {
            let before = g.held.get(&ino).copied();
            match g.observe(ino, now, self.park_ms()) {
                Observed::Lapsing => {
                    if let Some(h) = g.held.get_mut(&ino) {
                        if h.renewing.is_none() {
                            h.renewing = Some(now.honour);
                            out.late.push((ino, *h));
                        }
                    }
                }
                Observed::Gone => {
                    if before.is_some_and(|h| h.lapsing.is_some()) {
                        out.gave_up += 1;
                    } else if let Some(h) = g.held.remove(&ino) {
                        // As the tick always dropped a lapsed grant.
                        g.tombstone(h.id);
                        g.taint(ino);
                        g.capture(ino, h.until);
                    }
                }
                Observed::Honoured => {}
            }
        }
        self.track(&g);
        out
    }

    /// The honoured grant on `ino`, if any.
    pub fn honoured(&self, ino: u64, now: Now) -> Option<HeldGrant> {
        self.lock()
            .held
            .get(&ino)
            .copied()
            .filter(|h| h.until > now.honour)
    }

    /// Whether I/O on `ino` must be refused: this node holds local locks
    /// on it and no honoured grant. One atomic load when no local lock
    /// exists anywhere.
    ///
    /// Plan 43 C7-F F-c: [`Fence::Parked`] while the grant the local locks
    /// are under is lapsing (nothing is captured then: the caller waits for
    /// the owner's answer and asks again).
    pub fn fenced(&self, ino: u64, now: Now) -> Fence {
        if self.local_inos.load(Ordering::Relaxed) == 0 {
            return Fence::Ok;
        }
        let mut g = self.lock();
        if !g.fenced_at(ino, now.honour) {
            return Fence::Ok;
        }
        let obs = g.observe(ino, now, self.park_ms());
        self.track(&g);
        if obs == Observed::Lapsing {
            return Fence::Parked;
        }
        g.stats.fenced_io += 1;
        g.capture(ino, now.honour);
        self.refence(&g);
        Fence::Fenced
    }

    /// Whether any lock owner on this node may be fenced at `now`
    /// ([`Self::fenced_owners`] has something to say). One relaxed load
    /// with no local lock anywhere; two while every local lock is under
    /// a grant honoured until later.
    pub fn owner_fence_armed(&self, now: Now) -> bool {
        self.local_inos.load(Ordering::Relaxed) != 0
            && now.honour.0 >= self.fence_from_ms.load(Ordering::Relaxed)
    }

    /// The lock owners fenced on this node at `now`: every owner of a
    /// local lock whose grant lapsed, was lost, or was replaced after a
    /// lapse — captured once and kept until that owner has no local lock
    /// left (unlock or close). The caller refuses every op such an owner
    /// (or its process) issues, on any file (`EIO`).
    pub fn fenced_owners(&self, now: Now) -> Vec<FencedOwner> {
        if !self.owner_fence_armed(now) {
            return Vec::new();
        }
        let mut g = self.lock();
        // Only while an owner not fenced yet may have lapsed: an owner
        // that keeps its lock after the lapse costs the others' ops the
        // table lock, not a walk of every local lock.
        if now.honour.0 >= self.lapse_from_ms.load(Ordering::Relaxed) {
            g.capture_lapsed(now, self.park_ms());
            self.track(&g);
        }
        g.fenced_owners.values().copied().collect()
    }

    /// Plan 43 C7-F F-c: the lock owners (not fenced) whose local locks are
    /// under a lapsing grant — ops of theirs the owner fence would refuse
    /// wait instead. Empty with one relaxed load while no grant is lapsing.
    pub fn parked_owners(&self, now: Now) -> Vec<FencedOwner> {
        if !self.lapsing_any() {
            return Vec::new();
        }
        let mut g = self.lock();
        // A lapsing grant past its deadline gives up here (and its owners
        // are captured: they are fenced from now on).
        let inos: Vec<u64> = g
            .held
            .iter()
            .filter(|(_, h)| h.lapsing.is_some())
            .map(|(i, _)| *i)
            .collect();
        let mut out = Vec::new();
        for ino in inos {
            if g.observe(ino, now, self.park_ms()) != Observed::Lapsing {
                continue;
            }
            let since = g.held.get(&ino).map_or(now.honour, |h| h.until);
            let grant = g.held.get(&ino).map(|h| h.id);
            if let Some(v) = g.local.get(&ino) {
                for l in v {
                    if !g.fenced_owners.contains_key(&l.owner) {
                        out.push(FencedOwner {
                            owner: l.owner,
                            pid: l.pid,
                            pid_start: l.pid_start,
                            since,
                            grant,
                        });
                    }
                }
            }
        }
        self.track(&g);
        out
    }

    /// Whether nothing is held, locked or tainted on this node (two
    /// relaxed loads): [`Self::take_discard`] has nothing to say, and the
    /// caller reads no clock for it.
    pub fn nothing_to_discard(&self) -> bool {
        self.local_inos.load(Ordering::Relaxed) == 0 && self.tracked.load(Ordering::Relaxed) == 0
    }

    /// Whether no local lock exists on this node (one relaxed load: the
    /// fences' and the fencing token's fast path).
    pub fn lockers_none(&self) -> bool {
        self.local_inos.load(Ordering::Relaxed) == 0
    }

    /// Every local lock's owner and the process that took it (the
    /// callers a mutation is tagged for, matched by the FUSE side).
    /// Empty, with one relaxed load, while no local lock exists.
    pub fn lockers(&self) -> Vec<(u64, u32, u64)> {
        if self.local_inos.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let g = self.lock();
        let mut out: Vec<(u64, u32, u64)> = g
            .local
            .values()
            .flatten()
            .map(|l| (l.owner, l.pid, l.pid_start))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Plan 30 §M14 phase 2: the tag a mutation of the lock owners
    /// `owners` carries — per minting sequencer, the earliest honoured
    /// window among the grants their local locks are under (see
    /// [`LockTag`]). `Err` when one of their locks is under no honoured,
    /// covering grant: the owner is fenced (its op is refused, `EIO`).
    /// Empty when they hold no local lock.
    pub fn owner_tag(&self, owners: &[u64], now: Now, clock: u64) -> Result<LockTag, OwnerFenced> {
        if owners.is_empty() {
            return Ok(LockTag::NONE);
        }
        let g = self.lock();
        let mut per_minter: BTreeMap<u64, LockToken> = BTreeMap::new();
        for (ino, v) in &g.local {
            for l in v.iter().filter(|l| owners.contains(&l.owner)) {
                let h = g
                    .held
                    .get(ino)
                    .filter(|h| h.until > now.honour && h.mode.covers(l.mode()))
                    .ok_or(OwnerFenced)?;
                // Minted from the honour window, now (plan 43 C7-H1, R8; C7b:
                // the wall stamp, the honour end with this node's clock id
                // and the time left).
                let t = LockToken::minted(h.id, h.until, now, clock);
                let e = per_minter.entry(h.id.node).or_insert(t);
                if t.until_ms < e.until_ms {
                    *e = t;
                }
            }
        }
        Ok(LockTag(per_minter.into_values().collect()))
    }

    /// A mutation tagged with `tag` is in flight from this node: none of
    /// its grants is released before [`Self::tag_end`].
    pub fn tag_begin(&self, tag: &LockTag, now: Now) {
        if tag.is_empty() {
            return;
        }
        let mut g = self.lock();
        // An entry pinned by an op left in doubt goes once its pin is
        // over, whether or not a release ever asked about its grant (one
        // that lapsed instead stayed for ever).
        g.inflight.retain(|_, e| !e.over(now));
        g.stats.tagged_ops += 1;
        for t in &tag.0 {
            g.inflight.entry(t.grant).or_default().active += 1;
        }
    }

    /// The mutation [`Self::tag_begin`] announced was answered. `in_doubt`:
    /// it may still execute somewhere (a forward neither answered nor
    /// refused) — until its tokens' windows are over, after which every
    /// executor rejects it; the grants are not released before then.
    pub fn tag_end(&self, tag: &LockTag, in_doubt: bool, now: Now) {
        if tag.is_empty() {
            return;
        }
        let mut wake = Vec::new();
        let mut g = self.lock();
        for t in &tag.0 {
            if in_doubt {
                g.note_doubted(t.grant, now.wait, None);
            }
            if !g.inflight.contains_key(&t.grant) {
                continue;
            }
            if in_doubt {
                g.pin(t.grant, t.until_h, now);
            }
            let e = g.inflight.get_mut(&t.grant).expect("checked");
            e.active = e.active.saturating_sub(1);
            if e.over(now) {
                g.inflight.remove(&t.grant);
                // A recalled grant's release may be waiting for this (an
                // idle one's too: `idle_before` marks it recalled).
                let id = g.current_id(t.grant);
                if let Some(ino) = g
                    .held
                    .iter()
                    .find(|(_, h)| h.id == id && h.recalled && g.inflight_of(id).is_none())
                    .map(|(ino, _)| *ino)
                {
                    wake.push(ino);
                }
            }
        }
        let hook = g.release_wake.clone();
        drop(g);
        if let Some(hook) = hook {
            for ino in wake {
                hook(ino);
            }
        }
    }

    /// A mutation tagged with `tag` ended in doubt (it may still execute
    /// somewhere): its grants are pinned until its tokens' windows are
    /// over now, before its answer goes out, not only at its
    /// [`Self::tag_end`] — a new turn the answer lets the application
    /// start must already find the pin (`LocalOutcome::Predecessor`).
    /// `rid`: the op's, when known, so the doubt is resolved by its
    /// outcome ([`Self::resolve_doubt`]).
    pub fn pin_in_doubt(&self, tag: &LockTag, rid: Option<crate::rid::Rid>) {
        if tag.is_empty() {
            return;
        }
        let mut g = self.lock();
        let now = g.now_or(Now::same(0));
        for t in &tag.0 {
            g.pin(t.grant, t.until_h, now);
            g.note_doubted(t.grant, now.wait, rid);
        }
    }

    /// A close (`FLUSH`, `RELEASE`) or the release's flush starts
    /// publishing the file `grant` is held on, under that grant: what was
    /// written under the locks the close just dropped, which a new turn
    /// on the node waits for (`LocalOutcome::Predecessor`), until
    /// [`Self::publish_end`]. The release waits for it too
    /// ([`Self::release_blocked`]).
    pub fn publish_begin(&self, grant: GrantId) {
        self.lock().inflight.entry(grant).or_default().publishing += 1;
    }

    /// The publication [`Self::publish_begin`] announced is over. Like
    /// [`Self::tag_end`], wakes a recalled grant's release that waited
    /// for it, when nothing else holds the grant back.
    pub fn publish_end(&self, grant: GrantId, now: Now) {
        let mut g = self.lock();
        let Some(e) = g.inflight.get_mut(&grant) else {
            return;
        };
        e.publishing = e.publishing.saturating_sub(1);
        if !e.over(now) {
            return;
        }
        g.inflight.remove(&grant);
        let id = g.current_id(grant);
        let wake = g
            .held
            .iter()
            .find(|(_, h)| h.id == id && h.recalled && g.inflight_of(id).is_none())
            .map(|(ino, _)| *ino);
        let hook = g.release_wake.clone();
        drop(g);
        if let (Some(hook), Some(ino)) = (hook, wake) {
            hook(ino);
        }
    }

    /// Install the hook [`Self::tag_end`] calls when a recalled grant's
    /// last tagged mutation in flight ended (the release waiting for it
    /// goes on then instead of at its next poll).
    pub fn set_release_wake(&self, wake: ReleaseWake) {
        self.lock().release_wake = Some(wake);
    }

    /// The grants this node holds now (`[minter, seq]` each), for
    /// `status`.
    pub fn held_ids(&self) -> Vec<GrantId> {
        self.lock().held.values().map(|h| h.id).collect()
    }

    /// Plan 43 C7-F F-c: whether a grant `tag` names is held here and
    /// lapsing at `now` (its owner has not said yet whether a late renewal is
    /// admitted): a mutation issued under it waits for the verdict.
    pub fn tag_lapsing(&self, tag: &LockTag, now: Now) -> bool {
        if tag.is_empty() || !self.active() {
            return false;
        }
        let mut g = self.lock();
        let inos: Vec<u64> = g
            .held
            .iter()
            .filter(|(_, h)| tag.0.iter().any(|t| t.grant == h.id))
            .map(|(i, _)| *i)
            .collect();
        let mut any = false;
        for ino in inos {
            any |= g.observe(ino, now, self.park_ms()) == Observed::Lapsing;
        }
        self.track(&g);
        any
    }

    /// Whether every grant `tag` names is held here and honoured at
    /// `now`.
    pub fn tag_honoured(&self, tag: &LockTag, now: Now) -> bool {
        let g = self.lock();
        tag.0.iter().all(|t| {
            g.held
                .values()
                .any(|h| h.id == t.grant && h.until > now.honour)
        })
    }

    /// `tag` with each token's window brought up to date: a grant this
    /// node still holds and honours at `now` is named with the end of
    /// its current window (renewals move it), any other keeps the window
    /// it had. Phase 2 takes an op's token when it is sent, not when the
    /// op started: a close whose flush took long, or a retry, is not
    /// refused for the window its op set out with.
    pub fn refresh_tag(&self, tag: &LockTag, now: Now) -> LockTag {
        if tag.is_empty() {
            return LockTag::NONE;
        }
        let g = self.lock();
        LockTag(
            tag.0
                .iter()
                .map(|t| {
                    // A grant this node still holds and honours is named with
                    // the end of its current window (a renewal moved it); the
                    // token is received here, now (the op leaves from here).
                    match g
                        .held
                        .values()
                        .find(|h| h.id == t.grant && h.until > now.honour)
                    {
                        Some(h) if h.until.0 >= t.until_h => {
                            LockToken::minted(t.grant, h.until, now, t.clock)
                        }
                        _ => LockToken {
                            remaining_ms: (t.until_h - now.honour.0).max(0),
                            recv: Some(now.honour.0),
                            ..*t
                        },
                    }
                })
                .collect(),
        )
    }

    /// Whether `grant` must not be released yet: a mutation tagged with it
    /// is in flight, or one left in doubt may still execute (phase 2's
    /// release ordering — a released grant's stale op must never land
    /// after the next holder's). `Some(t)`: not before `t` (in doubt
    /// only; `None` with ops in flight: when they are answered).
    pub fn release_blocked(&self, grant: GrantId, now: Now) -> Option<Option<WaitMs>> {
        let mut g = self.lock();
        // Every id of the grant (`superseded`): an op tagged with an older
        // one holds this release back as much as one tagged with this id.
        let e = g.inflight_of(grant)?;
        // A close publishing the file may be tagged with another file's
        // grant (`Inflight::publishing`): counted here on its own.
        if e.active > 0 || e.publishing > 0 {
            return Some(None);
        }
        if e.pinned_until > now.wait {
            return Some(Some(e.pinned_until));
        }
        for id in g.lineage(grant) {
            g.inflight.remove(&id);
        }
        None
    }

    /// The owners fenced at `now`, for `status`: as
    /// [`Self::fenced_owners`] (a lapse no op has looked at yet is
    /// captured here too), but empty — with no capture — while no local
    /// lock exists.
    pub fn fenced_owners_snapshot(&self, now: Now) -> Vec<FencedOwner> {
        if self.local_inos.load(Ordering::Relaxed) == 0 {
            return self.lock().fenced_owners.values().copied().collect();
        }
        self.fenced_owners(now)
    }

    /// An op of a fenced owner was refused (counted).
    pub fn note_owner_fenced_op(&self) {
        self.lock().stats.owner_fenced_ops += 1;
    }

    /// Plan 30 §M14, the fence at every publication point (close,
    /// release, `fsync`, a recalled grant's flush, a new lock): whether
    /// `ino`'s dirty data must be **discarded** instead of published —
    /// `Some(fenced)` then (`fenced`: local locks are still under the
    /// lapsed grant, so the caller answers `EIO` even with nothing
    /// dirty), `None` when it may be published.
    ///
    /// A held grant found lapsed here is dropped (as the renewal tick
    /// would), which taints the inode; a taint is consumed (the caller
    /// discards). One relaxed load of two counters when this node holds
    /// no grant, no local lock and no taint.
    ///
    /// Plan 43 C7-F F-c: a lapsed grant that pins something (local locks,
    /// or a write under it) is not dropped here: it is lapsing, and the
    /// answer is [`Discard::Parked`] — the caller waits for the owner's
    /// verdict and asks again. `write`: the caller is about to write under
    /// the grant (it is then marked as pinning data: [`HeldGrant::written`]).
    pub fn take_discard(&self, ino: u64, now: Now, write: bool) -> Discard {
        if self.local_inos.load(Ordering::Relaxed) == 0 && self.tracked.load(Ordering::Relaxed) == 0
        {
            return Discard::Publish;
        }
        let mut g = self.lock();
        if g.observe(ino, now, self.park_ms()) == Observed::Lapsing {
            self.track(&g);
            return Discard::Parked;
        }
        let lapsed = g.held.get(&ino).copied().filter(|h| h.until <= now.honour);
        if let Some(h) = lapsed {
            g.held.remove(&ino);
            g.tombstone(h.id);
            g.taint(ino);
        }
        let fenced = g.fenced_at(ino, now.honour);
        if fenced {
            // Fenced since the dropped grant's window ended, not since now.
            g.capture(ino, lapsed.map_or(now.honour, |h| h.until));
        }
        let tainted = g.taint.remove(&ino);
        if fenced {
            g.stats.fenced_io += 1;
        }
        if write && !fenced {
            if let Some(h) = g.held.get_mut(&ino) {
                h.written = true;
            }
        }
        // Nothing changed on the common path (an honoured grant, no taint):
        // the counters track what did.
        if lapsed.is_some()
            || fenced
            || tainted
            || g.held.len() + g.taint.len() != self.tracked.load(Ordering::Relaxed)
        {
            self.track(&g);
        }
        if fenced || tainted {
            Discard::Discard { fenced }
        } else {
            Discard::Publish
        }
    }

    /// Dirty data of `ino` was discarded: an error event every open file
    /// description of it reports once (see the module doc). Returns the
    /// event's sequence number (what a description that is told now
    /// records as seen).
    pub fn note_discard(&self, ino: u64) -> u64 {
        let mut g = self.lock();
        let seq = self.error_events.fetch_add(1, Ordering::Relaxed) + 1;
        g.errors.insert(ino, seq);
        seq
    }

    /// The sequence number of `ino`'s latest discard error event (0:
    /// none). One relaxed load while no event was ever noted.
    pub fn error_seq(&self, ino: u64) -> u64 {
        if self.error_events.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        self.lock().errors.get(&ino).copied().unwrap_or(0)
    }

    /// Every inode's latest discard error event (a handover carries them).
    pub fn export_errors(&self) -> Vec<(u64, u64)> {
        if self.error_events.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        self.lock().errors.iter().map(|(i, s)| (*i, *s)).collect()
    }

    /// Adopt a previous process's events ([`Self::export_errors`]), and
    /// number every later event above `floor` and above each of them: a
    /// description that crossed having seen event `n` must see the next
    /// one as newer.
    pub fn import_errors(&self, errors: &[(u64, u64)], floor: u64) {
        let mut g = self.lock();
        let mut top = floor;
        for (ino, seq) in errors {
            let e = g.errors.entry(*ino).or_insert(0);
            *e = (*e).max(*seq);
            top = top.max(*seq);
        }
        self.error_events.fetch_max(top, Ordering::Relaxed);
    }

    /// No description of `ino` is open any more: nobody is owed its
    /// error events.
    pub fn forget_errors(&self, ino: u64) {
        if self.error_events.load(Ordering::Relaxed) == 0 {
            return;
        }
        self.lock().errors.remove(&ino);
    }

    // ------------------------------------------------------ node: local locks

    /// The strongest local lock on `ino`, if any.
    pub fn local_mode(&self, ino: u64) -> Option<LockMode> {
        self.lock()
            .local
            .get(&ino)
            .and_then(|v| v.iter().map(LocalLock::mode).max())
    }

    pub fn local_locks(&self, ino: u64) -> Vec<LocalLock> {
        self.lock().local.get(&ino).cloned().unwrap_or_default()
    }

    /// `getlk`: the first local lock of another owner conflicting with
    /// the range, if the grant held covers the request; else what the
    /// sequencer must be asked.
    pub fn local_test(
        &self,
        ino: u64,
        owner: u64,
        write: bool,
        start: u64,
        end: u64,
        now: Now,
    ) -> LocalOutcome {
        self.local_test_going_last(ino, owner, write, start, end, now, |_| false)
    }

    /// [`Self::local_test`] that names a lock of an owner `going` names
    /// (one of a description on its way out, whose locks Linux has
    /// dropped already: the engine's `lock_handles`) only when no other
    /// lock is in the way. It is still in the way here: the caller waits
    /// for it to go.
    #[allow(clippy::too_many_arguments)]
    pub fn local_test_going_last(
        &self,
        ino: u64,
        owner: u64,
        write: bool,
        start: u64,
        end: u64,
        now: Now,
        going: impl Fn(u64) -> bool,
    ) -> LocalOutcome {
        let need = if write {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        };
        let g = self.lock();
        if let Some(v) = g.local.get(&ino) {
            let conflicts =
                |l: &&LocalLock| l.owner != owner && l.overlaps(start, end) && (write || l.write);
            if let Some(c) = v
                .iter()
                .filter(conflicts)
                .find(|l| !going(l.owner))
                .or_else(|| v.iter().find(conflicts))
            {
                return LocalOutcome::Conflict(*c);
            }
        }
        let continuing = g
            .local
            .get(&ino)
            .is_some_and(|v| v.iter().any(|l| l.owner == owner));
        let covered = g.held.get(&ino).is_some_and(|h| {
            h.until > now.honour
                && (!h.recalled || h.first_use || continuing)
                && h.mode.covers(need)
        });
        if covered {
            LocalOutcome::Done
        } else {
            LocalOutcome::NeedGrant(need)
        }
    }

    /// `setlk`: take the lock under the grant held (POSIX semantics: the
    /// owner's own overlapping ranges are replaced), or say what is in
    /// the way. The grant must cover the request and be honoured and
    /// not recalled; else `NeedGrant`.
    pub fn local_set(&self, ino: u64, lock: LocalLock, now: Now) -> LocalOutcome {
        let need = lock.mode();
        let mut g = self.lock();
        let conflict = g.local.get(&ino).and_then(|v| {
            v.iter()
                .find(|l| {
                    l.owner != lock.owner
                        && l.overlaps(lock.start, lock.end)
                        && (lock.write || l.write)
                })
                .copied()
        });
        if let Some(c) = conflict {
            g.stats.local_conflicts += 1;
            return LocalOutcome::Conflict(c);
        }
        // A recalled grant still serves an owner that already holds a
        // lock under it (SQLite upgrading within its transaction): the
        // release waits for that owner's unlock anyway. New owners wait
        // for a fresh grant.
        let continuing = g
            .local
            .get(&ino)
            .is_some_and(|v| v.iter().any(|l| l.owner == lock.owner));
        let covered = g.held.get(&ino).is_some_and(|h| {
            h.until > now.honour
                && (!h.recalled || h.first_use || continuing)
                && h.mode.covers(need)
        });
        if !covered {
            // Plan 43 C7-F F-c: a grant that is lapsing is not replaced by
            // a fresh one under the application's feet (a new grant would
            // fence its owners): the request waits for the owner's verdict.
            if g.observe(ino, now, self.park_ms()) == Observed::Lapsing {
                self.track(&g);
                return LocalOutcome::Parked;
            }
            return LocalOutcome::NeedGrant(need);
        }
        // A new turn on this node under a grant that what an earlier turn
        // counts as done is still on its way under — a close's publication
        // of the file, or a tagged op left in doubt and not yet past its
        // window — waits for it: the grant is live at its minter, so the
        // token would let it land after this turn's writes (review round
        // 2, should-fix 1). A tagged op still waiting for its answer is
        // not waited for: its caller has not seen it done, so it is
        // concurrent with the unlock (another thread of the process, or
        // xcheckfs mirroring every application through one process), and
        // a non-blocking request refused behind it answered `EAGAIN` with
        // no lock in the way. The release waits for every tagged op.
        let new_turn = g.local.get(&ino).is_none_or(|v| v.is_empty());
        if new_turn {
            let id = g.held.get(&ino).map(|h| h.id);
            if id.is_some_and(|id| {
                g.inflight_of(id)
                    .is_some_and(|e| e.publishing > 0 || e.pinned_until > now.wait)
            }) {
                // Counted once per request by the caller, which may ask
                // again many times (`LockStats::predecessor_waits`).
                return LocalOutcome::Predecessor;
            }
        }
        if let Some(h) = g.held.get_mut(&ino) {
            h.first_use = false;
            h.idle_since = None;
        }
        let was_empty = g.local.get(&ino).is_none_or(|v| v.is_empty());
        let v = g.local.entry(ino).or_default();
        // POSIX merges an owner's locks of one type that overlap or touch
        // into one: `F_GETLK` then reports the whole lock, not the piece
        // set last (xcheckfs saw ext4 answer 4+24 where this said 4+8).
        // The ranges merged are only that owner's same-type locks (an
        // owner's locks never overlap, and none of one type touch: one
        // pass finds every lock the union touches), so cutting the union
        // is exact. The result goes where Linux keeps it (`place`).
        let mut lock = lock;
        for l in v.iter() {
            if l.owner == lock.owner
                && l.write == lock.write
                && l.start <= lock.end.saturating_add(1)
                && lock.start <= l.end.saturating_add(1)
            {
                lock.start = lock.start.min(l.start);
                lock.end = lock.end.max(l.end);
            }
        }
        let block = v.iter().position(|l| l.owner == lock.owner);
        Self::cut(v, lock.owner, lock.start, lock.end);
        v.insert(Self::place(v, &lock, block), lock);
        if was_empty {
            self.local_inos.fetch_add(1, Ordering::Relaxed);
        }
        g.stats.local_hits += 1;
        self.refence(&g);
        LocalOutcome::Done
    }

    /// Remove `owner`'s locks within `[start, end]` (splitting as POSIX
    /// does). Returns whether `ino` has no local lock left.
    pub fn local_unlock(&self, ino: u64, owner: u64, start: u64, end: u64, now: Now) -> bool {
        let mut g = self.lock();
        let fenced = g.fenced_at(ino, now.honour);
        let lapsing = g.held.get(&ino).is_some_and(|h| h.lapsing.is_some());
        let Some(v) = g.local.get_mut(&ino) else {
            return true;
        };
        let touched = v.iter().any(|l| l.owner == owner && l.overlaps(start, end));
        Self::cut(v, owner, start, end);
        let idle = v.is_empty();
        if idle {
            g.local.remove(&ino);
            self.local_inos.fetch_sub(1, Ordering::Relaxed);
            if let Some(h) = g.held.get_mut(&ino) {
                h.idle_since = Some(now.wait);
            }
        }
        if fenced && touched && !lapsing {
            // Unlocked under a lapsed grant: the fence lifts with the
            // last lock; the taint keeps what was written under it from
            // being published. (Not while the grant is lapsing: it may
            // be admitted late, and then nothing was lost; if it is
            // refused the taint is set then, `Self::lost`.)
            g.taint(ino);
        }
        if idle {
            g.quiet_drop_lapsing(ino);
        }
        g.lift_if_unlocked(owner);
        self.track(&g);
        idle
    }

    /// Drop every lock of `owner` on `ino` (a close). Returns whether
    /// `ino` has no local lock left.
    pub fn local_release_owner(&self, ino: u64, owner: u64, now: Now) -> bool {
        self.local_release_owner_dropped(ino, owner, now).0
    }

    /// [`Self::local_release_owner`], also saying whether it dropped any
    /// lock: `(no local lock left, dropped)`.
    pub fn local_release_owner_dropped(&self, ino: u64, owner: u64, now: Now) -> (bool, bool) {
        let mut g = self.lock();
        let fenced = g.fenced_at(ino, now.honour);
        let lapsing = g.held.get(&ino).is_some_and(|h| h.lapsing.is_some());
        let Some(v) = g.local.get_mut(&ino) else {
            return (true, false);
        };
        let before = v.len();
        v.retain(|l| l.owner != owner);
        let dropped = v.len() != before;
        let idle = v.is_empty();
        if fenced && dropped && !lapsing {
            g.taint(ino);
        }
        if idle {
            g.local.remove(&ino);
            self.local_inos.fetch_sub(1, Ordering::Relaxed);
            if let Some(h) = g.held.get_mut(&ino) {
                h.idle_since = Some(now.wait);
            }
            g.quiet_drop_lapsing(ino);
        }
        g.lift_if_unlocked(owner);
        self.track(&g);
        (idle, dropped)
    }

    /// Grants idle (no local lock under them) since before `before`,
    /// marked recalled so their release proceeds like a recall's.
    pub fn idle_before(&self, before: WaitMs) -> Vec<(u64, HeldGrant)> {
        let mut g = self.lock();
        let mut out = Vec::new();
        for (ino, h) in g.held.iter_mut() {
            if h.idle_since.is_some_and(|t| t < before)
                && !h.recalled
                && !h.releasing
                && h.lapsing.is_none()
            {
                h.recalled = true;
                out.push((*ino, *h));
            }
        }
        out
    }

    pub fn local_idle(&self, ino: u64) -> bool {
        self.lock().local.get(&ino).is_none_or(|v| v.is_empty())
    }

    /// Where `lock` goes in a file's list, as Linux keeps it
    /// (`posix_lock_inode`): each owner's locks together, sorted by
    /// start, the owners in the order their first lock came. `F_GETLK`
    /// then names the first lock in the way that the kernel would
    /// ([`Self::local_test`]). `v` holds no lock of `lock`'s owner that
    /// overlaps it ([`Self::cut`]); `block`: where the owner's locks
    /// began before that cut. A lock that replaced all of them (merged
    /// with them, or over them) keeps their place, as the kernel extends
    /// or replaces the lock in its list; an owner that had none comes
    /// last.
    fn place(v: &[LocalLock], lock: &LocalLock, block: Option<usize>) -> usize {
        let Some(first) = v.iter().position(|l| l.owner == lock.owner) else {
            // The cut removed only that owner's locks, all at and after
            // `block`: the others before it are where they were.
            return block.map_or(v.len(), |b| b.min(v.len()));
        };
        let block = v[first..]
            .iter()
            .position(|l| l.owner != lock.owner)
            .map_or(v.len(), |n| first + n);
        v[first..block]
            .iter()
            .position(|l| l.start > lock.start)
            .map_or(block, |n| first + n)
    }

    /// POSIX range subtraction for one owner: remove `owner`'s locks
    /// within `[start, end]`, keeping what is left of each in its place
    /// (a lock cut in two leaves both pieces there, in order).
    fn cut(v: &mut Vec<LocalLock>, owner: u64, start: u64, end: u64) {
        let mut out = Vec::with_capacity(v.len() + 1);
        for l in v.drain(..) {
            if l.owner != owner || !l.overlaps(start, end) {
                out.push(l);
                continue;
            }
            if l.start < start {
                out.push(LocalLock {
                    end: start - 1,
                    ..l
                });
            }
            if l.end > end {
                out.push(LocalLock {
                    start: end + 1,
                    ..l
                });
            }
        }
        *v = out;
    }
}

impl crate::Meta {
    pub fn locks(&self) -> &LockTables {
        &self.locks
    }

    /// F1: before this node reports a position as covering what it
    /// executed — a grant's floor or position, an outwait barrier's cut, a
    /// delegate's stream head — every tagged write that passed its check
    /// at the write under a grant no longer live here has committed
    /// ([`LockTables::unsettled_writes`]). Such a write holds the store's
    /// writer lock from its check to its commit, so taking the lock once
    /// is the wait; nothing else is. A write that checks later finds the
    /// grant ended (refused), or its window over at an executor that only
    /// judges it by the window (a cut as of a time past it needs nothing
    /// from it). `true` if it waited. One atomic load while no tagged
    /// write is in a transaction.
    /// Tests only (`test-util`; the simulation's `WriterLockHold`): `hook`
    /// runs on every thread about to wait for this store's writer lock,
    /// before it waits — where a write parked inside its transaction (a
    /// slow commit) is let go, as every writer would wait for it.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_writer_wait_hook_for_tests(
        &self,
        hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
    ) {
        *self.db.wait_hook.lock().unwrap_or_else(|e| e.into_inner()) = hook;
    }

    /// Tests only (`test-util`): `hook` runs on every thread about to open a
    /// read transaction on this store, before it does — where a read that
    /// went to a stalled device is parked.
    #[cfg(any(test, feature = "test-util"))]
    pub fn set_read_hook_for_tests(&self, hook: Option<std::sync::Arc<dyn Fn() + Send + Sync>>) {
        *self.db.read_hook.lock().unwrap_or_else(|e| e.into_inner()) = hook;
    }

    pub fn settle_tagged_writes(&self) -> bool {
        if !self.locks.settles() || !self.locks.unsettled_writes() {
            return false;
        }
        drop(self.db.write_tx());
        self.locks.with_stats(|s| s.settle_waits += 1);
        true
    }

    /// Plan 30 §M14 phase 2: run `f` — a journaled write that is not a
    /// `MutateOp` execution (the holder's own manifest commit) — under the
    /// fencing token `tag`, checked at `now` as
    /// [`crate::mutate::execute_tagged`] checks it, and recorded with the
    /// transaction's `journal_tx` row.
    pub fn with_lock_tag<T>(
        &self,
        tag: &LockTag,
        now: Now,
        f: impl FnOnce() -> Result<T, crate::MetaError>,
    ) -> Result<T, crate::MetaError> {
        if self.locks.check_tag(tag, now).is_err() {
            return Err(crate::MetaError::LockLapsed);
        }
        let _tag = crate::store::journal::PendingLockTag::set(tag, now, &self.locks);
        let out = f()?;
        crate::mutate::note_landed(tag);
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(mode: LockMode, until: i64) -> HeldGrant {
        HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode,
            until: HonourMs(until),
            renew_at: HonourMs(until / 2),
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        }
    }

    fn lk(owner: u64, write: bool, start: u64, end: u64) -> LocalLock {
        LocalLock {
            owner,
            pid: 1,
            pid_start: 0,
            write,
            start,
            end,
        }
    }

    /// A table with a whole-file exclusive grant held on inode 7.
    fn granted() -> LockTables {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, i64::MAX));
        t
    }

    /// `(owner, write, start, end)` of each local lock on inode 7, in
    /// the table's order.
    fn table(t: &LockTables) -> Vec<(u64, bool, u64, u64)> {
        t.local_locks(7)
            .iter()
            .map(|l| (l.owner, l.write, l.start, l.end))
            .collect()
    }

    /// Finding 12 (xcheckfs, ext4 against a mount): `F_GETLK` names the
    /// lock Linux names, the first conflicting one in its list, which
    /// keeps each owner's locks together, in the order the owners first
    /// locked the file, and sorted by start (`posix_lock_inode`). The
    /// table appended each new lock instead: owner 10's `W[58,75]` then
    /// `W[29,38]`, tested by owner 11 over the whole file, answered
    /// 58+18 where ext4 answers 29+10.
    #[test]
    fn a_test_names_the_first_conflicting_lock_in_the_kernels_order() {
        let t = granted();
        let test = |owner, start, end| match t.local_test(7, owner, true, start, end, Now::same(0))
        {
            LocalOutcome::Conflict(l) => Some((l.owner, l.start, l.end)),
            _ => None,
        };
        assert_eq!(
            t.local_set(7, lk(10, true, 58, 75), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(10, true, 29, 38), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(test(11, 1, u64::MAX), Some((10, 29, 38)));
        // Owners in the order they first locked the file, whatever their
        // ranges: owner 12's lower range comes after owner 10's.
        assert_eq!(
            t.local_set(7, lk(12, true, 0, 9), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(test(11, 0, u64::MAX), Some((10, 29, 38)));
        assert_eq!(
            table(&t),
            [(10, true, 29, 38), (10, true, 58, 75), (12, true, 0, 9)]
        );
        // A lock that replaces all of an owner's locks keeps their place
        // (lockdiff on the mount: B `W[26,43]`, A `W[58,69]`, B
        // `W[42,51]`, C tests `W[46,68]`: ext4 names B's 26+26).
        let u = granted();
        assert_eq!(
            u.local_set(7, lk(20, true, 26, 43), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            u.local_set(7, lk(21, true, 58, 69), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            u.local_set(7, lk(20, true, 42, 51), Now::same(0)),
            LocalOutcome::Done
        );
        match u.local_test(7, 22, true, 46, 68, Now::same(0)) {
            LocalOutcome::Conflict(l) => assert_eq!((l.owner, l.start, l.end), (20, 26, 51)),
            other => panic!("{other:?}"),
        }
        // So does one of the other type over all of them.
        assert_eq!(
            u.local_set(7, lk(20, false, 20, 55), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(table(&u), [(20, false, 20, 55), (21, true, 58, 69)]);
        // An owner that unlocked everything and locks again comes last.
        t.local_unlock(7, 10, 0, u64::MAX, Now::same(0));
        assert_eq!(
            t.local_set(7, lk(10, true, 20, 29), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(test(11, 0, u64::MAX), Some((12, 0, 9)));
        assert_eq!(table(&t), [(12, true, 0, 9), (10, true, 20, 29)]);
    }

    /// An owner's locks as Linux keeps them: touching or overlapping
    /// ranges of one type merged into one (`F_GETLK` reports the whole
    /// run, never a piece of it), a lock of the other type inside one
    /// splitting it, an unlock cutting it, each in start order.
    #[test]
    fn an_owners_locks_are_merged_split_and_ordered_as_the_kernel_keeps_them() {
        let t = granted();
        let set = |write, start, end| {
            assert_eq!(
                t.local_set(7, lk(10, write, start, end), Now::same(0)),
                LocalOutcome::Done
            )
        };
        set(true, 0, 19);
        // A read lock inside its own write lock splits it.
        set(false, 5, 9);
        assert_eq!(
            table(&t),
            [(10, true, 0, 4), (10, false, 5, 9), (10, true, 10, 19)]
        );
        // Converted back: one write lock again.
        set(true, 5, 9);
        assert_eq!(table(&t), [(10, true, 0, 19)]);
        // A partial unlock cuts it; filling the hole merges it again.
        t.local_unlock(7, 10, 8, 11, Now::same(0));
        assert_eq!(table(&t), [(10, true, 0, 7), (10, true, 12, 19)]);
        set(true, 8, 11);
        assert_eq!(table(&t), [(10, true, 0, 19)]);
        // Touching ranges merge, both sides at once.
        set(true, 30, 34);
        set(true, 40, 44);
        set(true, 35, 39);
        assert_eq!(table(&t), [(10, true, 0, 19), (10, true, 30, 44)]);
        // A range to the end merges what it touches before it.
        set(true, 45, u64::MAX);
        assert_eq!(table(&t), [(10, true, 0, 19), (10, true, 30, u64::MAX)]);
        // A read lock over the end of one write run and into a gap:
        // merged with nothing of the other type.
        set(false, 15, 25);
        assert_eq!(
            table(&t),
            [
                (10, true, 0, 14),
                (10, false, 15, 25),
                (10, true, 30, u64::MAX)
            ]
        );
        // A test by another owner names the whole run in its way.
        match t.local_test(7, 11, false, 40, 41, Now::same(0)) {
            LocalOutcome::Conflict(l) => assert_eq!((l.start, l.end), (30, u64::MAX)),
            other => panic!("{other:?}"),
        }
    }

    /// A test names the locks of owners its caller says are going (those
    /// of a description whose `RELEASE` is on its way, gone on Linux)
    /// last: the first other lock in the way, theirs only when no other
    /// lock is (they are still held here, and the caller waits for them).
    #[test]
    fn a_test_names_the_locks_of_owners_on_their_way_out_last() {
        let t = granted();
        assert_eq!(
            t.local_set(7, lk(10, true, 48, 48), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(12, false, 53, u64::MAX), Now::same(0)),
            LocalOutcome::Done
        );
        let test =
            |going: u64| match t
                .local_test_going_last(7, 11, true, 43, 66, Now::same(0), |o| o == going)
            {
                LocalOutcome::Conflict(l) => Some((l.owner, l.start, l.end)),
                _ => None,
            };
        assert_eq!(test(0), Some((10, 48, 48)));
        assert_eq!(test(10), Some((12, 53, u64::MAX)));
        assert_eq!(test(12), Some((10, 48, 48)));
        t.local_release_owner(7, 12, Now::same(0));
        assert_eq!(test(10), Some((10, 48, 48)));
        t.local_release_owner(7, 10, Now::same(0));
        assert_eq!(test(10), None);
    }

    #[test]
    fn a_handoff_copy_is_installed_only_into_a_consistent_table() {
        let t = LockTables::default();
        let copy = |node: u64, seq: u64, mode: LockMode| Grant {
            id: GrantId { node: 9, seq },
            node,
            ino: 7,
            mode,
            until: WaitMs(100),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        // Node 2 was granted again (a newer id): the old copy stays out.
        t.install(copy(2, 5, LockMode::Shared));
        assert!(!t.install_if_consistent(copy(2, 1, LockMode::Exclusive), WaitMs(0)));
        // Node 3's exclusive copy conflicts with node 2's grant (made
        // after the copy's grant left): out.
        assert!(!t.install_if_consistent(copy(3, 2, LockMode::Exclusive), WaitMs(0)));
        // A compatible copy goes in, once.
        assert!(t.install_if_consistent(copy(3, 3, LockMode::Shared), WaitMs(0)));
        assert!(!t.install_if_consistent(copy(3, 3, LockMode::Shared), WaitMs(0)));
        assert_eq!(t.grants_len(), 2);
    }

    /// A grant that moved carrying what was left of its record replaces the
    /// node's record on the inode but never shortens it: a reclaim answered
    /// in a grace before the move arrived extended its holder's window.
    #[test]
    fn install_keeping_later_never_shortens_the_replaced_record() {
        let at = |seq: u64, until: i64| Grant {
            id: GrantId { node: 9, seq },
            node: 2,
            ino: 7,
            mode: LockMode::Exclusive,
            until: WaitMs(until),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        let t = LockTables::default();
        // A later record of the same node on the inode (another id): kept.
        t.install(at(1, 900));
        t.install_keeping_later(at(2, 300));
        assert_eq!(t.grants_len(), 1);
        assert_eq!(t.get(GrantId { node: 9, seq: 1 }), None);
        assert_eq!(
            t.get(GrantId { node: 9, seq: 2 }).unwrap().until,
            WaitMs(900)
        );
        // An earlier one: replaced by the new one.
        t.install_keeping_later(at(3, 1500));
        assert_eq!(t.grants_len(), 1);
        assert_eq!(
            t.get(GrantId { node: 9, seq: 3 }).unwrap().until,
            WaitMs(1500)
        );
        // Another node's record on the inode is left alone.
        let other = Grant {
            node: 3,
            ..at(4, 5000)
        };
        t.install(other);
        t.install_keeping_later(at(5, 100));
        assert_eq!(t.grants_len(), 2);
        assert_eq!(
            t.get(GrantId { node: 9, seq: 5 }).unwrap().until,
            WaitMs(1500)
        );
        assert_eq!(
            t.get(GrantId { node: 9, seq: 4 }).unwrap().until,
            WaitMs(5000)
        );
    }

    /// An expired record is no grant: a copy of the same node, or a
    /// conflicting one, goes in over it (sim `locks-unlinked-delegated-
    /// dbackup-random` seed 5681: the root's stale record of the holder
    /// kept the grant that came with an unlink out of its table).
    #[test]
    fn an_expired_record_does_not_keep_a_copy_out() {
        let t = LockTables::default();
        let copy = |node: u64, seq: u64, mode: LockMode, until: i64| Grant {
            id: GrantId { node: 9, seq },
            node,
            ino: 7,
            mode,
            until: WaitMs(until),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        t.install(copy(2, 1, LockMode::Exclusive, 100));
        t.install(copy(3, 2, LockMode::Shared, 100));
        // Still live at 50: both keep the copy out.
        assert!(!t.install_if_consistent(copy(2, 3, LockMode::Exclusive, 500), WaitMs(50)));
        assert_eq!(t.grants_len(), 2);
        // Expired at 100: the copy is the inode's only grant, and the
        // dropped records are remembered as ended.
        assert!(t.install_if_consistent(copy(2, 3, LockMode::Exclusive, 500), WaitMs(100)));
        assert_eq!(t.grants_len(), 1);
        assert!(t.was_ended(GrantId { node: 9, seq: 1 }));
        assert!(t.was_ended(GrantId { node: 9, seq: 2 }));
        assert!(t.take_expired_dropped(), "the backup's mirror is stale");
        assert_eq!(
            t.conflicting(7, 4, LockMode::Shared, WaitMs(200))
                .iter()
                .map(|g| g.id.seq)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    /// The sequencer learns of every outwait, wherever the expired
    /// record was dropped (here a token check and a conflicting request),
    /// once; a released grant is no outwait.
    #[test]
    fn expired_records_dropped_anywhere_are_reported_as_outwaited_once() {
        let t = LockTables::default();
        let a = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(100), 0);
        let b = t.grant(1, 3, 8, LockMode::Exclusive, WaitMs(300), 0);
        let c = t.grant(1, 4, 9, LockMode::Exclusive, WaitMs(100), 0);
        t.forget(c);
        assert!(t.take_outwaited().is_empty(), "nothing dropped yet");
        assert!(t
            .check_tag(&LockTag(vec![LockToken::window(b, 300)]), Now::same(150))
            .is_ok());
        let first = t.take_outwaited();
        assert_eq!(first.iter().map(|g| g.id).collect::<Vec<_>>(), vec![a]);
        assert_eq!(first[0].until, WaitMs(100));
        assert!(t.take_outwaited().is_empty(), "reported once");
        assert!(t
            .conflicting(8, 5, LockMode::Exclusive, WaitMs(300))
            .is_empty());
        assert_eq!(
            t.take_outwaited().iter().map(|g| g.id).collect::<Vec<_>>(),
            vec![b]
        );
    }

    /// The lineage of a held grant lives as long as the grant and no
    /// longer: thousands of renamings of other grants do not evict it (an
    /// op in doubt under its old id held the release back only until the
    /// count-bounded map dropped the entry), and once the grant ends its
    /// entry goes.
    #[test]
    fn a_held_grants_lineage_is_kept_while_it_is_held_and_only_then() {
        let t = LockTables::default();
        let now = Now::same(0);
        let mk = |seq, until| HeldGrant {
            id: GrantId { node: 2, seq },
            ..held(LockMode::Exclusive, until)
        };
        t.install_held(7, mk(1, i64::MAX));
        // An op tagged with seq 1 ends in doubt; the grant goes on as 2.
        t.note_doubted(GrantId { node: 2, seq: 1 }, WaitMs(0), None);
        t.install_held(7, mk(2, i64::MAX));
        let cur = GrantId { node: 2, seq: 2 };
        assert!(t.was_doubted(cur));
        // Many other grants, each renamed once, then ended.
        for ino in 100..(100 + 2 * ENDED_KEPT as u64) {
            t.install_held(ino, mk(10 * ino, i64::MAX));
            t.install_held(ino, mk(10 * ino + 1, i64::MAX));
            assert!(t.drop_held(
                ino,
                GrantId {
                    node: 2,
                    seq: 10 * ino + 1
                }
            ));
        }
        assert!(t.was_doubted(cur), "the held grant's lineage was evicted");
        assert_eq!(t.lineage(cur).len(), 2);
        assert!(t.lock().lineages.len() <= 2, "ended grants' lineages kept");
        // The grant ends: its lineage goes with the next renaming.
        assert!(t.drop_held(7, cur));
        t.install_held(8, mk(3, i64::MAX));
        t.install_held(8, mk(4, i64::MAX));
        assert!(!t.lock().lineages.contains_key(&7));
        let _ = now;
    }

    #[test]
    fn an_older_grant_of_the_same_owner_is_refused_after_a_newer_one() {
        let t = LockTables::default();
        let mk = |seq| HeldGrant {
            id: GrantId { node: 2, seq },
            ..held(LockMode::Exclusive, 100)
        };
        assert!(matches!(t.install_held(7, mk(9)), Installed::Ok { .. }));
        // The older one's push lands after the newer one (which the owner
        // made to replace it).
        assert_eq!(t.install_held(7, mk(8)), Installed::Released);
        assert_eq!(t.held(7).map(|h| h.id.seq), Some(9));
        // Another owner's ids are not compared.
        assert!(matches!(
            t.install_held(
                7,
                HeldGrant {
                    id: GrantId { node: 3, seq: 1 },
                    ..held(LockMode::Exclusive, 100)
                }
            ),
            Installed::Ok { .. }
        ));
    }

    #[test]
    fn an_exclusive_local_lock_under_a_shared_grant_is_fenced() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(t.fenced(7, Now::same(0)), Fence::Ok);
        // The exclusive grant is replaced by a shared one (another local
        // owner's request after the exclusive one lapsed).
        t.install_held(
            7,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Shared, 100)
            },
        );
        assert_eq!(
            t.fenced(7, Now::same(0)),
            Fence::Fenced,
            "an exclusive local lock needs an exclusive grant"
        );
    }

    #[test]
    fn floor_join_keeps_the_newest_streams_past_the_cap() {
        let mut a = Position::ZERO;
        for g in 1..=8 {
            assert!(a.streams.raise(g, 10 * g));
        }
        let mut b = Position {
            seq: 4,
            pending: Some(crate::session::JournalPos { epoch: 2, jseq: 5 }),
            streams: Default::default(),
        };
        assert!(b.streams.raise(9, 1));
        assert!(b.streams.raise(3, 99));
        let j = floor_join(&a, &b);
        assert_eq!(j.seq, 4);
        assert_eq!(j.pending, b.pending);
        let s: BTreeMap<u64, u64> = j.streams_wire().into_iter().collect();
        // Generation 1 (the oldest) made room for 9; 3 took the max.
        assert!(!s.contains_key(&1), "{s:?}");
        assert_eq!(s.get(&9), Some(&1));
        assert_eq!(s.get(&3), Some(&99));
        assert_eq!(s.get(&8), Some(&80));
        // `Position::join` would have dropped b's streams wholesale.
        assert!(!a.join(&b).streams_wire().iter().any(|(g, _)| *g == 9));
    }

    #[test]
    fn local_locks_need_a_covering_grant() {
        let t = LockTables::default();
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
        t.install_held(7, held(LockMode::Shared, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
        assert_eq!(
            t.local_set(7, lk(1, false, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        // A lapsed grant covers nothing.
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(100)),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
    }

    #[test]
    fn an_owners_touching_locks_of_one_type_merge() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, false, 4, 11), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(1, false, 15, 18), Now::same(0)),
            LocalOutcome::Done
        );
        // Overlapping one, touching the other: one lock 4..=27.
        assert_eq!(
            t.local_set(7, lk(1, false, 12, 27), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(t.local_locks(7), [lk(1, false, 4, 27)]);
        // Another type, or another owner, does not merge.
        assert_eq!(
            t.local_set(7, lk(1, true, 28, 30), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(2, false, 31, 40), Now::same(0)),
            LocalOutcome::Done
        );
        let mut v = t.local_locks(7);
        v.sort_by_key(|l| l.start);
        assert_eq!(
            v,
            [
                lk(1, false, 4, 27),
                lk(1, true, 28, 30),
                lk(2, false, 31, 40)
            ]
        );
        // A read lock over the owner's write lock replaces that part,
        // and merges with the read lock it now touches.
        assert_eq!(
            t.local_set(7, lk(1, false, 28, 29), Now::same(0)),
            LocalOutcome::Done
        );
        let mut v = t.local_locks(7);
        v.sort_by_key(|l| l.start);
        assert_eq!(
            v,
            [
                lk(1, false, 4, 29),
                lk(1, true, 30, 30),
                lk(2, false, 31, 40)
            ]
        );
    }

    #[test]
    fn posix_ranges_split_and_conflict_by_owner() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 100), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(2, false, 50, 60), Now::same(0)),
            LocalOutcome::Conflict(lk(1, true, 0, 100))
        );
        // Unlock the middle: two pieces remain.
        assert!(!t.local_unlock(7, 1, 40, 60, Now::same(0)));
        let v = t.local_locks(7);
        assert_eq!(v, vec![lk(1, true, 0, 39), lk(1, true, 61, 100)]);
        // Now owner 2 fits in the hole; shared/shared overlap is fine.
        assert_eq!(
            t.local_set(7, lk(2, false, 45, 55), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(3, false, 45, 55), Now::same(0)),
            LocalOutcome::Done
        );
        assert!(!t.local_release_owner(7, 1, Now::same(0)));
        assert!(!t.local_release_owner(7, 2, Now::same(0)));
        assert!(t.local_release_owner(7, 3, Now::same(0)));
        assert!(t.local_idle(7));
    }

    #[test]
    fn fencing_is_free_without_local_locks_and_bites_after_lapse() {
        let t = LockTables::default();
        assert_eq!(t.fenced(7, Now::same(0)), Fence::Ok);
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(t.fenced(7, Now::same(50)), Fence::Ok);
        assert_eq!(t.fenced(7, Now::same(100)), Fence::Fenced);
        // Another inode without local locks is not fenced.
        assert_eq!(t.fenced(8, Now::same(100)), Fence::Ok);
        assert!(t.local_unlock(7, 1, 0, 10, Now::same(100)));
        assert_eq!(t.fenced(7, Now::same(100)), Fence::Ok);
    }

    #[test]
    fn a_grant_that_ends_without_its_release_taints_the_dirty_data() {
        let t = LockTables::default();
        // Nothing held, locked or tainted: nothing to discard.
        assert_eq!(t.take_discard(7, Now::same(0), false), Discard::Publish);
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        // Honoured: publish.
        assert_eq!(t.take_discard(7, Now::same(50), false), Discard::Publish);
        // Lapsed with the lock still held: fenced (discard, EIO even with
        // nothing dirty) — checked before the close drops the owner.
        assert_eq!(
            t.take_discard(7, Now::same(100), false),
            Discard::Discard { fenced: true }
        );
        assert!(t.held(7).is_none(), "the lapsed grant is dropped");
        // The close drops the owner under the fence: tainted, so the
        // release that follows discards too — but owes no EIO of its own.
        assert!(t.local_release_owner(7, 1, Now::same(100)));
        assert_eq!(
            t.take_discard(7, Now::same(100), false),
            Discard::Discard { fenced: false }
        );
        assert_eq!(t.take_discard(7, Now::same(100), false), Discard::Publish);

        // An explicit unlock after the lapse, then the close.
        t.install_held(
            8,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Exclusive, 200)
            },
        );
        assert_eq!(
            t.local_set(8, lk(1, true, 0, 10), Now::same(150)),
            LocalOutcome::Done
        );
        assert!(t.local_unlock(8, 1, 0, 10, Now::same(250)));
        assert_eq!(
            t.fenced(8, Now::same(250)),
            Fence::Ok,
            "the fence lifts with the last lock"
        );
        assert_eq!(
            t.take_discard(8, Now::same(250), false),
            Discard::Discard { fenced: false },
            "but the data stays tainted"
        );

        // A cached grant with no local lock (unlocked, file still open)
        // that lapses: found at the next publication point.
        t.install_held(
            9,
            HeldGrant {
                id: GrantId { node: 1, seq: 3 },
                ..held(LockMode::Exclusive, 300)
            },
        );
        assert_eq!(
            t.local_set(9, lk(1, true, 0, 10), Now::same(250)),
            LocalOutcome::Done
        );
        assert!(t.local_unlock(9, 1, 0, 10, Now::same(260)));
        assert_eq!(t.take_discard(9, Now::same(299), false), Discard::Publish);
        assert_eq!(
            t.take_discard(9, Now::same(300), false),
            Discard::Discard { fenced: false }
        );

        // Lost at a renewal, or dropped lapsed by the tick: tainted.
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(
            10,
            HeldGrant {
                id,
                ..held(LockMode::Shared, 400)
            },
        );
        assert!(t.lost(10, id, Now::same(0)));
        assert_eq!(
            t.take_discard(10, Now::same(0), false),
            Discard::Discard { fenced: false }
        );
        let id2 = GrantId { node: 1, seq: 4 };
        t.install_held(
            11,
            HeldGrant {
                id: id2,
                ..held(LockMode::Shared, 400)
            },
        );
        assert!(t.drop_held(11, id2));
        assert_eq!(
            t.take_discard(11, Now::same(0), false),
            Discard::Discard { fenced: false }
        );
        assert_eq!(t.take_discard(11, Now::same(0), false), Discard::Publish);
    }

    fn lk_pid(owner: u64, pid: u32) -> LocalLock {
        LocalLock {
            owner,
            pid,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        }
    }

    /// The owner fence: a lock owner whose grant lapsed under its local
    /// lock is fenced (everywhere — the caller checks every op against
    /// [`LockTables::fenced_owners`]) until its own locks are gone; a new
    /// grant on the file does not lift it, other owners are untouched.
    #[test]
    fn a_lapsed_owner_is_fenced_until_its_locks_are_gone() {
        let t = LockTables::default();
        assert!(
            !t.owner_fence_armed(Now::same(0)),
            "no local lock: one load"
        );
        t.install_held(7, held(LockMode::Exclusive, 100));
        t.install_held(
            8,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Exclusive, 1_000)
            },
        );
        assert_eq!(
            t.local_set(7, lk_pid(1, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(8, lk_pid(2, 20), Now::same(0)),
            LocalOutcome::Done
        );
        // Both honoured: not armed (two loads), nobody fenced.
        assert!(!t.owner_fence_armed(Now::same(99)));
        assert!(t.fenced_owners(Now::same(99)).is_empty());
        // Ino 7's grant lapses: its owner is fenced, the other is not.
        assert!(t.owner_fence_armed(Now::same(100)));
        let f = t.fenced_owners(Now::same(100));
        assert_eq!(
            f,
            vec![FencedOwner {
                owner: 1,
                pid: 10,
                pid_start: 0,
                since: HonourMs(100),
                grant: Some(GrantId { node: 1, seq: 1 }),
            }]
        );
        assert_eq!(t.stats().owners_fenced, 1);
        // A new grant on ino 7 (another local owner's request) lifts the
        // per-inode fence, not the owner's: the lock was not held
        // throughout.
        t.install_held(
            7,
            HeldGrant {
                id: GrantId { node: 1, seq: 3 },
                installed: HonourMs(150),
                ..held(LockMode::Exclusive, 900)
            },
        );
        assert_eq!(t.fenced(7, Now::same(150)), Fence::Ok);
        assert_eq!(t.fenced_owners(Now::same(150)).len(), 1);
        // Owner 2's unlock elsewhere changes nothing for owner 1.
        assert!(t.local_unlock(8, 2, 0, u64::MAX, Now::same(160)));
        assert_eq!(t.fenced_owners(Now::same(160)).len(), 1);
        // Owner 1's unlock lifts it; with every remaining grant honoured
        // the fast path is back to "not armed".
        assert!(t.local_unlock(7, 1, 0, u64::MAX, Now::same(170)));
        assert!(t.fenced_owners(Now::same(170)).is_empty());
        assert!(!t.owner_fence_armed(Now::same(170)));
    }

    /// A grant that is lost (the owner forgot it), or dropped lapsed by
    /// the renewal tick, fences its owners at once; a close lifts it.
    #[test]
    fn a_lost_or_dropped_grant_fences_its_owners_at_once() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.install_held(7, held(LockMode::Exclusive, 10_000));
        assert_eq!(
            t.local_set(7, lk_pid(1, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk_pid(3, 10), Now::same(0)),
            LocalOutcome::Conflict(lk_pid(1, 10))
        );
        assert!(t.lost(7, id, Now::same(0)));
        assert!(
            t.owner_fence_armed(Now::same(1)),
            "armed at once, not at the ttl"
        );
        assert_eq!(t.fenced_owners(Now::same(1)).len(), 1);
        assert!(
            !t.local_release_owner(7, 9, Now::same(2)),
            "another owner's close"
        );
        assert_eq!(t.fenced_owners(Now::same(2)).len(), 1);
        assert!(t.local_release_owner(7, 1, Now::same(3)));
        assert!(t.fenced_owners(Now::same(3)).is_empty());

        let id2 = GrantId { node: 1, seq: 2 };
        t.install_held(
            8,
            HeldGrant {
                id: id2,
                ..held(LockMode::Shared, 10_000)
            },
        );
        assert_eq!(
            t.local_set(
                8,
                LocalLock {
                    write: false,
                    ..lk_pid(4, 40)
                },
                Now::same(0)
            ),
            LocalOutcome::Done
        );
        assert!(t.drop_held(8, id2));
        assert_eq!(
            t.fenced_owners(Now::same(5)).first().map(|f| f.pid),
            Some(40)
        );
    }

    /// The carried review item: a recalled grant waiting for its first
    /// local lock is renewed only for the first-use budget, and released
    /// once it ran out (or at once when the requester gives up), so the
    /// owner's waiters are not held for ever.
    #[test]
    fn a_recalled_grant_waits_for_its_first_use_only_so_long() {
        let t = LockTables::default();
        t.set_first_use_budget_ms(1_000);
        let id = GrantId { node: 1, seq: 1 };
        t.note_pending_recall(7, id);
        let mut h = held(LockMode::Exclusive, 10_000);
        h.renew_at.0 = 100;
        assert_eq!(t.install_held(7, h), Installed::Ok { recalled: true });
        assert!(t.begin_release(7).is_none(), "pinned by its first use");
        // Inside the budget: renewed.
        assert_eq!(t.next_renewal(Now::same(200)), Some(HonourMs(100)));
        assert_eq!(t.due_renewals(Now::same(200)).len(), 1);
        t.renewed(
            7,
            id,
            id,
            LockMode::Exclusive,
            HonourMs(200),
            10_000,
            0,
            true,
            1,
            Now::same(0),
        );
        // Past it: not renewed, and the tick's sweep unpins it.
        assert!(t.due_renewals(Now::same(5_000)).is_empty());
        assert_eq!(t.next_renewal(Now::same(1_000)), None);
        assert!(t.expire_first_use(Now::same(999)).is_empty());
        assert_eq!(t.expire_first_use(Now::same(1_000)), vec![7]);
        assert_eq!(t.stats().first_use_abandoned, 1);
        assert!(t.begin_release(7).is_some());
        assert!(t.end_release(7, id));

        // The requester gives up before the budget: released at once.
        let id2 = GrantId { node: 1, seq: 2 };
        t.note_pending_recall(8, id2);
        assert_eq!(
            t.install_held(
                8,
                HeldGrant {
                    id: id2,
                    ..held(LockMode::Exclusive, 10_000)
                }
            ),
            Installed::Ok { recalled: true }
        );
        assert!(
            t.abandon_first_use(8, Now::same(50)),
            "recalled and idle: release it"
        );
        assert!(!t.abandon_first_use(8, Now::same(50)), "once");
        assert!(t.begin_release(8).is_some());
        // Not recalled: the grant stays cached, just no longer pinned.
        t.install_held(
            9,
            HeldGrant {
                id: GrantId { node: 1, seq: 3 },
                ..held(LockMode::Exclusive, 10_000)
            },
        );
        assert!(!t.abandon_first_use(9, Now::same(60)));
        assert!(!t.held(9).unwrap().first_use);
        // ... and idle from the give-up: the idle sweep reclaims it.
        assert_eq!(t.held(9).unwrap().idle_since, Some(WaitMs(60)));
        assert!(t.idle_before(WaitMs(60)).iter().all(|(ino, _)| *ino != 9));
        assert!(t.idle_before(WaitMs(61)).iter().any(|(ino, _)| *ino == 9));
    }

    #[test]
    fn a_released_grant_leaves_nothing_to_discard_and_discard_errors_are_sequenced() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert!(t.local_unlock(7, 1, 0, 10, Now::same(10)));
        assert_eq!(t.recall_held(7, id), Some(false));
        assert!(t.begin_release(7).is_some());
        assert!(t.end_release(7, id));
        assert_eq!(t.take_discard(7, Now::same(500), false), Discard::Publish);
        assert_eq!(t.error_seq(7), 0);
        let seq = t.note_discard(7);
        assert_eq!(
            t.take_discard(7, Now::same(500), false),
            Discard::Publish,
            "an error event is not a discard"
        );
        assert_eq!(t.error_seq(7), seq);
        assert_eq!(t.error_seq(8), 0, "events are per inode");
        let later = t.note_discard(7);
        assert!(later > seq, "every event is newer than the last");
        t.forget_errors(7);
        assert_eq!(t.error_seq(7), 0);
        // A handover: the next process's tables adopt the events, and
        // number the next one above everything that crossed.
        let next = LockTables::default();
        next.import_errors(&[(9, later)], later + 5);
        assert_eq!(next.error_seq(9), later);
        assert!(next.note_discard(7) > later + 5);
    }

    #[test]
    fn a_pending_recall_installs_the_grant_recalled() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.note_pending_recall(7, id);
        assert_eq!(
            t.install_held(7, held(LockMode::Exclusive, 100)),
            Installed::Ok { recalled: true }
        );
        assert!(t.held(7).unwrap().recalled);
        // The request it was granted for takes its one local lock first;
        // the release waits for that lock's unlock.
        assert!(t.begin_release(7).is_none());
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(2, false, 20, 30), Now::same(0)),
            LocalOutcome::NeedGrant(LockMode::Shared)
        );
        assert!(t.begin_release(7).is_none());
        assert!(t.local_unlock(7, 1, 0, 10, Now::same(0)));
        assert!(t.begin_release(7).is_some());
        assert!(t.end_release(7, id));
        // Released: a reply for the same id that crosses the release is
        // refused.
        assert_eq!(
            t.install_held(7, held(LockMode::Exclusive, 100)),
            Installed::Released
        );
    }

    #[test]
    fn a_renewal_comes_inside_the_window() {
        // 5 s at a 1 s margin: honoured for 4 s, renewed at 2 s.
        assert_eq!(renew_point(HonourMs(100), 5_000, 1_000), HonourMs(2_100));
        // A delegate's short grant: honoured for 1.076 s, renewed at 538 ms
        // (at `ttl/2` it would have been after the window closed).
        assert_eq!(renew_point(HonourMs(0), 2_076, 1_000), HonourMs(538));
        for ttl in [1_000i64, 1_500, 2_000, 2_500, 5_000] {
            assert!(renew_point(HonourMs(0), ttl, 1_000) <= HonourMs((ttl - 1_000).max(0)));
        }
    }

    #[test]
    fn renewals_only_while_honoured() {
        let t = LockTables::default();
        let mut h = held(LockMode::Shared, 100);
        h.renew_at.0 = 50;
        t.install_held(7, h);
        assert!(t.due_renewals(Now::same(40)).is_empty());
        assert_eq!(t.due_renewals(Now::same(60)).len(), 1);
        // In flight: not again.
        assert!(t.due_renewals(Now::same(70)).is_empty());
        t.renewed(
            7,
            h.id,
            h.id,
            LockMode::Shared,
            HonourMs(60),
            100,
            10,
            false,
            1,
            Now::same(0),
        );
        assert_eq!(t.held(7).unwrap().until.0, 150);
        // Lapsed: never.
        assert!(t.due_renewals(Now::same(200)).is_empty());
    }

    /// Sim `locks-blips-tight` seed 568: a renewal of id 1 is answered
    /// "renewed as id 2" after a recall of id 2 already moved the held
    /// grant to id 2. The answer extends that grant; a stale answer for
    /// some other id still does not.
    #[test]
    fn a_renewal_answered_under_the_id_a_recall_moved_to_extends_the_grant() {
        let t = LockTables::default();
        let mut h = held(LockMode::Exclusive, 100);
        h.renew_at.0 = 50;
        t.install_held(7, h);
        assert_eq!(t.due_renewals(Now::same(60)).len(), 1);
        let newer = GrantId { node: 1, seq: 2 };
        assert_eq!(t.recall_held(7, newer), Some(false));
        assert_eq!(t.held(7).unwrap().id, newer);
        t.renewed(
            7,
            h.id,
            newer,
            LockMode::Exclusive,
            HonourMs(60),
            1_000,
            10,
            false,
            1,
            Now::same(0),
        );
        let now = t.held(7).unwrap();
        assert_eq!((now.id, now.until.0, now.recalled), (newer, 1_050, true));
        assert!(now.renewing.is_none());
        let other = GrantId { node: 1, seq: 3 };
        t.renewed(
            7,
            h.id,
            other,
            LockMode::Exclusive,
            HonourMs(500),
            1_000,
            10,
            false,
            1,
            Now::same(0),
        );
        assert_eq!(t.held(7).unwrap().until.0, 1_050, "not this grant's");
    }

    #[test]
    fn sequencer_table_conflicts_and_expiry() {
        let t = LockTables::default();
        let a = t.grant(9, 1, 7, LockMode::Shared, WaitMs(100), 0);
        let b = t.grant(9, 2, 7, LockMode::Shared, WaitMs(100), 0);
        assert!(t.conflicting(7, 3, LockMode::Shared, WaitMs(0)).is_empty());
        assert_eq!(t.conflicting(7, 3, LockMode::Exclusive, WaitMs(0)).len(), 2);
        // Node 1 upgrading: only node 2 is in the way.
        assert_eq!(
            t.conflicting(7, 1, LockMode::Exclusive, WaitMs(0)),
            vec![t.get(b).unwrap()]
        );
        assert!(t.mark_recalled(b));
        assert!(!t.mark_recalled(b));
        assert_eq!(t.expired(WaitMs(100)).len(), 2);
        assert!(t
            .conflicting(7, 3, LockMode::Exclusive, WaitMs(100))
            .is_empty());
        assert!(t.get(a).is_none());
        // A regrant to the same node replaces its entry.
        let c = t.grant(9, 1, 7, LockMode::Exclusive, WaitMs(300), 0);
        let d = t.grant(9, 1, 7, LockMode::Exclusive, WaitMs(300), 0);
        assert!(t.get(c).is_none());
        assert!(t.get(d).is_some());
        assert_eq!(t.grants_len(), 1);
    }
}

#[cfg(test)]
mod idle_tests {
    use super::*;

    #[test]
    fn an_idle_cache_is_marked_for_release() {
        let t = LockTables::default();
        let h = HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode: LockMode::Exclusive,
            until: HonourMs(1_000),
            renew_at: HonourMs(500),
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        };
        t.install_held(7, h);
        assert!(
            t.idle_before(WaitMs(100)).is_empty(),
            "never used: not idle"
        );
        let l = LocalLock {
            owner: 1,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: 10,
        };
        assert_eq!(t.local_set(7, l, Now::same(0)), LocalOutcome::Done);
        assert!(t.local_unlock(7, 1, 0, 10, Now::same(10)));
        assert!(t.idle_before(WaitMs(10)).is_empty());
        assert_eq!(t.idle_before(WaitMs(11)).len(), 1);
        assert!(t.held(7).unwrap().recalled);
        assert!(t.begin_release(7).is_some());
    }
}

#[cfg(test)]
mod supersede_tests {
    use super::*;

    fn held(seq: u64) -> HeldGrant {
        HeldGrant {
            id: GrantId { node: 1, seq },
            mode: LockMode::Exclusive,
            until: HonourMs(5_000),
            renew_at: HonourMs(2_500),
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        }
    }

    fn lock() -> LocalLock {
        LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        }
    }

    /// EC2 campaign 8: the owner re-affirmed the grant under a new id
    /// while the old one was on its way here; its recall names the new
    /// id. The held grant adopts it (and is recalled), so the release
    /// names the id the owner has — instead of the recall finding
    /// nothing and the cached grant serving local locks until it lapsed.
    #[test]
    fn a_recall_by_a_newer_id_of_the_same_owner_adopts_it() {
        let t = LockTables::default();
        assert!(matches!(t.install_held(7, held(4)), Installed::Ok { .. }));
        assert_eq!(t.local_set(7, lock(), Now::same(0)), LocalOutcome::Done);
        // Another owner's id, or an older one of ours: not this grant.
        assert!(t.recall_held(7, GrantId { node: 2, seq: 9 }).is_none());
        assert!(t.recall_held(7, GrantId { node: 1, seq: 3 }).is_none());
        assert!(!t.held(7).unwrap().recalled);
        assert_eq!(t.recall_held(7, GrantId { node: 1, seq: 6 }), Some(true));
        let h = t.held(7).unwrap();
        assert!(h.recalled);
        assert_eq!(h.id, GrantId { node: 1, seq: 6 });
        assert_eq!(t.stats().recalled_superseded, 1);
        // A late install of the old id is refused (released here).
        assert!(matches!(t.install_held(7, held(4)), Installed::Released));
        // Under a recalled grant a new owner needs a fresh grant.
        assert!(matches!(
            t.local_set(
                7,
                LocalLock {
                    owner: 10,
                    ..lock()
                },
                Now::same(0)
            ),
            LocalOutcome::Conflict(_)
        ));
        assert!(t.local_unlock(7, 9, 0, u64::MAX, Now::same(1)));
        assert_eq!(
            t.begin_release(7).map(|h| h.id),
            Some(GrantId { node: 1, seq: 6 })
        );
    }

    /// A recall that adopts a newer id raises the newest id the node has
    /// seen of that owner: after the adopted id is released, a reply that
    /// carries an id in between (the recall overtook it) is refused, not
    /// installed beside the owner's next grantee (two exclusive holders).
    #[test]
    fn a_reply_older_than_an_adopted_and_released_id_is_refused() {
        let t = LockTables::default();
        assert!(matches!(t.install_held(7, held(4)), Installed::Ok { .. }));
        // The first use of the grant, then idle.
        assert_eq!(t.local_set(7, lock(), Now::same(0)), LocalOutcome::Done);
        assert!(t.local_unlock(7, 9, 0, u64::MAX, Now::same(1)));
        // The recall of id 6 overtook the reply that carries 5.
        assert_eq!(t.recall_held(7, GrantId { node: 1, seq: 6 }), Some(false));
        let h = t.begin_release(7).expect("idle and recalled: released");
        assert_eq!(h.id, GrantId { node: 1, seq: 6 });
        assert!(t.end_release(7, h.id));
        assert!(t.held(7).is_none());
        assert!(matches!(t.install_held(7, held(5)), Installed::Released));
        assert!(
            t.held(7).is_none(),
            "nothing installed beside the owner's next grantee"
        );
        // A newer id of the owner is still installed.
        assert!(matches!(t.install_held(7, held(7)), Installed::Ok { .. }));
    }

    /// The owner's newer id arriving as a reply (the request a push had
    /// answered) merges: the local lock continues under it, `first_use`
    /// and the recalled flag are the held grant's, the old id is dead.
    #[test]
    fn a_newer_id_of_the_same_owner_merges_the_local_state() {
        let t = LockTables::default();
        assert!(matches!(t.install_held(7, held(4)), Installed::Ok { .. }));
        assert_eq!(t.local_set(7, lock(), Now::same(0)), LocalOutcome::Done);
        assert_eq!(t.recall_held(7, GrantId { node: 1, seq: 4 }), Some(true));
        let newer = HeldGrant {
            until: HonourMs(9_000),
            ..held(5)
        };
        assert!(matches!(
            t.install_held(7, newer),
            Installed::Ok { recalled: true }
        ));
        let h = t.held(7).unwrap();
        assert_eq!(h.id, GrantId { node: 1, seq: 5 });
        assert!(h.recalled && !h.first_use);
        assert_eq!(h.until.0, 9_000);
        assert!(matches!(t.install_held(7, held(4)), Installed::Released));
        // A different owner's grant on the inode is a fresh one.
        let other = HeldGrant {
            id: GrantId { node: 2, seq: 1 },
            owner: 2,
            ..held(1)
        };
        assert!(matches!(t.install_held(7, other), Installed::Ok { .. }));
        assert!(t.held(7).unwrap().first_use);
    }
}

#[cfg(test)]
mod token_tests {
    use super::*;
    use constellation_types::clock::Ms;

    fn held(mode: LockMode, until: i64) -> HeldGrant {
        HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode,
            until: HonourMs(until),
            renew_at: HonourMs(until / 2),
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        }
    }

    fn lk(owner: u64, write: bool, start: u64, end: u64) -> LocalLock {
        LocalLock {
            owner,
            pid: 1,
            pid_start: 0,
            write,
            start,
            end,
        }
    }

    // ---- plan 30 §M14 phase 2: the fencing token ----

    /// The executor's check. A grant live in this table (the minter's)
    /// passes whatever the token's window says: the holder renewed it,
    /// so no conflicting grant exists (lock-fence-token review must-fix
    /// 2). One this table does not have is judged by the window; one it
    /// ended — released, or outwaited (its record expired) — is dead at
    /// once and stays dead for `TTL + 2 × margin` past its record (review
    /// should-fix 3: an outwaited id used to be forgotten at the first
    /// prune). A move to a delegate is not an end.
    #[test]
    fn a_token_dies_with_its_grant_or_its_window_where_the_grant_is_unknown() {
        let t = LockTables::default();
        t.set_token_memory_ms(3_000);
        let id = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(1_000), 0);
        let tag = LockTag(vec![LockToken::window(id, 500)]);
        assert_eq!(t.check_tag(&LockTag::NONE, Now::same(i64::MAX)), Ok(()));
        assert_eq!(t.check_tag(&tag, Now::same(499)), Ok(()));
        assert_eq!(
            t.check_tag(&tag, Now::same(700)),
            Ok(()),
            "live here: exact"
        );
        // Moved away (a delegation): not ended; the window judges.
        let moved = t.take_where(|_| true);
        assert_eq!(moved.len(), 1);
        assert_eq!(t.check_tag(&tag, Now::same(100)), Ok(()));
        assert!(
            t.check_tag(&tag, Now::same(500)).is_err(),
            "unknown here, at the window: dead"
        );
        t.install(moved[0]);
        // Released: dead at once, whatever the window says.
        assert!(t.forget(id).is_some());
        assert!(t.check_tag(&tag, Now::same(100)).is_err());
        // Outwaited: the record expired (a conflicting request found it
        // so); a token whose window still runs is refused all the same,
        // through 257 other ends (the prune) and for the memory's span.
        let out = t.grant(1, 3, 8, LockMode::Exclusive, WaitMs(2_000), 0);
        let late = LockTag(vec![LockToken::window(out, 9_000)]);
        assert!(t
            .conflicting(8, 4, LockMode::Exclusive, WaitMs(2_000))
            .is_empty());
        for k in 0..300 {
            let g = t.grant(1, 10 + k, 100 + k, LockMode::Shared, WaitMs(2_100), 0);
            t.forget(g);
        }
        assert!(
            t.check_tag(&late, Now::same(4_000)).is_err(),
            "outwaited: remembered"
        );
        assert!(t.check_tag(&late, Now::same(4_999)).is_err());
        // Past `until + memory` only the window is left.
        assert_eq!(t.check_tag(&late, Now::same(5_001)), Ok(()));
        let s = t.stats();
        assert_eq!(s.token_rejections, 4);
    }

    /// Review round 2, must-fix 2: a grant installed from a copy (a
    /// backup's mirror, a root's reinstated handoff copy) may be one its
    /// last sequencer ended, so it passes a token only inside its window
    /// until its holder renews it here; from that renewal on, a token
    /// whose window ends after it passes whatever the window, an older
    /// one still only inside it.
    #[test]
    fn an_installed_copy_is_judged_by_the_window_until_its_holder_renews() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 9 };
        let copy = Grant {
            id,
            node: 2,
            ino: 7,
            mode: LockMode::Exclusive,
            until: WaitMs(10_000),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        assert!(t.install_if_consistent(copy, WaitMs(0)));
        let old = LockTag(vec![LockToken::window(id, 500)]);
        assert_eq!(t.check_tag(&old, Now::same(499)), Ok(()));
        assert!(
            t.check_tag(&old, Now::same(500)).is_err(),
            "unconfirmed: the window"
        );
        assert_eq!(
            t.extend(id, 2, WaitMs(12_000), (1, 1_000)),
            Some((LockMode::Exclusive, false))
        );
        assert_eq!(t.get(id).unwrap().confirmed, Some((1, 1_000)));
        assert!(
            t.check_tag(&old, Now::same(1_200)).is_err(),
            "sent before the renewal"
        );
        let new = LockTag(vec![LockToken::window(id, 1_500)]);
        assert_eq!(
            t.check_tag(&new, Now::same(3_000)),
            Ok(()),
            "confirmed: exact"
        );
        // A later renewal never moves the confirmation forward.
        t.extend(id, 2, WaitMs(13_000), (1, 2_000));
        assert_eq!(t.get(id).unwrap().confirmed, Some((1, 1_000)));
        // A grant made here is confirmed from the start.
        let made = t.grant(1, 3, 8, LockMode::Exclusive, WaitMs(10_000), 0);
        assert_eq!(t.get(made).unwrap().confirmed, Grant::MINTED);
    }

    /// A token is taken when its op is sent: a renewal since the op began
    /// moves its window on (`refresh_tag`); a grant no longer honoured
    /// here keeps the old window (the executor judges it).
    #[test]
    fn a_tokens_window_is_refreshed_from_the_held_grant() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 1_000));
        let id = GrantId { node: 1, seq: 1 };
        let old = LockTag(vec![LockToken::window(id, 600)]);
        let fresh = t.refresh_tag(&old, Now::same(500));
        assert_eq!(fresh.0[0].until_ms, 1_000);
        assert_eq!((fresh.0[0].until_h, fresh.0[0].remaining_ms), (1_000, 500));
        assert_eq!(fresh.0[0].recv, Some(500), "received here, now");
        assert!(t.tag_honoured(&fresh, Now::same(500)));
        // Past the held window: not honoured, nothing to refresh from; the
        // old window stays (the executor judges it), with what is left of
        // it, none.
        let stale = t.refresh_tag(&old, Now::same(1_000));
        assert_eq!(
            (
                stale.0[0].until_ms,
                stale.0[0].until_h,
                stale.0[0].remaining_ms
            ),
            (600, 600, 0)
        );
        assert!(!t.tag_honoured(&old, Now::same(1_000)));
        let other = LockTag(vec![LockToken::window(GrantId { node: 1, seq: 9 }, 600)]);
        let kept = t.refresh_tag(&other, Now::same(500));
        assert_eq!((kept.0[0].until_ms, kept.0[0].remaining_ms), (600, 100));
        assert!(!t.tag_honoured(&other, Now::same(500)));
    }

    /// The release ordering's bookkeeping does not leak: an entry pinned
    /// by an op left in doubt goes once its pin is over, at the next
    /// op's start, even if no release ever asks about its grant (review
    /// should-fix 4). The last op of a recalled grant wakes its release.
    #[test]
    fn in_flight_pins_expire_and_the_last_op_wakes_the_release() {
        let t = LockTables::default();
        let woken = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let w = woken.clone();
        t.set_release_wake(std::sync::Arc::new(move |ino| w.lock().unwrap().push(ino)));
        let lapsed = GrantId { node: 1, seq: 7 };
        let tag = |grant, until_ms| LockTag(vec![LockToken::window(grant, until_ms)]);
        t.tag_begin(&tag(lapsed, 300), Now::same(0));
        t.tag_end(&tag(lapsed, 300), true, Now::same(10));
        assert_eq!(t.lock().inflight.len(), 1, "pinned");
        // Another grant's op, after the pin: the stale entry goes.
        t.install_held(
            7,
            HeldGrant {
                recalled: true,
                ..held(LockMode::Exclusive, 1_000)
            },
        );
        let live = GrantId { node: 1, seq: 1 };
        t.tag_begin(&tag(live, 900), Now::same(400));
        assert_eq!(t.lock().inflight.len(), 1, "only the live grant's");
        assert!(woken.lock().unwrap().is_empty());
        t.tag_end(&tag(live, 900), false, Now::same(410));
        assert_eq!(
            *woken.lock().unwrap(),
            vec![7],
            "the recalled grant's release"
        );
    }

    /// The node side: an owner's tag names, per minter, the earliest
    /// honoured window among its locks' grants; an owner with a lock
    /// under no honoured, covering grant gets none (it is fenced).
    #[test]
    fn an_owners_tag_is_the_earliest_window_per_minter() {
        let t = LockTables::default();
        let g = |node, seq, mode, until| HeldGrant {
            id: GrantId { node, seq },
            ..held(mode, until)
        };
        t.install_held(7, g(1, 1, LockMode::Exclusive, 900));
        t.install_held(8, g(1, 2, LockMode::Exclusive, 600));
        t.install_held(9, g(3, 1, LockMode::Shared, 800));
        assert_eq!(
            t.local_set(7, lk(10, true, 0, u64::MAX), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(8, lk(10, true, 0, u64::MAX), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(9, lk(10, false, 0, u64::MAX), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(11, false, 0, 0), Now::same(0)),
            LocalOutcome::Conflict(lk(10, true, 0, u64::MAX))
        );
        assert_eq!(t.owner_tag(&[], Now::same(0), 7), Ok(LockTag::NONE));
        assert_eq!(t.owner_tag(&[12], Now::same(0), 7), Ok(LockTag(Vec::new())));
        let tag = t.owner_tag(&[10], Now::same(100), 7).unwrap();
        // Minted at honour reading 100 on clock 7: the window's end on both
        // readings, the time left, and received here, now.
        let minted = |grant, until: i64| LockToken {
            grant,
            until_ms: until,
            until_h: until,
            clock: 7,
            remaining_ms: until - 100,
            recv: Some(100),
        };
        assert_eq!(
            tag.0,
            vec![
                minted(GrantId { node: 1, seq: 2 }, 600),
                minted(GrantId { node: 3, seq: 1 }, 800),
            ]
        );
        assert_eq!(tag.until_ms(), 600);
        assert_eq!(
            LockTag::from_wire(&tag.to_wire(HonourMs(100)), Some(HonourMs(100))),
            tag
        );
        // Past one grant's window: no tag (fenced).
        assert_eq!(t.owner_tag(&[10], Now::same(600), 7), Err(OwnerFenced));
        let mut lockers = t.lockers();
        lockers.sort();
        assert_eq!(lockers, vec![(10, 1, 0)]);
    }

    /// Release ordering's bookkeeping: a grant is held back while a tagged
    /// op is in flight, and after one ended in doubt until its window.
    #[test]
    fn a_grant_with_tagged_ops_in_flight_is_held_back() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 4 };
        let tag = LockTag(vec![LockToken::window(id, 500)]);
        assert_eq!(t.release_blocked(id, Now::same(0)), None);
        t.tag_begin(&tag, Now::same(0));
        t.tag_begin(&tag, Now::same(0));
        assert_eq!(t.release_blocked(id, Now::same(0)), Some(None));
        t.tag_end(&tag, false, Now::same(10));
        assert_eq!(t.release_blocked(id, Now::same(10)), Some(None));
        t.tag_end(&tag, true, Now::same(20));
        assert_eq!(
            t.release_blocked(id, Now::same(20)),
            Some(Some(WaitMs(500)))
        );
        assert_eq!(t.release_blocked(id, Now::same(500)), None);
        assert_eq!(t.stats().tagged_ops, 2);
    }

    /// Review round 2, should-fix 1: owner 10's op under the grant ends
    /// in doubt, 10 unlocks, and owner 11 on the same node locks under the
    /// same held grant. The grant is live at its minter, so 10's op could
    /// land after 11's writes: 11's lock waits (`Predecessor`) until the
    /// op's window is over — from the moment it was found in doubt, before
    /// its answer went out. So does a lock behind a close's publication of
    /// the file (what the turn wrote, published after its locks went). An
    /// owner that joins a turn still holding a lock there does not wait.
    #[test]
    fn a_new_turn_waits_for_the_previous_turns_tagged_ops() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 10_000));
        let id = GrantId { node: 1, seq: 1 };
        let tag = LockTag(vec![LockToken::window(id, 500)]);
        assert_eq!(
            t.local_set(7, lk(10, true, 0, 9), Now::same(0)),
            LocalOutcome::Done
        );
        t.tag_begin(&tag, Now::same(0));
        t.publish_begin(id);
        assert_eq!(
            t.local_set(7, lk(11, false, 20, 29), Now::same(0)),
            LocalOutcome::Done,
            "a lock beside the turn's"
        );
        t.local_unlock(7, 11, 0, u64::MAX, Now::same(0));
        t.local_unlock(7, 10, 0, u64::MAX, Now::same(10));
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 9), Now::same(10)),
            LocalOutcome::Predecessor,
            "a close's publication in flight"
        );
        t.publish_end(id, Now::same(12));
        t.pin_in_doubt(&tag, None);
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 9), Now::same(15)),
            LocalOutcome::Predecessor,
            "in doubt, before the op's answer and scope end"
        );
        t.tag_end(&tag, true, Now::same(20));
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 9), Now::same(499)),
            LocalOutcome::Predecessor,
            "in doubt inside its window"
        );
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 9), Now::same(500)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.release_blocked(id, Now::same(500)),
            None,
            "nothing left behind"
        );
    }

    /// A tagged op still waiting for its answer when its owner's turn ends
    /// (another thread of the process: every op the process issues carries
    /// its grants) is concurrent with the unlock: the next turn does not
    /// wait for it — a non-blocking lock refused behind it answered
    /// `EAGAIN` with no lock in the way (xcheckfs: `setlk` OK on ext4,
    /// conflict here, under load) — but the release still does.
    #[test]
    fn a_new_turn_does_not_wait_for_an_unanswered_op() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 10_000));
        let id = GrantId { node: 1, seq: 1 };
        let tag = LockTag(vec![LockToken::window(id, 500)]);
        assert_eq!(
            t.local_set(7, lk(10, true, 0, 9), Now::same(0)),
            LocalOutcome::Done
        );
        t.tag_begin(&tag, Now::same(0));
        t.local_unlock(7, 10, 0, u64::MAX, Now::same(10));
        assert_eq!(
            t.release_blocked(id, Now::same(10)),
            Some(None),
            "the release waits"
        );
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 9), Now::same(10)),
            LocalOutcome::Done,
            "the next turn does not"
        );
        t.tag_end(&tag, false, Now::same(20));
        assert_eq!(t.release_blocked(id, Now::same(20)), None);
    }

    /// The review nit: a shared lock under an honoured shared grant is not
    /// fenced because another owner's exclusive lock on the same file lost
    /// its cover. (The owner replaced the node's exclusive grant with a
    /// fresh shared one before the exclusive one lapsed here — after a
    /// takeover, say: the shared owner was covered throughout.)
    #[test]
    fn only_the_uncovered_owner_is_fenced_on_a_shared_grant() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 200));
        assert_eq!(
            t.local_set(7, lk(10, true, 0, 9), Now::same(0)),
            LocalOutcome::Done
        );
        assert_eq!(
            t.local_set(7, lk(11, false, 20, 29), Now::same(0)),
            LocalOutcome::Done
        );
        t.install_held(
            7,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                installed: HonourMs(150),
                ..held(LockMode::Shared, 300)
            },
        );
        let fenced: Vec<u64> = t
            .fenced_owners(Now::same(160))
            .iter()
            .map(|f| f.owner)
            .collect();
        assert_eq!(fenced, vec![10], "the shared owner keeps its lock");
    }

    /// A reading with the wall and honour clocks apart (an executor whose
    /// wall clock is `wall`, whose honour clock reads `honour`).
    fn readings(wall: i64, honour: i64) -> Now {
        Now {
            wall: Ms(wall),
            honour: HonourMs(honour),
            ..Now::same(0)
        }
    }

    /// A token as an executor holds it that does not hold the grant: minted
    /// by a holder whose window ended at honour reading 50 000 of clock 9
    /// (wall stamp `until_ms`), with 3 s left when the op left it, received
    /// here at honour reading 70 000.
    fn foreign(until_ms: i64) -> LockTag {
        LockTag(vec![LockToken {
            grant: GrantId { node: 1, seq: 77 },
            until_ms,
            until_h: 50_000,
            clock: 9,
            remaining_ms: 3_000,
            recv: Some(70_000),
        }])
    }

    /// Plan 43 C7b, row 6, phase 1: an executor that does not hold the grant
    /// judges the token by its own honour clock, from its receipt plus the
    /// time the holder said it had left, whatever its wall clock says of the
    /// holder's. (Fails before: the token carried one wall stamp, and an
    /// executor whose wall clock was behind the holder's ran the op after
    /// the holder's window — and the minter's outwait — had ended.)
    #[test]
    fn a_token_dies_at_receipt_plus_remaining_on_the_honour_clock() {
        let t = LockTables::default();
        // The wall stamp is a minute ahead of the executor's wall clock (the
        // executor is behind the holder): the old check passes at any time.
        let tag = foreign(1_000_000);
        assert_eq!(t.check_tag(&tag, readings(10_000, 72_999)), Ok(()));
        assert!(
            t.check_tag(&tag, readings(10_000, 73_000)).is_err(),
            "receipt 70 000 + 3 000 left"
        );
        assert!(t.check_tag(&tag, readings(10_000, 90_000)).is_err());
    }

    /// The absolute stamp still refuses (phase 1 keeps both bounds): an
    /// executor whose wall clock is past the holder's stamp refuses a token
    /// whose remaining time says it has time left.
    #[test]
    fn the_absolute_stamp_still_refuses() {
        let t = LockTables::default();
        let tag = foreign(20_000);
        assert_eq!(t.check_tag(&tag, readings(19_999, 71_000)), Ok(()));
        assert!(t.check_tag(&tag, readings(20_000, 71_000)).is_err());
    }

    /// Plan 43 C7b, row 29: a copy installed without its holder is confirmed
    /// by the holder's next renewal, in the holder's own readings: a token
    /// passes exactly iff its window (the holder's `until_h`) ran past the
    /// renewal's send on the same clock. (Fails before: the renewal's
    /// arrival on the sequencer's wall clock was compared with the token's
    /// wall stamp.)
    #[test]
    fn confirmation_is_on_the_holders_clock() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 77 };
        let copy = Grant {
            id,
            node: 2,
            ino: 7,
            mode: LockMode::Exclusive,
            until: WaitMs(10_000),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        assert!(t.install_if_consistent(copy, WaitMs(0)));
        let token = |until_h: i64, clock: u64| {
            LockTag(vec![LockToken {
                grant: id,
                until_ms: 1_000_000,
                until_h,
                clock,
                remaining_ms: 0,
                recv: Some(0),
            }])
        };
        let now = readings(0, 1_000_000);
        // Unconfirmed: judged by the window, which is over (receipt 0, none left).
        assert!(t.check_tag(&token(90, 9), now).is_err());
        // The holder (clock 9) renews, sending at its honour reading 100.
        t.extend(id, 2, WaitMs(20_000), (9, 100));
        assert_eq!(t.get(id).unwrap().confirmed, Some((9, 100)));
        // A token whose window ran past that send is exact, whatever the
        // window; one that ended before it, or is of another clock, is not.
        assert_eq!(t.check_tag(&token(101, 9), now), Ok(()));
        assert!(t.check_tag(&token(100, 9), now).is_err());
        assert!(t.check_tag(&token(90, 9), now).is_err());
        assert!(
            t.check_tag(&token(101, 8), now).is_err(),
            "another clock's reading proves nothing"
        );
        // A later renewal of the same clock never moves the confirmation on.
        t.extend(id, 2, WaitMs(30_000), (9, 5_000));
        assert_eq!(t.get(id).unwrap().confirmed, Some((9, 100)));
    }

    /// Another clock id (a restarted holder) replaces the confirmation: a
    /// token of the earlier incarnation is unconfirmed again, a minted grant
    /// is confirmed for every clock.
    #[test]
    fn another_clock_is_unconfirmed() {
        let mut g = Grant {
            id: GrantId { node: 1, seq: 5 },
            node: 2,
            ino: 7,
            mode: LockMode::Shared,
            until: WaitMs(1),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        };
        let id = g.id;
        let token = |until_h: i64, clock: u64| LockToken {
            grant: id,
            until_ms: 0,
            until_h,
            clock,
            remaining_ms: 0,
            recv: None,
        };
        assert!(!g.confirms(&token(i64::MAX, 9)), "no renewal yet");
        // A renewal stamped with no clock (an unset certificate) confirms
        // nothing: `(0, _)` would be "minted", any clock.
        g.confirm(0, 100);
        assert_eq!(g.confirmed, Grant::UNCONFIRMED);
        assert!(!g.confirms(&token(101, 9)));
        g.confirm(9, 100);
        assert!(g.confirms(&token(101, 9)));
        g.confirm(10, 7);
        assert!(!g.confirms(&token(101, 9)), "replaced by the new clock");
        assert!(g.confirms(&token(8, 10)));
        g.confirmed = Grant::MINTED;
        g.confirm(11, 3);
        assert!(g.confirms(&token(i64::MIN + 1, 12)), "minted: any clock");
    }

    /// Plan 43 C7b, row 31: a token that came by the S3 inbox or the persisted
    /// replay queue has no receipt, so no window: it passes exactly (the
    /// grant live and confirmed in this table) or is refused, even before
    /// its holder's window ended. (Fails before: it was judged by the wall
    /// stamp, which says nothing of an op drained long after it was sent.)
    #[test]
    fn an_inbox_token_is_exact_or_refused() {
        let t = LockTables::default();
        let now = readings(10_000, 71_000);
        let wired = foreign(1_000_000).to_wire(HonourMs(70_000));
        let tag = LockTag::from_wire(&wired, None);
        assert_eq!(tag.0[0].recv, None);
        assert!(
            t.check_tag(&tag, now).is_err(),
            "not held here: no window to judge it by"
        );
        // The same token received over P2P at 70 000 is judged by the window.
        let p2p = LockTag::from_wire(&wired, Some(HonourMs(70_000)));
        assert_eq!(t.check_tag(&p2p, now), Ok(()));
        // Held and confirmed here: exact, with or without a receipt.
        let id = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(100_000), 0);
        let mut tag = tag;
        tag.0[0].grant = id;
        assert_eq!(t.check_tag(&tag, now), Ok(()));
        // Ended here: refused.
        t.forget(id);
        assert!(t.check_tag(&tag, now).is_err());
    }

    /// An op that sat in its sender's queue before it was sent has spent
    /// that time of the window: the wire form takes it off the remainder.
    #[test]
    fn the_wire_form_takes_off_the_time_spent_here() {
        let tag = LockTag(vec![LockToken {
            grant: GrantId { node: 1, seq: 1 },
            until_ms: 0,
            until_h: 5_000,
            clock: 9,
            remaining_ms: 2_000,
            recv: Some(3_000),
        }]);
        assert_eq!(tag.to_wire(HonourMs(3_000))[0].5, 2_000);
        assert_eq!(tag.to_wire(HonourMs(4_500))[0].5, 500);
        assert_eq!(tag.to_wire(HonourMs(9_000))[0].5, 0, "never negative");
    }

    /// Plan 43 C7b, row 31: a token with no receipt (a stranded op replayed
    /// from the persisted queue, an op drained from the S3 inbox) leaves with
    /// no time left, so the executor it is forwarded to, which stamps a
    /// receipt of its own, judges it exactly or refuses it. (Fails before:
    /// it left with the remainder its holder named when the op first left
    /// it, and the next executor honoured that afresh from its receipt,
    /// however long the op had waited: only the wall stamp refused it.)
    #[test]
    fn a_token_with_no_receipt_leaves_with_no_time_left() {
        let mut tag = foreign(1_000_000);
        tag.0[0].recv = None;
        let wired = tag.to_wire(HonourMs(70_000));
        assert_eq!(wired[0].5, 0);
        // Forwarded over P2P, received at 70 000: not held there, refused.
        let t = LockTables::default();
        let now = readings(10_000, 70_001);
        let fwd = LockTag::from_wire(&wired, Some(HonourMs(70_000)));
        assert!(t.check_tag(&fwd, now).is_err());
        // Held and confirmed there: exact.
        let id = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(100_000), 0);
        let mut fwd = fwd;
        fwd.0[0].grant = id;
        assert_eq!(t.check_tag(&fwd, now), Ok(()));
    }

    /// F1 with a token that has no receipt: it passed exactly, so it has no
    /// window; once its grant leaves the table without ending here (its
    /// generation moved away), the write in its transaction is unsettled,
    /// whatever the wall stamp says. (Fails before: the pin's honour bound
    /// was `i64::MAX`, so only the holder's wall stamp could end it.)
    #[test]
    fn a_pinned_write_without_a_receipt_is_unsettled_once_its_grant_moved() {
        let t = LockTables::default();
        t.set_clock(std::sync::Arc::new(|| readings(10_000, 70_000)));
        let id = t.grant(1, 2, 7, LockMode::Exclusive, WaitMs(100_000), 5);
        let mut tag = foreign(1_000_000);
        tag.0[0].grant = id;
        tag.0[0].recv = None;
        assert_eq!(t.check_tag(&tag, readings(10_000, 70_000)), Ok(()));
        // In its transaction: pinned.
        assert_eq!(t.check_tag_at_write(&tag, readings(10_000, 70_000)), Ok(()));
        assert!(!t.unsettled_writes(), "live here");
        assert_eq!(t.take_by_gen(5).len(), 1);
        assert!(t.unsettled_writes(), "moved: no window to wait by");
        t.unpin(&tag);
        assert!(!t.unsettled_writes());
    }

    /// The token's cost on the wire (postcard, as `MutateRequest` and the
    /// inbox carry it): one byte without a lock; one grant from a minter
    /// with a full-width node id, a seq in the millions, a unix-ms window
    /// on both readings, a clock id of full width and 20 s left, 39 bytes
    /// more (40 in all; plan 43 C7b added the honour end, the clock id and
    /// the time left to the 21 of the phase-2 token).
    #[test]
    fn a_token_costs_a_byte_without_a_lock_and_40_with_one() {
        let none = postcard::to_allocvec(&LockTag::NONE.to_wire(HonourMs(0))).unwrap();
        assert_eq!(none.len(), 1);
        let one = LockTag(vec![LockToken {
            grant: GrantId {
                node: u64::MAX - 7,
                seq: 3_000_000,
            },
            until_ms: 1_790_000_000_000,
            until_h: 1_790_000_000_000,
            clock: u64::MAX - 3,
            remaining_ms: 20_000,
            recv: Some(1_789_999_980_000),
        }]);
        let bytes = postcard::to_allocvec(&one.to_wire(HonourMs(1_789_999_980_000))).unwrap();
        assert_eq!(bytes.len(), 1 + 10 + 4 + 6 + 6 + 10 + 3, "{bytes:?}");
    }

    /// A fence's start is the grant's lapse, whichever path notices it
    /// (an op, the publish gate, the renewal tick, `status`): from then on
    /// every op of the owner is refused. A harness compares it with when
    /// another node got the lock.
    #[test]
    fn a_fence_starts_when_the_grant_lapsed_whoever_notices() {
        for path in 0..3 {
            let t = LockTables::default();
            t.install_held(7, held(LockMode::Exclusive, 100));
            assert_eq!(
                t.local_set(7, lk(10, true, 0, u64::MAX), Now::same(0)),
                LocalOutcome::Done
            );
            match path {
                0 => assert_eq!(
                    t.take_discard(7, Now::same(5_000), false),
                    Discard::Discard { fenced: true }
                ),
                1 => assert_eq!(t.fenced(7, Now::same(5_000)), Fence::Fenced),
                _ => assert_eq!(t.fenced_owners_snapshot(Now::same(5_000)).len(), 1),
            }
            let since: Vec<i64> = t
                .fenced_owners(Now::same(5_000))
                .iter()
                .map(|f| f.since.0)
                .collect();
            assert_eq!(since, vec![100], "path {path}");
        }
    }

    // ---- plan 43 C7-F F-c: a lapsed grant parks instead of fencing ----

    /// A table that parks lapses for 60 s (the core sets it from
    /// `lock_owner_wait_max_ms`).
    fn parking() -> LockTables {
        let t = LockTables::default();
        t.set_lapse_park_ms(60_000);
        t
    }

    /// An exclusive local lock of owner 10 under a grant honoured until 100.
    fn locked() -> LockTables {
        let t = parking();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(
            t.local_set(7, lk(10, true, 0, u64::MAX), Now::same(0)),
            LocalOutcome::Done
        );
        t
    }

    /// The park gates look only while a grant is lapsing or about to end:
    /// no table lookups on the hot path of a node that holds a lock, whose
    /// grant is far from its end (the lone-node lock-latency measurement:
    /// a write under a lock paid three table checks and clock readings per
    /// op before this).
    #[test]
    fn the_park_gates_look_only_near_a_grants_end() {
        let t = parking();
        // Nothing held: nothing can lapse.
        assert!(!t.may_park(Now::same(0)));
        t.install_held(7, held(LockMode::Exclusive, 1_000));
        assert!(!t.may_park(Now::same(0)));
        assert!(!t.may_park(Now::same(1_000 - PARK_LOOKAHEAD_MS - 1)));
        assert!(t.may_park(Now::same(1_000 - PARK_LOOKAHEAD_MS)));
        assert!(t.may_park(Now::same(1_500)), "past the end");
        // A renewal moves the end; the bound is recomputed once passed.
        t.install_held(
            7,
            HeldGrant {
                until: HonourMs(5_000),
                ..held(LockMode::Exclusive, 5_000)
            },
        );
        assert!(!t.may_park(Now::same(1_500)));
        assert!(!t.may_park(Now::same(4_000)));
        // A second, earlier grant is seen at once (never missed).
        t.install_held(
            8,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Shared, 4_200)
            },
        );
        assert!(t.may_park(Now::same(4_160)));
        // A lapsing grant keeps it open, however far the clock says the end is.
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        assert!(t.may_park(Now::same(0)));
    }

    /// Review of F-c: the frontend's wrap look (`may_park_est_ahead` with
    /// [`WRAP_LOOKAHEAD_MS`]) opens before the gate's own and stays open
    /// while a grant is lapsing; out of it nothing is wrapped. The estimate's
    /// offset follows the renewal tick (`lapse_tick`), not only a full
    /// reading near a grant's end.
    #[test]
    fn the_wrap_look_opens_before_the_gate_and_follows_the_tick() {
        let t = parking();
        t.install_held(7, held(LockMode::Exclusive, 5_000));
        assert!(!t.may_park(Now::same(0)));
        let ahead = |w: i64| t.may_park_est_ahead(WaitMs(w), WRAP_LOOKAHEAD_MS);
        assert!(!ahead(5_000 - WRAP_LOOKAHEAD_MS - 1));
        assert!(ahead(5_000 - WRAP_LOOKAHEAD_MS));
        assert!(!t.may_park_est(WaitMs(5_000 - WRAP_LOOKAHEAD_MS)));
        // The honour reading moved ahead of the wait reading (a suspend, a
        // forward step, drift): the tick carries the offset to the estimate.
        t.lapse_tick(Now {
            wall: Ms(0),
            honour: HonourMs(2_000),
            wait: WaitMs(0),
            ..Now::same(0)
        });
        assert!(ahead(5_000 - 2_000 - WRAP_LOOKAHEAD_MS));
        // Lapsing: open whatever the clock says.
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        assert!(t.may_park_est_ahead(WaitMs(0), WRAP_LOOKAHEAD_MS));
    }

    /// The fences answer `Parked` — nothing captured, nothing tainted —
    /// while the grant under a local lock is lapsing, on every path that
    /// used to fence: the inode's I/O, the lock owner's ops, the publish
    /// points, a new lock request. (Before F-c each of them fenced at the
    /// first look, `EIO`.)
    #[test]
    fn a_lapsed_grant_under_local_locks_parks_every_fence_instead_of_capturing() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(99)), Fence::Ok);
        assert!(!t.lapsing_any());
        assert_eq!(t.fenced(7, Now::same(100)), Fence::Parked);
        assert!(t.lapsing_any());
        assert!(t.lapsing(7));
        assert!(t.fenced_owners(Now::same(101)).is_empty(), "not captured");
        let parked = t.parked_owners(Now::same(101));
        assert_eq!(parked.len(), 1);
        assert_eq!((parked[0].owner, parked[0].since), (10, HonourMs(100)));
        assert_eq!(t.take_discard(7, Now::same(101), false), Discard::Parked);
        assert_eq!(t.take_discard(7, Now::same(101), true), Discard::Parked);
        assert_eq!(
            t.local_set(7, lk(11, true, 0, 5), Now::same(101)),
            LocalOutcome::Conflict(lk(10, true, 0, u64::MAX))
        );
        assert_eq!(
            t.local_set(7, lk(10, true, 0, 5), Now::same(101)),
            LocalOutcome::Parked,
            "a new request waits for the verdict, it does not ask for a grant"
        );
        let s = t.stats();
        assert_eq!((s.lapse_parks, s.fenced_io, s.owners_fenced), (1, 0, 0));
        assert!(t.held(7).is_some(), "the grant is kept");
    }

    /// Without the feature (the park bound unset: the rule before) the same
    /// lapse fences at the first look.
    #[test]
    fn a_lapse_is_fenced_at_once_without_a_park_bound() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 100));
        t.local_set(7, lk(10, true, 0, u64::MAX), Now::same(0));
        assert_eq!(t.fenced(7, Now::same(100)), Fence::Fenced);
        assert_eq!(t.fenced_owners(Now::same(100)).len(), 1);
    }

    /// The owner admitted a late renewal: the grant is honoured again, the
    /// fence is open, and nothing was captured or tainted.
    #[test]
    fn a_renewal_that_reopens_the_window_resumes_a_lapsing_grant() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        let id = t.held(7).unwrap().id;
        // The late renewal, sent at 130 (window 100, margin 10, ttl 100).
        t.renewed(
            7,
            id,
            id,
            LockMode::Exclusive,
            HonourMs(130),
            100,
            10,
            false,
            1,
            Now::same(135),
        );
        assert!(!t.lapsing_any());
        assert_eq!(t.fenced(7, Now::same(136)), Fence::Ok);
        assert!(t.fenced_owners(Now::same(136)).is_empty());
        assert_eq!(t.take_discard(7, Now::same(136), false), Discard::Publish);
        let s = t.stats();
        assert_eq!((s.lapse_parks, s.lapse_parks_resumed), (1, 1));
        assert_eq!(
            s.lapse_park_ms_max, 15,
            "from the first look, 120, to the answer, 135"
        );
    }

    /// An answer that comes too late to reopen the window (its send was
    /// before the lapse by more than its ttl) leaves the grant lapsing.
    #[test]
    fn a_renewal_that_does_not_reach_the_present_leaves_the_grant_lapsing() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(300)), Fence::Parked);
        let id = t.held(7).unwrap().id;
        t.renewed(
            7,
            id,
            id,
            LockMode::Exclusive,
            HonourMs(150),
            100,
            10,
            false,
            1,
            Now::same(300),
        );
        assert_eq!(t.fenced(7, Now::same(301)), Fence::Parked);
        assert_eq!(t.stats().lapse_parks_resumed, 0);
    }

    /// The owner refused (`Lost`): as before F-c — owners captured since
    /// the window's end, the data tainted, `EIO`.
    #[test]
    fn a_refused_late_renewal_fences_as_a_lapse_always_did() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        let id = t.held(7).unwrap().id;
        assert!(t.lost(7, id, Now::same(140)));
        assert!(!t.lapsing_any());
        assert_eq!(t.fenced(7, Now::same(141)), Fence::Fenced);
        let since: Vec<i64> = t
            .fenced_owners(Now::same(141))
            .iter()
            .map(|f| f.since.0)
            .collect();
        assert_eq!(since, vec![100], "since the lapse, not since the refusal");
        assert_eq!(
            t.take_discard(7, Now::same(141), false),
            Discard::Discard { fenced: true }
        );
        let s = t.stats();
        assert_eq!(
            (s.lapse_parks_refused, s.lost, s.lapse_park_ms_max),
            (1, 1, 20)
        );
    }

    /// No answer inside the bound: the lapse is handled as before (dropped,
    /// tainted, owners captured since the window's end), counted.
    #[test]
    fn a_park_past_its_bound_gives_up_and_fences() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        // The bound: 60 s from the first look.
        assert_eq!(t.fenced(7, Now::same(60_119)), Fence::Parked);
        assert_eq!(t.fenced(7, Now::same(60_120)), Fence::Fenced);
        assert!(t.held(7).is_none());
        let since: Vec<i64> = t
            .fenced_owners(Now::same(60_121))
            .iter()
            .map(|f| f.since.0)
            .collect();
        assert_eq!(since, vec![100]);
        let s = t.stats();
        assert_eq!((s.lapse_park_gave_up, s.lapse_parks_refused), (1, 0));
        assert_eq!(s.lapse_park_ms_max, 60_000);
    }

    /// A lapsed grant that pins nothing (no local lock, no write under it)
    /// is dropped as before: nothing waits for it.
    #[test]
    fn a_lapsed_grant_that_pins_nothing_is_dropped_as_before() {
        let t = parking();
        t.install_held(7, held(LockMode::Exclusive, 100));
        let tick = t.lapse_tick(Now::same(100));
        assert!(tick.late.is_empty() && tick.gave_up == 0);
        assert!(t.held(7).is_none());
        assert_eq!(t.stats().lapse_parks, 0);
    }

    /// A write under the grant pins it too (its dirty data waits for the
    /// verdict); the next write waits behind it, and so does a publish.
    #[test]
    fn a_write_under_a_grant_pins_it_across_a_lapse() {
        let t = parking();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.take_discard(7, Now::same(10), true), Discard::Publish);
        assert!(t.held(7).unwrap().written);
        assert_eq!(t.take_discard(7, Now::same(100), true), Discard::Parked);
        assert_eq!(t.take_discard(7, Now::same(100), false), Discard::Parked);
        assert_eq!(
            t.fenced(7, Now::same(100)),
            Fence::Ok,
            "no local lock: not fenced I/O"
        );
        let id = t.held(7).unwrap().id;
        t.renewed(
            7,
            id,
            id,
            LockMode::Exclusive,
            HonourMs(110),
            100,
            10,
            false,
            1,
            Now::same(112),
        );
        assert_eq!(t.take_discard(7, Now::same(113), true), Discard::Publish);
    }

    /// The renewal tick: a lapsing grant is asked for once per renewal
    /// (marked renewing), again once the renewal failed; one past the bound
    /// gives up.
    #[test]
    fn the_tick_asks_for_one_late_renewal_at_a_time_and_gives_up_at_the_bound() {
        let t = locked();
        let tick = t.lapse_tick(Now::same(120));
        assert_eq!(tick.late.len(), 1);
        assert_eq!(t.lapse_tick(Now::same(121)).late.len(), 0, "one in flight");
        let id = t.held(7).unwrap().id;
        t.renewal_failed(7, id);
        assert_eq!(t.lapse_tick(Now::same(150)).late.len(), 1);
        let tick = t.lapse_tick(Now::same(60_121));
        assert_eq!((tick.late.len(), tick.gave_up), (0, 1));
        assert!(t.held(7).is_none());
    }

    /// The last unlock under a lapsing grant that nothing else pins drops
    /// it quietly (nothing to protect, nothing tainted); with a write under
    /// it, it stays lapsing for the verdict.
    #[test]
    fn unlocking_under_a_lapsing_grant_taints_nothing_and_drops_what_pins_nothing() {
        let t = locked();
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        assert!(t.local_unlock(7, 10, 0, u64::MAX, Now::same(125)));
        assert!(t.held(7).is_none(), "nothing pins it any more");
        assert_eq!(
            t.take_discard(7, Now::same(126), false),
            Discard::Publish,
            "no taint"
        );

        let t = locked();
        assert_eq!(t.take_discard(7, Now::same(10), true), Discard::Publish);
        assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
        assert!(t.local_unlock(7, 10, 0, u64::MAX, Now::same(125)));
        assert!(t.held(7).is_some(), "written data keeps it lapsing");
        assert_eq!(t.take_discard(7, Now::same(126), false), Discard::Parked);
    }

    /// A recall of a lapsing grant marks it recalled and the release waits
    /// for the verdict (`begin_release`); an idle cache is not released.
    #[test]
    fn a_lapsing_grant_is_not_released_before_its_verdict() {
        let t = parking();
        t.install_held(7, held(LockMode::Exclusive, 100));
        t.take_discard(7, Now::same(10), true);
        t.abandon_first_use(7, Now::same(10));
        assert_eq!(t.recall_held(7, t.held(7).unwrap().id), Some(false));
        assert_eq!(t.take_discard(7, Now::same(100), false), Discard::Parked);
        assert!(t.begin_release(7).is_none(), "the verdict first");
        let id = t.held(7).unwrap().id;
        t.renewed(
            7,
            id,
            id,
            LockMode::Exclusive,
            HonourMs(110),
            100,
            10,
            true,
            1,
            Now::same(112),
        );
        assert!(t.held(7).unwrap().recalled);
        assert!(
            t.begin_release(7).is_some(),
            "then the recall's release goes"
        );
    }

    // ---- the owner's memory of lapsed records and the late admission ----

    fn mint(t: &LockTables, node: u64, ino: u64, mode: LockMode, until: i64) -> GrantId {
        t.grant(1, node, ino, mode, WaitMs(until), 0)
    }

    /// Another node's request drops the expired record (`conflicting`).
    fn outwait(t: &LockTables, ino: u64, now: i64) {
        t.conflicting(ino, 99, LockMode::Shared, WaitMs(now));
    }

    fn admit(
        t: &LockTables,
        id: GrantId,
        node: u64,
        mode: LockMode,
        now: i64,
    ) -> Option<LateAdmitted> {
        t.admit_late(
            id,
            node,
            mode,
            WaitMs(now + 120),
            false,
            Now::same(now),
            (1, now),
        )
    }

    /// The record is still in the table, expired and not dropped: a late
    /// renewal extends it (looked up before anything drops it), is confirmed
    /// from the admission so an older token is judged by its window, and
    /// forgets the id's end.
    #[test]
    fn a_late_renewal_of_a_settled_record_still_in_the_table_is_admitted() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Exclusive, 100);
        let a = admit(&t, id, 2, LockMode::Exclusive, 150).expect("admitted");
        assert!(!a.from_memory);
        let g = t.get(id).unwrap();
        assert_eq!(
            (g.until, g.confirmed, g.settled),
            (WaitMs(270), Some((1, 150)), true)
        );
        assert_eq!(t.stats().late_admitted, 1);
        // A token whose window ended at the lapse is refused now, one past
        // the admission passes.
        let old = LockTag(vec![LockToken::window(id, 120)]);
        assert!(t.check_tag(&old, Now::same(160)).is_err());
        let fresh = LockTag(vec![LockToken::window(id, 300)]);
        assert!(t.check_tag(&fresh, Now::same(160)).is_ok());
    }

    /// Dropped by an outwait (another node asked): remembered, and a late
    /// renewal is admitted from the memory — the record is live again, no
    /// longer remembered as ended.
    #[test]
    fn a_late_renewal_of_a_dropped_settled_record_is_admitted_from_its_memory() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Exclusive, 100);
        outwait(&t, 7, 150);
        assert!(t.get(id).is_none() && t.was_ended(id));
        assert_eq!(t.lapsed_len(), 1);
        let a = admit(&t, id, 2, LockMode::Exclusive, 160).expect("admitted");
        assert!(a.from_memory);
        assert_eq!(t.lapsed_len(), 0);
        assert!(!t.was_ended(id));
        let g = t.get(id).unwrap();
        assert_eq!((g.until, g.confirmed), (WaitMs(280), Some((1, 160))));
        assert!(
            t.conflicting(7, 3, LockMode::Shared, WaitMs(170)).len() == 1,
            "live again"
        );
    }

    /// An outwait by the recall's timer (`forget_outwaited`) remembers too;
    /// a release (`forget`) does not.
    #[test]
    fn an_outwait_is_remembered_and_a_release_is_not() {
        let t = LockTables::default();
        let a = mint(&t, 2, 7, LockMode::Exclusive, 100);
        let b = mint(&t, 3, 8, LockMode::Exclusive, 100);
        t.forget_outwaited(a);
        t.forget(b);
        assert!(admit(&t, a, 2, LockMode::Exclusive, 150).is_some());
        assert!(admit(&t, b, 3, LockMode::Exclusive, 150).is_none());
    }

    /// Never admitted: an unknown id, another node's, a mode the record
    /// does not cover, an unsettled record (a reclaim, a copy), and the same
    /// ones from the memory.
    #[test]
    fn a_late_renewal_is_refused_unless_the_grant_is_known_settled_and_covers_it() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Shared, 100);
        let unknown = GrantId { node: 1, seq: 999 };
        assert!(admit(&t, unknown, 2, LockMode::Shared, 150).is_none());
        assert!(
            admit(&t, id, 5, LockMode::Shared, 150).is_none(),
            "another node's"
        );
        assert!(
            admit(&t, id, 2, LockMode::Exclusive, 150).is_none(),
            "a stronger mode"
        );
        // An unsettled record, live or dropped.
        let reclaimed = GrantId { node: 4, seq: 1 };
        t.install(Grant {
            id: reclaimed,
            node: 6,
            ino: 8,
            mode: LockMode::Exclusive,
            until: WaitMs(100),
            recalled: false,
            gen: 0,
            confirmed: Some((1, 0)),
            settled: false,
        });
        assert!(admit(&t, reclaimed, 6, LockMode::Exclusive, 150).is_none());
        outwait(&t, 8, 150);
        assert_eq!(t.lapsed_len(), 1, "only the settled one is remembered");
        assert!(admit(&t, reclaimed, 6, LockMode::Exclusive, 160).is_none());
        assert_eq!(t.stats().late_admitted, 0);
        assert_eq!(t.stats().late_refused, 5);
    }

    /// A record this table replaced by a newer one of the same node while it
    /// was live (a re-affirmation, `note_continues`) never lapsed: a late
    /// renewal of the old id — the holder adopted the new one by a recall
    /// while its renewal was out — is a renewal of the new. A new grant made
    /// after the old one was outwaited is not (it is no continuation: its
    /// memory was superseded).
    #[test]
    fn a_late_renewal_of_an_id_replaced_while_live_is_a_renewal_of_its_replacement() {
        let t = LockTables::default();
        let old = mint(&t, 2, 7, LockMode::Exclusive, 100);
        let old_rec = t.get(old).unwrap();
        let new = mint(&t, 2, 7, LockMode::Exclusive, 400);
        t.note_continues(&old_rec, new);
        let a = admit(&t, old, 2, LockMode::Exclusive, 150).expect("admitted");
        assert_eq!(a.id, new);
        assert!(
            admit(&t, old, 5, LockMode::Exclusive, 150).is_none(),
            "another node's"
        );
        // Outwaited, then granted afresh: no continuation.
        let t = LockTables::default();
        let old = mint(&t, 2, 7, LockMode::Exclusive, 100);
        outwait(&t, 7, 150);
        let _fresh = mint(&t, 2, 7, LockMode::Exclusive, 400);
        assert!(admit(&t, old, 2, LockMode::Exclusive, 170).is_none());
    }

    /// Review of F-c: an *unsettled* record (a non-final mirror's, a
    /// reclaim's) replaced by a mint for the same node while live is no
    /// continuation: the mint is settled from its own issue, but nothing
    /// proves what was granted between the old grant's issue and its
    /// arrival here, so a late renewal of the old id stays refused.
    #[test]
    fn an_unsettled_record_replaced_while_live_does_not_continue() {
        let t = LockTables::default();
        let old = GrantId { node: 4, seq: 1 };
        t.install(Grant {
            id: old,
            node: 2,
            ino: 7,
            mode: LockMode::Exclusive,
            until: WaitMs(300),
            recalled: false,
            gen: 0,
            confirmed: Grant::UNCONFIRMED,
            settled: false,
        });
        let old_rec = t.get(old).unwrap();
        let new = mint(&t, 2, 7, LockMode::Exclusive, 400);
        t.note_continues(&old_rec, new);
        assert!(admit(&t, old, 2, LockMode::Exclusive, 150).is_none());
        assert!(t.get(new).is_some(), "the new grant itself stays");
    }

    /// The answer to a late renewal of an id this node no longer holds (a
    /// recall made it adopt a newer one) frees the grant held now for its
    /// next late renewal: left marked as renewing, it was never asked again
    /// and parked for the whole bound (sim `locks-pause` seed 1364).
    #[test]
    fn an_answer_for_a_replaced_id_lets_the_grant_held_now_ask_again() {
        let t = parking();
        let first = GrantId { node: 1, seq: 1 };
        t.install_held(
            7,
            HeldGrant {
                id: first,
                ..held(LockMode::Shared, 100)
            },
        );
        t.take_discard(7, Now::same(10), true);
        assert_eq!(t.lapse_tick(Now::same(120)).late.len(), 1);
        let second = GrantId { node: 1, seq: 2 };
        assert_eq!(t.recall_held(7, second), Some(false));
        assert_eq!(t.held(7).unwrap().id, second);
        // The adoption leaves no renewal in flight (4a0fb20): the adopted id
        // is asked for at the next tick, then it is the one in flight.
        let late = t.lapse_tick(Now::same(130)).late;
        assert_eq!(late.iter().map(|(_, h)| h.id).collect::<Vec<_>>(), [second]);
        assert!(
            t.lapse_tick(Now::same(135)).late.is_empty(),
            "one in flight"
        );
        // `Lost` for the old id: not the grant held, which is asked for again
        // (a duplicate of the one in flight at worst).
        assert!(!t.lost(7, first, Now::same(140)));
        t.renewal_failed(7, first);
        assert_eq!(t.lapse_tick(Now::same(150)).late.len(), 1);
    }

    /// A different id installed over a lapsing grant (a push) is a refusal as
    /// a lapse always was: its owners captured since the old window's end,
    /// the data tainted; the same id (the owner re-affirms it) resumes it.
    #[test]
    fn a_push_of_another_id_over_a_lapsing_grant_fences_and_the_same_id_resumes() {
        for same in [false, true] {
            let t = locked();
            assert_eq!(t.fenced(7, Now::same(120)), Fence::Parked);
            let id = if same {
                t.held(7).unwrap().id
            } else {
                GrantId { node: 1, seq: 9 }
            };
            t.install_held(
                7,
                HeldGrant {
                    id,
                    installed: HonourMs(130),
                    ..held(LockMode::Exclusive, 400)
                },
            );
            if same {
                assert!(t.fenced_owners(Now::same(131)).is_empty());
                assert_eq!(t.fenced(7, Now::same(131)), Fence::Ok);
                assert_eq!(t.stats().lapse_parks_resumed, 1);
            } else {
                let since: Vec<i64> = t
                    .fenced_owners(Now::same(131))
                    .iter()
                    .map(|f| f.since.0)
                    .collect();
                assert_eq!(since, vec![100]);
                assert_eq!(t.stats().lapse_parks_refused, 1);
                // The new grant is honoured, so the file is no longer
                // fenced as a whole (its lock owners are, above), but the
                // data written under the lapsed one is discarded as a lapse
                // always did.
                assert_eq!(
                    t.take_discard(7, Now::same(131), false),
                    Discard::Discard { fenced: false }
                );
            }
        }
    }

    /// The memory is superseded by every change that could let another node
    /// act on the inode: each path, one by one. A late renewal is refused
    /// after any of them (and only after them).
    #[test]
    fn every_path_that_adds_a_record_supersedes_the_lapsed_memory() {
        // (label, what happens on inode 7 after node 2's exclusive grant
        // lapsed and was dropped)
        type Step = fn(&LockTables);
        let steps: Vec<(&str, Step)> = vec![
            ("a conflicting mint", |t| {
                mint(t, 3, 7, LockMode::Exclusive, 500);
            }),
            ("a mint of the same node", |t| {
                mint(t, 2, 7, LockMode::Shared, 500);
            }),
            ("an install", |t| {
                t.install(Grant {
                    id: GrantId { node: 9, seq: 1 },
                    node: 3,
                    ino: 7,
                    mode: LockMode::Shared,
                    until: WaitMs(500),
                    recalled: false,
                    gen: 0,
                    confirmed: Some((1, 0)),
                    settled: false,
                });
            }),
            ("install_keeping_later", |t| {
                t.install_keeping_later(Grant {
                    id: GrantId { node: 9, seq: 2 },
                    node: 3,
                    ino: 7,
                    mode: LockMode::Shared,
                    until: WaitMs(500),
                    recalled: false,
                    gen: 0,
                    confirmed: Some((1, 0)),
                    settled: false,
                });
            }),
            ("install_if_consistent", |t| {
                assert!(t.install_if_consistent(
                    Grant {
                        id: GrantId { node: 9, seq: 3 },
                        node: 3,
                        ino: 7,
                        mode: LockMode::Shared,
                        until: WaitMs(500),
                        recalled: false,
                        gen: 0,
                        confirmed: Some((1, 0)),
                        settled: false,
                    },
                    WaitMs(160)
                ));
            }),
            ("the inode leaving (take_where)", |t| {
                t.take_where(|i| i == 7);
            }),
            ("the generation leaving (take_by_gen)", |t| {
                t.take_by_gen(0);
            }),
            ("clear_grants", |t| {
                t.clear_grants();
            }),
        ];
        for (label, step) in steps {
            let t = LockTables::default();
            let id = mint(&t, 2, 7, LockMode::Exclusive, 100);
            outwait(&t, 7, 150);
            assert_eq!(t.lapsed_len(), 1, "{label}: remembered");
            step(&t);
            assert_eq!(t.lapsed_len(), 0, "{label}: superseded");
            assert!(
                admit(&t, id, 2, LockMode::Exclusive, 170).is_none(),
                "{label}: refused"
            );
        }
        // An upgrade of another node's shared record to exclusive, beside a
        // shared memory.
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Shared, 100);
        outwait(&t, 7, 150);
        let other = mint(&t, 4, 7, LockMode::Shared, 500);
        assert_eq!(t.lapsed_len(), 1, "a shared record conflicts with nothing");
        assert!(t.upgrade(other, LockMode::Exclusive, WaitMs(500)));
        assert_eq!(t.lapsed_len(), 0, "an upgrade does");
        assert!(admit(&t, id, 2, LockMode::Shared, 170).is_none());
        // A compatible shared record of another node leaves a shared
        // memory be (nothing conflicting was granted).
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Shared, 100);
        outwait(&t, 7, 150);
        mint(&t, 3, 7, LockMode::Shared, 500);
        assert_eq!(t.lapsed_len(), 1);
        assert!(admit(&t, id, 2, LockMode::Shared, 170).is_some());
        // Another inode's grants touch nothing.
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Exclusive, 100);
        outwait(&t, 7, 150);
        mint(&t, 3, 8, LockMode::Exclusive, 500);
        assert!(admit(&t, id, 2, LockMode::Exclusive, 170).is_some());
    }

    /// A late admission beside a live conflicting record is refused (a
    /// supersede path would be missing): never admitted next to it.
    #[test]
    #[cfg_attr(debug_assertions, should_panic(expected = "conflicting"))]
    fn a_late_admission_never_goes_in_beside_a_live_conflicting_record() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, LockMode::Exclusive, 100);
        outwait(&t, 7, 150);
        // A conflicting live record the supersede paths did not see.
        t.lock().grants.insert(
            GrantId { node: 9, seq: 5 },
            Grant {
                id: GrantId { node: 9, seq: 5 },
                node: 3,
                ino: 7,
                mode: LockMode::Exclusive,
                until: WaitMs(900),
                recalled: false,
                gen: 0,
                confirmed: Some((1, 0)),
                settled: true,
            },
        );
        assert!(admit(&t, id, 2, LockMode::Exclusive, 170).is_none());
    }

    /// The bounded memory evicts the oldest: an evicted entry only refuses.
    #[test]
    fn the_lapsed_memory_is_bounded_and_evicts_the_oldest() {
        let t = LockTables::default();
        let first = mint(&t, 2, 1, LockMode::Exclusive, 100);
        for ino in 2..(ENDED_KEPT as u64 + 3) {
            mint(&t, 2, ino, LockMode::Exclusive, 100);
        }
        t.conflicting(99_999, 99, LockMode::Exclusive, WaitMs(150));
        assert_eq!(t.lapsed_len(), ENDED_KEPT);
        assert!(admit(&t, first, 2, LockMode::Exclusive, 160).is_none());
    }

    /// The final mirror's snapshot carries every record with its flags (the
    /// expired ones, and the memory, as `lapsed`); the successor keeps the
    /// settled lapsed ones as its own memory, not live, unless a live
    /// record supersedes them.
    #[test]
    fn a_final_snapshot_carries_live_records_and_the_lapsed_memory() {
        let old = LockTables::default();
        let live = mint(&old, 2, 7, LockMode::Exclusive, 500);
        let expired = mint(&old, 3, 8, LockMode::Exclusive, 100);
        let dropped = mint(&old, 4, 9, LockMode::Exclusive, 100);
        outwait(&old, 9, 150);
        let snap = old.final_snapshot(Now::same(200));
        let by = |id: GrantId| snap.iter().find(|w| w.id == id).copied().unwrap();
        assert!(!by(live).lapsed && by(live).settled);
        assert!(
            by(expired).lapsed && by(expired).settled,
            "expired, not dropped"
        );
        assert!(
            by(dropped).lapsed && by(dropped).settled,
            "dropped: remembered"
        );
        // The successor.
        let new = LockTables::default();
        for w in &snap {
            let g = w.into_grant(WaitMs(700));
            if w.lapsed {
                new.install_lapsed(g);
            } else {
                new.install(g);
            }
        }
        assert_eq!(new.live_grants_len(WaitMs(300)), 1);
        assert_eq!(new.lapsed_len(), 2);
        assert!(admit(&new, expired, 3, LockMode::Exclusive, 210).is_some());
        assert!(admit(&new, dropped, 4, LockMode::Exclusive, 210).is_some());
        // A live record of another node on the inode keeps a memory out.
        let new = LockTables::default();
        mint(&new, 5, 8, LockMode::Exclusive, 900);
        new.install_lapsed(by(expired).into_grant(WaitMs(700)));
        assert_eq!(new.lapsed_len(), 0);
    }
}

/// Plan 41 L1: the atomic window primitives and the tables' mutex kept free
/// of the store.
#[cfg(test)]
mod window_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    const X: LockMode = LockMode::Exclusive;
    const S: LockMode = LockMode::Shared;

    fn at(ms: i64) -> Now {
        Now::same(ms)
    }

    /// A settled record of `node` on `ino` in `mode` ending at `until`
    /// (wait clock), minted by node 1, in the root's table.
    fn mint(t: &LockTables, node: u64, ino: u64, mode: LockMode, until: i64) -> GrantId {
        t.grant(1, node, ino, mode, WaitMs(until), 0)
    }

    fn held_of(id: GrantId, mode: LockMode, until: i64, renew_at: i64) -> HeldGrant {
        HeldGrant {
            id,
            mode,
            until: HonourMs(until),
            renew_at: HonourMs(renew_at),
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since: None,
            installed: HonourMs(0),
            lapsing: None,
            written: false,
            route: None,
        }
    }

    /// Run `once` the first time the table reaches one of `points`.
    fn gap_once(
        t: &LockTables,
        points: &'static [&'static str],
        once: impl Fn(&LockTables) + Send + Sync + 'static,
    ) {
        let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
        t.set_gap_hook_for_tests(Some(Arc::new(move |t, point| {
            if points.contains(&point) && armed.swap(false, Ordering::SeqCst) {
                once(t);
            }
        })));
    }

    #[test]
    fn an_expired_record_is_forgotten_and_remembered_as_lapsed() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        let Some(Ok(g)) = t.forget_outwaited_if_expired(id, at(150)) else {
            panic!("expired")
        };
        assert_eq!((g.id, g.until), (id, WaitMs(100)));
        assert!(t.get(id).is_none() && t.was_ended(id));
        assert!(t.lapsed_get(id).is_some(), "a settled record is remembered");
        // Gone: nothing to forget.
        assert!(t.forget_outwaited_if_expired(id, at(150)).is_none());
    }

    #[test]
    fn a_live_record_is_kept_and_its_end_named() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        assert_eq!(
            t.forget_outwaited_if_expired(id, at(99)),
            Some(Err(WaitMs(100)))
        );
        assert!(t.get(id).is_some() && !t.was_ended(id));
    }

    #[test]
    fn the_expiry_is_judged_on_the_tables_clock_inside_the_step() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        // The caller's reading is old; the table's clock is what counts.
        t.set_clock(Arc::new(|| at(500)));
        assert!(matches!(
            t.forget_outwaited_if_expired(id, at(10)),
            Some(Ok(_))
        ));
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        t.set_clock(Arc::new(|| at(50)));
        assert_eq!(
            t.forget_outwaited_if_expired(id, at(500)),
            Some(Err(WaitMs(100))),
            "a set clock replaces the caller's reading"
        );
    }

    /// The gap a two-step sequence has: an extension that lands after the
    /// record was read and before the forget. The primitive reads inside its
    /// critical section.
    #[test]
    fn an_extension_landing_before_the_check_keeps_the_record() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        gap_once(&t, &["forget_if_expired"], move |t| {
            t.extend(id, 2, WaitMs(5_000), (1, 120)).unwrap();
        });
        assert_eq!(
            t.forget_outwaited_if_expired(id, at(150)),
            Some(Err(WaitMs(5_000)))
        );
        assert!(t.get(id).is_some());
    }

    #[test]
    fn a_grant_never_ends_sooner_than_the_record_it_replaces() {
        let t = LockTables::default();
        let old = mint(&t, 2, 7, X, 100);
        // The caller computed its end from an older reading of the record;
        // the record has been extended since.
        t.extend(old, 2, WaitMs(5_000), (1, 50)).unwrap();
        let new = mint(&t, 2, 7, X, 900);
        assert!(t.get(old).is_none());
        assert_eq!(t.get(new).unwrap().until, WaitMs(5_000));
        // A later end of its own stands; another node's record is not it.
        let other = mint(&t, 3, 7, S, 80);
        assert_eq!(t.get(other).unwrap().until, WaitMs(80));
        let new = mint(&t, 2, 7, X, 9_000);
        assert_eq!(t.get(new).unwrap().until, WaitMs(9_000));
    }

    #[test]
    fn extend_live_extends_only_a_live_record_of_the_node() {
        let t = LockTables::default();
        let id = mint(&t, 2, 7, X, 100);
        assert_eq!(
            t.extend_live(id, 2, WaitMs(300), at(50), (1, 0)),
            Some((X, false))
        );
        assert_eq!(t.get(id).unwrap().until, WaitMs(300));
        // Never shortens.
        t.extend_live(id, 2, WaitMs(200), at(50), (1, 0)).unwrap();
        assert_eq!(t.get(id).unwrap().until, WaitMs(300));
        // Another node's id, an expired record: nothing.
        assert!(t.extend_live(id, 3, WaitMs(900), at(50), (1, 0)).is_none());
        assert!(t.extend_live(id, 2, WaitMs(900), at(300), (1, 0)).is_none());
        assert_eq!(t.get(id).unwrap().until, WaitMs(300));
        // The table's clock is the one read.
        t.set_clock(Arc::new(|| at(400)));
        assert!(t.extend_live(id, 2, WaitMs(900), at(50), (1, 0)).is_none());
    }

    #[test]
    fn a_lapsed_window_is_dropped_and_its_id_is_dead() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(7, held_of(id, X, 100, 50));
        assert!(t.drop_held_if_lapsed(7, id, at(150)));
        assert!(t.held(7).is_none());
        // Tombstoned: a reply that crosses the drop does not reinstall it.
        assert_eq!(
            t.install_held(7, held_of(id, X, 900, 450)),
            Installed::Released
        );
    }

    #[test]
    fn a_window_renewed_before_the_drop_is_kept() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(7, held_of(id, X, 100, 50));
        // The reader saw the window over; a renewal lands before the drop.
        gap_once(&t, &["drop_held_if_lapsed"], move |t| {
            t.renewed(7, id, id, X, HonourMs(140), 1_000, 100, false, 1, at(140));
        });
        assert!(!t.drop_held_if_lapsed(7, id, at(150)));
        assert_eq!(t.held(7).unwrap().until, HonourMs(1_040));
    }

    /// A grant held on inode 7 whose window (ending at 100) is over and that
    /// pins a write: lapsing at 150, parked for 5 s.
    fn lapsing() -> (LockTables, GrantId) {
        let t = LockTables::default();
        t.set_lapse_park_ms(5_000);
        let id = GrantId { node: 1, seq: 5 };
        let mut h = held_of(id, X, 100, 50);
        h.written = true;
        t.install_held(7, h);
        assert!(t.lapse_pending(7, at(150)));
        (t, id)
    }

    #[test]
    fn a_lapsing_grant_and_another_id_are_left_alone() {
        let (t, id) = lapsing();
        assert!(
            !t.drop_held_if_lapsed(7, id, at(150)),
            "parked, not dropped"
        );
        assert!(t.held(7).is_some());
        // Not the grant held: nothing to drop, nothing held under that id.
        let other = GrantId { node: 1, seq: 9 };
        assert!(t.drop_held_if_lapsed(7, other, at(150)));
        assert!(t.drop_held_if_lapsed(8, id, at(150)));
        assert!(t.held(7).is_some());
    }

    #[test]
    fn a_lanes_renewal_marks_are_stale_after_a_while_and_a_lapsing_grant_is_never_due() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(7, held_of(id, X, 10_000, 100));
        assert_eq!(
            t.due_renewals_lane(at(200), 100, false, |_, _| false).len(),
            1
        );
        // Marked at 200: not due again at 250 (fresh), due at 300 (stale).
        assert!(t
            .due_renewals_lane(at(250), 100, false, |_, _| false)
            .is_empty());
        assert_eq!(
            t.due_renewals_lane(at(300), 100, false, |_, _| false).len(),
            1
        );
        assert_eq!(t.held(7).unwrap().renewing, Some(HonourMs(300)));
        // The core's tick counts a mark of any age.
        assert!(t.due_renewals(at(900)).is_empty());
        // Lapsing: never (a late renewal is an add, the core's).
        let (t, _) = lapsing();
        assert!(t
            .due_renewals_lane(at(200), 0, false, |_, _| false)
            .is_empty());
        // Nor with an open window, a state the table does not reach.
        t.lock().held.get_mut(&7).unwrap().until = HonourMs(10_000);
        assert!(t
            .due_renewals_lane(at(200), 0, false, |_, _| false)
            .is_empty());
    }

    /// Fail-before (plan 41 L3): the core's renewal tick marked the grant a
    /// moment before its step stuck on a commit; the lane waited out the
    /// forward timeout for a mark nobody could clear (a lone node's 5 s
    /// stall: the grant lapsed with the lane on, 4 runs in 5). With the
    /// core stuck the lane renews at once, and still respects its own marks.
    #[test]
    fn a_mark_the_stuck_core_set_is_not_waited_for_but_the_lanes_own_is() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(7, held_of(id, X, 10_000, 100));
        // The core's tick marks it.
        assert_eq!(t.due_renewals(at(200)).len(), 1);
        // The core runs: the lane waits the mark out.
        assert!(t
            .due_renewals_lane(at(300), 10_000, false, |_, _| false)
            .is_empty());
        // The core is stuck: not the lane's mark, renewed now.
        assert_eq!(
            t.due_renewals_lane(at(300), 10_000, true, |_, _| false)
                .len(),
            1
        );
        // Now it is the lane's own: respected while stuck.
        assert!(t
            .due_renewals_lane(at(350), 10_000, true, |i, g| i == 7 && g == id)
            .is_empty());
    }

    /// An own grant: the owner's record (this node, 1, as the root) and the
    /// held grant of the same id.
    fn own(t: &LockTables, rec_until: i64, held_until: i64) -> GrantId {
        let id = mint(t, 1, 7, X, rec_until);
        t.install_held(7, held_of(id, X, held_until, 100));
        id
    }

    #[test]
    fn an_own_renewal_extends_the_record_and_the_window_together() {
        let t = LockTables::default();
        let id = own(&t, 800, 1_000);
        assert!(t.renew_own(7, id, 1, |_| 5_000, 4_000, 100, at(600), 1));
        let e = t.get(id).unwrap();
        assert_eq!(e.until, WaitMs(600 + 4_000 + 100));
        let h = t.held(7).unwrap();
        assert_eq!(h.until, HonourMs(600 + 4_000 - 100));
        assert_eq!(h.renew_at, renew_point(HonourMs(600), 4_000, 100));
        assert_eq!((h.renewing, h.route), (None, Some(1)));
        // The cap bounds the ttl; the window never shortens.
        assert!(t.renew_own(7, id, 1, |_| 500, 4_000, 100, at(700), 1));
        assert_eq!(t.held(7).unwrap().until, HonourMs(4_500));
        assert_eq!(t.get(id).unwrap().until, WaitMs(4_700));
    }

    #[test]
    fn an_own_renewal_is_refused_where_the_core_would_refuse() {
        let refused = |what: &str, build: &dyn Fn(&LockTables) -> GrantId, cap: i64| {
            let t = LockTables::default();
            let id = build(&t);
            let (rec, held) = (t.get(id), t.held(7));
            assert!(
                !t.renew_own(7, id, 1, |_| cap, 4_000, 100, at(600), 1),
                "{what}"
            );
            assert_eq!((t.get(id), t.held(7)), (rec, held), "{what}: untouched");
        };
        refused("no cap left", &|t| own(t, 800, 1_000), 0);
        refused("the window is over", &|t| own(t, 800, 500), 5_000);
        refused("the record is over", &|t| own(t, 500, 1_000), 5_000);
        refused(
            "another node's record",
            &|t| {
                let id = mint(t, 2, 7, X, 800);
                t.install_held(7, held_of(id, X, 1_000, 100));
                id
            },
            5_000,
        );
        refused(
            "a delegate's record",
            &|t| {
                let id = t.grant(1, 1, 7, X, WaitMs(800), 3);
                t.install_held(7, held_of(id, X, 1_000, 100));
                id
            },
            5_000,
        );
        refused(
            "a weaker record than the grant held",
            &|t| {
                let id = mint(t, 1, 7, S, 800);
                t.install_held(7, held_of(id, X, 1_000, 100));
                id
            },
            5_000,
        );
        refused(
            "no record",
            &|t| {
                let id = GrantId { node: 1, seq: 77 };
                t.install_held(7, held_of(id, X, 1_000, 100));
                id
            },
            5_000,
        );
        refused(
            "being released",
            &|t| {
                let id = own(t, 800, 1_000);
                t.lock().held.get_mut(&7).unwrap().releasing = true;
                id
            },
            5_000,
        );
        refused(
            "lapsing",
            &|t| {
                let id = own(t, 800, 1_000);
                // Not a state the table reaches with an open window: set.
                t.lock().held.get_mut(&7).unwrap().lapsing = Some(Lapsing {
                    since: HonourMs(1),
                    at: WaitMs(1),
                    deadline: WaitMs(9_000),
                });
                id
            },
            5_000,
        );
        refused(
            "recalled, nothing pins it",
            &|t| {
                let id = own(t, 800, 1_000);
                t.recall_held(7, id).unwrap();
                t.expire_first_use(at(60_000));
                id
            },
            5_000,
        );
        // Another id on the inode.
        let t = LockTables::default();
        own(&t, 800, 1_000);
        assert!(!t.renew_own(
            7,
            GrantId { node: 1, seq: 99 },
            1,
            |_| 5_000,
            4_000,
            100,
            at(600),
            1
        ));
    }

    #[test]
    fn an_own_renewal_reads_the_tables_clock_inside_the_step() {
        let t = LockTables::default();
        let id = own(&t, 800, 1_000);
        t.set_clock(Arc::new(|| at(1_200)));
        // The caller's reading (600) says live; the table's (1200) says the
        // window is over.
        assert!(!t.renew_own(7, id, 1, |_| 5_000, 4_000, 100, at(600), 1));
    }

    #[test]
    fn a_renewal_records_who_answered_and_a_not_owner_clears_it() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(7, held_of(id, X, 1_000, 100));
        t.renewed(7, id, id, X, HonourMs(200), 1_000, 100, false, 3, at(200));
        assert_eq!(t.held(7).unwrap().route, Some(3));
        // Another node's NotOwner does not clear it.
        t.clear_route(7, id, 4);
        assert_eq!(t.held(7).unwrap().route, Some(3));
        t.clear_route(7, id, 3);
        assert_eq!(t.held(7).unwrap().route, None);
    }

    #[test]
    fn take_ids_takes_what_the_snapshot_named_and_forgets_its_memory() {
        let t = LockTables::default();
        let a = mint(&t, 2, 1, X, 1_000);
        let b = mint(&t, 2, 2, S, 1_000);
        let c = mint(&t, 2, 3, X, 1_000);
        // A lapsed memory on inode 2 (another node's, settled).
        let m = Grant {
            id: GrantId { node: 1, seq: 90 },
            node: 5,
            ino: 2,
            mode: S,
            until: WaitMs(50),
            recalled: false,
            gen: 0,
            confirmed: Grant::MINTED,
            settled: true,
        };
        t.install_lapsed(m);
        assert_eq!(t.lapsed_len(), 1);
        let snap = t.snapshot_inos();
        assert_eq!(snap.len(), 4);
        // After the snapshot: a grant of another node lands on inode 2.
        let late = t.grant(1, 6, 2, S, WaitMs(2_000), 0);
        let ids: std::collections::HashSet<GrantId> = snap
            .iter()
            .filter(|(_, ino)| *ino == 2)
            .map(|(id, _)| *id)
            .collect();
        let taken = t.take_ids(&ids);
        assert_eq!(taken.iter().map(|g| g.id).collect::<Vec<_>>(), vec![b]);
        assert!(t.get(a).is_some() && t.get(c).is_some());
        assert!(
            t.get(late).is_some(),
            "a grant added after the snapshot stays"
        );
        assert_eq!(t.lapsed_len(), 0, "the memory of the inode goes with it");
        // Nothing named: nothing taken.
        assert!(t.take_ids(&Default::default()).is_empty());
    }

    #[test]
    fn the_predicate_runs_outside_the_tables_mutex_once_per_inode() {
        let t = LockTables::default();
        for node in [2, 3] {
            mint(&t, node, 1, S, 1_000);
        }
        mint(&t, 2, 2, X, 1_000);
        let calls = AtomicUsize::new(0);
        let taken = t.take_where_outside(|ino| {
            calls.fetch_add(1, Ordering::SeqCst);
            // Under the mutex this would deadlock (a std mutex is not
            // reentrant) or trip the debug guard.
            assert_eq!(t.grants_len(), 3);
            ino == 1
        });
        assert_eq!(taken.len(), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "once per inode");
        assert_eq!(t.grants_len(), 1);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "the lock tables' mutex")]
    fn a_store_read_under_the_tables_mutex_is_caught() {
        let meta = crate::Meta::open_in_memory().unwrap();
        mint(meta.locks(), 2, 1, X, 1_000);
        meta.locks().take_where(|_| {
            let _ = meta.delegation_table();
            true
        });
    }

    #[test]
    fn a_predicate_may_read_the_store_outside_the_mutex() {
        let meta = crate::Meta::open_in_memory().unwrap();
        mint(meta.locks(), 2, 1, X, 1_000);
        let taken = meta.locks().take_where_outside(|_| {
            let _ = meta.delegation_table();
            true
        });
        assert_eq!(taken.len(), 1);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "the lock tables' mutex")]
    fn a_store_write_under_the_tables_mutex_is_caught() {
        let meta = crate::Meta::open_in_memory().unwrap();
        mint(meta.locks(), 2, 1, X, 1_000);
        meta.locks().take_where(|_| {
            let _ = meta.next_delegation_gen(1);
            true
        });
    }

    /// The token check logs after the guard, so a log call that blocks (or a
    /// subscriber that reads the table) cannot hold the tables.
    #[test]
    fn the_token_checks_log_outside_the_guard() {
        use std::sync::Mutex as M;
        struct Probe(Arc<M<Vec<bool>>>);
        impl tracing::Subscriber for Probe {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {
                // Debug builds: panics if the event is emitted under the
                // tables' mutex (the guard count, [`assert_no_guard`]).
                assert_no_guard("a log event");
                self.0.lock().unwrap().push(true);
            }
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }
        let t = Arc::new(LockTables::default());
        let id = mint(&t, 2, 7, X, 10_000);
        let tag = LockTag(vec![LockToken::window(id, 5_000)]);
        let seen = Arc::new(M::new(Vec::new()));
        let probe = Probe(seen.clone());
        tracing::subscriber::with_default(probe, || {
            t.check_tag(&tag, at(100)).unwrap();
            t.check_tag_at_write(&tag, at(100)).unwrap();
            t.unpin(&tag);
            // A refusal logs too.
            let dead = LockTag(vec![LockToken::window(GrantId { node: 1, seq: 999 }, 50)]);
            assert!(t.check_tag_at_write(&dead, at(100)).is_err());
        });
        assert!(
            seen.lock().unwrap().len() >= 3,
            "{:?}",
            seen.lock().unwrap()
        );
    }
}
