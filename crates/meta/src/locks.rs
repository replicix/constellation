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
//! All times are unix milliseconds on this node's clock. The sequencer
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
//! - a grant is renewed only while it is honoured
//!   ([`LockTables::due_renewals`]); a lapsed grant is lost, never
//!   reclaimed late;
//! - I/O on a file this node holds local locks on is **fenced** once its
//!   grant lapsed ([`LockTables::fenced`]): `EIO` until a lock is taken
//!   again (NFSv4's rule);
//! - so is the **lock owner** itself, everywhere on this node
//!   ([`LockTables::fenced_owners`]): an application that guards other
//!   files with the lock (git creates, links and renames under one
//!   `flock`) gets `EIO` from every write and namespace op it issues
//!   until its local locks are gone — other processes are not touched;
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
//! # Cost when unused
//!
//! [`LockTables::fenced`] is on every read and write, and
//! [`LockTables::owner_fence_armed`] on every mutating op; with no local
//! lock anywhere each is one relaxed atomic load (two loads while locks
//! are held under grants that are still honoured).

use crate::session::Position;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
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
pub fn renew_point(sent_ms: i64, ttl_ms: i64, margin_ms: i64) -> i64 {
    sent_ms + (ttl_ms - margin_ms).max(0) / 2
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

/// A grant the sequencer made, as its table keeps it (and as it travels
/// to a backup's mirror or with a delegation move).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub id: GrantId,
    pub node: u64,
    pub ino: u64,
    pub mode: LockMode,
    /// Live until this node's clock reaches it (`granted + ttl + margin`).
    pub until_ms: i64,
    pub recalled: bool,
    /// The M11 generation it was granted (or installed) under; 0 for the
    /// root's table. A generation's grants leave with it.
    pub gen: u64,
}

/// What a delegate hands back to the root with its recall answer: the
/// subtree's grants and the *floor* its holders released at (every
/// position a later grant under the subtree must carry — see
/// `core::locks`'s `LockState::floors`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockHandback {
    pub grants: Vec<Grant>,
    pub floor: Position,
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

/// A grant this node holds (the cache).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldGrant {
    pub id: GrantId,
    pub mode: LockMode,
    /// Honoured while this node's clock is below this.
    pub until_ms: i64,
    /// Renew from here (`sent + ttl/2`).
    pub renew_at_ms: i64,
    /// Who granted it (renewals go to the current owner, not here).
    pub owner: u64,
    pub recalled: bool,
    /// The position the replica must reach before reading under it.
    pub position: Position,
    /// A renewal is in flight (sent at this time).
    pub renewing: Option<i64>,
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
    pub idle_since_ms: Option<i64>,
    /// When this node installed it (its clock): a recalled grant waiting
    /// for its first local lock is renewed only for
    /// [`LockTables::first_use_budget_ms`] from here.
    pub installed_ms: i64,
}

/// A kernel-facing lock of one local process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalLock {
    pub owner: u64,
    /// The process that took it (its thread group id; 0: unknown).
    pub pid: u32,
    /// When that process started (0: unknown): the pid names it only
    /// with this, across pid reuse.
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

/// Counters for `status.locks`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockStats {
    /// Local lock requests, and how they were answered.
    pub requests: u64,
    pub local_hits: u64,
    pub local_conflicts: u64,
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
    // ---- sequencer side ----
    pub grants_made: u64,
    pub recalls_sent: u64,
    pub recalls_released: u64,
    pub recalls_expired: u64,
    pub reclaimed: u64,
    pub waiters_parked: u64,
    pub grace_refusals: u64,
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
    stats: LockStats,
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
    /// The process that took the lock (0: unknown) and its start time
    /// (0: unknown).
    pub pid: u32,
    pub pid_start: u64,
    pub since_ms: i64,
}

const RELEASED_KEPT: usize = 512;
/// Past this many `(inode, owner)` pairs the oldest inodes' go (a lost
/// entry only admits what it would have refused before).
const NEWEST_KEPT: usize = 4096;

impl Inner {
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
    fn capture(&mut self, ino: u64, now_ms: i64) {
        let Some(v) = self.local.get(&ino) else {
            return;
        };
        for l in v {
            if let std::collections::btree_map::Entry::Vacant(e) = self.fenced_owners.entry(l.owner)
            {
                e.insert(FencedOwner {
                    owner: l.owner,
                    pid: l.pid,
                    pid_start: l.pid_start,
                    since_ms: now_ms,
                });
                self.stats.owners_fenced += 1;
            }
        }
    }

    /// Capture the owners of every inode that is fenced at `now_ms`.
    fn capture_lapsed(&mut self, now_ms: i64) {
        let fenced: Vec<u64> = self
            .local
            .keys()
            .copied()
            .filter(|ino| self.fenced_at(*ino, now_ms))
            .collect();
        for ino in fenced {
            self.capture(ino, now_ms);
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
                    .map_or(i64::MIN, |h| h.until_ms)
            })
            .min()
            .unwrap_or(i64::MAX)
    }

    /// Local locks on `ino` and no honoured grant *covering* them. An
    /// exclusive local lock under a shared grant is fenced too: its
    /// exclusive grant lapsed and another local owner's request brought a
    /// shared one, which other nodes may share (sim
    /// `locks-released-writes` seed 211727: the exclusive holder's next
    /// write went through under node 3's shared grant while node 1 held
    /// one too).
    fn fenced_at(&self, ino: u64, now_ms: i64) -> bool {
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
            .is_some_and(|h| h.until_ms > now_ms && h.mode.covers(needed))
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
    quarantine_ms: AtomicI64,
    /// How long after its install a recalled grant waiting for its first
    /// local lock is still renewed (0: [`FIRST_USE_BUDGET_MS`]).
    first_use_budget_ms: AtomicI64,
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
}

impl LockTables {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// After a change to `held` or `taint` (under the table lock).
    fn track(&self, g: &Inner) {
        self.tracked
            .store(g.held.len() + g.taint.len(), Ordering::Relaxed);
        self.refence(g);
    }

    /// After a change to `held`, `local` or the fenced owners.
    fn refence(&self, g: &Inner) {
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

    /// Lock grants an earlier tenure made may be live until `until_ms`:
    /// grant nothing new before then (never lowered).
    pub fn set_quarantine(&self, until_ms: i64) {
        self.quarantine_ms.fetch_max(until_ms, Ordering::Relaxed);
    }

    pub fn quarantine_until(&self) -> i64 {
        self.quarantine_ms.load(Ordering::Relaxed)
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
    pub fn own_grant(&self, ino: u64, node: u64, now_ms: i64) -> Option<Grant> {
        self.lock()
            .grants
            .values()
            .find(|e| e.ino == ino && e.node == node && e.until_ms > now_ms)
            .copied()
    }

    /// Strengthen (never weaken) a grant in place, keeping its id and its
    /// recalled flag, and extend it.
    pub fn upgrade(&self, id: GrantId, mode: LockMode, until_ms: i64) -> bool {
        let mut g = self.lock();
        match g.grants.get_mut(&id) {
            Some(e) => {
                e.mode = e.mode.max(mode);
                e.until_ms = e.until_ms.max(until_ms);
                true
            }
            None => false,
        }
    }

    /// Make a grant; `me` is this sequencer's id (the grant's minter),
    /// `gen` the generation it is made under (0: the root).
    pub fn grant(
        &self,
        me: u64,
        node: u64,
        ino: u64,
        mode: LockMode,
        until_ms: i64,
        gen: u64,
    ) -> GrantId {
        let id = GrantId {
            node: me,
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed) + 1,
        };
        let mut g = self.lock();
        g.grants.retain(|_, e| !(e.node == node && e.ino == ino));
        g.grants.insert(
            id,
            Grant {
                id,
                node,
                ino,
                mode,
                until_ms,
                recalled: false,
                gen,
            },
        );
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
    pub fn install_if_consistent(&self, grant: Grant) -> bool {
        let mut g = self.lock();
        if g.grants
            .values()
            .any(|e| e.ino == grant.ino && (e.node == grant.node || e.mode.conflicts(grant.mode)))
        {
            return false;
        }
        g.grants.insert(grant.id, grant);
        true
    }

    /// Live grants on `ino` held by nodes other than `node` that conflict
    /// with `mode`. Expired grants are dropped on the way.
    pub fn conflicting(&self, ino: u64, node: u64, mode: LockMode, now_ms: i64) -> Vec<Grant> {
        let mut g = self.lock();
        g.grants.retain(|_, e| e.until_ms > now_ms);
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
        now_ms: i64,
    ) -> Option<Grant> {
        self.conflicting(ino, node, mode, now_ms).into_iter().next()
    }

    pub fn get(&self, id: GrantId) -> Option<Grant> {
        self.lock().grants.get(&id).copied()
    }

    /// Extend a known grant of `node`: `(mode, recalled)`, or `None` when
    /// unknown.
    pub fn extend(&self, id: GrantId, node: u64, until_ms: i64) -> Option<(LockMode, bool)> {
        let mut g = self.lock();
        let e = g.grants.get_mut(&id).filter(|e| e.node == node)?;
        e.until_ms = e.until_ms.max(until_ms);
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

    pub fn forget(&self, id: GrantId) -> Option<Grant> {
        self.lock().grants.remove(&id)
    }

    /// Grants whose `until_ms` has passed (still in the table).
    pub fn expired(&self, now_ms: i64) -> Vec<Grant> {
        self.lock()
            .grants
            .values()
            .filter(|e| e.until_ms <= now_ms)
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
        taken
    }

    /// Remove and return the grants whose inode satisfies `under`.
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
        taken
    }

    pub fn clear_grants(&self) -> usize {
        let mut g = self.lock();
        let n = g.grants.len();
        g.grants.clear();
        n
    }

    pub fn grants_len(&self) -> usize {
        self.lock().grants.len()
    }

    /// Grants still live at `now_ms` (`grants_len` counts the expired
    /// ones too, until a `conflicting` call purges them).
    pub fn live_grants_len(&self, now_ms: i64) -> usize {
        self.lock()
            .grants
            .values()
            .filter(|g| g.until_ms > now_ms)
            .count()
    }

    /// Whether this owner's table has a grant made under generation `gen`.
    pub fn has_grants_of_gen(&self, gen: u64) -> bool {
        self.lock().grants.values().any(|g| g.gen == gen)
    }

    // ------------------------------------------------------ node: grants

    pub fn held(&self, ino: u64) -> Option<HeldGrant> {
        self.lock().held.get(&ino).copied()
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
        if g.pending_recalls
            .remove(&ino)
            .is_some_and(|ids| ids.contains(&held.id))
        {
            held.recalled = true;
        }
        // Local locks under no honoured grant (it lapsed, or was dropped,
        // before this one came): their owners stay fenced until their
        // locks are gone, whatever this grant covers.
        if g.fenced_at(ino, held.installed_ms) {
            g.capture(ino, held.installed_ms);
        }
        // Every grant gets the local lock it was asked for before a
        // recall can release it (sim seed 94033: an owner's own fresh
        // grant recalled and released in the same event, before its
        // FUSE thread ran). A lapsed unused grant is dropped by the
        // renewal tick.
        held.first_use = true;
        if let Some(old) = g.held.get(&ino).copied().filter(|o| o.id == held.id) {
            held.mode = old.mode.max(held.mode);
            held.until_ms = old.until_ms.max(held.until_ms);
            held.renew_at_ms = old.renew_at_ms.max(held.renew_at_ms);
            held.recalled |= old.recalled;
            held.releasing = old.releasing;
            held.idle_since_ms = old.idle_since_ms;
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
            held.until_ms = old.until_ms.max(held.until_ms);
            held.recalled |= old.recalled;
            held.releasing = old.releasing;
            held.first_use = old.first_use;
            held.idle_since_ms = old.idle_since_ms;
            held.installed_ms = old.installed_ms;
        }
        let recalled = held.recalled;
        if let Some(old) = g.held.insert(ino, held) {
            if old.id != held.id {
                // Replaced (an upgrade, a fresh grant after a lapse): the
                // old id is dead here.
                g.tombstone(old.id);
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
                self.track(&g);
                true
            }
            _ => false,
        }
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
            g.tombstone(old);
            g.stats.recalled_superseded += 1;
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
        if !h.recalled || busy || h.releasing || h.first_use {
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
                g.capture(ino, h.until_ms);
                self.track(&g);
                true
            }
            _ => false,
        }
    }

    pub fn drop_held_any(&self, ino: u64) -> Option<HeldGrant> {
        let mut g = self.lock();
        let h = g.held.remove(&ino);
        if let Some(h) = h {
            g.tombstone(h.id);
            g.taint(ino);
            g.capture(ino, h.until_ms);
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
    fn renewable(h: &HeldGrant, pinned: bool, now_ms: i64, budget_ms: i64) -> bool {
        !h.recalled || pinned || (h.first_use && now_ms < h.installed_ms + budget_ms)
    }

    /// The requester a grant was installed for gave up without taking its
    /// local lock: the grant no longer waits for that first use (a recall
    /// then releases it), and, free of local locks, it is idle from
    /// `now_ms` (`CONSTELLATION_LOCK_CACHE_IDLE_MS` reclaims it — unused, it
    /// would otherwise be renewed until a recall came). `true` if the
    /// grant is recalled and free of local locks — the caller lets the
    /// core release it.
    pub fn abandon_first_use(&self, ino: u64, now_ms: i64) -> bool {
        let mut g = self.lock();
        let idle = g.local.get(&ino).is_none_or(|v| v.is_empty());
        let Some(h) = g.held.get_mut(&ino).filter(|h| h.first_use) else {
            return false;
        };
        h.first_use = false;
        if idle && h.idle_since_ms.is_none() {
            h.idle_since_ms = Some(now_ms);
        }
        let recalled = h.recalled;
        g.stats.first_use_abandoned += 1;
        recalled && idle
    }

    /// Recalled grants whose first local lock never came within the
    /// first-use budget: no longer pinned (the renewal tick releases
    /// them). Returns their inodes.
    pub fn expire_first_use(&self, now_ms: i64) -> Vec<u64> {
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let Inner {
            held, local, stats, ..
        } = &mut *g;
        let mut out = Vec::new();
        for (ino, h) in held.iter_mut() {
            if h.recalled
                && h.first_use
                && now_ms >= h.installed_ms + budget
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
    /// [`Self::renewable`]. Marks them renewing at `now_ms`.
    pub fn due_renewals(&self, now_ms: i64) -> Vec<(u64, HeldGrant)> {
        let budget = self.first_use_budget_ms();
        let mut g = self.lock();
        let mut due = Vec::new();
        let Inner { held, local, .. } = &mut *g;
        for (ino, h) in held.iter_mut() {
            let pinned = local.get(ino).is_some_and(|v| !v.is_empty());
            if h.until_ms > now_ms
                && now_ms >= h.renew_at_ms
                && h.renewing.is_none()
                && Self::renewable(h, pinned, now_ms, budget)
            {
                h.renewing = Some(now_ms);
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
    pub fn next_renewal_ms(&self, now_ms: i64) -> Option<i64> {
        let budget = self.first_use_budget_ms();
        let g = self.lock();
        g.held
            .iter()
            .filter(|(ino, h)| {
                let pinned = g.local.get(ino).is_some_and(|v| !v.is_empty());
                h.until_ms > now_ms
                    && h.renewing.is_none()
                    && Self::renewable(h, pinned, now_ms, budget)
            })
            .map(|(_, h)| h.renew_at_ms)
            .min()
    }

    /// A renewal of `id` sent at `sent_ms` was answered: the owner holds
    /// `now_id` (the same, or a newer grant of this node whose reply was
    /// lost — adopted here, with its mode) in `mode`.
    #[allow(clippy::too_many_arguments)]
    pub fn renewed(
        &self,
        ino: u64,
        id: GrantId,
        now_id: GrantId,
        mode: LockMode,
        sent_ms: i64,
        ttl_ms: i64,
        margin_ms: i64,
        recalled: bool,
    ) {
        let mut g = self.lock();
        if let Some(h) = g.held.get_mut(&ino).filter(|h| h.id == id) {
            h.renewing = None;
            h.until_ms = h.until_ms.max(sent_ms + ttl_ms - margin_ms);
            h.renew_at_ms = h.renew_at_ms.max(renew_point(sent_ms, ttl_ms, margin_ms));
            h.recalled |= recalled;
            if now_id != id {
                h.id = now_id;
                h.mode = h.mode.max(mode);
                g.tombstone(id);
            }
            g.stats.renewals += 1;
            self.refence(&g);
        }
    }

    /// A renewal was not answered, or answered "not the owner": try
    /// again at the next tick.
    pub fn renewal_failed(&self, ino: u64, id: GrantId) {
        let mut g = self.lock();
        if let Some(h) = g.held.get_mut(&ino).filter(|h| h.id == id) {
            h.renewing = None;
        }
    }

    /// The owner does not know the grant: it is lost.
    pub fn lost(&self, ino: u64, id: GrantId) -> bool {
        let mut g = self.lock();
        let gone = matches!(g.held.get(&ino), Some(h) if h.id == id);
        if gone {
            let since = g.held.remove(&ino).map_or(0, |h| h.until_ms);
            g.tombstone(id);
            g.taint(ino);
            g.capture(ino, since);
            g.stats.lost += 1;
            self.track(&g);
        }
        gone
    }

    /// The honoured grant on `ino`, if any.
    pub fn honoured(&self, ino: u64, now_ms: i64) -> Option<HeldGrant> {
        self.lock()
            .held
            .get(&ino)
            .copied()
            .filter(|h| h.until_ms > now_ms)
    }

    /// Whether I/O on `ino` must be refused: this node holds local locks
    /// on it and no honoured grant. One atomic load when no local lock
    /// exists anywhere.
    pub fn fenced(&self, ino: u64, now_ms: i64) -> bool {
        if self.local_inos.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let mut g = self.lock();
        let fenced = g.fenced_at(ino, now_ms);
        if fenced {
            g.stats.fenced_io += 1;
            g.capture(ino, now_ms);
            self.refence(&g);
        }
        fenced
    }

    /// Whether any lock owner on this node may be fenced at `now_ms`
    /// ([`Self::fenced_owners`] has something to say). One relaxed load
    /// with no local lock anywhere; two while every local lock is under
    /// a grant honoured until later.
    pub fn owner_fence_armed(&self, now_ms: i64) -> bool {
        self.local_inos.load(Ordering::Relaxed) != 0
            && now_ms >= self.fence_from_ms.load(Ordering::Relaxed)
    }

    /// The lock owners fenced on this node at `now_ms`: every owner of a
    /// local lock whose grant lapsed, was lost, or was replaced after a
    /// lapse — captured once and kept until that owner has no local lock
    /// left (unlock or close). The caller refuses every op such an owner
    /// (or its process) issues, on any file (`EIO`).
    pub fn fenced_owners(&self, now_ms: i64) -> Vec<FencedOwner> {
        if !self.owner_fence_armed(now_ms) {
            return Vec::new();
        }
        let mut g = self.lock();
        // Only while an owner not fenced yet may have lapsed: an owner
        // that keeps its lock after the lapse costs the others' ops the
        // table lock, not a walk of every local lock.
        if now_ms >= self.lapse_from_ms.load(Ordering::Relaxed) {
            g.capture_lapsed(now_ms);
            self.refence(&g);
        }
        g.fenced_owners.values().copied().collect()
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
    pub fn take_discard(&self, ino: u64, now_ms: i64) -> Option<bool> {
        if self.local_inos.load(Ordering::Relaxed) == 0 && self.tracked.load(Ordering::Relaxed) == 0
        {
            return None;
        }
        let mut g = self.lock();
        if let Some(h) = g.held.get(&ino).copied().filter(|h| h.until_ms <= now_ms) {
            g.held.remove(&ino);
            g.tombstone(h.id);
            g.taint(ino);
        }
        let fenced = g.fenced_at(ino, now_ms);
        if fenced {
            g.capture(ino, now_ms);
        }
        let tainted = g.taint.remove(&ino);
        if fenced {
            g.stats.fenced_io += 1;
        }
        self.track(&g);
        (fenced || tainted).then_some(fenced)
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
        now_ms: i64,
    ) -> LocalOutcome {
        let need = if write {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        };
        let g = self.lock();
        if let Some(v) = g.local.get(&ino) {
            if let Some(c) = v
                .iter()
                .find(|l| l.owner != owner && l.overlaps(start, end) && (write || l.write))
            {
                return LocalOutcome::Conflict(*c);
            }
        }
        let continuing = g
            .local
            .get(&ino)
            .is_some_and(|v| v.iter().any(|l| l.owner == owner));
        let covered = g.held.get(&ino).is_some_and(|h| {
            h.until_ms > now_ms && (!h.recalled || h.first_use || continuing) && h.mode.covers(need)
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
    pub fn local_set(&self, ino: u64, lock: LocalLock, now_ms: i64) -> LocalOutcome {
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
            h.until_ms > now_ms && (!h.recalled || h.first_use || continuing) && h.mode.covers(need)
        });
        if !covered {
            return LocalOutcome::NeedGrant(need);
        }
        if let Some(h) = g.held.get_mut(&ino) {
            h.first_use = false;
            h.idle_since_ms = None;
        }
        let was_empty = g.local.get(&ino).is_none_or(|v| v.is_empty());
        let v = g.local.entry(ino).or_default();
        Self::cut(v, lock.owner, lock.start, lock.end);
        v.push(lock);
        if was_empty {
            self.local_inos.fetch_add(1, Ordering::Relaxed);
        }
        g.stats.local_hits += 1;
        self.refence(&g);
        LocalOutcome::Done
    }

    /// Remove `owner`'s locks within `[start, end]` (splitting as POSIX
    /// does). Returns whether `ino` has no local lock left.
    pub fn local_unlock(&self, ino: u64, owner: u64, start: u64, end: u64, now_ms: i64) -> bool {
        let mut g = self.lock();
        let fenced = g.fenced_at(ino, now_ms);
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
                h.idle_since_ms = Some(now_ms);
            }
        }
        if fenced && touched {
            // Unlocked under a lapsed grant: the fence lifts with the
            // last lock; the taint keeps what was written under it from
            // being published.
            g.taint(ino);
        }
        g.lift_if_unlocked(owner);
        self.track(&g);
        idle
    }

    /// Drop every lock of `owner` on `ino` (a close). Returns whether
    /// `ino` has no local lock left.
    pub fn local_release_owner(&self, ino: u64, owner: u64, now_ms: i64) -> bool {
        let mut g = self.lock();
        let fenced = g.fenced_at(ino, now_ms);
        let Some(v) = g.local.get_mut(&ino) else {
            return true;
        };
        let before = v.len();
        v.retain(|l| l.owner != owner);
        let dropped = v.len() != before;
        let idle = v.is_empty();
        if fenced && dropped {
            g.taint(ino);
        }
        if idle {
            g.local.remove(&ino);
            self.local_inos.fetch_sub(1, Ordering::Relaxed);
            if let Some(h) = g.held.get_mut(&ino) {
                h.idle_since_ms = Some(now_ms);
            }
        }
        g.lift_if_unlocked(owner);
        self.track(&g);
        idle
    }

    /// Grants idle (no local lock under them) since before `before_ms`,
    /// marked recalled so their release proceeds like a recall's.
    pub fn idle_before(&self, before_ms: i64) -> Vec<(u64, HeldGrant)> {
        let mut g = self.lock();
        let mut out = Vec::new();
        for (ino, h) in g.held.iter_mut() {
            if h.idle_since_ms.is_some_and(|t| t < before_ms) && !h.recalled && !h.releasing {
                h.recalled = true;
                out.push((*ino, *h));
            }
        }
        out
    }

    pub fn local_idle(&self, ino: u64) -> bool {
        self.lock().local.get(&ino).is_none_or(|v| v.is_empty())
    }

    /// POSIX range subtraction for one owner.
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(mode: LockMode, until: i64) -> HeldGrant {
        HeldGrant {
            id: GrantId { node: 1, seq: 1 },
            mode,
            until_ms: until,
            renew_at_ms: until / 2,
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: 0,
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

    #[test]
    fn a_handoff_copy_is_installed_only_into_a_consistent_table() {
        let t = LockTables::default();
        let copy = |node: u64, seq: u64, mode: LockMode| Grant {
            id: GrantId { node: 9, seq },
            node,
            ino: 7,
            mode,
            until_ms: 100,
            recalled: false,
            gen: 0,
        };
        // Node 2 was granted again (a newer id): the old copy stays out.
        t.install(copy(2, 5, LockMode::Shared));
        assert!(!t.install_if_consistent(copy(2, 1, LockMode::Exclusive)));
        // Node 3's exclusive copy conflicts with node 2's grant (made
        // after the copy's grant left): out.
        assert!(!t.install_if_consistent(copy(3, 2, LockMode::Exclusive)));
        // A compatible copy goes in, once.
        assert!(t.install_if_consistent(copy(3, 3, LockMode::Shared)));
        assert!(!t.install_if_consistent(copy(3, 3, LockMode::Shared)));
        assert_eq!(t.grants_len(), 2);
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
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        assert!(!t.fenced(7, 0));
        // The exclusive grant is replaced by a shared one (another local
        // owner's request after the exclusive one lapsed).
        t.install_held(
            7,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Shared, 100)
            },
        );
        assert!(
            t.fenced(7, 0),
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
            t.local_set(7, lk(1, true, 0, 10), 0),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
        t.install_held(7, held(LockMode::Shared, 100));
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), 0),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
        assert_eq!(t.local_set(7, lk(1, false, 0, 10), 0), LocalOutcome::Done);
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        // A lapsed grant covers nothing.
        assert_eq!(
            t.local_set(7, lk(1, true, 0, 10), 100),
            LocalOutcome::NeedGrant(LockMode::Exclusive)
        );
    }

    #[test]
    fn posix_ranges_split_and_conflict_by_owner() {
        let t = LockTables::default();
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 100), 0), LocalOutcome::Done);
        assert_eq!(
            t.local_set(7, lk(2, false, 50, 60), 0),
            LocalOutcome::Conflict(lk(1, true, 0, 100))
        );
        // Unlock the middle: two pieces remain.
        assert!(!t.local_unlock(7, 1, 40, 60, 0));
        let v = t.local_locks(7);
        assert_eq!(v, vec![lk(1, true, 0, 39), lk(1, true, 61, 100)]);
        // Now owner 2 fits in the hole; shared/shared overlap is fine.
        assert_eq!(t.local_set(7, lk(2, false, 45, 55), 0), LocalOutcome::Done);
        assert_eq!(t.local_set(7, lk(3, false, 45, 55), 0), LocalOutcome::Done);
        assert!(!t.local_release_owner(7, 1, 0));
        assert!(!t.local_release_owner(7, 2, 0));
        assert!(t.local_release_owner(7, 3, 0));
        assert!(t.local_idle(7));
    }

    #[test]
    fn fencing_is_free_without_local_locks_and_bites_after_lapse() {
        let t = LockTables::default();
        assert!(!t.fenced(7, 0));
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        assert!(!t.fenced(7, 50));
        assert!(t.fenced(7, 100));
        // Another inode without local locks is not fenced.
        assert!(!t.fenced(8, 100));
        assert!(t.local_unlock(7, 1, 0, 10, 100));
        assert!(!t.fenced(7, 100));
    }

    #[test]
    fn a_grant_that_ends_without_its_release_taints_the_dirty_data() {
        let t = LockTables::default();
        // Nothing held, locked or tainted: nothing to discard.
        assert_eq!(t.take_discard(7, 0), None);
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        // Honoured: publish.
        assert_eq!(t.take_discard(7, 50), None);
        // Lapsed with the lock still held: fenced (discard, EIO even with
        // nothing dirty) — checked before the close drops the owner.
        assert_eq!(t.take_discard(7, 100), Some(true));
        assert!(t.held(7).is_none(), "the lapsed grant is dropped");
        // The close drops the owner under the fence: tainted, so the
        // release that follows discards too — but owes no EIO of its own.
        assert!(t.local_release_owner(7, 1, 100));
        assert_eq!(t.take_discard(7, 100), Some(false));
        assert_eq!(t.take_discard(7, 100), None);

        // An explicit unlock after the lapse, then the close.
        t.install_held(
            8,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Exclusive, 200)
            },
        );
        assert_eq!(t.local_set(8, lk(1, true, 0, 10), 150), LocalOutcome::Done);
        assert!(t.local_unlock(8, 1, 0, 10, 250));
        assert!(!t.fenced(8, 250), "the fence lifts with the last lock");
        assert_eq!(
            t.take_discard(8, 250),
            Some(false),
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
        assert_eq!(t.local_set(9, lk(1, true, 0, 10), 250), LocalOutcome::Done);
        assert!(t.local_unlock(9, 1, 0, 10, 260));
        assert_eq!(t.take_discard(9, 299), None);
        assert_eq!(t.take_discard(9, 300), Some(false));

        // Lost at a renewal, or dropped lapsed by the tick: tainted.
        let id = GrantId { node: 1, seq: 5 };
        t.install_held(
            10,
            HeldGrant {
                id,
                ..held(LockMode::Shared, 400)
            },
        );
        assert!(t.lost(10, id));
        assert_eq!(t.take_discard(10, 0), Some(false));
        let id2 = GrantId { node: 1, seq: 4 };
        t.install_held(
            11,
            HeldGrant {
                id: id2,
                ..held(LockMode::Shared, 400)
            },
        );
        assert!(t.drop_held(11, id2));
        assert_eq!(t.take_discard(11, 0), Some(false));
        assert_eq!(t.take_discard(11, 0), None);
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
        assert!(!t.owner_fence_armed(0), "no local lock: one load");
        t.install_held(7, held(LockMode::Exclusive, 100));
        t.install_held(
            8,
            HeldGrant {
                id: GrantId { node: 1, seq: 2 },
                ..held(LockMode::Exclusive, 1_000)
            },
        );
        assert_eq!(t.local_set(7, lk_pid(1, 10), 0), LocalOutcome::Done);
        assert_eq!(t.local_set(8, lk_pid(2, 20), 0), LocalOutcome::Done);
        // Both honoured: not armed (two loads), nobody fenced.
        assert!(!t.owner_fence_armed(99));
        assert!(t.fenced_owners(99).is_empty());
        // Ino 7's grant lapses: its owner is fenced, the other is not.
        assert!(t.owner_fence_armed(100));
        let f = t.fenced_owners(100);
        assert_eq!(
            f,
            vec![FencedOwner {
                owner: 1,
                pid: 10,
                pid_start: 0,
                since_ms: 100
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
                installed_ms: 150,
                ..held(LockMode::Exclusive, 900)
            },
        );
        assert!(!t.fenced(7, 150));
        assert_eq!(t.fenced_owners(150).len(), 1);
        // Owner 2's unlock elsewhere changes nothing for owner 1.
        assert!(t.local_unlock(8, 2, 0, u64::MAX, 160));
        assert_eq!(t.fenced_owners(160).len(), 1);
        // Owner 1's unlock lifts it; with every remaining grant honoured
        // the fast path is back to "not armed".
        assert!(t.local_unlock(7, 1, 0, u64::MAX, 170));
        assert!(t.fenced_owners(170).is_empty());
        assert!(!t.owner_fence_armed(170));
    }

    /// A grant that is lost (the owner forgot it), or dropped lapsed by
    /// the renewal tick, fences its owners at once; a close lifts it.
    #[test]
    fn a_lost_or_dropped_grant_fences_its_owners_at_once() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.install_held(7, held(LockMode::Exclusive, 10_000));
        assert_eq!(t.local_set(7, lk_pid(1, 10), 0), LocalOutcome::Done);
        assert_eq!(
            t.local_set(7, lk_pid(3, 10), 0),
            LocalOutcome::Conflict(lk_pid(1, 10))
        );
        assert!(t.lost(7, id));
        assert!(t.owner_fence_armed(1), "armed at once, not at the ttl");
        assert_eq!(t.fenced_owners(1).len(), 1);
        assert!(!t.local_release_owner(7, 9, 2), "another owner's close");
        assert_eq!(t.fenced_owners(2).len(), 1);
        assert!(t.local_release_owner(7, 1, 3));
        assert!(t.fenced_owners(3).is_empty());

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
                0
            ),
            LocalOutcome::Done
        );
        assert!(t.drop_held(8, id2));
        assert_eq!(t.fenced_owners(5).first().map(|f| f.pid), Some(40));
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
        h.renew_at_ms = 100;
        assert_eq!(t.install_held(7, h), Installed::Ok { recalled: true });
        assert!(t.begin_release(7).is_none(), "pinned by its first use");
        // Inside the budget: renewed.
        assert_eq!(t.next_renewal_ms(200), Some(100));
        assert_eq!(t.due_renewals(200).len(), 1);
        t.renewed(7, id, id, LockMode::Exclusive, 200, 10_000, 0, true);
        // Past it: not renewed, and the tick's sweep unpins it.
        assert!(t.due_renewals(5_000).is_empty());
        assert_eq!(t.next_renewal_ms(1_000), None);
        assert!(t.expire_first_use(999).is_empty());
        assert_eq!(t.expire_first_use(1_000), vec![7]);
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
        assert!(t.abandon_first_use(8, 50), "recalled and idle: release it");
        assert!(!t.abandon_first_use(8, 50), "once");
        assert!(t.begin_release(8).is_some());
        // Not recalled: the grant stays cached, just no longer pinned.
        t.install_held(
            9,
            HeldGrant {
                id: GrantId { node: 1, seq: 3 },
                ..held(LockMode::Exclusive, 10_000)
            },
        );
        assert!(!t.abandon_first_use(9, 60));
        assert!(!t.held(9).unwrap().first_use);
        // ... and idle from the give-up: the idle sweep reclaims it.
        assert_eq!(t.held(9).unwrap().idle_since_ms, Some(60));
        assert!(t.idle_before(60).iter().all(|(ino, _)| *ino != 9));
        assert!(t.idle_before(61).iter().any(|(ino, _)| *ino == 9));
    }

    #[test]
    fn a_released_grant_leaves_nothing_to_discard_and_discard_errors_are_sequenced() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        assert!(t.local_unlock(7, 1, 0, 10, 10));
        assert_eq!(t.recall_held(7, id), Some(false));
        assert!(t.begin_release(7).is_some());
        assert!(t.end_release(7, id));
        assert_eq!(t.take_discard(7, 500), None);
        assert_eq!(t.error_seq(7), 0);
        let seq = t.note_discard(7);
        assert_eq!(
            t.take_discard(7, 500),
            None,
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
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        assert_eq!(
            t.local_set(7, lk(2, false, 20, 30), 0),
            LocalOutcome::NeedGrant(LockMode::Shared)
        );
        assert!(t.begin_release(7).is_none());
        assert!(t.local_unlock(7, 1, 0, 10, 0));
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
        assert_eq!(renew_point(100, 5_000, 1_000), 2_100);
        // A delegate's short grant: honoured for 1.076 s, renewed at 538 ms
        // (at `ttl/2` it would have been after the window closed).
        assert_eq!(renew_point(0, 2_076, 1_000), 538);
        for ttl in [1_000i64, 1_500, 2_000, 2_500, 5_000] {
            assert!(renew_point(0, ttl, 1_000) <= (ttl - 1_000).max(0));
        }
    }

    #[test]
    fn renewals_only_while_honoured() {
        let t = LockTables::default();
        let mut h = held(LockMode::Shared, 100);
        h.renew_at_ms = 50;
        t.install_held(7, h);
        assert!(t.due_renewals(40).is_empty());
        assert_eq!(t.due_renewals(60).len(), 1);
        // In flight: not again.
        assert!(t.due_renewals(70).is_empty());
        t.renewed(7, h.id, h.id, LockMode::Shared, 60, 100, 10, false);
        assert_eq!(t.held(7).unwrap().until_ms, 150);
        // Lapsed: never.
        assert!(t.due_renewals(200).is_empty());
    }

    #[test]
    fn sequencer_table_conflicts_and_expiry() {
        let t = LockTables::default();
        let a = t.grant(9, 1, 7, LockMode::Shared, 100, 0);
        let b = t.grant(9, 2, 7, LockMode::Shared, 100, 0);
        assert!(t.conflicting(7, 3, LockMode::Shared, 0).is_empty());
        assert_eq!(t.conflicting(7, 3, LockMode::Exclusive, 0).len(), 2);
        // Node 1 upgrading: only node 2 is in the way.
        assert_eq!(
            t.conflicting(7, 1, LockMode::Exclusive, 0),
            vec![t.get(b).unwrap()]
        );
        assert!(t.mark_recalled(b));
        assert!(!t.mark_recalled(b));
        assert_eq!(t.expired(100).len(), 2);
        assert!(t.conflicting(7, 3, LockMode::Exclusive, 100).is_empty());
        assert!(t.get(a).is_none());
        // A regrant to the same node replaces its entry.
        let c = t.grant(9, 1, 7, LockMode::Exclusive, 300, 0);
        let d = t.grant(9, 1, 7, LockMode::Exclusive, 300, 0);
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
            until_ms: 1_000,
            renew_at_ms: 500,
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: 0,
        };
        t.install_held(7, h);
        assert!(t.idle_before(100).is_empty(), "never used: not idle");
        let l = LocalLock {
            owner: 1,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: 10,
        };
        assert_eq!(t.local_set(7, l, 0), LocalOutcome::Done);
        assert!(t.local_unlock(7, 1, 0, 10, 10));
        assert!(t.idle_before(10).is_empty());
        assert_eq!(t.idle_before(11).len(), 1);
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
            until_ms: 5_000,
            renew_at_ms: 2_500,
            owner: 1,
            recalled: false,
            position: Position::ZERO,
            renewing: None,
            releasing: false,
            first_use: false,
            idle_since_ms: None,
            installed_ms: 0,
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
        assert_eq!(t.local_set(7, lock(), 0), LocalOutcome::Done);
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
                0
            ),
            LocalOutcome::Conflict(_)
        ));
        assert!(t.local_unlock(7, 9, 0, u64::MAX, 1));
        assert_eq!(
            t.begin_release(7).map(|h| h.id),
            Some(GrantId { node: 1, seq: 6 })
        );
    }

    /// The owner's newer id arriving as a reply (the request a push had
    /// answered) merges: the local lock continues under it, `first_use`
    /// and the recalled flag are the held grant's, the old id is dead.
    #[test]
    fn a_newer_id_of_the_same_owner_merges_the_local_state() {
        let t = LockTables::default();
        assert!(matches!(t.install_held(7, held(4)), Installed::Ok { .. }));
        assert_eq!(t.local_set(7, lock(), 0), LocalOutcome::Done);
        assert_eq!(t.recall_held(7, GrantId { node: 1, seq: 4 }), Some(true));
        let newer = HeldGrant {
            until_ms: 9_000,
            ..held(5)
        };
        assert!(matches!(
            t.install_held(7, newer),
            Installed::Ok { recalled: true }
        ));
        let h = t.held(7).unwrap();
        assert_eq!(h.id, GrantId { node: 1, seq: 5 });
        assert!(h.recalled && !h.first_use);
        assert_eq!(h.until_ms, 9_000);
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
