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
//! - dirty data written under a grant that ended *without* its release's
//!   flush (lapsed, lost, or unlocked while fenced) is never published:
//!   the inode is **tainted**, and every point that would publish it
//!   (close, release, `fsync`, a recalled grant's flush, the next lock)
//!   asks [`LockTables::take_discard`] first and throws the data away.
//!
//! # Cost when unused
//!
//! [`LockTables::fenced`] is on every read and write; with no local lock
//! anywhere it is one relaxed atomic load.

use crate::session::Position;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    #[serde(default)]
    pub gen: u64,
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
}

/// A kernel-facing lock of one local process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalLock {
    pub owner: u64,
    pub pid: u32,
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
    /// Node side: inodes whose dirty data may have been written under a
    /// grant that ended without the release's flush (see
    /// [`LockTables::take_discard`]); `Owed`: it was discarded while no
    /// publish point was there to report it, and the next one reports
    /// `EIO`.
    taint: BTreeMap<u64, Taint>,
    stats: LockStats,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Taint {
    Dirty,
    Owed,
}

const RELEASED_KEPT: usize = 512;

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
        self.taint.insert(ino, Taint::Dirty);
    }

    /// Local locks on `ino` and no honoured grant.
    fn fenced_at(&self, ino: u64, now_ms: i64) -> bool {
        self.local.get(&ino).is_some_and(|v| !v.is_empty())
            && !self.held.get(&ino).is_some_and(|h| h.until_ms > now_ms)
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
}

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
        if g.pending_recalls
            .remove(&ino)
            .is_some_and(|ids| ids.contains(&held.id))
        {
            held.recalled = true;
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
        let h = g.held.get_mut(&ino).filter(|h| h.id == id)?;
        h.recalled = true;
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
        match g.held.get(&ino) {
            Some(h) if h.id == id => {
                g.held.remove(&ino);
                g.tombstone(id);
                g.taint(ino);
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
        }
        self.track(&g);
        h
    }

    /// Grants to renew: honoured, past `renew_at`, no renewal in flight.
    /// Marks them renewing at `now_ms`.
    pub fn due_renewals(&self, now_ms: i64) -> Vec<(u64, HeldGrant)> {
        let mut g = self.lock();
        let mut due = Vec::new();
        let Inner { held, local, .. } = &mut *g;
        for (ino, h) in held.iter_mut() {
            // A recalled grant is renewed only while local locks are
            // under it (its release is what the owner waits for).
            let pinned = local.get(ino).is_some_and(|v| !v.is_empty());
            if h.until_ms > now_ms
                && now_ms >= h.renew_at_ms
                && h.renewing.is_none()
                && (!h.recalled || pinned)
            {
                h.renewing = Some(now_ms);
                due.push((*ino, *h));
            }
        }
        due
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
            h.renew_at_ms = h.renew_at_ms.max(sent_ms + ttl_ms / 2);
            h.recalled |= recalled;
            if now_id != id {
                h.id = now_id;
                h.mode = h.mode.max(mode);
                g.tombstone(id);
            }
            g.stats.renewals += 1;
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
            g.held.remove(&ino);
            g.tombstone(id);
            g.taint(ino);
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
        }
        fenced
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
        let tainted = g.taint.get(&ino) == Some(&Taint::Dirty);
        if tainted {
            g.taint.remove(&ino);
        }
        if fenced {
            g.stats.fenced_io += 1;
        }
        self.track(&g);
        (fenced || tainted).then_some(fenced)
    }

    /// Dirty data of `ino` was discarded where nobody could be told (a
    /// new lock, a recalled grant's flush): the next close or `fsync`
    /// reports `EIO` ([`Self::take_owed`]).
    pub fn owe(&self, ino: u64) {
        let mut g = self.lock();
        g.taint.entry(ino).or_insert(Taint::Owed);
        self.track(&g);
    }

    /// Whether an `EIO` for discarded data is owed on `ino` (cleared).
    pub fn take_owed(&self, ino: u64) -> bool {
        if self.tracked.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let mut g = self.lock();
        let owed = g.taint.get(&ino) == Some(&Taint::Owed);
        if owed {
            g.taint.remove(&ino);
            self.track(&g);
        }
        owed
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
            self.track(&g);
        }
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
            self.track(&g);
        }
        if idle {
            g.local.remove(&ino);
            self.local_inos.fetch_sub(1, Ordering::Relaxed);
            if let Some(h) = g.held.get_mut(&ino) {
                h.idle_since_ms = Some(now_ms);
            }
        }
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
        }
    }

    fn lk(owner: u64, write: bool, start: u64, end: u64) -> LocalLock {
        LocalLock {
            owner,
            pid: 1,
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

    #[test]
    fn a_released_grant_leaves_nothing_to_discard_and_owed_eio_is_reported_once() {
        let t = LockTables::default();
        let id = GrantId { node: 1, seq: 1 };
        t.install_held(7, held(LockMode::Exclusive, 100));
        assert_eq!(t.local_set(7, lk(1, true, 0, 10), 0), LocalOutcome::Done);
        assert!(t.local_unlock(7, 1, 0, 10, 10));
        assert_eq!(t.recall_held(7, id), Some(false));
        assert!(t.begin_release(7).is_some());
        assert!(t.end_release(7, id));
        assert_eq!(t.take_discard(7, 500), None);
        assert!(!t.take_owed(7));
        t.owe(7);
        assert_eq!(t.take_discard(7, 500), None, "owed is not a discard");
        assert!(t.take_owed(7));
        assert!(!t.take_owed(7));
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
        };
        t.install_held(7, h);
        assert!(t.idle_before(100).is_empty(), "never used: not idle");
        let l = LocalLock {
            owner: 1,
            pid: 1,
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
