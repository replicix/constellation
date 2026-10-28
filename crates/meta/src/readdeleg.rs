//! Plan 30 §M8: read delegations for `cto=strict` — both sides' tables,
//! in memory, shared by the authority core (through its `Replica` port)
//! and the FUSE threads.
//!
//! # What a delegation is
//!
//! A time-bounded promise from the sequencer (the lease holder) to one
//! node: *no mutation that touches inode `ino` — its attributes, its
//! manifest, its xattrs, or, for a directory, its entries — is
//! acknowledged to anyone but you until you have stopped honouring this
//! delegation*. While it holds one, the node answers `cto=strict` opens
//! and lookups under `ino` from its own replica (after its replica has
//! reached the grant's position once), with no round trip.
//!
//! # Why both tables live here
//!
//! The sequencer's own FUSE writes never enter the core (the fast path
//! executes them on the FUSE thread), yet they must recall delegations
//! too. The grant table is therefore readable by FUSE threads, and the
//! ordering argument that makes it safe is one mutex:
//! - the core *inserts* a grant here **before** it reads the position it
//!   answers with;
//! - a FUSE write commits (and joins the unshipped key set) **before** it
//!   looks here for grants touching what it wrote.
//!
//! Either the write's lookup sees the grant (and recalls it), or the grant
//! was inserted after that lookup, hence after the write committed, and
//! the position the core reads afterwards includes the write. The
//! delegate's table is here for the symmetric reason: FUSE threads check
//! it on every strict open, and the core drops entries on a recall.
//!
//! # Time
//!
//! Times are unix milliseconds on one of this node's clocks (plan 43
//! C7-H1, `constellation_types::clock`): the delegate's [`HeldDelegation`]
//! is stamped and judged on its **honour** clock (the core's `now.honour`,
//! `protocol().now()` on FUSE threads: a backward wall step cannot extend
//! it, a forward one ends it with the clock); the holder's grant table
//! ([`ReadGrant`], the quarantines) is on its **wait** clock (a forward
//! wall step cannot end the wait for a grant early; a persisted horizon
//! is wall-clock and converted at load). The holder records a grant as live until `granted_at + ttl + margin`; the
//! delegate honours it only until `sent_at + ttl − margin`, measured from
//! when it *sent* the request — both margins the lease's
//! (`crates/model/src/cto.rs` has the argument and the model that checks
//! it).

use crate::session::Position;
use constellation_types::clock::{HonourMs, Now, WaitMs};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// The inodes a delegation must be recalled for when `records` are
/// acknowledged: every inode they change, **and every dentry's parent**
/// (a directory delegation covers its entries). Deliberately not
/// `KeySet::from_records`, whose coverage keys leave the parent out since
/// plan 30 M6's rebase onto M5's per-key base.
pub fn recall_inos(records: &[crate::record::LogRecord]) -> Vec<u64> {
    let t = crate::replay::TouchSet::from_records(records.iter());
    let mut inos: Vec<u64> = t.inos.into_iter().collect();
    inos.extend(t.dentries.iter().map(|(p, _)| *p));
    inos.sort_unstable();
    inos.dedup();
    inos
}

thread_local! {
    static EXECUTED_VICTIMS: std::cell::RefCell<Vec<u64>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// M16: inodes an op executed on this thread changed although its
/// records do not name them — the inode an `unlink`/`rmdir` removed a
/// link of (its `nlink` and `ctime`; with the last link, the inode
/// itself), and the inodes a `rename` moved or replaced. The records
/// name only the parent and the entry, so [`recall_inos`] alone left a
/// read delegation on the unlinked inode live: a strict open of it by
/// inode (a kernel-cached dentry) was answered from the delegate's stale
/// replica, after the unlink was acknowledged. Filled inside the op's own
/// write transaction (so it is exactly what the op changed, however
/// concurrent ops raced it), read by [`recall_inos_executed`].
pub(crate) fn note_victim(ino: u64) {
    EXECUTED_VICTIMS.with(|v| v.borrow_mut().push(ino));
}

/// Cleared by `mutate::execute` before each op.
pub(crate) fn clear_victims() {
    EXECUTED_VICTIMS.with(|v| v.borrow_mut().clear());
}

/// The inodes to recall read delegations on once the op that produced
/// `records` executed **on this thread** (right after
/// `mutate::execute` / `execute_mutate` returned it): [`recall_inos`]
/// plus the inodes it changed that the records do not name.
pub fn recall_inos_executed(records: &[crate::record::LogRecord]) -> Vec<u64> {
    let mut inos = recall_inos(records);
    EXECUTED_VICTIMS.with(|v| inos.append(&mut v.borrow_mut()));
    inos.sort_unstable();
    inos.dedup();
    inos
}

/// [`recall_inos`] for an op not yet executed (an inbox op recalls
/// before it runs): `KeySet::from_op` keeps parents, except for a raw
/// `Records` op, which goes through [`recall_inos`].
pub fn recall_inos_of_op(op: &crate::mutate::MutateOp) -> Vec<u64> {
    match op {
        crate::mutate::MutateOp::Records { records } => recall_inos(records),
        op => {
            let mut inos = crate::session::KeySet::from_op(op).inos;
            inos.sort_unstable();
            inos.dedup();
            inos
        }
    }
}

/// A grant the sequencer made, as its table keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadGrant {
    pub id: u64,
    pub node: u64,
    pub ino: u64,
    /// Live until this node's **wait** clock reaches it (`granted + ttl +
    /// margin`).
    pub until: WaitMs,
}

/// What a mutation's acknowledgement must wait for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecallNeed {
    /// Live grants on the touched inodes, held by other nodes.
    pub grants: Vec<ReadGrant>,
    /// Grants this node may have made before it restarted are unknown;
    /// every acknowledgement waits until this passes.
    pub quarantine_until: Option<WaitMs>,
}

impl RecallNeed {
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty() && self.quarantine_until.is_none()
    }
}

/// A delegation this node holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldDelegation {
    /// Honoured while this node's honour clock is below this.
    pub until: HonourMs,
    /// The position the replica must reach before a read under it.
    pub position: Position,
    pub epoch: u64,
    pub holder: u64,
    pub grant: u64,
    /// When the lease-backed TTL started (the request's send time): past
    /// half its lifetime, a read under it renews it in the background
    /// (honour clock).
    pub renew_at: HonourMs,
}

/// Counters for `status.cto` (plan 30 §M8's measurements).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CtoStats {
    /// Strict opens/lookups, by how they were answered.
    pub strict_reads: u64,
    pub holder_local: u64,
    pub delegation_local: u64,
    pub read_index: u64,
    /// ReadIndex answers that granted a delegation (delegate side).
    pub delegations_installed: u64,
    /// Grants not installed because a recall overtook their reply.
    pub delegations_raced: u64,
    pub renewals: u64,
    /// ReadIndex round trips that failed (answered from the replica).
    pub degraded: u64,
    /// No P2P: the open tailed S3 to head instead.
    pub s3_tail: u64,
    /// Recalls this node received as a delegate.
    pub recalled: u64,
    /// Sum and count of ReadIndex round-trip + wait times (ms), and a
    /// log2 histogram (bucket `i` < 2^i ms).
    pub read_index_ms_total: u64,
    pub read_index_ms: [u64; 14],
    /// Holder side: grants made, and FUSE writes that waited for recalls
    /// with the total time they waited.
    pub grants: u64,
    pub fuse_writes_recalled: u64,
    pub fuse_recall_wait_ms_total: u64,
}

#[derive(Default)]
struct Inner {
    grants: BTreeMap<u64, ReadGrant>,
    next_grant: u64,
    quarantine_until: WaitMs,
    /// See [`ReadDelegations::leave_alone`].
    kernel_drain_until: WaitMs,
    held: HashMap<u64, HeldDelegation>,
    /// Bumped by every recall received: a reply whose grant a recall
    /// overtook is not installed (see [`ReadDelegations::install`]).
    recall_gen: u64,
    /// Renewals in flight (one per ino).
    renewing: HashMap<u64, HonourMs>,
}

/// Both sides' tables (see the module doc).
pub struct ReadDelegations {
    inner: Mutex<Inner>,
    stats: Mutex<CtoStats>,
    /// Grants live right now (for `status`), refreshed on every change.
    live_grants: AtomicU64,
    /// No other node has shown itself yet (see [`Self::leave_alone`]).
    alone: std::sync::atomic::AtomicBool,
}

impl Default for ReadDelegations {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            stats: Mutex::default(),
            live_grants: AtomicU64::new(0),
            alone: std::sync::atomic::AtomicBool::new(true),
        }
    }
}

fn bucket(ms: u64) -> usize {
    if ms == 0 {
        return 0;
    }
    ((64 - ms.leading_zeros()) as usize).min(13)
}

impl ReadDelegations {
    // ---- holder side ----

    /// Record a grant to `node` on `ino`, live until `until` (this node's
    /// wait clock). Must be called **before** the position the answer
    /// carries is read.
    pub fn grant(&self, node: u64, ino: u64, until: WaitMs) -> u64 {
        let mut g = self.inner.lock().unwrap();
        g.next_grant += 1;
        let id = g.next_grant;
        g.grants.insert(
            id,
            ReadGrant {
                id,
                node,
                ino,
                until,
            },
        );
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        drop(g);
        self.stats.lock().unwrap().grants += 1;
        id
    }

    /// [`Self::grant`] for several inodes at once, one lock, all live
    /// until `until`: the read-ahead of a ReadIndex answer. Each is an
    /// ordinary grant (a write that touches the inode recalls it like any
    /// other); like `grant`, it must be called before the position the
    /// answer carries is read. Returns `(ino, grant id)` in order.
    ///
    /// An inode `node` holds a grant on that is live at `now` is left
    /// out: no write has touched it since (that would have recalled the
    /// grant, and the recall's ack forgotten it), so the delegation `node`
    /// got then covers it still, and a second grant would only be a second
    /// recall for the next write. (A delegation `node` let lapse sooner
    /// than this table's margin reads strictly, as before.)
    pub fn grant_many(
        &self,
        node: u64,
        inos: &[u64],
        until: WaitMs,
        now: WaitMs,
    ) -> Vec<(u64, u64)> {
        let mut g = self.inner.lock().unwrap();
        let live: std::collections::HashSet<u64> = g
            .grants
            .values()
            .filter(|x| x.node == node && x.until > now)
            .map(|x| x.ino)
            .collect();
        let mut made = Vec::with_capacity(inos.len());
        for &ino in inos.iter().filter(|ino| !live.contains(ino)) {
            g.next_grant += 1;
            let id = g.next_grant;
            g.grants.insert(
                id,
                ReadGrant {
                    id,
                    node,
                    ino,
                    until,
                },
            );
            made.push((ino, id));
        }
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        drop(g);
        self.stats.lock().unwrap().grants += made.len() as u64;
        made
    }

    /// What acknowledging a mutation that touched `inos` must wait for,
    /// at `now` (wait clock): every live grant on one of them held by a node other
    /// than `except` (the writer's own delegation is not recalled — its
    /// reads are read-your-writes), and the restart quarantine. Expired
    /// grants are pruned on the way.
    pub fn touching(&self, inos: &[u64], except: Option<u64>, now: WaitMs) -> RecallNeed {
        let mut g = self.inner.lock().unwrap();
        g.grants.retain(|_, x| x.until > now);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        let q = g.quarantine_until.max(g.kernel_drain_until);
        let quarantine_until = (q > now).then_some(q);
        if g.grants.is_empty() {
            return RecallNeed {
                grants: Vec::new(),
                quarantine_until,
            };
        }
        let grants = g
            .grants
            .values()
            .filter(|x| Some(x.node) != except && inos.contains(&x.ino))
            .copied()
            .collect();
        RecallNeed {
            grants,
            quarantine_until,
        }
    }

    /// How many grants `node` holds that are live at `now` (the bound on a
    /// node's read-ahead).
    pub fn live_for_node(&self, node: u64, now: WaitMs) -> usize {
        self.inner
            .lock()
            .unwrap()
            .grants
            .values()
            .filter(|x| x.node == node && x.until > now)
            .count()
    }

    /// Every live grant (a release recalls them all first).
    pub fn all_live(&self, now: WaitMs) -> RecallNeed {
        let mut g = self.inner.lock().unwrap();
        g.grants.retain(|_, x| x.until > now);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        // A release waits for the kernel drain (a lone sequencer's cached
        // entries), not for the restart quarantine: a grant from before a
        // restart was capped by the lease being released, and releasing
        // it extends nothing.
        RecallNeed {
            grants: g.grants.values().copied().collect(),
            quarantine_until: (g.kernel_drain_until > now).then_some(g.kernel_drain_until),
        }
    }

    /// The grant was recalled (acked) or expired.
    pub fn forget(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        g.grants.remove(&id);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
    }

    pub fn is_live(&self, id: u64, now: WaitMs) -> bool {
        self.inner
            .lock()
            .unwrap()
            .grants
            .get(&id)
            .is_some_and(|x| x.until > now)
    }

    /// Grants from before a restart may be live until `until` (this node's
    /// wait clock).
    pub fn set_quarantine(&self, until: WaitMs) {
        let mut g = self.inner.lock().unwrap();
        g.quarantine_until = g.quarantine_until.max(until);
    }

    /// The later of the restart quarantine and the kernel drain.
    pub fn quarantine_until(&self) -> WaitMs {
        let g = self.inner.lock().unwrap();
        g.quarantine_until.max(g.kernel_drain_until)
    }

    /// Whether no other node has shown itself to this one yet. A lone
    /// sequencer's `cto=strict` mount keeps the kernel's attribute and
    /// entry caches (a short TTL): nothing but its own FUSE writes can
    /// change what it serves, and those go through the kernel. That is
    /// what makes strict mode free on a single node.
    pub fn is_alone(&self) -> bool {
        self.alone.load(Ordering::SeqCst)
    }

    /// Another node showed itself (a forwarded op, an inbox batch, a
    /// ReadIndex, a lease request, the roster, a peer link). From now on
    /// strict mounts answer the kernel with no cache TTL; entries the
    /// kernel cached before may live `drain_ms` longer, so every
    /// acknowledgement of a mutation (and a release) waits until then —
    /// once per mount. Returns the drain deadline when this call flipped.
    pub fn leave_alone(&self, now: WaitMs, drain_ms: u64) -> Option<WaitMs> {
        if !self.alone.swap(false, Ordering::SeqCst) {
            return None;
        }
        let until = now.plus(drain_ms);
        let mut g = self.inner.lock().unwrap();
        g.kernel_drain_until = g.kernel_drain_until.max(until);
        Some(until)
    }

    pub fn live_grants(&self) -> u64 {
        self.live_grants.load(Ordering::Relaxed)
    }

    /// Whether any grant is still honoured at `now` (wait clock): counted
    /// from the table, which `live_grants` is not (it is refreshed when
    /// the table changes, so it keeps counting grants that lapsed since).
    pub fn has_live_grant(&self, now: WaitMs) -> bool {
        self.inner
            .lock()
            .unwrap()
            .grants
            .values()
            .any(|g| g.until > now)
    }

    // ---- delegate side ----

    /// The recall generation to record when sending a ReadIndex.
    pub fn recall_gen(&self) -> u64 {
        self.inner.lock().unwrap().recall_gen
    }

    /// Install a granted delegation on `ino` — unless a recall arrived
    /// since the request was sent (`gen_at_send` is stale): the recall may
    /// have been for this very grant, having overtaken its reply, and the
    /// holder then believes it gone. Returns whether it was installed.
    pub fn install(&self, ino: u64, held: HeldDelegation, gen_at_send: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        g.renewing.remove(&ino);
        if g.recall_gen != gen_at_send {
            drop(g);
            self.stats.lock().unwrap().delegations_raced += 1;
            return false;
        }
        g.held.insert(ino, held);
        drop(g);
        self.stats.lock().unwrap().delegations_installed += 1;
        true
    }

    /// A recall: stop honouring the delegation on `ino` (every one when
    /// `None`) and bump the generation. Called before the ack is sent.
    pub fn recall(&self, ino: Option<u64>) {
        let mut g = self.inner.lock().unwrap();
        g.recall_gen += 1;
        match ino {
            Some(ino) => {
                g.held.remove(&ino);
            }
            None => g.held.clear(),
        }
        drop(g);
        self.stats.lock().unwrap().recalled += 1;
    }

    /// The delegation on `ino`, if it is honoured at `now` (a reading of
    /// this node's honour clock), and whether it is due for a background
    /// renewal (claimed here: only the first caller past the renewal point
    /// is told to renew).
    pub fn valid(&self, ino: u64, now: HonourMs) -> Option<(HeldDelegation, bool)> {
        let mut g = self.inner.lock().unwrap();
        let held = *g.held.get(&ino)?;
        if now >= held.until {
            g.held.remove(&ino);
            return None;
        }
        let mut renew = false;
        if now >= held.renew_at {
            let stale = g
                .renewing
                .get(&ino)
                .is_none_or(|since| now.since(*since) > 1_000);
            if stale {
                g.renewing.insert(ino, now);
                renew = true;
            }
        }
        Some((held, renew))
    }

    /// Whether a delegation on `ino` is honoured at `now`, with none of
    /// [`Self::valid`]'s effects (no renewal is claimed, a lapsed one is
    /// kept).
    pub fn is_held(&self, ino: u64, now: HonourMs) -> bool {
        self.inner
            .lock()
            .unwrap()
            .held
            .get(&ino)
            .is_some_and(|h| now < h.until)
    }

    /// A renewal attempt ended without a grant.
    pub fn renewal_done(&self, ino: u64) {
        self.inner.lock().unwrap().renewing.remove(&ino);
    }

    /// A segment of a newer epoch was applied: every delegation granted
    /// under an older one is void (an optimization — each was capped by
    /// the lease that backed it, which is what makes this safe anyway).
    pub fn void_below_epoch(&self, epoch: u64) {
        let mut g = self.inner.lock().unwrap();
        g.held.retain(|_, h| h.epoch >= epoch);
    }

    /// Plan 30 §M11 phase 2b: a delegate's generation ended (its
    /// `Recall` applied): every delegation it granted is void.
    pub fn void_epoch(&self, epoch: u64) {
        let mut g = self.inner.lock().unwrap();
        g.held.retain(|_, h| h.epoch != epoch);
    }

    pub fn held_count(&self) -> usize {
        self.inner.lock().unwrap().held.len()
    }

    // ---- counters ----

    pub fn stats(&self) -> CtoStats {
        *self.stats.lock().unwrap()
    }

    pub fn count(&self, f: impl FnOnce(&mut CtoStats)) {
        f(&mut self.stats.lock().unwrap());
    }

    /// One strict read that went to the sequencer, and how long it took
    /// (round trip plus the wait for the position).
    pub fn note_read_index(&self, ms: u64) {
        let mut s = self.stats.lock().unwrap();
        s.read_index += 1;
        s.read_index_ms_total += ms;
        s.read_index_ms[bucket(ms)] += 1;
    }
}

// ------------------------------------------------------------ Meta API

impl crate::store::Meta {
    /// [`recall_inos_of_op`] plus the inodes the op would change without
    /// naming them (M16, see [`note_victim`]), looked up now: for the
    /// paths that recall *before* executing (an inbox op, a delegate's
    /// write). The lookup precedes the op's own transaction, so a
    /// concurrent rename onto the same name in between could change the
    /// victim; the grant table's `blocked` entries and the entry's own
    /// recall (its parent is always included) bound what that can serve.
    pub fn recall_inos_of_op_now(&self, op: &crate::mutate::MutateOp) -> Vec<u64> {
        use crate::mutate::MutateOp;
        let mut inos = recall_inos_of_op(op);
        let mut child = |parent: u64, name: &str| {
            if let Ok(Some(ino)) = self.child_ino(parent, name) {
                inos.push(ino);
            }
        };
        match op {
            MutateOp::Unlink { parent, name } | MutateOp::Rmdir { parent, name } => {
                child(*parent, name)
            }
            MutateOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            }
            | MutateOp::Exchange {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                child(*parent, name);
                child(*new_parent, new_name);
            }
            _ => {}
        }
        inos.sort_unstable();
        inos.dedup();
        inos
    }
}

/// Local kv key: the latest `until` of any grant this node made, persisted
/// before the grant is answered (see [`crate::store::Meta::note_grant_horizon`]).
pub(crate) const KV_READ_GRANT_HORIZON: &str = "read_grant_horizon_ms";

/// Local kv key: the same for lock grants (plan 30 §M14), kept apart: a
/// lock grant may be honoured for much longer than a read delegation, and
/// only new lock grants wait for it, not acknowledgements.
pub(crate) const KV_LOCK_GRANT_HORIZON: &str = "lock_grant_horizon_ms";

/// Local kv key: where the takeover floor this tenure armed ends (plan 43
/// C7a, commit 4), a boot stamp like the two horizons above. The horizons
/// make a restart wait out what the floor protected (no acknowledgement, no
/// lock grant); this one is the floor itself, which also keeps the lease
/// from being released inside it (`Core::in_takeover_floor`).
pub(crate) const KV_TAKEOVER_FLOOR: &str = "takeover_floor_horizon_ms";

/// Local kv key: the epoch of a fast claim CAS that is to arm a takeover
/// floor, written before the CAS goes out and removed once the floor's
/// horizons are durable (plan 43 C7a, commit 4). The value is the epoch,
/// decimal; **readers ignore the tokens after it, writers may append**
/// (the format is append-only).
pub(crate) const KV_TAKEOVER_FLOOR_PENDING: &str = "takeover_floor_pending";

/// Tests only (`test-util`): fail this thread's next `n` writes of a
/// persisted horizon ([`crate::store::Meta::note_grant_horizon`] and its
/// kin) with an I/O error, nothing written: a store that cannot record what
/// a grant or a claim needs to survive a restart.
#[cfg(any(test, feature = "test-util"))]
pub fn fail_horizon_writes_on_this_thread_for_tests(n: u32) {
    horizon_fault::LEFT.with(|c| c.set(n));
}

#[cfg(any(test, feature = "test-util"))]
mod horizon_fault {
    thread_local! {
        pub(super) static LEFT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    }

    pub(super) fn take() -> bool {
        LEFT.with(|c| {
            let n = c.get();
            c.set(n.saturating_sub(1));
            n > 0
        })
    }
}

impl crate::store::Meta {
    /// This node's read-delegation tables (plan 30 §M8).
    pub fn read_delegations(&self) -> &ReadDelegations {
        &self.read_delegations
    }

    /// Persist that grants may be live until `until` (on this process's wait
    /// clock), before the grant that needs it is answered: a holder that
    /// restarts inside its own live lease re-adopts it under the same
    /// epoch, so its delegates learn nothing, and the restarted holder has
    /// lost its (in-memory) table. It waits this horizon out before
    /// acknowledging any mutation ([`Self::load_grant_quarantine`]). Written
    /// at most once per second of horizon growth, as a boot stamp
    /// ([`crate::store::horizon`]).
    pub fn note_grant_horizon(&self, until: WaitMs, now: Now) -> Result<(), crate::MetaError> {
        self.note_horizon(KV_READ_GRANT_HORIZON, until, now)
            .map(|_| ())
    }

    /// [`Self::note_grant_horizon`] for a lock grant (and a renewal): a
    /// holder that restarts inside its lease grants no lock until it has
    /// passed ([`Self::load_lock_quarantine`]); acknowledgements do not
    /// wait for it. Returns the horizon now durable (≥ `until`, on the wait
    /// clock). Called off the authority core
    /// (`Action::PersistLockHorizon`): the sync can take seconds on a loaded
    /// disk.
    pub fn note_lock_grant_horizon(
        &self,
        until: WaitMs,
        now: Now,
    ) -> Result<WaitMs, crate::MetaError> {
        self.note_horizon(KV_LOCK_GRANT_HORIZON, until, now)
    }

    /// Covers `until` with a stamped write unless what is already known
    /// (what the previous process left, loaded at start, and what this one
    /// wrote and synced) covers it. The write is for `until` rounded up a
    /// second, and since it is made only for an `until` beyond what is
    /// known and written it never lowers what the stored value required (a
    /// crash right after it must not forget the previous boot's obligation).
    ///
    /// Writes of one key are in value order: the key's writer lock is held
    /// across the read of the highest value written, the `kv_set` and the
    /// update of that value, so a writer that decided on a lower `until`
    /// before another wrote a higher one finds it, and writes nothing over
    /// it (the lock horizon has two writers, the core and the off-core
    /// persist lane). The sync is after, outside the lock, and covers every
    /// write before it; `known` (what a restart can rely on, and the answer)
    /// moves only once it is done.
    fn note_horizon(
        &self,
        key: &'static str,
        until: WaitMs,
        now: Now,
    ) -> Result<WaitMs, crate::MetaError> {
        let known = self.known_horizon(key);
        if until <= known {
            return Ok(known);
        }
        let value = {
            let mut written = self.horizon_writer(key).lock().unwrap();
            // What is stored, or on its way: another writer may have moved
            // it since `known` was read.
            let stored = (*written).max(self.known_horizon(key));
            let value = if until <= stored {
                stored
            } else {
                // Round up so continuous granting writes about once a second.
                let value = until.plus(1_000).max(stored);
                #[cfg(any(test, feature = "test-util"))]
                if horizon_fault::take() {
                    return Err(crate::MetaError::Io(std::io::Error::other(
                        "injected horizon write fault (testing only)",
                    )));
                }
                #[cfg(test)]
                {
                    let hook = self.horizon_write_hook.lock().unwrap().clone();
                    if let Some(hook) = hook {
                        hook(key, value);
                    }
                }
                self.kv_set(
                    key,
                    &crate::store::horizon::format_stamp(&now.stamp_of_wait(value)),
                )?;
                value
            };
            *written = value;
            value
        };
        // M16: synced — a holder that forgot it on a power loss would
        // acknowledge writes behind grants still honoured elsewhere. One
        // sync covers every write before it, this thread's and another's.
        self.sync()?;
        let mut known = self.horizon_wait.lock().unwrap();
        let slot = known.entry(key).or_insert(WaitMs(0));
        *slot = (*slot).max(value);
        Ok(value)
    }

    /// What is known of `key`'s horizon: loaded, or written and synced.
    fn known_horizon(&self, key: &str) -> WaitMs {
        self.horizon_wait
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(WaitMs(0))
    }

    /// The writer lock of `key` ([`Meta::horizon_written`]).
    fn horizon_writer(&self, key: &str) -> &std::sync::Mutex<WaitMs> {
        &self.horizon_written[match key {
            KV_READ_GRANT_HORIZON => 0,
            KV_LOCK_GRANT_HORIZON => 1,
            KV_TAKEOVER_FLOOR => 2,
            _ => unreachable!("not a horizon key: {key}"),
        }]
    }

    /// Persist that this tenure's takeover floor ends at `until` (on this
    /// process's wait clock; plan 43 C7a, commit 4), as a boot stamp, before
    /// the floor's work goes on: a restart inside the floor
    /// ([`Self::load_takeover_floor`]) is still inside it.
    pub fn note_takeover_floor(&self, until: WaitMs, now: Now) -> Result<(), crate::MetaError> {
        self.note_horizon(KV_TAKEOVER_FLOOR, until, now).map(|_| ())
    }

    /// At start: where the previous incarnation's takeover floor ends on this
    /// process's wait clock (see [`Self::load_grant_quarantine`] for how the
    /// stamp is read; `default_span_ms` for a value without a boot, the
    /// longest a floor leads its arming). `None`: none, or none left.
    pub fn load_takeover_floor(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        self.load_horizon(KV_TAKEOVER_FLOOR, now, default_span_ms)
    }

    /// The tenure that armed the floor is over (a release, a deposition):
    /// its floor goes with it (the quarantines it set run on). Not synced:
    /// a floor that comes back after a crash only holds a release back.
    pub fn clear_takeover_floor(&self) -> Result<(), crate::MetaError> {
        let mut written = self.horizon_writer(KV_TAKEOVER_FLOOR).lock().unwrap();
        self.horizon_wait.lock().unwrap().remove(KV_TAKEOVER_FLOOR);
        *written = WaitMs(0);
        self.kv_del(KV_TAKEOVER_FLOOR)
    }

    /// Plan 43 C7a, commit 4: a fast claim CAS for `epoch` is about to go
    /// out and may arm a floor when its marker lands; a crash between the
    /// two leaves nothing else to say that the successor owes one. Durable,
    /// before the CAS.
    pub fn set_takeover_floor_pending(&self, epoch: u64) -> Result<(), crate::MetaError> {
        self.kv_set_durable(KV_TAKEOVER_FLOOR_PENDING, &epoch.to_string())
    }

    /// The epoch [`Self::set_takeover_floor_pending`] named, if its mark is
    /// still there (a value that does not start with a number is none).
    pub fn takeover_floor_pending(&self) -> Option<u64> {
        let v = self.kv_get(KV_TAKEOVER_FLOOR_PENDING).ok().flatten()?;
        v.split_whitespace().next()?.parse().ok()
    }

    /// The floor's horizons are durable (or the CAS did not arm one): the
    /// mark goes. Not synced: a mark that survives a crash only makes the
    /// next re-adoption of that epoch wait a floor it did not owe.
    pub fn clear_takeover_floor_pending(&self) -> Result<(), crate::MetaError> {
        self.kv_del(KV_TAKEOVER_FLOOR_PENDING)
    }

    /// At start: grants the previous incarnation made may still be honoured
    /// until the persisted horizon; quarantine until then, on this node's
    /// wait clock: from the raw clock when it was written on this boot (a
    /// step of the wall clock, up or down, while the node was up or down
    /// moves nothing), the recorded span from now otherwise (another boot, or
    /// a value an older build wrote: `default_span_ms`, the longest a read
    /// delegation outlives its grant, plus the margin and a second). `None`:
    /// nothing was granted, or nothing is left of it.
    pub fn load_grant_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        let until = self.load_horizon(KV_READ_GRANT_HORIZON, now, default_span_ms)?;
        self.read_delegations.set_quarantine(until);
        Some(until)
    }

    /// At start: lock grants the previous incarnation made may still be
    /// honoured until the persisted lock horizon; no new lock grant until
    /// then (see [`Self::load_grant_quarantine`]; `default_span_ms` is the
    /// longest a lock or a delegation carried into a lock outlives its
    /// grant, plus the margin and a second).
    pub fn load_lock_quarantine(&self, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        let until = self.load_horizon(KV_LOCK_GRANT_HORIZON, now, default_span_ms)?;
        self.locks.set_quarantine(until);
        Some(until)
    }

    fn load_horizon(&self, key: &'static str, now: Now, default_span_ms: i64) -> Option<WaitMs> {
        let stored = self.kv_get(key).ok().flatten()?;
        let stamp = crate::store::horizon::parse_stamp(&stored, default_span_ms);
        let until = now.wait_of_stamp(&stamp);
        // A horizon that has passed is known (to a graceful shutdown) to
        // have passed.
        let mut known = self.horizon_wait.lock().unwrap();
        let slot = known.entry(key).or_insert(WaitMs(0));
        *slot = (*slot).max(until.unwrap_or(now.wait));
        until
    }

    /// At a graceful shutdown, after the core's final flush: a persisted
    /// horizon that has passed on this process's wait clock is deleted (a
    /// durable write), so that the next start, which could be a reboot,
    /// waits for nothing it does not owe: an idle node's restart is not
    /// delayed by a horizon that lapsed long ago. One that has not passed
    /// stays. Never from a crash path (nothing runs there), and never for a
    /// key this process neither loaded nor wrote (its stored value is not
    /// known to have passed).
    pub fn clear_passed_horizons(&self, now: Now) -> Result<(), crate::MetaError> {
        let mut cleared = false;
        for key in [
            KV_READ_GRANT_HORIZON,
            KV_LOCK_GRANT_HORIZON,
            KV_TAKEOVER_FLOOR,
        ] {
            let mut written = self.horizon_writer(key).lock().unwrap();
            let passed = self
                .horizon_wait
                .lock()
                .unwrap()
                .get(key)
                .is_some_and(|until| now.wait >= (*until).max(*written));
            if passed {
                self.kv_del(key)?;
                self.horizon_wait.lock().unwrap().remove(key);
                *written = WaitMs(0);
                cleared = true;
            }
        }
        if cleared {
            self.sync()?;
        }
        Ok(())
    }

    /// Whether this node's *unshipped* journal touched the record of any
    /// of `inos`: the part of a read-ahead answer's position that is not
    /// yet in a shipped segment, for the children it granted.
    pub fn unshipped_touches_inos(&self, inos: &[u64]) -> bool {
        let mine = self.unshipped.lock().unwrap();
        inos.iter().any(|i| mine.inos.contains(i))
    }

    /// Whether this node's *unshipped* journal touched `ino` (its record;
    /// with `dir`, also any entry in it), or the entry `(ino, name)`: the
    /// part of a ReadIndex position that is not yet in any shipped
    /// segment. Conservative like [`Self::unshipped_overlaps`].
    pub fn unshipped_touches_read(
        &self,
        ino: u64,
        dir: bool,
        child: Option<(&str, Option<u64>)>,
    ) -> bool {
        let mine = self.unshipped.lock().unwrap();
        if mine.inos.contains(&ino) {
            return true;
        }
        if dir && mine.dentries.iter().any(|(p, _)| *p == ino) {
            return true;
        }
        if let Some((name, child_ino)) = child {
            if mine.dentries.iter().any(|(p, n)| *p == ino && n == name) {
                return true;
            }
            if child_ino.is_some_and(|c| mine.inos.contains(&c)) {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_types::clock::{HonourMs, Now, WaitMs};

    #[test]
    fn a_create_recalls_its_parent_directory() {
        let recs = vec![crate::record::LogRecord::Create {
            parent: 1,
            name: "a".into(),
            ino: 7,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }];
        assert_eq!(recall_inos(&recs), vec![1, 7]);
        let unlink = vec![crate::record::LogRecord::Unlink {
            parent: 3,
            name: "b".into(),
            time_ns: 1,
        }];
        assert_eq!(recall_inos(&unlink), vec![3]);
    }

    #[test]
    fn grant_many_makes_ordinary_grants_and_is_held_has_no_effects() {
        let t = ReadDelegations::default();
        let made = t.grant_many(2, &[10, 11, 12], WaitMs(1_000), WaitMs(0));
        assert_eq!(
            made.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            [10, 11, 12]
        );
        assert_eq!(t.live_grants(), 3);
        // A grant the node holds live is not made twice; another node's
        // grant, or a lapsed one, is no reason to skip.
        let again = t.grant_many(2, &[10, 13], WaitMs(1_000), WaitMs(500));
        assert_eq!(again.iter().map(|(i, _)| *i).collect::<Vec<_>>(), [13]);
        assert_eq!(t.grant_many(3, &[10], WaitMs(1_000), WaitMs(500)).len(), 1);
        assert_eq!(
            t.grant_many(2, &[10], WaitMs(3_000), WaitMs(1_000)).len(),
            1
        );
        // Touching one inode recalls exactly its grant.
        let need = t.touching(&[11], None, WaitMs(0));
        assert_eq!(
            need.grants.iter().map(|g| g.id).collect::<Vec<_>>(),
            [made[1].1]
        );
        // `is_held` neither claims a renewal nor drops a lapsed delegation.
        let held = HeldDelegation {
            until: HonourMs(100),
            position: Position::default(),
            epoch: 1,
            holder: 1,
            grant: 1,
            renew_at: HonourMs(10),
        };
        assert!(t.install(10, held, t.recall_gen()));
        assert!(t.is_held(10, HonourMs(50)));
        assert!(!t.is_held(10, HonourMs(100)));
        assert!(
            t.valid(10, HonourMs(50)).unwrap().1,
            "the renewal is still unclaimed"
        );
        assert_eq!(t.held_count(), 1, "a lapsed one is kept by is_held");
        assert!(!t.is_held(10, HonourMs(100)) && t.held_count() == 1);
    }

    #[test]
    fn touching_filters_by_ino_node_and_time() {
        let t = ReadDelegations::default();
        let a = t.grant(2, 10, WaitMs(1_000));
        let _b = t.grant(3, 11, WaitMs(1_000));
        let c = t.grant(4, 10, WaitMs(500));
        let need = t.touching(&[10], Some(9), WaitMs(400));
        let ids: Vec<u64> = need.grants.iter().map(|g| g.id).collect();
        assert_eq!(ids, vec![a, c]);
        // The writer's own grant is not recalled.
        let need = t.touching(&[10], Some(2), WaitMs(400));
        assert_eq!(
            need.grants.iter().map(|g| g.id).collect::<Vec<_>>(),
            vec![c]
        );
        // Expired grants are gone.
        let need = t.touching(&[10], None, WaitMs(600));
        assert_eq!(
            need.grants.iter().map(|g| g.id).collect::<Vec<_>>(),
            vec![a]
        );
        assert_eq!(t.live_grants(), 2);
        t.forget(a);
        assert!(t.touching(&[10], None, WaitMs(600)).is_empty());
    }

    #[test]
    fn quarantine_blocks_every_ack_until_it_passes() {
        let t = ReadDelegations::default();
        t.set_quarantine(WaitMs(2_000));
        let need = t.touching(&[1], None, WaitMs(1_000));
        assert!(need.grants.is_empty());
        assert_eq!(need.quarantine_until, Some(WaitMs(2_000)));
        assert!(t.touching(&[1], None, WaitMs(2_000)).is_empty());
    }

    fn held(until: i64) -> HeldDelegation {
        HeldDelegation {
            until: HonourMs(until),
            position: Position::ZERO,
            epoch: 1,
            holder: 1,
            grant: 1,
            renew_at: HonourMs(until - 100),
        }
    }

    #[test]
    fn a_recall_overtaking_its_grant_voids_the_install() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.recall(Some(5));
        assert!(!t.install(5, held(1_000), gen));
        assert!(t.valid(5, HonourMs(0)).is_none());
        let gen = t.recall_gen();
        assert!(t.install(5, held(1_000), gen));
        assert!(t.valid(5, HonourMs(0)).is_some());
        assert_eq!(t.stats().delegations_raced, 1);
    }

    #[test]
    fn validity_expiry_and_one_renewal_at_a_time() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.install(5, held(1_000), gen);
        assert_eq!(t.valid(5, HonourMs(100)).map(|(_, r)| r), Some(false));
        assert_eq!(t.valid(5, HonourMs(950)).map(|(_, r)| r), Some(true));
        assert_eq!(
            t.valid(5, HonourMs(960)).map(|(_, r)| r),
            Some(false),
            "claimed once"
        );
        assert!(t.valid(5, HonourMs(1_000)).is_none(), "expired");
        assert_eq!(t.held_count(), 0);
    }

    #[test]
    fn a_newer_epoch_voids_older_delegations() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.install(5, held(1_000), gen);
        t.void_below_epoch(2);
        assert!(t.valid(5, HonourMs(0)).is_none());
    }

    /// The holder's own manifest commit (the FUSE flush, not
    /// `execute_mutate`) joins the unshipped key set, so a ReadIndex for
    /// the file includes the unshipped journal position.
    #[test]
    fn a_direct_manifest_commit_is_unshipped() {
        use crate::MetaStore;
        let meta = crate::store::Meta::open_in_memory().unwrap();
        meta.set_holder_epoch(1);
        let f = meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        assert!(!meta.unshipped_touches_read(f.ino, false, None));
        meta.set_manifest_dirty(f.ino, None, b"M", 1, None, None, &[])
            .unwrap();
        assert!(meta.unshipped_touches_read(f.ino, false, None));
    }

    #[test]
    fn the_grant_horizon_persists_and_quarantines() {
        let meta = crate::store::Meta::open_in_memory().unwrap();
        // One boot throughout (raw clock = wait clock = the numbers).
        let at = |ms: i64| Now::same_on_boot(ms, 1);
        assert_eq!(meta.load_grant_quarantine(at(0), 7_500), None);
        meta.note_grant_horizon(WaitMs(5_000), at(0)).unwrap();
        meta.note_grant_horizon(WaitMs(5_500), at(0)).unwrap();
        // The horizon is 6 000 (the write is rounded up a second): 5 000 ms
        // left at 1 000.
        assert_eq!(
            meta.load_grant_quarantine(at(1_000), 7_500),
            Some(WaitMs(6_000))
        );
        assert_eq!(meta.read_delegations().quarantine_until(), WaitMs(6_000));
        assert_eq!(meta.load_grant_quarantine(at(7_000), 7_500), None);
    }

    /// Lock grants keep a horizon of their own: it quarantines new lock
    /// grants after a restart, not acknowledgements.
    #[test]
    fn the_lock_grant_horizon_is_kept_apart() {
        let meta = crate::store::Meta::open_in_memory().unwrap();
        let at = |ms: i64| Now::same_on_boot(ms, 1);
        assert_eq!(meta.load_lock_quarantine(at(0), 27_500), None);
        // The horizon now durable: the write's (rounded up), then the
        // stored one for a lower ask.
        assert_eq!(
            meta.note_lock_grant_horizon(WaitMs(21_000), at(0)).unwrap(),
            WaitMs(22_000)
        );
        assert_eq!(
            meta.note_lock_grant_horizon(WaitMs(21_500), at(0)).unwrap(),
            WaitMs(22_000)
        );
        assert_eq!(meta.load_grant_quarantine(at(1_000), 7_500), None);
        assert_eq!(meta.read_delegations().quarantine_until(), WaitMs(0));
        assert_eq!(
            meta.load_lock_quarantine(at(1_000), 27_500),
            Some(WaitMs(22_000))
        );
        assert_eq!(meta.locks().quarantine_until(), WaitMs(22_000));
        assert_eq!(meta.load_lock_quarantine(at(23_000), 27_500), None);
    }

    /// The lock horizon has two writers, the core and the off-core persist
    /// lane. One that decided on a lower `until` and is held on its way to
    /// the store while the other writes and syncs a higher one must not
    /// write its lower value over it: the stored horizon, what a restart
    /// waits, never decreases, and `known` never claims more than the disk
    /// holds. Fails on the parent of the monotonic write: the held writer
    /// (decided on 22 000 from a `known` of 0) overwrites 31 000.
    #[test]
    fn a_held_lower_horizon_write_does_not_lower_a_higher_one() {
        use std::sync::mpsc;
        use std::time::Duration;
        let meta = std::sync::Arc::new(crate::store::Meta::open_in_memory().unwrap());
        let at = |ms: i64| Now::same_on_boot(ms, 1);
        let stored = |m: &crate::store::Meta| {
            let v = m.kv_get(KV_LOCK_GRANT_HORIZON).unwrap().unwrap();
            at(0)
                .wait_of_stamp(&crate::store::horizon::parse_stamp(&v, 27_500))
                .unwrap()
        };
        // The first writer is held just before its write reaches the store.
        let (held_tx, held_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let held_tx = std::sync::Mutex::new(held_tx);
        let go_rx = std::sync::Mutex::new(go_rx);
        *meta.horizon_write_hook.lock().unwrap() = Some(std::sync::Arc::new(move |key, v| {
            if key == KV_LOCK_GRANT_HORIZON && v == WaitMs(22_000) {
                held_tx.lock().unwrap().send(()).unwrap();
                let _ = go_rx.lock().unwrap().recv();
            }
        }));
        let first = {
            let meta = meta.clone();
            std::thread::spawn(move || meta.note_lock_grant_horizon(WaitMs(21_000), at(0)))
        };
        held_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        // The other writer wants more. Where writes of the key are
        // serialised it waits for the held one; otherwise it is done at once.
        let (done_tx, done_rx) = mpsc::channel();
        let second = {
            let meta = meta.clone();
            std::thread::spawn(move || {
                let r = meta.note_lock_grant_horizon(WaitMs(30_000), at(0));
                done_tx.send(()).unwrap();
                r
            })
        };
        let _ = done_rx.recv_timeout(Duration::from_millis(300));
        go_tx.send(()).unwrap();
        assert_eq!(first.join().unwrap().unwrap(), WaitMs(22_000));
        assert_eq!(second.join().unwrap().unwrap(), WaitMs(31_000));
        assert_eq!(
            stored(&meta),
            WaitMs(31_000),
            "the stored lock horizon was lowered by a writer that had decided earlier"
        );
        assert_eq!(meta.known_horizon(KV_LOCK_GRANT_HORIZON), WaitMs(31_000));
        // A later, lower ask changes nothing.
        assert_eq!(
            meta.note_lock_grant_horizon(WaitMs(25_000), at(0)).unwrap(),
            WaitMs(31_000)
        );
        assert_eq!(stored(&meta), WaitMs(31_000));
    }
}

/// Plan 43 C7a: the restart horizons are boot stamps, read on the raw clock
/// on the boot they were written on and as their recorded span after
/// another, whatever the wall clock did meanwhile (Appendix B row 14). Each
/// "restart" is a second `Meta` over the first one's directory.
#[cfg(test)]
mod boot_stamped_horizon_tests {
    use super::*;
    use crate::store::Meta;
    use constellation_types::clock::{BootNow, Ms};

    /// A reading on `boot`, the raw clock at `raw` ms since boot, the wait
    /// clock at `wait` and the wall at `wall`.
    fn at(boot: u64, raw: i64, wait: i64, wall: i64) -> Now {
        Now {
            wall: Ms(wall),
            honour: HonourMs(wall),
            wait: WaitMs(wait),
            boot: BootNow {
                id: boot,
                raw_ms: raw,
                budget_ppm: 0,
            },
        }
    }

    const WALL: i64 = 1_780_000_000_000;
    /// The lock horizon's default span (what an older build's value waits).
    const LOCK_SPAN: i64 = 27_500;
    const READ_SPAN: i64 = 7_500;

    /// The previous process: boot 7, raw 100 000 ms since boot, wait clock
    /// 5 000, grants out until wait 25 500 (lock) and 10 500 (read). The
    /// writes round up a second. Returns the directory kept for the restart.
    fn first_process() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 100_000, 5_000, WALL);
        assert_eq!(
            meta.note_lock_grant_horizon(WaitMs(25_500), now).unwrap(),
            WaitMs(26_500)
        );
        meta.note_grant_horizon(WaitMs(10_500), now).unwrap();
        dir
    }

    /// Expectation (fail-before: the wall form): the lock horizon is not
    /// moved by a forward step of the wall clock while the node was down, on
    /// the same boot. The previous process wrote 21 500 ms ahead at raw 100
    /// 000; the new one starts at raw 103 000 (3 s later) with the wall
    /// stepped 10 s on: 18 500 ms are left. On the parent the horizon was a
    /// wall value read back against the stepped wall: 8 500.
    #[test]
    fn a_lock_horizon_survives_a_forward_step_while_down() {
        let dir = first_process();
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 103_000, 400, WALL + 3_000 + 10_000);
        let until = meta.load_lock_quarantine(now, LOCK_SPAN).unwrap();
        assert_eq!(until.since(now.wait), 18_500);
        assert_eq!(meta.locks().quarantine_until(), until);
        // A step backward does not stretch it either.
        let meta2 = {
            drop(meta);
            Meta::open(dir.path()).unwrap()
        };
        let now = at(7, 103_000, 400, WALL + 3_000 - 3_600_000);
        assert_eq!(
            meta2
                .load_lock_quarantine(now, LOCK_SPAN)
                .unwrap()
                .since(now.wait),
            18_500
        );
    }

    /// Plan 43 C7a (commit 4). Expectation: a takeover floor is persisted as a
    /// boot stamp and read like the horizons: on the same boot from the raw
    /// clock (a wall step, either way, while the node was up or down moves
    /// nothing), after a reboot as the span recorded; one that has passed
    /// is none. Fail-before: nothing was persisted (the mutant that
    /// drops the write reads `None` at once).
    #[test]
    fn a_takeover_floor_survives_a_restart_and_a_wall_step() {
        let dir = tempfile::tempdir().unwrap();
        let meta = Meta::open(dir.path()).unwrap();
        let first = at(7, 100_000, 5_000, WALL);
        meta.note_takeover_floor(WaitMs(25_500), first).unwrap();
        drop(meta);
        // Same boot, 3 s on, the wall 10 s forward: 23 500 - 3 000 + 1 000
        // (the write rounds up) = 21 500 left.
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 103_000, 400, WALL + 13_000);
        let until = meta.load_takeover_floor(now, 30_000).unwrap();
        assert_eq!(until.since(now.wait), 18_500);
        // Another boot: the whole span (21 500 at the write), from now.
        drop(meta);
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(8, 3_000, 400, WALL + 13_000);
        let until = meta.load_takeover_floor(now, 30_000).unwrap();
        assert_eq!(until.since(now.wait), 21_500);
        // Past it on the same boot: none.
        drop(meta);
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 130_000, 400, WALL);
        assert_eq!(meta.load_takeover_floor(now, 30_000), None);
        // The tenure ended: no floor to come back.
        meta.clear_takeover_floor().unwrap();
        let now = at(7, 103_000, 400, WALL);
        assert_eq!(meta.load_takeover_floor(now, 30_000), None);
    }

    /// Plan 43 C7a (commit 4). Expectation: the mark of a claim CAS names its
    /// epoch (append-only: tokens after the number are ignored, a value that
    /// is no number is no mark) and goes when cleared.
    #[test]
    fn the_claim_mark_names_its_epoch() {
        let meta = Meta::open_in_memory().unwrap();
        assert_eq!(meta.takeover_floor_pending(), None);
        meta.set_takeover_floor_pending(7).unwrap();
        assert_eq!(meta.takeover_floor_pending(), Some(7));
        meta.kv_set(KV_TAKEOVER_FLOOR_PENDING, "9 future tokens")
            .unwrap();
        assert_eq!(meta.takeover_floor_pending(), Some(9));
        meta.kv_set(KV_TAKEOVER_FLOOR_PENDING, "garbage").unwrap();
        assert_eq!(meta.takeover_floor_pending(), None);
        meta.set_takeover_floor_pending(8).unwrap();
        meta.clear_takeover_floor_pending().unwrap();
        assert_eq!(meta.takeover_floor_pending(), None);
    }

    /// Plan 43 C7a (commit 4). Expectation: a graceful shutdown clears a
    /// floor that has passed and keeps one that has not, like the horizons.
    #[test]
    fn a_graceful_shutdown_clears_a_passed_floor_and_keeps_a_live_one() {
        let dir = tempfile::tempdir().unwrap();
        let meta = Meta::open(dir.path()).unwrap();
        let first = at(7, 100_000, 5_000, WALL);
        meta.note_takeover_floor(WaitMs(25_500), first).unwrap();
        // Live at wait 20 000: kept.
        meta.clear_passed_horizons(at(7, 115_000, 20_000, WALL))
            .unwrap();
        assert!(meta.kv_get(KV_TAKEOVER_FLOOR).unwrap().is_some());
        // Passed (the write was rounded up to 26 500): cleared.
        meta.clear_passed_horizons(at(7, 122_000, 27_000, WALL))
            .unwrap();
        assert_eq!(meta.kv_get(KV_TAKEOVER_FLOOR).unwrap(), None);
    }

    /// Expectation (fail-before: the wall form): a forward step while the
    /// node is *up*, after the last write, then a restart: the same, the
    /// horizon is read on the raw clock. The previous process went on for 2
    /// s after its write (a step of +10 s, covered by no new write), crashed
    /// at raw 102 500 and the next started at raw 103 000.
    #[test]
    fn a_lock_horizon_survives_a_forward_step_after_the_last_write() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            let t0 = at(7, 100_000, 5_000, WALL);
            let durable = meta.note_lock_grant_horizon(WaitMs(25_500), t0).unwrap();
            // 2 s on, the wall stepped +10 s: a renewal inside the horizon
            // asks for less than is durable and writes nothing.
            let t1 = at(7, 102_000, 7_000, WALL + 2_000 + 10_000);
            let n = meta.sync_count();
            assert_eq!(
                meta.note_lock_grant_horizon(WaitMs(24_000), t1).unwrap(),
                durable
            );
            assert_eq!(meta.sync_count(), n);
        }
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 103_000, 400, WALL + 3_000 + 10_000);
        let until = meta.load_lock_quarantine(now, LOCK_SPAN).unwrap();
        // Written at raw 100 000 for 21 500 ms ahead: 18 500 are left at
        // raw 103 000.
        assert_eq!(until.since(now.wait), 18_500);
    }

    /// Expectation: after a reboot (another boot id) the horizon is the
    /// whole recorded span from the start of the process, whether the raw
    /// clock of the new boot reads less or more than the old one's and the
    /// wall has stepped back or on.
    #[test]
    fn a_read_horizon_after_a_reboot_waits_its_span() {
        for (raw, wall) in [(3_000, WALL - 5_000), (900_000, WALL + 9_000_000)] {
            let dir = first_process();
            let meta = Meta::open(dir.path()).unwrap();
            let now = at(8, raw, 50, wall);
            // The read grant's horizon was written for wait 11 500 (10 500 and
            // the second), 6 500 ms ahead of 5 000: that span.
            let until = meta.load_grant_quarantine(now, READ_SPAN).unwrap();
            assert_eq!(until.since(now.wait), 6_500, "raw {raw}");
            assert_eq!(meta.read_delegations().quarantine_until(), until);
            // The lock horizon: its span, 21 500.
            let locks = meta.load_lock_quarantine(now, LOCK_SPAN).unwrap();
            assert_eq!(locks.since(now.wait), 21_500);
        }
        // A process that does not know its boot waits the span too.
        let dir = first_process();
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(0, 103_000, 50, WALL);
        assert_eq!(
            meta.load_grant_quarantine(now, READ_SPAN)
                .unwrap()
                .since(now.wait),
            6_500
        );
    }

    /// Expectation: a value an older build wrote (the wall end alone,
    /// however far in the past the wall says it is) waits the caller's
    /// default span; garbage the same; neither is trusted for a boot.
    #[test]
    fn an_old_format_horizon_waits_the_default_span() {
        let dir = tempfile::tempdir().unwrap();
        {
            let meta = Meta::open(dir.path()).unwrap();
            meta.kv_set(KV_LOCK_GRANT_HORIZON, "1700000000000").unwrap();
            meta.kv_set(KV_READ_GRANT_HORIZON, "soon").unwrap();
        }
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 103_000, 400, WALL);
        assert_eq!(
            meta.load_lock_quarantine(now, LOCK_SPAN)
                .unwrap()
                .since(now.wait),
            LOCK_SPAN
        );
        assert_eq!(
            meta.load_grant_quarantine(now, READ_SPAN)
                .unwrap()
                .since(now.wait),
            READ_SPAN
        );
    }

    /// Expectation (pitfall 6): a write never lowers what the stored value
    /// required. The restarted process loads 18 500 ms of lock horizon; a
    /// grant that needs less asks for nothing to be written (the stored
    /// stamp is untouched); one that needs more writes a stamp covering it,
    /// and a second restart reads the later of the two.
    #[test]
    fn a_horizon_write_never_lowers_the_loaded_quarantine() {
        let dir = first_process();
        let stored = Meta::open(dir.path())
            .unwrap()
            .kv_get(KV_LOCK_GRANT_HORIZON)
            .unwrap();
        let meta = Meta::open(dir.path()).unwrap();
        let now = at(7, 103_000, 400, WALL + 3_000);
        let loaded = meta.load_lock_quarantine(now, LOCK_SPAN).unwrap();
        assert_eq!(loaded.0, 400 + 18_500);
        // A grant ending before it: nothing written, the answer is the
        // loaded horizon.
        let n = meta.sync_count();
        let t1 = at(7, 104_000, 1_400, WALL + 4_000);
        assert_eq!(
            meta.note_lock_grant_horizon(WaitMs(1_400 + 5_000), t1)
                .unwrap(),
            loaded
        );
        assert_eq!(meta.sync_count(), n);
        assert_eq!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap(), stored);
        // One beyond it: written (rounded up), covering it.
        let want = WaitMs(loaded.0 + 4_000);
        let durable = meta.note_lock_grant_horizon(want, t1).unwrap();
        assert_eq!(durable, want.plus(1_000));
        drop(meta);
        // A crash now and a restart 2 s on: the later horizon.
        let meta = Meta::open(dir.path()).unwrap();
        let t2 = at(7, 106_000, 100, WALL + 6_000);
        let until = meta.load_lock_quarantine(t2, LOCK_SPAN).unwrap();
        // Written at raw 104 000 for `durable - 1 400` ahead.
        assert_eq!(until.since(t2.wait), durable.since(t1.wait) - 2_000);
        // (The loaded horizon alone would have had 15 500 left by then.)
        assert!(until.since(t2.wait) > 15_500);
    }

    /// Expectation (D3b): a graceful shutdown deletes a horizon that has
    /// passed on the wait clock, so that the next start (a reboot) waits for
    /// nothing; one still live stays and is read as before.
    #[test]
    fn a_graceful_shutdown_clears_a_passed_horizon() {
        let dir = first_process();
        let meta = Meta::open(dir.path()).unwrap();
        // Loaded here (so known), and live at 1 s: nothing to clear yet.
        let t = |ms: i64| at(7, 100_000 + ms, 5_000 + ms, WALL + ms);
        meta.load_lock_quarantine(t(0), LOCK_SPAN);
        meta.load_grant_quarantine(t(0), READ_SPAN);
        meta.clear_passed_horizons(t(1_000)).unwrap();
        assert!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap().is_some());
        assert!(meta.kv_get(KV_READ_GRANT_HORIZON).unwrap().is_some());
        // Past the lock horizon (21 500 after its write) and the read one:
        // both go, durably.
        let n = meta.sync_count();
        meta.clear_passed_horizons(t(30_000)).unwrap();
        assert_eq!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap(), None);
        assert_eq!(meta.kv_get(KV_READ_GRANT_HORIZON).unwrap(), None);
        assert!(meta.sync_count() > n);
        drop(meta);
        // After a reboot: nothing to wait for.
        let meta = Meta::open(dir.path()).unwrap();
        let after = at(9, 1_000, 10, WALL + 3_600_000);
        assert_eq!(meta.load_lock_quarantine(after, LOCK_SPAN), None);
        assert_eq!(meta.load_grant_quarantine(after, READ_SPAN), None);
    }

    /// Expectation: only a horizon that has passed is cleared: a live lock
    /// horizon stays (a node that stops gracefully with grants out still
    /// owes its successor the wait), as does one this process never loaded
    /// or wrote.
    #[test]
    fn a_graceful_shutdown_keeps_a_live_horizon() {
        let dir = first_process();
        let meta = Meta::open(dir.path()).unwrap();
        let t = |ms: i64| at(7, 100_000 + ms, 5_000 + ms, WALL + ms);
        // Never loaded in this process: not known to have passed.
        meta.clear_passed_horizons(t(60_000)).unwrap();
        assert!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap().is_some());
        assert!(meta.kv_get(KV_READ_GRANT_HORIZON).unwrap().is_some());
        // Written in this process, 21 500 ms ahead: live at 20 000, gone at
        // 21 500.
        let w = at(7, 200_000, 10_000, WALL + 100_000);
        let durable = meta.note_lock_grant_horizon(WaitMs(30_500), w).unwrap();
        assert_eq!(durable, WaitMs(31_500));
        meta.clear_passed_horizons(at(7, 220_000, 30_000, WALL))
            .unwrap();
        assert!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap().is_some());
        meta.clear_passed_horizons(at(7, 221_500, 31_500, WALL))
            .unwrap();
        assert_eq!(meta.kv_get(KV_LOCK_GRANT_HORIZON).unwrap(), None);
        // The read horizon was never loaded here either: stays.
        assert!(meta.kv_get(KV_READ_GRANT_HORIZON).unwrap().is_some());
    }
}
