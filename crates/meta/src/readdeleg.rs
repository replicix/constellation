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
//! All times are unix milliseconds on this node's clock (the core's `now`,
//! `now_unix_ms()` on FUSE threads). The holder records a grant as live
//! until `granted_at + ttl + margin`; the delegate honours it only until
//! `sent_at + ttl − margin`, measured from when it *sent* the request —
//! both margins the lease's (`crates/model/src/cto.rs` has the argument
//! and the model that checks it).

use crate::session::Position;
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
    /// Live until this node's clock reaches it (`granted + ttl + margin`).
    pub until_ms: i64,
}

/// What a mutation's acknowledgement must wait for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecallNeed {
    /// Live grants on the touched inodes, held by other nodes.
    pub grants: Vec<ReadGrant>,
    /// Grants this node may have made before it restarted are unknown;
    /// every acknowledgement waits until this passes.
    pub quarantine_until: Option<i64>,
}

impl RecallNeed {
    pub fn is_empty(&self) -> bool {
        self.grants.is_empty() && self.quarantine_until.is_none()
    }
}

/// A delegation this node holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeldDelegation {
    /// Honoured while this node's clock is below this.
    pub until_ms: i64,
    /// The position the replica must reach before a read under it.
    pub position: Position,
    pub epoch: u64,
    pub holder: u64,
    pub grant: u64,
    /// When the lease-backed TTL started (the request's send time): past
    /// half its lifetime, a read under it renews it in the background.
    pub renew_at_ms: i64,
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
    quarantine_until: i64,
    /// See [`ReadDelegations::leave_alone`].
    kernel_drain_until: i64,
    held: HashMap<u64, HeldDelegation>,
    /// Bumped by every recall received: a reply whose grant a recall
    /// overtook is not installed (see [`ReadDelegations::install`]).
    recall_gen: u64,
    /// Renewals in flight (one per ino).
    renewing: HashMap<u64, i64>,
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

    /// Record a grant to `node` on `ino`, live until `until_ms`. Must be
    /// called **before** the position the answer carries is read.
    pub fn grant(&self, node: u64, ino: u64, until_ms: i64) -> u64 {
        let mut g = self.inner.lock().unwrap();
        g.next_grant += 1;
        let id = g.next_grant;
        g.grants.insert(
            id,
            ReadGrant {
                id,
                node,
                ino,
                until_ms,
            },
        );
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        drop(g);
        self.stats.lock().unwrap().grants += 1;
        id
    }

    /// What acknowledging a mutation that touched `inos` must wait for,
    /// at `now_ms`: every live grant on one of them held by a node other
    /// than `except` (the writer's own delegation is not recalled — its
    /// reads are read-your-writes), and the restart quarantine. Expired
    /// grants are pruned on the way.
    pub fn touching(&self, inos: &[u64], except: Option<u64>, now_ms: i64) -> RecallNeed {
        let mut g = self.inner.lock().unwrap();
        g.grants.retain(|_, x| x.until_ms > now_ms);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        let q = g.quarantine_until.max(g.kernel_drain_until);
        let quarantine_until = (q > now_ms).then_some(q);
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

    /// Every live grant (a release recalls them all first).
    pub fn all_live(&self, now_ms: i64) -> RecallNeed {
        let mut g = self.inner.lock().unwrap();
        g.grants.retain(|_, x| x.until_ms > now_ms);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
        // A release waits for the kernel drain (a lone sequencer's cached
        // entries), not for the restart quarantine: a grant from before a
        // restart was capped by the lease being released, and releasing
        // it extends nothing.
        RecallNeed {
            grants: g.grants.values().copied().collect(),
            quarantine_until: (g.kernel_drain_until > now_ms).then_some(g.kernel_drain_until),
        }
    }

    /// The grant was recalled (acked) or expired.
    pub fn forget(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        g.grants.remove(&id);
        self.live_grants
            .store(g.grants.len() as u64, Ordering::Relaxed);
    }

    pub fn is_live(&self, id: u64, now_ms: i64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .grants
            .get(&id)
            .is_some_and(|x| x.until_ms > now_ms)
    }

    /// Grants from before a restart may be live until `until_ms`.
    pub fn set_quarantine(&self, until_ms: i64) {
        let mut g = self.inner.lock().unwrap();
        g.quarantine_until = g.quarantine_until.max(until_ms);
    }

    /// The later of the restart quarantine and the kernel drain.
    pub fn quarantine_until(&self) -> i64 {
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
    pub fn leave_alone(&self, now_ms: i64, drain_ms: u64) -> Option<i64> {
        if !self.alone.swap(false, Ordering::SeqCst) {
            return None;
        }
        let until = now_ms + drain_ms as i64;
        let mut g = self.inner.lock().unwrap();
        g.kernel_drain_until = g.kernel_drain_until.max(until);
        Some(until)
    }

    pub fn live_grants(&self) -> u64 {
        self.live_grants.load(Ordering::Relaxed)
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

    /// The delegation on `ino`, if it is honoured at `now_ms`, and whether
    /// it is due for a background renewal (claimed here: only the first
    /// caller past the renewal point is told to renew).
    pub fn valid(&self, ino: u64, now_ms: i64) -> Option<(HeldDelegation, bool)> {
        let mut g = self.inner.lock().unwrap();
        let held = *g.held.get(&ino)?;
        if now_ms >= held.until_ms {
            g.held.remove(&ino);
            return None;
        }
        let mut renew = false;
        if now_ms >= held.renew_at_ms {
            let stale = g
                .renewing
                .get(&ino)
                .is_none_or(|since| now_ms - since > 1_000);
            if stale {
                g.renewing.insert(ino, now_ms);
                renew = true;
            }
        }
        Some((held, renew))
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

/// Local kv key: the latest `until` of any grant this node made, persisted
/// before the grant is answered (see [`crate::store::Meta::note_grant_horizon`]).
pub(crate) const KV_READ_GRANT_HORIZON: &str = "read_grant_horizon_ms";

impl crate::store::Meta {
    /// This node's read-delegation tables (plan 30 §M8).
    pub fn read_delegations(&self) -> &ReadDelegations {
        &self.read_delegations
    }

    /// Persist that grants may be live until `until_ms`, before the grant
    /// that needs it is answered: a holder that restarts inside its own
    /// live lease re-adopts it under the same epoch, so its delegates
    /// learn nothing, and the restarted holder has lost its (in-memory)
    /// table. It waits this horizon out before acknowledging any mutation
    /// ([`Self::load_grant_quarantine`]). Written at most once per second
    /// of horizon growth.
    pub fn note_grant_horizon(&self, until_ms: i64) -> Result<(), crate::MetaError> {
        let stored = self
            .kv_get(KV_READ_GRANT_HORIZON)?
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);
        if until_ms > stored {
            // Round up so continuous granting writes about once a second.
            let value = until_ms + 1_000;
            self.kv_set(KV_READ_GRANT_HORIZON, &value.to_string())?;
        }
        Ok(())
    }

    /// At start: grants the previous incarnation made may still be live
    /// until the persisted horizon; quarantine until then.
    pub fn load_grant_quarantine(&self, now_ms: i64) -> Option<i64> {
        let until = self
            .kv_get(KV_READ_GRANT_HORIZON)
            .ok()
            .flatten()
            .and_then(|v| v.parse::<i64>().ok())?;
        (until > now_ms).then(|| {
            self.read_delegations.set_quarantine(until);
            until
        })
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
    fn touching_filters_by_ino_node_and_time() {
        let t = ReadDelegations::default();
        let a = t.grant(2, 10, 1_000);
        let _b = t.grant(3, 11, 1_000);
        let c = t.grant(4, 10, 500);
        let need = t.touching(&[10], Some(9), 400);
        let ids: Vec<u64> = need.grants.iter().map(|g| g.id).collect();
        assert_eq!(ids, vec![a, c]);
        // The writer's own grant is not recalled.
        let need = t.touching(&[10], Some(2), 400);
        assert_eq!(
            need.grants.iter().map(|g| g.id).collect::<Vec<_>>(),
            vec![c]
        );
        // Expired grants are gone.
        let need = t.touching(&[10], None, 600);
        assert_eq!(
            need.grants.iter().map(|g| g.id).collect::<Vec<_>>(),
            vec![a]
        );
        assert_eq!(t.live_grants(), 2);
        t.forget(a);
        assert!(t.touching(&[10], None, 600).is_empty());
    }

    #[test]
    fn quarantine_blocks_every_ack_until_it_passes() {
        let t = ReadDelegations::default();
        t.set_quarantine(2_000);
        let need = t.touching(&[1], None, 1_000);
        assert!(need.grants.is_empty());
        assert_eq!(need.quarantine_until, Some(2_000));
        assert!(t.touching(&[1], None, 2_000).is_empty());
    }

    fn held(until: i64) -> HeldDelegation {
        HeldDelegation {
            until_ms: until,
            position: Position::ZERO,
            epoch: 1,
            holder: 1,
            grant: 1,
            renew_at_ms: until - 100,
        }
    }

    #[test]
    fn a_recall_overtaking_its_grant_voids_the_install() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.recall(Some(5));
        assert!(!t.install(5, held(1_000), gen));
        assert!(t.valid(5, 0).is_none());
        let gen = t.recall_gen();
        assert!(t.install(5, held(1_000), gen));
        assert!(t.valid(5, 0).is_some());
        assert_eq!(t.stats().delegations_raced, 1);
    }

    #[test]
    fn validity_expiry_and_one_renewal_at_a_time() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.install(5, held(1_000), gen);
        assert_eq!(t.valid(5, 100).map(|(_, r)| r), Some(false));
        assert_eq!(t.valid(5, 950).map(|(_, r)| r), Some(true));
        assert_eq!(t.valid(5, 960).map(|(_, r)| r), Some(false), "claimed once");
        assert!(t.valid(5, 1_000).is_none(), "expired");
        assert_eq!(t.held_count(), 0);
    }

    #[test]
    fn a_newer_epoch_voids_older_delegations() {
        let t = ReadDelegations::default();
        let gen = t.recall_gen();
        t.install(5, held(1_000), gen);
        t.void_below_epoch(2);
        assert!(t.valid(5, 0).is_none());
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
        meta.set_manifest_dirty(f.ino, None, b"M", 1, &[]).unwrap();
        assert!(meta.unshipped_touches_read(f.ino, false, None));
    }

    #[test]
    fn the_grant_horizon_persists_and_quarantines() {
        let meta = crate::store::Meta::open_in_memory().unwrap();
        assert_eq!(meta.load_grant_quarantine(0), None);
        meta.note_grant_horizon(5_000).unwrap();
        meta.note_grant_horizon(5_500).unwrap();
        assert_eq!(meta.load_grant_quarantine(1_000), Some(6_000));
        assert_eq!(meta.read_delegations().quarantine_until(), 6_000);
        assert_eq!(meta.load_grant_quarantine(7_000), None);
    }
}
