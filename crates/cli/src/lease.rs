//! Lease state machine for the mounting node (DESIGN.md §4/§5).
//!
//! [`LeaseKeeper`] owns this node's relationship with `leases/p0.json`:
//! acquire, renew at half-TTL, release on clean unmount or after an
//! idle period, and — the case that matters — notice when it has been
//! *deposed*.
//!
//! Two rules are load-bearing and are enforced here rather than by
//! convention:
//!
//! 1. **Takeover ordering.** Claiming a lease whose previous holder was
//!    a different node is only legal after applying everything that
//!    holder flushed. [`LeaseKeeper::commit`] refuses such a claim
//!    unless the caller hands over a [`TailedToHead`] witness, which
//!    only the shipper's tail produces.
//! 2. **Deposition is terminal.** Once a renew CAS fails and the lease
//!    turns out to belong to somebody else, this node never ships
//!    again: its unshipped journal is a stranded branch whose
//!    reintegration is phase 4, and silently discarding or replaying it
//!    would be data loss either way.
//!
//! The FUSE threads do not talk to S3. They read [`LeaseView`], a
//! handful of atomics, on every mutating op (nanoseconds) and only fall
//! back to the sync task's channel when the view says the lease is not
//! currently valid.
//!
//! ### Leases are sticky (plan 26 Step 7)
//!
//! A holder used to hand the lease back after 30 s without a mutation,
//! whether or not anyone wanted it. For the overwhelmingly common
//! single-writer-at-a-time mount that bought nothing and cost the next
//! write three S3 round trips (GET lease → tail → CAS PUT) on the FUSE
//! path. Idle release is now *conditional*: it fires only once a peer has
//! recorded itself in [`Lease::wanted_by`], and never before
//! [`LEASE_MIN_DWELL_MS`] of tenure, so two nodes alternating writes
//! cannot ping-pong the lease between them.
//!
//! Worst-case reasoning, with P2P down: a requester registers `wanted_by`
//! at its first `Acquire` (one CAS round trip); the holder notices at its
//! next renewal (at most TTL/2 = 30 s away), finishes any in-flight batch
//! and releases; the requester claims on its next retry. Today's worst
//! case was the same 30 s (the unconditional idle release) or an EIO after
//! 2×TTL if the holder was busy. So sticky leases are never worse than
//! what they replace, and they remove the 3-round-trip re-acquire from the
//! common path entirely. With P2P up, forwarding (ADR-14) means the
//! requester never needs the lease at all, and the existing `HandOff`
//! request remains the fast path — it does not go through `wanted_by`.
//!
//! Note what the requester's edit does *not* rely on: it is a CAS swap
//! that can lose, and a lost swap is simply dropped (we try again on the
//! next `Acquire`). Nothing here treats a 412 as a fast failure — measured
//! against AWS a stale `If-Match` takes 599 ms to come back rejected, four
//! times a plain GET, server-side and not the client's retries — so the
//! conflict paths retry on a later round rather than in a tight loop.

use anyhow::{bail, Result};
use constellation_store_s3::lease::{lease_ttl_ms, now_unix_ms, LeaseMode};
use constellation_store_s3::{Lease, LeaseStore, LeaseTag, StoreError};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Default write-idle period after which a holder hands the lease back
/// so a peer can take it without waiting out the TTL.
pub const DEFAULT_IDLE_RELEASE_MS: u64 = 30_000;

/// Minimum tenure before a holder may idle-release, however long it has
/// been idle and however loudly a peer is asking. Two nodes writing to the
/// same partition in turn would otherwise trade the lease on every lull,
/// and each trade is a CAS pair plus an epoch bump that fences whatever
/// the previous holder had not yet flushed.
pub const LEASE_MIN_DWELL_MS: u64 = 5_000;

/// Treat the lease as unusable this close to expiry: renewal happens at
/// half-TTL, so a mutation landing inside the margin should route
/// through the sync task instead of racing the clock.
const EXPIRY_MARGIN_MS: i64 = 1_000;

pub fn idle_release_ms() -> u64 {
    std::env::var("CONSTELLATION_LEASE_IDLE_RELEASE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_IDLE_RELEASE_MS)
}

/// Lock-free lease snapshot shared with the FUSE threads and the
/// control API.
#[derive(Debug, Default)]
pub struct LeaseView {
    /// Unix ms until which this node holds the lease; 0 = not held.
    valid_until_ms: AtomicI64,
    holder: AtomicU64,
    epoch: AtomicU64,
    lost: AtomicBool,
    /// Unix ms of the last gated mutation, for idle release.
    last_write_ms: AtomicI64,
    /// Continuation-epoch local authority (no S3 lease object).
    epoch_held: AtomicBool,
}

impl LeaseView {
    /// Usable right now, with enough margin to finish an op.
    pub fn usable(&self) -> bool {
        !self.lost.load(Ordering::Relaxed)
            && (self.epoch_held.load(Ordering::Relaxed)
                || self.valid_until_ms.load(Ordering::Relaxed) - now_unix_ms() > EXPIRY_MARGIN_MS)
    }

    pub fn is_lost(&self) -> bool {
        self.lost.load(Ordering::Relaxed)
    }

    /// Record write activity; keeps the idle-release timer from firing
    /// under a running workload.
    pub fn touch(&self) {
        self.last_write_ms.store(now_unix_ms(), Ordering::Relaxed);
    }

    pub fn idle_for_ms(&self) -> i64 {
        now_unix_ms() - self.last_write_ms.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> constellation_api::LeaseStatus {
        let now = now_unix_ms();
        let until = self.valid_until_ms.load(Ordering::Relaxed);
        constellation_api::LeaseStatus {
            held: until > now,
            holder: self.holder.load(Ordering::Relaxed),
            epoch: self.epoch.load(Ordering::Relaxed),
            expires_in_ms: if until > 0 { until - now } else { 0 },
            lost: self.lost.load(Ordering::Relaxed),
        }
    }

    fn set_held(&self, lease: &Lease) {
        self.holder.store(lease.holder, Ordering::Relaxed);
        self.epoch.store(lease.epoch, Ordering::Relaxed);
        self.valid_until_ms
            .store(lease.expires_unix_ms, Ordering::Relaxed);
    }

    fn clear(&self) {
        self.valid_until_ms.store(0, Ordering::Relaxed);
        self.epoch_held.store(false, Ordering::Relaxed);
    }
}

/// What the current lease object allows this node to do.
#[derive(Debug)]
pub enum Plan {
    /// Already held by us and still valid.
    Held,
    /// No lease object exists: CAS-create one.
    Create,
    /// Claimable (released, expired, or our own from a previous life).
    /// `needs_tail` marks a genuine takeover from another node.
    Claim {
        prev: Lease,
        tag: LeaseTag,
        needs_tail: bool,
    },
    /// Another node holds an unexpired lease. `prev`/`tag` are carried so
    /// the caller can register itself in [`Lease::wanted_by`] without a
    /// second read: the classify GET already paid for them.
    Busy {
        holder: u64,
        expires_in_ms: i64,
        prev: Lease,
        tag: LeaseTag,
    },
}

impl Plan {
    /// True when committing this plan takes authority away from another
    /// node, and therefore requires having applied its flushed log.
    pub fn needs_tail(&self) -> bool {
        matches!(
            self,
            Plan::Claim {
                needs_tail: true,
                ..
            }
        )
    }
}

/// Proof that the shared log has been applied up to head. Only the
/// shipper's tail can mint one, which is what makes takeover ordering a
/// compile-time-shaped requirement rather than a comment.
pub struct TailedToHead(());

impl TailedToHead {
    pub(crate) fn witness() -> Self {
        Self(())
    }
}

pub struct LeaseKeeper {
    store: LeaseStore,
    node_id: u64,
    ttl_ms: u64,
    idle_release_ms: u64,
    view: Arc<LeaseView>,
    /// The lease we believe we hold, with the tag needed to swap it.
    held: Option<(Lease, LeaseTag)>,
    /// When the current tenure began; gates [`LEASE_MIN_DWELL_MS`].
    held_since: Option<Instant>,
    /// Node ids waiting for this partition, as of the last lease object we
    /// read. Empty means nobody is asking, and a sticky lease is kept.
    wanted: Vec<u64>,
    /// Open continuation-epoch promise forbids S3 takeover (DESIGN.md §5.3).
    takeover_gate: Arc<AtomicBool>,
    /// Diagnostic tag naming the mechanism behind the next acquisition
    /// (fuse-acquire, claim-offer, ship-journal, ...). Logged by
    /// [`Self::commit`] so lease churn can be attributed from logs alone.
    acquire_reason: &'static str,
}

impl LeaseKeeper {
    pub fn new(store: LeaseStore, node_id: u64) -> Self {
        Self {
            store,
            node_id,
            ttl_ms: lease_ttl_ms(),
            idle_release_ms: idle_release_ms(),
            view: Arc::new(LeaseView::default()),
            held: None,
            held_since: None,
            wanted: Vec::new(),
            takeover_gate: Arc::new(AtomicBool::new(false)),
            acquire_reason: "unspecified",
        }
    }

    /// Tag the mechanism that is about to acquire (purely diagnostic).
    pub fn note_acquire_reason(&mut self, reason: &'static str) {
        self.acquire_reason = reason;
    }

    pub fn share_takeover_gate(&mut self, gate: Arc<AtomicBool>) {
        self.takeover_gate = gate;
    }

    pub fn view(&self) -> Arc<LeaseView> {
        self.view.clone()
    }

    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// Epoch to stamp into a segment, or `None` when this node may not
    /// ship (no lease, expired, or deposed).
    pub fn ship_epoch(&self) -> Option<u64> {
        if self.view.is_lost() {
            return None;
        }
        if self.view.epoch_held.load(Ordering::Relaxed) {
            let e = self.view.epoch.load(Ordering::Relaxed);
            return Some(e.max(1));
        }
        let (lease, _) = self.held.as_ref()?;
        (self.view.usable() && lease.holder == self.node_id).then_some(lease.epoch)
    }

    pub fn holds_authority(&self) -> bool {
        self.view.usable()
    }

    pub fn authority_epoch(&self) -> u64 {
        self.view.epoch.load(Ordering::Relaxed).max(1)
    }

    pub fn is_lost(&self) -> bool {
        self.view.is_lost()
    }

    pub fn force_lost(&mut self) {
        self.held = None;
        self.view.clear();
        self.view.lost.store(true, Ordering::Relaxed);
    }

    pub fn clear_lost(&mut self) {
        self.view.lost.store(false, Ordering::Relaxed);
    }

    /// Epoch-local authority: no S3 CAS. Used while a continuation epoch
    /// is the authority root (handoff without shipping).
    pub fn adopt_epoch_hold(&mut self, epoch: u64) {
        self.view.lost.store(false, Ordering::Relaxed);
        self.view.holder.store(self.node_id, Ordering::Relaxed);
        self.view.epoch.store(epoch.max(1), Ordering::Relaxed);
        self.view
            .valid_until_ms
            .store(now_unix_ms() + 365 * 24 * 3600 * 1000, Ordering::Relaxed);
        self.view.epoch_held.store(true, Ordering::Relaxed);
        self.view.touch();
    }

    pub fn release_local(&mut self) {
        self.held = None;
        self.view.clear();
    }

    pub fn extend_local(&mut self) {
        if self.holds_authority() {
            self.view
                .valid_until_ms
                .store(now_unix_ms() + 365 * 24 * 3600 * 1000, Ordering::Relaxed);
            self.view.epoch_held.store(true, Ordering::Relaxed);
        }
    }

    /// Read the lease and decide what this node may do with it.
    pub async fn classify(&self) -> Result<Plan> {
        if self.view.is_lost() {
            bail!("this node was deposed as lease holder; refusing to reacquire");
        }
        let now = now_unix_ms();
        let Some((prev, tag)) = self.store.get().await? else {
            return Ok(Plan::Create);
        };
        if prev.holder == self.node_id && !prev.released && !prev.is_expired(now) {
            // Our own live lease: either we already track it, or we are
            // a restart of the process that took it.
            if self.held.is_some() {
                return Ok(Plan::Held);
            }
            return Ok(Plan::Claim {
                needs_tail: false,
                prev,
                tag,
            });
        }
        if !prev.is_claimable(now) {
            return Ok(Plan::Busy {
                holder: prev.holder,
                expires_in_ms: prev.expires_in_ms(now),
                prev,
                tag,
            });
        }
        let needs_tail = prev.holder != 0 && prev.holder != self.node_id;
        if needs_tail && self.takeover_gate.load(Ordering::Relaxed) {
            tracing::warn!(
                holder = prev.holder,
                "refusing S3 lease takeover: an open continuation-epoch promise is binding"
            );
            return Ok(Plan::Busy {
                holder: prev.holder,
                expires_in_ms: prev.expires_in_ms(now),
                prev,
                tag,
            });
        }
        Ok(Plan::Claim {
            prev,
            tag,
            needs_tail,
        })
    }

    /// Commit an acquisition. `tailed` must be present for a takeover
    /// from another node: the new holder may only start writing once it
    /// has applied everything the old one flushed (DESIGN.md §4).
    pub async fn commit(&mut self, plan: Plan, tailed: Option<TailedToHead>) -> Result<bool> {
        let (lease, result) = match plan {
            Plan::Held => return Ok(true),
            Plan::Busy { .. } => return Ok(false),
            Plan::Create => {
                let lease = Lease::granted(self.store.partition(), self.node_id, 1, self.ttl_ms);
                let r = self.store.try_create(&lease).await;
                (lease, r)
            }
            Plan::Claim {
                prev,
                tag,
                needs_tail,
            } => {
                if needs_tail && tailed.is_none() {
                    bail!(
                        "refusing to take over partition {} from node {} without \
                         applying its flushed log first",
                        self.store.partition(),
                        prev.holder
                    );
                }
                if needs_tail && self.store.mode() == LeaseMode::SingleWriter {
                    bail!(
                        "backend has no If-Match support, so lease takeover from node {} \
                         cannot be made safe; mount this filesystem from one node at a \
                         time or use a backend with etag CAS (see `constellation doctor`)",
                        prev.holder
                    );
                }
                // Same holder re-adopting keeps the epoch (nothing
                // changed hands); a real handover bumps it so the
                // predecessor's late segments are recognizable.
                let epoch = if prev.holder == self.node_id && !prev.released {
                    prev.epoch.max(1)
                } else {
                    prev.epoch + 1
                };
                let lease =
                    Lease::granted(self.store.partition(), self.node_id, epoch, self.ttl_ms);
                let r = self.store.try_swap(&lease, &tag).await;
                (lease, r)
            }
        };
        match result {
            Ok(tag) => {
                tracing::info!(
                    holder = lease.holder,
                    epoch = lease.epoch,
                    ttl_ms = self.ttl_ms,
                    reason = self.acquire_reason,
                    "acquired partition lease"
                );
                self.view.set_held(&lease);
                self.view.touch();
                // A fresh grant answers every pending request by definition
                // (`Lease::granted` clears `wanted_by`), and starts the
                // dwell clock that keeps the next one from being answered
                // the instant it arrives.
                self.wanted.clear();
                self.held_since = Some(Instant::now());
                self.held = Some((lease, tag));
                self.refresh_condemned().await;
                Ok(true)
            }
            // Somebody else got there first; the caller retries.
            Err(StoreError::CasConflict) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Renew when past half-TTL. Detects deposition.
    pub async fn renew_if_due(&mut self) -> Result<()> {
        if self.view.epoch_held.load(Ordering::Relaxed) {
            self.extend_local();
            return Ok(());
        }
        let Some((lease, _)) = self.held.as_ref() else {
            return Ok(());
        };
        if lease.expires_in_ms(now_unix_ms()) > (self.ttl_ms / 2) as i64 {
            return Ok(());
        }
        self.renew_now().await
    }

    /// Renew unconditionally (also the deposition probe: a CAS failure
    /// here is how a frozen-then-resumed holder learns it is out).
    pub async fn renew_now(&mut self) -> Result<()> {
        let Some((lease, tag)) = self.held.take() else {
            return Ok(());
        };
        let renewed = lease.renewed(self.ttl_ms);
        match self.store.try_swap(&renewed, &tag).await {
            Ok(tag) => {
                self.view.set_held(&renewed);
                self.held = Some((renewed, tag));
                self.refresh_condemned().await;
                Ok(())
            }
            Err(StoreError::CasConflict) => {
                // Not necessarily a deposition any more: a peer that wants
                // this partition edits `wanted_by` in place, which changes
                // the etag and so fails exactly this CAS. Re-read before
                // concluding anything, and keep the view intact until we
                // know — clearing it first would stall the FUSE threads on
                // a lease we still hold.
                if let Some((cur, fresh_tag)) = self.store.get().await? {
                    if cur.holder == self.node_id
                        && cur.epoch == lease.epoch
                        && !cur.released
                        && !cur.is_expired(now_unix_ms())
                    {
                        self.wanted = cur.wanted_by.clone();
                        tracing::debug!(
                            wanted_by = ?self.wanted,
                            epoch = cur.epoch,
                            "renew CAS lost to a handoff request, not a takeover"
                        );
                        let renewed = cur.renewed(self.ttl_ms);
                        return match self.store.try_swap(&renewed, &fresh_tag).await {
                            Ok(tag) => {
                                self.view.set_held(&renewed);
                                self.held = Some((renewed, tag));
                                self.refresh_condemned().await;
                                Ok(())
                            }
                            // Lost again: somebody is moving faster than we
                            // can read. Fall back to the deposition probe,
                            // which is authoritative.
                            Err(StoreError::CasConflict) => {
                                self.view.clear();
                                self.diagnose_lost_renew(&lease).await
                            }
                            Err(e) => {
                                self.held = Some((cur, fresh_tag));
                                Err(e.into())
                            }
                        };
                    }
                }
                self.view.clear();
                self.diagnose_lost_renew(&lease).await
            }
            Err(e) => {
                // Transient store failure: keep the lease we have and
                // retry on the next tick (it is still unexpired).
                self.held = Some((lease, tag));
                Err(e.into())
            }
        }
    }

    /// Record `self.node_id` in a *foreign* holder's lease object: the
    /// S3-only way of asking for a partition somebody else is sitting on.
    /// Only `wanted_by` changes — holder, epoch and expiry are copied
    /// across — so this can never move write authority, and a losing CAS
    /// is not an error: somebody else edited or took the lease in the
    /// meantime and the next `Acquire` re-reads it anyway.
    ///
    /// Returns whether the request landed.
    pub async fn register_wanted(&self, prev: &Lease, tag: &LeaseTag) -> Result<bool> {
        match self.store.try_swap(&prev.wanting(self.node_id), tag).await {
            Ok(_) => Ok(true),
            Err(StoreError::CasConflict) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn refresh_condemned(&self) {
        match constellation_store_s3::read_condemned(&self.store.inner()).await {
            Ok(Some(list)) => {
                tracing::debug!(
                    epoch = list.epoch,
                    hashes = list.hashes.len(),
                    "refreshed GC condemned pointer at lease renewal"
                );
            }
            Ok(None) => {}
            Err(error) => {
                // Renewal already succeeded. Upload-time refresh remains the
                // safety gate, so a transient read failure here costs only
                // the early freshness hint.
                tracing::debug!(%error, "could not refresh GC condemned pointer");
            }
        }
    }

    /// A renew CAS failed. Re-read: a different holder or epoch means
    /// this node is out, permanently.
    async fn diagnose_lost_renew(&mut self, mine: &Lease) -> Result<()> {
        match self.store.get().await? {
            Some((cur, tag)) if cur.holder == self.node_id && cur.epoch == mine.epoch => {
                // Our own object, only the tag was stale (a retried PUT
                // landing twice). Adopt the fresh tag and carry on.
                self.view.set_held(&cur);
                self.held = Some((cur, tag));
                Ok(())
            }
            Some((cur, _)) => {
                self.mark_lost(cur.holder, cur.epoch, mine.epoch);
                Ok(())
            }
            None => {
                // The lease object vanished (manual surgery / GC). Treat
                // it as deposition: we cannot prove we still have
                // authority, and something else clearly rewrote history.
                self.mark_lost(0, 0, mine.epoch);
                Ok(())
            }
        }
    }

    fn mark_lost(&mut self, holder: u64, epoch: u64, my_epoch: u64) {
        self.held = None;
        self.view.clear();
        self.view.lost.store(true, Ordering::Relaxed);
        self.view.holder.store(holder, Ordering::Relaxed);
        self.view.epoch.store(epoch, Ordering::Relaxed);
        tracing::error!(
            new_holder = holder,
            new_epoch = epoch,
            my_epoch,
            node = self.node_id,
            "LEASE LOST: another node took write authority for this partition. \
             Refusing to ship further segments. Unshipped local writes stay in \
             the journal for reintegration (not yet implemented)."
        );
    }

    /// Write-idle, nothing pending, **and somebody is waiting**: hand the
    /// lease back rather than making them sit out the TTL.
    ///
    /// The requester condition is what makes leases sticky (see the module
    /// doc). Without it an idle holder gave up write authority that nobody
    /// else wanted, and paid three S3 round trips on the FUSE path to take
    /// it back the moment it wrote again. The dwell floor bounds the other
    /// direction: a lease handed over cannot be handed back immediately.
    pub fn idle_release_due(&self, journal_backlog: u64) -> bool {
        !self.view.epoch_held.load(Ordering::Relaxed)
            && self.held.is_some()
            && journal_backlog == 0
            && !self.wanted.is_empty()
            && self.view.idle_for_ms() >= self.idle_release_ms as i64
            && self.held_for_ms() >= LEASE_MIN_DWELL_MS as i64
    }

    /// How long this node has held the current lease, in ms. `0` when it
    /// holds none, which keeps [`Self::idle_release_due`] false there.
    fn held_for_ms(&self) -> i64 {
        self.held_since
            .map(|at| at.elapsed().as_millis() as i64)
            .unwrap_or(0)
    }

    /// Test hook: make the two *timers* in [`Self::idle_release_due`] read
    /// as elapsed without sleeping through them. Only the timers — whether
    /// a requester is registered, and whether the journal is drained, is
    /// what the tests using this are about.
    #[cfg(test)]
    pub(crate) fn expire_idle_timers_for_test(&mut self) {
        self.idle_release_ms = 0;
        self.held_since =
            Some(Instant::now() - std::time::Duration::from_millis(LEASE_MIN_DWELL_MS));
    }

    /// Node ids currently recorded as waiting for this partition.
    #[cfg(test)]
    pub(crate) fn wanted_by(&self) -> &[u64] {
        &self.wanted
    }

    /// Give the lease up (idle release, or clean unmount after the final
    /// flush). Best effort: an unreleased lease merely costs the next
    /// holder a TTL wait.
    ///
    /// On a successful CAS swap, returns the new object's ETag so a
    /// peer can skip a classify GET and claim against that version.
    pub async fn release(&mut self) -> Result<Option<String>> {
        let Some((lease, tag)) = self.held.take() else {
            return Ok(None);
        };
        self.view.clear();
        // `Lease::released` drops `wanted_by`: the partition is free, so
        // every pending request has just been answered.
        self.wanted.clear();
        self.held_since = None;
        match self.store.try_swap(&lease.released(), &tag).await {
            Ok(new_tag) => {
                tracing::info!(epoch = lease.epoch, "released partition lease");
                Ok(new_tag.etag())
            }
            Err(StoreError::CasConflict) => {
                self.diagnose_lost_renew(&lease).await?;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_store_s3::{LeaseMode, LeaseStore};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn open_epoch_promise_refuses_s3_takeover() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let a = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
        let mut ka = LeaseKeeper::new(a, 1);
        assert!(ka.commit(Plan::Create, None).await.unwrap());
        ka.release().await.unwrap();

        let b = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let mut kb = LeaseKeeper::new(b, 2);
        kb.share_takeover_gate(Arc::new(AtomicBool::new(true)));
        match kb.classify().await.unwrap() {
            Plan::Busy { holder, .. } => assert_eq!(holder, 1),
            other => panic!("takeover must be refused, got {other:?}"),
        }
    }
}
