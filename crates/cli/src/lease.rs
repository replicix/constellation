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
//!
//! ### Locking rules for the keepers map (plan 30 M2b)
//!
//! `node_runtime`/`main::run_sync_round` share one
//! `tokio::sync::Mutex<HashMap<String, LeaseKeeper>>` ("the keepers
//! lock") across the periodic sync task and every forwarded mutation's
//! dispatch (`node_runtime::dispatch_mutate`/`dispatch_forward`, plan 30
//! M2b item 1). What each side may assume:
//!
//! - **Renewal never needs exclusion.** A still-valid lease stays valid
//!   for the whole time its renewal CAS is in flight, so a forwarded
//!   execute may run concurrently with one. `run_sync_round` therefore
//!   only takes the lock to call [`LeaseKeeper::prepare_renew`] (a
//!   synchronous read that hands back an owned [`RenewAttempt`]), drops
//!   it for [`RenewAttempt::run`] (the actual CAS, needing no
//!   `LeaseKeeper` reference at all — see that type's doc for why reads
//!   made during it are still safe), then re-takes it only to call
//!   [`LeaseKeeper::apply_renew`]. A dispatch that reads
//!   `ship_epoch()`/`is_lost()` while the CAS is in flight sees the old,
//!   still-accurate state — exactly as if the round had not started
//!   renewing yet.
//! - **Ordinary shipping never needs exclusion either.** The segment PUT
//!   only drains whatever was in the journal as of the moment it was
//!   read; a mutation landing in the journal while the PUT is in flight
//!   is simply picked up by a later round. `Shipper::run_ordinary_round`
//!   (in `shipper.rs`) takes the lock only to snapshot which partitions
//!   are held and at what epoch, then ships using that snapshot with the
//!   lock released.
//! - **Release and handoff are the one case that does need exclusion.**
//!   Both end with a final flush (draining the journal to zero) followed
//!   by a release CAS; if a forwarded execute landed in the gap between
//!   those two steps, it would be journaled under an epoch this node is
//!   about to give up, and nobody would ever ship it (the invariant this
//!   whole scheme protects: "an accepted op never lands after the final
//!   flush of a release/handoff"). `run_sync_round`'s release/handoff
//!   pass therefore keeps holding the keepers lock across the entire
//!   decision, the final flush (`Shipper::ship_atime_before_release`, or
//!   the forced drain's own `sync_all`), and [`LeaseKeeper::release`]'s
//!   CAS — unchanged from before this milestone. [`LeaseKeeper::release`]
//!   also fences the view *before* its own CAS (see that method's doc),
//!   which is what closes the FUSE-local fast path the instant a release
//!   begins, independent of the keepers lock.
//! - Every other keepers-lock user (`SyncRequest::Acquire`/`HandOff`/
//!   `ClaimOffer`/`Leave`/`Reintegrate` in `node_runtime.rs`) still
//!   cancels the in-flight round before running (plan 30 M2b item 1 only
//!   exempts `Mutate`/`Forward`), so it never overlaps a round's own
//!   locking at all and needs no new reasoning here.

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

/// How long a registered requester may go unanswered before the holder
/// stops waiting for its own write-idle window and hands the lease over at
/// the next drained batch.
///
/// Without this, `idle_release_due` only fired on the idle timer, so under
/// sustained contention each holder kept the lease for
/// `CONSTELLATION_LEASE_IDLE_RELEASE_MS` (30 s by default) and a third
/// node could sit behind two full tenures — past the FUSE acquire deadline
/// of 2xTTL, which surfaces to userspace as EIO on a write. `chaos-ci`
/// reproduces exactly that: two waiters gave up 121.27 s in, 74 us apart.
///
/// Plan 26 Step 7's prose already said the holder "finishes any in-flight
/// batch, releases"; its code block said to wait out the idle window too.
/// This is the prose, bounded: the idle threshold still governs while
/// nobody has been waiting long, and `LEASE_MIN_DWELL_MS` still bounds the
/// handoff rate, so a lease cannot be traded more than once per dwell.
pub const LEASE_WANTED_GRACE_MS: u64 = 5_000;

/// How long [`LeaseKeeper::begin_handoff_pause`] closes this node's own
/// FUSE fast path while the sync task forces a stuck backlog to zero.
///
/// `idle_release_due`'s `journal_backlog == 0` term is right for the
/// common case (a genuinely idle holder), but under sustained local
/// traffic — a create storm hammering the very node that holds the
/// lease — `journal_backlog` can read non-zero on every single round
/// indefinitely: new local mutations land straight in the journal via
/// the fast path (no lock the sync task holds), so nothing ever forces a
/// gap. `LEASE_WANTED_GRACE_MS` alone cannot fix this: it bounds how
/// long a *check* waits before being willing to release, not whether the
/// backlog the check inspects is ever actually zero. Without this pause,
/// three-way contention with P2P unavailable could starve a waiter past
/// the FUSE acquire deadline (2xTTL) and surface as EIO on an otherwise
/// healthy, merely busy cluster — `create-storm-s3-only` reproduces
/// exactly that (plan 29 M3c).
///
/// Self-expiring rather than manually cleared, for two independent
/// reasons:
///
/// 1. The sync task's `select!` can drop a round mid-flight for a
///    fresher request (same hazard `renew_now`/`release` were fixed for
///    in M3b), and a pause that only ever got cleared on the success
///    path would wedge this node's own writes forever if a round doing
///    the draining got cancelled first.
/// 2. It must survive a *successful* release, not just a failed one:
///    once this node hands the lease back, its own blocked local writes
///    are sitting in the same `Acquire` queue as the actual waiter and
///    would otherwise race it for the reclaim on equal footing — and,
///    being local, tend to win, since the waiter's retry is on its own
///    independent backoff schedule while the pausing node's blocked
///    write is released to retry the instant the CAS succeeds. Keeping
///    the pause running past the release gives the waiter this window
///    uncontested.
pub const HANDOFF_PAUSE_MS: i64 = 2_000;

/// Treat the lease as unusable this close to expiry: renewal happens at
/// half-TTL, so a mutation landing inside the margin should route
/// through the sync task instead of racing the clock.
const EXPIRY_MARGIN_MS: i64 = 1_000;

/// [`EXPIRY_MARGIN_MS`], clamped to a quarter of the configured TTL.
///
/// A fixed 1 s margin is wider than a short TTL: at
/// `CONSTELLATION_LEASE_TTL_MS=200` (snapshot-churn uses it to keep GC's
/// condemned-list wait short) the lease was *never* usable, so the node
/// never shipped a segment and its journal grew without bound. Renewal at
/// half-TTL still leaves a quarter of it as headroom. Read once: this is
/// on every gated FUSE mutation.
fn expiry_margin_ms() -> i64 {
    static MARGIN: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MARGIN.get_or_init(|| EXPIRY_MARGIN_MS.min(lease_ttl_ms() as i64 / 4))
}

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
    /// Unix ms until which [`LeaseKeeper::begin_handoff_pause`] has
    /// closed the ordinary (non-epoch) fast path; 0 or past means open.
    handoff_pause_until_ms: AtomicI64,
}

impl LeaseView {
    /// Usable right now, with enough margin to finish an op.
    ///
    /// Deliberately blind to [`LeaseKeeper::begin_handoff_pause`]: this
    /// is what [`LeaseKeeper::ship_epoch`] (and so the shipper's
    /// authority to ship/ack the existing journal) is built on, and a
    /// pause that also closed *this* would stop the very drain it
    /// exists for from ever making progress — see
    /// [`Self::open_for_new_mutation`] for the gate that does watch it.
    pub fn usable(&self) -> bool {
        if self.lost.load(Ordering::Relaxed) {
            return false;
        }
        if self.epoch_held.load(Ordering::Relaxed) {
            return true;
        }
        self.valid_until_ms.load(Ordering::Relaxed) - now_unix_ms() > expiry_margin_ms()
    }

    /// As [`Self::usable`], but also closed while
    /// [`LeaseKeeper::begin_handoff_pause`] has this partition's *new*
    /// local mutations paused (plan 29 M3c). Checked at every FUSE-side
    /// point that would otherwise let a new create/write/unlink land
    /// straight in the journal, bypassing the pause. `epoch_held` (the
    /// P2P/continuation-epoch path) is unaffected either way, matching
    /// the requirement to leave that path's behaviour alone.
    pub fn open_for_new_mutation(&self) -> bool {
        if self.epoch_held.load(Ordering::Relaxed) {
            return self.usable();
        }
        if self.handoff_pause_until_ms.load(Ordering::Relaxed) > now_unix_ms() {
            return false;
        }
        self.usable()
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
        // Deliberately does *not* touch `handoff_pause_until_ms`: a
        // pause must outlive a successful release (see that field's
        // doc) so the waiter it was for gets a fair, uncontested window
        // to claim the lease before this node's own blocked writes are
        // allowed to compete for it again.
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
    /// When `wanted` last went from empty to non-empty; gates
    /// [`LEASE_WANTED_GRACE_MS`]. `None` whenever nobody is waiting.
    wanted_since: Option<Instant>,
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
            wanted_since: None,
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
            Plan::Held => {
                // `classify` just saw our own live, unreleased lease on S3.
                // Re-arm the view: a release dropped mid-CAS leaves it
                // cleared while `held` is still set.
                if let Some((lease, _)) = &self.held {
                    self.view.set_held(lease);
                    self.view.touch();
                }
                return Ok(true);
            }
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
                self.wanted_since = None;
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
    ///
    /// `run_sync_round` no longer calls this directly (plan 30 M2b: it
    /// calls the split [`Self::prepare_renew`]/[`RenewAttempt::run`]/
    /// [`Self::apply_renew`] instead, so it can drop the keepers lock for
    /// the CAS — see the module doc's "Locking rules"). Kept as a
    /// production-shaped convenience: it is the concise way tests exercise
    /// the due-check itself (`renew_if_due_skips_until_half_ttl_then_renews`),
    /// and it documents the gate `prepare_renew` applies before handing
    /// back a job.
    #[allow(dead_code)]
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
    ///
    /// A thin, `&mut self`-holding wrapper around the same three-phase
    /// split `run_sync_round` uses to renew *without* holding the keepers
    /// map's lock across the CAS (plan 30 M2b, see the module doc's
    /// "Locking rules"): [`Self::prepare_renew_unconditional`] (sync),
    /// [`RenewAttempt::run`] (the actual I/O, needs no `&LeaseKeeper` at
    /// all), [`Self::apply_renew`] (sync bookkeeping). Calling all three
    /// back-to-back here, under one borrow of `self`, reproduces the
    /// original single-future `renew_now` exactly for the tests
    /// (`lease.rs`, `shipper.rs`) that still call it directly on an
    /// unshared keeper not behind any lock at all — `run_sync_round`
    /// itself now calls the three steps separately (see above) rather
    /// than through this wrapper, precisely so it can drop the lock
    /// between them.
    ///
    /// Cancellation-safe by construction, same as before this split:
    /// nothing about `self` is touched until `apply_renew` runs, which
    /// only happens after `RenewAttempt::run`'s await has fully resolved.
    /// `renew_now_is_cancellation_safe` exercises this directly: a caller
    /// racing this whole method's completion against something else in a
    /// `tokio::select!` and dropping it mid-CAS leaves the keeper exactly
    /// as it was, so the next attempt simply retries to completion.
    #[allow(dead_code)]
    pub async fn renew_now(&mut self) -> Result<()> {
        let Some(attempt) = self.prepare_renew_unconditional() else {
            return Ok(());
        };
        let outcome = attempt.run().await;
        self.apply_renew(outcome).await
    }

    /// Phase 1 (sync, no I/O) of the renewal split: due-check plus
    /// [`Self::prepare_renew_unconditional`]. Called under the keepers
    /// lock by `run_sync_round`; returns `None` (no lock needed further)
    /// when nothing is due this round. The `epoch_held` (continuation
    /// authority, no S3 object) case has no I/O to move off the lock, so
    /// it is applied instantly here rather than through a `RenewAttempt`.
    pub fn prepare_renew(&mut self) -> Option<RenewAttempt> {
        if self.view.epoch_held.load(Ordering::Relaxed) {
            self.extend_local();
            return None;
        }
        let (lease, _) = self.held.as_ref()?;
        if lease.expires_in_ms(now_unix_ms()) > (self.ttl_ms / 2) as i64 {
            return None;
        }
        self.prepare_renew_unconditional()
    }

    /// [`Self::prepare_renew`] without the half-TTL due-check — the
    /// unconditional renewal `renew_now`/[`Self::release`]'s sibling
    /// paths need. `None` only when this node holds no S3-backed lease at
    /// all (continuation-epoch or never-acquired).
    fn prepare_renew_unconditional(&mut self) -> Option<RenewAttempt> {
        if self.view.epoch_held.load(Ordering::Relaxed) {
            self.extend_local();
            return None;
        }
        let (lease, tag) = self.held.as_ref()?;
        Some(RenewAttempt {
            store: self.store.clone(),
            node_id: self.node_id,
            ttl_ms: self.ttl_ms,
            mine: lease.clone(),
            tag: tag.clone(),
        })
    }

    /// Phase 3 (sync bookkeeping plus one best-effort read) of the
    /// renewal split: apply a [`RenewAttempt::run`] result. Called under
    /// the keepers lock by `run_sync_round`, after the lock-free CAS in
    /// `RenewAttempt::run` has fully resolved. Mirrors exactly what the
    /// original single-future `renew_now` did with each outcome.
    pub async fn apply_renew(&mut self, outcome: RenewOutcome) -> Result<()> {
        match outcome {
            RenewOutcome::Renewed { lease, tag } => {
                self.view.set_held(&lease);
                self.wanted = lease.wanted_by.clone();
                if self.wanted.is_empty() {
                    self.wanted_since = None;
                } else if self.wanted_since.is_none() {
                    self.wanted_since = Some(Instant::now());
                }
                self.held = Some((lease, tag));
                self.refresh_condemned().await;
                Ok(())
            }
            RenewOutcome::Lost {
                holder,
                epoch,
                my_epoch,
            } => {
                self.mark_lost(holder, epoch, my_epoch);
                Ok(())
            }
            RenewOutcome::Err {
                error,
                held_update,
                view_cleared,
            } => {
                if view_cleared {
                    self.view.clear();
                }
                if let Some((lease, tag)) = held_update {
                    self.held = Some((lease, tag));
                }
                Err(error)
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
        journal_backlog == 0 && self.wants_handoff()
    }

    /// Same conditions as [`Self::idle_release_due`] minus the journal
    /// backlog term: dwell and wanted timers alone justify handing the
    /// lease back. Used to decide whether to *force* the backlog to zero
    /// via [`Self::begin_handoff_pause`] rather than whether to release
    /// this instant — a busy holder can have every timer here satisfied
    /// while `journal_backlog` never once reads zero on its own (see
    /// [`HANDOFF_PAUSE_MS`]'s doc).
    pub fn wants_handoff(&self) -> bool {
        !self.view.epoch_held.load(Ordering::Relaxed)
            && self.held.is_some()
            && !self.wanted.is_empty()
            && self.held_for_ms() >= LEASE_MIN_DWELL_MS as i64
            && (self.view.idle_for_ms() >= self.idle_release_ms as i64
                || self.wanted_for_ms() >= LEASE_WANTED_GRACE_MS as i64)
    }

    /// Close this node's own FUSE fast path for [`HANDOFF_PAUSE_MS`]: new
    /// local mutations fall back to the ordinary `Acquire` queue (the
    /// `SyncRequest::Acquire` handler checks [`Self::is_paused_for_handoff`]
    /// and declines rather than re-affirming `Plan::Held`, or the pause
    /// would have no effect on this node's own writes). Gives the caller
    /// a bounded window to drain `journal_backlog` to a true zero and
    /// release, instead of waiting for a lull a sustained workload may
    /// never give.
    pub fn begin_handoff_pause(&self) {
        self.view
            .handoff_pause_until_ms
            .store(now_unix_ms() + HANDOFF_PAUSE_MS, Ordering::Relaxed);
    }

    /// Whether [`Self::begin_handoff_pause`]'s window is still open.
    pub fn is_paused_for_handoff(&self) -> bool {
        self.view.handoff_pause_until_ms.load(Ordering::Relaxed) > now_unix_ms()
    }

    /// How long somebody has been waiting for this partition, in ms. `0`
    /// when nobody is, which keeps the grace arm of
    /// [`Self::idle_release_due`] false there.
    fn wanted_for_ms(&self) -> i64 {
        self.wanted_since
            .map(|at| at.elapsed().as_millis() as i64)
            .unwrap_or(0)
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

    /// Test hook: satisfy the dwell floor and age the requester's wait past
    /// [`LEASE_WANTED_GRACE_MS`], while leaving `idle_release_ms` alone so
    /// the write-idle arm of [`Self::idle_release_due`] stays false. Lets a
    /// test exercise the starvation bound on its own — the arm that a busy
    /// holder under contention actually takes.
    #[cfg(test)]
    pub(crate) fn expire_wanted_grace_for_test(&mut self) {
        self.held_since =
            Some(Instant::now() - std::time::Duration::from_millis(LEASE_MIN_DWELL_MS));
        self.wanted_since =
            Some(Instant::now() - std::time::Duration::from_millis(LEASE_WANTED_GRACE_MS));
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
    ///
    /// Cancellation-safe like [`Self::renew_now`]: `self.held` is only
    /// peeked (`clone`), and every field this mutates is only mutated
    /// after the CAS await resolves. The caller (the sync task's
    /// `select!`) can drop this future mid-CAS when a fresher request
    /// arrives; a `.take()` up front used to clear `held` regardless of
    /// whether the release actually landed, so a cancelled release could
    /// leave a node believing it had handed the lease back — and so never
    /// retrying — while the S3 object still showed it as holder, which is
    /// how a taken-over node's release could go unobserved by the very
    /// reintegration it was supposed to unblock.
    pub async fn release(&mut self) -> Result<Option<String>> {
        let Some((lease, tag)) = self.held.clone() else {
            return Ok(None);
        };
        // Fence the FUSE threads *before* the CAS: a mutation committed
        // while the release is in flight would be journaled under an epoch
        // the requester is about to supersede, after the caller's final
        // flush. Clearing the view is still cancellation-safe: `held` is
        // untouched, so a dropped release is followed by `renew_if_due`,
        // whose `set_held` restores the view.
        self.view.clear();
        match self.store.try_swap(&lease.released(), &tag).await {
            Ok(new_tag) => {
                // `Lease::released` drops `wanted_by`: the partition is
                // free, so every pending request has just been answered.
                self.wanted.clear();
                self.wanted_since = None;
                self.held_since = None;
                self.held = None;
                tracing::info!(epoch = lease.epoch, "released partition lease");
                Ok(new_tag.etag())
            }
            Err(StoreError::CasConflict) => {
                // Someone moved the object before our release landed (a
                // takeover, or a `wanted_by` edit). `diagnose_lost_renew`
                // re-reads and decides: still ours (a retried PUT landing
                // twice) keeps `held` as-is, anyone else's marks us lost.
                self.diagnose_lost_renew(&lease).await?;
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Everything a renewal CAS needs, captured by
/// [`LeaseKeeper::prepare_renew`]/[`LeaseKeeper::prepare_renew_unconditional`]
/// under the keepers lock so [`Self::run`] can perform the actual S3
/// round trip(s) without it. See `cli::lease`'s module doc, "Locking
/// rules".
///
/// Deliberately holds no reference to the originating [`LeaseKeeper`]:
/// `store` is an owned clone (cheap — see [`LeaseStore`]'s doc) and
/// `mine`/`tag` are the exact lease/tag this node believes it holds,
/// snapshotted at prepare time. Reads of the *original* keeper
/// (`ship_epoch`/`is_lost`, via its still-untouched `view`/`held`) made
/// while a `RenewAttempt` is in flight see that still-valid snapshot,
/// which is safe precisely because nothing here mutates it — the old
/// lease is genuinely still valid until [`LeaseKeeper::apply_renew`]
/// says otherwise.
pub struct RenewAttempt {
    store: LeaseStore,
    node_id: u64,
    ttl_ms: u64,
    mine: Lease,
    tag: LeaseTag,
}

/// What [`RenewAttempt::run`] learned, for [`LeaseKeeper::apply_renew`]
/// to fold back into the keeper. Mirrors, one for one, the outcomes the
/// original single-future `renew_now` used to apply inline.
pub enum RenewOutcome {
    /// Renewed (same holder, possibly a fresher `wanted_by`) at either
    /// the first attempt or the "lost the CAS to a `wanted_by` edit, not
    /// a takeover" retry — `lease.wanted_by` already reflects whichever
    /// path got here, so `apply_renew` does not need to know which.
    Renewed { lease: Lease, tag: LeaseTag },
    /// Deposed: re-reading after a lost CAS shows a different holder, or
    /// no lease object at all (0/0).
    Lost {
        holder: u64,
        epoch: u64,
        my_epoch: u64,
    },
    /// Transient failure. `held_update` mirrors the one case where the
    /// original code adopted a freshly re-read lease/tag despite the
    /// renewal itself failing (a second CAS lost to something other than
    /// a conflict); `view_cleared` mirrors the one case where the
    /// original code had already cleared the view (fencing FUSE writes)
    /// before hitting this error, and so must still apply that.
    Err {
        error: anyhow::Error,
        held_update: Option<(Lease, LeaseTag)>,
        view_cleared: bool,
    },
}

impl RenewAttempt {
    /// The lock-free phase: run the CAS (and, on a lost race against a
    /// `wanted_by` edit, the follow-up read-and-retry `renew_now` always
    /// did), producing a [`RenewOutcome`] for [`LeaseKeeper::apply_renew`]
    /// to apply. Touches nothing but its own owned fields and `self.store`
    /// — no `LeaseKeeper`, no keepers map, so no lock of any kind.
    pub async fn run(self) -> RenewOutcome {
        let Self {
            store,
            node_id,
            ttl_ms,
            mine,
            tag,
        } = self;
        let renewed = mine.renewed(ttl_ms);
        match store.try_swap(&renewed, &tag).await {
            Ok(tag) => RenewOutcome::Renewed {
                lease: renewed,
                tag,
            },
            Err(StoreError::CasConflict) => {
                // Not necessarily a deposition: a peer that wants this
                // partition edits `wanted_by` in place, which changes the
                // etag and so fails exactly this CAS. Re-read before
                // concluding anything — a read failure here is not
                // evidence of anything, so it propagates untouched
                // (`view_cleared: false`, matching "the view is intact
                // until we know").
                let current = match store.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        return RenewOutcome::Err {
                            error: e.into(),
                            held_update: None,
                            view_cleared: false,
                        }
                    }
                };
                if let Some((cur, fresh_tag)) = current {
                    if cur.holder == node_id
                        && cur.epoch == mine.epoch
                        && !cur.released
                        && !cur.is_expired(now_unix_ms())
                    {
                        tracing::debug!(
                            wanted_by = ?cur.wanted_by,
                            epoch = cur.epoch,
                            "renew CAS lost to a handoff request, not a takeover"
                        );
                        let renewed = cur.renewed(ttl_ms);
                        return match store.try_swap(&renewed, &fresh_tag).await {
                            Ok(tag) => RenewOutcome::Renewed {
                                lease: renewed,
                                tag,
                            },
                            // Lost again: somebody is moving faster than we
                            // can read. Fall back to the deposition probe,
                            // which is authoritative.
                            Err(StoreError::CasConflict) => {
                                Self::diagnose(&store, node_id, &mine).await
                            }
                            Err(e) => RenewOutcome::Err {
                                error: e.into(),
                                held_update: Some((cur, fresh_tag)),
                                view_cleared: false,
                            },
                        };
                    }
                }
                Self::diagnose(&store, node_id, &mine).await
            }
            // Transient store failure: nothing to apply — the lease we
            // still (believe we) have is simply retried next tick, still
            // unexpired from our own point of view.
            Err(e) => RenewOutcome::Err {
                error: e.into(),
                held_update: None,
                view_cleared: false,
            },
        }
    }

    /// The deposition probe: every path that reaches it has already lost
    /// a renewal CAS with no better explanation, so `apply_renew` always
    /// clears the view for it (`view_cleared: true`) regardless of what
    /// this re-read finds.
    async fn diagnose(store: &LeaseStore, node_id: u64, mine: &Lease) -> RenewOutcome {
        match store.get().await {
            Ok(Some((cur, tag))) if cur.holder == node_id && cur.epoch == mine.epoch => {
                // Our own object, only the tag was stale (a retried PUT
                // landing twice). Adopt the fresh tag and carry on.
                RenewOutcome::Renewed { lease: cur, tag }
            }
            Ok(Some((cur, _))) => RenewOutcome::Lost {
                holder: cur.holder,
                epoch: cur.epoch,
                my_epoch: mine.epoch,
            },
            // The lease object vanished (manual surgery / GC). Treat it as
            // deposition: we cannot prove we still have authority, and
            // something else clearly rewrote history.
            Ok(None) => RenewOutcome::Lost {
                holder: 0,
                epoch: 0,
                my_epoch: mine.epoch,
            },
            Err(e) => RenewOutcome::Err {
                error: e.into(),
                held_update: None,
                view_cleared: true,
            },
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

    /// A backend whose `put_opts` never resolves, so a caller racing it in
    /// a `tokio::select!` (like `node_runtime`'s sync task does against
    /// incoming `SyncRequest`s) and dropping the loser gets a real,
    /// mid-flight cancellation rather than a completed-then-discarded one.
    #[derive(Debug)]
    struct HangingStore(InMemory);

    impl std::fmt::Display for HangingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HangingStore({})", self.0)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for HangingStore {
        async fn put_opts(
            &self,
            _location: &object_store::path::Path,
            _payload: object_store::PutPayload,
            _opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            std::future::pending().await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.0.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.0.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.0.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.0.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.0.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.0.copy_opts(from, to, options).await
        }
    }

    /// Root-cause regression for plan 29 M3b's `deposed-reintegration`
    /// flake: `node_runtime`'s sync task races a round's completion
    /// against incoming `SyncRequest`s in a `tokio::select!` and drops
    /// the round when a request wins — exactly what happens when a
    /// frozen-then-resumed holder's overdue renewal collides with the
    /// depositor's segment arriving over the gossip push. `renew_now`
    /// used to `.take()` `self.held` before the CAS await, so a
    /// cancellation there permanently corrupted the keeper: nothing ever
    /// renewed it again (`held` was gone) and nothing ever marked it lost
    /// (the CAS-conflict path that calls `mark_lost` never got to run) —
    /// the node was neither a holder nor a confirmed-deposed one, forever.
    /// A cancelled attempt must leave the keeper exactly as it found it,
    /// so the next attempt can simply retry to completion.
    #[tokio::test]
    async fn renew_now_is_cancellation_safe() {
        let backend = InMemory::new();
        // Seed the lease object through the real backend first, then swap
        // the keeper onto a hanging decorator over the *same* backend so
        // the CAS it attempts still targets real, pre-existing state.
        let real: Arc<dyn object_store::ObjectStore> = Arc::new(backend);
        let store = LeaseStore::new(real.clone(), "p0", LeaseMode::Cas);
        let mut keeper = LeaseKeeper::new(store, 1);
        assert!(keeper.commit(Plan::Create, None).await.unwrap());
        assert!(keeper.held.is_some(), "must hold after a fresh Create");

        // Swap in the hanging store (a distinct `LeaseStore`, same
        // underlying object store) and race `renew_now` against a timeout
        // that always loses — simulating the `select!` picking the other
        // branch and dropping this future.
        let hanging = LeaseStore::new(
            Arc::new(HangingStore(InMemory::new())) as Arc<dyn object_store::ObjectStore>,
            "p0",
            LeaseMode::Cas,
        );
        // The hanging store has no lease object of its own, but `put_opts`
        // never returns regardless of what `get` would say, so `renew_now`
        // hangs at the CAS itself without ever needing a prior read.
        keeper.store = hanging;
        let held_before = keeper.held.clone();
        let outcome =
            tokio::time::timeout(std::time::Duration::from_millis(20), keeper.renew_now()).await;
        assert!(outcome.is_err(), "the hanging store must actually time out");
        assert_eq!(
            keeper.held, held_before,
            "a cancelled renewal must leave `held` exactly as it was"
        );
        assert!(
            !keeper.is_lost(),
            "a cancelled renewal must not be mistaken for a confirmed deposition"
        );

        // Swap back onto the real store: the next attempt must simply
        // pick up where the cancelled one left off and succeed normally.
        keeper.store = LeaseStore::new(real, "p0", LeaseMode::Cas);
        keeper.renew_now().await.unwrap();
        assert!(keeper.held.is_some());
        assert!(!keeper.is_lost());
    }

    /// Companion to the cancellation test above: when the CAS genuinely
    /// loses (a real takeover, not a dropped future), `renew_now` must
    /// still reach `mark_lost` and report it — this is the path the
    /// cancellation bug was stealing every time it fired.
    #[tokio::test]
    async fn renew_now_detects_a_genuine_takeover() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let a = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
        let mut ka = LeaseKeeper::new(a, 1);
        ka.ttl_ms = 10; // expires almost immediately, below
        assert!(ka.commit(Plan::Create, None).await.unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // B takes over from A's point of view: claim the same partition
        // with a different node id, exactly like an expired-lease takeover.
        let b = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let mut kb = LeaseKeeper::new(b, 2);
        match kb.classify().await.unwrap() {
            Plan::Claim { prev, tag, .. } => {
                assert!(kb
                    .commit(
                        Plan::Claim {
                            prev,
                            tag,
                            needs_tail: true
                        },
                        Some(TailedToHead::witness())
                    )
                    .await
                    .unwrap());
            }
            other => panic!("expected a claimable lease, got {other:?}"),
        }

        // A's own renewal, still holding its stale tag, must now discover
        // the takeover and mark itself lost rather than getting stuck.
        ka.renew_now().await.unwrap();
        assert!(ka.held.is_none());
        assert!(ka.is_lost(), "a real takeover must be reported as loss");
    }

    /// `renew_if_due` is the due-check `renew_now` itself skips (tests
    /// above call `renew_now` directly to bypass exactly this gate). Plan
    /// 30 M2b moved `run_sync_round`'s own call to the split
    /// `prepare_renew`/`apply_renew` pair, so this is the one direct
    /// exercise of the due-check left — `prepare_renew` shares the same
    /// gate (see its doc).
    #[tokio::test]
    async fn renew_if_due_skips_until_half_ttl_then_renews() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let a = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let mut keeper = LeaseKeeper::new(a, 1);
        keeper.ttl_ms = 1_000; // half-TTL = 500ms
        assert!(keeper.commit(Plan::Create, None).await.unwrap());
        let expires_before = keeper.held.as_ref().unwrap().0.expires_unix_ms;

        keeper.renew_if_due().await.unwrap();
        assert_eq!(
            keeper.held.as_ref().unwrap().0.expires_unix_ms,
            expires_before,
            "well within the first half of the TTL: renew_if_due must not touch the lease"
        );

        // Back-date our own view of the expiry past the half-TTL mark.
        // `try_swap`'s CAS keys off the etag, not this field, so the
        // in-memory store's real object is untouched and the renewal
        // below still succeeds — this isolates the due-check itself from
        // whether the lease has actually gone stale. The sleep guarantees
        // the millisecond clock `renewed()` stamps the new expiry from
        // has actually ticked forward from `expires_before`, so the
        // comparison below cannot pass merely because both reads landed
        // in the same millisecond.
        keeper.held.as_mut().unwrap().0.expires_unix_ms -= keeper.ttl_ms as i64 / 2 + 1;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        keeper.renew_if_due().await.unwrap();
        assert!(
            keeper.held.as_ref().unwrap().0.expires_unix_ms > expires_before,
            "past half-TTL: renew_if_due must renew and push expiry back out"
        );
        assert!(!keeper.is_lost());
    }

    /// Root-cause regression for plan 29 M3c's `create-storm-s3-only`
    /// EIO: `idle_release_due` requires `journal_backlog == 0`, which a
    /// sustained local workload can keep from ever being true. Once
    /// dwell/wanted alone justify a handoff, `wants_handoff` must say so
    /// regardless of backlog — it is the signal `run_sync_round` uses to
    /// start forcing the backlog down rather than waiting for a lull.
    #[tokio::test]
    async fn wants_handoff_ignores_backlog_but_idle_release_due_does_not() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let a = LeaseStore::new(store.clone(), "p0", LeaseMode::Cas);
        let mut ka = LeaseKeeper::new(a, 1);
        assert!(ka.commit(Plan::Create, None).await.unwrap());
        assert!(!ka.wants_handoff(), "nobody is waiting yet");

        let b = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let kb = LeaseKeeper::new(b, 2);
        match ka.classify().await.unwrap() {
            Plan::Held => {}
            other => panic!("expected our own held lease, got {other:?}"),
        }
        // B registers itself as a waiter directly on the stored object
        // (mirroring what `register_wanted` does over the wire).
        let (cur, tag) = kb.store.get().await.unwrap().unwrap();
        kb.store.try_swap(&cur.wanting(2), &tag).await.unwrap();
        ka.renew_now().await.unwrap(); // picks up the edited `wanted_by`
        assert_eq!(ka.wanted_by(), &[2]);

        ka.expire_wanted_grace_for_test();
        assert!(
            ka.wants_handoff(),
            "dwell + wanted grace elapsed must want a handoff regardless of backlog"
        );
        assert!(
            !ka.idle_release_due(1),
            "idle_release_due must still refuse while the backlog is non-zero"
        );
        assert!(
            ka.idle_release_due(0),
            "idle_release_due must agree once the backlog actually reads zero"
        );
    }

    /// [`LeaseKeeper::begin_handoff_pause`] must close
    /// [`LeaseView::open_for_new_mutation`] for a short, bounded window
    /// and then reopen on its own — nothing explicitly clears it on the
    /// "gave up this round" path, by design (see `HANDOFF_PAUSE_MS`'s
    /// doc): a round that gets cancelled mid-drain must not wedge the
    /// node forever. `usable()` (and so `ship_epoch`) must stay true
    /// throughout: the shipper still needs authority to drain the
    /// backlog the pause was started to force to zero.
    #[tokio::test]
    async fn handoff_pause_closes_new_mutations_but_not_shipping() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let mut k = LeaseKeeper::new(s, 1);
        assert!(k.commit(Plan::Create, None).await.unwrap());
        assert!(k.view().usable());
        assert!(k.view().open_for_new_mutation());
        assert!(!k.is_paused_for_handoff());

        k.begin_handoff_pause();
        assert!(k.is_paused_for_handoff());
        assert!(
            !k.view().open_for_new_mutation(),
            "a paused keeper must refuse a *new* local mutation"
        );
        assert!(
            k.view().usable(),
            "a paused keeper must still be able to ship/ack its existing backlog"
        );
        assert!(
            k.ship_epoch().is_some(),
            "shipping authority must survive the pause"
        );

        // Self-expiry: manufacture an already-elapsed pause rather than
        // sleeping past the real (2s) budget in a unit test.
        k.view()
            .handoff_pause_until_ms
            .store(now_unix_ms() - 1, Ordering::Relaxed);
        assert!(!k.is_paused_for_handoff());
        assert!(
            k.view().open_for_new_mutation(),
            "new mutations must reopen on their own"
        );
    }

    /// A successful release must *not* clear an in-progress pause: it
    /// exists precisely to stop this node's own next local write from
    /// winning the reclaim race against the waiter the release was for
    /// (see [`HANDOFF_PAUSE_MS`]'s doc). Only its own expiry lifts it.
    #[tokio::test]
    async fn release_does_not_clear_an_in_progress_handoff_pause() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let s = LeaseStore::new(store, "p0", LeaseMode::Cas);
        let mut k = LeaseKeeper::new(s, 1);
        assert!(k.commit(Plan::Create, None).await.unwrap());
        k.begin_handoff_pause();
        assert!(k.is_paused_for_handoff());

        k.release().await.unwrap();
        assert!(
            k.is_paused_for_handoff(),
            "a release must not cut short an in-progress handoff pause"
        );

        // It still expires on its own.
        k.view()
            .handoff_pause_until_ms
            .store(now_unix_ms() - 1, Ordering::Relaxed);
        assert!(!k.is_paused_for_handoff());
    }
}
