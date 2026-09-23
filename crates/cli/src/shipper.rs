//! Metadata log sync (DESIGN.md §4): one component both **tails** the
//! shared S3 log (applying other nodes' segments to the local replica)
//! and **ships** the local journal as CAS-created segments. The CAS
//! collision on a sequence number is the multi-writer conflict
//! detector: the loser applies the winner's segment and retries at the
//! next sequence (DESIGN.md "losers re-tail and retry").
//!
//! From phase 3 shipping is **lease-gated**: a segment may only be
//! written while this node holds an unexpired lease on the partition,
//! and it is stamped with that lease's epoch. Tailing records the
//! highest epoch applied; a segment arriving with a lower epoch is a
//! deposed holder's late write and is skipped as a fencing violation.
//!
//! A holder therefore does not tail the partitions it holds: while our
//! lease is live nobody else may append, so that LIST can only ever
//! return our own segments and costs a full round trip per sync round to
//! learn nothing. Measured against AWS S3 from Hungary, dropping it takes
//! a writer from 2.6 to 5.1 segments/s (24.5 → 33.1 same-region, 5.4 →
//! 6.2 against OVH Milan) — see plan 26's appendix. Three paths still
//! read a stream we believe we hold, and each is load-bearing:
//! [`Shipper::tail_to_head`] (the takeover witness must see *every*
//! stream), the `AlreadyExists` arm of [`Shipper::ship_part`] (our own
//! unacked segment from before a restart, or a deposed holder's late
//! write), and any round where the keeper no longer reports a ship epoch
//! — expired, released, or deposed — which drops it out of the held set
//! on its own.
//!
//! What a *follower* does instead is probe: a single speculative GET at
//! the sequence it expects next ([`TAIL_PROBE_IDLE`]), widening to
//! [`TAIL_GET_CONCURRENCY`] the moment that hits, and a LIST only when
//! the wide probe also saturates and it may therefore be arbitrarily far
//! behind.
//! The idle poll — overwhelmingly the common case, since most nodes are
//! caught up most of the time — is then a 404 rather than a listing:
//! measured, no slower (177 vs 175 ms HU→AWS, 34 vs 34 ms against OVH)
//! and a request class ~12.5× cheaper on AWS. The one measured
//! counter-example is same-region idle LIST at 14 ms against a 25 ms
//! GET-404; there it is the request price, not the latency, that carries
//! the choice. Catch-up keeps LIST because one 1000-key page still beats
//! k round trips when genuinely behind.
//!
//! The leaseless convergence path (foreign records that touch pending
//! local state are skipped via `TouchSet`, ours being later in the
//! global log) is retained as a safety net for journal rows that were not
//! captured as speculation. Plan 30 §M3b made the captured case exact
//! instead: a segment tailed while this node has unshipped captured
//! transactions is applied *before* them (`Meta::apply_segment`). With
//! leases neither is reached in normal operation — the harness asserts
//! the conflict counter stays at zero — but a backend without `If-Match`,
//! or a future relaxed mode, still needs deterministic convergence.
//!
//! Plan 30 §M3b also adds the **epoch marker**: right after the CAS of a
//! takeover from a holder that did not release (its lease expired — it
//! died, or is paused), before its takeover gate replays anything, the new
//! holder ships an empty segment at its epoch
//! ([`Shipper::ship_epoch_marker`]). It
//! fences any late segment of the old epoch (a deposed holder's PUT
//! racing the takeover), so the gate's replays by rid can never be
//! duplicated by a late copy of the same op; and it makes the stranding
//! visible to every other node at once — their tailers strand the old
//! epoch's shadows on it — instead of whenever the new holder next
//! happens to write.

use crate::lease::{LeaseKeeper, TailedToHead};
use anyhow::{bail, Context, Result};
use constellation_meta::{LogRecord, Meta, MetaStore};
use constellation_store_s3::log::PARTITION;
use constellation_store_s3::{Lease, LeaseMode, LeaseStore, LeaseTag, LogStore};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

/// Publish a metadata commit after this many shipped segments (across all
/// partitions), and unconditionally on shutdown. Plan 29 M0b retired the
/// byte-proportional cadence and the whole-DB `VACUUM INTO` checkpoint it
/// existed to size: a commit is a delta of changed keys, not a copy of the
/// namespace, so a plain segment-count cadence is enough.
const PUBLISH_EVERY: u64 = 32;

/// Default for `CONSTELLATION_PUBLISH_IDLE_S`: publish whenever `ns` has
/// dirty keys and this many seconds have passed since the last publish
/// attempt, even with no segment shipped since (plan 29 M2, moved up
/// from plan 29 M3). Without this, a node that is idle — or only ever
/// tailing foreign segments, which dirty `ns` just as surely as a local
/// write but never advance `shipped_since_publish` — could leave its
/// head commit, and the log-retention floor riding on it, stale
/// indefinitely.
const PUBLISH_IDLE_S_DEFAULT: u64 = 30;

fn publish_idle_interval() -> std::time::Duration {
    let secs: u64 = std::env::var("CONSTELLATION_PUBLISH_IDLE_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PUBLISH_IDLE_S_DEFAULT);
    std::time::Duration::from_secs(secs)
}

/// Max journal records per segment.
const SEGMENT_BATCH: usize = 10_000;

/// Ceiling on [`Shipper::register_wanted_by`]'s retry-after-conflict
/// cooldown, independent of TTL — see that function's doc.
const REGISTER_WANTED_COOLDOWN_CEILING_MS: u64 = 2_000;

/// Concurrent segment-payload GETs while tailing one partition, and the
/// width of the speculative GET-next probe. Each GET is a full S3 round
/// trip; a reader far from the bucket that fetched a burst's segments one
/// at a time could not keep up with a writer sitting next to it.
///
/// 16 rather than 8 on measurement: catching up over 64 segments takes
/// 1693 ms at k=16 against 2201 ms at k=8 from Europe to AWS (279 vs 333
/// same-region, 476 vs 596 against OVH Milan) — see plan 26's appendix.
/// This is the *catch-up* width; the idle probe is
/// [`TAIL_PROBE_IDLE`] wide.
const TAIL_GET_CONCURRENCY: usize = 16;

/// Width of the *idle* GET-next probe: one request.
///
/// [`LogStore::get_run`] returns the longest **contiguous** run from
/// `from`, so when `from` is absent — the overwhelmingly common case, a
/// caught-up node asking "anything new?" — the other k-1 GETs cannot
/// contribute to the answer no matter what they find. A 16-wide idle
/// probe therefore buys one bit of information for 16 requests.
///
/// Narrowing it is what makes a tight idle ceiling affordable. Per node
/// per partition per day, at AWS list price (GET $0.0004/1k, LIST
/// $0.005/1k — the 12.5× ratio the appendix measures):
///
/// | poll | requests/day | GET-equivalents |
/// |---|---:|---:|
/// | pre-plan 500 ms LIST | 172,800 | 2,160,000 |
/// | 30 s ceiling, k=16 | 46,080 | 46,080 |
/// | 10 s ceiling, k=16 | 138,240 | 138,240 |
/// | **10 s ceiling, k=1** | **8,640** | **8,640** |
///
/// So a 10 s ceiling with a 1-wide probe is 5.3× cheaper than a 30 s
/// ceiling with a 16-wide one *and* three times fresher — the width, not
/// the ceiling, was the expensive half. The width still widens to
/// [`TAIL_GET_CONCURRENCY`] the moment a probe hits, so catch-up keeps
/// the measured k=16 behaviour.
const TAIL_PROBE_IDLE: usize = 1;

/// Byte ceiling on one shipped segment. `SEGMENT_BATCH` bounds a segment
/// by record *count*, which says nothing about its size: a batch of
/// manifests with spilled chunk lists can be orders of magnitude larger
/// per record than a batch of renames. An oversized segment is a
/// pathological single PUT (retried whole on failure, buffered whole by
/// every tailer), so the batch is cut to the largest prefix that fits and
/// the remainder ships next round.
const SEGMENT_MAX_BYTES: usize = 4 << 20;

/// Slack for the segment envelope's own fields (`v`, `node`, `epoch` and
/// the record-vector length, all postcard varints) when measuring a batch
/// against [`SEGMENT_MAX_BYTES`]. A record's standalone postcard encoding
/// is byte-identical to its encoding inside the vector, so the sum of
/// record lengths plus this is the payload size.
const SEGMENT_ENVELOPE_SLACK: usize = 64;

/// Postcard envelope for a zstd-compressed S3 log segment. `v` is
/// reserved so a future format bump can reject old readers without a
/// dual decoder.
#[derive(Serialize, Deserialize)]
struct SegmentEnvelope {
    v: u32,
    node: u64,
    #[serde(default)]
    epoch: u64,
    records: Vec<LogRecord>,
}

struct Segment {
    node: u64,
    epoch: u64,
    records: Vec<LogRecord>,
}

fn encode(node: u64, epoch: u64, records: &[LogRecord]) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(&SegmentEnvelope {
        v: 2,
        node,
        epoch,
        records: records.to_vec(),
    })?)
}

/// How many leading records of `records` fit in one segment under
/// [`SEGMENT_MAX_BYTES`].
///
/// A record's postcard encoding is the same standalone as it is inside the
/// envelope's vector, so the sizes add up without a trial encode per
/// prefix. The first record always "fits": a single record larger than the
/// cap must still ship, or it would block its partition's stream forever —
/// the cap bounds batching, it is not a limit on what may be journaled.
fn records_within_cap(records: &[LogRecord]) -> Result<usize> {
    let mut total = SEGMENT_ENVELOPE_SLACK;
    for (i, rec) in records.iter().enumerate() {
        let len = rec.to_postcard()?.len();
        if i > 0 && total + len > SEGMENT_MAX_BYTES {
            return Ok(i);
        }
        total += len;
    }
    Ok(records.len())
}

fn decode(payload: &[u8]) -> Result<Segment> {
    let env: SegmentEnvelope = postcard::from_bytes(payload)
        .map_err(|e| anyhow::anyhow!("log segment postcard decode: {e}"))?;
    anyhow::ensure!(env.v == 2, "unsupported log segment version {}", env.v);
    Ok(Segment {
        node: env.node,
        epoch: env.epoch,
        records: env.records,
    })
}

pub fn segment_node(payload: &[u8]) -> Option<u64> {
    decode(payload).ok().map(|segment| segment.node)
}

pub struct Shipper {
    meta: Arc<Meta>,
    log: LogStore,
    node_id: u64,
    lease_mode: LeaseMode,
    /// Per-partition stream state (next_seq, max_epoch, log handle).
    parts: HashMap<String, PartState>,
    /// Segments shipped since the last publish; drives [`PUBLISH_EVERY`].
    shipped_since_publish: u64,
    /// Continuation epoch: journal locally, do not CAS-create segments.
    skip_ship: Arc<std::sync::atomic::AtomicBool>,
    /// Live spool observability shared with the control API.
    pub spool: Arc<std::sync::Mutex<SpoolInfo>>,
    /// When this node last wrote itself into a partition's `wanted_by`
    /// (plan 26 Step 7b). A blocked FUSE thread retries `Acquire` every
    /// few hundred ms and the holder only looks at the object once per
    /// half-TTL, so re-registering faster than that is pure request
    /// traffic — and each attempt is a CAS PUT, the expensive class.
    wanted_registered_at: HashMap<String, Instant>,
    /// When this node last read a partition it believes it holds
    /// (plan 26 Step 3, staleness bound). See [`HELD_TAIL_MAX_STALENESS`].
    held_tail_at: HashMap<String, Instant>,
    /// P2P handle for push invalidation. Disabled by default so the
    /// existing tests and the no-P2P path need no changes.
    peers: constellation_net::Peers,
    /// Offline designation (phase 4a, DESIGN.md §5.2): when a foreign
    /// node ships records touching a designated path, the designee's
    /// ack is awaited (bounded) before the batch is treated as fully
    /// published. `None` when no designations exist for this mount.
    designations: Option<std::sync::Arc<crate::designation::DesignationManager>>,
    /// Plan 28 §11: the `mtree` publisher, when one is wired up.
    ///
    /// `None` for most tests and for a read-only member, which publishes
    /// nothing (it ships no segments, so it has no authority to commit
    /// one) and instead bootstraps from commits published by writers; the
    /// daemon turns this on in `node_runtime` once the node cache exists.
    ///
    /// Behind an async mutex because a publish runs as its own spawned
    /// task: the daemon's sync loop drops a running round whenever an
    /// explicit request arrives (every write-through close sends one),
    /// and a publish that ran inline was dropped with it — under steady
    /// write traffic, every time, so commits starved
    /// (`multi-partition-retention-is-per-partition` saw one commit in
    /// ~1000 segments). A spawned publish finishes regardless, holding
    /// the lock while it runs.
    publisher: Option<Arc<tokio::sync::Mutex<crate::mtree_publish::TreePublisher>>>,
    /// Lease epoch of the last segment we shipped, recorded so a commit
    /// can carry its author's epoch (§P3: a deposed holder's late
    /// commit must be recognizable exactly as a late log segment is).
    last_ship_epoch: u64,
    /// When a publish last ran (landed, deferred, or found nothing to
    /// do) or this `Shipper` was constructed, for
    /// [`Shipper::publish_idle_due`]. Seeded at construction rather than
    /// left `None` so a freshly mounted, otherwise-idle node does not
    /// publish immediately — the idle window is measured from "since
    /// this node last did anything", and mounting counts.
    last_publish_attempt: Instant,
    /// Plan 30 §M13: the S3 inbox, when the daemon wired one up: polled
    /// from the sync round (`inbox::holder_round`) and drained inside the
    /// takeover gate (`complete_gate`). `None` for tests and tools.
    inbox: Option<Arc<crate::inbox::InboxRuntime>>,
}

struct PartState {
    log: LogStore,
    next_seq: u64,
    max_epoch: u64,
}

/// Snapshot of sync progress (updated on every sync attempt).
#[derive(Default, Clone)]
pub struct SpoolInfo {
    /// Highest log sequence shipped or applied.
    pub head_seq: u64,
    /// Foreign records skipped because pending local ops won.
    pub conflicts: u64,
    /// Segments skipped because their lease epoch was already superseded.
    pub fenced: u64,
    pub last_error: Option<String>,
    /// `run_managed_sync_round` invocations that ran to completion (plan
    /// 30 M2b measurement). Before that milestone, under forwarding load
    /// almost every round was cancelled instead — see
    /// `ship_rounds_cancelled`'s doc — so a healthy holder should show
    /// this climbing steadily relative to it.
    pub ship_rounds_completed: u64,
    /// Rounds dropped mid-flight because a non-`Nudge`,
    /// non-`Mutate`/`Forward` `SyncRequest` arrived first (plan 30 M2b:
    /// `Mutate`/`Forward` no longer cancel a round at all — see
    /// `node_runtime`'s sync task doc). Kept because it is still useful:
    /// a node fielding a steady stream of `Acquire`/`HandOff` traffic
    /// (frequent handoffs, or peers hammering a busy partition) will
    /// still show real cancellations here even after this milestone.
    pub ship_rounds_cancelled: u64,
    /// Plan 30 §M3a: speculative entries (requester shadows and `Exists`
    /// hints) rolled back because a later epoch stranded them.
    pub speculation_rolled_back: u64,
    /// Stranded ops replayed by rid and accepted (by the current holder,
    /// or executed locally by the takeover gate).
    pub stranded_replayed: u64,
    /// Stranded ops whose replay was refused and materialized as a
    /// `.constellation-conflict/` copy (or, for an op with nothing to
    /// copy, only counted and logged).
    pub replay_conflicts: u64,
    /// Plan 30 §M3b: this node's own unshipped transactions rolled back
    /// because it was deposed (queued for replay by rid).
    pub local_rolled_back: u64,
    /// Plan 30 §M3b: deposition recoveries run.
    pub depositions: u64,
    /// Plan 30 §M3b: epoch-marker segments shipped after a takeover.
    pub epoch_markers: u64,
    /// Plan 30 §M4: refused replays whose conflict copy could not be made
    /// yet (at least one failed attempt), and those failing for at least
    /// `recovery::LEASE_FALLBACK` — gauges the replay drain sets.
    pub replay_copies_pending: u64,
    pub replay_copies_stalled: u64,
}

impl Shipper {
    /// Attach to an existing local replica. `applied_seq` is the log
    /// position the replica covers (from the local kv store); segments
    /// beyond it are tailed on the first `sync`.
    #[allow(dead_code)]
    pub fn attach(meta: Arc<Meta>, log: LogStore, node_id: u64) -> Result<Self> {
        Self::attach_with_mode(meta, log, node_id, LeaseMode::Cas)
    }

    pub fn attach_with_mode(
        meta: Arc<Meta>,
        log: LogStore,
        node_id: u64,
        lease_mode: LeaseMode,
    ) -> Result<Self> {
        let applied = meta.applied_seq()?;
        let head = applied;
        let mut parts = HashMap::new();
        parts.insert(
            PARTITION.to_string(),
            PartState {
                log: log.with_partition(PARTITION),
                next_seq: applied + 1,
                max_epoch: 0,
            },
        );
        Ok(Self {
            meta,
            log,
            node_id,
            lease_mode,
            parts,
            shipped_since_publish: 0,
            skip_ship: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            spool: Arc::new(std::sync::Mutex::new(SpoolInfo {
                head_seq: head,
                ..Default::default()
            })),
            wanted_registered_at: HashMap::new(),
            held_tail_at: HashMap::new(),
            peers: constellation_net::Peers::disabled(),
            designations: None,
            publisher: None,
            last_ship_epoch: 0,
            last_publish_attempt: Instant::now(),
            inbox: None,
        })
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn meta(&self) -> &Arc<Meta> {
        &self.meta
    }

    /// Attach the P2P handle so shipped segments are announced to peers.
    pub fn set_peers(&mut self, peers: constellation_net::Peers) {
        self.peers = peers;
    }

    /// Turn on plan 28's `mtree` publish (§11).
    ///
    /// Separate from `attach_with_mode` for the reason `set_peers` is:
    /// the publisher needs a node cache, a blob store and a commit
    /// chain, none of which exist at attach time, and every caller that
    /// does not have them must keep working unchanged.
    ///
    /// [`crate::mtree_publish::TreePublisher::restore`] runs here so a
    /// restart picks up the published root when — and only when — it
    /// can prove the tree is level with the replica.
    pub fn enable_tree_publish(
        &mut self,
        mut publisher: crate::mtree_publish::TreePublisher,
    ) -> Result<()> {
        publisher.restore()?;
        self.publisher = Some(Arc::new(tokio::sync::Mutex::new(publisher)));
        Ok(())
    }

    /// The publisher handle, for warming it up off the sync loop.
    pub fn publisher_handle(
        &self,
    ) -> Option<Arc<tokio::sync::Mutex<crate::mtree_publish::TreePublisher>>> {
        self.publisher.clone()
    }

    /// Lock the publisher (waiting for an in-flight publish when `wait`).
    /// `None` when there is no publisher, or it is busy and the caller
    /// would rather not wait.
    ///
    /// Plan 29 M2: there is no batch to hand over here any more. Every
    /// write to `ns` — ours via `ship_part`'s journal drain, a foreign
    /// one via `apply_decoded_segment`'s replay — dirties its own keys
    /// in the same transaction, durably, in `constellation_meta::Meta`
    /// itself. A publish reads that dirty set directly
    /// (`TreePublisher::publish`), so there is nothing left for the
    /// transport layer to accumulate or hand off.
    async fn take_publisher(
        &mut self,
        wait: bool,
    ) -> Option<tokio::sync::OwnedMutexGuard<crate::mtree_publish::TreePublisher>> {
        let publisher = self.publisher.clone()?;
        if wait {
            Some(publisher.lock_owned().await)
        } else {
            publisher.try_lock_owned().ok()
        }
    }

    /// Publish the current dirty set as a commit. Best effort: a publish
    /// that cannot land must not stop the log from shipping. `dirty`
    /// survives a failure, so the next round carries the same keys.
    async fn publish_tree(&mut self, wait: bool) {
        self.last_publish_attempt = Instant::now();
        let epoch = self.last_ship_epoch;
        // Busy means the previous publish is still running; it will be
        // followed by the next cadence's (unless the caller must publish
        // now — shutdown — and waits for it instead).
        let Some(mut publisher) = self.take_publisher(wait).await else {
            return;
        };
        let task = tokio::spawn(async move { publisher.publish(epoch).await.map(|_| ()) });
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "metadata tree publish failed; retrying next round")
            }
            Err(e) => tracing::warn!(error = %e, "metadata tree publish task failed"),
        }
    }

    /// Publish now and return the commit that reflects this replica —
    /// what a snapshot retains. Callers ship the journal first (the
    /// publisher only sees shipped and tailed records).
    pub async fn publish_now(&mut self) -> Result<(u64, constellation_mtree::NodeHash)> {
        self.last_publish_attempt = Instant::now();
        let epoch = self.last_ship_epoch;
        let mut publisher = self
            .take_publisher(true)
            .await
            .context("this mount has no metadata tree publisher")?;
        tokio::spawn(async move { publisher.publish_now(epoch).await })
            .await
            .context("metadata publish task")?
    }

    /// A publisher is wired up, its dirty set is non-empty, and no
    /// publish has run for `CONSTELLATION_PUBLISH_IDLE_S` (default 30s).
    /// The every-`PUBLISH_EVERY`-segments cadence only fires for a node
    /// that is actively shipping its own writes; a node that is mostly
    /// idle, or mostly tailing foreign segments (which dirty `ns` just
    /// as surely, but never increment `shipped_since_publish`), would
    /// otherwise leave its head commit — and the log-retention floor
    /// that rides on it — stale indefinitely.
    fn publish_idle_due(&self) -> bool {
        self.publisher.is_some()
            && self.meta.has_dirty()
            && self.last_publish_attempt.elapsed() >= publish_idle_interval()
    }

    /// Plan 30 §M4 item 3: whether this node publishes commits at all —
    /// only while it holds the lease (`Meta::holder_epoch`, which the lease
    /// keeper sets the moment its CAS wins and clears on release or
    /// deposition). Every other node tails the same log the holder
    /// publishes from.
    fn is_publisher(&self) -> bool {
        self.meta.holder_epoch() != 0
    }

    /// The idle publish: the holder publishes; a follower instead clears
    /// the dirty keys the head commit already covers
    /// (`TreePublisher::follow_head`), so its dirty set does not grow
    /// without bound and it stays ready to publish the moment it becomes
    /// the holder. Best effort, like every publish.
    async fn publish_or_follow(&mut self) -> Result<()> {
        if self.is_publisher() {
            return self.publish().await;
        }
        self.last_publish_attempt = Instant::now();
        let Some(mut publisher) = self.take_publisher(false).await else {
            return Ok(());
        };
        let task = tokio::spawn(async move { publisher.follow_head().await });
        match task.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::debug!(error = %e, "follower head check failed; retrying later"),
            Err(e) => tracing::debug!(error = %e, "follower head check task failed"),
        }
        Ok(())
    }

    /// The published root, for tests.
    #[cfg(test)]
    pub fn tree_published(&self) -> Option<(constellation_mtree::NodeHash, u64)> {
        self.publisher.as_ref()?.try_lock().ok()?.published()
    }

    pub fn log(&self) -> &LogStore {
        &self.log
    }

    #[allow(dead_code)] // convenience constructor for a keeper matching this shipper; not yet wired to a call site
    pub fn lease_keeper(&self, part: &str) -> LeaseKeeper {
        LeaseKeeper::new(
            LeaseStore::new(self.log.inner(), part, self.lease_mode),
            self.node_id,
        )
        .with_holder_epoch(self.meta.holder_epoch_cell())
    }

    pub fn set_skip_ship(&self, skip: bool) {
        self.skip_ship
            .store(skip, std::sync::atomic::Ordering::Relaxed);
    }

    /// Plan 30 §M13: attach the S3 inbox runtime.
    pub fn set_inbox(&mut self, inbox: Arc<crate::inbox::InboxRuntime>) {
        self.inbox = Some(inbox);
    }

    pub fn inbox(&self) -> Option<&Arc<crate::inbox::InboxRuntime>> {
        self.inbox.as_ref()
    }

    /// Attach the offline-designation manager (DESIGN.md §5.2) so a
    /// foreign flush touching a designated path waits for the
    /// designee's ack before being treated as fully published.
    pub fn set_designations(
        &mut self,
        designations: std::sync::Arc<crate::designation::DesignationManager>,
    ) {
        self.designations = Some(designations);
    }

    fn ensure_part(&mut self, id: &str) {
        if self.parts.contains_key(id) {
            return;
        }
        let applied = self.meta.applied_seq().unwrap_or(0);
        self.parts.insert(
            id.to_string(),
            PartState {
                log: self.log.with_partition(id),
                next_seq: applied + 1,
                max_epoch: 0,
            },
        );
    }

    /// Compatibility helper used by existing single-partition tests.
    #[allow(dead_code)]
    pub fn next_seq(&self) -> u64 {
        self.parts.get(PARTITION).map(|p| p.next_seq).unwrap_or(1)
    }

    /// The highest lease epoch applied or shipped on `part`'s stream
    /// (0 if unknown). Plan 30 §M3b: a keeper holding a lower epoch than
    /// this was deposed (`main::run_sync_round` renews it at once).
    pub fn max_epoch(&self, part: &str) -> u64 {
        self.parts.get(part).map(|p| p.max_epoch).unwrap_or(0)
    }

    /// Last shipped sequence for `part` (`next_seq - 1`), if known.
    pub fn last_shipped_seq(&self, part: &str) -> Option<u64> {
        self.parts.get(part).map(|p| p.next_seq.saturating_sub(1))
    }

    /// One full ordinary sync round. A persisted deposition keeps the node
    /// tail-only: no lease may be acquired and no local journal may ship
    /// until the deposition recovery (plan 30 §M3b,
    /// `recovery::recover_deposed`) has rolled the stranded journal back
    /// and cleared it.
    pub async fn sync_all(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        loop {
            let held = self.held_partitions(leases);
            self.tail_all_except(&held).await?;
            if matches!(self.meta.kv_get("lease_lost")?.as_deref(), Some("1")) {
                tracing::debug!(
                    "deposed node remains tail-only until its deposition recovery runs; \
                     refusing ordinary lease acquisition and journal shipping"
                );
                return Ok(());
            }
            if !self.ship_all(leases).await? {
                // Nothing left to ship this round. `ship_part`'s own
                // cadence only fires on the back of a shipped segment,
                // so a node that is idle, or only ever tailing foreign
                // segments, would otherwise never publish on its own —
                // check the idle timer here instead.
                if self.publish_idle_due() {
                    self.publish_or_follow().await?;
                }
                // Same reasoning for read-time atime (plan 29 M3b): a
                // held partition with nothing else to ship this round may
                // still be sitting on a backlog of read-time bumps from a
                // holder that only ever serves reads (ride-along and
                // ship-then-release both need other lease activity to
                // fire). `held` is exactly this round's cache-coherent
                // "we hold it" set, already computed above.
                for part in &held {
                    if let Some(lease) = leases.get(part) {
                        self.ship_atime_if_stale(part, lease).await;
                    }
                }
                return Ok(());
            }
        }
    }

    /// The periodic sync task's ordinary round (`main::run_sync_round`,
    /// plan 30 M2b). Same tail/deposition-check/ship shape as
    /// [`Self::sync_all`], but takes the keepers mutex itself instead of
    /// an already-held guard, so it can drop it around the one thing
    /// that must not run locked: the segment PUT (see `lease.rs`'s
    /// module doc, "Locking rules" — renewal is `run_sync_round`'s own
    /// job, done before this is called, for the same reason).
    ///
    /// The lock is re-taken only for: the per-loop snapshot (which
    /// partitions are held, and at what epoch — plain reads), the idle
    /// branch's read-time-atime drain (needs a live `&LeaseKeeper`), and
    /// the rare fallback where a partition has pending journal but no
    /// local lease record at all yet (this process's first-ever write,
    /// before any acquire) or has lost its epoch since the snapshot
    /// (expired/deposed mid-round). That fallback still needs `&mut
    /// HashMap` to acquire or reacquire, so it runs under the full lock
    /// exactly like [`Self::ship_all`] always has — a once-ever (or
    /// once-per-deposition) event, not the steady-state hot path this
    /// milestone targets.
    pub async fn run_ordinary_round(
        &mut self,
        keepers: &Arc<tokio::sync::Mutex<HashMap<String, LeaseKeeper>>>,
    ) -> Result<()> {
        loop {
            let (held, epochs): (HashSet<String>, HashMap<String, u64>) = {
                let g = keepers.lock().await;
                let held = self.held_partitions(&g);
                let epochs = g
                    .iter()
                    .filter_map(|(p, k)| k.ship_epoch().map(|e| (p.clone(), e)))
                    .collect();
                (held, epochs)
            };
            self.tail_all_except(&held).await?;
            if matches!(self.meta.kv_get("lease_lost")?.as_deref(), Some("1")) {
                tracing::debug!(
                    "deposed node remains tail-only until its deposition recovery runs; \
                     refusing ordinary lease acquisition and journal shipping"
                );
                return Ok(());
            }
            let grouped = self.meta.take_journal_grouped(SEGMENT_BATCH)?;
            if grouped.is_empty() {
                if self.publish_idle_due() {
                    self.publish_or_follow().await?;
                }
                if !held.is_empty() {
                    let g = keepers.lock().await;
                    for part in &held {
                        if let Some(lease) = g.get(part) {
                            self.ship_atime_if_stale(part, lease).await;
                        }
                    }
                }
                return Ok(());
            }
            let mut more = false;
            // Partitions the snapshot above did not find a current epoch
            // for: shipped below under the full lock, same as
            // `ship_all`. `take_journal_grouped` only *peeks* the journal
            // (a read transaction; `ack_journal_rows_at` is the only
            // thing that ever removes rows), so a `batch` this pass ends
            // up not shipping is simply re-read by the next round — never
            // lost, whether that is because acquisition failed here or
            // because this method returns before reaching it.
            let mut fallback: Vec<(String, constellation_meta::JournalBatch)> = Vec::new();
            for (part, batch) in grouped {
                self.ensure_part(&part);
                match epochs.get(&part) {
                    Some(&epoch) => {
                        if self.ship_part(&part, Some(epoch), batch).await? {
                            more = true;
                        }
                    }
                    None => fallback.push((part, batch)),
                }
            }
            if !fallback.is_empty() {
                let mut g = keepers.lock().await;
                for (part, batch) in fallback {
                    if !g.contains_key(&part) {
                        let mut keeper = LeaseKeeper::new(
                            LeaseStore::new(self.log.inner(), &part, self.lease_mode),
                            self.node_id,
                        )
                        .with_holder_epoch(self.meta.holder_epoch_cell());
                        keeper.note_acquire_reason("ship-pending-journal");
                        match acquire_lease_for(self, &mut keeper, &part).await {
                            Ok(true) => {
                                g.insert(part.clone(), keeper);
                            }
                            // A live foreign holder: leave `batch`
                            // unshipped this round. The journal itself is
                            // untouched (see the comment above this loop),
                            // so the next round's `take_journal_grouped`
                            // simply reads the same rows again — matching
                            // `ship_all`'s existing (pre-M2b) behavior for
                            // this same corner case.
                            Ok(false) => continue,
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    part,
                                    "lease acquisition for pending journal failed"
                                );
                                continue;
                            }
                        }
                    }
                    let needs_reacquire = g
                        .get(&part)
                        .is_some_and(|keeper| !keeper.is_lost() && keeper.ship_epoch().is_none());
                    if needs_reacquire {
                        let keeper = g.get_mut(&part).expect("checked above");
                        keeper.note_acquire_reason("ship-reacquire");
                        match acquire_lease_for(self, keeper, &part).await {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    part,
                                    "lease reacquisition for pending journal failed"
                                );
                                continue;
                            }
                        }
                    }
                    let Some(lease) = g.get(&part) else {
                        continue;
                    };
                    if self.ship_part(&part, lease.ship_epoch(), batch).await? {
                        more = true;
                    }
                }
            }
            if !more {
                return Ok(());
            }
        }
    }

    /// Single-partition convenience used by existing tests and by the
    /// FUSE write-gate's default p0 path.
    pub async fn sync(&mut self, lease: &LeaseKeeper) -> Result<()> {
        self.sync_one(PARTITION, lease).await
    }

    pub async fn sync_one(&mut self, part: &str, lease: &LeaseKeeper) -> Result<()> {
        // Holding the lease means no other node may append here, so the
        // tail is skipped (see the module doc). It has to be replaced by
        // an explicit `ensure_part`: the tail is what used to register a
        // partition acquired straight from `Plan::Create`, and `ship_part`
        // indexes `self.parts` unconditionally.
        self.ensure_part(part);
        loop {
            if lease.ship_epoch().is_none() {
                self.tail_part(part).await?;
            }
            if !self.ship_part_taking(part, lease).await? {
                return Ok(());
            }
        }
    }

    /// Tail-only round over every known partition.
    pub async fn tail_to_head(&mut self) -> Result<TailedToHead> {
        self.tail_all().await?;
        Ok(TailedToHead::witness())
    }

    pub async fn tail_part_to_head(&mut self, part: &str) -> Result<TailedToHead> {
        self.tail_part(part).await?;
        Ok(TailedToHead::witness())
    }

    /// Tail every known partition, unconditionally. This is what the
    /// takeover witness needs: a claim on a stream is only legal once
    /// *every* stream is applied, so it must not skip anything.
    async fn tail_all(&mut self) -> Result<()> {
        self.tail_all_except(&HashSet::new()).await
    }

    /// Tail every known partition except the ones in `held`.
    ///
    /// A held partition is still registered in `self.parts`; only its
    /// LIST is skipped.
    async fn tail_all_except(&mut self, held: &HashSet<String>) -> Result<()> {
        let mut seen: HashSet<String> = HashSet::new();
        loop {
            let ids: Vec<String> = self.parts.keys().cloned().collect();
            let todo: Vec<String> = ids.into_iter().filter(|id| !seen.contains(id)).collect();
            if todo.is_empty() {
                return Ok(());
            }
            for id in &todo {
                self.ensure_part(id);
            }
            let probed: Vec<String> = todo
                .iter()
                .filter(|id| !held.contains(*id))
                .cloned()
                .collect();
            // One *parallel* probe sweep across the partitions. Each
            // partition is its own S3 prefix, so a tree that has split
            // into N partitions costs N round trips here; sequentially
            // that dominates the sync round on a WAN mount (~200 ms ×
            // N per round, before a single segment is even applied).
            let probes = futures::future::join_all(probed.iter().map(|id| {
                let log = self.parts[id].log.with_partition(id);
                let next = self.parts[id].next_seq;
                async move { log.get_run(next, TAIL_PROBE_IDLE).await }
            }))
            .await;
            for (id, probe) in probed.into_iter().zip(probes) {
                self.tail_part_probed(&id, probe?, TAIL_PROBE_IDLE).await?;
            }
            seen.extend(todo);
        }
    }

    async fn tail_part(&mut self, part: &str) -> Result<()> {
        self.ensure_part(part);
        let probe = self.probe_run(part, TAIL_PROBE_IDLE).await?;
        self.tail_part_probed(part, probe, TAIL_PROBE_IDLE).await
    }

    /// The speculative GET-next probe: `width` segment GETs from this
    /// partition's next expected sequence, in flight together. The reply
    /// is the longest contiguous run that exists.
    async fn probe_run(&self, part: &str, width: usize) -> Result<Vec<(u64, Vec<u8>)>> {
        let st = &self.parts[part];
        Ok(st.log.get_run(st.next_seq, width).await?)
    }

    /// Tail `part` from an already-fetched probe run.
    ///
    /// The probe *is* the steady-state poll: a follower with nothing to
    /// apply pays k GET-404s, no LIST at all. A saturated run (every probe
    /// hit) is the signal that this node may be arbitrarily far behind,
    /// and then one LIST page — up to 1000 keys for roughly the price of
    /// two GETs — is the cheapest way to learn how far, so catch-up drops
    /// back to LIST + a pipelined fetch and loops for another probe.
    /// Measured HU→AWS over 64 segments: 1693 ms this way against 3132 ms
    /// for the LIST-every-round tailer it replaces.
    ///
    /// Termination: the loop only re-lists after applying a full run of k
    /// segments, so `next_seq` advances by at least k per iteration.
    async fn tail_part_probed(
        &mut self,
        part: &str,
        mut run: Vec<(u64, Vec<u8>)>,
        mut width: usize,
    ) -> Result<()> {
        loop {
            let saturated = run.len() >= width;
            for (seq, payload) in std::mem::take(&mut run) {
                self.apply_segment_payload(part, seq, &payload)?;
            }
            if !saturated {
                return Ok(());
            }
            if width < TAIL_GET_CONCURRENCY {
                // The narrow idle probe hit, so this node is not idle
                // after all. Widen before reaching for a LIST: one more
                // pipelined GET sweep usually drains a normal arrival,
                // and a LIST here would price every single new segment at
                // a listing.
                width = TAIL_GET_CONCURRENCY;
                run = self.probe_run(part, width).await?;
                continue;
            }
            let next = self.parts[part].next_seq;
            let seqs = self.parts[part].log.list_segments_from(next).await?;
            self.apply_listed(part, seqs).await?;
            run = self.probe_run(part, width).await?;
        }
    }

    /// Fetch and apply the contiguous head of an already-fetched listing.
    /// GETs are pipelined ([`TAIL_GET_CONCURRENCY`] in flight); `buffered`
    /// yields them in order, so records still apply in strict sequence.
    async fn apply_listed(&mut self, part: &str, seqs: Vec<u64>) -> Result<()> {
        use futures::StreamExt;
        let next = self.parts[part].next_seq;
        let run: Vec<u64> = seqs
            .iter()
            .copied()
            .enumerate()
            .take_while(|(i, seq)| *seq == next + *i as u64)
            .map(|(_, seq)| seq)
            .collect();
        if run.is_empty() {
            return Ok(());
        }
        let base = self.parts[part].log.with_partition(part);
        let mut fetched = futures::stream::iter(run.into_iter().map(|seq| {
            let log = base.with_partition(base.partition());
            async move { (seq, log.get_segment(seq).await) }
        }))
        .buffered(TAIL_GET_CONCURRENCY);
        while let Some((seq, payload)) = fetched.next().await {
            let payload = payload?;
            self.apply_segment_payload(part, seq, &payload)?;
        }
        Ok(())
    }

    /// Apply a gossip-pushed segment without an S3 GET. Returns false for
    /// a gap so the caller can nudge the ordinary tailer.
    pub fn try_apply_pushed(
        &mut self,
        part: &str,
        seq: u64,
        advertised_epoch: u64,
        payload: &[u8],
    ) -> Result<bool> {
        self.ensure_part(part);
        if seq != self.parts[part].next_seq {
            return Ok(false);
        }
        // E2E pushes carry the sealed segment (see `ship_segment`). Open it
        // with the partition DEK. A missing key or any open failure falls
        // back to the ordinary S3 tailer, which refreshes the key first.
        let opened;
        let payload = if self.parts[part].log.is_e2e() {
            match self.parts[part].log.open_segment(seq, payload) {
                Ok(p) => {
                    opened = p;
                    &opened[..]
                }
                Err(_) => return Ok(false),
            }
        } else {
            payload
        };
        let seg = decode(payload)?;
        if seg.epoch != advertised_epoch {
            bail!(
                "pushed segment epoch mismatch: advertised {advertised_epoch}, payload {}",
                seg.epoch
            );
        }
        self.apply_decoded_segment(part, seq, seg)?;
        Ok(true)
    }

    fn apply_segment_payload(&mut self, part: &str, seq: u64, payload: &[u8]) -> Result<()> {
        let seg = decode(payload)?;
        self.apply_decoded_segment(part, seq, seg)
    }

    fn apply_decoded_segment(&mut self, part: &str, seq: u64, seg: Segment) -> Result<()> {
        if seg.node == self.node_id {
            // Our own segment, shipped but not acked before a restart (or a
            // takeover's epoch marker). Ride-along atime rows never come
            // from the journal, so only the rest must match its head; an
            // atime-only or empty (marker) segment matches an empty head.
            let journaled: Vec<&LogRecord> = seg
                .records
                .iter()
                .filter(|r| !matches!(r, LogRecord::Atime { .. }))
                .collect();
            // Plan 30 §M4: the head, or — when held-back transactions
            // were skipped — a subsequence of whole transactions.
            let Some(seqs) = self.meta.match_own_segment(&journaled)? else {
                bail!(
                    "segment {seq} of {part} claims our node id {} but does not match \
                     the journal: state dir reuse or id collision",
                    self.node_id
                );
            };
            self.meta.ack_journal_rows_at(&seqs, seq)?;
            tracing::info!(
                seq,
                part,
                records = seg.records.len(),
                "recovered unacked segment"
            );
        } else if seg.epoch > 0 && seg.epoch < self.parts[part].max_epoch {
            self.spool.lock().unwrap().fenced += 1;
            tracing::error!(
                seq,
                part,
                node = seg.node,
                epoch = seg.epoch,
                max_epoch = self.parts[part].max_epoch,
                records = seg.records.len(),
                "FENCING VIOLATION: segment from a superseded lease epoch; skipping"
            );
            self.meta.set_applied_seq(seq)?;
        } else {
            // Only *pending* (unshipped) local records may suppress a
            // foreign one: ours sit later in the global log than
            // anything we tail, so ours win everywhere. Speculative
            // forwarded records (shadows) must NOT suppress: the holder
            // already sequenced them, so a peer's record for the same
            // inode can legitimately follow ours. Skipping it would drop
            // it for good — the applied position never revisits a
            // segment — leaving each requester pinned to its own value.
            //
            // Plan 30 §M3b: only this node's *uncaptured* journal suppresses
            // (`Meta::pending_touches`). Captured transactions are exact
            // instead: the segment is inserted before them.
            let pending = self.meta.pending_touches()?;
            // Plan 30 §M3: one transaction strands whatever speculation
            // this segment's epoch supersedes — requester shadows and hints,
            // and, if this node was deposed, its own unshipped transactions
            // (rolled back, redone around, ops queued for replay by rid —
            // `recovery::drain_pending_replays` runs them) — applies the
            // records, retires shadows by rid and hints by position, and
            // advances `applied_seq`.
            let applied = self
                .meta
                .apply_segment(seq, seg.epoch, &seg.records, &pending)?;
            let skipped = applied.skipped;
            if applied.stranded.any() {
                tracing::warn!(
                    seq,
                    part,
                    epoch = seg.epoch,
                    shadows = applied.stranded.shadows,
                    hints = applied.stranded.hints,
                    local = applied.stranded.locals,
                    "segment from a later epoch stranded speculative state; rolled back, \
                     stranded ops queued for replay by rid"
                );
                let mut spool = self.spool.lock().unwrap();
                spool.speculation_rolled_back +=
                    (applied.stranded.shadows + applied.stranded.hints) as u64;
                spool.local_rolled_back += applied.stranded.locals as u64;
            }
            if applied.inserted_before_local {
                tracing::info!(
                    seq,
                    part,
                    epoch = seg.epoch,
                    "applied a late segment before this node's unshipped transactions \
                     (rolled back, applied, redone)"
                );
            }
            if skipped > 0 {
                self.spool.lock().unwrap().conflicts += skipped as u64;
            }
            tracing::debug!(
                seq,
                part,
                node = seg.node,
                epoch = seg.epoch,
                records = seg.records.len(),
                skipped,
                "applied foreign segment"
            );
        }
        let st = self.parts.get_mut(part).unwrap();
        st.max_epoch = st.max_epoch.max(seg.epoch);
        st.next_seq = seq + 1;
        {
            let mut spool = self.spool.lock().unwrap();
            spool.head_seq = spool.head_seq.max(seq);
            spool.last_error = None;
        }
        // Applied (or, for our own recovered segment, confirmed
        // applied): `apply_foreign`/journal replay above already dirtied
        // whatever keys these records touched, in the same transaction —
        // there is nothing left to note here (plan 29 M2).
        Ok(())
    }

    /// Ship every partition that has journaled records. A partition
    /// without a lease keeper yet gets one created and acquired here:
    /// keepers are otherwise only made by the FUSE write gate, so a
    /// daemon that restarted with a stranded child-partition journal
    /// would never ship it.
    /// The journal is read **once** here and each partition's batch is
    /// handed to [`Shipper::ship_part`]. Reading it again per partition
    /// (as this used to) meant decoding every journaled record twice per
    /// round for a batch that cannot have changed in between — the FUSE
    /// side only appends, and appends are picked up by the next round.
    async fn ship_all(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<bool> {
        let grouped = self.meta.take_journal_grouped(SEGMENT_BATCH)?;
        if grouped.is_empty() {
            return Ok(false);
        }
        let mut more = false;
        for (part, batch) in grouped {
            self.ensure_part(&part);
            if !leases.contains_key(&part) {
                let mut keeper = LeaseKeeper::new(
                    LeaseStore::new(self.log.inner(), &part, self.lease_mode),
                    self.node_id,
                )
                .with_holder_epoch(self.meta.holder_epoch_cell());
                keeper.note_acquire_reason("ship-pending-journal");
                match acquire_lease_for(self, &mut keeper, &part).await {
                    Ok(true) => {
                        leases.insert(part.clone(), keeper);
                    }
                    // A live foreign holder: leave the records journaled
                    // and retry next round.
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(error = %e, part, "lease acquisition for pending journal failed");
                        continue;
                    }
                }
            }
            let needs_reacquire = leases
                .get(&part)
                .is_some_and(|keeper| !keeper.is_lost() && keeper.ship_epoch().is_none());
            if needs_reacquire {
                let keeper = leases.get_mut(&part).expect("checked above");
                keeper.note_acquire_reason("ship-reacquire");
                match acquire_lease_for(self, keeper, &part).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            part,
                            "lease reacquisition for pending journal failed"
                        );
                        continue;
                    }
                }
            }
            let Some(lease) = leases.get(&part) else {
                continue;
            };
            if self.ship_part(&part, lease.ship_epoch(), batch).await? {
                more = true;
            }
        }
        Ok(more)
    }

    /// [`Shipper::ship_part`] for a caller that has not already read the
    /// journal (the single-partition `sync_one` path).
    async fn ship_part_taking(&mut self, part: &str, lease: &LeaseKeeper) -> Result<bool> {
        let batch = self
            .meta
            .take_journal_grouped(SEGMENT_BATCH)?
            .into_iter()
            .find(|(p, _)| p == part)
            .map(|(_, batch)| batch)
            .unwrap_or_default();
        self.ship_part(part, lease.ship_epoch(), batch).await
    }

    /// Ship one journal batch for `part`. Returns true if another
    /// round is needed (more records pending, or a CAS collision).
    ///
    /// One PUT per round, never a pipeline. Pipelining segment PUTs is a
    /// measured 5–10× on ship rate (plan 26's appendix: 5.1 → 29.4 seg/s
    /// HU→AWS at depth 8, 33 → 231 same-region, 6.2 → 41.6 against OVH),
    /// and it is still deliberately not done: a holder reaches the same
    /// *records* per second by letting the journal accumulate while one
    /// PUT is in flight and shipping one larger segment. At `SEGMENT_BATCH`
    /// = 10k records and a 200 ms round trip that is ~50k records/s, an
    /// order of magnitude above what the FUSE path in front of it
    /// produces. Depth > 1 would buy throughput we do not need in exchange
    /// for a real hazard: with several sequences in flight, a failure of
    /// one leaves a hole that stalls every tailer behind it until it is
    /// filled or the stream is repaired. The byte cap below is what an
    /// accumulating journal actually needs.
    ///
    /// Takes `epoch` as a plain value rather than `&LeaseKeeper` (plan 30
    /// M2b): the PUT below is the one piece of the ordinary ship path
    /// that must not run with the keepers map locked (see `lease.rs`'s
    /// module doc, "Locking rules"), and this is the only thing it ever
    /// read from the keeper — no `&LeaseKeeper` reference survives past
    /// the caller's snapshot.
    async fn ship_part(
        &mut self,
        part: &str,
        epoch: Option<u64>,
        batch: constellation_meta::JournalBatch,
    ) -> Result<bool> {
        if self.skip_ship.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(false);
        }
        let Some(epoch) = epoch else {
            return Ok(false);
        };
        if batch.is_empty() {
            return Ok(false);
        }
        let mut records: Vec<LogRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        // Ride-along atime drain (plan 20): a write segment is going out
        // for this partition anyway, so fold in the partition's pending
        // read-time atime bumps rather than pay a separate PUT. Cleared
        // only after the PUT succeeds below — a failed ship leaves the
        // rows for next round, and a duplicate re-ship is absorbed by
        // the max-merge on replay. Atime never triggers a ship on its
        // own here; an atime-only partition relies on the idle-release
        // drain (ship-then-release) instead.
        let journaled = records.len();
        let atime_rows = self.meta.take_atime_of(part, SEGMENT_BATCH)?;
        for (ino, atime_ns, time_ns) in &atime_rows {
            records.push(LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            });
        }
        // Cut to what fits in one segment. The ride-along atime rows sit at
        // the end and so are the first thing dropped, which is right: they
        // are droppable by definition, and a journaled record cut here is
        // simply shipped by the next round (this returns `true`, so the
        // caller comes straight back).
        //
        // Plan 30 §M3b: never inside a transaction. An op's records and its
        // `Completed { rid }` ship together or not at all — a holder dying
        // between two segments that split them would leave the op in
        // effect with its rid uncompleted, and a replay by rid would then
        // execute it a second time. `batch` itself ends on a transaction
        // boundary (`take_journal_grouped`); a byte cut is moved back to
        // the last boundary before it, or — when even the first
        // transaction is over the cap — forward to the end of that one.
        let fits = records_within_cap(&records)?;
        let (shipped_journal, shipped_atime) = if fits >= journaled {
            (journaled, fits - journaled)
        } else {
            (self.meta.whole_tx_prefix(&batch, fits)?, 0)
        };
        let fits = shipped_journal + shipped_atime;
        records.truncate(fits);
        let batch = &batch[..shipped_journal];
        let atime_rows = &atime_rows[..shipped_atime];
        let payload = encode(self.node_id, epoch, &records)?;
        let next_seq = self.parts[part].next_seq;
        match self.parts[part].log.put_segment(next_seq, &payload).await {
            Ok(()) => {}
            // The sequence we aimed at is taken. Because a holder no
            // longer tails its own stream, this CAS collision is the only
            // thing left that tells it the stream moved without it, so
            // the segment has to be absorbed here rather than by the next
            // round's tail: normally our own unacked segment from before
            // a restart (recovered by `apply_decoded_segment`), otherwise
            // a deposed holder's late write, which the fence rejects.
            // Skipping this would spin forever at the same sequence.
            Err(constellation_store_s3::StoreError::AlreadyExists) => {
                self.tail_part(part).await?;
                return Ok(true);
            }
            Err(e) => return Err(e).context("shipping log segment"),
        }
        let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
        self.meta.ack_journal_rows_at(&seqs, next_seq)?;
        if !atime_rows.is_empty() {
            let inos: Vec<_> = atime_rows.iter().map(|(ino, _, _)| *ino).collect();
            self.meta.clear_atime(part, &inos)?;
        }
        tracing::debug!(
            seq = next_seq,
            part,
            epoch,
            records = records.len(),
            "shipped log segment"
        );
        self.last_ship_epoch = self.last_ship_epoch.max(epoch);
        // Push invalidation: tell peers the segment is durable so they
        // tail now rather than at their next poll. Best effort by
        // design — the poll is what guarantees they converge.
        //
        // In E2E mode the pushed body is the *sealed* segment (identical
        // to the S3 object), not plaintext: the gossip topic is joinable
        // by anyone who read `gossip_secret` from `meta.json`, so an
        // unsealed push would leak filenames and structure to a bucket
        // reader. A peer opens it with the passphrase-derived DEK; without
        // the DEK it is as opaque as the S3 object. Encode failure just
        // drops the hint (peers still converge via the S3 poll).
        let pushed = if self.parts[part].log.is_e2e() {
            self.parts[part].log.seal_segment(next_seq, &payload).ok()
        } else {
            Some(payload.clone())
        };
        self.peers
            .announce_segment(part, next_seq, epoch, pushed)
            .await;
        // Offline designation flush-ack (DESIGN.md §5.2, phase 4a): if
        // any shipped record touches a path designated to a different
        // node, wait (bounded) for that designee's ack. The segment is
        // already durable in S3 at this point — the CAS above is what
        // committed it, and the log's exactly-once sequencing means it
        // cannot be un-shipped — so a missed/timed-out ack cannot be
        // turned into "stay journaled and retry" without either
        // double-shipping the same records under a new seq or
        // restructuring the journal-ack/seq coupling. This is logged as
        // a best-effort verification point (the ~1 RTT cost DESIGN.md
        // budgets for) rather than a hard gate; see PROGRESS.md for the
        // scope note.
        if let Some(designations) = self.designations.clone() {
            self.verify_flush_acks(&designations, part, next_seq, &records)
                .await;
        }
        let st = self.parts.get_mut(part).unwrap();
        st.max_epoch = st.max_epoch.max(epoch);
        st.next_seq = next_seq + 1;
        {
            let mut spool = self.spool.lock().unwrap();
            spool.head_seq = spool.head_seq.max(next_seq);
            spool.last_error = None;
        }
        self.shipped_since_publish += 1;
        if self.publish_is_due() {
            self.publish_in_background();
        }
        Ok(true)
    }

    /// The segment-count cadence's publish, off the ship path (plan 30
    /// §M3b). A publish is several S3 round trips (pack and blob PUTs, the
    /// commit CAS, condemned-list reads) and, for a holder with an
    /// unshipped journal, reads every outstanding `Local` row's
    /// before-images; awaited here it stalled shipping for all of that
    /// every `PUBLISH_EVERY` segments while forwarded ops kept arriving.
    /// It needs nothing from this round: it reads one fjall snapshot and
    /// holds only the publisher's own lock, so it runs as its own task and
    /// the next cadence skips while it is still busy.
    fn publish_in_background(&mut self) {
        self.shipped_since_publish = 0;
        self.last_publish_attempt = Instant::now();
        let epoch = self.last_ship_epoch;
        let Some(publisher) = self.publisher.clone() else {
            return;
        };
        let Ok(mut publisher) = publisher.try_lock_owned() else {
            return;
        };
        tokio::spawn(async move {
            if let Err(e) = publisher.publish(epoch).await {
                tracing::warn!(error = %e, "metadata tree publish failed; retrying next round");
            }
        });
    }

    /// Drain a partition's pending read-time atime (plan 20) into one
    /// final segment under `lease`'s epoch, then clear it — called
    /// before an idle lease release so a read-heavy holder's atime
    /// reaches the cluster instead of dying with the lease. Best effort:
    /// if the segment cannot be put, the rows are
    /// dropped (atime is droppable by definition) and the release
    /// proceeds regardless — atime must never delay a handoff.
    pub async fn ship_atime_before_release(&mut self, part: &str, lease: &LeaseKeeper) {
        self.ensure_part(part);
        let Some(epoch) = lease.ship_epoch() else {
            let _ = self.meta.drop_atime_of(part);
            return;
        };
        let rows = match self.meta.take_atime_of(part, SEGMENT_BATCH) {
            Ok(r) if !r.is_empty() => r,
            Ok(_) => return,
            Err(e) => {
                tracing::debug!(error = %e, part, "atime drain read failed; dropping");
                let _ = self.meta.drop_atime_of(part);
                return;
            }
        };
        let records: Vec<LogRecord> = rows
            .iter()
            .map(|(ino, atime_ns, time_ns)| LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            })
            .collect();
        let next_seq = self.parts[part].next_seq;
        let payload = match encode(self.node_id, epoch, &records) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(error = %e, part, "atime segment encode failed; dropping");
                let _ = self.meta.drop_atime_of(part);
                return;
            }
        };
        match self.parts[part].log.put_segment(next_seq, &payload).await {
            Ok(()) => {
                let inos: Vec<_> = rows.iter().map(|(ino, _, _)| *ino).collect();
                let _ = self.meta.clear_atime(part, &inos);
                self.after_atime_segment(part, next_seq, epoch, payload)
                    .await;
                tracing::debug!(
                    seq = next_seq,
                    part,
                    epoch,
                    rows = records.len(),
                    "shipped final atime segment before release"
                );
            }
            // Someone else advanced the stream, or the PUT failed: the
            // rows are droppable, and we are releasing anyway.
            Err(e) => {
                tracing::debug!(error = %e, part, "final atime ship failed; dropping rows");
                let _ = self.meta.drop_atime_of(part);
            }
        }
    }

    /// Bookkeeping after an atime-only segment landed at `seq`: advance
    /// the stream, publish the spool head and push the segment to peers
    /// (best effort, as in [`Self::ship_part`]).
    async fn after_atime_segment(&mut self, part: &str, seq: u64, epoch: u64, payload: Vec<u8>) {
        let st = self.parts.get_mut(part).unwrap();
        st.max_epoch = st.max_epoch.max(epoch);
        st.next_seq = seq + 1;
        {
            let mut spool = self.spool.lock().unwrap();
            spool.head_seq = spool.head_seq.max(seq);
        }
        let pushed = if self.parts[part].log.is_e2e() {
            self.parts[part].log.seal_segment(seq, &payload).ok()
        } else {
            Some(payload)
        };
        self.peers.announce_segment(part, seq, epoch, pushed).await;
    }

    /// Standalone atime-only ship (plan 29 M3b,
    /// `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S`). The two existing atime
    /// ship paths both piggyback on other lease activity — `ship_part`'s
    /// ride-along only fires behind a real write segment, and
    /// `ship_atime_before_release` only fires when the lease is about to
    /// be released — so a holder that keeps busily absorbing read-time
    /// bumps but never writes and never idles would otherwise leave them
    /// in `atime_journal` forever. Called once per sync round that
    /// shipped nothing else; a no-op unless the oldest pending row is
    /// older than the ceiling. Unlike the release-time drain, a failure
    /// here leaves the rows queued for the next round rather than
    /// dropping them — the lease is not going anywhere, so there is no
    /// reason to give up on them.
    pub async fn ship_atime_if_stale(&mut self, part: &str, lease: &LeaseKeeper) {
        let Some(epoch) = lease.ship_epoch() else {
            return;
        };
        let oldest_ns = match self.meta.atime_oldest_pending_ns(part) {
            Ok(Some(t)) => t,
            Ok(None) => return,
            Err(e) => {
                tracing::debug!(error = %e, part, "atime staleness check failed");
                return;
            }
        };
        let age_ns = constellation_fs_core::types::now_ns().saturating_sub(oldest_ns);
        if age_ns < crate::atime::ship_max_delay().as_nanos() as i64 {
            return;
        }
        self.ensure_part(part);
        let rows = match self.meta.take_atime_of(part, SEGMENT_BATCH) {
            Ok(r) if !r.is_empty() => r,
            Ok(_) => return,
            Err(e) => {
                tracing::debug!(error = %e, part, "standalone atime drain read failed");
                return;
            }
        };
        let records: Vec<LogRecord> = rows
            .iter()
            .map(|(ino, atime_ns, time_ns)| LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            })
            .collect();
        let next_seq = self.parts[part].next_seq;
        let payload = match encode(self.node_id, epoch, &records) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(error = %e, part, "standalone atime segment encode failed");
                return;
            }
        };
        match self.parts[part].log.put_segment(next_seq, &payload).await {
            Ok(()) => {
                let inos: Vec<_> = rows.iter().map(|(ino, _, _)| *ino).collect();
                let _ = self.meta.clear_atime(part, &inos);
                self.after_atime_segment(part, next_seq, epoch, payload)
                    .await;
                tracing::debug!(
                    seq = next_seq,
                    part,
                    epoch,
                    rows = records.len(),
                    "shipped standalone atime segment (ship-max-delay)"
                );
            }
            // Someone else advanced the stream: pick it up on the next
            // tail rather than losing these rows or wedging on a stale
            // sequence.
            Err(constellation_store_s3::StoreError::AlreadyExists) => {
                let _ = self.tail_part(part).await;
            }
            Err(e) => {
                tracing::debug!(error = %e, part, "standalone atime ship failed; retrying next round");
            }
        }
    }

    /// Which directory a record's traffic is attributed to. Records that
    /// name an inode rather than a parent (`setattr`, `write_manifest` —
    /// the bulk of a file write) are attributed to that inode's parent
    /// directory, otherwise a file-heavy directory would never accumulate
    /// traffic against itself.
    fn traffic_dir_of(&self, rec: &LogRecord) -> Option<u64> {
        match rec {
            LogRecord::Mkdir { parent, .. }
            | LogRecord::Create { parent, .. }
            | LogRecord::Symlink { parent, .. }
            | LogRecord::Mknod { parent, .. }
            | LogRecord::Link { parent, .. }
            | LogRecord::Unlink { parent, .. }
            | LogRecord::Rmdir { parent, .. }
            | LogRecord::Rename { parent, .. } => Some(*parent),
            LogRecord::Setattr { ino, .. } | LogRecord::WriteManifest { ino, .. } => {
                self.meta.parent_of(*ino).ok().flatten()
            }
            _ => None,
        }
    }

    /// Best-effort verification that the designee (if any) has seen a
    /// just-shipped batch touching its designated path. See the caller
    /// for why this cannot be a hard journal-ack gate. Distinct
    /// directories in the batch are deduplicated so a single foreign
    /// designation is only asked about once per shipped segment.
    async fn verify_flush_acks(
        &self,
        designations: &crate::designation::DesignationManager,
        part: &str,
        seq: u64,
        records: &[LogRecord],
    ) {
        let mut checked_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
        for rec in records {
            let Some(dir) = self.traffic_dir_of(rec) else {
                continue;
            };
            let Ok(path) = self.meta.path_of(dir) else {
                continue;
            };
            if !checked_paths.insert(path.clone()) {
                continue;
            }
            if !designations.await_flush_ack(&path, part, seq).await {
                tracing::warn!(
                    part,
                    seq,
                    path,
                    "designee did not ack this flush within the bound; \
                     the write is durable in S3 but the designee's view \
                     may lag briefly"
                );
            }
        }
    }

    /// Enough segments shipped since the last publish. Shutdown and
    /// explicit sync paths bypass this and publish unconditionally.
    /// (Plan 30 §M3a's "a node with outstanding speculation does not
    /// publish" is enforced below all of them, in
    /// `TreePublisher::publish`.)
    fn publish_is_due(&self) -> bool {
        self.shipped_since_publish >= PUBLISH_EVERY
    }

    /// Publish the pending metadata tree edits as a commit (see
    /// [`Shipper::publish_tree`]). A mount with no publisher (a read-only
    /// member, or most tests) has nothing to publish and this is a no-op.
    pub async fn publish(&mut self) -> Result<()> {
        // The cadence counter is reset *before* the work, not after it.
        // The daemon's sync loop drops a running round whenever an
        // explicit request arrives (a write-through close sends one), so
        // this future can be cancelled at any await below; resetting only
        // on success would leave a cancelled publish due on every
        // following segment. A cancelled publish now simply waits for the
        // next cadence — the publish itself runs in a spawned task
        // ([`Shipper::publish_tree`]) and is cancellation-safe regardless.
        //
        // Plan 30 §M3b: waits for a cadence publish still running in the
        // background (`publish_in_background`) rather than skipping — an
        // explicit, idle or shutdown publish must actually publish.
        self.shipped_since_publish = 0;
        self.publish_tree(true).await;
        Ok(())
    }

    /// Final sync + publish on clean unmount.
    #[allow(dead_code)]
    pub async fn shutdown(&mut self, lease: &LeaseKeeper) -> Result<()> {
        self.sync(lease).await?;
        if self.meta.has_dirty() {
            self.publish().await?;
        }
        Ok(())
    }

    pub async fn shutdown_all(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        let initial = MetaStore::journal_len(&*self.meta).unwrap_or(0);
        if initial > 0 {
            tracing::info!(journal_backlog = initial, "shipping journal before unmount");
        }
        let mut last_progress = Instant::now();
        loop {
            let held = self.held_partitions(leases);
            self.tail_all_except(&held).await?;
            if matches!(self.meta.kv_get("lease_lost")?.as_deref(), Some("1")) {
                tracing::info!(
                    journal_backlog = MetaStore::journal_len(&*self.meta).unwrap_or(0),
                    "deposed node remains tail-only until its deposition recovery runs; \
                     journal will not ship on this unmount"
                );
                return Ok(());
            }
            let shipped_more = self.ship_all(leases).await?;
            let remaining = MetaStore::journal_len(&*self.meta).unwrap_or(0);
            if initial > 0
                && (!shipped_more || last_progress.elapsed() >= std::time::Duration::from_secs(5))
            {
                tracing::info!(
                    journal_backlog = remaining,
                    initial,
                    "journal ship progress"
                );
                last_progress = Instant::now();
            }
            if !shipped_more {
                break;
            }
        }
        // Plan 30 §M4 item 3: only the lease holder publishes. A follower's
        // dirty keys are the holder's to publish (it tailed the same log).
        if self.meta.has_dirty()
            && (self.is_publisher() || leases.values().any(|k| k.ship_epoch().is_some()))
        {
            self.publish().await?;
        }
        if initial > 0 {
            tracing::info!(shipped = initial, "journal ship complete");
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn journal_backlog(&self) -> u64 {
        MetaStore::journal_len(&*self.meta).unwrap_or(0)
    }

    /// Whether `part` has unshipped journal records: 0 or 1, never a
    /// count. Every caller only asks "is anything left?", and it runs
    /// on every sync round, so it reads at most one row instead of
    /// decoding the whole journal (which made each round O(backlog)
    /// while forwarded mutations queued behind it). There is one
    /// partition since plan 29 M0a, so `part` is not consulted.
    pub fn journal_backlog_of(&self, _part: &str) -> u64 {
        MetaStore::take_journal(&*self.meta, 1)
            .map(|rows| rows.len() as u64)
            .unwrap_or(0)
    }

    /// Ask a live foreign holder for `part` by writing this node into the
    /// lease object's `wanted_by` list (plan 26 Step 7b).
    ///
    /// This is the only signal a requester has when P2P is down, and it
    /// costs one CAS PUT: the holder's next renewal fails against the
    /// edited object, which is how it learns to release. Everything here
    /// is best effort — a lost CAS means somebody else edited or took the
    /// lease first, and the next `Acquire` re-reads it — so nothing in the
    /// acquisition path branches on the outcome.
    ///
    /// Only in [`LeaseMode::Cas`]. Without `If-Match` a swap is a blind
    /// overwrite, and blindly rewriting a live holder's lease object is
    /// precisely the race that mode cannot afford.
    async fn register_wanted_by(
        &mut self,
        keeper: &LeaseKeeper,
        part: &str,
        prev: &Lease,
        tag: &LeaseTag,
    ) {
        if self.lease_mode != LeaseMode::Cas || prev.wanted_by.contains(&self.node_id) {
            return;
        }
        // Half a TTL is the holder's own renewal period: registering more
        // often than it can look cannot make it *notice* any sooner. The
        // timestamp is taken per *attempt*, not per success — a CAS that
        // lost still means the object moved under us, and hammering it
        // from a blocked FUSE thread would only add request traffic.
        //
        // Capped (plan 29 M3c): a lost CAS here can also mean a *second*
        // waiter's registration collided with ours, not just the holder's
        // own renewal — with several waiters and a short TTL, half-TTL
        // spacing left as few as four attempts inside the FUSE acquire
        // deadline (2xTTL), and `create-storm-s3-only` could see two
        // simultaneously-blocked waiters repeatedly collide with each
        // other, each collision costing a full half-TTL before either
        // retried. The cap only shortens the *retry-after-conflict* path;
        // once registered, `wanted_by.contains` above still skips every
        // later attempt regardless of the cap, so a healthy single-waiter
        // handoff is unaffected.
        let cooldown = std::time::Duration::from_millis(
            (constellation_store_s3::lease::lease_ttl_ms().max(2) / 2)
                .min(REGISTER_WANTED_COOLDOWN_CEILING_MS),
        );
        if self
            .wanted_registered_at
            .get(part)
            .is_some_and(|at| at.elapsed() < cooldown)
        {
            return;
        }
        self.wanted_registered_at
            .insert(part.to_string(), Instant::now());
        match keeper.register_wanted(prev, tag).await {
            Ok(landed) => tracing::debug!(
                part,
                holder = prev.holder,
                landed,
                "registered a handoff request in the partition lease"
            ),
            Err(error) => {
                tracing::debug!(%error, part, "could not register a handoff request")
            }
        }
    }
}

/// How long this node may go without reading a partition it believes it
/// holds. Caps how stale a *deposed* holder's view can get.
///
/// [`LeaseKeeper::ship_epoch`] alone is not enough to skip the read. It is
/// `None` for a keeper that knows it is expired, released or deposed — but
/// a keeper only *finds out* at its own renewal CAS, which is half a TTL
/// away (30 s at the default 60 s TTL). Until then its view still says
/// "usable", the partition stays in the held set, and its stream stays
/// unread: a deposed holder cannot see the new holder's writes for up to
/// TTL/2. Correctness was never at risk (the epoch fences anything the
/// deposed node ships), but the read staleness is real, and it is what
/// made `two-clients-shared`, `lease-handover` and `rename-across-partitions`
/// regress against a plan-26-free tree.
///
/// So the skip is tied to *freshness* rather than to usability: a held
/// partition is read again once this long has passed since we last read
/// it, whatever the lease view claims.
///
/// This keeps Step 3's measured win. That win — 2.6 → 5.1 shipped seg/s
/// HU→AWS — came from dropping a LIST that ran *per shipped segment*, a
/// cost that scales with throughput; this costs at most one probe per
/// bound per partition, a cost that scales with time. Over the 500-file
/// burst in `wan-writer-ships-put-only` the two are orders of magnitude
/// apart. And after Step 4b the read is a GET-next probe rather than a
/// LIST: ~1/12.5 of the request price on AWS and latency-neutral (177 ms
/// GET-404 vs 175 ms idle LIST HU→AWS). A holder probing its own stream
/// finds nothing at `next_seq` and returns on the first 404, so it never
/// reaches the LIST catch-up path — `wan-writer-ships-put-only`'s
/// zero-LIST assertion still holds.
///
/// The bound only forces a read when a sync round happens anyway; it
/// never schedules one. An idle node polling at the 30 s ceiling is
/// therefore still bounded by its poll, not by this.
const HELD_TAIL_MAX_STALENESS: std::time::Duration = std::time::Duration::from_secs(5);

impl Shipper {
    /// The partitions whose streams this node may skip reading this round:
    /// it is the sole legal appender *and* it has read them recently
    /// enough that a silent deposition cannot have gone unnoticed for
    /// longer than [`HELD_TAIL_MAX_STALENESS`].
    ///
    /// Records the read time for the partitions it lets through to the
    /// tail, so the cost is one probe per bound rather than one per round.
    fn held_partitions(&mut self, leases: &HashMap<String, LeaseKeeper>) -> HashSet<String> {
        let now = Instant::now();
        let mut held = HashSet::new();
        for (part, keeper) in leases.iter() {
            if keeper.ship_epoch().is_none() {
                // Known not to hold it: read it, and do not claim the read
                // as a held-partition refresh.
                continue;
            }
            match self.held_tail_at.get(part) {
                // Seen recently enough that a silent deposition cannot
                // have gone unnoticed for longer than the bound.
                Some(at) if now.duration_since(*at) < HELD_TAIL_MAX_STALENESS => {
                    held.insert(part.clone());
                }
                // Stale: falls through to the read this round, and that
                // read is what the next bound is measured from.
                Some(_) => {
                    self.held_tail_at.insert(part.clone(), now);
                }
                // First round holding it. Acquisition already tailed this
                // stream to head (`acquire_lease_for`'s takeover witness),
                // so there is nothing to catch up on; start the bound from
                // that read rather than immediately repeating it.
                None => {
                    self.held_tail_at.insert(part.clone(), now);
                    held.insert(part.clone());
                }
            }
        }
        held
    }

    /// Test hook: age a held partition's read clock past
    /// [`HELD_TAIL_MAX_STALENESS`] instead of sleeping through it, so a
    /// test can exercise the bound — the only thing that brings a deposed
    /// holder's stream back in production — without a wall-clock wait.
    #[cfg(test)]
    pub(crate) fn expire_held_tail_for_test(&mut self, part: &str) {
        let aged = Instant::now()
            .checked_sub(HELD_TAIL_MAX_STALENESS * 2)
            .expect("monotonic clock is far enough from its origin");
        self.held_tail_at.insert(part.to_string(), aged);
    }
}

/// Acquire the lease for `part` if it is free, tailing that stream to
/// head first when this would be a takeover from another node. Returns
/// false when a live foreign holder still owns it (the caller waits and
/// retries), or when this node holds it but its takeover gate has not
/// completed yet (retried by the next call or sync round). This is the
/// *only* acquisition path, which is what makes the takeover ordering rule
/// structural.
pub async fn acquire_lease(ship: &mut Shipper, keeper: &mut LeaseKeeper) -> Result<bool> {
    acquire_lease_for(ship, keeper, PARTITION).await
}

pub async fn acquire_lease_for(
    ship: &mut Shipper,
    keeper: &mut LeaseKeeper,
    part: &str,
) -> Result<bool> {
    let plan = keeper.classify().await?;
    if let crate::lease::Plan::Busy {
        holder,
        expires_in_ms,
        prev,
        tag,
    } = &plan
    {
        tracing::debug!(
            holder,
            expires_in_ms,
            part,
            "partition lease held by another node"
        );
        ship.register_wanted_by(keeper, part, prev, tag).await;
    }
    // Plan 30 §M4: a re-adoption of our own unreleased lease that this
    // keeper does not track is gated like a takeover, so it tails first too
    // (`LeaseKeeper::readopts`).
    let takeover = plan.needs_tail() || keeper.readopts(&plan);
    let tailed = if takeover {
        Some(ship.tail_part_to_head(part).await?)
    } else {
        None
    };
    match keeper.commit_cas(plan, tailed).await? {
        crate::lease::CasOutcome::NotWon => Ok(false),
        // Ours already. A gate left pending by an earlier failure is
        // retried here, so a FUSE thread's own `Acquire` drives it too.
        crate::lease::CasOutcome::Held => Ok(complete_gate(ship, keeper, part).await),
        crate::lease::CasOutcome::Won(won) => {
            // Plan 30 §M3 takeover gate. The view stays closed (and
            // nothing ships) from the CAS until the gate completes:
            // 1. a takeover from a holder that did not release ships an
            //    empty epoch-marker segment first, so no late segment of
            //    the old epoch can land after anything the gate replays,
            //    and third nodes strand promptly (a released predecessor
            //    flushed first and ships nothing more: no marker);
            // 2. roll back what the new epoch strands and replay every
            //    queued stranded op locally, in order
            //    (`recovery::takeover_gate`).
            // A failure leaves the gate pending (see `complete_gate`).
            let pending = crate::lease::PendingGate {
                epoch: won.epoch(),
                takeover: won.takeover,
                marker_shipped: !won.marker,
            };
            keeper.open_won(won, Some(pending)).await;
            Ok(complete_gate(ship, keeper, part).await)
        }
    }
}

/// Run, or resume, `keeper`'s pending takeover gate (plan 30 §M3b).
/// Returns whether it is complete (always, when none is pending).
///
/// Called right after a won CAS (`acquire_lease_for`), by a later
/// `Acquire` that finds the lease already ours, and by every sync round
/// (`main::run_sync_round`) while one is pending. Holds whatever the
/// caller holds (the keepers lock, the shipper) across the marker PUT:
/// this is the acquisition path, which already does S3 I/O under them.
///
/// **Failure** leaves the lease held but the view closed: no FUSE write,
/// local forward or peer's forwarded op executes, and nothing ships
/// (`LeaseKeeper::ship_epoch` is `None`), until a retry completes the
/// gate. Opening anyway would let new ops run ahead of — and validate
/// without — the stranded ops still queued, which is the reordering the
/// gate exists to prevent; releasing would strand the replays the gate
/// already executed behind a lease nobody else can take for a TTL. A
/// holder that can never complete it (a persistently failing metadata
/// store) behaves like a paused one: writes wait out the FUSE acquire
/// deadline and fail with `EIO`, and a peer that wants the lease gets it
/// by the ordinary idle release once the backlog reads zero.
pub async fn complete_gate(ship: &mut Shipper, keeper: &mut LeaseKeeper, part: &str) -> bool {
    let Some(mut gate) = keeper.pending_gate() else {
        return true;
    };
    if !gate.marker_shipped {
        match ship.ship_epoch_marker(part, gate.epoch).await {
            Ok(()) => {
                gate.marker_shipped = true;
                keeper.set_pending_gate(Some(gate));
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    part,
                    epoch = gate.epoch,
                    "could not ship the takeover's epoch marker; the takeover gate stays \
                     pending and new mutations wait"
                );
                return false;
            }
        }
    }
    let meta = ship.meta.clone();
    let spool = ship.spool.clone();
    match crate::recovery::takeover_gate(&meta, &spool, ship.node_id, gate.epoch, gate.takeover) {
        Ok(()) => {
            // Plan 30 §M13: still inside the gate, after this node's own
            // stranded ops and before the view opens, every batch of
            // every older epoch's inbox — a requester's op the previous
            // holder never polled lands before anything issued after the
            // takeover, and one the previous holder did execute is
            // deduplicated. A failed drain keeps the gate pending like a
            // failed replay would.
            if let Some(inbox) = ship.inbox.clone() {
                if let Err(error) =
                    crate::inbox::drain_at_takeover(&inbox, &meta, ship.node_id, gate.epoch).await
                {
                    tracing::error!(
                        %error,
                        part,
                        epoch = gate.epoch,
                        "takeover gate: draining older epochs' inbox batches failed; the gate                          stays pending and new mutations wait"
                    );
                    return false;
                }
            }
            keeper.finish_gate();
            true
        }
        Err(error) => {
            tracing::error!(
                %error,
                part,
                epoch = gate.epoch,
                "takeover gate failed; this node holds the lease but keeps new mutations \
                 closed until a later round completes the gate"
            );
            false
        }
    }
}

impl Shipper {
    /// Plan 30 §M3b: ship an empty segment at `epoch` as the first segment
    /// of a takeover's tenure (see the module doc). A collision is a late
    /// segment of an older epoch, or our own unacked segment from before a
    /// restart; it is absorbed by tailing, exactly like `ship_part`'s, and
    /// the marker retried at the next sequence. Bounded: a stream that
    /// keeps moving under a node that holds its lease is a fault, not a
    /// race.
    pub async fn ship_epoch_marker(&mut self, part: &str, epoch: u64) -> Result<()> {
        self.ensure_part(part);
        for _ in 0..16 {
            let payload = encode(self.node_id, epoch, &[])?;
            let seq = self.parts[part].next_seq;
            match self.parts[part].log.put_segment(seq, &payload).await {
                Ok(()) => {
                    // Our own segment, applied here trivially: advance the
                    // position (it carries no journal rows to ack).
                    self.meta.ack_journal_rows_at(&[], seq)?;
                    self.after_atime_segment(part, seq, epoch, payload).await;
                    self.spool.lock().unwrap().epoch_markers += 1;
                    tracing::info!(seq, part, epoch, "shipped the takeover's epoch marker");
                    return Ok(());
                }
                Err(constellation_store_s3::StoreError::AlreadyExists) => {
                    self.tail_part(part).await?;
                }
                Err(e) => return Err(e).context("shipping the epoch marker"),
            }
        }
        bail!("{part}: the log kept moving while shipping the epoch-{epoch} marker")
    }
}

#[allow(dead_code)]
pub fn new_keeper(
    store: Arc<dyn object_store::ObjectStore>,
    part: &str,
    node_id: u64,
    mode: LeaseMode,
) -> LeaseKeeper {
    LeaseKeeper::new(LeaseStore::new(store, part, mode), node_id)
}

/// Plan 28 S6: rebuild the replica from the commit chain's head, then
/// tail the log from the commit's `applied` position. `false` when the
/// chain is empty (a fresh filesystem with no commit yet), so the caller
/// falls back to a genesis replay of the whole log.
///
/// A half-written replica is removed on failure, so the fallback (or a
/// retry) starts from nothing rather than from a partial load.
async fn bootstrap_from_tree(db_path: &std::path::Path, log: &LogStore) -> Result<bool> {
    let scratch = db_path.with_extension("bootstrap-nodes");
    let _ = std::fs::remove_dir_all(&scratch);
    let result = async {
        let reader = crate::mtree_read::ChainReader::for_log(log, &scratch)?;
        if reader.head().await?.is_none() {
            return Ok(None);
        }
        let meta = Arc::new(Meta::open(db_path)?);
        let Some(loaded) =
            crate::mtree_read::bootstrap_from_commit(&reader, Arc::clone(&meta)).await?
        else {
            return Ok(None);
        };
        let replayed = replay_from(&meta, log, PARTITION, loaded.commit.applied).await?;
        crate::mtree_publish::remember_loaded_commit(&meta, &loaded.commit)?;
        for ino in meta.orphans()? {
            meta.reap_orphan(ino)?;
        }
        tracing::info!(
            seq = loaded.commit.seq,
            inodes = loaded.inodes,
            dentries = loaded.dentries,
            replayed,
            "bootstrapped metadata replica from commit"
        );
        anyhow::Ok(Some(()))
    }
    .await;
    let _ = std::fs::remove_dir_all(&scratch);
    match result {
        Ok(Some(())) => Ok(true),
        Ok(None) => {
            remove_db(db_path);
            Ok(false)
        }
        Err(e) => {
            remove_db(db_path);
            Err(e.context("bootstrapping the metadata replica from the commit chain"))
        }
    }
}

/// `db_path` is an fjall database directory (plan 29 M1: no more
/// `.db`/`-wal`/`-shm` sibling files to clean up).
fn remove_db(db_path: &std::path::Path) {
    let _ = std::fs::remove_dir_all(db_path);
}

/// Apply `part`'s contiguous log run after `start` and record where it
/// stopped. Shared by both bootstrap paths (from a commit, and genesis).
async fn replay_from(meta: &Meta, log: &LogStore, part: &str, start: u64) -> Result<usize> {
    let part_log = log.with_partition(part);
    let mut applied = start;
    let mut replayed = 0usize;
    let seqs = part_log.list_segments_from(start + 1).await?;
    // A first segment past `start + 1` means retention already pruned the
    // records this base needs. Stopping at the gap would hand back a
    // replica silently missing that history, so this must fail loudly.
    if let Some(&first) = seqs.first() {
        if first > start + 1 {
            bail!(
                "{part}: the log starts at segment {first}, but this bootstrap base \
                 covers only up to {start}; segments in between were pruned"
            );
        }
    }
    for seq in seqs {
        if seq != applied + 1 {
            break;
        }
        let seg = decode(&part_log.get_segment(seq).await?)?;
        replayed += seg.records.len();
        meta.apply_records(&seg.records)
            .with_context(|| format!("replaying {part} log segment {seq}"))?;
        applied = seq;
    }
    meta.set_applied_seq(applied)?;
    Ok(replayed)
}

/// Build a fresh local replica from S3. Used when the state dir has no
/// metadata DB: a fresh mount, or a read-only member, which bootstraps
/// from commits published by writers exactly like everyone else.
///
/// Plan 29 M0b retired the whole-DB `VACUUM INTO` checkpoint: the source
/// is the commit chain's head when one exists (restore the tree, then
/// replay the log from its `applied` position), or — a genuinely fresh
/// filesystem with no commit yet — a replay of the whole log from seq 1.
pub async fn bootstrap(db_path: &std::path::Path, log: &LogStore) -> Result<()> {
    if bootstrap_from_tree(db_path, log).await? {
        return Ok(());
    }
    let meta = Meta::open(db_path)?;
    let replayed = replay_from(&meta, log, PARTITION, 0).await?;
    for ino in meta.orphans()? {
        meta.reap_orphan(ino)?;
    }
    // Plan 25: a brand-new replica has never written local content, so any
    // `pending_upload` row would be another node's obligation — foreign
    // replay never inserts into that table, so this is normally a no-op;
    // it stays as a defensive clear after bootstrap.
    meta.clear_pending_uploads()
        .context("clearing inherited pending_upload rows after bootstrap")?;
    tracing::info!(replayed, "bootstrapped metadata replica from genesis");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseKeeper;
    use constellation_meta::MetaStore;
    use constellation_store_s3::{LeaseMode, LeaseStore, LogStore};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::sync::Arc as StdArc;

    struct Node {
        meta: Arc<Meta>,
        ship: Shipper,
        lease: LeaseKeeper,
    }

    /// The publish cadence is a plain segment count: below `PUBLISH_EVERY`
    /// nothing is due, and at or above it a publish fires.
    #[test]
    fn publish_is_due_at_the_segment_count() {
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let mut ship = node_on(store, 1).ship;
        ship.shipped_since_publish = PUBLISH_EVERY - 1;
        assert!(!ship.publish_is_due());
        ship.shipped_since_publish = PUBLISH_EVERY;
        assert!(ship.publish_is_due());
        ship.shipped_since_publish = PUBLISH_EVERY + 1;
        assert!(ship.publish_is_due());
    }

    /// Plan 29 M3b: `CONSTELLATION_ATIME_SHIP_MAX_DELAY_S` is the
    /// standalone timer that ships a holder's pending read-time atime
    /// even when it neither writes (no ride-along in `ship_part`) nor
    /// releases the lease (no `ship_atime_before_release`) — the busy
    /// read-only holder case. A fresh backlog under a generous ceiling
    /// must be left alone; the same backlog under a zero ceiling (i.e.
    /// already "stale") must ship and clear; a holder with nothing queued
    /// must never ship an empty segment. One test, not three, because the
    /// env var it drives is process-global — parallel `#[test]` fns
    /// setting it independently would race.
    #[tokio::test]
    async fn ship_atime_if_stale_only_ships_past_the_ceiling() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        acquire_lease(&mut a.ship, &mut a.lease).await.unwrap();

        // Nothing queued at all: must not ship an empty segment.
        unsafe {
            std::env::set_var("CONSTELLATION_ATIME_SHIP_MAX_DELAY_S", "0");
        }
        a.ship.ship_atime_if_stale(PARTITION, &a.lease).await;
        assert_eq!(
            a.meta.atime_backlog_of(PARTITION).unwrap(),
            0,
            "still nothing pending"
        );

        let f = a.meta.create(1, "f", 0o644, 0, 0).unwrap();
        a.meta
            .queue_atime(&[(f.ino, f.ctime_ns + 10, f.ctime_ns + 10)])
            .unwrap();

        unsafe {
            std::env::set_var("CONSTELLATION_ATIME_SHIP_MAX_DELAY_S", "3600");
        }
        a.ship.ship_atime_if_stale(PARTITION, &a.lease).await;
        assert_eq!(
            a.meta.atime_backlog_of(PARTITION).unwrap(),
            1,
            "a backlog younger than the ceiling must not ship yet"
        );

        unsafe {
            std::env::set_var("CONSTELLATION_ATIME_SHIP_MAX_DELAY_S", "0");
        }
        a.ship.ship_atime_if_stale(PARTITION, &a.lease).await;
        assert_eq!(
            a.meta.atime_backlog_of(PARTITION).unwrap(),
            0,
            "a backlog older than the ceiling must ship and clear"
        );
        let seg = segment(&store, a.ship.last_shipped_seq(PARTITION).unwrap()).await;
        assert!(
            seg.records
                .iter()
                .any(|r| matches!(r, LogRecord::Atime { ino, .. } if *ino == f.ino)),
            "the shipped segment must carry the atime record: {:?}",
            seg.records
        );

        unsafe {
            std::env::remove_var("CONSTELLATION_ATIME_SHIP_MAX_DELAY_S");
        }
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        node_on(store.clone() as StdArc<dyn ObjectStore>, id)
    }

    /// `node`, but over any backing store — the request-counting decorator
    /// below is not an `InMemory`.
    fn node_on(store: StdArc<dyn ObjectStore>, id: u64) -> Node {
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let log = LogStore::new(store.clone());
        let ship = Shipper::attach(meta.clone(), log, id).unwrap();
        let lease = LeaseKeeper::new(
            LeaseStore::new(
                store,
                constellation_store_s3::log::PARTITION,
                LeaseMode::Cas,
            ),
            id,
        );
        Node { meta, ship, lease }
    }

    impl Node {
        /// What the daemon's sync task does per round: take authority if
        /// it is available, then tail + ship.
        async fn sync(&mut self) {
            acquire_lease(&mut self.ship, &mut self.lease)
                .await
                .unwrap();
            self.ship.sync(&self.lease).await.unwrap();
        }

        async fn release(&mut self) {
            self.lease.release().await.unwrap();
        }
    }

    fn names(meta: &Meta, parent: u64) -> Vec<(String, u64)> {
        let mut v: Vec<_> = meta
            .readdir(parent)
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.ino))
            .collect();
        v.sort();
        v
    }

    async fn segment(store: &StdArc<InMemory>, seq: u64) -> Segment {
        let log = LogStore::new(store.clone());
        decode(&log.get_segment(seq).await.unwrap()).unwrap()
    }

    /// Two writers on disjoint names: both replicas converge to the
    /// union, and ino prefixes never collide. With leases they take
    /// turns; each release/acquire bumps the epoch.
    #[tokio::test]
    async fn two_nodes_disjoint_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let da = a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        let db = b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        assert_ne!(da.ino >> 40, db.ino >> 40, "ino prefixes must differ");

        a.sync().await; // ships A's segment under epoch 1
        a.release().await; // A goes idle
        b.sync().await; // B takes over (epoch 2), tails A, ships
        a.sync().await; // A tails B

        assert_eq!(names(&a.meta, 1), names(&b.meta, 1));
        assert_eq!(names(&a.meta, 1).len(), 2);
        assert_eq!(segment(&store, 1).await.epoch, 1);
        assert_eq!(
            segment(&store, 2).await.epoch,
            2,
            "handover bumps the epoch"
        );
        assert_eq!(a.ship.spool.lock().unwrap().conflicts, 0);
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 0);
    }

    /// The leaseless convergence path is still correct (it is the safety
    /// net for backends without `If-Match`): two writers create the same
    /// name, and the record later in the global log wins on every
    /// replica. Here the lease is bypassed deliberately.
    #[tokio::test]
    async fn same_name_conflict_converges_last_wins() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let fa = a.meta.create(1, "x", 0o644, 0, 0).unwrap();
        let fb = b.meta.create(1, "x", 0o644, 0, 0).unwrap();
        assert_ne!(fa.ino, fb.ino);

        a.sync().await; // A's create is log seq 1
        a.release().await;
        b.sync().await; // B tails A (conflict: B's pending wins), ships at 2
        a.sync().await; // A tails B: last-wins -> B's ino

        let ia = a.meta.lookup(1, "x").unwrap().unwrap().ino;
        let ib = b.meta.lookup(1, "x").unwrap().unwrap().ino;
        assert_eq!(ia, ib, "replicas must agree");
        assert_eq!(ia, fb.ino, "the later log record wins");
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 1);
    }

    /// Two non-holders forward a chmod for the *same* inode (the chaos
    /// `chmod_duel` shape). The holder sequences both, so each
    /// requester's shadowed record is followed in the log by its peer's.
    /// A shadow must therefore never suppress a foreign record the way a
    /// pending local record does: a skip here is permanent — the segment
    /// is marked applied and never revisited — so each requester would
    /// keep its own mode for good and the replicas would never agree.
    #[tokio::test]
    async fn forwarded_duel_on_one_inode_converges_on_every_replica() {
        use constellation_meta::{execute_mutate, MutateOp};

        let store = StdArc::new(InMemory::new());
        let mut holder = node(&store, 1);
        let mut a = node(&store, 2);
        let mut b = node(&store, 3);

        // The holder owns the file; both requesters tail it in.
        let f = holder.meta.create(1, "duel", 0o644, 0, 0).unwrap();
        holder.sync().await;
        a.ship.tail_to_head().await.unwrap();
        b.ship.tail_to_head().await.unwrap();

        let mode_of = |m: &Meta| m.lookup(1, "duel").unwrap().unwrap().mode & 0o777;
        assert_eq!(mode_of(&a.meta), 0o644, "requester must see the file first");

        // Each requester forwards a chmod: the holder executes and
        // journals it, the requester shadows and applies the records so
        // it can read its own write before the segment ships.
        for (i, (requester, mode)) in [(&mut a, 0o600u32), (&mut b, 0o640u32)]
            .into_iter()
            .enumerate()
        {
            let op = MutateOp::Setattr {
                ino: f.ino,
                mode: Some(mode),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            };
            let rid = constellation_meta::Rid {
                node: 2 + i as u64,
                incarnation: 1,
                seq: 0,
            };
            let records = execute_mutate(&holder.meta, &op, Some(rid)).unwrap();
            crate::forward::apply_accepted(&requester.meta, 1, rid, &op, &records).unwrap();
            assert_eq!(mode_of(&requester.meta), mode, "read-your-write");
        }

        // The holder publishes both records; the requesters tail the
        // authoritative order.
        holder.sync().await;
        a.ship.tail_to_head().await.unwrap();
        b.ship.tail_to_head().await.unwrap();

        assert_eq!(
            mode_of(&holder.meta),
            0o640,
            "the chmod later in the log wins at the sequencer"
        );
        assert_eq!(
            mode_of(&a.meta),
            mode_of(&holder.meta),
            "requester A pinned its own shadowed chmod instead of converging"
        );
        assert_eq!(
            mode_of(&b.meta),
            mode_of(&holder.meta),
            "requester B pinned its own shadowed chmod instead of converging"
        );
    }

    /// Deep sequential workflow: A builds a tree and publishes; B
    /// tails, mutates, publishes; A tails. Replicas stay identical.
    #[tokio::test]
    async fn sequential_cross_node_edits_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let d = a.meta.mkdir(1, "proj", 0o755, 0, 0).unwrap();
        let f = a.meta.create(d.ino, "main.rs", 0o644, 0, 0).unwrap();
        a.meta.set_manifest(f.ino, b"v1", 2).unwrap();
        a.sync().await;
        a.release().await;
        b.sync().await;

        // B sees A's tree, edits and renames.
        let bd = b.meta.lookup(1, "proj").unwrap().unwrap();
        let bf = b.meta.lookup(bd.ino, "main.rs").unwrap().unwrap();
        assert_eq!(b.meta.manifest(bf.ino).unwrap().unwrap(), b"v1");
        b.meta.set_manifest(bf.ino, b"v2-longer", 9).unwrap();
        b.meta.rename(bd.ino, "main.rs", bd.ino, "lib.rs").unwrap();
        b.sync().await;
        a.sync().await;

        let af = a.meta.lookup(d.ino, "lib.rs").unwrap().unwrap();
        assert_eq!(a.meta.manifest(af.ino).unwrap().unwrap(), b"v2-longer");
        assert!(a.meta.lookup(d.ino, "main.rs").unwrap().is_none());
        assert_eq!(af.size, 9);
        assert_eq!(a.ship.spool.lock().unwrap().conflicts, 0);
        assert_eq!(b.ship.spool.lock().unwrap().conflicts, 0);
    }

    /// A node that crashed after PUT but before ack recovers by
    /// recognizing its own segment at the head of the log.
    #[tokio::test]
    async fn own_segment_recovery_after_lost_response() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();

        // Simulate the lost response: the segment lands in S3 but the
        // journal was never acked and next_seq never advanced.
        let records: Vec<LogRecord> = a
            .meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        a.ship
            .log
            .put_segment(1, &encode(1, 1, &records).unwrap())
            .await
            .unwrap();

        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0, "journal acked");
        assert_eq!(a.ship.next_seq(), 2);
        // The op is not applied twice (dir still exists exactly once).
        assert_eq!(names(&a.meta, 1).len(), 1);
    }

    /// Nothing ships without authority, and what ships carries the
    /// holder's epoch.
    #[tokio::test]
    async fn ship_requires_the_lease_and_stamps_its_epoch() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();

        // Leaseless sync: the journal must stay put.
        a.ship.sync(&a.lease).await.unwrap();
        assert_eq!(a.meta.journal_len().unwrap(), 1, "shipped without a lease");
        assert!(a.ship.log.list_segments().await.unwrap().is_empty());

        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        let seg = segment(&store, 1).await;
        assert_eq!((seg.node, seg.epoch), (1, 1));
    }

    /// B takes A's expired lease. The takeover must apply A's flushed
    /// log first — B's replica shows A's tree before B writes anything.
    #[tokio::test]
    async fn takeover_applies_the_predecessors_log_first() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert!(b.meta.lookup(1, "from-a").unwrap().is_none());

        // A vanishes without releasing; its lease expires.
        expire_lease(&store).await;
        assert!(acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        assert!(
            b.meta.lookup(1, "from-a").unwrap().is_some(),
            "takeover must not precede applying the old holder's log"
        );
        assert_eq!(b.lease.ship_epoch(), Some(2));
    }

    /// Committing a takeover without the tail witness is refused: the
    /// ordering rule is enforced by the type, not by convention.
    #[tokio::test]
    async fn takeover_without_tail_witness_is_refused() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        expire_lease(&store).await;

        let plan = b.lease.classify().await.unwrap();
        assert!(plan.needs_tail());
        let err = b.lease.commit(plan, None).await.unwrap_err();
        assert!(err.to_string().contains("without applying its flushed log"));
    }

    /// A deposed holder refuses to ship and keeps its journal intact;
    /// the fence rejects the late segment if it ever lands.
    #[tokio::test]
    async fn deposed_holder_refuses_to_ship() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        // A stalls (frozen process) and its lease expires; B takes over.
        expire_lease(&store).await;
        b.sync().await;
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;

        // A wakes up with unshipped records and tries to continue.
        a.meta.mkdir(1, "stranded", 0o755, 0, 0).unwrap();
        a.lease.renew_now().await.unwrap();
        assert!(a.lease.is_lost(), "A must notice it was deposed");
        assert_eq!(a.lease.ship_epoch(), None);

        a.ship.sync(&a.lease).await.unwrap();
        assert_eq!(
            a.meta.journal_len().unwrap(),
            1,
            "stranded writes must stay in the journal, not vanish or ship"
        );
        // And it never reacquires: deposition is terminal until a
        // phase-4 reintegration path exists.
        assert!(acquire_lease(&mut a.ship, &mut a.lease).await.is_err());
        // B's namespace is intact.
        assert_eq!(names(&b.meta, 1).len(), 2);
        assert!(b.meta.lookup(1, "stranded").unwrap().is_none());
    }

    /// The durable lost bit is an authority gate, not just mount-time
    /// recovery metadata. Even if the in-memory keeper map is empty, an
    /// ordinary sync must not manufacture a fresh keeper and ship the
    /// stranded branch. Plan 30 §M3b: the deposition recovery is what
    /// clears it — the stranded transaction (uncaptured here: this node
    /// never recorded a holder epoch, so it is rebuilt from the log) leaves
    /// the journal and is queued for replay by rid, never shipped from here.
    #[tokio::test]
    async fn persisted_deposition_blocks_ordinary_reacquisition() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "stranded", 0o755, 0, 0).unwrap();
        a.meta.kv_set("lease_lost", "1").unwrap();
        let mut leases = HashMap::new();

        a.ship.sync_all(&mut leases).await.unwrap();
        assert!(
            leases.is_empty(),
            "ordinary sync manufactured a keeper for a deposed node"
        );
        assert_eq!(a.meta.journal_len().unwrap(), 1);
        assert!(a.ship.log.list_segments().await.unwrap().is_empty());

        let dir = tempfile::TempDir::new().unwrap();
        a.lease.force_lost();
        crate::recovery::recover_deposed(&mut a.ship, &mut a.lease, Some(dir.path()))
            .await
            .unwrap();
        assert!(!a.lease.is_lost());
        assert_eq!(a.meta.kv_get("lease_lost").unwrap().as_deref(), Some("0"));
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        assert!(a.meta.lookup(1, "stranded").unwrap().is_none());
        assert_eq!(a.meta.pending_replays().unwrap().len(), 1);
        assert!(a.ship.log.list_segments().await.unwrap().is_empty());
    }

    /// Plan 30 §M4 item 1: a keeper that does not track the lease the
    /// bucket says is its own (a restart, or a takeover CAS whose reply was
    /// lost though it landed) re-adopts it through the takeover gate: an
    /// epoch marker at the same epoch, and a queued replay executed before
    /// the view opens. A keeper that does track it re-adopts as before.
    #[tokio::test]
    async fn a_restart_readopts_its_own_lease_through_the_gate() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.meta.mkdir(1, "shipped", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert_eq!(a.ship.spool.lock().unwrap().epoch_markers, 0);
        // Something the previous incarnation left queued for replay.
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 7,
        };
        let op = constellation_meta::MutateOp::Mkdir {
            parent: 1,
            name: "queued".into(),
            ino: (1 << 40) | 600,
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        a.meta.queue_replay(rid, &op).unwrap();

        // "Restart": a fresh keeper for the same node over the same bucket.
        let mut restarted = LeaseKeeper::new(
            LeaseStore::new(
                store.clone() as StdArc<dyn ObjectStore>,
                constellation_store_s3::log::PARTITION,
                LeaseMode::Cas,
            ),
            1,
        );
        assert!(acquire_lease(&mut a.ship, &mut restarted).await.unwrap());
        assert_eq!(restarted.ship_epoch(), Some(1), "same holder, same epoch");
        assert_eq!(a.ship.spool.lock().unwrap().epoch_markers, 1);
        let marker = segment(&store, 2).await;
        assert_eq!((marker.node, marker.epoch), (1, 1));
        assert!(marker.records.is_empty());
        assert!(
            a.meta.lookup(1, "queued").unwrap().is_some(),
            "gate replayed it"
        );
        assert!(a.meta.pending_replays().unwrap().is_empty());

        // The keeper that tracks its lease re-adopts without a gate.
        assert!(acquire_lease(&mut a.ship, &mut restarted).await.unwrap());
        assert_eq!(a.ship.spool.lock().unwrap().epoch_markers, 1);
    }

    /// Plan 30 §M3b, end to end on two replicas: a holder with captured,
    /// unshipped work is deposed by a takeover after its lease expired.
    /// The new holder's first segment is an empty epoch marker; tailing it
    /// strands the old holder's transaction (rolled back, queued by rid);
    /// replayed through the new holder, it lands exactly once.
    #[tokio::test]
    async fn a_deposed_holder_rolls_back_and_replays_through_the_new_holder() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        a.lease.share_holder_epoch(a.meta.holder_epoch_cell());
        b.lease.share_holder_epoch(b.meta.holder_epoch_cell());

        a.meta.mkdir(1, "shipped", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert_eq!(a.meta.holder_epoch(), 1);
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        let op = constellation_meta::MutateOp::Mkdir {
            parent: 1,
            name: "stranded".into(),
            ino: (1 << 40) | 500,
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        constellation_meta::execute_mutate(&a.meta, &op, Some(rid)).unwrap();
        assert_eq!(a.meta.speculation_counts().unwrap().local, 1);

        // A stops renewing; B takes over after expiry.
        expire_lease(&store).await;
        b.sync().await;
        assert_eq!(b.meta.holder_epoch(), 2);
        let marker = segment(&store, 2).await;
        assert_eq!((marker.node, marker.epoch), (2, 2));
        assert!(marker.records.is_empty(), "the marker is empty");
        assert_eq!(b.ship.spool.lock().unwrap().epoch_markers, 1);

        // A resumes and tails: the marker strands its transaction.
        a.ship.tail_to_head().await.unwrap();
        assert!(a.meta.lookup(1, "stranded").unwrap().is_none());
        assert!(a.meta.lookup(1, "shipped").unwrap().is_some());
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        assert_eq!(a.meta.completed_position(rid).unwrap(), None);
        let queued = a.meta.pending_replays().unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!((queued[0].rid, &queued[0].op), (rid, &op));
        assert_eq!(a.ship.spool.lock().unwrap().local_rolled_back, 1);

        // A's renewal now finds B: the deposition recovery has nothing left
        // to roll back and clears the loss.
        a.lease.renew_now().await.unwrap();
        assert!(a.lease.is_lost());
        crate::recovery::recover_deposed(&mut a.ship, &mut a.lease, None)
            .await
            .unwrap();
        assert!(!a.lease.is_lost());

        // The replay reaches B (as the drain's forward would) — twice, as
        // a retry would; B executes it once.
        for _ in 0..2 {
            if b.meta.completed_position(rid).unwrap().is_none() {
                constellation_meta::execute_mutate(&b.meta, &queued[0].op, Some(rid)).unwrap();
            }
        }
        b.sync().await;
        a.ship.tail_to_head().await.unwrap();
        for replica in [&a.meta, &b.meta] {
            assert_eq!(
                names(replica, 1)
                    .iter()
                    .filter(|(n, _)| n == "stranded")
                    .count(),
                1
            );
        }
        assert_eq!(names(&a.meta, 1), names(&b.meta, 1));
    }

    /// Plan 30 §M3b: a third node's shadow accepted by a holder that died
    /// strands as soon as the next holder's epoch marker arrives, even
    /// though the takeover's own op ships nothing.
    #[tokio::test]
    async fn a_takeover_marker_strands_a_third_nodes_shadow_at_once() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let mut c = node(&store, 3);
        a.sync().await; // A holds epoch 1.
        let rid = constellation_meta::Rid {
            node: 3,
            incarnation: 1,
            seq: 1,
        };
        let op = constellation_meta::MutateOp::Mkdir {
            parent: 1,
            name: "phantom".into(),
            ino: (3 << 40) | 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        let records = constellation_meta::execute_mutate(&a.meta, &op, Some(rid)).unwrap();
        crate::forward::apply_accepted(&c.meta, 1, rid, &op, &records).unwrap();
        assert!(c.meta.has_outstanding_speculation());

        // A dies before shipping; B takes over and ships nothing but its
        // marker (its own op, say, was refused).
        expire_lease(&store).await;
        assert!(acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        c.ship.tail_to_head().await.unwrap();
        assert!(
            !c.meta.has_outstanding_speculation(),
            "the marker alone strands the dead holder's shadow"
        );
        assert!(c.meta.lookup(1, "phantom").unwrap().is_none());
        assert_eq!(c.meta.pending_replays().unwrap().len(), 1);
    }

    /// Plan 30 §M3b's publish rule, with the plan's test helper: a holder
    /// with unshipped (captured) work publishes, and the commit's root is
    /// exactly the root of a replica rebuilt by replaying the log up to the
    /// commit's `applied` position — not the holder's live replica.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_holder_publishes_the_log_prefix_while_its_journal_is_non_empty() {
        use constellation_store_s3::{CommitChain, SHARD0};

        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.lease.share_holder_epoch(a.meta.holder_epoch_cell());
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let _nodes = enable_publisher(&mut a, &store, 1);

        let d = a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        a.meta.create(d.ino, "shipped", 0o644, 0, 0).unwrap();
        a.sync().await;
        // Unshipped, captured work on top of the shipped prefix.
        a.meta.create(d.ino, "unshipped", 0o644, 0, 0).unwrap();
        a.meta.rename(d.ino, "shipped", 1, "moved").unwrap();
        assert!(a.meta.journal_len().unwrap() > 0);
        assert_eq!(a.meta.speculation_counts().unwrap().local, 2);
        a.ship.publish().await.unwrap();

        let chain = CommitChain::new(backend.clone());
        let head = chain.discover_head(0).await.unwrap().expect("a commit");
        let commit = chain.get(head).await.unwrap().unwrap();
        assert_eq!(commit.applied, a.meta.applied_seq().unwrap());
        let published = commit.root(SHARD0).unwrap();
        let want = log_prefix_root(&store, commit.applied).await;
        assert_eq!(published, want, "the commit is the log prefix at `applied`");
        assert!(
            a.meta.has_dirty(),
            "the substituted keys stay dirty for the next publish"
        );

        // Once the work ships, the next publish carries it.
        a.sync().await;
        a.ship.publish().await.unwrap();
        let head = chain.discover_head(0).await.unwrap().unwrap();
        let commit = chain.get(head).await.unwrap().unwrap();
        let want = log_prefix_root(&store, commit.applied).await;
        assert_eq!(commit.root(SHARD0).unwrap(), want);
    }

    /// Plan 30 §M3's test helper: the root of a fresh replica built by
    /// replaying the shared log up to `applied` — what a commit claiming
    /// that position must equal.
    async fn log_prefix_root(
        store: &StdArc<InMemory>,
        applied: u64,
    ) -> constellation_mtree::NodeHash {
        use constellation_fs_core::cache::DiskCache;
        use constellation_mtree::{record, Hasher, Tree};
        use constellation_store_s3::{BlobStore, NodeCache, PackStore};
        let log = LogStore::new(store.clone());
        let replica = Meta::open_in_memory().unwrap();
        for seq in 1..=applied {
            let seg = decode(&log.get_segment(seq).await.unwrap()).unwrap();
            replica.apply_records(&seg.records).unwrap();
        }
        let dir = tempfile::TempDir::new().unwrap();
        let backend = StdArc::new(InMemory::new()) as StdArc<dyn ObjectStore>;
        let cache = Arc::new(NodeCache::new(
            PackStore::new(backend.clone()),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        let tree = Tree::with_config(cache, record::config()).unwrap();
        crate::mtree_publish::rebuild_root(&replica, &tree, &BlobStore::new(backend, Hasher::Plain))
            .unwrap()
    }

    /// A late segment stamped with a superseded epoch is fenced out
    /// rather than applied over the current holder's state.
    ///
    /// Since plan 26 Step 3 the holder does not poll the stream it holds,
    /// so a deposed predecessor's late write is not found by a tail: it
    /// surfaces when the holder's own next CAS lands on the sequence that
    /// write took. That is the only path a live cluster has — the fence
    /// still has to reject it, and the holder still has to ship *past* it
    /// rather than over it.
    #[tokio::test]
    async fn lower_epoch_segment_is_fenced() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        expire_lease(&store).await;
        b.sync().await; // epoch 2
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;

        // Forge a deposed holder's flush at the next free sequence.
        let stranded = vec![LogRecord::Mkdir {
            parent: 1,
            name: "zombie".into(),
            ino: 1 << 40 | 99,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 0,
        }];
        let seq = b.ship.next_seq();
        b.ship
            .log
            .put_segment(seq, &encode(1, 1, &stranded).unwrap())
            .await
            .unwrap();

        // B keeps writing; its CAS collides with the zombie's sequence.
        b.meta.mkdir(1, "after", 0o755, 0, 0).unwrap();
        b.sync().await;

        assert!(
            b.meta.lookup(1, "zombie").unwrap().is_none(),
            "a superseded epoch must not mutate the namespace"
        );
        assert_eq!(b.ship.spool.lock().unwrap().fenced, 1);
        assert_eq!(
            b.meta.applied_seq().unwrap(),
            seq + 1,
            "B must ship past the fenced sequence, not over it"
        );
        assert!(
            b.meta.lookup(1, "after").unwrap().is_some(),
            "the collision must not cost B the write that hit it"
        );
    }

    /// Rewrite the lease object so it is already expired, simulating a
    /// holder that stopped renewing.
    async fn expire_lease(store: &StdArc<InMemory>) {
        use constellation_store_s3::lease::Lease;
        let ls = LeaseStore::new(
            store.clone(),
            constellation_store_s3::log::PARTITION,
            LeaseMode::Cas,
        );
        let (cur, tag) = ls.get().await.unwrap().unwrap();
        let expired = Lease {
            expires_unix_ms: 1,
            ..cur
        };
        ls.try_swap(&expired, &tag).await.unwrap();
    }

    /// A genesis bootstrap (no commit, no prior state) never inherits a
    /// `pending_upload` obligation: nothing but a local write populates
    /// that table, and replay never does (plan 25 step 2).
    #[tokio::test]
    async fn bootstrap_from_genesis_has_no_pending_upload() {
        let dir = tempfile::tempdir().unwrap();
        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let log = LogStore::new(store);

        let boot = dir.path().join("boot.db");
        bootstrap(&boot, &log).await.unwrap();
        let restored = Meta::open(&boot).unwrap();
        assert_eq!(restored.pending_upload_count().unwrap(), 0);
    }

    /// An `InMemory` that records the prefix of every listing and the key
    /// of every GET. LIST is the request class plan 26 is trying to keep
    /// off the steady-state path (12.5x the price of a GET on AWS, and
    /// ~180 ms from Europe whether it returns anything or not), so the
    /// assertions below count calls rather than infer them from behaviour.
    #[derive(Debug, Default)]
    struct CountingStore {
        inner: InMemory,
        listed: std::sync::Mutex<Vec<String>>,
        got: std::sync::Mutex<Vec<String>>,
        put: std::sync::Mutex<Vec<String>>,
    }

    impl CountingStore {
        /// How many listings were issued under `prefix`.
        fn lists_of(&self, prefix: &str) -> usize {
            self.listed
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.starts_with(prefix))
                .count()
        }

        /// How many object GETs were issued under `prefix`.
        fn gets_of(&self, prefix: &str) -> usize {
            self.got
                .lock()
                .unwrap()
                .iter()
                .filter(|p| p.starts_with(prefix))
                .count()
        }

        /// How many PUTs were issued to `key`.
        fn puts_of(&self, key: &str) -> usize {
            self.put
                .lock()
                .unwrap()
                .iter()
                .filter(|k| *k == key)
                .count()
        }

        fn note(&self, prefix: Option<&OPath>) {
            self.listed
                .lock()
                .unwrap()
                .push(prefix.map(|p| p.to_string()).unwrap_or_default());
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &OPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.put.lock().unwrap().push(location.to_string());
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &OPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &OPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.got.lock().unwrap().push(location.to_string());
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<OPath>>,
        ) -> BoxStream<'static, object_store::Result<OPath>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&OPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.note(prefix);
            self.inner.list(prefix)
        }
        /// Overridden rather than inherited: the blanket implementation
        /// routes through `list`, which would still be counted, but the
        /// tailer calls this one and the count must name what it called.
        fn list_with_offset(
            &self,
            prefix: Option<&OPath>,
            offset: &OPath,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.note(prefix);
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&OPath>,
        ) -> object_store::Result<ListResult> {
            self.note(prefix);
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &OPath,
            to: &OPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// While this node holds p0's lease it is the only node allowed to
    /// append there, so the per-round LIST of `log/p0` can only ever
    /// return segments it wrote itself. Measured HU->AWS, dropping it
    /// takes a writer from 2.6 to 5.1 segments/s; the ship loop must be
    /// PUT-only (plan 26 Step 3).
    #[tokio::test]
    async fn holder_ships_without_listing_its_own_stream() {
        let counting = StdArc::new(CountingStore::default());
        let mut a = node_on(counting.clone() as StdArc<dyn ObjectStore>, 1);

        assert!(acquire_lease(&mut a.ship, &mut a.lease).await.unwrap());
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        // Everything before this point (acquisition, and the tail it does
        // when the claim is a takeover) is allowed to list.
        let base = counting.lists_of("log/p0");

        for i in 0..10 {
            a.meta.create(1, &format!("f{i}"), 0o644, 0, 0).unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }

        assert_eq!(
            counting.lists_of("log/p0") - base,
            0,
            "a lease holder listed the stream only it may append to: {:?}",
            counting.listed.lock().unwrap()
        );
        assert_eq!(a.meta.journal_len().unwrap(), 0, "every round must ship");
        assert_eq!(a.ship.last_shipped_seq(PARTITION), Some(10));
    }

    /// The one case the skipped self-tail would strand: a holder that
    /// crashed between the segment PUT and the journal ack comes back,
    /// reacquires its own lease, and aims at a sequence its own segment
    /// already occupies. Nothing tails p0 for it any more, so the CAS
    /// collision itself has to absorb that segment — otherwise the ship
    /// loop retries the same sequence forever.
    #[tokio::test]
    async fn restarted_holder_recovers_own_unacked_segment_without_tailing_all() {
        let counting = StdArc::new(CountingStore::default());
        let store = counting.clone() as StdArc<dyn ObjectStore>;
        let a = node_on(store.clone(), 1);
        a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        let records: Vec<LogRecord> = a
            .meta
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        // The PUT lands, the ack never does: the journal still holds the
        // records and `next_seq` never advanced.
        a.ship
            .log
            .put_segment(1, &encode(1, 1, &records).unwrap())
            .await
            .unwrap();

        // Restart: a fresh shipper over the same replica, holding p0.
        let mut ship = Shipper::attach(a.meta.clone(), LogStore::new(store.clone()), 1).unwrap();
        let mut keeper = LeaseKeeper::new(LeaseStore::new(store, PARTITION, LeaseMode::Cas), 1);
        assert!(acquire_lease(&mut ship, &mut keeper).await.unwrap());
        assert!(keeper.ship_epoch().is_some(), "the restart holds p0");
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), keeper);

        let (lists_before, gets_before) = (counting.lists_of("log/p0"), counting.gets_of("log/p0"));
        ship.sync_all(&mut leases).await.unwrap();
        assert_eq!(a.meta.journal_len().unwrap(), 0, "journal acked");
        assert_eq!(ship.next_seq(), 2, "next_seq must advance past our own");
        assert_eq!(names(&a.meta, 1).len(), 1, "the op must not apply twice");
        // Step 4b moved the tail from LIST to a GET-next probe, so the
        // evidence that the forced tail ran is a GET of the stream, not a
        // listing of it. The listing must now stay at zero throughout —
        // this run is short enough that the probe alone reaches the
        // recovered segment.
        assert!(
            counting.gets_of("log/p0") > gets_before,
            "recovery must come from the forced tail after AlreadyExists"
        );
        assert_eq!(
            counting.lists_of("log/p0"),
            lists_before,
            "the probe covers a one-segment recovery; no LIST is needed"
        );

        // And that tail is one-shot: an ordinary round after it is back to
        // PUT-only (here, with nothing to ship, to no requests at all).
        let settled = (counting.lists_of("log/p0"), counting.gets_of("log/p0"));
        ship.sync_all(&mut leases).await.unwrap();
        assert_eq!(
            (counting.lists_of("log/p0"), counting.gets_of("log/p0")),
            settled,
            "the recovery tail must not become the steady state"
        );
    }

    /// A follower's steady-state poll must not touch LIST at all: it is
    /// the speculative GET-next probe, whose misses cost a GET each
    /// (~1/12.5 of a LIST on AWS, and no slower — 177 vs 175 ms from
    /// Europe). LIST stays for catch-up, where one page beats k GETs, and
    /// this pins both halves: the 20-segment backlog below is fetched with
    /// a listing, the ten idle rounds after it with probes only.
    #[tokio::test]
    async fn tailer_uses_get_probes_not_list_in_steady_state() {
        let counting = StdArc::new(CountingStore::default());
        let store = counting.clone() as StdArc<dyn ObjectStore>;
        let mut a = node_on(store.clone(), 1);
        let mut b = node_on(store.clone(), 2);

        assert!(acquire_lease(&mut a.ship, &mut a.lease).await.unwrap());
        let mut a_leases = HashMap::new();
        a_leases.insert(PARTITION.to_string(), a.lease);
        // Deeper than one probe (k = 16) so the follower starts out in
        // catch-up mode rather than steady state.
        for i in 0..20 {
            a.meta.create(1, &format!("f{i}"), 0o644, 0, 0).unwrap();
            a.ship.sync_all(&mut a_leases).await.unwrap();
        }

        let mut b_leases: HashMap<String, LeaseKeeper> = HashMap::new();
        let (lists, gets) = (counting.lists_of("log/p0"), counting.gets_of("log/p0"));
        b.ship.sync_all(&mut b_leases).await.unwrap();
        assert_eq!(names(&b.meta, 1).len(), 20, "the follower caught up");
        assert!(
            counting.lists_of("log/p0") > lists,
            "a follower 20 segments behind must still use a LIST page: \
             1000 keys in one round trip beats k GETs at that depth"
        );
        assert!(counting.gets_of("log/p0") > gets);

        // Steady state: nothing is being written anywhere.
        let (lists, gets) = (counting.lists_of("log/p0"), counting.gets_of("log/p0"));
        for _ in 0..10 {
            b.ship.sync_all(&mut b_leases).await.unwrap();
        }
        assert_eq!(
            counting.lists_of("log/p0") - lists,
            0,
            "an idle follower must not list the log: {:?}",
            counting.listed.lock().unwrap()
        );
        // One first-miss GET per round. The probe asks for k sequences at
        // once, but `buffered` polls them in order and this store answers
        // the head synchronously, so its siblings are never polled and
        // never reach the backend. Against a real S3 the head returns
        // pending and all k requests do go out — k 404s of a class costing
        // ~1/12.5 of the LIST they replace, and that is what the sync
        // task's idle backoff (30 s ceiling vs a 500 ms poll) pays for.
        assert_eq!(
            counting.gets_of("log/p0") - gets,
            10,
            "each idle round must cost exactly one probe head (seq {}), \
             and nothing else",
            b.ship.last_shipped_seq(PARTITION).unwrap_or(0) + 1
        );
    }

    /// `SEGMENT_BATCH` bounds a segment by record count, which says
    /// nothing about its size. Three multi-MiB records must not become one
    /// oversized PUT: the batch is cut to the largest prefix under
    /// `SEGMENT_MAX_BYTES`, the rest stays journaled, and successive
    /// rounds drain it with no record lost or shipped twice.
    #[tokio::test]
    async fn oversized_batch_is_split_at_the_byte_cap() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        let f = a.meta.create(1, "big", 0o644, 0, 0).unwrap();
        a.sync().await; // the create ships as seq 1, journal empty again

        // ~3 MiB each: two fit in no segment, one does.
        let blob = vec![b'x'; 3 << 20];
        for i in 0..3 {
            a.meta
                .set_xattr(
                    f.ino,
                    &format!("user.blob{i}"),
                    &blob,
                    constellation_meta::SetXattrMode::Set,
                )
                .unwrap();
        }
        assert_eq!(a.meta.journal_len().unwrap(), 3);

        let log = LogStore::for_partition(store.clone(), PARTITION);
        for round in 0..3u64 {
            assert!(
                a.ship.ship_part_taking(PARTITION, &a.lease).await.unwrap(),
                "round {round} must ship"
            );
            assert_eq!(
                a.meta.journal_len().unwrap(),
                2 - round,
                "round {round} ships exactly one record and leaves the rest"
            );
            let payload = log.get_segment(2 + round).await.unwrap();
            assert!(
                payload.len() <= SEGMENT_MAX_BYTES,
                "segment {} is {} bytes, over the {SEGMENT_MAX_BYTES} cap",
                2 + round,
                payload.len()
            );
            assert_eq!(decode(&payload).unwrap().records.len(), 1);
        }
        assert!(
            !a.ship.ship_part_taking(PARTITION, &a.lease).await.unwrap(),
            "the journal is drained"
        );

        a.release().await;
        b.sync().await;
        for i in 0..3 {
            assert_eq!(
                b.meta
                    .get_xattr(f.ino, &format!("user.blob{i}"))
                    .unwrap()
                    .as_deref(),
                Some(&blob[..]),
                "the peer converges across the split batch"
            );
        }
    }

    /// A keeper that is deposed mid-flight must start reading its stream
    /// again — its lease is what earned it the right to skip the read.
    ///
    /// The load-bearing half is the part with **no renewal in it**.
    /// `ship_epoch()` does return `None` once the keeper is lost, but the
    /// keeper only learns it is lost at its renewal CAS, half a TTL away;
    /// nothing in production calls `renew_now` on demand the way a test
    /// can. So the staleness bound, not the renewal, is what has to bring
    /// the stream back, and that is what this asserts first. (An earlier
    /// version of this test drove `renew_now` immediately, which was true
    /// about correctness and silent about the 30 s window — the gap that
    /// regressed `two-clients-shared`, `lease-handover` and
    /// `rename-across-partitions` against a plan-26-free tree.)
    #[tokio::test]
    async fn a_deposed_keeper_leaves_the_held_set_and_tails_again() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;

        // A stalls; its lease expires and B takes over and writes.
        expire_lease(&store).await;
        b.sync().await;
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;

        // A has not noticed yet: its own view still says the lease is
        // usable, so p0 stays in the held set and stays unread.
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        assert!(leases[PARTITION].ship_epoch().is_some());
        a.ship.sync_all(&mut leases).await.unwrap();
        assert!(
            a.meta.lookup(1, "from-b").unwrap().is_none(),
            "a holder must not list the partition it believes it holds"
        );

        // Production timing: no renewal, just time passing. Once the
        // held-read clock is older than the bound the stream is read
        // again, even though the keeper still believes it holds the lease.
        assert!(
            leases[PARTITION].ship_epoch().is_some(),
            "the keeper must still believe it holds the lease, or this \
             would be testing the renewal path again"
        );
        a.ship.expire_held_tail_for_test(PARTITION);
        a.ship.sync_all(&mut leases).await.unwrap();
        assert!(
            a.meta.lookup(1, "from-b").unwrap().is_some(),
            "the staleness bound must bring a deposed holder's stream back \
             without waiting for its renewal"
        );

        // The renewal CAS is still where it learns it was deposed.
        leases
            .get_mut(PARTITION)
            .unwrap()
            .renew_now()
            .await
            .unwrap();
        assert!(leases[PARTITION].is_lost());
        assert_eq!(
            leases[PARTITION].ship_epoch(),
            None,
            "a lost keeper must not report a ship epoch, or it would keep \
             suppressing the tail of a stream it no longer owns"
        );

        a.ship.sync_all(&mut leases).await.unwrap();
        assert!(
            a.meta.lookup(1, "from-b").unwrap().is_some(),
            "the deposed node must tail the new holder's writes on its next round"
        );
    }

    /// The lease object as it currently stands in the bucket.
    async fn lease_object(store: &StdArc<InMemory>) -> Lease {
        lease_object_of(&LeaseStore::new(store.clone(), PARTITION, LeaseMode::Cas)).await
    }

    async fn lease_object_of(leases: &LeaseStore) -> Lease {
        leases.get().await.unwrap().unwrap().0
    }

    /// Leases are sticky: an idle holder keeps write authority until some
    /// peer actually asks for it. Releasing it to nobody costs the next
    /// local write three S3 round trips to take it back, and buys nothing
    /// — which is the whole of plan 26 Step 7.
    #[tokio::test]
    async fn active_holder_never_releases_idle_without_a_requester() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0, "nothing left to ship");

        // Both idle timers are elapsed and the journal is drained: every
        // condition the old unconditional release had is met.
        a.lease.expire_idle_timers_for_test();
        assert!(
            !a.lease.idle_release_due(0),
            "an idle holder must keep a lease nobody is waiting for"
        );

        // Renewing is where a request would be noticed; there is none.
        a.lease.renew_now().await.unwrap();
        assert!(!a.lease.is_lost());
        assert!(a.lease.wanted_by().is_empty());
        assert!(!a.lease.idle_release_due(0));
        let cur = lease_object(&store).await;
        assert_eq!((cur.holder, cur.epoch, cur.released), (1, 1, false));

        // Non-vacuity: the requester list is the *only* difference. One
        // peer signs it, A's next renew picks it up, and the same call
        // with the same timers now says release.
        let leases = LeaseStore::new(store.clone(), PARTITION, LeaseMode::Cas);
        let (live, tag) = leases.get().await.unwrap().unwrap();
        leases.try_swap(&live.wanting(2), &tag).await.unwrap();
        a.lease.renew_now().await.unwrap();
        a.lease.expire_idle_timers_for_test();
        assert_eq!(a.lease.wanted_by(), &[2]);
        assert!(a.lease.idle_release_due(0));
    }

    /// A holder that never goes write-idle must still answer a waiting
    /// requester. `chaos-ci` is the case: three mounts creating files at
    /// once, so `idle_for_ms` never reaches `idle_release_ms`, handoffs
    /// happened only on the 30 s idle timer, and the third node sat behind
    /// two full tenures until the FUSE acquire deadline (2xTTL) expired and
    /// the write returned EIO — measured at 121.27 s.
    ///
    /// The grace arm is what breaks that: once somebody has been waiting
    /// longer than `LEASE_WANTED_GRACE_MS`, the next drained batch hands the
    /// lease over regardless of how busy this node still is.
    #[tokio::test]
    async fn busy_holder_releases_to_a_requester_that_has_waited() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0, "nothing left to ship");

        // A peer signs the waiting list and A picks it up at its renewal.
        let leases = LeaseStore::new(store.clone(), PARTITION, LeaseMode::Cas);
        let (live, tag) = leases.get().await.unwrap().unwrap();
        leases.try_swap(&live.wanting(2), &tag).await.unwrap();
        a.lease.renew_now().await.unwrap();
        assert_eq!(a.lease.wanted_by(), &[2]);

        // A is *not* write-idle: it has just shipped, so `idle_for_ms` is
        // far below the 30 s threshold. Under the old condition this was
        // the deadlock -- a requester registered, a holder that never idles.
        assert!(
            !a.lease.idle_release_due(0),
            "dwell has not elapsed yet, so nothing is due"
        );

        // Age only the dwell and the requester's wait; the idle threshold
        // is untouched, so the write-idle arm stays false and the release
        // below can only come from the grace arm.
        a.lease.expire_wanted_grace_for_test();
        assert!(
            a.lease.idle_release_due(0),
            "a requester that has waited past the grace must be answered \
             even though this node is still writing"
        );

        // Still gated on the journal: an in-flight batch finishes first.
        assert!(
            !a.lease.idle_release_due(1),
            "a drained journal is still the precondition"
        );
    }

    /// The S3-only handoff, end to end and with P2P out of the picture: B
    /// cannot take a live lease, so it signs the waiting list; A learns of
    /// it through its own renewal CAS failing, releases once idle, and B
    /// claims at a bumped epoch that fences A's late writes.
    #[tokio::test]
    async fn requester_registers_wanted_by_and_gets_the_lease() {
        let counting = StdArc::new(CountingStore::default());
        let store = counting.clone() as StdArc<dyn ObjectStore>;
        let mut a = node_on(store.clone(), 1);
        let mut b = node_on(store.clone(), 2);
        let leases = LeaseStore::new(store.clone(), PARTITION, LeaseMode::Cas);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        let held = leases.get().await.unwrap().unwrap().0;
        assert_eq!((held.holder, held.epoch), (1, 1));

        // B wants the partition. It does not get it, and it does not move
        // it either: holder, epoch and expiry come back untouched.
        assert!(!acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        let busy = leases.get().await.unwrap().unwrap().0;
        assert_eq!(busy.wanted_by, vec![2], "B signed the waiting list");
        assert_eq!(
            (busy.holder, busy.epoch, busy.expires_unix_ms, busy.released),
            (held.holder, held.epoch, held.expires_unix_ms, held.released),
            "a requester may not move the lease"
        );

        // Retrying does not re-register: the holder only looks once per
        // half-TTL, so a blocked writer asking again is pure request cost.
        let puts = counting.puts_of("leases/p0.json");
        for _ in 0..5 {
            assert!(!acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        }
        assert_eq!(
            counting.puts_of("leases/p0.json"),
            puts,
            "five more Acquire retries must not cost five more CAS PUTs"
        );

        // A's renewal CAS fails against B's edit — and that is a handoff
        // request, not a deposition.
        a.lease.renew_now().await.unwrap();
        assert!(!a.lease.is_lost());
        assert_eq!(a.lease.wanted_by(), &[2]);
        assert_eq!(a.lease.ship_epoch(), Some(1), "A still holds p0");

        // With a requester registered, the idle release fires.
        a.lease.expire_idle_timers_for_test();
        assert!(a.lease.idle_release_due(0));
        a.release().await;
        let released = lease_object_of(&leases).await;
        assert!(released.released, "A handed it back");
        assert!(
            released.wanted_by.is_empty(),
            "the request has been answered; it must not bind the next holder"
        );

        // B takes over, applying A's flushed log first.
        b.meta.mkdir(1, "from-b", 0o755, 0, 0).unwrap();
        b.sync().await;
        assert_eq!(b.lease.ship_epoch(), Some(2), "a handover bumps the epoch");
        assert!(b.meta.lookup(1, "from-a").unwrap().is_some());

        // A's late flush, stamped with the epoch it no longer holds, is
        // fenced rather than applied.
        let stranded = vec![LogRecord::Mkdir {
            parent: 1,
            name: "zombie".into(),
            ino: 1 << 40 | 99,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 0,
        }];
        let seq = b.ship.next_seq();
        b.ship
            .log
            .put_segment(seq, &encode(1, 1, &stranded).unwrap())
            .await
            .unwrap();
        b.meta.mkdir(1, "after", 0o755, 0, 0).unwrap();
        b.sync().await;
        assert!(b.meta.lookup(1, "zombie").unwrap().is_none());
        assert_eq!(b.ship.spool.lock().unwrap().fenced, 1);
    }

    /// A renew CAS that fails is not by itself evidence of anything: since
    /// Step 7b a requester edits the lease object in place, which fails the
    /// holder's CAS exactly as a takeover would. Mistaking the two would
    /// make any node that is asked for a partition stop shipping forever
    /// (deposition is terminal), so the holder has to re-read and tell them
    /// apart.
    #[tokio::test]
    async fn wanted_by_edit_is_not_a_deposition() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);

        a.meta.mkdir(1, "from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert!(!acquire_lease(&mut b.ship, &mut b.lease).await.unwrap());
        let leases = LeaseStore::new(store.clone(), PARTITION, LeaseMode::Cas);
        let edited_tag = leases.get().await.unwrap().unwrap().1;

        a.lease.renew_now().await.unwrap();
        assert!(!a.lease.is_lost(), "a handoff request must not depose");
        assert_eq!(a.lease.ship_epoch(), Some(1), "same epoch, same holder");
        let (cur, tag) = leases.get().await.unwrap().unwrap();
        assert_eq!((cur.holder, cur.epoch, cur.released), (1, 1, false));
        assert_eq!(cur.wanted_by, vec![2], "the request survives the renewal");
        // Surviving the CAS failure is not enough: the renewal itself has
        // to land against the fresh tag, or the lease drifts toward expiry
        // and the handoff turns back into a takeover.
        assert_ne!(tag, edited_tag, "the retried renew must have swapped");
        // And A has to come away knowing who is waiting — that is the only
        // thing that will ever make it release.
        assert_eq!(a.lease.wanted_by(), &[2]);

        // And authority is intact: A keeps shipping until it chooses to
        // release, which is what "finishes any in-flight batch" means.
        a.meta.mkdir(1, "also-from-a", 0o755, 0, 0).unwrap();
        a.sync().await;
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        assert_eq!(a.ship.last_shipped_seq(PARTITION), Some(2));
    }

    /// Plan 28 §11's wiring, end to end through the shipper: shipped
    /// records reach the publisher, and `publish` commits the tree.
    /// Give `n` a plan 28 tree publisher over `store`. The returned
    /// directory holds its node cache and must outlive it.
    fn enable_publisher(n: &mut Node, store: &StdArc<InMemory>, id: u64) -> tempfile::TempDir {
        use constellation_fs_core::cache::DiskCache;
        use constellation_mtree::{record, Hasher};
        use constellation_store_s3::{BlobStore, CommitChain, NodeCache, PackStore};
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let dir = tempfile::TempDir::new().unwrap();
        let cache = Arc::new(NodeCache::new(
            PackStore::new(backend.clone()),
            Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()),
            Hasher::Plain,
            tokio::runtime::Handle::current(),
        ));
        n.ship
            .enable_tree_publish(crate::mtree_publish::TreePublisher::new(
                n.meta.clone(),
                cache,
                BlobStore::new(backend.clone(), Hasher::Plain),
                CommitChain::new(backend),
                record::config(),
                id,
                tokio::runtime::Handle::current(),
            ))
            .unwrap();
        dir
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_publish_commits_the_metadata_tree() {
        use constellation_store_s3::{CommitChain, SHARD0};

        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let backend = store.clone() as StdArc<dyn ObjectStore>;
        let _nodes = enable_publisher(&mut a, &store, 1);

        let d = a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        a.meta.create(d.ino, "f", 0o644, 0, 0).unwrap();
        a.sync().await;
        a.ship.publish().await.unwrap();

        let chain = CommitChain::new(backend);
        let head = chain.discover_head(0).await.unwrap().expect("a commit");
        let commit = chain.get(head).await.unwrap().unwrap();
        assert_eq!((commit.seq, commit.author), (1, 1));
        assert!(commit.root(SHARD0).is_some());
        assert!(commit.agg.keys >= 6, "{:?}", commit.agg);
        assert_eq!(commit.agg.files, 1, "only regular files count (§P7)");
        assert!(a.ship.tree_published().is_some());

        // A second round with nothing new publishes nothing: an empty
        // changed set is not a commit.
        a.ship.publish().await.unwrap();
        assert_eq!(chain.discover_head(0).await.unwrap(), Some(1));
    }

    /// Plan 28 S6: a fresh replica restores the chain head and tails the
    /// log from the commit's vector, and ends up equal — table by table
    /// — to the replica that published it (plan 27's honest check that
    /// builder and reader agree, kept). Every segment the commit covers
    /// is deleted first, so nothing but the commit chain can have
    /// produced the result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_fresh_replica_bootstraps_from_the_commit_chain() {
        use constellation_meta::{SetXattrMode, SnapshotRow};
        use object_store::ObjectStoreExt;

        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let _nodes = enable_publisher(&mut a, &store, 1);

        let d = a.meta.mkdir(1, "d", 0o755, 0, 0).unwrap();
        let f = a.meta.create(d.ino, "f", 0o644, 7, 8).unwrap();
        a.meta
            .set_manifest(f.ino, &vec![0xab; 4096], 1 << 20)
            .unwrap();
        a.meta
            .set_xattr(f.ino, "user.small", b"v", SetXattrMode::Set)
            .unwrap();
        // A set big enough to spill into 0x03 keys, and one value big
        // enough to spill into a blob.
        let big = a.meta.create(d.ino, "big", 0o600, 0, 0).unwrap();
        for i in 0..40 {
            a.meta
                .set_xattr(big.ino, &format!("user.k{i}"), b"vvvv", SetXattrMode::Set)
                .unwrap();
        }
        a.meta
            .set_xattr(big.ino, "user.huge", &vec![7u8; 40_000], SetXattrMode::Set)
            .unwrap();
        a.meta.symlink(d.ino, "s", &"t".repeat(3000), 0, 0).unwrap();
        a.meta.link(f.ino, 1, "hard").unwrap();
        a.meta.write_quota(Some(1 << 40)).unwrap();
        a.meta
            .record_snapshot(&SnapshotRow {
                id: "snap-1".into(),
                path: "/d".into(),
                name: "one".into(),
                root_hash: "ab".repeat(32),
                created_unix_ms: 42,
            })
            .unwrap();
        a.sync().await;
        a.ship.publish().await.unwrap();

        // Shipped after the commit: only the log tail carries these.
        a.meta.create(d.ino, "late", 0o644, 0, 0).unwrap();
        a.meta.unlink(d.ino, "s").unwrap();
        a.sync().await;

        // Every segment the commit covers, as retention would: with
        // them gone, only the commit can supply the state before its
        // vector (the log alone would otherwise rebuild it from seq 1).
        let chain = constellation_store_s3::CommitChain::new(store.clone());
        let head = chain
            .get(chain.discover_head(0).await.unwrap().unwrap())
            .await
            .unwrap()
            .unwrap();
        let covered = head.applied;
        assert!(covered >= 1);
        for seq in 1..=covered {
            store
                .delete(&constellation_store_s3::layout::log_segment(PARTITION, seq))
                .await
                .unwrap();
        }

        let dir = tempfile::TempDir::new().unwrap();
        let db = dir.path().join("fresh.db");
        bootstrap(&db, &LogStore::new(store.clone())).await.unwrap();
        let fresh = Meta::open(&db).unwrap();
        assert_replicas_equal(&fresh, &a.meta);
        assert_eq!(fresh.applied_seq().unwrap(), a.meta.applied_seq().unwrap());
        assert!(fresh.child_ino(d.ino, "late").unwrap().is_some());
        assert!(fresh
            .chunk_ref_exists(&constellation_fs_core::ChunkHash([0; 32]))
            .is_ok());
    }

    /// A bootstrap base older than the log's retention floor must fail
    /// loudly: replaying from the first segment present would hand back
    /// a replica silently missing everything that was pruned.
    #[tokio::test]
    async fn replay_refuses_a_base_the_log_was_pruned_past() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        for i in 0..4 {
            a.meta.mkdir(1, &format!("d{i}"), 0o755, 0, 0).unwrap();
            a.sync().await;
        }
        let log = LogStore::new(store.clone());
        use object_store::ObjectStoreExt;
        store
            .delete(&constellation_store_s3::layout::log_segment(PARTITION, 1))
            .await
            .unwrap();
        let fresh = Meta::open_in_memory().unwrap();
        let err = replay_from(&fresh, &log, PARTITION, 0).await.unwrap_err();
        assert!(err.to_string().contains("pruned"), "{err:#}");
        // From a base the log still covers, the same replay succeeds.
        let covered = Meta::open_in_memory().unwrap();
        assert!(replay_from(&covered, &log, PARTITION, 1).await.unwrap() > 0);
    }

    /// Table-by-table equality of two replicas' shared state, reporting
    /// only the rows that differ (a full dump buries them in manifests).
    fn assert_replicas_equal(got: &Meta, want: &Meta) {
        let got = got.dump_replicated().unwrap();
        let want = want.dump_replicated().unwrap();
        let short = |row: &String| row.chars().take(240).collect::<String>();
        let missing: Vec<String> = want
            .iter()
            .filter(|r| !got.contains(r))
            .map(short)
            .collect();
        let extra: Vec<String> = got
            .iter()
            .filter(|r| !want.contains(r))
            .map(short)
            .collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "replicas differ\nmissing: {missing:#?}\nextra: {extra:#?}"
        );
    }
}
