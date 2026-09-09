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
//! The leaseless convergence path (foreign records that touch pending
//! local state are skipped via [`TouchSet`], ours being later in the
//! global log) is retained as a safety net. With leases it is
//! unreachable in normal operation — the harness asserts the conflict
//! counter stays at zero — but a backend without `If-Match`, or a
//! future relaxed mode, still needs deterministic convergence.

use crate::lease::{LeaseKeeper, TailedToHead};
use anyhow::{bail, Context, Result};
use constellation_meta::replay::TouchSet;
use constellation_meta::{LogRecord, MetaStore, SqliteMeta};
use constellation_store_s3::log::{CheckpointVector, PARTITION};
use constellation_store_s3::{LeaseMode, LeaseStore, LogStore};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Checkpoint after this many shipped segments (across all partitions).
const CHECKPOINT_EVERY: u64 = 32;
/// Floor on the gap between checkpoints; disabled by default.
///
/// A checkpoint copies the whole metadata DB, so it costs roughly 2ms per
/// MiB and grows with the namespace: the segment counter alone fires it
/// every ~100ms under a small-file rsync, and at 50k files that is ~180
/// checkpoints and 120MB written where 1 checkpoint and 38MB would do.
///
/// Spacing them out is nonetheless **not** a free win, which is why the
/// default is 0. Measured on a 50k-file mount, a 60s floor cut write
/// amplification 3x and lifted 8-writer throughput ~8%, but made
/// single-threaded small-file work 1.5x slower (465-501us per file to
/// 764-786us). Frequent checkpointing was accidentally throttling the
/// shipper; without it the shipper's other background work contends for
/// the metadata connection that FUSE handlers also need. Until that
/// contention is addressed, a floor trades latency for I/O rather than
/// buying both, so it is opt-in.
const CHECKPOINT_MIN_INTERVAL_S: u64 = 0;

fn checkpoint_min_interval() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("CONSTELLATION_CHECKPOINT_MIN_INTERVAL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(CHECKPOINT_MIN_INTERVAL_S),
    )
}

/// The segment count is the trigger; `min_interval` only ever delays it.
/// A zero interval (the default) leaves the count in sole charge.
fn checkpoint_is_due(
    shipped_since_ckpt: u64,
    since_last_ckpt: Option<std::time::Duration>,
    min_interval: std::time::Duration,
) -> bool {
    if shipped_since_ckpt < CHECKPOINT_EVERY {
        return false;
    }
    match since_last_ckpt {
        None => true,
        Some(elapsed) => elapsed >= min_interval,
    }
}
/// Max journal records per segment.
const SEGMENT_BATCH: usize = 10_000;

/// Concurrent segment-payload GETs while tailing one partition. Each
/// GET is a full S3 round trip; a reader far from the bucket that
/// fetched a burst's segments one at a time could not keep up with a
/// writer sitting next to it.
const TAIL_GET_CONCURRENCY: usize = 8;

pub fn part_split_ops() -> u64 {
    std::env::var("CONSTELLATION_PART_SPLIT_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(512)
}

pub fn part_merge_idle_s() -> u64 {
    std::env::var("CONSTELLATION_PART_MERGE_IDLE_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3600)
}

/// Env: `CONSTELLATION_PART_AUTOSPLIT=on` re-enables automatic
/// splitting of hot directories into their own partitions. **Off by
/// default.**
///
/// The heuristic was written when the partition lease was the only way
/// to write: concurrent writers on different subtrees had to pass one
/// lease back and forth, and carving a hot subtree out gave each its
/// own. Forwarded mutations (ADR-14) removed that motivation — a
/// non-holder now asks the holder to journal its op in ~1 RTT and the
/// lease stays put — so directory heat no longer implies contention.
///
/// What it does still imply is cost. Every partition is another stream
/// to LIST each sync round, another lease to CAS and renew, and a merge
/// that only reclaims it after [`part_merge_idle_s`]. A single-writer
/// bulk ingest (rsync of a source tree, image unpack) makes every
/// directory hot in turn and can carve out a partition per directory
/// for no benefit at all.
///
/// Splitting is still the only way to scale metadata *append* across
/// holders, and the only way two regions get a lease each, so the
/// machinery stays. It just should not be driven by heat: a future
/// automatic trigger belongs on holder-side evidence (sustained
/// forwards from several distinct nodes, holder journal backlog).
pub fn part_autosplit() -> bool {
    matches!(
        std::env::var("CONSTELLATION_PART_AUTOSPLIT")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "on" | "1" | "true"
    )
}

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
    meta: Arc<SqliteMeta>,
    log: LogStore,
    node_id: u64,
    lease_mode: LeaseMode,
    /// Per-partition stream state (next_seq, max_epoch, log handle).
    parts: HashMap<String, PartState>,
    shipped_since_ckpt: u64,
    /// When the last checkpoint finished; `None` until the first one.
    /// Enforces `checkpoint_min_interval` against the segment counter.
    last_ckpt_at: Option<Instant>,
    /// Continuation epoch: journal locally, do not CAS-create segments.
    skip_ship: Arc<std::sync::atomic::AtomicBool>,
    /// Live spool observability shared with the control API.
    pub spool: Arc<std::sync::Mutex<SpoolInfo>>,
    /// Per-directory write-op counts used by the split heuristic.
    /// Keyed by the directory inode under a partition root.
    dir_ops: HashMap<u64, DirTraffic>,
    /// Whether the heat-based split trigger is armed at all (see
    /// [`part_autosplit`]). Resolved once at attach rather than read per
    /// ship, so tests can drive it without racing on a process-global
    /// environment variable.
    autosplit: bool,
    last_ship_at: HashMap<String, Instant>,
    /// P2P handle for push invalidation. Disabled by default so the
    /// existing tests and the no-P2P path need no changes.
    peers: constellation_net::Peers,
    /// Offline designation (phase 4a, DESIGN.md §5.2): when a foreign
    /// node ships records touching a designated path, the designee's
    /// ack is awaited (bounded) before the batch is treated as fully
    /// published. `None` when no designations exist for this mount.
    designations: Option<std::sync::Arc<crate::designation::DesignationManager>>,
}

struct PartState {
    log: LogStore,
    next_seq: u64,
    max_epoch: u64,
}

/// Write-op traffic for one directory, accumulated over an unbroken run
/// of shipped segments (DESIGN.md §4 / plan 01: "sustains more than
/// `CONSTELLATION_PART_SPLIT_OPS` records across at least two
/// consecutive shipped segments").
///
/// Two properties matter and both are load-bearing:
///
/// * Counting per *segment* would make the policy a function of segment
///   batching — and therefore of the sync interval — rather than of real
///   traffic: a busy directory written one file at a time ships many
///   2-record segments and would never reach the threshold. So records
///   accumulate across segments.
/// * The run must be **consecutive**. A lifetime total would eventually
///   split every directory that is merely long-lived, since any write
///   ever seen would still count. A directory that goes quiet — even
///   briefly, as any bursty workload does — starts over.
#[derive(Default)]
struct DirTraffic {
    /// Records seen for this directory during the current unbroken run.
    ops: u64,
    /// How many consecutive shipped segments the run spans.
    segments: u64,
    /// True once the run is both wide enough (≥2 segments, so a single
    /// burst never carves a partition) and heavy enough.
    armed: bool,
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
}

impl Shipper {
    /// Attach to an existing local replica. `applied_seq` is the log
    /// position the replica covers (from the local kv store); segments
    /// beyond it are tailed on the first `sync`.
    #[allow(dead_code)]
    pub fn attach(meta: Arc<SqliteMeta>, log: LogStore, node_id: u64) -> Result<Self> {
        Self::attach_with_mode(meta, log, node_id, LeaseMode::Cas)
    }

    pub fn attach_with_mode(
        meta: Arc<SqliteMeta>,
        log: LogStore,
        node_id: u64,
        lease_mode: LeaseMode,
    ) -> Result<Self> {
        let mut parts = HashMap::new();
        let mut head = 0u64;
        for (id, _) in meta.partitions()? {
            let applied = meta.applied_seq_of(&id)?;
            head = head.max(applied);
            parts.insert(
                id.clone(),
                PartState {
                    log: log.with_partition(&id),
                    next_seq: applied + 1,
                    max_epoch: 0,
                },
            );
        }
        if parts.is_empty() {
            let applied = meta.applied_seq()?;
            head = applied;
            parts.insert(
                PARTITION.into(),
                PartState {
                    log: log.with_partition(PARTITION),
                    next_seq: applied + 1,
                    max_epoch: 0,
                },
            );
        }
        Ok(Self {
            meta,
            log,
            node_id,
            lease_mode,
            parts,
            shipped_since_ckpt: 0,
            last_ckpt_at: None,
            skip_ship: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            spool: Arc::new(std::sync::Mutex::new(SpoolInfo {
                head_seq: head,
                ..Default::default()
            })),
            dir_ops: HashMap::new(),
            autosplit: part_autosplit(),
            last_ship_at: HashMap::new(),
            peers: constellation_net::Peers::disabled(),
            designations: None,
        })
    }

    /// Attach the P2P handle so shipped segments are announced to peers.
    pub fn set_peers(&mut self, peers: constellation_net::Peers) {
        self.peers = peers;
    }

    /// Arm or disarm the heat-based split trigger, bypassing
    /// [`part_autosplit`]'s environment lookup. Tests that exercise
    /// splitting run in one process alongside tests that must not
    /// split, so the switch cannot be process-global.
    #[cfg(test)]
    pub fn set_autosplit(&mut self, on: bool) {
        self.autosplit = on;
    }

    pub fn log(&self) -> &LogStore {
        &self.log
    }

    pub fn lease_keeper(&self, part: &str) -> LeaseKeeper {
        LeaseKeeper::new(
            LeaseStore::new(self.log.inner(), part, self.lease_mode),
            self.node_id,
        )
    }

    pub fn set_skip_ship(&self, skip: bool) {
        self.skip_ship
            .store(skip, std::sync::atomic::Ordering::Relaxed);
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
        let applied = self.meta.applied_seq_of(id).unwrap_or(0);
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

    /// Last shipped sequence for `part` (`next_seq - 1`), if known.
    pub fn last_shipped_seq(&self, part: &str) -> Option<u64> {
        self.parts.get(part).map(|p| p.next_seq.saturating_sub(1))
    }

    /// One full ordinary sync round. A persisted deposition is terminal:
    /// tailing may continue, but no lease may be acquired and no local
    /// journal may ship until explicit reintegration succeeds.
    pub async fn sync_all(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        self.sync_all_inner(leases, false).await
    }

    /// The explicit reintegration path is the sole exception to the
    /// persisted deposition gate. It needs temporary write authority to
    /// append the classified records, while `lease_lost` remains durable
    /// until the complete procedure succeeds.
    pub async fn sync_all_for_reintegration(
        &mut self,
        leases: &mut HashMap<String, LeaseKeeper>,
    ) -> Result<()> {
        self.sync_all_inner(leases, true).await
    }

    async fn sync_all_inner(
        &mut self,
        leases: &mut HashMap<String, LeaseKeeper>,
        reintegrating: bool,
    ) -> Result<()> {
        loop {
            self.tail_all().await?;
            if !reintegrating && matches!(self.meta.kv_get("lease_lost")?.as_deref(), Some("1")) {
                tracing::debug!(
                    "deposed node remains tail-only until reintegration; \
                     refusing ordinary lease acquisition and journal shipping"
                );
                return Ok(());
            }
            self.consider_xpart_aborts_all(leases).await?;
            if !self.ship_all(leases).await? {
                self.maybe_split_merge(leases).await?;
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
        loop {
            self.tail_part(part).await?;
            self.consider_xpart_aborts(part, lease).await?;
            if !self.ship_part(part, lease).await? {
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

    /// Tail every known partition. Applying a `part_split` can reveal a
    /// partition we did not know about, so this repeats until no new
    /// stream appears — otherwise `tail_to_head` would return while a
    /// freshly discovered child stream was still unread, which matters
    /// because lease takeover uses it as the "I have seen everything"
    /// witness.
    async fn tail_all(&mut self) -> Result<()> {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        loop {
            let mut ids: Vec<String> = self.parts.keys().cloned().collect();
            for (id, _) in self.meta.partitions()? {
                if !ids.iter().any(|x| x == &id) {
                    ids.push(id);
                }
            }
            let todo: Vec<String> = ids.into_iter().filter(|id| !seen.contains(id)).collect();
            if todo.is_empty() {
                return Ok(());
            }
            for id in &todo {
                self.ensure_part(id);
            }
            // One *parallel* LIST sweep across the partitions. Each
            // partition is its own S3 prefix, so a tree that has split
            // into N partitions costs N round trips here; sequentially
            // that dominates the sync round on a WAN mount (~200 ms ×
            // N per round, before a single segment is even fetched).
            let listings = futures::future::join_all(todo.iter().map(|id| {
                let log = self.parts[id].log.with_partition(id);
                let next = self.parts[id].next_seq;
                async move { log.list_segments_from(next).await }
            }))
            .await;
            for (id, listing) in todo.into_iter().zip(listings) {
                self.tail_part_listed(&id, listing?).await?;
                seen.insert(id);
            }
        }
    }

    async fn tail_part(&mut self, part: &str) -> Result<()> {
        self.ensure_part(part);
        let next = self.parts[part].next_seq;
        let seqs = self.parts[part].log.list_segments_from(next).await?;
        self.tail_part_listed(part, seqs).await
    }

    /// Tail `part` starting from an already-fetched listing, re-listing
    /// until no new contiguous segment appears. Payload GETs for the
    /// contiguous run are pipelined ([`TAIL_GET_CONCURRENCY`] in
    /// flight); `buffered` yields them in order, so records still apply
    /// in strict sequence.
    async fn tail_part_listed(&mut self, part: &str, mut seqs: Vec<u64>) -> Result<()> {
        use futures::StreamExt;
        loop {
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
            drop(fetched);
            let next = self.parts[part].next_seq;
            seqs = self.parts[part].log.list_segments_from(next).await?;
        }
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
            let grouped = self.meta.take_journal_grouped(seg.records.len())?;
            let journal = grouped
                .into_iter()
                .find(|(p, _)| p == part)
                .map(|(_, r)| r)
                .unwrap_or_default();
            let matches = journal.len() == seg.records.len()
                && journal.iter().map(|(_, r)| r).eq(seg.records.iter());
            if !matches {
                bail!(
                    "segment {seq} of {part} claims our node id {} but does not match \
                     the journal head: state dir reuse or id collision",
                    self.node_id
                );
            }
            let seqs: Vec<u64> = journal.iter().map(|(s, _)| *s).collect();
            self.meta.ack_journal_rows_at(&seqs, part, seq)?;
            self.note_xpart_shipped(&seg.records)?;
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
            self.meta.set_applied_seq_of(part, seq)?;
        } else {
            // Only *pending* (unshipped) local records may suppress a
            // foreign one: ours sit later in the global log than
            // anything we tail, so ours win everywhere. Shadowed
            // forwarded records must NOT suppress: the holder already
            // sequenced them, so a peer's record for the same inode can
            // legitimately follow ours. Skipping it would drop it for
            // good — `set_applied_seq_of` below never revisits a
            // segment — leaving each requester pinned to its own value.
            let pending =
                TouchSet::from_records(self.meta.take_journal(usize::MAX)?.iter().map(|(_, r)| r));
            let skipped = self.meta.apply_foreign(&seg.records, &pending)?;
            self.meta.shadow_retire_matching(seg.epoch, &seg.records)?;
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
            self.meta.set_applied_seq_of(part, seq)?;
            self.note_policy_records(&seg.records);
            self.last_ship_at.insert(part.to_string(), Instant::now());
        }
        let st = self.parts.get_mut(part).unwrap();
        st.max_epoch = st.max_epoch.max(seg.epoch);
        st.next_seq = seq + 1;
        {
            let mut spool = self.spool.lock().unwrap();
            spool.head_seq = spool.head_seq.max(seq);
            spool.last_error = None;
        }
        // A split record may have introduced a new partition.
        for (id, _) in self.meta.partitions()? {
            self.ensure_part(&id);
        }
        Ok(())
    }

    /// Ship every partition that has journaled records. A partition
    /// without a lease keeper yet gets one created and acquired here:
    /// keepers are otherwise only made by the FUSE write gate, so a
    /// daemon that restarted with a stranded child-partition journal
    /// would never ship it.
    async fn ship_all(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<bool> {
        let grouped = self.meta.take_journal_grouped(SEGMENT_BATCH)?;
        if grouped.is_empty() {
            return Ok(false);
        }
        let mut more = false;
        for (part, _batch) in grouped {
            self.ensure_part(&part);
            if !leases.contains_key(&part) {
                let mut keeper = LeaseKeeper::new(
                    LeaseStore::new(self.log.inner(), &part, self.lease_mode),
                    self.node_id,
                );
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
            if self.ship_part(&part, lease).await? {
                more = true;
            }
        }
        Ok(more)
    }

    /// Ship one journal batch for `part`. Returns true if another
    /// round is needed (more records pending, or a CAS collision).
    async fn ship_part(&mut self, part: &str, lease: &LeaseKeeper) -> Result<bool> {
        if self.skip_ship.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(false);
        }
        let Some(epoch) = lease.ship_epoch() else {
            return Ok(false);
        };
        let grouped = self.meta.take_journal_grouped(SEGMENT_BATCH)?;
        let Some((_, batch)) = grouped.into_iter().find(|(p, _)| p == part) else {
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
        let atime_rows = self.meta.take_atime_of(part, SEGMENT_BATCH)?;
        for (ino, atime_ns, time_ns) in &atime_rows {
            records.push(LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            });
        }
        let payload = encode(self.node_id, epoch, &records)?;
        let next_seq = self.parts[part].next_seq;
        match self.parts[part].log.put_segment(next_seq, &payload).await {
            Ok(()) => {}
            Err(constellation_store_s3::StoreError::AlreadyExists) => return Ok(true),
            Err(e) => return Err(e).context("shipping log segment"),
        }
        let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
        self.meta.ack_journal_rows_at(&seqs, part, next_seq)?;
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
        self.note_xpart_shipped(&records)?;
        self.note_shipped(part, &records);
        // Push invalidation: tell peers the segment is durable so they
        // tail now rather than at their next poll. Best effort by
        // design — the poll is what guarantees they converge.
        self.peers
            .announce_segment(part, next_seq, epoch, Some(payload.clone()))
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
        self.shipped_since_ckpt += 1;
        if self.checkpoint_is_due() {
            self.checkpoint().await?;
        }
        Ok(true)
    }

    /// Drain a partition's pending read-time atime (plan 20) into one
    /// final segment under `lease`'s epoch, then clear it — called
    /// before an idle lease release or partition merge so a read-heavy
    /// holder's atime reaches the cluster instead of dying with the
    /// lease. Best effort: if the segment cannot be put, the rows are
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
                let st = self.parts.get_mut(part).unwrap();
                st.max_epoch = st.max_epoch.max(epoch);
                st.next_seq = next_seq + 1;
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

    fn note_shipped(&mut self, part: &str, records: &[LogRecord]) {
        self.last_ship_at.insert(part.to_string(), Instant::now());
        if !self.autosplit {
            // Nothing consumes `dir_ops` while the trigger is disarmed,
            // and `retain` below deliberately keeps armed entries for a
            // later round — so tracking here would grow without bound.
            return;
        }
        // A directory that already is a partition root cannot split
        // again, so its traffic is not worth tracking.
        let roots: std::collections::HashSet<u64> = self
            .meta
            .partitions()
            .unwrap_or_default()
            .into_iter()
            .map(|(_, root)| root)
            .collect();
        let mut touched: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for rec in records {
            if let Some(dir) = self.traffic_dir_of(rec) {
                if roots.contains(&dir) {
                    continue;
                }
                self.dir_ops.entry(dir).or_default().ops += 1;
                touched.insert(dir);
            }
        }
        let thresh = part_split_ops();
        self.dir_ops.retain(|ino, t| {
            if roots.contains(ino) {
                return false;
            }
            if touched.contains(ino) {
                t.segments += 1;
                t.armed = t.segments >= 2 && t.ops >= thresh;
                return true;
            }
            // Not in this segment: the run is broken. Keep only an
            // already-armed candidate, whose split is journaled on a
            // later round (typically a segment with no traffic for it).
            t.armed
        });
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

    /// After a src/dst/abort half is durable in the log, update the
    /// local pending table so the holder can abort an orphan src.
    fn note_xpart_shipped(&self, records: &[LogRecord]) -> Result<()> {
        for rec in records {
            match rec {
                LogRecord::RenameXpartDst { txid, .. } => {
                    self.meta.mark_xpart_dst(*txid)?;
                    self.meta.unpark_xpart(*txid)?;
                }
                LogRecord::RenameXpartAbort { txid } => {
                    self.meta.unpark_xpart(*txid)?;
                }
                LogRecord::RenameXpartSrc { txid, .. } => {
                    if self.meta.xpart_dst_seen(*txid)? {
                        self.meta.unpark_xpart(*txid)?;
                    } else {
                        let dst_journaled = self
                            .meta
                            .take_journal_grouped(usize::MAX)?
                            .iter()
                            .any(|(_, recs)| {
                                recs.iter().any(|(_, r)| {
                                    matches!(r, LogRecord::RenameXpartDst { txid: t, .. } if *t == *txid)
                                })
                            });
                        if !dst_journaled {
                            self.meta.park_xpart(*txid, "src", rec)?;
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn note_policy_records(&mut self, records: &[LogRecord]) {
        for rec in records {
            match rec {
                LogRecord::PartSplit {
                    new_part, at_ino, ..
                } => {
                    self.ensure_part(new_part);
                    // A partition root cannot itself split again.
                    self.dir_ops.remove(at_ino);
                }
                LogRecord::PartMerge { part, .. } => {
                    self.parts.remove(part);
                }
                _ => {}
            }
        }
    }

    /// A `RenameXpartSrc` whose partner never appears is voided by
    /// appending `RenameXpartAbort` on the src stream. Only the current
    /// holder of the src partition may do this, and only after the src
    /// half is durable (parked) with no dst at the dst stream's head.
    async fn consider_xpart_aborts(&mut self, held_part: &str, lease: &LeaseKeeper) -> Result<()> {
        if lease.ship_epoch().is_none() {
            return Ok(());
        }
        let pending = self.meta.pending_xparts()?;
        for (txid, half, rec) in pending {
            if half != "src" {
                continue;
            }
            let LogRecord::RenameXpartSrc { part, .. } = rec else {
                continue;
            };
            if part != held_part {
                continue;
            }
            if self.meta.xpart_dst_seen(txid)? {
                self.meta.unpark_xpart(txid)?;
                continue;
            }
            let has_dst_pending = self
                .meta
                .pending_xparts()?
                .iter()
                .any(|(t, h, _)| *t == txid && h == "dst");
            if has_dst_pending {
                continue;
            }
            let has_dst_journaled =
                self.meta
                    .take_journal_grouped(usize::MAX)?
                    .iter()
                    .any(|(_, recs)| {
                        recs.iter().any(|(_, r)| {
                        matches!(r, LogRecord::RenameXpartDst { txid: t, .. } if *t == txid)
                    })
                    });
            if has_dst_journaled {
                continue;
            }
            self.meta
                .journal_on(&part, &LogRecord::RenameXpartAbort { txid })?;
            tracing::warn!(txid, part, "aborting orphan rename_xpart src");
        }
        Ok(())
    }

    async fn consider_xpart_aborts_all(
        &mut self,
        leases: &HashMap<String, LeaseKeeper>,
    ) -> Result<()> {
        for (part, k) in leases {
            self.consider_xpart_aborts(part, k).await?;
        }
        Ok(())
    }

    /// Enough segments shipped *and* enough time elapsed. Shutdown and
    /// explicit sync paths bypass this and checkpoint unconditionally.
    fn checkpoint_is_due(&self) -> bool {
        checkpoint_is_due(
            self.shipped_since_ckpt,
            self.last_ckpt_at.map(|at| at.elapsed()),
            checkpoint_min_interval(),
        )
    }

    /// Snapshot the local DB as a checkpoint covering every partition
    /// this replica has seen, plus a VECTOR.json sidecar.
    pub async fn checkpoint(&mut self) -> Result<()> {
        let mut vector = CheckpointVector::default();
        let mut covered = 0u64;
        for (id, st) in &self.parts {
            let seq = st.next_seq.saturating_sub(1);
            vector.applied.insert(id.clone(), seq);
            covered = covered.max(seq);
        }
        if covered == 0 {
            return Ok(());
        }
        // Copying the DB takes tens of milliseconds on a large namespace, so
        // it does not belong on a runtime worker.
        let meta = Arc::clone(&self.meta);
        let started = Instant::now();
        let snap = tokio::task::spawn_blocking(move || meta.snapshot())
            .await
            .context("checkpoint snapshot task")??;
        let snapshot_ms = started.elapsed().as_millis();
        self.log
            .put_checkpoint_with_vector(covered, &snap, &vector)
            .await?;
        self.shipped_since_ckpt = 0;
        self.last_ckpt_at = Some(Instant::now());
        tracing::info!(
            seq = covered,
            bytes = snap.len(),
            parts = vector.applied.len(),
            snapshot_ms,
            "wrote metadata checkpoint"
        );
        Ok(())
    }

    /// Final sync + checkpoint on clean unmount.
    #[allow(dead_code)]
    pub async fn shutdown(&mut self, lease: &LeaseKeeper) -> Result<()> {
        self.sync(lease).await?;
        if self.shipped_since_ckpt > 0 {
            self.checkpoint().await?;
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
            self.tail_all().await?;
            if matches!(self.meta.kv_get("lease_lost")?.as_deref(), Some("1")) {
                tracing::info!(
                    journal_backlog = MetaStore::journal_len(&*self.meta).unwrap_or(0),
                    "deposed node remains tail-only until reintegration; \
                     journal will not ship on this unmount"
                );
                return Ok(());
            }
            self.consider_xpart_aborts_all(leases).await?;
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
                self.maybe_split_merge(leases).await?;
                break;
            }
        }
        if self.shipped_since_ckpt > 0 {
            self.checkpoint().await?;
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

    pub fn journal_backlog_of(&self, part: &str) -> u64 {
        self.meta
            .take_journal_grouped(usize::MAX)
            .ok()
            .and_then(|g| g.into_iter().find(|(p, _)| p == part))
            .map(|(_, r)| r.len() as u64)
            .unwrap_or(0)
    }

    /// Directories currently over the split threshold (two consecutive
    /// shipped segments, each ≥ `CONSTELLATION_PART_SPLIT_OPS`).
    #[allow(dead_code)]
    pub fn split_candidates(&self) -> Vec<u64> {
        self.dir_ops
            .iter()
            .filter(|(_, t)| t.armed)
            .map(|(ino, _)| *ino)
            .collect()
    }

    async fn maybe_split_merge(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        // Merging stays enabled regardless, so a filesystem that split
        // under an earlier policy can still collapse back.
        let might_split = self.autosplit && self.dir_ops.values().any(|t| t.armed);
        let might_merge = self.meta.partitions()?.len() > 1;
        if !might_split && !might_merge {
            return Ok(());
        }
        // Single-node filesystems must never split (or merge).
        let nodes = match constellation_store_s3::list_node_ids(self.log.inner()).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(error = %e, "list_node_ids failed; skipping split/merge");
                return Ok(());
            }
        };
        if nodes.len() < 2 {
            return Ok(());
        }
        if might_split {
            self.maybe_split(leases).await?;
        }
        if might_merge {
            self.maybe_merge(leases).await?;
        }
        Ok(())
    }

    async fn maybe_split(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        let candidates: Vec<u64> = self
            .dir_ops
            .iter()
            .filter(|(_, t)| t.armed)
            .map(|(ino, _)| *ino)
            .collect();
        for ino in candidates {
            // Only a *direct* subdirectory of a partition root splits.
            let Some(parent) = parent_of(&self.meta, ino) else {
                continue;
            };
            let parent_part = self.meta.partition_of(parent)?;
            let child_part = self.meta.partition_of(ino)?;
            if parent_part != child_part {
                continue; // already its own partition
            }
            let Some(lease) = leases.get(&parent_part) else {
                continue;
            };
            if lease.ship_epoch().is_none() {
                continue; // we don't hold the parent
            }
            // Refuse to split if this dir is already a partition root.
            if self.meta.partitions()?.iter().any(|(_, root)| *root == ino) {
                continue;
            }
            let new_part = self.meta.next_part_id()?;
            self.log
                .ensure_partition_key(&new_part)
                .await
                .context("creating partition encryption key")?;
            let rec = LogRecord::PartSplit {
                part: parent_part.clone(),
                at_ino: ino,
                new_part: new_part.clone(),
                time_ns: constellation_fs_core::types::now_ns(),
            };
            self.meta.journal_on(&parent_part, &rec)?;
            self.ensure_part(&new_part);
            self.last_ship_at.insert(new_part.clone(), Instant::now());
            tracing::info!(part = %parent_part, new_part = %new_part, at_ino = ino, "splitting partition");
            self.dir_ops.remove(&ino);
        }
        Ok(())
    }

    async fn maybe_merge(&mut self, leases: &mut HashMap<String, LeaseKeeper>) -> Result<()> {
        let idle = std::time::Duration::from_secs(part_merge_idle_s());
        let parts = self.meta.partitions()?;
        for (id, root) in parts {
            if id == PARTITION {
                continue;
            }
            let last = self.last_ship_at.get(&id).copied();
            let quiet = match last {
                Some(t) => t.elapsed() >= idle,
                None => {
                    // Never shipped on this process; treat as idle only
                    // if the stream has no records past seq 0 and we
                    // hold the parent.
                    self.parts.get(&id).map(|p| p.next_seq <= 1).unwrap_or(true)
                }
            };
            if !quiet {
                continue;
            }
            if self.journal_backlog_of(&id) > 0 {
                continue;
            }
            let parent_ino = parent_of(&self.meta, root).unwrap_or(1);
            let parent_part = self.meta.partition_of(parent_ino)?;
            if parent_part == id {
                continue;
            }
            // Holder of the PARENT performs the merge, after taking
            // the child's lease.
            let Some(parent_lease) = leases.get(&parent_part) else {
                continue;
            };
            if parent_lease.ship_epoch().is_none() {
                continue;
            }
            // The parent holder must also hold (or find idle) the child
            // so a live writer on the child cannot race the merge.
            match leases.get(&id) {
                Some(child_lease) if child_lease.ship_epoch().is_none() => continue,
                None => continue,
                Some(_) => {}
            }
            let rec = LogRecord::PartMerge {
                part: id.clone(),
                into_part: parent_part.clone(),
                time_ns: constellation_fs_core::types::now_ns(),
            };
            self.meta.journal_on(&parent_part, &rec)?;
            // Forget this subtree's traffic: it was, by definition, hot
            // enough to split once. Leaving the run armed would re-split
            // the directory on the very next round, so a merge could
            // never settle (split/merge must be hysteretic).
            self.dir_ops.remove(&root);
            if let Some(child_log) = self.parts.get(&id) {
                let _ = child_log.log.seal().await;
            }
            tracing::info!(part = %id, into = %parent_part, "merging partition");
        }
        Ok(())
    }
}

fn parent_of(meta: &SqliteMeta, ino: u64) -> Option<u64> {
    meta.parent_of(ino).ok().flatten()
}

/// Acquire the lease for `part` if it is free, tailing that stream to
/// head first when this would be a takeover from another node. Returns
/// false when a live foreign holder still owns it (the caller waits and
/// retries). This is the *only* acquisition path, which is what makes
/// the takeover ordering rule structural.
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
    } = &plan
    {
        tracing::debug!(
            holder,
            expires_in_ms,
            part,
            "partition lease held by another node"
        );
    }
    let tailed = if plan.needs_tail() {
        Some(ship.tail_part_to_head(part).await?)
    } else {
        None
    };
    keeper.commit(plan, tailed).await
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

/// Build a fresh local replica from S3: latest checkpoint (if any) plus
/// replay of newer segments. Used when the state dir has no metadata DB.
///
/// The checkpoint is a whole-DB snapshot; `checkpoints/VECTOR.json`
/// records every partition's applied_seq at snapshot time. Bootstrap
/// restores the snapshot and then tails each partition from its vector
/// entry (falling back to p0-only for pre-partition checkpoints).
pub async fn bootstrap(db_path: &std::path::Path, log: &LogStore) -> Result<()> {
    let from_seq = match log.get_latest_checkpoint().await? {
        Some((seq, snapshot)) => {
            std::fs::write(db_path, &snapshot).context("writing checkpoint snapshot")?;
            tracing::info!(seq, "restored metadata checkpoint");
            seq
        }
        None => 0,
    };
    let vector = log.get_checkpoint_vector().await?;
    let meta = SqliteMeta::open(db_path)?;
    let mut replayed = 0usize;
    let mut parts: Vec<String> = vector.applied.keys().cloned().collect();
    if parts.is_empty() {
        parts.push(PARTITION.into());
    }
    for (id, _) in meta.partitions().unwrap_or_default() {
        if !parts.iter().any(|p| p == &id) {
            parts.push(id);
        }
    }
    for part in parts {
        let start = vector
            .applied
            .get(&part)
            .copied()
            .unwrap_or(if part == PARTITION { from_seq } else { 0 });
        let part_log = log.with_partition(&part);
        let mut applied = start;
        for seq in part_log.list_segments_from(start + 1).await? {
            if seq != applied + 1 {
                break;
            }
            let seg = decode(&part_log.get_segment(seq).await?)?;
            replayed += seg.records.len();
            meta.apply_records(&seg.records)
                .with_context(|| format!("replaying {part} log segment {seq}"))?;
            applied = seq;
        }
        meta.set_applied_seq_of(&part, applied)?;
    }
    for ino in meta.orphans()? {
        meta.reap_orphan(ino)?;
    }
    tracing::info!(from_seq, replayed, "bootstrapped metadata replica");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseKeeper;
    use constellation_meta::MetaStore;
    use constellation_store_s3::{LeaseMode, LeaseStore, LogStore};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use std::sync::Arc as StdArc;
    use std::time::Duration;

    struct Node {
        meta: Arc<SqliteMeta>,
        ship: Shipper,
        lease: LeaseKeeper,
    }

    /// Checkpointing every 32 shipped segments fires roughly every 100ms
    /// under small-file writes, and each one copies the whole metadata DB.
    /// The interval floor exists to space that out, but it costs
    /// single-threaded latency (see `CHECKPOINT_MIN_INTERVAL_S`), so the
    /// default must leave the segment count in sole charge.
    #[test]
    fn checkpoint_trigger_is_segment_count_until_a_floor_is_set() {
        let none = Duration::from_secs(CHECKPOINT_MIN_INTERVAL_S);
        assert_eq!(none, Duration::ZERO, "the floor must default to off");

        // Below the segment count, nothing triggers a checkpoint.
        assert!(!checkpoint_is_due(CHECKPOINT_EVERY - 1, None, none));
        assert!(!checkpoint_is_due(
            CHECKPOINT_EVERY - 1,
            Some(Duration::from_secs(3600)),
            none
        ));

        // At the count, the default fires regardless of recency.
        assert!(checkpoint_is_due(CHECKPOINT_EVERY, None, none));
        assert!(checkpoint_is_due(
            CHECKPOINT_EVERY,
            Some(Duration::ZERO),
            none
        ));

        // A configured floor delays it, and only until the gap is met.
        let floor = Duration::from_secs(60);
        assert!(!checkpoint_is_due(
            CHECKPOINT_EVERY,
            Some(Duration::from_secs(59)),
            floor
        ));
        assert!(checkpoint_is_due(
            CHECKPOINT_EVERY,
            Some(Duration::from_secs(60)),
            floor
        ));
        // The first checkpoint of a mount has no gap to wait out.
        assert!(checkpoint_is_due(CHECKPOINT_EVERY, None, floor));
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        let meta = Arc::new(SqliteMeta::open_in_memory().unwrap());
        meta.set_node_prefix(id).unwrap();
        let log = LogStore::new(store.clone());
        let mut ship = Shipper::attach(meta.clone(), log, id).unwrap();
        // These tests exercise the split/merge machinery itself, so they
        // arm the heat trigger explicitly. Production leaves it off (see
        // `part_autosplit`); `a_hot_directory_holds_together_while_autosplit_is_off`
        // covers that default.
        ship.set_autosplit(true);
        let lease = LeaseKeeper::new(
            LeaseStore::new(
                store.clone(),
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

    fn names(meta: &SqliteMeta, parent: u64) -> Vec<(String, u64)> {
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

    /// Every record on one partition's stream, in sequence order.
    async fn all_records(store: &StdArc<InMemory>, part: &str) -> Vec<LogRecord> {
        let log = LogStore::for_partition(store.clone(), part);
        let mut out = Vec::new();
        for seq in log.list_segments().await.unwrap() {
            out.extend(
                decode(&log.get_segment(seq).await.unwrap())
                    .unwrap()
                    .records,
            );
        }
        out
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
        let part = constellation_store_s3::log::PARTITION;

        // The holder owns the file; both requesters tail it in.
        let f = holder.meta.create(1, "duel", 0o644, 0, 0).unwrap();
        holder.sync().await;
        a.ship.tail_to_head().await.unwrap();
        b.ship.tail_to_head().await.unwrap();

        let mode_of = |m: &SqliteMeta| m.lookup(1, "duel").unwrap().unwrap().mode & 0o777;
        assert_eq!(mode_of(&a.meta), 0o644, "requester must see the file first");

        // Each requester forwards a chmod: the holder executes and
        // journals it, the requester shadows and applies the records so
        // it can read its own write before the segment ships.
        for (requester, mode) in [(&mut a, 0o600u32), (&mut b, 0o640u32)] {
            let op = MutateOp::Setattr {
                ino: f.ino,
                mode: Some(mode),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            };
            let records = execute_mutate(&holder.meta, &op).unwrap();
            crate::forward::apply_accepted(&requester.meta, part, 1, &records).unwrap();
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
    /// stranded branch. Explicit reintegration is the only bypass.
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

        a.ship
            .sync_all_for_reintegration(&mut leases)
            .await
            .unwrap();
        assert_eq!(a.meta.journal_len().unwrap(), 0);
        assert_eq!(a.ship.log.list_segments().await.unwrap(), [1]);
    }

    /// A late segment stamped with a superseded epoch is fenced out
    /// rather than applied over the current holder's state.
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

        b.sync().await;
        assert!(
            b.meta.lookup(1, "zombie").unwrap().is_none(),
            "a superseded epoch must not mutate the namespace"
        );
        assert_eq!(b.ship.spool.lock().unwrap().fenced, 1);
        assert_eq!(b.meta.applied_seq().unwrap(), seq);
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

    /// The traffic heuristic must actually arm and fire: a directory that
    /// sustains ≥ threshold write ops across at least two consecutive
    /// shipped segments becomes its own partition, the split record lands
    /// on the PARENT stream, and later writes land on the child stream.
    ///
    /// Regression: the counter required each individual *segment* to meet
    /// the threshold. The syncer ships whatever is journaled every sync
    /// interval, so a hot directory written one file at a time produces
    /// many tiny segments and could never arm — no split ever happened
    /// (the harness saw only `p0` forever). Traffic must accumulate
    /// across consecutive segments instead.
    #[tokio::test]
    async fn traffic_threshold_splits_a_hot_directory() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        // A second registered node: single-node filesystems never split.
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();

        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;

        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        // Write the threshold's worth of ops ONE AT A TIME, each shipped
        // as its own small segment — the realistic daemon shape that the
        // old per-segment rule could never satisfy.
        for i in 0..thresh {
            a.meta
                .create(hot.ino, &format!("f{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        // One more round: the armed candidate is journaled as a split.
        a.ship.sync_all(&mut leases).await.unwrap();

        let parts = a.meta.partitions().unwrap();
        let child = parts.iter().find(|(id, _)| id != PARTITION);
        assert!(
            child.is_some(),
            "hot dir never split; partitions={parts:?} candidates={:?}",
            a.ship.split_candidates()
        );
        let (child_id, child_root) = child.unwrap();
        assert_eq!(*child_root, hot.ino, "child must be rooted at /hot");
        assert_eq!(a.meta.partition_of(hot.ino).unwrap(), *child_id);

        // The split record itself belongs to the parent stream.
        let parent_recs = all_records(&store, PARTITION).await;
        assert!(
            parent_recs
                .iter()
                .any(|r| matches!(r, LogRecord::PartSplit { at_ino, .. } if *at_ino == hot.ino)),
            "part_split must be carried by the parent stream"
        );

        // Post-split writes flow to the child stream.
        let mut kc = LeaseKeeper::new(LeaseStore::new(store.clone(), child_id, LeaseMode::Cas), 1);
        acquire_lease_for(&mut a.ship, &mut kc, child_id)
            .await
            .unwrap();
        a.meta.create(hot.ino, "after", 0o644, 0, 0).unwrap();
        a.ship.sync_one(child_id, &kc).await.unwrap();
        let child_recs = all_records(&store, child_id).await;
        assert!(
            child_recs
                .iter()
                .any(|r| matches!(r, LogRecord::Create { name, .. } if name == "after")),
            "post-split writes must land on the child stream"
        );

        // A second node bootstrapping from the log alone agrees.
        let mut b = node(&store, 2);
        b.ship.tail_to_head().await.unwrap();
        assert_eq!(b.meta.partition_of(hot.ino).unwrap(), *child_id);
        assert!(b.meta.lookup(hot.ino, "after").unwrap().is_some());
    }

    /// A journal batch for a partition this node has no lease keeper for
    /// must still be shipped: the keeper is created and acquired lazily.
    ///
    /// Regression: `ship_all` skipped any partition missing from the
    /// `leases` map. Lease keepers were only ever created by the FUSE
    /// write gate, so after a remount (which starts with a `p0` keeper
    /// only) a stranded journal for a child partition was never shipped
    /// and its records were invisible to every other node — exactly what
    /// `rename-across-partitions` hit after `kill9`.
    #[tokio::test]
    async fn stranded_child_journal_ships_without_a_preexisting_keeper() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartSplit {
                    part: "p0".into(),
                    at_ino: hot.ino,
                    new_part: "p1".into(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.sync().await;
        assert_eq!(a.meta.partition_of(hot.ino).unwrap(), "p1");

        // Records for p1 are journaled, but only p0 has a keeper — the
        // state a daemon is in right after a remount.
        a.meta.create(hot.ino, "stranded", 0o644, 0, 0).unwrap();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        assert_eq!(a.ship.journal_backlog_of("p1"), 1);

        a.ship.sync_all(&mut leases).await.unwrap();

        assert_eq!(
            a.ship.journal_backlog_of("p1"),
            0,
            "p1's journal must drain even though no keeper existed for it"
        );
        assert!(
            leases.contains_key("p1"),
            "a keeper for p1 must have been created lazily"
        );
        let child = all_records(&store, "p1").await;
        assert!(
            child
                .iter()
                .any(|r| matches!(r, LogRecord::Create { name, .. } if name == "stranded")),
            "the stranded record must reach p1's stream: {child:?}"
        );
        // And a second node sees it from the log alone.
        let mut b = node(&store, 2);
        b.ship.tail_to_head().await.unwrap();
        assert!(b.meta.lookup(hot.ino, "stranded").unwrap().is_some());
    }

    /// File writes must count as traffic against the *directory* holding
    /// the file. `write_manifest`/`setattr` name the file inode, so
    /// attributing them to that inode meant a directory full of file
    /// writes (the normal case, and what the harness does) never armed.
    #[tokio::test]
    async fn file_writes_count_toward_their_directory() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;

        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        // Only manifest writes on a file inside /hot — no dentry op names
        // /hot at all after the initial create, so the directory can only
        // arm if file writes are attributed to their parent.
        let f = a.meta.create(hot.ino, "big", 0o644, 0, 0).unwrap();
        a.ship.sync_all(&mut leases).await.unwrap();
        for i in 0..thresh {
            a.meta.set_manifest(f.ino, b"M", 1 + i).unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        a.ship.sync_all(&mut leases).await.unwrap();

        let parts = a.meta.partitions().unwrap();
        assert!(
            parts.iter().any(|(_, root)| *root == hot.ino),
            "file-write traffic must split /hot; partitions={parts:?}"
        );
    }

    /// Merging a child back must clear that subtree's traffic run.
    /// Otherwise the still-armed counter immediately re-splits the same
    /// directory (the harness saw `p0`+`p2` right after the merge), so a
    /// merge could never settle — splits must be hysteretic.
    #[tokio::test]
    async fn merge_clears_traffic_so_it_does_not_immediately_resplit() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;

        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        for i in 0..thresh {
            a.meta
                .create(hot.ino, &format!("f{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        a.ship.sync_all(&mut leases).await.unwrap();
        let child = a
            .meta
            .partitions()
            .unwrap()
            .into_iter()
            .find(|(id, _)| id != PARTITION)
            .expect("expected a split");
        assert_eq!(child.1, hot.ino);

        // Traffic continues on /hot while it is its own partition — the
        // real harness shape (a post-split write, then the merge).
        let mut kc = LeaseKeeper::new(LeaseStore::new(store.clone(), &child.0, LeaseMode::Cas), 1);
        acquire_lease_for(&mut a.ship, &mut kc, &child.0)
            .await
            .unwrap();
        leases.insert(child.0.clone(), kc);
        for i in 0..thresh {
            a.meta
                .create(hot.ino, &format!("post{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }

        // Merge it back the way the policy does.
        a.meta
            .journal_on(
                PARTITION,
                &LogRecord::PartMerge {
                    part: child.0.clone(),
                    into_part: PARTITION.into(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.ship.sync_all(&mut leases).await.unwrap();
        assert_eq!(a.meta.partition_of(hot.ino).unwrap(), PARTITION);

        // Several idle rounds must NOT resurrect the split.
        for _ in 0..4 {
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        let parts = a.meta.partitions().unwrap();
        assert_eq!(
            parts.len(),
            1,
            "merged subtree re-split immediately: {parts:?} candidates={:?}",
            a.ship.split_candidates()
        );
    }

    /// Partition ids must be globally unique. `next_part_id` is a local
    /// counter, so two nodes splitting different directories would both
    /// mint `p1` and the "replicated" partition map would disagree per
    /// node (observed live: c0 saw `[p0, p2]` while c1 saw `[p0, p1]`).
    /// Ids are therefore node-scoped.
    #[tokio::test]
    async fn concurrent_splits_on_two_nodes_get_distinct_ids() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let d1 = a.meta.mkdir(1, "one", 0o755, 0, 0).unwrap();
        let d2 = a.meta.mkdir(1, "two", 0o755, 0, 0).unwrap();
        a.sync().await;
        b.ship.tail_to_head().await.unwrap();

        let id_a = a.meta.next_part_id().unwrap();
        let id_b = b.meta.next_part_id().unwrap();
        assert_ne!(
            id_a, id_b,
            "two nodes minted the same partition id ({id_a}); the replicated \
             partition map would diverge"
        );

        // Both splits replay everywhere and both partitions survive.
        a.meta
            .journal_on(
                PARTITION,
                &LogRecord::PartSplit {
                    part: PARTITION.into(),
                    at_ino: d1.ino,
                    new_part: id_a.clone(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.sync().await;
        a.release().await;
        b.meta
            .journal_on(
                PARTITION,
                &LogRecord::PartSplit {
                    part: PARTITION.into(),
                    at_ino: d2.ino,
                    new_part: id_b.clone(),
                    time_ns: 2,
                },
            )
            .unwrap();
        b.sync().await;
        a.ship.tail_to_head().await.unwrap();

        for m in [&a.meta, &b.meta] {
            assert_eq!(m.partition_of(d1.ino).unwrap(), id_a);
            assert_eq!(m.partition_of(d2.ino).unwrap(), id_b);
        }
    }

    /// A directory that is written *intermittently* must never split, no
    /// matter how long it lives or how many writes it sees in total.
    ///
    /// Regression: traffic was accumulated as a lifetime total, so any
    /// long-lived directory eventually crossed the threshold and split.
    /// That silently broke `lease-handover`, which assumes the filesystem
    /// stays a single partition. The run must be consecutive.
    #[tokio::test]
    async fn intermittent_traffic_never_splits() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        let warm = a.meta.mkdir(1, "warm", 0o755, 0, 0).unwrap();
        let other = a.meta.mkdir(1, "other", 0o755, 0, 0).unwrap();
        a.sync().await;

        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        // Far more than the threshold in total, but never two consecutive
        // shipped segments in a row: every write is followed by a segment
        // that only touches an unrelated directory.
        for i in 0..(thresh * 3) {
            a.meta
                .create(warm.ino, &format!("w{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
            a.meta
                .create(other.ino, &format!("o{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        a.ship.sync_all(&mut leases).await.unwrap();

        let parts = a.meta.partitions().unwrap();
        assert_eq!(
            parts.len(),
            1,
            "intermittently written dirs must not split: {parts:?} candidates={:?}",
            a.ship.split_candidates()
        );
    }

    /// A single-node filesystem must never split, however hot a directory
    /// gets: with one registered node there is nobody to hand work to.
    #[tokio::test]
    async fn single_node_never_splits() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
            .await
            .unwrap();
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;
        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        for wave in 0..3 {
            for i in 0..thresh {
                a.meta
                    .create(hot.ino, &format!("f{wave}-{i}"), 0o644, 0, 0)
                    .unwrap();
                a.ship.sync_all(&mut leases).await.unwrap();
            }
        }
        assert_eq!(
            a.meta.partitions().unwrap().len(),
            1,
            "a single-node filesystem must never split"
        );
    }

    /// The shipped default. A lone writer walking a tree (rsync, image
    /// unpack) makes every directory hot in turn; with several nodes
    /// merely *enrolled*, the old always-on heuristic carved a partition
    /// out of each one — 89 of them on a `rsync -a /usr`, each costing a
    /// LIST per sync round and a lease to renew, none of them relieving
    /// any contention, because forwarded mutations already let the other
    /// nodes write without taking the lease.
    #[tokio::test]
    async fn a_hot_directory_holds_together_while_autosplit_is_off() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        a.ship.set_autosplit(false);
        // Two enrolled nodes: the only thing that used to stand between
        // this workload and a split.
        for _ in 0..2 {
            constellation_store_s3::claim_node_id(store.clone() as StdArc<dyn ObjectStore>)
                .await
                .unwrap();
        }
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;

        let thresh = part_split_ops();
        let mut leases = HashMap::new();
        leases.insert(PARTITION.to_string(), a.lease);
        for i in 0..thresh {
            a.meta
                .create(hot.ino, &format!("f{i}"), 0o644, 0, 0)
                .unwrap();
            a.ship.sync_all(&mut leases).await.unwrap();
        }
        a.ship.sync_all(&mut leases).await.unwrap();

        assert_eq!(
            a.meta.partitions().unwrap().len(),
            1,
            "heat alone must not carve a partition while autosplit is off"
        );
        assert!(
            a.ship.split_candidates().is_empty(),
            "a disarmed trigger must not accumulate candidates either"
        );
    }

    /// Split: records land in the child stream after the split point; a
    /// second node tails both streams and converges.
    #[tokio::test]
    async fn split_child_stream_and_second_node_converges() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;
        a.release().await;

        let rec = LogRecord::PartSplit {
            part: "p0".into(),
            at_ino: hot.ino,
            new_part: "p1".into(),
            time_ns: 1,
        };
        a.meta.journal_on("p0", &rec).unwrap();
        a.sync().await;
        assert_eq!(a.meta.partition_of(hot.ino).unwrap(), "p1");

        a.meta.create(hot.ino, "child-file", 0o644, 0, 0).unwrap();
        // After split, shipping p1 needs a keeper for p1.
        let mut k1 = LeaseKeeper::new(LeaseStore::new(store.clone(), "p1", LeaseMode::Cas), 1);
        acquire_lease_for(&mut a.ship, &mut k1, "p1").await.unwrap();
        a.ship.sync_one("p1", &k1).await.unwrap();

        b.sync().await;
        acquire_lease_for(&mut b.ship, &mut b.lease, "p0")
            .await
            .ok();
        b.ship.tail_to_head().await.unwrap();
        assert_eq!(b.meta.partition_of(hot.ino).unwrap(), "p1");
        assert!(b.meta.lookup(hot.ino, "child-file").unwrap().is_some());
        let p1 = LogStore::for_partition(store.clone(), "p1");
        assert!(
            !p1.list_segments().await.unwrap().is_empty(),
            "child stream must contain the post-split records"
        );
    }

    /// Merge: child sealed, records flow to parent, replicas converge.
    #[tokio::test]
    async fn merge_seals_child_and_replicas_converge() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartSplit {
                    part: "p0".into(),
                    at_ino: hot.ino,
                    new_part: "p1".into(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartMerge {
                    part: "p1".into(),
                    into_part: "p0".into(),
                    time_ns: 2,
                },
            )
            .unwrap();
        a.sync().await;
        LogStore::for_partition(store.clone(), "p1")
            .seal()
            .await
            .unwrap();
        assert!(LogStore::for_partition(store.clone(), "p1")
            .is_sealed()
            .await
            .unwrap());
        assert_eq!(a.meta.partition_of(hot.ino).unwrap(), "p0");
        a.meta.create(hot.ino, "after-merge", 0o644, 0, 0).unwrap();
        a.sync().await;
        b.ship.tail_to_head().await.unwrap();
        assert_eq!(b.meta.partition_of(hot.ino).unwrap(), "p0");
        assert!(b.meta.lookup(hot.ino, "after-merge").unwrap().is_some());
    }

    /// Xpart rename: both halves applied on a tailing replica; abort
    /// path drops the dst half before shipping, next writer appends the
    /// abort, every replica keeps the file at the source.
    #[tokio::test]
    async fn xpart_rename_and_abort() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let mut b = node(&store, 2);
        let hot = a.meta.mkdir(1, "hot", 0o755, 0, 0).unwrap();
        let cold = a.meta.mkdir(1, "cold", 0o755, 0, 0).unwrap();
        let f = a.meta.create(hot.ino, "x", 0o644, 0, 0).unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartSplit {
                    part: "p0".into(),
                    at_ino: hot.ino,
                    new_part: "p1".into(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.sync().await;
        b.ship.tail_to_head().await.unwrap();

        // Happy path: both halves journaled and shipped.
        a.meta
            .rename_xpart(hot.ino, "x", cold.ino, "y", "p1", "p0")
            .unwrap();
        let mut k1 = LeaseKeeper::new(LeaseStore::new(store.clone(), "p1", LeaseMode::Cas), 1);
        acquire_lease_for(&mut a.ship, &mut a.lease, "p0")
            .await
            .unwrap();
        acquire_lease_for(&mut a.ship, &mut k1, "p1").await.unwrap();
        a.ship.sync_one("p1", &k1).await.unwrap();
        a.ship.sync_one("p0", &a.lease).await.unwrap();
        b.ship.tail_to_head().await.unwrap();
        assert!(b.meta.lookup(cold.ino, "y").unwrap().is_some());
        assert!(b.meta.lookup(hot.ino, "x").unwrap().is_none());

        // Abort path: journal only the src half, ship it, then the next
        // writer sees the orphan and appends abort. File stays at source.
        let f2 = a.meta.create(hot.ino, "z", 0o644, 0, 0).unwrap();
        a.ship.sync_one("p1", &k1).await.unwrap();
        a.meta
            .journal_on(
                "p1",
                &LogRecord::RenameXpartSrc {
                    txid: 99,
                    part: "p1".into(),
                    from_parent: hot.ino,
                    name: "z".into(),
                    ino: f2.ino,
                    time_ns: 3,
                },
            )
            .unwrap();
        a.ship.sync_one("p1", &k1).await.unwrap();
        // Replica B tails the src half (parks it) with no dst.
        b.ship.tail_to_head().await.unwrap();
        // Next write round on A (src holder) must abort.
        a.ship.sync_one("p1", &k1).await.unwrap();
        b.ship.tail_to_head().await.unwrap();
        assert!(
            a.meta.lookup(hot.ino, "z").unwrap().is_some(),
            "abort keeps the file at the source"
        );
        assert!(b.meta.lookup(hot.ino, "z").unwrap().is_some());
        assert!(b.meta.lookup(cold.ino, "z").unwrap().is_none());
        let _ = f;
    }

    /// Bootstrap with the applied-seq vector across 3 partitions.
    #[tokio::test]
    async fn bootstrap_vector_across_three_partitions() {
        let store = StdArc::new(InMemory::new());
        let mut a = node(&store, 1);
        let d1 = a.meta.mkdir(1, "a", 0o755, 0, 0).unwrap();
        let d2 = a.meta.mkdir(1, "b", 0o755, 0, 0).unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartSplit {
                    part: "p0".into(),
                    at_ino: d1.ino,
                    new_part: "p1".into(),
                    time_ns: 1,
                },
            )
            .unwrap();
        a.sync().await;
        a.meta
            .journal_on(
                "p0",
                &LogRecord::PartSplit {
                    part: "p0".into(),
                    at_ino: d2.ino,
                    new_part: "p2".into(),
                    time_ns: 2,
                },
            )
            .unwrap();
        a.sync().await;
        a.meta.create(d1.ino, "f1", 0o644, 0, 0).unwrap();
        a.meta.create(d2.ino, "f2", 0o644, 0, 0).unwrap();
        let mut k1 = LeaseKeeper::new(LeaseStore::new(store.clone(), "p1", LeaseMode::Cas), 1);
        let mut k2 = LeaseKeeper::new(LeaseStore::new(store.clone(), "p2", LeaseMode::Cas), 1);
        acquire_lease_for(&mut a.ship, &mut k1, "p1").await.unwrap();
        acquire_lease_for(&mut a.ship, &mut k2, "p2").await.unwrap();
        a.ship.sync_one("p1", &k1).await.unwrap();
        a.ship.sync_one("p2", &k2).await.unwrap();
        a.ship.checkpoint().await.unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("meta.db");
        bootstrap(&db, &LogStore::new(store.clone())).await.unwrap();
        let restored = SqliteMeta::open(&db).unwrap();
        let parts: Vec<String> = restored
            .partitions()
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(parts.contains(&"p0".into()));
        assert!(parts.contains(&"p1".into()));
        assert!(parts.contains(&"p2".into()));
        assert!(restored.lookup(d1.ino, "f1").unwrap().is_some());
        assert!(restored.lookup(d2.ino, "f2").unwrap().is_some());
    }
}
