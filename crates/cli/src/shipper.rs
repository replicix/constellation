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
use constellation_store_s3::{Lease, LeaseMode, LeaseStore, LeaseTag, LogStore};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
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

/// Default checkpoint cadence ratio: fire once shipped-log bytes since the
/// last checkpoint reach `ratio ×` the last snapshot's uncompressed size.
/// At 1.0 the checkpoint write traffic is bounded at ~1 byte per byte of
/// shipped log regardless of namespace size, replacing the count-only
/// cadence that copied the whole DB every 32 segments (54 GiB written to
/// protect a 0.17 GiB log on a 1.85M-file rsync — see plan 26).
const CHECKPOINT_RATIO: f64 = 1.0;

/// Env `CONSTELLATION_CHECKPOINT_RATIO`: shipped-log bytes ÷ last snapshot
/// bytes to fire a checkpoint. Values `<= 0` (or unparseable) fall back to
/// the default with a warning — a zero/negative ratio would checkpoint on
/// every segment once a baseline exists, the very amplification this
/// bounds. Read once at attach (like `autosplit`) so a test can drive it
/// without racing the process-global environment.
fn checkpoint_ratio() -> f64 {
    match std::env::var("CONSTELLATION_CHECKPOINT_RATIO") {
        Ok(raw) => match raw.parse::<f64>() {
            Ok(v) if v > 0.0 => v,
            _ => {
                tracing::warn!(
                    value = %raw,
                    "CONSTELLATION_CHECKPOINT_RATIO must be a positive number; \
                     falling back to {CHECKPOINT_RATIO}"
                );
                CHECKPOINT_RATIO
            }
        },
        Err(_) => CHECKPOINT_RATIO,
    }
}

/// The segment count is a *floor*: never checkpoint below `CHECKPOINT_EVERY`
/// shipped segments. Above it, a byte-proportional gate governs — fire once
/// `bytes_since_ckpt >= ratio × last_ckpt_bytes` — so the whole-DB copy is
/// paid for in proportion to the log it lets us truncate rather than on a
/// fixed segment count. `last_ckpt_bytes == 0` (no baseline yet: a fresh
/// mount, or a restart whose seed failed) leaves the count in sole charge,
/// exactly as before this plan. `min_interval` only ever further delays a
/// due checkpoint (opt-in, default off).
fn checkpoint_is_due(
    shipped_since_ckpt: u64,
    bytes_since_ckpt: u64,
    last_ckpt_bytes: u64,
    ratio: f64,
    since_last_ckpt: Option<std::time::Duration>,
    min_interval: std::time::Duration,
) -> bool {
    if shipped_since_ckpt < CHECKPOINT_EVERY {
        return false;
    }
    let bytes_ok =
        last_ckpt_bytes == 0 || (bytes_since_ckpt as f64) >= ratio * (last_ckpt_bytes as f64);
    if !bytes_ok {
        return false;
    }
    match since_last_ckpt {
        None => true,
        Some(elapsed) => elapsed >= min_interval,
    }
}
/// Max journal records per segment.
const SEGMENT_BATCH: usize = 10_000;

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
    meta: Arc<SqliteMeta>,
    log: LogStore,
    node_id: u64,
    lease_mode: LeaseMode,
    /// Per-partition stream state (next_seq, max_epoch, log handle).
    parts: HashMap<String, PartState>,
    shipped_since_ckpt: u64,
    /// Uncompressed bytes of log shipped since the last checkpoint. Drives
    /// the byte-proportional cadence gate (plan 26 Step 1).
    bytes_since_ckpt: u64,
    /// Uncompressed size of the last checkpoint's snapshot; the cadence
    /// baseline. Seeded from `LATEST` across restarts
    /// ([`Shipper::seed_checkpoint_baseline`]); `0` means "no baseline
    /// yet", leaving the segment count in sole charge of the first one.
    last_ckpt_bytes: u64,
    /// Checkpoint cadence ratio, resolved once at attach (see
    /// [`checkpoint_ratio`]).
    ckpt_ratio: f64,
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
            bytes_since_ckpt: 0,
            last_ckpt_bytes: 0,
            ckpt_ratio: checkpoint_ratio(),
            last_ckpt_at: None,
            skip_ship: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            spool: Arc::new(std::sync::Mutex::new(SpoolInfo {
                head_seq: head,
                ..Default::default()
            })),
            dir_ops: HashMap::new(),
            autosplit: part_autosplit(),
            last_ship_at: HashMap::new(),
            wanted_registered_at: HashMap::new(),
            held_tail_at: HashMap::new(),
            peers: constellation_net::Peers::disabled(),
            designations: None,
        })
    }

    /// Attach the P2P handle so shipped segments are announced to peers.
    pub fn set_peers(&mut self, peers: constellation_net::Peers) {
        self.peers = peers;
    }

    /// Seed the byte-proportional cadence baseline from the checkpoint that
    /// already exists in S3, so the first checkpoint after a restart fires
    /// on bytes shipped rather than on the segment count alone.
    ///
    /// Best effort: `attach_with_mode` is sync and has no runtime, so this
    /// is a separate async step run right after attach. A missing `LATEST`
    /// or any read error leaves `last_ckpt_bytes = 0`, which means the
    /// count trigger governs the first checkpoint — exactly the pre-plan-26
    /// behaviour, so a failure here is never worse than not seeding.
    pub async fn seed_checkpoint_baseline(&mut self) {
        match self.log.get_checkpoint_ref().await {
            Ok(Some(r)) => {
                self.last_ckpt_bytes = r.bytes;
                tracing::debug!(
                    seq = r.seq,
                    bytes = r.bytes,
                    "seeded checkpoint cadence baseline from S3"
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(error = %e, "checkpoint baseline seed failed; count trigger governs the first checkpoint");
            }
        }
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
            let held = self.held_partitions(leases);
            self.tail_all_except(&held).await?;
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
            self.consider_xpart_aborts(part, lease).await?;
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

    /// Tail every known partition except the ones in `held`. Applying a
    /// `part_split` can reveal a partition we did not know about, so this
    /// repeats until no new stream appears — otherwise `tail_to_head`
    /// would return while a freshly discovered child stream was still
    /// unread, which matters because lease takeover uses it as the "I
    /// have seen everything" witness.
    ///
    /// A held partition is still registered in `self.parts` (a split can
    /// reveal a child we will have to ship to); only its LIST is skipped.
    async fn tail_all_except(&mut self, held: &HashSet<String>) -> Result<()> {
        let mut seen: HashSet<String> = HashSet::new();
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
            if self.ship_part(&part, lease, batch).await? {
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
        self.ship_part(part, lease, batch).await
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
    async fn ship_part(
        &mut self,
        part: &str,
        lease: &LeaseKeeper,
        batch: constellation_meta::JournalBatch,
    ) -> Result<bool> {
        if self.skip_ship.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(false);
        }
        let Some(epoch) = lease.ship_epoch() else {
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
        let fits = records_within_cap(&records)?;
        let shipped_journal = fits.min(journaled);
        let shipped_atime = fits.saturating_sub(journaled);
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
        self.shipped_since_ckpt += 1;
        // `payload` is the uncompressed postcard envelope, consistent with
        // `snap.len()` (also uncompressed) as the baseline it is compared
        // against in the cadence gate.
        self.bytes_since_ckpt += payload.len() as u64;
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
                        let dst_journaled = self.meta.journal_has_xpart_dst(*txid)?;
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
            if self.meta.journal_has_xpart_dst(txid)? {
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
            self.bytes_since_ckpt,
            self.last_ckpt_bytes,
            self.ckpt_ratio,
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
        // The freshly written snapshot is the new cadence baseline.
        self.last_ckpt_bytes = snap.len() as u64;
        self.bytes_since_ckpt = 0;
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
            let held = self.held_partitions(leases);
            self.tail_all_except(&held).await?;
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
            // No key coordination on split: an E2E DEK is derived from the
            // master key (`E2eKeys::dek`), so every node computes the new
            // partition's key locally with no keyring write.
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
        // often than it can look cannot make it release any sooner. The
        // timestamp is taken per *attempt*, not per success — a CAS that
        // lost still means the object moved under us, and hammering it
        // from a blocked FUSE thread would only add request traffic.
        let cooldown = std::time::Duration::from_millis(
            constellation_store_s3::lease::lease_ttl_ms().max(2) / 2,
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

fn parent_of(meta: &SqliteMeta, ino: u64) -> Option<u64> {
    meta.parent_of(ino).ok().flatten()
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
    // Plan 25: a brand-new replica has never written local content. Any
    // `pending_upload` rows inherited from a (possibly poisoned) cluster
    // checkpoint are another node's obligation — foreign replay never
    // inserts into that table, so a single post-replay clear is enough.
    meta.clear_pending_uploads()
        .context("clearing inherited pending_upload rows after bootstrap")?;
    tracing::info!(from_seq, replayed, "bootstrapped metadata replica");
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

        // With no byte baseline (`last_ckpt_bytes == 0`) the byte gate is
        // inert and the segment count is in sole charge, exactly as before
        // plan 26. `bytes_since` and `ratio` must not matter here.
        let due = |shipped, since| checkpoint_is_due(shipped, 0, 0, 1.0, since, none);

        // Below the segment count, nothing triggers a checkpoint.
        assert!(!due(CHECKPOINT_EVERY - 1, None));
        assert!(!due(CHECKPOINT_EVERY - 1, Some(Duration::from_secs(3600))));

        // At the count, the default fires regardless of recency.
        assert!(due(CHECKPOINT_EVERY, None));
        assert!(due(CHECKPOINT_EVERY, Some(Duration::ZERO)));

        // A configured floor delays it, and only until the gap is met.
        let floor = Duration::from_secs(60);
        assert!(!checkpoint_is_due(
            CHECKPOINT_EVERY,
            0,
            0,
            1.0,
            Some(Duration::from_secs(59)),
            floor
        ));
        assert!(checkpoint_is_due(
            CHECKPOINT_EVERY,
            0,
            0,
            1.0,
            Some(Duration::from_secs(60)),
            floor
        ));
        // The first checkpoint of a mount has no gap to wait out.
        assert!(checkpoint_is_due(CHECKPOINT_EVERY, 0, 0, 1.0, None, floor));
    }

    /// Once a baseline exists, the segment count is only a floor: the
    /// checkpoint fires when shipped-log bytes reach `ratio ×` the last
    /// snapshot size, so the whole-DB copy is paid for in proportion to
    /// the log it lets us truncate.
    #[test]
    fn checkpoint_trigger_is_byte_proportional_once_seeded() {
        let none = Duration::ZERO;
        let baseline = 1_000_000u64;
        let due =
            |shipped, bytes, ratio| checkpoint_is_due(shipped, bytes, baseline, ratio, None, none);

        // At the count but a byte short of ratio 1.0 → not yet.
        assert!(!due(CHECKPOINT_EVERY, 999_999, 1.0));
        // Exactly the baseline → fire.
        assert!(due(CHECKPOINT_EVERY, 1_000_000, 1.0));
        // A smaller ratio fires earlier (half the baseline).
        assert!(due(CHECKPOINT_EVERY, 500_000, 0.5));
        assert!(!due(CHECKPOINT_EVERY, 499_999, 0.5));
        // The segment count remains a hard floor regardless of bytes: no
        // checkpoint below it even with a mountain of bytes shipped.
        assert!(!due(CHECKPOINT_EVERY - 1, 10_000_000, 1.0));
    }

    fn node(store: &StdArc<InMemory>, id: u64) -> Node {
        node_on(store.clone() as StdArc<dyn ObjectStore>, id)
    }

    /// `node`, but over any backing store — the request-counting decorator
    /// below is not an `InMemory`.
    fn node_on(store: StdArc<dyn ObjectStore>, id: u64) -> Node {
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

    /// A pre-plan-25 checkpoint that still embeds `pending_upload` must
    /// not poison a fresh bootstrap (plan 25 step 2).
    #[tokio::test]
    async fn bootstrap_clears_pending_from_poisoned_checkpoint() {
        use constellation_fs_core::ChunkHash;
        use rusqlite::Connection;

        let dir = tempfile::tempdir().unwrap();
        let writer_path = dir.path().join("writer.db");
        let (ino, manifest) = {
            let m = SqliteMeta::open(&writer_path).unwrap();
            m.set_node_prefix(1).unwrap();
            let f = m.create(1, "f", 0o644, 0, 0).unwrap();
            let h = ChunkHash::of(b"poison-chunk");
            m.set_manifest_dirty(f.ino, None, b"M", 1, &[h]).unwrap();
            assert_eq!(m.pending_upload_count().unwrap(), 1);
            (f.ino, m.manifest(f.ino).unwrap())
        };
        // Produce checkpoint bytes that still contain pending_upload
        // (bypass `snapshot()`'s strip): VACUUM INTO a sibling file.
        let poison_path = dir.path().join("poison.db");
        {
            let c = Connection::open(&writer_path).unwrap();
            c.execute(
                "VACUUM INTO ?1",
                rusqlite::params![poison_path.to_string_lossy()],
            )
            .unwrap();
        }
        let poison = SqliteMeta::open(&poison_path).unwrap();
        assert_eq!(poison.pending_upload_count().unwrap(), 1);
        drop(poison);
        // Re-VACUUM so the put_checkpoint payload is a single file with
        // pending still present (no dangling WAL).
        let payload_path = dir.path().join("payload.db");
        {
            let c = Connection::open(&poison_path).unwrap();
            c.execute(
                "VACUUM INTO ?1",
                rusqlite::params![payload_path.to_string_lossy()],
            )
            .unwrap();
        }
        let payload = std::fs::read(&payload_path).unwrap();

        let store: StdArc<dyn ObjectStore> = StdArc::new(InMemory::new());
        let log = LogStore::new(store);
        log.put_checkpoint(1, &payload).await.unwrap();

        let boot = dir.path().join("boot.db");
        bootstrap(&boot, &log).await.unwrap();
        let restored = SqliteMeta::open(&boot).unwrap();
        assert_eq!(
            restored.pending_upload_count().unwrap(),
            0,
            "bootstrap must clear inherited pending_upload"
        );
        let kept = restored.lookup(1, "f").unwrap().unwrap();
        assert_eq!(kept.ino, ino);
        assert_eq!(restored.manifest(ino).unwrap(), manifest);
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
}
