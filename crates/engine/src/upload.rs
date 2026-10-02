//! The chunk upload runtime: the durable pending-upload queue's drain
//! (`upload_dirty_chunks*`), its adaptive concurrency, the dedup ladder,
//! and the bookkeeping for chunks forwarded to (or from) another node
//! while still pending (`meta::store::remote`).
//!
//! Lived in `cli/src/main.rs` until plan 31 C3 moved it here unchanged:
//! the authority core's driver (`crate::authority_driver`) calls into it
//! on every round, so it is engine code, not CLI code.

use crate::{backend, fault, writeback};
use anyhow::{bail, Result};
use constellation_fs_core::cache::DiskCache;
use constellation_meta::Meta;
use constellation_store_s3::{ChunkStore, CompressionSetting};

/// Drain `Meta::pending_uploads()` — the durable not-yet-uploaded
/// set (plan 07 step 1) — rather than `DiskCache::dirty_chunks()`,
/// which cannot survive a crash (`DiskCache::rescan` legitimately marks
/// everything `Clean`; only the meta journal's transaction-coupled
/// table knows what still owes S3 a PUT).
///
/// A pending row whose chunk is missing from the local cache is
/// unrecoverable content (a torn-disk case, impossible on a clean crash
/// given the write ordering `flush_inode` uses): log loudly, leave the
/// row, and refuse rather than silently drop it — returning an error
/// here already makes every caller treat the round as failed and skip
/// shipping (see `run_managed_sync_round`'s existing epoch-propose-on-
/// failure path), which is exactly the "journal must wait" behavior.
use constellation_upload_concurrency::{AdaptiveConcurrency, ConcurrencyGate, ConcurrencyPermit};

/// Ceiling for the adaptive search and for a user-pinned override alike.
/// Bounds both real S3 concurrency and (via [`ConcurrencyGate`]) worst-case
/// pending-upload memory: at most this many chunk buffers are ever held
/// at once regardless of how many rows `pending_upload` has queued.
const UPLOAD_CONCURRENCY_HARD_MAX: usize = 128;
/// Plan 30 §M7: upload slots above the adaptive target that small inode
/// drains may use, and what counts as small. Without them an `fsync` of a
/// one-chunk marker file written during a write-back burst waited for
/// most of the burst's upload backlog (2.5 s of a 3.4 s pass on floci):
/// the pass queues up to the hard maximum of uploads on the gate, and a
/// release wakes every waiter at once.
const PRIORITY_UPLOAD_RESERVE: usize = 4;
const PRIORITY_DRAIN_CHUNKS: u64 = 4;

/// See docs/explanation/DESIGN.md §5b step 2 / `docs/plans/v1/done/08-p5b-streaming-writeback.md`.
/// A durable pending-upload queue (the metadata store's fjall
/// `pending_upload` partition) is drained by a bounded pool;
/// the pool costs two things once it exists (dedup-probe RTT and the
/// create-vs-overwrite decision), both handled by `put_mode` below.
///
/// Concurrency itself is adaptive by default
/// (`constellation_upload_concurrency::AdaptiveConcurrency`): a single
/// upload's latency is dominated by RTT to the bucket region, so a
/// client far from the bucket but sitting on a fat pipe (e.g. a home
/// connection in the EU against a `us-west-2` bucket) needs a lot more
/// parallelism than one on a thin or nearby link to fill that
/// bandwidth-delay product, and a fixed pool size tuned for one path is
/// wrong for the other. `CONSTELLATION_UPLOAD_CONCURRENCY` still pins a
/// fixed value for anyone who wants to opt out of the search entirely.
///
/// The policy and gate live in their own crate
/// (`crates/upload-concurrency`) so `bench/uploadbench` can drive the
/// exact production algorithm against a synthetic or live S3 target,
/// rather than a reimplementation that could drift from what ships here.
/// Plan 31 C8, `UploadMode::UnmeteredOnly`: while the host's network is
/// metered, the *opportunistic* uploads hold — the sync round's background
/// pass takes no new chunk, and a `back` close's forwarded chunks are not
/// handed to a peer. Every chunk stays exactly where write-back leaves it
/// (a `pending_upload` row and the cached bytes, both durable locally), and
/// the ship plan defers just the manifests that name one (plan 30 §M7's
/// deferral, the same as a burst still uploading). What is *not* held: an
/// explicit durability request — `fsync`/`O_SYNC`/`--fsync-mode s3`'s
/// inode drain, a barrier, a snapshot's forced publish, a lease handoff's,
/// an unmount's or a suspension's final flush — each needs its chunks in
/// S3 to answer at all, and waiting out a metered network there would hold
/// the authority core's one job slot (and the lease renewals behind it).
#[derive(Debug, Default)]
pub struct UploadHold {
    held: std::sync::atomic::AtomicBool,
    /// Deferrals because of the hold: a background pass skipped with
    /// chunks pending, or a chunk a running pass left behind.
    deferred: std::sync::atomic::AtomicU64,
    /// Times the hold was put on.
    engaged: std::sync::atomic::AtomicU64,
}

impl UploadHold {
    pub fn set(&self, held: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        if !self.held.swap(held, Relaxed) && held {
            self.engaged.fetch_add(1, Relaxed);
        }
    }

    pub fn is_held(&self) -> bool {
        self.held.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// `(deferrals, times engaged)`.
    pub fn counters(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.deferred.load(Relaxed), self.engaged.load(Relaxed))
    }

    fn note_deferred(&self) {
        self.deferred
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct UploadRuntime {
    pub gate: ConcurrencyGate,
    /// Plan 31 C8: `UploadMode::UnmeteredOnly`'s hold.
    pub hold: UploadHold,
    controller: Option<std::sync::Mutex<AdaptiveConcurrency>>,
    max_concurrency: usize,
    create_if_absent: bool,
    pub probe: std::sync::Mutex<writeback::ProbePolicy>,
    decisions: std::sync::atomic::AtomicU64,
    pub(crate) coop: Option<std::sync::Arc<crate::coop::Coop>>,
    pub existence: std::sync::Arc<crate::existence::Existence>,
    /// Chunks a drain is uploading right now. A sync round's drain and a
    /// flush's `drain_inode` overlap freely; without this, both read the
    /// same pending row and both PUT it (plan 30 §M11's
    /// `delegated-subtrees` measured 1.3–1.7 chunk PUTs per file). The
    /// second drain leaves the row to the first and waits for its ack.
    in_flight: std::sync::Mutex<std::collections::HashSet<constellation_fs_core::ChunkHash>>,
    /// Chunks smaller than this skip `Probe` (see [`Self::put_mode`]).
    probe_min_bytes: u64,
    /// Chunks this node named in a forwarded manifest while they were
    /// still pending here (a `back` close), and the nodes it forwarded to:
    /// each is told once the chunk is up (`meta::store::remote`).
    forwarded: std::sync::Mutex<
        std::collections::HashMap<
            constellation_fs_core::ChunkHash,
            std::collections::BTreeSet<u64>,
        >,
    >,
    /// Reports owed, per node: forwarded chunks now durable.
    durable_reports:
        std::sync::Mutex<std::collections::HashMap<u64, Vec<constellation_fs_core::ChunkHash>>>,
    /// Since when reports to a node have not been delivered (see
    /// [`Self::requeue_report`]).
    report_attempts: std::sync::Mutex<std::collections::HashMap<u64, std::time::Instant>>,
    /// When this node next checks S3 itself for a chunk another node
    /// forwarded as pending (the fallback when its report never comes),
    /// and the current backoff.
    remote_polls: std::sync::Mutex<
        std::collections::HashMap<
            constellation_fs_core::ChunkHash,
            (std::time::Instant, std::time::Duration),
        >,
    >,
    /// Chunks another node reported durable recently, kept for a minute:
    /// its report can overtake the forward that names them (the report
    /// and the forward travel on different streams), and a forward whose
    /// chunks were already reported must not await them.
    reported: std::sync::Mutex<
        std::collections::HashMap<constellation_fs_core::ChunkHash, std::time::Instant>,
    >,
    /// Pending chunks this node found at mount: a crash may have lost
    /// whom it forwarded them to (`forwarded` is in memory), so once up
    /// they are reported to every peer — a node that awaits one acks it,
    /// the rest ignore the report.
    inherited: std::sync::Mutex<std::collections::HashSet<constellation_fs_core::ChunkHash>>,
    /// EC2 finding 1: when a chunk PUT (or existence probe) last
    /// completed on this node, unix ms — how a drain that is taking long
    /// tells a slow upload (progress) from an unreachable S3 (none), and
    /// only the latter hands its chunks to a peer.
    last_put_ms: std::sync::atomic::AtomicI64,
    /// EC2 finding 1: chunk handoffs (`authority_driver::ChunkHandoff`).
    pub handoff: HandoffStats,
    /// Upload passes started and chunk PUTs attempted (a pass that only
    /// finds the chunk durable, or only awaits it, attempts none): how a
    /// test tells "this `fsync` uploaded nothing" from "it was quick".
    pub(crate) passes: std::sync::atomic::AtomicU64,
    pub(crate) put_attempts: std::sync::atomic::AtomicU64,
}

/// The `durable_reports` key of a report owed to every peer.
pub(crate) const REPORT_TO_ALL: u64 = 0;

/// How long an undelivered durable report is retried.
const REPORT_RETRY: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a durable report is remembered for a forward it overtook.
const REPORTED_KEEP: std::time::Duration = std::time::Duration::from_secs(60);

/// First S3 check of a chunk another node forwarded as pending, if its
/// report has not come by then; the checks back off to [`REMOTE_POLL_MAX`].
const REMOTE_POLL_FIRST: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_POLL_MAX: std::time::Duration = std::time::Duration::from_secs(16);

/// `CONSTELLATION_REMOTE_CHUNK_WAIT_S` (default 60): how long a pass that
/// must leave nothing pending (a barrier, a forced publish, an unmount's
/// final flush) — and a reader that needs the bytes — waits for chunks
/// another node forwarded as pending before giving up on them.
pub fn remote_chunk_wait() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("CONSTELLATION_REMOTE_CHUNK_WAIT_S")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60),
    )
}

/// The chunks a forwarded `SetManifest` names that are still pending here
/// (`meta::store::remote`): empty for any other op, and for a close that
/// uploaded first (`--write-mode through`). A spilled manifest's chunk
/// list comes from the local cache, where this node's flush put it.
pub(crate) fn forwarded_pending_chunks(
    meta: &Meta,
    cache: &DiskCache,
    op: &constellation_meta::MutateOp,
) -> Vec<constellation_fs_core::ChunkHash> {
    let constellation_meta::MutateOp::SetManifest { ino, manifest, .. } = op else {
        return Vec::new();
    };
    let mut named = std::collections::BTreeSet::new();
    let spilled = match constellation_fs_core::Manifest::decode(manifest).map(|m| m.chunks) {
        Ok(constellation_fs_core::ChunkInfo::Inline(chunks)) => {
            named.extend(chunks.into_values());
            None
        }
        Ok(constellation_fs_core::ChunkInfo::Spilled(blob)) => Some(blob),
        // Undecodable: whatever is pending for the inode (below).
        Err(_) => Some(constellation_fs_core::ChunkHash([0; 32])),
    };
    if let Some(blob) = spilled {
        named.insert(blob);
        if let Ok(Some(bytes)) = cache.get_verified(&blob) {
            if let Ok(list) = constellation_fs_core::manifest::decode_chunk_list(&bytes) {
                named.extend(list.into_values());
            }
        }
        // The list may not be readable here (the blob uploaded and
        // evicted): every chunk still pending for the inode counts too.
        // Rare (files past the inline limit) and a local scan.
        if let Ok(rows) = meta.pending_uploads() {
            named.extend(rows.into_iter().filter(|(_, i)| i == ino).map(|(h, _)| h));
        }
    }
    named
        .into_iter()
        .filter(|hash| meta.upload_pending_for_hash(hash).unwrap_or(true))
        .collect()
}

/// `CONSTELLATION_PROBE_MIN_BYTES` (default 256 KiB; `0` lets every
/// chunk probe): the size below which an upload always uses a
/// conditional create instead of a HEAD-first probe. 256 KiB is about
/// 2 ms at 1 Gbit/s — less than any S3 round trip it saves.
fn probe_min_bytes() -> u64 {
    std::env::var("CONSTELLATION_PROBE_MIN_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(256 * 1024)
}

/// EC2 finding 1: this node's chunk handoffs, for `status`.
#[derive(Default)]
pub struct HandoffStats {
    pub sent: std::sync::atomic::AtomicU64,
    pub ok: std::sync::atomic::AtomicU64,
    pub chunks: std::sync::atomic::AtomicU64,
    pub accepted: std::sync::atomic::AtomicU64,
    /// When a drain last needed a handoff (unix ms): while recent, and
    /// uploads still make no progress, the next drain hands off at once.
    pub(crate) last_needed_ms: std::sync::atomic::AtomicI64,
    /// A round's handoff of forwarded chunks is in flight.
    pub(crate) round_busy: std::sync::atomic::AtomicBool,
}

/// Releases a drain's claim on its chunks when it ends, however it ends.
struct InFlightClaim<'a> {
    upload: &'a UploadRuntime,
    hashes: Vec<constellation_fs_core::ChunkHash>,
}

impl Drop for InFlightClaim<'_> {
    fn drop(&mut self) {
        let mut in_flight = self.upload.in_flight.lock().unwrap();
        for hash in &self.hashes {
            in_flight.remove(hash);
        }
    }
}

impl UploadRuntime {
    pub fn new(
        create_if_absent: bool,
        coop: Option<std::sync::Arc<crate::coop::Coop>>,
        existence: std::sync::Arc<crate::existence::Existence>,
    ) -> Self {
        let max = std::env::var("CONSTELLATION_UPLOAD_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(UPLOAD_CONCURRENCY_HARD_MAX)
            .clamp(1, UPLOAD_CONCURRENCY_HARD_MAX);
        let fixed = std::env::var("CONSTELLATION_UPLOAD_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|n| n.clamp(1, max));
        let (initial, controller) = match fixed {
            Some(n) => (n, None),
            // Start conservatively (like TCP slow start) and let the
            // controller climb; an aggressive initial guess on a
            // constrained link just causes early retries/backoff.
            None => (
                4.min(max),
                Some(std::sync::Mutex::new(AdaptiveConcurrency::new(
                    4.min(max),
                    1,
                    max,
                ))),
            ),
        };
        debug_assert!(
            controller
                .as_ref()
                .is_none_or(|c| c.lock().unwrap().current() == initial),
            "gate and controller must start in agreement"
        );
        Self {
            gate: ConcurrencyGate::new(initial),
            controller,
            max_concurrency: max,
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop,
            existence,
            in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            probe_min_bytes: probe_min_bytes(),
            forwarded: Default::default(),
            durable_reports: Default::default(),
            report_attempts: Default::default(),
            remote_polls: Default::default(),
            reported: Default::default(),
            inherited: Default::default(),
            last_put_ms: std::sync::atomic::AtomicI64::new(0),
            handoff: HandoffStats::default(),
            passes: Default::default(),
            put_attempts: Default::default(),
            hold: UploadHold::default(),
        }
    }

    /// EC2 finding 1: whether this node's S3 path has made no progress for
    /// `for_ms`: no chunk PUT and no S3 request of any kind has succeeded
    /// (`backend::last_s3_completion_ms`) in that long.
    pub(crate) fn uploads_stalled(&self, for_ms: i64) -> bool {
        let last = self
            .last_put_ms
            .load(std::sync::atomic::Ordering::Relaxed)
            .max(backend::last_s3_completion_ms());
        constellation_store_s3::lease::now_unix_ms() - last >= for_ms
    }

    /// `hashes` went out in a manifest forwarded to `to` while still
    /// pending here.
    pub(crate) fn note_forwarded(&self, hashes: &[constellation_fs_core::ChunkHash], to: u64) {
        if hashes.is_empty() {
            return;
        }
        let mut forwarded = self.forwarded.lock().unwrap();
        for hash in hashes {
            forwarded.entry(*hash).or_default().insert(to);
        }
    }

    /// Another node reported `hashes` durable.
    pub(crate) fn note_reported(&self, hashes: &[constellation_fs_core::ChunkHash]) {
        let now = std::time::Instant::now();
        let mut reported = self.reported.lock().unwrap();
        reported.retain(|_, at| now.duration_since(*at) < REPORTED_KEEP);
        for hash in hashes {
            reported.insert(*hash, now);
        }
    }

    /// `hashes` without the ones another node reported durable recently.
    pub(crate) fn not_reported(
        &self,
        hashes: Vec<constellation_fs_core::ChunkHash>,
    ) -> Vec<constellation_fs_core::ChunkHash> {
        let reported = self.reported.lock().unwrap();
        if reported.is_empty() {
            return hashes;
        }
        hashes
            .into_iter()
            .filter(|hash| !reported.contains_key(hash))
            .collect()
    }

    /// The pending chunks found at mount (see `inherited`).
    pub fn inherit(&self, hashes: impl IntoIterator<Item = constellation_fs_core::ChunkHash>) {
        self.inherited.lock().unwrap().extend(hashes);
    }

    /// Whether any chunk forwarded as pending is still owed a report.
    pub(crate) fn has_forwarded(&self) -> bool {
        !self.forwarded.lock().unwrap().is_empty()
    }

    /// Chunks forwarded as pending that are still pending here, with the
    /// nodes they were forwarded to (EC2 finding 1: what a round hands to
    /// a peer when this node's S3 path is down).
    pub(crate) fn forwarded_pending(&self) -> Vec<(constellation_fs_core::ChunkHash, Vec<u64>)> {
        self.forwarded
            .lock()
            .unwrap()
            .iter()
            .map(|(h, nodes)| (*h, nodes.iter().copied().collect()))
            .collect()
    }

    /// `hash` is up: owe its report to every node it was forwarded to
    /// (to every peer, for one found pending at mount).
    pub(crate) fn note_up(&self, hash: &constellation_fs_core::ChunkHash) {
        let nodes = self.forwarded.lock().unwrap().remove(hash);
        let inherited = self.inherited.lock().unwrap().remove(hash);
        let mut reports = self.durable_reports.lock().unwrap();
        if inherited {
            reports.entry(REPORT_TO_ALL).or_default().push(*hash);
        }
        for node in nodes.into_iter().flatten() {
            reports.entry(node).or_default().push(*hash);
        }
    }

    /// A report that could not go out (its node unreachable, or no peer
    /// known yet right after a restart): owed again at the next pass, for
    /// up to [`REPORT_RETRY`] of failures in a row; past that the
    /// recipient's own S3 check covers it.
    pub(crate) fn requeue_report(&self, node: u64, hashes: Vec<constellation_fs_core::ChunkHash>) {
        let mut attempts = self.report_attempts.lock().unwrap();
        let since = *attempts.entry(node).or_insert_with(std::time::Instant::now);
        if since.elapsed() > REPORT_RETRY {
            attempts.remove(&node);
            return;
        }
        self.durable_reports
            .lock()
            .unwrap()
            .entry(node)
            .or_default()
            .extend(hashes);
    }

    pub(crate) fn report_delivered(&self, node: u64) {
        self.report_attempts.lock().unwrap().remove(&node);
    }

    /// The reports owed, per node (taken: see [`Self::requeue_report`]).
    pub(crate) fn take_durable_reports(
        &self,
    ) -> std::collections::HashMap<u64, Vec<constellation_fs_core::ChunkHash>> {
        std::mem::take(&mut *self.durable_reports.lock().unwrap())
    }

    /// The reports owed for `hashes` only, per node (taken); the rest
    /// stay owed for the background passes. An `fsync`'s drain sends its
    /// own file's reports before it returns, not every peer's (plan 39b).
    pub(crate) fn take_durable_reports_of(
        &self,
        hashes: &std::collections::HashSet<constellation_fs_core::ChunkHash>,
    ) -> std::collections::HashMap<u64, Vec<constellation_fs_core::ChunkHash>> {
        let mut owed = self.durable_reports.lock().unwrap();
        let mut taken = std::collections::HashMap::new();
        owed.retain(|node, owed| {
            let (mine, rest): (Vec<_>, Vec<_>) = owed.drain(..).partition(|h| hashes.contains(h));
            if !mine.is_empty() {
                taken.insert(*node, mine);
            }
            *owed = rest;
            !owed.is_empty()
        });
        taken
    }

    /// Whether this node should check S3 now for `hash`, which another
    /// node forwarded as pending; schedules the next check if so.
    fn remote_poll_due(&self, hash: &constellation_fs_core::ChunkHash) -> bool {
        let now = std::time::Instant::now();
        let mut polls = self.remote_polls.lock().unwrap();
        match polls.get_mut(hash) {
            None => {
                polls.insert(*hash, (now + REMOTE_POLL_FIRST, REMOTE_POLL_FIRST));
                false
            }
            Some((next, delay)) if now >= *next => {
                *delay = (*delay * 2).min(REMOTE_POLL_MAX);
                *next = now + *delay;
                true
            }
            Some(_) => false,
        }
    }

    pub(crate) fn forget_remote_poll(&self, hash: &constellation_fs_core::ChunkHash) {
        self.remote_polls.lock().unwrap().remove(hash);
    }

    /// Hard ceiling on real concurrency and thus on worst-case pending-
    /// upload memory: at most this many chunk buffers are held at once,
    /// however many rows `pending_upload` has queued.
    fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }

    async fn permit(&self) -> ConcurrencyPermit<'_> {
        self.gate.acquire().await
    }

    /// Plan 30 §M7: a permit for a small inode drain (an `fsync` or a
    /// write-through close of a small file) that must not wait behind the
    /// round's bulk upload pass — see `ConcurrencyGate::acquire_priority`.
    async fn permit_priority(&self) -> ConcurrencyPermit<'_> {
        self.gate.acquire_priority(PRIORITY_UPLOAD_RESERVE).await
    }

    fn record_success(&self, bytes: u64, latency: std::time::Duration, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_success(now, bytes, latency);
        if new_target != self.gate.target() {
            tracing::debug!(
                concurrency = new_target,
                previous = self.gate.target(),
                "adaptive upload concurrency adjusted"
            );
            self.gate.set_target(new_target);
        }
    }

    fn record_error(&self, now: std::time::Instant) {
        let Some(controller) = &self.controller else {
            return;
        };
        let new_target = controller.lock().unwrap().on_error(now);
        let previous = self.gate.target();
        if new_target != previous {
            tracing::debug!(
                concurrency = new_target,
                previous,
                "upload failed; backing off adaptive concurrency"
            );
            self.gate.set_target(new_target);
        } else {
            tracing::debug!(
                concurrency = new_target,
                "upload failure coalesced with current congestion episode"
            );
        }
    }

    /// The dedup-ladder rung for a chunk of `len` bytes.
    ///
    /// A small chunk probes only on a positive hint (a peer's digest or
    /// this node's existence cache says the bytes are already there): the
    /// adaptive policy's guess alone does not make it. `Probe` spends a
    /// HEAD to save sending the body on a hit, and a miss then costs a
    /// second serialized round trip (HEAD, then PUT). Below
    /// [`probe_min_bytes`] the body is cheaper than that round trip, so a
    /// conditional create (one round trip, hit or miss) is never slower —
    /// the OVH run's non-owner paid HEAD + PUT on every small file once
    /// its ladder had switched to `Probe`.
    pub(crate) fn put_mode(
        &self,
        hash: &constellation_fs_core::ChunkHash,
        len: usize,
    ) -> constellation_store_s3::ChunkPutMode {
        let small = self.create_if_absent && (len as u64) < self.probe_min_bytes;
        if self.existence.peer_hints_enabled()
            && self
                .coop
                .as_ref()
                .is_some_and(|coop| coop.peer_digest_contains(hash))
        {
            self.existence.note_peer_hint();
            return constellation_store_s3::ChunkPutMode::Probe;
        }
        if self.existence.contains(hash) {
            return constellation_store_s3::ChunkPutMode::Probe;
        }
        if small {
            return constellation_store_s3::ChunkPutMode::Create;
        }
        let n = self
            .decisions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.probe.lock().unwrap().enabled() || n.is_multiple_of(16) {
            constellation_store_s3::ChunkPutMode::Probe
        } else if self.create_if_absent {
            constellation_store_s3::ChunkPutMode::Create
        } else {
            constellation_store_s3::ChunkPutMode::Overwrite
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn for_test(create_if_absent: bool) -> Self {
        Self::new(
            create_if_absent,
            None,
            crate::existence::Existence::new(1024, false, None),
        )
    }

    /// Test helper: pin a fixed concurrency (no adaptive controller),
    /// bypassing environment variables so tests are hermetic.
    #[cfg(test)]
    fn for_test_fixed(create_if_absent: bool, concurrency: usize) -> Self {
        Self {
            gate: ConcurrencyGate::new(concurrency),
            controller: None,
            max_concurrency: concurrency.max(1),
            create_if_absent,
            probe: std::sync::Mutex::new(writeback::ProbePolicy::default()),
            decisions: std::sync::atomic::AtomicU64::new(0),
            coop: None,
            existence: crate::existence::Existence::new(1024, false, None),
            in_flight: std::sync::Mutex::new(std::collections::HashSet::new()),
            probe_min_bytes: probe_min_bytes(),
            forwarded: Default::default(),
            durable_reports: Default::default(),
            report_attempts: Default::default(),
            remote_polls: Default::default(),
            reported: Default::default(),
            inherited: Default::default(),
            last_put_ms: std::sync::atomic::AtomicI64::new(0),
            handoff: HandoffStats::default(),
            passes: Default::default(),
            put_attempts: Default::default(),
            hold: UploadHold::default(),
        }
    }
}

/// Plan 30 §M4: [`upload_dirty_chunks_report`] for a caller that needs
/// every pending chunk durable — an inode drain before a replay or an
/// `fsync`, a handoff's or an unmount's final flush — so an unrecoverable
/// chunk is still an error here.
pub async fn upload_dirty_chunks(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
) -> Result<()> {
    let report =
        upload_dirty_chunks_report(cache, meta, store, compression, upload, only_ino, only_part)
            .await?;
    if let Some((hash, _)) = report.missing.first() {
        bail!("pending upload chunk {hash} missing from local cache");
    }
    Ok(())
}

/// Why an `fsync`'s drain of one inode did not leave it durable although
/// no request failed (plan 39b). Never a success: the rows stay pending.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DrainShortfall {
    /// Pending chunks of the inode that are neither in the local cache nor
    /// in S3: the content is gone, and no wait brings it back. Permanent
    /// (`EIO`; the classifier matches "missing from local cache"), and
    /// every later `fsync` of the inode fails the same way while the rows
    /// remain — until the file is rewritten or removed.
    #[error(
        "{count} pending upload chunk(s) of inode {ino} (first {hash}) missing from local cache \
         and not in S3"
    )]
    Lost {
        ino: constellation_fs_core::Ino,
        hash: constellation_fs_core::ChunkHash,
        count: usize,
    },
    /// Chunks of the inode another node forwarded as pending (a `back`
    /// close there, `meta::store::remote`) are not in S3 yet: that node
    /// has them and reports them once up. Transient: the `fsync` keeps
    /// waiting under plan 39's policy.
    #[error(
        "{awaiting} chunk(s) of inode {ino} another node forwarded as pending are not in S3 yet \
         (waited {waited_s} s)"
    )]
    AwaitingRemote {
        ino: constellation_fs_core::Ino,
        awaiting: u64,
        waited_s: u64,
    },
}

/// What one upload pass found it cannot upload.
#[derive(Debug, Default)]
pub(crate) struct UploadReport {
    /// Pending `(chunk, ino)` rows whose chunk is gone from the local
    /// cache: unrecoverable content (plan 30 §M4).
    pub(crate) missing: Vec<(constellation_fs_core::ChunkHash, constellation_fs_core::Ino)>,
    /// Chunks another node forwarded as pending that are not in S3 yet
    /// (`meta::store::remote`): not lost, only not up yet.
    pub(crate) awaiting: u64,
}

/// Upload every pending chunk (or one inode's). A chunk missing from the
/// local cache is no longer an error (plan 30 §M4 item 2): it is reported,
/// and recorded in `Meta::note_unrecoverable_chunks`, so the ship plan
/// holds back just the records that need it and everything else ships.
/// Any other failure (S3 unreachable, a PUT that keeps failing) is still
/// an error for the round.
pub(crate) async fn upload_dirty_chunks_report(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
) -> Result<UploadReport> {
    upload_dirty_chunks_pass(
        cache,
        meta,
        store,
        compression,
        upload,
        only_ino,
        only_part,
        false,
        0,
    )
    .await
}

/// The sync round's opportunistic pass: [`upload_dirty_chunks_report`],
/// except that it takes no new chunk while the upload hold is on
/// ([`UploadHold`]).
pub(crate) async fn upload_dirty_chunks_background(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
) -> Result<UploadReport> {
    if upload.hold.is_held() {
        let pending = meta.pending_upload_count().unwrap_or(0);
        if pending > 0 {
            upload.hold.note_deferred();
            tracing::debug!(pending, "chunk uploads held (metered network)");
        }
        return Ok(UploadReport::default());
    }
    upload_dirty_chunks_pass(cache, meta, store, compression, upload, None, None, true, 0).await
}

/// Plan 30 §M9 × §M4: enroll the chunk lists of adopted spilled
/// manifests (`Meta::adopted_spills`) as pending uploads, from the list
/// blob in the local cache or in S3. A blob that is in neither stays a
/// pending row of its own (this pass then records it unrecoverable, which
/// holds the manifest), and its mark keeps the manifest deferred until a
/// later pass can read it.
async fn expand_adopted_spills(cache: &DiskCache, meta: &Meta, store: &ChunkStore) -> Result<()> {
    for (ino, blob) in meta.adopted_spills()? {
        // Verified: the chunk hashes read out of this list become pending
        // upload rows, and a silently rotted list blob would enroll
        // garbage hashes (plan 38 §2.3 — see `DiskCache::get_verified`).
        let bytes = match cache.get_verified(&blob)? {
            Some(bytes) => bytes,
            None => match store.get_chunk(&blob).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::debug!(%error, ino, %blob, "adopted chunk list not readable yet");
                    continue;
                }
            },
        };
        let chunks: Vec<constellation_fs_core::ChunkHash> =
            match constellation_fs_core::manifest::decode_chunk_list(&bytes) {
                Ok(list) => list.into_values().collect(),
                Err(error) => {
                    tracing::warn!(%error, ino, %blob, "adopted chunk list undecodable; left held");
                    continue;
                }
            };
        tracing::info!(
            ino,
            chunks = chunks.len(),
            "enrolled an adopted manifest's chunk list for its durability check"
        );
        meta.expand_adopted_spill(ino, &blob, &chunks)?;
    }
    Ok(())
}

/// One pass of [`upload_dirty_chunks_report`]: `depth` counts the passes
/// a row another drain had claimed sent this one back for. `background`:
/// the upload hold stops it taking new chunks mid-pass.
#[allow(clippy::too_many_arguments)]
async fn upload_dirty_chunks_pass(
    cache: &DiskCache,
    meta: &Meta,
    store: &ChunkStore,
    compression: CompressionSetting,
    upload: &UploadRuntime,
    only_ino: Option<constellation_fs_core::Ino>,
    only_part: Option<&str>,
    background: bool,
    depth: u8,
) -> Result<UploadReport> {
    use futures::StreamExt;
    upload
        .passes
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if only_ino.is_none() {
        expand_adopted_spills(cache, meta, store).await?;
    }
    let mut grouped: std::collections::HashMap<
        constellation_fs_core::ChunkHash,
        Vec<constellation_fs_core::Ino>,
    > = std::collections::HashMap::new();
    // An inode's drain (an `fsync`'s) reads just that inode's rows through
    // the by-inode mirror: O(its rows), not O(the whole queue).
    let rows = match only_ino {
        Some(ino) => meta
            .pending_uploads_for_ino(ino)?
            .into_iter()
            .map(|hash| (hash, ino))
            .collect(),
        None => meta.pending_uploads()?,
    };
    for (hash, ino) in rows {
        if let Some(wanted) = only_part {
            if wanted != "p0" {
                continue;
            }
        }
        grouped.entry(hash).or_default().push(ino);
    }
    // Rows another drain is uploading right now are its; this one waits
    // for their acks below instead of uploading them again.
    let mut deferred: Vec<constellation_fs_core::ChunkHash> = Vec::new();
    {
        let mut in_flight = upload.in_flight.lock().unwrap();
        grouped.retain(|hash, _| {
            if in_flight.contains(hash) {
                deferred.push(*hash);
                false
            } else {
                in_flight.insert(*hash);
                true
            }
        });
    }
    let _claim = InFlightClaim {
        upload,
        hashes: grouped.keys().copied().collect(),
    };
    let total = grouped.len() as u64;
    let priority = only_ino.is_some() && total <= PRIORITY_DRAIN_CHUNKS;
    if total > 0 {
        tracing::debug!(
            pending_chunks = total,
            concurrency = upload.gate.target(),
            max_concurrency = upload.max_concurrency(),
            "uploading pending chunks"
        );
    }
    // Materialize at most the configured maximum number of upload
    // futures. Each one reads bytes only after winning the adaptive gate,
    // so both future state and chunk buffers stay independent of a backlog
    // that may contain tens of thousands of rows.
    //
    // Missing-cache rows (torn disk, or a poisoned inherited backlog that
    // self-heal missed) must still fail the round so the journal does not
    // ship — but logging ERROR once per hash per round produced multi-GB
    // logs (plan 25). Count them and emit a single summary below.
    let missing_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let missing_sample = std::sync::Arc::new(std::sync::Mutex::new(
        None::<constellation_fs_core::ChunkHash>,
    ));
    let missing_rows = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
        constellation_fs_core::ChunkHash,
        constellation_fs_core::Ino,
    )>::new()));
    let awaiting = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let in_flight = futures::stream::iter(grouped.into_iter().map(|(hash, inos)| {
        let missing_count = missing_count.clone();
        let missing_sample = missing_sample.clone();
        let missing_rows = missing_rows.clone();
        let awaiting = awaiting.clone();
        async move {
            let _permit = if priority {
                upload.permit_priority().await
            } else {
                upload.permit().await
            };
            if background && upload.hold.is_held() {
                // The network turned metered mid-pass: leave the row.
                upload.hold.note_deferred();
                return Ok(None);
            }
            if fault::lose_chunk(&hash) {
                tracing::warn!(%hash, "fault injection: dropping a pending chunk from the cache");
                let _ = cache.remove(&hash);
            }
            // `get_verified`, not `get`: these bytes are about to become
            // `chunk/<hash>` in the bucket, so the local copy is hashed even
            // under `--cache-verify admit` (plan 38 §2.3 trusts the local
            // copy for *readers*, who refetch; a wrong-content object is
            // permanent and poisons the chunk for every node). A corrupt
            // copy is dropped and the row takes the path below.
            let Some(data) = cache.get_verified(&hash)? else {
                // Another node forwarded a manifest naming this chunk while
                // it was still uploading there (`meta::store::remote`): it
                // reports it once it is up. Until then it is awaited, not
                // lost; S3 is checked here only now and then, in case the
                // report never comes.
                let remote = inos
                    .iter()
                    .any(|ino| meta.remote_chunk(&hash, *ino).ok().flatten().is_some());
                if remote {
                    if upload.remote_poll_due(&hash) && store.chunk_durable(&hash).await? {
                        tracing::debug!(%hash, "a forwarded chunk is in S3; acknowledged");
                        return Ok(Some((
                            hash,
                            inos,
                            constellation_store_s3::ChunkPutMode::Create,
                            true,
                        )));
                    }
                    awaiting.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return Ok(None);
                }
                // Not in the cache is not the same as lost: the content
                // may already be durable in the bucket. The common way
                // there: a same-content writer on this node uploaded it
                // and the chunk was demoted to clean (and then evicted)
                // before this row's writer had enrolled it — demotion
                // checks for pending rows, and this one did not exist
                // yet. Content addressing makes that upload this row's
                // too, exactly as a `Probe` HEAD hit would: acknowledge
                // it instead of holding the writer's records back.
                if store.chunk_durable(&hash).await? {
                    tracing::info!(
                        %hash,
                        "pending chunk not in the local cache but durable in S3; acknowledged"
                    );
                    return Ok(Some((
                        hash,
                        inos,
                        constellation_store_s3::ChunkPutMode::Create,
                        true,
                    )));
                }
                missing_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                {
                    let mut sample = missing_sample.lock().unwrap();
                    if sample.is_none() {
                        *sample = Some(hash);
                    }
                }
                missing_rows
                    .lock()
                    .unwrap()
                    .extend(inos.iter().map(|ino| (hash, *ino)));
                return Ok(None);
            };
            let bytes = data.len() as u64;
            let mode = upload.put_mode(&hash, data.len());
            let mut last = None;
            let started = std::time::Instant::now();
            for attempt in 0..3 {
                upload
                    .put_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                match store.put_chunk_mode(&hash, &data, compression, mode).await {
                    Ok(result) => {
                        upload.last_put_ms.store(
                            constellation_store_s3::lease::now_unix_ms(),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                        let now = std::time::Instant::now();
                        // A Probe hit only performed HEAD; counting the
                        // chunk's logical bytes as uploaded would report
                        // impossible goodput and drive concurrency upward
                        // during deduplicated workloads.
                        if !result.existed {
                            upload.record_success(bytes, now.duration_since(started), now);
                        }
                        return Ok(Some((hash, inos, mode, result.existed)));
                    }
                    Err(error) => last = Some(error),
                }
                if attempt < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
            upload.record_error(std::time::Instant::now());
            Err(anyhow::anyhow!("upload {hash} failed: {}", last.unwrap()))
        }
    }))
    .buffer_unordered(upload.max_concurrency());
    futures::pin_mut!(in_flight);
    let mut first_error = None;
    let mut completed = 0u64;
    let mut last_progress = std::time::Instant::now();
    let progress_interval = std::env::var("CONSTELLATION_UPLOAD_PROGRESS_INTERVAL_S")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| std::time::Duration::from_secs(10));
    while let Some(result) = in_flight.next().await {
        match result {
            Ok(None) => {}
            Ok(Some((hash, inos, mode, existed))) => {
                upload.existence.insert(&hash);
                if mode == constellation_store_s3::ChunkPutMode::Probe {
                    upload.probe.lock().unwrap().record(existed);
                }
                for ino in inos {
                    meta.ack_upload(&hash, ino)?;
                }
                upload.note_up(&hash);
                upload.forget_remote_poll(&hash);
                if !meta.upload_pending_for_hash(&hash)? {
                    cache.set_state(&hash, constellation_fs_core::cache::ChunkState::Clean);
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        completed += 1;
        // Rows only awaited (another node's upload) are not progress:
        // a pass of nothing but those would log this every round.
        let awaited_only =
            completed == total && awaiting.load(std::sync::atomic::Ordering::Relaxed) >= total;
        if total > 0
            && !awaited_only
            && (completed == total || last_progress.elapsed() >= progress_interval)
        {
            tracing::info!(
                completed,
                total,
                concurrency = upload.gate.target(),
                "pending chunk upload progress"
            );
            last_progress = std::time::Instant::now();
        }
    }
    let missing = missing_count.load(std::sync::atomic::Ordering::Relaxed);
    let missing_rows = std::mem::take(&mut *missing_rows.lock().unwrap());
    if missing > 0 {
        let sample = *missing_sample.lock().unwrap();
        tracing::warn!(
            missing_pending_chunks = missing,
            sample_hash = ?sample,
            "pending upload chunks missing from local cache (unrecoverable content); \
             leaving the pending rows and holding back only the records that need them"
        );
    }
    // A full pass sees every pending row, so its list replaces the
    // recorded set (a chunk that turned up again stops poisoning); a
    // limited pass only adds to it.
    meta.note_unrecoverable_chunks(&missing_rows, only_ino.is_none())?;
    if let Some(error) = first_error {
        return Err(error);
    }
    if total > 0 {
        tracing::debug!(uploaded = total, "pending chunk upload complete");
    }
    // The chunks another drain claimed: this pass is complete only once
    // they are acked too (a write-through flush must not name a hash S3
    // lacks). A claim that ends with the row still pending (the other
    // drain failed, or found the chunk missing) leaves the row to this
    // pass: it goes round again and claims it itself, so a missing
    // chunk is reported here (M4's held records) and a transient error
    // there is retried here, never turned into this caller's error.
    let mut unclaimed_pending = false;
    for hash in deferred {
        let started = std::time::Instant::now();
        loop {
            if !meta.upload_pending_for_hash(&hash)? {
                break;
            }
            let claimed = upload.in_flight.lock().unwrap().contains(&hash);
            if !claimed {
                unclaimed_pending = true;
                break;
            }
            if started.elapsed() > std::time::Duration::from_secs(120) {
                anyhow::bail!("upload {hash} still in flight elsewhere after 120 s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
    if unclaimed_pending {
        if depth >= 3 {
            anyhow::bail!("pending uploads left behind by concurrent drains after 4 passes");
        }
        drop(_claim);
        return Box::pin(upload_dirty_chunks_pass(
            cache,
            meta,
            store,
            compression,
            upload,
            only_ino,
            only_part,
            background,
            depth + 1,
        ))
        .await;
    }
    Ok(UploadReport {
        missing: missing_rows,
        awaiting: awaiting.load(std::sync::atomic::Ordering::Relaxed),
    })
}

/// Plan 07's `pending_upload`-driven regression tests for
/// `upload_dirty_chunks`: the durable not-yet-uploaded set must survive
/// a crash even though `DiskCache::rescan` legitimately reports every
/// rediscovered chunk `Clean` (prerequisite 1), and a failed drain must
/// leave the pending row rather than silently dropping it (the "journal
/// must wait" state that backs prerequisite 2's unmount gate).
#[cfg(test)]
pub(crate) mod pending_upload_tests {
    use super::*;
    use constellation_fs_core::cache::{CacheVerify, ChunkState};
    use constellation_fs_core::ChunkHash;
    use constellation_meta::MetaStore;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Wraps an in-memory backend and can be told to fail every `put`,
    /// simulating a cut S3 path without needing toxiproxy for a unit
    /// test.
    #[derive(Debug)]
    pub(crate) struct FailingStore {
        inner: InMemory,
        fail_puts: AtomicBool,
        delay_puts: AtomicBool,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        puts: AtomicUsize,
        heads: AtomicUsize,
    }

    impl FailingStore {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: InMemory::new(),
                fail_puts: AtomicBool::new(false),
                delay_puts: AtomicBool::new(false),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                puts: AtomicUsize::new(0),
                heads: AtomicUsize::new(0),
            })
        }

        pub(crate) fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn set_delay_puts(&self, delay: bool) {
            self.delay_puts.store(delay, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for FailingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FailingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FailingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(active, Ordering::SeqCst);
            if self.delay_puts.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            if self.fail_puts.load(Ordering::SeqCst) {
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                return Err(object_store::Error::Generic {
                    store: "FailingStore",
                    source: "S3 path is cut (test injection)".into(),
                });
            }
            let result = self.inner.put_opts(location, payload, opts).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<ObjPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjPath,
            to: &ObjPath,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    struct Fixture {
        meta: Meta,
        cache: Arc<DiskCache>,
        cache_dir: PathBuf,
        store: Arc<ChunkStore>,
        failing: Arc<FailingStore>,
        _cache_tmp: tempfile::TempDir,
    }

    impl Fixture {
        /// Reopen the cache from the same directory: `DiskCache::open`
        /// rebuilds accounting purely from what is on disk, the same
        /// path a real remount after `kill -9` takes.
        fn reopen_cache_simulating_crash(&mut self) {
            self.cache = Arc::new(DiskCache::open(&self.cache_dir, 64 * 1024 * 1024).unwrap());
        }
    }

    fn fixture() -> Fixture {
        let failing = FailingStore::new();
        let cache_tmp = tempfile::tempdir().unwrap();
        let cache_dir = cache_tmp.path().to_path_buf();
        Fixture {
            meta: Meta::open_in_memory().unwrap(),
            cache: Arc::new(DiskCache::open(&cache_dir, 64 * 1024 * 1024).unwrap()),
            cache_dir,
            store: Arc::new(ChunkStore::new(failing.clone() as Arc<dyn ObjectStore>)),
            failing,
            _cache_tmp: cache_tmp,
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// Regression test for prerequisite 1: after a "crash",
    /// `DiskCache::rescan` reports the chunk `Clean` (it cannot tell
    /// uploaded from un-uploaded content from a directory listing
    /// alone), but the durable `pending_upload` row must still drive the
    /// drain to completion.
    #[test]
    fn drain_finds_pending_row_even_though_cache_reports_clean_after_rescan() {
        let mut f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"post-crash content".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();

        // Simulate the crash: reopening the cache rebuilds accounting
        // purely from disk and legitimately reports Clean (see
        // `cache::tests::rescan_rebuilds_accounting`).
        f.reopen_cache_simulating_crash();
        assert_eq!(f.cache.state_of(&hash), Some(ChunkState::Clean));
        assert_eq!(f.meta.pending_uploads().unwrap(), vec![(hash, file.ino)]);

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();

        assert!(
            f.meta.pending_uploads().unwrap().is_empty(),
            "drain must ack the pending row once the chunk is uploaded"
        );
        let uploaded = rt().block_on(f.store.get_chunk(&hash)).unwrap();
        assert_eq!(uploaded, data);
    }

    /// Regression test for prerequisite 2's failure mode: while S3 is
    /// unreachable, the drain must refuse (not silently drop the
    /// pending row), which is exactly the signal the clean-unmount path
    /// uses to call `set_skip_ship(true)` rather than shipping a
    /// manifest for content that never reached S3.
    #[test]
    fn failed_drain_leaves_the_pending_row_for_the_next_attempt() {
        let f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let data = b"never uploaded".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();

        f.failing.set_fail_puts(true);
        let err = rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ));
        assert!(err.is_err(), "drain must fail while S3 is unreachable");
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(hash, file.ino)],
            "a failed drain must not ack the row it could not upload"
        );

        // Heal, retry: the very next attempt must succeed and ack.
        f.failing.set_fail_puts(false);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// Plan 29 M6's characterization test, flipped by plan 30 §M4 item 2
    /// (see `bench/remote/RESULTS.md` anomaly #2): a pending row whose chunk
    /// is gone from the local cache used to fail the *entire* round, and so
    /// block every other inode's manifest from ever shipping, forever. Now
    /// Plan 38 §2.3 lets `--cache-verify admit` (the default) trust the
    /// local copy of a chunk this process hashed, which is a trade made
    /// for *readers*: a reader that gets rotted bytes refuses them and
    /// refetches, and the bucket still holds the truth. The uploader must
    /// not make that trade — a `chunk/<H>` whose content is not `H` is
    /// permanent (`ChunkPutMode::Create` treats `AlreadyExists` as a dedup
    /// hit, so the node holding the correct bytes never overwrites it) and
    /// poisons the chunk for every node. So the upload read hashes
    /// regardless (`DiskCache::get_verified`) and a rotted chunk takes the
    /// "missing" path instead of being published.
    #[test]
    fn a_locally_rotted_dirty_chunk_is_never_published() {
        let f = fixture();
        f.meta.set_holder_epoch(1);
        let file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "rotted",
                0o644,
                0,
                0,
            )
            .unwrap();
        let data = b"content that a bad sector will eat".to_vec();
        let hash = ChunkHash::of(&data);
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        // Default mode: this process wrote the chunk, so its entry is
        // `verified` and a plain read would hand the bytes over unchecked.
        assert_eq!(f.cache.verify_mode(), CacheVerify::Admit);
        let hex = hash.to_hex();
        let path = f
            .cache_dir
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(hex.clone());
        let mut rot = data.clone();
        rot[3] ^= 0x80;
        std::fs::write(&path, &rot).unwrap();

        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .expect("a rotted chunk is reported, not an error");
        assert_eq!(report.missing, vec![(hash, file.ino)]);
        // Nothing under the chunk's name in the bucket at all — not an
        // object that merely fails `get_chunk`'s own hash check — and the
        // corrupt local copy is gone (a refetch, or fsck, repairs it).
        assert!(!rt().block_on(f.store.chunk_durable(&hash)).unwrap());
        assert!(!f.cache.contains(&hash));
    }

    /// the upload pass reports it instead of failing, records it as
    /// unrecoverable, and the ship plan holds back only that inode's
    /// manifest (and whatever depends on it): **other inodes still
    /// publish**, including ones written after the broken one.
    #[test]
    fn one_missing_chunk_holds_back_only_its_own_records() {
        let f = fixture();
        // Holder capture on (this node holds the lease), so every
        // transaction has a key set and the isolation is exact.
        f.meta.set_holder_epoch(1);
        let healthy_file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "healthy",
                0o644,
                0,
                0,
            )
            .unwrap();
        let healthy_data = b"perfectly fine content".to_vec();
        let healthy_hash = ChunkHash::of(&healthy_data);
        f.cache
            .insert(&healthy_hash, &healthy_data, ChunkState::Dirty)
            .unwrap();
        f.meta
            .set_manifest_dirty(
                healthy_file.ino,
                None,
                b"M",
                healthy_data.len() as u64,
                &[healthy_hash],
            )
            .unwrap();

        // "broken": a pending_upload row with nothing behind it in the
        // cache -- the observed field condition, reproduced directly
        // rather than via whatever race produces it in practice.
        let broken_file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "broken",
                0o644,
                0,
                0,
            )
            .unwrap();
        let broken_hash = ChunkHash::of(b"bytes that are gone");
        f.meta
            .set_manifest_dirty(broken_file.ino, None, b"M2", 4, &[broken_hash])
            .unwrap();
        assert!(
            f.cache.get(&broken_hash).unwrap().is_none(),
            "the broken hash must not be in the cache"
        );
        // Written after the broken one, touching none of its keys.
        let later_file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "later", 0o644, 0, 0)
            .unwrap();

        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .expect("a missing chunk no longer fails the round");
        assert_eq!(report.missing, vec![(broken_hash, broken_file.ino)]);
        assert_eq!(
            f.meta.unrecoverable_chunks().unwrap(),
            vec![(broken_hash, broken_file.ino)]
        );
        // The healthy chunk uploaded and acked; only the broken row stays.
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(broken_hash, broken_file.ino)]
        );
        let uploaded = rt().block_on(f.store.get_chunk(&healthy_hash)).unwrap();
        assert_eq!(uploaded, healthy_data);
        // A caller that needs everything durable (an fsync, an unmount's
        // final flush) still gets the error.
        assert!(rt()
            .block_on(upload_dirty_chunks(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .is_err());

        // The ship plan: everything but the broken manifest ships — the
        // healthy file, the broken file's own create (it names no chunk),
        // and the later file.
        let batch: Vec<(u64, constellation_meta::LogRecord)> = f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, batch)| batch)
            .collect();
        let manifests: Vec<u64> = batch
            .iter()
            .filter_map(|(_, rec)| match rec {
                constellation_meta::LogRecord::WriteManifest { ino, .. } => Some(*ino),
                _ => None,
            })
            .collect();
        assert_eq!(manifests, vec![healthy_file.ino]);
        let creates: Vec<String> = batch
            .iter()
            .filter_map(|(_, rec)| match rec {
                constellation_meta::LogRecord::Create { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(creates, vec!["healthy", "broken", "later"]);
        let held = f.meta.held_summary();
        assert_eq!(held.transactions, 1, "{held:?}");
        assert_eq!(held.inodes[&broken_file.ino].missing, vec![broken_hash]);
        assert!(later_file.ino != broken_file.ino);

        // Ship what was planned: the held manifest is still journaled,
        // still outstanding speculation, and still held next round.
        let seqs: Vec<u64> = batch.iter().map(|(seq, _)| *seq).collect();
        f.meta.ack_journal_rows_at(&seqs, 1).unwrap();
        let rest: Vec<(u64, constellation_meta::LogRecord)> =
            constellation_meta::MetaStore::take_journal(&f.meta, usize::MAX).unwrap();
        assert!(rest.iter().any(|(_, rec)| matches!(
            rec,
            constellation_meta::LogRecord::WriteManifest { ino, .. } if *ino == broken_file.ino
        )));
        assert!(f.meta.take_journal_grouped(10_000).unwrap().is_empty());
        assert_eq!(f.meta.speculation_counts().unwrap().local, 1);
    }

    /// Plan 30 §M9 × §M4: a successor adopts, from its predecessor's
    /// backup tail, a manifest naming one chunk the predecessor uploaded
    /// and one it never did (a write-back close acknowledged before its
    /// upload). The adopted manifest never ships while a chunk it names is
    /// missing from the bucket: it is deferred until the upload pass has
    /// looked, then held (M4 status) — and it ships once the missing chunk
    /// turns up (the predecessor back, uploading). The same for a spilled
    /// manifest, whose list is read from its blob.
    fn adopted_manifest_ships_only_once_its_chunks_are_durable(spilled: bool) {
        use constellation_fs_core::{manifest::encode_chunk_list, Manifest};
        let f = fixture();
        f.meta.set_holder_epoch(2);
        let file = f
            .meta
            .create(
                constellation_fs_core::types::ROOT_INO,
                "adopted",
                0o644,
                0,
                0,
            )
            .unwrap();
        let shipped: Vec<u64> = constellation_meta::MetaStore::take_journal(&f.meta, usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(s, _)| s)
            .collect();
        f.meta.ack_journal_rows_at(&shipped, 1).unwrap();

        let uploaded = b"uploaded by the predecessor".to_vec();
        let lost = b"only on the predecessor's disk".to_vec();
        let (up_hash, lost_hash) = (ChunkHash::of(&uploaded), ChunkHash::of(&lost));
        rt().block_on(
            f.store
                .put_chunk(&up_hash, &uploaded, CompressionSetting::RAW),
        )
        .unwrap();
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, up_hash), (1u64, lost_hash)].into_iter().collect();
        let (manifest, blob) = if spilled {
            let blob = encode_chunk_list(&chunks);
            let blob_hash = ChunkHash::of(&blob);
            rt().block_on(
                f.store
                    .put_chunk(&blob_hash, &blob, CompressionSetting::RAW),
            )
            .unwrap();
            let (m, spill) = Manifest::from_sparse_chunks(4096, 8192, chunks, 0, ChunkHash::of);
            assert!(spill.is_some());
            (m, Some(blob_hash))
        } else {
            (
                Manifest::from_sparse_chunks(4096, 8192, chunks, 8, ChunkHash::of).0,
                None,
            )
        };
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 7,
        };
        f.meta
            .apply_adopted_records(
                &[
                    constellation_meta::LogRecord::WriteManifest {
                        ino: file.ino,
                        base_manifest: None,
                        manifest: manifest.encode(),
                        size: 8192,
                        time_ns: 1,
                    },
                    constellation_meta::LogRecord::Completed { rid },
                ],
                Some(rid),
            )
            .unwrap();
        let manifests_shipped = |f: &Fixture| -> Vec<u64> {
            f.meta
                .take_journal_grouped(10_000)
                .unwrap()
                .into_iter()
                .flat_map(|(_, batch)| batch)
                .filter_map(|(_, rec)| match rec {
                    constellation_meta::LogRecord::WriteManifest { ino, .. } => Some(ino),
                    _ => None,
                })
                .collect()
        };
        // Enrolled: deferred before any upload pass has looked.
        assert!(!f.meta.pending_uploads().unwrap().is_empty());
        assert!(manifests_shipped(&f).is_empty(), "shipped before any check");
        if spilled {
            assert_eq!(
                f.meta.adopted_spills().unwrap(),
                vec![(file.ino, blob.unwrap())]
            );
        }

        // The upload pass: the uploaded chunk (and the blob) are
        // acknowledged from S3; the other is recorded unrecoverable.
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert_eq!(report.missing, vec![(lost_hash, file.ino)]);
        assert!(
            f.meta.adopted_spills().unwrap().is_empty(),
            "the list was expanded"
        );
        assert_eq!(
            f.meta.pending_uploads().unwrap(),
            vec![(lost_hash, file.ino)]
        );
        assert!(
            manifests_shipped(&f).is_empty(),
            "a dangling manifest shipped"
        );
        let held = f.meta.held_summary();
        assert_eq!(held.transactions, 1, "{held:?}");
        assert_eq!(held.inodes[&file.ino].missing, vec![lost_hash]);
        // Held work is `Local` speculation: never published.
        assert_eq!(f.meta.speculation_counts().unwrap().local, 1);

        // The predecessor is back and uploads: the next pass finds the
        // chunk in S3, and the manifest ships.
        rt().block_on(
            f.store
                .put_chunk(&lost_hash, &lost, CompressionSetting::RAW),
        )
        .unwrap();
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert!(report.missing.is_empty());
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert_eq!(manifests_shipped(&f), vec![file.ino]);
    }

    #[test]
    fn an_adopted_manifest_ships_only_once_its_chunks_are_durable() {
        adopted_manifest_ships_only_once_its_chunks_are_durable(false);
    }

    #[test]
    fn an_adopted_spilled_manifest_ships_only_once_its_chunks_are_durable() {
        adopted_manifest_ships_only_once_its_chunks_are_durable(true);
    }

    /// Two failovers inside one upload window: B adopts A's manifest (its
    /// chunk only on A), then B is deposed before shipping it; the
    /// stranded adopted transaction is replayed by rid through the next
    /// holder C as a `Records` op. C must enroll its chunks for the
    /// durability check as B did — before, `Records` executed without
    /// enrolling anything and C shipped a manifest naming a missing chunk.
    #[test]
    fn a_stranded_adopted_manifest_replayed_elsewhere_is_still_held() {
        use constellation_fs_core::Manifest;
        let b = fixture();
        let c = fixture();
        b.meta.set_holder_epoch(2);
        let file = b
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        let on_c = c
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "f", 0o644, 0, 0)
            .unwrap();
        assert_eq!(file.ino, on_c.ino, "the same file on both replicas");
        let lost = ChunkHash::of(b"only on A");
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, lost)].into_iter().collect();
        let manifest = Manifest::from_sparse_chunks(4096, 9, chunks, 8, ChunkHash::of).0;
        let rid = constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq: 3,
        };
        b.meta
            .apply_adopted_records(
                &[
                    constellation_meta::LogRecord::WriteManifest {
                        ino: file.ino,
                        base_manifest: None,
                        manifest: manifest.encode(),
                        size: 9,
                        time_ns: 1,
                    },
                    constellation_meta::LogRecord::Completed { rid },
                ],
                Some(rid),
            )
            .unwrap();
        // B is deposed (C's epoch 3): the adopted transaction strands.
        b.meta.set_holder_epoch(0);
        b.meta.strand_below_epoch(3).unwrap();
        let queue = b.meta.pending_replays().unwrap();
        let queued = queue.iter().find(|q| q.rid == rid).expect("queued by rid");
        assert!(matches!(
            queued.op,
            constellation_meta::MutateOp::Records { .. }
        ));

        // C executes the replay as holder.
        c.meta.set_holder_epoch(3);
        constellation_meta::execute_mutate(&c.meta, &queued.op, Some(rid)).unwrap();
        assert_eq!(c.meta.pending_uploads().unwrap(), vec![(lost, file.ino)]);
        rt().block_on(upload_dirty_chunks_report(
            &c.cache,
            &c.meta,
            &c.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        let shipped: Vec<constellation_meta::LogRecord> = c
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, batch)| batch)
            .map(|(_, rec)| rec)
            .collect();
        assert!(
            !shipped
                .iter()
                .any(|r| matches!(r, constellation_meta::LogRecord::WriteManifest { .. })),
            "C shipped a manifest naming a chunk the bucket lacks: {shipped:?}"
        );
        assert!(c.meta.held_summary().inodes.contains_key(&file.ino));
    }

    /// The other way out: the predecessor never comes back, and the
    /// operator drops the held manifest (`repair drop-held`): it becomes a
    /// refused replay (a conflict copy with the lost chunk as a hole).
    #[test]
    fn an_adopted_manifest_with_a_lost_chunk_can_be_dropped() {
        use constellation_fs_core::Manifest;
        let f = fixture();
        f.meta.set_holder_epoch(2);
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "lost", 0o644, 0, 0)
            .unwrap();
        let lost_hash = ChunkHash::of(b"gone with the predecessor");
        let chunks: constellation_fs_core::manifest::SparseChunks =
            [(0u64, lost_hash)].into_iter().collect();
        let manifest = Manifest::from_sparse_chunks(4096, 25, chunks, 8, ChunkHash::of).0;
        f.meta
            .apply_adopted_records(
                &[constellation_meta::LogRecord::WriteManifest {
                    ino: file.ino,
                    base_manifest: None,
                    manifest: manifest.encode(),
                    size: 25,
                    time_ns: 1,
                }],
                None,
            )
            .unwrap();
        rt().block_on(upload_dirty_chunks_report(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        // (The summary is the last ship plan's; the create ships.)
        assert!(f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .all(|(_, rec)| !matches!(rec, constellation_meta::LogRecord::WriteManifest { .. })));
        assert!(f.meta.held_summary().inodes.contains_key(&file.ino));
        let dropped = f.meta.drop_held(file.ino, 1).unwrap();
        assert_eq!(dropped.dropped, 1, "{dropped:?}");
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        let queue = f.meta.pending_replays().unwrap();
        assert_eq!(queue.len(), 1);
        assert!(queue[0].refused.is_some(), "a conflict copy is queued");
        assert!(f
            .meta
            .take_journal_grouped(10_000)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .all(|(_, rec)| !matches!(rec, constellation_meta::LogRecord::WriteManifest { .. })));
    }

    #[test]
    fn upload_pool_honours_its_bound() {
        let f = fixture();
        f.failing.set_delay_puts(true);
        for i in 0..12u8 {
            let file = f
                .meta
                .create(
                    constellation_fs_core::types::ROOT_INO,
                    &format!("f-{i}"),
                    0o644,
                    0,
                    0,
                )
                .unwrap();
            let data = vec![i; 4096];
            let hash = ChunkHash::of(&data);
            f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
            f.meta
                .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
                .unwrap();
        }
        let upload = UploadRuntime::for_test_fixed(true, 3);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert!(f.failing.max_in_flight.load(Ordering::SeqCst) <= 3);
        assert!(
            f.failing.max_in_flight.load(Ordering::SeqCst) >= 2,
            "the test must observe actual parallelism"
        );
    }

    fn queue(f: &Fixture, name: &str, data: &[u8]) -> ChunkHash {
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, name, 0o644, 0, 0)
            .unwrap();
        let hash = ChunkHash::of(data);
        f.cache.insert(&hash, data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        hash
    }

    /// A hinted hash confirms with a HEAD and skips the PUT entirely; an
    /// unhinted one keeps the adaptive fallback, because no hint source can
    /// prove absence any more (plan 26 step 8 deleted the LIST seed that
    /// could). Neither decision costs a LIST.
    #[test]
    fn hinted_hash_probes_and_unhinted_hash_keeps_the_adaptive_fallback() {
        let f = fixture();
        let known_data = b"already in S3";
        let known = ChunkHash::of(known_data);
        rt().block_on(f.store.put_chunk_mode(
            &known,
            known_data,
            CompressionSetting::RAW,
            constellation_store_s3::ChunkPutMode::Create,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);

        queue(&f, "known", known_data);
        queue(&f, "new", b"not in S3");
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&known);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();

        assert_eq!(
            f.failing.heads.load(Ordering::SeqCst),
            2,
            "the hint confirms with a HEAD; the unhinted chunk probes too"
        );
        assert_eq!(
            f.failing.puts.load(Ordering::SeqCst),
            1,
            "only the chunk that is genuinely absent is uploaded"
        );
        assert_eq!(upload.existence.report().bloom_hits, 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn bloom_false_positive_still_calls_store_before_ack() {
        let f = fixture();
        let data = b"forced false positive";
        let hash = queue(&f, "false-positive", data);
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&hash);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    #[test]
    fn peer_hit_selects_probe_but_peer_miss_retains_adaptive_head() {
        let f = fixture();
        let hinted = ChunkHash::of(b"hinted");
        let bloom = constellation_net::Bloom::from_hashes(&[hinted.0]);
        let coop = crate::coop::Coop::new_for_upload_test(f.cache.clone(), f.store.clone());
        coop.apply_digest(constellation_net::DigestSnapshot {
            node_id: 2,
            generation: 1,
            bits: bloom.bits,
            nbits: bloom.nbits,
            k: bloom.k,
            n: bloom.n,
            bucket: 0,
            buckets: 1,
        });
        let existence = crate::existence::Existence::new(1024, true, None);
        let mut upload = UploadRuntime::new(true, Some(coop), existence);
        upload.probe_min_bytes = 0;
        assert_eq!(
            upload.put_mode(&hinted, 1 << 20),
            constellation_store_s3::ChunkPutMode::Probe
        );
        assert_eq!(upload.existence.report().peer_hints, 1);
        assert_eq!(
            upload.put_mode(&ChunkHash::of(b"peer miss"), 1 << 20),
            constellation_store_s3::ChunkPutMode::Probe,
            "an unhinted hash must keep the adaptive probe"
        );
    }

    #[test]
    fn condemned_hash_overwrites_even_when_existence_bloom_claims_present() {
        let f = fixture();
        let data = b"condemned existence hit";
        let hash = queue(&f, "condemned", data);
        rt().block_on(constellation_store_s3::publish_condemned(
            f.store.inner(),
            vec![hash.to_hex()],
            1,
        ))
        .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);
        let existence = crate::existence::Existence::new(1024, true, None);
        existence.insert(&hash);
        let mut upload = UploadRuntime::new(true, None, existence);
        upload.probe_min_bytes = 0;

        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        // The hinted HEAD comes first now (`CondemnedView`: the pointer is
        // read only once the object is there); the chunk is not in S3,
        // so the probe's miss uploads it.
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());

        // In S3 and condemned: the HEAD finds it, the pointer (read after
        // it) lists it, and the bytes go up again.
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "again", 0o644, 0, 0)
            .unwrap();
        f.cache.insert(&hash, data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(file.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        f.failing.puts.store(0, Ordering::SeqCst);
        f.failing.heads.store(0, Ordering::SeqCst);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 1);
        assert_eq!(
            f.failing.puts.load(Ordering::SeqCst),
            1,
            "a condemned hit is re-uploaded"
        );
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// The sequencer's side of a non-owner's `back` close: a chunk the
    /// forward named as pending is in neither its cache nor S3. The pass
    /// awaits it — not "missing" (no poison, no held records), not an
    /// error even for a strict drain — and checks S3 itself only once the
    /// first backoff is over; found there, the row is acked.
    #[test]
    fn a_chunk_forwarded_as_pending_is_awaited_not_lost() {
        let f = fixture();
        let file = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "fwd", 0o644, 0, 0)
            .unwrap();
        let data = b"uploading on the forwarder";
        let hash = ChunkHash::of(data);
        f.meta.enroll_remote_chunks(file.ino, &[hash], 2).unwrap();
        let upload = UploadRuntime::for_test(true);
        let pass = |upload: &UploadRuntime| {
            rt().block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                upload,
                None,
                None,
            ))
            .unwrap()
        };
        let report = pass(&upload);
        assert!(report.missing.is_empty());
        assert_eq!(report.awaiting, 1);
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 0, "no S3 check yet");
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            Some(file.ino),
            None,
        ))
        .expect("an awaited chunk is not a lost one");
        assert!(
            f.meta.unrecoverable_chunks().unwrap().is_empty(),
            "nothing poisoned"
        );
        // The forwarder's upload lands; its report was lost; the backoff
        // runs out and this node finds the chunk itself.
        rt().block_on(f.store.put_chunk(&hash, data, CompressionSetting::RAW))
            .unwrap();
        upload
            .remote_polls
            .lock()
            .unwrap()
            .insert(hash, (std::time::Instant::now(), REMOTE_POLL_FIRST));
        let report = pass(&upload);
        assert_eq!(report.awaiting, 0);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert!(!f.meta.awaits_remote_chunk(&hash).unwrap());
    }

    /// The forwarder's side: once its pass puts a chunk it forwarded as
    /// pending up, it owes the node it forwarded to a report — and only
    /// for those chunks.
    #[test]
    fn a_chunk_forwarded_as_pending_is_reported_once_up() {
        let f = fixture();
        let forwarded = queue(&f, "forwarded", b"forwarded while pending");
        let other = queue(&f, "other", b"never forwarded");
        let upload = UploadRuntime::for_test(true);
        upload.note_forwarded(&[forwarded], 9);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        let reports = upload.take_durable_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[&9], vec![forwarded]);
        assert!(!reports[&9].contains(&other));
        assert!(upload.take_durable_reports().is_empty(), "sent once");
    }

    /// Plan 39b: an `fsync`'s drain takes only its own file's reports;
    /// every other report stays owed for the background passes.
    #[test]
    fn an_fsync_takes_only_its_own_files_reports() {
        let (mine, theirs, shared) = (
            ChunkHash::of(b"this file"),
            ChunkHash::of(b"another file"),
            ChunkHash::of(b"both"),
        );
        let upload = UploadRuntime::for_test(true);
        upload.note_forwarded(&[mine, shared], 2);
        upload.note_forwarded(&[theirs, shared], 3);
        upload.note_forwarded(&[theirs], 4);
        for hash in [mine, theirs, shared] {
            upload.note_up(&hash);
        }
        let only: std::collections::HashSet<_> = [mine, shared].into_iter().collect();
        let mut taken = upload.take_durable_reports_of(&only);
        for hashes in taken.values_mut() {
            hashes.sort();
        }
        let mut both = vec![mine, shared];
        both.sort();
        assert_eq!(
            taken,
            [(2, both), (3, vec![shared])].into_iter().collect(),
            "this file's reports, to every node owed one"
        );
        assert_eq!(
            upload.take_durable_reports(),
            [(3, vec![theirs]), (4, vec![theirs])].into_iter().collect(),
            "the rest stays owed"
        );
    }

    /// What a forward says is still pending: the chunks its manifest names
    /// that have a pending row here, nothing for any other op.
    #[test]
    fn a_forward_names_only_the_chunks_still_pending() {
        let f = fixture();
        let pending = queue(&f, "p", b"pending here");
        let durable = ChunkHash::of(b"already up");
        let manifest = |hashes: &[ChunkHash]| {
            constellation_fs_core::Manifest::from_sparse_chunks(
                4096,
                4096 * hashes.len() as u64,
                hashes
                    .iter()
                    .enumerate()
                    .map(|(i, h)| (i as u64, *h))
                    .collect(),
                8,
                ChunkHash::of,
            )
            .0
            .encode()
        };
        let op = constellation_meta::MutateOp::SetManifest {
            ino: 5,
            base_manifest: None,
            manifest: manifest(&[durable, pending]),
            size: 8192,
        };
        assert_eq!(
            forwarded_pending_chunks(&f.meta, &f.cache, &op),
            vec![pending]
        );
        let other = constellation_meta::MutateOp::Unlink {
            parent: constellation_fs_core::types::ROOT_INO,
            name: "p".into(),
        };
        assert!(forwarded_pending_chunks(&f.meta, &f.cache, &other).is_empty());
    }

    /// The OVH run's finding 3: a small chunk does not probe on the
    /// ladder's guess — a HEAD-first probe of a unique small file is two
    /// serialized round trips where a conditional create is one. A
    /// positive hint (the bytes were seen up) still probes: a HEAD is
    /// cheaper than re-sending them.
    #[test]
    fn a_small_chunk_creates_instead_of_probing() {
        let f = fixture();
        let data = b"a small file's only chunk";
        let hash = queue(&f, "small", data);
        let existence = crate::existence::Existence::new(1024, true, None);
        let hinted = ChunkHash::of(b"seen up before");
        existence.insert(&hinted);
        let upload = UploadRuntime::new(true, None, existence);
        assert_eq!(
            upload.put_mode(&hinted, 16),
            constellation_store_s3::ChunkPutMode::Probe,
            "a hinted small chunk probes"
        );
        assert!(
            upload.probe.lock().unwrap().enabled(),
            "the ladder leans to Probe"
        );
        assert_eq!(
            upload.put_mode(&hash, data.len()),
            constellation_store_s3::ChunkPutMode::Create
        );
        assert_eq!(
            upload.put_mode(&hash, 4 << 20),
            constellation_store_s3::ChunkPutMode::Probe,
            "a large chunk still follows the ladder"
        );
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &upload,
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.failing.heads.load(Ordering::SeqCst), 0);
        assert_eq!(f.failing.puts.load(Ordering::SeqCst), 1);
        assert!(f.meta.pending_uploads().unwrap().is_empty());
    }

    /// The clean-demotion race, as a deterministic interleaving. Writer 1
    /// and writer 2 store the same content on one node. Writer 2's
    /// `cache_for_upload` merges into writer 1's dirty entry, but writer
    /// 2 has not enrolled its pending row yet (that happens at its
    /// manifest commit) when writer 1's upload round finishes: the round
    /// sees no pending row for the hash and demotes the chunk to clean,
    /// and cache pressure evicts it. Writer 2 then enrols. Its chunk is
    /// gone from the cache but durable in S3 (writer 1 uploaded the very
    /// same bytes), so its round must acknowledge it rather than report it
    /// lost and hold writer 2's records back.
    #[test]
    fn a_writer_enrolled_after_demotion_and_eviction_is_acknowledged_not_held() {
        let f = fixture();
        let data = b"same bytes, two writers".to_vec();
        let hash = ChunkHash::of(&data);
        let root = constellation_fs_core::types::ROOT_INO;
        let w1 = f.meta.create(root, "w1", 0o644, 0, 0).unwrap();
        let w2 = f.meta.create(root, "w2", 0o644, 0, 0).unwrap();
        // Writer 1 caches and commits.
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        f.meta
            .set_manifest_dirty(w1.ino, None, b"M1", data.len() as u64, &[hash])
            .unwrap();
        // Writer 2's cache_for_upload: a merge into the dirty entry.
        f.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        // Writer 1's round uploads and, seeing no pending row, demotes.
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        assert_eq!(f.cache.state_of(&hash), Some(ChunkState::Clean));
        // Cache pressure evicts the clean chunk; then writer 2 enrols.
        f.cache.prune_to(0).unwrap();
        assert!(!f.cache.contains(&hash));
        f.meta
            .set_manifest_dirty(w2.ino, None, b"M2", data.len() as u64, &[hash])
            .unwrap();

        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert!(
            report.missing.is_empty(),
            "held although durable: {:?}",
            report.missing
        );
        assert!(f.meta.pending_uploads().unwrap().is_empty());
        assert!(f.meta.unrecoverable_chunks().unwrap().is_empty());
        assert_eq!(rt().block_on(f.store.get_chunk(&hash)).unwrap(), data);
    }

    /// The acknowledgement above trusts S3 exactly as far as a dedup
    /// does: a chunk bucket GC has condemned may be deleted at any moment,
    /// so a pending row whose chunk is gone from the cache and condemned
    /// in the bucket is still reported lost (and its records held), never
    /// acknowledged.
    #[test]
    fn a_cache_missing_chunk_condemned_in_s3_is_still_reported_lost() {
        let f = fixture();
        let data = b"condemned and evicted".to_vec();
        let hash = queue(&f, "condemned-evicted", &data);
        rt().block_on(upload_dirty_chunks(
            &f.cache,
            &f.meta,
            &f.store,
            CompressionSetting::RAW,
            &UploadRuntime::for_test(true),
            None,
            None,
        ))
        .unwrap();
        rt().block_on(constellation_store_s3::publish_condemned(
            f.store.inner(),
            vec![hash.to_hex()],
            1,
        ))
        .unwrap();
        f.cache.prune_to(0).unwrap();
        let late = f
            .meta
            .create(constellation_fs_core::types::ROOT_INO, "late", 0o644, 0, 0)
            .unwrap();
        f.meta
            .set_manifest_dirty(late.ino, None, b"M", data.len() as u64, &[hash])
            .unwrap();
        let report = rt()
            .block_on(upload_dirty_chunks_report(
                &f.cache,
                &f.meta,
                &f.store,
                CompressionSetting::RAW,
                &UploadRuntime::for_test(true),
                None,
                None,
            ))
            .unwrap();
        assert_eq!(report.missing, vec![(hash, late.ino)]);
        assert_eq!(f.meta.pending_uploads().unwrap(), vec![(hash, late.ino)]);
    }
}
