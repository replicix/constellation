//! Adaptive sequential readahead (DESIGN.md §7).

use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::SparseChunks;
use constellation_fs_core::{ChunkHash, Ino};
use constellation_store_s3::{ChunkStore, DecodePriority};
use constellation_upload_concurrency::{AdaptiveConcurrency, ConcurrencyGate};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

const REORDER_WINDOW: u64 = 16 << 20;
const STREAM_IDLE: Duration = Duration::from_secs(60);
/// How long a sequential stream may go without a read before its
/// not-yet-started readahead backlog is cancelled (DESIGN.md §7 "abandoned
/// reader"). This is deliberately much shorter than `STREAM_IDLE`: the
/// latter only reclaims bookkeeping after a full minute, but a reader that
/// stops consuming should stop costing us bandwidth and cache space almost
/// immediately, not a minute later. Chunks already popped into flight are
/// left to finish (they are bounded by the concurrency gate and cheap to
/// keep — cancelling a part-way GET would need to plumb abort handles
/// through the fetch future and clean up a torn spill file for a marginal
/// saving). Only *queued* work — chunks reserved but not yet dispatched —
/// is dropped. The stream's cursor/window bookkeeping survives so a reader
/// that resumes is recognized as still sequential and does not cold-start
/// from `min_window`.
const PREFETCH_ABANDON_IDLE: Duration = Duration::from_millis(2000);
const MAX_STREAMS: usize = 512;
const DEFAULT_MIN_WINDOW: u64 = 8 << 20;
const DEFAULT_MAX_WINDOW: u64 = 2 << 30;
const DEFAULT_MAX_CONCURRENCY: usize = 128;
const ABSOLUTE_MAX_CONCURRENCY: usize = 512;
/// Opening background-fetch concurrency. A high-latency S3 path needs many
/// requests in flight before it delivers useful throughput (measured: ~1.3 s
/// per 4 MiB GET, so 8 requests cap a cold stream near 24 Mbps). Slow-start
/// from 8 wasted tens of seconds on a 200 ms path before the controller found
/// the knee, so open at a value the sweep shows is safe on a thin link and let
/// AIMD search upward from there.
const INITIAL_CONCURRENCY: usize = 32;
const QUEUE_ROUNDS: usize = 2;

struct Stream {
    cursor: u64,
    window: u64,
    last_hit: Instant,
    sequential: bool,
    /// First chunk index not yet handed to the scheduler. Readahead is
    /// enqueued incrementally from here, so a grown window costs one pass
    /// over its leading edge rather than a full rescan on every read.
    next_index: u64,
    /// Chunk size in effect for this stream, so the idle-abandon sweep can
    /// re-derive `next_index` from `cursor` after trimming the window
    /// without needing a `read()` to supply it.
    chunk_bytes: u64,
}

#[derive(Default)]
pub(crate) struct PrefetchStats {
    inflight: AtomicU64,
    queued: AtomicU64,
    stalls: AtomicU64,
    gate_target: AtomicU64,
    scan_ahead_files: AtomicU64,
    scan_ahead_bytes: AtomicU64,
    /// Times a stream's queued (not-yet-started) readahead backlog was
    /// cancelled because the reader stopped consuming (see
    /// `PREFETCH_ABANDON_IDLE`). Chunks already in flight are not counted
    /// here — only work that never hit the network.
    abandoned: AtomicU64,
    /// Chunks dropped from queues by those cancellations — the number of
    /// GETs saved from a reader that never came back for them.
    abandoned_chunks: AtomicU64,
    windows: Mutex<HashMap<Ino, u64>>,
}

impl PrefetchStats {
    pub(crate) fn snapshot(&self) -> constellation_api::PrefetchStatus {
        let windows = self.windows.lock().unwrap();
        let streams = windows.len() as u64;
        let window_bytes = windows.values().copied().max().unwrap_or(0);
        drop(windows);
        constellation_api::PrefetchStatus {
            inflight: self.inflight.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            streams,
            window_bytes,
            stalls: self.stalls.load(Ordering::Relaxed),
            gate_target: self.gate_target.load(Ordering::Relaxed) as u32,
            scan_ahead_files: self.scan_ahead_files.load(Ordering::Relaxed),
            scan_ahead_bytes: self.scan_ahead_bytes.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            abandoned_chunks: self.abandoned_chunks.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn note_scan(&self, files: u64, bytes: u64) {
        self.scan_ahead_files.fetch_add(files, Ordering::Relaxed);
        self.scan_ahead_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn note_abandon(&self, chunks: u64) {
        if chunks == 0 {
            return;
        }
        self.abandoned.fetch_add(1, Ordering::Relaxed);
        self.abandoned_chunks.fetch_add(chunks, Ordering::Relaxed);
    }
}

struct Queues {
    by_stream: HashMap<Ino, VecDeque<ChunkHash>>,
    ready: VecDeque<Ino>,
    reserved: HashSet<ChunkHash>,
    active: HashMap<ChunkHash, Ino>,
    active_by_stream: HashMap<Ino, usize>,
}

struct Scheduler {
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    coop: Option<Arc<crate::coop::Coop>>,
    gate: Arc<ConcurrencyGate>,
    controller: Option<Mutex<AdaptiveConcurrency>>,
    stats: Arc<PrefetchStats>,
    queues: Mutex<Queues>,
    wake: tokio::sync::Notify,
}

impl Scheduler {
    fn enqueue(&self, ino: Ino, hashes: impl IntoIterator<Item = ChunkHash>) {
        let mut queues = self.queues.lock().unwrap();
        let mut added = 0u64;
        for hash in hashes {
            if self.cache.contains(&hash) || !queues.reserved.insert(hash) {
                continue;
            }
            let queue = queues.by_stream.entry(ino).or_default();
            let was_empty = queue.is_empty();
            queue.push_back(hash);
            added += 1;
            if was_empty {
                queues.ready.push_back(ino);
            }
        }
        self.stats.queued.fetch_add(added, Ordering::Relaxed);
        drop(queues);
        self.wake.notify_one();
    }

    fn pop(&self) -> Option<ChunkHash> {
        let mut queues = self.queues.lock().unwrap();
        let quota = self
            .gate
            .target()
            .div_ceil(queues.by_stream.len().max(1))
            .max(2);
        let ready = queues.ready.len();
        let ino = (0..ready).find_map(|_| {
            let ino = queues.ready.pop_front()?;
            let active = queues.active_by_stream.get(&ino).copied().unwrap_or(0);
            if active < quota {
                Some(ino)
            } else {
                queues.ready.push_back(ino);
                None
            }
        })?;
        let queue = queues.by_stream.get_mut(&ino).unwrap();
        let hash = queue.pop_front().unwrap();
        if queue.is_empty() {
            queues.by_stream.remove(&ino);
        } else {
            queues.ready.push_back(ino);
        }
        queues.active.insert(hash, ino);
        *queues.active_by_stream.entry(ino).or_default() += 1;
        self.stats.queued.fetch_sub(1, Ordering::Relaxed);
        self.stats.inflight.fetch_add(1, Ordering::Relaxed);
        Some(hash)
    }

    fn finish(&self, hash: &ChunkHash) {
        let mut queues = self.queues.lock().unwrap();
        let finished = if let Some(ino) = queues.active.remove(hash) {
            let active = queues.active_by_stream.get_mut(&ino).unwrap();
            *active -= 1;
            if *active == 0 {
                queues.active_by_stream.remove(&ino);
            }
            true
        } else {
            false
        };
        queues.reserved.remove(hash);
        if finished {
            self.stats.inflight.fetch_sub(1, Ordering::Relaxed);
        }
        self.wake.notify_one();
    }

    /// Drop this stream's not-yet-started queued backlog (used both on file
    /// close and by the idle-abandon sweep). Returns how many chunks were
    /// dropped, so callers can distinguish "reader closed a spent stream"
    /// from "reader stopped mid-file with a live backlog".
    fn forget(&self, ino: Ino) -> u64 {
        let mut queues = self.queues.lock().unwrap();
        let mut dropped = 0u64;
        if let Some(pending) = queues.by_stream.remove(&ino) {
            dropped = pending.len() as u64;
            self.stats.queued.fetch_sub(dropped, Ordering::Relaxed);
            for hash in pending {
                queues.reserved.remove(&hash);
            }
        }
        queues.ready.retain(|candidate| *candidate != ino);
        dropped
    }

    fn is_inflight(&self, hash: &ChunkHash) -> bool {
        self.queues.lock().unwrap().active.contains_key(hash)
    }

    /// Give a demand read ownership of a queued chunk, or report that an
    /// already-running background transfer should be awaited. This closes the
    /// queued-but-not-active race without putting foreground I/O behind the
    /// background concurrency gate.
    fn claim_for_demand(&self, hash: &ChunkHash) -> bool {
        let mut queues = self.queues.lock().unwrap();
        if queues.active.contains_key(hash) {
            return true;
        }
        if !queues.reserved.remove(hash) {
            return false;
        }
        self.stats.queued.fetch_sub(1, Ordering::Relaxed);
        let owner = queues
            .by_stream
            .iter()
            .find(|(_, pending)| pending.contains(hash))
            .map(|(ino, _)| *ino);
        if let Some(ino) = owner {
            let empty = {
                let pending = queues.by_stream.get_mut(&ino).unwrap();
                pending.retain(|candidate| candidate != hash);
                pending.is_empty()
            };
            if empty {
                queues.by_stream.remove(&ino);
                queues.ready.retain(|candidate| *candidate != ino);
            }
        }
        false
    }

    fn record_success(&self, bytes: u64, service_time: Duration) {
        let Some(controller) = &self.controller else {
            return;
        };
        let target = controller
            .lock()
            .unwrap()
            .on_success(Instant::now(), bytes, service_time);
        self.gate.set_target(target);
        self.stats
            .gate_target
            .store(target as u64, Ordering::Relaxed);
    }

    fn record_error(&self) {
        let Some(controller) = &self.controller else {
            return;
        };
        let target = controller.lock().unwrap().on_error(Instant::now());
        self.gate.set_target(target);
        self.stats
            .gate_target
            .store(target as u64, Ordering::Relaxed);
    }

    async fn run(self: Arc<Self>) {
        loop {
            let notified = self.wake.notified();
            if self.queues.lock().unwrap().ready.is_empty() {
                notified.await;
                continue;
            }
            let permit = self.gate.acquire_owned().await;
            let Some(hash) = self.pop() else {
                drop(permit);
                continue;
            };
            let scheduler = self.clone();
            tokio::spawn(async move {
                let result = if scheduler.cache.contains(&hash) {
                    Ok(None)
                } else if let Some(coop) = &scheduler.coop {
                    coop.fetch_for_prefetch(&hash).await
                } else {
                    let result = async {
                        let mut spill = scheduler.cache.begin_spill()?;
                        let (bytes, _, service_time) = if scheduler.store.is_e2e() {
                            let mut cipher_spill = scheduler.cache.begin_spill()?;
                            scheduler
                                .store
                                .get_chunk_to_writer_e2e(
                                    &hash,
                                    &mut cipher_spill,
                                    &mut spill,
                                    DecodePriority::Background,
                                )
                                .await?
                        } else {
                            scheduler
                                .store
                                .get_chunk_to_writer(&hash, &mut spill)
                                .await?
                        };
                        if let Err(error) =
                            scheduler
                                .cache
                                .commit_spill(&hash, spill, ChunkState::Clean)
                        {
                            tracing::debug!(
                                chunk = %hash.to_hex(),
                                %error,
                                "prefetch cache commit failed"
                            );
                        }
                        Ok::<_, anyhow::Error>(Some((bytes, service_time)))
                    }
                    .await;
                    result
                };
                match result {
                    Ok(Some((bytes, service_time))) => {
                        scheduler.record_success(bytes, service_time);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        scheduler.record_error();
                        tracing::debug!(
                            chunk = %hash.to_hex(),
                            %error,
                            "prefetch failed"
                        );
                    }
                }
                scheduler.finish(&hash);
                drop(permit);
            });
        }
    }
}

pub struct Prefetcher {
    rt: Handle,
    streams: Arc<Mutex<HashMap<Ino, Stream>>>,
    scheduler: Arc<Scheduler>,
    min_window: u64,
    max_window: u64,
    stats: Arc<PrefetchStats>,
}

impl Prefetcher {
    pub fn new(
        rt: Handle,
        store: Arc<ChunkStore>,
        cache: Arc<DiskCache>,
        coop: Option<Arc<crate::coop::Coop>>,
    ) -> Self {
        let configured_min = env_u64("CONSTELLATION_PREFETCH_MIN_BYTES", DEFAULT_MIN_WINDOW);
        let configured_max = env_u64("CONSTELLATION_PREFETCH_MAX_BYTES", DEFAULT_MAX_WINDOW);
        let max_window = configured_max.min(cache.usage().budget / 4).max(1);
        let min_window = configured_min.min(max_window);
        let max_concurrency = env_usize(
            "CONSTELLATION_PREFETCH_MAX_CONCURRENCY",
            DEFAULT_MAX_CONCURRENCY,
        )
        .clamp(1, ABSOLUTE_MAX_CONCURRENCY);
        let fixed = std::env::var("CONSTELLATION_PREFETCH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(1, max_concurrency));
        let initial = fixed.unwrap_or_else(|| INITIAL_CONCURRENCY.min(max_concurrency));
        let stats = Arc::new(PrefetchStats::default());
        stats.gate_target.store(initial as u64, Ordering::Relaxed);
        let scheduler = Arc::new(Scheduler {
            store,
            cache,
            coop,
            gate: Arc::new(ConcurrencyGate::new(initial)),
            controller: fixed.map_or_else(
                || {
                    Some(Mutex::new(AdaptiveConcurrency::with_intervals(
                        initial,
                        1,
                        max_concurrency,
                        Duration::from_millis(500),
                        Duration::from_secs(2),
                    )))
                },
                |_| None,
            ),
            stats: stats.clone(),
            queues: Mutex::new(Queues {
                by_stream: HashMap::new(),
                ready: VecDeque::new(),
                reserved: HashSet::new(),
                active: HashMap::new(),
                active_by_stream: HashMap::new(),
            }),
            wake: tokio::sync::Notify::new(),
        });
        rt.spawn(scheduler.clone().run());
        let streams: Arc<Mutex<HashMap<Ino, Stream>>> = Arc::new(Mutex::new(HashMap::new()));
        rt.spawn(sweep_abandoned_streams(streams.clone(), scheduler.clone()));
        Self {
            rt,
            streams,
            scheduler,
            min_window,
            max_window,
            stats,
        }
    }

    /// Called on every read. Detects sequential access and schedules
    /// background fetches for upcoming chunks.
    pub fn on_read(&self, ino: Ino, offset: u64, len: u64, chunk_size: u32, hashes: &SparseChunks) {
        let now = Instant::now();
        let chunk_bytes = u64::from(chunk_size).max(1);
        let (sequential, cursor, window, active_streams) = {
            let mut streams = self.streams.lock().unwrap();
            let stale: Vec<Ino> = streams
                .iter()
                .filter(|(_, stream)| now.duration_since(stream.last_hit) > STREAM_IDLE)
                .map(|(ino, _)| *ino)
                .collect();
            for stale_ino in stale {
                streams.remove(&stale_ino);
                self.stats.windows.lock().unwrap().remove(&stale_ino);
                self.scheduler.forget(stale_ino);
            }
            if streams.len() >= MAX_STREAMS && !streams.contains_key(&ino) {
                if let Some(oldest) = streams
                    .iter()
                    .min_by_key(|(_, stream)| stream.last_hit)
                    .map(|(ino, _)| *ino)
                {
                    streams.remove(&oldest);
                    self.stats.windows.lock().unwrap().remove(&oldest);
                    self.scheduler.forget(oldest);
                }
            }
            let end = offset.saturating_add(len);
            match streams.get_mut(&ino) {
                Some(stream)
                    if end >= stream.cursor.saturating_sub(REORDER_WINDOW)
                        && offset <= stream.cursor.saturating_add(REORDER_WINDOW) =>
                {
                    stream.cursor = stream.cursor.max(end);
                    stream.last_hit = now;
                    stream.sequential = true;
                }
                Some(stream) => {
                    *stream = Stream {
                        cursor: end,
                        window: self.min_window,
                        last_hit: now,
                        sequential: false,
                        next_index: end / chunk_bytes,
                        chunk_bytes,
                    };
                }
                None => {
                    let sequential = offset == 0;
                    streams.insert(
                        ino,
                        Stream {
                            cursor: end,
                            window: self.min_window,
                            last_hit: now,
                            sequential,
                            next_index: end / chunk_bytes,
                            chunk_bytes,
                        },
                    );
                }
            }
            let stream = streams.get(&ino).unwrap();
            (
                stream.sequential,
                stream.cursor,
                stream.window,
                streams
                    .values()
                    .filter(|candidate| candidate.sequential)
                    .count()
                    .max(1),
            )
        };
        if !sequential || hashes.is_empty() {
            return;
        }
        let window = scheduled_window(
            window,
            chunk_bytes,
            self.scheduler.gate.target(),
            active_streams,
            self.max_window,
        );
        // Enqueue only the newly exposed leading edge. `next_index` also
        // skips forward past the reader's cursor so a stream that outran its
        // own readahead does not re-offer chunks it has already consumed.
        let last = cursor.saturating_add(window).div_ceil(chunk_bytes);
        let first = {
            let mut streams = self.streams.lock().unwrap();
            let Some(stream) = streams.get_mut(&ino) else {
                return;
            };
            stream.window = stream.window.max(window);
            let first = stream.next_index.max(cursor.div_ceil(chunk_bytes));
            stream.next_index = first.max(last);
            first
        };
        self.stats.windows.lock().unwrap().insert(ino, window);
        if first >= last {
            return;
        }
        self.scheduler.enqueue(
            ino,
            (first..last).filter_map(|index| hashes.get(&index).copied()),
        );
    }

    /// Grow a live stream's byte window when a demand read waits for data.
    /// Random-access streams are left alone: a miss there is not evidence
    /// that readahead is running behind.
    pub fn note_stall(&self, ino: Ino) {
        if let Some(stream) = self.streams.lock().unwrap().get_mut(&ino) {
            if !stream.sequential {
                return;
            }
            stream.window = stream.window.saturating_mul(2).min(self.max_window);
            self.stats.stalls.fetch_add(1, Ordering::Relaxed);
            self.stats
                .windows
                .lock()
                .unwrap()
                .insert(ino, stream.window);
        }
    }

    /// Whether a background fetch for this chunk is currently running.
    /// The read path waits for it instead of issuing a duplicate GET.
    pub fn is_inflight(&self, hash: &ChunkHash) -> bool {
        self.scheduler.is_inflight(hash)
    }

    pub fn claim_for_demand(&self, hash: &ChunkHash) -> bool {
        self.scheduler.claim_for_demand(hash)
    }

    /// Forget an inode's cursor (last close).
    pub fn forget(&self, ino: Ino) {
        self.streams.lock().unwrap().remove(&ino);
        self.stats.windows.lock().unwrap().remove(&ino);
        self.scheduler.forget(ino);
    }

    pub(crate) fn stats(&self) -> Arc<PrefetchStats> {
        self.stats.clone()
    }

    pub(crate) fn enqueue_scan(&self, files: Vec<crate::scan::ScanFile>) {
        let mut count = 0u64;
        let mut bytes = 0u64;
        for file in files {
            count += 1;
            bytes = bytes.saturating_add(file.bytes);
            self.scheduler.enqueue(file.ino, file.hashes);
            if let Some(list_hash) = file.chunk_list {
                let cache = self.scheduler.cache.clone();
                let scheduler = self.scheduler.clone();
                self.rt.spawn(async move {
                    for _ in 0..3000 {
                        match cache.get(&list_hash) {
                            Ok(Some(encoded)) => {
                                if let Ok(chunks) =
                                    constellation_fs_core::manifest::decode_chunk_list(&encoded)
                                {
                                    scheduler.enqueue(file.ino, chunks.values().take(2).copied());
                                }
                                return;
                            }
                            Ok(None) => tokio::time::sleep(Duration::from_millis(10)).await,
                            Err(_) => return,
                        }
                    }
                });
            }
        }
        self.stats.note_scan(count, bytes);
    }
}

/// Background sweep: cancel a sequential stream's not-yet-started readahead
/// once the reader has gone quiet for `PREFETCH_ABANDON_IDLE`.
///
/// Why this exists (DESIGN.md §7 "abandoned reader"): the scheduler enqueues
/// readahead up to `window` bytes ahead of the cursor on every read, but
/// nothing previously un-enqueued that work if the reader simply stopped —
/// e.g. an rsync/tar that got killed, a video scrub that abandoned a seek
/// target, or a client that closed the fd without EOF. `STREAM_IDLE` (60s)
/// eventually reaps the whole stream, but by then a wide window (up to
/// `max_window`, gated by `CONSTELLATION_PREFETCH_MAX_BYTES`) may have
/// already queued tens to hundreds of chunks that will never be read —
/// wasted S3 GETs and cache evictions of data someone else still wants.
///
/// This sweep runs far more often and only trims the *queue*: chunks already
/// popped into flight are left to finish (see `PREFETCH_ABANDON_IDLE`'s
/// doc-comment for why), and the stream's cursor/window survive so a reader
/// that resumes mid-file is still recognized as sequential.
async fn sweep_abandoned_streams(
    streams: Arc<Mutex<HashMap<Ino, Stream>>>,
    scheduler: Arc<Scheduler>,
) {
    let tick = (PREFETCH_ABANDON_IDLE / 4).max(Duration::from_millis(1));
    loop {
        tokio::time::sleep(tick).await;
        abandon_idle_streams(&streams, &scheduler, Instant::now());
    }
}

/// One pass of the idle-abandon sweep, factored out so it can be driven
/// deterministically from tests without needing to wait on a real timer or
/// spin up the scheduler's fetch loop.
fn abandon_idle_streams(
    streams: &Mutex<HashMap<Ino, Stream>>,
    scheduler: &Scheduler,
    now: Instant,
) {
    let idle: Vec<Ino> = {
        let streams = streams.lock().unwrap();
        streams
            .iter()
            .filter(|(_, stream)| {
                stream.sequential && now.duration_since(stream.last_hit) > PREFETCH_ABANDON_IDLE
            })
            .map(|(ino, _)| *ino)
            .collect()
    };
    for ino in idle {
        let dropped = scheduler.forget(ino);
        if dropped == 0 {
            continue;
        }
        scheduler.stats.note_abandon(dropped);
        // Rewind next_index to the cursor so a resumed reader is
        // re-offered the chunks we just dropped instead of skipping
        // them as "already enqueued".
        if let Some(stream) = streams.lock().unwrap().get_mut(&ino) {
            stream.next_index = stream.cursor / stream.chunk_bytes.max(1);
        }
        tracing::debug!(
            ino,
            chunks = dropped,
            "prefetch: abandoned reader, dropped queued readahead"
        );
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn scheduled_window(
    current: u64,
    chunk_size: u64,
    gate_target: usize,
    active_streams: usize,
    max_window: u64,
) -> u64 {
    let fair_slots = gate_target.div_ceil(active_streams.max(1));
    let target_bytes = chunk_size
        .saturating_mul(fair_slots as u64)
        .saturating_mul(QUEUE_ROUNDS as u64);
    current.max(target_bytes).min(max_window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    fn hash(byte: u8) -> ChunkHash {
        ChunkHash::of(&[byte])
    }

    fn scheduler(cache: Arc<DiskCache>) -> Arc<Scheduler> {
        Arc::new(Scheduler {
            store: Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            coop: None,
            gate: Arc::new(ConcurrencyGate::new(4)),
            controller: None,
            stats: Arc::new(PrefetchStats::default()),
            queues: Mutex::new(Queues {
                by_stream: HashMap::new(),
                ready: VecDeque::new(),
                reserved: HashSet::new(),
                active: HashMap::new(),
                active_by_stream: HashMap::new(),
            }),
            wake: tokio::sync::Notify::new(),
        })
    }

    #[test]
    fn scheduler_rotates_between_ready_streams() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap()));
        scheduler.enqueue(1, [hash(1), hash(2)]);
        scheduler.enqueue(2, [hash(3)]);
        assert_eq!(scheduler.pop(), Some(hash(1)));
        assert_eq!(scheduler.pop(), Some(hash(3)));
        assert_eq!(scheduler.pop(), Some(hash(2)));
    }

    #[test]
    fn a_new_stream_gets_completions_until_inflight_shares_rebalance() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap()));
        scheduler.enqueue(1, [hash(1), hash(2), hash(3), hash(4), hash(7), hash(8)]);
        let active: Vec<_> = (0..4).map(|_| scheduler.pop().unwrap()).collect();
        scheduler.enqueue(2, [hash(5), hash(6)]);

        scheduler.finish(&active[0]);
        assert_eq!(scheduler.pop(), Some(hash(5)));
        scheduler.finish(&active[1]);
        assert_eq!(scheduler.pop(), Some(hash(6)));
        scheduler.finish(&active[2]);
        assert_eq!(scheduler.pop(), Some(hash(7)));
    }

    #[test]
    fn demand_claim_removes_a_queued_fetch_without_disturbing_other_streams() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap()));
        scheduler.enqueue(1, [hash(1), hash(2)]);
        scheduler.enqueue(2, [hash(3)]);

        assert!(!scheduler.claim_for_demand(&hash(2)));
        assert!(!scheduler.queues.lock().unwrap().reserved.contains(&hash(2)));
        assert_eq!(scheduler.pop(), Some(hash(1)));
        assert_eq!(scheduler.pop(), Some(hash(3)));
    }

    #[test]
    fn demand_waits_when_the_background_fetch_is_already_active() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap()));
        scheduler.enqueue(1, [hash(1)]);
        assert_eq!(scheduler.pop(), Some(hash(1)));
        assert!(scheduler.claim_for_demand(&hash(1)));
    }

    #[test]
    fn gate_target_sets_a_fair_per_stream_queue_floor() {
        assert_eq!(scheduled_window(8 << 20, 4 << 20, 8, 1, 2 << 30), 64 << 20);
        assert_eq!(scheduled_window(8 << 20, 4 << 20, 8, 2, 2 << 30), 32 << 20);
        assert_eq!(scheduled_window(8 << 20, 4 << 20, 512, 1, 1 << 30), 1 << 30);
    }

    #[test]
    fn status_reports_live_streams_and_largest_window() {
        let stats = PrefetchStats::default();
        stats.windows.lock().unwrap().insert(1, 32 << 20);
        stats.windows.lock().unwrap().insert(2, 64 << 20);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.streams, 2);
        assert_eq!(snapshot.window_bytes, 64 << 20);
    }

    #[tokio::test]
    async fn reorder_survives_but_seek_resets_window() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let prefetch = Prefetcher::new(
            Handle::current(),
            Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            None,
        );
        let hashes = SparseChunks::new();
        prefetch.on_read(7, 0, 128 << 10, 4 << 20, &hashes);
        prefetch.note_stall(7);
        let grown = prefetch.streams.lock().unwrap()[&7].window;
        prefetch.on_read(7, 256 << 10, 128 << 10, 4 << 20, &hashes);
        prefetch.on_read(7, 128 << 10, 128 << 10, 4 << 20, &hashes);
        assert_eq!(prefetch.streams.lock().unwrap()[&7].window, grown);

        prefetch.on_read(7, 64 << 20, 128 << 10, 4 << 20, &hashes);
        assert_eq!(
            prefetch.streams.lock().unwrap()[&7].window,
            prefetch.min_window
        );
    }

    #[tokio::test]
    async fn random_first_read_does_not_enqueue_prefetch() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let prefetch = Prefetcher::new(
            Handle::current(),
            Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            None,
        );
        let mut hashes = SparseChunks::new();
        hashes.insert(17, hash(1));
        prefetch.on_read(7, 64 << 20, 128 << 10, 4 << 20, &hashes);
        assert!(prefetch.scheduler.queues.lock().unwrap().ready.is_empty());
    }

    #[tokio::test]
    async fn sequential_reads_enqueue_each_chunk_at_most_once() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let prefetch = Prefetcher::new(
            Handle::current(),
            Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            None,
        );
        let mut hashes = SparseChunks::new();
        for index in 0..64 {
            hashes.insert(index, hash(index as u8));
        }
        // Walk the first four chunks in 1 MiB steps. Re-enqueue attempts show
        // up as repeated queue growth for already-offered chunks.
        for step in 0..16u64 {
            prefetch.on_read(7, step << 20, 1 << 20, 4 << 20, &hashes);
        }
        let queues = prefetch.scheduler.queues.lock().unwrap();
        let queued = queues.by_stream.get(&7).map_or(0, |pending| pending.len());
        assert_eq!(
            queued,
            queues.reserved.len(),
            "every reserved chunk must be queued exactly once"
        );
        let unique: HashSet<_> = queues.by_stream[&7].iter().copied().collect();
        assert_eq!(unique.len(), queued, "a chunk was enqueued twice");
    }

    #[tokio::test]
    async fn random_access_stalls_do_not_inflate_the_window() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let prefetch = Prefetcher::new(
            Handle::current(),
            Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            None,
        );
        let mut hashes = SparseChunks::new();
        hashes.insert(17, hash(1));
        prefetch.on_read(7, 64 << 20, 128 << 10, 4 << 20, &hashes);
        prefetch.note_stall(7);
        assert_eq!(
            prefetch.streams.lock().unwrap()[&7].window,
            prefetch.min_window
        );
        assert_eq!(prefetch.stats.snapshot().stalls, 0);
    }

    #[tokio::test]
    async fn multi_second_fetch_does_not_expire_a_live_stream() {
        let dir = TempDir::new().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap());
        let prefetch = Prefetcher::new(
            Handle::current(),
            Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
            cache,
            None,
        );
        let mut hashes = SparseChunks::new();
        for index in 0..64 {
            hashes.insert(index, hash(index as u8));
        }
        prefetch.on_read(7, 0, 1 << 20, 4 << 20, &hashes);
        prefetch.note_stall(7);
        let grown = prefetch.streams.lock().unwrap()[&7].window;
        prefetch
            .streams
            .lock()
            .unwrap()
            .get_mut(&7)
            .unwrap()
            .last_hit = Instant::now() - Duration::from_secs(5);

        prefetch.on_read(7, 1 << 20, 1 << 20, 4 << 20, &hashes);
        assert!(prefetch.streams.lock().unwrap()[&7].window >= grown);
    }

    /// The idle-abandon sweep must cancel a sequential stream's queued
    /// (not-yet-started) readahead once the reader goes quiet for longer
    /// than `PREFETCH_ABANDON_IDLE`, without touching the stream's
    /// cursor/window bookkeeping (a resumed reader should not cold-start).
    /// Uses the bare `scheduler()` test helper (no `run()` fetch loop
    /// attached) and drives one sweep pass directly so the assertions are
    /// deterministic instead of racing a real background timer against a
    /// mock store that would otherwise drain the queue via fetch failures.
    #[test]
    fn idle_reader_backlog_is_abandoned_but_stream_state_survives() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()));
        scheduler.enqueue(7, [hash(1), hash(2), hash(3)]);
        assert_eq!(
            scheduler.queues.lock().unwrap().by_stream[&7].len(),
            3,
            "expected the readahead to still be queued, not yet fetched"
        );

        let streams = Arc::new(Mutex::new(HashMap::new()));
        let cursor_before = 4 << 20;
        let window_before = 64 << 20;
        streams.lock().unwrap().insert(
            7,
            Stream {
                cursor: cursor_before,
                window: window_before,
                last_hit: Instant::now() - PREFETCH_ABANDON_IDLE - Duration::from_millis(1),
                sequential: true,
                next_index: 20,
                chunk_bytes: 4 << 20,
            },
        );

        abandon_idle_streams(&streams, &scheduler, Instant::now());

        assert_eq!(
            scheduler.queues.lock().unwrap().by_stream.get(&7),
            None,
            "queued backlog should be fully dropped"
        );
        assert_eq!(scheduler.stats.snapshot().abandoned_chunks, 3);
        assert_eq!(scheduler.stats.snapshot().abandoned, 1);

        // Cursor/window survive: a resumed reader is still sequential and
        // does not cold-start from min_window. next_index rewinds to the
        // cursor so the dropped chunks get re-offered, not skipped.
        let locked = streams.lock().unwrap();
        let stream = &locked[&7];
        assert_eq!(stream.cursor, cursor_before);
        assert_eq!(stream.window, window_before);
        assert!(stream.sequential);
        assert_eq!(stream.next_index, cursor_before / stream.chunk_bytes);
    }

    /// A stream that is idle but has nothing queued (already fully served,
    /// or never had backlog) must not be reported as "abandoned" — that
    /// counter should only reflect readahead actually cancelled.
    #[test]
    fn idle_stream_with_empty_queue_is_not_counted_as_abandoned() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()));
        let streams = Arc::new(Mutex::new(HashMap::new()));
        streams.lock().unwrap().insert(
            7,
            Stream {
                cursor: 4 << 20,
                window: 8 << 20,
                last_hit: Instant::now() - PREFETCH_ABANDON_IDLE - Duration::from_millis(1),
                sequential: true,
                next_index: 1,
                chunk_bytes: 4 << 20,
            },
        );

        abandon_idle_streams(&streams, &scheduler, Instant::now());

        assert_eq!(scheduler.stats.snapshot().abandoned, 0);
        assert_eq!(scheduler.stats.snapshot().abandoned_chunks, 0);
    }

    /// A recently-active stream (idle for less than the threshold) must be
    /// left alone even if it has a live backlog.
    #[test]
    fn recently_active_stream_is_not_abandoned() {
        let dir = TempDir::new().unwrap();
        let scheduler = scheduler(Arc::new(DiskCache::open(dir.path(), 1 << 30).unwrap()));
        scheduler.enqueue(7, [hash(1)]);
        let streams = Arc::new(Mutex::new(HashMap::new()));
        streams.lock().unwrap().insert(
            7,
            Stream {
                cursor: 4 << 20,
                window: 8 << 20,
                last_hit: Instant::now(),
                sequential: true,
                next_index: 1,
                chunk_bytes: 4 << 20,
            },
        );

        abandon_idle_streams(&streams, &scheduler, Instant::now());

        assert_eq!(
            scheduler.queues.lock().unwrap().by_stream[&7].len(),
            1,
            "an actively-read stream's backlog must not be trimmed"
        );
        assert_eq!(scheduler.stats.snapshot().abandoned, 0);
    }
}
