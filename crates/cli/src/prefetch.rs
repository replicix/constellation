//! Adaptive sequential readahead (DESIGN.md §7).

use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::SparseChunks;
use constellation_fs_core::{ChunkHash, Ino};
use constellation_store_s3::ChunkStore;
use constellation_upload_concurrency::{AdaptiveConcurrency, ConcurrencyGate};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

const REORDER_WINDOW: u64 = 16 << 20;
const STREAM_IDLE: Duration = Duration::from_secs(2);
const MAX_STREAMS: usize = 512;
const DEFAULT_MIN_WINDOW: u64 = 8 << 20;
const DEFAULT_MAX_WINDOW: u64 = 256 << 20;
const DEFAULT_MAX_CONCURRENCY: usize = 128;
const INITIAL_CONCURRENCY: usize = 8;

struct Stream {
    cursor: u64,
    window: u64,
    last_hit: Instant,
}

struct Queues {
    by_stream: HashMap<Ino, VecDeque<ChunkHash>>,
    ready: VecDeque<Ino>,
    reserved: HashSet<ChunkHash>,
    active: HashSet<ChunkHash>,
}

struct Scheduler {
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    coop: Option<Arc<crate::coop::Coop>>,
    gate: Arc<ConcurrencyGate>,
    controller: Option<Mutex<AdaptiveConcurrency>>,
    queues: Mutex<Queues>,
    wake: tokio::sync::Notify,
}

impl Scheduler {
    fn enqueue(&self, ino: Ino, hashes: impl IntoIterator<Item = ChunkHash>) {
        let mut queues = self.queues.lock().unwrap();
        for hash in hashes {
            if self.cache.contains(&hash) || !queues.reserved.insert(hash) {
                continue;
            }
            let queue = queues.by_stream.entry(ino).or_default();
            let was_empty = queue.is_empty();
            queue.push_back(hash);
            if was_empty {
                queues.ready.push_back(ino);
            }
        }
        drop(queues);
        self.wake.notify_one();
    }

    fn pop(&self) -> Option<ChunkHash> {
        let mut queues = self.queues.lock().unwrap();
        let ino = queues.ready.pop_front()?;
        let queue = queues.by_stream.get_mut(&ino).unwrap();
        let hash = queue.pop_front().unwrap();
        if queue.is_empty() {
            queues.by_stream.remove(&ino);
        } else {
            queues.ready.push_back(ino);
        }
        queues.active.insert(hash);
        Some(hash)
    }

    fn finish(&self, hash: &ChunkHash) {
        let mut queues = self.queues.lock().unwrap();
        queues.active.remove(hash);
        queues.reserved.remove(hash);
        self.wake.notify_one();
    }

    fn forget(&self, ino: Ino) {
        let mut queues = self.queues.lock().unwrap();
        if let Some(pending) = queues.by_stream.remove(&ino) {
            for hash in pending {
                queues.reserved.remove(&hash);
            }
        }
        queues.ready.retain(|candidate| *candidate != ino);
    }

    fn is_inflight(&self, hash: &ChunkHash) -> bool {
        self.queues.lock().unwrap().active.contains(hash)
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
    }

    fn record_error(&self) {
        let Some(controller) = &self.controller else {
            return;
        };
        let target = controller.lock().unwrap().on_error(Instant::now());
        self.gate.set_target(target);
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
                                .get_chunk_to_writer_e2e(&hash, &mut cipher_spill, &mut spill)
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
    streams: Mutex<HashMap<Ino, Stream>>,
    scheduler: Arc<Scheduler>,
    min_window: u64,
    max_window: u64,
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
        .clamp(1, DEFAULT_MAX_CONCURRENCY);
        let fixed = std::env::var("CONSTELLATION_PREFETCH_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .map(|value| value.clamp(1, max_concurrency));
        let initial = fixed.unwrap_or_else(|| INITIAL_CONCURRENCY.min(max_concurrency));
        let scheduler = Arc::new(Scheduler {
            store,
            cache,
            coop,
            gate: Arc::new(ConcurrencyGate::new(initial)),
            controller: fixed.map_or_else(
                || {
                    Some(Mutex::new(AdaptiveConcurrency::new(
                        initial,
                        1,
                        max_concurrency,
                    )))
                },
                |_| None,
            ),
            queues: Mutex::new(Queues {
                by_stream: HashMap::new(),
                ready: VecDeque::new(),
                reserved: HashSet::new(),
                active: HashSet::new(),
            }),
            wake: tokio::sync::Notify::new(),
        });
        rt.spawn(scheduler.clone().run());
        Self {
            streams: Mutex::new(HashMap::new()),
            scheduler,
            min_window,
            max_window,
        }
    }

    /// Called on every read. Detects sequential access and schedules
    /// background fetches for upcoming chunks.
    pub fn on_read(&self, ino: Ino, offset: u64, len: u64, chunk_size: u32, hashes: &SparseChunks) {
        let now = Instant::now();
        let (sequential, cursor, window) = {
            let mut streams = self.streams.lock().unwrap();
            streams.retain(|_, stream| now.duration_since(stream.last_hit) <= STREAM_IDLE);
            if streams.len() >= MAX_STREAMS && !streams.contains_key(&ino) {
                if let Some(oldest) = streams
                    .iter()
                    .min_by_key(|(_, stream)| stream.last_hit)
                    .map(|(ino, _)| *ino)
                {
                    streams.remove(&oldest);
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
                    (true, stream.cursor, stream.window)
                }
                Some(stream) => {
                    *stream = Stream {
                        cursor: end,
                        window: self.min_window,
                        last_hit: now,
                    };
                    (false, end, self.min_window)
                }
                None => {
                    streams.insert(
                        ino,
                        Stream {
                            cursor: end,
                            window: self.min_window,
                            last_hit: now,
                        },
                    );
                    (offset == 0, end, self.min_window)
                }
            }
        };
        if !sequential || hashes.is_empty() {
            return;
        }
        let chunk_size = u64::from(chunk_size);
        let first = cursor.div_ceil(chunk_size);
        let last = cursor.saturating_add(window).div_ceil(chunk_size);
        self.scheduler.enqueue(
            ino,
            (first..last).filter_map(|index| hashes.get(&index).copied()),
        );
    }

    /// Grow a live stream's byte window when a demand read waits for data.
    pub fn note_stall(&self, ino: Ino) {
        if let Some(stream) = self.streams.lock().unwrap().get_mut(&ino) {
            stream.window = stream.window.saturating_mul(2).min(self.max_window);
        }
    }

    /// Whether a background fetch for this chunk is currently running.
    /// The read path waits for it instead of issuing a duplicate GET.
    pub fn is_inflight(&self, hash: &ChunkHash) -> bool {
        self.scheduler.is_inflight(hash)
    }

    /// Forget an inode's cursor (last close).
    pub fn forget(&self, ino: Ino) {
        self.streams.lock().unwrap().remove(&ino);
        self.scheduler.forget(ino);
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
            queues: Mutex::new(Queues {
                by_stream: HashMap::new(),
                ready: VecDeque::new(),
                reserved: HashSet::new(),
                active: HashSet::new(),
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
}
