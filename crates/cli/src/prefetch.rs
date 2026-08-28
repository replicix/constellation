//! Sequential readahead (DESIGN.md §7): when a file is read
//! sequentially, the next chunks are fetched into the disk cache in the
//! background so the reader never stalls on S3 latency.

use anyhow::anyhow;
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_store_s3::ChunkStore;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

/// How many chunks ahead of the read cursor to keep in flight.
const DEPTH: u64 = 8;

pub struct Prefetcher {
    rt: Handle,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    coop: Option<Arc<crate::coop::Coop>>,
    /// Next expected sequential offset per inode.
    cursors: Mutex<HashMap<Ino, u64>>,
    /// Chunks currently being fetched (dedup across reads and inodes).
    inflight: Arc<Mutex<HashSet<ChunkHash>>>,
}

impl Prefetcher {
    pub fn new(
        rt: Handle,
        store: Arc<ChunkStore>,
        cache: Arc<DiskCache>,
        coop: Option<Arc<crate::coop::Coop>>,
    ) -> Self {
        Self {
            rt,
            store,
            cache,
            coop,
            cursors: Mutex::new(HashMap::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Called on every read. Detects sequential access and schedules
    /// background fetches for upcoming chunks.
    pub fn on_read(&self, ino: Ino, offset: u64, len: u64, chunk_size: u32, hashes: &[ChunkHash]) {
        let sequential = {
            let mut cursors = self.cursors.lock().unwrap();
            let seq = offset == 0 || cursors.get(&ino) == Some(&offset);
            cursors.insert(ino, offset + len);
            seq
        };
        if !sequential || hashes.is_empty() {
            return;
        }
        let last_read_chunk = (offset + len.max(1) - 1) / chunk_size as u64;
        for idx in last_read_chunk + 1..=last_read_chunk + DEPTH {
            let Some(hash) = hashes.get(idx as usize).copied() else {
                break;
            };
            if self.cache.contains(&hash) {
                continue;
            }
            if !self.inflight.lock().unwrap().insert(hash) {
                continue; // already being fetched
            }
            let (store, cache, inflight, coop) = (
                self.store.clone(),
                self.cache.clone(),
                self.inflight.clone(),
                self.coop.clone(),
            );
            self.rt.spawn(async move {
                let result = if let Some(coop) = coop {
                    coop.fetch(&hash).await.map(|_| ())
                } else {
                    match store.get_chunk(&hash).await {
                        Ok(data) => {
                            let _ = cache.insert(&hash, &data, ChunkState::Clean);
                            Ok(())
                        }
                        Err(e) => Err(anyhow!("{e}")),
                    }
                };
                if let Err(e) = result {
                    tracing::debug!(chunk = %hash.to_hex(), error = %e, "prefetch failed");
                }
                inflight.lock().unwrap().remove(&hash);
            });
        }
    }

    /// Whether a background fetch for this chunk is currently running.
    /// The read path waits for it instead of issuing a duplicate GET.
    pub fn is_inflight(&self, hash: &ChunkHash) -> bool {
        self.inflight.lock().unwrap().contains(hash)
    }

    /// Forget an inode's cursor (last close).
    pub fn forget(&self, ino: Ino) {
        self.cursors.lock().unwrap().remove(&ino);
    }
}
