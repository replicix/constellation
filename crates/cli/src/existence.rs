//! Mount-time S3 existence hint for cold, high-dedup uploads (phase 8c).
//!
//! A complete LIST snapshot may prove a miss and avoid a HEAD before a
//! conditional create. Hits are advisory only: they select `Probe`, whose
//! HEAD (and PUT on a 404) remains the correctness operation. The filter is
//! add-only after seeding; bucket GC can therefore leave stale hits, but
//! those cost only a HEAD and can never acknowledge an upload by themselves.

use constellation_fs_core::ChunkHash;
use constellation_net::bloom::{Bloom, BITS_PER_ENTRY};
use constellation_store_s3::ChunkStore;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_BYTES: usize = 4 * 1024 * 1024;
const LIST_CONCURRENCY: usize = 8;

fn enabled(name: &str) -> bool {
    !std::env::var(name).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        )
    })
}

pub struct Existence {
    bloom: Mutex<Bloom>,
    max_entries: usize,
    list_enabled: bool,
    peer_hint_enabled: bool,
    complete: AtomicBool,
    listed: AtomicU64,
    bloom_hits: AtomicU64,
    bloom_misses: AtomicU64,
    peer_hints: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExistenceReport {
    pub listed: u64,
    pub complete: bool,
    pub bloom_hits: u64,
    pub bloom_misses: u64,
    pub peer_hints: u64,
}

impl Existence {
    pub fn from_env() -> Arc<Self> {
        let bytes = std::env::var("CONSTELLATION_EXISTENCE_BLOOM_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_BYTES)
            .max(8);
        Self::new(
            bytes,
            enabled("CONSTELLATION_EXISTENCE_LIST"),
            enabled("CONSTELLATION_EXISTENCE_PEER_HINT"),
        )
    }

    pub(crate) fn new(bytes: usize, list_enabled: bool, peer_hint_enabled: bool) -> Arc<Self> {
        let max_entries = bytes.saturating_mul(8) / BITS_PER_ENTRY;
        Arc::new(Self {
            bloom: Mutex::new(Bloom::with_capacity_and_max_bytes(max_entries, bytes)),
            max_entries,
            list_enabled,
            peer_hint_enabled,
            complete: AtomicBool::new(false),
            listed: AtomicU64::new(0),
            bloom_hits: AtomicU64::new(0),
            bloom_misses: AtomicU64::new(0),
            peer_hints: AtomicU64::new(0),
        })
    }

    /// Spawn after mount setup. The short delay lets `fuser::mount` finish
    /// attaching before LIST work begins, while uploads remain free to use
    /// the ordinary adaptive fallback until `complete` flips.
    pub fn spawn_seed(self: &Arc<Self>, store: Arc<ChunkStore>, rt: &tokio::runtime::Runtime) {
        if !self.list_enabled {
            return;
        }
        let this = self.clone();
        rt.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let started = Instant::now();
            let sink = this.clone();
            match constellation_store_s3::scan_chunk_hashes(
                store.inner().clone(),
                this.max_entries,
                LIST_CONCURRENCY,
                move |hash| sink.insert(&hash),
            )
            .await
            {
                Ok(scan) => {
                    let listed = scan.listed as u64;
                    this.listed.store(listed, Ordering::Release);
                    this.complete.store(scan.complete, Ordering::Release);
                    tracing::info!(
                        listed,
                        complete = scan.complete,
                        elapsed_ms = started.elapsed().as_millis(),
                        "S3 existence LIST seed finished"
                    );
                }
                Err(error) => {
                    tracing::warn!(%error, "S3 existence LIST seed failed; uploads use adaptive probes");
                }
            }
        });
    }

    pub fn peer_hints_enabled(&self) -> bool {
        self.peer_hint_enabled
    }

    pub fn note_peer_hint(&self) {
        self.peer_hints.fetch_add(1, Ordering::Relaxed);
    }

    /// `Some(true)` selects a confirming probe; `Some(false)` is a proven
    /// miss from a complete seed. `None` retains the adaptive fallback.
    pub fn contains(&self, hash: &ChunkHash) -> Option<bool> {
        if !self.complete.load(Ordering::Acquire) {
            return None;
        }
        let hit = self.bloom.lock().unwrap().contains(&hash.0);
        if hit {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.bloom_misses.fetch_add(1, Ordering::Relaxed);
        }
        Some(hit)
    }

    pub fn insert(&self, hash: &ChunkHash) {
        self.bloom.lock().unwrap().insert(&hash.0);
    }

    pub fn report(&self) -> ExistenceReport {
        ExistenceReport {
            listed: self.listed.load(Ordering::Acquire),
            complete: self.complete.load(Ordering::Acquire),
            bloom_hits: self.bloom_hits.load(Ordering::Relaxed),
            bloom_misses: self.bloom_misses.load(Ordering::Relaxed),
            peer_hints: self.peer_hints.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn seed_for_test(&self, hashes: &[ChunkHash], complete: bool) {
        let mut bloom = self.bloom.lock().unwrap();
        for hash in hashes {
            bloom.insert(&hash.0);
        }
        drop(bloom);
        self.listed.store(hashes.len() as u64, Ordering::Release);
        self.complete.store(complete, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_seed_never_proves_a_miss() {
        let existence = Existence::new(64, true, true);
        assert_eq!(existence.contains(&ChunkHash::of(b"unknown")), None);
    }

    #[test]
    fn complete_seed_reports_hits_and_misses() {
        let existence = Existence::new(64, true, true);
        let known = ChunkHash::of(b"known");
        existence.insert(&known);
        existence.complete.store(true, Ordering::Release);
        assert_eq!(existence.contains(&known), Some(true));
        assert_eq!(existence.contains(&ChunkHash::of(b"unknown")), Some(false));
        let report = existence.report();
        assert_eq!((report.bloom_hits, report.bloom_misses), (1, 1));
    }
}
