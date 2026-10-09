//! Existence hints for cold, high-dedup uploads (phase 8c, plan 26 step 8).
//!
//! Two sources, both advisory, both one-sided. The replica's `chunk_ref`
//! table is the authoritative one: replay maintains it from *foreign*
//! records as well as local ones, so it names every chunk hash referenced
//! anywhere in the cluster, and it is already on disk before the first
//! upload runs. The in-process bloom sits in front of it as a cache for the
//! hashes this node uploaded itself or was hinted about by a peer digest —
//! things the replica learns only once the manifest is journaled.
//!
//! A hit selects `Probe`, whose HEAD (and PUT on a 404) remains the
//! correctness operation, so a false positive costs one HEAD and can never
//! acknowledge an upload by itself. Bucket GC may leave stale hits behind
//! for the same reason: they are harmless.
//!
//! There is deliberately **no negative answer**. Proving a hash absent from
//! the bucket would take a complete LIST of every chunk object, which is
//! what this module used to do at mount time: 59.5k objects took 23 s and it
//! is O(hours) at ten million, against a request class that is the most
//! expensive there is (12.5x a GET on AWS) and whose 1000-key page costs
//! ~331 ms from Europe to us-west-2, ~344 ms to OVH Milan and ~171 ms
//! same-region. Nothing downstream needs the negative: an unhinted upload
//! falls back to the adaptive probe policy, and `Create` is a conditional
//! `If-None-Match: *` PUT that fails safely on a hash already present.

use constellation_fs_core::ChunkHash;
use constellation_meta::Meta;
use constellation_net::bloom::{Bloom, BITS_PER_ENTRY};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const DEFAULT_BYTES: usize = 4 * 1024 * 1024;

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
    meta: Option<Arc<Meta>>,
    peer_hint_enabled: bool,
    bloom_hits: AtomicU64,
    chunk_ref_hits: AtomicU64,
    misses: AtomicU64,
    peer_hints: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ExistenceReport {
    pub bloom_hits: u64,
    pub chunk_ref_hits: u64,
    /// Upload decisions no hint source could answer; these take the
    /// adaptive probe fallback.
    pub misses: u64,
    pub peer_hints: u64,
}

impl Existence {
    /// Mount-path constructor: the replica is the hint source, and it is
    /// consulted lazily per upload rather than copied into the bloom, so
    /// there is no startup cost proportional to the bucket.
    pub fn with_meta(meta: Arc<Meta>) -> Arc<Self> {
        let bytes = std::env::var("CONSTELLATION_EXISTENCE_BLOOM_BYTES")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_BYTES)
            .max(8);
        Self::new(
            bytes,
            enabled("CONSTELLATION_EXISTENCE_PEER_HINT"),
            Some(meta),
        )
    }

    pub(crate) fn new(bytes: usize, peer_hint_enabled: bool, meta: Option<Arc<Meta>>) -> Arc<Self> {
        let max_entries = bytes.saturating_mul(8) / BITS_PER_ENTRY;
        Arc::new(Self {
            bloom: Mutex::new(Bloom::with_capacity_and_max_bytes(max_entries, bytes)),
            meta,
            peer_hint_enabled,
            bloom_hits: AtomicU64::new(0),
            chunk_ref_hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            peer_hints: AtomicU64::new(0),
        })
    }

    pub fn peer_hints_enabled(&self) -> bool {
        self.peer_hint_enabled
    }

    pub fn note_peer_hint(&self) {
        self.peer_hints.fetch_add(1, Ordering::Relaxed);
    }

    /// `true` means "probably in the bucket already" and selects a
    /// confirming probe. `false` means only "no hint" — never "absent" —
    /// and leaves the adaptive fallback in charge.
    pub fn contains(&self, hash: &ChunkHash) -> bool {
        if self.bloom.lock().unwrap().contains(&hash.0) {
            self.bloom_hits.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        if let Some(meta) = self.meta.as_ref() {
            match meta.chunk_ref_exists(hash) {
                Ok(true) => {
                    self.chunk_ref_hits.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                Ok(false) => {}
                // The replica is a hint, not a dependency: a reader error
                // degrades the upload to the adaptive probe, not to a
                // failure.
                Err(error) => tracing::debug!(
                    %error,
                    "chunk_ref existence probe failed; upload uses the adaptive fallback"
                ),
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        false
    }

    pub fn insert(&self, hash: &ChunkHash) {
        self.bloom.lock().unwrap().insert(&hash.0);
    }

    pub fn report(&self) -> ExistenceReport {
        ExistenceReport {
            bloom_hits: self.bloom_hits.load(Ordering::Relaxed),
            chunk_ref_hits: self.chunk_ref_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            peer_hints: self.peer_hints.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::manifest::{ChunkInfo, Manifest};
    use constellation_meta::LogRecord;

    /// Manifest bytes that actually decode, so that replay's `chunk_ref`
    /// bookkeeping can extract the hash from them.
    fn manifest_of(hash: ChunkHash) -> Vec<u8> {
        Manifest {
            layout: constellation_fs_core::ChunkLayout::new(4096),
            file_len: 4096,
            chunks: ChunkInfo::Inline([(0u64, hash)].into_iter().collect()),
        }
        .encode()
    }

    /// A replica that replayed someone else's manifest already knows the
    /// hash is in the bucket, so the upload path can pick `Probe` with no
    /// LIST anywhere in the mount.
    #[test]
    fn chunk_ref_hit_selects_probe_without_a_list() {
        let hash = ChunkHash::of(b"written by another node");
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let existence = Existence::new(64, true, Some(meta.clone()));
        assert!(
            !existence.contains(&hash),
            "nothing references the hash yet"
        );

        meta.apply_records(&[
            LogRecord::Create {
                parent: constellation_fs_core::types::ROOT_INO,
                name: "remote".into(),
                ino: 4242,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            },
            LogRecord::WriteManifest {
                ino: 4242,
                base_manifest: None,
                manifest: manifest_of(hash),
                size: 4096,
                time_ns: 2,
                mtime_ns: 2,
            },
        ])
        .unwrap();

        assert!(existence.contains(&hash));
        let report = existence.report();
        assert_eq!(
            (report.chunk_ref_hits, report.bloom_hits, report.misses),
            (1, 0, 1)
        );
    }

    /// Without a replica behind it the bloom is the only source, and it
    /// only ever answers for what this node put in it.
    #[test]
    fn bloom_answers_only_for_inserted_hashes() {
        let existence = Existence::new(64, true, None);
        let known = ChunkHash::of(b"known");
        assert!(!existence.contains(&known));
        existence.insert(&known);
        assert!(existence.contains(&known));
        assert!(!existence.contains(&ChunkHash::of(b"unknown")));
        let report = existence.report();
        assert_eq!((report.bloom_hits, report.misses), (1, 2));
    }
}
