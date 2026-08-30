//! Pinning: keep a subtree fully resident and current (DESIGN.md §7).
//!
//! A pin is **node-local**: each node decides what it keeps, so pins are
//! not replicated through the metadata log. Pinning does three things:
//!
//! 1. **Admission control** — refuse up front if the subtree cannot fit
//!    the cache budget alongside existing pins (reserve-before-accept,
//!    DESIGN.md §9). Refusing early is the whole point: accepting a pin
//!    we cannot honour would silently break the availability promise the
//!    user just asked for.
//! 2. **Eager fetch** — download every chunk of the subtree with bounded
//!    concurrency, marking each `Pinned` so the LRU evictor skips it.
//! 3. **Follow** — when tailing applies a manifest change under a pinned
//!    path, fetch the new chunks. New chunks are pinned; chunks that
//!    dropped out of the subtree are demoted to clean so they can be
//!    evicted normally.

use anyhow::{bail, Context, Result};
use constellation_fs_core::cache::{ChunkState, DiskCache};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_meta::SqliteMeta;
use constellation_store_s3::ChunkStore;
use std::collections::HashSet;
use std::sync::Arc;

/// Parallel chunk fetches while filling a pin. Enough to keep S3
/// pipelined without starving foreground reads.
const FETCH_CONCURRENCY: usize = 8;

/// What a pin needs, computed from the replica.
#[derive(Debug, Default)]
pub struct PinFootprint {
    /// Logical bytes of the files in the subtree.
    pub bytes: u64,
    /// Distinct chunks the subtree references (deduplicated).
    pub chunks: Vec<ChunkHash>,
}

pub struct PinManager {
    meta: Arc<SqliteMeta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    coop: Option<Arc<crate::coop::Coop>>,
}

impl PinManager {
    pub fn new(
        meta: Arc<SqliteMeta>,
        store: Arc<ChunkStore>,
        cache: Arc<DiskCache>,
        coop: Option<Arc<crate::coop::Coop>>,
    ) -> Self {
        Self {
            meta,
            store,
            cache,
            coop,
        }
    }

    /// Resolve a pin path to its inode, rejecting anything unusable with
    /// a message meant for a human at a terminal.
    pub fn resolve(&self, path: &str) -> Result<Ino> {
        if !path.starts_with('/') {
            bail!("pin path must be absolute, got {path:?}");
        }
        self.meta
            .resolve_path(path)
            .with_context(|| format!("resolving {path}"))?
            .ok_or_else(|| anyhow::anyhow!("no such path: {path}"))
    }

    /// Everything under `ino`: distinct chunks and logical size.
    ///
    /// Chunk lists that spilled to their own object are followed, so a
    /// large file's footprint is counted correctly rather than as the one
    /// spill chunk.
    pub async fn footprint(&self, ino: Ino) -> Result<PinFootprint> {
        let manifests = self.meta.subtree_manifests(ino)?;
        let mut seen = HashSet::new();
        let mut out = PinFootprint::default();
        for (_, bytes, size) in manifests {
            out.bytes += size;
            let m = Manifest::decode(&bytes).context("decoding a manifest")?;
            match &m.chunks {
                ChunkInfo::Inline(list) => {
                    for h in list.values() {
                        if seen.insert(*h) {
                            out.chunks.push(*h);
                        }
                    }
                }
                ChunkInfo::Spilled(h) => {
                    // The spill object itself must be resident too, or the
                    // file is unreadable offline.
                    if seen.insert(*h) {
                        out.chunks.push(*h);
                    }
                    let blob = self.get_chunk(h).await?;
                    for c in decode_chunk_list(&blob)
                        .context("decoding a spilled chunk list")?
                        .into_values()
                    {
                        if seen.insert(c) {
                            out.chunks.push(c);
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Pin `path`: admission-check, record, then fill the cache.
    ///
    /// The check compares what is *not yet resident* against the free
    /// budget, because chunks already cached cost nothing extra to keep.
    /// Chunk sizes are not known until a chunk is fetched, so the
    /// estimate uses the subtree's mean chunk size; the per-insert
    /// reserve-before-accept check in `DiskCache` remains the hard guard,
    /// and this one exists to fail fast with an actionable message
    /// instead of half-filling a pin and reporting `CacheFull`.
    pub async fn pin(&self, path: &str) -> Result<String> {
        let ino = self.resolve(path)?;
        let fp = self.footprint(ino).await?;
        let usage = self.cache.usage();
        let missing = fp.chunks.iter().filter(|h| !self.cache.contains(h)).count() as u64;
        let mean_chunk = if fp.chunks.is_empty() {
            0
        } else {
            fp.bytes.div_ceil(fp.chunks.len() as u64)
        };
        let est_incoming = missing * mean_chunk;
        let free = usage.budget.saturating_sub(usage.used);
        if est_incoming > free {
            bail!(
                "pinning {path} needs roughly {est_incoming} more bytes but only {free} of \
                 the {} byte cache budget is free ({} used). Raise --cache-size or unpin \
                 another subtree first.",
                usage.budget,
                usage.used
            );
        }
        self.meta.add_pin(path, ino)?;
        let filled = self.fill(&fp.chunks).await;
        Ok(format!(
            "pinned {path}: {} chunks ({} bytes logical), {} newly fetched",
            fp.chunks.len(),
            fp.bytes,
            filled
        ))
    }

    /// Unpin `path` and demote its chunks so the LRU can reclaim them.
    ///
    /// A chunk shared with a still-pinned subtree stays pinned, which is
    /// why this recomputes the remaining pins rather than blindly
    /// demoting everything it just released.
    pub async fn unpin(&self, path: &str) -> Result<String> {
        if !self.meta.remove_pin(path)? {
            bail!("{path} is not pinned");
        }
        // Chunks still covered by another pin must stay pinned.
        let mut keep = HashSet::new();
        for (_, ino) in self.meta.pins()? {
            if let Ok(fp) = self.footprint(ino).await {
                keep.extend(fp.chunks);
            }
        }
        let mut demoted = 0u64;
        if let Ok(ino) = self.resolve(path) {
            if let Ok(fp) = self.footprint(ino).await {
                for h in fp.chunks {
                    if !keep.contains(&h) && self.cache.set_state(&h, ChunkState::Clean) {
                        demoted += 1;
                    }
                }
            }
        }
        Ok(format!("unpinned {path}: {demoted} chunks now evictable"))
    }

    /// Per-pin status for the control API.
    pub async fn status(&self) -> Vec<constellation_api::PinStatus> {
        let mut out = Vec::new();
        for (path, ino) in self.meta.pins().unwrap_or_default() {
            let fp = self.footprint(ino).await.unwrap_or_default();
            let cached = fp.chunks.iter().filter(|h| self.cache.contains(h)).count();
            out.push(constellation_api::PinStatus {
                path,
                bytes: fp.bytes,
                chunks_cached: cached as u64,
                chunks_total: fp.chunks.len() as u64,
            });
        }
        out
    }

    /// Re-fill every pin. Called after tailing applied foreign records,
    /// so a peer's write under a pinned path becomes locally resident
    /// without waiting for someone to read it.
    pub async fn refresh_all(&self) {
        for (path, ino) in self.meta.pins().unwrap_or_default() {
            match self.footprint(ino).await {
                Ok(fp) => {
                    let n = self.fill(&fp.chunks).await;
                    if n > 0 {
                        tracing::debug!(path, fetched = n, "pin refreshed");
                    }
                }
                Err(e) => tracing::debug!(error = %e, path, "pin refresh failed; will retry"),
            }
        }
    }

    /// Fetch and pin any chunk not already resident. Returns how many
    /// were newly fetched. Errors are logged, not propagated: a pin that
    /// cannot be completed right now is retried on the next refresh, and
    /// failing the whole operation would be worse than a partial fill.
    async fn fill(&self, chunks: &[ChunkHash]) -> u64 {
        let mut fetched = 0u64;
        let mut pending: Vec<ChunkHash> = Vec::new();
        for h in chunks {
            if self.cache.contains(h) {
                // Already here: just make sure the evictor skips it.
                self.cache.set_state(h, ChunkState::Pinned);
            } else {
                pending.push(*h);
            }
        }
        // Bounded concurrency so filling a pin cannot monopolise the
        // connection pool that foreground reads share.
        let mut tasks = tokio::task::JoinSet::new();
        let mut queue = pending.into_iter();
        loop {
            while tasks.len() < FETCH_CONCURRENCY {
                match queue.next() {
                    Some(h) => {
                        let store = self.store.clone();
                        let coop = self.coop.clone();
                        tasks.spawn(async move {
                            let data = if let Some(coop) = coop {
                                coop.fetch(&h).await.map_err(|e| anyhow::anyhow!("{e}"))
                            } else {
                                store
                                    .get_chunk(&h)
                                    .await
                                    .map_err(|e| anyhow::anyhow!("{e}"))
                            };
                            (h, data)
                        });
                    }
                    None => break,
                }
            }
            let Some(joined) = tasks.join_next().await else {
                break;
            };
            let Ok((h, data)) = joined else { continue };
            match data {
                Ok(d) => match self.cache.insert(&h, &d, ChunkState::Pinned) {
                    Ok(()) => fetched += 1,
                    Err(e) => {
                        tracing::warn!(error = %e, chunk = %h.to_hex(),
                            "could not cache a pinned chunk")
                    }
                },
                Err(e) => {
                    tracing::debug!(error = %e, chunk = %h.to_hex(),
                        "pinned chunk fetch failed; will retry")
                }
            }
        }
        fetched
    }

    async fn get_chunk(&self, hash: &ChunkHash) -> Result<Vec<u8>> {
        if let Ok(Some(d)) = self.cache.get(hash) {
            return Ok(d);
        }
        if let Some(coop) = &self.coop {
            return coop.fetch(hash).await;
        }
        Ok(self.store.get_chunk(hash).await?)
    }
}
