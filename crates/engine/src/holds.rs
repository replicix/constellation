//! Open-orphan holds (DESIGN.md §3 "Unlink while open", §14 rule 4).
//!
//! An `unlink` removes the name everywhere at once; a node that still has
//! the file open keeps serving its handles from the orphan record in its
//! own replica (`meta.orphans`, node-local). Nothing in the bucket names
//! that inode any more — not the tree, not any live manifest another
//! node's GC round can see — so the chunks would be candidates the moment
//! they were older than `gc.horizon`, however recent the unlink. The
//! hold is the bucket-side claim that closes that gap: one object per
//! node, `holds/<node>.json`, listing the chunk hashes of every orphan
//! this node has open, with an expiry. A GC round keeps every hash a
//! live hold names, both when it marks and again right before it deletes
//! (`gc::sweep_chunks`).
//!
//! # Cadence, and what it does and does not guarantee
//!
//! The object is rewritten when the held set changes and re-stamped
//! every `refresh` while it is non-empty; it is deleted when the set
//! empties and at a clean unmount. A crashed node stops renewing and its
//! hold expires after `ttl`, so nothing leaks. The writer is nudged by
//! the sync task after every applied segment, so a foreign unlink of a
//! locally open file is usually claimed within a round trip, not a
//! refresh period.
//!
//! What this protects is exact: a chunk a live hold names is not
//! deleted. What is *not* exact is when a node gets to write its hold. A
//! GC round waits one lease TTL between marking and deleting and re-reads
//! holds right before deleting, so a node that has applied the unlink by
//! then and can reach S3 is covered. A node that applies it later (it
//! lags the log by more than a lease TTL, or is partitioned from S3)
//! may find the chunks gone when it next reads uncached bytes: `EIO` on
//! that handle. That is the edge case §3 accepts; it never dangles a
//! committed reference, because no committed state names an orphan.
//!
//! # The reaper
//!
//! The same pass reaps every orphan *no* view has open. A foreign unlink
//! of a file nobody here has open used to leave its record in `orphans`
//! until the next bootstrap (only the FUSE `release` path reaped, and it
//! only sees inodes that were open); now the record — and the local
//! `chunk_ref` rows that keep the chunks in the existence hint — go as
//! soon as this pass notices. Reading the orphan set *before* the open
//! set is what makes that safe: an inode can only be opened while it has
//! a name, so a handle on an orphan present at the first read is either
//! counted at the second read or already released.

use anyhow::{Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_meta::{Meta, MetaStore};
use constellation_store_s3::{layout, ChunkStore};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// What a mounted view reports: the inodes with at least one open
/// handle.
pub trait OpenHandles: Send + Sync {
    fn open_inos(&self) -> Vec<Ino>;
}

/// Every mounted view's open-handle table, weakly (a view goes away with
/// its FUSE session), mirroring `locks::LockFlushers`.
#[derive(Default)]
pub struct HoldSources {
    views: Mutex<Vec<(u64, Weak<dyn OpenHandles>)>>,
}

impl HoldSources {
    pub fn register(&self, id: u64, view: Weak<dyn OpenHandles>) {
        let mut v = self.views.lock().unwrap();
        v.retain(|(_, w)| w.strong_count() > 0);
        v.push((id, view));
    }

    pub fn unregister(&self, id: u64) {
        self.views.lock().unwrap().retain(|(i, _)| *i != id);
    }

    /// Whether any live view has `ino` open.
    pub fn is_open(&self, ino: Ino) -> bool {
        self.open_inos().contains(&ino)
    }

    /// The union of every live view's open inodes.
    pub fn open_inos(&self) -> HashSet<Ino> {
        let views: Vec<Arc<dyn OpenHandles>> = self
            .views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, w)| w.upgrade())
            .collect();
        views.iter().flat_map(|v| v.open_inos()).collect()
    }
}

/// The hold object's body. `chunks` are hex chunk hashes: the GC
/// readers (`gc::hold_roots`, `mtree_gc::hold_hashes`) collect every
/// 64-hex string in the document, whatever the key, so the shape can
/// grow without touching them. `inodes` are for operators (`fsck`,
/// debugging): decimal, so they never parse as hashes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldBody {
    pub v: u32,
    pub node: u64,
    pub expires_unix_ms: i64,
    pub inodes: Vec<Ino>,
    pub chunks: Vec<String>,
}

pub const HOLD_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct HoldConfig {
    /// Re-stamp cadence while the held set is non-empty.
    pub refresh: Duration,
    /// How far ahead each write sets `expires_unix_ms`.
    pub ttl: Duration,
}

impl HoldConfig {
    /// `CONSTELLATION_HOLD_REFRESH_MS` (default: half the lease TTL); the
    /// expiry is three refresh periods out, so one missed or slow
    /// refresh never lets a live hold lapse.
    pub fn from_env(lease_ttl_ms: u64) -> Self {
        let refresh_ms = std::env::var("CONSTELLATION_HOLD_REFRESH_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or((lease_ttl_ms / 2).max(1));
        Self {
            refresh: Duration::from_millis(refresh_ms),
            ttl: Duration::from_millis(refresh_ms.saturating_mul(3)),
        }
    }
}

/// This node's hold writer and orphan reaper.
pub struct Holds {
    store: Arc<dyn ObjectStore>,
    chunks: Arc<ChunkStore>,
    meta: Arc<Meta>,
    node_id: u64,
    sources: Arc<HoldSources>,
    config: HoldConfig,
    /// The sync task pokes this after applying segments.
    nudge: tokio::sync::Notify,
    /// Whether a hold object of ours is (as far as we know) in the
    /// bucket. Starts `true`: a previous incarnation may have left one,
    /// and the first pass must clear it if nothing is held now.
    published: AtomicBool,
    /// The last body written, to skip rewrites of an unchanged set until
    /// its refresh is due.
    last: Mutex<Option<(HoldBody, std::time::Instant)>>,
}

impl Holds {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        chunks: Arc<ChunkStore>,
        meta: Arc<Meta>,
        node_id: u64,
        sources: Arc<HoldSources>,
        config: HoldConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            store,
            chunks,
            meta,
            node_id,
            sources,
            config,
            nudge: tokio::sync::Notify::new(),
            published: AtomicBool::new(true),
            last: Mutex::new(None),
        })
    }

    /// Run a pass soon (the sync task, after applied segments; a view,
    /// after a release of an orphan).
    pub fn nudge(&self) {
        self.nudge.notify_one();
    }

    /// Every view's open-handle table.
    pub fn sources(&self) -> &HoldSources {
        &self.sources
    }

    /// The orphans some view has open right now, without reaping the
    /// rest: what a namespace rebuild must carry across
    /// (`authority_driver::rebuild_replica`). Orphans before opens, as
    /// in [`Self::held_orphans`].
    pub fn open_orphans(&self) -> Result<HashSet<Ino>> {
        let orphans = self.meta.orphans().context("listing orphans")?;
        if orphans.is_empty() {
            return Ok(HashSet::new());
        }
        let open = self.sources.open_inos();
        Ok(orphans
            .into_iter()
            .filter(|ino| open.contains(ino))
            .collect())
    }

    /// The periodic loop, until `stop`.
    pub async fn run(self: Arc<Self>, stop: Arc<AtomicBool>) {
        loop {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            if let Err(error) = self.refresh_once().await {
                tracing::warn!(error = %format!("{error:#}"), "open-orphan hold refresh failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.refresh) => {}
                _ = self.nudge.notified() => {}
            }
        }
    }

    /// The inodes this node currently holds open as orphans, after
    /// reaping the orphans no view has open. Orphans first, then opens:
    /// see the module doc.
    fn held_orphans(&self) -> Result<Vec<Ino>> {
        let orphans = self.meta.orphans().context("listing orphans")?;
        if orphans.is_empty() {
            return Ok(Vec::new());
        }
        let open = self.sources.open_inos();
        let mut held = Vec::new();
        for ino in orphans {
            if open.contains(&ino) {
                held.push(ino);
            } else {
                self.meta
                    .reap_orphan(ino)
                    .with_context(|| format!("reaping orphan inode {ino}"))?;
            }
        }
        held.sort_unstable();
        Ok(held)
    }

    /// Every chunk hash the held orphans' manifests name (the spill chunk
    /// of a spilled list included, as `gc::live_roots` counts them).
    async fn held_chunks(&self, inodes: &[Ino]) -> Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        for &ino in inodes {
            let Some(bytes) = self.meta.manifest(ino)? else {
                continue;
            };
            for hash in manifest_chunk_hashes(&self.chunks, &bytes).await? {
                out.insert(hash.to_hex());
            }
        }
        Ok(out)
    }

    /// One pass: compute the held set, then write, re-stamp or delete
    /// the hold object as the set and the cadence require.
    pub async fn refresh_once(&self) -> Result<()> {
        let inodes = self.held_orphans()?;
        let key = layout::hold(self.node_id);
        if inodes.is_empty() {
            if self.published.swap(false, Ordering::Relaxed) {
                match self.store.delete(&key).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => {
                        self.published.store(true, Ordering::Relaxed);
                        return Err(error).context("withdrawing the open-orphan hold");
                    }
                }
                *self.last.lock().unwrap() = None;
                tracing::debug!(node = self.node_id, "open-orphan hold withdrawn");
            }
            return Ok(());
        }
        let chunks: Vec<String> = self.held_chunks(&inodes).await?.into_iter().collect();
        let now = constellation_store_s3::lease::now_unix_ms();
        let body = HoldBody {
            v: HOLD_VERSION,
            node: self.node_id,
            expires_unix_ms: now + self.config.ttl.as_millis() as i64,
            inodes,
            chunks,
        };
        let unchanged_and_fresh = self
            .last
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|(last, at)| {
                last.inodes == body.inodes
                    && last.chunks == body.chunks
                    && at.elapsed() < self.config.refresh
            });
        if unchanged_and_fresh && self.published.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.store
            .put(&key, PutPayload::from(serde_json::to_vec(&body)?))
            .await
            .context("writing the open-orphan hold")?;
        self.published.store(true, Ordering::Relaxed);
        tracing::debug!(
            node = self.node_id,
            inodes = body.inodes.len(),
            chunks = body.chunks.len(),
            "open-orphan hold written"
        );
        *self.last.lock().unwrap() = Some((body, std::time::Instant::now()));
        Ok(())
    }

    /// Clean unmount: withdraw whatever this node holds (best effort; the
    /// TTL reclaims it otherwise).
    pub async fn withdraw(&self) {
        let key = layout::hold(self.node_id);
        match self.store.delete(&key).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                self.published.store(false, Ordering::Relaxed);
            }
            Err(error) => {
                tracing::debug!(%error, "withdrawing the open-orphan hold failed; the TTL reclaims it")
            }
        }
    }
}

/// The chunk hashes a manifest names: its inline map, or the spill chunk
/// plus everything the spilled list names.
pub async fn manifest_chunk_hashes(chunks: &ChunkStore, bytes: &[u8]) -> Result<Vec<ChunkHash>> {
    let manifest = Manifest::decode(bytes)?;
    Ok(match manifest.chunks {
        ChunkInfo::Inline(hashes) => hashes.into_values().collect(),
        ChunkInfo::Spilled(spill) => {
            let mut out = vec![spill];
            out.extend(decode_chunk_list(&chunks.get_chunk(&spill).await?)?.into_values());
            out
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::manifest::{ChunkInfo, Manifest};
    use constellation_fs_core::types::ROOT_INO;
    use constellation_fs_core::ChunkLayout;
    use object_store::memory::InMemory;

    struct Opens(Mutex<Vec<Ino>>);

    impl OpenHandles for Opens {
        fn open_inos(&self) -> Vec<Ino> {
            self.0.lock().unwrap().clone()
        }
    }

    fn inline_manifest(hash: ChunkHash) -> Vec<u8> {
        Manifest {
            layout: ChunkLayout::new(4096),
            file_len: 4,
            chunks: ChunkInfo::Inline([(0u64, hash)].into_iter().collect()),
        }
        .encode()
    }

    /// An orphan a view has open is claimed in `holds/<node>.json` with
    /// its chunk hashes and a future expiry; once released it is reaped
    /// and the hold withdrawn. An orphan nobody has open is reaped at
    /// once and never claimed.
    #[tokio::test]
    async fn an_open_orphan_is_held_and_a_released_one_is_reaped() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(7).unwrap();
        let hash = ChunkHash::of(b"held bytes");
        let open = meta.create(ROOT_INO, "open", 0o644, 0, 0).unwrap();
        meta.set_manifest(open.ino, &inline_manifest(hash), 4)
            .unwrap();
        let closed = meta.create(ROOT_INO, "closed", 0o644, 0, 0).unwrap();
        meta.set_manifest(
            closed.ino,
            &inline_manifest(ChunkHash::of(b"closed bytes")),
            4,
        )
        .unwrap();
        let view = Arc::new(Opens(Mutex::new(vec![open.ino])));
        let sources = Arc::new(HoldSources::default());
        sources.register(1, Arc::downgrade(&view) as Weak<dyn OpenHandles>);
        const TTL: Duration = Duration::from_millis(150);
        let holds = Holds::new(
            store.clone(),
            chunks,
            meta.clone(),
            7,
            sources,
            HoldConfig {
                refresh: Duration::from_millis(50),
                ttl: TTL,
            },
        );

        // Nothing unlinked yet: the first pass withdraws a stale object
        // (a previous incarnation's) and claims nothing.
        store
            .put(&layout::hold(7), PutPayload::from(b"{}".to_vec()))
            .await
            .unwrap();
        holds.refresh_once().await.unwrap();
        assert!(store.head(&layout::hold(7)).await.is_err());

        meta.unlink(ROOT_INO, "open").unwrap();
        meta.unlink(ROOT_INO, "closed").unwrap();
        assert_eq!(meta.orphans().unwrap().len(), 2);
        let before = constellation_store_s3::lease::now_unix_ms();
        holds.refresh_once().await.unwrap();
        let after = constellation_store_s3::lease::now_unix_ms();
        let body: HoldBody = serde_json::from_slice(
            &store
                .get(&layout::hold(7))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body.node, 7);
        assert_eq!(body.inodes, vec![open.ino]);
        assert_eq!(body.chunks, vec![hash.to_hex()]);
        // The pass stamps its own clock plus the configured TTL, so the
        // expiry is bracketed by the clock either side of the pass. Read
        // that way rather than as "still in the future now": getting the
        // object back and parsing it can take longer than a 150 ms TTL on
        // a loaded host, which failed the old `> now_unix_ms()` form
        // without the stamp being any different.
        assert!(
            (before + TTL.as_millis() as i64..=after + TTL.as_millis() as i64)
                .contains(&body.expires_unix_ms),
            "{} not stamped {:?} ahead of [{before}, {after}]",
            body.expires_unix_ms,
            TTL
        );
        assert_eq!(
            meta.orphans().unwrap(),
            vec![open.ino],
            "the orphan nobody has open is reaped"
        );
        // The chunk GC's reader sees the claim while it is live — asked
        // at the last millisecond it covers, so the answer does not
        // depend on how long this test took to get here either.
        let roots = crate::gc::hold_roots(store.clone(), body.expires_unix_ms - 1)
            .await
            .unwrap();
        assert!(roots.contains(&hash));
        // ... and not once it has expired.
        let roots = crate::gc::hold_roots(store.clone(), body.expires_unix_ms)
            .await
            .unwrap();
        assert!(!roots.contains(&hash));

        // The last handle closes: reaped and withdrawn.
        view.0.lock().unwrap().clear();
        holds.refresh_once().await.unwrap();
        assert!(store.head(&layout::hold(7)).await.is_err());
        assert!(meta.orphans().unwrap().is_empty());
        assert!(meta.getattr(open.ino).unwrap().is_none());
    }
}
