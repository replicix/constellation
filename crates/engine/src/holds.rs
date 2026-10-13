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
//!
//! # Orphans the platform still references
//!
//! A view also reports an orphan it unlinked while its platform may
//! still reference it without a handle (an `O_PATH` descriptor that can
//! reopen it, `View::reap_after_unlink`), until the platform lets go.
//! The reaper leaves those like open ones. The hold claims one only once
//! the reference is older than [`Holds::claim_after`]: the unlinking
//! lookup's own reference, which the platform lets go of as the unlink
//! returns, is the common case and must not cost a hold write per
//! `unlink`. A longer one is claimed within that grace: the view asks
//! the loop to wake for it ([`Holds::wake_in`]), whatever the refresh
//! period — well inside the lease TTL a GC round waits between marking
//! and deleting, so it is covered as an open orphan is.

use anyhow::{Context, Result};
use constellation_fs_core::manifest::{decode_chunk_list, ChunkInfo, Manifest};
use constellation_fs_core::{ChunkHash, Ino};
use constellation_meta::{Meta, MetaStore};
use constellation_store_s3::{layout, ChunkStore};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// What a mounted view reports: the inodes with at least one open
/// handle, and the orphans its platform still references.
pub trait OpenHandles: Send + Sync {
    fn open_inos(&self) -> Vec<Ino>;

    /// Of [`Self::open_inos`], those only the platform references (no
    /// handle), and since when (`None`: since before this process).
    fn referenced(&self) -> Vec<(Ino, Option<Instant>)> {
        Vec::new()
    }
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
        self.live()
            .iter()
            .any(|view| view.open_inos().contains(&ino))
    }

    /// Whether any live view has a handle open on `ino` (not only its
    /// platform's reference, [`OpenHandles::referenced`]).
    pub fn has_handle(&self, ino: Ino) -> bool {
        self.live().iter().any(|view| {
            view.open_inos().contains(&ino)
                && !view
                    .referenced()
                    .iter()
                    .any(|(referenced, _)| *referenced == ino)
        })
    }

    /// The union of every live view's open inodes.
    pub fn open_inos(&self) -> HashSet<Ino> {
        self.live().iter().flat_map(|v| v.open_inos()).collect()
    }

    /// The inodes no view has open and every view that reports them has
    /// referenced for less than `grace` — not to be claimed yet — and how
    /// long until the first of them is due.
    pub fn unclaimed(&self, grace: Duration) -> (HashSet<Ino>, Option<Duration>) {
        let mut young = HashMap::new();
        let mut claimed = HashSet::new();
        for view in self.live() {
            let referenced = view.referenced();
            let by_reference: HashSet<Ino> = referenced.iter().map(|(ino, _)| *ino).collect();
            claimed.extend(
                view.open_inos()
                    .into_iter()
                    .filter(|ino| !by_reference.contains(ino)),
            );
            for (ino, since) in referenced {
                match since.map(|at| grace.saturating_sub(at.elapsed())) {
                    Some(left) if !left.is_zero() => {
                        let due = young.entry(ino).or_insert(left);
                        *due = left.min(*due);
                    }
                    _ => {
                        claimed.insert(ino);
                    }
                }
            }
        }
        young.retain(|ino, _| !claimed.contains(ino));
        let due = young.values().min().copied();
        (young.into_keys().collect(), due)
    }

    fn live(&self) -> Vec<Arc<dyn OpenHandles>> {
        self.views
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(_, w)| w.upgrade())
            .collect()
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
    /// When the loop must run a pass before its regular refresh: a
    /// referenced orphan comes due for its claim ([`Self::wake_in`], and
    /// a pass that left one unclaimed).
    wake_at: Mutex<Option<Instant>>,
    /// Pokes the loop to re-read `wake_at` without running a pass.
    rewake: tokio::sync::Notify,
    /// Passes run, failed ones included ([`Self::passes`]).
    passes: std::sync::atomic::AtomicU64,
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
            wake_at: Mutex::new(None),
            rewake: tokio::sync::Notify::new(),
            passes: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Run a pass no later than `after` from now — not now: the claim of
    /// an orphan a view's platform references without a handle comes
    /// due after [`Self::claim_after`] (a view, right after the unlink),
    /// and must not wait for the regular refresh. Costs a timer, not a
    /// pass.
    pub fn wake_in(&self, after: Duration) {
        let at = Instant::now() + after;
        {
            let mut wake_at = self.wake_at.lock().unwrap();
            // Already due by then (an `rm -rf`: one wake per unlink): the
            // loop has nothing to re-arm.
            if wake_at.is_some_and(|current| current <= at) {
                return;
            }
            *wake_at = Some(at);
        }
        self.rewake.notify_one();
    }

    /// How many passes ran since this writer started (each lists the
    /// replica's orphans): what a caller that should not have caused one
    /// can check.
    pub fn passes(&self) -> u64 {
        self.passes.load(Ordering::Relaxed)
    }

    /// How long a view's platform must have referenced an orphan without
    /// a handle before the hold claims it (the module doc): a quarter of
    /// the refresh period, at most a second — a GC round waits a whole
    /// lease TTL, twice the default refresh, before it deletes.
    pub fn claim_after(&self) -> Duration {
        (self.config.refresh / 4).min(Duration::from_secs(1))
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
            let refresh_at = Instant::now() + self.config.refresh;
            loop {
                let at = match *self.wake_at.lock().unwrap() {
                    Some(wake_at) => refresh_at.min(wake_at),
                    None => refresh_at,
                };
                tokio::select! {
                    _ = tokio::time::sleep_until(at.into()) => break,
                    _ = self.nudge.notified() => break,
                    _ = self.rewake.notified() => {}
                }
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

    /// The held orphans with data, and every chunk hash their manifests
    /// name (the spill chunk of a spilled list included, as
    /// `gc::live_roots` counts them). An orphan with no manifest — a
    /// removed directory a working directory keeps, a file never written —
    /// has nothing for GC to keep: it is left out, so it never costs a
    /// hold write.
    async fn with_data(&self, inodes: &[Ino]) -> Result<(Vec<Ino>, BTreeSet<String>)> {
        let mut chunks = BTreeSet::new();
        let mut held = Vec::with_capacity(inodes.len());
        for &ino in inodes {
            let Some(bytes) = self.meta.manifest(ino)? else {
                continue;
            };
            held.push(ino);
            for hash in manifest_chunk_hashes(&self.chunks, &bytes).await? {
                chunks.insert(hash.to_hex());
            }
        }
        Ok((held, chunks))
    }

    /// One pass: compute the held set, then write, re-stamp or delete
    /// the hold object as the set and the cadence require.
    pub async fn refresh_once(&self) -> Result<()> {
        self.passes.fetch_add(1, Ordering::Relaxed);
        // This pass is the wake that was due: dropped before anything here
        // can fail, so a failing pass (an unreadable replica) is retried at
        // the refresh, not at once and forever. A wake still ahead stays.
        {
            let now = Instant::now();
            let mut wake_at = self.wake_at.lock().unwrap();
            *wake_at = wake_at.filter(|at| *at > now);
        }
        let mut inodes = self.held_orphans()?;
        let (unclaimed, due) = self.sources.unclaimed(self.claim_after());
        inodes.retain(|ino| !unclaimed.contains(ino));
        if let Some(due) = due {
            // This pass's own: when the first reference it left comes due.
            let at = Instant::now() + due;
            let mut wake_at = self.wake_at.lock().unwrap();
            *wake_at = Some(wake_at.map_or(at, |current| current.min(at)));
        }
        let (inodes, chunks) = self.with_data(&inodes).await?;
        let chunks: Vec<String> = chunks.into_iter().collect();
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

    /// A view whose platform references orphans without a handle.
    struct Referenced(Mutex<Vec<(Ino, Option<Instant>)>>);

    impl OpenHandles for Referenced {
        fn open_inos(&self) -> Vec<Ino> {
            self.referenced().into_iter().map(|(ino, _)| ino).collect()
        }

        fn referenced(&self) -> Vec<(Ino, Option<Instant>)> {
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

    /// An orphan a view's platform references without a handle (an
    /// `O_PATH` descriptor) is never reaped while referenced, and claimed
    /// only once the reference is older than `claim_after` — the unlink's
    /// own lookup, let go of at once, costs no hold write — with the pass
    /// waking for it. One a handle has open is claimed at once, and one
    /// referenced since before this process too.
    #[tokio::test]
    async fn a_referenced_orphan_is_kept_and_claimed_after_a_grace() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let chunks = Arc::new(ChunkStore::new(store.clone()));
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(7).unwrap();
        let hash = ChunkHash::of(b"referenced");
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        meta.set_manifest(f.ino, &inline_manifest(hash), 4).unwrap();
        meta.unlink(ROOT_INO, "f").unwrap();
        let view = Arc::new(Referenced(Mutex::new(vec![(f.ino, Some(Instant::now()))])));
        let sources = Arc::new(HoldSources::default());
        sources.register(1, Arc::downgrade(&view) as Weak<dyn OpenHandles>);
        let holds = Holds::new(
            store.clone(),
            chunks,
            meta.clone(),
            7,
            sources.clone(),
            HoldConfig {
                refresh: Duration::from_secs(60),
                ttl: Duration::from_secs(180),
            },
        );
        assert_eq!(holds.claim_after(), Duration::from_secs(1));

        holds.refresh_once().await.unwrap();
        assert_eq!(meta.orphans().unwrap(), vec![f.ino], "kept, not reaped");
        assert!(sources.is_open(f.ino) && !sources.has_handle(f.ino));
        assert!(
            store.head(&layout::hold(7)).await.is_err(),
            "not claimed yet"
        );
        let wake_at = holds
            .wake_at
            .lock()
            .unwrap()
            .expect("the pass wakes for it");
        assert!(wake_at <= Instant::now() + Duration::from_secs(1));

        // Another view holding it open claims it at once.
        let other = Arc::new(Opens(Mutex::new(vec![f.ino])));
        sources.register(2, Arc::downgrade(&other) as Weak<dyn OpenHandles>);
        assert!(sources.has_handle(f.ino));
        holds.refresh_once().await.unwrap();
        assert!(store.head(&layout::hold(7)).await.is_ok());
        drop(other);
        holds.refresh_once().await.unwrap();
        assert!(store.head(&layout::hold(7)).await.is_err());

        // Past the grace (or referenced since before this process).
        for since in [Instant::now().checked_sub(Duration::from_secs(2)), None] {
            view.0.lock().unwrap()[0].1 = since;
            holds.refresh_once().await.unwrap();
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
            assert_eq!(
                (body.inodes, body.chunks),
                (vec![f.ino], vec![hash.to_hex()])
            );
            assert!(holds
                .wake_at
                .lock()
                .unwrap()
                .is_none_or(|at| at <= Instant::now() + Duration::from_secs(1)));
        }

        // The platform lets go: reaped and withdrawn.
        view.0.lock().unwrap().clear();
        holds.refresh_once().await.unwrap();
        assert!(meta.orphans().unwrap().is_empty());
        assert!(store.head(&layout::hold(7)).await.is_err());
    }

    /// A pass that fails (here: the bucket refuses the hold) is not
    /// rerun at once and forever: the wake that brought it is dropped
    /// before anything in it can fail, and the next try is the refresh
    /// (or a later wake). The pass reads the replica before it writes,
    /// and a replica that cannot be read (no fault hook in `Meta`) took
    /// the same path before the wake was dropped first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_pass_is_not_rerun_back_to_back() {
        let store = crate::upload::pending_upload_tests::FailingStore::new();
        store.set_fail_puts(true);
        let dyn_store: Arc<dyn ObjectStore> = store.clone();
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(7).unwrap();
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        meta.set_manifest(f.ino, &inline_manifest(ChunkHash::of(b"f")), 4)
            .unwrap();
        meta.unlink(ROOT_INO, "f").unwrap();
        // Referenced since before this process: claimed (written) at once.
        let view = Arc::new(Referenced(Mutex::new(vec![(f.ino, None)])));
        let sources = Arc::new(HoldSources::default());
        sources.register(1, Arc::downgrade(&view) as Weak<dyn OpenHandles>);
        let holds = Holds::new(
            dyn_store.clone(),
            Arc::new(ChunkStore::new(dyn_store)),
            meta,
            7,
            sources,
            HoldConfig {
                refresh: Duration::from_secs(60),
                ttl: Duration::from_secs(180),
            },
        );
        // A wake that is due, as one a view asked for and that came due.
        *holds.wake_at.lock().unwrap() = Instant::now().checked_sub(Duration::from_millis(1));
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(holds.clone().run(stop.clone()));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.puts(), 1, "one failed pass, then the refresh");
        assert_eq!(*holds.wake_at.lock().unwrap(), None);
        // A view's wake still runs a pass when it is due, once.
        holds.wake_in(Duration::from_millis(100));
        holds.wake_in(Duration::from_millis(200));
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(store.puts(), 2);
        stop.store(true, Ordering::Relaxed);
        holds.nudge();
        task.await.unwrap();
    }

    /// A referenced orphan with no data — a removed directory that is a
    /// working directory, a file never written — has nothing for GC to
    /// keep: it is neither reaped nor claimed, and alone it costs no hold
    /// write; one with data beside it is claimed alone.
    #[tokio::test]
    async fn an_orphan_without_data_is_kept_but_never_claimed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        meta.set_node_prefix(7).unwrap();
        let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        meta.rmdir(ROOT_INO, "d").unwrap();
        let empty = meta.create(ROOT_INO, "empty", 0o644, 0, 0).unwrap();
        meta.unlink(ROOT_INO, "empty").unwrap();
        let view = Arc::new(Referenced(Mutex::new(vec![
            (d.ino, None),
            (empty.ino, None),
        ])));
        let sources = Arc::new(HoldSources::default());
        sources.register(1, Arc::downgrade(&view) as Weak<dyn OpenHandles>);
        let holds = Holds::new(
            store.clone(),
            Arc::new(ChunkStore::new(store.clone())),
            meta.clone(),
            7,
            sources,
            HoldConfig {
                refresh: Duration::from_secs(60),
                ttl: Duration::from_secs(180),
            },
        );
        holds.refresh_once().await.unwrap();
        let mut orphans = meta.orphans().unwrap();
        orphans.sort();
        assert_eq!(orphans, {
            let mut v = vec![d.ino, empty.ino];
            v.sort();
            v
        });
        assert!(store.head(&layout::hold(7)).await.is_err(), "no hold write");

        // A file with data beside them is claimed alone.
        let hash = ChunkHash::of(b"data");
        let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        meta.set_manifest(f.ino, &inline_manifest(hash), 4).unwrap();
        meta.unlink(ROOT_INO, "f").unwrap();
        view.0.lock().unwrap().push((f.ino, None));
        holds.refresh_once().await.unwrap();
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
        assert_eq!(
            (body.inodes, body.chunks),
            (vec![f.ino], vec![hash.to_hex()])
        );
    }
}
