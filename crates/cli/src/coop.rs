//! Cooperative cache: digest gossip, peer serving, source-selecting fetch
//! (DESIGN.md §7, plan 05).
//!
//! A local miss consults in-memory peer blooms (zero extra messages),
//! then the latency-adaptive selector in [`crate::sources`] ranks S3
//! against the peers that claim the chunk. S3 remains the correctness
//! anchor: a peer that declines, times out, or fails blake3 verification
//! is recorded as an error and the fetch falls back.
//!
//! Rendezvous hashing (an alternative to "whoever already has it") is
//! intentionally not wired; the selector only ranks sources that already
//! claim the chunk.

use crate::sources::{Selector, SourceId};
use anyhow::{bail, Result};
use constellation_fs_core::cache::{ChunkState, DigestBatch, DigestChange, DiskCache};
use constellation_fs_core::ChunkHash;
use constellation_net::bloom::{
    bucket_count_for, bucket_index, ENTRIES_PER_BUCKET, MAX_BITS_BYTES, MAX_BUCKETS,
};
use constellation_net::{Bloom, PathKind, Payload, Peers};
use constellation_store_s3::ChunkStore;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_GLOBAL_SERVES: u32 = 16;
const MAX_PER_PEER_SERVES: u32 = 4;
const S3_FETCH_ATTEMPTS: usize = 3;
const S3_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const MAX_PENDING_ADDS: usize = 65_536;

fn exceeds_digest_capacity(entries: usize) -> bool {
    entries > (MAX_BUCKETS as usize).saturating_mul(ENTRIES_PER_BUCKET)
}

fn parse_enabled(value: Option<&str>) -> bool {
    !value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        )
    })
}

#[derive(Clone, Copy)]
struct CoopConfig {
    enabled: bool,
    digest_interval: Duration,
    digest_ttl: Duration,
}

impl CoopConfig {
    fn from_env() -> Self {
        let enabled = parse_enabled(std::env::var("CONSTELLATION_COOP").ok().as_deref());
        let interval_s: u64 = std::env::var("CONSTELLATION_DIGEST_INTERVAL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30)
            .max(1);
        let ttl_s = std::env::var("CONSTELLATION_DIGEST_TTL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(interval_s.saturating_mul(4))
            .max(interval_s.saturating_mul(2));
        Self {
            enabled,
            digest_interval: Duration::from_secs(interval_s),
            digest_ttl: Duration::from_secs(ttl_s),
        }
    }
}

/// Drop a peer's digest rather than grow past this. Equals
/// [`MAX_BUCKETS`] × [`MAX_BITS_BYTES`]: the largest cache we will
/// remember (~13 TiB at 4 MiB chunks). A bigger sender is treated as
/// unknown — we fall back to S3 for that peer — so the largest cache
/// in the fleet cannot dictate everyone else's RSS.
const MAX_DIGEST_BYTES: usize = (MAX_BUCKETS as usize) * MAX_BITS_BYTES;

struct PeerDigest {
    generation: u64,
    buckets: u32,
    blooms: HashMap<u32, Bloom>,
    received_at: Instant,
}

impl PeerDigest {
    fn contains(&self, hash: &[u8; 32]) -> bool {
        let i = bucket_index(hash, self.buckets);
        self.blooms.get(&i).is_some_and(|b| b.contains(hash))
    }

    fn byte_len(&self) -> usize {
        self.blooms.values().map(|b| b.bits.len()).sum()
    }
}

#[derive(Default)]
struct Counters {
    peer_hits: AtomicU64,
    peer_misses: AtomicU64,
    s3_fetches: AtomicU64,
    hedges_fired: AtomicU64,
    bytes_served: AtomicU64,
    stale_digests_pruned: AtomicU64,
    digest_rebuilds: AtomicU64,
    digest_capacity_exceeded: AtomicU64,
}

pub struct Coop {
    cache: Arc<DiskCache>,
    store: Arc<ChunkStore>,
    peers: Peers,
    node_id: u64,
    chunk_size: u32,
    selector: Mutex<Selector>,
    digests: Mutex<HashMap<u64, PeerDigest>>,
    counters: Counters,
    budget: Mutex<ServeBudget>,
    config: CoopConfig,
}

/// Global and per-peer serving concurrency, under one lock so the
/// admission check and the reservation cannot interleave.
#[derive(Default)]
struct ServeBudget {
    global: u32,
    per_peer: HashMap<String, u32>,
}

enum FetchResult {
    Data {
        data: Vec<u8>,
        ttfb_ms: f64,
        total_ms: f64,
        rtt: Option<Duration>,
        path: PathKind,
    },
    Miss,
    Fail,
}

impl Coop {
    pub fn new(
        cache: Arc<DiskCache>,
        store: Arc<ChunkStore>,
        peers: Peers,
        node_id: u64,
        chunk_size: u32,
    ) -> Arc<Self> {
        Self::new_with_config(
            cache,
            store,
            peers,
            node_id,
            chunk_size,
            CoopConfig::from_env(),
        )
    }

    fn new_with_config(
        cache: Arc<DiskCache>,
        store: Arc<ChunkStore>,
        peers: Peers,
        node_id: u64,
        chunk_size: u32,
        config: CoopConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache,
            store,
            peers,
            node_id,
            chunk_size,
            selector: Mutex::new(Selector::default()),
            digests: Mutex::new(HashMap::new()),
            counters: Counters::default(),
            budget: Mutex::new(ServeBudget::default()),
            config,
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_upload_test(cache: Arc<DiskCache>, store: Arc<ChunkStore>) -> Arc<Self> {
        Self::new_with_config(
            cache,
            store,
            Peers::disabled(),
            1,
            1 << 20,
            CoopConfig {
                enabled: true,
                digest_interval: Duration::from_secs(1),
                digest_ttl: Duration::from_secs(60),
            },
        )
    }

    pub fn apply_digest(&self, d: constellation_net::DigestSnapshot) {
        if d.node_id == self.node_id {
            return;
        }
        let buckets = d.buckets.clamp(1, MAX_BUCKETS);
        if d.bucket >= buckets {
            return;
        }
        let Some(bloom) = Bloom::from_wire_bytes(d.nbits, d.k, d.n, d.bits) else {
            return;
        };
        let mut map = self.digests.lock().unwrap();
        let entry = map.entry(d.node_id).or_insert_with(|| PeerDigest {
            generation: 0,
            buckets,
            blooms: HashMap::new(),
            received_at: Instant::now(),
        });
        if entry.buckets != buckets {
            entry.buckets = buckets;
            entry.blooms.clear();
        }
        if d.generation < entry.generation {
            return;
        }
        let new_len = bloom.bits.len();
        let old_len = entry
            .blooms
            .get(&d.bucket)
            .map(|b| b.bits.len())
            .unwrap_or(0);
        if entry.byte_len() - old_len + new_len > MAX_DIGEST_BYTES {
            return;
        }
        entry.generation = entry.generation.max(d.generation);
        entry.received_at = Instant::now();
        entry.blooms.insert(d.bucket, bloom);
    }

    pub fn apply_delta(&self, d: constellation_net::DigestDelta) {
        if d.node_id == self.node_id {
            return;
        }
        let buckets = d.buckets.clamp(1, MAX_BUCKETS);
        let mut map = self.digests.lock().unwrap();
        let Some(entry) = map.get_mut(&d.node_id) else {
            return;
        };
        if entry.buckets != buckets || d.generation < entry.generation {
            return;
        }
        entry.generation = d.generation;
        entry.received_at = Instant::now();
        for h in d.adds {
            let i = bucket_index(&h, entry.buckets);
            if let Some(b) = entry.blooms.get_mut(&i) {
                b.insert(&h);
            }
        }
    }

    /// Serve one clean/pinned chunk, honoring the global and per-peer
    /// concurrency caps. Excess demand is a miss (`found: false`).
    ///
    /// The read itself goes to a blocking thread: it is a synchronous
    /// file read plus a blake3 verify of up to a whole chunk, and this
    /// runs inside the peer accept path on a runtime worker.
    pub async fn serve_chunk(self: &Arc<Self>, hash: [u8; 32], from_hex: &str) -> Option<Vec<u8>> {
        if !self.config.enabled {
            return None;
        }
        let _slot = self.try_serve_slot(from_hex)?;
        let this = self.clone();
        let data = tokio::task::spawn_blocking(move || this.cache.get_servable(&ChunkHash(hash)))
            .await
            .ok()?
            .ok()
            .flatten()?;
        let data = self
            .store
            .protect_peer_chunk(&ChunkHash(hash), &data)
            .ok()?;
        self.counters
            .bytes_served
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Some(data)
    }

    fn try_serve_slot(&self, from_hex: &str) -> Option<ServeSlot<'_>> {
        let mut b = self.budget.lock().unwrap();
        if b.global >= MAX_GLOBAL_SERVES {
            return None;
        }
        let n = b.per_peer.entry(from_hex.to_string()).or_insert(0);
        if *n >= MAX_PER_PEER_SERVES {
            return None;
        }
        *n += 1;
        b.global += 1;
        Some(ServeSlot {
            coop: self,
            from: from_hex.to_string(),
        })
    }

    fn holders(&self, hash: &ChunkHash) -> Vec<u64> {
        let key = hash.0;
        let mut map = self.digests.lock().unwrap();
        let before = map.len();
        let active = self.peers.is_enabled().then(|| {
            self.peers
                .snapshot()
                .into_iter()
                .map(|peer| peer.node_id)
                .collect::<HashSet<_>>()
        });
        map.retain(|node_id, digest| {
            digest.received_at.elapsed() <= self.config.digest_ttl
                && active.as_ref().is_none_or(|ids| ids.contains(node_id))
        });
        self.counters
            .stale_digests_pruned
            .fetch_add((before - map.len()) as u64, Ordering::Relaxed);
        let mut ids: Vec<u64> = map
            .iter()
            .filter(|(_, d)| d.contains(&key))
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Whether any live, non-stale cooperative-cache digest claims `hash`.
    ///
    /// This is deliberately membership-only: upload policy may use the
    /// answer to select a confirming HEAD, but it must not treat a peer's
    /// cache advertisement as proof that S3 contains the object.
    pub fn peer_digest_contains(&self, hash: &ChunkHash) -> bool {
        self.config.enabled && !self.holders(hash).is_empty()
    }

    fn candidates(&self, hash: &ChunkHash) -> Vec<SourceId> {
        let mut cands = Vec::new();
        if self.config.enabled {
            for id in self.holders(hash) {
                cands.push(SourceId::Peer(id));
            }
        }
        cands.push(SourceId::S3);
        cands
    }

    /// Cache-first fetch with source selection. Inserts a successful
    /// miss as `Clean`.
    pub async fn fetch(&self, hash: &ChunkHash) -> Result<Vec<u8>> {
        if let Ok(Some(d)) = self.cache.get(hash) {
            return Ok(d);
        }
        let data = self.fetch_uncached(hash).await?;
        let _ = self.cache.insert(hash, &data, ChunkState::Clean);
        Ok(data)
    }

    async fn fetch_uncached(&self, hash: &ChunkHash) -> Result<Vec<u8>> {
        let cands = self.candidates(hash);
        let size = u64::from(self.chunk_size);
        let (primary, hedge, deadline) = {
            let mut sel = self.selector.lock().unwrap();
            let primary = sel.pick(&cands, size);
            let hedge = sel.next_best(&cands, primary, size);
            let deadline = sel.hedge_deadline_ms(primary, size);
            sel.begin(primary);
            (primary, hedge, deadline)
        };

        let primary_f = self.fetch_from(primary, hash);
        tokio::pin!(primary_f);
        let result = if let Some(hsrc) = hedge {
            let sleep = tokio::time::sleep(Duration::from_millis(deadline));
            tokio::select! {
                r = &mut primary_f => self.settle(primary, r),
                _ = sleep => {
                    self.counters.hedges_fired.fetch_add(1, Ordering::Relaxed);
                    self.selector.lock().unwrap().begin(hsrc);
                    let hedge_f = self.fetch_from(hsrc, hash);
                    tokio::pin!(hedge_f);
                    tokio::select! {
                        r = &mut primary_f => {
                            match r {
                                FetchResult::Data { .. } => {
                                    self.selector.lock().unwrap().end(hsrc);
                                    self.settle(primary, r)
                                }
                                other => {
                                    self.note_fail(primary, &other);
                                    self.settle(hsrc, hedge_f.await)
                                }
                            }
                        }
                        r = &mut hedge_f => {
                            match r {
                                FetchResult::Data { .. } => {
                                    self.selector.lock().unwrap().record_cancelled(primary);
                                    self.settle(hsrc, r)
                                }
                                other => {
                                    self.selector.lock().unwrap().end(hsrc);
                                    self.note_fail(hsrc, &other);
                                    self.settle(primary, primary_f.await)
                                }
                            }
                        }
                    }
                }
            }
        } else {
            self.settle(primary, primary_f.await)
        };

        match result {
            Some(data) => Ok(data),
            None => {
                // Last resort: S3 if we have not already succeeded via it.
                if primary != SourceId::S3 && hedge != Some(SourceId::S3) {
                    self.selector.lock().unwrap().begin(SourceId::S3);
                    match self.settle(SourceId::S3, self.fetch_from(SourceId::S3, hash).await) {
                        Some(data) => Ok(data),
                        None => bail!("chunk {} unavailable from peers and S3", hash.to_hex()),
                    }
                } else {
                    bail!("chunk {} unavailable from peers and S3", hash.to_hex())
                }
            }
        }
    }

    fn settle(&self, src: SourceId, r: FetchResult) -> Option<Vec<u8>> {
        match r {
            FetchResult::Data {
                data,
                ttfb_ms,
                total_ms,
                rtt,
                path,
            } => {
                let n = data.len() as u64;
                let mut selector = self.selector.lock().unwrap();
                selector.record_transport(src, rtt, path);
                selector.record_ok(src, ttfb_ms, n, total_ms);
                drop(selector);
                match src {
                    SourceId::S3 => {
                        self.counters.s3_fetches.fetch_add(1, Ordering::Relaxed);
                    }
                    SourceId::Peer(_) => {
                        self.counters.peer_hits.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Some(data)
            }
            other => {
                self.note_fail(src, &other);
                None
            }
        }
    }

    fn note_fail(&self, src: SourceId, r: &FetchResult) {
        self.selector.lock().unwrap().record_err(src);
        if matches!(src, SourceId::Peer(_)) && !matches!(r, FetchResult::Data { .. }) {
            self.counters.peer_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Both arms report first-byte and end-to-end separately, because
    /// the selector learns TTFB and goodput as independent terms — one
    /// combined duration would leave goodput frozen at its prior.
    async fn fetch_from(&self, src: SourceId, hash: &ChunkHash) -> FetchResult {
        let t0 = Instant::now();
        let (data, ttfb, rtt, path) = match src {
            SourceId::S3 => match retry_s3(|| self.store.get_chunk_timed(hash)).await {
                Ok((data, ttfb)) => (data, ttfb, None, PathKind::Unknown),
                Err(_) => return FetchResult::Fail,
            },
            SourceId::Peer(id) => match self.peers.request_chunk(id, &hash.0).await {
                Ok(Some(fetch)) => {
                    let Ok(data) = self.store.open_peer_chunk(hash, &fetch.data) else {
                        return FetchResult::Fail;
                    };
                    (data, fetch.ttfb, fetch.rtt, fetch.path)
                }
                Ok(None) => return FetchResult::Miss,
                Err(_) => return FetchResult::Fail,
            },
        };
        let total_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if self.store.hash(&data) != *hash {
            return FetchResult::Fail;
        }
        FetchResult::Data {
            data,
            ttfb_ms: ttfb.as_secs_f64() * 1000.0,
            total_ms,
            rtt,
            path,
        }
    }

    pub fn report(&self) -> constellation_api::CoopStatus {
        let per_source = self
            .selector
            .lock()
            .unwrap()
            .all_stats()
            .into_iter()
            .map(|(id, s)| constellation_api::SourceStatus {
                id: id.label(),
                ttfb_ms_ewma: s.ttfb_ewma_ms,
                goodput_mbps_ewma: s.goodput_bps * 8.0 / 1_000_000.0,
                err_rate: s.err_rate,
                transport_rtt_ms: s.transport_rtt_ms,
                path: match s.path {
                    PathKind::Direct => "direct",
                    PathKind::Relay => "relay",
                    PathKind::Unknown => "unknown",
                }
                .into(),
            })
            .collect();
        constellation_api::CoopStatus {
            peer_hits: self.counters.peer_hits.load(Ordering::Relaxed),
            peer_misses: self.counters.peer_misses.load(Ordering::Relaxed),
            s3_fetches: self.counters.s3_fetches.load(Ordering::Relaxed),
            hedges_fired: self.counters.hedges_fired.load(Ordering::Relaxed),
            bytes_served_to_peers: self.counters.bytes_served.load(Ordering::Relaxed),
            stale_digests_pruned: self.counters.stale_digests_pruned.load(Ordering::Relaxed),
            digest_rebuilds: self.counters.digest_rebuilds.load(Ordering::Relaxed),
            digest_capacity_exceeded: self
                .counters
                .digest_capacity_exceeded
                .load(Ordering::Relaxed)
                != 0,
            per_source,
        }
    }

    /// Gossip local cache membership from the cache's digest journal —
    /// never a full `servable_hashes()` scan on the 250 ms tick.
    ///
    /// One hash-prefix bucket per interval (a full snapshot of that
    /// slice); add-only deltas for hashes in buckets already sent.
    /// Each node chooses `buckets` from its own cache size.
    pub async fn publish_loop(self: Arc<Self>) {
        if !self.config.enabled || !self.peers.is_enabled() {
            return;
        }
        let mut last_full = Instant::now()
            .checked_sub(Duration::from_secs(86_400))
            .unwrap_or_else(Instant::now);
        let mut last_n: u64 = 0;
        let mut generation: u64 = 0;
        let mut rotate: u32 = 0;
        let mut tracker = DigestTracker::default();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let batch = self.cache.take_digest_events();
            if batch.rebuild {
                self.counters
                    .digest_rebuilds
                    .fetch_add(1, Ordering::Relaxed);
            }
            tracker.apply(batch);
            let n = tracker.len() as u64;
            if exceeds_digest_capacity(tracker.len()) {
                self.counters
                    .digest_capacity_exceeded
                    .store(1, Ordering::Relaxed);
            } else {
                self.counters
                    .digest_capacity_exceeded
                    .store(0, Ordering::Relaxed);
            }
            let nb = tracker.buckets.max(1);
            let interval = self.config.digest_interval;
            let churn = {
                let base = last_n.max(1) as f64;
                (n as f64 - last_n as f64).abs() / base >= 0.10
            };
            let due = last_full.elapsed() >= interval;
            if due || churn || generation == 0 {
                let bucket = tracker.next_snapshot_bucket(rotate);
                let subset = tracker.bucket_members(bucket);
                let bloom = Bloom::from_hashes(&subset);
                let next_gen = generation.saturating_add(1);
                let payload = Payload::CacheDigest {
                    node_id: self.node_id,
                    generation: next_gen,
                    bits: bloom.bits,
                    nbits: bloom.nbits,
                    k: bloom.k,
                    n: bloom.n,
                    bucket,
                    buckets: nb,
                };
                if self.peers.gossip(payload).await.is_ok() {
                    generation = next_gen;
                    last_full = Instant::now();
                    last_n = n;
                    tracker.note_snapshotted(bucket);
                    rotate = rotate.wrapping_add(1);
                }
            } else {
                let adds = tracker.take_delta(constellation_net::message::MAX_GOSSIP_DELTA_ADDS);
                if adds.is_empty() {
                    continue;
                }
                let next_gen = generation.saturating_add(1);
                let payload = Payload::CacheDigestDelta {
                    node_id: self.node_id,
                    generation: next_gen,
                    adds: adds.clone(),
                    buckets: nb,
                };
                if self.peers.gossip(payload).await.is_ok() {
                    generation = next_gen;
                    last_n = n;
                } else {
                    tracker.restore_delta(adds);
                }
            }
        }
    }
}

/// Retry only the S3 leg. Retrying `fetch_uncached` would re-run source
/// selection and could contact the same peer and hedge several times.
async fn retry_s3<T, E, F, Fut>(mut operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    for attempt in 0..S3_FETCH_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt + 1 == S3_FETCH_ATTEMPTS => return Err(error),
            Err(_) => tokio::time::sleep(S3_RETRY_BACKOFF).await,
        }
    }
    unreachable!("S3_FETCH_ATTEMPTS is non-zero")
}

/// Local view of servable membership, fed by [`DiskCache::take_digest_events`].
struct DigestTracker {
    current: HashSet<[u8; 32]>,
    pending_adds: HashSet<[u8; 32]>,
    dirty: HashSet<u32>,
    snapshotted: HashSet<u32>,
    buckets: u32,
    pending_limit: usize,
}

impl Default for DigestTracker {
    fn default() -> Self {
        Self {
            current: HashSet::new(),
            pending_adds: HashSet::new(),
            dirty: HashSet::new(),
            snapshotted: HashSet::new(),
            buckets: 1,
            pending_limit: MAX_PENDING_ADDS,
        }
    }
}

impl DigestTracker {
    #[cfg(test)]
    fn with_pending_limit(limit: usize) -> Self {
        Self {
            pending_limit: limit,
            ..Self::default()
        }
    }

    fn len(&self) -> usize {
        self.current.len()
    }

    fn apply(&mut self, batch: DigestBatch) {
        if batch.rebuild {
            self.current = batch
                .events
                .into_iter()
                .filter_map(|event| match event {
                    DigestChange::Add(h) => Some(h.0),
                    DigestChange::Remove(_) => None,
                })
                .collect();
            self.buckets = bucket_count_for(self.current.len()).max(1);
            self.pending_adds.clear();
            self.mark_all_dirty();
            return;
        }
        for event in batch.events {
            match event {
                DigestChange::Remove(h) if self.current.remove(&h.0) => {
                    self.dirty.insert(bucket_index(&h.0, self.buckets));
                    self.pending_adds.remove(&h.0);
                }
                DigestChange::Add(h) if self.current.insert(h.0) => {
                    let bucket = bucket_index(&h.0, self.buckets);
                    if self.snapshotted.contains(&bucket) && !self.dirty.contains(&bucket) {
                        self.pending_adds.insert(h.0);
                    }
                }
                _ => {}
            }
        }
        let nb = self.stable_bucket_count();
        if nb != self.buckets {
            self.buckets = nb;
            self.pending_adds.clear();
            self.mark_all_dirty();
        } else if self.pending_adds.len() > self.pending_limit {
            // A snapshot is cheaper and bounded; current remains the
            // authoritative local set from which each bucket is rebuilt.
            self.pending_adds.clear();
            self.mark_all_dirty();
        }
    }

    fn stable_bucket_count(&self) -> u32 {
        let desired = bucket_count_for(self.current.len()).max(1);
        if desired >= self.buckets {
            return desired;
        }
        let mut candidate = self.buckets;
        while candidate > desired {
            let next = candidate / 2;
            let low_water = (next as usize)
                .saturating_mul(ENTRIES_PER_BUCKET)
                .saturating_mul(3)
                / 4;
            if self.current.len() > low_water {
                break;
            }
            candidate = next.max(1);
        }
        candidate
    }

    fn mark_all_dirty(&mut self) {
        self.dirty.clear();
        self.snapshotted.clear();
        for b in 0..self.buckets {
            self.dirty.insert(b);
        }
    }

    fn bucket_members(&self, bucket: u32) -> Vec<[u8; 32]> {
        self.current
            .iter()
            .copied()
            .filter(|h| bucket_index(h, self.buckets) == bucket)
            .collect()
    }

    fn next_snapshot_bucket(&self, rotate: u32) -> u32 {
        self.dirty
            .iter()
            .copied()
            .next()
            .unwrap_or(rotate % self.buckets.max(1))
    }

    fn note_snapshotted(&mut self, bucket: u32) {
        self.dirty.remove(&bucket);
        self.snapshotted.insert(bucket);
        self.pending_adds
            .retain(|h| bucket_index(h, self.buckets) != bucket);
    }

    /// Adds whose bucket has already been snapshotted. The rest wait:
    /// a delta into an unseen bucket is ignored by the receiver.
    fn take_delta(&mut self, limit: usize) -> Vec<[u8; 32]> {
        let out: Vec<_> = self.pending_adds.iter().copied().take(limit).collect();
        for h in &out {
            self.pending_adds.remove(h);
        }
        out
    }

    fn restore_delta(&mut self, adds: Vec<[u8; 32]>) {
        self.pending_adds.extend(adds);
    }
}

struct ServeSlot<'a> {
    coop: &'a Coop,
    from: String,
}

impl Drop for ServeSlot<'_> {
    fn drop(&mut self) {
        if let Ok(mut b) = self.coop.budget.lock() {
            b.global = b.global.saturating_sub(1);
            if let Some(n) = b.per_peer.get_mut(&self.from) {
                *n = n.saturating_sub(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_net::Peers;
    use constellation_store_s3::CompressionSetting;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn h(n: u8) -> [u8; 32] {
        ChunkHash::of(&[n]).0
    }

    fn h64(n: u64) -> [u8; 32] {
        ChunkHash::of(&n.to_le_bytes()).0
    }

    /// The `TempDir` is returned, not leaked: the cache directory must
    /// outlive the `Coop` and nothing longer.
    fn coop() -> (Arc<Coop>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let store = Arc::new(ChunkStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        (Coop::new(cache, store, Peers::disabled(), 1, 1 << 20), dir)
    }

    fn put_bucket(c: &Coop, node: u64, gen: u64, hashes: &[[u8; 32]], bucket: u32, buckets: u32) {
        let subset: Vec<_> = hashes
            .iter()
            .copied()
            .filter(|h| bucket_index(h, buckets) == bucket)
            .collect();
        let bloom = Bloom::from_hashes(&subset);
        c.apply_digest(constellation_net::DigestSnapshot {
            node_id: node,
            generation: gen,
            bits: bloom.bits,
            nbits: bloom.nbits,
            k: bloom.k,
            n: bloom.n,
            bucket,
            buckets,
        });
    }

    #[test]
    fn digest_delta_applies_on_top_of_a_snapshot() {
        let (c, _dir) = coop();
        put_bucket(&c, 2, 1, &[h(1)], 0, 1);
        assert_eq!(c.holders(&ChunkHash(h(1))), vec![2]);
        assert!(c.holders(&ChunkHash(h(2))).is_empty());
        c.apply_delta(constellation_net::DigestDelta {
            node_id: 2,
            generation: 2,
            adds: vec![h(2)],
            buckets: 1,
        });
        assert_eq!(c.holders(&ChunkHash(h(1))), vec![2]);
        assert_eq!(c.holders(&ChunkHash(h(2))), vec![2]);
    }

    /// A bucket we have not received yet must not claim membership —
    /// that would be a false positive from an empty filter.
    #[test]
    fn a_delta_does_not_invent_a_bucket() {
        let (c, _dir) = coop();
        let hashes = [h(1), h(2)];
        let nb = 4u32;
        let b1 = bucket_index(&h(1), nb);
        let b2 = bucket_index(&h(2), nb);
        put_bucket(&c, 2, 1, &hashes, b1, nb);
        c.apply_delta(constellation_net::DigestDelta {
            node_id: 2,
            generation: 2,
            adds: vec![h(2)],
            buckets: nb,
        });
        assert_eq!(c.holders(&ChunkHash(h(1))), vec![2]);
        if b1 != b2 {
            assert!(
                c.holders(&ChunkHash(h(2))).is_empty(),
                "delta filled a bucket we never snapshotted"
            );
        }
    }

    #[test]
    fn two_buckets_of_one_peer_are_consulted_independently() {
        let (c, _dir) = coop();
        let hashes = [h(1), h(2), h(3), h(4)];
        let nb = 2u32;
        put_bucket(&c, 2, 1, &hashes, 0, nb);
        put_bucket(&c, 2, 2, &hashes, 1, nb);
        for x in &hashes {
            assert_eq!(c.holders(&ChunkHash(*x)), vec![2], "missed {x:?}");
        }
    }

    /// A 200 GiB node and a 4 TiB node advertise different bucket
    /// counts. Lookups must use each sender's count, not a global one.
    #[test]
    fn peers_with_different_bucket_counts_are_both_findable() {
        let (c, _dir) = coop();
        let hashes = [h(7)];
        put_bucket(&c, 2, 1, &hashes, 0, 1);
        let nb = 8u32;
        let b = bucket_index(&h(7), nb);
        put_bucket(&c, 3, 1, &hashes, b, nb);
        assert_eq!(c.holders(&ChunkHash(h(7))), vec![2, 3]);
    }

    #[tokio::test]
    async fn s3_retry_succeeds_after_two_transient_failures() {
        let attempts = AtomicUsize::new(0);
        let got = retry_s3(|| {
            let n = attempts.fetch_add(1, AtomicOrdering::SeqCst);
            async move {
                if n < 2 {
                    Err("transient")
                } else {
                    Ok(7)
                }
            }
        })
        .await;
        assert_eq!(got, Ok(7));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
    }

    #[tokio::test]
    async fn s3_retry_stops_after_the_bounded_attempt_count() {
        let attempts = AtomicUsize::new(0);
        let got: Result<(), _> = retry_s3(|| {
            attempts.fetch_add(1, AtomicOrdering::SeqCst);
            async { Err("still down") }
        })
        .await;
        assert_eq!(got, Err("still down"));
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), S3_FETCH_ATTEMPTS);
    }

    /// A real fetch must leave the selector knowing something it did not
    /// know before. Reporting one end-to-end duration as both first byte
    /// and total silently froze goodput at its prior.
    #[tokio::test]
    async fn a_fetch_teaches_the_selector_a_measured_goodput() {
        let (c, _dir) = coop();
        let data = vec![7u8; 256 * 1024];
        let hash = ChunkHash::of(&data);
        c.store
            .put_chunk(&hash, &data, CompressionSetting::RAW)
            .await
            .unwrap();

        assert_eq!(c.fetch(&hash).await.unwrap(), data);

        let stats = c.selector.lock().unwrap().stats(SourceId::S3);
        let prior = Selector::default().stats(SourceId::S3).goodput_bps;
        assert_eq!(stats.samples, 1);
        assert!(
            stats.goodput_bps != prior,
            "goodput never left its prior of {prior} B/s"
        );
        assert!(
            stats.goodput_bps.is_finite() && stats.goodput_bps > 0.0,
            "implausible goodput {}",
            stats.goodput_bps
        );
    }

    #[tokio::test]
    async fn a_clean_chunk_is_served_and_gives_its_slot_back() {
        let (c, _dir) = coop();
        let data = vec![9u8; 4096];
        let hash = ChunkHash::of(&data);
        c.cache.insert(&hash, &data, ChunkState::Clean).unwrap();
        assert_eq!(c.serve_chunk(hash.0, "peer-a").await.unwrap(), data);
        assert_eq!(
            c.budget.lock().unwrap().global,
            0,
            "the serve leaked its reservation"
        );
    }

    /// Dirty chunks are not durable yet, so serving one would hand a
    /// peer bytes that S3 cannot confirm.
    #[tokio::test]
    async fn a_dirty_chunk_is_never_served() {
        let (c, _dir) = coop();
        let data = vec![3u8; 4096];
        let hash = ChunkHash::of(&data);
        c.cache.insert(&hash, &data, ChunkState::Dirty).unwrap();
        assert!(c.serve_chunk(hash.0, "peer-a").await.is_none());
        assert_eq!(
            c.budget.lock().unwrap().global,
            0,
            "the decline leaked its reservation"
        );
    }

    #[test]
    fn one_peer_cannot_consume_more_than_its_share() {
        let (c, _dir) = coop();
        let held: Vec<_> = (0..MAX_PER_PEER_SERVES)
            .map(|_| c.try_serve_slot("peer-a").expect("within the per-peer cap"))
            .collect();
        assert!(
            c.try_serve_slot("peer-a").is_none(),
            "peer-a exceeded its cap"
        );
        assert!(
            c.try_serve_slot("peer-b").is_some(),
            "one busy peer must not lock out another"
        );
        drop(held);
        assert!(
            c.try_serve_slot("peer-a").is_some(),
            "slots must be reusable once released"
        );
    }

    #[test]
    fn the_global_cap_holds_across_peers() {
        let (c, _dir) = coop();
        let peers = MAX_GLOBAL_SERVES / MAX_PER_PEER_SERVES;
        let held: Vec<_> = (0..peers)
            .flat_map(|p| {
                let name = format!("peer-{p}");
                (0..MAX_PER_PEER_SERVES).map(move |_| name.clone())
            })
            .map(|name| c.try_serve_slot(&name).expect("within the global cap"))
            .collect();
        assert_eq!(held.len() as u32, MAX_GLOBAL_SERVES);
        assert!(
            c.try_serve_slot("newcomer").is_none(),
            "a fresh peer got in past the global cap"
        );
    }

    #[test]
    fn the_tracker_follows_the_journal_not_a_full_scan() {
        let (c, _dir) = coop();
        let _ = c.cache.take_digest_events();
        let data = vec![1u8; 32];
        let hash = ChunkHash::of(&data);
        c.cache.insert(&hash, &data, ChunkState::Clean).unwrap();

        let mut t = DigestTracker::default();
        t.apply(c.cache.take_digest_events());
        assert!(t.current.contains(&hash.0));
        assert!(
            t.take_delta(usize::MAX).is_empty(),
            "a delta before the first snapshot would be ignored by peers"
        );

        t.note_snapshotted(bucket_index(&hash.0, t.buckets));
        let extra = vec![2u8; 32];
        let extra_h = ChunkHash::of(&extra);
        c.cache.insert(&extra_h, &extra, ChunkState::Clean).unwrap();
        t.apply(c.cache.take_digest_events());
        assert_eq!(t.take_delta(usize::MAX), vec![extra_h.0]);

        c.cache.remove(&hash).unwrap();
        t.apply(c.cache.take_digest_events());
        assert!(!t.current.contains(&hash.0));
        assert!(t.dirty.contains(&bucket_index(&hash.0, t.buckets)));
    }

    #[test]
    fn a_rebuild_replaces_membership_and_dirties_every_bucket() {
        let mut t = DigestTracker::default();
        t.apply(DigestBatch {
            events: vec![DigestChange::Add(ChunkHash(h(1)))],
            rebuild: false,
        });
        t.apply(DigestBatch {
            events: vec![
                DigestChange::Add(ChunkHash(h(9))),
                DigestChange::Add(ChunkHash(h(8))),
            ],
            rebuild: true,
        });
        assert_eq!(t.len(), 2);
        assert!(!t.current.contains(&h(1)));
        assert!(t.current.contains(&h(9)) && t.current.contains(&h(8)));
        assert!(t.take_delta(usize::MAX).is_empty());
        assert!(!t.dirty.is_empty());
    }

    #[test]
    fn tracker_replays_add_then_remove_in_order() {
        let hash = ChunkHash(h(1));
        let mut t = DigestTracker::default();
        t.apply(DigestBatch {
            events: vec![DigestChange::Add(hash), DigestChange::Remove(hash)],
            rebuild: false,
        });
        assert!(!t.current.contains(&hash.0));
    }

    #[test]
    fn tracker_replays_remove_then_add_in_order() {
        let hash = ChunkHash(h(1));
        let mut t = DigestTracker::default();
        t.current.insert(hash.0);
        t.apply(DigestBatch {
            events: vec![DigestChange::Remove(hash), DigestChange::Add(hash)],
            rebuild: false,
        });
        assert!(t.current.contains(&hash.0));
    }

    #[test]
    fn pending_delta_overflow_collapses_to_snapshots() {
        let mut t = DigestTracker::with_pending_limit(2);
        t.note_snapshotted(0);
        t.apply(DigestBatch {
            events: (1..=3)
                .map(|n| DigestChange::Add(ChunkHash(h(n))))
                .collect(),
            rebuild: false,
        });
        assert!(t.pending_adds.is_empty());
        assert_eq!(t.dirty, HashSet::from([0]));
    }

    #[test]
    fn deltas_are_batched_without_losing_the_remainder() {
        let mut t = DigestTracker::default();
        t.note_snapshotted(0);
        t.apply(DigestBatch {
            events: (1..=3)
                .map(|n| DigestChange::Add(ChunkHash(h(n))))
                .collect(),
            rebuild: false,
        });
        assert_eq!(t.take_delta(2).len(), 2);
        assert_eq!(t.take_delta(2).len(), 1);
        assert!(t.take_delta(2).is_empty());
    }

    #[test]
    fn bucket_count_has_downsize_hysteresis() {
        let mut t = DigestTracker {
            buckets: 2,
            ..DigestTracker::default()
        };
        t.current.extend((0..ENTRIES_PER_BUCKET as u64).map(h64));
        assert_eq!(
            t.stable_bucket_count(),
            2,
            "hovering at the one-bucket boundary must not repartition"
        );
        t.current = (0..(ENTRIES_PER_BUCKET * 3 / 4) as u64).map(h64).collect();
        assert_eq!(t.stable_bucket_count(), 1);
    }

    #[test]
    fn stale_peer_digests_expire_and_old_generations_are_rejected() {
        let (c, _dir) = coop();
        put_bucket(&c, 2, 5, &[h(1)], 0, 2);
        put_bucket(&c, 2, 4, &[h(2)], 1, 2);
        assert!(
            c.holders(&ChunkHash(h(2))).is_empty(),
            "an older generation filled an unseen bucket"
        );
        c.digests.lock().unwrap().get_mut(&2).unwrap().received_at =
            Instant::now() - c.config.digest_ttl - Duration::from_secs(1);
        assert!(c.holders(&ChunkHash(h(1))).is_empty());
        assert_eq!(c.counters.stale_digests_pruned.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn disabled_coop_neither_selects_nor_serves_peers() {
        let (base, dir) = coop();
        let disabled = Coop::new_with_config(
            base.cache.clone(),
            base.store.clone(),
            Peers::disabled(),
            1,
            1 << 20,
            CoopConfig {
                enabled: false,
                digest_interval: Duration::from_secs(30),
                digest_ttl: Duration::from_secs(120),
            },
        );
        put_bucket(&disabled, 2, 1, &[h(1)], 0, 1);
        assert_eq!(disabled.candidates(&ChunkHash(h(1))), vec![SourceId::S3]);
        assert!(disabled.serve_chunk(h(1), "peer").await.is_none());
        drop(dir);
    }

    #[test]
    fn coop_kill_switch_values_are_parsed_once_at_construction() {
        assert!(parse_enabled(None));
        assert!(parse_enabled(Some("yes")));
        assert!(!parse_enabled(Some("off")));
        assert!(!parse_enabled(Some(" FALSE ")));
        assert!(!parse_enabled(Some("0")));
    }

    #[test]
    fn digest_capacity_limit_is_observable_before_fpr_degrades() {
        let capacity = MAX_BUCKETS as usize * ENTRIES_PER_BUCKET;
        assert!(!exceeds_digest_capacity(capacity));
        assert!(exceeds_digest_capacity(capacity + 1));
    }
}
