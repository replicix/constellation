//! Cooperative cache: digest gossip, peer serving, source-selecting fetch
//! (DESIGN.md §7, plan 06).
//!
//! A local miss consults in-memory knowledge of peers' caches (zero
//! extra messages), then the latency-adaptive selector in
//! [`crate::sources`] ranks S3 against the peers that claim the chunk.
//! S3 remains the correctness anchor: a peer that declines, times out,
//! or fails blake3 verification is recorded and the fetch falls back.
//!
//! How peers learn each other's caches is `CONSTELLATION_COOP_DIGEST`:
//!
//! * `exact` (default, plan 30 §M15, [`exact`]): an exact mirror of each
//!   peer's set, kept current by pushed deltas and range-based set
//!   reconciliation. No false positives by construction.
//! * `bloom` (plans 06/14): gossiped bloom snapshots per hash-prefix
//!   bucket plus add-only deltas, ~1% false positives, removals stale
//!   until the bucket's next snapshot. Kept so the two can be measured
//!   side by side (`status.coop.digest_*`, `peer_false_positives`).
//!
//! All nodes of a fleet should run the same mode: an exact node ignores
//! bloom digests and vice versa, so a mixed pair just falls back to S3.
//!
//! False-positive accounting is mode-independent: a holder declines a
//! chunk it lacks with `Absent`, unless it dropped it within its
//! recent-removal window (`RecentlyRemoved`, a propagation race). Only
//! `Absent` counts as a false-positive peer fetch.
//!
//! Rendezvous hashing (an alternative to "whoever already has it") is
//! intentionally not wired; the selector only ranks sources that already
//! claim the chunk — or, for a chunk no mirror lists yet, the node that
//! wrote the manifest naming it ([`fresh`]: a new file read on another
//! node before the writer's delta arrived).

mod exact;
mod fresh;

use crate::sources::{Selector, SourceId};
use anyhow::{bail, Result};
use constellation_fs_core::cache::{ChunkState, DigestBatch, DigestChange, DiskCache, SpillFile};
use constellation_fs_core::ChunkHash;
use constellation_net::bloom::{
    bucket_count_for, bucket_index, ENTRIES_PER_BUCKET, MAX_BITS_BYTES, MAX_BUCKETS,
};
use constellation_net::message::{wire_len, SIGNED_ENVELOPE};
use constellation_net::reconcile::key_of;
use constellation_net::{Bloom, ChunkDecline, PathKind, Payload, Peers};
use constellation_store_s3::{ChunkStore, DecodePriority};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_GLOBAL_SERVES: u32 = 16;
const MAX_PER_PEER_SERVES: u32 = 4;
const S3_FETCH_ATTEMPTS: usize = 3;
const S3_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const MAX_PENDING_ADDS: usize = 65_536;
/// First and largest pause between retries of a peer that answered
/// `Busy`. A serving slot frees when one transfer ends (milliseconds on
/// a LAN); the whole wait is bounded by the S3 ETA, see `fetch_uncached`.
const BUSY_RETRY_FIRST: Duration = Duration::from_millis(2);
const BUSY_RETRY_MAX: Duration = Duration::from_millis(20);
/// Ceiling on how long a fetch waits for a peer's serving slot,
/// whatever the S3 ETA says (a peer chunk request is bounded at 5 s).
const MAX_PEER_WAIT_MS: f64 = 5_000.0;

/// How long an epoch member's chunk fetch waits for a serving slot on
/// another member (see `Coop::epoch_members`).
const EPOCH_MEMBER_FETCH_WAIT: Duration = Duration::from_secs(2);

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

/// Which peer-membership protocol this node speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DigestMode {
    Exact,
    Bloom,
}

impl DigestMode {
    /// `CONSTELLATION_COOP_DIGEST`: `exact` (default) or `bloom`.
    /// Anything else keeps the default and says so.
    fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("exact") | Some("rbsr") => Self::Exact,
            Some("bloom") => Self::Bloom,
            Some(other) => {
                tracing::warn!(
                    value = other,
                    "unknown CONSTELLATION_COOP_DIGEST; using exact"
                );
                Self::Exact
            }
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Bloom => "bloom",
        }
    }
}

#[derive(Clone, Copy)]
struct CoopConfig {
    enabled: bool,
    digest_interval: Duration,
    digest_ttl: Duration,
    mode: DigestMode,
}

impl CoopConfig {
    /// How long a holder remembers a dropped chunk, so a requester whose
    /// view has not caught up yet is charged a race, not a false
    /// positive. Two digest intervals covers a missed heartbeat plus a
    /// session; 10 s floors it for short harness intervals.
    fn recent_grace(&self) -> Duration {
        self.digest_interval
            .saturating_mul(2)
            .max(Duration::from_secs(10))
    }
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
            mode: DigestMode::parse(std::env::var("CONSTELLATION_COOP_DIGEST").ok().as_deref()),
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
    peer_errors: AtomicU64,
    s3_fetches: AtomicU64,
    /// Chunks fetched from another member of an open continuation epoch
    /// (its dirty epoch writes; see `Coop::epoch_members`).
    epoch_member_fetches: AtomicU64,
    hedges_fired: AtomicU64,
    bytes_served: AtomicU64,
    stale_digests_pruned: AtomicU64,
    digest_rebuilds: AtomicU64,
    digest_capacity_exceeded: AtomicU64,
    /// Peer answered `Absent` for a chunk our digest/mirror said it had.
    peer_false_positives: AtomicU64,
    /// Peer answered `RecentlyRemoved`: a propagation race.
    peer_stale_misses: AtomicU64,
    /// Digest-plane wire bytes and messages (both modes: blooms, bloom
    /// deltas, summaries, exact deltas, reconciliation rounds).
    digest_bytes_sent: AtomicU64,
    digest_bytes_received: AtomicU64,
    digest_messages: AtomicU64,
    /// Time spent in digest-plane code (building/applying/answering).
    digest_cpu_us: AtomicU64,
    reconcile_sessions: AtomicU64,
    reconcile_rounds: AtomicU64,
    reconcile_failures: AtomicU64,
    /// Part of `digest_cpu_us` spent answering or applying rounds.
    reconcile_cpu_us: AtomicU64,
    /// Fetches of a chunk no mirror listed, from the node that wrote the
    /// manifest naming it ([`fresh`]): served, or declined.
    fresh_hint_hits: AtomicU64,
    fresh_hint_misses: AtomicU64,
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
    /// Requester side of the holder's per-peer serving cap: at most
    /// [`MAX_PER_PEER_SERVES`] of our fetches in flight to one peer, so a
    /// burst (a demand read plus its readahead) queues here instead of
    /// being declined `Busy` by the holder. See `fetch_uncached`.
    peer_slots: Mutex<HashMap<u64, Arc<tokio::sync::Semaphore>>>,
    config: CoopConfig,
    /// This node's published servable set and recent removals (both
    /// modes; see [`exact::LocalSet`]).
    local: Mutex<exact::LocalSet>,
    /// Exact mode: one mirror per peer.
    mirrors: Mutex<HashMap<u64, exact::PeerMirror>>,
    /// Exact mode: peers that need a reconciliation session.
    sync_tx: tokio::sync::mpsc::UnboundedSender<u64>,
    sync_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<u64>>>,
    /// EC2 finding 1: dirty chunks this node has handed to a peer to
    /// upload for it (`ChunkHandoff`), served to peers while the handoff
    /// is in flight although they are not durable yet (counted: several
    /// handoffs may offer one chunk).
    offered: Mutex<HashMap<ChunkHash, u32>>,
    /// Plan 30 §M10: the members of this node's open continuation epoch
    /// (empty outside one). Their epoch writes' chunks cannot reach S3
    /// before the close, yet a member installs the others' manifests
    /// from the hold owner's stream: members serve one another their
    /// dirty chunks, and a reader tries them when no digest names a
    /// holder.
    epoch_members: Mutex<Option<Arc<Mutex<Vec<u64>>>>>,
    /// Who wrote the chunks of the manifests recently applied from
    /// other nodes ([`fresh`]).
    fresh: Mutex<fresh::FreshHints>,
}

/// [`Coop::offer`]'s claim: the chunks stay servable until it drops.
pub(crate) struct Offer {
    coop: Arc<Coop>,
    hashes: Vec<ChunkHash>,
}

impl Drop for Offer {
    fn drop(&mut self) {
        let mut offered = self.coop.offered.lock().unwrap();
        for hash in &self.hashes {
            if let Some(n) = offered.get_mut(hash) {
                *n -= 1;
                if *n == 0 {
                    offered.remove(hash);
                }
            }
        }
    }
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
    Spilled {
        hash: ChunkHash,
        spill: SpillFile,
        bytes: u64,
        ttfb_ms: f64,
        total_ms: f64,
    },
    Miss(ChunkDecline),
    Fail,
}

impl FetchResult {
    fn is_success(&self) -> bool {
        matches!(self, Self::Data { .. } | Self::Spilled { .. })
    }
}

struct Fetched {
    data: Option<Vec<u8>>,
    bytes: u64,
    source: SourceId,
    service_time: Duration,
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
        let (sync_tx, sync_rx) = tokio::sync::mpsc::unbounded_channel();
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
            peer_slots: Mutex::new(HashMap::new()),
            local: Mutex::new(exact::LocalSet::new(config.recent_grace())),
            mirrors: Mutex::new(HashMap::new()),
            sync_tx,
            sync_rx: Mutex::new(Some(sync_rx)),
            config,
            offered: Mutex::new(HashMap::new()),
            epoch_members: Mutex::new(None),
            fresh: Mutex::new(fresh::FreshHints::default()),
        })
    }

    /// The hook for the manifests the replica applies from other nodes
    /// (`Meta::set_foreign_apply_hook`): remember who wrote their chunks,
    /// so a read that finds no mirror listing one asks the writer before
    /// S3 ([`fresh`]).
    pub fn note_foreign_records(&self, records: &[constellation_meta::LogRecord]) {
        if !self.config.enabled {
            return;
        }
        let written = fresh::written_chunks(records);
        if written.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut hints = self.fresh.lock().unwrap();
        for (origin, hash) in written {
            if origin != self.node_id {
                hints.note(origin, &hash, now);
            }
        }
    }

    /// EC2 finding 1: make `hashes` servable to peers — dirty or not —
    /// while the returned claim lives (a `ChunkHandoff` in flight).
    pub(crate) fn offer(self: &Arc<Self>, hashes: &[ChunkHash]) -> Offer {
        let mut offered = self.offered.lock().unwrap();
        for hash in hashes {
            *offered.entry(*hash).or_default() += 1;
        }
        Offer {
            coop: self.clone(),
            hashes: hashes.to_vec(),
        }
    }

    /// Wire the epoch manager's member list (see `epoch_members`).
    pub fn set_epoch_members(&self, members: Arc<Mutex<Vec<u64>>>) {
        *self.epoch_members.lock().unwrap() = Some(members);
    }

    /// The other members of this node's open continuation epoch.
    fn epoch_peers(&self) -> Vec<u64> {
        self.epoch_members
            .lock()
            .unwrap()
            .as_ref()
            .map(|m| {
                m.lock()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|n| *n != self.node_id)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn is_offered(&self, hash: &ChunkHash) -> bool {
        self.offered.lock().unwrap().contains_key(hash)
    }

    /// EC2 finding 1: fetch `hash` from `peer` for a `ChunkHandoff` it
    /// sent us (it offered the chunk), verified like any peer fetch.
    pub(crate) async fn fetch_handed_off(&self, peer: u64, hash: &ChunkHash) -> Result<Vec<u8>> {
        let busy_until = Instant::now() + Duration::from_secs(5);
        match self
            .fetch_from(
                SourceId::Peer(peer),
                hash,
                DecodePriority::Demand,
                busy_until,
            )
            .await
        {
            FetchResult::Data { data, .. } => Ok(data),
            FetchResult::Miss(why) => anyhow::bail!("peer {peer} declined {hash}: {why:?}"),
            _ => anyhow::bail!("fetching {hash} from peer {peer} failed"),
        }
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
                // `main.rs`'s upload-hint test feeds a bloom snapshot.
                mode: DigestMode::Bloom,
            },
        )
    }

    pub fn apply_digest(&self, d: constellation_net::DigestSnapshot) {
        if d.node_id == self.node_id || self.config.mode != DigestMode::Bloom {
            return;
        }
        self.note_bloom_rx(d.bits.len() + 48);
        let started = Instant::now();
        self.apply_digest_inner(d);
        self.note_digest_cpu(started);
    }

    fn note_bloom_rx(&self, body: usize) {
        self.counters
            .digest_bytes_received
            .fetch_add((body + SIGNED_ENVELOPE) as u64, Ordering::Relaxed);
        self.counters
            .digest_messages
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_digest_cpu(&self, started: Instant) {
        self.counters.digest_cpu_us.fetch_add(
            started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
    }

    fn apply_digest_inner(&self, d: constellation_net::DigestSnapshot) {
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
        if d.node_id == self.node_id || self.config.mode != DigestMode::Bloom {
            return;
        }
        self.note_bloom_rx(d.adds.len() * 33 + 24);
        let started = Instant::now();
        self.apply_delta_inner(d);
        self.note_digest_cpu(started);
    }

    fn apply_delta_inner(&self, d: constellation_net::DigestDelta) {
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
    /// concurrency caps. Excess demand is `Busy`; a chunk we do not hold
    /// is `Absent` — or `RecentlyRemoved` if we dropped it inside the
    /// recent-removal window, so the requester can tell a propagation
    /// race from a false positive in what we advertised.
    ///
    /// The read itself goes to a blocking thread: it is a synchronous
    /// file read plus a blake3 verify of up to a whole chunk, and this
    /// runs inside the peer accept path on a runtime worker.
    pub async fn serve_chunk(
        self: &Arc<Self>,
        hash: [u8; 32],
        from_hex: &str,
    ) -> Result<Vec<u8>, ChunkDecline> {
        // A chunk handed off to a peer (EC2 finding 1) is served to it
        // although dirty, and whether or not the cooperative cache is on;
        // so is every chunk while a continuation epoch is open (its
        // members read one another's epoch writes; see `epoch_members`).
        let offered = self.is_offered(&ChunkHash(hash)) || !self.epoch_peers().is_empty();
        if !self.config.enabled && !offered {
            return Err(ChunkDecline::Busy);
        }
        let _slot = self.try_serve_slot(from_hex).ok_or(ChunkDecline::Busy)?;
        let this = self.clone();
        let read = tokio::task::spawn_blocking(move || {
            if offered {
                this.cache.get(&ChunkHash(hash))
            } else {
                this.cache.get_servable(&ChunkHash(hash))
            }
        })
        .await
        .map_err(|_| ChunkDecline::Busy)?
        .map_err(|_| ChunkDecline::Busy)?;
        let Some(data) = read else {
            return Err(self.absence_reason(&hash));
        };
        let data = self
            .store
            .protect_peer_chunk(&ChunkHash(hash), &data)
            .map_err(|_| ChunkDecline::Busy)?;
        self.counters
            .bytes_served
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data)
    }

    fn absence_reason(&self, hash: &[u8; 32]) -> ChunkDecline {
        let local = self.local.lock().unwrap();
        if local.recently_held(key_of(hash), Instant::now()) {
            ChunkDecline::RecentlyRemoved
        } else {
            ChunkDecline::Absent
        }
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
        match self.config.mode {
            DigestMode::Exact => self.exact_holders(key_of(&hash.0)),
            DigestMode::Bloom => self.bloom_holders(hash),
        }
    }

    fn bloom_holders(&self, hash: &ChunkHash) -> Vec<u64> {
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

    /// The sources to rank for `hash`, and the peer among them that is
    /// only there on a [`fresh`] hint (no mirror lists the chunk yet).
    fn candidates(&self, hash: &ChunkHash) -> (Vec<SourceId>, Option<u64>) {
        let mut cands = Vec::new();
        let mut hinted = None;
        if self.config.enabled {
            for id in self.holders(hash) {
                cands.push(SourceId::Peer(id));
            }
            if cands.is_empty() {
                hinted = self
                    .fresh
                    .lock()
                    .unwrap()
                    .origin(hash, Instant::now())
                    .filter(|id| self.peers.is_enabled() && self.peers.knows(*id));
                if let Some(id) = hinted {
                    cands.push(SourceId::Peer(id));
                }
            }
        }
        cands.push(SourceId::S3);
        (cands, hinted)
    }

    /// Cache-first fetch with source selection. Inserts a successful
    /// miss as `Clean`.
    pub async fn fetch(&self, hash: &ChunkHash) -> Result<Vec<u8>> {
        if let Ok(Some(d)) = self.cache.get(hash) {
            return Ok(d);
        }
        let fetched = self.fetch_uncached(hash, true).await?;
        fetched
            .data
            .ok_or_else(|| anyhow::anyhow!("successful demand fetch returned no data"))
    }

    /// Fetch for background readahead and report only an actual winning S3
    /// transfer to the adaptive S3 concurrency controller.
    pub(crate) async fn fetch_for_prefetch(
        &self,
        hash: &ChunkHash,
    ) -> Result<Option<(u64, Duration)>> {
        if self.cache.contains(hash) {
            return Ok(None);
        }
        let fetched = self.fetch_uncached(hash, false).await?;
        let bytes = fetched.bytes;
        let telemetry = (fetched.source == SourceId::S3).then_some((bytes, fetched.service_time));
        Ok(telemetry)
    }

    async fn fetch_uncached(&self, hash: &ChunkHash, read_back: bool) -> Result<Fetched> {
        // `read_back` already tells us whether the application is blocked
        // on this fetch (`fetch`) or it is speculative readahead nobody is
        // waiting on yet (`fetch_for_prefetch`) — reuse that as the E2E
        // decode-gate priority rather than threading a second flag with
        // the same meaning through every helper below.
        let priority = if read_back {
            DecodePriority::Demand
        } else {
            DecodePriority::Background
        };
        // Plan 30 §M10: in an open continuation epoch a chunk no digest
        // names may be another member's epoch write — dirty there (digests
        // list clean chunks only) and not in S3 before the close. Ask each
        // member in turn before the usual sources. The bytes are verified
        // against the hash like any peer's, so a member that lacks the
        // chunk, is gone or answers garbage costs a miss, never wrong
        // data; with none holding it the read fails (S3 has it neither).
        if self.config.enabled && self.holders(hash).is_empty() {
            let members = self.epoch_peers();
            if !members.is_empty() {
                let until = Instant::now() + EPOCH_MEMBER_FETCH_WAIT;
                for id in members {
                    let src = SourceId::Peer(id);
                    self.selector.lock().unwrap().begin(src);
                    let r = self.fetch_from(src, hash, priority, until).await;
                    if !r.is_success() {
                        self.selector.lock().unwrap().end(src);
                    }
                    if let Some(fetched) = self.settle(hash, src, r, read_back, None) {
                        self.counters
                            .epoch_member_fetches
                            .fetch_add(1, Ordering::Relaxed);
                        return Ok(fetched);
                    }
                }
            }
        }
        let (cands, hinted) = self.candidates(hash);
        let size = u64::from(self.chunk_size);
        let (primary, hedge, deadline, s3_eta_ms) = {
            let mut sel = self.selector.lock().unwrap();
            self.sync_probe_rtts(&mut sel);
            let primary = sel.pick(&cands, size);
            let hedge = sel.next_best(&cands, primary, size);
            let deadline = sel.hedge_deadline_ms(primary, size);
            let s3_eta_ms = sel.eta_ms(SourceId::S3, size);
            sel.begin(primary);
            (primary, hedge, deadline, s3_eta_ms)
        };
        // How long a peer may keep us waiting for a serving slot before S3
        // would have been faster: the selector's own S3 ETA, the number it
        // just ranked this peer against. Queueing that long on a holder
        // that is streaming our other chunks still beats an S3 GET.
        let wait_ms = if s3_eta_ms.is_finite() {
            s3_eta_ms.clamp(1.0, MAX_PEER_WAIT_MS)
        } else {
            MAX_PEER_WAIT_MS
        };
        let peer_wait_until = Instant::now() + Duration::from_secs_f64(wait_ms / 1000.0);
        let _peer_slot = match primary {
            SourceId::Peer(id) => match self.peer_slot(id, peer_wait_until).await {
                Some(permit) => Some(permit),
                None => {
                    // Our own fetches keep this holder's serving budget
                    // full for longer than S3 would take: S3 now wins.
                    // Not the peer's fault, so no miss is recorded.
                    self.selector.lock().unwrap().end(primary);
                    return self.last_resort_s3(hash, priority, read_back).await;
                }
            },
            SourceId::S3 => None,
        };

        // Whether S3 was actually contacted (not merely armed as a hedge
        // candidate): a primary that fails before the hedge deadline
        // elapses ends the race in the first `select!` arm below without
        // ever spawning the hedge, so `hedge == Some(SourceId::S3)` alone
        // does not mean S3 was tried. Gating the last-resort fallback on
        // this instead of on `hedge` fixes a real gap where a fast peer
        // decline (a normal event under exact-mode churn) with an S3
        // hedge candidate skipped S3 entirely and the fetch failed even
        // though S3 was never asked.
        let mut s3_tried = primary == SourceId::S3;

        let primary_f = self.fetch_from(primary, hash, priority, peer_wait_until);
        tokio::pin!(primary_f);
        let result = if let Some(hsrc) = hedge {
            let sleep = tokio::time::sleep(Duration::from_millis(deadline));
            tokio::select! {
                r = &mut primary_f => self.settle(hash, primary, r, read_back, hinted),
                _ = sleep => {
                    self.counters.hedges_fired.fetch_add(1, Ordering::Relaxed);
                    self.selector.lock().unwrap().begin(hsrc);
                    s3_tried = s3_tried || hsrc == SourceId::S3;
                    let hedge_f = self.fetch_from(hsrc, hash, priority, peer_wait_until);
                    tokio::pin!(hedge_f);
                    tokio::select! {
                        r = &mut primary_f => {
                            match r {
                                result if result.is_success() => {
                                    self.selector.lock().unwrap().end(hsrc);
                                    self.settle(hash, primary, result, read_back, hinted)
                                }
                                other => {
                                    self.note_fail(hash, primary, &other, hinted);
                                    self.settle(hash, hsrc, hedge_f.await, read_back, hinted)
                                }
                            }
                        }
                        r = &mut hedge_f => {
                            match r {
                                result if result.is_success() => {
                                    self.selector.lock().unwrap().record_cancelled(primary);
                                    self.settle(hash, hsrc, result, read_back, hinted)
                                }
                                other => {
                                    self.selector.lock().unwrap().end(hsrc);
                                    self.note_fail(hash, hsrc, &other, hinted);
                                    self.settle(hash, primary, primary_f.await, read_back, hinted)
                                }
                            }
                        }
                    }
                }
            }
        } else {
            self.settle(hash, primary, primary_f.await, read_back, hinted)
        };

        match result {
            Some(fetched) => Ok(fetched),
            // Last resort: S3, unless it was already actually tried.
            None if !s3_tried => self.last_resort_s3(hash, priority, read_back).await,
            None => bail!("chunk {} unavailable from peers and S3", hash.to_hex()),
        }
    }

    /// S3 as the final source: a read never fails without asking it.
    async fn last_resort_s3(
        &self,
        hash: &ChunkHash,
        priority: DecodePriority,
        read_back: bool,
    ) -> Result<Fetched> {
        self.selector.lock().unwrap().begin(SourceId::S3);
        let r = self.fetch_s3_spilled(hash, priority).await;
        match self.settle(hash, SourceId::S3, r, read_back, None) {
            Some(fetched) => Ok(fetched),
            None => bail!("chunk {} unavailable from peers and S3", hash.to_hex()),
        }
    }

    fn peer_limiter(&self, node_id: u64) -> Arc<tokio::sync::Semaphore> {
        self.peer_slots
            .lock()
            .unwrap()
            .entry(node_id)
            .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(MAX_PER_PEER_SERVES as usize)))
            .clone()
    }

    /// One of our [`MAX_PER_PEER_SERVES`] request slots to `node_id`, or
    /// `None` if none frees up before `until`.
    async fn peer_slot(
        &self,
        node_id: u64,
        until: Instant,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        let limiter = self.peer_limiter(node_id);
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(until),
            limiter.acquire_owned(),
        )
        .await
        .ok()?
        .ok()
    }

    /// `hinted`: the peer that was a candidate only on a [`fresh`] hint.
    fn settle(
        &self,
        requested: &ChunkHash,
        src: SourceId,
        r: FetchResult,
        read_back: bool,
        hinted: Option<u64>,
    ) -> Option<Fetched> {
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
                if let SourceId::Peer(id) = src {
                    self.counters.peer_hits.fetch_add(1, Ordering::Relaxed);
                    if hinted == Some(id) {
                        self.counters
                            .fresh_hint_hits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                let bytes = data.len() as u64;
                let _ = self.cache.insert(requested, &data, ChunkState::Clean);
                let demand_data = read_back.then_some(data);
                Some(Fetched {
                    data: demand_data,
                    bytes,
                    source: src,
                    service_time: Duration::from_secs_f64(total_ms / 1000.0),
                })
            }
            FetchResult::Spilled {
                hash,
                spill,
                bytes,
                ttfb_ms,
                total_ms,
            } => {
                debug_assert_eq!(&hash, requested);
                self.cache
                    .commit_spill(requested, spill, ChunkState::Clean)
                    .ok()?;
                let data = if read_back {
                    self.cache.get(requested).ok().flatten()
                } else {
                    None
                };
                if read_back && data.is_none() {
                    return None;
                }
                let mut selector = self.selector.lock().unwrap();
                selector.record_transport(src, None, PathKind::Unknown);
                selector.record_ok(src, ttfb_ms, bytes, total_ms);
                drop(selector);
                self.counters.s3_fetches.fetch_add(1, Ordering::Relaxed);
                Some(Fetched {
                    data,
                    bytes,
                    source: src,
                    service_time: Duration::from_secs_f64(total_ms / 1000.0),
                })
            }
            other => {
                self.note_fail(requested, src, &other, hinted);
                None
            }
        }
    }

    fn note_fail(&self, hash: &ChunkHash, src: SourceId, r: &FetchResult, hinted: Option<u64>) {
        let mut sel = self.selector.lock().unwrap();
        if let (SourceId::Peer(id), FetchResult::Miss(_)) = (src, r) {
            if hinted == Some(id) {
                // A hint is a guess (the writer may have evicted the
                // chunk, or a rid-less transaction was misattributed):
                // no false positive, no penalty for the peer.
                sel.end(src);
                self.counters
                    .fresh_hint_misses
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        match r {
            FetchResult::Miss(why) => {
                sel.record_miss(src);
                drop(sel);
                if let SourceId::Peer(id) = src {
                    self.counters.peer_misses.fetch_add(1, Ordering::Relaxed);
                    match why {
                        ChunkDecline::Absent => {
                            self.counters
                                .peer_false_positives
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                peer = id,
                                chunk = %hash.to_hex(),
                                mode = self.config.mode.as_str(),
                                "false-positive peer fetch"
                            );
                        }
                        ChunkDecline::RecentlyRemoved => {
                            self.counters
                                .peer_stale_misses
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        ChunkDecline::Busy => {}
                    }
                    self.forget_peer_key(id, key_of(&hash.0), *why);
                }
            }
            FetchResult::Fail => {
                sel.record_err(src);
                if matches!(src, SourceId::Peer(_)) {
                    self.counters.peer_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            FetchResult::Data { .. } | FetchResult::Spilled { .. } => {}
        }
    }

    /// Both arms report first-byte and end-to-end separately, because
    /// the selector learns TTFB and goodput as independent terms — one
    /// combined duration would leave goodput frozen at its prior.
    ///
    /// A peer that answers `Busy` (its serving budget is full, e.g. other
    /// requesters share its global cap) is retried with a short backoff
    /// until `busy_until` — the S3 ETA the peer was ranked against. Busy
    /// says nothing about membership, so it is never a false positive,
    /// and waiting for a slot on a fast holder beats an S3 GET.
    async fn fetch_from(
        &self,
        src: SourceId,
        hash: &ChunkHash,
        priority: DecodePriority,
        busy_until: Instant,
    ) -> FetchResult {
        let SourceId::Peer(id) = src else {
            return self.fetch_s3_spilled(hash, priority).await;
        };
        let mut backoff = BUSY_RETRY_FIRST;
        let (t0, fetch) = loop {
            // Per attempt, so goodput is not charged for time spent queued.
            let t0 = Instant::now();
            match self.peers.request_chunk(id, &hash.0).await {
                Ok(Ok(fetch)) => break (t0, fetch),
                Ok(Err(ChunkDecline::Busy)) if Instant::now() + backoff < busy_until => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(BUSY_RETRY_MAX);
                }
                Ok(Err(why)) => return FetchResult::Miss(why),
                Err(_) => return FetchResult::Fail,
            }
        };
        let Ok(data) = self.store.open_peer_chunk(hash, &fetch.data) else {
            return FetchResult::Fail;
        };
        let (ttfb, rtt, path) = (fetch.ttfb, fetch.rtt, fetch.path);
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

    async fn fetch_s3_spilled(&self, hash: &ChunkHash, priority: DecodePriority) -> FetchResult {
        for attempt in 0..S3_FETCH_ATTEMPTS {
            let Ok(mut spill) = self.cache.begin_spill() else {
                return FetchResult::Fail;
            };
            let fetched = if self.store.is_e2e() {
                let Ok(mut cipher) = self.cache.begin_spill() else {
                    return FetchResult::Fail;
                };
                self.store
                    .get_chunk_to_writer_e2e(hash, &mut cipher, &mut spill, priority)
                    .await
            } else {
                self.store.get_chunk_to_writer(hash, &mut spill).await
            };
            match fetched {
                Ok((bytes, ttfb, total)) => {
                    return FetchResult::Spilled {
                        hash: *hash,
                        spill,
                        bytes,
                        ttfb_ms: ttfb.as_secs_f64() * 1000.0,
                        total_ms: total.as_secs_f64() * 1000.0,
                    };
                }
                Err(_) if attempt + 1 < S3_FETCH_ATTEMPTS => {
                    tokio::time::sleep(S3_RETRY_BACKOFF).await;
                }
                Err(_) => return FetchResult::Fail,
            }
        }
        FetchResult::Fail
    }

    /// Push measured probe RTTs into the selector so cold peers are not
    /// scored with LAN priors while the Peers panel already knows ~180 ms.
    fn sync_probe_rtts(&self, sel: &mut Selector) {
        for peer in self.peers.snapshot() {
            let Some(ms) = peer.rtt_ms else {
                continue;
            };
            sel.record_transport(
                SourceId::Peer(peer.node_id),
                Some(Duration::from_millis(ms)),
                peer.path,
            );
        }
    }

    pub fn report(&self) -> constellation_control::proto::types::CoopStatus {
        let mut sel = self.selector.lock().unwrap();
        self.sync_probe_rtts(&mut sel);
        let per_source = sel
            .all_stats()
            .into_iter()
            .map(
                |(id, s)| constellation_control::proto::types::SourceStatus {
                    id: id.label(),
                    // Lat/BW stay None until a real transfer — priors must not
                    // look like measurements next to probe RTT.
                    ttfb_ms_ewma: (s.ok_samples > 0).then_some(s.ttfb_ewma_ms),
                    goodput_mbps_ewma: (s.goodput_samples > 0)
                        .then_some(s.goodput_bps * 8.0 / 1_000_000.0),
                    aggregate_mbps_ewma: (s.ok_samples > 0)
                        .then_some(s.aggregate_bps_live() * 8.0 / 1_000_000.0),
                    hit_rate: s.hit_rate,
                    miss_rate: s.miss_rate,
                    err_rate: s.err_rate,
                    ok_samples: s.ok_samples,
                    transport_rtt_ms: s.transport_rtt_ms,
                    path: match s.path {
                        PathKind::Direct => "direct",
                        PathKind::Relay => "relay",
                        PathKind::Unknown => "unknown",
                    }
                    .into(),
                },
            )
            .collect();
        drop(sel);
        let (peer_set_entries, peer_set_bytes) = self.peer_set_footprint();
        constellation_control::proto::types::CoopStatus {
            peer_hits: self.counters.peer_hits.load(Ordering::Relaxed),
            peer_misses: self.counters.peer_misses.load(Ordering::Relaxed),
            peer_errors: self.counters.peer_errors.load(Ordering::Relaxed),
            s3_fetches: self.counters.s3_fetches.load(Ordering::Relaxed),
            epoch_member_fetches: self.counters.epoch_member_fetches.load(Ordering::Relaxed),
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
            digest_mode: self.config.mode.as_str().into(),
            peer_false_positives: self.counters.peer_false_positives.load(Ordering::Relaxed),
            peer_stale_misses: self.counters.peer_stale_misses.load(Ordering::Relaxed),
            digest_bytes_sent: self.counters.digest_bytes_sent.load(Ordering::Relaxed),
            digest_bytes_received: self.counters.digest_bytes_received.load(Ordering::Relaxed),
            digest_messages: self.counters.digest_messages.load(Ordering::Relaxed),
            digest_cpu_us: self.counters.digest_cpu_us.load(Ordering::Relaxed),
            reconcile_sessions: self.counters.reconcile_sessions.load(Ordering::Relaxed),
            reconcile_rounds: self.counters.reconcile_rounds.load(Ordering::Relaxed),
            reconcile_failures: self.counters.reconcile_failures.load(Ordering::Relaxed),
            reconcile_cpu_us: self.counters.reconcile_cpu_us.load(Ordering::Relaxed),
            fresh_hint_hits: self.counters.fresh_hint_hits.load(Ordering::Relaxed),
            fresh_hint_misses: self.counters.fresh_hint_misses.load(Ordering::Relaxed),
            local_set_entries: self.local.lock().unwrap().keys.len() as u64,
            peer_set_entries,
            peer_set_bytes,
        }
    }

    /// Size of what this node holds about peers' caches: mirror keys
    /// (exact) or inserted bloom entries (bloom), and resident bytes.
    fn peer_set_footprint(&self) -> (u64, u64) {
        match self.config.mode {
            DigestMode::Exact => {
                let map = self.mirrors.lock().unwrap();
                map.values().fold((0, 0), |(n, b), pm| {
                    (
                        n + pm.mirror.keys.len() as u64,
                        b + pm.mirror.keys.approx_bytes() as u64,
                    )
                })
            }
            DigestMode::Bloom => {
                let map = self.digests.lock().unwrap();
                map.values().fold((0, 0), |(n, b), d| {
                    (
                        n + d.blooms.values().map(|bl| bl.n).sum::<u64>(),
                        b + d.byte_len() as u64,
                    )
                })
            }
        }
    }

    /// Gossip local cache membership from the cache's digest journal —
    /// never a full `servable_hashes()` scan on the 250 ms tick — in the
    /// configured [`DigestMode`]. Exact mode also runs the
    /// reconciliation driver that keeps this node's peer mirrors current.
    pub async fn publish_loop(self: Arc<Self>) {
        if !self.config.enabled || !self.peers.is_enabled() {
            return;
        }
        match self.config.mode {
            DigestMode::Exact => {
                tokio::spawn(self.clone().sync_driver());
                self.exact_publish_loop().await
            }
            DigestMode::Bloom => self.bloom_publish_loop().await,
        }
    }

    /// Drain the journal into the published set; returns the batch for
    /// the bloom tracker (the exact path consumes it here).
    fn drain_digest_events(&self, emit: bool) -> (DigestBatch, exact::Absorbed) {
        let batch = self.cache.take_digest_events();
        if batch.rebuild {
            self.counters
                .digest_rebuilds
                .fetch_add(1, Ordering::Relaxed);
        }
        let absorbed = self
            .local
            .lock()
            .unwrap()
            .absorb(&batch, Instant::now(), emit);
        (batch, absorbed)
    }

    async fn gossip_digest(&self, payload: Payload) -> bool {
        let bytes = wire_len(&payload) as u64;
        let ok = self.peers.gossip(payload).await.is_ok();
        if ok {
            self.counters
                .digest_bytes_sent
                .fetch_add(bytes, Ordering::Relaxed);
            self.counters
                .digest_messages
                .fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    /// Exact mode: per-tick deltas, and a summary heartbeat every digest
    /// interval (or at once after a change too large to push).
    async fn exact_publish_loop(self: Arc<Self>) {
        let mut last_summary: Option<Instant> = None;
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let started = Instant::now();
            let (_, absorbed) = self.drain_digest_events(true);
            self.note_digest_cpu(started);
            for delta in absorbed.deltas {
                // A lost delta is repaired by the next summary's session.
                let _ = self
                    .gossip_digest(Payload::CacheSetDelta {
                        node_id: self.node_id,
                        delta,
                    })
                    .await;
            }
            let due = last_summary.is_none_or(|t| t.elapsed() >= self.config.digest_interval);
            if due || absorbed.silent {
                let summary = self.local.lock().unwrap().summary();
                if self
                    .gossip_digest(Payload::CacheSummary {
                        node_id: self.node_id,
                        summary,
                    })
                    .await
                {
                    last_summary = Some(Instant::now());
                }
            }
        }
    }

    /// Bloom mode (plans 06/14). One hash-prefix bucket per interval (a
    /// full snapshot of that slice); add-only deltas for hashes in
    /// buckets already sent. Each node chooses `buckets` from its own
    /// cache size.
    async fn bloom_publish_loop(self: Arc<Self>) {
        let mut last_full = Instant::now()
            .checked_sub(Duration::from_secs(86_400))
            .unwrap_or_else(Instant::now);
        let mut last_n: u64 = 0;
        let mut generation: u64 = 0;
        let mut rotate: u32 = 0;
        let mut tracker = DigestTracker::default();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            let started = Instant::now();
            let (batch, _) = self.drain_digest_events(false);
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
                self.note_digest_cpu(started);
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
                if self.gossip_digest(payload).await {
                    generation = next_gen;
                    last_full = Instant::now();
                    last_n = n;
                    tracker.note_snapshotted(bucket);
                    rotate = rotate.wrapping_add(1);
                }
            } else {
                let adds = tracker.take_delta(constellation_net::message::MAX_GOSSIP_DELTA_ADDS);
                self.note_digest_cpu(started);
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
                if self.gossip_digest(payload).await {
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
#[cfg(test)]
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

    fn test_config(mode: DigestMode) -> CoopConfig {
        CoopConfig {
            enabled: true,
            digest_interval: Duration::from_secs(30),
            digest_ttl: Duration::from_secs(120),
            mode,
        }
    }

    fn coop_with(mode: DigestMode) -> (Arc<Coop>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskCache::open(dir.path(), 1 << 20).unwrap());
        let store = Arc::new(ChunkStore::new(Arc::new(
            object_store::memory::InMemory::new(),
        )));
        (
            Coop::new_with_config(
                cache,
                store,
                Peers::disabled(),
                1,
                1 << 20,
                test_config(mode),
            ),
            dir,
        )
    }

    /// Bloom-mode coop (the digest tests below feed bloom snapshots).
    /// The `TempDir` is returned, not leaked: the cache directory must
    /// outlive the `Coop` and nothing longer.
    fn coop() -> (Arc<Coop>, tempfile::TempDir) {
        coop_with(DigestMode::Bloom)
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
        assert_eq!(
            c.serve_chunk(hash.0, "peer-a").await,
            Err(ChunkDecline::Absent)
        );
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
                ..test_config(DigestMode::Bloom)
            },
        );
        put_bucket(&disabled, 2, 1, &[h(1)], 0, 1);
        assert_eq!(disabled.candidates(&ChunkHash(h(1))).0, vec![SourceId::S3]);
        assert_eq!(
            disabled.serve_chunk(h(1), "peer").await,
            Err(ChunkDecline::Busy)
        );
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

    /// An object store whose reads arrive only after `delay`, like the
    /// harness's toxiproxied 200 ms S3. Writes and listings pass through.
    #[derive(Debug)]
    struct DelayedGets {
        inner: Arc<dyn object_store::ObjectStore>,
        delay: Duration,
    }

    impl std::fmt::Display for DelayedGets {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "DelayedGets")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for DelayedGets {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            tokio::time::sleep(self.delay).await;
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// The holder's side of the P2P service: chunk serving only.
    struct ServeOnly(Arc<Coop>);

    impl constellation_net::PeerService for ServeOnly {
        fn segment_published(&self, _part: &str, _seq: u64, _epoch: u64) {}
        fn lease_requested(
            &self,
            part: String,
            _requester: u64,
            _epoch_applied: Option<u64>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Payload> + Send + '_>> {
            Box::pin(async move {
                Payload::LeaseHandoff {
                    part,
                    epoch: 0,
                    released: false,
                    etag: None,
                    head_seq: None,
                }
            })
        }
        fn serve_chunk(
            &self,
            hash: [u8; 32],
            from_hex: String,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Vec<u8>, ChunkDecline>> + Send + '_>,
        > {
            Box::pin(async move { self.0.serve_chunk(hash, &from_hex).await })
        }
        fn node_id(&self) -> u64 {
            self.0.node_id
        }
    }

    /// Plan 30 §M15 round 3: `coop-cache-hit`'s burst. A cold reader
    /// asks one holder for 8 chunks at once (a demand read plus its
    /// readahead) while S3 is slow. The holder serves at most
    /// `MAX_PER_PEER_SERVES` (4) at a time per requester. Every chunk
    /// must come from the peer and none from S3: the excess queues on the
    /// requester's slots instead of being declined `Busy` and spilled to
    /// S3 (which the pre-fix tree did for 4 of the 8).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_burst_past_the_serve_cap_is_served_by_the_peer_not_s3() {
        const CHUNK: usize = 64 * 1024;
        const N: usize = 8;
        assert!(
            N as u32 > MAX_PER_PEER_SERVES,
            "the burst must exceed the cap"
        );
        let dir = tempfile::tempdir().unwrap();
        let topic = constellation_net::topic_for(Some(&[4u8; 32]), "coop-burst");
        let (key_a, _) = constellation_net::load_or_create(&dir.path().join("a.key")).unwrap();
        let (key_b, _) = constellation_net::load_or_create(&dir.path().join("b.key")).unwrap();
        let pa = constellation_net::P2p::spawn(key_a, topic).await.unwrap();
        let pb = constellation_net::P2p::spawn(key_b, topic).await.unwrap();
        let registry = vec![
            (
                1u64,
                pa.pubkey_hex(),
                serde_json::to_value(pa.addr()).unwrap(),
            ),
            (
                2u64,
                pb.pubkey_hex(),
                serde_json::to_value(pb.addr()).unwrap(),
            ),
        ];
        let peers_a = Peers::new(pa, 1);
        let peers_b = Peers::new(pb, 2);
        peers_a.refresh_registry(registry.clone());
        peers_b.refresh_registry(registry);

        // S3 holds every chunk, 2 s away: a fallback would succeed, just
        // slowly — and would show up in `s3_fetches`. The scenario's S3 is
        // ~200 ms away; 2 s keeps the test about the queueing, not about
        // this machine's speed. The queued chunks may wait for a slot as
        // long as the S3 ETA, and an S3 hedge (armed after the peer's
        // ~13 ms prior deadline) only wins if the peer is slower than S3.
        // At 200 ms a loaded runner could lose either race. A `Busy`
        // decline (the pre-fix behaviour) spills to S3 whatever the ETA,
        // so the regression still shows.
        const S3_MS: f64 = 2_000.0;
        let s3 = Arc::new(DelayedGets {
            inner: Arc::new(object_store::memory::InMemory::new()),
            delay: Duration::from_millis(S3_MS as u64),
        });
        let store = Arc::new(ChunkStore::new(s3));
        let chunks: Vec<Vec<u8>> = (0..N).map(|i| vec![i as u8 + 1; CHUNK]).collect();
        let hashes: Vec<ChunkHash> = chunks.iter().map(|d| ChunkHash::of(d)).collect();

        let cache_a = Arc::new(DiskCache::open(dir.path().join("a"), 4 << 20).unwrap());
        let a = Coop::new_with_config(
            cache_a.clone(),
            store.clone(),
            peers_a.clone(),
            1,
            CHUNK as u32,
            test_config(DigestMode::Exact),
        );
        let _ = cache_a.take_digest_events();
        for (h, data) in hashes.iter().zip(&chunks) {
            store
                .put_chunk(h, data, CompressionSetting::RAW)
                .await
                .unwrap();
            cache_a.insert(h, data, ChunkState::Clean).unwrap();
        }
        // The two endpoints settle (gossip neighbours, paths) before the
        // burst, as they did while these uploads each read the condemned
        // pointer first (8 x 200 ms); a unique upload reads nothing now.
        tokio::time::sleep(Duration::from_millis(1600)).await;
        let (_, published) = a.drain_digest_events(true);
        let service = Arc::new(ServeOnly(a.clone()));
        tokio::spawn(async move { peers_a.serve(service).await });

        let cache_b = Arc::new(DiskCache::open(dir.path().join("b"), 4 << 20).unwrap());
        let b = Coop::new_with_config(
            cache_b,
            store.clone(),
            peers_b,
            2,
            CHUNK as u32,
            test_config(DigestMode::Exact),
        );
        for d in published.deltas {
            b.apply_set_delta(1, d);
        }
        // B has measured S3's first byte, as the scenario's reader has by
        // the time it reads.
        {
            let mut sel = b.selector.lock().unwrap();
            for _ in 0..8 {
                sel.record_ok(SourceId::S3, S3_MS, CHUNK as u64, S3_MS + 5.0);
            }
        }
        assert_eq!(b.holders(&hashes[0]), vec![1], "B's mirror names A");

        let fetched = futures::future::join_all(hashes.iter().map(|h| b.fetch(h))).await;
        for (got, want) in fetched.into_iter().zip(&chunks) {
            assert_eq!(&got.expect("burst fetch failed"), want);
        }
        let status = b.report();
        assert_eq!(
            (status.peer_hits, status.s3_fetches),
            (N as u64, 0),
            "every chunk of the burst must come from the peer: {status:?}"
        );
        assert_eq!(status.peer_false_positives, 0);
        assert_eq!(status.peer_stale_misses, 0);
    }

    #[test]
    fn digest_mode_parsing_defaults_to_exact() {
        assert_eq!(DigestMode::parse(None), DigestMode::Exact);
        assert_eq!(DigestMode::parse(Some("exact")), DigestMode::Exact);
        assert_eq!(DigestMode::parse(Some(" BLOOM ")), DigestMode::Bloom);
        assert_eq!(DigestMode::parse(Some("nonsense")), DigestMode::Exact);
    }

    /// A chunk evicted moments ago is a race, not a false positive; one
    /// we never held is `Absent` and is what the counter charges.
    #[tokio::test]
    async fn decline_reasons_separate_races_from_false_positives() {
        let (c, _dir) = coop_with(DigestMode::Exact);
        let _ = c.cache.take_digest_events();
        let data = vec![5u8; 4096];
        let hash = ChunkHash::of(&data);
        c.cache.insert(&hash, &data, ChunkState::Clean).unwrap();
        c.drain_digest_events(true);
        assert!(c.serve_chunk(hash.0, "peer-a").await.is_ok());

        // Evicted but not yet drained: still published, so a race.
        c.cache.remove(&hash).unwrap();
        assert_eq!(
            c.serve_chunk(hash.0, "peer-a").await,
            Err(ChunkDecline::RecentlyRemoved)
        );
        // Drained: remembered inside the grace window, still a race.
        c.drain_digest_events(true);
        assert_eq!(
            c.serve_chunk(hash.0, "peer-a").await,
            Err(ChunkDecline::RecentlyRemoved)
        );
        // Never held: a false positive on the requester's side.
        assert_eq!(
            c.serve_chunk(h(77), "peer-a").await,
            Err(ChunkDecline::Absent)
        );
    }

    /// Exact mode: a summary with a matching root confirms the mirror,
    /// a chained delta keeps it exact, and lookups answer from it.
    #[test]
    fn exact_mirror_follows_summaries_and_deltas() {
        let (owner, _d1) = coop_with(DigestMode::Exact);
        let (reader, _d2) = coop_with(DigestMode::Exact);
        let _ = owner.cache.take_digest_events();
        let a = vec![1u8; 1024];
        let b = vec![2u8; 1024];
        let (ha, hb) = (ChunkHash::of(&a), ChunkHash::of(&b));
        owner.cache.insert(&ha, &a, ChunkState::Clean).unwrap();
        let (_, first) = owner.drain_digest_events(true);
        assert_eq!(first.deltas.len(), 1);

        // Peers::disabled() has no registry, so the reader keeps every
        // mirror (no active-set pruning) — exactly what this needs.
        reader.apply_summary(2, owner.local.lock().unwrap().summary());
        assert!(
            reader.holders(&ha).is_empty(),
            "an unsynced mirror must not claim anything"
        );
        for d in first.deltas {
            reader.apply_set_delta(2, d);
        }
        assert_eq!(reader.holders(&ha), vec![2]);
        assert!(reader.peer_digest_contains(&ha));

        owner.cache.insert(&hb, &b, ChunkState::Clean).unwrap();
        owner.cache.remove(&ha).unwrap();
        let (_, second) = owner.drain_digest_events(true);
        for d in second.deltas {
            reader.apply_set_delta(2, d);
        }
        assert!(reader.holders(&ha).is_empty(), "removal did not propagate");
        assert_eq!(reader.holders(&hb), vec![2]);
        let summary = owner.local.lock().unwrap().summary();
        reader.apply_summary(2, summary);
        let map = reader.mirrors.lock().unwrap();
        assert!(map.get(&2).unwrap().mirror.keys == owner.local.lock().unwrap().keys);
        assert!(map.get(&2).unwrap().fresh(Duration::from_secs(60)));
    }

    /// The responder answers rounds from the published set; driving a
    /// session with it makes a lagging mirror exact.
    #[tokio::test]
    async fn exact_reconcile_reply_repairs_a_lagging_mirror() {
        use constellation_net::reconcile::{KeySet, Session};
        let (owner, _d) = coop_with(DigestMode::Exact);
        let _ = owner.cache.take_digest_events();
        for n in 0..300u64 {
            let data = n.to_le_bytes().repeat(16);
            owner
                .cache
                .insert(&ChunkHash::of(&data), &data, ChunkState::Clean)
                .unwrap();
        }
        owner.drain_digest_events(true);
        let mut mirror = KeySet::new();
        let mut session = Session::new(0);
        while let Some(queries) = session.next_request(&mirror) {
            let Payload::ReconcileReply { reply } = owner.reconcile_reply(queries).await else {
                panic!("wrong reply kind");
            };
            session.apply(&mut mirror, &reply).unwrap();
        }
        assert!(mirror == owner.local.lock().unwrap().keys);
        let status = owner.report();
        assert!(status.reconcile_cpu_us <= status.digest_cpu_us);
        assert!(status.digest_bytes_sent > 0 && status.digest_bytes_received > 0);
        assert_eq!(status.local_set_entries, 300);
    }

    /// A bloom-mode node declines rounds; an exact node ignores blooms.
    #[tokio::test]
    async fn modes_do_not_cross_talk() {
        let (bloom, _d1) = coop_with(DigestMode::Bloom);
        let Payload::ReconcileReply { reply } = bloom.reconcile_reply(Vec::new()).await else {
            panic!("wrong reply kind");
        };
        assert_eq!(reply.processed, 0);
        let (exact, _d2) = coop_with(DigestMode::Exact);
        put_bucket(&exact, 2, 1, &[h(1)], 0, 1);
        assert!(exact.holders(&ChunkHash(h(1))).is_empty());
    }

    #[test]
    fn digest_capacity_limit_is_observable_before_fpr_degrades() {
        let capacity = MAX_BUCKETS as usize * ENTRIES_PER_BUCKET;
        assert!(!exceeds_digest_capacity(capacity));
        assert!(exceeds_digest_capacity(capacity + 1));
    }
}
