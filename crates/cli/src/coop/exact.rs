//! Exact chunk-location mirrors for the cooperative cache (plan 30 §M15).
//!
//! `CONSTELLATION_COOP_DIGEST=exact` (the default) replaces the bloom
//! digests with the range-based reconciliation in
//! [`constellation_net::reconcile`]:
//!
//! * **Publishing.** Every 250 ms the publisher drains the cache's digest
//!   journal into [`LocalSet`], this node's *published* servable set,
//!   and gossips the net change as one or more [`Delta`]s (8-byte keys,
//!   adds and removes). Every digest interval it gossips a [`Summary`]
//!   heartbeat (root fingerprint, count, seq) — ~150 bytes.
//! * **Mirroring.** A receiver keeps one [`Mirror`] per peer. A delta on
//!   the mirror's exact base keeps it exact; anything else (a gap,
//!   reordering, a restart, a summary whose root differs) queues a
//!   reconciliation session with that peer, which transfers only the
//!   differing ranges. With no mirror yet the same session is the
//!   initial sync, paged at the reply budget.
//! * **Lookups.** A peer is a candidate source for a chunk only if its
//!   mirror holds the chunk's key and was confirmed within the digest
//!   TTL. Mirrors of peers that left the registry are dropped.
//! * **Liveness without gossip.** Every digest interval the driver also
//!   probes peers whose mirror has not been confirmed for two intervals
//!   (one root query, ~200 bytes each way when in sync), so a broken
//!   gossip mesh degrades freshness, not exactness.
//!
//! No reconciliation traffic is on a read or write path: sessions run on
//! their own tasks, and a lookup is a local binary search. Across
//! continents a session costs a few background round trips; the steady
//! state is the pushed delta and needs none.

use super::Coop;
use constellation_fs_core::cache::{DigestBatch, DigestChange};
use constellation_net::message::wire_len;
use constellation_net::reconcile::{
    encode_keys, key_of, respond, start_bits_for, Delta, DeltaOutcome, Key, KeySet, Mirror, Query,
    Session, Summary, MAX_DELTA_KEYS, REPLY_BUDGET,
};
use constellation_net::{ChunkDecline, Payload};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A background round trip may cross an ocean and include a cold QUIC
/// dial; the lease path's 500 ms would fail those spuriously.
const RECONCILE_TIMEOUT: Duration = Duration::from_secs(5);
/// Minimum spacing of sessions with one peer, so a peer churning
/// continuously is resynced at a bounded rate instead of back to back.
const RESYNC_MIN_GAP: Duration = Duration::from_millis(500);
/// Concurrent sessions per node (each with a different peer).
const MAX_CONCURRENT_SESSIONS: usize = 4;
/// More change than this many delta frames in one tick (e.g. a cache
/// rebuild) is published silently: seq advances, peers see the new
/// summary and reconcile, which is cheaper than a burst of gossip.
const MAX_DELTAS_PER_TICK: usize = 8;
/// Refuse to mirror a peer whose set is larger than this (64 MiB of
/// keys): the largest cache in the fleet must not dictate everyone
/// else's memory. That peer is treated as unknown, as with the bloom
/// digest's `MAX_DIGEST_BYTES`.
pub(super) const MAX_MIRROR_KEYS: usize = 8 * 1024 * 1024;
/// Bound on remembered recent removals (8-byte key + timestamp each).
const RECENT_CAP: usize = 262_144;

/// This node's published servable set plus what it recently dropped.
///
/// Maintained in both digest modes: the recent-removal memory is what
/// lets `serve_chunk` tell a propagation race ("I dropped it moments
/// ago") from a false positive ("I never had it, or dropped it long
/// ago"), and that distinction is how false-positive peer fetches are
/// counted for either mode.
pub(super) struct LocalSet {
    pub(super) keys: KeySet,
    incarnation: u64,
    seq: u64,
    recent: HashMap<Key, Instant>,
    recent_order: VecDeque<(Key, Instant)>,
    grace: Duration,
}

/// Result of absorbing one journal drain.
#[derive(Default)]
pub(super) struct Absorbed {
    pub(super) deltas: Vec<Delta>,
    /// The set changed but too much to push: publish a summary now.
    pub(super) silent: bool,
}

impl LocalSet {
    pub(super) fn new(grace: Duration) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let incarnation = nanos ^ (u64::from(std::process::id()) << 40);
        Self {
            keys: KeySet::new(),
            incarnation,
            seq: 0,
            recent: HashMap::new(),
            recent_order: VecDeque::new(),
            grace,
        }
    }

    pub(super) fn summary(&self) -> Summary {
        Summary {
            incarnation: self.incarnation,
            seq: self.seq,
            root: self.keys.root_fingerprint(),
            count: self.keys.len() as u64,
        }
    }

    /// Would a miss on `key` be a propagation race rather than a
    /// false positive? True if it was dropped within the grace window,
    /// or is still published because the drop has not been drained yet.
    pub(super) fn recently_held(&self, key: Key, now: Instant) -> bool {
        self.keys.contains(key)
            || self
                .recent
                .get(&key)
                .is_some_and(|at| now.duration_since(*at) <= self.grace)
    }

    fn note_removed(&mut self, key: Key, now: Instant) {
        self.recent.insert(key, now);
        self.recent_order.push_back((key, now));
    }

    fn prune_recent(&mut self, now: Instant) {
        while let Some(&(key, at)) = self.recent_order.front() {
            let expired = now.duration_since(at) > self.grace;
            if !expired && self.recent_order.len() <= RECENT_CAP {
                break;
            }
            self.recent_order.pop_front();
            if self.recent.get(&key) == Some(&at) {
                self.recent.remove(&key);
            }
        }
    }

    /// Fold one journal drain into the published set. With `emit`, the
    /// net change comes back as chained deltas ready to gossip.
    pub(super) fn absorb(&mut self, batch: &DigestBatch, now: Instant, emit: bool) -> Absorbed {
        self.prune_recent(now);
        // Final desired presence of every touched key; the journal is
        // ordered, so a later event for the same key wins.
        let mut touched: HashMap<Key, bool> = HashMap::new();
        if batch.rebuild {
            let fresh: HashSet<Key> = batch
                .events
                .iter()
                .filter_map(|e| match e {
                    DigestChange::Add(h) => Some(key_of(&h.0)),
                    DigestChange::Remove(_) => None,
                })
                .collect();
            for k in self.keys.iter() {
                if !fresh.contains(&k) {
                    touched.insert(k, false);
                }
            }
            for k in fresh {
                touched.insert(k, true);
            }
        } else {
            for e in &batch.events {
                match e {
                    DigestChange::Add(h) => touched.insert(key_of(&h.0), true),
                    DigestChange::Remove(h) => touched.insert(key_of(&h.0), false),
                };
            }
        }
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for (k, want) in touched {
            match (want, self.keys.contains(k)) {
                (true, false) => adds.push(k),
                (false, true) => removes.push(k),
                _ => {}
            }
        }
        if adds.is_empty() && removes.is_empty() {
            return Absorbed::default();
        }
        adds.sort_unstable();
        removes.sort_unstable();
        for &k in &removes {
            self.note_removed(k, now);
        }
        let total = adds.len() + removes.len();
        if !emit || total > MAX_DELTA_KEYS * MAX_DELTAS_PER_TICK {
            for &k in &removes {
                self.keys.remove(k);
            }
            for &k in &adds {
                self.keys.insert(k);
            }
            self.seq += 1;
            return Absorbed {
                deltas: Vec::new(),
                silent: emit,
            };
        }
        let ops: Vec<(Key, bool)> = removes
            .iter()
            .map(|&k| (k, false))
            .chain(adds.iter().map(|&k| (k, true)))
            .collect();
        let mut deltas = Vec::new();
        for chunk in ops.chunks(MAX_DELTA_KEYS) {
            let base = self.keys.root_fingerprint();
            let mut a = Vec::new();
            let mut r = Vec::new();
            for &(k, add) in chunk {
                if add {
                    self.keys.insert(k);
                    a.push(k);
                } else {
                    self.keys.remove(k);
                    r.push(k);
                }
            }
            self.seq += 1;
            deltas.push(Delta {
                incarnation: self.incarnation,
                seq: self.seq,
                base,
                root: self.keys.root_fingerprint(),
                adds: encode_keys(0, &a),
                removes: encode_keys(0, &r),
            });
        }
        Absorbed {
            deltas,
            silent: false,
        }
    }
}

/// This node's knowledge of one peer's set.
#[derive(Default)]
pub(super) struct PeerMirror {
    pub(super) mirror: Mirror,
    /// Last time the mirror was known to be current: a matching
    /// summary, an exact delta, or a completed session.
    pub(super) confirmed_at: Option<Instant>,
    /// The newest owner summary seen (gossip or a session reply).
    advertised: Option<Summary>,
    last_session: Option<Instant>,
    /// The peer's set exceeds [`MAX_MIRROR_KEYS`]; not mirrored.
    oversize: bool,
}

impl PeerMirror {
    pub(super) fn fresh(&self, ttl: Duration) -> bool {
        !self.oversize && self.confirmed_at.is_some_and(|t| t.elapsed() <= ttl)
    }

    fn advertise(&mut self, s: Summary) {
        let newer = match &self.advertised {
            None => true,
            Some(cur) => cur.incarnation != s.incarnation || s.seq >= cur.seq,
        };
        if newer {
            self.advertised = Some(s);
        }
    }

    fn known_different(&self) -> bool {
        self.advertised.is_some_and(|s| {
            s.root != self.mirror.keys.root_fingerprint()
                || s.count != self.mirror.keys.len() as u64
        })
    }
}

/// Drop guard that reports a finished (or panicked) session to the
/// driver, so a peer can never be stuck "in session" forever.
struct SessionDone {
    node_id: u64,
    tx: tokio::sync::mpsc::UnboundedSender<u64>,
}

impl Drop for SessionDone {
    fn drop(&mut self) {
        let _ = self.tx.send(self.node_id);
    }
}

fn micros(d: Duration) -> u64 {
    d.as_micros().min(u128::from(u64::MAX)) as u64
}

impl Coop {
    fn exact_active(&self) -> bool {
        self.config.enabled && self.config.mode == super::DigestMode::Exact
    }

    fn note_rx(&self, payload: &Payload) {
        self.counters
            .digest_bytes_received
            .fetch_add(wire_len(payload) as u64, Ordering::Relaxed);
        self.counters
            .digest_messages
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_tx(&self, payload: &Payload) {
        self.counters
            .digest_bytes_sent
            .fetch_add(wire_len(payload) as u64, Ordering::Relaxed);
        self.counters
            .digest_messages
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_cpu(&self, started: Instant, reconcile: bool) {
        let us = micros(started.elapsed());
        self.counters.digest_cpu_us.fetch_add(us, Ordering::Relaxed);
        if reconcile {
            self.counters
                .reconcile_cpu_us
                .fetch_add(us, Ordering::Relaxed);
        }
    }

    pub(super) fn request_sync(&self, node_id: u64) {
        let _ = self.sync_tx.send(node_id);
    }

    /// A peer's gossiped heartbeat.
    pub fn apply_summary(&self, node_id: u64, summary: Summary) {
        if !self.exact_active() || node_id == self.node_id {
            return;
        }
        self.note_rx(&Payload::CacheSummary { node_id, summary });
        let started = Instant::now();
        let need_sync = {
            let mut map = self.mirrors.lock().unwrap();
            let pm = map.entry(node_id).or_default();
            pm.advertise(summary);
            if pm.oversize && (summary.count as usize) <= MAX_MIRROR_KEYS {
                pm.oversize = false;
            }
            if pm.oversize {
                false
            } else if pm.mirror.matches(&summary) {
                pm.confirmed_at = Some(Instant::now());
                false
            } else {
                true
            }
        };
        self.note_cpu(started, false);
        if need_sync {
            self.request_sync(node_id);
        }
    }

    /// A peer's gossiped per-tick change.
    pub fn apply_set_delta(&self, node_id: u64, delta: Delta) {
        if !self.exact_active() || node_id == self.node_id {
            return;
        }
        let payload = Payload::CacheSetDelta { node_id, delta };
        self.note_rx(&payload);
        let Payload::CacheSetDelta { delta, .. } = payload else {
            unreachable!()
        };
        let started = Instant::now();
        let need_sync = {
            let mut map = self.mirrors.lock().unwrap();
            let pm = map.entry(node_id).or_default();
            if pm.oversize {
                false
            } else {
                match pm.mirror.apply_delta(&delta) {
                    DeltaOutcome::Exact => {
                        pm.confirmed_at = Some(Instant::now());
                        let count = pm.mirror.keys.len() as u64;
                        pm.advertise(Summary {
                            incarnation: delta.incarnation,
                            seq: delta.seq,
                            root: delta.root,
                            count,
                        });
                        false
                    }
                    DeltaOutcome::Diverged | DeltaOutcome::Malformed => true,
                    DeltaOutcome::Stale => false,
                }
            }
        };
        self.note_cpu(started, false);
        if need_sync {
            self.request_sync(node_id);
        }
    }

    /// Answer one reconciliation round against the published set.
    pub async fn reconcile_reply(&self, queries: Vec<Query>) -> Payload {
        let payload = Payload::ReconcileRequest { queries };
        if !self.exact_active() {
            return decline();
        }
        self.note_rx(&payload);
        let Payload::ReconcileRequest { queries } = payload else {
            unreachable!()
        };
        let started = Instant::now();
        let reply = {
            let local = self.local.lock().unwrap();
            respond(&local.keys, local.summary(), &queries, REPLY_BUDGET)
        };
        self.note_cpu(started, true);
        let payload = Payload::ReconcileReply { reply };
        self.note_tx(&payload);
        payload
    }

    /// Peers whose fresh mirror holds `key`, pruning mirrors of peers
    /// that left the registry, are really this node (a shared node key:
    /// a fetch from them would dial ourselves), or are long past
    /// confirmation.
    pub(super) fn exact_holders(&self, key: Key) -> Vec<u64> {
        let active = self.peers.is_enabled().then(|| {
            self.peers
                .remote_snapshot()
                .into_iter()
                .map(|peer| peer.node_id)
                .collect::<HashSet<_>>()
        });
        let ttl = self.config.digest_ttl;
        let forget_after = ttl.saturating_mul(10);
        let mut map = self.mirrors.lock().unwrap();
        let before = map.len();
        map.retain(|id, pm| {
            active.as_ref().is_none_or(|ids| ids.contains(id))
                && pm.confirmed_at.is_none_or(|t| t.elapsed() <= forget_after)
        });
        self.counters
            .stale_digests_pruned
            .fetch_add((before - map.len()) as u64, Ordering::Relaxed);
        let mut ids: Vec<u64> = map
            .iter()
            .filter(|(_, pm)| pm.fresh(ttl) && pm.mirror.keys.contains(key))
            .map(|(id, _)| *id)
            .collect();
        ids.sort_unstable();
        ids
    }

    /// A holder declined a chunk our mirror said it has.
    ///
    /// `Absent` is a true false positive: the mirror disagrees with the
    /// owner, so drop that key now (no repeat fetch) and resync rather
    /// than trust the rest. `RecentlyRemoved` is a propagation race the
    /// owner's next delta already covers; touching the mirror here would
    /// knock it off that delta's base and force a needless session.
    pub(super) fn forget_peer_key(&self, node_id: u64, key: Key, why: ChunkDecline) {
        if self.config.mode != super::DigestMode::Exact || why != ChunkDecline::Absent {
            return;
        }
        let removed = {
            let mut map = self.mirrors.lock().unwrap();
            map.get_mut(&node_id)
                .is_some_and(|pm| pm.mirror.keys.remove(key))
        };
        if removed {
            self.request_sync(node_id);
        }
    }

    /// Candidates for the liveness sweep: known remote peers (never this
    /// node, nor one sharing its endpoint key) whose mirror is missing or
    /// has not been confirmed for two digest intervals.
    fn sweep_candidates(&self) -> Vec<u64> {
        let stale_after = self.config.digest_interval.saturating_mul(2);
        let map = self.mirrors.lock().unwrap();
        self.peers
            .remote_snapshot()
            .into_iter()
            .map(|p| p.node_id)
            .filter(|id| match map.get(id) {
                None => true,
                Some(pm) => {
                    !pm.oversize && pm.confirmed_at.is_none_or(|t| t.elapsed() > stale_after)
                }
            })
            .collect()
    }

    fn session_gap_ok(&self, node_id: u64, now: Instant) -> bool {
        let map = self.mirrors.lock().unwrap();
        map.get(&node_id)
            .and_then(|pm| pm.last_session)
            .is_none_or(|t| now.duration_since(t) >= RESYNC_MIN_GAP)
    }

    /// Schedules reconciliation sessions: on demand (a summary or delta
    /// that did not match), on a liveness sweep, at most one per peer
    /// and [`MAX_CONCURRENT_SESSIONS`] overall.
    pub(super) async fn sync_driver(self: Arc<Self>) {
        let Some(mut rx) = self.sync_rx.lock().unwrap().take() else {
            return;
        };
        let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let mut pending: BTreeSet<u64> = BTreeSet::new();
        let mut running: HashSet<u64> = HashSet::new();
        let mut sweep = tokio::time::interval(self.config.digest_interval);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let retry = tokio::time::sleep(RESYNC_MIN_GAP);
            tokio::select! {
                got = rx.recv() => match got {
                    Some(id) => { pending.insert(id); }
                    None => return,
                },
                Some(id) = done_rx.recv() => { running.remove(&id); }
                _ = sweep.tick() => pending.extend(self.sweep_candidates()),
                _ = retry, if !pending.is_empty() => {}
            }
            while let Ok(id) = rx.try_recv() {
                pending.insert(id);
            }
            while let Ok(id) = done_rx.try_recv() {
                running.remove(&id);
            }
            // On-demand requests (summaries, deltas, declines) name a
            // node id; one that is really us can only fail to dial.
            pending.retain(|id| *id != self.node_id && !self.peers.is_self_node(*id));
            let now = Instant::now();
            let ready: Vec<u64> = pending
                .iter()
                .copied()
                .filter(|id| !running.contains(id) && self.session_gap_ok(*id, now))
                .take(MAX_CONCURRENT_SESSIONS.saturating_sub(running.len()))
                .collect();
            for id in ready {
                pending.remove(&id);
                running.insert(id);
                let this = self.clone();
                let guard = SessionDone {
                    node_id: id,
                    tx: done_tx.clone(),
                };
                tokio::spawn(async move {
                    let _guard = guard;
                    this.run_session(id).await;
                });
            }
        }
    }

    /// One reconciliation session against `node_id`'s published set.
    async fn run_session(&self, node_id: u64) {
        if node_id == self.node_id || self.peers.is_self_node(node_id) {
            return;
        }
        self.counters
            .reconcile_sessions
            .fetch_add(1, Ordering::Relaxed);
        let start_bits = {
            let mut map = self.mirrors.lock().unwrap();
            let pm = map.entry(node_id).or_default();
            pm.last_session = Some(Instant::now());
            if pm.oversize {
                return;
            }
            let advertised = pm.advertised.map(|s| s.count as usize).unwrap_or(0);
            if pm.known_different() {
                start_bits_for(pm.mirror.keys.len().max(advertised))
            } else {
                // Not known to differ: a one-query "are we in sync?"
                // probe that doubles as the liveness check.
                0
            }
        };
        let mut session = Session::new(start_bits);
        loop {
            let queries = {
                let map = self.mirrors.lock().unwrap();
                let Some(pm) = map.get(&node_id) else {
                    return;
                };
                session.next_request(&pm.mirror.keys)
            };
            let Some(queries) = queries else {
                break;
            };
            let request = Payload::ReconcileRequest { queries };
            self.note_tx(&request);
            let reply = match self
                .peers
                .request_to_node_timeout(node_id, &request, RECONCILE_TIMEOUT)
                .await
            {
                Ok(payload @ Payload::ReconcileReply { .. }) => {
                    self.note_rx(&payload);
                    match payload {
                        Payload::ReconcileReply { reply } => reply,
                        _ => unreachable!(),
                    }
                }
                Ok(_) | Err(_) => {
                    self.counters
                        .reconcile_failures
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let started = Instant::now();
            let outcome = {
                let mut map = self.mirrors.lock().unwrap();
                let Some(pm) = map.get_mut(&node_id) else {
                    return;
                };
                if reply.processed == 0 {
                    // A decline (bloom-mode or coop-disabled peer) carries
                    // a placeholder summary: do not advertise it.
                    Err(())
                } else if reply.summary.count as usize > MAX_MIRROR_KEYS {
                    pm.oversize = true;
                    pm.mirror = Mirror::default();
                    self.counters
                        .digest_capacity_exceeded
                        .store(1, Ordering::Relaxed);
                    Err(())
                } else {
                    pm.advertise(reply.summary);
                    session.apply(&mut pm.mirror.keys, &reply).map_err(|error| {
                        tracing::debug!(node_id, ?error, "reconciliation session aborted");
                    })
                }
            };
            self.note_cpu(started, true);
            self.counters
                .reconcile_rounds
                .fetch_add(1, Ordering::Relaxed);
            if outcome.is_err() {
                self.counters
                    .reconcile_failures
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let resync = {
            let mut map = self.mirrors.lock().unwrap();
            let Some(pm) = map.get_mut(&node_id) else {
                return;
            };
            // Freshly reconciled: current as of this session even if the
            // owner changed while it ran.
            pm.confirmed_at = Some(Instant::now());
            match pm.advertised {
                Some(s) => !pm.mirror.matches(&s),
                None => false,
            }
        };
        if resync || !session.is_done() {
            // The owner churned mid-session (or the round cap hit): the
            // next session only walks the ranges that changed since.
            self.request_sync(node_id);
        }
    }
}

fn decline() -> Payload {
    Payload::ReconcileReply {
        reply: constellation_net::reconcile::Reply {
            summary: Summary {
                incarnation: 0,
                seq: 0,
                root: [0; 16],
                count: 0,
            },
            processed: 0,
            answers: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_fs_core::ChunkHash;

    fn h(n: u64) -> ChunkHash {
        ChunkHash::of(&n.to_le_bytes())
    }

    fn batch(events: Vec<DigestChange>) -> DigestBatch {
        DigestBatch {
            events,
            rebuild: false,
        }
    }

    #[test]
    fn absorb_publishes_net_changes_as_chained_deltas() {
        let mut local = LocalSet::new(Duration::from_secs(10));
        let now = Instant::now();
        let empty_root = local.keys.root_fingerprint();
        let out = local.absorb(
            &batch(vec![
                DigestChange::Add(h(1)),
                DigestChange::Add(h(2)),
                // Added and removed in one tick: no net change.
                DigestChange::Add(h(3)),
                DigestChange::Remove(h(3)),
            ]),
            now,
            true,
        );
        assert_eq!(out.deltas.len(), 1);
        let d = &out.deltas[0];
        assert_eq!(d.base, empty_root);
        assert_eq!(d.root, local.keys.root_fingerprint());
        assert_eq!(local.keys.len(), 2);
        assert_eq!(local.summary().seq, 1);

        // A mirror following the chain stays exact.
        let mut mirror = Mirror::default();
        assert_eq!(mirror.apply_delta(d), DeltaOutcome::Exact);
        let out = local.absorb(&batch(vec![DigestChange::Remove(h(1))]), now, true);
        assert_eq!(mirror.apply_delta(&out.deltas[0]), DeltaOutcome::Exact);
        assert!(mirror.keys == local.keys);
        assert!(mirror.matches(&local.summary()));

        // Nothing changed: nothing to publish, seq unchanged.
        let seq = local.summary().seq;
        let out = local.absorb(&batch(vec![DigestChange::Add(h(2))]), now, true);
        assert!(out.deltas.is_empty() && !out.silent);
        assert_eq!(local.summary().seq, seq);
    }

    #[test]
    fn a_large_tick_is_split_and_a_huge_one_goes_silent() {
        let mut local = LocalSet::new(Duration::from_secs(10));
        let now = Instant::now();
        let adds = (0..(MAX_DELTA_KEYS as u64 * 2 + 5))
            .map(|n| DigestChange::Add(h(n)))
            .collect();
        let out = local.absorb(&batch(adds), now, true);
        assert_eq!(out.deltas.len(), 3);
        let mut mirror = Mirror::default();
        for d in &out.deltas {
            assert_eq!(mirror.apply_delta(d), DeltaOutcome::Exact);
        }
        assert!(mirror.keys == local.keys);

        let huge = (0..(MAX_DELTA_KEYS * MAX_DELTAS_PER_TICK) as u64 + 1)
            .map(|n| DigestChange::Add(h(1_000_000 + n)))
            .collect();
        let out = local.absorb(&batch(huge), now, true);
        assert!(out.deltas.is_empty() && out.silent);
        let expected = mirror.keys.len() + MAX_DELTA_KEYS * MAX_DELTAS_PER_TICK + 1;
        assert_eq!(local.keys.len(), expected, "a silent tick still applies");
    }

    #[test]
    fn a_rebuild_publishes_the_difference_only() {
        let mut local = LocalSet::new(Duration::from_secs(10));
        let now = Instant::now();
        local.absorb(
            &batch(vec![DigestChange::Add(h(1)), DigestChange::Add(h(2))]),
            now,
            true,
        );
        let out = local.absorb(
            &DigestBatch {
                events: vec![DigestChange::Add(h(2)), DigestChange::Add(h(3))],
                rebuild: true,
            },
            now,
            true,
        );
        assert_eq!(out.deltas.len(), 1);
        assert_eq!(local.keys.len(), 2);
        assert!(local.keys.contains(key_of(&h(3).0)));
        assert!(!local.keys.contains(key_of(&h(1).0)));
        assert!(local.recently_held(key_of(&h(1).0), now));
    }

    #[test]
    fn recent_removals_expire_after_the_grace_window() {
        let mut local = LocalSet::new(Duration::from_millis(50));
        let t0 = Instant::now();
        local.absorb(&batch(vec![DigestChange::Add(h(1))]), t0, true);
        local.absorb(&batch(vec![DigestChange::Remove(h(1))]), t0, true);
        let k = key_of(&h(1).0);
        assert!(local.recently_held(k, t0));
        let later = t0 + Duration::from_millis(100);
        assert!(!local.recently_held(k, later));
        local.absorb(&batch(vec![]), later, true);
        assert!(local.recent.is_empty() && local.recent_order.is_empty());
        // Never held at all: not recent.
        assert!(!local.recently_held(key_of(&h(9).0), t0));
    }
}
