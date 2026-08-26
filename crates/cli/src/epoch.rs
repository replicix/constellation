//! Continuation-epoch coordinator for the mount daemon (DESIGN.md §5.3).
//!
//! Wraps [`constellation_net::EpochMachine`] with SQLite persistence and
//! the P2P propose/ack/activate exchange. FUSE threads only read the
//! three atomics (`active`, `frozen`, `blocks_takeover`).

use anyhow::Result;
use constellation_meta::SqliteMeta;
use constellation_net::{component_covers_roster, EpochMachine, EpochPromise, Payload};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub struct EpochManager {
    node_id: u64,
    meta: Arc<SqliteMeta>,
    peers: constellation_net::Peers,
    machine: Mutex<EpochMachine>,
    roster: Mutex<Vec<u64>>,
    s3_failure_since: Mutex<Option<std::time::Instant>>,
    pub active: Arc<AtomicBool>,
    pub frozen: Arc<AtomicBool>,
    pub blocks_takeover: Arc<AtomicBool>,
    flushing: AtomicBool,
}

impl EpochManager {
    pub fn new(node_id: u64, meta: Arc<SqliteMeta>, peers: constellation_net::Peers) -> Self {
        let loaded = meta
            .load_open_epoch()
            .ok()
            .flatten()
            .map(|(id, members, base, at, state)| {
                let mut p = EpochPromise::new(id, members, base, at);
                p.state = match state.as_str() {
                    "active" => constellation_net::EpochState::Active,
                    "frozen" => constellation_net::EpochState::Frozen,
                    "closed" => constellation_net::EpochState::Closed,
                    _ => constellation_net::EpochState::Promised,
                };
                p
            });
        let machine = loaded.map(EpochMachine::from_promise).unwrap_or_default();
        let mgr = Self {
            node_id,
            meta,
            peers,
            machine: Mutex::new(machine),
            roster: Mutex::new(Vec::new()),
            s3_failure_since: Mutex::new(None),
            active: Arc::new(AtomicBool::new(false)),
            frozen: Arc::new(AtomicBool::new(false)),
            blocks_takeover: Arc::new(AtomicBool::new(false)),
            flushing: AtomicBool::new(false),
        };
        mgr.sync_flags();
        mgr
    }

    fn sync_flags(&self) {
        let m = self.machine.lock().unwrap();
        self.active.store(m.is_active(), Ordering::Relaxed);
        self.frozen.store(m.is_frozen(), Ordering::Relaxed);
        self.blocks_takeover
            .store(m.blocks_s3_takeover(), Ordering::Relaxed);
        if let Some(p) = m.current() {
            let state = match p.state {
                constellation_net::EpochState::Promised => "promised",
                constellation_net::EpochState::Active => "active",
                constellation_net::EpochState::Frozen => "frozen",
                constellation_net::EpochState::Closed => "closed",
            };
            let _ = self
                .meta
                .persist_epoch(&p.epoch_id, &p.members, &p.base, p.promised_at, state);
        }
    }

    pub fn status(&self) -> constellation_api::EpochStatus {
        let m = self.machine.lock().unwrap();
        let (active, epoch_id, members) = m.status();
        constellation_api::EpochStatus {
            active,
            epoch_id,
            members,
        }
    }

    pub fn writes_ok(&self) -> bool {
        self.active.load(Ordering::Relaxed) && !self.frozen.load(Ordering::Relaxed)
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::Relaxed)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn is_open(&self) -> bool {
        self.blocks_takeover.load(Ordering::Relaxed)
    }

    pub fn is_flushing(&self) -> bool {
        self.flushing.load(Ordering::Relaxed)
    }

    pub fn finish_flushing(&self) {
        self.flushing.store(false, Ordering::Relaxed);
    }

    pub fn note_s3_success(&self) {
        *self.s3_failure_since.lock().unwrap() = None;
    }

    pub fn set_roster(&self, ids: Vec<u64>) {
        *self.roster.lock().unwrap() = ids;
    }

    pub fn roster(&self) -> Vec<u64> {
        self.roster.lock().unwrap().clone()
    }

    pub fn shared_log_advanced(&self, applied: &BTreeMap<String, u64>) -> bool {
        self.machine
            .lock()
            .unwrap()
            .current()
            .is_some_and(|promise| {
                promise
                    .base
                    .iter()
                    .any(|(part, base)| applied.get(part).copied().unwrap_or(0) > *base)
            })
    }

    /// Persist a promise (BEFORE any ack is sent) then reply.
    pub fn handle_propose(
        &self,
        epoch_id: String,
        members: Vec<u64>,
        base: Vec<(String, u64)>,
        _proposer: u64,
    ) -> Payload {
        let base: BTreeMap<String, u64> = base.into_iter().collect();
        let p = EpochPromise::new(epoch_id.clone(), members, base, now_ms());
        let accepted = {
            let mut m = self.machine.lock().unwrap();
            m.persist_promise(p).is_ok()
        };
        self.sync_flags();
        Payload::EpochAck {
            epoch_id,
            member: self.node_id,
            accepted,
        }
    }

    pub fn handle_activate(&self, epoch_id: String, _members: Vec<u64>, _base: Vec<(String, u64)>) {
        let _ = self.machine.lock().unwrap().activate(&epoch_id);
        self.sync_flags();
        tracing::info!(epoch_id, "continuation epoch activated");
    }

    pub async fn check_liveness(&self) {
        if !self.is_open() {
            return;
        }
        let members = self
            .machine
            .lock()
            .unwrap()
            .current()
            .map(|p| p.members.clone())
            .unwrap_or_default();
        let mut live = vec![self.node_id];
        for id in members.iter().copied().filter(|id| *id != self.node_id) {
            if self.peers.ping_node(id).await {
                live.push(id);
            }
        }
        self.machine.lock().unwrap().note_live_members(&live);
        self.sync_flags();
        if self.is_frozen() {
            tracing::error!(
                missing = ?members.iter().filter(|m| !live.contains(m)).collect::<Vec<_>>(),
                "continuation epoch frozen: lost contact with a member (EROFS)"
            );
        }
    }

    /// If S3 is down and the live component covers the roster, propose.
    pub async fn maybe_propose(&self, base: BTreeMap<String, u64>) -> Result<bool> {
        if self.is_open() {
            return Ok(self.is_active());
        }
        {
            let now = std::time::Instant::now();
            let mut since = self.s3_failure_since.lock().unwrap();
            match *since {
                None => {
                    *since = Some(now);
                    return Ok(false);
                }
                Some(first)
                    if now.duration_since(first) < std::time::Duration::from_millis(500) =>
                {
                    return Ok(false);
                }
                Some(_) => {}
            }
        }
        let roster = self.roster();
        if roster.is_empty() {
            return Ok(false);
        }
        // A single-node roster covers itself even with P2P disabled.
        // Multi-node needs successful current pings; cached status is
        // deliberately insufficient for this safety decision.
        let mut live = vec![self.node_id];
        if roster.len() > 1 {
            for id in roster.iter().copied().filter(|id| *id != self.node_id) {
                if self.peers.ping_node(id).await {
                    live.push(id);
                }
            }
        }
        if !component_covers_roster(self.node_id, &live, &roster) {
            return Ok(false);
        }
        let epoch_id = format!("{}-{}", self.node_id, now_ms());
        let p = EpochPromise::new(epoch_id.clone(), roster.clone(), base.clone(), now_ms());
        {
            let mut m = self.machine.lock().unwrap();
            m.persist_promise(p).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        self.sync_flags();
        let base_vec: Vec<(String, u64)> = base.into_iter().collect();
        let mut acked = vec![self.node_id];
        for id in roster.iter().copied().filter(|id| *id != self.node_id) {
            let payload = Payload::EpochPropose {
                epoch_id: epoch_id.clone(),
                members: roster.clone(),
                base: base_vec.clone(),
                proposer: self.node_id,
            };
            match self.peers.request_to_node(id, &payload).await {
                Ok(Payload::EpochAck {
                    accepted: true,
                    member,
                    ..
                }) => acked.push(member),
                other => {
                    tracing::warn!(peer = id, ?other, "epoch propose not acked");
                    return Ok(false);
                }
            }
        }
        if !roster.iter().all(|m| acked.contains(m)) {
            return Ok(false);
        }
        {
            let mut m = self.machine.lock().unwrap();
            m.activate(&epoch_id).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        self.sync_flags();
        self.peers
            .announce_epoch_activate(&epoch_id, &roster, &base_vec)
            .await;
        tracing::info!(epoch_id, members = ?roster, "continuation epoch active");
        Ok(true)
    }

    pub fn close(&self) {
        // Read before the `if let`: a lock guard created in the
        // scrutinee lives through the whole body and would deadlock the
        // second lock below.
        let id = {
            self.machine
                .lock()
                .unwrap()
                .current()
                .map(|p| p.epoch_id.clone())
        };
        if let Some(id) = id {
            self.machine.lock().unwrap().close();
            let _ = self.meta.set_epoch_state(&id, "closed");
        }
        self.flushing.store(true, Ordering::Relaxed);
        self.sync_flags();
        tracing::info!("continuation epoch closed");
    }
}
