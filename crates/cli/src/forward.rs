//! Forwarded mutations: non-holders ask the lease holder to validate and
//! journal ops over iroh (~1 LAN RTT) instead of rotating the lease.
//!
//! Safety still comes from the S3 lease (ADR-2): the holder is the only
//! appender. This module is the requester/holder glue around that rule.

use constellation_meta::{
    execute_mutate, MetaStore, MutateOp, MutateOutcome, SqliteMeta, TouchSet,
};
use constellation_net::{Payload, Peers};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Env: forward request timeout in ms (default 500).
pub fn forward_timeout_ms() -> u64 {
    std::env::var("CONSTELLATION_FORWARD_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500)
}

/// Cached holder id per partition, plus forward counters for status.
#[derive(Default)]
pub struct ForwardState {
    holders: Mutex<HashMap<String, u64>>,
    pub ok: AtomicU64,
    pub err: AtomicU64,
    pub latencies_us: Mutex<Vec<u64>>,
    pub pushed_applied: AtomicU64,
    next_req_id: AtomicU64,
}

impl ForwardState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn note_holder(&self, part: &str, holder: u64) {
        self.holders
            .lock()
            .unwrap()
            .insert(part.to_string(), holder);
    }

    pub fn cached_holder(&self, part: &str) -> Option<u64> {
        self.holders.lock().unwrap().get(part).copied()
    }

    pub fn clear_holder(&self, part: &str) {
        self.holders.lock().unwrap().remove(part);
    }

    pub fn next_req_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn record_ok(&self, took: Duration) {
        self.ok.fetch_add(1, Ordering::Relaxed);
        let mut v = self.latencies_us.lock().unwrap();
        v.push(took.as_micros() as u64);
        if v.len() > 256 {
            let drain = v.len() - 256;
            v.drain(0..drain);
        }
    }

    pub fn record_err(&self) {
        self.err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn p50_ms(&self) -> Option<u64> {
        let mut v = self.latencies_us.lock().unwrap().clone();
        if v.is_empty() {
            return None;
        }
        v.sort_unstable();
        Some(v[v.len() / 2] / 1000)
    }
}

/// Map MetaError to errno (same table as fusefs::errno).
pub fn meta_errno(e: &constellation_meta::MetaError) -> i32 {
    use constellation_meta::MetaError::*;
    match e {
        NoEnt(_) | NoEntry => libc::ENOENT,
        Exists => libc::EEXIST,
        NotDir => libc::ENOTDIR,
        IsDir => libc::EISDIR,
        NotEmpty => libc::ENOTEMPTY,
        NoData => libc::ENODATA,
        Invalid(_) => libc::EINVAL,
        Conflict => libc::EAGAIN,
        Sqlite(_) | Json(_) | Postcard(_) => libc::EIO,
    }
}

/// Holder: execute a forwarded op if we hold the lease for `part`.
pub fn holder_execute(
    meta: &SqliteMeta,
    ship_epoch: Option<u64>,
    is_lost: bool,
    known_holder: u64,
    op_bytes: &[u8],
) -> MutateOutcome {
    if is_lost {
        return MutateOutcome::Busy;
    }
    let Some(epoch) = ship_epoch else {
        return MutateOutcome::NotHolder {
            holder: known_holder,
        };
    };
    let op = match MutateOp::from_postcard(op_bytes) {
        Ok(o) => o,
        Err(_) => return MutateOutcome::Errno(libc::EINVAL),
    };
    match execute_mutate(meta, &op) {
        Ok(records) => MutateOutcome::Accepted { epoch, records },
        // Hand back what is current so the requester can rebase its
        // whole-file manifest without waiting to tail our segment.
        Err(constellation_meta::MetaError::Conflict) => match &op {
            MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                manifest: meta.manifest(*ino).unwrap_or(None),
            },
            _ => MutateOutcome::Errno(libc::EAGAIN),
        },
        Err(e) => MutateOutcome::Errno(meta_errno(&e)),
    }
}

/// Apply an accepted outcome on the requester: shadow + apply_foreign.
pub fn apply_accepted(
    meta: &SqliteMeta,
    part: &str,
    epoch: u64,
    records: &[constellation_meta::LogRecord],
) -> Result<(), constellation_meta::MetaError> {
    meta.shadow_insert(part, epoch, records)?;
    meta.apply_foreign(records, &TouchSet::default())?;
    Ok(())
}

/// Ask `holder` to execute `op`. Returns the outcome, or Busy on
/// transport / decode failure.
pub async fn request_mutate(
    peers: &Peers,
    forward: &ForwardState,
    part: &str,
    requester: u64,
    holder: u64,
    op: &MutateOp,
) -> MutateOutcome {
    let op_bytes = match op.to_postcard() {
        Ok(b) => b,
        Err(_) => return MutateOutcome::Errno(libc::EINVAL),
    };
    let req_id = forward.next_req_id();
    let payload = Payload::MutateRequest {
        part: part.to_string(),
        requester,
        req_id,
        epoch_seen: 0,
        op: op_bytes,
    };
    let started = Instant::now();
    let timeout = Duration::from_millis(forward_timeout_ms());
    let reply = tokio::time::timeout(timeout, peers.request_to_node(holder, &payload)).await;
    match reply {
        Ok(Ok(body)) => match body {
            Payload::MutateReply {
                req_id: rid,
                outcome,
            } if rid == req_id => {
                let outcome = if outcome.is_empty() {
                    MutateOutcome::Busy
                } else {
                    MutateOutcome::from_postcard(&outcome).unwrap_or(MutateOutcome::Busy)
                };
                match &outcome {
                    MutateOutcome::Accepted { .. } => forward.record_ok(started.elapsed()),
                    MutateOutcome::NotHolder { holder } => {
                        forward.note_holder(part, *holder);
                        forward.record_err();
                    }
                    _ => forward.record_err(),
                }
                outcome
            }
            _ => {
                forward.record_err();
                MutateOutcome::Busy
            }
        },
        _ => {
            forward.record_err();
            MutateOutcome::Busy
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holder_execute_not_holder() {
        let meta = SqliteMeta::open_in_memory().unwrap();
        let op = MutateOp::Unlink {
            parent: 1,
            name: "x".into(),
        }
        .to_postcard()
        .unwrap();

        match holder_execute(&meta, None, false, 7, &op) {
            MutateOutcome::NotHolder { holder: 7 } => {}
            other => panic!("{other:?}"),
        }
    }

    /// A requester composes a whole-file manifest on the image it last
    /// saw. If that image has moved on, installing it would drop the
    /// chunks that landed in between, so the holder must refuse *and*
    /// hand back what is current — otherwise the requester cannot
    /// rebase without waiting to tail the holder's segment.
    #[test]
    fn holder_execute_returns_the_current_manifest_on_a_stale_base() {
        let meta = SqliteMeta::open_in_memory().unwrap();
        let f = meta.create(1, "wd", 0o644, 0, 0).unwrap();
        meta.set_manifest_with_base(f.ino, None, b"current", 7)
            .unwrap();

        let op = MutateOp::SetManifest {
            ino: f.ino,
            base_manifest: Some(b"stale".to_vec()),
            manifest: b"mine".to_vec(),
            size: 4,
        }
        .to_postcard()
        .unwrap();

        match holder_execute(&meta, Some(1), false, 0, &op) {
            MutateOutcome::Conflict { manifest } => {
                assert_eq!(manifest.as_deref(), Some(&b"current"[..]))
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            meta.manifest(f.ino).unwrap().as_deref(),
            Some(&b"current"[..]),
            "the refused commit must not have landed"
        );
    }
}
