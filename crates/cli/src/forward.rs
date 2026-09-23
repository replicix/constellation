//! Forwarded mutations: non-holders ask the lease holder to validate and
//! journal ops over iroh (~1 LAN RTT) instead of rotating the lease.
//!
//! Safety still comes from the S3 lease (ADR-2): the holder is the only
//! appender. This module is the requester/holder glue around that rule.

use crate::keygate::KeyGate;
use constellation_fs_core::Ino;
use constellation_meta::{execute_mutate, Meta, MetaStore, MutateOp, MutateOutcome};
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

/// Env: bound on forwards actually in flight (network round trip +
/// requester-side apply) at once, per node. Distinct from however many
/// are merely queued behind the ordering gate (`ForwardState::gate`):
/// this only limits concurrency, it never affects correctness. Default
/// 64 (plan 29 M5).
pub fn forward_max_inflight() -> usize {
    std::env::var("CONSTELLATION_FORWARD_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(64)
}

/// The set of inodes `op` reads or writes, for the requester-side
/// ordering gate (plan 29 M5, `crate::keygate::KeyGate`). Deliberately
/// conservative: a name that cannot be resolved against the local
/// replica (not observed yet, or genuinely absent) is simply left out,
/// which is still safe because every case below already includes the
/// parent -- and any other op racing to touch the same not-yet-resolved
/// child necessarily touches that parent too, so serializing on the
/// parent alone is a safe (if slightly less concurrent) fallback.
pub fn conflict_keys(op: &MutateOp, meta: &Meta) -> Vec<Ino> {
    let lookup = |parent: Ino, name: &str| -> Option<Ino> {
        meta.lookup(parent, name).ok().flatten().map(|a| a.ino)
    };
    let mut keys = match op {
        MutateOp::Mkdir { parent, .. }
        | MutateOp::Create { parent, .. }
        | MutateOp::Symlink { parent, .. }
        | MutateOp::Mknod { parent, .. } => vec![*parent],
        // A hardlink bumps the target's nlink, so it conflicts with
        // anything else touching that inode, not just the new dentry's
        // parent.
        MutateOp::Link { ino, parent, .. } => vec![*parent, *ino],
        MutateOp::Unlink { parent, name } | MutateOp::Rmdir { parent, name } => {
            let mut k = vec![*parent];
            k.extend(lookup(*parent, name));
            k
        }
        // Rename can touch up to four inodes: the two directories (entry
        // add/remove, mtime/ctime), the moved inode itself (its `..`
        // link, or just its ctime), and whatever it replaces at the
        // destination (unlinked as part of the same op).
        MutateOp::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let mut k = vec![*parent, *new_parent];
            k.extend(lookup(*parent, name));
            k.extend(lookup(*new_parent, new_name));
            k
        }
        MutateOp::Setattr { ino, .. }
        | MutateOp::SetManifest { ino, .. }
        | MutateOp::SetXattr { ino, .. }
        | MutateOp::RemoveXattr { ino, .. } => vec![*ino],
        // Scratch -> shared publish: creates `ino` under `parent`.
        MutateOp::Publish { parent, ino, .. } => vec![*parent, *ino],
        // Best-effort batch; included for completeness (see the module
        // doc on why `AtimeBatch` never actually goes through the gate).
        MutateOp::AtimeBatch { entries } => entries.iter().map(|(ino, ..)| *ino).collect(),
        // Plan 30 §M3b: a replayed transaction without an op of its own.
        // Every inode and parent its records name.
        MutateOp::Records { records } => {
            let touched = constellation_meta::TouchSet::from_records(records.iter());
            let mut k: Vec<Ino> = touched.inos.into_iter().collect();
            k.extend(touched.dentries.into_iter().map(|(parent, _)| parent));
            k
        }
    };
    keys.sort_unstable();
    keys.dedup();
    keys
}

/// Env: `CONSTELLATION_FORWARD=off|0|false` disables requester-side
/// forwarding. Non-holder mutations then fall back to ordinary lease
/// acquisition (P2P handoff, then S3 CAS) — the pre-forwarding
/// behavior. Exists for operators who want writer-follows-lease
/// placement, and for the harness to exercise the takeover path
/// deliberately (`p2p-handover`).
pub fn forwarding_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("CONSTELLATION_FORWARD") {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "off" | "0" | "false"
        ),
        Err(_) => true,
    })
}

/// The `Rid::incarnation` every [`ForwardState::next_system_rid`]
/// carries: a reserved namespace no mount incarnation reaches.
pub const SYSTEM_RID_INCARNATION: u32 = u32::MAX;

/// Cached holder id per partition, plus forward counters for status.
pub struct ForwardState {
    holders: Mutex<HashMap<String, u64>>,
    pub ok: AtomicU64,
    pub err: AtomicU64,
    pub latencies_us: Mutex<Vec<u64>>,
    pub pushed_applied: AtomicU64,
    next_req_id: AtomicU64,
    /// Requester-side ordering gate (plan 29 M5): see `crate::keygate`.
    /// Serializes forwards whose conflict-key sets overlap so a spawned
    /// forward task (`node_runtime.rs`'s `SyncRequest::Forward` arm)
    /// cannot land on the holder, or get applied back here, out of the
    /// order this node's own FUSE threads issued them in.
    pub gate: Arc<KeyGate>,
    /// Bounds forwards actually in flight (network round trip + apply)
    /// at once; see [`forward_max_inflight`].
    pub inflight: Arc<tokio::sync::Semaphore>,
    /// Plan 30 §M2 status counters: holder-side dedup hits (a retried
    /// rid answered from `recent`/`completed` instead of re-executed)
    /// and requester-side same-rid forward retries.
    pub dedup_hits: AtomicU64,
    pub retries: AtomicU64,
    /// Plan 30 §M2 GC: this requester's own "highest contiguous rid.seq
    /// whose reply arrived" tracker, sent as `acked_through` on every
    /// subsequent request so the holder can drop its `recent` cache for
    /// this requester's incarnation up to that point.
    ///
    /// Shared (via `Arc`) with every `SyncHandle` this node hands out
    /// (`fusefs::SyncHandle::acked`), not owned solely by `ForwardState`:
    /// a rid's seq must be marked done on *every* completion path of
    /// `mutate_op_rebasable` — the local-holder fast path, a
    /// designation's `Proceed`, an explicit refusal, and the lease-path
    /// fallback all end an op without ever going through this module —
    /// not just the forward path this struct otherwise deals with.
    /// Missing any of those left `acked_through` permanently stuck
    /// behind the first such op, which in turn made every later
    /// `forget_acked_through` call a no-op and the holder's `recent` map
    /// grow without bound (plan 30 M2 coordinator review).
    acked: Arc<Mutex<AckTracker>>,
    /// This mount's persisted incarnation (`Meta::bump_incarnation`),
    /// folded into every [`ForwardState::next_system_rid`] so system
    /// rids never repeat across mounts.
    mount_incarnation: u32,
    /// Per-mount counter behind [`ForwardState::next_system_rid`];
    /// restarts at 0 on every mount (only its low 32 bits are used).
    system_rid_seq: AtomicU64,
}

/// Tracks the highest contiguous seq acknowledged, from a set of
/// out-of-order completions (disjoint forwards may finish in any
/// order). `floor` only ever advances past a run with no gap.
///
/// `pub(crate)` (not private): shared via `Arc` with
/// `fusefs::SyncHandle`, so `mutate_op_rebasable` can mark a rid done on
/// paths this module never sees.
#[derive(Default)]
pub(crate) struct AckTracker {
    floor: u64,
    done: std::collections::BTreeSet<u64>,
}

impl AckTracker {
    pub(crate) fn mark_done(&mut self, seq: u64) {
        self.done.insert(seq);
        while self.done.remove(&(self.floor + 1)) {
            self.floor += 1;
        }
    }

    fn floor(&self) -> u64 {
        self.floor
    }
}

impl ForwardState {
    /// `mount_incarnation` is the value this mount's
    /// `Meta::bump_incarnation` returned.
    pub fn new(mount_incarnation: u32) -> Arc<Self> {
        Arc::new(Self {
            holders: Mutex::new(HashMap::new()),
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            latencies_us: Mutex::new(Vec::new()),
            pushed_applied: AtomicU64::new(0),
            next_req_id: AtomicU64::new(0),
            gate: KeyGate::new(),
            inflight: Arc::new(tokio::sync::Semaphore::new(forward_max_inflight())),
            dedup_hits: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            acked: Arc::new(Mutex::new(AckTracker::default())),
            mount_incarnation,
            system_rid_seq: AtomicU64::new(0),
        })
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

    /// A rid for a mutation not issued through the FUSE write path's own
    /// allocator (`fusefs::SyncHandle::next_rid_seq`) — retention
    /// pruning's forwarded unlinks, conflict-copy steps and best-effort
    /// atime batches.
    ///
    /// `incarnation` is the reserved [`SYSTEM_RID_INCARNATION`]
    /// (`u32::MAX`), which no real mount incarnation (bumped by one per
    /// mount) can ever reach, so these never collide with — or share a
    /// holder `recent` bucket with — a genuine FUSE-issued rid from the
    /// same node. `seq` is `mount_incarnation << 32 | counter`: the
    /// counter is volatile and restarts at 0 every mount, so the mount's
    /// persisted incarnation in the high bits is what keeps a system rid
    /// from being reissued after a restart and answered as "already
    /// done" from a `completed` row (or a holder's `recent` entry) the
    /// previous mount left behind within the retention window. Each call
    /// still allocates a fresh counter value, so two *different*
    /// system-generated ops are never confused for retries of each
    /// other. The counter wraps within its 32 bits rather than spilling
    /// into the incarnation bits; a reuse would take 2^32 system ops in
    /// one mount, all within one completion-retention window.
    pub fn next_system_rid(&self, node: u64) -> constellation_meta::Rid {
        let counter = self.system_rid_seq.fetch_add(1, Ordering::Relaxed) & u64::from(u32::MAX);
        constellation_meta::Rid {
            node,
            incarnation: SYSTEM_RID_INCARNATION,
            seq: (u64::from(self.mount_incarnation) << 32) | counter,
        }
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

    /// The value to send as `acked_through` on the next request. Marking
    /// a rid done (`AckTracker::mark_done`) happens on the shared
    /// tracker directly — see [`ForwardState::acked_tracker`] and
    /// `fusefs::SyncHandle::acked` — since every completion path of
    /// `mutate_op_rebasable` must do it, not just the forward path this
    /// struct otherwise deals with.
    pub fn acked_through(&self) -> u64 {
        self.acked.lock().unwrap().floor()
    }

    /// The shared tracker itself, for `fusefs::SyncHandle` to hold its
    /// own clone of — see [`ForwardState::acked`]'s doc for why a single
    /// `Arc` needs to reach both sides.
    pub(crate) fn acked_tracker(&self) -> Arc<Mutex<AckTracker>> {
        self.acked.clone()
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
        Fjall(_) | Io(_) | Record(_) | Key(_) | Json(_) | Postcard(_) => libc::EIO,
    }
}

/// Holder: execute a forwarded op if we hold the lease for `part`.
///
/// Plan 30 §M2 dedup: before executing, checks whether `rid` already
/// completed — either durably (`meta.completed_position`, a segment
/// this replica has tailed, which for the *holder's own* replica means
/// an earlier tenure already shipped it) or in the volatile `recent`
/// map (this same tenure executed it moments ago and has not shipped
/// yet). Either way, the reply is the identical `Accepted` a retry must
/// see — "an executed rid is never executed again." Only a fresh
/// (never-seen) rid reaches `execute_mutate`, and only its `Ok` outcome
/// is remembered: a refusal is never recorded, so a retried refused op
/// is simply re-evaluated (plan 30 §M2: "a retried refused op is
/// re-evaluated, and takes effect (or not) at the retry, which is
/// linearizable").
#[allow(clippy::too_many_arguments)]
pub fn holder_execute(
    meta: &Meta,
    ship_epoch: Option<u64>,
    is_lost: bool,
    known_holder: u64,
    op_bytes: &[u8],
    ship_floor: u64,
    rid: constellation_meta::Rid,
    forward: Option<&ForwardState>,
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
    // Only the volatile `recent` map can answer a *network* retry with
    // full records (an already-shipped rid's records are no longer
    // cheaply available locally — see `Meta::recent_outcome`'s doc); the
    // durable `completed` keyspace resolves the lease-path retry
    // instead (`fusefs.rs::mutate_op_rebasable`), where only presence,
    // not the records, is needed.
    if let Some(records) = meta.recent_outcome(rid) {
        if let Some(forward) = forward {
            forward.dedup_hits.fetch_add(1, Ordering::Relaxed);
        }
        return MutateOutcome::Accepted { epoch, records };
    }
    // Plan 30 §M3a: a rid whose outcome `recent` no longer holds (acked
    // and dropped, aged out, or completed by an earlier tenure) may still
    // have taken effect. The requester that retries it — a stranded op's
    // replay by rid above all — must not have it executed twice, so a
    // rid the log (or this tenure's own journal, which writes `completed`
    // in the op's transaction) already completed is answered as accepted
    // with no records: the effect is already in the log, and reaches the
    // requester by tailing.
    if meta.completed_position(rid).ok().flatten().is_some() {
        if let Some(forward) = forward {
            forward.dedup_hits.fetch_add(1, Ordering::Relaxed);
        }
        return MutateOutcome::Accepted {
            epoch,
            records: Vec::new(),
        };
    }
    let executed = execute_mutate(meta, &op, Some(rid));
    match executed {
        Ok(records) => {
            meta.remember_outcome(rid, &records);
            MutateOutcome::Accepted { epoch, records }
        }
        // Hand back what is current so the requester can rebase its
        // whole-file manifest without waiting to tail our segment.
        Err(constellation_meta::MetaError::Conflict) => match &op {
            MutateOp::SetManifest { ino, .. } => MutateOutcome::Conflict {
                manifest: meta.manifest(*ino).unwrap_or(None),
            },
            _ => MutateOutcome::Errno(libc::EAGAIN),
        },
        // POSIX keeps the refusal (`mkdir` is a lock primitive), but this
        // requester's replica may not have the entry the refusal is about
        // yet, and its very next step resolves that name locally. Hand it
        // back with the errno; `MutateOutcome::Exists` explains why
        // applying it early is safe.
        Err(constellation_meta::MetaError::Exists) => match named_child(&op) {
            Some((parent, name)) => match meta.entry_as_record(parent, name) {
                Ok(Some(record)) => MutateOutcome::Exists {
                    records: vec![record],
                    ship_floor,
                    epoch,
                },
                _ => MutateOutcome::Errno(libc::EEXIST),
            },
            None => MutateOutcome::Errno(libc::EEXIST),
        },
        Err(e) => MutateOutcome::Errno(meta_errno(&e)),
    }
}

/// The `(parent, name)` a create-family op names, if any.
fn named_child(op: &MutateOp) -> Option<(Ino, &str)> {
    match op {
        MutateOp::Mkdir { parent, name, .. }
        | MutateOp::Create { parent, name, .. }
        | MutateOp::Symlink { parent, name, .. }
        | MutateOp::Mknod { parent, name, .. }
        | MutateOp::Link { parent, name, .. } => Some((*parent, name.as_str())),
        _ => None,
    }
}

/// After a forwarded op the holder refused because of state this replica
/// may not have seen yet, the entry the refusal is about and whether it
/// must exist locally before the errno reaches the caller.
///
/// The create-family `EEXIST` case is normally answered by
/// [`MutateOutcome::Exists`] carrying the entry; this covers the cases
/// where that record must not be applied (see the caller's
/// applied-position guard) and the `unlink`/`rmdir` mirror image, where
/// there is nothing to carry: `ENOENT` means the name is already gone on
/// the holder, while a stale local entry would make the kernel answer
/// the next `O_EXCL` create with `EEXIST` without ever asking the
/// holder.
pub fn causal_wait_target(op: &MutateOp, outcome: &MutateOutcome) -> Option<(Ino, String, bool)> {
    match outcome {
        // `Exists` normally carries the entry and is applied directly,
        // but not when that would be unsafe (the requester applied log
        // segments while the refusal was in flight, so the entry it
        // carries may already have been deleted) or when the holder had
        // nothing to send. Then this is the fallback.
        MutateOutcome::Exists { .. } => named_child(op).map(|(p, n)| (p, n.to_string(), true)),
        MutateOutcome::Errno(libc::EEXIST) => {
            named_child(op).map(|(p, n)| (p, n.to_string(), true))
        }
        MutateOutcome::Errno(libc::ENOENT) => match op {
            MutateOp::Unlink { parent, name } | MutateOp::Rmdir { parent, name } => {
                Some((*parent, name.clone(), false))
            }
            _ => None,
        },
        _ => None,
    }
}

/// How long a requester waits for [`causal_wait_target`] to hold locally
/// before returning the errno anyway (the entry may have changed again).
pub const CAUSAL_WAIT: Duration = Duration::from_secs(3);

/// Apply an accepted outcome on the requester: install its records ahead
/// of the log as a speculation-log shadow (plan 30 §M3a,
/// `Meta::install_shadow`), recording `rid`, `op` and the accepting
/// holder's `epoch` so the shadow retires when its `Completed` arrives
/// from the log, or is rolled back and replayed by rid if a later epoch
/// strands it. Returns whether it was installed: not when this replica
/// already tailed the op's completion (the effect is already part of the
/// log prefix), and — plan 30 §M3b — not when this node now holds the
/// lease at a higher epoch than the one that accepted it (the op is
/// queued for replay by rid instead; see `Meta::install_shadow`).
pub fn apply_accepted(
    meta: &Meta,
    epoch: u64,
    rid: constellation_meta::Rid,
    op: &MutateOp,
    records: &[constellation_meta::LogRecord],
) -> Result<bool, constellation_meta::MetaError> {
    meta.install_shadow(rid, epoch, op, records)
}

/// Whether records the holder handed back may be installed ahead of the
/// log on this replica.
///
/// Everything a replica applies from the log is applied in the holder's
/// order, so it can only move forward. A forwarded op's reply is the one
/// place records arrive *outside* that order, to spare the caller a
/// round trip through S3 — and a record installed out of order can move
/// the replica *backwards*: the entry it names may already have been
/// deleted by a later record this replica has applied, and re-inserting
/// it resurrects it for good, since the log has nothing after the delete
/// to correct it with.
///
/// `ship_floor` is the lowest sequence anything the holder does from now
/// on can ship in. A delete of what the reply describes is necessarily
/// later than the reply, so it ships at or above the floor; a replica
/// whose replay is still below the floor therefore cannot have seen one,
/// and installing the records early is exactly equivalent to applying
/// them in order, just sooner. At or above it, the caller must not
/// install them: the log will deliver them in order anyway.
pub fn safe_to_install_early(meta: &Meta, ship_floor: u64) -> bool {
    meta.applied_seq().is_ok_and(|applied| applied < ship_floor)
}

/// Plan 30 §M2: "the same holder, with backoff (three attempts or 2s)"
/// before falling to a redirected holder and then the lease path.
pub const MAX_FORWARD_RETRY_ATTEMPTS: u32 = 3;

/// Backoff before retry `attempt` (1-based): 200ms, 400ms, 600ms — three
/// attempts sum to 1.2s, comfortably under the plan's "2s" budget even
/// with the request timeouts themselves on top.
pub fn forward_retry_backoff(attempt: u32) -> Duration {
    Duration::from_millis(200 * u64::from(attempt))
}

/// Ask `holder` to execute `op`, identified by `rid` (plan 30 §M2:
/// stable across every retry, unlike the wire `req_id` correlation id
/// minted fresh below). Returns the outcome, or Busy on transport /
/// decode failure. `acked_through` is this requester's own GC receipt
/// (see [`ForwardState::acked_through`]).
#[allow(clippy::too_many_arguments)]
pub async fn request_mutate(
    peers: &Peers,
    forward: &ForwardState,
    part: &str,
    requester: u64,
    holder: u64,
    op: &MutateOp,
    rid: constellation_meta::Rid,
    acked_through: u64,
) -> MutateOutcome {
    request_mutate_with(
        peers,
        forward,
        part,
        requester,
        holder,
        op,
        rid,
        acked_through,
        Duration::from_millis(forward_timeout_ms()),
    )
    .await
}

/// As [`request_mutate`], with an explicit timeout. Atime batches use a
/// shorter deadline (`CONSTELLATION_ATIME_FORWARD_TIMEOUT_MS`) than the
/// ordinary mutation forward: a missed atime bump is free to lose.
#[allow(clippy::too_many_arguments)]
pub async fn request_mutate_with(
    peers: &Peers,
    forward: &ForwardState,
    part: &str,
    requester: u64,
    holder: u64,
    op: &MutateOp,
    rid: constellation_meta::Rid,
    acked_through: u64,
    timeout: Duration,
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
        rid: (rid.node, rid.incarnation, rid.seq),
        acked_through,
    };
    let started = Instant::now();
    let reply = tokio::time::timeout(timeout, peers.request_to_node(holder, &payload)).await;
    match reply {
        Ok(Ok(body)) => match body {
            Payload::MutateReply {
                req_id: reply_req_id,
                outcome,
            } if reply_req_id == req_id => {
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

    fn test_rid(seq: u64) -> constellation_meta::Rid {
        constellation_meta::Rid {
            node: 1,
            incarnation: 1,
            seq,
        }
    }

    /// Plan 30 §M2 holder dedup, as a unit test: a second `holder_execute`
    /// call with the *same* rid must not execute the op again (no second
    /// journal row), and must answer with the identical records the
    /// first call produced — the shape a retried forward relies on. A
    /// *different* rid for a different op still executes normally.
    #[test]
    fn holder_execute_dedups_a_retried_rid() {
        use constellation_fs_core::types::ROOT_INO;

        let holder = Meta::open_in_memory().unwrap();
        let op_bytes = MutateOp::Create {
            parent: ROOT_INO,
            name: "f".into(),
            ino: (1 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
        .to_postcard()
        .unwrap();
        let rid = test_rid(1);
        let forward = ForwardState::new(1);

        let first = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &op_bytes,
            1,
            rid,
            Some(&forward),
        );
        let first_records = match first {
            MutateOutcome::Accepted { records, .. } => records,
            other => panic!("expected Accepted, got {other:?}"),
        };
        assert_eq!(forward.dedup_hits.load(Ordering::Relaxed), 0);
        let journal_len_after_first = holder.journal_len().unwrap();

        // Retry: identical reply, no second execution.
        let second = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &op_bytes,
            1,
            rid,
            Some(&forward),
        );
        let second_records = match second {
            MutateOutcome::Accepted { records, .. } => records,
            other => panic!("expected Accepted (from cache), got {other:?}"),
        };
        assert_eq!(
            first_records, second_records,
            "a retried rid must get an identical reply"
        );
        assert_eq!(
            holder.journal_len().unwrap(),
            journal_len_after_first,
            "an executed rid must never be executed again"
        );
        assert_eq!(forward.dedup_hits.load(Ordering::Relaxed), 1);

        // A genuinely different op (fresh rid) still executes.
        let op2_bytes = MutateOp::Create {
            parent: ROOT_INO,
            name: "g".into(),
            ino: (1 << 40) | 2,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
        .to_postcard()
        .unwrap();
        let third = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &op2_bytes,
            1,
            test_rid(2),
            Some(&forward),
        );
        assert!(matches!(third, MutateOutcome::Accepted { .. }));
        assert_eq!(
            holder.journal_len().unwrap(),
            journal_len_after_first + 2,
            "a different op must still execute (its own record + Completed)"
        );
        assert_eq!(forward.dedup_hits.load(Ordering::Relaxed), 1, "unaffected");
    }

    /// Plan 30 §M2 coordinator review: `acked_through` must advance past
    /// an op that never went through `holder_execute`/`ForwardState` at
    /// all — the shape `mutate_op_rebasable`'s local-holder fast path,
    /// an early refusal, or the lease-path fallback takes. Simulated
    /// here by marking some seqs done directly on the *shared* tracker
    /// (`ForwardState::acked_tracker`, the same `Arc`
    /// `fusefs::SyncHandle::acked` holds) with no forward involved,
    /// interleaved with seqs that genuinely go through `holder_execute`.
    /// Before this fix, only the forwarded seqs ever got marked, so the
    /// very first "local" seq in the sequence would permanently stall
    /// the contiguous floor.
    #[test]
    fn acked_through_advances_across_local_and_forwarded_completions() {
        use constellation_fs_core::types::ROOT_INO;

        let forward = ForwardState::new(1);
        let acked = forward.acked_tracker();
        let holder = Meta::open_in_memory().unwrap();
        const N: u64 = 30;

        for seq in 0..N {
            let rid = test_rid(seq);
            if seq % 3 == 0 {
                // "Local" completion: mutate_op_rebasable's own paths
                // that never touch `forward`/`holder_execute` at all.
                acked.lock().unwrap().mark_done(seq);
            } else {
                let op_bytes = MutateOp::Create {
                    parent: ROOT_INO,
                    name: format!("f{seq}"),
                    ino: (1 << 40) | seq,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                }
                .to_postcard()
                .unwrap();
                let outcome = holder_execute(
                    &holder,
                    Some(1),
                    false,
                    1,
                    &op_bytes,
                    1,
                    rid,
                    Some(&forward),
                );
                assert!(matches!(outcome, MutateOutcome::Accepted { .. }));
                acked.lock().unwrap().mark_done(seq);
            }
        }
        assert_eq!(
            forward.acked_through(),
            N - 1,
            "the contiguous floor must reach the last seq even though \
             two thirds of them never went through a forward"
        );

        // `recent` on the holder must now be fully reclaimable: nothing
        // is younger than `acked_through`, so pruning empties this
        // requester's whole bucket, not just the forwarded third of it.
        holder.forget_acked_through(1, 1, forward.acked_through());
        for seq in 0..N {
            if seq % 3 != 0 {
                assert!(
                    holder.recent_outcome(test_rid(seq)).is_none(),
                    "seq {seq} should have been forgotten once fully acked"
                );
            }
        }
    }

    /// Plan 30 §M2's in-doubt retry, in process on two replicas (the
    /// requester's own `Meta` and the holder's): the first attempt's
    /// reply is dropped (simulating the timeout `CONSTELLATION_FAULT_FORWARD_REPLY_DELAY_MS`
    /// reproduces in the harness), so the requester never applies it.
    /// The retry, under the *same* rid, gets the holder's cached
    /// outcome and the requester applies that instead — exactly once,
    /// converging both replicas without a second execution anywhere.
    #[test]
    fn in_doubt_retry_across_two_replicas_executes_exactly_once() {
        use constellation_fs_core::types::ROOT_INO;

        let holder = Meta::open_in_memory().unwrap();
        let requester = Meta::open_in_memory().unwrap();
        let op = MutateOp::Mkdir {
            parent: ROOT_INO,
            name: "d".into(),
            ino: (1 << 40) | 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
        };
        let op_bytes = op.to_postcard().unwrap();
        let rid = test_rid(1);

        // Attempt 1: holder executes, but the reply is lost (the
        // requester's `tokio::time::timeout` fires first) — it is never
        // applied here.
        let attempt1 = holder_execute(&holder, Some(1), false, 1, &op_bytes, 1, rid, None);
        assert!(matches!(attempt1, MutateOutcome::Accepted { .. }));

        // Attempt 2: the requester retries the *same* rid to the same
        // holder. Plan 30 §M2's fix is exactly this: the holder answers
        // from `recent` instead of re-executing, so this reply is safe
        // to apply even though the op already took effect.
        let attempt2 = holder_execute(&holder, Some(1), false, 1, &op_bytes, 1, rid, None);
        let (epoch, records) = match attempt2 {
            MutateOutcome::Accepted { epoch, records } => (epoch, records),
            other => panic!("expected Accepted, got {other:?}"),
        };
        apply_accepted(&requester, epoch, rid, &op, &records).unwrap();

        assert_eq!(
            holder.journal_len().unwrap(),
            2,
            "executed exactly once (the Mkdir record + its Completed marker)"
        );
        assert_eq!(
            holder.dump_replicated().unwrap(),
            requester.dump_replicated().unwrap(),
            "both replicas converge on a single execution"
        );
    }

    /// Plan 30 §M3a: once `recent` no longer holds a rid's outcome (acked
    /// and dropped, aged out, or completed by an earlier tenure), a retry
    /// of it — a stranded op's replay by rid, typically — is still
    /// answered from `completed`, with no records, and never executed a
    /// second time.
    #[test]
    fn holder_execute_answers_a_completed_rid_without_reexecuting() {
        use constellation_fs_core::types::ROOT_INO;

        let holder = Meta::open_in_memory().unwrap();
        let op_bytes = MutateOp::Create {
            parent: ROOT_INO,
            name: "f".into(),
            ino: (1 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
        .to_postcard()
        .unwrap();
        let rid = test_rid(1);
        let first = holder_execute(&holder, Some(1), false, 1, &op_bytes, 1, rid, None);
        assert!(matches!(first, MutateOutcome::Accepted { .. }));
        let journal_len = holder.journal_len().unwrap();
        holder.forget_acked_through(rid.node, rid.incarnation, rid.seq);
        assert!(holder.recent_outcome(rid).is_none());

        let forward = ForwardState::new(1);
        let retry = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &op_bytes,
            1,
            rid,
            Some(&forward),
        );
        match retry {
            MutateOutcome::Accepted { records, .. } => assert!(records.is_empty()),
            other => panic!("expected Accepted, got {other:?}"),
        }
        assert_eq!(
            holder.journal_len().unwrap(),
            journal_len,
            "not re-executed"
        );
        assert_eq!(forward.dedup_hits.load(Ordering::Relaxed), 1);
    }

    /// System rids must not repeat across mounts: the per-mount counter
    /// restarts at 0, so two successive mounts' first system rids differ
    /// only because the mount incarnation is folded in. They stay in the
    /// reserved system namespace, disjoint from FUSE rids.
    #[test]
    fn system_rids_differ_across_mounts() {
        let first_mount = ForwardState::new(1);
        let second_mount = ForwardState::new(2);
        let a = first_mount.next_system_rid(7);
        let b = second_mount.next_system_rid(7);
        assert_ne!(a, b);
        assert_eq!(a.incarnation, SYSTEM_RID_INCARNATION);
        assert_eq!(b.incarnation, SYSTEM_RID_INCARNATION);
        assert_ne!(first_mount.next_system_rid(7), a, "fresh rid per call");
    }

    /// A restarted node's first system op (e.g. a retention prune's
    /// `unlink_now`) must execute, not be answered from the `completed`
    /// row — or the holder's `recent` entry — the previous mount's first
    /// system op left behind within the retention window.
    #[test]
    fn a_second_mounts_system_op_is_not_answered_from_the_first_mounts_completion() {
        use constellation_fs_core::types::ROOT_INO;

        let holder = Meta::open_in_memory().unwrap();
        let create = |name: &str, ino| {
            MutateOp::Create {
                parent: ROOT_INO,
                name: name.into(),
                ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
            }
            .to_postcard()
            .unwrap()
        };

        let first_mount = ForwardState::new(1);
        let rid1 = first_mount.next_system_rid(1);
        let first = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &create("a", (1 << 40) | 1),
            1,
            rid1,
            Some(&first_mount),
        );
        assert!(matches!(first, MutateOutcome::Accepted { .. }));
        assert!(holder.completed_position(rid1).unwrap().is_some());
        let journal_len = holder.journal_len().unwrap();

        let second_mount = ForwardState::new(2);
        let rid2 = second_mount.next_system_rid(1);
        let second = holder_execute(
            &holder,
            Some(1),
            false,
            1,
            &create("b", (1 << 40) | 2),
            1,
            rid2,
            Some(&second_mount),
        );
        match second {
            MutateOutcome::Accepted { records, .. } => assert!(!records.is_empty()),
            other => panic!("expected Accepted, got {other:?}"),
        }
        assert!(holder.journal_len().unwrap() > journal_len, "executed");
        assert_eq!(second_mount.dedup_hits.load(Ordering::Relaxed), 0);
    }

    /// Plan 29 M6: a create-family op the holder refuses with `EEXIST`
    /// carries the entry that is already there, so the requester can
    /// resolve that name without waiting for the holder's segment. The
    /// refusal itself stands — POSIX requires `mkdir` on an existing
    /// name to fail, and `mkdir` is a lock primitive — but a caller that
    /// acts on it (`mkdir -p` walking in) must not then get `ENOENT`.
    #[test]
    fn an_eexist_refusal_carries_the_entry_that_is_already_there() {
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;

        let holder = Meta::open_in_memory().unwrap();
        let existing = holder.mkdir(ROOT_INO, "d", 0o755, 7, 9).unwrap();
        let op = MutateOp::Mkdir {
            parent: ROOT_INO,
            name: "d".into(),
            ino: 0,
            mode: 0o755,
            uid: 7,
            gid: 9,
        }
        .to_postcard()
        .unwrap();

        let records = match holder_execute(&holder, Some(1), false, 1, &op, 1, test_rid(1), None) {
            MutateOutcome::Exists { records, .. } => records,
            other => panic!("expected Exists, got {other:?}"),
        };

        // A replica that has never seen the entry can install it from the
        // refusal alone, and the holder's later segment is idempotent.
        let replica = Meta::open_in_memory().unwrap();
        assert!(replica.lookup(ROOT_INO, "d").unwrap().is_none());
        replica.apply_records(&records).unwrap();
        let seen = replica.lookup(ROOT_INO, "d").unwrap().expect("installed");
        assert_eq!(seen.ino, existing.ino);
        let parent_nlink = replica.getattr(ROOT_INO).unwrap().unwrap().nlink;
        replica.apply_records(&records).unwrap();
        assert_eq!(
            replica.getattr(ROOT_INO).unwrap().unwrap().nlink,
            parent_nlink,
            "re-applying the same creation must not double-count the parent's nlink"
        );
    }

    /// A create-family op whose name is genuinely free is unaffected, and
    /// a non-create op that fails with `EEXIST` keeps the bare errno.
    #[test]
    fn a_plain_errno_is_still_a_plain_errno() {
        use constellation_fs_core::types::ROOT_INO;

        let holder = Meta::open_in_memory().unwrap();
        let op = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "gone".into(),
        }
        .to_postcard()
        .unwrap();
        match holder_execute(&holder, Some(1), false, 1, &op, 1, test_rid(2), None) {
            MutateOutcome::Errno(e) => assert_eq!(e, libc::ENOENT),
            other => panic!("expected a bare errno, got {other:?}"),
        }
    }

    #[test]
    fn holder_execute_not_holder() {
        let meta = Meta::open_in_memory().unwrap();
        let op = MutateOp::Unlink {
            parent: 1,
            name: "x".into(),
        }
        .to_postcard()
        .unwrap();

        match holder_execute(&meta, None, false, 7, &op, 1, test_rid(3), None) {
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
        let meta = Meta::open_in_memory().unwrap();
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

        match holder_execute(&meta, Some(1), false, 0, &op, 1, test_rid(4), None) {
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

    mod conflict_keys_tests {
        use super::*;
        use constellation_fs_core::types::ROOT_INO;

        fn sorted(mut v: Vec<Ino>) -> Vec<Ino> {
            v.sort_unstable();
            v
        }

        #[test]
        fn create_family_keys_on_parent_only() {
            let meta = Meta::open_in_memory().unwrap();
            for op in [
                MutateOp::Mkdir {
                    parent: ROOT_INO,
                    name: "d".into(),
                    ino: 999,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                },
                MutateOp::Create {
                    parent: ROOT_INO,
                    name: "f".into(),
                    ino: 999,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
                MutateOp::Symlink {
                    parent: ROOT_INO,
                    name: "s".into(),
                    ino: 999,
                    target: "x".into(),
                    uid: 0,
                    gid: 0,
                },
                MutateOp::Mknod {
                    parent: ROOT_INO,
                    name: "n".into(),
                    ino: 999,
                    kind: 3,
                    mode: 0o600,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                },
            ] {
                assert_eq!(conflict_keys(&op, &meta), vec![ROOT_INO], "{op:?}");
            }
        }

        #[test]
        fn link_keys_on_parent_and_target_ino() {
            let meta = Meta::open_in_memory().unwrap();
            let op = MutateOp::Link {
                ino: 42,
                parent: ROOT_INO,
                name: "l".into(),
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![ROOT_INO, 42])
            );
        }

        #[test]
        fn unlink_resolves_child_when_present() {
            let meta = Meta::open_in_memory().unwrap();
            let f = meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
            let op = MutateOp::Unlink {
                parent: ROOT_INO,
                name: "f".into(),
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![ROOT_INO, f.ino])
            );
        }

        #[test]
        fn rmdir_resolves_child_when_present() {
            let meta = Meta::open_in_memory().unwrap();
            let d = meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
            let op = MutateOp::Rmdir {
                parent: ROOT_INO,
                name: "d".into(),
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![ROOT_INO, d.ino])
            );
        }

        #[test]
        fn unlink_falls_back_to_parent_only_when_unresolvable() {
            // Conservative fallback: a name this replica has never seen
            // still yields a safe (if less concurrent) key set.
            let meta = Meta::open_in_memory().unwrap();
            let op = MutateOp::Unlink {
                parent: ROOT_INO,
                name: "ghost".into(),
            };
            assert_eq!(conflict_keys(&op, &meta), vec![ROOT_INO]);
        }

        #[test]
        fn rename_resolves_both_parents_and_moved_and_replaced_ino() {
            let meta = Meta::open_in_memory().unwrap();
            let src_dir = meta.mkdir(ROOT_INO, "src", 0o755, 0, 0).unwrap();
            let dst_dir = meta.mkdir(ROOT_INO, "dst", 0o755, 0, 0).unwrap();
            let moved = meta.create(src_dir.ino, "a", 0o644, 0, 0).unwrap();
            let replaced = meta.create(dst_dir.ino, "b", 0o644, 0, 0).unwrap();
            let op = MutateOp::Rename {
                parent: src_dir.ino,
                name: "a".into(),
                new_parent: dst_dir.ino,
                new_name: "b".into(),
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![src_dir.ino, dst_dir.ino, moved.ino, replaced.ino])
            );
        }

        #[test]
        fn rename_without_a_replacement_target_omits_it() {
            let meta = Meta::open_in_memory().unwrap();
            let src_dir = meta.mkdir(ROOT_INO, "src", 0o755, 0, 0).unwrap();
            let dst_dir = meta.mkdir(ROOT_INO, "dst", 0o755, 0, 0).unwrap();
            let moved = meta.create(src_dir.ino, "a", 0o644, 0, 0).unwrap();
            let op = MutateOp::Rename {
                parent: src_dir.ino,
                name: "a".into(),
                new_parent: dst_dir.ino,
                new_name: "new".into(),
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![src_dir.ino, dst_dir.ino, moved.ino])
            );
        }

        #[test]
        fn attr_like_ops_key_on_their_own_ino_only() {
            let meta = Meta::open_in_memory().unwrap();
            for op in [
                MutateOp::Setattr {
                    ino: 5,
                    mode: Some(0o600),
                    uid: None,
                    gid: None,
                    size: None,
                    atime_ns: None,
                    mtime_ns: None,
                },
                MutateOp::SetManifest {
                    ino: 5,
                    base_manifest: None,
                    manifest: b"m".to_vec(),
                    size: 0,
                },
                MutateOp::SetXattr {
                    ino: 5,
                    name: "user.x".into(),
                    value: b"v".to_vec(),
                    mode: 0,
                },
                MutateOp::RemoveXattr {
                    ino: 5,
                    name: "user.x".into(),
                },
            ] {
                assert_eq!(conflict_keys(&op, &meta), vec![5], "{op:?}");
            }
        }

        #[test]
        fn publish_keys_on_parent_and_ino() {
            let meta = Meta::open_in_memory().unwrap();
            let op = MutateOp::Publish {
                ino: 77,
                parent: ROOT_INO,
                name: "p".into(),
                mode: 0o644,
                uid: 0,
                gid: 0,
                mtime_ns: 0,
                manifest: b"m".to_vec(),
                size: 0,
                xattrs: vec![],
            };
            assert_eq!(
                sorted(conflict_keys(&op, &meta)),
                sorted(vec![ROOT_INO, 77])
            );
        }

        #[test]
        fn atime_batch_keys_on_every_ino_in_the_batch() {
            let meta = Meta::open_in_memory().unwrap();
            let op = MutateOp::AtimeBatch {
                entries: vec![(3, 1, 1), (1, 2, 2), (3, 3, 3)],
            };
            assert_eq!(conflict_keys(&op, &meta), vec![1, 3]);
        }
    }

    /// In-process, no-harness correctness check for the requester-side
    /// ordering gate (plan 29 M5): a two-replica setup exercising the
    /// same `conflict_keys` -> `KeyGate::acquire` -> `holder_execute` ->
    /// `apply_accepted` pipeline `node_runtime.rs`'s spawned `Forward`
    /// task runs, under a fake network with two independently-adversarial
    /// legs (see `forward_via_gate`), asserting the requester's replica
    /// converges with the holder's despite that.
    ///
    /// `without_the_gate_a_reorder_is_observable` (below) demonstrates,
    /// with the exact same helper minus the `gate.acquire` call, that
    /// this is not a vacuous check: the same workload measurably
    /// diverges when nothing serializes overlapping forwards.
    mod ordering_gate_pipeline_tests {
        use super::*;
        use constellation_fs_core::types::ROOT_INO;
        use constellation_meta::MetaStore;

        /// Exactly what the spawned `SyncRequest::Forward` task in
        /// `node_runtime.rs` does per op, minus the real network hop.
        /// The fake hop has two independently-tunable legs -- `pre`
        /// before the holder executes (models request transit) and
        /// `post` after, before this task applies locally (models reply
        /// transit) -- because a single delay before everything (as a
        /// first draft of this test used) cannot desynchronize "the
        /// order the holder executed ops in" from "the order this task
        /// applies them in": with no `.await` between `holder_execute`
        /// and `apply_accepted`, a single-delay model has every task
        /// execute-then-apply as one atomic step relative to the others,
        /// which is ordering-safe by construction even with no gate at
        /// all, and would make this test pass for the wrong reason. A
        /// `post` delay that inverts relative to `pre` (later-executing
        /// ops reply back fastest) is what actually reproduces plan 29
        /// M5's hazard: two requester tasks racing their *own*
        /// holder-execute-vs-local-apply pair against each other, one
        /// egregiously reordered.
        async fn forward_via_gate(
            gate: Option<&Arc<KeyGate>>,
            holder: &Meta,
            requester: &Meta,
            op: MutateOp,
            pre: Duration,
            post: Duration,
        ) {
            let keys = conflict_keys(&op, requester);
            let _guard = match gate {
                Some(gate) => Some(gate.acquire(keys).await),
                None => None,
            };
            tokio::time::sleep(pre).await;
            let op_bytes = op.to_postcard().unwrap();
            // Every call is logically a distinct op (this helper is
            // invoked once per op throughout this module's tests, never
            // as a retry of a prior call), so each needs its own rid —
            // a shared/fixed one would make `holder_execute`'s plan 30
            // §M2 dedup wrongly treat the second op as a replay of the
            // first and answer from `recent` instead of executing it.
            static NEXT_TEST_SEQ: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            let seq = NEXT_TEST_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let outcome =
                holder_execute(holder, Some(1), false, 1, &op_bytes, 1, test_rid(seq), None);
            tokio::time::sleep(post).await;
            match outcome {
                MutateOutcome::Accepted { epoch, records } => {
                    apply_accepted(requester, epoch, test_rid(seq), &op, &records).unwrap();
                }
                other => panic!("expected Accepted, got {other:?}"),
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_creates_same_and_disjoint_dirs_converge() {
            let holder = Arc::new(Meta::open_in_memory().unwrap());
            let requester = Arc::new(Meta::open_in_memory().unwrap());
            let gate = KeyGate::new();

            // Three shared directories, created through the same
            // pipeline (sequentially) so both replicas start identical.
            let mut dirs = Vec::new();
            for i in 0..3u64 {
                let ino = 1000 + i;
                let op = MutateOp::Mkdir {
                    parent: ROOT_INO,
                    name: format!("dir{i}"),
                    ino,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                };
                forward_via_gate(
                    Some(&gate),
                    &holder,
                    &requester,
                    op,
                    Duration::ZERO,
                    Duration::ZERO,
                )
                .await;
                dirs.push(ino);
            }

            let total = 60u64;
            let mut tasks = Vec::new();
            for i in 0..total {
                let dir = dirs[(i % dirs.len() as u64) as usize];
                let ino = 2000 + i;
                let op = MutateOp::Create {
                    parent: dir,
                    name: format!("f{i}"),
                    ino,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                };
                let (gate, holder, requester) = (gate.clone(), holder.clone(), requester.clone());
                // Adversarial: request transit grows with `i`, reply
                // transit shrinks with `i`, so send order, holder
                // execution order, and reply-arrival order are all
                // different permutations of 0..total.
                let pre = Duration::from_micros(i * 100);
                let post = Duration::from_micros((total - i) * 100);
                tasks.push(tokio::spawn(async move {
                    forward_via_gate(Some(&gate), &holder, &requester, op, pre, post).await;
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }

            assert_eq!(
                holder.dump_replicated().unwrap(),
                requester.dump_replicated().unwrap()
            );
        }

        /// Ordering-sensitive case: many concurrent `Setattr`s on *one*
        /// inode from this requester. Whichever order the holder
        /// actually executes them in decides the final mtime; the
        /// requester must apply the same records in the same relative
        /// order to end up with the same value, even though each op's
        /// fake request/reply transit times are independently reversed.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_overlapping_setattrs_preserve_holder_order() {
            let holder = Arc::new(Meta::open_in_memory().unwrap());
            let requester = Arc::new(Meta::open_in_memory().unwrap());
            let gate = KeyGate::new();

            let f = holder.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
            // Mirror the create onto the requester (equivalent to it
            // having been forwarded and applied first).
            requester
                .create_at(ROOT_INO, "f", f.ino, 0o644, 0, 0)
                .unwrap();

            let total = 40i64;
            let mut tasks = Vec::new();
            for i in 0..total {
                let op = MutateOp::Setattr {
                    ino: f.ino,
                    mode: None,
                    uid: None,
                    gid: None,
                    size: None,
                    atime_ns: None,
                    mtime_ns: Some(i),
                };
                let (gate, holder, requester) = (gate.clone(), holder.clone(), requester.clone());
                let pre = Duration::from_micros(i as u64 * 150);
                let post = Duration::from_micros((total - i) as u64 * 150);
                tasks.push(tokio::spawn(async move {
                    forward_via_gate(Some(&gate), &holder, &requester, op, pre, post).await;
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }

            let holder_attr = holder.getattr(f.ino).unwrap().unwrap();
            let requester_attr = requester.getattr(f.ino).unwrap().unwrap();
            assert_eq!(
                holder_attr.mtime_ns, requester_attr.mtime_ns,
                "requester must see the same final mtime the holder computed"
            );
            assert_eq!(
                holder.dump_replicated().unwrap(),
                requester.dump_replicated().unwrap()
            );
        }

        /// Negative control for the two tests above: same workload, same
        /// adversarial transit times, but `gate: None`. This must
        /// observe the requester's final mtime disagree with the
        /// holder's at least sometimes, proving the positive tests are
        /// not passing merely because nothing in this workload can ever
        /// reorder -- i.e. that `KeyGate` is load-bearing for them.
        /// Retries a few seeds because the exact reorder this reproduces
        /// depends on real scheduler timing, not just the programmed
        /// delays (same reason plan 29 M4's own probe needed a live
        /// trace rather than a static argument).
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn without_the_gate_a_reorder_is_observable() {
            let total = 40i64;
            for attempt in 0..20 {
                let holder = Arc::new(Meta::open_in_memory().unwrap());
                let requester = Arc::new(Meta::open_in_memory().unwrap());
                let f = holder.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
                requester
                    .create_at(ROOT_INO, "f", f.ino, 0o644, 0, 0)
                    .unwrap();

                let mut tasks = Vec::new();
                for i in 0..total {
                    let op = MutateOp::Setattr {
                        ino: f.ino,
                        mode: None,
                        uid: None,
                        gid: None,
                        size: None,
                        atime_ns: None,
                        mtime_ns: Some(i + attempt * 1000),
                    };
                    let (holder, requester) = (holder.clone(), requester.clone());
                    let pre = Duration::from_micros(i as u64 * 150);
                    let post = Duration::from_micros((total - i) as u64 * 150);
                    tasks.push(tokio::spawn(async move {
                        forward_via_gate(None, &holder, &requester, op, pre, post).await;
                    }));
                }
                for t in tasks {
                    t.await.unwrap();
                }

                let holder_attr = holder.getattr(f.ino).unwrap().unwrap();
                let requester_attr = requester.getattr(f.ino).unwrap().unwrap();
                if holder_attr.mtime_ns != requester_attr.mtime_ns {
                    return; // reproduced the hazard; the control holds.
                }
            }
            panic!(
                "expected at least one of 20 ungated attempts to reorder; \
                 the workload may no longer be adversarial enough to prove \
                 the gate is load-bearing"
            );
        }
    }
}
