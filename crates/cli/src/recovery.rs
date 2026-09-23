//! Plan 30 §M3a: replaying stranded ops by rid, and the takeover gate.
//!
//! When a later epoch strands a requester's speculation
//! (`Meta::apply_segment` while tailing, or `Meta::strand_below_epoch` at
//! a takeover), the speculation log rolls the effects back and queues
//! each stranded shadow's op, with its original rid, in `pending_replay`.
//! This module turns that queue back into log records, exactly once:
//!
//! - **Takeover gate** ([`takeover_gate`]). A node that wins a lease CAS
//!   first rolls back everything the new epoch strands, then executes
//!   every queued op locally, in order, before its view opens
//!   (`LeaseKeeper::commit_gated`). A new holder therefore never validates
//!   an op against phantom state (bug B's second shape), and the ops its
//!   own clients were already told succeeded land before anything that
//!   arrives after the takeover.
//! - **Drain** ([`drain_pending_replays`], a periodic task). A node that
//!   does not hold the lease sends each queued op, oldest first and one at
//!   a time, down the ordinary forward path (`SyncRequest::Forward`, the
//!   same one `mutate_op_rebasable` uses), so it reaches the current holder
//!   — or executes locally if this node holds after all — and an accepted
//!   reply becomes an ordinary shadow again, retiring when its `Completed`
//!   arrives from the log. After [`LEASE_FALLBACK`] without progress (no
//!   reachable holder: P2P down, or nobody has taken over yet) it asks for
//!   the lease itself, which runs the takeover gate.
//!
//! Exactly-once comes from M2: every path checks `completed` first, and
//! a holder answers a rid it already executed from `recent` or `completed`
//! (`forward::holder_execute`) rather than executing it again.
//!
//! **Refusals.** A replay the current state no longer admits (the name
//! now exists, the target is gone, a manifest's base is stale) is a
//! genuine conflict between the stranded op and what the log did instead.
//! It is materialized the way reintegration materializes a stranded
//! journal record: a `<parent>/.constellation-conflict/<name>@<node>-<ts>`
//! copy (empty file or directory, carrying the op's manifest when it had
//! one), plus a status counter. An `unlink`/`rmdir` refused with `ENOENT`
//! is not a conflict — the name is gone, which is what the op wanted
//! (reintegration's "gone already is clean"). The refusal (reason and
//! timestamp) is persisted before the copy is made, so an interrupted
//! materialization resumes with the same name.

use crate::forward::{self, ForwardState};
use crate::fusefs::SyncRequest;
use crate::shipper::SpoolInfo;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::reintegrate::{conflict_dentry_name, CONFLICT_DIR};
use constellation_meta::{
    execute_mutate, Meta, MetaStore, MutateOp, MutateOutcome, Refusal, Rid, StrandedOp,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How often the drain task looks at the replay queue. The queue is empty
/// in normal operation, so an idle tick is one fjall read.
pub const DRAIN_INTERVAL: Duration = Duration::from_millis(250);

/// How long the head of the replay queue may go without a holder
/// answering before this node asks for the lease itself (the takeover
/// gate then replays locally). Long enough that a live holder behind a
/// slow link or a lease in the middle of changing hands is waited for;
/// short enough that a node whose P2P path is down does not sit on
/// acknowledged ops for long.
pub const LEASE_FALLBACK: Duration = Duration::from_secs(10);

/// Plan 30 §M3a's takeover gate, run by `LeaseKeeper::commit_gated` right
/// after this node's lease CAS won at `epoch` and before its view opens.
///
/// For a takeover (the lease moved here from another node), roll back
/// every speculative entry accepted below `epoch`: the tail to head that
/// preceded the CAS retired everything the previous holders shipped, so
/// what is left can never reach the log. Then execute every queued
/// stranded op locally, in order — for a takeover, and for any other
/// acquisition that finds the queue non-empty.
///
/// Best effort by construction: it runs inside the commit, after the CAS,
/// where there is nothing left to abort. A failure is logged and the
/// queue keeps whatever was not resolved, for the drain to retry.
pub(crate) fn takeover_gate(
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    node_id: u64,
    epoch: u64,
    takeover: bool,
) {
    if takeover {
        match meta.strand_below_epoch(epoch) {
            Ok(stranded) if stranded.any() => {
                tracing::warn!(
                    epoch,
                    shadows = stranded.shadows,
                    hints = stranded.hints,
                    "takeover stranded speculative state; rolled back, replaying stranded ops"
                );
                spool.lock().unwrap().speculation_rolled_back +=
                    (stranded.shadows + stranded.hints) as u64;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(%error, epoch, "takeover gate: rolling back stranded speculation failed");
                return;
            }
        }
    }
    let queued = match meta.pending_replays() {
        Ok(queued) => queued,
        Err(error) => {
            tracing::error!(%error, "takeover gate: reading the replay queue failed");
            return;
        }
    };
    for op in queued {
        if let Err(error) = replay_locally(meta, spool, node_id, &op) {
            tracing::error!(%error, rid = ?op.rid, "takeover gate: local replay failed");
            return;
        }
    }
}

/// Execute one queued op on this node, as the holder: skip it if its rid
/// already took effect, execute it with the same rid otherwise, and
/// materialize a refusal as a conflict copy.
fn replay_locally(
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    node_id: u64,
    queued: &StrandedOp,
) -> anyhow::Result<()> {
    if let Some(refusal) = &queued.refused {
        materialize_local(meta, node_id, &queued.op, refusal)?;
        meta.forget_replay(queued.queue_seq)?;
        return Ok(());
    }
    if meta.completed_position(queued.rid)?.is_some() {
        meta.forget_replay(queued.queue_seq)?;
        return Ok(());
    }
    match execute_mutate(meta, &queued.op, Some(queued.rid)) {
        Ok(_) => {
            tracing::info!(rid = ?queued.rid, "stranded op replayed locally");
            spool.lock().unwrap().stranded_replayed += 1;
        }
        Err(error) => {
            let errno = forward::meta_errno(&error);
            if refusal_is_satisfied(&queued.op, errno) {
                spool.lock().unwrap().stranded_replayed += 1;
            } else {
                let refusal = refusal_now(format!("{error}"));
                meta.mark_replay_refused(queued.queue_seq, refusal.clone())?;
                tracing::error!(
                    rid = ?queued.rid,
                    op = ?queued.op,
                    reason = %refusal.reason,
                    "stranded op replay refused; materializing a conflict copy"
                );
                spool.lock().unwrap().replay_conflicts += 1;
                materialize_local(meta, node_id, &queued.op, &refusal)?;
            }
        }
    }
    meta.forget_replay(queued.queue_seq)?;
    Ok(())
}

/// The drain task's persistent state between ticks.
#[derive(Default)]
pub struct DrainState {
    /// When the head of the queue last stopped making progress, if it is
    /// stuck.
    stuck_since: Option<Instant>,
}

/// One drain pass: replay queued stranded ops, oldest first, through the
/// ordinary forward path, stopping at the first one that cannot be
/// resolved yet (order matters: a later op may depend on an earlier one).
///
/// `held_epoch` is the epoch this node holds `part` at, if it does. A
/// holder never tails a later-epoch segment, so a shadow from an older
/// epoch that was installed after its takeover gate ran (a forward's
/// reply racing the takeover) would otherwise never strand; the pass
/// strands it here first, and then replays it like any other.
#[allow(clippy::too_many_arguments)]
pub async fn drain_pending_replays(
    meta: &Arc<Meta>,
    spool: &Mutex<SpoolInfo>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    part: &str,
    held_epoch: Option<u64>,
    state: &mut DrainState,
) {
    if let Some(epoch) = held_epoch {
        if meta.has_outstanding_speculation() {
            match meta.strand_below_epoch(epoch) {
                Ok(stranded) if stranded.any() => {
                    tracing::warn!(
                        epoch,
                        shadows = stranded.shadows,
                        hints = stranded.hints,
                        "holder had speculation from an older epoch; rolled back for replay"
                    );
                    spool.lock().unwrap().speculation_rolled_back +=
                        (stranded.shadows + stranded.hints) as u64;
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(%error, "stranding older-epoch speculation failed"),
            }
        }
    }
    let queued = match meta.pending_replays() {
        Ok(queued) => queued,
        Err(error) => {
            tracing::warn!(%error, "reading the replay queue failed");
            return;
        }
    };
    if queued.is_empty() {
        state.stuck_since = None;
        return;
    }
    for op in queued {
        match drain_one(meta, spool, sync_tx, forward, node_id, part, &op).await {
            Ok(true) => state.stuck_since = None,
            Ok(false) => {
                // The cached holder may be the dead one that stranded the
                // op: make the next attempt read the lease object instead
                // (`dispatch_forward` does when the cache is empty).
                forward.clear_holder(part);
                let since = *state.stuck_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= LEASE_FALLBACK {
                    tracing::warn!(
                        rid = ?op.rid,
                        stuck_s = since.elapsed().as_secs(),
                        "no holder has answered a stranded op's replay; acquiring the lease \
                         to replay it locally"
                    );
                    let (reply, _) = tokio::sync::oneshot::channel();
                    let _ = sync_tx.send(SyncRequest::Acquire {
                        part: part.to_string(),
                        reply,
                    });
                    state.stuck_since = Some(Instant::now());
                }
                return;
            }
            Err(error) => {
                tracing::warn!(%error, rid = ?op.rid, "stranded op replay failed; retrying");
                return;
            }
        }
    }
}

/// Try to resolve one queued op. `Ok(true)` once it is resolved (and
/// forgotten), `Ok(false)` when no holder could take it yet.
async fn drain_one(
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    part: &str,
    queued: &StrandedOp,
) -> anyhow::Result<bool> {
    if let Some(refusal) = &queued.refused {
        // The op itself will never take effect, so later ops need not
        // wait for its conflict copy: it stays queued until the copy is
        // made, without holding up the rest of the queue.
        if materialize_remote(meta, sync_tx, forward, node_id, part, &queued.op, refusal).await? {
            meta.forget_replay(queued.queue_seq)?;
        }
        return Ok(true);
    }
    if meta.completed_position(queued.rid)?.is_some() {
        meta.forget_replay(queued.queue_seq)?;
        return Ok(true);
    }
    let Some(outcome) = submit(sync_tx, part, queued.op.clone(), queued.rid).await else {
        return Ok(false);
    };
    let refusal = match outcome {
        MutateOutcome::Accepted { .. } => {
            tracing::info!(rid = ?queued.rid, "stranded op replayed by rid");
            spool.lock().unwrap().stranded_replayed += 1;
            meta.forget_replay(queued.queue_seq)?;
            return Ok(true);
        }
        MutateOutcome::Busy | MutateOutcome::NotHolder { .. } => return Ok(false),
        MutateOutcome::Errno(errno) if refusal_is_satisfied(&queued.op, errno) => {
            spool.lock().unwrap().stranded_replayed += 1;
            meta.forget_replay(queued.queue_seq)?;
            return Ok(true);
        }
        MutateOutcome::Errno(errno) => format!("refused with errno {errno}"),
        MutateOutcome::Exists { .. } => "the name now exists".to_string(),
        MutateOutcome::Conflict { .. } => "stale manifest base".to_string(),
    };
    let refusal = refusal_now(refusal);
    meta.mark_replay_refused(queued.queue_seq, refusal.clone())?;
    tracing::error!(
        rid = ?queued.rid,
        op = ?queued.op,
        reason = %refusal.reason,
        "stranded op replay refused; materializing a conflict copy"
    );
    spool.lock().unwrap().replay_conflicts += 1;
    if materialize_remote(meta, sync_tx, forward, node_id, part, &queued.op, &refusal).await? {
        meta.forget_replay(queued.queue_seq)?;
    }
    // Resolved as far as ordering goes either way (see above).
    Ok(true)
}

/// Send `op` down the ordinary forward path (holder-local execution, or a
/// forward to the cached holder with M2's same-rid retries) and wait for
/// the outcome. `None` if the sync task is gone or failed the request.
async fn submit(
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    part: &str,
    op: MutateOp,
    rid: Rid,
) -> Option<MutateOutcome> {
    let (reply, rx) = tokio::sync::oneshot::channel();
    sync_tx
        .send(SyncRequest::Forward {
            part: part.to_string(),
            op,
            rid,
            reply,
        })
        .ok()?;
    rx.await.ok()?.ok()
}

/// A refusal that is not a conflict: removing a name that is already gone
/// is what the op wanted (reintegration's "gone already is clean").
fn refusal_is_satisfied(op: &MutateOp, errno: i32) -> bool {
    matches!(op, MutateOp::Unlink { .. } | MutateOp::Rmdir { .. }) && errno == libc::ENOENT
}

fn refusal_now(reason: String) -> Refusal {
    Refusal {
        reason,
        ts_unix: constellation_fs_core::types::now_ns() / 1_000_000_000,
    }
}

// --------------------------------------------------------- conflict copies

/// What a refused op's conflict copy looks like: under which directory,
/// named what, a directory or a file, and with which manifest.
struct ConflictCopy {
    parent: Ino,
    name: String,
    dir: bool,
    manifest: Option<(Vec<u8>, u64)>,
}

/// The conflict copy for `op`, mirroring `reintegrate::materialize`'s
/// choices. `None` for ops that leave nothing worth copying (a rename, an
/// xattr edit, an atime batch): those are counted and logged only.
fn conflict_copy(meta: &Meta, op: &MutateOp) -> Option<ConflictCopy> {
    let at_ino = |ino: Ino, manifest: Option<(Vec<u8>, u64)>| {
        let parent = meta.parent_of(ino).ok().flatten().unwrap_or(ROOT_INO);
        let path = meta.path_of(ino).unwrap_or_else(|_| format!("ino-{ino}"));
        let name = path.rsplit('/').next().unwrap_or("file").to_string();
        ConflictCopy {
            parent,
            name,
            dir: false,
            manifest,
        }
    };
    let copy = match op {
        MutateOp::Mkdir { parent, name, .. } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: true,
            manifest: None,
        },
        MutateOp::Create { parent, name, .. }
        | MutateOp::Symlink { parent, name, .. }
        | MutateOp::Mknod { parent, name, .. }
        | MutateOp::Link { parent, name, .. } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: None,
        },
        MutateOp::Publish {
            parent,
            name,
            manifest,
            size,
            ..
        } => ConflictCopy {
            parent: *parent,
            name: name.clone(),
            dir: false,
            manifest: Some((manifest.clone(), *size)),
        },
        MutateOp::SetManifest {
            ino,
            manifest,
            size,
            ..
        } => at_ino(*ino, Some((manifest.clone(), *size))),
        MutateOp::Setattr { ino, .. } => {
            let manifest = meta.manifest(*ino).ok().flatten().map(|m| {
                let size = meta
                    .getattr(*ino)
                    .ok()
                    .flatten()
                    .map(|a| a.size)
                    .unwrap_or(0);
                (m, size)
            });
            at_ino(*ino, manifest)
        }
        MutateOp::Unlink { .. }
        | MutateOp::Rmdir { .. }
        | MutateOp::Rename { .. }
        | MutateOp::SetXattr { .. }
        | MutateOp::RemoveXattr { .. }
        | MutateOp::AtimeBatch { .. } => return None,
    };
    let parent = if meta.getattr(copy.parent).ok().flatten().is_some() {
        copy.parent
    } else {
        ROOT_INO
    };
    Some(ConflictCopy { parent, ..copy })
}

/// The op that creates `copy` under `dir` as `dest` (its manifest, if
/// any, is a second step).
fn copy_op(meta: &Meta, copy: &ConflictCopy, dir: Ino, dest: &str) -> anyhow::Result<MutateOp> {
    let ino = meta.allocate_ino(dir)?;
    Ok(if copy.dir {
        MutateOp::Mkdir {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: 0o755,
            uid: 0,
            gid: 0,
        }
    } else {
        MutateOp::Create {
            parent: dir,
            name: dest.to_string(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    })
}

fn conflict_dir_op(meta: &Meta, parent: Ino) -> anyhow::Result<MutateOp> {
    Ok(MutateOp::Mkdir {
        parent,
        name: CONFLICT_DIR.to_string(),
        ino: meta.allocate_ino(parent)?,
        mode: 0o755,
        uid: 0,
        gid: 0,
    })
}

fn lookup_ino(meta: &Meta, parent: Ino, name: &str) -> Option<Ino> {
    meta.lookup(parent, name).ok().flatten().map(|a| a.ino)
}

/// Materialize `op`'s conflict copy as the holder: plain local
/// executions, journaled and shipped like any other write.
fn materialize_local(
    meta: &Meta,
    node_id: u64,
    op: &MutateOp,
    refusal: &Refusal,
) -> anyhow::Result<()> {
    let Some(copy) = conflict_copy(meta, op) else {
        return Ok(());
    };
    let dir = match lookup_ino(meta, copy.parent, CONFLICT_DIR) {
        Some(dir) => dir,
        None => {
            execute_mutate(meta, &conflict_dir_op(meta, copy.parent)?, None)?;
            lookup_ino(meta, copy.parent, CONFLICT_DIR)
                .ok_or_else(|| anyhow::anyhow!("conflict directory vanished"))?
        }
    };
    let dest = conflict_dentry_name(&copy.name, node_id, refusal.ts_unix);
    if lookup_ino(meta, dir, &dest).is_some() {
        return Ok(());
    }
    execute_mutate(meta, &copy_op(meta, &copy, dir, &dest)?, None)?;
    if let Some((manifest, size)) = &copy.manifest {
        let ino = lookup_ino(meta, dir, &dest)
            .ok_or_else(|| anyhow::anyhow!("conflict copy vanished"))?;
        execute_mutate(
            meta,
            &MutateOp::SetManifest {
                ino,
                base_manifest: None,
                manifest: manifest.clone(),
                size: *size,
            },
            None,
        )?;
    }
    tracing::error!(
        path = %format!("{CONFLICT_DIR}/{dest}"),
        reason = %refusal.reason,
        "stranded op materialized as a conflict copy"
    );
    Ok(())
}

/// Materialize `op`'s conflict copy through the forward path (this node
/// is not the holder). `Ok(false)` when a step could not reach a holder
/// yet; the caller retries with the same persisted [`Refusal`], so the
/// copy keeps its name.
async fn materialize_remote(
    meta: &Meta,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    part: &str,
    op: &MutateOp,
    refusal: &Refusal,
) -> anyhow::Result<bool> {
    let Some(copy) = conflict_copy(meta, op) else {
        return Ok(true);
    };
    // Each step is a system-generated op with its own rid: an entry that
    // already exists (an earlier interrupted attempt, or another node's
    // conflict directory) is as good as a fresh one.
    let step = |op: MutateOp| async move {
        match submit(sync_tx, part, op, forward.next_system_rid(node_id)).await {
            Some(MutateOutcome::Accepted { .. })
            | Some(MutateOutcome::Exists { .. })
            | Some(MutateOutcome::Errno(libc::EEXIST)) => true,
            Some(other) => {
                tracing::warn!(?other, "conflict copy step not accepted yet");
                false
            }
            None => false,
        }
    };
    if lookup_ino(meta, copy.parent, CONFLICT_DIR).is_none()
        && !step(conflict_dir_op(meta, copy.parent)?).await
    {
        return Ok(false);
    }
    let Some(dir) = lookup_ino(meta, copy.parent, CONFLICT_DIR) else {
        return Ok(false);
    };
    let dest = conflict_dentry_name(&copy.name, node_id, refusal.ts_unix);
    if lookup_ino(meta, dir, &dest).is_none() && !step(copy_op(meta, &copy, dir, &dest)?).await {
        return Ok(false);
    }
    if let Some((manifest, size)) = &copy.manifest {
        let Some(ino) = lookup_ino(meta, dir, &dest) else {
            return Ok(false);
        };
        let set = MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest: manifest.clone(),
            size: *size,
        };
        if !step(set).await {
            return Ok(false);
        }
    }
    tracing::error!(
        path = %format!("{CONFLICT_DIR}/{dest}"),
        reason = %refusal.reason,
        "stranded op materialized as a conflict copy"
    );
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_meta::LogRecord;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 2,
            incarnation: 1,
            seq,
        }
    }

    fn create_op(name: &str, ino: Ino) -> MutateOp {
        MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    fn accept(holder: &Meta, requester: &Meta, rid: Rid, op: &MutateOp, epoch: u64) {
        let records = execute_mutate(holder, op, Some(rid)).unwrap();
        forward::apply_accepted(requester, epoch, rid, op, &records).unwrap();
    }

    /// The requester-takes-over shape of bug B (M0's
    /// `holder-crash-phantom-new-holder`): the holder that accepted a
    /// forwarded create dies before shipping; the requester takes over.
    /// The takeover gate rolls the shadow back and replays the create
    /// locally with its rid, so it is journaled (it will ship) and a later
    /// create of the same name is refused against real state, not phantom
    /// state.
    #[test]
    fn the_takeover_gate_replays_a_stranded_create_locally() {
        let old_holder = Meta::open_in_memory().unwrap();
        let requester = Meta::open_in_memory().unwrap();
        let spool = Mutex::new(SpoolInfo::default());
        let op = create_op("phantom", (5 << 40) | 1);
        accept(&old_holder, &requester, rid(1), &op, 1);
        assert!(requester.has_outstanding_speculation());
        assert_eq!(requester.journal_len().unwrap(), 0);

        takeover_gate(&requester, &spool, 2, 2, true);

        assert!(!requester.has_outstanding_speculation());
        assert!(requester.pending_replays().unwrap().is_empty());
        assert!(requester.lookup(ROOT_INO, "phantom").unwrap().is_some());
        let journal: Vec<LogRecord> = requester
            .take_journal(usize::MAX)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        assert!(
            journal
                .iter()
                .any(|r| matches!(r, LogRecord::Completed { rid: r } if *r == rid(1))),
            "replayed with its original rid: {journal:?}"
        );
        let spool = spool.lock().unwrap();
        assert_eq!(spool.speculation_rolled_back, 1);
        assert_eq!(spool.stranded_replayed, 1);
        assert_eq!(spool.replay_conflicts, 0);
    }

    /// A stranded create whose name the log gave to another inode in the
    /// meantime is refused on replay and materialized as a conflict copy.
    #[test]
    fn a_refused_replay_is_materialized_as_a_conflict_copy() {
        let old_holder = Meta::open_in_memory().unwrap();
        let requester = Meta::open_in_memory().unwrap();
        let spool = Mutex::new(SpoolInfo::default());
        let op = create_op("taken", (5 << 40) | 1);
        accept(&old_holder, &requester, rid(1), &op, 1);
        // The next holder's segment gives the name to someone else (and
        // strands the shadow on the way in).
        let winner = vec![LogRecord::Create {
            parent: ROOT_INO,
            name: "taken".into(),
            ino: (6 << 40) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 5,
        }];
        let applied = requester
            .apply_segment(1, 2, &winner, &constellation_meta::TouchSet::default())
            .unwrap();
        assert_eq!(applied.stranded.shadows, 1);

        takeover_gate(&requester, &spool, 2, 3, true);

        assert!(requester.pending_replays().unwrap().is_empty());
        assert_eq!(
            requester.lookup(ROOT_INO, "taken").unwrap().unwrap().ino,
            (6 << 40) | 1,
            "the log's winner keeps the name"
        );
        let dir = requester
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict directory");
        let copies = requester.readdir(dir.ino).unwrap();
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert!(copies[0].name.starts_with("taken@2-"), "{copies:?}");
        let spool = spool.lock().unwrap();
        assert_eq!(spool.replay_conflicts, 1);
        assert_eq!(spool.stranded_replayed, 0);
    }

    /// A stranded unlink of a name that is already gone is satisfied, not
    /// a conflict.
    #[test]
    fn a_replayed_unlink_of_a_missing_name_is_not_a_conflict() {
        let requester = Meta::open_in_memory().unwrap();
        let spool = Mutex::new(SpoolInfo::default());
        let op = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "gone".into(),
        };
        requester
            .install_shadow(rid(1), 1, &op, &[LogRecord::Completed { rid: rid(1) }])
            .unwrap();
        takeover_gate(&requester, &spool, 2, 2, true);
        assert!(requester.pending_replays().unwrap().is_empty());
        assert!(requester.lookup(ROOT_INO, CONFLICT_DIR).unwrap().is_none());
        assert_eq!(spool.lock().unwrap().replay_conflicts, 0);
    }
}
