//! Plan 30 §M3a/§M3b: replaying stranded ops by rid, the takeover gate,
//! and a deposed holder's recovery.
//!
//! When a later epoch strands speculation — a requester's shadows and
//! hints, or (§M3b) a deposed holder's own unshipped journal — the
//! speculation log rolls the effects back and queues each stranded op,
//! with its original rid, in `pending_replay` (`Meta::apply_segment` while
//! tailing, `Meta::strand_below_epoch` at a takeover or deposition). This
//! module turns that queue back into log records, exactly once:
//!
//! - **Takeover gate** ([`takeover_gate`]). A node that wins a lease CAS
//!   first ships an empty epoch-marker segment (a takeover only —
//!   `shipper::complete_gate`), then rolls back everything the new epoch
//!   strands, then executes every queued op locally, in order, before its
//!   view opens. A new holder therefore never validates an op against
//!   phantom state (bug B's second shape), and the ops its own clients
//!   were already told succeeded land before anything that arrives after
//!   the takeover. The continuation-epoch path runs the same gate
//!   ([`adopt_epoch_hold_gated`]).
//! - **Deposition** ([`recover_deposed`], §M3b). A holder that learns it
//!   was deposed — a renewal finds another holder, or a segment from a
//!   later epoch arrives (the new holder's marker, at the latest) — rolls
//!   its unshipped journal back and queues its ops. This replaces
//!   reintegration's classify-against-a-side-replica pass: every
//!   transaction is re-executed through the current holder by rid, so
//!   only a genuine overlap (the log did something the op now conflicts
//!   with) becomes a conflict copy.
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
//! (`forward::holder_execute`) rather than executing it again. A stranded
//! transaction's own `completed` row is deleted with it (it never took
//! effect in the log), so neither check can claim it did.
//!
//! **Refusals.** A replay the current state no longer admits (the name
//! now exists, the target is gone, a manifest's base is stale) is a
//! genuine conflict between the stranded op and what the log did instead.
//! It is materialized as a
//! `<parent>/.constellation-conflict/<name>@<node>-<ts>` copy (empty file
//! or directory, carrying the op's manifest when it had one), plus a
//! status counter. An `unlink`/`rmdir` refused with `ENOENT` is not a
//! conflict — the name is gone, which is what the op wanted. A size-only
//! `setattr` (the FUSE `O_TRUNC` path) queued ahead of a manifest commit
//! for the same inode is folded into it, as reintegration did: the commit
//! carries the final size, and replaying the truncate alone would cut
//! whatever the log put there instead when the commit is then refused.
//! The refusal (reason and timestamp) is persisted before the copy is
//! made, so an interrupted materialization resumes with the same name.

use crate::forward::{self, ForwardState};
use crate::fusefs::SyncRequest;
use crate::shipper::SpoolInfo;
use constellation_fs_core::types::ROOT_INO;
use constellation_fs_core::Ino;
use constellation_meta::reintegrate::{conflict_dentry_name, CONFLICT_DIR};
use constellation_meta::{
    execute_mutate, Meta, MetaStore, MutateOp, MutateOutcome, Refusal, Rid, StrandedOp,
};
use std::collections::HashMap;
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

/// The takeover gate (plan 30 §M3), run by `shipper::complete_gate` once
/// this node's lease CAS won at `epoch` (and, for a takeover, its epoch
/// marker is durable), before its view opens.
///
/// For a takeover (the lease moved here from another node), roll back
/// every speculative entry accepted below `epoch` — this node's own
/// shadows and hints, and any of its own transactions left from an older
/// tenure: the tail to head that preceded the CAS retired everything the
/// previous holders shipped, so what is left can never reach the log.
/// Then execute every queued stranded op locally, in order — for a
/// takeover, and for any other acquisition that finds the queue non-empty.
///
/// Idempotent, so a failed gate is simply run again: an error leaves
/// whatever was not resolved queued, and the caller keeps the view closed
/// (`shipper::complete_gate`'s doc says why).
pub(crate) fn takeover_gate(
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    node_id: u64,
    epoch: u64,
    takeover: bool,
) -> anyhow::Result<()> {
    if takeover {
        let stranded = meta.strand_below_epoch(epoch)?;
        if stranded.any() {
            tracing::warn!(
                epoch,
                shadows = stranded.shadows,
                hints = stranded.hints,
                local = stranded.locals,
                "takeover stranded speculative state; rolled back, replaying stranded ops"
            );
            let mut spool = spool.lock().unwrap();
            spool.speculation_rolled_back += (stranded.shadows + stranded.hints) as u64;
            spool.local_rolled_back += stranded.locals as u64;
        }
    }
    let queued = meta.pending_replays()?;
    for op in &queued {
        if folded_into_later_manifest(op, &queued) {
            meta.forget_replay(op.queue_seq)?;
            spool.lock().unwrap().stranded_replayed += 1;
            continue;
        }
        replay_locally(meta, spool, node_id, op)?;
    }
    Ok(())
}

/// Plan 30 §M3b: the continuation-epoch counterpart of the S3 path's
/// takeover gate. Taking continuation authority over P2P changes no epoch
/// (the previous authority keeps its journal and ships it later), so
/// nothing is stranded; the queued replays still run locally before the
/// view opens. A failure leaves the gate pending, and the next round's
/// re-adoption retries it.
pub(crate) fn adopt_epoch_hold_gated(
    keeper: &mut crate::lease::LeaseKeeper,
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    node_id: u64,
    epoch: u64,
) {
    keeper.adopt_epoch_hold_gated(epoch, |epoch| {
        match takeover_gate(meta, spool, node_id, epoch, false) {
            Ok(()) => true,
            Err(error) => {
                tracing::error!(%error, epoch, "continuation-epoch gate failed; retrying next round");
                false
            }
        }
    });
}

/// Plan 30 §M3b: this node was deposed (`keeper` is lost). Tail to head —
/// so this node's own segments that landed without an ack are recognized
/// as shipped, and the new holder's segments strand what they strand —
/// then strand whatever of its own unshipped work is left below
/// `keeper.lost_floor()` and clear the deposition. From here the node is
/// an ordinary non-holder: the drain replays the queued ops by rid
/// through the new holder, which executes each exactly once.
///
/// With holder capture off (the performance-gate fallback) there are no
/// before-images to roll back with, so the namespace is rebuilt from the
/// shared log instead (a side replica bootstrapped from the head commit
/// and the log after it, swapped in by `Meta::replace_ns_from_rebuilt`),
/// with the journal's ops queued the same way.
pub(crate) async fn recover_deposed(
    ship: &mut crate::shipper::Shipper,
    keeper: &mut crate::lease::LeaseKeeper,
    state_dir: Option<&std::path::Path>,
) -> anyhow::Result<String> {
    use anyhow::Context;
    ship.tail_to_head()
        .await
        .context("tailing to head before a deposition recovery")?;
    let meta = ship.meta().clone();
    let spool = ship.spool.clone();
    let floor = keeper.lost_floor().max(1);
    let mut rolled_back = 0usize;
    let mut how = "rolled back from before-images";
    if meta.holder_capture() {
        rolled_back += meta.strand_below_epoch(floor)?.locals;
    }
    // Whatever is still journaled was never captured (capture off, or a
    // transaction from before it was switched on): no before-images to
    // undo it with, so rebuild from the log and queue it all.
    let uncaptured = meta.journal_len()? as usize;
    if uncaptured > 0 {
        let dir = state_dir.context("rebuilding a deposed replica needs a state directory")?;
        let view_path = dir.join(".deposition-rebuild.db");
        let _ = std::fs::remove_dir_all(&view_path);
        crate::shipper::bootstrap(&view_path, ship.log())
            .await
            .context("bootstrapping the shared log for a deposition rebuild")?;
        let side = Meta::open(&view_path)?;
        meta.replace_ns_from_rebuilt(&side)?;
        drop(side);
        let _ = std::fs::remove_dir_all(&view_path);
        rolled_back += uncaptured;
        how = "rolled back (rebuilt from the shared log where uncaptured)";
    }
    keeper.clear_lost();
    meta.kv_set("lease_lost", "0")?;
    {
        let mut spool = spool.lock().unwrap();
        spool.depositions += 1;
        spool.local_rolled_back += rolled_back as u64;
    }
    let queued = meta.pending_replays()?.len();
    let summary = format!(
        "deposition recovered: {rolled_back} unshipped transaction(s) {how}; \
         {queued} op(s) queued for replay by rid"
    );
    tracing::warn!(floor, rolled_back, queued, "{summary}");
    Ok(summary)
}

/// Whether `op` is a size-only `setattr` (the FUSE truncate path) with a
/// manifest commit for the same inode queued after it: the commit carries
/// the final size, so the truncate is folded into it (see the module
/// doc's "Refusals").
fn folded_into_later_manifest(op: &StrandedOp, queue: &[StrandedOp]) -> bool {
    let MutateOp::Setattr {
        ino,
        mode: None,
        uid: None,
        gid: None,
        size: Some(_),
        atime_ns: None,
        mtime_ns: None,
    } = &op.op
    else {
        return false;
    };
    op.refused.is_none()
        && queue.iter().any(|later| {
            later.queue_seq > op.queue_seq
                && matches!(&later.op, MutateOp::SetManifest { ino: m, .. } if m == ino)
        })
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
    // Plan 30 §M13: refused through a holder's inbox before this node
    // took over — an outcome, not a re-evaluation.
    if let Some(errno) = meta.refused_errno(queued.rid)? {
        if !refusal_is_satisfied(&queued.op, errno) {
            let refusal = refusal_now(format!("refused with errno {errno} (inbox)"));
            meta.mark_replay_refused(queued.queue_seq, refusal.clone())?;
            spool.lock().unwrap().replay_conflicts += 1;
            materialize_local(meta, node_id, &queued.op, &refusal)?;
        } else {
            spool.lock().unwrap().stranded_replayed += 1;
        }
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

/// First and largest pause between attempts at a refused replay's
/// conflict copy that could not be made yet (plan 30 §M4, round 2).
const COPY_BACKOFF_MIN: Duration = Duration::from_millis(250);
const COPY_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Retry bookkeeping for one refused replay whose conflict copy has not
/// been materialized yet (see [`DrainState`]).
#[derive(Clone, Copy, Debug)]
struct CopyRetry {
    attempts: u32,
    /// When the first attempt failed: how long the copy has been stalled.
    since: tokio::time::Instant,
    /// Not before this.
    next_at: tokio::time::Instant,
}

/// The drain task's persistent state between ticks.
///
/// Plan 30 §M4 (round 2): a refused replay's conflict copy is made
/// through the forward path (`materialize_remote`), and M3a's rule is that
/// it never holds up the rest of the queue — the refused op itself will
/// never take effect, only its copy lags. Before this, a copy that could
/// not be made was simply retried on every 250 ms tick, forever, with a
/// warning each time, and — because it counted as "resolved as far as
/// ordering goes" — it never fed the stuck-head logic that asks for the
/// lease. That is what the M4 tester saw after a sustained S3 cut: the
/// node still named in the lease object (its own view closed, so its own
/// forwards answer `Busy`) never re-acquired it, and the copy never
/// landed. Now each copy backs off (250 ms doubling to 10 s), asks for the
/// lease itself once it has been stalled for [`LEASE_FALLBACK`] (at most
/// once per that interval), and shows up in `status` as stalled; it is
/// never dropped, since it may be the only surviving copy of the data.
#[derive(Default)]
pub struct DrainState {
    /// When the head of the queue last stopped making progress, if it is
    /// stuck.
    stuck_since: Option<Instant>,
    /// Refused replays whose conflict copy failed at least once, by queue
    /// seq.
    copies: HashMap<u64, CopyRetry>,
    /// When a stalled copy last asked for the lease.
    copy_acquired_at: Option<tokio::time::Instant>,
}

impl DrainState {
    /// `(pending, stalled)`: copies that failed at least once, and those
    /// failing for at least [`LEASE_FALLBACK`].
    fn copy_counts(&self) -> (u64, u64) {
        let stalled = self
            .copies
            .values()
            .filter(|c| c.since.elapsed() >= LEASE_FALLBACK)
            .count();
        (self.copies.len() as u64, stalled as u64)
    }
}

fn copy_backoff(attempts: u32) -> Duration {
    COPY_BACKOFF_MIN
        .saturating_mul(1u32 << attempts.saturating_sub(1).min(16))
        .min(COPY_BACKOFF_MAX)
}

/// One drain pass: replay queued stranded ops, oldest first, through the
/// ordinary forward path, stopping at the first one that cannot be
/// resolved yet (order matters: a later op may depend on an earlier one).
///
/// `held_epoch` is the epoch this node holds `part` at, if it does. A
/// holder never tails a later-epoch segment, so speculation from an older
/// epoch left on it would otherwise never strand; the pass strands it here
/// first, and then replays it like any other. Plan 30 §M3b made the main
/// source of that — a forward's reply racing this node's own takeover —
/// impossible (`Meta::install_shadow` queues it instead), so this is a
/// backstop.
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
                        local = stranded.locals,
                        "holder had speculation from an older epoch; rolled back for replay"
                    );
                    let mut spool = spool.lock().unwrap();
                    spool.speculation_rolled_back += (stranded.shadows + stranded.hints) as u64;
                    spool.local_rolled_back += stranded.locals as u64;
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
    // Copies resolved by another path (the takeover gate) are forgotten.
    state
        .copies
        .retain(|seq, _| queued.iter().any(|q| q.queue_seq == *seq));
    {
        let (pending, stalled) = state.copy_counts();
        let mut spool = spool.lock().unwrap();
        spool.replay_copies_pending = pending;
        spool.replay_copies_stalled = stalled;
    }
    if queued.is_empty() {
        state.stuck_since = None;
        return;
    }
    for op in &queued {
        if folded_into_later_manifest(op, &queued) {
            if let Err(error) = meta.forget_replay(op.queue_seq) {
                tracing::warn!(%error, "dropping a folded truncate from the replay queue failed");
                return;
            }
            spool.lock().unwrap().stranded_replayed += 1;
            continue;
        }
        match drain_one(meta, spool, sync_tx, forward, node_id, part, op, state).await {
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
/// forgotten) — or, for a refused op, once it no longer holds up the
/// queue — `Ok(false)` when no holder could take it yet.
#[allow(clippy::too_many_arguments)]
async fn drain_one(
    meta: &Meta,
    spool: &Mutex<SpoolInfo>,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    part: &str,
    queued: &StrandedOp,
    state: &mut DrainState,
) -> anyhow::Result<bool> {
    if let Some(refusal) = &queued.refused {
        // The op itself will never take effect, so later ops need not
        // wait for its conflict copy: it stays queued until the copy is
        // made, without holding up the rest of the queue.
        materialize_copy(
            meta, sync_tx, forward, node_id, part, queued, refusal, state,
        )
        .await?;
        return Ok(true);
    }
    if meta.completed_position(queued.rid)?.is_some() {
        meta.forget_replay(queued.queue_seq)?;
        return Ok(true);
    }
    // Plan 30 §M13: a replay the holder refused through its inbox is an
    // outcome in the log, not something to submit again; it ends like a
    // refused P2P replay would.
    if let Some(errno) = meta.refused_errno(queued.rid)? {
        if refusal_is_satisfied(&queued.op, errno) {
            spool.lock().unwrap().stranded_replayed += 1;
            meta.forget_replay(queued.queue_seq)?;
            return Ok(true);
        }
        let refusal = refusal_now(format!("refused with errno {errno} (inbox)"));
        meta.mark_replay_refused(queued.queue_seq, refusal.clone())?;
        tracing::error!(
            rid = ?queued.rid,
            op = ?queued.op,
            reason = %refusal.reason,
            "stranded op replay refused through the inbox; materializing a conflict copy"
        );
        spool.lock().unwrap().replay_conflicts += 1;
        if materialize_remote(meta, sync_tx, forward, node_id, part, &queued.op, &refusal).await? {
            meta.forget_replay(queued.queue_seq)?;
        }
        return Ok(true);
    }
    // Plan 30 §M3b: a deposed holder's own manifest commit names chunks
    // it may not have uploaded yet (it was still holding, so its round
    // would have uploaded them before shipping). A forwarded manifest must
    // never name a hash S3 does not have (`fusefs::commit_manifest_forwarded`
    // uploads first for the same reason), so drain the inode's pending
    // chunks before the replay leaves this node.
    if let MutateOp::SetManifest { ino, .. } | MutateOp::Publish { ino, .. } = &queued.op {
        if !drain_inode(sync_tx, *ino).await {
            return Ok(false);
        }
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
    materialize_copy(
        meta, sync_tx, forward, node_id, part, queued, &refusal, state,
    )
    .await?;
    // Resolved as far as ordering goes either way (see above).
    Ok(true)
}

/// One attempt, subject to backoff, at `queued`'s conflict copy (see
/// [`DrainState`]): forget the replay once the copy exists; otherwise
/// back off, and ask for the lease once the copy has been stalled for
/// [`LEASE_FALLBACK`]. Never blocks the queue, never drops the copy.
#[allow(clippy::too_many_arguments)]
async fn materialize_copy(
    meta: &Meta,
    sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
    forward: &ForwardState,
    node_id: u64,
    part: &str,
    queued: &StrandedOp,
    refusal: &Refusal,
    state: &mut DrainState,
) -> anyhow::Result<()> {
    let now = tokio::time::Instant::now();
    if state
        .copies
        .get(&queued.queue_seq)
        .is_some_and(|retry| now < retry.next_at)
    {
        return Ok(());
    }
    let made = match materialize_remote(meta, sync_tx, forward, node_id, part, &queued.op, refusal)
        .await
    {
        Ok(made) => made,
        Err(error) => {
            tracing::debug!(%error, rid = ?queued.rid, "conflict copy attempt failed");
            false
        }
    };
    if made {
        meta.forget_replay(queued.queue_seq)?;
        state.copies.remove(&queued.queue_seq);
        return Ok(());
    }
    let retry = state.copies.entry(queued.queue_seq).or_insert(CopyRetry {
        attempts: 0,
        since: now,
        next_at: now,
    });
    retry.attempts += 1;
    retry.next_at = now + copy_backoff(retry.attempts);
    let (attempts, stalled_for) = (retry.attempts, now.duration_since(retry.since));
    if attempts == 1 || attempts.is_multiple_of(10) {
        tracing::warn!(
            rid = ?queued.rid,
            attempts,
            stalled_s = stalled_for.as_secs(),
            "a refused replay's conflict copy could not be made yet; backing off"
        );
    }
    // No holder took the steps: often this node is still named in the
    // lease object with its own view closed (after an S3 outage), so its
    // forwards answer `Busy` and nothing else would re-acquire. Ask for the
    // lease, as a stuck queue head does.
    let asked_recently = state
        .copy_acquired_at
        .is_some_and(|at| now.duration_since(at) < LEASE_FALLBACK);
    if stalled_for >= LEASE_FALLBACK && !asked_recently {
        tracing::warn!(
            rid = ?queued.rid,
            stalled_s = stalled_for.as_secs(),
            "a conflict copy has been stalled; acquiring the lease to make it locally"
        );
        let (reply, _) = tokio::sync::oneshot::channel();
        let _ = sync_tx.send(SyncRequest::Acquire {
            part: part.to_string(),
            reply,
        });
        state.copy_acquired_at = Some(now);
    }
    Ok(())
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

/// Upload `ino`'s pending chunks through the sync task
/// (`SyncRequest::DrainInode`). `false` when that failed or the task is
/// gone; the caller retries the replay later.
async fn drain_inode(sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>, ino: Ino) -> bool {
    let (reply, rx) = tokio::sync::oneshot::channel();
    if sync_tx
        .send(SyncRequest::DrainInode { ino, reply })
        .is_err()
    {
        return false;
    }
    match rx.await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::debug!(%error, ino, "chunk drain before a manifest replay failed; retrying");
            false
        }
        Err(_) => false,
    }
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
        | MutateOp::AtimeBatch { .. }
        | MutateOp::Records { .. } => return None,
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
                // The caller logs (with backoff) when a copy keeps failing.
                tracing::debug!(?other, "conflict copy step not accepted yet");
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

        takeover_gate(&requester, &spool, 2, 2, true).unwrap();

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

        takeover_gate(&requester, &spool, 2, 3, true).unwrap();

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

    /// Plan 30 §M3b: a deposed holder's `O_TRUNC` + write (a size-only
    /// `setattr`, then a manifest commit composed on the old manifest)
    /// against a log that moved the file on: the truncate folds into the
    /// commit, the commit is refused as an edit-vs-edit overlap and
    /// materialized, and the log's version keeps its content and size.
    #[test]
    fn a_truncate_before_a_refused_manifest_commit_is_folded_not_replayed() {
        let holder = Meta::open_in_memory().unwrap();
        let spool = Mutex::new(SpoolInfo::default());
        let f = holder.create(ROOT_INO, "same", 0o644, 0, 0).unwrap().ino;
        holder.set_manifest(f, b"winner", 6).unwrap();
        holder
            .queue_replay(
                rid(1),
                &MutateOp::Setattr {
                    ino: f,
                    mode: None,
                    uid: None,
                    gid: None,
                    size: Some(0),
                    atime_ns: None,
                    mtime_ns: None,
                },
            )
            .unwrap();
        holder
            .queue_replay(
                rid(2),
                &MutateOp::SetManifest {
                    ino: f,
                    base_manifest: Some(b"baseline".to_vec()),
                    manifest: b"loser".to_vec(),
                    size: 5,
                },
            )
            .unwrap();

        takeover_gate(&holder, &spool, 2, 3, false).unwrap();

        assert!(holder.pending_replays().unwrap().is_empty());
        assert_eq!(holder.manifest(f).unwrap().as_deref(), Some(&b"winner"[..]));
        assert_eq!(holder.getattr(f).unwrap().unwrap().size, 6);
        let dir = holder
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict directory");
        let copies = holder.readdir(dir.ino).unwrap();
        assert_eq!(copies.len(), 1, "{copies:?}");
        assert!(copies[0].name.starts_with("same@2-"), "{copies:?}");
        let copy = copies[0].ino;
        assert_eq!(
            holder.manifest(copy).unwrap().as_deref(),
            Some(&b"loser"[..])
        );
        let spool = spool.lock().unwrap();
        assert_eq!(spool.replay_conflicts, 1);
    }

    /// Plan 30 §M3b: a holder's own unshipped transaction, stranded by a
    /// deposition, replays by rid on the next holder exactly like a
    /// requester's shadow does: the new holder's gate executes it once,
    /// and a second replay of the same rid is answered from `completed`.
    #[test]
    fn a_deposed_holders_transaction_replays_once_on_the_next_holder() {
        let deposed = Meta::open_in_memory().unwrap();
        let next = Meta::open_in_memory().unwrap();
        let spool = Mutex::new(SpoolInfo::default());
        let op = create_op("stranded-local", (5 << 40) | 9);
        deposed.set_holder_epoch(1);
        execute_mutate(&deposed, &op, Some(rid(4))).unwrap();
        deposed.set_holder_epoch(0);
        let stranded = deposed.strand_below_epoch(2).unwrap();
        assert_eq!(stranded.locals, 1);
        let queued = deposed.pending_replays().unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].rid, rid(4));
        assert!(deposed
            .lookup(ROOT_INO, "stranded-local")
            .unwrap()
            .is_none());

        // The next holder replays it (as the drain's forward would).
        next.queue_replay(queued[0].rid, &queued[0].op).unwrap();
        next.queue_replay(queued[0].rid, &queued[0].op).unwrap();
        takeover_gate(&next, &spool, 7, 2, true).unwrap();
        assert!(next.lookup(ROOT_INO, "stranded-local").unwrap().is_some());
        let journal = next.take_journal(usize::MAX).unwrap();
        let creates = journal
            .iter()
            .filter(
                |(_, r)| matches!(r, LogRecord::Create { name, .. } if name == "stranded-local"),
            )
            .count();
        assert_eq!(creates, 1, "exactly once: {journal:?}");
        assert_eq!(spool.lock().unwrap().stranded_replayed, 1);
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
        takeover_gate(&requester, &spool, 2, 2, true).unwrap();
        assert!(requester.pending_replays().unwrap().is_empty());
        assert!(requester.lookup(ROOT_INO, CONFLICT_DIR).unwrap().is_none());
        assert_eq!(spool.lock().unwrap().replay_conflicts, 0);
    }

    /// Plan 30 §M4 round 2: a refused replay's conflict copy on a node that
    /// cannot reach a holder (its S3 path is gone, so its own forwards
    /// answer `Busy`): the copy never holds up later replays, is retried
    /// with backoff rather than every tick, asks for the lease once it has
    /// been stalled for `LEASE_FALLBACK`, shows as stalled, is never
    /// dropped — and is made as soon as a holder takes the steps again.
    #[tokio::test]
    async fn a_stalled_conflict_copy_backs_off_and_never_blocks_the_queue() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let spool = Mutex::new(SpoolInfo::default());
        let forward = ForwardState::new(1);
        meta.queue_replay(rid(1), &create_op("lost", (5 << 40) | 1))
            .unwrap();
        meta.queue_replay(rid(2), &create_op("later", (5 << 40) | 2))
            .unwrap();
        let first = meta.pending_replays().unwrap()[0].queue_seq;
        meta.mark_replay_refused(
            first,
            Refusal {
                reason: "test".into(),
                ts_unix: 1,
            },
        )
        .unwrap();

        // The sync task of a node whose S3 path is gone: whatever needs the
        // holder answers `Busy` (the ordinary replay goes to a reachable
        // holder), until `available` flips and ops execute here.
        let (sync_tx, mut sync_rx) = tokio::sync::mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let acquires = Arc::new(AtomicUsize::new(0));
        let available = Arc::new(AtomicBool::new(false));
        {
            let (meta, attempts, acquires, available) = (
                meta.clone(),
                attempts.clone(),
                acquires.clone(),
                available.clone(),
            );
            tokio::spawn(async move {
                while let Some(req) = sync_rx.recv().await {
                    match req {
                        SyncRequest::Forward { op, reply, .. } => {
                            let later =
                                matches!(&op, MutateOp::Create { name, .. } if name == "later");
                            let outcome = if later {
                                MutateOutcome::Accepted {
                                    epoch: 1,
                                    records: Vec::new(),
                                }
                            } else if available.load(Ordering::SeqCst) {
                                let records = execute_mutate(&meta, &op, None).unwrap();
                                MutateOutcome::Accepted { epoch: 1, records }
                            } else {
                                attempts.fetch_add(1, Ordering::SeqCst);
                                MutateOutcome::Busy
                            };
                            let _ = reply.send(Ok(outcome));
                        }
                        SyncRequest::Acquire { .. } => {
                            acquires.fetch_add(1, Ordering::SeqCst);
                        }
                        _ => {}
                    }
                }
            });
        }
        async fn pass(
            meta: &Arc<Meta>,
            spool: &Mutex<SpoolInfo>,
            sync_tx: &tokio::sync::mpsc::UnboundedSender<SyncRequest>,
            forward: &ForwardState,
            state: &mut DrainState,
        ) {
            drain_pending_replays(meta, spool, sync_tx, forward, 2, "p0", None, state).await;
            // Let the fake sync task see what was sent.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut state = DrainState::default();

        // The copy fails; the later replay is not held up.
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        let left = meta.pending_replays().unwrap();
        assert_eq!(left.len(), 1, "{left:?}");
        assert_eq!(left[0].queue_seq, first);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(spool.lock().unwrap().stranded_replayed, 1);

        // Right away again: backing off, no new attempt.
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        // Due again: one more attempt, and the backoff doubles.
        let now = tokio::time::Instant::now();
        state.copies.get_mut(&first).unwrap().next_at = now;
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let wait = state.copies[&first].next_at - tokio::time::Instant::now();
        assert!(wait > Duration::from_millis(300), "{wait:?}");
        assert_eq!(acquires.load(Ordering::SeqCst), 0);

        // Stalled for LEASE_FALLBACK: it asks for the lease (once per
        // interval), and status shows it; it is still queued.
        let now = tokio::time::Instant::now();
        {
            let retry = state.copies.get_mut(&first).unwrap();
            retry.since = now
                .checked_sub(LEASE_FALLBACK + Duration::from_secs(1))
                .unwrap();
            retry.next_at = now;
        }
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        assert_eq!(acquires.load(Ordering::SeqCst), 1);
        state.copies.get_mut(&first).unwrap().next_at = tokio::time::Instant::now();
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        assert_eq!(
            acquires.load(Ordering::SeqCst),
            1,
            "at most once per interval"
        );
        {
            let spool = spool.lock().unwrap();
            assert_eq!(
                (spool.replay_copies_pending, spool.replay_copies_stalled),
                (1, 1)
            );
        }
        assert_eq!(meta.pending_replays().unwrap().len(), 1, "never dropped");

        // A holder takes the steps again: the copy is made and forgotten.
        available.store(true, Ordering::SeqCst);
        state.copies.get_mut(&first).unwrap().next_at = tokio::time::Instant::now();
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        assert!(meta.pending_replays().unwrap().is_empty());
        let dir = meta
            .lookup(ROOT_INO, CONFLICT_DIR)
            .unwrap()
            .expect("conflict directory");
        let copies = meta.readdir(dir.ino).unwrap();
        assert!(
            copies.iter().any(|c| c.name.starts_with("lost@2-")),
            "{copies:?}"
        );
        pass(&meta, &spool, &sync_tx, &forward, &mut state).await;
        let spool = spool.lock().unwrap();
        assert_eq!(
            (spool.replay_copies_pending, spool.replay_copies_stalled),
            (0, 0)
        );
    }
}
