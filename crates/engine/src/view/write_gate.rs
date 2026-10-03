//! The write gate (DESIGN.md §5) and the read waits: the lease, the
//! mutation paths (fast path, delegate, the core), durability
//! acknowledgements, `cto=strict`'s read index, and `fsync`'s barrier.

use super::*;

/// A cheap, non-cryptographic random value in `[0, 1)`, fresh per call.
/// `RandomState::new()` draws its keys from the OS RNG each time it is
/// constructed, so hashing nothing still yields a value that varies
/// call to call and thread to thread — exactly what jittering a retry
/// backoff needs, without pulling in a `rand` dependency for one call
/// site. Never used where actual unpredictability (security) matters.
pub(super) fn jitter_fraction() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

/// Plan 30 §M9: how long a fast-path acknowledgement waits for
/// durability before the op is treated as in doubt (a backup that stops
/// answering is reconfigured out within `CONSTELLATION_BACKUP_ACK_TIMEOUT_MS`;
/// a lost lease ends the wait at once).
pub(super) const DURABLE_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Why a mutation did not commit. Separate from a bare errno so that an
/// optimistic-concurrency rejection can carry the state to rebase onto.
pub(crate) enum MutateFail {
    Errno(Code),
    /// The base this update was composed on is no longer current.
    /// `manifest` is the holder's image when it came back over the wire;
    /// `None` means rebase from the local replica, which is
    /// authoritative whenever this node executed the mutation itself.
    Conflict {
        manifest: Option<Vec<u8>>,
    },
}

pub(super) fn mutate_fail(e: MetaError) -> MutateFail {
    match e {
        MetaError::Conflict => MutateFail::Conflict { manifest: None },
        other => MutateFail::Errno(other.code()),
    }
}

impl View {
    /// Execute a namespace mutation locally when we hold the partition,
    /// otherwise ask the holder to validate and journal it. Busy or stale
    /// holder information falls back to the ordinary lease acquisition path.
    pub(crate) fn mutate_op(
        &self,
        part_hint_ino: Ino,
        op: constellation_meta::MutateOp,
    ) -> Result<(), Code> {
        self.mutate_op_rebasable(part_hint_ino, op)
            .map_err(|failure| match failure {
                MutateFail::Errno(e) => e,
                // Callers that cannot rebase surface the conflict as a
                // retryable error rather than losing the update.
                MutateFail::Conflict { .. } => Code::Again,
            })
    }

    /// As [`Self::mutate_op`], but reporting an optimistic-concurrency
    /// rejection as [`MutateFail::Conflict`] so a caller holding the
    /// material to recompose its update can rebase and retry.
    ///
    /// Plan 30 §M2 GC: every completion path — success, an explicit
    /// refusal, or giving up entirely — marks this op's rid "acked"
    /// exactly once before returning (never left for only the forward
    /// path to do, which is what previously stalled `acked_through`
    /// forever behind the first op that executed locally, took a
    /// designation/read-only/departed refusal, or fell back to the
    /// lease path). A rid that this call never actually sends anywhere
    /// (an early refusal, or forwarding disabled) still gets marked: the
    /// holder never learned of it, so marking it is a no-op for GC
    /// purposes, but *skipping* it would leave a permanent hole in the
    /// contiguous `acked_through` floor for every later op's seq to
    /// stall behind. The one exception is the earliest checks
    /// (`is_synthetic`, no `self.sync`), which run *before* a rid is
    /// even allocated — there is nothing to mark yet.
    /// Plan 30 §M6: every local read path's session wait (read-your-
    /// writes and monotonic reads per node; see `constellation_meta::
    /// session`). A single-node mount (no sync handle) observes nothing
    /// and skips it. Bounded by `CONSTELLATION_SESSION_WAIT_MS`; a timeout
    /// answers from the replica anyway (degraded, not an error).
    pub(crate) fn session_wait(&self, keys: &[ReadKey]) {
        if self.sync.is_none() {
            return;
        }
        constellation_vfs::watch::stage("session wait");
        let _ = self.meta.session_wait(keys);
        constellation_vfs::watch::stage("running");
    }

    /// Plan 30 §M8: the read wait of a `cto=strict` open (`dir: false`)
    /// or lookup (`dir: true`, `name`) of `ino`, reading `keys`. Bounded
    /// mode, and a mount without a sync handle, is M6's session wait.
    ///
    /// Strict: the sequencer reads its own replica (authoritative); a
    /// node holding a read delegation on `ino` reads locally once its
    /// replica has reached the grant's position (renewing it in the
    /// background past half its lifetime); anyone else asks the sequencer
    /// (`SyncRequest::ReadIndex`) and waits for the position it answers.
    /// Every path ends in the session wait, bounded as M6's is: a
    /// sequencer that does not answer degrades the read, never fails it.
    pub(crate) fn strict_read(&self, ino: Ino, dir: bool, name: Option<&str>, keys: &[ReadKey]) {
        let Some(h) = &self.sync else {
            return;
        };
        constellation_vfs::watch::stage("session wait");
        if !h.cto_strict {
            let _ = self.meta.session_wait(keys);
            constellation_vfs::watch::stage("running");
            return;
        }
        let deleg = self.meta.read_delegations();
        deleg.count(|s| s.strict_reads += 1);
        if h.lease.reads_locally() {
            deleg.count(|s| s.holder_local += 1);
            let _ = self.meta.session_wait(keys);
            return;
        }
        let now = constellation_store_s3::lease::now_unix_ms();
        if let Some((held, renew)) = deleg.valid(ino, now) {
            deleg.count(|s| s.delegation_local += 1);
            if renew {
                // In use and past half its life: renew in the background
                // (the answer installs the new grant; nobody waits).
                deleg.count(|s| s.renewals += 1);
                let (reply, _) = tokio::sync::oneshot::channel();
                let _ = h.tx.send(SyncRequest::ReadIndex {
                    ino,
                    dir,
                    name: name.map(str::to_string),
                    reply,
                });
            }
            let waited = self.meta.session_wait_at(keys, &held.position);
            tracing::debug!(
                target: "constellation::cto",
                ino,
                dir,
                name,
                position = ?held.position,
                ?waited,
                "strict read under a delegation"
            );
            return;
        }
        let started = std::time::Instant::now();
        constellation_vfs::watch::stage("strict read index (core reply)");
        let (reply, answer) = tokio::sync::oneshot::channel();
        let sent =
            h.tx.send(SyncRequest::ReadIndex {
                ino,
                dir,
                name: name.map(str::to_string),
                reply,
            })
            .is_ok();
        let answer = if sent {
            answer.blocking_recv().ok()
        } else {
            None
        };
        constellation_vfs::watch::stage("session wait (strict)");
        match answer {
            Some(constellation_authority::ReadAnswer::Holder) => {
                deleg.count(|s| s.holder_local += 1);
                let _ = self.meta.session_wait(keys);
            }
            Some(constellation_authority::ReadAnswer::Position {
                position,
                delegated,
            }) => {
                let waited = self.meta.session_wait_at(keys, &position);
                tracing::debug!(
                    target: "constellation::cto",
                    ino,
                    dir,
                    name,
                    ?position,
                    delegated,
                    ?waited,
                    applied = ?self.meta.session().applied(),
                    "strict read after a ReadIndex"
                );
                deleg.note_read_index(started.elapsed().as_millis() as u64);
            }
            Some(constellation_authority::ReadAnswer::Tailed) => {
                tracing::debug!(target: "constellation::cto", ino, dir, name, "strict read: tailed S3");
                deleg.count(|s| s.s3_tail += 1);
                let _ = self.meta.session_wait(keys);
            }
            Some(constellation_authority::ReadAnswer::Degraded) | None => {
                deleg.count(|s| s.degraded += 1);
                let _ = self.meta.session_wait(keys);
            }
        }
    }

    /// Plan 30 §M8: a write this node's FUSE fast path executed as the
    /// sequencer returns only once no other node honours a read
    /// delegation on what it touched. One lock and an empty map when
    /// nobody holds one (every single-node and bounded-only cluster).
    pub(super) fn recall_after_local_write(
        &self,
        h: &SyncHandle,
        records: &[constellation_meta::LogRecord],
    ) {
        // (Called right after the op executed on this thread: the inodes
        // it changed that its records do not name are recalled too.)
        self.recall_after_local_inos(h, constellation_meta::recall_inos_executed(records));
    }

    /// [`Self::recall_after_local_write`] for writes that do not go
    /// through `execute_mutate` (the holder's own manifest commit).
    pub(super) fn recall_after_local_inos(&self, h: &SyncHandle, inos: Vec<Ino>) {
        let now = constellation_store_s3::lease::now_unix_ms();
        if self
            .meta
            .read_delegations()
            .touching(&inos, None, now)
            .is_empty()
        {
            return;
        }
        let started = std::time::Instant::now();
        constellation_vfs::watch::stage("read-delegation recall (core reply)");
        let (reply, done) = tokio::sync::oneshot::channel();
        if h.tx.send(SyncRequest::Recall { inos, reply }).is_ok() {
            let _ = done.blocking_recv();
        }
        constellation_vfs::watch::stage("running");
        let waited = started.elapsed().as_millis() as u64;
        self.meta.read_delegations().count(|s| {
            s.fuse_writes_recalled += 1;
            s.fuse_recall_wait_ms_total += waited;
        });
    }

    pub(crate) fn mutate_op_rebasable(
        &self,
        part_hint_ino: Ino,
        op: constellation_meta::MutateOp,
    ) -> Result<(), MutateFail> {
        if Self::is_synthetic(part_hint_ino) {
            return Err(MutateFail::Errno(Code::ReadOnly));
        }
        let Some(h) = &self.sync else {
            return constellation_meta::execute_mutate(&self.meta, &op, None)
                .map(|_| ())
                .map_err(mutate_fail);
        };
        // This op's exactly-once identity, allocated once, here, before
        // any forward or lease-acquisition attempt below, and kept
        // unchanged across every retry this call goes through (a
        // forward, a redirected forward, or the lease-path fallback). A
        // caller that needs a genuinely new op after a rebase (e.g.
        // `SetManifest`'s optimistic-concurrency retry) calls back into
        // this function again, which allocates a fresh one.
        let rid = constellation_meta::Rid {
            node: h.node_id,
            incarnation: h.incarnation,
            seq: h
                .next_rid_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        };
        let started = std::time::Instant::now();
        let result = self.mutate_op_rebasable_with_rid(h, part_hint_ino, &op, rid);
        h.acked.lock().unwrap().push(rid.seq);
        let took = started.elapsed();
        if took >= slow_fuse_op() {
            tracing::warn!(?rid, ?took, ?op, ok = result.is_ok(), "slow FUSE mutation");
        }
        result
    }

    pub(super) fn mutate_op_rebasable_with_rid(
        &self,
        h: &SyncHandle,
        _part_hint_ino: Ino,
        op: &constellation_meta::MutateOp,
        rid: constellation_meta::Rid,
    ) -> Result<(), MutateFail> {
        if h.read_only_member {
            return Err(MutateFail::Errno(Code::ReadOnly));
        }
        if h.departed
            .as_ref()
            .is_some_and(|departed| departed.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(MutateFail::Errno(Code::Io));
        }
        if h.epoch_frozen
            .as_ref()
            .is_some_and(|frozen| frozen.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(MutateFail::Errno(Code::ReadOnly));
        }
        // Plan 30 §M11: the delegate's own writes run here at local speed
        // (the sequencer's fast path, under a grant instead of the
        // lease); the core streams them from the journal. A write whose
        // `deps` this replica lacks, or whose grant is not honoured, goes
        // through the core, which parks it.
        if let Some(admission) = h
            .delegates
            .admit_for(&self.meta, &constellation_meta::TouchSet::from_op(op))
        {
            let gen = admission.gen;
            let session = self.meta.session();
            // The op's `deps` (`observed` plus every position this
            // node's clients were answered with); `None` (the streams
            // overflow) goes through the core, to the root.
            if let Some(deps) = session.deps().filter(|d| session.reaches(d)) {
                constellation_vfs::watch::stage("delegate execute (meta)");
                let result = self.meta.delegate_execute(op, Some(rid), gen, deps);
                // Journaled (or refused): the admission ends here, before
                // the core can answer a recall with a `through` this op
                // would be past.
                drop(admission);
                // `Journaled`, not `Nudge`: the core streams the row on
                // any event (`deleg_after_event`); a nudge would start a
                // sync round per write, whose upload drain races the
                // flush's own (each chunk uploaded twice).
                let _ = h.tx.send(SyncRequest::Journaled);
                return match result {
                    Ok((records, idx)) => {
                        // This replica holds the stream through `idx`
                        // (what the core's path notes in its adapter):
                        // the next write of this node's clients carries
                        // it in its `deps`, so a marker written after
                        // this data into another subtree waits for it
                        // there (harness `marker-order` under load).
                        session.note_stream(gen, idx);
                        h.delegates.note_executed();
                        self.recall_after_local_write(h, &records);
                        Ok(())
                    }
                    Err(e) => Err(mutate_fail(e)),
                };
            }
        }
        // Plan 30 §M12: the root's fast path executes only what the root
        // owns. A name a live delegation owns — a subtree, or a hash
        // range of this very directory — goes through the core, which
        // forwards it to the delegate or recalls the grant first. The
        // check and the execution hold the delegation gate shared, so no
        // grant is journaled between them (`Meta::deleg_gate`; before it,
        // the root executed its own creates and unlinks in a split
        // directory behind the range's delegate: two winners under
        // harness `chaos-soak-4`). Placement counts these executions
        // too (`note_fast_path`), or the root's share of a directory is
        // invisible to it.
        constellation_vfs::watch::stage("delegation gate");
        let Some(gate) = self.meta.root_fast_path(op) else {
            h.delegates.note_routed();
            return self.submit_to_core(h, op, rid, false);
        };
        // Plan 30 §M3b: the fast path admits the op (counted in flight)
        // atomically with respect to a release's final flush + CAS — see
        // `lease.rs`'s module doc, "The releasing flag".
        if let Some(admitted) = h.lease.admit() {
            constellation_vfs::watch::stage("local execute (meta)");
            let result = constellation_meta::execute_mutate(&self.meta, op, Some(rid));
            // Plan 30 §M9: under a durability gate, the row this op
            // journaled (at or below the tip now) must reach the backups
            // or the log before the acknowledgement.
            let jseq = if h.lease.ack_gated() && result.is_ok() {
                Some(self.meta.journal_tip().unwrap_or(u64::MAX))
            } else {
                None
            };
            // The row is journaled: a release's quiescence wait need not
            // wait on the recall below (it recalls every grant itself).
            drop(admitted);
            drop(gate);
            h.delegates
                .note_fast_path(constellation_meta::TouchSet::from_op(op));
            return match result {
                Ok(records) => {
                    h.lease.touch();
                    if let Some(jseq) = jseq {
                        if let Some(outcome) = self.ack_when_durable(h, rid, op, jseq) {
                            return outcome;
                        }
                    }
                    self.recall_after_local_write(h, &records);
                    Ok(())
                }
                Err(e) => Err(mutate_fail(e)),
            };
        }
        drop(gate);
        if h.lease.is_lost() {
            return Err(MutateFail::Errno(Code::Io));
        }
        // Plan 30 M5: everything else — forwarding with its same-rid
        // retries, the inbox when there is no P2P path, the lease path
        // with its in-doubt resolution against `completed` (and an inbox
        // refusal, which is an outcome too), the causal wait, the
        // deadline — is the authority core's client machine. One channel
        // round trip; the reply is the op's outcome or "in doubt".
        self.submit_to_core(h, op, rid, false)
    }

    /// Plan 30 §M9: the fast path's acknowledgement wait under a
    /// durability gate. `Some(outcome)` ends the op with it; `None`
    /// means acknowledged as usual (durable, or the gate went off with
    /// the lease still this node's — `Local` again). A lost lease
    /// leaves the op in doubt: resubmitted by rid through the core,
    /// which resolves it against `completed` (the stranded row is
    /// replayed by rid).
    pub(super) fn ack_when_durable(
        &self,
        h: &SyncHandle,
        rid: constellation_meta::Rid,
        op: &constellation_meta::MutateOp,
        jseq: u64,
    ) -> Option<Result<(), MutateFail>> {
        if self.local_ack_durable(h, jseq) {
            None
        } else {
            Some(self.submit_to_core(h, op, rid, true))
        }
    }

    /// Plan 30 §M9: wait until the local journal row at or below `jseq`
    /// is durable under the lease's acknowledgement policy (or the gate
    /// went off with the lease still this node's). `false`: in doubt —
    /// the lease was lost meanwhile, or the budget ran out; the caller
    /// resolves the op through the core. Every fast-path acknowledgement
    /// under a gate goes through here: `execute_mutate`'s ops and the
    /// holder's own manifest commit on close (without it the close
    /// returned before its row was durable, and the file's next read
    /// waited for it in `Meta::durability_pending` instead).
    pub(super) fn local_ack_durable(&self, h: &SyncHandle, jseq: u64) -> bool {
        let _ = h.tx.send(SyncRequest::Journaled);
        let session = self.meta.session();
        constellation_vfs::watch::stage("durability acknowledgement");
        match session.wait_durable(jseq, DURABLE_WAIT_BUDGET) {
            constellation_meta::DurableWait::Durable(waited) => {
                session.count_fast_ack(waited);
                true
            }
            constellation_meta::DurableWait::Ungated => true,
            constellation_meta::DurableWait::Lost | constellation_meta::DurableWait::TimedOut => {
                session.count_fast_ack_in_doubt();
                false
            }
        }
    }

    pub(super) fn submit_to_core(
        &self,
        h: &SyncHandle,
        op: &constellation_meta::MutateOp,
        rid: constellation_meta::Rid,
        in_doubt: bool,
    ) -> Result<(), MutateFail> {
        constellation_vfs::watch::stage("mutation submitted to the core (reply)");
        let (tx, rx) = tokio::sync::oneshot::channel();
        if h.tx
            .send(SyncRequest::Submit {
                op: op.clone(),
                rid,
                policy: constellation_authority::Policy::Client,
                in_doubt,
                reply: tx,
            })
            .is_err()
        {
            return Err(MutateFail::Errno(Code::Io));
        }
        // On the `fsync` path (plan 39) a forwarded commit's wait (its
        // forward deadline is tens of seconds) ends on the caller's death,
        // the soft timeout or the kernel cap like every other wait there:
        // overshooting the cap would let the kernel abort the connection.
        match crate::fsync_wait::recv(&self.rt, rx) {
            Ok(constellation_authority::ClientReply::Outcome(outcome)) => match outcome {
                constellation_meta::MutateOutcome::Accepted { .. } => Ok(()),
                // Never a client outcome: the core retries it.
                constellation_meta::MutateOutcome::Held { .. } => {
                    crate::fsync_wait::note(crate::fsync_wait::Failure::Transient(
                        "metadata commit held".into(),
                    ));
                    Err(MutateFail::Errno(Code::Io))
                }
                constellation_meta::MutateOutcome::Errno(e) => Err(MutateFail::Errno(e)),
                // The name exists on the holder; the core installed the
                // entry it sent with the refusal, so the caller's next
                // lookup resolves here too.
                constellation_meta::MutateOutcome::Exists { .. } => {
                    Err(MutateFail::Errno(Code::Exists))
                }
                constellation_meta::MutateOutcome::Conflict { manifest } => {
                    Err(MutateFail::Conflict { manifest })
                }
                constellation_meta::MutateOutcome::Busy
                | constellation_meta::MutateOutcome::NotHolder { .. } => {
                    crate::fsync_wait::note(crate::fsync_wait::Failure::Transient(
                        "no sequencer answered the metadata commit".into(),
                    ));
                    Err(MutateFail::Errno(Code::Io))
                }
            },
            // Neither executed here nor answered by a holder within the
            // deadline: `EIO`, and the op stays retryable — by the same
            // rid, which is what lets an `fsync` retry it (plan 39: the
            // base check refuses a duplicate and the rebase lays the
            // flush over whatever survived).
            Ok(constellation_authority::ClientReply::InDoubt) => {
                crate::fsync_wait::note(crate::fsync_wait::Failure::Transient(
                    "metadata commit in doubt (S3 or the sequencer unreachable)".into(),
                ));
                Err(MutateFail::Errno(Code::Io))
            }
            Err(_) => Err(MutateFail::Errno(Code::Io)),
        }
    }

    /// fsync() barrier. In `--fsync-mode s3`, block until the journal
    /// (up to now) is durable in the shared log; otherwise just nudge.
    pub(crate) fn sync_barrier(&self, ino: Ino) -> Result<(), Code> {
        self.sync_barrier_at(ino, Durability::Configured)
    }

    /// [`Self::sync_barrier`] at an explicit durability: `Configured` is
    /// the mount's `--fsync-mode`, `Local` never waits for the log,
    /// `Durable` always does.
    pub(crate) fn sync_barrier_at(&self, ino: Ino, level: Durability) -> Result<(), Code> {
        // Local durability first, in every mode: the metadata engine
        // commits to OS buffers, so fsync(2) must force them to disk.
        self.meta.sync().map_err(|e| e.code())?;
        let Some(h) = &self.sync else { return Ok(()) };
        let to_log = match level {
            Durability::Local => false,
            Durability::Durable => true,
            _ => h.fsync_s3,
        };
        if !to_log {
            let _ = h.tx.send(SyncRequest::Nudge);
            return Ok(());
        }
        constellation_vfs::watch::stage("fsync barrier (core reply)");
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if h.tx
            .send(SyncRequest::Barrier {
                ino,
                reply: reply_tx,
            })
            .is_err()
        {
            return Err(Code::Io);
        }
        match crate::fsync_wait::recv(&self.rt, reply_rx) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(failure)) => {
                if crate::fsync_wait::in_scope() {
                    tracing::debug!(error = %failure, class = failure.class.as_str(), "fsync barrier attempt failed");
                } else {
                    tracing::warn!(error = %failure, "fsync barrier: sync failed");
                }
                crate::fsync_wait::note_sync_failure(&failure);
                Err(Code::Io)
            }
            Err(()) => Err(Code::Io),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::jitter_fraction;

    #[test]
    fn jitter_fraction_stays_in_unit_range_and_varies() {
        let samples: Vec<f64> = (0..64).map(|_| jitter_fraction()).collect();
        for &v in &samples {
            assert!((0.0..1.0).contains(&v), "{v} outside [0, 1)");
        }
        // Not a statistical test, just a guard against a constant
        // fallback silently defeating the whole point of jittering.
        assert!(
            samples.windows(2).any(|w| w[0] != w[1]),
            "jitter_fraction must not be constant: {samples:?}"
        );
    }
}
