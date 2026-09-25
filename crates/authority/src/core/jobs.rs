//! The authority job slot: the sync round (what `main::run_managed_sync_
//! round` plus `run_sync_round` plus `Shipper::run_ordinary_round` did),
//! lease acquisition (`shipper::acquire_lease_for` plus the `SyncRequest::
//! Acquire` arm), a peer's handoff (`SyncRequest::HandOff`), GC's
//! tail-to-head, and the flush before a leave or an unmount: each a phase
//! machine that issues one S3 request at a time and advances on its
//! result.
//!
//! At most one job runs; the others queue. That is the structural form of
//! the keepers lock: the release/handoff "final flush then CAS" section
//! cannot interleave with an acquisition or another round because they
//! are the same slot, and a forwarded or local mutation never needs the
//! slot at all (see the module doc of `core`).

use super::lease::{PendingGate, Plan};
use super::{Core, S3For};
use crate::action::{Action, ControlOk, S3Op};
use crate::event::{CasFailure, Control, PeerMsg, S3Result, UploadResult};
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq};
use crate::replica::Replica;
use crate::segment;
use constellation_fs_core::Ino;
use constellation_meta::{JournalPos, LogRecord};
use constellation_store_s3::{Lease, LeaseTag};

/// A request for the slot.
#[derive(Debug, Clone)]
pub(crate) enum JobReq {
    Round {
        poll_triggered: bool,
    },
    Acquire {
        reason: &'static str,
        ask_handoff: bool,
    },
    Handoff {
        req: OpId,
        from: NodeId,
    },
    TailToHead {
        control: OpId,
    },
    /// Flush the journal, publish if the holder, release; `stop` ends
    /// the core (unmount).
    Flush {
        control: OpId,
        stop: bool,
    },
}

/// The kind of the job in the slot (`status`, tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    Round,
    Acquire,
    Handoff,
    TailToHead,
    Flush,
}

/// How many times a marker or a ship retries after a CAS collision
/// before the job gives up (`ship_epoch_marker`'s bound).
const COLLISION_RETRIES: u32 = 16;
/// How many tail attempts a handoff's catch-up makes before claiming
/// anyway (`HANDOFF_CATCH_UP_ATTEMPTS`).
const CATCH_UP_ATTEMPTS: u32 = 10;
/// Slack for the segment envelope's own fields when measuring a batch
/// against the byte cap.
const SEGMENT_ENVELOPE_SLACK: usize = 64;

/// What a segment PUT carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShipPurpose {
    /// Journal rows (with ride-along atime).
    Journal,
    /// An atime-only segment because the oldest row is stale.
    AtimeStale,
    /// An atime-only segment before a release; droppable on failure.
    AtimeBeforeRelease,
}

#[derive(Debug, Clone)]
enum Phase {
    /// `Action::UploadDirtyChunks` outstanding.
    Upload,
    /// A `SegmentRun` outstanding; `then` says what the tail was for.
    Tail { then: TailThen },
    /// `LeaseSwap(renewed)` outstanding (`retry`: after a `wanted_by`
    /// edit was re-read).
    Renew {
        retry: bool,
        /// Plan 30 §M10: the object the swap writes. On success the held
        /// lease becomes exactly it (it used to be recomputed at the
        /// result's time, an expiry later than the object's by the
        /// round trip — which also broke the carried-lease match of an
        /// epoch's flush re-claim).
        sent: Box<Lease>,
    },
    /// `LeaseGet` after a lost renewal CAS (`final_probe`: the second
    /// swap lost too, so this read is the deposition probe).
    RenewReread { final_probe: bool },
    /// The gate's epoch-marker `SegmentPut` outstanding.
    Marker { attempts: u32 },
    /// M13: the gate's `InboxDrain` outstanding.
    InboxDrain,
    /// A segment `SegmentPut` outstanding.
    Ship {
        seqs: Vec<u64>,
        seq: Seq,
        payload: Vec<u8>,
        epoch: Epoch,
        /// Plan 30 §M6: the journal seq this segment ships through (the
        /// envelope's `through`).
        through: u64,
        attempts: u32,
        atime_inos: Vec<Ino>,
        purpose: ShipPurpose,
    },
    /// A flush's publish outstanding.
    FlushPublish { op: OpId },
    /// `LeaseSwap(released)` outstanding.
    Release,
    /// Plan 30 §M8: waiting for read delegations to be recalled before
    /// the release CAS.
    RecallBeforeRelease,
    /// `LeaseGet` after a release CAS conflict.
    ReleaseReread,
    /// A deposition recovery's `RebuildReplica` outstanding.
    Recover { op: OpId },
    /// Acquire: `LeaseGet` outstanding.
    Get,
    /// Acquire: the CAS outstanding, and the object it writes (adopted
    /// exactly on success — see `Renew::sent`).
    Cas { plan: Plan, sent: Box<Lease> },
    /// Acquire: `LeaseRequest` sent to `holder`; waiting for its answer.
    /// `epoch_mode`: a continuation epoch's P2P-only handoff.
    LeaseRequest { req: OpId, epoch_mode: bool },
    /// Plan 30 §M10: a TTL takeover's promise check (`promise.rs`); the
    /// plan waits in `pending_plan`.
    PromiseCheck,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TailThen {
    /// The round's deposition recovery: strand and clear lost after.
    Recover,
    /// The round's ordinary tail.
    Round,
    /// A continuation epoch is open: the round's first step is a probe
    /// (S3 back?) before anything else.
    EpochProbe,
    /// A marker collided: retry the marker.
    Marker,
    /// A ship collided: retry the ship.
    Ship,
    /// Acquire: tail to head before a takeover CAS.
    Takeover,
    /// Acquire: catch up to a handoff's head before re-reading the lease.
    CatchUp,
    /// The tail-to-head control request.
    Control,
}

#[derive(Debug)]
enum What {
    Round {
        poll_triggered: bool,
        head_before: Seq,
        published: bool,
        /// This round closed the continuation epoch.
        epoch_closed: bool,
        /// The release this round issues is the epoch flush's.
        epoch_flush_release: bool,
        /// Deposition recovery: transactions rolled back so far.
        recover_rolled: u64,
        /// Plan 30 §M10 (found by the `epoch-missing-node` scenario): this
        /// round's epoch probe found S3 reachable. Only such a round may
        /// close an open epoch — a round that started before the epoch
        /// opened reaches its upload pass with S3 still down, and closing
        /// there tore the epoch down the instant it activated.
        epoch_probed: bool,
    },
    Acquire {
        reason: &'static str,
        ask_handoff: bool,
        attempts: u32,
        /// A handoff's head to catch up to, and the tails spent on it.
        catch_up: Option<(Seq, u32)>,
    },
    Handoff {
        req: OpId,
        from: NodeId,
        epoch: Epoch,
    },
    TailToHead {
        control: OpId,
    },
    Flush {
        control: OpId,
        stop: bool,
        published: bool,
    },
}

#[derive(Debug)]
pub(crate) struct Job {
    what: What,
    phase: Phase,
    /// The S3 request this phase is waiting on.
    op: Option<OpId>,
    /// The tail width of the run in flight (saturation check).
    width: usize,
}

impl Job {
    pub fn kind(&self) -> JobKind {
        match self.what {
            What::Round { .. } => JobKind::Round,
            What::Acquire { .. } => JobKind::Acquire,
            What::Handoff { .. } => JobKind::Handoff,
            What::TailToHead { .. } => JobKind::TailToHead,
            What::Flush { .. } => JobKind::Flush,
        }
    }
}

/// The state of a pending takeover gate after one step of it.
enum GateStep {
    Done,
    NeedMarker,
    NeedDrain,
    Failed(String),
    /// Plan 30 §M8: the restart quarantine has not passed; draining the
    /// inbox would acknowledge ops (their outcomes ride the log) while a
    /// previous incarnation's read delegations may still be honoured.
    /// Retried like a failure, without the alarm.
    Wait,
}

/// How many of `records` fit in one segment under `cap` bytes
/// (`shipper::records_within_cap`).
fn records_within_cap(records: &[LogRecord], cap: usize) -> Result<usize, String> {
    let mut total = SEGMENT_ENVELOPE_SLACK;
    for (i, rec) in records.iter().enumerate() {
        let len = rec.to_postcard().map_err(|e| e.to_string())?.len();
        if total + len > cap && i > 0 {
            return Ok(i);
        }
        total += len;
    }
    Ok(records.len())
}

impl Core {
    // ---- the slot ----

    /// Queue a job, starting it now if the slot is free. An `Acquire`
    /// coalesces with a queued one; a `Round` with a queued round.
    pub(crate) fn enqueue_job(
        &mut self,
        now: Ms,
        req: JobReq,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let dup = self.queued_jobs.iter().any(|q| {
            matches!(
                (q, &req),
                (JobReq::Acquire { .. }, JobReq::Acquire { .. })
                    | (JobReq::Round { .. }, JobReq::Round { .. })
            )
        });
        if dup {
            return;
        }
        if let (Some(job), JobReq::Acquire { .. }) = (&self.job, &req) {
            if job.kind() == JobKind::Acquire {
                return;
            }
        }
        self.queued_jobs.push_back(req);
        if self.job.is_none() {
            self.start_next_job(now, replica, out);
        }
    }

    fn start_next_job(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        // M7: streamed segments that arrived while the job owned the
        // cursor go in first.
        if self.job.is_none() {
            self.stream_drain(now, replica, out);
        }
        while self.job.is_none() {
            let Some(req) = self.queued_jobs.pop_front() else {
                if self.nudged {
                    self.nudged = false;
                    self.arm_poll(now, 0, out);
                }
                return;
            };
            match req {
                JobReq::Round { poll_triggered } => {
                    self.begin_round(now, poll_triggered, replica, out)
                }
                JobReq::Acquire {
                    reason,
                    ask_handoff,
                } => self.begin_acquire(now, reason, ask_handoff, replica, out),
                JobReq::Handoff { req, from } => self.begin_handoff(now, req, from, replica, out),
                JobReq::TailToHead { control } => {
                    // Plan 30 §M8: strict reads arriving from now on are
                    // not covered by this tail; they queue the next.
                    if self.rd.read_tail_open == Some(control) {
                        self.rd.read_tail_open = None;
                    }
                    self.job = Some(Job {
                        what: What::TailToHead { control },
                        phase: Phase::Tail {
                            then: TailThen::Control,
                        },
                        op: None,
                        width: 0,
                    });
                    self.issue_tail(out);
                }
                JobReq::Flush { control, stop } => self.begin_flush(control, stop, out),
            }
        }
    }

    /// Plan 30 §M7: whether a streamed segment may be applied now. A job
    /// that captured a sequence (a ship or marker PUT in flight, an
    /// acquisition's CAS or gate) owns the cursor; one that is only
    /// uploading or tailing does not — applying the next sequence during a
    /// tail is what the tail itself would do (`apply_incoming` skips a
    /// run's segments below the cursor).
    pub(crate) fn cursor_free(&self) -> bool {
        match &self.job {
            None => true,
            Some(job) => matches!(job.phase, Phase::Upload | Phase::Tail { .. }),
        }
    }

    fn set_phase(&mut self, phase: Phase, op: Option<OpId>) {
        if let Some(job) = self.job.as_mut() {
            job.phase = phase;
            job.op = op;
        }
    }

    pub(crate) fn start_round(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        self.enqueue_job(
            now,
            JobReq::Round {
                poll_triggered: true,
            },
            replica,
            out,
        );
    }

    // ---- shared steps ----

    fn issue_tail(&mut self, out: &mut Vec<Action>) {
        let width = self.cfg.tail_width.max(1);
        self.issue_tail_width(width, out);
    }

    fn issue_tail_width(&mut self, width: usize, out: &mut Vec<Action>) {
        let from = self.ship.next_seq;
        let op = self.issue_s3(S3Op::SegmentRun { from, width }, S3For::Job, out);
        if let Some(job) = self.job.as_mut() {
            job.op = Some(op);
            job.width = width;
        }
    }

    /// Apply a probed run. `Ok(true)` when the run was saturated (the
    /// caller probes again).
    fn apply_run(
        &mut self,
        now: Ms,
        run: Vec<(Seq, Vec<u8>)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<bool, String> {
        let width = self.job.as_ref().map(|j| j.width).unwrap_or(1);
        let n = run.len();
        for (seq, payload) in run {
            self.apply_incoming(now, seq, &payload, replica, out)
                .map_err(|e| e.to_string())?;
        }
        if n > 0 {
            self.stats.segments_applied += n as u64;
            self.answer_awaiting_log(now, replica, out);
        }
        Ok(n >= width && n > 0)
    }

    /// `Shipper::apply_decoded_segment`: own-segment recovery, epoch
    /// fencing, or a foreign apply that strands what it supersedes.
    pub(crate) fn apply_incoming(
        &mut self,
        now: Ms,
        seq: Seq,
        payload: &[u8],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> anyhow::Result<()> {
        if seq < self.ship.next_seq {
            return Ok(());
        }
        anyhow::ensure!(
            seq == self.ship.next_seq,
            "segment {seq} arrived before {}",
            self.ship.next_seq
        );
        let seg = segment::decode(payload)?;
        if seg.node == self.cfg.node_id {
            // (A marker's `TailFollows` is no journal row.)
            let rows: Vec<LogRecord> = seg
                .records
                .iter()
                .filter(|r| !matches!(r, LogRecord::TailFollows { .. }))
                .cloned()
                .collect();
            let Some(seqs) = replica.journal_head_matching(&rows)? else {
                anyhow::bail!(
                    "segment {seq} claims our node id {} but does not match the journal: \
                     state dir reuse or id collision",
                    self.cfg.node_id
                );
            };
            replica.ack_journal(&seqs, seq, Some(seg.journal_pos()))?;
            self.stats.own_recovered += 1;
            // Plan 30 §M9: durable in the log, whichever way we learned it.
            self.note_shipped(&seqs, seg.through);
            // M12 round 2 (long-sessions seed 10146): the segment is in
            // the window a forward reply's `base` is computed from, like
            // one whose PUT answered — or a reply names a base below a
            // rename this segment carried, and the requester's shadow
            // installs on a state where the name still exists (its
            // create skipped, the inode never materialised: node 1 ended
            // with a dentry to an inode it did not have).
            self.note_shipped_touches(seq, payload);
            tracing::info!(
                node = self.cfg.node_id,
                seq,
                records = seg.records.len(),
                "recovered unacked segment"
            );
            // A marker whose PUT landed but whose reply was lost: the gate
            // need not ship another.
            if seg
                .records
                .iter()
                .all(|r| matches!(r, LogRecord::TailFollows { .. }))
            {
                if let Some(gate) = self.lease.gate.as_mut() {
                    if gate.epoch == seg.epoch {
                        gate.marker_shipped = true;
                    }
                }
            }
        } else if seg.epoch > 0 && seg.epoch < self.ship.max_epoch {
            self.stats.fenced += 1;
            tracing::error!(
                node = self.cfg.node_id,
                seq,
                epoch = seg.epoch,
                max_epoch = self.ship.max_epoch,
                "FENCING VIOLATION: segment from a superseded lease epoch; skipping"
            );
            replica.skip_segment(seq)?;
        } else {
            let applied = replica.apply_segment(
                seq,
                seg.epoch,
                seg.through,
                &seg.rows,
                &seg.origins,
                &seg.records,
            )?;
            tracing::debug!(
                node = self.cfg.node_id,
                seq,
                epoch = seg.epoch,
                from = seg.node,
                records = seg.records.len(),
                retired = applied.retired,
                skipped = applied.skipped,
                "applied foreign segment"
            );
            self.stats.conflicts += applied.skipped as u64;
            if applied.stranded.any() {
                self.stats.speculation_rolled_back +=
                    (applied.stranded.shadows + applied.stranded.hints) as u64;
                self.stats.local_rolled_back += applied.stranded.locals as u64;
            }
            // A segment above the epoch we hold is a deposition: only a
            // lease CAS winner can have written it. Plan 30 §M9: give the
            // lease up at once (a fast takeover's fence), not at the next
            // renewal — a deposed holder must stop answering strict reads
            // as soon as it can know.
            if let Some(mine) = self.lease.epoch() {
                if seg.epoch > mine && !self.lease.lost && !self.lease.epoch_held() {
                    self.ship.renew_now = true;
                    if self.lease.held.is_some() {
                        self.deposed(now, seg.node, seg.epoch, mine, replica, out);
                    }
                }
            }
            if seg.node != 0 && seg.epoch >= self.ship.max_epoch {
                self.lease.cached_holder = Some(seg.node);
            }
            // Plan 30 §M9: a backup's tail is trimmed by the log itself;
            // the pre-S3 stream cursor follows the log.
            self.backup_note_segment(seg.epoch, seg.through, &seg.rows, replica);
            // Plan 30 §M11: a delegate's reply base is the last *applied*
            // segment that touched the keys (it ships nothing itself);
            // and the table may have changed.
            if !self.dl.mine.is_empty() {
                self.note_shipped_touches(seq, payload);
            }
            if seg
                .records
                .iter()
                .any(|r| matches!(r, LogRecord::Delegate { .. } | LogRecord::Recall { .. }))
            {
                self.delegation_sync(now, replica, out);
            }
        }
        self.ship.max_epoch = self.ship.max_epoch.max(seg.epoch);
        self.ship.next_seq = seq + 1;
        self.ship.head_seq = self.ship.head_seq.max(seq);
        self.ship.last_error = None;
        // Plan 30 §M9: pre-S3 batches that overtook this segment.
        self.retry_stream_ahead(now, replica, out);
        // M7: a holder streams what its cursor passes, read back from S3
        // included (its own unacked segments), so subscribers never miss
        // a sequence it knows.
        self.stream_passed(now, seq, seg.epoch, payload, out);
        Ok(())
    }

    /// Take the next batch (with ride-along atime) and PUT it. `false`
    /// when the journal is empty (or this node may not ship).
    fn issue_ship(
        &mut self,
        now: Ms,
        attempts: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<bool, String> {
        if self.skip_ship {
            return Ok(false);
        }
        let Some(epoch) = self.lease.journal_ship_epoch(now, &self.cfg) else {
            tracing::debug!(
                node = self.cfg.node_id,
                held = self.lease.held.is_some(),
                lost = self.lease.lost,
                gate = self.lease.gate.is_some(),
                "not shipping: no ship epoch"
            );
            return Ok(false);
        };
        let batch = replica
            .take_journal(self.cfg.segment_batch)
            .map_err(|e| e.to_string())?;
        if batch.is_empty() {
            tracing::trace!(node = self.cfg.node_id, "not shipping: empty batch");
            return Ok(false);
        }
        let mut records: Vec<LogRecord> = batch.iter().map(|(_, r)| r.clone()).collect();
        let journaled = records.len();
        // Ride-along atime drain (plan 20): a write segment is going out
        // anyway, so fold in the pending read-time bumps. Cleared only
        // after the PUT succeeds.
        let atime_rows = replica
            .take_atime(self.cfg.segment_batch)
            .map_err(|e| e.to_string())?;
        for (ino, atime_ns, time_ns) in &atime_rows {
            records.push(LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            });
        }
        // Cut to what fits in one segment, never inside a transaction
        // (plan 30 §M3b): the atime rows sit at the end and go first.
        let fits = records_within_cap(&records, self.cfg.segment_max_bytes)?;
        let (shipped_journal, shipped_atime) = if fits >= journaled {
            (journaled, fits - journaled)
        } else {
            (
                replica
                    .whole_tx_prefix(&batch, fits)
                    .map_err(|e| e.to_string())?,
                0,
            )
        };
        records.truncate(shipped_journal + shipped_atime);
        let seqs: Vec<u64> = batch[..shipped_journal].iter().map(|(s, _)| *s).collect();
        let atime_inos: Vec<Ino> = atime_rows[..shipped_atime]
            .iter()
            .map(|(ino, _, _)| *ino)
            .collect();
        // Plan 30 §M6: every journal row at or below `through` will have
        // shipped once this lands (a held-back transaction — M4 — stops
        // it below itself): the acked watermark this ship will leave.
        let through = replica
            .journal_through_after(&seqs)
            .map_err(|e| e.to_string())?;
        // Plan 30 §M11: each row's delegation origin rides the envelope.
        let origins = replica.journal_origins(&seqs);
        if origins.iter().any(|o| o.0 != 0) {
            tracing::trace!(
                node = self.cfg.node_id,
                ?seqs,
                ?origins,
                "shipping delegate origins"
            );
        }
        let payload = segment::encode(self.cfg.node_id, epoch, through, &seqs, &origins, &records)
            .map_err(|e| e.to_string())?;
        let seq = self.ship.next_seq;
        let op = self.issue_s3(
            S3Op::SegmentPut {
                seq,
                payload: payload.clone(),
            },
            S3For::Job,
            out,
        );
        self.set_phase(
            Phase::Ship {
                seqs,
                seq,
                payload,
                epoch,
                through,
                attempts,
                atime_inos,
                purpose: ShipPurpose::Journal,
            },
            Some(op),
        );
        Ok(true)
    }

    /// An atime-only segment (plan 20 / plan 29 M3b): `false` when there
    /// is nothing pending or this node may not ship.
    fn issue_atime_ship(
        &mut self,
        now: Ms,
        attempts: u32,
        purpose: ShipPurpose,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if self.skip_ship {
            return false;
        }
        let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) else {
            return false;
        };
        let rows = match replica.take_atime(self.cfg.segment_batch) {
            Ok(r) if !r.is_empty() => r,
            Ok(_) => return false,
            Err(e) => {
                tracing::debug!(node = self.cfg.node_id, error = %e, "atime drain read failed");
                if purpose == ShipPurpose::AtimeBeforeRelease {
                    let _ = replica.drop_atime();
                }
                return false;
            }
        };
        let records: Vec<LogRecord> = rows
            .iter()
            .map(|(ino, atime_ns, time_ns)| LogRecord::Atime {
                ino: *ino,
                atime_ns: *atime_ns,
                time_ns: *time_ns,
            })
            .collect();
        let through = replica.journal_acked_seq().unwrap_or(0);
        let Ok(payload) = segment::encode(self.cfg.node_id, epoch, through, &[], &[], &records)
        else {
            return false;
        };
        let atime_inos: Vec<Ino> = rows.iter().map(|(ino, _, _)| *ino).collect();
        let seq = self.ship.next_seq;
        let op = self.issue_s3(
            S3Op::SegmentPut {
                seq,
                payload: payload.clone(),
            },
            S3For::Job,
            out,
        );
        self.set_phase(
            Phase::Ship {
                seqs: Vec::new(),
                seq,
                payload,
                epoch,
                through,
                attempts,
                atime_inos,
                purpose,
            },
            Some(op),
        );
        true
    }

    /// Whether the oldest pending atime row is older than the standalone
    /// ship ceiling.
    fn atime_stale(&self, now: Ms, replica: &dyn Replica) -> bool {
        let _ = now;
        let Ok(Some(oldest_ns)) = replica.atime_oldest_pending_ns() else {
            return false;
        };
        let age_ns = constellation_fs_core::types::now_ns().saturating_sub(oldest_ns);
        age_ns >= (self.cfg.atime_ship_max_delay_ms as i64).saturating_mul(1_000_000)
    }

    /// Remember what segment `seq` touched (`Core::shipped_touches`).
    fn note_shipped_touches(&mut self, seq: Seq, payload: &[u8]) {
        let Ok(seg) = segment::decode(payload) else {
            return;
        };
        let touches = constellation_meta::replay::TouchSet::from_records(seg.records.iter());
        self.shipped_touches.push_back((seq, touches));
        while self.shipped_touches.len() > super::SHIPPED_TOUCH_WINDOW {
            if let Some((old, _)) = self.shipped_touches.pop_front() {
                self.shipped_floor = self.shipped_floor.max(old);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn ship_landed(
        &mut self,
        now: Ms,
        seqs: &[u64],
        seq: Seq,
        (epoch, through): (Epoch, u64),
        payload: Vec<u8>,
        atime_inos: &[Ino],
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), String> {
        replica
            .ack_journal(
                seqs,
                seq,
                Some(JournalPos {
                    epoch,
                    jseq: through,
                }),
            )
            .map_err(|e| e.to_string())?;
        if !atime_inos.is_empty() {
            let _ = replica.clear_atime(atime_inos);
        }
        self.ship.next_seq = seq + 1;
        self.ship.head_seq = self.ship.head_seq.max(seq);
        self.ship.max_epoch = self.ship.max_epoch.max(epoch);
        self.ship.last_ship_epoch = self.ship.last_ship_epoch.max(epoch);
        self.ship.last_error = None;
        if !seqs.is_empty() {
            self.ship.shipped_since_publish += 1;
        }
        self.stats.segments_shipped += 1;
        self.note_shipped_touches(seq, &payload);
        // Plan 30 §M9: these rows are durable in the log (`ack=s3`'s
        // acknowledgements, and a backup's, wait for exactly this).
        self.note_shipped(seqs, through);
        tracing::debug!(
            target: "constellation_authority::stream",
            node = self.cfg.node_id,
            seq,
            epoch,
            records = seqs.len(),
            "ship"
        );
        self.stream_passed(now, seq, epoch, &payload, out);
        out.push(Action::Announce {
            seq,
            epoch,
            payload,
        });
        // The segment-count cadence's publish, off the ship path (plan 30
        // §M3b's `publish_in_background`): a round under sustained load
        // ships until its journal drains, so waiting for the round's end
        // could postpone every commit.
        if self.cfg.publisher
            && self.ship.shipped_since_publish >= self.cfg.publish_every
            && self.publishing.is_none()
            && !self.skip_ship
            && !replica.has_outstanding_speculation()
        {
            let op = self.op_id();
            self.publishing = Some(op);
            self.ship.shipped_since_publish = 0;
            self.ship.last_publish = now;
            out.push(Action::Publish {
                op,
                epoch: self.ship.last_ship_epoch,
            });
            if let Some(Job {
                what: What::Round { published, .. },
                ..
            }) = self.job.as_mut()
            {
                *published = true;
            }
        }
        Ok(())
    }

    fn issue_marker(&mut self, attempts: u32, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(gate) = self.lease.gate else {
            return;
        };
        // Plan 30 §M6: the new tenure's journal position starts here, above
        // every position of the tenure it ends.
        let through = replica.journal_acked_seq().unwrap_or(0);
        let payload = segment::encode(
            self.cfg.node_id,
            gate.epoch,
            through,
            &[],
            &[],
            &marker_records(&gate),
        )
        .expect("marker segment");
        let seq = self.ship.next_seq;
        let op = self.issue_s3(S3Op::SegmentPut { seq, payload }, S3For::Job, out);
        self.set_phase(Phase::Marker { attempts }, Some(op));
    }

    fn issue_drain(&mut self, out: &mut Vec<Action>) {
        let Some(gate) = self.lease.gate else {
            return;
        };
        let op = self.issue_s3(
            S3Op::InboxDrain {
                below_epoch: gate.epoch,
            },
            S3For::Job,
            out,
        );
        self.set_phase(Phase::InboxDrain, Some(op));
    }

    /// `shipper::complete_gate`: after the marker (or with none needed),
    /// strand what the new epoch supersedes, replay the queue locally,
    /// drain the older epochs' inbox, open the view.
    fn complete_gate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) -> GateStep {
        let Some(gate) = self.lease.gate else {
            return GateStep::Done;
        };
        if !gate.marker_shipped {
            return GateStep::NeedMarker;
        }
        if gate.takeover {
            match replica.strand_below_epoch(gate.epoch) {
                Ok(stranded) => {
                    if stranded.any() {
                        tracing::warn!(
                            node = self.cfg.node_id,
                            epoch = gate.epoch,
                            shadows = stranded.shadows,
                            hints = stranded.hints,
                            local = stranded.locals,
                            "takeover stranded speculative state; rolled back, replaying"
                        );
                        self.stats.speculation_rolled_back +=
                            (stranded.shadows + stranded.hints) as u64;
                        self.stats.local_rolled_back += stranded.locals as u64;
                    }
                }
                Err(e) => return GateStep::Failed(e.to_string()),
            }
        }
        // Plan 30 §M9: what this node held as the predecessor's backup
        // goes into its own journal now, in journal order — before the
        // replay of anything stranded, so the replay finds it completed.
        if let Some(prev_epoch) = gate.backup_tail_epoch {
            if let Err(e) = self.apply_backup_tail(prev_epoch, replica) {
                return GateStep::Failed(e);
            }
            if let Some(g) = self.lease.gate.as_mut() {
                g.backup_tail_epoch = None;
            }
            // Plan 30 §M14: the predecessor's lock table, as last
            // mirrored; the floor covers what the mirror missed.
            self.lock_install_mirror(now, replica);
        }
        if let Err(e) = self.replay_queue_locally(now, replica, out) {
            return GateStep::Failed(e.to_string());
        }
        // Plan 30 §M9: the journal work is done; what waits now (the
        // quarantine, the drain) keeps the view closed, not the shipper.
        if let Some(g) = self.lease.gate.as_mut() {
            g.shippable = true;
        }
        if !gate.drained {
            if replica.read_delegations().quarantine_until() > now.0 {
                return GateStep::Wait;
            }
            return GateStep::NeedDrain;
        }
        self.lease.gate = None;
        GateStep::Done
    }

    fn marker_landed(
        &mut self,
        now: Ms,
        seq: Seq,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<(), String> {
        let Some(gate) = self.lease.gate.as_mut() else {
            return Ok(());
        };
        let epoch = gate.epoch;
        let through = replica.journal_acked_seq().map_err(|e| e.to_string())?;
        replica
            .ack_journal(
                &[],
                seq,
                Some(JournalPos {
                    epoch,
                    jseq: through,
                }),
            )
            .map_err(|e| e.to_string())?;
        let Some(gate) = self.lease.gate.as_mut() else {
            return Ok(());
        };
        gate.marker_shipped = true;
        self.ship.next_seq = seq + 1;
        self.ship.head_seq = self.ship.head_seq.max(seq);
        self.ship.max_epoch = self.ship.max_epoch.max(epoch);
        self.stats.epoch_markers += 1;
        let records = marker_records(gate);
        let payload = segment::encode(self.cfg.node_id, epoch, through, &[], &[], &records)
            .expect("marker segment");
        self.stream_passed(now, seq, epoch, &payload, out);
        out.push(Action::Announce {
            seq,
            epoch,
            payload,
        });
        tracing::info!(
            node = self.cfg.node_id,
            seq,
            epoch,
            "shipped the takeover's epoch marker"
        );
        // Plan 30 §M9: a fast takeover's acknowledgement floor counts
        // from here.
        self.note_marker_landed(now, replica, out);
        Ok(())
    }

    /// `LeaseKeeper::mark_lost` plus what the keeper's callers do next.
    /// Plan 30 §M9: every acknowledgement parked for durability is
    /// answered as not given (the requester retries by rid).
    pub(crate) fn deposed(
        &mut self,
        now: Ms,
        holder: NodeId,
        epoch: Epoch,
        my_epoch: Epoch,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        tracing::error!(
            node = self.cfg.node_id,
            new_holder = holder,
            new_epoch = epoch,
            my_epoch,
            "LEASE LOST: another node took write authority"
        );
        self.lease.mark_lost(holder, epoch, my_epoch);
        replica.set_holder_epoch(0);
        let _ = replica.persist_lost(true);
        self.inbox.holder = None;
        self.ack_abort_parked(now, replica, out);
        self.deleg_on_lease_gone(now, replica, out);
    }

    /// Plan 30 §M9: whether the job in the slot has a lease CAS (or its
    /// re-read) in flight — a reconfiguration waits for it rather than
    /// racing our own renewal or release on the object's version.
    pub(crate) fn lease_cas_in_flight(&self) -> bool {
        match &self.job {
            None => false,
            Some(job) => matches!(
                job.phase,
                Phase::Renew { .. }
                    | Phase::RenewReread { .. }
                    | Phase::Release
                    | Phase::ReleaseReread
                    | Phase::Cas { .. }
            ),
        }
    }

    /// `recovery::recover_deposed` after the tail to head: roll back from
    /// the before-images; journal rows without any (holder capture off)
    /// need the namespace rebuilt from the log, which the driver does.
    /// `Ok(true)` when the recovery is complete now.
    fn recover_deposed(
        &mut self,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<bool, String> {
        let floor = self.lease.lost_floor.max(1);
        let mut rolled = 0u64;
        if replica.holder_capture() {
            let stranded = replica
                .strand_below_epoch(floor)
                .map_err(|e| e.to_string())?;
            rolled += stranded.locals as u64;
            self.stats.speculation_rolled_back += (stranded.shadows + stranded.hints) as u64;
        }
        if let Some(Job {
            what: What::Round { recover_rolled, .. },
            ..
        }) = self.job.as_mut()
        {
            *recover_rolled = rolled;
        }
        let uncaptured = replica.journal_len().map_err(|e| e.to_string())?;
        if uncaptured > 0 {
            let op = self.op_id();
            self.set_phase(Phase::Recover { op }, Some(op));
            out.push(Action::RebuildReplica { op });
            return Ok(false);
        }
        self.finish_recovery(rolled, "rolled back from before-images", replica)?;
        Ok(true)
    }

    fn finish_recovery(
        &mut self,
        rolled: u64,
        how: &str,
        replica: &dyn Replica,
    ) -> Result<(), String> {
        self.lease.clear_lost();
        replica.persist_lost(false).map_err(|e| e.to_string())?;
        self.stats.depositions += 1;
        self.stats.local_rolled_back += rolled;
        self.mark_unacked_replays(replica);
        let queued = replica.pending_replays().map(|q| q.len()).unwrap_or(0);
        let summary = format!(
            "deposition recovered: {rolled} unshipped transaction(s) {how}; \
             {queued} op(s) queued for replay by rid"
        );
        tracing::warn!(node = self.cfg.node_id, "{summary}");
        self.last_recovery = Some(summary);
        Ok(())
    }

    pub(crate) fn on_rebuild_done(
        &mut self,
        now: Ms,
        op: OpId,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if !matches!(job.phase, Phase::Recover { op: o } if o == op) {
            return;
        }
        if !ok {
            self.finish_round(
                now,
                Some("rebuilding the deposed replica from the log failed".into()),
                replica,
                out,
            );
            return;
        }
        let rolled = match &job.what {
            What::Round { recover_rolled, .. } => *recover_rolled,
            _ => 0,
        };
        let uncaptured = replica.journal_len().unwrap_or(0);
        match self.finish_recovery(
            rolled + uncaptured,
            "rolled back (rebuilt from the shared log where uncaptured)",
            replica,
        ) {
            Ok(()) => self.round_renew(now, replica, out),
            Err(error) => self.finish_round(now, Some(error), replica, out),
        }
    }

    /// Whether this holder may skip reading its own stream this round
    /// (`Shipper::held_partitions`).
    fn skip_tail_as_holder(&mut self, now: Ms) -> bool {
        if self.lease.ship_epoch(now, &self.cfg).is_none() || self.lease.epoch_held() {
            return false;
        }
        if self.ship.probe_now {
            // Plan 30 §M9: a strict read waits for proof of S3 liveness.
            self.ship.probe_now = false;
            self.ship.held_tail_at = Some(now);
            return false;
        }
        match self.ship.held_tail_at {
            Some(at) if now.since(at) < self.cfg.held_tail_staleness_ms as i64 => true,
            Some(_) => {
                self.ship.held_tail_at = Some(now);
                false
            }
            None => {
                self.ship.held_tail_at = Some(now);
                true
            }
        }
    }

    // ---- the round ----

    fn begin_round(
        &mut self,
        now: Ms,
        poll_triggered: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.nudged = false;
        tracing::trace!(node = self.cfg.node_id, poll_triggered, "round begins");
        self.job = Some(Job {
            what: What::Round {
                poll_triggered,
                head_before: self.ship.head_seq,
                published: false,
                epoch_closed: false,
                epoch_flush_release: false,
                recover_rolled: 0,
                epoch_probed: false,
            },
            phase: Phase::Upload,
            op: None,
            width: 0,
        });
        if self.epoch.open {
            // A continuation epoch is open: writes are local, nothing
            // ships; the round only probes whether S3 is back. Frozen (a
            // member is missing), only the hold's owner probes: when S3
            // is back it flushes and closes like an active epoch's holder
            // would (plan 30 §M10 — a member that never returns must not
            // keep the journal out of S3 forever; the flush's re-claim
            // CAS is the arbiter, and it fails if an admin leave fenced
            // the lease).
            if self.epoch.frozen && !self.lease.epoch_held() {
                self.finish_round(now, None, replica, out);
                return;
            }
            self.set_phase(
                Phase::Tail {
                    then: TailThen::EpochProbe,
                },
                None,
            );
            self.issue_tail(out);
            return;
        }
        let op = self.op_id();
        self.set_phase(Phase::Upload, Some(op));
        out.push(Action::UploadDirtyChunks {
            op,
            ino: None,
            round: true,
            complete: !self.round_waiters.is_empty() || self.publish_forced,
        });
    }

    pub(crate) fn on_uploads_done(
        &mut self,
        now: Ms,
        op: OpId,
        result: UploadResult,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if job.op != Some(op) || !matches!(job.phase, Phase::Upload) {
            return;
        }
        let held = match &result {
            UploadResult::Done { held } => *held,
            _ => self.held_back,
        };
        self.held_back = held;
        match job.what {
            What::Round { .. } => match result {
                UploadResult::Done { .. } => {
                    let probed = matches!(
                        self.job.as_ref().map(|j| &j.what),
                        Some(What::Round {
                            epoch_probed: true,
                            ..
                        })
                    );
                    if self.epoch.open && !probed {
                        // Started before the epoch opened: S3 is not
                        // known to be back. Nothing ships; the next round
                        // probes.
                        self.finish_round(now, None, replica, out);
                        return;
                    }
                    if self.epoch.open {
                        // S3 is back and this node may ship: close the
                        // epoch; the journal ships under an ordinary S3
                        // lease from here.
                        if self.lease.epoch_held() {
                            // Plan 30 §M10: silent until the journal is in.
                            self.pr.flush_pending = true;
                        }
                        out.push(Action::EpochClose);
                        self.skip_ship = false;
                        self.lease.release_local();
                        self.deleg_on_lease_gone(now, replica, out);
                        replica.set_holder_epoch(0);
                        if let Some(Job {
                            what: What::Round { epoch_closed, .. },
                            ..
                        }) = self.job.as_mut()
                        {
                            *epoch_closed = true;
                        }
                    }
                    self.round_after_upload(now, replica, out);
                }
                UploadResult::Failed(error) => {
                    self.finish_round(
                        now,
                        Some(format!("chunk upload failed; not shipping: {error}")),
                        replica,
                        out,
                    );
                }
                UploadResult::Skip => self.finish_round(now, None, replica, out),
            },
            What::Handoff { .. } | What::Flush { .. } => match result {
                UploadResult::Done { .. } => {
                    self.lease.releasing = true;
                    self.flush_continue(now, 0, replica, out);
                }
                _ => self.finish_flush_job(now, false, replica, out),
            },
            _ => {}
        }
    }

    fn round_after_upload(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lease.lost {
            self.set_phase(
                Phase::Tail {
                    then: TailThen::Recover,
                },
                None,
            );
            self.issue_tail(out);
            return;
        }
        self.round_renew(now, replica, out);
    }

    fn round_renew(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let due = match &self.lease.held {
            Some((lease, _)) => {
                self.ship.renew_now || lease.expires_in_ms(now.0) <= (self.cfg.ttl_ms / 2) as i64
            }
            None => false,
        };
        if due && !self.lease.lost {
            let (mine, tag) = self.lease.held.clone().expect("held");
            let renewed = self.lease.renewed_lease(now, &self.cfg, &mine);
            let op = self.issue_s3(
                S3Op::LeaseSwap {
                    lease: renewed.clone(),
                    tag,
                },
                S3For::Job,
                out,
            );
            self.set_phase(
                Phase::Renew {
                    retry: false,
                    sent: Box::new(renewed),
                },
                Some(op),
            );
            return;
        }
        self.round_gate(now, replica, out);
    }

    fn round_gate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lease.gate.is_some() && !self.lease.lost {
            match self.complete_gate(now, replica, out) {
                GateStep::Done => {}
                GateStep::NeedMarker => {
                    self.issue_marker(0, replica, out);
                    return;
                }
                GateStep::NeedDrain => {
                    self.issue_drain(out);
                    return;
                }
                GateStep::Failed(error) => {
                    tracing::error!(%error, node = self.cfg.node_id, "takeover gate failed; retrying next round");
                }
                GateStep::Wait => {
                    tracing::debug!(
                        node = self.cfg.node_id,
                        "takeover gate waits out the read-delegation quarantine"
                    );
                }
            }
        }
        self.round_tail(now, replica, out);
    }

    fn round_tail(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.skip_tail_as_holder(now) {
            self.round_ship(now, 0, replica, out);
            return;
        }
        // M7: the holder's stream has delivered everything it reported;
        // S3 has nothing a round could find sooner.
        if self.stream_covers_tail(now) {
            self.stats.stream_tail_skips += 1;
            self.round_ship(now, 0, replica, out);
            return;
        }
        self.set_phase(
            Phase::Tail {
                then: TailThen::Round,
            },
            None,
        );
        let width = self.stream_tail_width(now);
        self.issue_tail_width(width, out);
    }

    fn round_ship(&mut self, now: Ms, attempts: u32, replica: &dyn Replica, out: &mut Vec<Action>) {
        match self.issue_ship(now, attempts, replica, out) {
            Ok(true) => {}
            Ok(false) => {
                // Journal but no authority: acquire for it (the
                // `ship-pending-journal` fallback), after this round.
                if !self.lease.lost
                    && !self.epoch.active
                    && self.lease.gate.is_none()
                    && self.lease.held.is_none()
                    && !self.lease.epoch_held()
                    && replica.journal_has_undelegated()
                {
                    self.queued_jobs.push_back(JobReq::Acquire {
                        reason: "ship-pending-journal",
                        ask_handoff: self.cfg.p2p,
                    });
                }
                // A holder with nothing else to ship may still sit on a
                // stale backlog of read-time bumps (plan 29 M3b).
                if attempts == 0
                    && self.atime_stale(now, replica)
                    && self.issue_atime_ship(now, 0, ShipPurpose::AtimeStale, replica, out)
                {
                    return;
                }
                self.round_publish(now, replica, out);
            }
            Err(error) => self.finish_round(now, Some(error), replica, out),
        }
    }

    fn round_publish(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let holder_epoch = self.lease.ship_epoch(now, &self.cfg);
        let cadence_due =
            self.publish_forced || self.ship.shipped_since_publish >= self.cfg.publish_every;
        let idle_due = self.cfg.publisher
            && replica.has_dirty()
            && now.since(self.ship.last_publish) >= self.cfg.publish_idle_ms as i64;
        if !self.cfg.publisher || self.publishing.is_some() || self.skip_ship {
            self.round_release(now, replica, out);
            return;
        }
        match holder_epoch {
            Some(epoch) if (cadence_due || idle_due) && !replica.has_outstanding_speculation() => {
                let op = self.op_id();
                self.publishing = Some(op);
                self.publish_forced = false;
                self.ship.shipped_since_publish = 0;
                self.ship.last_publish = now;
                out.push(Action::Publish {
                    op,
                    epoch: self.ship.last_ship_epoch.max(epoch),
                });
                if let Some(Job {
                    what: What::Round { published, .. },
                    ..
                }) = self.job.as_mut()
                {
                    *published = true;
                }
            }
            // M4: a follower's idle cadence clears what the head commit
            // already covers instead of publishing.
            None if idle_due => {
                let op = self.op_id();
                self.publishing = Some(op);
                self.ship.last_publish = now;
                out.push(Action::FollowHead { op });
            }
            _ => {}
        }
        self.round_release(now, replica, out);
    }

    fn round_release(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        let backlog = replica.journal_len().unwrap_or(0);
        // The continuation epoch's flush: its journal is in S3 and the
        // lease taken for it can go.
        if self.epoch.flushing
            && self.lease.held.is_some()
            && !self.lease.lost
            && !self.lease.epoch_held()
            && backlog == 0
        {
            self.lease.releasing = true;
            if let Some(Job {
                what:
                    What::Round {
                        epoch_flush_release,
                        ..
                    },
                ..
            }) = self.job.as_mut()
            {
                *epoch_flush_release = true;
            }
            self.release_after_atime(now, replica, out);
            return;
        }
        if self.lease.held.is_none() || self.lease.lost || !self.lease.wants_handoff(now, &self.cfg)
        {
            self.finish_round(now, None, replica, out);
            return;
        }
        // Plan 30 §M11: the root lease stays while a delegation is live —
        // a successor could not honour the grants' expiries (it has no
        // clock for them), and the delegates keep their local speed
        // exactly by the root not moving. Other writers forward.
        if self.deleg_live_generations() > 0 {
            self.finish_round(now, None, replica, out);
            return;
        }
        // Plan 30 §M14: nor while lock grants are live (a successor would
        // start a grace; the holders keep their locks instead).
        if replica.locks().grants_len() > 0 {
            self.finish_round(now, None, replica, out);
            return;
        }
        if self
            .lease
            .idle_release_due(now, &self.cfg, backlog, self.held_back)
        {
            tracing::info!(node = self.cfg.node_id, "idle-releasing the lease");
            self.lease.releasing = true;
            self.lease.begin_handoff_pause(now, &self.cfg);
            self.release_after_atime(now, replica, out);
            return;
        }
        if backlog > 0 {
            // Force the backlog to zero: close the fast path for a while
            // so the next round can drain and release.
            self.lease.begin_handoff_pause(now, &self.cfg);
            self.nudged = true;
        }
        self.finish_round(now, None, replica, out);
    }

    /// Ship-then-release (plan 20): pending read-time atime goes out in
    /// one last segment before the lease does.
    fn release_after_atime(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.issue_atime_ship(now, 0, ShipPurpose::AtimeBeforeRelease, replica, out) {
            return;
        }
        self.issue_release(now, replica, out);
    }

    /// The release CAS — but a released lease can be taken over at once,
    /// so plan 30 §M8's read delegations must all be recalled (or
    /// outwaited) first. `releasing` is up by now: no new grant is made
    /// while the recalls run or the CAS is in flight.
    fn issue_release(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lease.held.is_none() {
            return;
        }
        self.lease.releasing = true;
        if self.park_release_for_recalls(now, replica, out) {
            self.set_phase(Phase::RecallBeforeRelease, None);
            return;
        }
        let Some((lease, tag)) = self.lease.held.clone() else {
            return;
        };
        let op = self.issue_s3(
            S3Op::LeaseSwap {
                lease: lease.released(),
                tag,
            },
            S3For::Job,
            out,
        );
        self.set_phase(Phase::Release, Some(op));
    }

    /// Plan 30 §M8: the recalls a release waited for are done.
    pub(crate) fn release_after_recalls(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if !matches!(job.phase, Phase::RecallBeforeRelease) {
            return;
        }
        let kind = job.kind();
        if self.lease.held.is_some() && !self.lease.lost {
            self.issue_release(now, replica, out);
            return;
        }
        // The lease went away while the recalls ran: nothing to release.
        self.lease.releasing = false;
        match kind {
            JobKind::Round => self.finish_round(now, None, replica, out),
            _ => self.finish_flush_job(now, false, replica, out),
        }
    }

    fn finish_round(
        &mut self,
        now: Ms,
        failed: Option<String>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.take() else {
            return;
        };
        let What::Round {
            poll_triggered,
            head_before,
            published,
            ..
        } = job.what
        else {
            self.job = Some(job);
            return;
        };
        self.stats.rounds_completed += 1;
        if let Some(error) = &failed {
            tracing::warn!(node = self.cfg.node_id, %error, "sync round failed; will retry");
            self.ship.last_error = Some(error.clone());
        }
        let backlog = replica.journal_len().unwrap_or(0);
        let productive = self.nudged
            || self.ship.head_seq != head_before
            || backlog > 0
            || !poll_triggered
            || failed.is_some()
            || !self.inbox.pending.is_empty();
        if productive {
            self.idle_rounds = 0;
        } else {
            self.idle_rounds = self.idle_rounds.saturating_add(1);
        }
        let waiters = std::mem::take(&mut self.round_waiters);
        for (op, req) in waiters {
            let result = match (&failed, req) {
                (Some(error), _) => Err(error.clone()),
                (None, Control::PublishNow) => {
                    if published {
                        Ok(ControlOk::Published {
                            applied: replica.applied_seq().unwrap_or(0),
                        })
                    } else {
                        Err("publish deferred: not the holder, or speculation outstanding".into())
                    }
                }
                (None, Control::Reintegrate) => {
                    if self.lease.lost {
                        // Not recovered this round: leave the request for
                        // the next one.
                        self.round_waiters.push((op, Control::Reintegrate));
                        continue;
                    }
                    Ok(ControlOk::Text(self.last_recovery.clone().unwrap_or_else(
                        || "not deposed: nothing to recover".into(),
                    )))
                }
                (None, _) => {
                    if backlog == 0 {
                        Ok(ControlOk::Done)
                    } else {
                        Err("journal not shipped: no lease".into())
                    }
                }
            };
            out.push(Action::ControlDone { op, result });
        }
        if !self.round_waiters.is_empty() {
            self.nudged = true;
        }
        out.push(Action::RoundDone { failed });
        let delay = if self.nudged {
            0
        } else {
            self.next_poll_ms(now)
        };
        self.arm_poll(now, delay, out);
        self.nudged = false;
        self.inbox_holder_tick(now, replica, out);
        self.start_next_job(now, replica, out);
    }

    // ---- acquire ----

    fn begin_acquire(
        &mut self,
        now: Ms,
        reason: &'static str,
        ask_handoff: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.job = Some(Job {
            what: What::Acquire {
                reason,
                ask_handoff,
                attempts: 0,
                catch_up: None,
            },
            phase: Phase::Get,
            op: None,
            width: 0,
        });
        if self.retired() {
            // Plan 30 §M10: admin `leave --node-id` retired this node.
            self.finish_acquire(now, false, replica, out);
            return;
        }
        if self.lease.lost || replica.lost_persisted() {
            // Recovered by the next round; the callers retry.
            self.nudged = true;
            self.finish_acquire(now, false, replica, out);
            return;
        }
        if self.lease.is_paused_for_handoff(now) {
            self.finish_acquire(now, false, replica, out);
            return;
        }
        if self.epoch.active && !self.epoch.frozen {
            // A continuation epoch: authority moves by P2P handoff only.
            if self.lease.epoch_held() {
                self.acquire_gate(now, replica, out);
                return;
            }
            let target = self
                .lease
                .cached_holder
                .filter(|h| *h != self.cfg.node_id)
                .or_else(|| {
                    self.links
                        .values()
                        .find(|l| l.connected && l.node != self.cfg.node_id)
                        .or_else(|| self.links.values().find(|l| l.node != self.cfg.node_id))
                        .map(|l| l.node)
                });
            match target {
                Some(holder) if self.cfg.p2p => {
                    let req = self.op_id();
                    self.set_phase(
                        Phase::LeaseRequest {
                            req,
                            epoch_mode: true,
                        },
                        None,
                    );
                    self.set_timer(
                        now.plus(self.cfg.handoff_request_timeout_ms),
                        super::Timer::JobRequestTimeout(req),
                        out,
                    );
                    out.push(Action::Send {
                        to: holder,
                        msg: PeerMsg::LeaseRequest { req },
                    });
                }
                _ => self.finish_acquire(now, false, replica, out),
            }
            return;
        }
        if self.epoch.open && !self.epoch.active {
            tracing::debug!(
                node = self.cfg.node_id,
                "continuation epoch open: no S3 acquisition"
            );
            self.finish_acquire(now, false, replica, out);
            return;
        }
        let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
        self.set_phase(Phase::Get, Some(op));
    }

    fn acquire_classified(
        &mut self,
        now: Ms,
        object: Option<(Lease, LeaseTag)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some((lease, _)) = &object {
            self.lease.note_object(now, lease);
        }
        let plan = self.lease.classify(now, &self.cfg, object, self.bk.sealed);
        match plan {
            Plan::Held => {
                self.acquire_gate(now, replica, out);
            }
            Plan::Refused(why) => {
                tracing::warn!(node = self.cfg.node_id, why, "lease acquisition refused");
                self.finish_acquire(now, false, replica, out);
            }
            Plan::Create => {
                let lease = self.lease.granted_lease(now, &self.cfg, None);
                let op = self.issue_s3(
                    S3Op::LeaseCreate {
                        lease: lease.clone(),
                    },
                    S3For::Job,
                    out,
                );
                self.set_phase(
                    Phase::Cas {
                        plan: Plan::Create,
                        sent: Box::new(lease),
                    },
                    Some(op),
                );
            }
            Plan::Busy {
                holder,
                epoch,
                prev,
                tag,
            } => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    holder,
                    epoch,
                    "lease held by another node"
                );
                // Register in `wanted_by` (best effort, result ignored) and,
                // once, ask the holder over P2P for a fast handoff.
                if !prev.wanted_by.contains(&self.cfg.node_id) {
                    self.issue_s3(
                        S3Op::LeaseSwap {
                            lease: prev.wanting(self.cfg.node_id),
                            tag,
                        },
                        S3For::RegisterWanted,
                        out,
                    );
                }
                let (ask, attempts) = match &self.job.as_ref().expect("job").what {
                    What::Acquire {
                        ask_handoff,
                        attempts,
                        ..
                    } => (*ask_handoff, *attempts),
                    _ => (false, 0),
                };
                // (Not while a takeover permit names this holder: it is
                // known silent, and the permit's own rule decides when
                // the lease is claimable.)
                let permitted = self.lease.takeover_permit == Some((epoch, holder));
                if ask && self.cfg.p2p && attempts == 0 && holder != self.cfg.node_id && !permitted
                {
                    let req = self.op_id();
                    self.set_phase(
                        Phase::LeaseRequest {
                            req,
                            epoch_mode: false,
                        },
                        None,
                    );
                    self.set_timer(
                        now.plus(self.cfg.handoff_request_timeout_ms),
                        super::Timer::JobRequestTimeout(req),
                        out,
                    );
                    out.push(Action::Send {
                        to: holder,
                        msg: PeerMsg::LeaseRequest { req },
                    });
                    return;
                }
                self.finish_acquire(now, false, replica, out);
            }
            Plan::Claim { takeover, .. } => {
                if takeover {
                    if let Plan::Claim { prev, .. } = &plan {
                        if self.promise_check_needed(now, prev) {
                            let prev = prev.clone();
                            self.set_phase(Phase::PromiseCheck, None);
                            self.pending_plan = Some(plan);
                            self.promise_check_begin(now, &prev, out);
                            return;
                        }
                    }
                    self.set_phase(
                        Phase::Tail {
                            then: TailThen::Takeover,
                        },
                        None,
                    );
                    // Stash the plan for after the tail.
                    self.pending_plan = Some(plan);
                    self.issue_tail(out);
                    return;
                }
                self.acquire_cas(now, plan, out);
            }
        }
    }

    /// Plan 30 §M10: the promise check decided. Allowed: the takeover
    /// proceeds exactly as without it (tail to head, then the CAS naming
    /// the lease object read). Refused: the acquisition fails; the client
    /// op retries on its backoff and asks again.
    pub(crate) fn promise_check_resolved(
        &mut self,
        now: Ms,
        allowed: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if !self
            .job
            .as_ref()
            .is_some_and(|j| matches!(j.phase, Phase::PromiseCheck))
        {
            return;
        }
        if !allowed {
            self.finish_acquire(now, false, replica, out);
            return;
        }
        self.set_phase(
            Phase::Tail {
                then: TailThen::Takeover,
            },
            None,
        );
        self.issue_tail(out);
    }

    fn acquire_cas(&mut self, now: Ms, plan: Plan, out: &mut Vec<Action>) {
        let Plan::Claim { prev, tag, .. } = &plan else {
            return;
        };
        let lease = self.lease.granted_lease(now, &self.cfg, Some(prev));
        let op = self.issue_s3(
            S3Op::LeaseSwap {
                lease: lease.clone(),
                tag: tag.clone(),
            },
            S3For::Job,
            out,
        );
        self.set_phase(
            Phase::Cas {
                plan,
                sent: Box::new(lease),
            },
            Some(op),
        );
    }

    fn acquire_won(
        &mut self,
        now: Ms,
        plan: Plan,
        sent: Lease,
        tag: LeaseTag,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let (prev, takeover, marker) = match plan {
            Plan::Claim {
                prev,
                takeover,
                marker,
                ..
            } => (Some(prev), takeover, marker),
            _ => (None, false, false),
        };
        let lease = sent;
        let epoch = lease.epoch;
        // Plan 30 §M9: this node's own lease at the epoch of a takeover
        // CAS whose outcome was unknown is that takeover having landed:
        // its predecessor is the lease that CAS replaced (the sealed
        // holder's), for the strict-read floor and the backup tail alike.
        let ambiguous = self.lease.ambiguous_claim.take();
        let prev = match (prev, ambiguous) {
            (Some(p), Some((claimed, replaced)))
                if p.holder == self.cfg.node_id && p.epoch == claimed && epoch == claimed =>
            {
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch,
                    prev_holder = replaced.holder,
                    prev_epoch = replaced.epoch,
                    "an earlier takeover CAS had landed; its predecessor stays the one it replaced"
                );
                Some(replaced)
            }
            (p, _) => p,
        };
        tracing::info!(
            node = self.cfg.node_id,
            epoch,
            takeover,
            marker,
            policy = ?lease.ack_policy,
            "acquired the lease"
        );
        replica.set_holder_epoch(epoch);
        // Plan 30 §M9: assess now whether a backup could be had — the
        // ops queued behind this acquisition run before the tenure's first
        // selection pass, and an unassessed tenure holds their
        // acknowledgements until it (`durable_jseq`).
        self.ack.eligible = Some(!self.backup_candidates(now).is_empty());
        // Plan 30 §M9: a takeover of an unexpired lease (a seal, or
        // `ack=s3`) is what the permit allowed; the predecessor's
        // strict-read horizon becomes this tenure's acknowledgement floor
        // once the marker lands, and a backup's tail is re-applied inside
        // the gate.
        self.lease.takeover_permit = None;
        let fast_prev = prev
            .as_ref()
            .filter(|p| {
                takeover && !p.released && !p.is_expired(now.0) && p.holder != self.cfg.node_id
            })
            .map(|p| (p.expires_unix_ms, p.granted_delegations));
        if let Some(p) = prev.as_ref().filter(|_| fast_prev.is_some()) {
            self.note_fast_takeover(p);
        }
        // The sealed backup's tail is re-applied by the tenure that
        // succeeds the epoch it backed: a takeover from that holder, or
        // this node's own lease at the very next epoch — its takeover
        // landed but its gate never ran (it crashed or restarted inside
        // it, backup-crash-slow seed 601692; its CAS reported failure
        // although it applied, 603631). The tail and the role are on
        // disk; nobody else can have held in between.
        let sealed = self.bk.sealed;
        // Plan 30 §M14: a released (unexpired) lease's lock grants may
        // still be honoured: a grace before any new grant.
        if prev.as_ref().is_some_and(|p| {
            takeover && p.released && !p.is_expired(now.0) && p.holder != self.cfg.node_id
        }) {
            self.lock_on_released_takeover(now);
        }
        let backup_tail_epoch = prev
            .as_ref()
            .and_then(|p| {
                self.bk.role.filter(|r| {
                    if p.holder != self.cfg.node_id {
                        takeover && r.epoch == p.epoch && r.holder == p.holder
                    } else {
                        p.epoch == r.epoch + 1 && epoch == p.epoch && sealed >= r.epoch
                    }
                })
            })
            .map(|r| r.epoch);
        // Plan 30 §M3 takeover gate; M13: every acquisition drains the
        // older epochs' inbox before its view opens.
        let gate = PendingGate {
            epoch,
            takeover,
            marker_shipped: !marker,
            drained: !self.cfg.inbox,
            fast_prev,
            backup_tail_epoch,
            shippable: false,
        };
        if takeover {
            self.stats.takeovers += 1;
        }
        self.lease.adopt(now, lease, tag, Some(gate));
        self.shipped_touches.clear();
        self.shipped_floor = self.ship.head_seq;
        self.ship.held_tail_at = Some(now);
        self.acquire_gate(now, replica, out);
    }

    fn acquire_gate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lease.lost {
            // Deposed while the gate's marker or drain was in flight: the
            // gate is void (`mark_lost`), and "no gate" is not "open".
            self.finish_acquire(now, false, replica, out);
            return;
        }
        match self.complete_gate(now, replica, out) {
            GateStep::Done => self.finish_acquire(now, true, replica, out),
            GateStep::NeedMarker => self.issue_marker(0, replica, out),
            GateStep::NeedDrain => self.issue_drain(out),
            GateStep::Failed(error) => {
                tracing::error!(%error, node = self.cfg.node_id, "takeover gate failed; the view stays closed");
                self.finish_acquire(now, false, replica, out);
            }
            GateStep::Wait => {
                tracing::info!(
                    node = self.cfg.node_id,
                    "takeover gate waits out the read-delegation quarantine"
                );
                self.finish_acquire(now, false, replica, out);
            }
        }
    }

    fn finish_acquire(
        &mut self,
        now: Ms,
        acquired: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.pending_plan = None;
        self.promise_check_abandon(out);
        let Some(job) = self.job.take() else {
            return;
        };
        if let What::Acquire { reason, .. } = job.what {
            tracing::debug!(
                node = self.cfg.node_id,
                reason,
                acquired,
                "acquisition finished"
            );
        }
        if acquired {
            self.nudged = true;
        }
        let waiters = std::mem::take(&mut self.acquire_waiters);
        for (op, _) in waiters {
            let (holder, epoch) = if acquired {
                (self.cfg.node_id, self.lease.epoch().unwrap_or(0))
            } else {
                self.lease
                    .last_seen
                    .as_ref()
                    .filter(|l| !l.is_claimable(now.0))
                    .map(|l| (l.holder, l.epoch))
                    .unwrap_or((0, 0))
            };
            out.push(Action::ControlDone {
                op,
                result: Ok(ControlOk::Lease {
                    acquired,
                    holder,
                    epoch,
                }),
            });
        }
        if acquired {
            // Plan 30 §M11: the generations a predecessor left live —
            // known before anything queued behind the takeover executes
            // (phase 2b: the root's own op under an inherited delegation
            // recalls it first, like a forwarded one).
            self.delegation_sync(now, replica, out);
        }
        self.on_acquire_finished(now, acquired, replica, out);
        if acquired {
            self.delegation_sync(now, replica, out);
            // M13/M5: start polling requesters' inboxes now, not at the
            // end of the first round — that round can spend seconds in
            // the upload pass (the write-back delay) with nothing armed.
            self.inbox_holder_tick(now, replica, out);
        }
        self.start_next_job(now, replica, out);
    }

    // ---- handoff / flush (flush then release) ----

    fn begin_handoff(
        &mut self,
        now: Ms,
        req: OpId,
        from: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(epoch) = self.lease.ship_epoch(now, &self.cfg) else {
            self.stats.handoffs_declined += 1;
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LeaseHandoff {
                    req,
                    released: false,
                    epoch: 0,
                    head_seq: None,
                },
            });
            self.start_next_job(now, replica, out);
            return;
        };
        let op = self.op_id();
        self.job = Some(Job {
            what: What::Handoff { req, from, epoch },
            phase: Phase::Upload,
            op: Some(op),
            width: 0,
        });
        out.push(Action::UploadDirtyChunks {
            op,
            ino: None,
            round: false,
            complete: true,
        });
    }

    fn begin_flush(&mut self, control: OpId, stop: bool, out: &mut Vec<Action>) {
        let op = self.op_id();
        self.job = Some(Job {
            what: What::Flush {
                control,
                stop,
                published: false,
            },
            phase: Phase::Upload,
            op: Some(op),
            width: 0,
        });
        out.push(Action::UploadDirtyChunks {
            op,
            ino: None,
            round: false,
            complete: true,
        });
    }

    /// Ship until the journal is empty, publish (a flush, as the holder,
    /// with dirty keys), then release.
    fn flush_continue(
        &mut self,
        now: Ms,
        attempts: u32,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match self.issue_ship(now, attempts, replica, out) {
            Ok(true) => {}
            Ok(false) => {
                let holder = self.lease.ship_epoch(now, &self.cfg);
                if let Some(Job {
                    what: What::Flush { published, .. },
                    ..
                }) = self.job.as_mut()
                {
                    if !*published
                        && self.cfg.publisher
                        && holder.is_some()
                        && replica.has_dirty()
                        && self.publishing.is_none()
                    {
                        *published = true;
                        let op = self.op_id();
                        self.publishing = Some(op);
                        self.ship.shipped_since_publish = 0;
                        self.ship.last_publish = now;
                        let epoch = self.ship.last_ship_epoch.max(holder.unwrap_or(0));
                        out.push(Action::Publish { op, epoch });
                        self.set_phase(Phase::FlushPublish { op }, Some(op));
                        return;
                    }
                }
                if self.lease.held.is_some() && !self.lease.lost {
                    self.lease.begin_handoff_pause(now, &self.cfg);
                    self.release_after_atime(now, replica, out);
                } else {
                    self.finish_flush_job(now, true, replica, out);
                }
            }
            Err(error) => {
                tracing::warn!(node = self.cfg.node_id, %error, "flush before release failed");
                self.finish_flush_job(now, false, replica, out);
            }
        }
    }

    pub(crate) fn on_flush_publish_done(
        &mut self,
        now: Ms,
        op: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if !matches!(job.phase, Phase::FlushPublish { op: o } if o == op) {
            return;
        }
        self.flush_continue(now, 0, replica, out);
    }

    fn finish_flush_job(
        &mut self,
        now: Ms,
        released: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.lease.releasing = false;
        let Some(job) = self.job.take() else {
            return;
        };
        match job.what {
            What::Handoff { req, from, epoch } => {
                if released {
                    self.stats.handoffs_served += 1;
                } else {
                    self.stats.handoffs_declined += 1;
                }
                out.push(Action::Send {
                    to: from,
                    msg: PeerMsg::LeaseHandoff {
                        req,
                        released,
                        epoch,
                        head_seq: released.then_some(self.ship.head_seq),
                    },
                });
            }
            What::Flush { control, stop, .. } => {
                out.push(Action::ControlDone {
                    op: control,
                    result: if released {
                        Ok(ControlOk::Done)
                    } else {
                        Err("flush or release failed".into())
                    },
                });
                if stop {
                    self.stopped = true;
                }
            }
            _ => {}
        }
        self.start_next_job(now, replica, out);
    }

    // ---- results ----

    pub(crate) fn on_job_s3(
        &mut self,
        now: Ms,
        op: OpId,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if job.op != Some(op) {
            return;
        }
        let phase = job.phase.clone();
        let kind = job.kind();
        match (phase, result) {
            // ---- tails ----
            (Phase::Tail { then }, S3Result::SegmentRun(Ok(run))) => {
                match self.apply_run(now, run, replica, out) {
                    Ok(true) => self.issue_tail(out),
                    Ok(false) => self.after_tail(now, then, replica, out),
                    Err(error) => self.job_failed(now, error, replica, out),
                }
            }
            (
                Phase::Tail {
                    then: TailThen::EpochProbe,
                },
                S3Result::SegmentRun(Err(_)),
            ) => {
                // S3 is still away: the epoch stays open.
                self.finish_round(now, None, replica, out);
            }
            (Phase::Tail { .. }, S3Result::SegmentRun(Err(e))) => {
                self.job_failed(now, format!("tailing: {}", e.0), replica, out)
            }
            // ---- renewal ----
            (Phase::Renew { ref sent, .. }, S3Result::LeasePut(Ok(tag))) => {
                if self.lease.held.is_none() {
                    self.lease_gone_mid_job(now, kind, replica, out);
                    return;
                }
                let renewed = (**sent).clone();
                self.lease.renewed(now, renewed, tag);
                self.ship.renew_now = false;
                self.round_gate(now, replica, out);
            }
            (Phase::Renew { retry, .. }, S3Result::LeasePut(Err(CasFailure::Conflict))) => {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::RenewReread { final_probe: retry }, Some(op));
            }
            (Phase::Renew { .. }, S3Result::LeasePut(Err(CasFailure::Failed(e)))) => {
                // A round that cannot reach S3 fails (as `run_sync_round`
                // did): what the driver's continuation-epoch proposal
                // counts, a holder that skips its own tail included.
                self.finish_round(
                    now,
                    Some(format!("lease renewal failed: {e}")),
                    replica,
                    out,
                );
            }
            (Phase::RenewReread { final_probe }, S3Result::LeaseGet(Ok(current))) => {
                let Some((mine, _)) = self.lease.held.clone() else {
                    self.lease_gone_mid_job(now, kind, replica, out);
                    return;
                };
                match current {
                    Some((cur, tag))
                        if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
                    {
                        if final_probe || cur.released || cur.is_expired(now.0) {
                            // Only the tag was stale (a retried PUT landing
                            // twice): adopt it and carry on.
                            self.lease.renewed(now, cur, tag);
                            self.round_gate(now, replica, out);
                        } else {
                            // Lost the CAS to a `wanted_by` edit: renew
                            // against the fresh object.
                            let renewed = self.lease.renewed_lease(now, &self.cfg, &cur);
                            self.lease.renewed(now, cur, tag.clone());
                            let op = self.issue_s3(
                                S3Op::LeaseSwap {
                                    lease: renewed.clone(),
                                    tag,
                                },
                                S3For::Job,
                                out,
                            );
                            self.set_phase(
                                Phase::Renew {
                                    retry: true,
                                    sent: Box::new(renewed),
                                },
                                Some(op),
                            );
                        }
                    }
                    Some((cur, _)) => {
                        self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out);
                        self.set_phase(
                            Phase::Tail {
                                then: TailThen::Recover,
                            },
                            None,
                        );
                        self.issue_tail(out);
                    }
                    None => {
                        self.deposed(now, 0, 0, mine.epoch, replica, out);
                        self.set_phase(
                            Phase::Tail {
                                then: TailThen::Recover,
                            },
                            None,
                        );
                        self.issue_tail(out);
                    }
                }
            }
            (Phase::RenewReread { .. }, S3Result::LeaseGet(Err(e))) => {
                self.finish_round(
                    now,
                    Some(format!("lease re-read failed: {}", e.0)),
                    replica,
                    out,
                );
            }
            // ---- the gate's marker ----
            (Phase::Marker { .. }, S3Result::SegmentPut(Ok(()))) => {
                let seq = self.ship.next_seq;
                if let Err(error) = self.marker_landed(now, seq, replica, out) {
                    self.job_failed(now, error, replica, out);
                    return;
                }
                match kind {
                    JobKind::Acquire => self.acquire_gate(now, replica, out),
                    _ => self.round_gate(now, replica, out),
                }
            }
            (Phase::Marker { attempts }, S3Result::SegmentPut(Err(CasFailure::Conflict))) => {
                if attempts >= COLLISION_RETRIES {
                    self.job_failed(
                        now,
                        "the log kept moving while shipping the epoch marker".into(),
                        replica,
                        out,
                    );
                    return;
                }
                self.set_phase(
                    Phase::Tail {
                        then: TailThen::Marker,
                    },
                    None,
                );
                self.marker_attempts = attempts + 1;
                self.issue_tail(out);
            }
            (Phase::Marker { .. }, S3Result::SegmentPut(Err(CasFailure::Failed(e)))) => {
                tracing::warn!(node = self.cfg.node_id, error = %e, "epoch marker PUT failed; gate stays pending");
                match kind {
                    JobKind::Acquire => self.finish_acquire(now, false, replica, out),
                    _ => self.round_tail(now, replica, out),
                }
            }
            // ---- the gate's inbox drain ----
            (Phase::InboxDrain, S3Result::InboxDrain(Ok(batches))) => {
                match self.inbox_drain(now, batches, replica, out) {
                    Ok(()) => {
                        if let Some(gate) = self.lease.gate.as_mut() {
                            gate.drained = true;
                        }
                        match kind {
                            JobKind::Acquire => self.acquire_gate(now, replica, out),
                            _ => self.round_gate(now, replica, out),
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, node = self.cfg.node_id, "takeover gate: draining older epochs' inbox batches failed; the gate stays pending");
                        match kind {
                            JobKind::Acquire => self.finish_acquire(now, false, replica, out),
                            _ => self.round_tail(now, replica, out),
                        }
                    }
                }
            }
            (Phase::InboxDrain, S3Result::InboxDrain(Err(e))) => {
                tracing::warn!(node = self.cfg.node_id, error = %e.0, "takeover gate: inbox drain LIST failed; the gate stays pending");
                match kind {
                    JobKind::Acquire => self.finish_acquire(now, false, replica, out),
                    _ => self.round_tail(now, replica, out),
                }
            }
            // ---- shipping ----
            (
                Phase::Ship {
                    seqs,
                    seq,
                    payload,
                    epoch,
                    through,
                    atime_inos,
                    purpose,
                    ..
                },
                S3Result::SegmentPut(Ok(())),
            ) => {
                if let Err(error) = self.ship_landed(
                    now,
                    &seqs,
                    seq,
                    (epoch, through),
                    payload,
                    &atime_inos,
                    replica,
                    out,
                ) {
                    self.job_failed(now, error, replica, out);
                    return;
                }
                match purpose {
                    ShipPurpose::Journal => match kind {
                        JobKind::Round => self.round_ship(now, 0, replica, out),
                        _ => self.flush_continue(now, 0, replica, out),
                    },
                    ShipPurpose::AtimeStale => self.round_publish(now, replica, out),
                    ShipPurpose::AtimeBeforeRelease => self.issue_release(now, replica, out),
                }
            }
            (
                Phase::Ship {
                    attempts, purpose, ..
                },
                S3Result::SegmentPut(Err(CasFailure::Conflict)),
            ) => {
                if attempts >= COLLISION_RETRIES {
                    if purpose == ShipPurpose::AtimeBeforeRelease {
                        let _ = replica.drop_atime();
                        self.issue_release(now, replica, out);
                        return;
                    }
                    self.job_failed(
                        now,
                        "the log kept moving while shipping".into(),
                        replica,
                        out,
                    );
                    return;
                }
                self.ship_attempts = attempts + 1;
                self.ship_purpose = purpose;
                self.set_phase(
                    Phase::Tail {
                        then: TailThen::Ship,
                    },
                    None,
                );
                self.issue_tail(out);
            }
            (Phase::Ship { purpose, .. }, S3Result::SegmentPut(Err(CasFailure::Failed(e)))) => {
                match purpose {
                    ShipPurpose::Journal => {
                        self.job_failed(now, format!("shipping log segment: {e}"), replica, out)
                    }
                    ShipPurpose::AtimeStale => {
                        tracing::debug!(node = self.cfg.node_id, error = %e, "standalone atime ship failed; rows stay queued");
                        self.round_publish(now, replica, out);
                    }
                    ShipPurpose::AtimeBeforeRelease => {
                        tracing::debug!(node = self.cfg.node_id, error = %e, "final atime ship failed; dropping rows");
                        let _ = replica.drop_atime();
                        self.issue_release(now, replica, out);
                    }
                }
            }
            // ---- release ----
            (Phase::Release, S3Result::LeasePut(Ok(_))) => {
                let epoch = self.lease.epoch().unwrap_or(0);
                self.lease.released();
                self.deleg_on_lease_gone(now, replica, out);
                replica.set_holder_epoch(0);
                self.stats.releases += 1;
                self.inbox.holder = None;
                tracing::info!(node = self.cfg.node_id, epoch, "released the lease");
                match kind {
                    JobKind::Round => {
                        if let Some(Job {
                            what:
                                What::Round {
                                    epoch_flush_release: true,
                                    ..
                                },
                            ..
                        }) = self.job.as_ref()
                        {
                            out.push(Action::EpochFlushed);
                        }
                        self.finish_round(now, None, replica, out)
                    }
                    _ => self.finish_flush_job(now, true, replica, out),
                }
            }
            (Phase::Release, S3Result::LeasePut(Err(CasFailure::Conflict))) => {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::ReleaseReread, Some(op));
            }
            (Phase::Release, S3Result::LeasePut(Err(CasFailure::Failed(e)))) => {
                tracing::warn!(node = self.cfg.node_id, error = %e, "lease release failed; keeping it");
                self.lease.releasing = false;
                match kind {
                    JobKind::Round => self.finish_round(now, None, replica, out),
                    _ => self.finish_flush_job(now, false, replica, out),
                }
            }
            (Phase::ReleaseReread, S3Result::LeaseGet(result)) => {
                let Some((mine, _)) = self.lease.held.clone() else {
                    self.lease.releasing = false;
                    self.lease_gone_mid_job(now, kind, replica, out);
                    return;
                };
                match result {
                    Ok(Some((cur, tag)))
                        if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
                    {
                        self.lease.renewed(now, cur, tag);
                    }
                    Ok(Some((cur, _))) => {
                        self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out)
                    }
                    Ok(None) => self.deposed(now, 0, 0, mine.epoch, replica, out),
                    Err(e) => {
                        tracing::warn!(node = self.cfg.node_id, error = %e.0, "lease re-read failed")
                    }
                }
                self.lease.releasing = false;
                match kind {
                    JobKind::Round => self.finish_round(now, None, replica, out),
                    _ => self.finish_flush_job(now, false, replica, out),
                }
            }
            // ---- acquire ----
            (Phase::Get, S3Result::LeaseGet(Ok(object))) => {
                self.acquire_classified(now, object, replica, out)
            }
            (Phase::Get, S3Result::LeaseGet(Err(e))) => {
                tracing::warn!(node = self.cfg.node_id, error = %e.0, "lease read failed");
                self.finish_acquire(now, false, replica, out);
            }
            (Phase::Cas { plan, sent }, S3Result::LeasePut(Ok(tag))) => {
                self.acquire_won(now, plan, *sent, tag, replica, out)
            }
            (Phase::Cas { .. }, S3Result::LeasePut(Err(CasFailure::Conflict))) => {
                self.finish_acquire(now, false, replica, out)
            }
            (Phase::Cas { plan, sent }, S3Result::LeasePut(Err(CasFailure::Failed(e)))) => {
                tracing::warn!(node = self.cfg.node_id, error = %e, "lease CAS failed");
                if let Plan::Claim {
                    prev,
                    takeover: true,
                    ..
                } = &plan
                {
                    if prev.holder != self.cfg.node_id {
                        self.lease.ambiguous_claim = Some((sent.epoch, prev.clone()));
                    }
                }
                self.finish_acquire(now, false, replica, out);
            }
            (phase, result) => {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?phase,
                    ?result,
                    "unexpected S3 result for the job phase"
                );
            }
        }
    }

    fn after_tail(
        &mut self,
        now: Ms,
        then: TailThen,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match then {
            TailThen::Recover => match self.recover_deposed(replica, out) {
                Ok(true) => self.round_renew(now, replica, out),
                Ok(false) => {}
                Err(error) => self.finish_round(now, Some(error), replica, out),
            },
            TailThen::Round => self.round_ship(now, 0, replica, out),
            TailThen::EpochProbe => {
                // S3 is reachable. The current epoch holder publishes
                // first; a previous holder keeps its promise open until it
                // has tailed that publication.
                // Plan 30 §M10's claim rule: an S3 holder whose lease the
                // epoch could not carry is the authority too, and closes
                // the epoch the same way (nobody else can: the epoch has
                // no holder to ship past `base`).
                let holds =
                    self.lease.epoch_held() || (self.lease.held.is_some() && !self.lease.lost);
                if !holds && self.ship.head_seq <= self.epoch.base {
                    self.finish_round(now, None, replica, out);
                    return;
                }
                if let Some(Job {
                    what: What::Round { epoch_probed, .. },
                    ..
                }) = self.job.as_mut()
                {
                    *epoch_probed = true;
                }
                let op = self.op_id();
                self.set_phase(Phase::Upload, Some(op));
                out.push(Action::UploadDirtyChunks {
                    op,
                    ino: None,
                    round: true,
                    complete: true,
                });
            }
            TailThen::Marker => {
                if self.lease.gate.is_none() {
                    // The tail deposed this node (a newer epoch's segment:
                    // the lease was taken while it retried), which voided
                    // the gate: there is no marker to ship. Finish the job
                    // like a failed marker PUT does, or it holds the slot
                    // for good and no round ever tails again (the M12
                    // coder's backup-crash seed 892).
                    match self.job.as_ref().map(|j| j.kind()) {
                        Some(JobKind::Acquire) => self.finish_acquire(now, false, replica, out),
                        _ => self.round_tail(now, replica, out),
                    }
                    return;
                }
                let attempts = self.marker_attempts;
                self.issue_marker(attempts, replica, out);
            }
            TailThen::Ship => {
                let attempts = self.ship_attempts;
                match self.ship_purpose {
                    ShipPurpose::Journal => match self.job.as_ref().map(|j| j.kind()) {
                        Some(JobKind::Round) => self.round_ship(now, attempts, replica, out),
                        _ => self.flush_continue(now, attempts, replica, out),
                    },
                    purpose => {
                        if !self.issue_atime_ship(now, attempts, purpose, replica, out) {
                            match purpose {
                                ShipPurpose::AtimeBeforeRelease => {
                                    self.issue_release(now, replica, out)
                                }
                                _ => self.round_publish(now, replica, out),
                            }
                        }
                    }
                }
            }
            TailThen::Takeover => {
                let Some(plan) = self.pending_plan.take() else {
                    self.finish_acquire(now, false, replica, out);
                    return;
                };
                self.acquire_cas(now, plan, out);
            }
            TailThen::CatchUp => {
                // Plan 30 §M2: reach the departing holder's head before
                // claiming (bounded), so the coverage rule is exact.
                let again = match self.job.as_mut().map(|j| &mut j.what) {
                    Some(What::Acquire {
                        catch_up: Some((target, attempts)),
                        ..
                    }) if self.ship.head_seq < *target && *attempts < CATCH_UP_ATTEMPTS => {
                        *attempts += 1;
                        true
                    }
                    _ => false,
                };
                if again {
                    self.issue_tail(out);
                    return;
                }
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::Get, Some(op));
            }
            TailThen::Control => {
                if let Some(Job {
                    what: What::TailToHead { control },
                    ..
                }) = self.job.take()
                {
                    out.push(Action::ControlDone {
                        op: control,
                        result: Ok(ControlOk::Done),
                    });
                    self.read_tail_done(control, &Ok(ControlOk::Done), out);
                }
                self.start_next_job(now, replica, out);
            }
        }
    }

    /// Plan 30 §M9: the lease went away while a renewal or release had
    /// an S3 request in flight — `apply_incoming` deposes the holder at
    /// once on a higher epoch's segment (a fast takeover's fence), which
    /// before M9 only the CAS path itself could do. A deposed node runs
    /// the recovery from here (the round's `Recover` tail, as the CAS
    /// path would); otherwise the job just ends.
    fn lease_gone_mid_job(
        &mut self,
        now: Ms,
        kind: JobKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        tracing::debug!(
            node = self.cfg.node_id,
            lost = self.lease.lost,
            "lease gone while its renewal or release was in flight"
        );
        if self.lease.lost && kind == JobKind::Round {
            self.set_phase(
                Phase::Tail {
                    then: TailThen::Recover,
                },
                None,
            );
            self.issue_tail(out);
            return;
        }
        match kind {
            JobKind::Round => self.finish_round(now, None, replica, out),
            _ => self.finish_flush_job(now, false, replica, out),
        }
    }

    fn job_failed(&mut self, now: Ms, error: String, replica: &dyn Replica, out: &mut Vec<Action>) {
        match self.job.as_ref().map(|j| j.kind()) {
            Some(JobKind::Round) => self.finish_round(now, Some(error), replica, out),
            Some(JobKind::Acquire) => {
                tracing::warn!(node = self.cfg.node_id, %error, "acquisition failed");
                self.finish_acquire(now, false, replica, out);
            }
            Some(JobKind::Handoff) | Some(JobKind::Flush) => {
                tracing::warn!(node = self.cfg.node_id, %error, "flush failed");
                self.finish_flush_job(now, false, replica, out);
            }
            Some(JobKind::TailToHead) => {
                if let Some(Job {
                    what: What::TailToHead { control },
                    ..
                }) = self.job.take()
                {
                    let result = Err(error);
                    self.read_tail_done(control, &result, out);
                    out.push(Action::ControlDone {
                        op: control,
                        result,
                    });
                }
                self.start_next_job(now, replica, out);
            }
            None => {}
        }
    }

    /// A peer answered the acquisition's `LeaseRequest`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_lease_handoff(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        released: bool,
        epoch: Epoch,
        head_seq: Option<Seq>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let _ = from;
        let Some(job) = self.job.as_ref() else {
            return;
        };
        let Phase::LeaseRequest { req: r, epoch_mode } = job.phase else {
            return;
        };
        if r != req {
            return;
        }
        if !released {
            self.finish_acquire(now, false, replica, out);
            return;
        }
        if epoch_mode {
            // A continuation epoch's P2P-only handoff: local authority,
            // gated like a takeover (no marker: nothing ships). Plan 30
            // §M10: the predecessor hands over only with an empty journal
            // (`on_lease_request`).
            let mine = self.lease.epoch().unwrap_or(epoch).max(1);
            self.lease.adopt_epoch_hold(now, mine);
            replica.set_holder_epoch(0);
            self.lease.gate = Some(PendingGate {
                epoch: mine,
                takeover: false,
                marker_shipped: true,
                drained: true,
                fast_prev: None,
                backup_tail_epoch: None,
                shippable: false,
            });
            self.acquire_gate(now, replica, out);
            return;
        }
        if let Some(Job {
            what: What::Acquire {
                attempts, catch_up, ..
            },
            ..
        }) = self.job.as_mut()
        {
            *attempts += 1;
            *catch_up = head_seq.map(|h| (h, 0));
        }
        // Catch up to the departing holder's head before claiming, so the
        // gate never strands a shadow whose confirming segment is merely
        // not applied yet (plan 30 §M3a), and the coverage rule is exact.
        self.set_phase(
            Phase::Tail {
                then: TailThen::CatchUp,
            },
            None,
        );
        self.issue_tail(out);
    }

    /// Plan 30 §M9: a takeover permit arrived (a sealed backup's, or the
    /// `ack=s3` silence rule's) while an acquisition waits for the silent
    /// holder's handoff answer — a wait of `handoff_request_timeout_ms`
    /// that would otherwise delay the takeover by that much. Stop waiting
    /// and re-classify now: with the permit the unexpired lease is
    /// claimable. A late answer or the request's timer find the phase
    /// gone and are ignored.
    pub(crate) fn permit_interrupts_handoff_wait(&mut self, out: &mut Vec<Action>) {
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if job.kind() != JobKind::Acquire
            || !matches!(
                job.phase,
                Phase::LeaseRequest {
                    epoch_mode: false,
                    ..
                }
            )
        {
            return;
        }
        let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
        self.set_phase(Phase::Get, Some(op));
    }

    pub(crate) fn on_job_peer_failed(
        &mut self,
        now: Ms,
        req: OpId,
        to: NodeId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let _ = to;
        let Some(job) = self.job.as_ref() else {
            return;
        };
        if matches!(job.phase, Phase::LeaseRequest { req: r, .. } if r == req) {
            self.finish_acquire(now, false, replica, out);
        }
    }
}

/// Plan 30 §M9 × §M6: an epoch marker's records — none, or the
/// announcement that this tenure re-ships its predecessor's acknowledged
/// backup tail (`LogRecord::TailFollows`), which keeps readers' older
/// observations waiting past the marker.
fn marker_records(gate: &PendingGate) -> Vec<LogRecord> {
    gate.backup_tail_epoch
        .map(|prev_epoch| LogRecord::TailFollows { prev_epoch })
        .into_iter()
        .collect()
}
