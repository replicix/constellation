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
use crate::event::{CasFailure, Control, PeerMsg, S3Failure, S3Result, UploadResult};
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq};
use crate::replica::Replica;
use crate::segment;
use constellation_fs_core::Ino;
use constellation_meta::{JournalPos, LogRecord};
use constellation_store_s3::{Lease, LeaseTag};
use constellation_types::Code;

/// EC2 campaign 8 A-1: the reason of an acquisition started because a
/// peer forwarded an op while the lease still names this node (see
/// `Core::readopt_own_lease_for_forward`). It claims only that lease — its
/// own, unreleased — and gives up on anything else.
pub(crate) const READOPT_REASON: &str = "readopt-for-forward";

/// How many whole sync rounds (each opening with a complete upload pass)
/// a `Control::Barrier` may see end with its rows still unshipped before
/// it fails. Its rows ship within one such round unless they cannot
/// (held back behind a chunk that cannot be uploaded, plan 30 §M4) or a
/// queued job cut the round short; the caller retries a failure. The
/// bound counts rounds that *end*: under a writer that never lets the
/// round end, a barrier behind a held-back row never reaches it and waits
/// for its caller's own timeout instead.
const BARRIER_ROUNDS: u32 = 3;

/// A `Control::Barrier` waiting for this node's journal to ship through
/// `upto`, the journal's tip when it was admitted.
///
/// Fix (snap-drain-busy): a barrier used to be answered at the end of a
/// round, and only if the *whole* journal was empty then. A holder whose
/// own clients keep writing never has an empty journal once S3 adds a
/// few tens of milliseconds per request — a round ships until the journal
/// runs dry, which it then never does — so a snapshot's drain on a busy
/// holder took 10–45 s or failed ("journal not shipped: no lease", while
/// holding the lease). What a barrier promises is narrower: everything
/// journaled before it asked is in the log. So it waits for exactly
/// that, and is answered as soon as the segment that carries its last
/// row lands, mid-round, whatever was journaled since.
#[derive(Debug)]
pub(crate) struct BarrierWait {
    op: OpId,
    upto: u64,
    /// Rounds begun since it was admitted.
    rounds: u32,
}

impl BarrierWait {
    pub(crate) fn new(op: OpId, upto: u64) -> BarrierWait {
        BarrierWait {
            op,
            upto,
            rounds: 0,
        }
    }
}

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
        /// When the request arrived (see `begin_handoff`'s staleness rule).
        at: Ms,
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
    /// A `SegmentGap` outstanding after an empty tail: is the cursor at
    /// the head, or past the retained log?
    GapCheck { then: TailThen },
    /// The replica is being rebuilt from the head commit after a
    /// retention gap (`Action::RebuildReplica` outstanding).
    RetentionRebuild { op: OpId },
    /// `LeaseSwap(renewed)` outstanding (`retry`: after a `wanted_by`
    /// edit was re-read).
    Renew {
        retry: bool,
        /// Issued between two segments of a round's ship loop (the
        /// renewal came due while it shipped): the loop resumes after it.
        mid_ship: bool,
        /// Plan 30 §M10: the object the swap writes. On success the held
        /// lease becomes exactly it (it used to be recomputed at the
        /// result's time, an expiry later than the object's by the
        /// round trip — which also broke the carried-lease match of an
        /// epoch's flush re-claim).
        sent: Box<Lease>,
    },
    /// `LeaseGet` after a lost renewal CAS (`final_probe`: the second
    /// swap lost too, so this read is the deposition probe).
    RenewReread { final_probe: bool, mid_ship: bool },
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
    /// `LeaseGet` after a release CAS conflict, or after a release CAS
    /// that failed without an answer (`in_doubt`: it may have landed).
    ReleaseReread { in_doubt: bool },
    /// A deposition recovery's `RebuildReplica` outstanding.
    Recover { op: OpId },
    /// Acquire: `LeaseGet` outstanding.
    Get,
    /// Acquire: the CAS outstanding, and the object it writes (adopted
    /// exactly on success — see `Renew::sent`).
    Cas { plan: Plan, sent: Box<Lease> },
    /// Acquire: `LeaseGet` after a CAS that failed without an answer (it
    /// may have landed: `CasFailure::Failed`).
    CasReread { plan: Plan, sent: Box<Lease> },
    /// Acquire: `LeaseRequest` sent to `holder`; waiting for its answer.
    /// `epoch_mode`: a continuation epoch's P2P-only handoff.
    LeaseRequest { req: OpId, epoch_mode: bool },
    /// Plan 30 §M10: a TTL takeover's promise check (`promise.rs`); the
    /// plan waits in `pending_plan`.
    PromiseCheck,
    /// A member's epoch probe found S3 back: `LeaseGet` outstanding, to
    /// learn whether the carried lease is still the epoch's (see
    /// `epoch_carrier_checked`).
    EpochCarrierCheck,
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
        // A queued re-adoption (A-1) gives way to an ordinary acquisition
        // coalescing with it: that one claims whatever is claimable.
        if let JobReq::Acquire {
            reason,
            ask_handoff,
        } = &req
        {
            if *reason != READOPT_REASON {
                for q in self.queued_jobs.iter_mut() {
                    if let JobReq::Acquire {
                        reason: queued,
                        ask_handoff: queued_ask,
                    } = q
                    {
                        if *queued == READOPT_REASON {
                            *queued = reason;
                            *queued_ask = *ask_handoff;
                        }
                    }
                }
            }
        }
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

    /// An acquisition is in the slot or queued for it.
    pub(crate) fn acquiring(&self) -> bool {
        self.job
            .as_ref()
            .is_some_and(|j| j.kind() == JobKind::Acquire)
            || self
                .queued_jobs
                .iter()
                .any(|q| matches!(q, JobReq::Acquire { .. }))
    }

    /// A-1: the acquisition in the slot is a re-adoption for a forward
    /// (`READOPT_REASON`), its takeover gate included.
    pub(crate) fn readopting(&self) -> bool {
        matches!(
            self.job.as_ref().map(|j| &j.what),
            Some(What::Acquire {
                reason: READOPT_REASON,
                ..
            })
        )
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
                JobReq::Round { poll_triggered } => self.begin_round(poll_triggered, out),
                JobReq::Acquire {
                    reason,
                    ask_handoff,
                } => self.begin_acquire(now, reason, ask_handoff, replica, out),
                JobReq::Handoff { req, from, at } => {
                    self.begin_handoff(now, req, from, at, replica, out)
                }
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
            // (An acquisition waiting for the holder's answer to its
            // lease request touches no replica state until the answer
            // comes, and then tails anyway: a claim of a placement offer
            // must not freeze this node's view of the holder's writes
            // for as long as the holder takes to answer — up to the
            // request timeout, `visibility-s3-latency`.)
            Some(job) => matches!(
                job.phase,
                Phase::Upload | Phase::Tail { .. } | Phase::LeaseRequest { .. }
            ),
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

    // ---- the retention gap check (DESIGN.md §14 "Falling behind
    // segment GC") ----
    //
    // A GET-next `404` cannot tell "at head" from "pruned past": GC
    // deletes segments below the head commit's `applied` minus the
    // retention window once they are old enough, and a replica that was
    // offline (or partitioned) past that point probes a deleted slot for
    // ever, believing itself current — and, worse, could CAS-create its
    // takeover marker into that deleted slot and fork the log. One LIST
    // with offset answers exactly (`S3Op::SegmentGap`): nothing at or
    // after the cursor means head; a later segment means the range was
    // pruned and the replica must be rebuilt from the head commit.
    //
    // When it runs: always before a takeover CAS, unless the acquisition
    // reached a handoff's reported head (segments above a holder's head
    // are above every floor any round could have used: floors come from
    // commits, commits from holders, and the CAS fences any later
    // holder). For a follower's rounds: when a gossip hint or the stream's
    // reported head lies at or past the cursor (rate-limited), and as a
    // backstop every `gap_check_ms` — plus once on the first empty probe
    // after a mount. A holder never checks: it is the sole appender.

    /// Whether the empty tail just applied should be followed by a LIST
    /// before `then` proceeds.
    fn gap_check_due(&self, now: Ms, then: TailThen) -> bool {
        if self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.lost {
            return false;
        }
        let next = self.ship.next_seq;
        match then {
            TailThen::Takeover | TailThen::CatchUp => {
                !self.catch_up_reached() && !self.readopting_own_lease()
            }
            TailThen::Round | TailThen::Control | TailThen::Recover => {
                let since = self
                    .gap_checked_at
                    .map(|at| now.since(at))
                    .unwrap_or(i64::MAX);
                let hinted = self.stream_hinted() >= next
                    || self.stream_head().is_some_and(|head| head >= next);
                (hinted && since >= self.cfg.gap_hint_check_ms as i64)
                    || since >= self.cfg.gap_check_ms as i64
            }
            TailThen::EpochProbe | TailThen::Marker | TailThen::Ship => false,
        }
    }

    /// M4's re-adoption of this node's own lease after a restart: the
    /// object still names this node, so nobody else has appended since it
    /// last did, and its cursor is the head. Exact, and not a clock
    /// judgment: had another node taken the lease, the object would name
    /// that node.
    fn readopting_own_lease(&self) -> bool {
        matches!(
            &self.pending_plan,
            Some(Plan::Claim { prev, .. }) if prev.holder == self.cfg.node_id
        )
    }

    /// An acquisition that caught up to a handoff's reported head: the
    /// cursor is provably above every retention floor.
    fn catch_up_reached(&self) -> bool {
        matches!(
            self.job.as_ref().map(|j| &j.what),
            Some(What::Acquire {
                catch_up: Some((target, _)),
                ..
            }) if self.ship.head_seq >= *target
        )
    }

    fn issue_gap_check(&mut self, then: TailThen, out: &mut Vec<Action>) {
        let from = self.ship.next_seq;
        let op = self.issue_s3(S3Op::SegmentGap { from }, S3For::Job, out);
        self.set_phase(Phase::GapCheck { then }, Some(op));
    }

    fn after_gap_check(
        &mut self,
        now: Ms,
        then: TailThen,
        first: Option<Seq>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.gap_checked_at = Some(now);
        let next = self.ship.next_seq;
        match first {
            None => {
                // At head. A catch-up whose target lies beyond what exists
                // (the departing holder counted a segment that never
                // landed) is complete as far as the log goes.
                if let Some(What::Acquire {
                    catch_up: Some((target, _)),
                    ..
                }) = self.job.as_mut().map(|j| &mut j.what)
                {
                    *target = (*target).min(self.ship.head_seq);
                }
                self.after_tail(now, then, replica, out);
            }
            Some(first) if first <= next => {
                // A segment landed between the probe and the LIST.
                self.set_phase(Phase::Tail { then }, None);
                self.issue_tail(out);
            }
            Some(first) => {
                tracing::error!(
                    node = self.cfg.node_id,
                    cursor = next,
                    first_retained = first,
                    journal = replica.journal_len().unwrap_or(0),
                    "the log was pruned past this replica's position; \
                     rebuilding the replica from the head commit"
                );
                self.stats.retention_gaps += 1;
                self.pending_plan = None;
                let op = self.op_id();
                self.set_phase(Phase::RetentionRebuild { op }, Some(op));
                out.push(Action::RebuildReplica { op });
            }
        }
    }

    /// The retention rebuild finished: the cursor restarts from the
    /// rebuilt replica's applied position (the head commit's, plus the
    /// retained log after it) and the job ends — a round quietly, an
    /// acquisition unacquired (its op retries from the new position), a
    /// tail-to-head answered.
    fn retention_rebuild_done(
        &mut self,
        now: Ms,
        ok: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let error = if ok {
            let applied = replica.applied_seq().unwrap_or(0);
            self.ship.next_seq = applied + 1;
            self.ship.head_seq = applied;
            replica.clear_streamed();
            tracing::warn!(
                node = self.cfg.node_id,
                applied,
                "replica rebuilt from the head commit after a log retention gap"
            );
            None
        } else {
            Some("rebuilding the replica after a log retention gap failed".to_string())
        };
        match self.job.as_ref().map(|j| j.kind()) {
            Some(JobKind::Acquire) => self.finish_acquire(now, false, replica, out),
            Some(JobKind::TailToHead) => {
                if let Some(Job {
                    what: What::TailToHead { control },
                    ..
                }) = self.job.take()
                {
                    let result = match error {
                        None => Ok(ControlOk::Done),
                        Some(error) => Err(error),
                    };
                    out.push(Action::ControlDone {
                        op: control,
                        result: result.clone(),
                    });
                    self.read_tail_done(control, &result, out);
                }
                self.start_next_job(now, replica, out);
            }
            _ => self.finish_round(now, error, replica, out),
        }
    }

    /// Apply a probed run. `Ok(true)` when the run was saturated (the
    /// caller probes again).
    ///
    /// `round`: a round's tail, which a live log stream may race — a
    /// tail leaves the cursor free, so the stream applies what arrives
    /// while the GETs are out. A run whose last segment the stream had
    /// already applied is then no sign that S3 is ahead of this node:
    /// the stream reached the run's end first, and this round's follow-up
    /// would be a full-width run of 404s (`visibility-after-burst`: the
    /// one-GET backstop probe "hit" the marker the stream had just
    /// delivered, and 16 GETs followed — 17 tail GETs in about one run
    /// in three). An acquisition's tail keeps probing on any full run:
    /// it must reach the head before its CAS.
    fn apply_run(
        &mut self,
        now: Ms,
        run: Vec<(Seq, Vec<u8>)>,
        round: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Result<bool, String> {
        let width = self.job.as_ref().map(|j| j.width).unwrap_or(1);
        let n = run.len();
        let mut last_new = false;
        for (seq, payload) in run {
            last_new = seq >= self.ship.next_seq;
            self.apply_incoming(now, seq, &payload, replica, out)
                .map_err(|e| e.to_string())?;
        }
        if n > 0 {
            self.stats.segments_applied += n as u64;
            self.answer_awaiting_log(now, replica, out);
        }
        Ok(n >= width && n > 0 && (last_new || !round))
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
            // A newer tenure strands what the old one streamed ahead: the
            // stream's watermark no longer stands for this replica.
            if applied.stranded.any() || seg.epoch > self.ship.max_epoch {
                replica.clear_streamed();
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
        if !seqs.is_empty() {
            self.answer_shipped_barriers(replica, out);
        }
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
        // Whatever the old tenure streamed ahead is stranded or re-shipped
        // under this one.
        replica.clear_streamed();
        if gate.takeover {
            // With a predecessor's backup tail to re-apply, this node's
            // own rows as a delegate too: they must follow the tail, not
            // precede it (`strand_for_takeover`). Once: the tail is
            // cleared below, and a later pass of the gate (waiting for
            // the drain) must not strand the tail's own re-journaled rows.
            let own_gens: Vec<u64> = if gate.backup_tail_epoch.is_some() {
                self.dl.mine.keys().copied().collect()
            } else {
                Vec::new()
            };
            match replica.strand_for_takeover(gate.epoch, &own_gens) {
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

    /// The job's phase, for diagnostics (the simulation's quiescence
    /// report): its `Debug` form, without payloads.
    pub fn job_phase(&self) -> Option<String> {
        self.job.as_ref().map(|j| {
            let text = format!("{:?}", j.phase);
            text.split(['{', '('])
                .next()
                .unwrap_or(&text)
                .trim()
                .to_string()
        })
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
        self.fail_barriers(out);
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
                    | Phase::ReleaseReread { .. }
                    | Phase::Cas { .. }
                    | Phase::CasReread { .. }
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
        replica.clear_streamed();
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
        if matches!(job.phase, Phase::RetentionRebuild { op: o } if o == op) {
            self.retention_rebuild_done(now, ok, replica, out);
            return;
        }
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

    fn begin_round(&mut self, poll_triggered: bool, out: &mut Vec<Action>) {
        self.nudged = false;
        for barrier in &mut self.barriers {
            barrier.rounds += 1;
        }
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
            // member is missing) or not, every member probes: the hold's
            // owner flushes and closes like an active epoch's holder
            // would (plan 30 §M10 — a member that never returns must not
            // keep the journal out of S3 forever; the flush's re-claim
            // CAS is the arbiter, and it fails if an admin leave fenced
            // the lease), and a member closes once the carried lease has
            // moved (`epoch_carrier_checked`), which it learns only by
            // asking S3. (Fix "S3 client recovery after a cut": a frozen
            // member used to skip its probe, so it stayed in the epoch —
            // applying nothing, its stream from the owner ended by the
            // close — until the missing member returned and unfroze it:
            // `epoch-member-dies-with-chunk`'s B.) A frozen epoch that
            // carries no lease has no hold owner either: every member
            // probes, or nobody would ever close it (the follow-up (d)
            // hang: S3 back, the cluster frozen for good).
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
            complete: !self.round_waiters.is_empty()
                || !self.barriers.is_empty()
                || self.publish_forced,
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
                            // And the carried lease is re-claimed, whether
                            // or not anything waits to ship: members close
                            // only once it has moved (`epoch_reclaim_due`).
                            self.end_epoch_hold(replica, true);
                        } else if self.owes_carried_move() {
                            // The owed close (`owes_move`): it owes the
                            // carried lease's re-claim as a hold owner
                            // would, should the release not have landed.
                            self.end_epoch_hold(replica, true);
                        }
                        out.push(Action::EpochClose);
                        self.skip_ship = false;
                        self.epoch_close_release(now, replica, out);
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

    /// Whether the held lease should be renewed now: half its TTL is
    /// gone (or a deposition hint asked for it). Never under an epoch
    /// hold: the epoch carries exactly the lease object, and its other
    /// members close once S3 shows another (`epoch_carrier_checked`), so a
    /// renewal that lands changes what they wait on while this node still
    /// holds the epoch (chunk epoch-liveness-gap, `locks-blips-tight-faults`
    /// seed 409: a round begun before the activation renewed the lease it
    /// held again, the renewal landed as S3 was cut, the hold owner
    /// crashed, and the members closed and took the lease over at its
    /// expiry beside the hold it re-adopted at its restart). The hold
    /// needs no S3 lease; the flush's re-claim after the close renews it.
    fn renew_due(&self, now: Ms) -> bool {
        !self.lease.lost
            && !self.lease.epoch_held()
            && match &self.lease.held {
                Some((lease, _)) => {
                    self.ship.renew_now
                        || lease.expires_in_ms(now.0) <= (self.cfg.ttl_ms / 2) as i64
                }
                None => false,
            }
    }

    fn issue_renew(&mut self, now: Ms, mid_ship: bool, out: &mut Vec<Action>) {
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
                mid_ship,
                sent: Box::new(renewed),
            },
            Some(op),
        );
    }

    fn round_renew(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.renew_due(now) {
            self.issue_renew(now, false, out);
            return;
        }
        self.round_gate(now, replica, out);
    }

    /// Where a round goes after its renewal: the gate (the renewal
    /// opened the round), or back into the ship loop it interrupted.
    fn round_after_renew(
        &mut self,
        now: Ms,
        mid_ship: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if mid_ship {
            self.round_ship(now, 0, replica, out);
        } else {
            self.round_gate(now, replica, out);
        }
    }

    fn round_gate(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.lease.gate.is_some() && !self.lease.lost {
            match self.complete_gate(now, replica, out) {
                GateStep::Done => {
                    // The gate opened in a round, not in the acquisition
                    // (its marker PUT failed there, or it waited for the
                    // quarantine or the drain): the root is usable only
                    // now, so the generations a predecessor left live are
                    // learned here — `finish_acquire` saw a closed gate
                    // (long-delegated seed 70705: never inherited, the
                    // delegate's stream refused for good and its
                    // acknowledged writes never appended).
                    self.delegation_sync(now, replica, out);
                }
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
        // The holder's periodic look at its own stream (`held_tail_
        // staleness_ms`): nobody else may have appended, so one GET of
        // the next sequence answers it (EC2 finding R2-2: a full-width
        // probe cost an idle holder ~100 GET-404s a minute); a hit is
        // followed by a full-width run like any saturated probe.
        if self.lease.ship_epoch(now, &self.cfg).is_some() && !self.lease.epoch_held() {
            self.set_phase(
                Phase::Tail {
                    then: TailThen::Round,
                },
                None,
            );
            self.issue_tail_width(1, out);
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
                } else if self.lease.gate.is_none()
                    && (self.epoch_reclaim_due()
                        || (!self.epoch.open
                            && self.epoch_reclaim_pending(now)
                            && replica.locks().live_grants_len(now.0) > 0))
                    && !self
                        .queued_jobs
                        .iter()
                        .any(|j| matches!(j, JobReq::Acquire { .. }))
                {
                    // Or the lease a close let go with lock grants kept
                    // under it: their holders' renewals wait for it,
                    // whether or not this node owed the carried lease's
                    // re-claim (an S3 holder the epoch did not carry, or
                    // a re-claim whose CAS was in doubt and landed).
                    self.queued_jobs.push_back(JobReq::Acquire {
                        reason: "epoch-close-reclaim",
                        ask_handoff: false,
                    });
                }
                self.dead_root_check(now, out);
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

    /// A root that dies with no backup is replaced only by a TTL
    /// takeover, and only the lease path starts one. An op a sequencer
    /// accepted that waits for the log to carry it (`AwaitingLog`: the
    /// root's append, or a delegate's stream appended by the root) is on
    /// no path at all: with the root dead it waited out its client
    /// deadline (2 × TTL) before its retry took the lease path. So a
    /// node with such an op older than the inbox's P2P grace reads the
    /// lease each round, and takes the root over once the lease ran out
    /// unrenewed (`on_dead_root_get`); a live root's lease never does,
    /// and nothing is asked of a live holder. A lease last seen live is
    /// not read again before the expiry it showed (a holder that is only
    /// slow renews before then), so the check costs about one read per
    /// renewal period, not one per round; nor while suspended, where the
    /// acquisition is refused anyway.
    pub(crate) fn dead_root_check(&mut self, now: Ms, out: &mut Vec<Action>) {
        // (This node's own unexpired lease, an epoch close's pending
        // re-claim, is no dead root's either.)
        let seen_live = self
            .lease
            .last_seen
            .as_ref()
            .is_some_and(|l| l.holder != 0 && !l.released && !l.is_expired(now.0));
        if self.dead_root_get
            || seen_live
            || self.mode.suspended
            || self.lease.held.is_some()
            || self.lease.lost
            || self.lease.gate.is_some()
            || self.epoch.active
            || self.lease.epoch_held()
            || !self.awaits_dead_root(now)
            || self
                .queued_jobs
                .iter()
                .any(|j| matches!(j, JobReq::Acquire { .. }))
        {
            return;
        }
        self.dead_root_get = true;
        self.issue_s3(S3Op::LeaseGet, S3For::DeadRoot, out);
    }

    fn awaits_dead_root(&self, now: Ms) -> bool {
        let grace = self.cfg.inbox_p2p_grace_ms as i64;
        self.clients.values().any(|c| {
            matches!(c.phase, super::client::Phase::AwaitingLog { .. })
                && now.since(c.submitted) >= grace
        })
    }

    pub(crate) fn on_dead_root_get(
        &mut self,
        now: Ms,
        result: S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.dead_root_get = false;
        let S3Result::LeaseGet(Ok(Some((lease, _)))) = result else {
            return;
        };
        self.lease.note_object(now, &lease);
        // Only a lease that ran out unrenewed is a dead root's: a released
        // one was let go with everything shipped (the op's records come
        // with the next tail), and taking it would only move it.
        if lease.holder == 0
            || lease.holder == self.cfg.node_id
            || lease.released
            || !lease.is_expired(now.0)
            || self.lease.held.is_some()
            || !self.awaits_dead_root(now)
        {
            return;
        }
        self.stats.dead_root_acquires += 1;
        tracing::warn!(
            node = self.cfg.node_id,
            holder = lease.holder,
            epoch = lease.epoch,
            "ops wait for a root whose lease ran out: taking the root over"
        );
        self.enqueue_job(
            now,
            JobReq::Acquire {
                reason: "dead-root",
                ask_handoff: false,
            },
            replica,
            out,
        );
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
        // lease taken for it can go — unless lock grants are live under
        // it: then the flush is over and the lease stays, as an idle
        // holder's would (below). Releasing dropped every grant a moment
        // after the close kept them (`epoch_close_release`), fencing the
        // lock holders with `EIO` all the same.
        let flushed = self.epoch.flushing
            && self.lease.held.is_some()
            && !self.lease.lost
            && !self.lease.epoch_held()
            && backlog == 0;
        if flushed && replica.locks().live_grants_len(now.0) > 0 {
            out.push(Action::EpochFlushed);
        } else if flushed {
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
        // Plan 30 §M14: nor while the table holds any grant, expired ones
        // included (a successor would start a grace; the holders keep their
        // locks instead). Counting only live grants here let a fresh
        // takeover idle-release with waiters parked behind expired entries
        // and left two non-holders pointing at each other
        // (`locks-failover-backup` seed 1593).
        if replica.locks().grants_len() > 0 {
            self.finish_round(now, None, replica, out);
            return;
        }
        // EC2 finding 2: the wanters are across a P2P partition from a
        // holder its own side is using: the lease stays (they keep being
        // served through the inbox).
        if self.keeps_lease_for_p2p_side(now) {
            self.finish_round(now, None, replica, out);
            return;
        }
        if self
            .lease
            .idle_release_due(now, &self.cfg, backlog, self.held_back)
        {
            tracing::info!(node = self.cfg.node_id, "idle-releasing the lease");
            self.lease.releasing = true;
            self.begin_handoff_pause(now);
            self.release_after_atime(now, replica, out);
            return;
        }
        if backlog > 0 {
            // Force the backlog to zero: close the fast path for a while
            // so the next round can drain and release — but follow up
            // at once only if this round moved the log. A backlog that
            // did not ship is one the plan defers or holds (a manifest
            // waiting for a chunk only another node has, `store::held`),
            // and a round that ships nothing re-run with no delay is a
            // busy loop for as long as that node is away (fix "capture
            // under an epoch hold": flex seed 1007's holder ran 684 433
            // rounds at one instant). The poll retries at its cadence,
            // and the chunk's arrival nudges nothing it needs to.
            let moved = matches!(
                self.job.as_ref().map(|j| &j.what),
                Some(What::Round { head_before, .. }) if self.ship.head_seq != *head_before
            );
            self.begin_handoff_pause(now);
            if moved {
                self.nudged = true;
            }
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
        // Durable before the CAS goes out: a restart after it landed must
        // know the lease was let go (`PromiseState::released`), or an
        // activation carrying it would hold an epoch on a lease anyone
        // may claim without promises. Not persisted, it does not go out.
        if !self.record_release(&lease, replica) {
            self.lease.releasing = false;
            self.job_failed(now, "persisting the released lease".into(), replica, out);
            return;
        }
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
        self.epoch_hold_after_kept_release(now, replica);
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
                (None, req) => Err(format!("{req:?} is not answered by a sync round")),
            };
            out.push(Action::ControlDone { op, result });
        }
        self.finish_barriers(now, failed.as_deref(), replica, out);
        if !self.round_waiters.is_empty() || !self.barriers.is_empty() {
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

    /// Whether `barrier`'s rows are all in the log: none at or below its
    /// position is left in the journal, and none left it because a
    /// deposition stranded it.
    ///
    /// A deposition takes rows out of the journal without shipping them
    /// (`recover_deposed` strands them and queues them for replay by rid
    /// to the new holder), so an emptied journal proves nothing while
    /// this node is deposed or still has replays to run. Three guards
    /// cover it: a barrier is refused while `lease.lost` (`on_control`),
    /// every waiting barrier fails as the node is deposed
    /// ([`Self::fail_barriers`]) — before the recovery round strands
    /// anything — and a barrier admitted after the recovery waits here
    /// until this node's own replays have settled (the replayed op is
    /// then the new holder's, durable under its acknowledgement policy
    /// like any forwarded write).
    fn barrier_shipped(&self, barrier: &BarrierWait, replica: &dyn Replica) -> bool {
        !self.lease.lost
            && !replica
                .journal_unshipped_through(barrier.upto)
                .unwrap_or(true)
            && !self.own_replays_unsettled(replica)
    }

    /// The refusal of a barrier on a deposed node: transient, the caller
    /// retries once the recovery round has run.
    pub(crate) fn barrier_refused_deposed() -> String {
        "journal not shipped: this node was deposed and is recovering \
         (its unshipped rows are replayed to the new holder)"
            .into()
    }

    /// This node was deposed: fail every waiting barrier now, before the
    /// recovery round strands its rows (which would empty the journal
    /// without shipping them — [`Self::barrier_shipped`]).
    pub(crate) fn fail_barriers(&mut self, out: &mut Vec<Action>) {
        for barrier in std::mem::take(&mut self.barriers) {
            out.push(Action::ControlDone {
                op: barrier.op,
                result: Err(Self::barrier_refused_deposed()),
            });
        }
    }

    /// A segment landed: answer every barrier whose rows have now all
    /// shipped, without waiting for the round to end (a busy holder's
    /// round ships for as long as its clients write).
    fn answer_shipped_barriers(&mut self, replica: &dyn Replica, out: &mut Vec<Action>) {
        if self.barriers.is_empty() {
            return;
        }
        let barriers = std::mem::take(&mut self.barriers);
        for barrier in barriers {
            if self.barrier_shipped(&barrier, replica) {
                out.push(Action::ControlDone {
                    op: barrier.op,
                    result: Ok(ControlOk::Done),
                });
            } else {
                self.barriers.push(barrier);
            }
        }
    }

    /// A round ended: answer each barrier it settles. A barrier still
    /// short of its position waits for the next round while this node can
    /// ship and it has not yet seen [`BARRIER_ROUNDS`] whole rounds (one
    /// admitted mid-round may have missed that round's complete upload
    /// pass, or a queued job cut the round short).
    fn finish_barriers(
        &mut self,
        now: Ms,
        failed: Option<&str>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if self.barriers.is_empty() {
            return;
        }
        let can_ship = self.lease.journal_ship_epoch(now, &self.cfg).is_some();
        let barriers = std::mem::take(&mut self.barriers);
        for barrier in barriers {
            let result = if self.barrier_shipped(&barrier, replica) {
                Ok(ControlOk::Done)
            } else if let Some(error) = failed {
                Err(error.to_string())
            } else if !can_ship {
                Err(format!(
                    "journal not shipped through position {}: {}",
                    barrier.upto,
                    if self.lease.lost {
                        "this node was deposed and is recovering"
                    } else if self.own_replays_unsettled(replica) {
                        "this node's stranded ops are still being replayed to the holder"
                    } else if self.lease.held.is_some() {
                        "this node's write lease is not usable yet (a takeover gate or an expiry)"
                    } else {
                        "this node does not hold the write lease"
                    }
                ))
            } else if barrier.rounds < BARRIER_ROUNDS {
                self.barriers.push(barrier);
                continue;
            } else if self.own_replays_unsettled(replica) {
                Err(format!(
                    "journal not shipped through position {} after {} sync rounds: \
                     this node's stranded ops are still being replayed",
                    barrier.upto, barrier.rounds
                ))
            } else {
                Err(format!(
                    "journal not shipped through position {} after {} sync rounds \
                     (held back behind a chunk that cannot be uploaded)",
                    barrier.upto, barrier.rounds
                ))
            };
            out.push(Action::ControlDone {
                op: barrier.op,
                result,
            });
        }
    }

    // ---- acquire ----

    /// Close new local mutations while the lease is handed off or
    /// released. The successor needs a few S3 round trips to claim it
    /// (catch up to the head, read, CAS, ship its marker); an old holder
    /// whose own writes retried acquisition before that took the lease
    /// straight back through S3 at slow S3 (a flip and a flop, several
    /// seconds of stalled writes on both nodes). So the pause is at least
    /// `handoff_pause_ms` and eight observed S3 round trips.
    pub(crate) fn begin_handoff_pause(&mut self, now: Ms) {
        let ms = self
            .cfg
            .handoff_pause_ms
            .max(self.ack.s3_rtt_ms.saturating_mul(8));
        self.lease.pause_until = now.plus(ms);
    }

    fn begin_acquire(
        &mut self,
        now: Ms,
        reason: &'static str,
        ask_handoff: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // Plan 31 C8 (`mode.rs`): a forward-only or suspended node closes
        // some of the ways into holding the lease.
        let admitted = self.mode.admit_acquire(reason, ask_handoff);
        let ask_handoff = admitted.unwrap_or(false);
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
        if admitted.is_none() {
            tracing::debug!(
                node = self.cfg.node_id,
                reason,
                suspended = self.mode.suspended,
                "acquisition refused by the authority mode"
            );
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
                        msg: PeerMsg::LeaseRequest {
                            req,
                            epoch_applied: Some(replica.applied_seq().unwrap_or(0)),
                        },
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
        // A claim of the holder's placement offer asks the holder first,
        // over P2P: it re-checks the move against who writes now and may
        // decline (`placement::Placement::declines_claim`), and a decline
        // then costs no S3 request at all. Only a handoff is followed by
        // the S3 claim (catch-up, lease read, CAS).
        let offered_by = self
            .lease
            .cached_holder
            .filter(|h| *h != self.cfg.node_id && self.lease.held.is_none());
        if let (Some(holder), true) = (offered_by, reason == "claim-offer" && ask_handoff) {
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
                msg: PeerMsg::LeaseRequest {
                    req,
                    epoch_applied: None,
                },
            });
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
        if self.acquire_overtaken_by_epoch(now, replica, out) {
            return;
        }
        let plan = self.lease.classify(now, &self.cfg, object, self.bk.sealed);
        if let Some(What::Acquire {
            reason: READOPT_REASON,
            ..
        }) = self.job.as_ref().map(|j| &j.what)
        {
            // A-1: only this node's own lease, unreleased, is re-adopted
            // for a forward; a released one is a handoff in flight, and
            // anything else is somebody else's to decide.
            let own = match &plan {
                Plan::Held => true,
                Plan::Claim { prev, .. } => prev.holder == self.cfg.node_id && !prev.released,
                _ => false,
            };
            if !own {
                tracing::debug!(
                    node = self.cfg.node_id,
                    ?plan,
                    "re-adoption for a forward: the lease is not ours any more"
                );
                self.finish_acquire(now, false, replica, out);
                return;
            }
        }
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
                // once, ask the holder over P2P for a fast handoff. Not for
                // a claim of the holder's own placement offer: the holder
                // offers again while the move still pays, whereas a
                // registration outlives the reason for it — the holder
                // released to a node whose burst had ended, while it was
                // writing itself, and had to take the lease back through
                // S3 (seconds of stalled writes and reads at 300 ms per
                // request; EC2 campaign 6, visibility-s3-latency). Nor for
                // a dead-root takeover: it only claims a lease that ran
                // out, so a holder it finds live (renewed since the
                // check's read, or a dead `Backup` holder whose listed
                // backups are still in their claim grace) is asked for
                // nothing.
                let offered = matches!(
                    &self.job.as_ref().expect("job").what,
                    What::Acquire {
                        reason: "claim-offer" | "dead-root",
                        ..
                    }
                );
                // Plan 31 C8: a forward-only node never asks a live
                // holder for the lease — a registration would make it
                // hand over (`wants_handoff`).
                if !offered && !self.mode.forwards() && !prev.wanted_by.contains(&self.cfg.node_id)
                {
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
                        msg: PeerMsg::LeaseRequest {
                            req,
                            epoch_applied: None,
                        },
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

    /// A continuation epoch opened while this acquisition's lease read
    /// (or its takeover's tail, or promise check) was out: it ends here,
    /// before its CAS. A member of an open epoch runs no S3 acquisition
    /// (plan 30 §M10's rule (b)); the check at the job's start does not
    /// cover one already running. Chunk epoch-liveness-gap,
    /// `locks-blips-tight-faults` seed 409: the hold owner's re-claim,
    /// queued at its close, had its lease read out (slow replies) when its
    /// fresh epoch activated and it held the closed lease again; the CAS
    /// that followed landed, the other members saw the carried object
    /// replaced and closed while it held, and took the lease over at its
    /// expiry beside the hold it re-adopted after a crash.
    fn acquire_overtaken_by_epoch(
        &mut self,
        now: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if !self.epoch.open {
            return false;
        }
        tracing::debug!(
            node = self.cfg.node_id,
            "a continuation epoch opened during the acquisition: no S3 CAS"
        );
        self.finish_acquire(now, false, replica, out);
        true
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
        // An epoch's close kept its tenure's lock grants: they stand if
        // this CAS replaced exactly the lease the close let go.
        let replaced = prev
            .as_ref()
            .filter(|p| !p.released)
            .map(|p| (p.holder, p.epoch, p.expires_unix_ms));
        self.epoch_tenure_resumed(replaced, false, replica);
        // Plan 30 §M14: a released (unexpired) lease's lock grants may
        // still be honoured: a grace before any new grant.
        if prev.as_ref().is_some_and(|p| {
            takeover && p.released && !p.is_expired(now.0) && p.holder != self.cfg.node_id
        }) {
            self.lock_on_released_takeover(now, replica);
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
        self.release_superseded(true, replica);
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
            if reason == READOPT_REASON && !acquired {
                // A-1: the lease was not ours to re-adopt after all (or
                // this node could not claim it): forwards hear `NotHolder`
                // again for a TTL, so a requester that can reach S3 takes
                // its own lease path instead of being held on and on.
                self.readopt_refused_until = now.plus(self.cfg.ttl_ms);
            }
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
        at: Ms,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A request that waited for the slot longer than its requester
        // waits for the answer is declined: the requester has given up
        // (and gone on forwarding), so a release now would leave the
        // lease to nobody, and this holder's own writes would take it
        // back through S3 once its pause ran out (`visibility-s3-
        // latency`: a claim queued 12 s behind a round's ship loop under
        // slow S3, served while this node was the one writing).
        let stale = now.since(at) >= self.cfg.handoff_request_timeout_ms as i64;
        if stale {
            tracing::info!(
                node = self.cfg.node_id,
                requester = from,
                waited_ms = now.since(at),
                "declining a stale lease request: its requester stopped waiting"
            );
        }
        let epoch = self.lease.ship_epoch(now, &self.cfg).filter(|_| !stale);
        let Some(epoch) = epoch else {
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
                    if !*published && self.cfg.publisher && holder.is_some() && replica.has_dirty()
                    {
                        // A cadence publish still in flight is waited for,
                        // not counted as this flush's: it covers the
                        // replica as of its start, not what shipped since,
                        // and an unmount's stop ends the process — the
                        // daemon's publish task with it — once the release
                        // lands. (Before: the flush released at once, and
                        // a clean unmount under load could leave no commit
                        // at all.) Its `PublishDone` re-enters here.
                        if let Some(in_flight) = self.publishing {
                            self.set_phase(Phase::FlushPublish { op: in_flight }, Some(in_flight));
                            return;
                        }
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
                // Fix "capture under an epoch hold": a handoff whose flush
                // could not drain the journal — what is left is deferred
                // (a manifest waiting for a chunk only another node has)
                // or held — is declined, not served. Released, the lease
                // moves and the successor's epoch strands those rows
                // here: rolled back, replayed by rid through it, deferred
                // there again, and the next handoff back repeats it —
                // flex-crash seed 3021 bounced its lease to epoch 111
                // this way, every bounce a rollback and a replay of the
                // same acknowledged writes. The requester forwards to
                // this node meanwhile, as an idle holder with a backlog
                // already makes it (`idle_release_due`).
                let undrained = replica.journal_len().unwrap_or(0);
                let handoff = matches!(
                    self.job.as_ref().map(|j| &j.what),
                    Some(What::Handoff { .. })
                );
                if handoff && undrained > 0 {
                    tracing::info!(
                        node = self.cfg.node_id,
                        undrained,
                        "handoff declined: the journal cannot drain (deferred or held rows)"
                    );
                    self.finish_flush_job(now, false, replica, out);
                    return;
                }
                // The handoff was admitted with no lock grant out
                // (`on_lease_request`), but grants go on being made while
                // it uploads and ships: released now, they would be
                // dropped under their holders' I/O (`locks-blips-tight`
                // seed 2723). Declined like a live grant at admission.
                if handoff && replica.locks().grants_len() > 0 {
                    tracing::info!(
                        node = self.cfg.node_id,
                        grants = replica.locks().grants_len(),
                        "handoff declined: lock grants were made while it flushed"
                    );
                    self.finish_flush_job(now, false, replica, out);
                    return;
                }
                if self.lease.held.is_some() && !self.lease.lost {
                    self.begin_handoff_pause(now);
                    // Closed from here, as an idle release is: no grant
                    // (nor anything else) is admitted while the last atime
                    // segment ships ahead of the release CAS.
                    self.lease.releasing = true;
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
        self.epoch_hold_after_kept_release(now, replica);
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
                // S3 answered: a round's failure from before (an outage)
                // is not the state any more, even when the tail finds
                // nothing new. (`status.spool.last_ship_error` kept a cut's
                // "error sending request" for minutes on a node at head,
                // which read as an S3 client that never recovered.)
                self.ship.last_error = None;
                let empty = run.is_empty();
                let round = matches!(then, TailThen::Round);
                match self.apply_run(now, run, round, replica, out) {
                    Ok(true) => self.issue_tail(out),
                    Ok(false) if empty && self.gap_check_due(now, then) => {
                        self.issue_gap_check(then, out)
                    }
                    Ok(false) => self.after_tail(now, then, replica, out),
                    Err(error) => self.job_failed(now, error, replica, out),
                }
            }
            (Phase::GapCheck { then }, S3Result::SegmentGap(Ok(first))) => {
                self.after_gap_check(now, then, first, replica, out)
            }
            (Phase::GapCheck { .. }, S3Result::SegmentGap(Err(e))) => {
                self.job_failed(now, format!("log gap check: {}", e.0), replica, out)
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
            (
                Phase::Renew {
                    ref sent, mid_ship, ..
                },
                S3Result::LeasePut(Ok(tag)),
            ) => {
                if self.lease.held.is_none() {
                    self.lease_gone_mid_job(now, kind, replica, out);
                    return;
                }
                let renewed = (**sent).clone();
                self.lease.renewed(now, renewed, tag);
                self.ship.renew_now = false;
                self.round_after_renew(now, mid_ship, replica, out);
            }
            (
                Phase::Renew {
                    retry, mid_ship, ..
                },
                S3Result::LeasePut(Err(CasFailure::Conflict)),
            ) => {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(
                    Phase::RenewReread {
                        final_probe: retry,
                        mid_ship,
                    },
                    Some(op),
                );
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
            (
                Phase::RenewReread {
                    final_probe,
                    mid_ship,
                },
                S3Result::LeaseGet(Ok(current)),
            ) => {
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
                            self.round_after_renew(now, mid_ship, replica, out);
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
                                    mid_ship,
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
                    // Plan 30 × campaign 6: a round ships until the
                    // journal is empty, and with writes arriving faster
                    // than one segment PUT (slow S3) that is never: the
                    // lease, renewed only as a round opens, lapsed under
                    // a live, writing holder — its backups stopped
                    // hearing from it at expiry and sealed it. Renew
                    // between two segments once half the TTL is gone.
                    ShipPurpose::Journal if kind == JobKind::Round && self.renew_due(now) => {
                        self.issue_renew(now, true, out);
                    }
                    // A job waits for the slot (a lease request above
                    // all): the round ends after this segment and the
                    // next round ships the rest. Under sustained writes
                    // the ship loop never runs dry, and a lease request
                    // used to wait past its requester's timeout, to be
                    // served (or, now, declined) long after it mattered.
                    ShipPurpose::Journal
                        if kind == JobKind::Round
                            && self
                                .queued_jobs
                                .iter()
                                .any(|j| !matches!(j, JobReq::Round { .. })) =>
                    {
                        self.nudged = true;
                        self.round_publish(now, replica, out);
                    }
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
                self.release_landed(now, kind, replica, out)
            }
            (Phase::Release, S3Result::LeasePut(Err(CasFailure::Conflict))) => {
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::ReleaseReread { in_doubt: false }, Some(op));
            }
            (Phase::Release, S3Result::LeasePut(Err(CasFailure::Failed(e)))) => {
                // No answer is not "not released": a PUT that timed out
                // may have landed, and a released lease is anyone's at
                // once. Keeping it then made two holders (long_backup
                // seed 52088: the deposed one went on acknowledging its
                // own writes under `Local`, rolled back and replayed
                // after the new holder's). The re-read says which;
                // `releasing` stays up, so nothing new is admitted until
                // it answers.
                tracing::warn!(
                    node = self.cfg.node_id,
                    error = %e,
                    "lease release failed without an answer; re-reading the lease"
                );
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::ReleaseReread { in_doubt: true }, Some(op));
            }
            (Phase::ReleaseReread { in_doubt }, S3Result::LeaseGet(result)) => {
                let Some((mine, _)) = self.lease.held.clone() else {
                    self.lease.releasing = false;
                    self.lease_gone_mid_job(now, kind, replica, out);
                    return;
                };
                match result {
                    // Our own release, landed.
                    Ok(Some((cur, _)))
                        if cur.holder == self.cfg.node_id
                            && cur.epoch == mine.epoch
                            && cur.released =>
                    {
                        self.release_landed(now, kind, replica, out);
                        return;
                    }
                    Ok(Some((cur, tag)))
                        if cur.holder == self.cfg.node_id && cur.epoch == mine.epoch =>
                    {
                        self.lease.renewed(now, cur, tag);
                    }
                    Ok(Some((cur, _))) => {
                        self.deposed(now, cur.holder, cur.epoch, mine.epoch, replica, out)
                    }
                    Ok(None) => self.deposed(now, 0, 0, mine.epoch, replica, out),
                    Err(e) if in_doubt => {
                        // Still unknown: the lease is given up here. Had
                        // the release not landed, the object names this
                        // node, unreleased, until it expires; the next
                        // acquisition re-adopts it through the gate.
                        tracing::warn!(
                            node = self.cfg.node_id,
                            error = %e.0,
                            "lease re-read failed after a release in doubt; giving the lease up"
                        );
                        let carried_mine = self.epoch_carries(&mine);
                        self.pr.released = Some((mine.epoch, mine.expires_unix_ms));
                        self.pr.given_up = self.pr.released;
                        self.lease.released();
                        self.owe_given_up_move(carried_mine, replica);
                        self.deleg_on_lease_gone(now, replica, out);
                        replica.set_holder_epoch(0);
                        self.inbox.holder = None;
                        if self.epoch_refuses_writes() {
                            // Nobody holds the epoch now: what waits for
                            // the lease hears `EROFS` at once, not at its
                            // deadline.
                            self.refuse_waiting_for_lease(now, Code::ReadOnly, replica, out);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(node = self.cfg.node_id, error = %e.0, "lease re-read failed")
                    }
                }
                self.lease.releasing = false;
                self.epoch_hold_after_kept_release(now, replica);
                match kind {
                    JobKind::Round => self.finish_round(now, None, replica, out),
                    _ => self.finish_flush_job(now, false, replica, out),
                }
            }
            // ---- acquire ----
            (Phase::Get, S3Result::LeaseGet(Ok(object))) => {
                self.acquire_classified(now, object, replica, out)
            }
            (Phase::EpochCarrierCheck, S3Result::LeaseGet(Ok(object))) => {
                self.epoch_carrier_checked(now, object.map(|(l, _)| l), replica, out)
            }
            (Phase::EpochCarrierCheck, S3Result::LeaseGet(Err(_))) => {
                // S3 is away again: the epoch stays open.
                self.finish_round(now, None, replica, out);
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
                tracing::warn!(node = self.cfg.node_id, error = %e, "lease CAS failed; re-reading the lease");
                let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                self.set_phase(Phase::CasReread { plan, sent }, Some(op));
            }
            (Phase::CasReread { plan, sent }, S3Result::LeaseGet(result)) => {
                self.acquire_cas_reread(now, plan, *sent, result, replica, out)
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

    /// The re-read after an acquisition CAS that failed without an answer.
    /// The object is the tenure the CAS wrote (same holder, epoch and
    /// expiry, not released; a waiter's `wanted_by` edit since does not
    /// matter): it landed (S3's "applied, then timed out"), and the lease
    /// is this node's — won, as if the answer had come, adopting the
    /// object read with its tag. Taken for lost, nothing made this node
    /// adopt it until it expired: its peers read it as the holder and
    /// asked it, and it answered every op and lock request
    /// `NotHolder`/`NotOwner` (the lock waiters of `locks-blips-tight` with
    /// in-doubt lease PUTs timed out after 60 s). Anything else: not
    /// acquired, yet still in doubt — a PUT that timed out client-side can
    /// apply after the re-read answered — so the in-doubt bookkeeping runs
    /// whatever the re-read said (inert unless a later acquisition finds
    /// exactly the object the CAS wrote).
    fn acquire_cas_reread(
        &mut self,
        now: Ms,
        plan: Plan,
        sent: Lease,
        result: Result<Option<(Lease, LeaseTag)>, S3Failure>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let tenure = |l: &Lease| (l.holder, l.epoch, l.expires_unix_ms, l.released);
        match result {
            Ok(Some((cur, tag))) if tenure(&cur) == tenure(&sent) => {
                tracing::info!(
                    node = self.cfg.node_id,
                    epoch = sent.epoch,
                    "the lease CAS in doubt landed"
                );
                self.stats.acquire_cas_in_doubt_landed += 1;
                self.acquire_won(now, plan, cur, tag, replica, out);
            }
            result => {
                if let Err(e) = result {
                    tracing::warn!(node = self.cfg.node_id, error = %e.0, "lease re-read failed after a CAS in doubt");
                }
                self.note_acquire_cas_in_doubt(&plan, &sent);
                self.finish_acquire(now, false, replica, out);
            }
        }
    }

    /// An acquisition CAS writing `sent` whose outcome is unknown: what a
    /// later acquisition that finds `sent` must know about it (a kept
    /// tenure's grants, a takeover's predecessor).
    fn note_acquire_cas_in_doubt(&mut self, plan: &Plan, sent: &Lease) {
        if let Plan::Claim { prev, .. } = plan {
            self.epoch_tenure_cas_in_doubt(prev, sent);
        }
        if let Plan::Claim {
            prev,
            takeover: true,
            ..
        } = plan
        {
            if prev.holder != self.cfg.node_id {
                self.lease.ambiguous_claim = Some((sent.epoch, prev.clone()));
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
                // (EC2 follow-up (d): an epoch that carries no lease has no
                // holder; any member closes it once S3 is back, and the
                // lease is then CAS's again, as before the epoch.)
                if !holds && self.pr.carried.is_some() {
                    // The epoch carries another member's lease: it stays
                    // open here until that holder has taken its authority
                    // back to S3. Ask the lease object.
                    let op = self.issue_s3(S3Op::LeaseGet, S3For::Job, out);
                    self.set_phase(Phase::EpochCarrierCheck, Some(op));
                    return;
                }
                self.epoch_probe_close(out);
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
                if self.acquire_overtaken_by_epoch(now, replica, out) {
                    return;
                }
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

    /// The release CAS landed (its answer, or a re-read after none).
    fn release_landed(
        &mut self,
        now: Ms,
        kind: JobKind,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let epoch = self.lease.epoch().unwrap_or(0);
        if let Some((l, _)) = &self.lease.held {
            self.pr.released = Some((l.epoch, l.expires_unix_ms));
        }
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

    /// An acquisition's lease CAS is out, or the re-read after one that
    /// failed without an answer: the object it writes may stand in S3
    /// now, or land later (`EpochClaimView::in_doubt`).
    pub(crate) fn acquire_cas_unsettled(&self) -> bool {
        self.job
            .as_ref()
            .is_some_and(|j| matches!(j.phase, Phase::Cas { .. } | Phase::CasReread { .. }))
    }

    /// The epoch probe's round may close the epoch: mark it probed and
    /// run the complete upload pass; `on_uploads_done` closes.
    fn epoch_probe_close(&mut self, out: &mut Vec<Action>) {
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

    /// A member of an epoch carrying another member's lease read the lease
    /// object once S3 was back. It closes the epoch only once that object
    /// is no longer the carried lease: the holder re-claimed it (its flush
    /// ships the epoch's journal under it), released it, or lost it.
    /// Until then the holder may still own the epoch's authority, and this
    /// member keeps promising nothing. (Flex-crash seed 4309: the rule was
    /// "a segment past `base`", and a member paused at the activation had
    /// a `base` below the holder's pre-outage segments; tailing those
    /// after the heal closed its epoch, it promised, and a third node took
    /// the lease over while the paused holder still held the epoch.)
    ///
    /// An earlier object of the carried holder and lease epoch is the
    /// carried lease not yet reached: the carrier claimed the latest
    /// object its re-claim CAS in doubt may have written
    /// (`Core::epoch_closed_claim`), and that CAS did not land. Nothing
    /// but the carrier's own re-claim after its close writes a later one
    /// (a holder's expiry only grows, and a CAS on the object orders its
    /// writes).
    fn epoch_carrier_checked(
        &mut self,
        now: Ms,
        object: Option<Lease>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if let Some(l) = &object {
            self.lease.note_object(now, l);
        }
        let carried = self.pr.carried;
        let still_carried = match (&object, carried) {
            (Some(l), Some(c)) => {
                !l.released
                    && l.holder == c.node
                    && l.epoch == c.epoch
                    && l.expires_unix_ms <= c.expires_unix_ms
            }
            _ => false,
        };
        // Or the carried lease is this node's own and it owes the move
        // (`owes_move`: it was releasing that lease at the activation,
        // flex-crash seed 16364): nobody holds the epoch, and its members
        // wait for this node.
        let owed = carried.is_some_and(|c| c.node == self.cfg.node_id)
            && !self.lease.epoch_held()
            && self.owes_carried_move();
        if !still_carried || owed {
            self.epoch_probe_close(out);
            return;
        }
        let _ = replica;
        // Still waiting for the holder — but upload this node's own
        // pending chunks now (the round stays unprobed, so
        // `on_uploads_done` closes nothing). A write this member forwarded
        // to the holder inside the epoch left its chunks enrolled here,
        // and the holder's flush waits for them (`remote_chunk_wait`)
        // before it publishes: waiting for the holder first made each
        // side wait for the other until the 60 s limit
        // (`continuation-epoch` with a carried lease). Chunks are
        // content-addressed: uploading them never needs the lease.
        let op = self.op_id();
        self.set_phase(Phase::Upload, Some(op));
        out.push(Action::UploadDirtyChunks {
            op,
            ino: None,
            round: true,
            complete: false,
        });
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
            // (`on_lease_request`). The hold's epoch is the one this
            // node's flush will ship under (`epoch_hold_epoch_for`): the
            // carried lease's for the carrier, the next for anyone else,
            // so what it journals and captures under the hold is not
            // stranded by its own flush gate, and the positions it
            // answers with are reached only by the flush's segments.
            let mine = self.epoch_hold_epoch_for(epoch);
            if self
                .pr
                .carried
                .is_some_and(|c| self.hold_ended_is(c.epoch, c.expires_unix_ms))
                && replica.persist_epoch_hold_ended(None).is_ok()
            {
                // Handed back to this node: its own again.
                self.pr.hold_ended = None;
            }
            self.adopt_epoch_hold(now, mine, replica);
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
        // This node backed the holder that just handed it the lease: the
        // holder flushed its whole journal to the log before releasing,
        // so the backup tail is void, and its silence from now on is the
        // handoff, not a failure — sealing it (1.5 s later) only raced
        // this node's own claim.
        if self
            .bk
            .role
            .is_some_and(|r| r.holder == from && r.epoch == epoch)
        {
            tracing::debug!(
                node = self.cfg.node_id,
                holder = from,
                epoch,
                "handed the lease by the holder this node backs: backup role ends"
            );
            self.backup_role_ends(replica);
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
