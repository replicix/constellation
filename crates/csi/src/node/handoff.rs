//! The node plugin's side of plan 37 §8's FUSE session handover: one
//! engine pod's sessions moved to its replacement on the same node, driven
//! step by step over the two pods' sockets ([`run`]). The daemon's side —
//! and why the state dir's lock, not this driver, is what keeps the two
//! pods from ever serving at once — is `constellation`'s
//! `crate::handoff_socket`; this module only orders the steps and undoes
//! them.
//!
//! | step | request | undone by |
//! |---|---|---|
//! | 0 Credentials (37-k6a) | old `Credentials` onto a socketpair this plugin reads, relayed to new `Credentials` on a second one: the `fs.unlock` credentials the old pod holds, which the replacement has no other way to get (plan 37 §9; this plugin forgets a `static-ephemeral` class's secret when it restarts, and a chart upgrade restarts it). Nothing pauses yet | new `Abort` |
//! | 1-3 Quiesce, Drain, Snapshot | old `Prepare` | old `Abort` (a failed prepare resumes in place by itself; one still running when the abort comes does as soon as it ends) |
//! | 4 Transfer | old `Transfer` onto a socketpair this plugin reads ([`constellation_control::handoff_wire`]) | old `Abort` |
//! | 4 Transfer, relayed | new `Receive`, every record and descriptor on a second socketpair (this plugin's copies closed once sent) | new `Abort` (drops what it holds), old `Abort` |
//! | 4 Seal | new `Seal`: it serves once the state dir is free | new `Abort`, old `Abort` |
//! | 6 Cutover | old `Commit`: it marks the state dir committed, stops its engine without draining and exits, freeing the state dir | — the point of no return |
//! | 5 Resume | new `Status` until `Resumed` | — |
//!
//! So every failure before `Commit` ends with the old pod serving again
//! ([`Outcome::RolledBack`]), and the old pod's own deadline (the
//! `deadline_ms` of its `Prepare`) does the same if this plugin dies half
//! way. A failure after `Commit` cannot be undone: the views the new pod
//! could not resume end (`ENOTCONN`), and kubelet's republish restages them
//! against the new pod (settled decision 12, [`Outcome::Lost`]). §8's
//! per-view split across both pods does not exist here — one engine owns
//! the state dir, so it owns every view.
//!
//! **Two budgets.** Everything up to the `Commit` is bounded by
//! [`HandoffConfig::total`]; the sender aborts by itself a margin after it
//! (and refuses a commit from then on), and the replacement's seal
//! deadline is later still, so a standby never gives up on a commit that
//! can still come. After the `Commit` the only copies of the sessions are
//! the replacement's: nothing here may give them up, so the wait for its
//! `Resumed` has its own, long bound ([`HandoffConfig::resume`]) and ends
//! early only on the replacement's own `Failed` or on its pod ending
//! ([`ReplacementWatch`]). Past that bound the outcome is
//! [`Outcome::Unresolved`]: the replacement is left alone (it may still
//! serve), never deleted on a timeout.
//!
//! **Timing (K0 basis).** K0 Track A measured the fd round trip itself at
//! p50 0.82 ms / max 21.5 ms, and showed that a reader paused for 10 s
//! only *delays* callers (no error at any queue depth). So the drain bound
//! is a liveness bound, not a correctness one: [`HandoffConfig::drain`]
//! keeps §8's 5 s (half the pause K0 showed harmless), and the steps up to
//! the commit keep §8's 30 s total. The commit itself is a replica sync,
//! not a flush (the receiver is the same node on the same state dir and
//! ships what is left), so the pause a writer sees — from the drain's
//! start to the first request the new pod serves — is the drain, the
//! relay and the new engine's start.

use crate::control_client::ControlClient;
use constellation_control::proto::types::{
    HandoffParams, HandoffPhase, HandoffReport, HandoffState, HandoffTarget,
};
use constellation_control::proto::ControlError;
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// How long either end of a record stream may wait on the other.
const STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// The handoff's bounds (chart values `engineProfile.handoff*`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandoffConfig {
    /// §8 step 2's `--handoff-drain-timeout`: how long the old engine may
    /// wait for the reads and `fsync`s it answers off its FUSE workers
    /// (`CONSTELLATION_CSI_HANDOFF_DRAIN_TIMEOUT_MS`, default 5 s). Past it
    /// the prepare refuses and the attempt counts as failed; nothing is
    /// abandoned. An op held on a FUSE worker itself is not bounded by it:
    /// the prepare then outlasts [`Self::total`], this plugin aborts it,
    /// and the old engine serves again the moment that op is answered.
    pub drain: Duration,
    /// §8 "Timeouts": every step up to the commit
    /// (`CONSTELLATION_CSI_HANDOFF_TOTAL_TIMEOUT_MS`, default 30 s). The
    /// old engine's own deadline is a margin past it: it serves again by
    /// itself, and refuses a commit, if none came by then.
    pub total: Duration,
    /// After the commit, how long the replacement may take to serve before
    /// the handoff is left [`Outcome::Unresolved`]
    /// (`CONSTELLATION_CSI_HANDOFF_RESUME_TIMEOUT_MS`, default 300 s): it
    /// holds the only copies of the sessions by then, so this is a bound
    /// on waiting, never a reason to delete it.
    pub resume: Duration,
    /// §8 "Failure handling": attempts per unit and desired spec before
    /// the rollout gives up and leaves the old pod serving
    /// (`CONSTELLATION_CSI_HANDOFF_MAX_ATTEMPTS`, default 3).
    pub max_attempts: u32,
}

impl Default for HandoffConfig {
    fn default() -> HandoffConfig {
        HandoffConfig {
            drain: Duration::from_secs(5),
            total: Duration::from_secs(30),
            resume: Duration::from_secs(300),
            max_attempts: 3,
        }
    }
}

impl HandoffConfig {
    pub fn from_env() -> Result<HandoffConfig, String> {
        let mut cfg = HandoffConfig::default();
        let read = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let ms = |k: &str, v: String| {
            v.trim()
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| format!("{k}={v:?} must be milliseconds"))
        };
        if let Some(v) = read("CONSTELLATION_CSI_HANDOFF_DRAIN_TIMEOUT_MS") {
            cfg.drain = ms("CONSTELLATION_CSI_HANDOFF_DRAIN_TIMEOUT_MS", v)?;
        }
        if let Some(v) = read("CONSTELLATION_CSI_HANDOFF_TOTAL_TIMEOUT_MS") {
            cfg.total = ms("CONSTELLATION_CSI_HANDOFF_TOTAL_TIMEOUT_MS", v)?;
        }
        if let Some(v) = read("CONSTELLATION_CSI_HANDOFF_RESUME_TIMEOUT_MS") {
            cfg.resume = ms("CONSTELLATION_CSI_HANDOFF_RESUME_TIMEOUT_MS", v)?;
        }
        if let Some(v) = read("CONSTELLATION_CSI_HANDOFF_MAX_ATTEMPTS") {
            cfg.max_attempts = v
                .trim()
                .parse()
                .map_err(|_| format!("CONSTELLATION_CSI_HANDOFF_MAX_ATTEMPTS={v:?}"))?;
        }
        if cfg.drain >= cfg.total {
            return Err(format!(
                "the handoff drain timeout ({:?}) must be shorter than its total ({:?})",
                cfg.drain, cfg.total
            ));
        }
        Ok(cfg)
    }
}

/// Where a handoff failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Credentials,
    Prepare,
    Transfer,
    Receive,
    Seal,
    Commit,
    Resume,
}

impl Step {
    pub fn name(self) -> &'static str {
        match self {
            Step::Credentials => "credentials",
            Step::Prepare => "prepare",
            Step::Transfer => "transfer",
            Step::Receive => "receive",
            Step::Seal => "seal",
            Step::Commit => "commit",
            Step::Resume => "resume",
        }
    }
}

/// How a handoff ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Every view is served by the new pod; the old one exited.
    Succeeded { views: usize, elapsed: Duration },
    /// The new pod serves, but these views could not be resumed: their
    /// mounts ended, and kubelet's republish restages them.
    Partial {
        views: usize,
        failed: Vec<String>,
        elapsed: Duration,
    },
    /// Failed before the commit: the old pod serves every view again.
    /// `restored` is false only if even the `Abort` could not be confirmed
    /// (the old pod's own deadline then restores it).
    RolledBack {
        step: Step,
        error: String,
        restored: bool,
    },
    /// Failed after the commit: the old pod is gone and the new one does
    /// not serve (it said so, or its pod ended); every view ends until it
    /// is restaged.
    Lost { step: Step, error: String },
    /// Committed, but the new pod neither served nor failed within
    /// [`HandoffConfig::resume`]. It holds the only copies of the sessions
    /// and may still serve: it is left alone, and adopted once it does.
    Unresolved { error: String },
}

impl Outcome {
    /// The metric label of this outcome.
    pub fn label(&self) -> &'static str {
        match self {
            Outcome::Succeeded { .. } => "succeeded",
            Outcome::Partial { .. } => "partial",
            Outcome::RolledBack { .. } => "rolled_back",
            Outcome::Lost { .. } => "lost",
            Outcome::Unresolved { .. } => "unresolved",
        }
    }

    /// The new pod serves (whatever it could resume).
    pub fn cut_over(&self) -> bool {
        matches!(self, Outcome::Succeeded { .. } | Outcome::Partial { .. })
    }
}

/// Handoff counters (`constellation_csi_handoff_total{outcome}` and the
/// last duration), exported by the node plugin's metrics endpoint.
#[derive(Debug, Default)]
pub struct HandoffMetrics {
    pub succeeded: AtomicU64,
    pub partial: AtomicU64,
    pub rolled_back: AtomicU64,
    pub lost: AtomicU64,
    pub unresolved: AtomicU64,
    /// Rollouts that gave up after [`HandoffConfig::max_attempts`] and left
    /// the old pod serving (§8's fallback).
    pub fallback: AtomicU64,
    pub last_duration_ms: AtomicU64,
}

impl HandoffMetrics {
    pub fn record(&self, outcome: &Outcome) {
        let counter = match outcome {
            Outcome::Succeeded { elapsed, .. } | Outcome::Partial { elapsed, .. } => {
                self.last_duration_ms
                    .store(elapsed.as_millis() as u64, Ordering::Relaxed);
                if matches!(outcome, Outcome::Succeeded { .. }) {
                    &self.succeeded
                } else {
                    &self.partial
                }
            }
            Outcome::RolledBack { .. } => &self.rolled_back,
            Outcome::Lost { .. } => &self.lost,
            Outcome::Unresolved { .. } => &self.unresolved,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Prometheus text exposition.
    pub fn render(&self) -> String {
        let mut out = String::from(
            "# HELP constellation_csi_handoff_total Engine-pod FUSE session handoffs by outcome.\n\
             # TYPE constellation_csi_handoff_total counter\n",
        );
        for (label, counter) in [
            ("succeeded", &self.succeeded),
            ("partial", &self.partial),
            ("rolled_back", &self.rolled_back),
            ("lost", &self.lost),
            ("unresolved", &self.unresolved),
        ] {
            out.push_str(&format!(
                "constellation_csi_handoff_total{{outcome=\"{label}\"}} {}\n",
                counter.load(Ordering::Relaxed)
            ));
        }
        out.push_str(&format!(
            "# HELP constellation_csi_handoff_fallback_total Rollouts that gave up and left the old engine pod serving.\n\
             # TYPE constellation_csi_handoff_fallback_total counter\n\
             constellation_csi_handoff_fallback_total {}\n\
             # HELP constellation_csi_handoff_last_duration_seconds The last cut-over handoff, prepare to resume.\n\
             # TYPE constellation_csi_handoff_last_duration_seconds gauge\n\
             constellation_csi_handoff_last_duration_seconds {}\n",
            self.fallback.load(Ordering::Relaxed),
            self.last_duration_ms.load(Ordering::Relaxed) as f64 / 1000.0
        ));
        out
    }
}

fn socket(phase: HandoffPhase) -> HandoffParams {
    HandoffParams {
        target: HandoffTarget::Socket,
        phase: Some(phase),
        ..HandoffParams::default()
    }
}

/// How much longer than the handoff's total the old engine waits before it
/// aborts by itself (and refuses a commit).
const SENDER_MARGIN: Duration = Duration::from_secs(5);

/// How much longer than the old engine's own deadline the replacement waits
/// for a commit before it gives up: a sealed standby must never give up on
/// a commit that can still come.
const SEAL_MARGIN: Duration = Duration::from_secs(10);

/// How long the commit's answer may take (the old engine syncs its
/// replica and answers; it no longer flushes to S3).
const COMMIT_TIMEOUT: Duration = Duration::from_secs(15);

/// How often the new pod's `Status` is asked while it resumes.
const POLL: Duration = Duration::from_millis(50);

/// How often, after the commit, whether the new pod still runs is asked.
const WATCH_EVERY: Duration = Duration::from_secs(2);

/// Whether a replacement's pod has ended — deleted, terminated, or its
/// container restarted (what it received is gone then) — asked while it
/// resumes after the commit.
#[async_trait::async_trait]
pub trait ReplacementWatch: Send + Sync {
    /// `Some(why)` once it has.
    async fn ended(&self) -> Option<String>;
}

/// A replacement whose own `Status` is the only word on it.
pub struct Unwatched;

#[async_trait::async_trait]
impl ReplacementWatch for Unwatched {
    async fn ended(&self) -> Option<String> {
        None
    }
}

/// Hand every view of `old` to `new` (a standby on the same state dir) —
/// the module docs' table, within its two budgets; `watch` says whether
/// `new`'s pod still runs.
pub async fn run(
    old: &dyn ControlClient,
    new: &dyn ControlClient,
    watch: &dyn ReplacementWatch,
    cfg: &HandoffConfig,
) -> Outcome {
    // 0. The credentials, before any session stops: the pause the K5
    // notes gate starts at `Prepare`, and so does `elapsed`.
    if let Err(error) = step(Step::Credentials, cfg.total, relay_credentials(old, new)).await {
        return roll_back(old, Some(new), Step::Credentials, error).await;
    }
    let started = Instant::now();
    let deadline = started + cfg.total;
    let left = || deadline.saturating_duration_since(Instant::now());

    // 1-3. Quiesce, drain, snapshot.
    let prepare = HandoffParams {
        drain_timeout_ms: Some(cfg.drain.as_millis() as u64),
        // The old engine's own abort comes after this side has given up
        // (and aborted it itself), never in the middle of a commit.
        deadline_ms: Some((cfg.total + SENDER_MARGIN).as_millis() as u64),
        ..socket(HandoffPhase::Prepare)
    };
    let prepared = match step(Step::Prepare, left(), old.node_handoff(prepare)).await {
        Ok(report) => report,
        Err(error) => return roll_back(old, None, Step::Prepare, error).await,
    };
    // The old engine started its deadline before it answered, so it ends
    // before this.
    let sender_gives_up = Instant::now() + cfg.total + SENDER_MARGIN;
    // Plan 38 §3(e): only `/dev/fuse` sessions are ever handed over. The
    // daemon refuses anything else itself; a report saying otherwise is a
    // bug somewhere, and is refused loudly rather than sent.
    if let Some(v) = prepared
        .views
        .iter()
        .find(|v| !v.transport.is_empty() && v.transport != "dev_fuse")
    {
        let error = format!(
            "refusing to hand over {}: served over {}, not /dev/fuse",
            v.mountpoint, v.transport
        );
        tracing::error!("{error}");
        return roll_back(old, None, Step::Prepare, error).await;
    }
    let expected = prepared.views.len();

    // 4. Transfer, onto a socket this side reads on a thread of its own.
    let (ours, theirs) = match std::os::unix::net::UnixStream::pair() {
        Ok(pair) => pair,
        Err(e) => {
            return roll_back(old, None, Step::Transfer, format!("socketpair: {e}")).await;
        }
    };
    let reader = tokio::task::spawn_blocking(move || {
        let mut ours = ours;
        ours.set_read_timeout(Some(STREAM_TIMEOUT))?;
        let mut records = Vec::new();
        while let Some(record) = constellation_control::handoff_wire::read_record(&mut ours)? {
            records.push(record);
        }
        Ok::<_, std::io::Error>(records)
    });
    let sent = old.node_handoff_fd(socket(HandoffPhase::Transfer), theirs.into());
    if let Err(error) = step(Step::Transfer, left(), sent).await {
        reader.abort();
        return roll_back(old, None, Step::Transfer, error).await;
    }
    let records = match tokio::time::timeout(left(), reader).await {
        Ok(Ok(Ok(records))) => records,
        Ok(Ok(Err(e))) => {
            return roll_back(
                old,
                None,
                Step::Transfer,
                format!("reading the records: {e}"),
            )
            .await
        }
        Ok(Err(e)) => return roll_back(old, None, Step::Transfer, e.to_string()).await,
        Err(_) => {
            return roll_back(
                old,
                None,
                Step::Transfer,
                "reading the records timed out".into(),
            )
            .await
        }
    };
    if records.len() != expected {
        let error = format!(
            "{} record(s) for {expected} prepared view(s)",
            records.len()
        );
        return roll_back(old, None, Step::Transfer, error).await;
    }

    // 4, relayed: every record and descriptor to the new pod on a stream
    // of its own (a handle table of any size: nothing rides in a control
    // frame), this side's copies closed once written (the old pod keeps
    // its own until the commit).
    let (ours, theirs) = match std::os::unix::net::UnixStream::pair() {
        Ok(pair) => pair,
        Err(e) => {
            return roll_back(old, Some(new), Step::Receive, format!("socketpair: {e}")).await;
        }
    };
    let writer = tokio::task::spawn_blocking(move || {
        let mut ours = ours;
        ours.set_write_timeout(Some(STREAM_TIMEOUT))?;
        for (record, fd) in &records {
            constellation_control::handoff_wire::write_record(&mut ours, record, fd.as_fd())?;
        }
        constellation_control::handoff_wire::write_end(&mut ours)
    });
    let got = step(
        Step::Receive,
        left(),
        new.node_handoff_fd(socket(HandoffPhase::Receive), theirs.into()),
    )
    .await;
    let wrote = writer.await;
    let error = match (got, wrote) {
        (Ok(r), Ok(Ok(()))) => match r.state {
            Some(HandoffState::Standby { received }) if received as usize == expected => None,
            other => Some(format!(
                "the replacement holds {other:?} after {expected} record(s) were sent"
            )),
        },
        (Err(error), _) => Some(error),
        (_, Ok(Err(e))) => Some(format!("relaying the records: {e}")),
        (_, Err(e)) => Some(format!("relaying the records: {e}")),
    };
    if let Some(error) = error {
        return roll_back(old, Some(new), Step::Receive, error).await;
    }
    let seal = HandoffParams {
        deadline_ms: Some(
            (sender_gives_up.saturating_duration_since(Instant::now()) + SEAL_MARGIN).as_millis()
                as u64,
        ),
        ..socket(HandoffPhase::Seal)
    };
    if let Err(error) = step(Step::Seal, left(), new.node_handoff(seal)).await {
        return roll_back(old, Some(new), Step::Seal, error).await;
    }
    if left().is_zero() {
        let error = format!(
            "the handoff took longer than {:?} before its commit",
            cfg.total
        );
        return roll_back(old, Some(new), Step::Commit, error).await;
    }

    // 6. Commit: the old pod marks the state dir, stops and exits.
    let committed = old.node_handoff(socket(HandoffPhase::Commit));
    if let Err(error) = step(Step::Commit, COMMIT_TIMEOUT, committed).await {
        // Did it commit? One that answers `Committed` did (it is exiting),
        // and one that no longer answers is gone: the state dir is, or is
        // about to be, free and the standby takes over. One that answers
        // anything else did not — still prepared, or its own deadline
        // already served the sessions again: undo.
        let status = tokio::time::timeout(
            Duration::from_secs(5),
            old.node_handoff(socket(HandoffPhase::Status)),
        )
        .await;
        match status {
            Ok(Ok(r)) if r.state == Some(HandoffState::Committed) => {}
            Ok(Ok(_)) => return roll_back(old, Some(new), Step::Commit, error).await,
            Ok(Err(_)) | Err(_) => tracing::warn!(
                error,
                "the commit's answer was lost and the old engine pod no longer answers; \
                 the new one takes over"
            ),
        }
    }

    // 5. Resume: the new pod takes the state dir and serves. It holds the
    // only copies of the sessions now: wait for it (`cfg.resume`), and
    // give up on it only when it says so or its pod ends.
    let committed_at = Instant::now();
    let mut watched_at = committed_at;
    loop {
        let status = tokio::time::timeout(
            Duration::from_secs(5),
            new.node_handoff(socket(HandoffPhase::Status)),
        )
        .await;
        match status {
            Ok(Ok(r)) => match r.state {
                Some(HandoffState::Resumed { failed }) if failed.is_empty() => {
                    return Outcome::Succeeded {
                        views: expected,
                        elapsed: started.elapsed(),
                    }
                }
                Some(HandoffState::Resumed { failed }) => {
                    return Outcome::Partial {
                        views: expected - failed.len().min(expected),
                        failed,
                        elapsed: started.elapsed(),
                    }
                }
                Some(HandoffState::Failed { reason }) => {
                    return Outcome::Lost {
                        step: Step::Resume,
                        error: reason,
                    }
                }
                Some(HandoffState::Standby { .. }) => {
                    return Outcome::Lost {
                        step: Step::Resume,
                        error: "the replacement restarted: the sessions it held are gone".into(),
                    }
                }
                _ => {}
            },
            // Transient: the standby is busy starting its engine.
            Ok(Err(e)) => tracing::debug!(error = %e.message, "the new engine pod's status"),
            Err(_) => {}
        }
        if watched_at.elapsed() >= WATCH_EVERY {
            watched_at = Instant::now();
            if let Some(why) = watch.ended().await {
                return Outcome::Lost {
                    step: Step::Resume,
                    error: why,
                };
            }
        }
        if committed_at.elapsed() >= cfg.resume {
            return Outcome::Unresolved {
                error: format!(
                    "the new engine pod neither served nor failed within {:?} of the commit",
                    cfg.resume
                ),
            };
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Step 0: the old pod's `fs.unlock` credentials to the new one, through
/// this process as one opaque frame each way
/// ([`constellation_control::handoff_wire::write_secret`]) — never parsed,
/// never logged, wiped once written. The new pod checks them against the
/// bucket before it answers.
async fn relay_credentials(
    old: &dyn ControlClient,
    new: &dyn ControlClient,
) -> Result<HandoffReport, ControlError> {
    let io = |what: &str, e: std::io::Error| ControlError::failed(format!("{what}: {e}"));
    let (ours, theirs) = std::os::unix::net::UnixStream::pair().map_err(|e| io("socketpair", e))?;
    let reader = tokio::task::spawn_blocking(move || {
        let mut ours = ours;
        ours.set_read_timeout(Some(STREAM_TIMEOUT))?;
        constellation_control::handoff_wire::read_secret(&mut ours)
    });
    if let Err(e) = old
        .node_handoff_fd(socket(HandoffPhase::Credentials), theirs.into())
        .await
    {
        reader.abort();
        return Err(e);
    }
    let frame: Option<zeroize::Zeroizing<Vec<u8>>> = reader
        .await
        .map_err(|e| ControlError::failed(format!("reading the credentials: {e}")))?
        .map_err(|e| io("reading the credentials", e))?;
    let (ours, theirs) = std::os::unix::net::UnixStream::pair().map_err(|e| io("socketpair", e))?;
    let writer = tokio::task::spawn_blocking(move || {
        let mut ours = ours;
        ours.set_write_timeout(Some(STREAM_TIMEOUT))?;
        constellation_control::handoff_wire::write_secret(
            &mut ours,
            frame.as_deref().map(|f| &f[..]),
        )
    });
    let got = new
        .node_handoff_fd(socket(HandoffPhase::Credentials), theirs.into())
        .await;
    let wrote = writer
        .await
        .map_err(|e| ControlError::failed(format!("relaying the credentials: {e}")))?;
    let report = got?;
    wrote.map_err(|e| io("relaying the credentials", e))?;
    Ok(report)
}

/// One request of `what`, bounded by `within`: a step that hangs is a
/// failed step, never a stuck handoff.
async fn step(
    what: Step,
    within: Duration,
    request: impl std::future::Future<Output = Result<HandoffReport, ControlError>>,
) -> Result<HandoffReport, String> {
    match tokio::time::timeout(within, request).await {
        Ok(Ok(report)) => Ok(report),
        Ok(Err(e)) => Err(format!("{}: {}", what.name(), e.message)),
        Err(_) => Err(format!("{} timed out", what.name())),
    }
}

/// Undo every step before the commit: the new pod (when it was reached)
/// drops what it holds, the old one serves again.
async fn roll_back(
    old: &dyn ControlClient,
    new: Option<&dyn ControlClient>,
    step: Step,
    error: String,
) -> Outcome {
    tracing::warn!(
        step = step.name(),
        error,
        "engine-pod handoff failed; rolling back"
    );
    if let Some(new) = new {
        if let Err(e) = tokio::time::timeout(
            Duration::from_secs(10),
            new.node_handoff(socket(HandoffPhase::Abort)),
        )
        .await
        .map_err(|_| "timed out".to_string())
        .and_then(|r| r.map_err(|e| e.message))
        {
            // It gives up by itself at its seal deadline (or its standby
            // timeout), and never serves while the old pod holds the
            // state dir: harmless, if untidy.
            tracing::warn!(error = %e, "the replacement did not confirm its abort");
        }
    }
    let restored = match tokio::time::timeout(
        Duration::from_secs(10),
        old.node_handoff(socket(HandoffPhase::Abort)),
    )
    .await
    {
        Ok(Ok(r)) => matches!(r.state, Some(HandoffState::Serving) | None),
        Ok(Err(e)) => {
            tracing::error!(error = %e.message, "the old engine pod refused the abort; its own \
                deadline resumes its sessions");
            false
        }
        Err(_) => {
            tracing::error!(
                "the old engine pod did not answer the abort; its own deadline \
                resumes its sessions"
            );
            false
        }
    };
    Outcome::RolledBack {
        step,
        error,
        restored,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_client::InMemoryControl;
    use constellation_control::proto::types::{MountSource, ViewMountParams};
    use std::sync::Arc;

    const STAGINGS: [&str; 2] = ["/staging/pvc-a/globalmount", "/staging/pvc-b/globalmount"];

    /// An engine serving two staged views, and a standby to replace it.
    async fn pair() -> (Arc<InMemoryControl>, Arc<InMemoryControl>) {
        let old = Arc::new(InMemoryControl::serving("uuid-1"));
        for (i, staging) in STAGINGS.iter().enumerate() {
            let subtree = format!("/volumes/pvc-{i}");
            old.plant_dir(&subtree);
            old.view_mount(ViewMountParams {
                subtree,
                source: MountSource::PreopenedFd {
                    mountpoint: Some(staging.into()),
                    opts: Default::default(),
                },
                labels: Default::default(),
                qos: Default::default(),
                confine_links: true,
            })
            .await
            .unwrap();
        }
        let new = Arc::new(InMemoryControl::standby_for(&old));
        (old, new)
    }

    fn cfg() -> HandoffConfig {
        HandoffConfig {
            drain: Duration::from_millis(500),
            total: Duration::from_secs(5),
            resume: Duration::from_secs(20),
            max_attempts: 3,
        }
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    #[tokio::test]
    async fn a_handoff_moves_every_view_and_the_old_engine_exits() {
        let (old, new) = pair().await;
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        assert!(
            matches!(outcome, Outcome::Succeeded { views: 2, .. }),
            "{outcome:?}"
        );
        assert!(old.handed_off(), "committed and gone");
        assert!(old.view_mountpoints().is_empty());
        assert_eq!(
            sorted(new.view_mountpoints()),
            sorted(STAGINGS.map(String::from).to_vec())
        );
        assert_eq!(new.fds_received(), 2, "one descriptor per view, relayed");
        use HandoffPhase::*;
        assert_eq!(
            old.handoff_calls(),
            [Credentials, Prepare, Transfer, Commit]
        );
        let calls = new.handoff_calls();
        assert_eq!(calls[..3], [Credentials, Receive, Seal]);
        assert!(calls[3..].iter().all(|p| *p == Status));
        // The standby waits for a commit past the moment the old engine
        // stops accepting one (its own deadline, `total` + the margin).
        let sender = old.handoff_params(Prepare).unwrap().deadline_ms.unwrap();
        let seal = new.handoff_params(Seal).unwrap().deadline_ms.unwrap();
        assert_eq!(sender, (cfg().total + SENDER_MARGIN).as_millis() as u64);
        assert!(
            seal > sender + SEAL_MARGIN.as_millis() as u64 / 2,
            "seal {seal} ms, sender {sender} ms"
        );
    }

    /// Must-fix 1 of 37-k5a's review: after the commit the replacement
    /// holds the only copies of the sessions, so its slow start is waited
    /// for on the resume budget, not the pre-commit one (here it serves
    /// 2 s after a commit that came near the end of a 1 s total).
    #[tokio::test]
    async fn a_slow_resume_after_the_commit_is_waited_for_on_its_own_budget() {
        let (old, new) = pair().await;
        new.resume_after(Duration::from_secs(2));
        let cfg = HandoffConfig {
            total: Duration::from_secs(1),
            ..cfg()
        };
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg).await;
        assert!(
            matches!(outcome, Outcome::Succeeded { views: 2, .. }),
            "{outcome:?}"
        );
        assert!(!new.handoff_calls().contains(&HandoffPhase::Abort));
    }

    /// A replacement that neither serves nor fails within the resume
    /// budget is left alone — never aborted, never counted as lost.
    #[tokio::test]
    async fn a_replacement_that_never_resumes_is_unresolved_not_discarded() {
        let (old, new) = pair().await;
        new.resume_never();
        let cfg = HandoffConfig {
            resume: Duration::from_millis(500),
            ..cfg()
        };
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg).await;
        assert!(matches!(outcome, Outcome::Unresolved { .. }), "{outcome:?}");
        assert!(old.handed_off());
        assert!(!new.handoff_calls().contains(&HandoffPhase::Abort));
        assert!(!outcome.cut_over());
    }

    /// After the commit, a replacement whose pod ended is lost at once —
    /// no wait for the resume budget.
    #[tokio::test]
    async fn a_replacement_whose_pod_ended_after_the_commit_is_lost() {
        struct Ended;
        #[async_trait::async_trait]
        impl ReplacementWatch for Ended {
            async fn ended(&self) -> Option<String> {
                Some("pod deleted".into())
            }
        }
        let (old, new) = pair().await;
        new.resume_never();
        let started = Instant::now();
        let outcome = run(old.as_ref(), new.as_ref(), &Ended, &cfg()).await;
        assert_eq!(
            outcome,
            Outcome::Lost {
                step: Step::Resume,
                error: "pod deleted".into()
            }
        );
        assert!(started.elapsed() < cfg().resume);
    }

    /// §8 "Failure handling": a failure at any step before the commit
    /// leaves the old engine serving every view, the replacement holding
    /// nothing — and both were told.
    #[tokio::test]
    async fn every_failure_before_the_commit_rolls_back() {
        use HandoffPhase::*;
        for (on_old, phase, step) in [
            (true, Credentials, Step::Credentials),
            (false, Credentials, Step::Credentials),
            (true, Prepare, Step::Prepare),
            (true, Transfer, Step::Transfer),
            (false, Receive, Step::Receive),
            (false, Seal, Step::Seal),
            (true, Commit, Step::Commit),
        ] {
            let (old, new) = pair().await;
            if on_old {
                old.fail_handoff(phase);
            } else {
                new.fail_handoff(phase);
            }
            let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
            match &outcome {
                Outcome::RolledBack {
                    step: got,
                    restored: true,
                    ..
                } if *got == step => {}
                other => panic!("{phase:?}: {other:?}"),
            }
            assert!(!old.handed_off(), "{phase:?}: the old engine stays");
            assert!(!old.handoff_prepared(), "{phase:?}: and serves again");
            assert_eq!(old.view_mountpoints().len(), 2, "{phase:?}");
            assert!(
                new.view_mountpoints().is_empty(),
                "{phase:?}: nothing double-served"
            );
            assert_eq!(old.handoff_calls().last(), Some(&Abort), "{phase:?}");
            // The replacement is told once it was reached.
            let reached = !matches!(phase, Prepare | Transfer);
            assert_eq!(new.handoff_calls().contains(&Abort), reached, "{phase:?}");
            assert!(!outcome.cut_over());
        }
    }

    /// 37-k6a: a replacement started `--await-unlock` gets the old
    /// engine's `fs.unlock` credentials from the old engine itself, before
    /// any session stops — this plugin holds none to send (it restarted).
    #[tokio::test]
    async fn an_awaiting_standby_gets_the_old_engines_credentials_first() {
        use constellation_control::proto::types::{FsUnlockParams, UnlockCredentials};
        use constellation_control::proto::Secret;
        let (old, new) = pair().await;
        old.fs_unlock(FsUnlockParams {
            fs: "s3://b/pool".into(),
            credentials: UnlockCredentials {
                access_key_id: Some(Secret::new("KEY-A")),
                secret_access_key: Some(Secret::new("secret-a")),
                ..Default::default()
            },
        })
        .await
        .unwrap();
        new.gate();
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        assert!(
            matches!(outcome, Outcome::Succeeded { views: 2, .. }),
            "{outcome:?}"
        );
        assert!(!new.is_gated(), "unlocked by the handed-over credentials");
        assert_eq!(new.last_unlock_key().as_deref(), Some("KEY-A"));
        use HandoffPhase::*;
        assert_eq!(old.handoff_calls()[..2], [Credentials, Prepare]);
        assert_eq!(new.handoff_calls()[..2], [Credentials, Receive]);
    }

    /// An awaiting replacement whose predecessor holds no credentials
    /// cannot serve: the handoff stops before any session does.
    #[tokio::test]
    async fn an_awaiting_standby_with_nothing_to_get_rolls_back_before_the_prepare() {
        let (old, new) = pair().await;
        new.gate();
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        match &outcome {
            Outcome::RolledBack {
                step: Step::Credentials,
                error,
                restored: true,
            } => assert!(error.contains("no fs.unlock credentials"), "{error}"),
            other => panic!("{other:?}"),
        }
        use HandoffPhase::*;
        assert_eq!(old.handoff_calls(), [Credentials, Abort]);
        assert_eq!(new.handoff_calls(), [Credentials, Abort]);
        assert_eq!(old.view_mountpoints().len(), 2);
    }

    /// The old engine's own deadline served its sessions again just before
    /// the commit: it answers, so the handoff rolls back (the standby must
    /// not wait for a state dir that is not coming).
    #[tokio::test]
    async fn a_commit_lost_to_the_old_engines_deadline_rolls_back() {
        let (old, new) = pair().await;
        old.deadline_before_commit();
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        assert!(
            matches!(
                outcome,
                Outcome::RolledBack {
                    step: Step::Commit,
                    restored: true,
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert!(!old.handed_off());
        assert_eq!(old.view_mountpoints().len(), 2);
        assert!(new.handoff_calls().contains(&HandoffPhase::Abort));
        assert!(new.view_mountpoints().is_empty());
    }

    /// Plan 38 §3(e): a session not on `/dev/fuse` is never handed over,
    /// and says so — nothing is transferred.
    #[tokio::test]
    async fn a_session_not_on_dev_fuse_is_refused_before_the_transfer() {
        let (old, new) = pair().await;
        old.report_transport("uring");
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        match outcome {
            Outcome::RolledBack {
                step: Step::Prepare,
                error,
                ..
            } => assert!(error.contains("not /dev/fuse"), "{error}"),
            other => panic!("{other:?}"),
        }
        use HandoffPhase::*;
        assert_eq!(old.handoff_calls(), [Credentials, Prepare, Abort]);
        assert_eq!(new.handoff_calls(), [Credentials]);
    }

    /// After the commit nothing can be undone: a replacement that cannot
    /// resume loses the views (kubelet's republish restages them).
    #[tokio::test]
    async fn a_resume_failing_after_the_commit_is_lost_not_rolled_back() {
        let (old, new) = pair().await;
        new.fail_resume("the engine did not start");
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        assert_eq!(
            outcome,
            Outcome::Lost {
                step: Step::Resume,
                error: "the engine did not start".into()
            }
        );
        assert!(old.handed_off());
        assert!(!old.handoff_calls().contains(&HandoffPhase::Abort));
    }

    #[tokio::test]
    async fn an_engine_with_no_view_has_nothing_to_hand_over() {
        let old = Arc::new(InMemoryControl::serving("uuid-1"));
        let new = Arc::new(InMemoryControl::standby_for(&old));
        let outcome = run(old.as_ref(), new.as_ref(), &Unwatched, &cfg()).await;
        assert!(
            matches!(
                outcome,
                Outcome::RolledBack {
                    step: Step::Prepare,
                    ..
                }
            ),
            "{outcome:?}"
        );
    }

    #[test]
    fn metrics_count_every_outcome() {
        let m = HandoffMetrics::default();
        m.record(&Outcome::Succeeded {
            views: 1,
            elapsed: Duration::from_millis(1500),
        });
        m.record(&Outcome::RolledBack {
            step: Step::Seal,
            error: String::new(),
            restored: true,
        });
        let text = m.render();
        assert!(text.contains("constellation_csi_handoff_total{outcome=\"succeeded\"} 1"));
        assert!(text.contains("constellation_csi_handoff_total{outcome=\"rolled_back\"} 1"));
        assert!(text.contains("constellation_csi_handoff_total{outcome=\"lost\"} 0"));
        assert!(text.contains("constellation_csi_handoff_last_duration_seconds 1.5"));
    }

    #[test]
    fn the_defaults_are_section_8s() {
        let ok = HandoffConfig::default();
        assert!(ok.drain < ok.total);
        assert_eq!(ok.drain, Duration::from_secs(5));
        assert_eq!(ok.total, Duration::from_secs(30));
        assert!(
            ok.resume > ok.total,
            "the post-commit wait is the longer one"
        );
    }
}
