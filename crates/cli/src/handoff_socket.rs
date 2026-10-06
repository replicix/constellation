//! `node.handoff{target: Socket}`: handing FUSE sessions to *another
//! process* — plan 37 §8's engine-pod replacement — in phases a node
//! plugin drives, where `daemon --upgrade` (`crate::handover`) hands them
//! to the next image of the same process.
//!
//! # Who serves: the state dir's lock decides
//!
//! The replacement engine pod mounts the same `node-identity/<unit>/`
//! hostPath (§7: the node identity, the meta store), and an engine opens
//! its state dir exclusively (`daemon.lock`). So the sessions cannot move
//! without the engine moving too, and the lock is what keeps the two
//! processes from ever serving at once: the receiver serves only once it
//! holds the lock, and the sender gives it up only by exiting, after
//! `Commit`. Every step before `Commit` is undone by `Abort` (or, if the
//! plugin goes away, by the sender's own deadline); after it the sender is
//! gone, which is §8's "old pod is the source of truth until step 6"
//! made concrete — with the difference that step 6 here is where the
//! engine is released, not log hygiene (recorded in plan 37's K5 notes).
//!
//! # The sender (the serving daemon)
//!
//! - **`Prepare`** (§8 steps 1-3): the views' preflight (`/dev/fuse` only —
//!   a ring session is refused by name, plan 38 §3(e); no cluster lock, no
//!   blocking lock wait), then each session is detached
//!   (`SessionControl::detach_within`: it stops reading, the accepted
//!   requests drain, pending writes are published, the handle table is
//!   exported). The kernel queues every new request meanwhile (K0
//!   question 4: callers block, never fail). Any failure resumes the views
//!   already detached, in place. The daemon is then "upgrading" (no view
//!   comes or goes) until `Commit` or `Abort`, and a watchdog aborts on its
//!   own once `deadline_ms` passes.
//!
//!   `drain_timeout_ms` bounds only the reads and `fsync`s the engine
//!   answers off its FUSE workers (deferred); fuser's detach joins its
//!   workers without a bound, so an op held *on* a worker — a write behind
//!   the backpressure barrier, a mutation forwarded to a remote holder, a
//!   slow S3 request — holds the prepare as long as it lasts. The plugin's
//!   step timeout then gives up and sends `Abort`, which a prepare still
//!   under way records: the moment it ends it serves its sessions again in
//!   place (as it does when it outlived its own `deadline_ms`, nobody
//!   waiting for it any more) rather than parking them for the watchdog.
//! - **`Transfer`** (step 4): one record per prepared view — the
//!   `crate::handover::MountHandoff` the in-place upgrade uses, in a
//!   versioned envelope — each with a duplicate of the view's `/dev/fuse`
//!   descriptor, onto the stream socket attached to the request
//!   (`constellation_control::handoff_wire`). The sender keeps its own
//!   copies: an `Abort` still has everything it needs. The records are
//!   written outside the sender's state lock, so `Status` and `Abort`
//!   answer while a slow reader holds the stream (an `Abort` landing
//!   mid-write wins: the transfer then fails and no commit can follow).
//! - **`Commit`** (step 6): refused once the sender's own deadline has
//!   passed (the watchdog's abort wins). Otherwise the commit marker is
//!   written into the state dir ([`COMMIT_MARKER`]: the standby's proof
//!   that the descriptors it holds are now the only ones), each view is
//!   closed for the handover (its ephemeral clone and open-orphan claims
//!   left for the receiver), and the engine stops **without draining**
//!   (`Engine::stop_for_local_handoff`): the receiver is this node — same
//!   identity, same `meta.db` — so the journal, the pending chunk uploads
//!   and the lease are left exactly where a restart would find them, and
//!   the receiver ships them (the lease re-adopted at its epoch, through
//!   the takeover gate: M4's own-lease `Plan::Claim`, logged
//!   `takeover=true marker=true`). Before stopping, the engine asks the
//!   lease's committed backups to hold their seal watch
//!   (`constellation_engine::LOCAL_HANDOFF_BACKUP_HOLD`, ended early by the
//!   receiver's first append): without it a backup seals the epoch once the
//!   holder has been silent for its takeover budget (1.5 s), shorter than
//!   the receiver's engine start, and the receiver then runs without a
//!   backup until a new one is brought up. What
//!   stands between the old process and the new one serving is then a
//!   replica sync, not a flush to S3 under a busy writer. The mount
//!   records and the control socket are removed, and the process exits 0
//!   a moment after answering — releasing `daemon.lock` for the receiver.
//!   Exit 0 whatever happens: a restarted container (`restartPolicy:
//!   OnFailure`) would race the receiver for the lock.
//! - **`Abort`**: every prepared session is served again, in place (or, a
//!   prepare still under way, as soon as it ends).
//!
//! - **`Credentials`** (37-k6a, before `Prepare`: nothing pauses): the
//!   credentials `fs.unlock` gave this engine pod (`serve --await-unlock`,
//!   plan 37 §9) — the store its static source signs from, so a rotation
//!   since is included — written as one frame onto the stream socket
//!   attached to the request (`handoff_wire::write_secret`; an empty frame
//!   when it holds none). The standby has no other source: nothing is in
//!   its pod spec or environment, and the node plugin forgets a
//!   `static-ephemeral` class's secret when it restarts — which a chart
//!   upgrade, the very thing that rolls engine pods, does. The frame is
//!   never logged; the node plugin relays it without reading it. Admin is
//!   not enough to ask: only the node plugin — the caller matched its
//!   `kind = "service"` grant ([`NODE_PLUGIN_LABEL`]), never the owner
//!   rule, since anybody who can exec into the pod runs as the owner — and
//!   only while a standby waits on the state dir ([`STANDBY_MARKER`]) and
//!   nothing is committed ([`may_take_credentials`]).
//!
//! # The receiver (a standby `constellation serve --handoff-socket`)
//!
//! `serve` with `--handoff-socket` that finds the state dir locked does
//! not fail: it waits as a **standby** — pre-opening meanwhile what its
//! engine start needs no state dir for (the backend client and its probe,
//! the P2P endpoint: `constellation_engine::Engine::preopen`), since that
//! start is the pause the sessions' callers see — serving only `node.ping` and
//! `node.handoff` on the handoff socket (its control socket is bound only
//! once it serves, so its pod is not `Ready` before). `Receive` reads the
//! records and their descriptors from the stream socket attached to it
//! (`handoff_wire`, as `Transfer` wrote them: a handle table of any size)
//! and holds them — nothing is read from a descriptor; `Seal` says every
//! record is in, after which the standby takes the state dir's lock as
//! soon as it is free, starts the node and resumes each view on its
//! descriptor without `FUSE_INIT` (`crate::handover::resume_mount`:
//! `Engine::open_view_resumed`, then `FuseSession::resume`). An `Abort`
//! before the sender committed — or the seal's deadline passing with no
//! commit marker — drops every descriptor it holds and exits 0: the sender
//! still holds its own copies and serves them again. Once the marker is
//! there the standby holds the **only** copies, so it neither aborts nor
//! gives up: it waits for the sender's process to exit, however long its
//! exit takes. A view that fails to resume ends its mount (`ENOTCONN`
//! until kubelet's republish restages it, settled decision 12); `Status`
//! reports it.
//!
//! Once it serves, the receiver's handoff socket keeps answering `Status`
//! as the *receiver* (`Resumed`, with what failed) for the life of the
//! process — the node plugin asks it there, also later, for a handoff it
//! left pending — while its control socket answers `Status` as a
//! *sender* (`Serving`, or a handoff of its own under way): two roles, two
//! questions, not two answers to one.
//!
//! A standby started `--await-unlock` has no credentials of its own: it
//! pre-opens nothing until a `Credentials` step brings them (they are
//! checked against the bucket first — a pair the bucket refuses fails the
//! step, before any session stopped), and refuses `Receive` until then.
//! Its engine starts on them. A `SIGTERM`/`SIGINT` before the `Seal`
//! ends the standby as an `Abort` does. Once sealed it does not: the node
//! plugin sends `Commit` right after `Seal` answers, without asking
//! whether the standby still lives, so a standby that exited before the
//! marker appeared could leave no process holding the sessions. A sealed
//! standby notes the signal and keeps waiting: past the seal's deadline
//! with no marker it exits (the sender, whose own deadline is earlier,
//! serves its copies again); once the marker is there it holds the only
//! copies, serves them, and passes the signal on to the node, whose own
//! handling (§7: `SIGTERM` deferred while views are mounted) takes it
//! from there.
//!
//! A handed-over descriptor only ever reaches `FuseSession::resume` (never
//! the `FUSE_INIT` handshake of a new mount, which would wait forever on
//! it — K0 question 1); it arrives blocking (`SessionControl::detach`
//! clears `O_NONBLOCK`) and the resumed session sets its own mode.

use crate::handover::{self, Detached, MountHandoff, HANDOVER_VERSION};
use crate::node_runtime::NodeRuntime;
use anyhow::{Context, Result};
use constellation_control::authz::ServiceMatch;
use constellation_control::proto::types::{
    HandedOffView, HandoffParams, HandoffPhase, HandoffReport, HandoffState, Pong,
};
use constellation_control::proto::ControlError;
use serde::{Deserialize, Serialize};
use std::os::fd::{AsFd, IntoRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// How long a sender stays prepared with no `Commit` when the request
/// names no deadline (plan 37 §8's `--handoff-total-timeout`).
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// Written into the state dir by a sender's `Commit` before it lets go of
/// anything, removed by the receiver once it holds the state dir (and by
/// the next `Prepare`): while it exists, the receiver's descriptors are
/// the only ones left, and it waits for the state dir however long that
/// takes.
pub const COMMIT_MARKER: &str = "handoff.committed";

/// How long a committed sender lingers after answering, so the answer
/// leaves before the process does. Short: it is part of the pause writers
/// see, and a plugin that misses the answer treats a sender that no longer
/// answers as committed anyway.
const EXIT_AFTER_COMMIT: Duration = Duration::from_millis(50);

/// How long either side waits on the other end of a record stream before
/// the step fails (a peer that stopped reading or writing must not wedge
/// a daemon with every session stopped).
const STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a standby waits for its first `Seal`
/// (`CONSTELLATION_HANDOFF_STANDBY_TIMEOUT_S`, default 300): a node plugin
/// that died before sealing must not leave it waiting forever. A CSI
/// engine pod's liveness probe also asks the handoff socket
/// (`control-relay --ping --or-socket`), so kubelet does not restart a
/// standby that waits this long.
fn standby_timeout() -> Duration {
    Duration::from_secs(
        std::env::var("CONSTELLATION_HANDOFF_STANDBY_TIMEOUT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
    )
}

/// Test hook (`CONSTELLATION_FAULT_HANDOFF_<what>_MS`): a delay the
/// handoff tests stretch a step with — a prepare held by an op on a FUSE
/// worker, a sender slow to exit after its commit. `None` unset.
fn fault_delay(what: &str) -> Option<Duration> {
    std::env::var(format!("CONSTELLATION_FAULT_HANDOFF_{what}_MS"))
        .ok()
        .and_then(|v| v.parse().ok())
        .map(Duration::from_millis)
}

/// One view on the wire: what the receiver resumes it from. The
/// descriptor travels beside it (`handoff_wire`); `mount.fuse_fd` is
/// meaningless here.
#[derive(Debug, Serialize, Deserialize)]
struct SocketRecord {
    /// `crate::handover::HANDOVER_VERSION`: the record is a
    /// [`MountHandoff`], whose format that versions.
    version: u32,
    /// The handover generation the receiver serves as.
    generation: u32,
    /// The binary that wrote it (for the log).
    from_version: String,
    mount: MountHandoff,
}

/// A sender's prepared sessions.
pub(crate) struct SocketPrepared {
    detached: Vec<Detached>,
    /// A `Transfer` is writing the records (outside the state's lock).
    transferring: bool,
    transferred: bool,
    epoch: u64,
    /// Past this the sender serves the sessions again by itself, and a
    /// `Commit` is refused.
    deadline: Instant,
}

/// A sender's socket-handoff bookkeeping (in `HandoverState`).
#[derive(Default)]
pub(crate) struct SenderState {
    inner: Mutex<SenderInner>,
    epoch: AtomicU64,
    /// Committed: exiting (what `Status` answers from then on).
    committed: std::sync::atomic::AtomicBool,
}

#[derive(Default)]
struct SenderInner {
    /// A `Prepare` still detaching (its epoch), and whether an `Abort`
    /// arrived meanwhile.
    preparing: Option<(u64, bool)>,
    prepared: Option<SocketPrepared>,
}

fn report(
    detail: String,
    views: Vec<HandedOffView>,
    state: HandoffState,
    started: Instant,
) -> HandoffReport {
    HandoffReport {
        detail,
        views,
        state: Some(state),
        elapsed_ms: started.elapsed().as_millis() as u64,
    }
}

fn handed_off(detached: &[Detached]) -> Vec<HandedOffView> {
    detached
        .iter()
        .map(|(t, h)| HandedOffView {
            id: t.id.as_u64(),
            mountpoint: t.info.mountpoint.display().to_string(),
            handles: h
                .view
                .as_ref()
                .map(|v| v.handles.handles.len() as u64)
                .unwrap_or(0),
            transport: h.fuse.transport().name().to_string(),
        })
        .collect()
}

fn marker(state_dir: &Path) -> PathBuf {
    state_dir.join(COMMIT_MARKER)
}

/// Written into the state dir by a standby for as long as it waits
/// (removed when the wait ends, whichever way; one a killed standby left
/// is removed by the state dir's next holder at its start): a serving
/// engine hands its credentials over only while it exists
/// ([`may_take_credentials`]).
pub const STANDBY_MARKER: &str = "handoff.standby";

fn standby_marker(state_dir: &Path) -> PathBuf {
    state_dir.join(STANDBY_MARKER)
}

/// Remove a [`STANDBY_MARKER`] a killed standby left: nobody waits as the
/// standby of a holder that only now starts (`serve`, on taking the state
/// dir).
pub(crate) fn clear_stale_standby_marker(state_dir: &Path) {
    let _ = std::fs::remove_file(standby_marker(state_dir));
}

/// [`STANDBY_MARKER`], for the life of a standby's wait.
struct StandbyMarker(PathBuf);

impl StandbyMarker {
    fn write(state_dir: &Path) -> Result<StandbyMarker> {
        let path = standby_marker(state_dir);
        std::fs::write(&path, format!("pid {}\n", std::process::id()))
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(StandbyMarker(path))
    }
}

impl Drop for StandbyMarker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// `node.handoff{target: Socket}` on a serving daemon (the sender phases).
/// `service`: the `kind = "service"` grant the caller matched
/// (`CallCtx::service`).
pub fn sender(
    node: &Arc<NodeRuntime>,
    p: &HandoffParams,
    fd: Option<OwnedFd>,
    service: Option<&ServiceMatch>,
) -> Result<HandoffReport, ControlError> {
    if !cfg!(target_os = "linux") {
        return Err(ControlError::unsupported(
            "handing sessions to another process is Linux-only",
        ));
    }
    let started = Instant::now();
    match p.phase {
        None => Err(ControlError::invalid(
            "a handoff to a socket names its phase (prepare, transfer, commit or abort)",
        )),
        Some(HandoffPhase::Prepare) => prepare(node, p, started),
        Some(HandoffPhase::Transfer) => {
            let fd = fd.ok_or_else(|| {
                ControlError::invalid("transfer needs the receiving socket attached")
            })?;
            transfer(node, fd, started)
        }
        Some(HandoffPhase::Commit) => commit(node, started),
        Some(HandoffPhase::Abort) => Ok(abort(node, "aborted by request", started, None)),
        Some(HandoffPhase::Status) => {
            let inner = node.handover.socket.inner.lock().unwrap();
            let state = match (&inner.prepared, &inner.preparing) {
                (None, _) if node.handover.socket.committed.load(Ordering::SeqCst) => {
                    HandoffState::Committed
                }
                (None, None) => HandoffState::Serving,
                (None, Some(_)) => HandoffState::Prepared,
                (Some(p), _) if p.transferred => HandoffState::Transferred,
                (Some(_), _) => HandoffState::Prepared,
            };
            Ok(report(String::new(), Vec::new(), state, started))
        }
        Some(HandoffPhase::Credentials) => {
            may_take_credentials(
                service,
                node.engine().state_dir(),
                node.handover.socket.committed.load(Ordering::SeqCst),
            )?;
            let fd = fd.ok_or_else(|| {
                ControlError::invalid("credentials need the receiving socket attached")
            })?;
            send_credentials(node, fd, started)
        }
        Some(HandoffPhase::Receive | HandoffPhase::Seal) => Err(ControlError::invalid(
            "this daemon is serving, not a standby: it has nothing to receive",
        )),
    }
}

/// The label of the node plugin's `kind = "service"` grant on a node-owned
/// engine pod's sockets (`constellation_csi::engine_pods::node_engine_policy`).
pub const NODE_PLUGIN_LABEL: &str = "csi-node-plugin";

/// Who may take a serving engine's credentials with a `Credentials` step,
/// and when (module docs): the node plugin — the caller matched its
/// `kind = "service"` grant ([`NODE_PLUGIN_LABEL`]) on this socket; the
/// owner rule makes the engine's own uid admin, and anybody who can exec
/// into its pod runs as that uid — and only while a standby waits on
/// `state_dir` ([`STANDBY_MARKER`]) and nothing is committed yet.
fn may_take_credentials(
    service: Option<&ServiceMatch>,
    state_dir: &Path,
    committed: bool,
) -> Result<(), ControlError> {
    if service.is_none_or(|s| s.label != NODE_PLUGIN_LABEL) {
        return Err(ControlError::denied(format!(
            "only the CSI node plugin (its kind = \"service\" grant, label = \
             {NODE_PLUGIN_LABEL:?}) takes this engine's credentials, for a handoff; the owner and \
             any other grant do not"
        )));
    }
    if committed || !standby_marker(state_dir).exists() {
        return Err(ControlError::invalid(
            "no handoff is pending: no standby waits on this engine's state dir",
        ));
    }
    Ok(())
}

/// The sender's `Credentials` (module docs): what `fs.unlock` gave this
/// engine, as one generation of its store, onto `fd`.
fn send_credentials(
    node: &Arc<NodeRuntime>,
    fd: OwnedFd,
    started: Instant,
) -> Result<HandoffReport, ControlError> {
    let mut sock = UnixStream::from(fd);
    let io = |e: std::io::Error| ControlError::failed(format!("writing the credentials: {e}"));
    sock.set_nonblocking(false).map_err(io)?;
    sock.set_write_timeout(Some(STREAM_TIMEOUT)).map_err(io)?;
    let frame = node
        .handoff_secrets()
        .map(|store| unlock_credentials(&store))
        .transpose()?
        .flatten();
    constellation_control::handoff_wire::write_secret(&mut sock, frame.as_deref().map(|f| &f[..]))
        .map_err(io)?;
    let detail = match frame {
        Some(_) => "the fs.unlock credentials were handed over",
        None => "this engine holds no fs.unlock credentials",
    };
    tracing::info!(sent = frame.is_some(), "socket handoff: credentials step");
    Ok(report(
        detail.into(),
        Vec::new(),
        HandoffState::Serving,
        started,
    ))
}

/// `store`'s credentials (one generation) as `fs.unlock` carries them,
/// serialized; `None` when it holds none. Every copy is wiped once dropped:
/// the `Secret`s on drop, the encoding in a buffer sized up front (no
/// reallocation leaves one behind).
fn unlock_credentials(
    store: &constellation_platform::EphemeralSecretStore,
) -> Result<Option<zeroize::Zeroizing<Vec<u8>>>, ControlError> {
    use constellation_control::proto::Secret;
    use constellation_platform::CredentialSource as C;
    let (_, secrets) = store.snapshot();
    let text = |name: &str| -> Result<Option<Secret>, ControlError> {
        secrets
            .get(name)
            .map(|s| {
                s.expose_str().map(Secret::new).ok_or_else(|| {
                    // Never the value, nor any part of it.
                    ControlError::failed(format!(
                        "this engine's {name} is not UTF-8, so a handoff cannot carry it \
                         (fs.unlock only ever gives text): nothing was handed over, and this \
                         pod keeps serving; give it its credentials again with fs.unlock"
                    ))
                })
            })
            .transpose()
    };
    let credentials = constellation_control::proto::types::UnlockCredentials {
        access_key_id: text(C::ACCESS_KEY_ID)?,
        secret_access_key: text(C::SECRET_ACCESS_KEY)?,
        session_token: text(C::SESSION_TOKEN)?,
        e2e_passphrase: text(crate::serve::E2E_PASSPHRASE)?,
    };
    if credentials == Default::default() {
        return Ok(None);
    }
    // JSON escapes a byte into at most 6.
    let values: usize = secrets.values().map(|s| s.expose().len()).sum();
    let mut out = zeroize::Zeroizing::new(Vec::with_capacity(6 * values + 256));
    serde_json::to_writer(&mut *out, &credentials)
        .map_err(|e| ControlError::failed(format!("encoding the credentials: {e}")))?;
    Ok(Some(out))
}

fn prepare(
    node: &Arc<NodeRuntime>,
    p: &HandoffParams,
    started: Instant,
) -> Result<HandoffReport, ControlError> {
    let state = &node.handover;
    if !state.begin() {
        return Err(ControlError::failed("a handover is already under way")
            .with_code(constellation_types::Code::Busy));
    }
    let deadline = p
        .deadline_ms
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_DEADLINE);
    let epoch = state.socket.epoch.fetch_add(1, Ordering::SeqCst) + 1;
    state.socket.inner.lock().unwrap().preparing = Some((epoch, false));
    let fail = |why: String| {
        state.socket.inner.lock().unwrap().preparing = None;
        ControlError::failed(state.fail_with(why))
    };
    // A marker left by an earlier handoff whose receiver never took over
    // (this daemon holds the state dir, so that handoff is over).
    clear_stale_marker(node.engine().state_dir());
    let targets = handover::targets(node, &p.views).map_err(fail)?;
    handover::check_targets(&targets, "handoff").map_err(fail)?;
    if let Some(delay) = fault_delay("PREPARE_DELAY") {
        std::thread::sleep(delay);
    }
    let drain = p.drain_timeout_ms.map(Duration::from_millis);
    let detached = handover::detach_targets(node, targets, drain).map_err(fail)?;
    // Plan 38 §3(e), asserted again on what was actually detached: a CSI
    // session is pinned to `/dev/fuse` (`view.mount{PreopenedFd}` serves
    // it handover-capable), and `detach` refuses anything else; a
    // non-`/dev/fuse` handoff here would be a bug upstream, so it is
    // refused loudly rather than sent.
    if let Some((t, h)) = detached
        .iter()
        .find(|(_, h)| !h.fuse.transport().is_dev_fuse())
    {
        let why = format!(
            "refusing the handoff: {} is served over {}, not /dev/fuse",
            t.info.mountpoint.display(),
            h.fuse.transport()
        );
        tracing::error!("{why}");
        for (t, h) in detached {
            handover::resume_in_place(node, t, h.fuse);
        }
        return Err(fail(why));
    }
    let views = handed_off(&detached);
    {
        let mut inner = state.socket.inner.lock().unwrap();
        let aborted = matches!(inner.preparing.take(), Some((e, true)) if e == epoch);
        // Nobody waits for a prepare that was aborted while it detached,
        // or that outlived its own deadline (the plugin gave up on it):
        // its sessions are served again now, not parked for the watchdog.
        let late = started.elapsed() >= deadline;
        if aborted || late {
            drop(inner);
            let why = if aborted {
                "aborted while it was preparing"
            } else {
                "its deadline passed while it was preparing"
            };
            for (t, h) in detached {
                handover::resume_in_place(node, t, h.fuse);
            }
            let why = format!(
                "the handoff's prepare took {:?}: {why}; serving the sessions again in place",
                started.elapsed()
            );
            tracing::warn!("{why}");
            return Err(ControlError::failed(state.fail_with(why)));
        }
        inner.prepared = Some(SocketPrepared {
            detached,
            transferring: false,
            transferred: false,
            epoch,
            deadline: Instant::now() + deadline,
        });
    }
    // §8 "Timeouts": a plugin that went away mid-handoff must not leave
    // the mounts stalled — the sessions are served again here once the
    // deadline passes without a `Commit`.
    let watched = Arc::downgrade(node);
    let _ = std::thread::Builder::new()
        .name("handoff-deadline".into())
        .spawn(move || {
            std::thread::sleep(deadline);
            let Some(node) = watched.upgrade() else {
                return;
            };
            let report = abort(&node, "deadline passed", Instant::now(), Some(epoch));
            if report.views.is_empty() {
                return;
            }
            tracing::error!(
                ?deadline,
                "no commit within the handoff deadline; served the sessions again in place"
            );
        });
    let detail = format!(
        "prepared {} view(s) for a handoff (drained in {:?}; aborts by itself after {deadline:?})",
        views.len(),
        started.elapsed()
    );
    tracing::info!("{detail}");
    Ok(report(detail, views, HandoffState::Prepared, started))
}

fn transfer(
    node: &Arc<NodeRuntime>,
    fd: OwnedFd,
    started: Instant,
) -> Result<HandoffReport, ControlError> {
    let io = |e: std::io::Error| ControlError::failed(format!("writing the handoff stream: {e}"));
    // The records are built (and the descriptors duplicated) under the
    // state, and written without it: a peer slow to read must not block
    // `Status` and `Abort` for the stream's timeout. `transferring` keeps
    // a second transfer out meanwhile, and the epoch tells an abort (or
    // the deadline's) that landed during the write.
    let (epoch, records) = {
        let mut inner = node.handover.socket.inner.lock().unwrap();
        let Some(prepared) = inner.prepared.as_mut() else {
            return Err(ControlError::invalid(
                "nothing is prepared: transfer follows a prepare",
            ));
        };
        if prepared.transferring {
            return Err(ControlError::invalid("a transfer is already under way"));
        }
        let generation = node.handover.generation() + 1;
        let mut records = Vec::with_capacity(prepared.detached.len());
        for (t, h) in &prepared.detached {
            let view = h
                .view
                .as_ref()
                .map_err(|e| ControlError::failed(format!("{e:#}")))?;
            let record = SocketRecord {
                version: HANDOVER_VERSION,
                generation,
                from_version: env!("CONSTELLATION_VERSION").to_string(),
                mount: MountHandoff {
                    old_id: t.id.as_u64(),
                    subtree: t.info.subtree.clone(),
                    mountpoint: t.info.mountpoint.clone(),
                    fs_name: t.info.fs_name.clone(),
                    allow_other: t.info.allow_other,
                    read_only: t.info.read_only,
                    fuse_threads: t.info.fuse_threads,
                    fuse_fd: -1,
                    init: h.fuse.init,
                    foreign: h.fuse.foreign,
                    passthrough: h.fuse.passthrough.clone(),
                    view: view.clone(),
                },
            };
            let body = serde_json::to_vec(&record)
                .map_err(|e| ControlError::failed(format!("encoding a handoff record: {e}")))?;
            let conn = h.fuse.fuse_fd.as_fd().try_clone_to_owned().map_err(io)?;
            records.push((body, conn));
        }
        prepared.transferring = true;
        (prepared.epoch, records)
    };
    let written = (|| {
        let mut sock = UnixStream::from(fd);
        sock.set_nonblocking(false)?;
        sock.set_write_timeout(Some(STREAM_TIMEOUT))?;
        for (body, conn) in &records {
            constellation_control::handoff_wire::write_record(&mut sock, body, conn.as_fd())?;
        }
        constellation_control::handoff_wire::write_end(&mut sock)
    })();
    // Our duplicates go: the prepared views keep their own descriptors.
    drop(records);
    let mut inner = node.handover.socket.inner.lock().unwrap();
    let Some(prepared) = inner.prepared.as_mut().filter(|p| p.epoch == epoch) else {
        return Err(ControlError::failed(
            "the handoff was aborted while its records were being written: the sessions are \
             served here again",
        ));
    };
    prepared.transferring = false;
    written.map_err(io)?;
    prepared.transferred = true;
    let views = handed_off(&prepared.detached);
    drop(inner);
    let detail = format!("transferred {} view(s)", views.len());
    tracing::info!("{detail}");
    Ok(report(detail, views, HandoffState::Transferred, started))
}

fn commit(node: &Arc<NodeRuntime>, started: Instant) -> Result<HandoffReport, ControlError> {
    let engine = node.engine().clone();
    let state_dir = engine.state_dir().to_path_buf();
    let prepared = {
        let mut inner = node.handover.socket.inner.lock().unwrap();
        match inner.prepared.as_ref() {
            Some(p) if p.transferred && Instant::now() >= p.deadline => {
                // The watchdog's abort is due (or running): it wins, and
                // the receiver, finding no marker, gives up.
                return Err(ControlError::failed(
                    "too late to commit: the handoff's deadline passed, and the sessions are \
                     served here again",
                ));
            }
            Some(p) if p.transferred => {
                // Under the lock the deadline's abort takes too: one of the
                // two wins, and `Status` tells which. The marker first: from
                // here on the receiver's copies may be the only ones. Our
                // mount records go just before it: once it is written the
                // receiver owns the sessions, and our reaper (were we
                // killed now) must not abort their connections.
                let records: Vec<_> = p
                    .detached
                    .iter()
                    .filter_map(|(t, _)| {
                        let id = t.id.as_u64();
                        crate::daemon_lock::take_mount(&state_dir, id).map(|r| (id, r))
                    })
                    .collect();
                write_marker(&state_dir, p.epoch).map_err(|e| {
                    for (id, r) in &records {
                        crate::daemon_lock::restore_mount(&state_dir, *id, r);
                    }
                    ControlError::failed(format!(
                        "writing the commit marker (nothing was committed): {e}"
                    ))
                })?;
                node.handover.socket.committed.store(true, Ordering::SeqCst);
                inner.prepared.take().expect("checked")
            }
            Some(_) => {
                return Err(ControlError::invalid(
                    "nothing was transferred: commit follows a transfer",
                ))
            }
            None => return Err(ControlError::invalid("nothing is prepared to commit")),
        }
    };
    let views = handed_off(&prepared.detached);
    for (t, _) in &prepared.detached {
        engine.close_view_for_handover(&t.view);
        node.mounts.lock().unwrap().remove(&t.id);
    }
    node.close_headless_view();
    let backlog = (
        constellation_meta::MetaStore::journal_len(&**engine.meta()).unwrap_or(0),
        engine.meta().pending_upload_count().unwrap_or(0),
    );
    let holding = match engine.stop_for_local_handoff() {
        Ok(holding) => holding,
        Err(e) => {
            // Everything is in the OS already (commits are written through
            // on commit); only the power-loss window stays open.
            tracing::warn!(error = %format!("{e:#}"), "the pre-handoff replica sync failed");
            Vec::new()
        }
    };
    // Our copies go now: the receiver (or the plugin relaying) holds the
    // connections.
    drop(prepared);
    node.forget_control_socket();
    let detail = format!(
        "committed {} view(s) in {:?}; left for the receiver: {} journal record(s), {} pending \
         chunk upload(s); backups holding their seal watch: {holding:?}; exiting so it can take \
         the state dir",
        views.len(),
        started.elapsed(),
        backlog.0,
        backlog.1
    );
    tracing::info!("{detail}");
    // Answer first (`EXIT_AFTER_COMMIT`), then exit 0 — never through the node's shutdown (that
    // would drain, which is the receiver's to do now) and never non-zero
    // (a container restart would race the receiver for `daemon.lock`).
    let exit_after = fault_delay("EXIT_DELAY").unwrap_or(EXIT_AFTER_COMMIT);
    let _ = std::thread::Builder::new()
        .name("handoff-exit".into())
        .spawn(move || {
            std::thread::sleep(exit_after);
            tracing::info!("handed off; exiting");
            std::process::exit(0);
        });
    Ok(report(detail, views, HandoffState::Committed, started))
}

/// Remove a [`COMMIT_MARKER`] the state dir's holder finds: a handoff's
/// whose receiver never took over (it removes the marker once it holds
/// the state dir), so that handoff is over — logged, since it means a
/// committed pair crashed before the receiver served.
pub(crate) fn clear_stale_marker(state_dir: &Path) {
    let path = marker(state_dir);
    match std::fs::read_to_string(&path) {
        Ok(left) => {
            tracing::warn!(
                marker = %path.display(),
                left = left.trim(),
                "a handoff commit marker was left by a handoff whose receiver never served; \
                 removing it"
            );
            let _ = std::fs::remove_file(&path);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            tracing::warn!(marker = %path.display(), error = %e, "reading a stale handoff marker");
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// [`COMMIT_MARKER`], durable before anything is let go.
fn write_marker(state_dir: &Path, epoch: u64) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(marker(state_dir))?;
    writeln!(f, "epoch {epoch} pid {}", std::process::id())?;
    f.sync_all()
}

/// Serve every prepared session again, in place (`Abort`, the deadline's
/// watchdog — which names its `epoch`, so a stale one never aborts a later
/// prepare). A prepare still detaching is told to, as soon as it ends.
fn abort(
    node: &Arc<NodeRuntime>,
    why: &str,
    started: Instant,
    epoch: Option<u64>,
) -> HandoffReport {
    let prepared = {
        let mut guard = node.handover.socket.inner.lock().unwrap();
        let inner = &mut *guard;
        let ours = inner
            .prepared
            .as_ref()
            .is_some_and(|p| epoch.is_none_or(|e| e == p.epoch));
        if ours {
            inner.prepared.take()
        } else if let (None, None, Some((_, aborted))) =
            (&inner.prepared, epoch, inner.preparing.as_mut())
        {
            *aborted = true;
            return report(
                "a prepare is under way: it serves its sessions again in place as soon as it \
                 ends"
                    .into(),
                Vec::new(),
                HandoffState::Serving,
                started,
            );
        } else {
            None
        }
    };
    let Some(prepared) = prepared else {
        return report(
            "nothing is prepared".into(),
            Vec::new(),
            HandoffState::Serving,
            started,
        );
    };
    let views = handed_off(&prepared.detached);
    for (t, h) in prepared.detached {
        handover::resume_in_place(node, t, h.fuse);
    }
    node.handover
        .finish(Some(format!("socket handoff abandoned: {why}")));
    let detail = format!("serving {} view(s) again in place ({why})", views.len());
    tracing::warn!("{detail}");
    report(detail, views, HandoffState::Serving, started)
}

// ---------------------------------------------------------------------------
// The receiver
// ---------------------------------------------------------------------------

enum Phase {
    Waiting,
    Sealed {
        deadline: Instant,
        /// The sender's commit marker was seen: the descriptors held here
        /// are the only ones, and nothing gives them up any more.
        committed: bool,
    },
    /// The lock is ours: too late to abort.
    Resuming,
    Resumed {
        failed: Vec<String>,
    },
    Failed {
        reason: String,
    },
}

struct Received {
    record: SocketRecord,
    fd: OwnedFd,
}

/// A standby's state, shared by its handoff socket's handlers and the
/// thread waiting to serve ([`standby`]).
struct Standby {
    inner: Mutex<(Phase, Vec<Received>)>,
    changed: Condvar,
    state_dir: PathBuf,
    /// `serve --await-unlock`: what takes a `Credentials` step's
    /// credentials (module docs). `None`: the standby uses its
    /// environment's, and drops any handed over.
    credentials: Option<CredentialsHook>,
    /// A `Credentials` step delivered them (`Receive` waits for it when
    /// [`Self::credentials`] is set).
    has_credentials: std::sync::atomic::AtomicBool,
}

/// What a standby that awaits its credentials does with the ones a
/// `Credentials` step brought: checks them and keeps them for its engine
/// (`crate::serve`). Its `Ok` is the step's detail. Never logs them.
pub type CredentialsHook = Box<
    dyn Fn(constellation_control::proto::types::UnlockCredentials) -> Result<String, ControlError>
        + Send
        + Sync,
>;

impl Standby {
    fn state(&self) -> HandoffState {
        let inner = self.inner.lock().unwrap();
        match &inner.0 {
            Phase::Waiting => HandoffState::Standby {
                received: inner.1.len() as u64,
            },
            Phase::Sealed { .. } | Phase::Resuming => HandoffState::Sealed,
            Phase::Resumed { failed } => HandoffState::Resumed {
                failed: failed.clone(),
            },
            Phase::Failed { reason } => HandoffState::Failed {
                reason: reason.clone(),
            },
        }
    }

    fn views(&self) -> Vec<HandedOffView> {
        self.inner
            .lock()
            .unwrap()
            .1
            .iter()
            .map(|r| HandedOffView {
                id: r.record.mount.old_id,
                mountpoint: r.record.mount.mountpoint.display().to_string(),
                handles: r.record.mount.view.handles.handles.len() as u64,
                transport: r.record.mount.init.transport.name().to_string(),
            })
            .collect()
    }

    /// Whether the sender committed (its marker is in the state dir).
    fn sender_committed(&self) -> bool {
        marker(&self.state_dir).exists()
    }

    fn handle(
        &self,
        p: &HandoffParams,
        fd: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        let started = Instant::now();
        let done = |detail: String| Ok(report(detail, self.views(), self.state(), started));
        match p.phase {
            Some(HandoffPhase::Status) => done(String::new()),
            Some(HandoffPhase::Credentials) => {
                let fd = fd
                    .ok_or_else(|| ControlError::invalid("credentials need the stream attached"))?;
                if !matches!(self.inner.lock().unwrap().0, Phase::Waiting) {
                    return Err(ControlError::invalid("this standby is sealed"));
                }
                let mut sock = UnixStream::from(fd);
                let io = |e: std::io::Error| {
                    ControlError::failed(format!("reading the credentials: {e}"))
                };
                sock.set_nonblocking(false).map_err(io)?;
                sock.set_read_timeout(Some(STREAM_TIMEOUT)).map_err(io)?;
                let frame =
                    constellation_control::handoff_wire::read_secret(&mut sock).map_err(io)?;
                let Some(hook) = &self.credentials else {
                    return done(
                        "this standby takes its credentials from its environment; the handed-over \
                         ones were dropped"
                            .into(),
                    );
                };
                let Some(frame) = frame else {
                    return Err(ControlError::invalid(
                        "the sender holds no fs.unlock credentials, and this standby waits for \
                         them (--await-unlock)",
                    ));
                };
                // Never the parser's message: it may quote the input.
                let credentials = serde_json::from_slice(&frame).map_err(|_| {
                    ControlError::invalid("the handed-over credentials are not readable")
                })?;
                drop(frame);
                let detail = hook(credentials)?;
                self.has_credentials.store(true, Ordering::SeqCst);
                done(detail)
            }
            Some(HandoffPhase::Receive) => {
                let fd = fd.ok_or_else(|| {
                    ControlError::invalid("receive needs the record stream attached")
                })?;
                if !matches!(self.inner.lock().unwrap().0, Phase::Waiting) {
                    return Err(ControlError::invalid("this standby is sealed"));
                }
                if self.credentials.is_some() && !self.has_credentials.load(Ordering::SeqCst) {
                    return Err(ControlError::invalid(
                        "this standby waits for its credentials (--await-unlock): a Credentials \
                         step comes before Receive",
                    ));
                }
                // Read without holding the state: `Status` keeps answering.
                let records = read_records(fd)?;
                let mut inner = self.inner.lock().unwrap();
                if !matches!(inner.0, Phase::Waiting) {
                    return Err(ControlError::invalid("this standby is sealed"));
                }
                let names: Vec<String> = records
                    .iter()
                    .map(|r| r.record.mount.mountpoint.display().to_string())
                    .collect();
                if let Some(dup) = records.iter().enumerate().find_map(|(i, r)| {
                    let mp = &r.record.mount.mountpoint;
                    (inner.1.iter().any(|h| &h.record.mount.mountpoint == mp)
                        || records[..i]
                            .iter()
                            .any(|h| &h.record.mount.mountpoint == mp))
                    .then(|| mp.display().to_string())
                }) {
                    return Err(ControlError::invalid(format!("{dup} was already received")));
                }
                inner.1.extend(records);
                drop(inner);
                done(format!("holding {}", names.join(", ")))
            }
            Some(HandoffPhase::Seal) => {
                let mut inner = self.inner.lock().unwrap();
                match inner.0 {
                    Phase::Waiting if inner.1.is_empty() => {
                        return Err(ControlError::invalid("nothing was received to seal"))
                    }
                    Phase::Waiting => {}
                    _ => return Err(ControlError::invalid("this standby is already sealed")),
                }
                let deadline = p
                    .deadline_ms
                    .map(Duration::from_millis)
                    .unwrap_or(DEFAULT_DEADLINE);
                inner.0 = Phase::Sealed {
                    deadline: Instant::now() + deadline,
                    committed: false,
                };
                drop(inner);
                self.changed.notify_all();
                done(format!(
                    "sealed; serving once the state dir is free (giving up after {deadline:?} \
                     unless the sender commits)"
                ))
            }
            Some(HandoffPhase::Abort) => {
                let mut inner = self.inner.lock().unwrap();
                match inner.0 {
                    Phase::Sealed {
                        committed: true, ..
                    } => {
                        return Err(ControlError::invalid(
                            "too late to abort: the sender committed, and this standby holds \
                             the only copies of its sessions",
                        ))
                    }
                    Phase::Sealed { .. } if self.sender_committed() => {
                        return Err(ControlError::invalid(
                            "too late to abort: the sender committed, and this standby holds \
                             the only copies of its sessions",
                        ))
                    }
                    Phase::Waiting | Phase::Sealed { .. } => {
                        inner.0 = Phase::Failed {
                            reason: "aborted by request".into(),
                        };
                        // The descriptors go now: the sender serves its own.
                        inner.1.clear();
                    }
                    Phase::Failed { .. } => {}
                    Phase::Resuming | Phase::Resumed { .. } => {
                        return Err(ControlError::invalid(
                            "too late to abort: this standby holds the state dir and serves",
                        ))
                    }
                }
                drop(inner);
                self.changed.notify_all();
                done("aborted".into())
            }
            _ => Err(ControlError::invalid(
                "a standby receives a handoff (credentials, receive, seal, abort, status)",
            )),
        }
    }
}

/// Every record (and its descriptor) on a `Receive`'s stream, checked:
/// all of them, or none.
fn read_records(fd: OwnedFd) -> Result<Vec<Received>, ControlError> {
    let mut sock = UnixStream::from(fd);
    let io = |e: std::io::Error| ControlError::failed(format!("reading the handoff stream: {e}"));
    sock.set_nonblocking(false).map_err(io)?;
    sock.set_read_timeout(Some(STREAM_TIMEOUT)).map_err(io)?;
    let mut records = Vec::new();
    while let Some((body, fd)) =
        constellation_control::handoff_wire::read_record(&mut sock).map_err(io)?
    {
        let record: SocketRecord = serde_json::from_slice(&body)
            .map_err(|e| ControlError::invalid(format!("an unreadable handoff record: {e}")))?;
        if record.version != HANDOVER_VERSION {
            return Err(ControlError::invalid(format!(
                "a version-{} handoff record (this binary speaks {HANDOVER_VERSION})",
                record.version
            )));
        }
        // Plan 38 §3(e): only a `/dev/fuse` connection is ever resumed
        // (`check_resumable` would refuse it later; this refuses it while
        // the sender can still take it back).
        record
            .mount
            .init
            .check_resumable()
            .map_err(|e| ControlError::invalid(format!("not resumable: {e}")))?;
        records.push(Received { record, fd });
    }
    if records.is_empty() {
        return Err(ControlError::invalid(
            "the handoff stream carried no record",
        ));
    }
    Ok(records)
}

/// What a standby's wait ended with.
pub enum StandbyOutcome {
    /// The state dir's lock is ours (held for the process's life): start
    /// the node and [`resume`] these.
    Adopt(Adoption),
    /// Aborted, or a deadline passed: exit 0.
    GiveUp(String),
}

/// What a sealed standby resumes.
pub struct Adoption {
    standby: Arc<Standby>,
    received: Vec<Received>,
    signal: Option<&'static str>,
}

impl Adoption {
    /// A signal that came once sealed, before the sender committed (or
    /// after): the node handles it once it serves
    /// (`NodeRuntime::deliver_signal`).
    pub fn signal(&self) -> Option<&'static str> {
        self.signal
    }

    /// The handover generation the node serves as.
    pub fn generation(&self) -> u32 {
        self.received
            .iter()
            .map(|r| r.record.generation)
            .max()
            .unwrap_or(1)
    }
}

/// Wait as a standby on `socket` (module docs) until a sealed handoff can
/// take `state_dir`'s lock, or until it is aborted or times out. The
/// handoff socket keeps answering `Status` afterwards, for the life of the
/// process.
///
/// `credentials`: a standby that awaits them (`serve --await-unlock`, the
/// module docs). `stop`: the signal that ends the wait, once one came
/// (`None` until then) — at once until sealed; once sealed, at the seal's
/// deadline if the sender has not committed by then, and otherwise passed
/// on with the adoption ([`Adoption::signal`]).
pub fn standby(
    rt: &tokio::runtime::Handle,
    socket: &Path,
    state_dir: &Path,
    credentials: Option<CredentialsHook>,
    stop: tokio::sync::watch::Receiver<Option<&'static str>>,
) -> Result<StandbyOutcome> {
    let standby = Arc::new(Standby {
        inner: Mutex::new((Phase::Waiting, Vec::new())),
        changed: Condvar::new(),
        state_dir: state_dir.to_path_buf(),
        credentials,
        has_credentials: std::sync::atomic::AtomicBool::new(false),
    });
    // Before the socket: the plugin asks the sender for its credentials
    // as soon as this answers.
    let _waiting = StandbyMarker::write(state_dir)?;
    serve_standby(rt, socket, &standby)?;
    tracing::info!(
        socket = %socket.display(),
        state_dir = %state_dir.display(),
        "the state dir is held by another process: waiting as a handoff standby"
    );
    crate::startup::phase("waiting as a handoff standby");
    let give_up_at = Instant::now() + standby_timeout();
    // A signal that came once sealed: honoured at the deadline, or passed
    // on to the node once it serves.
    let mut noted: Option<&'static str> = None;
    loop {
        let signal = *stop.borrow();
        let mut inner = standby.inner.lock().unwrap();
        if let Some(signal) = signal {
            match &inner.0 {
                // Unsealed, the sender commits nothing: its own copies
                // serve again, as on an `Abort`.
                Phase::Waiting => {
                    let reason = format!("stopped by {signal}");
                    inner.0 = Phase::Failed {
                        reason: reason.clone(),
                    };
                    inner.1.clear();
                    return Ok(StandbyOutcome::GiveUp(reason));
                }
                // Sealed, the sender may commit at any moment — whether
                // this process still lives or not: giving up now, before
                // its marker exists, can leave no process holding the
                // sessions. So the signal only ends the wait as the
                // deadline does (the sender refuses a commit past its own,
                // earlier, one), and if the marker comes first it is the
                // node's to handle once it serves.
                Phase::Sealed { .. } if noted.is_none() => {
                    noted = Some(signal);
                    tracing::warn!(
                        signal,
                        "signal received while sealed: waiting for the sender's commit (then \
                         serving, and handling the signal) or the deadline (then exiting)"
                    );
                }
                _ => {}
            }
        }
        match &inner.0 {
            Phase::Failed { reason } => return Ok(StandbyOutcome::GiveUp(reason.clone())),
            Phase::Waiting if Instant::now() >= give_up_at => {
                inner.0 = Phase::Failed {
                    reason: "no handoff was sealed in time".into(),
                };
                inner.1.clear();
                return Ok(StandbyOutcome::GiveUp(
                    "no handoff was sealed in time".into(),
                ));
            }
            Phase::Waiting => {
                let _ = standby
                    .changed
                    .wait_timeout(inner, Duration::from_millis(500))
                    .unwrap();
            }
            Phase::Sealed {
                deadline,
                committed,
            } => {
                let (deadline, committed) = (*deadline, *committed);
                drop(inner);
                // The lock is taken without holding the state: an `Abort`
                // can still land between two attempts (until the sender's
                // marker is seen, which ends that).
                match crate::take_state_dir_lock(state_dir)? {
                    crate::LockOutcome::BecomeDaemon => {
                        let mut inner = standby.inner.lock().unwrap();
                        if let Phase::Failed { reason } = &inner.0 {
                            // Aborted just now: the lock is ours, but the
                            // descriptors are gone; exiting frees it.
                            return Ok(StandbyOutcome::GiveUp(reason.clone()));
                        }
                        inner.0 = Phase::Resuming;
                        let received = std::mem::take(&mut inner.1);
                        drop(inner);
                        // Ours now; the next handoff's sender starts clean.
                        let _ = std::fs::remove_file(marker(state_dir));
                        return Ok(StandbyOutcome::Adopt(Adoption {
                            standby: standby.clone(),
                            received,
                            signal: noted,
                        }));
                    }
                    crate::LockOutcome::Attach if committed => {
                        std::thread::sleep(Duration::from_millis(20))
                    }
                    crate::LockOutcome::Attach => {
                        // Checked under the state, so no `Abort` slips in
                        // between: from the marker on, the sender's copies
                        // are gone (or going) and these are the only ones.
                        let mut inner = standby.inner.lock().unwrap();
                        if !matches!(inner.0, Phase::Sealed { .. }) {
                            continue;
                        }
                        if standby.sender_committed() {
                            tracing::info!(
                                "the sender committed: waiting for it to exit, without a \
                                 deadline (these are the only copies of its sessions)"
                            );
                            inner.0 = Phase::Sealed {
                                deadline,
                                committed: true,
                            };
                        } else if Instant::now() >= deadline {
                            // No commit, and none can come any more (the
                            // sender refuses one past its own, earlier,
                            // deadline): it serves its own copies again.
                            let reason = match noted {
                                Some(signal) => format!(
                                    "stopped by {signal}; the sender did not commit before the \
                                     deadline"
                                ),
                                None => "the sender did not commit before the deadline".into(),
                            };
                            inner.0 = Phase::Failed {
                                reason: reason.clone(),
                            };
                            inner.1.clear();
                            return Ok(StandbyOutcome::GiveUp(reason));
                        } else {
                            drop(inner);
                            std::thread::sleep(Duration::from_millis(20));
                        }
                    }
                }
            }
            Phase::Resuming | Phase::Resumed { .. } => unreachable!("only this thread resumes"),
        }
    }
}

fn serve_standby(rt: &tokio::runtime::Handle, socket: &Path, standby: &Arc<Standby>) -> Result<()> {
    use constellation_control::methods::{NodeHandoff, NodePing};
    let _guard = rt.enter();
    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let listener = constellation_control::transport::UnixSocketListener::bind(socket)
        .with_context(|| format!("binding the handoff socket {}", socket.display()))?;
    let bound = std::fs::canonicalize(listener.path()).unwrap_or_else(|_| socket.to_path_buf());
    let (owner, _) = constellation_platform::native().process.effective_ids();
    let policy =
        crate::control::load_policy(crate::control::policy_path().as_deref(), owner, Some(bound));
    let mut router = constellation_control::Router::new().with_policy(policy);
    router.register::<NodePing, _, _>(|_c, _p| async { Ok(Pong {}) });
    let handler = standby.clone();
    router.register::<NodeHandoff, _, _>(move |mut ctx, p| {
        let handler = handler.clone();
        let fd = ctx.take_fd();
        async move {
            tokio::task::spawn_blocking(move || {
                if !matches!(
                    p.target,
                    constellation_control::proto::types::HandoffTarget::Socket
                ) {
                    return Err(ControlError::invalid(
                        "a standby only receives socket handoffs",
                    ));
                }
                handler.handle(&p, fd)
            })
            .await
            .map_err(|e| ControlError::failed(format!("the handoff handler panicked: {e}")))?
        }
    });
    // Held for the process's life, as the daemon's own control server.
    std::mem::forget(constellation_control::server::serve_router(
        listener,
        Arc::new(router),
    ));
    Ok(())
}

/// Resume every adopted view on `node` (module docs); the ones that fail
/// end their mounts and are reported by `Status`.
pub fn resume(node: &Arc<NodeRuntime>, adoption: Adoption) -> usize {
    let Adoption {
        standby, received, ..
    } = adoption;
    let mut failed = Vec::new();
    let mut served = 0;
    for Received { record, fd } in received {
        let mut mount = record.mount;
        let name = mount.mountpoint.display().to_string();
        mount.fuse_fd = fd.into_raw_fd();
        tracing::info!(view = %name, from = %record.from_version, "resuming a handed-off view");
        match handover::resume_mount(node, mount) {
            Ok(_) => served += 1,
            Err(e) => {
                tracing::error!(view = %name, error = %format!("{e:#}"),
                    "a handed-off view could not be resumed; its mount ends");
                failed.push(format!("{name}: {e:#}"));
            }
        }
    }
    standby.inner.lock().unwrap().0 = Phase::Resumed { failed };
    standby.changed.notify_all();
    served
}

/// Mark a standby that could not even start its node as failed (its
/// views end with the process).
pub fn failed(adoption: Adoption, reason: String) {
    let mut inner = adoption.standby.inner.lock().unwrap();
    inner.0 = Phase::Failed { reason };
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_platform::{CredentialSource as C, EphemeralSecretStore};

    /// A held pair goes out as `fs.unlock` takes it, an empty store as
    /// nothing, and a value a handoff cannot carry fails with a cause that
    /// names the key, never the value.
    #[test]
    fn the_sender_encodes_its_held_credentials() {
        let store = EphemeralSecretStore::new();
        assert!(unlock_credentials(&store).unwrap().is_none());
        store
            .replace(&[
                (C::ACCESS_KEY_ID, Some(b"AKID".as_slice())),
                (C::SECRET_ACCESS_KEY, Some(b"s\"e/c\\ret".as_slice())),
            ])
            .unwrap();
        let frame = unlock_credentials(&store).unwrap().unwrap();
        let back: constellation_control::proto::types::UnlockCredentials =
            serde_json::from_slice(&frame).unwrap();
        assert_eq!(back.access_key_id.unwrap().expose(), "AKID");
        assert_eq!(back.secret_access_key.unwrap().expose(), "s\"e/c\\ret");
        assert!(back.session_token.is_none());

        store
            .replace(&[(C::SECRET_ACCESS_KEY, Some(b"\xffSECRETBYTES".as_slice()))])
            .unwrap();
        let err = unlock_credentials(&store).unwrap_err();
        assert!(err.message.contains(C::SECRET_ACCESS_KEY), "{err:?}");
        assert!(err.message.contains("not UTF-8"), "{err:?}");
        assert!(!err.message.contains("SECRETBYTES"), "{err:?}");
    }

    /// The sender's `Credentials` gate: the node plugin's service grant,
    /// and a standby waiting on the state dir, before any commit.
    #[test]
    fn only_the_node_plugin_takes_credentials_for_a_pending_handoff() {
        let dir = tempfile::tempdir().unwrap();
        let grant = |label: &str| ServiceMatch {
            uid: 0,
            socket: dir.path().join("control.sock"),
            label: label.into(),
        };
        let plugin = grant(NODE_PLUGIN_LABEL);
        let other = grant("csi-controller");
        for service in [None, Some(&other)] {
            let err = may_take_credentials(service, dir.path(), false).unwrap_err();
            assert_eq!(
                err.kind,
                constellation_control::ErrorKind::Denied,
                "{err:?}"
            );
        }
        let err = may_take_credentials(Some(&plugin), dir.path(), false).unwrap_err();
        assert!(err.message.contains("no handoff is pending"), "{err:?}");
        let waiting = StandbyMarker::write(dir.path()).unwrap();
        may_take_credentials(Some(&plugin), dir.path(), false).unwrap();
        assert!(may_take_credentials(Some(&plugin), dir.path(), true).is_err());
        assert!(may_take_credentials(Some(&other), dir.path(), false).is_err());
        drop(waiting);
        assert!(may_take_credentials(Some(&plugin), dir.path(), false).is_err());
    }
}
