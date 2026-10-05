//! `NodeRuntime`: the daemon's host for one node (one state dir), shared
//! by every mounted view.
//!
//! Plan 31 C4c moved the node itself into the engine: `NodeRuntime::start`
//! builds `HostServices::native()`, an `EngineHost` on the daemon's
//! runtime with the node's budget, and adds the one engine this daemon
//! serves (plan 21: one daemon per state dir, any number of views);
//! `add_mount` opens a view of it (`Engine::open_view`) and mounts it
//! through `constellation-frontend-fuse` on a dedicated OS thread. What
//! stays here is what a host owns: the control socket and web UI (built
//! lazily, the first time a view is added, since the control service
//! reports the first view's readahead — `crate::control`), the FUSE sessions and their threads, the mount records
//! a takeover reads (`daemon_lock`), the control socket's locator
//! (`control.path`)/`daemon.pid`, and
//! signals.
//!
//! `remove_mount` unmounts exactly the requested view (via its
//! `FuseUnmounter`) and joins that view's thread, leaving siblings
//! untouched. Every view's session thread performs its own per-view
//! teardown (`Engine::close_view`: unregistration, ephemeral clone
//! removal) and, if it happens to be the last view standing, triggers
//! `NodeRuntime::shutdown` itself — this is what makes an
//! externally-triggered unmount (a bare `fusermount -u`, a kernel-forced
//! unmount, or a crash) behave the same as an explicit `remove_mount`
//! call.
//!
//! Plan 31 C4b adds the in-place upgrade (`crate::handover`, whose module
//! doc has the sequence): a view's session thread may also end
//! *detached*, in which case it tears nothing down (the view is handed
//! over), and [`NodeRuntime::wait_all`] keeps the process alive while a
//! handover is under way.

use anyhow::{bail, Context, Result};
use constellation_engine::{
    DeferredEvents, Engine, EngineConfig, EngineHost, EngineProfile, FsId, ResourceBudget, View,
    ViewSpec,
};
use constellation_platform::{EphemeralSecretStore, HostServices};
use constellation_types::Code;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::log_buffer;

/// Detach a stale FUSE mount left behind by a previous daemon that exited
/// without unmounting (crash, `kill`, or an orphaned view). Such a
/// mountpoint answers `stat` with `ENOTCONN`; if we don't clear it first,
/// building a fresh FUSE session over it fails with the same "Transport
/// endpoint is not connected". Best-effort and quiet on the common case
/// (no stale mount): only acts when the path actually reports `ENOTCONN`.
fn clear_stale_mount(host: &HostServices, mountpoint: &std::path::Path) {
    match std::fs::metadata(mountpoint) {
        // A live FUSE mount or an ordinary directory stats fine — leave it.
        Ok(_) => return,
        Err(e) if Code::from_io_error(&e) == Code::NotConnected => {}
        // Anything else (NotFound, permission, …) is not ours to fix here.
        Err(_) => return,
    }
    tracing::warn!(
        ?mountpoint,
        "detaching stale FUSE mount from a previous daemon before remounting"
    );
    // A lazy unmount (`fusermount3 -uz` on Linux, falling back to
    // `fusermount` for older systems) is the portable way to drop a dead
    // FUSE mount from userspace.
    if let Err(error) = host
        .mounts
        .unmount(mountpoint, constellation_platform::UnmountMode::Lazy)
    {
        tracing::warn!(
            ?mountpoint,
            %error,
            "could not detach stale mount automatically; \
             run `fusermount3 -uz <mountpoint>` if the remount fails"
        );
    }
}

/// What `add_mount`/`remove_mount` answer during an in-place upgrade (the
/// attaching `mount` matches it to retry).
pub const UPGRADING: &str = "the daemon is being upgraded in place; retry shortly";

/// Everything needed to start the daemon's node, independent of any
/// particular mounted view.
pub struct NodeConfig {
    /// Which filesystem this is, on the host (registry name or state dir).
    pub fs_id: FsId,
    pub engine: EngineConfig,
    pub web_ui: u16,
    pub log_buffer: log_buffer::LogBuffer,
    /// Plan 38 Z1b: what transport this daemon's *plain* mounts ask for
    /// (`--fuse-transport`/`CONSTELLATION_FUSE_TRANSPORT`, resolved once
    /// before the fork). Handover-capable sessions ignore it.
    pub fuse_transport: constellation_frontend_fuse::TransportConfig,
    /// Plan 31 C4b: this image's place in a chain of in-place upgrades
    /// (`None`: a fresh start), with the control socket the previous
    /// image bound.
    pub resumed: Option<Resumed>,
    /// Bind the control socket here instead of the per-user runtime dir
    /// (`constellation serve`: plan 37's engine pods put it on a hostPath
    /// the node plugin can reach).
    pub control_socket: Option<PathBuf>,
    /// Keep serving with no view mounted (`constellation serve`): the last
    /// view going away no longer shuts the node down; a signal does. A
    /// first `SIGTERM` waits until no view is left (plan 37 §7: an engine
    /// pod ignores `SIGTERM` while it serves a view, so kubelet's grace
    /// period, not the signal, bounds a pod deletion its node plugin did
    /// not drain); `SIGINT` and a second `SIGTERM` drain at once.
    pub persistent: bool,
    /// Handlers installed before the node started (`constellation
    /// serve`, which waits for `fs.unlock` or as a handoff standby first):
    /// the node takes them over instead of installing its own, so a signal
    /// that came in between is not lost. `None`: installed at start.
    pub signals: Option<StopSignals>,
}

/// `SIGTERM`/`SIGINT` handlers, installed once and handed on: a signal
/// that arrives while nobody waits on them is kept until somebody does
/// (a handler installed later would not see it).
#[cfg(unix)]
pub struct StopSignals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
}

#[cfg(not(unix))]
pub struct StopSignals;

#[cfg(unix)]
impl StopSignals {
    /// Within a runtime.
    pub fn install() -> Result<StopSignals> {
        use anyhow::Context;
        use tokio::signal::unix::{signal, SignalKind};
        Ok(StopSignals {
            term: signal(SignalKind::terminate()).context("installing a SIGTERM handler")?,
            int: signal(SignalKind::interrupt()).context("installing a SIGINT handler")?,
        })
    }

    /// The next one's name (`SIGTERM` or `SIGINT`). Cancel-safe.
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.term.recv() => SIGTERM,
            _ = self.int.recv() => SIGINT,
        }
    }
}

/// [`StopSignals::recv`]'s names, as [`NodeRuntime::deliver_signal`]
/// takes them.
pub const SIGTERM: &str = "SIGTERM";
pub const SIGINT: &str = "SIGINT";

/// What a handed-over image inherits besides its views
/// (`crate::handover`).
pub struct Resumed {
    pub generation: u32,
    pub control: Option<std::os::unix::net::UnixListener>,
}

/// Everything needed to mount one view (root, subtree, or snapshot
/// selector) of an already-running `NodeRuntime`.
pub struct ViewConfig {
    /// Raw inner-path / `@snapshot` selector argument, as given on the
    /// command line (`"/"` for the root).
    pub inner_path: String,
    pub mountpoint: PathBuf,
    pub allow_other: bool,
    pub fs_name: String,
    pub fuse_threads: usize,
    pub rw_snapshot: bool,
    pub clone_name: Option<String>,
    pub ephemeral: bool,
    /// `--confine-links` (plan 31 §6.12).
    pub confine_links: bool,
    /// Plan 31 §9.10 (`view.mount`): labels and per-view admission limits.
    pub labels: std::collections::BTreeMap<String, String>,
    pub qos: constellation_engine::ViewQos,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MountId(u64);

impl MountId {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// A snapshot of one mounted view, for listing (`MountList`, `fs list`).
pub struct MountInfo {
    pub id: MountId,
    pub subtree: String,
    pub mountpoint: PathBuf,
    pub since: Instant,
    /// What this mount's FUSE session reports about its transport (plan
    /// 38 §5): the negotiated transport, ring queue depth, the fallback
    /// its handshake took, zero-copy reads.
    pub fuse: Arc<constellation_frontend_fuse::SessionStats>,
    pub view: Arc<View>,
    pub qos: constellation_engine::ViewQos,
    pub confine_links: bool,
}

pub(crate) struct MountHandle {
    pub(crate) subtree: String,
    pub(crate) mountpoint: PathBuf,
    since: Instant,
    /// Plan 38 §5: what this session reports about its transport — the
    /// one `FUSE_INIT` negotiated (`dev_fuse`/`uring`/`uring_zc`, fixed
    /// for the connection's life), its ring queue depth and the fallback
    /// it took — reported per mount by `node.status.fuse`.
    pub(crate) fuse: Arc<constellation_frontend_fuse::SessionStats>,
    unmounter: Mutex<constellation_frontend_fuse::FuseUnmounter>,
    /// Plan 31 C4b: what a handover needs of the view and its session.
    pub(crate) fs_name: String,
    pub(crate) allow_other: bool,
    pub(crate) read_only: bool,
    pub(crate) fuse_threads: usize,
    pub(crate) control: constellation_frontend_fuse::SessionControl,
    pub(crate) sink: constellation_frontend_fuse::FuseNotifySink,
    pub(crate) caps: constellation_vfs::FrontendCaps,
    pub(crate) view: Arc<View>,
    pub(crate) qos: constellation_engine::ViewQos,
    pub(crate) confine_links: bool,
}

pub struct NodeRuntime {
    host: HostServices,
    /// The process's engines: this daemon's one (plan 21). Held for the
    /// daemon's life; the views reach the engine through `engine`.
    #[allow(dead_code)]
    engines: EngineHost,
    engine: Arc<Engine>,
    web_ui: u16,
    log_buffer: log_buffer::LogBuffer,
    /// Built lazily, the first time a view is added (see module docs).
    status: Mutex<Option<Arc<constellation_engine::control::EngineControl>>>,
    pub(crate) mounts: Mutex<HashMap<MountId, MountHandle>>,
    /// Session-thread handles, kept separate from `mounts` so a thread's
    /// own teardown (which removes its `mounts` entry) never races
    /// `remove_mount`'s attempt to join it.
    threads: Mutex<HashMap<MountId, std::thread::JoinHandle<()>>>,
    shutdown_started: AtomicBool,
    /// Plan 31 C4b: the handover state (`crate::handover`).
    pub(crate) handover: crate::handover::HandoverState,
    /// The node's own settings, as the next image restarts it.
    pub(crate) handoff_config: crate::handover::NodeHandoff,
    /// Plan 38 Z1b: `NodeConfig::fuse_transport`, for every mount this
    /// daemon makes from now on.
    pub(crate) fuse_transport: constellation_frontend_fuse::TransportConfig,
    /// The control socket a previous image bound (served from, instead of
    /// binding anew, by `ensure_status`).
    control_listener: Mutex<Option<std::os::unix::net::UnixListener>>,
    /// [`NodeConfig::control_socket`].
    control_socket: Option<PathBuf>,
    /// [`NodeConfig::persistent`].
    persistent: bool,
    /// A persistent node got a signal while it served views: it shuts
    /// down when the last of them goes ([`NodeConfig::persistent`]).
    stop_when_empty: AtomicBool,
    /// Set once [`Self::shutdown`] has finished ([`Self::wait_stopped`]).
    stopped: (Mutex<bool>, std::sync::Condvar),
    /// The view a headless node serves its control API from
    /// ([`Self::serve_headless`]); never FUSE-mounted.
    headless_view: Mutex<Option<Arc<View>>>,
    /// [`Self::keep_handoff_secrets`].
    handoff_secrets: Mutex<Option<EphemeralSecretStore>>,
    /// [`Self::deliver_signal`]: `true` for a `SIGTERM`.
    delivered_signals: tokio::sync::mpsc::UnboundedSender<bool>,
}

/// The options `add_mount` mounts a view with. Plan 38 §3(e): a
/// `view.mount{PreopenedFd}` session (`preopened`) is one somebody else
/// mounted and may ask back for (plan 37's CSI engine-pod replacement hands
/// the descriptor on), so it is handover-capable and pinned to `/dev/fuse`
/// whatever the transport knob says. A mount this process made is plain:
/// it takes the knob's policy, and `node.handoff`/`daemon --upgrade`
/// refuses to detach it if that policy got it a ring (the refusal names
/// the transport).
fn mount_options_for(
    fs_name: String,
    fuse_threads: usize,
    tuning: constellation_frontend_fuse::KernelTuning,
    transport: constellation_frontend_fuse::TransportConfig,
    preopened: bool,
) -> constellation_frontend_fuse::MountOptions {
    if preopened {
        constellation_frontend_fuse::MountOptions::handover_capable(
            fs_name,
            fuse_threads,
            tuning,
            transport,
            constellation_frontend_fuse::HandoverCapable,
        )
    } else {
        constellation_frontend_fuse::MountOptions::new(fs_name, fuse_threads, tuning, transport)
    }
}

/// Plan 38 Z2c: the transport a plain mount asks for when neither
/// `--fuse-transport` nor `CONSTELLATION_FUSE_TRANSPORT` says — the engine
/// profile's (`EngineProfile::fuse_transport`: the ladder, except under
/// `CONSTELLATION_PROFILE=mobile`). Read from the environment the daemon
/// will pick its profile from, so it agrees with [`NodeRuntime::start`];
/// an unknown profile is the same error here, before the fork.
pub(crate) fn profile_transport() -> Result<constellation_frontend_fuse::TransportPolicy> {
    let profile = EngineProfile::from_env(EngineProfile::desktop()).map_err(anyhow::Error::msg)?;
    Ok(transport_of(profile.fuse_transport))
}

fn transport_of(
    mode: constellation_engine::FuseTransportMode,
) -> constellation_frontend_fuse::TransportPolicy {
    match mode {
        constellation_engine::FuseTransportMode::Auto => {
            constellation_frontend_fuse::TransportPolicy::Auto
        }
        constellation_engine::FuseTransportMode::DevFuse => {
            constellation_frontend_fuse::TransportPolicy::DevFuse
        }
    }
}

impl NodeRuntime {
    /// This daemon's node.
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Start the daemon's node: an `EngineHost` on `rt` whose budget is
    /// this node's own (so the one engine gets exactly what it asked
    /// for), with the node's engine in it. No view is mounted yet.
    pub fn start(cfg: NodeConfig, rt: tokio::runtime::Handle) -> Result<Arc<Self>> {
        let NodeConfig {
            fs_id,
            mut engine,
            web_ui,
            log_buffer,
            resumed,
            fuse_transport,
            control_socket,
            persistent,
            signals,
        } = cfg;
        let handoff_config = crate::handover::NodeHandoff::of(&engine, web_ui, fuse_transport);
        let (generation, control_listener) = match resumed {
            Some(r) => (r.generation, r.control),
            None => (0, None),
        };
        let host = HostServices::native();
        let engines = EngineHost::start(
            rt.clone(),
            ResourceBudget {
                memory_bytes: host.process.memory_budget().unwrap_or(u64::MAX),
                cache_bytes: engine.cache_size,
                staging_bytes: u64::MAX,
            },
        );
        if engine.on_phase.is_none() {
            engine.on_phase = Some(Arc::new(crate::startup::phase));
        }
        // Plan 31 C8: the daemon is a desktop/server host; the environment
        // may pick another profile (`EngineProfile::from_env`:
        // `CONSTELLATION_PROFILE` and its per-mode overrides).
        let profile =
            EngineProfile::from_env(EngineProfile::desktop()).map_err(anyhow::Error::msg)?;
        let engine = engines.add_engine(fs_id, engine, host.clone(), profile)?;
        // Plan 38 Z3b: `--cache-verify always` (or its env override, which
        // only the engine has resolved) turns passthrough off for every
        // mount, with that as the reason `node.status` reports.
        let fuse_transport =
            if engine.cache().verify_mode() == constellation_fs_core::cache::CacheVerify::Always {
                fuse_transport.with_cache_verify_always()
            } else {
                fuse_transport
            };
        let (delivered_signals, mut delivered) = tokio::sync::mpsc::unbounded_channel();
        let node = Arc::new(NodeRuntime {
            host,
            engines,
            engine,
            web_ui,
            log_buffer,
            status: Mutex::new(None),
            mounts: Mutex::new(HashMap::new()),
            threads: Mutex::new(HashMap::new()),
            shutdown_started: AtomicBool::new(false),
            handover: crate::handover::HandoverState::new(generation),
            handoff_config,
            fuse_transport,
            control_listener: Mutex::new(control_listener),
            control_socket,
            persistent,
            stop_when_empty: AtomicBool::new(false),
            stopped: (Mutex::new(false), std::sync::Condvar::new()),
            headless_view: Mutex::new(None),
            handoff_secrets: Mutex::new(None),
            delivered_signals,
        });

        // Signals are node-level: unmount every currently-mounted view,
        // then run the one clean node shutdown. The actual unmount+join
        // work happens on a plain OS thread (not this async task) so a
        // slow drain never blocks a tokio worker; a second signal aborts
        // immediately regardless of how far that drain got. A signal
        // `deliver_signal` passes on counts as one received here.
        {
            let node = node.clone();
            rt.spawn(async move {
                #[cfg(unix)]
                {
                    let mut signals = match signals {
                        Some(signals) => signals,
                        None => match StopSignals::install() {
                            Ok(signals) => signals,
                            Err(e) => {
                                tracing::warn!(error = %format!("{e:#}"),
                                    "failed to install the signal handlers; SIGTERM and Ctrl-C \
                                     will not unmount");
                                return;
                            }
                        },
                    };
                    // Plan 37 §7: only SIGTERM is deferred, and only on a
                    // persistent node serving views — they are served for
                    // somebody else (a CSI node plugin's staging mounts),
                    // go when it unmounts them, and the node with the last
                    // one. SIGINT (an interactive Ctrl-C) drains at once,
                    // and so does a second SIGTERM: unmount, flush, exit.
                    let mut deferred = false;
                    loop {
                        let term = tokio::select! {
                            name = signals.recv() => name == SIGTERM,
                            Some(term) = delivered.recv() => term,
                        };
                        if term {
                            tracing::info!("SIGTERM received; unmounting FUSE");
                        } else {
                            tracing::info!("SIGINT received; unmounting FUSE");
                        }
                        if !term || !node.persistent || deferred {
                            break;
                        }
                        // Checked again after the flag is set, so a last
                        // view leaving in between is not missed (its
                        // thread reads the flag after removing it).
                        if node.mounts.lock().unwrap().is_empty() {
                            break;
                        }
                        node.stop_when_empty.store(true, Ordering::SeqCst);
                        let views = node.mounts.lock().unwrap().len();
                        if views == 0 {
                            break;
                        }
                        deferred = true;
                        tracing::warn!(
                            views,
                            "SIGTERM received while views are mounted: staying up until the last \
                             one is unmounted, then exiting (a second SIGTERM drains now)"
                        );
                    }
                    let drain = node.clone();
                    std::thread::spawn(move || {
                        for id in drain.mount_ids() {
                            if let Err(e) = drain.remove_mount(id) {
                                tracing::warn!(error = %e, "signal-triggered unmount failed");
                            }
                        }
                        // A persistent node outlives its last view, so the
                        // signal itself is what ends it.
                        if drain.persistent {
                            let _ = drain.shutdown();
                        }
                    });
                    // A second signal during the post-unmount drain aborts immediately
                    // so a hung ship/upload cannot trap the process forever.
                    tokio::select! {
                        _ = signals.recv() => {}
                        Some(_) = delivered.recv() => {}
                    }
                    tracing::error!("second signal during shutdown; exiting immediately");
                    std::process::exit(130);
                }
                #[cfg(not(unix))]
                {
                    let _ = (signals, delivered);
                    tracing::warn!("signal-driven FUSE unmount is only supported on Unix");
                }
            });
        }

        Ok(node)
    }

    /// Mount one view (root, subtree, or snapshot selector) and spawn its
    /// FUSE session on a dedicated OS thread. Returns immediately; the
    /// thread runs until the view is unmounted (via `remove_mount`, an
    /// external `fusermount -u`, or process shutdown).
    pub fn add_mount(self: &Arc<Self>, view: ViewConfig) -> Result<MountId> {
        self.add_mount_from(view, None)
    }

    /// [`Self::add_mount`] on a `/dev/fuse` descriptor someone else mounted
    /// (`view.mount` with `MountSource::PreopenedFd`, plan 31 §6.11): the
    /// session starts with `FUSE_INIT` on `fd`. The view is known by
    /// `mounted_at` — where the sender says it mounted it (plan 37's
    /// staging path, which need not exist in this mount namespace) — or
    /// else by `fd:<n>`. This process never unmounts it: removing the view
    /// ends the session and closes the connection
    /// (`SessionControl::unmount` on a foreign mount), and the sender
    /// unmounts its mount.
    pub fn add_mount_fd(
        self: &Arc<Self>,
        mut view: ViewConfig,
        fd: std::os::fd::OwnedFd,
        mounted_at: Option<PathBuf>,
    ) -> Result<MountId> {
        let name = match mounted_at {
            Some(path) => path,
            None => PathBuf::from(format!("fd:{}", std::os::fd::AsRawFd::as_raw_fd(&fd))),
        };
        // The name is what `view.unmount` finds the view by: two views
        // answering to one name would make it unmount the wrong one.
        if self
            .mounts
            .lock()
            .unwrap()
            .values()
            .any(|m| m.mountpoint == name)
        {
            bail!("a view is already mounted at {}", name.display());
        }
        view.mountpoint = name;
        self.add_mount_from(view, Some(fd))
    }

    fn add_mount_from(
        self: &Arc<Self>,
        view: ViewConfig,
        preopened: Option<std::os::fd::OwnedFd>,
    ) -> Result<MountId> {
        // Plan 31 C4b: a view added now would not be handed over (the
        // node is being shut down for the next image): the attaching
        // `mount` retries on this message, and the next image takes it.
        if self.handover.in_progress() {
            bail!("{UPGRADING}");
        }
        // Refuse to attach a view onto a daemon whose final shutdown has
        // already begun. Once the last view is removed the FUSE thread
        // runs `shutdown()` (drain + ship, then exit); a view added after
        // that point is never joined, so when the drain completes the
        // process exits and orphans the new kernel mount, leaving a dead
        // mountpoint (`Transport endpoint is not connected`). Rejecting
        // here lets the client fall through to `BecomeDaemon` cleanly.
        if self.shutdown_started.load(Ordering::SeqCst) || self.engine.is_shutting_down() {
            bail!("daemon is shutting down; retry once it has exited");
        }
        let ViewConfig {
            inner_path,
            mountpoint,
            allow_other,
            fs_name,
            fuse_threads,
            rw_snapshot,
            clone_name,
            ephemeral,
            confine_links,
            labels,
            qos,
        } = view;
        let spec = ViewSpec {
            root: inner_path.clone(),
            rw_snapshot,
            clone_name,
            ephemeral,
            confine_links,
            labels,
            qos,
        };
        // What the FUSE frontend declares for this view: it forwards
        // POSIX/`flock` locks under `--locks cluster`, except on a frozen
        // snapshot view, where nothing can be written.
        let frozen_view = spec.is_frozen();
        let caps = constellation_frontend_fuse::caps(self.engine.locks_cluster() && !frozen_view);
        // The FUSE session's notifier exists only once it is mounted, and
        // the mount needs the view: invalidations reach it from then on.
        let events = DeferredEvents::new();
        let fs = self.engine.open_view(spec, caps.clone(), events.clone())?;

        // First view: build the status object + start the control API and
        // web UI (see module docs for why this waits for a mountpoint).
        self.ensure_status(&fs);

        let mount_record_name = fs_name.clone();
        // Self-heal a stale mountpoint left by a previous daemon that
        // exited without unmounting (crash, kill, or an orphaned view
        // attached during shutdown). Such a mountpoint answers stat with
        // `ENOTCONN`; a fresh `Session::new` on it fails with the same
        // "Transport endpoint is not connected". Lazily detach it first so
        // the remount just works instead of surfacing os error 107.
        if preopened.is_none() {
            clear_stale_mount(&self.host, &mountpoint);
        }
        let engine = &self.engine;
        tracing::info!(?mountpoint, state_dir = ?engine.state_dir(), fs = %engine.fsmeta().uuid, "mounting");
        // The kernel queue is sized for the host's worker count (the
        // node-wide thread plan), whatever this view's own count.
        let tuning = constellation_frontend_fuse::KernelTuning::for_workers(
            crate::parallelism::thread_plan().fuse,
        );
        let mut mount_options = mount_options_for(
            fs_name,
            fuse_threads,
            tuning,
            self.fuse_transport,
            preopened.is_some(),
        );
        mount_options.allow_other = allow_other;
        mount_options.read_only = frozen_view;
        // An explicit session, so `remove_mount`/signals can unmount from
        // inside this process (its `FuseUnmounter`); without it, an
        // external kill leaves a dead mountpoint that needs `fusermount3
        // -u`.
        let source = match preopened {
            Some(fd) => constellation_frontend_fuse::MountSource::PreopenedFd(fd),
            None => mount_options.source(&mountpoint),
        };
        let session = match constellation_frontend_fuse::mount_source(
            fs.clone(),
            source,
            &mount_options,
            caps.clone(),
        ) {
            Ok(session) => session,
            Err(error) => {
                engine.close_view(&fs);
                return Err(error).context("FUSE mount");
            }
        };
        let sink = session.notifier();
        events.set(Arc::new(sink.clone()));
        Ok(self.serve_session(
            session,
            fs,
            SessionInfo {
                subtree: inner_path,
                mountpoint,
                fs_name: mount_record_name,
                allow_other,
                read_only: frozen_view,
                fuse_threads,
                sink,
                caps,
                qos,
                confine_links,
            },
        ))
    }

    /// Record a mounted session and run it on its own OS thread (see the
    /// module doc for the thread's teardown).
    pub(crate) fn serve_session(
        self: &Arc<Self>,
        mut session: constellation_frontend_fuse::FuseSession<View>,
        fs: Arc<View>,
        info: SessionInfo,
    ) -> MountId {
        let SessionInfo {
            subtree,
            mountpoint,
            fs_name,
            allow_other,
            read_only,
            fuse_threads,
            sink,
            caps,
            qos,
            confine_links,
        } = info;
        let unmounter = session.unmounter();
        let control = session.control();
        // Plan 38 §2.4/§5: what the ladder actually negotiated for this
        // connection, logged once here and reported per mount by
        // `node.status`. A mount that asked for `auto` and got
        // `dev_fuse` is the ladder degrading as designed -- fuser logged
        // the reason during the handshake -- not a failure.
        let fuse = session.stats();
        match fuse.last_fallback() {
            None => tracing::info!(
                ?mountpoint,
                transport = fuse.transport().name(),
                "FUSE transport"
            ),
            Some(fallback) => tracing::info!(
                ?mountpoint,
                transport = fuse.transport().name(),
                fallback_from = fallback.from.name(),
                fallback_reason = fallback.reason.as_str(),
                detail = %fallback.detail,
                "FUSE transport (fell back)"
            ),
        }
        let id = MountId(fs.id());
        // So that a takeover after a kill can abort this mount's
        // connection if it is left wedged (`daemon_lock::abort_stale_mounts`).
        if let Err(e) =
            crate::daemon_lock::record_mount(self.engine.state_dir(), id.0, &mountpoint, &fs_name)
        {
            tracing::warn!(error = %e, "recording the mount in the state dir failed");
        }
        self.mounts.lock().unwrap().insert(
            id,
            MountHandle {
                subtree,
                mountpoint,
                since: Instant::now(),
                fuse,
                unmounter: Mutex::new(unmounter),
                fs_name,
                allow_other,
                read_only,
                fuse_threads,
                control,
                sink,
                caps,
                view: fs.clone(),
                qos,
                confine_links,
            },
        );

        let node = self.clone();
        let thread = std::thread::spawn(move || {
            match session.run() {
                // Handed over (`crate::handover`): the view, its record
                // and its mount entry are the handover's to carry on.
                Ok(constellation_frontend_fuse::SessionExit::Detached) => return,
                Ok(constellation_frontend_fuse::SessionExit::Unmounted) => {}
                Err(e) => tracing::warn!(error = %e, "FUSE session ended with an error"),
            }
            crate::daemon_lock::forget_mount(node.engine.state_dir(), id.0);
            tracing::info!("FUSE detached");
            node.engine.close_view(&fs);
            drop(fs);
            // This view is gone: drop its bookkeeping entry, and if it
            // was the last one, run the one node-wide clean shutdown.
            let now_empty = {
                let mut mounts = node.mounts.lock().unwrap();
                mounts.remove(&id);
                mounts.is_empty()
            };
            if now_empty && !node.persistent {
                // `shutdown` logs its own failure (and records it for the
                // process's exit status).
                let _ = node.shutdown();
            } else if now_empty && node.stop_when_empty.load(Ordering::SeqCst) {
                // The deferred signal (`NodeConfig::persistent`). The view
                // went through `view.unmount`, which joins this thread
                // before it answers: shutting down here would end the
                // process before that answer is written, so the drain runs
                // on its own thread, a moment later.
                std::thread::spawn(move || {
                    std::thread::sleep(Duration::from_millis(500));
                    tracing::info!("the last view is gone; exiting on the earlier signal");
                    let _ = node.shutdown();
                });
            }
        });
        self.threads.lock().unwrap().insert(id, thread);
        id
    }

    /// The status object and the control API/web UI, started the first
    /// time a view is added.
    pub(crate) fn ensure_status(self: &Arc<Self>, first: &Arc<View>) {
        let mut status_guard = self.status.lock().unwrap();
        if status_guard.is_some() {
            return;
        }
        let e = &self.engine;
        // Bound here (not by a `serve` that binds for us) so that a handover
        // can pass the very listener on (`crate::handover`): clients that
        // connect during it wait in the backlog for the next image. The
        // socket lives in the per-user runtime dir; the state dir records
        // where (`control.path`), for every client that knows only it.
        //
        // Resolved and bound *before* the router is built, so the router's
        // policy can be told this daemon's own canonical socket path
        // (plan 33 U1's `kind = "service"` grants match against it, never
        // anything a client claims — see `Policy::with_bound_socket`).
        // `UnixSocketListener::bind`/`from_std` register with Tokio's
        // reactor, so this needs the engine's runtime entered first — the
        // same guard the actual `serve_router` call below has always needed.
        let _guard = e.runtime().enter();
        let bind_result = (|| -> Result<constellation_control::transport::UnixSocketListener> {
            use constellation_control::transport::{
                locate_socket, socket_path_for_state_dir, UnixSocketListener,
            };
            let state_dir = e.state_dir();
            let inherited = self.control_listener.lock().unwrap().take();
            match inherited {
                Some(listener) => {
                    // An explicit socket (`serve`'s, whose credential gate
                    // bound it first) is where it is bound; a handed-over
                    // one is where the old image recorded it.
                    let path = match (&self.control_socket, locate_socket(state_dir)) {
                        (Some(path), _) => path.clone(),
                        (None, Some(path)) => path,
                        (None, None) => socket_path_for_state_dir(&*self.host.dirs, state_dir)?,
                    };
                    UnixSocketListener::from_std(listener, &path)
                        .context("serving the handed-over control socket")
                }
                None => {
                    let path = match &self.control_socket {
                        Some(path) => path.clone(),
                        None => socket_path_for_state_dir(&*self.host.dirs, state_dir)?,
                    };
                    UnixSocketListener::bind(&path)
                        .with_context(|| format!("binding control socket {}", path.display()))
                }
            }
        })();
        let bound_socket = bind_result.as_ref().ok().map(|listener| {
            let path = listener.path();
            std::fs::canonicalize(path).unwrap_or_else(|error| {
                // We just bound it, so this should not happen; if it does,
                // the literal path is still the best thing to match a
                // `kind = "service"` grant against, but say so — the grant
                // may now fail to match a canonicalised path.
                tracing::warn!(path = %path.display(), %error,
                    "cannot canonicalise the control socket just bound");
                path.to_path_buf()
            })
        });
        let host = Arc::new(crate::control::DaemonHost {
            node: Arc::downgrade(self),
        });
        let status = constellation_engine::control::EngineControl::new(
            e.clone(),
            host,
            self.log_buffer.clone(),
            first.prefetch_stats(),
            env!("CONSTELLATION_VERSION"),
        );
        *status_guard = Some(status.clone());
        drop(status_guard);
        let router = Arc::new(crate::control::daemon_router(
            &status,
            e.state_dir(),
            bound_socket,
        ));
        let served = (|| -> Result<()> {
            use constellation_control::transport::record_socket;
            let listener = bind_result?;
            let state_dir = e.state_dir();
            *self.handover.control.lock().unwrap() = Some(listener.try_clone_std()?);
            record_socket(state_dir, listener.path()).context("recording control.path")?;
            // Held for the daemon's life: dropping the handle does not stop
            // the server, and the listener's own drop (which would remove
            // the socket file) never runs before exit or `exec`.
            std::mem::forget(constellation_control::server::serve_router(
                listener,
                router.clone(),
            ));
            Ok(())
        })();
        if let Err(err) = served {
            tracing::warn!(error = %format!("{err:#}"), "control API unavailable");
        }
        if self.web_ui != 0 {
            match e
                .runtime()
                .block_on(constellation_control::web::serve(self.web_ui, router))
            {
                Ok(address) => {
                    tracing::info!(%address, "web UI listening (localhost only)");
                    // Plan 32 §6.3: under `CONSTELLATION_SNAPACCT=auto` an
                    // enabled web UI keeps the accounting index maintained.
                    e.snapacct().set_web_ui(true);
                }
                Err(error) => tracing::warn!(%error, "web UI unavailable"),
            }
        }
    }

    /// Unmount exactly this view (via its `FuseUnmounter`) and join its
    /// session thread. Siblings are untouched. If this was the last view,
    /// the thread itself runs `shutdown()` before this call returns.
    pub fn remove_mount(&self, id: MountId) -> Result<()> {
        if self.handover.in_progress() {
            bail!("{UPGRADING}");
        }
        {
            let mounts = self.mounts.lock().unwrap();
            let handle = mounts
                .get(&id)
                .with_context(|| format!("no such mount: {id:?}"))?;
            let unmount_result = handle.unmounter.lock().unwrap().unmount();
            if let Err(e) = unmount_result {
                tracing::warn!(error = %e, "unmount request failed (already unmounted?)");
            }
        }
        if let Some(thread) = self.threads.lock().unwrap().remove(&id) {
            if thread.join().is_err() {
                tracing::warn!("FUSE session thread panicked");
            }
        }
        Ok(())
    }

    /// Block until every view's session has ended, and no handover is
    /// under way (one that has to be abandoned serves the views again, on
    /// new threads). What keeps a daemon's main thread alive.
    pub fn wait_all(&self) {
        loop {
            let threads: Vec<_> = self.threads.lock().unwrap().drain().collect();
            if threads.is_empty() {
                if self.handover.in_progress() {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                return;
            }
            for (_, thread) in threads {
                if thread.join().is_err() {
                    tracing::warn!("FUSE session thread panicked");
                }
            }
        }
    }

    pub fn mounts(&self) -> Vec<MountInfo> {
        self.mounts
            .lock()
            .unwrap()
            .iter()
            .map(|(id, handle)| MountInfo {
                id: *id,
                subtree: handle.subtree.clone(),
                mountpoint: handle.mountpoint.clone(),
                since: handle.since,
                fuse: handle.fuse.clone(),
                view: handle.view.clone(),
                qos: handle.qos,
                confine_links: handle.confine_links,
            })
            .collect()
    }

    fn mount_ids(&self) -> Vec<MountId> {
        self.mounts.lock().unwrap().keys().copied().collect()
    }

    /// Clean shutdown: the engine's drain (`Engine::shutdown`), then
    /// remove `control.sock`/`daemon.pid` so a waiting `umount`/`export`
    /// (or a later `mount` probing for a live daemon) sees this process
    /// is really gone rather than timing out. Idempotent — called once,
    /// when the last mount is removed or on signal; later calls are a
    /// harmless no-op. A drain that leaves something unshipped is
    /// returned and recorded for [`Self::shutdown_error`], so the
    /// foreground `mount` exits non-zero.
    pub fn shutdown(&self) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(view) = self.headless_view.lock().unwrap().take() {
            self.engine.close_view(&view);
        }
        let result = self.engine.shutdown();
        // Best-effort, and unconditional even if the drain above failed:
        // a process that is exiting either way must not leave files
        // behind that make it look like a live daemon is still here.
        let state_dir = self.engine.state_dir();
        constellation_control::transport::forget_socket(state_dir);
        let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
        let (stopped, cv) = &self.stopped;
        *stopped.lock().unwrap() = true;
        cv.notify_all();
        result
    }

    /// Serve the control API on `listener`, already bound at the
    /// configured control socket (`serve --await-unlock` bound it to wait
    /// for its credentials), instead of binding it anew: clients that
    /// connected meanwhile wait in its backlog. Before
    /// [`Self::serve_headless`].
    pub fn adopt_control_listener(&self, listener: std::os::unix::net::UnixListener) {
        *self.control_listener.lock().unwrap() = Some(listener);
    }

    /// Keep `secrets` — what `fs.unlock` gave `serve --await-unlock` (or
    /// what its predecessor handed over), the very store its static
    /// credential source signs from, so a rotation is in it too — for a
    /// socket handoff's `Credentials` step to give its successor
    /// (`crate::handoff_socket`). In memory only, like the store itself.
    pub(crate) fn keep_handoff_secrets(&self, secrets: EphemeralSecretStore) {
        *self.handoff_secrets.lock().unwrap() = Some(secrets);
    }

    /// Act on a `SIGTERM` or `SIGINT` (`name`, as [`StopSignals::recv`]
    /// names it) as if the node's own handler had received it just now
    /// (§7's deferral included): one that came before the node could
    /// handle it (`constellation serve`: a handoff standby that got it
    /// once sealed, and adopted its views anyway).
    pub fn deliver_signal(&self, name: &'static str) {
        let _ = self.delivered_signals.send(name == SIGTERM);
    }

    /// What [`Self::keep_handoff_secrets`] kept.
    pub(crate) fn handoff_secrets(&self) -> Option<EphemeralSecretStore> {
        self.handoff_secrets.lock().unwrap().clone()
    }

    /// A node with no FUSE view at all (`constellation serve`): open an
    /// unmounted view of the root for the control service to report from,
    /// and start the control API. Fails when the control socket could not
    /// be bound — a headless node is reachable through nothing else.
    pub fn serve_headless(self: &Arc<Self>) -> Result<()> {
        let caps = constellation_vfs::FrontendCaps {
            push_inval: constellation_vfs::PushInval::None,
            cluster_locks: false,
            ..constellation_frontend_fuse::caps(false)
        };
        let view = self
            .engine
            .open_view(ViewSpec::new("/"), caps, DeferredEvents::new())?;
        self.ensure_status(&view);
        *self.headless_view.lock().unwrap() = Some(view);
        if self.handover.control.lock().unwrap().is_none() {
            bail!("the control API could not be started (see the log above)");
        }
        Ok(())
    }

    /// Close the headless view (`serve`'s), if any: a socket handoff's
    /// commit closes every view before the engine drains.
    pub(crate) fn close_headless_view(&self) {
        if let Some(view) = self.headless_view.lock().unwrap().take() {
            self.engine.close_view(&view);
        }
    }

    /// Remove this daemon's control socket and its records (`control.path`,
    /// `daemon.pid`): a socket handoff's sender, about to exit, so nothing
    /// finds a socket nobody will answer once the receiver binds its own.
    pub(crate) fn forget_control_socket(&self) {
        let state_dir = self.engine.state_dir();
        if let Some(path) = &self.control_socket {
            let _ = std::fs::remove_file(path);
        }
        constellation_control::transport::forget_socket(state_dir);
        let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
    }

    /// Block until [`Self::shutdown`] has finished.
    pub fn wait_stopped(&self) {
        let (stopped, cv) = &self.stopped;
        let mut done = stopped.lock().unwrap();
        while !*done {
            done = cv.wait(done).unwrap();
        }
    }

    /// Why the node-wide shutdown left something unshipped, if it did.
    pub fn shutdown_error(&self) -> Option<String> {
        self.engine.shutdown_error()
    }
}

/// A session's mount-level settings ([`NodeRuntime::serve_session`]).
pub(crate) struct SessionInfo {
    pub(crate) subtree: String,
    pub(crate) mountpoint: PathBuf,
    pub(crate) fs_name: String,
    pub(crate) allow_other: bool,
    pub(crate) read_only: bool,
    pub(crate) fuse_threads: usize,
    pub(crate) sink: constellation_frontend_fuse::FuseNotifySink,
    pub(crate) caps: constellation_vfs::FrontendCaps,
    pub(crate) qos: constellation_engine::ViewQos,
    pub(crate) confine_links: bool,
}

impl std::fmt::Debug for MountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_store_s3::{ChunkStore, FsMeta};

    fn start_node(
        rt: &tokio::runtime::Handle,
        backend: &str,
        state_dir: PathBuf,
    ) -> Arc<NodeRuntime> {
        let mut engine = EngineConfig::new(backend);
        engine.state_dir = Some(state_dir.clone());
        engine.cache_size = 16 * 1024 * 1024;
        NodeRuntime::start(
            NodeConfig {
                fs_id: FsId::new(state_dir.display().to_string()),
                engine,
                web_ui: 0,
                log_buffer: log_buffer::LogBuffer::default(),
                resumed: None,
                fuse_transport: Default::default(),
                control_socket: None,
                persistent: false,
                signals: None,
            },
            rt.clone(),
        )
        .expect("NodeRuntime::start")
    }

    /// Plan 38 Z2c: a `view.mount{PreopenedFd}` session (plan 37's CSI
    /// pods) is pinned to `/dev/fuse` whatever the node's knob says; a
    /// mount this daemon makes takes the knob, the ladder by default; the
    /// mobile profile's default is `/dev/fuse`.
    #[test]
    fn a_preopened_view_mount_is_pinned_and_a_plain_one_takes_the_knob() {
        use constellation_frontend_fuse::{KernelTuning, TransportConfig, TransportPolicy};
        let policy = TransportPolicy::Auto;
        let cfg = TransportConfig {
            policy,
            uring_queue_depth: None,
            ..Default::default()
        };
        let csi = mount_options_for("csi".into(), 2, KernelTuning::for_workers(2), cfg, true);
        assert_eq!(csi.transport(), TransportPolicy::DevFuse, "{policy}");
        assert!(csi.is_handover_capable());
        let plain = mount_options_for("plain".into(), 2, KernelTuning::for_workers(2), cfg, false);
        assert_eq!(plain.transport(), policy);
        assert!(!plain.is_handover_capable());
        let shipped = mount_options_for(
            "default".into(),
            2,
            KernelTuning::for_workers(2),
            TransportConfig::default(),
            false,
        );
        assert_eq!(shipped.transport(), TransportPolicy::Auto);
        assert_eq!(
            transport_of(EngineProfile::desktop().fuse_transport),
            TransportPolicy::Auto
        );
        assert_eq!(
            transport_of(EngineProfile::mobile().fuse_transport),
            TransportPolicy::DevFuse
        );
    }

    fn view(inner_path: &str, mountpoint: PathBuf) -> ViewConfig {
        ViewConfig {
            inner_path: inner_path.to_string(),
            mountpoint,
            allow_other: false,
            fs_name: "constellation-test".to_string(),
            fuse_threads: 1,
            rw_snapshot: false,
            clone_name: None,
            ephemeral: false,
            confine_links: false,
            labels: Default::default(),
            qos: Default::default(),
        }
    }

    /// Polls `f` until it reports true or `deadline` elapses. FUSE attach
    /// and cross-node sync (the periodic sync task, ~500ms interval) are
    /// asynchronous; a fixed sleep would be either flaky (too short) or
    /// needlessly slow (too long) depending on host load.
    fn eventually(deadline: std::time::Duration, mut f: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        loop {
            if f() {
                return true;
            }
            if start.elapsed() > deadline {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    /// The storm-hang follow-up: a final flush that cannot ship what the
    /// node holds must neither wedge nor pass for a clean exit. Here a
    /// pending upload whose chunk is not in the cache (as with a lost
    /// chunk) fails the drain: `shutdown` returns promptly, says what was
    /// left behind, records it for the process's exit status, and leaves
    /// the row for the next mount. A node with nothing to ship shuts down
    /// cleanly and records nothing.
    #[test]
    fn a_failed_final_flush_is_reported_and_left_for_the_next_mount() {
        unsafe {
            std::env::set_var("CONSTELLATION_P2P", "off");
        }
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        {
            let store = ChunkStore::new(
                rt.block_on(crate::backend::open_backend(&backend))
                    .expect("open backend"),
            );
            let meta = FsMeta::new(1024 * 1024, "raw");
            rt.block_on(store.create_fs(&meta)).expect("create_fs");
        }

        let clean = start_node(rt.handle(), &backend, root.path().join("state-clean"));
        clean.shutdown().expect("nothing to ship: a clean shutdown");
        assert_eq!(clean.shutdown_error(), None);

        let node = start_node(rt.handle(), &backend, root.path().join("state-lost"));
        let lost = constellation_fs_core::ChunkHash::of(b"never reached the cache");
        node.engine().meta().add_pending_upload(&lost, 42).unwrap();
        let started = Instant::now();
        let error = node
            .shutdown()
            .expect_err("the drain cannot upload the lost chunk");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(30),
            "a failed drain must not wedge the shutdown"
        );
        let message = error.to_string();
        assert!(
            message.contains("final flush failed") && message.contains("1 pending chunk upload"),
            "{message}"
        );
        assert_eq!(node.shutdown_error(), Some(message));
        assert_eq!(
            node.engine().meta().pending_upload_count().unwrap(),
            1,
            "the pending row stays for the next mount"
        );
        // Idempotent: a second call neither drains again nor clears it.
        node.shutdown().unwrap();
        assert!(node.shutdown_error().is_some());
    }

    /// Regression for the daemon-sharing refactor (plan 21): two views of
    /// ONE `NodeRuntime` (root + a subtree) must both see the same live
    /// replica, and a peer node must converge with writes made through
    /// either view exactly as it would have with two independent
    /// single-view processes before this plan — the refactor changed how
    /// many *processes* serve a filesystem, not what gets replicated.
    /// This also covers the plan's view-agnostic-control-op case: pinning
    /// a path does not depend on which, or how many, views expose it.
    #[test]
    fn two_views_of_one_node_converge_with_a_peer_and_pin_is_view_agnostic() {
        // The gossip/bootstrap poll (up to ~10s) is pure overhead for an
        // in-process test with no real peer discovery to do.
        unsafe {
            std::env::set_var("CONSTELLATION_P2P", "off");
        }
        let root = tempfile::tempdir().unwrap();
        let backend = format!("file://{}/backend", root.path().display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        // `fs create`, once, shared by both nodes (same backend prefix) —
        // mirrors `constellation fs create` before any `mount`.
        {
            let store = ChunkStore::new(
                rt.block_on(crate::backend::open_backend(&backend))
                    .expect("open backend"),
            );
            let meta = FsMeta::new(1024 * 1024, "raw");
            rt.block_on(store.create_fs(&meta)).expect("create_fs");
        }

        let a_root_mnt = root.path().join("a-root");
        let a_sub_mnt = root.path().join("a-sub");
        let b_mnt = root.path().join("b-root");
        std::fs::create_dir_all(&a_root_mnt).unwrap();
        std::fs::create_dir_all(&a_sub_mnt).unwrap();
        std::fs::create_dir_all(&b_mnt).unwrap();

        let node_a = start_node(rt.handle(), &backend, root.path().join("state-a"));
        let a_root_id = node_a
            .add_mount(view("/", a_root_mnt.clone()))
            .expect("mount a root");

        std::fs::create_dir(a_root_mnt.join("sub")).expect("mkdir sub via root view");
        let a_sub_id = node_a
            .add_mount(view("/sub", a_sub_mnt.clone()))
            .expect("mount a subtree");

        let node_b = start_node(rt.handle(), &backend, root.path().join("state-b"));
        let b_id = node_b
            .add_mount(view("/", b_mnt.clone()))
            .expect("mount b root");

        // Write through the root view, read back through the subtree view
        // of the SAME node: both must see one shared replica, not two.
        std::fs::write(a_root_mnt.join("sub/from-root.txt"), b"via-root").unwrap();
        assert!(
            eventually(std::time::Duration::from_secs(5), || {
                std::fs::read(a_sub_mnt.join("from-root.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-root".as_slice())
            }),
            "subtree view did not see a write made through the root view of the same node"
        );

        // Write through the subtree view, read back through the root view.
        std::fs::write(a_sub_mnt.join("from-sub.txt"), b"via-sub").unwrap();
        assert!(
            eventually(std::time::Duration::from_secs(5), || {
                std::fs::read(a_root_mnt.join("sub/from-sub.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-sub".as_slice())
            }),
            "root view did not see a write made through the subtree view of the same node"
        );

        // Both writes converge on the independent peer node — the same
        // cross-node correctness two single-view processes had before
        // this plan, now proven against a node hosting two views at once.
        assert!(
            eventually(std::time::Duration::from_secs(15), || {
                std::fs::read(b_mnt.join("sub/from-root.txt"))
                    .ok()
                    .as_deref()
                    == Some(b"via-root".as_slice())
                    && std::fs::read(b_mnt.join("sub/from-sub.txt"))
                        .ok()
                        .as_deref()
                        == Some(b"via-sub".as_slice())
            }),
            "peer node did not converge on writes made through either view of the multi-view node"
        );

        // View-agnostic control op: pinning "/sub" must not depend on
        // which — or how many — views currently expose it.
        let state_a = root.path().join("state-a");
        let pin = rt.block_on(
            crate::control::call::<constellation_control::methods::PinAdd>(
                &state_a,
                constellation_control::proto::types::PathParams {
                    path: "/sub".into(),
                },
            ),
        );
        assert!(pin.is_ok(), "pin failed: {pin:?}");
        let list_pins = |rt: &tokio::runtime::Runtime| {
            rt.block_on(crate::control::call::<
                constellation_control::methods::PinList,
            >(&state_a, Default::default()))
                .unwrap()
                .pins
        };
        let pins_before = list_pins(&rt);
        assert!(
            pins_before.iter().any(|p| p.path == "/sub"),
            "pin not listed: {pins_before:?}"
        );

        // Detach the subtree view; the pin (node-level, tracked against
        // the metadata replica, not against any one FUSE session) must
        // survive — proving it never depended on that view being mounted.
        node_a.remove_mount(a_sub_id).expect("unmount a subtree");
        let pins_after = list_pins(&rt);
        assert_eq!(
            pins_before.len(),
            pins_after.len(),
            "pin set changed after unmounting a view that never held any pins"
        );
        assert!(pins_after.iter().any(|p| p.path == "/sub"));

        node_a.remove_mount(a_root_id).expect("unmount a root");
        node_b.remove_mount(b_id).expect("unmount b root");
    }
}
