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
//! lazily, the first time a view is added, since `DaemonStatus` reports
//! mountpoints), the FUSE sessions and their threads, the mount records
//! a takeover reads (`daemon_lock`), `control.sock`/`daemon.pid`, and
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

use anyhow::{bail, Context, Result};
use constellation_engine::{
    DeferredEvents, Engine, EngineConfig, EngineHost, EngineProfile, FsId, ResourceBudget, View,
    ViewSpec,
};
use constellation_platform::HostServices;
use constellation_types::Code;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::log_buffer;
use constellation_vfs::OpWatch;

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

/// Everything needed to start the daemon's node, independent of any
/// particular mounted view.
pub struct NodeConfig {
    /// Which filesystem this is, on the host (registry name or state dir).
    pub fs_id: FsId,
    pub engine: EngineConfig,
    pub web_ui: u16,
    pub log_buffer: log_buffer::LogBuffer,
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
}

struct MountHandle {
    subtree: String,
    mountpoint: PathBuf,
    since: Instant,
    unmounter: Mutex<constellation_frontend_fuse::FuseUnmounter>,
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
    status: Mutex<Option<Arc<crate::DaemonStatus>>>,
    mounts: Mutex<HashMap<MountId, MountHandle>>,
    /// Session-thread handles, kept separate from `mounts` so a thread's
    /// own teardown (which removes its `mounts` entry) never races
    /// `remove_mount`'s attempt to join it.
    threads: Mutex<HashMap<MountId, std::thread::JoinHandle<()>>>,
    shutdown_started: AtomicBool,
}

impl NodeRuntime {
    /// This daemon's node.
    #[cfg(test)]
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Plan 30 §M8: this node's mounts are `--cto strict`.
    pub fn cto_strict(&self) -> bool {
        self.engine.cto_strict()
    }

    /// The request watchdog every view's ops register with (`status`'s
    /// `fuse_requests`).
    pub fn op_watch(&self) -> &OpWatch {
        self.engine.op_watch()
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
        } = cfg;
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
        let engine = engines.add_engine(fs_id, engine, host.clone(), EngineProfile::desktop())?;
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
        });

        // Signals are node-level: unmount every currently-mounted view,
        // then run the one clean node shutdown. The actual unmount+join
        // work happens on a plain OS thread (not this async task) so a
        // slow drain never blocks a tokio worker; a second signal aborts
        // immediately regardless of how far that drain got.
        {
            let node = node.clone();
            rt.spawn(async move {
                #[cfg(unix)]
                {
                    use tokio::signal::unix::{signal, SignalKind};
                    let Ok(mut sigint) = signal(SignalKind::interrupt()) else {
                        tracing::warn!("failed to install SIGINT handler; Ctrl-C will not unmount");
                        return;
                    };
                    let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
                        tracing::warn!("failed to install SIGTERM handler");
                        return;
                    };
                    tokio::select! {
                        _ = sigint.recv() => tracing::info!("SIGINT received; unmounting FUSE"),
                        _ = sigterm.recv() => tracing::info!("SIGTERM received; unmounting FUSE"),
                    }
                    let drain = node.clone();
                    std::thread::spawn(move || {
                        for id in drain.mount_ids() {
                            if let Err(e) = drain.remove_mount(id) {
                                tracing::warn!(error = %e, "signal-triggered unmount failed");
                            }
                        }
                    });
                    // A second signal during the post-unmount drain aborts immediately
                    // so a hung ship/upload cannot trap the process forever.
                    tokio::select! {
                        _ = sigint.recv() => {}
                        _ = sigterm.recv() => {}
                    }
                    tracing::error!("second signal during shutdown; exiting immediately");
                    std::process::exit(130);
                }
                #[cfg(not(unix))]
                {
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
        } = view;
        let spec = ViewSpec {
            root: inner_path.clone(),
            rw_snapshot,
            clone_name,
            ephemeral,
            confine_links,
            ..ViewSpec::default()
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
        clear_stale_mount(&self.host, &mountpoint);
        let engine = &self.engine;
        tracing::info!(?mountpoint, state_dir = ?engine.state_dir(), fs = %engine.fsmeta().uuid, "mounting");
        let mount_options = constellation_frontend_fuse::MountOptions {
            fs_name,
            allow_other,
            read_only: frozen_view,
            n_threads: fuse_threads,
            // The kernel queue is sized for the host's worker count (the
            // node-wide thread plan), whatever this view's own count.
            tuning: constellation_frontend_fuse::KernelTuning::for_workers(
                crate::parallelism::thread_plan().fuse,
            ),
        };
        // An explicit session, so `remove_mount`/signals can unmount from
        // inside this process (its `FuseUnmounter`); without it, an
        // external kill leaves a dead mountpoint that needs `fusermount3
        // -u`.
        let mut session =
            match constellation_frontend_fuse::mount(fs.clone(), &mountpoint, &mount_options, caps)
            {
                Ok(session) => session,
                Err(error) => {
                    engine.close_view(&fs);
                    return Err(error).context("FUSE mount");
                }
            };
        let unmounter = session.unmounter();
        events.set(Arc::new(session.notifier()));

        let id = MountId(fs.id());
        // So that a takeover after a kill can abort this mount's
        // connection if it is left wedged (`daemon_lock::abort_stale_mounts`).
        if let Err(e) = crate::daemon_lock::record_mount(
            engine.state_dir(),
            id.0,
            &mountpoint,
            &mount_record_name,
        ) {
            tracing::warn!(error = %e, "recording the mount in the state dir failed");
        }
        self.mounts.lock().unwrap().insert(
            id,
            MountHandle {
                subtree: inner_path,
                mountpoint: mountpoint.clone(),
                since: Instant::now(),
                unmounter: Mutex::new(unmounter),
            },
        );

        let node = self.clone();
        let thread = std::thread::spawn(move || {
            if let Err(e) = session.run() {
                tracing::warn!(error = %e, "FUSE session ended with an error");
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
            if now_empty {
                // `shutdown` logs its own failure (and records it for the
                // process's exit status).
                let _ = node.shutdown();
            }
        });
        self.threads.lock().unwrap().insert(id, thread);

        Ok(id)
    }

    /// The status object and the control API/web UI, started the first
    /// time a view is added.
    fn ensure_status(self: &Arc<Self>, first: &Arc<View>) {
        let mut status_guard = self.status.lock().unwrap();
        if status_guard.is_some() {
            return;
        }
        let e = &self.engine;
        let status = Arc::new(crate::DaemonStatus {
            meta: e.meta().clone(),
            cache: e.cache().clone(),
            staging_budget: e.staging_budget().clone(),
            core: e.core_status().clone(),
            lease: e.lease().clone(),
            fs_uuid: e.fsmeta().uuid.to_string(),
            backend: e.backend_url().to_string(),
            node: self.clone(),
            node_id: e.node_id(),
            started: e.started(),
            peers: e.peers().clone(),
            pins: e.pins().clone(),
            designations: e.designations().clone(),
            epochs: e.epochs().clone(),
            reintegration: e.reintegration().clone(),
            sync_tx: e.sync_tx().clone(),
            store: e.store().inner().clone(),
            departed: e.departed().clone(),
            rt: e.runtime().clone(),
            coop: e.coop().clone(),
            prefetch_stats: first.prefetch_stats(),
            write_mode: e.write_mode().clone(),
            upload: e.upload().clone(),
            snapshots: e.snapshots().clone(),
            log_buffer: self.log_buffer.clone(),
            forward: e.forward().clone(),
            placement: e.placement().clone(),
            atime: e.atime().clone(),
            prune_stats: e.prune_stats().clone(),
            lease_mode: e.lease_mode(),
            read_only_member: e.read_only_member(),
            last_sync_ms: e.last_sync_ms().clone(),
            state_dir: e.state_dir().to_path_buf(),
            compression: e.compression(),
        });
        *status_guard = Some(status.clone());
        drop(status_guard);
        let _guard = e.runtime().enter();
        if let Err(err) = constellation_api::serve(e.state_dir(), status.clone()) {
            tracing::warn!(error = %err, "control API unavailable");
        }
        if self.web_ui != 0 {
            match e
                .runtime()
                .block_on(constellation_api::web::serve(self.web_ui, status))
            {
                Ok(address) => {
                    tracing::info!(%address, "web UI listening (localhost only)")
                }
                Err(error) => tracing::warn!(%error, "web UI unavailable"),
            }
        }
    }

    /// Unmount exactly this view (via its `FuseUnmounter`) and join its
    /// session thread. Siblings are untouched. If this was the last view,
    /// the thread itself runs `shutdown()` before this call returns.
    pub fn remove_mount(&self, id: MountId) -> Result<()> {
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

    /// Block the calling thread until the given view's session ends,
    /// however it ends (an explicit `remove_mount`, an external
    /// `fusermount -u`, or the kernel force-unmounting it). Used by the
    /// CLI's `mount` command to preserve "mount blocks until unmounted".
    pub fn join_mount(&self, id: MountId) -> Result<()> {
        let thread = self.threads.lock().unwrap().remove(&id);
        if let Some(thread) = thread {
            if thread.join().is_err() {
                tracing::warn!("FUSE session thread panicked");
            }
        }
        Ok(())
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
            })
            .collect()
    }

    fn mount_ids(&self) -> Vec<MountId> {
        self.mounts.lock().unwrap().keys().copied().collect()
    }

    /// Invalidate every mounted view's cached quota cap after a live
    /// `SetQuota` (`Engine::invalidate_quota_caches`).
    pub fn invalidate_quota_caches(&self) {
        self.engine.invalidate_quota_caches();
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
        let result = self.engine.shutdown();
        // Best-effort, and unconditional even if the drain above failed:
        // a process that is exiting either way must not leave files
        // behind that make it look like a live daemon is still here.
        let state_dir = self.engine.state_dir();
        let _ = std::fs::remove_file(state_dir.join(constellation_api::SOCKET_NAME));
        let _ = std::fs::remove_file(state_dir.join("daemon.pid"));
        result
    }

    /// Why the node-wide shutdown left something unshipped, if it did.
    pub fn shutdown_error(&self) -> Option<String> {
        self.engine.shutdown_error()
    }
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
            },
            rt.clone(),
        )
        .expect("NodeRuntime::start")
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
        let sock_a = root
            .path()
            .join("state-a")
            .join(constellation_api::SOCKET_NAME);
        let pin = rt
            .block_on(constellation_api::call(
                &sock_a,
                &constellation_api::Request::Pin {
                    path: "/sub".into(),
                },
            ))
            .unwrap();
        assert!(
            matches!(pin, constellation_api::Response::Ok { .. }),
            "pin failed: {pin:?}"
        );
        let list_pins = |rt: &tokio::runtime::Runtime| -> Vec<constellation_api::PinStatus> {
            match rt
                .block_on(constellation_api::call(
                    &sock_a,
                    &constellation_api::Request::ListPins,
                ))
                .unwrap()
            {
                constellation_api::Response::Pins { pins } => pins,
                other => panic!("unexpected response {other:?}"),
            }
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
