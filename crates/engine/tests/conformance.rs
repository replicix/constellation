//! The engine's `View`, driven through `Vfs` by the conformance kit
//! (plan 31 C6, `crates/vfs/src/conformance/ENGINE_TARGET.md`): kernel-free
//! and in-process, one fresh engine per test — `Engine::start` on a local
//! file backend in a temp dir, views through `Engine::open_view`,
//! exactly as the daemon opens them for a FUSE mount (a `SyncHandle`, so
//! the lease, the write gate and `ClusterLocks` are the real ones).
//!
//! The `invalidation` group needs a second view whose mutations reach the
//! first one's events the way another node's do ([`Node::peer`]): a
//! second engine, with a state dir and a replica of its own, started on
//! the same backend. The fixture's `settle` ships what the peer did, has
//! the first engine tail the log to its head (it applies the peer's
//! segments as foreign records, which queues the kernel invalidations) and
//! waits until they reached the recorded events.
//!
//! `CONFORMANCE_FILTER=<group::name>` narrows the run.

use constellation_engine::sync::SyncRequest;
use constellation_engine::{
    DeferredEvents, Engine, EngineConfig, EngineProfile, P2pMode, View, ViewSpec,
};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_vfs::conformance::{
    run, ConformanceTarget, Declared, Fixture, Hooks, RecordingEvents, RunOptions, TargetOpts,
};
use constellation_vfs::{Blocking, Caller, FrontendCaps, FrontendEvents, OpCtx, OpKind, Vfs};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// One node: its runtime, its engine, and the temp dir holding the
/// backend and the state dir, shared by every view of a fixture. Dropping
/// the last reference shuts the engine down before the runtime goes.
struct Node {
    engine: Option<Engine>,
    rt: Option<tokio::runtime::Runtime>,
    /// The backend every node of a fixture shares (`file://…`).
    backend: String,
    /// The temp dir of the first node of a fixture; a peer's is its origin's.
    dir: Option<tempfile::TempDir>,
    origin: Option<Arc<Node>>,
    /// The views of the peers this node's `second_view`s started, for
    /// `settle` to ship what they did.
    peer_views: Mutex<Vec<Weak<View>>>,
    peers_started: AtomicUsize,
}

impl Node {
    fn start() -> Arc<Node> {
        let dir = tempfile::TempDir::new().unwrap();
        let backend = format!("file://{}/backend", dir.path().display());
        // A real multi-threaded runtime: the concurrency tests call the
        // view from many threads, each of which may wait on the engine.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let store = ChunkStore::new(
            rt.block_on(constellation_engine::backend::open_backend(&backend))
                .expect("open the backend"),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .expect("create the filesystem");
        let engine = Engine::start(
            EngineConfig {
                state_dir: Some(dir.path().join("state")),
                cache_size: 256 * 1024 * 1024,
                // `--locks cluster` (it needs P2P: the profile listens on
                // this node's own endpoint, with no peer to find): the
                // views' `ClusterLocks` are live, so the lock and deferral
                // tests run.
                locks: Some(true),
                runtime: Some(rt.handle().clone()),
                ..EngineConfig::new(&backend)
            },
            HostServices::native(),
            EngineProfile::desktop(),
        )
        .expect("Engine::start");
        Arc::new(Node {
            engine: Some(engine),
            rt: Some(rt),
            backend,
            dir: Some(dir),
            origin: None,
            peer_views: Mutex::new(Vec::new()),
            peers_started: AtomicUsize::new(0),
        })
    }

    /// Another node of this filesystem: a second engine (its own runtime,
    /// state dir and replica) on the same backend, so what it changes
    /// reaches this node the way a remote node's changes do — through the
    /// log. P2P is off: nothing tells this node, `settle` tails the log
    /// (and [`Self::learn_roster`] makes the origin read the peer's inbox).
    ///
    /// The local file backend has no `If-Match`, so the engine runs in its
    /// single-writer lease mode: a peer is safe as long as it is the only
    /// one that mutates, which is all the `invalidation` group asks of it
    /// (the recorded node only reads).
    fn peer(self: &Arc<Self>) -> Arc<Node> {
        let origin = self.origin.as_ref().unwrap_or(self);
        let n = self.peers_started.fetch_add(1, Ordering::SeqCst);
        let state = origin
            .dir
            .as_ref()
            .expect("the origin owns the temp dir")
            .path()
            .join(format!("state-peer-{n}"));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let engine = Engine::start(
            EngineConfig {
                state_dir: Some(state),
                cache_size: 64 * 1024 * 1024,
                runtime: Some(rt.handle().clone()),
                ..EngineConfig::new(&self.backend)
            },
            HostServices::native(),
            EngineProfile {
                p2p: P2pMode::Off,
                ..EngineProfile::desktop()
            },
        )
        .expect("Engine::start (a peer)");
        Arc::new(Node {
            engine: Some(engine),
            rt: Some(rt),
            backend: self.backend.clone(),
            dir: None,
            origin: Some(origin.clone()),
            peer_views: Mutex::new(Vec::new()),
            peers_started: AtomicUsize::new(0),
        })
    }

    /// Tell this node's engine about the nodes that joined since it
    /// started, as its registry poll does every 5 s (hard-wired in
    /// `Engine::start`). A holder reads the inbox of a requester only once
    /// it knows it; until then a peer's first mutation, which it submits
    /// to this node's inbox because this node holds the lease, waits for
    /// that poll (up to 5 s) and then for the inbox's own backed-off
    /// polling (`quick_inbox_polling`).
    fn learn_roster(&self) {
        let engine = self.engine();
        let roster = engine
            .runtime()
            .block_on(constellation_engine::epoch::refresh_roster(
                engine.epochs(),
                engine.store().inner().clone(),
            ))
            .expect("the registry's roster");
        engine
            .sync_tx()
            .send(SyncRequest::Roster(roster))
            .expect("the sync task runs");
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("running")
    }

    /// A view of `root` (`"/"` for the whole tree) for a frontend with
    /// `caps`, telling `events` what other nodes change. Every view of a
    /// node shares its replica.
    fn view(
        &self,
        caps: &FrontendCaps,
        root: &str,
        confine_links: bool,
        events: Arc<dyn FrontendEvents>,
    ) -> Result<Arc<View>, String> {
        let mut spec = ViewSpec::new(root);
        spec.confine_links = confine_links;
        self.engine()
            .open_view(spec, caps.clone(), events)
            .map_err(|e| format!("{e:#}"))
    }

    /// Everything the peers started from this node did is in this node's
    /// events: the peers' views shipped it (`sync_view`), this node's
    /// engine tailed the log to its head, and the invalidations that
    /// applying it queued were delivered.
    fn settle(&self) {
        let caller = Caller::root();
        let peers: Vec<Arc<View>> = self
            .peer_views
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for view in peers {
            Blocking::run(|r| view.sync_view(&OpCtx::new(OpKind::SyncView, &caller), r))
                .expect("a peer ships what it did");
        }
        let engine = self.engine();
        let (reply, tailed) = tokio::sync::oneshot::channel();
        engine
            .sync_tx()
            .send(SyncRequest::TailToHead { reply })
            .expect("the sync task runs");
        engine
            .runtime()
            .block_on(tailed)
            .expect("the sync task answers")
            .expect("tailing to the log head");
        assert!(
            engine.wait_invalidations_delivered(Duration::from_secs(30)),
            "the invalidations were not delivered"
        );
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.take() {
            let _ = engine.shutdown();
        }
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// What the engine declares of itself (see ENGINE_TARGET.md): it honours
/// `RENAME_NOREPLACE`/`RENAME_EXCHANGE`, and a blocked lock wait ends
/// `Intr` when the op's `CancelToken` is set and `TimedOut` when its
/// deadline passes (`ClusterLocks`, since the blocking locks answer
/// `EINTR`, commit 346374b; the deadline since the plan 31 closing of the
/// conformance gaps). That a Linux FUSE *mount* sets the token only on a
/// signal is the adapter's business, not this target's.
fn declared() -> Declared {
    Declared {
        rename_flags: true,
        cancellable_waits: true,
    }
}

/// `path` relative to the view's root, which is `/` for a fixture.
fn absolute(path: &str) -> String {
    format!("/{}", path.trim_start_matches('/'))
}

/// The fixture's view, for a hook (a weak reference: the fixture owns it).
fn view_of(fx: &Fixture<View>) -> WeakView {
    WeakView(Arc::downgrade(&fx.vfs))
}

struct WeakView(std::sync::Weak<View>);

impl WeakView {
    fn evict_cached(&self, ino: u64) -> Result<usize, String> {
        self.0
            .upgrade()
            .ok_or_else(|| "the view is gone".to_string())?
            .evict_cached(ino)
    }
}

struct EngineTarget;

impl ConformanceTarget for EngineTarget {
    type V = View;

    fn fresh(&self, caps: &FrontendCaps, _opts: TargetOpts) -> Fixture<View> {
        let node = Node::start();
        let events = RecordingEvents::new();
        let view = node
            .view(caps, "/", false, events.clone())
            .expect("the root view");
        let mut fx = Fixture::new(view, caps.clone());
        fx.declared = declared();
        let (n1, n2, c2) = (node.clone(), node.clone(), caps.clone());
        let (n3, c3, n4) = (node.clone(), caps.clone(), node.clone());
        let evicting = view_of(&fx);
        fx.hooks = Hooks {
            // Cold reads (the deferral group): the file's chunks leave
            // the local cache once they are up in the backend.
            evict: Some(Arc::new(move |ino| evicting.evict_cached(ino).map(|_| ()))),
            snapshot: Some(Arc::new(move |path: &str, name: &str| {
                let engine = n1.engine();
                engine
                    .runtime()
                    .block_on(engine.snapshots().create(&absolute(path), name))
                    .map(|_| ())
                    .map_err(|e| format!("{e:#}"))
            })),
            subtree_view: Some(Arc::new(move |path: &str, confine: bool| {
                let view = n2.view(&c2, &absolute(path), confine, DeferredEvents::new())?;
                let mut sub = Fixture::new(view, c2.clone());
                sub.declared = declared();
                sub.keepalive.push(Box::new(n2.clone()));
                Ok(sub)
            })),
            // The `invalidation` group: a view of a *second engine* on the
            // same backend (`Node::peer`), whose changes reach `events`
            // through the log, as another node's do. (A second view of the
            // one engine would not: the kernel that made a change keeps
            // its own caches right, so the engine tells it nothing.)
            second_view: Some(Arc::new(move || {
                let peer = n3.peer();
                n3.learn_roster();
                let view = peer.view(&c3, "/", false, DeferredEvents::new())?;
                n3.peer_views.lock().unwrap().push(Arc::downgrade(&view));
                let mut other = Fixture::new(view, c3.clone());
                other.declared = declared();
                other.keepalive.push(Box::new(peer));
                Ok(other)
            })),
            events: Some(events),
            settle: Some(Arc::new(move || n4.settle())),
        };
        // The node (engine, runtime, temp dir) lives as long as the fixture.
        fx.keepalive.push(Box::new(node));
        fx
    }

    fn name(&self) -> String {
        "engine (Engine::start on a local backend, View via open_view)".into()
    }
}

/// The holder's inbox polling, quickened for this test process
/// (`CONSTELLATION_INBOX_COLD_MAX_MS`, `CONSTELLATION_INBOX_IDLE_MAX_MS`,
/// read when an engine starts). The recorded node holds the lease from its
/// start, so the peer's mutations travel through its inbox in the object
/// store, and a holder polls a requester it has heard nothing from at a
/// backed-off interval of up to 10 s: the first mutation of a peer would
/// wait for that poll (measured: 10 s per `invalidation` test). Only the
/// cadence of that polling changes; no test of this binary has a second
/// writer, and the `invalidation` group asserts what the events tell, not
/// when the peer's op is picked up. Set once, before the first engine
/// starts.
fn quick_inbox_polling() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("CONSTELLATION_INBOX_COLD_MAX_MS", "100");
        std::env::set_var("CONSTELLATION_INBOX_IDLE_MAX_MS", "100");
    });
}

fn conforms_as(caps: FrontendCaps) {
    quick_inbox_polling();
    let report = run(
        &EngineTarget,
        &RunOptions {
            filter: std::env::var("CONFORMANCE_FILTER").ok(),
            caps,
            ..RunOptions::default()
        },
    );
    println!("{}", report.render());
    report.assert_ok();
}

#[test]
fn the_engine_view_conforms() {
    // Every view `Engine::open_view` builds has a `SyncHandle`, whose
    // `ClusterLocks` is live under `--locks cluster`.
    conforms_as(FrontendCaps::linux_fuse(true));
}

/// As the FUSE adapter declares itself (`constellation_frontend_fuse::caps`):
/// it reports the kernel's `FORGET`s, so the view keeps the files it
/// unlinks, data and all, for the kit's references until the kit's
/// `forget` (DESIGN.md §3, "Kernel references count as holds").
#[test]
fn the_engine_view_conforms_as_a_fuse_mount() {
    conforms_as(FrontendCaps {
        reports_forget: true,
        ..FrontendCaps::linux_fuse(true)
    });
}
