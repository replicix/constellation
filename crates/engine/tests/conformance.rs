//! The engine's `View`, driven through `Vfs` by the conformance kit
//! (plan 31 C6, `crates/vfs/src/conformance/ENGINE_TARGET.md`): kernel-free
//! and in-process, one fresh engine per test — `Engine::start` on a local
//! file backend in a temp dir, views through `Engine::open_view`,
//! exactly as the daemon opens them for a FUSE mount (a `SyncHandle`, so
//! the lease, the write gate and `ClusterLocks` are the real ones).
//!
//! `CONFORMANCE_FILTER=<group::name>` narrows the run.

use constellation_engine::{DeferredEvents, Engine, EngineConfig, EngineProfile, View, ViewSpec};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_vfs::conformance::{
    run, ConformanceTarget, Declared, Fixture, Hooks, RunOptions, TargetOpts,
};
use constellation_vfs::FrontendCaps;
use std::sync::Arc;

/// One node: its runtime, its engine, and the temp dir holding the
/// backend and the state dir, shared by every view of a fixture. Dropping
/// the last reference shuts the engine down before the runtime goes.
struct Node {
    engine: Option<Engine>,
    rt: Option<tokio::runtime::Runtime>,
    _dir: tempfile::TempDir,
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
            _dir: dir,
        })
    }

    fn engine(&self) -> &Engine {
        self.engine.as_ref().expect("running")
    }

    /// A view of `root` (`"/"` for the whole tree) for a frontend with
    /// `caps`. Every view of a node shares its replica.
    fn view(
        &self,
        caps: &FrontendCaps,
        root: &str,
        confine_links: bool,
    ) -> Result<Arc<View>, String> {
        let mut spec = ViewSpec::new(root);
        spec.confine_links = confine_links;
        self.engine()
            .open_view(spec, caps.clone(), DeferredEvents::new())
            .map_err(|e| format!("{e:#}"))
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

/// What the engine does not do (see ENGINE_TARGET.md): a
/// `ClusterLocks::lock` wait does not consult `OpCtx::cancel`.
fn declared() -> Declared {
    Declared {
        rename_flags: true,
        cancellable_waits: false,
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
        let view = node.view(caps, "/", false).expect("the root view");
        let mut fx = Fixture::new(view, caps.clone());
        fx.declared = declared();
        let (n1, n2, c2) = (node.clone(), node.clone(), caps.clone());
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
                let view = n2.view(&c2, &absolute(path), confine)?;
                let mut sub = Fixture::new(view, c2.clone());
                sub.declared = declared();
                sub.keepalive.push(Box::new(n2.clone()));
                Ok(sub)
            })),
            // No `second_view`/`events`: a second view of one engine gets
            // no invalidations for the first's mutations (the kernel that
            // made them keeps its own caches right); only a *remote*
            // node's replayed records produce them, and a second engine
            // on this file backend cannot be one (no `If-Match`: single
            // writer). The `invalidation` group skips, naming it.
            ..Hooks::default()
        };
        // The node (engine, runtime, temp dir) lives as long as the fixture.
        fx.keepalive.push(Box::new(node));
        fx
    }

    fn name(&self) -> String {
        "engine (Engine::start on a local backend, View via open_view)".into()
    }
}

#[test]
fn the_engine_view_conforms() {
    let report = run(
        &EngineTarget,
        &RunOptions {
            filter: std::env::var("CONFORMANCE_FILTER").ok(),
            // Every view `Engine::open_view` builds has a `SyncHandle`,
            // whose `ClusterLocks` is live under `--locks cluster`.
            caps: FrontendCaps::linux_fuse(true),
            ..RunOptions::default()
        },
    );
    println!("{}", report.render());
    report.assert_ok();
}
