//! Plan 40 R3a, at the engine's door: what makes a directory cookie the same
//! on every node of a filesystem and different between filesystems, and
//! that a filesystem with nothing to key it by is not served.
//!
//! One engine per node on a local file backend (as `conformance.rs`), views
//! through `Engine::open_view`; names are listed through `Vfs::readdir`
//! and their cookies compared.

use constellation_engine::{
    DeferredEvents, Engine, EngineConfig, EngineProfile, P2pMode, View, ViewSpec,
};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_vfs::{Blocking, Caller, CollectDir, Fh, FrontendCaps, Name, OpCtx, OpKind, Vfs};
use std::collections::BTreeMap;
use std::sync::Arc;

struct Node {
    engine: Option<Engine>,
    rt: Option<tokio::runtime::Runtime>,
    _dir: Option<tempfile::TempDir>,
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn config(backend: &str, state: std::path::PathBuf, rt: &tokio::runtime::Runtime) -> EngineConfig {
    EngineConfig {
        state_dir: Some(state),
        cache_size: 64 * 1024 * 1024,
        runtime: Some(rt.handle().clone()),
        ..EngineConfig::new(backend)
    }
}

fn profile() -> EngineProfile {
    EngineProfile {
        p2p: P2pMode::Off,
        ..EngineProfile::desktop()
    }
}

/// A filesystem on a fresh local backend, described by `meta`; its first
/// node, and the backend's URL.
fn first_node(meta: &FsMeta) -> (Node, String, std::path::PathBuf) {
    let dir = tempfile::TempDir::new().unwrap();
    let backend = format!("file://{}/backend", dir.path().display());
    let rt = runtime();
    let store = ChunkStore::new(
        rt.block_on(constellation_engine::backend::open_backend(&backend))
            .expect("open the backend"),
    );
    rt.block_on(store.create_fs(meta))
        .expect("create the filesystem");
    let state = dir.path().join("state");
    let engine = Engine::start(
        config(&backend, state.clone(), &rt),
        HostServices::native(),
        profile(),
    )
    .expect("Engine::start");
    let root = dir.path().to_path_buf();
    (
        Node {
            engine: Some(engine),
            rt: Some(rt),
            _dir: Some(dir),
        },
        backend,
        root,
    )
}

fn view_of(node: &Node) -> Arc<View> {
    node.engine
        .as_ref()
        .unwrap()
        .open_view(
            ViewSpec::new("/"),
            FrontendCaps::linux_fuse(false),
            DeferredEvents::new(),
        )
        .expect("a view")
}

const NAMES: usize = 40;

/// Directories `n0..` under the root, then shipped to the backend.
fn populate(view: &Arc<View>) {
    let caller = Caller::root();
    for i in 0..NAMES {
        Blocking::run(|r| {
            view.mkdir(
                &OpCtx::new(OpKind::Mkdir, &caller),
                1,
                Name::new(&format!("name-{i}")),
                0o755,
                r,
            )
        })
        .unwrap();
    }
    Blocking::run(|r| view.sync_view(&OpCtx::new(OpKind::SyncView, &caller), r))
        .expect("the view ships what it did");
}

/// name -> cookie, of the root, in pages of 7.
fn cookies(view: &View) -> BTreeMap<String, u64> {
    let caller = Caller::root();
    let mut out = BTreeMap::new();
    let mut cookie = 0;
    loop {
        let (sink, wait) = CollectDir::pair(7);
        view.readdir(
            &OpCtx::new(OpKind::Readdir, &caller),
            1,
            Fh(0),
            cookie,
            false,
            sink,
        );
        let page = wait.wait().unwrap();
        let Some(last) = page.last() else {
            return out;
        };
        cookie = last.next;
        for e in page {
            let name = String::from_utf8(e.name.into_bytes()).unwrap();
            if name.starts_with("name-") {
                assert!(out.insert(name, e.next).is_none(), "listed twice");
            }
        }
    }
}

/// The cookie of a name is the filesystem's: the same on every node that
/// serves it (a second engine, its own replica and state), and not the
/// same on another filesystem.
#[test]
fn the_cookie_is_the_same_on_two_nodes_and_differs_between_filesystems() {
    let (a, backend, root) = first_node(&FsMeta::new(1024 * 1024, "raw"));
    let view_a = view_of(&a);
    populate(&view_a);
    let on_a = cookies(&view_a);
    assert_eq!(on_a.len(), NAMES);

    // The second node of the same filesystem: its own runtime, state dir
    // and replica, bootstrapped from the log.
    let rt = runtime();
    let engine = Engine::start(
        config(&backend, root.join("state-b"), &rt),
        HostServices::native(),
        profile(),
    )
    .expect("Engine::start (the second node)");
    let b = Node {
        engine: Some(engine),
        rt: Some(rt),
        _dir: None,
    };
    let view_b = view_of(&b);
    let on_b = cookies(&view_b);
    assert_eq!(on_b, on_a, "one filesystem, one cookie per name");

    // Another filesystem, the same names.
    let (c, _, _) = first_node(&FsMeta::new(1024 * 1024, "raw"));
    let view_c = view_of(&c);
    populate(&view_c);
    let on_c = cookies(&view_c);
    assert_eq!(on_c.len(), NAMES);
    let same = on_a.iter().filter(|(n, c)| on_c[*n] == **c).count();
    assert_eq!(same, 0, "the key is the filesystem's");
}

/// Q5: a filesystem that is not E2E and has no `gossip_secret` (created
/// before the field existed) has no key to derive the cookies from: the
/// engine does not start on it.
#[test]
fn a_non_e2e_filesystem_without_a_gossip_secret_refuses_to_start() {
    let mut meta = FsMeta::new(1024 * 1024, "raw");
    assert!(!meta.e2e);
    meta.gossip_secret = None;
    let dir = tempfile::TempDir::new().unwrap();
    let backend = format!("file://{}/backend", dir.path().display());
    let rt = runtime();
    let store = ChunkStore::new(
        rt.block_on(constellation_engine::backend::open_backend(&backend))
            .unwrap(),
    );
    rt.block_on(store.create_fs(&meta)).unwrap();
    match Engine::start(
        config(&backend, dir.path().join("state"), &rt),
        HostServices::native(),
        profile(),
    ) {
        Ok(engine) => {
            let _ = engine.shutdown();
            panic!("the engine started on a filesystem with no gossip_secret");
        }
        Err(e) => {
            let message = format!("{e:#}");
            assert!(message.contains("gossip_secret"), "{message}");
            assert!(message.contains("recreate"), "{message}");
        }
    }
    rt.shutdown_background();
}
