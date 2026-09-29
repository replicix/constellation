# The engine as a conformance target (ready to drop in)

Plan 31 C6 asks for the conformance kit to run against the real engine as
well as the reference filesystem. `constellation-engine` was being
restructured while this was written (the `Engine`/`EngineHost`/`ViewSpec`
API), so the instance below could not be compiled here: it is written
against the public items `crates/engine/src/view/quota_tests.rs::test_fs`
and `vfs_tests.rs` use today, and needs at most small fix-ups (a
`FsDependencies` field added since, a renamed constructor). It needs **no
new engine code**: every item it touches is `pub` (checked against the
tree at the time of writing: `View::new`, `View::apply_spec`,
`View::set_subtree_root`, `FsDependencies`, `SnapshotManager::create`,
`InFlight::disabled`, `AtimeAccumulator`, `StagingBudget`, `PruneStats`).

## Where it goes

`crates/engine/tests/conformance.rs`, with these in
`crates/engine/Cargo.toml`:

```toml
[dev-dependencies]
constellation-vfs = { workspace = true, features = ["conformance"] }
# already present: tempfile.workspace = true
```

and the `conformance` CI job (plan 31 §12) gains one line next to the
kit's own run:

```yaml
      - run: cargo test -p constellation-engine --test conformance
```

## What it declares, and why

- `Declared { rename_flags: false, .. }`: `View::rename` accepts and
  ignores `RENAME_NOREPLACE`/`RENAME_EXCHANGE` (its `_flags` parameter, and
  the comment above it: "as they never were"). `namespace::rename_noreplace`
  and `namespace::rename_exchange` therefore **skip** naming the gap. This is
  a finding of the kit, not a decision: plan 31 §6.2 puts the flags on the
  trait, the kit asserts the contract, and the engine does not honour it yet.
  Implementing them in `View::rename` and flipping this flag to `true` is the
  fix.
- `Declared { cancellable_waits: false, .. }`: `ClusterLocks::lock` waits on
  its own thread and does not consult `OpCtx::cancel` (and a Linux FUSE mount
  never has a token set anyway: plan 31 §6.3). The `cancellation` group
  skips, naming the gap.
- No `events`/`second_view` hooks: an in-process second `View` over the same
  `Meta` does not push invalidations the way another *node's* log replay
  does, so the `invalidation` group skips. It needs a two-`Engine` fixture
  over one `InMemory` object store (the pattern at the bottom of
  `engine/src/shipper.rs`) whose first engine's `open_view(spec, caps,
  events)` is given the `RecordingEvents`; add it as a second fixture
  constructor when the `Engine` API settles.
- Hooks it does offer: `subtree_view` (a second `View` over the same
  replica, `set_subtree_root` + `apply_spec` with `confine_links`) and
  `snapshot` (`SnapshotManager::create`, driven on the fixture's runtime).

## Expected first-run findings

Places the kit asserts the contract and the engine may not meet it (each
was chosen because plan 31 or the engine's own docs state the rule, but none
could be run against the engine here): `hard_link_refusals` accepts
`EPERM` *or* `EISDIR` for a directory source (the replica says `EISDIR`);
`readdir::removal_between_pages_never_repeats_or_loses` needs cookies that
survive an unlink of an already-listed entry; `confinement::*` needs the
`Stale` answers `confine.rs` documents, including for `statfs`, `readdir`
and the xattr ops on an outside inode; `io::fallocate_modes` needs
`PUNCH_HOLE|KEEP_SIZE` to be implemented. Triage a failure by reading the
message (every assertion names the step), re-running with
`CONFORMANCE_FILTER=<group::name>`, and comparing against the same test on
the reference target (`cargo test -p constellation-vfs --features
conformance`).

## The file

```rust
//! The engine's `View`, driven through `Vfs` by the conformance kit
//! (plan 31 C6): kernel-free, in-process, one fresh in-memory replica per
//! test.

use constellation_engine::atime::{AtimeAccumulator, AtimeMode, AtimeStats};
use constellation_engine::kernel_inval::InFlight;
use constellation_engine::prune::PruneStats;
use constellation_engine::snapshot::SnapshotManager;
use constellation_engine::staging::StagingBudget;
use constellation_engine::view::{FsDependencies, View};
use constellation_engine::ViewSpec;
use constellation_fs_core::cache::DiskCache;
use constellation_fs_core::DEFAULT_CHUNK_SIZE;
use constellation_meta::Meta;
use constellation_store_s3::{ChunkStore, CompressionSetting};
use constellation_vfs::conformance::{
    run, ConformanceTarget, Declared, Fixture, Hooks, RunOptions, TargetOpts,
};
use constellation_vfs::{FrontendCaps, OpWatch};
use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;

/// One "node": the replica, the chunk store and cache, a runtime, and a
/// snapshot manager, shared by every view of a fixture.
struct Node {
    meta: Arc<Meta>,
    store: Arc<ChunkStore>,
    cache: Arc<DiskCache>,
    snapshots: Arc<SnapshotManager>,
    /// A real multi-threaded runtime: the concurrency tests call the view
    /// from many threads, each of which may `block_on` a fetch.
    rt: tokio::runtime::Runtime,
    dir: tempfile::TempDir,
}

impl Node {
    fn new() -> Arc<Node> {
        let dir = tempfile::TempDir::new().unwrap();
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let cache = Arc::new(DiskCache::open(dir.path().join("cache"), 1 << 30).unwrap());
        let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
        let snapshots = Arc::new(SnapshotManager::new(
            meta.clone(),
            store.clone(),
            DEFAULT_CHUNK_SIZE,
            1,
        ));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        Arc::new(Node { meta, store, cache, snapshots, rt, dir })
    }

    /// A view of `root` (`"/"` for the whole tree) for a frontend with
    /// `caps`. Every view of a node shares its replica, so their trees are
    /// one tree.
    fn view(&self, caps: &FrontendCaps, root: &str, confine_links: bool) -> Result<View, String> {
        let _enter = self.rt.handle().enter();
        let deps = FsDependencies {
            meta: self.meta.clone(),
            store: self.store.clone(),
            cache: self.cache.clone(),
            rt: self.rt.handle().clone(),
            sync: None,
            coop: None,
            staging_dir: self.dir.path().join("staging"),
            staging_budget: StagingBudget::new(1 << 30),
            snapshots: self.snapshots.clone(),
            atime: Arc::new(AtimeAccumulator::new(AtimeMode::Off, AtimeStats::new())),
            prune_stats: PruneStats::new(),
            inflight: InFlight::disabled(),
            holds: None,
            watch: OpWatch::manual("conformance", Duration::from_secs(30)),
            caps: caps.clone(),
            host: constellation_platform::HostServices::native(),
        };
        let mut view = View::new(deps, DEFAULT_CHUNK_SIZE, CompressionSetting::RAW);
        let spec = ViewSpec { root: root.into(), confine_links, ..ViewSpec::default() };
        view.apply_spec(&spec);
        if root != "/" {
            view.set_subtree_root(root).map_err(|e| format!("{e:#}"))?;
        }
        Ok(view)
    }
}

struct EngineTarget;

impl ConformanceTarget for EngineTarget {
    type V = View;

    fn fresh(&self, caps: &FrontendCaps, _opts: TargetOpts) -> Fixture<View> {
        let node = Node::new();
        let view = node.view(caps, "/", false).expect("the root view");
        let mut fx = Fixture::new(Arc::new(view), caps.clone());
        // What the engine does not do yet (see ENGINE_TARGET.md).
        fx.declared = Declared { rename_flags: false, cancellable_waits: false };
        let (n1, n2, c2) = (node.clone(), node.clone(), caps.clone());
        fx.hooks = Hooks {
            snapshot: Some(Arc::new(move |path: &str, name: &str| {
                // `path` is relative to the view's root, which is `/`.
                let abs = format!("/{}", path.trim_start_matches('/'));
                n1.rt
                    .block_on(n1.snapshots.create(&abs, name))
                    .map(|_| ())
                    .map_err(|e| format!("{e:#}"))
            })),
            subtree_view: Some(Arc::new(move |path: &str, confine: bool| {
                let abs = format!("/{}", path.trim_start_matches('/'));
                let view = n2.view(&c2, &abs, confine)?;
                let mut sub = Fixture::new(Arc::new(view), c2.clone());
                sub.declared = Declared { rename_flags: false, cancellable_waits: false };
                sub.keepalive.push(Box::new(n2.clone()));
                Ok(sub)
            })),
            ..Hooks::default()
        };
        // The node (runtime, temp dir, replica) lives as long as the fixture.
        fx.keepalive.push(Box::new(node));
        fx
    }

    fn name(&self) -> String {
        "engine (View over an in-memory replica)".into()
    }
}

#[test]
fn the_engine_view_conforms() {
    let report = run(
        &EngineTarget,
        &RunOptions {
            filter: std::env::var("CONFORMANCE_FILTER").ok(),
            // cluster_locks false: the view is built without a `SyncHandle`,
            // so the lock ops are `NotImplemented`, and the tests that need
            // ClusterLocks skip naming it.
            caps: FrontendCaps::linux_fuse(false),
            ..RunOptions::default()
        },
    );
    println!("{}", report.render());
    report.assert_ok();
}
```

## Notes for whoever wires it

- `Node` is `Send + Sync` for the fixture's `keepalive` only if the
  runtime and `TempDir` are (they are); drop order does not matter.
- The lock and cancellation groups need a `SyncHandle` with `locks:
  Some(ClusterLocks)`, i.e. the `SyncRequest` receiver of a real `Engine`;
  they become runnable when the target builds its views with
  `Engine::open_view(spec, FrontendCaps::linux_fuse(true), events)`
  instead of `View::new`. Then flip `RunOptions::caps` to
  `FrontendCaps::linux_fuse(true)` and remove `cancellable_waits: false` only
  once `ClusterLocks::lock` honours the token.
- Run it as every frontend's `FrontendCaps` and `PolicyStack` (plan 31 §8):
  loop `run(&EngineTarget, &RunOptions { caps, .. })` over the caps of each
  frontend crate that exists, exactly as the reference target's tests do.
