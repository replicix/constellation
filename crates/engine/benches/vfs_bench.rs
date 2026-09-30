//! `vfs-bench` (plan 31 §6.9, C7): what the frontend-side path around a
//! `Vfs` op costs, in-process, no kernel — and whether it allocates.
//!
//! §6.9's targets: **VFS dispatch overhead ≤ 1 µs/op beyond the work
//! itself**, and **no per-op heap allocation for inline replies**.
//!
//! # What is measured
//!
//! For each backend and each inline op (`getattr`, `lookup` of an
//! existing name, `read` of a cached 4 KiB file) the op is driven two
//! ways, from the same thread, into the same `Vfs`:
//!
//! - **direct**: `OpCtx::new` + a bare responder — what a frontend needs at
//!   minimum, and what the FUSE adapter did before C7;
//! - **observed**: the whole frontend-side path a FUSE callback runs today
//!   (`constellation_vfs::Observer`): the op id, the `vfs.op` span
//!   (created and entered), the `OpCtx`, and the responder wrapped to
//!   record the op's outcome and latency in the metrics.
//!
//! The **dispatch overhead is observed − direct**: the work itself (the
//! backend) cancels; each scenario runs both ways in alternating rounds
//! and the overhead is the median of the per-round differences, so a
//! backend whose state drifts under load (the engine's read path) does not
//! read as dispatch cost. The backends are the `observer` alone (no backend:
//! the absolute cost of the wrapper), a `MockVfs` over its in-memory
//! reference filesystem, and the engine's real `View` on a local file
//! backend (an absolute number for the whole op, too). `Caller`
//! construction is in both paths, per op, as the adapter does it.
//!
//! The responder is a bare sink: what a FUSE reply does with the result is
//! the kernel's, not dispatch's. No tracing subscriber is installed, so
//! the span is the disabled one — the production default; with a
//! subscriber enabling `constellation_vfs::observe=debug` the subscriber
//! pays for what it records.
//!
//! **Allocations** are counted by a `#[global_allocator]` wrapper, per
//! thread (the engine's background threads do not count), as
//! `allocs/op` = allocations on the bench thread across a run ÷ ops.
//!
//! # Gating
//!
//! `cargo bench -p constellation-engine --bench vfs_bench` (`make
//! vfs-bench`) prints the table, then runs the criterion groups, and
//! **exits non-zero** if any scenario's dispatch overhead is 1 µs or more
//! or the observed path allocates more per op than the direct one (at all
//! with no backend; by more than half an allocation per op over one, whose
//! own count may wobble between runs). Without
//! `--bench` (`cargo test --benches`, a debug build) it runs every
//! scenario once and checks nothing about time.

use constellation_engine::{
    DeferredEvents, Engine, EngineConfig, EngineProfile, P2pMode, ViewSpec,
};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_vfs::mock::MockVfs;
use constellation_vfs::{
    Blocking, Caller, Fh, FrontendCaps, Ino, LockOwner, Name, Observer, OpCtx, OpKind, OpMetrics,
    OpenFlags, OpenOwner, ReadData, Responder, Vfs, VfsResult, ViewIdentity, WriteData, ROOT_INO,
};
use criterion::{black_box, Criterion};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------- allocation

/// Counts the allocations of the calling thread (a thread-local with a
/// `const` initialiser and no destructor: no allocation of its own).
struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

fn count(size: usize) {
    let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
    let _ = BYTES.try_with(|c| c.set(c.get() + size as u64));
}

// SAFETY: every method forwards to `System` with the caller's layout;
// counting touches only thread-local `Cell`s.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn allocations() -> (u64, u64) {
    (ALLOCS.with(Cell::get), BYTES.with(Cell::get))
}

// ----------------------------------------------------------------- scenarios

/// A responder that takes the result and drops it (a FUSE reply encodes
/// into a kernel buffer; that is not dispatch).
struct Sink;

impl<T> Responder<T> for Sink {
    fn done(self, result: VfsResult<T>) {
        black_box(result).ok();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Direct,
    Observed,
}

/// Which inline op to drive.
#[derive(Clone, Copy)]
enum Sel {
    Getattr(Ino),
    Lookup(Ino, &'static [u8]),
    Read(Ino, Fh, u32),
    /// No backend: complete a responder and nothing else.
    Nothing,
}

impl Sel {
    fn name(self) -> &'static str {
        match self {
            Sel::Getattr(_) | Sel::Nothing => "getattr",
            Sel::Lookup(..) => "lookup",
            Sel::Read(..) => "read",
        }
    }
}

/// `iters` ops of `sel` into `vfs`, one way (`mode`).
#[inline(never)]
fn drive<V: Vfs>(vfs: &V, obs: &Observer, mode: Mode, sel: Sel, iters: u64) {
    for _ in 0..iters {
        // As the FUSE adapter builds it per request: uid, gid, pid.
        let caller = Caller::new(1000, 1000, Some(4242));
        match (mode, sel) {
            (Mode::Direct, Sel::Getattr(ino)) => {
                vfs.getattr(&OpCtx::new(OpKind::Getattr, &caller), ino, None, Sink)
            }
            (Mode::Direct, Sel::Lookup(parent, name)) => vfs.lookup(
                &OpCtx::new(OpKind::Lookup, &caller),
                parent,
                Name::new(name),
                Sink,
            ),
            (Mode::Direct, Sel::Read(ino, fh, len)) => {
                vfs.read(&OpCtx::new(OpKind::Read, &caller), ino, fh, 0, len, Sink)
            }
            (Mode::Direct, Sel::Nothing) => {
                let cx = OpCtx::new(OpKind::Getattr, &caller);
                black_box(&cx);
                Sink.done(Ok(()));
            }
            (Mode::Observed, Sel::Getattr(ino)) => {
                let op = obs.begin(OpKind::Getattr, ino);
                let _in = op.enter();
                vfs.getattr(&op.ctx(&caller), ino, None, op.responder(Sink));
            }
            (Mode::Observed, Sel::Lookup(parent, name)) => {
                let op = obs.begin(OpKind::Lookup, parent);
                let _in = op.enter();
                vfs.lookup(
                    &op.ctx(&caller),
                    parent,
                    Name::new(name),
                    op.responder(Sink),
                );
            }
            (Mode::Observed, Sel::Read(ino, fh, len)) => {
                let op = obs.begin(OpKind::Read, ino);
                let _in = op.enter();
                vfs.read(&op.ctx(&caller), ino, fh, 0, len, op.responder(Sink));
            }
            (Mode::Observed, Sel::Nothing) => {
                let op = obs.begin(OpKind::Getattr, 1);
                let _in = op.enter();
                black_box(op.ctx(&caller));
                op.responder(Sink).done(Ok(()));
            }
        }
    }
}

struct Scenario {
    backend: &'static str,
    op: &'static str,
    run: RefCell<Box<dyn FnMut(Mode, u64)>>,
}

impl Scenario {
    fn new<V: Vfs>(
        backend: &'static str,
        vfs: Arc<V>,
        sel: Sel,
        // Bounded-memory backends (the mock records every call).
        after_chunk: impl Fn(&V) + 'static,
    ) -> Self {
        let obs = Observer::with_metrics(
            "bench",
            &ViewIdentity {
                id: 1,
                metric_view: Some("pv-bench".into()),
            },
            OpMetrics::detached("bench", Some("pv-bench")),
        );
        Scenario {
            backend,
            op: sel.name(),
            run: RefCell::new(Box::new(move |mode, iters| {
                let mut left = iters;
                while left > 0 {
                    let n = left.min(1024);
                    drive(&*vfs, &obs, mode, sel, n);
                    after_chunk(&vfs);
                    left -= n;
                }
            })),
        }
    }

    fn run(&self, mode: Mode, iters: u64) {
        (self.run.borrow_mut())(mode, iters);
    }

    /// Both ways, `ROUNDS` times, alternating which goes first: a backend
    /// whose own state drifts as ops accumulate (the engine's read path
    /// keeps readahead and atime state) then drifts under both alike, and
    /// the overhead is the median of the per-round differences.
    fn measure(&self, iters: u64) -> Measured {
        const ROUNDS: usize = 15;
        for mode in [Mode::Direct, Mode::Observed] {
            self.run(mode, iters.min(2000)); // warm caches and lazy paths
        }
        let mut direct = Vec::new();
        let mut observed = Vec::new();
        let mut diffs = Vec::new();
        let (mut d_allocs, mut o_allocs) = ((0.0, 0.0), (0.0, 0.0));
        for round in 0..ROUNDS {
            let order = if round % 2 == 0 {
                [Mode::Direct, Mode::Observed]
            } else {
                [Mode::Observed, Mode::Direct]
            };
            let (mut d, mut o) = (0.0, 0.0);
            for mode in order {
                let (a0, b0) = allocations();
                let start = Instant::now();
                self.run(mode, iters);
                let ns = start.elapsed().as_nanos() as f64 / iters as f64;
                let (a1, b1) = allocations();
                let per_op = (
                    (a1 - a0) as f64 / iters as f64,
                    (b1 - b0) as f64 / iters as f64,
                );
                match mode {
                    Mode::Direct => (d, d_allocs) = (ns, per_op),
                    Mode::Observed => (o, o_allocs) = (ns, per_op),
                }
            }
            direct.push(d);
            observed.push(o);
            diffs.push(o - d);
        }
        Measured {
            direct_ns: median(&mut direct),
            observed_ns: median(&mut observed),
            overhead_ns: median(&mut diffs),
            direct_allocs: d_allocs,
            observed_allocs: o_allocs,
        }
    }
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[v.len() / 2]
}

/// One scenario's numbers: ns/op, and (allocations/op, bytes/op).
struct Measured {
    direct_ns: f64,
    observed_ns: f64,
    overhead_ns: f64,
    direct_allocs: (f64, f64),
    observed_allocs: (f64, f64),
}

// ------------------------------------------------------------------ backends

/// A drained, populated view: `/f` holds 4 KiB, opened read-only at `fh`.
struct Populated {
    file: Ino,
    fh: Fh,
}

/// Create `/f` with 4 KiB in it, close it, and open it for reading.
fn populate<V: Vfs>(vfs: &V) -> Populated {
    let caller = Caller::root();
    let rw = OpenFlags::READ | OpenFlags::WRITE | OpenFlags::CREATE;
    let (entry, opened) = Blocking::run(|r| {
        vfs.create(
            &OpCtx::new(OpKind::Create, &caller),
            ROOT_INO,
            Name::new(b"f"),
            0o644,
            rw,
            OpenOwner::NONE,
            r,
        )
    })
    .expect("create /f");
    let (file, wfh) = (entry.attr.ino, opened.fh);
    let data = vec![0x5a_u8; 4096];
    let n = Blocking::run(|r| {
        vfs.write(
            &OpCtx::new(OpKind::Write, &caller),
            file,
            wfh,
            0,
            WriteData::Borrowed(&data),
            rw,
            r,
        )
    })
    .expect("write /f");
    assert_eq!(n, 4096);
    Blocking::run(|r| {
        vfs.flush(
            &OpCtx::new(OpKind::Flush, &caller),
            file,
            wfh,
            LockOwner(1),
            r,
        )
    })
    .expect("flush /f");
    Blocking::run(|r| {
        vfs.release(
            &OpCtx::new(OpKind::Release, &caller),
            file,
            wfh,
            rw,
            Some(LockOwner(1)),
            r,
        )
    })
    .expect("release /f");
    let opened = Blocking::run(|r| {
        vfs.open(
            &OpCtx::new(OpKind::Open, &caller),
            file,
            OpenFlags::READ,
            OpenOwner::NONE,
            r,
        )
    })
    .expect("open /f");
    let p = Populated {
        file,
        fh: opened.fh,
    };
    // What the scenarios will do must work, or they measure refusals.
    let read: ReadData =
        Blocking::run(|r| vfs.read(&OpCtx::new(OpKind::Read, &caller), p.file, p.fh, 0, 4096, r))
            .expect("read /f");
    assert_eq!(read.len(), 4096, "the whole file reads back");
    Blocking::run(|r| {
        vfs.lookup(
            &OpCtx::new(OpKind::Lookup, &caller),
            ROOT_INO,
            Name::new(b"f"),
            r,
        )
    })
    .expect("lookup /f");
    p
}

fn scenarios_of<V: Vfs>(
    backend: &'static str,
    vfs: &Arc<V>,
    after_chunk: impl Fn(&V) + Clone + 'static,
) -> Vec<Scenario> {
    let p = populate(&**vfs);
    vec![
        Scenario::new(
            backend,
            vfs.clone(),
            Sel::Getattr(ROOT_INO),
            after_chunk.clone(),
        ),
        Scenario::new(
            backend,
            vfs.clone(),
            Sel::Lookup(ROOT_INO, b"f"),
            after_chunk.clone(),
        ),
        Scenario::new(
            backend,
            vfs.clone(),
            Sel::Read(p.file, p.fh, 4096),
            after_chunk,
        ),
    ]
}

/// One engine on a local file backend, offline (no P2P), as the
/// conformance kit runs it. Held for the process's life.
struct Node {
    _dir: tempfile::TempDir,
    _rt: tokio::runtime::Runtime,
    _engine: Engine,
}

fn engine_view() -> (Node, Arc<constellation_engine::View>) {
    let dir = tempfile::TempDir::new().unwrap();
    let backend = format!("file://{}/backend", dir.path().display());
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
            runtime: Some(rt.handle().clone()),
            ..EngineConfig::new(&backend)
        },
        HostServices::native(),
        EngineProfile {
            p2p: P2pMode::Off,
            ..EngineProfile::desktop()
        },
    )
    .expect("Engine::start");
    let view = engine
        .open_view(
            ViewSpec::new("/"),
            FrontendCaps::linux_fuse(false),
            DeferredEvents::new(),
        )
        .expect("open the root view");
    (
        Node {
            _dir: dir,
            _rt: rt,
            _engine: engine,
        },
        view,
    )
}

fn scenarios() -> (Vec<Scenario>, Node) {
    let mut all = vec![Scenario::new(
        "observer",
        Arc::new(MockVfs::new()),
        Sel::Nothing,
        |_| {},
    )];
    let mock = Arc::new(MockVfs::reference(FrontendCaps::linux_fuse(false)));
    let scenario_mock = mock.clone();
    all.extend(scenarios_of("mock", &scenario_mock, |m: &MockVfs| {
        m.clear_records()
    }));
    mock.clear_records();
    let (node, view) = engine_view();
    all.extend(scenarios_of("view", &view, |_| {}));
    (all, node)
}

// -------------------------------------------------------------------- report

/// Dispatch overhead the gate allows, ns.
const OVERHEAD_LIMIT_NS: f64 = 1000.0;

/// Print the direct/observed table; whether every scenario is within the
/// §6.9 targets.
fn report(all: &[Scenario], smoke: bool) -> bool {
    println!(
        "\n{:<9} {:<8} {:>12} {:>12} {:>12}   {:>17}   {:>17}",
        "backend",
        "op",
        "direct ns",
        "observed ns",
        "overhead ns",
        "direct alloc/op",
        "observed alloc/op"
    );
    let mut ok = true;
    for s in all {
        let iters = if smoke {
            1
        } else if s.backend == "view" {
            20_000
        } else {
            200_000
        };
        let m = s.measure(iters);
        println!(
            "{:<9} {:<8} {:>12.1} {:>12.1} {:>+12.1}   {:>8.3} ({:>5.0}B)   {:>8.3} ({:>5.0}B)",
            s.backend,
            s.op,
            m.direct_ns,
            m.observed_ns,
            m.overhead_ns,
            m.direct_allocs.0,
            m.direct_allocs.1,
            m.observed_allocs.0,
            m.observed_allocs.1
        );
        let (overhead, dallocs, oallocs) = (m.overhead_ns, m.direct_allocs.0, m.observed_allocs.0);
        if smoke {
            continue;
        }
        if overhead >= OVERHEAD_LIMIT_NS {
            println!("  FAIL: dispatch overhead {overhead:.0} ns is not under 1 us");
            ok = false;
        }
        // With no backend the path must not allocate at all. Over a real
        // one, the backend's own count may wobble a little between two
        // runs (a map growing, a cache evicting on this thread); an
        // allocation per op in the path shows as a whole one.
        let extra_limit = if s.backend == "observer" { 0.0 } else { 0.5 };
        if oallocs - dallocs > extra_limit {
            println!(
                "  FAIL: the observed path allocates {:.3}/op more than the direct one",
                oallocs - dallocs
            );
            ok = false;
        }
    }
    println!(
        "\ntargets (plan 31 §6.9): overhead < {OVERHEAD_LIMIT_NS:.0} ns/op; no extra allocation per inline op.\n"
    );
    ok
}

fn criterion_groups(c: &mut Criterion, all: &[Scenario]) {
    let mut group = c.benchmark_group("vfs_dispatch");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    for s in all {
        for (mode, label) in [(Mode::Direct, "direct"), (Mode::Observed, "observed")] {
            group.bench_function(format!("{}/{}/{}", s.backend, s.op, label), |b| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    s.run(mode, iters);
                    start.elapsed()
                })
            });
        }
    }
    group.finish();
}

fn main() {
    // `cargo bench` passes `--bench`; `cargo test --benches` does not.
    let measuring = std::env::args().any(|a| a == "--bench");
    let (all, _node) = scenarios();
    let ok = report(&all, !measuring);
    let mut c = Criterion::default().configure_from_args().without_plots();
    criterion_groups(&mut c, &all);
    c.final_summary();
    if measuring && !ok {
        std::process::exit(1);
    }
}
