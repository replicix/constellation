mod corpus;
mod engine;
mod engines;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use constellation_mtree::record::{Attrs, Kind};
use rand::prelude::*;
use rand::rngs::SmallRng;
use serde::Serialize;

use corpus::{generate, Corpus, InoAlloc};
use engine::{Engine, LatSummary, Latencies};
use engines::fjall3_engine::Fjall3Engine;
use engines::fjall_engine::FjallEngine;
use engines::mtree_engine::MtreeEngine;
use engines::redb_engine::RedbEngine;
use engines::sqlite_engine::{SqliteEngine, SqliteTuning};

#[derive(Parser, Debug)]
struct Args {
    /// sqlite | sqlite-tuned | mtree | redb | fjall
    #[arg(long)]
    engine: String,
    #[arg(long, default_value_t = 1_500_000)]
    entries: usize,
    #[arg(long, default_value_t = 256)]
    cache_mb: usize,
    #[arg(long, default_value = "1,2,4,8,16,32")]
    threads: String,
    /// Total aging mutations as a multiple of the initial key count.
    #[arg(long, default_value_t = 4.0)]
    aging_multiplier: f64,
    #[arg(long, default_value_t = 60)]
    mixed_secs: u64,
    #[arg(long, default_value = "/mnt/enginebench")]
    data_dir: PathBuf,
    #[arg(long, default_value = "results.jsonl")]
    out: PathBuf,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Skip aging + aged phase (fast smoke test).
    #[arg(long, default_value_t = false)]
    quick: bool,
    /// Only for `--engine fjall3-custom`: worker thread count.
    #[arg(long, default_value_t = 4)]
    fjall_workers: usize,
    /// Only for `--engine fjall3-custom`: max memtable size in MiB (0 = default 64 MiB).
    #[arg(long, default_value_t = 0)]
    fjall_membuf_mb: u64,
    /// Only for `--engine fjall3-custom`: pin L0-L2 filter/index blocks.
    #[arg(long, default_value_t = false)]
    fjall_pin: bool,
    /// Only for `--engine fjall3-custom`: expect_point_read_hits + hash-ratio.
    #[arg(long, default_value_t = false)]
    fjall_pointread: bool,
}

fn parse_threads(s: &str) -> Vec<usize> {
    s.split(',').filter_map(|t| t.trim().parse().ok()).collect()
}

fn proc_io_write_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("write_bytes:"))
                .and_then(|l| l.split(':').nth(1))
                .and_then(|v| v.trim().parse().ok())
        })
        .unwrap_or(0)
}

fn now_ns() -> i64 {
    1_760_000_000_000_000_000
}

/// Peak RSS ever reached by this process so far (`VmHWM`, kB -> bytes).
/// Monotonic non-decreasing for the life of the process, so sampling it
/// at any point (and especially right before exit) gives the true peak,
/// unlike an instantaneous `VmRSS` read.
fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

#[derive(Serialize, Default)]
struct PhaseReport {
    engine: String,
    phase: String,
    cache_mb: usize,
    entries: usize,
    single_thread: HashMap<String, LatSummary>,
    read_scaling: Vec<(usize, f64)>,
    write_scaling: Vec<(usize, f64)>,
    disk_bytes: u64,
    bytes_written_total_engine: u64,
    proc_io_write_bytes_delta: u64,
    wall_secs: f64,
    peak_rss_bytes: u64,
}

#[derive(Serialize, Default)]
struct AgingReport {
    engine: String,
    cache_mb: usize,
    mutations: u64,
    wall_secs: f64,
    proc_io_write_bytes_delta: u64,
    disk_bytes_before: u64,
    disk_bytes_after: u64,
    peak_rss_bytes: u64,
}

#[derive(Serialize, Default)]
struct MixedWindow {
    t_offset_s: f64,
    ops: u64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
}

#[derive(Serialize, Default)]
struct MixedReport {
    engine: String,
    cache_mb: usize,
    threads: usize,
    windows: Vec<MixedWindow>,
}

fn build_engine(name: &str, dir: &PathBuf, cache_mb: usize, args: &Args) -> Arc<dyn Engine> {
    let cache_bytes = cache_mb * 1024 * 1024;
    match name {
        "sqlite" => Arc::new(SqliteEngine::create(&dir.join("sqlite.db"), SqliteTuning::Product)),
        "sqlite-tuned" => Arc::new(SqliteEngine::create(
            &dir.join("sqlite_tuned.db"),
            SqliteTuning::Tuned { cache_kib: -(cache_mb as i64 * 1024), mmap_bytes: (cache_bytes as i64) * 4 },
        )),
        "mtree" => Arc::new(MtreeEngine::create(&dir.join("mtree"), cache_bytes)),
        "redb" => Arc::new(RedbEngine::create(&dir.join("redb.db"), cache_bytes)),
        "fjall" => Arc::new(FjallEngine::create(&dir.join("fjall"), cache_bytes as u64)),
        "fjall-tuned" => Arc::new(FjallEngine::create_tuned(&dir.join("fjall_tuned"), cache_bytes as u64)),
        "fjall3" => Arc::new(Fjall3Engine::create(&dir.join("fjall3"), cache_bytes as u64)),
        "fjall3-tuned" => Arc::new(Fjall3Engine::create_tuned(&dir.join("fjall3_tuned"), cache_bytes as u64)),
        "fjall3-custom" => Arc::new(Fjall3Engine::create_custom(
            &dir.join("fjall3_custom"),
            cache_bytes as u64,
            args.fjall_workers,
            (args.fjall_membuf_mb > 0).then_some(args.fjall_membuf_mb),
            args.fjall_pin,
            args.fjall_pointread,
        )),
        other => panic!("unknown engine {other}"),
    }
}

fn load_corpus(engine: &dyn Engine, corpus: &Corpus) {
    if engine.getattr(1).is_none() {
        let attrs = Attrs { kind: Kind::Dir, mode: 0o40755, uid: 0, gid: 0, nlink: 2, size: 4096, mtime_ns: now_ns(), ctime_ns: now_ns(), rdev: 0 };
        engine.init_root(attrs);
    }
    for (i, r) in corpus.recs.iter().enumerate() {
        match r.kind {
            Kind::Dir => engine.mkdir(r.parent, &r.name, r.ino, r.attrs()),
            Kind::Symlink => engine.symlink(r.parent, &r.name, r.ino, r.attrs(), r.symlink_target.as_deref().unwrap_or(b"target")),
            _ => engine.create(r.parent, &r.name, r.ino, r.attrs(), &r.xattrs),
        }
        if i % 20_000 == 0 {
            engine.flush();
        }
    }
    for l in &corpus.extra_links {
        engine.link(l.parent, &l.name, l.ino);
    }
    engine.flush();
}

struct Samples {
    dentries: Vec<(u64, Vec<u8>)>,
    inos: Vec<u64>,
    dirs: Vec<u64>,
    big_dirs: Vec<u64>,
}

fn sample(corpus: &Corpus, seed: u64) -> Samples {
    let mut rng = SmallRng::seed_from_u64(seed);
    let n = corpus.recs.len();
    let dentries: Vec<(u64, Vec<u8>)> = (0..200_000.min(n)).map(|_| {
        let r = &corpus.recs[rng.random_range(0..n)];
        (r.parent, r.name.clone())
    }).collect();
    let inos: Vec<u64> = (0..200_000.min(n)).map(|_| corpus.recs[rng.random_range(0..n)].ino).collect();
    let dirs: Vec<u64> = corpus.recs.iter().filter(|r| r.kind == Kind::Dir).map(|r| r.ino).take(50_000).collect();
    Samples { dentries, inos, dirs, big_dirs: corpus.big_dirs.clone() }
}

fn time_op<F: FnMut()>(lat: &mut Latencies, mut f: F) {
    let t0 = Instant::now();
    f();
    lat.push(t0.elapsed().as_nanos() as u64);
}

fn bench_single_thread(engine: &dyn Engine, s: &Samples, ino_alloc: &InoAlloc) -> HashMap<String, LatSummary> {
    let mut out = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(777);

    let mut lat = Latencies::default();
    for _ in 0..30_000.min(s.dentries.len()) {
        let (p, n) = &s.dentries[rng.random_range(0..s.dentries.len())];
        time_op(&mut lat, || { engine.lookup(*p, n); });
    }
    out.insert("lookup".into(), lat.summary());

    let mut lat = Latencies::default();
    for _ in 0..30_000.min(s.inos.len()) {
        let ino = s.inos[rng.random_range(0..s.inos.len())];
        time_op(&mut lat, || { engine.getattr(ino); });
    }
    out.insert("getattr".into(), lat.summary());

    let mut lat = Latencies::default();
    for _ in 0..3_000.min(s.dirs.len()) {
        let d = s.dirs[rng.random_range(0..s.dirs.len())];
        time_op(&mut lat, || { engine.readdir(d, None, 100); });
    }
    out.insert("readdir_page100".into(), lat.summary());

    let mut lat = Latencies::default();
    for _ in 0..3_000.min(s.dirs.len()) {
        let d = s.dirs[rng.random_range(0..s.dirs.len())];
        time_op(&mut lat, || { engine.readdirplus(d, None, 100); });
    }
    out.insert("readdirplus_page100".into(), lat.summary());

    if !s.big_dirs.is_empty() {
        let mut lat = Latencies::default();
        for _ in 0..500 {
            let d = s.big_dirs[rng.random_range(0..s.big_dirs.len())];
            time_op(&mut lat, || { engine.readdirplus(d, None, 100); });
        }
        out.insert("readdirplus_bigdir_page100".into(), lat.summary());
    }

    let mut lat = Latencies::default();
    for _ in 0..10_000.min(s.inos.len()) {
        let ino = s.inos[rng.random_range(0..s.inos.len())];
        time_op(&mut lat, || { engine.listxattr(ino); });
    }
    out.insert("listxattr".into(), lat.summary());

    // Writes: create+unlink pairs under a scratch directory so repeated
    // runs do not grow the corpus.
    let scratch = s.dirs[0];
    let mut lat = Latencies::default();
    let mut created = Vec::new();
    for _ in 0..10_000 {
        let ino = ino_alloc.next();
        let name = format!("scratch{ino:x}").into_bytes();
        let attrs = Attrs { kind: Kind::File, mode: 0o100644, uid: 1000, gid: 1000, nlink: 1, size: 4096, mtime_ns: now_ns(), ctime_ns: now_ns(), rdev: 0 };
        time_op(&mut lat, || { engine.create(scratch, &name, ino, attrs, &[]); });
        created.push(name);
    }
    out.insert("create".into(), lat.summary());
    engine.flush();

    let mut lat = Latencies::default();
    for name in &created {
        let ino = s.inos[0]; // dummy, unused by setattr's key lookup path (uses ino directly)
        let _ = ino;
        if let Some((real_ino, _)) = engine.lookup(scratch, name) {
            let attrs = Attrs { kind: Kind::File, mode: 0o100644, uid: 1000, gid: 1000, nlink: 1, size: 8192, mtime_ns: now_ns() + 1, ctime_ns: now_ns() + 1, rdev: 0 };
            time_op(&mut lat, || { engine.setattr(real_ino, attrs); });
        }
    }
    out.insert("setattr".into(), lat.summary());

    let mut lat = Latencies::default();
    for name in &created {
        time_op(&mut lat, || { engine.unlink(scratch, name); });
    }
    out.insert("unlink".into(), lat.summary());
    engine.flush();

    out
}

fn bench_read_scaling(engine: Arc<dyn Engine>, s: &Samples, threads_list: &[usize]) -> Vec<(usize, f64)> {
    let mut out = Vec::new();
    for &t in threads_list {
        let total = Arc::new(AtomicU64::new(0));
        let dur = Duration::from_millis(1500);
        std::thread::scope(|scope| {
            let start = Instant::now();
            let mut handles = Vec::new();
            for tid in 0..t {
                let engine = engine.clone();
                let total = total.clone();
                let dentries = s.dentries.clone();
                handles.push(scope.spawn(move || {
                    let mut rng = SmallRng::seed_from_u64(1000 + tid as u64);
                    let mut n = 0u64;
                    while start.elapsed() < dur {
                        let (p, name) = &dentries[rng.random_range(0..dentries.len())];
                        std::hint::black_box(engine.lookup(*p, name));
                        n += 1;
                    }
                    total.fetch_add(n, Ordering::Relaxed);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
        let ops = total.load(Ordering::Relaxed);
        out.push((t, ops as f64 / dur.as_secs_f64()));
    }
    out
}

fn bench_write_scaling(engine: Arc<dyn Engine>, s: &Samples, ino_alloc: Arc<InoAlloc>, threads_list: &[usize]) -> Vec<(usize, f64)> {
    let mut out = Vec::new();
    for &t in threads_list {
        let total = Arc::new(AtomicU64::new(0));
        let dur = Duration::from_millis(1200);
        let dirs = s.dirs.clone();
        std::thread::scope(|scope| {
            let start = Instant::now();
            let mut handles = Vec::new();
            for tid in 0..t {
                let engine = engine.clone();
                let total = total.clone();
                let ino_alloc = ino_alloc.clone();
                let dirs = dirs.clone();
                handles.push(scope.spawn(move || {
                    let mut rng = SmallRng::seed_from_u64(2000 + tid as u64);
                    let mut n = 0u64;
                    while start.elapsed() < dur {
                        let ino = ino_alloc.next();
                        let dir = dirs[rng.random_range(0..dirs.len())];
                        let name = format!("wr{tid}_{ino:x}").into_bytes();
                        let attrs = Attrs { kind: Kind::File, mode: 0o100644, uid: 1000, gid: 1000, nlink: 1, size: 4096, mtime_ns: now_ns(), ctime_ns: now_ns(), rdev: 0 };
                        engine.create(dir, &name, ino, attrs, &[]);
                        n += 1;
                    }
                    total.fetch_add(n, Ordering::Relaxed);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        });
        engine.flush();
        let ops = total.load(Ordering::Relaxed);
        out.push((t, ops as f64 / dur.as_secs_f64()));
    }
    out
}

/// Aging: `n_hot` freshly created directories under heavy, skewed churn
/// (create/unlink/setattr/rename), plus a cold-tail component that
/// occasionally touches an original bulk-loaded entry. Continues the
/// global ino counter (§S1b), so hot-dir children scatter in ino space
/// exactly as a live filesystem's would.
fn age(engine: &dyn Engine, corpus: &Corpus, ino_alloc: &InoAlloc, mutations: u64, seed: u64) -> Vec<u64> {
    let mut rng = SmallRng::seed_from_u64(seed);
    const N_HOT: usize = 300;
    let root = 1u64;
    let mut hot_dirs = Vec::with_capacity(N_HOT);
    for i in 0..N_HOT {
        let ino = ino_alloc.next();
        let name = format!("hot{i:04x}").into_bytes();
        let attrs = Attrs { kind: Kind::Dir, mode: 0o40755, uid: 1000, gid: 1000, nlink: 2, size: 4096, mtime_ns: now_ns(), ctime_ns: now_ns(), rdev: 0 };
        engine.mkdir(root, &name, ino, attrs);
        hot_dirs.push(ino);
    }
    let mut children: Vec<Vec<Vec<u8>>> = vec![Vec::new(); N_HOT];
    let mut active = 0usize;
    let mut since_rotate = 0u64;
    let rotate_every = 400u64;

    let n_cold = corpus.recs.len().max(1);
    for step in 0..mutations {
        since_rotate += 1;
        if since_rotate > rotate_every {
            active = (active + 1) % N_HOT;
            since_rotate = 0;
        }
        if rng.random_bool(0.08) {
            // Cold tail: touch a uniformly random original entry.
            let r = &corpus.recs[rng.random_range(0..n_cold)];
            if r.ino != 1 {
                let mut attrs = r.attrs();
                attrs.mtime_ns = now_ns() + step as i64;
                engine.setattr(r.ino, attrs);
            }
            continue;
        }
        let hidx = if rng.random_bool(0.75) { active } else { rng.random_range(0..N_HOT) };
        let dir = hot_dirs[hidx];
        let has_children = !children[hidx].is_empty();
        let op: f64 = rng.random();
        if op < 0.45 || !has_children {
            let ino = ino_alloc.next();
            let name = format!("f{step:x}").into_bytes();
            let attrs = Attrs { kind: Kind::File, mode: 0o100644, uid: 1000, gid: 1000, nlink: 1, size: rng.random_range(0..65536), mtime_ns: now_ns() + step as i64, ctime_ns: now_ns() + step as i64, rdev: 0 };
            engine.create(dir, &name, ino, attrs, &[]);
            children[hidx].push(name);
        } else if op < 0.70 {
            let idx = rng.random_range(0..children[hidx].len());
            let name = children[hidx].swap_remove(idx);
            engine.unlink(dir, &name);
        } else if op < 0.90 {
            let idx = rng.random_range(0..children[hidx].len());
            let name = &children[hidx][idx];
            if let Some((ino, mut attrs)) = engine.lookup(dir, name) {
                attrs.mtime_ns = now_ns() + step as i64;
                attrs.size = attrs.size.wrapping_add(4096);
                engine.setattr(ino, attrs);
            }
        } else {
            // Rename within/across hot dirs.
            let idx = rng.random_range(0..children[hidx].len());
            let name = children[hidx].swap_remove(idx);
            let dst_idx = rng.random_range(0..N_HOT);
            let new_name = format!("mv{step:x}").into_bytes();
            engine.rename(dir, &name, hot_dirs[dst_idx], &new_name);
            children[dst_idx].push(new_name);
        }
        if step % 20_000 == 0 {
            engine.flush();
        }
    }
    engine.flush();
    hot_dirs
}

/// Samples `Engine::compaction_debt` once per second into `debt.log`
/// under `data_dir`, tagged with `phase`, for the duration between
/// `start` and the returned stop function being called. No-op for
/// engines that don't implement `compaction_debt` (returns `None`).
fn spawn_debt_sampler(
    engine: Arc<dyn Engine>,
    phase: &'static str,
    data_dir: PathBuf,
) -> (Arc<std::sync::atomic::AtomicBool>, std::thread::JoinHandle<()>) {
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let r2 = running.clone();
    let handle = std::thread::spawn(move || {
        let path = data_dir.join("debt.log");
        let mut f = match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(_) => return,
        };
        use std::io::Write;
        let t0 = Instant::now();
        while r2.load(Ordering::Relaxed) {
            if let Some((l0, tables, flushes, compactions, compacting_s)) = engine.compaction_debt() {
                let _ = writeln!(
                    f,
                    "{phase} t={:.1}s l0_tables={l0} total_tables={tables} outstanding_flushes={flushes} active_compactions={compactions} time_compacting_s={compacting_s:.2}",
                    t0.elapsed().as_secs_f64()
                );
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    (running, handle)
}

fn run_mixed(engine: Arc<dyn Engine>, s: &Samples, ino_alloc: Arc<InoAlloc>, secs: u64, threads: usize) -> Vec<MixedWindow> {
    let windows = Arc::new(std::sync::Mutex::new(Vec::<(f64, Vec<u64>)>::new()));
    let start = Instant::now();
    let dur = Duration::from_secs(secs);
    let dirs = s.dirs.clone();
    let dentries = s.dentries.clone();
    std::thread::scope(|scope| {
        for tid in 0..threads {
            let engine = engine.clone();
            let windows = windows.clone();
            let ino_alloc = ino_alloc.clone();
            let dirs = dirs.clone();
            let dentries = dentries.clone();
            scope.spawn(move || {
                let mut rng = SmallRng::seed_from_u64(9000 + tid as u64);
                let mut cur_window = (start.elapsed().as_secs_f64() / 1.0).floor() as i64;
                let mut local: Vec<u64> = Vec::new();
                while start.elapsed() < dur {
                    let t0 = Instant::now();
                    let r: f64 = rng.random();
                    if r < 0.70 {
                        let (p, name) = &dentries[rng.random_range(0..dentries.len())];
                        std::hint::black_box(engine.lookup(*p, name));
                    } else if r < 0.90 {
                        let d = dirs[rng.random_range(0..dirs.len())];
                        std::hint::black_box(engine.readdirplus(d, None, 32));
                    } else {
                        let ino = ino_alloc.next();
                        let dir = dirs[rng.random_range(0..dirs.len())];
                        let name = format!("mix{tid}_{ino:x}").into_bytes();
                        let attrs = Attrs { kind: Kind::File, mode: 0o100644, uid: 1000, gid: 1000, nlink: 1, size: 4096, mtime_ns: now_ns(), ctime_ns: now_ns(), rdev: 0 };
                        engine.create(dir, &name, ino, attrs, &[]);
                    }
                    local.push(t0.elapsed().as_nanos() as u64);
                    let w = (start.elapsed().as_secs_f64() / 1.0).floor() as i64;
                    if w != cur_window {
                        let mut wl = windows.lock().unwrap();
                        wl.push((cur_window as f64, std::mem::take(&mut local)));
                        cur_window = w;
                    }
                }
                if !local.is_empty() {
                    let mut wl = windows.lock().unwrap();
                    wl.push((cur_window as f64, local));
                }
            });
        }
    });
    let raw = Arc::try_unwrap(windows).unwrap().into_inner().unwrap();
    let mut by_window: HashMap<i64, Vec<u64>> = HashMap::new();
    for (t, samples) in raw {
        by_window.entry(t as i64).or_default().extend(samples);
    }
    let mut keys: Vec<i64> = by_window.keys().copied().collect();
    keys.sort();
    keys.into_iter().map(|k| {
        let mut v = by_window.remove(&k).unwrap();
        v.sort_unstable();
        let n = v.len().max(1);
        let at = |q: f64| v[((n as f64 - 1.0) * q).round() as usize] as f64 / 1000.0;
        MixedWindow { t_offset_s: k as f64, ops: v.len() as u64, p50_us: at(0.5), p99_us: at(0.99), p999_us: at(0.999) }
    }).collect()
}

fn main() {
    let args = Args::parse();
    let threads_list = parse_threads(&args.threads);
    std::fs::create_dir_all(&args.data_dir).unwrap();
    let engine_dir = args.data_dir.join(&args.engine).join(format!("cache{}", args.cache_mb));
    let _ = std::fs::remove_dir_all(&engine_dir);
    std::fs::create_dir_all(&engine_dir).unwrap();

    eprintln!("[{}] generating corpus: {} entries", args.engine, args.entries);
    let ino_alloc = Arc::new(InoAlloc::starting_at(2));
    let corpus = generate(args.entries, args.seed, &ino_alloc);
    eprintln!(
        "[{}] corpus: {} recs, {} dirs, {} big dirs, {} extra links",
        args.engine, corpus.recs.len(), corpus.dir_count, corpus.big_dirs.len(), corpus.extra_links.len()
    );

    let engine = build_engine(&args.engine, &engine_dir, args.cache_mb, &args);
    let s = sample(&corpus, args.seed ^ 0xabcd);

    let io0 = proc_io_write_bytes();
    let t0 = Instant::now();
    load_corpus(engine.as_ref(), &corpus);
    let load_wall = t0.elapsed().as_secs_f64();
    let io1 = proc_io_write_bytes();
    eprintln!("[{}] load done in {:.1}s, proc_io write delta {} MiB, disk_bytes {} MiB",
        args.engine, load_wall, (io1 - io0) / 1_000_000, engine.disk_bytes() / 1_000_000);

    let out = std::fs::OpenOptions::new().create(true).append(true).open(&args.out).unwrap();
    let mut writer = std::io::BufWriter::new(out);
    use std::io::Write;

    // ---- fresh phase ----
    let io_before = proc_io_write_bytes();
    let t0 = Instant::now();
    let single = bench_single_thread(engine.as_ref(), &s, &ino_alloc);
    let read_scaling = bench_read_scaling(engine.clone(), &s, &threads_list);
    let write_scaling = bench_write_scaling(engine.clone(), &s, ino_alloc.clone(), &threads_list);
    let wall = t0.elapsed().as_secs_f64();
    let io_after = proc_io_write_bytes();
    let fresh = PhaseReport {
        engine: args.engine.clone(), phase: "fresh".into(), cache_mb: args.cache_mb, entries: args.entries,
        single_thread: single, read_scaling, write_scaling,
        disk_bytes: engine.disk_bytes(), bytes_written_total_engine: engine.bytes_written_total(),
        proc_io_write_bytes_delta: io_after.saturating_sub(io_before), wall_secs: wall,
        peak_rss_bytes: peak_rss_bytes(),
    };
    writeln!(writer, "{}", serde_json::json!({"phase_report": fresh})).unwrap();
    writer.flush().unwrap();
    eprintln!("[{}] fresh phase done", args.engine);

    if args.quick {
        return;
    }

    // ---- aging ----
    let total_keys_est = (args.entries as f64 * 3.0) as u64; // inode+dentry+rdentry
    let mutations = (total_keys_est as f64 * args.aging_multiplier) as u64;
    let disk_before = engine.disk_bytes();
    let io_before = proc_io_write_bytes();
    let t0 = Instant::now();
    let (debt_running, debt_handle) = spawn_debt_sampler(engine.clone(), "aging", args.data_dir.clone());
    let hot_dirs = age(engine.as_ref(), &corpus, &ino_alloc, mutations, args.seed ^ 0x5eed);
    debt_running.store(false, Ordering::Relaxed);
    let _ = debt_handle.join();
    let wall = t0.elapsed().as_secs_f64();
    let io_after = proc_io_write_bytes();
    let aging_report = AgingReport {
        engine: args.engine.clone(), cache_mb: args.cache_mb, mutations, wall_secs: wall,
        proc_io_write_bytes_delta: io_after.saturating_sub(io_before),
        disk_bytes_before: disk_before, disk_bytes_after: engine.disk_bytes(),
        peak_rss_bytes: peak_rss_bytes(),
    };
    writeln!(writer, "{}", serde_json::json!({"aging_report": aging_report})).unwrap();
    writer.flush().unwrap();
    eprintln!("[{}] aging done: {} mutations in {:.1}s", args.engine, mutations, wall);

    if let Some((before, after)) = engine.compact() {
        eprintln!("[{}] compaction: {} MiB -> {} MiB", args.engine, before / 1_000_000, after / 1_000_000);
        writeln!(writer, "{}", serde_json::json!({"compaction_report": {"engine": args.engine, "cache_mb": args.cache_mb, "bytes_before": before, "bytes_after": after}})).unwrap();
        writer.flush().unwrap();
    }

    if let (Some(bulk_dir), Some(&hot_dir)) = (corpus.big_dirs.first(), hot_dirs.first()) {
        let bulk_cost = engine.cold_leaf_reads(*bulk_dir, 100);
        let hot_cost = engine.cold_leaf_reads(hot_dir, 100);
        if let (Some(b), Some(h)) = (bulk_cost, hot_cost) {
            eprintln!("[{}] locality: bulk-built dir cold reads={}, incrementally-aged dir cold reads={}", args.engine, b, h);
            writeln!(writer, "{}", serde_json::json!({"locality_report": {"engine": args.engine, "cache_mb": args.cache_mb, "bulk_dir_cold_reads": b, "aged_dir_cold_reads": h}})).unwrap();
            writer.flush().unwrap();
        }
    }

    // ---- aged phase ----
    let io_before = proc_io_write_bytes();
    let t0 = Instant::now();
    let single = bench_single_thread(engine.as_ref(), &s, &ino_alloc);
    let read_scaling = bench_read_scaling(engine.clone(), &s, &threads_list);
    let write_scaling = bench_write_scaling(engine.clone(), &s, ino_alloc.clone(), &threads_list);
    let wall = t0.elapsed().as_secs_f64();
    let io_after = proc_io_write_bytes();
    let aged = PhaseReport {
        engine: args.engine.clone(), phase: "aged".into(), cache_mb: args.cache_mb, entries: args.entries,
        single_thread: single, read_scaling, write_scaling,
        disk_bytes: engine.disk_bytes(), bytes_written_total_engine: engine.bytes_written_total(),
        proc_io_write_bytes_delta: io_after.saturating_sub(io_before), wall_secs: wall,
        peak_rss_bytes: peak_rss_bytes(),
    };
    writeln!(writer, "{}", serde_json::json!({"phase_report": aged})).unwrap();
    writer.flush().unwrap();
    eprintln!("[{}] aged phase done", args.engine);

    // ---- sustained mixed workload ----
    let mixed_threads = 8usize;
    let (debt_running, debt_handle) = spawn_debt_sampler(engine.clone(), "mixed", args.data_dir.clone());
    let windows = run_mixed(engine.clone(), &s, ino_alloc.clone(), args.mixed_secs, mixed_threads);
    debt_running.store(false, Ordering::Relaxed);
    let _ = debt_handle.join();
    let mixed = MixedReport { engine: args.engine.clone(), cache_mb: args.cache_mb, threads: mixed_threads, windows };
    writeln!(writer, "{}", serde_json::json!({"mixed_report": mixed})).unwrap();
    writer.flush().unwrap();
    eprintln!("[{}] mixed workload done", args.engine);

    eprintln!("[{}] final disk_bytes {} MiB, corpus logical bytes {} MiB",
        args.engine, engine.disk_bytes() / 1_000_000, corpus.total_logical_bytes / 1_000_000);
}
