//! `log_writer` (plan 44 §8): what one log event costs the logging thread,
//! with the synchronous writer this plan replaced and with the sink queue,
//! on a file sink.
//!
//! The sink is stderr redirected to a file in `LOG_BENCH_DIR` (default: the
//! system temp dir), exactly as a daemon's is (`daemon.log`, `mount.log`),
//! so the file system under that directory is the only thing that differs
//! between runs: run it with the directory on ext4, xfs, btrfs and tmpfs.
//! Each cell runs in a child process of its own (its stderr is the file),
//! three times; the table prints the median.
//!
//! Cells: one thread; eight threads flat out (the queue's worst case: the
//! writer is outrun for as long as the run lasts); eight threads at 80 000
//! lines/s together (a rate three orders of magnitude past a busy daemon's,
//! paced by spinning so the worst event is not a sleep's jitter).
//!
//! Each event is an `info!` of six fields through the fmt layer, about 250
//! bytes formatted. Columns: mean cost per event on the logging threads, the
//! worst single event, allocations per event over **all** threads (the
//! writer thread's included), and for the queue the time the writer needed
//! to drain after the last event (the file is the same bytes either way) and
//! the queue's peak length in entries (above 4096 the deque grew under the
//! lock).
//!
//! `LogBuffer`'s queue mode here is `LogBuffer::start` on `StderrSink` with
//! a 1 GiB bound, so a flat-out cell cannot drop: a drop would be cheaper
//! than a write and flatter the queue. The synchronous column is a copy of
//! the writer as it was before plan 44 (a `String` per line in the ring, one
//! unbuffered `write(2)` to stderr per event, under std's stderr lock).
//!
//! Exit status: non-zero if the queue allocates more per event than the
//! synchronous writer, or costs more than 10% above it per event in a cell
//! (`--bench` only; without it, `cargo test --benches`, every cell runs once
//! with few events and nothing is checked). `LOG_BENCH_REPS=<n>` sets the
//! repetitions per cell (3); `LOG_BENCH_JSON=<path>` also writes the table.

use constellation_engine::log_buffer::{LogBuffer, QueueConfig, StderrSink};
use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::VecDeque;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::fmt::MakeWriter;

// ---------------------------------------------------------------- allocation

/// Counts every allocation of the process while `COUNTING` is on (the
/// writer thread's included), with one relaxed atomic.
struct Counting;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every method forwards to `System` with the caller's layout;
// counting touches only an atomic.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

// ------------------------------------------------- the writer as it was

/// A copy of `log_buffer.rs` before plan 44: lock the ring, one `String`
/// per completed line, unlock, then an unbuffered write of the bytes to
/// stderr on the calling thread.
#[derive(Clone, Default)]
struct OldBuffer(Arc<Mutex<OldState>>);

#[derive(Default)]
struct OldState {
    lines: VecDeque<String>,
    partial: Vec<u8>,
    total: u64,
}

struct OldWriter(OldBuffer);

impl<'a> MakeWriter<'a> for OldBuffer {
    type Writer = OldWriter;
    fn make_writer(&'a self) -> OldWriter {
        OldWriter(self.clone())
    }
}

impl Write for OldWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut state = self.0 .0.lock().unwrap();
        for byte in bytes {
            if *byte == b'\n' {
                let line = String::from_utf8_lossy(&state.partial).into_owned();
                state.partial.clear();
                state.lines.push_back(line);
                state.total += 1;
                if state.lines.len() > 2_000 {
                    state.lines.pop_front();
                }
            } else {
                state.partial.push(*byte);
            }
        }
        drop(state);
        let _ = std::io::stderr().write_all(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

// ------------------------------------------------------------------- a cell

#[derive(Clone, Copy, PartialEq, Eq)]
enum Variant {
    Sync,
    Queue,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::Sync => "sync",
            Variant::Queue => "queue",
        }
    }
}

#[derive(Clone, Copy)]
struct Cell {
    name: &'static str,
    threads: usize,
    /// Lines per second over all threads; 0: flat out.
    rate: u64,
    /// Events per thread in a measuring run.
    events: u64,
}

const CELLS: [Cell; 3] = [
    Cell {
        name: "1 thread",
        threads: 1,
        rate: 0,
        events: 300_000,
    },
    Cell {
        name: "8 threads flat out",
        threads: 8,
        rate: 0,
        events: 100_000,
    },
    Cell {
        name: "8 threads at 80k/s",
        threads: 8,
        rate: 80_000,
        events: 10_000,
    },
];

#[derive(Debug, Clone, Copy, Default)]
struct Result {
    mean_ns: f64,
    worst_ns: u64,
    allocs_per_event: f64,
    drain_ms: f64,
    dropped: u64,
    peak_entries: u64,
}

fn event(i: u64) {
    tracing::info!(
        op = "write",
        ino = 4242u64,
        off = i * 4096,
        len = 4096u32,
        node = "a",
        took_us = 17u64,
        "tagged op landed"
    );
}

/// One run of `events` per thread; the per-thread totals and worst event.
fn run_threads(dispatch: &tracing::Dispatch, cell: Cell, events: u64) -> (Duration, u64, u64) {
    let barrier = Arc::new(Barrier::new(cell.threads));
    let handles: Vec<_> = (0..cell.threads)
        .map(|_| {
            let dispatch = dispatch.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    let interval = (cell.rate > 0).then(|| {
                        Duration::from_nanos(1_000_000_000 * cell.threads as u64 / cell.rate)
                    });
                    barrier.wait();
                    let mut busy = Duration::ZERO;
                    let mut worst = 0u64;
                    let mut next = Instant::now();
                    for i in 0..events {
                        if let Some(interval) = interval {
                            next += interval;
                            while Instant::now() < next {
                                std::hint::spin_loop();
                            }
                        }
                        let started = Instant::now();
                        event(i);
                        let took = started.elapsed();
                        busy += took;
                        worst = worst.max(took.as_nanos() as u64);
                    }
                    (busy, worst, events)
                })
            })
        })
        .collect();
    let mut busy = Duration::ZERO;
    let mut worst = 0;
    let mut total = 0;
    for h in handles {
        let (b, w, n) = h.join().unwrap();
        busy += b;
        worst = worst.max(w);
        total += n;
    }
    (busy, worst, total)
}

/// The child: measure one cell with one variant, print `RESULT ...` on
/// stdout. Its stderr is the sink file.
fn child(variant: Variant, cell: Cell, events: u64) {
    use tracing_subscriber::layer::SubscriberExt;
    let (dispatch, queue): (tracing::Dispatch, Option<LogBuffer>) = match variant {
        Variant::Sync => {
            let layer = tracing_subscriber::fmt::layer().with_writer(OldBuffer::default());
            (
                tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
                None,
            )
        }
        Variant::Queue => {
            let buffer = LogBuffer::default();
            buffer
                .start(QueueConfig::with_limit(1 << 30), Box::new(StderrSink))
                .unwrap();
            let layer = tracing_subscriber::fmt::layer().with_writer(buffer.clone());
            (
                tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
                Some(buffer),
            )
        }
    };
    let drain = |queue: &Option<LogBuffer>| {
        if let Some(q) = queue {
            let started = Instant::now();
            assert!(
                q.flush(Duration::from_secs(120)).complete,
                "the writer did not drain"
            );
            started.elapsed()
        } else {
            Duration::ZERO
        }
    };
    // Warm up (page cache, thread pools, the deque capacity), then the
    // timing pass with counting off, then an allocation pass.
    run_threads(&dispatch, cell, events.min(5_000));
    drain(&queue);
    let (busy, worst, total) = run_threads(&dispatch, cell, events);
    let drained = drain(&queue);
    COUNTING.store(true, Ordering::SeqCst);
    ALLOCS.store(0, Ordering::SeqCst);
    let alloc_events = events.min(20_000);
    let (_, _, alloc_total) = run_threads(&dispatch, cell, alloc_events);
    drain(&queue);
    COUNTING.store(false, Ordering::SeqCst);
    let allocs = ALLOCS.load(Ordering::SeqCst);
    let dropped = queue.as_ref().map_or(0, |q| q.stats().dropped_lines);
    let peak = queue.as_ref().map_or(0, |q| q.stats().queue_entries_max);
    println!(
        "RESULT {} {} {} {} {} {}",
        busy.as_nanos() as f64 / total as f64,
        worst,
        allocs as f64 / alloc_total as f64,
        drained.as_secs_f64() * 1000.0,
        dropped,
        peak
    );
}

fn parent_run(dir: &std::path::Path, variant: Variant, cell: usize, events_scale: u64) -> Result {
    // A fresh file each run: the runs of a long matrix would otherwise fill
    // the file system under test.
    let path = dir.join(format!("sink-{}-{cell}.log", variant.name()));
    let _ = std::fs::remove_file(&path);
    let sink = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .unwrap();
    let out = Command::new(std::env::current_exe().unwrap())
        .args([
            "--child",
            variant.name(),
            &cell.to_string(),
            &events_scale.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::from(sink))
        .output()
        .unwrap();
    assert!(out.status.success(), "the child failed: {}", out.status);
    let text = String::from_utf8_lossy(&out.stdout);
    let line = text
        .lines()
        .find_map(|l| l.strip_prefix("RESULT "))
        .expect("the child prints RESULT");
    let f: Vec<f64> = line.split(' ').map(|n| n.parse().unwrap()).collect();
    Result {
        mean_ns: f[0],
        worst_ns: f[1] as u64,
        allocs_per_event: f[2],
        drain_ms: f[3],
        dropped: f[4] as u64,
        peak_entries: f[5] as u64,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--child") {
        let variant = if args[2] == "sync" {
            Variant::Sync
        } else {
            Variant::Queue
        };
        let cell = CELLS[args[3].parse::<usize>().unwrap()];
        child(variant, cell, args[4].parse().unwrap());
        return;
    }
    let measuring = args.iter().any(|a| a == "--bench");
    let reps = if measuring {
        std::env::var("LOG_BENCH_REPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n: &usize| *n > 0)
            .unwrap_or(3)
    } else {
        1
    };
    let dir = std::env::var_os("LOG_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("log-writer-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    println!(
        "log_writer: sink files under {} (set LOG_BENCH_DIR to pick the file system)",
        dir.display()
    );
    println!(
        "{:<22} {:<6} {:>10} {:>12} {:>13} {:>10} {:>8} {:>11}",
        "cell",
        "writer",
        "ns/event",
        "worst (us)",
        "allocs/event",
        "drain (ms)",
        "dropped",
        "peak queue"
    );
    let mut ok = true;
    let mut rows = Vec::new();
    for (index, cell) in CELLS.iter().enumerate() {
        let events = if measuring { cell.events } else { 200 };
        let mut medians = Vec::new();
        for variant in [Variant::Sync, Variant::Queue] {
            let runs: Vec<Result> = (0..reps)
                .map(|_| parent_run(&dir, variant, index, events))
                .collect();
            let m = Result {
                mean_ns: median(runs.iter().map(|r| r.mean_ns).collect()),
                worst_ns: median(runs.iter().map(|r| r.worst_ns as f64).collect()) as u64,
                allocs_per_event: median(runs.iter().map(|r| r.allocs_per_event).collect()),
                drain_ms: median(runs.iter().map(|r| r.drain_ms).collect()),
                dropped: runs.iter().map(|r| r.dropped).sum(),
                peak_entries: runs.iter().map(|r| r.peak_entries).max().unwrap_or(0),
            };
            println!(
                "{:<22} {:<6} {:>10.0} {:>12.1} {:>13.3} {:>10.1} {:>8} {:>11}",
                cell.name,
                variant.name(),
                m.mean_ns,
                m.worst_ns as f64 / 1000.0,
                m.allocs_per_event,
                m.drain_ms,
                m.dropped,
                m.peak_entries
            );
            rows.push(format!(
                "{{\"cell\":\"{}\",\"writer\":\"{}\",\"ns_per_event\":{:.0},\"worst_us\":{:.1},\"allocs_per_event\":{:.3},\"drain_ms\":{:.1},\"dropped\":{},\"peak_queue_entries\":{}}}",
                cell.name,
                variant.name(),
                m.mean_ns,
                m.worst_ns as f64 / 1000.0,
                m.allocs_per_event,
                m.drain_ms,
                m.dropped,
                m.peak_entries
            ));
            medians.push(m);
        }
        let (old, new) = (medians[0], medians[1]);
        if new.allocs_per_event > old.allocs_per_event + 0.05 {
            println!("  FAIL: the queue allocates more per event than the synchronous writer");
            ok = false;
        }
        if new.mean_ns > old.mean_ns * 1.10 {
            println!(
                "  FAIL: the queue costs more than 10% above the synchronous writer per event"
            );
            ok = false;
        }
        if new.dropped > 0 {
            println!("  FAIL: the queue dropped lines (the cell proves nothing)");
            ok = false;
        }
    }
    if let Ok(path) = std::env::var("LOG_BENCH_JSON") {
        std::fs::write(
            path,
            format!(
                "{{\"schema\":1,\"ok\":{ok},\"rows\":[{}]}}\n",
                rows.join(",")
            ),
        )
        .expect("write LOG_BENCH_JSON");
    }
    let _ = std::fs::remove_dir_all(&dir);
    if measuring && !ok {
        std::process::exit(1);
    }
}
