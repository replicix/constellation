//! Reproducer for a shutdown deadlock in fjall 3.1.10's
//! `impl Drop for DatabaseInner` (see `vendor/fjall/CONSTELLATION-PATCH.md`).
//!
//! Upstream's drop loop is
//!
//! ```text
//! while active_thread_counter > 0 {
//!     sender.send(WorkerMessage::Close);   // blocking, flume::bounded(1_000)
//!     sleep(10us);
//! }
//! ```
//!
//! and the pool keeps its own receiver, so the channel never disconnects.
//! While the workers are busy (a flush or compaction in flight), one `Close`
//! lands per loop turn and the channel fills up within ~1000 turns. From
//! then on the dropping thread sits in a blocking `send`. When the last
//! worker takes its `Close`, flume moves the dropping thread's pending
//! `Close` into the freed slot and wakes it, so the channel is full again.
//! The worker is now on its way out but still counted: it decrements
//! `active_thread_counter` only when its thread function returns. If the
//! dropping thread gets to its next `counter > 0` check first, it sends
//! into a full channel that nobody will ever receive from again, and
//! blocks forever.
//!
//! Naturally that window is a few instructions wide (the worker has to be
//! preempted in it), so this test widens it the way a slow log sink or a
//! descheduled thread would: a `log` logger that sleeps when fjall logs
//! `"Worker #N closes because DB is dropping"`, which is exactly the line
//! a worker runs between taking its `Close` and decrementing the counter.
//! Set `FJALL_DROP_WIDEN=0` to run without it (natural rate only).
//!
//! Every drop runs on its own thread under a timeout, so a hang fails the
//! test instead of wedging the test binary. `FJALL_DROP_ITERS` sets the
//! iteration count (default 12).

use fjall::{Database, KeyspaceCreateOptions};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Sleeps inside the one debug line a fjall worker logs between receiving
/// its `Close` and dropping out of `active_thread_counter`. Everything
/// else is ignored.
struct WidenWorkerExit;

impl log::Log for WidenWorkerExit {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() == log::Level::Debug && metadata.target().starts_with("fjall")
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata())
            && record
                .args()
                .as_str()
                .map_or_else(|| record.args().to_string(), str::to_string)
                .contains("closes because DB is dropping")
        {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn flush(&self) {}
}

static WIDEN: WidenWorkerExit = WidenWorkerExit;

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Cheap incompressible bytes, so flushes and compactions write real data.
struct XorShift(u64);

impl XorShift {
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            let bytes = self.0.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// Opens a database, writes enough into several small-memtable keyspaces
/// that flushes and compactions are in flight, then drops it on a helper
/// thread. Returns how long the drop took, or `None` if it did not finish
/// within `timeout`.
fn open_load_drop(seed: u64, timeout: Duration) -> Option<Duration> {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::builder(dir.path())
        .worker_threads(4)
        .open()
        .unwrap();

    let keyspaces: Vec<_> = (0..4)
        .map(|k| {
            db.keyspace(&format!("ks{k}"), || {
                KeyspaceCreateOptions::default().max_memtable_size(16 * 1024 * 1024)
            })
            .unwrap()
        })
        .collect();

    let mut rng = XorShift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut value = vec![0u8; 1024];
    for i in 0u64..48_000 {
        rng.fill(&mut value);
        let ks = &keyspaces[(i % 4) as usize];
        ks.insert(rng.0.to_be_bytes(), &value).unwrap();
    }
    // Seal every active memtable so each worker has a large flush (and the
    // compactions it triggers) in flight while the database is dropped.
    for ks in &keyspaces {
        ks.rotate_memtable().unwrap();
    }

    // Keyspace handles do not keep the database alive; drop them first so
    // the `Database` below is the last reference and its drop is the real
    // `DatabaseInner::drop`.
    drop(keyspaces);

    let (done_tx, done_rx) = mpsc::channel();
    let started = Instant::now();
    std::thread::Builder::new()
        .name("fjall-drop".into())
        .spawn(move || {
            drop(db);
            let _ = done_tx.send(());
        })
        .unwrap();

    match done_rx.recv_timeout(timeout) {
        Ok(()) => {
            // Only remove the directory once fjall has let go of it. On a
            // hang the dropping thread still uses it, so leak it instead.
            drop(dir);
            Some(started.elapsed())
        }
        Err(_) => {
            let _ = dir.keep();
            None
        }
    }
}

#[test]
fn dropping_a_busy_database_never_hangs() {
    if env_or("FJALL_DROP_WIDEN", 1u8) != 0 {
        log::set_logger(&WIDEN).expect("no other logger in this test binary");
        log::set_max_level(log::LevelFilter::Debug);
    }

    let iterations: u64 = env_or("FJALL_DROP_ITERS", 12);
    let timeout = Duration::from_secs(env_or("FJALL_DROP_TIMEOUT_SECS", 20));

    let mut slowest = Duration::ZERO;
    for i in 0..iterations {
        match open_load_drop(i, timeout) {
            Some(took) => slowest = slowest.max(took),
            None => panic!(
                "iteration {i}/{iterations}: dropping the fjall Database did not finish \
                 within {timeout:?} (DatabaseInner::drop deadlock)"
            ),
        }
    }
    eprintln!("{iterations} drops ok, slowest drop {slowest:?}");
}
