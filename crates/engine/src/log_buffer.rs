//! The daemon's log: the control ring and a non-blocking sink queue
//! (plan 44).
//!
//! Every `tracing` event of a daemon reaches [`LogWriter::write`] as one
//! whole line (the fmt layer formats an event into a buffer and makes one
//! `write_all`). The line goes to two places under **one mutex** and as
//! **one `Arc<str>`**:
//!
//! - the control ring (the last 2 000 lines, for `node.logs.tail` and the
//!   web UI), which keeps every line whether or not the sink got it, and
//! - the sink queue, drained by one writer thread (`cst-log-writer`) to the
//!   process's stderr (`daemon.log`, a harness `mount.log`, a container's
//!   pipe) in batches.
//!
//! No daemon thread ever waits for the sink: not the authority core in the
//! middle of a step, not a FUSE worker holding the store's writer lock, not
//! the holder's heartbeat thread. A sink that stalls (a buffered `write(2)`
//! to a busy btrfs can wait seconds; a pipe nobody reads waits forever)
//! stalls the writer thread alone, and the queue absorbs what the daemon
//! logs meanwhile.
//!
//! # Overflow: the newest line is dropped
//!
//! The queue is bounded in **bytes**: each line costs `len + 1 + 64` (the
//! `Arc` header, the deque slot and allocator rounding), from its admission
//! until the writer has written it (`queued_bytes + inflight_bytes`), so a
//! batch stuck in a stalled `write(2)` still counts and the memory bound
//! holds. An ordinary line is admitted while the queue stays within
//! `limit - limit/8`; a WARN or ERROR within `limit`: in a storm of
//! info/debug lines behind a stalled sink the last eighth stays free for
//! warnings and errors. Neither ever waits.
//!
//! A line that does not fit is **dropped**, and the dropped one is the
//! *incoming* one: that is O(1) and touches nothing the writer may be
//! holding; it keeps the lines that led into the stall (usually what
//! explains it); and it leaves one contiguous gap per episode, marked where
//! it happened. Dropping the oldest, or blocking for ERROR, was rejected: a
//! blocked error logged by the core during a disk stall is exactly what
//! cost a delegation.
//!
//! An **episode** opens at the first drop and closes when the queue is back
//! under `limit/2`. While it is open ordinary lines are dropped too (a
//! WARN or ERROR that fits is still admitted), so the gap stays one
//! contiguous run and the summary lands exactly at it; the writer drains a
//! stall's backlog in 256 KiB writes, so the window between three-quarters
//! full and half full is short. Closing enqueues, ahead of whatever comes
//! next, **one summary line** (`log lines dropped ... dropped_lines=N`)
//! formatted here in the fmt layer's default layout. It is not a `tracing`
//! event: it bypasses `RUST_LOG` (a drop must be visible whatever the
//! filter) and cannot recurse into the writer. The check runs in the caller
//! that admits a line and in the writer after each batch, so a storm that
//! ends and a log that goes quiet both close it.
//!
//! # Lifecycle
//!
//! A [`LogBuffer`] starts in **sync** mode, exactly the writer this module
//! replaced: the calling thread writes the bytes to stderr. Every
//! short-lived CLI command stays there (its stderr is a terminal or a
//! script's pipe, and its `eprintln!` output stays in order with its log
//! lines). The daemons call [`LogBuffer::start`] **after** the daemonizer's
//! fork: the fork copies only the calling thread, so a writer thread (or a
//! queued line) that existed before it would be dead in the child, or
//! written twice. [`LogBuffer::flush`] is the bounded barrier the exit, the
//! handover's `exec` and the abort path use; [`install_panic_hook`] routes a
//! panic's message through the same queue.
//!
//! `kill -9`, `SIGSEGV` and a double-panic abort lose what is queued: on a
//! sink that keeps up, the lines of the last few microseconds; behind a
//! stalled sink, the stall's backlog.
//!
//! The control audit trail is a separate file written synchronously and
//! never drops; the `SIGUSR2` backtrace handler writes from signal context
//! (no mutex, no queue). Neither is part of this module.
//!
//! # Fault point
//!
//! `CONSTELLATION_FAULT_LOG_STALL_FILE=<path>` (load campaign): while that
//! file exists, each sink write first sleeps for the milliseconds it holds
//! **holding std's stderr lock**, then writes. `<ms>[ once][ pid=<n>]`, the
//! store-stall grammar: `once` removes the file at the first write that sees
//! it, `pid=<n>` stalls only the daemon of that pid. Write the file whole
//! (a temporary name, then a rename). On the writer thread in queue mode, on
//! the logging thread in sync mode, where every other thread that logs then
//! queues on the lock exactly as behind a stalled `write(2)` (the fail-before
//! arm of plan 44). It fires once per `write()` call of the sink, so a retry
//! after `WouldBlock` or a partial write stalls again unless the file says
//! `once`. Never logs. Unset, one cached lookup.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Once, OnceLock, PoisonError};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::format::Writer as FmtWriter;
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::fmt::MakeWriter;

/// The control ring's size, in lines.
const RING_CAPACITY: usize = 2_000;
/// The default sink queue bound ([`QueueConfig::from_env`]).
pub const DEFAULT_QUEUE_BYTES: usize = 16 << 20;
/// A configured bound below this is raised to it.
pub const MIN_QUEUE_BYTES: usize = 64 << 10;
/// What a queued line costs beyond its bytes: the `Arc` header, the deque
/// slot, allocator rounding.
const LINE_OVERHEAD: usize = 64;
/// One `write(2)` of the writer thread carries up to this many bytes.
const WRITE_CHUNK: usize = 256 << 10;
/// The queue is a list of blocks of this many entries: a push allocates at
/// most one block (16 KiB), never grows or copies a big array under the lock,
/// and a block is freed as soon as the writer has consumed it, so what a
/// stall's backlog took is returned at once.
const BLOCK_ENTRIES: usize = 1024;
/// The writer's byte buffer's idle capacity.
const IDLE_BUF_BYTES: usize = 64 << 10;
/// A batch that grew the writer's byte buffer past this shrinks it back.
const SHRINK_BUF_BYTES: usize = 1 << 20;
/// How long the writer polls a sink that answers `WouldBlock` (a descriptor
/// somebody made non-blocking) before it gives the chunk up.
const WOULD_BLOCK_BOUND: Duration = Duration::from_secs(10);
/// [`LogBuffer::viewer_flush`] calls that may wait at once.
const MAX_VIEWER_FLUSHES: usize = 2;
/// Panic records (one per `push_panic`) that bypass the queue's bound; later
/// ones are admitted like WARN and ERROR lines.
const MAX_PANIC_RECORDS: u64 = 64;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while logging must not turn every later log call into a panic.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How the sink queue is sized ([`LogBuffer::start`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueConfig {
    /// The queue's bound in bytes; `0`: no queue, the synchronous writer.
    pub limit: usize,
    /// What [`QueueConfig::from_env`] could not use (to be logged as a WARN
    /// once the writer is up: the writer cannot log before it exists).
    pub warning: Option<String>,
}

impl QueueConfig {
    /// `CONSTELLATION_LOG_QUEUE_BYTES`: the queue's bound in bytes, read once
    /// at start. Default 16 MiB: at the info level a busy node writes a few
    /// KB/s (18 KB/s with every target at debug), so it holds minutes of a
    /// stall at normal rates and seconds of a 1 MB/s storm; memory is taken
    /// only while lines wait. Values below 64 KiB are raised to 64 KiB, an
    /// unparsable value keeps the default (with a warning). **`0` keeps the
    /// synchronous writer** (a slow disk stalls the daemon): for A/B
    /// measurements and fail-before arms only.
    pub fn from_env() -> Self {
        Self::from_value(
            std::env::var("CONSTELLATION_LOG_QUEUE_BYTES")
                .ok()
                .as_deref(),
        )
    }

    pub fn from_value(value: Option<&str>) -> Self {
        let Some(text) = value.map(str::trim).filter(|t| !t.is_empty()) else {
            return Self::with_limit(DEFAULT_QUEUE_BYTES);
        };
        match text.parse::<usize>() {
            Ok(0) => Self::sync(),
            Ok(n) => Self::with_limit(n.max(MIN_QUEUE_BYTES)),
            Err(_) => Self {
                limit: DEFAULT_QUEUE_BYTES,
                warning: Some(format!(
                    "CONSTELLATION_LOG_QUEUE_BYTES={text:?} is not a byte count; \
                     keeping the default of {DEFAULT_QUEUE_BYTES}"
                )),
            },
        }
    }

    /// No queue: every write goes to the sink on the calling thread.
    pub fn sync() -> Self {
        Self::with_limit(0)
    }

    pub fn with_limit(limit: usize) -> Self {
        Self {
            limit,
            warning: None,
        }
    }
}

/// The result of [`LogBuffer::flush`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flush {
    /// Everything accepted before the call has been written.
    pub complete: bool,
    /// Accepted lines not written when the call returned.
    pub unwritten_lines: u64,
}

/// The log an image had not written when it `exec`ed, handed to the next
/// image (plan 44 W6): `lines` in order, the first `in_doubt` of them the
/// chunk that was being written, the old image's drop counters (its
/// accounting continues), and `lost`, the lines that did not fit the memfd's
/// bound.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExecTail {
    pub lines: Vec<Arc<str>>,
    pub in_doubt: usize,
    pub lost: u64,
    pub dropped_lines: u64,
    pub dropped_bytes: u64,
    pub drop_episodes: u64,
    pub sink_errors: u64,
}

const TAIL_MAGIC: &[u8; 8] = b"CSTLOGT1";
/// The size of the header [`ExecTail::encode_header`] writes.
pub const TAIL_HEADER_LEN: usize = 8 + 7 * 8;

impl ExecTail {
    /// The bytes of the memfd: the header ([`ExecTail::encode_header`]),
    /// then each line as a `u32` length and its bytes. No allocation beyond
    /// the one buffer, whose size is at most the old image's queue bound plus
    /// 4 bytes a line.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(TAIL_HEADER_LEN + self.lines_len());
        out.extend_from_slice(&self.encode_header(self.lines.len()));
        self.encode_lines_into(&mut out);
        out
    }

    /// The fixed-size header at the memfd's start: the magic, seven
    /// little-endian `u64`s (`in_doubt`, `lost`, the four counters, the line
    /// count `count`). A tail that grew after it was written is brought up to
    /// date by appending lines ([`ExecTail::encode_lines`]) and rewriting this.
    pub fn encode_header(&self, count: usize) -> [u8; TAIL_HEADER_LEN] {
        let mut out = [0u8; TAIL_HEADER_LEN];
        out[..8].copy_from_slice(TAIL_MAGIC);
        for (i, n) in [
            self.in_doubt as u64,
            self.lost,
            self.dropped_lines,
            self.dropped_bytes,
            self.drop_episodes,
            self.sink_errors,
            count as u64,
        ]
        .into_iter()
        .enumerate()
        {
            out[8 + 8 * i..16 + 8 * i].copy_from_slice(&n.to_le_bytes());
        }
        out
    }

    /// The lines alone, each as a `u32` length and its bytes (what follows
    /// the header).
    pub fn encode_lines(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.lines_len());
        self.encode_lines_into(&mut out);
        out
    }

    fn lines_len(&self) -> usize {
        self.lines.iter().map(|l| l.len() + 4).sum()
    }

    fn encode_lines_into(&self, out: &mut Vec<u8>) {
        for line in &self.lines {
            out.extend_from_slice(&(line.len() as u32).to_le_bytes());
            out.extend_from_slice(line.as_bytes());
        }
    }

    /// How many lines a memfd's bytes say they hold or lost, when the header
    /// parses: what the new image counts as lost when it cannot read the rest.
    pub fn lines_in_header(bytes: &[u8]) -> Option<u64> {
        let rest = bytes.strip_prefix(TAIL_MAGIC)?;
        let field = |i: usize| -> Option<u64> {
            let chunk = rest.get(8 * i..8 * i + 8)?;
            Some(u64::from_le_bytes(chunk.try_into().ok()?))
        };
        field(1)?.checked_add(field(6)?)
    }

    /// [`ExecTail::encode`]'s inverse; `None` for anything that is not
    /// exactly what it writes (the caller then counts the loss it was told
    /// of, if any).
    pub fn decode(bytes: &[u8]) -> Option<ExecTail> {
        let rest = bytes.strip_prefix(TAIL_MAGIC)?;
        let mut fields = [0u64; 7];
        let mut rest = rest;
        for field in &mut fields {
            let (head, tail) = rest.split_first_chunk::<8>()?;
            *field = u64::from_le_bytes(*head);
            rest = tail;
        }
        let [in_doubt, lost, dropped_lines, dropped_bytes, drop_episodes, sink_errors, count] =
            fields;
        let mut lines = Vec::with_capacity((count as usize).min(rest.len() / 4));
        for _ in 0..count {
            let (len, tail) = rest.split_first_chunk::<4>()?;
            let len = u32::from_le_bytes(*len) as usize;
            if tail.len() < len {
                return None;
            }
            let (text, tail) = tail.split_at(len);
            lines.push(Arc::from(std::str::from_utf8(text).ok()?));
            rest = tail;
        }
        rest.is_empty().then_some(ExecTail {
            lines,
            in_doubt: (in_doubt as usize).min(count as usize),
            lost,
            dropped_lines,
            dropped_bytes,
            drop_episodes,
            sink_errors,
        })
    }
}

/// What `constellation status` reports about the writer (plan 44 §4.3).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogStats {
    /// A sink queue and writer thread exist (false: the synchronous writer).
    pub queued_mode: bool,
    pub limit_bytes: u64,
    /// Bytes waiting or in a write (the bound's measure, overhead included).
    pub queued_bytes: u64,
    pub queued_lines: u64,
    /// The high-water mark of `queued_bytes`.
    pub queued_bytes_max: u64,
    /// The high-water mark of the waiting queue's length, in entries.
    pub queue_entries_max: u64,
    pub accepted_lines: u64,
    /// Lines written to the sink (not those whose write failed).
    pub written_lines: u64,
    /// Lines dropped: the queue was full, a sink write failed, or the
    /// previous image's `exec` took them.
    pub dropped_lines: u64,
    pub dropped_bytes: u64,
    pub drop_episodes: u64,
    /// The longest single sink write: a slow disk shows here first.
    pub longest_write_ms: u64,
    /// How long the write in progress has been running (0: none).
    pub writing_for_ms: u64,
    pub sink_errors: u64,
    /// Lines the previous image's writer had not written at its `exec` and
    /// handed to this one through a memfd (written first, in order).
    pub carried_at_previous_exec: u64,
    /// Lines the previous image could not hand over (no memfd, or more than
    /// its bound): counted as dropped.
    pub lost_at_previous_exec: u64,
    /// The writer's byte buffer (bytes) and the queue's blocks, for the
    /// idle-capacity test.
    pub writer_buffer_bytes: u64,
    pub queue_blocks: u64,
    /// Blocks the queue has allocated over its life (it reuses emptied ones).
    pub blocks_allocated: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// The caller writes to the sink (today's behaviour).
    Sync,
    /// A writer thread drains the queue; `limit` bounds it in bytes.
    Queue { limit: usize },
}

/// Why lines were dropped (the summary line's words).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Cause {
    /// The queue was full: the sink did not keep up.
    Full,
    /// A write to the sink failed (`ENOSPC`, `EIO`, a closed pipe).
    SinkFailed,
    /// The previous image's `exec` came before its writer was done.
    Exec,
}

impl Cause {
    fn words(self) -> &'static str {
        match self {
            Cause::Full => "the log sink did not keep up",
            Cause::SinkFailed => "writes to the log sink failed",
            Cause::Exec => "the previous image exec'd before its log writer was done",
        }
    }
}

/// Drops since the last summary line.
struct Episode {
    first_unix_ms: u64,
    lines: u64,
    bytes: u64,
    cause: Cause,
}

/// How many emptied blocks the queue keeps for reuse (64 KiB): without them
/// a log whose writer keeps up (every batch a line or two) would allocate a
/// block per batch, an allocation per line more than the writer it replaces.
const SPARE_BLOCKS: usize = 4;

/// The sink queue: a list of fixed-size blocks (see [`BLOCK_ENTRIES`]).
#[derive(Default)]
struct Blocks {
    blocks: VecDeque<VecDeque<Arc<str>>>,
    len: usize,
    /// Emptied blocks, kept (up to [`SPARE_BLOCKS`]) for the next push.
    spare: Vec<VecDeque<Arc<str>>>,
    /// Blocks allocated over the queue's life (the reuse test reads it).
    allocated: u64,
}

impl Blocks {
    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push_back(&mut self, line: Arc<str>) {
        match self.blocks.back_mut() {
            Some(block) if block.len() < BLOCK_ENTRIES => block.push_back(line),
            _ => {
                let mut block = self.spare.pop().unwrap_or_else(|| {
                    self.allocated += 1;
                    VecDeque::with_capacity(BLOCK_ENTRIES)
                });
                block.push_back(line);
                self.blocks.push_back(block);
            }
        }
        self.len += 1;
    }

    #[cfg(test)]
    fn front(&self) -> Option<&Arc<str>> {
        self.blocks.front().and_then(VecDeque::front)
    }

    /// The oldest line. Its block, when that empties, goes to `emptied` (up
    /// to [`SPARE_BLOCKS`] of them: the writer hands them back with
    /// [`Blocks::adopt_spares`]) or is freed.
    fn pop_front(&mut self, emptied: &mut Vec<VecDeque<Arc<str>>>) -> Option<Arc<str>> {
        let block = self.blocks.front_mut()?;
        let line = block.pop_front();
        if block.is_empty() {
            if let Some(block) = self.blocks.pop_front() {
                if emptied.len() < SPARE_BLOCKS {
                    emptied.push(block);
                }
            }
        }
        if line.is_some() {
            self.len -= 1;
        }
        line
    }

    /// The newest `n` lines (all of them when fewer), oldest first.
    fn last_n(&self, n: usize) -> Vec<Arc<str>> {
        let mut out: Vec<Arc<str>> = self
            .blocks
            .iter()
            .rev()
            .flat_map(|b| b.iter().rev())
            .take(n)
            .cloned()
            .collect();
        out.reverse();
        out
    }

    /// Every queued line, oldest first.
    fn iter(&self) -> impl Iterator<Item = &Arc<str>> {
        self.blocks.iter().flat_map(VecDeque::iter)
    }

    /// Move everything queued into `batch`, which must be empty (O(1)): the
    /// two lists of blocks trade places, so neither is ever reallocated, and
    /// `self` keeps its spare blocks.
    fn swap_out(&mut self, batch: &mut Blocks) {
        debug_assert!(batch.is_empty());
        std::mem::swap(&mut self.blocks, &mut batch.blocks);
        batch.len = std::mem::take(&mut self.len);
    }

    /// Take back the blocks a consumed batch emptied.
    fn adopt_spares(&mut self, emptied: &mut Vec<VecDeque<Arc<str>>>) {
        while self.spare.len() < SPARE_BLOCKS {
            let Some(block) = emptied.pop() else {
                break;
            };
            self.spare.push(block);
        }
        emptied.clear();
    }
}

struct State {
    // The control ring.
    lines: VecDeque<Arc<str>>,
    partial: Vec<u8>,
    /// Lines ever completed (the position `since` counts from).
    total: u64,
    // The sink queue.
    mode: Mode,
    queue: Blocks,
    /// What the writer took out of `queue` and has not finished: the batch
    /// not yet in a chunk, and the chunk in the write now (`in_write`), kept
    /// where an `exec` can read them (see [`LogBuffer::freeze_for_exec`]).
    batch: Blocks,
    in_write: Vec<Arc<str>>,
    /// The writer takes nothing more (an `exec` is about to carry what is
    /// left to the next image).
    frozen: bool,
    queued_bytes: usize,
    inflight_bytes: usize,
    writer_idle: bool,
    /// Lines admitted to the queue, and lines the writer is done with (the
    /// flush barrier's coordinates; summary and panic lines count).
    accepted: u64,
    written: u64,
    episode: Option<Episode>,
    /// The last write to the sink failed: no summary line is queued (it
    /// would meet the same failure, and each one would open the next episode)
    /// until a write succeeds.
    sink_failing: bool,
    /// Lines the writer is done with whose write failed (in `written`, the
    /// barrier's coordinate, but not written).
    failed: u64,
    panic_records: u64,
    writer: Option<ThreadId>,
    // Counters.
    queued_bytes_max: usize,
    queue_entries_max: usize,
    dropped_lines: u64,
    dropped_bytes: u64,
    drop_episodes: u64,
    sink_errors: u64,
    carried_at_previous_exec: u64,
    lost_at_previous_exec: u64,
    /// Where the queue stood when an `exec`'s freeze copied it (see
    /// [`LogBuffer::exec_tail_update`]).
    exec_mark: Option<ExecMark>,
}

/// What [`LogBuffer::freeze_for_exec`] left for [`LogBuffer::exec_tail_update`]:
/// the lines accepted by then, the bytes the tail holds and its bound.
#[derive(Clone, Copy)]
struct ExecMark {
    accepted: u64,
    bytes: usize,
    limit: usize,
}

impl State {
    fn new() -> Self {
        State {
            lines: VecDeque::new(),
            partial: Vec::new(),
            total: 0,
            mode: Mode::Sync,
            queue: Blocks::default(),
            batch: Blocks::default(),
            in_write: Vec::new(),
            frozen: false,
            queued_bytes: 0,
            inflight_bytes: 0,
            writer_idle: false,
            accepted: 0,
            written: 0,
            episode: None,
            sink_failing: false,
            failed: 0,
            panic_records: 0,
            writer: None,
            queued_bytes_max: 0,
            queue_entries_max: 0,
            dropped_lines: 0,
            dropped_bytes: 0,
            drop_episodes: 0,
            sink_errors: 0,
            carried_at_previous_exec: 0,
            lost_at_previous_exec: 0,
            exec_mark: None,
        }
    }

    fn used(&self) -> usize {
        self.queued_bytes + self.inflight_bytes
    }

    fn push_ring(&mut self, line: Arc<str>) {
        self.total += 1;
        self.lines.push_back(line);
        if self.lines.len() > RING_CAPACITY {
            self.lines.pop_front();
        }
    }

    /// Put `line` in the queue past every bound check; true when the writer
    /// must be woken.
    fn enqueue(&mut self, line: Arc<str>) -> bool {
        self.queued_bytes += line.len() + 1 + LINE_OVERHEAD;
        self.queued_bytes_max = self.queued_bytes_max.max(self.used());
        self.queue.push_back(line);
        self.queue_entries_max = self.queue_entries_max.max(self.queue.len());
        self.accepted += 1;
        std::mem::take(&mut self.writer_idle)
    }

    /// Admit a completed line to the queue (queue mode); true when the
    /// writer must be woken.
    fn admit(&mut self, line: Arc<str>, reserved: bool) -> bool {
        let Mode::Queue { limit } = self.mode else {
            return false;
        };
        let mut wake = false;
        if self.episode.is_some() && !self.sink_failing && self.used() < limit / 2 {
            wake |= self.close_episode(false);
        }
        let cost = line.len() + 1 + LINE_OVERHEAD;
        let cap = if reserved { limit } else { limit - limit / 8 };
        // An open episode drops ordinary lines too, so the gap is one run
        // and the summary lands exactly at it.
        // (An episode of failed sink writes does not block: its gap is
        // what the failed chunks held, and what is admitted now is the
        // writer's chance to find the sink back.)
        // (Nor does one whose sink failed after the queue filled: nothing
        // would be written, so nothing would find the sink back.)
        let blocked = !reserved
            && !self.sink_failing
            && self
                .episode
                .as_ref()
                .is_some_and(|e| e.cause == Cause::Full);
        if blocked || self.used() + cost > cap {
            self.record_loss(1, line.len() as u64 + 1, Cause::Full);
            return wake;
        }
        wake | self.enqueue(line)
    }

    /// Count `lines` (`bytes` of them) as dropped, in the open episode (one
    /// is opened if none is).
    fn record_loss(&mut self, lines: u64, bytes: u64, cause: Cause) {
        self.dropped_lines += lines;
        self.dropped_bytes += bytes;
        let opened = self.episode.is_none();
        let episode = self.episode.get_or_insert_with(|| Episode {
            first_unix_ms: unix_ms(),
            lines: 0,
            bytes: 0,
            cause,
        });
        episode.lines += lines;
        episode.bytes += bytes;
        if cause == Cause::SinkFailed {
            // A sink that failed after a full queue: the episode is the
            // sink's now (it no longer holds ordinary lines back).
            episode.cause = cause;
        }
        if opened {
            self.drop_episodes += 1;
        }
    }

    /// Enqueue the summary line of the open episode, ahead of whatever
    /// comes next; true when the writer must be woken. Not while the sink
    /// is failing, and unless `force`d (the final flushes of an exit, an
    /// `exec` and an abort) only when the line fits in what the bound leaves:
    /// a viewer's `flush` must not grow the queue past it.
    fn close_episode(&mut self, force: bool) -> bool {
        let Some(episode) = &self.episode else {
            return false;
        };
        if !force && self.sink_failing {
            // Not even built: it would be thrown away at every admission.
            return false;
        }
        let limit = match self.mode {
            Mode::Queue { limit } => limit,
            Mode::Sync => 0,
        };
        let line: Arc<str> = summary_line(episode, limit).into();
        if !force && self.used() + line.len() + 1 + LINE_OVERHEAD > limit.max(1) {
            return false;
        }
        self.episode = None;
        self.push_ring(line.clone());
        self.enqueue(line)
    }

    /// Admit the lines of a panic message: the first [`MAX_PANIC_RECORDS`]
    /// messages of the process past the bound (the message that says why
    /// the daemon died must not meet a full queue), later ones like WARN
    /// and ERROR lines (the reserve; dropped and counted in an episode when
    /// that is full too).
    fn push_panic(&mut self, text: &str) -> bool {
        let mut wake = false;
        let over = self.panic_records >= MAX_PANIC_RECORDS;
        self.panic_records += 1;
        for line in text.lines() {
            let line: Arc<str> = line.into();
            self.push_ring(line.clone());
            if over {
                wake |= self.admit(line, true);
            } else {
                wake |= self.enqueue(line);
            }
        }
        wake
    }
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The fmt layer's default layout, its timestamp from the same formatter,
/// no ANSI.
fn summary_line(episode: &Episode, limit: usize) -> String {
    let mut stamp = String::new();
    let _ = SystemTime.format_time(&mut FmtWriter::new(&mut stamp));
    format!(
        "{stamp}  WARN constellation::log: log lines dropped: {} \
         dropped_lines={} dropped_bytes={} first_dropped_unix_ms={} queue_limit_bytes={limit}",
        episode.cause.words(),
        episode.lines,
        episode.bytes,
        episode.first_unix_ms
    )
}

struct Shared {
    state: Mutex<State>,
    /// The queue got a line (the writer waits here while it is empty).
    work: Condvar,
    /// The writer finished a chunk (flush waits here).
    written: Condvar,
    origin: Instant,
    /// `ms since origin + 1` at the start of the sink write in progress;
    /// 0 when none. Atomics, so status never waits behind the writer.
    write_started: AtomicU64,
    longest_write_ms: AtomicU64,
    writer_buffer_bytes: AtomicU64,
    /// Sync mode writes here instead of stderr (the naive arms of the tests).
    sync_sink: Option<Mutex<Box<dyn Write + Send>>>,
    /// [`LogBuffer::viewer_flush`] calls waiting now.
    viewer_flushes: AtomicUsize,
}

impl Shared {
    fn new(sync_sink: Option<Box<dyn Write + Send>>) -> Self {
        Shared {
            state: Mutex::new(State::new()),
            work: Condvar::new(),
            written: Condvar::new(),
            origin: Instant::now(),
            write_started: AtomicU64::new(0),
            longest_write_ms: AtomicU64::new(0),
            writer_buffer_bytes: AtomicU64::new(0),
            sync_sink: sync_sink.map(Mutex::new),
            viewer_flushes: AtomicUsize::new(0),
        }
    }

    fn now_ms(&self) -> u64 {
        self.origin.elapsed().as_millis() as u64
    }

    /// Sync mode's write: on the calling thread, to the test sink or stderr.
    fn sync_write(&self, bytes: &[u8]) {
        match &self.sync_sink {
            Some(sink) => {
                let _ = lock(sink).write_all(bytes);
            }
            None => {
                let _ = StderrSink.write_all(bytes);
            }
        }
    }

    fn sync_flush(&self) {
        match &self.sync_sink {
            Some(sink) => {
                let _ = lock(sink).flush();
            }
            None => {
                let _ = std::io::stderr().flush();
            }
        }
    }
}

/// The daemon's log: control ring and sink queue. `Clone` shares them.
#[derive(Clone)]
pub struct LogBuffer(Arc<Shared>);

impl Default for LogBuffer {
    fn default() -> Self {
        LogBuffer(Arc::new(Shared::new(None)))
    }
}

impl LogBuffer {
    /// A buffer whose synchronous mode writes to `sink` instead of stderr,
    /// serialized by a mutex like std's stderr lock (the naive arms of the
    /// tests).
    #[cfg(test)]
    pub(crate) fn with_sync_sink_for_test(sink: Box<dyn Write + Send>) -> Self {
        LogBuffer(Arc::new(Shared::new(Some(sink))))
    }

    /// A queue-mode buffer with no writer thread: lines wait in the queue
    /// until the test removes them with [`LogBuffer::drain_for_test`].
    #[cfg(test)]
    pub(crate) fn queue_only_for_test(limit: usize) -> Self {
        let buf = Self::default();
        lock(&buf.0.state).mode = Mode::Queue { limit };
        buf
    }

    /// Remove up to `bytes` of queued cost, oldest first (the writer's
    /// effect, without a writer).
    #[cfg(test)]
    pub(crate) fn drain_for_test(&self, bytes: usize) {
        let mut st = lock(&self.0.state);
        let mut left = bytes;
        while let Some(line) = st.queue.front() {
            let cost = line.len() + 1 + LINE_OVERHEAD;
            if cost > left {
                break;
            }
            left -= cost;
            st.queue.pop_front(&mut Vec::new());
            st.queued_bytes -= cost;
            st.written += 1;
        }
    }

    /// Panic while holding the state lock (poisons it).
    #[cfg(test)]
    pub(crate) fn poison_for_test(&self) {
        let shared = self.0.clone();
        let _ = std::thread::spawn(move || {
            let _guard = shared.state.lock().unwrap();
            panic!("poisoning the log state on purpose");
        })
        .join();
    }

    pub fn tail(&self, count: usize) -> Vec<String> {
        let state = lock(&self.0.state);
        state
            .lines
            .iter()
            .rev()
            .take(count)
            .rev()
            .map(|l| l.to_string())
            .collect()
    }

    /// The last `count` lines and the position after them, for a follower
    /// (`node.logs.tail` with `follow`) to pass to [`LogBuffer::since`].
    pub fn tail_at(&self, count: usize) -> (u64, Vec<String>) {
        let state = lock(&self.0.state);
        let lines = state
            .lines
            .iter()
            .rev()
            .take(count)
            .rev()
            .map(|l| l.to_string())
            .collect();
        (state.total, lines)
    }

    /// The lines completed after position `seen` (as many as the ring still
    /// holds), and the new position.
    pub fn since(&self, seen: u64) -> (u64, Vec<String>) {
        let state = lock(&self.0.state);
        let new = state
            .total
            .saturating_sub(seen)
            .min(state.lines.len() as u64) as usize;
        let lines = state
            .lines
            .iter()
            .skip(state.lines.len() - new)
            .map(|l| l.to_string())
            .collect();
        (state.total, lines)
    }

    /// A writer for a line of `level`: WARN and ERROR keep the queue's
    /// reserved eighth.
    pub fn writer_for_level(&self, level: &tracing::Level) -> LogWriter {
        LogWriter {
            buffer: self.clone(),
            reserved: *level <= tracing::Level::WARN,
        }
    }

    /// Start the writer thread: from now on `write` queues and returns.
    /// Idempotent; a `limit` of 0 (see [`QueueConfig::sync`]) starts
    /// nothing. Call it after the daemonizer's fork, never before (see the
    /// module doc).
    pub fn start(&self, config: QueueConfig, sink: Box<dyn Write + Send>) -> std::io::Result<()> {
        if config.limit == 0 {
            return Ok(());
        }
        let mut st = lock(&self.0.state);
        if matches!(st.mode, Mode::Queue { .. }) {
            return Ok(());
        }
        let shared = self.0.clone();
        let handle = std::thread::Builder::new()
            .name("cst-log-writer".into())
            .spawn(move || writer_loop(shared, sink))?;
        st.writer = Some(handle.thread().id());
        st.mode = Mode::Queue {
            limit: config.limit,
        };
        Ok(())
    }

    /// The previous image's writer had `lines` unwritten when it gave up at
    /// its `exec`: they are lines this daemon lost, so they are counted as
    /// dropped (`status`, `/metrics`) and a `log lines dropped` summary line
    /// says so at once, where the log checks and an operator look. `lines` is
    /// what the old image counted when it stopped waiting: at least the lines
    /// it had not written then, short of whatever it logged in the moments
    /// before the `exec`, and over by what its last chunk wrote meanwhile.
    pub fn note_exec_loss(&self, lines: u64) {
        let mut st = lock(&self.0.state);
        st.lost_at_previous_exec = lines;
        if lines == 0 {
            return;
        }
        st.record_loss(lines, 0, Cause::Exec);
        if st.close_episode(true) {
            self.0.work.notify_one();
        }
    }

    /// Stop the writer taking anything more and copy out what it has not
    /// finished writing, oldest first: the chunk in its write now (in doubt:
    /// the write may complete before the `exec`), the rest of its batch and
    /// the queue, up to the queue's byte bound (what does not fit is counted
    /// in `lost`). For an `exec` that hands the log to the next image
    /// ([`ExecTail`]); the lines stay here, so [`LogBuffer::unfreeze`] undoes
    /// it when the `exec` fails. Sync mode has nothing to carry.
    pub fn freeze_for_exec(&self) -> ExecTail {
        let mut st = lock(&self.0.state);
        let limit = match st.mode {
            Mode::Queue { limit } => limit,
            Mode::Sync => 0,
        };
        st.frozen = true;
        let mut tail = ExecTail {
            in_doubt: st.in_write.len(),
            dropped_lines: st.dropped_lines,
            dropped_bytes: st.dropped_bytes,
            drop_episodes: st.drop_episodes,
            sink_errors: st.sink_errors,
            ..ExecTail::default()
        };
        let mut bytes = 0usize;
        let mut full = false;
        for line in st
            .in_write
            .iter()
            .chain(st.batch.iter())
            .chain(st.queue.iter())
        {
            bytes += line.len() + 1;
            full |= bytes > limit;
            if full {
                tail.lost += 1;
            } else {
                tail.lines.push(line.clone());
            }
        }
        tail.in_doubt = tail.in_doubt.min(tail.lines.len());
        st.exec_mark = Some(ExecMark {
            accepted: st.accepted,
            bytes: bytes.min(limit),
            limit,
        });
        tail
    }

    /// What the log took in since [`LogBuffer::freeze_for_exec`] copied it,
    /// for the last moment before the `exec` (other threads still log): a
    /// tail with the lines accepted since (those that fit the tail's bound),
    /// `lost` raised by those that do not, and the counters as they stand
    /// now. `sent` is what the freeze returned. The caller appends the lines
    /// to what it handed over and rewrites the header
    /// ([`ExecTail::encode_header`]). The writer is frozen, so the queue only
    /// grew at its end and the newest `accepted - mark` lines are the new
    /// ones; nothing is lost silently but what is logged after this call.
    pub fn exec_tail_update(&self, sent: &ExecTail) -> ExecTail {
        let st = lock(&self.0.state);
        let mut update = ExecTail {
            in_doubt: sent.in_doubt,
            lost: sent.lost,
            dropped_lines: st.dropped_lines,
            dropped_bytes: st.dropped_bytes,
            drop_episodes: st.drop_episodes,
            sink_errors: st.sink_errors,
            ..ExecTail::default()
        };
        let Some(mark) = st.exec_mark else {
            return update;
        };
        let late = st.accepted.saturating_sub(mark.accepted) as usize;
        if late == 0 {
            return update;
        }
        let mut bytes = mark.bytes;
        let mut full = false;
        for line in st.queue.last_n(late) {
            bytes += line.len() + 1;
            full |= bytes > mark.limit;
            if full {
                update.lost += 1;
            } else {
                update.lines.push(line);
            }
        }
        update
    }

    /// The writer takes lines again ([`LogBuffer::freeze_for_exec`], when the
    /// `exec` did not happen).
    pub fn unfreeze(&self) {
        lock(&self.0.state).frozen = false;
        self.0.work.notify_one();
    }

    /// The new image's side of an `exec` that carried the old one's unwritten
    /// log: the lines go to the sink first, in order, ahead of anything this
    /// image logs (call it right after [`LogBuffer::start`], before the first
    /// line), the ring gets them too, and the old image's counters
    /// continue here. A line the old image could not fit is counted as
    /// dropped (the loss and its summary line, as for a
    /// [`LogBuffer::note_exec_loss`]). If the old image's last write was in
    /// flight at the `exec`, its lines may be written twice: a line before
    /// them says so.
    pub fn adopt_exec_tail(&self, tail: ExecTail) {
        let mut st = lock(&self.0.state);
        st.dropped_lines += tail.dropped_lines;
        st.dropped_bytes += tail.dropped_bytes;
        st.drop_episodes += tail.drop_episodes;
        st.sink_errors += tail.sink_errors;
        st.carried_at_previous_exec = tail.lines.len() as u64;
        st.lost_at_previous_exec = tail.lost;
        let mut text = String::new();
        let stamp = |text: &mut String| {
            let mut stamp = String::new();
            let _ = SystemTime.format_time(&mut FmtWriter::new(&mut stamp));
            text.push_str(&stamp);
        };
        let mut lines: Vec<Arc<str>> = Vec::with_capacity(tail.lines.len() + 2);
        if tail.in_doubt > 0 {
            stamp(&mut text);
            text.push_str(&format!(
                "  WARN constellation::log: the previous image's last write was in flight at its \
                 exec: its {} line(s) below may repeat lines it wrote",
                tail.in_doubt
            ));
            lines.push(std::mem::take(&mut text).into());
        }
        lines.extend(tail.lines.iter().cloned());
        if !tail.lines.is_empty() {
            stamp(&mut text);
            text.push_str(&format!(
                "  INFO constellation::log: the {} line(s) above were unwritten at the previous \
                 image's exec and carried over by it",
                tail.lines.len()
            ));
            lines.push(std::mem::take(&mut text).into());
        }
        let sync = st.mode == Mode::Sync;
        let mut wake = false;
        for line in &lines {
            st.push_ring(line.clone());
            if !sync {
                // Past the bound, like a panic's message: it is bounded by
                // the old image's own bound.
                wake |= st.enqueue(line.clone());
            }
        }
        if tail.lost > 0 {
            st.record_loss(tail.lost, 0, Cause::Exec);
            wake |= st.close_episode(true);
        }
        drop(st);
        if wake {
            self.0.work.notify_one();
        }
        if sync {
            let mut bytes = Vec::new();
            for line in &lines {
                bytes.extend_from_slice(line.as_bytes());
                bytes.push(b'\n');
            }
            self.0.sync_write(&bytes);
        }
    }

    /// The queue exists (a writer thread was started).
    pub fn is_queued(&self) -> bool {
        matches!(lock(&self.0.state).mode, Mode::Queue { .. })
    }

    /// Wait up to `within` for every line accepted before this call to be
    /// written. A drop episode still open is closed first when its summary
    /// line fits in what the bound leaves (a viewer calling this in a loop
    /// must not grow the queue past it), so the summary is part of what is
    /// written. Lines logged after the call are not waited for. On the
    /// writer thread itself it returns at once; in sync mode there is
    /// nothing to wait for.
    pub fn flush(&self, within: Duration) -> Flush {
        self.flush_with(within, false)
    }

    /// [`LogBuffer::flush`] for a caller that is not the process itself (the
    /// control method `node.logs.flush`, which any viewer may call in a loop
    /// and which holds a thread of a shared blocking pool while it waits):
    /// at most [`MAX_VIEWER_FLUSHES`] wait at a time, and a call that finds
    /// them taken returns at once, `complete: false` with what is unwritten.
    pub fn viewer_flush(&self, within: Duration) -> Flush {
        struct Slot<'a>(&'a AtomicUsize);
        impl Drop for Slot<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let waiting = self.0.viewer_flushes.fetch_add(1, Ordering::AcqRel);
        let _slot = Slot(&self.0.viewer_flushes);
        if waiting >= MAX_VIEWER_FLUSHES {
            let st = lock(&self.0.state);
            return Flush {
                complete: false,
                unwritten_lines: st.accepted.saturating_sub(st.written),
            };
        }
        self.flush(within)
    }

    /// [`LogBuffer::flush`] for the last flush of a process (exit, `exec`,
    /// abort): an open episode is closed whatever the queue holds, its summary
    /// is the one line the process still has to say about what it lost.
    pub fn flush_final(&self, within: Duration) -> Flush {
        self.flush_with(within, true)
    }

    fn flush_with(&self, within: Duration, last: bool) -> Flush {
        let mut st = lock(&self.0.state);
        if st.mode == Mode::Sync {
            drop(st);
            self.0.sync_flush();
            return Flush {
                complete: true,
                unwritten_lines: 0,
            };
        }
        if st.writer == Some(std::thread::current().id()) {
            let unwritten_lines = st.accepted.saturating_sub(st.written);
            return Flush {
                complete: unwritten_lines == 0,
                unwritten_lines,
            };
        }
        if st.close_episode(last) {
            self.0.work.notify_one();
        }
        let target = st.accepted;
        let deadline = Instant::now() + within;
        loop {
            if st.written >= target {
                return Flush {
                    complete: true,
                    unwritten_lines: 0,
                };
            }
            let now = Instant::now();
            if now >= deadline {
                return Flush {
                    complete: false,
                    unwritten_lines: target - st.written,
                };
            }
            st = self
                .0
                .written
                .wait_timeout(st, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    pub fn stats(&self) -> LogStats {
        let st = lock(&self.0.state);
        let started = self.0.write_started.load(Ordering::Relaxed);
        LogStats {
            queued_mode: matches!(st.mode, Mode::Queue { .. }),
            limit_bytes: match st.mode {
                Mode::Queue { limit } => limit as u64,
                Mode::Sync => 0,
            },
            queued_bytes: st.used() as u64,
            queued_lines: (st.queue.len() + st.batch.len() + st.in_write.len()) as u64,
            queued_bytes_max: st.queued_bytes_max as u64,
            queue_entries_max: st.queue_entries_max as u64,
            accepted_lines: st.accepted,
            written_lines: st.written.saturating_sub(st.failed),
            dropped_lines: st.dropped_lines,
            dropped_bytes: st.dropped_bytes,
            drop_episodes: st.drop_episodes,
            longest_write_ms: self.0.longest_write_ms.load(Ordering::Relaxed),
            writing_for_ms: if started == 0 {
                0
            } else {
                self.0.now_ms().saturating_sub(started - 1)
            },
            sink_errors: st.sink_errors,
            carried_at_previous_exec: st.carried_at_previous_exec,
            lost_at_previous_exec: st.lost_at_previous_exec,
            writer_buffer_bytes: self.0.writer_buffer_bytes.load(Ordering::Relaxed),
            queue_blocks: st.queue.blocks.len() as u64,
            blocks_allocated: st.queue.allocated,
        }
    }

    /// Queue a panic's message past the bound (it does not wait: the
    /// panicking thread may hold locks others need). False when the caller
    /// should write it itself: sync mode, the writer thread (it would queue
    /// for a thread that is dying), or the state lock stays busy (a panic
    /// raised under it).
    pub(crate) fn push_panic(&self, text: &str) -> bool {
        let mut st = {
            let mut tries = 0;
            loop {
                match self.0.state.try_lock() {
                    Ok(guard) => break guard,
                    Err(std::sync::TryLockError::Poisoned(p)) => break p.into_inner(),
                    Err(std::sync::TryLockError::WouldBlock) => {
                        tries += 1;
                        if tries > 100 {
                            return false;
                        }
                        std::thread::sleep(Duration::from_micros(100));
                    }
                }
            }
        };
        if st.mode == Mode::Sync || st.writer == Some(std::thread::current().id()) {
            return false;
        }
        if st.push_panic(text) {
            self.0.work.notify_one();
        }
        true
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> LogWriter {
        LogWriter {
            buffer: self.clone(),
            reserved: false,
        }
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> LogWriter {
        self.writer_for_level(meta.level())
    }
}

/// One event's writer (the fmt layer makes one per event).
pub struct LogWriter {
    buffer: LogBuffer,
    reserved: bool,
}

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let shared = &*self.buffer.0;
        let mut st = lock(&shared.state);
        let state = &mut *st;
        let mut wake = false;
        let mut rest = bytes;
        while let Some(at) = memchr::memchr(b'\n', rest) {
            let (head, tail) = (&rest[..at], &rest[at + 1..]);
            // The common case: the whole line is in this write, one
            // allocation for the line and none for anything else.
            let line: Arc<str> = if state.partial.is_empty() {
                Arc::from(&*String::from_utf8_lossy(head))
            } else {
                state.partial.extend_from_slice(head);
                let line = Arc::from(&*String::from_utf8_lossy(&state.partial));
                state.partial.clear();
                line
            };
            // Admit first: a summary line that closes an episode goes into
            // the ring ahead of the line that found the queue drained.
            wake |= state.admit(Arc::clone(&line), self.reserved);
            state.push_ring(line);
            rest = tail;
        }
        state.partial.extend_from_slice(rest);
        let sync = state.mode == Mode::Sync;
        drop(st);
        if wake {
            shared.work.notify_one();
        }
        if sync {
            shared.sync_write(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // In queue mode the writer thread flushes its sink; waiting here
        // would be the blocking this module removes.
        let shared = &*self.buffer.0;
        if lock(&shared.state).mode == Mode::Sync {
            shared.sync_flush();
        }
        Ok(())
    }
}

/// stderr as a sink: the fault point's stall under std's stderr lock, then
/// the write. Follows the daemonizer's `dup2` onto fd 2.
pub struct StderrSink;

impl Write for StderrSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut lock = std::io::stderr().lock();
        stall_for_fault();
        lock.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut lock = std::io::stderr().lock();
        stall_for_fault();
        lock.write_all(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

/// The sink-write fault point's grammar: `<ms>[ once][ pid=<n>]`.
#[derive(Debug, PartialEq, Eq)]
struct LogStall {
    ms: u64,
    once: bool,
    pid: Option<u32>,
}

fn parse_log_stall(text: &str) -> Option<LogStall> {
    let mut it = text.split_whitespace();
    let ms: u64 = it.next()?.parse().ok()?;
    let mut spec = LogStall {
        ms,
        once: false,
        pid: None,
    };
    for word in it {
        if word == "once" && !spec.once {
            spec.once = true;
            continue;
        }
        let pid = word.strip_prefix("pid=").filter(|_| spec.pid.is_none())?;
        spec.pid = Some(pid.parse().ok()?);
    }
    (ms > 0).then_some(spec)
}

/// The stall `path` asks of the process with id `pid` now, taking a `once`
/// file away. Never logs (it runs inside the log path).
fn take_log_stall(path: &std::path::Path, pid: u32) -> Option<Duration> {
    let spec = parse_log_stall(&std::fs::read_to_string(path).ok()?)?;
    if spec.pid.is_some_and(|p| p != pid) {
        return None;
    }
    if spec.once && std::fs::remove_file(path).is_err() {
        // Another write took this one.
        return None;
    }
    Some(Duration::from_millis(spec.ms))
}

fn stall_for_fault() {
    static PATH: OnceLock<Option<std::path::PathBuf>> = OnceLock::new();
    let path = PATH.get_or_init(|| {
        std::env::var_os("CONSTELLATION_FAULT_LOG_STALL_FILE").map(std::path::PathBuf::from)
    });
    if let Some(path) = path {
        if let Some(stall) = take_log_stall(path, std::process::id()) {
            std::thread::sleep(stall);
        }
    }
}

/// The writer thread: move the queue into `batch` (O(1) under the lock),
/// then, one chunk at a time, take up to 256 KiB of lines out of it, write
/// them, account for them. What the writer holds stays in the shared state
/// (`batch`, and the chunk in `in_write`) until its write has returned, so an
/// `exec` can carry it to the next image. Each line's block is freed as the
/// line is consumed.
fn writer_loop(shared: Arc<Shared>, mut sink: Box<dyn Write + Send>) {
    let mut buf: Vec<u8> = Vec::with_capacity(IDLE_BUF_BYTES);
    let mut torn = false;
    let mut emptied: Vec<VecDeque<Arc<str>>> = Vec::with_capacity(SPARE_BLOCKS);
    let mut chunk: Vec<Arc<str>> = Vec::new();
    loop {
        let cost = {
            let mut st = lock(&shared.state);
            loop {
                if !st.frozen {
                    if st.batch.is_empty() && !st.queue.is_empty() {
                        st.inflight_bytes += std::mem::take(&mut st.queued_bytes);
                        let State { queue, batch, .. } = &mut *st;
                        queue.swap_out(batch);
                    }
                    if !st.batch.is_empty() {
                        break;
                    }
                }
                st.writer_idle = true;
                st = shared.work.wait(st).unwrap_or_else(PoisonError::into_inner);
            }
            st.writer_idle = false;
            chunk.clear();
            let (mut bytes, mut cost) = (0usize, 0usize);
            while bytes < WRITE_CHUNK {
                let Some(line) = st.batch.pop_front(&mut emptied) else {
                    break;
                };
                bytes += line.len() + 1;
                cost += line.len() + 1 + LINE_OVERHEAD;
                chunk.push(line);
            }
            st.in_write.clear();
            st.in_write.extend(chunk.iter().cloned());
            st.queue.adopt_spares(&mut emptied);
            cost
        };
        for line in &chunk {
            buf.extend_from_slice(line.as_bytes());
            buf.push(b'\n');
        }
        let lines = chunk.len() as u64;
        chunk.clear();
        write_chunk(&shared, &mut sink, &mut buf, &mut torn, lines, cost);
        if buf.capacity() > SHRINK_BUF_BYTES {
            buf = Vec::with_capacity(IDLE_BUF_BYTES);
        }
        shared
            .writer_buffer_bytes
            .store(buf.capacity() as u64, Ordering::Relaxed);
    }
}

/// Write all of `buf` to `sink`: the bytes that went out, and how it ended.
/// A sink that answers `WouldBlock` (a descriptor somebody made
/// non-blocking) is polled every 2 ms for up to [`WOULD_BLOCK_BOUND`], not
/// spun on and not given up on at once.
fn write_all_polling(sink: &mut dyn Write, buf: &[u8]) -> (usize, std::io::Result<()>) {
    use std::io::ErrorKind;
    let mut done = 0;
    let mut blocked_since: Option<Instant> = None;
    while done < buf.len() {
        match sink.write(&buf[done..]) {
            Ok(0) => return (done, Err(ErrorKind::WriteZero.into())),
            Ok(n) => {
                done += n;
                blocked_since = None;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                let since = *blocked_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= WOULD_BLOCK_BOUND {
                    return (done, Err(e));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(e) => return (done, Err(e)),
        }
    }
    (done, Ok(()))
}

/// Write one chunk and account for it. A chunk whose write failed
/// (`ENOSPC`, `EIO`, a closed pipe) is **dropped, not written**: its lines
/// are counted as dropped in an episode (its summary follows once a write
/// succeeds again) and as `sink_errors`. A write that failed half way leaves
/// a torn line in the file: the next chunk starts with a newline, so the
/// fragment stays a line of its own (the residual: that fragment).
fn write_chunk(
    shared: &Shared,
    sink: &mut Box<dyn Write + Send>,
    buf: &mut Vec<u8>,
    torn: &mut bool,
    lines: u64,
    cost: usize,
) {
    if std::mem::take(torn) {
        buf.insert(0, b'\n');
    }
    shared
        .write_started
        .store(shared.now_ms() + 1, Ordering::Relaxed);
    let started = Instant::now();
    let (done, result) = write_all_polling(&mut **sink, buf);
    let _ = sink.flush();
    shared
        .longest_write_ms
        .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
    shared.write_started.store(0, Ordering::Relaxed);
    let bytes = buf.len() as u64;
    *torn = result.is_err() && done > 0 && buf[done - 1] != b'\n';
    buf.clear();
    {
        let mut st = lock(&shared.state);
        st.written += lines;
        st.inflight_bytes = st.inflight_bytes.saturating_sub(cost);
        st.in_write.clear();
        match result {
            Ok(()) => st.sink_failing = false,
            Err(_) => {
                st.sink_errors += 1;
                st.sink_failing = true;
                st.failed += lines;
                st.record_loss(lines, bytes, Cause::SinkFailed);
            }
        }
        // A storm that ended, or a log gone quiet, closes its episode once
        // the batch is through (the next pass writes the summary).
        if st.batch.is_empty() {
            if let Mode::Queue { limit } = st.mode {
                if st.episode.is_some() && st.used() < limit / 2 {
                    st.close_episode(false);
                }
            }
        }
    }
    shared.written.notify_all();
}

/// The process's buffer, for the exit, exec, abort and panic paths that
/// have no handle to it.
static PROCESS: OnceLock<LogBuffer> = OnceLock::new();

/// Register `buffer` as the process's (the first registration wins).
pub fn set_process(buffer: &LogBuffer) {
    let _ = PROCESS.set(buffer.clone());
}

/// The process's writer has been started (the daemonizer's fork asserts it
/// has not).
pub fn process_started() -> bool {
    PROCESS.get().is_some_and(LogBuffer::is_queued)
}

/// [`LogBuffer::flush_final`] on the process's buffer (nothing to do when
/// none is registered): the flush of an exit, an `exec` or an abort.
pub fn flush_process(within: Duration) -> Flush {
    match PROCESS.get() {
        Some(buffer) => buffer.flush_final(within),
        None => Flush {
            complete: true,
            unwritten_lines: 0,
        },
    }
}

/// A flush of the process's buffer that is not its last (an upgrade's drain
/// before it detaches the views, while the daemon serves on): an open episode
/// is closed only when its summary fits, as for any viewer's flush.
pub fn drain_process(within: Duration) -> Flush {
    match PROCESS.get() {
        Some(buffer) => buffer.flush(within),
        None => Flush {
            complete: true,
            unwritten_lines: 0,
        },
    }
}

/// The previous image's writer had `lines` unwritten when it gave up at its
/// `exec` (see [`LogBuffer::note_exec_loss`]), on the process's buffer.
pub fn note_unwritten_at_exec(lines: u64) {
    if let Some(buffer) = PROCESS.get() {
        buffer.note_exec_loss(lines);
    }
}

/// The daemon's last words (the error `main` returns): queued like a
/// panic's message, outside `RUST_LOG` (the filter that decides which events
/// are logged must not decide whether a daemon says why it ended), first line
/// in the fmt layer's layout at ERROR. False when no writer is started: the
/// caller prints it itself.
pub fn queue_last_words(text: &str) -> bool {
    let Some(buffer) = PROCESS.get() else {
        return false;
    };
    let mut stamp = String::new();
    let _ = SystemTime.format_time(&mut FmtWriter::new(&mut stamp));
    let mut lines = text.lines();
    let Some(first) = lines.next() else {
        return true;
    };
    let mut out = format!("{stamp} ERROR constellation: {first}");
    for line in lines {
        out.push('\n');
        out.push_str(line);
    }
    buffer.push_panic(&out)
}

/// [`LogBuffer::freeze_for_exec`] on the process's buffer (an empty tail
/// when none is registered or it is synchronous).
pub fn freeze_process_for_exec() -> ExecTail {
    PROCESS
        .get()
        .map(LogBuffer::freeze_for_exec)
        .unwrap_or_default()
}

/// [`LogBuffer::exec_tail_update`] on the process's buffer.
pub fn exec_tail_update_process(sent: &ExecTail) -> ExecTail {
    PROCESS
        .get()
        .map(|buffer| buffer.exec_tail_update(sent))
        .unwrap_or_default()
}

/// [`LogBuffer::unfreeze`] on the process's buffer.
pub fn unfreeze_process() {
    if let Some(buffer) = PROCESS.get() {
        buffer.unfreeze();
    }
}

/// [`LogBuffer::adopt_exec_tail`] on the process's buffer.
pub fn adopt_process_exec_tail(tail: ExecTail) {
    if let Some(buffer) = PROCESS.get() {
        buffer.adopt_exec_tail(tail);
    }
}

/// Route a panic's message through the process's queue, in order with the
/// lines logged before it (std's hook writes it to stderr at once, behind a
/// stalled sink, with every lock the panicking thread holds still held).
/// The panicking thread does not wait. Falls back to the hook that was
/// installed before when no buffer is registered or started, or when the
/// panic is the writer's own. Idempotent.
pub fn install_panic_hook() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if let Some(buffer) = PROCESS.get() {
                if buffer.push_panic(&format_panic(info)) {
                    return;
                }
            }
            previous(info);
        }));
    });
}

/// What std's default hook prints: the thread, the location and the message,
/// then the backtrace when `RUST_BACKTRACE` asks for one, else (once per
/// process) the note that says how.
fn format_panic(info: &std::panic::PanicHookInfo<'_>) -> String {
    static NOTE_PRINTED: AtomicBool = AtomicBool::new(false);
    let payload = info.payload();
    let message = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("Box<dyn Any>");
    let thread = std::thread::current();
    let name = thread.name().unwrap_or("<unnamed>");
    let mut text = match info.location() {
        Some(at) => format!(
            "thread '{name}' panicked at {}:{}:{}:\n{message}",
            at.file(),
            at.line(),
            at.column()
        ),
        None => format!("thread '{name}' panicked:\n{message}"),
    };
    let backtrace = std::backtrace::Backtrace::capture();
    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        text.push_str(&format!("\nstack backtrace:\n{backtrace}"));
    } else if !NOTE_PRINTED.swap(true, Ordering::Relaxed) {
        text.push_str(
            "\nnote: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A sink whose `write` blocks until the test opens its gate, and that
    /// records what it got.
    #[derive(Clone)]
    struct BlockingSink {
        gate: Arc<(Mutex<bool>, Condvar)>,
        got: Arc<Mutex<Vec<u8>>>,
    }

    impl BlockingSink {
        fn shut() -> Self {
            BlockingSink {
                gate: Arc::new((Mutex::new(false), Condvar::new())),
                got: Arc::default(),
            }
        }

        fn open(&self) {
            *lock(&self.gate.0) = true;
            self.gate.1.notify_all();
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&lock(&self.got)).into_owned()
        }

        fn lines(&self) -> Vec<String> {
            self.text().lines().map(str::to_owned).collect()
        }
    }

    impl Write for BlockingSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            // Bounded: a test that fails with the gate still shut must
            // fail, not leave this thread (and the test binary) hanging.
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut open = lock(&self.gate.0);
            while !*open {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(std::io::ErrorKind::TimedOut.into());
                }
                open = self
                    .gate
                    .1
                    .wait_timeout(open, left)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            drop(open);
            lock(&self.got).extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn queued(limit: usize, sink: &BlockingSink) -> LogBuffer {
        let buffer = LogBuffer::default();
        buffer
            .start(QueueConfig::with_limit(limit), Box::new(sink.clone()))
            .unwrap();
        buffer
    }

    fn log(buffer: &LogBuffer, text: &str) {
        let mut w = buffer.make_writer();
        w.write_all(format!("{text}\n").as_bytes()).unwrap();
    }

    fn log_at(buffer: &LogBuffer, level: tracing::Level, text: &str) {
        let mut w = buffer.writer_for_level(&level);
        w.write_all(format!("{text}\n").as_bytes()).unwrap();
    }

    fn summary_count(lines: &[String]) -> usize {
        lines
            .iter()
            .filter(|l| l.contains("log lines dropped"))
            .count()
    }

    fn field(line: &str, name: &str) -> u64 {
        let key = format!("{name}=");
        let at = line.find(&key).unwrap() + key.len();
        line[at..]
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap()
            .parse()
            .unwrap()
    }

    /// Four threads, 2 500 writes of 200-byte lines each.
    fn hammer(buffer: &LogBuffer, finished: &Arc<AtomicUsize>) -> Vec<std::thread::JoinHandle<()>> {
        (0..4)
            .map(|t| {
                let buffer = buffer.clone();
                let finished = finished.clone();
                std::thread::spawn(move || {
                    let line = format!("{t:0>199}");
                    for _ in 0..2_500 {
                        log(&buffer, &line);
                    }
                    finished.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect()
    }

    #[test]
    fn a_stalled_sink_never_blocks_the_logging_thread() {
        let sink = BlockingSink::shut();
        let buffer = queued(64 << 10, &sink);
        let finished = Arc::new(AtomicUsize::new(0));
        let started = Instant::now();
        let threads = hammer(&buffer, &finished);
        while finished.load(Ordering::SeqCst) < 4 && started.elapsed() < Duration::from_millis(200)
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        let (done, took) = (finished.load(Ordering::SeqCst), started.elapsed());
        let stats = buffer.stats();
        // Let any thread that is stuck go before asserting.
        sink.open();
        for t in threads {
            t.join().unwrap();
        }
        assert!(
            done == 4 && took < Duration::from_millis(200),
            "{done} of 4 threads finished 10 000 writes behind a stalled sink in {took:?}"
        );
        assert!(stats.dropped_lines > 0, "{stats:?}");
        assert_eq!(stats.dropped_lines + stats.accepted_lines, 10_000);

        // The naive arm: the synchronous writer on the same stalled sink.
        let naive_sink = BlockingSink::shut();
        let naive = LogBuffer::with_sync_sink_for_test(Box::new(naive_sink.clone()));
        let finished = Arc::new(AtomicUsize::new(0));
        let threads = hammer(&naive, &finished);
        std::thread::sleep(Duration::from_millis(500));
        let done = finished.load(Ordering::SeqCst);
        assert!(
            done < 4,
            "the synchronous writer let {done} of 4 threads finish behind a stalled sink"
        );
        naive_sink.open();
        for t in threads {
            t.join().unwrap();
        }
    }

    #[test]
    fn drops_leave_one_marked_gap_in_order() {
        let sink = BlockingSink::shut();
        let buffer = queued(64 << 10, &sink);
        const N: u64 = 1_000;
        for i in 0..N {
            log(&buffer, &format!("line {i:05}"));
        }
        let stalled = buffer.stats();
        assert!(stalled.dropped_lines > 0, "{stalled:?}");
        sink.open();
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        // Lines logged after the episode closed come after its summary.
        for i in N..N + 10 {
            log(&buffer, &format!("line {i:05}"));
        }
        assert!(buffer.flush(Duration::from_secs(5)).complete);

        let lines = sink.lines();
        let at = lines
            .iter()
            .position(|l| l.contains("log lines dropped"))
            .expect("a summary line");
        assert_eq!(summary_count(&lines), 1, "{lines:?}");
        let kept: Vec<_> = (0..at).map(|i| format!("line {i:05}")).collect();
        assert_eq!(&lines[..at], &kept[..], "a contiguous prefix");
        let k = at as u64;
        assert_eq!(field(&lines[at], "dropped_lines"), N - k);
        let after: Vec<_> = (N..N + 10).map(|i| format!("line {i:05}")).collect();
        assert_eq!(&lines[at + 1..], &after[..]);
        let stats = buffer.stats();
        assert_eq!(stats.dropped_lines, N - k);
        assert_eq!(stats.drop_episodes, 1);
        assert_eq!(field(&lines[at], "dropped_bytes"), stats.dropped_bytes);
        assert!(lines[at].contains("  WARN constellation::log: log lines dropped:"));
        assert!(lines[at].contains("queue_limit_bytes=65536"));
    }

    #[test]
    fn a_quiet_log_closes_its_episode_when_the_writer_catches_up() {
        let sink = BlockingSink::shut();
        let buffer = queued(64 << 10, &sink);
        for i in 0..1_000 {
            log(&buffer, &format!("line {i:05}"));
        }
        sink.open();
        // No flush and no further line: the writer's own check closes it.
        let deadline = Instant::now() + Duration::from_secs(5);
        while summary_count(&sink.lines()) == 0 {
            assert!(Instant::now() < deadline, "no summary: {:?}", sink.lines());
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn an_open_episode_drops_ordinary_lines_until_the_queue_is_half_empty() {
        // 399-byte lines: cost 464.
        let buffer = LogBuffer::queue_only_for_test(64 << 10);
        let line = "x".repeat(399);
        for _ in 0..200 {
            log(&buffer, &line);
        }
        assert!(buffer.stats().dropped_lines > 0);
        // The writer drains a little: the queue is still above half, so the
        // gap stays open and an ordinary line that would fit is dropped
        // (one contiguous gap); a warning that fits is admitted.
        buffer.drain_for_test(16 << 10);
        let before = buffer.stats();
        assert!(before.queued_bytes > 32 << 10, "{before:?}");
        log(&buffer, "ordinary during the episode");
        log_at(&buffer, tracing::Level::WARN, "warning during the episode");
        let after = buffer.stats();
        assert_eq!(after.dropped_lines, before.dropped_lines + 1);
        assert_eq!(after.accepted_lines, before.accepted_lines + 1);
        assert_eq!(after.drop_episodes, 1);
        // Below half: the next line closes the episode, summary first.
        buffer.drain_for_test(usize::MAX);
        log(&buffer, "after the episode");
        let tail = buffer.tail(2);
        assert!(tail[0].contains("log lines dropped"), "{tail:?}");
        assert_eq!(tail[1], "after the episode");
        assert_eq!(buffer.stats().drop_episodes, 1);
        let mut st = lock(&buffer.0.state);
        assert!(st
            .queue
            .pop_front(&mut Vec::new())
            .unwrap()
            .contains("log lines dropped"));
        assert_eq!(
            &*st.queue.pop_front(&mut Vec::new()).unwrap(),
            "after the episode"
        );
    }

    #[test]
    fn warnings_use_the_reserve() {
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let buffer = queued(limit, &sink);
        let line = "w".repeat(100);
        let cost = 100 + 1 + LINE_OVERHEAD;
        let ordinary_cap = limit - limit / 8;
        while buffer.stats().queued_bytes as usize + cost <= ordinary_cap {
            log(&buffer, &line);
        }
        let filled = buffer.stats();
        log(&buffer, &line);
        assert_eq!(buffer.stats().dropped_lines, 1, "the next info line drops");
        log_at(&buffer, tracing::Level::WARN, &line);
        let after_warn = buffer.stats();
        assert_eq!(after_warn.dropped_lines, 1, "a WARN line is accepted");
        assert_eq!(after_warn.queued_bytes, filled.queued_bytes + cost as u64);
        // Fill the reserve with errors; at the limit both drop.
        while buffer.stats().queued_bytes as usize + cost <= limit {
            log_at(&buffer, tracing::Level::ERROR, &line);
        }
        let dropped = buffer.stats().dropped_lines;
        log(&buffer, &line);
        log_at(&buffer, tracing::Level::WARN, &line);
        log_at(&buffer, tracing::Level::ERROR, &line);
        assert_eq!(buffer.stats().dropped_lines, dropped + 3);
        assert!(buffer.stats().queued_bytes as usize <= limit);
        sink.open();
    }

    #[test]
    fn flush_waits_for_what_came_before_and_is_bounded() {
        let sink = BlockingSink::shut();
        let buffer = queued(1 << 20, &sink);
        for i in 0..50 {
            log(&buffer, &format!("queued {i}"));
        }
        let started = Instant::now();
        let flush = buffer.flush(Duration::from_millis(100));
        let took = started.elapsed();
        assert!(!flush.complete);
        assert_eq!(flush.unwritten_lines, 50);
        assert!(
            took >= Duration::from_millis(100) && took < Duration::from_millis(500),
            "{took:?}"
        );
        sink.open();
        let flush = buffer.flush(Duration::from_secs(5));
        assert_eq!(
            flush,
            Flush {
                complete: true,
                unwritten_lines: 0
            }
        );
        assert_eq!(sink.lines().len(), 50);

        // On the writer thread itself it returns at once (the sink is
        // called from there).
        struct Reentrant {
            buffer: Arc<OnceLock<LogBuffer>>,
            result: Arc<Mutex<Option<(Flush, Duration)>>>,
        }
        impl Write for Reentrant {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let started = Instant::now();
                let flush = self.buffer.get().unwrap().flush(Duration::from_secs(10));
                *lock(&self.result) = Some((flush, started.elapsed()));
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let cell = Arc::new(OnceLock::new());
        let result = Arc::new(Mutex::new(None));
        let buffer = LogBuffer::default();
        buffer
            .start(
                QueueConfig::with_limit(1 << 20),
                Box::new(Reentrant {
                    buffer: cell.clone(),
                    result: result.clone(),
                }),
            )
            .unwrap();
        cell.set(buffer.clone()).ok();
        log(&buffer, "one");
        let deadline = Instant::now() + Duration::from_secs(5);
        let (flush, took) = loop {
            if let Some(r) = *lock(&result) {
                break r;
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        };
        assert!(!flush.complete);
        assert!(took < Duration::from_secs(1), "{took:?}");
    }

    #[test]
    fn the_ring_keeps_every_line_and_the_same_text() {
        let sink = BlockingSink::shut();
        let buffer = queued(64 << 10, &sink);
        let mut expected: Vec<String> = Vec::new();
        // A line split over two writes is one line in the ring and the sink.
        {
            let mut w = buffer.make_writer();
            w.write_all(b"split ").unwrap();
            w.write_all(b"in two\n").unwrap();
            expected.push("split in two".into());
        }
        // Several lines in one write.
        buffer.make_writer().write_all(b"a\nb\nc\n").unwrap();
        expected.extend(["a", "b", "c"].map(String::from));
        // Invalid UTF-8 is replaced the way the ring always replaced it.
        buffer.make_writer().write_all(b"bad \xff byte\n").unwrap();
        expected.push("bad \u{fffd} byte".into());
        let mut dropped = Vec::new();
        for i in 0..1_000 {
            let line = format!("bulk {i:05}");
            let before = buffer.stats().dropped_lines;
            log(&buffer, &line);
            if buffer.stats().dropped_lines > before {
                dropped.push(line);
            } else {
                expected.push(line);
            }
        }
        assert!(!dropped.is_empty());
        let tail = buffer.tail(2_000);
        assert_eq!(tail.len(), 1_005);
        for line in &dropped {
            assert!(tail.contains(line), "the ring lost {line}");
        }
        sink.open();
        // The flush closes the episode: its summary is the only extra line.
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        let lines = sink.lines();
        assert_eq!(lines.len(), expected.len() + 1);
        assert_eq!(lines[..expected.len()], expected[..]);
        assert!(lines[expected.len()].contains("log lines dropped"));
        assert_eq!(buffer.since(0).0, 1_006);
    }

    #[test]
    fn a_poisoned_buffer_keeps_logging() {
        let sink = BlockingSink::shut();
        sink.open();
        let buffer = queued(1 << 20, &sink);
        log(&buffer, "before");
        buffer.poison_for_test();
        log(&buffer, "after");
        assert_eq!(buffer.tail(10), ["before", "after"]);
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(sink.lines(), ["before", "after"]);
        let _ = buffer.stats();

        let naive = LogBuffer::with_sync_sink_for_test(Box::new(sink.clone()));
        naive.poison_for_test();
        log(&naive, "sync after");
        assert_eq!(naive.tail(1), ["sync after"]);
    }

    #[test]
    fn memory_is_bounded_and_returned() {
        let sink = BlockingSink::shut();
        let limit = 16 << 20;
        let buffer = queued(limit, &sink);
        // One line that grows the writer's byte buffer past its idle size,
        // then tiny lines: past 64 Ki entries queued.
        log(&buffer, &"y".repeat(2 << 20));
        for _ in 0..300_000 {
            log(&buffer, "x");
            assert!(buffer.stats().queued_bytes as usize <= limit);
        }
        let stalled = buffer.stats();
        assert!(stalled.queued_bytes as usize <= limit, "{stalled:?}");
        assert!(stalled.queued_bytes_max as usize <= limit, "{stalled:?}");
        assert!(stalled.dropped_lines > 0);
        sink.open();
        assert!(buffer.flush(Duration::from_secs(30)).complete);
        // One more batch, so every deque has been through a swap.
        log(&buffer, "last");
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        let idle = buffer.stats();
        assert_eq!(idle.queued_bytes, 0);
        assert!(
            idle.writer_buffer_bytes <= SHRINK_BUF_BYTES as u64,
            "{idle:?}"
        );
        // What the backlog took is back: no block is held for an empty queue.
        assert_eq!(idle.queue_blocks, 0, "{idle:?}");
    }

    /// A log whose writer keeps up reuses its emptied blocks: one block
    /// allocated for thousands of lines logged and flushed one by one (before
    /// the spares, every batch allocated one: an allocation per line more than
    /// the writer this replaced).
    #[test]
    fn a_writer_that_keeps_up_reuses_its_blocks() {
        let sink = BlockingSink::shut();
        sink.open();
        let buffer = queued(1 << 20, &sink);
        for i in 0..2_000 {
            log(&buffer, &format!("line {i}"));
            assert!(buffer.flush(Duration::from_secs(5)).complete);
        }
        let stats = buffer.stats();
        assert!(stats.blocks_allocated <= 3, "{stats:?}");
    }

    /// The stalled backlog is held in 1 024-entry blocks, so a push never
    /// grows or copies a large array under the lock, and the blocks go as the
    /// writer consumes them.
    #[test]
    fn a_backlog_is_held_in_blocks_that_go_as_they_are_consumed() {
        let sink = BlockingSink::shut();
        let buffer = queued(16 << 20, &sink);
        for _ in 0..20_000 {
            log(&buffer, "x");
        }
        let stats = buffer.stats();
        // What the writer took before it stalled is in its write; the rest
        // is queued in blocks of 1 024.
        // (How much the writer took before it stalled depends on how soon
        // it woke: the blocks follow what is left, never more than the
        // queue's high-water mark needs.)
        let queued = stats.queue_blocks as usize;
        assert!(
            (1..=stats.queue_entries_max.div_ceil(BLOCK_ENTRIES as u64) as usize).contains(&queued),
            "{stats:?}"
        );
        sink.open();
        assert!(buffer.flush(Duration::from_secs(30)).complete);
        assert_eq!(buffer.stats().queue_blocks, 0);
    }

    #[test]
    fn a_dead_writer_leaves_the_log_bounded() {
        struct Panicking;
        impl Write for Panicking {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                panic!("the sink panicked on purpose");
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buffer = LogBuffer::default();
        buffer
            .start(QueueConfig::with_limit(64 << 10), Box::new(Panicking))
            .unwrap();
        for i in 0..2_000 {
            log(&buffer, &format!("line {i:0>150}"));
        }
        let stats = buffer.stats();
        assert!(stats.dropped_lines > 0, "{stats:?}");
        assert!(stats.queued_bytes <= 64 << 10);
        assert!(!buffer.flush(Duration::from_millis(50)).complete);
        assert_eq!(buffer.tail(1).len(), 1);
    }

    /// A sink that misbehaves as a disk does: `plan` lists what each `write`
    /// call does, then every call succeeds.
    #[derive(Clone)]
    struct ScriptedSink {
        plan: Arc<Mutex<std::collections::VecDeque<Step>>>,
        got: Arc<Mutex<Vec<u8>>>,
    }

    #[derive(Clone, Copy)]
    enum Step {
        /// Take this many bytes, then succeed.
        Take(usize),
        /// Take this many bytes of the call, then fail it with `ENOSPC`-like
        /// `StorageFull` (0: take none).
        FailAfter(usize),
        WouldBlock,
    }

    impl ScriptedSink {
        fn new(plan: &[Step]) -> Self {
            ScriptedSink {
                plan: Arc::new(Mutex::new(plan.iter().copied().collect())),
                got: Arc::default(),
            }
        }

        fn lines(&self) -> Vec<String> {
            String::from_utf8_lossy(&lock(&self.got))
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    impl Write for ScriptedSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let step = lock(&self.plan).pop_front();
            match step {
                None => {
                    lock(&self.got).extend_from_slice(bytes);
                    Ok(bytes.len())
                }
                Some(Step::Take(n)) => {
                    let n = n.min(bytes.len());
                    lock(&self.got).extend_from_slice(&bytes[..n]);
                    Ok(n)
                }
                Some(Step::FailAfter(n)) => {
                    let n = n.min(bytes.len());
                    lock(&self.got).extend_from_slice(&bytes[..n]);
                    if n > 0 {
                        // A partial write is reported as one; the failure
                        // is the next call's.
                        lock(&self.plan).push_front(Step::FailAfter(0));
                        Ok(n)
                    } else {
                        Err(std::io::ErrorKind::StorageFull.into())
                    }
                }
                Some(Step::WouldBlock) => Err(std::io::ErrorKind::WouldBlock.into()),
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn queued_on(limit: usize, sink: ScriptedSink) -> LogBuffer {
        let buffer = LogBuffer::default();
        buffer
            .start(QueueConfig::with_limit(limit), Box::new(sink))
            .unwrap();
        buffer
    }

    /// A viewer's `flush` in a loop behind a stalled sink closes episodes
    /// only while their summary fits: the queue never passes its bound
    /// (before, each call put one more summary line past it: 5 MB for a 64
    /// KiB bound in 20 000 calls).
    #[test]
    fn a_viewers_flush_cannot_grow_the_queue_past_its_bound() {
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let buffer = queued(limit, &sink);
        for i in 0..20_000 {
            log(&buffer, &format!("line {i:0>150}"));
            let flush = buffer.flush(Duration::ZERO);
            assert!(!flush.complete || i == 0);
            assert!(
                buffer.stats().queued_bytes as usize <= limit,
                "queued {} of {limit} after {i} calls",
                buffer.stats().queued_bytes
            );
        }
        let stats = buffer.stats();
        assert!(stats.queued_bytes_max as usize <= limit, "{stats:?}");
        // The summaries it did put in are bounded by the reserve, not by the
        // number of calls.
        assert!(stats.drop_episodes < 200, "{stats:?}");
        sink.open();
    }

    #[test]
    fn the_final_flush_closes_the_episode_whatever_the_queue_holds() {
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let buffer = queued(limit, &sink);
        let mut i = 0;
        // Fill queue and reserve with errors so no summary fits.
        while buffer.stats().queued_bytes as usize + 200 <= limit {
            log_at(&buffer, tracing::Level::ERROR, &format!("e {i:0>150}"));
            i += 1;
        }
        log(&buffer, "dropped"); // opens the episode
        assert_eq!(buffer.stats().drop_episodes, 1);
        let before = buffer.stats().accepted_lines;
        assert!(!buffer.flush(Duration::ZERO).complete);
        assert_eq!(
            buffer.stats().accepted_lines,
            before,
            "a viewer's flush: no summary fits"
        );
        assert!(!buffer.flush_final(Duration::ZERO).complete);
        assert_eq!(
            buffer.stats().accepted_lines,
            before + 1,
            "the final flush says it"
        );
        sink.open();
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(summary_count(&sink.lines()), 1);
    }

    /// Panics caught and survived keep reaching the log for the life of the
    /// process: past the 64 that bypass the bound they are admitted like
    /// WARN and ERROR lines, and what the reserve cannot hold is counted in
    /// an episode with its summary line (before, the 65th and every later
    /// one was dropped with no episode, even with an empty queue).
    #[test]
    fn panics_past_the_cap_are_logged_or_counted() {
        let sink = BlockingSink::shut();
        sink.open();
        let buffer = queued(1 << 20, &sink);
        for i in 0..200 {
            assert!(buffer.push_panic(&format!("thread 'x' panicked at f.rs:{i}:1:\nboom {i}")));
        }
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(sink.lines().len(), 400, "every message, both lines");
        assert_eq!(buffer.stats().dropped_lines, 0);

        // Behind a stalled sink with the reserve exhausted: counted, marked.
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let buffer = queued(limit, &sink);
        let mut i = 0;
        while buffer.stats().queued_bytes as usize + 200 <= limit {
            log_at(&buffer, tracing::Level::ERROR, &format!("e {i:0>150}"));
            i += 1;
        }
        for i in 0..200 {
            buffer.push_panic(&format!("thread 'x' panicked at f.rs:{i}:1:\nboom {i}"));
        }
        let stats = buffer.stats();
        assert!(
            stats.dropped_lines > 0 && stats.drop_episodes >= 1,
            "{stats:?}"
        );
        sink.open();
        assert!(buffer.flush_final(Duration::from_secs(10)).complete);
        let lines = sink.lines();
        let panics = lines.iter().filter(|l| l.starts_with("boom ")).count()
            + lines.iter().filter(|l| l.starts_with("thread 'x'")).count();
        let said: u64 = lines
            .iter()
            .filter(|l| l.contains("log lines dropped"))
            .map(|l| field(l, "dropped_lines"))
            .sum();
        // Every panic line is in the sink or counted, and the summary lines
        // say exactly what was counted.
        assert_eq!(panics as u64 + stats.dropped_lines, 400, "{stats:?}");
        assert_eq!(said, stats.dropped_lines);
    }

    /// A sink write that fails drops the chunk, counted, and the summary
    /// follows once a write succeeds (before, the lines counted as written
    /// and nothing said they were lost). A write that failed half way leaves
    /// a torn line: the next chunk starts with a newline, so the fragment
    /// stays a line of its own.
    #[test]
    fn a_failing_sink_drops_its_chunk_counted_and_marked() {
        // The first chunk (one line) writes 5 bytes and fails.
        let sink = ScriptedSink::new(&[Step::FailAfter(5)]);
        let buffer = queued_on(1 << 20, sink.clone());
        log(&buffer, "first line, torn");
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        let stats = buffer.stats();
        assert_eq!(stats.sink_errors, 1);
        assert_eq!(stats.dropped_lines, 1, "{stats:?}");
        assert_eq!(
            stats.written_lines, 0,
            "a failed write is not written: {stats:?}"
        );
        assert_eq!(stats.drop_episodes, 1);
        // No summary while the sink is failing: the next write is the test.
        assert!(!sink.lines().iter().any(|l| l.contains("log lines dropped")));
        log(&buffer, "second line");
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        // The torn fragment is its own line; the summary marks the loss.
        let lines = sink.lines();
        assert_eq!(lines[0], "first");
        assert_eq!(lines[1], "second line");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let lines = sink.lines();
            if let Some(l) = lines.iter().find(|l| l.contains("log lines dropped")) {
                assert_eq!(field(l, "dropped_lines"), 1);
                assert!(l.contains("writes to the log sink failed"), "{l}");
                break;
            }
            assert!(Instant::now() < deadline, "no summary: {lines:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(buffer.stats().dropped_lines, 1);
    }

    /// A sink whose behaviour the test switches: shut (blocks), failing
    /// (`StorageFull`), or fine.
    #[derive(Clone)]
    struct SwitchSink {
        mode: Arc<(Mutex<u8>, Condvar)>,
        got: Arc<Mutex<Vec<u8>>>,
    }

    impl SwitchSink {
        const SHUT: u8 = 0;
        const FAILING: u8 = 1;
        const FINE: u8 = 2;

        fn new() -> Self {
            SwitchSink {
                mode: Arc::new((Mutex::new(Self::SHUT), Condvar::new())),
                got: Arc::default(),
            }
        }

        fn set(&self, mode: u8) {
            *lock(&self.mode.0) = mode;
            self.mode.1.notify_all();
        }

        fn lines(&self) -> Vec<String> {
            String::from_utf8_lossy(&lock(&self.got))
                .lines()
                .map(str::to_owned)
                .collect()
        }
    }

    impl Write for SwitchSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let (guard, timeout) = self
                .mode
                .1
                .wait_timeout_while(lock(&self.mode.0), Duration::from_secs(30), |m| {
                    *m == Self::SHUT
                })
                .unwrap_or_else(PoisonError::into_inner);
            if timeout.timed_out() {
                return Err(std::io::ErrorKind::TimedOut.into());
            }
            if *guard == Self::FAILING {
                return Err(std::io::ErrorKind::StorageFull.into());
            }
            drop(guard);
            lock(&self.got).extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// A queue that filled behind a stalled sink, whose sink then failed (a
    /// full disk) and then recovered, logs again: the ordinary lines after
    /// the recovery reach the sink without a WARN or ERROR having to be
    /// written first (before, the episode of the full queue kept blocking
    /// them, the failing mark was only cleared by a write, and nothing was
    /// written: 0 of 200 lines reached the sink, 2 200 counted dropped).
    #[test]
    fn ordinary_lines_resume_after_a_full_queue_and_a_failed_then_recovered_sink() {
        let sink = SwitchSink::new();
        let buffer = LogBuffer::default();
        buffer
            .start(QueueConfig::with_limit(64 << 10), Box::new(sink.clone()))
            .unwrap();
        for i in 0..2_000 {
            log(&buffer, &format!("full {i:0>100}"));
        }
        assert!(buffer.stats().drop_episodes >= 1);
        sink.set(SwitchSink::FAILING);
        // (No flush here: a viewer's flush would close the episode of the
        // full queue before the failure is seen, and hide the wedge.)
        let deadline = Instant::now() + Duration::from_secs(5);
        while buffer.stats().sink_errors < 1 || buffer.stats().queued_bytes > 0 {
            assert!(Instant::now() < deadline, "{:?}", buffer.stats());
            std::thread::sleep(Duration::from_millis(5));
        }
        sink.set(SwitchSink::FINE);
        for i in 0..200 {
            log(&buffer, &format!("after {i}"));
        }
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        let after = sink
            .lines()
            .iter()
            .filter(|l| l.starts_with("after "))
            .count();
        assert_eq!(after, 200, "{:?}", buffer.stats());
        // And the episode says what it was.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !sink.lines().iter().any(|l| l.contains("log lines dropped")) {
            assert!(
                Instant::now() < deadline,
                "no summary: {:?}",
                buffer.stats()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// A viewer's flush holds a thread while it waits: two may wait at once,
    /// a third (behind a stalled sink) is told at once that it is not done.
    #[test]
    fn viewer_flushes_are_limited_and_the_excess_returns_at_once() {
        let sink = BlockingSink::shut();
        let buffer = queued(1 << 20, &sink);
        log(&buffer, "queued behind the stall");
        let waiters: Vec<_> = (0..MAX_VIEWER_FLUSHES)
            .map(|_| {
                let buffer = buffer.clone();
                std::thread::spawn(move || buffer.viewer_flush(Duration::from_secs(10)))
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(5);
        while buffer.0.viewer_flushes.load(Ordering::Acquire) < MAX_VIEWER_FLUSHES {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        let started = Instant::now();
        let third = buffer.viewer_flush(Duration::from_secs(10));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert!(!third.complete && third.unwritten_lines >= 1, "{third:?}");
        sink.open();
        for w in waiters {
            assert!(w.join().unwrap().complete);
        }
        assert_eq!(buffer.0.viewer_flushes.load(Ordering::Acquire), 0);
        assert!(buffer.viewer_flush(Duration::from_secs(5)).complete);
    }

    /// A sink that keeps failing does not make the writer spin out summary
    /// lines: one pass per accepted line, no summary until a write works.
    #[test]
    fn a_sink_that_keeps_failing_is_not_spun_on() {
        let sink = ScriptedSink::new(&[Step::FailAfter(0); 50]);
        let buffer = queued_on(1 << 20, sink.clone());
        for i in 0..20 {
            log(&buffer, &format!("line {i}"));
            assert!(buffer.flush(Duration::from_secs(5)).complete);
        }
        std::thread::sleep(Duration::from_millis(200));
        let stats = buffer.stats();
        assert_eq!(stats.sink_errors, 20, "{stats:?}");
        assert_eq!(
            stats.accepted_lines, 20,
            "no summary line was queued: {stats:?}"
        );
        assert_eq!(stats.dropped_lines, 20);
        assert_eq!(stats.drop_episodes, 1);
    }

    /// A sink that answers `WouldBlock` (a non-blocking descriptor) is
    /// polled and the chunk finished, not dropped.
    #[test]
    fn a_would_block_sink_is_polled_not_dropped() {
        let sink = ScriptedSink::new(&[Step::WouldBlock, Step::WouldBlock, Step::Take(4)]);
        let buffer = queued_on(1 << 20, sink.clone());
        log(&buffer, "a line that needs three tries");
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(sink.lines(), ["a line that needs three tries"]);
        let stats = buffer.stats();
        assert_eq!(
            (stats.dropped_lines, stats.sink_errors, stats.written_lines),
            (0, 0, 1)
        );
    }

    /// Lines the previous image left unwritten at its `exec` are lost lines:
    /// counted as dropped and said at once in a summary line (before, a
    /// separate counter and a warning that no check read).
    #[test]
    fn an_exec_loss_is_counted_as_dropped_with_a_summary_line() {
        let sink = BlockingSink::shut();
        sink.open();
        let buffer = queued(1 << 20, &sink);
        buffer.note_exec_loss(0);
        assert_eq!(buffer.stats().dropped_lines, 0);
        buffer.note_exec_loss(376);
        let stats = buffer.stats();
        assert_eq!(
            (
                stats.dropped_lines,
                stats.drop_episodes,
                stats.lost_at_previous_exec
            ),
            (376, 1, 376)
        );
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        let lines = sink.lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(field(&lines[0], "dropped_lines"), 376);
        assert!(
            lines[0].contains("exec'd before its log writer was done"),
            "{lines:?}"
        );
    }

    /// The tail the old image carries over an `exec`, bit for bit.
    #[test]
    fn an_exec_tail_encodes_and_decodes() {
        let tail = ExecTail {
            lines: vec!["one".into(), "".into(), "tre\u{e9}s".into()],
            in_doubt: 2,
            lost: 7,
            dropped_lines: 11,
            dropped_bytes: 222,
            drop_episodes: 3,
            sink_errors: 1,
        };
        let bytes = tail.encode();
        assert_eq!(ExecTail::decode(&bytes), Some(tail.clone()));
        assert_eq!(
            ExecTail::decode(&ExecTail::default().encode()),
            Some(ExecTail::default())
        );
        // Anything that is not exactly what `encode` writes is no tail.
        for cut in [0, 7, 8, 40, bytes.len() - 1] {
            assert_eq!(ExecTail::decode(&bytes[..cut]), None, "cut at {cut}");
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert_eq!(ExecTail::decode(&longer), None);
        let mut other = bytes;
        other[0] = b'X';
        assert_eq!(ExecTail::decode(&other), None);
    }

    /// A stalled old image's unwritten log is the new image's first output,
    /// in order, ahead of the new image's own lines; the old image's counters
    /// continue (before, the lines were lost and counted dropped).
    #[test]
    fn a_frozen_log_is_written_first_by_the_next_image_in_order() {
        let old_sink = BlockingSink::shut();
        let old = queued(1 << 20, &old_sink);
        for i in 0..50 {
            log(&old, &format!("old {i:02}"));
        }
        // The writer has a chunk in its (stalled) write.
        let deadline = Instant::now() + Duration::from_secs(5);
        while lock(&old.0.state).in_write.is_empty() {
            assert!(Instant::now() < deadline, "the writer never took a chunk");
            std::thread::sleep(Duration::from_millis(2));
        }
        let tail = old.freeze_for_exec();
        assert_eq!(
            tail.lines.len(),
            50,
            "everything unwritten, in doubt or not"
        );
        assert!(tail.in_doubt >= 1 && tail.lost == 0, "{tail:?}");
        // Frozen: the writer takes nothing more, even when the sink opens
        // after the write in flight (that chunk is in doubt: it may be
        // written by both images).
        let decoded = ExecTail::decode(&tail.encode()).unwrap();

        let new_sink = BlockingSink::shut();
        new_sink.open();
        let new = queued(1 << 20, &new_sink);
        new.adopt_exec_tail(decoded);
        for i in 0..5 {
            log(&new, &format!("new {i}"));
        }
        assert!(new.flush(Duration::from_secs(5)).complete);
        let lines = new_sink.lines();
        let old_at = lines.iter().position(|l| l == "old 00").unwrap();
        let first_new = lines.iter().position(|l| l == "new 0").unwrap();
        assert!(old_at < first_new, "{lines:?}");
        let olds: Vec<_> = lines.iter().filter(|l| l.starts_with("old ")).collect();
        let want: Vec<String> = (0..50).map(|i| format!("old {i:02}")).collect();
        assert_eq!(olds, want.iter().collect::<Vec<_>>());
        assert!(lines
            .iter()
            .any(|l| l.contains("may repeat lines it wrote")));
        assert!(lines
            .iter()
            .any(|l| l.contains("50 line(s) above were unwritten")));
        assert_eq!(new.tail(1000).len(), lines.len(), "the ring has them too");
        let stats = new.stats();
        assert_eq!(
            (stats.carried_at_previous_exec, stats.lost_at_previous_exec),
            (50, 0)
        );
        assert_eq!(stats.dropped_lines, 0);

        // The exec failed instead: the old image's writer goes on.
        old.unfreeze();
        old_sink.open();
        assert!(old.flush(Duration::from_secs(5)).complete);
        assert_eq!(old_sink.lines().len(), 50);
    }

    /// The tail is the chunk in the write, then the rest of the writer's
    /// batch, then the queue: the order the lines were logged in.
    #[test]
    fn a_frozen_log_is_the_chunk_in_write_then_the_batch_then_the_queue() {
        let sink = BlockingSink::shut();
        let buffer = queued(1 << 20, &sink);
        {
            // The writer is idle: set the three places by hand.
            let mut st = lock(&buffer.0.state);
            st.in_write.push("a".into());
            st.batch.push_back("b".into());
            st.batch.push_back("c".into());
            st.queue.push_back("d".into());
        }
        let tail = buffer.freeze_for_exec();
        let lines: Vec<&str> = tail.lines.iter().map(|l| &**l).collect();
        assert_eq!(lines, ["a", "b", "c", "d"]);
        assert_eq!((tail.in_doubt, tail.lost), (1, 0));
    }

    /// Lines other threads log after the freeze (R3-1): the update has them,
    /// in order, up to the tail's bound, counts those past it as lost, and has
    /// the counters as they stand; header and lines patched on, the memfd's
    /// bytes decode to the whole.
    #[test]
    fn the_update_after_a_freeze_has_what_was_logged_since_in_order() {
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let buffer = queued(limit, &sink);
        log(&buffer, "early 0");
        log(&buffer, "early 1");
        let sent = buffer.freeze_for_exec();
        assert_eq!(sent.lines.len(), 2);
        // Nothing since: no lines, the same loss.
        let none = buffer.exec_tail_update(&sent);
        assert!(none.lines.is_empty() && none.lost == sent.lost, "{none:?}");
        log(&buffer, "late 0");
        log(&buffer, "late 1");
        let update = buffer.exec_tail_update(&sent);
        let lines: Vec<&str> = update.lines.iter().map(|l| &**l).collect();
        assert_eq!(lines, ["late 0", "late 1"]);
        assert_eq!(update.lost, 0);
        // The memfd after the patch: the first bytes, the late lines, the
        // header rewritten with the whole count.
        let mut bytes = sent.encode();
        bytes.extend_from_slice(&update.encode_lines());
        bytes[..TAIL_HEADER_LEN]
            .copy_from_slice(&update.encode_header(sent.lines.len() + update.lines.len()));
        let whole = ExecTail::decode(&bytes).expect("a whole tail");
        let all: Vec<&str> = whole.lines.iter().map(|l| &**l).collect();
        assert_eq!(all, ["early 0", "early 1", "late 0", "late 1"]);
        // A storm past the bound: what fits the tail's bound is carried, the
        // rest is counted lost, and the drops the queue itself made are in the
        // counters. Nothing is unaccounted for.
        let big = "x".repeat(4_000);
        let mut logged = 0u64;
        for _ in 0..200 {
            log(&buffer, &big);
            logged += 1;
        }
        let update = buffer.exec_tail_update(&sent);
        let carried: usize = update.lines.iter().map(|l| l.len() + 1).sum::<usize>()
            + sent.lines.iter().map(|l| l.len() + 1).sum::<usize>();
        assert!(carried <= limit, "{carried} of {limit}");
        let accepted_since = buffer.stats().accepted_lines - 2;
        assert_eq!(
            update.lines.len() as u64 + update.lost - sent.lost,
            accepted_since,
            "every line the queue took after the freeze is carried or lost"
        );
        assert_eq!(
            accepted_since + (update.dropped_lines - sent.dropped_lines),
            2 + logged,
            "and every other one is a counted drop"
        );
        buffer.unfreeze();
    }

    /// An unreadable tail is counted from its header.
    #[test]
    fn the_header_says_how_many_lines_a_broken_tail_held() {
        let tail = ExecTail {
            lines: vec!["a".into(), "b".into()],
            lost: 5,
            ..ExecTail::default()
        };
        let bytes = tail.encode();
        assert_eq!(ExecTail::lines_in_header(&bytes), Some(7));
        assert_eq!(
            ExecTail::lines_in_header(&bytes[..TAIL_HEADER_LEN]),
            Some(7)
        );
        assert_eq!(
            ExecTail::lines_in_header(&bytes[..TAIL_HEADER_LEN - 1]),
            None
        );
        assert_eq!(ExecTail::lines_in_header(b"CSTLOGX1"), None);
        assert_eq!(ExecTail::lines_in_header(b""), None);
    }

    /// The old image's drop accounting continues in the new one, and what did
    /// not fit the old queue's bound is counted as lost (and said so).
    #[test]
    fn the_counters_travel_and_what_did_not_fit_is_lost_and_counted() {
        let sink = BlockingSink::shut();
        let limit = 64 << 10;
        let old = queued(limit, &sink);
        // Fill past the bound with panic messages (they bypass it), then
        // drop an ordinary line: a counter to carry.
        for i in 0..40 {
            old.push_panic(&format!("panic {i} {}", "x".repeat(4_000)));
        }
        log(&old, "an ordinary line, dropped");
        let tail = old.freeze_for_exec();
        assert!(
            tail.lost > 0,
            "the bound holds the memfd: {}",
            tail.lines.len()
        );
        let carried: usize = tail.lines.iter().map(|l| l.len() + 1).sum();
        assert!(carried <= limit, "{carried} bytes of {limit}");
        assert!(tail.dropped_lines >= 1);

        let sink2 = BlockingSink::shut();
        sink2.open();
        let new = queued(limit, &sink2);
        new.adopt_exec_tail(tail.clone());
        assert!(new.flush_final(Duration::from_secs(5)).complete);
        let stats = new.stats();
        assert_eq!(stats.lost_at_previous_exec, tail.lost);
        assert_eq!(stats.dropped_lines, tail.dropped_lines + tail.lost);
        let lines = sink2.lines();
        let said: u64 = lines
            .iter()
            .filter(|l| l.contains("log lines dropped"))
            .map(|l| field(l, "dropped_lines"))
            .sum();
        assert_eq!(said, tail.lost);
        old.unfreeze();
        sink.open();
    }

    const CHILD_ENV: &str = "CST_LOGW_CHILD";
    const PIPE_TEST: &str =
        "log_buffer::tests::a_process_whose_stderr_is_a_pipe_nobody_reads_keeps_logging";

    /// The child of the pipe test: the real sink (`stderr`, a pipe nobody
    /// reads); `CST_LOGW_CHILD` is `queue` or `sync`.
    fn pipe_child(mode: &str) -> ! {
        use tracing_subscriber::layer::SubscriberExt;
        let buffer = LogBuffer::default();
        let config = if mode == "sync" {
            QueueConfig::sync()
        } else {
            QueueConfig::with_limit(1 << 20)
        };
        buffer.start(config, Box::new(StderrSink)).unwrap();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(buffer));
        let started = Instant::now();
        tracing::subscriber::with_default(subscriber, || {
            for i in 0..20_000 {
                let payload = "p".repeat(160);
                tracing::info!(i, %payload, "a line for a pipe nobody reads");
            }
        });
        println!("ELAPSED_MS={}", started.elapsed().as_millis());
        // Exit now: the writer is parked in a write to the full pipe.
        std::process::exit(0);
    }

    /// Re-execute this test binary on [`PIPE_TEST`] with stderr a pipe
    /// nobody reads; the child's `ELAPSED_MS`, or None when it was still
    /// running after `wait`.
    fn run_pipe_child(mode: &str, wait: Duration) -> Option<u64> {
        use std::process::{Command, Stdio};
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", PIPE_TEST, "--nocapture"])
            .env(CHILD_ENV, mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + wait;
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(status.success(), "the child failed: {status}");
        let mut out = String::new();
        std::io::Read::read_to_string(&mut child.stdout.take().unwrap(), &mut out).unwrap();
        Some(
            out.lines()
                .find_map(|l| l.strip_prefix("ELAPSED_MS="))
                .and_then(|n| n.parse().ok())
                .expect("the child prints ELAPSED_MS"),
        )
    }

    #[test]
    fn a_process_whose_stderr_is_a_pipe_nobody_reads_keeps_logging() {
        if let Ok(mode) = std::env::var(CHILD_ENV) {
            pipe_child(&mode);
        }
        let elapsed = run_pipe_child("queue", Duration::from_secs(10))
            .expect("the queued child must exit within 10 s");
        assert!(elapsed < 1_000, "20 000 lines took {elapsed} ms");
        // The naive arm: the synchronous writer is stuck in the full pipe.
        assert!(
            run_pipe_child("sync", Duration::from_secs(3)).is_none(),
            "the synchronous writer finished against a pipe nobody reads"
        );
    }

    #[test]
    fn the_fault_point_parses_and_fires_once() {
        let spec = |ms, once, pid| Some(LogStall { ms, once, pid });
        assert_eq!(parse_log_stall("250"), spec(250, false, None));
        assert_eq!(parse_log_stall("250 once\n"), spec(250, true, None));
        assert_eq!(parse_log_stall("250 once pid=7"), spec(250, true, Some(7)));
        assert_eq!(parse_log_stall("250 pid=7"), spec(250, false, Some(7)));
        for bad in [
            "",
            "0",
            "x",
            "250 onc",
            "250 once once",
            "250 pid=x",
            "250 caller=1",
        ] {
            assert_eq!(parse_log_stall(bad), None, "{bad:?}");
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stall");
        let me = std::process::id();
        assert_eq!(take_log_stall(&path, me), None, "no file, no stall");
        // Without `once` the file stays and fires every time.
        std::fs::write(&path, "40").unwrap();
        assert_eq!(take_log_stall(&path, me), Some(Duration::from_millis(40)));
        assert_eq!(take_log_stall(&path, me), Some(Duration::from_millis(40)));
        // `once` removes it at the first write that sees it.
        std::fs::write(&path, format!("40 once pid={me}")).unwrap();
        assert_eq!(take_log_stall(&path, me), Some(Duration::from_millis(40)));
        assert!(!path.exists());
        assert_eq!(take_log_stall(&path, me), None);
        // Another pid's file is ignored, and left for its owner.
        std::fs::write(&path, format!("40 once pid={}", me + 1)).unwrap();
        assert_eq!(take_log_stall(&path, me), None);
        assert!(path.exists());
    }

    #[test]
    fn the_queue_config_follows_the_environment_variable() {
        let limit = QueueConfig::from_value;
        assert_eq!(limit(None).limit, 16 << 20);
        assert_eq!(limit(Some("")).limit, 16 << 20);
        assert_eq!(limit(Some("0")), QueueConfig::sync());
        assert_eq!(limit(Some("1")).limit, 64 << 10);
        assert_eq!(limit(Some("134217728")).limit, 128 << 20);
        let bad = limit(Some("lots"));
        assert_eq!(bad.limit, 16 << 20);
        assert!(bad.warning.unwrap().contains("lots"));
    }

    #[test]
    fn a_panic_message_goes_through_the_queue_past_the_bound() {
        let sink = BlockingSink::shut();
        sink.open();
        let buffer = queued(64 << 10, &sink);
        log(&buffer, "A");
        assert!(buffer.push_panic("thread 'x' panicked at f.rs:1:1:\nboom"));
        log(&buffer, "B");
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(
            sink.lines(),
            ["A", "thread 'x' panicked at f.rs:1:1:", "boom", "B"]
        );
        // Past the per-process cap they are admitted like WARN lines.
        for _ in 0..MAX_PANIC_RECORDS {
            buffer.push_panic("again");
        }
        assert!(buffer.flush(Duration::from_secs(5)).complete);
        assert_eq!(buffer.stats().dropped_lines, 0);
        assert_eq!(sink.lines().len(), 4 + MAX_PANIC_RECORDS as usize);
        // Sync mode: the caller writes it itself.
        assert!(!LogBuffer::default().push_panic("sync"));
    }
}
