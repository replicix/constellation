# Plan 44 — Non-blocking log writer: logging never blocks a daemon thread

Read `docs/plans/v1/CONVENTIONS.md` first. This plan makes every log
write of a daemon (`mount`, `serve`, a resumed `daemon --resume-from`)
return without waiting for the log's sink. Today each `tracing` event is
written to stderr synchronously by the thread that logged it
(`crates/engine/src/log_buffer.rs:76-99`); a daemonized node's stderr is
`daemon.log` in its state dir, a harness daemon's is its `mount.log`, a
container's is the runtime's pipe. When that sink stalls, every thread
that logs stops with it: the authority core in the middle of a step, a
FUSE worker holding the store's writer lock, the holder's off-core
heartbeat thread.

The maintainer's decision (2026-10-18): **logging must never block a
daemon thread — all threads. When the writer cannot keep up, log lines
are dropped and counted, rather than the caller blocking.** It reverses
the decision of 2026-10-14 (plan 41 §1, "accepted interim guidance": host
mitigations only), on the evidence of §1.

The mechanism (§4): the control ring the writer already feeds and a new
bounded sink queue share one mutex and one `Arc<str>` per line (no
allocation beyond today's); one writer thread per process drains the
queue to stderr in batches. A full queue drops the incoming line
(WARN/ERROR keep a reserved eighth of the queue), counts it, and marks
the gap in the log with one summary line once the writer catches up.
The writer starts only after the daemonizer's fork, is flushed with a
bound at exit, before the handover's `exec` and before an abort, and a
panic's message goes through the same queue. The harness runs its
daemons with a queue large enough never to drop, and fails a run's
verification when any daemon dropped a line (§5).

Rules carried into this plan from the maintainer: consistency and
integrity first, then performance, never one for the other (a trade-off
is reported, not shipped); trace the history before changing a
mechanism and extend its documentation; no backward-compatibility shims
and **no version bumps**; every fix or behaviour change comes with a test
that fails before it (or, for a new mechanism, against the naive variant
it guards against); under host load (CPU starvation, pauses, slow local
disk, slow or jittery S3 and peer links) no errors, failures or
corruption — only latency may degrade, and a load-induced harness
failure is a bug; a change made for one host file system must be neutral
or better on ext4, xfs, btrfs and tmpfs, measured; complex changes get an
independent review before they land.

## Dependencies

- Plan 31 (the engine crate, the control protocol and its schema, the
  handover `exec`, C4b/C5). Committed.
- Plan 21 (daemonization: the fork before any thread exists). Committed.
- The F2 holder heartbeat on a thread of its own (`0228149`) and the
  P2P close before the handover's `exec` (`4537353`). Committed; this
  plan keeps both and removes the log write as a way to stall either.
- Coordinates with plan 41: its lane still never logs (§5.8 there), its
  L1 still moves logging out from under the lock tables' mutex (a log
  call there no longer blocks on the disk, but formatting under the
  mutex still costs); its §1 interim guidance is superseded by this plan
  (edited in the same patch as this plan).
- Supersedes the per-thread "never block" log mode drafted for the
  off-core delegation renewal sender (the investigation of 2026-10-17/18,
  item 5b): with every thread non-blocking, that sender needs no mode of
  its own.

## 1. History

Verified against `main` at `0a4a4ea`.

| When | Commit | What |
|---|---|---|
| 2026-08-30 | `fc0f8bf` (phase 7) | The in-memory log ring for the web UI and `/metrics`: the `tracing` writer pushes each line into a 2 000-line ring and tees the bytes to stderr. |
| 2026-09-09 | `dbc4b78` (plan 21) | Named filesystems and the mount daemon: `mount` forks before any runtime exists; the child's stdout/stderr go to `<state-dir>/daemon.log` (append). |
| 2026-09-29 | `f53b956`, `e026321` (plan 31 C3/C5) | The ring moves to `crates/engine/src/log_buffer.rs`; `node.logs.tail` streams it. The tee is unchanged: `LogWriter::write` locks the ring, then `std::io::stderr().write_all(bytes)` on the calling thread. |
| 2026-10-14 | — (decision) | The maintainer declines a non-blocking log writer "for now"; host guidance instead: `lazytime`/`noatime` on the state dir, logs on a fast disk (plan 41 §1, §11 point 8). |
| 2026-10-16 | `0228149` | The holder's liveness heartbeat moves to its own thread and runtime (`cst-holder-beat`, `crates/engine/src/authority_driver.rs:1339`) so a busy core cannot seal a live holder. Its logging still goes through the synchronous writer. |
| 2026-10-17/18 | — (investigation) | Delegations lost under a co-tenant build on the same btrfs-over-EBS volume (below). |
| 2026-10-18 | — (decision) | Logging never blocks a daemon thread; drop and count. This plan. |

The evidence of 2026-10-17/18 (a diagnostic build that split each slow
core step into its stderr write, its thread CPU time and its run-queue
delay from `/proc/thread-self/schedstat`; `harness` logs on the same
btrfs volume as the daemons' state):

- Of the six core steps of 0.5 s or more logged in 68 loaded runs, five
  were spent in `LogWriter::write` (0.69 s, 0.70 s at the same instant on
  both nodes, 6.85 s, 12.14 s, 12.55 s), one behind the store's writer
  lock; CPU time 110 µs and run-queue delay tens of µs at a load of 35:
  the thread was waiting on the disk, not starved.
- The 12.55 s one lost a delegation by itself (`event="Timer"
  diag_log_write_us=12550665`, no store wait): the delegate's renewal is
  a core step.
- The F2 heartbeat thread blocked 1.555 s (on an iroh `WARN ... failed
  closing path` line its runtime logged) and 7.25 s in stderr writes —
  against a backup's 1.5 s takeover window. A holder whose beat thread
  blocks that long is sealed by its backups.
- Kernel side, the blocked threads sat in `folio_wait_bit_common` (a
  buffered write waiting for the page's writeback: btrfs keeps pages
  stable for its checksums), `wait_log_commit`,
  `btrfs_btree_wait_writeback_range`, `join_running_log_trans`: an
  appending `write(2)` to a file on a busy btrfs can wait seconds.
  `lazytime`/`noatime` do not remove that wait, and "logs on a fast
  disk" is advice an operator may not be able to follow (a container's
  log pipe, a single-volume host).

## 2. What the code does today

### 2.1 The subscriber and the writer

- `crates/cli/src/main.rs:1289-1316` (`main`): one `tracing_subscriber`
  registry for every command: the `IrohNoise` filter layer, then
  `fmt::layer().with_writer(move || log_writer.writer())` under an
  `EnvFilter` (`RUST_LOG`, default `info`, `aws_config=warn` forced). It
  is installed **before** `Cli::parse()` and before `mount`'s
  `daemonize::fork_if_needed` (`main.rs:2667`); the first event
  (`selected host-aware thread plan`, `main.rs:1319`) is logged before
  the fork.
- `tracing_subscriber`'s fmt layer formats each event into a
  thread-local `String` and makes **one** `write_all` per event through
  `make_writer_for(event.metadata())` (`fmt_layer.rs:1049` in 0.3.23):
  `LogWriter::write` receives whole lines, with the event's level
  available to the `MakeWriter`.
- `LogWriter::write` (`log_buffer.rs:76-95`): locks the ring's mutex
  (`.unwrap()`: a poisoned ring panics every later log call), walks the
  bytes one by one, allocates one `String` per completed line
  (`from_utf8_lossy(..).into_owned()`), keeps the last 2 000, unlocks,
  then `std::io::stderr().write_all(bytes)` — unbuffered, under std's
  process-wide stderr lock, so a thread stalled in that write also stalls
  every other thread that logs (they queue on the lock). `flush` flushes
  stderr. No error is ever returned.
- Where stderr goes: `--foreground` (the harness, containers, systemd
  units): whatever the parent gave — the harness's `mount.log` file
  (`crates/harness/src/client.rs:474-476`, `File::create`, rotated to
  `mount.log.<n>` per incarnation), a pipe. Daemonized: the platform's
  `ForkDaemon::detach` (`crates/platform/src/unix.rs:140-168`) forks,
  `setsid`s and `dup2`s an append-mode `daemon.log` onto fd 1 and 2. No
  rotation anywhere in the daemon.
- Other binaries: the CSI driver (`crates/csi/src/main.rs:82`), `chaos`,
  and the harness itself use `tracing_subscriber::fmt()` to stdio
  directly; none is a storage daemon (§10).

### 2.2 Other writes to stderr in a daemon

- No `eprintln!` on a daemon's serving path. The ones in
  `crates/engine/src` and `crates/fs-core/src` are all in `#[cfg(test)]`
  modules; the ones in `crates/cli/src/main.rs` (`:2389-3423`) run on
  the main thread during `mount`'s startup and attach (before serving)
  or in `umount`'s client, and `daemonize::parent_exit` runs in the
  parent CLI process. Left as they are.
- The `SIGUSR2` backtrace handler (`crates/platform/src/unix.rs`,
  `enable_backtraces`) writes a stalled thread's backtrace to stderr from
  the signal handler. Left as it is (§10).
- No panic hook is installed: a panic's message is std's default hook,
  written synchronously to stderr by the panicking thread before it
  unwinds (with every lock it holds still held).
- The control audit trail is a separate file (`FileAuditSink`,
  `crates/control/src/audit.rs:248-265`), written synchronously, one
  line per *mutating* control call; it must never drop and is not part
  of this plan (§10).

### 2.3 Exit, exec, fork

- Exits: `std::process::exit` at `crates/cli/src/node_runtime.rs:711`
  (second signal during shutdown), `handoff_socket.rs:771` (the old
  container image after a committed handoff), `handover.rs:565,600`
  (handover failed, no rollback), plus the CLI's own; `main` returning
  after a clean unmount. `std::process::abort()` at
  `crates/engine/src/node.rs:1222` (the authority driver panicked) right
  after a `tracing::error!`.
- The handover's `exec`: `crates/cli/src/handover.rs:533-548`
  (`exec_body`), reached from the upgrade (`:1025-1026`, after `exec'ing
  the new binary` is logged) and the rollback paths (`roll_back`,
  `:553-600`). The new image's first line is `resuming a handed-over
  daemon` (`:1059`). fd 2 crosses the `exec`.
- The only `fork` without `exec` is the daemonizer's, while the process
  is single-threaded. Every other child (`fusermount3`, the zombie
  reaper, probes) is spawned with `Command` (fork+exec); the `pre_exec`
  closures (`crates/harness/src/client.rs:526`, `daemon_lock.rs`) make
  only syscalls.

### 2.4 Who reads the log, and what depends on it being complete

- **The harness's verification** (`crates/harness/src/verify.rs`, with
  `CONSTELLATION_HARNESS_VERIFY=1`): `parse_logs` (`:1101`) reads every
  incarnation's `mount.log*` for `tagged op executed`, `tagged op
  landed` (`constellation::token_exec`, debug), `granted a lock`
  (`constellation_authority::core::locks`, debug) and `acquired the
  lease`; `check_token_order` and `check_lease_epochs` are safety
  checkers built on those lines. A dropped `granted a lock` or `tagged
  op landed` line silently removes a pair from the token check; a dropped
  `acquired the lease` line removes a node from the split-brain check.
  `check_blind` (`:1199`) already fails on markers that do not parse and
  on executed-without-landed; nothing detects a *missing* line today
  (none could go missing, short of a `kill -9` between an op's commit and
  its log call).
- **The hang watch** (`verify.rs:1522`, `watch_loop`): polls
  `node.status` every 1-5 s; unaffected, gains the drop counter (§5.3).
- **Scenario assertions on log text**: many scenarios `grep` a daemon's
  log (`Client::log_text`, `tail_log`) for lines such as `exec'ing the
  new binary`, `acquired the lease`, `slow core step`, stall reports.
  With the harness's queue size (§4.6) no line is dropped; a line may
  reach the file microseconds later than today, and every scenario
  already waits (`eventually`) or reads after the event it checks.
- **Operators**: `docs/reference/features/named-filesystems.md:218-237`
  (startup phases in `daemon.log`),
  `docs/how-to-guides/operations/diagnose-lease-thrash.md`, the
  troubleshooting sections that name WARN lines. A missing line during a
  stall is visible as the summary line (§4.3) and in status.
- **`node.logs.tail`** (the web UI, `constellation logs`): served from the
  in-memory ring, which keeps every line whether or not the sink got it.

## 3. Requirements

1. No daemon thread waits for the log's sink, for any length of time,
   whatever the sink (a stalled file system, a full pipe nobody reads, a
   terminal that is not read, `/dev/full`).
2. A line that cannot be queued is dropped and counted; the count is in
   `constellation status` and `/metrics`, and the log itself says where
   lines are missing and how many.
3. Lines that are written keep today's order and bytes (the format, the
   ANSI codes, the per-thread order, the global order of acceptance).
4. No allocation per line beyond today's one; no measurable cost in
   `vfs-bench` or the harness `bench`; cheaper than today per line on a
   file sink (§8 measures it).
5. The tail is not lost when the process ends on purpose: exit, the
   handover's `exec`, an abort after a driver panic, a panic — each with
   a bound, so a stalled sink cannot hold the process.
6. The daemonizer's fork stays safe: no writer thread and no queued line
   exist when it forks.
7. The harness's checkers never go silently blind: harness daemons do not
   drop in practice, and any drop fails the run's verification.

## 4. Design

### 4.1 The mechanism: one mutex, one `Arc<str>`, one writer thread

`LogBuffer` keeps its role (the control ring) and gains the sink queue,
under the **same** mutex the ring already takes per line:

```rust
pub struct LogBuffer(Arc<Shared>);          // Clone + Default, as today
struct Shared { state: Mutex<State>, work: Condvar, written: Condvar }
struct State {
    // the control ring, as today, but `Arc<str>` lines
    lines: VecDeque<Arc<str>>, partial: Vec<u8>, total: u64,
    // the sink queue
    mode: Mode,                             // Sync (today) | Queue { limit }
    queue: VecDeque<Arc<str>>,              // capacity pre-reserved: 4096
    queued_bytes: usize, inflight_bytes: usize, inflight_lines: usize,
    writer_idle: bool,
    accepted: u64, written: u64,            // lines, for flush barriers
    episode: Option<Episode>,               // drops since the last summary
    stats: Counters,                        // §4.3
    writer: Option<std::thread::ThreadId>,
}
```

- `LogWriter::write(bytes)`: lock (poison-tolerant:
  `unwrap_or_else(PoisonError::into_inner)` everywhere — a panic while
  logging must not turn every later log call into a panic); split
  `bytes` at `\n` with a slice search, not a byte loop; for each
  completed line build **one** `Arc<str>` (lossy UTF-8, as today; the
  `partial` buffer only when a line spans two calls), push a clone into
  the ring and, in `Queue` mode, admit it to the queue (§4.2). Unlock;
  wake the writer only if it was idle (`writer_idle`, cleared by the
  waker), so a busy writer costs the caller no futex syscall. In `Sync`
  mode, after unlocking, the fault point (§6 W3) and
  `std::io::stderr().write_all(bytes)` exactly as today.
- The writer thread (`cst-log-writer`), one per process: wait on `work`
  while the queue is empty; swap the whole queue out (`mem::swap` with
  its own empty deque, so both keep their capacity: O(1) under the
  lock), move `queued_bytes` to `inflight_bytes`, stamp the write's
  start, unlock; append each line plus `\n` to a reused byte buffer and
  `write_all` it to the sink in chunks of up to 256 KiB (one `write(2)`
  per chunk instead of one per line); relock, advance `written`, clear
  `inflight_*`, record the write's duration, close a drop episode if the
  queue is below half (§4.3), unlock, `written.notify_all()`. Shrink the
  byte buffer and the spare deque back to 64 KiB / 4096 entries after a
  batch that grew them past 1 MiB / 64 Ki entries, so a stall does not
  leave its peak resident. Sink errors (`EPIPE`, `ENOSPC`, `EIO`) are
  ignored as today, and counted.
- Ordering: one queue filled under one mutex in acceptance order, one
  consumer: the file's order is the order in which events entered
  `write`, which preserves each thread's order and is the same global
  order the stderr lock gave before.
- Cost per line (measured, §8): the mutex the ring already took, the one
  `Arc<str>` allocation that replaces the ring's `String`, a memcpy into
  the batch buffer on another thread; the caller no longer makes a
  syscall at all.

### 4.2 Overflow: drop the newest, with a reserve for WARN and ERROR

- The bound is in bytes: each line costs `len + 1 + 64` (the `Arc`
  header, the deque slot and allocator rounding), counted from admission
  until the writer has written it (`queued_bytes + inflight_bytes`), so
  a batch stuck in a stalled `write(2)` still counts and the memory
  bound holds.
- `LogBuffer` implements `tracing_subscriber::fmt::MakeWriter`;
  `make_writer_for(meta)` hands out a `LogWriter` of class `Reserved`
  for `WARN`/`ERROR` and `Ordinary` otherwise. An ordinary line is
  admitted while the queue stays within `limit − limit/8`, a reserved one
  within `limit`: in a storm of info/debug lines behind a stalled sink,
  the last eighth stays free for warnings and errors. Neither ever waits.
- A line that does not fit is **dropped**: the newest, i.e. the incoming
  one. Why not the oldest: dropping the incoming line is O(1) and touches
  nothing the writer may be holding; it keeps the lines that led into the
  stall (usually what explains it); and it leaves one contiguous gap per
  episode, marked exactly where it happened (§4.3). The newest lines are
  still in the in-memory ring (`node.logs.tail`).
- Why not block for ERROR (or anything): that is the blocking this plan
  removes; an error logged by the core during a disk stall is exactly the
  case that cost a delegation.
- A panic's message (§4.4) bypasses the bound, at most 64 per process
  (more are dropped and counted).

### 4.3 The drop counter and the summary line

- Counters (in `State`, read by status): `dropped_lines`,
  `dropped_bytes`, `drop_episodes`, `queued_bytes_max` (high-water mark),
  `written_lines`, `longest_write_ms` (the longest single sink write: a
  slow disk shows here before it shows anywhere else), `writing_for_ms`
  (the write in progress now, 0 when idle), `sink_errors`,
  `unwritten_at_previous_exec` (§4.4).
- An **episode** opens at the first drop (its unix ms, lines, bytes) and
  closes when the queue is back under `limit/2` — checked by the caller
  that admits a line and by the writer after each batch, so a storm that
  ends and a log that goes quiet both close it. Closing enqueues, ahead
  of whatever comes next, one line formatted on the spot in the fmt
  layer's default layout (its timestamp through
  `tracing_subscriber::fmt::time::SystemTime`, no ANSI):

  ```text
  2026-10-18T10:15:42.123456Z  WARN constellation::log: log lines dropped: the log sink did not keep up dropped_lines=1834 dropped_bytes=402117 first_dropped_unix_ms=1792318530000 queue_limit_bytes=16777216
  ```

  It is not a `tracing` event: it bypasses `RUST_LOG` (a drop must be
  visible whatever the filter) and cannot recurse into the writer. It is
  pushed into the ring too. The `limit/2` hysteresis keeps a sustained
  overload from alternating one summary per accepted line.
- At a flush (§4.4) an open episode is closed first, so the summary is
  part of what the flush writes.

### 4.4 Lifecycle: start after the fork; flush at exit, exec, abort; panics

- **Sync until started.** `LogBuffer::default()` is in `Sync` mode:
  exactly today's behaviour. Every short-lived CLI command stays there
  (its stderr is a terminal or a script's pipe, and its `eprintln!`
  output stays interleaved in order with its log lines). The daemons
  switch at the top of `cmd_mount_body` (`main.rs:2838`, reached by both
  `--foreground` and the daemonized child, **after**
  `fork_if_needed`), `serve::cmd_serve` (`serve.rs:131`) and
  `handover::resume_main` (`handover.rs:1031`), through one helper,
  `start_daemon_logging(&log_buffer)` in `main.rs`: `start` the writer
  (sink: `std::io::stderr()`, which follows the detach's `dup2`),
  register the buffer as the process's (`log_buffer::set_process`), the
  panic hook, and `libc::atexit(flush_at_exit)` (the engine has no
  `libc`, by design; the hook lives in `cli`). `start` is idempotent.
  A `mount` that only attaches to a running daemon also starts it and
  exits soon after: harmless.
- **Fork safety.** The writer never exists before `fork_if_needed`, and
  `Sync` mode keeps no queued line, so the forked child inherits neither
  a dead writer nor lines it would write twice. `fork_if_needed` gets a
  `debug_assert!(!log_buffer::process_started())` and its comment
  (`daemonize.rs:92-96`) is rewritten. No log call is allowed in a
  `pre_exec` closure (it could take the log mutex another thread held at
  the fork): a comment at `Client::sandbox` and `daemon_lock.rs`'s
  `CommandExt` uses.
- **Flush barrier.** `LogBuffer::flush(within) -> Flush { complete,
  unwritten_lines }`: close an open episode, take `target = accepted`,
  wait on `written` until `written >= target` or the deadline. Lines
  logged after the call are not waited for. Called on the writer thread
  itself it returns at once (incomplete). `log_buffer::flush_process`
  does it on the registered buffer (no-op when none).
- **Exit.** `flush_at_exit` (an `extern "C" fn` under `catch_unwind`) runs
  from `exit(3)`, so it covers `std::process::exit` anywhere and a
  return from `main`: `flush_process(1 s)`. One second: an exit is not on
  a latency path, but the second-signal exit (`node_runtime.rs:711`)
  promises to be prompt and a container restart waits for it.
- **Exec.** `exec_body` (`handover.rs:533`) calls `flush_process(200 ms)`
  right before `Command::exec`; when lines are left it sets
  `CONSTELLATION_LOG_UNWRITTEN_AT_EXEC=<n>` on the command. The new
  image (`resume_main`, after starting its writer) reads it, keeps it in
  `unwritten_at_previous_exec` and logs one WARN: `the previous image
  left N log line(s) unwritten at its exec (its log sink was stalled)`.
  200 ms: the handover's gap is application-visible latency; a stalled
  sink may add at most that, and the loss is reported. (A writer thread
  inside a `write(2)` in uninterruptible sleep delays `execve`'s
  thread teardown until the write returns — the kernel's, not ours; today
  the exec'ing thread's own last log write has the same exposure. §11.)
- **Abort.** `node.rs:1222`: `log_buffer::flush_process(500 ms)` between
  the `tracing::error!` and `std::process::abort()`, so the line that
  says why survives a healthy sink.
- **Panics.** The hook (installed by `start_daemon_logging`; it takes
  std's hook with `take_hook` and keeps it only for the fallback below)
  formats what std's
  default hook prints — `thread '<name>' panicked at <location>:\n<msg>`,
  then the backtrace when `std::backtrace::Backtrace::capture()` captured
  one (`RUST_BACKTRACE`), else std's `note: run with RUST_BACKTRACE=1 ...`
  line — and pushes it into the queue past the bound (§4.2). It does not
  wait: the panicking thread may hold locks others need. If the
  panicking thread is the writer itself, or no buffer is registered, it
  calls std's previous hook (direct stderr). A panic that ends the
  process reaches `exit` (main) or the abort above, which flush.
- **Signals.** `SIGTERM`/`SIGINT` end through the clean shutdown and
  `exit`. `SIGKILL`, `SIGSEGV` and a double-panic abort lose what is
  queued — on a sink that keeps up, the lines of the last few
  microseconds; behind a stalled sink, the stall's backlog (§5.4 for the
  harness).

### 4.5 Configuration

- `CONSTELLATION_LOG_QUEUE_BYTES`: the queue's bound in bytes, read once
  at `start`. Default **16 MiB** (16 777 216): at the info level a busy
  node writes a few KB/s (the kept logs of 2026-10-17: 0.5-3 KB/s at
  info, 18 KB/s with every target at debug), so 16 MiB holds minutes of a
  stall at normal rates and seconds of a 1 MB/s storm; it is allocated
  only while lines wait. Values below 64 KiB are raised to 64 KiB; an
  unparsable value keeps the default and logs one WARN after the start.
  **`0` keeps the synchronous writer** (today's behaviour, a slow disk
  stalls the daemon): for A/B measurements and the fail-before arms
  only, documented as such.
- The constants (reserve `1/8`, close at `1/2`, line overhead 64 B, exit
  1 s, exec 200 ms, abort 500 ms, 64 panic records, 256 KiB write chunk)
  are not knobs.

### 4.6 The harness's setting

- `Client::cmd` (`crates/harness/src/client.rs:263`) sets
  `CONSTELLATION_LOG_QUEUE_BYTES=134217728` (128 MiB) for every daemon,
  before the client's own `env` (a scenario may override it, §7.3).
  128 MiB against the worst stall measured (12.55 s) needs a sustained
  10 MB/s of logging to overflow; the verification's debug targets under
  `load-probe` are expected well below 1 MB/s — W5 measures the peak
  (`queued_bytes_max` across the campaign cells) and the plan's exit
  requires at least 4× headroom, else the default is raised. Memory is
  only taken while lines wait.
- Not `0` (synchronous) in the harness: the loaded harness runs are where
  the blocking was found; running the harness on the writer production
  does not use would hide exactly this class of failure.

### 4.7 Alternatives considered

- **`tracing_appender::non_blocking` (lossy).** Not a dependency today; it
  copies each line into its own `Vec<u8>` (one more allocation per line,
  on top of the ring's), bounds the channel in lines (128 000 by
  default), not bytes; spawns its worker when constructed (before our
  fork, so dead in the daemon child, unless built after the fork beside a
  pre-fork writer); flushes only from `WorkerGuard`'s `Drop`, which
  `process::exit` and `exec` skip; has no level reserve and no in-log
  summary (only `error_counter()`); and the ring would still need its own
  per-line lock. A small custom queue (~300 lines) fits better.
- **A per-thread never-block mode** for the threads that must not stall
  (the investigation's item 5b): the evidence has the core, FUSE workers
  and tokio workers blocked too; the decision covers all threads.
- **Drop the oldest.** §4.2.
- **Block (or wait a bounded time) for WARN/ERROR.** §4.2.
- **`O_NONBLOCK` on fd 2.** Regular files ignore it (a buffered write to a
  busy btrfs still waits); on a pipe it splits lines at `EAGAIN`; and the
  descriptor's flags are shared with the parent (the harness, a shell).
- **A separate logger process** (the daemon writes into a pipe, a child
  copies it to the file): it would also survive `kill -9`, but the
  in-process side still needs the queue when the pipe fills, and it adds
  a process to supervise across the handover. Possible follow-up (§12).
- **journald / syslog.** An operator's choice of sink; the daemon must not
  block on any of them either, which this plan gives for free.

## 5. The harness: verification never goes silently blind

### 5.1 No drops in harness runs

The 128 MiB queue (§4.6). Any drop is a finding, not noise: it fails
verification (5.2, 5.3), and the scenario that drops on purpose
(`log-sink-stall` part B) declares it (§7.3).

### 5.2 `check_blind` reads the summary line

`parse_logs` gains a fifth marker, `log lines dropped`, counted in
`LineCounts::dropped` (raw and parsed, so a format drift is caught like
the others) with its `dropped_lines=` per log label in a new
`LockEvidence::dropped: Vec<(String, u64)>`. `check_blind` adds, per
entry: `checker blind: <label>: <n> log line(s) dropped (the daemon's log
sink did not keep up; status log.dropped_lines): the token, grant and
epoch checks cannot see them` — a violation, unless the log was declared
by `verify::expect_log_drops(&client)` (then it is reported and skipped).

### 5.3 The hang watch reads the counter

`watch_loop` already polls every daemon's `node.status`: a nonzero
`log.dropped_lines` adds the same violation once per pid (this covers a
daemon killed before its summary line reached the file), and a status
without a `log` section is schema drift (`watch_blind`).

### 5.4 The kill -9 tail

A `kill -9` loses what is queued. Today the same blind spot exists for an
op killed between its commit and its log call (a window of
microseconds); with the queue it is as long as the writer's lag —
microseconds on a sink that keeps up, the backlog behind a stalled one.
So, with verification on, `Client::kill9` and `kill9_within` first
call `node.logs.flush` with `within_ms: 500` (control timeout 1.5 s),
unless the daemon is stopped (`crate::load::proc_state == 'T'`: a
`SIGSTOP`ped daemon cannot answer) — one round trip on a healthy sink.
A complete flush leaves the same microsecond window as before. An
incomplete one, a failed call or a stopped daemon is printed (`verify:
b's mount.log.1 may lack up to N line(s) logged before its kill -9
(<why>)`) and counted in the scenario's verify summary as a partial tail
— a note, not a violation: a killed process's last lines are not a
property of the daemon, and failing on them would make a load-injected
run fail on the checker's own limit. The token check over that
incarnation's earlier lines is unaffected.

### 5.5 A break mode proves the check

`load-probe`'s `CONSTELLATION_HARNESS_LOAD_PROBE_BREAK` gains `log-drop`:
after the run, a forged summary line (`... WARN constellation::log: log
lines dropped: ... dropped_lines=3 ...`) is appended to a's log; the run
must fail naming `log line(s) dropped`.

## 6. Milestones, files and functions

Order: W1, W2, W3, W4, W5. Each milestone builds, passes fmt/clippy and
its tests before the next starts; W1-W3 get one independent review
together before W5's campaign.

### W1 — The queue (engine)

- `crates/engine/Cargo.toml`: `tracing-subscriber.workspace = true`
  (for `MakeWriter` and the summary's timestamp; the workspace already
  builds it for `cli`).
- `crates/engine/src/log_buffer.rs`, rewritten (module doc: §4's rules,
  why the newest is dropped, the bounds and why):
  - `LogBuffer` as §4.1; `tail`, `tail_at`, `since` keep their
    signatures (`Vec<String>`, built from the ring's `Arc<str>`s).
  - `impl<'a> MakeWriter<'a> for LogBuffer` (`make_writer` → `Ordinary`,
    `make_writer_for` → by level).
  - `pub struct QueueConfig { limit: usize }` with `from_env()` (§4.5)
    and `sync()`.
  - `pub fn start(&self, config: QueueConfig, sink: Box<dyn Write +
    Send>) -> std::io::Result<()>` (idempotent; `Sync` config: no
    thread).
  - `pub fn flush(&self, within: Duration) -> Flush`;
    `pub struct Flush { pub complete: bool, pub unwritten_lines: u64 }`.
  - `pub fn stats(&self) -> LogStats` (the §4.3 counters plus `mode`,
    `limit`, `queued_bytes`, `queued_lines`).
  - `pub(crate) fn push_panic(&self, text: String)` (§4.4).
  - `#[cfg(test)] fn with_sync_sink_for_test(sink)`: `Sync` mode writing
    to the given sink instead of stderr (the naive arms of §7.1).
  - Process registration: `pub fn set_process(buf: &LogBuffer)`
    (`OnceLock`), `pub fn process_started() -> bool`,
    `pub fn flush_process(within: Duration) -> Flush`,
    `pub fn install_panic_hook()`,
    `pub fn note_unwritten_at_exec(n: u64)`.
  - The fault point `CONSTELLATION_FAULT_LOG_STALL_FILE=<path>`: file
    content `<ms>[ once][ pid=<n>]` (the store-stall grammar,
    `crates/meta/src/store/gate.rs:217-240`; a small parser here, the
    path cached in a `OnceLock`, one cached `None` when unset): while
    the file exists, each sink write first sleeps `<ms>` **holding
    std's stderr lock** (`stderr().lock()`, then the sleep, then the
    write) — on the writer thread in `Queue` mode, on the logging thread
    in `Sync` mode, where every other thread that logs then queues on
    that lock exactly as behind a stalled `write(2)` today (the
    fail-before arms); `once` removes it. Never logs.
- Unit tests in the module (§7.1).
- `crates/engine/src/node.rs:1222`: flush before the abort (§4.4).

### W2 — Wiring (cli, control)

- `crates/cli/src/main.rs`: the subscriber's writer becomes
  `.with_writer(log_buffer.clone())`; `fn start_daemon_logging(buf:
  &LogBuffer)` (§4.4) with `extern "C" fn flush_at_exit()` and the
  `atexit` registration; calls at the top of `cmd_mount_body`.
- `crates/cli/src/serve.rs` (`cmd_serve`), `crates/cli/src/handover.rs`
  (`resume_main`: start, then read `CONSTELLATION_LOG_UNWRITTEN_AT_EXEC`;
  `exec_body`: the flush and the variable; factor the pre-`exec` step as
  `fn before_exec(cmd: &mut Command, flush: impl FnOnce(Duration) ->
  Flush)` so it is unit-testable).
- `crates/cli/src/daemonize.rs`: the `debug_assert!` and the comment
  (§4.4).
- `crates/control/src/proto/types.rs`: `StatusReport.log: LogStatus`
  (last field; doc comments per field: what a nonzero value means for an
  operator), `LogFlushParams { within_ms: u64 }` (capped at 10 000),
  `LogFlushReport { complete: bool, unwritten_lines: u64, dropped_lines:
  u64 }`.
- `crates/control/src/methods.rs`: `NodeLogsFlush { name:
  "node.logs.flush", role: Viewer, mutating: false, stream: None, params:
  LogFlushParams, result: LogFlushReport }`; the role table in the module
  doc (`:40`).
- `crates/engine/src/control/mod.rs`: register it (the wait runs in
  `tokio::task::spawn_blocking`, never on a worker);
  `crates/engine/src/control/service.rs` (`status()`): fill `log` from
  `self.log_buffer.stats()`; `crates/engine/src/control/parity_tests.rs`:
  the new method's params.
- `crates/control/src/web.rs` (`render_metrics`):
  `constellation_log_dropped_lines_total` (counter) and
  `constellation_log_queued_bytes` (gauge); `crates/engine/src/control/ops.rs`'s
  metrics test.
- Schema: `CONSTELLATION_BLESS=1 cargo test -p constellation-control
  schema`.

### W3 — Harness

- `crates/harness/src/client.rs`: the default queue size in `cmd`
  (§4.6); the kill barrier in `kill9`/`kill9_within` (§5.4) through
  `crate::verify::note_kill_tail`.
- `crates/harness/src/verify.rs`: §5.2, §5.3, §5.4 (`ScenarioState`
  gains `partial_tails` and `expected_drops`; the scenario summary line
  prints partial tails), `expect_log_drops`; module doc.
- `crates/harness/src/load.rs`: load item `log-stall:<ms>[:every_ms]`
  (`Kind::LogStall`, preset `log-stall12` = `log-stall:12000:20000`):
  `daemon_env` sets `CONSTELLATION_FAULT_LOG_STALL_FILE` to
  `load_dir(work)/log-stall`; the injector writes `<ms> once pid=<pid>`
  every `every_ms` (whole, by rename, as `spawn_store_stall` does). It is
  **not** a probe-blind injector: a log stall must cost the workload no
  latency, so the latency bounds apply.
- `crates/harness/src/scenarios/logsink.rs` (new), registered in
  `scenarios.rs` and listed in `crates/harness/src/k8s/parity.rs` (the
  catalog test requires every name; skip reason: needs the fault file on
  the node's host): `log-sink-stall` (§7.3).
- `crates/harness/src/scenarios/campaign.rs`: the `log-drop` break
  (§5.5).
- `crates/harness/src/scenarios/handover.rs` (`upgrade_under_load`): after
  the three upgrades, the log has, in order, three `exec'ing the new
  binary` lines each followed by a `resuming a handed-over daemon` line
  and no `unwritten at its exec` warning (the idle-sink case of §7.4).

### W4 — Documentation

- `docs/reference/configuration.md`: a "Logging" subsection under
  "Environment variables" (`CONSTELLATION_LOG_QUEUE_BYTES`, the drop
  policy in two sentences, `RUST_LOG` pointer); the fault table row for
  `CONSTELLATION_FAULT_LOG_STALL_FILE`.
- `docs/reference/features/named-filesystems.md` (`daemon.log`,
  `:218-237`): logging never blocks the daemon; lines may be dropped
  behind a stalled disk and the `log lines dropped` line says how many;
  `constellation status`'s `log` section; the host guidance that still
  helps (`lazytime`/`noatime`, the log on a device that does not stall:
  fewer drops, nothing else).
- `docs/how-to-guides/operations/diagnose-lease-thrash.md` and the
  troubleshooting sections that tell operators to read `daemon.log`: a
  `log lines dropped` line or `log.longest_write_ms` in seconds means the
  disk under the log stalls; the daemon kept running.
- `docs/how-to-guides/development/TESTING.md`: the load item, the
  scenario, the break mode, the harness's queue size, `check_blind`'s new
  rule, the kill-9 barrier and partial tails.
- `docs/explanation/DECISIONS.md`: **ADR-32: Logging never blocks a
  daemon thread** (context §1, decision §4.1-4.4, consequences: lines
  can be lost behind a stalled sink, counted and marked; the harness's
  stance §5; the 2026-10-14 decision it reverses).
- `docs/plans/v1/PROGRESS.md`: a `## Fix:`-style entry with the
  before/after numbers (§8) and the gate results.

### W5 — Measurements and the campaign

§8 and §9.

## 7. Tests

### 7.1 Unit tests (`crates/engine/src/log_buffer.rs`)

A test sink: `BlockingSink`, a `Write` whose `write` blocks on a
`Mutex<bool>`/`Condvar` gate until the test opens it, and records what
it got.

1. **`a_stalled_sink_never_blocks_the_logging_thread`** (the fail-before,
   in-process): `QueueConfig { limit: 64 KiB }`, the sink gated shut;
   10 000 writes of 200-byte lines from 4 threads return in under 200 ms
   in all; `dropped_lines > 0`, `dropped_lines + accepted == 10 000`.
   The **naive arm** in the same test: the same writes in `Sync` mode
   through the fault point (`CONSTELLATION_FAULT_LOG_STALL_FILE` cannot
   be set per test — use the sink-injecting constructor
   `LogBuffer::with_sync_sink_for_test(sink)`, which makes `Sync` mode
   write to the given sink instead of stderr) do not finish within
   500 ms (a thread is left blocked, released by opening the gate).
2. **`drops_leave_one_marked_gap_in_order`**: gate shut, limit 64 KiB,
   log lines `0..N`; open the gate; flush; the sink holds a prefix
   `0..k`, then exactly one `log lines dropped` line whose
   `dropped_lines` equals `N' − k` for the dropped run, then the lines
   accepted after the episode closed, all in increasing order; the stats
   agree with the line.
3. **`warnings_use_the_reserve`**: queue filled to `limit − limit/8` with
   info lines behind a shut gate: the next info line is dropped, a WARN
   line is accepted; at `limit` both are dropped.
4. **`flush_waits_for_what_came_before_and_is_bounded`**: gate shut,
   `flush(100 ms)` returns `complete: false` with the queued count in
   100-150 ms; open the gate, `flush(5 s)` is complete; a flush on the
   writer thread returns at once.
5. **`the_ring_keeps_every_line_and_the_same_text`**: `tail()` after
   dropped lines has them all; a line split over two `write` calls is
   one line in both the ring and the sink; the sink's bytes equal the
   bytes given (minus the dropped lines).
6. **`a_poisoned_buffer_keeps_logging`**: a thread panics while holding
   the state lock (test hook); later writes and `tail` work.
7. **`memory_is_bounded_and_returned`**: behind a shut gate,
   `queued_bytes + inflight_bytes ≤ limit` at all times (including a
   batch stuck in the sink); after the gate opens and a flush, the
   writer's buffers are back to their idle capacities.
8. **`a_process_whose_stderr_is_a_pipe_nobody_reads_keeps_logging`** (the
   real sink): the test re-executes its own binary
   (`std::env::current_exe()`, `--exact <this test> --nocapture`, env
   `CST_LOGW_CHILD=1`) with `stderr(Stdio::piped())` that the parent never
   reads. The child installs a subscriber on a `LogBuffer` started with a
   1 MiB queue on `std::io::stderr()`, logs 20 000 lines (about 4 MB, far
   past the pipe's 64 KiB), prints `ELAPSED_MS=<n>` on stdout and exits.
   The parent requires the child to exit within 10 s and `ELAPSED_MS <
   1000`. Naive arm: the same child with `QueueConfig::sync()` is still
   running after 3 s (then killed). Measured on the prototype: the
   synchronous writer never finishes; the queue logs 20 000 lines in
   28 ms.
9. **`the_fault_point_parses_and_fires_once`**: the grammar; `once`
   removes the file; `pid=` other than ours is ignored.

### 7.2 cli tests

1. `before_exec_flushes_with_a_bound_and_reports_the_rest`
   (`handover.rs`): a buffer behind a shut gate: `before_exec` returns
   within 200-260 ms and sets `CONSTELLATION_LOG_UNWRITTEN_AT_EXEC` to the
   unwritten count; with an open gate the variable is not set.
2. `a_panic_reaches_the_log_in_order` (child-process test as 7.1.8,
   stderr to a temp file): log `A`, panic in a spawned thread, log `B`,
   exit: the file has `A`, `thread '<name>' panicked at`, `B` in that
   order, once each.
3. `exit_flushes_the_tail` (child-process): a 2 s fault stall on the
   first batch, 1 000 lines, `std::process::exit(0)`: the child exits in
   1.0-1.3 s; with no stall every line is in the file.
4. The control parity test and the schema test (`node.logs.flush`,
   `status.log`).

### 7.3 Harness scenario `log-sink-stall`

New file `crates/harness/src/scenarios/logsink.rs`; verification on.

- **Part A (no drops, nothing stalls):** two nodes, `--locks`; `b`
  mounted first (root, lease holder), `a` its backup (wait until `b`
  lists it, as `load-probe-takeover` does). `b` runs with
  `RUST_LOG=info,constellation_authority::core::locks=debug` (its core
  logs on every renewal); a writer on `b` holds an `flock` on `/lk` and
  appends + `fsync`s every 100 ms; the log sink of `b` is stalled 12 s
  three times, 15 s apart (`CONSTELLATION_FAULT_LOG_STALL_FILE`,
  `12000 once`). Asserts: `a`'s `backup_takeovers` +0 and `b`'s lease
  epoch unchanged; the writer saw no error and every append+`fsync` took
  under 2 s; `b`'s `log.longest_write_ms` ≥ 11 000 (the stall happened:
  else the scenario proved nothing and fails); `log.dropped_lines == 0`
  (128 MiB); after the last stall and a `node.logs.flush`, `b`'s log has
  lines stamped inside each stall window (they were queued, not lost).
- **Part B (drops, counted and marked):** a lone node `c` with
  `CONSTELLATION_LOG_QUEUE_BYTES=65536`, `RUST_LOG=debug` for a chatty
  target, declared with `verify::expect_log_drops`; one 5 s stall:
  `log.dropped_lines > 0`, the log holds `log lines dropped` lines whose
  `dropped_lines` sum to the status counter, and `drop_episodes` equals
  their number; FUSE ops during the stall within the scenario's latency
  bound.
- **Part C (the delegation of the evidence):** `delegroot.rs`'s
  `delegated_pair` (made `pub(crate)`): `/d1` delegated to `b`, its
  scenario `ENV` (5 s grants, about 3.5 s of renewal slack); a writer on
  `b` in `/d1`; `b`'s sink stalled 12 s twice: `b`'s generation
  unchanged, `a`'s `reclaimed`/`recalls_sent` +0, `b`'s `lapsed` +0, no
  write error, `b`'s `log.longest_write_ms` ≥ 11 000.
- **Control arm** (`CONSTELLATION_HARNESS_LOG_SINK_STALL_CONTROL=1`, run
  once per gate, §9): parts A and C with the stalled node at
  `CONSTELLATION_LOG_QUEUE_BYTES=0` (the synchronous writer; the fault
  then sleeps in the logging thread, as a stalled disk does today). They
  must **fail** their assertions (a seal or takeover of `b`, or the
  delegation reclaimed); a control arm that passes fails the scenario
  ("the stall did not reach a logging thread: the scenario proves
  nothing").

### 7.4 Handover

- `upgrade-under-load` with the order check (W3), idle.
- `upgrade-under-load` under `CONSTELLATION_HARNESS_LOAD=log-stall:3000:4000`:
  passes as today (zero errors, every write lands); each upgrade's gap
  (the scenario already measures it) grows by at most 250 ms over the
  idle runs; where a stall overlapped an `exec`, the new image's
  `unwritten at its exec` warning is present and its count equals the
  status `log.unwritten_at_previous_exec`.

### 7.5 Verification itself

- `load-probe` with `CONSTELLATION_HARNESS_LOAD_PROBE_BREAK=log-drop` fails
  naming the drop (§5.5); `verify.rs` unit tests: a log with a summary
  line is blind, an unparsable summary line is format drift, an
  `expect_log_drops` log is reported and not a violation, a status with
  `log.dropped_lines > 0` is a violation in `watch_blind`'s neighbour.

## 8. Performance

Prototype of §4.1-4.2 (standalone, `tracing-subscriber` 0.3.23, the same
fmt layer and a copy of today's `LogWriter` for A/B; an event of six
fields, ~250 bytes formatted):

| Host, sink | today: per event | queue: per event | allocs/event (both) |
|---|---:|---:|---:|
| dev host, file on zfs, 1 thread | 2.7-2.9 µs | 1.13-1.17 µs | 1.000 |
| dev host, `/dev/null`, 1 thread | 1.34 µs | 1.34 µs | 1.000 |
| dev host, file, 8 threads flat out | 4.5-4.7 µs | 0.67-0.78 µs | 1.000 |
| Linux test host (btrfs on EBS), file, 1 thread | 1.9-2.1 µs (worst 0.27-0.35 ms) | 1.14 µs (worst 18 µs) | 1.000 |
| test host, file, 8 threads flat out (1.6 M lines/s) | 1.9-2.1 µs (worst 0.35 ms) | 0.55-0.61 µs (worst 2.85 ms) | 1.000 |
| test host, file, 8 threads at 80 k lines/s | worst 50-121 µs | worst 31-69 µs | 1.001 |
| dev host, pipe nobody reads, 20 000 lines | never finishes (killed at 10 s) | 1.4 µs; 28 ms in all | 1.000 |

The output is byte-identical modulo timestamps (51 000 lines, same md5
with the timestamps removed). The one worse number, the worst single
event with 8 threads logging flat out (1.6 M lines/s, three orders of
magnitude past any daemon's rate), is the queue's `VecDeque` growing
under the mutex while callers outrun the writer; W1 pre-reserves 4096
entries and keeps both deques' capacity across swaps, and W5 re-measures
it. At realistic rates the queue's worst case is below today's.

Gates (W5): `vfs_bench` (no subscriber installed: unchanged by
construction, run to show it); `tests/perf-gate.sh`'s harness `bench`
(3 repetitions, against `tests/perf-baseline.json`: no regression); a
`log_writer` micro-bench in `crates/engine/benches/` (the prototype's
three modes, the counting allocator of `vfs_bench`), required: allocs per
line ≤ the synchronous writer's, per-line cost on a file sink ≤ it, on
ext4, xfs, btrfs and tmpfs (the sink's file system is the only one
involved). Record the numbers in PROGRESS.

## 9. Gates

In order, all green:

1. `cargo fmt --all` (no diff), `cargo clippy --workspace --all-targets
   -- -D warnings`.
2. `cargo test --workspace` (the new unit and child-process tests; the
   control schema and parity tests).
3. `bash tests/smoke.sh`, `bash tests/integration.sh`.
4. `tests/perf-gate.sh` (vfs-bench and the harness bench), and §8's
   micro-bench on the four file systems.
5. `target/release/harness run` — the full matrix, every scenario
   PASSED, `log-sink-stall` included (its control arm run once with
   `CONSTELLATION_HARNESS_LOG_SINK_STALL_CONTROL=1`: the arm fails as
   required, the scenario passes).
6. pjdfstest (`docker compose --profile test run --rm compliance`):
   8798/8798.
7. Loaded, verification on (`CONSTELLATION_HARNESS_VERIFY=1`, the lock
   targets at debug, node logs kept), on the Linux test host:
   - `load-probe-takeover` under `pause:lock-lapse+500:10000@b` (the
     established cell), seeds 42..61: no regression against the latest
     PROGRESS numbers, zero drops.
   - the same cell with `,log-stall:12000:20000@b` added, seeds 42..61:
     no failure, no `[bug]` errno, zero drops, no `log lines dropped`
     line, latency verdicts as in the cell without the log stall.
   - `delegate-handoff-renewal` ×20 under the co-tenant disk load of the
     2026-10-17 investigation (a build writing GBs to the same volume)
     and ×20 under `log-stall:12000:15000@b`: zero delegations lost to a
     core step stalled in a log write (`slow core step` lines whose step
     did not wait on the store: none), `cst-holder-beat` never sealed.
   - `upgrade-under-load` idle and under `log-stall:3000:4000` (§7.4).
   - `load-probe` with the `log-drop` break: fails naming it.
   - Record per cell the peak `log.queued_bytes_max` (headroom ≥ 4× the
     harness's 128 MiB, §4.6).

## 10. What not to touch

- The log **format**: no new fields in ordinary lines, no change of
  `RUST_LOG` handling, `IrohNoise`, the `aws_config=warn` override, ANSI.
- The control audit trail (`FileAuditSink`): synchronous, never drops;
  it records only mutating control calls. A follow-up may move it off
  the tokio worker, never make it lossy.
- The `SIGUSR2` backtrace handler (signal context: no mutex, no queue).
- The CSI driver, `chaos`, the harness's own logging: not daemons of the
  file system.
- Short-lived CLI commands (they stay synchronous, §4.4) and the
  `eprintln!`s of `mount`'s startup and `umount`'s client.
- Plan 41's lane rules (it still never logs) and L1's move of logging out
  from under the lock tables' mutex.
- The handoff format (`DaemonHandoff`): the unwritten count travels in
  the `exec`'s environment, not in the handoff.
- No log rotation, no journald integration, no per-target queues.

## 11. Risks

- **Lines lost to `kill -9`, `SIGSEGV`, OOM-kill.** Bounded by the queue's
  occupancy: empty within microseconds on a sink that keeps up. The
  harness's barrier and partial-tail note (§5.4).
- **A stalled `write(2)` delays `execve` and process exit.** The writer
  thread in an uninterruptible write holds up the kernel's thread
  teardown at `exec` and at exit; the flush bounds only our wait. Today
  the logging thread itself has the same exposure; no regression.
  Recorded, not solvable without moving the write out of the process
  (§4.7's logger process).
- **Memory.** 16 MiB (128 MiB in the harness) at most while a sink
  stalls, returned after; counted per line with its overhead.
- **Silent loss.** Every drop is counted in status and `/metrics`, and
  marked in the log once the writer catches up; a sink that never comes
  back shows `writing_for_ms` growing in status.
- **Order across a panic.** A panic's message is queued with the rest,
  in order; it no longer appears on stderr ahead of earlier queued lines.
- **The writer thread dies** (it cannot panic short of an allocation
  failure): lines are then queued until the bound and dropped; status
  shows `writing_for_ms` 0 with `queued_bytes` at the limit. A test hook
  covers the panic-in-writer path of the panic hook.

## 12. Open questions for the maintainer

1. Default queue size 16 MiB (proposed) — or smaller for small hosts.
2. `CONSTELLATION_LOG_QUEUE_BYTES=0` keeps the synchronous writer as an
   A/B and test switch (proposed) — or no synchronous mode at all once
   the plan's fail-before evidence is recorded.
3. The `kill -9` tail in the harness: a note (proposed, §5.4) — or a
   violation, accepting failures under load injectors that stall the
   sink at a kill.
4. A logger process (survives `kill -9`, takes the disk wait out of the
   process entirely) as a follow-up — or not.

## 13. Exit criteria

- [ ] W1: the queue, the reserve, the summary line, the flush, the fault
      point; unit tests 7.1.1-7.1.9 with their naive arms.
- [ ] W2: the daemons start the writer after the fork; exit, exec, abort
      and panic paths flush or queue as §4.4; status `log`,
      `node.logs.flush`, metrics, schema re-blessed; tests 7.2.
- [ ] W3: the harness's 128 MiB, `check_blind` and the watch on drops,
      the kill barrier, the `log-stall` load item, `log-sink-stall` with
      its control arm, the `log-drop` break, the upgrade order check.
- [ ] W4: configuration, named filesystems, troubleshooting, TESTING,
      ADR-32, PROGRESS.
- [ ] W5: §8's measurements on four file systems, every gate of §9, the
      peak queue occupancy per campaign cell recorded.
- [ ] Independent review of W1-W3 before W5's campaign.

## 14. PROGRESS.md

One entry, `## Fix: logging never blocks a daemon thread (plan 44)`: the
evidence of §1 (numbers only), the design in five lines, the fail-before
results (unit tests' naive arms, the scenario's control arm), §8's
before/after table on each file system, the campaign cells with seeds and
verdicts, the peak queue occupancy, and the residual risks of §11.

## Sources

- In-repo: `crates/engine/src/log_buffer.rs`, `crates/cli/src/{main,daemonize,handover,serve,node_runtime,handoff_socket}.rs`,
  `crates/platform/src/unix.rs` (`ForkDaemon::detach`, `enable_backtraces`),
  `crates/engine/src/{node,authority_driver}.rs`, `crates/engine/src/control/{mod,service,streams,ops,parity_tests}.rs`,
  `crates/control/src/{audit,methods,web,schema}.rs`, `crates/control/src/proto/types.rs`,
  `crates/meta/src/store/gate.rs` (the store-stall fault grammar),
  `crates/harness/src/{client,verify,load}.rs`, `crates/harness/src/scenarios/{campaign,handover}.rs`,
  `crates/harness/src/k8s/parity.rs`, plan 41 (§1, §5.8, §11), `docs/reference/configuration.md`,
  `docs/reference/features/named-filesystems.md`; commits in §1.
- `tracing-subscriber` 0.3.23: `fmt/fmt_layer.rs` (one `write_all` per
  event through `make_writer_for`), `fmt/writer.rs` (`MakeWriter`),
  `fmt/time` (`SystemTime`); `tracing-appender` 0.2 (`non_blocking`,
  `WorkerGuard`) for §4.7.
- Linux: `execve(2)` and `exit_group(2)` wait for every thread to leave
  the kernel; btrfs stable pages (`folio_wait_writeback`) for the wait
  in §1.
