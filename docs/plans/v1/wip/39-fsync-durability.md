# Plan 39 — fsync durability under S3 outages: `hard` by default

Read `docs/plans/v1/CONVENTIONS.md` first. This plan changes what an
`fsync(2)` does while the bucket is unreachable: today it gives up after a
fixed number of attempts and answers `EIO`; after it, it behaves like an NFS
`hard` mount — it keeps retrying a *transient* failure until the data is
durable, answers `EIO` promptly only for a failure waiting cannot fix, ends
early when its caller is killed (but not for a handled signal), and can be
bounded by an explicit, documented operator opt-in
(`--fsync-timeout`), the analogue of nfs(5)'s `soft`. It also makes
`fsyncdir` a real barrier and reports a lock-fence discard to every open file
description errseq-style. It does **not** change what each `--fsync-mode`
waits *for* (§6).

The decision (hard by default, soft on request, never break consistency or
durability) was taken by the maintainer before this plan was written; this
document records the research behind it and the design that implements it.

## Dependencies

- Plan 30 (§M9/§M10: ack policies, continuation epochs — what an `fsync`
  waits for under each), plan 31 (C4's `Vfs` contract and the FUSE adapter,
  the completion pool, `CancelToken`; C4b's handover handle table), plan 38
  (the vendored `fuser` patch series this plan adds `0003` to). Committed.
- Coordinates with nothing in flight: the only shared file with plan 38's
  open milestones is `vendor/fuser` (a new, independent patch).

## 1. Problem

What the code did before this plan (verified against `main` at `5a56fb9`):

- `View::fsync` (`crates/engine/src/view/ops.rs`): the lock fence
  (`lock_publish_gate`) → `flush_inode(ino, force_through = true)` (commit the
  manifest at the sequencer; `drain_inode` waits for the inode's chunks to be
  PUT) → `sync_barrier_at` (fsync the local fjall store; under
  `--fsync-mode s3` also `SyncRequest::Barrier`, which uploads the inode's
  chunks and runs a sync round that ships the journal). The FUSE adapter
  ignores `datasync`.
- **Every wait was attempt-counted.** A chunk is PUT up to three times with a
  flat 50 ms pause (`crates/engine/src/upload.rs`), each attempt wrapping
  object_store's own retry series (`CONSTELLATION_S3_MAX_RETRIES`,
  `CONSTELLATION_S3_RETRY_TIMEOUT_MS` = 30 s). One failed PUT failed the drain,
  the drain failed the `fsync`: `EIO`. The journal barrier was one sync round;
  a round that could not ship was `EIO`. A forwarded manifest commit left in
  doubt (`CONSTELLATION_S3_LESS_OP_DEADLINE_MS`, the 40 s forward deadline)
  was `EIO`. Peer chunk handoff (`authority_driver.rs`,
  `CONSTELLATION_CHUNK_HANDOFF_AFTER_MS` = 6 s) already let an `fsync` succeed
  through a peer that can reach S3.
- **No data was dropped on `EIO`** — chunks stay in the cache and in the
  durable `pending_upload` rows, background rounds retry — so the failure was
  one of availability, not durability. But it was reported as `EIO` for an
  800 ms blip: the harness, whose daemons run with
  `CONSTELLATION_S3_MAX_RETRIES=2` / `RETRY_TIMEOUT_MS=2000`, saw `fio-blips`
  (800 ms cuts under `fio --end_fsync=1`) fail about one run in four.
- **A latent false success.** After a failed drain the write session was
  already published, so a *second* `fsync` under `--fsync-mode local` found
  nothing to flush, skipped the drain and returned 0 while the chunks the first
  one could not upload were still only local.
- **`fsyncdir` was not implemented.** fuser's default answers `ENOSYS`; the
  Linux kernel then sets `no_fsyncdir` and answers every later directory
  `fsync` on that mount 0 without asking — "fsync the directory after
  create/rename" was a silent no-op.
- **`FUSE_INTERRUPT` was answered `ENOSYS`.** The kernel then sets
  `no_interrupt` for the connection, and a caller of a request the daemon has
  already read waits it out *uninterruptibly*, `SIGKILL` included
  (`fs/fuse/dev.c`, `request_wait_answer`). An `fsync` waiting on S3 would
  have been an unkillable D-state process.
- **The lock fence reported a discard once per inode.** Writes made under a
  lapsed cluster-lock grant are discarded and `EIO` is owed; the flag was the
  inode's (`take_owed`), so the first publication point consumed it and a
  second descriptor's `fsync` returned 0 for discarded data — the fsyncgate
  shape (§2).

## 2. Research

- **POSIX `fsync(3p)`**: "If the fsync() function fails, outstanding I/O
  operations are not guaranteed to have been completed." `EIO` ("an I/O error
  occurred while reading from or writing to the file system") and `EINTR`
  ("the fsync() function was interrupted by a signal") are both conforming. A
  failed `fsync` guarantees nothing; a successful one guarantees the data
  reached the storage device, whatever that means for the filesystem. So
  waiting is conforming, `EINTR` is conforming, and what an implementation must
  never do is return 0 for data that is not durable.
- **nfs(5), `soft` / `hard`**: with `hard`, "NFS requests are retried
  indefinitely"; with `soft`, the client fails a request after `retrans`
  retransmissions, and "A so-called 'soft' timeout can cause silent data
  corruption in certain cases. As such, use the soft option only when client
  responsiveness is more important than data integrity." `hard` is the
  default, and `intr`/`nointr` have been no-ops since 2.6.25: a `hard` mount's
  waits are killable.
- **AWS EFS** ("Mounting EFS file systems", recommended NFS mount options)
  recommends `hard`, not `soft`, for exactly that reason, and a large `timeo`.
- **"fsyncgate" (2018)**: PostgreSQL treated a failed `fsync` as retryable; on
  Linux the failed writeback's pages had been marked clean and the error was
  reported once, so the *retried* `fsync` succeeded and the data was gone.
  PostgreSQL 12 now PANICs on any `fsync` failure (`data_sync_retry = off`).
  Two lessons: never drop dirty data on error, and never let a later `fsync`
  report success for data an earlier one failed to persist. Linux 4.13
  (`errseq_t`, Jeff Layton) made writeback errors reportable to **every** open
  file description once (each samples the mapping's error sequence at open and
  compares at `fsync`), instead of to whichever caller asked first.
- **Kernel FUSE request timeout**: `fs.fuse.default_request_timeout` and
  `fs.fuse.max_request_timeout` (seconds, 0 = none,
  `Documentation/admin-guide/sysctl/fs.rst`). They first appear in **Linux
  6.15** (`fs/fuse/sysctl.c`: absent in v6.13 and v6.14, present in v6.15 —
  checked against the tagged sources). A server can set its own timeout in
  `FUSE_INIT` (`FUSE_REQUEST_TIMEOUT`); `default_request_timeout` applies when
  it does not; a nonzero `max_request_timeout` caps either and on its own opts
  every connection in. When a request outlives it, the kernel **aborts the
  whole connection** (every open file then fails `ENOTCONN`), with a margin of
  up to `FUSE_TIMEOUT_TIMER_FREQ`. Opt-in: both default to 0 (this host:
  `0`/`0`). The vendored `fuser` never offers `FUSE_REQUEST_TIMEOUT`.
- **Kernel interrupt semantics** (`fs/fuse/dev.c`, `request_wait_answer`,
  read on `master`): a pending signal on a request already sent to userspace
  queues `FUSE_INTERRUPT`; a *fatal* signal does not abort the wait once the
  request is in userspace ("Either request is already in userspace, or it was
  forced. Wait it out.") unless the request was marked `abort_on_kill`. An
  `ENOSYS` reply to an interrupt sets `no_interrupt` for the connection;
  `EAGAIN` requeues it. So: **fatal signals do not, by themselves, end an
  `fsync` the daemon is holding**; the daemon must answer the request. And
  the kernel queues a request's interrupt **once**, for the first signal
  (`queue_interrupt` is a no-op while the request is already on the
  interrupt list), which may well be a handled one; a `SIGKILL` that comes
  later sends nothing new.
- **Fatal signals, as the kernel sees them** (`kernel/signal.c`): when a
  signal will kill the process — `SIGKILL`, or any signal whose disposition
  is the default fatal one and is not blocked, or another thread's
  `exit_group` — `complete_signal`/`zap_other_threads` add `SIGKILL` to
  *every thread's private* pending set; `fatal_signal_pending(task)` tests
  exactly that bit. A FUSE request's `pid` is the calling thread's id
  (`task_pid(current)`), and `/proc/<tid>/status` shows that thread's private
  set as `SigPnd` (`ShdPnd`: the process's shared one).

## 3. Decision and design

**Hard by default.** An `fsync` never returns `EIO` merely because S3 is
transiently unreachable. **Soft on request**: `--fsync-timeout <dur>` /
`CONSTELLATION_FSYNC_TIMEOUT` bounds the wait; when it elapses `EIO`, the data
still pending. **Consistency and durability are never traded**: no path drops
data, no path returns 0 for data that is not as durable as the mode says.

### 3.1 Error classification

`constellation_store_s3::classify` (`crates/store-s3/src/classify.rs`): one
function answering "will the same request, re-sent unchanged, plausibly
succeed later?" It walks the error's source chain for the types that decide
(`object_store::Error`'s variant — 403 becomes `PermissionDenied`, 404
`NotFound`, 412 `Precondition`; the HTTP client's `HttpErrorKind`; an
`io::Error`'s kind; `StoreError`), then decides the rest from the rendered
chain, which carries the status line and the S3 error body
(`<Code>…</Code>`). The text path is also the only one for failures that reach
the caller as text (a sync round's outcome crosses the authority core as a
string). The engine adds one rule (`SyncFailure::from_error`): a
`MetaError` anywhere in the chain is a local failure, permanent.

| Failure | Class | Why |
|---|---|---|
| connect refused / reset / aborted, DNS failure, TLS/transport "error sending request" | Transient | the network or the endpoint is away |
| request or read timeout (`HttpErrorKind::Timeout`, `io::ErrorKind::TimedOut`) | Transient | same |
| truncated response (`HttpErrorKind::Decode`) | Transient | same |
| any 5xx (`InternalError`, `ServiceUnavailable`, 502/504) | Transient | server side |
| 429, `SlowDown`, `ThrottlingException`, `KMS.ThrottlingException` | Transient | back off |
| 400 `RequestTimeout`, `IncompleteBody`, `BadDigest` | Transient | the request was cut in transit |
| 409 `OperationAborted` / `ConditionalRequestConflict`, 412 (lost CAS) | Transient | protocol race: re-read and retry |
| `ExpiredToken` / `TokenRefreshRequired` with refreshable credentials (SDK chain with expiring creds, an engine's own source) | Transient | the provider replaces it |
| credential source unreachable (`StoreError::AwsCredentials`) | Transient | IMDS/SSO may answer later |
| `journal not shipped: no lease`, a metadata commit in doubt or held | Transient | the lease/sequencer is unreachable with S3 |
| unrecognised | **Transient** | see below |
| 403 `AccessDenied`, `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `AccountProblem`, `AllAccessDisabled` | Permanent | credentials or policy |
| `ExpiredToken` with static credentials | Permanent | never heals |
| 404 `NoSuchBucket` (any `NotFound`) | Permanent | |
| 400 `InvalidRequest`, `InvalidArgument`, `MalformedXML`, `EntityTooLarge`, `InvalidDigest`, other 4xx | Permanent | the request itself is wrong |
| `RequestTimeTooSkewed` | Permanent | host clock: needs an operator |
| `KMS.DisabledException`, `KMS.NotFoundException`, `KMS.KMSInvalidStateException`, `KMS.AccessDeniedException`, `KMS.KeyUnavailableException`, `KMS.InvalidKeyUsageException` | Permanent | the key |
| `PermanentRedirect`, `NotImplemented`, `MethodNotAllowed` | Permanent | wrong endpoint/region |
| pending chunk missing from the local cache; local `MetaError`; corrupt object; local `StorageFull`/`ReadOnlyFilesystem` | Permanent | local content or disk |

**Unrecognised is transient**, as an NFS `hard` mount retries an RPC with no
answer: waiting is always safe for durability, an `EIO` never is (whoever
receives it may throw the data away), and the wait stays bounded by the
caller's death, by the opt-in timeout, and is logged after ten seconds.

### 3.2 Hard waiting on the `fsync` path

`crate::fsync_wait::FsyncWaits::run` (`crates/engine/src/fsync_wait.rs`) is
the retry loop. An attempt is the same sequence as before — `flush_inode`
(force-through), the barrier of the mount's mode — re-using the same
machinery: the inode drain with its peer handoff, the barrier round. The waits
inside an attempt (the sync task's replies to `DrainInode` and `Barrier`) run
inside a thread-local `Scope`: they poll the cancel token and the deadline
while they wait (`fsync_wait::recv`), and record why they failed (`note`), from
the classified `SyncFailure` the sync task now replies with. The loop then:

- **Transient** → capped exponential backoff with full jitter (`Backoff`:
  100 ms doubling to 5 s, each delay drawn from `[d/2, d]`), then another
  attempt;
- **Permanent** → `EIO` at once (logged at warn, counted);
- **no failure recorded** → the refusal has nothing to do with waiting (a lock
  fence, a local error): returned as it was, unretried.

A failed attempt never loses its place: **every retry, and the first attempt
of an `fsync` after a failed one, drains the inode's pending chunks again**
(`View::fsync_owed` across `fsync`s, the attempt counter within one), because
the write session was already published and `flush_inode` alone would not wait
for them. That closes the latent false success of §1. What it drains is still
only what the mode waits for: nothing in an active continuation epoch, where
`flush_detached` skips the drain too (§6).

The other sync-path waits found: the forwarded/in-doubt manifest commit
(`submit_to_core`'s `InDoubt`, `Busy`/`NotHolder`, `Held`) is noted transient,
so the attempt is retried — safe because an in-doubt op is retried by design
(the base check refuses a duplicate and the rebase lays the flush over what
survived). Its reply wait — the forward deadline is tens of seconds — is a
`fsync_wait::recv` too, as is the lease acquisition's per-probe reply and its
backoff sleep (`fsync_wait::sleep`): every wait inside an attempt ends on the
caller's death, the soft timeout and the kernel cap, so none can overshoot the
cap and let the kernel abort the connection. `CONSTELLATION_REMOTE_CHUNK_WAIT_S`
bounds a *reader's* wait for a non-owner's chunk, not an `fsync`'s, and is
unchanged.

`O_SYNC`/`O_DSYNC` writes (a write plus an `fdatasync`) take the same loop
(`View::flush_sync_write`), with the same owed bookkeeping — a failed one
leaves the inode owed, so the next `fsync` drains again instead of finding no
write session and answering 0 — and, like `fsync`, wait on the `fsync` pool,
never on a frontend worker, ended early by the caller's death (§3.3) or the
soft timeout/kernel cap. Cut short they answer **`EIO`**, never `EINTR`: the
bytes are written, but an `O_SYNC` write that returns claims them durable, and
they are not; the data stays pending.

Unchanged: background uploads (still three attempts per pass, retried every
round), `close()` and `--write-mode` (a `close` runs outside any scope, so its
waits block and fail exactly as before), the handoff, the hold of metered
uploads.

### 3.3 Killable, not interruptible

The policy is NFS `hard`'s (nfs(5): since 2.6.25 a `hard` mount's waits are
killable, and `intr` is ignored): **a waiting `fsync`, `fsyncdir` or
`O_SYNC`/`O_DSYNC` write ends early only when its caller is being killed.** A
signal the process handles, ignores or blocks — a timer, a latch signal, a
`SIGINT`/`SIGTERM` caught for a clean shutdown — leaves it waiting, with no
`EINTR`: applications that treat any `fsync` failure as fatal (PostgreSQL
before 12 PANICs) would otherwise fail without any outage, on a healthy 200 ms
chunk PUT. (Maintainer's decision for the review round; the first version
answered `EINTR` to any interrupt.)

- Vendored `fuser` patch `0003-interrupt.patch`: `Filesystem::interrupt(req,
  unique)`, called for every `FUSE_INTERRUPT`, never answered (so the kernel
  keeps sending them; `ENOSYS` would set `no_interrupt` for good).
- The adapter's `Interrupts` table: `fsync`/`fsyncdir` and `O_SYNC`/`O_DSYNC`
  writes register their request's `unique` with the caller's thread id and
  carry a `CancelToken` in their `OpCtx`. An interrupt only **marks** the
  request. A marked request's token is cancelled once the caller's thread
  has a fatal signal pending (`fatal_signal_pending`: `SIGKILL` in
  `/proc/<tid>/status`'s `SigPnd` or `ShdPnd`, §2) — checked when the
  interrupt arrives, and every 100 ms after by a watcher thread while a
  marked request still waits, because the kernel interrupts a request once
  and the fatal signal may come after a handled one.
  - Thread, not process: the request names the thread, the kernel's own
    `fatal_signal_pending()` is per thread, and every default-fatal signal
    is converted into a per-thread `SIGKILL` (§2), so "any signal that will
    kill the process" is detected, not only an explicit `SIGKILL`.
  - Unknowable is "no": a caller with `pid` 0 or invisible in the daemon's
    `/proc` (another pid namespace, `hidepid`) keeps waiting as before plan
    39 — until durable, or the soft timeout/kernel cap.
- An interrupt can overtake its request (the reader that takes it is not the
  thread that registers it; over io_uring the request may first queue for an
  offload thread, fuser's `dispatch_on_ring`). It is remembered for up to a
  minute (4096 at most) and marks the request as it registers. Linux request
  uniques are never reused (`fuse_get_unique` only increments), so a
  remembered interrupt can only match the request it names; the bounds only
  bound the memory of interrupts for ops that never register.
- The engine's wait sees the token and answers (`EINTR` for `fsync`, which
  the dying caller never reads; `EIO` for an `O_SYNC` write); the drain keeps
  running in the background (its reply dropped), the data stays pending.
- **Off the workers**: an `fsync` and an `O_SYNC` write's publication wait on
  the engine's `fsync` pool (`fsync_wait::pool`, an elastic pool of up to 1024
  threads named `fsync-wait-N`), never on a FUSE worker, whenever the frontend
  can answer from another thread — otherwise a long outage (a database writing
  its WAL with `O_DSYNC`) could pin every worker, and with them the delivery
  of the very interrupt that ends a killed caller's wait. The adapter counts
  the reply as a bounded deferral: a `daemon --upgrade` drains it for
  `CONSTELLATION_HANDOVER_READ_DRAIN_MS` and refuses if it still waits.
- On the ring transport interrupts arrive on the `/dev/fuse` reader while the
  request they name may run on an offload thread; the adapter's table is
  shared by both, so an offloaded `fsync` is killable the same way.
- Lock waits (`F_SETLKW`/`flock`) are not wired to interrupts by this plan
  (deferred: they would change a visible behaviour — `EINTR` from a blocked
  `flock` — that deserves its own decision and test).

### 3.4 Opt-in soft timeout and the kernel request timeout

- `--fsync-timeout <dur>` on `mount` (`500ms`, `30s`, `2m`; `0`/`off`/`hard`
  = none, and an explicit `hard` on the flag overrides the environment),
  `CONSTELLATION_FSYNC_TIMEOUT` as the default; parsed before the fork,
  carried in `NodeHandoff` across `daemon --upgrade` (`0` there: an explicit
  `hard`), node-wide. `EngineConfig::fsync_timeout` (`None`: no flag;
  `Some(None)`: hard; `Some(Some(t))`: soft). When it elapses: `EIO`, data pending,
  background retry continues, the next `fsync` waits for it again.
- The kernel cap: at engine start `/proc/sys/fs/fuse/default_request_timeout`
  and `max_request_timeout` are read, combined as the kernel combines them
  for a server that sets none, and when a timeout is in force every wait is
  capped at `t − min(t/5, 5 s)` (logged once at warn), so the daemon answers
  `EIO` before the kernel aborts the whole connection. The effective limit is
  the smaller of the two.

### 3.5 Observability

- A wait past 10 s — counted from the start of the call, and checked while a
  reply is awaited, so a first attempt that is itself slow (three PUT tries,
  each an object_store retry series of up to 30 s) is covered — logs once per
  inode at warn ("fsync: S3 unreachable, still trying", with the last error)
  and once at info when it ends.
- `node.status.fsync`: `mode` (`hard`/`soft`), `timeout_ms`, `kernel_cap_ms`,
  `waiting` (every `fsync`/`O_SYNC` publication in progress, from its start),
  `longest_wait_ms` (the oldest current wait), `max_wait_ms`,
  `waited`, `retries`, `timeouts`, `permanent_errors`, `interrupted`; `/metrics`
  `constellation_fsync_*`; the event stream's gauges `fsync_waiting`,
  `fsync_longest_wait_ms`.

### 3.6 `fsyncdir`

The adapter implements `fsyncdir` as the view's `fsync` on the directory
inode: no write session to publish, so it is the barrier — the local store
synced (every namespace mutation of the directory is already committed at the
sequencer when its op returned), and under `--fsync-mode s3` the journal
shipped. Same kill, timeout and retry behaviour as `fsync`.

### 3.7 Per-descriptor error reporting for discarded writes

errseq semantics for the lock fence's discards:

- A view hands out **one `Fh` per open** (`view::durable::Handles`), each
  remembering its inode (§6.12's confinement check is now "this handle was
  given out for this inode") and the newest discard error event it has seen.
- `LockTables::note_discard(ino)` records an event (a global sequence);
  `error_seq(ino)` reads it (one relaxed load while none was ever noted);
  `forget_errors` drops it once **no view on the node** has the inode open
  (the events are the node's — one `LockTables` per `Meta` — while a view's
  last close is only that view's: another mount's older description would
  otherwise `fsync` to 0 over discarded data).
- An open samples the inode's sequence (a description opened after a discard
  never reports it); `lock_publish_gate(ino, fh)` reports `EIO` once per
  description whose sample is older; a discard found at a publication point is
  reported by that description at once and recorded for the others.
- The handle table crosses a session handover (`HandleTableSnapshot.handles`),
  so handed-over descriptors keep working, and so do the node's not-yet-
  forgotten events (`HandleTableSnapshot.errors`); the import numbers later
  events above everything that crossed. The format change bumps
  `HANDOVER_VERSION` to 3, so the ABI probe refuses an upgrade or downgrade
  across it up front instead of failing to parse the handoff after the old
  image is gone (or serving `Fh == ino` to a kernel holding per-open
  numbers).

## 4. What changed, per path

| Path | Before | After |
|---|---|---|
| `fsync` chunk drain | 3 PUT attempts × object_store series, then `EIO` | retried while transient (backoff 100 ms→5 s), `EIO` for permanent, `EINTR` when the caller is killed, `EIO` at the soft timeout/kernel cap |
| `fsync` journal barrier (`--fsync-mode s3`) | one round, `EIO` if it failed | same loop, the round's text classified |
| `fsync` forwarded commit in doubt | `EIO` | retried (same rid semantics); its reply wait honours the kill, timeout and cap |
| second `fsync` after a failed one (`local`) | could return 0 with chunks unuploaded | drains again: never a false success |
| `O_SYNC`/`O_DSYNC` write | as `fsync`'s old behaviour, on the worker | as `fsync`'s new one, on the `fsync` pool, killable; `EIO` (not `EINTR`) when cut short; a failure leaves the inode owed |
| `fsyncdir` | `ENOSYS` → kernel no-ops forever | the barrier |
| `FUSE_INTERRUPT` | `ENOSYS` → `no_interrupt` | marks a waiting `fsync`/`fsyncdir`/`O_SYNC` write; ended only once its caller has a fatal signal pending |
| lock-fence discard | `EIO` once per inode | `EIO` once per description open at the discard, across mounts of the node and across `daemon --upgrade` |
| `close()`, background uploads, `--write-mode`, lock waits | — | unchanged |

## 5. Milestones

- **F1** — classification (`classify.rs`, table test), classified
  `SyncFailure` replies, `fsync_wait` (loop, backoff, scope, soft timeout,
  kernel cap, stats), `View::fsync` on the `fsync` pool, the owed drain,
  `O_SYNC`; engine tests with a scripted sync task (transient retried until
  success; permanent `EIO` and the next `fsync` drains again).
- **F2** — `fuser` patch `0003`, adapter `Interrupts`, `fsyncdir`; wire tests
  (`fsyncdir` reaches `Vfs::fsync`; an overtaking interrupt cancels its
  `fsync` and is not answered).
- **F3** — errseq handles (`Handles`, `note_discard`/`error_seq`), handover
  table; the two-descriptor test.
- **F4** — `--fsync-timeout`, `node.status.fsync`, `/metrics`, schema
  re-bless; docs (`configuration.md`, `durability-and-failover.md`,
  `TESTING.md`).
- **F5** — harness: `fio-blips` 10/10 unchanged; `fsync-hard-outage`,
  `fsync-soft-timeout`, `fsync-interrupt`, `fsyncdir-barrier`.

All five landed in one chunk (`PROGRESS.md`, "Plan 39"), then a review round
(killable-only interrupts, `O_SYNC` writes on the pool, `HANDOVER_VERSION` 3,
node-wide discard-error lifetime, the remaining sync-path waits made
interruptible, the slow-first-attempt warning).

## 6. Open: `--fsync-mode local` semantics

**Deferred to the maintainer** (a separate analysis is in progress). Today
`--fsync-mode local` does wait for the inode's chunk uploads when the `fsync`
publishes a write session (the flush is force-through), but not for the
journal, and not at all inside an active continuation epoch (plan 30 §M10:
a single node's S3 cut starts one within about a second, after which `local`
`fsync`s return once the node's own store is on disk). Whether `local` should
keep waiting for chunk uploads is that analysis's question. **This plan does
not change what any mode waits for**; it changes only how failures and
outages are handled *while* waiting. In particular the owed drain of §3.2
applies the epoch rule exactly as `flush_detached` does, and the outage
scenarios run under `--fsync-mode s3`, where the wait for the bucket is the
mode's contract regardless of epochs.

**Recorded outcome of this plan under `local` (review, to be fixed by chunk
39b).** Traced by the reviewer: a `local` `fsync` started *before* an epoch
drains on its first attempt, which fails as transient; its retry's owed drain
is gated on `!epoch_active()`, and `finish_flush` returns `through &&
!epoch_active`. Once the continuation epoch activates (about 1 s into a
single-node or whole-cluster cut), the retry skips the drain and `meta.sync()`
returns 0. So **a `local` `fsync` started before an epoch can succeed with its
chunks only on this node (its cache and `pending_upload` rows) once the epoch
activates** — process-crash/reboot durability, not node-loss durability.
Before plan 39 the same `fsync` returned `EIO`. That is consistent with plan
30's epoch rule but not with `writeback.rs`'s "applications asking for a
barrier keep the stronger rule" outside the epoch exception. Separately,
`local` never drains chunks published by an earlier `close()` under
`--write-mode back`, or after a failed write-through close, since
`flush_inode` then finds no session. **Chunk 39b** owns the fix: on every
`fsync` in both modes drain the inode's `pending_upload` rows rather than rely
on "this `fsync` published a session", decide explicitly whether an epoch
exempts that (and remove the epoch gate from the owed drain if not), fold
`fsync_owed` (per view, in memory) into that rule, and update `writeback.rs`
and this section.

## 7. Exit criteria

- [x] One classification function with a unit-tested table (§3.1).
- [x] No attempt-counted give-up on the `fsync` path for transient failures;
  permanent → prompt `EIO` with data pending; never a false success (engine
  tests `an_fsync_retries_a_transient_drain_failure_until_it_succeeds`,
  `a_permanent_drain_failure_is_eio_and_the_next_fsync_drains_again`).
- [x] Killable, not interruptible: `FUSE_INTERRUPT` ends a waiting `fsync`,
  `fsyncdir` or `O_SYNC` write only when its caller has a fatal signal pending
  (adapter unit test; wire tests for `fsync` and `O_SYNC`/`O_DSYNC` writes;
  harness `fsync-interrupt`: a handled `SIGINT` keeps waiting, `SIGKILL`
  reaps the process within seconds).
- [x] A failed `O_SYNC` write leaves the next `fsync` draining again
  (`a_failed_o_sync_write_leaves_the_next_fsync_draining_again`).
- [x] `--fsync-timeout` / `CONSTELLATION_FSYNC_TIMEOUT`, kernel cap, both
  documented with the nfs(5) warning (harness `fsync-soft-timeout`).
- [x] Logs and `node.status.fsync` / `/metrics`; schema re-blessed.
- [x] `fsyncdir` implemented and tested (wire test; harness
  `fsyncdir-barrier`, which fails 3/3 with the old `ENOSYS`).
- [x] errseq reporting for lock-fence discards (two-descriptor test through a
  real lapsed grant; a second view keeps the event alive; the events cross a
  handover, `HANDOVER_VERSION` 3).
- [x] `fio-blips` 10/10 without relaxing it; new scenarios 3× each; the
  regression set and every CONVENTIONS gate green.

## Sources

- POSIX.1-2017 `fsync(3p)`: <https://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html>
- nfs(5), "soft / hard" and "intr / nointr": <https://man7.org/linux/man-pages/man5/nfs.5.html>
- AWS EFS, recommended NFS mount options (`hard`): <https://docs.aws.amazon.com/efs/latest/ug/mounting-fs-nfs-mount-settings.html>
- "PostgreSQL's fsync() surprise", LWN 2018: <https://lwn.net/Articles/752063/>; PostgreSQL wiki "Fsync Errors": <https://wiki.postgresql.org/wiki/Fsync_Errors>
- errseq_t, Linux 4.13: <https://lwn.net/Articles/724307/>; `lib/errseq.c`
- Kernel FUSE request timeout sysctls: `Documentation/admin-guide/sysctl/fs.rst`
  (`fuse` section), `fs/fuse/sysctl.c` (present from v6.15), `fs/fuse/inode.c`
  (`fuse_init_server_timeout`)
- Fatal signals: `kernel/signal.c` (`complete_signal`, `zap_other_threads`),
  `include/linux/sched/signal.h` (`fatal_signal_pending`), `fs/proc/array.c`
  (`SigPnd`/`ShdPnd`)
- Kernel interrupt handling: `fs/fuse/dev.c` (`request_wait_answer`,
  `fuse_dev_do_write`'s `FUSE_INT_REQ_BIT` branch); `Documentation/filesystems/fuse.rst`,
  "Interrupting filesystem operations"
