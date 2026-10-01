# Vendored fuser 0.18.0: Constellation's patch series

Two named series live in `patches/`, applied in filename order:

1. `0001-constellation-session-handover.patch` — plan 31 §6.11 / C4b. This
   file's first half.
2. `0002-io-uring-transport.patch` — plan 38 §3(a) / Z1, behind the
   `io-uring` cargo feature, off by default. "The io-uring transport",
   below.

## FUSE session handover (`patches/0001-constellation-session-handover.patch`)

This directory is fuser **0.18.0** exactly as published on crates.io
(checksum `b82b6597d216503555ead6b358f341ef748869bf5c6fbae6a0cb9dd231baecfd`,
the `Cargo.lock` entry before vendoring; upstream commit
`9c957f74efe715112049298cdf1d601781829c8d`, tag `v0.18.0`), used through
`[patch.crates-io]` in the workspace `Cargo.toml` and listed in its
`exclude` list, exactly like `vendor/fjall`. Only what a build needs is
kept: `src/`, `build.rs`, `Cargo.toml`, `README.md` and the license
(`LICENSE.md`, MIT, unchanged).

It exists for plan 31 §6.11 / C4b: a FUSE session that can be handed from
one process image to the next **without the kernel ever seeing an
unmount** (`constellation daemon --upgrade`; plan 37's CSI engine-pod
replacement uses the same primitive). Stock fuser 0.18 cannot do that, for
three separate reasons, each fixed by one named patch below.

The whole patch set is `patches/0001-constellation-session-handover.patch`
(a `git diff` against the pristine crate). `tools/vendor-fuser.sh <version>`
re-vendors a new upstream release and re-applies it, failing loudly on any
hunk that no longer applies. Every hunk is marked `CONSTELLATION PATCH
(<name>)` in the source.

### The patches

#### `negotiated-init` — learn what `FUSE_INIT` agreed (`src/session.rs`, `src/lib.rs`)

The kernel sends `FUSE_INIT` once per connection. Whoever serves the
connection next must know what the first server agreed to, both to check it
can serve it at all and to tell its filesystem which capabilities are in
force (its `Filesystem::init` is not called again). Upstream computes the
agreement inside `handshake()` and throws it away (it keeps only
`proto_version`, which nothing reads).

- New public `NegotiatedInit` (the kernel's and our protocol versions, the
  offered and the agreed `InitFlags` bits, `max_readahead`, `max_write`,
  `max_background`, `congestion_threshold`, `time_gran_ns`, `max_pages`,
  `max_stack_depth`); `serde` under the existing `serializable` feature (it
  crosses processes). `check_resumable()`: same major version, no agreed
  capability this build does not know, a `max_write` that fits this
  build's request buffer.
- `handshake()` records it (computed exactly as `Init::reply` computes the
  flags it sends); `Session::negotiated_init()` returns it.

fuser frames every reply and notification by its compiled ABI, never by the
negotiated minor, so resuming needs no other per-version state.

#### `from-fd-resumed` — serve an initialised connection without a handshake (`src/session.rs`)

`Session::new` and `Session::from_fd` both end in `handshake()`, which
requires the *first* request it reads to be `FUSE_INIT` and fails closed
(`EIO` to the request, `InvalidData` to the caller) on anything else. On a
handed-over connection the first request is an ordinary one, so both refuse
it — precisely right for a new mount, precisely wrong for a resumed one.

- New `Session::from_fd_resumed(fs, fd, acl, config, init: NegotiatedInit)`:
  checks `init.check_resumable()`, builds the session with `proto_version`
  and the negotiated record pre-set, and never calls `handshake()`. The
  ordinary dispatch loop then serves it; a stray `FUSE_INIT` there is
  answered `EIO`, as upstream already does after a handshake.

**Choice of shape.** Plan 31 §6.11 offered two: this dedicated constructor,
or folding `INIT` into the dispatch loop behind an `initialized` flag the
resume pre-sets (Mountpoint's fork's structure). fuser 0.18's dispatch loop
already treats `INIT` as an error arm and needs no per-connection init
state (nothing reads `proto_version` after the handshake), so the flag
would guard nothing: the constructor is the smaller patch, it leaves the
handshake path byte-for-byte upstream's for every ordinary mount, and it
composes with a later move to Mountpoint's loop (which would simply make
`from_fd_resumed` set the flag).

#### `detach` — stop serving without unmounting or closing (`src/session.rs`, `src/channel.rs`, `src/mnt/mod.rs`, `src/lib.rs`)

A worker parked in `read(2)` on `/dev/fuse` cannot be woken except by a
request, and every way `run()` can end destroys the filesystem, drops the
channel (closing the descriptor: the kernel aborts the connection when the
last one closes) and drops the `Mount` (unmounting it).

- `Session::detacher()` arms a session (before it runs) and returns a
  `SessionDetacher` (`Clone`, `Send`). An armed session's descriptors are
  switched to `O_NONBLOCK` and each worker reads through
  `Channel::receive_or_wake`: a non-blocking `read`, and on `EAGAIN` a
  `poll(2)` on the descriptor *and* a private close-on-exec pipe. The stop
  flag is checked before every read. `SessionDetacher::detach()` sets the
  flag and writes one byte to the pipe (never read, so it stays readable):
  every worker returns before its next read, having finished — and
  answered — the request it was dispatching. A request that was read is
  therefore always answered; one that was not stays in the kernel's queue
  for the next server. Under load a worker's first `read` succeeds and no
  `poll` is made; an idle worker pays one extra `read` + `poll` per wake-up.
  Sessions that are not armed keep upstream's blocking loop unchanged.
  Refused for an `auto_unmount` mount (its helper unmounts when this
  process's socket to it closes).
- `Session::run_detachable()` is `run()` returning a `SessionEnd`:
  `Ended` (upstream's behaviour: the filesystem destroyed) or
  `Detached(DetachedSession { filesystem, fd, init })` when every worker
  stopped for the detach — the filesystem **not** destroyed (handed back),
  the `Mount` **disarmed** instead of unmounted (`Mount::disarm`, new), and
  `fd` a `dup` of the session's descriptor (the same open file, so the same
  connection and queues). `run()` is now `run_detachable()` mapped to `()`.
- `DetachedSession`, `SessionDetacher`, `SessionEnd` and `NegotiatedInit`
  are re-exported from the crate root.

The descriptor is non-blocking when handed out; `from_fd_resumed` sessions
are armed by their user (`constellation-frontend-fuse` arms every session),
so they read it the same way.

#### Build hygiene (`Cargo.toml`)

- The `[[example]]` targets and `[dev-dependencies]` are removed (their
  files are not vendored; the library is all that is built).
- A `[lints.rust] warnings = "allow"` table, as `vendor/fjall` has: a path
  dependency is not built with `--cap-lints allow`.

### What uses it, and the proof

`crates/frontend-fuse/src/session.rs` (`SessionControl::detach`,
`FuseSession::resume`; its module doc has the detach protocol and its
tests run a real kernel mount: detach with nothing in flight, a detach that
waits for an op in flight, a resume serving requests the kernel queued
while nobody read) and `crates/cli/src/handover.rs` (`daemon --upgrade`,
whose module doc has the whole sequence). The harness scenarios
`session-handover-idle` and `upgrade-under-load` are the end-to-end gate:
a real mount handed over under a live writer, zero `ENOTCONN`/`EIO`.

### Upgrading fuser

`tools/vendor-fuser.sh <version>` downloads the crate from crates.io
(or takes `--from <dir>` holding an unpacked crate), replaces this
directory's `src/`, `build.rs`, `Cargo.toml`, `README.md` and `LICENSE.md`
with the pristine files, and `git apply`s every `patches/*.patch` in order;
any hunk that does not apply fails the script, and nothing is left
half-applied. Then update the version/checksum above, `cargo update -p
fuser`, and run the frontend's tests and the two handover scenarios. If a
hunk conflicts, re-make the change by hand on the new source and
regenerate the patch with `git diff --no-index <pristine> vendor/fuser`.

### Dropping the patch

If upstream fuser gains an equivalent (a resumable session and a
non-destructive stop), switch `constellation-frontend-fuse` to it, remove
the `[patch.crates-io]` entry and the `exclude` entry, delete this
directory and `tools/vendor-fuser.sh`, and keep the handover tests and
scenarios.

## The io-uring transport (`patches/0002-io-uring-transport.patch`)

Plan 38 (`docs/plans/v1/wip/38-fuse-read-path-transport.md`) §3(a),
milestone Z1. `patches/0002-io-uring-transport.patch` adds the userspace
half of **FUSE-over-io_uring** (kernel 6.14+, ABI 7.42) behind a new cargo
feature `io-uring`, **off by default**: without the feature this directory
is fuser 0.18.0 plus patch 0001 and nothing else. Every hunk is marked
`CONSTELLATION PATCH (io-uring)`; `tools/vendor-fuser.sh` applies 0001 then
0002, in filename order, and `--check` proves the pair reproduces this tree
byte for byte.

`constellation-frontend-fuse` forwards the feature under the same name
(`io-uring = ["fuser/io-uring"]`) and the workspace default leaves it off.

### Where it came from, and the rubric that let it in

The transport was built by the **Skory/fuser** fork
(`github.com/Skory/fuser`, branches `io-uring/*`, design gist
`gist.github.com/Skory/4fb49ff602919596fa3c444c8abecd87`), as seven commits
stacked on that fork's `master`:

| commit | subject |
|---|---|
| `e0bc812` | Declare the FUSE-over-io_uring ABI |
| `dc8e01d` | Add the io_uring ring transport module |
| `1998085` | Prepare Session and reply plumbing for a second transport |
| `3891ee0` | Wire the io_uring transport into Session |
| `dad0b1d` | Add io_uring flags to the examples and io_uring test targets |
| `29174e0` | Add `ReplyData::fill` for read replies written in place |
| `0104570` | Add a transport benchmark comparing /dev/fuse and io_uring |

Plan 38 §3(a) recorded every claim about that code as **REPORTED** and made
milestone Z1 re-verify it against the actually-vendored source before
anything depended on it, by a fixed rubric mirroring plan 37 K0's
("verified against the vendored source, not assumed",
`docs/plans/v1/wip/31-core-frontend-backend.md` §6.11). The rubric, run on
2026-09-30/10-01 against `io-uring/bench` at `0104570`:

| # | Criterion | Finding | Evidence |
|---|---|---|---|
| a | **Licence** | **MIT, unchanged.** Same file, same copyright holder as upstream fuser; no added licence, no added notice. | `LICENSE.md` on `io-uring/bench` is byte-identical to `v0.18.0`'s ("The MIT License (MIT) … Copyright (c) 2020-present Christopher Berner"); `Cargo.toml` `license = "MIT"`. No new dependency beyond `io-uring` `0.7.14` (MIT OR Apache-2.0; the lock resolves 0.7.15) and three `nix` features already in the tree. `LICENSE.md` here is byte-identical to `v0.18.0`'s and to the fork's (`diff` of all three). |
| b | **Base version vs our 0.18.0** | **The ring is separable; the fork's other drift is not ours.** The fork's `master` is `v0.18.0` + 52 commits, +4,052/−251 lines in `src/` alone (FUSE_STATX, TMPFILE, SYNCFS, `FUSE_ALLOW_IDMAP`, an ABI cleanup, a restructured `Session`) — none of it the ring. The ring stack's own `src/` diff is 47 hunks; applied to pristine 0.18.0, **26 land cleanly and 21 reject**, and every rejection is a context clash with that unrelated drift, not with the ring. All four new files (`src/uring/{mem,mod,ring,staging}.rs`, 4,266 lines = 56% of the patch) are additions and apply untouched. | `git diff --stat v0.18.0 origin/master -- src/`; `git apply --reject` of `git diff origin/master origin/io-uring/bench -- src/` onto a `v0.18.0` worktree: rejects in `session.rs` (13), `request.rs` (5), `lib.rs` (2), `mnt/mount_options.rs` (1). Crucially, 0.18.0 **already has** every type the integration needs — `ReplySender` (as an enum with a `Channel` variant), `RequestWithSender`, `SessionEventLoop`, `FilesystemHolder`, `DevFuse`, `FuseReadBuf`, `receive_retrying` — so the 21 rejected hunks were re-made by hand, small. |
| c | **Its own test suite** | **Runs, and passes, against *our* copy.** The fork's suite: `master` 109 passed / 1 failed, the ring stack 130/1 without the feature and **209/1** with it (the single failure, `mnt::test::mount_unmount_external`, is pre-existing on `master` and a privilege problem — `EPERM` from the external mount helper with no root). Re-run against this vendored tree after the lift: **72 passed / 0 failed** without the feature, **156 / 0** with it (including the three this patch adds for the gather form and `Transport`), of which 18 real-kernel ring tests skip loudly on a `fuse.enable_uring=N` host and run on a 6.14+ one — all 156 pass on a kernel 7.0 host with `fuse.enable_uring=Y`, where they do run. The fork's own `fuse_over_io_uring_tests_ran` guard **fails** if the kernel advertises the flag while those tests skipped, so a silently-never-exercised ring cannot pass as green. | `cargo test --lib [--features io-uring]` in `/tmp/skory-fuser` and in `vendor/fuser`. The count differs from the fork's because the fork's suite also covers its unrelated drift (statx, tmpfile, idmap), which is not vendored. |
| d | **Soundness of the `unsafe`, and the threading model** | **Sound as read; no unexplained `unsafe`.** 51 `unsafe` mentions in the transport (`ring.rs` 36, `staging.rs` 9, `mem.rs` 6 — `mod.rs` has none), **every one carrying a `SAFETY:` comment naming the invariant it rests on**, and both `unsafe impl Send`/`Sync` (`RingMemory`, `EntryIov`, `EntryPtr`) stating the invariant that licenses them. Specifically checked, one by one: **`SINGLE_ISSUER` + `DEFER_TASKRUN`** — the ring is built `IORING_SETUP_R_DISABLED` and *enabled from the ring's own thread* (`register_enable_rings` inside `thread_main`), which is the documented way to bind the issuer to a thread other than the creator, and every SQE of a ring is pushed by that thread. **Replies from foreign threads** — `RingCommit` is `Clone + Send`; a commit from any other thread writes the reply, queues the entry index in `Live::pending` under the lock and kicks an `eventfd`, and the ring thread drains `pending` before every wait; a commit from the ring thread itself skips the `eventfd` (`ring_thread: OnceLock<ThreadId>`). A dedicated per-entry state machine (`InKernel`/`Dispatching`/`Deferred`/`Dispatched`/`Committing`/`Pending`/`Dead`) makes a duplicate commit a rejected error rather than a second write. **Entry buffer lifetimes** — one anonymous `MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE` mapping per ring, `MADV_DONTFORK` (so a `fork(2)` in the host process cannot leave the kernel writing into a child's copy), page-aligned strides, all arithmetic `checked_*`, and the kernel-shared header read only through `ptr::read_unaligned` so no Rust reference is ever formed over it; `staging.rs`'s own tests are written to be run under Miri (a heap stride behind a raw pointer, "so Miri checks the aliasing discipline of the code under test rather than the fixture"). **Cancellation on teardown** — `RingMemory` is held in `ManuallyDrop` and **leaked rather than unmapped** while `Live::in_kernel != 0`, so an unmap can never race a pending SQE; a `shutdown` flag exists precisely because the kernel posts no CQE at unmount for an entry held in userspace. **Unwind safety** — a panic inside a `fill` closure answers `EIO` through a `FillGuard` drop rather than leaving the request unanswered, and a panicking callback confines itself to its own ring (that ring answers `EIO`; other rings and the `/dev/fuse` reader keep serving). Layout is pinned by tests against the ABI (`assert_eq!(HEADER_SZ, 288)`, `OP_IN_OFFSET == 128`, …) rather than assumed. | Read of `src/uring/*.rs` and the `Session` integration; `grep` of every `unsafe` against its preceding comment. |
| e | **Code size and shape** | **A clean lift for the module, a hand re-make for the seam.** 57% of the patch is the four new `src/uring/` files, taken as they stand. The seam into `Session`/`reply.rs`/`request.rs` is small in kind — one `ReplySender::Ring(RingCommit)` variant, one `Option<RingSet>` field, one hook in `handshake()`, one `serve_ring()` supervisor — and was re-made against 0.18.0 + patch 0001 because of (b). Nothing in the transport needed redesigning around this plan's own `NegotiatedInit`/`Transport` extension: the fork already produces exactly the shape §3(a) asked for (one ring per worker thread with the kernel's per-CPU queues partitioned across them, a dedicated `/dev/fuse` reader for `INIT`/`FORGET`/`INTERRUPT`/notifications, `ReplyData::fill`, replies from any thread, graceful fallback, `clone_fd` ignored). | `patches/0002-io-uring-transport.patch`: +7,620/−30 lines over 14 files, of which `src/uring/*` is +4,266 and `session.rs` is +2,623 — and 2,177 of `session.rs`'s 3,540 lines are now its test modules, so the non-test seam is about 440 lines. |

**Verdict: VENDOR (lift the ring machinery, re-make the seam).** Every
REPORTED claim plan 38 §3(a) depends on held; the code is better documented
and better tested than the "open PRs, not a released crate" risk (§8) led
the plan to expect; and the one thing the rubric could have vetoed on —
unexplained `unsafe` or a threading model that only allows replies from the
issuing thread — is not what is there. The fallback the plan named for a
failed rubric (a narrower from-scratch patch against the ring primitives)
was not needed.

#### Where this patch deliberately differs from the fork

- **`Config::io_uring` / `Config::io_uring_queue_depth`, not `KernelConfig`.**
  Plan 38 §3(a) names `KernelConfig`; the fork puts both on
  `mnt::mount_options::Config` and that is the better home, so this patch
  keeps the fork's. `KernelConfig` only exists inside `Filesystem::init`,
  which runs *during* the handshake, whereas the transport is the caller's
  choice and has to be validated **before** anything is mounted (`n_threads
  == 0`, a zero depth, the feature missing, a non-Linux target: all refused
  by `Session::new`/`from_fd` up front). Keeping the fork's placement also
  keeps the patch rebaseable against it. `Config` is `#[non_exhaustive]`,
  so the two fields are not a breaking change; `Default` is hand-written
  now, only so the depth defaults to 8 rather than 0.
- **`io_uring_queue_depth` is `u32`, not `usize`** — it feeds
  `io_uring_setup(2)`'s entry count and the `payload_sz: u32` arithmetic.
- **`add_capabilities` refuses `FUSE_OVER_IO_URING`.** The fork's `master`
  gained a general `UNSUPPORTED_CAPABILITIES` set that 0.18.0 has no
  equivalent for, so this patch adds the one refusal that matters instead:
  echoing the bit tells the kernel to route every request to ring queues,
  and a mount whose *filesystem* asked for the bit without the session
  having registered any is unservable. Only the session may request it,
  through the new `KernelConfig::enable_io_uring()`, and only once its
  rings exist.
- **`NegotiatedInit.transport` and the handover refusal are *not* behind
  the feature** — see the next section.
- **A public host probe, `fuser::uring_unavailable()`** (feature-gated,
  Linux only), which the fork has no equivalent of. It answers "would a
  session asking for the ring get it here?" by reading
  `fuse.enable_uring` *and* making a real `io_uring_setup(2)` — the call a
  seccomp policy or container runtime can deny with nothing in sysfs to
  show for it (plan 38 §8). fuser's own ring tests skip on it and
  `crates/frontend-fuse`'s fallback test derives its expectation from it,
  so a legitimate fallback on a `enable_uring=Y` host that forbids
  `io_uring_setup` is not mistaken for a broken transport. A session never
  consults it: it tries, and logs the real reason when it falls back.
- **`serve_channels` is not taken.** The fork refactored upstream's
  `/dev/fuse` thread fan-out into a helper; patch 0001 already rewrote that
  loop (it returns `LoopEnd` so a detach can stop it), and a ring session
  is never detachable, so the ring path gets its own supervisor
  (`serve_ring`) and 0001's loop is left exactly as it was.
- **`[dev-dependencies] tempfile` comes back.** Patch 0001 dropped every
  dev-dependency along with the unvendored examples, which left fuser's own
  suite unbuildable here. Restoring the one the `#[cfg(test)]` code
  actually needs makes `cargo test --manifest-path vendor/fuser/Cargo.toml`
  the standing gate both patch series are re-verified by on every upstream
  upgrade. `vendor-fuser.sh --check` ignores the `Cargo.lock` and `target/`
  that produces (both gitignored).
- **A gather form of `fill` is added on top of the fork's.** The fork has
  `ReplyData::fill(max_len, |buf| ..)`; plan 38 §3(a) also asks for a
  variant taking already-separate segments, because the FUSE adapter's
  `ReadData` is a `SmallVec<[Bytes; 4]>` and joining it into one slice is
  the copy this plan exists to remove. `ReplyData::gather(&[impl
  AsRef<[u8]>])` (with `ReplySender::gather`, `ReplyRaw::send_gather` and
  `ll::ResponseSegments`) writes the segments straight into the ring
  entry's payload in order over a ring, and sends them as a single
  `writev(2)` with no copy at all over `/dev/fuse` — so even the fallback
  does strictly less work than today's `contiguous()` + `data()`. Up to
  four segments are handled without allocating. Plan 38 Z2a is what wires
  the adapter to it; this patch only provides it, with tests on both
  transports.
- **One flaky test of the fork's own was fixed.** `uring::mem`'s
  `vm_flags` helper took a mapping's address range from one
  `/proc/self/maps` read and then looked that exact range up in a separate
  `/proc/self/smaps` read. A concurrent test creating or freeing a mapping
  in between makes the kernel merge or split the entry, after which the
  range starts no `smaps` line and the helper panicked on `None`
  (reproduced 1 run in ~12 here). It now finds the entry covering the
  address in `smaps` alone. This is a test-harness race, not a transport
  bug.
- **One upstream 0.18.0 bug had to be worked around.** 0.18.0 requests
  `FUSE_ABORT_ERROR` but its event loop treats only `ENODEV` as the end of
  the connection, so an administratively aborted mount (fusectl's `abort`)
  surfaces as an `ECONNABORTED` *error* from `Session::run` rather than a
  clean end. Upstream fixed this after 0.18.0 (fuser issue #212, part of
  the fork's own unrelated drift). The fork's
  `ring_session_ends_cleanly_after_abort` test is what found it here, on a
  real 7.0 kernel. Rather than change the shared loop -- which would change
  what a plain `/dev/fuse` session returns -- the ring transport's own
  `/dev/fuse` reader in `serve_ring` maps `ECONNABORTED` to a clean end. A
  future re-vendor that picks up the upstream fix should delete that arm.
- **The ring session tests were re-homed.** 0.18.0 has no test module in
  `session.rs` or `lib.rs`, so the fork's `uring_test` module (its
  real-kernel coverage) landed with a small `mod test` carrying just the
  `fusectl` helpers it needs, and `lib.rs`'s single ring test moved to
  `uring/mod.rs`. Three of the fork's tests were adapted to 0.18.0's
  narrower `Filesystem` trait (`open`/`setattr` without `kill_suid_gid`)
  and to patch 0001's `negotiated: Option<NegotiatedInit>`.

### The ring budget, as it actually sizes itself

Plan 38 §4 works out the ring payload budget as `queues x depth x
max_write` and leaves the `max_write` choice to this milestone, "measured
against real RSS, not assumed here". Measured, on the 8-CPU / 8 GiB kernel
7.0 test box, from fuser's own `debug!` at ring creation:

```
io_uring: 8 queues over 2 rings, depth 8, payload 16777216 bytes per entry,
          1074003968 bytes reserved
```

That is **1.00 GiB of address space per mount** — 8 possible CPUs × depth 8
× (one page of header area + 16 MiB of payload) — and it does not depend on
the ring count, only on the kernel's possible-CPU count: `RingSet` spreads
the same 8 queues over however many rings `n_threads` asks for (2 rings ×
32 entries above, 1 ring × 64 entries in the single-worker case). It lands
at the **top** of §4's predicted 64 MiB–1 GiB span, for one reason:
`constellation-frontend-fuse` never calls `KernelConfig::set_max_write`, so
the payload is sized from fuser's default `MAX_WRITE_SIZE` of 16 MiB.

Two things keep that honest rather than alarming, and one is a decision for
plan 38 Z1b:

- The mapping is `MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE`, so what is
  reserved is *address space*: a page becomes resident only when a request
  or reply touches it. The number above is the ceiling, not the RSS.
- Pages a request touched are never returned, so a client that keeps every
  entry busy with full-sized writes can walk the RSS up to that ceiling.
  **Sizing the ring payload — `set_max_write`, a smaller
  `io_uring_queue_depth`, or both — is therefore a Z1b decision**, taken
  against Z1b's fio gate's measured RSS rather than against this ceiling.
  Z1a only establishes what the ceiling is and where it comes from.

§4 also asks for the *measured* RSS next to that range, on Constellation's
own worker count. Measured on the same box (8 CPUs, kernel 7.0,
`enable_uring=Y`, 6 FUSE workers = `2·⌈√8⌉`, one view mounted, the same
io-uring build both times, `/proc/<daemon>/status` idle and after a 128 MiB
`dd` write + full read back):

| | `/dev/fuse` | ring |
|---|---|---|
| `VmSize`, idle | 3,315,192 kB | 4,228,880 kB |
| `VmRSS`, idle | 41,008 kB | 40,972 kB |
| `VmRSS`, after 128 MiB written and read | 262,044 kB | 237,904 kB |

So the ring costs **+0.87 GiB of address space and no measurable RSS** at
this workload — the ceiling really is a ceiling. It takes a client that
keeps every entry busy with full-sized writes to approach it, which is what
Z1b's fio gate is for; the sizing decision stays Z1b's.

For comparison, today's `/dev/fuse` path already spends `workers x 16 MiB`
of *touched* per-worker request buffer (`FUSE_BUFFER_BYTES`,
`crates/frontend-fuse/src/threads.rs`) — 96 MiB at this box's 6 workers
(`recommended_workers`'s `2·⌈√8⌉`) — so the ring's cost is an addition to
that, not a replacement for it.

### `Transport`, and why a ring session can never be handed over

Milestone Z0a (`bench/fuse-uring-handover/RESULTS.md`: a raw-uapi C server,
4 variants × 5 repeats, three kernels, 60 runs) established that a FUSE
connection served over io_uring can be **neither handed over losslessly nor
downgraded back to `/dev/fuse`**. The connection survives the old process
(60/60), but once every queue has had an entry the kernel switches
`fiq->ops` to the ring and never switches back, so the next process's
`read(2)` on `/dev/fuse` gets nothing; re-registering works mechanically
(720/720) yet orphans every request that sat in an old entry, leaving its
caller **unkillable** until the connection is aborted through fusectl. Plan
38 §3(e) therefore pins handover-capable sessions to `/dev/fuse` for good
and requires `detach` to refuse anything else.

This patch is where that refusal lives:

- New public `Transport { DevFuse, Uring, UringZeroCopy }` (`Display` and
  `name()` for logs, metric labels and error text). `UringZeroCopy` is a
  declared-but-never-negotiated placeholder for plan 38 Z4.
- `NegotiatedInit` gains `transport: Transport`, set by the handshake to
  whatever the session ended up with. `Session::transport()` reports it.
  The field is `#[serde(default)]` (to `DevFuse`, which is what every
  connection negotiated before this patch was): `NegotiatedInit` is what
  crosses `constellation daemon --upgrade`'s `exec`, and that upgrade
  detaches every session *before* the new image parses the handoff, so a
  required field here would destroy every mount of any pre-0002 daemon
  being upgraded to a 0002 one, with no rollback. Covered by
  `crates/frontend-fuse`'s `a_pre_z1a_handoff_parses_as_dev_fuse`.
- **`NegotiatedInit::check_resumable()` refuses any non-`DevFuse`
  transport**, with an error naming it — so `Session::from_fd_resumed`
  cannot serve a ring connection, and a `from_fd_resumed` session is
  `DevFuse` by construction.
- **`Session::detacher()` refuses to arm a ring session** at all, rather
  than promising a handover the kernel cannot honor.

`Transport`, the `NegotiatedInit` field and the `check_resumable` refusal
are **not** behind `#[cfg(feature = "io-uring")]`, deliberately: a build
*without* the feature must still refuse to resume a connection a build
*with* it negotiated over a ring, rather than silently read `/dev/fuse` and
hang.

### What a build without the feature does and does not get

Everything that *touches* io_uring is behind
`#[cfg(all(feature = "io-uring", target_os = "linux"))]`: the whole
`src/uring` module, `Session::ring`, `create_rings`, `serve_ring` and its
thread supervisor, `SessionEventLoop::handle_fetch`,
`ReplySender::Ring`, `Channel::device`, `KernelConfig::enable_io_uring`.
Without the feature the `io-uring` crate is not in the dependency graph at
all (it is a `cfg(target_os = "linux")`-only **optional** dependency, so
`cargo tree -e features -p fuser` shows it only when the feature is on),
nothing calls `io_uring_setup(2)`, nothing maps a ring buffer, and no
thread but upstream's own `/dev/fuse` readers is spawned.

What the patch does add unconditionally, and why each has to be there:

| Unconditional surface | Why |
|---|---|
| `Transport`, `NegotiatedInit::transport`, `Session::transport()`, the `check_resumable` refusal | A feature-off build must refuse a ring handoff a feature-on build produced (above). |
| `Config::io_uring`, `Config::io_uring_queue_depth`, `validate_transport` | So `Session::new`/`from_fd` can **refuse** `io_uring: true` in a build that cannot serve it, up front and by name, instead of ignoring it. The fork's own design. |
| `add_capabilities` refusing `FUSE_OVER_IO_URING` | Echoing the bit without registered queues makes a mount unservable, feature or no feature. |
| `ReplyData::fill` (and `ReplySender::fill`, `EioOnUnwind`, `log_send`) | `fill` is a transport-independent API: over a ring it writes the entry's payload in place, over `/dev/fuse` it fills a heap buffer and `writev`s it, behaving exactly like `data()`. Gating it would make the caller (`crates/frontend-fuse`'s `ReadReply`, plan 38 Z2) need two code paths for no gain. |
| `RequestWithSender` holding a `ReplySender` rather than a `ChannelSender` | One field's type; `ReplySender::Channel` is what a `/dev/fuse` request carries, exactly as before. |

So a feature-off build is not *byte*-identical to pre-0002 fuser — it
carries the table above — but it contains **no io_uring code, no io_uring
dependency and no new threads**, and every behaviour a `/dev/fuse` mount
can observe is unchanged (fuser's own suite: 66 tests before 0002, 72
after, all passing, the six new ones being `Config::default`'s two new
fields, `validate_transport`'s refusals, `ReplyData::fill` over
`/dev/fuse` (twice) and `ReplyData::gather` over `/dev/fuse`).

### What uses it, and the proof

`crates/frontend-fuse/src/session.rs`: `MountOptions::io_uring` asks for
the transport (runtime-negotiated, never granted unless the build, the
kernel and the process's capabilities all allow it),
`FuseSession::transport()` reports what was granted, `FuseHandoff::transport()`
reports what a handoff carries, and `SessionControl::detach` refuses a
non-`DevFuse` session **first**, before anything is quiesced, with
`Code::NotSupported` and a message naming the transport. Its tests
(`a_mount_that_asks_for_the_ring_serves_on_whatever_it_gets`,
`resume_refuses_a_ring_handoff`) assert the transport the running kernel
can actually grant, so the same test is the fallback gate on a
`fuse.enable_uring=N` host and the ring gate on a 6.14+ one.

Every mount Constellation makes today leaves `io_uring: false`. Plan 38
Z1a's `CONSTELLATION_FUSE_URING=1` is a **test hook** so the smoke and
harness lanes can run a whole daemon on the ring; Z1b replaces it with a
user-facing `TransportPolicy` and `CONSTELLATION_FUSE_TRANSPORT`.

### Upgrading fuser with both series

Unchanged from the section above, with one addition: a conflicting hunk in
0002 is re-made by hand on the new source and the patch regenerated as

```
git diff --no-index <pristine-plus-0001> vendor/fuser
```

(the *pristine plus 0001* tree, not the pristine one — 0002 applies on top
of 0001). Then re-run fuser's own suite both ways
(`cargo test --manifest-path vendor/fuser/Cargo.toml [--features io-uring]`)
on a host with `fuse.enable_uring=Y`, where the 18 real-kernel ring tests
actually run.
