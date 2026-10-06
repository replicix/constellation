# Vendored fuser 0.18.0: Constellation's patch series

Four named series live in `patches/`, applied in filename order:

1. `0001-constellation-session-handover.patch` — plan 31 §6.11 / C4b. This
   file's first half.
2. `0002-io-uring-transport.patch` — plan 38 §3(a) / Z1, behind the
   `io-uring` cargo feature, off by default. "The io-uring transport",
   below.
3. `0003-interrupt.patch` — plan 39 §3.3. "FUSE_INTERRUPT", at the end.
4. `0004-ring-idle-wait.patch` — the `ring-stress-hang` fix. "The ring's idle
   wait", after it.

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

Plan 38 (`docs/plans/v1/done/38-fuse-read-path-transport.md`) §3(a),
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
| c | **Its own test suite** | **Runs, and passes, against *our* copy.** The fork's suite: `master` 109 passed / 1 failed, the ring stack 130/1 without the feature and **209/1** with it (the single failure, `mnt::test::mount_unmount_external`, is pre-existing on `master` and a privilege problem — `EPERM` from the external mount helper with no root). Re-run against this vendored tree after the lift: **72 passed / 0 failed** without the feature, **157 / 0** with it (including the three this patch adds for the gather form and `Transport`, and plan 38 Z1b's transport-parity test), of which 18 real-kernel ring tests skip loudly on a `fuse.enable_uring=N` host and run on a 6.14+ one — all 156 pass on a kernel 7.0 host with `fuse.enable_uring=Y`, where they do run. The fork's own `fuse_over_io_uring_tests_ran` guard **fails** if the kernel advertises the flag while those tests skipped, so a silently-never-exercised ring cannot pass as green. | `cargo test --lib [--features io-uring]` in `/tmp/skory-fuser` and in `vendor/fuser`. The count differs from the fork's because the fork's suite also covers its unrelated drift (statx, tmpfile, idmap), which is not vendored. |
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
- **An in-memory ring seam for transport-parity testing, and the parity
  test itself.** Plan 38 §6 asks for a `tests/wire.rs`-style adapter test
  proving "the adapter's translation layer (fuser decode → `Vfs` call →
  responder → fuser encode) is identical regardless of transport, without
  a real kernel or root". The fork has no such test and no seam for one:
  its ring coverage either mounts a real kernel mount or pokes an entry's
  state machine. `uring::ring::test::dispatch_over_a_fake_ring` is the
  seam — it scatters a contiguous `/dev/fuse`-shaped request into an
  entry of the fork's own `fake_ring` (a `Ring` over `/dev/zero`, no
  io_uring at all, since `Ring::new` and `RingIo::open` are already
  separate), stages it exactly as the ring thread does, dispatches it
  through the same `SessionEventLoop::handle_fetch`, and reads the reply
  back out of the entry in the `/dev/fuse` wire shape. The test,
  `session.rs`'s `transport_parity_is_byte_for_byte`, dispatches eight
  requests (an entry reply, an errno reply, an attr reply, a read
  answered by `data()` *and* by `fill()`, a request carrying a payload of
  its own, an incrementally built `readdir`, and an xattr size probe)
  down both transports and asserts the reply bytes are equal. It runs
  anywhere — no root, no mount, no `fuse.enable_uring=Y` — which is the
  point: the 18 real-kernel ring tests skip on the dev host, and this
  one does not. Plan 38 Z2a added the Constellation-side counterpart
  over a whole session (`InMemoryRingKernel`, below) rather than
  exporting `SessionEventLoop`, `RingCommit` and an entry constructor.

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

### Plan 38 Z2a: the adapter integration's hunks

Z2a (the FUSE adapter on the ring) added six things to this patch, each
marked `CONSTELLATION PATCH (io-uring)`:

- **Blocking callbacks leave the ring thread** (`session.rs`:
  `dispatch_on_ring`, `offload_thread`; `uring/ring.rs`: `RingCommit::hold`,
  `HeldRequest`, `Ring::finish_dispatch`). A ring thread serves every
  request the kernel queues on its CPUs' queues one at a time, so a callback
  that blocked on it blocked them all — Z1b's `s3-cut-one-node` ring-leg
  finding (a `stat` behind a `close` whose flush waited on a cut S3). A ring
  thread now dispatches only `READ`, `GETATTR`, `READLINK`,
  `GETXATTR`, `LISTXATTR`, `STATFS`, `ACCESS`, `RELEASEDIR` and `DESTROY`
  itself (`READDIR[PLUS]` was on the list at first; a listing from its start
  may make a `--cto strict` read-position round trip to the sequencer, the
  reason `LOOKUP` is not on it either, so it left in the Z2a review round);
  everything else (and any opcode the list does not know) is
  *held* — `RingCommit::hold` marks the entry, the ring thread returns to
  its queues, and the `HeldRequest` carries the request, still in the
  entry's own buffers, to one of `n_threads` `fuser-offload-<i>` threads,
  whose dispatch is exactly the ring thread's. While held, a reply from any
  thread is stashed (`Deferred`) even for a request without a payload: a
  direct reply would re-arm the entry and the next fetch would be staged
  over the held slice. Dropping the `HeldRequest` runs `finish_dispatch`
  (the end of dispatch the ring thread used to run inline: write a stashed
  reply, answer a request given no reply object, or wait as `Dispatched`).
  The `HeldRequest` doc is the one place the borrow invariant is stated: a
  `FUSE_WRITE`'s data is a slice of the entry's payload buffer — no copy —
  and the entry cannot be re-armed before that slice is dead. Measured:
  the stall test in `crates/frontend-fuse/tests/wire_uring.rs` fails with
  every opcode on the ring and passes with the split; `s3-cut-one-node`
  passes seeds 1–8 on `auto`. An offload-thread panic stops every ring
  entering the filesystem (`RingHandler::offload_panicked`), since the
  offload threads serve all rings; a ring-thread panic still confines itself
  to its ring.
- **Blocking lock requests may hold at most `depth - 1` entries of a queue**
  (`uring/ring.rs`: `RingCommit::reserve_lock_wait`, `Live::lock_waits`,
  `HeldRequest::downgrade_lock_wait`; `session.rs`: `RingHandler::handle`).
  A ring request holds its entry until it is answered, and the kernel queues
  a request on its CPU's queue until an entry of that queue is free — it can
  be answered nowhere else (`/dev/fuse` does not find it: it is on the
  queue's own processing list). A `FUSE_SETLKW` that waits for a lock holds
  its entry for as long as the holder takes, and the holder's own next
  request (a `write()` before its unlock) may be queued on the same CPU: with
  every entry of that queue held by waiters, nothing frees one — a deadlock
  `/dev/fuse` cannot have, where a waiting request holds nothing. So a ring
  thread counts each `FUSE_SETLKW` it fetches against its queue's budget
  until the entry's reply is committed (or the entry dies); one that would
  exceed `depth - 1` is served as `FUSE_SETLK` — the request is small, so a
  rewritten copy is dispatched — and a contended answer (`EAGAIN`) is
  committed as `ENOLCK`, an error both `F_SETLKW` and `flock(2)` document
  for exhausted lock resources. The protocol leaves nothing better: an
  errno is the only answer that frees the entry without waiting.
  `crates/frontend-fuse/tests/wire_uring.rs`'s
  `blocking_lock_waits_never_take_a_queues_last_entry` fails without the
  budget (its second waiter takes the queue's last entry and is never
  answered) and passes with it.
- **Each budget downgrade is reported** (plan 38 Z2c; `uring/mod.rs`:
  `LockWaitDowngrades`; `mnt/mount_options.rs`:
  `Config::io_uring_lock_wait_downgrades`; `uring/ring.rs`:
  `Ring::lock_wait_downgrades`, called from `HeldRequest::downgrade_lock_wait`).
  A downgraded request is answered differently from what the caller asked
  (`ENOLCK` instead of a wait), and the filesystem never sees the original
  opcode — the rewritten `FUSE_SETLK` is all that reaches `Filesystem::setlk`
  — so only this crate can count them. The hook runs on the ring thread,
  once per downgrade, before the dispatch; Constellation counts per session
  and process-wide (`node.status`'s `lock_wait_downgrades`,
  `constellation_fuse_lock_wait_downgrades_total`). `wire_uring.rs`'s budget
  test asserts it fires exactly once for its one downgrade.
- **A refused registration is a constructor error**
  (`uring/ring.rs`: `register_all`; `uring/mod.rs`: `RegistrationRefused`).
  The kernel validates a `REGISTER` when it is issued and posts a refusal's
  CQE inside the same `io_uring_enter`, so `register_all` reaps right after
  its submit; a refusal retires the refused entries (nothing is leaked) and
  fails `RingSet::start`, and so the session's constructor, with
  `RegistrationRefused` (`RegistrationRefused::is` tells it apart). Before,
  the refusal surfaced in `serve` as a fatal CQE once the session already
  ran, with every request on the mount blocked on queues that never became
  ready. The INIT reply has committed the connection to rings by then, so
  the caller's fallback is a new mount (`crates/frontend-fuse`'s
  `mount_source`). `Config::io_uring_malformed_register` (hidden) makes
  every REGISTER name one iovec instead of two, which the real kernel
  refuses with `EINVAL` — the fault the harness's
  `transport-refused-registration` provokes. The ring unit tests that use a
  non-FUSE device's synchronous `EOPNOTSUPP` to exercise `serve`'s fatal-CQE
  path keep it through a test hook (`IoHooks::refusals_in_serve`).
- **`InMemoryRingKernel`** (`uring/memory.rs`, exported; `Config::io_uring_kernel`,
  hidden): the seam for testing a whole ring session without a kernel. `RingIo`
  has two backends now, the kernel's io_uring and this; the memory backend
  decodes the SQEs a ring thread pushes the way `fs/fuse/dev_uring.c` does
  (`REGISTER` with its two iovecs — and refuses a malformed one as the kernel
  does —, `COMMIT_AND_FETCH`, the wake eventfd's `POLL_ADD`), writes each
  request into a registered entry through those iovecs, and reads each reply
  back out. `FUSE_INIT`/`DESTROY` still go over the session's descriptor (a
  socket pair in tests). `crates/frontend-fuse/tests/wire_uring.rs` is its
  user.
- **`ReplyData::transport()`**, so the adapter's read reply keeps the
  `/dev/fuse` path byte for byte (`contiguous()` + `data()`) and uses
  `gather` only over a ring.
- **`gather` no longer zeroes the payload first** (`RingCommit::fill_with`,
  `ReplySender::fill_with`): it overwrites every byte it reports, so `fill`'s
  memset only doubled the copy. The unzeroed bytes are the entry's own
  (anonymous memory, written only by this entry's requests and replies).

`ring_session_ends_cleanly_after_abort` was adapted: the `RELEASE` after
its read now runs on an offload thread and may still be held when the
abort lands, so its reply may be dropped (debug level) once the ring has
left; the test still requires nothing left in the kernel and no error.

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
  ring session whose queues are all zero-copy queues (plan 38 Z4a, below).
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

### Plan 38 Z4a: zero-copy queues on 7.3 (ABI 7.46)

Plan 38 §3(d) and milestone Z4, fuser side. A session that gets the ring
also tries **zero-copy queues** when the kernel offers buffer pools and the
process has `CAP_SYS_ADMIN`; `Transport::UringZeroCopy` is what it then
negotiates. A filesystem marks an open for zero-copy with
`ReplyOpen::opened_zero_copy` and answers its reads with
`ReplyData::read_fixed(fd, offset, len)`. Constellation's reads are routed
to it by plan 38 Z4b, outside this crate: the engine marks read-only opens
(`Opened::zero_copy`, answered with `opened_zero_copy`) and answers a read
inside one verified chunk with the chunk file, which `crates/frontend-fuse`'s
`ReadReply` hands to `read_fixed` (`docs/plans/v1/PROGRESS.md`, "Plan 38 Z4").
Constellation asks for zero-copy queues only when
`CONSTELLATION_FUSE_URING_ZERO_COPY` is `auto` or `pinned` (opt-in since
Z4b's measurements); this crate's own `Config::io_uring_zero_copy` default
is unchanged.

#### The ABI, re-verified against the running kernel (2026-10-02)

The plan assumed (REPORTED, from Joanne Koong's `[PATCH v7 0/6] fuse: add
io-uring buffer pools`) that this ABI lands in 7.3. It does. The zero-copy
box runs `7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64` (Fedora Rawhide,
`fuse.enable_uring=Y`, `CONFIG_FUSE_IO_URING=y`). The constants and structs
below are copied from `include/uapi/linux/fuse.h` of that exact commit
(`torvalds/linux` `165768bb70265b5c38cf0b73fafd75be235f8b14`, `Makefile`
`7.3.0-rc4`, `FUSE_KERNEL_MINOR_VERSION 46`). The installed
`kernel-headers-7.3.0-0.rc5` `/usr/include/linux/fuse.h` is identical apart
from the `#ifdef __KERNEL__` include guard.

| Item | Value (uapi) | Where this patch uses it |
|---|---|---|
| ABI version | 7.46: "add `FUSE_IO_URING_CMD_ADD_QUEUE`, `FUSE_HAS_IO_URING_BUFPOOL`, `fuse_uring_cmd_req` bufpool struct, bufpool offset field to `fuse_uring_ent_in_out`, `FUSE_URING_ZERO_COPY`, `FUSE_URING_ENT_ZERO_COPY` and `FOPEN_IO_URING_ZERO_COPY`" | `ll/fuse_abi.rs` |
| `FUSE_IO_URING_CMD_ADD_QUEUE` | `3` (`enum fuse_uring_cmd`) | `fuse_uring_cmd`, `Ring::add_queue_sqe` |
| `FUSE_IO_URING_CMD_ADD_BUFPOOL` | `4` | `fuse_uring_cmd`, `Ring::add_bufpool_sqe` |
| `FUSE_URING_ZERO_COPY` | `(1 << 0)`, `fuse_uring_cmd_req.flags` of an `ADD_QUEUE`; "only supported for queues with bufpools on privileged servers" | `abi::FUSE_URING_ZERO_COPY` |
| `FUSE_URING_ENT_ZERO_COPY` | `(1 << 0)`, `fuse_uring_ent_in_out.flags` on fetch: "the ent's payload is zero-copied" | `abi::FUSE_URING_ENT_ZERO_COPY` |
| `FUSE_HAS_IO_URING_BUFPOOL` | `(1ULL << 43)`, INIT flag: "kernel supports io-uring buffer pools" | `InitFlags::FUSE_HAS_IO_URING_BUFPOOL` |
| `FOPEN_IO_URING_ZERO_COPY` | `(1 << 8)`; "Honored only when the serving io-uring queue was set up for zero-copy (`FUSE_URING_ZERO_COPY`) and the request carries page payload. Otherwise reads/writes fall back to copying." | `FopenFlags::FOPEN_IO_URING_ZERO_COPY` |
| `struct fuse_uring_ent_in_out` | `u64 flags; u64 commit_id; u32 payload_sz; u32 offset; u64 reserved` (`offset`: "Offset into the bufpool, if bufpools are used"; 7.42's `padding`) | `fuse_uring_ent_in_out::offset`, `mem::POOL_OFFSET_OFFSET` (276) |
| `struct fuse_uring_cmd_req` | `u64 flags; u64 commit_id; u16 qid; u8 padding[6]; union { struct { u64 uaddr; u32 len; u32 reserved; } bufpool; u16 ent_zero_copy_buf_index; }` | `FUSE_URING_CMD_REQ_UNION_OFFSET` (24), `fuse_uring_bufpool` |
| `IORING_URING_CMD_FIXED` | `(1U << 0)` in `sqe->uring_cmd_flags` (`include/uapi/linux/io_uring.h`) | set by `io-uring`'s `UringCmd80::buf_index(Some(_))` |
| `IORING_OP_READ_FIXED` | `4` | `opcode::ReadFixed` |

And from `fs/fuse/dev_uring.c` at the same commit (source reading), the
behaviour the code relies on:

- **`ADD_QUEUE`** (`fuse_uring_add_queue`): `-EINVAL` for an unknown flag or
  `qid >= nr_queues`; **`-EPERM` for `FUSE_URING_ZERO_COPY` without
  `capable(CAP_SYS_ADMIN)`** (checked in the initial user namespace);
  `-EEXIST` for a queue that already exists (a REGISTER creates a missing
  queue itself, without zero-copy). Completes inline, never `-EIOCBQUEUED`.
- **`ADD_BUFPOOL`** (`fuse_uring_add_bufpool`): the queue must exist (so
  after `ADD_QUEUE`) and have no payload mode yet (so before its first
  REGISTER); `flags` and `reserved` must be 0; the pool is cut into
  `len / max_payload_sz` buffers; it is *registered* iff the SQE carries
  `IORING_URING_CMD_FIXED`, and `sqe->buf_index` is then its fixed-buffer
  index, which **every REGISTER and COMMIT_AND_FETCH of that queue must name
  again** (`fuse_uring_cmd_index_ok`). The fixed buffer is only looked up
  when a payload is imported, in the ring the command came from.
- **REGISTER on a pool queue** must name an empty payload iovec; **a
  zero-copy queue refuses an entry without a pool** ("Can only use zero copy
  with bufpools", `-EINVAL`), and `ent_zero_copy_buf_index` must be 0 on a
  queue that is not zero-copy.
- **The zero-copy read** (`fuse_uring_set_up_zero_copy`, `can_zero_copy_req`):
  only `FUSE_READ`/`FUSE_WRITE` with page arguments, of an open whose
  `FOPEN_IO_URING_ZERO_COPY` was set, on a zero-copy queue. The kernel
  registers the request's folios (pinned user pages under `O_DIRECT`, page
  cache folios otherwise) with `io_buffer_register_bvec` into the entry's
  slot of the **issuing ring's** buffer table, sets `FUSE_URING_ENT_ZERO_COPY`
  and selects no pool buffer for a read (nothing copyable). A
  kernel-registered buffer starts at address 0, so `READ_FIXED` names offsets
  into the request's pages. On commit the kernel skips the folio copy
  (`skip_folio_copy`) and takes `payload_sz` as the reply's length; the slot
  is unregistered when the request ends. Writes are zero-copied the same way:
  their data is only in the registered pages.
- **A failed `ADD_QUEUE` or `ADD_BUFPOOL` does not disable the ring** (only a
  failed REGISTER sets `fch->io_uring = 0`, Z0a's trap): the command returns
  its error with a `pr_info_once`, and the connection is still a ring
  connection. What *does* trap is the order: a queue created for zero-copy
  refuses an entry with its own payload buffer, and that refusal is a failed
  REGISTER. So every decision is made before the first REGISTER (below).

**Observed on the real kernel** (`session::uring_test::read_fixed_serves_a_real_mount_on_whatever_transport_it_gets`,
run as root inside `unshare -U -r -m`, where `CapEff` shows `CAP_SYS_ADMIN`
but the kernel's `capable()` does not): the first zero-copy `ADD_QUEUE` came
back `-EPERM` synchronously (dmesg: `FUSE_IO_URING_CMD_ADD_QUEUE failed
err=-1`, once), the session logged `io_uring zero-copy unavailable: the
kernel refused zero-copy queue 0 (Operation not permitted (os error 1));
serving io_uring without it` once, registered every entry with its own
payload buffer, and the mount served `dd iflag=direct` and buffered reads
byte for byte on plain `Uring`. In the same namespace with the pool
registered, the first refusal is the buffer table's: pinning the pool is
charged to `RLIMIT_MEMLOCK` there (`ENOMEM`), and the session degraded the
same way.

**Operator-visible:** a session that tries zero-copy negotiates `max_write`
= `max_pages x PAGE_SIZE` (1 MiB at the default `max_pages_limit` of 256),
not fuser's 16 MiB, so the INIT reply, `NegotiatedInit::max_write` and the
daemon's own debug line (`INIT response: ... max write 1048576`) say 1 MiB on
a `uring_zc` mount -- and on a mount that tried zero-copy and degraded to
`uring`, since the value is settled before the INIT reply. No request changes
size (the kernel never sends more than `max_pages` pages). Neither
`node.status` nor the harness's transport census reports `max_write` today.

#### What the patch does

- **Negotiation** (`Session::zero_copy_plan`, at INIT, before the reply):
  tried only when `Config::io_uring_zero_copy` (default `true`), the kernel's
  INIT offers `FUSE_HAS_IO_URING_BUFPOOL` (never assumed from a version) and
  the process has `CAP_SYS_ADMIN` (`capget(2)`; an `InMemoryRingKernel` stands
  in for it in tests). Each "no" is a debug line, not a failure: without the
  capability nothing is attempted at all.
- **Pool sizing** (plan 38 §4): a pool buffer is the kernel's
  `max_payload_sz = max(8192, max_write, max_pages * PAGE_SIZE)`, with
  `max_pages` as the kernel holds it (clamped to
  `/proc/sys/fs/fuse/max_pages_limit`, or 32 without `FUSE_MAX_PAGES`). It must
  be exact (`PoolMemory`): the kernel reports a buffer by its byte offset and a
  reply written past it would land in the next one. A session that tries
  zero-copy lowers `max_write` to `max_pages * PAGE_SIZE` first, because no
  read or write request carries more than `max_pages` pages whatever
  `max_write` says: fuser's default 16 MiB `max_write` becomes 1 MiB (256
  pages), which changes no request and keeps each buffer the size of a
  request. Each queue gets `depth` buffers (`io_uring_queue_depth x
  max_payload_sz`), so a request is never held back for want of a buffer that
  plain `Uring` would have had. One mapping per ring (per worker) holds its
  queues' pools, mapped after everything else the ring needs (entries, ring
  threads), so a limited address space (`RLIMIT_AS`) costs zero-copy before
  it costs the ring. Slot `i + 1` of the ring's buffer table is entry `i`'s
  zero-copy slot (libfuse's draft uses the same layout).
- **Registered or not** (`Config::io_uring_register_pool`, **default
  `false`**). Unregistered, the pool is plain memory the kernel imports per
  request (`import_ubuf`, `fs/fuse/dev_uring.c`): no `IORING_URING_CMD_FIXED`,
  no `buf_index` on REGISTER/COMMIT_AND_FETCH, nothing pinned up front. Its
  pages become resident as requests first use them **and are never given
  back**, so a busy session converges on the whole pool, `possible CPUs x
  depth x max_payload_sz` (256 MiB at 32 CPUs, depth 8, 1 MiB; 1 GiB at depth
  32) -- measured below. Registered (`true`, Constellation's `pinned`), the
  pool is the fixed buffer at index 0 of the ring's buffer table, every page
  pinned and resident from the mount on and charged to `RLIMIT_MEMLOCK`
  without `CAP_IPC_LOCK`; the kernel then skips the per-request import.
- **Setup before the first REGISTER** (`Ring::set_up_table`,
  `Ring::set_up_queues`, `RingSet::start`), three session-wide barriers:
  each ring thread, once enabled, registers its buffer table and reports;
  only if **every** ring's table was taken does any ring go on (otherwise
  every ring withdraws its table, nothing having been created in the kernel),
  sending `ADD_QUEUE(FUSE_URING_ZERO_COPY)` for its first queue, then the
  rest, then `ADD_BUFPOOL` for every queue; `RingSet::start` waits for every
  ring's outcome, settles the session's transport, tells every ring
  (`Ring::set_session_zero_copy`) and logs it once, and only then lets any
  ring register. So the likeliest refusal -- a table the process may not pin
  or account (`ENOMEM`) -- can never leave one ring zero-copy and another not:
  one session, one transport, and `ReplyData::transport` reports the
  session's on every reply. A refused table or first `ADD_QUEUE` leaves
  nothing in the kernel: the entries register with their own payload buffers
  (plain `Uring`), and the reason is logged once. Past the first queue
  the ring is committed to pools (a zero-copy queue accepts nothing else): a
  later queue the kernel only creates without zero-copy, or a pool it only
  takes unregistered, is a note in that one log line (the session is then
  `Uring` resp. still `UringZeroCopy`); only a queue or pool refused outright
  fails the session's constructor with `RegistrationRefused`, as a refused
  REGISTER does, so the caller's existing fallback (a fresh mount on
  `/dev/fuse`) applies.
- **Fetches on a pool queue** (`Ring::locate_payload`): the reply goes to the
  pool buffer the kernel picked (`fuse_uring_ent_in_out.offset`), and a
  request's payload is copied from it to the entry's own buffer, where the
  staged header continues into it as before (`stage_request` is unchanged).
  A request the kernel gave no buffer (nothing to copy either way: `FLUSH`,
  `RELEASE`, `FSYNC`, ... -- `fuse_uring_req_has_copyable_payload`) arrives
  at offset 0, indistinguishable from the pool's first buffer, which another
  entry may hold; an opcode whose reply the kernel reads no payload from
  (`has_out_args`) therefore gets a reply capacity of 0, so a reply with a
  payload is `EINVAL` (`write_reply`) rather than a write into that buffer.
  Each kind of refused fetch (bad offset, oversized payload, zero-copied
  non-read) is logged once on its own.
  The trade-off, stated: a `FUSE_WRITE`'s data is one copy away from the
  kernel's on a pool queue, where an entry's own buffer lends it with none.
  A zero-copied request that is not a read is answered `EIO`, logged once:
  its data is only in the registered pages, which nothing here hands to
  `Filesystem::write` (`opened_zero_copy` is documented as read-only opens
  only).
- **`ReplyData::read_fixed(src, offset, len)`** (`RingCommit::read_fixed`):
  on a zero-copied request the entry goes `PendingRead` -> `Reading` (two new
  states, counted `in_kernel`, so the ring never exits under a read), the ring
  thread pushes one `IORING_OP_READ_FIXED` from `src` into the entry's slot
  (`user_data` bit 63), and its completion writes the out header and
  `payload_sz = res` (or the error) and pushes the COMMIT_AND_FETCH. `src` is
  owned until then. Callable from any thread, like `fill`: a foreign caller
  queues the read and wakes the ring thread. Anywhere else -- `/dev/fuse`, a
  ring without zero-copy, a read the kernel did not zero-copy -- it is a
  `pread(2)` into the reply buffer through `fill_with`. A read stashed while
  its request is held (`Stashed::Read`) goes out from `finish_dispatch`.
- **Any other reply to a zero-copied request** (`data`, `fill`, `gather`,
  which is what Constellation's adapter sends today) is bounced into the
  pages (`Ring::bounce_in`): written into a per-ring memfd at the entry's own
  range, then one `READ_FIXED` from it, the range punched out afterwards. A
  short `READ_FIXED` from the memfd is `EIO`, never a truncated reply (from
  the caller's file a short read is the end of the file and is replied as
  such). Two copies instead of a ring's one; this is the path plan 38 §3(d) leaves to
  chunk-spanning reads, and it means `opened_zero_copy` can never make a reply
  silently wrong. (libfuse's draft bounces through a pipe, whose capacity a
  1 MiB reply exceeds.)
- **`ReplyOpen::opened_zero_copy(fh, flags)`** sets `FOPEN_IO_URING_ZERO_COPY`;
  **`ReplyData::zero_copy()`** says whether this request's pages are
  registered; **`ReplyData::transport()`** reports `UringZeroCopy` on a
  session whose queues all are.
- `Transport::UringZeroCopy` is negotiated now: `Session::transport()` and
  `NegotiatedInit::transport` (updated after `start`) report it, and
  `check_resumable`/`detacher` refuse it like `Uring`.

#### Tests

- `InMemoryRingKernel::with_buffer_pools(capable)` models a 7.3 kernel:
  `ADD_QUEUE`/`ADD_BUFPOOL` (with `EPERM` for an incapable process and knobs
  to refuse zero-copy queues, registered pools or one ring's buffer table, and
  to make every `READ_FIXED` short), pool buffers and their offsets, the
  fixed-index check, the buffer table, `send_zero_copy` (a read whose data the
  "kernel" expects in registered pages) and `READ_FIXED` served with
  `pread(2)`.
- `session::uring_test`: `zero_copy_queues_serve_reads_through_registered_pages`
  (once with the pools unregistered, the default, once registered:
  `read_fixed`, short at EOF; `data`/`gather` bounced; a read not
  zero-copied is a `pread`; getattr and a write's payload through pool
  buffers; a zero-copied write answered `EIO`; `max_write` lowered; every
  reply's transport `UringZeroCopy`),
  `without_buffer_pools_or_cap_sys_admin_the_ring_is_plain` (an INIT without
  `FUSE_HAS_IO_URING_BUFPOOL` -- a 7.0-7.2 kernel -- and a process without
  the capability: no `ADD_QUEUE`, no pool, plain `Uring`),
  `a_refused_zero_copy_queue_degrades_to_plain_uring_before_any_register`,
  `a_refused_registered_pool_is_handed_over_unregistered`,
  `a_refused_buffer_table_on_one_ring_keeps_every_ring_plain` (ring 1's table
  `ENOMEM` after ring 0's was taken: ring 0 withdraws it, no queue anywhere is
  zero-copy, every reply says `Uring`),
  `a_short_bounce_read_is_an_error_and_a_short_file_read_is_not`, and the
  real-kernel `read_fixed_serves_a_real_mount_on_whatever_transport_it_gets`
  (above), run with the pool unregistered -- the shipped default -- and, in
  the initial user namespace, registered too: as root on 7.3 both negotiate
  `UringZeroCopy` and zero-copy the `O_DIRECT` and buffered reads.
- Root-only teardown fixes found on 7.3 (the suite is 172/172 as root and as a
  user): `MountImpl::umount_impl` (`mnt/fuse_pure.rs`) treats root's `EBUSY`
  like the unprivileged `EPERM` and detaches lazily -- a session dropped
  with a caller still inside its mount was otherwise left mounted, its ring
  answering `EIO` until something ended the connection
  (`dropped_session_unmounts_and_drains`,
  `panic_in_fill_on_a_ring_thread_answers_eio`,
  `teardown_ends_a_ring_whose_entries_are_all_held`). The abandoned
  `from_fd` ring already cancels its registered commands the only way the
  kernel offers -- closing the ring, whose teardown sends each cancelable
  `URING_CMD` `IO_URING_F_CANCEL` (`io_uring_try_cancel_uring_cmd`;
  `IORING_OP_ASYNC_CANCEL` does not reach `URING_CMD`s, `io_try_cancel`), and
  the connection aborts within milliseconds; what failed in
  `dropped_from_fd_session_aborts_the_connection` was the test: a request
  that reached an entry while its ring was torn down is ended `ECANCELED`
  (`fuse_uring_send_in_task`, `tw.cancel`), which it now accepts beside
  `ENOTCONN`/`ECONNABORTED`, and its leak check now sizes the entries from
  the negotiated `max_write` (1 MiB on a zero-copy session). The leak line
  names its ring (`ring 0 leaking N bytes`), so the session tests tell their
  ring from the ring unit tests' (numbered 7) without relying on a size.
- `crates/frontend-fuse/tests/wire_uring.rs` runs its byte-for-byte parity
  round and its foreign-thread cold read on a third leg, zero-copy queues,
  with every read sent zero-copied: the adapter's `gather` replies arrive
  through the bounce, byte-identical to `/dev/fuse`.

#### Memory, measured (plan 38 §4)

Re-measured for the fix round (2026-10-02, kernel 7.3.0-rc4): 32 possible
CPUs, the daemon's 12 FUSE workers (12 rings), `--locks local`,
`CONSTELLATION_FUSE_TRANSPORT=auto`, depth 8 unless noted, one view
(`constellation mount --foreground` on a local backend). Columns: `VmSize`
and `VmRSS` of the daemon idle (2 s after the mount); `VmRSS` after **one
256 MiB workload** (256 MiB written, `drop_caches`, read back with `dd
iflag=direct bs=1M` and buffered); `VmRSS` after a **sustained** one on top
(three passes of 8 `O_DIRECT` 1 MiB readers pinned to each of the 32 CPUs,
256 concurrent, 32 MiB each); and the resident part of the buffer-pool
mapping itself (`Rss` of its VMA in `/proc/<pid>/smaps`) at the three points.

| run | transport | `VmSize` | `VmRSS` idle | after one | after sustained | pool resident idle / one / sustained |
|---|---|---|---|---|---|---|
| user, `uring` | `uring` | 6.20 GB | 44 MB | 299 MB | 781 MB | -- |
| root, zero-copy `off` | `uring` | 6.20 GB | 44-52 MB | 405-482 MB | 759-841 MB | -- |
| root, `auto` (unpinned; Constellation's default until Z4b's fix round made zero-copy opt-in) | `uring_zc` | 2.70 GB | 43-50 MB | 347-471 MB | 731-845 MB | 0 / 14-30 / **256 MB** |
| root, `pinned` | `uring_zc` | 2.70 GB | 299-303 MB | 643-713 MB | 732-862 MB | **256 / 256 / 256 MB** |
| root, `auto`, depth 32 | `uring_zc` | 4.21 GB | 44-48 MB | 425-518 MB | 827-918 MB | not isolated (*) |
| root, `pinned`, depth 32 | `uring_zc` | 4.21 GB | 1,071-1,074 MB | 1,355-1,469 MB | 1,518-1,628 MB | 1,024 / 1,024 / 1,024 MB |

(Ranges: two or three runs each.) The pinned pool is exactly its budget,
resident from the mount on: 32 queues x 8 x 1 MiB = 256 MiB, 1 GiB at depth
32 (the default for a cluster-lock mount on `uring`). The unpinned pool
costs nothing idle and a few MB after one sequential workload, but **a busy
mount converges on the same 256 MiB and keeps it**: the sustained workload,
eight requests in flight per CPU, touched every buffer of every queue, and
nothing gives a touched page back. By the same mechanism depth 32 converges
on 1 GiB under 32 requests in flight per CPU (not driven here: the sustained
workload keeps 8 in flight). (*) At depth 32 the kernel merged the
pool's VMA with a neighbour, so `smaps` does not isolate it. `VmSize` drops
because the lowered `max_write` shrinks every entry's reservation from 16 MiB
to 1 MiB. The totals are dominated by the daemon's own caches and vary by
tens of MB between runs; the pool column is the exact one.

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
| `ReplyData::read_fixed`, `ReplyData::zero_copy`, `ReplyOpen::opened_zero_copy`, `Config::io_uring_zero_copy`/`io_uring_register_pool`, the 7.46 ABI constants (plan 38 Z4a) | Transport-independent, as `fill` is: over `/dev/fuse` `read_fixed` is a `pread(2)` into a heap buffer sent with `writev(2)`, `zero_copy()` is `false`, and the kernel ignores `FOPEN_IO_URING_ZERO_COPY` outside a zero-copy queue. |

So a feature-off build is not *byte*-identical to pre-0002 fuser — it
carries the table above — but it contains **no io_uring code, no io_uring
dependency and no new threads**, and every behaviour a `/dev/fuse` mount
can observe is unchanged (fuser's own suite: 66 tests before 0002, 72
after, all passing, the six new ones being `Config::default`'s two new
fields, `validate_transport`'s refusals, `ReplyData::fill` over
`/dev/fuse` (twice) and `ReplyData::gather` over `/dev/fuse`).

### What uses it, and the proof

`crates/frontend-fuse/src/session.rs`: `MountOptions::transport`
(`TransportPolicy::Auto`) asks for the transport (runtime-negotiated,
never granted unless the build, the kernel and the process's capabilities
all allow it),
`FuseSession::transport()` reports what was granted, `FuseHandoff::transport()`
reports what a handoff carries, and `SessionControl::detach` refuses a
non-`DevFuse` session **first**, before anything is quiesced, with
`Code::NotSupported` and a message naming the transport. Its tests
(`a_mount_that_asks_for_the_ring_serves_on_whatever_it_gets`,
`resume_refuses_a_ring_handoff`) assert the transport the running kernel
can actually grant, so the same test is the fallback gate on a
`fuse.enable_uring=N` host and the ring gate on a 6.14+ one.

What asks for `io_uring: true` is plan 38 Z1b's `TransportPolicy`
(`crates/frontend-fuse`'s `MountOptions::transport`, set from
`--fuse-transport`/`CONSTELLATION_FUSE_TRANSPORT`): `Auto` asks, `DevFuse`
— still the default for every mount — does not. Z1a's blunter
`CONSTELLATION_FUSE_URING=1` test hook is gone with it.

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

## FUSE_INTERRUPT (`patches/0003-interrupt.patch`)

Plan 39 §3.3 (`docs/plans/v1/wip/39-fsync-durability.md`): an `fsync` that
waits for an unreachable S3 must end when its caller is killed (NFS
`hard`'s "killable"), and the daemon only learns of a signal through the
interrupt. Upstream 0.18 answers every `FUSE_INTERRUPT` with `ENOSYS`
(`// TODO: handle FUSE_INTERRUPT`), and the kernel's answer to that is to
set `no_interrupt` on the connection (`fs/fuse/dev.c`,
`fuse_dev_do_write`) and never send another one — after which a signalled
caller of a request the daemon already read waits it out uninterruptibly,
`SIGKILL` included (`request_wait_answer`: "Either request is already in
userspace, or it was forced. Wait it out.").

- New `Filesystem::interrupt(&self, req, unique: RequestId)`, default a
  no-op, called for every `FUSE_INTERRUPT` with the `unique` it names.
- The dispatcher sends no reply to an interrupt (the protocol needs one
  only to have it requeued, `EAGAIN`), and `Interrupt` joins the
  operations a non-owner may issue under `allow_root` (the kernel sends it
  for anyone's request).

A filesystem that ignores `interrupt` sees no change except that the kernel
keeps sending interrupts, which it does not answer. Constellation's
adapter (`crates/frontend-fuse/src/adapter.rs`, `Interrupts`) marks the
named request and cancels its `CancelToken` once the caller's thread has a
fatal signal pending (`/proc/<tid>/status`), polling while it waits — the
kernel sends one interrupt per request, for the first signal, which may be
a handled one — and remembers an interrupt that overtook its request (up
to a minute; Linux uniques are never reused). On the ring transport
interrupts still arrive over `/dev/fuse` (0002's reader thread), through
the same dispatcher, while the request they name may be served on one of
0002's offload threads: the adapter's table is shared by both.

Regenerated as `git diff --no-index <pristine-plus-0001-and-0002>
vendor/fuser`; `tools/vendor-fuser.sh --check` verifies the series.

The upstream issue to file for this patch, with a verified reproducer, is `../ISSUE-fuser.md`.

## The ring's idle wait (`patches/0004-ring-idle-wait.patch`)

The `ring-stress-hang` chunk (`docs/plans/v1/PROGRESS.md`). In two
`stress-ng-fs-nodes` runs on the io_uring transport (overload-cascade-2's
`gate1` and `diag1`, 2026-10-05), processes on the lease holder waited in
`request_wait_answer` through the deadline while the daemon had nothing older
than a second in flight: the kernel held requests the daemon never answered.
That signature was not reproduced afterwards. In 16 ring runs (some with every
FUSE request traced), every hang held its requests in the daemon (the
delegates' livelock, not the ring; `/dev/fuse` hangs the same way). The ring
never stranded a reply, and every request the kernel queued for a ring reached
an entry. So the patch closes the path every candidate mechanism shares, makes
it visible, and leaves the rest to the counters:

- **A ring thread's wait is bounded** (`IDLE_WAIT`, 1 s; `RingIo::submit_and_wait`
  takes the timeout). A reply made on another thread is queued in
  `Live::pending` and announced through the wake eventfd, whose multishot poll
  completes the ring thread's wait; if that announcement is lost -- in this
  crate or in the kernel's deferred task-work wake-up (`DEFER_TASKRUN`) -- the
  thread slept until some other completion on its queues woke it, which at the
  end of a workload is never, while the caller waited in the kernel and the
  filesystem considered the request answered. Now the next pass flushes it
  within a second. Entering the kernel also runs whatever task work it queued
  for the thread, so a fetch whose wake-up went missing is served by the same
  pass. Cost: one wake-up a second per idle ring.
- **A stranded reply is counted and logged** (`Ring::note_stranded`). After a
  wait that ended with no completion at all, every reply queued more than
  100 ms earlier (`RingEntry::handed_ms`; a younger one may still have its
  eventfd write under way) had no wake-up: counted in
  `RingHealth::stranded_commits`, logged at most once a minute per ring.
- **Entries userspace holds too long are reported** (`Ring::report_held`, the
  `fuser-ring-watch` thread `RingSet::watch_held` starts). Every fetch records
  its time, opcode and unique; the watchdog (every quarter of the threshold, at
  least once a second) logs each entry held past `RingHealth`'s threshold once
  per fetch, with its state and a per-queue census (in the kernel / in
  userspace / dead), and keeps `RingHealth::entries_held_long`. Blocking lock
  requests (`RingEntry::lock_wait`), which wait for the holder by design, are
  logged at debug and not counted. This shows what a filesystem's own count of
  requests in flight cannot: a request still queued for an offload thread
  (under overload, tens of seconds deep) or one answered whose commit did not
  reach the kernel.
- `Config::io_uring_health: Option<RingHealth>` (public, re-exported) carries
  the threshold and the counters; `None` keeps the bounded wait and drops the
  rest. A zero threshold runs no watchdog.

Tests: `uring::ring::test::a_reply_whose_wake_up_is_lost_is_flushed_by_the_idle_wait`
(a test hook, `RingHooks::lost_wakes`, drops one eventfd write; the reply still
reaches the kernel, counted once, and an announced one is not counted; it
fails with the unbounded wait) and
`report_held_counts_long_held_entries_but_not_lock_waits`.

Regenerated as `git diff --no-index --no-prefix a b` of pristine 0.18.0 with
0001-0003 applied (`a`) against this tree (`b`); `tools/vendor-fuser.sh
--check` verifies the series.
