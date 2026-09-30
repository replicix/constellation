# Vendored fuser 0.18.0: FUSE session handover

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

## The patches

### `negotiated-init` — learn what `FUSE_INIT` agreed (`src/session.rs`, `src/lib.rs`)

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

### `from-fd-resumed` — serve an initialised connection without a handshake (`src/session.rs`)

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

### `detach` — stop serving without unmounting or closing (`src/session.rs`, `src/channel.rs`, `src/mnt/mod.rs`, `src/lib.rs`)

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

### Build hygiene (`Cargo.toml`)

- The `[[example]]` targets and `[dev-dependencies]` are removed (their
  files are not vendored; the library is all that is built).
- A `[lints.rust] warnings = "allow"` table, as `vendor/fjall` has: a path
  dependency is not built with `--cap-lints allow`.

## What uses it, and the proof

`crates/frontend-fuse/src/session.rs` (`SessionControl::detach`,
`FuseSession::resume`; its module doc has the detach protocol and its
tests run a real kernel mount: detach with nothing in flight, a detach that
waits for an op in flight, a resume serving requests the kernel queued
while nobody read) and `crates/cli/src/handover.rs` (`daemon --upgrade`,
whose module doc has the whole sequence). The harness scenarios
`session-handover-idle` and `upgrade-under-load` are the end-to-end gate:
a real mount handed over under a live writer, zero `ENOTCONN`/`EIO`.

## Upgrading fuser

`tools/vendor-fuser.sh <version>` downloads the crate from crates.io
(or takes `--from <dir>` holding an unpacked crate), replaces this
directory's `src/`, `build.rs`, `Cargo.toml`, `README.md` and `LICENSE.md`
with the pristine files, and `git apply`s every `patches/*.patch` in order;
any hunk that does not apply fails the script, and nothing is left
half-applied. Then update the version/checksum above, `cargo update -p
fuser`, and run the frontend's tests and the two handover scenarios. If a
hunk conflicts, re-make the change by hand on the new source and
regenerate the patch with `git diff --no-index <pristine> vendor/fuser`.

## Dropping the patch

If upstream fuser gains an equivalent (a resumable session and a
non-destructive stop), switch `constellation-frontend-fuse` to it, remove
the `[patch.crates-io]` entry and the `exclude` entry, delete this
directory and `tools/vendor-fuser.sh`, and keep the handover tests and
scenarios.
