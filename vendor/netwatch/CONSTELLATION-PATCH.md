# Vendored netwatch 0.19.3: lost wakeups on the UDP socket

This directory holds netwatch **0.19.3** exactly as published on crates.io
(`.crate` sha256 `39da9cad…6ff95e96`, upstream commit
`0030e57fc70981895bb86212ff0f059cc41c8a4e` of
`https://github.com/n0-computer/net-tools`, path `netwatch`). It is used
through `[patch.crates-io]` in the workspace `Cargo.toml`, and it is the
copy iroh 1.1 runs on. The kept files are `src/`, `tests/`, `build.rs`,
`Cargo.toml` and `README.md`. netwatch is MIT OR Apache-2.0 (see the
`license` field in `Cargo.toml`). The published crate ships no license
texts.

## What is changed

There are two changes, both marked `CONSTELLATION PATCH`.

1. **`src/udp.rs`**: the fix. `UdpSocket`'s `send_waker` and `recv_waker`
   were `AtomicWaker`s. They are now `Wakers`, which keeps every parked
   task's waker and wakes all of them. A send that goes through also wakes
   the senders still parked (`Wakers::wake_parked`, a single atomic load
   when nobody is parked). `poll_read_socket` no longer takes its waker
   back after the second `try_read` succeeds. A `Wakers` cannot remove one
   task's waker, so that task only gets a spurious wake later.
2. **`Cargo.toml`**: `warnings = "allow"` in `[lints.rust]`. This is build
   hygiene, as in `vendor/fjall`: a path dependency is not built with
   `--cap-lints allow`.

The directory is in the workspace `exclude` list, so it is built as a plain
dependency and is not linted or tested as a member. The `atomic-waker`
dependency is now unused. It is left in place so that the manifest stays
upstream's.

## The bug

Many tasks send on one netwatch `UdpSocket`: every noq connection driver,
and every iroh `RemoteStateActor` relaying the Initial packets of a dial
that has no path yet. A sender parks in two cases: a rebind holds the
socket's `RwLock` for writing, or the socket is not writable. While parked,
it registers its waker in the socket's single `AtomicWaker`, so each sender
that parks replaces the waker of the one that parked before it. The rebind's
`wake_all` (or tokio's writability, which also keeps one waker) then wakes
only the last of them. The others stay parked until something else happens
to wake their task. `crates/net/tests/udp_rebind_wakeups.rs` has 16 tasks
sending on one socket while it is rebound every millisecond. Upstream leaves
15 of the 16 parked for good. With the patch, all of them finish in about
0.3 s.

iroh rebinds on every *major* link change. On a host where container
networks come and go (a CI or build box, a Kubernetes node) that happens
about once a second. When the parked sender is a `RemoteStateActor`, the
only thing that wakes it is its own 60 s idle timer. Its 16-slot inbox
fills up in the meantime. iroh's socket actor hands every new dial
(`ResolveRemote`) and every new connection (`AddConnection`) to these
actors one at a time, with an awaited `send`, so it blocks on that full
inbox. The whole endpoint then cannot dial anyone, and cannot hand an
accepted connection to its protocol handler, for the rest of the minute.
Peers still complete QUIC handshakes with it, so their pooled connections
look alive, but their requests are never answered.

Constellation hit this as `p2p-restart-auth`. After a whole-cluster
`kill -9` and restart (`git-under-flock-faults` seed 1001), forwards to the
root stalled for 50–60 s. Restarts are when the most dials are in flight,
many of them to dead addresses that retransmit Initials through the actors.
The "authentication failed" lines are an after-effect of the stall, not an
identity problem. They are noq refusing an Initial it cannot decrypt with
the client keys: stale handshake packets of dials that were abandoned while
the endpoint was stuck. The `p2p-cluster-restart` harness scenario
reproduces the stall in 4 of 4 runs without this patch and in 0 of 16 with
it.

## Upstream status

Checked on 2026-10-03: crates.io has no netwatch newer than 0.19.3, and
`main` of `n0-computer/net-tools` still has `send_waker` and `recv_waker`
as single `AtomicWaker`s in `netwatch/src/udp.rs` (version 0.19.3), so no
upstream release fixes this yet. The licence texts here (`LICENSE-MIT`,
`LICENSE-APACHE`) are copied from the repository root of that repository at
the commit above, as the crate itself ships none.

One hole is left, as upstream: `Wakers::wake_parked` wakes parked tasks
when a send goes through, but tokio hands a writability wakeup to one task
only. If that task is cancelled before it sends, the other parked senders
wait for the next successful send or rebind (iroh's socket sends and
rebinds keep happening, so in practice this is short).

## Dropping the patch

Once an upstream netwatch release has fixed the lost wakeup, do this:

1. Remove the `netwatch` line from `[patch.crates-io]` and
   `vendor/netwatch` from `exclude`.
2. Delete this directory.
3. Keep `crates/net/tests/udp_rebind_wakeups.rs`: it fails against an
   affected netwatch.

Upstream iroh also has a fragility of its own that this patch does not
change: the socket actor awaits a send to each `RemoteStateActor` inbox,
so any one stuck actor stalls every other peer of the endpoint. That is
worth an upstream issue too.

The upstream issue to file for this patch, with a verified reproducer, is `../ISSUE-netwatch.md`.
