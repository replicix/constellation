# K0 Track A results: fd passing and FUSE session handover across three processes

Plan 37 §15, questions 1-5. Run 2026-10-01 on one host: Ubuntu 24.04, kernel
`7.0.0-31-generic`, 32 CPUs. Nobody has root on it, so the probe ran as root in a
`--privileged --device /dev/fuse` `ubuntu:24.04` container (same kernel) — see
[run.sh](run.sh) — and question 5 ran in a three-node `kind` v0.33.0 cluster created
from [`tests/csi/kind-config.yaml`](../../tests/csi/kind-config.yaml).

Raw data: `results/k0-track-a.json` (headline, `run.sh`),
`results/k0-track-a-deep-queue.json` (`run.sh --deep`, a deeper client queue),
`results/k0-question5-kind.txt` (the cluster), `results/k0-stage-volume.txt` (the
`NodeStageVolume` shape against the real daemon). Every number below is in those
files; the per-leg percentiles the questions are gated on are computed by the probe
itself (`legs_ms`) rather than by hand afterwards.

## Answers

| question | answer |
|---|---|
| **(1)** Does stock `Session::from_fd` fail on an already-initialised connection, as §8 step 5 predicts? | **Yes, exactly as predicted — and it has a second, worse failure mode.** With a real request waiting, `from_fd` replies **`EIO`** to that request and returns `InvalidData`, "Received non-init FUSE operation during handshake". With *nothing* waiting it does not return at all and burns **99.6 % of a core** doing it; clearing `O_NONBLOCK` on the handover descriptor first turns that into an ordinary block (0.0025 % of a core), which is what identifies the cause. |
| **(2)** Does the vendored resume path serve ordinary requests with no re-`INIT`? | **Yes.** 44 handoffs, every `FuseSession::resume` Ok, the resumed session reporting the same `NegotiatedInit` the detaching one handed over on every handoff, and getattr/read/write/readdir through the resumed session all correct — 79 GB read back through the connection (`O_DIRECT`, so every read was answered by a server) with **0 byte mismatches**, and **0 client errors** in 6.2 M operations. "No re-`INIT`" itself is by construction: `from_fd_resumed` never calls `handshake()`. |
| **(3)** What does the fd-passing round trip cost? | **p50 0.82 ms, p90 1.16 ms, max 21.5 ms** over 40 handoffs; the `SCM_RIGHTS` send + `from_fd_resumed` + first request leg alone is p50 0.19 ms / max 0.32 ms. At 80 concurrent clients: p50 1.29 ms, p90 2.01 ms, max 12.3 ms. §8's target is **2 s**; the medians are three orders of magnitude under it and the worst handoff of 50 is two. |
| **(4)** Do in-flight requests survive the pause, or does the kernel's queue produce client-visible errors? | **They survive; blocking only, never an error.** With nobody reading `/dev/fuse` for 0.5 / 2 / 5 / 10 s, both the writer's longest `pwrite` and the reader's longest `pread` are the pause + 1.3-5.4 ms and the error count is **0**, at every pause length, including with 80 requests outstanding — above `congestion_threshold` (48) and above `max_background` (64). |
| **(5)** Does `kind` + `/dev/fuse` + a `privileged: true` pod work? | **Yes, and the pod half of §12 is confirmed in a stronger form; the node half is refuted.** A privileged pod on an `extraMounts` worker ran the whole chain with 0 errors. An unprivileged pod cannot, with or without the device bind-mounted in, with or without `SYS_ADMIN`. And kind's node containers get `/dev/fuse` *without* `extraMounts`, because kind runs them `--privileged`. |

## Setup

8 fuse worker threads, so the session negotiates `max_background = 64` and
`congestion_threshold = 48` (`frontend-fuse`'s `threads::max_background`). The kernel
agreed FUSE 7.40 (it offered 7.45), `max_write` 16 MiB, `max_pages` 4096,
`max_readahead` 128 KiB, `time_gran` 1 ns — identical before and after every handoff.

| run | clients | load |
|---|---|---|
| headline | 8 writers (64 KiB `pwrite` of the pattern), 4 readers (64 KiB **`O_DIRECT`** `pread` + byte verify of the writers' files), 2 fio threads (`randrw`, 64k, 60 s) | 5.0 M writer ops / 329 GB, 1.2 M reader ops / 79 GB |
| deep queue | 64 writers, 16 readers, no fio | 343 k writer ops / 22 GB, 111 k reader ops / 7.3 GB |

Every client descriptor is opened **once, before the first handoff**, and used across
all of them: the handles have to survive every handover, as an application's do.

The readers use `O_DIRECT` deliberately. Without it the kernel answers them from the
page cache, no request reaches `/dev/fuse`, and their numbers describe nothing that
happened on the connection — the first revision of this probe had exactly that bug
(its readers were 0.2 ms through a 10 s pause with nobody serving, because no read
ever left the page cache). The writers' files are what the readers read, so a
reader's bytes are a writer's bytes; the pattern is a pure function of the absolute
offset, so a read racing a write over the same range still has to match byte for
byte.

The probe **asserts** §15's "zero I/O errors" rather than only recording it: a client
error, a byte mismatch, a failed post-resume check, a resumed session reporting a
different `NegotiatedInit`, or a non-zero fio status is a non-zero exit. Both runs
below exited 0.

## (1) `Session::from_fd` on a live connection

Three passes, each: mount, serve, `SessionControl::detach` (which stops reading
without unmounting), then hand the descriptor to a second process that calls the
*stock* constructor. That process does nothing else, so its own CPU time over the
window is the constructor's.

| pass | what the second process saw | CPU over the window | what the client racing it saw |
|---|---|---|---|
| a real `statfs` waiting (`waiting` = 1), descriptor as handed over | `Err(InvalidData)` in 0.7 ms, "Received non-init FUSE operation during handshake" | 0.6 ms | **`EIO`** (errno 5), 1.3 ms after the descriptor reached that process |
| nothing waiting (`waiting` = 0), descriptor as handed over | **did not return within 5 s** | **4983 ms of 5000 ms = 99.6 % of a core** | — |
| nothing waiting, `O_NONBLOCK` cleared on the descriptor first | **did not return within 5 s** | 0.12 ms of 5000 ms = 0.0025 % of a core | — |

The first pass is §8 step 5's prediction, reproduced exactly, including the detail
that matters most: the handshake does not merely fail, it *answers a real client
request with `EIO`* before failing. The kernel does not resend `FUSE_INIT` on an
existing connection — that is precisely why the handshake read an ordinary operation.
(The client's whole syscall took 302 ms, but 300 ms of that is the probe's own
deliberate wait to get the request queued; 1.3 ms is what the failed handshake cost
it.)

The second and third passes are new, and together they identify the mechanism rather
than infer it. `SessionControl::detach` hands back a descriptor that is still
`O_NONBLOCK` (an armed detachable session sets it on the shared open file
description, and the flag travels with the open file description through
`SCM_RIGHTS`), while `fuser`'s `Channel::receive_retrying` treats `EAGAIN` as "retry
immediately". Clearing `O_NONBLOCK` and changing nothing else turns 99.6 % of a core
into 0.0025 %: the same non-return, now an ordinary blocking `read`. So on a quiesced
connection the stock constructor livelocks. `FuseSession::resume` is unaffected —
`from_fd_resumed` skips the handshake and an armed session `poll`s before reading —
but a *misuse* of the handover descriptor is a spinning core, not an error, which is
worth a guard when K5 builds the production path.

## (2) The resumed session

44 handoffs in one chain (40 back to back, then one per pause length), through 45
distinct server processes, with the writers and readers never stopping:

- every `FuseSession::resume` returned `Ok`;
- the **resumed session's own** `negotiated_init()` — read back out of the session,
  not echoed from the request that carried it — equalled the detaching session's on
  all 44 rows (`init_matches`). That says the negotiated state survived the hop; it
  is not by itself evidence about re-`INIT`;
- **no re-`INIT` is by construction**: `Session::from_fd_resumed`
  (`vendor/fuser/src/session.rs:406`) stores the carried `NegotiatedInit` and never
  calls `handshake()`, so there is no code path on which a `FUSE_INIT` could be
  written. What the measurement adds is that the connection then *works*: after the
  last resume, `getattr` reports the right size, a 4 KiB `O_DIRECT` `pread` at 4 MiB
  into a writer's file differs in **0 bytes** from the pattern, a write through a
  **freshly opened** handle reads back correctly, and `readdir` lists all 11 entries;
- across the whole run, writers and readers recorded **0 errors and 0 mismatches**
  over 6.2 M operations, with every reader operation answered by a server process
  over the handed-over connection (`O_DIRECT`), and fio (which creates and uses its
  own handles) exited 0 with `error: 0`.

## (3) Round trip

`detach_ms` is the driver's request-to-descriptor-in-hand time (it includes §8 step
2's drain: the session stops its workers, drains deferred reads and runs
`Vfs::sync_view`). `resume_ms` is `SCM_RIGHTS` send → the new process is serving.
`first_op` is the driver's first client call after the resume. The next server process
is already running before the clock starts, as a new engine pod would be.

Over the back-to-back handoffs only (a paused row's legs are not comparable). At
n = 40 and n = 10, **p99 is the maximum by construction**, so it is reported as such:

| run | leg | p50 | p90 | max |
|---|---|---|---|---|
| headline (40) | detach | 0.62 ms | 0.96 ms | 21.2 ms |
| headline (40) | resume | 0.19 ms | 0.24 ms | 0.32 ms |
| headline (40) | **round trip** | **0.82 ms** | **1.16 ms** | **21.5 ms** |
| headline (40) | first client op | 0.006 ms | 0.37 ms | 109.3 ms |
| deep queue (10) | detach | 0.80 ms | 1.62 ms | 11.8 ms |
| deep queue (10) | resume | 0.41 ms | 0.53 ms | 0.62 ms |
| deep queue (10) | round trip | 1.29 ms | 2.01 ms | 12.3 ms |
| deep queue (10) | first client op | 0.009 ms | 0.70 ms | 0.76 ms |

One of the 40 headline handoffs is the entire tail: detach 21.2 ms, first client op
109 ms, and the longest client syscalls anywhere in that same window 212 ms (writer)
and 239 ms (reader). The other 39 are ≤ 1.4 ms round trip with ≤ 12.5 ms longest
client syscall. The host's load average was 20-55 across 32 CPUs during the run, from
unrelated work; the probe cannot separate that from a handoff cost, so the outlier is
reported rather than explained away. It is still 100× under §8's 2 s target.

## (4) The paused reader

One handoff per pause length, with the connection held by the driver (nobody reading
`/dev/fuse`) for that long in between. `waiting` is
`/sys/fs/fuse/connections/<id>/waiting`, sampled before the detach, during the pause
(maximum), and after the resume.

Headline run, 12 client threads + 2 fio threads:

| pause | `waiting` before / during / after | writer's longest `pwrite` | reader's longest `pread` | writer errors | reader errors |
|---|---|---|---|---|---|
| 0.5 s | 7 / 13 / 9 | 501.5 ms | 501.5 ms | 0 | 0 |
| 2 s | 7 / 13 / 9 | 2001.4 ms | 2001.4 ms | 0 | 0 |
| 5 s | 6 / 13 / 8 | 5001.4 ms | 5001.3 ms | 0 | 0 |
| 10 s | 8 / 13 / 9 | 10002.4 ms | 10002.4 ms | 0 | 0 |

Deep-queue run, 80 client threads (`max_background` 64, `congestion_threshold` 48):

| pause | `waiting` before / during / after | writer's longest `pwrite` | reader's longest `pread` | writer errors | reader errors |
|---|---|---|---|---|---|
| 0.5 s | 70 / 80 / 74 | 504.8 ms | 501.8 ms | 0 | 0 |
| 2 s | 71 / 80 / 71 | 2002.7 ms | 2001.7 ms | 0 | 0 |
| 5 s | 72 / 80 / 74 | 5002.4 ms | 5001.9 ms | 0 | 0 |
| 10 s | 69 / 80 / 74 | 10005.4 ms | 10002.2 ms | 0 | 0 |

Three things to read out of this:

1. **The stall is the pause, to within milliseconds, and nothing else happens.** No
   errno reaches a caller at any depth or any pause length, up to 10 s — twice §8's
   default `--handoff-drain-timeout` and a third of its `--handoff-total-timeout`.
2. **The queue goes past every published bound and still does not error.** `waiting`
   reached 80 — every client outstanding at once — which is above
   `congestion_threshold` (48) and above `max_background` (64): those bound
   *background* requests (readahead, writeback), not the synchronous ones these
   clients issue. Congestion throttles, it does not fail.
3. **Both directions block for the whole pause.** Writers and readers alike wait out
   the pause and are answered afterwards. This is where the earlier revision of this
   probe was blind: its readers were served from the page cache and reported
   sub-millisecond `pread`s straight through a 10 s outage, which measured the cache
   and not the connection.

## (5) kind, `/dev/fuse` and the privileged pod

`kind` v0.33.0, three nodes (`kindest/node:v1.37.0-zfs`, the host's ZFS docker
data root needs it; the committed config pins no image), created from
`tests/csi/kind-config.yaml`: control-plane with no `extraMounts`, two workers with
`/dev/fuse` bind-mounted in. The pod image is `ubuntu:24.04` + `fuse3` + the probe.

| pod | node | securityContext | result |
|---|---|---|---|
| privileged | worker (`extraMounts`) | `privileged: true` | **the whole chain ran**: 5 handoffs + a 1 s pause, 176 k writer ops, 92 k reader ops, 0 errors, 0 mismatches, all post-resume checks OK, round trip p50 0.53 ms |
| privileged, no `extraMounts` | control-plane | `privileged: true` | **also worked** (3 handoffs, 0 errors) — see below |
| plain | worker2 | none | `/dev/fuse` does not exist in the container → `ENOENT` |
| device bind-mounted in | worker2 | none, `hostPath: /dev/fuse` | device visible (`crw-rw-rw- 10, 229`), `open("/dev/fuse")` → **`EPERM`** |
| device bind-mounted in | worker2 | `capabilities: add: [SYS_ADMIN]`, `hostPath: /dev/fuse` | device visible, `open("/dev/fuse")` → **`EPERM`** |

The last row is the one §12 cites as REPORTED and this run makes VERIFIED, in a
slightly stronger form than §12 states it: bind-mounting the device in and granting
`SYS_ADMIN` is *still* not enough — the container's device cgroup denies the open —
`privileged: true` is what works. (The `SYS_ADMIN` pod did manage to mount `fusectl`,
which the plain one could not, so the capability was genuinely in force.)

The correction: **on this host the `extraMounts` were not what made `/dev/fuse`
available.** kind runs its node containers `--privileged`, and runc populates a
privileged container's `/dev` with every host device, so even the control-plane node
— which this config deliberately leaves without `extraMounts`, as a control group —
has a working `/dev/fuse` (a fresh `tmpfs` node, where the workers' is the host's
devtmpfs bind-mounted in by kind). `kubernetes-sigs/kind#2540`'s "rootful operation
does not need fuse" is about kind's own explicit mount, not about what a privileged
node container ends up with. The `extraMounts` stay in the committed config: they are
harmless, they are what makes the device present deterministically rather than as a
side effect of runc's privileged-container behaviour, and they are the documented
pattern for rootless and restricted runtimes.

`results/k0-question5-kind.txt` was produced before the probe's reader fix (its JSON
has the pre-fix shape, and its readers were page-cache served), and the cluster is
deleted, so it was not re-run: what question 5 asks — is the device there, and can a
pod use it — does not depend on how the probe's readers reach the filesystem. Its
in-pod handoff legs (round trip p50 0.53 ms) agree with the host runs above.

Not testable from here: a GitHub-hosted `ubuntu-latest` runner. Per §15's pre-agreed
fallback this is recorded as **"self-hosted runner for `kind-e2e`/`upgrade-under-load`
until verified on a hosted runner"**, with `csi-unit`/`csi-sanity` staying on hosted
runners.

## The `NodeStageVolume` shape, against the real daemon

`crates/cli/examples/stage_volume_probe.rs` ([stage-volume.sh](stage-volume.sh),
`results/k0-stage-volume.txt`) does the same first hop with the product's control
protocol, authz and engine in the path instead of a probe socket and a toy `Vfs`: it
calls `fuse_mount_fd` itself and hands the descriptor to a running `constellation`
daemon with `view.mount{source: PreopenedFd}` over its unix control socket.

```text
mounted /tmp/k0a-stage/mnt-b with mount(2); handing the fd over
view 2 attached: subtree /, known as fd:35
wrote and read back 48 bytes, 1 entries in the root, size 48
view.unmount: timeout: view.unmount did not finish within 10s
unmounted /tmp/k0a-stage/mnt-b by path
views left: ["/tmp/k0a-stage/mnt-a"]
```

**Staging works.** The daemon serves a mount it never mounted; ordinary file I/O
through it is correct; `view.list` shows the view with its `pv=k0a-probe` label, named
`fd:35` in place of a mountpoint.

**Unstaging does not — a gap K0 found and K3a/K5 must close.** `view.unmount` for a
preopened-fd view never answers. `NodeRuntime::remove_mount` asks the session to
unmount, `SessionControl::unmount` refuses ("the mountpoint of a preopened FUSE session
is unknown" — nothing ever called `FuseSession::set_mountpoint` on this path), the
refusal is only logged, and `remove_mount` then blocks in `thread.join()` on a session
nothing has ended. The caller times out; the view is removed only once *somebody else*
unmounts the kernel mount by path. The workaround is the right division of labour
anyway — the node plugin made the mount, so `NodeUnstageVolume` unmounts it — but the
daemon must either learn the mountpoint at `view.mount` time or end the session without
unmounting, and `view.unmount` must answer either way rather than hang.

## What this probe does not cover

- **The control protocol for the handover.** `node.handoff{target: Socket}` is not
  implemented: the daemon answers `Unsupported`, "handing sessions to another process
  over a socket is plan 37's" (`crates/cli/src/control.rs`). The probe therefore
  drives `SessionControl::detach` / `FuseSession::resume` directly and passes the
  descriptor over its own `SCM_RIGHTS` socket. Building `Socket` — and §8's
  `Prepare`/`Commit`/`Abort` phases, which `HandoffParams` has no field for today — is
  K5's work.
- **The engine's handle table.** `ProbeVfs` makes handles process-independent on
  purpose (see [README.md](README.md)); `View::export_handles` /
  `Engine::open_view_resumed` crossing a handover with real open files is what the
  `upgrade-under-load` harness scenario exercises, against a real engine — in-process
  and exec-based, not across a socket, which stays K5's first real test of it.
- **Cluster-lock traffic.** The probe serves with `caps(false)`, so §8 step 1's
  "refuse a detach while a blocking lock wait is in flight" path is never entered
  here; `session.rs`'s own unit tests cover it.
