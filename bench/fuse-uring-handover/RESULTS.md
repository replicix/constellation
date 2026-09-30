# Z0a results: FUSE-over-io_uring vs. session handover

Plan 38 §3(e) / §7 Z0a. Run 2026-09-30 on three kernels. The program, variants and run
timeline are described in [README.md](README.md). The numbers are in
[results/summary-tables.md](results/summary-tables.md) (generated). Every run is one line
in `results/<kernel>/summary.jsonl`, and the raw per-run directories
(`results/<kernel>/v<variant>-r<repeat>/`) are not committed; `run.sh` regenerates them.

## Answer

| question | answer (all three kernels, all 60 runs) |
|---|---|
| **(a)** Does the connection survive A's ring going away? | **Yes.** It is never aborted, and nothing returns `ENOTCONN`. |
| **(b)** Do requests return to plain `/dev/fuse` so B can serve without a ring? | **No.** Once the ring has become ready, the connection never delivers a request over `/dev/fuse` again: 0 requests in 60 runs. Without ring entries every request waits in the kernel. |
| **(c)** Can B register fresh entries and receive requests on them? | **Yes, mechanically.** 720/720 REGISTERs were accepted (16 per run, variants 2–4), and B then served everything new, verified. |
| **…but is the handover lossless, like the `/dev/fuse` one?** | **No, for two reasons.** (1) Every request that sat in one of A's ring entries when A's io_uring went away is lost. Its caller blocks for good in `request_wait_answer`, unkillable (D state after `SIGTERM`), until the connection is aborted through fusectl: 102 of 120 client reads in flight at the handover. (2) A request queued while no entry is available is **not** dispatched by B's REGISTER. It waits until a later request on the same per-CPU queue is committed, which can mean forever on a quiet mount. |

**Decision for `TransportPolicy` (§3(e)):** none of the three outcomes the plan anticipated
fits exactly, and the closest safe policy is the third. **Handover-capable sessions (plan
37's CSI engine pods, the `constellation daemon --upgrade` target mount) are `DevFuse`,
permanently on every kernel tested (6.17, 7.0, 7.3-rc4), not "until Z0a proves
re-registration works".** A mount that is already serving over the ring cannot be handed
over losslessly, and it cannot be downgraded to `/dev/fuse` either (answer (b)). So
`daemon --upgrade` / `node.handoff` must refuse to detach a session whose ring is active,
with a clear error, rather than try. Plain mounts, which are never handed over, keep `Auto`.
The transport is fixed when the mount starts: a session cannot opt out of the ring later.
Revisit only if upstream adds (i) a way for a server to stop taking requests on its entries
while keeping the connection (a commit-without-fetch or an unregister-entry command), and
(ii) dispatch of already-queued requests when an entry is registered. Neither exists in
7.3-rc5.

## Setup

| label | kernel | OS | `dev_uring.c` read |
|---|---|---|---|
| u2510 | 6.17.0-1019-aws (Ubuntu 6.17.0-1019.19-aws, base 6.17.13) | Ubuntu 25.10 | stable `v6.17.13` (byte-identical to `v6.17`) |
| u2604 | 7.0.0-1012-aws (Ubuntu 7.0.0-1012.12-aws, base 7.0.14) | Ubuntu 26.04.1 | stable `v7.0.14` (byte-identical to `v7.0`) |
| rawhide | 7.3.0-0.rc4.260925g165768bb7026.42.fc46 | Fedora Rawhide 20260929 | `torvalds/linux@165768bb7026` (byte-identical to `v7.3-rc5`) |

All three: EC2 c7a.2xlarge, 8 vCPUs, so 8 possible CPUs and **8 ring queues**, with
`CONFIG_FUSE_IO_URING=y` and `fuse.enable_uring=Y`, everything run as root. The Ubuntu
kernels carry distro patches that were not read; the upstream `dev_uring.c` of their
stable base was, and every observation matched what it predicts. A 6.14 kernel was not
run. Upstream `v6.14`'s `dev_uring.c` differs from 6.17's only in list ordering
(`list_move` → `list_move_tail` for the in-userspace/request queues), in the new
request-timeout helpers, and in where `FR_URING` is set, none of it on the paths below.
So 6.14 is expected to behave like 6.17, but that is untested.

Route: plan 38 offers two routes. Route 2 was taken, a raw-uapi server in C with liburing,
because B has to be controlled exactly (when it registers, and recording every
command's result). Route 1 (patched libfuse) was not attempted.

## Kernel source, read before running

The FUSE ring belongs to the **connection** (`fc->ring` / 7.3 `fch->ring`), not to the
io_uring instance: the io_uring instance only owns the `IORING_OP_URING_CMD`s that park
entries. Functions and line numbers per version (6.17.13 / 7.0.14 / 7.3-rc4):

| what | where (6.17 / 7.0 / 7.3) | consequence |
|---|---|---|
| The connection is aborted only when the **last** `/dev/fuse` file reference is released | `fuse_dev_release` dev.c:2490 / 2545 / 2244 (`atomic_dec_and_test(&fc->dev_count)`, 7.3 `list_empty(&fch->devices)`) | B holds the same open file (inherited), so A's exit aborts nothing. Predicts **(a) yes** |
| io_uring teardown or process death cancels A's parked commands through `IO_URING_F_CANCEL` → `fuse_uring_cancel`, which touches **only `FRRS_AVAILABLE` entries** | dev_uring.c:498 / 500 / 581 | 6.17/7.0 move them to `ent_in_userspace` with no command (leaked until unmount), 7.3 `kfree`s them. Queues are not stopped, and `ring->ready` stays true |
| The first time every queue has an available entry, `fiq->ops` switches to `fuse_io_uring_ops`, and **nothing switches it back** | `fuse_uring_do_register` :976 / 982 / 1357, `WRITE_ONCE(fiq->ops, …)` :996 / 1002 / 1388 | From then on `send_req` goes to the ring. `/dev/fuse` only gets `FORGET` and `INTERRUPT` (`fuse_io_uring_ops`). Predicts **(b) no** |
| A new request goes to the queue of the CPU the caller runs on. With no available entry it is appended to `queue->fuse_req_queue` | `fuse_uring_queue_fuse_req` :1258 / 1266 / 1797 (`task_cpu(current)` :1236 / 1244 / 1775, `list_add_tail` :1286 / 1293 / 1822) | With A gone and before B registers, requests wait there. They are still `FR_PENDING`, so they are killable |
| REGISTER checks only `initialized`, `connected`, `!aborted`, `enable_uring \|\| fc->io_uring`. It reuses the existing ring and queue and adds an entry | `fuse_uring_cmd` :1125 / 1132 / 1645, `fuse_uring_register` :1078 / 1084 / 1500 | Predicts **(c) yes**, for any process that holds the fd. Two registrants coexist, because entries are just list members |
| …but registering does **not** look at `queue->fuse_req_queue` | `fuse_uring_do_register` (above) | Queued requests are pulled only when an entry is re-armed on commit: `fuse_uring_next_fuse_req` :838 / 843, 7.3 `fuse_uring_get_next_fuse_req` :1181. Predicts the **gap stall** |
| A server has no way to stop taking requests without losing its entries: `COMMIT_AND_FETCH` always re-arms, and there is no commit-only or unregister command. Re-armed entries go to the **head** of the available list | `fuse_uring_ent_avail` :743 / 748 / 1093 (`list_move`) | While A is alive its entries are picked first (LIFO), even with B registered. Any request dispatched to A after its last reap is owned by an entry that dies with A. Predicts the **in-flight loss**, including in the overlap variant |
| A request owned by an entry in `FRRS_USERSPACE` survives the io_uring exit (cancel only takes `AVAILABLE`). B cannot complete it: commit reads the reply through `ent->headers`, a **user pointer into A's address space** | `fuse_uring_commit` → 7.3 `copy_header_from_ring` :695 (6.17/7.0 `copy_from_user(&req->out.h, &ent->headers->in_out, …)`) | The request stays `FR_SENT` forever. `request_wait_answer`'s last wait is uninterruptible, so the caller is unkillable until abort |
| A dispatch whose task work is cancelled because the io_uring is dying: 6.17/7.0 park the entry with the request still attached (lost). 7.3 ends the request with `-ECANCELED` | `fuse_uring_send_in_task` `-ECANCELED` :1225 / 1233 / 1754 | Predicted: on 7.3, some lost requests might surface as `ECANCELED` errors rather than hangs. **Not observed** (see below) |
| 7.3 only: the ring is created at `FUSE_INIT` (`fuse_uring_conn_init`, dev.c:80). `ADD_QUEUE` refuses an existing queue (`-EEXIST`), and `ADD_BUFPOOL` refuses a queue whose payload mode is already set. A bufpool records the **registrant's user address** | dev_uring.c:302, :1537, :1568 | **Source reading only, not run:** zero-copy/bufpool queues (Z4) can't be re-created by B at all, and B's plain REGISTER on them fails the `bufpool_enabled` check (`-EINVAL`). Worse for handover than the per-entry payload mode tested here |

## Observations

`results/summary-tables.md`, abridged. Each cell has 5 runs × 2 clients:

| variant | (a) alive | (b) B `/dev/fuse` requests | (c) REGISTER accepted / B ring requests per run | client reads lost at handover (6.17 / 7.0 / 7.3) | gap probe | kick probes |
|---|---|---|---|---|---|---|
| 1: A tears down, B `/dev/fuse` only | 15/15 | **0** | — | 10/10 · 10/10 · 10/10 | hung | 0/120 ok, all hung (killable) |
| 2: then B registers 3 s later | 15/15 | **0** | 240/240 · 38–40 | 10/10 · 10/10 · 10/10 | done **3.74–3.78 s after REGISTER, 0.0 s after the kick** | 120/120 ok |
| 3: B registers first (overlap) | 15/15 | **0** | 240/240 · 346k–465k | 3/10 · 5/10 · 4/10 | served at once | 120/120 ok |
| 4: A `SIGKILL`ed, ring live; B registers 3 s later | 15/15 | **0** | 240/240 · 38–42 | 10/10 · 10/10 · 10/10 | as 2 | 120/120 ok |

- **The three kernels behave identically.** No `dmesg` line from `fuse` or io_uring in
  any of the 60 runs (no WARN from the teardown paths). No client ever got an error or
  a corrupt byte (`err=0`, `bad=0` everywhere). The loss shows up as a hang, never as an
  error.
- **(b):** after A is gone, B's `/dev/fuse` reader got nothing, not even `FORGET`s, in all
  60 runs. With B serving only `/dev/fuse` (variant 1) every new request hangs: the gap
  probe and all 8 kick probes, each pinned to its own CPU/queue. Those were still
  `FR_PENDING`, and `SIGTERM` killed them.
- **In-flight loss:** each client always has one read outstanding. In variants 1, 2 and 4
  that read sat in one of A's entries when A's io_uring went away, and it was lost in
  **30/30 client-runs per kernel**. The stall starts exactly when A goes away (`stalls`
  = `[[0.0, null]]` relative to A's exit). The client stays in `request_wait_answer`,
  survives `SIGTERM` in state `D`, and is released only by `echo 1 >
  /sys/fs/fuse/connections/<id>/abort`, which was needed in 57 of 60 runs.
  `/sys/fs/fuse/connections/<id>/waiting` shows the orphans: 2 right after A exits (the
  two lost reads), 3 once the gap probe queues. In variant 3 (overlap) each client's
  outstanding read was either on one of A's entries or on one of B's when A went, and 12
  of 30 were lost. The overlap only lowers the odds: A's re-armed entries are always at
  the head of the available list, so while A lives it takes most requests. Variant 4
  (`SIGKILL`, ring live) is indistinguishable from the explicit `io_uring_queue_exit` of
  variants 1–2. Process death and a clean teardown reach the same `fuse_uring_cancel`.
- **7.3's `-ECANCELED` path never triggered.** In our A, the last reap and the
  `io_uring_queue_exit` are microseconds apart. Requests dispatched in that window had
  their completion posted (task work already run) and were not reaped, rather than being
  cancelled in task work. So the prediction that some 7.3 losses would surface as errors
  was the one mismatch. Both shapes exist in the source, and only the hang was seen.
- **Gap stall (c's caveat):** the gap probe (CPU 3, issued 1 s after A was gone) finished
  **3.74–3.78 s after B's REGISTER returned, and 0.0 s after the kick**, in 30/30 runs of
  variants 2 and 4. B's fresh entries sat available while a request waited in the same
  queue's `fuse_req_queue`. It moved only when the kick probe on CPU 3 took B's entry and
  its commit pulled the queued request (`fuse_uring_next_fuse_req`). On a mount where
  nothing else touches that CPU's queue, the request would wait indefinitely. A
  single-threaded caller cannot unstick itself.
- **What B needs to register at all:** entries on **every** queue (`nr_possible_cpus`, not
  `nr_online`), each with a payload of at least `max(8 KiB, max_write, max_pages ×
  PAGE_SIZE)` from the INIT that A negotiated (`NegotiatedInit` already carries both),
  and `IORING_SETUP_SQE128`. A REGISTER that fails for any reason also clears
  `fc->io_uring`. On 7.3, every later REGISTER then gets `-EOPNOTSUPP`, while the ring
  stays "ready" and keeps swallowing requests. So one bad registration leaves the mount
  unservable. (Source reading. Every REGISTER in this run succeeded.)

## Why this is not the `/dev/fuse` handover

Plan 31 §6.11's detach **stops reading `/dev/fuse` and drains in-flight ops** before the fd
changes hands (`FuseSession::detach`, `crates/frontend-fuse/src/session.rs`, tested by
`detach_waits_for_an_op_in_flight`). Everything that arrives in the gap waits in the
kernel's `fiq` pending queue, and B picks it up with its first `read(2)`. The ring has no
counterpart to either step:

1. A cannot stop fetching. Replying *is* re-arming (`COMMIT_AND_FETCH`). An entry that
   is not re-armed can only be given up by destroying the io_uring that owns it, and
   whatever request that entry holds by then is orphaned.
2. B's first REGISTER is not B's first `read(2)`. Requests that queued in the gap are
   not delivered to it.

The crash shape (variant 4) has a `/dev/fuse` analogue, not measured here: requests A had
read but not answered when it was `SIGKILL`ed are orphaned too. What is specific to the
ring is that the **graceful** path, the one `daemon --upgrade` and the CSI pod
replacement use, cannot avoid the loss.

## Reproduce

```sh
cd bench/fuse-uring-handover && ./build.sh && sudo ./run.sh --repeats 5
python3 analyze.py --matrix results/*/summary.jsonl
```
