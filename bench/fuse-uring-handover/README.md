# fuse-uring-handover: can a FUSE-over-io_uring session be handed over?

Plan 38 milestone Z0a (`docs/plans/v1/wip/38-fuse-read-path-transport.md` §3(e), §7).
Constellation's session handover (`FuseSession::detach` → `SessionHandoff` →
`Session::from_fd_resumed`, plan 31 §6.11) passes the `/dev/fuse` fd to a freshly exec'd
process B and lets process A exit. With the `/dev/fuse` transport the kernel keeps queuing
requests on the connection across the gap. With FUSE-over-io_uring, the ring entries the
kernel delivers requests to belong to A's io_uring instance, which dies with A. This bench
answers three questions on real kernels:

- **(a)** does the connection survive the loss of its ring entries?
- **(b)** do requests go back to plain `read(2)`/`write(2)` on `/dev/fuse`, so B can serve
  without a ring?
- **(c)** can B register fresh ring entries (`FUSE_IO_URING_CMD_REGISTER`) on the
  already-initialised connection, and do requests reach them?

The findings and the decision they imply are in [RESULTS.md](RESULTS.md).

## The program

`src/hospike.c` is a raw-uapi FUSE server (plan 38's route 2: no libfuse, since stock
libfuse cannot resume a connection that has already seen `FUSE_INIT`). It uses liburing
and a vendored copy of the kernel's `include/uapi/linux/fuse.h` (`src/fuse_kernel.h`,
v7.3-rc5, whose ring structs are layout-compatible with 6.14+). It serves a one-file
read-only filesystem: `/probe`, 256 MiB of a deterministic pattern that every client
verifies, opened with `FOPEN_DIRECT_IO` so every read reaches the daemon.

| command | what it does |
|---|---|
| `hospike serve-a --mnt DIR --variant N --out DIR [--gap S]` | Process A. `mount(2)` with `fd=`, answers `FUSE_INIT` with `FUSE_OVER_IO_URING`, registers `nr_cpus × 2` ring entries (payload 1 MiB each) from one thread and one io_uring, serves, and keeps a `/dev/fuse` reader thread (which only ever sees `FORGET`/`INTERRUPT` once the ring is ready). `SIGUSR1` starts the handover. |
| `hospike serve-b ...` | Process B, exec'd by A with the `/dev/fuse` fd inherited (for the kernel the same as `SCM_RIGHTS`: B holds a reference to the same open file). Always runs a `/dev/fuse` reader; registers its own ring entries depending on the variant. |
| `hospike client --file F --log L` | Continuous reader: 128 KiB `pread`s, verified, reopening the file every 64 reads (so `OPEN`/`FLUSH` and the background `RELEASE` go through the ring too). Writes a status line every 100 ms (`ok`, `err`, `bad`, `inflight_ms` = how long the current syscall has been waiting). |
| `hospike probe --file F` | One open + 4 KiB pread + verify; prints `ok`/error and its latency. |

Counters (requests served per transport and opcode, REGISTER outcomes, CQE errors,
timestamps) are written to `<out>/{a,b}.stats` every 250 ms.

### Variants (plan 38 §7, Z0a)

| # | A | B | measures |
|---|---|---|---|
| 1 | spawns B, waits for it to start, `io_uring_queue_exit`, exits | only reads `/dev/fuse` | (a), (b) |
| 2 | as 1 | as 1, then `--gap` s (3) after A is gone registers `nr_cpus × 2` fresh entries | (c) |
| 3 | spawns B, keeps serving until B reports its entries registered, then tears down and exits | registers first (overlap), then serves | two registrants |
| 4 | spawns B, waits for it to start, `raise(SIGKILL)` with the ring live | as 2 | crash shape |

A accepted REGISTER has no completion until a request is delivered to that entry (the
kernel returns `-EIOCBQUEUED`); a rejected one completes at once with a negative `res`. So
"accepted" is "no CQE within 300 ms", and the first negative result is recorded.

### One run (`run.sh`)

1. A mounts `/mnt/hospike` and gets its ring ready.
2. Two clients start, pinned to CPU 1 and CPU 5 (FUSE-over-io_uring picks the queue by
   the CPU the requesting task runs on).
3. After 3 s, `SIGUSR1` to A.
4. 1 s after A is gone, a **gap probe** is started on CPU 3: a request issued while nobody
   serves the ring. In variants 2 and 4, B registers 3 s after A is gone.
5. 7 s after A is gone, the **kick**: one probe pinned to every CPU, 3 s budget each.
6. Final state of every client and probe (running, `S` in `request_wait_answer`, or `D`),
   `/sys/fs/fuse/connections/<id>/waiting` at each step, and new `dmesg` lines.
7. Teardown: `SIGTERM` to clients and probes. Whatever survives that is stuck in the
   kernel. A request still pending in a queue is killable, but one that was already
   handed to a ring entry is not. The connection is then aborted through fusectl, and
   the run records that it had to be.

`analyze.py` turns each run directory into one JSON line in
`results/<uname -r>/summary.jsonl`; `analyze.py --table results/*/summary.jsonl` prints
them one line per run.

## Running it

Root, a kernel with `CONFIG_FUSE_IO_URING=y` (6.14+), gcc and liburing headers
(`dnf install gcc make liburing-devel` / `apt install build-essential liburing-dev`):

```sh
./build.sh
sudo ./run.sh                       # 4 variants x 3 repeats, ~6 min
sudo ./run.sh --variants "2 3" --repeats 5 --gap 3
python3 analyze.py --table results/*/summary.jsonl
```

`run.sh` sets `fuse.enable_uring=Y` and mounts fusectl if needed. It leaves nothing
mounted or running behind. Where a run left a process stuck, the connection is aborted,
and that frees it.
