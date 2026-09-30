# fuse-read-path: zero-copy techniques for a FUSE read() reply

Measures how much the Linux-specific reply paths (splice, vmsplice, FUSE-over-io_uring,
io_uring zero-copy, passthrough) buy over plain copies when a FUSE daemon serves reads
out of a local chunk cache, the way Constellation serves its disk cache.

The daemon under test is `chunkfs` (`src/chunkfs.c`), a small read-only libfuse lowlevel
filesystem. A store directory holds chunk files `<store>/<name>.<idx>` (all exactly
`--chunk-size` bytes except the last, default 4 MiB, like Constellation's cache files) and
optionally a whole-file copy `<store>/<name>.whole`. The mount exposes `/<name>`. Only
lookup, getattr, readdir, open, release and read are implemented; attributes and entries
are cached for a day so metadata traffic stays out of the numbers.

## Modes

| `--mode` | reply path | user-space copies | kernel copies into the FUSE page cache |
|---|---|---|---|
| `copy` | `pread` the requested range of the chunk file(s) into a per-thread buffer, `fuse_reply_buf` (`writev` on /dev/fuse). Portable baseline, the pre-memcache Constellation read path without its whole-chunk re-read. | 1 (pread) | 1 (writev) |
| `memcache` | first touch reads the whole chunk into a process-resident buffer (bounded by `--memcache-bytes`, default 8 GiB; beyond that the read falls back to `copy` and is counted); reply `fuse_reply_iov` on the resident bytes. Constellation's current read path (the chunk memory cache). | 0 when resident | 1 |
| `mmap` | `mmap` each chunk file once (MAP_SHARED), `fuse_reply_iov` pointing into the mapping. Same copy count as memcache without holding anonymous RAM; the obvious portable alternative. | 0 | 1 (+ page faults on first touch) |
| `splice` | `fuse_reply_data` with `FUSE_BUF_IS_FD|FUSE_BUF_FD_SEEK` buffers on the chunk fds and `FUSE_BUF_SPLICE_MOVE`: libfuse vmsplices the header into a pipe, splices file→pipe, then pipe→/dev/fuse with `SPLICE_F_MOVE`. | 0 | 1 (or 0 if the page is stolen, see caveats) |
| `splice-nomove` | same without `SPLICE_F_MOVE`. | 0 | 1 |
| `vmsplice` | hand-rolled: `vmsplice` {`fuse_out_header`, mmapped chunk pages} into a per-thread pipe, `splice` pipe→/dev/fuse. Needs `req->unique` and the channel fd from libfuse's private `fuse_i.h`, hence the build inside the libfuse tree. | 0 | 1 |
| `vmsplice --gift` (runner: `vmsplice-gift`) | data pages vmspliced with `SPLICE_F_GIFT`, final splice with `SPLICE_F_MOVE`. | 0 | 1 (the kernel cannot steal page-cache pages, see caveats) |
| `uring` | FUSE-over-io_uring transport (`-o io_uring`, kernel 6.14+); `pread` straight into the ring entry's payload buffer from `fuse_req_get_payload()`, reply on that buffer. No /dev/fuse read/writev syscalls. | 1 (pread into payload) | 1 (payload → page cache) |
| `uring-bufpool` | as `uring` with `-o io_uring_bufpool` (`FUSE_CAP_IO_URING_BUFPOOL`, kernel 7.3+, libfuse PR #1640): one kernel-managed pool per queue instead of a payload buffer per ring entry. | 1 | 1 |
| `uring-zc` | io_uring zero-copy (kernel 7.3 `FUSE_URING_ZERO_COPY`, `FOPEN_IO_URING_ZERO_COPY`): the kernel registers the request's page-cache/user pages as a fixed buffer and the daemon issues `IORING_OP_READ_FIXED` from the chunk file into them (`fuse_do_zero_copy()`). Only in `build/chunkfs-zc`, built from the unmerged `joannekoong/libfuse` `zero_copy_v7` draft. Root only. | 0 | 0 (the file read DMA/copies straight into the client pages) |
| `passthrough` | `FUSE_CAP_PASSTHROUGH` + `fuse_passthrough_open()` on `<name>.whole` (kernel 6.9+, root). Reads never reach the daemon: the ceiling. Maps to Constellation only when a file is one backing file. | 0 | 0 (reads are served from the backing file's page cache) |

A mode the host cannot run is refused at startup or at FUSE_INIT with the reason
(e.g. `uring: /sys/module/fuse/parameters/enable_uring is not Y`). Silent downgrades are
detected rather than trusted: chunkfs interposes `writev`/`splice`/`vmsplice` (libfuse
calls them through the PLT) and counts, per reply, whether a splice-mode reply actually went
out through `writev`; uring modes count reads that arrived over io_uring; `uring-zc` counts
zero-copy submissions. The runner turns these into `checks` warnings on each result.

## Layout

```
build.sh        clone + build libfuse (and optionally the zero-copy draft), chunkfs, mkdata, fio
run.sh          prepare / cell / matrix / smoke
record.py       one JSONL line per cell from the raw files
summarize.py    markdown tables (median of repeats); --line for one-line summaries
src/chunkfs.c   the filesystem (symlinked into third_party/libfuse/example/, see build.sh)
src/mkdata.c    store generator + verifier (content is a pure function of name and offset)
RUNBOOK.md      copy-paste procedure for the remote box
third_party/ build/ data/ results/   generated, git-ignored
```

## Build

Needs a C compiler, git, meson + ninja (`pip install --user meson ninja` works), and
pkg-config. `build.sh` uses the system liburing (>= 2.x with `IORING_URING_CMD_FIXED`) and
fio (with the libaio engine) if present, otherwise builds liburing, libaio and fio from source
into `third_party/`.

```sh
./build.sh          # libfuse master + chunkfs, mkdata, fio   -> build/
./build.sh --zc     # additionally joannekoong/libfuse zero_copy_v7 -> build/chunkfs-zc (needs libnuma headers)
```

`LIBFUSE_REF=<commit>` pins libfuse; `FROM_SOURCE=1` forces liburing/fio from source.
chunkfs is built as an extra target appended to `third_party/libfuse/example/meson.build`
(between `# chunkfs-begin/end` markers), so it can `#include "fuse_i.h"`.
`build/chunkfs` is a wrapper script (a symlink would break the binary's `$ORIGIN` runpath).

On Fedora: `sudo dnf install -y gcc make meson ninja-build pkgconf-pkg-config git python3
liburing-devel fio fio-engine-libaio numactl-devel util-linux`. On Ubuntu without root, see
"Local runs" below.

## Run

```sh
sudo ./run.sh prepare                 # data/store: seq1_0 4 GiB, seq8_0..7 1 GiB, rand_0 1 GiB,
                                      # small_0..4095 64 KiB, odd_0 (3 chunks + 12563 B), + .whole copies
sudo ./run.sh matrix                  # everything, 3 repeats, warm + cold -> results/<host>-<utc>.jsonl
sudo ./run.sh cell splice seq1 warm   # one cell
sudo ./run.sh --modes copy,uring --workloads rand4k-1j-dio --caches warm --repeats 5 matrix
./summarize.py results/*.jsonl        # tables again
```

`--quick` switches to a ~1 GiB store in `data/store-quick` and 5 s time-based runs (smoke
testing); `./run.sh --quick smoke` mounts every mode, verifies it and runs `seq1` and
`rand4k-1j-dio` once. `./run.sh --help` lists all options (`--chunkfs-opts "--threads 16
--clone-fd"`, `--read-ahead-kb`, `--runtime`, `--keep-going`, ...).

The full data set is 13.3 GiB of chunks plus the same again as `.whole` copies. Each cell
touches at most 8 GiB of it (seq8), so warm cells fit in RAM on a 30 GiB box even with
memcache holding its 8 GiB.

### Workloads (fio, `--ioengine=psync --direct=0` unless noted)

| id | fio |
|---|---|
| `seq1` | 1 job, sequential read, bs=1M, `seq1_0` (4 GiB) |
| `seq1-dio` | same with `--direct=1` |
| `seq8` | 8 jobs, each its own 1 GiB file `seq8_$jobnum`, bs=1M |
| `rand4k-1j`, `rand4k-8j` | random 4 KiB reads on `rand_0` (1 GiB), 1 or 8 jobs, 30 s time-based |
| `rand4k-1j-dio`, `rand4k-8j-dio` | same with `--direct=1` |
| `rand128k-aio` | random 128 KiB reads, libaio, iodepth 32, 1 job, one pass over `rand_0` |
| `smallfiles` | 4096 files of 64 KiB (one short chunk each), read sequentially (`--nrfiles --file_service_type=sequential --openfiles=1`) |

### What one cell does

1. warm: `cat` the workload's backing files (chunk files; `.whole` for passthrough; both
   for uring-zc, which reads chunk-spanning requests from `.whole`) into the page cache; memcache additionally gets `--preload <prefix>` so its
   chunks are resident in the daemon before the mount.
2. mount chunkfs, wait until `stat <mnt>/odd_0` succeeds.
3. correctness: `mkdata verify` reads `odd_0` (multi-chunk, short last chunk) sequentially
   and at 300 random offsets/lengths up to 2 MiB (crossing chunk boundaries), buffered and
   with O_DIRECT, plus `small_1`, and `cmp`s `odd_0` against the concatenation of its chunk
   files. A mismatch aborts the matrix (`--keep-going` records it and continues).
4. cold: `sync; echo 3 > /proc/sys/vm/drop_caches`; warm: `cat` the backing files again.
5. `SIGUSR1` to chunkfs starts its measurement window (rusage and counters snapshot, peak
   RSS reset via `/proc/self/clear_refs`), then `/proc/stat`, `/proc/diskstats`,
   `/proc/<pid>/io` and backing-file residency (`fincore`) are sampled around the fio run.
6. `SIGUSR2` makes chunkfs write its stats JSON, then unmount, append one JSON line.

Raw files per cell (fio JSON, chunkfs log and stats, verify log, samples) are kept in
`results/<name>.raw/<workload>.<cache>.<mode>.r<rep>/`.

## Result fields

Each JSONL line carries the environment (`kernel`, `libfuse_commit`, `fio_version`,
`store_fs`, `nproc`, `mem_gib`, `enable_uring`, `pipe_max_size`, `euid`), the cell
(`mode`, `workload`, `cache`, `rep`, `status`, `error`, `chunkfs_args`, `fio_args`,
`chunk_size`, `runtime`, `read_ahead_kb`) and:

| field | meaning |
|---|---|
| `fio.bw_mib_s`, `fio.iops` | fio aggregate read bandwidth / IOPS |
| `fio.lat_mean_us`, `fio.clat_p50_us`, `fio.clat_p99_us` | fio total latency mean, completion latency percentiles |
| `fio.usr_cpu_pct`, `fio.sys_cpu_pct` | fio's own CPU (passthrough moves the work here) |
| `daemon.utime_s`, `daemon.stime_s`, `daemon.cpu_s` | chunkfs `getrusage(RUSAGE_SELF)` over the fio window (includes the kernel copy done in the daemon's writev/splice) |
| `daemon.cpu_s_per_gib` | daemon CPU seconds per GiB the daemon served (not per GiB fio read: page-cache hits never reach the daemon) |
| `daemon.maxrss_kib` | peak RSS during the window (`VmHWM` after a reset at window start); `rss_anon_kib` / `rss_file_kib` split at the end (memcache is anon, mmap/vmsplice are file-backed) |
| `daemon.reads`, `daemon.gib_served` | READ requests and bytes the daemon answered in the window |
| `daemon.req_size_hist`, `req_size_p50`, `req_size_min/max` | READ request sizes the kernel sent, log2 buckets (bucket `128K` = 64K < size <= 128K) |
| `daemon.fallbacks` | replies that did not take the mode's path (splice → writev, vmsplice pipe too small, memcache over limit, uring request without payload, zc on a non-zc open) |
| `daemon.writev_calls`, `splice_calls`, `splice_move_calls`, `vmsplice_calls`, `vmsplice_gift_calls` | syscalls the daemon issued in the window |
| `daemon.uring_reads`, `zc_reads`, `zc_spanning_whole` | reads that came over io_uring / were submitted zero-copy / spanned two chunks and were read from `.whole` |
| `daemon.pipe_probe` | default pipe size and result of growing one to 2 MiB (negative errno = cannot) |
| `negotiated.*` | from the INIT reply: `io_uring`, `io_uring_bufpool`, `passthrough`, `splice_write`, `splice_move`, `max_write`, `max_readahead`, `max_pages`, `want` (capability names), `init_status` (libfuse's `FUSE_INIT:` io_uring status lines) |
| `sys_cpu_busy_pct` | whole-machine CPU busy % during fio (`/proc/stat`, busy = not idle/iowait); the fair comparison when work moves between daemon, fio and kernel threads |
| `disk_read_mib` | bytes read from block devices during fio (warm runs should be ~0) |
| `daemon_storage_read_mib` | the daemon's own `read_bytes` from `/proc/<pid>/io` |
| `backing_resident_before_pct` / `_after_pct` | how much of the workload's backing files was in the page cache before/after (drops if SPLICE_F_MOVE steals pages) |
| `verify` | the correctness check output |
| `checks` | warnings: fallbacks, reads not over io_uring, passthrough reads reaching the daemon, warm runs touching the disk, read errors |

`summarize.py` prints one table per workload/cache with medians over the repeats, the
throughput ratio against `copy`, and the `checks` as notes.

## Local runs (unprivileged)

Without root: `enable_uring` cannot be flipped (uring modes refuse), passthrough and
uring-zc need CAP_SYS_ADMIN, drop_caches is impossible (cold cells are skipped). `copy`,
`memcache`, `mmap`, `splice`, `splice-nomove`, `vmsplice(-gift)` work through the setuid
`fusermount3`:

```sh
./build.sh && ./run.sh --quick prepare
./run.sh --quick --caches warm --modes copy,memcache,mmap,splice,splice-nomove,vmsplice,vmsplice-gift --repeats 1 matrix
```

## Caveats

- **Pipes of unprivileged daemons.** A user above `/proc/sys/fs/pipe-user-pages-soft`
  (16384 pages summed over all of that user's pipes) gets 2-page pipes that cannot be
  grown. libfuse then silently replies with `writev` for every splice reply (plus an extra
  bounce buffer), and `vmsplice` can only carry replies of one page. `pipe-max-size`
  (1 MiB) also caps unprivileged pipes below one 1 MiB reply + header. chunkfs prints a
  warning and records `pipe_probe`; the counters show the fallback. Root (CAP_SYS_RESOURCE)
  is exempt. This is a real deployment concern for an unprivileged Constellation daemon.
- **libfuse splice specifics.** `FUSE_CAP_SPLICE_WRITE`/`MOVE` are *not* enabled by
  default in libfuse master's `do_init` (only `SPLICE_READ` with a `write_buf` op); without
  setting them `fuse_reply_data()` silently writev()s. Replies under two pages always use
  writev by design, so the 4 KiB workloads compare nothing for `splice`.
- **SPLICE_F_MOVE steals the chunk cache.** When a pipe buffer is a full page that lines
  up with a FUSE page-cache page, fuse steals it: `page_cache_pipe_buf_try_steal` removes
  it from the *chunk file's* page cache and it becomes the FUSE file's page. Observed on
  7.3/btrfs: after a warm `splice` run 0% (smallfiles) to 75% (seq8, rand128k) of the backing
  files were still cached, vs 100% for `splice-nomove`; `backing_resident_after_pct` shows
  it per cell. For Constellation this means a MOVE read evicts its own disk-cache pages.
- **libfuse splice on cold data** splices file→pipe with `SPLICE_F_NONBLOCK`; on uncached
  pages that returns short, and libfuse then preads the rest, reads the spliced part back
  out of the pipe and writev()s the reply (counted as a fallback: about a third of the
  replies in cold `seq8`). A zero-byte `EAGAIN` sets `broken_splice_nonblock`, which turns
  splice off for the rest of the session.
- **SPLICE_F_GIFT** on mmapped file pages is accepted by vmsplice, but fuse can only steal
  a page whose refcount is 1, which a page-cache page never has; gift degrades to a copy.
- **Buffered random reads hit the FUSE page cache.** A fresh mount per cell means the FUSE
  page cache starts empty, but `rand4k-*` without O_DIRECT re-reads the same 1 GiB for
  30 s, so after the first pass nearly everything is served by the kernel without the
  daemon (`daemon.reads` shows how much reached it). The `-dio` variants measure the
  daemon; readahead also turns buffered 4 KiB reads into larger requests.
- **Cold random reads get prefetched by some modes.** `memcache` reads the whole 4 MiB
  chunk on first touch and a fault on an `mmap`/`vmsplice` mapping reads around it
  (`read_ahead_kb` of the store device), while `copy`, `splice` and `uring*` issue exactly
  the 4 KiB the kernel asked for. A 30 s cold `rand4k` run therefore pulls the whole 1 GiB
  file in within seconds for the former and becomes a warm run (see `disk_read_mib`).
- **io_uring queues are per CPU.** A request is queued on the ring of the CPU that issued
  it, so one sequential reader's readahead is served by one daemon thread, where the
  /dev/fuse transport spreads it over the worker pool; single-stream buffered reads can be
  slower over io_uring even though per-request cost is much lower (4 KiB O_DIRECT).
- **Passthrough + O_DIRECT** reads the backing file with O_DIRECT, i.e. from disk even
  when warm (`disk_read_mib` shows it).
- **Request sizes.** On 7.0 buffered reads arrive as <= 256 KiB requests mostly; on 7.3
  (which lifted the 128 KiB buffered-read limit) they are 1 MiB (`max_pages` = 256, set by
  libfuse from `max_write`; `/proc/sys/fs/fuse/max_pages_limit` caps it).
- **io_uring memory.** libfuse allocates one queue per CPU with `--uring-q-depth` (8)
  entries of `max_write` (1 MiB) payload each: ~128 MiB on 16 CPUs, visible in RSS.
- **uring-zc is a draft.** `zero_copy_v7` (2026-08) is unmerged. Known problems:
  `-o io_uring_zero_copy` writes an int through a bool and zeroes `io_uring_q_depth`
  unless the latter comes later on the command line (chunkfs orders the options);
  the daemon aborts with `Received invalid commit_id=0` at unmount (stats are written
  before unmount, via SIGUSR2); `fi->zerocopy` is not handed back on READ, so chunkfs keeps
  the flag in `fh`; `fuse_do_zero_copy()` issues one READ_FIXED per request, so a request
  that spans two chunks is read from `<name>.whole` (`zc_spanning_whole`).
- **Filesystem under the store** matters (`store_fs`): the remote box's root is btrfs
  (checksums on cold reads, large folios); the local dev box is ZFS (ARC, not the page
  cache: `fincore` shows nothing and local numbers are only indicative).
- **libaio on buffered files is synchronous**; `rand128k-aio` without O_DIRECT is a
  queue-depth-1 workload in practice.
- `cpu_s_per_gib` excludes CPU spent by fio and kernel threads; read it together with
  `sys_cpu_busy_pct` and `fio.sys_cpu_pct`.
