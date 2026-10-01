# fuse-handover-probe: fd passing and session handover between three processes

Plan 37 milestone K0, Track A (`docs/plans/v1/wip/37-kubernetes-csi.md` §8, §15
questions 1-5). Constellation's CSI driver (plan 37) mounts a PV in the **node
plugin** (privileged: it calls `mount(2)` itself through
`constellation_platform::linux::fuse_mount_fd`), hands the `/dev/fuse` descriptor to
an **engine pod** over `SCM_RIGHTS`, and — when that engine pod is replaced — hands
the still-mounted connection on to its successor, so the replacement is a stall and
not an `ENOTCONN` outage.

K0 is the spike that proves the mechanism before K1-K7 are built on it. This bench is
its Track A: `crates/frontend-fuse/examples/handover_probe.rs`, a throwaway
measurement binary with no Kubernetes and no engine in it, walking exactly that chain
with a writer and a reader on the mountpoint throughout:

```text
  A (driver, "node plugin")      B ("old engine pod")      C ("new engine pod")
  ── fuse_mount_fd(mnt) ────────▶ SCM_RIGHTS ──▶ from_fd + FUSE_INIT, serves
     (closes its own copy)
  ── {"cmd":"detach"} ──────────▶ SessionControl::detach
  ◀── SCM_RIGHTS + NegotiatedInit ─┘   (nobody reads /dev/fuse from here)
  ── SCM_RIGHTS + NegotiatedInit ───────────────────────▶ FuseSession::resume, serves
  ── {"cmd":"exit"} ────────────▶ B exits
```

The findings are in [RESULTS.md](RESULTS.md) and, in the form the plan is gated on,
in the plan's own "K0 results" section.

## What it serves, and why not a mock

`ProbeVfs` (in the example) is a flat read/write passthrough to one scratch
directory: a fixed inode table (`2 + index` over the directory's names, sorted) and
`fh == ino`. Two properties matter, and an in-memory mock has neither.

- **Any process can serve the connection.** The numbering comes from the directory,
  so B and C agree on it with no handle table crossing between them. The probe
  therefore measures the FUSE *transport* of a handover — the descriptor, the
  negotiated `FUSE_INIT`, the kernel's queue — and not the engine's
  `HandleTableSnapshot`, which the `upgrade-under-load` harness scenario already
  covers against a real engine and real data.
- **The data is continuous across the handover**, because the bytes live in the
  scratch directory: a reader verifies after a handoff exactly what a writer wrote
  before it (the probe counts byte mismatches, not just errors). Writers `pwrite`
  an offset-keyed pattern into their own files; readers `pread` *those* files with
  **`O_DIRECT`** and check the pattern. The `O_DIRECT` is the point: without it the
  kernel answers a reader from cached pages, the read never reaches `/dev/fuse`,
  and the reader's numbers say nothing about the handover it was supposed to cross
  (K0's first revision of this probe had exactly that bug — its readers were 0.2 ms
  through a 10 s pause because no request ever left the page cache).

## Running it

The probe calls `mount(2)` itself, so it needs `CAP_SYS_ADMIN` and `/dev/fuse`.

```bash
cargo build --release --example handover_probe -p constellation-frontend-fuse
bench/fuse-handover-probe/run.sh --in-container          # privileged docker, installs fio
bench/fuse-handover-probe/run.sh --deep --in-container   # the deep-queue run
bench/fuse-handover-probe/run.sh                         # directly, if you are root
```

Useful knobs: `--handoffs N`, `--pauses 500,2000,5000,10000` (ms with nobody reading
`/dev/fuse`), `--writers`/`--readers`/`--threads`, `--span-mib`, `--fio <binary>`,
`--skip-q1`/`--skip-chain`, `--json <path>`, `--keep`.

The chain phase **asserts** §15's "zero I/O errors": a client error, a byte
mismatch, a failed post-resume check, a resumed session reporting a different
`NegotiatedInit`, or a non-zero fio status exits non-zero (after writing the JSON,
which is exactly what a failing run's reader wants). So a kernel that one day does
surface `EIO` to a writer during a handover turns this red instead of hiding in a
field nobody reads.

The probe mounts `fusectl` on `/sys/fs/fuse/connections` when it is not there (a
container gets a fresh `sysfs` without it), so the kernel's per-connection
`waiting`/`max_background`/`congestion_threshold` are readable wherever it runs.

## Results in this directory

| file | what |
|---|---|
| `results/k0-track-a.json` | the headline run (`run.sh`): 40 handoffs, pauses 0.5/2/5/10 s, 8 writers + 4 readers + fio |
| `results/k0-track-a-deep-queue.json` | `run.sh --deep`: 64 writers + 16 readers, to push the kernel queue past `congestion_threshold` and `max_background` |
| `results/k0-question5-kind.txt` | question 5: a kind cluster from `tests/csi/kind-config.yaml`, `/dev/fuse` per node, and six pod variants (privileged / unprivileged / device-mounted / `SYS_ADMIN`) |
| `results/k0-stage-volume.txt` | the `NodeStageVolume` shape against the real daemon (`stage-volume.sh` → `crates/cli/examples/stage_volume_probe.rs`) |

## The second probe

`crates/cli/examples/stage_volume_probe.rs` does the chain's *first hop* — mount(2)
here, `SCM_RIGHTS`, serve there — with the product's control protocol, authz and
engine in the path: `view.mount` with `MountSource::PreopenedFd` against a running
`constellation` daemon. `stage-volume.sh [--in-container]` sets up a filesystem on
the local file backend, starts the daemon and runs it.
