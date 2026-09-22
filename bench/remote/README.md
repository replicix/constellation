# Remote real-world benchmark driver (plan 29 post-M5 verification, Phase A)

python3 stdlib only. Drives the `constellation` CLI over ssh against the
4-host EC2 fleet described in `SPEC.md`, mounting a real S3-backed
filesystem and measuring FUSE-level operation latency/throughput from
each node concurrently.

## Layout

- `common.py` -- shared primitives: ssh/scp fan-out, binary install,
  `fs create`/`mount`/`umount`/`write-mode`/`status` wrappers, JSON
  `status` parsing, quiesce-wait (poll `journal_backlog` +
  `pending_uploads` to 0), log collection. **Reused by Phase B** (the
  adversarial-correctness driver) -- keep it generic here.
- `bench_worker.py` -- runs ON each mount host (deployed via
  `common.deploy_worker`). One process per node, N threads inside,
  wall-clock start barrier (`--start-at`), per-op latency + errno
  recording, JSON result file. Modes: `create_empty`, `small_write`,
  `stat_hot`, `readdir`, `rename_unlink`, `seq_write`, `seq_read`,
  `visibility_write`, `visibility_poll`, `untar`, `findwc`.
- `phase_a.py` -- orchestrator: mounts a plain (and optionally E2E) FS
  on the fleet, runs each spec row at the requested node counts/thread
  counts, aggregates, writes `results/<run>/<row>.json` +
  `phase_a_summary.json`.
- `smoke.py` -- 2-node sanity check (fs create, cross-node registration
  via `mount --s3`, write/read both directions, P2P/forwarding evidence
  via `status`).

## Running

```bash
# Fresh S3 prefix per run -- never reuse an old one (fs create is NOT
# idempotent; see the docstring on common.fs_create_first).
PREFIX="constellation-verify-$(date -u +%Y%m%dT%H%M%SZ)-bench"

# 2-node smoke test first, always:
python3 smoke.py "$PREFIX-smoke" a b

# Full (or partial) Phase A matrix:
python3 phase_a.py "$PREFIX" results/<timestamp> 1 2 5 6 7
# row selectors: 1 2 3 4 5 6 7 e2e (default: all of 1-7)
```

Node names `a b c d` map to the fixed fleet IPs in `common.HOSTS` (the 4
hosts from `SPEC.md`). Env var `SEQ_MB` overrides the per-thread size
(MiB) for row 5's sequential write/read (default 512; spec asks for
2048 = 2 GiB, scaled down here to fit the time budget -- see
`bench/remote/RESULTS.md` for the note on this deviation).

## Pitfalls discovered while building this (read before extending)

1. **`fs create` is not idempotent / has no "just register" mode.**
   Running it on a second node against a prefix another node already
   created fails with "filesystem already exists at this prefix" (see
   `crates/cli/src/main.rs` `Command::Fs { command: FsCommand::Create }`
   -- it unconditionally calls `store.create_fs`). The *actual*
   multi-node join path is `mount --s3 <url> NAME MOUNTPOINT` on every
   node other than the creator (`common.fs_create_first` +
   `common.mount(..., prefix=...)`).
2. **`--state-dir` forces the ad-hoc/unregistered mount path**, bypassing
   the registry entirely (see `cmd_mount` in `crates/cli/src/main.rs`:
   `if state_dir.is_none() { ...Target::Named... }`). Never pass
   `--state-dir` for a registered-name mount used across nodes; use the
   bare `NAME` for `mount`/`umount`/`status`/`log tail` instead and let
   it use the CLI's own default (`~/.local/share/constellation/<name>`).
3. **A prefix is permanently bound to one filesystem.** Re-running the
   matrix needs a brand new prefix (and typically a new local registry
   `NAME` too, since a name that's already registered under an old
   prefix on some host refuses to be repointed -- `constellation export
   NAME` first if you must reuse a name).
4. The control-machine-to-EC2 ssh/scp link in this sandbox is bandwidth
   limited to roughly 200-400 KB/s (NOT the inter-host or host-to-S3
   AWS-backbone bandwidth, which is fast) -- avoid transferring the
   ~43 MB binary or large payloads over it; `common.install_binary_if_needed`
   skips the transfer when the version already matches, and workloads
   fetch tarballs directly on each host via `curl`, not via scp from the
   control machine.
5. `status <NAME>` prints one tracing INFO line then a single JSON
   object -- `common.parse_status_json` finds the first `{` and decodes
   just that object, ignoring the log line and the trailing newline.
6. Re-running the same row/directory twice writes the same deterministic
   filenames (`n<node>-<thread>-<i>`) and collides with `EEXIST` on the
   node that succeeded last time. `phase_a.py` suffixes directories with
   the node-count/write-mode so repeat invocations of *different* rows
   don't collide; re-running the *identical* row twice will still
   collide -- pick a fresh output subdirectory if you need a clean rerun.
