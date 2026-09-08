# Plan 21 — Named filesystems, a shared mount daemon, and a local registry

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context: `docs/explanation/DESIGN.md`
§5 (write authority / availability matrix, per-subtree), §10 (Control Plane),
§13 (Snapshots and Clones, esp. "Mounting subtrees and snapshots anywhere").

## Goal

Today every `constellation` filesystem is addressed by a raw `--s3 URL` or a
raw `--state-dir DIR`, and every `mount` invocation is its own OS process with
its own cluster node identity, cache, and metadata replica — even when two
mounts share the same bucket/prefix on the same machine. Operators juggle
paths by hand, like running `zfs`/`zpool` by device path instead of pool name.

This plan makes filesystems nameable and durable across invocations, the way
`zpool`/`zfs` operate on a pool name resolved through a local
import cache, while also closing a real, already-specified architecture gap:
DESIGN.md §13 says plainly —

> One daemon per (bucket, prefix) per machine serves all its mounts — they
> share the replica, chunk cache, leases, and P2P endpoint.

— but the implementation deliberately deferred that
(`docs/plans/v1/PROGRESS.md:579-581`, phase 6a): *"Each mount remains one
process ... sharing one daemon among several FUSE sessions is not needed for
correctness or the snapshot-mount scenario."* That was true for correctness.
It stopped being sufficient once naming needs one daemon to answer for every
view (root + subtrees) registered under one name — the whole point of "mount
myfs brings up everything you last had mounted" is that those views share one
node identity, one cache, one set of leases, not N independent ones.

DESIGN.md §10 also already lists `fs create|mount|umount` as CLI surface;
`umount` does not exist yet (`crates/cli/src/main.rs` has no `Unmount`
variant — shutdown today is SIGTERM/SIGINT or an external `fusermount -u`).
This plan adds it as part of the same work, since the daemon model needs a
clean way to detach one mountpoint without killing the others.

Non-goals: no changes to the S3 layout, log format, lease protocol, or
control-socket wire format beyond adding new verbs. No cluster-wide identity
changes — the registry is purely local-per-machine bookkeeping, same as
`zpool.cache`. No change to how snapshots/clones work, only to how many
processes serve them.

## Settled decisions (from design discussion, do not relitigate)

- **Naming syntax**: `myfs:/sub/tree` — a bare token before `:` with no `/`
  is a filesystem name; the existing path/selector argument follows
  unchanged. A leading `/` is never reinterpreted (back-compat with literal
  paths passed alongside explicit `--state-dir`). Commands with no
  path/selector argument (`status`, `quota get/set`, `cache ls|stat|prune`,
  `pins`, `designations`, `reintegrate`, `leave`, `write-mode`, `log tail`,
  `gc run|verify`, `fsck`, `debug snap-refs`) take the bare name as a new
  first positional.
- **Registry location**: per-user, `$XDG_CONFIG_HOME/constellation/registry.toml`
  (fallback `~/.config/constellation/registry.toml`), override via
  `CONSTELLATION_REGISTRY`. No system-wide tier for v1.
- **No separate `register`/`unregister` verbs.** `mount` is the only thing
  that writes the registry, and it does so implicitly: give it new
  arguments (`--s3`, a new mountpoint, a new subtree) and it overwrites the
  stored config for that name/view, the same way `zpool import` refreshes
  `zpool.cache`. `constellation forget NAME` removes the local entry only
  (no data touched) — the one explicit "leave the registry" verb, roughly
  `zpool export` without the "must not currently be busy" ceremony.
- **`fs create` gets a mandatory positional name**: `constellation fs create
  NAME --s3 URL [...]`, matching `zfs create pool/dataset`. It registers
  `NAME` with no views/mounts yet.
- **Subtree views are separate FUSE sessions inside one daemon process, not
  separate cluster nodes.** One name maps to one `NodeRuntime` (one node
  identity, one `meta.db`, one cache, one lease keeper) hosting zero or more
  mounted views, each an independent `fuser::Session` on its own thread.
  Views are recorded per-name as `[[NAME.mounts]]` rows: `subtree`,
  `mountpoint`, and per-view options (the process/cache/leases are
  name-level, not per-row).
- **`mount NAME`** (bare, no subtree, no mountpoint) starts (or attaches to)
  the daemon for `NAME` and mounts *every* registered view for it, each at
  its stored mountpoint. `mount NAME[:/sub] MOUNTPOINT` mounts/updates just
  that one view. `mount NAME[:/sub]` with no mountpoint uses that view's
  stored mountpoint (error if not yet registered).
- **`unmount` mirrors it**: `unmount NAME:/sub` detaches one view;
  bare `unmount NAME` detaches every view the daemon currently has mounted.
  The daemon process exits (after its normal clean-shutdown sequence) once
  its last view is removed.
- **Daemonization follows JuiceFS**, the closest comparable tool
  (S3-backed, POSIX, multi-node, production-oriented, no libfuse
  fork-on-mount to lean on): `mount` backgrounds itself by default
  (fork + `setsid`, PID file under `state_dir`, logs redirected to the file
  `constellation log tail` already reads), with `-f`/`--foreground` to opt
  out for debugging, and the new `unmount`/`stop` path as the clean way down
  instead of `kill`.
- **Deferred, not decided against**: what a bare `myfs:/path` control command
  (`pin`, `quota`, ...) targets when a name currently has more than one
  mounted view. Implement the multi-view daemon first; come back to this
  once it's real and can be judged against actual multi-view usage rather
  than hypothetically. Until resolved, such commands require exactly one
  mounted view for the name and error listing the mounted views otherwise.

## Architecture overview

```
constellation mount myfs /mnt --s3 s3://bucket/prefix
        │
        ├─ registry lookup/merge  (Step 4)
        ├─ probe <state_dir>/control.sock
        │     alive  → send MountAdd over the socket, return   (Step 2/6)
        │     absent → this process becomes the daemon:
        │                 NodeRuntime::start(...)               (Step 0)
        │                 NodeRuntime::add_mount(root, /mnt)     (Step 1)
        │                 daemonize (fork/setsid/PID file)       (Step 3)
        │                 serve control socket, block            (Step 1/2)
```

## Step 0 — Extract `NodeRuntime` from `mount()`

`mount()` (`crates/cli/src/main.rs:832-2115` roughly) currently does
everything in one function body for one FUSE session. Split what is
genuinely per-node (built once, shared by every view) from what is
per-view (the FUSE session itself).

**Per-node** (unchanged logic, just relocated into `NodeRuntime::start`):
backend open, `fsmeta`/E2E keyring load, `state_dir` creation, `meta.db`
open + `node_id` claim/validate (`main.rs:876-947`), staging GC + budget
(`main.rs:952-970`), `DiskCache::open[_keyed]` (`main.rs:972-977`),
`SnapshotManager` (`main.rs:982-988`), conditional-write capability probe +
`LeaseMode` (`main.rs:1038-1056`), `WriteModeState`, `LeaseKeeper` +
`lease_views` map (`main.rs:1057-1073`), P2P endpoint (`main.rs:1088`), the
periodic GC task (`main.rs:1089-1118`), `EpochManager` (`main.rs:1119+`),
the sync/shipper task, and the control-socket server.

**Per-view** (moves into a new `NodeRuntime::add_mount`): `inner_path` /
`@snapshot` selector parsing, `rw`/`--clone-name`/`--ephemeral` clone
creation, `mounted_path` resolution (`main.rs:989-1032`), the `FuseFs`
construction, and the `fuser::Session::new/run` pair
(around `main.rs:2079-2115`).

New module `crates/cli/src/node_runtime.rs`:

```rust
pub struct NodeRuntime {
    // node_id, meta, store, cache, snapshots, lease keeper/views,
    // write_mode, peers, epochs, sync_tx, staging_budget, state_dir, ...
    mounts: std::sync::Mutex<HashMap<MountId, MountHandle>>,
}

impl NodeRuntime {
    pub async fn start(cfg: NodeConfig) -> Result<Arc<Self>>;
    pub async fn add_mount(self: &Arc<Self>, view: ViewConfig) -> Result<MountId>;
    pub async fn remove_mount(&self, id: MountId) -> Result<()>;
    pub fn mounts(&self) -> Vec<MountInfo>;
    /// Clean shutdown: drain shipper, release leases, close meta.db.
    /// Called once, when the last mount is removed or on signal.
    pub async fn shutdown(&self) -> Result<()>;
}
```

`add_mount` spawns its `fuser::Session::run()` on a dedicated OS thread
(FUSE callbacks are already synchronous worker-thread code per
`CONVENTIONS.md`); the thread's `FuseFs` holds `Arc<NodeRuntime>` plus its
own `mounted_path`/root inode and its own `ephemeral_clone` cleanup state.
`remove_mount` unmounts that one session (fuser's session handle gives you
the mountpoint; unmount it — see the existing `SessionUnmounter` hook
referenced at `main.rs:2076` — and join the thread) without touching
siblings.

Existing single-view scenarios (harness, smoke/integration tests) must
still work driving `NodeRuntime` with exactly one `add_mount` call — no
behavior change for that case, just a different call shape.

## Step 1 — Control socket grows mount-management verbs

`crates/api` (`Request`/`Response` enums, server loop in
`crates/api/src/lib.rs`) gains:

```rust
MountAdd { subtree: String, mountpoint: PathBuf, opts: MountViewOpts },
MountRemove { subtree: String },   // "" = root
MountList,                          // -> Vec<{ subtree, mountpoint, since }>
```

Server-side handlers call the corresponding `NodeRuntime` methods. This is
what lets a second CLI invocation extend an *already-running* daemon rather
than starting a new process, and what `constellation mount myfs` (bare) uses
internally to bring up every registered view in one daemon.

## Step 2 — Local registry

New crate module `crates/cli/src/registry.rs`. File at
`$XDG_CONFIG_HOME/constellation/registry.toml` (or `CONSTELLATION_REGISTRY`).
Format:

```toml
[myfs]
s3 = "s3://bucket/prefix"

[[myfs.mounts]]
subtree = ""                 # "" = root
mountpoint = "/mnt"
state_dir = "/home/user/.local/share/constellation/myfs"
cache_size = "10G"
cache_dir = ""                # optional override, default <state_dir>/cache
allow_other = false
fsync_mode = "local"
write_mode = "through"
```

`state_dir` moves from UUID-keyed (`default_state_dir`,
`main.rs:3847-3857`) to name-keyed when a name is given:
`$XDG_DATA_HOME/constellation/<name>`. Keep the UUID-keyed default for the
no-name / raw `--s3`/`--state-dir` path — unchanged back-compat.

API: `Registry::load()`, `.entry(name) -> Option<&FsEntry>`,
`.merge_and_save(name, overrides) -> FsEntry` (the "any explicit argument
overwrites the stored value" rule), `.forget(name)`.

Concurrency note: the registry file itself needs simple advisory locking
(flock) around merge-and-save, since two `mount` invocations for different
names could run concurrently; this is a local convenience file, not a
correctness-critical store, so a coarse whole-file lock is enough.

## Step 3 — CLI: name resolution helper

New `crates/cli/src/target.rs`: given a raw string like `myfs:/data` or a
bare `myfs`, split on `:` **only** when the part before it contains no `/`;
look it up in the registry. Returns an enum:

```rust
enum Target {
    Named { name: String, path: Option<String>, entry: FsEntry },
    Raw(String), // legacy literal path/selector, requires --state-dir/--s3
}
```

Every subcommand that currently takes `path: String` / `selector: String`
plus `#[arg(long)] state_dir: PathBuf` switches `state_dir` to
`Option<PathBuf>` and resolves the effective state dir as: explicit
`--state-dir` if given, else the registry lookup from the positional
`Target`, else error. Commands with no path/selector argument
(`status`, `pins`, `designations`, `reintegrate`, `leave`, `write-mode`,
`quota get/set`, `inspect`, `cache ls/stat/prune`, `log tail`,
`gc run/verify`, `fsck`, `debug snap-refs`) gain a new leading positional
`name: Option<String>` used the same way.

When a name resolves to a daemon with more than one mounted view and the
command needs exactly one state dir (see "deferred" above): if exactly one
view is currently mounted, use it; otherwise error listing the mounted
views and ask for `--state-dir` explicitly.

## Step 4 — `mount`/`unmount` command rework

`Command::Mount` argument shape becomes (`crates/cli/src/main.rs:49-109`
area):

```rust
Mount {
    target: String,                 // "myfs" or "myfs:/sub" or legacy raw path with --s3
    mountpoint: Option<PathBuf>,
    #[arg(long)] s3: Option<String>,
    #[arg(long)] state_dir: Option<PathBuf>,
    #[arg(long, short = 'f')] foreground: bool,
    // cache_size, allow_other, fs_name, fsync_mode, write_mode, read_only_member,
    // rw, clone_name, ephemeral, web_ui: unchanged, all become registry overrides
}
```

Dispatch (`crates/cli/src/main.rs`, new `cmd_mount`):

1. Resolve `target` via Step 3 helper; merge any explicit flags into the
   registry entry (Step 2's `merge_and_save`) — this is the one place that
   *writes* the registry.
2. Probe `<state_dir>/control.sock` (reuse `control_call`'s connect logic,
   `main.rs:2527`).
3. Socket alive → send `MountAdd` for the resolved view (or, for a bare
   name with no subtree/mountpoint given, `MountAdd` for every registered
   view not already in `MountList`). Print result, exit 0. No new process.
4. Socket absent → this invocation becomes the daemon: `NodeRuntime::start`,
   then `add_mount` for the resolved view (bare name: for every registered
   view), then daemonize (Step 5) unless `--foreground`, then serve the
   control socket and block.

New `Command::Unmount { target: String, #[arg(long)] state_dir: Option<PathBuf> }`:
resolve target, connect to the socket, send `MountRemove` (bare name → one
`MountRemove` per currently-mounted view). If the daemon's mount count
reaches zero it runs its shutdown sequence and exits; the CLI call itself
just waits for the socket to close (or a bounded timeout) before returning.

## Step 5 — Daemonization (JuiceFS-style)

New `crates/cli/src/daemonize.rs`. On `mount` without `--foreground`, after
`NodeRuntime::start` succeeds but before entering the blocking serve loop:

- Fork; parent waits (via a pipe) for the child to report "control socket
  bound and first mount attached" or an error, then exits with the child's
  status — so failures (bad `--s3`, bind conflicts, wrong passphrase) still
  surface synchronously to the invoking shell instead of silently
  backgrounding a dead process.
- Child: `setsid()`, redirect stdout/stderr to
  `state_dir/daemon.log` (the same ring buffer `log_buffer` already feeds —
  route `tracing` output there instead of stdout when daemonized), write
  `state_dir/daemon.pid`.
- `constellation unmount` (last view) and existing SIGTERM/SIGINT handling
  both go through `NodeRuntime::shutdown`; on clean exit remove the PID
  file.
- `--foreground`/`-f` skips all of this and behaves exactly like `mount`
  does today (useful for the harness and debugging — the harness's
  `Client` in `crates/harness/src/client.rs` should keep using
  `--foreground` so it retains direct process-lifetime control).

## Step 6 — `fs create` and `forget`

`FsCommand::Create` (`main.rs:348-...`) gets a mandatory leading positional
`name: String` alongside the existing `--s3` and format options; on success
it calls `Registry::merge_and_save(name, { s3, ..no mounts })`.

New top-level `Command::Forget { name: String }`: `Registry::forget(name)`,
refusing (with a clear message, not a crash) if the daemon for that name is
currently reachable — point the user at `unmount` first. No data is
touched; this only edits the local TOML file.

## Step 7 — `constellation list`

New `Command::List` (or `Fs { command: FsCommand::List }` — pick whichever
reads better next to `fs create`/`fs passwd`): print every registry entry —
NAME, S3, and for each view: SUBTREE, MOUNTPOINT, MOUNTED?(probe socket +
`MountList`), STATE-DIR. Analogous to `zfs list`/`zpool list`.

## Config knobs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_REGISTRY` | `$XDG_CONFIG_HOME/constellation/registry.toml` | registry file path |
| `CONSTELLATION_NO_DAEMONIZE` | unset | force `--foreground` behavior cluster-wide (CI/harness convenience) |

## Tests

Unit:
- Registry: merge-and-save overwrite semantics, `forget`, TOML round-trip,
  concurrent-writer lock.
- Target-resolution helper: `myfs:/data` vs `/myfs:backup` (leading-`/`
  never reinterpreted) vs bare `myfs` vs unregistered raw path (falls back,
  requires explicit `--state-dir`).
- `NodeRuntime::add_mount`/`remove_mount` against an in-memory backend:
  two views, remove one, assert the other keeps serving reads and the
  process-level state (node_id, lease keeper) is untouched.

Multi-node / in-process (pattern at the bottom of `crates/cli/src/shipper.rs`):
- Two views of the same `NodeRuntime` (root + subtree) converge with a peer
  node exactly as two independent single-view processes did before this
  plan — this is the regression test that the daemon-sharing refactor
  changed *how many processes*, not correctness.
- `MountAdd` against a running daemon from a second CLI invocation actually
  avoids claiming a second `node_id`.

Harness (`crates/harness/src/scenarios.rs`):
- New scenario: `mount myfs /mnt --s3 ...`, then `mount myfs:/sub /mnt2`
  from a second CLI call, assert one process (one `node_id`, one PID file),
  both mountpoints usable; `unmount myfs:/sub`, assert `/mnt` keeps working
  and the daemon is still alive; `unmount myfs`, assert clean exit and PID
  file removed.
- Daemonize crash-reporting path: bad `--s3` on first mount exits
  non-zero *synchronously* (parent waits on the pipe) rather than
  backgrounding silently.
- Keep `Client` in the harness on `--foreground` throughout (per Step 5) so
  existing scenarios are unaffected by backgrounding.

Compliance: pjdfstest must stay a FULL pass — this plan does not touch the
FUSE data path, only process/session lifecycle around it, but run it
against a daemonized `mount myfs` (not just `--foreground`) at least once
to catch anything daemonization-specific (fd inheritance, cwd, signal
delivery through `setsid`).

## Suggested implementation order

1. Step 0 (`NodeRuntime` extraction) with `--foreground`-equivalent
   single-view behavior only — inert refactor, all existing tests must
   pass unchanged before moving on.
2. Step 1 (control socket verbs) + multi-view `add_mount`/`remove_mount`,
   proven by the in-process multi-view test above. Still no CLI/registry
   changes yet — reachable only via direct `NodeRuntime` calls in tests.
3. Step 5 (daemonization) in isolation, gated behind `--foreground`
   defaulting off only once Step 1 is solid.
4. Steps 2-4, 6-7 (registry + CLI naming + `fs create`/`forget`/`list`) —
   the user-facing layer, now with a real daemon underneath it.
5. Harness scenario + docs (`README.md`, `docs/reference/configuration.md`,
   a new `docs/reference/features/named-filesystems.md`).

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- Explicit confirmation (paste output) that a two-view mount (`mount myfs`
  then `mount myfs:/sub`) results in exactly one OS process and one
  `node_id`.
- pjdfstest FULL pass with the mount under test daemonized (not
  `--foreground`).
- `docs/plans/v1/PROGRESS.md` note resolving the phase-6a deferral cited
  above — record that the daemon-sharing model is now implemented and why
  (naming UX, not correctness) it was worth doing.
