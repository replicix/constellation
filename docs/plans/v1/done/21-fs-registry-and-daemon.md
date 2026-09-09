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

**Breaking changes are explicitly allowed.** This is pre-1.0 surface: CLI
argument shapes, the control-socket response types, the state-dir layout and
the test/tooling call sites may all change where the new model is cleaner.
Do not preserve an old spelling "just in case" — update every call site
instead (see "Call-site sweep" below).

Non-goals: no changes to the S3 layout, log format, or lease protocol. No
cluster-wide identity changes — the registry is purely local-per-machine
bookkeeping, same as `zpool.cache`. No change to how snapshots/clones work,
only to how many processes serve them. The control-socket wire format does
change, but only where multi-view forces it (`StatusReport`, see Step 1).

## Settled decisions (from design discussion, do not relitigate)

- **Naming syntax**: `myfs:/sub/tree` — a bare token before `:` with no `/`
  is a filesystem name; the existing path/selector argument follows
  unchanged. A leading `/` is never reinterpreted, so a literal path passed
  alongside an explicit `--state-dir` still means what it says. A bare name
  with no `:` at all means "the whole filesystem" for commands that take a
  path — see the per-command table in Step 3, not a blanket rule, since a
  handful of commands (snapshot selectors, `debug snap-refs`) don't fit it
  cleanly.
- **`pin`/`unpin`/`offline`/`online`/`inspect` stop requiring `--state-dir`
  once a name is registered.** `pin myfs` pins the whole filesystem (root);
  `pin myfs:/path` pins only that subtree and its descendants. `--state-dir`
  remains the explicit escape hatch for an unregistered/ad-hoc mount; the
  command fails with a clear "no such filesystem" error if `myfs` has no
  registry entry and no `--state-dir` was given.
- **A subtree view is just another entrypoint into the same filesystem, and
  the daemon does not care which one an operation arrived through.** Control
  paths are filesystem-absolute and always were (`pin /data` today resolves
  against the replica, not against a mount's root inode). `pin myfs:/data`
  therefore pins `/data` whether `/data` is reachable through the root mount,
  through a subtree mount at `/mnt/data`, through both, or through neither.
  The number of mounted views never changes what a control command means and
  never makes one ambiguous.
- **Registry location**: per-user, `$XDG_CONFIG_HOME/constellation/registry.toml`
  (fallback `~/.config/constellation/registry.toml`), override via
  `CONSTELLATION_REGISTRY`. No system-wide tier for v1.
- **No separate `register`/`unregister` verbs, and no `forget` either.**
  `mount` is the only thing that *writes* a live entry: give it new
  arguments (a new mountpoint, a new subtree, new per-view options) and it
  overwrites the stored config for that name/view, the same way `zpool
  import` refreshes `zpool.cache`. `--s3` is the one exception — see
  "Identity is pinned to the name" below. The only way to *remove* a name is
  the new **`export`** command (Step 6): one-shot teardown — leave the
  cluster if a `node_id` was ever claimed, unmount every view, delete the
  state dir, then delete the registry row. For a name that was registered
  (e.g. via `fs create`) but never mounted, `export` just deletes the row.
- **Identity is pinned to the name.** A name owns one state dir, which owns
  one filesystem UUID and one `node_id`. `mount myfs --s3 OTHER` where
  `myfs` already has a populated state dir is an **error**, not a silent
  repoint: it would leave the old `meta.db` serving a different filesystem.
  Point a name somewhere else by `export myfs` first. `--s3` on a name whose
  state dir is empty (registered but never mounted) just fills it in.
- **`fs create` gets a mandatory positional name**: `constellation fs create
  NAME --s3 URL [...]`, matching `zfs create pool/dataset`. It registers
  `NAME` with no views/mounts yet.
- **Subtree views are separate FUSE sessions inside one daemon process, not
  separate cluster nodes.** One name maps to one `NodeRuntime` (one node
  identity, one `meta.db`, one cache, one lease keeper) hosting zero or more
  mounted views, each an independent `fuser::Session` on its own thread.
  Views are recorded per-name as `[[NAME.mounts]]` rows carrying only what is
  genuinely per-view (see Step 2's schema).
- **`mount NAME`** (bare, no subtree, no mountpoint) starts (or attaches to)
  the daemon for `NAME` and mounts *every* registered view for it, each at
  its stored mountpoint. `mount NAME[:/sub] MOUNTPOINT` mounts/updates just
  that one view. `mount NAME[:/sub]` with no mountpoint uses that view's
  stored mountpoint (error if not yet registered).
- **A bare `mount NAME` is all-or-nothing**: if any registered view fails
  to mount, the invocation rolls back the views *it* brought up, reports
  every failure on stderr, and exits non-zero. No half-mounted filesystem
  is left behind for a script to trip over. If this invocation also started
  the daemon, the daemon runs its clean shutdown and exits with it; if it
  was attaching to a daemon that already had views mounted, those
  pre-existing views are untouched and the daemon stays up.
- **`unmount` mirrors it**: `unmount NAME:/sub` detaches one view;
  bare `unmount NAME` detaches every view the daemon currently has mounted.
  The daemon process exits (after its normal clean-shutdown sequence) once
  its last view is removed.
- **`write-mode` is node-level**, shared by every mounted view of a name —
  `constellation write-mode myfs through` affects the whole daemon.
  Matches how `WriteModeState` is already constructed once per process,
  before any per-view code runs; no new per-view state.
- **`clone`'s destination has no name prefix** — `clone myfs:/data@friday
  /data-copy` — the destination is always within the same filesystem as
  the selector, matching today's behavior (clone cannot cross filesystems).
- **`debug snap-refs` takes the name as a separate positional**, not a
  colon prefix on `id` — `debug snap-refs myfs <id>` — since a snapshot id
  isn't path-shaped and stretching the colon convention onto it would be
  more confusing than a second positional.
- **Daemonization follows JuiceFS**, the closest comparable tool
  (S3-backed, POSIX, multi-node, production-oriented, no libfuse
  fork-on-mount to lean on): `mount` backgrounds itself by default
  (fork + `setsid`, PID file under `state_dir`, logs redirected to the file
  `constellation log tail` already reads), with `-f`/`--foreground` to opt
  out for debugging, and the new `unmount`/`stop` path as the clean way down
  instead of `kill`.

## Architecture overview

```
constellation mount myfs /mnt --s3 s3://bucket/prefix
        │
        ├─ registry lookup/merge  (Step 4)
        ├─ fork immediately unless --foreground  (Step 5)
        │     parent: block on the status pipe, exit with the child's verdict
        │     child:  everything below
        ├─ take the exclusive state-dir lock  (Step 4)
        ├─ probe <state_dir>/control.sock
        │     alive  → send MountAdd over the socket, return   (Step 1/6)
        │     absent → this process becomes the daemon:
        │                 NodeRuntime::start(...)               (Step 0)
        │                 NodeRuntime::add_mount(root, /mnt)    (Step 0)
        │                 report success up the pipe            (Step 5)
        │                 serve control socket, block           (Step 1)
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
the sync/shipper task, the control-socket server, and the web UI
(`main.rs:2047-2052` — one HTTP port per daemon, not per view).

**Per-view** (moves into a new `NodeRuntime::add_mount`): `inner_path` /
`@snapshot` selector parsing, `rw`/`--clone-name`/`--ephemeral` clone
creation, `mounted_path` resolution (`main.rs:989-1032`), the `FuseFs`
construction, the per-session mount options (`FSName`, `allow_other` ACL,
the `RO` flag for read-only snapshot selectors, `n_threads`), and the
`fuser::Session::new/run` pair (around `main.rs:2079-2115`).

New module `crates/cli/src/node_runtime.rs`:

```rust
pub struct NodeRuntime {
    // node_id, meta, store, cache, snapshots, lease keeper/views,
    // write_mode, peers, epochs, sync_tx, staging_budget, state_dir, ...
    mounts: std::sync::Mutex<HashMap<MountId, MountHandle>>,
}

impl NodeRuntime {
    pub fn start(cfg: NodeConfig, rt: tokio::runtime::Handle) -> Result<Arc<Self>>;
    pub fn add_mount(self: &Arc<Self>, view: ViewConfig) -> Result<MountId>;
    pub fn remove_mount(&self, id: MountId) -> Result<()>;
    pub fn mounts(&self) -> Vec<MountInfo>;   // { id, subtree, mountpoint, since }
    /// Clean shutdown: drain shipper, release leases, close meta.db.
    /// Called once, when the last mount is removed or on signal.
    pub fn shutdown(&self) -> Result<()>;
}
```

Note the signatures are **synchronous, taking a `tokio::runtime::Handle`**,
not `async fn`. Today's `mount()` is a sync function that owns the
`Runtime` and drives async work through `rt.block_on` / `rt.enter()`
(`main.rs:2043`), and `DaemonStatus` already stores a `Handle` for exactly
this reason (`main.rs:3167`). Keeping `NodeRuntime` sync-with-a-`Handle`
makes Step 0 the inert refactor it is supposed to be; turning the whole
mount path async is a separate change and is out of scope here.

`add_mount` spawns its `fuser::Session::run()` on a dedicated OS thread
(FUSE callbacks are already synchronous worker-thread code per
`CONVENTIONS.md`); the thread's `FuseFs` holds `Arc<NodeRuntime>` plus its
own `mounted_path`/root inode and its own `ephemeral_clone` cleanup state
(today that cleanup runs after `session.run()` at `main.rs:2121` — it moves
into the per-view thread's teardown). `remove_mount` unmounts that one
session via its `SessionUnmounter` (`main.rs:2076`) and joins the thread,
without touching siblings.

Each view gets its own `fuser` worker threads (`n_threads`,
`main.rs:2073`), so kernel worker threads scale with view count. What the
views actually share — and what this plan is for — is the node identity,
the replica, the chunk cache, the lease keeper, the P2P endpoint and the
shipper. Say so in the docs; don't oversell "one daemon" as "one thread
pool".

**Signals become node-level.** Today SIGINT/SIGTERM unmount the single
session then drain (`main.rs:2080-2114`). Under `NodeRuntime` the handler
must unmount *every* view, then run `NodeRuntime::shutdown` once. The
"second signal aborts immediately" escape hatch stays as-is.

Existing single-view scenarios (harness, smoke/integration tests) must
still work driving `NodeRuntime` with exactly one `add_mount` call — no
behavior change for that case, just a different call shape.

## Step 1 — Control socket grows mount-management verbs

`crates/api` (`Request`/`Response` enums, server loop in
`crates/api/src/lib.rs`) gains:

```rust
MountAdd { subtree: String, mountpoint: PathBuf, opts: MountViewOpts },
MountRemove { mountpoint: PathBuf },   // the view is identified by where it is mounted
MountList,                             // -> Vec<{ id, subtree, mountpoint, since }>
```

`MountRemove` is keyed by **mountpoint**, not by subtree: the same subtree
may legitimately be mounted at two places, and the mountpoint is what the
user names on the command line. `MountList` also returns the `MountId` for
callers that want to be precise.

Server-side handlers call the corresponding `NodeRuntime` methods. This is
what lets a second CLI invocation extend an *already-running* daemon rather
than starting a new process, and what `constellation mount myfs` (bare) uses
internally to bring up every registered view in one daemon.

**Breaking wire change**: `DaemonStatus.mountpoint` (`main.rs:3155`) and the
`StatusReport.mountpoint` field it feeds become `mounts: Vec<MountInfo>`.
Update the `status` printer, the web UI, and any harness assertion that
reads the old field. This is the one existing response type multi-view
forces to change; do it properly rather than reporting an arbitrary view's
mountpoint.

**`leave` becomes multi-view.** `DaemonStatus::leave` (`main.rs:3452-3492`)
currently captures `self.mountpoint` and shells out to `fusermount3 -u` for
that one path after replying. It must iterate every mounted view instead,
using the same "reply first, detach after" ordering.

## Step 2 — Local registry

New crate module `crates/cli/src/registry.rs`. File at
`$XDG_CONFIG_HOME/constellation/registry.toml` (or `CONSTELLATION_REGISTRY`).
Format — node-level settings live under `[NAME]`, and `[[NAME.mounts]]`
rows carry **only** what is genuinely per-view:

```toml
[myfs]
s3 = "s3://bucket/prefix"
state_dir = "/home/user/.local/share/constellation/myfs"
cache_size = "10G"
cache_dir = ""                # optional override, default <state_dir>/cache
fsync_mode = "local"
write_mode = "through"
read_only_member = false
web_ui = 8080

[[myfs.mounts]]
subtree = ""                  # "" = root; may also be a @snapshot selector
mountpoint = "/mnt"
allow_other = false
fs_name = "myfs"
# rw / clone_name / ephemeral for snapshot views, when used
```

This is the schema the settled decisions imply: the process, cache, leases,
node identity and write mode are name-level, so they cannot sit in a
per-mount row where two rows could disagree.

`state_dir` moves from UUID-keyed (`default_state_dir`,
`main.rs:3847-3857`) to `$XDG_DATA_HOME/constellation/<name>` when a name is
given. The UUID-keyed default stays for the no-name / raw `--s3` /
`--state-dir` path, which has no name to key on.

API: `Registry::load()`, `.entry(name) -> Option<&FsEntry>`,
`.merge_and_save(name, overrides) -> FsEntry` (the "any explicit argument
overwrites the stored value" rule, with the `--s3` exception from the
settled decisions), `.remove(name)` (used only by `export`, Step 6).

Concurrency note: the registry file itself needs simple advisory locking
(flock) around merge-and-save, since two `mount` invocations for different
names could run concurrently; this is a local convenience file, not a
correctness-critical store, so a coarse whole-file lock is enough. It is
*not* the lock that protects the state dir — see Step 4.

## Step 3 — CLI: name resolution helper

New `crates/cli/src/target.rs`: given a raw string like `myfs:/data` or a
bare `myfs`, split on `:` **only** when the part before it contains no `/`;
look it up in the registry. Returns an enum:

```rust
enum Target {
    Named { name: String, path: Option<String>, entry: FsEntry },
    Raw(String), // literal path/selector, requires --state-dir/--s3
}
```

Every subcommand that currently takes `path: String` / `selector: String`
plus `#[arg(long)] state_dir: PathBuf` switches `state_dir` to
`Option<PathBuf>` and resolves the effective state dir as: explicit
`--state-dir` if given, else the registry lookup from the positional
`Target`, else error.

There is exactly one state dir per name, so this resolution never depends
on how many views happen to be mounted. A control command against a name
with zero mounted views fails the same way it does today against a stopped
daemon: the socket is not there, and the error says so.

### Per-command resolution (settled, per the discussion above)

| Command | Today's positional(s) | New form | Bare `myfs` means |
|---|---|---|---|
| `pin` | `<path>` | `pin myfs[:/path]` | whole fs (root) |
| `unpin` | `<path>` | `unpin myfs[:/path]` | whole fs (root) |
| `offline` | `<path>` | `offline myfs[:/path] [--ro]` | whole fs (root) |
| `online` | `<path>` | `online myfs[:/path]` | whole fs (root) |
| `inspect` | `<path>` | `inspect myfs[:/path]` | whole fs (root) |
| `pins` | — | `pins myfs` | n/a — node-level, lists every pin regardless of which view it was set through |
| `designations` | — | `designations myfs` | n/a — node-level |
| `reintegrate` | — | `reintegrate myfs` | n/a — node-level |
| `leave` | — | `leave myfs [--node-id] [--force]` | n/a — node-level, detaches every view |
| `write-mode` | `<mode>` | `write-mode myfs <mode>` | n/a — node-level (see settled decisions) |
| `quota get/set` | — | `quota get/set myfs [size]` | n/a — always whole-fs, never per-subtree |
| `cache ls/stat/prune` | — | `cache ls/stat/prune myfs [--target]` | n/a — node-level (cache is shared across views) |
| `log tail` | — | `log tail myfs [--lines]` | n/a — node-level |
| `status` | — | `status myfs` | n/a — node-level; now lists every mounted view |
| `snapshot create/delete` | `<selector>` | `snapshot create/delete myfs:/path@name` | not applicable — a selector always names a path (`@name` is required already, independent of this plan) |
| `snapshot ls` | `[path]` | `snapshot ls myfs[:/path]` | list from root |
| `clone` | `<selector> <dest>` | `clone myfs:/path@name <dest>` | not applicable (selector always names a path); `<dest>` is a plain path, same fs implied, no prefix |
| `debug snap-refs` | `<id>` | `debug snap-refs myfs <id>` | separate positional, `id` is never path-shaped |
| `gc run/verify` | — (`--s3` required today) | `gc run/verify myfs [--orphans]` | resolves `--s3` always, `--state-dir` opportunistically if a daemon is registered |
| `fsck` | — (`--s3` required today) | `fsck myfs [--repair] [--force-release]` | same as `gc` |
| `doctor` | — (`--s3` required today) | `doctor myfs` | resolves `--s3` from the registry; still needs `--s3` for a name that was never registered |
| `fs passwd` | — (`--s3` required today) | `fs passwd myfs` | resolves `--s3` from the registry |

All of these keep `--state-dir`/`--s3` as explicit escape hatches for
ad-hoc, unregistered mounts.

## Step 4 — `mount`/`unmount` command rework

`Command::Mount` argument shape becomes (`crates/cli/src/main.rs:49-109`
area):

```rust
Mount {
    target: String,                 // "myfs" or "myfs:/sub" or a raw path with --s3
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
   *writes* the registry. Reject an `--s3` that contradicts a populated
   state dir (settled: "identity is pinned to the name").
2. Unless `--foreground`, fork now (Step 5). Everything below runs in the
   child.
3. **Take an exclusive `flock` on `<state_dir>/daemon.lock`, held for the
   daemon's whole lifetime.** This is what makes step 4 race-free: two
   concurrent `mount myfs` invocations must not both conclude "no socket,
   I'm the daemon" and open the same `meta.db` twice.
   - Lock acquired → no daemon is running for this name. If a
     `control.sock` or `daemon.pid` is lying around it is stale from a
     crash: unlink and continue.
   - Lock busy → a daemon owns this state dir; go to step 4 and talk to it.
     If its socket is missing while the lock is held, that is a real error
     (daemon starting up or wedged) — retry briefly, then fail loudly.
4. Socket alive → send `MountAdd` for the resolved view (or, for a bare
   name with no subtree/mountpoint given, `MountAdd` for every registered
   view not already in `MountList`). Print result, exit. No new process.
5. Socket absent (and lock held) → this invocation becomes the daemon:
   `NodeRuntime::start`, then `add_mount` for the resolved view (bare name:
   for every registered view), then report readiness up the pipe (Step 5),
   then serve the control socket and block.

**All-or-nothing bare mount, for both paths above.** When a bare
`mount NAME` brings up several views, every `add_mount` must succeed. On
the first failure, keep going far enough to collect the remaining errors
(so the user sees all of them, not just the first), then `remove_mount`
every view this invocation added, print each failure on stderr naming the
mountpoint and the reason, and exit non-zero.

Roll back only what this invocation added. Attaching to a daemon that
already serves views must never unmount those: they belong to an earlier,
successful command. If this invocation started the daemon, rollback empties
it, so it runs `NodeRuntime::shutdown` and exits rather than lingering with
zero mounts.

A targeted `mount NAME:/sub MOUNTPOINT` has exactly one view, so it either
works or fails — the rollback path is a no-op there.

New `Command::Unmount { target: String, #[arg(long)] state_dir: Option<PathBuf> }`:
resolve target, connect to the socket, send `MountRemove` (bare name → one
`MountRemove` per currently-mounted view, keyed by mountpoint from
`MountList`). If the daemon's mount count reaches zero it runs its shutdown
sequence and exits; the CLI call itself just waits for the socket to close
(or a bounded timeout) before returning.

## Step 5 — Daemonization (JuiceFS-style)

New `crates/cli/src/daemonize.rs`. On `mount` without `--foreground`, fork
**before any runtime work happens** — before `NodeRuntime::start`, before
the tokio runtime spawns workers, before any FUSE session thread exists:

- `fork()` only carries the calling thread into the child. Forking after
  the runtime, shipper, lease keeper, cache and FUSE threads exist gives
  the child a corpse: worker threads gone, mutexes possibly locked by
  threads that no longer run. So the fork is the *first* thing `cmd_mount`
  does after registry resolution, and the child does all the real work.
- Parent: block reading the status pipe until the child reports either
  "control socket bound and first mount attached" or an error (with its
  message and exit code); print and exit with the child's status. Failures
  (bad `--s3`, bind conflicts, wrong passphrase, unmountable mountpoint)
  still surface synchronously to the invoking shell instead of silently
  backgrounding a dead process. If the child dies without writing, the
  parent reports that too rather than hanging — treat EOF-on-pipe as
  failure.
- Child: `setsid()`, then `NodeRuntime::start` + `add_mount(s)`, redirect
  stdout/stderr to `state_dir/daemon.log` (the same ring buffer
  `log_buffer` already feeds — route `tracing` output there instead of
  stdout when daemonized), write `state_dir/daemon.pid`, write the verdict
  up the pipe — success only once *every* requested view is attached, and
  otherwise the collected per-view errors after rollback — then close the
  pipe and serve. A failed bare mount therefore reports through the same
  synchronous path as a bad `--s3`: the shell sees the errors and a
  non-zero status, and no daemon is left behind.
- `constellation unmount` (last view) and existing SIGTERM/SIGINT handling
  both go through `NodeRuntime::shutdown`; on clean exit remove the PID
  file and the socket, and release `daemon.lock`.
- `--foreground`/`-f` skips all of this and behaves exactly like `mount`
  does today (useful for the harness and debugging — the harness's
  `Client` in `crates/harness/src/client.rs` should keep using
  `--foreground` so it retains direct process-lifetime control).

## Step 6 — `fs create` and `export`

`FsCommand::Create` (`main.rs:348-...`) gets a mandatory leading positional
`name: String` alongside the existing `--s3` and format options; on success
it calls `Registry::merge_and_save(name, { s3, ..no mounts })`.

New top-level `Command::Export { name: String, #[arg(long)] force: bool }`
— the only way to remove a name, a one-shot teardown. **Order matters**:
self-leave is not an offline operation. `DaemonStatus::leave`
(`main.rs:3467-3491`) drives `SyncRequest::Leave` through the running sync
task to drain the journal and release leases, then detaches the mounts. So
leave happens *while the daemon is still up*, not after it:

1. Look up `name` in the registry; error if absent.
2. If no `node_id` was ever claimed for the entry's state dir (no
   `meta.db`, or `kv_get("node_id")` unset — registered via `fs create` but
   never mounted), skip to step 5. There is no cluster state and no daemon.
3. If the daemon is reachable (control socket alive): send `Request::Leave
   { node_id: None, force }`. That drains the shipper, releases leases,
   tombstones the node's registry record, and — per the Step 1 change —
   detaches every mounted view. Wait for the socket to close, bounded by a
   timeout; `--force` skips waiting and proceeds regardless (documented as
   "the local state may be left mid-shutdown; safe because leases/replica
   recovery on next mount handles it", not a silent data-loss shortcut).
4. If the daemon is *not* reachable but a `node_id` is claimed, the node
   left ungracefully or was never cleanly stopped. Retire it from the
   cluster registry with the admin path — the same `leave::admin_leave` the
   `--node-id` form uses (`main.rs:3454-3466`), run directly against the
   store rather than through a socket. There is no journal to drain from
   here: an unshipped tail in the dead daemon's spool is exactly the crash
   case that lease expiry + replica recovery already covers. Say so in the
   command's `--help` and refuse without `--force` if `daemon.lock` is
   still held by a live process (that means the daemon is starting up or
   wedged, not gone).
5. Delete the state dir (`meta.db`, cache, socket, PID/lock files). This is
   required, not optional: the state dir is name-keyed now, so leaving it
   behind means a later `mount myfs` reopens a replica whose `kv left=1`
   makes mount bail (`main.rs:885`) telling the user to pass a fresh
   `--state-dir` — for a name they just exported.
6. `Registry::remove(name)`.

This fully replaces the `forget` idea: a never-mounted entry is handled by
step 2's early exit, and a fully torn-down entry is handled by steps 3-6 in
one command — there is no state left that a separate lightweight command
would still need to clean up.

## Step 7 — `fs list`

New `FsCommand::List` (next to `fs create`/`fs passwd`): print every
registry entry — NAME, S3, STATE-DIR, and for each view: SUBTREE,
MOUNTPOINT, MOUNTED? (probe socket + `MountList`). Analogous to
`zfs list`/`zpool list`.

## Call-site sweep

Breaking changes are fine, but they must actually be swept. `fs create`
gaining a mandatory positional and `mount` daemonizing by default touch
every place that drives the binary:

- `tests/lib.sh:24-26` (`fs_create` — needs a name) and `tests/lib.sh:29-41`
  (`fs_mount` backgrounds with `&` and tracks `MOUNT_PID`; a self-daemonizing
  mount makes that PID exit immediately and the `mountpoint -q` poll
  race-prone). Either pass `--foreground` there or set
  `CONSTELLATION_NO_DAEMONIZE`; `fs_unmount` should move to
  `constellation unmount`.
- `mount.sh` / `umount.sh` at the repo root.
- `crates/harness/src/client.rs` (`mount` at :201, `unmount` at :335,
  and the wrong-passphrase check at :182) — stays on `--foreground` per
  Step 5, but `fs create` and any `StatusReport.mountpoint` assertion still
  need updating.
- `README.md`, `docs/reference/configuration.md`, and any DESIGN.md-adjacent
  how-to that shows `mount --s3 ... /mnt`.

## Config knobs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_REGISTRY` | `$XDG_CONFIG_HOME/constellation/registry.toml` | registry file path |
| `CONSTELLATION_NO_DAEMONIZE` | unset | force `--foreground` behavior for every `mount` in this environment (CI/harness convenience) |

## Tests

Unit:
- Registry: merge-and-save overwrite semantics, node-level vs per-view key
  placement, `--s3` contradiction refused against a populated state dir,
  `remove`, TOML round-trip, concurrent-writer lock.
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
- View-agnostic control ops: `pin /data` applied while `/data` is mounted
  as a subtree view, as a root view, as both, and as neither — same
  resulting pin set every time.
- `MountAdd` against a running daemon from a second CLI invocation actually
  avoids claiming a second `node_id`.

Harness (`crates/harness/src/scenarios.rs`):
- New scenario: `mount myfs /mnt --s3 ...`, then `mount myfs:/sub /mnt2`
  from a second CLI call, assert one process (one `node_id`, one PID file),
  both mountpoints usable; `unmount myfs:/sub`, assert `/mnt` keeps working
  and the daemon is still alive; `unmount myfs`, assert clean exit and PID
  file removed.
- Concurrent daemon start: two `mount myfs` invocations racing from a cold
  state dir produce exactly one daemon and one `node_id`, and the loser
  attaches instead of failing.
- Stale-socket takeover: SIGKILL the daemon, leave `control.sock` and
  `daemon.pid` behind, then `mount myfs` again — it takes the lock,
  unlinks the corpses, and comes up.
- Failed bare mount rolls back: two registered views, one mountpoint made
  unmountable; assert non-zero exit, the failure named on stderr, *neither*
  mountpoint left mounted, and no daemon process or PID file surviving.
  Then the attach variant: `mount myfs:/a /mnt-a` succeeds first, a
  subsequent bare `mount myfs` fails on `/mnt-b` — assert `/mnt-a` is still
  mounted and serving and the daemon is still alive, i.e. rollback touched
  only what the second invocation added.
- Daemonize crash-reporting path: bad `--s3` on first mount exits
  non-zero *synchronously* (parent waits on the pipe) rather than
  backgrounding silently; plus the child-dies-without-writing variant
  (parent must report failure, not hang).
- `export myfs`: mounted-with-two-views case (leaves the cluster, unmounts
  both, daemon exits, state dir gone, registry row gone, node tombstoned in
  the cluster registry); the dead-daemon case (SIGKILL first, then
  `export` — admin retire path runs, state dir and row gone); the
  never-mounted case (`fs create` only, then `export` — registry row gone,
  no cluster call attempted, assert on a mock/spy that no leave logic was
  invoked); and remount-after-export (`fs create` + `mount` the same name
  again succeeds, i.e. no `left=1` corpse survived).
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
2. Step 1 (control socket verbs, multi-view `StatusReport`, multi-view
   `leave`) + multi-view `add_mount`/`remove_mount`, proven by the
   in-process multi-view test above. Still no CLI/registry changes yet —
   reachable only via direct `NodeRuntime` calls in tests.
3. Step 5 (daemonization) plus the state-dir lock from Step 4, in
   isolation, only once Step 1 is solid.
4. Steps 2-4, 6-7 (registry + CLI naming + `fs create`/`export`/`fs list`) —
   the user-facing layer, now with a real daemon underneath it.
5. Call-site sweep + harness scenarios + docs (`README.md`,
   `docs/reference/configuration.md`, a new
   `docs/reference/features/named-filesystems.md`).

## Gates + report

Per `docs/plans/v1/CONVENTIONS.md`, plus:
- Explicit confirmation (paste output) that a two-view mount (`mount myfs`
  then `mount myfs:/sub`) results in exactly one OS process and one
  `node_id`.
- Explicit confirmation that a bare mount with one unmountable view exits
  non-zero and leaves nothing mounted and no daemon running.
- pjdfstest FULL pass with the mount under test daemonized (not
  `--foreground`).
- `docs/plans/v1/PROGRESS.md` note resolving the phase-6a deferral cited
  above — record that the daemon-sharing model is now implemented and why
  (naming UX, not correctness) it was worth doing, plus the breaking
  changes this plan makes (CLI shapes, `StatusReport`, state-dir layout).
