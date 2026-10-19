# Named filesystems, the local registry, and the shared mount daemon

Plan 21 makes filesystems nameable and durable across invocations, the way
`zpool`/`zfs` operate on a pool name resolved through a local import cache,
instead of by raw `--s3 URL` or `--state-dir DIR` every time.

## Table of Contents

- [The registry](#the-registry)
- [Naming syntax](#naming-syntax)
- [`mount` and `umount`](#mount-and-umount)
- [One daemon per name](#one-daemon-per-name)
- [Daemonization](#daemonization)
- [`export`](#export)
- [Ad-hoc, unregistered mounts](#ad-hoc-unregistered-mounts)
- [Config knobs](#config-knobs)

## The registry

A per-user TOML file, `$XDG_CONFIG_HOME/constellation/registry.toml`
(`CONSTELLATION_REGISTRY` overrides the path). Node-level settings
(backend, cache size, write mode, ...) live under `[NAME]`; per-view
settings (subtree, mountpoint, `--allow-other`, ...) live in
`[[NAME.mounts]]` rows, since two mountpoints of the same filesystem
share one node identity, cache, and lease keeper but can genuinely
disagree on those.

`mount` writes a name's live entry — give it new arguments and it
overwrites the stored config for that name/view, the same way `zpool
import` refreshes `zpool.cache`; `fs create NAME` (and the control
protocol's `fs.create`/`fs.import`) register one too. `export` (below) is
the only way to remove one. There is no separate `register`/`unregister`
verb. The CLI and the control service go through one implementation of
these rules, the filesystem manager (`constellation_engine::manager`).

A name is one path component: not empty, not `.` or `..`, and without
`/`, `\`, `:` or control characters (its default state dir is
`<data dir>/NAME`, which `export` deletes; `NAME:/path` separates a name
from a path). A name registered before this rule existed that breaks it
can still be listed (`fs list`) and exported (`export`), but no longer
mounted or otherwise written: export it and register a valid name.

Identity is pinned to the name. A name's backend URL changes — by
`mount NAME --s3 URL`, `fs create NAME --s3 URL` or `fs.create` — only
while its state dir is not in use: no replica (`meta.db`) and no daemon
holding its `daemon.lock` — which `mount NAME` takes as it registers the
name, before it reads `meta.json`, prompts or forks, and hands to its
daemon. A name registered by `fs create` and never
mounted can therefore be created again elsewhere; one in use needs
`export NAME` first. `fs.import` refuses any other URL for a registered
name.

`fs create` refuses a URL where a filesystem already exists, before the
backend preflight or the passphrase prompt; the control protocol's
`fs.create` answers the existing filesystem when the parameters match
(plan 31 §9.2).

## Naming syntax

A bare token before `:` with no `/` in it is a filesystem name; the path
or selector follows. A leading `/` is never reinterpreted as a name, so a
literal path passed alongside an explicit `--state-dir` still means what
it says.

```
pin myfs             # whole filesystem (root)
pin myfs:/data        # just /data
pin /data --state-dir DIR   # unregistered, ad-hoc mount
```

Most control commands (`pin`, `unpin`, `offline`, `online`, `inspect`,
`status`, `leave`, `write-mode`, `quota get`/`quota set`, `cache`,
`log tail`, `snapshot`, `clone`, `debug snap-refs`) take a name this
way. `quota` is always whole-filesystem, node-level, like `write-mode`
— there is no per-subtree cap. `--state-dir`/`--s3` remain explicit
escape hatches for an ad-hoc, unregistered target.

## `mount` and `umount`

```
constellation mount TARGET [MOUNTPOINT] [--s3 URL] [--state-dir DIR] [--foreground]
constellation umount TARGET [--no-wait]
```

`TARGET` is a registered name (`myfs`), a name with a subtree/snapshot
selector (`myfs:/data`), or — only together with `--s3`/`--state-dir` — a
literal path/selector for an ad-hoc, unregistered mount (see below). It's
the same argument shape for both commands; `umount` just never takes a
`MOUNTPOINT`, since a view is already uniquely identified by name(:subtree).

- `mount NAME:/sub MOUNTPOINT` mounts (or updates) exactly that one view.
- `mount NAME[:/sub]` with no mountpoint reuses that view's stored
  mountpoint (an error if it was never registered).
- A bare `mount NAME` (no subtree, no mountpoint) brings up *every* view
  currently registered for that name, all-or-nothing: if any view fails
  to attach, every view *this invocation* added is rolled back, every
  failure is printed on stderr, and the process exits non-zero. Views
  that were already up before this invocation (served by an
  already-running daemon this one merely attached to) are never touched
  by that rollback.
- `umount NAME:/sub` detaches one view; bare `umount NAME` detaches
  every view the daemon currently has mounted. The daemon exits once its
  last view is gone. See [what `umount` waits for](#what-umount-waits-for).

### What `umount` waits for

Each view is detached from the kernel and torn down before `umount`
moves on. A view still in use — files open on it, a shell's working
directory in it — leaves the mount table at once (an unprivileged
unmount is lazy) but keeps serving those until the last is closed;
`umount` says so and waits for it, since only then can the daemon
drain. When that was the daemon's last view, the daemon runs its
clean-unmount drain — every pending write-back upload into the backend,
the journal shipped, then a final metadata commit published and its
leases released — and exits. By default `umount` waits for that too,
printing the drain's progress every few seconds:

```
unmounted /mnt/myfs; that was the daemon's last view: it drains and exits
waiting for the daemon's clean-unmount drain: 3233 uploads (6.6GB) and 3171390 journal records left (5s)…
…
drain complete: every pending upload is in the backend and the journal is shipped; the daemon has exited
```

A drain is clean once every upload is in the backend and the journal is
shipped. A final commit publish that stalls after that (an S3 path too
slow for it) is given up, and the drain still counts as clean: the
shipped journal holds everything, and the next lease holder publishes
from it. When `umount` returns successfully, what the mount wrote is in
the backend (under `--write-mode back` as well) and the daemon process
is gone: a
crash or `kill -9` of anything afterwards loses none of it, and a
`mount` right after starts a new daemon at once. There is no time
limit: the drain ends by itself once it stops making progress
(`CONSTELLATION_SHUTDOWN_STALL_S`, S3 unreachable), and `umount` then
fails, quoting what the daemon left behind (it stays in the state dir
and ships at the next mount). It fails the same way if the daemon dies
during the drain.

- **Ctrl-C** stops the waiting, not the drain: the daemon drains on in
  the background and exits when it is done (`umount` says so and exits
  non-zero).
- **`--no-wait`** returns once the views are detached and leaves the
  drain to the daemon (`constellation status NAME` shows how far it
  got). Nothing is lost by not waiting — what has not shipped is on the
  node's disk and ships at the next mount — but it is not in the backend
  yet.
- **`umount NAME:/sub`** of a view that is not the last one returns once
  the view is detached; the daemon keeps serving the others. A
  persistent daemon (`constellation serve`) keeps running after its last
  view, and `umount` says so instead of waiting for an exit.
- **`umount NAME` again** while the daemon drains (after `--no-wait` or
  a Ctrl-C) waits for the drain like the first one would have.
- A signal (`SIGTERM`, `SIGINT`) unmounts every view at once; one still
  in use keeps the daemon up until its last file is closed.

A `mount` of the name issued while its daemon drains does not attach to
it: a daemon past the start of its drain has stopped its engine and
cannot serve a view again. The `mount` waits for that daemon to exit
(printing the drain's progress) and then starts the next daemon, which
takes up whatever the drain left. That wait has no time limit either;
only a draining daemon that stops answering for
`CONSTELLATION_ATTACH_TIMEOUT_MS` fails the `mount`. Before this, the
`mount` waited silently (its log named the phase "attaching views to the
running daemon") and gave up after 600 s, and `umount` usually returned
before the drain had even started, inviting exactly that remount.

`--s3`, `--cache-size`, `--write-mode`, and the other flags below become
registry overrides: give one explicitly and it replaces the stored
value; omit it and the stored value (or a fresh default) is used. `--s3`
is refused if it would repoint a name whose state dir is already
populated — identity is pinned to the name; run `export` first to really
repoint it.

A name's state dir is `$XDG_DATA_HOME/constellation/NAME`
(`~/.local/share/constellation/NAME`) unless `mount` is given
`--state-dir DIR`: DIR then becomes the name's state dir, stored like
every other override, so `status NAME`, `umount NAME` and `export NAME`
find it there (`export` deletes it). DIR must be new or an empty
directory (`export` deletes it with everything in it, so `--state-dir .`
in your home directory is refused), and not another name's. The same
identity rule as for `--s3` refuses a `--state-dir` that would move a
name whose state dir is in use: populated, or held by a daemon still
starting up there.

## One daemon per name

One process serves every mounted view of a name — they share the
metadata replica, the chunk cache, the lease keeper, and the P2P
endpoint. `mount myfs /mnt` followed by `mount myfs:/sub /mnt2` from a
*second* CLI invocation results in exactly one OS process and one
`node_id`: the second invocation takes a per-name `daemon.lock`, finds it
already held, and sends `MountAdd` over the control socket instead of
starting a new process.

A held `daemon.lock` is not by itself proof of a daemon that will answer:
the lock and the control socket belong to the holder's file table, which
outlives `kill -9` for as long as any thread of that process is still in
the kernel (EC2 campaign 6, finding B-1: a killed lease holder lingered
as a zombie with one thread stuck, its listener accepting connections
nobody served). So the second invocation first pings the holder
(`CONSTELLATION_CONTROL_TIMEOUT_MS`, default 10 s) and attaches only to
a daemon that answers; a holder that does not answer is classified from
`/proc`:

- alive (running, sleeping, stopped, or a zombie leader whose other
  threads can still run): never taken over — the mount keeps trying
  until `CONSTELLATION_ATTACH_TIMEOUT_MS` (default 120 s; a daemon still
  bootstrapping has no socket yet) and then fails, naming the pid and
  its state;
- killed by the kernel (thread-group leader a zombie and SIGKILL
  pending, or every remaining thread past `exit_mm`): taken over — its
  lock file is moved to `daemon.lock.wedged-<pid>` (the zombie keeps its
  lock on that inode), the stale `control.sock` and `daemon.pid` are
  removed, and the mount becomes the daemon on a fresh lock.

`constellation status` is bounded by the same control timeout. The
daemon logs each startup phase with its duration, and a
`startup-watchdog` thread warns every `CONSTELLATION_STARTUP_WARN_S`
(30) that a phase is still running, so a stuck startup names its phase
in `daemon.log` (a `log lines dropped` line in it, or a `log.longest_write_ms`
in seconds in `constellation status`, means the disk under the log stalled;
see [the daemon's log never blocks it](#the-daemons-log-never-blocks-it)).

## Daemonization

`mount` backgrounds itself by default (fork + `setsid`, matching
JuiceFS): the parent blocks on a status pipe until the child reports
either "every requested view attached" or a specific error, then exits
with that verdict — a bad `--s3`, a bind conflict, or an unmountable
mountpoint still surfaces synchronously to the shell instead of quietly
leaving a dead background process. Before its verdict the child may
report progress — a `mount` waiting for the previous daemon's drain says
how far it is — which the parent prints on stderr. `-f`/`--foreground`
opts out (used by
the test harness and interactive debugging); `CONSTELLATION_NO_DAEMONIZE`
forces foreground behavior for every `mount` in an environment (CI
convenience). A daemonized mount's PID file and log live at
`<state-dir>/daemon.pid` and `<state-dir>/daemon.log`.

### The daemon's log never blocks it

A daemon (`mount`, `serve`, a resumed `daemon --resume-from`) does not wait
for `daemon.log`: each log line goes into a queue (16 MiB by default,
`CONSTELLATION_LOG_QUEUE_BYTES`) and one writer thread appends the queue to
the file in batches. A disk that stalls (a buffered write to a busy btrfs or
a loaded network volume can wait seconds) stalls that thread alone; the
authority core, the FUSE workers and the lease heartbeat keep running.
Behind a stall longer than the queue holds, the *incoming* line is dropped
(the last eighth of the queue is kept for `WARN` and `ERROR` lines), and
once the file is writable again one line marks the gap:

```text
2026-10-18T10:15:42.123456Z  WARN constellation::log: log lines dropped: the log sink did not keep up dropped_lines=1834 dropped_bytes=402117 first_dropped_unix_ms=1792318530000 queue_limit_bytes=16777216
```

`constellation status` has a `log` section: `dropped_lines` (nonzero: the
file is missing lines, the daemon was not slowed), `longest_write_ms` (the
longest single write to the file, in seconds when the disk under it stalled,
whether or not lines were dropped), `writing_for_ms` (the write in progress
now, growing without end when the file's disk is wedged), `queued_bytes` and
`queued_bytes_max`, and `sink_errors` (writes that failed: a full disk).
`constellation log tail` reads the daemon's in-memory ring of the last 2 000
lines, which has every line whether or not the file got it. What was queued
is written before a clean exit and before the handover's `exec` (an upgrade
drains the log, up to 2 s, before it detaches the views; what is still unwritten
at the `exec` goes to the new image in a memfd and is the first thing it writes;
only what cannot be carried is counted as dropped and says so in a `log lines
dropped` line); a write to the file that fails (a full disk) drops its chunk,
counted in `dropped_lines` and `sink_errors`; a `kill -9` loses the lines of the
last moments (behind a stalled disk, the backlog). A
`log` section with `mode: "sync"` means the queue is off
(`CONSTELLATION_LOG_QUEUE_BYTES=0`, for measurements only) and a slow disk
does stall the daemon.

Host guidance that still helps, with fewer drops as the only effect:
`lazytime`/`noatime` on the state directory's file system, and the state
directory (or at least `daemon.log`) on a device that does not stall.

## `export`

The only way to remove a registered name:

```
constellation export NAME [--force]
```

1. If the name was never mounted (`fs create` only), just drops the
   registry row.
2. If the daemon is reachable, sends `Leave` — drains the journal,
   releases leases, tombstones the node's cluster registry record, and
   detaches every view — then waits for the socket to close.
3. If the daemon is unreachable but a `node_id` was claimed, retires it
   from the cluster registry directly (the admin-leave path); refuses if
   `daemon.lock` is still held by a live process unless `--force`.
4. Deletes the state dir and the registry row.

Before any of this, `export` refuses a name whose state dir holds (or is)
another registered name's state or cache dir, the registry, or the data,
config or runtime dir, and refuses when one of those host dirs cannot be
located — a row written before names were checked (`mount ..` registered
`..`) must not take every other name's state with it.

The state dir must go: it is name-keyed now, so leaving it behind would
make a later `mount NAME` reopen a replica that already recorded
"left the cluster" and refuse to remount.

## Ad-hoc, unregistered mounts

Passing a literal path/selector as the target (`/`, `/sub`, not a
name) together with an explicit `--s3`/`--state-dir` never touches the
registry — this is the escape hatch for a one-off or scripted mount that
should not become a named, durable entry:

```
constellation mount / /mnt --s3 s3://bucket/prefix --state-dir /tmp/scratch
```

## Config knobs

| Var | Default | Meaning |
|---|---|---|
| `CONSTELLATION_REGISTRY` | `$XDG_CONFIG_HOME/constellation/registry.toml` | registry file path |
| `CONSTELLATION_NO_DAEMONIZE` | unset | force `--foreground` behavior for every `mount` (CI/harness convenience) |

## References

- [`docs/explanation/DESIGN.md`](../../explanation/DESIGN.md) §10 (Control Plane), §13 (Snapshots and Clones)
- [`docs/plans/v1/done/21-fs-registry-and-daemon.md`](../../plans/v1/done/21-fs-registry-and-daemon.md)
