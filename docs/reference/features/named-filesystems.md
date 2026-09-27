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

`mount` is the only command that *writes* a live entry — give it new
arguments and it overwrites the stored config for that name/view, the
same way `zpool import` refreshes `zpool.cache`. `export` (below) is the
only way to remove one. There is no separate `register`/`unregister`
verb.

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
constellation umount TARGET
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
  last view is gone.

`--s3`, `--cache-size`, `--write-mode`, and the other flags below become
registry overrides: give one explicitly and it replaces the stored
value; omit it and the stored value (or a fresh default) is used. `--s3`
is refused if it would repoint a name whose state dir is already
populated — identity is pinned to the name; run `export` first to really
repoint it.

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
in `daemon.log`.

## Daemonization

`mount` backgrounds itself by default (fork + `setsid`, matching
JuiceFS): the parent blocks on a status pipe until the child reports
either "every requested view attached" or a specific error, then exits
with that verdict — a bad `--s3`, a bind conflict, or an unmountable
mountpoint still surfaces synchronously to the shell instead of quietly
leaving a dead background process. `-f`/`--foreground` opts out (used by
the test harness and interactive debugging); `CONSTELLATION_NO_DAEMONIZE`
forces foreground behavior for every `mount` in an environment (CI
convenience). A daemonized mount's PID file and log live at
`<state-dir>/daemon.pid` and `<state-dir>/daemon.log`.

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

The state dir must go: it is name-keyed now, so leaving it behind would
make a later `mount NAME` reopen a replica that already recorded
"left the cluster" and refuse to remount.

## Ad-hoc, unregistered mounts

Passing a literal path/selector as the target (not a registered name)
together with an explicit `--s3`/`--state-dir` never touches the
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
