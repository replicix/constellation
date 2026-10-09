# Quickstart: create, mount, operate, unmount

A tour of the commands you will use most, in the order you will use them.
Every command here was run against a build of this repository. It needs FUSE
(`fusermount3`) on the host.

The examples use `constellation` for the binary (`target/release/constellation`
after `cargo build -p constellation --release`) and a filesystem called `demo`.
The name is yours to choose; it is a local handle (like a `zpool` name) and
every later command must use the same one. To follow along without S3, use a
local directory as the backend: `--s3 /var/tmp/demo-bucket` (single node only).

## 1. Create

Check that the backend supports conditional writes, then register and create
the filesystem. Do this once per bucket prefix, on an empty prefix.

```bash
export AWS_PROFILE=my-profile            # or AWS_ACCESS_KEY_ID/... or an instance role
BUCKET=s3://my-bucket/constellation-demo

constellation doctor demo --s3 "$BUCKET"        # PUT/GET/LIST/If-None-Match/If-Match
constellation fs create demo --s3 "$BUCKET"
constellation fs list                           # every registered filesystem and its views
```

Useful `fs create` options: `--max-size 10G` (logical size cap),
`--compression raw|zstd|zstd:LEVEL`, `--chunk-size`, and `--e2e` (encrypt
content and filenames; prompts for a passphrase). For a custom S3 endpoint see
[use a custom S3 endpoint](../how-to-guides/operations/use-custom-s3-endpoint.md).

## 2. Mount

```bash
mkdir -p /mnt/demo
constellation mount demo /mnt/demo              # backgrounds itself
```

Variants:

```bash
constellation mount demo:/projects /mnt/projects      # just a subtree
constellation mount demo /mnt/demo --cache-size 20G   # bigger local chunk cache
constellation mount demo /mnt/demo --web-ui           # + web UI, see §5
constellation mount demo /mnt/demo -f                 # stay in the foreground
```

A bare `constellation mount demo` re-mounts every view already registered for
the name. One daemon per filesystem serves all of its views.

Use `/mnt/demo` like any directory:

```bash
mkdir /mnt/demo/projects && echo hello > /mnt/demo/projects/a.txt
```

## 3. Look at it

```bash
constellation status demo              # JSON: mounts, spool backlog, cache, leases, peers
constellation status demo | jq .spool  # pick one section
constellation quota get demo           # logical bytes used / cap
constellation inspect demo:/projects/a.txt   # one file or directory's inode, size, mode, times
constellation cache stat demo          # local chunk cache
constellation log tail demo --lines 50       # recent daemon log
```

## 4. Change settings on a live mount

```bash
constellation quota set demo 50G                 # or `unlimited`
constellation quota set demo 5G --subtree /projects   # cap one directory
constellation write-mode demo back               # close() returns once journaled locally
constellation write-mode demo through            # close() waits for S3; drains the queue first
constellation pin demo:/projects                 # keep a subtree fully cached
constellation pins demo                          # list pins
constellation unpin demo:/projects
constellation cache prune demo --target-bytes 1G # drop clean cached chunks
```

Mount-time options (`--atime`, `--cto`, `--fsync-mode`, ...) are listed in the
[configuration reference](../reference/configuration.md).

## 5. Web UI

Start the mount with `--web-ui` (port 8080) or `--web-ui 9090`, then open
`http://127.0.0.1:8080/`. It serves on localhost only and shows the same data
as `constellation status`.

```bash
constellation mount demo /mnt/demo --web-ui 9090
```

## 6. Snapshots and clones

Snapshots are immutable and name a directory: `fs:/path@name`.

```bash
constellation snapshot create demo:/projects@before-refactor
constellation snapshot ls demo                  # name, created, origin, used, expires
constellation snapshot space demo               # where the space goes
constellation snapshot hold demo:/projects@before-refactor      # never expire or delete
constellation snapshot release demo:/projects@before-refactor
```

Read a snapshot (read-only), or branch a writable copy from it:

```bash
constellation mount demo:/projects@before-refactor /mnt/snap     # read-only view
constellation clone demo:/projects@before-refactor /projects-fix # writable subtree
```

Take them automatically with a policy (`interval:keep`, here hourly for a day
then daily for 30 days). Check it first, then set it:

```bash
constellation snapshot policy check '1h:1d 1d:30d'
constellation snapshot policy set demo:/projects '1h:1d 1d:30d'
constellation snapshot policy ls demo
constellation snapshot sched status demo
constellation snapshot policy rm demo:/projects     # its snapshots are kept
```

Delete when done: `constellation snapshot delete demo:/projects@before-refactor`
(`--dry-run` first; held snapshots need `--force`). More in
[snapshot policies](../reference/features/snapshot-policies.md).

## 7. Maintenance

```bash
constellation fsck demo --s3 "$BUCKET"          # check bucket, replica and cache consistency
constellation gc verify demo --s3 "$BUCKET"     # what garbage collection would delete
constellation gc run demo --s3 "$BUCKET"        # collect it
```

## 8. Unmount and clean up

```bash
constellation umount demo              # every view; the daemon exits with the last one
constellation umount demo:/projects    # or just one view
```

`umount` keeps the registration and the local state, so `mount demo` brings it
back. To remove the filesystem from this machine (leave the cluster, detach
all views, delete the local state and the name), use:

```bash
constellation export demo
```

This does not delete anything in the bucket.

## Next

- [Named filesystems](../reference/features/named-filesystems.md): registry, state directories, ad-hoc mounts
- [Configuration reference](../reference/configuration.md)
- [Kubernetes CSI](../how-to-guides/kubernetes-csi.md)
