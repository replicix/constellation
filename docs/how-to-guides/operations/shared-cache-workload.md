# Run a shared content-addressed cache workload

Use a scratch directory for node-local temporary output, then publish completed
cache entries into the shared namespace with one rename.

## 1. Create shared and scratch paths

```bash
mkdir -p /mnt/cache/tmp /mnt/cache/objects
setfattr -n user.constellation.scratch -v 1 /mnt/cache/tmp
getfattr -n user.constellation.scratch /mnt/cache/tmp
```

The `tmp` directory exists on every node because its directory inode and xattr
are shared. Its children are node-private.

## 2. Write and publish atomically

Write a complete regular file under `tmp`, close it, then rename it to its
content-addressed destination:

```bash
key=0123456789abcdef
tmp=/mnt/cache/tmp/.${key}.$$
dest=/mnt/cache/objects/$key

generate-cache-entry >"$tmp"
sync "$tmp"                    # optional: use the mount's fsync policy
mv -T "$tmp" "$dest"          # scratch → shared Publish
```

Publish drains the file, reuses identical content-addressed chunks already in
S3, and commits the destination manifest as one shared mutation. Do not try to
rename a scratch directory or move a shared file into scratch; those boundary
operations fail with `EXDEV`.

## 3. Verify forwarding instead of handoff

Run the workload from two nodes, then inspect each node:

```bash
constellation status
journalctl --user -u constellation | grep 'handed the lease to a peer'
```

In the status JSON, `forwarded_ok` should increase and `forwarded_err` should
remain low. The web UI shows the same counters. During steady state, the log
search should return no new `handed the lease to a peer` lines.

## 4. Tune burst behavior if needed

The default holder idle release is 30 seconds. For workloads with pauses longer
than that, keep the lease sticky across the expected gap:

```bash
export CONSTELLATION_LEASE_IDLE_RELEASE_MS=120000
constellation mount --s3 "$BUCKET" /mnt/cache
```

Increase this only when idle release causes reacquisition churn. Placement and
forwarding already handle active writers without requiring a long idle timer.

## Visibility expectations

- A node sees its own scratch entries immediately.
- Other nodes never see those scratch entries, even under the same names.
- The published destination becomes visible after segment push or the next S3
  sync poll.
- Mount startup purges local scratch entries. Scratch is not recovery storage.
- Concurrent Publish to an already-identical destination deduplicates by
  manifest; different content follows normal destination replacement and
  lease ordering.

See [Scratch directories](../../reference/features/scratch-directories.md) and
[Forwarded mutations](../../reference/features/forwarded-mutations.md).
