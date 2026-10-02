# Scratch directories

Scratch directories are explicit node-private staging trees inside a
Constellation mount. Their contents become shared only through Publish.

## Table of Contents

- [Terminology](#terminology)
- [Marking a directory](#marking-a-directory)
- [Local and shared behavior](#local-and-shared-behavior)
- [Boundary operation matrix](#boundary-operation-matrix)
- [Publish and deduplication](#publish-and-deduplication)
- [Lifecycle and limits](#lifecycle-and-limits)
- [References](#references)

## Terminology

- **Scratch root**: a shared directory whose
  `user.constellation.scratch` xattr is exactly `1`.
- **Scratch entry**: a node-private inode below a scratch root.
- **Publish**: rename of a regular scratch file into a shared directory.

## Marking a directory

Set the xattr on an existing shared directory:

```bash
setfattr -n user.constellation.scratch -v 1 /mnt/constellation/cache/tmp
```

The marker itself is shared metadata. Entries created below it are not.

## Local and shared behavior

Creates, subdirectory creation, writes, closes, renames within the scratch
tree, and unlinks use node-local metadata and the local chunk cache. They do
not require a partition lease and do not emit shared log records. Another
node sees the marked scratch root but not these entries.

## Boundary operation matrix

| Operation | Result |
|---|---|
| scratch → scratch rename | Local rename |
| scratch regular file → shared rename | Publish |
| scratch directory or special file → shared rename | `EXDEV` |
| shared → scratch rename | `EXDEV` |
| shared → shared rename | Normal shared mutation |
| hard link involving scratch | Unsupported across the boundary (`EXDEV`) |
| create/write/unlink below scratch | Local only |

Publish is intentionally rename-only. Copying a file into a shared path is an
ordinary shared write, not promotion of its scratch inode.

## Publish and deduplication

Publish drains the scratch file's dirty chunks, then commits a `Publish`
mutation containing its stable inode number, attributes, manifest, and size.
Chunk keys are content-addressed, so bytes already present in S3 are reused.

If the destination already contains a regular file with an identical
manifest, Publish is treated as already satisfied. No duplicate namespace or
manifest records are appended.

## Lifecycle and limits

- All scratch inode and dentry rows are purged when the mount starts.
- Scratch state is node-private and is not restored after a crash or remount.
- A crash before Publish cannot leave a shared-log orphan.
- Only regular files can be published.
- A scratch root, or anything below one, cannot carry an automatic snapshot
  policy (`user.constellation.snapshots`): snapshots of node-private content
  are meaningless. `setxattr` answers `EINVAL`, and so does marking as
  scratch a directory that carries a policy or has a policy root anywhere
  below it. The refusal's reason is in `node.status` →
  `snapsched.last_parse_error`, recorded against the expression
  `user.constellation.scratch=1` with a message starting "scratch refused".
- Scratch directory trees, symlinks, devices, and hard-link graphs cannot be
  promoted as a unit.

Scratch chunks that reached object storage but were never published are
unreferenced content-addressed objects and are reclaimed by normal bucket GC.

## References

- [Shared-cache workload](../../how-to-guides/operations/shared-cache-workload.md)
- [ADR-16](../../explanation/DECISIONS.md#adr-16-scratch-directories-are-explicit-and-node-private)
- [DESIGN.md: Scratch directories](../../explanation/DESIGN.md#scratch-directories)
