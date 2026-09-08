# Plan 17 — Phase 8f: extended attributes

Read `docs/plans/v1/CONVENTIONS.md` first. Prerequisites: plan 16 (8e)
committed (same FUSE files). Spec: `docs/DESIGN.md` §12
(`user.constellation.rsize` xattr; unix metadata). Do not edit
DESIGN.md.

## POSIX xattr

FUSE: `getxattr`, `setxattr`, `listxattr`, `removexattr`. Lease-gate
mutating ops.

Persist in the replica, journaled in the same transaction as today:

- Table `xattr(ino, name, value BLOB, PRIMARY KEY(ino, name))`.
- Log records `set_xattr {ino, name, value, time_ns}` and
  `remove_xattr {ino, name, time_ns}` (additive serde variants).
- Replay upserts/deletes the table. Unlink/rmdir of the inode
  drops its xattrs. Clone copies xattrs with the inode (eager
  clone already copies inode rows — copy xattr rows too).
- Snapshot tree blobs: include xattrs on file/dir entries if the
  tree format can take an optional field without breaking old
  blobs (version the tree header or treat missing as empty). If
  that fights the frozen encoder too hard, persist xattrs only on
  the live replica for this phase and **say so in PROGRESS.md** —
  frozen views would not show them until a follow-up.

Limits: name ≤ 255 bytes, value ≤ 64 KiB (`ERANGE`/`E2BIG` as
Linux does). `XATTR_CREATE` / `XATTR_REPLACE` flags.

Namespaces: implement `user.*` fully. `trusted.*` only for
CAP_SYS_ADMIN if `Request` lets you see the caller (otherwise
refuse with `EPERM`). Skip `security.*` / `system.*` unless
trivial (`ENOTSUP`).

## Virtual constellation attributes

DESIGN §12: `du -sh` is one row. For this phase:

- `user.constellation.rsize` and `user.constellation.rcount` are
  **read-only** virtual xattrs (set/remove → `EPERM`).
- Maintain `inode.rsize` / `inode.rcount` (logical bytes and
  descendant file count) on namespace mutations if the update
  sites are localized; otherwise compute with a bounded SQL
  subtree query and document the cost. Prefer maintained columns
  matching DESIGN (`rsize, rcount` on dirs).
- Values are decimal ASCII bytes (no NUL). Files: rsize = size,
  rcount = 1.

Do not store these in the `xattr` table.

## Tests

Unit: set/get/list/remove; create vs replace flags; unlink drops
rows; journal+replay; virtual rsize on a small tree; user xattr
survives remount (in-process sqlite reopen).

Harness `xattr-roundtrip`: set `user.foo` on a file and a dir;
second node after sync sees it; remove; rsize on a dir with two
files matches sum of sizes (logical, holes count in `st_size` /
rsize the same way `du` would — document whether holes are
included: **logical file_len**, so a 1 GiB holey file contributes
1 GiB to rsize).

pjdfstest 8798/8798 unchanged.

## Out of scope

- ACL xattrs, selinux, capabilities.
- Seeking/fallocate (8e).
- DESIGN.md edits.

## Gates + report

Per CONVENTIONS.md. Phase 8f table in `docs/PROGRESS.md`. Note
whether snapshot views expose xattrs.
