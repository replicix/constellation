# Plan 14 — Phase 8e: fallocate, hole punching, sparse manifests

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plan 13 (8d)
committed. Spec: `docs/DESIGN.md` §3 (manifests, holes), §12
(`fallocate`/`truncate` as metadata-only; `SEEK_HOLE`/`SEEK_DATA`).
Do not edit DESIGN.md.

## Why not a 4 MiB all-zeroes chunk

Today `flush_inode` materializes extension holes as a real zero
buffer, hashes it, and `cache_for_upload`s it (`cli/src/fusefs.rs`
"Hole created by extension without data: a zero chunk"). Dedup would
collapse a 1 TiB sparse file onto **one** S3 object, which is fine
for bytes-in-the-bucket, and GC `deref` is keyed by unique hash, so
refcount cardinality is also fine.

What is **not** fine:

- The manifest (inline or spilled) still stores one 32-byte hash
  **per chunk slot**. 1 TiB / 4 MiB = 262,144 slots ≈ 8 MiB of
  repeated hashes, plus the in-memory `Vec` of the same length on
  every flush/read/pin walk.
- Flush hashes a 4 MiB zero buffer 262k times (CPU) or at least
  allocates it once per slot.
- Pin, prefetch, coop, and `pending_upload` treat that hash as a
  real object: useless GET/PUT/digest traffic for "nothing".
- `SEEK_HOLE` cannot distinguish "user wrote zeroes" from "never
  allocated" if both are the same object (optional: we *want* them
  equivalent for POSIX reads, but SEEK_HOLE should skip both).

**Decision (implement this, do not leave a zero-chunk path):** a
chunk that is all zeroes, or a range that was punched/extended
without data, is a **hole**. Holes are not S3 objects, not cache
entries, not deref rows, not pending uploads.

## Sparse chunk list

In-memory and on disk, stop requiring a dense `Vec` of length
`chunk_count(file_len)`.

- In-memory: keep `file_len` + `chunk_size` + a map (or run-length
  list) of **only present data chunks** `index → ChunkHash`. Missing
  indices in `[0, chunk_count)` are holes. `Manifest` / `ChunkInfo`
  should expose this without forcing callers to allocate 262k
  sentinels. A 1 TiB empty file is `file_len` plus an empty map.
- On disk: spilled blob currently `count u32 + count*32 hashes`
  (dense). Add a compact encoding with a magic prefix so old blobs
  still decode:
  - existing blobs (no magic / length matches dense) keep working;
  - new sparse blobs: e.g. `CLH1` + `n_data u32` + `n_data * (index
    u64 + hash 32)` (or RLE of hole runs — pick one, document it).
  A 1 TiB file with one written chunk must encode in tens of bytes,
  not 8 MiB.
- Inline manifests (≤ `INLINE_CHUNKS_MAX` **data** chunks, not
  slots) stay small; a huge sparse file with 0–8 written chunks
  should stay inline even if `chunk_count` is huge.
- Sentinel: if you need a dense adapter, `ChunkHash::HOLE` (all
  zero bytes) is read-as-zeros and **never** PUT. Prefer not to
  expand to dense except in tests. Do **not** use blake3(zeros) as
  the hole mark — that would still look like a real object.

All walks (GC deref diff, pin, snapshot tree, flush, read) skip
holes: no fetch, no upload, no deref insert for a hole.

All-zero **writes** (e.g. `dd if=/dev/zero`) collapse to holes on
seal/flush, same as punch. Reads still return zeros. That is the
optimization the 4 MiB zero-chunk would have been trying to do,
without the object.

## FUSE `fallocate`

Implement `Filesystem::fallocate` (fuser 0.15). Lease-gated like
other mutations.

Modes (linux `falloc.h`):

- **0 (allocate):** extend `file_len` if `offset+len` is past EOF;
  new space is holes. Metadata-only. Do not write zeroes.
- **`FALLOC_FL_KEEP_SIZE`:** allocate/punch without changing
  `file_len` (punch inside the file only).
- **`FALLOC_FL_PUNCH_HOLE` (must be with KEEP_SIZE on Linux):**
  drop whole-chunk coverage in the range; for a partial chunk,
  RMW the remainder (zeros in the punched part) into a new data
  chunk, or drop the chunk if the remainder is all zeros.
- **`FALLOC_FL_ZERO_RANGE`:** treat as punch (sparse zeros), not
  a written zero object.
- Unimplemented flags (`COLLAPSE`, `INSERT`, `UNSHARE`): `EOPNOTSUPP`.

Wire staging: punching a dirty staged range should `fallocate`
punch the staging file (already used in `release_chunk`) and
clear dirty runs in that range.

`truncate` already extends with holes in staging; flush must use
the sparse representation instead of zero chunks. That is a
**required regression fix**, not optional.

## `SEEK_HOLE` / `SEEK_DATA`

Implement FUSE `lseek` if the fuser version exposes it; otherwise
document the gap. Hole/data offsets come from the sparse map +
`file_len`. Beyond EOF: `ENXIO` per POSIX. Unwritten staging
ranges are holes.

## Tests

Unit (`fs-core::manifest`, `cli` flush/fallocate):

- Empty 1 GiB (or simulated 1 TiB) extend: encoded manifest stays
  tiny; no pending upload; no S3 put in a counting store.
- Punch a middle 4 MiB chunk out of a 3-chunk file; read back
  zeros there; remaining chunks unchanged hashes.
- Partial-chunk punch RMW; remainder not all-zero stays a data
  chunk.
- Writing a full chunk of zeros becomes a hole (no upload).
- Old dense spilled blobs still decode.
- Deref/GC: punching the last reference of a real hash records
  deref; holes never appear in `deref`.

Harness:

- `fallocate-sparse`: `ftruncate`/`fallocate` a file many times
  `--cache-size` (e.g. 256 MiB file, 32 MiB cache) with only a
  few written bytes at the start and end; assert RSS/cache
  ceilings, `SEEK_HOLE`/`SEEK_DATA` (or equivalent reads), and
  a second node sees the same sparse layout (no 64 extra 4 MiB
  objects in the bucket for the hole span).
- Punch + rewrite in the hole; model-verify.

pjdfstest must stay 8798/8798. Do not add exclusions.

## Out of scope

- xattr (plan 15 / 8f).
- Slice overlays, pack, CDC.
- Changing DESIGN.md.

## Gates + report

Per CONVENTIONS.md. Phase 8e table in `docs/PROGRESS.md`. State
clearly: zero-chunk dedup was rejected; holes are first-class;
paste `fallocate-sparse` object-count and size numbers.
