# prollybench

Plan 28 Step 0: the three measurements that decide whether an S3-native
prolly-tree metadata plane (one versioned Merkle map instead of replica +
op log + checkpoint) is worth building.

Standalone, like `bench/dbbench`: excluded from the workspace, own
lockfile, no product code touched. It depends on `constellation-meta`
only to size today's shipped `LogRecord` segments against plan 28's
commits, so the "bytes per op today" column is the real encoding
(postcard envelope, zstd level 3) rather than a re-implementation.

## What it implements

A working prolly tree, not a mock: `node.rs` (explicit node format,
offset table, keys-only boundary function), `tree.rs` (bulk build, point
read, ordered cursor, incremental commit, structural diff, three-way
merge, augmented aggregates), `keys.rs` (the plan's §P6 key encoding),
`store.rs` (three residency tiers, pack writer, reachability sweep, pack
compactor), `corpus.rs` (a census-shaped synthetic namespace).

The `0x02` dentry value has three shapes, selected by `--enc` (or
`--no-attr-copy`) and honoured by every subcommand: `copy` is §P6 as
written, `nocopy` drops the attr copy, and `dentry-auth` makes the dentry
authoritative for `nlink == 1` so no `0x01` record exists. `b06.rs` is the
S1 measurement that prices all three against each other.

The correctness properties the plan leans on are unit tests, not prose:
insertion order does not change the root hash, an incrementally applied
tree is byte-identical to a bulk-built one, delete-then-reinsert returns
the original hash, diff is O(difference), disjoint branches merge to one
hash, and overlapping ones produce the exact conflict set.

```bash
cargo test --release
cargo run --release -- --help
```

## Running the measurements

Global flags come before the subcommand.

```bash
# Everything: 0.1, 0.2, 0.3, then the steady-state run (default 20 min).
cargo run --release -- --entries 11900000 --minutes 20 --out RESULTS.md

# Individual sections.
cargo run --release -- --entries 11900000 --samples 300000 reads
cargo run --release -- --entries 11900000 commits          # includes aged-tree §0.2
cargo run --release -- diff
cargo run --release -- --entries 11900000 --samples 400000 threads  # §0.5
cargo run --release -- --entries 11900000 --minutes 20 --gc-every 10 sustained

# S1 — the three `0x02` value shapes measured against each other.
cargo run --release -- --entries 11900000 --pack-dir /tmp/pb-s1 attr-copy --dirs 32

# Any section under one chosen shape (default `copy`, i.e. §P6 as written).
cargo run --release -- --no-attr-copy --entries 11900000 commits
cargo run --release -- --enc dentry-auth --entries 11900000 reads

# Appendix B shapes: one directory with 10M entries, in its own process.
cargo run --release -- big-dir --entries 10000000

# Clip the geometric node-size tail (canonicality is preserved; see node.rs).
cargo run --release -- --entries 11900000 --max-entries 256 reads
```

`--pack-dir` holds the exported packs (~1.2 GiB at census scale for the
read tiers, ~2 GiB during the steady-state run). Prefer a path *outside*
any syncthing/rsync tree (e.g. `/tmp/prollybench-packs`) — compaction
rewrites thousands of pack files and will pin a file-sync daemon to a
core if it is watching.

Memory: `reads` / `threads` / `commits` build the census tree in RAM for
tier (a) and peak near 12 GiB. `sustained` keeps leaves in packs on disk
behind `--leaf-cache` MiB and stays under 2 GiB — but only if the garbage
between GC rounds is bounded, so keep `--gc-every` around 10 at
`--commit-ops 10000`. A larger `--gc-every` at census scale will exhaust
a 64 GiB host.

Results, and the reading of them, are in
`docs/plans/v1/done/28-s3-native-metadata-store.md` §14.
