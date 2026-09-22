# Combined Step 0 results (incl. aged + threads)

# prollybench — plan 28 Step 0 results

Host: 32 cores, generated 2026-09-11. Single-threaded unless stated. Node entry clamp: none.

Corpus: 11900000 entries, 1.50 TiB logical bytes, 35819001 keys, generated in 1 s.
Bulk build: 35819001 keys in 17 s (2.13 M/s), 2.75 GiB resident.

### Tree structure at census scale

| Level | Nodes | Entries | Encoded bytes | Mean entries/node | Mean node bytes |
|---|---:|---:|---:|---:|---:|
| 0 (leaf) | 306096 | 35819001 | 2.73 GiB | 117 | 9.35 KiB |
| 1 | 2676 | 306096 | 19.57 MiB | 114 | 7.49 KiB |
| 2 | 31 | 2676 | 179.64 KiB | 86 | 5.79 KiB |
| 3 | 1 | 31 | 2.12 KiB | 31 | 2.12 KiB |

- Keys: **35819001** (11900000 inodes + 11900000 dentries + 11900000 reverse dentries + 119000 spilled xattrs)
- Levels: **4** (3 interior hops + 1 leaf)
- Interior bytes (all non-leaf nodes): **19.75 MiB**; leaf bytes: **2.73 GiB** (zstd ≈ 1.16 GiB at 43%)
- Leaf entries/node: p1 2, p50 81, p99 536, max 1545 (geometric, no clamp — see `node.rs`)
- Aggregates from the root node alone: 11484979 files, 1.50 TiB logical bytes

### 0.2 Commit cost by write shape

Every commit is applied to the same census-scale root, so the numbers are the marginal cost of a batch against a 23.8M-key filesystem.

| Shape | ops | keys written | nodes written | bytes | zstd bytes | packs | B/op (zstd) | today's segment B/op | ratio | CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| clustered (one directory) | 1000 | 3000 | 25 | 348.95 KiB | 146.26 KiB | 1 | 150 | 45 | 3.31× | 3 ms |
| clustered (one directory) | 10000 | 30000 | 260 | 2.36 MiB | 735.91 KiB | 1 | 75 | 45 | 1.67× | 23 ms |
| clustered (one directory) | 100000 | 300000 | 2562 | 22.55 MiB | 6.45 MiB | 7 | 68 | 45 | 1.50× | 187 ms |
| semi-clustered (build tree) | 1000 | 3000 | 68 | 957.13 KiB | 453.84 KiB | 1 | 465 | 46 | 10.11× | 6 ms |
| semi-clustered (build tree) | 10000 | 30000 | 382 | 4.25 MiB | 1.68 MiB | 2 | 176 | 45 | 3.87× | 30 ms |
| semi-clustered (build tree) | 100000 | 300000 | 2662 | 24.85 MiB | 7.69 MiB | 8 | 81 | 45 | 1.78× | 181 ms |
| scattered (random-ino chmod) | 1000 | 2000 | 2940 | 54.04 MiB | 23.66 MiB | 24 | 24811 | 10 | 2459.67× | 303 ms |
| scattered (random-ino chmod) | 10000 | 19996 | 19863 | 409.24 MiB | 152.75 MiB | 153 | 16018 | 9 | 1772.53× | 1678 ms |
| scattered (random-ino chmod) | 100000 | 199158 | 102875 | 1.71 GiB | 640.89 MiB | 641 | 6721 | 9 | 757.84× | 7276 ms |

**Gate 0.2a: clustered ≤ 2× today's log bytes per op → FAIL.** Scattered is documented, not gated.


### 0.2 aged tree — create/unlink/rename generations

Fresh import keeps `0x01` order correlated with directory order (`alloc_ino` is `counter++`). After years of create/unlink/rename the dentry leaves of a directory stay clustered while its inode leaves scatter. Age the corpus with 10 generations, then re-measure both create-shaped clustered commits and directory-local setattr (the shape the aging gate is about).
Aged in 41.5 s (10 × 20k create/rename/unlink ops); tracked live files after aging: 2000000.

| Tree | Shape | ops | keys written | nodes written | bytes | zstd bytes | packs | B/op (zstd) | today's segment B/op | ratio | CPU |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| fresh | clustered create | 1000 | 3000 | 34 | 334.13 KiB | 133.38 KiB | 1 | 137 | 45 | 3.02× | 2 ms |
| fresh | clustered touch (one dir setattr) | 1000 | 1824 | 91 | 1.16 MiB | 473.46 KiB | 1 | 485 | 6 | 83.42× | 6 ms |
| fresh | clustered create | 10000 | 30000 | 257 | 2.37 MiB | 738.18 KiB | 1 | 76 | 45 | 1.68× | 16 ms |
| fresh | clustered touch (one dir setattr) | 10000 | 8708 | 114 | 1.45 MiB | 623.99 KiB | 1 | 64 | 4 | 14.76× | 9 ms |
| fresh | clustered create | 100000 | 300000 | 2669 | 22.53 MiB | 6.46 MiB | 7 | 68 | 45 | 1.51× | 179 ms |
| fresh | clustered touch (one dir setattr) | 100000 | 9988 | 114 | 1.44 MiB | 615.88 KiB | 1 | 6 | 3 | 2.33× | 9 ms |
| aged | clustered create | 1000 | 3000 | 42 | 340.70 KiB | 146.99 KiB | 1 | 151 | 45 | 3.34× | 2 ms |
| aged | clustered touch (one dir setattr) | 1000 | 1200 | 88 | 1.08 MiB | 443.25 KiB | 1 | 454 | 5 | 93.20× | 5 ms |
| aged | clustered create | 10000 | 30000 | 246 | 2.35 MiB | 727.81 KiB | 1 | 75 | 45 | 1.66× | 16 ms |
| aged | clustered touch (one dir setattr) | 10000 | 1736 | 90 | 1.08 MiB | 446.81 KiB | 1 | 46 | 3 | 17.26× | 5 ms |
| aged | clustered create | 100000 | 300000 | 2631 | 22.54 MiB | 6.44 MiB | 7 | 68 | 45 | 1.50× | 180 ms |
| aged | clustered touch (one dir setattr) | 100000 | 1736 | 90 | 1.08 MiB | 446.81 KiB | 1 | 5 | 2 | 2.07× | 6 ms |

**Gate 0.2 aged: clustered ≤ 4× today's log bytes per op → FAIL for directory-local setattr (worst 93.20×), PASS for create (worst 3.34×).** The gate is about setattr: creates keep sequential `alloc_ino` regardless of age.

---

### 0.5 Thread scaling

Immutable content-addressed nodes: a lookup is a pure function of `(root, key)`. Tier (b) uses a 64 MiB leaf cache — the size at which the single-threaded gate failed.

| Threads | (a) lookup/s | (a) scaling | (b) 64 MiB lookup/s | (b) scaling | (b) pack reads/lookup |
|---:|---:|---:|---:|---:|---:|
| 1 | 694 k/s | 1.00× | 51 k/s | 1.00× | 0.94 |
| 2 | 1.23 M/s | 1.77× | 97 k/s | 1.90× | 0.94 |
| 4 | 2.01 M/s | 2.90× | 188 k/s | 3.69× | 0.94 |
| 8 | 2.32 M/s | 3.34× | 326 k/s | 6.41× | 0.94 |
| 16 | 2.29 M/s | 3.31× | 389 k/s | 7.65× | 0.94 |
| 32 | 2.34 M/s | 3.38× | 382 k/s | 7.51× | 0.94 |

**Gate 0.5: tier (b) ≥ 150 k/s aggregate at ≤ 8 threads with a 64 MiB leaf cache → PASS (326 k/s best).**

#### Commit build across shards
A 32k-op clustered batch, applied whole (1 thread) versus split into N key-range shards applied in parallel from the same root and merged left-to-right.

| Shards | wall | ops/s | nodes written | vs 1-thread |
|---:|---:|---:|---:|---:|
| 1 (whole) | 58 ms | 556 k/s | 807 | 1.00× |
| 2 | 79 ms | 406 k/s | 639 | 0.73× |
| 4 | 125 ms | 256 k/s | 655 | 0.46× |
| 8 | 217 ms | 147 k/s | 687 | 0.27× |
| 16 | 344 ms | 93 k/s | 751 | 0.17× |

#### Mark + compaction parallelism
Produce a dirty pack set with commits from a single tip (no retention window — superseded nodes are dead), then mark and compact at 1…32 threads. §14.5 spent 939 of 1,205 s on single-threaded GC; the question is whether that divides by cores.
Dirty tip: 35 whole-dead packs, 1995 partial, 1.04 GiB live / 1.25 GiB dead bytes.

| Threads | mark | mark scaling | compact (≤512 MiB budget) | compact MiB/s | compact scaling |
|---:|---:|---:|---:|---:|---:|
| 1 | 8773.1 ms | 1.00× | 3062 ms / 512.07 MiB | 167.2 | 1.00× |
| 2 | 4630.4 ms | 1.89× | 2618 ms / 512.15 MiB | 195.6 | 1.17× |
| 4 | 2740.1 ms | 3.20× | 1820 ms / 511.82 MiB | 281.2 | 1.68× |
| 8 | 2107.0 ms | 4.16× | 1484 ms / 414.37 MiB | 279.2 | 1.67× |
| 16 | 2262.6 ms | 3.88× | 1608 ms / 410.65 MiB | 255.5 | 1.53× |
| 32 | 2271.7 ms | 3.86× | 2767 ms / 513.20 MiB | 185.4 | 1.11× |

---

### S1 — settling the dentry attr copy

Host: 8 cores, generated 2026-09-12, 11900000 entries. Reproduce with
`prollybench --entries 11900000 --pack-dir /tmp/pb-s1 attr-copy --dirs 32 --generations 10 --age-ops 20000`.
A parallel build was running on the same host, so wall-clock columns carry noise; byte and pack-read counts are deterministic.

Three `0x02` value shapes through the same code paths against the same corpus: `copy` is §P6 as written (ino + a denormalized attr copy), `nocopy` is ino + kind (so `ls -la` is a range scan plus a point read per child into `0x01`), and `dentry-auth` is the plan's escape hatch — for `nlink == 1` the dentry *is* the record, there is no `0x01` key, and `getattr(ino)` hops through `0x04` first. Directories keep their `0x01` record in all three.

- `copy`: built 35819001 keys in 69 s, 2.73 GiB of leaves.
- `nocopy`: built 35819001 keys in 71 s, 2.20 GiB of leaves.
- `dentry-auth`: built 24218019 keys in 46 s, 2.02 GiB of leaves.

#### Footprint at census scale
The copy inflates every replica, not just writes. zstd leaf bytes are every leaf compressed at the pack writer's level, not a sample: the variants change which key range dominates the first leaves, so a sampled ratio would not be comparable.

| Variant | keys | levels | leaves | leaf bytes | zstd leaf bytes | interior bytes | vs `copy` (zstd leaves) |
|---|---:|---:|---:|---:|---:|---:|---:|
| copy | 35819001 | 4 | 306096 | 2.73 GiB | 1.00 GiB | 19.75 MiB | +0.0% |
| nocopy | 35819001 | 4 | 306096 | 2.20 GiB | 914.22 MiB | 19.75 MiB | -10.9% |
| dentry-auth | 24218019 | 4 | 207159 | 2.02 GiB | 886.77 MiB | 14.87 MiB | -13.6% |

#### `setattr` bytes per op
The §14.4 and §14.8 rows recomputed per variant. Aged with 10 generations of create/rename/unlink plus a quarter-of-children churn over the 8 widest directories, so the aged densest directory stays wide (fresh 4994 children over 12 distinct 1024-ino buckets; aged 4241 over 134) and the only thing that moved is where its children's `0x01` records live.

| Tree | Shape | ops | Variant | keys written | nodes | zstd bytes | B/op | today's segment B/op | ratio |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|
| fresh | clustered setattr (one directory) | 1000 | copy | 1824 | 91 | 473.46 KiB | 485 | 6 | 83.42× |
| fresh | clustered setattr (one directory) | 1000 | nocopy | 912 | 46 | 332.23 KiB | 340 | 6 | 58.55× |
| fresh | clustered setattr (one directory) | 1000 | dentry-auth | 912 | 46 | 339.57 KiB | 348 | 6 | 59.84× |
| fresh | scattered setattr (random ino) | 1000 | copy | 2000 | 2940 | 23.66 MiB | 24811 | 10 | 2459.67× |
| fresh | scattered setattr (random ino) | 1000 | nocopy | 1000 | 1484 | 14.41 MiB | 15109 | 10 | 1497.89× |
| fresh | scattered setattr (random ino) | 1000 | dentry-auth | 1031 | 1500 | 18.48 MiB | 19374 | 10 | 1920.71× |
| fresh | clustered setattr (one directory) | 10000 | copy | 8708 | 114 | 623.99 KiB | 64 | 4 | 14.76× |
| fresh | clustered setattr (one directory) | 10000 | nocopy | 4354 | 67 | 483.09 KiB | 49 | 4 | 11.43× |
| fresh | clustered setattr (one directory) | 10000 | dentry-auth | 4354 | 48 | 332.86 KiB | 34 | 4 | 7.88× |
| fresh | scattered setattr (random ino) | 10000 | copy | 19996 | 19863 | 152.75 MiB | 16018 | 9 | 1772.53× |
| fresh | scattered setattr (random ino) | 10000 | nocopy | 9998 | 9904 | 100.86 MiB | 10577 | 9 | 1170.40× |
| fresh | scattered setattr (random ino) | 10000 | dentry-auth | 10267 | 10229 | 126.96 MiB | 13313 | 9 | 1473.20× |
| fresh | clustered setattr (one directory) | 100000 | copy | 9988 | 114 | 615.88 KiB | 6 | 3 | 2.33× |
| fresh | clustered setattr (one directory) | 100000 | nocopy | 4994 | 67 | 477.45 KiB | 5 | 3 | 1.81× |
| fresh | clustered setattr (one directory) | 100000 | dentry-auth | 4994 | 48 | 326.67 KiB | 3 | 3 | 1.24× |
| fresh | scattered setattr (random ino) | 100000 | copy | 199158 | 102875 | 640.89 MiB | 6721 | 9 | 757.84× |
| fresh | scattered setattr (random ino) | 100000 | nocopy | 99579 | 51532 | 429.97 MiB | 4509 | 9 | 508.43× |
| fresh | scattered setattr (random ino) | 100000 | dentry-auth | 102048 | 52637 | 534.49 MiB | 5605 | 9 | 632.03× |
| aged | clustered setattr (one directory) | 1000 | copy | 1820 | 423 | 2.98 MiB | 3128 | 6 | 527.67× |
| aged | clustered setattr (one directory) | 1000 | nocopy | 910 | 386 | 2.89 MiB | 3028 | 6 | 511.91× |
| aged | clustered setattr (one directory) | 1000 | dentry-auth | 899 | 39 | 259.50 KiB | 266 | 6 | 45.01× |
| aged | scattered setattr (random ino) | 1000 | copy | 2000 | 2942 | 23.69 MiB | 24847 | 10 | 2463.29× |
| aged | scattered setattr (random ino) | 1000 | nocopy | 1000 | 1484 | 14.37 MiB | 15071 | 10 | 1494.12× |
| aged | scattered setattr (random ino) | 1000 | dentry-auth | 1031 | 1505 | 18.51 MiB | 19414 | 10 | 1924.67× |
| aged | clustered setattr (one directory) | 10000 | copy | 7732 | 638 | 3.92 MiB | 412 | 4 | 102.68× |
| aged | clustered setattr (one directory) | 10000 | nocopy | 3866 | 602 | 3.80 MiB | 398 | 4 | 99.69× |
| aged | clustered setattr (one directory) | 10000 | dentry-auth | 3861 | 39 | 257.68 KiB | 26 | 4 | 6.60× |
| aged | scattered setattr (random ino) | 10000 | copy | 19996 | 19835 | 152.53 MiB | 15995 | 9 | 1769.90× |
| aged | scattered setattr (random ino) | 10000 | nocopy | 9998 | 9892 | 100.58 MiB | 10547 | 9 | 1167.11× |
| aged | scattered setattr (random ino) | 10000 | dentry-auth | 10267 | 10226 | 126.76 MiB | 13293 | 9 | 1470.91× |
| aged | clustered setattr (one directory) | 100000 | copy | 8482 | 649 | 3.95 MiB | 41 | 3 | 15.92× |
| aged | clustered setattr (one directory) | 100000 | nocopy | 4241 | 618 | 3.83 MiB | 40 | 3 | 15.34× |
| aged | clustered setattr (one directory) | 100000 | dentry-auth | 4243 | 40 | 256.42 KiB | 3 | 3 | 1.01× |
| aged | scattered setattr (random ino) | 100000 | copy | 199158 | 102564 | 638.91 MiB | 6700 | 9 | 755.50× |
| aged | scattered setattr (random ino) | 100000 | nocopy | 99579 | 51307 | 427.55 MiB | 4483 | 9 | 505.57× |
| aged | scattered setattr (random ino) | 100000 | dentry-auth | 102048 | 52536 | 534.12 MiB | 5601 | 9 | 631.59× |

#### `ls -la` of a wide directory
32 directories of ≥1000 entries, scanned whole. Distinct packs and pack reads are properties of the operation and are traced on the single-threaded pass. The tier is re-prepared before each pass — cache cleared for (b), page cache dropped for (c) — so the 8-thread column is a second cold run rather than a replay against the warm cache the serial pass just filled.

| Tree | Tier | Variant | dirents/dir | distinct packs/dir | pack reads/dir | ms/dir (1 thread) | ms/dir (8 threads) | speed-up |
|---|---|---|---:|---:|---:|---:|---:|---:|
| fresh | (b) 64 MiB | copy | 3002 | 1.06 | 27.1 | 2.329 | 0.648 | 3.60× |
| fresh | (b) 64 MiB | nocopy | 3002 | 2.41 | 57.5 | 9.586 | 3.985 | 2.41× |
| fresh | (b) 64 MiB | dentry-auth | 3002 | 1.25 | 27.1 | 2.604 | 0.660 | 3.94× |
| fresh | (c) cold | copy | 3002 | 4.06 | 30.4 | 2.248 | 0.459 | 4.90× |
| fresh | (c) cold | nocopy | 3002 | 5.44 | 12038.8 | 321.091 | 66.396 | 4.84× |
| fresh | (c) cold | dentry-auth | 3002 | 2.41 | 30.4 | 3.612 | 0.773 | 4.67× |
| aged | (b) 64 MiB | copy | 2942 | 1.03 | 26.0 | 4.514 | 0.842 | 5.36× |
| aged | (b) 64 MiB | nocopy | 2944 | 3.44 | 72.0 | 7.863 | 4.185 | 1.88× |
| aged | (b) 64 MiB | dentry-auth | 2942 | 1.12 | 26.1 | 2.986 | 0.561 | 5.32× |
| aged | (c) cold | copy | 2942 | 4.03 | 29.3 | 4.437 | 0.691 | 6.42× |
| aged | (c) cold | nocopy | 2944 | 6.94 | 11804.3 | 287.898 | 68.928 | 4.18× |
| aged | (c) cold | dentry-auth | 2942 | 2.12 | 29.3 | 2.979 | 0.518 | 5.75× |

#### `getattr(ino)` — what the escape hatch costs
`copy` and `nocopy` read one `0x01` leaf. `dentry-auth` has no `0x01` record for a `nlink == 1` file, so it probes `0x04 | ino` for the name and then reads the dentry: two leaves in two distant ranges.

| Tier | Variant | getattr/s (1 thread) | getattr/s (8 threads) | scaling | pack reads/getattr |
|---|---|---:|---:|---:|---:|
| (b) 64 MiB | copy | 16 k/s | 45 k/s | 2.91× | 0.95 |
| (b) 64 MiB | nocopy | 16 k/s | 55 k/s | 3.43× | 0.95 |
| (b) 64 MiB | dentry-auth | 8 k/s | 28 k/s | 3.55× | 1.93 |
| (c) cold | copy | 8 k/s | 33 k/s | 3.96× | 4.00 |
| (c) cold | nocopy | 7 k/s | 40 k/s | 5.37× | 4.00 |
| (c) cold | dentry-auth | 3 k/s | 13 k/s | 3.83× | 7.92 |

Aging took 110–149 s per variant.

**Verdict: keep the attr copy (§P6 unchanged).** `nocopy` saves 10.9% of stored leaf bytes and 1.3–1.5× on `setattr`, but charges 12,039 pack reads for a cold `ls -la` against `copy`'s 30.4, and that is the one read cost here that does not thread away (still 66 ms/dir at 8 threads). On the aged tree its write saving is 3%. `dentry-auth` is the only shape that writes less without reading a directory worse, and is recorded in §14.10 as the measured escape hatch rather than adopted. Full reasoning in §14.10 of `docs/plans/v1/done/28-s3-native-metadata-store.md`.
