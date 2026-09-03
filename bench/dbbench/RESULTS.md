# dbbench results — realistic workload (transactions + journal)

Date: 2026-09-03  
Host: 32-core desktop, ZFS root, warm page cache  
Corpus: `data/corpus-src.db` — vacuumed live Constellation metadata replica  
(161 939 inodes, 161 934 dentries, 23.8 MiB logical; production schema with manifests)

## What changed vs the earlier averages-only run

Every write op now mirrors the actual `SqliteMeta` contract:

1. **Transactions** — each write (setattr or create) commits atomically via one
   explicit transaction, not a bare single-statement auto-commit.
2. **Journal row in the same tx** — a 64-byte dummy record is inserted into a
   `journal` table inside every write transaction, exactly as production does
   (`crates/meta/src/lib.rs`: *"every mutating call journals the corresponding
   log record in the same transaction"*).
3. **create = multi-key tx** — inserts inode + dentry + journal in one commit,
   reflecting `SqliteMeta::create`.
4. **Durability settings aligned** — SQLite: `synchronous=NORMAL` / WAL (same
   as production). LMDB: `WRITE_MAP` (direct page write, no dirty-page copy;
   replaces the earlier `NO_SYNC`). RocksDB: `sync=true` `WriteOptions` on
   every write batch.

Archived engines (fjall, redb) keep their code but are not in the default run.
Run them with `--include-archived`.

```bash
cd bench/dbbench
cargo run --release -- --threads 8
cargo run --release -- --threads 8 --include-archived  # add fjall + redb
cargo run --release -- rocksdb --threads 8              # single engine
```

## Isolated throughput

| Engine | Store | Load | Lookup (1T) | Lookup (8T) | readdir | getattr | setattr | create |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **lmdb** | 256 MiB† | 0.11 s | **3.47 M/s** | **21.9 M/s** | **2.41 M dirs/s** | **2.84 M/s** | 47 k/s | 36 k/s |
| **rocksdb** | 482 MiB‡ | 0.07 s | 2.38 M/s | 14.6 M/s | 0.37 M dirs/s | 1.71 M/s | **261 k/s** | **276 k/s** |
| sqlite-mt | 126 MiB | 0.22 s | 0.84 M/s | 2.44 M/s | 0.62 M dirs/s | 0.78 M/s | 47 k/s | 28 k/s |
| **sqlite** (mutex) | 113 MiB | 0.22 s | 1.04 M/s | 0.26 M/s | 0.71 M dirs/s | 0.98 M/s | 48 k/s | 28 k/s |

† LMDB with `WRITE_MAP`; map pre-allocates up to 256 MiB on-disk.  
‡ RocksDB after ~250 k sync writes; LSM write-amp inflates store significantly.

The **setattr/create collapse vs the earlier no-journal run** is real: adding
the journal row inside the same tx roughly doubles the write cost (one extra
page dirty + AUTOINCREMENT lookup for SQLite; a second sub-tree write for LMDB;
a third CF entry in every sync batch for RocksDB).

## Isolated latency (200 k samples each)

| Engine | lookup p50 | lookup p99 | setattr p50 | setattr p99 | setattr max | create p50 | create p99 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **lmdb** | **300 ns** | **526 ns** | 20.3 µs | 25.6 µs | 539 µs | 27.3 µs | 35.7 µs |
| rocksdb | 433 ns | 857 ns | **4.0 µs** | **9.0 µs** | 415 µs | **3.3 µs** | **4.7 µs** |
| sqlite | 993 ns | 2.5 µs | 16.1 µs | 27.7 µs | **4.96 ms** | 26.2 µs | 106 µs |
| sqlite-mt | 996 ns | 1.3 µs | 16.4 µs | 26.9 µs | 2.95 ms | 26.2 µs | 106 µs |

Key observation: **LMDB write latency doubles vs the no-journal run** (~14 µs →
~21 µs) because the journal key sorts after all inode keys and dirtied a second
B-tree page per commit. **RocksDB write latency is nearly unchanged** — a sync
`WriteBatch` with an extra CF key barely adds cost, and the WAL serialises
everything into a single `fdatasync`.

## Mixed concurrent (8T, 15 s) — op mix 45/30/15/7/3

Op mix: 45 % lookup / 30 % getattr / 15 % readdir / 7 % setattr / 3 % create.
(The writer `Mutex` is still shared across threads; this stresses lock contention.)

| Engine | ops/s | p50 | p99 | p99.9 | max | stalls | win min / med |
|---|---:|---:|---:|---:|---:|---:|---:|
| **rocksdb** | **961 k** | **1.7 µs** | **64 µs** | 144 µs | 5.0 ms | 9/150 | 363 k / 984 k |
| sqlite-mt | 156 k | 19 µs | 508 µs | 2.6 ms | 13 ms | 8/150 | 100 k / 158 k |
| lmdb | 102 k | **2.3 µs** | 1.55 ms | 3.7 ms | 39 ms | 0/150 | 50 k / 74 k |
| **sqlite** | **62 k** | 26 µs | 1.28 ms | 4.8 ms | **17 ms** | **17/150** | 19 k / 55 k |

**LMDB's mixed ops/s drops from 208 k to 102 k** — the write-exclusive LMDB
lock now holds through two dirty pages (inode + journal), starving concurrent
readers more than the no-journal setattr-only run. Its mixed p99 spikes to
~1.55 ms as a result. **RocksDB is the clear mixed winner**: sync batches still
under 10 µs median because RocksDB's WAL pipeline handles concurrent writers
without a global write lock.

## Realism assessment

| Requirement | sqlite | sqlite-mt | lmdb | rocksdb |
|---|:---:|:---:|:---:|:---:|
| Multi-key atomic tx (inode+dentry+journal) | ✓ | ✓ | ✓ | ✓ (batch) |
| Journal row in same tx | ✓ | ✓ | ✓ | ✓ (same batch) |
| Durable on commit (no data loss after clean shutdown) | ✓ NORMAL | ✓ NORMAL | ✓ WRITE_MAP | ✓ sync |
| Crash-safe WAL (power-cut safe without fsync) | WAL | WAL | ✗ WRITE_MAP | WAL |
| Concurrent readers without blocking writers | ✗ mutex | ✓ TLS | ✗ excl lock | ✓ MVCC |
| Store size after write-heavy phase | 113 MiB | 126 MiB | 256 MiB | 482 MiB |

**Power-cut safety** is not required by Constellation's local replica store
(the S3 log is the durability layer; the local DB is a performance cache rebuilt
from checkpoints on corruption). `WRITE_MAP` is therefore acceptable for LMDB.
RocksDB's WAL gives stronger guarantees at no extra cost.

## Archived engines (from earlier run, no-journal setattr only)

Kept for historical comparison. Re-run with `--include-archived`.

| Engine | Store | Lookup (8T) | setattr p50 | mixed ops/s | mixed p99 | stalls |
|---|---:|---:|---:|---:|---:|---:|
| fjall | 48 MiB | 9.52 M/s | 3.2 µs | 1.27 M/s | 79 µs | 4/150 |
| redb | 65 MiB | 2.64 M/s | 30.2 µs | 111 k/s | 1.61 ms | 40/150 |

Raw output: `data/RESULTS-raw.txt`
