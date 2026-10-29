# Directory hash-range split benchmark

Does splitting one hot shared directory into name-hash ranges (plan 30
§M12, "GIGA+", see [Delegations](../../docs/reference/features/delegations.md#hot-shared-directories))
pay off, and under which network topology? The results and the verdict
are in [RESULTS.md](RESULTS.md).

## What runs

The harness scenario `dirsplit-bench`
(`crates/harness/src/scenarios/dirsplit.rs`, run by name only) mounts one
root `r` (never a writer) and `N` writers `w0..` of one filesystem on one
host, each placed on a *site*:

- **S3 distance**: every node reaches S3 through its own toxiproxy route
  (`S3Env::site_proxy`, the process backend) with a latency toxic each
  way of half the site's S3 latency (jitter a tenth of that), so one
  request/response costs the site's S3 latency, followed by a request-counting relay
  (`reqlog::CountingProxy`) for the S3 PUT/GET counts.
- **P2P distance**: `tc netem` on `lo`, one band per distinct one-way
  delay and a `u32` filter for every ordered pair of the daemons' UDP
  ports, so each pair of nodes gets its own delay. Filters are installed
  as each daemon mounts (before any P2P link exists) and refreshed
  before every phase for ports opened later.
- The root stays put: `CONSTELLATION_LEASE_PLACEMENT=off` and no
  handoff on forwarded demand. Everything else is the default
  (`ack_policy=local`, backups within the 5 ms RTT budget, automatic
  subtree placement on).

Each phase runs one workload on a fresh directory, `DIRSPLIT_THREADS`
(4) closed-loop threads per writer on the writer's own mount, a warm-up
(5 s; 20 s for `b`, whose placement window must see the load first) and
a 15 s measurement. Files are empty, so no data goes to S3. Workloads:

| workload | op |
|---|---|
| `uniform` | create (`open(O_CREAT|O_EXCL)` + `close`) of a random name |
| `mix` | a create, and the unlink of the thread's 17th-newest file |
| `rename` | rename one of the thread's 16 files to a new random name |
| `own` | a create of a random name hashing into the writer's own range of `N` (**synthetic** best case for a split) |

Configurations:

| config | meaning |
|---|---|
| `a` | `CONSTELLATION_DELEGATION_SPLIT=0`: no hash-range split; the placement may still delegate the directory whole to a dominant writer (reported as `whole`) |
| `b` | the default automatic placement (`SPLIT=20`); reports whether a split fired |
| `c` | a hand split, `N` ranges, range `i` delegated to writer `i` (for `own` that is the writer that dominates it) |
| `d` | the directory delegated whole to `w0` |

`a`, `c` and `d` share a cluster mounted with `SPLIT=0`, their order
rotated by one per repetition; `b` runs on a cluster of its own (the knob
is read at mount); `rename` runs on a cluster per configuration (see
below). After each phase every delegation of the directory is
recalled, and the root and the last writer must list exactly the names
the writers left (`converged`).

Recorded per phase (one JSON line): ops/s in total and per writer,
p50/p90/p99/max op latency (in total and per op kind), errors by kind,
every daemon's CPU (utime+stime over the window), S3 requests per node
by class, the delegation counters' deltas, the directory's delegation
table at the window's start and end, when a `b` split fired, and each
node's P2P RTTs and backups.

## Topologies

Site 0 holds the root. `S3` is one request/response; RTT is the P2P
round trip.

| name | layout | S3 site 0 / others | RTT same site / other |
|---|---|---|---|
| T1 | every node on site 0 | 20 / - ms | 0.5 ms |
| T2 | every node on site 0 | 80 / - ms | 0.5 ms |
| T3 | root + first half of the writers on site 0, the rest on site 1 | 20 / 80 ms | 0.5 / 60 ms |
| T4 | every node on a site of its own | 80 / 80 ms | - / 60 ms |
| T5 | the root on site 0, every writer on site 1 | 20 / 80 ms | 0.5 / 60 ms |
| X-rtt*R*-s3*S* | T5 with root RTT `R` and S3 `S` for every node | S / S | 0.5 / R ms |

Why these values: an RTT within one AZ is 0.1-0.5 ms (0.5 is the
pessimistic end, and a tenth of the 5 ms backup budget); between regions
on one continent 20-80 ms (60 is typical, e.g. us-east to us-west or
eu-west to us-east). S3's first-byte latency for small objects in its own
region is 10-30 ms; from another region it adds the inter-region RTT
(80 = 20 + 60).

## Rerunning

On a Linux host with FUSE, `sudo -n` for `tc`, the native `versitygw`
and `toxiproxy-server` (`tests/ci/install-native-s3.sh`):

```sh
cargo build --release -p constellation -p constellation-harness
export BIN_DIR=$PWD/target/release S3_BIN_DIR=/path/to/s3-binaries OUT=/tmp/dirsplit-out
bench/dirsplit/matrix.sh T1:4 T3:4 X:10:20     # cells: T<k>:<writers>, X:<rtt_ms>:<s3_ms>
bench/dirsplit/analyze.py $OUT > tables.md
```

`run-cell.sh` runs one cell: `REPS` (default 3) repetitions of a
cluster for `uniform`, `mix` and `own` under `a`, `c` and `d` (order
rotated per repetition), one cluster per configuration for `rename` (a
hand split's cross-range renames leave the root slower, which would skew
later phases), and a `b` cluster for every workload, `rename` last. Every
harness run is under `timeout`; the cell starts once the host's 1-minute
load average is below `MAX_LOAD` (4). One phase directly:

```sh
DIRSPLIT_LAYOUT=half DIRSPLIT_WRITERS=4 DIRSPLIT_S3_NEAR_MS=20 DIRSPLIT_S3_FAR_MS=80 \
DIRSPLIT_RTT_LOCAL_MS=0.5 DIRSPLIT_RTT_REMOTE_MS=60 DIRSPLIT_WORKLOADS=uniform \
DIRSPLIT_CONFIGS=a,c DIRSPLIT_OUT=/tmp/x.jsonl CONSTELLATION_BIN=target/release/constellation \
target/release/harness run dirsplit-bench --s3-backend process
```

Other knobs: `DIRSPLIT_THREADS`, `DIRSPLIT_WARM_S`, `DIRSPLIT_BWARM_S`,
`DIRSPLIT_MEASURE_S`, `DIRSPLIT_ROTATE`, `DIRSPLIT_REP`, `DIRSPLIT_LABEL`,
and `DIRSPLIT_ENV=K=V,...` for extra daemon environment. A crashed run's
`lo` qdisc is removed by the next run (its marker names a dead pid) or by
`sudo tc qdisc del dev lo root`.
