# Constellation Goals

Constellation is a distributed, POSIX-compliant file system for loosely
connected machines, built on S3-compatible object storage. One static binary,
no mandatory services beyond the bucket.

## High Level Goals

- **S3 is the only required infrastructure.** A filesystem lives in one
  S3-compatible bucket (AWS S3, MinIO, R2, GCS interop, ...). Anything with
  working credentials for the bucket is a full member; there is no metadata
  server, no coordinator service, no user database to operate.
- **Safe concurrent read/write from any number of hosts**, POSIX-compliant by
  default (close-to-open consistency), with per-mount/per-subtree knobs to
  make it stricter (full POSIX locking) or looser (async/relaxed) — the user
  chooses the correctness/performance trade-off.
- **Conflicts are prevented, not merged.** Every write traces to exactly one
  non-concurrent authority (subtree lease, offline designation, or
  continuation epoch). Unlike syncthing-style tools there are no conflict
  copies in the default modes.
- **Offline-first.** Any node can designate a subtree (or the whole tree) for
  offline use and keep writing without connectivity; other nodes fall back to
  read-only for that subtree while the designee is away and everything
  reintegrates automatically on reconnect — even a few minutes of
  connectivity suffice to sync up.
- **LAN-fast when peers are near.** Nodes connect peer-to-peer (encrypted
  QUIC with NAT traversal); data and coordination take the fastest measured
  path — S3 is the arbiter of record, peers are the fast path.
- **Local-FS-fast when you own the data.** A node holding the lease over a
  subtree performs metadata operations at local speed and syncs in the
  background; the common single-writer case pays no coordination latency.
- **Works over the internet**: through NATs, encrypted and mutually
  authenticated everywhere, with no port forwarding.
- **All file operations supported**: rename (instant, even for huge
  directories), hardlinks, symlinks, sparse files, unix permissions.
- **Effectively unlimited storage** through an LRU chunk cache: the local
  disk holds the hot set (plus pinned subtrees); cold data lives only in S3.
  Individual files may exceed the cache — reads and writes both stream
  (prefetch/eager-upload), so file size is bounded by S3, not local disk.
- **Compact and fast at personal scale**: designed and tested for 1–10M
  files, 1–5 TB, 3–10 nodes; the on-bucket format is partitioned so 100M+
  files need no migration.
- **Fully administrable** from a CLI and an embedded web UI: mounts, pins,
  offline designations, cache state, peers, latencies, exports, fsck.
- **Thoroughly tested**: POSIX suites, property-based tests, deterministic
  multi-node fault-injection simulation, and end-to-end scenario tests are
  part of the definition of done.

## Non-Goals

- Not a root/system filesystem: it syncs user data, not `/usr`.
- Not optimized for live databases or other sustained scattered-small-write
  workloads inside the mount (a known mitigation — slice overlays — is format-
  reserved but deliberately unimplemented).
- No storage-side enforcement of unix permissions between mutually distrusting
  users: access control is bucket access (IAM); uid/gid/mode are attributes,
  enforced by each mounting kernel (NFS-style).
- No blockchain, no DHT, no global peer discovery: membership is the bucket.
- macOS, other Unix-like and Windows support is desirable (WinFsp) but not av1
  requirement; Linux first.

## Motivating Scenarios

### Three machines, one life

- **Desktop** — primary workplace, holds leases over nearly everything and
  runs at local speed.
- **Laptop** — used next to the desktop (LAN sync within seconds via P2P
  push) and taken on the road: `constellation offline ~/data` before leaving
  guarantees exclusive write authority while away, regardless of
  connectivity; everything reintegrates when back.
- **Server** — always-on read-mostly follower tailing the metadata log as a
  live backup; never blocks anyone. Periodic `snapshot create` on it gives
  restic-class deduplicated point-in-time history with zero scan cost (the
  change journal replaces the tree walk).

If home internet dies while both desktop and laptop are on the LAN, they form
a continuation epoch (all write-eligible nodes present) and keep working at
full speed, flushing to S3 when it returns.

Current pain this replaces: syncthing scans millions of files, burns CPU,
knows nothing about locking or client state, and produces conflict copies for
concurrent edits — catastrophic around git workflows.

### Latency-sensitive web fleet

Ten web servers serve static files from a bucket. With mountpoint-s3 each
node pays S3's 30–100 ms time-to-first-byte once per file per node and caches
duplicate copies. As Constellation read-only followers they share caches:
bloom-filter digests gossip which chunks each node holds, and a miss is
fetched from a same-region peer in ~1 ms with automatic, latency-measured
fallback to S3. Cold cost is paid roughly once per cluster, not once per node.

## Prior Art (and why not just use it)

| System | What we take | Why it isn't enough |
|---|---|---|
| **JuiceFS** | metadata/data decoupling, chunked data plane, engine abstraction | requires an always-reachable metadata service; no offline mode; no P2P |
| **Coda** | hoarding/pinning, disconnected operation, reintegration | 1990s implementation; server-centric; no object storage |
| **syncthing** | the itch itself | no locking, no client state, conflict copies, full-tree rescans |
| **mountpoint-s3** | prefetcher design, S3 throughput techniques | 1:1 object mapping: no rename, no edits, no multi-writer, no cache sharing |
| **NFSv4** | delegations (basis of offline designation), close-to-open contract | needs a server; no offline writes; no object storage |
| **restic** | the closest storage-layer relative: content-addressed encrypted blobs, git-style snapshot trees, CDC + packfiles (our two reserved entry types), keys-in-repo E2E — a decade of proof this shape works on dumb object stores | a camera, not the scene: batch backups of a filesystem living elsewhere; re-walks the whole tree every run (the scan tax again); mount is read-only and slow (no metadata replica); repo-level lock files only — `prune` takes an exclusive lock, while our GC must run concurrently because the FS never stops |
| **git-annex** | content tracking, partial checkouts | not a live filesystem; manual workflow |

Key enablers that make this design possible now: S3 conditional writes
(`If-Match`/`If-None-Match`, 2024+) provide a linearizable CAS primitive on
plain object storage, and iroh 1.0 provides encrypted dial-by-public-key QUIC
with ~90%+ NAT hole-punching success and stateless relays.
