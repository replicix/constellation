# Close-to-open modes

What a read on one node is guaranteed to see of writes made on other
nodes (plan 30 M6–M8). Every node gets per-session guarantees
(read-your-writes and monotonic reads). On top of that, a mount chooses
between `bounded` close-to-open (the default) and `strict` close-to-open
with `--cto`.

## Table of Contents

- [Terminology](#terminology)
- [Modes](#modes)
- [Details](#details)
  - [Positions](#positions)
  - [Session guarantees](#session-guarantees)
  - [Direct log streams](#direct-log-streams)
  - [Strict reads: ReadIndex](#strict-reads-readindex)
  - [Read delegations](#read-delegations)
  - [Recalls and what writers wait for](#recalls-and-what-writers-wait-for)
  - [Clocks and margins](#clocks-and-margins)
  - [Without P2P](#without-p2p)
  - [Cost by topology](#cost-by-topology)
  - [Known gaps](#known-gaps)
- [Configuration](#configuration)
- [Status](#status)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **Sequencer**: the node that orders a file's mutations: the lease
  holder, or the delegate of the file's subtree (see
  [Delegations](delegations.md)).
- **Position**: a point in the metadata history: a log sequence, plus
  the sequencer's unshipped journal position `(epoch, jseq)` if it had
  one, plus up to 8 delegation stream positions.
- **Observed watermark**: the highest position this node's clients have
  seen in a reply whose effects are not in its replica yet.
- **ReadIndex**: a strict reader asking the sequencer for the position
  that covers every acknowledged mutation of what it reads (Raft's read
  index, Ongaro §6.4).
- **Read delegation**: a short, leased permission from the sequencer to
  answer opens and lookups of one inode locally.
- **Recall**: the sequencer asking a delegation's holder to drop it
  before a mutation of that inode is acknowledged.
- **Degraded read**: a read that gave up waiting and answered from the
  local replica. It is counted, never an error.

## Modes

| Mode | Guarantee | Cost |
|---|---|---|
| `bounded` (default) | An open reads the local replica, which follows the log within the visibility bound. Per-node session guarantees hold. | none on the read path; a read waits only if its own client has seen something the replica has not |
| `strict` | An `open`, `lookup` or first `readdir` chunk on a node that is not the sequencer sees every `close` another node completed before it began. | one round trip to the sequencer on a first open, then local under a read delegation; writers wait for recalls |

The mode is per mount (`--cto bounded|strict`), with
`CONSTELLATION_CTO` as the default when the flag is absent. It is not
stored in the registry. A sequencer serves strict readers whatever its
own mount's mode is.

The default is `bounded`. Plan 30 M16 decides from the EC2 measurements
whether to change it.

## Details

### Positions

Every reply to a forwarded mutation carries the position it was
evaluated at: for `Accepted`, the position of its records. Log segments
carry the journal position they ship through, so a replica can tell
whether it has everything a position names. Positions from a tenure
that ended before shipping order below everything of the next epoch: a
reader waiting for effects that were stranded stops waiting at the new
holder's epoch marker, because it is not owed them.

### Session guarantees

A node keeps an `observed` watermark. It is raised only by replies
whose effects are not installed locally: a refusal, an `Exists` that
installed no hint, an op that waited for the log. An accepted forwarded
op installed as a shadow does not raise it, because the shadow already
covers its keys. The watermark is in memory only; a restart starts a
new session.

Every FUSE read path waits before it reads:

| Operation | What it waits for |
|---|---|
| `lookup` | the dentry `(parent, name)` |
| `getattr`, `readlink`, `open`, `getxattr`, `listxattr` | the inode |
| `readdir` (first chunk only) | the directory |

The wait (`Meta::session_wait`) goes like this:

1. A queued replay of this node's own stranded op touching the keys:
   wait, since that is the session's own acknowledged write missing.
   Under a `Backup` or `S3` acknowledgement policy, the holder's own
   reads also wait for unshipped rows they would observe to become
   durable (see [Durability and failover](durability-and-failover.md)).
2. The replica has reached `observed`: read at once (the fast path).
3. Every key is covered by speculation at or past `observed`: read at
   once. A directory listing is never covered this way.
4. Otherwise wait for the replica to advance, up to
   `CONSTELLATION_SESSION_WAIT_MS` (2 s). On timeout, answer anyway,
   log a warning once per daemon, and count it.

The result is read-your-writes and monotonic reads for every client of
a node, without any cost when the node is idle.

### Direct log streams

Followers get the log from the holder over direct QUIC streams
(`LogSubscribe`), not from gossip payloads. The holder streams
*shipped* segments in order, numbered per subscription, and keeps a
ring of recent segments for late subscribers. Subscribers apply frames
through exactly the tail code path, fencing included.

- A gap, a timeout (`CONSTELLATION_LOG_STREAM_TIMEOUT_MS`), or the
  holder ending the stream drops the subscriber back to S3 tailing; it
  resubscribes with backoff.
- The holder never waits for a subscriber. One that falls more than
  `CONSTELLATION_LOG_STREAM_BUFFER_BYTES` behind is dropped to S3.
- A caught-up subscriber still probes S3 with one GET every
  `CONSTELLATION_LOG_STREAM_BACKSTOP_MS`.
- `SegmentPublished` gossip is now only a hint to tail at once; gossip
  carries membership and digests.
- Under a `Backup` acknowledgement policy, the holder also streams
  backup-acknowledged transactions ahead of S3 (`StreamAhead`), which
  subscribers apply as speculation.

Records applied from other nodes also push kernel entry and inode
invalidations (`CONSTELLATION_KERNEL_INVALIDATE`), so the kernel's
attribute cache does not add a second of staleness.

### Strict reads: ReadIndex

In strict mode, a node that is not the sequencer asks before it reads:

1. If this node is the sequencer, it reads locally (after the session
   wait).
2. If it holds a valid read delegation on the inode, it reads locally,
   waiting only for the position the delegation came with.
3. Otherwise it sends `ReadIndex { ino, dir, name }` to the sequencer
   and waits for the answer:
   - a **position**: wait until the replica reaches it, then read;
   - **tailed**: there is no live sequencer or no P2P path; the node
     tailed S3 to head and reads;
   - no answer within `CONSTELLATION_READ_INDEX_BUDGET_MS` (2 s):
     degraded, then an ordinary session wait.

The sequencer answers only while its lease is usable and its view is
open (not in a takeover gate, not releasing): a usable lease is
exclusive, so its replica is the authority. The position it answers is
its log head, plus its unshipped journal position only if that journal
touched what the reader reads. A file nobody else writes therefore
costs one round trip and no wait, however busy the sequencer is with
other files. The answer carries a position, not the inode record: a
record installed as a hint could be regressed by a later segment of an
older write.

With strict mode, the kernel's attribute and entry cache TTLs are 0, so
every open and path step reaches the daemon. A node that is the
sequencer and has seen no other node yet uses a short TTL instead (half
the lease margin, at most 1 s). The first sign of another node turns it
to 0, and every acknowledgement and release then waits once for the
cached entries to expire.

### Read delegations

A ReadIndex answer may carry a read delegation on the inode: for a file,
its attributes, manifest and xattrs (granted on `open`); for a
directory, its entries (granted on `lookup` and `readdir`). While a node
holds one, opens and lookups under it are answered locally, with no
round trip. It is renewed in the background (by another ReadIndex) once
half its TTL has passed and it is still in use.

- A grant lasts `CONSTELLATION_READ_DELEGATION_TTL_MS` (5 s), capped so
  that it never outlives the lease that backs it.
- The sequencer makes the grant before it reads the position, and
  persists a *grant horizon* before it answers, so a sequencer that
  restarts inside its lease grants nothing new until earlier grants
  have expired.
- No grants inside a continuation epoch, and none while an inbox op on
  the inode waits for a recall.
- A delegation that races a recall is not installed.
- `CONSTELLATION_READ_DELEGATIONS=0` turns grants off on this node as
  sequencer: every strict read then costs a round trip. With P2P off
  there are no grants.

### Recalls and what writers wait for

Before the acknowledgement of any mutation that touched a delegated
inode leaves the sequencer, every delegation on it held by a node other
than the writer is recalled (`ReadRecall`, answered `ReadRecalled`) or
outwaited (it is live until `granted + ttl + margin` on the
sequencer's clock). This covers a forwarded op's reply, a local op's
reply, the FUSE fast path, an inbox op (recalled before it executes),
and a release of the lease. A change to a dentry also recalls the
parent directory's delegation, and an `unlink`, `rmdir` or `rename`
also recalls the inodes it changes that its records do not name: the
unlinked (or replaced) inode, whose `nlink` and `ctime` change or which
is gone, and a renamed inode. Otherwise a node holding a delegation on
an unlinked file would keep answering a strict open of it by inode (a
dentry its kernel still caches) from its stale replica after the
unlink completed. The executed paths take these from the op's own
transaction; an inbox op and a delegate's write, which recall before
executing, look them up just before.

The wait is parked, not blocking: other keys, the ship round and other
requesters proceed. A forwarded op whose reply is held longer than half
the forward timeout is answered `Held { retry_ms }`, and the requester
retries the same rid (see
[Forwarded mutations](forwarded-mutations.md#wire-messages)). The
requester's retry is answered from dedup once the wait is over.

A successor that takes the lease over before it expired (a sealed
backup, or `ack=s3`) waits out the previous tenure's grant horizon
before it acknowledges a mutation, if that tenure granted any (the
lease object's `granted_delegations` flag).

### Clocks and margins

The delegate honours a grant until `sent + ttl − margin` on its own
clock, measured from when it sent the request. The sequencer treats it
as live until `granted + ttl + margin` on its own clock. `margin` is the
lease's expiry margin, `min(1 s, lease TTL / 4)`. Every condition
reduces to the lease's own rule, `margin > 2 × drift`: at the defaults,
clocks may differ from real time by up to ±500 ms. The Stateright model
in `crates/model/src/cto.rs` checks the argument.

### Without P2P

There is no ReadIndex, no delegation and no recall. A strict open tails
S3 to head and reads. That makes every close whose records are *in the
log* visible, which covers every write that went through the
[S3 inbox](forwarded-mutations.md#the-inbox-forwarding-without-p2p).
The sequencer's own writes are acknowledged before they ship, so under
P2P off they become visible to other nodes' strict opens only at its
next ship.

### Cost by topology

Measured on one host behind an S3 emulator (plan 30 M8); WAN numbers
come from `bench/remote`.

| Topology | Bounded | Strict |
|---|---|---|
| Single node | open p50 ≈ 51 µs | the same; a one-time drain of about 1 s when a second node first appears |
| LAN, first open | local | ≈ 0.4–0.5 ms (one round trip) |
| LAN, open under a delegation | local | ≈ 80–120 µs |
| LAN, open right after another node's close | 12–16 of 80 opens saw stale content | p50 ≈ 1 ms, 0 of 80 stale |
| Across continents, file nobody else writes | local | one WAN round trip on the first open, then local |
| Across continents, write then open | stale until the log arrives | one WAN round trip, the minimum strict close-to-open allows |

A writer's acknowledgement waits for recalls only on inodes another
node holds a delegation on; a silent delegate costs up to its TTL plus
the margin (6 s at the defaults).

### Known gaps

- A directory listing is satisfied only by the applied position, so an
  `ls` after a refusal waits even when speculation covers the entry.
- A degraded strict read can take the ReadIndex budget plus a session
  wait.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `--cto bounded\|strict` | `bounded` | the mode |
| `CONSTELLATION_CTO` | unset | default for `--cto` |
| `CONSTELLATION_SESSION_WAIT_MS` | `2000` | session wait bound; `0` disables the wait |
| `CONSTELLATION_READ_INDEX_BUDGET_MS` | `2000` | how long a strict read waits for the sequencer |
| `CONSTELLATION_READ_DELEGATIONS` | on | grant read delegations (as sequencer) |
| `CONSTELLATION_READ_DELEGATION_TTL_MS` | `5000` | read delegation lifetime |
| `CONSTELLATION_LOG_STREAMS` | on | direct log streams |
| `CONSTELLATION_KERNEL_INVALIDATE` | on | push kernel invalidations for foreign applies |

The log-stream tuning knobs are in
[Configuration](../configuration.md#sessions-close-to-open-and-log-streams).

## Status

`constellation status` (JSON) reports:

- `session`: `budget_ms`, `reads`, `fast`, `covered`, `waited`,
  `timeouts`, `degraded_held`, `replay_blocked`, `waits_ms` (a
  histogram), `wait_ms_total`, `raised`;
- `cto`, reader side: `strict`, `grants_enabled`, `strict_reads`,
  `holder_local`, `delegation_local`, `read_index`, `s3_tail`,
  `degraded`, `read_index_ms`, `renewals`, `delegations_installed`,
  `delegations_raced`, `delegations_held`, `recalled`;
- `cto`, sequencer side: `read_index_served`, `read_index_refused`,
  `grants`, `live_grants`, `recalls_sent`, `recalls_acked`,
  `recalls_expired`, `recall_waits`, `recall_wait_ms_total`,
  `held_replies`, `held_retries`, `fuse_writes_recalled`,
  `parked_acks`, `recalls_in_flight`;
- `log_stream`: `enabled`, `upstream`, `live`, `buffered`, `applied`,
  `tail_skips`, `subscribes`, `refused`, `ended`, `gaps`, `lost`,
  `timeouts`, `overflows`, `serving`, `served`.

## Troubleshooting

### Reads are slow after a refused write

`session.waited` and `waits_ms` rise: the node's client saw a refusal
whose cause is not in the local replica yet, and reads wait for it. If
`session.timeouts` rises too, the replica is not catching up: check
`log_stream.live` and S3 tailing.

### Strict opens take a round trip every time

Check `cto.delegation_local` against `cto.read_index`. Delegations are
not granted when the sequencer has `CONSTELLATION_READ_DELEGATIONS=0`,
inside a continuation epoch, or with P2P off. A file that another node
writes all the time is recalled all the time (`cto.recalled`).

### A write on one node stalls for seconds

`cto.recall_waits` and `recall_wait_ms_total` on the sequencer rise: a
strict reader holds a delegation on what the write touched and does not
answer the recall (suspended, partitioned). The write waits up to the
delegation's TTL plus the margin.

### `cto.degraded` rises

The sequencer did not answer within the budget: it is overloaded,
unreachable, or in a takeover gate. Degraded reads are bounded-mode
reads.

## FAQ

- **Does bounded mode ever show a node its own writes late?** No:
  read-your-writes holds in both modes.
- **Does strict mode make `stat` strict?** `lookup`, `open` and the
  first `readdir` chunk are strict. A `getattr` on an inode the kernel
  already resolved gets the session guarantees only.
- **Does strict mode need P2P?** It works without it, but degrades to
  "every close whose records are in the log" (see
  [Without P2P](#without-p2p)).

## References

- Plan 30 §M6–§M8 ([plan](../../plans/v1/wip/30-write-path-resilience-and-scale-out.md))
- [ADR-20](../../explanation/DECISIONS.md#adr-20-positions-session-guarantees-and-two-close-to-open-modes)
- [`crates/meta/src/session.rs`](../../../crates/meta/src/session.rs),
  [`crates/meta/src/readdeleg.rs`](../../../crates/meta/src/readdeleg.rs),
  [`crates/authority/src/core/readindex.rs`](../../../crates/authority/src/core/readindex.rs),
  [`crates/authority/src/core/stream.rs`](../../../crates/authority/src/core/stream.rs),
  [`crates/cli/src/cto.rs`](../../../crates/cli/src/cto.rs)
- Models: `crates/model/src/positions.rs`, `crates/model/src/cto.rs`;
  harness: `session-*`, `visibility-after-burst`, `cto-*`,
  `chaos-ci-strict`
- [Forwarded mutations](forwarded-mutations.md),
  [Durability and failover](durability-and-failover.md)
