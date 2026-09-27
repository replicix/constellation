# Write-path hygiene

How Constellation reads S3's answers to a conditional write, what
`constellation doctor` reports about a provider, how one lost chunk is
kept from blocking every other file's metadata, and which node publishes
metadata commits (plan 30 milestone 4).

## Table of Contents

- [Terminology](#terminology)
- [Details](#details)
  - [Conditional writes and their error codes](#conditional-writes-and-their-error-codes)
  - [`constellation doctor`](#constellation-doctor)
  - [Held records (`status.held`)](#held-records-statusheld)
  - [`constellation repair drop-held`](#constellation-repair-drop-held)
  - [Who publishes commits](#who-publishes-commits)
  - [Peer paths (`status.p2p.peers[].paths`)](#peer-paths-statusp2ppeerspaths)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [References](#references)

## Terminology

- **CAS site**: a place where Constellation commits state with one
  conditional PUT: a lease create or swap, a log segment (including a
  takeover's epoch marker), a metadata commit, a designation, a node
  registry claim, the condemned-chunk pointer, and content-addressed
  creates (chunks, blobs, packs).
- **Unrecoverable chunk**: a chunk a pending upload names that is gone
  from the local cache, so it can never be uploaded.
- **Held record**: a journal record that is not shipped because it needs
  an unrecoverable chunk, directly or through a key it shares with one
  that does.
- **Lease holder**: the one node allowed to append to the metadata log.

## Details

### Conditional writes and their error codes

Every CAS site handles the answers distinctly:

| Answer | Meaning | What Constellation does |
|---|---|---|
| success | the write landed | done |
| `409 Conflict` | another conditional write on the key is in flight; this one did not happen | retries the *same* attempt (same body, same precondition), up to `CONSTELLATION_CAS_BUSY_RETRIES` times (default 5, 50 ms doubling to 1 s) |
| `412 Precondition Failed` | the key exists (create) or moved (swap): a lost race | reads the object back; if it is byte for byte the body just written, the write is ours (an earlier attempt landed behind a retried 5xx or a lost reply), otherwise the race is lost and the caller re-reads |
| `404` on `If-Match` | the object the swap expected is gone | same as a lost race: re-read |
| 5xx after retries, timeout | unknown | an error: the caller retries later; a lease renewal never concludes it was deposed from one |

For content-addressed creates (chunks, blobs, packs) an existing object
is a successful dedup, but a `409` is not: it is retried, so a chunk is
never counted as stored when nothing was written.

A re-adoption of a lease the bucket says is this node's own — a restart,
or a takeover whose reply was lost although the write landed — is gated
like a takeover: the node tails to the head, ships an epoch marker, and
replays anything stranded before it opens for writes.

**Limitation (`object_store` 0.14).** Its S3 client reports a `409` on a
create as `AlreadyExists`, the same variant as a `412`, and a `404` on a
swap as `Precondition`, the same as a `412`. Constellation tells them
apart from the wrapped error (a `412` on create wraps another
`object_store` error; the HTTP status is read from the error text). If a
future `object_store` words it differently, a `409` on create degrades to
a lost race, and the read-back finds the key empty and retries: still
correct, one GET slower.

### `constellation doctor`

Besides "does `If-None-Match`/`If-Match` work at all", `doctor` records
what the provider answers at each edge and warns on anything the rules
above do not know:

| Probe | Known answers |
|---|---|
| create over an existing key | `412` (AWS, MinIO, R2), `304` → lost race |
| `If-Match` with a stale etag | `412` → lost race |
| `If-Match` on a missing key | `404` (AWS) → missing, `412` (R2, MinIO) → lost race |
| 8 concurrent creates of one key | exactly one success; the rest `412` or `409` |
| 8 concurrent swaps from one etag | exactly one success; the rest `412` or `409` |

An unknown answer prints a warning. Two winners in a concurrent probe is
a **violation** — the provider does not enforce the precondition
atomically — and `doctor` fails: leases, segments and commits would not
be exclusive.

`doctor` also reports **bucket versioning**, read from the probe PUT's
version id (no extra request, no bucket-level API): `enabled`,
`suspended`, or `off (or not reported)`. It is informational; nothing
relies on it. With versioning on, every object GC deletes stays as a
noncurrent version until a lifecycle rule expires it.

The control API's `doctor` request returns the same data
(`cas_probes`, `versioning`).

### Held records (`status.held`)

A pending chunk that is gone from the local cache used to fail every
sync round on the node, so no metadata of any file shipped again. Now the
upload pass records it, and each ship holds back only:

- the transactions whose manifest names an unrecoverable chunk (the
  seeds), and
- every later transaction that touches a key a held one touched
  (transitively) — for example a `chmod`, rename or unlink of that file.

Everything else ships. A held transaction stays ordinary speculation: the
node's own replica shows it, published commits show the file as it was
before it, and a deposition rolls it back and replays it through the new
holder (where it is held again). While anything is held, the node's
journal is not empty, so it keeps the lease; other nodes keep writing
through it.

`status.held`:

| Field | Meaning |
|---|---|
| `transactions`, `records` | what the last ship round held back |
| `oldest_seq` | the oldest held journal sequence |
| `opaque` | holder capture is off, so a held transaction's keys are unknown and everything after it is held too |
| `inodes[]` | each inode with unrecoverable chunks: `ino`, `path`, `missing_chunks` (hex), `seeds` |
| `deferred` | transactions waiting (not held) for a chunk still uploading — this node's, or one another node forwarded as pending |
| `remote[]` | each chunk another node is expected to upload: `ino`, `path`, `node`, `chunk` (hex), `age_s` |

The web UI shows a red "held" line with the paths, and
`constellation_held_transactions` is exported as a metric.

### `constellation repair drop-held`

```bash
constellation repair drop-held myfs <ino>
```

Discards an inode's held records:

- each seed (the manifest naming the lost chunk) becomes a conflict copy
  `<dir>/.constellation-conflict/<name>@<node>-<ts>`, full length, with
  the lost chunks as holes (zeros); a spilled manifest's copy keeps only
  the length;
- every held transaction that depended on it is rolled back and replayed
  by its request id, exactly like a deposed holder's journal; a replay
  the namespace no longer admits becomes a conflict copy as well;
- the unrecoverable pending uploads are removed.

The copies and replays are made by the replay drain within a fraction of
a second. A copy that cannot be made yet (no reachable holder, for
example during an S3 outage) is retried with backoff (250 ms doubling to
10 s) without holding up later replays; after 10 s the node asks for the
lease to make it locally, and `status.speculation.copies_pending` /
`copies_stalled` (and the `constellation_speculation_copies_stalled`
metric, and a red dashboard note) show it. A copy is never dropped. It needs holder capture on (the default); with it off the
command refuses, because uncaptured records cannot be rolled back.

The dropped op's outcome is journaled as a refusal (`Refused`, `EIO`)
and ships with the log: a node that still holds the write as
speculation — its requester's shadow, or a copy streamed ahead of the
log — rolls it back when the refusal arrives, and a retry by request id
is answered refused everywhere. Nothing is lost silently: the copy is
the artifact.

### A chunk only a departed node had

A manifest another node forwarded before its chunks were in S3 (a
`--write-mode back` close, or any write inside a continuation epoch,
where nothing reaches S3 until the close) is *deferred* on the
sequencer, not held: `status.held.deferred` counts it and
`status.held.remote` names the chunk and the node expected to upload it.
It ships by itself once that node's upload pass puts the chunk up (or
the sequencer finds it in S3). Everything that does not depend on it
keeps shipping.

If that node is gone for good — its disk with it — the write can never
complete. The procedure:

1. On the sequencer, `constellation status` → `held.remote`: note the
   `ino`, `path`, `node` and `age_s` of each awaited chunk. An `age_s`
   growing past any plausible return, with the node absent from the
   registry, is the signal; a node that merely rebooted uploads its
   chunks when it is back, and nothing needs doing.
2. Decide per inode. `constellation repair drop-held myfs <ino> --remote`
   declares that inode's remote chunks unrecoverable and then does what
   `drop-held` does: the manifest becomes a conflict copy with the
   missing chunks as holes, its dependents are rolled back and replayed,
   the pending rows go, and the refusal ships. Run it on the node that
   lists the inode; a node that took the lease over after a deposition
   may list the same write again (the deposed node's replay forwarded
   it on) and drops it the same way.
3. If the node comes back after all, its chunks are still content
   addressed: a new write of the file uses them, but the dropped write
   stays a refusal (its requester's copy of it was rolled back; the
   conflict copy has what survived).

Never use `--remote` for a node that will return: its write would ship
by itself, and the drop turns an acknowledged write into a refusal.

### Who publishes commits

Only the lease holder publishes metadata commits: on its segment cadence,
on its idle timer, and at clean unmount. Every other node runs the idle
timer too, but only probes the chain head; once the head commit covers
its applied position (and it has nothing speculative or journaled) it
clears the dirty marks that commit already reflects. An explicit
snapshot still publishes on any node. See
[Configuration → Publish cadence](../configuration.md#publish-cadence).

### Peer paths (`status.p2p.peers[].paths`)

See [P2P relays → Status surface](p2p-relays.md#status-surface) for the
fields and for how a direct path fails over to a relay.

## Troubleshooting

### `status.held` lists an inode

The file's latest content was lost before it reached S3 (its cache file
disappeared while its upload was pending). Other files are unaffected.
If the application can rewrite the file, do so and then run
`constellation repair drop-held` for the inode; otherwise run it to keep
what is left (the conflict copy has the surviving chunks). The node
cannot release its lease while anything is held.

### `doctor` warns about unknown semantics

The provider answered a CAS edge with a status the rules do not know
(for example a `500` for a stale `If-Match`). Constellation will treat
such an answer as a transient error, which is safe but may slow lease
changes. Report the provider and the line.

## FAQ

**Is bucket versioning required or recommended?** Neither. Constellation
never reads old versions.

**Does a `409` ever mean the write happened?** No: S3 returns it when
another conditional write on the same key is in progress, before
applying this one. The ambiguous case is a `5xx` or a timeout, which the
read-back covers.

## References

- `docs/plans/v1/wip/30-write-path-resilience-and-scale-out.md` §M4
- `crates/store-s3/src/cas.rs`, `crates/store-s3/src/probe.rs`
- `crates/meta/src/store/held.rs`
- `crates/cli/src/mtree_publish.rs` (`TreePublisher::follow_head`)
- `crates/net/src/paths.rs`
