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
| `409 Conflict` | another conditional write on the key is in flight; this one did not happen | retries the *same* attempt (same body, same precondition), up to `REPLICIX_CAS_BUSY_RETRIES` times (default 5, 50 ms doubling to 1 s) |
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
through it, and a peer that wants the lease does not pause this node's own
writes for a backlog that only an operator can drain: a round that ships
nothing while rows are held or deferred does not renew the handoff pause.

`status.held`:

| Field | Meaning |
|---|---|
| `transactions`, `records` | what the last ship round held back |
| `oldest_seq` | the oldest held journal sequence |
| `opaque` | a held transaction was journaled without before-images (a write from before this node held the lease), so its keys are unknown and everything after it is held too |
| `inodes[]` | each inode with unrecoverable chunks: `ino`, `path`, `missing_chunks` (hex), `seeds` |
| `drop_sessions_deferred` | open write sessions (or flushes in flight) naming a lost chunk that `repair drop-held` could not discard since start (the file was busy, or a flush had the session detached); their chunks stay held, nothing is lost: run the drop again |
| `deferred` | transactions waiting (not held) for a chunk still uploading — this node's, or one another node forwarded as pending |
| `remote[]` | each chunk another node is expected to upload: `ino`, `path`, `node`, `chunk` (hex), `age_s` |
| `awaiting[]` | each row of another node that `repair drop-held` rolled back and whose answer was never given, waiting for its requester's retry (see below): `node`, `rid`, `age_s`, `keys` |

The web UI shows a red "held" line with the paths, and
`constellation_held_transactions` (and `constellation_held_awaiting_retry`)
are exported as metrics.

### `constellation repair drop-held`

```bash
constellation repair drop-held myfs <ino>
```

Discards an inode's held records:

- each seed (the manifest naming the lost chunk) becomes a conflict copy
  `<dir>/.constellation-conflict/<name>@<node>-<ts>-<seq>`, full length, with
  the lost chunks as holes (zeros); a spilled manifest's copy keeps only
  the length;
- of the held transactions that depend on it by key, only the ones that need
  the lost *content* are rolled back with it (a later write of the damaged
  file, a size change of it, a copy of it): this node's own are replayed by
  their request ids, exactly like a deposed holder's journal, and a replay the
  namespace no longer admits becomes a conflict copy as well. Every other one
  (a rename, chmod or unlink of the file, a create or unlink in its
  directory) neither wrote nor read a byte the loss took: it is redone in
  place, with its journal row, request id and answer, and ships once the seed
  is gone (`redone` in the command's account). Another node's rolled-back
  row is not replayed here (see below). A dropper that is not the sequencer
  in force rolls back everything that depended on the seed, as a deposition
  does;
- the unrecoverable pending uploads are removed -- except those of a chunk
  an operation of this node, or a write session, still names (below).

The copies and replays are made by the replay drain within a fraction of
a second. A copy that cannot be made yet (no reachable holder, for
example during an S3 outage) is retried with backoff (250 ms doubling to
10 s) without holding up later replays; after 10 s the node asks for the
lease to make it locally, and `status.speculation.copies_pending` /
`copies_stalled` (and the `constellation_speculation_copies_stalled`
metric, and a red dashboard note) show it. A copy is never dropped. A held transaction journaled without before-images
(a write from before this node held the lease) makes the command refuse: it cannot be rolled back.

What the client of a dropped operation hears is the ordinary in-doubt
answer: `EIO`, as for any operation whose outcome is not yet known. The
operation itself may be replayed by its request id after the drop (and
land in the log then, behind later operations of the same client), so a
client that sees `EIO` and moves on can still find the operation's effect
later; a client that retries by request id gets the replay's outcome. An
operation the client was already told `OK` for (under a policy that
acknowledges before the effect is durable) is not excused by the drop: it
is the ordinary rolled-back acknowledgement.

The dropped op's outcome is journaled as a refusal (`Refused`, `EIO`)
and ships with the log: a node that still holds the write as
speculation — its requester's shadow, or a copy streamed ahead of the
log — rolls it back when the refusal arrives, and a retry by request id
is answered refused everywhere. Nothing is lost silently: the copy is
the artifact.

**A lost chunk an operation still names stays held.** "No pending row" means
durable to everything that composes or ships a manifest, so the drop does not
remove the row, the mark and the remote mark of a lost chunk that an
operation of this node that has not journaled yet still names -- a close
waiting behind a handoff pause, a forward the holder has not answered, an
operation parked behind the replay queue, a delegate's parked operation --
or that an open write session of the file has sealed (a session in a flush
in flight keeps every lost chunk of the file). Its manifest would otherwise
ship naming a chunk S3 never received: a permanent `EIO` for every reader,
the writer told OK, no conflict copy. The command says so ("N lost chunk(s)
are still named by ... kept held; run drop-held again once they have
journaled") and the operation that executes later is a seed of the next drop.
After the drop the node discards each open write session that sealed a lost
chunk, as a lapsed lock's writes are: its bytes are gone with the chunk, and
every open description reports `EIO` once at its next close or `fsync`. A
session the drop cannot touch at that moment (another thread holds the file's
operation lock, or a flush has the session detached) is not discarded: the
command says how many, `status.held.drop_sessions_deferred` counts them, their
chunks stay held and the manifest they commit is held as a seed of the next
drop. Operations the drop itself rolled back keep nothing held (their rows are the
seeds). Another node's operation names this node's chunks only through the
pending list it forwards (`--remote`).

**The drop is decided in the log.** `repair drop-held` decides nothing
locally: it journals one `DropHeld` record (an archive-class row with no keys,
so it ships ahead of everything the seed holds) naming each transaction the
drop will roll back as a member keys it (the `TailOf` pair if the transaction
began with one, else its tenure's epoch and first journal sequence) and the
request id it completes, and persists the plan. The command answers
`pending` until the archive carries the record; only then does the node
apply it (strand the seed, redo the rest, refuse the queued seeds, write the
`Refused` rows, remove the pending rows), and the command prints the final
account. The wait is one archive round trip. Every replica applies the record
from the log: it records the named identities as retired (the takeover's
re-ship and the kept-tail merge skip a retired transaction, so a late copy a
member served can never land behind its own refusal), a member's copy and a
backup's tail transaction of one become empty transactions of the same span,
and what a member was streamed of one as committed is rolled back without a
replay (this node's queue owns the replay or the refusal). A parked answer
of a request id the drop refused is settled with the refusal, and any parked
answer is what the store says when it is released, never a snapshot.

**A dropper without the lease decides nothing.** The command on a node that is
not the sequencer in force is refused before anything changes ("run it on the
holder"): its held rows are what a deposition strands anyway, and the
successor re-ships their copies. A node deposed (or dead)
before the record shipped leaves a sealed tail that differs from the members'
copies by that record alone: the successor's takeover skips it (`tail_drop_fenced`,
warned) and re-ships the rows it named as the acknowledged rows they are; the
command answers that the lease was lost and nothing was dropped. "In the log"
means "shipped by this node": the plan records it when the record's row is
acknowledged, so a restarted dropper applies a record that shipped at startup
and cancels one that did not once the log has closed its tenure. A node that
dies after the record shipped but before it applied the drop (the one
residual) leaves the seed's rows retired (nothing lands) and no conflict copy
and no `Refused` row for them: the lost write's surviving bytes are not
preserved as an artifact, and a requester's retry by request id executes
afresh from the requester's own bytes. It is counted
(`drop_decided_unapplied` in the simulation, `tail_retired` at the successor's
gate) and accepted as the operator's cost of a holder death inside a drop.

**Acknowledged operations the drop cannot avoid undoing.** A seed's own
acknowledged write, and an acknowledged operation that needs the lost
content, are rolled back for good. The command lists each (`acked_lost`),
the node warns once per request id, and `status.held.acked_lost_total`
counts them: the documented double fault of a lost chunk plus an
acknowledged operation that needs it. A redone row whose client was told in
doubt lands when the operator's drop releases it, as a queued replay lands
when the drain runs it; a client that retried under a new request id gets two
effects, as with a replay. A kept in-doubt create keeps its inode allocated:
a requester's retry under a new request id allocates afresh.

### Another node's rows

An operator's drop re-executes only this node's own rows. A row of another
node that the drop rolled back and whose answer was certainly never given
belongs to its requester: that node's client entry retries the request id
until its deadline, and a second copy queued here would run twice. It stays
in the replay queue instead, in the place it had, as a *placeholder*
(`status.held.awaiting[]`): its request id, the keys it touched, no op to
run.

*Certainly never given* is what the sequencer can know. The drop runs in the
node's core (`constellation repair drop-held` is a request to it), which
knows two things: a lone sequencer answers another node only once the log
carries its row, and a held row never ships, so the answer of an `Archive`
row was never given; and a row journaled for the backup group has its answer
parked in the core until the group holds it, so a group row whose answer is
still parked has not been answered either (that parked answer is aborted,
`Busy`, in the same step, so the retry comes at once). Nothing else is known:
a group row whose answer has left (or may have: the core was restarted since,
and its parked answers are gone), an epoch hold's row and this node's own
keep their replayable copy.

- What depends on it waits. The replay drain holds every later entry on
  those keys behind the placeholder (entries on other keys run), and an op
  that arrives from another node on those keys waits for the queue entries
  ahead of it, answered `Held` meanwhile.
- The requester's retry executes in the dropped row's place — after the
  entries before it, before the ones after — and resolves the placeholder;
  what waited goes on. That holds while the dropping node remains the
  sequencer. If another node has taken the lease over by then, the retry
  goes to it and executes there with no queue of this node's in front of
  it: ungated, in the order it arrives. No acknowledged effect is lost either
  way (the placeholder was an unanswered row, and this node's own copies
  replay by rid to whoever holds the lease); only the position the retry
  would have taken is not kept.
- If the retry never comes (the requester is gone, or gave up and settled),
  after `CONSTELLATION_DROP_HELD_AWAIT_MS` (default the cap, floored at one settle window: the client
  deadline minus two settle windows, 60 s at the default TTL, and never more;
  this node's own operations on the placeholder's keys wait behind it, and
  must still have their window left when it ends; the clock stands still
  while a continuation epoch refuses writes) the
  sequencer in force settles the request id: `Refused { rid, ECANCELED }`
  in the log, committed like any refusal, counted in
  `status.ack.await_retry_settled`. The op never takes effect (a late retry
  is answered as the settled `EIO`), and the dependents go on: a success
  on the state without the dropped row, or a refusal that is a conflict copy
  for an acknowledged op of this node.

A row whose answer may have been given (its backup group's, once released; an
epoch hold's) keeps its replayable copy: its requester heard the answer and
is not retrying. A copy of another node's op kept this way and the
requester's own retry (it never got the answer) are one op to the sequencer:
the retry waits behind the copy where it is queued, and anywhere else is
answered from the rid's outcome.

An op from another node, or the sequencer's own, that arrives on keys a queue
entry ahead of it holds waits for the queue (`status.ack.parked_behind_queue`);
an inbox batch whose op must wait halts at it — the batch is not cut, ops
behind it in the same requester's order wait too — and the halts are counted
(`status.ack.inbox_halted`). The wait ends when the entries ahead are done or
settled; if the node loses the lease, the parked ops are answered `Busy` at
once and their requesters retry. Time an op spends parked behind the queue
does not count toward `replay_lease_fallback_ms`.

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

- `docs/plans/v1/done/30-write-path-resilience-and-scale-out.md` §M4
- `cas.rs` and `probe.rs` of `replicix-objcas`
- `crates/meta/src/store/held.rs`
- `crates/cli/src/mtree_publish.rs` (`TreePublisher::follow_head`)
- `crates/net/src/paths.rs`
