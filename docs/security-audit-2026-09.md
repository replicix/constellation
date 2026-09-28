# Constellation security audit — September 2026

Scope: an extensive security review and fuzzing campaign across every channel
that ingests bytes Constellation does not itself produce — the iroh P2P fast
path (QUIC direct streams + iroh-gossip), the control-plane HTTP/Unix API, the
FUSE filesystem, and the decoders for postcard/zstd data fetched from S3 or
peers. All findings below were fixed on branch `claude/wizardly-planck-8xavdr`;
each fix ships with regression tests.

## Method

- **Attack-surface mapping** of `crates/net`, `crates/api` + `crates/cli` (web
  + FUSE wiring), and every `bytes -> Result<T>` decoder in `crates/store-s3`,
  `crates/fs-core`, `crates/mtree`, `crates/meta`.
- **Coverage-guided fuzzing** with `cargo-fuzz` / libFuzzer: 14 structure-aware
  targets under `fuzz/`, seeded and run with `-rss_limit_mb` / `-malloc_limit_mb`
  so an unbounded allocation is reported as a finding rather than swallowed.
- **Threat model.** P2P peers are semi-trusted: enrollment requires a write to
  the S3 node registry, so IAM is the trust root. The audit still treats an
  *enrolled-but-malicious* peer as in scope (a compromised node, or a node
  behaving badly), because the codebase itself already defends that boundary in
  places, and any inconsistency there is a real gap. Anyone with S3 write access
  to the bucket can plant a crafted object, so every decoder of bucket bytes is
  reachable by "any bytes at all", not only by a holder of valid content. The
  localhost HTTP API is unauthenticated *by design*; the audit treats the
  browser-based remote attacker (DNS rebinding) as in scope because binding to
  loopback does not exclude them.

## Findings and fixes

Severity reflects impact on *this* system given the trust model above
(availability/DoS unless noted; none reached data corruption, thanks to S3
remaining the commit point and every fetched chunk being hash-verified).

### 1. `Tree::decode` — unbounded allocation → process abort (HIGH)

`crates/fs-core/src/tree.rs`. `Tree::decode` and `read_xattrs` read a `u32`
element count from a snapshot-tree object and immediately called
`Vec::with_capacity(count)`. A count near `u32::MAX` reserves hundreds of GB and
aborts the process in `handle_alloc_error` (an *uncatchable* abort, worse than a
panic). Reachable from a single corrupt or maliciously-written `.snapshots`
object. **Independently reproduced by the `fscore_tree` fuzz target** (OOM,
`malloc(1.5 GB)` from a 12-byte input claiming 33.5M xattrs).

Fix: bound the reservation by what the remaining input could actually encode
(`count.min(input.len() / MIN_ENTRY_BYTES)`), the same discipline
`manifest::decode_chunk_list` already used. A larger count is guaranteed
truncated and fails cleanly in the decode loop.

### 2. zstd decompression bombs — unbounded output (HIGH)

`crates/store-s3` (`codec.rs`, `format.rs`, `log.rs`, `inbox.rs`, `packs.rs`).
Every zstd path used `zstd::decode_all` with no output bound and checked the
decompressed length only *afterwards*. A crafted frame a few bytes long expands
to gigabytes, exhausting memory before the check runs. Chunk objects, log
segments, inbox batches, and pack node frames were all affected; the streaming
chunk path wrote unbounded bytes into the on-disk cache before `finish` noticed.

Fix: a single `codec::decompress_bounded(payload, max_out)` choke point that
streams through the decoder capped at `max_out + 1` bytes, rejecting
over-expansion as it is produced. Each caller passes a domain ceiling
(`MAX_DECOMPRESSED_LEN` 256 MiB for chunks, 64 MiB segments, 16 MiB inbox
batches, 4 MiB pack nodes). `StreamingDecoder` refuses an over-ceiling declared
length up front and aborts mid-stream once output exceeds the header length.

### 3. HTTP API reachable via DNS rebinding (HIGH)

`crates/api/src/web.rs`. The control API binds `127.0.0.1` with no auth by
design, but a malicious web page can reach it through DNS rebinding (the browser
same-origin check is IP/port-based; cf. CVE-2025-49596). Every mutating endpoint
— `FsckRun{repair}`, `PruneRun`, `GcRun`, `SnapshotDelete`, `Leave`, `MountAdd`
(mount FUSE over an arbitrary path), `SetQuota`, `ForceRelease`, `DropHeld` — was
callable this way.

Fix: a `Host`/`Origin` guard middleware that 403s any request whose `Host` is
not a loopback authority (`localhost` / `127.0.0.1` / `[::1]`, with or without a
port) or whose `Origin`, when present, is non-loopback. `router()` was factored
out of `serve()` so the stack is testable in-process (7 guard tests).

### 4. P2P claimed-id spoofing / unbounded map growth (MEDIUM)

`crates/net/src/peers.rs`. The gossip handlers for `CacheDigest`,
`CacheDigestDelta`, `PeerRtts`, and `MutateRequest` used a `node_id`/`requester`
field from the message body *without* binding it to the Ed25519 signer — unlike
the sibling `CacheSummary`/`CacheSetDelta` handlers, which do. An enrolled peer
could impersonate another node's cache digest, or fabricate unlimited distinct
fake node ids to grow the per-peer digest map (up to ~4 MiB/entry) without bound.
For `MutateRequest`, a forged `requester`/`acked_through` could disturb another
node's exactly-once dedup bookkeeping.

Fix: bind every claimed id to the signer (`node_id_for_key(&hex) == Some(id)`)
before dispatch, matching the existing pattern; drop with a log on mismatch.

### 5. FUSE mutex-poison cascade (MEDIUM)

`crates/cli/src/fusefs.rs`. `WriteShards::lock` / `InodeOps::lock` and their
`Drop` used `.lock().unwrap()` / `panic!` on a poisoned mutex. A single panic
while holding one shard's lock poisons it, and every later op on any inode
hashing to that shard then panics too — cascading across worker threads until
that inode range is permanently wedged (and, given how FUSE interacts with the
kernel, potentially an unkillable mount, not just a crash).

Fix: recover with `.unwrap_or_else(|e| e.into_inner())` (the guarded maps stay
structurally valid), matching the existing `fuse_watch` convention.

### 6. Miscellaneous hardening (LOW)

- **Overlong varints** (`crates/net/src/reconcile.rs`): `get_varint` accepted
  non-minimal encodings, so two distinct byte strings decoded to the same key
  set (wire malleability). **Found by the `net_reconcile_keys` fuzz target's
  round-trip invariant.** Now rejects a redundant trailing-zero continuation.
- **Delta key cap** (`reconcile.rs`): `Mirror::apply_delta` now enforces
  `MAX_DELTA_KEYS` on receive (was sender-side only).
- **Inbound stream read deadline** (`peers.rs`): a peer can no longer hold a
  per-connection stream permit open indefinitely without sending a frame (30s).
- **Gossip head-of-line blocking** (`peers.rs`): a slow gossiped `MutateRequest`
  no longer stalls all other gossip processing (handled off the receive loop).
- **`MountAdd.fuse_threads`** (`cli/src/main.rs`): rejected outside `1..=1024`
  instead of spawning an unbounded thread count.
- **Unix control socket** (`api/src/lib.rs`): unbounded line buffering replaced
  with an 8 MiB-capped `read_until`.
- **`do_write` overflow** (`cli/src/fusefs_ops.rs`): `offset + len` → `checked_add`
  (EFBIG), and `rename` now enforces `NAME_MAX` like every other name op.
- **Replay accounting overflow** (`meta/src/replay.rs`): usage-counter deltas go
  through a saturating `size_delta`, so a hostile inode `size` near `u64::MAX`
  can neither go silently negative nor panic at `i64::MIN`.

## Fuzzing campaign

14 targets over the untrusted decoders. Two crashes, both fixed above; the rest
ran to their time budget with no crash, giving good confidence in the decoders
they cover.

| Target | Result |
|---|---|
| `fscore_tree` | **OOM** — finding #1 (fixed) |
| `net_reconcile_keys` | **crash** — finding #6 overlong varint (fixed) |
| `net_payload_postcard` | clean (cov ~4459; whole `Payload` enum) |
| `net_signed_decode` | clean (cov ~861) |
| `net_reconcile_respond` | clean (cov ~1164) |
| `net_reconcile_session` | clean (cov ~744) |
| `net_bloom` | clean (cov ~114) |
| `fscore_manifest` | clean (cov ~202) |
| `mtree_node` / `mtree_record` | clean |
| `store_object` / `store_inbox_pack` | clean |
| `api_request` / `meta_blobs` | clean |

Reproduce: `cd fuzz && cargo +nightly fuzz run <target>`.

## Dependency posture

The most severe recent advisories in this stack are already patched in the
lockfile: `quinn-proto 0.11.15` (past CVE-2026-31812 parse-panic and
CVE-2026-25800 assembler memory exhaustion), `h2 0.4.18` (past RUSTSEC-2026-0258),
`fuser 0.18` (past RUSTSEC-2021-0154), `rustls 0.23.43` (past RUSTSEC-2026-0285).
Recommend wiring `cargo audit` / `cargo deny check advisories` into CI — several
of these landed within the last few months.

## Verified defenses (no change needed)

The codebase is unusually disciplined on the peer-input path; the audit
confirmed, among others: the 4-byte P2P frame cap checked before allocation and
`Signed::decode` refusing trailing bytes / non-64-byte signatures; signature
verified before dispatch and the author bound to the transport key on direct
streams; log-segment and chunk bodies length-capped before allocation and
blake3-verified; `bloom::from_wire_bytes` validating `k`/`nbits`/length; the
allowlist gating gossip and connections with rate-limited miss-refresh; the axum
2 MiB default body limit; `meta::resolve_path` treating `..` as a literal dentry
(no host traversal); and the `mtree` node/record/key decoders being total and
bounds-checked at the untrusted boundary.
