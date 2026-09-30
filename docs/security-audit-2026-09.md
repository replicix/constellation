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

Fix: every untrusted zstd reader decompresses through a reader capped at
`max_out + 1` bytes (`codec::decompress_bounded`), so over-expansion is
rejected as it is produced. The cap bounds the *output size*, never the
compression ratio, so a legitimate object that compresses extremely well (a
run of zeros) decodes like any other. What the cap is depends on whether the
object has a structural size bound:

- **Chunk objects** carry a declared length: output is capped at exactly that
  length, and the declared length itself must be under a memory-safety
  ceiling (`codec::max_decompressed_len`, default 1 GiB, env
  `CONSTELLATION_MAX_DECOMPRESSED_BYTES`).
- **Log segments and inbox batches** have no length header and no structural
  bound, so they decompress to that same ceiling (`decompress_to_ceiling`).
  A first version of this fix capped them at 64 MiB and 16 MiB from design-doc
  typical sizes; review against the producers showed legitimate objects past
  both: a subtree clone journals as one `Clone` record that grows with the tree
  (and a transaction larger than the 4 MiB segment target still ships whole),
  and a full inbox batch of 512 `SetXattr` ops is ~32 MiB. Refusing a real log
  segment would stall replay on every replica, so the cap is generous,
  configurable, and its error names the variable. Likewise a spilled chunk
  list is one chunk object of 40 bytes per data chunk, which is why the chunk
  ceiling is not tied to the 64 MiB chunk size.
- **Pack nodes** are structurally bounded (≤ 256 entries, leaf values above
  1 KiB spilled to blobs), so they keep a fixed 4 MiB cap.

`StreamingDecoder` (the stream-to-cache-file path) enforces the declared length
inside its writer, per write, so a bomb delivered in a single network read never
reaches the cache file; the first version checked only between reads.

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

## Round two

A second pass over what the first round had not covered: the direct-stream
P2P handlers whose identity fields were left unbound, the pre-authentication
QUIC handshake path, the E2E encryption layer, every remaining object read
back from the bucket (`meta.json`, the node registry, leases, designations,
snapshots, the GC pointer, heartbeats, delegation tables), an adversarial
review of the round-one diff itself, and `cargo audit` against the lockfile.

### 7. `meta.json` chunk size trusted on load (HIGH)

`crates/store-s3/src/store.rs`. `validate_chunk_size` ran only at `fs create`;
`load_fs` returned whatever the bucket held. Every read and write path divides
by `chunk_size` (`ChunkLayout::chunk_count`/`slices`, and the FUSE write path
directly), so `"chunk_size": 0` in a corrupt or hostile `meta.json` panicked
each node the moment it touched a file — a cluster-wide crash loop from one
object — and `u32::MAX` drove 4 GiB allocations per chunk read.

Fix: validate on load; an invalid `meta.json` fails the mount with an error.

### 8. P2P identity binding, completed (MEDIUM)

`crates/net/src/peers.rs`. Round one bound the claimed node id to the signing
key for four message types; every other request that names its sender
(`BackupAppend.holder`, `DelegRecall.root`, `LockRecall.owner`,
`ReadRecall.holder`, `DelegateStream.from`, `EpochPropose.proposer`, …) still
reached its handler unchecked. Every constructor fills these fields with the
sending node's own id, so a mismatch is only ever a forgery. The check now
runs once at dispatch for all of them (`claimed_node_id`), where the
authenticated identity lives, instead of handler by handler.

Also fixed: the binding looked the sender up in the *dial* directory, which
omits a peer whose registry address does not parse even though it stays on
the allowlist — such a peer's forwarded writes would have been dropped as
forged. Identity now resolves through a separate key→node map built from
every registry record.

### 9. Pre-authentication connection tracking (MEDIUM)

`crates/net/src/endpoint.rs`. The QUIC handshake completes before the
allowlist check (which happens in the ALPN handler), and the after-handshake
hook tracked every inbound connection for later eviction — keyed by the
remote's key, holding a weak handle that pins the dead connection's
allocation. Anyone who could reach the endpoint could grow that map without
bound, one fresh random key per connection, before ever being refused. Inbound
connections are now tracked only when their key is already on the allowlist.

### 10. Registry and designation records trusted their body over their key (MEDIUM)

`crates/store-s3/src/nodes.rs`, `designation.rs`. `claim_node_id` makes the
registry *key* unique, but readers keyed on the body's `node_id`, so a second
object claiming an existing node's id could take over that id in the peer
directory (routing "node N" to another endpoint). Likewise a designation
stored at any key other than its own path's hash counted in every overlap
check yet could never be released — a permanent, cheap `offline <path>`
denial. Both readers now ignore a record whose body does not match its key
(as `heartbeat.rs` already did).

### 11. Unchecked epoch arithmetic on bucket-writable fields (MEDIUM)

`lease.rs`, `gc.rs`, `cli/singleton.rs`. `epoch + 1` on a lease or GC pointer
read from the bucket; release builds have no overflow checks, so an object at
`u64::MAX` wrapped the fencing epoch to 0 on the next takeover. Leases past
`MAX_EPOCH` (half the range) are now refused on read, the GC bump is checked,
and status arithmetic on `expires_unix_ms` saturates. `SnapshotStore::get`
also gained the version check `list` already had.

### Review of the round-one diff

An adversarial re-read of every round-one hunk against the surrounding code
found no security regression and independently re-derived two of the claims
(the LEB128 minimality proof behind the varint check; the 35-byte tree-entry
minimum, byte for byte from the encoder). Three follow-ups were taken:

- The Unix control socket's new capped reader dropped, without a reply, a
  final request sent without a trailing newline before the peer closed —
  behaviour the `lines()` reader it replaced did not have. No client in the
  tree does this; it is now answered like any other request.
- The usage counter's `AtomicI64::fetch_add` wrapped silently, so two
  hostile records with `size` near `u64::MAX` — each clamped individually by
  `size_delta` — could still wrap the total to a plausible wrong value. It now
  saturates.
- The API's `fuse_threads` band (1..=1024) was 16× looser than the CLI's own
  hard cap; both now use `FUSE_THREAD_HARD_MAX` (64).

### 12. E2E encryption: unauthenticated `meta.json` allowed a silent downgrade (HIGH)

`crates/store-s3/src/e2e.rs`, new `crates/cli/src/e2e_pin.rs`. The AEAD
construction (XChaCha20-Poly1305, random 192-bit nonces, the object path as
associated data, per-filesystem random master, keyed-BLAKE3 subkeys, keyed
content addressing) reviewed as sound. But `meta.json` — plain JSON in the
bucket — alone decided whether a mount was E2E, and E2E exists precisely to
protect data from whoever controls that bucket. Flipping `"e2e": false` made
every later mount write plaintext with no prompt and no warning; splicing in
another filesystem's keyring block (same passphrase reused) would have opened
to that filesystem's keys.

Fix: a local trust-on-first-use pin per filesystem (`registry.e2e.toml`, next
to the registry, same lock-then-rename discipline). Before any prompt or key
derivation, a pinned-E2E filesystem now reporting plaintext is refused; after
the passphrase opens the keyring, the master key it opened to must match the
pinned fingerprint, or the keys are never used. The fingerprint is of the
master key, not the keyring block, so `fs passwd` (a rewrap of the same
master) passes on every node while a spliced block — which opens to a
different master — is refused. An operator accepts a genuine change with
`CONSTELLATION_ACCEPT_E2E_CHANGE=1`, which re-pins. Wired into mount, the
daemon, `fs create`, `fs passwd`, `gc` and `fsck`.

Also in this area: Argon2id defaults raised from OWASP's memory-constrained
floor (19 MiB, t=2) to 64 MiB, t=3; per-call DEKs are
returned zeroizing; the local plaintext chunk cache directory is created
0700.

### 13. Dependencies (MEDIUM)

`cargo audit` (which the first round's research had summarized incorrectly):
`rustls 0.23.43` was vulnerable to RUSTSEC-2026-0285 (fixed in 0.23.45), and
`chacha20 0.10.1`, under the E2E AEAD, was yanked. Both bumped in the lockfile.

## Fuzzing campaign

14 targets over the untrusted decoders, ~44 min of wall time (5–7 min per
target, `-rss_limit_mb=1024 -malloc_limit_mb=512`). Two distinct bugs, both
fixed above; the other 12 targets ran their full budgets with no crash, OOM, or
timeout.

| Target | Execs | Features | Result |
|---|---:|---:|---|
| `fscore_tree` | — | — | **OOM** from an 8-byte input (`malloc(4.5 GB)`) — finding #1, fixed |
| `net_reconcile_keys` | — | — | **assertion**: overlong varint round trip — finding #6, fixed |
| `net_payload_postcard` | 3.6M | 8522 | clean (the whole ~70-variant `Payload` enum) |
| `net_signed_decode` | 8.0M | 1371 | clean |
| `net_reconcile_respond` | 1.9M | 3213 | clean |
| `net_reconcile_session` | 7.1M | 2522 | clean |
| `net_bloom` | 115.6M | 168 | clean |
| `fscore_manifest` | 66.4M | 384 | clean |
| `mtree_node` | 92.2M | 559 | clean |
| `mtree_record` | 21.1M | 564 | clean |
| `store_object` | 25.6M | 140 | clean |
| `store_inbox_pack` | 1.4M | 235 | clean |
| `api_request` (now `control_request`, plan 31 C5) | 9.9M | 7591 | clean |
| `store_control_json` (round two) | 4.2M | 14454 | clean (meta.json, leases, designations, snapshots, GC pointer, registry, heartbeat, delegation table) |
| `meta_blobs` | 9.1M | 10382 | clean |

Reproduce: `cd fuzz && cargo +nightly fuzz run <target>`. The store targets pin
`CONSTELLATION_MAX_DECOMPRESSED_BYTES` to 64 MiB at startup so libFuzzer's
malloc limit still flags any allocation past the ceiling.

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
