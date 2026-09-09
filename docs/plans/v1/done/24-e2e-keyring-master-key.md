# Plan 24 — E2E single-file keyring: one master key, everything derived

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §8 "Security" (keyring, per-partition DEKs,
keyed addressing, `fs passwd` never rotates data keys). Code context:
`crates/store-s3/src/e2e.rs` (the keyring today: `E2eKeys`, `Keyring`,
`WrappedKeys`, `wrap`/`wrap_with_key`/`unwrap`, `put_keyring`,
`load_keyring`, `change_passphrase`, `ensure_partition`,
`refresh_partition`), `crates/store-s3/src/store.rs` (`FsMeta`,
`create_fs`, `load_fs`), `crates/store-s3/src/log.rs`
(`ensure_partition_key`, `dek(part)` in `seal_segment`/`open_segment`/
checkpoints), `crates/cli/src/main.rs` (the `fs create` / `fs passwd`
handlers and the mount key-unlock path), `crates/cli/src/node_runtime.rs`
(mount unlocks the keyring once), `crates/cli/src/shipper.rs`
(`ensure_partition_key` on split, and the gossip seal path).

Builds on plan 10 (E2E) and subsumes the keyring-wrapped gossip secret
added alongside gossip-payload sealing.

**Development phase — no backwards compatibility.** There is no format
migration and no version negotiation. Existing E2E filesystems are
recreated, not upgraded. Do not add v1/v2 dual-read paths.

## Goal

Collapse the E2E key material to a single wrapped **master key (KMK)**
stored inside `meta.json`, and **derive** every other key from it. The
addressing key, every per-partition DEK, and the gossip topic seed become
`KDF(KMK, purpose)` — nothing else is stored.

Two payoffs:

- **`fs passwd` is a live operation.** A running node holds the KMK in
  memory; a passphrase change only rewraps the KMK envelope in
  `meta.json`. Nothing in a live node's memory is invalidated — partition
  splits, tailing, and gossip keep working with no remount.
- **The keyring stops being mutable state.** With DEKs derived rather
  than stored, a partition split needs no key persistence at all. The
  whole `ensure_partition` / `refresh_partition` CAS dance and the
  separate `keys/keyring.json` object are deleted. `meta.json` is the
  only per-filesystem object, and it stays tiny and effectively
  immutable (rewritten only by `passwd`).

No data is re-encrypted; chunk identities, DEK values, and the gossip
topic id are all stable for a given passphrase because they are fixed
functions of the KMK, which `passwd` never changes.

## Why single-file + derived keys is the right shape

The keyring today is mutable and grows: every partition split writes a
fresh random DEK into `keys/keyring.json` under CAS, and every node must
re-read the keyring to learn peer-created DEKs. That mutability is the
only reason the keyring cannot simply live in the write-once `meta.json`.
Derive the DEKs instead and the keyring becomes a fixed-size, immutable
blob — at which point folding it into `meta.json` is clean: one object,
one conditional-create at `fs create`, no second round trip, no window
where identity exists without keys.

Public (no-passphrase) readers of `meta.json` — GC, fsck, anyone reading
`chunk_size` — download only a single wrapped 32-byte value plus a salt,
never a per-partition DEK map. The identity object stays small and
constant.

## The key hierarchy

- **KMK**: one random 32-byte master key, generated at `fs create`.
- **KEK**: derived from the passphrase with Argon2id (params + salt in
  `meta.json`). Wraps only the KMK (XChaCha20-Poly1305, AAD
  `b"keyring-master"`). Ephemeral — materialised only during unlock,
  create, and `passwd`, then zeroized.
- **Derived, never stored** (all `KDF(KMK, purpose)`):
  - addressing key — keyed chunk-hash identity (DESIGN §8).
  - per-partition DEK — `KDF(KMK, dek-purpose(part))`.
  - gossip topic seed — the P2P topic id for E2E filesystems.

A running node holds the KMK (and, for hot paths, the precomputed
addressing key and gossip seed); it derives any partition's DEK locally
and needs no keyring reads or writes ever again.

## Settled decisions

Do not relitigate these.

- **One object.** `FsMeta` gains an optional keyring block
  `{ argon2_params, salt, wrapped_master }`, present iff `e2e`. Delete
  `keys/keyring.json`, `put_keyring`, `load_keyring`, the `Keyring` and
  `WrappedKeys` structs, and `ensure_partition` / `refresh_partition` /
  `ensure_partition_key`.
- **`E2eKeys` holds the KMK.** Replace `deks`/`wrapping_key` with
  `master_key: Box<[u8; KEY_LEN]>`. Precompute and cache the fixed
  `addressing_key` and `gossip_secret` at unlock; derive `dek(part)` on
  demand (BLAKE3 keyed hash is sub-microsecond). `dek` becomes infallible
  (`-> [u8; KEY_LEN]`, no `Result`) since a DEK is always derivable;
  update the handful of `dek(..)?` call sites in `log.rs`/`store.rs`.
  `mlock`/zeroize the master key exactly as the KEK was.
- **Domain separation is the security-critical invariant.** Derivation is
  `blake3::keyed_hash(&kmk, message)` where `message` carries an
  unambiguous, collision-free purpose tag. Fixed purposes
  (addressing, gossip) use distinct hardcoded, application-scoped
  contexts; the per-partition purpose embeds the partition name such that
  **no partition name can make a DEK message equal a fixed-purpose
  message or another partition's message**. Centralise this in one
  `fn derive(&self, purpose: KeyPurpose) -> [u8; KEY_LEN]` with a
  `KeyPurpose` enum (`Addressing`, `Gossip`, `Dek(&str)`); no ad-hoc
  string concatenation at call sites. A collision test is mandatory
  (see Tests) — this is the one genuinely new failure mode option A
  introduces and it must be closed by construction and by test.
- **Derivation, not storage, for the gossip seed too.** `FsMeta` keeps a
  plaintext `gossip_secret` for **non-E2E** filesystems only (S3 is the
  trust boundary there); for E2E it is `None` and the seed comes from
  `KMK`. The existing `start_p2p` preference (E2E keys' seed over
  `meta.json`) already covers this.
- **`fs passwd` rewrites only the master envelope, in `meta.json`.** Read
  `meta.json`, unwrap `wrapped_master` with the old KEK → KMK, derive a
  new KEK from the new passphrase + a fresh salt, write `meta.json` back
  with the new `salt`/`wrapped_master` and every other field unchanged,
  via CAS (`PutMode::Update`). It is a live operation: running nodes hold
  the KMK and are unaffected.
- **Create is one conditional PUT.** `fs create --e2e` prompts the
  passphrase (already ordered before any write), generates the KMK,
  builds the keyring block, and writes `meta.json` once with
  `PutMode::Create`. Identity and keys are created atomically — no
  separate keyring write, no orphan window.

## Step 1 — Crypto core (`e2e.rs`)

- Add `KeyPurpose` and `E2eKeys::derive`, the single source of all
  derived keys. Document the message construction and why it is
  collision-free.
- Replace the `Keyring`/`WrappedKeys` types with a `KeyringBlock`
  `{ argon2_params: Argon2Params, salt: String, wrapped_master: String }`.
- `E2eKeys`: `master_key` + cached `addressing_key`/`gossip_secret`;
  infallible `dek(part)`; `addressing_key()`/`gossip_secret()` return the
  cached values; drop the DEK map, `insert_partition`, `ensure_partition`,
  `refresh_partition`. Update `try_lock`/`Drop`/`Debug`.
- `generate`/create path: random KMK.
- `seal_master(kmk, kek) -> String` / `open_master(block, passphrase) ->
  KMK`; `unlock(block, passphrase) -> E2eKeys`.
- `change_passphrase` moves to operate on the `meta.json` keyring block.

## Step 2 — `FsMeta` carries the keyring block (`store.rs`)

- `FsMeta` gains `#[serde(skip_serializing_if = "Option::is_none")]
  keyring: Option<KeyringBlock>` (Some iff `e2e`). `create_fs` is
  unchanged in mechanism (one `PutMode::Create`); it now just serialises a
  meta that already contains the block. `load_fs` is unchanged — public
  readers deserialize and ignore the block.
- Keep the plaintext `gossip_secret` field for non-E2E; ensure E2E sets
  it `None` (already done in the create flow).

## Step 3 — Drop the per-partition key machinery (`log.rs`, `shipper.rs`)

- `get_segment`: remove the `refresh_partition` call; `dek(part)` is
  always available.
- `seal_segment`/`open_segment`/checkpoint paths: `dek(part)` without `?`.
- Delete `ensure_partition_key` and its call in `shipper.rs` on split — a
  split no longer touches any key object.

## Step 4 — CLI wiring (`main.rs`, `node_runtime.rs`)

- `fs create --e2e`: prompt passphrase → derive KEK → generate KMK →
  build `KeyringBlock` → set on `FsMeta` → `create_fs`. No `put_keyring`.
- Mount: after `load_fs`, if `e2e`, prompt passphrase and
  `unlock(meta.keyring, passphrase)` → `E2eKeys`. No `load_keyring` S3
  GET (the block is already in the loaded meta).
- `fs passwd`: the new `change_passphrase` against `meta.json`. The
  handler messaging stays ("passphrase changed; data-encryption keys were
  not rotated"), and should now also state that mounted nodes need no
  remount.
- Unused imports (`put_keyring`, `load_keyring`) removed from
  `main.rs`/`lib.rs`.

## Tests

Unit (`crates/store-s3/src/e2e.rs`, `InMemory` where a store is needed):

- **Derivation determinism & independence**: same KMK yields stable
  addressing key, gossip seed, and `dek(p0)`; a different KMK yields
  different values for all three.
- **Domain-separation / collision (mandatory)**: assert that
  `Addressing`, `Gossip`, and `Dek(part)` for a battery of adversarial
  partition names — `"addressing"`, `"gossip"`, names containing the
  purpose tags and separators, empty, and very long — are pairwise
  distinct, and that `Dek(a) != Dek(b)` for `a != b`. This test is the
  guard on the one new failure mode.
- **Unlock round-trip & wrong passphrase**: build a keyring block, unlock
  with the right passphrase (KMK and all derived keys match), wrong
  passphrase fails.
- **`passwd` is a live envelope rewrap** (headline): unlock `keys_before`
  from a meta block; `change_passphrase(old, new)`; then, using the
  *original* `keys_before` handle (never re-unlocked), derive a DEK for a
  brand-new partition name and seal/open a segment with it successfully —
  proving a live node keeps full function across a passphrase change.
  Assert the old passphrase no longer unlocks the block and the KMK is
  unchanged.
- **Secret hygiene**: the serialized `meta.json` for an E2E filesystem
  contains none of the raw KMK / addressing / DEK / gossip bytes (only
  the wrapped master and salt).
- **Single-object create**: `fs create --e2e` writes exactly one object;
  no `keys/…` key exists afterward.

Harness scenario (`crates/harness/src/scenarios.rs`):

- `passwd-live-cluster`: mount two nodes on an E2E filesystem, run a
  workload, `fs passwd` **without** unmounting either node, then drive
  activity that forces a partition split and a cross-node tail. Assert
  with the model oracle that the namespace converges and stays
  byte-correct, that neither node was remounted, and that a fresh mount
  afterward requires the **new** passphrase (old one refused). This
  proves "passphrase change without remounting" as behaviour, not just a
  unit property.

## Definition of done

The standard gates from `CONVENTIONS.md`, all green:

1. `cargo fmt --all` clean; `cargo clippy --workspace --all-targets -- -D warnings` clean.
2. `cargo test --workspace` — zero failures, including the collision and
   live-`passwd` tests.
3. `bash tests/smoke.sh` and `bash tests/integration.sh` — pass
   (including an E2E create + mount + `passwd` path).
4. `target/release/harness run` — every scenario PASSED, including
   `passwd-live-cluster`.
5. `docker compose --profile test run --rm compliance` — pjdfstest stays
   a FULL pass.
6. `docs/plans/v1/PROGRESS.md` updated; feature/operations docs updated to
   describe the single-file keyring, derived keys, and live `passwd`. Do
   not edit `docs/explanation/DESIGN.md`; record any spec contradiction in
   PROGRESS.md.
