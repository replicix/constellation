# Plan 07 — Phase 6b: sharing (exports) + E2E passphrase mode

Read `docs/plans/CONVENTIONS.md` first. Prerequisites: plans 00–06
committed. Spec: `docs/DESIGN.md` §8 "Security" (encryption at rest,
keyed addressing, sharing = IAM) and the exports bullet. Two mostly
independent features — do exports first (smaller), then E2E.

## Part A — `constellation share --export`

Subtrees cannot be IAM-scoped (content addressing), so an export
materializes PLAIN objects under an IAM-scopable prefix.

1. `constellation share --export <path> [--expire <duration>]`
   (CLI → control API): walk the subtree, write each file as a plain
   object `exports/<export-id>/<relative-path>` (no chunking, no
   compression headers — bytes as-is, standard `Content-Type`
   guessing optional), plus `exports/<export-id>/.manifest.json`
   `{v, path, created_unix_ms, expires_unix_ms, creator, files,
   bytes}`. export-id = short random token.
2. `share ls`, `share rm <id>` management; expiry enforcement =
   daemon-side GC sweep (the lease-holding node — or any node,
   guarded by a CAS-claimed `exports/.gc-lock` with TTL — deletes
   expired export prefixes on a timer, default hourly,
   `CONSTELLATION_EXPORT_GC_INTERVAL_S`).
3. Large subtree exports stream with bounded concurrency; the
   control-API reply returns immediately with the id while the upload
   continues (status visible via `share ls` — `complete: bool` in the
   manifest, rewritten at the end).
4. E2E interaction (Part B): exporting from an E2E filesystem writes
   PLAINTEXT objects — that is the feature's point (share outside the
   trust domain) but must be explicit: require `--plaintext-ok` on
   E2E filesystems.

## Part B — E2E passphrase mode

Per-filesystem, fixed at `fs create` (never a migration).

1. **Keyring**: `fs create --e2e` prompts (or takes
   `CONSTELLATION_PASSPHRASE` env for tests) a passphrase; generate:
   a random 32-byte **addressing key** and a per-partition data
   encryption key (DEK; one for p0 now, more created as partitions
   split). Wrap all of them with XChaCha20-Poly1305 under an argon2id
   KEK (`argon2` crate, sane cost defaults, parameters stored
   alongside) into `keys/keyring.json` `{v, argon2_params, salt,
   wrapped: {addressing_key, deks: {p0: ...}}}`. `meta.json` gains
   `e2e: true`.
2. **Mount**: E2E filesystems require the passphrase (prompt or env);
   unwrap the keyring; hold keys in memory only, `mlock` the pages
   (`memsec` or `region` crate; document if mlock fails — degrade
   with a warning, do not abort).
3. **Chunk path**: in E2E mode every chunk is
   compress-then-encrypt-then-put: existing codec output encrypted
   XChaCha20-Poly1305 (DEK of the owning partition; random 24-byte
   nonce in the self-describing header; AAD = chunk hash). Decrypt +
   verify on read, both from S3 and from peers (cooperative cache
   serves ciphertext as stored; peers of the same FS hold the same
   keys).
4. **Keyed addressing**: chunk identity = `blake3::keyed_hash(
   addressing_key, plaintext)` instead of plain blake3 (DESIGN §8:
   kills confirmation-of-file). This flows everywhere a hash is
   computed for identity: fs-core chunk hashing gets an optional key,
   threaded from mount config. Dedup still works within the FS.
5. **Metadata plane**: log segments and checkpoints also encrypt
   (they contain filenames): same DEK-per-partition, same envelope
   trick — a version byte prefix distinguishing encrypted segments.
   Tree blobs (snapshots) ride the chunk path so they are covered.
   The lease/registry/nodes objects stay plaintext (they hold no user
   content; document this boundary).
6. **Key rotation / passphrase change**: `constellation fs passwd`
   re-wraps the keyring under a new KEK (DEKs unchanged — one small
   object rewrite). Full DEK rotation is out of scope (note it).

## Tests

Unit: keyring wrap/unwrap round-trip incl. wrong-passphrase failure;
encrypt/decrypt chunk round-trip with AAD mismatch rejection; keyed
addressing produces different hashes than plain for the same bytes
(and identical within the same key). Export manifest/expiry logic
with a fake clock.

Harness scenarios:
- `export-lifecycle`: create files, export with short expiry,
  verify plain objects readable straight from the bucket
  (s3 client in the harness), wait past expiry + GC tick, verify
  gone; `share rm` for the manual path.
- `e2e-basic`: `fs create --e2e`; mount with passphrase; run a
  baseline workload block + model verify; assert every `chunks/` and
  `log/` object in the bucket is high-entropy/ciphertext (cheap
  check: no known plaintext markers — e.g. write a file full of
  `AAAA...`, fetch the raw chunk object, assert the plaintext pattern
  is absent); remount cold (fresh state dir) with the passphrase and
  verify content; wrong passphrase must fail the mount cleanly.
- `e2e-two-nodes`: both nodes mount with the passphrase; run the
  `two-clients-shared` workload shape; peers serve encrypted chunks
  (coop counters) and verify correctly.

## Gates + report

Per CONVENTIONS.md. Note perf impact of E2E on the bench
(`harness bench` with and without) in the report and PROGRESS.md.
