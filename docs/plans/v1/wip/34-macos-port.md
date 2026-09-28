# Plan 34 — macOS port: an embedded NFSv4.1 frontend on the core

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §10 (control plane), the lock and write-path
sections plan 30 extended (strict locks, `cto=strict`, flush-on-close
fencing), and `docs/explanation/GOALS.md` ("macOS … desirable … Linux
first"). Code context (historical — this is what the evidence in "Why, and
where we actually are" below was measured against; by the time this plan
executes, the engine and VFS-adjacent code have moved into
`constellation-engine`/`constellation-vfs`/`constellation-platform`/`constellation-control`
per plan 31 C2–C5, and this plan's own new code lives in
`crates/frontend-nfs`): `crates/cli/src/fusefs.rs` + `fusefs_ops.rs` (the
`ConstellationFs` core and its fuser adapter), `crates/cli/src/node_runtime.rs:1589-1700`
(mount/unmount of a view), `crates/cli/src/kernel_inval.rs` (`NotifySink`),
`crates/cli/src/locks.rs`, `crates/cli/src/daemonize.rs`,
`crates/cli/src/daemon_lock.rs`, `crates/cli/src/fuse_watch.rs`,
`crates/cli/src/parallelism.rs`, `crates/meta/src/{mutate.rs,record.rs}`
(errnos on the wire and in the journal), `crates/harness/src/{s3env.rs,client.rs,docker.rs,scenarios.rs,main.rs}`,
`tests/*.sh`, `.github/workflows/{ci,nightly}.yml`, `Makefile` (`dist-macos`).

**Depends on plan 31 (C1–C8) committed.** Plan 33 is optional, needed only
for the `.dmg`'s bundled UI app — the headless `constellation` binary itself
stays independent of plan 33. This plan is independent of plans 19, 23 and 32.

This plan does three jobs:

1. It records a mount-strategy decision, backed by source-level evidence.
2. It specifies the macOS-specific port work as milestones, on top of the
   plan-31 core.
3. It defines the GitHub Actions lanes that prove macOS behaves like Linux.

This is one of two remaining platform ports built directly on the plan-31
core. Plan 35 (Windows, a native WinFsp frontend) is being written in
parallel. Both ports depend on plan 31 directly — **not on each other** —
and build on the same shared layers: C1 (the portable `Code` errno enum),
C2 (`constellation-platform` host services), C3 (`constellation-engine`),
C4 (the `constellation-vfs` contract, with `constellation-frontend-fuse`
rebuilt on it), C5 (the control protocol / `constellation-control`), C6
(test architecture: the conformance kit, harness capabilities, the parity
framework), C7 (observability) and C8 (engine profiles/lifecycle). Nothing
in this plan builds Windows support; see plan 35. Earlier drafts of this
plan, when it was itself numbered 31, specified those shared layers
directly; this revision points at plan 31's milestones instead and keeps
only the macOS-specific residue: the NFS frontend crate, `platform::macos`
completion, macOS CI lanes, and packaging.

The research behind it was done on 2026-09-28. Every "VERIFIED" claim below
was checked against a source checkout in `/tmp` (listed at the end) or against
a cross-compile of this tree. "REPORTED" means secondary sources only; M0
re-checks those on a real runner before anything is built on them.

## Why, and where we actually are

The README and PROGRESS.md (phase 8 exit) both claim:

> "macOS CI compiles and runs mount-less workspace tests"

**That is no longer true.** Three separate problems break the build:

1. **The CI lane is not running.** The last macOS nightly that went green was
   run 33606356001 (2026-09-02). Since then, every nightly and PR workflow has
   failed before its first step, because the account's Actions billing is
   blocked ("recent account payments have failed or your spending limit needs
   to be increased"). Nobody has noticed the macOS lane.
2. **The tree does not type-check for Darwin.** VERIFIED:
   `cargo check --workspace --all-targets --target aarch64-apple-darwin` fails
   with 50 errors in 7 files. The check used zig as the C compiler and a stub
   `fuse.pc`. The errors are:

   | File | Errors | Cause |
   |---|---|---|
   | `crates/cli/src/fusefs_ops.rs` | 22 | `mode_t`/`S_IF*` are `u16` and `F_*LCK` are `i16` on Apple (`:352-357`, `:1391-1448`); `FALLOC_FL_*` is Linux-only (`:1794-1799`) |
   | `crates/harness/src/scenarios.rs` | 8 | `libc::fallocate` and `FALLOC_FL_*` (`:4837`, `:6740-6742`); Apple `setxattr`/`getxattr`/`removexattr` take extra `position`/`options` arguments (`:6655-6697`) |
   | `crates/cli/src/locks.rs` | 7 | `i16` lock types (`:175-198`) |
   | `crates/cli/src/fusefs.rs` | 5 | `FALLOC_FL_*` in tests (`:4297`, `:4431`, `:4452`) |
   | `crates/cli/src/main.rs` | 4 | Apple xattr arity (`:3903-3951`) |
   | `crates/cli/src/fuse_watch.rs` | 2 | `gettid` and `SYS_tgkill` (`:124`, `:330`); added 2026-09-28 in 5437fa6 |
   | `crates/chaos/src/op.rs` | 2 | `posix_fadvise` (`:20`) |

   **Fixing this table is now plan 31's job, not this plan's.** These
   blockers are fixed as part of plan 31 C1 (the portable `Code` enum
   replaces the raw `libc::E*`/`i16` lock-type/`mode_t` casts in
   `fusefs_ops.rs` and `locks.rs`), C2 (`FALLOC_FL_*`, Apple xattr arity and
   `posix_fadvise` move behind `constellation-platform`'s `FsPrimitives`),
   and C3 (the mechanical engine extraction carries the fixed modules with
   it). This plan keeps the table only as the evidence that motivated the
   NFS decision below, and picks up where plan 31 C1–C3 leave off: the
   macOS-specific residue plan 31 does not build — a Keychain `SecretStore`,
   `F_PUNCHHOLE`/`F_FULLFSYNC`, `MountTable` via `statfs`, NFS mount/unmount
   plumbing, `$TMPDIR` socket paths, the `RLIMIT_NOFILE` raise — is
   specified under "macOS host integration" (M2) below.
3. **Even with those fixed, the product cannot mount on current macOS.**
   - fuser 0.18's `build.rs` has no pure-Rust mount on macOS. It hard-requires
     `pkg-config fuse` (macFUSE libfuse2) and panics without it. The binary
     would therefore dyld-link `libfuse.2.dylib` for *every* subcommand.
   - It mounts via `fuse_mount_compat25`. Since macFUSE 5.3.0 (commit
     `970ef74d`, 2026-06-06), that function is literally `return -1;` on
     `__APPLE__`. VERIFIED in `/tmp/macfuse-library/lib/helper.c:747-754`;
     tracked as fuser issue #752, still open.
   - The fuser maintainer stopped maintaining macOS support in 2025 (#306).
     The README says "macOS (untested)".

Before choosing how to fix this, we had to answer the real question: **how
should Constellation mount on macOS at all?**

## Decision: an embedded NFSv4.1 server, mounted by the macOS kernel NFS client

### The options, with evidence

| Option | Kernel extension or approval | Mountable on GitHub-hosted runners | What semantics survive | Licence | Verdict |
|---|---|---|---|---|---|
| **macFUSE kext + fuser** | The kext needs Reduced Security plus a reboot on Apple Silicon, and cannot load in VMs (macFUSE wiki). | **No.** Runners are VMs with no reboot. | Most of them, but the problems pile up (list below). | Closed-source kext; redistributing it with commercial software needs written permission (`/tmp/macfuse/LICENSE.txt` clause 4). | **Rejected for now; deferred path documented below** |
| **macFUSE FSKit backend** (`-o backend=fskit`) | The user enables it in System Settings. The only non-interactive trick found is REPORTED (macfuse #1194). | **No.** An FSKit mount probe on `macos-15-arm64` fails in every run, e.g. clio-core runs 36422724205 and 36248063928. | No notify API, no caller uid/gid (`fuse_context`), mounts only under `/Volumes`. | As above | **Rejected.** fuser's fd model cannot even reach it: `MFChannelGetFileDescriptor` returns `ENOTSUP` for FSKit channels (VERIFIED `/tmp/macfuse-mount/Mount/MFMount.h:313-335`). |
| **FUSE-T** (a user-space NFS/SMB shim) | None | Yes, with a *patched* fuser (framed reads, a `fuse-t.pc` probe). Shown working by cipher-box run 34008694459. | Locks never reach the FS. Notifications work on the SMB backend only. No fallocate. | Closed source; commercial use needs a paid licence. | **Rejected.** It is our own NFS option with a proprietary server in the middle. |
| **Apple FSKit, native** | A signed, notarised `.appex` in a host app; the user enables it. | No | From macOS 27: `DataCacheHandler`, `setCacheState` invalidation, `FSContext` uid. No lock handler anywhere. | Apple | **Deferred** until macOS 27 can be the floor. It slots in as a third frontend. |
| **File Provider** | None | n/a | A sync engine, not a POSIX filesystem: no advisory locks, no open/close or fsync visibility. | Apple | **Rejected** |
| **Embedded NFSv3** (nfsserve / nfs3_server) | None | Yes. ZeroFS CI mounts on `macos-latest`. | xattrs degrade to AppleDouble `._*` files, locks are node-local only, no hard links in nfsserve, and fsync is invisible unless the server answers UNSTABLE writes and handles COMMIT. | BSD-3 | **Fallback only** (see M0 exit) |
| **Embedded NFSv4.1** (vendored fork of embednfs) | **None.** Mounting is allowed for non-root users on a mount point they own. | **Yes.** Loopback NFS mounts work on hosted runners. | xattrs via named attributes, in-band locks the server arbitrates, OPEN/CLOSE, COMMIT, LINK, caller uid/gids, case-sensitive attributes. The one loss is kernel push-invalidation; see M1b. | MIT, compatible with MPL-2.0 | **Chosen** |

The macFUSE+fuser problems behind the first row's "semantics" cell, all
VERIFIED in `/tmp/fuser@v0.18.0`:

- Dispatch is single-threaded on macOS: `session.rs:259` refuses `n_threads != 1` off Linux.
- The ABI is 7.19, so there is no `lseek` (SEEK_HOLE), no parallel dirops and no max_pages.
- Every volume is advertised as case-insensitive (`lib.rs:112-116`, issue #749), which is wrong for a cluster that Linux peers write to.
- Init flag bits 23-31 are misinterpreted on macFUSE (#751).
- Mount readiness is asynchronous (#736).

### Why NFSv4.1 is the best fit for Constellation specifically

These points are VERIFIED in Apple's NFS client source, `/tmp/apple-nfs`
(NFS-343.100.5, 2026-04-13), unless marked otherwise.

- **It is kext-free and needs no approval, just like Linux FUSE's user
  mounts.**
  - `mount_nfs` runs as the user on a directory the user owns. XNU forces
    `nosuid,nodev` for non-root mounts (`vfs_syscalls.c:1268-1276`).
  - hf-mount ships exactly this, run from a LaunchAgent without sudo.
- **The mount runs over a unix socket, not a TCP port.**
  - `mount_nfs` accepts an `AF_LOCAL` server address (`mount_nfs.c:1600-1625`, `:2697-2738`).
  - A socket in a 0700 directory means no other local user can reach the
    server by spoofing `AUTH_SYS`.
  - This is the macOS equivalent of `/dev/fuse` being handed to one process.
- **v4.1 carries what our semantics hang on:**
  - OPEN/CLOSE give us the close-time flush fence.
  - LOCK/LOCKT/LOCKU with lock-owners reach the server, so cluster locks keep working.
  - COMMIT gives us fsync.
  - `namedattr` gives us real xattrs; the man page says it covers "extended
    attributes and named streams (e.g. FinderInfo and resource forks)"
    (`mount_nfs.8:494`).
  - SEQUENCE and sessions are implemented by the client (`kext/nfs4_subs.c:389+`).
  - The 4.1 callback path exists (`kext/nfs_socket.c:3091`), which delegations need.
  - v4.1 is the highest version the client speaks (`mount_nfs.8:222-226`).
- **The concurrency problem goes away.**
  - The server is async and multiplexes requests, instead of a single fuser
    dispatch thread.
  - It reports `case_insensitive=false`, which the fuser/macFUSE route cannot do.
- **The same frontend runs on Linux.** The Linux kernel NFS client can mount
  it over TCP loopback. That lets CI separate "NFS frontend semantics" from
  "macOS", which is exactly what the parity lanes below need.
- **No third-party install for users, and the licences are clean.** embednfs
  is MIT (`/tmp/embednfs`, 0.4.1, ~22k lines including tests). It already
  targets the macOS client over localhost. Its trait has `RequestContext`,
  `Xattrs`, `HardLinks` and `CommitSupport` hooks (`crates/embednfs/src/fs.rs:538-720`).
- **What it lacks, and what M1 therefore adds in a vendored fork:**
  - open/close, lock and delegation state is server-internal and not exposed to the trait;
  - there is no unix-socket transport;
  - there are no delegations or callbacks.

  This mirrors how `vendor/fjall` is carried, with a `CONSTELLATION-PATCH.md`.

What we give up compared with Linux FUSE, stated plainly (details in "Semantic
differences" below):

- **No push cache invalidation.** Until M1b, coherence is attribute-cache TTL,
  set to 1 s to match FUSE's `TTL`.
- **No fallocate or hole punch.** NFSv4.1 has no ALLOCATE/DEALLOCATE; those arrive in v4.2.

## Settled decisions

Taken from the research above; do not relitigate them.

### Now built in plan 31, not here

Earlier drafts of this plan (when it was itself numbered 31) specified the
shared layers below directly. They are plan 31's job now:

- **The portable errno table** (`constellation-types::Code`, a
  `#[repr(u16)]` enum with its own fixed wire discriminants — not the
  Linux errno number — plus Linux/macOS/Windows-NTSTATUS conversions at
  each platform's boundary) — plan 31 C1. This plan
  only supplies the *content* facts the macOS conversion table needs
  (settled decisions 5–6 below); C1 owns the enum, the conversion call
  sites, and the `libc::E*` removal from library logic.
- **`fuser` becoming a Linux-only dependency**, and
  `constellation-frontend-fuse` becoming a thin adapter over `Vfs` — plan 31 C4.
- **The `constellation-platform` crate itself**, its `HostServices` trait
  bundle, and the non-macOS module stubs — plan 31 C2, which also gives the
  `macos` module a baseline implementation. See "macOS host integration"
  (M2) below for what this plan completes on top of it.
- **`constellation-engine` extraction** (`ConstellationFs` → `View`) —
  plan 31 C3.
- **The `constellation-vfs` contract itself**: `Vfs`, `OpCtx`, `Responder`,
  `Caller`, the `NamePolicy`/`XattrPolicy`/`IdentityMap` hooks,
  `PolicyStack`, `OpWatch` — plan 31 C4. This plan's frontend only
  *implements* `Vfs` and *instantiates* `XattrPolicy`; it does not define
  the contract.
- **The control-protocol transport abstraction** (`constellation-control`,
  the `UnixSocket`/`InProcess` transports) and the portable xattr path —
  plan 31 C5. Xattr/scratch/prune controls that an earlier draft of this
  plan gave a `constellation xattr` CLI subcommand now go through control's
  `browse.xattr` method instead; this plan adds nothing here.
- **The open `--frontend` enum, its dispatch plumbing, and the `winfsp`
  reservation** — plan 31's crate/decision layer. This plan only sets
  macOS's default (`nfs`) and its `fuse` error text (settled decision 2
  below).
- **The harness's `--results-json`/`--shard`/`make check-cross`/the
  `cross-check` CI job** — plan 31 C0. **The harness's `--frontend`,
  `--s3-backend process`, the derived `Cap` enum, `vfs::conformance`,
  `MockVfs`, the Rust-ported `harness smoke`/`harness interop`, and
  `tests/platform-parity.toml` + `tests/parity.py`** — plan 31 C6.
- **Everything under the old "Accommodating Windows (plan 33)" section**:
  the open frontend enum, the platform crate's `windows` stub, the
  NTSTATUS `Code` conversion, the VFS contract's handle/`Caller`/policy
  shape, and the `NamedPipe`-ready control transport are all plan 31's job
  now. Windows itself is plan 35's.

### Settled decisions (macOS-specific)

1. **The macOS frontend is embedded NFSv4.1.** It is a vendored fork of
   embednfs in `vendor/embednfs`, wrapped by a new crate `crates/frontend-nfs`
   (`constellation-frontend-nfs`) implementing plan 31's `Vfs` trait.
   - The frontend serves one listener per mounted view.
   - Other frontends considered but not built:
     - macFUSE (a documented deferred path);
     - native FSKit (revisit when macOS 27 is the floor);
     - FUSE-T.
2. **Frontend selection on macOS defaults to `nfs`.** The `--frontend
   fuse|nfs|winfsp` enum and its dispatch plumbing are plan 31's (see
   above); this plan only sets macOS's behaviour within it:
   - the default is `nfs` on macOS (unchanged: `fuse` on Linux);
   - `fuse` on macOS is a clean error naming the deferred macFUSE path (see
     "Deferred" below).
3. **Transport.**
   - **macOS:** an `AF_LOCAL` socket at `$TMPDIR/cnfs-<8-hex view id>.sock`.
     `$TMPDIR` is the per-user 0700 `/var/folders/…/T/`, about 49 chars, which
     keeps the path well under the 104-byte `sun_path` limit. The path goes
     into the mount record.
   - **Linux:** the kernel client cannot mount over `AF_LOCAL`, so TCP
     `127.0.0.1:<ephemeral>` is used. The server rejects peers whose source
     port is ≥1024, the classic NFS `secure` check: only the kernel (root) can
     connect.
   - **Both:** the server rejects `AUTH_SYS` uids other than the mounting
     user, unless `--allow-other` is set.
4. **Mount options** are owned by one function, `nfs::mount_args()`, and
   unit-tested.
   - **macOS:** `vers=4.1,namedattr,port=<sock>,rsize=1048576,wsize=1048576,actimeo=1,nobrowse,hard,intr`.
   - **Linux:** `vers=4.1,proto=tcp,port=<p>,rsize=…,wsize=…,actimeo=1,hard`.
   - The harness adds `soft,timeo=…` so that kill‑9 scenarios can finish.
   - M0 may change individual options (for example `deadtimeout`, `locallocks`
     behaviour or `nobrowse`). It must record why in this file.
5. **xattr names on macOS** — the macOS instance of plan 31's `XattrPolicy`
   hook (C4): the kernel-visible name `X` ↔ the stored name `user.X`.
   - Consequences:
     - Linux `user.constellation.scratch` appears on macOS as `constellation.scratch`.
     - A macOS `com.apple.quarantine` is stored as `user.com.apple.quarantine`, which Linux sees as an ordinary user xattr.
     - Linux `trusted.*` and `security.*` names are invisible on macOS.
   - The virtual `rsize`/`rcount` xattrs are exposed as `constellation.rsize`/`constellation.rcount`.
     - They are **not** listed by listxattr on macOS (`FrontendCaps::virtual_xattrs_listed = false`).
       `copyfile(3)`/Finder enumerate every listed name, which would trigger
       recursive computations and EPERM on copy.
     - They are readable by explicit name.
   - Resource-fork writes at `position != 0` return `ENOTSUP`.
   - Scratch and prune controls go through the control API's `browse.xattr`
     (plan 31 C5), not a literal `user.` name, so this mapping only matters
     where the NFS frontend itself exposes kernel-visible xattrs.
6. **Missing-xattr errno.**
   - On macOS the kernel boundary returns `ENOATTR` (93), never `ENODATA`.
   - Internally, the portable `Code::NoData` (plan 31 C1) stays canonical;
     only this frontend's macOS-facing boundary translates it to `ENOATTR`.
7. **The default paths stay XDG-style on macOS**: `~/.config/constellation`
   and `~/.local/share/constellation` — the macOS instance of plan 31's
   `dirs` seam in `constellation-platform` (C2).
   - `~/Library/Application Support/…` would push `control.sock` past 104
     bytes for usernames longer than ~5 characters.
   - It would also split the registry's location between documentation and
     code. The docs say this explicitly.
8. **Daemonization on macOS spawns a fresh process instead of forking** —
   the macOS instance of plan 31's `daemon` seam in `constellation-platform`
   (C2), completed by this plan's M2.
   - A plain fork would follow the pre-fork S3/TLS check
     (`check_fs_before_mount`). That check initialises Security.framework/
     CoreFoundation, and a fork without exec is unsupported afterwards.
   - macOS therefore uses `posix_spawn(current_exe, --foreground --daemon-child, ready-fd)`,
     keeping the existing readiness-pipe protocol.
   - Linux keeps `fork`.
9. **CI hosts.** macOS CI runs on GitHub-hosted `macos-26` (arm64) for PR lanes
   and on `macos-15` plus `macos-26` for nightly lanes.
   - The repo is private, so macOS minutes bill at the macOS multiplier. PR
     lanes stay short; the full matrix runs nightly and sharded.
   - An `x86_64` type-check runs on every PR.
   - `macos-26-intel` runs a nightly smoke.
   - arm64 macOS CI runners have no Docker, which is why the harness needs
     plan 31 C6's native process S3 backend (`--s3-backend process`,
     versitygw + toxiproxy) to run here at all; this plan does not build
     that backend, only uses it (see M3 below).

## Semantic differences the NFS frontend introduces

This table is the contract the parity file (owned by plan 31 C6; extended
by this plan's M4 additions below) encodes. Rows marked "M0" depend on
client behaviour that M0 must observe on a real runner.

| Behaviour | Linux FUSE (reference) | NFS frontend on macOS | NFS frontend on Linux |
|---|---|---|---|
| Entry and attribute caching | 1 s TTL plus push `FrontendEvents::invalidate` (`Entry`/`Attr`/`Data` variants) | `actimeo=1`. There is no push until M1b delegations; after that, recall on remote change. | Same as macOS |
| Close-to-open / flush fence | FUSE `flush` per `close(2)` with lock_owner | The client flushes dirty data on close and sends CLOSE when the open-owner's last open closes; the fence runs on CLOSE (M0: confirm per-fd vs per-owner) | Same |
| fsync | FUSE `fsync` | WRITE is replied UNSTABLE; `fsync` → COMMIT → `flush_inode(sync)` | Same |
| Cross-node advisory locks (`--locks cluster`, strict locks) | FUSE getlk/setlk | LOCK/LOCKT/LOCKU → Constellation's lock service; flock via v4 (M0 confirm) | Same (Linux emulates flock with whole-file locks) |
| User xattrs, scratch and prune markers | `user.*` | Named attributes with the name mapping from settled decision 5 | **None.** The Linux client does xattrs only via v4.2 (RFC 8276), so xattr scenarios are capability-skipped. |
| Virtual rsize/rcount | Listed and readable | Readable by name, not listed | None |
| fallocate / punch hole / SEEK_HOLE | Supported | `ENOTSUP` (not in v4.1) | `ENOTSUP` |
| Hard links, mknod, FIFOs, sockets | Supported | LINK; CREATE with NF4CHR/BLK/FIFO/SOCK | Same |
| Open-then-unlinked files | FUSE keeps the inode | The client may silly-rename to `.nfs*`. M0 checks whether macOS honours `OPEN4_RESULT_PRESERVE_UNLINKED`. If not, M1 keeps `.nfs*` names node-private through the scratch mechanism, never published. | Same (M0) |
| Permission checks | Kernel `DefaultPermissions` plus our checks | Server-side from `AUTH_SYS` uid and ≤16 gids. This fixes the macOS `/proc` supplementary-group gap. | Same |
| Case sensitivity | Sensitive | Sensitive (`case_insensitive=false`, `case_preserving=true`) | Sensitive |
| Unicode normalisation | Bytes | Bytes; no normalisation, documented | Bytes |
| Daemon killed | `ENOTCONN` until `fusermount3 -uz` | `hard`: callers block until the server is back; `umount -f` detaches. Harness uses `soft`. | Same; `umount -f` |
| `.DS_Store` / Finder / Spotlight noise | n/a | `nobrowse`; docs recommend `DSDontWriteNetworkStores`; with `namedattr` there are no AppleDouble `._*` files | n/a |

Mixed Linux/macOS clusters are a goal: one bucket with nodes of both kinds.
- Plan 31 C1 makes the wire and the journal portable.
- This plan's M4 interop lane proves it.
- uid/gid mapping between OSes (macOS 501 vs Linux 1000) is **not** solved
  here. It is documented as a requirement that users keep matching numeric ids.

## Milestones

These are plan 34's own milestones (M0–M5), numbered separately from plan
31's C0–C8. Every milestone here assumes plan 31 C1–C8 are already
committed, except M0, a throwaway spike that touches none of the product
and needs no core at all. Each milestone ends with the CONVENTIONS gates
green on Linux **and** the macOS lanes that exist at that point green. The
Linux pjdfstest lane must stay a FULL pass throughout — plan 31 C4 (the
`constellation-vfs` contract and the `frontend-fuse` rebuild) is the layer
most likely to disturb it, and keeping it green there is a precondition
this plan relies on, not a deliverable of it.

### M0 — Spike on a real runner: go/no-go for NFSv4.1 (timebox: 3 days)

**What to build.** A throwaway example outside the product, in `vendor/embednfs/examples/probe.rs`:
- embednfs's `memfs`, served over an `AF_LOCAL` listener (add the transport now; it is ~50 lines);
- mounted as a non-root user on `macos-15` and `macos-26`;
- and over TCP loopback on `ubuntu-latest`.

Commit a temporary workflow `.github/workflows/nfs-probe.yml`
(`workflow_dispatch` only) that runs a probe script. Record a matrix of answers
in this file, under "M0 results". Questions:

1. Does `mount_nfs -o vers=4.1,namedattr,port=/path.sock` mount as a
   non-root user on each macOS version? If 4.1 is refused on macOS 15, is 4.0
   accepted? Record which macOS version first ships 4.1.
2. Do `xattr -w`/`-p`/`-d`, `setxattr(2)` with a `com.apple.*` name, and
   Finder-style `copyfile` reach the named-attribute hooks? Check that no
   `._*` files appear.
3. Do `fcntl(F_SETLK)` and `flock(2)` from two processes reach LOCK/LOCKU
   with distinct lock-owners? Is LOCKT used for `F_GETLK`?
4. Is CLOSE sent per `close(2)` or per open-owner? Does `fsync(2)` produce a
   COMMIT after UNSTABLE writes?
5. Unlinking an open file: does the client silly-rename to `.nfs*` if the
   server sets `OPEN4_RESULT_PRESERVE_UNLINKED`?
6. If the server offers a read delegation, does the client accept it, and does
   it honour CB_RECALL on the 4.1 backchannel over the unix socket?
7. What happens when the server is killed and restarted on the same socket
   with stable file handles: does the mount recover under `hard` and fail
   under `soft,timeo=`? Does `umount -f` always detach?
8. What are the throughput and small-file create rates against memfs? This is
   a sanity check (≥ 200 MB/s sequential, ≥ 2k creates/s); it is not a gate.
9. The same questions for the Linux kernel client over TCP, minus xattrs.

**Pre-agreed consequences.** No need to reopen the decision for any of these:

- **4.1 only works on macOS 26:** the macOS floor becomes 26 and `macos-15`
  leaves the matrix.
- **Neither 4.1 nor 4.0 mounts as non-root:** mount through a tiny
  `launchd`-free `sudo -n mount_nfs` path, and document it.
- **`AF_LOCAL` fails:** use TCP loopback with the reserved-port check on macOS too.
- **Named attributes fail:** xattrs on macOS go through the control API only
  (`browse.xattr`, plan 31 C5), and xattr scenarios are capability-skipped on macOS.
- **Locks don't reach the server:** use `locallocks`, add a capability flag,
  and document that `--locks cluster` is unavailable on macOS.
- **Delegations don't work:** skip M1b; TTL coherence is final.
- **More than two of the above fail:** stop and report. The NFSv3 fallback
  (nfs3_server, BSD-3, UNSTABLE+COMMIT hooks) is then evaluated in a revised plan.

### M1 — `crates/frontend-nfs`: the NFSv4.1 frontend (runs on Linux and macOS)

- Vendor embednfs at the commit M0 used into `vendor/embednfs`, with a
  `CONSTELLATION-PATCH.md` in the `vendor/fjall` style. Patches:
  1. **Transports.** `AF_LOCAL` listener. TCP listener with a reserved-port
     peer check. Both check the `AUTH_SYS` uid against the mount owner.
  2. **Hooks exposed to the trait:** `open(ctx, h, share_access, open_owner)`,
     `close(ctx, h, open_owner)`, and `lock`/`lockt`/`locku(ctx, h, lock_owner, range, type, blocking)`.
     - The server keeps its protocol state machine (stateids, seqids).
     - Constellation owns lock *arbitration*.
  3. **WRITE returns UNSTABLE**, and COMMIT calls the trait. The
     `CommitSupport` hook already exists.
  4. **Stable file handles**: `fs_uuid ‖ view_id ‖ ino`. Handles survive a
     daemon restart, so a `hard` mount recovers.
  5. **`OPEN4_RESULT_PRESERVE_UNLINKED`**, if M0 shows the client honours it.
  6. **Named-attribute plumbing**, so the macOS `XattrPolicy` instance
     (settled decision 5) owns the name mapping, applied through plan 31's
     `PolicyStack`.
- `crates/frontend-nfs` implements the embednfs `FileSystem` trait,
  translating each embednfs call into a call against plan 31's `Vfs` trait
  and completing it through a `Responder`. Async NFS dispatch suits
  deferred completion naturally (plan 31 C4's completion barrier), so cold
  reads, lock waits and admission backpressure can defer without blocking a
  dispatch thread; calls that don't defer complete inline. Per plan 31's
  threading contract, this frontend owns its own dispatch threads and never
  calls in from a tokio runtime worker.
  - It exports `mount_args()` (settled decision 4) and a `FrontendEvents`
    impl: it bumps the inode's change attribute on the relevant
    `Invalidation` variant, so the next GETATTR after the TTL sees it;
    delegation recall (`CB_RECALL`) is added in M1b.
  - It declares `FrontendCaps`. Concrete values for the two lanes this
    plan builds:

    | Field | macOS NFS | Linux NFS |
    |---|---|---|
    | `push_inval` | `None` (until M1b: `Attr` — recall triggers a refetch, not a push of new data) | same |
    | `per_close_flush` | `true` | `true` |
    | `cluster_locks` | `true` (M0-confirmed; falls back per the M0 "locks don't reach the server" contingency) | `true` |
    | `xattrs` | `Named` | `None` (the Linux NFS client has no xattr support before v4.2 / RFC 8276) |
    | `virtual_xattrs_listed` | `false` | `false` (moot: no xattrs at all) |
    | `hard_links` | `true` | `true` |
    | `fallocate` | `false` (no ALLOCATE/DEALLOCATE before v4.2) | `false` |
    | `seek_hole` | `false` | `false` |
    | `special_files` | `true` (CREATE with NF4CHR/BLK/FIFO/SOCK) | `true` |
    | `case` | `Sensitive` | `Sensitive` |
    | `max_io` | 1 MiB (`rsize`/`wsize`) | 1 MiB |
    | `deferrable` | all ops (async NFS dispatch) | all ops |
    | `open_unlinked` | `SillyRename` (or `Keep`, if M0 shows `OPEN4_RESULT_PRESERVE_UNLINKED` is honoured) | same, M0-dependent |

    The harness's `Cap` enum (plan 31 C6) is derived from this table, not
    hand-maintained.
  - Applies the macOS `XattrPolicy` (`user.X` ↔ `X`, virtual xattrs not
    listed) via plan 31's `PolicyStack`.
- Wire `--frontend nfs`'s mount/unmount path through the engine's
  view-open/-close flow (plan 31 C3/C4) and `platform::macos`'s mount
  primitives (this plan's M2 completion of the C2 seam):
  1. start the listener;
  2. run `mount_nfs` (macOS) or `mount -t nfs4` (Linux; `sudo -n` when not root);
  3. poll `platform::is_mounted` for readiness;
  4. on unmount, run `umount` and then fall back to `umount -f`.
- **Tests:**
  - **Protocol-level unit tests** in `crates/frontend-nfs`, reusing
    embednfs's wire-encoding test helpers: a scripted client drives
    COMPOUNDs against the real `Vfs` impl backed by an in-memory
    `Engine`+`View` (`Meta::open_in_memory()` + `InMemory` object store).
    They cover:
    - create/write/UNSTABLE/COMMIT, lock conflict across two lock-owners,
      xattr name mapping, stale-handle behaviour across a restart, and
      `.nfs*` handling.
  - These run everywhere, including the macOS PR lane, with no mount. They
    complement — and are exercised as one of the `PolicyStack`+`FrontendCaps`
    combinations by — plan 31 C6's `vfs::conformance` kit in CI (see M4).
  - Mount-level tests are `#[ignore]` unless `CONSTELLATION_TEST_NFS_MOUNT=1`;
    the smoke lanes (M3/M4 below) set it.

### M1b — Push invalidation through read delegations (only if M0 Q6 passed)

- Grant read delegations on OPEN for regular files.
- This frontend's `FrontendEvents` impl recalls the delegation (`CB_RECALL`
  over the backchannel) whenever the engine would otherwise have delivered a
  `Data`/`Attr` invalidation.
- Directory entry invalidation has no v4.1 mechanism. It stays TTL-based and is
  documented.
- **Gate:** the harness coherence scenarios that pass on Linux FUSE pass on
  both NFS lanes without TTL-sized sleeps. Scenarios that inherently need
  push entry-invalidation get a parity entry with that reason.

### M2 — macOS host integration: completing `platform::macos`

Plan 31 C2 builds `constellation-platform`'s `macos` module with a baseline
`HostServices` implementation (dirs, a basic `FsPrimitives`, a `SecretStore`
scaffold, process/mount-table shapes). This plan completes the
macOS-specific pieces C2 leaves for it — as plan 31 itself notes of
`SecretStore`, "file-backed on Linux; macOS Keychain later in 34":

- **`posix_spawn` daemonize** (settled decision 8): `posix_spawn(current_exe,
  --foreground --daemon-child, ready-fd)`, keeping the existing
  readiness-pipe protocol. A plain `fork` is unsafe here because the
  pre-fork S3/TLS check initialises Security.framework/CoreFoundation,
  which is unsupported after a fork without exec.
- **libproc process facts**: `proc_pidinfo` with `PROC_PIDTBSDINFO`/
  `PROC_PIDTASKINFO` for liveness, zombie detection, thread count and RSS
  (daemon-lock takeover, the startup watchdog); `getgrouplist` for
  supplementary groups (the NFS server's `Caller::in_group` fallback).
- **`F_PUNCHHOLE` and `F_FULLFSYNC` in `FsPrimitives`**: `fcntl(F_PUNCHHOLE,
  fpunchhole_t)` for staging hole-punch on APFS (falls back to no-op where
  unsupported); `F_FULLFSYNC` is what Rust std's `sync_all` already uses on
  Apple — keep it (durability first) and measure its cost in the M4 bench
  lane (informational, not gated).
- **A Keychain-backed `SecretStore`** for `node.key` and the E2E pin,
  replacing C2's file-backed default on this platform.
- **`MountTable` via `statfs`**: `f_mntonname`/`f_fstypename` for "is this
  path a live mount", plus stale-mount detection (`ETIMEDOUT`/`EIO`/
  `ENOTCONN` on a short-timeout probe `stat`) and `umount`/`umount -f`.
- **NFS mount/unmount plumbing**: `mount_nfs` with `nfs::mount_args()`
  (settled decision 4), polling readiness, and the `umount` → `umount -f`
  fallback on teardown.
- **`$TMPDIR` socket paths**: the `AF_LOCAL` listener path at
  `$TMPDIR/cnfs-<8-hex view id>.sock`, kept under the 104-byte `sun_path`
  limit (settled decision 3).
- **`RLIMIT_NOFILE` raise** at startup: soft limit to
  `min(hard, kern.maxfilesperproc)`.
- **`ensure_allow_other_supported`**: on the NFS frontend this is enforced
  in-server (settled decision 3 / `--allow-other`), so there is no
  `fuse.conf`-style host check to port.

**Docs:**
- `docs/how-to-guides/` gets "Mount on macOS": no install step, mount-point
  ownership, `nobrowse`, Finder `.DS_Store` advice, what differs (link the
  semantic-differences table), how to unmount a hung mount, and an optional
  LaunchAgent plist for mount-at-login.
- The README quick start gets an OS note.
- The macOS rows go into `docs/reference/`.

### M3 — Harness and suites run natively on macOS

Plan 31 C6 builds the cross-platform harness machinery an earlier draft of
this plan specified itself: the `S3Backend` enum and `--s3-backend process`
(versitygw + toxiproxy, motivated in part by this port — arm64 macOS
runners have no Docker), `--frontend`, the derived `Cap` enum, and the
platform-neutral `Client`/scenario calls that used to hardcode `/proc`,
`fusermount3` and `mountpoint -q`. This milestone only adds what's left
over once that machinery exists:

- **`--frontend nfs`** exercised end to end through the harness's
  mount/abort paths on macOS — the harness's own `--frontend` dispatch is
  C6's; this milestone is what runs the `nfs` value there. The NFS variant
  of "abort" is `umount -f`.
- **Shell suites, the parts that stay bash.** Plan 31 C6 prefers porting
  each suite into a Rust harness subcommand (`harness smoke`, `harness
  stress`, `harness compliance`); the `.sh` files that remain thin wrappers
  still need macOS fixes:
  - a `lib.sh` `is_mounted`/`unmount` pair that dispatches on `uname -s`;
  - replace `truncate -s` with `dd`/`mkfile`;
  - use `mktemp -d "${TMPDIR:-/tmp}/…"` then `realpath`, because `/tmp` is
    `/private/tmp` on macOS;
  - bash 3.2 compatible: no `mapfile`, no associative arrays;
  - `smoke.sh` gains `FRONTEND=nfs`.
- **GNU-only usages hit specifically when running on macOS's BSD userland:**
  - `touch -d "60 days ago"` → set times with `filetime`/`utimensat`;
  - `rsync --inplace` → check with `rsync --version`, skip if it is openrsync;
  - `strace` stays optional and Linux-only.
- **pjdfstest on macOS NFS.**
  - Build the C suite from a pinned upstream ref. It supports Darwin in
    `tests/conf` and `misc.sh:230-246`, but upstream never runs it on macOS
    (`ci/test.sh` exits 0).
  - Run it as root against a root-owned NFS view mount.
  - Add a **reasoned** baseline, `tests/pjdfstest-baseline-macos-nfs.txt`,
    two-way like xfstests: every entry carries a reason, and stale entries fail.
  - Run the **same suite** against the Linux NFS frontend. Its baseline
    `tests/pjdfstest-baseline-linux-nfs.txt` should be a subset of the macOS
    one; differences are explained.
  - The Linux FUSE lane keeps its empty baseline (plan 31 C6's `linux-fuse`
    reference lane) — "no compliance exceptions" remains the rule there.
- `make deps-macos`: `brew install versitygw toxiproxy fio stress-ng coreutils`.
  `make harness-native` runs the harness with `--s3-backend process`
  (plan 31 C6).

### M4 — CI: macOS/NFS lanes, parity additions, cross-OS interop

The workflows are specified in full below — macOS/NFS jobs only. `cross-check`,
the `linux-fuse`/`linux-fuse-process` lanes, and the `parity` aggregation
job itself are plan 31 C0/C6's; this plan's jobs feed results into them,
they are not redefined here. Also in this milestone:

- **Additions to `tests/platform-parity.toml`.** Plan 31 C6 owns the file
  and the checker `tests/parity.py`; this plan adds the macOS/NFS-specific
  `[[expect]]` entries below.
- **Lane names `linux-nfs` and `macos-nfs`**, following plan 31 C6's
  `<os>-<frontend>` convention with `linux-fuse` as the reference.
- The interop lane.
- Removal of the M0 probe workflow.

### M5 — Packaging

- **`make dist-macos`** builds `aarch64-apple-darwin` and `x86_64-apple-darwin`.
  It then `lipo`s them into a universal binary, sets
  `MACOSX_DEPLOYMENT_TARGET` to the M0-decided floor, and ad-hoc signs
  (`codesign -s -`).
- **When `MACOS_SIGNING_*` secrets exist**, sign with the Developer ID
  hardened runtime and `notarytool submit --wait`. No entitlements are needed,
  because there is no third-party dylib.
- **Optional: bundle plan 33's UI app into a `.dmg`.** When plan 33 is
  committed and its macOS UI bundle artifact is available, `make dist-macos`
  additionally produces a `.dmg` containing both the headless `constellation`
  binary and the UI `.app`; without plan 33 this step is skipped and only
  the binary archive is built. The headless binary itself always stays
  independent of plan 33 — see the header dependency line. Building the UI
  `.app` itself is plan 33's own CI job; this plan only combines the two
  artifacts when both are available.
- **`RELEASING.md`:** the macOS archive is universal; verify with `lipo -info`
  and `codesign -dv`.
- A Homebrew tap formula is **out of scope**. Note it as the next step.

## CI definitions

All new jobs use the existing pattern: `dtolnay/rust-toolchain@stable` plus
the pinned `rust-toolchain.toml`, and `Swatinem/rust-cache@v2`. macOS runner
images already ship pkgconf and Rust 1.98.1 (VERIFIED
`/tmp/runner-images/images/macos/*Readme.md`), and have passwordless sudo
(`scripts/build/configure-machine.sh:99-100`). Docker is not available on
macOS runners.

**Precondition, owned by the coordinator rather than the executing model:**
restore the GitHub Actions billing. Until then no lane runs on GitHub,
including the Linux ones. Plan 31's `cross-check` (C0) is also a
precondition for every lane below, not a deliverable of this plan — it
catches Darwin and Windows-library breakage for Linux-minute prices before
these macOS lanes ever have to.

### PR lanes (`ci.yml`), added across M1, M3 and M4

```yaml
  macos:
    name: macOS build + unit tests + conformance kit (arm64, x86_64 check)
    runs-on: macos-26
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
          targets: x86_64-apple-darwin
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
      - run: cargo check --workspace --all-targets --target x86_64-apple-darwin
      # Plan 31 C6's kernel-free conformance kit, run against this
      # frontend's Vfs impl and FrontendCaps. Exact invocation is plan 31's
      # to finalize (REPORTED here, not VERIFIED).
      - run: cargo test -p constellation-vfs --features conformance -- --frontend nfs

  macos-smoke:
    name: macOS NFS mount smoke (local-dir + versitygw)
    needs: macos
    runs-on: macos-26
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: brew install versitygw
      - run: cargo build -p constellation
      - name: Smoke (local directory backend, user-mode NFS mount, no sudo)
        run: FRONTEND=nfs bash tests/smoke.sh
      - name: Daemon lifecycle + mount-level NFS unit tests
        run: CONSTELLATION_TEST_NFS_MOUNT=1 cargo test -p constellation -- --ignored nfs_mount
      - name: Smoke against versitygw (conditional writes, doctor)
        run: FRONTEND=nfs S3_BACKEND=process bash tests/integration.sh
      - if: failure()
        uses: actions/upload-artifact@v4
        with: { name: macos-smoke-logs, path: "target/test-logs/**" }

  linux-nfs-smoke:
    name: Linux NFS-frontend smoke (parity reference for macOS)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: sudo apt-get update && sudo apt-get install -y nfs-common
      - run: cargo build -p constellation
      - run: FRONTEND=nfs bash tests/smoke.sh
```

### Nightly lanes (`nightly.yml`); replace today's `macos` job

Only the `linux-nfs` cell below is this plan's; `linux-fuse` and
`linux-fuse-process` are plan 31 C6's lanes, listed here only so the 2×2
shape is visible:

| Lane | OS | Frontend | S3 backend | Job | Purpose | Owner |
|---|---|---|---|---|---|---|
| `linux-fuse` | Linux | FUSE | floci (docker) | existing `harness` | reference | plan 31 C6 |
| `linux-fuse-process` | Linux | FUSE | versitygw (process) | plan 31's | backend delta | plan 31 C6 |
| `linux-nfs` | Linux | NFS | versitygw (process) | `harness-linux-nfs` (below) | frontend delta | **this plan** |
| `macos-nfs` | macOS | NFS | versitygw (process) | `harness-macos` (below) | OS delta, the thing we are proving | **this plan** |

```yaml
  harness-linux-nfs:
    # The linux-fuse and linux-fuse-process lanes are plan 31 C6's; this is
    # only the linux-nfs cell, the frontend-delta half of this plan's 2x2.
    needs: unit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: sudo apt-get update && sudo apt-get install -y fuse3 nfs-common fio stress-ng
      - run: bash tests/ci/install-native-s3.sh   # pinned versitygw + toxiproxy tarballs, sha256-checked
      - run: cargo build --release -p constellation -p constellation-harness -p constellation-chaos
      - run: |
          target/release/harness run --s3-backend process --frontend nfs \
            --results-json results-linux-nfs.json 2>&1 | tee harness-linux-nfs.log
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: "harness-linux-nfs", path: "*-linux-nfs.*" }

  harness-macos:
    needs: lint
    strategy:
      fail-fast: false
      matrix:
        runner: [macos-26, macos-15]     # macos-15 dropped if M0 sets the floor at 26
        shard: [1, 2, 3]                  # tune so each shard stays < 90 min
    runs-on: ${{ matrix.runner }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: brew install versitygw toxiproxy fio stress-ng
      - run: cargo build --release -p constellation -p constellation-harness -p constellation-chaos
      - run: |
          target/release/harness run --s3-backend process --frontend nfs \
            --shard ${{ matrix.shard }}/3 \
            --results-json results-macos-nfs-${{ matrix.runner }}-${{ matrix.shard }}.json \
            2>&1 | tee harness-macos-${{ matrix.runner }}-${{ matrix.shard }}.log
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: harness-macos-${{ matrix.runner }}-${{ matrix.shard }}
          path: "*-macos-*"

  compliance-nfs:
    needs: lint
    strategy:
      fail-fast: false
      matrix:
        runner: [ubuntu-latest, macos-26]
    runs-on: ${{ matrix.runner }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - if: runner.os == 'Linux'
        run: sudo apt-get update && sudo apt-get install -y nfs-common autoconf automake
      - if: runner.os == 'macOS'
        run: brew install autoconf automake
      - run: cargo build -p constellation
      - run: sudo -E FRONTEND=nfs bash tests/compliance.sh 2>&1 | tee compliance-nfs-${{ runner.os }}.log
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: "compliance-nfs-${{ runner.os }}", path: "compliance-nfs-*.log" }

  macos-stress-bench:
    needs: lint
    runs-on: macos-26
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: brew install versitygw toxiproxy fio stress-ng
      - run: cargo build --release -p constellation -p constellation-harness
      - run: FRONTEND=nfs bash tests/stress.sh 2>&1 | tee stress-macos.log
      - run: target/release/harness bench --json --label macos-nfs > bench-macos.json   # informational, not gated
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: macos-stress-bench, path: "*-macos.*" }

  macos-intel-smoke:
    needs: lint
    runs-on: macos-26-intel
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace
      - run: cargo build -p constellation && FRONTEND=nfs bash tests/smoke.sh

  interop:
    # Mixed-OS cluster proof without cross-runner networking: the bucket
    # travels as an artifact. Linux writes → macOS verifies+writes →
    # Linux verifies. Exercises plan 31 C1 (errnos in the journal, rdev)
    # end to end.
    needs: [unit]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: bash tests/ci/install-native-s3.sh
      - run: cargo build --release -p constellation -p constellation-harness
      - run: target/release/harness interop write --stage linux-1 --out bucket-linux-1.tar.zst
      - uses: actions/upload-artifact@v4
        with: { name: interop-linux-1, path: bucket-linux-1.tar.zst }
  interop-macos:
    needs: interop
    runs-on: macos-26
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/download-artifact@v4
        with: { name: interop-linux-1 }
      - run: brew install versitygw
      - run: cargo build --release -p constellation -p constellation-harness
      - run: target/release/harness interop verify-and-write --in bucket-linux-1.tar.zst --stage macos-1 --out bucket-macos-1.tar.zst
      - uses: actions/upload-artifact@v4
        with: { name: interop-macos-1, path: bucket-macos-1.tar.zst }
  interop-linux-verify:
    needs: interop-macos
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/download-artifact@v4
        with: { name: interop-macos-1 }
      - run: bash tests/ci/install-native-s3.sh
      - run: cargo build --release -p constellation -p constellation-harness
      - run: target/release/harness interop verify --in bucket-macos-1.tar.zst

  macos-package:
    needs: harness-macos
    runs-on: macos-26
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: "x86_64-apple-darwin,aarch64-apple-darwin" }
      - run: make dist-macos
      - run: lipo -info target/dist/*/constellation && codesign -dv target/dist/*/constellation
      - uses: actions/upload-artifact@v4
        with: { name: macos-package, path: target/dist/* }
```

- **Parity aggregation.** The `parity` job that runs `tests/parity.py` is
  defined once, in plan 31 C6. This plan adds `harness-macos`,
  `compliance-nfs` and `harness-linux-nfs` (above) to that job's `needs:`
  list, and its macOS/NFS `[[expect]]` entries (below) to
  `tests/platform-parity.toml`.
- **Interop tooling.** `harness interop` (the `write`/`verify-and-write`/
  `verify` subcommands) is plan 31 C6's. This lane uses it as follows:
  - It starts the process backend and runs a seeded, model-checked workload.
    The workload covers renames, xattrs, a refused op (so a `Refused` record
    lands in the journal), a FIFO/char-dev `mknod`, and a snapshot.
  - It exports the bucket through the S3 API: `aws s3 sync` to a plain
    directory, then `tar --zstd`. It never tars versitygw's data directory
    directly, because the posix backend keeps object metadata in xattrs, which
    do not survive a cross-OS tar round trip. Import on the other side is the
    reverse `aws s3 sync` into a fresh versitygw.
  - `aws` is preinstalled on both runner families.

### The parity contract: additions to `tests/platform-parity.toml`

```toml
# This file is owned by plan 31 (C6); this plan (34) contributes the
# macOS/NFS-specific [[expect]] entries below.
#
# Reference lane: linux-fuse. For every scenario, every lane's outcome must
# equal the reference unless an [[expect]] entry covers that (scenario, lane).
# Entries that no longer match reality fail the check too (two-way, like the
# xfstests baseline). Wildcards are allowed only for capability skips.
#
# Lane names are "<os>-<frontend>". The checker supports any number of
# lanes, each compared against the reference lane above.
#
# Reserved for plan 35 (not added by this plan): "windows-winfsp". When that
# lane exists it is expected to need its own [[expect]] entries for at least
# ClusterLocks, HardLinks and Fallocate, the way linux-nfs and macos-nfs do
# below.

[[expect]]
scenario = "*"
lanes = ["linux-nfs", "macos-nfs"]
outcome = "skipped"
cap = "Fallocate"
reason = "NFSv4.1 has no ALLOCATE/DEALLOCATE (v4.2 only)"

[[expect]]
scenario = "*"
lanes = ["linux-nfs"]
outcome = "skipped"
cap = "Xattr"
reason = "Linux NFS client supports xattrs only via NFSv4.2 RFC 8276"

[[expect]]
scenario = "*"
lanes = ["linux-nfs", "macos-nfs"]
outcome = "skipped"
cap = "FuseAbort"
reason = "scenario aborts via /sys/fs/fuse/connections; NFS has no equivalent"
```

What this plan relies on from plan 31 C6's checker (`tests/parity.py`):

- `macos-nfs` must equal `linux-nfs` for every scenario, **with no wildcards**.
  Any per-scenario macOS-only entry needs a reason that names a macOS behaviour.
  "Flaky" is never an accepted reason.
- A `failed` outcome can never be expected. Only `skipped` is.
- The shard results of one runner are merged before comparison. A scenario
  missing from every shard is a failure.

## Deferred, with the path recorded

- **macFUSE frontend.** If a user base appears that needs kernel push
  invalidation or fallocate on macOS before FSKit is viable:
  - use fuser with `features = ["macos-no-mount"]`;
  - mount through macFUSE's libfuse3 (`fuse_session_new`, then
    `fuse_session_mount`, then `fuse_session_fd`) — the #752 workaround,
    which returns the device fd for the kext backend;
  - hand that fd to `fuser::Session::from_fd` (VERIFIED un-gated, `session.rs:198`);
  - offload handlers from the single dispatch thread to a dedicated blocking pool.

  It stays kext-only (FSKit exposes no fd) and cannot be mount-tested in hosted CI.
- **Native FSKit.** A third frontend over `constellation-vfs` once macOS 27
  is the floor. Candidate crates: `objc2-fs-kit`, and `fskit-rs`, which
  bridges Swift and Rust over localhost.
- **Windows.** Not built here; it is plan 35's whole job. Plan 31 shapes
  the `constellation-vfs` contract and `constellation-platform` crate so
  plan 35 can plug WinFsp in without reworking them. Windows explicitly
  does **not** get the NFSv4.1 frontend this plan builds for macOS:
  Microsoft's own client ("Client for NFS") is NFSv2/v3 only, with no v4
  port option, so it would need the rpcbind/portmapper protocol on port
  111 (a nonstarter for a loopback-only local mount) and is capped at 32 KB
  rsize/wsize; it is also an optional Windows feature that is not
  installed by default. WinFsp is the native, always-available mount path
  on Windows, which is why plan 35 builds a WinFsp frontend rather than
  reusing `crates/frontend-nfs`.
- **Android.** Plan 36; not accommodated here beyond the general
  `Vfs`/`FrontendCaps`/`FrontendEvents` contract plan 31 already shapes for
  every frontend.
- **uid/gid mapping** between OSes, and a Homebrew tap.

## Risks

- **embednfs is young**: first commit 2026-03-07, 10 stars. Mitigations:
  - M0 is a go/no-go;
  - vendoring gives us full control;
  - this plan's M1 protocol tests and the M3 NFS pjdfstest baselines
    surround it.

  The fallback (nfs3_server, degraded mode) is pre-agreed.
- **The macOS NFS client under `hard` hangs callers while the daemon is down.**
  This is the NFS equivalent of the FUSE `ENOTCONN` window. Mitigations are
  stable handles (restart recovers), the daemon-lock takeover in M2, and
  documented `umount -f`.
- **Attribute-cache coherence without M1b is weaker than Linux FUSE's push
  invalidation.** Any scenario that depends on push invalidation shows up in
  the parity report as a *named* difference, never a silent one.
- **CI cost.** The private repo pays the macOS minute multiplier. Controls:
  - PR macOS lanes are build/test plus smoke (≈15-20 min);
  - the full macOS harness runs nightly only, sharded;
  - plan 31's `cross-check` (C0) catches most breakage, Darwin and
    Windows-library alike, for Linux-minute prices, before these macOS
    lanes have to.

## Definition of done

The CONVENTIONS gates, PLUS:

1. **macOS PR lanes green**: `macos`, `macos-smoke` and `linux-nfs-smoke`.
   (Plan 31's `cross-check` (C0) is a precondition, checked once, not
   redefined here.)
2. **Nightly matrix green**:
   - `parity` (plan 31 C6, fed by this plan's jobs) reports zero unexplained
     differences;
   - both NFS pjdfstest baselines are committed with a reason per entry;
   - `interop` → `interop-macos` → `interop-linux-verify` passes;
   - `macos-package` produces a universal, signed (at least ad-hoc) archive.
3. **Linux reference lane unchanged**: pjdfstest 8798/8798 with an empty
   baseline, and the harness full matrix passes. This is plan 31's own gate;
   this plan must not be the thing that breaks it.
4. **Docs updated**:
   - `PROGRESS.md` has a plan-34 section with the M0 results matrix;
   - `TESTING.md` covers the new macOS/NFS lanes and this plan's use of the
     `--frontend`/`--s3-backend`/`--shard`/`--results-json` flags (defined
     once, by plan 31) plus the parity file's macOS/NFS entries;
   - the README status paragraph is corrected;
   - the macOS how-to guide exists.
5. **Report**: include the per-lane pass/skip/fail tallies, the parity
   summary table (macOS/NFS rows), both NFS pjdfstest tallies, and the M0
   matrix.

## M0 results

*(filled in by the executing model)*

## Sources checked out for this plan

| Path | Ref | Used for |
|---|---|---|
| `/tmp/fuser` | v0.18.0 (9c957f7); master c0420fc | build.rs mount selection, `Session::from_fd`, n_threads/clone_fd gates, macOS INIT flags, ABI 7.19 |
| `/tmp/macfuse`, `/tmp/macfuse-library`, `/tmp/macfuse-mount`, `/tmp/macfuse.wiki` | 5.4.0 (4852a23); libfuse-2.9 fe59bf43; MFMount 68f082d | `fuse_mount_compat25` stub, FSKit fd limitation, backends, licence |
| `/tmp/fuse-t`, `/tmp/fuse-t-libfuse`, `/tmp/fuse-t.wiki` | 4a3ed37; 4ddd212 | FUSE-T mechanism, limits, licence |
| `/tmp/apple-nfs`, `/tmp/apple` | NFS-343.100.5 (93733ff); XNU vfs sources | v4.1 sessions, `namedattr`, `AF_LOCAL` transport, non-root mounts, COMMIT/close-to-open, AppleDouble fallback |
| `/tmp/embednfs` | ac1a016 (0.4.1) | trait hooks, transport, missing state hooks |
| `/tmp/nfsserve`, `/tmp/nfs3_server-0.11.0`, `/tmp/zerofs`, `/tmp/zerofs_nfsserve-0.19.1`, `/tmp/hf-mount` | as cloned 2026-09-28 | NFSv3 fallback evaluation, macOS mount options used in production, CI evidence |
| `/tmp/fskit-rs` | 0.2.0 | FSKit-from-Rust feasibility |
| `/tmp/runner-images` | 7ef9dd0 | runner labels, preinstalled tools, sudo, no Docker |
| `/tmp/versitygw`, `/tmp/seaweedfs`, `/tmp/floci`, `/tmp/moto`, `/tmp/garage`, `/tmp/rustfs`, `/tmp/s3proxy` | as cloned 2026-09-28 | native S3 emulator conditional-write support |
| `/tmp/pjdfstest`, `/tmp/pjdfstest-rs` | upstream HEADs | Darwin support in the C suite; the Rust rewrite does not build for Darwin |
| this tree | a945b05 | `cargo check --target aarch64-apple-darwin` error census (zig cc, stub `fuse.pc`) |
