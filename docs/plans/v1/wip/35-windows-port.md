# Plan 35 — Windows port: a native WinFsp frontend on the core

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context:
`docs/explanation/DESIGN.md` §10 (control plane: "one control API — unix
socket default, optional TCP"), the lock sections (`### Locks`, `### Cluster
locks`) plan 30 hardened, and `docs/explanation/GOALS.md`:

> "macOS, other Unix-like and Windows support is desirable (WinFsp) but not a
> requirement; Linux first."

GOALS.md already names WinFsp as the expected Windows approach; this plan is
that work. Code context, cited here only as evidence of the pre-core-isation
state (the milestones below target the post-plan-31 crate layout, not this):
`crates/cli/src/{fusefs.rs,fusefs_ops.rs,locks.rs,daemonize.rs,
daemon_lock.rs,node_runtime.rs,registry.rs,staging.rs,e2e_pin.rs,main.rs}`,
`crates/api/src/lib.rs` (control API, unix-socket server),
`crates/harness/src/{s3env.rs,client.rs,scenarios.rs,main.rs}`, `tests/*.sh`,
`.github/workflows/{ci,nightly}.yml`, `vendor/fjall/` (the vendoring pattern
this plan reuses for `winfsp-sys`).

Depends on **plan 31 (`31-core-frontend-backend.md`) being committed**,
specifically its milestones C1–C8:

- **C1 — `constellation-types`.** The portable `Code` errno enum (its own
  fixed `#[repr(u16)]` wire discriminant, not any OS's native errno — a
  deliberate wire-format break, no migration of pre-plan-31 state per plan
  31 §2), replacing ad hoc `libc::E*` literals on the wire and in the
  journal, plus the portable `(major, minor)` rdev pair replacing Linux
  `makedev` packing. This plan adds the Windows NTSTATUS conversion as a
  `Code`-keyed match (settled decision 11) — a new consumer of C1's enum,
  not a change to it.
- **C2 — `constellation-platform`.** The `HostServices` trait bundle
  (`dirs`, `process`, `daemon`, `file_lock`, `fs: FsPrimitives`, `secrets:
  SecretStore`, `lifecycle: LifecycleSource`, `mounts: MountTable`) with
  Linux+macOS implementations and a `windows` module that C2 leaves as a
  compile-only stub returning `io::ErrorKind::Unsupported`. This plan fills
  that stub in (W1/W4, "Host integration" below).
- **C3 — `constellation-engine`.** The mechanical, behaviour-neutral
  extraction of the engine modules out of `crates/cli` (`ConstellationFs` →
  `View`). Nothing Windows-specific here; this plan assumes the extraction is
  done so `frontend-winfsp` has an `Engine`/`View` to call into.
- **C4 — `constellation-vfs`.** The frontend contract: `trait Vfs`, `OpCtx`,
  `Caller`, `Responder<T>`, `Blocking<T>`, `CancelToken`, `DirSink`,
  `FrontendEvents`, `Invalidation`, `FrontendCaps`, the policies
  (`NamePolicy`, `XattrPolicy`, `IdentityMap`, `PolicyStack`), `OpWatch`, and
  `vfs::conformance`/`vfs::mock::MockVfs`. `crates/frontend-fuse` becomes the
  reference adapter over this contract; this plan's `crates/frontend-winfsp`
  is a second, independent adapter over the same contract.
- **C5 — the control protocol.** `crates/control` (`constellation-control`)
  replacing `crates/api`: framing, handshake, services, streaming, cancel,
  the `Transport` trait, the `UnixSocket` and `InProcess` transports, and a
  reserved `NamedPipe` stub. This plan implements that `NamedPipe` transport
  (settled decision 13) and is the first consumer of the reserved slot.
- **C6 — test architecture.** The conformance kit, `MockVfs`, proptest, the
  harness's `--frontend`/`--s3-backend process`/derived `Cap`/platform-neutral
  `Client`/`harness smoke`/`harness interop`, `make check-cross`, and
  `tests/platform-parity.toml` + `tests/parity.py` with `<os>-<frontend>`
  lane naming and the `linux-fuse` reference lane. This plan adds only
  Windows-specific harness plumbing and `windows-winfsp` parity rows on top
  of C6 — it does not rebuild any of C6 itself.
- **C7 — observability.** Unified op metrics/tracing, `OpWatch` exposed over
  control, `vfs-bench`. This plan's frontend participates by construction
  (every op goes through `OpCtx`/`Responder`); no Windows-specific work here.
- **C8 — engine profiles and lifecycle.** `EngineProfile`, `LifecycleSource`,
  suspend/resume. Windows lifecycle events (power/network) are noted under
  "Host integration" as an optional `LifecycleSource` implementation, but no
  Windows lifecycle scenario is part of this plan's milestones or gates.

Plan 33 (`33-control-plane-and-ui.md`) owns control-plane security
(roles/audit/token-auth) and the cross-platform Tauri UI app; this plan's
only dependency on it is **optional**: the Windows package can bundle the
UI's MSI (W7), and "start at login" for the UI app is plan 33's integration
point to implement (this plan's own daemon-lifecycle work stops at spawning
and detecting the headless `constellation` daemon, not the UI). Plan 34
(`34-macos-port.md`) is a sibling port (NFSv4.1 on macOS) — it is referenced
here only for shared ideas (the errno-table pattern, the platform-crate
shape, comparable semantic trade-offs, CI/parity conventions), never as a
dependency of this plan. Plan 36 (`36-android-port.md`) is unrelated to this
plan.

It is independent of plans 19 and 23. Plan 31 is being revised in parallel to
carry these seams for exactly this plan; this file describes plan 35's own
milestones against that target shape, not against whatever is in `crates/cli`
today — none of it has landed yet, so this plan holds no line-number
citations into files that plan 31 hasn't written.

The research behind this plan was done on 2026-09-28, the same day as plan
31's. Every **VERIFIED** claim was checked against a source checkout in
`/tmp` (listed at the end), an official doc (Microsoft Learn, docs.rs), or a
`gh api`/`gh release`/`gh run` call against the real repo. **REPORTED** means
a secondary source only; W0 re-checks those on a real runner before anything
is built on them.

## Why

Windows has no Constellation client story today, beyond mounting a Linux
node's export over `\\wsl.localhost\` from inside WSL2 — a stopgap, not a
port. Concretely:

- **The product does not cross-compile for Windows at all.** VERIFIED:
  `cargo check --target x86_64-pc-windows-gnu` (zig cc) from this Linux tree
  compiles `meta`, `store-s3`, `net`, `fs-core`, `mtree`, `model`, and
  `upload-concurrency` clean, but fails on:
  - `api` — `crates/api/src/lib.rs:31` imports `tokio::net::UnixListener`
    unconditionally (control API is unix-socket-only pre-plan-31);
  - `authority` — three uses of `libc::ESTALE`, which does not exist in the
    Windows CRT errno set;
  - `chaos` — `std::os::unix`, `posix_fadvise`, `ESTALE`,
    `PermissionsExt`/`MetadataExt`.

  All three are **plan 31's core work, not this plan's**: its C0 baseline
  records them as known-failing Windows (and macOS) targets, and C1 (the
  `Code` enum, fixing `authority`'s `libc::ESTALE` literals) plus C5 (the
  transport-neutral control protocol, fixing `api`'s unconditional
  `UnixListener`) land the fixes as core work that happens to unblock
  Windows too — the same fixes plan 34 relies on for macOS. `chaos` gets
  `#[cfg(unix)]` gates in plan 31 C0–C1 as well; no Windows fault-injection
  story is promised by this plan or by 31. This plan starts from a tree
  where those three crates already cross-compile clean for
  `x86_64-pc-windows-gnu`, and only re-verifies it (W1) rather than doing
  the work itself.
- **`crates/cli` is the real porting job.** VERIFIED (this tree, a945b05):

  ```
  $ grep -rn 'std::os::unix\|libc::\|/proc\|fusermount' crates/cli/src | wc -l
  313
  ```

  | File | Hits | What's there |
  |---|---|---|
  | `fusefs.rs` | 109 | `fuser` `impl Filesystem`, `OsStrExt`, `Caller::in_group` via `/proc` supplementary groups |
  | `fusefs_ops.rs` | 90 | mode/dev bits, lock types, xattr syscalls |
  | `daemon_lock.rs` | 30 | `flock(2)`, `/proc/locks`, `/proc/<pid>/status`, `/proc/<pid>/task`, `/proc/self/mountinfo` — daemon liveness/takeover and stale-mount detection |
  | `main.rs` | 18 | xattr syscalls, misc `libc::` |
  | `fuse_watch.rs` | 13 | `gettid`/`SYS_tgkill`/`pthread_kill` watchdog |
  | `daemonize.rs` | 13 | `libc::fork()` (`:166`), `pipe2`, close-on-exec via `fcntl` |
  | `locks.rs` | 11 | `i16` FUSE lock types, `ReplyEmpty` |
  | `staging.rs` | 9 | `std::os::unix::fs::FileExt`, `libc::fallocate`+`FALLOC_FL_PUNCH_HOLE` (`:352`, `:394`) for hole-punch |
  | `node_runtime.rs` | 9 | `fuser::MountOption`/`SessionACL`/`Session`/`SessionUnmounter` — the mount/unmount wiring this plan's W3/W4 replace with WinFsp on Windows |
  | `snapshot.rs`, `authority_driver.rs` | 3 each | misc |
  | `startup.rs`, `registry.rs`, `recovery.rs`, `parallelism.rs`, `e2e_pin.rs` | 1 each | `registry.rs` reads `XDG_CONFIG_HOME`/`XDG_DATA_HOME` with a `$HOME`-based fallback (`:118-122`, `:286-287`) — portable in shape, needs Windows path roots; `e2e_pin.rs:314` takes a `flock(2)` on a passphrase-cache lock file |

  The two structurally hard pieces are the same two plan 31 already
  restructures as core work: the `fuser`-shaped `impl Filesystem` in
  `fusefs_ops.rs` (→ C4's `constellation-vfs` contract, frontend-typed only)
  and the daemon/mount lifecycle in
  `daemonize.rs`/`daemon_lock.rs`/`node_runtime.rs` (→ C2's
  `constellation-platform` crate). Windows needs a fourth frontend crate
  beside `frontend-fuse` (Linux), `frontend-nfs` (34, macOS) and `frontend-saf`
  (36, Android) — `crates/frontend-winfsp` — and a `platform::windows` module
  beside `linux`/`macos` (C2 ships it as a compile-only stub; this plan fills
  it in). No new abstraction, just a new implementation of ones plan 31 is
  already building.
- **The `winfsp` frontend slot already exists.** Plan 31's crate list
  reserves `crates/frontend-winfsp` + `crates/winfsp-sys` by name for this
  plan (see the design brief's crate table); frontend selection is an open
  set keyed by whatever frontends are compiled in, so `--frontend winfsp`
  parses everywhere and fails cleanly with "not built in this binary" until
  this plan lands the crate.

Before choosing *how* WinFsp should sit on the core, the real question was
the same one plan 34 asked for macOS: **what should even try to mount a
Constellation view on Windows?**

## Decision: a native WinFsp frontend, via our own FFI crate

### The options, with evidence

| Option | Approval / driver | Mountable on GitHub-hosted runners | What semantics survive | Licence | Verdict |
|---|---|---|---|---|---|
| **Windows built-in NFS client** | None (optional feature) | Yes | NFSv2/v3 only — no OPEN/CLOSE close-fence, no NLM-arbitrated locks reaching our server cleanly, 32 KB `rsize`/`wsize` cap, soft mounts by default, needs the RPC portmapper on port 111. Microsoft Learn ("NFS overview"): the client supports "NFSv2 and NFSv3"; `mount` (windows-commands/mount) has no port option and defaults `rsize`/`wsize` to 32 KB. | n/a (OS feature) | **Rejected.** No v4.1 sessions/OPEN-CLOSE/COMMIT on the *client* side; this is not the macOS NFSv4.1 story at all. |
| **WinFsp, native FSP_FILE_SYSTEM_INTERFACE** | A signed kernel driver, installed once by the user (MSI/winget), no per-run kernel-extension dance | **Yes.** `choco install winfsp` works non-interactively; proven daily by winfsp-rs's own CI (below). | Nearly everything: OPEN/CLOSE via Create/Cleanup/Close, byte-range locks (kernel, node-local), POSIX delete/rename, reparse-point symlinks, EAs, named streams, push invalidation via `FspFileSystemNotify`, POSIX uid/mode mapping helpers. No hard links, no v4.2-style sparse FSCTLs. | GPLv3 **+ FLOSS exception** covering MPL-2.0 (`/tmp/winfsp/License.txt`) | **Chosen** |
| **WinFsp, high-level FUSE compat layer** | same driver | Yes | Same driver, but the compat layer is `libfuse`-only (no `fuse_lowlevel.h`); it re-derives `fuser`-shaped semantics on top of the *same* native interface we'd otherwise use directly, adding a translation layer for no benefit — we do not have a libfuse binding to reuse on Windows the way `fuser` reuses one on Linux. | same | **Rejected.** Extra indirection with nothing to reuse; go native. |
| **Dokany** | A signed kernel driver (LGPL) | Yes (has its own CI) | Broadly comparable coverage to WinFsp via `dokan-rust` (MIT bindings, last commit 2025-05-01) | driver+lib LGPL, dokanctl/samples MIT; `dokan-rust` MIT | **Fallback**, held for if WinFsp's licence blocks a future proprietary/commercial edition |
| **Embedded SMB3 server** | None (user-space) | No mature Rust SMB3 server exists today; loopback port 445 is owned by Windows' own Server service; alternate client ports (`/TCPPORT:`) only ship on Windows 11 24H2 / Server 2025+ | Would give driver-free installs and oplock/lease push invalidation if it existed | n/a | **Deferred.** Long-term, driver-free option; revisit if a Rust SMB3 server matures or once 24H2/2025 is a safe floor. |
| **Cloud Files API** (placeholder/sync engine) | None | n/a | Not a POSIX filesystem: no advisory locks, no synchronous open/close/fsync visibility | n/a | **Rejected** |
| **ProjFS** | None | n/a | Same shape of gap as Cloud Files API — a projection layer, not a filesystem with our semantics | n/a | **Rejected** |
| **WSL2 + Linux build, mounted via `\\wsl.localhost\`** | WSL2 install | n/a (not a native Windows client) | Full Linux FUSE semantics, but it *is* the Linux binary — no native Windows executable, no drive-letter mount from a Windows path, Explorer/every Win32 app only sees it through the UNC bridge | n/a | **Stopgap only**, already available today with zero work; documented as the interim answer, not a deliverable of this plan |

### Why native WinFsp fits Constellation specifically

VERIFIED against `/tmp/winfsp @ ebd50e1` (2026-09-22) unless marked otherwise:

- **No kernel-extension dance per CI run**, unlike macFUSE's Reduced-Security
  requirement. The driver installs once (MSI, or `choco`/`winget install
  WinFsp.WinFsp` — VERIFIED the winget manifest exists:
  `microsoft/winget-pkgs` has `manifests/w/WinFsp/WinFsp/2.1.25156`, matching
  the latest GitHub release) and every subsequent mount is just a user-mode
  connection to it.
- **It gives us the close-time flush fence and real locks**, the two things
  plan 31 had to build an entire NFSv4.1 server to get on macOS. On Windows
  they are simply kernel-driver behaviour: `Cleanup`/`Close` map onto our
  flush fence, and byte-range locks are processed in
  `src/sys/file.c:2484` (`FsRtlProcessFileLock`) with no user-mode lock
  callback at all — which also tells us plainly that locks are **node-local
  only** (see settled decision 5).
- **Push invalidation exists**, unlike the macOS NFS frontend before its
  deferred coherence work (per plan 34): `FspFileSystemNotifyBegin/Notify/NotifyEnd`
  (`inc/winfsp/winfsp.h:1247-1307`).
- **POSIX identity and permission mapping ship in the DLL**, so we are not
  inventing uid/SID mapping from scratch:
  `FspPosixMapUidToSid`/`FspPosixMapSidToUid`
  (`src/shared/ku/posix.c:540-575`) and
  `FspPosixMapPermissionsToSecurityDescriptor`/the reverse.
- **Name mapping has a prior art we can copy exactly**: `FspPosixMapPosixToWindowsPath(Ex)`
  maps `/`→`\` and Windows-illegal ASCII to `U+F000|c`
  (`posix.c:1429-1440`), reversibly (`:1475`) — the same private-use-area
  convention Cygwin and SSHFS-Win use. We do not need to invent a name
  policy; `NamePolicy` (plan 31 C4) gets a Windows implementation of an
  already-standard mapping.
- **The FLOSS exception covers us.** WinFsp is GPLv3, but the exception in
  `/tmp/winfsp/License.txt` explicitly permits linking `winfsp-x64`/`-a64.dll`
  and redistributing the unmodified installer with OSD-licensed software
  (MPL-2.0 qualifies) provided we do not link/distribute it with proprietary
  software, and that user-facing docs carry the required notice. A
  commercial licence exists if we ever need to break that constraint —
  tracked as a risk, not blocking this plan.
- **winfsp-rs proves the CI shape works today**, but its `winfsp` crate is
  GPL-3.0 (no FLOSS-exception carve-out for the Rust bindings themselves,
  unlike the DLL), which would make our Windows binary GPL. So we take the
  *pattern*, not the crate: our own FFI (`crates/winfsp-sys`, MPL-2.0,
  loading the DLL dynamically the way `FspLoad` does via the registry-located
  install dir) covered by the same FLOSS exception as linking the DLL
  directly. VERIFIED: winfsp-rs's `.github/workflows/ntptfs.yml` does
  `choco install winfsp`, mounts at `R:`, and runs
  `winfsp-tests-x64 --external --resilient +* --case-insensitive-cmp` with a
  documented exclusion list — that is the model for this plan's compliance
  lane (see W5/W6, which build on plan 31 C6). Its last 5 scheduled runs are all green:

  ```
  $ gh run list -R SnowflakePowered/winfsp-rs -w ntptfs.yml -L 5
  success  2026-09-26T03:33:45Z  36215187136
  success  2026-09-19T03:09:51Z  35417766524
  success  2026-09-12T03:09:21Z  34669597346
  success  2026-09-05T02:58:28Z  33940559001
  success  2026-08-29T05:39:58Z  33236691004
  ```
- **The errno→NTSTATUS story has real prior art, but it is numbered wrong for
  us.** WinFsp's own high-level FUSE compat layer already does this
  translation: `fsp_fuse_ntstatus_from_errno`
  (`/tmp/winfsp/src/dll/fuse/fuse.c:1132-1156`) switches on `err` using a
  table in `/tmp/winfsp/src/dll/fuse/errno.i`. **VERIFIED, and worth stating
  precisely: that table is keyed by MSVCRT/Cygwin errno *numbers*, not Linux
  glibc numbers** — e.g. its `case 39` is `STATUS_LOCK_NOT_GRANTED` (Windows
  CRT's `EDEADLOCK`-adjacent slot), where glibc's `39` is `ENOTEMPTY`. Since
  plan 31 settled decision 6 makes Linux glibc numbering canonical
  everywhere in Constellation, we cannot reuse `errno.i` as a lookup table —
  we build our own `Code → NTSTATUS` match keyed by the *symbolic* enum
  variant, using `errno.i`'s STATUS_* choices as the semantic reference
  (mapping table below).

### Rust crate ecosystem check (facts e)

- **tokio named pipes**: VERIFIED via docs.rs against our locked version
  (`Cargo.lock`: `tokio 1.53.1`) — `tokio::net::windows::named_pipe`
  (`cfg(windows)`, feature `net`) exposes `ServerOptions`, `ClientOptions`,
  `NamedPipeServer`, `NamedPipeClient`. This is the control-API transport
  Windows plugs into plan 31 C5's `Transport` trait as a second
  implementation beside `UnixSocket`.
- **`object_store`**: VERIFIED. Upstream CI (`apache/arrow-rs-object-store`,
  `.github/workflows/ci.yml`) has a `windows` job, `runs-on: windows-latest`,
  `cargo test LocalFileSystem (win64)`.
- **`iroh`**: VERIFIED. `n0-computer/iroh`'s `.github/workflows/tests.yaml`
  builds and tests `x86_64-pc-windows-msvc` on `windows-latest` as a matrix
  target (`build_and_test_windows` job).
- **`fjall`** (vendored at `vendor/fjall`): VERIFIED. No CI files ship inside
  our vendored copy, but upstream `fjall-rs/fjall`'s `.github/workflows/test.yml`
  runs its test matrix on `os: [ubuntu-latest, windows-latest, macos-latest]`.
- None of these need a vendoring patch for Windows; the risk is exclusively
  in `crates/cli`'s Unix-only glue code (table above), not the storage/P2P
  stack.

## Settled decisions

Taken from the research above; do not relitigate them.

1. **The Windows frontend is native WinFsp**, wired through `--frontend
   winfsp` (already reserved by plan 31), default on Windows. It lives in
   `crates/frontend-winfsp`, over `constellation-vfs`'s `Vfs` trait (plan 31 C4), the same
   shape as the Linux FUSE and macOS NFS frontends.
2. **Our own FFI crate, `crates/winfsp-sys`** (MPL-2.0, hand-written or
   `bindgen`-generated bindings to `FSP_FILE_SYSTEM_INTERFACE`), dynamically
   loads the installed DLL — the `winfsp-rs` `winfsp` crate (GPL-3.0) is not
   a dependency anywhere in the tree. `winfsp-sys` links against the DLL the
   way `FspLoad` locates it (registry `HKLM\SOFTWARE\WOW6432Node\WinFsp` /
   `WinFsp` install path), not a static import lib baked at build time, so
   `cargo build` never needs the WinFsp SDK installed on a non-Windows CI
   runner.
3. **Identity.** Canonical identity stays POSIX (uid/gid), matching plan 31.
   - `FspPosixMapSidToUid`/`FspPosixMapUidToSid` give the mechanical SID↔uid
     conversion for the WinFsp-assigned "posix offset" SIDs
     (`S-1-5-21-X-Y-Z-RID` ↔ `0x30000+RID` for local SAM accounts,
     `0x100000+RID` for the primary domain, per `posix.c:540-575`).
   - Each registry view record gains an optional `windows_owner = { uid, gid
     }`, mapping *that mount's* Windows user's SID (resolved once at mount
     time) to the cluster's canonical POSIX uid/gid — so a Windows user's
     files show up under their Linux teammates' expected uid, instead of a
     WinFsp posix-offset pseudo-id nobody else recognizes. Unset, files
     created from Windows carry the raw WinFsp-mapped uid/gid (still
     internally consistent, just not aligned with any Linux account).
   - Security descriptors are **synthesized from mode bits** on every read
     via `FspPosixMapPermissionsToSecurityDescriptor`; `SetSecurity` maps
     back to mode via `FspPosixMapSecurityDescriptorToPermissions` on a
     best-effort basis. A `SetSecurity` request that cannot be represented as
     mode bits (e.g. a non-trivial DACL with per-ACE-type entries beyond
     owner/group/other) is **rejected with `STATUS_INVALID_PARAMETER`**
     rather than silently dropped or silently reduced — silent reduction
     would let a user believe an ACL is enforced when it is not, which is
     worse than a visible failure. `PersistentAcls = 0` (we do not store a
     Windows-native ACL anywhere; mode bits are the only representation).
4. **Names.** `NamePolicy` (plan 31 C4) gets a Windows implementation:
   - Linux names containing Windows-illegal characters
     (`\x01`-`\x1f < > : " | ? *` and trailing dot/space) round-trip through
     the WinFsp/Cygwin `U+F000`-private-use-area convention
     (`FspPosixMapPosixToWindowsPath[Ex]`, verified reversible in
     `posix.c:1429-1440`/`:1475`). Reused, not reinvented.
   - Reserved device names (`CON`, `PRN`, `AUX`, `NUL`, `COM1`-`9`,
     `LPT1`-`9`) are **not specially blocked**: WinFsp hands us full paths,
     not raw DOS-device namespace, so a Linux file literally named `CON`
     round-trips through the normal name policy like any other name. This is
     documented rather than special-cased, because blocking it would be a
     gratuitous asymmetry with Linux (where `CON` is an ordinary filename).
   - Non-UTF-8 Linux names (Linux allows arbitrary bytes; Windows requires
     UTF-16): each invalid byte is escaped losslessly into the same
     `U+F000`-`U+F0FF` private-use band the illegal-character mapping already
     uses (one extra byte-value range, same reversible scheme), so a name
     that is merely "Windows-hostile" and a name that is "not valid UTF-8"
     share one mechanism instead of two. This is a plan-35 extension of the
     WinFsp/Cygwin convention, not something WinFsp itself defines — call
     this REPORTED-by-design rather than VERIFIED-upstream, and give it a
     protocol-level round-trip test in W3.
5. **Case.** Default is case-insensitive, case-preserving lookup
   (`CaseSensitiveSearch = 0`, `CasePreservedNames = 1`), matching ordinary
   Windows volumes:
   - An exact-case match always wins when several entries differ only by
     case.
   - A case-folded lookup that is ambiguous (multiple entries fold to the
     same name, none an exact match) fails as not-found rather than picking
     one arbitrarily.
   - All entries are listed regardless of case collisions — nothing is
     hidden by directory enumeration.
   - `Create` that would case-insensitively collide with an existing entry
     opens the existing entry (ordinary Windows semantics) only when the
     match is unambiguous; an ambiguous case-collision on create is refused
     `STATUS_OBJECT_NAME_COLLISION`.
   - A `--case sensitive` mount option sets `CaseSensitiveSearch = 1` for
     users who want Linux-matching lookup semantics (still case-preserving);
     this is the parity-testing knob, same role as macOS's NFS frontend
     reporting `case_insensitive=false`.
6. **xattrs.** First cut: `ExtendedAttributes = 0` (EAs off entirely).
   - `constellation xattr …` (plan 31 C5's `browse.xattr` control method) is the only
     way to read/write/prune Constellation's scratch and prune-policy
     "virtual" xattrs on Windows — there is no in-band `setxattr`-equivalent
     exposed to WinFsp callers.
   - Mapping Linux `user.X` xattrs onto real Windows EAs (`GetEa`/`SetEa`) is
     **deferred**: EA names are case-insensitive and the kernel upper-cases
     them, which is lossy against Linux's case-sensitive xattr namespace, and
     needs its own name-mapping design (not the path `NamePolicy` scheme).
   - `NamedStreams = 0` in the first cut too: alternate-data-stream writes
     (e.g. Explorer's `Zone.Identifier` after a download) fail the way they
     do on a FAT32 volume — no error surprise, just no ADS support. Stream
     support is deferred alongside the EA mapping.
7. **Locks and share modes are node-local.** WinFsp processes byte-range
   locks entirely in the kernel driver (`FsRtlProcessFileLock`,
   `src/sys/file.c:2484`; enforcement in `read.c`/`write.c` via
   `FsRtlCheckLockFor*Access`) and share-mode checks the same way
   (`IoCheckShareAccess`, `src/sys/file.c:734`) — there is **no lock or
   share-mode callback anywhere in `FSP_FILE_SYSTEM_INTERFACE`**. This means:
   - Windows mounts never get `ClusterLocks` (the parity `Cap`); two Windows
     nodes locking the same file do not coordinate through Constellation at
     all, only through whatever the kernel driver does locally (nothing,
     across machines).
   - This is a strictly worse position than the macOS NFS frontend (which at
     least gets LOCK/LOCKT/LOCKU to the server) or Linux FUSE (`getlk`/
     `setlk`). It is documented prominently, not softened.
8. **Coherence.** `FileInfoTimeout = 1000` ms, matching FUSE's 1 s `TTL` and
   the macOS frontend's `actimeo=1`. A `NotifySink` implementation calls
   `FspFileSystemNotify` (names normalized to correct case first — the API
   requires it, `winfsp.h:1247-1307`) wherever `kernel_inval` would call
   `inval_inode`/`inval_entry` on Linux. Unlike the macOS NFS frontend
   (deferred, per plan 34), Windows gets push invalidation from W3 — the mechanism
   already exists in the driver, so there is no "if it works" gate here.
9. **Close semantics.** `Cleanup` is the close-time flush fence (WinFsp calls
   it once per handle close, analogous to FUSE `flush`); `Close` is release;
   `Flush` maps to fsync. `SetDelete`/`SupportsPosixUnlinkRename` (volume
   param) select POSIX delete-on-last-close/unlink semantics instead of
   Windows's default delete-pending-blocks-open-handles behaviour, matching
   `constellation-vfs`'s existing `open_unlinked: DeleteOnClose` model (plan
   31 C4) rather than adding a second one.
10. **Symlinks and special files.**
    - Symlinks are reparse points (`IO_REPARSE_TAG_SYMLINK`); relative
      targets go through `NamePolicy`'s `/`↔`\` translation, absolute POSIX
      targets are exposed verbatim (and are dangling by construction on
      Windows — documented, not fixed).
    - Creating a symlink from Windows needs `SeCreateSymbolicLinkPrivilege`
      or Developer Mode enabled — documented in the how-to guide, not worked
      around (this is a Windows platform requirement, not something WinFsp
      or we control).
    - Hard links are `STATUS_NOT_SUPPORTED` — a WinFsp limitation, not a
      Constellation choice (`doc/NTFS-Compatibility.asciidoc`,
      `src/sys/fileinfo.c:963,2025`). A hard link **created from Linux**
      still shows up on Windows as two independent names over the same
      content (Windows just can't create a third one, or observe that the
      two names share an inode).
    - FIFOs/sockets/device files created from Linux are listed with
      `FILE_ATTRIBUTE_SYSTEM`; opening one for data access on Windows fails
      `STATUS_ACCESS_DENIED`.
11. **errno → NTSTATUS mapping** is a `Code`-keyed (plan 31 C1) match in
    `crates/frontend-winfsp`, informed by but **not copy-pasted
    from** WinFsp's `errno.i` (see the errno-numbering note above). Initial
    table (symbolic, so the Linux-number-vs-Windows-number mismatch in
    `errno.i` cannot bite us):

    | `Code` | NTSTATUS | Source of the choice |
    |---|---|---|
    | `ENOENT` | `STATUS_OBJECT_NAME_NOT_FOUND` | `errno.i` case 2 |
    | `EACCES`, `EPERM` | `STATUS_ACCESS_DENIED` | `errno.i` case 1/13 |
    | `EEXIST` | `STATUS_OBJECT_NAME_COLLISION` | `errno.i` case 17 |
    | `EXDEV` | `STATUS_NOT_SAME_DEVICE` | `errno.i` case 18 |
    | `ENOTDIR` | `STATUS_NOT_A_DIRECTORY` | `errno.i` case 20 |
    | `EISDIR` | `STATUS_FILE_IS_A_DIRECTORY` | `errno.i` case 21 |
    | `EINVAL` | `STATUS_INVALID_PARAMETER` | `errno.i` case 22 |
    | `ENOSPC` | `STATUS_DISK_FULL` | `errno.i` case 27/28 |
    | `EROFS` | `STATUS_MEDIA_WRITE_PROTECTED` | `errno.i` case 30 |
    | `EMLINK` | `STATUS_TOO_MANY_LINKS` | `errno.i` case 31 |
    | `EPIPE` | `STATUS_PIPE_BROKEN` | `errno.i` case 32 |
    | `ENAMETOOLONG` | `STATUS_NAME_TOO_LONG` | `errno.i` case 38 (Windows branch) |
    | `EDEADLK` | `STATUS_POSSIBLE_DEADLOCK` | `errno.i` case 36 (Windows branch) |
    | `EAGAIN`/`EWOULDBLOCK` (lock path) | `STATUS_LOCK_NOT_GRANTED` | `errno.i` case 39 (Windows branch) — never reached in practice per decision 7, kept for `flock`-shaped internal callers |
    | `ENOTEMPTY` | `STATUS_DIRECTORY_NOT_EMPTY` | `errno.i` case 41 (Windows branch) |
    | `EBUSY` | `STATUS_DEVICE_BUSY` | `errno.i` case 16 |
    | `EIO` | `STATUS_IO_DEVICE_ERROR` | `errno.i` case 5 |
    | `ENOMEM` | `STATUS_INSUFFICIENT_RESOURCES` | `errno.i` case 7/12 |
    | `ELOOP` | `STATUS_REPARSE_POINT_NOT_RESOLVED` | `errno.i` case 114 (Windows branch) |
    | `ENODATA`/`ENOATTR` | `STATUS_NO_EAS_ON_FILE` | not in `errno.i` (FUSE compat has no EA concept); our own choice, matched to `GetEa`'s documented no-EA response |
    | `ENOSYS`, `EOPNOTSUPP`/`ENOTSUP` | `STATUS_NOT_SUPPORTED` | **deliberate deviation** from `errno.i`'s default-case fallback of `STATUS_ACCESS_DENIED` — that default exists because libfuse ops that return an unmapped errno are rare and usually really are permission refusals; we have a fully enumerated `Code` for every errno in the tree (plan 31 C1), so nothing should hit a default case, and if something does, `STATUS_NOT_SUPPORTED` is a far less misleading fallback than "access denied" |
    | *(default, should be unreachable)* | `STATUS_NOT_SUPPORTED` | logged once, like plan 31's unknown-errno-maps-to-`EIO` rule on the wire side |

    Round-trip tested the same way as plan 31 C1's wire table (one test per
    row) plus a fallback-unreachable assertion (`Code`'s variants are a
    closed enum; a `match` with no wildcard arm makes "unreachable" a compile
    error, not a runtime hope).
12. **rdev.** Reuses plan 31 C1's portable `(major, minor)` rdev pair;
    Windows has no native device-number concept at the filesystem layer for
    regular mounts, so mknod'd char/block devices from Linux carry their
    portable `(major, minor)` value opaquely (listed, not openable for
    data, same as FIFOs/sockets).
13. **Host integration.**
    - Control API transport: a named pipe,
      `\\.\pipe\constellation-<fs-uuid>`, with a DACL granting access only to
      the mounting user's SID (owner-only, the named-pipe equivalent of a
      0700 unix-socket directory). Plugs into plan 31 C5's `Transport`
      abstraction as a second implementation beside the unix socket.
    - Daemon spawn: `CreateProcess` with `DETACHED_PROCESS`, keeping the
      existing readiness-pipe protocol (a Windows anonymous pipe stands in
      for the unix `pipe2`/`O_CLOEXEC` pair in `daemonize.rs`). No fork
      equivalent is attempted — Windows has none, and `posix_spawn`-after-init
      (macOS's approach) has no Windows analogue either; `CreateProcess`
      re-execs cleanly from a fresh process image, so there is no
      post-init-state problem to work around.
    - An installable Windows **service** wrapper is explicitly **deferred**
      (out of scope for this plan); `mount` stays a foreground-or-detached
      CLI flow like Linux/macOS.
    - Mount target: a drive letter (`X:`) or an empty NTFS directory
      (WinFsp supports directory mounts same as drive letters).
    - Paths: `%APPDATA%\constellation` (registry, `node.key`) and
      `%LOCALAPPDATA%\constellation` (state/cache) — the Windows-idiomatic
      analogue of plan 31's XDG choice on macOS, made explicitly rather than
      reusing `$HOME`-based fallbacks that don't exist on Windows the same
      way.
    - File locks (host-side, e.g. the daemon-lock file itself, and
      `e2e_pin.rs`'s passphrase-cache lock): `LockFileEx`, the Windows
      equivalent of `flock(2)`, behind `platform::file_lock` alongside the
      existing Unix `flock` call sites.
    - Harness process control (suspend/resume/kill for chaos scenarios):
      `NtSuspendProcess`/`NtResumeProcess`/`TerminateProcess` through plan
      31's `platform::process` abstraction, mirroring its libproc-based
      macOS implementation.
14. **Targets:** `x86_64-pc-windows-msvc` and `aarch64-pc-windows-msvc`. MSVC,
    not `-gnu` — WinFsp's own headers and import libs assume MSVC ABI/tooling,
    and every reference (winfsp-rs, the ntptfs CI, `runner-images`) builds
    MSVC.
15. **Packaging.** A zip with `constellation.exe`, `LICENSE`, `README`, and
    the WinFsp FLOSS-exception notice text. WinFsp itself is **not** bundled
    in the first cut — users install it via `winget install WinFsp.WinFsp` or
    the MSI — even though the exception would permit shipping the unmodified
    installer, because bundling adds a second thing to keep in sync with
    upstream WinFsp security releases for no first-cut benefit; revisit if
    the winget/MSI install step turns out to be a real adoption blocker.
    The headless `constellation.exe` zip is this plan's only required
    artifact; a bundled `.msi` of plan 33's UI app is **optional** and, if
    plan 33 has published one by W7, `windows-package` may attach it
    alongside the zip — this plan does not build or own the UI's installer,
    it only has a slot for it (see W7).

## Semantic differences: Linux FUSE vs Windows WinFsp

This is the contract the parity file (W6) encodes.

| Behaviour | Linux FUSE (reference) | Windows WinFsp |
|---|---|---|
| Entry/attribute caching | 1 s TTL + push `inval_entry`/`inval_inode` | `FileInfoTimeout=1000ms` + push via `FspFileSystemNotify` (available from day one, no deferred gate) |
| Close-to-open / flush fence | `flush` per `close(2)` with lock_owner | `Cleanup` (per handle close) |
| fsync | `fsync` | `Flush` |
| Cross-node advisory locks | `getlk`/`setlk` → Constellation's lock service | **None.** Kernel-processed, node-local; `ClusterLocks` cap absent |
| Share-mode enforcement | n/a (POSIX has no share modes) | Kernel-enforced (`IoCheckShareAccess`), node-local, invisible to Constellation |
| User xattrs, scratch/prune markers | `user.*` in-band | Control-API only (`constellation xattr`); no in-band EA mapping in the first cut — `Xattr` cap absent |
| Virtual rsize/rcount | Listed and readable | Readable via control API only |
| fallocate / punch hole / SEEK_HOLE | Supported | `STATUS_NOT_SUPPORTED` — no sparse FSCTLs used; `Fallocate`/`SeekHole` caps absent |
| Hard links | Supported | `STATUS_NOT_SUPPORTED` on create; Linux-created hard links show as independent names |
| Symlinks | Native | Reparse points; creation needs `SeCreateSymbolicLinkPrivilege`/Developer Mode; absolute POSIX targets dangle |
| Special files (FIFO/socket/dev) | Fully functional | Listed (`FILE_ATTRIBUTE_SYSTEM`), `STATUS_ACCESS_DENIED` on data open |
| Open-then-unlinked files | FUSE keeps the inode | `SupportsPosixUnlinkRename`/`SetDelete` give POSIX unlink-on-last-close semantics directly — no silly-rename needed (unlike macOS NFS) |
| Permission checks | Kernel `DefaultPermissions` + our checks | Security descriptor synthesized from mode bits (`FspPosixMapPermissionsToSecurityDescriptor`); `SetSecurity` best-effort mapped back, non-representable ACLs refused |
| Case sensitivity | Sensitive | Insensitive+preserving by default (`--case sensitive` opts into sensitive) |
| Unicode normalisation | Bytes | UTF-16 on the wire; non-UTF-8 Linux names escaped via the `U+F000` private-use band (settled decision 4) |
| Timestamps | Linux `timespec` (ns) | Windows `FILETIME` (100 ns ticks since 1601); includes a **creation time** field Linux has no direct equivalent for (synthesized, documented) |
| mmap | Supported via page cache | Supported (WinFsp advertises memory-mapped I/O) |
| Daemon killed | `ENOTCONN` until `fusermount3 -uz` | Driver reports the mount as disconnected; a clean `umount`-equivalent (`net use /delete` or WinFsp's unmount) is required to detach |
| ADS / `Zone.Identifier` | n/a | `NamedStreams=0` in the first cut: ADS writes fail gracefully, like FAT32 |

Mixed Linux/Windows/macOS clusters are in scope for reads and writes at the
storage layer (S3 + journal are OS-agnostic once plan 31 C1 lands); this plan
does not attempt uid/gid alignment beyond settled decision 3's optional
`windows_owner` mapping, and does not attempt a live interop test beyond the
artifact-relay pattern in W6.

## Milestones

Each milestone ends with the CONVENTIONS gates green on Linux **and** the
Windows lanes that exist at that point green. The Linux pjdfstest lane must
stay a FULL pass after every milestone; nothing here should touch Linux
behaviour except where a shared core abstraction (plan 31's `Code`,
`constellation-vfs`, `constellation-platform`) gains a Windows arm.

### W0 — Spike on `windows-latest`: go/no-go for the native-FFI approach (timebox: 3 days)

**What to build.** A throwaway example outside the product,
`crates/winfsp-sys/examples/probe.rs`:
- a minimal in-memory filesystem implementing enough of
  `FSP_FILE_SYSTEM_INTERFACE` (Create/Open/Read/Write/Close/GetFileInfo/
  ReadDirectory) through a hand-written FFI surface loaded from the installed
  DLL;
- mounted at a drive letter on `windows-latest` via `choco install winfsp`;
- a `FspFileSystemNotify` call proven to reach a change-notification
  listener (`ReadDirectoryChangesW` from a second process);
- `winfsp-tests-x64 --external` run against the probe mount, to see the
  baseline failure set *before* any real semantics are implemented.

Commit a temporary workflow `.github/workflows/winfsp-probe.yml`
(`workflow_dispatch` only). Record a matrix of answers under "W0 results".
Questions:

1. Does the hand-written FFI load the DLL and call into it correctly without
   the `winfsp-sys` (winfsp-rs) crate's generated bindings as a starting
   point, or is regenerating a subset of them with `bindgen` against
   `inc/winfsp/winfsp.h` faster and safer? Record the decision and why.
2. Does `choco install winfsp` succeed non-interactively on `windows-latest`
   without a reboot? (REPORTED confidence from winfsp-rs CI; confirm here.)
3. Does `FspFileSystemNotify` reach a `ReadDirectoryChangesW` watcher within
   the 1 s `FileInfoTimeout` window, with correctly case-normalized names?
4. Do two processes opening the same file with conflicting byte-range locks
   observe kernel-only, node-local blocking exactly as settled decision 7
   predicts (i.e. confirm there is truly no way to intercept lock requests
   in user mode)?
5. What NTSTATUS does `winfsp-tests --external` produce for hard-link
   creation, fallocate-shaped ops, and EA ops against the bare probe (sanity
   check against the "not supported" predictions in settled decisions 6/10)?
6. Throughput/create-rate sanity check against the in-memory probe (≥
   200 MB/s sequential, ≥ 2k creates/s) — informational, not a gate.
7. Does `aarch64-pc-windows-msvc` cross-compile and, if a `windows-11-arm`
   runner is available to this account, mount successfully there too?

**Pre-agreed consequences.**

- **Hand-written FFI is materially harder than expected:** fall back to
  `bindgen` against the WinFsp SDK headers, still producing our own
  MPL-2.0 `winfsp-sys` crate (not depending on winfsp-rs's GPL-3.0 crate).
- **`choco install winfsp` fails non-interactively:** use the MSI silently
  (`msiexec /i winfsp.msi /quiet`) instead; document both paths.
- **Push invalidation does not reach watchers reliably:** treat coherence as
  TTL-only for the first cut (same fallback posture plan 34 takes before its
  own coherence work lands), file
  it as a Windows follow-up, and drop `PushInval` from the Windows `Cap` set.
- **`aarch64-pc-windows-msvc` doesn't mount or a runner isn't available:**
  ship it as build-only (`cargo check`/`cargo build`, no mount tests) until a
  runner exists; do not block the x86_64 lane on it.
- **More than two of the above fail, or the FFI approach is unworkable at
  all:** stop and report. Dokany (MIT `dokan-rust` bindings) is the
  pre-agreed fallback, evaluated in a revised plan.

### W1 — Windows compiles; mount-less tests pass; a PR lane guards it

- **Not this plan's work, only its precondition:** the three cross-compile
  failures found above (`api`, `authority`, `chaos`), the generic
  `constellation-platform`/`constellation-vfs`/`constellation-engine`/
  `constellation-control` extraction, and `make check-cross` (the
  Linux-side zig-cc type-check gate, including its Windows GNU-target leg)
  are plan 31 C0–C6, already built as core work before this plan starts.
  This milestone's only job is to confirm the tree plan 35 starts from
  actually has them: `cargo check --workspace --target
  x86_64-pc-windows-gnu` is clean except for the pieces this plan itself
  adds (`crates/winfsp-sys`, `crates/frontend-winfsp`,
  `platform::windows`'s still-stubbed body). If plan 31 has not landed by
  the time this plan starts, stop and wait rather than re-deriving its
  fixes here — duplicating them would fork the `Code`/`Transport`
  abstractions plan 31 owns.
- What *is* this plan's work, because it is Windows-specific and plan 31's
  C2 stub deliberately leaves it unimplemented: fill
  `platform::windows`'s `HostServices` (daemon-lock liveness/takeover,
  `daemonize`'s process-spawn path, `staging`'s hole-punch, dirs). This is
  scoped in full under "Host integration" (W4) rather than duplicated here;
  W1 only lands enough of it (dirs, file locks) to make `cargo test
  --workspace` pass mount-less on `windows-latest`.
- `fuse_watch.rs`'s stalled-op watchdog is **not ported**: plan 31 C4/C7
  generalise it into `constellation-vfs`'s `OpWatch`, which this plan's
  frontend (W3) uses directly — there is nothing Windows-specific left to
  write here, only a call site.
- `crates/frontend-fuse` (plan 31 C4) is untouched by this plan — this
  milestone only adds a sibling `crates/frontend-winfsp` that does not
  exist yet (built in W3).
- **Gates:**
  - `cargo clippy --workspace --all-targets -- -D warnings` and
    `cargo test --workspace` pass on `windows-latest`;
  - `cargo check --workspace --all-targets --target aarch64-pc-windows-msvc`
    passes (build-only, per W0's ARM contingency);
  - `make check-cross` (plan 31 C0/C6) stays clean with the Windows target
    included — this plan does not add a second cross-check tool.
- **Add the `windows` PR job** (see CI) in this milestone, not later.

### W2 — `crates/winfsp-sys`: the FFI crate

- Vendor/generate bindings for exactly the surface used: the
  `FSP_FILE_SYSTEM_INTERFACE` vtable, `FSP_FSCTL_VOLUME_PARAMS`,
  `FspFileSystemCreate`/`Delete`/`StartDispatcher`/`StopDispatcher`,
  `FspFileSystemNotify*`, the `FspPosixMap*` helpers, and `FspLoad`-style
  dynamic DLL resolution via the registry install path (`HKLM ...\WinFsp` —
  confirmed present by W0's probe).
- `CONSTELLATION-PATCH.md`/`UPSTREAM-ISSUE.md`-style notes are not needed
  here (this is bindings we wrote, not a vendored fork of someone else's
  source, unlike `vendor/fjall`/`vendor/embednfs`) — but the crate's
  `README.md` documents exactly which WinFsp release (`2.1.25156` at time of
  writing) the bindings were generated/hand-checked against, and the FLOSS
  exception's notice-text requirement.
- Unit tests: struct layout/size assertions against the real header
  (`#[cfg(test)]` `assert_eq!(size_of::<...>(), N)` for the volume-params and
  interface structs), and a loop-back "load the DLL, call
  `FspFileSystemCreate` with a null interface, expect a clean error" smoke
  test gated `#[cfg(windows)]` + `#[ignore]` unless WinFsp is installed.
- **Gate:** `crates/winfsp-sys` builds and its non-`#[ignore]` tests pass on
  `windows-latest`; it builds (type-check only) on Linux via the
  `x86_64-pc-windows-gnu` cross-check from W1.

### W3 — The WinFsp frontend over `constellation-vfs`

- `crates/frontend-winfsp` (using `crates/winfsp-sys`) implements every
  `FSP_FILE_SYSTEM_INTERFACE` callback listed in the verified-facts header
  (Create/Open/Overwrite/Cleanup/Close/Read/Write/Flush/GetFileInfo/
  SetBasicInfo/SetFileSize/CanDelete/Rename/GetSecurity/SetSecurity/
  ReadDirectory/reparse-point family/GetEa/SetEa/DispatcherStopped etc.) as
  thin translation to plan 31 C4's `trait Vfs`, calling each op with a
  `Responder` the way `crates/frontend-fuse` and plan 34's `frontend-nfs`
  do — the same adapter shape, a third implementation of the same contract.
- **Completion model, verified against `/tmp/winfsp/inc/winfsp/winfsp.h`
  (`ebd50e1`):** WinFsp's asynchronous-completion facility is narrower than
  "any op can defer" — it is documented per-callback, not as a blanket
  property of the interface:
  - Only **three** callbacks are documented as accepting `STATUS_PENDING`:
    `Read` ("STATUS_PENDING is supported allowing for asynchronous
    operation", `winfsp.h:437-438`, callback at `:441`), `Write`
    (`winfsp.h:467-468`, callback at `:471`) and `ReadDirectory`
    (`winfsp.h:696-697`, callback at `:703`). Every other callback we
    implement (`Create`, `Cleanup`, `Close`, `Flush`, `SetDelete`, `Rename`,
    `GetSecurity`/`SetSecurity`, the reparse-point family, `GetEa`/`SetEa`,
    …) has no `STATUS_PENDING` mention in its doc comment, and `Cleanup`/
    `Close` are declared `VOID`, not `NTSTATUS` (`winfsp.h:410`, `:420`), so
    they cannot report a pending status at all even in principle.
  - The deferral mechanism itself: `FspFileSystemSendResponse(FSP_FILE_SYSTEM
    *FileSystem, FSP_FSCTL_TRANSACT_RSP *Response)` (declared
    `winfsp.h:1240-1241`, documented `:1219-1238` — "This call is not
    required when the user mode file system performs synchronous processing
    of requests... allowed to return STATUS_PENDING to postpone sending a
    response to the FSD. At a later time the file system can use
    FspFileSystemSendResponse to send the response", explicitly scoped to
    Read/Write/ReadDirectory in that same comment) sends the deferred
    response, built from the operation context obtained via
    `FspFileSystemGetOperationContext()` (`winfsp.h:1318`) at the time the
    original callback returned `STATUS_PENDING`.
  - **Consequence for the `Responder` mapping:** `read`, `write` and
    `readdir` are the only ops where this frontend's `Responder`
    implementation may genuinely detach from the calling WinFsp dispatcher
    thread and complete later via `FspFileSystemSendResponse` (used for the
    engine's existing deferred cases — cold S3/peer fetch beyond the inline
    budget, admission waits). Every other `Vfs` op is driven through
    `Blocking<T>` (plan 31 C4): the dispatcher thread parks until the
    engine's responder fires, then returns the `NTSTATUS` synchronously,
    because WinFsp gives that callback no other way to answer. This is
    *not* a limitation Constellation introduces — it is what the vtable
    allows — and it is why the dispatcher needs enough threads (below) to
    avoid head-of-line blocking on ops that must stay synchronous, such as
    a `lock_acquire` reached indirectly (settled decision 7 means this
    should never actually block cluster-wide, but a local wait is still
    possible).
  - Locks/leases/forward round trips reached from `Create` or another
    synchronous-only callback still defer *inside the engine* (a Tokio
    task waits on the lease), but the WinFsp-facing thread blocks on that
    engine-side future via `Blocking<T>` rather than returning
    `STATUS_PENDING` — the deferral is invisible to WinFsp for anything but
    read/write/readdir.
- **Op mapping** (settled decision 9, restated as the `Vfs` call each
  callback makes): `Cleanup` → `Vfs::flush` (the close-time fence, called
  once per handle close, `Flags` telling us whether a delete was requested —
  see `SetDelete`/`CanDelete`, `winfsp.h:900-916`); `Close` → `Vfs::release`
  (last close of the handle); `Flush` → `Vfs::fsync(Durability::Durable)`
  (`winfsp.h:476-492` — the FSD already flushed its own caches before
  calling us, so this is purely our S3/durability barrier); `SetDelete` →
  the delete/unlink-on-close semantics `Vfs`'s `open_unlinked:
  DeleteOnClose` cap already models, via `SupportsPosixUnlinkRename`
  (settled decision 9) rather than a second silly-rename path. Cancellation:
  a WinFsp-cancelled IRP surfaces to us as a `CancelToken` set on the
  `OpCtx` (plan 31 C4's cancellation barrier), checked at the engine's wait
  points exactly as FUSE INTERRUPT and NFS disconnect are.
- **Windows `FrontendCaps`** (plan 31 C4), declared once for this frontend
  and consumed both by the engine and by the harness's derived `Cap` (C6):
  - `push_inval: Full` (via `FspFileSystemNotify`, present from day one
    unless W0 downgrades it — see settled decision 8 and the W0
    contingency table);
  - `per_close_flush: true` (`Cleanup` is the fence);
  - `cluster_locks: false` (settled decision 7 — no lock/share-mode
    callback exists in `FSP_FILE_SYSTEM_INTERFACE`);
  - `xattrs: None`, `virtual_xattrs_listed: false` (settled decision 6 —
    `ExtendedAttributes = 0`, control-API only);
  - `hard_links: false` (settled decision 10, `STATUS_NOT_SUPPORTED`);
  - `fallocate: false`, `seek_hole: false` (no sparse FSCTLs surfaced
    through this frontend; `staging.rs`'s own hole-punch is host-side disk
    I/O via `platform::windows`, not a `Vfs` capability — see W4/settled
    decision 13);
  - `special_files: true` (FIFOs/sockets/dev nodes from Linux are listed
    with `FILE_ATTRIBUTE_SYSTEM`, per settled decision 10; documented that
    opening one for data access is `STATUS_ACCESS_DENIED`, so the cap means
    "visible", not "usable");
  - `case: InsensitivePreserving` by default, `Sensitive` under `--case
    sensitive` (settled decision 5);
  - `max_io`: measured in W0's throughput probe and fixed here, not
    guessed in advance;
  - `deferrable: {Read, Write, ReadDir}` (per the completion-model note
    above; every other op completes on the WinFsp dispatcher thread);
  - `open_unlinked: DeleteOnClose`.
- `NamePolicy`'s Windows implementation (settled decision 4: `U+F000`
  private-use-area mapping for illegal characters and non-UTF-8 bytes,
  case-insensitive-preserving lookup with exact-match priority per settled
  decision 5) and `IdentityMap`'s Windows implementation (settled decision
  3: `FspPosixMapSidToUid`/`FspPosixMapUidToSid` for the mechanical
  SID↔uid conversion, plus the optional `windows_owner` mapping) are both
  plugged into plan 31 C4's `PolicyStack` as the Windows instance of the
  same adapter Linux and macOS install — no new dispatch mechanism, two new
  trait impls. `XattrPolicy`'s Windows instance is the degenerate case
  (settled decision 6: nothing passes through it in the first cut, since
  `xattrs: None`).
- Security descriptor synthesis/parsing (settled decision 3), the errno
  table (settled decision 11), and reparse-point symlinks (settled decision
  10) land here as the remaining per-callback logic that isn't a policy or
  a direct `Vfs` call.
- Every callback runs on a **dedicated blocking pool**, sized like the FUSE
  worker pool (`parallelism.rs`, `2×sqrt(CPUs)`, cap 64) — most `Vfs` calls
  block via `Blocking<T>` per the completion-model note above, and must
  never run on a tokio worker (CONVENTIONS "Never block the tokio
  runtime"). WinFsp's own dispatcher threading
  (`FspFileSystemStartDispatcher` with N threads) is configured to match,
  so we are not fighting two thread pools against each other.
- `FrontendEvents` implementation (plan 31 C4, the generalised
  `kernel_inval`/`NotifySink`): calls `FspFileSystemNotify`, with the
  name-normalization requirement (settled decision 8) handled by looking up
  the canonical on-disk case before notifying, and each `Invalidation`
  variant (`Entry`/`Attr`/`Data`/`Deleted`/`Fenced`/`ViewClosing`) mapped to
  the matching `FSP_FSCTL_NOTIFY_INFO` action.
- **Tests:**
  - Protocol-level unit tests against `constellation-vfs`'s `MockVfs` (plan
    31 C6) and against `Engine`+`View` backed by `Meta::open_in_memory()` +
    `InMemory` object store, driving the `FSP_FILE_SYSTEM_INTERFACE`
    callbacks directly (no real mount) — covers create/write/close-fence,
    case-collision handling, name round-trips (illegal chars and
    non-UTF-8 escaping), security-descriptor synthesis, the errno table's
    round-trip, and the STATUS_PENDING/`FspFileSystemSendResponse` path for
    read/write/readdir specifically.
  - These run on `windows-latest` only (the vtable types are
    Windows-specific), unlike plan 31's conformance kit, which runs the
    same `vfs::conformance` suite against every frontend's
    `PolicyStack`+`FrontendCaps` combination on every OS.
  - Mount-level twins are `#[ignore]` unless
    `CONSTELLATION_TEST_WINFSP_MOUNT=1`, matching plan 34's
    `CONSTELLATION_TEST_NFS_MOUNT` convention.
- **Gate:** CONVENTIONS gate set on Linux unchanged; the `vfs::conformance`
  kit passes against this frontend's `PolicyStack`+`FrontendCaps` on
  `windows-latest`; on Windows, `cargo test --workspace` plus the new
  mount-level tests (with WinFsp installed) pass.

### W4 — Windows host integration

`constellation-platform` (plan 31 C2) gains its `windows` module, replacing
the compile-only stub C2 ships, and implements every field of `HostServices`
(`{ dirs, process, daemon, file_lock, fs: FsPrimitives, secrets: SecretStore,
lifecycle: LifecycleSource, mounts: MountTable }`):

- **`dirs`.** `%APPDATA%\constellation` for the registry and `node.key`,
  `%LOCALAPPDATA%\constellation` for state/cache (settled decision 13) — the
  Windows-idiomatic split standing in for Linux's `XDG_CONFIG_HOME`/
  `XDG_DATA_HOME`, plugged into the same `dirs` trait method Linux/macOS
  already implement, not a special-cased path anywhere else in the tree.
- **`daemon`.** `CreateProcess` with `DETACHED_PROCESS`, keeping the
  existing readiness-pipe protocol: a Windows anonymous pipe stands in for
  the unix `pipe2`/`O_CLOEXEC` pair (settled decision 13). No fork
  equivalent is attempted — `CreateProcess` re-execs cleanly from a fresh
  process image, so there is no post-fork-state problem to work around, the
  way there is on Linux/macOS.
- **`process`.** Liveness: `OpenProcess`+`GetExitCodeProcess` instead of
  `/proc/<pid>/status`. Harness process control (suspend/resume/kill for
  chaos scenarios, W5): `NtSuspendProcess`/`NtResumeProcess`/
  `TerminateProcess`, mirroring the libproc-based macOS implementation
  behind the same trait method. There is no `/proc/locks`-equivalent, so
  daemon-lock liveness/takeover instead uses `LockFileEx`'s own semantics
  (the lock is self-releasing on process exit, so a failed non-blocking
  `LockFileEx(LOCKFILE_FAIL_IMMEDIATELY)` unambiguously means "someone
  alive holds it", without a separate pid lookup the way Linux's
  advisory-only `flock` needs one). Stale-mount detection is a
  short-timeout probe `stat`-equivalent against the mount root, watching
  for the WinFsp-reported disconnected state. `Caller::in_group`: WinFsp
  requests carry the caller's token; group membership comes from
  `GetTokenInformation(TokenGroups)`, not `getgrouplist`. Hostname:
  `GetComputerNameExW`. Open-file-limit equivalent: Windows has no
  per-process `RLIMIT_NOFILE` to raise; document that the relevant ceiling
  is the system-wide handle count, not tuned by this plan.
- **`file_lock`.** `LockFileEx`, the Windows equivalent of `flock(2)`, for
  every host-side lock file site: `daemon.lock` itself and
  `e2e_pin.rs`'s passphrase-cache lock.
- **`fs: FsPrimitives`.** `FSCTL_SET_SPARSE` + `FSCTL_SET_ZERO_DATA` on the
  local staging file for hole-punch (replacing `fallocate`+
  `FALLOC_FL_PUNCH_HOLE`; this is local disk I/O for `staging.rs`'s own
  cache file, unrelated to the WinFsp frontend's `fallocate`/`seek_hole`
  `FrontendCaps`, which stay `false` per W3 — a mounted view exposes no
  sparse operations to Windows callers even though the *local* staging
  store still uses them internally); `FlushFileBuffers` for the local
  fsync-strength primitive, the Windows analogue of `F_FULLFSYNC`/plain
  `fsync(2)`.
- **`secrets: SecretStore`.** **Chosen: DPAPI**
  (`CryptProtectData`/`CryptUnprotectData`, user-scope, with
  `CRYPTPROTECT_UI_FORBIDDEN` so a headless CI runner never blocks on a
  prompt), not Windows Credential Manager. Reasons:
  1. DPAPI keeps the same "encrypted blob on disk under `dirs`" shape the
     Linux file-backed `SecretStore` already uses (plan 31 C2) — this
     module only adds an encryption step, not a second storage paradigm
     with its own enumeration/lookup API.
  2. Credential Manager's generic-credential blob is capped at roughly
     2560 bytes per entry; `node.key` plus the E2E pin cache comfortably
     fits today, but DPAPI has no such per-secret ceiling if the stored
     bundle grows.
  3. Credential Manager entries can roam via a Microsoft account on some
     Windows configurations; `node.key` is a per-machine node identity and
     must **not** silently propagate to a second machine through account
     roaming. DPAPI user-scope protection has no such roaming path.
  4. DPAPI is well-understood in headless/CI contexts (`windows-latest`
     runners keep a loaded user profile); Credential Manager's Vault has
     had CI/service-context friction reported by other projects. This is
     REPORTED, not independently verified on `windows-latest` — W1/W4's
     gate run confirms it for this project specifically.
- **`lifecycle: LifecycleSource`.** **Optional in this plan**, REPORTED
  approach only: `RegisterPowerSettingNotification`/`WM_POWERBROADCAST` for
  power events and `NotifyIpInterfaceChange`/`NotifyAddrChange` for network
  changes, feeding plan 31 C8's `Suspending`/`Resumed`/`NetworkChanged`
  events. Not gated by this plan's milestones or Definition of done — C8's
  suspend/resume semantics are exercised by Linux/macOS harness scenarios
  only; a Windows implementation is a follow-up, tracked under "Deferred".
- **`mounts: MountTable`.** Constellation's own view→mount-point record
  (there is no Windows equivalent of `/proc/self/mountinfo` to read back
  from); a drive letter or empty NTFS directory (settled decision 13),
  recorded the same way the Linux/macOS `MountTable` implementations track
  their own mounts, not queried from the OS.

**The `NamedPipe` control transport** implements plan 31 C5's `Transport`
trait as the Windows counterpart to `UnixSocket`:
`\\.\pipe\constellation-<fs-uuid>` (settled decision 13), created with an
explicit security descriptor (SDDL) whose DACL grants access only to the
mounting user's owner SID — the named-pipe equivalent of a 0700 unix-socket
directory, and the same "peer-cred-shaped" trust boundary `UnixSocket`
establishes via `SO_PEERCRED`. On connect, the pipe's client SID (from
`GetNamedPipeClientProcessId`/`ImpersonateNamedPipeClient` plus
`GetTokenInformation`) becomes the `Transport`-supplied principal; mapping
that principal to a role (`viewer`/`operator`/`admin`) is plan 33's
allowlist-config work (C5/plan 31 only defines the `Transport` → principal
seam, not the role table), so this plan's contribution stops at "the pipe
correctly identifies its caller as a Windows SID", not at authorization
policy.

**Start-at-login for the UI app** (a per-user scheduled task or a `Run`
registry key) is explicitly **plan 33's** integration point, not this
plan's — this plan's daemon lifecycle work (above) covers spawning and
detecting the headless `constellation` daemon only. If plan 33's UI needs a
Windows-specific daemon-discovery helper beyond what `NamedPipe` and
`dirs` already expose, that is plan 33's milestone to add, referencing this
plan's transport rather than duplicating it.

- **Docs:** `docs/how-to-guides/` gets "Mount on Windows": WinFsp install
  step (winget/MSI), drive-letter vs directory mount, the `--case sensitive`
  flag, what differs (link the semantic-differences table), how to detach a
  wedged mount, and the `SeCreateSymbolicLinkPrivilege`/Developer Mode note
  for symlink creation. README quick start gets an OS note; the Windows rows
  go into `docs/reference/`.
- **Gate:** a real `mount` → `status` → `umount`-equivalent lifecycle test
  against a real (process-backend) S3, run on the Windows smoke lane.

### W5 — Harness on Windows

- `s3env.rs` gains the process backend Windows needs anyway (no Docker on
  GitHub-hosted Windows runners, same constraint as arm64 macOS): `versitygw`
  (VERIFIED: `versity/versitygw` ships
  `versitygw_v1.8.0_Windows_x86_64.zip`/`_arm64.zip`) + `toxiproxy`
  (VERIFIED: `Shopify/toxiproxy` ships `toxiproxy-server-windows-amd64.exe`/
  `toxiproxy-cli-windows-amd64.exe` and a `_windows_amd64.tar.gz` archive).
  Plan 31 C6 already builds the `docker|process` `S3Backend` enum (a core
  test-architecture seam, not an OS-specific one); this milestone only adds
  the Windows binary-fetch step, not a new abstraction.
- `client.rs`/`scenarios.rs` route through `constellation-platform` (plan 31 C6's stated
  approach) for mount-point checks, process liveness, RSS, and kill‑9-style
  abort (`TerminateProcess` stands in for `fusermount3`/`kill -9`; the
  WinFsp-equivalent of "abort at the kernel" is disconnecting the mount
  volume, not a `/sys/fs/fuse` write).
- `--frontend winfsp` threaded through `harness run`.
- **Capabilities.** Windows/WinFsp's harness `Cap` set is *derived* from the
  `FrontendCaps` this plan declares in W3 (plan 31 C6's rule: `Cap` is never
  hand-maintained) — none of `ClusterLocks`, `Xattr`, `VirtualXattr`
  (control-API-only doesn't count as the in-band cap), `Fallocate`,
  `SeekHole`, `FuseAbort`; `PushInval` present (unlike macOS before plan
  34's own coherence work lands) unless W0 downgrades it. Every scenario
  needing an absent cap reports `SKIPPED (unsupported: <cap>)`.
- **Conformance kit.** The Windows lanes also run plan 31 C6's
  `vfs::conformance` kit directly against `Engine`+`View` with the Windows
  `PolicyStack` (`NamePolicy`/`IdentityMap`/`XattrPolicy` from W3) and this
  plan's `FrontendCaps`, on `windows-latest` — the same kernel-free
  correctness suite every other frontend runs, not a Windows-specific test
  framework.
- **`--results-json`/`--shard`** (plan 31 C6) reused as-is; no Windows-specific
  change needed beyond running the binary.
- Shell-suite dependencies: `tests/*.sh` are bash scripts; Windows CI runs
  them under the Git-Bash/MSYS environment GitHub-hosted `windows-latest`
  ships (same approach as running bash steps in existing Windows Actions
  workflows across the ecosystem — REPORTED, confirm in W5's gate run) or,
  if that proves unreliable, the Rust-ported smoke suite from plan 31 C6 is
  used instead and the shell suites are marked Linux/macOS-only for Windows.
  Either way, `smoke.sh`/its Rust port gains `FRONTEND=winfsp`.
- **Long-path handling.** NTFS/WinFsp paths interacting with `MAX_PATH`
  (260 chars) and the `\\?\` long-path prefix: the harness's deep-nesting
  scenarios (if any exceed 260 chars) either use the `\\?\`-prefixed form
  when calling into Win32 APIs directly, or are capability-skipped on
  Windows with a documented reason — decide in W5 once the actual scenario
  list is checked against the limit.
- `make deps-windows`: fetch pinned `versitygw`/`toxiproxy` Windows binaries
  (sha256-checked, mirroring `tests/ci/install-native-s3.sh`'s Linux/macOS
  pattern) instead of a package-manager install (no Homebrew-equivalent
  single command covers both).
- pjdfstest on Windows: **not attempted**. pjdfstest is a C/POSIX-`ioctl`-ish
  suite with no Windows port story (plan 31 already notes upstream never
  runs it on Darwin either). The compliance lane instead reuses
  **`winfsp-tests --external`** (see W6), which is WinFsp's own
  purpose-built compliance suite — the same tool the ntptfs.yml reference CI
  uses.

### W6 — CI: lanes, compliance, parity, cross-OS interop

- Workflows specified in full below.
- `tests/platform-parity.toml` gains `windows-winfsp` entries (plan 31 C6's
  `<os>-<frontend>` naming; this plan does not change the checker, only adds
  rows and lane names it already understands).
- **Compliance lane**, modeled directly on winfsp-rs's `ntptfs.yml`: mount a
  real Constellation view (process-backend S3) at a drive letter, run
  `winfsp-tests-x64 --external --resilient +* --case-insensitive-cmp` (or
  `--case-sensitive-cmp` when `--case sensitive` is set) with a **reasoned,
  two-way baseline file** `tests/winfsp-tests-baseline-windows.txt` in the
  xfstests/pjdfstest-baseline style plan 31 already uses: every excluded
  test carries a reason (e.g. "hard links: WinFsp `STATUS_NOT_SUPPORTED`,
  settled decision 10"), and a baseline entry with no matching failure fails
  the check too.
- **Interop lane**: extends plan 34's Linux→macOS→Linux artifact-relay
  interop with a Windows leg (Linux writes → Windows verifies+writes → Linux
  verifies), exercising the `Code` wire mapping and name-policy round-trip
  end to end, using the same `harness interop` subcommand plan 31 adds (no
  new tooling, one more stage).
- Removal is not needed here (the W0 probe workflow is this plan's own
  throwaway, removed at the end of W0/W1, same as plan 34's `nfs-probe.yml`
  spike workflow).

### W7 — Packaging

- `make dist-windows` builds `x86_64-pc-windows-msvc` and (if W0/W1 keep it
  in scope) `aarch64-pc-windows-msvc`, zips each with the license/notice
  files (settled decision 15).
- Optional Authenticode signing when `WINDOWS_SIGNING_*` secrets exist
  (`signtool sign`), mirroring plan 34's "sign/notarize if secrets exist,
  ad-hoc otherwise" packaging posture for macOS — unsigned Windows binaries
  just show a SmartScreen warning, which is documented rather than blocking
  the release.
- **Optional UI bundle.** If plan 33's Tauri UI has a Windows `.msi` built
  by this point, `windows-package` attaches it to the release artifacts
  alongside `constellation.exe`'s zip (settled decision 15). This plan does
  not build the `.msi` itself and is not blocked on plan 33; the headless
  zip is the gate (below), the UI bundle is a nice-to-have addition.
- `RELEASING.md` gains the Windows archive's verification step.

## CI definitions

Same pattern as plan 31: `dtolnay/rust-toolchain@stable` + the pinned
`rust-toolchain.toml`, `Swatinem/rust-cache@v2`. `windows-latest` currently
means Windows Server 2025 (VERIFIED, `/tmp/runner-images` README:
`windows-latest`/`windows-2025` label the same image;
`images/windows/Windows2025-VS2026-Readme.md` lists Chocolatey 2.7.4
pre-installed). **WinFsp is not preinstalled** on any Windows runner image
(VERIFIED: no `winfsp` hit anywhere in `Windows2025-VS2026-Readme.md` /
`Windows2022-Readme.md` / `Windows11-Arm64-Readme.md`) — every job that needs
a mount installs it via `choco install winfsp -y` first, exactly like
winfsp-rs's own CI.

**Precondition, shared with plan 31, owned by the coordinator:** GitHub
Actions billing must be restored before any of these lanes run for real.

### PR lanes (`ci.yml`), added across W1/W3/W5

```yaml
  windows:
    name: Windows build + unit tests (x86_64, aarch64 check)
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy
          targets: aarch64-pc-windows-msvc
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace
      - run: cargo check --workspace --all-targets --target aarch64-pc-windows-msvc

  windows-crosscheck:
    # Cheap Linux-side guard; plan 31 C0/C6's `make check-cross` already
    # covers the Windows GNU-target leg (plus macOS/Android/FreeBSD) in one
    # invocation — this plan does not add a second cross-check tool, only a
    # CI job name developers can filter on for the Windows-relevant part of
    # its output.
    name: Windows type-check from Linux (plan 31's check-cross)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: x86_64-pc-windows-gnu }
      - uses: mlugg/setup-zig@v2
      - uses: Swatinem/rust-cache@v2
      - run: make check-cross

  windows-smoke:
    name: Windows WinFsp mount smoke (local-dir + versitygw)
    needs: windows
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: choco install winfsp -y --no-progress
      - run: cargo build -p constellation
      - name: Smoke (local directory backend, drive-letter mount)
        shell: bash
        run: FRONTEND=winfsp bash tests/smoke.sh
      - name: Daemon lifecycle + mount-level WinFsp unit tests
        run: cargo test -p constellation -- --ignored winfsp_mount
        env:
          CONSTELLATION_TEST_WINFSP_MOUNT: "1"
      - name: Smoke against versitygw (conditional writes, doctor)
        shell: bash
        run: FRONTEND=winfsp S3_BACKEND=process bash tests/integration.sh
      - if: failure()
        uses: actions/upload-artifact@v4
        with: { name: windows-smoke-logs, path: "target/test-logs/**" }
```

### Nightly lanes (`nightly.yml`)

The harness lane sits next to plan 34's macOS one, same two-by-two
philosophy (OS/frontend vs S3 backend), reusing `linux-fuse` as the shared
reference:

| Lane | OS | Frontend | S3 backend | Purpose |
|---|---|---|---|---|
| `linux-fuse` | Linux | FUSE | floci (docker) | reference (plan 31) |
| `windows-winfsp` | Windows | WinFsp | versitygw (process) | the thing we are proving |

```yaml
  harness-windows:
    needs: lint
    strategy:
      fail-fast: false
      matrix:
        shard: [1, 2, 3]     # tune so each shard stays < 90 min
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: choco install winfsp -y --no-progress
      - run: bash tests/ci/install-native-s3.sh   # pinned versitygw + toxiproxy Windows tarballs, sha256-checked
        shell: bash
      - run: cargo build --release -p constellation -p constellation-harness -p constellation-chaos
      - shell: bash
        run: |
          target/release/harness.exe run --s3-backend process --frontend winfsp \
            --shard ${{ matrix.shard }}/3 \
            --results-json results-windows-winfsp-${{ matrix.shard }}.json \
            2>&1 | tee harness-windows-${{ matrix.shard }}.log
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: harness-windows-${{ matrix.shard }}
          path: "*-windows-*"

  windows-arm64:
    needs: lint
    runs-on: windows-11-arm
    continue-on-error: true   # ARM runner availability/mount support unconfirmed until W0
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: aarch64-pc-windows-msvc }
      - uses: Swatinem/rust-cache@v2
      - run: choco install winfsp -y --no-progress
      - run: cargo build --target aarch64-pc-windows-msvc -p constellation
      - run: cargo test --target aarch64-pc-windows-msvc --workspace

  compliance-winfsp:
    needs: lint
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: choco install winfsp -y --no-progress
      - run: cargo build -p constellation
      - name: Mount and run winfsp-tests --external against it
        shell: bash
        run: bash tests/compliance-winfsp.sh 2>&1 | tee compliance-winfsp.log
      - run: python3 tests/parity_baseline_check.py tests/winfsp-tests-baseline-windows.txt compliance-winfsp.log
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: compliance-winfsp, path: "compliance-winfsp.*" }

  interop-windows:
    needs: interop   # the Linux leg from plan 34's interop job (macOS port; reused here, not redefined)
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/download-artifact@v4
        with: { name: interop-linux-1 }
      - run: choco install winfsp -y --no-progress
      - run: bash tests/ci/install-native-s3.sh
        shell: bash
      - run: cargo build --release -p constellation -p constellation-harness
      - shell: bash
        run: target/release/harness.exe interop verify-and-write --in bucket-linux-1.tar.zst --stage windows-1 --out bucket-windows-1.tar.zst
      - uses: actions/upload-artifact@v4
        with: { name: interop-windows-1, path: bucket-windows-1.tar.zst }
  interop-linux-verify-windows:
    needs: interop-windows
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: actions/download-artifact@v4
        with: { name: interop-windows-1 }
      - run: bash tests/ci/install-native-s3.sh
      - run: cargo build --release -p constellation -p constellation-harness
      - run: target/release/harness interop verify --in bucket-windows-1.tar.zst

  windows-package:
    needs: harness-windows
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: aarch64-pc-windows-msvc }
      - run: make dist-windows
      - uses: actions/upload-artifact@v4
        with: { name: windows-package, path: target/dist/* }
```

- **Parity aggregation.** The `parity` job that runs `tests/parity.py` is
  defined once, in plan 31 C6 (and extended by plan 34's `harness-macos`/
  `compliance-nfs`). This plan does not redefine that job; it adds
  `harness-windows` and `compliance-winfsp` to that job's `needs:` list
  (in whatever order the two ports' diffs land, the union of both plans'
  additions), and its Windows `[[expect]]` entries (below) to
  `tests/platform-parity.toml`. There is exactly one `parity` job in the
  merged CI, never a second Windows-only parity report.
- `harness interop verify-and-write --stage windows-1` and
  `harness.exe` invocation follow plan 31's `harness interop` subcommand
  exactly (Windows just needs the `.exe` suffix and Git-Bash `shell: bash`
  for the surrounding steps).

## The parity contract: additions to `tests/platform-parity.toml`

```toml
[[expect]]
scenario = "*"
lanes = ["windows-winfsp"]
outcome = "skipped"
cap = "ClusterLocks"
reason = "WinFsp processes byte-range locks in the kernel driver with no user-mode lock callback; locks are node-local only"

[[expect]]
scenario = "*"
lanes = ["windows-winfsp"]
outcome = "skipped"
cap = "Xattr"
reason = "no in-band EA mapping in the first cut; xattrs are control-API only (constellation xattr)"

[[expect]]
scenario = "*"
lanes = ["windows-winfsp"]
outcome = "skipped"
cap = "Fallocate"
reason = "no sparse FSCTLs used by the WinFsp frontend in the first cut"

[[expect]]
scenario = "*"
lanes = ["windows-winfsp"]
outcome = "skipped"
cap = "SeekHole"
reason = "no sparse-file support surfaced through WinFsp in the first cut"

[[expect]]
scenario = "*"
lanes = ["windows-winfsp"]
outcome = "skipped"
cap = "FuseAbort"
reason = "scenario aborts via /sys/fs/fuse/connections; WinFsp has no equivalent knob"

[[expect]]
scenario = "hardlink_*"
lanes = ["windows-winfsp"]
outcome = "skipped"
reason = "WinFsp: hard-link creation is STATUS_NOT_SUPPORTED (doc/NTFS-Compatibility.asciidoc)"
```

`tests/parity.py` (plan 31 C6) needs no code change — these are ordinary
`[[expect]]` rows in the same schema, for a lane name (`windows-winfsp`) the
`<os>-<frontend>` convention already covers.

## Deferred, with the path recorded

- **Embedded SMB3 server.** Revisit once a mature Rust SMB3 server exists, or
  once Windows 11 24H2 / Server 2025 (with alternate SMB client ports) is a
  safe floor for driver-free installs with oplock-based push invalidation.
- **EA-based xattr mapping and named streams.** Needs its own case-folding-
  aware name-mapping design distinct from `NamePolicy`; not blocking the
  first cut, which routes xattrs through the control API only.
- **Windows service wrapper.** `mount` stays a foreground/detached CLI flow;
  a proper service (auto-start at boot, Service Control Manager integration)
  is a follow-up.
- **Dokany fallback.** Pre-agreed at W0 if the native WinFsp FFI proves
  unworkable or its licence blocks a future commercial edition;
  `dokan-rust` (MIT, last commit 2025-05-01) is the landing spot.
- **Bundling the WinFsp installer.** The FLOSS exception permits it; deferred
  because it adds an upstream-sync burden with no first-cut benefit.
- **uid/gid alignment across three OSes.** Plan 31 already defers Linux/macOS
  uid alignment; this plan's `windows_owner` mapping is opt-in per view, not
  a cluster-wide identity system. A real cross-OS identity story is a
  separate, larger effort.

## Risks

- **WinFsp licence for a future proprietary/commercial edition.** The FLOSS
  exception is generous but not unconditional (no linking/distributing with
  proprietary software). Mitigation: a commercial WinFsp licence exists if
  ever needed; Dokany (LGPL/MIT) is the documented fallback.
- **`crates/winfsp-sys` is bindings we own and must keep in sync with WinFsp
  releases.** Mitigation: W2's README pins the exact release the bindings
  were checked against; struct-layout assertions catch a silent ABI drift on
  a WinFsp upgrade at compile/test time rather than at a mount-time crash.
- **Node-local locks and share modes are a real regression versus every
  other frontend.** Not fixable without a user-mode lock callback WinFsp
  does not expose. Mitigation: it is the most prominent line in the
  semantic-differences table and the parity file, never silently skipped.
- **Case-insensitivity aliasing with Linux peers.** A Linux cluster member
  can create two names a Windows client folds together
  (`README` / `readme`); settled decision 5's ambiguous-lookup-fails rule
  keeps this from silently picking one, but it is still a real surprise for
  users. Documented in the how-to guide.
- **Defender/AV real-time scanning on a mounted drive.** Every file access
  potentially triggers a scan; no mitigation beyond documenting a
  Defender-exclusion recommendation for the mount path (parallel to macOS's
  `.DS_Store`/Spotlight advice) — not something Constellation can control.
- **CI cost.** Windows minutes also carry a private-repo multiplier
  (2x, same order as macOS's). Controls mirror plan 31: PR lanes are
  build/test/smoke only (~15-20 min); the full harness matrix and compliance
  lane run nightly, sharded; `windows-crosscheck` catches most breakage at
  Linux-minute prices.
- **`windows-11-arm` runner availability/behaviour is unconfirmed.** W0
  explicitly checks this; `windows-arm64`'s nightly job is `continue-on-error`
  until confirmed, and the ARM64 target may ship build-only for a while.
- **This plan cannot start ahead of plan 31 C1–C8.** Unlike the old
  macOS-only dependency, this plan's frontend, transport, host-services and
  test-architecture seams span nearly all of plan 31's milestones (C1, C2,
  C4, C5, C6 directly; C3 transitively). A partial or reordered plan-31
  landing (e.g. C4 without C5) blocks this plan at whichever milestone
  needs the missing piece. Mitigation: W1's gate explicitly re-verifies the
  starting state rather than assuming it, so a partial landing is caught
  immediately rather than producing silent drift.
- **Only three WinFsp callbacks can truly defer (`Read`/`Write`/
  `ReadDirectory`, verified against `winfsp.h` in W3).** Every other op
  blocks its WinFsp dispatcher thread for the duration of the underlying
  `Vfs` call, including any op that transitively waits on a lease/forward
  round trip inside the engine. Mitigation: the dispatcher thread pool is
  sized like the FUSE worker pool specifically to absorb this (W3); if W0's
  or W5's throughput numbers show head-of-line blocking under load, the
  pool size (not the completion model, which is fixed by the vtable) is
  the knob to turn.

## Definition of done

The CONVENTIONS gates, PLUS:

1. **Windows PR lanes green**: `windows`, `windows-crosscheck`,
   `windows-smoke`.
2. **Nightly matrix green**:
   - `parity` reports zero unexplained differences on `windows-winfsp`;
   - the WinFsp-tests baseline is committed with a reason per excluded entry;
   - `interop` → `interop-windows` → `interop-linux-verify-windows` passes;
   - `windows-package` produces an archive for `x86_64-pc-windows-msvc` (and
     `aarch64-pc-windows-msvc` if W0/W1 kept it mount-tested, build-only
     otherwise), with plan 33's UI `.msi` attached if it exists by W7
     (optional, not gating).
3. **Plan 31's conformance kit** (`vfs::conformance`) passes against
   `Engine`+`View` with this plan's Windows `PolicyStack` and `FrontendCaps`
   on `windows-latest` — the cross-frontend correctness bar every port
   plan shares, not a Windows-specific substitute for it.
4. **Linux reference lane unchanged**: pjdfstest 8798/8798 with an empty
   baseline, and the harness full matrix passes.
5. **Docs updated**:
   - `PROGRESS.md` has a plan-35 section with the W0 results matrix;
   - `TESTING.md` covers the `windows-winfsp` lane, `--frontend winfsp`, and
     the WinFsp-tests baseline file;
   - the README gets a Windows quick-start note;
   - the Windows how-to guide exists.
6. **Report**: per-lane pass/skip/fail tallies, the parity summary table, the
   winfsp-tests baseline tally, and the W0 matrix.

## W0 results

*(filled in by the executing model)*

## Sources checked out for this plan

| Path | Ref | Used for |
|---|---|---|
| `/tmp/winfsp` | ebd50e1 (2026-09-22) | `FSP_FILE_SYSTEM_INTERFACE` surface, volume params, `FspFileSystemNotify*`, licence + FLOSS exception, lock/share-mode kernel-only enforcement (`src/sys/file.c`), POSIX mapping helpers (`src/shared/ku/posix.c`), name-mapping convention, the `errno.i`/`fsp_fuse_ntstatus_from_errno` FUSE-compat errno table and its non-Linux numbering; re-checked for this revision (W3's completion model): `STATUS_PENDING` is documented only on `Read`/`Write`/`ReadDirectory` (`inc/winfsp/winfsp.h:437-438,467-468,696-697`), `FspFileSystemSendResponse` (`:1219-1241`) and `FspFileSystemGetOperationContext` (`:1318`) |
| `/tmp/winfsp-rs` | 5342e76 | GPL-3.0 crate licence (why we don't depend on it), `ntptfs.yml` CI shape as the compliance-lane model |
| `/tmp/dokany` | c7a59fc | driver/library LGPL, samples MIT, fallback evaluation |
| `/tmp/dokan-rust` | as cloned 2026-09-28 (last upstream commit 2025-05-01) | MIT bindings, fallback landing spot |
| `/tmp/runner-images` | 7ef9dd0, refreshed 2026-09-28 | `windows-latest`/`windows-2025`/`windows-2022`/`windows-11-arm` labels, Chocolatey preinstalled, WinFsp absent from every Windows readme |
| `microsoft/winget-pkgs` (via `gh api`) | master, 2026-09-28 | `manifests/w/WinFsp/WinFsp/2.1.25156` confirms `winget install WinFsp.WinFsp` resolves |
| `winfsp/winfsp` releases (via `gh release view`) | v2.1 / `WinFsp 2025` | latest release version, `winfsp-2.1.25156.msi` + `winfsp-tests-2.1.25156.zip` assets |
| `versity/versitygw`, `Shopify/toxiproxy` releases (via `gh release view`) | v1.8.0 / 2.12.0 | confirmed Windows x86_64+arm64 (versitygw) and windows-amd64 (toxiproxy) binaries ship |
| `seaweedfs/seaweedfs` releases (via `gh release view`) | as of 2026-09-28 | Windows binaries confirmed as an unused fallback (versitygw already covers Windows) |
| `apache/arrow-rs-object-store`, `n0-computer/iroh`, `fjall-rs/fjall` workflow files (via `gh api`) | `main`, 2026-09-28 | `object_store` `windows` CI job on `windows-latest`; `iroh` `x86_64-pc-windows-msvc` matrix job; `fjall` `windows-latest` in its OS matrix |
| `docs.rs/tokio/1.53.1` (matches this tree's `Cargo.lock`) | 1.53.1 | `tokio::net::windows::named_pipe::{ServerOptions, ClientOptions}` confirmed present, `cfg(windows)` + `net` feature |
| `SnowflakePowered/winfsp-rs` Actions runs (via `gh run list`) | `ntptfs.yml`, last 5 scheduled runs 2026-08-29..2026-09-26 | all green — the native-WinFsp-on-`windows-latest` CI shape is proven in practice, not just in theory |
| Microsoft Learn: "NFS overview", `windows-commands/mount` | as of 2026-09-28 | Windows built-in NFS client is v2/v3-only, `mount` has no port option, 32 KB `rsize`/`wsize` — basis for rejecting the NFS option |
| this tree | a945b05 | `cargo check --target x86_64-pc-windows-gnu` per-crate results; `grep` census of `crates/cli/src` unix-specific call sites; workspace `license = "MPL-2.0"` |
