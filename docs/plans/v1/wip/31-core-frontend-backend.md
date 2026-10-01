# Plan 31 — Core-isation: `constellation-engine`, the `constellation-vfs` frontend contract, `constellation-platform`, the control protocol, and the test architecture that makes it all verifiable

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context: `docs/explanation/DESIGN.md`
§10 (Control Plane), and plan 30's write-path sections (`cto=strict`, layered
durability, strict locks — the semantics this plan must carry through a
frontend-neutral boundary unchanged). Code context (all VERIFIED against
commit `a945b05` unless marked REPORTED): `crates/cli/src/fusefs.rs` +
`fusefs_ops.rs` (`ConstellationFs`, the fuser adapter), `crates/cli/src/node_runtime.rs:1580-1682`
(mount wiring), `crates/cli/src/kernel_inval.rs` (`NotifySink`, the
deadlock analysis), `crates/cli/src/locks.rs` (the blocking-`setlk`
offload), `crates/cli/src/fuse_watch.rs` (the stalled-op watchdog),
`crates/api/src/{lib.rs,types.rs,web.rs}` (`Request`/`Response`,
`StatusSource`, the unix socket, the web adapter), `crates/meta/src/{mutate.rs,record.rs}`
(`MutateOutcome::Errno`, `LogRecord::Refused`), `crates/harness/src/{s3env.rs,client.rs,scenarios.rs,main.rs}`.
Depends on plan 30 being committed. Plans 33 (control-plane security + UI),
34 (macOS), 35 (Windows), 36 (Android) and 37 (Kubernetes CSI driver) all
depend on this plan; 34 and 35 were rewritten in parallel to remove the
architectural content that moved here (the errno table, the frontend-neutral
VFS layer, the `platform` crate, the control transport abstraction, the
harness process backend/caps/parity, `make check-cross`) and now reference
this plan's milestones by ID. Plan 37 is Linux-only and additionally depends
on the CSI-specific seams this plan adds to C2, C4 and C5 (§6.11, §9.8-9.10):
FUSE session handover, `EngineHost`, `EphemeralSecretStore`, and the fd-passing
transport feature.

This plan does four jobs:

1. It records the core-isation decision: one engine, one `constellation-vfs`
   contract, local persistence never forked per platform.
2. It specifies `constellation-vfs` — the trait, `OpCtx`, `Responder<T>`,
   response barriers, threading contract, events, capabilities and policies
   — in enough Rust detail that a coding model can implement it without
   re-deriving the design.
3. It specifies the control protocol: framing, handshake, every one of
   today's 37 methods mapped to a new method name, authz, and the outright
   deletion of `crates/api` (no shim — see "no backward compatibility,"
   §2).
4. It specifies the test architecture (conformance kit, `MockVfs`, harness
   process backend, parity checker) that proves C3/C4 changed nothing on
   Linux and that a future frontend can be graded against the same bar.

The research behind it was done on 2026-09-28 against this tree and against
three parallel research passes over `crates/cli`, `crates/api`,
`crates/meta` and `crates/harness`. Every "VERIFIED" claim was checked by
reading the cited file at the cited line. "REPORTED" means the plan-34/35
research this plan inherited, not independently re-checked here.

## 1. Why

`crates/cli` is 47,325 lines (VERIFIED: `find crates/cli/src -name '*.rs' |
xargs wc -l` totals exactly 47325). It is a monolith in the specific sense
that matters for what comes next: the storage engine (leases, shipper,
coop, gc, snapshots, epochs, locks, prefetch, writeback, e2e, prune, fsck —
40 modules, itemised in §5) and the kernel-facing FUSE adapter
(`fusefs.rs`, `fusefs_ops.rs`, `kernel_inval.rs`, `fuse_watch.rs`,
`node_runtime.rs`) live in the same binary crate with no module boundary
between them (VERIFIED: `crates/cli/src` has no `lib.rs` — `crates/cli/Cargo.toml`
declares only a `[[bin]]` target, so every one of the 48 `mod` declarations
in `main.rs` is private to the binary). `fuser` is an unconditional,
ungated workspace dependency (VERIFIED `crates/cli/Cargo.toml`:
`fuser.workspace = true`, no `[target.'cfg(...)'.dependencies]` section
anywhere in the file) — so today, "does the engine compile on macOS"
and "does FUSE compile on macOS" are the same question, and the answer
is no for a reason (no `/dev/fuse`) that has nothing to do with the engine.

Three concrete pressures make this the moment to split it:

- **Ports need a seam.** Plans 34 (macOS, NFSv4.1) and 35 (Windows, WinFsp)
  each need a frontend that is not FUSE, sitting on top of the same
  storage algorithms. Without a contract between them, each port would
  either fork the engine (rejected, §2) or grow its own copy of the FUSE
  adapter's policy logic (`real_ino` translation, synthetic-inode handling,
  the write gate, kernel-invalidation deadlock avoidance, the stalled-op
  watchdog) by re-deriving it from scratch, at the exact layer where a
  subtle miss corrupts data instead of failing loudly.
- **The control plane needs to be a real control plane.** Today's
  `crates/api` is a hand-rolled line-JSON protocol over a `<state_dir>/control.sock`
  with no authentication beyond "you can open the socket" (VERIFIED
  `crates/api/src/web.rs:2-3`: "The server intentionally has no
  authentication because it binds only to `127.0.0.1`" — true of the HTTP
  adapter, and the unix socket has no authz check either). Plan 33's UI
  needs roles, an audit log, streaming events and cancellable long-running
  operations; none of that fits the current `Request`/`Response` line-JSON
  enum, which was sized for a CLI talking to its own daemon.
- **Every future frontend needs to be gradeable against the same bar.**
  `crates/harness` (176 scenarios, VERIFIED via `grep -c 'name: "'
  crates/harness/src/scenarios.rs`) exists today to catch regressions on
  Linux FUSE. There is no mechanism that would catch a semantic drift
  between Linux FUSE and a future NFS or WinFsp frontend except manually
  re-running everything and eyeballing the diff. The conformance kit and
  the parity checker (§8) are that mechanism, and they need a
  frontend-neutral `Vfs` trait to drive.

## 2. Decision: one engine; local persistence is not split into per-platform backends

The alternative — a per-OS storage backend, so that (say) macOS gets its
own meta/cache/staging implementation — was considered and rejected.
Reasons, in order of how much evidence backs each:

1. **The code already runs everywhere it needs to.** fjall, `object_store`,
   `std::fs` and `memmap2` compile and run on every target OS Constellation
   cares about. The Windows cross-compile census (VERIFIED at a945b05 with
   `cargo check --target x86_64-pc-windows-gnu` and a zig-cc wrapper) already shows `meta`, `store-s3`, `net`,
   `fs-core`, `mtree`, `model` and `upload-concurrency` compiling clean for
   `x86_64-pc-windows-gnu` — the failures are in `api` (a direct
   `tokio::net::UnixListener` use), `authority` (three raw `libc::ESTALE`
   sites) and `chaos` (unix-only test helpers), none of which are the
   storage engine itself.
2. **The real cross-platform differences are small, enumerable
   capabilities, not algorithms.** Hole punch (`FALLOC_FL_PUNCH_HOLE` /
   `F_PUNCHHOLE` / `FSCTL_SET_ZERO_DATA`), preallocate, fsync strength
   (`F_FULLFSYNC`), advisory file locks, directory resolution, secret
   storage, process liveness, and power/network/background lifecycle
   (mobile) are the entire list. None of them touch how metadata is kept
   consistent, how chunks are content-addressed, or how leases are
   arbitrated.
3. **Duplicating storage code per platform would fork the
   consistency-critical write path.** Staging, `pending_upload`, fjall
   durability and cache eviction are exactly the code plan 30 spent 16
   milestones making exactly-once and session-consistent. A second
   implementation is a second place for the bugs plan 30 M0 found (§1.1 of
   that plan: at-least-once forwarded mutations, stranded phantom state)
   to recur, independently, on a platform with a thinner test harness. It
   would also double the size of the conformance/parity surface this plan
   is building (§8) for zero behavioural gain.
4. **The escape hatch already exists and is narrow.** If a platform ever
   needs a genuinely different local store (iOS's tight memory ceiling,
   say), the answer is an `EngineProfile` knob (§9) or a new
   `FsPrimitives` capability, not a forked engine.

Explicitly rejected along with per-OS backends: abstracting fjall behind a
storage trait (no platform needs it, and it adds indirection on the hot
metadata path for no current consumer), and abstracting `object_store`
(it already is the abstraction — that's what it's for).

### Settled: no backward compatibility (applies to plans 31–36)

The core-isation is a clean break, not an in-place migration. Plans 31–36 do
not keep a backward-compatible wire format, an old-protocol shim, a compat
socket path, or a data-format alias for the sake of an existing deployment. The
`constellation-control` protocol, `constellation-types::Code`'s wire
encoding, the rdev encoding, CLI flags, config/registry file formats and the
state-dir layout may all change outright between the pre-plan-31 tree and
the post-plan-31 tree. Concretely: `crates/api` (the old line-JSON protocol)
is deleted in C5, not kept alongside `constellation-control` as a shim; the
`Code` enum gets its own fixed, explicitly-numbered wire encoding instead of
reusing the Linux errno number (§7); rdev becomes a portable `(major,
minor)` pair instead of Linux `makedev` packing; and pre-plan-31 S3 buckets
and local state directories are **not** migrated — they must be recreated
against the new tree. C3/C4's "behaviour-neutral" gates (§11) are still
required, but they are about *correctness* (the write path must not
regress), not about *compatibility*: a coding model is free to rewrite code
outright for a cleaner design anywhere this plan doesn't require a
mechanical move, rather than being constrained to move-and-rename. This
rule applies uniformly to plans 33 (control-plane security/UI), 34 (macOS),
35 (Windows) and 36 (Android): none of them owe compatibility with the old
protocol, the old embedded web UI, or any pre-plan-31 on-disk/on-wire
format either.

### Options considered for the frontend/backend split itself

| Option | What it looks like | Verdict |
|---|---|---|
| **Status quo: one monolithic crate** | `crates/cli` keeps everything; each port copies FUSE-adapter policy logic by hand. | Rejected. Each port re-derives `real_ino`, the write gate, the kernel-invalidation deadlock rules (§6.3) and the stalled-op watchdog independently, at the layer where a miss corrupts data. No mechanism catches semantic drift between frontends. |
| **Per-platform engine backends** | A macOS engine, a Windows engine, sharing only wire formats. | Rejected — see above, points 1-4. |
| **Async-first rewrite of the core** | Turn `ConstellationFs`'s ops into `async fn` from the ground up, drop the `SyncHandle`/`blocking_recv` barrier pattern, and have every frontend drive an async trait directly. | Rejected. The core is **sync-with-barriers by design**, and every kernel-facing frontend delivers its callbacks on its own synchronous dispatch threads that the frontend owns, not on a runtime the engine controls: FUSE workers (VERIFIED `crates/cli/src/node_runtime.rs:1658`, one dedicated OS thread per mount running `session.run()`, itself dispatching onto further worker threads fuser owns), the WinFsp dispatcher, the NFS dispatch pool. fuser's own reply types are `Send + 'static` by design specifically so a synchronous callback can hand its reply to another thread and return (VERIFIED `fuser-0.18.0/src/reply.rs`: `pub(crate) trait Reply: Send + 'static`, and every `ReplyEntry`/`ReplyEmpty`/etc. consumes `self` by value with no thread-affinity requirement) — the ecosystem is already built around "answer this from wherever, eventually," which is exactly the `Responder<T>` model this plan adopts (§6.2), not a wholesale async rewrite. An async-first core would need every frontend adapter to bridge kernel-synchronous callbacks into async anyway, which is strictly more code than a deferral-capable sync trait, for a core whose actual concurrency (multiple FUSE workers, multiple mounts, the tokio-backed sync task) is already exploited today via `SyncHandle`'s channel-plus-`blocking_recv` pattern (VERIFIED `crates/cli/src/fusefs.rs:773-822`, `:1776-1783` and five further `blocking_recv` call sites). |
| **Chosen: single engine, sync-callable `Vfs` trait with `Responder`-based deferral** | One `constellation-engine`; frontends call synchronously and get either an inline reply or a deferred one via `Responder<T>`, matching how `locks.rs` already offloads blocking `setlk` (VERIFIED `crates/cli/src/locks.rs:240-262`: non-blocking answered inline, blocking spawns a dedicated `"lock-wait"` thread and completes the (proven-`Send`) `fuser::ReplyEmpty` from there). | **Chosen.** Generalises a pattern already proven correct in production code, requires no frontend to become async-aware, and lets deferral be adopted op-by-op (§6.2) instead of as a flag day. |

## 3. Layering

```
 frontends (kernel/OS-facing adapters)        management clients
 ┌──────────┬──────────┬──────────┬───────┐   ┌──────┬──────┬─────────┐
 │ fuse     │ nfs      │ winfsp   │ saf   │   │ CLI  │ UI   │ harness │
 │ (Linux,  │ (macOS,  │ (Windows)│(Andr.)│   └──┬───┴──┬───┴────┬────┘
 │ FreeBSD) │  Linux)  │          │       │      │ constellation-control
 └────┬─────┴────┬─────┴────┬─────┴───┬───┘      │ (protocol + client)
      │  constellation-vfs contract (Vfs, OpCtx, Responder, FrontendEvents,
      │  FrontendCaps, policies) ─────────────── ControlVfs (browse/transfer
      ▼                                            is just another frontend)
 constellation-engine  (the "backend": ONE implementation of all storage
   algorithms: Engine = node, View = mount; leases, shipper, coop, gc,
   snapshots, epochs, locks, prefetch, writeback, e2e, prune, fsck…)
      │ uses: meta(fjall) store-s3 net(iroh) authority mtree fs-core model…
      ▼
 constellation-platform (host services, cfg per OS; injected, never
   reached around): dirs, daemon/process, file locks, fs primitives,
   secrets, lifecycle/power/network, mount table
 constellation-types (Code errno enum, Ino, FileAttr, ids, caps) — no deps
```

Everything below `constellation-vfs` is the "backend" (one implementation,
per §2). Everything from `constellation-vfs` up is where OS/kernel-specific
and management-specific translation happens, and where each future port's
entire job is scoped to.

## 4. Crates (fixed names)

| Crate | Replaces / is new | Contents |
|---|---|---|
| `constellation-types` (`crates/types`) | new | `Code` errno enum (§7), `Ino`, `FileAttr`, `SetAttr`, `FileKind`, `Durability`, `LockRange`, capability structs, ids. No deps beyond serde. |
| `constellation-platform` (`crates/platform`) | new | Modules `linux`, `macos`, `windows`, `android`, `ios`, `freebsd`. Only `linux` and `macos` fully implemented here (macOS per plan 34's needs); the rest are compile-only stubs returning `io::ErrorKind::Unsupported`. Trait bundle `HostServices { dirs, process, daemon, file_lock, fs: FsPrimitives, secrets: SecretStore, lifecycle: LifecycleSource, mounts: MountTable }`. `platform::linux::fuse_mount_fd(target: &Path, opts: &MountOpts) -> io::Result<OwnedFd>` is a privileged mount helper that performs the FUSE `mount(2)` itself and returns the resulting `/dev/fuse` fd instead of handing the mount off to `fusermount3` — the one fusermount3-free path shared by the plain daemon (C4's in-place upgrade, §6.11) and plan 37's CSI node plugin, which needs the fd to hand to a separate engine process over `SCM_RIGHTS`. |
| `constellation-engine` (`crates/engine`) | absorbs 35 of `crates/cli`'s 40 engine modules outright, and the untangled remainder of the other 5 (§5) | Everything in §5's engine-module list, plus `ConstellationFs` renamed `View` (mount-scoped). `Engine::start(EngineConfig, HostServices, EngineProfile) -> Engine`; `Engine::open_view(ViewSpec, FrontendCaps, Arc<dyn FrontendEvents>) -> View`; `Engine::control() -> ControlService`. `Manager` (registry, fs create/import/export/passwd, doctor) usable in-process by the CLI and served by the daemon. |
| `constellation-vfs` (`crates/vfs`) | new | `trait Vfs`, `OpCtx`, `Caller`, `Responder<T>`, `Blocking<T>`, `CancelToken`, `DirSink`, `FrontendEvents`, `Invalidation`, `FrontendCaps`; policies `NamePolicy`, `XattrPolicy`, `IdentityMap`, `PolicyStack`; `OpWatch` (generalised `fuse_watch`); `vfs::conformance`; `vfs::mock::MockVfs`. |
| `constellation-control` (`crates/control`) | replaces `crates/api` (deleted outright, no shim — §2) | Protocol, framing, `Transport` trait; transports `UnixSocket` (peer-cred authz), `InProcess`, plus `NamedPipe` (stub here, implemented in 35) and `Remote` (defined in 33); the client library; the `web` adapter (moves unchanged; 33 hardens it). |
| `constellation-frontend-fuse` (`crates/frontend-fuse`) | extracted from `crates/cli` | Linux (FreeBSD-ready: fuser has a pure-Rust FreeBSD mount). |
| `crates/frontend-nfs`, `crates/frontend-winfsp` + `crates/winfsp-sys`, `crates/frontend-saf` + `crates/mobile` | new, built in 34/35/36 | Not built here; `constellation-vfs` is shaped so they plug in without touching it. |
| `crates/cli` | shrinks | The `constellation` binary: CLI parsing, daemon host (engine + frontends + control server), one-shot commands. Stays "one static binary" on Linux. Keeps `fusefs.rs`, `fusefs_ops.rs`, `kernel_inval.rs`, `fuse_watch.rs`, `node_runtime.rs`, `daemonize.rs`, `daemon_lock.rs`, `parallelism.rs`, `startup.rs`, `main.rs` through C3 (C4 turns the FUSE-specific pieces of these into the thin `constellation-frontend-fuse` adapter). |

### 4.1 `EngineHost`: N engines per process

Today's daemon (plan 21, VERIFIED `docs/plans/v1/done/21-fs-registry-and-daemon.md`)
already hosts one `Engine` (one node identity, one replica, one cache) with
any number of `View`s mounted from it — "one daemon per (bucket, prefix)
serves all its mounts" (plan 21 §Goal, quoting DESIGN.md §13). `EngineHost`
generalises that one level further, for two consumers that need it: dense
desktops/servers running several *different* Constellation filesystems in
one process, and plan 37's CSI engine pods, which are one process per
(filesystem, k8s node) and therefore mostly run N=1 but must not be
architecturally prevented from sharing a pod across filesystems later.

```rust
// constellation-engine
pub struct EngineHost {
    // one tokio::runtime::Runtime shared by every hosted Engine — not one
    // runtime per Engine, since that would multiply thread-pool overhead
    // per filesystem for no benefit; the per-inode ordering and admission
    // barriers (§6.3) are already per-View, not per-runtime.
    runtime: tokio::runtime::Handle,
    budget: Arc<ResourceBudget>, // memory/cache/staging, partitioned per engine
    engines: DashMap<FsId, Engine>,
}
impl EngineHost {
    pub fn start(runtime: tokio::runtime::Handle, budget: ResourceBudget) -> EngineHost;
    pub fn add_engine(&self, id: FsId, cfg: EngineConfig, host: HostServices, profile: EngineProfile) -> &Engine;
    pub fn remove_engine(&self, id: FsId) -> Option<Engine>; // used by node.leave (view first, then engine)
    pub fn engine(&self, id: FsId) -> Option<&Engine>;
}
pub struct ResourceBudget { pub memory_bytes: usize, pub cache_bytes: usize, pub staging_bytes: usize }
```

`ResourceBudget` is a host-wide ceiling that `EngineHost::add_engine`
partitions across its `Engine`s (equal share by default; an explicit
per-engine override is a `Server`-profile knob, §10.1) — this is what keeps
one noisy filesystem's cache growth from starving another's in a shared
process, the same problem plan 37's per-view QoS (§10.1) solves one layer
down for views that share one `Engine`. `crates/cli`'s daemon host becomes a
thin wrapper: it constructs one `EngineHost` and, per plan 21's existing
registry-driven bring-up, calls `add_engine` once per registered filesystem
instead of constructing one bare `Engine` directly. This is additive over
plan 21, not a rework of it: a single-filesystem daemon is `EngineHost` with
one entry, and plan 21's "one name resolves to one `Engine`'s views"
semantics is unchanged.

## 5. C3's module move list: what goes to `constellation-engine`, what stays, and what's tangled

`crates/cli/src` has exactly 50 `.rs` names at the top level plus
`coop/{exact,fresh}.rs` (VERIFIED `ls crates/cli/src/`; VERIFIED `ls
crates/cli/src/coop/`). 40 of those 50 are engine modules; the other 10 are
kernel/host-bound and stay in `crates/cli` through C3 (fusefs.rs,
fusefs_ops.rs, kernel_inval.rs, fuse_watch.rs, node_runtime.rs,
daemonize.rs, daemon_lock.rs, parallelism.rs, startup.rs, main.rs). All 48
non-`main.rs`, non-`fusefs_ops.rs` modules are declared via bare `mod X;`
in `main.rs:3-50` (VERIFIED) — none are `pub mod`, because `crates/cli` has
no `lib.rs` and is binary-only (VERIFIED: no `crates/cli/src/lib.rs`
exists; `crates/cli/Cargo.toml` has only a `[[bin]]` target). This means
C3's move is not purely mechanical file relocation: it also has to decide
`constellation-engine`'s public API surface, since nothing in the source
tree is `pub` beyond the crate today.

**`fusefs_ops.rs` is not a separate module.** It is textually spliced into
`fusefs.rs` via `include!("fusefs_ops.rs")` (VERIFIED `crates/cli/src/fusefs.rs:3545`,
with a pointer comment at `:3260`). It shares `fusefs.rs`'s scope entirely;
there is no `crate::fusefs_ops::` path. C4 treats the two files as one
unit when it rebuilds the FUSE adapter over `Vfs`.

### 35 clean engine modules (no `fuser`/`ConstellationFs`/kernel-module coupling — mechanical move)

VERIFIED via `grep` for `fuser::`, `crate::fusefs::`, `ConstellationFs`,
`crate::node_runtime::`, `crate::kernel_inval::`, `crate::fuse_watch::` across
each file, with every hit inspected for doc-comment-only mentions (which
don't block a move) vs real code:

`atime`, `backend`, `coop` (+ `coop/exact.rs`, `coop/fresh.rs` — all three
re-checked individually, zero hits), `cto` (2 hits, both `//`/`///` prose
naming `fusefs::ConstellationFs::strict_read`/`::ttl` — VERIFIED
`crates/cli/src/cto.rs:6,51` — not code), `designation`, `doctor`,
`e2e_pin`, `epoch`, `existence`, `fault`, `forward`, `fsck`, `held`,
`holds`, `inbox`, `lease` (1 hit, doc-comment prose only, `crates/cli/src/lease.rs:217`),
`leave`, `log_buffer`, `mtree_gc`, `mtree_publish`, `mtree_read`, `paths`,
`pin`, `placement`, `prefetch`, `registry`, `reintegrate`, `scan`,
`shipper`, `singleton`, `snapshot`, `sources`, `staging`, `target`,
`writeback`.

### 5 tangled engine modules — need untangling before or during the move

| Module | Lines | The tangle (VERIFIED) | What untangles it |
|---|---|---|---|
| `authority_driver.rs` | 4,291 (VERIFIED, matches the brief exactly) | `crates/cli/src/authority_driver.rs:27`: `use crate::fusefs::{AcquireProgress, HandoffResult, SyncRequest};` — real type imports. | `SyncRequest`, `AcquireProgress`, `HandoffResult` are defined **in `fusefs.rs`** (VERIFIED `:410`, `:378`, `:396`) — a `ConstellationFs`/FUSE-era design where the driver's request enum lives with the kernel adapter. Move the three types to `constellation-engine` (they describe engine-internal driver↔core traffic, not FUSE traffic) so `authority_driver` depends on them without depending on `fusefs`. |
| `gc.rs` | 1,259 | `crates/cli/src/gc.rs:44`: `Daemon(tokio::sync::mpsc::UnboundedSender<crate::fusefs::SyncRequest>)`; `:75`: `tx.send(crate::fusefs::SyncRequest::TailToHead { reply })`. | Same fix — `SyncRequest` moves to the engine crate. |
| `locks.rs` | 710 | The most heavily tangled: `crates/cli/src/locks.rs:41` (`use crate::fusefs::SyncRequest;`), `:128` (`pub inval: Option<crate::kernel_inval::InodeInvalidator>`), `:185,296,336,370` (`crate::fuse_watch::stage(...)`), and — the real split point — `:245-256`, the `lock()` method's `reply: fuser::ReplyEmpty` parameter and its private `answer()` closure that calls `reply.ok()`/`reply.error(fuser::Errno::from_i32(e))`. | This file **cannot move whole**. The arbitration/queueing logic (`ClusterLocks::set`, the grant/queue bookkeeping — the bulk of the 710 lines, all clean of `fuser`) is a mechanical move. The `lock()` method's reply plumbing (lines 240-262) is FUSE-adapter code and stays in `crates/cli` (later, `constellation-frontend-fuse`) as a thin wrapper that calls into the moved arbitration API and completes a `Responder<()>` instead of a `fuser::ReplyEmpty` directly — this is in fact the first concrete instance of C4's `Responder` pattern, so C3 can either stub it as a temporary adapter shim or land it in lockstep with the start of C4. The `crate::kernel_inval::InodeInvalidator` field and the `fuse_watch::stage` calls become `constellation-vfs`'s `FrontendEvents`/`OpWatch` calls once C4 lands; until then they stay as-is behind a thin re-export. |
| `prune.rs` | 1,044 | `crates/cli/src/prune.rs:145,731,746`: a `sync_tx: UnboundedSender<crate::fusefs::SyncRequest>` field and two sends. | Same fix as `gc.rs` — `SyncRequest` moves to the engine crate. |
| `recovery.rs` | (not separately reported) | `crates/cli/src/recovery.rs:18`: `use crate::fusefs::SyncRequest;` (its other import, `crate::forward::ForwardState`, is intra-engine and not a tangle). | Same fix. |

The single move that unblocks 4 of the 5 tangles is relocating `SyncRequest`
(VERIFIED defined `crates/cli/src/fusefs.rs:410-...`, a large enum — variants
include `Nudge`, `Roster(Vec<u64>)`, `EpochChanged`, `Publish{reply}`,
`Barrier{ino,reply}`, `DrainInode{ino,reply}`, `AcceptHandoff{...}`,
`TailToHead{reply}`, `Acquire{reply}`, `HandOff{...}`, `Lock{...}`,
`LockIdle{ino}`, `LockTest{...}`, `PeerLockMirror{...}`, `Shutdown{reply}`)
and its two payload structs `AcquireProgress` (`:378`) and `HandoffResult`
(`:396`) to `constellation-engine`. This is a channel-message enum between
the engine's own sync task and its own callers — it names no fuser type and
belongs with the engine, not the FUSE adapter; its current home in
`fusefs.rs` is a historical accident of `ConstellationFs` having been where
the sync task's handle (`SyncHandle`, §6.1) lived.

### Dependency direction, confirmed both ways

The expected direction — kernel code depending on engine code — is
pervasive and unremarkable: `crates/cli/src/fusefs.rs:17` (`use
crate::staging::{...}`), `:781,784,799,823,831,891,893` (fields typed
`crate::locks::ClusterLocks`, `crate::lease::LeaseView`,
`crate::writeback::WriteModeState`, `crate::coop::Coop`,
`crate::snapshot::SnapshotManager`, `crate::prefetch::Prefetcher`,
`crate::scan::ScanAhead`); `crates/cli/src/node_runtime.rs:230,259,646-647,737,778,849`
(fields/calls into `authority_driver`, `locks`, `gc`, `coop`, `mtree_publish`).
The reverse direction is exactly, and only, the 5-module list above — no
other engine module imports `fuser` or a kernel-bound module.

### Sizes for context

`authority_driver.rs` 4,291 lines, `coop.rs` 2,350 (+`coop/exact.rs` 802,
`coop/fresh.rs` 210), `gc.rs` 1,259, `mtree_gc.rs` 1,144, `epoch.rs` 1,065,
`prune.rs` 1,044, `prefetch.rs` 1,031, `mtree_read.rs` 1,015, `snapshot.rs`
793, `shipper.rs` 781, `sources.rs` 721, `locks.rs` 710, `fsck.rs` 686,
`staging.rs` 621, `lease.rs` 560, `registry.rs` 484 (all VERIFIED via `wc
-l`). Total `crates/cli/src` line count: **47,325** (VERIFIED, matches the
brief exactly).

## 6. The `constellation-vfs` contract

### 6.1 What today's code already establishes, that the trait must preserve

Reading `ConstellationFs` and its fuser adapter closely (both agents'
research, cross-checked) gives the concrete shape `Vfs` has to match:

- **`ConstellationFs`** (VERIFIED `crates/cli/src/fusefs.rs:879-929`, 30
  fields) is exactly what `View` (the renamed, mount-scoped engine type,
  §4) becomes: `meta: Arc<Meta>`, `store: Arc<ChunkStore>`, `cache:
  Arc<DiskCache>`, `rt: Handle`, `chunk_size: u32`, `writes: WriteShards`,
  `inode_ops: InodeOps` (per-inode write-session op ordering, see below),
  `opens: Mutex<HashMap<Ino,u32>>`, `prefetch`, `scan`, `coop:
  Option<Arc<Coop>>`, `sync: Option<SyncHandle>`, `staging_dir`,
  `staging_budget`, `staging_gen`, `snapshots`, `synthetic:
  Mutex<SyntheticRegistry>`, `tree_cache`, `view_root: Ino`, `quota_cache`,
  `usage_cache`, `statfs_ttl`, `atime`, `prune_stats`, `inflight:
  crate::kernel_inval::InFlight`, `holds`. `FuseFs(pub Arc<ConstellationFs>)`
  is a thin `Deref` newtype (VERIFIED `fusefs.rs:3535-3540`) — this is
  already, structurally, "a shared core behind a per-frontend adapter."
- **`InodeOps`** (VERIFIED `fusefs.rs:303-360`) is a 64-shard
  `Mutex<HashMap<Ino,(ThreadId,u32)>>` + `Condvar` giving per-inode
  ordering of write-session operations (read/write/truncate/fallocate/lseek
  and the flush that publishes them), re-entrant on the owning thread. This
  *is* the brief's "ordering barrier" (§6.4) already implemented; `Vfs`
  keeps it as an engine invariant, not something frontends coordinate.
- **`SyncHandle`** (VERIFIED `fusefs.rs:773-822`, full definition) is the
  FUSE-side handle to the metadata sync task: `tx:
  UnboundedSender<SyncRequest>`, plus `fsync_s3`, `cto_strict`, `locks:
  Option<Arc<ClusterLocks>>`, `lease: Arc<LeaseView>`, `delegates:
  Arc<DelegateView>`, `acquire_deadline`, `epoch_frozen`/`epoch_active`
  (`Option<Arc<AtomicBool>>`), `departed`, `read_only_member`,
  `write_mode`, plus (plan 30 §M2) `node_id`, `incarnation`,
  `next_rid_seq`, `acked`. The `SyncHandle` + `blocking_recv` barrier
  pattern (CONVENTIONS.md's own description) is the exact mechanism
  `Responder<T>`'s deferred path generalises: **every** site that today
  does
  ```rust
  let (tx, rx) = tokio::sync::oneshot::channel();
  h.tx.send(SyncRequest::Whatever { reply: tx })?;
  match rx.blocking_recv() { ... }
  ```
  (VERIFIED representative instance `fusefs.rs:1776-1783`, five further
  sites at `:1964,2030,2273,2322,2552`) is, structurally, a call that
  *could* instead hand a `Responder<T>` to the engine and return the
  calling thread immediately — C4/C7 convert them incrementally (§9, C7).
- **`Caller`** (VERIFIED `fusefs.rs:3297-3327`): `{ uid: u32, gid: u32,
  pid: u32 }` plus `in_group(&self, gid) -> bool` (reads
  `/proc/<pid>/status` for supplementary groups when `pid != 0`). The
  brief's sketch widens this to `gids: SmallVec<[u32;16]>` gathered once at
  the frontend edge, since `/proc` parsing is Linux-only and every other
  frontend (NFS's `AUTH_SYS`, WinFsp's SID mapping) hands over a full group
  list up front rather than a single `gid` plus a lazy lookup.
- **The op inventory.** `fusefs_ops.rs`'s `impl Filesystem for FuseFs`
  (VERIFIED, spliced into `fusefs.rs` at `:3545`) implements exactly:
  `init`, `lookup`, `getattr`, `setattr`, `readlink`, `mkdir`, `mknod`,
  `link`, `create`, `symlink`, `unlink`, `rmdir`, `rename`, `open`, `read`,
  `write`, `flush`, `fsync`, `release`, `readdir`, `setxattr`, `getxattr`,
  `listxattr`, `removexattr`, `statfs`, `fallocate`, `lseek`, `getlk`,
  `setlk` — 28 methods, each with a file:line (see the table below). **Not
  implemented** (confirmed absent by grep across the whole `cli` crate):
  `opendir`, `releasedir`, `fsyncdir`, `access`, `bmap`, `ioctl`,
  `copy_file_range` — these rely on fuser's ENOSYS-ish defaults, so `Vfs`
  can omit them too. The backend-side `ConstellationFs` methods these call
  into use domain names, not a blanket `do_*` convention; four do use
  `do_*` (`do_read`, `do_read_detached`, `do_write`, `do_fallocate` — all
  in the `fusefs_ops.rs`-included block, `:1488,1504,1604,1793`), plus
  `truncate`/`truncate_locked`/`seek_sparse`. xattr and lock backends are
  **not** separate `do_*` methods: xattr logic is inline in the
  `fusefs_ops.rs` handlers (calling straight into `self.meta.*xattr*`),
  and lock logic is `ClusterLocks::test`/`lock`/`unlock` in `locks.rs`.

  | fuser method | file:line | Inline policy before the backend call |
  |---|---|---|
  | `lookup` | `fusefs_ops.rs:62` | `fuse_watch::enter`, `real_ino`, `inflight.enter`, `checked_name!`, synthetic-node short-circuit, scratch-dir short-circuit, `strict_read` (plan 30 §M8), pending-write size overlay |
  | `getattr` | `fusefs_ops.rs:134` | `fuse_watch::enter`, `real_ino`, pending-write overlay |
  | `rename` | `fusefs_ops.rs:589` | `fuse_watch::enter`, `real_ino` on both parents, `inflight.enter([parent,newparent])`, `checked_name!` on both names, scratch-dir EXDEV branching |
  | `read` | `fusefs_ops.rs:764` | `fuse_watch::enter`, `real_ino`, `inflight.enter`, synthetic → `read_frozen`, `lock_fenced` check, then `do_read` |
  | `write` | `fusefs_ops.rs:796` | `fuse_watch::enter`, `real_ino`, `inflight.enter`, synthetic → `EROFS`, `lock_fenced`, `lock_discard_tainted`, `do_write`, then `O_SYNC`/`O_DSYNC` → `flush_inode` |
  | `readdir` | `fusefs_ops.rs:956` | `fuse_watch::enter`, `real_ino`, `inflight.enter`, synthetic-dir enumeration, `strict_read` at offset 0 |
  | `setlk` | `fusefs_ops.rs:1404` | `fuse_watch::enter`/`enter_blocking`, `real_ino`, `cluster_locks()` capability check (`ENOSYS` under `--locks local`), F_UNLCK short-circuit, type validation, synthetic → `ENOLCK`, delegates to `ClusterLocks::lock` (the offload point, §6.3) |

  Every handler answers via `reply.error(Errno::from_i32(e))` where `e` is
  produced by one of two file-local, centralised functions: `fn errno(e:
  &MetaError) -> i32` (VERIFIED `fusefs.rs:938-955` — full match, `NoEnt`→`ENOENT`,
  `Exists`→`EEXIST`, `NotDir`→`ENOTDIR`, `IsDir`→`EISDIR`,
  `NotEmpty`→`ENOTEMPTY`, `NoData`→`ENODATA`, `Invalid`→`EINVAL`,
  `Conflict`→`EAGAIN`, everything else→`EIO`) and `fn staging_errno(e:
  &StagingError) -> i32` (`fusefs.rs:931-936`). This centralisation is
  exactly what CONVENTIONS.md means by "map errors to errnos at the FUSE
  boundary only" and is what `Code` (§7) generalises to a portable,
  platform-independent representation.

### 6.2 The `Vfs` trait, `OpCtx`, `Responder<T>`

```rust
// constellation-vfs — the frontend contract. No frontend, and no
// constellation-engine internal, is FUSE-shaped past this file.

pub struct OpCtx<'a> {
    pub op: OpId,
    pub kind: OpKind,
    pub caller: &'a Caller,
    pub deadline: Option<Instant>,
    pub cancel: Option<&'a CancelToken>,
    pub span: &'a tracing::Span,
}

pub struct Caller {
    pub uid: u32,
    pub gids: smallvec::SmallVec<[u32; 16]>,
    pub pid: Option<u32>,
    pub principal: Principal, // Posix{uid,gid} today; Sid (35), AppSignature (36), later
}

/// Every op completes exactly once through its Responder. A fast path
/// completes it inline, on the calling thread, monomorphised per
/// frontend (no heap allocation, no dyn dispatch on the hot path). A
/// slow path — a lock grant, a lease handoff round trip, a cold S3/peer
/// fetch past the inline budget, an admission wait — moves the
/// Responder to the engine's completion pool and returns the calling
/// thread immediately. This generalises SyncHandle's
/// oneshot-channel-plus-blocking_recv pattern (fusefs.rs:773-822) from
/// "every caller blocks" to "the fast path never blocks, and only the
/// ops that already have to wait defer."
pub trait Responder<T>: Send + 'static {
    fn done(self, r: Result<T, VfsError>);
}

/// A Responder that parks the calling thread. Exists for simple
/// synchronous callers (tests, MockVfs scripts, a frontend not yet
/// converted to true deferral) — it is not how a real kernel frontend
/// answers a deferred op.
pub struct Blocking<T>(std::sync::mpsc::SyncSender<Result<T, VfsError>>);

pub trait Vfs: Send + Sync + 'static {
    fn lookup<R: Responder<Entry>>(&self, cx: &OpCtx, parent: Ino, name: &Name, r: R);
    fn getattr<R: Responder<Attr>>(&self, cx: &OpCtx, ino: Ino, fh: Option<Fh>, r: R);
    fn setattr<R: Responder<Attr>>(&self, cx: &OpCtx, ino: Ino, fh: Option<Fh>, set: &SetAttr, r: R);
    fn mknod<R: Responder<Entry>>(&self, cx: &OpCtx, parent: Ino, name: &Name, mode: u32, rdev: u32, r: R);
    fn mkdir<R: Responder<Entry>>(&self, cx: &OpCtx, parent: Ino, name: &Name, mode: u32, r: R);
    fn symlink<R: Responder<Entry>>(&self, cx: &OpCtx, parent: Ino, name: &Name, target: &[u8], r: R);
    fn link<R: Responder<Entry>>(&self, cx: &OpCtx, ino: Ino, new_parent: Ino, new_name: &Name, r: R);
    fn unlink<R: Responder<()>>(&self, cx: &OpCtx, parent: Ino, name: &Name, r: R);
    fn rmdir<R: Responder<()>>(&self, cx: &OpCtx, parent: Ino, name: &Name, r: R);
    fn rename<R: Responder<()>>(&self, cx: &OpCtx, parent: Ino, name: &Name, new_parent: Ino, new_name: &Name, flags: RenameFlags, r: R);
    fn readlink<R: Responder<Vec<u8>>>(&self, cx: &OpCtx, ino: Ino, r: R);
    fn statfs<R: Responder<StatFs>>(&self, cx: &OpCtx, ino: Ino, r: R);
    fn open<R: Responder<Opened>>(&self, cx: &OpCtx, ino: Ino, flags: OpenFlags, owner: OpenOwner, r: R);
    fn create<R: Responder<(Entry, Opened)>>(&self, cx: &OpCtx, parent: Ino, name: &Name, mode: u32, flags: OpenFlags, owner: OpenOwner, r: R);
    /// ReadData = SmallVec<[Bytes; 4]> scatter, zero-copy from cache/mmap.
    fn read<R: Responder<ReadData>>(&self, cx: &OpCtx, fh: Fh, off: u64, len: u32, r: R);
    /// data: borrowed slice for the inline path, Bytes when the write must outlive the call.
    fn write<R: Responder<u32>>(&self, cx: &OpCtx, fh: Fh, off: u64, data: WriteData<'_>, r: R);
    /// The close-time flush fence (plan 30 cto=strict), per close(2).
    fn flush<R: Responder<()>>(&self, cx: &OpCtx, fh: Fh, owner: OpenOwner, r: R);
    /// Last close of this handle.
    fn release<R: Responder<()>>(&self, cx: &OpCtx, fh: Fh, r: R);
    /// The layered durability barrier: Local | Peer | Durable(S3).
    fn fsync<R: Responder<()>>(&self, cx: &OpCtx, fh: Fh, level: Durability, r: R);
    fn readdir(&self, cx: &OpCtx, dh: Fh, cookie: u64, plus: bool, sink: &mut dyn DirSink) -> Result<(), VfsError>;
    fn fallocate<R: Responder<()>>(&self, cx: &OpCtx, fh: Fh, off: u64, len: u64, mode: FallocateMode, r: R);
    fn seek<R: Responder<u64>>(&self, cx: &OpCtx, fh: Fh, off: u64, whence: SeekWhence, r: R); // SEEK_DATA/SEEK_HOLE
    fn getxattr<R: Responder<Vec<u8>>>(&self, cx: &OpCtx, ino: Ino, name: &XattrName, r: R);
    fn setxattr<R: Responder<()>>(&self, cx: &OpCtx, ino: Ino, name: &XattrName, value: &[u8], mode: SetXattrMode, r: R);
    fn listxattr<R: Responder<Vec<XattrName>>>(&self, cx: &OpCtx, ino: Ino, r: R);
    fn removexattr<R: Responder<()>>(&self, cx: &OpCtx, ino: Ino, name: &XattrName, r: R);
    /// Only called if caps.cluster_locks; getlk/setlk(blocking)/unlock.
    fn lock_test<R: Responder<LockStatus>>(&self, cx: &OpCtx, ino: Ino, owner: LockOwner, range: LockRange, r: R);
    fn lock_acquire<R: Responder<()>>(&self, cx: &OpCtx, ino: Ino, owner: LockOwner, range: LockRange, sleep: bool, r: R);
    fn lock_release<R: Responder<()>>(&self, cx: &OpCtx, ino: Ino, owner: LockOwner, range: LockRange, r: R);
    /// Whole-view barrier: unmount, suspend, snapshot.
    fn sync_view<R: Responder<()>>(&self, cx: &OpCtx, r: R);
}
```

Notes tying this back to today's code:

- `RenameFlags` on `rename` covers `RENAME_NOREPLACE`/`RENAME_EXCHANGE`,
  which `fusefs_ops.rs:589`'s handler already parses from fuser's flags —
  the trait just makes the parsed form the boundary instead of a raw `u32`.
- `lookup`/`getattr`/`readdir` keep their inline "pending-write size
  overlay" and "synthetic node short-circuit" behaviour as `View`-internal
  logic beneath the trait, not as frontend policy — a WinFsp or NFS
  frontend must see the same pending-write-aware size a FUSE frontend
  does, so this cannot become per-frontend code.
- `fallocate`/`seek` return `Code::NotSupported` (via the `Responder`) on
  frontends whose `FrontendCaps` don't declare them (NFSv4.1 has no
  ALLOCATE/DEALLOCATE, WinFsp's fallocate story is a plan-35 question) —
  the trait method still exists so the conformance kit can assert the
  *absence* is deliberate and capability-gated, not silently missing.

### 6.3 Response barriers

1. **Completion barrier.** Every op completes exactly once through its
   `Responder`. Fast paths complete inline (no heap allocation; the
   responder is monomorphised per frontend via the generic `R:
   Responder<T>` parameter, so a FUSE adapter's inline responder can be a
   zero-sized wrapper around the fuser `Reply*` type it holds). Ops that
   wait on unbounded events *must* defer: lock grants, lease
   handoff/forwarding round trips, cold S3/peer fetches past the inline
   budget, admission waits. Deferring moves the `Responder` into the
   engine's completion pool and frees the frontend thread. FUSE (proven —
   §6.4 below) and NFS (async by construction) support deferred replies for
   any op. WinFsp: Read/Write/ReadDirectory only (see plan 35) —
   `STATUS_PENDING` + `FspFileSystemSendResponse` are documented only for
   those three callbacks (`winfsp.h:437-441,467-471,696-703`); `Cleanup`/
   `Close` are declared `VOID`, not `NTSTATUS`, and cannot defer at all.
   `FrontendCaps.deferrable` is therefore a per-op set, not a boolean: the
   engine only moves a `Responder` to its completion pool for ops in that
   set, and for every other op it completes on the calling thread (a
   bounded wait parks the frontend thread, exactly as today).
   Initially only today's lock offload defers (`locks.rs`'s `lock-wait`
   thread, generalised — see below); conversion of the other
   `blocking_recv` sites (§6.1) is incremental, behind the same trait, no
   frontend changes required per conversion.
2. **Ordering barrier.** Per-inode op ordering (`InodeOps`, §6.1) is an
   engine invariant, unconditionally enforced beneath `Vfs`. Frontends may
   issue concurrently; the engine guarantees the POSIX-visible order for
   the same handle/inode regardless of frontend concurrency model.
3. **Visibility/durability barriers**, each an explicit op on the trait:
   `flush` is the close-to-open publish fence (plan 30 `cto=strict`, today
   `strict_read`/the flush path in `fusefs_ops.rs:833`); `fsync(level)` is
   the layered durability barrier (`Local` = fjall + staging synced,
   `Peer` = replicated to a peer, `Durable` = S3 CAS committed — matching
   `SyncHandle.fsync_s3` today); `sync_view` is a whole-view barrier used
   by unmount, suspend and snapshot.
4. **Cancellation barrier.** A `CancelToken` is set by FUSE INTERRUPT (not
   available in fuser 0.18 — see the gap below), WinFsp cancel, NFS
   disconnect, or a control-client `Cancel` frame (§10). The engine checks
   it at every wait point and completes with `Code::Intr`. Cancellation
   never leaves partial metadata: writes are either applied or not — this
   is not new work, it is a restatement of what plan 30's `mutate_op_rebasable`
   already guarantees for the rebase path.

   **Known gap, carried forward explicitly rather than silently dropped:**
   fuser 0.18 delivers no `FUSE_INTERRUPT` (VERIFIED, doc comment
   `crates/cli/src/locks.rs:35` and `:238`: "fuser 0.18 has no
   `FUSE_INTERRUPT`: such a wait cannot be cancelled from the application").
   A Linux FUSE-frontend `CancelToken` is therefore **never set by the
   kernel** today; it exists in the trait for NFS/WinFsp/control-client
   cancellation and for tests. This is not a regression C4 introduces — it
   documents an existing Linux FUSE limitation in the frontend-neutral
   layer instead of leaving it implicit in `locks.rs`'s comments.
5. **Backpressure barrier.** Admission control per view: in-flight ops,
   staging bytes, forward queue depth. Ops over the limit defer, never
   spin; past the deadline they complete with `Code::Again` or
   `Code::TimedOut` per the frontend's `FrontendCaps`.

**Deferred-reply proof (why this is safe, not aspirational).** fuser's own
reply types establish the pattern this plan generalises. VERIFIED
`fuser-0.18.0/src/reply.rs`: `pub(crate) trait Reply: Send + 'static`, and
every concrete reply (`ReplyEntry`, `ReplyEmpty`, `ReplyAttr`, `ReplyData`,
`ReplyOpen`, `ReplyWrite`, …) implements it, wrapping a `ReplyRaw { unique:
RequestId, sender: Option<ReplySender> }` whose `ReplySender::Channel`
variant holds a `ChannelSender(Arc<DevFuse>)` (`DevFuse(File)` — a
`std::fs::File`, auto-`Send`+`Sync`, no `unsafe impl` needed or present).
Every completing method (`ReplyEmpty::ok(self)`, `::error(self, err)`,
`ReplyEntry::entry(self, ...)`, etc.) consumes `self` by value with no
thread-affinity requirement. `ReplyRaw` also has a `Drop` impl that answers
`EIO` with a warning if a reply was constructed but never explicitly
completed — replies are exactly-once even across a panic or early return,
and `Responder<T>` should mirror that fail-safe (a dropped, uncompleted
`Responder` completes its frontend-side reply with an error, never leaves
it silently pending).

`locks.rs::ClusterLocks::lock` (VERIFIED `crates/cli/src/locks.rs:240-262`,
full body captured above) is this pattern already in production: a
non-blocking `setlk` answers inline on the FUSE worker; a blocking one
spawns a dedicated `"lock-wait"` OS thread — explicitly *not* the tokio
blocking pool, because that pool is small (4 threads on one CPU) and the
release a waiter waits for may itself need the pool via `Action::LockFlush`,
so a pool-filling waiter could deadlock the whole node (comment,
`locks.rs:231-238`) — and completes the moved `fuser::ReplyEmpty` from that
thread. `Responder<T>`'s deferred path is exactly this, generalised past
locks to every op the barrier list above names, and past `fuser::ReplyEmpty`
to whatever completion type each frontend's adapter wraps.

### 6.4 Threading contract

- Ops may be called from any thread that is **not** a tokio runtime worker
  (debug-asserted in `OpCtx::new`), matching CONVENTIONS.md's existing rule
  ("never block the tokio runtime with sync waits").
- Frontends own their dispatch threads: FUSE workers (today, one dedicated
  OS thread per mount running `session.run()` — VERIFIED
  `crates/cli/src/node_runtime.rs:1658-1682` — itself dispatching to
  further fuser-owned worker threads per `fuse_config.n_threads`), the
  WinFsp dispatcher, the NFS dispatch pool, Android binder threads.
- The engine owns the tokio runtime and its completion pool.
- `Blocking<T>` exists for simple callers (tests, `MockVfs` scripts, a
  not-yet-deferred site).

### 6.5 Events: `FrontendEvents`, and the deadlock lesson from `kernel_inval.rs`

`kernel_inval.rs`'s module doc (VERIFIED `crates/cli/src/kernel_inval.rs:1-76`)
is, in full, the design note this section generalises. The load-bearing
part, quoted because it is the exact hazard `FrontendEvents` must not
reintroduce:

> "A notification is a `write(2)` on `/dev/fuse` that the kernel serves
> synchronously: `FUSE_NOTIFY_INVAL_ENTRY` takes the parent directory's
> `i_rwsem`... The VFS holds that very `i_rwsem` while a `lookup`,
> `create`, `mkdir`, `unlink`, `rename`, `readdir` or a directory `setattr`
> is *in flight*... So a notification for a directory with a request in
> flight blocks until that request is answered:
> - **Never answered from a thread that waits on the notification.** ...
>   Notifications are sent from one dedicated thread, fed through a queue
>   nothing blocks on...
> - **Not issued while a request on the same inode is known to be in
>   flight** (`InFlight`, maintained by the FUSE handlers): the
>   notification is held back and sent as soon as the last such request is
>   answered...
> - **Dropped once older than the TTL.**"

And, further down (`kernel_inval.rs:60-68`, campaign 6 finding B-1): a
daemon `kill -9`'d while its invalidation thread was blocked inside such a
kernel write became a zombie forever, because the kernel only releases a
dead daemon's requests when its last `/dev/fuse` fd closes, which needs
every thread to exit — a cycle only `abort_stale_mounts` breaks. This is
not a hypothetical; it happened.

`FrontendEvents` therefore has the same three rules baked in as
non-negotiable, frontend-agnostic requirements:

```rust
pub trait FrontendEvents: Send + Sync + 'static {
    /// Delivered from one dedicated notifier thread per view, coalesced
    /// and bounded. Never called while the engine holds an engine lock —
    /// that is the kernel_inval.rs lesson generalised: a frontend's
    /// delivery mechanism (a kernel write, a WinFsp notify call, an
    /// Android ContentResolver call) may itself block on kernel state
    /// that an in-flight op holds, so nothing that could be waiting on
    /// this call may ever be waited on by it.
    fn invalidate(&self, batch: &[Invalidation]);
}

pub enum Invalidation {
    Entry { parent: Ino, name: Name },
    Attr { ino: Ino },
    Data { ino: Ino, range: Option<(u64, u64)> },
    Deleted { parent: Ino, name: Name, ino: Ino },
    Fenced { ino: Ino },
    ViewClosing,
}
```

Delivery is gated on an `InFlight`-equivalent registry (the direct
generalisation of `crates/cli/src/kernel_inval.rs:374-409`'s `InFlight`/
`InFlightGuard`, which today tracks exactly the ops that hold a
kernel-visible lock: namespace ops and read/write/setattr/fallocate — see
the doc at `:381-386`), fed by one dedicated thread per view (today two:
`"kernel-inval"` running the sender loop and `"kernel-inval-watchdog"`
logging a notification stuck past `CONSTELLATION_KERNEL_INVAL_STALL_S`,
VERIFIED `kernel_inval.rs:249-268`), with a TTL-based drop for
notifications too old to matter (`DEFER_MAX = fusefs::TTL + 500ms`,
VERIFIED `kernel_inval.rs:88-90`). Implementations:

- **FUSE**: `impl FrontendEvents for FuseNotifySink` wraps `fuser::Notifier`
  exactly as `impl NotifySink for fuser::Notifier` does today (VERIFIED
  `kernel_inval.rs:141-149`) — a 1:1 rename, not a redesign.
- **NFS** (34): change attributes plus delegation recall.
- **WinFsp** (35): `FspFileSystemNotify`.
- **SAF** (36): `ContentResolver.notifyChange`.
- **ControlVfs**: UI subscription events (`stats.subscribe`/`events.subscribe`, §10).

### 6.6 `FrontendCaps`

Declared once per frontend; the single source of truth for both the
engine's behaviour and test capability gating (the harness `Cap` enum, §8,
is *derived* from it, never hand-maintained separately):

```rust
pub struct FrontendCaps {
    pub push_inval: PushInval,           // None | Attr | Full
    pub per_close_flush: bool,
    pub cluster_locks: bool,
    pub xattrs: XattrSupport,            // None | Native | Named
    pub virtual_xattrs_listed: bool,
    pub hard_links: bool,
    pub fallocate: bool,
    pub seek_hole: bool,
    pub special_files: bool,             // mknod FIFO/char/block/socket
    pub case: CasePolicy,                // Sensitive | InsensitivePreserving
    pub max_io: u32,
    pub deferrable: OpKindSet,           // ops whose Responder may complete on another thread
                                          // (FUSE, NFS: all; WinFsp: Read|Write|ReadDir; SAF: see 36)
    pub open_unlinked: OpenUnlinked,     // Keep | SillyRename | DeleteOnClose
    pub abortable: bool,                 // a kernel/driver-level forced-unmount hook exists
                                          // (Linux FUSE: /sys/fs/fuse/connections abort; the
                                          // harness derives Cap::FuseAbort from this field)
    pub passthrough: bool,               // the frontend reads a handle's data straight from a
                                          // backing file the engine hands it (Opened::backing,
                                          // plan 38 §3(c)); false everywhere until plan 38 Z3b
                                          // wires Linux FUSE's FOPEN_PASSTHROUGH reply, and the
                                          // engine offers none to a frontend that would drop it
}
```

Linux FUSE's instance is derived from what `fusefs_ops.rs` demonstrably
supports today: `push_inval: Full` (kernel_inval.rs), `cluster_locks: true`
(gated behind `--locks cluster`, `fusefs_ops.rs:1371,1404`'s
`cluster_locks()` check), `xattrs: Native`, `hard_links: true`,
`fallocate: true`, `seek_hole: true`, `open_unlinked: Keep` (FUSE keeps the
inode open past unlink — no silly-rename observed in this codebase),
`abortable: true` (the kernel exposes `/sys/fs/fuse/connections/<id>/abort`,
which the harness's kill-9/abort scenarios use).

### 6.7 Policies

- **`NamePolicy`**: bytes ↔ frontend names, reserved-char mapping,
  case-insensitive lookup with exact-match priority.
- **`XattrPolicy`**: namespace mapping (e.g. plan 34's macOS `user.X` ↔ `X`)
  and hiding virtual xattrs from listing — Linux FUSE's own virtual-xattr
  hiding (`RSIZE_XATTR`/`RCOUNT_XATTR` = `user.constellation.{rsize,rcount}`,
  VERIFIED `fusefs.rs:975-980`) becomes the reference `XattrPolicy` instance
  instead of ad hoc `virtual_xattr(name)` checks scattered per handler.
- **`IdentityMap`**: frontend principal ↔ POSIX uid/gids. POSIX
  uid/gid/mode stays canonical; frontends map at their edge.
- Applied by `PolicyStack` in `constellation-vfs`, so frontends never
  reimplement them; each stack is conformance-tested (§8).

### 6.8 `OpWatch`: the generalised `fuse_watch`

`fuse_watch.rs` (VERIFIED, full read) is a per-op stalled-request watchdog
motivated by a real incident (module doc: "campaign 7 saw a `getattr`...
and a file's `read` stay unanswered for the rest of the daemon's life"). It
registers every request for its whole duration (`enter`/`enter_blocking`),
tracks a mutable `stage` string the handler updates as it's about to wait
on something, and a monitor thread WARNs on anything older than
`CONSTELLATION_FUSE_REQUEST_STALL_S` (default 30s). Crucially it already
supports handing a request to another thread: `Watched::adopt()`
(`fuse_watch.rs:169-175`) is exactly what `locks.rs`'s `"lock-wait"` thread
calls so the watchdog follows the reply across the thread hop.

```rust
pub struct OpWatch { /* per-view registry, generalising fuse_watch::Registry */ }
impl OpWatch {
    pub fn enter(&self, op: &'static str, key: WatchKey) -> Watched;
    pub fn enter_blocking(&self, op: &'static str, key: WatchKey) -> Watched; // listed, never "stalled"
}
impl Watched {
    pub fn stage(&self, stage: &'static str);
    pub fn adopt(&self); // hand this request to another thread (the deferred-reply case)
}
```

`WatchKey` generalises `fuse_watch`'s `Ino` key to whatever `OpCtx`
addresses (an inode, a lock, a view-wide barrier). Backtrace capture on
stall (`CONSTELLATION_FUSE_STALL_BACKTRACE=1`, via `platform::process`)
carries over unchanged in mechanism, moved to `constellation-platform`
since it's a host service (thread-signal delivery), not FUSE-specific.

### 6.9 Performance targets (C0 baselines them; C4/C7 gate them)

- VFS dispatch overhead ≤ 1 µs/op beyond the work itself (criterion
  `vfs-bench`, in-process, no kernel).
- No per-op heap allocation for inline replies.
- Zero-copy reads (`Bytes` from cache/mmap).
- Harness `bench` and `fio-latency` p50/p99 within 3% of C0.
- pjdfstest stays 8798/8798.

### 6.10 Observability

- Every op gets an `OpId` and a tracing span, frontend → engine → S3/P2P.
- Metrics: `constellation_vfs_ops_total{frontend,op,outcome}` and
  `constellation_vfs_op_seconds{frontend,op}` histograms.
- `OpWatch`'s stalled-op registry is exposed by control `node.ops` and
  `stats.subscribe` (§10); `/metrics` keeps working.

### 6.11 FUSE session handover: `MountSource`, `SessionHandoff`, in-place upgrade

Every mount today comes from one call path: `Session::new` opens
`/dev/fuse` itself and mounts it. That is fine for a CLI-launched daemon but
wrong for two consumers this plan must not block: an in-place upgrade of a
plain-Linux daemon (replace the process, keep the mount alive under it), and
plan 37's CSI node plugin, which performs the privileged `mount(2)` itself
(`platform::linux::fuse_mount_fd`, §4.1) and hands the resulting fd to a
separate, unprivileged engine pod. Both need the same primitive: a FUSE
session that can be **handed from one process to another without the
kernel ever seeing an unmount**.

- **`MountSource`** — `constellation-frontend-fuse` stops assuming it always
  mounts:
  ```rust
  pub enum MountSource {
      Path(PathBuf, MountOpts),     // today's behaviour: mount(2) ourselves
      PreopenedFd(OwnedFd),         // fd already mounted elsewhere (fuse_mount_fd,
                                     // or handed over from a prior process)
  }
  ```
- **`FuseSession::detach()`/`resume()`** — the handover pair:
  ```rust
  pub struct SessionHandoff {
      pub fuse_fd: OwnedFd,
      pub handles: HandleTableSnapshot, // open file handles, their flags/owners
      pub view: ViewSpec,
      pub init: NegotiatedInit,         // the FUSE_INIT params the kernel already agreed to
  }
  impl FuseSession {
      /// Stops reading /dev/fuse, drains in-flight ops (sync_view-equivalent,
      /// §6.3), and returns everything the next process needs. The kernel
      /// queues requests for this fd for the whole gap — it never sees an
      /// unmount, so there is no ENOTCONN window for a caller mid-syscall.
      pub fn detach(self) -> SessionHandoff;
      /// Resumes serving fuse_fd with the given negotiated init params and
      /// handle table, WITHOUT re-running the FUSE_INIT handshake.
      pub fn resume(handoff: SessionHandoff, fs: impl Vfs) -> io::Result<FuseSession>;
  }
  ```
  `View::export_handles() -> HandleTableSnapshot` and `Engine::open_view_resumed(spec:
  ViewSpec, handles: HandleTableSnapshot, caps, events) -> View` are the
  engine-side counterparts: inode numbers are already stable meta inodes
  (nothing to reconcile there), so the only state that does not already live
  in `Meta`/`fjall` and needs to cross the handover explicitly is the
  open-file-handle table (`fh` → inode/flags/owner).
- **Risk, verified against the vendored source, not assumed:**
  `fuser` 0.18's `Session::new` and `Session::from_fd` (VERIFIED
  `fuser-0.18.0/src/session.rs:169-215`) both end by calling
  `self.handshake()` unconditionally. `handshake()` (VERIFIED `:283-360`)
  loops reading `/dev/fuse`, requires the **first** thing it reads to be a
  `ll::Operation::Init`, and returns
  `io::Error::new(InvalidData, "Received non-init FUSE operation during
  handshake")` — after replying `EIO` to the offending request — if it is
  anything else (`:333-345`). A handed-over fd has already completed
  `FUSE_INIT` once, in the *previous* process; the kernel does not resend
  it, so whatever request is next in the queue when the new process calls
  `Session::from_fd` is not `Init`, and `handshake()` fails closed exactly
  as designed for the case it was written for (a genuinely new mount) —
  which is precisely wrong for a resumed one. There is no existing
  "skip INIT, I already have `NegotiatedInit`" entry point (VERIFIED: `Session`'s
  only two constructors are `new` and `from_fd`, both above; no `cfg`, flag,
  or third constructor bypasses `handshake()`).
  **Conclusion: this needs a small vendored `fuser` patch**, in the
  `vendor/fjall` style already established in this workspace (VERIFIED
  `vendor/fjall/CONSTELLATION-PATCH.md`: a `[patch.crates-io]` path
  dependency, excluded from the workspace `exclude` list, with the exact
  upstream version, checksum and a hunk-level changelog). The shape of the
  fix is narrow: a `Session::from_fd_resumed(fs, fd, acl, config,
  proto_version: Version)` constructor that sets `self.proto_version =
  Some(proto_version)` directly (the field `handshake()` would otherwise
  populate) and calls `run()` without ever calling `handshake()`. C4 scopes
  and lands this patch (`vendor/fuser/`); plan 37's K0 proves it end-to-end
  against a real kernel mount (session handover under a live writer, zero
  `ENOTCONN`).
- **Vendoring and maintenance model: adopt Mountpoint's, not a one-off
  patch file.** AWS carries its own fuser fork the same way, but as a
  fully maintained copy rather than a single small patch: a separate crate
  (`mountpoint-s3-fuser`, own `Cargo.toml`, VERIFIED) tracked on a
  `fuser/fork` branch, kept in sync with upstream fuser releases by a
  `vendor-fuser.sh` script that re-applies the fork's patch set onto each
  new upstream commit and opens a PR for review (VERIFIED, Mountpoint's
  own README/`Cargo.toml` `homepage`). `vendor/fuser` follows the same
  shape as `vendor/fjall` already does in this workspace (`CONSTELLATION-PATCH.md`,
  a `[patch.crates-io]` path dependency, VERIFIED `vendor/fjall/CONSTELLATION-PATCH.md`),
  plus a rebase script modelled directly on Mountpoint's:
  `tools/vendor-fuser.sh` re-applies Constellation's patch set (currently
  just `from_fd_resumed`) onto a new upstream `fuser` tag and fails loudly
  on a conflict rather than silently dropping a hunk; `vendor/fuser/CONSTELLATION-PATCH.md`
  lists each patch by name, the upstream version it applies to, and why it
  exists, exactly like `vendor/fjall`'s changelog does today.
- **Checked against Mountpoint's fork, and found to need independent
  design — this was verified, not assumed.** Mountpoint's own fuser fork
  moved `INIT` handling out of a dedicated blocking pre-loop and into the
  ordinary per-request dispatch loop: `Operation::Init` is just one more
  match arm, and any other op arriving before it sees `initialized ==
  false` gets `EIO` (VERIFIED `mountpoint-s3-fuser/src/request.rs`,
  `session.rs`). This simplifies multi-worker dispatch (§C7's worker-pool
  candidate doesn't need a separate INIT phase), but it does **not** add a
  "skip INIT, resume with an already-negotiated handshake" entry point:
  `Session::from_fd` still sets `initialized = false` (VERIFIED
  `mountpoint-s3-fuser/src/session.rs:127-140`), so handing it an
  already-initialized kernel connection — exactly plan 37's handover
  scenario — would hit the same `!initialized` arm and fail with `EIO`.
  Mountpoint never needed this: every `from_fd` call in its whole CSI
  driver follows a brand-new `mount(2)` with a real `INIT` pending
  (`mountpoint-s3-csi-driver` commit `b450b22beae8bddc0d3c09551655b2b7d323e29b`,
  `pkg/driver/node/mounter/pod_mounter.go`). **`from_fd_resumed` therefore
  has no upstream precedent to adapt — it is the novel part of this
  plan's fuser patch, not a port of Mountpoint's fix.** It is still worth
  structuring the same way Mountpoint structures normal dispatch, since
  the two ideas compose: fold `INIT` into the ordinary dispatch loop (an
  `initialized: AtomicBool` the dispatch loop checks, as Mountpoint does)
  and have `from_fd_resumed` simply pre-set that flag to `true` from the
  supplied `NegotiatedInit` before entering the same loop, instead of
  `from_fd_resumed` being a parallel code path that skips dispatch
  altogether. C4 decides between the two shapes when it lands the patch;
  either way the harness scenario in this section is what proves it works,
  not the shape of the constructor.
- **Plain-Linux benefit, not just a CSI one.** The same primitive gives
  `constellation daemon --upgrade`: replace the running daemon binary with
  a new version while a mount stays live, by detaching the session, execing
  (or launching) the new binary with the inherited fd, and resuming — no
  `umount`/`mount` cycle, no window where the mountpoint returns
  `ENOTCONN` to a process with an open fd on it. This is exercisable and
  gated on plain Linux, independent of plan 37 ever landing.
- **Harness/test implications (a C4 gate item, not deferred to plan 37):**
  a new harness scenario category exercises session handover on Linux:
  detach/resume with no in-flight ops (baseline correctness), and an
  "upgrade under load" scenario that starts a sustained writer, triggers
  `constellation daemon --upgrade` mid-write, and asserts zero `ENOTCONN`/
  `EIO` observed by the writer and that every write either lands or is
  cleanly retried — this is the concrete, automatable proof that the
  `vendor/fuser` patch actually closes the gap, not just that it compiles.

### 6.12 Subtree confinement: a `View` guarantee

A `View` is rooted at a subtree (`ViewSpec`'s subtree root, §9.10), and
every frontend today relies on the convention that nothing above that
root is ever reachable — but until now that has been convention, not a
stated contract with a test behind it. This plan promotes it to an
explicit `Vfs` guarantee, alongside the policies in §6.7:

- **`..` at the view root resolves to the view root itself**, exactly like
  a real filesystem root, never to whatever the subtree's parent directory
  is in the underlying `Meta` tree.
- **No inode outside the subtree is reachable by lookup**: name
  resolution, symlink-target resolution (the kernel resolves targets
  itself, staying inside the mount by construction), and handle-based
  access (a `Handle`/`fh` can never be coerced into addressing an inode
  the view's root doesn't dominate, even via a stale or replayed handle)
  are all bounded to the subtree.
- **Snapshot synthetic paths stay confined too**: `.constellation/snapshot/<name>/...`
  (VERIFIED `docs/plans/v1/done/09-p6a-snapshots-clones.md` Step 3 — a
  lookup-only synthetic tree mirroring frozen content) mirrors history at
  the same relative path under the *view's own root*, not the
  filesystem's root — a view rooted at `/volumes/pvc-1` sees only
  `pvc-1`'s own history through `.constellation`, never a sibling
  volume's, even though both live in the same underlying filesystem.
- **Cross-view-root hard links are the one place confinement can be
  broken by an ordinary POSIX call, not just by resolution.** `link()`
  normally creates a second name for an existing inode anywhere in the
  same filesystem; if the second name falls outside the view's subtree
  (or, under plan 37's pool layout, in a different volume's subtree of
  the same underlying filesystem — §9.10's `labels`), that is exactly the
  confinement break the view boundary exists to prevent. `ViewSpec` gains
  a `confine_links: bool` option (default `false`, matching today's
  behaviour for a plain mount): when set, `link()` across the configured
  boundary returns `EXDEV` — the same errno the scratch-dir rename path
  already returns for a different boundary crossing today (VERIFIED
  `crates/cli/src/fusefs_ops.rs:633,649`), so existing callers (`cp`,
  `ln`, tar) degrade the way they already know how to handle, rather than
  hitting a new, surprising error. Plan 37 sets `confine_links: true` on
  every `/volumes/<pv>` view in a `layout: pool` StorageClass, so one
  tenant's PV can never be hard-linked into another's.
- **What confinement does not undo.** A hard link created *before*
  confinement was configured (or from outside the view, e.g. a pool's own
  maintenance path mounted with `confine_links: false`) leaves both names
  pointing at the same inode, and that inode's content and metadata
  remain visible — and mutable — through either name. Confinement governs
  `link()` calls made at the boundary; it is not a retroactive guarantee
  about pre-existing hard links, and that limit is documented rather than
  silently assumed away. Plan 37's pool layout avoids the case entirely by
  making `/volumes/*` hard-link-disjoint via `confine_links` from the
  first `mkdir`, so a fresh pool never accumulates a cross-volume link in
  the first place.

**Conformance kit and harness (C4/C6 gate).** `vfs::conformance` (§8)
gains a `confinement` test group: `..` at the view root, lookup/open/
readlink attempts at and past the root boundary, `.constellation`
synthetic-path confinement, and `link()` both with and without
`confine_links` (asserting `EXDEV` only when set). The harness gains a
matching scenario exercising the same cases against a real kernel mount,
confirming the kernel's own path resolution — not just the in-process
`Vfs` call — respects the boundary. C4's gate (§11) adds this scenario
alongside the session-handover ones; C6's gate adds the `confinement`
conformance group to the set the parity checker (§8) compares across
frontends.

## 7. `constellation-types::Code`: the portable errno

Today, raw `libc::E*` constants are used directly in application logic in
22 files across the workspace (VERIFIED: `grep -rn 'libc::E[A-Z]' crates/
--include=*.rs` → 238 hits; `grep -rln` → `crates/authority/src/core/{mod,replay,holder,inbox,client,delegate,tests}.rs`,
`crates/authority/tests/sim/run.rs`, `crates/harness/src/scenarios{.rs,/m6.rs,/m11.rs,/m14.rs,/ovh.rs}`,
`crates/cli/src/{daemon_lock,fusefs_ops,locks,main,fusefs,node_runtime,recovery,authority_driver}.rs`,
`crates/chaos/src/{op,elle}.rs`). Two crossing points already carry a raw
errno on the wire/in the journal:

- `MutateOutcome::Errno(i32)` (VERIFIED `crates/meta/src/mutate.rs:150-156`,
  a tuple variant inside the `MutateOutcome` enum), constructed at 5+ sites
  including `crates/authority/src/core/holder.rs:503,513,515` (contention/
  create-race fallbacks), `crates/authority/src/core/client.rs:982,1606`
  (I/O failure short-circuits), and consumed at
  `crates/cli/src/fusefs.rs:2278` (`MutateOutcome::Errno(e) =>
  Err(MutateFail::Errno(e))`, the FUSE-boundary conversion).
- `LogRecord::Refused { rid: Rid, errno: i32 }` (VERIFIED
  `crates/meta/src/record.rs:195`, doc comment `:184-194`: "a refusal is an
  outcome, never re-evaluated"), written at `crates/meta/src/store/inbox.rs:230`
  and `held.rs:844`, replayed at `crates/meta/src/replay.rs:642-648`
  (installs into the `completed` dedup table exactly like `LogRecord::Completed`).

`Code` wraps this with its **own** fixed, portable wire encoding — per the
"no backward compatibility" decision (§2), it does not reuse the Linux
errno number, so this is a wire-format break: pre-plan-31 S3 buckets and
local journals are not migrated and must be recreated:

```rust
// constellation-types::errno
#[repr(u16)]
#[non_exhaustive]
pub enum Code {
    NotFound = 1, Exists = 2, NotEmpty = 3, Stale = 4, NoData = 5,
    NameTooLong = 6, NoLock = 7, NotSupported = 8, Again = 9, Intr = 10,
    TimedOut = 11, Io = 12,
    // ... one variant per errno constant that appears in the 22-file census
    // above, each with a fixed, explicitly assigned discriminant that, once
    // shipped, never changes or is reused for a different meaning.
}
impl Code {
    pub fn to_wire(self) -> u16;       // Code's own discriminant, not any OS's errno
    pub fn from_wire(n: u16) -> Code;  // unknown values -> Code::Io, logged once
}
```

Each platform converts `Code` to/from its native representation **at the
boundary only**: Linux gets its own `Code ↔ i32` (libc errno) table — no
longer an identity mapping, since the wire form is `Code`'s own number, not
Linux's; macOS uses plan 34's existing errno table, rehomed as a `Code ↔
i32` conversion; Windows gets `Code ↔ NTSTATUS` in plan 35 — `Code` is
exactly the seam that needs, since NTSTATUS has no relationship to Linux
numbering at all. `libc::E*` disappears from library-crate logic outside
the conversion module; call sites match on `Code` variants. Apply at:
`MutateOutcome::Errno` encode/decode, `LogRecord::Refused` write and
replay (both now carry `Code`'s portable discriminant, not a raw libc
errno), `authority` client/holder/inbox `ESTALE` sites (the same sites
plan 34's M1/plan 35's W1 needed fixed for the Windows cross-check —
`Code::Stale` is now where they land instead of a raw `libc::ESTALE`), any
errno carried in P2P messages and control-API payloads (§10). rdev becomes
a portable `(major: u32, minor: u32)` pair on the wire and in the journal,
replacing Linux `makedev` packing; a `platform::{to_linux_rdev,
from_linux_rdev}` pair (plus macOS/Windows counterparts added in 34/35)
converts to/from each platform's native encoding only at the platform
boundary, owned by `constellation-types`/`constellation-platform` rather
than being a macOS-port-local concern. Pre-plan-31 buckets carrying the old
Linux-errno/makedev encodings are not migrated.

## 8. Validation and testing architecture

- **`vfs::conformance`**: a kernel-free suite driving `Vfs` with the
  `model` oracle (VERIFIED `crates/model`, 12,335 lines — the existing
  oracle the harness already checks against) and seeded workloads (the
  harness's `Workload` idea). Covers concurrency (multi-thread,
  multi-handle), cancellation, deferral, and invalidation-event assertions.
  Runs against `Engine`+`View` with every frontend's `PolicyStack`+
  `FrontendCaps` combination, on every OS in CI (on-device for Android).
- **`MockVfs`** for frontend-adapter unit tests: records calls, scripts
  replies — this is what lets a `constellation-frontend-fuse` or
  `-winfsp` adapter's translation logic (not the engine underneath) get
  unit-tested in isolation.
- **Property tests** (proptest) for policies and the `Code` mapping.
- **Harness additions**:
  - `--frontend` (today's harness has no frontend concept at all —
    VERIFIED, no `frontend` symbol found anywhere in `crates/cli` or
    `crates/api`; `Client::mount()` calls `mount_view(None, &[])`,
    VERIFIED `crates/harness/src/client.rs:282-284`, with no frontend
    parameter — this flag is entirely new).
  - `--s3-backend docker|process` (`docker` = today's floci + toxiproxy,
    VERIFIED `crates/harness/src/s3env.rs:1-6,12-13,96-146` — floci
    1.7.0-compat + toxiproxy 2.12.0 containers on a private docker
    network, `S3Env::start()` wires the proxy topology; `process` =
    versitygw + native toxiproxy, per plan 34's evaluation, so macOS/
    Windows CI that has no Docker can run the same scenarios).
  - `--shard i/n` and `--results-json` (added in C0, ahead of everything
    else, so every later milestone's harness runs produce machine-readable
    output from day one).
  - `Cap` derived from `FrontendCaps` (today's `Scenario` struct — VERIFIED
    `crates/harness/src/scenarios.rs:54-59`: `{ name: &'static str, desc:
    &'static str, requires: &'static [&'static str], run: fn(seed: u64) ->
    Result<()> }`, 176 entries, VERIFIED `grep -c 'name: "'` — gains a
    `caps: &'static [Cap]` field alongside `requires`).
  - Platform-neutral `Client` ops via `constellation-platform` (today
    `Client` — VERIFIED `crates/harness/src/client.rs:32-49` — calls
    `fusermount3` directly for unmount/kill9, VERIFIED `:526,581-590`, and
    signals via `libc::SIGSTOP`/`SIGCONT` at `:657-664`; these move behind
    `platform::mounts`/`platform::process`).
  - `harness smoke` (Rust-ported smoke), with the `.sh` scripts kept as
    thin wrappers.
  - `harness interop write|verify` (bucket moved as an artifact via `aws
    s3 sync`, per plan 34's interop-lane design — reused here rather than
    redesigned, since it already proves cross-OS journal/errno portability
    end to end).
- **Parity**: `tests/platform-parity.toml` + `tests/parity.py`, lanes
  named `<os>-<frontend>` (reference: `linux-fuse`). Only
  `skipped`-with-reason may differ from the reference; entries are
  two-way (a stale entry — one that no longer matches reality — fails the
  check too, exactly like the existing xfstests baseline convention).
  `tests/parity.py` supports any number of lanes from day one, each
  against its own declared reference, so plan 34/35/36 each add a lane
  without touching the checker.
- **`make check-cross`**: type-checks the whole workspace for
  `aarch64-apple-darwin` and the library crates (everything except
  `crates/cli`, which stays Unix-heavy until a WinFsp frontend exists) for
  `x86_64-pc-windows-gnu`, via zig-cc wrappers. No `tools/` directory or
  Makefile target for this exists yet (VERIFIED: `tools/` absent,
  `Makefile` has no `check-cross`/`check-darwin` target today) — C0
  creates both, adapting plan 34's already-drafted recipe (`CC_aarch64_apple_darwin`
  → `zig cc -target aarch64-macos` under `tools/zcc`, `AR=zig ar`; same
  wrapper exposes `-target x86_64-windows-gnu`).

## 9. The control protocol

### 9.1 Today's protocol, precisely

Line-delimited JSON, one `Request` per line, one `Response` per line
(VERIFIED module doc `crates/api/src/lib.rs:1-7`), over a unix socket at
`<state_dir>/control.sock` (`SOCKET_NAME`, VERIFIED `lib.rs:32`), served by
`pub fn serve(state_dir: &Path, source: Arc<dyn StatusSource>) -> Result<PathBuf>`
(VERIFIED `lib.rs:332-350`: binds, removes a stale socket left by a crash,
spawns one `tokio::spawn` per accepted connection). `handle()` (`:363-412`)
reads with `BufReader` + `read_until(b'\n', ...)` capped at 8 MiB
(`MAX_REQUEST_LINE`), deserializes `Request` with `serde_json::from_str`,
and calls `pub fn dispatch(source: &dyn StatusSource, request: Request) ->
Response` (VERIFIED `lib.rs:225-333`) — one exhaustive `match` over all 37
`Request` variants, each invoking the matching `StatusSource` trait method.
The doc comment at `lib.rs:225-228` states this centralisation is
deliberate: "both adapters deserialize the same enum and invoke this exact
function" — the axum HTTP adapter's `web::adapt()` (VERIFIED `web.rs:151-154`)
is a one-line wrapper around the same `dispatch()`. `StatusSource`
(VERIFIED `lib.rs:42`) requires only `fn status(&self) -> StatusReport`;
every other method (`pin`, `unpin`, `mount_add`, …) has a default that
refuses with a `String` message, so a partial implementor stays valid.

The web adapter (`crates/api/src/web.rs`) binds `TcpListener::bind((Ipv4Addr::LOCALHOST,
port))` — hard-coded loopback (VERIFIED `:41-49`) — and has **no
authentication** (VERIFIED module doc `:2-3`, confirmed by grep: no
`Authorization`/`Bearer`/token check anywhere in the file). Its only
protection is the loopback bind plus a DNS-rebinding Host/Origin guard
(VERIFIED `:107-140`, `ALLOWED_HOSTS = ["localhost","127.0.0.1","[::1]"]`
at `:76`, checked against both the `Host` header, refusing with 403 if
missing or not loopback, and — when present — the `Origin` header). Routes
(VERIFIED `:58-72`): `POST /api`, `GET /api/status`, `GET /api/download`,
`GET /metrics`, `GET /` and `GET /{*path}` (the embedded web UI, `rust_embed`
folder `webui/`). A unix↔HTTP parity test exists (`lib.rs:734-800`,
`#[cfg(feature="web")] async fn unix_and_http_adapters_have_request_parity()`)
but covers only 27 of the 37 `Request` variants (VERIFIED by exact
enumeration — it omits `SetQuota`, `GetQuota`, `DropHeld`, `PruneRun`,
`PruneList`, `GcRun`, `FsckRun`, `Delegate`, `Undelegate`, `ListDelegations`).
`Response::GcReport`/`FsckReport` carry an opaque `serde_json::Value`
specifically because `constellation-api` sits below `crates/cli` in the
dependency graph and cannot depend on `cli::gc::GcReport` (VERIFIED comment
`types.rs:266-273`) — this is exactly the kind of layering compromise
`constellation-control` sitting above `constellation-engine` (§4) removes:
the engine can define its own report types and `constellation-control` can
depend on the engine directly.

### 9.2 Every one of today's 37 methods, mapped to the control protocol

`Request` has exactly 37 variants (VERIFIED, `crates/api/src/types.rs:8-179`,
full enumeration cross-checked against the brief's list — it matches
exactly). The table below is the full census plus its control-protocol
mapping.

| # | old `Request` variant | old fields | control-protocol method | Service |
|---|---|---|---|---|
| 1 | `Ping` | — | `node.ping` | node |
| 2 | `Status` | — | `node.status` | node |
| 3 | `Pin` | `path` | `pin.add` | pin |
| 4 | `Unpin` | `path` | `pin.remove` | pin |
| 5 | `ListPins` | — | `pin.list` | pin |
| 6 | `Offline` | `path, read_only` | `designation.offline` | designation |
| 7 | `Delegate` | `path, node, range` | `designation.delegate` | designation |
| 8 | `Undelegate` | `path` | `designation.undelegate` | designation |
| 9 | `ListDelegations` | — | `designation.list_delegations` | designation |
| 10 | `Online` | `path` | `designation.online` | designation |
| 11 | `ListDesignations` | — | `designation.list` | designation |
| 12 | `Reintegrate` | — | `node.reintegrate` | node |
| 13 | `Leave` | `node_id, force` | `node.leave` | node |
| 14 | `SetWriteMode` | `mode` | `node.set_write_mode` | node |
| 15 | `PruneRun` | `path, dry_run` | `prune.run` | prune (+ plan 32 policy methods) |
| 16 | `PruneList` | — | `prune.list` | prune |
| 17 | `GcRun` | `verify_only` | `gc.run` | gc |
| 18 | `FsckRun` | `repair, force_release` | `fsck.run` | fsck |
| 19 | `SnapshotCreate` | `selector` | `snapshot.create` (gains an optional `hold: Option<String>` param — plan 32/37, §9.8) | snapshot |
| 20 | `SnapshotList` | `path` | `snapshot.list` | snapshot |
| 21 | `ListSnapshots` | `path` | `snapshot.list` (alias retired) | snapshot |
| 22 | `SnapshotDelete` | `selector` | `snapshot.delete` | snapshot |
| 23 | `Clone` | `selector, destination` | `clone.create` | clone |
| 24 | `SnapRefs` | `id` | `snapshot.refs` | snapshot |
| 25 | `ReadDir` | `path` | `browse.readdir` | browse (`ControlVfs`) |
| 26 | `Inspect` | `path` | `browse.inspect` | browse |
| 27 | `ForceRelease` | `part` | `locks.force_release` | locks |
| 28 | `LogTail` | `lines` | `node.logs.tail` | node |
| 29 | `Doctor` | — | `node.doctor` | node |
| 30 | `CacheList` | — | `cache.list` | cache |
| 31 | `CachePrune` | `target_bytes` | `cache.prune` | cache |
| 32 | `SetQuota` | `max_bytes` | `quota.set` | quota |
| 33 | `GetQuota` | — | `quota.get` | quota |
| 34 | `MountAdd` | `subtree, mountpoint, opts` | `view.mount` (gains a `source: MountSource` param — `Path` as today, or `PreopenedFd` via the transport's fd passing, §9.9; §6.11) | view |
| 35 | `MountRemove` | `mountpoint` | `view.unmount` | view |
| 36 | `MountList` | — | `view.list` (gains a `labels: BTreeMap<String,String>` filter, §9.10) | view |
| 37 | `DropHeld` | `ino, remote` | `locks.drop_held` | locks |

`ListSnapshots` is a confirmed additive alias of `SnapshotList` (VERIFIED
`types.rs:96-103`, "dispatch treats it identically") — the control protocol
collapses it to one method; the old alias is simply dropped (the old
protocol itself is deleted, not shimmed — §2).

**New methods**, none of which exist in `Request` today: `fs.*`
(registry list/create/import/export/passwd/doctor — `Manager`'s API,
currently CLI-only, not reachable over the control API at all), `view.*`
beyond mount/unmount/list stays as-is, `browse.stat/read/write/mkdir/rename/delete`
(`browse.delete{recursive}` removes a whole tree server-side, streaming
progress events and resumable after a crash — plan 37's trash purge uses
it; the rest of `ControlVfs`'s surface — today's `ReadDir`/`Inspect` only
cover listing and metadata, not mutation, over the control API),
`browse.xattr` (get/set/list/remove xattrs through `ControlVfs`, the
control-API-only xattr path plans 34/35/36 route scratch/prune/EA
controls through on frontends whose kernel-facing xattr cap is absent or
partial), `peers.*`
(peer/RTT/registry info — DESIGN.md §10 lists this as a control-plane
surface but it is not one of the 37 `Request` variants today), `stats.subscribe`,
`events.subscribe` (new streaming surfaces — the old protocol has no
`Event` frame at all, only request/response).

`fs.create` (part of `fs.*` above) is **idempotent**, keyed by
`(bucket, prefix)`: a call whose parameters match an existing filesystem's
returns that filesystem's uuid unchanged, not an error, and a call naming
an existing `(bucket, prefix)` with *different* parameters (chunk size,
compression, `e2e`, write mode) returns an error carrying `Code` (§7)
rather than silently reusing or silently changing the existing filesystem.
This is what lets plan 37's CSI controller call `fs.create` on every
`CreateVolume` for a `layout: pool` StorageClass without first checking
whether the pool filesystem already exists: the first caller creates it,
every later caller — including concurrent callers and callers on other
nodes — gets the same uuid back.

Also new, driven by plan 37's CSI needs (§9.8-9.10) rather than by any gap
in today's 37 methods: `fs.unlock` (supplies runtime credentials to an
already-registered filesystem, §9.8), `view.stats` (per-view statfs/rsize/
rcount, distinct from `node.status`'s whole-node summary — CSI's
`NodeGetVolumeStats` needs one view's numbers, not the daemon's), and
`node.handoff` (orchestrates the session-handover sequence of §6.11: stop
reading `/dev/fuse`, drain, export handles, hand off the fd — used by both
`constellation daemon --upgrade` on plain Linux and plan 37's engine-pod
replacement). `quota.set`/`quota.get` are unchanged in shape but are what
CSI's `ControllerExpandVolume` calls. `clone.create` and `node.leave`
already exist in the table above (rows 23, 13) with no shape change; CSI
uses them as-is (PVC cloning/volume-from-snapshot, and node drain).

### 9.3 Framing, handshake, frames

- **Framing**: `u32 length | u8 frame kind | payload`. After `Hello` the
  encoding is negotiated: JSON (default, debuggable — keeps today's
  human-readability for CLI debugging) or postcard (binary; UI and harness
  streams, where framing overhead matters at scenario-count scale).
- **Handshake**: `Hello{encodings, client:{name,version}}` →
  `Welcome{server_version, principal, roles, features[]}`. The handshake
  negotiates encoding and features only — there is no protocol-version
  field, because there is exactly one control protocol and no compatibility
  mode to select between.
- **Frames**: `Request{id,method,params}`, `Response{id,
  Ok(result)|Err{code:Code?, kind, message, details, remediation?}}`,
  `Event{sub_id, payload}`, `Cancel{id}`, `Chunk{id, seq, bytes, last}`
  (file transfer / log tail — replaces the old `LogTail`'s all-at-once
  `Vec<String>` with a streamable form).

### 9.4 Schema

Rust types in `constellation-control::proto` (serde); a JSON Schema
generated with `schemars`; TypeScript types generated for the UI (`ts-rs`
or `typeshare`, the choice is plan 33's), with a CI check that generated
files are current.

### 9.5 Authz

- The principal comes from the transport: peer uid/gids (unix socket, via
  `SO_PEERCRED` on Linux — nothing in today's `crates/api` does any
  peer-credential check at all, VERIFIED by the absence of `SO_PEERCRED`/
  `getsockopt` anywhere in `crates/api/src`), a Windows SID (35), in-process
  (the CLI talking to its own daemon, or the harness), an app signature
  (36), or a remote device key (33).
- Roles: `viewer` < `operator` < `admin`. By default the daemon owner is
  admin and everyone else is denied; a config allowlist grants groups
  roles. This is a strictly stronger default than today's "anyone who can
  open the socket, or reach 127.0.0.1, gets full access."
- Every method declares its minimum role.
- An audit log of mutating control actions: who, when, method, params
  digest.

### 9.6 `crates/api` is deleted outright, and transport relocation

- Per §2's "no backward compatibility" decision, `constellation-control`
  does not speak the old line-JSON protocol at all. `crates/api` —
  `dispatch()`'s 37-arm match (§9.1) included — is deleted from the
  workspace in C5, not kept alongside `constellation-control` as a shim.
  Every in-tree caller of the old protocol (the CLI, the harness) moves to
  `constellation-control` in the same milestone; there is no old-protocol-
  speaking client left to support.
- The control socket moves to a per-user runtime dir (0700):
  `$XDG_RUNTIME_DIR/constellation/` on Linux, `$TMPDIR` on macOS, kept
  ≤104 bytes (`sun_path`'s limit). There is no compat symlink or file kept
  at the old `<state_dir>/control.sock` path — `crates/harness/src/client.rs:780-793`'s
  hand-rolled `UnixStream` probe (VERIFIED — it dials `state_dir.join("control.sock")`
  directly, bypassing `constellation_api::call` entirely) is rewritten in
  the same milestone to use the new path and the new protocol; there is no
  transition period.

### 9.7 `ControlService` and `ControlVfs`

`ControlService` is implemented by the engine plus `Manager` — the same
object the CLI calls in-process and the daemon serves over
`constellation-control`. `ControlVfs` implements `Vfs` (§6) by wrapping
`Engine`/`View` with the authenticated control-protocol principal as
`Caller` — this is the brief's "browse/transfer is just another frontend"
made concrete: the UI's file browser, upload/download and rename/delete
(DESIGN.md §10's "file browser (rename/move/delete/up/download, inspect,
history)") drive the exact same `Vfs` trait a kernel mount does, gaining
every response-barrier guarantee (§6.3) for free instead of needing its
own ad hoc correctness story.

The existing web adapter keeps working, still loopback-only; plan 33 adds
token auth on top of it.

### 9.8 Credentials: `EphemeralSecretStore` and `CredentialSource`

`SecretStore` (§4, `constellation-platform`, C2) persists `node.key` and the
E2E pin to disk, which is correct for a desktop/server daemon and wrong for
plan 37's engine pods: StorageClass/VolumeSnapshotClass secrets (S3
credentials, an E2E passphrase) arrive per-request from the CSI plugin and
must never be written to a hostPath or a container filesystem. C2 adds a
second `SecretStore` implementation:

```rust
pub struct EphemeralSecretStore { /* memory only; never touches disk */ }

pub enum CredentialSource {
    AwsDefaultChain,                    // IRSA / EKS Pod Identity / workload identity
    Static(EphemeralSecretStore),       // supplied once, held in memory
    Refreshing(Box<dyn Fn() -> Credential + Send + Sync>), // re-supplied on rotation
}
```

`EngineConfig` gains a per-engine `credentials: CredentialSource`. The
control method `fs.unlock` (new, §9.2) supplies secrets at runtime —
`Static`/`Refreshing` sources are populated by it rather than by a config
file or CLI flag — and is how a CSI `NodeStageVolume`/`NodePublishVolume`
request's secret reaches the engine pod without ever landing on disk.
`AwsDefaultChain` needs no `fs.unlock` call at all: it defers to the AWS SDK's
own credential chain inside the engine pod, which is how IRSA/Pod Identity
already work for any AWS workload.

### 9.9 Transport: fd passing

`Transport` (§4, `constellation-control`) gains a capability for handing a
file descriptor across the control connection, needed for `view.mount`'s
`PreopenedFd` source (§6.11, §9.2 row 34):

```rust
pub trait Transport: Send + Sync {
    // ...existing framing methods...
    fn send_fd(&self, fd: BorrowedFd<'_>) -> io::Result<()>;
    fn recv_fd(&self) -> io::Result<OwnedFd>;
}
```

- **`UnixSocket`**: `SCM_RIGHTS` ancillary data alongside the frame —
  the standard unix-domain-socket fd-passing mechanism, and the transport
  plan 37's CSI node plugin uses to hand `platform::linux::fuse_mount_fd`'s
  result to the engine pod over a hostPath socket.
- **`InProcess`**: passes the fd directly (no serialization needed — caller
  and callee share an address space).
- **`NamedPipe`** (35): does not support fd passing. `view.mount{source:
  PreopenedFd}` is therefore a Linux/`UnixSocket`-and-`InProcess`-only
  capability, gated by a `Transport` capability flag the same way
  `FrontendCaps` gates per-frontend behaviour (§6.6) — a caller on an
  unsupported transport gets `Code::NotSupported`, not a hang.

### 9.10 View labels and per-view QoS

`ViewSpec` (the argument to `Engine::open_view`/`open_view_resumed`, §6.2,
§4.1) gains:

```rust
pub struct ViewSpec {
    // ...existing fields (subtree root, mount options, frontend caps)...
    pub labels: BTreeMap<String, String>,
    pub qos: ViewQos,
    pub confine_links: bool, // §6.12: link() across the view's subtree
                              // root returns EXDEV instead of succeeding
}
pub struct ViewQos {
    pub max_inflight_ops: Option<u32>,
    pub max_staging_bytes: Option<u64>,
}
```

`labels` is the general mechanism plan 37 uses to attach `{pv, pvc,
namespace}` to a CSI-provisioned view, but it is not CSI-specific: it is
visible in `view.list` (filterable — §9.2 row 36), in `OpWatch` entries, in
every op's tracing span, and in metrics — with a **bounded-cardinality
policy**: metrics attach only an explicit allowlisted subset of labels
(e.g. `pv`) to the `view` metric dimension (§6.10), never the full label
map, so an operator cannot accidentally create unbounded Prometheus series
by labelling views with high-cardinality values (namespaces are bounded in
practice; arbitrary user-supplied label values are not, so they stay in
`view.list`/tracing but are never promoted to a metric label).

`ViewQos`'s `max_inflight_ops`/`max_staging_bytes` are per-view instances of
the existing per-view admission control (§6.3's backpressure barrier,
today enforced per-view with no per-view *limit* configuration) — ops over
a view's own limit defer and, past deadline, return `Code::Again`, exactly
as the whole-engine backpressure barrier already does, just scoped tighter.
This is what isolates one PV from a noisy neighbour sharing the same
`Engine` (multiple PVs of one filesystem on one k8s node share an engine
pod, per plan 37's process model) without needing a separate `Engine` per
PV.

## 10. Engine profiles and lifecycle (for mobile, exercised on desktop)

```rust
pub struct EngineProfile {
    pub memory_budget: usize,
    pub cache_budget: usize,
    pub p2p: P2pMode,        // Listen | DialOnly | Off
    pub leases: LeaseMode,   // Hold | ForwardOnly
    pub uploads: UploadMode, // Always | UnmeteredOnly
    pub background: BackgroundMode, // Continuous | OnDemand
}
```

`LifecycleSource` host events: `Foreground`, `Background`,
`Suspending{deadline}`, `Resumed`, `NetworkChanged{reachable, metered}`,
`LowPower`. On `Suspending`, the engine runs `sync_view` for every view,
releases leases (forward-only mode) and quiesces P2P, then resumes
cleanly. Linux/macOS implement a manual `node.lifecycle` control method so
the harness can exercise this on Linux, ahead of any mobile port needing
it for real (C8).

### 10.1 `EngineProfile::Server`

Alongside whatever mobile-oriented presets 36 defines, C8 adds a `Server`
preset for dense multi-PV hosts (plan 37's engine pods, and any desktop/
server running `EngineHost` with several filesystems, §4.1): `p2p: Listen`,
`leases: Hold` (a server holds leases rather than forwarding — it is not
power/network constrained the way mobile is), `uploads: Always`,
`background: Continuous`, with `memory_budget`/`cache_budget` sized for a
long-lived process rather than a foreground-app budget. `Server` needs no
new fields on `EngineProfile` — it is a constructor (`EngineProfile::server(...)`)
producing a particular combination of the existing ones — and it is what
plan 37's engine pods use together with the per-view `ViewQos` (§9.10) to
keep noisy PVs from starving quiet ones on the same node.

## 11. Milestones

Each milestone ends with the CONVENTIONS gates green on Linux. C3 and C4
are the risky ones and are strictly behaviour-neutral: any harness or
pjdfstest delta on Linux at those milestones is a bug in the milestone, not
an accepted cost.

### C0 — Baselines and guardrails

- Record perf baselines: harness `bench`, meta-bench, fio-latency p50/p99,
  pjdfstest, harness `--results-json` for the full matrix.
- Add `--results-json` and `--shard` to the harness (§8) — first, so every
  later milestone's runs are machine-comparable from the start.
- Add `make check-cross` (§8) and the `tools/zcc` zig-cc wrapper; currently
  failing targets are recorded as known and fixed across C1-C3.
- Add CI job `cross-check` (§12).
- **Gate**: the six CONVENTIONS gates green, plus `make check-cross`
  recording (not yet fixing) today's failure census.

### C1 — `constellation-types`

- The portable `Code` enum (§7): a `#[repr(u16)]` enum with fixed,
  explicitly assigned discriminants, serialized as that number — not any
  OS's native errno. This is a wire-format break, per §2: pre-plan-31 S3
  buckets and local state dirs are not migrated and must be recreated.
- Conversions: a real Linux `Code ↔ i32` table (no longer identity), a
  macOS table (rehomed from plan 34's existing work), Windows NTSTATUS
  reserved for plan 35.
- Apply at `MutateOutcome::Errno` (`crates/meta/src/mutate.rs:150-156`),
  `LogRecord::Refused` (`crates/meta/src/record.rs:195`), the `authority`
  client/holder/inbox `ESTALE` sites, control payloads.
- rdev as a portable `(major, minor)` pair on the wire and in the journal,
  replacing Linux `makedev` packing; `platform::{to,from}_linux_rdev`
  converts at the Linux boundary only.
- `libc::E*` disappears from the 22-file census (§7) outside the
  conversion module.
- **Gate**: CONVENTIONS gates; a round-trip test per table entry; a golden
  test round-tripping `Code::NotEmpty` through its fixed wire discriminant
  and decoding it to the correct native errno on Linux (`ENOTEMPTY`, 39)
  and macOS (`ENOTEMPTY`, 66); an in-process two-node test (the
  `shipper.rs` pattern) forcing node A's `Code`s through the wire
  encoding.

### C2 — `constellation-platform`

- `HostServices` with Linux + macOS implementations; stubs for windows,
  android, ios, freebsd.
- Moves every `/proc`, `fusermount3`, fork, flock, `gethostname` and
  fallocate use out of engine/cli into `platform::linux`.
- `SecretStore` (file-backed on Linux; macOS Keychain deferred to 34's own
  milestone) for `node.key` and the E2E pin, plus the memory-only
  `EphemeralSecretStore` and the `CredentialSource` enum (§9.8) — the
  in-memory path plan 37's engine pods use so S3/E2E secrets never touch a
  container filesystem.
- `platform::linux::fuse_mount_fd` (§4.1): the privileged, fusermount3-free
  mount helper returning an owned `/dev/fuse` fd, shared by the plain-Linux
  daemon's future in-place upgrade (§6.11) and plan 37's CSI node plugin.
- `FsPrimitives`.
- **Gate**: CONVENTIONS gates; `make check-cross` shows `constellation-platform`
  itself clean for the Windows library-crate check.

### C3 — Extract `constellation-engine`

- A mechanical, behaviour-neutral move of the 40 engine modules (§5) out
  of `crates/cli`: the 35 clean ones move as-is; the 5 tangled ones
  (`authority_driver`, `gc`, `locks`, `prune`, `recovery`) move after
  `SyncRequest`/`AcquireProgress`/`HandoffResult` relocate from `fusefs.rs`
  to the engine crate (§5's single unblocking move for 4 of 5), with
  `locks.rs`'s `lock()` reply plumbing (lines 240-262) split out as a thin
  `crates/cli`-resident adapter that calls the moved arbitration API.
- `ConstellationFs` → `View`.
- `crates/cli` keeps the CLI and daemon host plus the fuse adapter
  (temporarily — through C4).
- Since `crates/cli` has no `lib.rs` today (§5), this milestone also
  defines `constellation-engine`'s first public API surface: every type
  the 35+5 modules exposed only as crate-private before now needs a
  deliberate `pub`/re-export decision, not a blanket `pub mod`.
- **Gate**: full CONVENTIONS gates, perf within noise of the C0 baseline.

### C4 — `constellation-vfs` contract, and FUSE rebuilt on it

- `Vfs`/`OpCtx`/`Responder`/`FrontendEvents`/`FrontendCaps`/policies/
  `OpWatch` (§6), implemented against `View`.
- `crates/frontend-fuse` becomes a thin adapter over `Vfs`: deferred
  replies for locks through `Responder` (generalising `locks.rs`'s
  `"lock-wait"` thread pattern, §6.3), `FrontendEvents` replacing
  `NotifySink` (a rename, not a redesign, §6.5).
- Every inline policy step the `fusefs_ops.rs` table in §6.1 lists (real_ino
  translation, `inflight.enter`, synthetic-node handling, `lock_fenced`,
  `checked_name!`, `strict_read`) becomes `View`-internal logic beneath
  `Vfs`, not per-adapter code — the adapter's job shrinks to translating
  fuser's callback shape into `Vfs` calls and translating `Responder`
  completions back into fuser replies.
- FUSE session handover (§6.11): `MountSource::{Path, PreopenedFd}`,
  `FuseSession::detach`/`resume`, `View::export_handles`,
  `Engine::open_view_resumed`, and the `vendor/fuser` patch that adds
  `Session::from_fd_resumed` (skips the `FUSE_INIT` handshake using an
  already-negotiated `NegotiatedInit`). Also lands `constellation daemon
  --upgrade` on plain Linux, exercising the primitive without plan 37.
- Subtree confinement (§6.12): `..`/lookup/handle bounding at the view
  root, `.constellation` synthetic-path confinement, and `ViewSpec::confine_links`
  (`link()` across the boundary → `EXDEV`).
- **Gate**: pjdfstest 8798/8798, harness full matrix, the §6.9 perf
  targets, all on Linux, unchanged from C3's baseline, plus the new session
  handover harness scenarios (§6.11): detach/resume with no in-flight ops,
  and an upgrade-under-load scenario asserting zero `ENOTCONN`/`EIO` for a
  sustained writer across `constellation daemon --upgrade`; plus the new
  subtree-confinement harness scenario (§6.12).

### C5 — The control protocol

- `crates/control` replaces `crates/api`: framing, handshake, the full
  37-method table (§9.2), streaming, cancel.
- `UnixSocket` transport with `SO_PEERCRED` authz and roles; `InProcess`
  transport; fd passing (`Transport::send_fd`/`recv_fd`, §9.9) on both,
  needed by `view.mount{source: PreopenedFd}` (§9.2 row 34, §6.11).
- `fs.unlock`, `view.stats`, `node.handoff` (§9.2, §9.8, §9.9) — the new
  methods plan 37 needs beyond the 37-method mapping.
- Client lib; the CLI and harness move to it; `crates/api` is deleted from
  the workspace in this milestone — no shim (§9.6).
- **Plan 32 overlap (settled).** Plan 32 (snapshot policies, owned by
  another session) adds control requests and web pages to `crates/api`.
  Whichever lands second carries the other's work: if plan 32 is committed
  before C5, C5 ports every plan-32 request into the control-protocol
  method table (`snapshot.policy.*`, the simulator, the space-accounting
  reads) with the same semantics before deleting `crates/api`, and its
  pages move to plan 33's SPA; if C5 lands first, plan 32 targets
  `constellation-control` directly. Either way no plan-32 method may be
  lost when `crates/api` is deleted, and C5's gate includes plan 32's CLI
  tests if they exist. This is also the rule plan 32's own "Coordination"
  section and plan 37 both point back to.
- `ControlService` implemented by the engine plus `Manager`; `ControlVfs`
  browse (§9.7).
- Schema generation (`schemars`, TS types).
- The existing web adapter keeps working, still loopback-only, now
  speaking the control protocol; 33 adds auth.
- **Gate**: CONVENTIONS gates; a unix-socket↔HTTP parity test covering the
  full control-protocol method table (§9.2), proving both transports
  dispatch identically (there is no old protocol left to compare against).

### C6 — Test architecture

- The conformance kit, `MockVfs` and proptest (§8), including the new
  `confinement` group (§6.12: view-root `..`, lookup/handle bounding,
  `.constellation` confinement, `link()`/`confine_links`).
- Harness: `--frontend`, `--s3-backend process`, derived `Cap`,
  platform-neutral `Client`, `harness smoke`, `harness interop`.
- `tests/platform-parity.toml` + `tests/parity.py` (§8).
- Linux lanes `linux-fuse` (docker floci, the reference) and
  `linux-fuse-process`.
- **Gate**: CONVENTIONS gates; both Linux lanes green, including the
  `confinement` conformance group; the parity checker itself unit-tested
  against a synthetic results set with a deliberate mismatch, to prove it
  actually fails closed.

### C7 — Observability and performance hardening

- Unified op metrics/tracing (§6.10); `OpWatch` exposed over control
  (`node.ops`).
- `vfs-bench` (criterion) added to the perf gate.
- Deferral of cold reads and lease waits via `Responder`, where the C0/C4
  measurements justify it — converting more of the `blocking_recv` sites
  cataloged in §6.1 from "always blocks" to "defers under load."
- **Mountpoint-derived performance candidates, measured here, not
  mandated.** `mountpoint-s3`/`mountpoint-s3-fs` (research clone, commit
  `e144a7bb84948045f0d7cde77060afaa7ed91b53`) made a handful of concrete,
  benchmarked design choices that are worth measuring against
  Constellation's own numbers before adopting any of them; each gets its
  own benchmark run and is adopted only if it wins:
  - **On-demand growing FUSE worker pool.** Mountpoint starts at 1 worker
    thread and grows by one whenever the *last idle* worker picks up work,
    capped at `max_worker_threads`, with `Forget` requests excluded from
    triggering growth (VERIFIED `mountpoint-s3-fs/src/fuse/session.rs`).
    Today's sizing is a fixed, host-computed `2 * ceil_sqrt(cpus)` chosen
    once at startup (VERIFIED `crates/cli/src/parallelism.rs:62-79`,
    `recommended_fuse_threads`, comment: "Twice sqrt(CPUs) gives useful
    oversubscription"). Benchmark: `harness bench` under a bursty
    open-file-count workload (today's fixed sizing either over-provisions
    idle threads or under-provisions a burst); adopt the growth policy
    only if it beats fixed sizing on both idle memory and burst latency.
  - **Userspace buffer pool for zero-copy, instead of `splice(2)`.**
    Mountpoint achieves its zero-copy read/write path entirely through its
    own userspace `Bytes`-style buffer pool (`mountpoint-s3-fs/src/memory/`,
    VERIFIED, no `splice` call anywhere in the fuser fork or
    `mountpoint-s3-fs` — grepped both) rather than `splice`-based FUSE
    zero-copy. This is a useful data point against §6.9's zero-copy target
    (`Bytes` from cache/mmap): it confirms `splice` is not required to hit
    that target. Benchmark: `vfs-bench`'s per-op allocation counter; adopt
    (or keep) the userspace-pool approach if it meets the ≤1µs/no-per-op-
    allocation target without `splice`.
  - **Prefetch window doubling to 64 MiB, with a kernel-readahead-sized
    first request.** Mountpoint doubles its flow-control read window on
    every sequential continuation, capped at 64 MiB, resets to the initial
    size on non-sequential access, and sizes its *first* request at 1 MiB
    + 128 KiB specifically to absorb Linux's own ~128 KiB kernel readahead
    in one round trip instead of two (VERIFIED
    `mountpoint-s3-fs/src/prefetch/caching_stream.rs:473-474`,
    `INITIAL_REQUEST_SIZE` constant + module doc). Constellation's own
    adaptive prefetcher (plan 18, `docs/plans/v1/done/18-p10a-adaptive-prefetch.md`)
    already grows a per-stream window on *stall* rather than on every read,
    starts at an 8 MiB floor, caps at 256 MiB (both larger than
    Mountpoint's numbers), and resets on a seek outside a 16 MiB reorder
    tolerance rather than on any non-sequential access — a materially
    different policy, not a smaller version of Mountpoint's. Benchmark:
    `fio-latency` (TTFB on a cold sequential read) specifically to check
    the kernel-readahead-alignment trick, which plan 18's design doesn't
    currently address; adopt only the pieces that measurably improve
    Constellation's own numbers, not the window-size constants wholesale.
  - **Memory limiter with a reserved read share.** Mountpoint's memory
    limiter reserves a fixed minimum number of "prunable" buffer slots
    (`PRUNABLE_RESERVED_PARTS`, VERIFIED `mountpoint-s3-fs/src/memory/limiter.rs`)
    off the top of its budget so a write-heavy workload can never starve
    read prefetch to zero, splitting the rest dynamically between
    `Upload`/`Prefetch`. Benchmark: `harness bench`'s mixed read/write
    scenario under a memory-constrained `EngineProfile` (§10); adopt a
    reserved-share split for Constellation's own cache/write budget
    (`ResourceBudget`) only if it measurably prevents write-starves-read
    regressions the current fully-dynamic split allows.
  - **S3 part size.** Mountpoint's default part size is 8 MiB for both
    read and write (VERIFIED `mountpoint-s3-client/src/s3_crt_client.rs:123-136`,
    `DEFAULT_PART_SIZE`), a fixed constant rather than something tuned at
    runtime. Constellation's `DEFAULT_CHUNK_SIZE` is 4 MiB (VERIFIED
    `crates/fs-core/src/lib.rs:20`), and `upload-concurrency` (VERIFIED
    `crates/upload-concurrency/src/lib.rs`) instead adaptively tunes
    *concurrency* (in-flight request count) rather than object size —  a
    different axis on the same throughput problem. Benchmark: `vfs-bench`
    plus a direct large-sequential-write throughput run at both chunk
    sizes; treat 8 MiB purely as a reference point to compare against, not
    a value to copy, since Constellation's chunk size is also the
    content-addressing unit (changing it is a wire-format break, §7),
    unlike Mountpoint's part size, which is a pure transport knob.
- **Gate**: CONVENTIONS gates; `vfs-bench` numbers recorded and within the
  §6.9 target; no regression on any earlier milestone's harness/pjdfstest
  baseline; each Mountpoint-derived candidate above has a recorded
  benchmark result and an explicit adopt/reject decision in the milestone
  writeup (silence is not adoption).

### C8 — Engine profiles and lifecycle

- `EngineProfile`, `LifecycleSource`, suspend/resume semantics (§10).
- `node.lifecycle` control method on Linux/macOS.
- New harness lifecycle scenarios on Linux (suspend mid-write, resume,
  verify no lost acks).
- **Gate**: CONVENTIONS gates; the new lifecycle scenarios pass; every
  earlier gate stays green (this is the last milestone before 33/34/35/36
  build on the result).

## 12. CI definitions

New job, added in C0, following the existing pattern (`dtolnay/rust-toolchain@stable`
plus the pinned `rust-toolchain.toml`, `Swatinem/rust-cache@v2` — matching
today's `ci.yml`, VERIFIED read in full: jobs `lint`, `test`, `integration`,
`harness`).

```yaml
  cross-check:
    name: Darwin + Windows-library type-check from Linux
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: "aarch64-apple-darwin,x86_64-pc-windows-gnu" }
      - uses: mlugg/setup-zig@v2
      - uses: Swatinem/rust-cache@v2
      - run: make check-cross
```

`nightly.yml` (VERIFIED job list today: `lint`, `unit`, `integration`,
`compliance`, `harness`, `xfstests`, `performance`, `linux-package`,
`macos`, `summary`) gains, across C6:

```yaml
  harness-lanes-linux:
    needs: unit
    strategy:
      fail-fast: false
      matrix:
        lane: [linux-fuse-process]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: sudo apt-get update && sudo apt-get install -y fuse3 fio stress-ng
      - run: bash tests/ci/install-native-s3.sh
      - run: cargo build --release -p constellation -p constellation-harness -p constellation-chaos
      - run: |
          target/release/harness run --s3-backend process --frontend fuse \
            --results-json results-linux-fuse-process.json 2>&1 | tee harness-linux-fuse-process.log
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: harness-linux-fuse-process, path: "*-linux-fuse-process.*" }

  conformance:
    needs: unit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace -p constellation-vfs --features conformance

  parity:
    if: always()
    needs: [harness, harness-lanes-linux, conformance]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with: { path: results, pattern: "harness-*", merge-multiple: true }
      - run: python3 tests/parity.py --expect tests/platform-parity.toml results/results-*.json | tee -a "$GITHUB_STEP_SUMMARY"
```

### `tests/platform-parity.toml`, seeded for C6 (grows as 34/35/36 add lanes)

```toml
# Reference lane: linux-fuse. For every scenario, every lane's outcome must
# equal the reference unless an [[expect]] entry covers that (scenario,
# lane). Entries that no longer match reality fail the check too (two-way,
# like the xfstests baseline). Wildcards are allowed only for capability
# skips.
#
# Lane names are "<os>-<frontend>". The checker supports any number of
# lanes, each compared against the reference lane above.
#
# Reserved for later plans (not added here): "macos-nfs" (34), "linux-nfs"
# (34), "windows-winfsp" (35), "android-saf" (36), "linux-csi" (37, a subset
# of the harness run through pods per plan 37's parity lane).

# linux-fuse-process differs from linux-fuse only in the S3 backend
# (versitygw + native toxiproxy instead of floci + dockerised toxiproxy), so
# it starts with NO expectations: any difference is a backend bug or a
# harness bug and must be fixed or explained here with a scenario-specific
# entry (never a wildcard). Entry shape, for reference:
#
# [[expect]]
# scenario = "<name or * (capability skips only)>"
# lanes = ["<os>-<frontend>", ...]
# outcome = "skipped"            # "failed" can never be expected
# cap = "<Cap>"                  # required when scenario = "*"
# reason = "<why this lane legitimately differs>"
#
# (The file is intentionally empty of entries here; a missing `expect` key
# means "no expectations". Plans 34/35/36 append `[[expect]]` tables.)
```

Both blocks above were validated to parse: the YAML parses cleanly when
wrapped under a top-level `jobs:` key (PyYAML), and the TOML parses with
`tomllib`.

## 13. Future ports (not planned in detail here)

- **FreeBSD**: fuser has a pure-Rust FreeBSD mount, so `frontend-fuse` plus
  a `platform::freebsd` module is the whole port. CI via
  `vmactions/freebsd-vm` on ubuntu runners (REPORTED; verify when planned).
- **iOS**: no mounts for apps; a File Provider extension
  (`NSFileProviderReplicatedExtension`) frontend, with an in-app engine
  using `EngineProfile` mobile and a tight memory budget; the Tauri iOS UI
  from plan 33; `platform::ios`.
- **Other Unix**: NetBSD/OpenBSD via fuser's pure-Rust paths, following the
  FreeBSD pattern.
- **Kubernetes CSI driver (plan 37)**: Linux-only, depends on C4/C5/C8 plus
  the CSI seams this plan adds (§4.1 `EngineHost`, §6.11 session handover,
  §9.8-9.10 credentials/fd-passing/view labels) and on plan 33's U1 (roles,
  allowlist, audit, service principals). It coordinates with plan 32
  (snapshot holds). Planned in detail in `37-kubernetes-csi.md`, not here.

### Compared with Mountpoint for Amazon S3

Mountpoint (AWS's FUSE driver for S3, and its CSI driver) is the closest
public reference point for plan 37's CSI work, and its own docs are
explicit that it is deliberately **not** a POSIX filesystem (VERIFIED
`mountpoint-s3/doc/SEMANTICS.md`): writes are sequential, single-writer,
whole-file only, with no in-place edits; `rename` is rejected everywhere
except S3 Express One Zone; there are no `flock`/`fcntl` locks at all;
and consistency is close-to-open by default, degrading further to a
metadata-TTL cache once caching is enabled, with negative lookups cached
too. That is a considered, stated trade-off on Mountpoint's part, not an
oversight — it buys a much simpler engine for a workload (many large
sequential reads against a durable object store) that never needed
POSIX's harder guarantees. Constellation's engine (this plan) keeps full
POSIX semantics, `getlk`/`setlk` cluster locks, a cross-node coop cache,
and epoch-fenced writes under concurrent writers throughout — a larger
scope than Mountpoint attempts, not a smaller one it happens to match.
Positioning, not a gap: the two projects are solving different problems,
and the performance ideas borrowed above (§C7, §6.11's vendoring model)
are borrowed because they are good engineering for *any* FUSE-over-object-
store frontend, independent of which POSIX guarantees sit on top.

## 14. Risks

- **C3/C4 are large mechanical refactors of the highest-stakes code in the
  tree** (the write path plan 30 spent 16 milestones hardening). Mitigation:
  both are declared behaviour-neutral with a hard gate (pjdfstest
  8798/8798, full harness matrix, perf within 3%) — any delta is treated
  as a bug in the milestone, not an acceptable refactor cost, and the
  5-module tangled list (§5) is scoped precisely enough in advance that
  there's no "discover the real shape mid-move" surprise.
- **`crates/cli` having no `lib.rs` means C3 also designs
  `constellation-engine`'s first public API**, which is more judgment than
  a pure file move. Mitigation: default to the narrowest `pub` surface
  that compiles, expand only when a caller (the CLI, or C4's `Vfs` impl)
  actually needs it — an API that's too narrow is a quick follow-up `pub`;
  one that's too wide is a compatibility promise made by accident.
- **The `locks.rs` split (§5) is the one module move that isn't purely
  mechanical.** Mitigation: it's scoped to exactly one method (`lock()`,
  lines 240-262) and can land as a temporary thin-adapter shim in C3,
  converted to a real `Responder` in C4 without a second migration of the
  arbitration logic underneath.
- **fuser 0.18's missing `FUSE_INTERRUPT` means the Linux FUSE frontend's
  `CancelToken` is inert in practice** (§6.3). Mitigation: documented
  explicitly rather than silently absorbed into "cancellation works" — the
  conformance kit's cancellation tests run against `MockVfs`/other
  frontends where the kernel does deliver cancellation, and the Linux FUSE
  adapter's gap is a named, tracked limitation, not a hidden one.
- **The control protocol's authz is new attack surface** (peer-cred checks,
  roles, an audit log — none of which exist today). Mitigation: plan 33
  owns hardening it further (remote transport, token auth for the headless
  web mode); this plan's job is only to get the unix-socket-local case to
  "peer-cred-checked with roles," a strict improvement over today's "opening
  the socket is authorization," not to close every future gap.
- **FUSE session handover (§6.11) needs a vendored `fuser` patch, and the
  gap it closes is only proven by a kernel-level test, not a compile-time
  one.** `Session::from_fd`'s unconditional `handshake()` (VERIFIED
  `fuser-0.18.0/src/session.rs:169-215,283-360`) makes a handed-over,
  already-`INIT`'d fd fail closed today. Mitigation: the patch is scoped to
  one new constructor that skips `handshake()` given an already-negotiated
  `NegotiatedInit` (the `vendor/fjall` pattern, already proven in this
  workspace — VERIFIED `vendor/fjall/CONSTELLATION-PATCH.md`); C4's gate
  requires the upgrade-under-load harness scenario to pass against a real
  kernel mount, not just a unit test of the handoff data structures.
  **This patch has no upstream precedent to lean on**: Mountpoint's own
  fuser fork (checked directly, not assumed — `mountpoint-s3-fuser`,
  commit `e144a7bb84948045f0d7cde77060afaa7ed91b53`) folds `INIT` into its
  dispatch loop but never resumes an already-initialized connection, so
  `from_fd_resumed` is original work, not a port; the maintenance cost is
  therefore also higher than "one small patch" — it needs the same
  ongoing-rebase discipline Mountpoint applies to its full fork
  (`tools/vendor-fuser.sh`, §6.11), not a fire-and-forget diff.
- **Two research passes independently confirmed the 37-request census and
  the module tangle list; a third pass (this document) re-verified the
  highest-leverage facts directly** (SyncHandle, InodeOps, the fuser
  Send-ness proof, the kernel_inval deadlock doc, the locks.rs offload).
  Residual risk is in the *un*-re-verified detail of the larger tables
  (e.g. the full `fusefs_ops.rs` line-by-line policy-work column) — a
  coding model implementing C4 should treat those columns as strong
  hints, not a substitute for reading the cited file:line itself before
  writing the adapter.

## 15. Definition of done

The CONVENTIONS gates, PLUS, per milestone (cumulative — a later
milestone's DoD includes every earlier one still green):

1. **C0**: `make check-cross` exists and runs (failures recorded, not yet
   required to pass); `--results-json`/`--shard` land in the harness;
   baseline numbers are committed to `docs/plans/v1/PROGRESS.md`.
2. **C1**: `Code` round-trip tests pass for every table entry; the golden
   cross-OS errno decode test passes; zero `libc::E*` outside the
   conversion module in the 22-file census.
3. **C2**: `constellation-platform` compiles for Linux, macOS (full) and
   windows/android/ios/freebsd (stubs); `make check-cross` shows it clean
   for the Windows library check.
4. **C3**: `constellation-engine` exists as its own crate; `crates/cli`
   shrinks by the 40-module move; pjdfstest 8798/8798; harness full matrix
   green; perf within 3% of the C0 baseline.
5. **C4**: `constellation-vfs` exists; `crates/frontend-fuse` exists and is
   what `crates/cli` mounts through; pjdfstest 8798/8798; harness full
   matrix green; `vfs-bench` numbers recorded and within target; FUSE
   session handover (§6.11) works end-to-end (`vendor/fuser`'s
   `from_fd_resumed`, `constellation daemon --upgrade`), and the
   upgrade-under-load harness scenario passes with zero `ENOTCONN`/`EIO`.
6. **C5**: `constellation-control` replaces `crates/api`, which is deleted
   from the workspace (no shim — §2); all 37 old methods have a
   control-protocol equivalent (§9.2's table, fully implemented, not just
   designed), plus `fs.unlock`/`view.stats`/`node.handoff` and fd passing
   (§9.8-9.9) work end-to-end; the unix↔HTTP parity test passes.
7. **C6**: the conformance kit runs in CI against `linux-fuse`; `MockVfs`
   has adapter unit-test coverage; `linux-fuse-process` lane green;
   `tests/parity.py` unit-tested against a synthetic mismatch (proves it
   fails closed).
8. **C7**: `vfs-bench` is part of the gated perf suite; `OpWatch` reachable
   over `node.ops`; metrics named per §6.10 are emitted and scraped by a
   test.
9. **C8**: lifecycle scenarios pass on Linux; `node.lifecycle` works
   end-to-end against a real daemon.
10. **Docs**: `docs/plans/v1/PROGRESS.md` gets a plan-31 section per
    milestone, in the established style; `docs/how-to-guides/development/TESTING.md`
    documents `--frontend`/`--s3-backend`/`--shard`/`--results-json` and the
    parity file; `docs/explanation/DESIGN.md` is **not** edited (per
    CONVENTIONS.md's rule) even though its §10 description of the control
    plane is now the old protocol's description — any contradiction is
    recorded in `PROGRESS.md`, not silently fixed in the spec.
11. **Report**: each milestone's report includes the harness summary line,
    the pjdfstest tally, and (from C4 on) the `vfs-bench` numbers next to
    the C0 baseline.

## Sources checked out for this plan

| Path | Used for |
|---|---|
| this tree, commit `a945b05` | every VERIFIED citation in §5-§10: `crates/cli/src/{fusefs,fusefs_ops,kernel_inval,fuse_watch,node_runtime,locks}.rs`, `crates/api/src/{lib,types,web}.rs`, `crates/meta/src/{mutate,record}.rs`, `crates/harness/src/{s3env,client,scenarios,main}.rs`, `Cargo.toml`, `crates/cli/Cargo.toml`, `.github/workflows/{ci,nightly}.yml`, `Makefile`, `docs/explanation/DESIGN.md` §10 |
| `/home/bra/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/fuser-0.18.0/` | `src/reply.rs` (the `Reply: Send + 'static` bound, per-type `ok`/`error`/`entry` signatures, `ReplyRaw`/`ReplySender`/`ChannelSender`/`DevFuse` Send chain), `src/session.rs:257-267` (`n_threads` Linux-only gating), `src/session.rs:169-215` (`Session::new`/`from_fd`, both ending in `self.handshake()`), `src/session.rs:283-360` (`handshake()`'s loop, its hard requirement that the first parsed operation be `Init`, and its `InvalidData` error on anything else — the basis for §6.11's session-handover risk) |
| `vendor/fjall/CONSTELLATION-PATCH.md` | the precedent this plan's `vendor/fuser` patch (§6.11) follows: a `[patch.crates-io]` path dependency, excluded from the workspace, with an exact upstream version/checksum and a hunk-level changelog |
| `docs/plans/v1/done/21-fs-registry-and-daemon.md` | the existing shared-daemon model (§Goal: "one daemon per (bucket, prefix)... share the replica, chunk cache, leases") that §4.1's `EngineHost` generalises from one engine with N views to N engines with their own views, without changing plan 21's per-filesystem semantics |
| `docs/plans/v1/wip/34-macos-port.md` | style template (section order, options-table format, CI YAML shape, parity TOML format), and REPORTED facts this plan carries forward without re-verifying: the Windows/Darwin cross-compile error census, the zig-cc `tools/zcc` recipe, the versitygw/toxiproxy process-backend evaluation |
| `/tmp/claude-1000/.../scratchpad/core-design-brief.md`, `/tmp/claude-1000/.../scratchpad/csi-brief.md` | the fixed plan numbering, crate names, trait sketches and milestone IDs this plan expands, plus the fixed CSI-seam names (§4.1, §6.11, §9.8-9.10) — every name in this document is taken verbatim from them |
| `mountpoint-s3` (`git clone --depth 1`, commit `e144a7bb84948045f0d7cde77060afaa7ed91b53`, `main`, 2026-09-28) and `mountpoint-s3-csi-driver` (commit `b450b22beae8bddc0d3c09551655b2b7d323e29b`, `main`, 2026-09-21), read via `/tmp/claude-1000/.../scratchpad/mountpoint-research.md` | §6.11's vendoring/INIT-dispatch-loop comparison and `tools/vendor-fuser.sh` precedent, §6.12's `EXDEV`-on-link precedent context, §C7's worker-pool/prefetch/memory-limiter/part-size benchmark candidates, and the "Compared with Mountpoint for Amazon S3" note (§13) — see that file's own §7 sources table for the exact file:line citations |
