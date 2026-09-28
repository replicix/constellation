# Plan 36 — Android port: a DocumentsProvider/SAF frontend, mirror folders, and an in-app engine

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context: `docs/explanation/DESIGN.md`
§5.2 (offline designations), §7 (pinning), §8 (P2P, addressing, E2E), §9
(admission/cache budgets), the lock and lease sections plan 30 hardened, and
`docs/explanation/GOALS.md`. Code context (this tree, commit `a945b05`,
paths read for this plan and cited by file/module doc-comment, not by
internal line number unless stated): `crates/cli/src/pin.rs` (294 lines,
`PinManager`/`PinFootprint`), `crates/cli/src/designation.rs` (133 lines,
`DesignationManager`), `crates/cli/src/epoch.rs` (1,065 lines, the
continuation-epoch coordinator), `crates/cli/src/reintegrate.rs` (33 lines,
`ReintegrationState`), `crates/cli/src/forward.rs` (132 lines, forwarding
knobs and the system-rid allocator), `crates/cli/src/lease.rs` (560 lines,
`LeaseView`), `crates/cli/src/e2e_pin.rs` (546 lines, the TOFU pin of a
filesystem's E2E state), `crates/store-s3/src/e2e.rs` (563 lines, KMK/E2E key
management), `crates/cli/src/coop/{exact.rs,fresh.rs}` (802 + 210 lines, the
cooperative cache's mirror/reconcile and fresh-chunk-hint machinery),
`crates/net/src/{endpoint.rs,identity.rs}` (1,500 + 176 lines, the iroh
endpoint and node identity).

**Depends on plan 31 (C0–C8) being committed** and **plan 33 (U0 at least)**
for the shared Svelte SPA this plan's app embeds. It uses the `Vfs` contract
(plan 31 C4), `constellation-control`'s `InProcess` transport (plan 31 C5),
`EngineProfile`/`LifecycleSource` (plan 31 C8), the conformance kit and
derived `Cap`s (plan 31 C6), and the `<os>-<frontend>` parity convention
(plan 31 C6). Its frontend declares `FrontendCaps` like every other
frontend; its tests run the conformance kit on-device plus the harness lane
`android-saf`, checked by the same `tests/parity.py`. The UI bundle is plan
33's Tauri mobile target. It is independent of plans 19, 23 and 32.

The research behind this plan was done on 2026-09-28. Every **VERIFIED**
claim below was checked directly against an Android Open Source Project doc
page, an `android.googlesource.com` source file, a `developer.android.com`
API reference/guide page, or this repository. **REPORTED** means a secondary
source (a blog, a forum thread, a GitHub issue, or general platform
knowledge not confirmed against a primary source this session) — A0 re-checks
every REPORTED item that gates a decision, on a real device, before anything
is built on it. Some items below are REPORTED specifically because the
official reference page did not return the exact text needed (noted inline);
these are exactly the claims A0's spike is designed to settle.

## Why

Constellation has no mobile story. The desktop ports (34 macOS, 35 Windows)
both mount a real kernel filesystem — FUSE, NFS or WinFsp — because desktop
OSes let an unprivileged, user-owned process do that. **Android does not.**
This is not a missing feature to work around with a cleverer frontend; it is
a platform boundary, and this plan's first job is to say precisely where
that boundary sits and what still fits inside it.

- **Apps cannot mount a filesystem.** VERIFIED
  (`source.android.com/docs/core/storage/scoped`): "Third-party apps cannot
  mount their own filesystems at `/storage/emulated`—this capability is
  restricted to system components through kernel-level restrictions (SELinux
  policies and absence of `CAP_SYS_ADMIN` capabilities for unprivileged
  processes)." There is no Android analogue of "a user-owned FUSE mount" the
  way Linux, macOS and (via WinFsp) Windows all have.
- **MediaProvider already owns the one FUSE mount that exists.** VERIFIED,
  same source: "On Android 11 or higher, MediaProvider functions as the
  filesystem handler (for FUSE) for external storage, making the file system
  on external storage and the MediaProvider database consistent." It is a
  userspace FUSE daemon that inspects every op to allow, deny or redact it
  per scoped-storage policy. Constellation cannot register a second handler
  for `/storage/emulated`, and cannot get underneath MediaProvider's FUSE
  layer without root.
- **The two sanctioned ways an unprivileged app publishes a filesystem-shaped
  surface are the Storage Access Framework** (a `DocumentsProvider`, backed
  by `StorageManager.openProxyFileDescriptor`/`ProxyFileDescriptorCallback`
  for a POSIX-like read/write file handle) **and MediaStore** (a database of
  media items, with write access mediated by `createWriteRequest`). Neither
  is a mountable POSIX filesystem: SAF is exposed through the system Files
  app and any app's document picker, backed by content:// URIs, not a path
  under which arbitrary programs (a game, a native library expecting
  `open(2)`, a build tool) can just read and write. This is settled Android
  architecture, not a Constellation gap.
- **Rooted devices are the one path back to a real mount**, via Magisk
  granting `/dev/fuse` access and the `mount` syscall to a process running as
  a privileged user. This is a real, working, but minority path (see the
  Decision table below) and is kept as an optional mode, not the plan's
  spine.

So "a full filesystem" on stock, unrooted Android can mean, at best: **(a)**
a SAF `DocumentsProvider` that every Android app already knows how to browse,
pick from and write through (Files app, "Open with", photo pickers, the
`ACTION_OPEN_DOCUMENT_TREE` flow used by e.g. a text editor saving back to a
Constellation path); **(b)** real files planted inside `/storage/emulated`
in the folders Android's convention-based apps expect (`DCIM/Camera`,
`Music`, `Download`), written through MediaStore's own APIs so those apps
find them, kept in sync with the cluster; and **(c)**, for the minority who
root their device, the same static Linux binary and FUSE frontend the Linux
port already ships, unmodified. This plan builds all three, with (a) and (b)
sharing one in-app engine.

## Decision: SAF/DocumentsProvider frontend + mirror folders, mobile `EngineProfile`, rooted FUSE as an optional escape hatch

### The options, with evidence

| Option | What it gives | What it cannot give | Verdict |
|---|---|---|---|
| **DocumentsProvider + `StorageManager.openProxyFileDescriptor`** | A tree any app can browse (Files app, document pickers, "Open with"), a POSIX-shaped read/write fd per open file, `ContentResolver.notifyChange` push invalidation. API 26+ for the proxy-fd path (VERIFIED, `developer.android.com` reference: `openProxyFileDescriptor` is API level 26). | Not a mount: nothing outside SAF-aware apps (a native binary calling `open("/storage/...")`, most games, many build tools) can reach it; each open file round-trips through a userspace daemon (`AppFuse`), so latency and (per REPORTED evidence below) `mmap` are worse than a kernel mount. | **Chosen**, for the "browse and edit cluster files from any Android app" job. |
| **Mirror folders in `/storage/emulated` via MediaStore-scoped APIs** | Real files under `DCIM/Camera`, `Music`, etc., visible to every convention-based app (Gallery, a music player) without SAF integration, using only `READ_MEDIA_IMAGES`/`VIDEO`/`AUDIO` + `READ_MEDIA_VISUAL_USER_SELECTED` + `MANAGE_MEDIA` — VERIFIED precedent, Immich mobile's manifest ships this exact permission set with **zero** `MANAGE_EXTERNAL_STORAGE` for a functionally identical camera-backup job (`android-research-sync.md` §3, `AndroidManifest.xml` + `ManageMediaPermissionDelegate.kt`). | Only the images/video/audio collections MediaStore governs (`DCIM`/`Pictures`/`Movies`/`Music`); no arbitrary path or file type. A file this app did not itself create still needs per-file consent unless `MANAGE_MEDIA` is granted, in which case batched `createWriteRequest`/`createDeleteRequest` (API 30+, VERIFIED `developer.android.com/training/data-storage/shared/media`) cover the rest without a per-file dialog. Not a general sync target. | **Chosen**, for the "camera roll / music library stays available offline and syncs" job in every flavor — not a competitor to SAF, a second surface for a different app population. See settled decision 4: this row alone is the **entire** mirror-folder story in the Play flavor; the `full` flavor additionally allows arbitrary non-media folders per the next row. |
| **`MANAGE_EXTERNAL_STORAGE` ("All files access") + direct `java.io.File`** | Full read/write to `/storage/emulated` by path, bypassing per-app scoping — the only way to mirror an arbitrary, non-media folder (e.g. `Documents`, any user-chosen path). | Restricted by Google Play policy to specific app categories — VERIFIED (Play Console Help, `support.google.com/googleplay`): permitted uses include "File manager apps, backup and restore apps, and document management apps"; usage "must be directly tied to the core functionality of the app" and Play prefers SAF/MediaStore where they suffice. It still does not let Constellation *mount* anything — it is a permission grant over MediaProvider's existing FUSE tree, not a new filesystem. Nextcloud, whose SAF frontend exposes arbitrary synced folders (a materially broader job than "mirror DCIM/Music"), does declare it (VERIFIED, `android-research-sync.md` §4); Immich, whose job is media-only, does not (previous row) — the permission tracks arbitrary-folder scope, not "does this app sync files" in general. | **Used narrowly, and only in the `full` flavor** (F-Droid + direct APK, settled decision 4/8): declared, and justified under "backup and restore" / "document management," only for arbitrary non-media folders the user explicitly configures for mirroring. The Play flavor never declares this permission at all — its mirror agent is confined to the row above. |
| **A custom ROM / system app** | Real mount, root-equivalent access. | Not installable on stock devices; not a product most users can adopt. | **Rejected.** |
| **WebDAV server the OS's Files app can attach** | No SAF integration needed; some file managers speak WebDAV. | Android's own Files app does not have a WebDAV client; this trades one integration problem for a different, narrower one, and adds a loopback HTTP listener on a phone. | **Rejected.** |
| **NFS/SMB export from the phone** | Same shape as WebDAV. | Android ships no NFS/SMB *client* UI integration for consuming its own loopback export, and exporting a server from a phone that sleeps/loses network constantly is the wrong direction anyway (a phone should be a client of other nodes' exports, if anything, not a server). | **Rejected.** |
| **Sync-client only, à la Syncthing (no SAF, no mirrors, app-private storage only)** | Simplest to build; works entirely inside the app's own sandbox, no permissions. | Fails the actual ask: files are invisible to every other app, defeating "a phone in the cluster." This is strictly a subset of what mirror folders already give for free. | **Rejected as the whole answer**; the app-private cache still exists internally as the SAF/mirror engine's staging area, just not exposed as the product. |
| **Rooted FUSE (Magisk + static Linux binary)** | The unmodified `crates/frontend-fuse` binary, cross-compiled `aarch64-unknown-linux-musl` (not `-android`), run under root with real `/dev/fuse` access — REPORTED working precedent: static `musl`-linked `fusermount`/FUSE tooling is used this way for `rclone mount` under Magisk on rooted devices (REPORTED, XDA Forums threads on "Fusermount on android (rclone mount)"; Android's bionic libc does not support fully static linking, which is exactly why the musl target, not the `-android` target, is used here). | Requires root — a small, technical minority of users; Magisk's mount-namespace isolation has been reported to hide FUSE mounts from other apps unless configured carefully (REPORTED, same XDA thread). Not distributable via Play. | **Kept as an optional mode**, zero new code: it is the existing Linux binary and `frontend-fuse`, invoked from a small Kotlin wrapper that shells out to `su`. Not a build target this plan invests engineering in beyond packaging it; see A5. |

### What "mobile" changes about the engine, not the frontend

The frontend choice above is half the story; the other half is that a phone
is not a workstation that happens to be small. Plan 31 C8's `EngineProfile`
and `LifecycleSource` exist because of this plan (design-brief "Engine
profiles and lifecycle (in 31, for mobile)"): `p2p: DialOnly` in the
background (no inbound QUIC listener draining battery/radio while the
screen is off), `leases: ForwardOnly` (a phone never holds a write lease
across a backgrounding — see settled decision 8), `uploads: UnmeteredOnly`
by default (no cellular data surprise bills), and `Suspending`/`Resumed`
lifecycle events sourced from `ConnectivityManager`/`PowerManager`/
`ProcessLifecycleOwner` instead of a signal handler. None of that is
Android-specific *code* in the engine — it is the mobile instance of
knobs plan 31 already defined for every platform.

## Prior art

Two research passes (2026-09-28) cloned and read eight SAF-provider/backend
apps and six background-sync/native-core apps, `--depth 1`, into
`/tmp/android-research/<name>`; commits and exact paths are in the Sources
table at the end of this document. Per app, what this plan borrows or
deliberately does not:

- **DAVx5** (`bitfireAT/davx5-ose` @ `e644134`) — the only app surveyed with
  a real random-access proxy-fd implementation, reads only. Borrowed: one
  dedicated `HandlerThread` per open proxy fd (VERIFIED,
  `RandomAccessCallback.kt:86-103`, "Using the main looper would cause
  ANRs"); its `AuthenticationRequiredException` pattern for expired
  credentials (VERIFIED, `DocumentProviderUtils.kt:70-80`); its
  per-mutation `notifyChange` discipline, notifying both source and target
  parent on move (VERIFIED, `MoveDocumentOperation.kt` etc.); its
  `RandomAccessCallbackWrapper` field-nulling `onRelease()` workaround for
  the AppFuseMount callback leak (VERIFIED,
  `RandomAccessCallbackWrapper.kt:20-29`); its per-instance (per-open-file,
  not global) `Mutex` around paged reads (VERIFIED, `PagingReader.kt:51-53`).
  Not borrowed: DAVx5 never uses the proxy fd for writes, downgrading to
  pipe+upload-on-close (`StreamingFileDescriptor.kt`) instead, because
  ranged WebDAV `PUT` is awkward to make correct — `frontend-saf` does use
  the proxy fd for writes (settled decision 16), since `Vfs`, unlike WebDAV,
  supports real random-access writes and DAVx5's reason to downgrade
  doesn't apply here.
- **Nextcloud** (`nextcloud/android` @ `007fe67`) — borrowed: the
  `queryChildDocuments` "serve cached rows immediately, set
  `EXTRA_LOADING=true`, refresh in background, `notifyChange` on
  completion" pattern (VERIFIED, `DocumentsStorageProvider.java:195-208`);
  its `WorkManager Constraints.addContentUriTrigger` local-write-detection
  design, now the plan's primary local-change signal (VERIFIED,
  `BackgroundJobManagerImpl.kt`'s `scheduleContentObserverJob()`, settled
  decision 13); its `Binder.clearCallingIdentity()`/`restoreCallingIdentity()`
  discipline around provider method bodies (VERIFIED). Not borrowed:
  Nextcloud's fully-synchronous "spawn a fresh `Thread` and `.join()` it"
  download-before-open, which its own comment calls a "dirty threading
  workaround" (VERIFIED, `DocumentsStorageProvider.java:246-248`) —
  `frontend-saf`'s proxy fd returns immediately instead (settled decision
  15). Its unimplemented `// TODO show a conflict notification` at
  `openDocument` (VERIFIED) independently confirms SAF has no in-band
  conflict hook, so this plan pushes conflicts to the notification channel
  instead (settled decision 18), as already designed.
- **Termux** (`termux-app` @ `8629e63`) — a negative example: zero
  `notifyChange`/`setNotificationUri`/`ContentObserver` calls anywhere in
  the provider (VERIFIED, zero grep hits), because its content changes
  only via Termux's own shell. Confirms `push_inval: Attr` (settled
  decision 6) is load-bearing for Constellation's cluster-driven changes in
  a way it never was for Termux; nothing borrowed beyond the observation
  that document IDs can just be the file's own path when there's a real
  local filesystem underneath (not Constellation's situation, see settled
  decision 5's `Ino`-based scheme).
- **Round-Sync / RCX** (`newhinton/Round-Sync` @ `bda00f8`,
  `x0b/rcx` @ `d98c4d6`) — a negative example on architecture: a real,
  separate `rclone rc` daemon process synchronized over a
  `ServiceConnection` with an explicit ready-wait poll loop (VERIFIED,
  `VirtualContentProvider.java:115-190`) — precisely the "is the daemon
  ready yet" race class plan 36's in-process design (settled decision 1)
  avoids by construction. Borrowed as risk items, not code: its "beta
  quality notes" comment block — some clients rely on cursor column order
  over name-based lookup, and the default client's timeout on slow
  mutations is **under 10 seconds** (VERIFIED,
  `VirtualContentProvider.java:68-84`) — now explicit A0/A6 checklist
  items (settled decision 15); its acknowledged, still-open
  `NetworkOnMainThreadException` TODO in `getDocumentType()` (VERIFIED,
  `VirtualContentProvider.java:935-936`), corroborating that a Binder pool
  thread is not automatically safe for blocking network calls.
- **Material Files** (`zhanghai/MaterialFiles` @ `c9b29cb`) — not a
  `DocumentsProvider` exporter; independent validation of Constellation's
  `Vfs`-trait design instead. Every one of its eight storage backends
  (local, root, SMB, SFTP, FTP, WebDAV, archive, and SAF-as-consumer)
  implements the same `java.nio.file.spi.FileSystemProvider` SPI, so the
  rest of the app (browsing, copy/move, search) is backend-agnostic
  (VERIFIED, file listing + `DocumentFileSystemProvider.kt`) — the same
  shape as `Vfs` unifying `frontend-saf` and every desktop frontend. No
  code borrowed; cited as corroboration that "one trait, many backends,
  one UI" is the right shape, from an app supporting more backend types
  than this plan needs.
- **Seafile** (`haiwen/seadroid` @ `9d6d490`) — same pipe/download-cache
  shape as Round-Sync, with its own local cache DB (VERIFIED). Confirms
  the "notify exactly the touched parent(s)" discipline a third
  independent time; nothing new borrowed beyond what DAVx5/Nextcloud
  already established.
- **Syncthing-Fork** (`Catfriend1/syncthing-android` @
  `02a95858e5192139353cd1188e9f5679eef0dd97`) — borrowed: the `specialUse`
  foreground-service type with a `PROPERTY_SPECIAL_USE_FGS_SUBTYPE`
  justification string for continuous background sync (VERIFIED,
  manifest, settled decision 14); its `RunConditionMonitor`
  single-decision-function shape for run conditions — Wi-Fi/SSID,
  charging, battery saver, time windows, metered handling (VERIFIED,
  `RunConditionMonitor.java`) — now the model for the plan-33 Android
  settings panel; its `WifiManager.MulticastLock` around local discovery
  (VERIFIED, `SyncthingRunnable.java`, settled decision 20); its
  `.sync-conflict-<timestamp>-<device-id>` conflict-file naming
  convention, adopted verbatim (settled decision 18). **Deliberately not
  copied**: its exec-a-`.so`-subprocess architecture — the upstream
  Syncthing Go binary is cross-compiled, shipped inside the APK's
  native-library directory literally named `libsyncthingnative.so`, and
  launched with `ProcessBuilder`/`exec` as a child process, not linked
  in-process (VERIFIED, `SyncthingRunnable.java`). This is the one core
  architectural choice this plan explicitly rejects as a model: it works
  around Android's no-detached-child-process restriction via a loophole
  (executing a binary named like a native lib from the app's own
  `lib/<abi>/` directory, which W^X policy exempts) that the fork's own
  docs describe as legacy and fragile — a proper in-process (`gomobile`)
  port "would involve a LOT of work" and nobody has done it (referencing
  `syncthing/syncthing-android` issues #29 and #1008), and newer Android
  versions restrict executing anything outside the app's own native-library
  directory, making the trick harder over time, not easier. Plan 36's
  in-process UniFFI+JNI design (settled decisions 1, 2, 17) avoids the
  whole category of problem instead of working around it — Syncthing-Fork's
  own choice corroborates that direction rather than contradicting it. Also
  not copied: its `REQUEST_IGNORE_BATTERY_OPTIMIZATIONS` battery-exemption
  request, a Play-restricted permission whose review friction is plausibly
  part of why the *official* `syncthing/syncthing-android` discontinued
  Play distribution entirely (settled decision 19).
- **Immich mobile** (`immich-app/immich`, `mobile/` @ `76c239c`) — the
  precedent for the permissions/flavors decision: zero
  `MANAGE_EXTERNAL_STORAGE`, using `READ_MEDIA_IMAGES`/`VIDEO`/`AUDIO` +
  `READ_MEDIA_VISUAL_USER_SELECTED` + `MANAGE_MEDIA` instead, for a
  camera-backup job functionally the same shape as Constellation's mirror
  folders (VERIFIED, manifest + `ManageMediaPermissionDelegate.kt`, settled
  decision 4). Also the precedent for combining `dataSync|shortService` on
  one foreground-service declaration (VERIFIED, manifest) — weighed
  against Syncthing-Fork's `specialUse` in settled decision 14.
- **Delta Chat** (`deltachat/deltachat-android` @ `aaa26f6a`,
  `chatmail/core` @ `1e36fb7`) — the precedent, and counter-example, for
  the JNI-boundary decision: embeds its Rust core via hand-written JNI and
  `ndk-build` (`jni/{Android.mk,Application.mk,dc_wrapper.c}`), with
  **zero** `uniffi` dependency anywhere in its workspace (VERIFIED). Plan
  36 does not switch its whole boundary to hand-written JNI on this
  evidence alone — UniFFI remains right for the low-frequency
  control/lifecycle/secret surface — but borrows hand-written JNI
  specifically for the hot `onRead`/`onWrite`/`onFsync` path, where Delta
  Chat's precedent and UniFFI's JNA-mediated per-call cost point the same
  direction (settled decision 17). Its iroh usage (`iroh = "0.35"`,
  `default-features = false`) was checked for Android-specific
  `netwatch`/network-change handling; none was found in the
  non-submodule source this session's shallow clone reached
  (REPORTED-absence-of-evidence, not a contradiction of settled decision
  20's directly-verified `netwatch`/`Endpoint::network_change()` finding,
  which was confirmed by reading `netwatch` 0.19.3's and `iroh` 1.1.0's own
  vendored source in this workspace's `~/.cargo/registry`, not by
  inference from Delta Chat).

## Settled decisions

Taken from the research above; do not relitigate them.

1. **Two frontends, one in-app engine.** `crates/frontend-saf` implements
   `Vfs`-driven `DocumentsProvider` + proxy-fd serving; the mirror-folder
   sync agent is **not** a `Vfs` frontend — it is a driver that turns `Vfs`
   change events into real files under MediaStore-governed directories and
   local filesystem writes back into `Vfs` ops (see "Mirror folders" below).
   Both share one `Engine` (plan 31's `constellation-engine`, `EngineProfile`
   mobile) hosted inside the Android app process — there is no separate
   daemon process and no fork/exec, because Android does not let an app spawn
   and keep alive a detached child process the way Linux/macOS/Windows do.
2. **`crates/mobile` is the UniFFI boundary.** It is a thin binding crate:
   `Engine::start`/`open_view`/`control()` from plan 31, exposed to Kotlin as
   a small set of UDL-described functions/objects (`EngineHandle`,
   `start(config) -> EngineHandle`, `open_view(spec) -> ViewHandle`,
   `control() -> ControlHandle`), plus callback interfaces the Kotlin side
   implements: `SecretStoreCallback` (Keystore-backed `SecretStore`) and
   `LifecycleCallback` (the `LifecycleSource` events). UniFFI is Mozilla's
   binding generator, in production use for exactly this shape of
   Rust-core/Kotlin-UI split — VERIFIED it targets Android
   (`mozilla.github.io/uniffi-rs`, the Kotlin/Gradle integration guide) and
   is what Mozilla uses to expose Rust to Kotlin in Firefox for Android
   (REPORTED, widely documented pattern, not independently re-verified this
   session). UniFFI's Kotlin bindings load the native library via JNA
   (REPORTED: JNA ≥5.12.0 required) — `crates/mobile`'s `Cargo.toml` pulls in
   `uniffi` as a build-dependency and the Gradle module's `build.gradle.kts`
   pulls in the generated Kotlin plus JNA. **This UniFFI/JNA boundary
   covers the control/lifecycle/secret surface only** (`EngineHandle`/
   `ViewHandle`/`ControlHandle`, `SecretStoreCallback`, `LifecycleCallback`)
   — the hot data path (`onRead`/`onWrite`/`onFsync` off `frontend-saf`'s
   proxy fd) bypasses UniFFI/JNA entirely in favor of hand-written JNI; see
   settled decision 17.
3. **The control transport is `InProcess`** (plan 31 C5): the Kotlin UI, the
   DocumentsProvider's binder thread, the foreground service and the mirror
   agent are all in the same process as the engine, so there is no socket,
   no named pipe, and no `Remote`/iroh-control transport needed for the
   local case. Plan 33's remote-management milestone (U7, optional) is how a
   *different* device's UI would reach this phone's engine, over iroh with
   a separate ALPN — this plan does not build that leg, only makes sure
   `crates/mobile` does not preclude it (the `Engine::control()` handle is
   the same object either transport would front).
4. **`MANAGE_EXTERNAL_STORAGE` is never requested by the Play flavor, at
   all.** The SAF `DocumentsProvider` needs **no storage permission at
   all** — that is the entire point of building on SAF instead of reaching
   for the broad permission first — and the Play flavor's mirror-folder
   agent is scoped to **MediaStore-governed media collections only**
   (images/video/audio; `DCIM`/`Pictures`/`Movies`/`Music`), using
   `READ_MEDIA_IMAGES`/`READ_MEDIA_VIDEO`/`READ_MEDIA_AUDIO` +
   `READ_MEDIA_VISUAL_USER_SELECTED` (Android 14+ partial-access handling)
   + `MANAGE_MEDIA` to modify/delete without a per-file consent dialog,
   falling back to batched `createWriteRequest`/`createDeleteRequest` where
   `MANAGE_MEDIA` isn't granted. Files the app itself created (its own
   `MediaStore`-inserted rows) need no extra permission at either tier.
   **Immich is the precedent**: a shipping camera-backup app functionally
   equivalent to Constellation's mirror-folder job, achieving full
   camera-roll read plus own-write-without-per-file-dialogs with this exact
   permission set and **zero** `MANAGE_EXTERNAL_STORAGE` (VERIFIED,
   `android-research-sync.md` §3, Immich's manifest +
   `ManageMediaPermissionDelegate.kt` wrapping
   `MediaStore.canManageMedia()`/`Settings.ACTION_REQUEST_MANAGE_MEDIA`;
   the `MANAGE_MEDIA` permission, `MediaStore.canManageMedia()` and
   `ACTION_REQUEST_MANAGE_MEDIA` all arrived together in API 31 / Android
   12 (VERIFIED: Android `Manifest.permission` reference "Added in API
   level 31", and the "Access media files from shared storage" guide).
   On API 29–30 the mirror agent uses batched `createWriteRequest`/
   `createDeleteRequest` consent (API 30) or, on 29, only files it
   created itself).
   **Arbitrary non-media folders** (`Documents`, any user-chosen path) are
   **not** offered by the Play flavor at all — there is no permission that
   grants that scope without All Files Access. They exist only in the
   **`full` flavor** (F-Droid + signed direct APK sideload, never
   distributed through Play — settled decision 19/distribution below),
   which requests `MANAGE_EXTERNAL_STORAGE`, declared and justified under
   "backup and restore" / "document management" (the two Play-permitted
   categories VERIFIED above), scoped Gradle-flavor-wide so the manifest
   itself differs, not a runtime toggle. This replaces the plan's earlier
   `safOnly`/`full` framing: **every** flavor ships the SAF frontend and a
   MediaStore-scoped mirror agent; only the `full` flavor additionally
   offers arbitrary-folder mirroring, and only the `full` flavor ever
   declares `MANAGE_EXTERNAL_STORAGE` — see A7 and Packaging.
5. **Document IDs are stable across restarts and map onto `Ino`.** A SAF
   document ID is a string the provider controls; Constellation's `Ino` is a
   `u64`. The mapping is `doc_id = base36(ino) ++ ':' ++ generation-tag`
   where the generation tag exists only to let a `SetDelete`-style
   "unlinked but a doc ID is still cached by some other app's picker
   result" situation return a clean not-found instead of resurrecting a
   different, later file that reused the same `Ino` (Constellation, like
   most POSIX filesystems, can reuse an `Ino` after `unlink`+GC). The
   generation counter is the same idea as NFS's stable-file-handle
   `fs_uuid‖view_id‖ino` from plan 34 M1, narrowed to the case SAF actually
   needs: detect reuse, not survive a restart across handle-space changes
   (`Ino` itself already survives restarts, being metadata-store state).
6. **`FrontendCaps` for `frontend-saf`**: `push_inval: Attr` (via
   `notifyChange`, not `Full` — see semantic differences below),
   `per_close_flush: true` (SAF's `ParcelFileDescriptor` close is the flush
   point), `cluster_locks: false` (no lock UI concept in SAF; `flock`/`fcntl`
   never reach a `DocumentsProvider` caller in the first place), `xattrs:
   None`, `hard_links: false`, `fallocate: false`, `seek_hole: false`,
   `special_files: false`, `case: Sensitive` (Constellation stores bytes;
   SAF does not impose folding), `max_io`: the proxy-fd chunk size chosen in
   A0 from the throughput spike, `deferrable: {}` (empty: `ProxyFileDescriptorCallback`'s
   `onRead`/`onWrite`/`onFsync` return their result synchronously, so every
   op completes on the calling thread. They run off Android's main thread
   on dedicated `Handler`/`Looper` threads, one per open proxy fd, so a
   blocking `Vfs` wait parks only that fd's thread and is never a
   UI-thread hazard — see architecture below),
   `open_unlinked: DeleteOnClose` (SAF has no silly-rename concept; a file
   deleted while open there is simply gone, matching `DeleteOnClose` rather
   than FUSE's "keep the inode" or NFS's silly-rename).
7. **`NamePolicy`/`XattrPolicy` for `frontend-saf`**: identity `NamePolicy`
   (SAF display names are UTF-16 Java strings; Constellation names are
   bytes — the policy rejects/escapes invalid UTF-8 the same way the
   Windows port's plan escapes non-UTF-8 names into a private-use band,
   reusing that mechanism rather than inventing a second one).
   `XattrPolicy` is moot: SAF has no xattr concept, so scratch/prune markers
   are control-API-only on Android, exactly like the Linux-NFS and Windows
   rows in plans 34/35's semantic-differences tables.
8. **Leases are forward-only, unconditionally, in the mobile profile.** A
   phone backgrounding is indistinguishable, from the engine's point of
   view, from a laptop closing its lid — except it happens far more often
   and Android can freeze or kill the process with much less notice
   (`Suspending{deadline}` may carry a deadline as short as a few seconds
   under Doze/App Standby). `EngineProfile.leases = ForwardOnly` means this
   phone's `View`s never hold a write lease past the current op; every
   mutation is either admitted locally under a lease another node holds
   (forwarded, per `crates/cli/src/forward.rs`'s existing requester-side
   forwarding path) or, if this phone is the only writer of a never-touched
   subtree, acquires-and-immediately-forwards-eligible per the same
   `authority` core logic every other node uses — there is no
   mobile-specific forwarding code, only the profile flag that makes
   `ForwardOnly` the *only* mode a phone ever runs in, where a desktop can
   choose `Hold`.
9. **Pinning and offline designation work as on desktop, budgeted smaller.**
   `crates/cli/src/pin.rs`'s admission control (`PinFootprint`, reserve-
   before-accept against the cache budget) is unchanged; the mobile
   `EngineProfile.cache_budget` is simply a much smaller number, tuned in A0
   against real device storage (typically far less free space than a
   workstation, and shared with the OS/other apps' caches). Offline
   designation (`DesignationManager`, `crates/cli/src/designation.rs`) is
   available but expected to be rare from a phone — a phone is far more
   often a *reader* of another node's designation than a designee itself,
   given its connectivity is the most intermittent in the cluster.
10. **E2E passphrase handling reuses `crates/store-s3/src/e2e.rs` and
    `crates/cli/src/e2e_pin.rs` unchanged, with the passphrase held in
    Android Keystore instead of a flock'd file.** The TOFU pin file
    (`e2e_pin.rs`'s per-filesystem fingerprint-of-the-master-key check)
    moves into the app's private storage (already sandboxed, no permission
    needed); the KMK-unwrap passphrase itself is not stored in plaintext
    anywhere — it is entered once, used to derive the Argon2id-wrapped KMK
    open per `e2e.rs`, and the *unwrapped* session key (not the passphrase)
    is what `platform::android`'s `SecretStore` optionally caches, encrypted
    at rest by an Android Keystore key that can be configured to require
    biometric/device-credential auth on each use
    (`BiometricPrompt`/`setUserAuthenticationRequired`, REPORTED standard
    Android Keystore API, not independently re-verified this session — A3
    confirms the exact API surface against the target `compileSdk`).
11. **Per-device scoped, short-lived S3 credentials.** A phone is the
    highest-risk device in the cluster to lose or have compromised (REPORTED
    general threat-model reasoning, not a claim about any specific IAM
    product). It is issued its own IAM principal (STS session credentials or
    an equivalent short-TTL token, whichever the control-plane's plan-33
    security hardening settles as the general per-device credential
    mechanism) rather than sharing the desktop's long-lived key, so a lost
    phone is revoked by revoking one principal, not rotating the whole
    cluster's credentials. This plan does not invent a new credential
    mechanism; it is the first consumer that actually needs per-device
    scoping to be real rather than theoretical, so it is the forcing
    function for whatever plan 33 U1 ships.
12. **Rooted FUSE mode is packaged, not engineered.** It runs the exact
    `aarch64-unknown-linux-musl` build the Linux static-binary release
    already produces (no Android-specific Rust code at all — `fuser`,
    `iroh`, `fjall` and everything else run exactly as they do on a Linux
    server), launched from a small Kotlin `su`-wrapper Activity/Shizuku
    integration. It is documented as unsupported-by-Play, best-effort, and
    is not on the critical path of any milestone below except A5, where it
    is packaged as an optional sideload artifact.
13. **Local-change detection is MediaStore-generation-based incremental
    diff, primary; `WorkManager` content-uri triggers drive it; a periodic
    full reconcile is the safety net; `FileObserver` is demoted to a
    `full`-flavor-only, non-media-folder mechanism.** The mirror driver's
    canonical signal is `MediaStore.getGeneration(context, volume)` plus a
    query for rows with `GENERATION_MODIFIED`/`GENERATION_ADDED` greater
    than the last-seen generation — VERIFIED (official `MediaStore`
    reference, fetched this session: generation numbers "monotonically
    increase" per volume and are documented as more robust than
    `DATE_MODIFIED`, since the latter can move backwards or sideways under
    `File#setLastModified`/a wrong system clock; `GENERATION_MODIFIED` is
    available from **API 30**; `MediaStore#getVersion` must be checked
    first, since a version change means generations were reset and a full
    resync is required). This diff is **triggered**, not polled, via
    `WorkManager Constraints.Builder().addContentUriTrigger(uri,
    descendants=true)` with `setTriggerContentUpdateDelay`/
    `setTriggerContentMaxDelay` debounce windows — **VERIFIED, Nextcloud's
    current (2025/2026-dated) production pattern**
    (`android-research-sync.md` §4, `BackgroundJobManagerImpl.kt`'s
    `scheduleContentObserverJob()`: 5s update delay, 10s max delay, a
    `CoroutineWorker` that re-arms the trigger in its own `finally` block
    since `addContentUriTrigger` fires once). Nextcloud's own history is
    direct evidence for demoting `FileObserver`: a repo-wide grep for
    `FileObserver` in its current tree returns **zero matches** (VERIFIED)
    — a large, actively maintained sync client removed it entirely in favor
    of this pattern. A live `ContentObserver` (registered while the app/FGS
    is active) supplements it for lower-latency detection during an active
    session, and a periodic full-reconcile `WorkManager` job (≥15 min,
    battery/charging-aware, gated at *execution* time by a power-state
    check as well as schedule time — Nextcloud's
    `PowerManagementService.blocksAutoUpload` does both, VERIFIED) is the
    safety net that also detects deletions, since neither generation diff
    nor a content-uri trigger fires on file removal in a way that
    distinguishes "deleted" from "never seen": deletions are detected by
    generation diff plus reconcile, never by assuming absence during
    partial media access. `FileObserver`/inotify is kept **only** for the
    `full` flavor's arbitrary non-media folders (settled decision 4), where
    no MediaStore signal exists at all; A0 verifies directly whether
    inotify fires reliably under `/storage/emulated`'s MediaProvider-FUSE
    layer for those paths (REPORTED-contradictory evidence going in: one
    secondary source claims `FileObserver` "bypasses FUSE," not
    corroborated by an AOSP/developer.android.com page) — if it does not,
    the `full` flavor's non-media mirroring falls back to the same
    periodic-reconcile safety net, at coarser latency, which is a
    pre-agreed fallback, not a blocker (see A0).
14. **Foreground-service strategy**: `specialUse` for continuous
    user-enabled sync, `dataSync|shortService` for bounded bursts,
    `WorkManager` for scheduled reconciliation, never `connectedDevice`.
    See the dedicated `platform::android` and the foreground-service
    strategy section below for the full VERIFIED precedent and the A0
    question this settles a default for, not a final answer.
15. **`frontend-saf`'s binder-thread and proxy-fd entry points follow four
    VERIFIED production patterns, not just the AOSP contract read
    literally.** (a) `openDocument`/`StorageManager.openProxyFileDescriptor`
    returns immediately — no cold fetch before the fd is handed back; data
    loads lazily on the first `onRead`. This is how DAVx5's proxy-fd path
    already behaves (its `fileDescriptor()` call returns fast; page loading
    happens only on `onRead`, VERIFIED) and closes the gap Round-Sync/RCX's
    own field notes flag: the default SAF client's timeout is **under 10
    seconds** (VERIFIED, `VirtualContentProvider.java:68-84`'s "beta
    quality notes"), and `openDocument` has no `EXTRA_LOADING`-shaped
    affordance to return a placeholder and fill in later the way a listing
    does — so it must simply return fast, always. (b) `queryChildDocuments`
    returns cached metadata immediately with `EXTRA_LOADING=true`, kicks a
    background refresh, and calls `notifyChange` on the child-documents URI
    when fresh data lands — VERIFIED, Nextcloud's `queryChildDocuments`
    (`DocumentsStorageProvider.java:195-208`, `ReloadFolderDocumentTask`).
    Binder-thread entry points (`query*`, `create`/`delete`/`rename`/`move`)
    carry a strict time budget and never block on network themselves,
    since a `DocumentsProvider` callback thread is **not** automatically
    exempt from `NetworkOnMainThreadException`-shaped restrictions despite
    running on a Binder pool thread rather than the caller's UI thread —
    Nextcloud's `openDocument` explicitly spawns-and-`join()`s a fresh
    `Thread` to work around exactly this (its own comment: "dirty
    threading workaround," VERIFIED `DocumentsStorageProvider.java:
    246-248`), and Round-Sync/RCX has an open, acknowledged
    `NetworkOnMainThreadException` TODO in `getDocumentType()` (VERIFIED,
    `VirtualContentProvider.java:935-936`) — A0 re-verifies directly that
    `frontend-saf`'s own dedicated-`HandlerThread` design sidesteps this
    rather than assuming any off-UI-thread placement is automatically safe.
    (c) An expired S3 credential or a Keystore-locked E2E session key throws
    `AuthenticationRequiredException` (API 26+) carrying a `PendingIntent`
    to the unlock/sign-in screen, rather than a bare
    `FileNotFoundException` — VERIFIED precedent, DAVx5's HTTP-401 handling
    (`DocumentProviderUtils.kt:70-80`); this is a genuine improvement over
    the majority (3 of 4) of real `DocumentsProvider`s surveyed, which just
    fail silently and rely on in-app UI to prompt re-login. When the
    session key is locked, `queryRoots` degrades the same way Nextcloud's
    lock-screen gate does — returns an empty root cursor rather than
    surfacing stale or inaccessible entries (VERIFIED,
    `DocumentsStorageProvider.java:121-126`). (d) Every mutation
    (`create`/`delete`/`rename`/`move`) calls `ContentResolver.notifyChange`
    on **every touched parent** — both source and target on a cross-directory
    rename/move — never a blanket invalidation; VERIFIED as the consistent
    discipline across DAVx5, Nextcloud, and Seafile (per-mutation, not
    global, `notifyChange`). Concurrency uses a per-open-fd dedicated
    `HandlerThread` (already settled decision 6) **guarded by a per-file
    lock, not a global mutex** — VERIFIED, DAVx5's `PagingReader.readPage()`
    Mutex is explicitly per-instance (per open file), with the code comment
    "a shared mutex would needlessly serialize reads across unrelated,
    concurrently open ... files" (`PagingReader.kt:51-53`). Finally, the
    Kotlin-side proxy-fd callback wrapper releases every reference it holds
    (not just closes resources) inside `onRelease()`, working around the
    AOSP `AppFuseMount` callback-registration memory leak
    (`issuetracker.google.com/issues/208788568`, fixed 2024-08-24): without
    the fix, every open proxy fd's callback — and everything it references,
    including `Vfs`/engine handles — stays resident until the app's whole
    AppFuse mount is torn down, not just that one file's close. VERIFIED,
    DAVx5's `RandomAccessCallbackWrapper.kt:20-29` documents and works
    around exactly this. A0 records which `minSdk`/target OEM images still
    carry the pre-fix behavior, since this is a new risk not previously in
    this plan's semantic-differences table.
16. **The proxy fd is used for writes too** (`"w"`, `"rw"`, `"rwt"` modes),
    not just reads — because `Vfs` supports correct random-access writes
    and truncation, unlike WebDAV `PUT`, which is exactly why DAVx5 (the
    one app surveyed with a real proxy-fd implementation) never uses
    proxy-fd `onWrite` for any backend and downgrades every write to a
    pipe-with-upload-on-close instead (VERIFIED,
    `RandomAccessCallback.kt:120-124` unconditionally throws
    `ErrnoException(EROFS)` for `onWrite`, comment: "ranged write requests
    not supported by WebDAV (yet)"). Constellation's `Vfs` doesn't have
    that constraint, so this plan makes the opposite choice deliberately
    rather than inheriting DAVx5's by default. A0 benchmarks proxy-fd
    writes against pipe+commit-on-release directly; the **pre-agreed
    fallback**, if proxy-fd writes prove unacceptably slow, is
    pipe+commit-on-release for `"w"` mode only (matching Nextcloud's
    API-26 `ParcelFileDescriptor.open(File, mode, Handler,
    OnCloseListener)` close-hook shape, VERIFIED
    `DocumentsStorageProvider.java:279-312`) — this is not a
    stop-the-plan finding, just a narrower default.
17. **The hot data path uses hand-written JNI, not UniFFI.** UniFFI stays
    the boundary for the control/lifecycle/secret APIs (settled decision
    2), but `onRead`/`onWrite`/`onFsync` — called once per I/O operation,
    potentially thousands of times a second under load — go through a
    hand-written JNI bridge (the `jni` crate) using direct `ByteBuffer`s,
    to avoid per-call JNA marshalling overhead and the extra copy UniFFI's
    generated Kotlin bindings would otherwise impose (UniFFI's Kotlin
    bindings load the native library via JNA, per settled decision 2,
    REPORTED not independently re-verified against byte-buffer-heavy hot
    paths specifically). **Delta Chat is the precedent for hand-written
    JNI on Android at this exact Rust-core-plus-iroh scale**: it embeds
    its Rust core via a hand-written C shim and `ndk-build`
    (`jni/{Android.mk,Application.mk,dc_wrapper.c}`), with **zero**
    `uniffi` dependency anywhere in its workspace (VERIFIED,
    `android-research-sync.md` §5) — a real counter-example to UniFFI
    from arguably the most prominent Rust-core-on-Android-with-iroh app
    that exists, not a confirmation of it, which is exactly why plan 36
    doesn't switch its whole boundary to hand-written JNI, only the path
    where per-call overhead is load-bearing. A1's gate includes a
    microbenchmark of per-call overhead (UniFFI/JNA vs. hand-written JNI)
    on the hot path specifically, to confirm the split is worth its added
    build complexity before `frontend-saf`/A2 commits to it.
18. **Conflict copies in mirror folders use Syncthing's naming
    convention**: `<stem>.sync-conflict-<YYYYMMDD-HHMMSS>-<short node
    id>.<ext>` — VERIFIED, `Catfriend1/syncthing-android`'s
    `util/Util.java` conflict-file regex (`android-research-sync.md` §1),
    a de facto standard format some target users will already recognize.
    A notification plus the plan-33 UI's conflict list (UX section below)
    surfaces them, since SAF has no in-band conflict hook: Nextcloud's own
    `DocumentsProvider` carries an unimplemented `// TODO show a conflict
    notification with a pending intent` at its `openDocument` call site
    (VERIFIED, `android-research-sync.md` §4) — independent confirmation
    from a shipping app that SAF genuinely has no better hook to punt to.
19. **Distribution priority: F-Droid (reproducible build) and a signed
    direct APK, from A7's first release, are the primary channels; Play is
    secondary.** The `full` flavor (F-Droid + direct APK) is what ships
    first; the Play flavor (MediaStore-scoped mirror only, settled
    decision 4) follows once its data-safety/permissions declaration is
    ready. This is a direct, concrete precedent, not hypothetical policy
    risk: **the official `syncthing/syncthing-android`'s README states its
    own December 2024 discontinuation was caused by "a combination of
    Google making Play publishing something between hard and impossible
    and no active maintenance"** (VERIFIED, quoted under 15 words,
    `android-research-sync.md` §2) — a well-established, functionally
    comparable sync app abandoning Play specifically over publishing
    friction, within the last two years. The maintained
    `Catfriend1/syncthing-android` fork is the de facto successor and
    ships today with Android 14/15 support and a `specialUse` FGS
    (VERIFIED), reinforcing it — not the archived original — as the
    reference implementation throughout this plan. The `full` flavor's
    F-Droid build must avoid proprietary dependencies (no Play Services);
    the reproducible-build and no-proprietary-blob audit itself is A7's
    job, not repeated here.
20. **`platform::android` acquires a `WifiManager.MulticastLock` only
    while LAN discovery is active, and Android's network-change signal for
    `crates/net` comes from `ConnectivityManager.NetworkCallback` pushed
    into iroh, not from `netwatch`'s netlink monitor, which is a
    documented no-op on Android.** Both halves are VERIFIED directly
    against the vendored source this session (not just the research
    reports): `netwatch` 0.19.3 (the version this workspace actually locks,
    `Cargo.lock`) ships `src/netmon/android.rs`, whose entire
    `RouteMonitor` implementation is: *"Very sad monitor. Android doesn't
    allow us to do this"* — it holds no netlink socket and never sends a
    `NetworkMessage::Change` on Android; `netmon/actor.rs` additionally
    drops the background wall-clock-jump poll from 15s to 1h on Android
    (`cfg(any(target_os = "ios", target_os = "android"))`) specifically to
    avoid battery drain, "Sleep detection won't work this way there" (code
    comment). This confirms Android 11+'s block on unprivileged
    `NETLINK_ROUTE`/`RTM_GETLINK` binding (REPORTED via general knowledge
    this plan's earlier research flagged as breaking Go's `net.Interfaces`
    and classic Syncthing; the netwatch source itself doesn't attempt
    netlink on Android at all rather than attempting and failing, so it is
    consistent with, but does not by itself independently confirm, that
    specific kernel-level restriction — A0 still verifies the underlying
    claim via a web search/AOSP doc if it ever matters beyond explaining
    netwatch's own design). The fix is exactly what `iroh::Endpoint`
    already exposes for this: `Endpoint::network_change()` (VERIFIED,
    present identically in iroh 1.1.0 — the version this workspace locks
    — and 1.2.0, `src/endpoint.rs`), whose doc comment states verbatim:
    *"some systems like android do not expose this functionality to
    native code. Android does however provide this functionality to Java
    code. This function allows for notifying iroh of any potential
    network changes."* `platform::android`'s `LifecycleSource` therefore
    feeds a `NetworkChanged` event from `ConnectivityManager.NetworkCallback`
    into `Endpoint::network_change()` on every network transition (Wi-Fi
    ↔ cellular, address change, etc.) as its **primary** interface/route
    change signal on Android, not a supplement to a netwatch signal that
    doesn't exist on this platform. `WifiManager.MulticastLock` is
    acquired only while iroh's local (LAN/mDNS) discovery is actively
    running, mirroring Syncthing-Fork's own justification — VERIFIED,
    `SyncthingRunnable.java`: "Android 11 blocks local discovery if we did
    not acquire MulticastLock" — and released immediately after, since
    holding it drains battery by keeping the Wi-Fi radio from powering
    down for multicast traffic (VERIFIED via `developer.android.com`'s
    `MulticastLock` reference). Delta Chat's own iroh embedding
    (`chatmail/core`, `iroh = "0.35"`) was checked for an
    Android-specific `netwatch`/network-change workaround and found none
    in the non-submodule source this session's shallow clone reached
    (REPORTED-absence-of-evidence, `android-research-sync.md` §5); its
    Android-specific `ConnectivityManager` wiring lives in the
    `deltachat-android` git submodule this session did not fetch, so it
    remains an open comparison point, not a contradiction of the finding
    above, which was independently confirmed by reading `netwatch`'s and
    `iroh`'s own vendored source directly rather than by inference from
    Delta Chat.

## Architecture

```
                     one Android app process
 ┌───────────────────────────────────────────────────────────────────┐
 │  Kotlin/Android layer                                              │
 │  ┌───────────────┐ ┌────────────────┐ ┌───────────┐ ┌────────────┐ │
 │  │ Tauri mobile   │ │ DocumentsProvider│ │ Foreground │ │ Mirror    │ │
 │  │ UI (plan 33's  │ │ + ProxyFileDesc-│ │ service    │ │ WorkManager│ │
 │  │ Svelte SPA)    │ │ riptorCallback  │ │ (specialUse/│ │ content-uri│ │
 │  │                │ │ bridge          │ │ dataSync|  │ │ trigger +  │ │
 │  │                │ │                 │ │ shortService)│ │ generation │ │
 │  │                │ │                 │ │            │ │ diff       │ │
 │  └──────┬─────────┘ └───┬─────────┬───┘ └─────┬──────┘ └─────┬──────┘ │
 │         │ Tauri IPC     │ binder  │             │ service     │ file   │
 │         │               │ calls   │             │ lifecycle   │ events │
 │         │               │(query/  │             │             │        │
 │         │               │ mutate/ │             │             │        │
 │         │               │ control)│             │             │        │
 │  ┌──────▼───────────────▼─────┐   │             │             │        │
 │  │  crates/mobile (UniFFI):    │   │             │             │        │
 │  │  EngineHandle, ViewHandle,  │   │             │             │        │
 │  │  ControlHandle,             │   │             │             │        │
 │  │  SecretStoreCallback,       │   │             │             │        │
 │  │  LifecycleCallback — the    │   │             │             │        │
 │  │  boundary for control/      │   │             │             │        │
 │  │  lifecycle/secret calls     │   │             │             │        │
 │  │  ONLY (not the hot I/O      │   │             │             │        │
 │  │  path — see JNI arrow →)    │   │             │             │        │
 │  └──────────────┬──────────────┘   │             │             │        │
 └─────────────────┼────────────────────┼───────────┼─────────────┼────────┘
                    │ in-process Rust    │ hand-written JNI (direct
                    │ (UniFFI/JNA)       │ ByteBuffers, bypasses
                    │                    │ crates/mobile entirely —
                    │                    │ onRead/onWrite/onFsync only)
        ┌───────────▼────────────────────▼──────────────────────────┐
        │  crates/frontend-saf (implements Vfs-driving DocumentsProvider│
        │  + proxy-fd server, hot path via hand-written JNI) ──      │
        │  constellation-control InProcess transport (for control/   │
        │  query/mutate calls) ── mirror-folder driver (not a Vfs    │
        │  frontend; a Vfs client)                                    │
        └───────────────────────────┬──────────────────────────────┘
                                     │
        ┌───────────────────────────▼──────────────────────────────┐
        │  constellation-engine  (EngineProfile: mobile — ForwardOnly│
        │  leases, DialOnly/Off P2P, UnmeteredOnly uploads, bounded  │
        │  cache_budget)                                             │
        └───────────────────────────┬──────────────────────────────┘
                                     │
        ┌───────────────────────────▼──────────────────────────────┐
        │  constellation-platform :: android (dirs from Context,     │
        │  Keystore SecretStore via callback, LifecycleSource from   │
        │  ConnectivityManager/PowerManager/ProcessLifecycleOwner,   │
        │  no daemon/fork — the engine simply lives in-process)      │
        └─────────────────────────────────────────────────────────┘
```

- **`crates/frontend-saf`** is the `Vfs`-driving half. A `DocumentsProvider`
  callback (`queryChildDocuments`, `queryDocument`, `openDocument`, …) is a
  Binder call on a binder thread pool thread — not a tokio worker, so it is
  exactly the kind of caller `OpCtx::new`'s debug-assert (plan 31) expects,
  and reaches the engine through `crates/mobile`'s UniFFI `ControlHandle`
  like every other query/mutation. `openDocument` returns the
  `ParcelFileDescriptor` from
  `StorageManager.openProxyFileDescriptor(mode, callback, handler)`
  **immediately** — no cold `Vfs::open` fetch happens before the fd is
  handed back, matching DAVx5's proxy-fd behavior and the <10s client
  timeout Round-Sync/RCX's own field notes document (settled decision 15);
  the `ProxyFileDescriptorCallback`'s `onRead`/`onWrite`/`onFsync`/
  `onGetSize`/`onRelease` (VERIFIED method list, `developer.android.com`
  reference for `StorageManager`/`ProxyFileDescriptorCallback`; API level
  26) are invoked serialized on whatever `Handler`/`Looper` was passed to
  `openProxyFileDescriptor` — `crates/frontend-saf` runs its own dedicated
  `HandlerThread` per open file (bounded pool, sized like the FUSE/NFS/WinFsp
  worker pools in plans 34/35), guarded by a **per-file lock, not a global
  mutex** (settled decision 15, DAVx5 precedent), so these calls are never
  on Android's main thread, never serialize unrelated open files against
  each other, and can block on a deferred `Vfs::Responder` exactly like a
  FUSE worker blocking on `SyncHandle`. Unlike every other Kotlin↔Rust call
  in this plan, these five callback methods cross via **hand-written JNI**
  (the `jni` crate, direct `ByteBuffer`s), not `crates/mobile`'s UniFFI/JNA
  boundary (settled decision 17) — the diagram above shows this as a
  separate arrow bypassing `crates/mobile` entirely, since it is the one
  path called often and latency-sensitively enough that JNA's per-call
  marshalling is a real risk, not the general Rust↔Kotlin boundary. The
  Kotlin-side proxy-fd callback wrapper nulls out every field it holds in
  `onRelease()`, not just closing resources, to work around the AOSP
  `AppFuseMount` callback-registration leak on pre-2024-08 framework builds
  (settled decision 15).
- **The proxy-fd path has real limits**, which is why it is not offered as
  "as good as a mount": REPORTED (not confirmed against literal doc text
  this session — flagged for A0) that `mmap` does not work usefully against
  an `AppFuse`-backed proxy fd (the round trip through a userspace daemon
  per page fault defeats the point of a shared mapping); every read/write is
  a Binder-mediated userspace hop, not a kernel-resident FUSE channel, so
  small-I/O latency and max throughput are both expected to be worse than
  `frontend-fuse` on Linux. A0 measures both directly, including the
  proxy-fd-writes-vs-pipe+commit-on-release benchmark from settled
  decision 16.
- **The mirror-folder driver is a `Vfs` *client*, not a frontend.** It opens
  a normal `View` (like the control-plane's `ControlVfs` does) and:
  - subscribes to `FrontendEvents`-shaped invalidations for the mirrored
    subtrees (reusing the same `Invalidation` enum plan 31 defined, just
    consumed instead of translated to a kernel notify call);
  - on a cluster-side change, writes the file into the real
    MediaStore-governed directory via `MediaStore`'s insert/write APIs (so
    the Gallery/Music apps index it normally) or, in the `full` flavor only,
    for the "any folder the user configured for mirroring" case gated
    behind the permission from settled decision 4, a direct file write;
  - on a local change, detects it primarily via `MediaStore`
    generation-diff (`MediaStore.getGeneration`/`GENERATION_MODIFIED`)
    triggered by a `WorkManager` `addContentUriTrigger` worker that re-arms
    itself, supplemented by a live `ContentObserver` while the app/FGS is
    active and a periodic full reconcile as the deletion-safe safety net
    (settled decision 13) — `FileObserver` is used only for the `full`
    flavor's non-MediaStore-governed folders, pending A0's on-device
    inotify-under-FUSE check — then reads the local file and issues the
    corresponding `Vfs` write/create/rename/unlink ops against the engine,
    tagged as an **offline-epoch reintegration** write exactly like a
    desktop node reconnecting after being designated offline
    (`crates/cli/src/designation.rs`/`reintegrate.rs`'s existing machinery:
    the mirror folder *is*, conceptually, a permanently-offline-designated
    subtree that happens to reintegrate continuously instead of on
    reconnect-from-a-long-outage).
- **`platform::android`** (plan 31's `HostServices` bundle, Android arm):
  - `dirs`: resolved from `Context.getFilesDir()`/`getNoBackupFilesDir()`
    (app-private, sandboxed, survives app updates, excluded from
    Auto Backup for the E2E pin/keys) rather than any XDG-style path — there
    is no `$HOME` on Android;
  - `secrets`: `SecretStore` backed by Android Keystore
    (`AndroidKeyStore` provider, hardware-backed where the device supports
    it) reached through the `SecretStoreCallback` UniFFI interface (Rust
    calls out to Kotlin for every secret op — the engine never touches
    JNI/Keystore APIs directly, keeping the Android-specific crypto surface
    entirely in reviewable Kotlin);
  - `lifecycle`: a `LifecycleSource` fed by `ConnectivityManager`'s network
    callbacks (reachability, metered-vs-unmetered — feeding
    `EngineProfile.uploads = UnmeteredOnly`), `PowerManager`'s Doze/battery
    state, and `ProcessLifecycleOwner` (app-wide foreground/background,
    distinct from any one `Activity`'s lifecycle) — mapped onto the same
    `Foreground`/`Background`/`Suspending{deadline}`/`Resumed`/
    `NetworkChanged`/`LowPower` events plan 31 C8 defined generically. Every
    `NetworkChanged` event additionally calls `Endpoint::network_change()`
    (settled decision 20) — this is not optional on Android the way it is
    on desktop, since `netwatch`'s netlink-based route monitor is a
    documented no-op on this platform (VERIFIED, `netwatch` 0.19.3's own
    `src/netmon/android.rs`) and `ConnectivityManager` is the *only* signal
    `crates/net`'s Android arm has for an interface/route change. A
    `WifiManager.MulticastLock` is acquired for the duration of any active
    local (LAN/mDNS) peer discovery and released immediately after
    (settled decision 20);
  - `daemon`/`process`/`file_lock`/`fs`: mostly not applicable (no daemon to
    spawn, no other process to detect liveness of); `fs` primitives
    (preallocate, fsync strength) map onto ordinary Linux syscalls, since
    Android's kernel is Linux — the *filesystem underneath the app's private
    storage* behaves like ext4/f2fs on any Linux box, it is only the
    *shared* storage path that is walled off, which is exactly the
    distinction this plan's architecture is built around.

## Mirror folders: design detail

- **Which folders.** Every flavor offers a fixed, small default set of
  MediaStore-governed media collections matching Android's own conventions:
  `DCIM/Camera` (photos/video), `Pictures`, `Movies`, `Music` (or a
  configurable subset), `Download` (opt-in, since it is the folder every
  other app also dumps into — a noisy source of local-write events),
  reached exclusively through `READ_MEDIA_IMAGES`/`VIDEO`/`AUDIO` +
  `READ_MEDIA_VISUAL_USER_SELECTED` + `MANAGE_MEDIA` (settled decision 4).
  **Arbitrary non-media folders (`Documents`, any user-chosen path) are a
  `full`-flavor-only, explicitly-consented advanced setting**, gated behind
  `MANAGE_EXTERNAL_STORAGE` (settled decision 4) — the Play flavor's
  settings UI simply does not offer this option at all, not merely hides it
  behind a permission prompt.
- **Pin semantics.** Every file placed under a mirror folder is implicitly
  pinned (`crates/cli/src/pin.rs`'s `PinManager`) for as long as it stays
  mirrored — the whole point is that it is available offline without a
  network round trip, which is exactly what a pin already guarantees
  elsewhere in the product. Un-mirroring a folder demotes its pins the same
  way `pin.rs`'s "Follow" step demotes chunks that drop out of a still-live
  pinned subtree.
- **Local edits → `Vfs` writes with offline-epoch reintegration.** A photo
  app editing a file in `DCIM/Camera` writes to the real local file
  directly (MediaStore does not intermediate raw byte writes to an
  already-existing file the app has write access to). The mirror driver
  observes that write via the generation-diff/`WorkManager`
  content-uri-trigger mechanism (settled decision 13; `FileObserver` only
  for the `full` flavor's non-media folders) and replays it into the
  cluster through the engine's ordinary write path, under the
  reintegration machinery `crates/cli/src/reintegrate.rs`/plan 30's
  rollback-and-replay-by-rid already implements for any node that was
  offline and comes back: the mirror folder is simply "always a little bit
  offline, reintegrating continuously" rather than "offline for a long
  outage, reintegrating once."
- **Conflict policy.** Two writers touching the same mirrored file — this
  phone's local edit and a cluster peer's concurrent write — resolve exactly
  as any other Constellation conflict resolves today (a conflict copy is
  materialized, per plan 30's refused-replay handling that
  `reintegrate.rs`'s doc comment already describes: "refused replays
  materialized as conflict copies"). The mirror driver's only extra job is
  presenting that conflict copy back into the mirrored folder in a way the
  Gallery/Music app and the user can actually see: the conflict copy's
  filename follows Syncthing's `<stem>.sync-conflict-<YYYYMMDD-HHMMSS>-
  <short node id>.<ext>` convention (settled decision 18), surfaced in the
  plan-33 UI's notification list — see UX below.
- **Deletion safety.** A local delete inside a mirror folder does **not**
  propagate as an immediate cluster delete by default — it is staged (a
  short, user-configurable grace window) so that "I deleted a photo from the
  Gallery app by accident" does not fan out to every other node before the
  user notices. This mirrors how `crates/cli/src/prune.rs`'s policy-driven
  deletion (plan 32, out of scope to edit but referenced for the pattern)
  already treats deletion as a policy decision, not an instant fact.
- **MediaStore scan triggering.** After the mirror driver writes a new file
  or updates an existing one from a cluster change, it inserts/updates the
  corresponding `MediaStore` row (or calls
  `MediaScannerConnection.scanFile`) so the Gallery/Music app indexes it
  without waiting for Android's periodic media scan — otherwise a photo that
  arrived from another node would be invisible to the Gallery app until the
  next scan, defeating the whole point of mirroring.
- **Storage budget.** Mirror folders count against the same
  `EngineProfile.cache_budget` as everything else pinned on this node — a
  phone with 8 GB free does not get an unbounded camera-roll mirror; the
  admission control in `pin.rs` refuses (with a clear UI message) mirroring
  a folder whose footprint does not fit, exactly as it refuses an oversized
  manual pin today.

## Credentials, security and battery/data policy

- **Credentials**: settled decision 11 (per-device scoped/short-lived S3
  credentials via Keystore-backed storage) plus the E2E passphrase handling
  in settled decision 10. Biometric unlock is optional, opt-in per device,
  configured through `BiometricPrompt` gating the Keystore key that protects
  the cached E2E session key — never gating the S3 credentials themselves
  (those must be usable by background sync without a fingerprint prompt
  every time, or background sync simply stops working, which is worse for
  security than the marginal risk it would close).
- **Battery policy**: uploads default to unmetered networks only
  (`EngineProfile.uploads = UnmeteredOnly`), background P2P is `DialOnly`
  (never `Listen` — no inbound QUIC listener while backgrounded, since Doze
  will suspend the process's networking anyway and an unreachable listener
  is just wasted setup). If iroh's local (LAN/mDNS) discovery is enabled,
  it additionally needs a `WifiManager.MulticastLock`, acquired only while
  discovery is actively running and released immediately after (settled
  decision 20) — otherwise Android 11+ silently drops multicast/mDNS
  traffic while the radio would sleep, and LAN-only peers simply stop being
  found in the background. The mirror driver's local-write processing is
  debounced and batched (the `WorkManager` content-uri trigger's 5s/10s
  update/max delay from settled decision 13) rather than reacting to every
  single write immediately, to avoid waking radios/CPU for every individual
  photo in a burst-mode camera sequence. Run conditions (Wi-Fi-only /
  specific SSIDs, charging-only, battery-saver respect, time windows,
  metered handling) are combined the way Syncthing-Fork's
  `RunConditionMonitor` combines them — one decision function, several
  `BroadcastReceiver`/`ConnectivityManager` inputs, a single boolean plus a
  human-readable reason string — surfaced in the plan-33 Android settings
  panel (see UX below) and mapped onto `EngineProfile`/`LifecycleSource`.
- **Data policy**: the default upload/download quality settings distinguish
  Wi-Fi from cellular (`ConnectivityManager.NetworkCapabilities`'s metered
  flag), and large pins/mirrors warn before enabling over a metered
  connection. All of this is UI-surfaced per plan 33's settings screen,
  with an Android-specific settings panel added by this plan (see UX below).

## `platform::android` and the foreground-service strategy

- **Foreground service types and time limits** (Android 15/16 behavior).
  VERIFIED (`developer.android.com/about/versions/15/behavior-changes-15`):
  apps targeting Android 15 (API 35) or higher get a combined **6-hour
  budget per 24-hour period** across all of an app's `dataSync`-type
  foreground services (and, separately, the same 6-hour/24-hour budget for
  the new `mediaProcessing` type introduced in the same release); at the
  limit the system calls `Service.onTimeout(int, int)`, the service must
  `stopSelf()` within a few seconds or the process is killed with a fatal
  `RemoteServiceException`; bringing the app to the foreground resets the
  timer. Android 15 also blocks several foreground service types —
  including `dataSync` — from being *started* out of a `BOOT_COMPLETED`
  receiver (VERIFIED, same page).
  - Constellation's continuous background sync therefore cannot be an
    always-on `dataSync` foreground service, and this plan does not choose
    `dataSync` alone for it: **`specialUse` is the foreground-service type
    for continuous, user-enabled background sync** (settled decision 14),
    declared with a `PROPERTY_SPECIAL_USE_FGS_SUBTYPE` justification
    string drafted as *"Constellation is a continuous cluster-sync app. If
    the user explicitly enables it running permanently in the background,
    it needs to monitor filesystem changes and send or receive changes
    made on this device or connected devices."* — directly modeled on
    `Catfriend1/syncthing-android`'s own shipping justification text
    (VERIFIED, `android-research-sync.md` §1, manifest `<property
    android:name="android.app.PROPERTY_SPECIAL_USE_FGS_SUBTYPE" .../>`),
    chosen specifically because it carries no 6-hour/24-hour budget and is
    not blocked from `BOOT_COMPLETED` starts the way `dataSync` is
    (VERIFIED, same source). **`dataSync|shortService` (combined on one
    `<service>` declaration) is used for bounded "sync now"/upload-burst
    windows** — an explicit user-triggered sync, a just-detected reconnect
    — VERIFIED precedent, Immich mobile's backup service declares exactly
    this combination (`android-research-sync.md` §3,
    `android:foregroundServiceType="dataSync|shortService"`):
    `shortService` is hard-capped at **~3 minutes** from
    `startForeground()`, times out via the same `Service.onTimeout()` hook
    as `dataSync` but with **no 6-hour/24-hour budget** at all — failing to
    stop leads to an ANR rather than a `RemoteServiceException`. Where a
    burst genuinely needs longer than ~3 minutes (a cold pin fetch over a
    slow link), it falls through to `dataSync`'s budget-tracked path
    instead. **`WorkManager`** (periodic/expedited/long-running workers
    with `setForeground`) drives scheduled mirror reconciliation and the
    periodic full-reconcile safety net (settled decision 13), independent
    of either foreground-service type. **`connectedDevice` is not used**:
    its official use-case text (VERIFIED,
    `developer.android.com/develop/background-work/services/fg-service-types`)
    is scoped to *"interactions with external devices that require a
    Bluetooth, NFC, IR, USB, or network connection"* — ambiguous at best
    for general P2P-over-QUIC-on-the-internet, which reads as meant for
    companion-device pairings, not a filesystem-sync cluster; `specialUse`
    is the safer default for the P2P-while-foregrounded case too, and A0
    does not need to re-litigate `connectedDevice` as a live option.
  - **A0 open question, not yet answered by any research pass**: while
    another app holds an open proxy fd (an active read/write against
    `frontend-saf`) or a bound `ServiceConnection`/provider connection into
    this app's process, does that binding itself keep the process at an
    elevated importance (per Android's process-priority model) such that
    active file serving needs **no** foreground service at all for the
    duration of that binding? If official documentation settles this, A0
    cites it directly (VERIFIED); if not, A0 measures it on-device by
    opening a file from another app and observing this app's process state
    under `adb shell dumpsys activity processes` while backgrounded. This
    does not change the `specialUse`/`dataSync|shortService`/`WorkManager`
    split above regardless of the answer — it only affects whether serving
    an already-open file needs its own FGS on top of whichever FGS (if any)
    is already running for sync.
- **No daemon, no fork.** Unlike every other platform this plan set covers,
  `platform::android`'s `daemon` seam is simply unimplemented/moot: there is
  one process (the app), the engine runs inside it, and "starting the
  daemon" from the plan-33 UI's perspective is "the app is running" — no
  discovery, no readiness-pipe protocol, no `posix_spawn`/`CreateProcess`
  equivalent.
- **Lifecycle bridge**: covered under Architecture above.

## Semantic differences the SAF frontend and mirror folders introduce

This table is the contract the parity file (A6) encodes.

| Behaviour | Linux FUSE (reference) | Android SAF frontend | Android mirror folders |
|---|---|---|---|
| Mountability | A kernel mount, reachable by any process via a path | Not a mount: reachable only by SAF-aware apps (Files app, document pickers, apps using `ACTION_OPEN_DOCUMENT`) | Real files, reachable by any app, but only inside a handful of MediaStore-governed directories |
| Entry/attribute caching, push invalidation | 1 s TTL + push `inval_entry`/`inval_inode` | `ContentResolver.notifyChange` on the child-documents URI (`FrontendCaps.push_inval = Attr`), per-touched-parent discipline (settled decision 15); no per-byte-range data invalidation concept in SAF | MediaStore generation-diff via `WorkManager` content-uri trigger, primary; live `ContentObserver` while active; periodic reconcile as safety net (settled decision 13); no TTL concept, event-driven only |
| Close-to-open / flush fence | `flush` per `close(2)` | `ParcelFileDescriptor` close via `onRelease` | Ordinary local `close(2)` — the Linux kernel underneath the app sandbox behaves normally; the *reintegration* into the cluster is what is asynchronous, not the local write |
| fsync | `fsync` | `onFsync` callback | Ordinary local `fsync` on the mirror file; durability into the cluster is a separate, later reintegration commit |
| Cross-node advisory locks | `getlk`/`setlk` → Constellation's lock service | **None.** `ClusterLocks` cap absent — SAF exposes no lock concept to callers at all | **None** — two apps editing the same mirrored file locally have no cross-app coordination beyond whatever the local filesystem's own semantics give them, same as any two apps sharing a folder on Android today |
| User xattrs, scratch/prune markers | `user.*` in-band | Control-API only; `Xattr`/`VirtualXattr` caps absent | Control-API only |
| fallocate / punch hole / SEEK_HOLE | Supported | Not exposed by `ProxyFileDescriptorCallback`; caps absent | Ordinary local filesystem behavior for the mirror file itself, irrelevant to the cluster path |
| Hard links, special files | Supported | Not representable through SAF's document model; caps absent | Not attempted — mirror folders only ever contain regular files |
| mmap | Supported via page cache | REPORTED not usefully supported on an `AppFuse`-backed proxy fd (A0 measures directly) | Ordinary local mmap on the mirror file, since it is a real file on a real local filesystem |
| Open-then-unlinked files | FUSE keeps the inode | `DeleteOnClose`-shaped: SAF has no silly-rename concept | Ordinary local unlink semantics |
| Permission checks | Kernel `DefaultPermissions` + our checks | The Android permission model (whichever app or system UI is driving the SAF call) substitutes for POSIX uid/gid entirely; `Caller` on this frontend carries the calling app's identity where available, not a POSIX uid | Governed by Android's scoped-storage/MediaStore permission rules, not POSIX permissions |
| Case sensitivity | Sensitive | Sensitive (Constellation stores bytes; SAF does not fold case) | Sensitive locally (Android's local filesystems are ext4/f2fs, case-sensitive) |
| Local-write notification | n/a (this is the kernel FUSE channel itself) | n/a | **Primary mechanism is `MediaStore` generation-diff (`getGeneration`/`GENERATION_MODIFIED`, API 30+) triggered by a `WorkManager` `addContentUriTrigger` worker that re-arms itself, VERIFIED as Nextcloud's current production design** (settled decision 13, `android-research-sync.md` §4) — not `FileObserver`, which this plan demotes to `full`-flavor-only non-media folders. **REPORTED, not VERIFIED this session, and A0 still settles it for that narrower `full`-flavor case**: whether `FileObserver`/inotify events reliably fire for writes made by *other apps* into `/storage/emulated` paths on Android 11+, where the tree is itself FUSE-mounted by MediaProvider. One secondary source found this session states "Android's FileObserver mechanism bypasses FUSE" (implying events *do* fire); this is not corroborated by an AOSP/developer.android.com page and must not be trusted without an on-device test. If it does not hold for those non-media folders, the mirror driver falls back to the same periodic-reconciliation safety net already primary for media folders, at coarser latency — a pre-agreed fallback, not a blocker (see A0). Deletions are never inferred from absence during partial media access; they are detected by generation diff plus reconcile, in both flavors. |
| Daemon killed / process killed | `ENOTCONN` until `fusermount3 -uz` | The `DocumentsProvider`'s process death is handled by Android restarting the provider on next access; open proxy fds from a killed process simply fail read/write with an I/O error to the caller, same as any Binder death | n/a (no separate daemon) |

Mixed Linux/macOS/Windows/Android clusters are in scope for reads and writes
at the storage layer (S3 + journal are OS-agnostic once plan 31 C1 lands).
This plan adds no uid/gid concept for Android at all — Android has none that
maps onto POSIX identity in a meaningful way, so files this phone creates
carry a fixed, documented synthetic uid/gid (configurable per view, same
shape as Windows's optional `windows_owner`, defaulting to `nobody`-shaped
values rather than guessing).

## UX specifics in the plan-33 SPA

Building on plan 33's screens 1–14: this plan adds an **Android** settings
sub-panel (screen 13, Settings) covering: which folders are mirrored (the
media-collection picker in every flavor; an additional arbitrary-folder
picker, gated behind the `MANAGE_EXTERNAL_STORAGE` prompt, shown only in
the `full` flavor build — settled decision 4) and their storage budget; the
run-condition controls (Wi-Fi/SSID, charging, battery saver, time windows,
metered handling — settled decision 14/Syncthing-Fork's
`RunConditionMonitor` model) and metered/unmetered data policy; biometric
unlock toggle for the E2E session key; the rooted-FUSE-mode toggle (visible
only when root is detected, clearly marked unsupported/best-effort); and
the mirror-folder conflict list (surfaced through the same notification
mechanism as plan 33's screen on notifications — errors, reintegration/
conflicts, quota thresholds, lease fencing — with mirror-folder conflict
copies, named per settled decision 18's convention, added as one more
notification kind). Beyond the settings panel:

- **System Files app integration**: the SAF `DocumentsProvider`'s root shows
  up automatically in the Files app and any `ACTION_OPEN_DOCUMENT`/
  `ACTION_OPEN_DOCUMENT_TREE` picker once the provider is registered — no
  extra UI work in the Tauri SPA is needed for this path to exist, only for
  it to be *discoverable* (a "browse in Files app" button/intent from the
  in-app Browser screen, screen 4).
- **Share-sheet**: `ACTION_SEND`/`ACTION_SEND_MULTIPLE` targeting a
  Constellation path, so "share to Constellation" appears in any app's share
  sheet — a small additional intent filter on a thin Kotlin
  `Activity`/`BroadcastReceiver`, translating the shared content URI into a
  `Vfs` write through `crates/mobile`.
- **Notifications**: the FGS's own persistent notification (required by
  Android for any foreground service, shown while syncing) is distinct
  from plan 33's alert notifications (errors/conflicts/quota); both surface
  through the same Android `NotificationChannel` infrastructure, on
  separate channels so a user can mute alerts without losing the
  required FGS notification (or vice versa, within what Android permits).

## Testing

- **Conformance kit cross-compiled and run on-device.** Plan 31's
  `vfs::conformance` suite (kernel-free, drives `Vfs` directly against the
  `model` oracle) is cross-compiled with `cargo-ndk` for
  `aarch64-linux-android` (and `x86_64-linux-android` for the emulator lane)
  as an ordinary test binary, pushed to the emulator/device with `adb push`,
  and run via `adb shell`. This needs no `DocumentsProvider`, no Binder, no
  app install — it is the same kind of "run the reference frontend
  conformance battery" the desktop ports already do, just executed through
  `adb` instead of `cargo test` directly.
- **JVM unit tests** (JUnit, Robolectric where a real `Context` isn't
  needed) for the Kotlin glue: document-ID↔`Ino` mapping helpers, the
  mirror driver's conflict-naming logic, notification-channel wiring — run
  under Gradle, no emulator required.
- **Instrumented tests** (`androidTest`, running on the emulator or a real
  device) for the `DocumentsProvider` itself (`ProviderTestRule` or an
  equivalent Espresso/`UiAutomator`-driven flow: create/read/write/delete/
  rename through the real content-resolver path) and for the proxy-fd
  bridge (open a document, read/write through the returned
  `ParcelFileDescriptor`, verify `onFsync`/`onRelease` ordering).
- **Mirror tests**: instrumented tests that write into a test mirror
  folder, assert the corresponding `Vfs` write landed (against an
  in-process `Engine` backed by `object_store::memory::InMemory` and
  `Meta::open_in_memory()`, the same pattern every other crate's unit tests
  use), and the reverse (a cluster-side change lands as a real file plus a
  MediaStore row).
- **Harness lane `android-saf`**: a *subset* of the harness scenario
  catalog — only the scenarios whose required `Cap`s are all present on
  this frontend (per settled decision 6's `FrontendCaps`, the harness
  derives `Cap` from it exactly as plan 31 C6 specifies) — driven through
  `crates/frontend-saf`'s conformance-kit binary running on-device, invoked
  from `harness run --frontend saf` on the CI host over `adb`, not by
  running the full `harness` binary itself on-device (the harness's process
  orchestration, toxiproxy, etc. stay on the host; only the frontend under
  test runs on the emulator).
- **A mixed lane**: a Linux node (the CI host, `linux-fuse`) plus the
  Android emulator node, sharing one S3-shaped backend: a host-run
  `versitygw` (plan 31 C6's process backend) reachable from the emulator at
  its host-loopback alias `10.0.2.2` (the standard Android emulator NAT
  redirect to the host's `localhost`; this is emulator-only — a real device
  cannot reach a host's loopback this way, so the mixed lane runs on the
  emulator, not on real-device CI). This proves a phone and a Linux node
  converge on the same filesystem.
- **Parity entries** — additions to `tests/platform-parity.toml` (plan 31
  C6's `<os>-<frontend>` convention; lane name `android-saf`):

  ```toml
  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "ClusterLocks"
  reason = "SAF exposes no lock concept to DocumentsProvider callers; locks never reach the engine from this frontend"

  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "Xattr"
  reason = "SAF has no xattr concept; scratch/prune markers are control-API only, same posture as linux-nfs and windows-winfsp"

  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "Fallocate"
  reason = "not exposed by ProxyFileDescriptorCallback"

  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "SeekHole"
  reason = "not exposed by ProxyFileDescriptorCallback"

  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "HardLinks"
  reason = "SAF's document model has no hard-link concept"

  [[expect]]
  scenario = "*"
  lanes = ["android-saf"]
  outcome = "skipped"
  cap = "FuseAbort"
  reason = "no /sys/fs/fuse equivalent; the frontend runs in-process with the engine, not as a separate mountable daemon"

  [[expect]]
  scenario = "mmap_*"
  lanes = ["android-saf"]
  outcome = "skipped"
  reason = "AppFuse-backed proxy fds do not support a useful mmap; confirmed in A0"
  ```

  `tests/parity.py` (plan 31 C6) needs no code change for this — one more
  lane name the `<os>-<frontend>` convention already covers.

## CI

An Android emulator runs on GitHub-hosted `ubuntu-latest` with KVM
acceleration via `reactivecircus/android-emulator-runner` (VERIFIED, its own
README documents the `api-level`/`target`/`arch` inputs and hardware
acceleration on Linux runners; VERIFIED separately, GitHub's own changelog:
"Hardware accelerated Android virtualization now available" on
Linux/Windows larger and standard hosted runners since the 2023/2024
changelog posts). REPORTED (a `reactivecircus/android-emulator-runner`
GitHub issue thread, "Automatically support KVM on Linux") that a
`udev`-rule step is sometimes still needed to grant the runner's user access
to `/dev/kvm` on `ubuntu-latest`; A0 confirms whether this repo's runners
need it or already have it (GitHub's own standard `ubuntu-latest` hosted
runners are documented to have nested virtualization/KVM enabled by
default as of the changelog above, but a private-repo minute-billing
runner's exact image can differ, so A0 checks rather than assumes).

```yaml
  android:
    name: Android build + unit tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: "aarch64-linux-android,x86_64-linux-android"
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "21" }
      - uses: android-actions/setup-android@v3
      - run: sdkmanager --install "ndk;27.2.12479018"   # r27 LTS; A0 confirms this is the pinned version
      - run: cargo install cargo-ndk
      - run: cargo ndk -t arm64-v8a -t x86_64 -o crates/mobile/android/app/src/main/jniLibs build --release -p constellation-mobile
      - run: cd crates/mobile/android && ./gradlew testDebugUnitTest
      - run: cd crates/mobile/android && ./gradlew assembleDebug

  android-cross-check:
    # Cheap Linux-side guard: library crates type-check for both Android
    # ABIs without needing the emulator or a JVM toolchain at all.
    name: Android library type-check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: "aarch64-linux-android,x86_64-linux-android"
      - uses: Swatinem/rust-cache@v2
      - run: cargo install cargo-ndk
      - run: |
          cargo ndk -t arm64-v8a check --workspace --exclude constellation \
            --exclude constellation-frontend-fuse --exclude constellation-frontend-nfs \
            --exclude constellation-frontend-winfsp --exclude winfsp-sys

  android-emulator:
    name: Android emulator matrix (conformance + instrumented)
    needs: android
    strategy:
      fail-fast: false
      matrix:
        api-level: [29, 34, 35, 36]   # 36 dropped from the matrix if A0 finds no system image published yet
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Enable KVM
        run: |
          echo 'KERNEL=="kvm", GROUP="kvm", MODE="0666", OPTIONS+="static_node=kvm"' | sudo tee /etc/udev/rules.d/99-kvm4all.rules
          sudo udevadm control --reload-rules
          sudo udevadm trigger --name-match=kvm
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: x86_64-linux-android }
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "21" }
      - run: cargo install cargo-ndk
      - run: cargo ndk -t x86_64 build --release -p constellation-vfs --features conformance-bin
      - uses: reactivecircus/android-emulator-runner@v2
        with:
          api-level: ${{ matrix.api-level }}
          target: google_apis
          arch: x86_64
          script: |
            adb push target/x86_64-linux-android/release/vfs-conformance /data/local/tmp/
            adb shell chmod 755 /data/local/tmp/vfs-conformance
            adb shell /data/local/tmp/vfs-conformance --json /data/local/tmp/results.json
            adb pull /data/local/tmp/results.json conformance-api${{ matrix.api-level }}.json
            cd crates/mobile/android && ./gradlew connectedDebugAndroidTest
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: android-emulator-api${{ matrix.api-level }}
          path: |
            conformance-api${{ matrix.api-level }}.json
            crates/mobile/android/app/build/reports/androidTests/**

  android-mixed-lane:
    name: Mixed Linux node + Android emulator node (shared S3 via 10.0.2.2)
    needs: android
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Enable KVM
        run: |
          echo 'KERNEL=="kvm", GROUP="kvm", MODE="0666", OPTIONS+="static_node=kvm"' | sudo tee /etc/udev/rules.d/99-kvm4all.rules
          sudo udevadm control --reload-rules
          sudo udevadm trigger --name-match=kvm
      - uses: dtolnay/rust-toolchain@stable
        with: { targets: x86_64-linux-android }
      - uses: Swatinem/rust-cache@v2
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "21" }
      - run: bash tests/ci/install-native-s3.sh   # pinned versitygw + toxiproxy, host-side
      - run: |
          target/release/versitygw posix /tmp/mixed-bucket --port 7070 &
          echo "S3 host ready on 127.0.0.1:7070, reachable from the emulator as 10.0.2.2:7070"
      - run: cargo build --release -p constellation --features linux-fuse-frontend
      - run: cargo install cargo-ndk
      - run: cargo ndk -t x86_64 build --release -p constellation-mobile
      - uses: reactivecircus/android-emulator-runner@v2
        with:
          api-level: 34
          target: google_apis
          arch: x86_64
          script: |
            target/release/harness run --frontend fuse --s3-endpoint http://127.0.0.1:7070 mixed-android-linux &
            adb shell am instrument -w -e s3_endpoint http://10.0.2.2:7070 \
              -e class com.constellation.mobile.MixedLaneTest \
              com.constellation.mobile.test/androidx.test.runner.AndroidJUnitRunner
            wait
```

## Packaging

- **F-Droid (reproducible build) and a signed direct APK are the primary,
  first-shipped channels; Play is secondary** (settled decision 19). Two
  Gradle flavors, `play` and `full` (replacing this plan's earlier
  `safOnly`/`full` framing — every flavor now ships both the SAF frontend
  and a mirror agent, settled decision 4): the `play` flavor's manifest
  declares only `READ_MEDIA_IMAGES`/`VIDEO`/`AUDIO` +
  `READ_MEDIA_VISUAL_USER_SELECTED` + `MANAGE_MEDIA` and **never**
  `MANAGE_EXTERNAL_STORAGE`; the `full` flavor additionally declares
  `MANAGE_EXTERNAL_STORAGE`, with the Play Console-style permissions
  justification text drafted under "backup and restore" / "document
  management" (VERIFIED categories per Play Console Help, quoted under
  Decision above) kept on file even though the `full` flavor is not
  distributed through Play — F-Droid's own review reads manifest and
  justification too. Build flavors in Gradle, not a runtime toggle, so the
  manifest itself (and therefore what each channel's review sees) differs
  per flavor. **A7 ships the `full` flavor's F-Droid build and signed APK
  first**; the `play` flavor's Play Console submission follows once its
  data-safety/permissions declaration is ready, not gating the first
  release.
- **Signing secrets**: `ANDROID_SIGNING_*` (keystore, key alias, passwords)
  as repo secrets, mirroring plans 34/35's "sign if secrets exist, otherwise
  produce an unsigned/debug-signed artifact" posture — an unsigned APK is
  still installable via sideload with a warning, same spirit as an
  unsigned Windows binary triggering SmartScreen.
- **Play vs F-Droid/sideload considerations**: this priority order is not
  precautionary hedging — it is a response to a concrete, recent
  precedent. The **official `syncthing/syncthing-android`** (a
  functionally comparable, well-established sync app) archived its repo
  in December 2024, its own README stating the reason as *"a combination
  of Google making Play publishing something between hard and impossible
  and no active maintenance"* (VERIFIED, quoted under 15 words,
  `android-research-sync.md` §2); its maintained fork,
  `Catfriend1/syncthing-android`, is the one still shipping on F-Droid/
  sideload today. Play's `MANAGE_EXTERNAL_STORAGE` review specifically is
  REPORTED to be slower/stricter than ordinary review (not independently
  verified this session as to typical turnaround), which only affects the
  `full` flavor's *Play* listing if one is ever pursued — the `play`
  flavor itself declares no such permission and carries none of that
  risk. F-Droid's own reproducible-build and no-proprietary-blob
  requirements (UniFFI/JNA and the Rust toolchain are all open source) are
  audited as part of A7, not deferred past it, given this precedent raises
  the priority of getting that audit done alongside the first release
  rather than as a following-up afterthought.
- **Rooted-FUSE-mode artifact**: packaged as a separate optional download
  (not through Play, which would reject an app that shells out to `su`,
  and not through F-Droid either, for the same reason), a plain zip of the
  `aarch64-unknown-linux-musl` static binary plus the Kotlin `su`-wrapper
  APK, clearly marked unsupported/best-effort.

## Milestones

Each milestone ends with the CONVENTIONS gates green on Linux **and** the
Android lanes that exist at that point green. Nothing here changes Linux
FUSE behaviour; the only shared surfaces this plan touches are plan 31's
`EngineProfile`/`LifecycleSource` (already generic) and
`tests/platform-parity.toml` (additive rows only).

### A0 — Spike on a real device/emulator: go/no-go for the SAF architecture (timebox: 4 days)

**What to build.** A throwaway example outside the product,
`crates/frontend-saf/examples/probe/` — a minimal Kotlin app with:

- a bare `DocumentsProvider` backed by an in-memory `Vfs::mock::MockVfs`
  (plan 31), proving the Binder-thread/`Vfs`-call shape works end to end;
- `StorageManager.openProxyFileDescriptor` against a synthetic large file,
  measured for sequential read/write throughput and small-I/O latency in
  both directions (settled decision 16's proxy-fd-write-vs-pipe benchmark),
  and an explicit test of whether `mmap(MAP_SHARED)` against the returned
  fd produces coherent reads of concurrent writes (settling the
  REPORTED-not-VERIFIED mmap question above);
- a `dataSync|shortService` foreground service that runs past both the
  ~3-minute `shortService` cap and the 6-hour `dataSync` mark on a test
  device/emulator with the clock advanced (or a `Service.onTimeout`
  override) to directly observe the shutdown behaviour of each type, plus
  a `specialUse` service to confirm the manifest `<property>` justification
  text (settled decision 14) is accepted as expected;
- a `MediaStore.getGeneration`/`GENERATION_MODIFIED` probe against a real
  volume, to confirm the API-30 floor and `getVersion`-reset behavior
  (settled decision 13) directly rather than trusting the reference-page
  text alone;
- `MediaStore.canManageMedia()`/`Settings.ACTION_REQUEST_MANAGE_MEDIA`
  exercised against a real device, to confirm the exact API-level floor
  (REPORTED as 31+ from Immich's code, settled decision 4) against the
  official `MediaStore` reference;
- a `FileObserver` registered on a real non-media (`full`-flavor-only)
  folder path, with a second, unrelated app instructed to write a file
  there, to settle the local-write-notification question for that narrow
  case directly, on at least two API levels (the oldest and newest in the
  intended support range) — media-folder local-change detection is no
  longer an open question this spike needs to answer, since settled
  decision 13 already fixes it to generation-diff/`WorkManager`, not
  `FileObserver`;
- a bound `ServiceConnection`/open-proxy-fd probe from a second app,
  observed via `adb shell dumpsys activity processes`, to answer the
  elevated-process-importance-while-bound question from the
  foreground-service-strategy section above.

Record a matrix of answers under "A0 results" below. Questions:

1. Real-world proxy-fd sequential throughput and 4 KiB random-I/O latency
   in both read and write directions, compared against a same-device
   `dd`/`fio` run on the app's own private storage (sanity baseline) —
   informational, not a hard gate, but it sets `FrontendCaps.max_io` and
   whether proxy-fd reads should be pre-fetched more aggressively than the
   desktop frontends' defaults, and directly answers settled decision 16's
   proxy-fd-writes-vs-pipe+commit-on-release choice.
2. Does `mmap(MAP_SHARED)` on the proxy fd work at all, and if so is it
   coherent? If not, is `MAP_PRIVATE` (copy-on-write, read-only-shaped)
   usable for anything Constellation needs?
3. Does a `dataSync`/`shortService` foreground service actually receive
   `onTimeout` at the documented mark (6 hours / ~3 minutes respectively),
   and does `stopSelf()` inside the timeout window cleanly avoid the
   `RemoteServiceException`/ANR? Is the `specialUse` justification-string
   property accepted without additional requirements beyond settled
   decision 14's draft text?
4. Does `MediaStore.getGeneration`/`GENERATION_MODIFIED` behave as the
   official reference describes (monotonic per volume, reset detected via
   `getVersion`) on real devices across the intended API range? Separately,
   for the `full` flavor's non-media folders only: do `FileObserver` events
   fire for a write made by a *different app* into a file under
   `/storage/emulated/0`, on both the oldest supported API level and the
   newest available? If not, the periodic-reconciliation safety net
   already primary for media folders is the pre-agreed fallback, at
   whatever latency it measures to.
5. Does `notifyChange` on a `buildChildDocumentsUri` URI reliably cause the
   system Files app / a document picker to re-query within a UI-visibly
   short time?
6. Does holding an active binding (an open proxy fd from another app, or a
   bound `ServiceConnection`) keep this app's process at an elevated
   importance such that active file serving needs no FGS of its own for
   the duration of that binding?
7. (Resolved: `MANAGE_MEDIA` and `canManageMedia()` are API 31+.) On API
   30, how many items can one `createWriteRequest` consent dialog cover in
   practice before the UX becomes unacceptable for a large camera roll?
8. Which `minSdk`/target OEM images still carry the pre-2024-08
   `AppFuseMount` proxy-fd-callback leak (`issuetracker.google.com/issues/
   208788568`), informing whether `frontend-saf`'s Kotlin wrapper needs
   DAVx5's field-nulling `onRelease()` workaround unconditionally or only
   below a given API level.
9. Does `cargo ndk -t arm64-v8a test` (or an on-device `adb`-pushed binary)
   actually run the plan-31 conformance kit unmodified, or does anything in
   `vfs::conformance` assume a capability Android's sandboxed `/data`
   partition does not have (e.g. `posix_fadvise`, certain `fallocate`
   flags) that needs a guard?
10. Does `reactivecircus/android-emulator-runner` need the extra `udev` KVM
    step on this repository's actual `ubuntu-latest` runners, or is
    `/dev/kvm` already accessible?

**Pre-agreed consequences.** No need to reopen the decision for any of
these:

- **Proxy-fd throughput is materially worse than expected (well under, say,
  50 MB/s sequential)**: lower `max_io`, add more aggressive read-ahead in
  `frontend-saf`, and document the gap prominently in the semantic-
  differences table and the UI's "why is this slower than my laptop"
  help text; this does not change the architecture.
- **Proxy-fd writes are unacceptably slow relative to pipe+commit-on-release**
  (settled decision 16): fall back to pipe+commit-on-release for `"w"` mode
  only, keeping proxy-fd for reads and for `"rw"`/`"rwt"` opens that are
  read-dominated; this is the pre-agreed fallback, not a stop-the-plan
  finding.
- **`mmap` does not work at all**: document it as unsupported outright
  (no `MAP_PRIVATE` fallback attempted), and note in the UI/docs that apps
  requiring `mmap`'d file access to a Constellation path should use the
  rooted-FUSE mode instead.
- **`FileObserver` does not fire for other apps' writes into the `full`
  flavor's non-media folders**: the periodic-reconciliation safety net
  already primary for media folders (settled decision 13) becomes the sole
  detection mechanism for those folders too, at whatever latency it
  measures to; this is the pre-agreed fallback, not a stop-the-plan
  finding, and does not touch media-folder detection at all (that path
  never depended on `FileObserver` in the first place).
- **`MediaStore.getGeneration`/`GENERATION_MODIFIED` behaves differently
  from the official reference** (e.g. does not reset cleanly on
  `getVersion` change, or is unreliable on some OEM images): fall back to
  the live `ContentObserver` plus periodic-reconcile combination as the
  primary signal instead of generation-diff, keeping the same
  `WorkManager`-triggered architecture; document the discrepancy, do not
  change the architecture.
- **`dataSync`/`shortService` FGS timeout behaviour differs from
  documented**: adjust the sync scheduler's budget tracking to match
  observed reality and file the discrepancy as a doc correction, not a
  design change.
- **The elevated-process-importance-while-bound question resolves "no"**
  (an open proxy fd/bound connection does not keep the process elevated):
  active file serving always runs inside whichever FGS (specialUse for a
  continuous session, dataSync|shortService for a burst) is already the
  plan's default; this changes nothing about the architecture, only
  confirms the FGS strategy was never optional.
- **`android-emulator-runner` needs the manual KVM `udev` step**: keep the
  step permanently in the CI YAML (already included above as a
  belt-and-braces default); this is not a blocking finding either way.
- **More than two of the above are materially worse than expected, or the
  proxy-fd path is unusable for real files (crashes, data corruption,
  unusable throughput)**: stop and report. There is no pre-agreed fallback
  frontend architecture beyond what the Decision table already rejected —
  a revised plan would have to re-open the SAF-vs-alternatives choice
  itself, most likely toward a narrower "mirror folders only, no SAF
  browsing" product for the first release.

### A1 — `crates/mobile`: the UniFFI boundary and mobile `EngineProfile` wiring

- UDL interface definitions for `EngineHandle`/`ViewHandle`/`ControlHandle`
  and the `SecretStoreCallback`/`LifecycleCallback` callback interfaces
  (settled decisions 2, 3).
- `platform::android` gains real implementations (not stubs) for `dirs`,
  `secrets` (routed through the callback), `lifecycle` (routed through the
  callback) — the other `HostServices` members (`daemon`, `process`,
  `file_lock`, `fs`) stay minimal/no-op per the Architecture section's note
  that most of them are moot on this platform.
- `EngineProfile` mobile defaults are set here (`p2p: DialOnly`,
  `leases: ForwardOnly`, `uploads: UnmeteredOnly`, `cache_budget` tuned
  against A0's device-storage findings).
- The `InProcess` control transport (plan 31 C5) is exercised end to end:
  a Kotlin call through `ControlHandle` reaches `Engine::control()`.
- `platform::android`'s `lifecycle` wiring feeds `NetworkChanged` events
  into `Endpoint::network_change()` (settled decision 20), since
  `netwatch`'s netlink-based monitor is a documented no-op on Android —
  exercised end to end here, not deferred to A3, since it only needs the
  `LifecycleCallback` plumbing this milestone already builds.
- The hand-written JNI hot-path bridge (settled decision 17) is scaffolded
  here alongside the UniFFI boundary, even though `frontend-saf` (A2) is
  its first real caller: a minimal `onRead`/`onWrite` round trip against a
  `ByteBuffer`, benchmarked head-to-head against an equivalent UniFFI/JNA
  call on the same data size. This microbenchmark is the gate for settled
  decision 17's split — if the JNI path's per-call overhead advantage over
  UniFFI/JNA is not measurable/material, A2 uses UniFFI for the hot path
  too and this milestone's scaffolding is deleted, not carried forward as
  dead code.
- **Tests**: `cargo ndk -t x86_64 test` runs `crates/mobile`'s unit tests
  on the emulator via `adb`; a JVM-side test instantiates the generated
  Kotlin bindings against a mock/in-memory engine config.
- **Gate**: `android-cross-check` (library type-check for both ABIs) green;
  `crates/mobile`'s own tests green on the emulator lane; the JNI-vs-UniFFI
  microbenchmark recorded (informs, but does not by itself block, A2).

### A2 — `crates/frontend-saf`: the `Vfs`-driving `DocumentsProvider` and proxy-fd bridge

- Implements every `Vfs` op needed by `DocumentsProvider`'s required methods
  (`queryRoots`, `queryChildDocuments`, `queryDocument`, `openDocument`) plus
  `createDocument`/`deleteDocument`/`renameDocument`/`FLAG_SUPPORTS_MOVE`'s
  `moveDocument`, as thin translation to `Vfs`, the same shape as every
  other frontend adapter in plans 34/35.
  Required-method list, `FLAG_SUPPORTS_*`/`Root` flags, `notifyChange`/
  `buildChildDocumentsUri`, and virtual-file (`FLAG_VIRTUAL_DOCUMENT`)
  support are all VERIFIED against `developer.android.com`'s
  "Create a custom document provider" guide.
- `isChildDocument` (REPORTED behaviour, not returned in the fetched guide
  excerpt — confirmed against the full reference page in this milestone,
  not assumed): implemented as an `Ino` ancestry walk against the metadata
  store, the same shape any POSIX "is this a descendant of that" check
  would take.
- `openDocument` wires to `StorageManager.openProxyFileDescriptor` with the
  dedicated per-open-file `HandlerThread` from the Architecture section,
  returning the fd **immediately** (settled decision 15) — no cold `Vfs`
  fetch before the fd is handed back; `ProxyFileDescriptorCallback`'s five
  methods translate 1:1 to `Vfs::read`/`write`/`fsync`/`getattr`(size)/
  `release`, called via the hand-written JNI bridge from A1 (settled
  decision 17) rather than UniFFI, using a `Responder` deferral so the
  `HandlerThread` is freed while a cold read/forwarded write is in flight
  (mirroring plan 31's "deferred replies" performance section). Per-file
  locking (not a global mutex, settled decision 15) guards concurrent
  access within one open file. `onWrite` calls into `Vfs::write` for real
  (settled decision 16, proxy-fd writes), with the pipe+commit-on-release
  fallback for `"w"` mode implemented but disabled unless A0's benchmark
  triggers the pre-agreed fallback. The Kotlin-side callback wrapper nulls
  every held reference in `onRelease()` (settled decision 15, DAVx5's
  `AppFuseMount`-leak workaround), gated by A0's minSdk/OEM findings on
  whether it's needed unconditionally.
- `queryChildDocuments` returns cached metadata with `EXTRA_LOADING=true`,
  refreshes in the background, then calls `notifyChange` on the
  child-documents URI when fresh data lands (settled decision 15,
  Nextcloud's pattern); every mutating operation (`create`/`delete`/
  `rename`/`move`) calls `notifyChange` on every touched parent, both
  source and target on a cross-directory move (settled decision 15,
  DAVx5/Nextcloud/Seafile's shared discipline). Binder-thread entry points
  carry a strict time budget and never block on network directly. An
  expired S3 credential or a locked E2E session key throws
  `AuthenticationRequiredException` (API 26+) with a `PendingIntent` into
  the plan-33 unlock screen (settled decision 15, DAVx5 precedent);
  `queryRoots` returns an empty root cursor while the session key is
  locked, rather than surfacing stale/inaccessible entries.
- `NamePolicy`/`XattrPolicy`/`FrontendCaps` land per settled decisions 6–7.
- Document-ID↔`Ino` mapping (settled decision 5) with its generation-tag
  reuse guard, unit-tested against a deliberately-reused `Ino` after
  unlink+GC.
- `NotifySink` implementation: `ContentResolver.notifyChange` on the
  affected `buildChildDocumentsUri`, coalesced the same way `kernel_inval`
  coalesces on Linux.
- **Tests**: protocol-level unit tests against `Vfs` backed by
  `Meta::open_in_memory()` + `InMemory` object store (no real
  `DocumentsProvider`/Binder involved) covering create/read/write/rename/
  delete, the document-ID reuse guard, and the errno-shaped `Vfs` error →
  SAF exception mapping. Instrumented tests (real `DocumentsProvider`
  through `ProviderTestRule`, real proxy fds) are added in A6, once the
  harness/CI plumbing exists to run them repeatably.
- **Gate**: `vfs::conformance` green on-device (A0's harness), unit tests
  green, CONVENTIONS gates unchanged on Linux.

### A3 — Kotlin app shell: `DocumentsProvider` registration, foreground service, Keystore bridge

- The Android Gradle project (`crates/mobile/android/`), with the
  `DocumentsProvider`, the `specialUse` continuous-sync foreground service
  (with the drafted `PROPERTY_SPECIAL_USE_FGS_SUBTYPE` justification text,
  settled decision 14), and the `dataSync|shortService` burst-sync
  foreground service all declared in the app manifest (or a Tauri Android
  plugin module's own manifest fragment, merged in by Gradle — the exact
  mechanism is an A3 decision, not assumed here, since this session did not
  find a primary-source confirmation of how Tauri 2's plugin manifest
  merging works in detail). The run-condition decision function
  (Wi-Fi/SSID, charging, battery saver, time windows, metered — settled
  decision 14, Syncthing-Fork's `RunConditionMonitor` shape) is built here
  too, since the plan-33 Android settings panel (A5 wiring) needs it.
- `SecretStoreCallback`'s Kotlin implementation: `AndroidKeyStore`-backed,
  with the biometric-gated variant from settled decision 10.
- `LifecycleCallback`'s Kotlin implementation: `ConnectivityManager`/
  `PowerManager`/`ProcessLifecycleOwner` wiring per the Architecture
  section, including the `NetworkChanged`-triggers-`Endpoint::network_change()`
  path (settled decision 20) and the `WifiManager.MulticastLock`
  acquire/release around active local discovery.
- WorkManager jobs for periodic/expedited sync, respecting the 6-hour
  `dataSync` FGS budget from A0's findings, and the `addContentUriTrigger`
  local-change-detection jobs from settled decision 13 (shared with A4's
  mirror driver, built here as the generic WorkManager scaffolding both
  consume).
- **Tests**: JVM unit tests for the Keystore/lifecycle glue (Robolectric
  where a real device isn't needed); an instrumented smoke test that the
  provider is discoverable via `Intent.ACTION_OPEN_DOCUMENT`.
- **Gate**: `android` PR job (build + unit tests, see CI) green.

### A4 — Mirror folders: the sync agent

- The generation-diff/`WorkManager`-content-uri-trigger local-change
  driver (settled decision 13, primary in every flavor for MediaStore-
  governed folders), the live `ContentObserver` supplement, and the
  periodic full-reconcile safety net (also the deletion-detection
  mechanism); `FileObserver` wired for the `full` flavor's non-media
  folders only, per A0's finding on inotify-under-FUSE reliability.
  MediaStore write/scan integration (`MediaScannerConnection.scanFile` or
  row insert/update), pin-on-mirror (settled decision 9 / Mirror folders
  section), offline-epoch reintegration wiring against
  `crates/cli/src/reintegrate.rs`'s existing rollback-and-replay-by-rid
  machinery, conflict-copy surfacing using the
  `<stem>.sync-conflict-<timestamp>-<node-id>.<ext>` naming convention
  (settled decision 18), and the staged-deletion grace window.
- The `MANAGE_EXTERNAL_STORAGE` request flow and its justification text
  (settled decision 4), gated behind the `full` Gradle flavor only
  (packaging, A7) — the `play` flavor never builds or exercises this code
  path at all, not merely hides it behind a runtime check.
- **Tests**: the mirror tests described under Testing above (in-process
  `Engine`, real local-filesystem writes to a test directory standing in
  for a mirror folder), plus a generation-diff-specific test asserting a
  write made while the app is backgrounded is picked up by the next
  triggered `WorkManager` run without a live `ContentObserver`.
- **Gate**: mirror tests green; CONVENTIONS gates unchanged on Linux.

### A5 — Credentials, battery/data policy, rooted FUSE packaging

- Per-device scoped S3 credential issuance (settled decision 11) — this
  milestone is the forcing function for plan 33 U1's per-device credential
  mechanism landing for real, not just specified; coordinate rather than
  duplicate if U1 has already shipped it by the time this milestone starts.
- Metered/unmetered upload policy enforcement, Doze/App-Standby-aware
  scheduling backoff.
- The rooted-FUSE-mode Kotlin `su`-wrapper (settled decision 12), packaged
  and smoke-tested manually on a rooted test device (not CI — no rooted
  emulator lane exists; this is a documented manual verification step, and
  the artifact is explicitly best-effort/unsupported).
- **Gate**: an on-device (or emulator, for the non-root parts) lifecycle
  test — app cold start → engine up → sync → backgrounded → `Suspending`
  handled cleanly (leases forwarded, P2P quiesced) → resumed.

### A6 — Testing and CI: conformance on-device, instrumented tests, parity, mixed lane

- All of the "Testing" section above landed as actual CI jobs: the
  `android`, `android-cross-check`, `android-emulator` (matrix), and
  `android-mixed-lane` jobs from the CI section.
- `tests/platform-parity.toml`'s `android-saf` rows (above) committed;
  `tests/parity.py` run against them with zero unexplained differences.
- Instrumented tests for the `DocumentsProvider` and proxy-fd bridge
  (deferred from A2) land here, running in the `android-emulator` matrix.
- **Gate**: the full CI section green on the matrix API levels that
  survived A0's system-image-availability check; `android-saf` parity
  report clean; Linux reference lane (pjdfstest 8798/8798, harness full
  matrix) unchanged.

### A7 — Packaging

- Gradle build flavors (`play`/`full`, settled decision 4), F-Droid
  reproducible build and signed direct APK for the `full` flavor (the
  first-shipped channel, settled decision 19), AAB for the `play` flavor's
  eventual Play Console submission (secondary, not gating this milestone),
  signing-secret wiring per the Packaging section.
- The F-Droid submission audit (reproducible build, no-proprietary-blob
  check) done here, not deferred past this plan, given the official
  `syncthing/syncthing-android` Play-discontinuation precedent (settled
  decision 19) raises its priority.
- Play Console data-safety and permissions-declaration content drafted for
  the `full` flavor's `MANAGE_EXTERNAL_STORAGE` request, kept on file for
  whenever the `play` flavor's own (permission-free) Play submission is
  pursued.
- `RELEASING.md` gains the Android artifacts' build/signing/verification
  steps, including the F-Droid reproducible-build verification procedure.
- **Gate**: `android`, `android-cross-check`, `android-emulator` and
  `android-mixed-lane` all green; a signed (or clearly-marked debug-signed)
  `full`-flavor APK (and F-Droid-buildable source) are produced by CI as
  release artifacts; the `play`-flavor AAB builds successfully even if its
  store submission is not part of this plan's Definition of Done.

## Risks

- **The proxy-fd path's real-world performance and `mmap` support are
  unverified until A0.** This is the single biggest unknown in the whole
  plan — everything downstream (whether the SAF frontend is a credible
  "browse and edit" experience at all, or a novelty) depends on it. A0 is
  timeboxed and gated precisely because of this.
- **`FileObserver` reliability on FUSE-mounted shared storage is REPORTED,
  not VERIFIED, and the two secondary signals found this session
  contradict each other in spirit** (one states FileObserver "bypasses"
  FUSE and should work; general engineering caution about inotify across a
  userspace FUSE boundary suggests it might not, or might be delayed).
  This risk is now scoped narrowly to the `full` flavor's non-media
  folders (settled decision 13) — media-folder detection no longer depends
  on `FileObserver` at all, having moved to `MediaStore` generation-diff
  via `WorkManager`, a VERIFIED shipping pattern (Nextcloud). Mitigation:
  the pre-agreed periodic-reconciliation fallback (A0), which does not
  depend on this question resolving favorably.
- **Android version fragmentation.** `openProxyFileDescriptor` needs API
  26+ (VERIFIED); scoped storage's FUSE-owned `/storage/emulated` is an
  Android 11+ behavior; `MediaStore` generation-diff needs API 30+
  (VERIFIED); `MediaStore.canManageMedia()`/`ACTION_REQUEST_MANAGE_MEDIA`
  is REPORTED as API 31+ (A0 confirms); `dataSync`'s 6-hour ceiling is an
  Android-15-targeting behavior. The plan's minimum supported API level is
  an A0/A1 decision informed by these floors, not fixed in this document.
- **Play policy risk on `MANAGE_EXTERNAL_STORAGE` is now confined to the
  `full` flavor's optional, secondary Play listing (if ever pursued),
  since the `play` flavor never declares this permission at all** (settled
  decision 4). Even so, All Files Access requests are reviewed individually
  and can be rejected or delayed (REPORTED, not independently verified as
  to typical turnaround this session) for whichever flavor ever carries it.
  Mitigation: distribution priority itself (settled decision 19) — F-Droid
  and a signed direct APK, not Play, are this plan's first-shipped
  channels, following the concrete precedent of the official
  `syncthing/syncthing-android`'s December 2024 discontinuation over
  exactly this kind of Play friction (VERIFIED, its own README). The `play`
  flavor, when it does ship, carries zero All-Files-Access risk by
  construction.
- **UniFFI/JNA and Tauri-Android-plugin integration details are REPORTED,
  general-knowledge patterns, not independently re-verified against this
  exact toolchain combination this session.** Mitigation: A0/A1/A3 build a
  real, if minimal, end-to-end example before any product code depends on
  the pattern.
- **A second, hand-written Rust↔Kotlin boundary (JNI, settled decision 17)
  alongside UniFFI adds review/maintenance surface a single-boundary
  design wouldn't have** — direct `ByteBuffer` handling across JNI is a
  classic source of use-after-free/lifetime bugs if done carelessly, and
  Delta Chat's own choice of hand-written JNI for its *entire* boundary
  (not just a hot path) shows this is a real, load-bearing engineering
  cost elsewhere in the ecosystem, not a trivial add-on. Mitigation: A1's
  microbenchmark gate means this cost is only paid if it's actually
  justified by measured per-call overhead; the JNI surface is deliberately
  kept to five methods (`onRead`/`onWrite`/`onFsync`/`onGetSize`/
  `onRelease`), not the whole engine API.
- **The AppFuseMount proxy-fd-callback memory leak
  (`issuetracker.google.com/issues/208788568`, fixed 2024-08-24) affects
  every open proxy fd on pre-fix framework builds** (settled decision 15)
  — without DAVx5's field-nulling `onRelease()` workaround, this would leak
  one callback object, and everything it references (including
  `Vfs`/engine handles via the JNI bridge), per file opened over the life
  of the app's AppFuse mount, on affected devices. Mitigation: the
  workaround is adopted unconditionally pending A0's minSdk/OEM-image
  findings on how narrowly it can be scoped.
- **`crates/net`'s Android network-change detection depends entirely on
  `ConnectivityManager.NetworkCallback` correctly reaching
  `Endpoint::network_change()` (settled decision 20), since `netwatch`
  provides no fallback signal on this platform at all** (VERIFIED,
  `netwatch` 0.19.3's `RouteMonitor` is a documented no-op on Android) — a
  missed or delayed Kotlin-side callback, a process death that drops the
  `NetworkCallback` registration, or an OEM battery-management quirk that
  suppresses the callback while backgrounded would leave iroh unaware of a
  real network change with no independent way to notice. Mitigation: the
  1-hour background wall-clock-jump poll `netwatch` already runs on
  Android (`netmon/actor.rs`, VERIFIED) is a coarse secondary signal, and
  `platform::android`'s `LifecycleSource` re-registers its
  `NetworkCallback` on every `Resumed` transition as a belt-and-braces
  reset, not just at cold start.
- **No rooted-emulator CI lane exists**, so the rooted-FUSE mode (settled
  decision 12) is verified manually, not continuously. Mitigation: it is
  explicitly the lowest-priority, "zero new engine code, packaging only"
  mode in this plan, and its breakage does not affect the SAF/mirror
  product.
- **Android NDK version drift.** VERIFIED (`developer.android.com/ndk/downloads/revision_history`):
  the most recent entries at research time are r29 (October 2025, standard
  release) and r27 (July 2024, the most recent LTS release). This plan pins
  the LTS release (r27) for CI reproducibility, matching plans 34/35's
  "pin a known-good toolchain" posture, and revisits the pin if r27 proves
  incompatible with a `compileSdk`/AGP requirement discovered in A0/A1.

## Definition of done

The CONVENTIONS gates, PLUS:

1. **Android PR lanes green**: `android`, `android-cross-check`.
2. **Nightly/scheduled matrix green**:
   - `android-emulator` across the surviving API-level matrix;
   - `android-mixed-lane`;
   - `parity` reports zero unexplained differences on `android-saf`;
   - a signed (or debug-signed) `full`-flavor APK, and an F-Droid-buildable
     source tree, produced by `android-package`; the `play`-flavor AAB
     builds successfully (its store submission is not required for DoD).
3. **Linux reference lane unchanged**: pjdfstest 8798/8798 with an empty
   baseline, and the harness full matrix passes.
4. **Docs updated**:
   - `PROGRESS.md` has a plan-36 section with the A0 results matrix;
   - `TESTING.md` covers the `android-saf` lane, the mixed lane, and the
     `--frontend saf` flag;
   - the README gets an Android quick-start note leading with F-Droid/
     direct-APK install, with Play noted as a secondary, follow-on channel;
   - an Android how-to guide exists: install (F-Droid/APK first), permissions
     explained (why SAF and MediaStore-scoped mirroring need no
     All-Files-Access permission in either flavor, and why only the `full`
     flavor's arbitrary-folder mirroring needs `MANAGE_EXTERNAL_STORAGE`),
     mirror-folder setup, conflict-copy naming, rooted-FUSE-mode caveat.
5. **Report**: the A0 matrix, the conformance-kit pass tally per API level,
   the parity summary table, and the mirror-test tally.

## A0 results

*(filled in by the executing model)*

## Sources checked out for this plan

| Source | Ref / date | Used for |
|---|---|---|
| `source.android.com/docs/core/storage/scoped` | fetched 2026-09-28 | MediaProvider-as-FUSE-handler for `/storage/emulated`, SELinux/`CAP_SYS_ADMIN` restriction on app-initiated mounts, app access-level tiers |
| `developer.android.com/reference/.../StorageManager#openProxyFileDescriptor` | fetched 2026-09-28 | API level 26 floor, method signature, `ProxyFileDescriptorCallback`'s five methods, Handler/Looper threading |
| `developer.android.com/guide/topics/providers/create-document-provider` | fetched 2026-09-28 | `DocumentsProvider` required methods, `FLAG_SUPPORTS_*`/`Root` flags, `notifyChange`/`buildChildDocumentsUri`, virtual-document flow |
| `developer.android.com/about/versions/15/behavior-changes-15` | fetched 2026-09-28 | `dataSync`/`mediaProcessing` 6-hour/24-hour foreground-service budget, `onTimeout`, `BOOT_COMPLETED` FGS-start restrictions |
| `support.google.com/googleplay` (Play Console Help, "Use of All files access permission") | via search, 2026-09-28 | Permitted-use categories for `MANAGE_EXTERNAL_STORAGE` (file manager, backup/restore, document management) |
| `developer.android.com/training/data-storage/shared/media` | via search, 2026-09-28 | `MediaStore.createWriteRequest`, API 30+, per-file consent, non-persistable grant |
| `developer.android.com/ndk/downloads/revision_history` | fetched 2026-09-28 | NDK r29 (Oct 2025, standard) and r27 (Jul 2024, LTS) as the two most recent relevant releases |
| `github.com/bbqsrc/cargo-ndk` | fetched 2026-09-28 | `ANDROID_NDK_HOME` detection, per-ABI target mapping, `CARGO_NDK_*` build-script env vars |
| `raw.githubusercontent.com/cberner/fuser/master/build.rs` | fetched 2026-09-28 | No Android-specific handling; falls through to a libfuse pkg-config probe and panics off Linux/macOS/BSD/Windows — confirms `fuser` is not viable for the SAF/mobile target and is only relevant to the rooted-FUSE mode's unmodified Linux binary |
| `mozilla.github.io/uniffi-rs` (Kotlin/Gradle integration guide) | via search, 2026-09-28 | UniFFI's Kotlin bindings target, Gradle integration shape |
| `v2.tauri.app` (mobile plugin development guide, Google Play distribution guide) | via search, 2026-09-28 | Tauri 2 Android plugin shape (Kotlin `Plugin`/`@TauriPlugin`), Android minimum SDK 24 default |
| `github.com/ReactiveCircus/android-emulator-runner` (README + issue #370) | via search, 2026-09-28 | Emulator-runner inputs; REPORTED `udev` KVM step sometimes needed on `ubuntu-latest` |
| `github.blog/changelog` (2023-02-23, 2024-04-02 posts) | via search, 2026-09-28 | Hardware-accelerated Android virtualization availability on GitHub-hosted Linux runners |
| XDA Forums, "Fusermount on android (rclone mount)" thread | via search, 2026-09-28 (REPORTED, community source) | Static-musl `fusermount`/FUSE-tooling precedent under root/Magisk; bionic's lack of full static-link support as the reason the `-musl` target is used instead of `-android` |
| this tree, commit `a945b05` | 2026-09-28 | `crates/cli/src/{pin,designation,epoch,reintegrate,forward,lease,e2e_pin}.rs`, `crates/store-s3/src/e2e.rs`, `crates/cli/src/coop/{exact,fresh}.rs`, `crates/net/src/{endpoint,identity}.rs` module doc-comments and file sizes; workspace `license = "MPL-2.0"` |
| `bitfireAT/davx5-ose` | `e644134`, cloned `--depth 1` into `/tmp/android-research/davx5-ose`, 2026-09-28 | Proxy-fd random-access read implementation, per-open-file `HandlerThread`, `AuthenticationRequiredException`, `AppFuseMount` leak workaround, per-mutation `notifyChange`, document-ID scheme (settled decisions 15, 16; Prior art) |
| `nextcloud/android` | `007fe67`, cloned into `/tmp/android-research/nextcloud-android` (sparse-checkout disabled to materialize `providers/`), 2026-09-28 | `EXTRA_LOADING`/async-reload `queryChildDocuments` pattern, `WorkManager Constraints.addContentUriTrigger` local-change detection (`BackgroundJobManagerImpl.kt`/`ContentObserverWork`), lock-screen root-gating, `MANAGE_EXTERNAL_STORAGE` declared for a broader arbitrary-folder job (settled decisions 13, 15; Prior art) |
| `termux/termux-app` | `8629e63`, `/tmp/android-research/termux-app`, 2026-09-28 | Negative-example `DocumentsProvider` (no `notifyChange`, path-as-document-ID) (Prior art) |
| `newhinton/Round-Sync` | `bda00f8`, `/tmp/android-research/Round-Sync`, 2026-09-28 | Separate-daemon architecture (negative example), "beta quality notes" (<10s client timeout, column-ordering caveat), open `NetworkOnMainThreadException` TODO (settled decision 15; Prior art) |
| `x0b/rcx` | `d98c4d6`, `/tmp/android-research/rcx`, 2026-09-28 | Near-identical fork of Round-Sync, used interchangeably for grep sweeps |
| `zhanghai/MaterialFiles` | `c9b29cb`, `/tmp/android-research/MaterialFiles`, 2026-09-28 | `java.nio.file.spi.FileSystemProvider` per-backend abstraction, independent validation of the `Vfs`-trait design (Prior art) |
| `haiwen/seadroid` | `9d6d490`, `/tmp/android-research/seadroid`, 2026-09-28 | Pipe/download-cache `DocumentsProvider` shape, `notifyChanged` discipline (Prior art) |
| `cryptomator/android` | `868730f`, `/tmp/android-research/cryptomator-android`, 2026-09-28 | Confirmed no `DocumentsProvider` usage (out of scope as a proxy-fd reference) |
| `Catfriend1/syncthing-android` (Syncthing-Fork) | `02a95858e5192139353cd1188e9f5679eef0dd97`, `/tmp/android-research/catfriend1-syncthing-android`, 2026-09-28 | `RunConditionMonitor` run-condition model, `specialUse` FGS + `PROPERTY_SPECIAL_USE_FGS_SUBTYPE` justification text, `WifiManager.MulticastLock`, `.sync-conflict-<timestamp>-<device>` naming, exec-a-`.so`-subprocess architecture (rejected as a model) (settled decisions 14, 18, 20; Prior art) |
| `syncthing/syncthing-android` (official, archived) | `64d2b8e362d45988dfc21dda3b0b494d7e8bb2c1`, `/tmp/android-research/official-syncthing-android`, 2026-09-28 | December 2024 Play-discontinuation precedent (settled decision 19) |
| `immich-app/immich`, `mobile/` | `76c239c34524209c3bb892d93ed1a6ee6b9a0076`, `/tmp/android-research/immich`, 2026-09-28 | Zero-`MANAGE_EXTERNAL_STORAGE` media-permission set (`READ_MEDIA_*`/`MANAGE_MEDIA`), `dataSync\|shortService` combined FGS declaration (settled decisions 4, 14) |
| `deltachat/deltachat-android` | `aaa26f6a9c2aa78b871dc52e82a12d00660177fc`, `/tmp/android-research/deltachat-android`, 2026-09-28 | Hand-written JNI + `ndk-build` embedding, zero `uniffi` dependency (settled decision 17; Prior art) |
| `chatmail/core` (deltachat-core-rust) | `1e36fb74bee804dd625f31270895b9be38024631`, `/tmp/android-research/deltachat-core-rust`, 2026-09-28 | iroh usage patterns (backup transfer, WebXDC realtime channels), `iroh = "0.35"`; checked (not found) for Android-specific `netwatch` workarounds (settled decision 20) |
| `developer.android.com/develop/background-work/services/fg-service-types` | fetched 2026-09-28 | `shortService` ~3-minute cap/no budget, `connectedDevice`'s Bluetooth/NFC/IR/USB/network-companion-device scoping (settled decision 14) |
| `developer.android.com/reference/android/net/wifi/WifiManager.MulticastLock` | fetched 2026-09-28 | `MulticastLock` battery-drain tradeoff, acquire-only-while-discovering guidance (settled decision 20) |
| `~/.cargo/registry/src/index.crates.io-*/netwatch-0.19.3/src/netmon/android.rs`, `netmon/actor.rs` | read directly this session, 2026-09-29 (the version this workspace's `Cargo.lock` actually locks) | VERIFIED: Android `RouteMonitor` is a no-op ("Very sad monitor. Android doesn't allow us to do this"), no netlink socket, background wall-clock poll dropped to 1h on Android (settled decision 20) |
| `~/.cargo/registry/src/index.crates.io-*/iroh-1.1.0/src/endpoint.rs` (the version this workspace's `Cargo.lock` locks; also checked identical in `iroh-1.2.0`) | read directly this session, 2026-09-29 | VERIFIED: `Endpoint::network_change()` public API and doc comment naming Android by name as the reason it exists (settled decision 20) |
| `developer.android.com/reference/android/provider/MediaStore` | via search, 2026-09-29 | VERIFIED: `GENERATION_MODIFIED`/`getGeneration()` API-30 floor, monotonic-per-volume semantics, `getVersion()`-reset guidance (settled decision 13) |
