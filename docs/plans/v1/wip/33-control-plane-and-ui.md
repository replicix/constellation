# Plan 33 — Control-plane security and the Constellation UI

Read `docs/plans/v1/CONVENTIONS.md` first. Spec context: `docs/explanation/DESIGN.md`
§10 (control plane). Depends on plan 31 (its control-protocol section
and milestones C1–C8, specifically C5 `constellation-control`); coordinates
with plan 32 (owned by another session — read-only here, see "Coordination
with plan 32" below); its remote-management milestone (U7) is deliberately
distinct from plan 23's remote support mode (also read-only here, see
"Distinction from plan 23" below). Plans 34 and 35 depend on 31 alone and
treat 33 as optional (the UI bundle for their installers). Plan 37
(Kubernetes CSI driver, Linux-only) depends on this plan's U1 alone
(roles, the allowlist, the audit log, and the service-principal grant kind
below) — it needs no screen, no Tauri app and no milestone past U1; the UI
milestones U2–U8 are irrelevant to it and it is not otherwise mentioned in
this plan beyond the service-principal and view/snapshot UI notes below.

Code read for this plan (verified 2026-09-28, commit a945b05): `crates/api/src/lib.rs`
(the `StatusSource` trait at :42, `dispatch` at :231, unix-socket `serve`/`handle`
at :334/:362, the line-delimited-JSON framing, the `unix_and_http_adapters_have_request_parity`
test at :738 that is today's protocol-parity guarantee), `crates/api/src/types.rs`
(the `Request`/`Response` enums at :7/:209, both internally tagged —
`#[serde(tag = "cmd", rename_all = "snake_case")]` and `#[serde(tag = "resp", …)]`
— and the pervasive `#[serde(default)]`/`skip_serializing_if` use across every
status struct), `crates/api/src/web.rs` (the axum adapter, the embedded
`webui/` assets via `rust-embed`, and the `guard_rebinding` DNS-rebinding
middleware at :110 — the module doc there states plainly that the server
"intentionally has no authentication because it binds only to `127.0.0.1`"),
`crates/api/webui/index.html` and `peers.html` (the existing vanilla-JS
control UI: a `call()` helper posting to `/api`, a 2-second poll loop, no
auth, no build step), `crates/api/Cargo.toml` (the `web` feature: axum 0.8,
rust-embed 8, mime_guess 2), `crates/cli/src/main.rs:140-160` (the `--web-ui`
mount flag, `CONSTELLATION_WEB_UI_PORT`, default port 8080 on bare `--web-ui`),
and `crates/net/src/endpoint.rs:958-970` plus `crates/net/src/peers.rs:794-817`
(the iroh endpoint and its ALPN dispatch — see Verification (g) below).

## Why

Two problems, one plan, because the fix to the first shapes the whole
design of the second.

1. **The control plane has no security boundary today.** `crates/api`'s
   unix socket accepts every request from every local user (no peer-cred
   check at all — `serve` in lib.rs:334 binds the socket and answers
   any connection), and the optional HTTP adapter binds `127.0.0.1` with
   the DNS-rebinding `Host`/`Origin` guard as its *only* protection —
   explicitly "no authentication" by the module's own doc comment. Any
   local process, and any browser tab that can be tricked into a same-origin
   request, can run `fsck --repair`, `leave`, `mount_add`, prune, or read
   any file through `ReadDir`/`Inspect`/download. Plan 31 replaces the
   transport (`constellation-control`, peer-cred `UnixSocket`, roles
   declared per method) but explicitly defers hardening the web adapter
   and building out a real principal/role/audit system to this plan.
2. **The UI needs full management coverage, not a read-only dashboard.**
   Today's embedded `webui/` is 21 KB of hand-rolled HTML/JS covering a
   fraction of the 37 control methods (dashboard, peers, a `ReadDir`
   browser, snapshot create/delete, doctor, logs — no filesystem registry,
   no mount management, no prune/GC/fsck runs, no quotas, no locks, no
   audit trail, no settings). Growing it by hand for every future control
   method (plan 32 alone adds a dozen) does not scale, has no test
   framework, and cannot become a real desktop/mobile app.
3. **Multiplatform.** Plans 34–36 add three more OS targets. Users need
   one UI that runs as a native app on desktop (Linux/macOS/Windows) and
   mobile (Android/iOS, plan 36), plus a headless mode for servers with no
   desktop session, without forking the UI once per OS.

## Verification ledger

Per the design brief's rule: mark every claim VERIFIED (checked against
this repo, an official doc page, or a cloned upstream repo) or REPORTED
(what a search engine's summary said, not independently re-derived). No
invented line numbers; everything cited above was read directly.

| # | Claim | Status | Evidence |
|---|---|---|---|
| a1 | Tauri 2 is the current stable major version; desktop runtime around 2.11.x as of mid-2026 | REPORTED | web search of `v2.tauri.app/release/` and `tauri.app/release/core/`; package pages for `tauri@2.0.0`, `tauri-cli`, `@tauri-apps/api` confirm the 2.x line is active and versioned independently per crate |
| a2 | Tauri 2 targets desktop Linux/macOS/Windows and mobile Android/iOS from one codebase | VERIFIED (doc) | `v2.tauri.app` overview page and `v2.tauri.app/start/prerequisites/`; mobile went stable 2024-10-02, before this plan's window |
| a3 | Tauri 2's IPC uses a capabilities/permissions system: capability files under `capabilities/`, JSON/JSON5/TOML for capabilities, TOML-only for permission definitions, and commands are denied unless a capability explicitly grants them (unlike Tauri 1) | VERIFIED (doc) | `v2.tauri.app/security/capabilities/`, `v2.tauri.app/security/permissions/`, `v2.tauri.app/reference/acl/capability/` |
| a4 | Tauri 2 supports a per-window Content-Security-Policy in `tauri.conf.json`, applied only when set, with automatic nonce/hash injection for bundled assets | VERIFIED (doc) | `v2.tauri.app/security/csp/` |
| a5 | `tray-icon` (crate `tauri-apps/tray-icon`) and the official notification plugin (`v2.tauri.app/plugin/notification/`) are both available for Tauri 2, the tray as a Cargo feature (`tray-icon`) plus a Rust/JS API, the notification plugin with a JS+Rust API and mobile action/attachment support | VERIFIED (doc) | `v2.tauri.app/plugin/notification/`, `github.com/tauri-apps/tray-icon`, `v2.tauri.app/reference/javascript/api/namespacetray/` |
| a6 | The Tauri 2 bundler's desktop targets are exactly `deb`, `rpm`, `appimage` (Linux), `app`/`dmg` (macOS), `nsis`/`msi` (Windows), selected via `bundle.targets`; Android APK/AAB and iOS builds are **not** part of `bundle.targets` — they come from the separate `tauri android build --apk`/`--aab` and `tauri ios build` commands | VERIFIED (doc) | `v2.tauri.app/reference/config/` (bundle targets enum), `v2.tauri.app/distribute/google-play/`, GitHub issue `tauri-apps/tauri#15419` confirming `android build` produces both APK and AAB |
| b | `tauri-driver`, driven directly (no third-party service), supports only Windows and Linux for desktop WebDriver automation; the documented reason is that "macOS has no WKWebView driver tool available". A third-party WebdriverIO service (`@wdio/tauri-service`) can additionally cover macOS via an *embedded* WebDriver server compiled into the app itself, which is a different mechanism from driving `tauri-driver` | VERIFIED (doc, exact quote fetched) | `v2.tauri.app/develop/tests/webdriver/` — quote: *"only Windows and Linux are supported on desktop, as macOS has no WKWebView driver tool available"* |
| c | Svelte 5 is stable (its own announcement: "Svelte 5 is alive"); Vite integration continues via the official `vite-plugin-svelte`, actively maintained into 2026 | VERIFIED (doc) / REPORTED (2026 activity) | `svelte.dev/blog/svelte-5-is-alive`, `svelte.dev/blog/whats-new-in-svelte-september-2026` |
| d | `ts-rs`'s `serde-compat` feature (on by default) understands `rename`, `rename_all`, `tag`, `content`, `untagged`, `skip`, `skip_serializing_if`, `flatten` and `default`; `typeshare` errors on `#[serde(flatten)]` and has a narrower attribute set | REPORTED | web search summarizing `github.com/Aleph-Alpha/ts-rs` docs and comparison posts; not independently read against typeshare's source in this session — see Risks |
| e | OpenSSH's `-L` local-forward flag has a `local_socket:remote_socket` form (`StreamLocal` forwarding) that tunnels a local unix-domain socket to a unix-domain socket on the remote host, usable from `ssh_config` as `LocalForward /local/socket /remote/socket` | VERIFIED (doc) | `man ssh` (`-L` four forms), `www.skreutz.com/posts/unix-domain-socket-forwarding-with-openssh/`, `www.25thandclement.com/~william/projects/streamlocal.html` |
| f | A macOS `LaunchAgent` plist under `~/Library/LaunchAgents/` with `RunAtLoad` (+ optionally `KeepAlive`) is the per-user, no-root equivalent of a `systemd --user` unit with `WantedBy=default.target`; only `Label` and `ProgramArguments` are mandatory keys | VERIFIED (doc) | Apple's `launchd.plist(5)` conventions as summarized by multiple sources; cross-checked structure against existing public LaunchAgent examples |
| g | `crates/net`'s iroh `Endpoint` is already built with more than one ALPN, and its `Router` already dispatches by ALPN to different handlers — so adding a third ALPN for control follows an established pattern, not a new mechanism | VERIFIED (repo) | `crates/net/src/endpoint.rs:958-970`: `Endpoint::builder(presets::Minimal)…alpns(vec![ALPN.to_vec(), iroh_gossip::ALPN.to_vec()])…bind()`; `crates/net/src/peers.rs:794-817`: `Router::builder(endpoint)…accept(ALPN, handler).accept(iroh_gossip::ALPN, inner.p2p.gossip().clone())…spawn()`; `Cargo.toml:86-87`: `iroh = "1"`, `iroh-gossip = "0.101"` |

Where (a1) and (d) are REPORTED rather than VERIFIED, U0 (the tech spike)
re-checks them against the pinned Cargo/npm lockfiles before any other
milestone starts, and records the pinned versions in the U0 results
section.

## Decision: UI technology

| Option | Cross-platform reach | Binary size / RAM | Rust integration | Team cost | Verdict |
|---|---|---|---|---|---|
| **Tauri 2** (chosen) | Linux, macOS, Windows, Android, iOS from one codebase (a2) | Small — OS webview, no bundled Chromium; Tauri's own comparisons put typical bundles under 10 MB vs. Electron's 100+ MB | The app's backend **is** Rust; the control client, transports and business logic live in the same language and crate graph as the daemon | One frontend stack (TS+Svelte) plus Rust glue already known to the team | **Chosen** |
| Electron | Desktop only (no first-party mobile) | Large — ships Chromium + Node per app (typically 100–200 MB, 100+ MB RAM at idle) | None; a Node process would re-implement the control client in JS or shell out | Familiar web stack, but no mobile story and a second runtime to secure | Rejected: no mobile path, and doubles the runtime surface (Node + the daemon) for no benefit here |
| Flutter | Linux, macOS, Windows, Android, iOS | Bundles its own Skia/Impeller renderer; larger than a webview but self-contained (no WebView2/WebKitGTK dependency) | Dart is the app language; talking to `constellation-control` needs an FFI bridge or a Dart reimplementation of the client | New language (Dart) for the whole team, plus an FFI boundary for every protocol change | Rejected: mobile-grade, but forces either a second client implementation or an FFI layer around every Rust type, doubling the maintenance surface the ts-rs/TS-types approach avoids |
| Slint / egui (native Rust GUI) | Desktop good; mobile immature (egui has no production iOS/Android story; Slint's mobile backends are newer and less proven) | Smallest, native widgets or immediate-mode rendering, minimal RAM | Best possible — pure Rust, the control client is called directly, no IPC boundary at all | No web/CSS skill reuse; charts, tables, rich text and accessibility all need to be built or vendored | Rejected for this plan: the no-IPC-boundary argument is real, but the 14-screen spec (tables, charts, a policy editor with two-way text/GUI sync, a retention-timeline SVG) is exactly the kind of rich, data-dense UI that a mature web/CSS toolkit is faster to build and iterate correctly; mobile parity is also weaker today than Tauri's |
| Pure web UI served by the daemon (grow today's `webui/`) | Any device with a browser, but only ever a *page*, never an installed app, no tray icon, no OS notifications, no offline shell | Zero extra binary | Already how `crates/api` works; no new client language | Cheapest short-term, but hits a wall: no tray, no native notifications, no "start at login", no packaged installer, and the DNS-rebinding class of bug (the module doc's own CVE-2025-49596 reference) is inherent to *any* browser-reachable surface, authenticated or not | Rejected as the *sole* answer, but its headless mode is kept (see Security model) precisely because the SPA this plan builds is reused there unchanged |
| Per-OS native apps (SwiftUI, WinUI/C#, GTK) | Full native fit per OS | Smallest per OS, but N codebases | Each needs its own control-client binding (or an FFI/gRPC-style bridge) | Highest: three to five UI codebases, three to five sets of bugs, three to five screens-worth of review per feature | Rejected: violates "one implementation, tested once" (plan 31's own stated engine philosophy) applied to the UI; the maintenance multiplier is the same argument plan 31 used to reject per-platform engines |

**Chosen: Tauri 2.** Its Rust core is not a thin shell — it **is** a
`constellation-control` client, exactly the kind of client the CLI, the
harness and (in 34/35) the port-specific tooling already are. The webview
is sandboxed by Tauri's capability/permission system (a3) and a strict CSP
(a4), never loads remote content, and the Rust side never opens a TCP
listener — the control connection is always a local transport (unix
socket with peer-cred authz, the Windows named pipe from 35, or in-process
on mobile, per plan 31's transport list). MIT/Apache-2.0, matching the
workspace's own MPL-2.0-compatible licensing posture (commit a945b05).

### Frontend stack

TypeScript + Svelte 5 (c) with Vite. One SPA, three hosts:

1. the Tauri webview (desktop and mobile app shell);
2. `constellation ui --web` (headless web mode, same build, served by the
   daemon's `web` adapter, replacing today's static `webui/` assets);
3. Playwright's browser driver in CI (against headless web mode with a
   `MockControlServer`, see Testing).

Reasons over React/Vue: Svelte 5's compiled output has no runtime
framework tax (small bundle — Performance budgets below), which matters
directly for `tauri-driver` smoke tests and for the retention-timeline
and performance-chart screens that must sustain 60 fps with live data
(plan 32 §7.4's simulator, screen 10's `stats.subscribe` charts). Vite
gives fast HMR during development and is Tauri's own documented default
frontend tooling.

### TS type generation: ts-rs, not typeshare

**Chosen: `ts-rs`.** `constellation-control::proto` (plan 31 C5) is built
directly on today's `crates/api/src/types.rs` shape: internally tagged
enums (`#[serde(tag = "cmd", rename_all = "snake_case")]` for `Request`,
`#[serde(tag = "resp", …)]` for `Response`), and near-universal
`#[serde(default)]`/`skip_serializing_if` on every status struct field (a
pattern that appears well over 200 times in `types.rs` alone — see the
citations above). `ts-rs`'s `serde-compat` feature is documented (d) to
understand exactly this attribute set, including `tag`, `content`,
`rename_all` and `skip_serializing_if`; `typeshare` is reported to reject
`#[serde(flatten)]` outright and has a narrower attribute surface. Even
though this codebase does not currently use `flatten` in the control
types, the broader attribute coverage means `ts-rs` needs zero changes to
today's serde annotations, while adopting `typeshare` would force an
audit (and likely rewrite) of every status struct's attributes before it
could even parse the crate. `ts-rs` also derives directly from the Rust
types with `#[ts(export)]` (no second IDL file to keep in sync — the same
"one schema, one truth" argument plan 31 makes for its JSON-Schema-via-`schemars`
generation), and its export step is a `cargo test` target, which slots
into the existing `cargo test --workspace` gate instead of adding a new
build step. U0 pins the exact `ts-rs` version (REPORTED claim (d) above
gets re-verified against the crate's real docs.rs page and changelog
before U1 starts).

**Risk noted, not blocking:** claim (d) is REPORTED, not independently
read against `typeshare`'s source. If U0's re-check finds `typeshare`
now handles internally-tagged enums with struct-variant payloads and
`skip_serializing_if` correctly, the choice is re-litigated in the U0
report rather than silently kept — this is the one place in this plan
where "verify, then decide" happens *after* the plan is written, because
the two crates' feature sets move independently of this repo.

## Settled decisions — do not relitigate

- **The UI's Rust core never opens a TCP listener.** It is a
  `constellation-control` client using local transports only: unix
  socket (Linux/macOS), named pipe (Windows, 35), in-process (mobile,
  36). Headless web mode is a property of the **daemon's** `web`
  adapter (today's `crates/api/src/web.rs`, hardened in U1), not of the
  Tauri app — the Tauri app never serves HTTP.
- **The webview never loads remote content.** No `<iframe>` to a
  non-local origin, no CDN scripts (bundle everything, matching plan
  31's "no remote content" instinct already present in the loopback
  web adapter's design). CSP is `default-src 'self'` plus the narrowest
  extra sources each screen's charting/SVG code actually needs.
  Violating this needs a written exception in the U0/U1 report, not a
  silent CSP loosening.
- **One IPC entry point.** The Tauri command surface is a small, named
  set (below), not "expose every control method as its own Tauri
  command." Authorization is enforced once, at the control protocol
  layer (plan 31 C5's role check per method), not duplicated in the
  Tauri capability files — capabilities restrict *which webviews* may
  call *which of these few commands*, not which control methods, so
  adding a control method never touches a capability file.
- **The old embedded `webui/` is retired, not grown.** `index.html`
  and `peers.html` are deleted outright once the SPA's Overview and Peers
  screens reach parity (U3/U6) — no compatibility redirect from `/` to the
  SPA's path is kept, per plan 31 §2's "no backward compatibility" rule.
- **Destructive control methods always require a role check plus, in
  the UI, a confirmation with a preview.** The UI never suppresses a
  server-side confirmation with `confirm_expiring`/similar
  fields (see plan 32 §7.3's `confirm_expiring: <n>` pattern) — the
  server-side guard exists precisely so a stale UI cannot confirm a
  different delta than the one it showed, and this plan's UI never
  works around that by hand-computing its own delta to pass.
- **Remote management (U7) is optional and off until paired.** No
  listener on the extra ALPN exists until the user runs the pairing
  flow once (see Security model). This mirrors plan 23's "absent
  unless armed" posture, but the mechanism differs — see "Distinction
  from plan 23" below.

## Security model

### Principals, transports, roles

Plan 31 C5 defines the principal source per transport (peer uid/gids, a
Windows SID, in-process, a remote device key) and the three roles
`viewer < operator < admin`, with every method declaring its minimum
role. This plan is where that declaration is enforced end-to-end and
where the operator-facing configuration, audit trail and UI for it live.

| Transport | Principal | Where it is decided | Notes |
|---|---|---|---|
| `UnixSocket` (Linux/macOS) | peer uid/gids via `SO_PEERCRED`/`getpeereid` | plan 31 C5 (transport), this plan (role mapping) | The daemon's own uid is always `admin` and cannot be demoted by the allowlist file (see the TOML format below) |
| `NamedPipe` (Windows, 35) | client SID via `GetNamedPipeClientProcessId`+token query | plan 35 (transport), this plan (role mapping) | Same allowlist format, `kind = "windows_sid"` grants |
| `InProcess` (mobile, 36) | the hosting app's own identity (there is exactly one principal: the app itself) | plan 31 C5 | Always `admin`; there is no other local process to isolate from |
| `Remote` (iroh `constellation-control`, U7 of this plan) | the paired device's iroh public key | this plan | Persistent, revocable grant in the allowlist file; never bypasses role checks |
| `UnixSocket`, service principal (plan 37's CSI plugin) | a container uid on a specific hostPath socket, matched by config rather than by the daemon-owner's own uid | this plan (§ Service principals below) | `kind = "service"` in the allowlist and in the audit log — distinguishes an automated driver from a human operator at the same peer-cred layer |

### The allowlist config

`control-acl.toml`, one per daemon state dir (default location follows
the platform's config-dir convention: `$XDG_CONFIG_HOME/constellation/`
on Linux, `~/Library/Application Support/constellation/` on macOS,
`%APPDATA%\constellation\` on Windows once 35 lands). It is a pure
allowlist: absence of a grant means `viewer` is denied too (default
deny), except the daemon's own owner, who is always `admin` and is not
expressible in the file — a malformed or truncated file can only narrow
access, never widen it past the owner.

```toml
# constellation control-plane access (plan 33 U1).
# Loaded once at daemon start and on SIGHUP; a parse error keeps the
# previous table in memory and logs at ERROR (fail safe, not fail open).
version = 1

[owner]
# Informational only — the actual rule ("the uid that started the
# daemon is admin") is hardcoded, never read from this file.
note = "the daemon's own uid is always admin; this section only documents it"

[[grant]]
kind = "unix_group"
name = "constellation-operators"
role = "operator"

[[grant]]
kind = "unix_user"
name = "backup-svc"
role = "viewer"

[[grant]]
kind = "windows_sid"
sid = "S-1-5-21-…-1001"
role = "operator"

# Populated by the pairing flow (U7), never hand-written in practice,
# but hand-editable for revocation without the app running.
[[grant]]
kind = "device"
key = "a1b2c3d4e5f6…ed25519-hex"
role = "operator"
label = "Attila's phone"
paired_unix_ms = 1790000000000

[audit]
enabled = true
path = "~/.local/state/constellation/audit.jsonl"
retention_days = 90
max_bytes = 104857600
```

### Service principals

A `UnixSocket` grant is normally matched to a human's uid/group. Plan 37's
CSI node plugin is not a human: it runs as a fixed container uid inside a
DaemonSet pod, on a per-engine-pod hostPath socket, and must be granted
`operator` (it calls `view.mount`, `fs.unlock`, `snapshot.create`, etc.) as
an unattended, automated caller. Rather than inventing a second
authorization mechanism for this one case, a grant may additionally match
on the *socket path*, so the same allowlist format covers it:

```toml
[[grant]]
kind = "service"
principal = "uid:0"
socket = "/var/lib/constellation-csi/<fs>-<node>/control.sock"
role = "operator"
label = "csi-node-plugin"
```

- `kind = "service"` grants are matched by `(uid, socket path)` rather
  than `(uid, any socket)` — a service principal is scoped to the one
  hostPath socket its engine pod exposes, not to every socket that uid
  might ever open, which keeps a compromised sidecar in one pod from
  reaching a different engine pod's control socket even if it somehow
  shared a uid.
- Every audit entry (below) from a `kind = "service"` grant carries
  `"principal":{"kind":"service","uid":0,"socket":"…","label":"csi-node-plugin"}`,
  so an operator reviewing the audit trail can immediately tell "the CSI
  driver did this" apart from "an admin typed this at a terminal" —
  plan 37's own audit requirement (every CSI-driven mutation carries the
  PV name in its params) composes with this unchanged, since
  `params_digest` still covers the request params as normal.
- This is additive to the `unix_user`/`unix_group`/`windows_sid`/`device`
  grant kinds already defined above; it introduces no new role and no new
  enforcement path, only a narrower match predicate for the existing
  `UnixSocket` transport.

- `[[grant]]` rows are evaluated in file order; the **highest** matching
  role wins when a principal matches more than one row (a user who is
  in `constellation-operators` and also has a personal `unix_user` row
  at `viewer` still gets `operator`) — least-surprise for admins who add
  a broad group grant and a narrower personal one in either order.
- Every mutating request path logs the row that granted it (principal,
  matched grant, role) at `debug`, so `doctor`/`status` can show "why
  am I only a viewer" without guesswork.
- The file is validated with a `cargo test` round-trip (parse → format →
  parse) and a fuzz target (parse never panics), matching plan 32's
  posture on its own policy-language parser.

### Audit log

JSON Lines, one object per **mutating** control action (methods below
`operator` that only read need no entry — the log is for accountability
of change, not a full request trace, which `LogTail`/`node.logs.tail`
already covers):

```json
{"ts":"2026-09-28T21:00:00.123Z","seq":4821,"principal":{"kind":"unix_user","uid":1000,"name":"bra"},"role":"admin","transport":"unix_socket","method":"fs.registry.create","params_digest":"blake3:9f2a7c…","outcome":"ok","latency_ms":42}
```

- `params_digest` is a BLAKE3 hash of the canonical (sorted-key) JSON
  encoding of the request params, never the params themselves — a path
  or a snapshot selector can be sensitive, and plan 31's own audit
  requirement is phrased as "who, when, method, params digest" for
  exactly this reason.
- `seq` is a monotonic per-daemon-incarnation counter, so gaps (file
  truncated, disk full) are self-evident.
- Rotation: size-based (`max_bytes`) plus age-based (`retention_days`);
  the retiring file is renamed `audit.jsonl.<unix_ms>` and kept until
  `retention_days` regardless of `max_bytes` churn on the live file.
- Screen 14 (Audit log) is a paginated, filterable read-only view over
  this file via a new `node.audit.tail`/`node.audit.query` control
  method (added in U1, read-role `admin` — the audit trail is itself
  sensitive: it reveals every path an admin touched).

### Headless web mode

`constellation ui --web [--port N]` starts the daemon's `web` adapter
(today's `crates/api/src/web.rs`, hardened here) serving the **same**
SPA build used by the Tauri app, over loopback HTTP, off by default —
this replaces the `--web-ui` mount flag's always-on-if-set-to-nonzero
behavior with an explicit, separate subcommand, because embedding a
management UI is a bigger decision than mounting a filesystem and
deserves its own opt-in.

Flow:

1. Starting the mode prints (and logs) a one-time URL:
   `http://127.0.0.1:8443/auth?token=<random-256-bit>`.
2. Visiting it exchanges the token for an `HttpOnly`, `SameSite=Strict`
   session cookie (no `Secure` flag requirement — this is loopback-only
   HTTP by design, matching today's adapter) plus a separate CSRF token
   returned in the page and required as an `X-CSRF-Token` header on
   every mutating request (double-submit pattern). The one-time token
   is single-use and expires after `CONSTELLATION_UI_WEB_TOKEN_TTL_S`
   (default 300 s) whether or not it was used.
3. `guard_rebinding` (web.rs:110, unchanged in spirit) still runs first,
   on every route including the auth exchange — DNS rebinding is a
   *pre*-authentication attack surface and stays closed regardless of
   the new token/cookie layer, per the module's own CVE-2025-49596
   citation.
4. The session cookie's principal maps to a role exactly like any other
   local transport: the session is created under the uid that ran
   `constellation ui --web` (or, if a remote operator forwarded a port
   in from elsewhere, under whatever local account received the token —
   the token itself is the credential, not the network path).
5. After auth, traffic is the control protocol (plan 31 §9) carried
   over the same HTTP POST endpoint the web adapter already serves; only
   the auth gate and the CSRF header are new on top of what plan 31 C5
   ships — there is no old `/api` payload to preserve, since `crates/api`
   is deleted (plan 31 §2).

**Remote access without opening a port:** `constellation ui --connect
ssh://user@host` runs `ssh -L <tmp-local-socket-path>:<remote-state-dir>/control.sock
user@host -N` in the background (OpenSSH's `local_socket:remote_socket`
form, verification (e)) and points the Tauri app's `UnixSocket`
transport at the forwarded local socket path instead of a local daemon.
This composes with the peer-cred authz model **without new plumbing**:
the remote `sshd` process that terminates the forwarded connection on
the far end runs as `user`, so the remote daemon's own `SO_PEERCRED`
check sees exactly the SSH login account as the peer — the same
allowlist-by-unix-user grant that governs a local session governs this
one. No token, no extra auth layer, no new server-side code path; the
transport is the same `UnixSocket` transport plan 31 already defines,
just dialed at the end of an SSH tunnel instead of `/run/user/…`.

### Remote management (U7, optional)

A second, independent way to reach a daemon: over its iroh endpoint, on
a **new ALPN** `constellation-control` (verification (g): the
endpoint already carries two ALPNs and dispatches by ALPN via
`Router::accept`, so this is a third `accept()` call, not a new
mechanism). Off until paired:

1. **Pairing.** `constellation ui pair --role operator --label "Attila's
   phone"` on the machine to be managed prints a ticket (the daemon's
   iroh `EndpointId` + known addresses + a one-time pairing secret,
   analogous to an iroh `NodeTicket`) and renders it as a QR code in
   the Tauri app; alternatively the ticket text can be copied/pasted.
   The daemon does **not** yet accept `constellation-control`
   connections at this point — only the pairing secret's exchange path
   is live, over the same ALPN, gated by the secret rather than by a
   device key it does not have yet.
2. **Acceptance.** The phone (or a second desktop) app's "Add device"
   flow dials the ticket's addresses, presents the one-time secret over
   the now-encrypted QUIC channel, and on success the daemon appends a
   `[[grant]] kind = "device"` row to `control-acl.toml` (the device's
   long-lived iroh public key, the requested role, capped at whatever
   role the *pairing operator* held — a `viewer` cannot mint an `admin`
   pairing) and persists it immediately.
3. **Use.** From then on the device's key is a normal `Remote`
   principal like any row in the allowlist; every request over
   `constellation-control` goes through the identical
   dispatch/role/audit path as a local unix-socket request. There is no
   special "remote" code path in the control dispatcher — only a
   different `Transport` implementation feeding the same principal →
   role resolution.
4. **Revocation.** Deleting the `[[grant]]` row (by hand, or from
   Screen 13's "Remote devices" list) takes effect on the next request;
   the daemon additionally closes any pooled iroh connection keyed to
   that `EndpointId` on revocation, so an already-open connection
   cannot keep working past the revoke.

### Distinction from plan 23 (do not conflate)

Plan 23's `constellation-support/1` and this plan's
`constellation-control` look superficially similar (both are iroh
ALPNs added to a daemon that otherwise only speaks the cluster ALPN)
and must stay clearly separate in code, docs and the UI:

| | Plan 23 remote support | Plan 33 remote management (U7) |
|---|---|---|
| ALPN | `constellation-support/1` | `constellation-control` |
| Compiled in by default? | **No** — behind the `remote-support` Cargo feature, off by default; a default build has no code path at all | **Yes** — part of the normal control-plane surface, like the unix socket, but the *listener* is dormant until paired |
| Identity | Fresh Ed25519 key generated at mount, never written to disk, dies with the process | The daemon's long-lived host node key (`~/.config/constellation/node.key`), the same identity the cluster ALPN uses |
| Who can connect | An exact, small set of published support-engineer public keys, baked in at `--support-allow` mount time | Any device whose key is in the persistent, revocable `control-acl.toml` allowlist, added by pairing |
| Session lifetime | The mount's process lifetime; no TTL, ended by killing the process | Indefinite, governed by the allowlist row; ended by revocation, not by stopping anything |
| What it grants | `ro`: read-only node introspection. `rw`: **node parity** — anything the node itself can do, including repairs, explicitly *not* scoped down further | Exactly the control protocol's role ladder (`viewer`/`operator`/`admin`), the same roles a local UI session gets — never "node parity"; there is no mode here that exceeds what an admin control-socket session can already do |
| Purpose | One-off debugging of a specific user's problem, with their explicit, out-of-band cooperation (they type a key) | Ongoing device management — the phone UI managing the user's own desktop daemon |

Concretely: this plan's control dispatcher must never accept a
`constellation-support/1` connection, plan 23's support dispatcher must
never accept a `constellation-control` connection, and the two
allowlists (support's compiled-in key set vs. this plan's
`control-acl.toml` device grants) are never merged or cross-checked.
Where both features are compiled in, a daemon may have both ALPNs live
at once; they remain two independent accept handlers on the same
`Router`, exactly as the cluster ALPN and the gossip ALPN are today.

### Daemon lifecycle from the UI

The app, on launch, tries the local transport in order: the well-known
per-user socket path (plan 31 C5's `$XDG_RUNTIME_DIR/constellation/` on
Linux, `$TMPDIR` on macOS, the named pipe on Windows), then `Ping`
(lib.rs's existing `ping()` helper is the model — connect success alone
does not prove liveness, a `Ping` round trip does). If absent, the
Overview screen (below) offers "Start Constellation" (spawns the daemon
process with the default filesystem, or opens the Filesystems screen if
none is registered yet) and a "Start at login" toggle:

- **Linux:** writes `~/.config/systemd/user/constellation.service`
  (`WantedBy=default.target`, `ExecStart=<binary> mount …`) and runs
  `systemctl --user enable --now constellation.service`.
- **macOS:** writes `~/Library/LaunchAgents/com.constellation.daemon.plist`
  with `Label` and `ProgramArguments` (the two mandatory keys per
  verification (f)) plus `RunAtLoad = true`, and loads it with
  `launchctl load -w`.
- **Windows (35):** a per-user Scheduled Task or `Run` registry key;
  specified in 35, this plan only reserves the toggle's UI slot and the
  Tauri command name (`toggle_start_at_login`) so 35 can implement the
  platform body without touching the UI.
- **Mobile (36):** the app *is* the engine process (in-process
  transport); there is no separate daemon to start, so this toggle is
  hidden on Android/iOS.

## UI architecture

### Process and IPC shape

```
┌─────────────────────────────── Tauri app process ───────────────────────────────┐
│  webview (Svelte 5 SPA, sandboxed by capabilities + CSP, no remote content)      │
│      │  invoke(cmd, args)         events: emit("control-event", payload)        │
│      ▼                                                     ▲                    │
│  Tauri Rust core                                                                 │
│    - control_call(method, params) -> Result<JsonValue, ControlError>            │
│    - control_subscribe(sub) -> subscription id; events streamed back            │
│    - control_cancel(id)                                                         │
│    - daemon_discover() / daemon_start() / toggle_start_at_login(bool)           │
│    - pair_generate_ticket(role, label) / pair_accept(ticket)                    │
│    - fs_pick_directory() / fs_save_downloaded_file(name) (native dialogs)       │
│    - open_in_file_manager(path)                                                 │
│      │                                                                          │
│      ▼  constellation-control client (plan 31 C5)                              │
│  Transport: UnixSocket | NamedPipe (35) | InProcess (36) | Remote (U7, iroh)    │
└────────────────────────────────────┬─────────────────────────────────────────────┘
                                      │  local transport only — never TCP
                                      ▼
                         constellation daemon's control server
```

The command surface above is deliberately small and named — see
"Settled decisions". `control_call` is the single generic pass-through
for the ~40+ control methods (today's 37 plus plan 31's `node.*`/`fs.*`/`view.*`/`browse.*`
additions and plan 32's dozen), because the wire protocol *itself*
carries authorization (plan 31 C5's per-method role) — duplicating a
per-method Tauri permission would be a second, easily-drifting copy of
the same table. What Tauri's capability file *does* restrict is which
window labels may call `control_call`/`control_subscribe` at all (only
the app's own main/mobile webview — there is exactly one window label
in this app, so the capability is a single narrow allow-rule) and it
denies every other Tauri core API (shell execution, arbitrary file
system access, HTTP fetch) outright, since none of them are needed —
the app reaches the outside world exclusively through the control
client.

### Generated types

`constellation-control::proto` (plan 31 C5) derives `ts_rs::TS` (behind
a `ts-rs` feature, so it costs nothing in the daemon binary) on every
`Request`/`Response`/status type. `cargo test -p constellation-control
--features ts-rs export_bindings` writes `.ts` files under
`crates/control/bindings/`; the frontend build copies (or, in dev,
symlinks) them into `ui/src/lib/generated/`. A CI job (below) fails if
`git diff --exit-code` on the generated files is non-empty after
running the export — the plan-31-style "generated files must be
current" contract test.

### State management

Svelte 5's runes (`$state`, `$derived`) hold the client-side mirror of
control state; there is deliberately no separate global store library —
Svelte 5's own reactivity primitives are sufficient for a single-SPA
app of this size, and adding one is unjustified surface. Screens
subscribe to the relevant `stats.subscribe`/`events.subscribe` streams
(plan 31 C5) through `control_subscribe`; the Rust core fans out
`Event` frames to Tauri events keyed by subscription id, so a screen
that navigates away can `control_cancel` and stop paying for updates it
no longer renders — mirroring the protocol's own `Cancel` frame
semantics end to end.

### Streaming transfers

Uploads/downloads (Screen 4, the Browser) use the protocol's `Chunk`
frames (plan 31 C5) rather than a side-channel HTTP endpoint — even in
headless web mode, where today's `download` handler
(`crates/api/src/web.rs:164`, streaming via a bounded `mpsc` so a large
file is never buffered whole) is the existing proof this pattern
already works over HTTP; the same backpressure discipline applies to
the Tauri IPC path (bounded buffer, producer blocks on a full channel,
never materializes a whole file in memory).

### Offline and degraded state

Every screen has three states beyond its happy path, driven by the same
signals the control client already exposes: **disconnected** (no daemon
reachable — shows the "Start Constellation" flow), **degraded** (daemon
reachable but reporting trouble — `StatusReport::spool.last_ship_error`,
`held`, `speculation.gate_pending`, `own_s3.stalled`, and friends
already exist in `types.rs` today and are surfaced as-is, not
re-derived), and **stale** (a `Remote` transport whose last successful
poll is older than a threshold — the UI marks every figure it shows
with an "as of" timestamp, echoing plan 32's own `as_of_seq`/`as_of_ms`
convention for its accounting numbers).

## Screens

Each screen: purpose, its control-method data sources, its actions with
their required role, and its empty/error/loading/destructive-action
handling. Methods named `plan-31-tbd.*` are ones plan 31 defines the
shape of but this plan does not invent; methods named explicitly with
today's `snake_case` `cmd` values are the old `Request` variants
(`crates/api/src/types.rs`) that plan 31 §9.2 maps 1:1 to a control-protocol
method — this plan always calls the control-protocol method (§9.2's
table), since `crates/api` itself is deleted in plan 31 C5, not kept as a
compatibility layer.

### 1. Overview

- **Purpose:** node health at a glance — the first thing an operator
  sees, and the tray's "open" target.
- **Data:** `node.status` (the control protocol's status, superset of
  today's `StatusReport`): uptime, version, `enrolled`, lease state,
  spool backlog, degraded-state banners (as listed above).
- **Actions:** none destructive here; "Start/Stop mount" shortcuts link
  to Screen 3. Role: `viewer` to view; the shortcuts need `operator`.
- **States:** empty = no filesystem registered yet → CTA to Screen 2.
  Error = disconnected (see Offline/degraded above). Loading = skeleton
  cards, not a blocking spinner (the rest of the shell — nav, tray
  state — stays interactive).

### 2. Filesystems

- **Purpose:** the registry: create, import, export, manage E2E
  passphrases, run doctor probes.
- **Data:** `fs.registry.list/create/import/export/passwd/doctor`
  (plan 31 C5), today's `Doctor`/`DoctorStatus` (`Request::Doctor`,
  `types.rs:339`) as the doctor pane's content.
- **Actions:** create/import/export (`operator`); `fs passwd` and
  delete-from-registry (`admin`). Export and delete require **typed-name
  confirmation** (the user types the filesystem's name to proceed) —
  the same UX rule plan 32 already uses for its expiring-policy confirm,
  applied here to the higher-stakes registry actions.
- **States:** empty = "no filesystems yet, create or import one". A
  failed doctor probe surfaces per-check red/green rows (`CasProbeStatus`,
  `types.rs:353`), not just pass/fail.
- **CSI pools (plan 37):** a pool filesystem (one `layout: pool`
  StorageClass, auto-created idempotently by `fs.create` on first
  `CreateVolume`, plan 31 §9.2) is a registry entry like any other, but
  this screen adds the StorageClass → pool filesystem → shard hierarchy
  on top, plus the volumes living inside each pool/shard.
  - **Data:** `fs.list` for the pool filesystems themselves (shards of one
    StorageClass share its `bucket`/`prefix-shard-<k>` naming, so they
    group under one StorageClass row) + `view.list{labels}` (plan 31
    §9.10, filtered to CSI-labelled `/volumes/*` views for each volume's
    PV/PVC/namespace) + `browse.stat/readdir` (plan 31 C5's `ControlVfs`)
    on each pool's `/volumes` (per-volume capacity vs. used, from the
    volume directory's `quota`/`rsize` xattrs) and `/.trash` (deleted-PV
    entries awaiting the singleton purge job).
  - **Actions:** viewing the StorageClass/pool/shard/volume table
    (capacity, snapshots, trash status) is `viewer`. Drilling into a
    specific volume's live content ("inspect": its xattrs, snapshot list,
    trash entry) and "purge trash now" (runs the pool's purge job
    immediately instead of waiting for its normal schedule) are both
    `admin`-only safe actions — inspecting one tenant's volume content
    crosses the pool's own trust boundary (one engine pod, and this UI,
    can reach every volume in a pool; a plain `operator` should not get
    that reach by default). Creating or deleting a volume is **not** an
    action this screen offers: that stays with Kubernetes
    (`PersistentVolumeClaim` create/delete), and the screen states this
    directly next to a disabled create button rather than omitting it
    silently.
  - **States:** empty = "no CSI-provisioned pools". A trash entry past its
    expected purge window is highlighted, the same "stale, never silently
    swept" treatment Screen 7 gives an orphaned designation — a stuck
    purge job is exactly the kind of silent failure this view exists to
    surface. A snapshot shown here that carries an external-owner hold
    uses the same "externally owned" badge as Screen 5.

### 3. Mounts / views

- **Purpose:** attach/detach views, per-OS frontend options, "open in
  file manager".
- **Data:** `MountList`/`MountAdd`/`MountRemove` (`types.rs:148-164`,
  carried forward as `view.mount/unmount/list`).
- **Actions:** mount/unmount (`operator`); unmounting the last view of
  a filesystem warns it will stop the daemon (today's daemon-exits-on-
  last-unmount behavior, per `crates/api`'s doc comments). "Open in file
  manager" calls the Tauri `open_in_file_manager` command (a thin OS-open
  wrapper, not a control method — it never touches the daemon).
- **States:** empty = "not mounted anywhere"; per-OS option sets differ
  (`MountViewOpts`'s `allow_other`/`fuse_threads` are Linux/FUSE-only
  today — the form hides fields the current platform's frontend caps
  don't support, sourced from plan 31's `FrontendCaps`).
- **CSI-provisioned views (plan 37):** `view.list`'s `labels` field (plan
  31 §9.10) is shown as chips on each view row (`pv`, `pvc`, `namespace`
  when present) and the list is filterable by label — an operator can type
  `namespace:prod` to see only that namespace's PVs. A view carrying CSI
  labels is otherwise an ordinary view: it can be inspected the same way,
  but its mount/unmount actions are informational only (a CSI-managed view
  is normally attached/detached by the CSI node plugin, not by hand) —
  the UI does not hide the buttons, it just does not pretend a manual
  unmount here is what a `NodeUnpublishVolume` call would do.

### 4. Browser

- **Purpose:** a tree/list view with stat details, upload/download,
  rename/delete, previews.
- **Data:** `browse.stat/readdir/read/write/mkdir/rename/delete`
  (plan 31 C5's `ControlVfs`), superseding today's `ReadDir`/`Inspect`
  (`types.rs:114-121`).
- **Actions:** browsing is `viewer`; write operations are `operator`.
  Delete requires confirmation showing the target's size (from `Inspect`);
  deleting a directory previews the entry count.
- **States:** empty directory shows "empty" not a blank table (today's
  `index.html:155` renders an empty `<table>` with just headers — this
  screen improves on that). A permission-denied read shows the
  `Code`-mapped message plus remediation, per the UX rules below.
- **Transfers:** upload/download stream via `Chunk` frames (see UI
  architecture); progress is cancellable per-file (`Cancel` frame) and
  shows throughput, matching the existing download handler's
  no-buffering discipline.

### 5. Snapshots and clones

- **Purpose:** list/create/delete/clone/`snap refs`, **plus plan 32's
  policy editor, retention simulator, and space accounting** (USED /
  WRITTEN / REFER, the space-overview bar, the snapshot table with
  KEPT BY/EXPIRES).
- **Data:** today's `SnapshotCreate/List/Delete/Clone/SnapRefs`
  (`types.rs:93-113`), plus plan 32 §7.6's additive methods
  (`snap_policy_check/set/remove/list/show/pause`, `snap_policy_simulate`,
  `snapshot_hold`, `snapshot_reclaim`, `snapshot_space`,
  `snapshot_delete_many`, `snap_sched_status/run`) once plan 32 lands.
- **Actions:** create/clone (`operator`); delete/hold (`operator`,
  multi-select delete previews reclaim bytes via `snapshot_reclaim`
  before confirming, per plan 32 §7.5); policy edit/remove (`admin` —
  a policy change can eventually delete a lot of data unattended, so it
  gets the higher bar even though a single manual snapshot delete does
  not).
- **States:** see "Coordination with plan 32" for how this screen
  behaves before plan 32's API methods exist.
- **Externally-owned snapshots (plan 37):** a snapshot carrying an
  external-owner hold (plan 32's `csi:<VolumeSnapshotContent uid>`
  namespace, once plan 32's hold-namespace change lands) is shown with an
  "externally owned" badge instead of a plain KEPT BY chip, and its delete
  action is disabled for `operator` — deleting it needs `admin` **and** an
  explicit "override external ownership" confirmation distinct from the
  normal delete confirm, so a CSI-managed snapshot is never removed by a
  reflexive click on the same button used for a manually created one. The
  confirmation copy names the owning `VolumeSnapshotContent` uid verbatim
  (from the hold string) so the operator can cross-check it against the
  cluster before overriding.

### 6. Usage

- **Purpose:** `rsize` per directory (treemap), quotas, cache usage and
  eviction, an S3 usage/cost estimate.
- **Data:** `CacheStatus`/`CacheList`/`CachePrune`/`SetQuota`/`GetQuota`
  (`types.rs:1869-1884`, `160-174`), `InspectStatus.manifest` for
  per-directory `rsize` (today's `rsize` virtual xattr, surfaced over
  control instead of xattr for the UI's sake).
- **Actions:** cache prune, quota set (`operator`); the S3 cost estimate
  is display-only (a configurable $/GB rate, entered once in Settings,
  applied client-side to `used_bytes` — never a real billing API call).
- **States:** the treemap shows a loading skeleton while a large
  directory's recursive `rsize` is computed (today's O(n) scan cost per
  plan 32's "Where we are" notes — this is not free, and the UI must
  not imply otherwise); a progress percentage if the control protocol
  can report one, else an indeterminate spinner with an explicit "this
  can take a while on large trees" note.

### 7. Pins, offline designations and delegations

- **Purpose:** three related node-locality features on one screen
  (they share a mental model: "which node owns/caches what").
- **Data:** `Pin/Unpin/ListPins` (`types.rs:11-18`),
  `Offline/Online/ListDesignations` (`:19-46`),
  `Delegate/Undelegate/ListDelegations` (`:26-42`).
- **Actions:** pin/unpin, offline/online (`operator`); delegate/undelegate
  (`admin` — delegation moves write authority, the highest-stakes
  action on this screen).
- **States:** empty = "nothing pinned/designated/delegated here". A
  designation on a path that no longer exists (directory deleted) is
  shown greyed out with a "stale" badge rather than silently dropped —
  matching plan 32's "orphaned, never silently swept" posture applied
  to a different feature.

### 8. Prune policies, GC and fsck

- **Purpose:** prune policy management (+ dry-run), GC and fsck runs
  with live progress.
- **Data:** `PruneRun/PruneList` (`types.rs:63-72`), `PruneStatus`
  (`:788-827`), `GcRun` (`:79-82`), `FsckRun` (`:87-92`).
- **Actions:** dry-run is `viewer` (it mutates nothing); real prune/GC/fsck
  runs are `admin` — fsck `repair: true` is the single most destructive
  button in the whole app and gets the strongest confirmation: typed
  confirmation **and** a link to the dry-run/doctor output that
  justified running it.
- **States:** a running GC/fsck shows live progress (the report is
  currently returned as opaque `serde_json::Value` per `Response::GcReport`/`FsckReport`,
  `types.rs:266-276` — the UI renders whatever shape arrives generically,
  a key/value table, rather than assuming a fixed schema, since the
  control crate intentionally does not depend on `cli`'s report types).

### 9. Peers / P2P

- **Purpose:** paths, latency, cooperative-cache sources — a direct
  evolution of today's `peers.html`.
- **Data:** `P2pStatus`/`PeerStatus`/`PeerPathsStatus`/`SourceStatus`
  (`types.rs:1264-1349`, `997-1026`) — this is the richest single struct
  cluster in today's API and the screen mirrors `peers.html`'s existing
  table (host, path, version, seen, latency, BW, hit/miss/err%) with
  live updates via `stats.subscribe` instead of the 2-second poll loop.
- **Actions:** `ForceRelease` (`types.rs:124-126`) is `operator`, with a
  note that it is "cooperative administration, not a fencing operation"
  (the code's own doc comment, surfaced verbatim as UI copy — precise
  language matters here).
- **States:** the synthetic S3 "peer" row (`PeerStatus.s3`, always first
  per the doc comment) is visually distinguished (a different icon, not
  just a text label) so operators don't mistake it for a cluster member.

### 10. Performance

- **Purpose:** live charts from `stats.subscribe`: ops/s, latency
  percentiles, S3 requests, upload queue, writeback.
- **Data:** `S3RequestStatus` (`:730-761`), `WritebackStatus`
  (`:880-922`), `PrefetchStatus` (`:852-878`), plus plan 31 C7's unified
  op metrics (`constellation_vfs_op_seconds` histograms) once available.
- **Actions:** none destructive; a "pause updates" toggle for reading a
  frozen frame. Role: `viewer`.
- **States:** this is the screen the Performance budgets section below
  is written for — see 60fps/N-points targets. No data yet (daemon just
  started) shows flat lines at zero, not empty charts with no axes.

### 11. Locks / leases / epochs (advanced)

- **Purpose:** the deepest internals screen, hidden behind "Advanced"
  (progressive disclosure, see UX rules) — `LockStatus`, `LeaseStatus`,
  `EpochStatus`, `CtoStatus`, `AckStatus` (`types.rs:1482-1824`,
  `1826-1843`, `1029-1087`, `1400-1480`, `1723-1824`).
- **Data:** as above, read-only.
- **Actions:** `ForceRelease` on a lease/lock (`admin` — releasing a
  lock another process believes it holds can corrupt an application's
  own invariants, not Constellation's; the confirmation dialog says so
  explicitly).
- **States:** this screen is dense by design (it is the "advanced"
  tier); a "copy as JSON" button on every card helps operators paste
  state into a bug report without transcription errors.

### 12. Logs

- **Purpose:** live tail with filters.
- **Data:** `node.logs.tail` (old `LogTail`, `types.rs:127-130`), streamed
  incrementally via `events.subscribe` rather than polled, once
  available; falls back to periodic `node.logs.tail` polling until
  `events.subscribe` streaming lands.
- **Actions:** read-only; `viewer`.
- **States:** a live-tailing pause button (so a fast log doesn't scroll
  away what the operator is reading); client-side text filter plus a
  level filter, matching the daemon's own tracing levels.

### 13. Settings

- **Purpose:** roles and allowlist editing, remote devices (pairing
  list + revoke), start-at-login, theme/language.
- **Data:** reads/writes `control-acl.toml` through a dedicated
  `admin`-only control method (never direct file access from the
  webview — the Tauri core is the only thing that touches the daemon's
  config, over the control protocol, so the same audit trail covers
  Settings changes as covers everything else).
- **Actions:** every allowlist edit is `admin`, audited, and shows a
  diff-style confirm ("adds `operator` for group
  `constellation-operators`") before saving.
- **States:** the remote-devices list shows "never used" vs. last-seen
  per device; revoking shows an immediate optimistic UI update plus the
  server confirmation, and reverts visibly if the server call fails.

### 14. Audit log

- **Purpose:** the read-only view over `audit.jsonl` described in
  Security model.
- **Data:** `node.audit.tail`/`node.audit.query` (new in U1), `admin`-only.
- **Actions:** filter by principal/method/date range; export the
  filtered view as JSON (a local file save via the Tauri
  `fs_save_downloaded_file` command, not a network call).
- **States:** empty = "no audited actions yet" (a fresh install, or
  auditing was just enabled); a gap in `seq` is called out inline
  ("12 entries missing here — log rotated or daemon restarted
  uncleanly") rather than silently rendering a discontinuity.

## Tray / menu bar

A status icon with four states — synced, syncing, offline, error —
driven by the same signals as Overview's banners (no separate
"tray-only" health computation). Menu: quick mount/unmount (per
registered filesystem), open folder (per mounted view), pause uploads
(maps to a writeback-pause control method, `operator`), Open
Constellation (raises the main window), Quit. Built on the `tray-icon`
crate/feature (verification a5); the icon asset is themed for light/dark
menu bars per-OS (macOS template images, Windows/Linux explicit
light/dark variants).

## Notifications

Native OS notifications (the notification plugin, verification a5) for:
errors (spool ship failures persisting past a threshold), reintegration/conflicts
(a `.constellation-conflict/` copy was materialized — surfaced with the
path), quota thresholds (configurable percentage, default 90%), and
lease fencing (`LeaseStatus.lost` transitioning true — the node was
deposed). Every notification opens the relevant screen on click (errors
→ Overview, conflicts → Browser at the conflict path, quota → Usage,
fencing → Locks/leases). Notifications are rate-limited per category
(no more than one per category per `CONSTELLATION_UI_NOTIFY_COOLDOWN_S`,
default 300) so a flapping condition does not spam the OS notification
center.

## UX rules

- **Progressive disclosure.** Screens 1–9 are the "basic" tier, always
  visible; Screens 11 (locks/leases/epochs) and parts of Screen 13
  (raw allowlist TOML editing, vs. the guided form) are behind an
  "Advanced" toggle in Settings, off by default — most operators never
  need lock internals, and hiding them by default keeps the nav from
  becoming plan 31's own 37-method surface rendered as 37 buttons.
- **Destructive actions show previews/dry-runs and typed confirmation.**
  Established per-screen above; the general rule: any action whose
  server-side handler can return a "would affect N things" preview
  (prune dry-run, fsck repair scope, snapshot delete reclaim estimate,
  policy-change delta) shows that preview before the confirm button is
  enabled, not just before the request is sent — a disabled-until-previewed
  confirm button prevents a rushed click on stale information.
- **Long operations stream progress and are cancellable.** Every
  screen-8/9-class operation (GC, fsck, prune run, large transfers)
  uses the `Cancel` frame; the UI never fakes cancellability with a
  client-side "abandon" that leaves the server-side operation running
  unbeknownst to the operator.
- **Every error shows a `Code`-mapped message plus remediation.** Plan
  31's `Response::Err{code, kind, message, details, remediation?}`
  shape is rendered with `remediation` as an actionable link when
  present (often "run Doctor", "see Logs", or a direct link to the
  relevant Settings row) and the raw `message` always visible
  underneath for operators who want the exact text (never hide the
  real error behind only a friendly paraphrase).
- **Offline and degraded states are always visible.** Not just on
  Overview — every screen's header repeats the current top-level state
  (a small persistent badge, not a full banner) so a user deep in the
  Browser screen still sees "degraded" without navigating back.

## Accessibility

WCAG 2.2 AA target, keyboard-complete (every action reachable and
completable without a mouse — confirmed per-screen in the U0 wireframe
review and re-checked by the axe + manual keyboard pass in each
milestone's gate), and:

- every icon-only control (tray menu items excepted, which are native
  OS menu text) has a text label or `aria-label`;
- color is never the sole signal (the health dot, the KEPT BY chips
  from plan 32, the destructive/confirm button pair all pair color with
  shape/text, per the `dataviz` skill's guidance already cited by plan
  32 §7.4);
- focus order follows visual/reading order on every screen, verified
  manually per milestone (automated tools do not catch this reliably);
  visible focus rings are never suppressed by CSS;
  charts (Screen 10, plan 32's simulator) expose an accessible data
  table alternative — a toggle, not a separate page, so screen-reader
  users are one action away from the same information sighted users
  get from the SVG.
- contrast: both the light and dark theme token sets (below) are
  validated at 4.5:1 for text, 3:1 for UI component boundaries, as part
  of the design-system build (a lint step, not a manual spot check).

## Internationalization

Every user-facing string lives in a message catalog (`ui/src/lib/i18n/`,
one JSON per locale, English as the source of truth and the only locale
shipped in U0–U6; additional locales are a post-U8 concern, not
blocking this plan). No string concatenation of translated fragments —
every message is a whole sentence/phrase with named placeholders
(`{count}`, `{path}`), because word order varies by language and plan
32's own copy (`"expires 312 snapshots, returns ≈ 41 GiB after GC"`) is
exactly the kind of multi-value sentence that breaks under naive
concatenation. Dates/times/byte sizes use locale-aware formatting
(`Intl.NumberFormat`/`Intl.DateTimeFormat`), replacing the hand-rolled
`bytes()`/`dur()` helpers in today's `index.html:44-77` with a proper
i18n-aware equivalent — those helpers are a reasonable v1 for a
no-build-step page but do not internationalize.

## Theming

Light and dark themes as CSS custom properties (design tokens, below),
following the system preference by default (`prefers-color-scheme`)
with an explicit override in Settings — matching today's UI's dark-only
`color-scheme:dark` (`index.html:7`) being extended to a real light
theme rather than staying dark-only. No third "high contrast" theme in
this plan; the AA contrast targets above apply to both shipped themes
directly.

## Design system

A small token + component set, not a general-purpose library:

- **Tokens:** color (semantic: `--color-danger`, `--color-warning`,
  `--color-ok`, `--color-accent`, plus neutral scale), spacing (4px
  base unit, matching today's `padding:4px 8px`-style choices already
  visible in `index.html`'s `table.compact`), radius, type scale (the
  existing `font:14px system-ui` base is kept — no custom webfont, for
  bundle size and because system-ui already renders correctly on every
  target OS).
- **Components:** table (sortable, with the `table.compact` numeric-column
  convention from today's CSS — `font-variant-numeric:tabular-nums`,
  right-aligned `.num` cells — carried forward verbatim, it is already
  correct), stat card, status dot (the `.dot.on`/`.dot.off` pattern,
  now themed for light mode too), confirm dialog (the one destructive-action
  primitive every screen reuses), toast/notification banner, chip (for
  KEPT BY tags and role badges), treemap (Usage), SVG timeline
  (Snapshots' retention simulator, plan 32 §7.4), line/bar chart
  (Performance).
- Built once in a `ui/src/lib/components/` library with Storybook-free
  visual tests (Playwright screenshot tests double as the component
  catalog's regression suite — see Testing, no separate Storybook
  dependency for a component set this size).

## Performance budgets

| Metric | Budget | Rationale |
|---|---|---|
| Initial bundle (gzipped JS+CSS, excluding fonts — none shipped) | ≤ 300 KB | Svelte 5's compiled output has no runtime tax; this is generous headroom over what a 14-screen SPA with charts typically needs and keeps `tauri-driver` smoke-test cold start fast |
| Time to interactive (first paint to Overview screen usable), desktop | ≤ 1.5 s on a cold Tauri launch | The webview itself starts near-instantly (no Chromium bootstrap, unlike Electron); this budget is almost entirely the SPA's own hydration |
| Time to interactive, headless web mode (post-auth) | ≤ 2 s over loopback HTTP | Slightly looser than the Tauri path since it pays real HTTP round trips for the initial bundle |
| Chart frame rate (Performance screen, Snapshots timeline) | 60 fps sustained with ≤ 5,000 rendered points; graceful degradation (level-of-detail downsampling, not frame drops) beyond that | Matches plan 32 §7.4's live-updating simulator requirement; 5,000 points covers a dense policy's steady-state count (plan 32's own worked examples top out at 510) with headroom |
| Memory, idle (Tauri app, one window, connected) | ≤ 150 MB resident | An order of magnitude under a typical Electron app's idle footprint; achievable because there is no bundled browser engine |
| Memory, Browser screen on a 100k-entry directory listing | ≤ 250 MB, via virtualized list rendering (never materialize every row's DOM node) | Directory sizes in this codebase's own harness scenarios reach 100k+ files; the UI must not choke on what the filesystem already handles |

Budgets are gated in CI (bundle size via a `du` check on the build
output, TTI and frame rate via the Playwright/`tauri-driver` smoke
suite with `performance.now()` instrumentation) starting at U4 (once
there is enough UI to measure meaningfully) and enforced (not just
measured) from U8 onward.

## Testing

- **Unit (vitest):** Svelte components in isolation, the control-client
  wrapper (mocked transport), i18n message interpolation, the retention-timeline
  rendering logic (plan 32's simulator output → SVG, pure function,
  easy to unit test without a daemon).
- **Contract test: generated types are current.** CI runs the `ts-rs`
  export (`cargo test -p constellation-control --features ts-rs
  export_bindings`) and fails on any diff against the committed
  `.ts` files — the same "generated file must be current" pattern
  plan 31 uses for its `schemars`-generated JSON Schema.
- **e2e (Playwright) against headless web mode with a `MockControlServer`.**
  A small in-repo Rust (or TS, whichever the U0 spike finds faster to
  iterate on) server implementing the `StatusSource`/control-dispatch
  surface with scripted, deterministic responses — no real daemon, no
  S3, no docker. This is the primary e2e lane: fast, hermetic, runs on
  every PR.
- **e2e against a real daemon, Linux only.** A second Playwright lane
  against `constellation ui --web` fronting a real daemon from
  `tests/smoke.sh`'s local-backend setup — catches drift between the
  mock and reality; runs on merge to the plan's integration branch, not
  on every PR (cost/signal tradeoff).
- **`tauri-driver` WebDriver smoke, Linux and Windows only** (verification
  b) — launches the actual packaged app and drives a handful of
  critical paths (app starts, discovers/starts the daemon, Overview
  renders, one mutating action round-trips). **No direct `tauri-driver`
  lane on macOS**; macOS desktop-app coverage comes from the Playwright
  headless-web-mode lane (the same SPA build) plus manual/beta-channel
  smoke testing before a macOS release, documented here as an accepted
  gap rather than silently skipped — U8's CI job list below reflects
  this explicitly (no `tauri-driver-macos` job exists).
- **Visual regression.** Playwright screenshot comparisons per
  component and per screen, both themes, at three widths (desktop,
  tablet — 36's Android tablet target, mobile). This is also the
  component catalog (see Design system).
- **Accessibility (axe).** `@axe-core/playwright` run against every
  screen in the e2e suite, zero violations at the `wcag2a`+`wcag2aa`
  rule sets gating merge from U3 onward (once there are enough screens
  to matter).
- **Control-protocol fuzzing of the new auth paths.** A `cargo-fuzz`
  target (or `proptest`, matching plan 31's stated preference for
  policy/`Code`-mapping fuzzing) over: the `control-acl.toml` parser
  (never panics, always terminates — plan 32's own parser-fuzzing
  precedent), the headless-web-mode token/CSRF exchange (malformed
  tokens, replayed tokens, wrong-origin CSRF headers all rejected, never
  panicking the server), and the pairing-ticket exchange (a malformed
  or replayed ticket is refused, never accepted, never crashes the
  accept loop). This directly targets the *new* attack surface this
  plan adds — the existing `guard_rebinding` logic already has its own
  unit tests (web.rs:614-659) and is not re-fuzzed here, only reused.

## Packaging

The UI app is **optional and entirely separate** from the `constellation`
binary, which stays headless and statically linked on Linux (unchanged
from today). Packaging outputs, per verification (a6):

| OS | Format(s) | Notes |
|---|---|---|
| Linux | `.deb`, `.rpm`, AppImage | All three from one `tauri build`; AppImage is the no-install-required path, useful for the same "just run it" audience the static `constellation` binary already serves |
| macOS | signed + notarized `.dmg` (and the `.app` it wraps) | Signing/notarization credentials are a U8 CI-secrets concern, not a design decision; the plan assumes they exist by U8 (an org-level Apple Developer account), and if they do not, U8's report says so plainly rather than shipping unsigned |
| Windows | `.msi` and/or NSIS `.exe` (both available; U8 picks one as the primary download and keeps the other as an alternate, based on 35's own installer conventions) | |
| Android (36) | APK (sideload/testing) and AAB (Play Store) via `tauri android build --apk`/`--aab`, **not** part of `bundle.targets` | Built and owned by 36's milestones; this plan only defines the SPA and command surface 36 packages |
| iOS (future ports section) | via `tauri ios build` | Out of scope for U0–U8; reserved for a future plan per the design brief's "Future ports" section |

An optional updater (Tauri's own updater plugin) is deferred past U8 —
noted as a future milestone, not designed here, because it needs a
signing-key and release-channel decision that belongs with whichever
plan owns the release process, not the UI plan.

## CI

```yaml
jobs:
  ui-lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - run: npm ci --prefix ui
      - run: npm run lint --prefix ui
      - run: npm run check --prefix ui   # svelte-check

  ui-test:
    runs-on: ubuntu-latest
    needs: ui-lint
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - run: npm ci --prefix ui
      - run: npm run test:unit --prefix ui   # vitest

  ts-bindings-current:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test -p constellation-control --features ts-rs export_bindings
      - run: git diff --exit-code -- crates/control/bindings ui/src/lib/generated

  e2e-web:
    runs-on: ubuntu-latest
    needs: ui-test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - uses: dtolnay/rust-toolchain@stable
      - run: npm ci --prefix ui
      - run: npx playwright install --with-deps chromium
      - run: cargo build --workspace
      - run: npm run test:e2e:web --prefix ui   # against MockControlServer

  e2e-web-real-daemon:
    runs-on: ubuntu-latest
    needs: e2e-web
    if: github.event_name != 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - uses: dtolnay/rust-toolchain@stable
      - run: npm ci --prefix ui
      - run: npx playwright install --with-deps chromium
      - run: cargo build --release --workspace
      - run: bash tests/smoke.sh --with-ui-web
      - run: npm run test:e2e:web:real --prefix ui

  axe-a11y:
    runs-on: ubuntu-latest
    needs: e2e-web
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - run: npm ci --prefix ui
      - run: npx playwright install --with-deps chromium
      - run: npm run test:a11y --prefix ui

  visual-regression:
    runs-on: ubuntu-latest
    needs: e2e-web
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - run: npm ci --prefix ui
      - run: npx playwright install --with-deps chromium
      - run: npm run test:visual --prefix ui -- --update-snapshots=missing

  acl-fuzz:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: nightly
      - run: cargo install cargo-fuzz --locked
      - run: cargo fuzz run control_acl_toml -- -max_total_time=120
        working-directory: crates/control

  tauri-build:
    needs: [ui-lint, ui-test]
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: ubuntu-latest
            args: ""
          - os: macos-latest
            args: ""
          - os: windows-latest
            args: ""
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - uses: dtolnay/rust-toolchain@stable
      - name: install Linux webview deps
        if: matrix.os == 'ubuntu-latest'
        run: |
          sudo apt-get update
          sudo apt-get install -y libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf
      - run: npm ci --prefix ui
      - run: npm run build --prefix ui
      - uses: tauri-apps/tauri-action@v0
        with:
          projectPath: ui
          args: ${{ matrix.args }}

  desktop-smoke-linux:
    needs: tauri-build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - uses: dtolnay/rust-toolchain@stable
      - run: sudo apt-get update && sudo apt-get install -y libwebkit2gtk-4.1-dev webkit2gtk-driver xvfb
      - run: npm ci --prefix ui
      - run: npm run build --prefix ui
      - run: xvfb-run -a npm run test:tauri-driver --prefix ui

  desktop-smoke-windows:
    needs: tauri-build
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: "22"
      - uses: dtolnay/rust-toolchain@stable
      - run: npm ci --prefix ui
      - run: npm run build --prefix ui
      - run: npm run test:tauri-driver --prefix ui
```

Notes on the file above: `e2e-web-real-daemon` is gated off pull
requests (`if: github.event_name != 'pull_request'`) to keep PR CI fast
and hermetic, running instead on merge to the integration branch, per
the cost/signal tradeoff stated in Testing. There is no
`desktop-smoke-macos` job, matching the documented `tauri-driver` gap
(verification b) — macOS app coverage is the `e2e-web` lane (same SPA
build, headless web mode) plus manual pre-release smoke, not a silent
omission.

## Milestones

Every milestone keeps the CONVENTIONS gates green on Linux (the `cli`/`api`/`control`
crates are still part of `cargo test --workspace`), plus the
UI-specific gates listed per milestone. "Optional" milestones (U7) may
ship after U8 without blocking it, but U8's CI must still pass with U7
absent (the remote-management code paths compile out cleanly, or are
simply unexercised if always compiled in but never paired — U0 decides
which and records it).

- **U0 — Tech spike plus design system/wireframes.**
  - Stand up a minimal Tauri 2 shell on Linux, talking to a real
    daemon's control socket (via plan 31 C5's `UnixSocket` transport,
    or, if C5 is not yet landed, directly against today's
    `crates/api` line-JSON protocol as a throwaway shim — recorded
    explicitly as throwaway in the report).
  - Re-verify claims (a1) and (d) against the actual pinned
    dependency versions (not a web search) and record them in the U0
    results section below; re-litigate the ts-rs/typeshare choice only
    if (d) no longer holds.
  - Spike the `constellation-control` third-ALPN accept path against
    `crates/net`'s existing two-ALPN `Router` pattern (verification g)
    to confirm no structural surprise before U7 is scheduled for real.
  - Produce wireframes for all 14 screens (low-fidelity, enough to
    validate the screen list and role assignments above against a
    real operator's mental model — a short user test with at least one
    person outside the writing session).
  - Build the design-token set and the first three design-system
    components (table, stat card, status dot) as the visual-regression
    baseline.
  - Gate: the spike app performs one real mutating round trip
    (`Ping`, then one `operator`-role action) against a locally running
    daemon; wireframes and token set committed; U0 results section
    filled in below.

- **U1 — Control security hardening.**
  - `control-acl.toml` parser + loader (+ round-trip test, + fuzz
    target), role resolution with the "highest matching grant wins"
    rule, the audit log (schema, rotation, `node.audit.tail/query`), and
    the `kind = "service"` grant (socket-scoped principal, § Service
    principals) plan 37's CSI node plugin is the first consumer of.
  - Headless web mode's token/cookie/CSRF exchange replacing today's
    unauthenticated `/api`; `guard_rebinding` kept and re-tested.
  - Depends on plan 31 C5 existing (the transport-level peer-cred
    authz and per-method role declarations); if C5 has not landed when
    U1 starts, U1 implements the allowlist/audit/token layers against
    today's `crates/api` directly and plan 31 C5's later landing is a
    mechanical rebase (the role/principal shapes are additive, not a
    redesign).
  - Gate: CONVENTIONS gates; `acl-fuzz` CI job green; a manual
    pen-test-style check that a non-allowlisted local user's socket
    connection is refused, and that a DNS-rebinding attempt against
    headless web mode is refused before the token exchange runs.

- **U2 — App shell plus daemon discovery/start.**
  - The Tauri project skeleton, the command surface (`control_call`,
    `control_subscribe`, `control_cancel`, `daemon_discover`,
    `daemon_start`, `toggle_start_at_login` stub for 35/36), the
    capability/permission JSON restricting the webview to exactly this
    surface, the strict CSP.
  - systemd user unit + LaunchAgent generation and enable/load.
  - Gate: `ui-lint`, `ui-test`, `ts-bindings-current` CI jobs green;
    app launches on a machine with no daemon running and successfully
    starts one via the UI.

- **U3 — Core screens: overview, filesystems, mounts.**
  - Screens 1–3 fully implemented against real control methods.
  - Old `webui/index.html`'s dashboard/peers summary functionality has
    a Svelte equivalent (peers itself lands with U9... no — peers is
    Screen 9, landing at U6; U3 only needs Overview's summary, not the
    full peer table).
  - Gate: `e2e-web`, `axe-a11y`, `visual-regression` CI jobs green for
    these three screens; accessibility keyboard-complete check passes
    manually.

- **U4 — Browser plus transfers.**
  - Screen 4 fully implemented, `Chunk`-frame streaming both
    directions, virtualized large-directory rendering.
  - Performance budgets start being measured (not yet enforced) in CI
    from this milestone.
  - Gate: as U3, plus the 100k-entry-directory memory budget measured
    and within the table above (enforcement starts at U8).

- **U5 — Snapshots, usage, pins, prune, GC, fsck.**
  - Screens 5–8 implemented. Screen 5 follows "Coordination with plan
    32" below for whichever of the two plans has landed first at the
    time U5 starts.
  - Gate: as U3/U4; if plan 32 has not landed, Screen 5's
    policy-editor/simulator/space sections ship as documented
    placeholders (see below), not missing entirely and not faked with
    client-side logic.

- **U6 — Performance, peers, logs, audit.**
  - Screens 9, 10, 12, 14. Tray/menu bar and notifications also land
    here (they depend on Overview's health signals, screen 1, and the
    peer/lease signals from screens 9/11 being real).
  - Gate: as prior UI milestones, plus the 60fps/5,000-point chart
    budget measured.

- **U7 — Remote management (optional).**
  - The `constellation-control` ALPN accept path, pairing flow (QR +
    ticket text), Screen 13's remote-devices management, revocation.
  - Gate: CONVENTIONS gates; the pairing/ticket fuzz target from
    `acl-fuzz` extended to cover the new exchange; a harness or
    in-process multi-node test proving a revoked device's next request
    is refused and its pooled connection is closed.

- **U8 — Packaging and CI.**
  - The full `tauri-build` matrix, `desktop-smoke-linux`/`-windows`,
    signing/notarization for macOS (or an explicit report that it is
    blocked on org credentials), the `.deb`/`.rpm`/AppImage/`.dmg`/`.msi`-or-NSIS
    outputs.
  - Performance budgets move from measured to enforced in CI.
  - Gate: every CI job in the YAML above green on a real PR; a
    downloaded, installed build of the app on each of Linux/macOS/Windows
    launches and completes the Screen-1-through-3 smoke path by hand
    once per OS, recorded in the milestone's report.

## Risks

- **`d` (ts-rs vs. typeshare) is REPORTED, not independently verified
  against source.** Mitigated by re-checking in U0 before any code
  depends on the choice, and by the choice being reversible in isolation
  (it only affects the type-generation step, not the wire protocol or
  any other design decision in this plan).
- **`tauri-driver` cannot smoke-test the packaged macOS app directly**
  (verification b). Mitigated by the `e2e-web` lane covering the same
  SPA build and by an explicit manual pre-release smoke step; not
  mitigated to full parity with Linux/Windows automated coverage — this
  is a real, accepted gap, stated plainly rather than glossed over.
- **Two sessions touch `crates/api/src/types.rs` at once (this plan and
  32).** Mitigated by both plans only *adding* enum variants/struct
  fields (never renaming or removing existing ones, per plan 31's
  "additive" convention already visible in today's `ListSnapshots`
  coexisting with `SnapshotList`) — see "Coordination with plan 32" for
  the explicit merge protocol.
- **The allowlist config is a new place to misconfigure a production
  system into either lockout (admin can't reach their own daemon) or
  overexposure (a too-broad group grant).** Mitigated by "the daemon's
  owner is always admin, unconditionally" (lockout of the *owner*
  specifically is structurally impossible) and by the audit log making
  overexposure discoverable after the fact; not mitigated against a
  deliberately-broad grant an admin chooses to write — that is a policy
  choice this plan does not second-guess.
- **Remote management (U7) widens the attack surface of every daemon
  that compiles it in**, even before pairing, because the ALPN accept
  handler exists and must safely refuse an unpaired connection. Mitigated
  by the pairing secret being freshly random per pairing attempt, one-time,
  and by the fuzz target explicitly covering malformed/replayed pairing
  exchanges (see U7's gate); further mitigated by U7 being the one
  milestone explicitly allowed to ship after U8 if its review needs
  more time.
- **Bundle-size and performance budgets were set from first principles
  (Svelte 5's known compiled-output characteristics), not measured
  against this specific 14-screen app before U0.** Mitigated by
  budgets being *measured* starting U4 and only *enforced* starting
  U8, leaving four milestones of real data to catch a budget that
  turns out unrealistic before it blocks anyone's CI.

## Coordination with plan 32

Plan 32 is owned by another session and this plan does not edit it or
its target file. Both plans, however, touch the same three files
(`crates/api/src/types.rs`, `lib.rs`, `web.rs`) and plan 32 explicitly
specifies a new web-UI page (`crates/api/webui/snapshots.html` in its
Step 7) that this plan's SPA must eventually host as Screen 5. The
coordination protocol, regardless of landing order:

1. **`crates/api`'s `Request`/`Response` enums are not a stable target
   for this plan.** Plan 32 §7.6 adds its own variants "additive… as
   established" in the existing `snake_case`-tagged style, on top of
   today's `crates/api`. Plan 31 §2 ("no backward compatibility")
   deletes `crates/api` outright in its C5 — a breaking replacement,
   not an additive one. This plan's U1 (allowlist/audit) and U7 (remote
   management) add no new old-style `Request`/`Response` variants of
   their own and target `constellation-control`'s methods directly, so
   they carry no old-protocol-side conflict. But **whichever of {plan
   31's C5, plan 32} lands second is not a mechanical rebase**: if plan
   32 lands first and adds `crates/api` variants (and its
   `webui/snapshots.html` page) that C5 has not yet mapped to a
   control-protocol method, C5's author must add plan 32's methods to
   the control-protocol method table (§9.2-equivalent) by hand before
   deleting `crates/api`, rather than assuming the two diffs merge
   without a decision. Plan 31's C5 now settles this ("Plan 32
   overlap"): whichever lands second carries the other's work, and no
   plan-32 method may be lost when `crates/api` is deleted.
2. **If plan 32 lands before this plan's U5:** Screen 5 is built
   directly against plan 32's real `snap_policy_*`/`snapshot_*`
   methods and its `SnapshotStatus` field additions (`origin`,
   `policy_ino`, `held`, `kept_by`, `expires_unix_ms`, `used`,
   `written`, `refer`, `lsize`, `as_of_seq`, per plan 32 §7.6). The
   vanilla-JS `webui/snapshots.html` plan 32 built is ported
   feature-for-feature into Svelte components (space-overview bar,
   policy cards, the tier-row editor with text/GUI sync, the retention
   timeline SVG, the snapshot table with inline USED/WRITTEN bars and
   KEPT BY chips) — plan 32's own rule that "the UI holds no retention
   or accounting logic" (§7) carries over unchanged: the Svelte
   components render what `snap_policy_simulate`/`snapshot_space`
   return, never reimplementing retention math client-side, exactly as
   plan 32 requires of its own vanilla-JS version.
3. **If this plan's U5 is reached before plan 32 lands:** Screen 5
   ships in two parts. The list/create/delete/clone/`snap refs`
   portion (today's methods, `types.rs:93-113`) is fully real. The
   policy-editor, simulator and space-accounting portion ships as a
   **documented placeholder**: the screen's layout, routing, and the
   tier-row editor's client-side form logic (validation of the
   calendar-dividing interval set, the text ↔ rows two-way sync) are
   built against plan 32 §1's grammar and §7.3's editor spec *as
   written in that plan's file* (read-only reference, not
   reimplementation of behavior — the placeholder's "Save" button is
   disabled with a "coming with the snapshot-policies feature" note,
   never wired to a fabricated client-side retention engine). This
   keeps the UI's information architecture stable so that when plan
   32's methods do land, only the API calls need wiring — no screen
   redesign.
4. **Either order, the last plan to land runs the full parity test
   suite** (this plan's `e2e-web`, `axe-a11y`, `visual-regression` for
   Screen 5, and plan 32's own `web-ui-smoke` scenario extension) as
   part of its own gate, since it is the one making the screen fully
   real.

## Definition of done

All of `docs/plans/v1/CONVENTIONS.md`'s gates, plus:

1. `cargo fmt --all` clean, `cargo clippy --workspace --all-targets -- -D warnings`
   clean, across `constellation-control`'s new `ts-rs`-feature code path
   too (clippy run once with the feature on, once with it off).
2. `cargo test --workspace` zero failures, including the `ts-bindings-current`
   contract check and the `acl-fuzz`/pairing fuzz targets run for at
   least the CI job's time budget locally before merge.
3. `npm run lint`, `npm run check` (svelte-check), `npm run test:unit`
   all green in `ui/`.
4. `e2e-web` (MockControlServer) and, on a machine with a real daemon
   available, `e2e-web-real-daemon` both green.
5. `axe-a11y` zero `wcag2a`/`wcag2aa` violations across every shipped
   screen; a manual keyboard-only pass completed and noted per
   milestone's report.
6. `desktop-smoke-linux` and `desktop-smoke-windows` (`tauri-driver`)
   green; a manual smoke pass on macOS recorded in the U8 report in
   lieu of automated `tauri-driver` coverage.
7. Performance budgets table: every metric measured and, from U8
   onward, within budget or the report explains the deviation and the
   follow-up.
8. `bash tests/smoke.sh` and `bash tests/integration.sh` (unaffected by
   this plan, but still run per CONVENTIONS) pass; `target/release/harness
   run` still fully PASSED (no UI-plan scenario should regress
   anything under `crates/harness` — this plan adds no harness
   scenarios of its own, since it has no distributed-systems behavior
   to fault-inject, only a client and a security layer).
9. `docker compose --profile test run --rm compliance` stays 8798/8798 —
   this plan changes nothing FUSE-facing, but the gate is run anyway
   per CONVENTIONS' "all of these, every plan" rule.
10. `docs/plans/v1/PROGRESS.md` updated with this milestone's rows;
    `docs/how-to-guides/development/TESTING.md` updated with the new
    CI jobs and the `MockControlServer`/`tauri-driver` lanes.

## U0 results

*(Filled in by the session that executes U0. Placeholder structure
below; do not fill in speculative numbers here — an empty table cell is
more honest than a guessed one.)*

| Item | Result |
|---|---|
| Pinned Tauri version (re-verified, not searched) | — |
| Pinned `ts-rs` version, and whether claim (d) still holds against its current docs | — |
| Wireframe review outcome (screen list/role assignments changed?) | — |
| Spike: `Ping` + one `operator` round trip over a real daemon | — |
| Spike: third-ALPN accept path against `crates/net`'s `Router` | — |
| Design tokens + first 3 components committed | — |

## Sources

| # | Source | What it grounded |
|---|---|---|
| 1 | `v2.tauri.app/release/`, `tauri.app/release/core/`, individual package release pages | Tauri 2 version currency (a1) |
| 2 | `v2.tauri.app` overview, `v2.tauri.app/start/prerequisites/` | Cross-platform reach incl. mobile (a2) |
| 3 | `v2.tauri.app/security/capabilities/`, `/security/permissions/`, `/reference/acl/capability/` | Capabilities/permissions model (a3) |
| 4 | `v2.tauri.app/security/csp/` | CSP configuration (a4) |
| 5 | `v2.tauri.app/plugin/notification/`, `github.com/tauri-apps/tray-icon`, `v2.tauri.app/reference/javascript/api/namespacetray/` | Tray and notification plugins (a5) |
| 6 | `v2.tauri.app/reference/config/` (fetched directly), `v2.tauri.app/distribute/google-play/`, `github.com/tauri-apps/tauri` issue #15419 | Bundler outputs, Android APK/AAB path (a6) |
| 7 | `v2.tauri.app/develop/tests/webdriver/` (fetched directly, exact quote) | `tauri-driver` platform support (b) |
| 8 | `webdriver.io/docs/desktop-testing/tauri/platform-support/`, `webdriver.io/docs/wdio-tauri-service/` | The WebdriverIO-service alternative path for macOS (context for b) |
| 9 | `svelte.dev/blog/svelte-5-is-alive`, `svelte.dev/blog/whats-new-in-svelte-september-2026` | Svelte 5 stability and Vite ecosystem activity (c) |
| 10 | `github.com/Aleph-Alpha/ts-rs`, `docs.rs/ts-rs`, comparison posts surfaced by search | ts-rs vs. typeshare serde-attribute coverage (d) — REPORTED, re-verify in U0 |
| 11 | `man ssh` (`-L` option forms), `www.skreutz.com/posts/unix-domain-socket-forwarding-with-openssh/`, `www.25thandclement.com/~william/projects/streamlocal.html` | `ssh -L local_socket:remote_socket` (e) |
| 12 | Apple `launchd.plist(5)` conventions as summarized by multiple secondary sources | LaunchAgent minimal structure (f) |
| 13 | `crates/net/src/endpoint.rs:958-970`, `crates/net/src/peers.rs:794-817`, `Cargo.toml:86-87` | iroh multi-ALPN endpoint/router pattern already in this repo (g) — read directly, not searched |
| 14 | `crates/api/src/lib.rs`, `types.rs`, `web.rs`, `webui/index.html`, `webui/peers.html`, `Cargo.toml` (crate) | Today's control API surface, the web adapter's security posture and its own module-doc citation of CVE-2025-49596 |
| 15 | `crates/cli/src/main.rs:140-160` | The existing `--web-ui`/`CONSTELLATION_WEB_UI_PORT` mount flag this plan's headless mode supersedes with an explicit subcommand |
| 16 | `docs/plans/v1/wip/32-snapshot-policies-and-space.md` (read-only) | The web-UI page spec (§7) this plan hosts in the SPA, and the additive `Request`/`SnapshotStatus` surface (§7.6) |
| 17 | `docs/plans/v1/wip/23-remote-support-mode.md` (read-only) | The feature this plan's remote-management milestone must stay distinct from |
| 18 | The design brief (`core-design-brief.md`, this session's scratchpad) | Fixed plan numbering, milestone IDs U0–U8/C0–C8, crate names, and the Plan 33 decisions section this plan expands |
