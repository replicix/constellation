//! The C5 gate (plan 31 §11 C5, §15 item 6): the unix socket and the HTTP
//! adapter dispatch identically, across the **whole** method table, against
//! a real engine.
//!
//! One `Engine` (local backend, P2P off) serves one `EngineControl`; a
//! fixture host "mounts" views by opening them in the engine (no kernel).
//! For each role — none, viewer, operator, admin — the same router is put
//! behind a real unix socket (the peer is this process's uid, granted the
//! role) and behind the HTTP adapter (in-process, as a principal granted
//! the same role), and every method in `METHODS` is called through both, one
//! after the other, with parameters chosen so that calling twice gives the
//! same outcome (reads; mutations that fail identically; idempotent writes).
//! Asserted per method and role:
//!
//! - the same outcome: `Ok`, or an error of the same kind and `Code`
//!   (`Denied` exactly when the role is below the method's minimum);
//! - for the read-only methods, the same result (minus clocks), streams
//!   included (`node.logs.tail` and `browse.read` bytes, the first
//!   `stats.subscribe` sample's names);
//! - the one deliberate difference: `node.handoff` is refused over HTTP
//!   whatever the role (`web::HTTP_REFUSED`), never over the socket.

use super::*;
use crate::{DeferredEvents, EngineConfig, EngineProfile, P2pMode, ViewSpec};
use constellation_control::authz::{Grant, Subject};
use constellation_control::methods::{method_info, MethodInfo, StreamKind, METHODS};
use constellation_control::proto::types::MountSource;
use constellation_control::proto::ErrorKind;
use constellation_control::transport::UnixSocketListener;
use constellation_control::{Client, Policy, Role};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_types::Code;
use futures::StreamExt;
use serde_json::{json, Value};
use std::time::Duration;
use tower::ServiceExt;

/// A host whose "mounts" are views opened in the engine.
struct FixtureHost {
    engine: Arc<Engine>,
    views: Mutex<Vec<HostView>>,
}

impl ControlHost for FixtureHost {
    fn views(&self) -> Vec<HostView> {
        self.views.lock().unwrap().clone()
    }

    fn mount(&self, p: &ViewMountParams, _fd: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
        let MountSource::Path { mountpoint, .. } = &p.source else {
            return Err(ControlError::unsupported("the fixture has no FUSE"));
        };
        let spec = ViewSpec {
            labels: p.labels.clone(),
            confine_links: p.confine_links,
            ..ViewSpec::new(p.subtree.clone())
        };
        let view = self
            .engine
            .open_view(
                spec,
                constellation_vfs::FrontendCaps::linux_fuse(false),
                DeferredEvents::new(),
            )
            .map_err(|e| ControlError::not_found(format!("{e:#}")))?;
        let hv = HostView {
            id: view.id(),
            subtree: p.subtree.clone(),
            mountpoint: mountpoint.clone(),
            since: Instant::now(),
            labels: p.labels.clone(),
            qos: p.qos.clone(),
            confine_links: p.confine_links,
            // The fixture has no FUSE (`mount` refuses a non-path source).
            fuse: None,
            view: Some(view),
        };
        self.views.lock().unwrap().push(hv.clone());
        Ok(ViewInfo {
            id: hv.id,
            subtree: hv.subtree,
            mountpoint: hv.mountpoint.display().to_string(),
            mounted_ms_ago: 0,
            labels: hv.labels,
            qos: hv.qos,
            confine_links: hv.confine_links,
        })
    }

    fn unmount(&self, mountpoint: &Path) -> Result<String, ControlError> {
        let mut views = self.views.lock().unwrap();
        let at = views
            .iter()
            .position(|v| v.mountpoint == mountpoint)
            .ok_or_else(|| {
                ControlError::not_found(format!("no view mounted at {}", mountpoint.display()))
            })?;
        let v = views.remove(at);
        if let Some(view) = v.view {
            self.engine.close_view(&view);
        }
        Ok(format!("unmounted {}", mountpoint.display()))
    }

    fn detach_all(&self) {}

    fn handover_status(&self) -> HandoverStatus {
        HandoverStatus::default()
    }

    fn handoff(
        &self,
        _: &HandoffParams,
        _: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        Err(ControlError::unsupported(
            "the fixture cannot hand sessions over",
        ))
    }
}

/// What an admin call with [`params_for`] must answer `Ok` (so the sweep
/// compares real results, not only matching refusals). Not `quota.set`:
/// its barrier takes the write lease, which this offline engine's sync task
/// does not grant here — it fails identically on both transports.
const MUST_SUCCEED: &[&str] = &[
    "node.ping",
    "node.status",
    "node.logs.tail",
    "node.ops",
    "node.doctor",
    "pin.list",
    "designation.list",
    "designation.list_delegations",
    "prune.list",
    "gc.run",
    "fsck.run",
    "snapshot.list",
    "snapshot.policy.check",
    "snapshot.policy.simulate",
    "snapshot.policy.list",
    "snapshot.policy.set",
    "snapshot.resolve",
    "snapshot.delete_many",
    "snapshot.sched.status",
    "snapshot.sched.run",
    "browse.readdir",
    "browse.inspect",
    "browse.stat",
    "browse.read",
    "browse.write",
    "browse.mkdir",
    "browse.xattr",
    "cache.list",
    "cache.prune",
    "quota.get",
    "view.list",
    "view.stats",
    "peers.list",
    "stats.subscribe",
    "events.subscribe",
    "fs.list",
    "fs.create",
];

/// Mutations whose first call changes what an identical second one finds.
const STATEFUL: &[&str] = &["locks.force_release"];

/// Parameters for which a second identical call answers like the first.
fn params_for(name: &str, root_view: u64, backend_dir: &str) -> Value {
    match name {
        "node.leave" => json!({"node_id": 987_654_321u64}),
        "node.set_write_mode" => json!({"mode": "sideways"}),
        "node.logs.tail" => json!({"lines": 5}),
        "node.ops" => json!({}),
        "node.handoff" => json!({}),
        "node.lifecycle" => json!({"event": "Foreground"}),
        "pin.add" | "pin.remove" | "designation.undelegate" | "designation.online" => {
            json!({"path": "/no-such-dir"})
        }
        "designation.offline" => json!({"path": "/no-such-dir"}),
        "designation.delegate" => json!({"path": "/no-such-dir", "node": 2}),
        "prune.run" => json!({"path": "/no-such-dir"}),
        "gc.run" => json!({"verify_only": true}),
        "fsck.run" => json!({}),
        "snapshot.create" => json!({"selector": "no-selector-here"}),
        "snapshot.list" => json!({}),
        "snapshot.delete" => json!({"selector": "/@no-such-snapshot"}),
        "snapshot.refs" => json!({"id": "no-such-id"}),
        "snapshot.hold" => json!({"id": "/@no-such-snapshot", "held": true}),
        // Nothing to name in this empty filesystem: both answer `Ok` with
        // nothing in them (a dry run, so no batch either).
        "snapshot.resolve" => json!({"selectors": []}),
        "snapshot.delete_many" => json!({"selectors": [], "dry_run": true}),
        "clone.create" => json!({"selector": "/@no-such-snapshot", "destination": "/c"}),
        // Over the root's (empty) history, simulated for one hour: two
        // creations whatever the clock, so both calls agree.
        "snapshot.policy.check" => {
            json!({"expr": "1d:7d 1h:1d", "against": "/", "simulate_ms": 3_600_000u64})
        }
        "snapshot.policy.simulate" => {
            json!({"path": "/", "expr": "1h:1d", "horizon_ms": 3_600_000u64})
        }
        // The root carries no policy until admin's `set` binds one (the
        // same expression every time, over the root's empty history: it
        // expires nothing, so no confirmation). `remove`/`pause` name a
        // missing directory, so both calls fail alike.
        "snapshot.policy.show" => json!({"path": "/no-such-dir"}),
        "snapshot.policy.set" => json!({"path": "/", "expr": "1d:7d 1h:1d"}),
        "snapshot.policy.remove" => json!({"path": "/no-such-dir"}),
        "snapshot.policy.pause" => json!({"path": "/no-such-dir", "paused": true}),
        // A dry run: it takes no lease and creates nothing, so the second
        // call finds what the first did.
        "snapshot.sched.status" => json!({}),
        "snapshot.sched.run" => json!({"dry_run": true}),
        "browse.readdir" | "browse.inspect" | "browse.stat" => json!({"path": "/"}),
        "browse.read" => json!({"path": "/f"}),
        "browse.write" => {
            json!({"path": "/f", "data": "cGFyaXR5", "create": true, "truncate": true})
        }
        "browse.mkdir" => json!({"path": "/d/e", "parents": true}),
        "browse.rename" => json!({"from": "/no-such", "to": "/x"}),
        "browse.delete" => json!({"path": "/no-such"}),
        "browse.xattr" => json!({"path": "/f", "op": "List"}),
        "locks.force_release" => json!({"part": "p0"}),
        "locks.drop_held" => json!({"ino": 987_654_321u64}),
        "cache.prune" => json!({"target_bytes": u64::MAX}),
        "quota.set" => json!({"max_bytes": null}),
        "view.mount" => {
            json!({"subtree": "/no-such-dir", "source": {"Path": {"mountpoint": "/parity/x"}}})
        }
        "view.unmount" => json!({"mountpoint": "/parity/none"}),
        "view.list" => json!({"labels": {"role": "root"}}),
        "view.stats" => json!({"id": root_view}),
        "stats.subscribe" => json!({"interval_ms": 100}),
        "events.subscribe" => json!({"topics": ["view"]}),
        "fs.create" => json!({"bucket": backend_dir}),
        "fs.import" => json!({"document": "not = [toml"}),
        "fs.export" => json!({"fs": "no-such-fs"}),
        "fs.passwd" => json!({"fs": "no-such-fs", "old_passphrase": "a", "new_passphrase": "b"}),
        "fs.doctor" => json!({"fs": "no-such-fs"}),
        "fs.unlock" => json!({"fs": "no-such-fs", "credentials": {}}),
        _ => json!({}),
    }
}

/// What one call came to: `Ok` with a comparable value, or the error's
/// kind and code.
#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    Ok(Value),
    Err(ErrorKind, Option<Code>),
}

impl Outcome {
    fn of(result: Result<Value, ControlError>) -> Outcome {
        match result {
            Ok(v) => Outcome::Ok(v),
            Err(e) => Outcome::Err(e.kind, e.code),
        }
    }
}

/// Drop what legitimately differs between two calls a moment apart.
fn stable(method: &str, value: Value) -> Value {
    fn strip(v: &mut Value, keys: &[&str]) {
        match v {
            Value::Object(map) => {
                for k in keys {
                    map.remove(*k);
                }
                for child in map.values_mut() {
                    strip(child, keys);
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|c| strip(c, keys)),
            _ => {}
        }
    }
    let mut value = value;
    match method {
        // The report is live counters: compare its identity and shape.
        "node.status" => {
            let keys: Vec<String> = value
                .as_object()
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            json!({"keys": keys, "fs_uuid": value["fs_uuid"], "node_id": value["node_id"],
                   "version": value["version"], "mounts": value["mounts"].as_array().map(Vec::len)})
        }
        // Samples: the same series, not the same values.
        "stats.subscribe" => {
            json!({"counters": value["counters"].as_object().map(|m| m.keys().cloned().collect::<Vec<_>>()),
                   "gauges": value["gauges"].as_object().map(|m| m.keys().cloned().collect::<Vec<_>>())})
        }
        // The daemon's clock places the synthetic ticks: compare what
        // the expression and the history decide, not when.
        "snapshot.policy.simulate" => {
            json!({"policy": value["policy"], "policy_ino": value["policy_ino"],
                   "cadence": value["cadence"], "created": value["created"],
                   "steady_state_bound": value["steady_state_bound"],
                   "truncated": value["truncated"]})
        }
        // The scheduler's counters move with every call (`ticks`), and
        // its clock decides the bucket: compare what the replica says.
        "snapshot.sched.status" => {
            json!({"node_id": value["node_id"], "enabled": value["enabled"],
                   "roots": value["roots"].as_array().map(|roots| roots.iter()
                       .map(|r| json!({"ino": r["ino"], "path": r["path"], "expr": r["expr"],
                                       "canonical": r["canonical"], "paused": r["paused"]}))
                       .collect::<Vec<_>>())})
        }
        "snapshot.sched.run" => {
            json!({"dry_run": value["dry_run"], "refused": value["refused"],
                   "roots": value["roots"].as_array().map(|roots| roots.iter()
                       .map(|r| json!({"ino": r["ino"], "path": r["path"], "outcome": r["outcome"]}))
                       .collect::<Vec<_>>())})
        }
        // How far the simulation got is measured from the daemon's
        // clock, which a non-aligned `now` shifts by the call's delay.
        "snapshot.policy.check" => {
            strip(&mut value, &["simulate_reached_ms"]);
            value
        }
        // Log lines and probe objects are written between the two calls.
        "node.logs.tail"
        | "node.doctor"
        | "cache.list"
        | "gc.run"
        | "fsck.run"
        | "node.reintegrate"
        | "locks.force_release"
        | "cache.prune"
        | "fs.doctor"
        | "fs.create" => Value::Null,
        _ => {
            strip(
                &mut value,
                &[
                    "mounted_ms_ago",
                    "uptime_s",
                    "unix_ms",
                    "last_seen_ms",
                    "coop",
                    "atime_ns",
                ],
            );
            value
        }
    }
}

/// One call over the unix socket.
async fn over_socket(client: &Client, m: &MethodInfo, params: Value) -> Outcome {
    match m.streaming {
        StreamKind::None => Outcome::of(client.call_json(m.name, params).await),
        StreamKind::Chunks => {
            let result = match m.name {
                "node.logs.tail" => {
                    let p = serde_json::from_value(params).unwrap();
                    match client.call_chunks::<NodeLogsTail>(p).await {
                        Ok(s) => s.collect_bytes().await,
                        Err(e) => Err(e),
                    }
                }
                _ => {
                    let p = serde_json::from_value(params).unwrap();
                    match client.call_chunks::<BrowseRead>(p).await {
                        Ok(s) => s.collect_bytes().await,
                        Err(e) => Err(e),
                    }
                }
            };
            Outcome::of(result.map(|b| json!(String::from_utf8_lossy(&b))))
        }
        StreamKind::Events => {
            let result = match m.name {
                "stats.subscribe" => {
                    let p = serde_json::from_value(params).unwrap();
                    match client.subscribe::<StatsSubscribe>(p).await {
                        Ok(mut s) => match s.next().await {
                            Some(Ok(sample)) => Ok(serde_json::to_value(sample).unwrap()),
                            Some(Err(e)) => Err(e),
                            None => Ok(Value::Null),
                        },
                        Err(e) => Err(e),
                    }
                }
                // Nothing happens while the test waits: that it opened is
                // the outcome.
                // A refusal is the stream's first item; a subscription
                // that is open simply has nothing yet.
                _ => {
                    let p = serde_json::from_value(params).unwrap();
                    match client.subscribe::<EventsSubscribe>(p).await {
                        Ok(mut s) => {
                            match tokio::time::timeout(Duration::from_millis(500), s.next()).await {
                                Ok(Some(Err(e))) => Err(e),
                                _ => Ok(json!("subscribed")),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
            };
            Outcome::of(result)
        }
    }
}

/// One call through the HTTP adapter.
async fn over_http(app: &axum::Router, m: &MethodInfo, params: Value) -> Outcome {
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/api")
        .header("host", "127.0.0.1")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({"method": m.name, "params": params}).to_string(),
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let json_body = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|t| t.starts_with("application/json"));
    if !status.is_success() || json_body {
        let body = axum::body::to_bytes(response.into_body(), 64 << 20)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        if let Some(error) = value.get("error") {
            let e: ControlError = serde_json::from_value(error.clone()).unwrap();
            assert_eq!(
                status,
                constellation_control::web::status_of(e.kind),
                "{}: HTTP status vs error kind",
                m.name
            );
            return Outcome::Err(e.kind, e.code);
        }
        return Outcome::Ok(value["ok"].clone());
    }
    let mut body = response.into_body().into_data_stream();
    match m.streaming {
        StreamKind::Chunks => {
            let mut bytes = Vec::new();
            while let Some(chunk) = body.next().await {
                bytes.extend_from_slice(&chunk.unwrap());
            }
            Outcome::Ok(json!(String::from_utf8_lossy(&bytes)))
        }
        _ if m.name == "stats.subscribe" => {
            let mut line = Vec::new();
            while !line.contains(&b'\n') {
                match body.next().await {
                    Some(chunk) => line.extend_from_slice(&chunk.unwrap()),
                    None => break,
                }
            }
            let end = line.iter().position(|b| *b == b'\n').unwrap_or(line.len());
            Outcome::Ok(serde_json::from_slice(&line[..end]).unwrap())
        }
        _ => Outcome::Ok(json!("subscribed")),
    }
}

#[test]
fn unix_socket_and_http_dispatch_the_whole_table_identically() {
    let root = tempfile::tempdir().unwrap();
    // `fs.*` reads the host registry: a private one.
    // SAFETY: set before any other thread of this test reads it; the key
    // is only read by the registry code this test drives.
    unsafe {
        std::env::set_var("CONSTELLATION_REGISTRY", root.path().join("registry.toml"));
    }
    let backend_dir = root.path().join("backend");
    let backend = format!("file://{}", backend_dir.display());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    {
        let store = ChunkStore::new(
            rt.block_on(crate::backend::open_backend(&backend))
                .expect("open backend"),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .expect("create_fs");
    }
    let engine = Arc::new(
        Engine::start(
            EngineConfig {
                state_dir: Some(root.path().join("state")),
                cache_size: 64 * 1024 * 1024,
                runtime: Some(rt.handle().clone()),
                ..EngineConfig::new(&backend)
            },
            HostServices::native(),
            EngineProfile {
                p2p: P2pMode::Off,
                ..EngineProfile::desktop()
            },
        )
        .expect("Engine::start"),
    );
    let host = Arc::new(FixtureHost {
        engine: engine.clone(),
        views: Mutex::new(Vec::new()),
    });
    let root_view = host
        .mount(
            &ViewMountParams {
                subtree: "/".into(),
                source: MountSource::Path {
                    mountpoint: "/parity/root".into(),
                    opts: Default::default(),
                },
                labels: [("role".to_string(), "root".to_string())].into(),
                qos: Default::default(),
                confine_links: false,
            },
            None,
        )
        .unwrap()
        .id;
    let prefetch = host.views()[0].view.as_ref().unwrap().prefetch_stats();
    let svc = EngineControl::new(
        engine.clone(),
        host.clone(),
        crate::log_buffer::LogBuffer::default(),
        prefetch,
        "parity",
    );
    let me = constellation_platform::native().process.effective_ids().0;
    let web_principal = Principal::Remote {
        device: "parity-web".into(),
    };
    let backend_dir = backend_dir.display().to_string();

    // The file `browse.read` reads, written once before the sweep.
    let admin_router = router(&svc);
    rt.block_on(constellation_control::dispatch_in_process(
        &admin_router,
        &Principal::InProcess,
        "browse.write",
        json!({"path": "/f", "data": "cGFyaXR5", "create": true}),
    ))
    .expect("seed /f");

    let mut compared = 0usize;
    for role in [
        None,
        Some(Role::Viewer),
        Some(Role::Operator),
        Some(Role::Admin),
    ] {
        let mut policy = Policy::in_process_only();
        if let Some(role) = role {
            policy = policy
                .with_grant(Grant {
                    subject: Subject::Uid(me),
                    role,
                })
                .with_grant(Grant {
                    subject: Subject::Device("parity-web".into()),
                    role,
                });
        }
        let router = Arc::new(super::router(&svc).with_policy(policy));
        let sock = root.path().join(format!("p-{}.sock", compared));
        let (client, app) = rt.block_on(async {
            let listener = UnixSocketListener::bind(&sock).unwrap();
            std::mem::forget(constellation_control::server::serve_router(
                listener,
                router.clone(),
            ));
            (
                Client::connect_unix(&sock).await.unwrap(),
                constellation_control::web::app_as(router.clone(), web_principal.clone()),
            )
        });
        for m in METHODS {
            let params = params_for(m.name, root_view, &backend_dir);
            let unix = rt.block_on(over_socket(&client, m, params.clone()));
            let http = rt.block_on(over_http(&app, m, params));
            let allowed = role.is_some_and(|r| r >= m.min_role);
            let what = format!("{} as {role:?}", m.name);
            if !allowed {
                assert_eq!(
                    unix,
                    Outcome::Err(ErrorKind::Denied, Some(Code::Access)),
                    "{what}: socket"
                );
            } else {
                assert_ne!(
                    unix,
                    Outcome::Err(ErrorKind::Denied, Some(Code::Access)),
                    "{what}: allowed but denied over the socket"
                );
            }
            if m.name == "node.handoff" {
                assert_eq!(
                    http,
                    Outcome::Err(ErrorKind::Denied, Some(Code::Access)),
                    "{what}: HTTP must refuse the handoff"
                );
                compared += 1;
                continue;
            }
            if role == Some(Role::Admin) && MUST_SUCCEED.contains(&m.name) {
                assert!(
                    matches!(unix, Outcome::Ok(_)),
                    "{what}: expected success over the socket: {unix:?}"
                );
            }
            if role == Some(Role::Admin) && m.name == "browse.read" {
                assert_eq!(unix, Outcome::Ok(json!("parity")), "{what}");
            }
            if role == Some(Role::Admin) && m.name == "view.list" {
                assert!(
                    matches!(&unix, Outcome::Ok(v) if v["views"].as_array().map(Vec::len) == Some(1)),
                    "{what}: the label filter keeps the root view: {unix:?}"
                );
            }
            let norm = |o: Outcome| match o {
                Outcome::Ok(v) => Outcome::Ok(stable(m.name, v)),
                e => e,
            };
            let (u, h) = (norm(unix.clone()), norm(http.clone()));
            if STATEFUL.contains(&m.name) {
                // The first call changes what the second finds (a released
                // lease is not held any more): the same authorization, and
                // both reached the handler.
                assert!(
                    allowed == !matches!(h, Outcome::Err(ErrorKind::Denied, _)),
                    "{what}: socket {unix:?} vs HTTP {http:?}"
                );
            } else if m.mutating
                && !matches!((&u, &h), (Outcome::Err(..), _) | (_, Outcome::Err(..)))
            {
                // Two successful mutations: the outcome, not the state after.
                assert!(matches!(h, Outcome::Ok(_)), "{what}: {unix:?} vs {http:?}");
            } else {
                assert_eq!(u, h, "{what}: socket {unix:?} vs HTTP {http:?}");
            }
            compared += 1;
        }
        // Plan 32 §0.4: `snapshot.hold` is an operator method, but its
        // `force` — overriding the hold's recorded owner — is an admin
        // one, and the check is on the argument, not the method. Both
        // transports go through the same handler, so both must agree.
        let hold = method_info("snapshot.hold").unwrap();
        let forced_params = json!({"id": "/@no-such-snapshot", "held": false, "force": true});
        let forced_unix = rt.block_on(over_socket(&client, hold, forced_params.clone()));
        let forced_http = rt.block_on(over_http(&app, hold, forced_params));
        for (how, forced) in [("socket", &forced_unix), ("HTTP", &forced_http)] {
            match role {
                Some(Role::Admin) => assert!(
                    !matches!(forced, Outcome::Err(ErrorKind::Denied, _)),
                    "admin may force a hold over {how}: {forced:?}"
                ),
                _ => assert!(
                    matches!(forced, Outcome::Err(ErrorKind::Denied, _)),
                    "only admin may force a hold, not {role:?}, over {how}: {forced:?}"
                ),
            }
        }
        assert_eq!(forced_unix, forced_http, "forced hold as {role:?}");

        // Plan 32 Step 5: an invalid policy is a *result* carrying the
        // byte offset (for the CLI's caret), identical on both transports
        // and for every role that may call the method.
        let check = method_info("snapshot.policy.check").unwrap();
        let invalid = json!({"expr": "1h:1d 7m:1d"});
        let invalid_unix = rt.block_on(over_socket(&client, check, invalid.clone()));
        let invalid_http = rt.block_on(over_http(&app, check, invalid));
        assert_eq!(invalid_unix, invalid_http, "invalid policy as {role:?}");
        if role.is_some() {
            match &invalid_unix {
                Outcome::Ok(v) => {
                    assert_eq!(v["ok"], json!(false), "{v}");
                    assert_eq!(v["error"]["offset"], json!(6), "{v}");
                }
                other => panic!("an invalid policy is a result, not {other:?}"),
            }
        }

        // Plan 37 §5 spells the CSI driver's call `snapshot.create{hold:
        // "csi:<content-uid>"}` — a string where the CLI sends a boolean.
        // Both spellings must reach the handler and be refused for the same
        // reason (no such path), never as a decoding error.
        let create = method_info("snapshot.create").unwrap();
        let shorthand = rt.block_on(over_socket(
            &client,
            create,
            json!({"selector": "/no-such-dir@s", "hold": "csi:content-uid"}),
        ));
        let spelled_out = rt.block_on(over_socket(
            &client,
            create,
            json!({"selector": "/no-such-dir@s", "hold": true, "held_by": "csi:content-uid"}),
        ));
        assert_eq!(
            shorthand, spelled_out,
            "both spellings of `hold` as {role:?}: {shorthand:?}"
        );
        assert!(
            !matches!(&shorthand, Outcome::Err(ErrorKind::Invalid, _)),
            "`hold` as a string must decode, not be an invalid request: {shorthand:?}"
        );
    }
    assert_eq!(compared, 4 * METHODS.len());
    assert!(method_info("node.handoff").is_some());

    // `snapshot.policy.check {against}` end to end, over real rows of the
    // root: two hourly autos of the root a day apart, a manual one, a
    // `csi:`-held auto one and another root's auto one. `1h:1d` anchored
    // on the newest keeps it and expires only the root's older, unheld
    // auto snapshot.
    let root_ino = constellation_fs_core::types::ROOT_INO;
    let day = 86_400_000i64;
    let t0 = constellation_store_s3::lease::now_unix_ms() - 3 * day;
    let auto = |id: &str, at: i64, policy_ino: u64| constellation_meta::SnapshotRow {
        origin: 1,
        policy_ino,
        ..constellation_meta::SnapshotRow::new(id, "/", id, "mtree:0:00:1", at)
    };
    let meta = engine.meta();
    meta.record_snapshot(&auto("old-auto", t0, root_ino))
        .unwrap();
    meta.record_snapshot(&auto("new-auto", t0 + 2 * day, root_ino))
        .unwrap();
    meta.record_snapshot(&constellation_meta::SnapshotRow::new(
        "manual",
        "/",
        "manual",
        "mtree:0:00:1",
        t0,
    ))
    .unwrap();
    meta.record_snapshot(&constellation_meta::SnapshotRow {
        held: true,
        held_by: Some("csi:content-uid".into()),
        ..auto("csi-held", t0 + 1, root_ino)
    })
    .unwrap();
    meta.record_snapshot(&auto("foreign", t0 + 2, root_ino + 4242))
        .unwrap();
    let checked = rt
        .block_on(constellation_control::dispatch_in_process(
            &admin_router,
            &Principal::InProcess,
            "snapshot.policy.check",
            json!({"expr": "1h:1d", "against": "/"}),
        ))
        .expect("snapshot.policy.check against /");
    let checked: api::SnapPolicyCheckResult = serde_json::from_value(checked).unwrap();
    let against = checked.against.expect("against");
    assert_eq!(
        (against.snapshots, against.would_expire),
        (5, 1),
        "{against:?}"
    );
    let expired: Vec<&str> = against
        .verdicts
        .iter()
        .filter(|v| !v.keep)
        .map(|v| v.id.as_str())
        .collect();
    assert_eq!(expired, ["old-auto"]);

    // The unix socket really did go through peer credentials: a stranger
    // policy denied the same process above; admin reached every handler.
    drop(svc);
    for view in host.views() {
        if let Some(view) = view.view {
            engine.close_view(&view);
        }
    }
    engine.shutdown().expect("clean shutdown");
    drop(rt);
}
