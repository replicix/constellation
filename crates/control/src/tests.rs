//! End-to-end tests: real servers, real clients, no daemon.
//!
//! Most tests run against both a real unix socket in a temp dir and the
//! in-process transport, in both encodings, so a divergence between
//! transports shows up as a failure here rather than in C5b.

use crate::audit::{params_digest, AuditOutcome, MemoryAuditSink};
use crate::authz::{Grant, Policy, Principal, Role, Subject};
use crate::client::{Client, ClientOptions};
use crate::methods::*;
use crate::proto::types::*;
use crate::proto::{
    Blob, Cancel, Chunk, ControlError, Empty, Encoding, ErrorKind, Event, FrameKind, Hello,
    JsonValue, Outcome, Request, Response, StreamEnd, Welcome,
};
use crate::server::{
    dispatch_in_process, dispatch_stream_in_process, serve, serve_connection, DispatchOptions,
    Router, ServeOptions, StreamItem,
};
use crate::transport::{
    Frame, InProcess, StreamTransport, Transport, UnixSocket, UnixSocketListener,
};
use bytes::Bytes;
use constellation_types::Code;
use futures::{stream, StreamExt};
use serde_json::json;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsFd;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

fn me() -> u32 {
    // SAFETY: getuid cannot fail.
    unsafe { libc::getuid() }
}

/// Flags the test observes to prove what the server side did.
#[derive(Default, Clone)]
struct Probes {
    release: Arc<Notify>,
    saw_cancel: Arc<AtomicBool>,
    stream_dropped: Arc<AtomicBool>,
    stubborn_started: Arc<AtomicBool>,
    ran: Arc<AtomicUsize>,
}

struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn test_router(p: &Probes) -> Router {
    let mut r = Router::new();
    r.register::<NodePing, _, _>(|_c, _p| async { Ok(Pong {}) });
    let ran = p.ran.clone();
    r.register::<PinAdd, _, _>(move |_c, params: PathParams| {
        let ran = ran.clone();
        async move {
            ran.fetch_add(1, Ordering::SeqCst);
            if params.path == "/missing" {
                return Err(ControlError::not_found("no such path"));
            }
            Ok(Ack::new(format!("pinned {}", params.path)))
        }
    });
    r.register::<GcRun, _, _>(|_c, params: GcRunParams| async move {
        Ok(GcReport {
            report: JsonValue(json!({"verify_only": params.verify_only, "deleted": [1, 2]})),
        })
    });
    // A slow call, released by the test.
    let release = p.release.clone();
    r.register::<QuotaGet, _, _>(move |_c, _p| {
        let release = release.clone();
        async move {
            release.notified().await;
            Ok(QuotaStatus {
                max_bytes: Some(7),
                used_bytes: 3,
            })
        }
    });
    // A call that honours cancellation.
    let saw = p.saw_cancel.clone();
    r.register::<NodeReintegrate, _, _>(move |ctx, _p| {
        let saw = saw.clone();
        async move {
            ctx.cancel.cancelled().await;
            saw.store(true, Ordering::SeqCst);
            Err(ControlError::cancelled())
        }
    });
    // A call that ignores cancellation entirely.
    let started = p.stubborn_started.clone();
    r.register::<NodeSetWriteMode, _, _>(move |_ctx, _p| {
        let started = started.clone();
        async move {
            started.store(true, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(Ack::new("finally"))
        }
    });
    // A handler that panics.
    r.register::<NodeLeave, _, _>(|_c, _p| async move {
        if true {
            panic!("boom");
        }
        Ok(Ack::new("unreachable"))
    });
    // Subscriptions.
    let dropped = p.stream_dropped.clone();
    r.register_events::<EventsSubscribe, _, _, _>(move |_c, params: EventsSubscribeParams| {
        let flag = DropFlag(dropped.clone());
        async move {
            let topic = params
                .topics
                .first()
                .cloned()
                .unwrap_or_else(|| "tick".into());
            Ok(stream::unfold(
                (0u64, flag, topic),
                |(n, flag, topic)| async move {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    let event = ControlEvent {
                        unix_ms: n,
                        topic: format!("{topic}.{n}"),
                        data: JsonValue(json!({"n": n})),
                    };
                    Some((Ok(event), (n + 1, flag, topic)))
                },
            ))
        }
    });
    r.register_events::<StatsSubscribe, _, _, _>(|_c, _p| async {
        Ok(stream::iter((0..3u64).map(|i| {
            Ok(StatsSample {
                unix_ms: i,
                counters: [("ops".to_string(), i * 10)].into(),
                gauges: [("load".to_string(), 0.5)].into(),
            })
        })))
    });
    // Chunks.
    r.register_chunks::<BrowseRead, _, _, _>(|_c, params: BrowseReadParams| async move {
        let big = Bytes::from(
            (0..2_500_000u32)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<u8>>(),
        );
        let mut items = vec![Ok(big)];
        if params.path == "/err" {
            items.push(Err(ControlError::failed("disk exploded")));
        } else {
            items.push(Ok(Bytes::from_static(b"tail")));
        }
        Ok(stream::iter(items))
    });
    // Empty, or (with `follow`) one line and then nothing until cancelled.
    r.register_chunks::<NodeLogsTail, _, _, _>(|_c, p: LogTailParams| async move {
        let lines = if p.follow {
            vec![Ok(Bytes::from_static(b"line 1\n"))]
        } else {
            Vec::new()
        };
        let tail = if p.follow {
            stream::pending().boxed()
        } else {
            stream::empty().boxed()
        };
        Ok(stream::iter(lines).chain(tail))
    });
    // A result bigger than any frame.
    r.register::<FsExport, _, _>(|_c, p: FsExportParams| async move {
        let size = if p.fs == "huge" { 9 * 1024 * 1024 } else { 3 };
        Ok(FsExportDocument {
            document: "x".repeat(size),
        })
    });
    // A handler that panics before it even returns its future.
    r.register::<ViewUnmount, _, _>(|_c, _p: ViewUnmountParams| {
        if true {
            panic!("sync boom");
        }
        async { Ok(Ack::new("unreachable")) }
    });
    // fd passing.
    r.register::<ViewMount, _, _>(|mut ctx, params: ViewMountParams| async move {
        match params.source {
            MountSource::PreopenedFd => {
                let fd = ctx.take_fd().expect("router guarantees an fd");
                let mut file = std::fs::File::from(fd);
                file.seek(SeekFrom::Start(0)).unwrap();
                let mut text = String::new();
                file.read_to_string(&mut text).unwrap();
                Ok(ViewInfo {
                    id: 1,
                    subtree: params.subtree,
                    mountpoint: format!("fd:{text}"),
                    ..Default::default()
                })
            }
            MountSource::Path { mountpoint, .. } => Ok(ViewInfo {
                id: 2,
                subtree: params.subtree,
                mountpoint: mountpoint.display().to_string(),
                labels: params.labels,
                ..Default::default()
            }),
        }
    });
    r
}

struct UnixServer {
    _dir: tempfile::TempDir,
    handle: crate::server::ServerHandle,
    path: std::path::PathBuf,
}

fn start_unix(
    router: Router,
    policy: Policy,
    audit: Arc<dyn crate::audit::AuditSink>,
) -> UnixServer {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control.sock");
    let listener = UnixSocketListener::bind(&path).unwrap();
    let handle = serve(listener, router, policy, audit);
    UnixServer {
        _dir: dir,
        handle,
        path,
    }
}

fn owner_policy() -> Policy {
    Policy::owner_only(me())
}

fn null_audit() -> Arc<dyn crate::audit::AuditSink> {
    Arc::new(crate::audit::NullAuditSink)
}

/// The transports the typed-call tests run over.
#[derive(Debug, Clone, Copy)]
enum Via {
    Unix,
    InProcess,
}

struct Rig {
    client: Client,
    probes: Probes,
    _server: Option<UnixServer>,
}

async fn rig(via: Via, encoding: Encoding) -> Rig {
    let probes = Probes::default();
    let options = match encoding {
        Encoding::Json => ClientOptions::default(),
        Encoding::Postcard => ClientOptions::postcard(),
    };
    match via {
        Via::Unix => {
            let server = start_unix(test_router(&probes), owner_policy(), null_audit());
            let client = Client::connect_unix_with(&server.path, options)
                .await
                .unwrap();
            Rig {
                client,
                probes,
                _server: Some(server),
            }
        }
        Via::InProcess => {
            let router = test_router(&probes).with_policy(owner_policy());
            let client = Client::in_process_as(router, Principal::InProcess, options)
                .await
                .unwrap();
            Rig {
                client,
                probes,
                _server: None,
            }
        }
    }
}

const ALL_VIA: [Via; 2] = [Via::Unix, Via::InProcess];
const ALL_ENC: [Encoding; 2] = [Encoding::Json, Encoding::Postcard];

async fn eventually<F: Fn() -> bool>(what: &str, f: F) {
    for _ in 0..400 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for: {what}");
}

// ---------------------------------------------------------------------------
// Handshake
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handshake_negotiates_the_clients_first_supported_encoding() {
    for via in ALL_VIA {
        let json = rig(via, Encoding::Json).await;
        assert_eq!(json.client.encoding(), Encoding::Json, "{via:?}");
        let bin = rig(via, Encoding::Postcard).await;
        assert_eq!(bin.client.encoding(), Encoding::Postcard, "{via:?}");
        let welcome = bin.client.welcome();
        assert_eq!(welcome.encoding, Encoding::Postcard);
        assert_eq!(
            welcome.roles,
            vec![Role::Viewer, Role::Operator, Role::Admin]
        );
        assert!(welcome.features.iter().any(|f| f == "fd-passing"));
        assert!(bin.client.supports_fd_passing());
        match (via, &welcome.principal) {
            (Via::Unix, Principal::Unix { uid, .. }) => assert_eq!(*uid, me()),
            (Via::InProcess, Principal::InProcess) => {}
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test]
async fn no_common_encoding_is_refused_with_a_structured_error() {
    let probes = Probes::default();
    let router = test_router(&probes).with_options(ServeOptions {
        encodings: vec![Encoding::Json],
        ..Default::default()
    });
    let err = Client::in_process_as(
        router,
        Principal::InProcess,
        ClientOptions {
            encodings: vec![Encoding::Postcard],
            ..ClientOptions::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("encoding"), "{err}");
}

#[tokio::test]
async fn a_connection_that_never_says_hello_is_dropped() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes).with_options(ServeOptions {
        handshake_timeout: Duration::from_millis(100),
        ..Default::default()
    }));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    let frame = tokio::time::timeout(Duration::from_secs(5), client_end.recv_frame())
        .await
        .expect("server must answer")
        .unwrap()
        .unwrap();
    assert_eq!(frame.kind, FrameKind::Response);
    let response: Response = Encoding::Json.from_bytes(&frame.payload).unwrap();
    match response.result {
        Outcome::Err(e) => assert_eq!(e.kind, ErrorKind::Timeout),
        Outcome::Ok(_) => panic!("expected an error"),
    }
    assert!(client_end.recv_frame().await.unwrap().is_none());
}

#[tokio::test]
async fn garbage_instead_of_hello_is_refused() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    client_end
        .send_frame(Frame::new(FrameKind::Request, Bytes::from_static(b"{}")))
        .await
        .unwrap();
    let frame = client_end.recv_frame().await.unwrap().unwrap();
    let response: Response = Encoding::Json.from_bytes(&frame.payload).unwrap();
    assert!(matches!(response.result, Outcome::Err(ref e) if e.code == Some(Code::Protocol)));
}

// ---------------------------------------------------------------------------
// Typed calls
// ---------------------------------------------------------------------------

#[tokio::test]
async fn typed_calls_round_trip_over_every_transport_and_encoding() {
    for via in ALL_VIA {
        for enc in ALL_ENC {
            let rig = rig(via, enc).await;
            let c = &rig.client;
            let why = format!("{via:?}/{enc:?}");
            assert_eq!(
                c.call::<NodePing>(Empty {}).await.unwrap(),
                Pong {},
                "{why}"
            );
            let ack = c
                .call::<PinAdd>(PathParams {
                    path: "/data".into(),
                })
                .await
                .unwrap();
            assert_eq!(ack.detail, "pinned /data", "{why}");
            let gc = c
                .call::<GcRun>(GcRunParams { verify_only: true })
                .await
                .unwrap();
            assert_eq!(
                gc.report.0,
                json!({"verify_only": true, "deleted": [1, 2]}),
                "{why}"
            );
            // A structured error keeps its code and kind.
            let err = c
                .call::<PinAdd>(PathParams {
                    path: "/missing".into(),
                })
                .await
                .unwrap_err();
            assert_eq!(
                (err.kind, err.code),
                (ErrorKind::NotFound, Some(Code::NotFound)),
                "{why}"
            );
            // view.mount with a plain path (no fd) round-trips its labels.
            let info = c
                .call::<ViewMount>(ViewMountParams {
                    subtree: "/a".into(),
                    source: MountSource::Path {
                        mountpoint: "/mnt/a".into(),
                        opts: Default::default(),
                    },
                    labels: [("pv".to_string(), "x".to_string())].into(),
                    qos: Default::default(),
                    confine_links: false,
                })
                .await
                .unwrap();
            assert_eq!(info.mountpoint, "/mnt/a", "{why}");
            assert_eq!(info.labels["pv"], "x", "{why}");
        }
    }
}

#[tokio::test]
async fn raw_json_calls_and_their_errors() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let out = rig
        .client
        .call_json("pin.add", json!({"path": "/x"}))
        .await
        .unwrap();
    assert_eq!(out, json!({"detail": "pinned /x"}));
    // A missing `params` (null) works for empty-params methods.
    let pong = rig
        .client
        .call_json("node.ping", serde_json::Value::Null)
        .await
        .unwrap();
    assert_eq!(pong, json!({}));
    let err = rig
        .client
        .call_json("pin.add", json!({"wrong": 1}))
        .await
        .unwrap_err();
    assert_eq!(
        (err.kind, err.code),
        (ErrorKind::Invalid, Some(Code::Invalid))
    );
    let err = rig
        .client
        .call_json("no.such", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    // Known method, no handler registered on this router.
    let err = rig
        .client
        .call_json("pin.list", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    assert!(err.message.contains("not implemented"), "{err}");
    // Streaming methods are refused up front, and postcard cannot do raw JSON.
    let err = rig
        .client
        .call_json("events.subscribe", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Invalid);
    let bin = self::rig(Via::Unix, Encoding::Postcard).await;
    let err = bin
        .client
        .call_json("node.ping", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
}

#[tokio::test]
async fn using_the_wrong_call_for_a_method_is_a_local_error() {
    let rig = rig(Via::InProcess, Encoding::Json).await;
    let err = rig
        .client
        .call::<EventsSubscribe>(Default::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Invalid);
    assert!(rig.client.subscribe::<NodePing>(Empty {}).await.is_err());
    assert!(rig.client.call_chunks::<NodePing>(Empty {}).await.is_err());
}

#[tokio::test]
async fn a_panicking_handler_fails_the_call_not_the_connection() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let err = rig
        .client
        .call::<NodeLeave>(Default::default())
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Failed);
    assert!(err.message.contains("panicked"), "{err}");
    assert_eq!(
        rig.client.call::<NodePing>(Empty {}).await.unwrap(),
        Pong {}
    );
}

// ---------------------------------------------------------------------------
// Concurrency and cancellation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_slow_call_does_not_block_a_fast_one_on_the_same_connection() {
    for via in ALL_VIA {
        let rig = rig(via, Encoding::Json).await;
        let slow = {
            let client = rig.client.clone();
            tokio::spawn(async move { client.call::<QuotaGet>(Default::default()).await })
        };
        // Give the slow request time to be in flight, then do many fast ones.
        tokio::time::sleep(Duration::from_millis(50)).await;
        for _ in 0..20 {
            tokio::time::timeout(
                Duration::from_secs(2),
                rig.client.call::<NodePing>(Empty {}),
            )
            .await
            .expect("fast call blocked behind the slow one")
            .unwrap();
        }
        assert!(!slow.is_finished(), "{via:?}: the slow call finished early");
        rig.probes.release.notify_one();
        let quota = slow.await.unwrap().unwrap();
        assert_eq!((quota.max_bytes, quota.used_bytes), (Some(7), 3));
    }
}

#[tokio::test]
async fn many_concurrent_calls_all_get_their_own_answer() {
    let rig = rig(Via::Unix, Encoding::Postcard).await;
    let mut tasks = Vec::new();
    for i in 0..64 {
        let client = rig.client.clone();
        tasks.push(tokio::spawn(async move {
            let ack = client
                .call::<PinAdd>(PathParams {
                    path: format!("/p{i}"),
                })
                .await
                .unwrap();
            assert_eq!(ack.detail, format!("pinned /p{i}"));
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::test]
async fn cancel_ends_an_in_flight_call_with_cancelled_and_eintr() {
    for via in ALL_VIA {
        let rig = rig(via, Encoding::Json).await;
        let pending = rig.client.start::<NodeReintegrate>(Empty {}).await.unwrap();
        let id = pending.id();
        tokio::time::sleep(Duration::from_millis(30)).await;
        rig.client.cancel(id).await;
        let err = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .expect("cancel must complete the call")
            .unwrap_err();
        assert_eq!(
            (err.kind, err.code),
            (ErrorKind::Cancelled, Some(Code::Intr)),
            "{via:?}"
        );
        assert!(
            rig.probes.saw_cancel.load(Ordering::SeqCst),
            "handler must see its token fire"
        );
        // The connection is still good.
        rig.client.call::<NodePing>(Empty {}).await.unwrap();
    }
}

#[tokio::test]
async fn a_handler_that_ignores_cancel_is_dropped_after_the_grace_period() {
    let probes = Probes::default();
    let router = test_router(&probes).with_options(ServeOptions {
        cancel_grace: Duration::from_millis(100),
        ..Default::default()
    });
    let client = Client::in_process(router).await.unwrap();
    let pending = client
        .start::<NodeSetWriteMode>(SetWriteModeParams {
            mode: "back".into(),
        })
        .await
        .unwrap();
    eventually("handler start", || {
        probes.stubborn_started.load(Ordering::SeqCst)
    })
    .await;
    let began = std::time::Instant::now();
    pending.cancel().await;
    let err = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("grace period must bound the wait")
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Cancelled);
    assert!(began.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn call_bounded_times_out_and_cancels_on_the_daemon() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let err = rig
        .client
        .call_bounded::<NodeReintegrate>(Empty {}, Duration::from_millis(100))
        .await
        .unwrap_err();
    assert_eq!(
        (err.kind, err.code),
        (ErrorKind::Timeout, Some(Code::TimedOut))
    );
    eventually("server-side cancel after timeout", || {
        rig.probes.saw_cancel.load(Ordering::SeqCst)
    })
    .await;
    // And a call that finishes in time is unaffected.
    let pong = rig
        .client
        .call_bounded::<NodePing>(Empty {}, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(pong, Pong {});
}

#[tokio::test]
async fn dropping_a_pending_call_cancels_it_on_the_daemon() {
    let rig = rig(Via::InProcess, Encoding::Json).await;
    let pending = rig.client.start::<NodeReintegrate>(Empty {}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(pending);
    eventually("cancel after drop", || {
        rig.probes.saw_cancel.load(Ordering::SeqCst)
    })
    .await;
}

#[tokio::test]
async fn dropping_the_connection_cancels_its_calls() {
    let probes = Probes::default();
    let server = start_unix(test_router(&probes), owner_policy(), null_audit());
    let client = Client::connect_unix(&server.path).await.unwrap();
    let pending = client.start::<NodeReintegrate>(Empty {}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(pending);
    drop(client);
    eventually("cancel after disconnect", || {
        probes.saw_cancel.load(Ordering::SeqCst)
    })
    .await;
}

#[tokio::test]
async fn the_daemon_going_away_fails_outstanding_and_new_calls() {
    let probes = Probes::default();
    let server = start_unix(test_router(&probes), owner_policy(), null_audit());
    let client = Client::connect_unix(&server.path).await.unwrap();
    let pending = client.start::<QuotaGet>(Default::default()).await.unwrap();
    server.handle.shutdown().await;
    let err = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("must not hang")
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unavailable);
    let err = client.call::<NodePing>(Empty {}).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unavailable);
    assert!(!client.is_connected());
}

#[tokio::test]
async fn too_many_calls_in_flight_is_unavailable_not_unbounded() {
    let probes = Probes::default();
    let router = test_router(&probes).with_options(ServeOptions {
        max_inflight_per_connection: 2,
        ..Default::default()
    });
    let client = Client::in_process(router).await.unwrap();
    let a = client.start::<QuotaGet>(Default::default()).await.unwrap();
    let b = client.start::<QuotaGet>(Default::default()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let err = client.call::<NodePing>(Empty {}).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unavailable);
    probes.release.notify_one();
    probes.release.notify_one();
    a.await.unwrap();
    b.await.unwrap();
    client.call::<NodePing>(Empty {}).await.unwrap();
}

#[tokio::test]
async fn reusing_an_in_flight_request_id_is_rejected() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes).with_options(ServeOptions {
        cancel_grace: Duration::from_millis(50),
        ..Default::default()
    }));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    handshake_raw(&client_end, Encoding::Json).await;
    let req = |id| {
        let r = Request {
            id,
            method: "quota.get".into(),
            params: Blob::Json(json!({})),
        };
        Frame::new(FrameKind::Request, Encoding::Json.to_bytes(&r).unwrap())
    };
    client_end.send_frame(req(5)).await.unwrap();
    client_end.send_frame(req(5)).await.unwrap();
    // Not a second response for id 5 (the original call would then have
    // two): a connection-level protocol error on id 0.
    let frame = client_end.recv_frame().await.unwrap().unwrap();
    let response: Response = Encoding::Json.from_bytes(&frame.payload).unwrap();
    assert_eq!(response.id, 0);
    assert!(matches!(response.result, Outcome::Err(e) if e.code == Some(Code::Protocol)));
    // The original call still gets exactly its own (cancelled) response.
    let frame = tokio::time::timeout(Duration::from_secs(10), client_end.recv_frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let response: Response = Encoding::Json.from_bytes(&frame.payload).unwrap();
    assert_eq!(response.id, 5);
    assert!(matches!(response.result, Outcome::Err(e) if e.kind == ErrorKind::Cancelled));
}

#[tokio::test]
async fn request_id_zero_is_reserved() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    handshake_raw(&client_end, Encoding::Json).await;
    let r = Request {
        id: 0,
        method: "node.ping".into(),
        params: Blob::Json(json!({})),
    };
    client_end
        .send_frame(Frame::new(
            FrameKind::Request,
            Encoding::Json.to_bytes(&r).unwrap(),
        ))
        .await
        .unwrap();
    let frame = client_end.recv_frame().await.unwrap().unwrap();
    let response: Response = Encoding::Json.from_bytes(&frame.payload).unwrap();
    assert_eq!(response.id, 0);
    assert!(matches!(response.result, Outcome::Err(e) if e.code == Some(Code::Protocol)));
    assert!(client_end.recv_frame().await.unwrap().is_none());
}

/// Requests and cancels racing each other, back to back: every id gets
/// exactly one response, nothing more arrives, and no in-flight slot leaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_racing_completion_yields_exactly_one_response_per_call() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes).with_options(ServeOptions {
        max_inflight_per_connection: 4,
        ..Default::default()
    }));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    handshake_raw(&client_end, Encoding::Postcard).await;
    let enc = Encoding::Postcard;
    const N: u64 = 300;
    let sender = {
        let t = client_end.clone();
        tokio::spawn(async move {
            for id in 1..=N {
                let r = Request {
                    id,
                    method: "pin.add".into(),
                    params: Blob::encode(enc, &PathParams { path: "/r".into() }).unwrap(),
                };
                t.send_frame(Frame::new(FrameKind::Request, enc.to_bytes(&r).unwrap()))
                    .await
                    .unwrap();
                t.send_frame(Frame::new(
                    FrameKind::Cancel,
                    enc.to_bytes(&Cancel { id }).unwrap(),
                ))
                .await
                .unwrap();
            }
        })
    };
    let mut seen = std::collections::HashSet::new();
    let mut unavailable = 0;
    while seen.len() < N as usize {
        let frame = tokio::time::timeout(Duration::from_secs(10), client_end.recv_frame())
            .await
            .expect("every call must be answered")
            .unwrap()
            .unwrap();
        assert_eq!(frame.kind, FrameKind::Response);
        let response: Response = enc.from_bytes(&frame.payload).unwrap();
        assert!(
            seen.insert(response.id),
            "two responses for {}",
            response.id
        );
        match response.result {
            Outcome::Ok(_) => {}
            Outcome::Err(e) if e.kind == ErrorKind::Cancelled => {}
            // Only 4 may be in flight; the rest are turned away.
            Outcome::Err(e) if e.kind == ErrorKind::Unavailable => unavailable += 1,
            Outcome::Err(e) => panic!("{e}"),
        }
    }
    sender.await.unwrap();
    assert!(unavailable < N, "some calls must have been admitted");
    // Nothing further: no late second answer to anything.
    assert!(
        tokio::time::timeout(Duration::from_millis(200), client_end.recv_frame())
            .await
            .is_err()
    );
    // And every slot was released: exactly 4 slow calls fit again, the
    // fifth is turned away.
    for (id, method) in (N + 1..=N + 5).zip(["quota.get"; 4].into_iter().chain(["node.ping"])) {
        let params = match method {
            "quota.get" => Blob::encode(enc, &QuotaGetParams::default()),
            _ => Blob::encode(enc, &Empty {}),
        };
        let r = Request {
            id,
            method: method.into(),
            params: params.unwrap(),
        };
        client_end
            .send_frame(Frame::new(FrameKind::Request, enc.to_bytes(&r).unwrap()))
            .await
            .unwrap();
    }
    let frame = client_end.recv_frame().await.unwrap().unwrap();
    let response: Response = enc.from_bytes(&frame.payload).unwrap();
    assert_eq!(response.id, N + 5);
    assert!(matches!(response.result, Outcome::Err(ref e) if e.kind == ErrorKind::Unavailable));
    let mut released = 0;
    while released < 4 {
        probes.release.notify_one();
        if let Ok(frame) =
            tokio::time::timeout(Duration::from_millis(50), client_end.recv_frame()).await
        {
            let response: Response = enc.from_bytes(&frame.unwrap().unwrap().payload).unwrap();
            assert!(matches!(response.result, Outcome::Ok(_)), "{response:?}");
            released += 1;
        }
    }
}

async fn handshake_raw(t: &Arc<InProcess>, enc: Encoding) -> Welcome {
    let hello = Hello {
        encodings: vec![enc],
        client: crate::proto::ClientInfo {
            name: "raw".into(),
            version: "0".into(),
        },
    };
    t.send_frame(Frame::new(
        FrameKind::Hello,
        Encoding::Json.to_bytes(&hello).unwrap(),
    ))
    .await
    .unwrap();
    let frame = t.recv_frame().await.unwrap().unwrap();
    assert_eq!(frame.kind, FrameKind::Welcome);
    Encoding::Json.from_bytes(&frame.payload).unwrap()
}

/// The next frame, or a test failure (not a hang) within 5 s.
async fn recv_within(t: &dyn Transport) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), t.recv_frame())
        .await
        .expect("no frame within 5 s")
        .unwrap()
        .expect("connection closed")
}

/// The server half of a handshake, by hand, for tests that play the daemon.
async fn welcome_raw(t: &dyn Transport, enc: Encoding) {
    let hello = t.recv_frame().await.unwrap().unwrap();
    assert_eq!(hello.kind, FrameKind::Hello);
    let welcome = Welcome {
        server_version: "test".into(),
        principal: Principal::InProcess,
        roles: Role::Admin.implied(),
        features: vec![],
        encoding: enc,
    };
    t.send_frame(Frame::new(
        FrameKind::Welcome,
        Encoding::Json.to_bytes(&welcome).unwrap(),
    ))
    .await
    .unwrap();
}

/// A call given up on while its request is still being written (here: a
/// daemon that is not reading) must neither tear the frame nor poison the
/// connection, and its `Cancel` must follow the whole request.
#[tokio::test]
async fn giving_up_on_a_call_mid_send_leaves_the_connection_usable() {
    let (a, b) = tokio::io::duplex(64);
    let daemon = Arc::new(StreamTransport::new(b, Principal::InProcess));
    let client_side = Arc::new(StreamTransport::new(a, Principal::InProcess));
    let (client, ()) = tokio::join!(
        async {
            Client::from_transport(client_side, ClientOptions::default())
                .await
                .unwrap()
        },
        welcome_raw(&*daemon, Encoding::Json)
    );
    // 256 KiB through a 64-byte pipe nobody drains: the write blocks.
    let big = "p".repeat(256 * 1024);
    let err = client
        .call_bounded::<PinAdd>(PathParams { path: big.clone() }, Duration::from_millis(50))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Timeout);
    let ping = {
        let client = client.clone();
        tokio::spawn(async move { client.call::<NodePing>(Empty {}).await })
    };
    // Now the daemon reads: the whole request, intact, then (in some
    // order) its Cancel and the ping.
    let first = recv_within(&*daemon).await;
    assert_eq!(first.kind, FrameKind::Request);
    let first: Request = Encoding::Json.from_bytes(&first.payload).unwrap();
    assert_eq!(first.params.as_json().unwrap()["path"], json!(big));
    let (mut cancelled, mut ping_id) = (false, None);
    while !cancelled || ping_id.is_none() {
        let frame = recv_within(&*daemon).await;
        match frame.kind {
            FrameKind::Cancel => {
                let c: Cancel = Encoding::Json.from_bytes(&frame.payload).unwrap();
                assert_eq!(c.id, first.id);
                cancelled = true;
            }
            FrameKind::Request => {
                let r: Request = Encoding::Json.from_bytes(&frame.payload).unwrap();
                assert_eq!(r.method, "node.ping");
                ping_id = Some(r.id);
            }
            other => panic!("{other:?}"),
        }
    }
    let pong = Response {
        id: ping_id.unwrap(),
        result: Outcome::Ok(Blob::Json(json!({}))),
    };
    daemon
        .send_frame(Frame::new(
            FrameKind::Response,
            Encoding::Json.to_bytes(&pong).unwrap(),
        ))
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(5), ping)
        .await
        .expect("the connection must still work")
        .unwrap()
        .unwrap();
    assert_eq!(pong, Pong {});
}

/// When the connection dies while a stream's buffer is full, the other
/// outstanding calls still fail promptly, and the stream reports the loss
/// after its backlog instead of ending as if complete.
#[tokio::test]
async fn a_dead_connection_fails_every_call_even_behind_a_full_stream() {
    for _ in 0..8 {
        // (HashMap order decides which slot is failed first; repeat.)
        let (client_end, daemon) = InProcess::pair();
        let (client, ()) = tokio::join!(
            async {
                Client::from_transport(client_end, ClientOptions::default())
                    .await
                    .unwrap()
            },
            welcome_raw(&*daemon, Encoding::Json)
        );
        let chunks = client
            .call_chunks::<BrowseRead>(BrowseReadParams {
                path: "/f".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let unary = client.start::<QuotaGet>(Default::default()).await.unwrap();
        let read_id = chunks.id();
        for _ in 0..2 {
            daemon.recv_frame().await.unwrap().unwrap();
        }
        // Exactly fill the chunk buffer, then vanish.
        for seq in 0..64u64 {
            let chunk = Chunk {
                id: read_id,
                seq,
                bytes: vec![seq as u8].into(),
                last: false,
            };
            daemon
                .send_frame(Frame::new(
                    FrameKind::Chunk,
                    Encoding::Json.to_bytes(&chunk).unwrap(),
                ))
                .await
                .unwrap();
        }
        drop(daemon);
        let err = tokio::time::timeout(Duration::from_secs(5), unary)
            .await
            .expect("an unread stream must not keep other calls hanging")
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Unavailable);
        let items: Vec<_> = chunks.collect().await;
        assert_eq!(items.len(), 65);
        assert!(items[..64].iter().all(|i| i.is_ok()));
        assert_eq!(
            items[64].as_ref().unwrap_err().kind,
            ErrorKind::Unavailable,
            "a truncated stream must say so"
        );
    }
}

/// A subscriber that stops polling while it awaits another call on the
/// same connection must not deadlock that call; it is cut off instead.
#[tokio::test]
async fn a_subscriber_that_falls_behind_is_cut_off_not_a_deadlock() {
    let (client_end, daemon) = InProcess::pair();
    let (client, ()) = tokio::join!(
        async {
            Client::from_transport(client_end, ClientOptions::default())
                .await
                .unwrap()
        },
        welcome_raw(&*daemon, Encoding::Json)
    );
    let mut events = client
        .subscribe::<EventsSubscribe>(Default::default())
        .await
        .unwrap();
    let sub_id = events.id();
    let call = {
        let client = client.clone();
        tokio::spawn(async move { client.call::<NodePing>(Empty {}).await })
    };
    daemon.recv_frame().await.unwrap().unwrap(); // subscribe
    let ping: Request = Encoding::Json
        .from_bytes(&daemon.recv_frame().await.unwrap().unwrap().payload)
        .unwrap();
    let daemon2 = daemon.clone();
    let pump = tokio::spawn(async move {
        for n in 0..1100u64 {
            let event = Event {
                sub_id,
                payload: Blob::Json(json!({"unix_ms": n, "topic": "t", "data": null})),
            };
            daemon2
                .send_frame(Frame::new(
                    FrameKind::Event,
                    Encoding::Json.to_bytes(&event).unwrap(),
                ))
                .await
                .unwrap();
        }
        let pong = Response {
            id: ping.id,
            result: Outcome::Ok(Blob::Json(json!({}))),
        };
        daemon2
            .send_frame(Frame::new(
                FrameKind::Response,
                Encoding::Json.to_bytes(&pong).unwrap(),
            ))
            .await
            .unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .expect("the unary call must not wait behind the unread subscription")
        .unwrap()
        .unwrap();
    pump.await.unwrap();
    // The daemon was told to stop the subscription.
    let cancel = daemon.recv_frame().await.unwrap().unwrap();
    assert_eq!(cancel.kind, FrameKind::Cancel);
    let cancel: Cancel = Encoding::Json.from_bytes(&cancel.payload).unwrap();
    assert_eq!(cancel.id, sub_id);
    // The subscriber gets its backlog, then the reason it was cut off.
    let mut ok = 0;
    let last = loop {
        match events.next().await.expect("an error item before the end") {
            Ok(_) => ok += 1,
            Err(e) => break e,
        }
    };
    assert_eq!(ok, 1024);
    assert_eq!(last.kind, ErrorKind::Unavailable);
    assert!(last.message.contains("fell"), "{last}");
    assert!(events.next().await.is_none());
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_subscription_streams_events_until_cancelled() {
    for via in ALL_VIA {
        for enc in ALL_ENC {
            let rig = rig(via, enc).await;
            let mut events = rig
                .client
                .subscribe::<EventsSubscribe>(EventsSubscribeParams {
                    topics: vec!["view".into()],
                })
                .await
                .unwrap();
            for n in 0..5u64 {
                let event = tokio::time::timeout(Duration::from_secs(5), events.next())
                    .await
                    .expect("event")
                    .expect("stream open")
                    .unwrap();
                assert_eq!(event.topic, format!("view.{n}"), "{via:?}/{enc:?}");
                assert_eq!(event.data.0, json!({"n": n}));
            }
            events.cancel().await;
            // The stream ends cleanly (no error item) once the cancel lands.
            let rest = tokio::time::timeout(Duration::from_secs(5), async {
                let mut last = None;
                while let Some(item) = events.next().await {
                    last = Some(item);
                    assert!(
                        last.as_ref().unwrap().is_ok(),
                        "no error item after our own cancel"
                    );
                }
                last
            })
            .await
            .expect("stream must end after cancel");
            let _ = rest;
            eventually("server stream dropped", || {
                rig.probes.stream_dropped.load(Ordering::SeqCst)
            })
            .await;
        }
    }
}

#[tokio::test]
async fn dropping_a_subscription_stops_it_on_the_daemon() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let mut events = rig
        .client
        .subscribe::<EventsSubscribe>(Default::default())
        .await
        .unwrap();
    events.next().await.unwrap().unwrap();
    drop(events);
    eventually("server stream dropped", || {
        rig.probes.stream_dropped.load(Ordering::SeqCst)
    })
    .await;
    rig.client.call::<NodePing>(Empty {}).await.unwrap();
}

#[tokio::test]
async fn a_finite_subscription_ends_by_itself() {
    let rig = rig(Via::InProcess, Encoding::Postcard).await;
    let samples: Vec<_> = rig
        .client
        .subscribe::<StatsSubscribe>(Default::default())
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(samples.len(), 3);
    let last = samples[2].as_ref().unwrap();
    assert_eq!(last.counters["ops"], 20);
    assert_eq!(last.gauges["load"], 0.5);
}

#[tokio::test]
async fn chunked_results_arrive_in_order_and_intact() {
    for via in ALL_VIA {
        for enc in ALL_ENC {
            let rig = rig(via, enc).await;
            let bytes = rig
                .client
                .call_chunks::<BrowseRead>(BrowseReadParams {
                    path: "/big".into(),
                    ..Default::default()
                })
                .await
                .unwrap()
                .collect_bytes()
                .await
                .unwrap();
            assert_eq!(bytes.len(), 2_500_004, "{via:?}/{enc:?}");
            assert!(bytes[..2_500_000]
                .iter()
                .enumerate()
                .all(|(i, b)| *b == (i % 251) as u8));
            assert_eq!(&bytes[2_500_000..], b"tail");
        }
    }
}

#[tokio::test]
async fn a_chunk_stream_that_fails_delivers_the_bytes_then_the_error() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let mut chunks = rig
        .client
        .call_chunks::<BrowseRead>(BrowseReadParams {
            path: "/err".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut total = 0;
    let mut error = None;
    while let Some(item) = chunks.next().await {
        match item {
            Ok(b) => total += b.len(),
            Err(e) => error = Some(e),
        }
    }
    assert!(
        total >= 1_000_000,
        "the good part arrived before the failure"
    );
    assert_eq!(error.unwrap().message, "disk exploded");
}

#[tokio::test]
async fn an_empty_chunk_stream_is_just_the_terminal_chunk() {
    let rig = rig(Via::InProcess, Encoding::Json).await;
    let bytes = rig
        .client
        .call_chunks::<NodeLogsTail>(LogTailParams {
            lines: 10,
            follow: false,
        })
        .await
        .unwrap()
        .collect_bytes()
        .await
        .unwrap();
    assert!(bytes.is_empty());
}

/// A chunk stream that pauses (a followed log) delivers what it has at
/// once, not when the next chunk happens to come along.
#[tokio::test]
async fn a_following_chunk_stream_delivers_what_it_has() {
    for via in ALL_VIA {
        let rig = rig(via, Encoding::Postcard).await;
        let mut tail = rig
            .client
            .call_chunks::<NodeLogsTail>(LogTailParams {
                lines: 10,
                follow: true,
            })
            .await
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), tail.next())
            .await
            .expect("the held-back chunk must be flushed while the stream waits")
            .unwrap()
            .unwrap();
        assert_eq!(&first[..], b"line 1\n", "{via:?}");
        tail.cancel().await;
        let rest = tokio::time::timeout(Duration::from_secs(5), tail.next())
            .await
            .expect("cancel ends it");
        assert!(rest.is_none(), "{via:?}: {rest:?}");
    }
}

#[tokio::test]
async fn a_result_too_big_for_a_frame_is_an_error_not_a_hang() {
    for via in ALL_VIA {
        for enc in ALL_ENC {
            let rig = rig(via, enc).await;
            let err = tokio::time::timeout(
                Duration::from_secs(10),
                rig.client
                    .call::<FsExport>(FsExportParams { fs: "huge".into() }),
            )
            .await
            .expect("the client must get an answer")
            .unwrap_err();
            assert_eq!(err.code, Some(Code::Overflow), "{via:?}/{enc:?}: {err}");
            // The connection is fine.
            let doc = rig
                .client
                .call::<FsExport>(FsExportParams { fs: "small".into() })
                .await
                .unwrap();
            assert_eq!(doc.document, "xxx");
        }
    }
}

#[tokio::test]
async fn a_handler_that_panics_before_returning_its_future_fails_the_call() {
    let probes = Probes::default();
    let router = test_router(&probes).with_options(ServeOptions {
        max_inflight_per_connection: 1,
        ..Default::default()
    });
    let client = Client::in_process(router).await.unwrap();
    for _ in 0..3 {
        let err = tokio::time::timeout(
            Duration::from_secs(5),
            client.call::<ViewUnmount>(ViewUnmountParams {
                mountpoint: "/m".into(),
            }),
        )
        .await
        .expect("a panic must still produce a response")
        .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Failed);
        assert!(err.message.contains("panicked"), "{err}");
    }
    // The one in-flight slot was released every time.
    client.call::<NodePing>(Empty {}).await.unwrap();
}

/// The frames themselves: 1 MiB split, `seq` counting, exactly one `last`,
/// then the `Response` carrying `StreamEnd`.
#[tokio::test]
async fn chunk_frames_have_seq_and_a_single_last() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    handshake_raw(&client_end, Encoding::Json).await;
    let req = Request {
        id: 9,
        method: "browse.read".into(),
        params: Blob::Json(json!({"path": "/big"})),
    };
    client_end
        .send_frame(Frame::new(
            FrameKind::Request,
            Encoding::Json.to_bytes(&req).unwrap(),
        ))
        .await
        .unwrap();
    let mut chunks = Vec::new();
    let end = loop {
        let frame = client_end.recv_frame().await.unwrap().unwrap();
        match frame.kind {
            FrameKind::Chunk => {
                chunks.push(Encoding::Json.from_bytes::<Chunk>(&frame.payload).unwrap())
            }
            FrameKind::Response => {
                break Encoding::Json
                    .from_bytes::<Response>(&frame.payload)
                    .unwrap();
            }
            other => panic!("{other:?}"),
        }
    };
    // 2_500_000 bytes -> 1 MiB + 1 MiB + remainder, then "tail".
    assert_eq!(chunks.len(), 4);
    assert!(chunks
        .iter()
        .enumerate()
        .all(|(i, c)| c.seq == i as u64 && c.id == 9));
    assert_eq!(chunks.iter().filter(|c| c.last).count(), 1);
    assert!(chunks.last().unwrap().last);
    assert!(chunks
        .iter()
        .all(|c| c.bytes.0.len() <= crate::server::MAX_CHUNK_BYTES));
    let Outcome::Ok(blob) = end.result else {
        panic!("expected Ok")
    };
    let end: StreamEnd = blob.decode().unwrap();
    assert_eq!((end.items, end.bytes), (2, 2_500_004));
}

/// Event frames carry the request id as `sub_id`, and a `Cancel` frame ends
/// the subscription with a `Cancelled` response.
#[tokio::test]
async fn event_frames_use_the_request_id_and_cancel_ends_them() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (client_end, server_end) = InProcess::pair();
    tokio::spawn(serve_connection(
        server_end,
        router,
        CancellationToken::new(),
    ));
    handshake_raw(&client_end, Encoding::Json).await;
    let req = Request {
        id: 4,
        method: "events.subscribe".into(),
        params: Blob::Json(json!({})),
    };
    client_end
        .send_frame(Frame::new(
            FrameKind::Request,
            Encoding::Json.to_bytes(&req).unwrap(),
        ))
        .await
        .unwrap();
    let frame = client_end.recv_frame().await.unwrap().unwrap();
    assert_eq!(frame.kind, FrameKind::Event);
    let event: Event = Encoding::Json.from_bytes(&frame.payload).unwrap();
    assert_eq!(event.sub_id, 4);
    client_end
        .send_frame(Frame::new(
            FrameKind::Cancel,
            Encoding::Json.to_bytes(&Cancel { id: 4 }).unwrap(),
        ))
        .await
        .unwrap();
    let response = loop {
        let frame = client_end.recv_frame().await.unwrap().unwrap();
        if frame.kind == FrameKind::Response {
            break Encoding::Json
                .from_bytes::<Response>(&frame.payload)
                .unwrap();
        }
    };
    assert_eq!(response.id, 4);
    assert!(matches!(response.result, Outcome::Err(e) if e.kind == ErrorKind::Cancelled));
}

// ---------------------------------------------------------------------------
// Authorization
// ---------------------------------------------------------------------------

fn unix_principal(uid: u32, gids: &[u32]) -> Principal {
    Principal::Unix {
        uid,
        gids: gids.to_vec(),
        pid: Some(1),
    }
}

async fn client_as(router: Router, principal: Principal) -> Client {
    Client::in_process_as(router, principal, ClientOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn the_owner_is_admin_and_a_stranger_is_denied_everything() {
    let probes = Probes::default();
    let policy = Policy::owner_only(1000);
    let mk = || test_router(&probes).with_policy(policy.clone());

    let owner = client_as(mk(), unix_principal(1000, &[1000])).await;
    assert_eq!(owner.welcome().roles, Role::Admin.implied());
    owner.call::<GcRun>(GcRunParams::default()).await.unwrap();

    let stranger = client_as(mk(), unix_principal(2000, &[2000])).await;
    assert!(stranger.welcome().roles.is_empty());
    for err in [
        stranger.call::<NodePing>(Empty {}).await.unwrap_err(),
        stranger
            .call::<GcRun>(GcRunParams::default())
            .await
            .unwrap_err(),
        // Even a method that does not exist: no probing the table.
        stranger.call_json("no.such", json!({})).await.unwrap_err(),
    ] {
        assert_eq!(
            (err.kind, err.code),
            (ErrorKind::Denied, Some(Code::Access)),
            "{err}"
        );
        assert!(err.remediation.is_some());
    }
    // The owner, by contrast, learns the method is unknown.
    assert_eq!(
        owner
            .call_json("no.such", json!({}))
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Unsupported
    );
    assert_eq!(probes.ran.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn an_allowlisted_group_gets_exactly_its_role() {
    let probes = Probes::default();
    let policy = Policy::owner_only(1000).with_grant(Grant {
        subject: Subject::Gid(5000),
        role: Role::Operator,
    });
    let router = test_router(&probes).with_policy(policy);
    let ops = client_as(router, unix_principal(3000, &[3000, 5000])).await;
    assert_eq!(ops.welcome().roles, vec![Role::Viewer, Role::Operator]);
    // viewer-level and operator-level methods work...
    ops.call::<NodePing>(Empty {}).await.unwrap();
    ops.call::<PinAdd>(PathParams { path: "/a".into() })
        .await
        .unwrap();
    // ...admin-level ones are Denied with the role gap spelled out.
    let err = ops.call::<GcRun>(GcRunParams::default()).await.unwrap_err();
    assert_eq!(err.kind, ErrorKind::Denied);
    let details = err.details.expect("details").0;
    assert_eq!(details["required"], "admin");
    assert_eq!(details["held"], "operator");
    assert_eq!(details["method"], "gc.run");
}

#[tokio::test]
async fn a_toml_allowlist_drives_the_same_decisions() {
    let probes = Probes::default();
    let policy = Policy::from_toml(
        "[[grant]]\nuid = 42\nrole = \"viewer\"\n",
        Some(1000),
        &crate::authz::SystemGroups,
    )
    .unwrap();
    let viewer = client_as(
        test_router(&probes).with_policy(policy),
        unix_principal(42, &[42]),
    )
    .await;
    viewer.call::<NodePing>(Empty {}).await.unwrap();
    let err = viewer
        .call::<PinAdd>(PathParams { path: "/a".into() })
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Denied);
    assert_eq!(
        probes.ran.load(Ordering::SeqCst),
        0,
        "the handler must not run when denied"
    );
}

#[tokio::test]
async fn a_real_socket_peer_is_identified_by_its_credentials() {
    let probes = Probes::default();
    // Server owned by "someone else": we, a stranger, are refused.
    let server = start_unix(
        test_router(&probes),
        Policy::owner_only(me() + 1),
        null_audit(),
    );
    let client = Client::connect_unix(&server.path).await.unwrap();
    assert!(client.welcome().roles.is_empty());
    assert_eq!(
        client.call::<NodePing>(Empty {}).await.unwrap_err().kind,
        ErrorKind::Denied
    );
    // Server owned by us: admin.
    let server = start_unix(test_router(&probes), Policy::owner_only(me()), null_audit());
    let client = Client::connect_unix(&server.path).await.unwrap();
    client.call::<GcRun>(GcRunParams::default()).await.unwrap();
    // And a grant by our own uid on a server owned by someone else.
    let policy = Policy::owner_only(me() + 1).with_grant(Grant {
        subject: Subject::Uid(me()),
        role: Role::Viewer,
    });
    let server = start_unix(test_router(&probes), policy, null_audit());
    let client = Client::connect_unix(&server.path).await.unwrap();
    client.call::<NodePing>(Empty {}).await.unwrap();
    assert_eq!(
        client
            .call::<GcRun>(GcRunParams::default())
            .await
            .unwrap_err()
            .kind,
        ErrorKind::Denied
    );
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn only_mutating_calls_are_audited_with_a_digest_and_never_the_params() {
    let probes = Probes::default();
    let audit = Arc::new(MemoryAuditSink::new());
    let policy = Policy::owner_only(1000).with_grant(Grant {
        subject: Subject::Uid(7),
        role: Role::Operator,
    });
    let router = test_router(&probes)
        .with_policy(policy)
        .with_audit(audit.clone());
    let router = Arc::new(router);

    let admin = client_as_arc(router.clone(), unix_principal(1000, &[1000])).await;
    let op = client_as_arc(router.clone(), unix_principal(7, &[7])).await;

    // Reads: not audited.
    admin.call::<NodePing>(Empty {}).await.unwrap();
    admin.call_json("node.ping", json!({})).await.unwrap();
    assert!(audit.records().is_empty());

    // A mutating call: audited, with the digest of the params as sent.
    admin
        .call_json("pin.add", json!({"path": "/SECRET-PATH-123"}))
        .await
        .unwrap();
    // A mutating call that fails: audited with the error kind.
    let _ = admin
        .call::<PinAdd>(PathParams {
            path: "/missing".into(),
        })
        .await;
    // A mutating call the caller may not make: audited as denied.
    assert!(op.call::<GcRun>(GcRunParams::default()).await.is_err());
    // A denied *read*: not audited.
    let stranger = client_as_arc(router.clone(), unix_principal(9, &[9])).await;
    assert!(stranger.call::<NodePing>(Empty {}).await.is_err());
    // Secret-bearing params: audited, but not even their digest is kept
    // (an unsalted hash of a passphrase is brute-forceable).
    let _ = admin
        .call::<FsPasswd>(FsPasswdParams {
            fs: "f".into(),
            old_passphrase: crate::proto::Secret::new("hunter2"),
            new_passphrase: crate::proto::Secret::new("hunter3"),
        })
        .await;

    let records = audit.records();
    assert_eq!(
        records
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        ["pin.add", "pin.add", "gc.run", "fs.passwd"]
    );
    assert_eq!(records[3].params_digest, crate::audit::WITHHELD_DIGEST);
    assert_eq!(records[0].outcome, AuditOutcome::Ok);
    assert_eq!(records[0].role, Some(Role::Admin));
    assert_eq!(
        records[0].params_digest,
        params_digest(&Blob::Json(json!({"path": "/SECRET-PATH-123"})))
    );
    assert_eq!(records[1].outcome, AuditOutcome::Err(ErrorKind::NotFound));
    assert_eq!(records[2].outcome, AuditOutcome::Err(ErrorKind::Denied));
    assert_eq!(records[2].role, Some(Role::Operator));
    assert!(records[0].principal.to_string().contains("uid=1000"));
    assert!(records.iter().all(|r| r.ts_unix_ms > 1_600_000_000_000));
    // Never the raw params, anywhere in what would be written.
    for r in &records {
        let line = serde_json::to_string(r).unwrap();
        assert!(!line.contains("SECRET-PATH-123"), "{line}");
    }
}

async fn client_as_arc(router: Arc<Router>, principal: Principal) -> Client {
    Client::in_process_as(router, principal, ClientOptions::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn the_file_sink_records_a_socket_call_as_one_json_line() {
    let probes = Probes::default();
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("audit.jsonl");
    let sink = Arc::new(crate::audit::FileAuditSink::open(&log).unwrap());
    let server = start_unix(test_router(&probes), owner_policy(), sink);
    let client = Client::connect_unix_with(&server.path, ClientOptions::postcard())
        .await
        .unwrap();
    client
        .call::<PinAdd>(PathParams { path: "/z".into() })
        .await
        .unwrap();
    client.call::<NodePing>(Empty {}).await.unwrap();
    eventually("audit line", || {
        std::fs::read_to_string(&log)
            .map(|t| !t.is_empty())
            .unwrap_or(false)
    })
    .await;
    let text = std::fs::read_to_string(&log).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 1, "{text}");
    let record: crate::audit::AuditRecord = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(record.method, "pin.add");
    assert_eq!(record.encoding, Encoding::Postcard);
    assert!(record.params_digest.starts_with("blake3:"));
    assert!(!text.contains("\"/z\""));
}

/// The only production-reachable path for a `kind = "service"` grant
/// (plan 33 U1): a real listener, a real peer-cred principal, the policy
/// told the path it actually bound. Covers what a hand-built `Policy` in the
/// `authz` unit tests cannot — that the bound path is plumbed through at all
/// (an unwired `with_bound_socket` makes every service grant inert), that
/// the two canonicalisations meet in the middle when the allowlist names the
/// socket through a symlinked parent (plan 37's hostPath shape), and that
/// the audit line a reader gets names the service.
#[tokio::test]
async fn a_service_grant_over_a_real_socket_grants_the_role_and_names_the_service() {
    let dir = tempfile::tempdir().unwrap();
    // The allowlist names the socket through a symlinked parent; the daemon
    // binds — and canonicalises — the real one.
    let real = dir.path().join("sockets");
    std::fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, dir.path().join("link")).unwrap();
    let configured = dir.path().join("link").join("control.sock");
    let path = real.join("control.sock");
    let elsewhere = dir.path().join("elsewhere.sock");

    let allowlist = format!(
        "[[grant]]\nkind = \"service\"\nprincipal = \"uid:{}\"\nsocket = {:?}\nrole = \"operator\"\nlabel = \"csi-node-plugin\"\n",
        me(),
        configured.display().to_string(),
    );

    // Matching: bound where the grant says, so pin.add (operator, mutating)
    // is allowed and audited as the service.
    let probes = Probes::default();
    let audit = Arc::new(MemoryAuditSink::new());
    let listener = UnixSocketListener::bind(&path).unwrap();
    let bound = std::fs::canonicalize(listener.path()).unwrap();
    assert_eq!(bound, std::fs::canonicalize(&path).unwrap());
    let policy = Policy::from_toml(&allowlist, None, &crate::authz::SystemGroups)
        .unwrap()
        .with_bound_socket(bound);
    // The grant's configured path canonicalised at load time, through the
    // symlink, to the very path the daemon bound.
    assert_eq!(
        policy.grants()[0].subject,
        Subject::Service {
            uid: me(),
            socket: std::fs::canonicalize(&path).unwrap(),
            label: "csi-node-plugin".into(),
        }
    );
    let handle = serve(listener, test_router(&probes), policy, audit.clone());
    let client = Client::connect_unix(&path).await.unwrap();
    let welcome_roles = client.welcome().roles.clone();
    client
        .call::<PinAdd>(PathParams { path: "/p".into() })
        .await
        .unwrap();
    assert_eq!(welcome_roles, Role::Operator.implied());
    let records = audit.records();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].role, Some(Role::Operator));
    assert_eq!(
        serde_json::to_value(&records[0].principal).unwrap(),
        json!({
            "kind": "service",
            "uid": me(),
            "socket": std::fs::canonicalize(&path).unwrap().display().to_string(),
            "label": "csi-node-plugin",
        })
    );
    drop(client);
    handle.shutdown().await;

    // Same uid, same allowlist, a daemon bound somewhere else: no match, so
    // not even a read is allowed. And a policy never told its bound path
    // (binding failed) fails closed the same way.
    for bound in [
        Some(
            std::fs::canonicalize(dir.path())
                .unwrap()
                .join("other.sock"),
        ),
        None,
    ] {
        let probes = Probes::default();
        let listener = UnixSocketListener::bind(&elsewhere).unwrap();
        let mut policy = Policy::from_toml(&allowlist, None, &crate::authz::SystemGroups).unwrap();
        if let Some(bound) = bound.clone() {
            policy = policy.with_bound_socket(bound);
        }
        let handle = serve(listener, test_router(&probes), policy, null_audit());
        let client = Client::connect_unix(&elsewhere).await.unwrap();
        assert!(client.welcome().roles.is_empty(), "{bound:?}");
        let err = client
            .call::<PinAdd>(PathParams { path: "/p".into() })
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Denied, "{bound:?}");
        drop(client);
        handle.shutdown().await;
        std::fs::remove_file(&elsewhere).ok();
    }
}

// ---------------------------------------------------------------------------
// File descriptors
// ---------------------------------------------------------------------------

fn tempfile_with(text: &str) -> std::fs::File {
    let mut f = tempfile::tempfile().unwrap();
    f.write_all(text.as_bytes()).unwrap();
    f.seek(SeekFrom::Start(0)).unwrap();
    f
}

fn preopened_mount() -> ViewMountParams {
    ViewMountParams {
        subtree: "/".into(),
        source: MountSource::PreopenedFd,
        labels: Default::default(),
        qos: Default::default(),
        confine_links: true,
    }
}

#[tokio::test]
async fn a_mount_fd_reaches_the_handler_over_the_socket_and_in_process() {
    for via in ALL_VIA {
        for enc in ALL_ENC {
            let rig = rig(via, enc).await;
            let file = tempfile_with("content-of-the-fd");
            let owned = file.as_fd().try_clone_to_owned().unwrap();
            let info = rig
                .client
                .call_with_fd::<ViewMount>(preopened_mount(), owned)
                .await
                .unwrap();
            assert_eq!(info.mountpoint, "fd:content-of-the-fd", "{via:?}/{enc:?}");
            // Forgetting the fd is a local, immediate error.
            let err = rig
                .client
                .call::<ViewMount>(preopened_mount())
                .await
                .unwrap_err();
            assert_eq!(err.kind, ErrorKind::Invalid);
            assert!(err.message.contains("call_with_fd"), "{err}");
        }
    }
}

/// A transport with no fd passing: prompt `NotSupported`, client side and
/// server side, never a hang.
#[tokio::test]
async fn fd_methods_on_a_transport_without_fd_passing_fail_promptly() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (a, b) = tokio::io::duplex(4096);
    let server_side = Arc::new(StreamTransport::new(b, Principal::InProcess));
    tokio::spawn(serve_connection(
        server_side,
        router,
        CancellationToken::new(),
    ));
    let client_side = Arc::new(StreamTransport::new(a, Principal::InProcess));
    let client = Client::from_transport(client_side, ClientOptions::default())
        .await
        .unwrap();
    assert!(!client.supports_fd_passing());
    assert!(!client.welcome().features.iter().any(|f| f == "fd-passing"));

    let within = Duration::from_secs(5);
    let file = tempfile_with("x");
    let err = tokio::time::timeout(
        within,
        client.call_with_fd::<ViewMount>(
            preopened_mount(),
            file.as_fd().try_clone_to_owned().unwrap(),
        ),
    )
    .await
    .expect("must not hang")
    .unwrap_err();
    assert_eq!(
        (err.kind, err.code),
        (ErrorKind::Unsupported, Some(Code::NotSupported))
    );

    let err = tokio::time::timeout(within, client.call::<ViewMount>(preopened_mount()))
        .await
        .expect("must not hang")
        .unwrap_err();
    assert_eq!(err.code, Some(Code::NotSupported));

    // Bypass the client's local check with a raw request: the *server*
    // must refuse, again without waiting for an fd that cannot arrive.
    let err = tokio::time::timeout(
        within,
        client.call_json(
            "view.mount",
            json!({"subtree": "/", "source": "PreopenedFd"}),
        ),
    )
    .await
    .expect("must not hang")
    .unwrap_err();
    assert_eq!(
        (err.kind, err.code),
        (ErrorKind::Unsupported, Some(Code::NotSupported))
    );
    // node.handoff to a socket needs an fd.
    let err = client
        .call::<NodeHandoff>(HandoffParams {
            target: HandoffTarget::Socket,
            ..HandoffParams::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, Some(Code::NotSupported));
    // The connection remains usable.
    client.call::<NodePing>(Empty {}).await.unwrap();
}

#[tokio::test]
async fn a_raw_request_without_its_fd_gets_invalid_on_an_fd_capable_transport() {
    let rig = rig(Via::Unix, Encoding::Json).await;
    let err = rig
        .client
        .call_json(
            "view.mount",
            json!({"subtree": "/", "source": "PreopenedFd"}),
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Invalid);
    assert!(err.message.contains("file descriptor"), "{err}");
}

/// A real socketpair, a real tempfile fd, read back through the received fd
/// after a full client -> router -> handler trip.
#[tokio::test]
async fn fd_passing_over_a_socketpair_end_to_end() {
    let probes = Probes::default();
    let router = Arc::new(test_router(&probes));
    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    let server_side = UnixSocket::from_stream(b).unwrap();
    tokio::spawn(serve_connection(
        server_side,
        router,
        CancellationToken::new(),
    ));
    let client = Client::from_transport(
        UnixSocket::from_stream(a).unwrap(),
        ClientOptions::postcard(),
    )
    .await
    .unwrap();
    let file = tempfile_with("through a socketpair");
    let info = client
        .call_with_fd::<ViewMount>(
            preopened_mount(),
            file.as_fd().try_clone_to_owned().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(info.mountpoint, "fd:through a socketpair");
}

// ---------------------------------------------------------------------------
// In-process dispatch (the web adapter's path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_in_process_takes_the_same_path_as_a_socket_call() {
    let probes = Probes::default();
    let audit = Arc::new(MemoryAuditSink::new());
    let policy = Policy::owner_only(1000);
    let router = Arc::new(
        test_router(&probes)
            .with_policy(policy)
            .with_audit(audit.clone()),
    );
    let admin = Principal::InProcess;

    let out = dispatch_in_process(&router, &admin, "pin.add", json!({"path": "/w"}))
        .await
        .unwrap();
    assert_eq!(out, json!({"detail": "pinned /w"}));
    let err = dispatch_in_process(&router, &admin, "pin.add", json!({"nope": 1}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Invalid);
    let err = dispatch_in_process(&router, &admin, "no.such", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unsupported);
    let err = dispatch_in_process(&router, &admin, "events.subscribe", json!({}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Invalid);
    // Authorization applies: a stranger through the "web adapter" is denied.
    let stranger = unix_principal(5, &[5]);
    let err = dispatch_in_process(&router, &stranger, "pin.add", json!({"path": "/w"}))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Denied);
    // ...and mutating dispatches are audited like any other.
    let methods: Vec<_> = audit
        .records()
        .into_iter()
        .map(|r| (r.method, r.outcome))
        .collect();
    assert_eq!(methods[0], ("pin.add".into(), AuditOutcome::Ok));
    assert_eq!(
        methods.last().unwrap().1,
        AuditOutcome::Err(ErrorKind::Denied)
    );

    // Cancellation and fds ride along.
    let token = CancellationToken::new();
    let call = {
        let router = router.clone();
        let token = token.clone();
        tokio::spawn(async move {
            dispatch_in_process_with_token(&router, "node.reintegrate", token).await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    token.cancel();
    let err = call.await.unwrap().unwrap_err();
    assert_eq!(err.kind, ErrorKind::Cancelled);

    let file = tempfile_with("web fd");
    let out = crate::server::dispatch_in_process_with(
        &router,
        &admin,
        "view.mount",
        json!({"subtree": "/", "source": "PreopenedFd"}),
        DispatchOptions {
            cancel: None,
            fd: Some(file.as_fd().try_clone_to_owned().unwrap()),
        },
    )
    .await
    .unwrap();
    assert_eq!(out["mountpoint"], "fd:web fd");
}

async fn dispatch_in_process_with_token(
    router: &Router,
    method: &str,
    cancel: CancellationToken,
) -> Result<serde_json::Value, ControlError> {
    crate::server::dispatch_in_process_with(
        router,
        &Principal::InProcess,
        method,
        json!({}),
        DispatchOptions {
            cancel: Some(cancel),
            fd: None,
        },
    )
    .await
}

#[tokio::test]
async fn dispatch_stream_in_process_yields_events_and_chunks() {
    let probes = Probes::default();
    let router = test_router(&probes);
    let items: Vec<_> = dispatch_stream_in_process(
        &router,
        &Principal::InProcess,
        "stats.subscribe",
        json!({}),
        DispatchOptions::default(),
    )
    .await
    .unwrap()
    .collect()
    .await;
    assert_eq!(items.len(), 3);
    match items[1].as_ref().unwrap() {
        StreamItem::Event(Blob::Json(v)) => assert_eq!(v["counters"]["ops"], 10),
        other => panic!("{other:?}"),
    }
    let total: usize = dispatch_stream_in_process(
        &router,
        &Principal::InProcess,
        "browse.read",
        json!({"path": "/x"}),
        DispatchOptions::default(),
    )
    .await
    .unwrap()
    .map(|i| match i.unwrap() {
        StreamItem::Chunk(b) => b.len(),
        other => panic!("{other:?}"),
    })
    .collect::<Vec<_>>()
    .await
    .iter()
    .sum();
    assert_eq!(total, 2_500_004);
    // A unary method is not a stream.
    assert!(dispatch_stream_in_process(
        &router,
        &Principal::InProcess,
        "node.ping",
        json!({}),
        DispatchOptions::default()
    )
    .await
    .is_err());
}

#[tokio::test]
async fn cancelling_a_quiet_in_process_stream_wakes_its_consumer() {
    let probes = Probes::default();
    let router = test_router(&probes);
    let cancel = CancellationToken::new();
    let mut tail = dispatch_stream_in_process(
        &router,
        &Principal::InProcess,
        "node.logs.tail",
        json!({"lines": 1, "follow": true}),
        DispatchOptions {
            cancel: Some(cancel.clone()),
            fd: None,
        },
    )
    .await
    .unwrap();
    assert!(matches!(tail.next().await, Some(Ok(StreamItem::Chunk(_)))));
    // The stream has nothing more; the consumer is parked on it.
    let waiter = tokio::spawn(async move { tail.next().await });
    tokio::task::yield_now().await;
    cancel.cancel();
    let item = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("an outside cancel must wake the consumer")
        .unwrap();
    assert!(matches!(item, Some(Err(e)) if e.kind == ErrorKind::Cancelled));
}

// ---------------------------------------------------------------------------
// The whole table: role enforcement and transport parity
// ---------------------------------------------------------------------------

/// A router with a stub for every method: each fails `Failed("stub")`.
/// Reaching the stub proves the call passed authorization and parameter
/// decoding; the role decision is what the parity test checks.
fn stub_router() -> Router {
    // A generic visitor cannot express `M: Method<Result = StreamEnd>` for
    // only the streaming methods, so the three shapes are stubbed through
    // small macros over the explicit lists instead; the test below proves
    // the lists cover the whole table.
    let mut router = Router::new();
    macro_rules! stub_unary { ($($m:ty),* $(,)?) => {$(
        router.register::<$m, _, _>(|_c, _p| async {
            Err::<<$m as Method>::Result, _>(ControlError::failed("stub"))
        });
    )*}}
    macro_rules! stub_events { ($($m:ty),* $(,)?) => {$(
        router.register_events::<$m, _, _, _>(|_c, _p| async {
            Err::<stream::Empty<Result<<$m as Method>::Event, ControlError>>, _>(
                ControlError::failed("stub"),
            )
        });
    )*}}
    macro_rules! stub_chunks { ($($m:ty),* $(,)?) => {$(
        router.register_chunks::<$m, _, _, _>(|_c, _p| async {
            Err::<stream::Empty<Result<Bytes, ControlError>>, _>(ControlError::failed("stub"))
        });
    )*}}
    stub_unary!(
        NodePing,
        NodeStatus,
        NodeReintegrate,
        NodeLeave,
        NodeSetWriteMode,
        NodeDoctor,
        NodeOps,
        NodeHandoff,
        NodeLifecycle,
        PinAdd,
        PinRemove,
        PinList,
        DesignationOffline,
        DesignationDelegate,
        DesignationUndelegate,
        DesignationListDelegations,
        DesignationOnline,
        DesignationList,
        PruneRun,
        PruneList,
        GcRun,
        FsckRun,
        SnapshotCreate,
        SnapshotList,
        SnapshotDelete,
        SnapshotHold,
        SnapshotResolve,
        SnapshotDeleteMany,
        SnapshotReclaim,
        SnapshotSpace,
        SnapshotSpaceVerify,
        SnapshotRefs,
        SnapshotPolicyCheck,
        SnapshotPolicySimulate,
        SnapshotPolicyList,
        SnapshotPolicyShow,
        SnapshotPolicySet,
        SnapshotPolicyRemove,
        SnapshotPolicyPause,
        SnapshotSchedStatus,
        SnapshotSchedRun,
        CloneCreate,
        BrowseReaddir,
        BrowseInspect,
        BrowseStat,
        BrowseWrite,
        BrowseMkdir,
        BrowseRename,
        BrowseDelete,
        BrowseXattr,
        LocksForceRelease,
        LocksDropHeld,
        CacheList,
        CachePrune,
        QuotaSet,
        QuotaGet,
        ViewMount,
        ViewUnmount,
        ViewList,
        ViewStats,
        PeersList,
        FsList,
        FsCreate,
        FsImport,
        FsExport,
        FsPasswd,
        FsDoctor,
        FsUnlock,
    );
    stub_events!(StatsSubscribe, EventsSubscribe);
    stub_chunks!(NodeLogsTail, BrowseRead);
    router
}

#[test]
fn the_stub_router_covers_the_whole_table() {
    let router = stub_router();
    let mut expected: Vec<&str> = METHODS.iter().map(|m| m.name).collect();
    expected.sort_unstable();
    assert_eq!(router.registered(), expected);
}

/// Minimal valid params for a method, so the stub is reached.
fn sample_params(name: &str) -> serde_json::Value {
    match name {
        "pin.add"
        | "pin.remove"
        | "designation.undelegate"
        | "designation.online"
        | "browse.readdir"
        | "browse.inspect"
        | "browse.stat" => json!({"path": "/p"}),
        "designation.offline" => json!({"path": "/p"}),
        "designation.delegate" => json!({"path": "/p", "node": 2}),
        "node.set_write_mode" => json!({"mode": "back"}),
        "node.leave" | "gc.run" | "fsck.run" | "prune.run" | "cache.prune" | "quota.set"
        | "view.list" | "view.stats" | "snapshot.list" | "node.ops" | "node.handoff"
        | "stats.subscribe" | "events.subscribe" | "fs.doctor" => json!({}),
        "node.logs.tail" => json!({"lines": 5}),
        "node.lifecycle" => json!({"event": "Foreground"}),
        "snapshot.create" | "snapshot.delete" => json!({"selector": "s"}),
        "snapshot.refs" => json!({"id": "i"}),
        "snapshot.hold" => json!({"id": "i", "held": true}),
        "snapshot.policy.check" => json!({"expr": "1h:1d"}),
        "snapshot.policy.simulate" => json!({"expr": "1h:1d", "horizon_ms": 3_600_000}),
        "snapshot.policy.list" => json!({}),
        "snapshot.policy.show" | "snapshot.policy.remove" => json!({"path": "/p"}),
        "snapshot.policy.set" => json!({"path": "/p", "expr": "1h:1d"}),
        "snapshot.policy.pause" => json!({"path": "/p", "paused": true}),
        "snapshot.resolve" | "snapshot.delete_many" | "snapshot.reclaim" => {
            json!({"selectors": ["s"]})
        }
        "snapshot.sched.status" | "snapshot.space" | "snapshot.space.verify" => json!({}),
        "snapshot.sched.run" => json!({"dry_run": true}),
        "clone.create" => json!({"selector": "s", "destination": "/d"}),
        "browse.read" => json!({"path": "/p"}),
        "browse.write" => json!({"path": "/p", "data": ""}),
        "browse.mkdir" => json!({"path": "/p"}),
        "browse.rename" => json!({"from": "/a", "to": "/b"}),
        "browse.delete" => json!({"path": "/p"}),
        "browse.xattr" => json!({"path": "/p", "op": "List"}),
        "locks.force_release" => json!({"part": "p0"}),
        "locks.drop_held" => json!({"ino": 5}),
        "view.mount" => json!({"subtree": "/", "source": {"Path": {"mountpoint": "/m"}}}),
        "view.unmount" => json!({"mountpoint": "/m"}),
        "fs.create" => json!({"bucket": "b"}),
        "fs.import" => json!({"document": "d"}),
        "fs.export" => json!({"fs": "f"}),
        "fs.passwd" => json!({"fs": "f", "old_passphrase": "a", "new_passphrase": "b"}),
        "fs.unlock" => json!({"fs": "f", "credentials": {}}),
        _ => json!({}),
    }
}

/// The terminal error of `name` called over the socket. Streaming methods
/// are driven through their typed calls (the stub fails before any item, so
/// the error is the first thing the stream yields, or the open itself).
async fn socket_outcome(client: &Client, name: &str, params: serde_json::Value) -> ControlError {
    macro_rules! first_err {
        ($opened:expr) => {
            match $opened {
                Err(e) => e,
                Ok(mut s) => match s.next().await {
                    Some(Err(e)) => e,
                    other => panic!(
                        "{name}: expected the stub's error, got {:?}",
                        other.map(|_| ())
                    ),
                },
            }
        };
    }
    match name {
        "stats.subscribe" => {
            first_err!(client.subscribe::<StatsSubscribe>(Default::default()).await)
        }
        "events.subscribe" => first_err!(
            client
                .subscribe::<EventsSubscribe>(Default::default())
                .await
        ),
        "node.logs.tail" => first_err!(
            client
                .call_chunks::<NodeLogsTail>(LogTailParams {
                    lines: 1,
                    follow: false
                })
                .await
        ),
        "browse.read" => first_err!(
            client
                .call_chunks::<BrowseRead>(BrowseReadParams {
                    path: "/p".into(),
                    ..Default::default()
                })
                .await
        ),
        _ => client
            .call_json(name, params)
            .await
            .expect_err("the stub always fails"),
    }
}

async fn dispatch_outcome(
    router: &Router,
    principal: &Principal,
    m: &MethodInfo,
    params: serde_json::Value,
) -> ControlError {
    if m.streaming == StreamKind::None {
        dispatch_in_process(router, principal, m.name, params)
            .await
            .expect_err("the stub always fails")
    } else {
        match dispatch_stream_in_process(
            router,
            principal,
            m.name,
            params,
            DispatchOptions::default(),
        )
        .await
        {
            Err(e) => e,
            Ok(_) => panic!("{}: the stub should fail when the stream opens", m.name),
        }
    }
}

/// For each of viewer/operator/admin, over the real unix socket AND the
/// in-process dispatcher: a method is `Denied` exactly when the role is
/// below its minimum, and otherwise reaches the handler. The two paths must
/// agree on every method — the C5 parity property for the whole table.
#[tokio::test]
async fn the_whole_table_is_role_enforced_identically_over_socket_and_dispatch() {
    for held in Role::ALL {
        let policy = Policy::in_process_only().with_grant(Grant {
            subject: Subject::Uid(me()),
            role: held,
        });
        let server = start_unix(stub_router(), policy.clone(), null_audit());
        let client = Client::connect_unix(&server.path).await.unwrap();
        assert_eq!(client.welcome().roles, held.implied());
        let dispatcher = stub_router().with_policy(policy);
        let peer = client.welcome().principal.clone();

        for m in METHODS {
            let params = sample_params(m.name);
            let a = socket_outcome(&client, m.name, params.clone()).await;
            let b = dispatch_outcome(&dispatcher, &peer, m, params).await;
            let why = format!("{held} {}", m.name);
            assert_eq!(
                (a.kind, a.code),
                (b.kind, b.code),
                "{why}: socket vs dispatch"
            );
            if held < m.min_role {
                assert_eq!(a.kind, ErrorKind::Denied, "{why}");
            } else {
                assert_eq!(
                    (a.kind, a.message.as_str()),
                    (ErrorKind::Failed, "stub"),
                    "{why}"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Registration mistakes
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "streams")]
fn registering_a_streaming_method_as_unary_panics() {
    let mut r = Router::new();
    r.register::<EventsSubscribe, _, _>(|_c, _p| async { Ok(StreamEnd::default()) });
}

#[test]
#[should_panic(expected = "registered twice")]
fn registering_a_method_twice_panics() {
    let mut r = Router::new();
    r.register::<NodePing, _, _>(|_c, _p| async { Ok(Pong {}) });
    r.register::<NodePing, _, _>(|_c, _p| async { Ok(Pong {}) });
}

#[test]
#[should_panic(expected = "not a chunked method")]
fn registering_events_as_chunks_panics() {
    let mut r = Router::new();
    r.register_chunks::<EventsSubscribe, _, _, _>(|_c, _p| async {
        Ok(stream::empty::<Result<Bytes, ControlError>>())
    });
}

// ---------------------------------------------------------------------------
// Server lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn serve_shutdown_cancels_calls_and_removes_the_socket() {
    let probes = Probes::default();
    let server = start_unix(test_router(&probes), owner_policy(), null_audit());
    let path = server.path.clone();
    let client = Client::connect_unix(&path).await.unwrap();
    let pending = client.start::<NodeReintegrate>(Empty {}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    server.handle.shutdown().await;
    eventually("in-flight call cancelled by shutdown", || {
        probes.saw_cancel.load(Ordering::SeqCst)
    })
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("must finish");
    // The listener is gone with the server task: its socket file is
    // removed and nobody accepts any more.
    assert!(!path.exists());
    assert!(Client::connect_unix(&path).await.is_err());
}

#[tokio::test]
async fn connecting_to_nothing_says_the_daemon_may_not_be_running() {
    let dir = tempfile::tempdir().unwrap();
    let err = Client::connect_unix(&dir.path().join("absent.sock"))
        .await
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::Unavailable);
    assert!(err.remediation.unwrap().contains("daemon"));
}
