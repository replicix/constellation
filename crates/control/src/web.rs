//! The localhost HTTP adapter and the embedded operator UI (feature `web`),
//! speaking the control protocol (plan 31 §9.7: "the existing web adapter
//! keeps working, still loopback-only, now speaking the control protocol").
//!
//! ## Routes
//!
//! | route | what |
//! |---|---|
//! | `POST /api` | `{"method": "...", "params": {...}}` → `{"ok": result}` or, with a 4xx/5xx status, `{"error": ControlError}`; a chunked method's body is its bytes, a subscription's an NDJSON stream of events |
//! | `GET /api/status` | `node.status`'s report itself (what the UI polls) |
//! | `GET /api/download?path=` | a file's bytes (`browse.stat` + `browse.read`), streamed |
//! | `GET /metrics` | Prometheus gauges from `node.status`, and its `vfs_ops` as the `constellation_vfs_ops_total` counter and `constellation_vfs_op_seconds` histogram |
//! | `GET /`, `GET /{*path}` | the embedded UI (`webui/`) |
//!
//! Every call goes through [`dispatch_in_process`] /
//! [`dispatch_stream_in_process`] on the daemon's own [`Router`]: the same
//! authorization, audit and handlers as a unix-socket call (the C5 parity
//! test compares the two across the whole method table).
//!
//! ## Who the caller is
//!
//! There is no authentication yet (plan 33 adds tokens); the only
//! protections are the loopback bind and the DNS-rebinding guard below, as
//! before. Every HTTP call therefore runs as [`WEB_PRINCIPAL`]
//! ([`Principal::InProcess`], admin under every policy): exactly the access
//! the old adapter gave, and audited like any other mutating call.
//! One method is refused over HTTP whatever the principal
//! ([`HTTP_REFUSED`]): `node.handoff`, which executes a binary (or needs a
//! descriptor HTTP cannot carry) — the old `Upgrade` was socket-only too.
//!
//! ## The DNS-rebinding guard
//!
//! Binding loopback is not enough on its own: a page the operator visits in
//! a browser can point a name it controls at `127.0.0.1` (DNS rebinding) and
//! then issue *same-origin* requests to this API — fsck repair, prune,
//! `view.mount`, `node.leave`, every destructive method — with no credential
//! to steal because there is none. This is the class of CVE-2025-49596. The
//! [`guard_rebinding`] middleware closes it by refusing any request whose
//! `Host` header (or, when present, `Origin`) does not name the loopback
//! interface: a rebinding attacker's page still carries its own domain in
//! those headers, while `curl` and the same-page UI carry a loopback host.

use crate::authz::Principal;
use crate::methods::{method_info, NodeStatus, StreamKind};
use crate::proto::types::{FileStat, FuseStatus, StatusReport, VfsOpSeries, VfsOpsStatus};
use crate::proto::{ControlError, ErrorKind};
use crate::server::{
    dispatch_in_process, dispatch_stream_in_process, DispatchOptions, Router, StreamItem,
};
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::{get, post},
    Json,
};
use futures::StreamExt;
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

#[derive(RustEmbed)]
#[folder = "webui/"]
struct Assets;

/// Who every HTTP call runs as (see the module docs).
pub const WEB_PRINCIPAL: Principal = Principal::InProcess;

/// Methods the HTTP adapter refuses whatever the principal.
pub const HTTP_REFUSED: &[&str] = &["node.handoff"];

#[derive(Clone)]
struct AppState {
    router: Arc<Router>,
    principal: Principal,
}

/// Start the localhost web endpoint on `port` (0: any free port) and return
/// where it listens.
pub async fn serve(port: u16, router: Arc<Router>) -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let app = app(router);
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(%error, "web UI server stopped");
        }
    });
    Ok(address)
}

/// The whole HTTP application, guard included, for [`serve`] and for tests
/// that drive it in-process.
pub fn app(router: Arc<Router>) -> axum::Router {
    app_as(router, WEB_PRINCIPAL)
}

/// [`app`] with every call made as `principal` (the parity test's roles;
/// plan 33's authenticated callers).
pub fn app_as(router: Arc<Router>, principal: Principal) -> axum::Router {
    let state = AppState { router, principal };
    axum::Router::new()
        .route("/api", post(api))
        .route("/api/status", get(status))
        .route("/api/download", get(download))
        .route("/metrics", get(metrics))
        .route("/", get(index))
        .route("/{*path}", get(asset))
        .with_state(state)
        // Applied last so it wraps the whole router: every route, static
        // asset included, passes the DNS-rebinding check first.
        .layer(axum::middleware::from_fn(guard_rebinding))
}

/// Loopback authorities a browser or `curl` may legitimately name when
/// reaching this API. Anything else in a `Host`/`Origin` header is treated
/// as a rebinding attempt (see the module doc).
const ALLOWED_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];

/// Is `host` — a raw `Host` header value or the authority of an `Origin` —
/// one of the loopback names, with or without a trailing `:port`?
fn is_allowed_host(host: &str) -> bool {
    if ALLOWED_HOSTS.contains(&host) {
        return true;
    }
    // Strip a `:port` suffix and re-check. `rsplit_once` takes the *last*
    // colon: for `127.0.0.1:8080` / `localhost:8080` that is the port
    // separator, and for the bracketed IPv6 form `[::1]:8080` it is also
    // the port (the address's own colons stay inside the brackets). A bare
    // `[::1]`, whose last colon is inside the address, never reaches here —
    // it matched exactly above. We accept the stripped form only when what
    // remains is exactly an allowed host, so `evil.example:80` is refused.
    matches!(host.rsplit_once(':'), Some((h, _)) if ALLOWED_HOSTS.contains(&h))
}

/// The host part of an `Origin` header (`scheme://host[:port]`, or the
/// literal `null`) is a loopback name.
fn is_allowed_origin(origin: &str) -> bool {
    match origin.split_once("://") {
        // Origin never carries a path, so everything after `://` is the
        // authority; apply the same loopback rules as the `Host` check.
        Some((_, authority)) => is_allowed_host(authority),
        None => false,
    }
}

/// DNS-rebinding guard (see the module doc). Rejects with 403 and a
/// plain-text reason when the `Host` header is missing or names a
/// non-loopback address, or when an `Origin` header is present and points
/// at a cross-site page.
async fn guard_rebinding(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> HttpResponse {
    let headers = request.headers();
    // HTTP/1.1 requires a Host header; a request without one is either
    // malformed or a deliberate attempt to slip past the check, so refuse.
    let host_ok = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .is_some_and(is_allowed_host);
    if !host_ok {
        return (
            StatusCode::FORBIDDEN,
            "refused: Host header is missing or not a loopback address (DNS-rebinding guard)\n",
        )
            .into_response();
    }
    // Browsers attach Origin on cross-site fetches; a same-page UI fetch or
    // a `curl` sends none or a loopback one. A present, non-loopback Origin
    // is the rebinding signal even if the Host somehow passed.
    if let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    {
        if !is_allowed_origin(origin) {
            return (
                StatusCode::FORBIDDEN,
                "refused: cross-origin request to the localhost API (DNS-rebinding guard)\n",
            )
                .into_response();
        }
    }
    next.run(request).await
}

/// `POST /api`'s body.
#[derive(Debug, Deserialize)]
pub struct ApiRequest {
    pub method: String,
    #[serde(default = "empty_object")]
    pub params: serde_json::Value,
}

fn empty_object() -> serde_json::Value {
    serde_json::json!({})
}

/// The HTTP status an error answers with.
pub fn status_of(kind: ErrorKind) -> StatusCode {
    match kind {
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Denied => StatusCode::FORBIDDEN,
        ErrorKind::Invalid => StatusCode::BAD_REQUEST,
        ErrorKind::Unsupported => StatusCode::NOT_IMPLEMENTED,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::Timeout => StatusCode::GATEWAY_TIMEOUT,
        ErrorKind::Cancelled | ErrorKind::Failed => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn error_response(error: ControlError) -> HttpResponse {
    (
        status_of(error.kind),
        Json(serde_json::json!({ "error": error })),
    )
        .into_response()
}

/// Refuse what HTTP never carries ([`HTTP_REFUSED`]).
fn refused_over_http(method: &str) -> Option<ControlError> {
    HTTP_REFUSED.contains(&method).then(|| {
        ControlError::denied(format!("{method} is refused over HTTP"))
            .with_remediation("use the daemon's control socket (`constellation daemon --upgrade`)")
    })
}

/// One unary call over HTTP, as the parity test and `POST /api` make it.
pub async fn call_unary(
    router: &Router,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, ControlError> {
    if let Some(refusal) = refused_over_http(method) {
        return Err(refusal);
    }
    dispatch_in_process(router, &WEB_PRINCIPAL, method, params).await
}

async fn api(State(state): State<AppState>, Json(request): Json<ApiRequest>) -> HttpResponse {
    let ApiRequest { method, params } = request;
    if let Some(refusal) = refused_over_http(&method) {
        return error_response(refusal);
    }
    let streaming = method_info(&method).map_or(StreamKind::None, |m| m.streaming);
    if streaming == StreamKind::None {
        return match dispatch_in_process(&state.router, &state.principal, &method, params).await {
            Ok(result) => Json(serde_json::json!({ "ok": result })).into_response(),
            Err(error) => error_response(error),
        };
    }
    let stream = match dispatch_stream_in_process(
        &state.router,
        &state.principal,
        &method,
        params,
        DispatchOptions::default(),
    )
    .await
    {
        Ok(stream) => stream,
        Err(error) => return error_response(error),
    };
    let (content_type, body) = match streaming {
        StreamKind::Chunks => (
            "application/octet-stream",
            Body::from_stream(stream.filter_map(|item| async move {
                match item {
                    Ok(StreamItem::Chunk(bytes)) => Some(Ok(bytes)),
                    Ok(StreamItem::Event(_)) => None,
                    Err(e) => Some(Err(std::io::Error::other(e.message))),
                }
            })),
        ),
        _ => (
            "application/x-ndjson",
            Body::from_stream(stream.filter_map(|item| async move {
                match item {
                    Ok(StreamItem::Event(blob)) => match blob.decode::<serde_json::Value>() {
                        Ok(value) => {
                            let mut line = value.to_string().into_bytes();
                            line.push(b'\n');
                            Some(Ok(bytes::Bytes::from(line)))
                        }
                        Err(e) => Some(Err(std::io::Error::other(e.message))),
                    },
                    Ok(StreamItem::Chunk(_)) => None,
                    Err(e) => Some(Err(std::io::Error::other(e.message))),
                }
            })),
        ),
    };
    let mut response = HttpResponse::new(body);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn node_status(router: &Router, principal: &Principal) -> Result<StatusReport, ControlError> {
    let value = dispatch_in_process(
        router,
        principal,
        <NodeStatus as crate::methods::Method>::NAME,
        serde_json::json!({}),
    )
    .await?;
    serde_json::from_value(value).map_err(|e| ControlError::failed(e.to_string()))
}

async fn status(State(state): State<AppState>) -> HttpResponse {
    match node_status(&state.router, &state.principal).await {
        Ok(report) => Json(report).into_response(),
        Err(error) => error_response(error),
    }
}

#[derive(Debug, Deserialize)]
struct DownloadQuery {
    path: String,
}

/// `browse.stat` for the size and kind, then `browse.read` streamed as the
/// body: at most a chunk or two in memory, whatever the file's size.
async fn download(
    State(state): State<AppState>,
    Query(query): Query<DownloadQuery>,
) -> HttpResponse {
    let stat = match dispatch_in_process(
        &state.router,
        &state.principal,
        "browse.stat",
        serde_json::json!({ "path": query.path }),
    )
    .await
    .and_then(|v| {
        serde_json::from_value::<FileStat>(v).map_err(|e| ControlError::failed(e.to_string()))
    }) {
        Ok(stat) => stat,
        Err(error) => return (StatusCode::BAD_REQUEST, error.message).into_response(),
    };
    if stat.kind != "file" {
        return (
            StatusCode::BAD_REQUEST,
            format!("{}: not a regular file", stat.path),
        )
            .into_response();
    }
    let stream = match dispatch_stream_in_process(
        &state.router,
        &state.principal,
        "browse.read",
        serde_json::json!({ "path": query.path }),
        DispatchOptions::default(),
    )
    .await
    {
        Ok(stream) => stream,
        Err(error) => return (StatusCode::BAD_REQUEST, error.message).into_response(),
    };
    let body = stream.filter_map(|item| async move {
        match item {
            Ok(StreamItem::Chunk(bytes)) => Some(Ok::<_, std::io::Error>(bytes)),
            Ok(StreamItem::Event(_)) => None,
            Err(e) => Some(Err(std::io::Error::other(e.message))),
        }
    });
    let file_name = stat
        .path
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("download")
        .to_string();
    let mut response = HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, stat.size)
        .header(header::CONTENT_DISPOSITION, content_disposition(&file_name))
        .body(Body::from_stream(body))
        .expect("download response is valid");
    // Belt-and-suspenders: keep proxies from buffering the whole body.
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn content_disposition(file_name: &str) -> HeaderValue {
    let safe: String = file_name
        .chars()
        .map(|c| match c {
            '"' | '\\' | '\r' | '\n' => '_',
            c if c.is_ascii_graphic() || c == ' ' => c,
            _ => '_',
        })
        .collect();
    let name = if safe.is_empty() {
        "download"
    } else {
        safe.as_str()
    };
    HeaderValue::from_str(&format!("attachment; filename=\"{name}\""))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment; filename=\"download\""))
}

async fn metrics(State(state): State<AppState>) -> HttpResponse {
    match node_status(&state.router, &state.principal).await {
        Ok(status) => (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            render_metrics(&status),
        )
            .into_response(),
        Err(error) => error_response(error),
    }
}

/// The Prometheus text `/metrics` serves for `status`.
pub fn render_metrics(status: &StatusReport) -> String {
    let mut output = String::new();
    macro_rules! gauge {
        ($name:literal, $help:literal, $value:expr) => {
            output.push_str(concat!("# HELP ", $name, " ", $help, "\n"));
            output.push_str(concat!("# TYPE ", $name, " gauge\n"));
            output.push_str(&format!(concat!($name, " {}\n"), $value));
        };
    }
    gauge!(
        "constellation_spool_backlog",
        "Unshipped metadata records.",
        status.spool.journal_backlog
    );
    gauge!(
        "constellation_spool_head_sequence",
        "Highest observed log sequence.",
        status.spool.head_seq
    );
    gauge!(
        "constellation_speculation_outstanding",
        "Shadows and hints applied ahead of the log, not yet confirmed.",
        status.speculation.outstanding
    );
    gauge!(
        "constellation_speculation_pending_replay",
        "Stranded ops queued for replay by rid.",
        status.speculation.pending_replay
    );
    gauge!(
        "constellation_speculation_rolled_back_total",
        "Speculative entries rolled back because a later epoch stranded them.",
        status.speculation.rolled_back
    );
    gauge!(
        "constellation_speculation_replayed_total",
        "Stranded ops replayed by rid and accepted.",
        status.speculation.stranded_replayed
    );
    gauge!(
        "constellation_speculation_replay_conflicts_total",
        "Refused stranded-op replays materialized as conflict copies.",
        status.speculation.replay_conflicts
    );
    gauge!(
        "constellation_speculation_local",
        "This node's own unshipped transactions captured as speculation.",
        status.speculation.local
    );
    gauge!(
        "constellation_speculation_local_rolled_back_total",
        "This node's own unshipped transactions rolled back after a deposition.",
        status.speculation.local_rolled_back
    );
    gauge!(
        "constellation_speculation_depositions_total",
        "Deposition recoveries run.",
        status.speculation.depositions
    );
    gauge!(
        "constellation_speculation_epoch_markers_total",
        "Epoch-marker segments shipped after a takeover.",
        status.speculation.epoch_markers
    );
    gauge!(
        "constellation_speculation_copies_stalled",
        "Refused replays whose conflict copy has failed for 10 s or more.",
        status.speculation.copies_stalled
    );
    gauge!(
        "constellation_fsync_waiting",
        "fsyncs waiting out an unreachable S3 after a failed attempt (plan 39).",
        status.fsync.waiting
    );
    gauge!(
        "constellation_fsync_longest_wait_ms",
        "How long the oldest waiting fsync has waited, in ms.",
        status.fsync.longest_wait_ms
    );
    gauge!(
        "constellation_fsync_max_wait_ms",
        "The longest any fsync has waited since start, in ms.",
        status.fsync.max_wait_ms
    );
    gauge!(
        "constellation_fsync_retries_total",
        "fsync attempts retried after a transient S3 failure.",
        status.fsync.retries
    );
    gauge!(
        "constellation_fsync_timeouts_total",
        "fsyncs answered EIO by --fsync-timeout or the kernel request-timeout cap.",
        status.fsync.timeouts
    );
    gauge!(
        "constellation_fsync_permanent_errors_total",
        "fsyncs answered EIO for an S3 failure waiting does not fix.",
        status.fsync.permanent_errors
    );
    gauge!(
        "constellation_fsync_interrupted_total",
        "fsyncs answered EINTR.",
        status.fsync.interrupted
    );
    gauge!(
        "constellation_held_transactions",
        "Journaled transactions held back behind unrecoverable pending chunks.",
        status.held.transactions
    );
    gauge!(
        "constellation_inbox_submitted_ops_total",
        "Mutations this node forwarded through the S3 inbox (plan 30 M13).",
        status.inbox.submitted_ops
    );
    gauge!(
        "constellation_inbox_submitted_batches_total",
        "Inbox batch objects this node wrote.",
        status.inbox.submitted_batches
    );
    gauge!(
        "constellation_inbox_pending_ops",
        "Inbox-submitted mutations still waiting for their outcome in the log.",
        status.inbox.pending_ops
    );
    gauge!(
        "constellation_inbox_executed_ops_total",
        "Inbox-submitted mutations this node executed as holder.",
        status.inbox.executed_ops
    );
    gauge!(
        "constellation_inbox_refused_ops_total",
        "Inbox-submitted mutations this node refused as holder (Refused records).",
        status.inbox.refused_ops
    );
    gauge!(
        "constellation_inbox_deduped_ops_total",
        "Inbox batch positions answered without executing (rid or watermark dedup).",
        status.inbox.deduped_ops
    );
    gauge!(
        "constellation_inbox_drained_batches_total",
        "Older epochs' inbox batches drained inside a takeover gate.",
        status.inbox.drained_batches
    );
    gauge!(
        "constellation_inbox_polls_total",
        "GET-next inbox polls this node made as holder.",
        status.inbox.polls
    );
    gauge!(
        "constellation_inbox_poll_hits_total",
        "Inbox polls that found a batch.",
        status.inbox.poll_hits
    );
    gauge!(
        "constellation_inbox_withdrawn_ops_total",
        "Inbox batches this node withdrew (overwrote with a tombstone) before a P2P forward.",
        status.inbox.withdrawn_ops
    );
    gauge!(
        "constellation_inbox_tombstones_read_total",
        "Withdrawn inbox batches this node's polls read and stepped past as holder.",
        status.inbox.tombstones_read
    );
    gauge!(
        "constellation_inbox_unavailable_total",
        "Forwards the inbox could not take; they took the lease path.",
        status.inbox.unavailable
    );
    gauge!(
        "constellation_inbox_avg_round_trip_ms",
        "Mean inbox round trip (queue to outcome) on this node, ms.",
        status.inbox.avg_round_trip_ms
    );
    gauge!(
        "constellation_inbox_avg_batch_ops",
        "Mean ops per inbox batch this node submitted.",
        status.inbox.avg_batch_ops
    );
    gauge!(
        "constellation_inbox_escalations_total",
        "Times sustained inbox demand made this node ask for the lease (plan 30 M13 hybrid).",
        status.inbox.escalations
    );
    gauge!(
        "constellation_inbox_local_ops_total",
        "Mutations this node executed locally as holder.",
        status.inbox.local_ops
    );
    gauge!(
        "constellation_cache_used_bytes",
        "Bytes resident in the local chunk cache.",
        status.cache.used_bytes
    );
    gauge!(
        "constellation_cache_budget_bytes",
        "Configured local chunk cache budget.",
        status.cache.budget_bytes
    );
    gauge!(
        "constellation_cache_chunks",
        "Chunks resident in the local cache.",
        status.cache.chunks
    );
    gauge!(
        "constellation_cache_pinned_bytes",
        "Pinned bytes protected from eviction.",
        status.cache.pinned_bytes
    );
    gauge!(
        "constellation_cache_memory_budget_bytes",
        "Chunk memory cache budget (0: off).",
        status.cache.memory_budget_bytes
    );
    gauge!(
        "constellation_cache_memory_used_bytes",
        "Verified chunk bytes resident in the chunk memory cache.",
        status.cache.memory_used_bytes
    );
    gauge!(
        "constellation_cache_memory_chunks",
        "Chunks resident in the chunk memory cache.",
        status.cache.memory_chunks
    );
    gauge!(
        "constellation_cache_memory_protected_bytes",
        "Chunk memory cache bytes in the protected (reused) segment.",
        status.cache.memory_protected_bytes
    );
    gauge!(
        "constellation_cache_memory_hits_total",
        "Chunk reads served from the chunk memory cache.",
        status.cache.memory_hits
    );
    gauge!(
        "constellation_cache_memory_misses_total",
        "Chunk reads that loaded and verified the disk cache copy.",
        status.cache.memory_misses
    );
    gauge!(
        "constellation_cache_memory_coalesced_total",
        "Chunk reads that waited for a concurrent load of the same chunk.",
        status.cache.memory_coalesced
    );
    gauge!(
        "constellation_cache_memory_evictions_total",
        "Chunks evicted from the chunk memory cache.",
        status.cache.memory_evictions
    );
    gauge!(
        "constellation_coop_peer_hits_total",
        "Cooperative cache peer hits.",
        status.coop.peer_hits
    );
    gauge!(
        "constellation_coop_peer_misses_total",
        "Cooperative-cache peer declines (busy, absent, recently removed).",
        status.coop.peer_misses
    );
    gauge!(
        "constellation_coop_peer_false_positives_total",
        "Peer fetches the holder answered Absent although our digest claimed it.",
        status.coop.peer_false_positives
    );
    gauge!(
        "constellation_coop_peer_stale_misses_total",
        "Peer fetches of a chunk the holder had just dropped (propagation race).",
        status.coop.peer_stale_misses
    );
    gauge!(
        "constellation_coop_digest_bytes_sent_total",
        "Cooperative-cache digest-plane bytes sent.",
        status.coop.digest_bytes_sent
    );
    gauge!(
        "constellation_coop_digest_bytes_received_total",
        "Cooperative-cache digest-plane bytes received.",
        status.coop.digest_bytes_received
    );
    gauge!(
        "constellation_coop_digest_cpu_us_total",
        "Microseconds spent building, applying and answering digests.",
        status.coop.digest_cpu_us
    );
    gauge!(
        "constellation_coop_reconcile_rounds_total",
        "Exact-mode reconciliation rounds this node initiated.",
        status.coop.reconcile_rounds
    );
    gauge!(
        "constellation_coop_peer_set_entries",
        "Entries held about peers' caches (mirror keys or bloom inserts).",
        status.coop.peer_set_entries
    );
    gauge!(
        "constellation_coop_peer_errors_total",
        "Cooperative-cache peer transport/hash failures.",
        status.coop.peer_errors
    );
    gauge!(
        "constellation_coop_s3_fetches_total",
        "Successful cooperative-cache S3 fetches.",
        status.coop.s3_fetches
    );
    gauge!(
        "constellation_coop_hedges_total",
        "Cooperative-cache hedges fired.",
        status.coop.hedges_fired
    );
    gauge!(
        "constellation_prefetch_inflight",
        "Background chunk fetches currently active.",
        status.prefetch.inflight
    );
    gauge!(
        "constellation_prefetch_queued",
        "Background chunks queued behind the active fetches.",
        status.prefetch.queued
    );
    gauge!(
        "constellation_prefetch_streams",
        "Live sequential prefetch streams.",
        status.prefetch.streams
    );
    gauge!(
        "constellation_prefetch_window_bytes",
        "Largest live sequential prefetch window.",
        status.prefetch.window_bytes
    );
    gauge!(
        "constellation_prefetch_stalls_total",
        "Demand reads that expanded a prefetch window.",
        status.prefetch.stalls
    );
    gauge!(
        "constellation_prefetch_gate_target",
        "Current adaptive background-fetch concurrency target.",
        status.prefetch.gate_target
    );
    gauge!(
        "constellation_scan_ahead_files_total",
        "Files submitted by directory scan-ahead.",
        status.prefetch.scan_ahead_files
    );
    gauge!(
        "constellation_scan_ahead_bytes_total",
        "Logical bytes submitted by directory scan-ahead.",
        status.prefetch.scan_ahead_bytes
    );
    gauge!(
        "constellation_existence_bloom_hits_total",
        "Upload decisions hinted present by the local existence bloom.",
        status.writeback.existence_bloom_hits
    );
    gauge!(
        "constellation_existence_chunk_ref_hits_total",
        "Upload decisions hinted present by the replica's chunk_ref index.",
        status.writeback.existence_chunk_ref_hits
    );
    gauge!(
        "constellation_existence_misses_total",
        "Upload decisions no existence hint could answer.",
        status.writeback.existence_misses
    );
    gauge!(
        "constellation_existence_peer_hints_total",
        "Upload probes selected by live cooperative-cache digests.",
        status.writeback.existence_peer_hints
    );
    gauge!(
        "constellation_lease_held",
        "Whether this node holds the p0 lease.",
        u8::from(status.lease.held)
    );
    gauge!(
        "constellation_lease_epoch",
        "Current p0 lease epoch.",
        status.lease.epoch
    );
    gauge!(
        "constellation_prune_runs_total",
        "Retention prune passes completed.",
        status.prune.runs
    );
    gauge!(
        "constellation_prune_deleted_total",
        "Entries removed by retention pruning.",
        status.prune.deleted
    );
    gauge!(
        "constellation_prune_bytes_freed_total",
        "Bytes freed by retention pruning (last-link unlinks).",
        status.prune.bytes_freed
    );
    gauge!(
        "constellation_prune_unparseable_roots",
        "Marked roots whose policy failed to parse (fail-closed).",
        status.prune.unparseable_roots
    );
    gauge!(
        "constellation_prune_inert_roots",
        "Marked roots inert for want of a quota (percentage lru).",
        status.prune.inert_roots
    );
    gauge!(
        "constellation_prune_refused_lag_total",
        "Prune passes refused because the replica was too stale.",
        status.prune.refused_lag
    );
    gauge!(
        "constellation_snapacct_building",
        "Whether the space-accounting index is behind the snapshot rows (queries answer building).",
        u8::from(status.snapacct.building)
    );
    gauge!(
        "constellation_snapacct_build_progress_pct",
        "Share of snapshot rows the accounting index has applied while building.",
        status.snapacct.build_progress_pct
    );
    gauge!(
        "constellation_snapacct_indexed_chunks",
        "Chunks held by at least one snapshot, as indexed.",
        status.snapacct.indexed_chunks
    );
    gauge!(
        "constellation_snapacct_index_bytes",
        "On-disk size of the accounting index's tables.",
        status.snapacct.index_bytes
    );
    gauge!(
        "constellation_snapacct_as_of_seq",
        "The commit the accounting numbers are as of.",
        status.snapacct.as_of_seq
    );
    gauge!(
        "constellation_snapacct_refresh_ms_last",
        "Duration of the last live-tree refresh, milliseconds.",
        status.snapacct.refresh_ms_last
    );
    gauge!(
        "constellation_snapacct_verify_mismatches",
        "Mismatches the last accounting verify found.",
        status.snapacct.verify_mismatches
    );
    gauge!(
        "constellation_snapacct_stalled_chains",
        "Snapshot chains the accounting index could not apply in its last pass.",
        status.snapacct.stalled_chains
    );
    gauge!(
        "constellation_snapacct_refreshes_deferred_total",
        "Live refreshes deferred because the replica had not applied the newest commit.",
        status.snapacct.refreshes_deferred
    );
    render_vfs_ops(&mut output, &status.vfs_ops);
    render_fuse(&mut output, &status.fuse);
    output
}

/// A Prometheus label value: backslash, quote and newline escaped.
fn label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

/// The unified op metrics (plan 31 §6.10) as a counter and a histogram:
/// `constellation_vfs_ops_total{frontend,view,op,outcome,transport}` and
/// `constellation_vfs_op_seconds{frontend,view,op,transport}`. `view` (the
/// allowlisted metric label of a view, plan 31 §9.10) is left out for a
/// view that has none; `transport` (plan 38 §5) is always there —
/// `dev_fuse`/`uring`/`uring_zc` on FUSE rows, `n/a` on any other.
fn render_vfs_ops(output: &mut String, ops: &VfsOpsStatus) {
    use std::fmt::Write;
    output.push_str(
        "# HELP constellation_vfs_ops_total Frontend operations completed, by outcome (ok or the error code's name).\n\
         # TYPE constellation_vfs_ops_total counter\n",
    );
    let labels = |s: &VfsOpSeries| {
        let mut l = format!("frontend=\"{}\"", label_value(&s.frontend));
        if let Some(view) = &s.view {
            let _ = write!(l, ",view=\"{}\"", label_value(view));
        }
        let _ = write!(l, ",op=\"{}\"", label_value(&s.op));
        let _ = write!(l, ",transport=\"{}\"", label_value(&s.transport));
        l
    };
    for series in &ops.series {
        let l = labels(series);
        for (outcome, n) in &series.outcomes {
            let _ = writeln!(
                output,
                "constellation_vfs_ops_total{{{l},outcome=\"{}\"}} {n}",
                label_value(outcome)
            );
        }
    }
    output.push_str(
        "# HELP constellation_vfs_op_seconds Frontend operation latency, from the request's arrival to its completion.\n\
         # TYPE constellation_vfs_op_seconds histogram\n",
    );
    for series in &ops.series {
        let l = labels(series);
        let mut cumulative = 0u64;
        for (i, n) in series.buckets.iter().enumerate() {
            cumulative += n;
            match ops.bucket_bounds_s.get(i) {
                Some(bound) => {
                    let _ = writeln!(
                        output,
                        "constellation_vfs_op_seconds_bucket{{{l},le=\"{bound}\"}} {cumulative}"
                    );
                }
                None => {
                    let _ = writeln!(
                        output,
                        "constellation_vfs_op_seconds_bucket{{{l},le=\"+Inf\"}} {cumulative}"
                    );
                }
            }
        }
        let _ = writeln!(
            output,
            "constellation_vfs_op_seconds_sum{{{l}}} {}",
            series.sum_ns as f64 / 1e9
        );
        let _ = writeln!(
            output,
            "constellation_vfs_op_seconds_count{{{l}}} {cumulative}"
        );
    }
}

/// Plan 38 §5's FUSE transport metrics: the process-wide
/// `constellation_fuse_transport_fallbacks_total{from,to,reason}` and
/// `constellation_fuse_zero_copy_reads_total` counters, and per mount (by
/// `mountpoint`, as many series as there are mounts) the
/// `constellation_fuse_passthrough_opens` and
/// `constellation_fuse_uring_queue_depth` gauges.
fn render_fuse(output: &mut String, fuse: &FuseStatus) {
    use std::fmt::Write;
    output.push_str(
        "# HELP constellation_fuse_transport_fallbacks_total FUSE sessions that asked for the io_uring transport and were served over another, by the rung that refused.\n\
         # TYPE constellation_fuse_transport_fallbacks_total counter\n",
    );
    for f in &fuse.transport_fallbacks {
        let _ = writeln!(
            output,
            "constellation_fuse_transport_fallbacks_total{{from=\"{}\",to=\"{}\",reason=\"{}\"}} {}",
            label_value(&f.from),
            label_value(&f.to),
            label_value(&f.reason),
            f.count
        );
    }
    let _ = write!(
        output,
        "# HELP constellation_fuse_zero_copy_reads_total FUSE reads served zero-copy from a registered buffer.\n\
         # TYPE constellation_fuse_zero_copy_reads_total counter\n\
         constellation_fuse_zero_copy_reads_total {}\n",
        fuse.zero_copy_reads_total
    );
    output.push_str(
        "# HELP constellation_fuse_passthrough_opens Open FUSE handles the kernel reads straight from a cached chunk file.\n\
         # TYPE constellation_fuse_passthrough_opens gauge\n",
    );
    for m in &fuse.mounts {
        let _ = writeln!(
            output,
            "constellation_fuse_passthrough_opens{{mountpoint=\"{}\"}} {}",
            label_value(&m.mountpoint),
            m.passthrough.opens
        );
    }
    output.push_str(
        "# HELP constellation_fuse_uring_queue_depth Ring entries per kernel queue of a FUSE mount's io_uring transport (0: /dev/fuse).\n\
         # TYPE constellation_fuse_uring_queue_depth gauge\n",
    );
    for m in &fuse.mounts {
        let _ = writeln!(
            output,
            "constellation_fuse_uring_queue_depth{{mountpoint=\"{}\",transport=\"{}\"}} {}",
            label_value(&m.mountpoint),
            label_value(&m.transport),
            m.uring_queue_depth
        );
    }
}

async fn index() -> HttpResponse {
    embedded("index.html")
}

async fn asset(axum::extract::Path(path): axum::extract::Path<String>) -> HttpResponse {
    embedded(path.trim_start_matches('/'))
}

fn embedded(path: &str) -> HttpResponse {
    match Assets::get(path) {
        Some(file) => HttpResponse::builder()
            .header(
                header::CONTENT_TYPE,
                mime_guess::from_path(path).first_or_octet_stream().as_ref(),
            )
            .body(Body::from(file.data))
            .expect("static response is valid"),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt; // for `oneshot`

    /// An empty router: the guard tests never reach a handler that needs one.
    fn empty() -> Arc<Router> {
        Arc::new(Router::new())
    }

    /// `node.handoff` executes a binary (or needs a descriptor): HTTP refuses
    /// it before the router sees it, whatever the principal's role.
    #[tokio::test]
    async fn handoff_is_refused_over_http_only() {
        let err = call_unary(&Router::new(), "node.handoff", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Denied);
        assert!(err.message.contains("HTTP"), "{err}");
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/api")
            .header("host", "localhost")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"method":"node.handoff","params":{}}"#))
            .unwrap();
        let response = app(empty()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    /// An unregistered method is `Unsupported` (501) with the error object.
    #[tokio::test]
    async fn api_errors_carry_the_control_error() {
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/api")
            .header("host", "127.0.0.1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"method":"pin.list"}"#))
            .unwrap();
        let response = app(empty()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["error"]["kind"], "unsupported");
    }

    /// Drive `GET /` through the full router with the given headers and
    /// return the response status. `/` maps to the embedded `index.html`,
    /// which exists, so an allowed request yields `200 OK` and a rejected
    /// one `403 FORBIDDEN` from the guard.
    async fn get_root(headers: &[(&str, &str)]) -> StatusCode {
        let app = app(empty());
        let mut builder = HttpRequest::builder().method("GET").uri("/");
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(Body::empty()).unwrap();
        app.oneshot(request).await.unwrap().status()
    }

    #[tokio::test]
    async fn missing_host_is_refused() {
        assert_eq!(get_root(&[]).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn foreign_host_is_refused() {
        assert_eq!(
            get_root(&[("host", "evil.example")]).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn loopback_ip_with_port_is_allowed() {
        assert_eq!(
            get_root(&[("host", "127.0.0.1:8080")]).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn localhost_is_allowed() {
        assert_eq!(get_root(&[("host", "localhost")]).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn ipv6_loopback_with_port_is_allowed() {
        assert_eq!(get_root(&[("host", "[::1]:8080")]).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn cross_site_origin_is_refused() {
        assert_eq!(
            get_root(&[("host", "localhost"), ("origin", "http://evil.example")]).await,
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn loopback_origin_is_allowed() {
        assert_eq!(
            get_root(&[("host", "localhost"), ("origin", "http://localhost:8080")]).await,
            StatusCode::OK
        );
    }
    #[test]
    fn vfs_op_metrics_render_as_a_counter_and_a_cumulative_histogram() {
        let ops = VfsOpsStatus {
            bucket_bounds_s: vec![0.00001, 0.5],
            series: vec![
                VfsOpSeries {
                    frontend: "fuse".into(),
                    view: Some("pv-\"1\"".into()),
                    transport: "dev_fuse".into(),
                    op: "getattr".into(),
                    outcomes: [("NotFound".to_string(), 1), ("ok".to_string(), 2)].into(),
                    buckets: vec![1, 1, 1],
                    sum_ns: 1_500_000_000,
                },
                VfsOpSeries {
                    frontend: "fuse".into(),
                    view: None,
                    transport: "n/a".into(),
                    op: "read".into(),
                    outcomes: [("ok".to_string(), 1)].into(),
                    buckets: vec![1, 0, 0],
                    sum_ns: 5_000,
                },
            ],
        };
        let mut out = String::new();
        render_vfs_ops(&mut out, &ops);
        for line in [
            "# TYPE constellation_vfs_ops_total counter",
            "constellation_vfs_ops_total{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\",outcome=\"ok\"} 2",
            "constellation_vfs_ops_total{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\",outcome=\"NotFound\"} 1",
            "constellation_vfs_ops_total{frontend=\"fuse\",op=\"read\",transport=\"n/a\",outcome=\"ok\"} 1",
            "# TYPE constellation_vfs_op_seconds histogram",
            "constellation_vfs_op_seconds_bucket{frontend=\"fuse\",op=\"read\",transport=\"n/a\",le=\"0.00001\"} 1",
            "constellation_vfs_op_seconds_bucket{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\",le=\"0.5\"} 2",
            "constellation_vfs_op_seconds_bucket{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\",le=\"+Inf\"} 3",
            "constellation_vfs_op_seconds_sum{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\"} 1.5",
            "constellation_vfs_op_seconds_count{frontend=\"fuse\",view=\"pv-\\\"1\\\"\",op=\"getattr\",transport=\"dev_fuse\"} 3",
        ] {
            assert!(out.lines().any(|l| l == line), "{line}\n{out}");
        }
    }

    #[test]
    fn fuse_transport_metrics_render_per_mount_and_process_wide() {
        use crate::proto::types::{FuseFallbackCount, FuseMountStatus, FusePassthroughStatus};
        let fuse = FuseStatus {
            mounts: vec![
                FuseMountStatus {
                    id: 1,
                    mountpoint: "/mnt/a".into(),
                    transport: "uring".into(),
                    uring_queue_depth: 8,
                    passthrough: FusePassthroughStatus {
                        enabled: true,
                        opens: 3,
                        unavailable_reason: None,
                    },
                    ..Default::default()
                },
                FuseMountStatus {
                    id: 2,
                    mountpoint: "/mnt/\"b\"".into(),
                    transport: "dev_fuse".into(),
                    ..Default::default()
                },
            ],
            transport_fallbacks: vec![FuseFallbackCount {
                from: "uring".into(),
                to: "dev_fuse".into(),
                reason: "kernel_not_offered".into(),
                count: 2,
            }],
            zero_copy_reads_total: 0,
        };
        let mut out = String::new();
        render_fuse(&mut out, &fuse);
        for line in [
            "# TYPE constellation_fuse_transport_fallbacks_total counter",
            "constellation_fuse_transport_fallbacks_total{from=\"uring\",to=\"dev_fuse\",reason=\"kernel_not_offered\"} 2",
            "# TYPE constellation_fuse_zero_copy_reads_total counter",
            "constellation_fuse_zero_copy_reads_total 0",
            "# TYPE constellation_fuse_passthrough_opens gauge",
            "constellation_fuse_passthrough_opens{mountpoint=\"/mnt/a\"} 3",
            "constellation_fuse_passthrough_opens{mountpoint=\"/mnt/\\\"b\\\"\"} 0",
            "# TYPE constellation_fuse_uring_queue_depth gauge",
            "constellation_fuse_uring_queue_depth{mountpoint=\"/mnt/a\",transport=\"uring\"} 8",
            "constellation_fuse_uring_queue_depth{mountpoint=\"/mnt/\\\"b\\\"\",transport=\"dev_fuse\"} 0",
        ] {
            assert!(out.lines().any(|l| l == line), "{line}\n{out}");
        }
    }
}
