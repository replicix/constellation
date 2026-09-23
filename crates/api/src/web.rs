//! Localhost-only HTTP adapter and embedded operator UI.
//!
//! The server intentionally has no authentication because it binds only to
//! `127.0.0.1`. Remote operation belongs behind an SSH/iroh tunnel; widening
//! the bind address without adding authentication would expose destructive
//! control requests.

use crate::{dispatch, DownloadSession, Request, Response, StatusSource};
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures::stream;
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

#[derive(RustEmbed)]
#[folder = "webui/"]
struct Assets;

#[derive(Clone)]
struct AppState {
    source: Arc<dyn StatusSource>,
}

/// Start the optional localhost web endpoint. Port zero disables it.
pub async fn serve(port: u16, source: Arc<dyn StatusSource>) -> anyhow::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let state = AppState { source };
    let app = Router::new()
        .route("/api", post(api))
        .route("/api/status", get(status))
        .route("/api/download", get(download))
        .route("/metrics", get(metrics))
        .route("/", get(index))
        .route("/{*path}", get(asset))
        .with_state(state);
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(%error, "web UI server stopped");
        }
    });
    Ok(address)
}

async fn api(State(state): State<AppState>, Json(request): Json<Request>) -> Json<Response> {
    Json(adapt(state.source.as_ref(), request))
}

/// HTTP's transport-independent adapter, exposed for exhaustive parity tests.
pub fn adapt(source: &dyn StatusSource, request: Request) -> Response {
    dispatch(source, request)
}

async fn status(State(state): State<AppState>) -> Json<Response> {
    Json(dispatch(state.source.as_ref(), Request::Status))
}

#[derive(Debug, Deserialize)]
struct DownloadQuery {
    path: String,
}

async fn download(
    State(state): State<AppState>,
    Query(query): Query<DownloadQuery>,
) -> HttpResponse {
    match state.source.open_download(&query.path) {
        Ok(session) => streaming_download(session),
        Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
    }
}

fn streaming_download(session: DownloadSession) -> HttpResponse {
    let DownloadSession {
        file_name,
        size,
        chunks,
    } = session;
    let stream = stream::unfold(chunks, |mut chunks| async move {
        match chunks.recv().await {
            Some(Ok(buf)) => Some((Ok::<_, std::io::Error>(Bytes::from(buf)), chunks)),
            Some(Err(message)) => Some((Err(std::io::Error::other(message)), chunks)),
            None => None,
        }
    });
    let mut response = HttpResponse::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, size)
        .header(header::CONTENT_DISPOSITION, content_disposition(&file_name))
        .body(Body::from_stream(stream))
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

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let status = state.source.status();
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
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        output,
    )
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
