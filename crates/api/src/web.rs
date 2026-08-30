//! Localhost-only HTTP adapter and embedded operator UI.
//!
//! The server intentionally has no authentication because it binds only to
//! `127.0.0.1`. Remote operation belongs behind an SSH/iroh tunnel; widening
//! the bind address without adding authentication would expose destructive
//! control requests.

use crate::{dispatch, Request, Response, StatusSource};
use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response as HttpResponse},
    routing::{get, post},
    Json, Router,
};
use rust_embed::RustEmbed;
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
        "constellation_existence_listed",
        "Chunk keys admitted by the mount-time S3 LIST seed.",
        status.writeback.existence_listed
    );
    gauge!(
        "constellation_existence_complete",
        "Whether the S3 existence LIST seed completed within its memory cap.",
        u8::from(status.writeback.existence_complete)
    );
    gauge!(
        "constellation_existence_bloom_hits_total",
        "Upload decisions whose complete existence filter reported present.",
        status.writeback.existence_bloom_hits
    );
    gauge!(
        "constellation_existence_bloom_misses_total",
        "Upload decisions whose complete existence filter proved absent.",
        status.writeback.existence_bloom_misses
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
        "constellation_partitions",
        "Partitions visible in the local replica.",
        status.partitions.len()
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
