//! The server runtime: a [`Router`] of typed handlers, and [`serve`] to put it
//! on a listener.
//!
//! ## One path for every caller
//!
//! Everything that answers a control call — a unix-socket connection, an
//! in-process [`Client`](crate::client::Client), the web adapter's
//! [`dispatch_in_process`] — goes through `Router::start`:
//!
//! 1. **Identify.** The transport supplies a [`Principal`]; the router's
//!    [`Policy`] maps it to a role. A principal with *no* role is denied
//!    even for method names that do not exist, so a stranger cannot map the
//!    method table.
//! 2. **Resolve.** Unknown method (or one this daemon registered no handler
//!    for) → `Unsupported`.
//! 3. **Authorize.** `M::MIN_ROLE` against the principal's role → `Denied`
//!    before the handler exists.
//! 4. **Audit ticket.** For mutating methods (including denied attempts) an
//!    an audit ticket is opened; whoever finishes the call completes it with
//!    the outcome, and a ticket dropped uncompleted (a task aborted mid-call)
//!    records `cancelled` rather than silently vanishing.
//! 5. **Decode and run.** The typed wrapper decodes the params blob, checks
//!    the fd requirement, and calls the handler.
//!
//! The web adapter, the CLI in-process and a socket client therefore differ
//! only in how bytes arrive; the C5 parity test compares them across the whole
//! method table.
//!
//! ## Concurrency and cancellation
//!
//! Each `Request` runs in its own task, so a slow call never blocks a fast
//! one on the same connection. `Cancel{id}` cancels that call's
//! [`CancellationToken`] (visible as [`CallCtx::cancel`]). A handler is
//! expected to notice and return promptly; if it does not within
//! [`ServeOptions::cancel_grace`] the future is dropped and the call
//! completes `Cancelled`/`EINTR` anyway — a stuck handler must not leave a
//! client waiting for a cancel it asked for. If the handler finishes inside
//! the grace period with a result, that result is what the client gets (the
//! work was done). Dropping the connection cancels all of its calls.
//!
//! Subscriptions and chunked results are streams: the task pumps items to
//! `Event`/`Chunk` frames until the stream ends, errors, or is cancelled,
//! then sends the one terminal `Response`.
//!
//! ## Limits
//!
//! [`ServeOptions::max_inflight_per_connection`] bounds concurrent calls per
//! connection (`Unavailable` beyond it) so one client cannot spawn unbounded
//! tasks; the handshake has a timeout so a connection that never says Hello
//! does not hold a task forever.

use crate::audit::{
    now_unix_ms, params_digest, AuditOutcome, AuditPrincipal, AuditRecord, AuditSink,
    NullAuditSink, WITHHELD_DIGEST,
};
use crate::authz::{no_role, Policy, Principal, Resolution, Role};
use crate::fd::OwnedFd;
use crate::methods::{method_info, Method, MethodInfo, StreamKind};
use crate::proto::{
    negotiate, Blob, Cancel, Chunk, ControlError, Encoding, ErrorKind, Event, Hello, Outcome,
    Request, Response, StreamEnd, Welcome, FEATURE_FD_PASSING, SUPPORTED_ENCODINGS,
};
use crate::proto::{FrameKind, MAX_FRAME_LEN};
use crate::transport::{Frame, Listener, Transport};
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use serde::Serialize;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The biggest single `Chunk` the server emits; larger buffers from a
/// handler are split. Well under the frame limit, big enough that framing
/// overhead vanishes.
pub const MAX_CHUNK_BYTES: usize = 1024 * 1024;

/// Tunables of a [`Router`]/[`serve`].
#[derive(Debug, Clone)]
pub struct ServeOptions {
    /// Encodings the server accepts; the client's first match wins.
    pub encodings: Vec<Encoding>,
    /// How long a new connection may take to send `Hello`.
    pub handshake_timeout: Duration,
    /// How long a cancelled unary handler may keep running before its
    /// future is dropped.
    pub cancel_grace: Duration,
    /// Concurrent calls per connection.
    pub max_inflight_per_connection: usize,
}

impl Default for ServeOptions {
    fn default() -> Self {
        ServeOptions {
            encodings: SUPPORTED_ENCODINGS.to_vec(),
            handshake_timeout: Duration::from_secs(10),
            cancel_grace: Duration::from_secs(2),
            max_inflight_per_connection: 256,
        }
    }
}

/// What a handler receives besides its parameters.
#[derive(Debug)]
pub struct CallCtx {
    /// Who is calling.
    pub principal: Principal,
    /// The role that admitted them.
    pub role: Role,
    /// Cancelled by `Cancel{id}` or a dropped connection. Long handlers
    /// `select!` on `cancel.cancelled()`.
    pub cancel: CancellationToken,
    /// The request id (for logs).
    pub call_id: u64,
    /// The method being served.
    pub method: &'static str,
    /// The connection's encoding.
    pub encoding: Encoding,
    fd: Option<OwnedFd>,
    fd_passing: bool,
}

impl CallCtx {
    /// A context for tests and embedders calling a handler directly.
    pub fn detached(principal: Principal, role: Role) -> CallCtx {
        CallCtx {
            principal,
            role,
            cancel: CancellationToken::new(),
            call_id: 0,
            method: "",
            encoding: Encoding::Json,
            fd: None,
            fd_passing: true,
        }
    }

    /// The file descriptor attached to the request, if any. Taking it
    /// transfers ownership.
    pub fn take_fd(&mut self) -> Option<OwnedFd> {
        self.fd.take()
    }

    /// Like [`take_fd`](Self::take_fd) but an absent fd is the error a
    /// handler should return. (The router has already answered
    /// `NotSupported`/`Invalid` for methods whose
    /// [`requires_fd`](Method::requires_fd) says so; this is for methods that
    /// take an *optional* fd.)
    pub fn require_fd(&mut self) -> Result<OwnedFd, ControlError> {
        self.take_fd().ok_or_else(|| {
            if self.fd_passing {
                ControlError::invalid(format!("{} needs a file descriptor attached", self.method))
            } else {
                ControlError::unsupported("this transport cannot pass file descriptors")
            }
        })
    }

    /// Whether the connection can carry descriptors at all.
    pub fn fd_passing(&self) -> bool {
        self.fd_passing
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// One item of a streaming call, before framing.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    Event(Blob),
    Chunk(Bytes),
}

type UnaryFn =
    dyn Fn(CallCtx, Blob) -> BoxFuture<'static, Result<Blob, ControlError>> + Send + Sync;
type ItemStream = BoxStream<'static, Result<StreamItem, ControlError>>;
type StreamFn =
    dyn Fn(CallCtx, Blob) -> BoxFuture<'static, Result<ItemStream, ControlError>> + Send + Sync;

enum Handler {
    Unary(Box<UnaryFn>),
    Stream(Box<StreamFn>),
}

struct Entry {
    info: MethodInfo,
    handler: Handler,
}

/// Typed handlers by method, plus the policy and audit sink they run under.
pub struct Router {
    handlers: HashMap<&'static str, Entry>,
    policy: Arc<Policy>,
    audit: Arc<dyn AuditSink>,
    features: Vec<String>,
    options: ServeOptions,
}

impl Default for Router {
    fn default() -> Self {
        Router::new()
    }
}

fn check_fd<M: Method>(ctx: &CallCtx, params: &M::Params) -> Result<(), ControlError> {
    if !M::requires_fd(params) {
        return Ok(());
    }
    if !ctx.fd_passing {
        return Err(ControlError::unsupported(format!(
            "{} with these parameters needs a file descriptor, and this transport cannot pass one",
            M::NAME
        ))
        .with_remediation("connect over the daemon's unix socket"));
    }
    if ctx.fd.is_none() {
        return Err(ControlError::invalid(format!(
            "{} with these parameters needs a file descriptor attached to the request",
            M::NAME
        )));
    }
    Ok(())
}

impl Router {
    /// An empty router: no handlers, the default policy (the current user
    /// and the in-process caller are admins on unix; only the in-process
    /// caller elsewhere), no audit sink.
    pub fn new() -> Router {
        #[cfg(unix)]
        let policy = Policy::current_user();
        #[cfg(not(unix))]
        let policy = Policy::in_process_only();
        Router {
            handlers: HashMap::new(),
            policy: Arc::new(policy),
            audit: Arc::new(NullAuditSink),
            features: Vec::new(),
            options: ServeOptions::default(),
        }
    }

    pub fn with_policy(mut self, policy: Policy) -> Router {
        self.policy = Arc::new(policy);
        self
    }

    pub fn with_audit(mut self, audit: Arc<dyn AuditSink>) -> Router {
        self.audit = audit;
        self
    }

    pub fn with_options(mut self, options: ServeOptions) -> Router {
        self.options = options;
        self
    }

    /// Advertise an extra `Welcome::features` entry.
    pub fn with_feature(mut self, feature: impl Into<String>) -> Router {
        self.features.push(feature.into());
        self
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    pub fn options(&self) -> &ServeOptions {
        &self.options
    }

    /// Whether a handler is registered for `name`.
    pub fn handles(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }

    /// The registered methods' names, sorted.
    pub fn registered(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.handlers.keys().copied().collect();
        names.sort_unstable();
        names
    }

    fn insert<M: Method>(&mut self, handler: Handler) {
        let previous = self.handlers.insert(
            M::NAME,
            Entry {
                info: M::INFO,
                handler,
            },
        );
        assert!(
            previous.is_none(),
            "handler for {} registered twice",
            M::NAME
        );
    }

    /// Register the handler of a non-streaming method.
    ///
    /// # Panics
    ///
    /// If `M` is a streaming method or already has a handler — both are
    /// programmer errors caught at startup.
    pub fn register<M, F, Fut>(&mut self, handler: F) -> &mut Router
    where
        M: Method,
        F: Fn(CallCtx, M::Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M::Result, ControlError>> + Send + 'static,
    {
        assert_eq!(
            M::STREAMING,
            StreamKind::None,
            "{} streams: use register_events or register_chunks",
            M::NAME
        );
        let wrapped: Box<UnaryFn> = Box::new(move |ctx: CallCtx, params: Blob| {
            let encoding = ctx.encoding;
            let params = match params.decode::<M::Params>() {
                Ok(p) => p,
                Err(e) => return Box::pin(async move { Err(e) }),
            };
            if let Err(e) = check_fd::<M>(&ctx, &params) {
                return Box::pin(async move { Err(e) });
            }
            let fut = handler(ctx, params);
            Box::pin(async move { Blob::encode(encoding, &fut.await?) })
        });
        self.insert::<M>(Handler::Unary(wrapped));
        self
    }

    /// Register a subscription: the handler returns a stream of events; the
    /// router frames them and ends the call when the stream ends.
    ///
    /// # Panics
    ///
    /// If `M` is not an events method or already has a handler.
    pub fn register_events<M, F, Fut, S>(&mut self, handler: F) -> &mut Router
    where
        M: Method<Result = StreamEnd>,
        F: Fn(CallCtx, M::Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, ControlError>> + Send + 'static,
        S: Stream<Item = Result<M::Event, ControlError>> + Send + 'static,
    {
        assert_eq!(
            M::STREAMING,
            StreamKind::Events,
            "{} is not a subscription method",
            M::NAME
        );
        let wrapped: Box<StreamFn> = Box::new(move |ctx: CallCtx, params: Blob| {
            let encoding = ctx.encoding;
            let params = match params.decode::<M::Params>() {
                Ok(p) => p,
                Err(e) => return Box::pin(async move { Err(e) }),
            };
            if let Err(e) = check_fd::<M>(&ctx, &params) {
                return Box::pin(async move { Err(e) });
            }
            let fut = handler(ctx, params);
            Box::pin(async move {
                let stream = fut.await?;
                let items: ItemStream = stream
                    .map(move |item| {
                        item.and_then(|event| Blob::encode(encoding, &event))
                            .map(StreamItem::Event)
                    })
                    .boxed();
                Ok(items)
            })
        });
        self.insert::<M>(Handler::Stream(wrapped));
        self
    }

    /// Register a bulk-data method: the handler returns a stream of byte
    /// buffers; the router splits them to at most [`MAX_CHUNK_BYTES`] and
    /// frames them as `Chunk`s.
    ///
    /// # Panics
    ///
    /// If `M` is not a chunks method or already has a handler.
    pub fn register_chunks<M, F, Fut, S>(&mut self, handler: F) -> &mut Router
    where
        M: Method<Result = StreamEnd>,
        F: Fn(CallCtx, M::Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, ControlError>> + Send + 'static,
        S: Stream<Item = Result<Bytes, ControlError>> + Send + 'static,
    {
        assert_eq!(
            M::STREAMING,
            StreamKind::Chunks,
            "{} is not a chunked method",
            M::NAME
        );
        let wrapped: Box<StreamFn> = Box::new(move |ctx: CallCtx, params: Blob| {
            let params = match params.decode::<M::Params>() {
                Ok(p) => p,
                Err(e) => return Box::pin(async move { Err(e) }),
            };
            if let Err(e) = check_fd::<M>(&ctx, &params) {
                return Box::pin(async move { Err(e) });
            }
            let fut = handler(ctx, params);
            Box::pin(async move {
                let stream = fut.await?;
                let items: ItemStream = stream.map(|item| item.map(StreamItem::Chunk)).boxed();
                Ok(items)
            })
        });
        self.insert::<M>(Handler::Stream(wrapped));
        self
    }

    /// Steps 1–5 of the module docs. Errors before the handler runs are
    /// returned here (and audited if the method is mutating).
    pub(crate) fn start(&self, inv: Invocation) -> Result<Started, ControlError> {
        let entry = self.handlers.get(inv.method.as_str());
        let known = entry
            .map(|e| e.info)
            .or_else(|| method_info(&inv.method).copied());
        let resolution = self.policy.resolve(&inv.principal);
        let role = resolution.as_ref().map(|r| r.role);
        let ticket = match known {
            Some(info) if info.mutating => {
                // Which row resolved the role, for every mutating call
                // (plan 33): lets `doctor`/`status` answer "why am I only a
                // viewer" without guesswork, and tells a service grant
                // apart from a same-uid human grant. This is the *resolved*
                // role, not the outcome — the check against the method's
                // minimum role is below and may still deny.
                match &resolution {
                    Some(r) => tracing::debug!(
                        principal = %inv.principal,
                        method = info.name,
                        role = %r.role,
                        min_role = %info.min_role,
                        matched = %r.matched,
                        service_label = r.service.as_ref().map(|s| s.label.as_str()),
                        "resolved the role of a mutating call"
                    ),
                    None => tracing::debug!(
                        principal = %inv.principal,
                        method = info.name,
                        "no allowlist row matches this principal; the mutating call is denied"
                    ),
                }
                AuditTicket::open(
                    self.audit.clone(),
                    &inv.principal,
                    resolution.as_ref(),
                    &info,
                    inv.encoding,
                    &inv.params,
                )
            }
            _ => AuditTicket::none(),
        };
        let reject = |ticket: AuditTicket, err: ControlError| {
            ticket.complete(AuditOutcome::Err(err.kind));
            Err(err)
        };

        let Some(role) = role else {
            return reject(ticket, no_role(&inv.principal));
        };
        let Some(entry) = entry else {
            let err = if known.is_some() {
                ControlError::unsupported(format!(
                    "{} is not implemented by this daemon",
                    inv.method
                ))
            } else {
                ControlError::unsupported(format!("unknown method {}", inv.method))
            };
            return reject(ticket, err);
        };
        if let Err(err) = self
            .policy
            .authorize_role(&inv.principal, &entry.info, Some(role))
        {
            return reject(ticket, err);
        }
        if inv.params.encoding() != inv.encoding {
            return reject(
                ticket,
                ControlError::protocol("params are not in the connection's encoding"),
            );
        }

        let ctx = CallCtx {
            principal: inv.principal,
            role,
            cancel: inv.cancel,
            call_id: inv.call_id,
            method: entry.info.name,
            encoding: inv.encoding,
            fd: inv.fd,
            fd_passing: inv.fd_passing,
        };
        Ok(match &entry.handler {
            Handler::Unary(f) => Started::Unary {
                fut: f(ctx, inv.params),
                ticket,
            },
            Handler::Stream(f) => Started::Stream {
                kind: entry.info.streaming,
                fut: f(ctx, inv.params),
                ticket,
            },
        })
    }
}

/// Everything `start` needs about one call.
pub(crate) struct Invocation {
    pub principal: Principal,
    pub method: String,
    pub params: Blob,
    pub cancel: CancellationToken,
    pub call_id: u64,
    pub fd: Option<OwnedFd>,
    pub fd_passing: bool,
    pub encoding: Encoding,
}

pub(crate) enum Started {
    Unary {
        fut: BoxFuture<'static, Result<Blob, ControlError>>,
        ticket: AuditTicket,
    },
    Stream {
        kind: StreamKind,
        fut: BoxFuture<'static, Result<ItemStream, ControlError>>,
        ticket: AuditTicket,
    },
}

/// An open audit record awaiting its outcome.
pub(crate) struct AuditTicket {
    inner: Option<TicketInner>,
}

struct TicketInner {
    sink: Arc<dyn AuditSink>,
    principal: AuditPrincipal,
    role: Option<Role>,
    method: &'static str,
    encoding: Encoding,
    digest: String,
}

impl AuditTicket {
    fn none() -> AuditTicket {
        AuditTicket { inner: None }
    }

    fn open(
        sink: Arc<dyn AuditSink>,
        principal: &Principal,
        resolution: Option<&Resolution>,
        info: &MethodInfo,
        encoding: Encoding,
        params: &Blob,
    ) -> AuditTicket {
        let digest = if info.secret_params {
            WITHHELD_DIGEST.to_string()
        } else {
            params_digest(params)
        };
        AuditTicket {
            inner: Some(TicketInner {
                sink,
                principal: AuditPrincipal::new(principal, resolution),
                role: resolution.map(|r| r.role),
                method: info.name,
                encoding,
                digest,
            }),
        }
    }

    pub(crate) fn complete(mut self, outcome: AuditOutcome) {
        self.finish(outcome);
    }

    fn finish(&mut self, outcome: AuditOutcome) {
        if let Some(t) = self.inner.take() {
            t.sink.record(&AuditRecord {
                ts_unix_ms: now_unix_ms(),
                principal: t.principal,
                role: t.role,
                method: t.method.to_string(),
                encoding: t.encoding,
                params_digest: t.digest,
                outcome,
            });
        }
    }
}

impl Drop for AuditTicket {
    fn drop(&mut self) {
        // Never completed: the task was aborted or the connection died
        // mid-call. Say so rather than leaving a hole in the trail.
        self.finish(AuditOutcome::Err(ErrorKind::Cancelled));
    }
}

fn outcome_of<T>(result: &Result<T, ControlError>) -> AuditOutcome {
    match result {
        Ok(_) => AuditOutcome::Ok,
        Err(e) => AuditOutcome::Err(e.kind),
    }
}

// ---------------------------------------------------------------------------
// Driving a started call
// ---------------------------------------------------------------------------

/// Run a unary call to completion, honouring cancellation with a grace
/// period (see the module docs).
async fn run_unary(
    mut fut: BoxFuture<'static, Result<Blob, ControlError>>,
    cancel: &CancellationToken,
    grace: Duration,
) -> Result<Blob, ControlError> {
    tokio::select! {
        biased;
        result = &mut fut => result,
        _ = cancel.cancelled() => match tokio::time::timeout(grace, &mut fut).await {
            Ok(result) => result,
            Err(_) => Err(ControlError::cancelled()),
        },
    }
}

/// Where a stream pump puts its output.
trait StreamSink: Send {
    fn item<'a>(&'a mut self, item: StreamItem) -> BoxFuture<'a, Result<(), ControlError>>;
    /// The handler has nothing more *right now*: push out anything held
    /// back, so a slow or open-ended stream (`node.logs.tail` with
    /// `follow`) delivers what it has instead of sitting on it until the
    /// next item.
    fn flush<'a>(&'a mut self) -> BoxFuture<'a, Result<(), ControlError>>;
    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<(), ControlError>>;
}

/// Pump `stream` into `sink` until it ends, errors or is cancelled. Returns
/// the counts for the terminal `StreamEnd`.
async fn pump_stream(
    fut: BoxFuture<'static, Result<ItemStream, ControlError>>,
    cancel: &CancellationToken,
    sink: &mut dyn StreamSink,
) -> Result<StreamEnd, ControlError> {
    let mut stream = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(ControlError::cancelled()),
        stream = fut => stream?,
    };
    let mut end = StreamEnd::default();
    loop {
        if cancel.is_cancelled() {
            return Err(ControlError::cancelled());
        }
        // An item that is ready now keeps the one-item chunk lookahead (so
        // the final data chunk can carry `last`); a stream that would wait
        // first gets what is held back flushed to the client.
        let next = match stream.next().now_or_never() {
            Some(next) => next,
            None => {
                sink.flush().await?;
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(ControlError::cancelled()),
                    next = stream.next() => next,
                }
            }
        };
        match next {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok(item)) => {
                end.items += 1;
                if let StreamItem::Chunk(bytes) = &item {
                    end.bytes += bytes.len() as u64;
                }
                sink.item(item).await?;
            }
        }
    }
    sink.finish().await?;
    Ok(end)
}

/// Frames a stream onto a connection: `Event`s as they come, `Chunk`s with
/// one item of lookahead so the final one can carry `last` — flushed
/// whenever the handler's stream would wait, so nothing is held back.
struct ConnSink<'a> {
    conn: &'a Conn,
    id: u64,
    kind: StreamKind,
    seq: u64,
    held: Option<Bytes>,
}

impl ConnSink<'_> {
    async fn send_chunk(&mut self, bytes: Bytes, last: bool) -> Result<(), ControlError> {
        let chunk = Chunk {
            id: self.id,
            seq: self.seq,
            bytes: bytes.into(),
            last,
        };
        self.seq += 1;
        self.conn.send(FrameKind::Chunk, &chunk).await
    }

    async fn push_chunk(&mut self, bytes: Bytes) -> Result<(), ControlError> {
        if let Some(prev) = self.held.replace(bytes) {
            self.send_chunk(prev, false).await?;
        }
        Ok(())
    }
}

impl StreamSink for ConnSink<'_> {
    fn item<'a>(&'a mut self, item: StreamItem) -> BoxFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            match item {
                StreamItem::Event(payload) => {
                    let event = Event {
                        sub_id: self.id,
                        payload,
                    };
                    self.conn.send(FrameKind::Event, &event).await
                }
                StreamItem::Chunk(mut bytes) => {
                    while bytes.len() > MAX_CHUNK_BYTES {
                        let piece = bytes.split_to(MAX_CHUNK_BYTES);
                        self.push_chunk(piece).await?;
                    }
                    self.push_chunk(bytes).await
                }
            }
        })
    }

    fn flush<'a>(&'a mut self) -> BoxFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if let Some(held) = self.held.take() {
                self.send_chunk(held, false).await?;
            }
            Ok(())
        })
    }

    fn finish<'a>(&'a mut self) -> BoxFuture<'a, Result<(), ControlError>> {
        Box::pin(async move {
            if self.kind == StreamKind::Chunks {
                let last = self.held.take().unwrap_or_default();
                self.send_chunk(last, true).await?;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

/// The sending half every task of one connection shares.
struct Conn {
    transport: Arc<dyn Transport>,
    encoding: Encoding,
}

impl Conn {
    async fn send<T: Serialize>(&self, kind: FrameKind, value: &T) -> Result<(), ControlError> {
        let payload = self.encoding.to_bytes(value)?;
        self.transport
            .send_frame(Frame::new(kind, payload))
            .await
            .map_err(ControlError::from)
    }

    /// Send the one terminal `Response` of call `id`. A result that cannot
    /// be framed (it does not encode, or exceeds [`MAX_FRAME_LEN`]) is
    /// replaced by an error saying so: the client must get *a* response,
    /// never wait forever for one the server could not send.
    async fn respond(&self, id: u64, result: Result<Blob, ControlError>) {
        let encode = |result: Result<Blob, ControlError>| {
            let response = Response {
                id,
                result: match result {
                    Ok(blob) => Outcome::Ok(blob),
                    Err(e) => Outcome::Err(e),
                },
            };
            let payload = self.encoding.to_bytes(&response)?;
            if payload.len() >= MAX_FRAME_LEN {
                return Err(
                    ControlError::from(constellation_types::Code::Overflow).with_details(
                        serde_json::json!({"bytes": payload.len(), "max": MAX_FRAME_LEN - 1}),
                    ),
                );
            }
            Ok(payload)
        };
        let payload = match encode(result) {
            Ok(payload) => payload,
            Err(e) => {
                tracing::warn!(id, error = %e, "response not sendable; answering with an error");
                let mut err =
                    ControlError::failed(format!("the result could not be sent: {}", e.message));
                err.code = e.code.or(err.code);
                err.details = e.details;
                match encode(Err(err)) {
                    Ok(payload) => payload,
                    Err(_) => {
                        match encode(Err(ControlError::failed("the result could not be sent"))) {
                            Ok(payload) => payload,
                            Err(_) => return,
                        }
                    }
                }
            }
        };
        if let Err(e) = self
            .transport
            .send_frame(Frame::new(FrameKind::Response, payload))
            .await
        {
            tracing::debug!(id, error = %e, "response not delivered (connection gone)");
        }
    }
}

/// Send a JSON `Response{id: 0}` error (the only error channel before an
/// encoding is agreed) and give up on the connection.
async fn refuse(transport: &Arc<dyn Transport>, error: ControlError) {
    let response = Response {
        id: 0,
        result: Outcome::Err(error),
    };
    if let Ok(payload) = Encoding::Json.to_bytes(&response) {
        let _ = transport
            .send_frame(Frame::new(FrameKind::Response, payload))
            .await;
    }
}

struct Handshake {
    encoding: Encoding,
}

async fn handshake(
    transport: &Arc<dyn Transport>,
    router: &Router,
) -> Result<Handshake, ControlError> {
    let first = tokio::time::timeout(router.options.handshake_timeout, transport.recv_frame())
        .await
        .map_err(|_| {
            ControlError::new(ErrorKind::Timeout, "no Hello within the handshake timeout")
        })?
        .map_err(ControlError::from)?;
    let Some(frame) = first else {
        return Err(ControlError::unavailable("connection closed before Hello"));
    };
    if frame.kind != FrameKind::Hello {
        return Err(ControlError::protocol("the first frame must be Hello"));
    }
    let hello: Hello = Encoding::Json.from_bytes(&frame.payload)?;
    let Some(encoding) = negotiate(&hello.encodings, &router.options.encodings) else {
        return Err(ControlError::unsupported(format!(
            "no common encoding (server speaks {:?})",
            router.options.encodings
        )));
    };
    let principal = transport.peer();
    let roles = router
        .policy
        .role_of(&principal)
        .map(Role::implied)
        .unwrap_or_default();
    let mut features = router.features.clone();
    if transport.supports_fd_passing() {
        features.push(FEATURE_FD_PASSING.to_string());
    }
    let welcome = Welcome {
        server_version: env!("CARGO_PKG_VERSION").to_string(),
        principal,
        roles,
        features,
        encoding,
    };
    let payload = Encoding::Json.to_bytes(&welcome)?;
    transport
        .send_frame(Frame::new(FrameKind::Welcome, payload))
        .await
        .map_err(ControlError::from)?;
    Ok(Handshake { encoding })
}

/// Run one connection until it closes or `shutdown` fires.
pub(crate) async fn serve_connection(
    transport: Arc<dyn Transport>,
    router: Arc<Router>,
    shutdown: CancellationToken,
) {
    let hs = match handshake(&transport, &router).await {
        Ok(hs) => hs,
        Err(e) => {
            tracing::debug!(error = %e, transport = transport.name(), "handshake failed");
            refuse(&transport, e).await;
            return;
        }
    };
    let principal = transport.peer();
    let conn = Arc::new(Conn {
        transport: transport.clone(),
        encoding: hs.encoding,
    });
    let conn_cancel = shutdown.child_token();
    let inflight: Arc<Mutex<HashMap<u64, CancellationToken>>> = Arc::default();
    let limit = router.options.max_inflight_per_connection;

    loop {
        let frame = tokio::select! {
            biased;
            _ = conn_cancel.cancelled() => break,
            frame = transport.recv_frame() => frame,
        };
        let mut frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!(error = %e, "connection read failed");
                break;
            }
        };
        match frame.kind {
            FrameKind::Request => {
                let request: Request = match conn.encoding.from_bytes(&frame.payload) {
                    Ok(r) => r,
                    Err(e) => {
                        conn.respond(0, Err(e)).await;
                        break;
                    }
                };
                let id = request.id;
                let token = conn_cancel.child_token();
                let admitted = {
                    let mut table = inflight.lock().unwrap_or_else(|p| p.into_inner());
                    if id == 0 {
                        // Id 0 is the connection-level error channel: a
                        // response to it would read as "connection refused".
                        Err(Rejection::Fatal(ControlError::protocol(
                            "request id 0 is reserved",
                        )))
                    } else if table.contains_key(&id) {
                        // Answering under the same id would give the
                        // original call two responses; the client's
                        // bookkeeping is broken, so end the connection.
                        Err(Rejection::Fatal(ControlError::protocol(format!(
                            "request id {id} is already in flight"
                        ))))
                    } else if table.len() >= limit {
                        Err(Rejection::Call(
                            ControlError::unavailable(format!(
                                "too many calls in flight on this connection (limit {limit})"
                            ))
                            .with_remediation(
                                "wait for outstanding calls or open another connection",
                            ),
                        ))
                    } else {
                        table.insert(id, token.clone());
                        Ok(())
                    }
                };
                match admitted {
                    Ok(()) => {}
                    // Answered inline, not on a spawned task: a client that
                    // floods requests without reading must be slowed down
                    // by its own full socket, not grow a task per request.
                    Err(Rejection::Call(e)) => {
                        conn.respond(id, Err(e)).await;
                        continue;
                    }
                    Err(Rejection::Fatal(e)) => {
                        conn.respond(0, Err(e)).await;
                        break;
                    }
                }
                let call = Call {
                    conn: conn.clone(),
                    router: router.clone(),
                    principal: principal.clone(),
                    inflight: inflight.clone(),
                    request,
                    fd: frame.take_fd(),
                    token,
                };
                tokio::spawn(call.run());
            }
            FrameKind::Cancel => match conn.encoding.from_bytes::<Cancel>(&frame.payload) {
                Ok(cancel) => {
                    let table = inflight.lock().unwrap_or_else(|p| p.into_inner());
                    if let Some(token) = table.get(&cancel.id) {
                        token.cancel();
                    }
                }
                Err(e) => {
                    conn.respond(0, Err(e)).await;
                    break;
                }
            },
            other => {
                let err =
                    ControlError::protocol(format!("unexpected {other:?} frame from a client"));
                conn.respond(0, Err(err)).await;
                break;
            }
        }
    }
    conn_cancel.cancel();
}

/// Why a request was not admitted.
enum Rejection {
    /// Answer this call with the error; the connection carries on.
    Call(ControlError),
    /// A protocol violation: answer on id 0 and close.
    Fatal(ControlError),
}

struct Call {
    conn: Arc<Conn>,
    router: Arc<Router>,
    principal: Principal,
    inflight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    request: Request,
    fd: Option<OwnedFd>,
    token: CancellationToken,
}

impl Call {
    async fn run(self) {
        let Call {
            conn,
            router,
            principal,
            inflight,
            request,
            fd,
            token,
        } = self;
        let id = request.id;
        let inv = Invocation {
            principal,
            method: request.method,
            params: request.params,
            cancel: token.clone(),
            call_id: id,
            fd,
            fd_passing: conn.transport.supports_fd_passing(),
            encoding: conn.encoding,
        };
        let work = async {
            match router.start(inv) {
                Err(e) => Err(e),
                Ok(Started::Unary { fut, ticket }) => {
                    // The handler itself runs on this task; a panic must not
                    // take the connection's response with it.
                    let result = std::panic::AssertUnwindSafe(run_unary(
                        fut,
                        &token,
                        router.options.cancel_grace,
                    ));
                    let result = futures::FutureExt::catch_unwind(result)
                        .await
                        .unwrap_or_else(|_| Err(ControlError::failed("the handler panicked")));
                    ticket.complete(outcome_of(&result));
                    result
                }
                Ok(Started::Stream { kind, fut, ticket }) => {
                    let mut sink = ConnSink {
                        conn: &conn,
                        id,
                        kind,
                        seq: 0,
                        held: None,
                    };
                    let end = std::panic::AssertUnwindSafe(pump_stream(fut, &token, &mut sink));
                    let end = futures::FutureExt::catch_unwind(end)
                        .await
                        .unwrap_or_else(|_| Err(ControlError::failed("the handler panicked")));
                    ticket.complete(outcome_of(&end));
                    end.and_then(|end| Blob::encode(conn.encoding, &end))
                }
            }
        };
        // Everything above — including the synchronous part of a handler
        // (the closure body before its `async` block) and the audit sink —
        // may panic; the call must still end in a Response and free its
        // in-flight slot.
        let result: Result<Blob, ControlError> = std::panic::AssertUnwindSafe(work)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(ControlError::failed("the handler panicked")));
        // Forget the id *before* answering, so a client that reuses ids
        // (ours never does) cannot race the bookkeeping. A `Cancel` that
        // arrives after this point is ignored, exactly as documented.
        inflight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&id);
        conn.respond(id, result).await;
    }
}

// ---------------------------------------------------------------------------
// serve
// ---------------------------------------------------------------------------

/// A running server. Dropping it does **not** stop it; call
/// [`shutdown`](ServerHandle::shutdown).
pub struct ServerHandle {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    router: Arc<Router>,
}

impl ServerHandle {
    /// Stop accepting, cancel every in-flight call and close connections.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.task.await;
    }

    /// A token that stops the server when cancelled.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// The router being served (for `dispatch_in_process` alongside).
    pub fn router(&self) -> Arc<Router> {
        self.router.clone()
    }
}

/// Serve `router` on `listener` under `policy`, recording mutating calls to
/// `audit`. Each accepted connection runs the handshake and then its calls
/// concurrently; see the module docs.
pub fn serve<L: Listener>(
    listener: L,
    router: Router,
    policy: Policy,
    audit: Arc<dyn AuditSink>,
) -> ServerHandle {
    let router = Arc::new(router.with_policy(policy).with_audit(audit));
    serve_router(listener, router)
}

/// [`serve`] for a router that already carries its policy and audit sink
/// (one shared with a [`dispatch_in_process`] caller, say).
pub fn serve_router<L: Listener>(mut listener: L, router: Arc<Router>) -> ServerHandle {
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let served = router.clone();
    let task = tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                biased;
                _ = token.cancelled() => break,
                accepted = listener.accept() => accepted,
            };
            match accepted {
                Ok(transport) => {
                    tokio::spawn(serve_connection(transport, served.clone(), token.clone()));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a control connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    });
    ServerHandle {
        shutdown,
        task,
        router,
    }
}

// ---------------------------------------------------------------------------
// In-process dispatch (the web adapter, the in-process CLI)
// ---------------------------------------------------------------------------

/// Optional extras of an in-process dispatch.
#[derive(Debug, Default)]
pub struct DispatchOptions {
    /// Cancel the call from outside; a fresh token when `None`.
    pub cancel: Option<CancellationToken>,
    /// A descriptor "attached" to the call (in-process fd passing).
    pub fd: Option<OwnedFd>,
}

/// Call `method` with JSON `params` directly on `router` as `principal`:
/// the same authorization, audit and handler path a socket call takes, with
/// no transport in between. Streaming methods are refused here; use
/// [`dispatch_stream_in_process`].
pub async fn dispatch_in_process(
    router: &Router,
    principal: &Principal,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, ControlError> {
    dispatch_in_process_with(
        router,
        principal,
        method,
        params,
        DispatchOptions::default(),
    )
    .await
}

/// [`dispatch_in_process`] with a cancel token and/or an fd.
pub async fn dispatch_in_process_with(
    router: &Router,
    principal: &Principal,
    method: &str,
    params: serde_json::Value,
    options: DispatchOptions,
) -> Result<serde_json::Value, ControlError> {
    let cancel = options.cancel.unwrap_or_default();
    let inv = Invocation {
        principal: principal.clone(),
        method: method.to_string(),
        params: Blob::Json(params),
        cancel: cancel.clone(),
        call_id: 0,
        fd: options.fd,
        fd_passing: true,
        encoding: Encoding::Json,
    };
    match router.start(inv)? {
        Started::Unary { fut, ticket } => {
            let result = run_unary(fut, &cancel, router.options.cancel_grace).await;
            ticket.complete(outcome_of(&result));
            match result? {
                Blob::Json(value) => Ok(value),
                Blob::Postcard(_) => Err(ControlError::failed("handler returned a binary blob")),
            }
        }
        Started::Stream { ticket, .. } => {
            ticket.complete(AuditOutcome::Err(ErrorKind::Invalid));
            Err(ControlError::invalid(format!(
                "{method} streams; use dispatch_stream_in_process"
            )))
        }
    }
}

/// Call a streaming method directly on `router`. Items arrive as JSON
/// events or raw chunk buffers; the stream ends when the method finishes
/// (an `Err` item first if it failed). Dropping the stream cancels the call.
pub async fn dispatch_stream_in_process(
    router: &Router,
    principal: &Principal,
    method: &str,
    params: serde_json::Value,
    options: DispatchOptions,
) -> Result<BoxStream<'static, Result<StreamItem, ControlError>>, ControlError> {
    let cancel = options.cancel.unwrap_or_default();
    let inv = Invocation {
        principal: principal.clone(),
        method: method.to_string(),
        params: Blob::Json(params),
        cancel: cancel.clone(),
        call_id: 0,
        fd: options.fd,
        fd_passing: true,
        encoding: Encoding::Json,
    };
    match router.start(inv)? {
        Started::Unary { ticket, .. } => {
            ticket.complete(AuditOutcome::Err(ErrorKind::Invalid));
            Err(ControlError::invalid(format!(
                "{method} does not stream; use dispatch_in_process"
            )))
        }
        Started::Stream { fut, ticket, .. } => {
            let stream = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    ticket.complete(AuditOutcome::Err(ErrorKind::Cancelled));
                    return Err(ControlError::cancelled());
                }
                stream = fut => match stream {
                    Ok(stream) => stream,
                    Err(e) => {
                        ticket.complete(AuditOutcome::Err(e.kind));
                        return Err(e);
                    }
                },
            };
            Ok(AuditedStream {
                inner: stream,
                cancelled: Box::pin(cancel.clone().cancelled_owned()),
                cancel,
                ticket: Some(ticket),
                done: false,
            }
            .boxed())
        }
    }
}

/// Completes the audit ticket when the stream ends, and cancels the call if
/// the consumer drops it early.
struct AuditedStream {
    inner: ItemStream,
    cancel: CancellationToken,
    /// Polled alongside `inner`, so a cancel from outside wakes a consumer
    /// waiting on a quiet stream instead of leaving it parked until the
    /// next item.
    cancelled: std::pin::Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
    ticket: Option<AuditTicket>,
    done: bool,
}

impl Stream for AuditedStream {
    type Item = Result<StreamItem, ControlError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        if self.done {
            return Poll::Ready(None);
        }
        if self.cancelled.as_mut().poll(cx).is_ready() {
            self.done = true;
            if let Some(t) = self.ticket.take() {
                t.complete(AuditOutcome::Err(ErrorKind::Cancelled));
            }
            return Poll::Ready(Some(Err(ControlError::cancelled())));
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(None) => {
                self.done = true;
                if let Some(t) = self.ticket.take() {
                    t.complete(AuditOutcome::Ok);
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                if let Some(t) = self.ticket.take() {
                    t.complete(AuditOutcome::Err(e.kind));
                }
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

impl Drop for AuditedStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
