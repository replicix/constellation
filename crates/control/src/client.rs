//! The client library: connect, handshake, and call methods by type.
//!
//! ```ignore
//! let client = Client::connect_unix(&path).await?;
//! let status = client.call::<NodeStatus>(Empty {}).await?;
//! let mut events = client.subscribe::<EventsSubscribe>(Default::default()).await?;
//! while let Some(event) = events.next().await { ... }
//! ```
//!
//! ## Shape
//!
//! One connection multiplexes any number of concurrent calls. A single
//! reader task owns the receiving half of the transport and routes each
//! frame by request id to whoever is waiting: a `oneshot` for a unary call,
//! a bounded `mpsc` for a stream. Sends go straight to the transport (which
//! serializes whole frames), so callers never queue behind each other.
//!
//! ## Lifetimes and cancellation
//!
//! Dropping a [`PendingCall`], [`EventStream`] or [`ChunkStream`] before it
//! finished sends `Cancel{id}` (best effort) and forgets the id, so an
//! abandoned call does not keep running on the daemon. [`Client::call_bounded`]
//! is just that plus a timer. When the connection dies, every outstanding
//! call fails `Unavailable` — nothing waits forever on a dead socket.
//!
//! ## Sends are never torn
//!
//! Every frame is written by a task of its own that the caller merely
//! awaits. Dropping a call future mid-write (a `select!`, a
//! [`Client::call_bounded`] timeout) therefore cannot leave half a frame
//! on the wire, which would poison the connection for every other call on
//! it. A `Cancel` for a dropped call is written only after its `Request`.
//!
//! ## Slow stream consumers
//!
//! Stream items are delivered by the *shared* reader task, so a consumer
//! that stops polling must not be able to stall the connection's other
//! calls — a UI that awaits `client.call(..)` while handling an event would
//! otherwise deadlock itself once the event buffer fills.
//!
//! - **Subscriptions** buffer up to 1024 events; a subscriber that falls
//!   further behind is cut off (the subscription is cancelled on the daemon
//!   and the stream yields an `Unavailable` error). Events are
//!   notifications; losing the tail of an unread backlog is the lesser evil.
//! - **Chunk streams** buffer 64 chunks and then *do* hold up the reader
//!   (head-of-line blocking): dropping file data is not an option, and an
//!   unbounded buffer would let a slow reader of a multi-gigabyte
//!   `browse.read` balloon memory. Do not await other calls on the same
//!   connection between chunks; give bulk transfers their own connection
//!   (`connect` is cheap), or consume promptly.
//!
//! A stream that ends without its terminal `Response` (the connection
//! dropped, the subscriber was cut off) always ends with an error item,
//! never silently.
//!
//! ## `call_json`
//!
//! A raw `serde_json::Value` call for debugging CLIs. It needs a connection
//! that negotiated JSON (the default): postcard cannot carry an untyped
//! value, so on a postcard connection it fails `Unsupported`.

use crate::authz::Principal;
use crate::fd::OwnedFd;
use crate::methods::{method_info, Method, StreamKind};
use crate::proto::ClientInfo;
use crate::proto::{
    Blob, Cancel, Chunk, ControlError, Encoding, ErrorKind, Event, FrameKind, Hello, Outcome,
    Request, Response, Welcome, FEATURE_FD_PASSING,
};
use crate::server::{serve_connection, Router};
use crate::transport::{Frame, InProcess, Transport, TransportError};
use bytes::Bytes;
use futures::Stream;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Chunks buffered per chunk stream before the reader task waits.
const CHUNK_BUFFER: usize = 64;
/// Events buffered per subscription before the subscriber is cut off.
const EVENT_BUFFER: usize = 1024;

/// How to introduce yourself.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Encodings to offer, most preferred first.
    pub encodings: Vec<Encoding>,
    pub name: String,
    pub version: String,
    pub handshake_timeout: Duration,
}

impl Default for ClientOptions {
    /// JSON only: debuggable, and what [`Client::call_json`] needs.
    fn default() -> Self {
        ClientOptions {
            encodings: vec![Encoding::Json],
            name: "constellation-control-client".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

impl ClientOptions {
    /// Offer postcard first (UI and harness streams), JSON as fallback.
    pub fn postcard() -> ClientOptions {
        ClientOptions {
            encodings: vec![Encoding::Postcard, Encoding::Json],
            ..ClientOptions::default()
        }
    }

    pub fn named(mut self, name: impl Into<String>, version: impl Into<String>) -> ClientOptions {
        self.name = name.into();
        self.version = version.into();
        self
    }
}

enum StreamMsg {
    Event(Blob),
    Chunk { seq: u64, bytes: Bytes },
    End(Result<Blob, ControlError>),
}

/// Why the reader let go of a stream without delivering its `End`.
type EndReason = Arc<std::sync::OnceLock<ControlError>>;

struct StreamSlot {
    tx: mpsc::Sender<StreamMsg>,
    /// Events: a full buffer cuts the subscriber off instead of blocking
    /// the shared reader (see the module docs).
    lossy: bool,
    reason: EndReason,
}

enum Slot {
    Unary(oneshot::Sender<Result<Blob, ControlError>>),
    Stream(StreamSlot),
}

struct State {
    pending: HashMap<u64, Slot>,
    closed: Option<ControlError>,
}

struct Shared {
    transport: Arc<dyn Transport>,
    encoding: Encoding,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Fail every outstanding call with `reason` and refuse new ones.
    /// Never waits: a stream whose buffer is full gets `reason` recorded
    /// and its sender dropped (the consumer sees the error after the
    /// backlog), so one unread stream cannot keep the others hanging.
    fn fail_all(&self, reason: ControlError) {
        let slots: Vec<Slot> = {
            let mut state = self.lock();
            state.closed = Some(reason.clone());
            state.pending.drain().map(|(_, slot)| slot).collect()
        };
        for slot in slots {
            match slot {
                Slot::Unary(tx) => {
                    let _ = tx.send(Err(reason.clone()));
                }
                Slot::Stream(slot) => {
                    let _ = slot.reason.set(reason.clone());
                    let _ = slot.tx.try_send(StreamMsg::End(Err(reason.clone())));
                }
            }
        }
    }

    /// Write `frame` on a task of its own (see "Sends are never torn").
    fn send_detached(&self, frame: Frame) -> JoinHandle<Result<(), TransportError>> {
        let transport = self.transport.clone();
        tokio::spawn(async move { transport.send_frame(frame).await })
    }

    fn cancel_frame(&self, id: u64) -> Option<Frame> {
        let payload = self.encoding.to_bytes(&Cancel { id }).ok()?;
        Some(Frame::new(FrameKind::Cancel, payload))
    }

    /// Send `Cancel{id}` and wait until it is written.
    async fn send_cancel(&self, id: u64) {
        if let Some(frame) = self.cancel_frame(id) {
            let _ = self.send_detached(frame).await;
        }
    }

    /// Send `Cancel{id}` without waiting (from the reader, which must keep
    /// reading while the write drains).
    fn spawn_cancel(&self, id: u64) {
        if let Some(frame) = self.cancel_frame(id) {
            drop(self.send_detached(frame));
        }
    }

    /// Handle one frame from the daemon. `Err` ends the connection.
    async fn handle(self: &Arc<Self>, frame: Frame) -> Result<(), ControlError> {
        match frame.kind {
            FrameKind::Response => {
                let response: Response = self.encoding.from_bytes(&frame.payload)?;
                if response.id == 0 {
                    // Connection-level refusal (protocol error on our side).
                    return Err(match response.result {
                        Outcome::Err(e) => e,
                        Outcome::Ok(_) => ControlError::protocol("Response with id 0"),
                    });
                }
                let slot = self.lock().pending.remove(&response.id);
                let result = match response.result {
                    Outcome::Ok(blob) => Ok(blob),
                    Outcome::Err(e) => Err(e),
                };
                match slot {
                    Some(Slot::Unary(tx)) => {
                        let _ = tx.send(result);
                    }
                    Some(Slot::Stream(slot)) => {
                        let msg = StreamMsg::End(result);
                        if slot.lossy {
                            if let Err(mpsc::error::TrySendError::Full(_)) = slot.tx.try_send(msg) {
                                let _ = slot.reason.set(fell_behind());
                            }
                        } else {
                            let _ = slot.tx.send(msg).await;
                        }
                    }
                    None => {} // cancelled and forgotten
                }
            }
            FrameKind::Event => {
                let event: Event = self.encoding.from_bytes(&frame.payload)?;
                self.deliver(event.sub_id, StreamMsg::Event(event.payload))
                    .await;
            }
            FrameKind::Chunk => {
                let chunk: Chunk = self.encoding.from_bytes(&frame.payload)?;
                self.deliver(
                    chunk.id,
                    StreamMsg::Chunk {
                        seq: chunk.seq,
                        bytes: chunk.bytes.0,
                    },
                )
                .await;
            }
            other => {
                return Err(ControlError::protocol(format!(
                    "unexpected {other:?} frame from the daemon"
                )))
            }
        }
        Ok(())
    }

    async fn deliver(self: &Arc<Self>, id: u64, msg: StreamMsg) {
        let (tx, lossy, reason) = match self.lock().pending.get(&id) {
            Some(Slot::Stream(slot)) => (slot.tx.clone(), slot.lossy, slot.reason.clone()),
            _ => return, // a stream we no longer track (or a unary id): drop it
        };
        let delivered = if lossy {
            match tx.try_send(msg) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let _ = reason.set(fell_behind());
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        } else {
            tx.send(msg).await.is_ok()
        };
        if !delivered {
            // The consumer went away or fell behind; forget the id (its
            // receiver then ends, with `reason` if one was set) and stop
            // the stream on the daemon too.
            self.lock().pending.remove(&id);
            self.spawn_cancel(id);
        }
    }
}

fn fell_behind() -> ControlError {
    ControlError::unavailable(format!(
        "the subscriber fell more than {EVENT_BUFFER} events behind; the subscription was cancelled"
    ))
    .with_remediation("poll the event stream promptly, or subscribe on a connection of its own")
}

/// The error for a stream whose receiver ended without an `End`.
fn ended_abnormally(reason: &EndReason) -> ControlError {
    reason.get().cloned().unwrap_or_else(|| {
        ControlError::unavailable("the stream ended without its terminal response")
    })
}

async fn read_loop(shared: Arc<Shared>) {
    let reason = loop {
        match shared.transport.recv_frame().await {
            Ok(Some(frame)) => {
                if let Err(e) = shared.handle(frame).await {
                    break e;
                }
            }
            Ok(None) => break ControlError::unavailable("the daemon closed the connection"),
            Err(e) => {
                let mut err = ControlError::from(e);
                err.kind = ErrorKind::Unavailable;
                break err;
            }
        }
    };
    shared.fail_all(reason);
}

struct Inner {
    shared: Arc<Shared>,
    welcome: Welcome,
    next_id: AtomicU64,
    reader: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// A connection to a daemon. Cheap to clone; clones share the connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("transport", &self.inner.shared.transport.name())
            .field("encoding", &self.inner.shared.encoding)
            .finish()
    }
}

/// Forgets an outstanding id (and cancels it on the daemon) when dropped
/// unfinished.
struct CallGuard {
    inner: Arc<Inner>,
    id: u64,
    finished: bool,
    cancel_requested: Arc<AtomicBool>,
    /// The request's write, while the caller is still waiting on it: a
    /// cancel for a call dropped mid-send must follow the request, or the
    /// daemon would ignore it and run the call anyway.
    sending: Option<JoinHandle<Result<(), TransportError>>>,
}

impl CallGuard {
    async fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::SeqCst);
        self.inner.shared.send_cancel(self.id).await;
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.inner.shared.lock().pending.remove(&self.id);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let inner = self.inner.clone();
            let id = self.id;
            let sending = self.sending.take();
            handle.spawn(async move {
                if let Some(sending) = sending {
                    if !matches!(sending.await, Ok(Ok(()))) {
                        return; // the request never went out whole
                    }
                }
                inner.shared.send_cancel(id).await
            });
        }
    }
}

#[cfg(unix)]
fn connect_error(path: &std::path::Path, e: std::io::Error) -> ControlError {
    let unreachable = matches!(
        e.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
    );
    let mut err = ControlError::from(e);
    err.message = format!("connecting to {}: {}", path.display(), err.message);
    if unreachable {
        err.kind = ErrorKind::Unavailable;
        err = err.with_remediation("is the constellation daemon running?");
    }
    err
}

impl Client {
    /// Connect to the daemon's unix socket with default options (JSON).
    #[cfg(unix)]
    pub async fn connect_unix(path: &std::path::Path) -> Result<Client, ControlError> {
        Client::connect_unix_with(path, ClientOptions::default()).await
    }

    #[cfg(unix)]
    pub async fn connect_unix_with(
        path: &std::path::Path,
        options: ClientOptions,
    ) -> Result<Client, ControlError> {
        let socket = crate::transport::UnixSocket::connect(path)
            .await
            .map_err(|e| connect_error(path, e))?;
        Client::from_transport(socket, options).await
    }

    /// A client talking to `router` inside this process, as
    /// [`Principal::InProcess`], over the full protocol path (frames,
    /// handshake, authz, audit).
    pub async fn in_process(router: impl Into<Arc<Router>>) -> Result<Client, ControlError> {
        Client::in_process_as(router, Principal::InProcess, ClientOptions::default()).await
    }

    /// [`in_process`](Self::in_process) with a chosen identity and options.
    pub async fn in_process_as(
        router: impl Into<Arc<Router>>,
        principal: Principal,
        options: ClientOptions,
    ) -> Result<Client, ControlError> {
        let (client_end, server_end) = InProcess::pair_as(principal);
        tokio::spawn(serve_connection(
            server_end,
            router.into(),
            CancellationToken::new(),
        ));
        Client::from_transport(client_end, options).await
    }

    /// Run the handshake over an already connected transport.
    pub async fn from_transport(
        transport: Arc<dyn Transport>,
        options: ClientOptions,
    ) -> Result<Client, ControlError> {
        let hello = Hello {
            encodings: options.encodings.clone(),
            client: ClientInfo {
                name: options.name,
                version: options.version,
            },
        };
        let payload = Encoding::Json.to_bytes(&hello)?;
        transport
            .send_frame(Frame::new(FrameKind::Hello, payload))
            .await?;
        let frame = tokio::time::timeout(options.handshake_timeout, transport.recv_frame())
            .await
            .map_err(|_| ControlError::new(ErrorKind::Timeout, "the daemon did not answer Hello"))??
            .ok_or_else(|| {
                ControlError::unavailable("the daemon closed the connection during the handshake")
            })?;
        let welcome: Welcome = match frame.kind {
            FrameKind::Welcome => Encoding::Json.from_bytes(&frame.payload)?,
            FrameKind::Response => {
                let response: Response = Encoding::Json.from_bytes(&frame.payload)?;
                return Err(match response.result {
                    Outcome::Err(e) => e,
                    Outcome::Ok(_) => ControlError::protocol("unexpected Ok during the handshake"),
                });
            }
            other => {
                return Err(ControlError::protocol(format!(
                    "expected Welcome, got {other:?}"
                )))
            }
        };
        if !options.encodings.contains(&welcome.encoding) {
            return Err(ControlError::protocol(format!(
                "the daemon chose {:?}, which we did not offer",
                welcome.encoding
            )));
        }
        let shared = Arc::new(Shared {
            transport,
            encoding: welcome.encoding,
            state: Mutex::new(State {
                pending: HashMap::new(),
                closed: None,
            }),
        });
        let reader = tokio::spawn(read_loop(shared.clone()));
        Ok(Client {
            inner: Arc::new(Inner {
                shared,
                welcome,
                next_id: AtomicU64::new(1),
                reader,
            }),
        })
    }

    /// What the daemon said about us and itself.
    pub fn welcome(&self) -> &Welcome {
        &self.inner.welcome
    }

    /// The negotiated encoding.
    pub fn encoding(&self) -> Encoding {
        self.inner.shared.encoding
    }

    /// Whether this connection can carry file descriptors.
    pub fn supports_fd_passing(&self) -> bool {
        self.inner.shared.transport.supports_fd_passing()
            && self
                .inner
                .welcome
                .features
                .iter()
                .any(|f| f == FEATURE_FD_PASSING)
    }

    /// Whether the connection is still up.
    pub fn is_connected(&self) -> bool {
        self.inner.shared.lock().closed.is_none()
    }

    // -- plumbing ---------------------------------------------------------

    fn register(&self, make: impl FnOnce() -> Slot) -> Result<u64, ControlError> {
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let mut state = self.inner.shared.lock();
        if let Some(reason) = &state.closed {
            return Err(reason.clone());
        }
        state.pending.insert(id, make());
        Ok(id)
    }

    fn guard(&self, id: u64) -> CallGuard {
        CallGuard {
            inner: self.inner.clone(),
            id,
            finished: false,
            cancel_requested: Arc::new(AtomicBool::new(false)),
            sending: None,
        }
    }

    /// Send the request for an already registered id; on failure forget it.
    async fn send_request(
        &self,
        guard: &mut CallGuard,
        method: &str,
        params: Blob,
        fd: Option<OwnedFd>,
    ) -> Result<(), ControlError> {
        let request = Request {
            id: guard.id,
            method: method.to_string(),
            params,
        };
        let payload = self.encoding().to_bytes(&request);
        let sent = match payload {
            Err(e) => Err(e),
            Ok(payload) => {
                let frame = Frame {
                    kind: FrameKind::Request,
                    payload,
                    fd,
                };
                let sending = guard.sending.insert(self.inner.shared.send_detached(frame));
                let written = sending.await;
                guard.sending = None;
                match written {
                    Ok(result) => result.map_err(ControlError::from),
                    Err(e) => Err(ControlError::failed(format!("sending the request: {e}"))),
                }
            }
        };
        if sent.is_err() {
            self.inner.shared.lock().pending.remove(&guard.id);
            guard.finished = true;
        }
        sent
    }

    fn check_streaming<M: Method>(want: StreamKind) -> Result<(), ControlError> {
        if M::STREAMING == want {
            return Ok(());
        }
        Err(ControlError::invalid(match M::STREAMING {
            StreamKind::None => format!("{} does not stream; use call", M::NAME),
            StreamKind::Events => format!("{} is a subscription; use subscribe", M::NAME),
            StreamKind::Chunks => format!("{} returns chunks; use call_chunks", M::NAME),
        }))
    }

    /// Refuse locally, and promptly, what the daemon could only refuse
    /// after a round trip: an fd on a connection that cannot carry one.
    fn check_fd_use<M: Method>(
        &self,
        params: &M::Params,
        has_fd: bool,
    ) -> Result<(), ControlError> {
        if has_fd || M::requires_fd(params) {
            if !self.supports_fd_passing() {
                return Err(ControlError::unsupported(format!(
                    "{} needs a file descriptor, and this connection cannot pass one",
                    M::NAME
                ))
                .with_remediation("connect over the daemon's unix socket"));
            }
            if !has_fd {
                return Err(ControlError::invalid(format!(
                    "{} with these parameters needs a file descriptor; use call_with_fd",
                    M::NAME
                )));
            }
        }
        Ok(())
    }

    async fn start_unary<M: Method>(
        &self,
        params: M::Params,
        fd: Option<OwnedFd>,
    ) -> Result<PendingCall<M::Result>, ControlError> {
        Client::check_streaming::<M>(StreamKind::None)?;
        self.check_fd_use::<M>(&params, fd.is_some())?;
        let blob = Blob::encode(self.encoding(), &params)?;
        let (tx, rx) = oneshot::channel();
        let id = self.register(|| Slot::Unary(tx))?;
        let mut guard = self.guard(id);
        self.send_request(&mut guard, M::NAME, blob, fd).await?;
        Ok(PendingCall {
            rx,
            guard,
            client: self.clone(),
            _result: PhantomData,
        })
    }

    // -- unary calls ------------------------------------------------------

    /// Call a non-streaming method and wait for its result.
    pub async fn call<M: Method>(&self, params: M::Params) -> Result<M::Result, ControlError> {
        self.start_unary::<M>(params, None).await?.await
    }

    /// Call a method with `fd` attached to the request (`view.mount` with
    /// `PreopenedFd`, `node.handoff`). Fails `NotSupported` at once on a
    /// connection that cannot pass descriptors.
    pub async fn call_with_fd<M: Method>(
        &self,
        params: M::Params,
        fd: OwnedFd,
    ) -> Result<M::Result, ControlError> {
        self.start_unary::<M>(params, Some(fd)).await?.await
    }

    /// [`call`](Self::call) with a deadline. On expiry the call is cancelled
    /// on the daemon and the error is `Timeout`/`ETIMEDOUT`.
    pub async fn call_bounded<M: Method>(
        &self,
        params: M::Params,
        timeout: Duration,
    ) -> Result<M::Result, ControlError> {
        let started = tokio::time::timeout(timeout, async {
            self.start_unary::<M>(params, None).await?.await
        })
        .await;
        match started {
            Ok(result) => result,
            Err(_) => Err(ControlError::new(
                ErrorKind::Timeout,
                format!("{} did not finish within {timeout:?}", M::NAME),
            )
            .with_code(constellation_types::Code::TimedOut)),
        }
    }

    /// Send the request but return a handle instead of waiting, so the
    /// caller can learn the id and [`cancel`](PendingCall::cancel) it.
    pub async fn start<M: Method>(
        &self,
        params: M::Params,
    ) -> Result<PendingCall<M::Result>, ControlError> {
        self.start_unary::<M>(params, None).await
    }

    /// Cancel the in-flight call `id`. The call itself completes with
    /// `Cancelled` (or with its result, if it beat the cancel).
    pub async fn cancel(&self, id: u64) {
        self.inner.shared.send_cancel(id).await;
    }

    /// Call `method` with raw JSON. See the module docs.
    pub async fn call_json(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ControlError> {
        if self.encoding() != Encoding::Json {
            return Err(ControlError::unsupported(
                "call_json needs a connection that negotiated JSON",
            ));
        }
        if matches!(method_info(method), Some(i) if i.streaming != StreamKind::None) {
            return Err(ControlError::invalid(format!(
                "{method} streams; use a typed call"
            )));
        }
        let (tx, rx) = oneshot::channel();
        let id = self.register(|| Slot::Unary(tx))?;
        let mut guard = self.guard(id);
        self.send_request(&mut guard, method, Blob::Json(params), None)
            .await?;
        let pending: PendingCall<serde_json::Value> = PendingCall {
            rx,
            guard,
            client: self.clone(),
            _result: PhantomData,
        };
        pending.await
    }

    // -- streams ----------------------------------------------------------

    async fn start_stream<M: Method>(
        &self,
        params: M::Params,
        want: StreamKind,
    ) -> Result<(mpsc::Receiver<StreamMsg>, CallGuard, EndReason), ControlError> {
        Client::check_streaming::<M>(want)?;
        self.check_fd_use::<M>(&params, false)?;
        let blob = Blob::encode(self.encoding(), &params)?;
        let lossy = want == StreamKind::Events;
        let (tx, rx) = mpsc::channel(if lossy { EVENT_BUFFER } else { CHUNK_BUFFER });
        let reason = EndReason::default();
        let slot = StreamSlot {
            tx,
            lossy,
            reason: reason.clone(),
        };
        let id = self.register(|| Slot::Stream(slot))?;
        let mut guard = self.guard(id);
        self.send_request(&mut guard, M::NAME, blob, None).await?;
        Ok((rx, guard, reason))
    }

    /// Open a subscription. The stream yields events until the daemon ends
    /// it, [`EventStream::cancel`] is called (the stream then just ends), or
    /// the connection drops (an `Unavailable` error item, then the end).
    pub async fn subscribe<M: Method>(
        &self,
        params: M::Params,
    ) -> Result<EventStream<M::Event>, ControlError> {
        let (rx, guard, reason) = self.start_stream::<M>(params, StreamKind::Events).await?;
        Ok(EventStream {
            rx,
            guard,
            reason,
            done: false,
            _event: PhantomData,
        })
    }

    /// Start a bulk-data method; the stream yields its chunks in order.
    pub async fn call_chunks<M: Method>(
        &self,
        params: M::Params,
    ) -> Result<ChunkStream, ControlError> {
        let (rx, guard, reason) = self.start_stream::<M>(params, StreamKind::Chunks).await?;
        Ok(ChunkStream {
            rx,
            guard,
            reason,
            done: false,
            next_seq: 0,
        })
    }
}

/// An in-flight unary call. Await it for the result.
pub struct PendingCall<T> {
    rx: oneshot::Receiver<Result<Blob, ControlError>>,
    guard: CallGuard,
    client: Client,
    _result: PhantomData<fn() -> T>,
}

impl<T> PendingCall<T> {
    /// The request id (for [`Client::cancel`]).
    pub fn id(&self) -> u64 {
        self.guard.id
    }

    /// Ask the daemon to cancel this call; keep awaiting it for the outcome.
    pub async fn cancel(&self) {
        self.guard.cancel().await;
    }

    /// The client this call belongs to.
    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl<T: serde::de::DeserializeOwned> Future for PendingCall<T> {
    type Output = Result<T, ControlError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(outcome) => {
                self.guard.finished = true;
                Poll::Ready(match outcome {
                    Ok(Ok(blob)) => blob.decode::<T>().map_err(|e| {
                        ControlError::protocol(format!("undecodable result: {}", e.message))
                    }),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(ControlError::unavailable("the connection was closed")),
                })
            }
        }
    }
}

/// A subscription's events. Dropping it cancels the subscription.
pub struct EventStream<E> {
    rx: mpsc::Receiver<StreamMsg>,
    guard: CallGuard,
    reason: EndReason,
    done: bool,
    _event: PhantomData<fn() -> E>,
}

impl<E> EventStream<E> {
    /// The subscription's id.
    pub fn id(&self) -> u64 {
        self.guard.id
    }

    /// End the subscription. Items already in flight may still arrive;
    /// then the stream ends.
    pub async fn cancel(&self) {
        self.guard.cancel().await;
    }
}

impl<E: serde::de::DeserializeOwned> Stream for EventStream<E> {
    type Item = Result<E, ControlError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    // The reader let go without an `End`: the connection
                    // died or we fell behind. Say so; never end quietly.
                    self.done = true;
                    self.guard.finished = true;
                    return Poll::Ready(Some(Err(ended_abnormally(&self.reason))));
                }
                Poll::Ready(Some(StreamMsg::Event(blob))) => {
                    return Poll::Ready(Some(blob.decode::<E>().map_err(|e| {
                        ControlError::protocol(format!("undecodable event: {}", e.message))
                    })));
                }
                Poll::Ready(Some(StreamMsg::Chunk { .. })) => continue,
                Poll::Ready(Some(StreamMsg::End(result))) => {
                    self.done = true;
                    self.guard.finished = true;
                    let asked = self.guard.cancel_requested.load(Ordering::SeqCst);
                    return match result {
                        Ok(_) => Poll::Ready(None),
                        // Cancelling a subscription is how it normally ends.
                        Err(e) if asked && e.kind == ErrorKind::Cancelled => Poll::Ready(None),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
            }
        }
    }
}

/// The chunks of a bulk-data call. Dropping it cancels the call.
pub struct ChunkStream {
    rx: mpsc::Receiver<StreamMsg>,
    guard: CallGuard,
    reason: EndReason,
    done: bool,
    next_seq: u64,
}

impl ChunkStream {
    pub fn id(&self) -> u64 {
        self.guard.id
    }

    pub async fn cancel(&self) {
        self.guard.cancel().await;
    }

    /// Read everything into one buffer (small results only).
    pub async fn collect_bytes(mut self) -> Result<Vec<u8>, ControlError> {
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(chunk) = self.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out)
    }
}

impl Stream for ChunkStream {
    type Item = Result<Bytes, ControlError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    // The reader let go without an `End`: the connection
                    // died or we fell behind. Say so; never end quietly.
                    self.done = true;
                    self.guard.finished = true;
                    return Poll::Ready(Some(Err(ended_abnormally(&self.reason))));
                }
                Poll::Ready(Some(StreamMsg::Chunk { seq, bytes })) => {
                    if seq != self.next_seq {
                        // Frames arrive in order on one connection, so a
                        // gap is a daemon bug; never hand out a file with
                        // a hole in it. (The guard cancels the rest.)
                        self.done = true;
                        return Poll::Ready(Some(Err(ControlError::protocol(format!(
                            "chunk {seq} arrived where chunk {} was due",
                            self.next_seq
                        )))));
                    }
                    self.next_seq += 1;
                    if bytes.is_empty() {
                        continue; // the terminal empty `last` chunk carries nothing
                    }
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(Some(StreamMsg::Event(_))) => continue,
                Poll::Ready(Some(StreamMsg::End(result))) => {
                    self.done = true;
                    self.guard.finished = true;
                    let asked = self.guard.cancel_requested.load(Ordering::SeqCst);
                    return match result {
                        Ok(_) => Poll::Ready(None),
                        Err(e) if asked && e.kind == ErrorKind::Cancelled => Poll::Ready(None),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
            }
        }
    }
}
