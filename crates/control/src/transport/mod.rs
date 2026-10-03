//! Transports: how frames get between a client and the daemon, who the
//! transport says the peer is, and whether it can carry file descriptors.
//!
//! A [`Transport`] is one *connected* endpoint that speaks whole
//! [`Frame`]s. It knows nothing about requests, encodings or methods — the
//! [server](crate::server) and [client](crate::client) sit on top of it and
//! are identical for every transport, which is what makes the C5 parity test
//! ("unix socket vs. HTTP dispatch") meaningful: only the bytes-on-the-wire
//! differ.
//!
//! | transport | bytes | peer identity | fd passing |
//! |---|---|---|---|
//! | `UnixSocket` (unix) | length-prefixed frames on a `SOCK_STREAM` | kernel peer credentials + supplementary groups | `SCM_RIGHTS` |
//! | [`InProcess`] | channels; no serialization of the frame envelope | whatever the creator says (default [`Principal::InProcess`]) | direct (an `OwnedFd` moves) |
//! | [`StreamTransport`] | length-prefixed frames on any `AsyncRead + AsyncWrite` (tests, a future TCP/TLS) | fixed by the creator | none |
//! | [`NamedPipe`] | stub until plan 35 | — | none |
//!
//! ## How a request says "an fd is attached"
//!
//! The plan's sketch (`send_fd`/`recv_fd` as separate calls) cannot tell the
//! receiver *which request* a descriptor belongs to when calls interleave.
//! Instead the descriptor is part of the frame: bit 7 of the frame's kind
//! byte ([`FLAG_FD`](crate::proto::FLAG_FD)) means "one descriptor rides with
//! this frame", and [`Frame::fd`] carries it. On the unix socket the sender
//! puts the `SCM_RIGHTS` ancillary data on the `sendmsg` that carries the
//! frame's first byte; the receiver collects every descriptor `recvmsg`
//! hands it into a FIFO and pairs the *n*-th flagged frame with the *n*-th
//! descriptor. Frames are written atomically under a lock and the kernel
//! never delivers a descriptor before the first byte of the message it was
//! sent with, so by the time a flagged frame is fully read its descriptor
//! has arrived. A flagged frame with no descriptor (a peer that lied) is a
//! connection error, not a hang.
//!
//! Handlers get the descriptor as `CallCtx::take_fd`, and the router
//! refuses a request that *needs* an fd on a transport that
//! [cannot carry one](Transport::supports_fd_passing) with
//! `Code::NotSupported` before any handler runs.
//!
//! ## Cancel safety
//!
//! `send_frame` is **not** cancel-safe on stream transports: dropping the
//! future halfway through a write leaves half a frame on the wire. Such a
//! transport marks itself poisoned and every later send fails, rather than
//! desynchronizing the peer. The server and client never drop a send in
//! flight (each send runs in a task that is not aborted mid-write).

mod inprocess;
mod path;
mod pipe;
mod stream;
#[cfg(unix)]
pub(crate) mod unix;

pub use inprocess::InProcess;
pub use path::{
    default_socket_path, ensure_socket_dir, forget_socket, instance_for_state_dir, locate_socket,
    record_socket, socket_path_for, socket_path_for_state_dir, SocketPathError, LOCATOR_FILE,
    SOCKET_SUFFIX,
};
pub use pipe::NamedPipe;
pub use stream::StreamTransport;
#[cfg(unix)]
pub use unix::{UnixSocket, UnixSocketListener};

use crate::authz::Principal;
use crate::fd::{BorrowedFd, OwnedFd};
use crate::proto::{ControlError, FrameError, FrameKind};
use bytes::Bytes;
use constellation_types::Code;
use futures::future::BoxFuture;
use std::sync::Arc;

/// One frame plus the descriptor riding with it, if any.
#[derive(Debug)]
pub struct Frame {
    pub kind: FrameKind,
    pub payload: Bytes,
    pub fd: Option<OwnedFd>,
}

impl Frame {
    pub fn new(kind: FrameKind, payload: Bytes) -> Frame {
        Frame {
            kind,
            payload,
            fd: None,
        }
    }

    pub fn with_fd(kind: FrameKind, payload: Bytes, fd: OwnedFd) -> Frame {
        Frame {
            kind,
            payload,
            fd: Some(fd),
        }
    }

    /// Take the attached descriptor (the "recv_fd" of the plan's sketch).
    pub fn take_fd(&mut self) -> Option<OwnedFd> {
        self.fd.take()
    }
}

/// Why a transport operation failed.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("bad frame: {0}")]
    Frame(#[from] FrameError),
    /// The peer sent something the transport layer cannot accept (a flagged
    /// frame without its descriptor, too many queued descriptors).
    #[error("protocol violation: {0}")]
    Protocol(String),
    #[error("connection closed")]
    Closed,
    /// This transport does not do that (fd passing on a stream, anything on
    /// the named-pipe stub).
    #[error("not supported: {0}")]
    NotSupported(&'static str),
}

impl TransportError {
    /// The portable errno for this failure.
    pub fn code(&self) -> Code {
        match self {
            TransportError::Io(e) => Code::from_io_error(e),
            TransportError::Frame(_) | TransportError::Protocol(_) => Code::Protocol,
            TransportError::Closed => Code::NotConnected,
            TransportError::NotSupported(_) => Code::NotSupported,
        }
    }
}

impl From<TransportError> for ControlError {
    fn from(e: TransportError) -> ControlError {
        let message = e.to_string();
        let mut err = ControlError::from(e.code());
        err.message = message;
        err
    }
}

/// A connected endpoint. See the [module docs](self).
pub trait Transport: Send + Sync + 'static {
    /// Send one frame. Frames from concurrent callers are written whole and
    /// never interleave. A frame carrying an fd on a transport that cannot
    /// pass one fails [`TransportError::NotSupported`].
    fn send_frame(&self, frame: Frame) -> BoxFuture<'_, Result<(), TransportError>>;

    /// The next frame; `Ok(None)` is a clean end of stream. One reader at a
    /// time (a second concurrent call waits its turn).
    fn recv_frame(&self) -> BoxFuture<'_, Result<Option<Frame>, TransportError>>;

    /// Whether descriptors can ride on frames.
    fn supports_fd_passing(&self) -> bool;

    /// Who the transport says the other end is. On a listener-side
    /// transport this is the connecting client; on a client-side transport
    /// it is the server (rarely interesting).
    fn peer(&self) -> Principal;

    /// A short name for logs (`"unix"`, `"in-process"`, …).
    fn name(&self) -> &'static str;

    /// Send `payload` as a `kind` frame with a duplicate of `fd` attached
    /// (`send_fd` of the plan's sketch: the caller keeps its own fd).
    fn send_fd<'a>(
        &'a self,
        kind: FrameKind,
        payload: Bytes,
        fd: BorrowedFd<'a>,
    ) -> BoxFuture<'a, Result<(), TransportError>> {
        Box::pin(async move {
            if !self.supports_fd_passing() {
                return Err(TransportError::NotSupported(
                    "this transport cannot pass file descriptors",
                ));
            }
            #[cfg(unix)]
            {
                let owned = fd.try_clone_to_owned()?;
                self.send_frame(Frame::with_fd(kind, payload, owned)).await
            }
            #[cfg(not(unix))]
            {
                let _ = (kind, payload, fd);
                Err(TransportError::NotSupported(
                    "no file descriptors on this host",
                ))
            }
        })
    }
}

/// Something that accepts connections (a bound socket, a test double).
pub trait Listener: Send + 'static {
    /// Wait for the next connection.
    fn accept(&mut self) -> BoxFuture<'_, std::io::Result<Arc<dyn Transport>>>;
}
