//! [`StreamTransport`]: framing over any `AsyncRead + AsyncWrite`.
//!
//! The portable baseline: whatever byte stream you have (a
//! `tokio::io::duplex` in tests, later a TLS-wrapped TCP stream for remote
//! devices or a Windows named pipe) becomes a control transport by wrapping
//! it here. It has no out-of-band channel, so
//! [`supports_fd_passing`](Transport::supports_fd_passing) is false and a
//! frame carrying a descriptor fails `NotSupported` *at the sender*,
//! promptly — the answer to "a method needing an fd on a transport without
//! one must not hang".

use super::{Frame, Transport, TransportError};
use crate::authz::Principal;
use crate::proto::{decode_frame, encode_frame};
use bytes::BytesMut;
use futures::future::BoxFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::Mutex;

struct Reader<S> {
    half: ReadHalf<S>,
    buf: BytesMut,
}

pub struct StreamTransport<S> {
    reader: Mutex<Reader<S>>,
    writer: Mutex<WriteHalf<S>>,
    poisoned: AtomicBool,
    peer: Principal,
}

impl<S: AsyncRead + AsyncWrite + Send + 'static> StreamTransport<S> {
    /// Wrap `stream`; `peer` is the identity the creator vouches for.
    pub fn new(stream: S, peer: Principal) -> StreamTransport<S> {
        let (read, write) = tokio::io::split(stream);
        StreamTransport {
            reader: Mutex::new(Reader {
                half: read,
                buf: BytesMut::with_capacity(8192),
            }),
            writer: Mutex::new(write),
            poisoned: AtomicBool::new(false),
            peer,
        }
    }
}

/// Marks the transport poisoned if a send is dropped before it finishes.
struct PoisonGuard<'a> {
    flag: &'a AtomicBool,
    finished: bool,
}

impl Drop for PoisonGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.flag.store(true, Ordering::SeqCst);
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Send + 'static> Transport for StreamTransport<S> {
    fn send_frame(&self, frame: Frame) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            if frame.fd.is_some() {
                return Err(TransportError::NotSupported(
                    "this transport cannot pass file descriptors",
                ));
            }
            let mut out = BytesMut::new();
            encode_frame(frame.kind, false, &frame.payload, &mut out)?;
            let mut writer = self.writer.lock().await;
            if self.poisoned.load(Ordering::SeqCst) {
                return Err(TransportError::Closed);
            }
            let mut guard = PoisonGuard {
                flag: &self.poisoned,
                finished: false,
            };
            let result = async {
                writer.write_all(&out).await?;
                writer.flush().await
            }
            .await;
            guard.finished = true;
            if result.is_err() {
                // `write_all` may have written part of the frame: nothing
                // after it can be trusted (as on the unix socket).
                self.poisoned.store(true, Ordering::SeqCst);
            }
            result.map_err(TransportError::from)
        })
    }

    fn recv_frame(&self) -> BoxFuture<'_, Result<Option<Frame>, TransportError>> {
        Box::pin(async move {
            let mut reader = self.reader.lock().await;
            loop {
                if let Some(raw) = decode_frame(&mut reader.buf)? {
                    if raw.has_fd {
                        return Err(TransportError::Protocol(
                            "frame announces a file descriptor on a transport without them".into(),
                        ));
                    }
                    return Ok(Some(Frame::new(raw.kind, raw.payload)));
                }
                let Reader { half, buf } = &mut *reader;
                let n = half.read_buf(buf).await?;
                if n == 0 {
                    return if reader.buf.is_empty() {
                        Ok(None)
                    } else {
                        Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into())
                    };
                }
            }
        })
    }

    fn supports_fd_passing(&self) -> bool {
        false
    }

    fn peer(&self) -> Principal {
        self.peer.clone()
    }

    fn name(&self) -> &'static str {
        "stream"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FrameKind;
    use bytes::Bytes;

    type Duplex = StreamTransport<tokio::io::DuplexStream>;

    fn pair() -> (Duplex, Duplex) {
        let (a, b) = tokio::io::duplex(64);
        (
            StreamTransport::new(a, Principal::InProcess),
            StreamTransport::new(b, Principal::InProcess),
        )
    }

    #[tokio::test]
    async fn frames_round_trip_across_a_tiny_pipe() {
        let (a, b) = pair();
        let big = Bytes::from(vec![7u8; 10_000]); // far larger than the 64-byte pipe
        let send = tokio::spawn(async move {
            a.send_frame(Frame::new(FrameKind::Request, big))
                .await
                .unwrap();
            a.send_frame(Frame::new(FrameKind::Cancel, Bytes::from_static(b"x")))
                .await
                .unwrap();
            a
        });
        let f1 = b.recv_frame().await.unwrap().unwrap();
        assert_eq!((f1.kind, f1.payload.len()), (FrameKind::Request, 10_000));
        let f2 = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(f2.kind, FrameKind::Cancel);
        drop(send.await.unwrap());
        assert!(b.recv_frame().await.unwrap().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fd_frames_are_refused_promptly() {
        use constellation_types::Code;
        let (a, _b) = pair();
        assert!(!a.supports_fd_passing());
        let file = std::fs::File::open("/dev/null").unwrap();
        let err = a
            .send_fd(
                FrameKind::Request,
                Bytes::new(),
                std::os::fd::AsFd::as_fd(&file),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotSupported);
    }

    #[tokio::test]
    async fn garbage_and_truncation_are_errors() {
        let (mut raw, other) = tokio::io::duplex(1024);
        let t = StreamTransport::new(other, Principal::InProcess);
        raw.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        assert!(matches!(
            t.recv_frame().await,
            Err(TransportError::Frame(_))
        ));

        let (mut raw, other) = tokio::io::duplex(1024);
        let t = StreamTransport::new(other, Principal::InProcess);
        raw.write_all(&[0, 0, 0, 9, 3, 1, 2]).await.unwrap(); // promises 8 bytes, gives 2
        drop(raw);
        assert!(matches!(t.recv_frame().await, Err(TransportError::Io(_))));
    }
}
