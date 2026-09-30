//! [`InProcess`]: a pair of connected endpoints inside one process.
//!
//! Frames move over bounded channels, so the byte-level codec and its 8 MiB
//! length prefix are skipped, but everything above it — envelope encoding,
//! handshake, authz, cancellation, streaming — runs unchanged. That is the
//! point: the in-process CLI, the harness, and the web adapter exercise the
//! *same* server path as a socket client. The frame-size limit still
//! applies (a frame the socket would refuse is refused here too), so a
//! large-payload bug cannot hide behind the in-process shortcut.
//!
//! File descriptors move as-is: an `OwnedFd` in a [`Frame`] is simply
//! handed to the other side, no `SCM_RIGHTS`.
//!
//! The creator decides what principal the *server* end sees
//! ([`InProcess::pair_as`]); by default it is [`Principal::InProcess`],
//! which the default policy treats as an admin.

use super::{Frame, Transport, TransportError};
use crate::authz::Principal;
use crate::proto::{FrameError, MAX_FRAME_LEN};
use futures::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Frames buffered per direction before a sender waits.
const CAPACITY: usize = 64;

pub struct InProcess {
    tx: mpsc::Sender<Frame>,
    rx: Mutex<mpsc::Receiver<Frame>>,
    peer: Principal,
}

impl InProcess {
    /// A connected pair `(client_end, server_end)`; the server sees
    /// [`Principal::InProcess`].
    pub fn pair() -> (Arc<InProcess>, Arc<InProcess>) {
        InProcess::pair_as(Principal::InProcess)
    }

    /// A pair where the server end sees `client` as the peer (to test
    /// authorization with a synthetic identity).
    pub fn pair_as(client: Principal) -> (Arc<InProcess>, Arc<InProcess>) {
        let (c2s_tx, c2s_rx) = mpsc::channel(CAPACITY);
        let (s2c_tx, s2c_rx) = mpsc::channel(CAPACITY);
        let client_end = Arc::new(InProcess {
            tx: c2s_tx,
            rx: Mutex::new(s2c_rx),
            peer: Principal::InProcess,
        });
        let server_end = Arc::new(InProcess {
            tx: s2c_tx,
            rx: Mutex::new(c2s_rx),
            peer: client,
        });
        (client_end, server_end)
    }
}

impl Transport for InProcess {
    fn send_frame(&self, frame: Frame) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            if frame.payload.len() + 1 > MAX_FRAME_LEN {
                return Err(FrameError::TooLarge {
                    len: frame.payload.len() + 1,
                    max: MAX_FRAME_LEN,
                }
                .into());
            }
            self.tx
                .send(frame)
                .await
                .map_err(|_| TransportError::Closed)
        })
    }

    fn recv_frame(&self) -> BoxFuture<'_, Result<Option<Frame>, TransportError>> {
        Box::pin(async move { Ok(self.rx.lock().await.recv().await) })
    }

    fn supports_fd_passing(&self) -> bool {
        true
    }

    fn peer(&self) -> Principal {
        self.peer.clone()
    }

    fn name(&self) -> &'static str {
        "in-process"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::FrameKind;
    use bytes::Bytes;

    #[tokio::test]
    async fn frames_flow_both_ways_and_close_ends_the_stream() {
        let (a, b) = InProcess::pair_as(Principal::Remote { device: "d".into() });
        a.send_frame(Frame::new(FrameKind::Hello, Bytes::from_static(b"hi")))
            .await
            .unwrap();
        let got = b.recv_frame().await.unwrap().unwrap();
        assert_eq!(got.kind, FrameKind::Hello);
        assert_eq!(&got.payload[..], b"hi");
        assert_eq!(b.peer(), Principal::Remote { device: "d".into() });
        b.send_frame(Frame::new(FrameKind::Welcome, Bytes::new()))
            .await
            .unwrap();
        assert_eq!(
            a.recv_frame().await.unwrap().unwrap().kind,
            FrameKind::Welcome
        );
        drop(a);
        assert!(b.recv_frame().await.unwrap().is_none());
        assert!(matches!(
            b.send_frame(Frame::new(FrameKind::Cancel, Bytes::new()))
                .await,
            Err(TransportError::Closed)
        ));
    }

    #[tokio::test]
    async fn oversize_frames_are_refused_like_on_a_socket() {
        let (a, _b) = InProcess::pair();
        let big = Bytes::from(vec![0u8; MAX_FRAME_LEN]);
        assert!(matches!(
            a.send_frame(Frame::new(FrameKind::Chunk, big)).await,
            Err(TransportError::Frame(_))
        ));
    }
}
