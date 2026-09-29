//! [`NamedPipe`]: the Windows transport, a stub until plan 35.
//!
//! The type exists on every host so the rest of the crate (and its callers)
//! compile everywhere and can name it; every operation answers
//! `Code::NotSupported`. Plan 35 fills it in with `tokio::net::windows::
//! named_pipe`, `GetNamedPipeClientProcessId`-based SID lookup for the
//! principal, and no fd passing (`supports_fd_passing` stays false, so
//! `view.mount{source: PreopenedFd}` is refused with `NotSupported` there,
//! exactly as plan 31 §9.9 requires).

use super::{Frame, Transport, TransportError};
use crate::authz::Principal;
use futures::future::BoxFuture;

const WHY: &str = "the named-pipe transport arrives with plan 35";

#[derive(Debug)]
pub struct NamedPipe {
    _private: (),
}

impl NamedPipe {
    /// Connect to the pipe `name`. Always `NotSupported` for now.
    pub async fn connect(name: &str) -> Result<NamedPipe, TransportError> {
        let _ = name;
        Err(TransportError::NotSupported(WHY))
    }

    /// Create the server end of the pipe `name`. Always `NotSupported` for
    /// now.
    pub async fn listen(name: &str) -> Result<NamedPipe, TransportError> {
        let _ = name;
        Err(TransportError::NotSupported(WHY))
    }
}

impl Transport for NamedPipe {
    fn send_frame(&self, _frame: Frame) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async { Err(TransportError::NotSupported(WHY)) })
    }

    fn recv_frame(&self) -> BoxFuture<'_, Result<Option<Frame>, TransportError>> {
        Box::pin(async { Err(TransportError::NotSupported(WHY)) })
    }

    fn supports_fd_passing(&self) -> bool {
        false
    }

    fn peer(&self) -> Principal {
        Principal::WindowsSid(String::new())
    }

    fn name(&self) -> &'static str {
        "named-pipe"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_types::Code;

    #[tokio::test]
    async fn every_entry_point_is_not_supported() {
        let err = NamedPipe::connect(r"\\.\pipe\constellation")
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotSupported);
        let err = NamedPipe::listen("x").await.unwrap_err();
        assert_eq!(err.code(), Code::NotSupported);
    }
}
