//! Which store failures say the store or the path is overloaded.

/// Whether a failed request to the store says the store or the path is overloaded, so that
/// fewer requests in flight would help. `error` is a store error
/// (a `StoreError`, alone or inside an `anyhow::Error`); its `source()` chain
/// is walked. Shared by the upload and the readahead controllers.
///
/// The walk stops at the first `HttpError`, whose kind is object_store's own
/// classification of the failure, and decides by it: Connect, Request and
/// Interrupted mean S3 was unreachable, any other kind is congestion. Only a
/// chain without an `HttpError` falls back to the `io::Error` kind check.
///
/// `false` when the request got no response because S3 was unreachable: a
/// connection that could not be made (refused, unreachable), or was reset,
/// aborted or interrupted. Fewer requests would not bring S3 back, and
/// halving the target on every cut left it far below the link's capacity, so
/// files rewritten during the cuts piled up pending rows that an `fsync`
/// then had to drain. `true` for everything else: timeouts, 5xx,
/// 429/SlowDown, decode errors and errors we do not recognise keep backing
/// off. A blackholed S3 (the connect times out) is classed Timeout, not
/// Connect, and still halves the target.
///
/// Only the error of the last of the three attempts is classified: two
/// timeouts followed by a reset count as an outage.
///
/// Accepted risk: a store that sheds load by resetting connections no
/// longer lowers the target.
pub(crate) fn is_congestion(error: &(dyn std::error::Error + 'static)) -> bool {
    use object_store::client::{HttpError, HttpErrorKind};
    use std::io::ErrorKind;
    let mut link = Some(error);
    while let Some(error) = link {
        if let Some(http) = error.downcast_ref::<HttpError>() {
            return !matches!(
                http.kind(),
                HttpErrorKind::Connect | HttpErrorKind::Request | HttpErrorKind::Interrupted
            );
        }
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            if matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::BrokenPipe
                    | ErrorKind::HostUnreachable
                    | ErrorKind::NetworkUnreachable
                    | ErrorKind::NetworkDown
            ) {
                return false;
            }
        }
        link = error.source();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Which failed-request errors count as congestion: an unreachable S3
    /// (Connect, Request, Interrupted, connection-level io kinds) does not,
    /// timeouts, decode errors and unrecognised errors do, also through the
    /// `StoreError` layer. An `HttpError` decides alone: a Timeout wrapping a
    /// connection reset is still congestion.
    #[test]
    fn outage_errors_are_not_congestion() {
        use object_store::client::{HttpError, HttpErrorKind};
        use std::io::{Error as IoError, ErrorKind};
        let http = |kind| HttpError::new(kind, IoError::from(ErrorKind::Other));
        let generic =
            |source: Box<dyn std::error::Error + Send + Sync>| object_store::Error::Generic {
                store: "test",
                source,
            };
        for kind in [
            HttpErrorKind::Connect,
            HttpErrorKind::Request,
            HttpErrorKind::Interrupted,
        ] {
            assert!(!is_congestion(&generic(Box::new(http(kind)))), "{kind:?}");
        }
        for kind in [
            HttpErrorKind::Timeout,
            HttpErrorKind::Decode,
            HttpErrorKind::Unknown,
        ] {
            assert!(is_congestion(&generic(Box::new(http(kind)))), "{kind:?}");
        }
        for kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
            ErrorKind::BrokenPipe,
            ErrorKind::HostUnreachable,
            ErrorKind::NetworkUnreachable,
            ErrorKind::NetworkDown,
        ] {
            let error = generic(Box::new(IoError::from(kind)));
            assert!(!is_congestion(&error), "{kind:?}");
        }
        assert!(is_congestion(&generic(Box::new(IoError::from(
            ErrorKind::TimedOut
        )))));
        assert!(is_congestion(&generic("503 SlowDown".into())));
        // Through the layers the store adds on the way up.
        let wrapped = constellation_store_s3::StoreError::from(generic(Box::new(http(
            HttpErrorKind::Request,
        ))));
        assert!(!is_congestion(&wrapped));
        let wrapped = constellation_store_s3::StoreError::from(generic(Box::new(http(
            HttpErrorKind::Timeout,
        ))));
        assert!(is_congestion(&wrapped));
        // The first HttpError decides; a reset buried under a Timeout does not.
        let buried = HttpError::new(
            HttpErrorKind::Timeout,
            IoError::from(ErrorKind::ConnectionReset),
        );
        assert!(is_congestion(&generic(Box::new(buried))));
    }
}
