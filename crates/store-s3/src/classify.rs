//! Plan 39 §3.1: is an object-store failure worth waiting out?
//!
//! An `fsync` behaves like an NFS `hard` mount (nfs(5)): it keeps retrying
//! while S3 is *transiently* unreachable and returns only once the data is
//! durable. That needs one answer to one question, asked of every failure
//! on the durability path: will the same request, re-sent later and
//! unchanged, plausibly succeed? [`ErrorClass::Transient`] if so — a
//! timeout, a refused or reset connection, a DNS failure, a 5xx, a 429 or
//! `SlowDown`, a request the bucket timed out, a conditional write that
//! lost a race, an expired token the credential provider will replace.
//! [`ErrorClass::Permanent`] if no amount of waiting fixes it without an
//! operator: access denied with valid credentials, no such bucket, a
//! malformed request, a KMS key that is disabled or gone, content this
//! node no longer has. A permanent failure answers the `fsync` with `EIO`
//! at once; the data stays pending, and the next `fsync` tries again.
//!
//! **How.** The error's source chain is walked for the types that carry a
//! decisive fact — [`object_store::Error`]'s variant (403 maps to
//! `PermissionDenied`), the HTTP client's [`HttpErrorKind`], an
//! [`std::io::Error`]'s kind, this crate's [`StoreError`] — and what they
//! leave open is decided from the rendered chain, which carries the
//! status line and the S3 error body (`<Code>AccessDenied</Code>`). The
//! text path is also the only one for failures that reach the caller as
//! text (a sync round's outcome crosses the authority core as a string).
//!
//! **The default is `Transient`.** A failure nobody recognises keeps the
//! `fsync` waiting, as an NFS `hard` mount keeps retrying an RPC with no
//! answer: waiting is always safe for durability, an `EIO` never is
//! (whoever gets it may drop the data it was protecting), and the wait
//! stays bounded by an interrupt, by the operator's opt-in
//! `--fsync-timeout`, and is logged once it passes ten seconds.
//!
//! **Expired tokens.** `ExpiredToken` is transient only while the
//! process's credentials come from a source that refreshes them (the SDK
//! chain's SSO, IMDS, web identity…, or an engine's own refreshing
//! source): [`set_refreshable_credentials`] records that when an S3 client
//! is built. Static keys with an expired session token never heal.

use std::sync::atomic::{AtomicBool, Ordering};

use object_store::client::{HttpError, HttpErrorKind};

use crate::error::StoreError;

/// See the module doc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Re-sending the same request later may succeed: wait and retry.
    Transient,
    /// It will not, without an operator: fail now (the data stays pending).
    Permanent,
}

impl ErrorClass {
    pub fn is_transient(self) -> bool {
        self == ErrorClass::Transient
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorClass::Transient => "transient",
            ErrorClass::Permanent => "permanent",
        }
    }
}

static REFRESHABLE: AtomicBool = AtomicBool::new(false);

/// Whether this process's S3 credentials are refreshed by their provider
/// (an `ExpiredToken` then heals by itself). Set when a client is built;
/// a process serving several filesystems is refreshable if any is.
pub fn set_refreshable_credentials(refreshable: bool) {
    if refreshable {
        REFRESHABLE.store(true, Ordering::Relaxed);
    }
}

fn refreshable() -> bool {
    REFRESHABLE.load(Ordering::Relaxed)
}

/// Classify `error` and everything it wraps (see the module doc).
pub fn classify(error: &(dyn std::error::Error + 'static)) -> ErrorClass {
    let mut next: Option<&(dyn std::error::Error + 'static)> = Some(error);
    let mut rendered = String::new();
    while let Some(e) = next {
        if let Some(class) = classify_typed(e) {
            return class;
        }
        if !rendered.is_empty() {
            rendered.push_str(": ");
        }
        rendered.push_str(&e.to_string());
        next = e.source();
    }
    classify_message(&rendered)
}

/// [`classify`] for an error chain the caller holds as `anyhow`'s.
pub fn classify_chain<'a>(
    chain: impl IntoIterator<Item = &'a (dyn std::error::Error + 'static)>,
) -> ErrorClass {
    let mut rendered = String::new();
    for e in chain {
        if let Some(class) = classify_typed(e) {
            return class;
        }
        if !rendered.is_empty() {
            rendered.push_str(": ");
        }
        rendered.push_str(&e.to_string());
    }
    classify_message(&rendered)
}

/// What one link of a chain decides on its own, if anything.
fn classify_typed(e: &(dyn std::error::Error + 'static)) -> Option<ErrorClass> {
    if let Some(e) = e.downcast_ref::<object_store::Error>() {
        return match e {
            // 401/403: almost always permanent, but the body says whether
            // it is a token the provider will replace.
            object_store::Error::PermissionDenied { .. }
            | object_store::Error::Unauthenticated { .. } => {
                Some(match code_class(&e.to_string()) {
                    Some(class) => class,
                    None => ErrorClass::Permanent,
                })
            }
            object_store::Error::NotFound { .. } => Some(ErrorClass::Permanent),
            // A conditional write or read that lost a race: the protocol
            // re-reads and retries.
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotModified { .. } => Some(ErrorClass::Transient),
            object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::InvalidPath { .. }
            | object_store::Error::UnknownConfigurationKey { .. } => Some(ErrorClass::Permanent),
            // `Generic` (the S3 client's own errors, wrapping the retry
            // error with its status and body) and the rest: look deeper.
            _ => None,
        };
    }
    if let Some(e) = e.downcast_ref::<HttpError>() {
        return match e.kind() {
            HttpErrorKind::Connect
            | HttpErrorKind::Request
            | HttpErrorKind::Timeout
            | HttpErrorKind::Interrupted
            | HttpErrorKind::Decode => Some(ErrorClass::Transient),
            _ => None,
        };
    }
    if let Some(e) = e.downcast_ref::<std::io::Error>() {
        return io_class(e.kind());
    }
    if let Some(e) = e.downcast_ref::<StoreError>() {
        return match e {
            // Wrapped errors: their own link decides.
            StoreError::ObjectStore(_) | StoreError::Io(_) => None,
            StoreError::CasConflict | StoreError::Conflict(_) => Some(ErrorClass::Transient),
            // A credential source that could not answer (IMDS or SSO
            // unreachable) may answer later.
            StoreError::AwsCredentials(_) => Some(ErrorClass::Transient),
            StoreError::CorruptObject(_)
            | StoreError::UnknownCodec(_)
            | StoreError::HashMismatch { .. }
            | StoreError::AlreadyExists
            | StoreError::InboxVersion(_)
            | StoreError::NotFound
            | StoreError::Meta(_)
            | StoreError::Compression(_)
            | StoreError::Json(_)
            | StoreError::Parallel(_) => Some(ErrorClass::Permanent),
            _ => None,
        };
    }
    None
}

fn io_class(kind: std::io::ErrorKind) -> Option<ErrorClass> {
    use std::io::ErrorKind::*;
    match kind {
        ConnectionRefused | ConnectionReset | ConnectionAborted | NotConnected | TimedOut
        | BrokenPipe | UnexpectedEof | Interrupted | WouldBlock | HostUnreachable
        | NetworkUnreachable | NetworkDown | AddrNotAvailable | AddrInUse => {
            Some(ErrorClass::Transient)
        }
        NotFound | PermissionDenied | StorageFull | ReadOnlyFilesystem | InvalidData
        | InvalidInput | Unsupported | QuotaExceeded | FileTooLarge => Some(ErrorClass::Permanent),
        _ => None,
    }
}

/// S3 error codes (the `<Code>` of an error body) that decide by
/// themselves. Matched as whole words in the rendered chain.
const PERMANENT_CODES: &[&str] = &[
    "AccessDenied",
    "AccountProblem",
    "AllAccessDisabled",
    "AuthorizationHeaderMalformed",
    "EntityTooLarge",
    "InvalidAccessKeyId",
    "InvalidArgument",
    "InvalidBucketName",
    "InvalidBucketState",
    "InvalidDigest",
    "InvalidEncryptionAlgorithmError",
    "InvalidObjectState",
    "InvalidRequest",
    "InvalidSecurity",
    "InvalidStorageClass",
    "InvalidToken",
    "KeyTooLongError",
    "MalformedXML",
    "MethodNotAllowed",
    "MissingSecurityHeader",
    "NoSuchBucket",
    "NoSuchKey",
    "NotImplemented",
    "NotSignedUp",
    "PermanentRedirect",
    "RequestTimeTooSkewed",
    "SignatureDoesNotMatch",
    "UnauthorizedAccess",
    // KMS (SSE-KMS): a key that is disabled, pending deletion, gone, or
    // that this principal may not use. `KMS.ThrottlingException` is
    // transient and listed below.
    "KMS.AccessDeniedException",
    "KMS.DisabledException",
    "KMS.InvalidKeyUsageException",
    "KMS.KMSInvalidStateException",
    "KMS.NotFoundException",
    "KMS.InvalidStateException",
    "KMS.KeyUnavailableException",
];

const TRANSIENT_CODES: &[&str] = &[
    "BadDigest",
    "ConditionalRequestConflict",
    "IncompleteBody",
    "InternalError",
    "KMS.ThrottlingException",
    "OperationAborted",
    "RequestTimeout",
    "ServiceUnavailable",
    "SlowDown",
    "ThrottlingException",
    "TooManyRequests",
];

const EXPIRED_TOKEN_CODES: &[&str] = &[
    "ExpiredToken",
    "ExpiredTokenException",
    "TokenRefreshRequired",
];

/// Whether `text` contains `word` delimited by non-identifier bytes (so
/// `InvalidRequest` does not match inside `InvalidRequestFoo`).
fn has_word(text: &str, word: &str) -> bool {
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'.' || b == b'_';
    let bytes = text.as_bytes();
    let mut start = 0;
    while let Some(at) = text[start..].find(word) {
        let begin = start + at;
        let end = begin + word.len();
        let before_ok = begin == 0 || !is_ident(bytes[begin - 1]);
        let after_ok = end == bytes.len() || !is_ident(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        start = begin + 1;
    }
    false
}

/// The S3 error code in `text`, if it names one this table decides.
fn code_class(text: &str) -> Option<ErrorClass> {
    if EXPIRED_TOKEN_CODES.iter().any(|c| has_word(text, c)) {
        return Some(if refreshable() {
            ErrorClass::Transient
        } else {
            ErrorClass::Permanent
        });
    }
    if TRANSIENT_CODES.iter().any(|c| has_word(text, c)) {
        return Some(ErrorClass::Transient);
    }
    if PERMANENT_CODES.iter().any(|c| has_word(text, c)) {
        return Some(ErrorClass::Permanent);
    }
    None
}

/// The HTTP status a rendered object_store error names ("status code:
/// 503", "status 403 Forbidden"), if any.
fn http_status(text: &str) -> Option<u16> {
    for marker in ["status code: ", "status code ", "status: ", "status "] {
        let mut start = 0;
        while let Some(at) = text[start..].find(marker) {
            let digits: String = text[start + at + marker.len()..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if digits.len() == 3 {
                if let Ok(status) = digits.parse::<u16>() {
                    return Some(status);
                }
            }
            start += at + marker.len();
        }
    }
    None
}

/// Classify a failure known only as text (see the module doc).
pub fn classify_message(text: &str) -> ErrorClass {
    // Local content gone: no request will bring it back.
    if text.contains("missing from local cache") {
        return ErrorClass::Permanent;
    }
    if let Some(class) = code_class(text) {
        return class;
    }
    if let Some(status) = http_status(text) {
        return match status {
            408 | 409 | 412 | 425 | 429 => ErrorClass::Transient,
            500..=599 => ErrorClass::Transient,
            400..=499 => ErrorClass::Permanent,
            _ => ErrorClass::Transient,
        };
    }
    ErrorClass::Transient
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(text: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: text.to_string().into(),
        }
    }

    /// The classification table (plan 39 §3.1), as object_store renders
    /// S3's answers: every row is one failure an `fsync` may meet.
    #[test]
    fn the_classification_table() {
        let transient = ErrorClass::Transient;
        let permanent = ErrorClass::Permanent;
        let rows: Vec<(&str, Box<dyn std::error::Error + Send + Sync>, ErrorClass)> = vec![
            (
                "connect refused (toxiproxy cut)",
                Box::new(generic(
                    "Error performing PUT http://127.0.0.1:4566/b/chunk/ab in 2s, after 2 retries \
                     - HTTP error: error sending request: client error (Connect): tcp connect \
                     error: Connection refused (os error 111)",
                )),
                transient,
            ),
            (
                "http connect error",
                Box::new(HttpError::new(HttpErrorKind::Connect, generic("refused"))),
                transient,
            ),
            (
                "http timeout",
                Box::new(HttpError::new(HttpErrorKind::Timeout, generic("timed out"))),
                transient,
            ),
            (
                "truncated response body",
                Box::new(HttpError::new(HttpErrorKind::Decode, generic("eof"))),
                transient,
            ),
            (
                "io reset",
                Box::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
                transient,
            ),
            (
                "dns failure",
                Box::new(generic(
                    "error sending request: dns error: failed to lookup address information",
                )),
                transient,
            ),
            (
                "503 SlowDown",
                Box::new(generic(
                    "Server returned non-2xx status code: 503 Service Unavailable: \
                     <Error><Code>SlowDown</Code><Message>Please reduce your request rate.\
                     </Message></Error>",
                )),
                transient,
            ),
            (
                "500 InternalError",
                Box::new(generic(
                    "Server returned non-2xx status code: 500 Internal Server Error: \
                     <Error><Code>InternalError</Code></Error>",
                )),
                transient,
            ),
            (
                "502 without a body",
                Box::new(generic(
                    "Server returned non-2xx status code: 502 Bad Gateway: ",
                )),
                transient,
            ),
            (
                "429",
                Box::new(generic(
                    "Server returned non-2xx status code: 429 Too Many Requests: ",
                )),
                transient,
            ),
            (
                "400 RequestTimeout",
                Box::new(generic(
                    "Server returned non-2xx status code: 400 Bad Request: \
                     <Error><Code>RequestTimeout</Code></Error>",
                )),
                transient,
            ),
            (
                "409 OperationAborted",
                Box::new(generic(
                    "Server returned non-2xx status code: 409 Conflict: \
                     <Error><Code>OperationAborted</Code></Error>",
                )),
                transient,
            ),
            (
                "412 lost CAS race",
                Box::new(object_store::Error::Precondition {
                    path: "lease".into(),
                    source: "etag".into(),
                }),
                transient,
            ),
            (
                "KMS throttled",
                Box::new(generic(
                    "Server returned non-2xx status code: 400 Bad Request: \
                     <Error><Code>KMS.ThrottlingException</Code></Error>",
                )),
                transient,
            ),
            (
                "credential source unreachable",
                Box::new(StoreError::AwsCredentials("IMDS timed out".into())),
                transient,
            ),
            (
                "403 AccessDenied",
                Box::new(object_store::Error::PermissionDenied {
                    path: "chunk/ab".into(),
                    source: "Server returned non-2xx status code: 403 Forbidden: \
                             <Error><Code>AccessDenied</Code></Error>"
                        .into(),
                }),
                permanent,
            ),
            (
                "403 SignatureDoesNotMatch",
                Box::new(generic(
                    "Server returned non-2xx status code: 403 Forbidden: \
                     <Error><Code>SignatureDoesNotMatch</Code></Error>",
                )),
                permanent,
            ),
            (
                "403 InvalidAccessKeyId",
                Box::new(generic("<Error><Code>InvalidAccessKeyId</Code></Error>")),
                permanent,
            ),
            (
                "404 NoSuchBucket",
                Box::new(object_store::Error::NotFound {
                    path: "chunk/ab".into(),
                    source: "<Error><Code>NoSuchBucket</Code></Error>".into(),
                }),
                permanent,
            ),
            (
                "400 InvalidRequest",
                Box::new(generic(
                    "Server returned non-2xx status code: 400 Bad Request: \
                     <Error><Code>InvalidRequest</Code></Error>",
                )),
                permanent,
            ),
            (
                "400 InvalidArgument",
                Box::new(generic(
                    "Server returned non-2xx status code: 400 Bad Request: \
                     <Error><Code>InvalidArgument</Code></Error>",
                )),
                permanent,
            ),
            (
                "KMS key disabled",
                Box::new(generic(
                    "Server returned non-2xx status code: 400 Bad Request: \
                     <Error><Code>KMS.DisabledException</Code></Error>",
                )),
                permanent,
            ),
            (
                "KMS key gone",
                Box::new(generic("<Error><Code>KMS.NotFoundException</Code></Error>")),
                permanent,
            ),
            (
                "unknown 4xx",
                Box::new(generic(
                    "Server returned non-2xx status code: 405 Method Not Allowed: ",
                )),
                permanent,
            ),
            (
                "corrupt chunk object",
                Box::new(StoreError::CorruptObject("bad frame".into())),
                permanent,
            ),
            (
                "local disk full",
                Box::new(std::io::Error::from(std::io::ErrorKind::StorageFull)),
                permanent,
            ),
            (
                "unrecognised failure",
                Box::new(generic("something nobody has seen before")),
                transient,
            ),
        ];
        for (name, error, want) in rows {
            assert_eq!(classify(error.as_ref()), want, "{name}: {error}");
        }
    }

    #[test]
    fn a_wrapped_error_is_classified_by_what_it_wraps() {
        let inner = StoreError::ObjectStore(object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "AccessDenied".into(),
        });
        assert_eq!(classify(&inner), ErrorClass::Permanent);
        let inner = StoreError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert_eq!(classify(&inner), ErrorClass::Transient);
    }

    #[test]
    fn messages_known_only_as_text() {
        for (text, want) in [
            ("journal not shipped: no lease", ErrorClass::Transient),
            // `Control::Barrier`'s refusals since fix snap-drain-busy.
            (
                "journal not shipped through position 412: this node does not hold the write lease",
                ErrorClass::Transient,
            ),
            (
                "journal not shipped through position 412 after 3 sync rounds \
                 (held back behind a chunk that cannot be uploaded)",
                ErrorClass::Transient,
            ),
            (
                "journal not shipped through position 412: this node's write lease is not \
                 usable yet (a takeover gate or an expiry)",
                ErrorClass::Transient,
            ),
            (
                "journal not shipped through position 412: this node was deposed and is recovering",
                ErrorClass::Transient,
            ),
            (
                "journal not shipped: this node was deposed and is recovering \
                 (its unshipped rows are replayed to the new holder)",
                ErrorClass::Transient,
            ),
            (
                "journal not shipped through position 412: this node's stranded ops are still \
                 being replayed to the holder",
                ErrorClass::Transient,
            ),
            (
                "pending upload chunk ab12 missing from local cache",
                ErrorClass::Permanent,
            ),
            (
                "upload ab12 failed: Generic S3 error: ... status code: 503 ...",
                ErrorClass::Transient,
            ),
            ("put: <Code>NoSuchBucket</Code>", ErrorClass::Permanent),
            ("timed out", ErrorClass::Transient),
            // Whole words only: a longer code is not the shorter one.
            ("<Code>InvalidRequestX</Code>", ErrorClass::Transient),
        ] {
            assert_eq!(classify_message(text), want, "{text}");
        }
    }

    #[test]
    fn an_expired_token_waits_only_for_credentials_that_refresh() {
        let text = "Server returned non-2xx status code: 400 Bad Request: \
                    <Error><Code>ExpiredToken</Code></Error>";
        // Static keys (the process default until a client says otherwise).
        if !refreshable() {
            assert_eq!(classify_message(text), ErrorClass::Permanent);
        }
        set_refreshable_credentials(true);
        assert_eq!(classify_message(text), ErrorClass::Transient);
        let denied = object_store::Error::PermissionDenied {
            path: "p".into(),
            source: "<Code>ExpiredToken</Code>".into(),
        };
        assert_eq!(classify(&denied), ErrorClass::Transient);
    }
}
