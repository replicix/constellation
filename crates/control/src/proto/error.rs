//! [`ControlError`]: the one error shape every control call fails with.
//!
//! An error crosses a process boundary, so it is data, not a Rust type:
//!
//! - [`ControlError::code`] is the portable errno ([`Code`], serialized as
//!   its own `u16`) when the failure *is* a filesystem refusal (`ENOENT`,
//!   `EACCES`, …). It is `None` for failures that have no errno flavour
//!   (an unknown method, a malformed request).
//! - [`ControlError::kind`] is the coarse class a UI or script branches on
//!   without knowing every errno: not found / denied / invalid / … . It is
//!   always present and is derived from the code when a code is set
//!   ([`ErrorKind::of_code`]).
//! - `message` is for humans, `remediation` is the "what to do about it"
//!   line (`constellation` CLI prints both), `details` carries structured
//!   context (e.g. which role was required) as free-form JSON.
//!
//! Cancellation is `kind: Cancelled` with `code: Intr` — a cancelled call
//! looks to a POSIX-minded caller exactly like an interrupted syscall.

use crate::proto::JsonValue;
use constellation_types::Code;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// The coarse class of a [`ControlError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// The named object (path, filesystem, snapshot, method target) does
    /// not exist.
    NotFound,
    /// The principal's role or the filesystem's permissions refuse it.
    Denied,
    /// The request is malformed or its parameters are wrong.
    Invalid,
    /// This daemon or transport does not implement the operation (an
    /// unknown method, or an fd on a transport that cannot pass one).
    Unsupported,
    /// The operation was attempted and failed.
    Failed,
    /// The caller cancelled it (or the connection dropped mid-call).
    Cancelled,
    /// A transient condition: retry later (busy, connection closed,
    /// too many calls in flight).
    Unavailable,
    /// The target exists / is in a state that conflicts with the request.
    Conflict,
    /// A client-side deadline elapsed ([`crate::client::Client::call_bounded`]).
    Timeout,
}

impl ErrorKind {
    /// The kind a filesystem [`Code`] belongs to.
    pub fn of_code(code: Code) -> ErrorKind {
        match code {
            Code::NotFound | Code::NoData => ErrorKind::NotFound,
            Code::Exists | Code::NotEmpty | Code::Already => ErrorKind::Conflict,
            Code::Access | Code::Perm | Code::ReadOnly => ErrorKind::Denied,
            Code::Invalid
            | Code::Range
            | Code::NameTooLong
            | Code::Domain
            | Code::NotDir
            | Code::IsDir
            | Code::Protocol
            | Code::BadMessage => ErrorKind::Invalid,
            Code::NotSupported | Code::NotImplemented => ErrorKind::Unsupported,
            Code::Intr | Code::Canceled => ErrorKind::Cancelled,
            Code::TimedOut => ErrorKind::Timeout,
            Code::Again
            | Code::Busy
            | Code::NoLock
            | Code::NotConnected
            | Code::ConnRefused
            | Code::ConnReset
            | Code::ConnAborted
            | Code::BrokenPipe
            | Code::HostUnreachable
            | Code::NetUnreachable
            | Code::NetDown => ErrorKind::Unavailable,
            _ => ErrorKind::Failed,
        }
    }

    /// The snake_case name (`"not_found"`), as it appears on the wire and
    /// in the audit log.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorKind::NotFound => "not_found",
            ErrorKind::Denied => "denied",
            ErrorKind::Invalid => "invalid",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::Failed => "failed",
            ErrorKind::Cancelled => "cancelled",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::Conflict => "conflict",
            ErrorKind::Timeout => "timeout",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failed control call. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct ControlError {
    /// The portable errno, when the failure is a filesystem refusal. On the
    /// wire it is `Code`'s own `u16`.
    #[schemars(with = "Option<u16>")]
    pub code: Option<Code>,
    pub kind: ErrorKind,
    pub message: String,
    /// Structured context; free-form.
    pub details: Option<JsonValue>,
    /// What the caller can do about it.
    pub remediation: Option<String>,
}

impl ControlError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> ControlError {
        ControlError {
            code: None,
            kind,
            message: message.into(),
            details: None,
            remediation: None,
        }
    }

    pub fn not_found(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::NotFound, message).with_code(Code::NotFound)
    }

    pub fn denied(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Denied, message).with_code(Code::Access)
    }

    pub fn invalid(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Invalid, message).with_code(Code::Invalid)
    }

    pub fn unsupported(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Unsupported, message).with_code(Code::NotSupported)
    }

    pub fn failed(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Failed, message).with_code(Code::Io)
    }

    pub fn unavailable(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Unavailable, message).with_code(Code::Again)
    }

    /// A call that was cancelled: `Cancelled` / `EINTR`.
    pub fn cancelled() -> ControlError {
        ControlError::new(ErrorKind::Cancelled, "the call was cancelled").with_code(Code::Intr)
    }

    /// A frame or payload that does not follow the protocol (`EPROTO`).
    pub fn protocol(message: impl Into<String>) -> ControlError {
        ControlError::new(ErrorKind::Invalid, message).with_code(Code::Protocol)
    }

    /// Set the errno; the kind is re-derived from it.
    pub fn with_code(mut self, code: Code) -> ControlError {
        self.code = Some(code);
        self.kind = ErrorKind::of_code(code);
        self
    }

    pub fn with_details(mut self, details: serde_json::Value) -> ControlError {
        self.details = Some(JsonValue(details));
        self
    }

    pub fn with_remediation(mut self, remediation: impl Into<String>) -> ControlError {
        self.remediation = Some(remediation.into());
        self
    }

    /// The errno this error stands for: its own code, else the closest one
    /// to its kind.
    pub fn errno(&self) -> Code {
        self.code.unwrap_or(match self.kind {
            ErrorKind::NotFound => Code::NotFound,
            ErrorKind::Denied => Code::Access,
            ErrorKind::Invalid => Code::Invalid,
            ErrorKind::Unsupported => Code::NotSupported,
            ErrorKind::Failed => Code::Io,
            ErrorKind::Cancelled => Code::Intr,
            ErrorKind::Unavailable => Code::Again,
            ErrorKind::Conflict => Code::Exists,
            ErrorKind::Timeout => Code::TimedOut,
        })
    }
}

impl From<Code> for ControlError {
    fn from(code: Code) -> ControlError {
        ControlError::new(ErrorKind::of_code(code), code.message()).with_code(code)
    }
}

impl From<std::io::Error> for ControlError {
    fn from(error: std::io::Error) -> ControlError {
        let code = Code::from_io_error(&error);
        ControlError::new(ErrorKind::of_code(code), error.to_string()).with_code(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_drives_kind() {
        assert_eq!(ControlError::from(Code::NotFound).kind, ErrorKind::NotFound);
        assert_eq!(ControlError::from(Code::Access).kind, ErrorKind::Denied);
        assert_eq!(ControlError::from(Code::Intr).kind, ErrorKind::Cancelled);
        assert_eq!(ControlError::from(Code::Exists).kind, ErrorKind::Conflict);
        assert_eq!(ControlError::cancelled().code, Some(Code::Intr));
        let io = std::io::Error::from_raw_os_error(2);
        assert_eq!(ControlError::from(io).kind, ErrorKind::NotFound);
    }

    #[test]
    fn code_travels_as_its_wire_number() {
        let e = ControlError::from(Code::NotFound);
        let v = serde_json::to_value(&e).unwrap();
        assert_eq!(v["code"], serde_json::json!(1));
        assert_eq!(v["kind"], "not_found");
        let back: ControlError = serde_json::from_value(v).unwrap();
        assert_eq!(back, e);
    }
}
