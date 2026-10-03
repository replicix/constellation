//! The wire protocol (plan 31 §9.3): frames, handshake, envelope.
//!
//! ```text
//!   client                               server
//!     | -- Hello {encodings, client} --->  |   (JSON)
//!     | <-- Welcome {server_version,       |   (JSON)
//!     |       principal, roles, features,  |
//!     |       encoding}                    |
//!     |                                    |
//!     | -- Request {id, method, params} -> |   (negotiated encoding)
//!     | <- Response {id, ok | err} ------- |
//!     |                                    |
//!     | -- Request (streaming method) ---> |
//!     | <- Event {sub_id=id, payload} ...  |   subscriptions
//!     | <- Chunk {id, seq, bytes, last}... |   bulk data
//!     | -- Cancel {id} ------------------> |
//!     | <- Response {id, ...} ------------ |   always exactly one
//! ```
//!
//! ## Design decisions
//!
//! - **No protocol version.** The handshake negotiates the *encoding* and
//!   *features* only (§9.3): there is one control protocol and no
//!   compatibility mode. Additive change happens through new methods and
//!   new `features` strings; a changed envelope or params type (the postcard
//!   encoding is positional, and nothing defaults) takes a new build on both
//!   peers.
//! - **Hello and Welcome are always JSON**, so an unknown or future peer
//!   can be spoken to before anything is agreed. The server picks the
//!   *client's first* encoding that it supports ([`negotiate`]).
//! - **Every call ends in exactly one `Response`.** A unary call is
//!   `Request → Response`. A streaming call is `Request → (Event|Chunk)* →
//!   Response`: the terminal `Response` carries the error that ended the
//!   stream (or [`StreamEnd`] on a clean finish, or `Cancelled` after a
//!   `Cancel`). This keeps client bookkeeping uniform: an id is live until
//!   its `Response` arrives.
//! - **A subscription's `sub_id` is its request id.** There is no separate
//!   subscribe/unsubscribe pair: `Cancel{id}` ends it.
//! - **`Chunk`s flow server→client only** in this version. Uploads go
//!   through `browse.write` with an `offset`, one bounded call per slice.
//! - **Params/results are opaque [`Blob`]s** in the envelope; see
//!   [`blob`] for why and how both encodings share one envelope type.
//!   Postcard is positional, so wire structs never use
//!   `skip_serializing_if`/`flatten`/internal tagging.

pub mod blob;
pub mod error;
pub mod frame;
pub mod types;

pub use blob::{base64_decode, base64_encode, Blob, ByteBuf, JsonValue, Secret};
pub use error::{ControlError, ErrorKind};
pub use frame::{
    decode_frame, encode_frame, encode_header, FrameError, FrameKind, RawFrame, FLAG_FD,
    MAX_FRAME_LEN,
};

use crate::authz::{Principal, Role};
use bytes::Bytes;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How payloads after the handshake are encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// Human-readable; the default for the CLI and any debugging tool.
    Json,
    /// Compact binary, for the UI and harness streams.
    Postcard,
}

impl Encoding {
    /// Serialize a whole frame payload.
    pub fn to_bytes<T: Serialize>(self, value: &T) -> Result<Bytes, ControlError> {
        match self {
            Encoding::Json => serde_json::to_vec(value)
                .map(Bytes::from)
                .map_err(|e| ControlError::failed(format!("encoding frame: {e}"))),
            Encoding::Postcard => postcard::to_allocvec(value)
                .map(Bytes::from)
                .map_err(|e| ControlError::failed(format!("encoding frame: {e}"))),
        }
    }

    /// Parse a whole frame payload. A failure is a protocol violation.
    pub fn from_bytes<T: serde::de::DeserializeOwned>(
        self,
        bytes: &[u8],
    ) -> Result<T, ControlError> {
        match self {
            Encoding::Json => serde_json::from_slice(bytes)
                .map_err(|e| ControlError::protocol(format!("malformed json frame: {e}"))),
            Encoding::Postcard => postcard::from_bytes(bytes)
                .map_err(|e| ControlError::protocol(format!("malformed postcard frame: {e}"))),
        }
    }
}

/// Every encoding this build speaks, in the order a client should list them
/// when it has no preference.
pub const SUPPORTED_ENCODINGS: [Encoding; 2] = [Encoding::Json, Encoding::Postcard];

/// The server's pick: the client's first encoding that `supported` holds.
pub fn negotiate(offered: &[Encoding], supported: &[Encoding]) -> Option<Encoding> {
    offered.iter().copied().find(|e| supported.contains(e))
}

/// Who the client says it is (diagnostics and the audit trail's context;
/// never trusted for authorization).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
}

/// Client → server, first frame. Always JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Hello {
    /// Encodings the client speaks, most preferred first.
    pub encodings: Vec<Encoding>,
    pub client: ClientInfo,
}

/// Server → client, the answer to `Hello`. Always JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Welcome {
    pub server_version: String,
    /// The identity the transport established for this connection.
    pub principal: Principal,
    /// Every role the principal holds (cumulative: an admin lists all
    /// three); empty means every method will be denied.
    pub roles: Vec<Role>,
    /// Capabilities of this connection, e.g. `"fd-passing"`.
    pub features: Vec<String>,
    /// The encoding all later payloads use.
    pub encoding: Encoding,
}

/// `Welcome::features` value: this transport can pass file descriptors.
pub const FEATURE_FD_PASSING: &str = "fd-passing";

/// One call. `params` is a [`Blob`] in the negotiated encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    /// Chosen by the client; unique among its in-flight calls, and never
    /// 0 (a `Response` with id 0 is a connection-level error; the server
    /// closes a connection that sends id 0 or reuses a live id). For a
    /// streaming method this is also the stream's `sub_id`/chunk `id`.
    pub id: u64,
    pub method: String,
    pub params: Blob,
    /// Whom the caller acts for, as the caller says (plan 37 K6a: the CSI
    /// driver names the PersistentVolume a call is about). Recorded in the
    /// audit line next to the principal, which stays the only identity
    /// authorization looks at: this is attribution a trusted service adds,
    /// never a credential. At most [`ON_BEHALF_OF_MAX`] bytes of
    /// `[A-Za-z0-9._:/@-]`; anything else is refused `Invalid`.
    pub on_behalf_of: Option<String>,
}

/// The message (`Unavailable`) of every call but `node.ping`/`fs.unlock`
/// to an engine that waits for its credentials (`constellation serve
/// --await-unlock`, plan 37 K6a), so a client can tell "unlock me first"
/// from an engine that is down.
pub const AWAITING_UNLOCK: &str = "this engine is waiting for fs.unlock to supply its credentials";

/// The longest [`Request::on_behalf_of`] a server accepts.
pub const ON_BEHALF_OF_MAX: usize = 253;

/// Whether `s` is an acceptable [`Request::on_behalf_of`]: short, and
/// nothing that could forge or break an audit or log line.
pub fn valid_on_behalf_of(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= ON_BEHALF_OF_MAX
        && s.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'/' | b'@' | b'-')
        })
}

/// The outcome half of a [`Response`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok(Blob),
    Err(ControlError),
}

/// The end of a call. Exactly one per `Request`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub result: Outcome,
}

/// One item of a subscription.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// The id of the `Request` that opened the subscription.
    pub sub_id: u64,
    pub payload: Blob,
}

/// Cancel the in-flight call `id`. Idempotent; a cancel for an id that has
/// already finished is ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cancel {
    pub id: u64,
}

/// One slice of a bulk result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// The id of the `Request` that produced it.
    pub id: u64,
    /// 0, 1, 2, … within one call.
    pub seq: u64,
    pub bytes: ByteBuf,
    /// The final chunk of a call that ended cleanly (possibly empty: a
    /// stream that paused has its data flushed as it goes). The terminal
    /// `Response` follows. A call that fails mid-stream sends no `last`
    /// chunk; its `Response` carries the error.
    pub last: bool,
}

/// The `Ok` payload of the terminal `Response` of a streaming call that
/// ended cleanly. The server fills it in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreamEnd {
    /// Events, or byte buffers, the handler produced (a large buffer may
    /// travel as several `Chunk` frames).
    pub items: u64,
    /// Payload bytes delivered (chunk streams; 0 for event streams).
    pub bytes: u64,
}

/// A parameter/result type with no fields (`{}` in JSON, zero bytes in
/// postcard).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Empty {}

/// A stream payload for methods that do not stream events.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct NoEvent {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_prefers_the_clients_first_supported() {
        use Encoding::*;
        assert_eq!(
            negotiate(&[Postcard, Json], &[Json, Postcard]),
            Some(Postcard)
        );
        assert_eq!(negotiate(&[Json, Postcard], &[Json, Postcard]), Some(Json));
        assert_eq!(negotiate(&[Postcard, Json], &[Json]), Some(Json));
        assert_eq!(negotiate(&[Postcard], &[Json]), None);
        assert_eq!(negotiate(&[], &[Json]), None);
    }

    #[test]
    fn request_is_readable_json_and_compact_postcard() {
        let req = Request {
            id: 7,
            method: "pin.add".into(),
            params: Blob::Json(serde_json::json!({"path": "/a"})),
            on_behalf_of: None,
        };
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"id":7,"method":"pin.add","params":{"path":"/a"},"on_behalf_of":null}"#
        );
        let req = Request {
            id: 7,
            method: "pin.add".into(),
            params: Blob::encode(Encoding::Postcard, &Empty {}).unwrap(),
            on_behalf_of: None,
        };
        let bytes = Encoding::Postcard.to_bytes(&req).unwrap();
        assert_eq!(
            Encoding::Postcard.from_bytes::<Request>(&bytes).unwrap(),
            req
        );
    }

    #[test]
    fn response_and_friends_round_trip_in_both_encodings() {
        for enc in SUPPORTED_ENCODINGS {
            let ok = Response {
                id: 3,
                result: Outcome::Ok(Blob::encode(enc, &StreamEnd { items: 2, bytes: 9 }).unwrap()),
            };
            let err = Response {
                id: 4,
                result: Outcome::Err(
                    ControlError::denied("no")
                        .with_details(serde_json::json!({"needs": "admin"}))
                        .with_remediation("ask an admin"),
                ),
            };
            for r in [ok, err] {
                let bytes = enc.to_bytes(&r).unwrap();
                assert_eq!(enc.from_bytes::<Response>(&bytes).unwrap(), r, "{enc:?}");
            }
            let ev = Event {
                sub_id: 1,
                payload: Blob::encode(enc, &Empty {}).unwrap(),
            };
            assert_eq!(
                enc.from_bytes::<Event>(&enc.to_bytes(&ev).unwrap())
                    .unwrap(),
                ev
            );
            let c = Cancel { id: 9 };
            assert_eq!(
                enc.from_bytes::<Cancel>(&enc.to_bytes(&c).unwrap())
                    .unwrap(),
                c
            );
            let chunk = Chunk {
                id: 1,
                seq: 0,
                bytes: vec![1, 2, 3].into(),
                last: true,
            };
            assert_eq!(
                enc.from_bytes::<Chunk>(&enc.to_bytes(&chunk).unwrap())
                    .unwrap(),
                chunk
            );
        }
    }

    #[test]
    fn hello_and_welcome_are_json() {
        let hello = Hello {
            encodings: vec![Encoding::Postcard, Encoding::Json],
            client: ClientInfo {
                name: "t".into(),
                version: "1".into(),
            },
        };
        let text = serde_json::to_string(&hello).unwrap();
        assert!(
            text.contains(r#""encodings":["postcard","json"]"#),
            "{text}"
        );
        let welcome = Welcome {
            server_version: "1".into(),
            principal: Principal::InProcess,
            roles: vec![Role::Viewer, Role::Operator, Role::Admin],
            features: vec![FEATURE_FD_PASSING.into()],
            encoding: Encoding::Json,
        };
        let back: Welcome =
            serde_json::from_str(&serde_json::to_string(&welcome).unwrap()).unwrap();
        assert_eq!(back, welcome);
    }

    #[test]
    fn garbage_payload_is_a_protocol_error() {
        let err = Encoding::Json
            .from_bytes::<Request>(b"not json")
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::Invalid);
        assert_eq!(err.code, Some(constellation_types::Code::Protocol));
        assert!(Encoding::Postcard
            .from_bytes::<Request>(&[0xff; 4])
            .is_err());
    }
}
