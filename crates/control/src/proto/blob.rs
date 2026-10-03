//! The value wrappers that make one set of serde types work under both wire
//! encodings: [`Blob`], [`JsonValue`], [`ByteBuf`] and [`Secret`].
//!
//! ## Why a `Blob`
//!
//! JSON is self-describing; postcard is not. A postcard decoder must know
//! the exact Rust type it is reading, so the envelope (which is one type for
//! every method) cannot contain a typed `params` field: the server does not
//! know which type to decode until it has read `method`. The envelope
//! therefore carries the parameters as an opaque [`Blob`]:
//!
//! - under **JSON**, a `Blob` is the inline JSON value —
//!   `{"id":7,"method":"pin.add","params":{"path":"/a"}}` — so the wire stays
//!   exactly as human-readable (and `curl`/`jq`-able) as a hand-written
//!   protocol;
//! - under **postcard**, a `Blob` is a length-prefixed byte string holding
//!   the parameters' own postcard encoding, which the typed method handler
//!   decodes once it knows the method.
//!
//! One `#[derive(Serialize, Deserialize)]` envelope struct serves both,
//! because a `Blob` picks its representation from
//! `Serializer::is_human_readable()` (true for `serde_json`, false for
//! `postcard`). The typed layer above ([`Blob::encode`]/[`Blob::decode`])
//! chooses the variant from the *negotiated* [`Encoding`].
//!
//! ## The other wrappers
//!
//! The same `is_human_readable` switch fixes the two things postcard cannot
//! do natively: [`JsonValue`] (free-form JSON needs `deserialize_any`, so
//! postcard carries it as a JSON string) and [`ByteBuf`] (raw bytes, shown as
//! base64 in JSON so a file chunk is still a legal JSON string).

use crate::proto::{ControlError, Encoding};
use bytes::Bytes;
use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use std::fmt;

/// An encoded method parameter/result/event payload. See the
/// [module docs](self).
#[derive(Debug, Clone, PartialEq)]
pub enum Blob {
    /// An inline JSON value (JSON encoding).
    Json(serde_json::Value),
    /// A postcard-encoded value (postcard encoding).
    Postcard(Vec<u8>),
}

impl Blob {
    /// Encode `value` in `encoding`.
    pub fn encode<T: Serialize>(encoding: Encoding, value: &T) -> Result<Blob, ControlError> {
        match encoding {
            Encoding::Json => serde_json::to_value(value)
                .map(Blob::Json)
                .map_err(|e| ControlError::failed(format!("encoding payload: {e}"))),
            Encoding::Postcard => postcard::to_allocvec(value)
                .map(Blob::Postcard)
                .map_err(|e| ControlError::failed(format!("encoding payload: {e}"))),
        }
    }

    /// Decode into the handler's type. A failure is the caller's fault
    /// (`Invalid`): the bytes did not match the method's schema.
    ///
    /// A JSON `null` is accepted where a struct with no required fields is
    /// expected (`jq -n '{method:"node.ping"}'` style requests omit
    /// `params` and serde fills `null`).
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, ControlError> {
        match self {
            Blob::Json(value) => match serde_json::from_value::<T>(value.clone()) {
                Ok(v) => Ok(v),
                Err(e) if value.is_null() => {
                    serde_json::from_value::<T>(serde_json::Value::Object(Default::default()))
                        .map_err(|_| ControlError::invalid(format!("invalid payload: {e}")))
                }
                Err(e) => Err(ControlError::invalid(format!("invalid payload: {e}"))),
            },
            Blob::Postcard(bytes) => postcard::from_bytes::<T>(bytes)
                .map_err(|e| ControlError::invalid(format!("invalid payload: {e}"))),
        }
    }

    /// Which encoding this blob is in.
    pub fn encoding(&self) -> Encoding {
        match self {
            Blob::Json(_) => Encoding::Json,
            Blob::Postcard(_) => Encoding::Postcard,
        }
    }

    /// The blob as JSON, when it is one.
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Blob::Json(v) => Some(v),
            Blob::Postcard(_) => None,
        }
    }

    /// The bytes the audit digest covers: for postcard, the received bytes;
    /// for JSON, a canonical rendering (object keys sorted, no whitespace)
    /// so the digest does not depend on the client's key order or the
    /// `preserve_order` feature of `serde_json` some other crate may enable.
    /// A JSON and a postcard rendering of the same logical parameters
    /// therefore digest differently; the log states which encoding it saw.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        match self {
            Blob::Postcard(bytes) => bytes.clone(),
            Blob::Json(value) => {
                let mut out = Vec::new();
                write_canonical(value, &mut out);
                out
            }
        }
    }
}

fn write_canonical(value: &serde_json::Value, out: &mut Vec<u8>) {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_string(key).unwrap_or_default().as_bytes());
                out.push(b':');
                write_canonical(&map[key], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        other => out.extend_from_slice(other.to_string().as_bytes()),
    }
}

impl Serialize for Blob {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Blob::Json(value) if serializer.is_human_readable() => value.serialize(serializer),
            Blob::Postcard(bytes) if !serializer.is_human_readable() => {
                serializer.serialize_bytes(bytes)
            }
            // A blob is created by `encode` for the negotiated encoding and
            // decoded from a frame in that encoding, so a mismatch is a
            // programming error, not a peer's.
            _ => Err(serde::ser::Error::custom(
                "Blob encoding does not match the frame encoding",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer).map(Blob::Json)
        } else {
            deserializer
                .deserialize_byte_buf(BytesVisitor)
                .map(Blob::Postcard)
        }
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a byte string")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
        Ok(v)
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
        let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0).min(1 << 20));
        while let Some(b) = seq.next_element::<u8>()? {
            out.push(b);
        }
        Ok(out)
    }
}

/// Free-form JSON that survives postcard. Inline under JSON, a JSON string
/// under postcard. Used for reports the engine will type later (gc/fsck)
/// and for [`ControlError::details`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct JsonValue(pub serde_json::Value);

impl Serialize for JsonValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            let text = serde_json::to_string(&self.0).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&text)
        }
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer).map(JsonValue)
        } else {
            let text = String::deserialize(deserializer)?;
            serde_json::from_str(&text)
                .map(JsonValue)
                .map_err(de::Error::custom)
        }
    }
}

impl JsonSchema for JsonValue {
    fn schema_name() -> Cow<'static, str> {
        "JsonValue".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        // Any JSON value.
        json_schema!({})
    }
}

impl From<serde_json::Value> for JsonValue {
    fn from(value: serde_json::Value) -> JsonValue {
        JsonValue(value)
    }
}

/// Binary data: raw bytes under postcard, a base64 string under JSON.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct ByteBuf(pub Bytes);

impl fmt::Debug for ByteBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ByteBuf({} bytes)", self.0.len())
    }
}

impl From<Vec<u8>> for ByteBuf {
    fn from(v: Vec<u8>) -> ByteBuf {
        ByteBuf(Bytes::from(v))
    }
}

impl From<Bytes> for ByteBuf {
    fn from(v: Bytes) -> ByteBuf {
        ByteBuf(v)
    }
}

impl Serialize for ByteBuf {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&base64_encode(&self.0))
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ByteBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let text = String::deserialize(deserializer)?;
            base64_decode(&text)
                .map(ByteBuf::from)
                .map_err(de::Error::custom)
        } else {
            deserializer
                .deserialize_byte_buf(BytesVisitor)
                .map(ByteBuf::from)
        }
    }
}

impl JsonSchema for ByteBuf {
    fn schema_name() -> Cow<'static, str> {
        "ByteBuf".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "contentEncoding": "base64",
            "description": "binary data (base64 under the JSON encoding, raw bytes under postcard)"
        })
    }
}

/// A secret string (passphrase, key material). Serializes as a plain
/// string; `Debug` never shows it; its bytes are wiped when it is dropped
/// (so are those of a message holding it, `UnlockCredentials` included).
/// What the codec copies on the way — a JSON encoder's output, a decoder's
/// scratch buffer for an escaped string — is the caller's to wipe.
#[derive(Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Secret {
        Secret(value.into())
    }

    /// The secret. Named so that a grep for `expose` finds every use.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for Secret {
    fn drop(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.0);
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([redacted])")
    }
}

impl JsonSchema for Secret {
    fn schema_name() -> Cow<'static, str> {
        "Secret".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({ "type": "string", "writeOnly": true })
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 with padding. Hand-rolled: the crate's dependency list
/// is deliberately short and this is 30 lines.
pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (*chunk.get(1).unwrap_or(&0) as u32) << 8
            | *chunk.get(2).unwrap_or(&0) as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// The inverse of [`base64_encode`]; rejects bad characters and bad padding.
pub fn base64_decode(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err("base64 length is not a multiple of 4".into());
    }
    let value = |c: u8| -> Result<u32, String> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a') as u32 + 26),
            b'0'..=b'9' => Ok((c - b'0') as u32 + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(format!("invalid base64 character {:?}", c as char)),
        }
    };
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let groups = bytes.len() / 4;
    for (i, quad) in bytes.chunks(4).enumerate() {
        let last = i + 1 == groups;
        let pad = quad.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return Err("bad base64 padding".into());
        }
        let mut n = 0u32;
        for &c in &quad[..4 - pad] {
            n = n << 6 | value(c)?;
        }
        n <<= 6 * pad as u32;
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_every_length() {
        for len in 0..40usize {
            let data: Vec<u8> = (0..len as u8).map(|b| b.wrapping_mul(37)).collect();
            let text = base64_encode(&data);
            assert_eq!(base64_decode(&text).unwrap(), data, "len {len}");
        }
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert!(base64_decode("TWE").is_err());
        assert!(base64_decode("T!==").is_err());
        assert!(base64_decode("=WFu").is_err());
    }

    #[test]
    fn blob_is_inline_json_and_bytes_in_postcard() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Env {
            id: u64,
            params: Blob,
        }
        let json = Env {
            id: 1,
            params: Blob::Json(serde_json::json!({"path": "/a"})),
        };
        assert_eq!(
            serde_json::to_string(&json).unwrap(),
            r#"{"id":1,"params":{"path":"/a"}}"#
        );
        let bin = Env {
            id: 1,
            params: Blob::Postcard(vec![1, 2, 3]),
        };
        let wire = postcard::to_allocvec(&bin).unwrap();
        assert_eq!(postcard::from_bytes::<Env>(&wire).unwrap(), bin);
        let jwire = serde_json::to_vec(&json).unwrap();
        assert_eq!(serde_json::from_slice::<Env>(&jwire).unwrap(), json);
        // A mismatched blob is an error, not silent garbage.
        assert!(postcard::to_allocvec(&json).is_err());
    }

    #[test]
    fn json_value_and_bytebuf_survive_postcard() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct T {
            report: JsonValue,
            data: ByteBuf,
        }
        let t = T {
            report: JsonValue(serde_json::json!({"a": [1, 2, {"b": null}]})),
            data: ByteBuf::from(vec![0, 255, 7]),
        };
        let wire = postcard::to_allocvec(&t).unwrap();
        assert_eq!(postcard::from_bytes::<T>(&wire).unwrap(), t);
        let text = serde_json::to_string(&t).unwrap();
        assert!(text.contains("\"AP8H\""), "{text}");
        assert_eq!(serde_json::from_str::<T>(&text).unwrap(), t);
    }

    #[test]
    fn canonical_digest_input_ignores_key_order() {
        let a = Blob::Json(serde_json::json!({"b": 1, "a": {"y": 1, "x": [1, 2]}}));
        let b = Blob::Json(serde_json::from_str(r#"{"a":{"x":[1,2],"y":1},"b":1}"#).unwrap());
        assert_eq!(a.canonical_bytes(), b.canonical_bytes());
    }

    #[test]
    fn secret_never_debug_prints() {
        let s = Secret::new("hunter2");
        assert!(!format!("{s:?}").contains("hunter2"));
        assert_eq!(s.expose(), "hunter2");
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"hunter2\"");
    }

    #[test]
    fn null_decodes_as_an_empty_struct() {
        #[derive(Deserialize, Debug, PartialEq)]
        struct Empty {}
        assert_eq!(
            Blob::Json(serde_json::Value::Null)
                .decode::<Empty>()
                .unwrap(),
            Empty {}
        );
        assert!(Blob::Json(serde_json::Value::Null).decode::<u32>().is_err());
    }
}
