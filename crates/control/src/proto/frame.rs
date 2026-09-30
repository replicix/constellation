//! The frame codec: `u32 length (BE) | u8 kind | payload`.
//!
//! - **`length`** counts everything after itself: the kind byte plus the
//!   payload, so it is never 0. It is checked against [`MAX_FRAME_LEN`]
//!   *before* any payload byte is buffered, so a peer cannot make the
//!   daemon allocate 4 GiB by announcing it. 8 MiB matches the old line
//!   protocol's `MAX_REQUEST_LINE`; bulk data (file reads, log tails) uses
//!   `Chunk` frames well below it.
//! - **`kind`** is the low bits of one byte ([`FrameKind`]); bit 7
//!   ([`FLAG_FD`]) says "a file descriptor rides with this frame". The other
//!   high bits are reserved and must be zero, so a garbage stream is
//!   rejected at its first byte instead of being mis-parsed.
//! - **`payload`** is JSON for `Hello`/`Welcome` always, and in the
//!   negotiated [`Encoding`](crate::proto::Encoding) afterwards.
//!
//! The codec here is byte-level and transport-independent: the unix socket
//! feeds it from `recvmsg`, the generic stream transport from `AsyncRead`,
//! and the in-process transport skips the length prefix and passes the
//! decoded [`RawFrame`] over a channel.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The largest `length` a frame may announce (kind byte + payload).
pub const MAX_FRAME_LEN: usize = 8 * 1024 * 1024;

/// Bit 7 of the kind byte: a file descriptor is attached to this frame.
pub const FLAG_FD: u8 = 0x80;

const KIND_MASK: u8 = 0x0f;
const RESERVED_MASK: u8 = 0x70;

/// What a frame is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FrameKind {
    Hello = 1,
    Welcome = 2,
    Request = 3,
    Response = 4,
    Event = 5,
    Cancel = 6,
    Chunk = 7,
}

impl FrameKind {
    pub const ALL: [FrameKind; 7] = [
        FrameKind::Hello,
        FrameKind::Welcome,
        FrameKind::Request,
        FrameKind::Response,
        FrameKind::Event,
        FrameKind::Cancel,
        FrameKind::Chunk,
    ];

    pub fn from_u8(n: u8) -> Option<FrameKind> {
        FrameKind::ALL.into_iter().find(|k| *k as u8 == n)
    }
}

/// A frame as it crosses a byte stream (the fd, if any, travels out of
/// band; see [`crate::transport`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub kind: FrameKind,
    /// [`FLAG_FD`] was set: the receiver must pair this frame with the next
    /// descriptor the transport received.
    pub has_fd: bool,
    pub payload: Bytes,
}

/// Why a byte stream is not a valid frame stream. All of these are fatal to
/// the connection: framing cannot resynchronize after garbage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FrameError {
    #[error("frame of {len} bytes exceeds the {max}-byte limit")]
    TooLarge { len: usize, max: usize },
    #[error("empty frame (length 0)")]
    Empty,
    #[error("unknown frame kind {0:#04x}")]
    UnknownKind(u8),
    #[error("reserved bits set in frame kind byte {0:#04x}")]
    ReservedBits(u8),
}

/// The 5-byte header for a frame with `payload_len` payload bytes.
pub fn encode_header(
    kind: FrameKind,
    has_fd: bool,
    payload_len: usize,
) -> Result<[u8; 5], FrameError> {
    let len = payload_len + 1;
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let mut header = [0u8; 5];
    header[..4].copy_from_slice(&(len as u32).to_be_bytes());
    header[4] = kind as u8 | if has_fd { FLAG_FD } else { 0 };
    Ok(header)
}

/// Append a whole frame to `out`.
pub fn encode_frame(
    kind: FrameKind,
    has_fd: bool,
    payload: &[u8],
    out: &mut BytesMut,
) -> Result<(), FrameError> {
    let header = encode_header(kind, has_fd, payload.len())?;
    out.reserve(header.len() + payload.len());
    out.put_slice(&header);
    out.put_slice(payload);
    Ok(())
}

/// Try to take one frame off the front of `buf`. `Ok(None)` means "need more
/// bytes"; the header is validated as soon as its five bytes are present, so
/// oversize and garbage frames fail before their payload arrives.
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<RawFrame>, FrameError> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if len == 0 {
        return Err(FrameError::Empty);
    }
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let kind_byte = buf[4];
    if kind_byte & RESERVED_MASK != 0 {
        return Err(FrameError::ReservedBits(kind_byte));
    }
    let kind = FrameKind::from_u8(kind_byte & KIND_MASK)
        .ok_or(FrameError::UnknownKind(kind_byte & KIND_MASK))?;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    buf.advance(5);
    let payload = buf.split_to(len - 1).freeze();
    Ok(Some(RawFrame {
        kind,
        has_fd: kind_byte & FLAG_FD != 0,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(kind: FrameKind, has_fd: bool, payload: &[u8]) -> BytesMut {
        let mut out = BytesMut::new();
        encode_frame(kind, has_fd, payload, &mut out).unwrap();
        out
    }

    #[test]
    fn every_kind_round_trips() {
        for kind in FrameKind::ALL {
            for has_fd in [false, true] {
                for payload in [&b""[..], b"x", b"hello world"] {
                    let mut wire = encode(kind, has_fd, payload);
                    let frame = decode_frame(&mut wire).unwrap().unwrap();
                    assert_eq!(frame.kind, kind);
                    assert_eq!(frame.has_fd, has_fd);
                    assert_eq!(&frame.payload[..], payload);
                    assert!(wire.is_empty());
                }
            }
        }
    }

    #[test]
    fn header_layout_is_length_kind_payload() {
        let wire = encode(FrameKind::Request, false, b"abc");
        assert_eq!(&wire[..], &[0, 0, 0, 4, 3, b'a', b'b', b'c']);
        let wire = encode(FrameKind::Chunk, true, b"");
        assert_eq!(&wire[..], &[0, 0, 0, 1, 0x87]);
    }

    #[test]
    fn partial_input_waits_and_pipelined_frames_split() {
        let mut wire = encode(FrameKind::Event, false, b"one");
        wire.extend_from_slice(&encode(FrameKind::Cancel, false, b"two"));
        let full = wire.clone();
        // Byte-at-a-time delivery yields the same two frames.
        let mut fed = BytesMut::new();
        let mut got = Vec::new();
        for byte in full.iter() {
            fed.put_u8(*byte);
            while let Some(frame) = decode_frame(&mut fed).unwrap() {
                got.push(frame);
            }
        }
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].kind, FrameKind::Event);
        assert_eq!(&got[1].payload[..], b"two");
    }

    #[test]
    fn oversize_is_rejected_before_the_payload_arrives() {
        // Announce 8 MiB + 1; no payload follows.
        let mut wire = BytesMut::new();
        wire.put_u32((MAX_FRAME_LEN + 1) as u32);
        wire.put_u8(FrameKind::Request as u8);
        assert!(matches!(
            decode_frame(&mut wire),
            Err(FrameError::TooLarge { .. })
        ));
        // Exactly the limit is allowed (and waits for its payload).
        let mut wire = BytesMut::new();
        wire.put_u32(MAX_FRAME_LEN as u32);
        wire.put_u8(FrameKind::Request as u8);
        assert_eq!(decode_frame(&mut wire), Ok(None));
        // Encoding refuses too.
        let big = vec![0u8; MAX_FRAME_LEN];
        assert!(matches!(
            encode_frame(FrameKind::Chunk, false, &big, &mut BytesMut::new()),
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[test]
    fn garbage_is_rejected() {
        let mut zero = BytesMut::from(&[0u8, 0, 0, 0, 3][..]);
        assert_eq!(decode_frame(&mut zero), Err(FrameError::Empty));
        let mut unknown = BytesMut::from(&[0u8, 0, 0, 1, 0x0e][..]);
        assert_eq!(
            decode_frame(&mut unknown),
            Err(FrameError::UnknownKind(0x0e))
        );
        let mut zero_kind = BytesMut::from(&[0u8, 0, 0, 1, 0][..]);
        assert_eq!(
            decode_frame(&mut zero_kind),
            Err(FrameError::UnknownKind(0))
        );
        let mut reserved = BytesMut::from(&[0u8, 0, 0, 1, 0x13][..]);
        assert_eq!(
            decode_frame(&mut reserved),
            Err(FrameError::ReservedBits(0x13))
        );
        // Text that was never a frame stream ("GET / HTTP/1.1").
        let mut http = BytesMut::from(&b"GET / HTTP/1.1\r\n"[..]);
        assert!(decode_frame(&mut http).is_err());
    }
}
