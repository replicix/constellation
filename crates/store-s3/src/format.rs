//! Self-describing chunk object format (DESIGN.md §3).
//!
//! ```text
//! offset  size  field
//! 0       4     magic "CCH1"
//! 4       1     format version (1)
//! 5       2     codec id (LE; registry in `codec`)
//! 7       1     codec level (i8)
//! 8       8     uncompressed length (LE)
//! 16      ...   payload
//! ```

use crate::codec::{self, Codec, CompressionSetting};
use crate::error::StoreError;
use std::io::Write;

const MAGIC: &[u8; 4] = b"CCH1";
const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 16;

struct HashWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
    bytes: u64,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.hasher.update(&buf[..written]);
        self.bytes += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

enum DecoderInner<W: Write> {
    Raw(HashWriter<W>),
    Zstd(zstd::stream::write::Decoder<'static, HashWriter<W>>),
}

/// Incrementally decodes one stored chunk object into a caller-owned writer.
pub struct StreamingDecoder<W: Write> {
    inner: DecoderInner<W>,
    expected_len: u64,
}

impl<W: Write> StreamingDecoder<W> {
    pub fn new(
        header: &[u8; HEADER_LEN],
        out: W,
        hasher: blake3::Hasher,
    ) -> Result<Self, StoreError> {
        if &header[..4] != MAGIC {
            return Err(StoreError::CorruptObject("bad magic".into()));
        }
        if header[4] != VERSION {
            return Err(StoreError::CorruptObject(format!(
                "unsupported format version {}; upgrade constellation",
                header[4]
            )));
        }
        let codec = Codec::from_id(u16::from_le_bytes(header[5..7].try_into().unwrap()))?;
        let expected_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let writer = HashWriter {
            inner: out,
            hasher,
            bytes: 0,
        };
        let inner = match codec {
            Codec::Raw => DecoderInner::Raw(writer),
            Codec::Zstd => DecoderInner::Zstd(
                zstd::stream::write::Decoder::new(writer)
                    .map_err(|error| StoreError::Compression(error.to_string()))?,
            ),
        };
        Ok(Self {
            inner,
            expected_len,
        })
    }

    pub fn write_payload(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        match &mut self.inner {
            DecoderInner::Raw(writer) => writer.write_all(bytes)?,
            DecoderInner::Zstd(decoder) => decoder.write_all(bytes)?,
        }
        Ok(())
    }

    /// Finish decoding and return `(plaintext bytes, plaintext hash)`.
    pub fn finish(self) -> Result<(u64, blake3::Hash), StoreError> {
        let writer = match self.inner {
            DecoderInner::Raw(writer) => writer,
            DecoderInner::Zstd(mut decoder) => {
                decoder
                    .flush()
                    .map_err(|error| StoreError::Compression(error.to_string()))?;
                decoder.into_inner()
            }
        };
        if writer.bytes != self.expected_len {
            return Err(StoreError::CorruptObject(format!(
                "decoded length {} != header {}",
                writer.bytes, self.expected_len
            )));
        }
        Ok((writer.bytes, writer.hasher.finalize()))
    }
}

/// Encode plaintext chunk data into a stored object, applying the given
/// compression setting (with the incompressible guard).
pub fn encode_object(data: &[u8], setting: CompressionSetting) -> Result<Vec<u8>, StoreError> {
    let (codec, level, payload) = match setting.compress(data)? {
        Some(compressed) => (setting.codec, setting.level, compressed),
        None => (Codec::Raw, 0i8, data.to_vec()),
    };
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&codec.id().to_le_bytes());
    out.push(level as u8);
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a stored object back to plaintext chunk data.
pub fn decode_object(obj: &[u8]) -> Result<Vec<u8>, StoreError> {
    if obj.len() < HEADER_LEN {
        return Err(StoreError::CorruptObject("truncated header".into()));
    }
    if &obj[..4] != MAGIC {
        return Err(StoreError::CorruptObject("bad magic".into()));
    }
    if obj[4] != VERSION {
        return Err(StoreError::CorruptObject(format!(
            "unsupported format version {}; upgrade constellation",
            obj[4]
        )));
    }
    let codec = Codec::from_id(u16::from_le_bytes(obj[5..7].try_into().unwrap()))?;
    let uncompressed_len = u64::from_le_bytes(obj[8..16].try_into().unwrap());
    let payload = &obj[HEADER_LEN..];
    let data = codec::decompress(codec, payload, uncompressed_len)?;
    if codec == Codec::Raw && data.len() as u64 != uncompressed_len {
        return Err(StoreError::CorruptObject(format!(
            "raw payload length {} != header {}",
            data.len(),
            uncompressed_len
        )));
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_zstd() {
        let data = vec![7u8; 65536];
        let obj = encode_object(&data, CompressionSetting::zstd(5).unwrap()).unwrap();
        assert!(obj.len() < data.len());
        assert_eq!(decode_object(&obj).unwrap(), data);
    }

    #[test]
    fn roundtrip_raw() {
        let data = b"hello world".to_vec();
        let obj = encode_object(&data, CompressionSetting::RAW).unwrap();
        assert_eq!(obj.len(), HEADER_LEN + data.len());
        assert_eq!(decode_object(&obj).unwrap(), data);
    }

    #[test]
    fn incompressible_stored_raw() {
        let data: Vec<u8> = (0..4096u32)
            .flat_map(|i| i.wrapping_mul(2654435761).to_le_bytes())
            .collect();
        let obj = encode_object(&data, CompressionSetting::zstd(19).unwrap()).unwrap();
        // Header says raw.
        assert_eq!(
            u16::from_le_bytes(obj[5..7].try_into().unwrap()),
            crate::codec::CODEC_RAW
        );
        assert_eq!(decode_object(&obj).unwrap(), data);
    }

    #[test]
    fn corrupt_rejected() {
        assert!(decode_object(b"short").is_err());
        let data = b"payload".to_vec();
        let mut obj = encode_object(&data, CompressionSetting::RAW).unwrap();
        obj[0] = b'X';
        assert!(decode_object(&obj).is_err());
        let mut obj2 = encode_object(&data, CompressionSetting::RAW).unwrap();
        obj2[4] = 99; // future version
        assert!(decode_object(&obj2)
            .unwrap_err()
            .to_string()
            .contains("version"));
        let mut obj3 = encode_object(&data, CompressionSetting::RAW).unwrap();
        obj3[5] = 0xff; // unknown codec
        obj3[6] = 0xff;
        assert!(matches!(
            decode_object(&obj3),
            Err(StoreError::UnknownCodec(_))
        ));
    }

    #[test]
    fn streaming_decode_matches_buffered_for_raw_and_zstd() {
        for setting in [
            CompressionSetting::RAW,
            CompressionSetting::zstd(3).unwrap(),
        ] {
            let data = vec![7u8; 65_537];
            let encoded = encode_object(&data, setting).unwrap();
            let header: [u8; HEADER_LEN] = encoded[..HEADER_LEN].try_into().unwrap();
            let mut out = Vec::new();
            let mut decoder =
                StreamingDecoder::new(&header, &mut out, blake3::Hasher::new()).unwrap();
            for piece in encoded[HEADER_LEN..].chunks(257) {
                decoder.write_payload(piece).unwrap();
            }
            let (bytes, hash) = decoder.finish().unwrap();
            assert_eq!(bytes, data.len() as u64);
            assert_eq!(hash, blake3::hash(&data));
            assert_eq!(out, data);
        }
    }

    #[test]
    fn streaming_decode_rejects_truncation() {
        let data = vec![9u8; 65_537];
        let encoded = encode_object(&data, CompressionSetting::zstd(3).unwrap()).unwrap();
        let header: [u8; HEADER_LEN] = encoded[..HEADER_LEN].try_into().unwrap();
        let mut out = Vec::new();
        let mut decoder = StreamingDecoder::new(&header, &mut out, blake3::Hasher::new()).unwrap();
        decoder
            .write_payload(&encoded[HEADER_LEN..encoded.len() - 2])
            .unwrap();
        assert!(decoder.finish().is_err());
    }
}
