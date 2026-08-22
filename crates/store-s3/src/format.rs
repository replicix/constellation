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

const MAGIC: &[u8; 4] = b"CCH1";
const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 16;

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
}
