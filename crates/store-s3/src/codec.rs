//! Compression codec registry (DESIGN.md §3, ADR-10).
//!
//! Codec IDs are a stable, append-only registry: 0 = raw, 1 = zstd.
//! Adding a codec is a code change only; stored objects are
//! self-describing (see `format`), so old data never migrates.

use crate::error::StoreError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

pub const CODEC_RAW: u16 = 0;
pub const CODEC_ZSTD: u16 = 1;

/// Absolute ceiling on the plaintext produced by decompressing one stored
/// object, independent of the length the object's own header declares.
///
/// The declared length is attacker/corruption-controlled, so it cannot be
/// trusted to bound decompression on its own: a crafted zstd frame a few
/// bytes long can expand to gigabytes ("decompression bomb"). Every object
/// that legitimately flows through [`decompress`] is far smaller than this —
/// a chunk is at most 64 MiB (`fs_core::validate_chunk_size`) and a log
/// segment is a few MiB — so 256 MiB leaves generous headroom while keeping
/// the worst-case allocation bounded. Anything claiming or expanding past it
/// is rejected before the memory is committed.
pub const MAX_DECOMPRESSED_LEN: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Codec {
    Raw,
    Zstd,
}

impl Codec {
    pub fn id(self) -> u16 {
        match self {
            Codec::Raw => CODEC_RAW,
            Codec::Zstd => CODEC_ZSTD,
        }
    }

    pub fn from_id(id: u16) -> Result<Self, StoreError> {
        match id {
            CODEC_RAW => Ok(Codec::Raw),
            CODEC_ZSTD => Ok(Codec::Zstd),
            other => Err(StoreError::UnknownCodec(other)),
        }
    }
}

/// A codec + level pair, e.g. `zstd:7` or `raw` (inheritable path setting).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionSetting {
    pub codec: Codec,
    pub level: i8,
}

impl CompressionSetting {
    pub const RAW: Self = Self {
        codec: Codec::Raw,
        level: 0,
    };

    pub fn zstd(level: i8) -> Result<Self, StoreError> {
        let range = zstd::compression_level_range();
        if !range.contains(&(level as i32)) {
            return Err(StoreError::Compression(format!(
                "zstd level {level} out of range {:?}",
                range
            )));
        }
        Ok(Self {
            codec: Codec::Zstd,
            level,
        })
    }

    /// Compress `data`. Returns `None` when compression does not help
    /// (incompressible guard: caller stores raw instead).
    pub fn compress(&self, data: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        match self.codec {
            Codec::Raw => Ok(None),
            Codec::Zstd => {
                let out = zstd::encode_all(data, self.level as i32)
                    .map_err(|e| StoreError::Compression(e.to_string()))?;
                if out.len() >= data.len() {
                    Ok(None)
                } else {
                    Ok(Some(out))
                }
            }
        }
    }
}

/// Decompress a payload according to its codec header fields.
pub fn decompress(
    codec: Codec,
    payload: &[u8],
    uncompressed_len: u64,
) -> Result<Vec<u8>, StoreError> {
    match codec {
        Codec::Raw => Ok(payload.to_vec()),
        Codec::Zstd => {
            // Reject an implausible declared length before touching memory,
            // then decompress under that length as a hard cap. A well-formed
            // object expands to exactly `uncompressed_len`; the exact check
            // afterwards still catches a truncated or padded frame.
            if uncompressed_len > MAX_DECOMPRESSED_LEN {
                return Err(StoreError::CorruptObject(format!(
                    "declared decompressed length {uncompressed_len} exceeds the {MAX_DECOMPRESSED_LEN}-byte ceiling"
                )));
            }
            let out = decompress_bounded(payload, uncompressed_len)?;
            if out.len() as u64 != uncompressed_len {
                return Err(StoreError::CorruptObject(format!(
                    "decompressed length {} != header {}",
                    out.len(),
                    uncompressed_len
                )));
            }
            Ok(out)
        }
    }
}

/// Decompress a zstd `payload`, producing at most `max_out` bytes.
///
/// A crafted zstd frame a few bytes long can expand to gigabytes (a
/// "decompression bomb"), and `zstd::decode_all` would materialize all of it
/// before any length check could run — enough to exhaust memory and abort the
/// process in `handle_alloc_error`. This reads through a [`Read::take`] that
/// stops one byte past `max_out`, so an over-expansion is detected as it is
/// produced and the worst-case allocation stays bounded by `max_out`.
///
/// `max_out` must already be a trusted ceiling: either a header length that
/// the caller has validated against [`MAX_DECOMPRESSED_LEN`] (as [`decompress`]
/// does), or a format-derived constant for objects that carry no length field
/// (a log segment, an inbox batch, a pack frame). It is the single choke point
/// every untrusted-zstd reader in this crate funnels through.
pub fn decompress_bounded(payload: &[u8], max_out: u64) -> Result<Vec<u8>, StoreError> {
    use std::io::Read;
    let mut decoder =
        zstd::stream::read::Decoder::new(payload).map_err(|e| StoreError::Compression(e.to_string()))?;
    let mut out = Vec::new();
    let produced = decoder
        .by_ref()
        .take(max_out.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Compression(e.to_string()))?;
    if produced as u64 > max_out {
        return Err(StoreError::CorruptObject(format!(
            "decompressed output exceeds the {max_out}-byte cap"
        )));
    }
    Ok(out)
}

impl fmt::Display for CompressionSetting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.codec {
            Codec::Raw => f.write_str("raw"),
            Codec::Zstd => write!(f, "zstd:{}", self.level),
        }
    }
}

impl FromStr for CompressionSetting {
    type Err = StoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once(':') {
            None if s == "raw" => Ok(Self::RAW),
            None if s == "zstd" => Self::zstd(3),
            Some(("zstd", lvl)) => {
                let level: i8 = lvl
                    .parse()
                    .map_err(|_| StoreError::Compression(format!("bad zstd level {lvl:?}")))?;
                Self::zstd(level)
            }
            _ => Err(StoreError::Compression(format!(
                "unknown compression setting {s:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_settings() {
        assert_eq!(
            "raw".parse::<CompressionSetting>().unwrap(),
            CompressionSetting::RAW
        );
        let z = "zstd:7".parse::<CompressionSetting>().unwrap();
        assert_eq!(z.codec, Codec::Zstd);
        assert_eq!(z.level, 7);
        assert_eq!("zstd".parse::<CompressionSetting>().unwrap().level, 3);
        assert!("zstd:99".parse::<CompressionSetting>().is_err());
        assert!("lzma".parse::<CompressionSetting>().is_err());
    }

    #[test]
    fn compress_roundtrip() {
        let s = CompressionSetting::zstd(3).unwrap();
        let data = vec![42u8; 100_000];
        let compressed = s.compress(&data).unwrap().expect("compressible");
        assert!(compressed.len() < data.len());
        let back = decompress(Codec::Zstd, &compressed, data.len() as u64).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn incompressible_guard() {
        let s = CompressionSetting::zstd(3).unwrap();
        // Random-ish bytes: zstd output >= input -> store raw.
        let data: Vec<u8> = (0..1000u32)
            .flat_map(|i| (i.wrapping_mul(2654435761)).to_le_bytes())
            .collect();
        assert!(s.compress(&data).unwrap().is_none());
        assert!(CompressionSetting::RAW.compress(&data).unwrap().is_none());
    }

    #[test]
    fn unknown_codec_rejected() {
        assert!(matches!(
            Codec::from_id(999),
            Err(StoreError::UnknownCodec(999))
        ));
    }

    /// A tiny frame that expands past the cap must return an error rather
    /// than decompress the whole (attacker-chosen) output into memory.
    #[test]
    fn decompress_bounded_refuses_a_bomb() {
        let bomb = zstd::encode_all(&vec![0u8; 2 << 20][..], 19).unwrap();
        assert!(bomb.len() < 1 << 16, "a run of zeros compresses tiny");
        // Cap below the true output: refused.
        assert!(matches!(
            decompress_bounded(&bomb, 1 << 20),
            Err(StoreError::CorruptObject(_))
        ));
        // Cap at or above the true output: the whole thing comes back.
        let out = decompress_bounded(&bomb, 2 << 20).unwrap();
        assert_eq!(out.len(), 2 << 20);
        assert!(out.iter().all(|&b| b == 0));
    }

    /// `decompress` refuses a header length past the absolute ceiling before
    /// it decompresses anything, and enforces the exact declared length.
    #[test]
    fn decompress_enforces_ceiling_and_exact_length() {
        let payload = zstd::encode_all(&vec![1u8; 4096][..], 3).unwrap();
        assert!(matches!(
            decompress(Codec::Zstd, &payload, MAX_DECOMPRESSED_LEN + 1),
            Err(StoreError::CorruptObject(_))
        ));
        // A frame that expands to more than the (honest, in-range) header.
        assert!(matches!(
            decompress(Codec::Zstd, &payload, 4095),
            Err(StoreError::CorruptObject(_))
        ));
        assert_eq!(decompress(Codec::Zstd, &payload, 4096).unwrap().len(), 4096);
    }
}
