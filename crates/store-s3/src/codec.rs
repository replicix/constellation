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
            let out =
                zstd::decode_all(payload).map_err(|e| StoreError::Compression(e.to_string()))?;
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
}
