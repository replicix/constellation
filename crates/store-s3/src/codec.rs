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

/// Env override for [`max_decompressed_len`].
pub const MAX_DECOMPRESSED_ENV: &str = "CONSTELLATION_MAX_DECOMPRESSED_BYTES";

/// Default for [`max_decompressed_len`]: 1 GiB.
pub const DEFAULT_MAX_DECOMPRESSED_LEN: u64 = 1 << 30;

/// Ceiling on the plaintext one stored object may decompress to (env
/// `CONSTELLATION_MAX_DECOMPRESSED_BYTES`, default 1 GiB).
///
/// This is a memory-safety bound, not a format limit. Neither a header's
/// declared length nor a frame's own content size can be trusted — a zstd
/// frame a few bytes long can expand to gigabytes — so something must cap
/// what one object can make this process allocate. But it must sit above
/// everything a legitimate producer writes, whatever its compression ratio,
/// and three of the objects decoded against it have no structural bound:
///
/// * a spilled chunk list is one chunk object of 40 bytes per data chunk
///   (a 25 TiB file at 4 MiB chunks is ~256 MiB of chunk list);
/// * `snapshot::eager_clone` journals a whole subtree as one `Clone` record,
///   and a transaction larger than `segment_max_bytes` still ships whole, so
///   one log segment grows with the cloned tree;
/// * an inbox batch is 512 ops with no byte limit, and a `SetXattr` op
///   carries its (up to 64 KiB) value inline.
///
/// So the cap is generous and configurable rather than derived from typical
/// sizes; an object past it fails with an error naming this variable, since
/// refusing one legitimate log segment stalls replay on every replica.
/// Objects with a real structural bound (pack nodes: `packs::MAX_NODE_BYTES`)
/// use [`decompress_bounded`] with that bound instead.
pub fn max_decompressed_len() -> u64 {
    static CEILING: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CEILING.get_or_init(|| {
        std::env::var(MAX_DECOMPRESSED_ENV)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_MAX_DECOMPRESSED_LEN)
    })
}

/// The error for an object past [`max_decompressed_len`]: says how to raise
/// it, so a legitimately huge object does not read as plain corruption.
pub(crate) fn ceiling_error(what: &str, len: u64) -> StoreError {
    StoreError::CorruptObject(format!(
        "{what} {len} exceeds the {}-byte decompression ceiling (set {MAX_DECOMPRESSED_ENV} \
         to raise it if the object is legitimate)",
        max_decompressed_len()
    ))
}

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
            if uncompressed_len > max_decompressed_len() {
                return Err(ceiling_error(
                    "declared decompressed length",
                    uncompressed_len,
                ));
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
/// `max_out` must already be a trusted bound: a header length the caller has
/// checked against [`max_decompressed_len`] (as [`decompress`] does), or a
/// structural limit of the format (a pack node). Objects that carry no length
/// field and have no structural bound use [`decompress_to_ceiling`]. Nothing
/// here looks at the compression ratio, so a legitimate object that happens to
/// compress extremely well (a run of zeros) decodes like any other.
pub fn decompress_bounded(payload: &[u8], max_out: u64) -> Result<Vec<u8>, StoreError> {
    let out = read_bounded(payload, max_out)?;
    if out.len() as u64 > max_out {
        return Err(StoreError::CorruptObject(format!(
            "decompressed output exceeds the {max_out}-byte cap"
        )));
    }
    Ok(out)
}

/// [`decompress_bounded`] at [`max_decompressed_len`], for objects with no
/// length header and no structural size bound (log segments, inbox batches).
pub fn decompress_to_ceiling(payload: &[u8]) -> Result<Vec<u8>, StoreError> {
    let ceiling = max_decompressed_len();
    let out = read_bounded(payload, ceiling)?;
    if out.len() as u64 > ceiling {
        return Err(ceiling_error("decompressed output of more than", ceiling));
    }
    Ok(out)
}

/// Decompress at most `max_out + 1` bytes; the caller checks for the extra one.
fn read_bounded(payload: &[u8], max_out: u64) -> Result<Vec<u8>, StoreError> {
    use std::io::Read;
    let mut decoder = zstd::stream::read::Decoder::new(payload)
        .map_err(|e| StoreError::Compression(e.to_string()))?;
    let mut out = Vec::new();
    decoder
        .by_ref()
        .take(max_out.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(|e| StoreError::Compression(e.to_string()))?;
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
        match decompress(Codec::Zstd, &payload, max_decompressed_len() + 1) {
            Err(StoreError::CorruptObject(msg)) => assert!(
                msg.contains(MAX_DECOMPRESSED_ENV),
                "the ceiling error must say how to raise it: {msg}"
            ),
            other => panic!("expected a ceiling error, got {other:?}"),
        }
        // A frame that expands to more than the (honest, in-range) header.
        assert!(matches!(
            decompress(Codec::Zstd, &payload, 4095),
            Err(StoreError::CorruptObject(_))
        ));
        assert_eq!(decompress(Codec::Zstd, &payload, 4096).unwrap().len(), 4096);
    }

    /// Bounding output is not bounding the ratio: legitimate objects that
    /// compress extremely well — the largest chunk (64 MiB) of zeros, and a
    /// headerless object well past the old per-path caps (64 MiB segments,
    /// 16 MiB inbox batches) — must still decode in full.
    #[test]
    fn highly_compressible_legitimate_objects_decode() {
        let max_chunk = 64usize << 20;
        let payload = zstd::encode_all(&vec![0u8; max_chunk][..], 3).unwrap();
        assert!(
            payload.len() < max_chunk / 1000,
            "a run of zeros compresses >1000x"
        );
        let out = decompress(Codec::Zstd, &payload, max_chunk as u64).unwrap();
        assert_eq!(out.len(), max_chunk);

        let big = (80usize << 20) + 17;
        let payload = zstd::encode_all(&vec![7u8; big][..], 3).unwrap();
        assert_eq!(decompress_to_ceiling(&payload).unwrap().len(), big);
    }
}
