//! File manifests: typed chunk lists with spill (DESIGN.md §3).
//!
//! Binary format (little-endian), versioned:
//!
//! ```text
//! magic "CMF1" | chunk_size u32 | file_len u64 | entry_type u8 | payload
//!   entry_type 0 (inline):   count u32, then count * 32-byte hashes
//!   entry_type 1 (manifest): 32-byte hash of a spilled chunk-list blob
//!   entry_types 2..=4 reserved: slice-overlay, pack, cdc (ADR-11)
//! ```
//!
//! A spilled blob is the inline payload (count + hashes) stored as a
//! content-addressed object in the chunk store.

use crate::chunk::{ChunkHash, ChunkLayout};
use crate::error::CoreError;

const MAGIC: &[u8; 4] = b"CMF1";

pub const ENTRY_INLINE: u8 = 0;
pub const ENTRY_MANIFEST: u8 = 1;
pub const ENTRY_SLICE_OVERLAY: u8 = 2; // reserved
pub const ENTRY_PACK: u8 = 3; // reserved
pub const ENTRY_CDC: u8 = 4; // reserved

/// Where the chunk list lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkInfo {
    /// Chunk hashes stored inline in the metadata row.
    Inline(Vec<ChunkHash>),
    /// Hash of a spilled chunk-list blob in the chunk store.
    Spilled(ChunkHash),
}

/// A file's data description: layout + chunk list (possibly spilled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub layout: ChunkLayout,
    pub file_len: u64,
    pub chunks: ChunkInfo,
}

impl Manifest {
    pub fn empty(chunk_size: u32) -> Self {
        Self {
            layout: ChunkLayout::new(chunk_size),
            file_len: 0,
            chunks: ChunkInfo::Inline(Vec::new()),
        }
    }

    /// Build a manifest from a full chunk list, spilling if it exceeds
    /// `inline_max`. Returns the manifest and, when spilled, the blob that
    /// must be stored in the chunk store under its hash.
    pub fn from_chunks(
        chunk_size: u32,
        file_len: u64,
        hashes: Vec<ChunkHash>,
        inline_max: usize,
    ) -> (Self, Option<Vec<u8>>) {
        let layout = ChunkLayout::new(chunk_size);
        debug_assert_eq!(layout.chunk_count(file_len), hashes.len() as u64);
        if hashes.len() <= inline_max {
            (
                Self {
                    layout,
                    file_len,
                    chunks: ChunkInfo::Inline(hashes),
                },
                None,
            )
        } else {
            let blob = encode_chunk_list(&hashes);
            let blob_hash = ChunkHash::of(&blob);
            (
                Self {
                    layout,
                    file_len,
                    chunks: ChunkInfo::Spilled(blob_hash),
                },
                Some(blob),
            )
        }
    }

    pub fn is_spilled(&self) -> bool {
        matches!(self.chunks, ChunkInfo::Spilled(_))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.layout.chunk_size.to_le_bytes());
        out.extend_from_slice(&self.file_len.to_le_bytes());
        match &self.chunks {
            ChunkInfo::Inline(hashes) => {
                out.push(ENTRY_INLINE);
                out.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
                for h in hashes {
                    out.extend_from_slice(&h.0);
                }
            }
            ChunkInfo::Spilled(h) => {
                out.push(ENTRY_MANIFEST);
                out.extend_from_slice(&h.0);
            }
        }
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, CoreError> {
        let err = |m: &str| CoreError::CorruptManifest(m.to_string());
        if data.len() < 17 || &data[..4] != MAGIC {
            return Err(err("bad magic or truncated header"));
        }
        let chunk_size = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let file_len = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let entry_type = data[16];
        let rest = &data[17..];
        let chunks = match entry_type {
            ENTRY_INLINE => {
                if rest.len() < 4 {
                    return Err(err("truncated inline count"));
                }
                let count = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
                let body = &rest[4..];
                if body.len() != count * 32 {
                    return Err(err("inline list length mismatch"));
                }
                ChunkInfo::Inline(decode_hashes(body))
            }
            ENTRY_MANIFEST => {
                if rest.len() != 32 {
                    return Err(err("bad spilled hash length"));
                }
                ChunkInfo::Spilled(ChunkHash(rest.try_into().unwrap()))
            }
            t @ (ENTRY_SLICE_OVERLAY | ENTRY_PACK | ENTRY_CDC) => {
                return Err(CoreError::CorruptManifest(format!(
                    "entry type {t} is reserved but unimplemented; upgrade constellation"
                )));
            }
            t => {
                return Err(CoreError::CorruptManifest(format!(
                    "unknown entry type {t}"
                )))
            }
        };
        Ok(Self {
            layout: ChunkLayout::new(chunk_size),
            file_len,
            chunks,
        })
    }
}

/// Encode a spilled chunk-list blob (count + hashes).
pub fn encode_chunk_list(hashes: &[ChunkHash]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + hashes.len() * 32);
    out.extend_from_slice(&(hashes.len() as u32).to_le_bytes());
    for h in hashes {
        out.extend_from_slice(&h.0);
    }
    out
}

/// Decode a spilled chunk-list blob.
pub fn decode_chunk_list(data: &[u8]) -> Result<Vec<ChunkHash>, CoreError> {
    if data.len() < 4 {
        return Err(CoreError::CorruptManifest(
            "truncated chunk list blob".into(),
        ));
    }
    let count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let body = &data[4..];
    if body.len() != count * 32 {
        return Err(CoreError::CorruptManifest(
            "chunk list blob length mismatch".into(),
        ));
    }
    Ok(decode_hashes(body))
}

fn decode_hashes(body: &[u8]) -> Vec<ChunkHash> {
    // Callers validate `body.len() % 32 == 0`, so the remainder is empty.
    body.as_chunks::<32>()
        .0
        .iter()
        .copied()
        .map(ChunkHash)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DEFAULT_CHUNK_SIZE, INLINE_CHUNKS_MAX};

    fn hashes(n: usize) -> Vec<ChunkHash> {
        (0..n).map(|i| ChunkHash::of(&i.to_le_bytes())).collect()
    }

    #[test]
    fn inline_roundtrip() {
        let cs = DEFAULT_CHUNK_SIZE;
        let hs = hashes(3);
        let file_len = cs as u64 * 2 + 5;
        let (m, blob) = Manifest::from_chunks(cs, file_len, hs.clone(), INLINE_CHUNKS_MAX);
        assert!(blob.is_none());
        assert!(!m.is_spilled());
        let decoded = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(decoded, m);
        assert_eq!(decoded.chunks, ChunkInfo::Inline(hs));
    }

    #[test]
    fn spill_roundtrip() {
        let cs = DEFAULT_CHUNK_SIZE;
        let n = INLINE_CHUNKS_MAX + 1;
        let hs = hashes(n);
        let file_len = cs as u64 * n as u64;
        let (m, blob) = Manifest::from_chunks(cs, file_len, hs.clone(), INLINE_CHUNKS_MAX);
        let blob = blob.expect("must spill");
        assert!(m.is_spilled());
        assert_eq!(decode_chunk_list(&blob).unwrap(), hs);
        // Blob hash matches the manifest's spilled reference.
        match Manifest::decode(&m.encode()).unwrap().chunks {
            ChunkInfo::Spilled(h) => assert_eq!(h, ChunkHash::of(&blob)),
            _ => panic!("expected spilled"),
        }
    }

    #[test]
    fn empty_manifest() {
        let m = Manifest::empty(DEFAULT_CHUNK_SIZE);
        let d = Manifest::decode(&m.encode()).unwrap();
        assert_eq!(d.file_len, 0);
        assert_eq!(d.chunks, ChunkInfo::Inline(vec![]));
    }

    #[test]
    fn reserved_types_rejected() {
        let m = Manifest::empty(DEFAULT_CHUNK_SIZE);
        let mut enc = m.encode();
        enc[16] = ENTRY_PACK;
        let e = Manifest::decode(&enc).unwrap_err();
        assert!(e.to_string().contains("reserved"));
    }

    #[test]
    fn corrupt_rejected() {
        assert!(Manifest::decode(b"nope").is_err());
        let m = Manifest::empty(DEFAULT_CHUNK_SIZE);
        let mut enc = m.encode();
        enc.truncate(10);
        assert!(Manifest::decode(&enc).is_err());
    }
}
