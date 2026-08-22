//! Chunk identity and per-file chunk layout math (DESIGN.md §3).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Content address of a chunk: blake3 of the uncompressed plaintext.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkHash(pub [u8; 32]);

impl ChunkHash {
    pub fn of(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
            let hi = (chunk[0] as char).to_digit(16)?;
            let lo = (chunk[1] as char).to_digit(16)?;
            out[i] = (hi * 16 + lo) as u8;
        }
        Some(Self(out))
    }
}

impl fmt::Debug for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChunkHash({})", &self.to_hex()[..12])
    }
}

impl fmt::Display for ChunkHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// A byte range of a file mapped onto one chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkSlice {
    /// Index of the chunk within the file.
    pub index: u64,
    /// Offset of the range start within the chunk.
    pub offset: u32,
    /// Length of the range within this chunk.
    pub len: u32,
}

/// Per-file chunk layout: uniform chunk size, frozen at file creation
/// (recorded in the manifest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLayout {
    pub chunk_size: u32,
}

impl ChunkLayout {
    pub fn new(chunk_size: u32) -> Self {
        Self { chunk_size }
    }

    /// Number of chunks needed for a file of `file_len` bytes.
    pub fn chunk_count(&self, file_len: u64) -> u64 {
        file_len.div_ceil(self.chunk_size as u64)
    }

    /// Length of chunk `index` in a file of `file_len` bytes.
    pub fn chunk_len(&self, file_len: u64, index: u64) -> u32 {
        let cs = self.chunk_size as u64;
        let start = index * cs;
        debug_assert!(start < file_len || (file_len == 0 && index == 0));
        (file_len.saturating_sub(start)).min(cs) as u32
    }

    /// Map a byte range of the file onto the chunks it covers.
    pub fn slices(&self, offset: u64, len: u64) -> impl Iterator<Item = ChunkSlice> + '_ {
        let cs = self.chunk_size as u64;
        let end = offset + len;
        let first = offset / cs;
        let last = if len == 0 { first } else { (end - 1) / cs + 1 };
        (first..last).map(move |index| {
            let chunk_start = index * cs;
            let start = offset.max(chunk_start);
            let stop = end.min(chunk_start + cs);
            ChunkSlice {
                index,
                offset: (start - chunk_start) as u32,
                len: (stop - start) as u32,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CS: u32 = 4 * 1024 * 1024;

    #[test]
    fn hash_roundtrip() {
        let h = ChunkHash::of(b"hello");
        assert_eq!(ChunkHash::from_hex(&h.to_hex()), Some(h));
        assert_eq!(h.to_hex().len(), 64);
        assert!(ChunkHash::from_hex("zz").is_none());
    }

    #[test]
    fn chunk_counts() {
        let l = ChunkLayout::new(CS);
        assert_eq!(l.chunk_count(0), 0);
        assert_eq!(l.chunk_count(1), 1);
        assert_eq!(l.chunk_count(CS as u64), 1);
        assert_eq!(l.chunk_count(CS as u64 + 1), 2);
    }

    #[test]
    fn chunk_lens() {
        let l = ChunkLayout::new(CS);
        let file = CS as u64 * 2 + 100;
        assert_eq!(l.chunk_len(file, 0), CS);
        assert_eq!(l.chunk_len(file, 1), CS);
        assert_eq!(l.chunk_len(file, 2), 100);
    }

    #[test]
    fn slice_mapping() {
        let l = ChunkLayout::new(CS);
        // Range crossing one chunk boundary.
        let s: Vec<_> = l.slices(CS as u64 - 10, 20).collect();
        assert_eq!(
            s,
            vec![
                ChunkSlice {
                    index: 0,
                    offset: CS - 10,
                    len: 10
                },
                ChunkSlice {
                    index: 1,
                    offset: 0,
                    len: 10
                },
            ]
        );
        // Empty range.
        assert_eq!(l.slices(123, 0).count(), 0);
        // Range within one chunk.
        let s: Vec<_> = l.slices(100, 50).collect();
        assert_eq!(
            s,
            vec![ChunkSlice {
                index: 0,
                offset: 100,
                len: 50
            }]
        );
        // Exactly one full chunk.
        let s: Vec<_> = l.slices(CS as u64, CS as u64).collect();
        assert_eq!(
            s,
            vec![ChunkSlice {
                index: 1,
                offset: 0,
                len: CS
            }]
        );
    }
}
