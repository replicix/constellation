//! VFS core: inode/dentry model, chunking (fixed-size, blake3 plaintext
//! identity), manifests with spill, LRU chunk cache accounting.
//!
//! See docs/DESIGN.md §3 (data plane), §6 (consistency), §7 (caching).

pub mod cache;
pub mod chunk;
pub mod error;
pub mod manifest;
pub mod types;

pub use chunk::{ChunkHash, ChunkLayout, ChunkSlice};
pub use error::CoreError;
pub use manifest::{ChunkInfo, Manifest};
pub use types::{FileAttr, Ino, InodeKind};

/// Default chunk size for the data plane (DESIGN.md §3); per-FS setting.
pub const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;

/// Manifest spill threshold: chunk lists longer than this move to a
/// content-addressed manifest blob (DESIGN.md §3).
pub const INLINE_CHUNKS_MAX: usize = 8;

/// Validate a per-filesystem chunk size (power of two, 1–64 MiB).
pub fn validate_chunk_size(size: u32) -> Result<(), CoreError> {
    const MIN: u32 = 1024 * 1024;
    const MAX: u32 = 64 * 1024 * 1024;
    if !(MIN..=MAX).contains(&size) || !size.is_power_of_two() {
        return Err(CoreError::InvalidChunkSize(size));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_size_bounds() {
        assert!(validate_chunk_size(DEFAULT_CHUNK_SIZE).is_ok());
        assert!(validate_chunk_size(1024 * 1024).is_ok());
        assert!(validate_chunk_size(64 * 1024 * 1024).is_ok());
        assert!(validate_chunk_size(512 * 1024).is_err());
        assert!(validate_chunk_size(128 * 1024 * 1024).is_err());
        assert!(validate_chunk_size(3 * 1024 * 1024).is_err());
        assert!(validate_chunk_size(0).is_err());
    }
}
