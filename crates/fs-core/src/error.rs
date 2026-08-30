use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("invalid chunk size {0} (must be a power of two between 1 MiB and 64 MiB)")]
    InvalidChunkSize(u32),

    #[error("corrupt manifest: {0}")]
    CorruptManifest(String),

    #[error("corrupt snapshot tree: {0}")]
    CorruptTree(String),

    #[error("chunk hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("cache budget exhausted: need {needed} bytes, {available} available")]
    CacheFull { needed: u64, available: u64 },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
