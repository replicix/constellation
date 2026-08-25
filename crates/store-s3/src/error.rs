use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("corrupt chunk object: {0}")]
    CorruptObject(String),

    #[error("unknown codec id {0}; upgrade constellation to read this object")]
    UnknownCodec(u16),

    #[error("chunk hash mismatch for {key}: stored object does not match its address")]
    HashMismatch { key: String },

    #[error("filesystem already exists at this prefix")]
    AlreadyExists,

    #[error("conditional write refused: the object changed since it was read")]
    CasConflict,

    #[error("no filesystem found at this prefix (missing meta.json)")]
    NotFound,

    #[error("meta.json: {0}")]
    Meta(String),

    #[error("compression: {0}")]
    Compression(String),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
