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

    /// An inbox batch (plan 30 §M13) written by a binary whose batch
    /// format this one does not read. Plan 30 waives compatibility, so
    /// this is a refusal, never a migration.
    #[error("inbox batch format version {0} is not supported by this binary")]
    InboxVersion(u32),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("no filesystem found at this prefix (missing meta.json)")]
    NotFound,

    #[error("meta.json: {0}")]
    Meta(String),

    #[error("compression: {0}")]
    Compression(String),

    #[error("node registry: {0}")]
    Registry(String),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("AWS credentials: {0}")]
    AwsCredentials(String),

    #[error("metadata node: {0}")]
    Node(#[from] constellation_mtree::MtreeError),

    /// A worker pool could not be built, or a blocking task panicked.
    /// Distinct from [`StoreError::Io`] because nothing about the
    /// bucket is implicated and a retry is pointless.
    #[error("parallel execution: {0}")]
    Parallel(String),
}
