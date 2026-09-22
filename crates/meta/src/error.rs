use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetaError {
    #[error("no such inode {0}")]
    NoEnt(u64),

    #[error("no such entry")]
    NoEntry,

    #[error("entry already exists")]
    Exists,

    #[error("not a directory")]
    NotDir,

    #[error("is a directory")]
    IsDir,

    #[error("directory not empty")]
    NotEmpty,

    #[error("attribute does not exist")]
    NoData,

    #[error("invalid argument: {0}")]
    Invalid(String),

    /// Optimistic concurrency failure: the caller composed its update on
    /// a base that is no longer current, so applying it would silently
    /// drop whatever landed in between. The caller must rebase and retry.
    #[error("stale base: concurrent update")]
    Conflict,

    #[error("fjall: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("record: {0}")]
    Record(#[from] constellation_mtree::record::RecordError),

    #[error("key: {0}")]
    Key(#[from] constellation_mtree::keys::KeyError),
}
