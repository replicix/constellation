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

    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("postcard: {0}")]
    Postcard(#[from] postcard::Error),
}
