use constellation_types::Code;
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

    /// Plan 30 §M14 phase 2 (the fencing token): the op was issued under
    /// a lock grant that is no longer live — its holder's window passed
    /// on this executor's clock, or the sequencer that minted it saw it
    /// released or outwaited. Nothing was journaled; the caller answers
    /// `EIO` and never replays the op.
    #[error("the lock grant the op was issued under is no longer live")]
    LockLapsed,

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

impl MetaError {
    /// The portable errno a caller answers this error with (plan 31 §7).
    /// The one mapping every refusal site shares: the authority core's
    /// holder/delegate/inbox refusals, the FUSE boundary, and recovery.
    /// Storage and codec failures (`Fjall`, `Io`, ...) are all `EIO`: the
    /// caller could not have avoided them, and they are not a property of
    /// the name it asked about.
    pub fn code(&self) -> Code {
        use MetaError::*;
        match self {
            NoEnt(_) | NoEntry => Code::NotFound,
            Exists => Code::Exists,
            NotDir => Code::NotDir,
            IsDir => Code::IsDir,
            NotEmpty => Code::NotEmpty,
            NoData => Code::NoData,
            Invalid(_) => Code::Invalid,
            Conflict => Code::Again,
            LockLapsed => Code::Io,
            Fjall(_) | Io(_) | Record(_) | Key(_) | Json(_) | Postcard(_) => Code::Io,
        }
    }
}
