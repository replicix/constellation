//! The one error an op completes with.

use constellation_types::Code;

/// Why an op failed: a portable [`Code`] (plan 31 §7). Every frontend
/// turns it into its own platform's error at its edge (FUSE: the Linux
/// errno, `constellation-frontend-fuse`'s `reply_code`) and nowhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VfsError(Code);

impl VfsError {
    pub const fn new(code: Code) -> Self {
        Self(code)
    }

    pub const fn code(self) -> Code {
        self.0
    }
}

impl From<Code> for VfsError {
    fn from(code: Code) -> Self {
        Self(code)
    }
}

impl From<VfsError> for Code {
    fn from(error: VfsError) -> Self {
        error.0
    }
}

impl std::fmt::Display for VfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for VfsError {}

/// An op's outcome.
pub type VfsResult<T> = Result<T, VfsError>;
