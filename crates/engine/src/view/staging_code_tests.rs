use super::*;
use std::io::{Error, ErrorKind};

/// The staging filesystem's own "too large" is the one errno that
/// survives as itself: `truncate(2)` past the maximum file size is
/// `EFBIG` (pjdfstest's `truncate/12.t`/`ftruncate/12.t` accept
/// `EFBIG`, `EINVAL` or success, and saw `EIO` before this).
#[test]
fn a_too_large_staging_length_is_efbig_and_nothing_else_changes() {
    let efbig = crate::staging::StagingError::Io(Error::from(ErrorKind::FileTooLarge));
    assert_eq!(staging_code(&efbig), Code::FileTooBig);
    // Every other staging IO error keeps today's shape.
    for kind in [
        ErrorKind::PermissionDenied,
        ErrorKind::NotFound,
        ErrorKind::OutOfMemory,
        ErrorKind::Other,
    ] {
        let e = crate::staging::StagingError::Io(Error::from(kind));
        assert_eq!(staging_code(&e), Code::Io, "{kind:?}");
    }
    assert_eq!(
        staging_code(&crate::staging::StagingError::Full {
            needed: 2,
            available: 1
        }),
        Code::NoSpace
    );
}
