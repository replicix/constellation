//! [`Code`]: the portable errno (plan 31 §7).
//!
//! Every refusal Constellation makes — a holder refusing a forwarded op, a
//! journaled `Refused` outcome, a FUSE reply — is a `Code`. The type
//! exists because two of those places are durable or cross a machine
//! boundary: a refusal is journaled, shipped in S3 log segments and
//! carried in P2P replies, and a raw errno number in any of them means
//! "whatever the writer's OS calls this number", which is a different
//! error on macOS than on Linux (`ENOTEMPTY` is 39 on one and 66 on the
//! other) and meaningless on Windows. So:
//!
//! - **The wire form is `Code`'s own number** ([`Code::to_wire`]), a fixed,
//!   explicitly assigned `u16` discriminant. It is not any OS's errno, and
//!   serde (postcard in the journal and on the wire, JSON in the control
//!   API) carries exactly that number.
//! - **Native errnos exist only at a platform boundary.** A frontend turns
//!   a `Code` into its kernel's number when it answers the kernel
//!   ([`Code::to_linux_errno`], [`Code::to_darwin_errno`]), and code that
//!   interprets a real syscall's failure turns it into a `Code`
//!   ([`Code::from_io_error`]). Nothing in between ever holds an OS errno.
//!
//! ## The discriminants never change
//!
//! A discriminant, once shipped, keeps its meaning forever and is never
//! reused, even if its variant is removed: it is in journals and S3 log
//! segments. New variants take the next unused number. The `wire_numbers_
//! are_pinned` test spells every assignment out so a renumbering fails
//! loudly rather than silently re-meaning old refusals.
//!
//! A number this build does not know (a newer peer's variant) decodes to
//! [`Code::Io`] rather than failing the whole record: an unknown refusal is
//! still a refusal, and EIO is the answer POSIX callers already treat as
//! "the filesystem could not do it". The first such decode is reported on
//! stderr once per process (this crate has no logging dependency).
//!
//! ## The native tables
//!
//! Both tables are plain numbers, not `libc` constants, so both can be
//! tested on any host (the tests cross-check them against `libc` on the
//! host they run on). The Linux table is the asm-generic numbering every
//! architecture Constellation builds for uses (x86_64, aarch64, arm,
//! riscv64, and Android's bionic); MIPS, SPARC, Alpha and PA-RISC renumber
//! part of the space and are not supported. Where an OS has two names for
//! one condition the table picks one and the reverse direction accepts
//! both:
//!
//! - Linux `EWOULDBLOCK` is `EAGAIN` (11) and `ENOTSUP` is `EOPNOTSUPP`
//!   (95) — the same numbers, so nothing to alias.
//! - Darwin `ENOTSUP` (45) and `EOPNOTSUPP` (102) are distinct numbers;
//!   [`Code::NotSupported`] leaves as `ENOTSUP`, and both come back as it.
//! - Darwin reports a missing xattr as `ENOATTR` (93), never `ENODATA`
//!   (plan 34 settled decision 6), so [`Code::NoData`] leaves as 93;
//!   Darwin's STREAMS `ENODATA` (96) comes back as `NoData` too.
//!
//! Any other native number with no variant maps to [`Code::Io`].

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

/// Declares [`Code`] and its whole table from one list, so a variant, its
/// wire number and its native numbers cannot drift apart.
macro_rules! codes {
    ($(
        $(#[$doc:meta])*
        $name:ident = $wire:literal, $posix:literal, linux $linux:literal, darwin $darwin:literal,
            $msg:literal;
    )*) => {
        /// The portable errno. See the [module docs](self) for the
        /// encoding rules; the discriminant is the wire number.
        #[repr(u16)]
        #[non_exhaustive]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Code {
            $( $(#[$doc])* $name = $wire, )*
        }

        impl Code {
            /// Every variant, in wire-number order.
            pub const ALL: &'static [Code] = &[$(Code::$name,)*];

            /// The code for wire number `n`; a number this build does not
            /// know is [`Code::Io`] (reported once per process).
            pub fn from_wire(n: u16) -> Code {
                match n {
                    $($wire => Code::$name,)*
                    _ => {
                        unknown_wire(n);
                        Code::Io
                    }
                }
            }

            /// The variant's own name (`"NotEmpty"`): a bounded, stable
            /// spelling for metric labels and logs.
            pub const fn name(self) -> &'static str {
                match self {
                    $(Code::$name => stringify!($name),)*
                }
            }

            /// The POSIX symbolic name (`"ENOTEMPTY"`), for diagnostics.
            pub const fn posix_name(self) -> &'static str {
                match self {
                    $(Code::$name => $posix,)*
                }
            }

            /// A short `strerror`-style description.
            pub const fn message(self) -> &'static str {
                match self {
                    $(Code::$name => $msg,)*
                }
            }

            /// The Linux (asm-generic) errno number.
            pub const fn to_linux_errno(self) -> i32 {
                match self {
                    $(Code::$name => $linux,)*
                }
            }

            /// The code for a Linux errno; unknown numbers are
            /// [`Code::Io`]. `EWOULDBLOCK`/`ENOTSUP` share `EAGAIN`'s/
            /// `EOPNOTSUPP`'s numbers, so they need no alias here.
            pub const fn from_linux_errno(errno: i32) -> Code {
                match Code::try_from_linux_errno(errno) {
                    Some(code) => code,
                    None => Code::Io,
                }
            }

            /// The code for a Linux errno, or `None` for a number with no
            /// variant — for a caller that must tell a real `EIO` from an
            /// errno this table does not know.
            pub const fn try_from_linux_errno(errno: i32) -> Option<Code> {
                match errno {
                    $($linux => Some(Code::$name),)*
                    _ => None,
                }
            }

            /// The Darwin (macOS/iOS) errno number.
            pub const fn to_darwin_errno(self) -> i32 {
                match self {
                    $(Code::$name => $darwin,)*
                }
            }

            /// The code for a Darwin errno; unknown numbers are
            /// [`Code::Io`]. Also accepts the two Darwin aliases the forward
            /// table does not emit: `EOPNOTSUPP` (102) and `ENODATA` (96).
            pub const fn from_darwin_errno(errno: i32) -> Code {
                match Code::try_from_darwin_errno(errno) {
                    Some(code) => code,
                    None => Code::Io,
                }
            }

            /// The code for a Darwin errno, or `None` for a number with no
            /// variant (see [`Code::try_from_linux_errno`]).
            pub const fn try_from_darwin_errno(errno: i32) -> Option<Code> {
                match errno {
                    DARWIN_EOPNOTSUPP => Some(Code::NotSupported),
                    DARWIN_ENODATA => Some(Code::NoData),
                    $($darwin => Some(Code::$name),)*
                    _ => None,
                }
            }
        }
    };
}

/// Darwin's second "not supported" number; `ENOTSUP` (45) is the one the
/// table emits.
const DARWIN_EOPNOTSUPP: i32 = 102;
/// Darwin's STREAMS `ENODATA`; a missing xattr is `ENOATTR` (93) there.
const DARWIN_ENODATA: i32 = 96;

codes! {
    // --- The fixed head (plan 31 §7). ---
    /// `ENOENT`.
    NotFound = 1, "ENOENT", linux 2, darwin 2, "no such file or directory";
    /// `EEXIST`.
    Exists = 2, "EEXIST", linux 17, darwin 17, "file exists";
    /// `ENOTEMPTY`.
    NotEmpty = 3, "ENOTEMPTY", linux 39, darwin 66, "directory not empty";
    /// `ESTALE`: the handle names something that no longer exists here
    /// (also the authority core's "this op was stranded, retry it").
    Stale = 4, "ESTALE", linux 116, darwin 70, "stale file handle";
    /// `ENODATA` on Linux, `ENOATTR` on Darwin: no such xattr.
    NoData = 5, "ENODATA", linux 61, darwin 93, "no data available";
    /// `ENAMETOOLONG`.
    NameTooLong = 6, "ENAMETOOLONG", linux 36, darwin 63, "file name too long";
    /// `ENOLCK`.
    NoLock = 7, "ENOLCK", linux 37, darwin 77, "no locks available";
    /// `EOPNOTSUPP`/`ENOTSUP`.
    NotSupported = 8, "EOPNOTSUPP", linux 95, darwin 45, "operation not supported";
    /// `EAGAIN`/`EWOULDBLOCK`.
    Again = 9, "EAGAIN", linux 11, darwin 35, "resource temporarily unavailable";
    /// `EINTR`.
    Intr = 10, "EINTR", linux 4, darwin 4, "interrupted system call";
    /// `ETIMEDOUT`.
    TimedOut = 11, "ETIMEDOUT", linux 110, darwin 60, "connection timed out";
    /// `EIO`; also what an unknown wire or native number decodes to.
    Io = 12, "EIO", linux 5, darwin 5, "input/output error";
    // --- Every other errno the workspace produced at plan 31 C1. ---
    /// `ENOTDIR`.
    NotDir = 13, "ENOTDIR", linux 20, darwin 20, "not a directory";
    /// `EISDIR`.
    IsDir = 14, "EISDIR", linux 21, darwin 21, "is a directory";
    /// `EINVAL`.
    Invalid = 15, "EINVAL", linux 22, darwin 22, "invalid argument";
    /// `EROFS`.
    ReadOnly = 16, "EROFS", linux 30, darwin 30, "read-only file system";
    /// `ENOSPC`.
    NoSpace = 17, "ENOSPC", linux 28, darwin 28, "no space left on device";
    /// `EACCES`.
    Access = 18, "EACCES", linux 13, darwin 13, "permission denied";
    /// `EPERM`.
    Perm = 19, "EPERM", linux 1, darwin 1, "operation not permitted";
    /// `EXDEV`.
    CrossDevice = 20, "EXDEV", linux 18, darwin 18, "invalid cross-device link";
    /// `ENXIO`.
    NoDeviceOrAddress = 21, "ENXIO", linux 6, darwin 6, "no such device or address";
    /// `ERANGE`.
    Range = 22, "ERANGE", linux 34, darwin 34, "result too large";
    /// `ENOSYS`: the operation does not exist at all, distinct from
    /// [`Code::NotSupported`] (it exists but not for this object) — FUSE
    /// kernels treat an `ENOSYS` reply as "never ask again".
    NotImplemented = 23, "ENOSYS", linux 38, darwin 78, "function not implemented";
    /// `EFBIG`.
    FileTooBig = 24, "EFBIG", linux 27, darwin 27, "file too large";
    /// `ENOTCONN`.
    NotConnected = 25, "ENOTCONN", linux 107, darwin 57, "transport endpoint is not connected";
    /// `EBUSY`.
    Busy = 26, "EBUSY", linux 16, darwin 16, "device or resource busy";
    /// `E2BIG`.
    TooBig = 27, "E2BIG", linux 7, darwin 7, "argument list too long";
    // --- Errnos an OS I/O error can plausibly carry through. ---
    /// `EDQUOT`.
    QuotaExceeded = 28, "EDQUOT", linux 122, darwin 69, "disk quota exceeded";
    /// `ENOMEM`.
    NoMemory = 29, "ENOMEM", linux 12, darwin 12, "cannot allocate memory";
    /// `EMFILE`.
    TooManyOpenFiles = 30, "EMFILE", linux 24, darwin 24, "too many open files";
    /// `ENFILE`.
    FileTableOverflow = 31, "ENFILE", linux 23, darwin 23, "too many open files in system";
    /// `EMLINK`.
    TooManyLinks = 32, "EMLINK", linux 31, darwin 31, "too many links";
    /// `ELOOP`.
    Loop = 33, "ELOOP", linux 40, darwin 62, "too many levels of symbolic links";
    /// `EOVERFLOW`.
    Overflow = 34, "EOVERFLOW", linux 75, darwin 84, "value too large for defined data type";
    /// `EBADF`.
    BadFd = 35, "EBADF", linux 9, darwin 9, "bad file descriptor";
    /// `EDEADLK` (Linux `EDEADLOCK` is the same number).
    Deadlock = 36, "EDEADLK", linux 35, darwin 11, "resource deadlock avoided";
    /// `ETXTBSY`.
    TextBusy = 37, "ETXTBSY", linux 26, darwin 26, "text file busy";
    /// `EFAULT`.
    Fault = 38, "EFAULT", linux 14, darwin 14, "bad address";
    /// `ECANCELED`.
    Canceled = 39, "ECANCELED", linux 125, darwin 89, "operation canceled";
    /// `ESPIPE`.
    IllegalSeek = 40, "ESPIPE", linux 29, darwin 29, "illegal seek";
    /// `EPIPE`.
    BrokenPipe = 41, "EPIPE", linux 32, darwin 32, "broken pipe";
    /// `ENODEV`.
    NoDevice = 42, "ENODEV", linux 19, darwin 19, "no such device";
    /// `ENOTTY`.
    NotTty = 43, "ENOTTY", linux 25, darwin 25, "inappropriate ioctl for device";
    /// `ESRCH`.
    NoProcess = 44, "ESRCH", linux 3, darwin 3, "no such process";
    /// `EILSEQ`.
    IllegalSequence = 45, "EILSEQ", linux 84, darwin 92, "invalid or incomplete multibyte or wide character";
    /// `ENOBUFS`.
    NoBufs = 46, "ENOBUFS", linux 105, darwin 55, "no buffer space available";
    /// `ECONNREFUSED`.
    ConnRefused = 47, "ECONNREFUSED", linux 111, darwin 61, "connection refused";
    /// `ECONNRESET`.
    ConnReset = 48, "ECONNRESET", linux 104, darwin 54, "connection reset by peer";
    /// `ECONNABORTED`.
    ConnAborted = 49, "ECONNABORTED", linux 103, darwin 53, "software caused connection abort";
    /// `EHOSTUNREACH`.
    HostUnreachable = 50, "EHOSTUNREACH", linux 113, darwin 65, "no route to host";
    /// `ENETUNREACH`.
    NetUnreachable = 51, "ENETUNREACH", linux 101, darwin 51, "network is unreachable";
    /// `ENETDOWN`.
    NetDown = 52, "ENETDOWN", linux 100, darwin 50, "network is down";
    /// `EADDRINUSE`.
    AddrInUse = 53, "EADDRINUSE", linux 98, darwin 48, "address already in use";
    /// `EADDRNOTAVAIL`.
    AddrNotAvailable = 54, "EADDRNOTAVAIL", linux 99, darwin 49, "cannot assign requested address";
    /// `EINPROGRESS`.
    InProgress = 55, "EINPROGRESS", linux 115, darwin 36, "operation now in progress";
    /// `EALREADY`.
    Already = 56, "EALREADY", linux 114, darwin 37, "operation already in progress";
    /// `ENOTSOCK`.
    NotSocket = 57, "ENOTSOCK", linux 88, darwin 38, "socket operation on non-socket";
    /// `EDOM`.
    Domain = 58, "EDOM", linux 33, darwin 33, "numerical argument out of domain";
    /// `ECHILD`.
    NoChild = 59, "ECHILD", linux 10, darwin 10, "no child processes";
    /// `ENOEXEC`.
    ExecFormat = 60, "ENOEXEC", linux 8, darwin 8, "exec format error";
    /// `ENOTBLK`.
    NotBlock = 61, "ENOTBLK", linux 15, darwin 15, "block device required";
    /// `EPROTO`.
    Protocol = 62, "EPROTO", linux 71, darwin 100, "protocol error";
    /// `EBADMSG`.
    BadMessage = 63, "EBADMSG", linux 74, darwin 94, "bad message";
}

static UNKNOWN_WIRE_REPORTED: AtomicBool = AtomicBool::new(false);

#[cold]
fn unknown_wire(n: u16) {
    if !UNKNOWN_WIRE_REPORTED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "constellation-types: unknown Code wire number {n} (a newer peer's errno?); \
             treating it as EIO (reported once per process)"
        );
    }
}

impl Code {
    /// The portable wire number (the discriminant). Never an OS errno.
    pub const fn to_wire(self) -> u16 {
        self as u16
    }

    /// This host's native errno number for the code.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const fn to_native(self) -> i32 {
        self.to_linux_errno()
    }

    /// This host's native errno number for the code.
    #[cfg(target_vendor = "apple")]
    pub const fn to_native(self) -> i32 {
        self.to_darwin_errno()
    }

    /// The code for this host's native errno number; unknown numbers are
    /// [`Code::Io`].
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const fn from_native(errno: i32) -> Code {
        Code::from_linux_errno(errno)
    }

    /// The code for this host's native errno number; unknown numbers are
    /// [`Code::Io`].
    #[cfg(target_vendor = "apple")]
    pub const fn from_native(errno: i32) -> Code {
        Code::from_darwin_errno(errno)
    }

    /// The code for this host's native errno number, or `None` for a
    /// number with no variant.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const fn try_from_native(errno: i32) -> Option<Code> {
        Code::try_from_linux_errno(errno)
    }

    /// The code for this host's native errno number, or `None` for a
    /// number with no variant.
    #[cfg(target_vendor = "apple")]
    pub const fn try_from_native(errno: i32) -> Option<Code> {
        Code::try_from_darwin_errno(errno)
    }

    /// The code of a real OS failure, exactly: `None` when `error` carries
    /// no OS errno, or one with no variant. Unlike
    /// [`Code::from_io_error`], never folds anything into [`Code::Io`], so
    /// a test client asserting "the mount answered `EIO`" cannot be
    /// satisfied by an error that was not one.
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    pub fn from_os_error(error: &std::io::Error) -> Option<Code> {
        match error.raw_os_error() {
            Some(raw) => Code::try_from_native(raw),
            None => None,
        }
    }

    /// The code for a real I/O failure: its OS errno when it carries one
    /// (on a host with an errno table), else its [`std::io::ErrorKind`].
    pub fn from_io_error(error: &std::io::Error) -> Code {
        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        if let Some(raw) = error.raw_os_error() {
            return Code::from_native(raw);
        }
        Code::from_io_error_kind(error.kind())
    }

    /// The code for an [`std::io::ErrorKind`], for errors that carry no OS
    /// errno (or come from a host without an errno table). Kinds with no
    /// closer meaning are [`Code::Io`].
    pub fn from_io_error_kind(kind: std::io::ErrorKind) -> Code {
        use std::io::ErrorKind as K;
        match kind {
            K::NotFound => Code::NotFound,
            K::PermissionDenied => Code::Access,
            K::ConnectionRefused => Code::ConnRefused,
            K::ConnectionReset => Code::ConnReset,
            K::HostUnreachable => Code::HostUnreachable,
            K::NetworkUnreachable => Code::NetUnreachable,
            K::ConnectionAborted => Code::ConnAborted,
            K::NotConnected => Code::NotConnected,
            K::AddrInUse => Code::AddrInUse,
            K::AddrNotAvailable => Code::AddrNotAvailable,
            K::NetworkDown => Code::NetDown,
            K::BrokenPipe => Code::BrokenPipe,
            K::AlreadyExists => Code::Exists,
            K::WouldBlock => Code::Again,
            K::NotADirectory => Code::NotDir,
            K::IsADirectory => Code::IsDir,
            K::DirectoryNotEmpty => Code::NotEmpty,
            K::ReadOnlyFilesystem => Code::ReadOnly,
            K::StaleNetworkFileHandle => Code::Stale,
            K::InvalidInput => Code::Invalid,
            K::TimedOut => Code::TimedOut,
            K::StorageFull => Code::NoSpace,
            K::NotSeekable => Code::IllegalSeek,
            K::QuotaExceeded => Code::QuotaExceeded,
            K::FileTooLarge => Code::FileTooBig,
            K::ResourceBusy => Code::Busy,
            K::ExecutableFileBusy => Code::TextBusy,
            K::Deadlock => Code::Deadlock,
            K::CrossesDevices => Code::CrossDevice,
            K::TooManyLinks => Code::TooManyLinks,
            K::InvalidFilename => Code::NameTooLong,
            K::ArgumentListTooLong => Code::TooBig,
            K::Interrupted => Code::Intr,
            K::Unsupported => Code::NotSupported,
            K::OutOfMemory => Code::NoMemory,
            _ => Code::Io,
        }
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message(), self.posix_name())
    }
}

impl std::error::Error for Code {}

impl Serialize for Code {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u16(self.to_wire())
    }
}

impl<'de> Deserialize<'de> for Code {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u16::deserialize(deserializer).map(Code::from_wire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Every wire assignment, spelled out. Changing a line here is a
    /// wire-format break that re-means journaled refusals: add variants at
    /// the end with fresh numbers instead.
    #[test]
    fn wire_numbers_are_pinned() {
        let pinned: &[(Code, u16)] = &[
            (Code::NotFound, 1),
            (Code::Exists, 2),
            (Code::NotEmpty, 3),
            (Code::Stale, 4),
            (Code::NoData, 5),
            (Code::NameTooLong, 6),
            (Code::NoLock, 7),
            (Code::NotSupported, 8),
            (Code::Again, 9),
            (Code::Intr, 10),
            (Code::TimedOut, 11),
            (Code::Io, 12),
            (Code::NotDir, 13),
            (Code::IsDir, 14),
            (Code::Invalid, 15),
            (Code::ReadOnly, 16),
            (Code::NoSpace, 17),
            (Code::Access, 18),
            (Code::Perm, 19),
            (Code::CrossDevice, 20),
            (Code::NoDeviceOrAddress, 21),
            (Code::Range, 22),
            (Code::NotImplemented, 23),
            (Code::FileTooBig, 24),
            (Code::NotConnected, 25),
            (Code::Busy, 26),
            (Code::TooBig, 27),
            (Code::QuotaExceeded, 28),
            (Code::NoMemory, 29),
            (Code::TooManyOpenFiles, 30),
            (Code::FileTableOverflow, 31),
            (Code::TooManyLinks, 32),
            (Code::Loop, 33),
            (Code::Overflow, 34),
            (Code::BadFd, 35),
            (Code::Deadlock, 36),
            (Code::TextBusy, 37),
            (Code::Fault, 38),
            (Code::Canceled, 39),
            (Code::IllegalSeek, 40),
            (Code::BrokenPipe, 41),
            (Code::NoDevice, 42),
            (Code::NotTty, 43),
            (Code::NoProcess, 44),
            (Code::IllegalSequence, 45),
            (Code::NoBufs, 46),
            (Code::ConnRefused, 47),
            (Code::ConnReset, 48),
            (Code::ConnAborted, 49),
            (Code::HostUnreachable, 50),
            (Code::NetUnreachable, 51),
            (Code::NetDown, 52),
            (Code::AddrInUse, 53),
            (Code::AddrNotAvailable, 54),
            (Code::InProgress, 55),
            (Code::Already, 56),
            (Code::NotSocket, 57),
            (Code::Domain, 58),
            (Code::NoChild, 59),
            (Code::ExecFormat, 60),
            (Code::NotBlock, 61),
            (Code::Protocol, 62),
            (Code::BadMessage, 63),
        ];
        assert_eq!(
            pinned.len(),
            Code::ALL.len(),
            "a variant is missing from the pin list"
        );
        for &(code, wire) in pinned {
            assert_eq!(code.to_wire(), wire, "{code:?}");
        }
    }

    #[test]
    fn wire_round_trips_every_variant() {
        let mut seen = HashSet::new();
        for &code in Code::ALL {
            assert!(
                seen.insert(code.to_wire()),
                "duplicate wire number for {code:?}"
            );
            assert_eq!(Code::from_wire(code.to_wire()), code);
        }
    }

    #[test]
    fn linux_round_trips_every_variant() {
        let mut seen = HashSet::new();
        for &code in Code::ALL {
            let n = code.to_linux_errno();
            assert!(n > 0, "{code:?}");
            assert!(seen.insert(n), "two codes leave as Linux errno {n}");
            assert_eq!(Code::from_linux_errno(n), code, "{code:?} via {n}");
        }
    }

    #[test]
    fn darwin_round_trips_every_variant() {
        let mut seen = HashSet::new();
        for &code in Code::ALL {
            let n = code.to_darwin_errno();
            assert!(n > 0, "{code:?}");
            assert!(seen.insert(n), "two codes leave as Darwin errno {n}");
            assert_eq!(Code::from_darwin_errno(n), code, "{code:?} via {n}");
        }
    }

    /// Plan 31 C1's golden test: one fixed wire number, two OS numbers.
    #[test]
    fn golden_not_empty_across_oses() {
        let wire = Code::NotEmpty.to_wire();
        assert_eq!(wire, 3);
        let decoded = Code::from_wire(wire);
        assert_eq!(decoded.to_linux_errno(), 39);
        assert_eq!(decoded.to_darwin_errno(), 66);
        assert_eq!(postcard::to_allocvec(&Code::NotEmpty).unwrap(), vec![3]);
        assert_eq!(serde_json::to_string(&Code::NotEmpty).unwrap(), "3");
    }

    #[test]
    fn aliases_decode_to_the_canonical_code() {
        // Linux: the alias names share numbers (checked against libc below).
        assert_eq!(Code::from_linux_errno(11), Code::Again);
        assert_eq!(Code::from_linux_errno(95), Code::NotSupported);
        // Darwin: distinct numbers for the same condition.
        assert_eq!(Code::from_darwin_errno(45), Code::NotSupported);
        assert_eq!(Code::from_darwin_errno(102), Code::NotSupported);
        assert_eq!(Code::from_darwin_errno(93), Code::NoData);
        assert_eq!(Code::from_darwin_errno(96), Code::NoData);
    }

    #[test]
    fn unknown_numbers_are_io() {
        assert_eq!(Code::from_wire(0), Code::Io);
        assert_eq!(Code::from_wire(64), Code::Io);
        assert_eq!(Code::from_wire(u16::MAX), Code::Io);
        assert_eq!(Code::from_linux_errno(0), Code::Io);
        assert_eq!(Code::from_linux_errno(-1), Code::Io);
        assert_eq!(Code::from_linux_errno(133), Code::Io); // EHWPOISON
        assert_eq!(Code::from_darwin_errno(106), Code::Io); // EQFULL
        assert_eq!(serde_json::from_str::<Code>("4242").unwrap(), Code::Io);
        assert_eq!(Code::try_from_linux_errno(0), None);
        assert_eq!(Code::try_from_linux_errno(133), None);
        assert_eq!(Code::try_from_linux_errno(5), Some(Code::Io));
        assert_eq!(Code::try_from_darwin_errno(106), None);
        assert_eq!(Code::try_from_darwin_errno(102), Some(Code::NotSupported));
        assert_eq!(postcard::from_bytes::<Code>(&[0]).unwrap(), Code::Io);
    }

    #[test]
    fn serde_carries_the_wire_number() {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Carrier {
            before: u8,
            code: Code,
            after: u8,
        }
        for &code in Code::ALL {
            let c = Carrier {
                before: 7,
                code,
                after: 9,
            };
            let bytes = postcard::to_allocvec(&c).unwrap();
            assert_eq!(postcard::from_bytes::<Carrier>(&bytes).unwrap(), c);
            let json = serde_json::to_string(&c).unwrap();
            assert_eq!(
                json,
                format!("{{\"before\":7,\"code\":{},\"after\":9}}", code.to_wire())
            );
            assert_eq!(serde_json::from_str::<Carrier>(&json).unwrap(), c);
        }
    }

    #[test]
    fn display_names_the_errno() {
        assert_eq!(
            Code::NotEmpty.to_string(),
            "directory not empty (ENOTEMPTY)"
        );
        assert_eq!(Code::Stale.posix_name(), "ESTALE");
    }

    #[test]
    fn io_errors_map_by_errno_then_kind() {
        use std::io::{Error, ErrorKind};
        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        for &code in Code::ALL {
            let e = Error::from_raw_os_error(code.to_native());
            assert_eq!(Code::from_io_error(&e), code, "{code:?}");
        }
        assert_eq!(Code::from_io_error(&Error::other("x")), Code::Io);
        #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
        {
            assert_eq!(Code::from_os_error(&Error::other("x")), None);
            assert_eq!(Code::from_os_error(&Error::from_raw_os_error(0)), None);
            let eio = Error::from_raw_os_error(Code::Io.to_native());
            assert_eq!(Code::from_os_error(&eio), Some(Code::Io));
        }
        assert_eq!(
            Code::from_io_error(&Error::new(ErrorKind::NotFound, "x")),
            Code::NotFound
        );
        assert_eq!(
            Code::from_io_error(&ErrorKind::WouldBlock.into()),
            Code::Again
        );
        assert_eq!(
            Code::from_io_error(&ErrorKind::UnexpectedEof.into()),
            Code::Io
        );
    }

    /// The numeric Linux table against the host's libc: catches a typo in
    /// the table, and an architecture whose numbering is not asm-generic.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn linux_table_matches_libc() {
        let expect: &[(Code, i32)] = &[
            (Code::NotFound, libc::ENOENT),
            (Code::Exists, libc::EEXIST),
            (Code::NotEmpty, libc::ENOTEMPTY),
            (Code::Stale, libc::ESTALE),
            (Code::NoData, libc::ENODATA),
            (Code::NameTooLong, libc::ENAMETOOLONG),
            (Code::NoLock, libc::ENOLCK),
            (Code::NotSupported, libc::EOPNOTSUPP),
            (Code::NotSupported, libc::ENOTSUP),
            (Code::Again, libc::EAGAIN),
            (Code::Again, libc::EWOULDBLOCK),
            (Code::Intr, libc::EINTR),
            (Code::TimedOut, libc::ETIMEDOUT),
            (Code::Io, libc::EIO),
            (Code::NotDir, libc::ENOTDIR),
            (Code::IsDir, libc::EISDIR),
            (Code::Invalid, libc::EINVAL),
            (Code::ReadOnly, libc::EROFS),
            (Code::NoSpace, libc::ENOSPC),
            (Code::Access, libc::EACCES),
            (Code::Perm, libc::EPERM),
            (Code::CrossDevice, libc::EXDEV),
            (Code::NoDeviceOrAddress, libc::ENXIO),
            (Code::Range, libc::ERANGE),
            (Code::NotImplemented, libc::ENOSYS),
            (Code::FileTooBig, libc::EFBIG),
            (Code::NotConnected, libc::ENOTCONN),
            (Code::Busy, libc::EBUSY),
            (Code::TooBig, libc::E2BIG),
            (Code::QuotaExceeded, libc::EDQUOT),
            (Code::NoMemory, libc::ENOMEM),
            (Code::TooManyOpenFiles, libc::EMFILE),
            (Code::FileTableOverflow, libc::ENFILE),
            (Code::TooManyLinks, libc::EMLINK),
            (Code::Loop, libc::ELOOP),
            (Code::Overflow, libc::EOVERFLOW),
            (Code::BadFd, libc::EBADF),
            (Code::Deadlock, libc::EDEADLK),
            (Code::Deadlock, libc::EDEADLOCK),
            (Code::TextBusy, libc::ETXTBSY),
            (Code::Fault, libc::EFAULT),
            (Code::Canceled, libc::ECANCELED),
            (Code::IllegalSeek, libc::ESPIPE),
            (Code::BrokenPipe, libc::EPIPE),
            (Code::NoDevice, libc::ENODEV),
            (Code::NotTty, libc::ENOTTY),
            (Code::NoProcess, libc::ESRCH),
            (Code::IllegalSequence, libc::EILSEQ),
            (Code::NoBufs, libc::ENOBUFS),
            (Code::ConnRefused, libc::ECONNREFUSED),
            (Code::ConnReset, libc::ECONNRESET),
            (Code::ConnAborted, libc::ECONNABORTED),
            (Code::HostUnreachable, libc::EHOSTUNREACH),
            (Code::NetUnreachable, libc::ENETUNREACH),
            (Code::NetDown, libc::ENETDOWN),
            (Code::AddrInUse, libc::EADDRINUSE),
            (Code::AddrNotAvailable, libc::EADDRNOTAVAIL),
            (Code::InProgress, libc::EINPROGRESS),
            (Code::Already, libc::EALREADY),
            (Code::NotSocket, libc::ENOTSOCK),
            (Code::Domain, libc::EDOM),
            (Code::NoChild, libc::ECHILD),
            (Code::ExecFormat, libc::ENOEXEC),
            (Code::NotBlock, libc::ENOTBLK),
            (Code::Protocol, libc::EPROTO),
            (Code::BadMessage, libc::EBADMSG),
        ];
        let covered: HashSet<Code> = expect.iter().map(|&(c, _)| c).collect();
        assert_eq!(
            covered.len(),
            Code::ALL.len(),
            "a variant is missing a libc cross-check"
        );
        for &(code, errno) in expect {
            assert_eq!(Code::from_linux_errno(errno), code, "{code:?} from {errno}");
            assert_eq!(Code::from_native(errno), code, "{code:?} from {errno}");
        }
        for &code in Code::ALL {
            let errno = code.to_linux_errno();
            assert!(
                expect.contains(&(code, errno)),
                "{code:?} leaves as {errno}"
            );
        }
    }

    /// The numeric Darwin table against the host's libc, on a Darwin host.
    #[cfg(target_vendor = "apple")]
    #[test]
    fn darwin_table_matches_libc() {
        let expect: &[(Code, i32)] = &[
            (Code::NotFound, libc::ENOENT),
            (Code::Exists, libc::EEXIST),
            (Code::NotEmpty, libc::ENOTEMPTY),
            (Code::Stale, libc::ESTALE),
            (Code::NoData, libc::ENOATTR),
            (Code::NameTooLong, libc::ENAMETOOLONG),
            (Code::NoLock, libc::ENOLCK),
            (Code::NotSupported, libc::ENOTSUP),
            (Code::Again, libc::EAGAIN),
            (Code::Intr, libc::EINTR),
            (Code::TimedOut, libc::ETIMEDOUT),
            (Code::Io, libc::EIO),
            (Code::NotDir, libc::ENOTDIR),
            (Code::IsDir, libc::EISDIR),
            (Code::Invalid, libc::EINVAL),
            (Code::ReadOnly, libc::EROFS),
            (Code::NoSpace, libc::ENOSPC),
            (Code::Access, libc::EACCES),
            (Code::Perm, libc::EPERM),
            (Code::CrossDevice, libc::EXDEV),
            (Code::NoDeviceOrAddress, libc::ENXIO),
            (Code::Range, libc::ERANGE),
            (Code::NotImplemented, libc::ENOSYS),
            (Code::FileTooBig, libc::EFBIG),
            (Code::NotConnected, libc::ENOTCONN),
            (Code::Busy, libc::EBUSY),
            (Code::TooBig, libc::E2BIG),
            (Code::QuotaExceeded, libc::EDQUOT),
            (Code::NoMemory, libc::ENOMEM),
            (Code::TooManyOpenFiles, libc::EMFILE),
            (Code::FileTableOverflow, libc::ENFILE),
            (Code::TooManyLinks, libc::EMLINK),
            (Code::Loop, libc::ELOOP),
            (Code::Overflow, libc::EOVERFLOW),
            (Code::BadFd, libc::EBADF),
            (Code::Deadlock, libc::EDEADLK),
            (Code::TextBusy, libc::ETXTBSY),
            (Code::Fault, libc::EFAULT),
            (Code::Canceled, libc::ECANCELED),
            (Code::IllegalSeek, libc::ESPIPE),
            (Code::BrokenPipe, libc::EPIPE),
            (Code::NoDevice, libc::ENODEV),
            (Code::NotTty, libc::ENOTTY),
            (Code::NoProcess, libc::ESRCH),
            (Code::IllegalSequence, libc::EILSEQ),
            (Code::NoBufs, libc::ENOBUFS),
            (Code::ConnRefused, libc::ECONNREFUSED),
            (Code::ConnReset, libc::ECONNRESET),
            (Code::ConnAborted, libc::ECONNABORTED),
            (Code::HostUnreachable, libc::EHOSTUNREACH),
            (Code::NetUnreachable, libc::ENETUNREACH),
            (Code::NetDown, libc::ENETDOWN),
            (Code::AddrInUse, libc::EADDRINUSE),
            (Code::AddrNotAvailable, libc::EADDRNOTAVAIL),
            (Code::InProgress, libc::EINPROGRESS),
            (Code::Already, libc::EALREADY),
            (Code::NotSocket, libc::ENOTSOCK),
            (Code::Domain, libc::EDOM),
            (Code::NoChild, libc::ECHILD),
            (Code::ExecFormat, libc::ENOEXEC),
            (Code::NotBlock, libc::ENOTBLK),
            (Code::Protocol, libc::EPROTO),
            (Code::BadMessage, libc::EBADMSG),
        ];
        for &(code, errno) in expect {
            assert_eq!(code.to_darwin_errno(), errno, "{code:?}");
        }
        assert_eq!(
            Code::from_darwin_errno(libc::EOPNOTSUPP),
            Code::NotSupported
        );
        assert_eq!(Code::from_darwin_errno(libc::ENODATA), Code::NoData);
    }
}
