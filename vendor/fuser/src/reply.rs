//! Filesystem operation reply
//!
//! A reply is passed to filesystem operation implementations and must be used to send back the
//! result of an operation. The reply can optionally be sent to another thread to asynchronously
//! work on an operation and provide the result later. Also it allows replying with a block of
//! data without cloning the data. A reply *must always* be used (by calling either `ok()` or
//! `error()` exactly once).

use std::convert::AsRef;
use std::ffi::OsStr;
use std::io::IoSlice;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::time::Duration;
#[cfg(target_os = "macos")]
use std::time::SystemTime;

use log::debug;
use log::error;
use log::warn;

use crate::Errno;
use crate::FileAttr;
use crate::FileType;
use crate::PollEvents;
use crate::channel::ChannelSender;
use crate::ll::Generation;
use crate::ll::INodeNo;
use crate::ll::flags::fopen_flags::FopenFlags;
use crate::ll::reply::DirEntList;
use crate::ll::reply::DirEntOffset;
use crate::ll::reply::DirEntPlusList;
use crate::ll::reply::DirEntry;
use crate::ll::reply::DirEntryPlus;
use crate::ll::reply::Response;
use crate::ll::{self};
use crate::passthrough::BackingId;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::uring::ring::RingCommit;

/// Generic reply callback to send data
#[derive(Debug, Clone)]
pub(crate) enum ReplySender {
    Channel(ChannelSender),
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    Ring(RingCommit),
    #[cfg(test)]
    Assert(AssertSender),
    #[cfg(test)]
    Sync(std::sync::mpsc::SyncSender<()>),
    /// Hands every reply's bytes to the test, which can also see that none was sent.
    #[cfg(test)]
    Capture(std::sync::mpsc::Sender<Vec<u8>>),
}

impl ReplySender {
    /// Send data.
    pub(crate) fn send(&self, data: &[IoSlice<'_>]) -> std::io::Result<()> {
        match self {
            ReplySender::Channel(sender) => sender.send(data),
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ReplySender::Ring(commit) => commit.commit(data),
            #[cfg(test)]
            ReplySender::Assert(sender) => sender.send(data),
            #[cfg(test)]
            ReplySender::Sync(sender) => {
                sender.send(()).unwrap();
                Ok(())
            }
            #[cfg(test)]
            ReplySender::Capture(sender) => {
                let bytes = data.iter().flat_map(|s| s.iter().copied()).collect();
                sender.send(bytes).unwrap();
                Ok(())
            }
        }
    }

    /// The ring writes into the entry's own buffer when it can; otherwise a heap buffer is
    /// filled and sent. Exactly one reply is sent whatever `f` does.
    pub(crate) fn fill<F>(&self, unique: ll::RequestId, max_len: usize, f: F) -> std::io::Result<()>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        self.fill_with(unique, max_len, true, f)
    }

    /// CONSTELLATION PATCH (io-uring): `fill`, with the ring's buffer handed to `f` unzeroed
    /// when `zero` is false (`RingCommit::fill_with`); a heap buffer is always zeroed.
    fn fill_with<F>(
        &self,
        unique: ll::RequestId,
        max_len: usize,
        zero: bool,
        f: F,
    ) -> std::io::Result<()>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        let f = match self {
            ReplySender::Ring(commit) => match commit.fill_with(max_len, zero, f)? {
                None => return Ok(()),
                Some(f) => f,
            },
            _ => f,
        };
        // Armed until the reply is out, so a panic while framing the count still answers EIO
        let guard = EioOnUnwind {
            sender: self,
            unique,
        };
        let mut buf = vec![0u8; max_len];
        let res = match f(&mut buf) {
            Ok(n) if n <= max_len && n <= MAX_PAYLOAD => {
                ll::ResponseSlice(&buf[..n]).with_iovec(unique, |iov| self.send(iov))
            }
            Ok(n) => {
                error!(
                    "reply for request {} claims {n} bytes in a {max_len} byte buffer; replying \
                     EINVAL",
                    unique.0
                );
                ll::ResponseErrno(Errno::EINVAL).with_iovec(unique, |iov| self.send(iov))
            }
            Err(errno) => ll::ResponseErrno(errno).with_iovec(unique, |iov| self.send(iov)),
        };
        std::mem::forget(guard);
        res
    }

    /// CONSTELLATION PATCH (io-uring): `fill`'s gather form. Over a ring
    /// the segments are written straight into the entry's payload, one
    /// copy and no syscall; over `/dev/fuse` they go out as one
    /// `writev(2)` with no copy at all, which is strictly less work than
    /// joining them into a buffer and calling `data()`.
    pub(crate) fn gather<B: AsRef<[u8]>>(
        &self,
        unique: ll::RequestId,
        segments: &[B],
    ) -> std::io::Result<()> {
        let total = segments
            .iter()
            .try_fold(0usize, |sum, s| sum.checked_add(s.as_ref().len()))
            .ok_or_else(|| std::io::Error::other("gathered reply length overflows"))?;
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if matches!(self, ReplySender::Ring(_)) {
            // Every byte of the buffer is overwritten, so it is not zeroed first
            return self.fill_with(unique, total, false, |buf| {
                let mut at = 0;
                for segment in segments {
                    let segment = segment.as_ref();
                    buf[at..at + segment.len()].copy_from_slice(segment);
                    at += segment.len();
                }
                Ok(at)
            });
        }
        let _ = total;
        let slices: smallvec::SmallVec<[IoSlice<'_>; 4]> =
            segments.iter().map(|s| IoSlice::new(s.as_ref())).collect();
        ll::ResponseSegments(slices.as_slice()).with_iovec(unique, |iov| self.send(iov))
    }

    /// CONSTELLATION PATCH (io-uring): the transport this reply goes out over.
    pub(crate) fn transport(&self) -> crate::Transport {
        match self {
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ReplySender::Ring(commit) => commit.transport(),
            _ => crate::Transport::DevFuse,
        }
    }

    /// CONSTELLATION PATCH (io-uring): whether the request's pages are registered for
    /// `READ_FIXED` (plan 38 Z4); see [`ReplyData::zero_copy`].
    pub(crate) fn zero_copied(&self) -> bool {
        match self {
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ReplySender::Ring(commit) => commit.zero_copied(),
            _ => false,
        }
    }

    /// CONSTELLATION PATCH (io-uring): a reply of `len` bytes of `src` from `offset`; see
    /// [`ReplyData::read_fixed`]. Over `/dev/fuse` (and over a ring whose request payload is
    /// still borrowed) it is `fill` with a `pread(2)` into the buffer.
    pub(crate) fn read_fixed(
        &self,
        unique: ll::RequestId,
        src: Box<dyn AsFd + Send>,
        offset: u64,
        len: usize,
    ) -> std::io::Result<()> {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        let src = match self {
            ReplySender::Ring(commit) => match commit.read_fixed(src, offset, len)? {
                None => return Ok(()),
                Some(src) => src,
            },
            _ => src,
        };
        self.fill_with(unique, len, false, |buf| {
            pread_full(src.as_fd(), buf, offset).map_err(Errno::from)
        })
    }

    /// Records that a reply object was created for the request, so a transport that answers
    /// unreplied requests itself knows one is coming.
    pub(crate) fn reply_created(&self) {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if let ReplySender::Ring(commit) = self {
            commit.reply_created();
        }
    }

    /// Open a backing file
    pub(crate) fn open_backing(&self, fd: BorrowedFd<'_>) -> std::io::Result<BackingId> {
        match self {
            ReplySender::Channel(sender) => sender.open_backing(fd),
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ReplySender::Ring(commit) => BackingId::create(commit.device(), fd),
            #[cfg(test)]
            ReplySender::Assert(_) | ReplySender::Sync(_) | ReplySender::Capture(_) => {
                unreachable!()
            }
        }
    }

    /// Wraps a raw backing file ID
    pub(crate) unsafe fn wrap_backing(&self, id: u32) -> BackingId {
        match self {
            ReplySender::Channel(sender) => unsafe { sender.wrap_backing(id) },
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ReplySender::Ring(commit) => unsafe { BackingId::wrap_raw(commit.device(), id) },
            #[cfg(test)]
            ReplySender::Assert(_) | ReplySender::Sync(_) | ReplySender::Capture(_) => {
                unreachable!()
            }
        }
    }
}

/// The largest payload `fuse_out_header.len` can describe.
const MAX_PAYLOAD: usize = u32::MAX as usize - size_of::<ll::fuse_abi::fuse_out_header>();

/// CONSTELLATION PATCH (io-uring): `pread(2)` until `buf` is full or the file ends; the count
/// read. Short only at the end of the file, as a read reply must be.
pub(crate) fn pread_full(
    fd: BorrowedFd<'_>,
    buf: &mut [u8],
    offset: u64,
) -> std::io::Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        let at = offset
            .checked_add(done as u64)
            .and_then(|at| libc::off_t::try_from(at).ok())
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
        match nix::sys::uio::pread(fd, &mut buf[done..], at) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(nix::errno::Errno::EINTR) => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(done)
}

/// Answers `EIO` if a `fill` closure unwinds, so the request gets its one reply; disarmed with
/// `mem::forget` once the reply is sent.
struct EioOnUnwind<'a> {
    sender: &'a ReplySender,
    unique: ll::RequestId,
}

impl Drop for EioOnUnwind<'_> {
    fn drop(&mut self) {
        warn!(
            "Reply closure for operation {} panicked, replying with I/O error",
            self.unique.0
        );
        let res =
            ll::ResponseErrno(Errno::EIO).with_iovec(self.unique, |iov| self.sender.send(iov));
        log_send(res);
    }
}

/// Logs a failed send; a reply after the connection ended is expected during teardown.
fn log_send(res: std::io::Result<()>) {
    match res {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotConnected => {
            debug!("Failed to send FUSE reply: {err}");
        }
        Err(err) => error!("Failed to send FUSE reply: {err}"),
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct AssertSender {
    expected: Vec<u8>,
}

#[cfg(test)]
impl AssertSender {
    fn send(&self, data: &[IoSlice<'_>]) -> std::io::Result<()> {
        let mut v = vec![];
        for x in data {
            v.extend_from_slice(x);
        }
        assert_eq!(self.expected, v);
        Ok(())
    }
}

/// Generic reply trait
pub(crate) trait Reply: Send + 'static {
    /// Create a new reply for the given request
    fn new(unique: ll::RequestId, sender: ReplySender) -> Self;
}

///
/// Raw reply
///
#[derive(Debug)]
pub(crate) struct ReplyRaw {
    /// Unique id of the request to reply to
    unique: ll::RequestId,
    /// Closure to call for sending the reply
    sender: Option<ReplySender>,
}

impl Reply for ReplyRaw {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyRaw {
        ReplyRaw {
            unique,
            sender: Some(sender),
        }
    }
}

impl ReplyRaw {
    /// CONSTELLATION PATCH (io-uring): see [`ReplyData::transport`].
    pub(crate) fn transport(&self) -> crate::Transport {
        self.sender
            .as_ref()
            .map_or(crate::Transport::DevFuse, ReplySender::transport)
    }

    /// CONSTELLATION PATCH (io-uring): see [`ReplyData::zero_copy`].
    pub(crate) fn zero_copied(&self) -> bool {
        self.sender.as_ref().is_some_and(ReplySender::zero_copied)
    }

    /// CONSTELLATION PATCH (io-uring): see [`ReplyData::read_fixed`].
    pub(crate) fn send_read_fixed(mut self, src: Box<dyn AsFd + Send>, offset: u64, len: usize) {
        assert!(self.sender.is_some());
        let sender = self.sender.take().unwrap();
        log_send(sender.read_fixed(self.unique, src, offset, len));
    }

    /// Reply to a request with the given error code and data. Must be called
    /// only once (the `ok` and `error` methods ensure this by consuming `self`)
    pub(crate) fn send_ll_mut(&mut self, response: &impl Response) {
        assert!(self.sender.is_some());
        let sender = self.sender.take().unwrap();
        let res = response.with_iovec(self.unique, |iov| sender.send(iov));
        log_send(res);
    }
    pub(crate) fn send_ll(mut self, response: &impl Response) {
        self.send_ll_mut(response);
    }

    /// CONSTELLATION PATCH (io-uring): `send_fill`'s gather form; see
    /// [`ReplySender::gather`].
    pub(crate) fn send_gather<B: AsRef<[u8]>>(mut self, segments: &[B]) {
        assert!(self.sender.is_some());
        let sender = self.sender.take().unwrap();
        log_send(sender.gather(self.unique, segments));
    }

    /// The sender answers a panic in `f` itself, so it is taken out first and `Drop` has
    /// nothing left to do.
    pub(crate) fn send_fill<F>(mut self, max_len: usize, f: F)
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        assert!(self.sender.is_some());
        let sender = self.sender.take().unwrap();
        log_send(sender.fill(self.unique, max_len, f));
    }

    /// Reply to a request with the given error code
    pub(crate) fn error(self, err: ll::Errno) {
        self.send_ll(&ll::ResponseErrno(err));
    }
}

impl Drop for ReplyRaw {
    fn drop(&mut self) {
        if self.sender.is_some() {
            warn!(
                "Reply not sent for operation {}, replying with I/O error",
                self.unique.0
            );
            self.send_ll_mut(&ll::ResponseErrno(ll::Errno::EIO));
        }
    }
}

///
/// Empty reply
///
#[derive(Debug)]
pub struct ReplyEmpty {
    reply: ReplyRaw,
}

impl Reply for ReplyEmpty {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyEmpty {
        ReplyEmpty {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEmpty {
    /// Reply to a request with nothing
    pub fn ok(self) {
        self.reply.send_ll(&ll::ResponseEmpty);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Data reply
///
#[derive(Debug)]
pub struct ReplyData {
    reply: ReplyRaw,
}

impl Reply for ReplyData {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyData {
        ReplyData {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyData {
    /// Reply to a request with the given data
    pub fn data(self, data: &[u8]) {
        self.reply.send_ll(&ll::ResponseSlice(data));
    }

    /// Reply to a request with data produced directly into the transport's reply buffer.
    ///
    /// `f` receives a zeroed buffer of exactly `max_len` bytes and returns how many bytes it
    /// filled, which is the length of the reply, or an error to reply with instead. Pass the
    /// `size` of the request as `max_len`: the kernel never asks for more than the negotiated
    /// `max_write`, which every transport can hold, whereas a `max_len` the transport cannot
    /// hold is a caller bug answered with `EINVAL` without running `f`. `f` may also be
    /// skipped once the connection has ended, when nothing is sent, as with `data()`. When `f`
    /// does run the request gets exactly one reply: the bytes it reports, its error, `EINVAL`
    /// for a count larger than `max_len` (or than a reply can carry), or `EIO` if it panics.
    ///
    /// Over io_uring the buffer is normally the ring entry's own, so the data is not copied
    /// again on its way to the kernel; over `/dev/fuse` it is a heap buffer sent with
    /// `writev(2)`, and the call behaves like `data()`.
    ///
    /// # Examples
    ///
    /// Serving a `read` request from a file without an intermediate buffer. A short count
    /// means end of file to the kernel, which is right for a regular file; a source that may
    /// return short reads before its end should loop until the buffer is full:
    ///
    /// ```
    /// use std::fs::File;
    /// use std::os::unix::fs::FileExt;
    ///
    /// use fuser::Errno;
    /// use fuser::ReplyData;
    ///
    /// fn read(file: &File, offset: u64, size: u32, reply: ReplyData) {
    ///     reply.fill(size as usize, |buf| {
    ///         file.read_at(buf, offset).map_err(Errno::from)
    ///     });
    /// }
    /// ```
    pub fn fill<F>(self, max_len: usize, f: F)
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        self.reply.send_fill(max_len, f);
    }

    /// CONSTELLATION PATCH (io-uring): reply with data that is already
    /// several borrowed segments, without joining them first.
    ///
    /// The reply is the concatenation of `segments`, in order. Over the
    /// io_uring transport they are written straight into the ring entry's
    /// payload buffer (one copy, no syscall); over `/dev/fuse` they go out
    /// as one `writev(2)`, with no copy at all — strictly less work than
    /// concatenating them and calling [`Self::data`].
    ///
    /// This is the read path of a filesystem whose bytes live in a list of
    /// reference-counted buffers rather than one slice (Constellation's
    /// `ReadData`, a `SmallVec<[Bytes; 4]>`). Up to four segments are
    /// handled without allocating.
    ///
    /// # Examples
    ///
    /// ```
    /// use fuser::ReplyData;
    ///
    /// fn read(chunks: &[Vec<u8>], reply: ReplyData) {
    ///     reply.gather(chunks);
    /// }
    /// ```
    pub fn gather<B: AsRef<[u8]>>(self, segments: &[B]) {
        self.reply.send_gather(segments);
    }

    /// CONSTELLATION PATCH (io-uring): the transport this reply goes out over --
    /// [`crate::Transport::Uring`] when the request came over an io_uring entry, whose payload
    /// buffer [`Self::fill`] and [`Self::gather`] write in place,
    /// [`crate::Transport::UringZeroCopy`] when the session's queues are all zero-copy queues
    /// (plan 38 Z4; whether *this* request was zero-copied is [`Self::zero_copy`]), and
    /// [`crate::Transport::DevFuse`] otherwise.
    pub fn transport(&self) -> crate::Transport {
        self.reply.transport()
    }

    /// CONSTELLATION PATCH (io-uring): whether this request's pages are registered for a
    /// `READ_FIXED`: a read of a file opened with [`ReplyOpen::opened_zero_copy`] that the
    /// kernel queued on a zero-copy queue (plan 38 Z4). [`Self::read_fixed`] then moves the
    /// data from the file to the reader with no copy by this process; any other reply to it
    /// is bounced through a memfd into those pages, one copy more than over a ring without
    /// zero-copy. `false` over `/dev/fuse` and on any other request.
    pub fn zero_copy(&self) -> bool {
        self.reply.zero_copied()
    }

    /// CONSTELLATION PATCH (io-uring): reply with `len` bytes of the file `src`, from `offset`
    /// (plan 38 §3(d), Z4).
    ///
    /// On a zero-copied request ([`Self::zero_copy`]) the ring's thread issues one
    /// `IORING_OP_READ_FIXED` from `src` into the pages the kernel registered for the request
    /// -- the reader's page cache, or its own buffer under `O_DIRECT` -- and commits the reply
    /// once the read completed: the bytes it read, fewer at the end of the file, or its error.
    /// No byte passes through this process. Anywhere else the bytes are read with `pread(2)`
    /// into the transport's reply buffer, as [`Self::fill`] would be with a closure doing the
    /// same: the ring entry's payload buffer, or a heap buffer sent with `writev(2)` over
    /// `/dev/fuse`. Either way the request gets exactly one reply, and `src` stays open (it is
    /// owned here) until the read is done, on whatever thread finishes it.
    ///
    /// Callable from any thread, like [`Self::fill`]. Pass the request's `size` as `len`, at
    /// most what `max_write` allows. A `len` the reply buffer cannot hold is answered `EINVAL`
    /// where the bytes go through one; on a zero-copied request a `len` past the request's
    /// registered pages is the kernel's to refuse, and the reply is `READ_FIXED`'s error,
    /// `EFAULT`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fs::File;
    /// use std::sync::Arc;
    ///
    /// use fuser::ReplyData;
    ///
    /// fn read(file: &Arc<File>, offset: u64, size: u32, reply: ReplyData) {
    ///     reply.read_fixed(Arc::clone(file), offset, size as usize);
    /// }
    /// ```
    pub fn read_fixed(self, src: impl AsFd + Send + 'static, offset: u64, len: usize) {
        self.reply.send_read_fixed(Box::new(src), offset, len);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Entry reply
///
#[derive(Debug)]
pub struct ReplyEntry {
    reply: ReplyRaw,
}

impl Reply for ReplyEntry {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyEntry {
        ReplyEntry {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyEntry {
    /// Reply to a request with the given entry
    pub fn entry(self, ttl: &Duration, attr: &FileAttr, generation: Generation) {
        self.reply.send_ll(&ll::ResponseStruct::new_entry(
            attr.ino,
            generation,
            &attr.into(),
            *ttl,
            *ttl,
        ));
    }

    /// Reply to a request with the given entry.
    ///
    /// Use this function instead of `ReplyEntry::entry` if the
    /// entry TTL needs to be different from the attribute TTL.
    ///
    /// * `attr_ttl` - Time-to-live for the file attributes. Controls how long the
    ///   kernel will cache inode metadata (e.g., size, permissions, timestamps)
    ///   before issuing a new `GETATTR` request.
    /// * `entry_ttl` - Time-to-live for the directory entry (name-to-inode mapping).
    ///   Controls how long the kernel will cache the existence of this
    ///   path without issuing a new `LOOKUP` request.
    pub fn entry_with_ttls(
        self,
        attr_ttl: &Duration,
        entry_ttl: &Duration,
        attr: &FileAttr,
        generation: Generation,
    ) {
        self.reply.send_ll(&ll::ResponseStruct::new_entry(
            attr.ino,
            generation,
            &attr.into(),
            *attr_ttl,
            *entry_ttl,
        ));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Attribute Reply
///
#[derive(Debug)]
pub struct ReplyAttr {
    reply: ReplyRaw,
}

impl Reply for ReplyAttr {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyAttr {
        ReplyAttr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyAttr {
    /// Reply to a request with the given attribute
    pub fn attr(self, ttl: &Duration, attr: &FileAttr) {
        self.reply
            .send_ll(&ll::ResponseStruct::new_attr(ttl, &attr.into()));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// XTimes Reply
///
#[cfg(target_os = "macos")]
#[derive(Debug)]
pub struct ReplyXTimes {
    reply: ReplyRaw,
}

#[cfg(target_os = "macos")]
impl Reply for ReplyXTimes {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyXTimes {
        ReplyXTimes {
            reply: Reply::new(unique, sender),
        }
    }
}

#[cfg(target_os = "macos")]
impl ReplyXTimes {
    /// Reply to a request with the given xtimes
    pub fn xtimes(self, bkuptime: SystemTime, crtime: SystemTime) {
        self.reply
            .send_ll(&ll::ResponseStruct::new_xtimes(bkuptime, crtime))
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Open Reply
///
#[derive(Debug)]
pub struct ReplyOpen {
    reply: ReplyRaw,
}

impl Reply for ReplyOpen {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyOpen {
        ReplyOpen {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyOpen {
    /// Reply to a request with the given open result
    /// # Panics
    /// When attempting to use kernel passthrough.
    /// Use [`opened_passthrough()`](Self::opened_passthrough) instead.
    pub fn opened(self, fh: ll::FileHandle, flags: FopenFlags) {
        assert!(!flags.contains(FopenFlags::FOPEN_PASSTHROUGH));
        self.reply
            .send_ll(&ll::ResponseStruct::new_open(fh, flags, 0));
    }

    /// Registers a fd for passthrough, returning a `BackingId`.  Once you have the backing ID,
    /// you can pass it as the 3rd parameter of [`ReplyOpen::opened_passthrough()`]. This is done in
    /// two separate steps because you must reuse backing IDs for the same inode for all open file handles.
    pub fn open_backing(&self, fd: impl std::os::fd::AsFd) -> std::io::Result<BackingId> {
        // TODO: assert passthrough capability is enabled.
        self.reply.sender.as_ref().unwrap().open_backing(fd.as_fd())
    }

    /// Wraps a raw FUSE `backing_id` value, returning a `BackingId`. Once you have the backing ID,
    /// you can pass it as the 3rd parameter of [`ReplyOpen::opened_passthrough()`]. This is done in
    /// two separate steps because you must reuse backing IDs for the same inode for all open file handles.
    ///
    /// This function takes ownership of the given backing ID value, automatically closing it once
    /// the returned `BackingId` is dropped. You may reobtain ownership of the backing ID by calling
    /// [`BackingId::into_raw()`].
    ///
    /// # Safety
    ///
    /// The given ID must be open and belong to this FUSE session, and may not be closed while the
    /// returned `BackingId` instance is still live.
    pub unsafe fn wrap_backing(&self, id: u32) -> BackingId {
        // TODO: assert passthrough capability is enabled.
        unsafe { self.reply.sender.as_ref().unwrap().wrap_backing(id) }
    }

    /// CONSTELLATION PATCH (io-uring): reply to an open with `FOPEN_IO_URING_ZERO_COPY` set
    /// (plan 38 Z4): the kernel registers the pages of every read of this open that it queues
    /// on a zero-copy io_uring queue ([`crate::Transport::UringZeroCopy`]) for the reply to fill
    /// with [`ReplyData::read_fixed`]. Elsewhere -- `/dev/fuse`, a ring without zero-copy --
    /// the flag is ignored and the open behaves as with [`Self::opened`].
    ///
    /// Only for opens that cannot write (`O_RDONLY`): the kernel zero-copies the writes of
    /// such an open too, and this crate cannot hand a zero-copied write's data to
    /// [`crate::Filesystem::write`] -- it answers one `EIO`.
    ///
    /// # Panics
    /// When `flags` asks for kernel passthrough.
    pub fn opened_zero_copy(self, fh: ll::FileHandle, flags: FopenFlags) {
        assert!(!flags.contains(FopenFlags::FOPEN_PASSTHROUGH));
        let flags = flags | FopenFlags::FOPEN_IO_URING_ZERO_COPY;
        self.reply
            .send_ll(&ll::ResponseStruct::new_open(fh, flags, 0));
    }

    /// Reply to a request with an opened backing id. Call [`ReplyOpen::open_backing()`]
    /// to get one of these.
    ///
    /// Note that you must reuse the given `BackingId` for all future [`ReplyOpen::opened_passthrough()`]
    /// invocations as long as the passed file handle stays open!
    pub fn opened_passthrough(self, fh: ll::FileHandle, flags: FopenFlags, backing_id: &BackingId) {
        // TODO: assert passthrough capability is enabled.
        let flags = flags | FopenFlags::FOPEN_PASSTHROUGH;
        self.reply.send_ll(&ll::ResponseStruct::new_open(
            fh,
            flags,
            backing_id.backing_id,
        ));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Write Reply
///
#[derive(Debug)]
pub struct ReplyWrite {
    reply: ReplyRaw,
}

impl Reply for ReplyWrite {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyWrite {
        ReplyWrite {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyWrite {
    /// Reply to a request with the number of bytes written
    pub fn written(self, size: u32) {
        self.reply.send_ll(&ll::ResponseStruct::new_write(size));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Statfs Reply
///
#[derive(Debug)]
pub struct ReplyStatfs {
    reply: ReplyRaw,
}

impl Reply for ReplyStatfs {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyStatfs {
        ReplyStatfs {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyStatfs {
    /// Reply to a statfs request with filesystem information
    #[allow(clippy::too_many_arguments)]
    pub fn statfs(
        self,
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
    ) {
        self.reply.send_ll(&ll::ResponseStruct::new_statfs(
            blocks, bfree, bavail, files, ffree, bsize, namelen, frsize,
        ));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Create reply
///
#[derive(Debug)]
pub struct ReplyCreate {
    reply: ReplyRaw,
}

impl Reply for ReplyCreate {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyCreate {
        ReplyCreate {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyCreate {
    /// Reply to a request with a newly created file entry and its newly open file handle
    /// # Panics
    /// When attempting to use kernel passthrough. Use `opened_passthrough()` instead.
    pub fn created(
        self,
        ttl: &Duration,
        attr: &FileAttr,
        generation: Generation,
        fh: ll::FileHandle,
        flags: FopenFlags,
    ) {
        assert!(!flags.contains(FopenFlags::FOPEN_PASSTHROUGH));
        self.reply.send_ll(&ll::ResponseStruct::new_create(
            ttl,
            &attr.into(),
            generation,
            fh,
            flags,
            0,
        ));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }

    /// Registers a fd for passthrough, returning a `BackingId`.  Once you have the backing ID,
    /// you can pass it as the 6th parameter of `ReplyCreate::created_passthrough()`.  This is done in
    /// two separate steps because you must reuse backing IDs for the same inode for all open file handles.
    pub fn open_backing(&self, fd: impl std::os::fd::AsFd) -> std::io::Result<BackingId> {
        self.reply.sender.as_ref().unwrap().open_backing(fd.as_fd())
    }

    /// Wraps a raw FUSE `backing_id` value, returning a `BackingId`. Once you have the backing ID,
    /// you can pass it as the 3rd parameter of [`ReplyCreate::created_passthrough()`]. This is done in
    /// two separate steps because you must reuse backing IDs for the same inode for all open file handles.
    ///
    /// This function takes ownership of the given backing ID value, automatically closing it once
    /// the returned `BackingId` is dropped. You may reobtain ownership of the backing ID by calling
    /// [`BackingId::into_raw()`].
    ///
    /// # Safety
    ///
    /// The given ID must be open and belong to this FUSE session, and may not be closed while the
    /// returned `BackingId` instance is still live.
    pub unsafe fn wrap_backing(&self, id: u32) -> BackingId {
        // TODO: assert passthrough capability is enabled.
        unsafe { self.reply.sender.as_ref().unwrap().wrap_backing(id) }
    }

    /// Reply to a request with an opened backing id. Call [`ReplyCreate::open_backing()`] to get one of
    /// these.
    ///
    /// Note that you must reuse the given `BackingId` for all future [`ReplyOpen::opened_passthrough()`]
    /// invocations as long as the passed file handle stays open!
    pub fn created_passthrough(
        self,
        ttl: &Duration,
        attr: &FileAttr,
        generation: Generation,
        fh: ll::FileHandle,
        flags: FopenFlags,
        backing_id: &BackingId,
    ) {
        self.reply.send_ll(&ll::ResponseStruct::new_create(
            ttl,
            &attr.into(),
            generation,
            fh,
            flags | FopenFlags::FOPEN_PASSTHROUGH,
            backing_id.backing_id,
        ));
    }
}

///
/// Lock Reply
///
#[derive(Debug)]
pub struct ReplyLock {
    reply: ReplyRaw,
}

impl Reply for ReplyLock {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyLock {
        ReplyLock {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyLock {
    /// Reply to a request with a file lock
    pub fn locked(self, start: u64, end: u64, typ: i32, pid: u32) {
        self.reply.send_ll(&ll::ResponseStruct::new_lock(&ll::Lock {
            range: (start, end),
            typ,
            pid,
        }));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Bmap Reply
///
#[derive(Debug)]
pub struct ReplyBmap {
    reply: ReplyRaw,
}

impl Reply for ReplyBmap {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyBmap {
        ReplyBmap {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyBmap {
    /// Reply to a request with a bmap
    pub fn bmap(self, block: u64) {
        self.reply.send_ll(&ll::ResponseStruct::new_bmap(block));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Ioctl Reply
///
#[derive(Debug)]
pub struct ReplyIoctl {
    reply: ReplyRaw,
}

impl Reply for ReplyIoctl {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyIoctl {
        ReplyIoctl {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyIoctl {
    /// Reply to a request with an ioctl
    pub fn ioctl(self, result: i32, data: &[u8]) {
        self.reply
            .send_ll(&ll::ResponseIoctl::new_ioctl(result, &[IoSlice::new(data)]));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Poll Reply
///
#[derive(Debug)]
pub struct ReplyPoll {
    reply: ReplyRaw,
}

impl Reply for ReplyPoll {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyPoll {
        ReplyPoll {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyPoll {
    /// Reply to a request with ready poll events
    pub fn poll(self, revents: PollEvents) {
        self.reply.send_ll(&ll::ResponseStruct::new_poll(revents));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Directory reply
///
#[derive(Debug)]
pub struct ReplyDirectory {
    reply: ReplyRaw,
    data: DirEntList,
}

impl ReplyDirectory {
    /// Creates a new `ReplyDirectory` with a specified buffer size.
    pub(crate) fn new(unique: ll::RequestId, sender: ReplySender, size: usize) -> ReplyDirectory {
        ReplyDirectory {
            reply: Reply::new(unique, sender),
            data: DirEntList::new(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    #[must_use]
    pub fn add<T: AsRef<OsStr>>(
        &mut self,
        ino: INodeNo,
        offset: u64,
        kind: FileType,
        name: T,
    ) -> bool {
        let name = name.as_ref();
        self.data
            .push(&DirEntry::new(ino, DirEntOffset(offset), kind, name))
    }

    /// Reply to a request with the filled directory buffer
    pub fn ok(self) {
        let response: ll::ResponseData = self.data.into();
        self.reply.send_ll(&response);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// `DirectoryPlus` reply
///
#[derive(Debug)]
pub struct ReplyDirectoryPlus {
    reply: ReplyRaw,
    buf: DirEntPlusList,
}

impl ReplyDirectoryPlus {
    /// Creates a new `ReplyDirectory` with a specified buffer size.
    pub(crate) fn new(
        unique: ll::RequestId,
        sender: ReplySender,
        size: usize,
    ) -> ReplyDirectoryPlus {
        ReplyDirectoryPlus {
            reply: Reply::new(unique, sender),
            buf: DirEntPlusList::new(size),
        }
    }

    /// Add an entry to the directory reply buffer. Returns true if the buffer is full.
    /// A transparent offset value can be provided for each entry. The kernel uses these
    /// value to request the next entries in further readdir calls
    pub fn add<T: AsRef<OsStr>>(
        &mut self,
        ino: INodeNo,
        offset: u64,
        name: T,
        ttl: &Duration,
        attr: &FileAttr,
        generation: Generation,
    ) -> bool {
        let name = name.as_ref();
        self.buf.push(&DirEntryPlus::new(
            ino,
            generation,
            DirEntOffset(offset),
            name,
            *ttl,
            attr.into(),
            *ttl,
        ))
    }

    /// Reply to a request with the filled directory buffer
    pub fn ok(self) {
        let response: ll::ResponseData = self.buf.into();
        self.reply.send_ll(&response);
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Xattr reply
///
#[derive(Debug)]
pub struct ReplyXattr {
    reply: ReplyRaw,
}

impl Reply for ReplyXattr {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyXattr {
        ReplyXattr {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyXattr {
    /// Reply to a request with the size of an extended attribute
    pub fn size(self, size: u32) {
        self.reply
            .send_ll(&ll::ResponseStruct::new_xattr_size(size));
    }

    /// Reply to a request with the data of an extended attribute
    pub fn data(self, data: &[u8]) {
        self.reply.send_ll(&ll::ResponseSlice(data));
    }

    /// Reply to a request with the given error code.
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

///
/// Lseek Reply
///
#[derive(Debug)]
pub struct ReplyLseek {
    reply: ReplyRaw,
}

impl Reply for ReplyLseek {
    fn new(unique: ll::RequestId, sender: ReplySender) -> ReplyLseek {
        ReplyLseek {
            reply: Reply::new(unique, sender),
        }
    }
}

impl ReplyLseek {
    /// Reply to a request with seeked offset
    pub fn offset(self, offset: i64) {
        self.reply.send_ll(&ll::ResponseStruct::new_lseek(offset));
    }

    /// Reply to a request with the given error code
    pub fn error(self, err: Errno) {
        self.reply.error(err);
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::Duration;
    use std::time::UNIX_EPOCH;

    use zerocopy::Immutable;
    use zerocopy::IntoBytes;

    use crate::FileAttr;
    use crate::FileType;
    use crate::reply::*;

    #[derive(Debug, IntoBytes, Immutable)]
    #[repr(C)]
    struct Data {
        a: u8,
        b: u8,
        c: u16,
    }

    #[test]
    fn serialize_empty() {
        assert!(().as_bytes().is_empty());
    }

    #[test]
    fn serialize_slice() {
        let data: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
        assert_eq!(data.as_bytes(), [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn serialize_struct() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        assert_eq!(data.as_bytes(), [0x12, 0x34, 0x78, 0x56]);
    }

    #[test]
    fn reply_raw() {
        let data = Data {
            a: 0x12,
            b: 0x34,
            c: 0x5678,
        };
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x12, 0x34, 0x78, 0x56,
            ],
        });
        let reply: ReplyRaw = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.send_ll(&ll::ResponseData::new_data(data.as_bytes()));
    }

    #[test]
    fn reply_error() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x10, 0x00, 0x00, 0x00, 0xbe, 0xff, 0xff, 0xff, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ],
        });
        let reply: ReplyRaw = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.error(Errno::from_i32(66));
    }

    #[test]
    fn reply_empty() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00,
            ],
        });
        let reply: ReplyEmpty = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.ok();
    }

    #[test]
    fn reply_data() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0xde, 0xad, 0xbe, 0xef,
            ],
        });
        let reply: ReplyData = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.data(&[0xde, 0xad, 0xbe, 0xef]);
    }

    /// Runs `fill` against a `Capture` sender and returns the one reply it sent
    fn filled<F>(max_len: usize, f: F) -> Vec<u8>
    where
        F: FnOnce(&mut [u8]) -> Result<usize, Errno>,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        let reply: ReplyData = Reply::new(ll::RequestId(0xdeadbeef), ReplySender::Capture(tx));
        reply.fill(max_len, f);
        let sent = rx.try_recv().expect("no reply was sent");
        assert!(rx.try_recv().is_err(), "a second reply was sent");
        sent
    }

    /// CONSTELLATION PATCH (io-uring)
    fn gathered<B: AsRef<[u8]>>(segments: &[B]) -> Vec<u8> {
        let (tx, rx) = std::sync::mpsc::channel();
        let reply: ReplyData = Reply::new(ll::RequestId(0xdeadbeef), ReplySender::Capture(tx));
        reply.gather(segments);
        let sent = rx.try_recv().expect("no reply was sent");
        assert!(rx.try_recv().is_err(), "a second reply was sent");
        sent
    }

    fn data_reply(payload: &[u8]) -> Vec<u8> {
        let mut expected = vec![
            0x10 + payload.len() as u8,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
        ];
        expected.extend_from_slice(&[0xef, 0xbe, 0xad, 0xde, 0x00, 0x00, 0x00, 0x00]);
        expected.extend_from_slice(payload);
        expected
    }

    fn errno_reply(errno: i32) -> Vec<u8> {
        let mut expected = vec![0x10, 0x00, 0x00, 0x00];
        expected.extend_from_slice(&(-errno).to_ne_bytes());
        expected.extend_from_slice(&[0xef, 0xbe, 0xad, 0xde, 0x00, 0x00, 0x00, 0x00]);
        expected
    }

    /// The same wire bytes as `data`, from a closure that fills the whole zeroed buffer
    #[test]
    fn reply_fill() {
        let sent = filled(4, |buf| {
            assert_eq!(buf, [0, 0, 0, 0]);
            buf.copy_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
            Ok(buf.len())
        });
        assert_eq!(sent, data_reply(&[0xde, 0xad, 0xbe, 0xef]));
    }

    /// Bytes the closure reported but did not write go out as zeros
    #[test]
    fn reply_fill_sends_the_count() {
        let sent = filled(8, |buf| {
            assert_eq!(buf.len(), 8);
            buf[..2].copy_from_slice(&[0xde, 0xad]);
            buf[2..].copy_from_slice(&[0xff; 6]);
            Ok(2)
        });
        assert_eq!(sent, data_reply(&[0xde, 0xad]));
        let sent = filled(4, |buf| {
            buf[0] = 1;
            Ok(4)
        });
        assert_eq!(sent, data_reply(&[1, 0, 0, 0]));
    }

    #[test]
    fn reply_fill_rejects_a_count_beyond_the_buffer() {
        let sent = filled(4, |buf| {
            buf.fill(0xaa);
            Ok(5)
        });
        assert_eq!(sent, errno_reply(libc::EINVAL));
    }

    #[test]
    fn reply_fill_error() {
        assert_eq!(filled(4, |_| Err(Errno::ENOENT)), errno_reply(libc::ENOENT));
    }

    #[test]
    fn reply_fill_panic_replies_once() {
        let (tx, rx) = std::sync::mpsc::channel();
        let reply: ReplyData = Reply::new(ll::RequestId(0xdeadbeef), ReplySender::Capture(tx));
        let result = thread::spawn(move || {
            reply.fill(4, |_| -> Result<usize, Errno> {
                panic!("deliberate panic in fill")
            });
        })
        .join();
        assert!(result.is_err());
        assert_eq!(rx.try_recv().unwrap(), errno_reply(libc::EIO));
        assert!(rx.try_recv().is_err(), "a second reply was sent");
    }

    #[test]
    fn reply_entry() {
        let mut expected = if cfg!(target_os = "macos") {
            vec![
                0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56,
                0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00, 0x00, 0x00,
                0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x99, 0x00, 0x00, 0x00,
            ]
        } else {
            vec![
                0x88, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00,
                0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
            ]
        };

        expected.extend(vec![0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        expected[0] = (expected.len()) as u8;

        let sender = ReplySender::Assert(AssertSender { expected });
        let reply: ReplyEntry = Reply::new(ll::RequestId(0xdeadbeef), sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: INodeNo(0x11),
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
            blksize: 0xbb,
        };
        reply.entry(&ttl, &attr, ll::Generation(0xaa));
    }

    #[test]
    fn reply_attr() {
        let mut expected = if cfg!(target_os = "macos") {
            vec![
                0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56,
                0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00,
                0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x99, 0x00,
                0x00, 0x00,
            ]
        } else {
            vec![
                0x70, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00,
                0x00, 0x00, 0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00,
            ]
        };

        expected.extend_from_slice(&[0xbb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        expected[0] = expected.len() as u8;

        let sender = ReplySender::Assert(AssertSender { expected });
        let reply: ReplyAttr = Reply::new(ll::RequestId(0xdeadbeef), sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: INodeNo(0x11),
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
            blksize: 0xbb,
        };
        reply.attr(&ttl, &attr);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn reply_xtimes() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
            ],
        });
        let reply: ReplyXTimes = Reply::new(ll::RequestId(0xdeadbeef), sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        reply.xtimes(time, time);
    }

    #[test]
    fn reply_open() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
            ],
        });
        let reply: ReplyOpen = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.opened(ll::FileHandle(0x1122), FopenFlags::from_bits_retain(0x33));
    }

    #[test]
    fn reply_write() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x22, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        });
        let reply: ReplyWrite = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.written(0x1122);
    }

    #[test]
    fn reply_statfs() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x60, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x44, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x66, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        });
        let reply: ReplyStatfs = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.statfs(0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88);
    }

    #[test]
    fn reply_create() {
        let mut expected = if cfg!(target_os = "macos") {
            vec![
                0xa8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56,
                0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00, 0x00, 0x00,
                0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0x99, 0x00, 0x00, 0x00, 0xbb, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        } else {
            vec![
                0x98, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x65, 0x87, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x87,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00, 0x21, 0x43, 0x00, 0x00,
                0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x12,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00, 0x78, 0x56, 0x00, 0x00,
                0x78, 0x56, 0x00, 0x00, 0xa4, 0x81, 0x00, 0x00, 0x55, 0x00, 0x00, 0x00, 0x66, 0x00,
                0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0x88, 0x00, 0x00, 0x00, 0xbb, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        };

        let insert_at = expected.len() - 16;
        expected.splice(
            insert_at..insert_at,
            vec![0xdd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
        );
        expected[0] = (expected.len()) as u8;

        let sender = ReplySender::Assert(AssertSender { expected });
        let reply: ReplyCreate = Reply::new(ll::RequestId(0xdeadbeef), sender);
        let time = UNIX_EPOCH + Duration::new(0x1234, 0x5678);
        let ttl = Duration::new(0x8765, 0x4321);
        let attr = FileAttr {
            ino: INodeNo(0x11),
            size: 0x22,
            blocks: 0x33,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0x55,
            uid: 0x66,
            gid: 0x77,
            rdev: 0x88,
            flags: 0x99,
            blksize: 0xdd,
        };
        reply.created(
            &ttl,
            &attr,
            ll::Generation(0xaa),
            ll::FileHandle(0xbb),
            FopenFlags::from_bits_retain(0x0c),
        );
    }

    #[test]
    fn reply_lock() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x33, 0x00, 0x00, 0x00, 0x44, 0x00, 0x00, 0x00,
            ],
        });
        let reply: ReplyLock = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.locked(0x11, 0x22, 0x33, 0x44);
    }

    #[test]
    fn reply_bmap() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
        });
        let reply: ReplyBmap = Reply::new(ll::RequestId(0xdeadbeef), sender);
        reply.bmap(0x1234);
    }

    #[test]
    fn reply_directory() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x50, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xef, 0xbe, 0xad, 0xde, 0x00, 0x00,
                0x00, 0x00, 0xbb, 0xaa, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x68, 0x65,
                0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0xdd, 0xcc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08, 0x00,
                0x00, 0x00, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x2e, 0x72, 0x73,
            ],
        });
        let mut reply = ReplyDirectory::new(ll::RequestId(0xdeadbeef), sender, 4096);
        assert!(!reply.add(INodeNo(0xaabb), 1, FileType::Directory, "hello"));
        assert!(!reply.add(INodeNo(0xccdd), 2, FileType::RegularFile, "world.rs"));
        reply.ok();
    }

    #[test]
    fn reply_xattr_size() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00, 0x00,
                0x00, 0x00, 0x78, 0x56, 0x34, 0x12, 0x00, 0x00, 0x00, 0x00,
            ],
        });
        let reply = ReplyXattr::new(ll::RequestId(0xdeadbeef), sender);
        reply.size(0x12345678);
    }

    #[test]
    fn reply_xattr_data() {
        let sender = ReplySender::Assert(AssertSender {
            expected: vec![
                0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0xBE, 0xAD, 0xDE, 0x00, 0x00,
                0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
            ],
        });
        let reply = ReplyXattr::new(ll::RequestId(0xdeadbeef), sender);
        reply.data(&[0x11, 0x22, 0x33, 0x44]);
    }

    #[test]
    fn async_reply() {
        let (tx, rx) = sync_channel::<()>(1);
        let reply: ReplyEmpty = Reply::new(ll::RequestId(0xdeadbeef), ReplySender::Sync(tx));
        thread::spawn(move || {
            reply.ok();
        });
        rx.recv().unwrap();
    }

    /// CONSTELLATION PATCH (io-uring): a gathered reply is byte for byte
    /// the reply `data()` would have sent for the joined segments, whatever
    /// the segment count -- including none, and including more than the
    /// four a `ReadData` holds inline.
    #[test]
    fn gather_sends_the_segments_joined() {
        assert_eq!(gathered::<&[u8]>(&[]), data_reply(b""));
        assert_eq!(gathered(&[b"hello".as_slice()]), data_reply(b"hello"));
        assert_eq!(
            gathered(&[b"hel".as_slice(), b"", b"lo wor", b"ld"]),
            data_reply(b"hello world")
        );
        let owned: Vec<Vec<u8>> = (0..7u8).map(|i| vec![i; 3]).collect();
        let joined: Vec<u8> = owned.iter().flatten().copied().collect();
        assert_eq!(gathered(&owned), data_reply(&joined));
        // Empty segments carry no iovec of their own but must not be lost
        assert_eq!(gathered(&[b"".as_slice(), b"x"]), data_reply(b"x"));
    }
}
