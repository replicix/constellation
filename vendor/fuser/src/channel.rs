use std::io;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::sync::Arc;

use nix::errno::Errno;

use crate::dev_fuse::DevFuse;
use crate::passthrough::BackingId;

/// A raw communication channel to the FUSE kernel driver
#[derive(Debug, Clone)]
pub(crate) struct Channel(Arc<DevFuse>);

impl AsFd for Channel {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl Channel {
    /// Create a new communication channel to the kernel driver by mounting the
    /// given path. The kernel driver will delegate filesystem operations of
    /// the given path to the channel.
    pub(crate) fn new(device: Arc<DevFuse>) -> Self {
        Self(device)
    }

    /// Receives data up to the capacity of the given buffer (can block).
    fn receive(&self, buffer: &mut [u8]) -> nix::Result<usize> {
        nix::unistd::read(&self.0, buffer)
    }

    /// Receives data up to the capacity of the given buffer (can block),
    /// retrying on errors that are safe to retry (ENOENT, EINTR, EAGAIN).
    ///
    /// - ENOENT: Operation interrupted. According to FUSE, this is safe to retry.
    /// - EINTR: Interrupted system call, retry.
    /// - EAGAIN: Explicitly instructed to try again.
    pub(crate) fn receive_retrying(&self, buffer: &mut [u8]) -> nix::Result<usize> {
        loop {
            match self.receive(buffer) {
                Ok(size) => return Ok(size),
                Err(Errno::ENOENT | Errno::EINTR | Errno::EAGAIN) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// CONSTELLATION PATCH (io-uring): the `/dev/fuse` descriptor every
    /// ring SQE names, and the one the passthrough ioctls go through.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    pub(crate) fn device(&self) -> Arc<DevFuse> {
        self.0.clone()
    }

    /// CONSTELLATION PATCH (detach): make reads on this descriptor return
    /// `EAGAIN` when no request is pending (an armed session polls first).
    pub(crate) fn set_nonblocking(&self) -> io::Result<()> {
        use nix::fcntl::FcntlArg;
        use nix::fcntl::OFlag;
        use nix::fcntl::fcntl;
        let flags = OFlag::from_bits_retain(fcntl(&self.0, FcntlArg::F_GETFL)?);
        fcntl(&self.0, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
        Ok(())
    }

    /// CONSTELLATION PATCH (detach): [`Self::receive_retrying`] on a
    /// non-blocking descriptor that also returns `Ok(None)` once `stop`
    /// is set: `stop` is checked before every read, and `wake` (readable
    /// once a stop is requested) is polled beside the descriptor, so a
    /// loop waiting for a request returns at once.
    pub(crate) fn receive_or_wake(
        &self,
        buffer: &mut [u8],
        wake: BorrowedFd<'_>,
        stop: &std::sync::atomic::AtomicBool,
    ) -> nix::Result<Option<usize>> {
        use nix::poll::PollFd;
        use nix::poll::PollFlags;
        use nix::poll::PollTimeout;
        use nix::poll::poll;
        loop {
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(None);
            }
            match self.receive(buffer) {
                Ok(size) => return Ok(Some(size)),
                Err(Errno::EAGAIN) => {
                    let mut fds = [
                        PollFd::new(self.0.as_fd(), PollFlags::POLLIN),
                        PollFd::new(wake, PollFlags::POLLIN),
                    ];
                    match poll(&mut fds, PollTimeout::NONE) {
                        Ok(_) | Err(Errno::EINTR) => {}
                        Err(err) => return Err(err),
                    }
                }
                Err(Errno::ENOENT | Errno::EINTR) => {}
                Err(err) => return Err(err),
            }
        }
    }

    /// Returns a sender object for this channel. The sender object can be
    /// used to send to the channel. Multiple sender objects can be used
    /// and they can safely be sent to other threads.
    pub(crate) fn sender(&self) -> ChannelSender {
        // Since write/writev syscalls are threadsafe, we can simply create
        // a sender by using the same file and use it in other threads.
        ChannelSender(self.0.clone())
    }

    /// Clone the FUSE device fd using FUSE_DEV_IOC_CLONE ioctl.
    ///
    /// This creates a new fd that can read FUSE requests independently,
    /// enabling true parallel request processing. The kernel distributes
    /// requests across all cloned fds.
    ///
    /// Requires Linux 4.5+. Returns an error on older kernels or non-Linux.
    #[cfg(target_os = "linux")]
    pub(crate) fn clone_fd(&self) -> io::Result<Channel> {
        use std::os::fd::AsRawFd;

        let new_dev = DevFuse::open()?;

        let mut source_fd = self.0.as_raw_fd() as u32;
        // SAFETY: fuse_dev_ioc_clone is a valid ioctl for /dev/fuse
        unsafe {
            crate::ll::ioctl::fuse_dev_ioc_clone(new_dev.as_raw_fd(), &mut source_fd)?;
        }

        Ok(Channel::new(Arc::new(new_dev)))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChannelSender(Arc<DevFuse>);

impl ChannelSender {
    pub(crate) fn send(&self, bufs: &[io::IoSlice<'_>]) -> io::Result<()> {
        let rc = nix::sys::uio::writev(&self.0, bufs)?;
        // writev is atomic, so do not need to check how many bytes are written.
        // libfuse does not do it either
        // https://github.com/libfuse/libfuse/blob/6278995cca991978abd25ebb2c20ebd3fc9e8a13/lib/fuse_lowlevel.c#L267
        debug_assert_eq!(bufs.iter().map(|b| b.len()).sum::<usize>(), rc);
        Ok(())
    }

    pub(crate) fn open_backing(&self, fd: BorrowedFd<'_>) -> std::io::Result<BackingId> {
        BackingId::create(&self.0, fd)
    }

    pub(crate) unsafe fn wrap_backing(&self, id: u32) -> BackingId {
        unsafe { BackingId::wrap_raw(&self.0, id) }
    }
}
