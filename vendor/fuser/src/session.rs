//! Filesystem session
//!
//! A session runs a filesystem implementation while it is being mounted to a specific mount
//! point. A session begins by mounting the filesystem and ends by unmounting it. While the
//! filesystem is mounted, the session loop receives, dispatches and replies to kernel requests
//! for filesystem operations under its mount point.

use std::borrow::Cow;
use std::fs::File;
use std::io;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::thread::{self};

use log::debug;
use log::error;
use log::info;
use log::warn;
use nix::unistd::Uid;
use nix::unistd::geteuid;
use parking_lot::Mutex;

use crate::Errno;
use crate::Filesystem;
use crate::KernelConfig;
use crate::MountOption;
use crate::ReplyEmpty;
use crate::Request;
use crate::channel::Channel;
use crate::channel::ChannelSender;
use crate::dev_fuse::DevFuse;
use crate::ll;
use crate::ll::Operation;
use crate::ll::ResponseErrno;
use crate::ll::Version;
use crate::ll::flags::init_flags::InitFlags;
use crate::ll::fuse_abi as abi;
use crate::mnt::Mount;
use crate::mnt::mount_options::Config;
use crate::mnt::mount_options::check_option_conflicts;
use crate::notify::Notifier;
use crate::read_buf::FuseReadBuf;
use crate::reply::Reply;
use crate::reply::ReplyRaw;
use crate::reply::ReplySender;
use crate::request::RequestWithSender;

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
pub(crate) const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Default, Debug, Eq, PartialEq, Clone, Copy)]
/// How requests should be filtered based on the calling UID.
pub enum SessionACL {
    /// Allow requests from any user. Corresponds to the `allow_other` mount option.
    All,
    /// Allow requests from root. Corresponds to the `allow_root` mount option.
    RootAndOwner,
    /// Allow requests from the owning UID. This is FUSE's default mode of operation.
    #[default]
    Owner,
}

impl SessionACL {
    /// Returns the mount option string for kernel/fusermount/libfuse paths.
    /// Both `All` and `RootAndOwner` map to `allow_other` - the kernel only
    /// understands `allow_other`, and fuser enforces the root-only restriction internally.
    #[allow(dead_code)]
    pub(crate) fn to_mount_option(self) -> Option<&'static str> {
        match self {
            SessionACL::All | SessionACL::RootAndOwner => Some("allow_other"),
            SessionACL::Owner => None,
        }
    }
}

/// Calls `destroy` on drop.
#[derive(Debug)]
pub(crate) struct FilesystemHolder<FS: Filesystem> {
    pub(crate) fs: Option<FS>,
}

impl<FS: Filesystem> FilesystemHolder<FS> {
    fn destroy(&mut self) {
        if let Some(mut fs) = self.fs.take() {
            fs.destroy();
        }
    }
}

impl<FS: Filesystem> Drop for FilesystemHolder<FS> {
    fn drop(&mut self) {
        self.destroy();
    }
}

#[derive(Debug)]
struct UmountOnDrop {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl UmountOnDrop {
    fn umount(&self) -> io::Result<()> {
        if let Some(mount) = self.mount.lock().take() {
            mount.umount()?;
        }
        Ok(())
    }
}

impl Drop for UmountOnDrop {
    fn drop(&mut self) {
        if let Err(e) = self.umount() {
            warn!("Failed to umount filesystem: {}", e);
        }
    }
}

/// The session data structure
#[derive(Debug)]
pub struct Session<FS: Filesystem> {
    /// Filesystem operation implementations. None after `destroy` called.
    pub(crate) filesystem: FilesystemHolder<FS>,
    /// Communication channel to the kernel driver
    pub(crate) ch: Channel,
    /// Handle to the mount.  Dropping this unmounts.
    mount: UmountOnDrop,
    /// Whether to restrict access to owner, root + owner, or unrestricted
    /// Used to implement `allow_root` and `auto_unmount`
    pub(crate) allowed: SessionACL,
    /// User that launched the fuser process
    pub(crate) session_owner: Uid,
    /// FUSE protocol version, as reported by the kernel.
    /// The field is set to `Some` when the init message is received.
    pub(crate) proto_version: Option<Version>,
    pub(crate) config: Config,
    /// CONSTELLATION PATCH (negotiated-init): what `FUSE_INIT` agreed,
    /// set by the handshake or given to [`Session::from_fd_resumed`].
    negotiated: Option<NegotiatedInit>,
    /// CONSTELLATION PATCH (detach): armed by [`Session::detacher`].
    detach: Option<Arc<DetachSignal>>,
}

/// CONSTELLATION PATCH (negotiated-init): the `FUSE_INIT` parameters a
/// session agreed with the kernel, recorded by the handshake
/// ([`Session::negotiated_init`]) so that another process can serve the
/// same connection without a second handshake the kernel will never ask
/// for ([`Session::from_fd_resumed`]).
///
/// fuser frames every reply and notification by its compiled ABI, not by
/// the negotiated minor, so resuming needs no per-version state; the
/// record is what the resuming side checks it is compatible with
/// ([`NegotiatedInit::check_resumable`]) and what its filesystem learns
/// the agreed capabilities from, since `Filesystem::init` is not called
/// again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serializable", derive(serde::Serialize, serde::Deserialize))]
pub struct NegotiatedInit {
    /// The protocol version the kernel's `FUSE_INIT` announced.
    pub kernel_major: u32,
    pub kernel_minor: u32,
    /// The protocol version this side answered with.
    pub proto_major: u32,
    pub proto_minor: u32,
    /// The capabilities the kernel offered (`InitFlags` bits).
    pub kernel_flags: u64,
    /// The capabilities in force: requested by the filesystem and offered
    /// by the kernel (`InitFlags` bits, `FUSE_INIT_EXT` included).
    pub flags: u64,
    pub max_readahead: u32,
    /// The largest write the kernel sends: the resuming side's request
    /// buffer must hold it.
    pub max_write: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub time_gran_ns: u32,
    pub max_pages: u16,
    pub max_stack_depth: u32,
}

impl NegotiatedInit {
    /// Whether this build can serve a connection negotiated as `self`:
    /// the same major version, no capability it does not know, and a
    /// request buffer that holds the largest write the kernel will send.
    pub fn check_resumable(&self) -> io::Result<()> {
        let refuse = |why: String| Err(io::Error::new(io::ErrorKind::InvalidInput, why));
        if self.proto_major != abi::FUSE_KERNEL_VERSION {
            return refuse(format!(
                "the connection was negotiated as FUSE {}.{}, this build speaks major {}",
                self.proto_major,
                self.proto_minor,
                abi::FUSE_KERNEL_VERSION
            ));
        }
        let unknown = self.flags & !InitFlags::all().bits();
        if unknown != 0 {
            return refuse(format!(
                "the connection agreed capabilities this build does not know: {unknown:#x}"
            ));
        }
        if self.max_write as usize > MAX_WRITE_SIZE {
            return refuse(format!(
                "the connection's max_write {} exceeds this build's request buffer {MAX_WRITE_SIZE}",
                self.max_write
            ));
        }
        Ok(())
    }

    /// The capabilities in force.
    pub fn flags(&self) -> InitFlags {
        InitFlags::from_bits_retain(self.flags)
    }
}

/// CONSTELLATION PATCH (detach): how [`SessionDetacher::detach`] reaches
/// the event loops. A thread blocked in `read(2)` on `/dev/fuse` cannot be
/// woken without a request arriving, so an armed session reads its
/// (non-blocking) descriptors only when `poll(2)` says a request is
/// pending, and polls this pipe beside them: writing a byte makes every
/// loop return. The flag is checked before every read, so a request that
/// was read is always dispatched and answered, and one that was not stays
/// queued in the kernel for whoever serves the descriptor next.
#[derive(Debug)]
pub(crate) struct DetachSignal {
    stop: AtomicBool,
    wake_read: OwnedFd,
    wake_write: OwnedFd,
}

impl DetachSignal {
    fn new() -> io::Result<Self> {
        use nix::fcntl::FcntlArg;
        use nix::fcntl::FdFlag;
        use nix::fcntl::fcntl;
        let (wake_read, wake_write) = nix::unistd::pipe()?;
        for fd in [&wake_read, &wake_write] {
            fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
        }
        Ok(Self {
            stop: AtomicBool::new(false),
            wake_read,
            wake_write,
        })
    }

    fn request(&self) {
        self.stop.store(true, Ordering::SeqCst);
        // Never read: the pipe stays readable, so every loop's poll
        // returns, now and on any later pass.
        let _ = nix::unistd::write(&self.wake_write, &[1u8]);
    }

    fn requested(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }
}

/// CONSTELLATION PATCH (detach): stops an armed session's event loops
/// from another thread without unmounting and without closing the
/// connection ([`Session::detacher`], [`Session::run_detachable`]).
#[derive(Debug, Clone)]
pub struct SessionDetacher(Arc<DetachSignal>);

impl SessionDetacher {
    /// Ask every event loop to stop before its next read. Loops finish
    /// the request they are dispatching first; `run_detachable` returns
    /// once all have stopped.
    pub fn detach(&self) {
        self.0.request();
    }

    /// Whether [`Self::detach`] was called.
    pub fn is_detaching(&self) -> bool {
        self.0.requested()
    }
}

/// CONSTELLATION PATCH (detach): how [`Session::run_detachable`] ended.
#[derive(Debug)]
pub enum SessionEnd<FS> {
    /// The connection ended (an unmount, `FUSE_DESTROY`, an abort): the
    /// filesystem was destroyed, as [`Session::run`] does.
    Ended,
    /// Detached: every request read was answered, and the connection is
    /// still mounted and open.
    Detached(DetachedSession<FS>),
}

/// CONSTELLATION PATCH (detach): a session stopped by
/// [`SessionDetacher::detach`]. The kernel queues requests for the
/// connection until something reads `fd` again: this process (a new
/// [`Session::from_fd_resumed`]) or another one it hands `fd` to.
#[derive(Debug)]
pub struct DetachedSession<FS> {
    /// The filesystem, not destroyed.
    pub filesystem: FS,
    /// A duplicate of the session's `/dev/fuse` descriptor (the same open
    /// file, so the same connection and request queues). Non-blocking,
    /// and close-on-exec as every descriptor fuser opens.
    pub fd: OwnedFd,
    /// What `FUSE_INIT` agreed.
    pub init: NegotiatedInit,
}

impl<FS: Filesystem> AsFd for Session<FS> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.ch.as_fd()
    }
}

impl<FS: Filesystem> Session<FS> {
    /// Create a new session by mounting the given filesystem to the given mountpoint
    /// # Errors
    /// Returns an error if the options are incorrect, or if the fuse device can't be mounted.
    pub fn new<P: AsRef<Path>>(
        filesystem: FS,
        mountpoint: P,
        options: &Config,
    ) -> io::Result<Session<FS>> {
        check_option_conflicts(options)?;

        let mountpoint = mountpoint.as_ref();
        info!("Mounting {}", mountpoint.display());
        // If AutoUnmount is requested, but not AllowRoot or AllowOther, return an error
        // because fusermount needs allow_root or allow_other to handle the auto_unmount option
        if options.mount_options.contains(&MountOption::AutoUnmount)
            && options.acl == SessionACL::Owner
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("auto_unmount requires acl != Owner, got: {:?}", options.acl),
            ));
        }
        let (file, mount) = Mount::new(mountpoint, &options.mount_options, options.acl)?;

        let ch = Channel::new(file);

        let mut session = Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(Some(mount))),
            },
            allowed: options.acl,
            session_owner: geteuid(),
            proto_version: None,
            config: options.clone(),
            negotiated: None,
            detach: None,
        };

        session.handshake()?;

        Ok(session)
    }

    /// Wrap an existing /dev/fuse file descriptor. This doesn't mount the
    /// filesystem anywhere; that must be done separately.
    pub fn from_fd(
        filesystem: FS,
        fd: OwnedFd,
        acl: SessionACL,
        config: Config,
    ) -> io::Result<Self> {
        let ch = Channel::new(Arc::new(DevFuse(File::from(fd))));
        let mut session = Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(None)),
            },
            allowed: acl,
            session_owner: geteuid(),
            proto_version: None,
            config,
            negotiated: None,
            detach: None,
        };

        session.handshake()?;

        Ok(session)
    }

    /// CONSTELLATION PATCH (from-fd-resumed): serve a `/dev/fuse`
    /// descriptor whose `FUSE_INIT` was already answered — by another
    /// session, in this process or another — without a handshake: the
    /// kernel sends `FUSE_INIT` once per connection, so the first request
    /// on such a descriptor is an ordinary one, which [`Session::from_fd`]
    /// would refuse. `init` is what that handshake agreed
    /// ([`Session::negotiated_init`], [`DetachedSession::init`]).
    /// `Filesystem::init` is not called: the filesystem is already
    /// initialised for this connection as far as the kernel knows.
    pub fn from_fd_resumed(
        filesystem: FS,
        fd: OwnedFd,
        acl: SessionACL,
        config: Config,
        init: NegotiatedInit,
    ) -> io::Result<Self> {
        init.check_resumable()?;
        let ch = Channel::new(Arc::new(DevFuse(File::from(fd))));
        Ok(Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(None)),
            },
            allowed: acl,
            session_owner: geteuid(),
            proto_version: Some(Version(init.kernel_major, init.kernel_minor)),
            config,
            negotiated: Some(init),
            detach: None,
        })
    }

    /// CONSTELLATION PATCH (negotiated-init): what `FUSE_INIT` agreed.
    pub fn negotiated_init(&self) -> Option<NegotiatedInit> {
        self.negotiated
    }

    /// CONSTELLATION PATCH (detach): arm this session for
    /// [`SessionDetacher::detach`] (call before running it). Refused for
    /// an `auto_unmount` mount, whose unmount is tied to this process.
    pub fn detacher(&mut self) -> io::Result<SessionDetacher> {
        if self
            .config
            .mount_options
            .contains(&MountOption::AutoUnmount)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "an auto_unmount session cannot be detached",
            ));
        }
        let signal = match &self.detach {
            Some(signal) => signal.clone(),
            None => {
                let signal = Arc::new(DetachSignal::new()?);
                self.detach = Some(signal.clone());
                signal
            }
        };
        Ok(SessionDetacher(signal))
    }

    /// Run the session loop in a background thread. If the returned handle is dropped,
    /// the filesystem is unmounted and the given session ends.
    pub fn spawn(self) -> io::Result<BackgroundSession> {
        let sender = self.ch.sender();
        // Take the fuse_session, so that we can unmount it
        let mount = std::mem::take(&mut *self.mount.mount.lock());
        let guard = thread::Builder::new()
            .name("fuser-bg".to_string())
            .spawn(move || self.run())?;
        Ok(BackgroundSession {
            guard,
            sender,
            mount,
        })
    }

    /// Run the session loop that receives kernel requests and dispatches them to method
    /// calls into the filesystem. This read-dispatch-loop is non-concurrent to prevent
    /// having multiple buffers (which take up much memory), but the filesystem methods
    /// may run concurrent by spawning threads.
    /// # Errors
    /// Returns any final error when the session comes to an end.
    pub fn run(self) -> io::Result<()> {
        // CONSTELLATION PATCH (detach): the loop is `run_detachable`'s; a
        // detached session is dropped here (its descriptor closes).
        self.run_detachable().map(|_| ())
    }

    /// CONSTELLATION PATCH (detach): [`Session::run`], which an armed
    /// session ([`Session::detacher`]) may also leave detached: then the
    /// filesystem is not destroyed, the mount is not unmounted, and the
    /// descriptor comes back open ([`SessionEnd::Detached`]).
    pub fn run_detachable(self) -> io::Result<SessionEnd<FS>> {
        let Session {
            filesystem,
            ch,
            mount: do_not_umount_yet,
            allowed,
            session_owner,
            proto_version: _,
            config,
            negotiated,
            detach,
        } = self;
        let master = ch.clone();

        let n_threads = config.n_threads.unwrap_or(1);

        if !cfg!(target_os = "linux") && n_threads != 1 {
            // TODO: check whether it works on macOS/FreeBSD and enable if it works.
            return Err(io::Error::other(
                "n_threads != 1 is only supported on Linux",
            ));
        }

        let Some(n_threads_minus_one) = n_threads.checked_sub(1) else {
            return Err(io::Error::other("n_threads"));
        };

        let mut filesystem = Arc::new(filesystem);

        let mut channels = Vec::with_capacity(n_threads);

        for _ in 0..n_threads_minus_one {
            if config.clone_fd {
                #[cfg(target_os = "linux")]
                {
                    channels.push(ch.clone_fd()?);
                    continue;
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(io::Error::other("clone_fd is only supported on Linux"));
                }
            } else {
                channels.push(ch.clone());
            }
        }
        channels.push(ch);

        let mut threads = Vec::with_capacity(n_threads);

        for (i, ch) in channels.into_iter().enumerate() {
            let thread_name = format!("fuser-{i}");
            if detach.is_some() {
                ch.set_nonblocking()?;
            }
            let event_loop = SessionEventLoop {
                thread_name: thread_name.clone(),
                filesystem: filesystem.clone(),
                ch,
                allowed,
                session_owner,
                detach: detach.clone(),
            };
            threads.push(
                thread::Builder::new()
                    .name(thread_name)
                    .spawn(move || event_loop.event_loop())?,
            );
        }

        let mut reply: io::Result<()> = Ok(());
        // CONSTELLATION PATCH (detach): whether every loop stopped for a
        // detach (none saw the connection end).
        let mut all_stopped = true;
        for thread in threads {
            let res = match thread.join() {
                Ok(res) => res,
                Err(_) => {
                    return Err(io::Error::other("event loop thread panicked"));
                }
            };
            match res {
                Ok(LoopEnd::Stopped) => {}
                Ok(LoopEnd::Ended) => all_stopped = false,
                Err(e) => {
                    all_stopped = false;
                    if reply.is_ok() {
                        reply = Err(e);
                    }
                }
            }
        }

        let Some(filesystem) = Arc::get_mut(&mut filesystem) else {
            return Err(io::Error::other(
                "BUG: must have one refcount for filesystem",
            ));
        };

        // CONSTELLATION PATCH (detach): hand the connection back instead
        // of ending it.
        if all_stopped && detach.as_ref().is_some_and(|d| d.requested()) {
            let init = negotiated
                .ok_or_else(|| io::Error::other("BUG: a detached session never negotiated"))?;
            let fd = master.as_fd().try_clone_to_owned()?;
            if let Some(mount) = do_not_umount_yet.mount.lock().take() {
                mount.disarm();
            }
            let Some(fs) = filesystem.fs.take() else {
                return Err(io::Error::other("BUG: the filesystem was destroyed"));
            };
            return Ok(SessionEnd::Detached(DetachedSession {
                filesystem: fs,
                fd,
                init,
            }));
        }

        filesystem.destroy();

        reply.map(|()| SessionEnd::Ended)
    }

    fn handshake(&mut self) -> io::Result<()> {
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();

        loop {
            // Read the init request from the kernel
            let size = match self.ch.receive_retrying(buf) {
                Ok(size) => size,
                Err(nix::errno::Errno::ENODEV) => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "FUSE device disconnected during handshake",
                    ));
                }
                Err(err) => return Err(err.into()),
            };

            // Parse the request
            let request = match ll::AnyRequest::try_from(&buf[..size]) {
                Ok(request) => request,
                Err(err) => {
                    error!("{err}");
                    return Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string()));
                }
            };

            // Extract the init operation
            let op = match request.operation() {
                Ok(op) => op,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Failed to parse FUSE operation",
                    ));
                }
            };

            let init = match op {
                ll::Operation::Init(init) => init,
                _ => {
                    error!("Received non-init FUSE operation before init: {}", request);
                    // Send error response and return error - non-init during handshake is invalid
                    <ReplyRaw as Reply>::new(
                        request.unique(),
                        ReplySender::Channel(self.ch.sender()),
                    )
                    .send_ll(&ResponseErrno(ll::Errno::EIO));
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Received non-init FUSE operation during handshake",
                    ));
                }
            };

            let v = init.version();
            if v.0 > abi::FUSE_KERNEL_VERSION {
                // Kernel has a newer major version than we support.
                // Send our version and wait for a second INIT request with a compatible version.
                debug!(
                    "INIT: Kernel version {} > our version {}, sending our version and waiting for next init",
                    v.0,
                    abi::FUSE_KERNEL_VERSION
                );
                let response = init.reply_version_only();
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&response);
                continue;
            }

            // We don't support ABI versions before 7.6
            if v < Version(7, 6) {
                error!("Unsupported FUSE ABI version {v}");
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&ResponseErrno(ll::Errno::EPROTO));
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Unsupported FUSE ABI version {v}"),
                ));
            }

            let mut config = KernelConfig::new(init.capabilities(), init.max_readahead(), v);

            // Call filesystem init method and give it a chance to return an error
            let Some(filesystem) = &mut self.filesystem.fs else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Bug: filesystem must be initialized during handshake",
                ));
            };
            let res = filesystem.init(Request::ref_cast(request.header()), &mut config);
            if let Err(error) = res {
                let errno = Errno::from_i32(error.raw_os_error().unwrap_or(0));
                <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                    .send_ll(&ResponseErrno(errno));
                return Err(error);
            }

            // Remember the ABI version supported by kernel and mark the session initialized.
            self.proto_version = Some(v);

            // Log capability status for debugging
            for bit in 0..64 {
                let bitflags = InitFlags::from_bits_retain(1 << bit);
                if bitflags == InitFlags::FUSE_INIT_EXT {
                    continue;
                }
                let bitflag_is_known = InitFlags::all().contains(bitflags);
                let kernel_supports = init.capabilities().contains(bitflags);
                let we_requested = config.requested.contains(bitflags);
                // On macOS, there's a clash between linux and macOS constants,
                // so we pick macOS ones (last).
                let name = if let Some((name, _)) = bitflags.iter_names().last() {
                    Cow::Borrowed(name)
                } else {
                    Cow::Owned(format!("(1 << {bit})"))
                };
                if we_requested && kernel_supports {
                    debug!("capability {name} enabled")
                } else if we_requested {
                    debug!("capability {name} not supported by kernel")
                } else if kernel_supports {
                    debug!("capability {name} not requested by client")
                } else if bitflag_is_known {
                    debug!("capability {name} not supported nor requested")
                }
            }

            // Reply with our desired version and settings.
            debug!(
                "INIT response: ABI {}.{}, flags {:#x}, max readahead {}, max write {}",
                abi::FUSE_KERNEL_VERSION,
                abi::FUSE_KERNEL_MINOR_VERSION,
                init.capabilities() & config.requested,
                config.max_readahead,
                config.max_write
            );

            let response = init.reply(&config);
            // CONSTELLATION PATCH (negotiated-init): what this reply
            // agrees to, as `Init::reply` computes it.
            let agreed = (config.requested | InitFlags::FUSE_INIT_EXT) & init.capabilities();
            self.negotiated = Some(NegotiatedInit {
                kernel_major: v.0,
                kernel_minor: v.1,
                proto_major: abi::FUSE_KERNEL_VERSION,
                proto_minor: abi::FUSE_KERNEL_MINOR_VERSION,
                kernel_flags: init.capabilities().bits(),
                flags: agreed.bits(),
                max_readahead: config.max_readahead,
                max_write: config.max_write,
                max_background: config.max_background,
                congestion_threshold: config.congestion_threshold(),
                time_gran_ns: config.time_gran.as_nanos() as u32,
                max_pages: config.max_pages(),
                max_stack_depth: if agreed.contains(InitFlags::FUSE_PASSTHROUGH) {
                    config.max_stack_depth
                } else {
                    0
                },
            });
            <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                .send_ll(&response);

            return Ok(());
        }
    }

    /// Unmount the filesystem
    pub fn unmount(&mut self) -> io::Result<()> {
        self.mount.umount()
    }

    /// Returns a thread-safe object that can be used to unmount the Filesystem
    pub fn unmount_callable(&mut self) -> SessionUnmounter {
        SessionUnmounter {
            mount: self.mount.mount.clone(),
        }
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.ch.sender())
    }
}

#[derive(Debug)]
/// A thread-safe object that can be used to unmount a Filesystem
pub struct SessionUnmounter {
    mount: Arc<Mutex<Option<Mount>>>,
}

impl SessionUnmounter {
    /// Unmount the filesystem
    pub fn unmount(&mut self) -> io::Result<()> {
        if let Some(mount) = std::mem::take(&mut *self.mount.lock()) {
            mount.umount()?;
        }
        Ok(())
    }
}

pub(crate) struct SessionEventLoop<FS: Filesystem> {
    /// Cache thread name for faster `debug!`.
    pub(crate) thread_name: String,
    pub(crate) ch: Channel,
    pub(crate) filesystem: Arc<FilesystemHolder<FS>>,
    pub(crate) allowed: SessionACL,
    pub(crate) session_owner: Uid,
    /// CONSTELLATION PATCH (detach): `Some` for an armed session.
    pub(crate) detach: Option<Arc<DetachSignal>>,
}

/// CONSTELLATION PATCH (detach): why an event loop returned.
enum LoopEnd {
    /// The connection ended.
    Ended,
    /// A detach was requested.
    Stopped,
}

impl<FS: Filesystem> SessionEventLoop<FS> {
    fn event_loop(&self) -> io::Result<LoopEnd> {
        // Buffer for receiving requests from the kernel. Only one is allocated and
        // it is reused immediately after dispatching to conserve memory and allocations.
        let mut buf = FuseReadBuf::new();
        let buf = buf.as_mut();
        loop {
            // Read the next request from the given channel to kernel driver
            // The kernel driver makes sure that we get exactly one request per read
            // CONSTELLATION PATCH (detach): an armed session reads only
            // when a request is pending or the detach pipe says stop.
            let received = match &self.detach {
                None => self.ch.receive_retrying(buf).map(Some),
                Some(d) => self
                    .ch
                    .receive_or_wake(buf, d.wake_read.as_fd(), &d.stop),
            };
            match received {
                Ok(None) => return Ok(LoopEnd::Stopped),
                Ok(Some(size)) => match RequestWithSender::new(self.ch.sender(), &buf[..size]) {
                    // Dispatch request
                    Some(req) => {
                        if let Ok(Operation::Destroy(_)) = req.request.operation() {
                            req.reply::<ReplyEmpty>().ok();
                            return Ok(LoopEnd::Ended);
                        } else {
                            req.dispatch(self)
                        }
                    }
                    // Quit loop on illegal request
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Invalid request",
                        ));
                    }
                },
                Err(nix::errno::Errno::ENODEV) => return Ok(LoopEnd::Ended),
                Err(err) => return Err(err.into()),
            }
        }
    }
}

/// The background session data structure
#[derive(Debug)]
pub struct BackgroundSession {
    /// Thread guard of the background session
    pub guard: JoinHandle<io::Result<()>>,
    /// Object for creating Notifiers for client use
    sender: ChannelSender,
    /// Ensures the filesystem is unmounted when the session ends
    mount: Option<Mount>,
}

impl BackgroundSession {
    /// Unmount the filesystem and join the background thread.
    pub fn umount_and_join(mut self) -> io::Result<()> {
        if let Some(mount) = self.mount.take() {
            mount.umount()?;
        }
        self.join()
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.sender.clone())
    }

    /// Join the filesystem thread.
    pub fn join(self) -> io::Result<()> {
        self.guard
            .join()
            .map_err(|_panic: Box<dyn std::any::Any + Send>| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "filesystem background thread panicked",
                )
            })?
    }
}
