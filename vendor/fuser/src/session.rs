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
use crate::ll::reply::Response;
use crate::mnt::Mount;
use crate::mnt::mount_options::Config;
use crate::mnt::mount_options::check_option_conflicts;
use crate::notify::Notifier;
use crate::read_buf::FuseReadBuf;
use crate::reply::Reply;
use crate::reply::ReplyRaw;
use crate::reply::ReplySender;
use crate::request::RequestWithSender;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::uring::RingSet;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::uring::ring::RingCommit;
#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::uring::ring::HeldRequest;

/// The max size of write requests from the kernel. The absolute minimum is 4k,
/// FUSE recommends at least 128k, max 16M. The FUSE default is 16M on macOS
/// and 128k on other systems.
pub(crate) const MAX_WRITE_SIZE: usize = 16 * 1024 * 1024;

// CONSTELLATION PATCH (io-uring): the ring session's thread supervisor
// reports a panic with the same text upstream's join loop does.
/// The error `run` ends with when a session thread panicked.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
const THREAD_PANICKED: &str = "event loop thread panicked";

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
    /// CONSTELLATION PATCH (io-uring): the io_uring rings, when the kernel
    /// serves this session over them. Declared after `mount` so that the
    /// unmount, which completes their kernel commands, happens first.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    ring: Option<RingSet>,
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

/// CONSTELLATION PATCH (io-uring): how the kernel and this session move
/// requests and replies -- settled by `FUSE_INIT` and fixed for the life of
/// the connection, because a connection whose ring queues became ready can
/// never serve over `/dev/fuse` again (plan 38 §3(e), milestone Z0a).
///
/// Unlike everything else the `io-uring` feature adds, this type and
/// [`NegotiatedInit::transport`] exist in every build: a build without the
/// feature must still **refuse** to resume a connection a build with it
/// negotiated over a ring, rather than silently read `/dev/fuse` and hang.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serializable", derive(serde::Serialize, serde::Deserialize))]
pub enum Transport {
    /// `read(2)` and `writev(2)` on `/dev/fuse`: every kernel, every
    /// platform, and the terminal fallback of every downgrade.
    #[default]
    DevFuse,
    /// FUSE-over-io_uring (kernel 6.14+, `fuse.enable_uring=Y`): requests
    /// and replies ride registered ring entries.
    Uring,
    /// FUSE-over-io_uring with the kernel's registered buffer pools
    /// (7.3+). Never negotiated yet -- the placeholder the zero-copy step
    /// fills in.
    UringZeroCopy,
}

impl Transport {
    /// The name this transport is reported and refused under.
    pub fn name(self) -> &'static str {
        match self {
            Self::DevFuse => "dev_fuse",
            Self::Uring => "uring",
            Self::UringZeroCopy => "uring_zc",
        }
    }

    /// Whether this is the portable `/dev/fuse` transport: the only one a
    /// connection can be handed to another process over.
    pub fn is_dev_fuse(self) -> bool {
        matches!(self, Self::DevFuse)
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
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
    /// CONSTELLATION PATCH (io-uring): the transport the session serves
    /// this connection over. Only `DevFuse` can be handed over.
    ///
    /// Defaulted when absent, because a handoff written by a build from
    /// before this patch has no such field and every connection it could
    /// have negotiated was `/dev/fuse`: without the default, upgrading
    /// across this patch would fail to parse the handoff after the old
    /// image already detached every session.
    #[cfg_attr(feature = "serializable", serde(default))]
    pub transport: Transport,
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
        // CONSTELLATION PATCH (io-uring): a ring connection is not
        // resumable at all. Its entries belonged to the previous process's
        // io_uring instance and died with it; the kernel never routes its
        // requests back to `/dev/fuse`, and re-registering loses every
        // request that sat in an old entry (plan 38 §3(e), Z0a).
        if !self.transport.is_dev_fuse() {
            return refuse(format!(
                "the connection is served over the {} transport, which cannot be handed over \
                 (only dev_fuse can); tear the mount down and remount instead",
                self.transport
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
        validate_transport(options)?;

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
            // CONSTELLATION PATCH (io-uring)
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ring: None,
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
        validate_transport(&config)?;
        let ch = Channel::new(Arc::new(DevFuse(File::from(fd))));
        let mut session = Session {
            filesystem: FilesystemHolder {
                fs: Some(filesystem),
            },
            ch,
            mount: UmountOnDrop {
                mount: Arc::new(Mutex::new(None)),
            },
            // CONSTELLATION PATCH (io-uring)
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ring: None,
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
            // CONSTELLATION PATCH (io-uring): a resumed session is always
            // `/dev/fuse`; `check_resumable` refused a ring one above.
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ring: None,
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
        // CONSTELLATION PATCH (io-uring): arming a ring session would
        // promise a handover the kernel cannot honor (plan 38 §3(e)).
        if !self.transport().is_dev_fuse() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "a session served over the {} transport cannot be detached",
                    self.transport()
                ),
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
            // CONSTELLATION PATCH (io-uring)
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            ring,
            allowed,
            session_owner,
            proto_version: _,
            config,
            negotiated,
            detach,
        } = self;
        let master = ch.clone();

        // CONSTELLATION PATCH (io-uring): a ring session is served by
        // `serve_ring` instead -- one thread per ring plus one `/dev/fuse`
        // reader for the requests the kernel never sends over a ring
        // (`FUSE_INIT`, `FORGET`, `INTERRUPT`, notification replies). It is
        // never detachable: `detacher()` refuses to arm it, because Z0a
        // (plan 38 §3(e)) showed a ring connection can be neither handed
        // over losslessly nor downgraded back to `/dev/fuse`.
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if let Some(ring) = ring {
            let mut filesystem = Arc::new(filesystem);
            // The closure is dropped with the call, so the only clones of
            // `filesystem` left afterwards belong to threads still running.
            let reply = serve_ring(ring, ch, &config, |thread_name, ch| SessionEventLoop {
                thread_name,
                filesystem: filesystem.clone(),
                ch,
                allowed,
                session_owner,
                detach: None,
            });
            let Some(filesystem) = Arc::get_mut(&mut filesystem) else {
                // Only a panic ends the supervisor early; the threads
                // still running hold references, and `destroy` runs when
                // the last of them exits. The panic is the real error.
                reply?;
                return Err(io::Error::other(
                    "BUG: must have one refcount for filesystem",
                ));
            };
            filesystem.destroy();
            // Then the unmount, which is what lets any thread the
            // supervisor left detached see the connection end.
            drop(do_not_umount_yet);
            return reply.map(|()| SessionEnd::Ended);
        }

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

            // CONSTELLATION PATCH (io-uring): the rings are created before
            // the INIT reply and the flag is echoed only when they exist, so
            // a kernel told to use ring queues always has some. Every refusal
            // -- no `FUSE_OVER_IO_URING` offered (kernel < 6.14 or
            // `fuse.enable_uring=N`), `io_uring_setup` denied by a seccomp
            // policy (EPERM) or unavailable (ENOSYS), a rejected ring size
            // (EINVAL) or a refused reservation (ENOMEM) -- is logged once,
            // here, and leaves the session on `/dev/fuse`.
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            if self.config.io_uring {
                if init.capabilities().contains(InitFlags::FUSE_OVER_IO_URING) {
                    match self.create_rings(&config) {
                        Ok(ring) => {
                            config.enable_io_uring();
                            self.ring = Some(ring);
                        }
                        Err(err) => warn!("io_uring requested but {err}; using /dev/fuse"),
                    }
                } else {
                    warn!(
                        "io_uring requested but the kernel did not advertise FUSE_OVER_IO_URING \
                         (fuse.enable_uring=N or kernel < 6.14); using /dev/fuse"
                    );
                }
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
                // CONSTELLATION PATCH (io-uring): the rings either exist by
                // now or the session fell back, so this is the transport the
                // connection is served over for good.
                transport: self.transport(),
            });
            // CONSTELLATION PATCH (io-uring): a reply that echoed the flag
            // but never arrived leaves the kernel routing to queues nothing
            // can be registered against, so the session cannot go on; a
            // `/dev/fuse` session is only failed by its event loop, as
            // upstream's `send_ll` does.
            #[cfg(all(feature = "io-uring", target_os = "linux"))]
            if self.ring.is_some() {
                let unique = request.unique();
                response.with_iovec(unique, |iov| self.ch.sender().send(iov))?;
                // From here every request on the mount waits for the queues
                // to be registered, so the ring threads register now and the
                // session is handed back ready to serve.
                if let Some(ring) = &mut self.ring {
                    ring.start()?;
                }
                return Ok(());
            }
            <ReplyRaw as Reply>::new(request.unique(), ReplySender::Channel(self.ch.sender()))
                .send_ll(&response);

            return Ok(());
        }
    }

    /// CONSTELLATION PATCH (io-uring): the rings of this session, from the
    /// negotiated buffer sizes: the kernel's REGISTER requires a payload of
    /// `max(8192, max_write, max_pages * PAGE_SIZE)` bytes per entry.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    fn create_rings(&self, config: &KernelConfig) -> io::Result<RingSet> {
        let payload_cap = (config.max_write as usize)
            .max(usize::from(config.max_pages()) * page_size::get())
            .max(8192);
        RingSet::new(
            self.ch.device(),
            self.mount.mount.lock().is_some(),
            self.config.n_threads.unwrap_or(1),
            self.config.io_uring_queue_depth,
            payload_cap,
            self.config.io_uring_kernel.as_ref(),
            self.config.io_uring_malformed_register,
        )
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

    /// CONSTELLATION PATCH (io-uring): the transport this session serves
    /// its connection over. `DevFuse` until the handshake registers rings.
    pub fn transport(&self) -> Transport {
        #[cfg(all(feature = "io-uring", target_os = "linux"))]
        if self.ring.is_some() {
            return Transport::Uring;
        }
        Transport::DevFuse
    }

    /// Returns an object that can be used to send notifications to the kernel
    pub fn notifier(&self) -> Notifier {
        Notifier::new(self.ch.sender())
    }
}

/// Rejects a thread count or transport choice this build or target cannot honor, before
/// anything is mounted.
fn validate_transport(config: &Config) -> io::Result<()> {
    if config.n_threads == Some(0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "n_threads must be at least 1",
        ));
    }
    if !config.io_uring {
        return Ok(());
    }
    if !cfg!(target_os = "linux") {
        return Err(io::Error::other(
            "io_uring transport is only supported on Linux",
        ));
    }
    if !cfg!(feature = "io-uring") {
        return Err(io::Error::other(
            "io_uring transport requires the io-uring cargo feature",
        ));
    }
    if config.io_uring_queue_depth == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "io_uring_queue_depth must be at least 1",
        ));
    }
    Ok(())
}

/// What a thread of a ring session tells `serve_ring`.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
enum Event {
    /// The thread at this index of `serve_ring`'s list is done, panicked or not.
    Exited(usize),
    /// A filesystem callback on a ring thread panicked; the reply it owed went out as EIO
    /// during the unwind.
    Panicked,
}

/// Sends `Event::Exited` when dropped, which the owning thread does as its last act.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
struct ExitNotice {
    events: std::sync::mpsc::Sender<Event>,
    thread: usize,
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl Drop for ExitNotice {
    fn drop(&mut self) {
        let _ = self.events.send(Event::Exited(self.thread));
    }
}

/// The `FetchHandler` of one ring thread; it lives exactly as long as the thread serves.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
struct RingHandler<FS: Filesystem> {
    ctx: SessionEventLoop<FS>,
    exit: ExitNotice,
    /// A callback panicked on this ring: it no longer enters the filesystem, as a `/dev/fuse`
    /// reader that died does not either; other rings and the reader are unaffected
    panicked: bool,
    /// CONSTELLATION PATCH (io-uring): where requests that may block are dispatched instead
    /// of on this thread (`dispatch_on_ring`).
    offload: std::sync::mpsc::Sender<HeldRequest>,
    /// CONSTELLATION PATCH (io-uring): a callback panicked on an offload thread. The offload
    /// threads serve every ring, so every ring stops entering the filesystem then, as this one
    /// does after a panic of its own (`panicked`).
    offload_panicked: Arc<std::sync::atomic::AtomicBool>,
}

/// CONSTELLATION PATCH (io-uring): whether a ring thread dispatches a request with this
/// opcode itself, rather than handing it to the session's offload threads.
///
/// A ring thread serves every request the kernel queues on its CPUs' queues, one at a time,
/// so a callback that blocks on it blocks them all -- a `stat` behind a `close` whose flush
/// waits on a remote store (plan 38 Z1b: `s3-cut-one-node` on the ring leg). A `/dev/fuse`
/// session has no such coupling: any of its `n_threads` readers takes the next request.
/// So only reads of data and of metadata that a filesystem answers from what it holds stay on
/// the ring, where the transport's saving is the whole point; a filesystem that must wait for
/// the data can reply from another thread (`ReplyData` is `Send`). Everything that creates,
/// changes, opens, syncs, locks or flushes -- the operations that wait for leases, for a
/// remote store, for another node -- directory listings (a listing from its start may ask
/// another node for a read position first, as a lookup may), and every opcode this list
/// does not know is offloaded: one thread hop, then exactly the dispatch the ring thread
/// would have made, with the request still in the entry's buffers (`HeldRequest`).
///
/// "Answers from what it holds" is not "never waits": a callback on this list may still wait
/// for a bounded time on its own node -- Constellation's `getattr`, `readlink` and xattr reads
/// wait for the local replica to catch up with this node's own writes (at most
/// `CONSTELLATION_SESSION_WAIT_MS`, then answer anyway), and any op may queue at the view's
/// admission gate when `max_inflight_ops` is set -- exactly the waits a read already has.
/// What it never does is wait on a remote store, a lease or another node's answer.
///
/// An offloaded request still holds its ring entry until it is answered, as does a read the
/// filesystem answers later from another thread. That is what bounds blocking lock requests
/// per queue (`RingCommit::reserve_lock_wait`).
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub(crate) fn dispatch_on_ring(opcode: u32) -> bool {
    use crate::ll::fuse_abi::fuse_opcode::*;
    matches!(
        crate::ll::fuse_abi::fuse_opcode::try_from(opcode),
        Ok(FUSE_READ
            | FUSE_GETATTR
            | FUSE_READLINK
            | FUSE_GETXATTR
            | FUSE_LISTXATTR
            | FUSE_STATFS
            | FUSE_ACCESS
            | FUSE_RELEASEDIR
            | FUSE_DESTROY)
    )
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl<FS: Filesystem> crate::uring::ring::FetchHandler for RingHandler<FS> {
    fn handle(&mut self, commit: RingCommit, request: &[u8]) {
        if self.panicked || self.offload_panicked.load(Ordering::SeqCst) {
            return commit.commit_errno(Errno::EIO);
        }
        // `fuse_in_header.opcode`; a request too short to have one is the parser's to refuse
        let opcode = request
            .get(4..8)
            .map_or(u32::MAX, |b| u32::from_ne_bytes(b.try_into().unwrap()));
        if request.len() >= 8 && !dispatch_on_ring(opcode) {
            // The receiving end lives as long as any sender; if the offload threads are gone
            // (spawning them failed part way), the request is served here after all
            let mut held = commit.hold(request);
            if opcode == crate::ll::fuse_abi::fuse_opcode::FUSE_SETLKW as u32
                && !held.commit().reserve_lock_wait()
            {
                // This queue already has `depth - 1` blocking lock requests waiting on their
                // entries; one more could take its last, which the lock holder's next request
                // may need (`RingCommit::reserve_lock_wait`)
                held.downgrade_lock_wait();
            }
            let Err(std::sync::mpsc::SendError(held)) = self.offload.send(held) else {
                return;
            };
            let ctx = &self.ctx;
            let dispatch = std::panic::AssertUnwindSafe(|| {
                ctx.handle_fetch(held.commit().clone(), held.request())
            });
            if std::panic::catch_unwind(dispatch).is_err() {
                self.panicked = true;
                let _ = self.exit.events.send(Event::Panicked);
            }
            return;
        }
        // A ring thread must outlive its panicking callback: only it can submit the EIO the
        // unwind committed, and its kernel commands hold the mount's queues
        let ctx = &self.ctx;
        let dispatch = std::panic::AssertUnwindSafe(|| ctx.handle_fetch(commit, request));
        if std::panic::catch_unwind(dispatch).is_err() {
            self.panicked = true;
            let _ = self.exit.events.send(Event::Panicked);
        }
    }
}

/// CONSTELLATION PATCH (io-uring): one offload thread: dispatches what ring threads hand it
/// (`dispatch_on_ring`), until every ring thread is gone.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn offload_thread<FS: Filesystem>(
    ctx: SessionEventLoop<FS>,
    jobs: Arc<Mutex<std::sync::mpsc::Receiver<HeldRequest>>>,
    events: std::sync::mpsc::Sender<Event>,
    panicked: Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<()> {
    loop {
        let held = jobs.lock().recv();
        let Ok(held) = held else {
            return Ok(());
        };
        if panicked.load(Ordering::SeqCst) {
            held.commit().commit_errno(Errno::EIO);
            continue;
        }
        let dispatch = std::panic::AssertUnwindSafe(|| {
            ctx.handle_fetch(held.commit().clone(), held.request())
        });
        if std::panic::catch_unwind(dispatch).is_err() {
            // As on a ring thread: the reply went out as EIO during the unwind, later requests
            // are answered EIO here, and the session ends with the panic
            panicked.store(true, Ordering::SeqCst);
            let _ = events.send(Event::Panicked);
        }
        // Dropping `held` ends the dispatch (`HeldRequest`'s doc), here and not on the ring
    }
}

/// Serves the session over its rings, with one `/dev/fuse` reader for the requests the kernel
/// never sends over a ring, until the connection ends or a thread fails.
///
/// CONSTELLATION PATCH (io-uring): plus `n_threads` offload threads for the requests a ring
/// thread does not dispatch itself (`dispatch_on_ring`), so that a callback that blocks holds
/// up one offload thread, as it holds up one reader of a `/dev/fuse` session, and not a ring.
///
/// The ring threads have no way to learn that the connection ended while every entry they
/// own is held by userspace, so they are told to leave once the `/dev/fuse` reader has seen
/// the end of the connection, and joined only then. A panic or error on any thread ends the
/// session at once, as `join_all` does for `/dev/fuse` sessions; the other threads are left
/// to exit with the connection, which is still alive at that point and is not told to shut
/// down: `Session::run` callers end it by dropping the mount, a `Session::spawn` session
/// keeps it, answering EIO, until the `BackgroundSession` is dropped.
///
/// CONSTELLATION PATCH (io-uring): the offload threads are joined last on the clean path, and
/// on the others left to exit with the ring threads that feed them, for the same reason: an
/// offload thread leaves once every ring thread is gone, a ring thread once the connection
/// ends, and `run`'s caller ends the connection only after `run` returns -- joining them on a
/// panic or error path would hang `run` instead of reporting the failure.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn serve_ring<FS: Filesystem>(
    mut ring: RingSet,
    ch: Channel,
    config: &Config,
    event_loop: impl Fn(String, Channel) -> SessionEventLoop<FS>,
) -> io::Result<()> {
    const DEV: usize = 0;
    if config.clone_fd {
        debug!("clone_fd has no effect with io_uring");
    }
    let (events_tx, events) = std::sync::mpsc::channel();
    let (offload_tx, offload_rx) = std::sync::mpsc::channel::<HeldRequest>();
    let offload_rx = Arc::new(Mutex::new(offload_rx));
    let offload_panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let offload = spawn_named((0..config.n_threads.unwrap_or(1).max(1)).map(|i| {
        let name = format!("fuser-offload-{i}");
        let ctx = event_loop(name.clone(), ch.clone());
        let (jobs, events, panicked) = (
            offload_rx.clone(),
            events_tx.clone(),
            offload_panicked.clone(),
        );
        (name, move || offload_thread(ctx, jobs, events, panicked))
    }));
    drop(offload_rx);
    let offload = match offload {
        Ok(threads) => threads,
        // The ring threads dispatch everything themselves then (`RingHandler::handle`)
        Err(err) => {
            error!("spawning the io_uring offload threads failed ({err}); dispatching on the rings");
            Vec::new()
        }
    };
    ring.serve(|index| {
        Box::new(RingHandler {
            ctx: event_loop(format!("fuser-ring-{index}"), ch.clone()),
            exit: ExitNotice {
                events: events_tx.clone(),
                thread: index + 1,
            },
            panicked: false,
            offload: offload_tx.clone(),
            offload_panicked: offload_panicked.clone(),
        })
    })?;
    // Only the ring threads' handlers hold senders now: the offload threads leave once the
    // last ring thread is gone and its queued requests are served.
    drop(offload_tx);
    let dev = spawn_named([("fuser-dev".to_string(), {
        let event_loop = event_loop("fuser-dev".to_string(), ch);
        let exit = ExitNotice {
            events: events_tx,
            thread: DEV,
        };
        move || {
            let _exit = exit;
            // CONSTELLATION PATCH (io-uring): a ring session is never armed
            // for a detach, so its reader only ever ends with the
            // connection. 0.18.0's loop treats only `ENODEV` as that end,
            // yet it negotiates `FUSE_ABORT_ERROR`, under which the kernel
            // reports an administratively aborted connection (fusectl's
            // `abort`) as `ECONNABORTED` -- still the end of the
            // connection, not a failure of the session. Upstream fixed
            // this after 0.18.0 (fuser issue #212); until a re-vendor
            // brings that in, the ring transport's own teardown path needs
            // it, so it is corrected here rather than in the shared loop,
            // which leaves every `/dev/fuse` session byte-for-byte as it
            // was.
            match event_loop.event_loop() {
                Ok(_) => Ok(()),
                Err(err) if err.raw_os_error() == Some(nix::errno::Errno::ECONNABORTED as i32) => {
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
    })])?;
    let mut threads: Vec<Option<JoinHandle<io::Result<()>>>> = dev
        .into_iter()
        .chain(ring.take_threads())
        .map(Some)
        .collect();
    loop {
        let event = events
            .recv()
            .map_err(|_| io::Error::other("BUG: every session thread exited unnoticed"))?;
        match event {
            Event::Panicked => return Err(io::Error::other(THREAD_PANICKED)),
            Event::Exited(DEV) => {
                join_all(threads[DEV].take())?;
                ring.shutdown();
                join_all(threads.into_iter().flatten())?;
                return join_all(offload);
            }
            // A ring leaving cleanly before the reader means the connection is ending
            Event::Exited(thread) => join_all(threads[thread].take())?,
        }
    }
}

/// CONSTELLATION PATCH (io-uring): a spawn failure returns at once; the
/// threads spawned before it keep running detached. Only a ring session
/// spawns through this; upstream's `/dev/fuse` loop is untouched.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
pub(crate) fn spawn_named<F>(
    threads: impl IntoIterator<Item = (String, F)>,
) -> io::Result<Vec<JoinHandle<io::Result<()>>>>
where
    F: FnOnce() -> io::Result<()> + Send + 'static,
{
    threads
        .into_iter()
        .map(|(name, body)| thread::Builder::new().name(name).spawn(body))
        .collect()
}

/// CONSTELLATION PATCH (io-uring): first error wins. A panicked thread ends
/// the join at once, leaving later threads detached.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
fn join_all(threads: impl IntoIterator<Item = JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut reply: io::Result<()> = Ok(());
    for thread in threads {
        let res = match thread.join() {
            Ok(res) => res,
            Err(_) => {
                return Err(io::Error::other(THREAD_PANICKED));
            }
        };
        if let Err(e) = res {
            if reply.is_ok() {
                reply = Err(e);
            }
        }
    }
    reply
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
                // CONSTELLATION PATCH (io-uring): the reply prototype names
                // the transport the request arrived on.
                Ok(Some(size)) => match RequestWithSender::new(
                    ReplySender::Channel(self.ch.sender()),
                    &buf[..size],
                ) {
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

    /// Dispatches one request fetched over a ring; the ring thread answers a request the
    /// filesystem was not given a reply object for.
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    fn handle_fetch(&self, commit: RingCommit, request: &[u8]) {
        let request = match ll::AnyRequest::try_from(request) {
            Ok(request) => request,
            // Unlike a `/dev/fuse` stream, one unparsable request leaves the ring usable
            Err(err) => {
                error!("{err}");
                return commit.commit_errno(Errno::EIO);
            }
        };
        // CONSTELLATION PATCH (io-uring): on the ring thread for what `dispatch_on_ring`
        // keeps there, on an offload thread for the rest (`offload_thread`).
        let req = RequestWithSender::from_request(ReplySender::Ring(commit), request);
        if let Ok(Operation::Destroy(_)) = req.request.operation() {
            req.reply::<ReplyEmpty>().ok();
        } else {
            req.dispatch(self)
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

// CONSTELLATION PATCH (io-uring): `spawn_named`/`join_all`, the ring
// session's thread supervisor.
#[cfg(all(test, feature = "io-uring", target_os = "linux"))]
mod thread_test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::*;

    /// Distinct closures have distinct types, so a mixed list needs boxing
    type Body = Box<dyn FnOnce() -> io::Result<()> + Send>;

    #[test]
    fn join_all_reports_the_first_error_after_joining_every_thread() {
        let done = Arc::new(AtomicUsize::new(0));
        let body = |result: io::Result<()>, delay: u64| -> Body {
            let done = done.clone();
            Box::new(move || {
                thread::sleep(Duration::from_millis(delay));
                done.fetch_add(1, Ordering::SeqCst);
                result
            })
        };
        let mut threads = spawn_named([
            ("a".to_string(), body(Err(io::Error::other("first")), 0)),
            ("b".to_string(), body(Err(io::Error::other("second")), 50)),
            ("c".to_string(), body(Ok(()), 0)),
        ])
        .unwrap();
        threads.push(thread::spawn(body(
            Err(io::Error::other("pre-spawned")),
            100,
        )));

        let err = join_all(threads).unwrap_err();
        assert_eq!(err.to_string(), "first");
        assert_eq!(
            done.load(Ordering::SeqCst),
            4,
            "every thread must be joined"
        );
    }

    #[test]
    fn join_all_reports_a_panic() {
        let threads = spawn_named([("p".to_string(), || -> io::Result<()> {
            panic!("deliberate panic from a test thread")
        })])
        .unwrap();
        let err = join_all(threads).unwrap_err();
        assert_eq!(err.to_string(), "event loop thread panicked");
    }

    #[test]
    fn spawn_named_names_the_threads() {
        let threads = spawn_named([("fuser-99".to_string(), || {
            if thread::current().name() == Some("fuser-99") {
                Ok(())
            } else {
                Err(io::Error::other("wrong thread name"))
            }
        })])
        .unwrap();
        join_all(threads).unwrap();
    }
}

// CONSTELLATION PATCH (io-uring): upstream 0.18.0 has no test module in
// `session.rs`; this one carries the ring-relevant tests of the Skory fork's
// own suite that do not depend on its other, unvendored changes, plus the
// `fusectl` helpers `uring_test` needs.
#[cfg(all(test, target_os = "linux"))]
mod test {
    use std::fs::File;
    use std::mem::ManuallyDrop;

    use super::*;

    /// Answers every operation with an error; enough to mount and unmount.
    struct AbortErrorFs;

    impl Filesystem for AbortErrorFs {}

    #[test]
    fn zero_threads_are_refused_before_mounting() {
        let config = Config {
            n_threads: Some(0),
            ..Config::default()
        };
        let err = validate_transport(&config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), "n_threads must be at least 1");
        let tmp = tempfile::tempdir().unwrap();
        let err = Session::new(AbortErrorFs, tmp.path(), &config)
            .err()
            .expect("n_threads == 0 must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap();
        assert!(!mounts.contains(tmp.path().to_str().unwrap()));
        let fd = OwnedFd::from(File::open("/dev/null").unwrap());
        let err = Session::from_fd(AbortErrorFs, fd, SessionACL::Owner, config)
            .err()
            .expect("n_threads == 0 must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    #[cfg(not(feature = "io-uring"))]
    fn io_uring_without_the_feature_is_refused() {
        let config = Config {
            io_uring: true,
            ..Config::default()
        };
        assert!(validate_transport(&Config::default()).is_ok());
        let err = validate_transport(&config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(
            err.to_string(),
            "io_uring transport requires the io-uring cargo feature"
        );

        let tmp = tempfile::tempdir().unwrap();
        let err = Session::new(AbortErrorFs, tmp.path(), &config)
            .err()
            .expect("io_uring without the feature must be refused");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap();
        assert!(!mounts.contains(tmp.path().to_str().unwrap()));
        let fd = OwnedFd::from(File::open("/dev/null").unwrap());
        let err = Session::from_fd(AbortErrorFs, fd, SessionACL::Owner, config)
            .err()
            .expect("io_uring without the feature must be refused");
        assert_eq!(
            err.to_string(),
            "io_uring transport requires the io-uring cargo feature"
        );
    }

    /// The filesystem is still destroyed exactly once and unmounted
    #[test]
    fn panic_in_callback_ends_the_session() {
        struct PanicFs(Arc<std::sync::atomic::AtomicUsize>);
        impl Filesystem for PanicFs {
            fn getattr(
                &self,
                _req: &Request,
                _ino: crate::INodeNo,
                _fh: Option<crate::FileHandle>,
                _reply: crate::ReplyAttr,
            ) {
                panic!("deliberate panic from a test filesystem's getattr()");
            }
            fn destroy(&mut self) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let tmp = ManuallyDrop::new(tempfile::tempdir().unwrap());
        let mountpoint = tmp.path().canonicalize().unwrap();
        let destroyed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let session =
            Session::new(PanicFs(destroyed.clone()), &mountpoint, &Config::default()).unwrap();
        let runner = std::thread::spawn(move || session.run());
        // The dropped reply answers with EIO, so this returns once the panic has happened
        assert!(std::fs::metadata(&mountpoint).is_err());

        let err = runner.join().unwrap().unwrap_err();
        assert_eq!(err.to_string(), "event loop thread panicked");
        assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap();
        assert!(
            !mounts
                .lines()
                .any(|l| l.split(' ').nth(1) == mountpoint.to_str())
        );
        ManuallyDrop::into_inner(tmp);
    }

    /// A root with one 5000-byte file whose `read` answers through `ReplyData::fill`
    struct FillFs {
        foreign: bool,
        panic: bool,
        destroyed: Arc<std::sync::atomic::AtomicUsize>,
    }

    const FILL_FILE_LEN: usize = 5000;

    fn fill_byte(offset: usize) -> u8 {
        (offset % 251) as u8
    }

    impl FillFs {
        fn attr(ino: crate::INodeNo) -> crate::FileAttr {
            let (kind, size, perm) = match ino.0 {
                2 => (crate::FileType::RegularFile, FILL_FILE_LEN as u64, 0o644),
                _ => (crate::FileType::Directory, 0, 0o755),
            };
            crate::FileAttr {
                ino,
                size,
                blocks: size.div_ceil(512),
                atime: std::time::SystemTime::UNIX_EPOCH,
                mtime: std::time::SystemTime::UNIX_EPOCH,
                ctime: std::time::SystemTime::UNIX_EPOCH,
                crtime: std::time::SystemTime::UNIX_EPOCH,
                kind,
                perm,
                nlink: 1,
                uid: geteuid().as_raw(),
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            }
        }
    }

    impl Filesystem for FillFs {
        fn destroy(&mut self) {
            self.destroyed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fn lookup(
            &self,
            _req: &Request,
            _parent: crate::INodeNo,
            name: &std::ffi::OsStr,
            reply: crate::ReplyEntry,
        ) {
            if name == "data.bin" {
                reply.entry(
                    &std::time::Duration::ZERO,
                    &Self::attr(crate::INodeNo(2)),
                    crate::Generation(0),
                );
            } else {
                reply.error(Errno::ENOENT);
            }
        }
        fn getattr(
            &self,
            _req: &Request,
            ino: crate::INodeNo,
            _fh: Option<crate::FileHandle>,
            reply: crate::ReplyAttr,
        ) {
            reply.attr(&std::time::Duration::ZERO, &Self::attr(ino));
        }
        /// Direct I/O without flush, so a `read(2)` is exactly one READ and `close(2)` sends
        /// nothing synchronous: once the panicking session's only thread is gone, any further
        /// request (the synchronous READ after a failed readahead, the FLUSH on close) would
        /// block the reader until the connection is aborted
        fn open(
            &self,
            _req: &Request,
            _ino: crate::INodeNo,
            _flags: crate::OpenFlags,
            reply: crate::ReplyOpen,
        ) {
            reply.opened(
                crate::FileHandle(0),
                crate::FopenFlags::FOPEN_DIRECT_IO | crate::FopenFlags::FOPEN_NOFLUSH,
            );
        }
        fn read(
            &self,
            _req: &Request,
            _ino: crate::INodeNo,
            _fh: crate::FileHandle,
            offset: u64,
            size: u32,
            _flags: crate::OpenFlags,
            _lock_owner: Option<crate::LockOwner>,
            reply: crate::ReplyData,
        ) {
            let panic = self.panic;
            let answer = move |buf: &mut [u8]| -> Result<usize, Errno> {
                if panic {
                    panic!("deliberate panic from a test filesystem's fill closure");
                }
                assert_eq!(buf.len(), size as usize);
                let start = (offset as usize).min(FILL_FILE_LEN);
                let end = start.saturating_add(size as usize).min(FILL_FILE_LEN);
                for (i, b) in buf[..end - start].iter_mut().enumerate() {
                    *b = fill_byte(start + i);
                }
                Ok(end - start)
            };
            if self.foreign {
                std::thread::spawn(move || reply.fill(size as usize, answer));
            } else {
                reply.fill(size as usize, answer);
            }
        }
    }

    #[test]
    fn fill_serves_reads() {
        use std::os::unix::fs::FileExt;

        for foreign in [false, true] {
            let tmp = ManuallyDrop::new(tempfile::tempdir().unwrap());
            let mountpoint = tmp.path().canonicalize().unwrap();
            let fs = FillFs {
                foreign,
                panic: false,
                destroyed: Arc::default(),
            };
            let session = Session::new(fs, &mountpoint, &Config::default()).unwrap();
            let bg = session.spawn().unwrap();
            let path = mountpoint.join("data.bin");
            let data = std::fs::read(&path).unwrap();
            assert_eq!(data.len(), FILL_FILE_LEN, "foreign={foreign}");
            assert!(data.iter().enumerate().all(|(i, b)| *b == fill_byte(i)));
            let file = std::fs::File::open(&path).unwrap();
            let mut tail = [0u8; 64];
            let n = file.read_at(&mut tail, FILL_FILE_LEN as u64 - 10).unwrap();
            assert_eq!(n, 10, "foreign={foreign}");
            assert!(
                tail[..n]
                    .iter()
                    .enumerate()
                    .all(|(i, b)| *b == fill_byte(FILL_FILE_LEN - 10 + i))
            );
            drop(file);
            bg.umount_and_join().unwrap();
            ManuallyDrop::into_inner(tmp);
        }
    }

    #[test]
    fn panic_in_fill_answers_eio_and_ends_the_session() {
        let tmp = ManuallyDrop::new(tempfile::tempdir().unwrap());
        let mountpoint = tmp.path().canonicalize().unwrap();
        let destroyed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fs = FillFs {
            foreign: false,
            panic: true,
            destroyed: destroyed.clone(),
        };
        let session = Session::new(fs, &mountpoint, &Config::default()).unwrap();
        let runner = std::thread::spawn(move || session.run());
        let err = std::fs::read(mountpoint.join("data.bin")).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));

        let err = runner.join().unwrap().unwrap_err();
        assert_eq!(err.to_string(), "event loop thread panicked");
        assert_eq!(destroyed.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap();
        assert!(
            !mounts
                .lines()
                .any(|l| l.split(' ').nth(1) == mountpoint.to_str())
        );
        ManuallyDrop::into_inner(tmp);
    }

    /// Mounts fusectl when root finds it missing and unmounts it when the last holder drops;
    /// the tests that abort a connection run concurrently on hosts that do not mount it
    pub(super) struct Fusectl;

    const FUSECTL: &str = "/sys/fs/fuse/connections";

    /// Live `Fusectl` values and whether one of them mounted fusectl
    static FUSECTL_HOLDERS: Mutex<(usize, bool)> = Mutex::new((0, false));

    impl Fusectl {
        pub(super) fn ensure() -> Option<Self> {
            let mut holders = FUSECTL_HOLDERS.lock();
            let mounted = std::fs::read_to_string("/proc/self/mounts")
                .is_ok_and(|m| m.lines().any(|l| l.split(' ').nth(1) == Some(FUSECTL)));
            if !mounted {
                if !geteuid().is_root() {
                    return None;
                }
                nix::mount::mount(
                    None::<&str>,
                    FUSECTL,
                    Some("fusectl"),
                    nix::mount::MsFlags::empty(),
                    None::<&str>,
                )
                .ok()?;
                holders.1 = true;
            }
            holders.0 += 1;
            Some(Self)
        }
    }

    impl Drop for Fusectl {
        fn drop(&mut self) {
            let mut holders = FUSECTL_HOLDERS.lock();
            holders.0 -= 1;
            if holders.0 == 0 && holders.1 {
                holders.1 = false;
                if let Err(err) = nix::mount::umount(FUSECTL) {
                    eprintln!("cannot unmount the fusectl the tests mounted: {err}");
                }
            }
        }
    }

    /// The fusectl abort file for the FUSE mount at `mountpoint`: the connection
    /// directory is named after the mount's anonymous device number.
    pub(super) fn fusectl_abort_path(mountpoint: &Path) -> Option<std::path::PathBuf> {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
        let mut device = None;
        for line in mountinfo.lines() {
            let mut fields = line.split(' ');
            let (Some(dev), Some(path)) = (fields.nth(2), fields.nth(1)) else {
                continue;
            };
            // mountinfo octal-escapes special characters; the tempdir path has none.
            // Later entries are mounted on top of earlier ones, so the last match wins
            if Path::new(path) == mountpoint {
                let (major, minor) = dev.split_once(':')?;
                let (major, minor): (u64, u64) = (major.parse().ok()?, minor.parse().ok()?);
                // fusectl names the directory with the raw kernel-internal
                // device number, (major << 20) | minor: fuse_ctl_add_conn()
                // prints fc->dev (= sb->s_dev) without re-encoding it
                device = Some((major << 20) | minor);
            }
        }
        let path = std::path::PathBuf::from(format!("/sys/fs/fuse/connections/{}/abort", device?));
        path.exists().then_some(path)
    }
}

/// Runtime tests of the io_uring transport against the kernel. They skip unless the fuse module
/// has `enable_uring=Y`, and are serialized because they inspect this process's threads and
/// captured log lines.
#[cfg(all(test, feature = "io-uring", target_os = "linux"))]
mod uring_test {
    use std::ffi::OsStr;
    use std::io::Read;
    use std::io::Write;
    use std::mem::ManuallyDrop;
    use std::os::unix::ffi::OsStrExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::Duration;
    use std::time::Instant;
    use std::time::SystemTime;

    use super::*;
    use crate::FileAttr;
    use crate::FileHandle;
    use crate::FileType;
    use crate::Generation;
    use crate::INodeNo;
    use crate::ReplyAttr;
    use crate::ReplyData;
    use crate::ReplyDirectory;
    use crate::ReplyEntry;
    use crate::ReplyWrite;
    use crate::ReplyXattr;
    use crate::ll::ResponseEmpty;
    use crate::uring::ring::RingIo;

    /// Records every log line with the writing thread's name; the ring tests hold `SERIAL`
    /// while they read it, so the ring lines they see are their own
    struct CaptureLogger;

    struct Line {
        thread: String,
        level: log::Level,
        text: String,
    }

    static LINES: Mutex<Vec<Line>> = Mutex::new(Vec::new());
    static SERIAL: Mutex<()> = Mutex::new(());

    impl log::Log for CaptureLogger {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            true
        }
        fn log(&self, record: &log::Record<'_>) {
            let text = format!("{}", record.args());
            let thread = thread::current().name().unwrap_or("?").to_string();
            if std::env::var_os("RUST_LOG").is_some() {
                eprintln!("[{} {thread} {}] {text}", record.level(), record.target());
            }
            LINES.lock().push(Line {
                thread,
                level: record.level(),
                text,
            });
        }
        fn flush(&self) {}
    }

    /// Serializes a ring test and starts it with an empty log
    fn serial() -> parking_lot::MutexGuard<'static, ()> {
        static LOGGER: CaptureLogger = CaptureLogger;
        let guard = SERIAL.lock();
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Debug);
        }
        LINES.lock().clear();
        guard
    }

    fn logged(level: log::Level, text: &str) -> Vec<String> {
        logged_by("", level, text)
    }

    /// Lines written by threads whose name starts with `thread`; a line without a ring-specific
    /// token is scoped this way, since the `/dev/fuse` session tests run alongside
    fn logged_by(thread: &str, level: log::Level, text: &str) -> Vec<String> {
        LINES
            .lock()
            .iter()
            .filter(|l| l.thread.starts_with(thread) && l.level == level && l.text.contains(text))
            .map(|l| l.text.clone())
            .collect()
    }

    /// Lines a thread other than the test's may still be about to write: waits for `n` of them
    fn wait_logged(level: log::Level, text: &str, n: usize) -> Vec<String> {
        wait_logged_by("", level, text, n)
    }

    fn wait_logged_by(thread: &str, level: log::Level, text: &str, n: usize) -> Vec<String> {
        wait_until(|| logged_by(thread, level, text).len() >= n);
        logged_by(thread, level, text)
    }

    fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !cond() {
            if Instant::now() > deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(5));
        }
        true
    }

    fn session_log() -> String {
        LINES
            .lock()
            .iter()
            .filter(|l| l.thread.starts_with("fuser-"))
            .fold(String::new(), |mut log, l| {
                log += &format!("[{} {}] {}\n", l.level, l.thread, l.text);
                log
            })
    }

    /// The crate's own host probe, which is what a caller's tests probe with
    /// ([`crate::uring_unavailable`]): one implementation, so a skip here and a
    /// skip there can never disagree.
    fn uring_unavailable() -> Option<String> {
        crate::uring::uring_unavailable()
    }

    fn thread_names() -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
            .map(|n| n.trim().to_string())
            .collect();
        names.sort();
        names
    }

    fn count_threads(prefix: &str) -> usize {
        thread_names()
            .iter()
            .filter(|n| n.starts_with(prefix))
            .count()
    }

    fn assert_threads(prefix: &str, n: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while count_threads(prefix) != n {
            assert!(
                Instant::now() < deadline,
                "expected {n} {prefix} threads, have {:?}",
                thread_names()
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_ring_threads_gone(timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while count_threads("fuser-ring-") + count_threads("fuser-dev") > 0 {
            if Instant::now() > deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(10));
        }
        true
    }

    fn assert_not_mounted(mountpoint: &Path) {
        let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap();
        assert!(
            !mounts
                .lines()
                .any(|l| l.split(' ').nth(1) == mountpoint.to_str()),
            "{} is still mounted:\n{mounts}",
            mountpoint.display()
        );
    }

    fn ring_config() -> Config {
        Config {
            io_uring: true,
            ..Config::default()
        }
    }

    /// What a ring of `depth` entries per queue reserves with the default 16 MiB `max_write`,
    /// to tell this session's mapping apart from the ring unit tests' fake rings in the log
    fn reserved_bytes(depth: usize) -> usize {
        usize::from(crate::uring::possible_cpus().unwrap())
            * depth
            * (page_size::get() + MAX_WRITE_SIZE)
    }

    const HELLO_INO: INodeNo = INodeNo(2);
    const HELLO: &[u8] = b"Hello World!\n";
    const BIG_INO: INodeNo = INodeNo(3);
    const BIG_LEN: usize = 4 << 20;
    const OTHER_INO: INodeNo = INodeNo(4);
    const OTHER: &[u8] = b"other\n";
    const XATTR: &[u8] = b"xattr value";

    fn big_byte(offset: usize) -> u8 {
        (offset % 251) as u8
    }

    /// A root with `hello.txt`, `other.txt` and a 4 MiB `big.bin`, attributes uncached so that
    /// every `stat` reaches it; the switches make it reply from elsewhere, or not at all
    #[derive(Default)]
    struct RingFs {
        /// `read` and `getattr` reply from a spawned thread that exits right after
        foreign: bool,
        /// How long `read` of `hello.txt` blocks the calling thread before returning
        park_hello: Duration,
        /// How long the spawned replier of `other.txt` waits before replying
        delay_other: Duration,
        /// The spawned replier of `other.txt` also waits for this, which `read` of
        /// `hello.txt` releases before it parks
        other_gate: Mutex<Option<mpsc::Receiver<()>>>,
        release_other: Option<mpsc::Sender<()>>,
        /// The replier of `other.txt` made its reply
        other_replied: Option<mpsc::Sender<Instant>>,
        /// Every `getattr` is kept here unanswered
        hold_getattr: Option<Arc<Mutex<Vec<ReplyAttr>>>>,
        /// `getattr` panics
        panic_getattr: bool,
        /// `read` answers with `ReplyData::fill` instead of `data`
        fill: bool,
        /// While set, `read` panics inside its `fill` closure, after touching the buffer
        panic_fill: Arc<AtomicBool>,
        /// How many times a `fill` closure of `read` ran
        fills: Arc<AtomicUsize>,
        /// `init` negotiates `FUSE_ABORT_ERROR`
        abort_error: bool,
        /// `read` of `hello.txt` was dispatched
        hello_read: Option<mpsc::Sender<Instant>>,
        /// `read` of `other.txt` was dispatched
        other_read: Option<mpsc::Sender<Instant>>,
        /// `unlink` hands its reply to the test instead of answering
        unlink_reply: Option<mpsc::Sender<ReplyEmpty>>,
        destroyed: Arc<AtomicUsize>,
    }

    /// The name of every thread `RingFs` replies from, so log lines can be scoped to them
    const REPLIER: &str = "ringfs-replier";

    fn replier(reply: impl FnOnce() + Send + 'static) {
        thread::Builder::new()
            .name(REPLIER.to_string())
            .spawn(reply)
            .unwrap();
    }

    impl RingFs {
        fn attr(ino: INodeNo) -> FileAttr {
            let (kind, size, perm) = match ino {
                HELLO_INO => (FileType::RegularFile, HELLO.len() as u64, 0o644),
                BIG_INO => (FileType::RegularFile, BIG_LEN as u64, 0o644),
                OTHER_INO => (FileType::RegularFile, OTHER.len() as u64, 0o644),
                _ => (FileType::Directory, 0, 0o755),
            };
            FileAttr {
                ino,
                size,
                blocks: size.div_ceil(512),
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                crtime: SystemTime::UNIX_EPOCH,
                kind,
                perm,
                nlink: 1,
                uid: geteuid().as_raw(),
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            }
        }

        fn ino_of(name: &OsStr) -> Option<INodeNo> {
            match name.as_bytes() {
                b"hello.txt" => Some(HELLO_INO),
                b"big.bin" => Some(BIG_INO),
                b"other.txt" => Some(OTHER_INO),
                _ => None,
            }
        }

        fn content(ino: INodeNo, offset: u64, size: u32) -> Vec<u8> {
            let bytes: Vec<u8> = match ino {
                HELLO_INO => HELLO.to_vec(),
                OTHER_INO => OTHER.to_vec(),
                BIG_INO => (0..BIG_LEN).map(big_byte).collect(),
                _ => Vec::new(),
            };
            let start = (offset as usize).min(bytes.len());
            let end = start.saturating_add(size as usize).min(bytes.len());
            bytes[start..end].to_vec()
        }
    }

    impl Filesystem for RingFs {
        fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> io::Result<()> {
            if self.abort_error {
                let _ = config.add_capabilities(InitFlags::FUSE_ABORT_ERROR);
            }
            Ok(())
        }
        fn destroy(&mut self) {
            self.destroyed.fetch_add(1, Ordering::SeqCst);
        }
        fn lookup(&self, _req: &Request, _parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
            match Self::ino_of(name) {
                Some(ino) => reply.entry(&Duration::ZERO, &Self::attr(ino), Generation(0)),
                None => reply.error(Errno::ENOENT),
            }
        }
        fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
            if self.panic_getattr {
                panic!("deliberate panic from a test filesystem's getattr()");
            }
            if let Some(held) = &self.hold_getattr {
                held.lock().push(reply);
                return;
            }
            if self.foreign {
                thread::spawn(move || reply.attr(&Duration::ZERO, &Self::attr(ino)));
            } else {
                reply.attr(&Duration::ZERO, &Self::attr(ino));
            }
        }
        fn read(
            &self,
            _req: &Request,
            ino: INodeNo,
            _fh: FileHandle,
            offset: u64,
            size: u32,
            _flags: crate::OpenFlags,
            _lock_owner: Option<crate::LockOwner>,
            reply: ReplyData,
        ) {
            let data = Self::content(ino, offset, size);
            if ino == HELLO_INO {
                if let Some(tx) = &self.hello_read {
                    tx.send(Instant::now()).unwrap();
                }
                if let Some(tx) = &self.release_other {
                    tx.send(()).unwrap();
                }
                thread::sleep(self.park_hello);
            }
            if ino == OTHER_INO {
                if let Some(tx) = &self.other_read {
                    tx.send(Instant::now()).unwrap();
                }
                let delay = self.delay_other;
                let gate = self.other_gate.lock().take();
                let replied = self.other_replied.clone();
                thread::spawn(move || {
                    thread::sleep(delay);
                    if let Some(gate) = gate {
                        gate.recv().unwrap();
                    }
                    reply.data(&data);
                    if let Some(tx) = replied {
                        tx.send(Instant::now()).unwrap();
                    }
                });
            } else if self.panic_fill.load(Ordering::SeqCst) {
                let answer = |buf: &mut [u8]| -> Result<usize, Errno> {
                    buf.fill(0xAB);
                    panic!("deliberate panic from a test filesystem's fill closure");
                };
                if self.foreign {
                    replier(move || reply.fill(size as usize, answer));
                } else {
                    reply.fill(size as usize, answer);
                }
            } else if self.fill {
                // The buffer is the size asked for; the file may end before that
                let fills = self.fills.clone();
                let answer = move |buf: &mut [u8]| {
                    fills.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(buf.len(), size as usize);
                    buf[..data.len()].copy_from_slice(&data);
                    Ok(data.len())
                };
                if self.foreign {
                    replier(move || reply.fill(size as usize, answer));
                } else {
                    reply.fill(size as usize, answer);
                }
            } else if self.foreign {
                replier(move || reply.data(&data));
            } else {
                reply.data(&data);
            }
        }
        fn write(
            &self,
            _req: &Request,
            _ino: INodeNo,
            _fh: FileHandle,
            _offset: u64,
            data: &[u8],
            _write_flags: crate::WriteFlags,
            _flags: crate::OpenFlags,
            _lock_owner: Option<crate::LockOwner>,
            reply: ReplyWrite,
        ) {
            reply.written(data.len() as u32);
        }
        fn readdir(
            &self,
            _req: &Request,
            _ino: INodeNo,
            _fh: FileHandle,
            offset: u64,
            mut reply: ReplyDirectory,
        ) {
            let entries = [
                (INodeNo(1), FileType::Directory, "."),
                (INodeNo(1), FileType::Directory, ".."),
                (HELLO_INO, FileType::RegularFile, "hello.txt"),
                (BIG_INO, FileType::RegularFile, "big.bin"),
                (OTHER_INO, FileType::RegularFile, "other.txt"),
            ];
            for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(ino, i as u64 + 1, kind, name) {
                    break;
                }
            }
            reply.ok();
        }
        fn getxattr(
            &self,
            _req: &Request,
            _ino: INodeNo,
            name: &OsStr,
            size: u32,
            reply: ReplyXattr,
        ) {
            if name.as_bytes() != b"user.test" {
                return reply.error(Errno::ENODATA);
            }
            if size == 0 {
                reply.size(XATTR.len() as u32);
            } else {
                reply.data(XATTR);
            }
        }
        fn unlink(&self, _req: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
            match &self.unlink_reply {
                Some(tx) => tx.send(reply).unwrap(),
                None => reply.ok(),
            }
        }
        /// Never answered: the dropped reply must turn into EIO
        fn setattr(
            &self,
            _req: &Request,
            _ino: INodeNo,
            _mode: Option<u32>,
            _uid: Option<u32>,
            _gid: Option<u32>,
            _size: Option<u64>,
            _atime: Option<crate::TimeOrNow>,
            _mtime: Option<crate::TimeOrNow>,
            _ctime: Option<SystemTime>,
            _fh: Option<FileHandle>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            _flags: Option<crate::BsdFileFlags>,
            _reply: ReplyAttr,
        ) {
        }
    }

    struct Mounted {
        tmp: Option<tempfile::TempDir>,
        mountpoint: std::path::PathBuf,
    }

    impl Drop for Mounted {
        /// Only a failed test gets here with the directory still held: end the connection so
        /// no client stays blocked in the mount, detach it and leave the directory behind
        fn drop(&mut self) {
            if let Some(tmp) = self.tmp.take() {
                if let Some(abort) = super::test::fusectl_abort_path(&self.mountpoint) {
                    let _ = std::fs::write(abort, b"1");
                }
                let _ = nix::mount::umount2(&self.mountpoint, nix::mount::MntFlags::MNT_DETACH);
                std::mem::forget(tmp);
            }
        }
    }

    impl Mounted {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let mountpoint = tmp.path().canonicalize().unwrap();
            Self {
                tmp: Some(tmp),
                mountpoint,
            }
        }

        fn session(&self, fs: RingFs, config: &Config) -> Session<RingFs> {
            let started = Instant::now();
            let session = Session::new(fs, &self.mountpoint, config).unwrap();
            eprintln!("Session::new with io_uring took {:?}", started.elapsed());
            assert!(session.ring.is_some(), "the kernel offered the ring");
            assert!(
                session
                    .negotiated
                    .unwrap()
                    .flags()
                    .contains(InitFlags::FUSE_OVER_IO_URING)
            );
            session
        }

        fn path(&self, name: &str) -> std::path::PathBuf {
            self.mountpoint.join(name)
        }

        fn finish(mut self) {
            assert!(
                wait_ring_threads_gone(Duration::from_secs(5)),
                "ring threads still running: {:?}",
                thread_names()
            );
            assert_not_mounted(&self.mountpoint);
            drop(self.tmp.take());
        }
    }

    /// `umount_and_join` that fails instead of wedging the test binary when teardown hangs
    fn umount_and_join_within(bg: BackgroundSession, timeout: Duration) -> io::Result<()> {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(bg.umount_and_join());
        });
        rx.recv_timeout(timeout)
            .unwrap_or_else(|_| panic!("umount_and_join did not return within {timeout:?}"))
    }

    /// CONSTELLATION PATCH (io-uring): plan 38 §6 -- the translation layer
    /// (fuser decode -> `Filesystem` call -> reply object -> fuser encode)
    /// must produce the **same bytes** whichever transport carries them,
    /// because the only thing that differs is where the reply is written: a
    /// `writev(2)` on `/dev/fuse`, or the fetched entry's own payload over
    /// a ring. A transport that answered `LOOKUP` differently from the
    /// other would be a compliance difference pjdfstest could only find on
    /// a kernel that offers the ring; this finds it anywhere.
    ///
    /// No kernel, no io_uring and no root: the `/dev/fuse` leg writes into
    /// a `SOCK_DGRAM` socket pair (`crates/frontend-fuse/tests/wire.rs`'s
    /// trick -- fuser's channel accepts any descriptor and the kernel's
    /// side of the protocol is whole messages), and the ring leg writes
    /// into an entry in an ordinary private mapping whose commit SQE is
    /// never submitted (`uring::ring::test::dispatch_over_a_fake_ring`).
    /// So it runs on a `fuse.enable_uring=N` host too, which is the point:
    /// the 18 real-kernel ring tests below skip there.
    #[test]
    fn transport_parity_is_byte_for_byte() {
        use crate::ll::fuse_abi::fuse_opcode;
        use crate::uring::ring::test::dispatch_over_a_fake_ring;
        use crate::uring::staging::test::in_header;
        use std::os::unix::net::UnixDatagram;

        const IN_HEADER_SZ: usize = size_of::<crate::ll::fuse_abi::fuse_in_header>();
        const OUT_HEADER_SZ: usize = size_of::<crate::ll::fuse_abi::fuse_out_header>();

        /// An event loop over `sock`, which stands in for `/dev/fuse`.
        fn event_loop(fs: RingFs, sock: UnixDatagram) -> SessionEventLoop<RingFs> {
            let device = Arc::new(crate::dev_fuse::DevFuse(std::fs::File::from(
                std::os::fd::OwnedFd::from(sock),
            )));
            SessionEventLoop {
                thread_name: "parity".to_string(),
                ch: Channel::new(device),
                filesystem: Arc::new(FilesystemHolder { fs: Some(fs) }),
                allowed: SessionACL::All,
                session_owner: geteuid(),
                detach: None,
            }
        }

        // One request, in the one shape both legs are given it in.
        let request = |opcode: u32, unique: u64, nodeid: u64, op_in: &[u8], payload: &[u8]| {
            let len = (IN_HEADER_SZ + op_in.len() + payload.len()) as u32;
            let mut bytes = in_header(len, opcode, unique).to_vec();
            // `in_header` zeroes everything but len/opcode/unique; the ino
            // the op addresses is the header's `nodeid` (offset 16).
            bytes[16..24].copy_from_slice(&nodeid.to_le_bytes());
            bytes.extend_from_slice(op_in);
            bytes.extend_from_slice(payload);
            bytes
        };

        // `fuse_read_in` / `fuse_write_in` / `fuse_getattr_in`, built by
        // hand in the kernel's little-endian layouts (the test is about
        // the *reply* bytes; the request layouts are asserted against the
        // ABI by `uring::mem`'s own size tests).
        let read_in = |fh: u64, offset: u64, size: u32| {
            let mut b = Vec::new();
            b.extend_from_slice(&fh.to_le_bytes());
            b.extend_from_slice(&offset.to_le_bytes());
            b.extend_from_slice(&size.to_le_bytes());
            b.extend_from_slice(&0u32.to_le_bytes()); // read_flags
            b.extend_from_slice(&0u64.to_le_bytes()); // lock_owner
            b.extend_from_slice(&0u32.to_le_bytes()); // flags
            b.extend_from_slice(&0u32.to_le_bytes()); // padding
            b
        };
        let write_in = |fh: u64, offset: u64, size: u32| {
            let mut b = Vec::new();
            b.extend_from_slice(&fh.to_le_bytes());
            b.extend_from_slice(&offset.to_le_bytes());
            b.extend_from_slice(&size.to_le_bytes());
            b.extend_from_slice(&0u32.to_le_bytes()); // write_flags
            b.extend_from_slice(&0u64.to_le_bytes()); // lock_owner
            b.extend_from_slice(&0u32.to_le_bytes()); // flags
            b.extend_from_slice(&0u32.to_le_bytes()); // padding
            b
        };
        let getattr_in = |fh: Option<u64>| {
            let mut b = Vec::new();
            b.extend_from_slice(&(if fh.is_some() { 1u32 } else { 0 }).to_le_bytes());
            b.extend_from_slice(&0u32.to_le_bytes()); // dummy
            b.extend_from_slice(&fh.unwrap_or(0).to_le_bytes());
            b
        };

        // (name, opcode, unique, nodeid, op_in, payload, whether `read` answers with `fill`)
        let cases: Vec<(&str, u32, u64, u64, Vec<u8>, Vec<u8>, bool)> = vec![
            // A found and a missing LOOKUP: an entry reply and an errno
            // reply, whose encodings differ in shape (16 + 128 against a
            // bare 16-byte header).
            (
                "lookup hit",
                fuse_opcode::FUSE_LOOKUP as u32,
                11,
                1,
                Vec::new(),
                b"hello.txt\0".to_vec(),
                false,
            ),
            (
                "lookup miss",
                fuse_opcode::FUSE_LOOKUP as u32,
                12,
                1,
                Vec::new(),
                b"nope\0".to_vec(),
                false,
            ),
            (
                "getattr",
                fuse_opcode::FUSE_GETATTR as u32,
                13,
                HELLO_INO.0,
                getattr_in(None),
                Vec::new(),
                false,
            ),
            // A read answered with `data()`: a heap buffer and a `writev`
            // over `/dev/fuse`, a copy into the entry over a ring.
            (
                "read data",
                fuse_opcode::FUSE_READ as u32,
                14,
                HELLO_INO.0,
                read_in(1, 0, 64),
                Vec::new(),
                false,
            ),
            // The same read answered with `fill()`: written in place in
            // the entry over a ring, into a heap buffer and `writev`n over
            // `/dev/fuse`. The bytes on the wire must still match.
            (
                "read fill",
                fuse_opcode::FUSE_READ as u32,
                15,
                HELLO_INO.0,
                read_in(1, 2, 32),
                Vec::new(),
                true,
            ),
            // A request with a payload of its own, which is what makes the
            // ring entry's staging non-trivial (the request continues into
            // the payload area).
            (
                "write",
                fuse_opcode::FUSE_WRITE as u32,
                16,
                HELLO_INO.0,
                write_in(1, 0, 11),
                b"hello world".to_vec(),
                false,
            ),
            // A reply the filesystem builds incrementally.
            (
                "readdir",
                fuse_opcode::FUSE_READDIR as u32,
                17,
                1,
                read_in(1, 0, 4096),
                Vec::new(),
                false,
            ),
            // The xattr size probe: a `ReplyXattr::size`, a different
            // encoding again.
            (
                "getxattr size probe",
                fuse_opcode::FUSE_GETXATTR as u32,
                18,
                HELLO_INO.0,
                {
                    let mut b = 0u32.to_le_bytes().to_vec(); // size: a probe
                    b.extend_from_slice(&0u32.to_le_bytes()); // padding
                    b
                },
                b"user.test\0".to_vec(),
                false,
            ),
        ];

        for (name, opcode, unique, nodeid, op_in, payload, fill) in cases {
            let bytes = request(opcode, unique, nodeid, &op_in, &payload);

            // The `/dev/fuse` leg: dispatch exactly as the event loop does
            // (`ReplySender::Channel`), then read the datagram back.
            let (ours, theirs) = UnixDatagram::pair().unwrap();
            let se = event_loop(
                RingFs {
                    fill,
                    ..RingFs::default()
                },
                ours,
            );
            let parsed =
                ll::AnyRequest::try_from(&bytes[..]).expect("a well-formed request");
            let req =
                RequestWithSender::from_request(ReplySender::Channel(se.ch.sender()), parsed);
            req.dispatch(&se);
            let mut buf = vec![0u8; 1 << 16];
            let n = theirs.recv(&mut buf).expect("a reply on the socket pair");
            let dev_fuse = buf[..n].to_vec();

            // The ring leg: the same request in an entry, dispatched
            // through the same `handle_fetch` the ring thread calls.
            let (ours, _theirs) = UnixDatagram::pair().unwrap();
            let se = event_loop(
                RingFs {
                    fill,
                    ..RingFs::default()
                },
                ours,
            );
            let ring = dispatch_over_a_fake_ring(&bytes, op_in.len(), |commit, req| {
                se.handle_fetch(commit, req)
            });

            // Not a vacuous comparison: every case but the errno reply
            // carries a body, and the errno reply is exactly a header.
            let want_body = name != "lookup miss";
            assert_eq!(
                dev_fuse.len() > OUT_HEADER_SZ,
                want_body,
                "{name}: the /dev/fuse leg answered {} bytes",
                dev_fuse.len()
            );
            assert_eq!(
                dev_fuse, ring,
                "{name}: /dev/fuse and the ring answered differently\n  dev_fuse: {dev_fuse:02x?}\n  ring:     {ring:02x?}"
            );
        }
    }

    #[test]
    fn validate_transport_rejects_what_this_build_cannot_serve() {
        assert!(validate_transport(&Config::default()).is_ok());
        assert!(validate_transport(&ring_config()).is_ok());
        let err = validate_transport(&Config {
            io_uring_queue_depth: 0,
            ..ring_config()
        })
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), "io_uring_queue_depth must be at least 1");
        // The depth is only checked once it matters
        assert!(
            validate_transport(&Config {
                io_uring_queue_depth: 0,
                ..Config::default()
            })
            .is_ok()
        );
        // The thread count is checked for both transports alike
        let err = validate_transport(&Config {
            n_threads: Some(0),
            ..ring_config()
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "n_threads must be at least 1");
        // Both constructors refuse before touching anything
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            io_uring_queue_depth: 0,
            ..ring_config()
        };
        let err = Session::new(RingFs::default(), tmp.path(), &config)
            .err()
            .expect("a depth of 0 must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let fd = OwnedFd::from(File::open("/dev/null").unwrap());
        let err = Session::from_fd(RingFs::default(), fd, SessionACL::Owner, config)
            .err()
            .expect("a depth of 0 must be refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    /// Covers the deferred path (getxattr), a reply from another thread (unlink) and a dropped reply
    #[test]
    fn ring_serves_a_mount() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping ring_serves_a_mount: {why}");
            return;
        }
        let m = Mounted::new();
        let destroyed = Arc::new(AtomicUsize::new(0));
        let (unlink_tx, unlink_rx) = mpsc::channel();
        let fs = RingFs {
            destroyed: destroyed.clone(),
            unlink_reply: Some(unlink_tx),
            ..RingFs::default()
        };
        let session = m.session(fs, &ring_config());
        assert_threads("fuser-ring-", 1);
        let created = logged(log::Level::Debug, "queues over 1 rings, depth 8");
        assert_eq!(created.len(), 1, "{created:?}");
        let queues = crate::uring::possible_cpus().unwrap();
        assert!(
            created[0].starts_with(&format!("io_uring: {queues} queues")),
            "{created:?}"
        );
        let registered = wait_logged(log::Level::Debug, "ring 0 registered", 1);
        assert_eq!(
            registered,
            [format!(
                "io_uring: ring 0 registered {} entries",
                usize::from(queues) * 8
            )]
        );
        let bg = session.spawn().unwrap();
        assert_threads("fuser-dev", 1);

        let meta = std::fs::metadata(m.path("hello.txt")).unwrap();
        assert_eq!(meta.len(), HELLO.len() as u64);
        let mut names: Vec<String> = std::fs::read_dir(&m.mountpoint)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["big.bin", "hello.txt", "other.txt"]);
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        let big = std::fs::read(m.path("big.bin")).unwrap();
        assert_eq!(big.len(), BIG_LEN);
        assert!(big.iter().enumerate().all(|(i, b)| *b == big_byte(i)));
        std::fs::OpenOptions::new()
            .write(true)
            .open(m.path("hello.txt"))
            .unwrap()
            .write_all(b"written through the ring")
            .unwrap();
        let path = std::ffi::CString::new(m.path("hello.txt").as_os_str().as_bytes()).unwrap();
        let name = c"user.test";
        let mut buf = [0u8; 64];
        let n = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
            )
        };
        assert_eq!(n, XATTR.len() as isize, "{}", io::Error::last_os_error());
        assert_eq!(&buf[..n as usize], XATTR);
        // The unlink is answered only once the ring thread served a later request, so it has
        // returned from `unlink` and the reply is a direct write into a `Dispatched` entry
        let (removed_tx, removed_rx) = mpsc::channel();
        let path = m.path("other.txt");
        thread::spawn(move || {
            let _ = removed_tx.send(std::fs::remove_file(path));
        });
        let unlink = unlink_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unlink was not dispatched");
        // The root's attributes need no lock the pending unlink holds on the directory
        std::fs::metadata(&m.mountpoint).unwrap();
        unlink.ok();
        removed_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("unlink was not answered")
            .unwrap();
        let err = std::fs::set_permissions(
            m.path("hello.txt"),
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));

        let started = Instant::now();
        bg.umount_and_join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(destroyed.load(Ordering::SeqCst), 1);
        assert_eq!(logged(log::Level::Debug, "ring 0 serving").len(), 1);
        let exited = logged(log::Level::Debug, "ring 0 exited");
        assert_eq!(exited.len(), 1, "{exited:?}");
        assert!(exited[0].contains("in_kernel=0"), "{exited:?}");
        // The ring unit tests run alongside and log errors for their fake rings, numbered 7
        assert!(logged(log::Level::Error, "ring 0").is_empty());
        assert!(logged(log::Level::Error, &format!("leaking {}", reserved_bytes(8))).is_empty());
        m.finish();
    }

    fn read_through_fill(m: &Mounted) {
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        let big = std::fs::read(m.path("big.bin")).unwrap();
        assert_eq!(big.len(), BIG_LEN);
        assert!(big.iter().enumerate().all(|(i, b)| *b == big_byte(i)));
        let mut tail = [0u8; 64];
        let n = std::os::unix::fs::FileExt::read_at(
            &File::open(m.path("hello.txt")).unwrap(),
            &mut tail,
            6,
        )
        .unwrap();
        assert_eq!(&tail[..n], &HELLO[6..]);
    }

    /// Every closure wrote straight into its ring entry
    #[test]
    fn fill_serves_reads() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping fill_serves_reads: {why}");
            return;
        }
        for foreign in [false, true] {
            let m = Mounted::new();
            let fills = Arc::new(AtomicUsize::new(0));
            let fs = RingFs {
                fill: true,
                foreign,
                fills: fills.clone(),
                ..RingFs::default()
            };
            let session = m.session(fs, &ring_config());
            let rings: Vec<Arc<crate::uring::ring::Ring>> =
                session.ring.as_ref().unwrap().rings().to_vec();
            let bg = session.spawn().unwrap();
            read_through_fill(&m);
            umount_and_join_within(bg, Duration::from_secs(5)).unwrap();
            // Every read above ran to EOF, so every READ was answered before the unmount;
            // the two counters are incremented at different points of a closure that may
            // run on an unjoined thread, so they are given time to agree
            let direct = || rings.iter().map(|r| r.direct_fills()).sum::<usize>();
            let filled = || fills.load(Ordering::SeqCst);
            assert!(
                wait_until(|| direct() == filled()),
                "foreign={foreign}: {} of {} fills went straight into their entry",
                direct(),
                filled()
            );
            assert!(filled() > 1, "foreign={foreign}");
            assert!(
                logged(log::Level::Error, "ring 0").is_empty(),
                "foreign={foreign}\n{}",
                session_log()
            );
            m.finish();
        }
    }

    #[test]
    fn panic_in_fill_on_a_ring_thread_answers_eio() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping panic_in_fill_on_a_ring_thread_answers_eio: {why}");
            return;
        }
        let m = Mounted::new();
        let fs = RingFs {
            panic_fill: Arc::new(AtomicBool::new(true)),
            ..RingFs::default()
        };
        let session = m.session(fs, &ring_config());
        let abort_path = super::test::fusectl_abort_path(&m.mountpoint);
        let abort = || {
            if let Some(path) = &abort_path {
                let _ = std::fs::write(path, b"1");
            }
        };
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || tx.send(session.run()).unwrap());

        let (read_tx, read_rx) = mpsc::channel();
        let path = m.path("hello.txt");
        thread::spawn(move || read_tx.send(std::fs::read(path).map(drop)).unwrap());
        let Ok(read) = read_rx.recv_timeout(Duration::from_secs(5)) else {
            abort();
            panic!("the read the panicking closure owed was never answered");
        };
        assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EIO));
        let Ok(reply) = rx.recv_timeout(Duration::from_secs(5)) else {
            abort();
            panic!("run did not return after the panic");
        };
        assert_eq!(reply.unwrap_err().to_string(), THREAD_PANICKED);
        if !wait_ring_threads_gone(Duration::from_secs(5)) {
            abort();
            panic!("threads still running: {:?}", thread_names());
        }
        // The unwind guard answered, and the ring left with nothing in the kernel
        let guarded = wait_logged_by(
            "fuser-ring-0",
            log::Level::Warn,
            "reply closure for unique",
            1,
        );
        assert!(!guarded.is_empty(), "{}", session_log());
        let exited = wait_logged(log::Level::Debug, "ring 0 exited, in_kernel=0", 1);
        assert_eq!(exited.len(), 1, "{}", session_log());
        assert!(
            logged_by("fuser-ring-0", log::Level::Warn, "Reply not sent").is_empty(),
            "{}",
            session_log()
        );
        assert!(
            logged_by("fuser-ring-0", log::Level::Error, "").is_empty(),
            "{}",
            session_log()
        );
        m.finish();
    }

    /// Only here does the guard's re-arming matter, because the session survives the panic
    #[test]
    fn panic_in_fill_on_a_foreign_thread_keeps_the_ring_serving() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping panic_in_fill_on_a_foreign_thread_keeps_the_ring_serving: {why}");
            return;
        }
        let m = Mounted::new();
        let panic_fill = Arc::new(AtomicBool::new(true));
        let fs = RingFs {
            fill: true,
            foreign: true,
            panic_fill: panic_fill.clone(),
            ..RingFs::default()
        };
        // One entry per queue, so the re-armed entry is the one that serves again
        let config = Config {
            io_uring_queue_depth: 1,
            ..ring_config()
        };
        let bg = m.session(fs, &config).spawn().unwrap();
        let (read_tx, read_rx) = mpsc::channel();
        let path = m.path("hello.txt");
        thread::spawn(move || read_tx.send(std::fs::read(path).map(drop)).unwrap());
        let read = read_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the read the panicking closure owed was never answered");
        assert_eq!(read.unwrap_err().raw_os_error(), Some(libc::EIO));
        // The guard ran on the filesystem's replier thread, not on the ring thread; the kernel
        // may retry the failed READ, so there can be more than one such line
        let guarded = wait_logged_by(REPLIER, log::Level::Warn, "reply closure for unique", 1);
        assert!(!guarded.is_empty(), "{}", session_log());
        assert!(logged_by("fuser-ring-", log::Level::Warn, "reply closure").is_empty());

        panic_fill.store(false, Ordering::SeqCst);
        read_through_fill(&m);
        umount_and_join_within(bg, Duration::from_secs(5)).unwrap();
        let exited = wait_logged(log::Level::Debug, "ring 0 exited, in_kernel=0", 1);
        assert_eq!(exited.len(), 1, "{}", session_log());
        assert!(
            logged(log::Level::Error, "ring 0").is_empty(),
            "{}",
            session_log()
        );
        // The guard's reply was the only one: the replier thread, where a reply object
        // dropped after the panic would speak up, logged nothing else
        assert!(
            logged_by(REPLIER, log::Level::Warn, "Reply not sent").is_empty(),
            "{}",
            session_log()
        );
        assert!(
            logged_by(REPLIER, log::Level::Error, "").is_empty(),
            "{}",
            session_log()
        );
        m.finish();
    }

    #[test]
    fn two_rings_share_the_queues() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping two_rings_share_the_queues: {why}");
            return;
        }
        let m = Mounted::new();
        let config = Config {
            n_threads: Some(2),
            ..ring_config()
        };
        let session = m.session(RingFs::default(), &config);
        assert_threads("fuser-ring-", 2);
        let created = logged(log::Level::Debug, "queues over 2 rings, depth 8");
        assert_eq!(created.len(), 1, "{created:?}");
        let queues = crate::uring::possible_cpus().unwrap();
        for (index, qids) in crate::uring::partition(queues, 2).iter().enumerate() {
            let text = format!("ring {index} registered");
            assert_eq!(
                wait_logged(log::Level::Debug, &text, 1),
                [format!("io_uring: {text} {} entries", qids.len() * 8)]
            );
        }
        let bg = session.spawn().unwrap();
        assert_threads("fuser-dev", 1);
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        assert_eq!(std::fs::read(m.path("other.txt")).unwrap(), OTHER);
        bg.umount_and_join().unwrap();
        assert_eq!(
            logged(log::Level::Debug, "ring 0 exited, in_kernel=0").len(),
            1
        );
        assert_eq!(
            logged(log::Level::Debug, "ring 1 exited, in_kernel=0").len(),
            1
        );
        m.finish();
    }

    /// With a ring there is one `/dev/fuse` reader, so `clone_fd` has nothing to apply to
    #[test]
    fn clone_fd_is_ignored_with_a_ring() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping clone_fd_is_ignored_with_a_ring: {why}");
            return;
        }
        let m = Mounted::new();
        let config = Config {
            n_threads: Some(2),
            clone_fd: true,
            ..ring_config()
        };
        let bg = m.session(RingFs::default(), &config).spawn().unwrap();
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        assert_threads("fuser-dev", 1);
        assert_threads("fuser-ring-", 2);
        assert_eq!(
            logged(log::Level::Debug, "clone_fd has no effect with io_uring").len(),
            1
        );
        bg.umount_and_join().unwrap();
        m.finish();
    }

    #[test]
    fn replying_thread_may_exit_before_the_next_request() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping replying_thread_may_exit_before_the_next_request: {why}");
            return;
        }
        let m = Mounted::new();
        let fs = RingFs {
            foreign: true,
            ..RingFs::default()
        };
        let bg = m.session(fs, &ring_config()).spawn().unwrap();

        let (tx, rx) = mpsc::channel();
        let mountpoint = m.mountpoint.clone();
        thread::spawn(move || {
            // Pin this thread to one CPU so every request lands on the same queue
            let cpu = (0..usize::from(crate::uring::possible_cpus().unwrap()))
                .find(|&cpu| pin_to_cpu(cpu))
                .expect("no CPU accepts this thread");
            for round in 0..5 {
                let meta = std::fs::metadata(mountpoint.join("hello.txt")).unwrap();
                assert_eq!(meta.len(), HELLO.len() as u64);
                let mut file = File::open(mountpoint.join("hello.txt")).unwrap();
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).unwrap();
                assert_eq!(buf, HELLO, "round {round} on cpu {cpu}");
                // Drop the page cache so the next round reads through the ring again
                unsafe {
                    libc::posix_fadvise(
                        std::os::fd::AsRawFd::as_raw_fd(&file),
                        0,
                        0,
                        libc::POSIX_FADV_DONTNEED,
                    )
                };
            }
            tx.send(cpu).unwrap();
        });
        let cpu = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("a request after a replying thread exited must still complete");
        eprintln!("five stat+read rounds completed pinned to cpu {cpu}");
        bg.umount_and_join().unwrap();
        m.finish();
    }

    /// The queues were registered by the ring threads, not by the constructing thread
    #[test]
    fn session_built_on_a_thread_that_exited_still_serves() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping session_built_on_a_thread_that_exited_still_serves: {why}");
            return;
        }
        let m = Mounted::new();
        let mountpoint = m.mountpoint.clone();
        let session = thread::spawn(move || {
            Session::new(RingFs::default(), &mountpoint, &ring_config()).unwrap()
        })
        .join()
        .unwrap();
        assert!(session.ring.is_some());
        let bg = session.spawn().unwrap();
        assert_eq!(std::fs::metadata(&m.mountpoint).unwrap().len(), 0);
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        std::fs::OpenOptions::new()
            .write(true)
            .open(m.path("hello.txt"))
            .unwrap()
            .write_all(b"x")
            .unwrap();
        bg.umount_and_join().unwrap();
        m.finish();
    }

    /// A reply made while the ring thread is inside a callback goes out when the callback returns
    #[test]
    fn foreign_reply_is_batched_behind_the_ring_thread() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping foreign_reply_is_batched_behind_the_ring_thread: {why}");
            return;
        }
        // Idle ring: the reply lands as soon as the replier makes it
        let m = Mounted::new();
        let fs = RingFs {
            delay_other: Duration::from_millis(100),
            ..RingFs::default()
        };
        let bg = m.session(fs, &ring_config()).spawn().unwrap();
        let started = Instant::now();
        assert_eq!(std::fs::read(m.path("other.txt")).unwrap(), OTHER);
        let took = started.elapsed();
        assert!(
            took < Duration::from_millis(600),
            "idle-ring wake took {took:?}"
        );
        bg.umount_and_join().unwrap();
        m.finish();

        // Busy ring: other.txt's read returns at once and its replier waits until hello.txt's
        // read has been dispatched, which parks the ring thread inside `read`. The reply
        // committed into the parked ring only goes out when `read` returns
        let park = Duration::from_millis(500);
        let m = Mounted::new();
        let (hello_tx, hello_rx) = mpsc::channel();
        let (other_tx, other_rx) = mpsc::channel();
        let (release_tx, gate_rx) = mpsc::channel();
        let (replied_tx, replied_rx) = mpsc::channel();
        let fs = RingFs {
            park_hello: park,
            hello_read: Some(hello_tx),
            other_read: Some(other_tx),
            other_gate: Mutex::new(Some(gate_rx)),
            release_other: Some(release_tx),
            other_replied: Some(replied_tx),
            ..RingFs::default()
        };
        let bg = m.session(fs, &ring_config()).spawn().unwrap();
        // Warm the lookups so the timed reads are the only requests in flight
        std::fs::metadata(m.path("other.txt")).unwrap();
        std::fs::metadata(m.path("hello.txt")).unwrap();
        let read_at = |name: &str| {
            let path = m.path(name);
            thread::spawn(move || {
                std::fs::read(path).unwrap();
                Instant::now()
            })
        };
        let other = read_at("other.txt");
        other_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let hello = read_at("hello.txt");
        let hello_dispatched = hello_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let other_replied = replied_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let other_done = other.join().unwrap();
        let hello_done = hello.join().unwrap();
        // The reply was made into the parked ring, or the run says nothing
        let replied_after = other_replied.duration_since(hello_dispatched);
        assert!(
            replied_after < park,
            "other.txt was replied {replied_after:?} after hello.txt parked the ring thread for {park:?}"
        );
        let waited = other_done.duration_since(hello_dispatched);
        eprintln!(
            "other.txt replied {replied_after:?} after hello.txt parked; done {waited:?} later"
        );
        assert!(
            waited >= park - Duration::from_millis(100),
            "other.txt completed {waited:?} after hello.txt parked the ring thread for {park:?}"
        );
        assert!(
            other_done.saturating_duration_since(hello_done) < Duration::from_millis(500),
            "other.txt completed {:?} after hello.txt returned",
            other_done.saturating_duration_since(hello_done)
        );
        bg.umount_and_join().unwrap();
        m.finish();
    }

    #[test]
    fn requests_before_run_wait_for_run() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping requests_before_run_wait_for_run: {why}");
            return;
        }
        let m = Mounted::new();
        let session = m.session(RingFs::default(), &ring_config());
        let (tx, rx) = mpsc::channel();
        let path = m.path("hello.txt");
        thread::spawn(move || tx.send(std::fs::metadata(path).map(|m| m.len())).unwrap());
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "nothing serves the mount before run"
        );
        let bg = session.spawn().unwrap();
        let len = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("run must serve the waiting request")
            .unwrap();
        assert_eq!(len, HELLO.len() as u64);
        bg.umount_and_join().unwrap();
        m.finish();
    }

    /// A request fetched before the drop is answered EIO
    #[test]
    fn dropped_session_unmounts_and_drains() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping dropped_session_unmounts_and_drains: {why}");
            return;
        }
        let m = Mounted::new();
        let destroyed = Arc::new(AtomicUsize::new(0));
        let fs = RingFs {
            destroyed: destroyed.clone(),
            ..RingFs::default()
        };
        let session = m.session(fs, &ring_config());
        let (tx, rx) = mpsc::channel();
        let path = m.path("hello.txt");
        thread::spawn(move || {
            let _ = tx.send(std::fs::metadata(path).map(drop));
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "nothing serves the mount before run"
        );
        drop(session);
        assert_eq!(destroyed.load(Ordering::SeqCst), 1);
        let err = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the fetched request was not answered")
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO), "{err}");
        m.finish();
        // The ring unit tests log the same line for their fake rings, which are numbered 7
        assert_eq!(
            logged(
                log::Level::Error,
                "ring 0 serving EIO until the connection ends"
            )
            .len(),
            1
        );
        assert_eq!(
            logged(log::Level::Debug, "ring 0 exited, in_kernel=0").len(),
            1
        );
        assert!(logged(log::Level::Error, &format!("leaking {}", reserved_bytes(8))).is_empty());
    }

    #[test]
    fn dropped_from_fd_session_aborts_the_connection() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping dropped_from_fd_session_aborts_the_connection: {why}");
            return;
        }
        if !geteuid().is_root() {
            eprintln!(
                "skipping dropped_from_fd_session_aborts_the_connection: mount(2) needs root"
            );
            return;
        }
        /// Detaches the hand-made mount however the test ends
        struct Detach(std::path::PathBuf);
        impl Drop for Detach {
            fn drop(&mut self) {
                let _ = nix::mount::umount2(&self.0, nix::mount::MntFlags::MNT_DETACH);
            }
        }
        let tmp = ManuallyDrop::new(tempfile::tempdir().unwrap());
        let mountpoint = tmp.path().canonicalize().unwrap();
        let device = DevFuse::open().unwrap();
        let options = format!(
            "fd={},rootmode=40000,user_id={},group_id={}",
            std::os::fd::AsRawFd::as_raw_fd(&device),
            nix::unistd::getuid(),
            nix::unistd::getgid()
        );
        nix::mount::mount(
            Some("/dev/fuse"),
            &mountpoint,
            Some("fuse"),
            nix::mount::MsFlags::MS_NOSUID | nix::mount::MsFlags::MS_NODEV,
            Some(options.as_str()),
        )
        .unwrap();
        let detach = Detach(mountpoint.clone());
        let fd = OwnedFd::from(device.0);
        let session =
            Session::from_fd(RingFs::default(), fd, SessionACL::Owner, ring_config()).unwrap();
        assert!(session.ring.is_some());
        let before = wait_logged(log::Level::Debug, "ring 0 registered", 1);
        assert_eq!(before.len(), 1, "{before:?}");
        drop(session);

        // Nothing serves the mount any more, so this either fails once the connection is
        // aborted or blocks
        let (tx, rx) = mpsc::channel();
        let path = mountpoint.clone();
        thread::spawn(move || tx.send(std::fs::metadata(path).map(drop)).unwrap());
        let err = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the connection was not aborted")
            .unwrap_err();
        assert!(
            matches!(
                err.raw_os_error(),
                Some(libc::ENOTCONN | libc::ECONNABORTED)
            ),
            "{err}"
        );
        assert!(wait_ring_threads_gone(Duration::from_secs(5)));
        // The ring unit tests log the same lines for their fake rings, which are numbered 7
        let abandoned = wait_logged(log::Level::Error, "ring 0 abandoning", 1);
        assert_eq!(abandoned.len(), 1, "{abandoned:?}");
        assert!(abandoned[0].contains("from_fd session was dropped before it was run"));
        let leaked = format!("leaking {} bytes", reserved_bytes(8));
        assert_eq!(wait_logged(log::Level::Error, &leaked, 1).len(), 1);
        drop(detach);
        assert_not_mounted(&mountpoint);
        ManuallyDrop::into_inner(tmp);
    }

    /// Pins the calling thread to `cpu`; false when that CPU cannot run it (offline)
    fn pin_to_cpu(cpu: usize) -> bool {
        // SAFETY: a zeroed cpu_set_t is valid, and the libc macros only touch it.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);
            libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) == 0
        }
    }

    /// With depth 1 and a request held on every queue nothing of the ring's is in the kernel
    #[test]
    fn teardown_ends_a_ring_whose_entries_are_all_held() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping teardown_ends_a_ring_whose_entries_are_all_held: {why}");
            return;
        }
        let Some(_fusectl) = super::test::Fusectl::ensure() else {
            eprintln!("skipping teardown_ends_a_ring_whose_entries_are_all_held: no fusectl");
            return;
        };
        let m = Mounted::new();
        let held = Arc::new(Mutex::new(Vec::new()));
        let fs = RingFs {
            hold_getattr: Some(held.clone()),
            ..RingFs::default()
        };
        let config = Config {
            io_uring_queue_depth: 1,
            ..ring_config()
        };
        let bg = m.session(fs, &config).spawn().unwrap();
        let abort_path =
            super::test::fusectl_abort_path(&m.mountpoint).expect("fusectl is mounted");

        // Held requests are only ever ended by the abort; a failure before it would leave
        // their threads unkillable and this process unable to exit
        let abort = || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&abort_path)
                .unwrap()
                .write_all(b"1")
                .unwrap();
        };

        // One unanswered request from every CPU, so that every queue's single entry is held
        let n_queues = usize::from(crate::uring::possible_cpus().unwrap());
        let (tx, rx) = mpsc::channel();
        let stats: Vec<_> = (0..n_queues)
            .map(|cpu| {
                let path = m.mountpoint.clone();
                let tx = tx.clone();
                thread::spawn(move || {
                    let pinned = pin_to_cpu(cpu);
                    tx.send(pinned).unwrap();
                    pinned.then(|| std::fs::metadata(path).map(drop))
                })
            })
            .collect();
        let pinned = rx.iter().take(n_queues).filter(|ok| *ok).count();
        if pinned < n_queues {
            eprintln!(
                "{} of {n_queues} CPUs are offline; their entries stay in the kernel",
                n_queues - pinned
            );
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while held.lock().len() < pinned {
            if Instant::now() > deadline {
                let arrived = held.lock().len();
                abort();
                panic!("only {arrived} of {pinned} requests arrived");
            }
            thread::sleep(Duration::from_millis(10));
        }

        // Abort the connection: the held requests fail, and no CQE reaches the ring
        abort();
        umount_and_join_within(bg, Duration::from_secs(5)).unwrap();
        for stat in stats {
            // Threads that could not be pinned made no request
            let Some(result) = stat.join().unwrap() else {
                continue;
            };
            let err = result.unwrap_err();
            assert!(
                matches!(
                    err.raw_os_error(),
                    Some(libc::ENOTCONN | libc::ECONNABORTED)
                ),
                "{err}"
            );
        }
        let exited = logged(log::Level::Debug, "ring 0 exited");
        assert_eq!(exited.len(), 1, "{exited:?}");
        assert_eq!(
            exited[0],
            format!("io_uring: ring 0 exited, in_kernel=0 outstanding={pinned}")
        );
        // The replies come too late and are dropped quietly
        held.lock().clear();
        let dropped = logged(log::Level::Debug, "dropping reply for unique");
        assert_eq!(
            dropped
                .iter()
                .filter(|l| l.ends_with("after ring 0 exited"))
                .count(),
            pinned
        );
        // A ring refusal of the late replies; a /dev/fuse test running alongside may log the
        // same prefix with a different cause
        assert!(
            logged(
                log::Level::Error,
                "Failed to send FUSE reply: duplicate reply"
            )
            .is_empty()
        );
        assert!(
            logged(
                log::Level::Error,
                "Failed to send FUSE reply: reply after the connection ended"
            )
            .is_empty()
        );
        assert!(logged(log::Level::Error, &format!("leaking {}", reserved_bytes(1))).is_empty());

        // The dead mount stays in the table after an abort, as after any abort
        let _ = nix::mount::umount2(&m.mountpoint, nix::mount::MntFlags::MNT_DETACH);
        m.finish();
    }

    /// The ring twin of `test::panic_in_callback_ends_the_session`
    #[test]
    fn panic_on_a_ring_thread_ends_the_session() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping panic_on_a_ring_thread_ends_the_session: {why}");
            return;
        }
        let m = Mounted::new();
        let destroyed = Arc::new(AtomicUsize::new(0));
        let fs = RingFs {
            panic_getattr: true,
            destroyed: destroyed.clone(),
            ..RingFs::default()
        };
        let session = m.session(fs, &ring_config());
        // Ends the connection if the session fails to, so a failure here cannot wedge the host
        let abort_path = super::test::fusectl_abort_path(&m.mountpoint);
        let abort = || {
            if let Some(path) = &abort_path {
                let _ = std::fs::write(path, b"1");
            }
        };
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || tx.send(session.run()).unwrap());

        // The dropped reply answers with EIO, so this returns once the panic has happened
        let (stat_tx, stat_rx) = mpsc::channel();
        let path = m.mountpoint.clone();
        thread::spawn(move || stat_tx.send(std::fs::metadata(path).map(drop)).unwrap());
        let Ok(stat) = stat_rx.recv_timeout(Duration::from_secs(5)) else {
            abort();
            panic!("the request the panicking callback owed was never answered");
        };
        assert_eq!(stat.unwrap_err().raw_os_error(), Some(libc::EIO));
        let Ok(reply) = rx.recv_timeout(Duration::from_secs(5)) else {
            abort();
            panic!("run did not return after the panic");
        };
        assert_eq!(reply.unwrap_err().to_string(), THREAD_PANICKED);
        if !wait_ring_threads_gone(Duration::from_secs(5)) {
            abort();
            panic!("threads still running: {:?}", thread_names());
        }
        // The detached threads finish their exit work after `run` returned
        assert!(
            wait_until(|| destroyed.load(Ordering::SeqCst) == 1),
            "destroyed {} times\n{}",
            destroyed.load(Ordering::SeqCst),
            session_log()
        );
        assert_eq!(unwound_replies(), 1, "{}", session_log());
        let exited = wait_logged(log::Level::Debug, "ring 0 exited, in_kernel=0", 1);
        assert_eq!(exited.len(), 1, "{}", session_log());
        m.finish();
    }

    /// Scoped by thread because the `/dev/fuse` panic test logs the same line from its reader
    fn unwound_replies() -> usize {
        wait_logged_by(
            "fuser-ring-0",
            log::Level::Warn,
            "Reply not sent for operation",
            1,
        )
        .len()
    }

    /// A mount nobody answers fails the test instead of wedging it
    fn stat_within(path: &Path, timeout: Duration) -> io::Result<()> {
        let (tx, rx) = mpsc::channel();
        let target = path.to_path_buf();
        thread::spawn(move || tx.send(std::fs::metadata(target).map(drop)));
        rx.recv_timeout(timeout)
            .unwrap_or_else(|_| panic!("stat {} was not answered", path.display()))
    }

    #[test]
    fn spawned_ring_session_answers_eio_after_a_panic() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping spawned_ring_session_answers_eio_after_a_panic: {why}");
            return;
        }
        let m = Mounted::new();
        let fs = RingFs {
            panic_getattr: true,
            ..RingFs::default()
        };
        let bg = m.session(fs, &ring_config()).spawn().unwrap();
        let stat = stat_within(&m.mountpoint, Duration::from_secs(5));
        assert_eq!(stat.unwrap_err().raw_os_error(), Some(libc::EIO));
        assert!(
            wait_until(|| bg.guard.is_finished()),
            "run did not return after the panic\n{}",
            session_log()
        );
        let stat = stat_within(&m.mountpoint, Duration::from_secs(5));
        assert_eq!(stat.unwrap_err().raw_os_error(), Some(libc::EIO));
        assert_eq!(
            unwound_replies(),
            1,
            "the second stat reached the filesystem\n{}",
            session_log()
        );
        let err = umount_and_join_within(bg, Duration::from_secs(5)).unwrap_err();
        assert_eq!(err.to_string(), THREAD_PANICKED);
        m.finish();
    }

    #[test]
    fn oversized_depth_falls_back_to_dev_fuse() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping oversized_depth_falls_back_to_dev_fuse: {why}");
            return;
        }
        let m = Mounted::new();
        let queues = usize::from(crate::uring::possible_cpus().unwrap());
        let depth = (crate::uring::ring::IORING_MAX_ENTRIES / queues + 1) as u32;
        let config = Config {
            io_uring_queue_depth: depth,
            ..ring_config()
        };
        let session = Session::new(RingFs::default(), &m.mountpoint, &config).unwrap();
        assert!(session.ring.is_none());
        assert!(
            !session
                .negotiated
                .unwrap()
                .flags()
                .contains(InitFlags::FUSE_OVER_IO_URING)
        );
        let warned = logged(log::Level::Warn, "io_uring requested but");
        assert_eq!(warned.len(), 1, "{warned:?}");
        assert_eq!(
            warned[0],
            format!(
                "io_uring requested but {queues} queues x depth {depth} exceed the 32768 entries \
                 an io_uring holds (lower io_uring_queue_depth or raise n_threads); using \
                 /dev/fuse"
            )
        );
        assert!(
            logged(log::Level::Debug, "queues over").is_empty(),
            "no ring was created"
        );
        let bg = session.spawn().unwrap();
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        assert_eq!(count_threads("fuser-ring-"), 0);
        bg.umount_and_join().unwrap();
        m.finish();
    }

    /// CONSTELLATION PATCH (io-uring): a REGISTER the kernel refuses after the INIT reply has
    /// committed the connection to rings fails the constructor with `RegistrationRefused` --
    /// not a session whose every request hangs on queues that never become ready -- and the
    /// failed session's drop unmounts, so a caller can mount again (plan 38 §2.4, the
    /// "refused ring registration mid-INIT" rung). The kernel's own refusal, provoked with a
    /// malformed REGISTER (one iovec instead of two).
    #[test]
    fn a_refused_registration_fails_the_constructor_and_unmounts() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping a_refused_registration_fails_the_constructor_and_unmounts: {why}");
            return;
        }
        let m = Mounted::new();
        let config = Config {
            io_uring_malformed_register: true,
            ..ring_config()
        };
        let err = Session::new(RingFs::default(), &m.mountpoint, &config)
            .err()
            .expect("a malformed REGISTER is refused");
        assert!(crate::RegistrationRefused::is(&err), "{err}");
        let refused = err
            .get_ref()
            .and_then(|e| e.downcast_ref::<crate::RegistrationRefused>())
            .unwrap();
        assert_eq!(refused.kernel_error().raw_os_error(), Some(libc::EINVAL));
        // Every REGISTER was refused, so nothing is in the kernel and nothing is leaked (the
        // size names this session's one ring, not a ring unit test's running alongside)
        let leaked = format!("leaking {} bytes", reserved_bytes(8));
        assert!(
            logged(log::Level::Error, &leaked).is_empty(),
            "{:?}",
            logged(log::Level::Error, "")
        );
        assert!(
            wait_ring_threads_gone(Duration::from_secs(5)),
            "{:?}",
            thread_names()
        );
        assert_not_mounted(&m.mountpoint);
        // The host is fine; the same mountpoint takes a ring session at once
        let bg = m.session(RingFs::default(), &ring_config()).spawn().unwrap();
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        bg.umount_and_join().unwrap();
        m.finish();
    }

    #[test]
    fn late_reply_after_the_connection_ended_is_not_an_error() {
        let _serial = serial();
        let send = |conn_dead: bool| {
            let commit = crate::uring::ring::test::refused_commit(conn_dead);
            <ReplyRaw as Reply>::new(ll::RequestId(7), ReplySender::Ring(commit))
                .send_ll(&ResponseEmpty);
        };
        // Scoped to the ring's causes: a /dev/fuse test running alongside may log the same
        // prefix at error
        let demoted = "Failed to send FUSE reply: reply after the connection ended";
        let duplicate = "Failed to send FUSE reply: duplicate reply";
        send(true);
        assert_eq!(logged(log::Level::Debug, demoted).len(), 1);
        assert!(logged(log::Level::Error, demoted).is_empty());
        send(false);
        assert_eq!(logged(log::Level::Error, duplicate).len(), 1);
        assert!(logged(log::Level::Debug, duplicate).is_empty());
    }

    /// The ring twin of `test::session_ends_cleanly_after_abort`
    #[test]
    fn ring_session_ends_cleanly_after_abort() {
        let _serial = serial();
        if let Some(why) = uring_unavailable() {
            eprintln!("skipping ring_session_ends_cleanly_after_abort: {why}");
            return;
        }
        let Some(_fusectl) = super::test::Fusectl::ensure() else {
            eprintln!("skipping ring_session_ends_cleanly_after_abort: no fusectl");
            return;
        };
        let m = Mounted::new();
        let fs = RingFs {
            abort_error: true,
            ..RingFs::default()
        };
        let session = m.session(fs, &ring_config());
        assert!(
            session
                .negotiated
                .unwrap()
                .flags()
                .contains(InitFlags::FUSE_ABORT_ERROR)
        );
        let bg = session.spawn().unwrap();
        assert_eq!(std::fs::read(m.path("hello.txt")).unwrap(), HELLO);
        let abort_path =
            super::test::fusectl_abort_path(&m.mountpoint).expect("fusectl is mounted");
        std::fs::write(abort_path, b"1").unwrap();
        umount_and_join_within(bg, Duration::from_secs(5))
            .expect("session must end cleanly after the connection was aborted");
        // CONSTELLATION PATCH (io-uring): the RELEASE that follows the read runs on an offload
        // thread (`dispatch_on_ring`) and may still be held when the abort lands; its reply is
        // then dropped (at debug level) once the ring has left. Nothing may be left in the
        // kernel, and nothing may be an error.
        let exited = logged(log::Level::Debug, "ring 0 exited,");
        assert_eq!(exited.len(), 1, "{exited:?}");
        assert!(
            exited[0].starts_with("io_uring: ring 0 exited, in_kernel=0 outstanding="),
            "{exited:?}"
        );
        assert!(logged(log::Level::Error, "ring 0").is_empty());
        // The dead mount stays in the table after an abort, as after any abort
        let _ = nix::mount::umount2(&m.mountpoint, nix::mount::MntFlags::MNT_DETACH);
        m.finish();
    }

    /// Where the kernel advertises the transport the runtime tests must have run, not skipped
    #[test]
    fn fuse_over_io_uring_tests_ran() {
        let advertised = std::fs::read_to_string("/sys/module/fuse/parameters/enable_uring")
            .is_ok_and(|v| v.trim() == "Y");
        if !advertised {
            eprintln!("skipping fuse_over_io_uring_tests_ran: fuse.enable_uring is not Y");
            return;
        }
        if let Some(why) = uring_unavailable() {
            panic!("the kernel advertises FUSE_OVER_IO_URING but the ring tests skipped: {why}");
        }
    }

    /// `Session::from_fd` over a socketpair standing in for the kernel; `close_peer` closes the
    /// kernel end first so the INIT reply cannot be written
    fn from_fd_over_socketpair(
        flags: InitFlags,
        close_peer: bool,
        config: Config,
    ) -> (io::Result<Session<RingFs>>, OwnedFd) {
        use zerocopy::IntoBytes;

        use crate::ll::fuse_abi::fuse_in_header;
        use crate::ll::fuse_abi::fuse_init_in;
        use crate::ll::fuse_abi::fuse_opcode;
        use crate::uring::staging::test::in_header;

        let (kernel, daemon) = nix::sys::socket::socketpair(
            nix::sys::socket::AddressFamily::Unix,
            nix::sys::socket::SockType::Stream,
            None,
            nix::sys::socket::SockFlag::SOCK_CLOEXEC,
        )
        .unwrap();
        let (flags_lo, flags_hi) = (flags | InitFlags::FUSE_INIT_EXT).pair();
        let len = (size_of::<fuse_in_header>() + size_of::<fuse_init_in>()) as u32;
        let header = in_header(len, fuse_opcode::FUSE_INIT as u32, 1);
        let arg = fuse_init_in {
            major: 7,
            minor: 45,
            max_readahead: 65536,
            flags: flags_lo,
            flags2: flags_hi,
            unused: [0; 11],
        };
        let request = [&header[..], arg.as_bytes()].concat();
        // Queued bytes stay readable after the writer closes; the daemon's write then fails
        nix::unistd::write(&kernel, &request).unwrap();
        let kernel = if close_peer {
            drop(kernel);
            // A stand-in for the closed end, so the caller's binding is the same either way
            OwnedFd::from(File::open("/dev/null").unwrap())
        } else {
            kernel
        };
        (
            Session::from_fd(RingFs::default(), daemon, SessionACL::Owner, config),
            kernel,
        )
    }

    #[test]
    fn kernel_without_the_flag_falls_back_to_dev_fuse() {
        let _serial = serial();
        let (session, kernel) =
            from_fd_over_socketpair(InitFlags::FUSE_ASYNC_READ, false, ring_config());
        let session = session.unwrap();
        assert!(session.ring.is_none());
        assert!(
            !session
                .negotiated
                .unwrap()
                .flags()
                .contains(InitFlags::FUSE_OVER_IO_URING)
        );
        assert_eq!(
            logged(log::Level::Warn, "io_uring requested but"),
            [
                "io_uring requested but the kernel did not advertise FUSE_OVER_IO_URING \
                 (fuse.enable_uring=N or kernel < 6.14); using /dev/fuse"
            ]
        );
        // fuse_out_header (16) then fuse_init_out; flags2 is at offset 32 of the latter
        let mut reply = [0u8; 256];
        let n = nix::unistd::read(&kernel, &mut reply).unwrap();
        assert!(n >= 16 + 36, "short INIT reply of {n} bytes");
        let flags2 = u32::from_ne_bytes(reply[16 + 32..16 + 36].try_into().unwrap());
        let (_, io_uring_hi) = InitFlags::FUSE_OVER_IO_URING.pair();
        assert_eq!(flags2 & io_uring_hi, 0);
        assert_eq!(count_threads("fuser-ring-"), 0);
    }

    #[test]
    fn unwritable_init_reply_fails_only_a_ring_session() {
        let _serial = serial();
        if let Err(e) = RingIo::open(8, 16) {
            eprintln!("skipping unwritable_init_reply_fails_only_a_ring_session: {e}");
            return;
        }
        let config = Config {
            io_uring_queue_depth: 1,
            ..ring_config()
        };
        let (session, _kernel) =
            from_fd_over_socketpair(InitFlags::FUSE_OVER_IO_URING, true, config);
        let err = session.err().expect("a ring session cannot go on");
        assert_eq!(err.raw_os_error(), Some(libc::EPIPE), "{err}");
        assert_eq!(
            logged(log::Level::Debug, "queues over 1 rings, depth 1").len(),
            1
        );
        assert!(wait_ring_threads_gone(Duration::from_secs(5)));
        assert_eq!(
            wait_logged(log::Level::Debug, "detaching 1 ring threads", 1).len(),
            1
        );
        assert!(logged(log::Level::Debug, "ring 0 registered").is_empty());
        assert!(logged(log::Level::Error, &format!("leaking {}", reserved_bytes(1))).is_empty());
        assert!(logged(log::Level::Error, "Failed to send FUSE reply: Broken pipe").is_empty());

        let (session, _kernel) =
            from_fd_over_socketpair(InitFlags::FUSE_OVER_IO_URING, true, Config::default());
        assert!(
            session.is_ok(),
            "a /dev/fuse session is failed by its event loop"
        );
        assert_eq!(
            logged(log::Level::Error, "Failed to send FUSE reply: Broken pipe").len(),
            1
        );
    }
}
