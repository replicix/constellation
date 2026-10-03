//! What a frontend can do: [`FrontendCaps`] (plan 31 §6.6).
//!
//! Declared once per frontend, the single source of truth for the
//! engine's behaviour towards it (the policies a view applies are derived
//! from it, [`crate::PolicyStack::for_caps`]) and for test capability
//! gating (the harness's `Cap` is derived from it, never kept separately).

use crate::ctx::OpKindSet;

/// How the frontend's cache can be told to forget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushInval {
    /// Not at all: the TTL is the only bound.
    None,
    /// Attributes only.
    Attr,
    /// Names, attributes and pages (FUSE's `NOTIFY_INVAL_*`).
    Full,
}

/// Extended attributes, as the frontend can carry them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XattrSupport {
    None,
    /// In-band, with Linux namespaces (`user.*`, `trusted.*`).
    Native,
    /// In-band under names the frontend maps (macOS, NFSv4.2 named
    /// attributes).
    Named,
}

/// How the frontend's platform compares names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasePolicy {
    Sensitive,
    /// Case-insensitive lookup, exact-match priority, case preserved.
    InsensitivePreserving,
}

/// What the frontend's platform does with a file unlinked while open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenUnlinked {
    /// The inode stays usable through open handles (POSIX, FUSE).
    Keep,
    /// The client renames it aside (`.nfs*`).
    SillyRename,
    /// Deleted when the last handle closes (Windows).
    DeleteOnClose,
}

/// One frontend's capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendCaps {
    pub push_inval: PushInval,
    /// `flush` is called on every `close(2)` (the close-to-open fence).
    pub per_close_flush: bool,
    /// Cluster-wide POSIX/`flock` locks reach the engine (`--locks
    /// cluster`); without, the platform keeps locks node-local.
    pub cluster_locks: bool,
    pub xattrs: XattrSupport,
    /// `listxattr` names the virtual `user.constellation.{rsize,rcount}`.
    pub virtual_xattrs_listed: bool,
    pub hard_links: bool,
    pub fallocate: bool,
    pub seek_hole: bool,
    /// `mknod` of FIFOs, sockets and device nodes.
    pub special_files: bool,
    pub case: CasePolicy,
    /// The largest single read/write the frontend sends.
    pub max_io: u32,
    /// Ops whose responder the frontend can complete on another thread
    /// (FUSE, NFS: all; WinFsp: read, write, readdir). The engine only
    /// defers ops in this set; the rest complete on the calling thread.
    pub deferrable: OpKindSet,
    pub open_unlinked: OpenUnlinked,
    /// A forced-abort hook exists (Linux FUSE:
    /// `/sys/fs/fuse/connections/<n>/abort`; the harness's `FuseAbort`).
    pub abortable: bool,
    /// The frontend can read a handle's data straight from a backing file
    /// the engine hands it ([`crate::Opened::backing`], plan 38 §3(c)).
    /// `false` means the engine never offers one, so it never opens a
    /// chunk file or holds it un-evictable for a reader that would ignore
    /// it. Linux FUSE declares `false` and turns it on at `FUSE_INIT`,
    /// when the kernel offered `FUSE_PASSTHROUGH` and the process holds
    /// `CAP_SYS_ADMIN` (plan 38 Z3b, `Vfs::frontend_negotiated`); there
    /// is no [`Cap`] for it because a scenario gates on the kernel and the
    /// capability, not on the frontend (as for `max_io` and `deferrable`).
    pub passthrough: bool,
    /// The frontend can answer a read with a range of a file its kernel
    /// reads straight into the reader's pages ([`crate::ReadData::zero_copy`],
    /// plan 38 §3(d)). `false` means the engine never marks an open
    /// [`crate::Opened::zero_copy`] nor answers a read that way. Linux FUSE
    /// declares `false` and turns it on once its session settled on
    /// zero-copy io_uring queues (kernel 7.3 buffer pools and
    /// `CAP_SYS_ADMIN`, plan 38 Z4), again through
    /// `Vfs::frontend_negotiated`.
    pub zero_copy: bool,
    /// With [`Self::zero_copy`]: the smallest read the engine answers
    /// zero-copy, in bytes, and the smallest file whose open it marks (a
    /// smaller file has no read that could qualify). Below it the copy is
    /// cheaper than the kernel's extra trip for a zero-copy answer (plan
    /// 38 Z4b's measurements; Linux FUSE:
    /// `CONSTELLATION_FUSE_ZERO_COPY_MIN_READ`). `0`: every read.
    pub zero_copy_min_read: u32,
}

impl FrontendCaps {
    /// Linux FUSE (`constellation-frontend-fuse`), derived from what the
    /// adapter demonstrably supports: kernel invalidation of names,
    /// attributes and pages; native xattrs with the virtual ones listed
    /// (as `getfattr -d` has always shown them); hard links, `fallocate`,
    /// `SEEK_DATA`/`SEEK_HOLE`, special files; case-sensitive names; any
    /// op answerable from another thread; unlinked-open inodes kept;
    /// abortable. `cluster_locks`: the mount forwards locks (`--locks
    /// cluster` on a writable view).
    pub fn linux_fuse(cluster_locks: bool) -> Self {
        Self {
            push_inval: PushInval::Full,
            per_close_flush: true,
            cluster_locks,
            xattrs: XattrSupport::Native,
            virtual_xattrs_listed: true,
            hard_links: true,
            fallocate: true,
            seek_hole: true,
            special_files: true,
            case: CasePolicy::Sensitive,
            // fuser 0.18's `MAX_WRITE_SIZE`, the largest request buffer
            // it negotiates.
            max_io: 16 * 1024 * 1024,
            deferrable: OpKindSet::ALL,
            open_unlinked: OpenUnlinked::Keep,
            abortable: true,
            // Off until `FUSE_INIT` says otherwise: the adapter flips it
            // (and tells the view, `Vfs::frontend_negotiated`) only when
            // the kernel offered `FUSE_PASSTHROUGH` and the process can
            // register backing files (plan 38 Z3b).
            passthrough: false,
            // Off until the session's transport is settled (plan 38 Z4).
            zero_copy: false,
            zero_copy_min_read: 0,
        }
    }
}

/// One thing a frontend can or cannot do, as tests gate on it (plan 31
/// §6.6, §8): the conformance kit's per-test requirements and the
/// harness's per-scenario ones are both lists of these, so a scenario or a
/// test is skipped for a frontend by *this* table, never by a second,
/// hand-maintained one. Derived from [`FrontendCaps`] by
/// [`FrontendCaps::caps`]; the name (`Display`, [`Cap::name`]) is what a
/// skip reason spells (`requires capability ClusterLocks`) and what
/// `tests/platform-parity.toml` refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Cap {
    /// The frontend's cache can be told to forget attributes at least
    /// (`push_inval` is `Attr` or `Full`).
    PushInval,
    /// ... and names and pages too (`push_inval` is `Full`).
    PushInvalFull,
    /// `flush` on every `close(2)`.
    PerCloseFlush,
    /// Cluster-wide POSIX/`flock` locks reach the engine.
    ClusterLocks,
    /// Extended attributes (`Native` or `Named`).
    Xattrs,
    /// `listxattr` names the virtual `user.constellation.*` attributes.
    VirtualXattrsListed,
    HardLinks,
    Fallocate,
    /// `SEEK_DATA`/`SEEK_HOLE`.
    SeekHole,
    /// `mknod` of FIFOs, sockets and device nodes.
    SpecialFiles,
    /// Case-insensitive, case-preserving names.
    CaseInsensitive,
    /// An unlinked file stays usable through its open handles.
    KeepOpenUnlinked,
    /// A forced-abort hook exists (Linux FUSE's `/sys/fs/fuse/connections`
    /// abort): the kill-and-abort scenarios need it.
    FuseAbort,
}

impl Cap {
    /// Every capability.
    pub const ALL: &'static [Cap] = &[
        Cap::PushInval,
        Cap::PushInvalFull,
        Cap::PerCloseFlush,
        Cap::ClusterLocks,
        Cap::Xattrs,
        Cap::VirtualXattrsListed,
        Cap::HardLinks,
        Cap::Fallocate,
        Cap::SeekHole,
        Cap::SpecialFiles,
        Cap::CaseInsensitive,
        Cap::KeepOpenUnlinked,
        Cap::FuseAbort,
    ];

    /// The capability's name, as skip reasons and the parity file spell it.
    pub const fn name(self) -> &'static str {
        match self {
            Cap::PushInval => "PushInval",
            Cap::PushInvalFull => "PushInvalFull",
            Cap::PerCloseFlush => "PerCloseFlush",
            Cap::ClusterLocks => "ClusterLocks",
            Cap::Xattrs => "Xattrs",
            Cap::VirtualXattrsListed => "VirtualXattrsListed",
            Cap::HardLinks => "HardLinks",
            Cap::Fallocate => "Fallocate",
            Cap::SeekHole => "SeekHole",
            Cap::SpecialFiles => "SpecialFiles",
            Cap::CaseInsensitive => "CaseInsensitive",
            Cap::KeepOpenUnlinked => "KeepOpenUnlinked",
            Cap::FuseAbort => "FuseAbort",
        }
    }
}

impl std::fmt::Display for Cap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Cap {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        Cap::ALL
            .iter()
            .copied()
            .find(|c| c.name() == s)
            .ok_or_else(|| format!("unknown capability {s:?}"))
    }
}

impl FrontendCaps {
    /// Whether this frontend has `cap`.
    pub fn has(&self, cap: Cap) -> bool {
        match cap {
            Cap::PushInval => self.push_inval != PushInval::None,
            Cap::PushInvalFull => self.push_inval == PushInval::Full,
            Cap::PerCloseFlush => self.per_close_flush,
            Cap::ClusterLocks => self.cluster_locks,
            Cap::Xattrs => self.xattrs != XattrSupport::None,
            Cap::VirtualXattrsListed => self.virtual_xattrs_listed,
            Cap::HardLinks => self.hard_links,
            Cap::Fallocate => self.fallocate,
            Cap::SeekHole => self.seek_hole,
            Cap::SpecialFiles => self.special_files,
            Cap::CaseInsensitive => self.case == CasePolicy::InsensitivePreserving,
            Cap::KeepOpenUnlinked => self.open_unlinked == OpenUnlinked::Keep,
            Cap::FuseAbort => self.abortable,
        }
    }

    /// Every capability this frontend has, in [`Cap::ALL`] order.
    pub fn caps(&self) -> Vec<Cap> {
        Cap::ALL.iter().copied().filter(|c| self.has(*c)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::OpKind;

    #[test]
    fn caps_derive_from_the_declaration_and_round_trip_by_name() {
        let fuse = FrontendCaps::linux_fuse(true);
        let all = fuse.caps();
        for cap in [
            Cap::PushInval,
            Cap::PushInvalFull,
            Cap::PerCloseFlush,
            Cap::ClusterLocks,
            Cap::Xattrs,
            Cap::VirtualXattrsListed,
            Cap::HardLinks,
            Cap::Fallocate,
            Cap::SeekHole,
            Cap::SpecialFiles,
            Cap::KeepOpenUnlinked,
            Cap::FuseAbort,
        ] {
            assert!(all.contains(&cap), "{cap}");
        }
        assert!(!all.contains(&Cap::CaseInsensitive));
        assert!(!FrontendCaps::linux_fuse(false).has(Cap::ClusterLocks));
        let mut bare = fuse.clone();
        bare.push_inval = PushInval::Attr;
        bare.xattrs = XattrSupport::None;
        bare.abortable = false;
        bare.case = CasePolicy::InsensitivePreserving;
        assert!(bare.has(Cap::PushInval) && !bare.has(Cap::PushInvalFull));
        assert!(!bare.has(Cap::Xattrs) && !bare.has(Cap::FuseAbort));
        assert!(bare.has(Cap::CaseInsensitive));
        for &cap in Cap::ALL {
            assert_eq!(cap.name().parse::<Cap>(), Ok(cap));
            assert_eq!(cap.to_string(), cap.name());
        }
        assert!("Nope".parse::<Cap>().is_err());
    }

    #[test]
    fn linux_fuse_declares_what_the_fuse_adapter_supports() {
        for cluster in [false, true] {
            let caps = FrontendCaps::linux_fuse(cluster);
            assert_eq!(caps.cluster_locks, cluster);
            assert_eq!(caps.push_inval, PushInval::Full);
            assert!(caps.per_close_flush);
            assert_eq!(caps.xattrs, XattrSupport::Native);
            assert!(
                caps.virtual_xattrs_listed,
                "Linux lists user.constellation.{{rsize,rcount}}"
            );
            assert!(caps.hard_links && caps.fallocate && caps.seek_hole && caps.special_files);
            assert_eq!(caps.case, CasePolicy::Sensitive);
            assert_eq!(caps.open_unlinked, OpenUnlinked::Keep);
            assert!(caps.abortable);
            assert!(
                !caps.passthrough,
                "declared off; FUSE_INIT turns it on where the kernel agrees"
            );
            assert_eq!(caps.max_io, 16 << 20);
            for &kind in OpKind::ALL {
                assert!(caps.deferrable.contains(kind), "{kind:?}");
            }
        }
    }
}
