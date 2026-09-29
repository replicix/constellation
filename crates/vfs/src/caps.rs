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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ctx::OpKind;

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
            assert_eq!(caps.max_io, 16 << 20);
            for &kind in OpKind::ALL {
                assert!(caps.deferrable.contains(kind), "{kind:?}");
            }
        }
    }
}
