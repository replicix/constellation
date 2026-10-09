//! Policies (plan 31 §6.7): how a frontend's names, xattrs and identities
//! become the engine's.
//!
//! A view applies its [`PolicyStack`] beneath the [`crate::Vfs`] trait, so
//! a frontend never reimplements one: the same bytes get the same answer
//! whichever frontend delivered them. Each policy's Linux instance is the
//! reference — exactly what the FUSE adapter did inline before plan 31 C4
//! (`checked_name!`, `checked_xattr_name`, the `virtual_xattr` checks) —
//! and other platforms' instances are variations of it (plan 34's macOS
//! `user.X` ↔ `X` mapping, its unlisted virtual xattrs).

use crate::caps::{CasePolicy, FrontendCaps};
use crate::ctx::Caller;
use crate::name::{Name, XattrName, XattrNameBuf};
use constellation_types::Code;
use std::borrow::Cow;

/// POSIX `NAME_MAX`: the metadata plane accepts longer names, so the
/// view enforces it.
pub const NAME_MAX: usize = 255;

/// The virtual, read-only recursive size of a directory (plan 17).
pub const RSIZE_XATTR: &str = "user.constellation.rsize";
/// The virtual, read-only recursive file count of a directory (plan 17).
pub const RCOUNT_XATTR: &str = "user.constellation.rcount";

/// Directory entry names: frontend bytes → the engine's stored name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamePolicy {
    /// Longest name, in bytes as the frontend delivered it.
    pub max_len: usize,
    pub case: CasePolicy,
}

impl NamePolicy {
    /// Linux: any bytes, at most [`NAME_MAX`] of them, stored in
    /// [`crate::name::stored_form`], case-sensitive.
    pub const fn linux() -> Self {
        Self {
            max_len: NAME_MAX,
            case: CasePolicy::Sensitive,
        }
    }

    /// The stored form of `name`, or `NameTooLong`.
    pub fn check<'a>(&self, name: &'a Name) -> Result<Cow<'a, str>, Code> {
        if name.len() > self.max_len {
            return Err(Code::NameTooLong);
        }
        Ok(crate::name::stored_form(name.as_bytes()))
    }
}

/// Extended attribute names: namespaces, and the virtual attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XattrPolicy {
    /// The read-only attributes the view computes (never stored):
    /// setting or removing one is `Perm`.
    pub virtual_names: &'static [&'static str],
    /// Whether `listxattr` names the virtual attributes. Neither Linux nor
    /// macOS: tools that copy attributes enumerate every listed name
    /// (`cp -a`, `rsync -X`, `tar --xattrs`, `copyfile(3)`), so a listed
    /// virtual attribute was copied as a real one onto other file
    /// systems and refused (`EPERM`) when written back here. They are
    /// read by name (`getfattr -n user.constellation.rsize`).
    pub list_virtual: bool,
}

impl XattrPolicy {
    /// Linux: `user.*` and `security.*` for everyone, `trusted.*` for
    /// uid 0, the virtual `user.constellation.{rsize,rcount}` unlisted.
    pub const fn linux() -> Self {
        Self {
            virtual_names: &[RSIZE_XATTR, RCOUNT_XATTR],
            list_virtual: false,
        }
    }

    /// The stored form of `name` for `caller`: `Range` when empty or
    /// longer than 255 bytes, `Invalid` when not UTF-8, `Perm` for
    /// `trusted.*` unless uid 0 (FUSE carries no capability bits: uid 0
    /// stands for the kernel-authenticated privileged caller),
    /// `NotSupported` for any other namespace (`system.*`: no ACLs).
    ///
    /// `security.*` is stored like `user.*`: who may set which one (file
    /// capabilities need `CAP_SETFCAP`, the rest `CAP_SYS_ADMIN` or the
    /// LSM's say) is the kernel's check, made before the request reaches
    /// the file system. Refused, an absent `security.capability` — which
    /// the kernel asks for on every write, to drop privileges — read
    /// `EOPNOTSUPP` where other file systems answer `ENODATA`, and
    /// `setcap` failed.
    pub fn check_name(&self, name: &XattrName, caller: &Caller) -> Result<String, Code> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > 255 {
            return Err(Code::Range);
        }
        let name = std::str::from_utf8(bytes).map_err(|_| Code::Invalid)?;
        if name.starts_with("user.") || name.starts_with("security.") {
            return Ok(name.to_string());
        }
        if name.starts_with("trusted.") {
            return if caller.uid == 0 {
                Ok(name.to_string())
            } else {
                Err(Code::Perm)
            };
        }
        Err(Code::NotSupported)
    }

    /// Whether `name` is one of the virtual attributes.
    pub fn is_virtual(&self, name: &str) -> bool {
        self.virtual_names.contains(&name)
    }

    /// What `listxattr` answers for an inode storing `stored`: with the
    /// virtual names when they are listed (and never otherwise), sorted,
    /// without duplicates.
    pub fn listing(&self, mut stored: Vec<String>) -> Vec<XattrNameBuf> {
        if self.list_virtual {
            stored.extend(self.virtual_names.iter().map(|n| n.to_string()));
        } else {
            stored.retain(|n| !self.is_virtual(n));
        }
        stored.sort();
        stored.dedup();
        stored.into_iter().map(XattrNameBuf::from).collect()
    }
}

/// Frontend principals ↔ POSIX identities. POSIX uid/gid/mode stay the
/// canonical form in the engine; frontends map at their edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentityMap {
    /// The caller's uid and primary gid, as they are (Linux, macOS).
    Posix,
}

impl IdentityMap {
    /// The owner a new inode created by `caller` gets.
    pub fn owner(&self, caller: &Caller) -> (u32, u32) {
        match self {
            IdentityMap::Posix => (caller.uid, caller.gid),
        }
    }
}

/// The policies one view applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStack {
    pub names: NamePolicy,
    pub xattrs: XattrPolicy,
    pub identity: IdentityMap,
}

impl PolicyStack {
    /// The Linux reference stack.
    pub const fn linux() -> Self {
        Self {
            names: NamePolicy::linux(),
            xattrs: XattrPolicy::linux(),
            identity: IdentityMap::Posix,
        }
    }

    /// The stack a frontend with `caps` gets: the Linux reference, with
    /// the name comparison and the virtual-xattr listing it declares.
    pub fn for_caps(caps: &FrontendCaps) -> Self {
        let mut stack = Self::linux();
        stack.names.case = caps.case;
        stack.xattrs.list_virtual = caps.virtual_xattrs_listed;
        stack
    }
}

impl Default for PolicyStack {
    fn default() -> Self {
        Self::linux()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[XattrNameBuf]) -> Vec<&str> {
        list.iter()
            .map(|n| std::str::from_utf8(n.as_bytes()).unwrap())
            .collect()
    }

    #[test]
    fn a_frontend_that_lists_the_virtual_xattrs_lists_them_sorted_and_deduplicated() {
        let mut caps = FrontendCaps::linux_fuse(false);
        caps.virtual_xattrs_listed = true;
        let p = PolicyStack::for_caps(&caps).xattrs;
        assert_eq!(
            names(&p.listing(vec!["user.z".into(), "user.a".into(), "user.a".into()])),
            [
                "user.a",
                "user.constellation.rcount",
                "user.constellation.rsize",
                "user.z"
            ]
        );
        assert_eq!(
            names(&p.listing(Vec::new())),
            ["user.constellation.rcount", "user.constellation.rsize"]
        );
    }

    #[test]
    fn linux_never_lists_them() {
        let p = PolicyStack::for_caps(&FrontendCaps::linux_fuse(false)).xattrs;
        assert_eq!(
            names(&p.listing(vec!["user.b".into(), RSIZE_XATTR.into(), "user.a".into()])),
            ["user.a", "user.b"]
        );
        // Still virtual (readable by name, refused for set/remove).
        assert!(p.is_virtual(RSIZE_XATTR) && p.is_virtual(RCOUNT_XATTR));
        assert!(!p.is_virtual("user.b"));
        assert_eq!(
            PolicyStack::for_caps(&FrontendCaps::linux_fuse(true)),
            PolicyStack::linux()
        );
    }

    #[test]
    fn linux_xattr_namespaces() {
        let p = XattrPolicy::linux();
        let user = Caller::with_groups(1000, 1000, &[]);
        let root = Caller::root();
        let check = |n: &[u8], c: &Caller| p.check_name(XattrName::new(n), c);
        assert_eq!(check(b"user.x", &user), Ok("user.x".into()));
        assert_eq!(check(b"trusted.x", &root), Ok("trusted.x".into()));
        assert_eq!(check(b"trusted.x", &user), Err(Code::Perm));
        assert_eq!(check(b"security.x", &user), Ok("security.x".into()));
        assert_eq!(check(b"system.x", &root), Err(Code::NotSupported));
        assert_eq!(
            check(b"system.posix_acl_access", &root),
            Err(Code::NotSupported)
        );
        assert_eq!(check(b"", &root), Err(Code::Range));
        assert_eq!(check(&[b'u'; 256], &root), Err(Code::Range));
        assert_eq!(check(b"user.\xff", &root), Err(Code::Invalid));
    }

    #[test]
    fn linux_names_are_any_bytes_within_name_max() {
        let p = NamePolicy::linux();
        assert_eq!(p.check(Name::new(b"a")).unwrap(), "a");
        assert_eq!(p.check(Name::new(&[b'x'; 255])).unwrap().len(), 255);
        assert_eq!(p.check(Name::new(&[b'x'; 256])), Err(Code::NameTooLong));
        // Not UTF-8: distinct names stay distinct, and the limit is on
        // the delivered bytes.
        let a = p.check(Name::new(b"a\xffb")).unwrap();
        let b = p.check(Name::new(b"a\xfeb")).unwrap();
        assert_ne!(a, b);
        assert_eq!(&*crate::name::wire_bytes(&a), b"a\xffb");
        assert!(p.check(Name::new(&[0xff; 255])).is_ok());
        assert_eq!(p.check(Name::new(&[0xff; 256])), Err(Code::NameTooLong));
    }

    #[test]
    fn posix_identity_is_the_callers_own() {
        let c = Caller::with_groups(7, 8, &[9]);
        assert_eq!(IdentityMap::Posix.owner(&c), (7, 8));
    }
}
