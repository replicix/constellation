//! Names as the frontend delivered them: bytes, not strings.
//!
//! A directory entry name and an xattr name arrive as whatever byte string
//! the caller's platform uses. The contract carries them unvalidated, as
//! bytes; turning one into the engine's stored form (UTF-8, `NAME_MAX`, a
//! namespace check) is a policy ([`crate::NamePolicy`],
//! [`crate::XattrPolicy`]) applied beneath the trait, so every frontend
//! gets the same answer for the same bytes. Each comes as a borrowed,
//! unsized view (`&Name`, like `&Path`: free to build from a request
//! buffer) and an owned buffer (`NameBuf`, for results and events).

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

macro_rules! byte_name {
    ($(#[$doc:meta])* $name:ident, $(#[$bufdoc:meta])* $buf:ident) => {
        $(#[$doc])*
        #[repr(transparent)]
        #[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name([u8]);

        impl $name {
            /// View `bytes` as a name (no copy, no validation).
            pub fn new<B: AsRef<[u8]> + ?Sized>(bytes: &B) -> &Self {
                let bytes: &[u8] = bytes.as_ref();
                // SAFETY: `repr(transparent)` over `[u8]`: the same
                // layout and pointer metadata, so the cast is sound (the
                // `std::path::Path` pattern).
                unsafe { &*(bytes as *const [u8] as *const Self) }
            }

            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            pub fn to_buf(&self) -> $buf {
                $buf(self.0.to_vec())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&String::from_utf8_lossy(&self.0), f)
            }
        }

        impl AsRef<[u8]> for $name {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        $(#[$bufdoc])*
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
        pub struct $buf(Vec<u8>);

        impl $buf {
            pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
                Self(bytes.into())
            }

            pub fn into_bytes(self) -> Vec<u8> {
                self.0
            }
        }

        impl Deref for $buf {
            type Target = $name;

            fn deref(&self) -> &$name {
                $name::new(&self.0)
            }
        }

        impl Borrow<$name> for $buf {
            fn borrow(&self) -> &$name {
                self
            }
        }

        impl AsRef<[u8]> for $buf {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl From<Vec<u8>> for $buf {
            fn from(bytes: Vec<u8>) -> Self {
                Self(bytes)
            }
        }

        impl From<String> for $buf {
            fn from(name: String) -> Self {
                Self(name.into_bytes())
            }
        }

        impl From<&str> for $buf {
            fn from(name: &str) -> Self {
                Self(name.as_bytes().to_vec())
            }
        }

        impl fmt::Debug for $buf {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&**self, f)
            }
        }
    };
}

byte_name!(
    /// One directory entry name (a path component), as the frontend
    /// delivered it.
    Name,
    /// An owned [`Name`].
    NameBuf
);

byte_name!(
    /// An extended attribute's name, as the frontend delivered it
    /// (namespace prefix included on Linux: `user.color`).
    XattrName,
    /// An owned [`XattrName`].
    XattrNameBuf
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_a_free_view_of_its_bytes_and_owns_on_demand() {
        let raw = b"caf\xc3\xa9";
        let name = Name::new(raw);
        assert_eq!(name.as_bytes(), raw);
        assert_eq!(name.len(), 5);
        assert!(std::ptr::eq(name.as_bytes().as_ptr(), raw.as_ptr()));
        let owned = name.to_buf();
        assert_eq!(&*owned, name);
        assert_eq!(format!("{owned:?}"), "\"café\"");
        let x: XattrNameBuf = "user.a".into();
        assert!(x.as_bytes() < XattrName::new("user.b").as_bytes());
    }
}
