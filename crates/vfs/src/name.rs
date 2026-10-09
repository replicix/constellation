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

use std::borrow::{Borrow, Cow};
use std::fmt;
use std::ops::Deref;

/// The first of the 128 code points that stand for the bytes 0x80..=0xFF
/// of a name that is not UTF-8 (U+10FF80..=U+10FFFF, the end of the
/// supplementary private use area B).
const ESCAPED: u32 = 0x10_FF80;

fn is_escape(c: char) -> bool {
    u32::from(c) >= ESCAPED
}

fn push_escaped(out: &mut String, byte: u8) {
    let c = char::from_u32(ESCAPED - 0x80 + u32::from(byte)).expect("U+10FF80..=U+10FFFF");
    out.push(c);
}

fn push_valid(out: &mut String, valid: &str) {
    for c in valid.chars() {
        if is_escape(c) {
            for byte in c.encode_utf8(&mut [0; 4]).bytes() {
                push_escaped(out, byte);
            }
        } else {
            out.push(c);
        }
    }
}

/// The stored (UTF-8) form of a name or a symlink target the frontend
/// delivered as `bytes`, from which [`wire_bytes`] gives them back
/// exactly. UTF-8 is stored as is; each byte of an invalid sequence is
/// stored as a code point of U+10FF80..=U+10FFFF, and so is each byte of
/// any of those code points the input itself holds, so that no two
/// byte strings share a stored form. (It was a lossy conversion: every
/// invalid sequence became U+FFFD, so `a\xffb` and `a\xfeb` were one
/// file, and a name was measured against `NAME_MAX` after each invalid
/// byte had grown to three.)
pub fn stored_form(bytes: &[u8]) -> Cow<'_, str> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if !s.chars().any(is_escape) {
            return Cow::Borrowed(s);
        }
    }
    let mut out = String::with_capacity(bytes.len());
    let mut rest = bytes;
    loop {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                push_valid(&mut out, valid);
                return Cow::Owned(out);
            }
            Err(e) => {
                let (valid, after) = rest.split_at(e.valid_up_to());
                push_valid(
                    &mut out,
                    std::str::from_utf8(valid).expect("valid up to here"),
                );
                let bad = e.error_len().unwrap_or(after.len());
                for &byte in &after[..bad] {
                    push_escaped(&mut out, byte);
                }
                rest = &after[bad..];
            }
        }
    }
}

/// The bytes a stored name or symlink target stands for: the inverse of
/// [`stored_form`].
pub fn wire_bytes(stored: &str) -> Cow<'_, [u8]> {
    if !stored.chars().any(is_escape) {
        return Cow::Borrowed(stored.as_bytes());
    }
    let mut out = Vec::with_capacity(stored.len());
    for c in stored.chars() {
        if is_escape(c) {
            out.push((u32::from(c) - ESCAPED + 0x80) as u8);
        } else {
            out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
        }
    }
    Cow::Owned(out)
}

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
