//! The portable vocabulary every Constellation layer speaks (plan 31 §4).
//!
//! This crate sits below everything else — the metadata replica, the
//! authority core, the P2P messages, the control API and every frontend —
//! and depends on nothing but `serde`. That is the point of it: a value
//! defined here means the same thing on every OS, on the wire, and in the
//! journal, and the translation to one platform's native representation
//! happens at that platform's boundary and nowhere else.
//!
//! For now it holds the two values that used to leak a Linux encoding into
//! shared state, and the protocol clocks:
//!
//! - [`Code`] ([`errno`]): the portable errno. It has its own fixed wire
//!   discriminants instead of reusing Linux's errno numbers, plus pure
//!   numeric Linux and Darwin conversion tables.
//! - [`Rdev`] ([`rdev`]): a device number as a `(major, minor)` pair instead
//!   of glibc's `makedev` packing, plus the Linux encodings (the 64-bit
//!   userspace `dev_t` and the 32-bit one FUSE carries).
//! - [`clock`]: the three readings a node takes of time (WALL, HONOUR and
//!   WAIT, plan 43 C7-H1) and the types that keep a stamp and its comparison
//!   on one of them. Pure; the host's sources live in
//!   `constellation_platform::clock`.
//!
//! Neither of the first two formats is compatible with the pre-plan-31 tree (plan 31 §2: no
//! backward compatibility). Buckets and state directories written before it
//! must be recreated.

pub mod clock;
pub mod errno;
pub mod rdev;

pub use errno::Code;
pub use rdev::Rdev;
