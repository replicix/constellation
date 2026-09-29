//! [`Rdev`]: a device number as a portable `(major, minor)` pair (plan 31
//! §7).
//!
//! A character or block device node's `rdev` is stored in the journal, in
//! S3 log segments and in the published metadata tree, and every OS packs
//! a `(major, minor)` pair into its `dev_t` differently: glibc's 64-bit
//! `makedev`, the Linux kernel's 32-bit `new_encode_dev` (what FUSE
//! carries), Darwin's 8/24-bit split. Storing any one of those packings
//! would make the stored value mean a different device on another OS, so
//! the stored and wire form is the pair itself, and each boundary packs
//! and unpacks its own encoding.
//!
//! ## The Linux encodings
//!
//! - [`to_linux_rdev`]/[`from_linux_rdev`]: glibc's `makedev`/`major`/
//!   `minor`, the 64-bit `st_rdev` userspace sees from `stat(2)` and passes
//!   to `mknod(2)`. A bijection between `u64` and `(u32, u32)`, so lossless
//!   both ways.
//! - [`to_linux_fuse_rdev`]/[`from_linux_fuse_rdev`]: the kernel's 32-bit
//!   `new_encode_dev`/`new_decode_dev`, which is what the FUSE protocol
//!   carries in `fuse_mknod_in.rdev` and expects back in `fuse_attr.rdev`
//!   (`fs/fuse/dir.c`, `fs/fuse/inode.c`); `fuser` passes both through
//!   untouched as a `u32`. Majors are 12 bits and minors 20 bits there,
//!   which is the kernel's whole `dev_t` range, so every value the kernel
//!   hands a FUSE daemon unpacks and repacks to the same `u32`. A pair
//!   outside that range (only possible if another OS created the node) is
//!   masked to it, which is lossy but keeps one field from bleeding into
//!   the other.
//!
//! Within the kernel's range the FUSE encoding is exactly the low 32 bits
//! of glibc's, which is why the pre-plan-31 code could store the FUSE
//! `u32` widened to `u64` and hand it back truncated.

use serde::{Deserialize, Serialize};

/// A device number. `Rdev::default()` (0, 0) is "no device", which is what
/// every non-device inode carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Rdev {
    pub major: u32,
    pub minor: u32,
}

impl Rdev {
    pub const fn new(major: u32, minor: u32) -> Rdev {
        Rdev { major, minor }
    }
}

/// glibc `makedev(major, minor)`: the 64-bit Linux userspace `dev_t`.
pub const fn to_linux_rdev(rdev: Rdev) -> u64 {
    let major = rdev.major as u64;
    let minor = rdev.minor as u64;
    ((major & 0x0000_0fff) << 8)
        | ((major & 0xffff_f000) << 32)
        | (minor & 0x0000_00ff)
        | ((minor & 0xffff_ff00) << 12)
}

/// glibc `major(dev)`/`minor(dev)`: the inverse of [`to_linux_rdev`].
pub const fn from_linux_rdev(dev: u64) -> Rdev {
    Rdev {
        major: (((dev >> 8) & 0x0000_0fff) | ((dev >> 32) & 0xffff_f000)) as u32,
        minor: ((dev & 0x0000_00ff) | ((dev >> 12) & 0xffff_ff00)) as u32,
    }
}

/// Largest major the kernel's 32-bit encoding holds (12 bits).
pub const LINUX_FUSE_MAJOR_MAX: u32 = 0xfff;
/// Largest minor the kernel's 32-bit encoding holds (20 bits).
pub const LINUX_FUSE_MINOR_MAX: u32 = 0xf_ffff;

/// The kernel's `new_encode_dev`: the 32-bit `rdev` a Linux FUSE reply
/// carries. Out-of-range fields are masked (see the module docs).
pub const fn to_linux_fuse_rdev(rdev: Rdev) -> u32 {
    let major = rdev.major & LINUX_FUSE_MAJOR_MAX;
    let minor = rdev.minor & LINUX_FUSE_MINOR_MAX;
    (minor & 0xff) | (major << 8) | ((minor & !0xff) << 12)
}

/// The kernel's `new_decode_dev`: the `(major, minor)` in a Linux FUSE
/// request's 32-bit `rdev`. Every `u32` decodes, and re-encodes to itself.
pub const fn from_linux_fuse_rdev(dev: u32) -> Rdev {
    Rdev {
        major: (dev & 0xf_ff00) >> 8,
        minor: (dev & 0xff) | ((dev >> 12) & 0xf_ff00),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic spread of `u32`s: every boundary-ish value plus an
    /// LCG walk, so the loops cover the bit-field seams without taking the
    /// 2^32 steps an exhaustive walk would.
    fn samples() -> Vec<u32> {
        let mut v: Vec<u32> = vec![0, 1, 0xff, 0x100, 0xfff, 0x1000, 0xf_ffff, 0x10_0000];
        v.extend([
            0xff_ffff,
            0x100_0000,
            0xfff_ffff,
            0x1000_0000,
            u32::MAX - 1,
            u32::MAX,
        ]);
        for bit in 0..32 {
            v.push(1 << bit);
            v.push((1u32 << bit).wrapping_sub(1));
        }
        let mut x: u32 = 0x9e37_79b9;
        for _ in 0..20_000 {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            v.push(x);
        }
        v
    }

    #[test]
    fn glibc_packing_round_trips_every_pair() {
        let s = samples();
        for &major in s.iter().step_by(37) {
            for &minor in s.iter().step_by(41) {
                let r = Rdev::new(major, minor);
                assert_eq!(from_linux_rdev(to_linux_rdev(r)), r);
            }
        }
    }

    #[test]
    fn glibc_packing_round_trips_every_dev() {
        let s = samples();
        for &hi in s.iter().step_by(13) {
            for &lo in s.iter().step_by(17) {
                let dev = ((hi as u64) << 32) | lo as u64;
                assert_eq!(to_linux_rdev(from_linux_rdev(dev)), dev, "{dev:#x}");
            }
        }
    }

    /// Known values from glibc's `makedev` (`<sys/sysmacros.h>`).
    #[test]
    fn glibc_packing_goldens() {
        assert_eq!(to_linux_rdev(Rdev::new(1, 3)), 0x103); // /dev/null
        assert_eq!(to_linux_rdev(Rdev::new(8, 1)), 0x801); // /dev/sda1
        assert_eq!(to_linux_rdev(Rdev::new(259, 0x12345)), 0x1231_0345);
        assert_eq!(
            to_linux_rdev(Rdev::new(0x1234_5678, 0)),
            0x1234_5000_0006_7800
        );
        assert_eq!(
            to_linux_rdev(Rdev::new(0, 0xffff_ffff)),
            0x0000_0fff_fff0_00ff
        );
    }

    #[test]
    fn fuse_encoding_round_trips_every_u32() {
        for dev in samples() {
            let r = from_linux_fuse_rdev(dev);
            assert!(r.major <= LINUX_FUSE_MAJOR_MAX && r.minor <= LINUX_FUSE_MINOR_MAX);
            assert_eq!(to_linux_fuse_rdev(r), dev, "{dev:#x}");
        }
    }

    #[test]
    fn fuse_encoding_round_trips_every_kernel_pair() {
        for major in 0..=LINUX_FUSE_MAJOR_MAX {
            for minor in [0, 1, 0xff, 0x100, 0x1234, 0xf_ff00, LINUX_FUSE_MINOR_MAX] {
                let r = Rdev::new(major, minor);
                assert_eq!(from_linux_fuse_rdev(to_linux_fuse_rdev(r)), r);
                // Within the kernel's range the FUSE encoding is glibc's
                // low 32 bits (the pre-plan-31 widening relied on this).
                assert_eq!(to_linux_fuse_rdev(r) as u64, to_linux_rdev(r));
            }
        }
    }

    #[test]
    fn fuse_encoding_masks_out_of_range_fields() {
        let r = Rdev::new(0x1001, 0x10_0002);
        assert_eq!(from_linux_fuse_rdev(to_linux_fuse_rdev(r)), Rdev::new(1, 2));
    }

    #[test]
    fn serde_is_the_pair() {
        let r = Rdev::new(8, 1);
        assert_eq!(postcard::to_allocvec(&r).unwrap(), vec![8, 1]);
        assert_eq!(
            serde_json::to_string(&r).unwrap(),
            r#"{"major":8,"minor":1}"#
        );
        let back: Rdev = postcard::from_bytes(&postcard::to_allocvec(&r).unwrap()).unwrap();
        assert_eq!(back, r);
    }
}
