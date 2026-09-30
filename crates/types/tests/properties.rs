//! Property tests (plan 31 §8) for the two portable values: [`Code`]'s
//! wire, Linux and Darwin encodings and [`Rdev`]'s packings, over
//! arbitrary values rather than the hand-picked ones of the unit tests.

use constellation_types::rdev::{
    from_linux_fuse_rdev, from_linux_rdev, to_linux_fuse_rdev, to_linux_rdev, LINUX_FUSE_MAJOR_MAX,
    LINUX_FUSE_MINOR_MAX,
};
use constellation_types::{Code, Rdev};
use proptest::prelude::*;
use std::collections::HashSet;

fn any_code() -> impl Strategy<Value = Code> {
    (0..Code::ALL.len()).prop_map(|i| Code::ALL[i])
}

fn linux_numbers() -> HashSet<i32> {
    Code::ALL.iter().map(|c| c.to_linux_errno()).collect()
}

fn darwin_numbers() -> HashSet<i32> {
    // The forward table plus the two aliases the reverse direction accepts.
    let mut set: HashSet<i32> = Code::ALL.iter().map(|c| c.to_darwin_errno()).collect();
    set.insert(102);
    set.insert(96);
    set
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn a_code_round_trips_through_every_encoding(code in any_code()) {
        prop_assert_eq!(Code::from_wire(code.to_wire()), code);
        prop_assert_eq!(Code::from_linux_errno(code.to_linux_errno()), code);
        prop_assert_eq!(Code::from_darwin_errno(code.to_darwin_errno()), code);
        prop_assert_eq!(Code::try_from_linux_errno(code.to_linux_errno()), Some(code));
        prop_assert_eq!(Code::try_from_darwin_errno(code.to_darwin_errno()), Some(code));
        // The portable number is not any OS's: it is the discriminant.
        prop_assert_eq!(code.to_wire(), code as u16);
        prop_assert!(code.to_linux_errno() > 0 && code.to_darwin_errno() > 0);
        prop_assert!(!code.message().is_empty() && code.posix_name().starts_with('E'));
    }

    #[test]
    fn an_unknown_wire_number_is_io_and_a_known_one_is_itself(n in any::<u16>()) {
        let known = Code::ALL.iter().find(|c| c.to_wire() == n);
        match known {
            Some(code) => prop_assert_eq!(Code::from_wire(n), *code),
            None => prop_assert_eq!(Code::from_wire(n), Code::Io),
        }
        // Whatever came in, what goes out is a number this build knows.
        let back = Code::from_wire(n).to_wire();
        prop_assert!(Code::ALL.iter().any(|c| c.to_wire() == back));
    }

    #[test]
    fn a_native_errno_maps_to_its_code_or_io(n in -8i32..2048) {
        let linux = Code::from_linux_errno(n);
        if linux_numbers().contains(&n) {
            prop_assert_eq!(linux.to_linux_errno(), n);
            prop_assert_eq!(Code::try_from_linux_errno(n), Some(linux));
        } else {
            prop_assert_eq!(linux, Code::Io);
            prop_assert_eq!(Code::try_from_linux_errno(n), None);
        }
        let darwin = Code::from_darwin_errno(n);
        if darwin_numbers().contains(&n) {
            prop_assert_eq!(Code::try_from_darwin_errno(n), Some(darwin));
            // The aliases decode to the canonical code, which leaves as
            // the canonical number.
            let canonical = darwin.to_darwin_errno();
            prop_assert_eq!(Code::from_darwin_errno(canonical), darwin);
        } else {
            prop_assert_eq!(darwin, Code::Io);
            prop_assert_eq!(Code::try_from_darwin_errno(n), None);
        }
    }

    #[test]
    fn converting_between_the_oses_goes_through_the_code(code in any_code()) {
        // Linux -> Code -> Darwin -> Code -> Linux is the identity on the
        // table: the Code is what is portable.
        let via_darwin = Code::from_darwin_errno(
            Code::from_linux_errno(code.to_linux_errno()).to_darwin_errno(),
        );
        prop_assert_eq!(via_darwin.to_linux_errno(), code.to_linux_errno());
    }

    #[test]
    fn serde_carries_exactly_the_wire_number(code in any_code(), n in any::<u16>()) {
        let json = serde_json::to_string(&code).unwrap();
        prop_assert_eq!(json, code.to_wire().to_string());
        let bytes = postcard::to_allocvec(&code).unwrap();
        let back: Code = postcard::from_bytes(&bytes).unwrap();
        prop_assert_eq!(back, code);
        // An arbitrary number decodes as `from_wire` says, never an error.
        let decoded: Code = serde_json::from_str(&n.to_string()).unwrap();
        prop_assert_eq!(decoded, Code::from_wire(n));
    }

    #[test]
    fn glibc_rdev_packing_is_a_bijection(major in any::<u32>(), minor in any::<u32>(), dev in any::<u64>()) {
        let r = Rdev::new(major, minor);
        prop_assert_eq!(from_linux_rdev(to_linux_rdev(r)), r);
        prop_assert_eq!(to_linux_rdev(from_linux_rdev(dev)), dev);
    }

    #[test]
    fn the_fuse_rdev_encoding_keeps_the_kernels_range_and_masks_the_rest(
        major in any::<u32>(),
        minor in any::<u32>(),
        dev in any::<u32>(),
    ) {
        // Every u32 the kernel sends decodes and re-encodes to itself.
        let r = from_linux_fuse_rdev(dev);
        prop_assert!(r.major <= LINUX_FUSE_MAJOR_MAX && r.minor <= LINUX_FUSE_MINOR_MAX);
        prop_assert_eq!(to_linux_fuse_rdev(r), dev);
        // A pair round-trips exactly when it is in range, else masked.
        let pair = Rdev::new(major, minor);
        let back = from_linux_fuse_rdev(to_linux_fuse_rdev(pair));
        prop_assert_eq!(back.major, major & LINUX_FUSE_MAJOR_MAX);
        prop_assert_eq!(back.minor, minor & LINUX_FUSE_MINOR_MAX);
        if major <= LINUX_FUSE_MAJOR_MAX && minor <= LINUX_FUSE_MINOR_MAX {
            prop_assert_eq!(back, pair);
            // ... and is then the low 32 bits of glibc's packing.
            prop_assert_eq!(to_linux_fuse_rdev(pair) as u64, to_linux_rdev(pair));
        }
    }

    #[test]
    fn rdev_serde_is_the_pair(major in any::<u32>(), minor in any::<u32>()) {
        let r = Rdev::new(major, minor);
        let bytes = postcard::to_allocvec(&r).unwrap();
        let back: Rdev = postcard::from_bytes(&bytes).unwrap();
        prop_assert_eq!(back, r);
        let json = serde_json::to_string(&r).unwrap();
        prop_assert_eq!(json, format!("{{\"major\":{major},\"minor\":{minor}}}"));
    }
}
