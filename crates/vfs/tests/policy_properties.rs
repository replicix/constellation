//! Property tests (plan 31 §8) for the policies and the small value types
//! of the contract: names, xattrs, identities and stacks, over arbitrary
//! bytes and capabilities.

use constellation_types::Code;
use constellation_vfs::{
    Caller, CasePolicy, FrontendCaps, IdentityMap, Name, NamePolicy, OpKind, OpKindSet, OpenFlags,
    OpenUnlinked, PolicyStack, PushInval, ReadData, SetXattrFlags, XattrName, XattrPolicy,
    XattrSupport, NAME_MAX,
};
use proptest::prelude::*;
use std::collections::BTreeSet;

const RSIZE: &str = "user.constellation.rsize";
const RCOUNT: &str = "user.constellation.rcount";

fn any_caps() -> impl Strategy<Value = FrontendCaps> {
    (
        (
            0..3usize,
            any::<bool>(),
            any::<bool>(),
            0..3usize,
            any::<bool>(),
        ),
        (
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            0..2usize,
        ),
        (any::<u32>(), 0..3usize, any::<bool>()),
    )
        .prop_map(
            |(
                (push, flush, locks, xattrs, virt),
                (links, fallocate, seek, special, case),
                (max_io, unlinked, abortable),
            )| {
                FrontendCaps {
                    push_inval: [PushInval::None, PushInval::Attr, PushInval::Full][push],
                    per_close_flush: flush,
                    cluster_locks: locks,
                    xattrs: [
                        XattrSupport::None,
                        XattrSupport::Native,
                        XattrSupport::Named,
                    ][xattrs],
                    virtual_xattrs_listed: virt,
                    hard_links: links,
                    fallocate,
                    seek_hole: seek,
                    special_files: special,
                    case: [CasePolicy::Sensitive, CasePolicy::InsensitivePreserving][case],
                    max_io,
                    deferrable: OpKindSet::ALL,
                    open_unlinked: [
                        OpenUnlinked::Keep,
                        OpenUnlinked::SillyRename,
                        OpenUnlinked::DeleteOnClose,
                    ][unlinked],
                    abortable,
                }
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn a_name_is_stored_lossily_within_name_max_and_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..400)) {
        let policy = NamePolicy::linux();
        let lossy = String::from_utf8_lossy(&bytes).into_owned();
        match policy.check(Name::new(&bytes)) {
            Ok(stored) => {
                prop_assert!(stored.len() <= NAME_MAX);
                prop_assert_eq!(&*stored, lossy.as_str());
                // Valid UTF-8 is stored untouched.
                if std::str::from_utf8(&bytes).is_ok() {
                    prop_assert_eq!(stored.as_bytes(), &bytes[..]);
                }
            }
            Err(code) => {
                prop_assert_eq!(code, Code::NameTooLong);
                prop_assert!(lossy.len() > NAME_MAX);
            }
        }
    }

    #[test]
    fn a_name_within_the_limit_is_the_identity(s in "[a-zA-Z0-9._ -]{1,255}") {
        let stored = NamePolicy::linux().check(Name::new(&s)).unwrap();
        prop_assert_eq!(&*stored, s.as_str());
    }

    #[test]
    fn xattr_names_are_classified_exactly_and_kept_as_given(
        bytes in proptest::collection::vec(any::<u8>(), 0..300),
        uid in prop_oneof![Just(0u32), 1u32..100_000],
    ) {
        let policy = XattrPolicy::linux();
        let caller = Caller::with_groups(uid, 1, &[]);
        let got = policy.check_name(XattrName::new(&bytes), &caller);
        let expected: Result<String, Code> = if bytes.is_empty() || bytes.len() > 255 {
            Err(Code::Range)
        } else {
            match std::str::from_utf8(&bytes) {
                Err(_) => Err(Code::Invalid),
                Ok(s) if s.starts_with("user.") => Ok(s.to_string()),
                Ok(s) if s.starts_with("trusted.") => {
                    if uid == 0 { Ok(s.to_string()) } else { Err(Code::Perm) }
                }
                Ok(_) => Err(Code::NotSupported),
            }
        };
        prop_assert_eq!(&got, &expected);
        // The mapping is the identity on what it accepts: nothing is
        // rewritten on the way in.
        if let Ok(stored) = got {
            prop_assert_eq!(stored.as_bytes(), &bytes[..]);
        }
    }

    #[test]
    fn a_listing_is_sorted_unique_and_leaks_no_hidden_name(
        stored in proptest::collection::vec(
            prop_oneof![
                "user\\.[a-z]{1,6}",
                "trusted\\.[a-z]{1,6}",
                Just(RSIZE.to_string()),
                Just(RCOUNT.to_string()),
            ],
            0..12,
        ),
        list_virtual in any::<bool>(),
    ) {
        let policy = XattrPolicy { list_virtual, ..XattrPolicy::linux() };
        let listed: Vec<String> = policy
            .listing(stored.clone())
            .into_iter()
            .map(|n| String::from_utf8(n.into_bytes()).unwrap())
            .collect();
        // Sorted, no duplicates.
        let mut sorted = listed.clone();
        sorted.sort();
        sorted.dedup();
        prop_assert_eq!(&listed, &sorted);
        // Virtual names appear iff the policy lists them, whatever the
        // inode stored.
        for v in [RSIZE, RCOUNT] {
            prop_assert_eq!(listed.iter().any(|n| n == v), list_virtual, "{}", v);
        }
        // Every real stored name is listed; nothing else is (bar the
        // virtual ones).
        let real: BTreeSet<&String> = stored.iter().filter(|n| !policy.is_virtual(n)).collect();
        let listed_real: BTreeSet<&String> = listed.iter().filter(|n| !policy.is_virtual(n)).collect();
        prop_assert_eq!(real, listed_real);
    }

    #[test]
    fn the_linux_stack_is_a_no_op_on_what_it_needs_no_change_for(
        uid in any::<u32>(),
        gid in any::<u32>(),
        names in proptest::collection::btree_set("user\\.[a-z]{1,6}", 0..8),
    ) {
        let stack = PolicyStack::linux();
        prop_assert_eq!(&stack, &PolicyStack::default());
        // Identity: the caller's own ids.
        let caller = Caller::with_groups(uid, gid, &[]);
        prop_assert_eq!(stack.identity.owner(&caller), (uid, gid));
        prop_assert_eq!(IdentityMap::Posix.owner(&caller), (uid, gid));
        // Xattr names: unchanged in, and the listing is the stored set plus
        // the two virtual names, in order.
        let stored: Vec<String> = names.iter().cloned().collect();
        let listed: Vec<String> = stack
            .xattrs
            .listing(stored.clone())
            .into_iter()
            .map(|n| String::from_utf8(n.into_bytes()).unwrap())
            .collect();
        let mut want = stored;
        want.push(RSIZE.to_string());
        want.push(RCOUNT.to_string());
        want.sort();
        prop_assert_eq!(listed, want);
    }

    #[test]
    fn a_stack_for_caps_is_linux_with_only_the_declared_differences(caps in any_caps()) {
        let stack = PolicyStack::for_caps(&caps);
        prop_assert_eq!(stack.names.case, caps.case);
        prop_assert_eq!(stack.names.max_len, NAME_MAX);
        prop_assert_eq!(stack.xattrs.list_virtual, caps.virtual_xattrs_listed);
        prop_assert_eq!(stack.xattrs.virtual_names, XattrPolicy::linux().virtual_names);
        prop_assert_eq!(stack.identity, IdentityMap::Posix);
        // Undoing the two differences gives the reference stack back.
        let mut undone = stack;
        undone.names.case = CasePolicy::Sensitive;
        undone.xattrs.list_virtual = true;
        prop_assert_eq!(undone, PolicyStack::linux());
    }

    #[test]
    fn a_caller_is_in_its_primary_and_listed_groups_only(
        gid in any::<u32>(),
        gids in proptest::collection::vec(any::<u32>(), 0..10),
        probe in any::<u32>(),
    ) {
        let caller = Caller::with_groups(1, gid, &gids);
        prop_assert!(caller.in_group(gid));
        prop_assert_eq!(caller.in_group(probe), probe == gid || gids.contains(&probe));
        // A caller without a pid never asks the host.
        let plain = Caller::new(1, gid, None);
        prop_assert_eq!(plain.in_group(probe), probe == gid);
    }

    #[test]
    fn op_kind_sets_hold_exactly_what_was_put_in(picks in proptest::collection::vec(any::<bool>(), OpKind::ALL.len())) {
        let chosen: Vec<OpKind> = OpKind::ALL.iter().zip(&picks).filter(|(_, p)| **p).map(|(k, _)| *k).collect();
        let set = OpKindSet::of(&chosen);
        for (kind, pick) in OpKind::ALL.iter().zip(&picks) {
            prop_assert_eq!(set.contains(*kind), *pick);
        }
        prop_assert_eq!(set.iter().collect::<Vec<_>>(), chosen);
    }

    #[test]
    fn flag_sets_are_a_boolean_algebra(a in 0u32..128, b in 0u32..128) {
        let (fa, fb) = (OpenFlags::from_bits(a), OpenFlags::from_bits(b));
        prop_assert_eq!(fa.union(fb).bits(), a | b);
        prop_assert!(fa.union(fb).contains(fa) && fa.union(fb).contains(fb));
        prop_assert_eq!(fa.intersects(fb), a & b != 0);
        prop_assert_eq!(fa.contains(fb), a & b == b);
        prop_assert_eq!(fa | fb, fb | fa);
    }

    #[test]
    fn setxattr_flags_name_one_mode_or_are_invalid(bits in 0u32..8) {
        let flags = SetXattrFlags::from_bits(bits);
        let valid = matches!(bits, 0..=2);
        prop_assert_eq!(flags.mode().is_ok(), valid);
        if !valid {
            prop_assert_eq!(flags.mode(), Err(Code::Invalid));
        }
    }

    #[test]
    fn read_data_is_the_concatenation_of_its_segments(
        segments in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..50), 0..6),
    ) {
        let mut data = ReadData::default();
        let mut want = Vec::new();
        for s in &segments {
            data.push(bytes::Bytes::from(s.clone()));
            want.extend_from_slice(s);
        }
        prop_assert_eq!(data.len(), want.len());
        prop_assert_eq!(data.is_empty(), want.is_empty());
        prop_assert_eq!(&*data.contiguous(), &want[..]);
    }
}
