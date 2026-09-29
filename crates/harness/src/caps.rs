//! Scenario capabilities (plan 31 §8): which scenarios a `--frontend` can
//! run.
//!
//! [`Cap`] is `constellation-vfs`'s, derived from the frontend's
//! [`FrontendCaps`] declaration ([`caps_of`]) and never kept as a second
//! table: `Cap::FuseAbort` is `FrontendCaps::abortable`, `Cap::ClusterLocks`
//! is `cluster_locks`, and so on. A [`crate::scenarios::Scenario`] lists the
//! caps it needs next to the host binaries it `requires`; `harness run`
//! skips one the selected frontend lacks, with a reason that names the
//! capability as a whole word (`requires capability ClusterLocks`), which is
//! what `tests/parity.py`'s wildcard matcher (`is_cap_skip`) looks for — a
//! capability skip may differ from the reference lane, a missing tool
//! (`… not installed`) may not.

use anyhow::{bail, Result};
pub use constellation_vfs::Cap;
use constellation_vfs::FrontendCaps;

/// The capabilities `caps` declares.
pub fn caps_of(caps: &FrontendCaps) -> Vec<Cap> {
    caps.caps()
}

/// The [`FrontendCaps`] of a `--frontend`. `fuse` is Linux FUSE with the
/// cluster locks it can forward (whether a scenario mounts with `--locks
/// cluster` is the scenario's own choice; the frontend *can*).
pub fn frontend_caps(frontend: &str) -> Result<FrontendCaps> {
    match frontend {
        "fuse" => Ok(FrontendCaps::linux_fuse(true)),
        other => bail!("no capability declaration for --frontend {other:?}"),
    }
}

/// Why a scenario needing `needs` cannot run on a frontend with `have`:
/// `None` when it can. The reason names every missing capability.
pub fn skip_reason(needs: &[Cap], have: &[Cap]) -> Option<String> {
    let missing: Vec<&str> = needs
        .iter()
        .filter(|c| !have.contains(c))
        .map(|c| c.name())
        .collect();
    if missing.is_empty() {
        None
    } else {
        Some(format!("requires capability {}", missing.join(", ")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenarios::{KNOWN_BUG_REPROS, SCENARIOS};
    use constellation_vfs::{PushInval, XattrSupport};

    /// `tests/parity.py`'s `is_cap_skip`: the cap as a whole word (not
    /// inside a longer identifier or hyphenated name), and not a missing
    /// tool.
    fn is_cap_skip(reason: &str, cap: &str) -> bool {
        if reason.ends_with(" not installed") {
            return false;
        }
        let word = |c: char| c.is_alphanumeric() || c == '_' || c == '-';
        reason.match_indices(cap).any(|(at, _)| {
            let before = reason[..at].chars().next_back();
            let after = reason[at + cap.len()..].chars().next();
            !before.is_some_and(word) && !after.is_some_and(word)
        })
    }

    #[test]
    fn the_caps_of_linux_fuse_are_derived_from_its_declaration() {
        let fuse = frontend_caps("fuse").unwrap();
        let caps = caps_of(&fuse);
        for cap in [
            Cap::FuseAbort,
            Cap::ClusterLocks,
            Cap::Xattrs,
            Cap::HardLinks,
            Cap::Fallocate,
            Cap::SeekHole,
            Cap::SpecialFiles,
            Cap::PushInval,
            Cap::PushInvalFull,
            Cap::KeepOpenUnlinked,
            Cap::PerCloseFlush,
            Cap::VirtualXattrsListed,
        ] {
            assert!(caps.contains(&cap), "{cap}");
        }
        assert!(!caps.contains(&Cap::CaseInsensitive));
        // Derived, not a second table: change the declaration, the caps
        // follow.
        let mut other = fuse.clone();
        other.abortable = false;
        other.cluster_locks = false;
        other.xattrs = XattrSupport::None;
        other.push_inval = PushInval::Attr;
        let caps = caps_of(&other);
        assert!(!caps.contains(&Cap::FuseAbort));
        assert!(!caps.contains(&Cap::ClusterLocks));
        assert!(!caps.contains(&Cap::Xattrs));
        assert!(caps.contains(&Cap::PushInval) && !caps.contains(&Cap::PushInvalFull));
        assert!(frontend_caps("nfs").is_err());
    }

    #[test]
    fn a_missing_capability_is_skipped_by_name_as_a_whole_word() {
        let have = [Cap::HardLinks, Cap::Xattrs];
        assert_eq!(skip_reason(&[], &have), None);
        assert_eq!(skip_reason(&[Cap::Xattrs], &have), None);
        let reason = skip_reason(&[Cap::ClusterLocks], &have).unwrap();
        assert_eq!(reason, "requires capability ClusterLocks");
        assert!(is_cap_skip(&reason, "ClusterLocks"));
        assert!(!is_cap_skip(&reason, "Cluster"), "whole word only");
        assert!(!is_cap_skip(&reason, "Xattrs"));
        let reason = skip_reason(&[Cap::FuseAbort, Cap::Xattrs, Cap::SeekHole], &have).unwrap();
        assert!(is_cap_skip(&reason, "FuseAbort") && is_cap_skip(&reason, "SeekHole"));
        assert!(
            !is_cap_skip(&reason, "Xattrs"),
            "a capability the frontend has is not named"
        );
        assert!(
            !is_cap_skip("fio not installed", "fio"),
            "a missing tool is not a capability skip"
        );
    }

    #[test]
    fn the_reference_lane_never_skips_a_scenario_for_a_capability() {
        let have = caps_of(&frontend_caps("fuse").unwrap());
        for s in SCENARIOS.iter().chain(KNOWN_BUG_REPROS) {
            assert_eq!(skip_reason(s.caps, &have), None, "{}", s.name);
            let mut sorted = s.caps.to_vec();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), s.caps.len(), "{} lists a cap twice", s.name);
        }
    }

    #[test]
    fn scenarios_that_obviously_need_a_capability_declare_it() {
        let needs = |name: &str| -> &'static [Cap] {
            SCENARIOS.iter().find(|s| s.name == name).unwrap().caps
        };
        assert!(needs("fuse-inval-storm").contains(&Cap::FuseAbort));
        assert!(needs("git-under-flock-faults").contains(&Cap::FuseAbort));
        assert!(needs("flock-cross-node").contains(&Cap::ClusterLocks));
        assert!(needs("sqlite-two-nodes").contains(&Cap::ClusterLocks));
        assert!(needs("xattr-roundtrip").contains(&Cap::Xattrs));
        assert!(needs("fallocate-sparse").contains(&Cap::Fallocate));
        assert!(needs("fallocate-sparse").contains(&Cap::SeekHole));
        assert!(needs("subtree-confinement").contains(&Cap::HardLinks));
        // A scenario with no special needs stays unconditional.
        assert!(needs("baseline").is_empty());
        // ... and every declared cap exists in the derived list.
        for s in SCENARIOS {
            for c in s.caps {
                assert!(Cap::ALL.contains(c));
            }
        }
    }
}
