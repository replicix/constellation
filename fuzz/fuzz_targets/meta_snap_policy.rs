//! Plan 32's snapshot-schedule language (`user.constellation.snapshots`).
//!
//! Two invariants, both load-bearing for a feature that deletes
//! snapshots: `parse` is total (it never panics, whatever bytes a
//! `setfattr` put in the xattr), and the canonical form is a fixed point
//! — printing a policy and parsing it back yields the same policy, on
//! every node, forever.
#![no_main]
use libfuzzer_sys::fuzz_target;

use constellation_meta::snapsched::SnapPolicy;

fuzz_target!(|data: &[u8]| {
    let Ok(src) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(policy) = SnapPolicy::parse(src) {
        let canonical = policy.to_string();
        let reparsed = SnapPolicy::parse(&canonical)
            .unwrap_or_else(|e| panic!("canonical form {canonical:?} failed to re-parse: {e}"));
        assert_eq!(
            reparsed, policy,
            "canonical form {canonical:?} of {src:?} parsed to a different policy"
        );
        assert_eq!(
            reparsed.to_string(),
            canonical,
            "printing is not idempotent for {src:?}"
        );
    }
});
