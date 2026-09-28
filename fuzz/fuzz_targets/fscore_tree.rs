//! Snapshot tree objects (`CTR1`/`CTR2`) fetched from the bucket. Runs
//! with -malloc_limit_mb so an untrusted count driving an allocation is
//! reported instead of aborting the fuzzer host.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(tree) = constellation_fs_core::Tree::decode(data) {
        let _ = tree.encode();
    }
});
