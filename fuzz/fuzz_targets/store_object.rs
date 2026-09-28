//! Chunk object decode: header parse + zstd decompression of bucket bytes.
//! Run with -malloc_limit_mb / -rss_limit_mb so an unbounded decompression
//! is reported as a finding.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_store_s3::format::decode_object(data);
});
