//! Chunk object decode: header parse + zstd decompression of bucket bytes.
//! Run with -malloc_limit_mb / -rss_limit_mb so an unbounded decompression
//! is reported as a finding.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(
    init: {
        // Pin the decompression ceiling below libFuzzer's malloc limit, so an
        // allocation past it is a real finding rather than a legitimate
        // ceiling-sized decode (the default ceiling is 1 GiB).
        std::env::set_var(
            replicix_chunkfmt::codec::MAX_DECOMPRESSED_ENV,
            (64u64 << 20).to_string(),
        );
    },
    |data: &[u8]| {
        let _ = replicix_chunkfmt::format::decode_object(data);
    }
);
