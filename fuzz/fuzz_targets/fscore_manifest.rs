//! Manifest and chunk-list decoders: bytes fetched from S3 by hash.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_fs_core::Manifest::decode(data);
    let _ = constellation_fs_core::manifest::decode_chunk_list(data);
});
