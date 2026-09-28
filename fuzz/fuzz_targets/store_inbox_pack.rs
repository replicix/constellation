//! Inbox batches and pack indexes: S3 objects written by other nodes.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(
    init: {
        // See store_object: keep the decompression ceiling under the malloc limit.
        std::env::set_var(
            constellation_store_s3::codec::MAX_DECOMPRESSED_ENV,
            (64u64 << 20).to_string(),
        );
    },
    |data: &[u8]| {
        let _ = constellation_store_s3::inbox::InboxBatch::decode(data);
        let _ = constellation_store_s3::packs::PackIndex::decode(data);
    }
);
