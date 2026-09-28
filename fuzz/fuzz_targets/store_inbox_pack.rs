//! Inbox batches and pack indexes: S3 objects written by other nodes.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_store_s3::inbox::InboxBatch::decode(data);
    let _ = constellation_store_s3::packs::PackIndex::decode(data);
});
