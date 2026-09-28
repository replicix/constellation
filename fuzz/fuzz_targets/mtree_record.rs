//! Record-level decoders for metadata rows that arrive inside log
//! segments and tree leaves.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_mtree::record::Attrs::decode(data);
    let _ = constellation_mtree::record::Attrs::decode_prefix(data);
    let _ = constellation_mtree::record::Payload::decode(data);
    let _ = constellation_mtree::record::InodeRecord::decode(data);
    let _ = constellation_mtree::record::DentryRecord::decode(data);
    let _ = constellation_mtree::record::decode_fields(data);
});
