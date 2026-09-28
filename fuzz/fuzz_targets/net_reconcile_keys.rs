//! Varint-gap key list decoder used by gossiped `CacheSetDelta` and
//! reconciliation `Answer::Items`. Peer-controlled bytes.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }
    let base = u64::from_le_bytes(data[..8].try_into().unwrap());
    let buf = &data[8..];
    if let Some(keys) = constellation_net::reconcile::decode_keys(base, buf) {
        // Decoded keys must be strictly increasing from base and re-encode
        // to the same bytes.
        let enc = constellation_net::reconcile::encode_keys(base, &keys);
        assert_eq!(enc, buf, "decode/encode round trip diverged");
    }
});
