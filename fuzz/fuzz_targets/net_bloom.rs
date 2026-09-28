//! Peer-supplied bloom digest parameters (`CacheDigest` fields), as the
//! coop cache admits them, then membership probes on whatever was admitted.
#![no_main]
use constellation_net::bloom::Bloom;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.len() < 20 {
        return;
    }
    let nbits = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let k = u32::from_le_bytes(data[8..12].try_into().unwrap());
    let n = u64::from_le_bytes(data[12..20].try_into().unwrap());
    let bits = data[20..].to_vec();
    if let Some(mut bloom) = Bloom::from_wire_bytes(nbits, k, n, bits) {
        let probe = *blake3::hash(data).as_bytes();
        let _ = bloom.contains(&probe);
        bloom.insert(&probe);
        assert!(bloom.contains(&probe), "inserted member must be found");
    }
});
