//! Raw-bytes attack on the P2P decoders: the signed envelope a gossip
//! datagram is fed into first, and the unsigned payload a direct-stream frame
//! body is. Decode must never panic; verify of
//! whatever decodes must never panic either.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_net::message::decode_payload(data);
    if let Ok(signed) = constellation_net::Signed::decode(data) {
        let _ = signed.verify();
    }
});
