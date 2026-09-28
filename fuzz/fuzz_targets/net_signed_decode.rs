//! Raw-bytes attack on the P2P signed envelope: what a peer's frame or a
//! gossip datagram is fed into first. Decode must never panic; verify of
//! whatever decodes must never panic either.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(signed) = constellation_net::Signed::decode(data) {
        let _ = signed.verify();
    }
});
