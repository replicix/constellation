//! Deep coverage of the ~70-variant `Payload` enum decode surface: this is
//! what `Signed::verify` runs on the signed body, i.e. what an enrolled
//! (allowlisted, signature-valid) but malicious peer fully controls. A
//! decodable payload is also re-encoded and signed to model the real
//! receive path end to end.
#![no_main]
use constellation_net::message::{Payload, Signed};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn key() -> &'static iroh::SecretKey {
    static KEY: OnceLock<iroh::SecretKey> = OnceLock::new();
    KEY.get_or_init(|| iroh::SecretKey::from_bytes(&[7u8; 32]))
}

fuzz_target!(|data: &[u8]| {
    let Ok(payload) = postcard::from_bytes::<Payload>(data) else {
        return;
    };
    // Encoding what decoded must not panic, and a signed round trip must
    // verify back to an equal payload.
    if let Ok(signed) = Signed::new(key(), &payload) {
        if let Ok(bare) = signed.encode_bare() {
            let back = Signed::decode(&bare).expect("self-encoded envelope decodes");
            let (_, got) = back.verify().expect("self-signed envelope verifies");
            assert_eq!(got, payload, "payload round trip changed the message");
        }
    }
});
