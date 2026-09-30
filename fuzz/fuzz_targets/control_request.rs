//! The control protocol's envelopes (plan 31 §9.3), exactly as a daemon
//! decodes them from a frame's payload: the always-JSON handshake, and a
//! request/response/cancel in either negotiated encoding.
#![no_main]
use constellation_control::proto::{Cancel, Encoding, Hello, Request, Response};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = Encoding::Json.from_bytes::<Hello>(data);
    for encoding in [Encoding::Json, Encoding::Postcard] {
        if let Ok(req) = encoding.from_bytes::<Request>(data) {
            // What decodes re-encodes; neither direction may panic.
            let _ = encoding.to_bytes(&req);
        }
        let _ = encoding.from_bytes::<Response>(data);
        let _ = encoding.from_bytes::<Cancel>(data);
    }
});
