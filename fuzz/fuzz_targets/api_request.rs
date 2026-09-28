//! The control-plane request enum, exactly as the HTTP `Json` extractor
//! and the Unix control socket deserialize it.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(req) = serde_json::from_slice::<constellation_api::Request>(data) {
        // Responses serialize back out; neither direction may panic.
        let _ = serde_json::to_vec(&req);
    }
    let _ = serde_json::from_slice::<constellation_api::Response>(data);
});
