//! The nested postcard blobs P2P payloads carry opaquely (`op`, `deps`,
//! `txs`): decoded on the receiving node with bytes a peer controls.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = constellation_meta::MutateOp::from_postcard(data);
    let _ = constellation_meta::MutateOutcome::from_postcard(data);
    let _ = constellation_meta::Position::from_postcard(data);
    let _ = constellation_meta::LogRecord::from_postcard(data);
    let _ = postcard::from_bytes::<Vec<constellation_meta::DelegateTx>>(data);
    let _ = postcard::from_bytes::<Vec<constellation_meta::BackupTx>>(data);
});
