//! The responder path a peer talks to directly: arbitrary `ReconcileRequest`
//! queries answered against a live set. The reply must never panic and must
//! respect the frame budget.
#![no_main]
use constellation_net::message::{Payload, Signed};
use constellation_net::reconcile::{respond, KeySet, Query, Summary, REPLY_BUDGET};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

fn owner() -> &'static KeySet {
    static SET: OnceLock<KeySet> = OnceLock::new();
    SET.get_or_init(|| {
        KeySet::from_keys((0..50_000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
    })
}

fn key() -> &'static iroh::SecretKey {
    static KEY: OnceLock<iroh::SecretKey> = OnceLock::new();
    KEY.get_or_init(|| iroh::SecretKey::from_bytes(&[9u8; 32]))
}

fuzz_target!(|data: &[u8]| {
    let Ok(queries) = postcard::from_bytes::<Vec<Query>>(data) else {
        return;
    };
    let set = owner();
    let summary = Summary {
        incarnation: 1,
        seq: 1,
        root: set.root_fingerprint(),
        count: set.len() as u64,
    };
    let reply = respond(set, summary, &queries, REPLY_BUDGET);
    // Whatever the queries were, the signed reply must fit one stream frame.
    let msg = Signed::new(key(), &Payload::ReconcileReply { reply }).expect("sign");
    let _ = msg.encode();
});
