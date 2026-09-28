//! Initiator side: an adversarial peer's `Reply` applied to a live session,
//! and an adversarial gossiped `Delta` applied to a mirror. Neither may
//! panic, and a rejected reply must leave the mirror usable.
#![no_main]
use constellation_net::reconcile::{Delta, KeySet, Mirror, Reply, Session};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let (which, rest) = data.split_at(1);
    if which[0] & 1 == 0 {
        let Ok(reply) = postcard::from_bytes::<Reply>(rest) else {
            return;
        };
        let mut mirror = KeySet::from_keys((0..512u64).map(|i| i.wrapping_mul(31)));
        let mut session = Session::new(if which[0] & 2 == 0 { 0 } else { 4 });
        let _ = session.next_request(&mirror);
        let _ = session.apply(&mut mirror, &reply);
        // The mirror must stay internally consistent after any reply.
        let keys: Vec<u64> = mirror.iter().collect();
        let rebuilt = KeySet::from_keys(keys);
        assert_eq!(
            mirror.root_fingerprint(),
            rebuilt.root_fingerprint(),
            "mirror accumulators diverged from its keys"
        );
    } else {
        let Ok(delta) = postcard::from_bytes::<Delta>(rest) else {
            return;
        };
        let mut m = Mirror::default();
        let _ = m.apply_delta(&delta);
        let _ = m.apply_delta(&delta);
    }
});
