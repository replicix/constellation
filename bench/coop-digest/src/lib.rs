//! Plan 47 P1: chunk-location digest candidates measured against the
//! status quo (the exact mirror and RBSR in `constellation_net`).

pub mod acc;
pub mod cand;
pub mod mirror;
pub mod publisher;
pub mod repair;
pub mod sim;
pub mod util;

#[cfg(test)]
mod tests {
    use crate::cand::Spec;
    use crate::mirror::{rbsr_auto, rbsr_repair};
    use crate::publisher::{Churn, Publisher};
    use crate::util::{peer_keys, Rng};
    use constellation_net::reconcile::{
        start_bits_for, sync_in_memory, DeltaOutcome, KeySet, Mirror,
    };

    fn specs() -> Vec<Spec> {
        vec![
            Spec::Exact,
            Spec::Ef,
            Spec::Suffix(4, 64),
            Spec::Suffix(3, 64),
            Spec::Cuckoo(16, 256),
            Spec::Cuckoo(24, 64),
            Spec::Quotient(14, 256),
            Spec::Fuse(512),
            Spec::Hybrid(16, 256),
        ]
    }

    /// Lossless chain keeps every candidate exact; after dropped deltas
    /// RBSR (generic driver, real owner) restores it; no false negatives.
    #[test]
    fn every_candidate_follows_the_chain_and_repairs() {
        for spec in specs() {
            let keys = peer_keys(11, 0, 60_000);
            let mut publ = Publisher::new(KeySet::from_keys(keys.iter().copied()), 1);
            let mut churn = Churn::new(5, &publ.keys);
            let mut m = spec.build(&keys, keys.len());
            let mut lost = Vec::new();
            for t in 0..40 {
                let ev = churn.tick(300);
                let out = publ.absorb(&ev, false, true);
                for d in out.deltas {
                    if t % 7 == 3 {
                        lost.push(d);
                    } else if lost.is_empty() {
                        assert_eq!(m.apply_delta(&d), DeltaOutcome::Exact, "{spec:?} t={t}");
                    } else {
                        m.apply_delta(&d);
                    }
                }
                if t == 20 {
                    // Repair mid-run.
                    let s = publ.summary();
                    let st = rbsr_auto(m.as_mut(), &publ.keys, s, &[]);
                    assert!(!st.failed, "{spec:?}");
                    assert!(m.matches(&s), "{spec:?} mid repair");
                    lost.clear();
                }
            }
            let s = publ.summary();
            let st = rbsr_auto(m.as_mut(), &publ.keys, s, &[]);
            assert!(!st.failed, "{spec:?}");
            assert!(
                m.matches(&s),
                "{spec:?} final: len {} vs {}",
                m.len(),
                s.count
            );
            for &k in churn.cur.iter().step_by(7) {
                assert!(m.contains(k), "{spec:?} false negative");
            }
            let mut r = Rng::new(2);
            let fp = (0..200_000).filter(|_| m.contains(r.next())).count();
            eprintln!(
                "{spec:?}: fp {fp}/200000 repair {}B {} rounds",
                st.bytes(),
                st.rounds
            );
        }
    }

    /// The generic driver on the real mirror behaves as the real
    /// `Session` (same rounds, same final set).
    #[test]
    fn generic_rbsr_matches_the_real_session() {
        let base = peer_keys(3, 0, 100_000);
        let mut owner = KeySet::from_keys(base.iter().copied());
        let mut r = Rng::new(4);
        for i in 0..500 {
            if i % 2 == 0 {
                owner.insert(r.next());
            } else {
                owner.remove(base[r.below(base.len() as u64) as usize]);
            }
        }
        let mut real = Mirror {
            keys: KeySet::from_keys(base.iter().copied()),
            ..Mirror::default()
        };
        let (rounds, _) = sync_in_memory(&mut real.keys, &owner, start_bits_for(base.len()));
        let mut gm = Spec::Exact.build(&base, base.len());
        let s = constellation_net::reconcile::Summary {
            incarnation: 1,
            seq: 1,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let st = rbsr_repair(gm.as_mut(), &owner, s, start_bits_for(base.len()), &[]);
        assert_eq!(st.rounds, rounds);
        assert_eq!(gm.root_fp(), real.keys.root_fingerprint());
    }

    /// Without a leaf depth the bench responder is the real one.
    #[test]
    fn respond_leaf_equals_respond_without_leaf_depth() {
        use constellation_net::reconcile::{respond, Query, Range, Summary, REPLY_BUDGET};
        let base = peer_keys(8, 0, 30_000);
        let owner = KeySet::from_keys(base.iter().copied());
        let mirror = KeySet::from_keys(base.iter().step_by(3).copied());
        let s = Summary {
            incarnation: 1,
            seq: 1,
            root: owner.root_fingerprint(),
            count: owner.len() as u64,
        };
        let qs: Vec<Query> = Range::level(8)
            .into_iter()
            .map(|r| {
                let a = mirror.acc(&r);
                Query {
                    range: r,
                    fp: a.fingerprint(),
                    count: a.count,
                }
            })
            .collect();
        let a = respond(&owner, s, &qs, REPLY_BUDGET);
        let b = crate::mirror::respond_leaf(&owner, s, &qs, REPLY_BUDGET, None);
        assert_eq!(a, b);
    }
}
