//! FX-5: the epoch hold's transfer numbers, through the real paths of
//! several cores (one `Harness` per node, their sends carried by hand).
//!
//! A child of `tests` (it uses its `Harness` and helpers).

use super::*;
use crate::core::{HoldRecord, Reclaim};
use crate::event::{Carrier, HoldOffer, OfferKind, XferNo};
use crate::replica::hold_faults;

/// Epoch members 1..=N, node 1 the carrier and the first hold owner.
struct Cluster {
    hs: Vec<Harness>,
    key: Carrier,
    members: Vec<NodeId>,
    ops: u64,
}

/// The faults are per thread: clear them whatever a test does.
struct Faults;
impl Faults {
    fn read(on: bool) {
        hold_faults::READ.with(|c| c.set(on));
    }
    fn write(on: bool) {
        hold_faults::WRITE.with(|c| c.set(on));
    }
}
impl Drop for Faults {
    fn drop(&mut self) {
        Faults::read(false);
        Faults::write(false);
    }
}

impl Cluster {
    /// The epoch of `members` (ids 1..=N) reported active at every node:
    /// node 1 holds the carried lease and so the hold; the others know it
    /// as the holder.
    fn new(members: &[NodeId]) -> Self {
        let mut hs: Vec<Harness> = members.iter().map(|n| Harness::new(*n)).collect();
        hs[0].hold(1, None);
        let lease = hs[0].core.lease.held.as_ref().unwrap().0.clone();
        let key = carrier_of(&lease);
        let mut c = Cluster {
            hs,
            key,
            members: members.to_vec(),
            ops: 100,
        };
        for id in members {
            c.report(*id, true, true, false);
        }
        for h in c.hs.iter_mut().skip(1) {
            h.core.lease.cached_holder = Some(1);
        }
        c
    }

    fn node(&mut self, id: NodeId) -> &mut Harness {
        &mut self.hs[id as usize - 1]
    }

    fn report_event(
        &self,
        carrier: Option<Carrier>,
        open: bool,
        active: bool,
        frozen: bool,
    ) -> Event {
        Event::Control {
            op: OpId(1 << 40),
            req: Control::Epoch {
                open,
                active,
                frozen,
                flushing: false,
                base: 0,
                members: self.members.clone(),
                carrier,
                stale_below: 1,
            },
        }
    }

    fn report(&mut self, id: NodeId, open: bool, active: bool, frozen: bool) -> Vec<Action> {
        let ev = self.report_event(Some(self.key), open, active, frozen);
        self.node(id).step(ev)
    }

    fn rec(&self, id: NodeId) -> Option<HoldRecord> {
        self.hs[id as usize - 1].meta.epoch_hold_record().unwrap()
    }

    /// The nodes whose core holds the epoch hold.
    fn holders(&self) -> Vec<NodeId> {
        self.members
            .iter()
            .copied()
            .filter(|n| self.hs[*n as usize - 1].core.lease.epoch_held())
            .collect()
    }

    fn op(&mut self) -> OpId {
        self.ops += 1;
        OpId(self.ops)
    }

    /// `taker` starts an acquisition: its `LeaseRequest` (target, request id).
    fn ask(&mut self, taker: NodeId) -> (NodeId, OpId) {
        let op = self.op();
        let out = self.node(taker).step(Event::Control {
            op,
            req: Control::Acquire,
        });
        lease_request(&out).unwrap_or_else(|| panic!("node {taker} sent no epoch request: {out:?}"))
    }

    /// `giver` serves `taker`'s request `req` (its `applied` at the log's head).
    fn serve(&mut self, giver: NodeId, taker: NodeId, req: OpId) -> Vec<Action> {
        let applied = Replica::applied_seq(&self.node(taker).meta).unwrap_or(0);
        self.node(giver).step(Event::Peer {
            from: taker,
            msg: PeerMsg::LeaseRequest {
                req,
                epoch_applied: Some(applied),
            },
        })
    }

    /// `taker` hears `answer` from `giver` to `req`.
    fn hear(
        &mut self,
        taker: NodeId,
        giver: NodeId,
        req: OpId,
        answer: HandoffAnswer,
    ) -> Vec<Action> {
        self.node(taker).step(Event::Peer {
            from: giver,
            msg: PeerMsg::LeaseHandoff { req, answer },
        })
    }

    /// One exchange: `taker` asks, `giver` answers. `lost`: the answer never
    /// arrives (the requester's request timed out first). The answer given.
    fn exchange(&mut self, taker: NodeId, giver: NodeId, lost: bool) -> HandoffAnswer {
        let (to, req) = self.ask(taker);
        assert_eq!(to, giver, "the request went to node {to}");
        let out = self.serve(giver, taker, req);
        let answer = answer_to(&out, taker).unwrap_or_else(|| panic!("no answer: {out:?}"));
        if lost {
            self.node(taker).step(Event::PeerFailed {
                req,
                to: giver,
                outage: false,
            });
        } else {
            self.hear(taker, giver, req, answer);
        }
        answer
    }

    fn offer(&self, n: u32, kind: OfferKind) -> HoldOffer {
        HoldOffer {
            key: self.key,
            n: XferNo(n),
            kind,
        }
    }

    fn held(&self, n: u32, at: Epoch) -> HoldRecord {
        HoldRecord::Held {
            key: self.key,
            n: XferNo(n),
            at,
        }
    }

    fn given(&self, n: u32, to: NodeId) -> HoldRecord {
        HoldRecord::Given {
            key: self.key,
            n: XferNo(n),
            to,
        }
    }

    fn ended(&self, n: u32, reclaim: Reclaim) -> HoldRecord {
        HoldRecord::Ended {
            key: self.key,
            n: XferNo(n),
            reclaim,
        }
    }

    /// Restart `id` from its replica (the same store), the epoch not yet
    /// reported.
    fn restart(&mut self, id: NodeId) {
        let h = self.node(id);
        h.core = Core::new(h.core.cfg.clone());
        let (now, mut out) = (h.clk(), Vec::new());
        h.core.start(now, &h.meta, &mut out);
        h.core.lease.cached_holder = Some(1);
    }
}

fn lease_request(out: &[Action]) -> Option<(NodeId, OpId)> {
    sends(out).into_iter().find_map(|(to, m)| match m {
        PeerMsg::LeaseRequest {
            req,
            epoch_applied: Some(_),
        } => Some((to, *req)),
        _ => None,
    })
}

fn answer_to(out: &[Action], to: NodeId) -> Option<HandoffAnswer> {
    sends(out).into_iter().find_map(|(n, m)| match m {
        PeerMsg::LeaseHandoff { answer, .. } if n == to => Some(*answer),
        _ => None,
    })
}

fn is_hold(answer: &HandoffAnswer, n: u32, kind: OfferKind) -> bool {
    matches!(answer, HandoffAnswer::Hold(o) if o.n == XferNo(n) && o.kind == kind)
}

fn declined(answer: &HandoffAnswer) -> bool {
    matches!(answer, HandoffAnswer::Declined { .. })
}

fn poll(h: &mut Harness) -> Vec<Action> {
    let id = h.core.poll_timer.expect("no poll armed");
    h.step(Event::Timer { id })
}

/// T1: the give persists its record before the answer leaves, and only
/// goes to a member of the epoch; a failed write declines and keeps the hold.
#[test]
fn a_give_persists_its_record_before_the_answer() {
    let mut c = Cluster::new(&[1, 2]);
    assert_eq!(
        c.rec(1),
        Some(c.held(0, 1)),
        "the carrier's adoption is a record"
    );
    let (to, req) = c.ask(2);
    assert_eq!(to, 1);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    assert!(is_hold(&answer, 1, OfferKind::Transfer), "{answer:?}");
    assert_eq!(c.rec(1), Some(c.given(1, 2)), "persisted with the let-go");
    assert!(!c.node(1).core.lease.epoch_held());
    assert_eq!(c.node(1).core.stats.handoffs_served, 1);

    // A failed write: declined, the hold kept, the record unchanged.
    let _faults = Faults;
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    Faults::write(true);
    let out = c.serve(1, 2, req);
    Faults::write(false);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert!(c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
    assert_eq!(c.node(1).core.stats.epoch_hold_give_persist_failed, 1);

    // An asker that is no member: declined (G4).
    let mut c = Cluster::new(&[1, 2]);
    let out = c.node(1).step(Event::Peer {
        from: 3,
        msg: PeerMsg::LeaseRequest {
            req: OpId(9),
            epoch_applied: Some(0),
        },
    });
    assert!(declined(&answer_to(&out, 3).unwrap()), "{out:?}");
    assert!(c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
    assert_eq!(c.node(1).core.stats.epoch_gives_declined_not_member, 1);
}

/// T2 (M1 shape 1): the answer of a hand-back is lost; the former giver
/// asks again and is re-offered the same number, which it takes.
#[test]
fn a_lost_hand_back_is_taken_by_the_former_giver() {
    let mut c = Cluster::new(&[1, 2]);
    assert!(is_hold(&c.exchange(2, 1, false), 1, OfferKind::Transfer));
    assert_eq!(c.holders(), vec![2]);
    assert_eq!(
        c.rec(2),
        Some(c.held(1, 1 + 1)),
        "a transferee journals at the next epoch"
    );
    // 1 asks 2 for the hold back: 2 gives n = 2, the answer is lost.
    c.node(1).core.lease.cached_holder = Some(2);
    let answer = c.exchange(1, 2, true);
    assert!(is_hold(&answer, 2, OfferKind::Transfer), "{answer:?}");
    assert_eq!(c.holders(), Vec::<NodeId>::new(), "nobody holds");
    assert_eq!(c.rec(2), Some(c.given(2, 1)));
    assert_eq!(c.rec(1), Some(c.given(1, 2)));
    assert_eq!(c.node(1).core.stats.epoch_hold_answers_dropped, 0);
    // 1 asks again: 2's record re-offers 2, and 1 takes it.
    let answer = c.exchange(1, 2, false);
    assert!(is_hold(&answer, 2, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![1]);
    assert_eq!(c.rec(1), Some(c.held(2, 1)));
    assert_eq!(c.node(2).core.stats.epoch_hold_reoffers, 1);
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers_taken, 1);
    assert_eq!(c.node(1).core.stats.epoch_hold_transfers_taken, 0);
}

/// T4 (shape 5): three members. A stale re-offer from a former holder is
/// refused (the asker has passed it), and a give lost on the way is
/// re-offered and taken by its recipient only. Exactly one holder at every step.
#[test]
fn three_members_a_stale_reoffer_is_refused() {
    let mut c = Cluster::new(&[1, 2, 3]);
    c.exchange(2, 1, false);
    c.node(3).core.lease.cached_holder = Some(2);
    c.exchange(3, 2, false);
    assert_eq!(c.holders(), vec![3]);
    // 2 asks 1 (rotation is not the subject): 1's record Given{1, 2} re-offers 1.
    c.node(2).core.lease.cached_holder = Some(1);
    let (to, req) = c.ask(2);
    assert_eq!(to, 1);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{answer:?}");
    c.hear(2, 1, req, answer);
    assert_eq!(c.holders(), vec![3], "the stale offer was taken");
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_stale, 1);
    assert_eq!(c.rec(2), Some(c.given(2, 3)), "a refusal changes no record");
    // 3 gives n = 3 to 2, lost; re-offered and taken by 2 only.
    c.node(2).core.lease.cached_holder = Some(3);
    let answer = c.exchange(2, 3, true);
    assert!(is_hold(&answer, 3, OfferKind::Transfer));
    assert_eq!(c.holders(), Vec::<NodeId>::new());
    // (The request that got no answer moved the rotation on; the giver's
    // nudge names the giver again.)
    let answer = c.recover(3, 2);
    assert!(is_hold(&answer, 3, OfferKind::Reoffer));
    assert_eq!(c.holders(), vec![2]);
    assert_eq!(c.rec(2), Some(c.held(3, 2)));
    assert_eq!(
        c.rec(1),
        Some(c.given(1, 2)),
        "the stale giver's record is left alone"
    );
}

/// T5 (shape 4): crossing re-offers: records G: Given{3, R}, R: Given{2, G}.
/// R takes 3, G refuses 2: exactly one holder.
#[test]
fn crossing_reoffers_are_taken_once() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false); // 1 -> 2 (1)
    c.node(1).core.lease.cached_holder = Some(2);
    c.exchange(1, 2, false); // 2 -> 1 (2)
    c.node(2).core.lease.cached_holder = Some(1);
    c.exchange(2, 1, true); // 1 -> 2 (3), lost
    assert_eq!(c.rec(1), Some(c.given(3, 2)));
    assert_eq!(c.rec(2), Some(c.given(2, 1)));
    assert!(c.holders().is_empty());
    // Both ask in the same step: the requests cross.
    c.node(1).core.lease.cached_holder = Some(2);
    let (_, req1) = c.ask(1);
    let (_, req2) = c.ask(2);
    let out_from_2 = c.serve(2, 1, req1);
    let out_from_1 = c.serve(1, 2, req2);
    let a_to_1 = answer_to(&out_from_2, 1).unwrap();
    let a_to_2 = answer_to(&out_from_1, 2).unwrap();
    assert!(is_hold(&a_to_1, 2, OfferKind::Reoffer), "{a_to_1:?}");
    assert!(is_hold(&a_to_2, 3, OfferKind::Reoffer), "{a_to_2:?}");
    c.hear(1, 2, req1, a_to_1);
    c.hear(2, 1, req2, a_to_2);
    assert_eq!(c.holders(), vec![2], "exactly one holder");
    assert_eq!(c.rec(2), Some(c.held(3, 2)));
    assert_eq!(c.rec(1), Some(c.given(3, 2)));
    assert_eq!(c.node(1).core.stats.epoch_hold_offers_refused_stale, 1);
}

/// A taker with an acquisition waiting at `req` (the answer is made by hand).
fn waiting(c: &mut Cluster, taker: NodeId, giver: NodeId) -> OpId {
    c.node(taker).core.lease.cached_holder = Some(giver);
    let (to, req) = c.ask(taker);
    assert_eq!(to, giver);
    req
}

/// T7: a hold answer, of either kind, is refused outside an active epoch of
/// the taker, from a non-member, for another key.
#[test]
fn a_hold_answer_is_refused_outside_an_active_epoch() {
    for kind in [OfferKind::Transfer, OfferKind::Reoffer] {
        // (a) the epoch closed, (b) froze, (c) was replaced.
        for (label, open, active, frozen) in [
            ("closed", false, false, false),
            ("frozen", true, false, true),
            ("replaced", true, false, false),
        ] {
            let mut c = Cluster::new(&[1, 2]);
            let req = waiting(&mut c, 2, 1);
            c.report(2, open, active, frozen);
            assert!(
                c.node(2).core.job().is_none(),
                "{label}: the report ends the acquisition waiting for the hold"
            );
            c.hear(2, 1, req, HandoffAnswer::Hold(c.offer(1, kind)));
            assert!(
                c.node(2).core.lease.epoch_hold().is_none(),
                "{label} {kind:?}"
            );
            assert_eq!(c.rec(2), None, "{label} {kind:?}");
            let s = c.node(2).core.stats;
            assert_eq!(
                s.epoch_hold_offers_refused_closed + s.epoch_hold_answers_dropped,
                1,
                "{label} {kind:?}: refused by cause"
            );
        }
        // (f) the take checks the epoch itself, whether or not a report
        // ended the acquisition first.
        let mut c = Cluster::new(&[1, 2]);
        let req = waiting(&mut c, 2, 1);
        c.node(2).core.epoch.frozen = true;
        c.hear(2, 1, req, HandoffAnswer::Hold(c.offer(1, kind)));
        assert!(!c.node(2).core.lease.epoch_held(), "(f) {kind:?}");
        assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_closed, 1);
        // (d) from a node that is no member of the epoch.
        let mut c = Cluster::new(&[1, 2]);
        let req = waiting(&mut c, 2, 1);
        c.hear(2, 3, req, HandoffAnswer::Hold(c.offer(1, kind)));
        assert!(!c.node(2).core.lease.epoch_held(), "{kind:?}");
        assert_eq!(c.rec(2), None);
        assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_not_member, 1);
        // (e) for another carried key.
        let mut c = Cluster::new(&[1, 2]);
        let req = waiting(&mut c, 2, 1);
        let other = HoldOffer {
            key: Carrier {
                expires_unix_ms: c.key.expires_unix_ms + 1,
                ..c.key
            },
            n: XferNo(1),
            kind,
        };
        c.hear(2, 1, req, HandoffAnswer::Hold(other));
        assert!(!c.node(2).core.lease.epoch_held(), "{kind:?}");
        assert_eq!(c.rec(2), None);
        assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_key, 1);
    }
}

/// T7 (the original transfer, red on main): the take checks the epoch, not
/// only the phase: a plain transfer answer that reaches a requester whose
/// epoch has closed is not taken.
#[test]
fn a_transfer_answer_after_the_epoch_closed_is_not_taken() {
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    assert!(is_hold(&answer, 1, OfferKind::Transfer));
    c.report(2, false, false, false);
    c.hear(2, 1, req, answer);
    assert!(!c.node(2).core.lease.epoch_held());
    assert_eq!(c.rec(2), None);
    assert_eq!(c.node(2).core.stats.epoch_hold_transfers_taken, 0);
}

/// T10: a taker that restarts restores its hold from the record, at the
/// epoch it journals under; the giver's re-offer of that number is refused.
#[test]
fn a_restarted_taker_restores_its_hold() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false);
    assert_eq!(c.rec(2), Some(c.held(1, 2)));
    c.restart(2);
    assert!(
        !c.node(2).core.lease.epoch_held(),
        "not before its epoch is reported"
    );
    c.report(2, true, true, false);
    assert!(
        c.node(2).core.lease.epoch_held(),
        "restored from the record"
    );
    assert_eq!(
        c.node(2).core.lease.epoch_hold(),
        Some(2),
        "at the epoch it journals under"
    );
    assert_eq!(c.rec(2), Some(c.held(1, 2)));
    assert_eq!(
        c.node(2).meta.holder_epoch(),
        2,
        "captures under the restored hold"
    );
    assert_eq!(c.holders(), vec![2]);
}

/// T11: duplicate and late answers: the second copy of an answer finds no
/// acquisition (counted as dropped); a stale offer for a fresh acquisition
/// is refused; one adoption in all.
#[test]
fn duplicate_and_late_answers_are_taken_once() {
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    c.hear(2, 1, req, answer);
    assert_eq!(c.holders(), vec![2]);
    c.hear(2, 1, req, answer);
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(1, OfferKind::Reoffer)),
    );
    assert_eq!(c.holders(), vec![2]);
    assert_eq!(c.node(2).core.stats.epoch_hold_answers_dropped, 2);
    assert_eq!(c.node(2).core.stats.epoch_hold_transfers_taken, 1);
    assert_eq!(c.rec(2), Some(c.held(1, 2)));
    // 2 hands it back (1 takes n = 2); a late copy of 1 for a fresh
    // acquisition of 2 is refused as stale.
    c.node(1).core.lease.cached_holder = Some(2);
    c.exchange(1, 2, false);
    assert_eq!(c.holders(), vec![1]);
    let req = waiting(&mut c, 2, 1);
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(1, OfferKind::Reoffer)),
    );
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_stale, 1);
    assert_eq!(c.holders(), vec![1]);
}

/// T12: a take whose record fails to write is declined; once the store
/// works the giver's re-offer is taken.
#[test]
fn a_take_whose_record_fails_is_declined() {
    let _faults = Faults;
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    Faults::write(true);
    c.hear(2, 1, req, answer);
    Faults::write(false);
    assert!(!c.node(2).core.lease.epoch_held());
    assert_eq!(c.rec(2), None);
    assert_eq!(c.node(2).core.stats.epoch_hold_take_persist_failed, 1);
    assert!(c.node(2).core.job().is_none(), "the acquisition ended");
    let answer = c.exchange(2, 1, false);
    assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![2]);
    assert_eq!(c.rec(2), Some(c.held(1, 2)));
}

/// T15: an unreadable record (a read error, and a garbage value) blocks
/// every transition; readable at a later event it resumes.
#[test]
fn an_unreadable_hold_record_blocks_every_transition() {
    let _faults = Faults;
    for garbage in [false, true] {
        let mut c = Cluster::new(&[1, 2]);
        c.exchange(2, 1, false);
        assert_eq!(c.holders(), vec![2]);
        // Node 2 restarts onto an unreadable record.
        if garbage {
            c.node(2).meta.kv_set("epoch_hold_rec", "garbage").unwrap();
        } else {
            Faults::read(true);
        }
        c.restart(2);
        c.report(2, true, true, false);
        assert!(
            !c.node(2).core.lease.epoch_held(),
            "restored an unreadable record"
        );
        assert!(c.node(2).core.stats.epoch_hold_record_unreadable >= 1);
        // It gives nothing, re-offers nothing, takes nothing.
        let out = c.node(2).step(Event::Peer {
            from: 1,
            msg: PeerMsg::LeaseRequest {
                req: OpId(77),
                epoch_applied: Some(0),
            },
        });
        assert!(declined(&answer_to(&out, 1).unwrap()), "{out:?}");
        c.hear(
            2,
            1,
            OpId(78),
            HandoffAnswer::Hold(c.offer(2, OfferKind::Transfer)),
        );
        assert!(!c.node(2).core.lease.epoch_held());
        // (T5: an offer to an acquisition that waits for it is declined too.)
        let (_, req) = c.ask(2);
        c.hear(
            2,
            1,
            req,
            HandoffAnswer::Hold(c.offer(2, OfferKind::Transfer)),
        );
        assert!(
            !c.node(2).core.lease.epoch_held(),
            "took on an unreadable record"
        );
        assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_record, 1);
        // Readable again at a later event: it resumes (restores).
        if garbage {
            let rec = c.held(1, 2);
            c.node(2)
                .meta
                .persist_epoch_hold_record(Some(&rec))
                .unwrap();
        } else {
            Faults::read(false);
        }
        poll(c.node(2));
        c.report(2, true, true, false);
        assert!(
            c.node(2).core.lease.epoch_held(),
            "garbage {garbage}: not resumed"
        );
        assert_eq!(c.rec(2), Some(c.held(1, 2)));
    }
}

/// T18: a re-offer is declined by cause, each counted once.
#[test]
fn a_reoffer_is_declined_by_cause() {
    // The giver 1 gave n = 1 to 2 (lost): its record re-offers to 2 only.
    let setup = || {
        let mut c = Cluster::new(&[1, 2, 3]);
        c.exchange(2, 1, true);
        c
    };
    // journal unreadable (R8)
    let mut c = setup();
    c.node(1).meta.fail_journal_emptiness_reads_for_tests(1);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert_eq!(
        c.node(1)
            .core
            .stats
            .epoch_hold_reoffers_declined_journal_err,
        1
    );
    // applied behind the giver's head
    let mut c = setup();
    c.node(1).core.ship.head_seq = 5;
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers_declined_behind, 1);
    // a requester that is not the recorded recipient (R4): declined, no count of a reoffer
    let mut c = setup();
    let out = c.node(1).step(Event::Peer {
        from: 3,
        msg: PeerMsg::LeaseRequest {
            req: OpId(9),
            epoch_applied: Some(0),
        },
    });
    assert!(declined(&answer_to(&out, 3).unwrap()), "{out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 0);
    // not an active epoch (R2)
    let mut c = setup();
    c.report(1, true, false, true);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers_declined_state, 1);
    // a recipient that is no longer a member (R6, a named assertion)
    let mut c = setup();
    let ev = Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open: true,
            active: true,
            frozen: false,
            flushing: false,
            base: 0,
            members: vec![1, 3],
            carrier: Some(c.key),
            stale_below: 1,
        },
    };
    c.node(1).step(ev);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "R6: {out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 0);
    // a retired giver (R9, a named assertion)
    let mut c = setup();
    c.node(1).core.pr.retired = true;
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "R9: {out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 0);
    // the epoch carries another lease now (R5): declined
    let mut c = setup();
    let other = Carrier {
        node: 3,
        epoch: 1,
        expires_unix_ms: c.key.expires_unix_ms + 7,
    };
    let ev = c.report_event(Some(other), true, true, false);
    c.node(1).step(ev);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "R5: {out:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 0);
    assert_eq!(c.poll_nudge(1, 2), None, "N3: nudged for another lease");
    // and a clean re-offer for contrast
    let mut c = setup();
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(is_hold(&answer_to(&out, 2).unwrap(), 1, OfferKind::Reoffer));
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 1);
}

/// G6: a hold with an unshipped journal, or one whose emptiness cannot be
/// read, is not given; each cause is counted apart, and nothing changes.
#[test]
fn a_give_with_a_journal_is_declined_by_cause() {
    let mut c = Cluster::new(&[1, 2]);
    create_nowait(c.node(1), 1, "row");
    assert!(!Replica::journal_is_empty(&c.node(1).meta).unwrap());
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert_eq!(c.node(1).core.stats.epoch_gives_declined_journal, 1);
    assert!(c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
    let mut c = Cluster::new(&[1, 2]);
    c.node(1).meta.fail_journal_emptiness_reads_for_tests(1);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    assert_eq!(c.node(1).core.stats.epoch_gives_declined_journal_err, 1);
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
}

/// G2 and G7 as named assertions: a frozen giver and a retired giver give
/// nothing (and write nothing).
#[test]
fn a_frozen_or_retired_holder_gives_nothing() {
    let mut c = Cluster::new(&[1, 2]);
    c.report(1, true, false, true);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "G2: {out:?}");
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
    let mut c = Cluster::new(&[1, 2]);
    c.node(1).core.pr.retired = true;
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "G7: {out:?}");
    assert!(c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
}

/// A `Released` answer (the S3 handoff's) into an epoch-mode request is
/// refused: it cannot carry a hold.
#[test]
fn a_released_answer_into_an_epoch_request_is_refused() {
    let mut c = Cluster::new(&[1, 2]);
    let req = waiting(&mut c, 2, 1);
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Released {
            epoch: 1,
            head_seq: Some(0),
        },
    );
    assert!(!c.node(2).core.lease.epoch_held());
    assert_eq!(c.rec(2), None);
    assert_eq!(c.node(2).core.stats.epoch_hold_released_refused, 1);
}

/// T13, both orders: the carrier is retired while the hold is in flight, and
/// the members abandon the epoch.
/// (a) the abandon is reported, then the late offer arrives: refused.
/// (b) the offer is taken, then the abandon is reported: the hold ends
/// (its record, `Ended`) in the step of the report, before any promise.
#[test]
fn the_carriers_retire_while_the_hold_is_in_flight_ends_the_epoch() {
    // (a)
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    c.report(2, false, false, false);
    c.hear(2, 1, req, answer);
    assert!(
        !c.node(2).core.lease.epoch_held(),
        "(a) a late offer was taken"
    );
    assert_eq!(c.rec(2), None);
    // (b)
    let mut c = Cluster::new(&[1, 2]);
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    let answer = answer_to(&out, 2).unwrap();
    c.hear(2, 1, req, answer);
    assert_eq!(c.holders(), vec![2]);
    c.report(2, false, false, false);
    assert!(
        !c.node(2).core.lease.epoch_held(),
        "(b) the hold outlived the epoch"
    );
    assert_eq!(
        c.rec(2),
        Some(c.ended(1, Reclaim::Settled)),
        "(b) its end is recorded"
    );
}

/// T14 (the review's not-verified item): a member that restarts with its
/// join gate persisted promises nothing before it relearns its open epoch
/// (`Meta::promise_issue` refuses while the gate stands).
#[test]
fn a_restarted_member_promises_nothing_before_it_relearns_its_epoch() {
    let mut c = Cluster::new(&[1, 2]);
    c.node(2).core.cfg.epoch_slack = 1;
    let now = c.node(2).clk();
    assert!(c.node(2).meta.promise_join_begin(now).unwrap(), "the gate");
    c.restart(2);
    c.node(2).core.cfg.epoch_slack = 1;
    assert!(
        !c.node(2).core.epoch.open,
        "the restarted core knows no epoch yet"
    );
    let key = lease_of(3, 4, c.node(2).now.0 - 10).renewal_key();
    let out = c.node(2).step(Event::Peer {
        from: 3,
        msg: PeerMsg::PromiseRequest { req: OpId(5), key },
    });
    assert!(
        matches!(
            sends(&out).as_slice(),
            [(3, PeerMsg::PromiseReply { granted: false, .. })]
        ),
        "{out:?}"
    );
    assert!(
        !s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::HeartbeatPut { .. })),
        "a heartbeat was published: {out:?}"
    );
}

/// `epoch_probe_until_close`, stopped as soon as `stop` holds at a quiet
/// point (no job): S3 back, the polls run, a round probes. Whether the epoch
/// closed.
fn drive_until(
    h: &mut Harness,
    carrier: Carrier,
    object: Lease,
    stop: &dyn Fn(&Harness) -> bool,
) -> bool {
    let mut out = Vec::new();
    h.core.start(h.clk(), &h.meta, &mut out);
    let mut closed = false;
    for _ in 0..200 {
        let mut next = Vec::new();
        for action in std::mem::take(&mut out) {
            if stop(h) {
                return closed;
            }
            match action {
                Action::EpochClose => {
                    closed = true;
                    next.extend(h.step(epoch_report(carrier, false, false, false, true)));
                }
                Action::UploadDirtyChunks { op, .. } => next.extend(h.step(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                })),
                Action::S3 { op, req } => {
                    let result = match req {
                        S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                        S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                        S3Op::LeaseGet => S3Result::LeaseGet(Ok(Some((object.clone(), tag())))),
                        S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                        _ => continue,
                    };
                    next.extend(h.step(Event::S3 { op, result }));
                }
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => next.extend(h.step(Event::Timer { id })),
                _ => {}
            }
        }
        if stop(h) {
            break;
        }
        if next.is_empty() {
            break;
        }
        h.advance(20);
        out = next;
    }
    closed
}

/// T19: the close waits for its record. The `Ended{Owed}` write fails at the
/// probed round: no `EpochClose`, the hold kept (and the node silent); the
/// next probed round closes. The re-claim's settle writes `Ended{Settled}`.
#[test]
fn the_close_waits_for_its_record() {
    let _faults = Faults;
    let mut c = Cluster::new(&[1, 2]);
    let object = c.node(1).core.lease.held.as_ref().unwrap().0.clone();
    let carrier = c.key;
    Faults::write(true);
    let closed = drive_until(c.node(1), carrier, object.clone(), &|h| {
        h.core.stats.epoch_hold_end_persist_failed >= 1 && h.core.job().is_none()
    });
    assert!(!closed, "closed without its record");
    assert!(c.node(1).core.lease.epoch_held(), "the hold stays");
    assert!(!c.node(1).core.pr.flush_pending, "silent for nothing");
    assert_eq!(c.rec(1), Some(c.held(0, 1)));
    assert!(c.node(1).core.stats.epoch_hold_end_persist_failed >= 1);
    Faults::write(false);
    let (closed, reclaimed) = epoch_probe_until_close(c.node(1), carrier, object);
    assert!(closed, "the next probed round closes");
    assert!(reclaimed.is_some(), "and re-claims the carried lease");
    // The re-claim moved the lease: its settle wrote `Ended { Settled }`.
    assert_eq!(c.rec(1), Some(c.ended(0, Reclaim::Settled)));
}

/// An involuntary end whose `Ended` write fails: the record stays `Held`
/// while memory holds nothing, the node gives and takes nothing, and a
/// restart restores nothing. The persisted gate of each end: a deposition
/// (`lease_lost`), a release in a frozen epoch (`lease_released`), a vanish
/// (`epoch_owes_move`).
#[test]
fn an_involuntary_end_whose_record_fails_is_not_restored() {
    let _faults = Faults;
    // Deposition.
    let mut c = Cluster::new(&[1, 2]);
    let (now, mut out) = (c.node(1).clk(), Vec::new());
    let mine = c.node(1).core.lease.epoch().unwrap();
    let h1 = c.node(1);
    h1.core.deposed(now, 0, mine + 1, mine, &h1.meta, &mut out);
    Faults::write(true);
    poll(c.node(1));
    assert!(!c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)), "the end could not be written");
    assert!(c.node(1).core.stats.epoch_hold_end_persist_failed >= 1);
    // (while dirty it gives nothing)
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "{out:?}");
    Faults::write(false);
    assert!(
        Replica::lost_persisted(&c.hs[0].meta),
        "the gate: lease_lost"
    );
    c.restart(1);
    c.report(1, true, true, false);
    assert!(!c.node(1).core.lease.epoch_held(), "deposition: restored");
    poll(c.node(1));
    assert_eq!(
        c.rec(1),
        Some(c.ended(0, Reclaim::Settled)),
        "retried once the store works"
    );

    // A release in a frozen epoch.
    let mut c = Cluster::new(&[1, 2]);
    c.report(1, true, false, true);
    let release = handoff_release_in_flight(c.node(1));
    Faults::write(true);
    c.node(1).step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Ok(tag())),
    });
    assert!(!c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)), "the end could not be written");
    Faults::write(false);
    assert!(
        Replica::lease_released(&c.hs[0].meta).is_some(),
        "the gate: lease_released"
    );
    c.restart(1);
    c.report(1, true, true, false);
    assert!(
        !c.node(1).core.lease.epoch_held(),
        "frozen release: restored"
    );
    assert!(
        c.node(1).core.pr.owes_move.is_some(),
        "its move is owed instead"
    );

    // A vanish (a release given up in doubt wipes the hold).
    let mut c = Cluster::new(&[1, 2]);
    Faults::write(true);
    c.node(1).core.lease.release_local();
    let h1 = c.node(1);
    h1.core.owe_given_up_move(true, &h1.meta);
    poll(c.node(1));
    assert!(!c.node(1).core.lease.epoch_held());
    assert_eq!(c.rec(1), Some(c.held(0, 1)), "the end could not be written");
    Faults::write(false);
    assert!(
        Replica::epoch_owes_move(&c.hs[0].meta).is_some(),
        "the gate: epoch_owes_move"
    );
    c.restart(1);
    c.report(1, true, true, false);
    assert!(!c.node(1).core.lease.epoch_held(), "vanish: restored");
}

/// The node closes its epoch as the hold owner (the `Ended { Owed }` record
/// written, `EpochClose` sent), stopped before the re-claim lands.
fn close_the_epoch(c: &mut Cluster, id: NodeId) {
    let object = c.node(1).core.lease.held.as_ref().map(|(l, _)| l.clone());
    let object = object.unwrap_or_else(|| {
        // (The carried lease as S3 shows it.)
        let k = c.key;
        let mut l = lease_of(k.node, k.epoch, k.expires_unix_ms);
        l.holder = k.node;
        l
    });
    let carrier = c.key;
    let closed = drive_until(c.node(id), carrier, object, &|h| {
        matches!(h.core.hold_record(), Some(HoldRecord::Ended { .. })) && !h.core.epoch.open
    });
    assert!(closed, "node {id} did not close");
}

/// MUST 1 (seeds 166, 2236): after the node's own close, a report with the
/// epoch still open (no activation) does not hold it again.
#[test]
fn a_late_report_after_its_own_close_stays_not_held() {
    let mut c = Cluster::new(&[1, 2]);
    close_the_epoch(&mut c, 1);
    assert_eq!(c.rec(1), Some(c.ended(0, Reclaim::Owed)));
    assert!(!c.node(1).core.lease.epoch_held());
    c.report(1, true, false, false);
    c.report(1, true, false, true);
    assert!(
        !c.node(1).core.lease.epoch_held(),
        "held again beside the next holder"
    );
    assert_eq!(c.rec(1), Some(c.ended(0, Reclaim::Owed)));
    assert_eq!(c.node(1).core.stats.epoch_holds_adopted_late, 0);
}

/// MUST 1: the closed-carried activation re-holds at the same number, and
/// an offer at or below it is refused.
#[test]
fn a_closed_carried_rehold_keeps_its_number_and_refuses_a_stale_offer() {
    let mut c = Cluster::new(&[1, 2]);
    close_the_epoch(&mut c, 1);
    // (As if the hold had been taken back at number 3 before the close.)
    let rec = c.ended(3, Reclaim::Owed);
    c.node(1)
        .meta
        .persist_epoch_hold_record(Some(&rec))
        .unwrap();
    c.node(1).core.pr.hold_rec = HoldRecState::Known(Some(rec));
    // The epoch closes, then another activation carries the same lease.
    c.report(1, false, false, false);
    c.report(1, true, true, false);
    assert!(
        c.node(1).core.lease.epoch_held(),
        "the closed lease is held again"
    );
    assert_eq!(c.rec(1), Some(c.held(3, 1)), "the same number");
    assert_eq!(c.node(1).core.stats.epoch_closed_leases_reheld, 1);
    // It gives n = 4, then hears an offer of 3 (stale), refused.
    c.exchange(2, 1, false);
    assert_eq!(c.rec(1), Some(c.given(4, 2)));
    // (A restart: the close's round is still in flight in this fixture.)
    c.restart(1);
    c.report(1, true, true, false);
    c.node(1).core.lease.cached_holder = Some(2);
    let (_, req) = c.ask(1);
    c.hear(
        1,
        2,
        req,
        HandoffAnswer::Hold(c.offer(3, OfferKind::Reoffer)),
    );
    assert!(!c.node(1).core.lease.epoch_held());
    assert_eq!(c.node(1).core.stats.epoch_hold_offers_refused_stale, 1);
}

/// MUST 1: a crash between the `Ended { Owed }` write and the close's
/// effect: the restart does not re-hold, for the carrier and for a transferee.
#[test]
fn a_crash_after_the_owed_record_closes_without_re_holding() {
    // The carrier.
    let mut c = Cluster::new(&[1, 2]);
    close_the_epoch(&mut c, 1);
    c.restart(1);
    c.report(1, true, true, false);
    assert!(!c.node(1).core.lease.epoch_held(), "carrier re-held");
    assert_eq!(c.rec(1), Some(c.ended(0, Reclaim::Owed)));
    // A transferee.
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false);
    close_the_epoch(&mut c, 2);
    assert_eq!(c.rec(2), Some(c.ended(1, Reclaim::Owed)));
    c.restart(2);
    c.report(2, true, true, false);
    assert!(!c.node(2).core.lease.epoch_held(), "transferee re-held");
    assert_eq!(c.rec(2), Some(c.ended(1, Reclaim::Owed)));
}

/// R3, a named assertion: a node that holds re-offers nothing, whatever a
/// record might say.
#[test]
fn a_holder_re_offers_nothing() {
    let mut c = Cluster::new(&[1, 2]);
    let rec = c.given(5, 2);
    let (_, req) = c.ask(2);
    let h1 = c.node(1);
    h1.core.pr.hold_rec = HoldRecState::Known(Some(rec));
    // (Not through a step: the end of an event checks the invariant.)
    let mut out = Vec::new();
    h1.core
        .on_lease_request(h1.clk(), 2, req, Some(0), &h1.meta, &mut out);
    let answer = answer_to(&out, 2).unwrap();
    assert!(declined(&answer), "{answer:?}");
    assert_eq!(c.node(1).core.stats.epoch_hold_reoffers, 0);
}

/// MUST 2: the keys of the earlier format are never read: a store that
/// still holds them (`epoch_hold` = "0" meant no hold) is not unreadable.
#[test]
fn the_old_keys_are_ignored() {
    let mut h = Harness::new(1);
    h.meta.kv_set("epoch_hold", "0").unwrap();
    h.meta.kv_set("epoch_hold_ended", "1:7:1").unwrap();
    h.core = Core::new(h.core.cfg.clone());
    let (now, mut out) = (h.clk(), Vec::new());
    h.core.start(now, &h.meta, &mut out);
    assert!(matches!(h.core.pr.hold_rec, HoldRecState::Known(None)));
    h.hold(1, None);
    let lease = h.core.lease.held.as_ref().unwrap().0.clone();
    h.step(epoch_report(carrier_of(&lease), true, true, false, false));
    assert!(h.core.lease.epoch_held(), "adopted: not stalled");
    assert_eq!(h.core.stats.epoch_hold_record_unreadable, 0);
}

// ---- the nudge ----

fn nudge_to(out: &[Action], to: NodeId) -> Option<(Carrier, XferNo)> {
    sends(out).into_iter().find_map(|(n, m)| match m {
        PeerMsg::EpochHoldNudge { key, n: no } if n == to => Some((*key, *no)),
        _ => None,
    })
}

impl Cluster {
    /// `giver` polls (its ordinary round): the nudge it sent to `to`, if any.
    fn poll_nudge(&mut self, giver: NodeId, to: NodeId) -> Option<(Carrier, XferNo)> {
        let h = self.node(giver);
        let mut out = Vec::new();
        // (The poll's handler itself: a round in flight leaves no timer.)
        h.core.on_poll(h.clk(), &h.meta, &mut out);
        nudge_to(&out, to)
    }

    /// `to` hears `giver`'s nudge: what it sends.
    fn hear_nudge(&mut self, to: NodeId, giver: NodeId, key: Carrier, n: XferNo) -> Vec<Action> {
        self.node(to).step(Event::Peer {
            from: giver,
            msg: PeerMsg::EpochHoldNudge { key, n },
        })
    }

    /// The whole recovery of a lost answer: `giver` polls, `to` asks, the
    /// giver re-offers, `to` hears it.
    fn recover(&mut self, giver: NodeId, to: NodeId) -> HandoffAnswer {
        let (key, n) = self
            .poll_nudge(giver, to)
            .unwrap_or_else(|| panic!("node {giver} sent no nudge"));
        let out = self.hear_nudge(to, giver, key, n);
        let (target, req) = lease_request(&out).unwrap_or_else(|| panic!("no ask: {out:?}"));
        assert_eq!(target, giver, "the nudge's giver is asked");
        let out = self.serve(giver, to, req);
        let answer = answer_to(&out, to).unwrap();
        self.hear(to, giver, req, answer);
        answer
    }
}

/// T6 (M2, shape 3): the answer is lost and nobody asks again (the
/// recipient is idle): the giver's round nudges it, its duty acquisition
/// asks the giver and takes the hold, and the epoch closes from there.
#[test]
fn an_idle_recipient_is_nudged_and_the_epoch_closes() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, true);
    assert!(c.holders().is_empty());
    assert!(c.node(2).core.job().is_none(), "the recipient is idle");
    let answer = c.recover(1, 2);
    assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![2]);
    assert_eq!(c.rec(2), Some(c.held(1, 2)));
    assert_eq!(c.node(1).core.stats.epoch_hold_nudges, 1);
    // S3 is back: the new owner flushes and closes the epoch.
    let carrier = c.key;
    let mut object = lease_of(carrier.node, carrier.epoch, carrier.expires_unix_ms);
    object.holder = carrier.node;
    let closed = drive_until(c.node(2), carrier, object, &|h| {
        matches!(h.core.hold_record(), Some(HoldRecord::Ended { .. })) && !h.core.epoch.open
    });
    assert!(closed, "the epoch did not close");
    assert_eq!(c.rec(2), Some(c.ended(1, Reclaim::Owed)));
}

/// T3 (shape 2): a request that sat queued at the holder past its
/// requester's timeout is served late; the answer finds nobody; the
/// giver's round nudges, and the recovery is the same.
#[test]
fn a_stale_request_served_late_is_recovered_by_the_nudge() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false);
    // 1 asks 2 for the hold back; 2 is paused; 1's request times out.
    c.node(1).core.lease.cached_holder = Some(2);
    let (_, req) = c.ask(1);
    c.node(1).step(Event::PeerFailed {
        req,
        to: 2,
        outage: false,
    });
    assert!(c.node(1).core.job().is_none());
    // 2 serves it later: it gives n = 2; the answer reaches a node with no
    // acquisition waiting for it.
    let out = c.serve(2, 1, req);
    let answer = answer_to(&out, 1).unwrap();
    assert!(is_hold(&answer, 2, OfferKind::Transfer));
    c.hear(1, 2, req, answer);
    assert_eq!(c.node(1).core.stats.epoch_hold_answers_dropped, 1);
    assert!(c.holders().is_empty());
    // 2's round nudges 1; 1 asks 2 and takes.
    let answer = c.recover(2, 1);
    assert!(is_hold(&answer, 2, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![1]);
    assert_eq!(c.rec(1), Some(c.held(2, 1)));
}

/// T8: a frozen giver re-offers nothing and nudges nothing; after the
/// unfreeze the next round nudges and the re-offer is taken.
#[test]
fn a_frozen_giver_reoffers_after_the_unfreeze() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, true);
    c.report(1, true, false, true);
    assert_eq!(c.poll_nudge(1, 2), None, "N2: a frozen giver nudges nobody");
    let (_, req) = c.ask(2);
    let out = c.serve(1, 2, req);
    assert!(declined(&answer_to(&out, 2).unwrap()), "R2: {out:?}");
    c.node(2).step(Event::PeerFailed {
        req,
        to: 1,
        outage: false,
    });
    c.report(1, true, true, false);
    let answer = c.recover(1, 2);
    assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![2]);
}

/// T9: the giver crashes after its record and before its answer left: after
/// the restart its round nudges from the record alone.
#[test]
fn a_giver_crashed_after_its_record_nudges_after_the_restart() {
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, true);
    c.restart(1);
    assert_eq!(c.rec(1), Some(c.given(1, 2)), "the record survived");
    // Not before it knows its epoch: nothing to nudge in.
    assert_eq!(c.poll_nudge(1, 2), None);
    c.report(1, true, true, false);
    let answer = c.recover(1, 2);
    assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{answer:?}");
    assert_eq!(c.holders(), vec![2]);
}

/// T17: the nudge is filtered, and its acquisition is a duty.
#[test]
fn the_nudge_is_filtered_and_its_acquisition_is_a_duty() {
    // A stale nudge (the recipient has passed n) is ignored.
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false);
    let key = c.key;
    let out = c.hear_nudge(2, 1, key, XferNo(1));
    assert!(lease_request(&out).is_none(), "{out:?}");
    assert_eq!(c.node(2).core.stats.epoch_hold_nudges_ignored_stale, 1);
    // A nudge from a non-member, for another key, or to a holder: ignored.
    let out = c.hear_nudge(2, 3, key, XferNo(9));
    assert!(lease_request(&out).is_none());
    let other = Carrier {
        expires_unix_ms: key.expires_unix_ms + 1,
        ..key
    };
    let out = c.hear_nudge(2, 1, other, XferNo(9));
    assert!(lease_request(&out).is_none());
    assert_eq!(c.node(2).core.stats.epoch_hold_nudges_ignored_state, 2);
    let out = c.hear_nudge(2, 1, key, XferNo(9));
    assert!(lease_request(&out).is_none(), "A5: it holds");
    assert_eq!(c.node(2).core.stats.epoch_hold_nudges_ignored_state, 3);

    // A nudge during a running acquisition is ignored; honoured at the next round.
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, true);
    let (_, req) = c.ask(2);
    let (key, n) = c.poll_nudge(1, 2).unwrap();
    let out = c.hear_nudge(2, 1, key, n);
    assert!(lease_request(&out).is_none(), "{out:?}");
    assert_eq!(c.node(2).core.stats.epoch_hold_nudges_ignored_busy, 1);
    c.node(2).step(Event::PeerFailed {
        req,
        to: 1,
        outage: false,
    });
    let (key, n) = c.poll_nudge(1, 2).unwrap();
    let out = c.hear_nudge(2, 1, key, n);
    assert!(
        lease_request(&out).is_some(),
        "the next round's nudge: {out:?}"
    );

    // A forward-only node and a suspended node take a hold given to them.
    for mode in [
        AuthorityMode {
            forward_only: true,
            suspended: false,
        },
        AuthorityMode {
            forward_only: false,
            suspended: true,
        },
    ] {
        let mut c = Cluster::new(&[1, 2]);
        c.exchange(2, 1, true);
        c.node(2).core.mode = mode;
        let answer = c.recover(1, 2);
        assert!(is_hold(&answer, 1, OfferKind::Reoffer), "{mode:?}");
        assert_eq!(c.holders(), vec![2], "{mode:?}");
    }
}

/// N4 and N5, named assertions: a node that holds nudges nobody, and nobody
/// is nudged who is no member of the epoch.
#[test]
fn a_holder_and_a_non_member_are_not_nudged() {
    let mut c = Cluster::new(&[1, 2]);
    let rec = c.given(1, 2);
    let h1 = c.node(1);
    h1.core.pr.hold_rec = HoldRecState::Known(Some(rec));
    let mut out = Vec::new();
    h1.core.nudge_hold_recipient(&mut out);
    assert!(out.is_empty(), "N4: a holder nudged: {out:?}");
    let mut c = Cluster::new(&[1, 2, 3]);
    c.exchange(2, 1, true);
    let ev = Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open: true,
            active: true,
            frozen: false,
            flushing: false,
            base: 0,
            members: vec![1, 3],
            carrier: Some(c.key),
            stale_below: 1,
        },
    };
    c.node(1).step(ev);
    assert_eq!(c.poll_nudge(1, 2), None, "N5: a non-member was nudged");
    // And a retired giver nudges nobody (N6).
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, true);
    c.node(1).core.pr.retired = true;
    assert_eq!(c.poll_nudge(1, 2), None, "N6");
}

// ---- the ask rotation ----

/// T16: an epoch-mode acquisition moves on past a member that declined or
/// stayed silent, never asks a non-member while the members are known, and
/// asks a nudge's giver first.
#[test]
fn the_epoch_ask_rotates_past_a_declining_or_silent_cached_holder() {
    // A cached holder that declines: the next attempt asks the next member
    // (past it, in id order, wrapping).
    let mut c = Cluster::new(&[1, 2, 3, 4]);
    c.node(4).core.lease.cached_holder = Some(2);
    let (to, req) = c.ask(4);
    assert_eq!(to, 2);
    let out = c.serve(2, 4, req);
    let answer = answer_to(&out, 4).unwrap();
    assert!(declined(&answer), "a non-holder declines");
    c.hear(4, 2, req, answer);
    let (to, req) = c.ask(4);
    assert_eq!(to, 3, "past the cached holder that declined");
    // ... and a silent one likewise: the request times out.
    c.node(4).step(Event::PeerFailed {
        req,
        to: 3,
        outage: false,
    });
    let (to, req) = c.ask(4);
    assert_eq!(to, 1, "wrapping");
    c.node(4).step(Event::PeerFailed {
        req,
        to: 1,
        outage: false,
    });
    let (to, _) = c.ask(4);
    assert_eq!(
        to, 2,
        "the rotation visits every member, the skipped one too"
    );
    // The cached holder changes: it is asked again.
    let mut c = Cluster::new(&[1, 2, 3]);
    c.node(3).core.lease.cached_holder = Some(2);
    let (_, req) = c.ask(3);
    c.node(3).step(Event::PeerFailed {
        req,
        to: 2,
        outage: false,
    });
    c.node(3).core.lease.cached_holder = Some(1);
    let (to, _) = c.ask(3);
    assert_eq!(to, 1, "a cached holder that changed is asked first");

    // A connected link of a non-member with a lower id is never asked.
    let mut c = Cluster::new(&[1, 2, 3]);
    let ev = Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open: true,
            active: true,
            frozen: false,
            flushing: false,
            base: 0,
            members: vec![2, 3],
            carrier: Some(c.key),
            stale_below: 1,
        },
    };
    c.node(3).step(ev);
    c.node(3).core.lease.cached_holder = None;
    c.node(3).step(Event::Peers {
        registry_complete: true,
        links: vec![crate::event::PeerLink {
            node: 1,
            connected: true,
            last_seen: None,
            rtt_ms: None,
            since: None,
        }],
        lapses: 0,
    });
    let (to, _) = c.ask(3);
    assert_eq!(to, 2, "a non-member link absorbed the attempt");

    // A nudge's giver is asked first, whatever the cache says.
    let mut c = Cluster::new(&[1, 2, 3]);
    c.exchange(2, 1, true);
    c.node(2).core.lease.cached_holder = Some(3);
    let answer = c.recover(1, 2);
    assert!(is_hold(&answer, 1, OfferKind::Reoffer));
}

/// T1: a hold answer to an S3 handoff request declines (the request that
/// asked for no hold).
#[test]
fn a_hold_answer_to_an_s3_request_declines() {
    let mut h = Harness::new(2);
    h.core.lease.cached_holder = Some(1);
    let out = h.step(Event::Control {
        op: OpId(900),
        req: Control::ClaimOffer { epoch: 1 },
    });
    let req = match sends(&out)[..] {
        [(
            1,
            PeerMsg::LeaseRequest {
                req,
                epoch_applied: None,
            },
        )] => *req,
        ref other => panic!("expected one S3 LeaseRequest: {other:?}"),
    };
    let key = Carrier {
        node: 1,
        epoch: 1,
        expires_unix_ms: h.now.0 + 1_000,
    };
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LeaseHandoff {
            req,
            answer: HandoffAnswer::Hold(HoldOffer {
                key,
                n: XferNo(1),
                kind: OfferKind::Transfer,
            }),
        },
    });
    assert!(!h.core.lease.epoch_held());
    assert!(h.core.job().is_none(), "the acquisition ended");
    assert_eq!(h.core.stats.epoch_hold_offers_refused_mode, 1);
}

/// T6: a node that holds, is retired, is deposed, or has an unwritten end of
/// a hold of its own takes no offer.
#[test]
fn an_offer_is_refused_by_a_node_that_cannot_take_it() {
    // It holds.
    let mut c = Cluster::new(&[1, 2]);
    let req = waiting(&mut c, 2, 1);
    let h2 = c.node(2);
    h2.adopt_hold_at(h2.clk(), 2);
    let rec = c.rec(2);
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(5, OfferKind::Transfer)),
    );
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_held, 1);
    assert_eq!(c.rec(2), rec, "a refusal changes no record");
    // Retired.
    let mut c = Cluster::new(&[1, 2]);
    let req = waiting(&mut c, 2, 1);
    c.node(2).core.pr.retired = true;
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(1, OfferKind::Transfer)),
    );
    assert!(!c.node(2).core.lease.epoch_held(), "a retired node took it");
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_closed, 1);
    // Deposed.
    let mut c = Cluster::new(&[1, 2]);
    let req = waiting(&mut c, 2, 1);
    c.node(2).core.lease.force_lost();
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(1, OfferKind::Transfer)),
    );
    assert!(!c.node(2).core.lease.epoch_held(), "a deposed node took it");
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_closed, 1);
    // An end of its own hold not written yet (the record still says Held).
    let _faults = Faults;
    let mut c = Cluster::new(&[1, 2]);
    c.exchange(2, 1, false);
    Faults::write(true);
    c.node(2).core.lease.release_local();
    // (an event ends the hold, the write fails)
    c.node(2).step(Event::Peer {
        from: 1,
        msg: PeerMsg::SegmentPublished { seq: 0, epoch: 0 },
    });
    assert!(c.node(2).core.hold_rec_dirty());
    let req = waiting(&mut c, 2, 1);
    c.hear(
        2,
        1,
        req,
        HandoffAnswer::Hold(c.offer(7, OfferKind::Transfer)),
    );
    Faults::write(false);
    assert!(!c.node(2).core.lease.epoch_held());
    assert_eq!(c.node(2).core.stats.epoch_hold_offers_refused_record, 1);
}

/// A carrier whose adoption's record fails to write holds nothing and tries
/// again at its next round.
#[test]
fn a_carrier_whose_adoption_record_fails_adopts_at_the_next_round() {
    let _faults = Faults;
    let mut h = Harness::new(1);
    h.hold(1, None);
    let lease = h.core.lease.held.as_ref().unwrap().0.clone();
    Faults::write(true);
    h.step(epoch_report(carrier_of(&lease), true, true, false, false));
    assert!(!h.core.lease.epoch_held(), "adopted without its record");
    assert!(h.core.stats.epoch_hold_adopt_persist_failed >= 1);
    Faults::write(false);
    let mut out = Vec::new();
    h.core.on_poll(h.clk(), &h.meta, &mut out);
    assert!(h.core.lease.epoch_held(), "not retried at the round");
    assert_eq!(
        h.meta.epoch_hold_record().unwrap().map(|r| r.n()),
        Some(XferNo(0))
    );
}

/// A3 to A1 of the nudge, against a node that is idle in the epoch: a
/// nudge from a non-member, for another lease, or in a frozen epoch is
/// ignored (a node that held would refuse them all for another reason).
#[test]
fn an_idle_node_ignores_a_nudge_that_is_not_for_its_epoch() {
    let mut c = Cluster::new(&[1, 2, 3]);
    let key = c.key;
    let other = Carrier {
        expires_unix_ms: key.expires_unix_ms + 1,
        ..key
    };
    // from a node outside the epoch's members (the epoch is [1, 2, 3]; 4 is not)
    let out = c.hear_nudge(3, 4, key, XferNo(9));
    assert!(lease_request(&out).is_none(), "A2: {out:?}");
    // for another carried lease
    let out = c.hear_nudge(3, 1, other, XferNo(9));
    assert!(lease_request(&out).is_none(), "A3: {out:?}");
    // in a frozen epoch
    c.report(3, true, false, true);
    let out = c.hear_nudge(3, 1, key, XferNo(9));
    assert!(lease_request(&out).is_none(), "A1: {out:?}");
    assert_eq!(c.node(3).core.stats.epoch_hold_nudges_ignored_state, 3);
    // and, as a contrast, an active epoch and a member: asked
    c.report(3, true, true, false);
    let out = c.hear_nudge(3, 1, key, XferNo(9));
    assert!(lease_request(&out).is_some(), "{out:?}");
}

/// The hold owner whose `Ended { Owed }` write fails at the close stays the
/// epoch's authority and not silent for nothing: `flush_pending` (the
/// silence) is set only once the record is written.
#[test]
fn a_close_whose_record_fails_does_not_silence_the_owner() {
    let _faults = Faults;
    let mut c = Cluster::new(&[1, 2]);
    // (A row in the journal keeps `flush_pending` from clearing itself.)
    create_nowait(c.node(1), 1, "row");
    let object = c.node(1).core.lease.held.as_ref().unwrap().0.clone();
    let carrier = c.key;
    Faults::write(true);
    let closed = drive_until(c.node(1), carrier, object, &|h| {
        h.core.stats.epoch_hold_end_persist_failed >= 1 && h.core.job().is_none()
    });
    assert!(!closed);
    assert!(
        !c.node(1).core.pr.flush_pending,
        "silent although it did not close"
    );
    assert!(c.node(1).core.lease.epoch_held());
}
