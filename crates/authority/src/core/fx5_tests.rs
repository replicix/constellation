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
    let answer = c.exchange(2, 3, false);
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

