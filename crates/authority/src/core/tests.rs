//! Pure tests of the core: events in, actions out, a real `Meta` as the
//! replica, no runtime, no clock. What the simulation cannot pin down
//! (which action a specific event produces) is pinned here.

use super::*;
use crate::action::{ClientReply, S3Op};
use crate::event::{CasFailure, PeerMsg, Policy, S3Result, UploadResult};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{Meta, MetaStore, MutateOp, MutateOutcome, Rid};
use constellation_store_s3::{Lease, LeaseMode, LeaseStore};

struct Harness {
    core: Core,
    meta: Meta,
    now: Ms,
}

impl Harness {
    fn new(node_id: NodeId) -> Self {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_node_prefix(node_id).unwrap();
        let mut cfg = Config::defaults(node_id, 1);
        cfg.ttl_ms = 10_000;
        cfg.forward_timeout_ms = 500;
        cfg.forward_backoff_ms = 100;
        cfg.forward_retries = 2;
        let mut core = Core::new(cfg);
        let now = Ms(1_000_000);
        let mut out = Vec::new();
        core.start(now, &meta, &mut out);
        // The peer directory knows nodes 2..=4 (never talked to: reachable).
        core.handle(
            now,
            Event::Peers {
                links: (2..=4)
                    .map(|node| crate::event::PeerLink {
                        node,
                        connected: false,
                        last_seen: None,
                    })
                    .collect(),
            },
            &meta,
        );
        Self { core, meta, now }
    }

    fn step(&mut self, event: Event) -> Vec<Action> {
        self.core.handle(self.now, event, &self.meta)
    }

    fn advance(&mut self, ms: u64) {
        self.now = self.now.plus(ms);
    }

    fn rid(&self, seq: u64) -> Rid {
        Rid {
            node: self.core.node_id(),
            incarnation: 1,
            seq,
        }
    }

    fn create(&self, name: &str) -> MutateOp {
        MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino: self.meta.allocate_ino(ROOT_INO).unwrap(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    /// Make this node the holder at `epoch`, view open, as an acquisition
    /// that completed would.
    fn hold(&mut self, epoch: Epoch, gate: Option<PendingGate>) {
        let lease = Lease {
            v: 1,
            partition: "p0".into(),
            holder: self.core.node_id(),
            epoch,
            expires_unix_ms: self.now.plus(10_000).0,
            released: false,
            wanted_by: Vec::new(),
        };
        self.core.lease.adopt(self.now, lease, tag(), gate);
        self.meta.set_holder_epoch(epoch);
    }
}

fn tag() -> constellation_store_s3::LeaseTag {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        let store = std::sync::Arc::new(object_store::memory::InMemory::new());
        LeaseStore::new(store, "p0", LeaseMode::Cas)
            .try_create(&Lease::granted("p0", 1, 1, 1000))
            .await
            .unwrap()
    })
}

fn sends(actions: &[Action]) -> Vec<(NodeId, &PeerMsg)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { to, msg } => Some((*to, msg)),
            _ => None,
        })
        .collect()
}

fn s3_ops(actions: &[Action]) -> Vec<(OpId, &S3Op)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::S3 { op, req } => Some((*op, req)),
            _ => None,
        })
        .collect()
}

fn timers(actions: &[Action], kind: TimerKind) -> Vec<TimerId> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::SetTimer { id, kind: k, .. } if *k == kind => Some(*id),
            _ => None,
        })
        .collect()
}

fn replies(actions: &[Action]) -> Vec<(Rid, &ClientReply)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Reply { rid, reply } => Some((*rid, reply)),
            _ => None,
        })
        .collect()
}

#[test]
fn start_arms_the_poll_and_the_replay_drain() {
    let meta = Meta::open_in_memory().unwrap();
    let mut core = Core::new(Config::defaults(1, 1));
    let mut out = Vec::new();
    core.start(Ms(0), &meta, &mut out);
    assert_eq!(timers(&out, TimerKind::Poll).len(), 1);
    assert_eq!(timers(&out, TimerKind::ReplayDrain).len(), 1);
    assert!(core.job().is_none());
}

#[test]
fn a_timed_out_forward_retries_the_same_rid_then_takes_the_lease_path() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let rid = h.rid(1);
    let op = h.create("a");
    // Submitted: forwarded to the cached holder with a timeout.
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
    });
    let sent = sends(&out);
    assert_eq!(sent.len(), 1);
    let PeerMsg::MutateRequest { rid: r, .. } = sent[0].1 else {
        panic!("{:?}", sent[0].1);
    };
    assert_eq!(*r, rid);
    let mut timeout = timers(&out, TimerKind::ForwardTimeout)[0];
    // Every timeout retries the same rid after a backoff, twice.
    for attempt in 1..=2 {
        h.advance(500);
        let out = h.step(Event::Timer { id: timeout });
        assert!(sends(&out).is_empty(), "no send during the backoff");
        let backoff = timers(&out, TimerKind::ForwardBackoff)[0];
        h.advance(100 * attempt);
        let out = h.step(Event::Timer { id: backoff });
        let sent = sends(&out);
        assert_eq!(sent.len(), 1, "attempt {attempt} resent");
        let PeerMsg::MutateRequest { rid: r, .. } = sent[0].1 else {
            panic!()
        };
        assert_eq!(*r, rid, "the retry carries the same rid");
        timeout = timers(&out, TimerKind::ForwardTimeout)[0];
    }
    assert_eq!(h.core.stats.forward_retries, 2);
    // The budget is spent: the lease path reads the lease object.
    h.advance(500);
    let out = h.step(Event::Timer { id: timeout });
    assert!(sends(&out).is_empty());
    let ops = s3_ops(&out);
    assert!(matches!(ops.as_slice(), [(_, S3Op::LeaseGet)]), "{ops:?}");
    assert_eq!(h.core.job(), Some(JobKind::Acquire));
    assert_eq!(h.core.stats.lease_path_taken, 1);
    assert!(matches!(
        h.core.clients().next(),
        Some((r, ClientPhase::WaitingLease)) if r == rid
    ));
}

#[test]
fn the_lease_path_resolves_an_in_doubt_rid_from_the_log() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let rid = h.rid(1);
    let op = h.create("a");
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: op.clone(),
    });
    let timeout = timers(&out, TimerKind::ForwardTimeout)[0];
    // Meanwhile the holder executed it and its segment was tailed here.
    let holder = Meta::open_in_memory().unwrap();
    holder.set_node_prefix(2).unwrap();
    let records = constellation_meta::execute_mutate(&holder, &op, Some(rid)).unwrap();
    crate::replica::Replica::apply_segment(&h.meta, 1, 1, 0, &records).unwrap();
    // The reply never comes; the retries run out; the lease path starts
    // and this node wins the lease outright (no object yet).
    let mut t = timeout;
    for attempt in 1..=2 {
        h.advance(500);
        let out = h.step(Event::Timer { id: t });
        let b = timers(&out, TimerKind::ForwardBackoff)[0];
        h.advance(100 * attempt);
        let out = h.step(Event::Timer { id: b });
        t = timers(&out, TimerKind::ForwardTimeout)[0];
    }
    h.advance(500);
    let out = h.step(Event::Timer { id: t });
    let (get, _) = s3_ops(&out)[0];
    let out = h.step(Event::S3 {
        op: get,
        result: S3Result::LeaseGet(Ok(None)),
    });
    let (create, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::LeaseCreate { .. }));
    let out = h.step(Event::S3 {
        op: create,
        result: S3Result::LeasePut(Ok(tag())),
    });
    // M13: every acquisition drains the older epochs' inbox inside its
    // gate before the view opens.
    let (drain, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::InboxDrain { below_epoch: 1 }));
    let out = h.step(Event::S3 {
        op: drain,
        result: S3Result::InboxDrain(Ok(Vec::new())),
    });
    // Held now, and the op was found completed: answered without a
    // second execution.
    assert_eq!(h.core.lease().epoch(), Some(1));
    let r = replies(&out);
    assert_eq!(r.len(), 1);
    assert!(matches!(
        r[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { records, .. }) if records.is_empty()
    ));
    assert_eq!(h.core.stats.forward_indoubt_resolved, 1);
    assert_eq!(
        constellation_meta::MetaStore::journal_len(&h.meta).unwrap(),
        0,
        "nothing was journaled locally"
    );
}

#[test]
fn a_peers_forward_is_busy_while_fenced_and_executes_once_the_gate_opens() {
    let mut h = Harness::new(1);
    h.hold(
        3,
        Some(PendingGate {
            epoch: 3,
            takeover: true,
            marker_shipped: true,
            drained: true,
        }),
    );
    let rid = Rid {
        node: 2,
        incarnation: 1,
        seq: 1,
    };
    let op = h.create("a");
    let request = PeerMsg::MutateRequest {
        req: OpId(77),
        rid,
        op: op.clone(),
        acked_through: 0,
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: request.clone(),
    });
    let sent = sends(&out);
    assert!(matches!(
        sent[0].1,
        PeerMsg::MutateReply {
            req: OpId(77),
            outcome: MutateOutcome::Busy,
            ..
        }
    ));
    // The gate completes (nothing queued): the same request executes.
    h.core.lease.gate = None;
    let out = h.step(Event::Peer {
        from: 2,
        msg: request.clone(),
    });
    let sent = sends(&out);
    let PeerMsg::MutateReply {
        outcome: MutateOutcome::Accepted { epoch, records },
        base,
        ..
    } = sent[0].1
    else {
        panic!("{:?}", sent[0].1)
    };
    assert_eq!(*epoch, 3);
    assert_eq!(records.len(), 2, "the create and its completion");
    assert_eq!(*base, Some(0), "nothing shipped or unshipped touched it");
    // A retry of the same rid is answered from `recent`, not executed.
    let out = h.step(Event::Peer {
        from: 2,
        msg: request,
    });
    let sent = sends(&out);
    assert!(matches!(
        sent[0].1,
        PeerMsg::MutateReply {
            outcome: MutateOutcome::Accepted { .. },
            ..
        }
    ));
    assert_eq!(h.core.stats.forward_dedup_hits, 1);
    assert_eq!(
        constellation_meta::MetaStore::journal_len(&h.meta).unwrap(),
        2,
        "executed exactly once"
    );
}

#[test]
fn a_reply_base_names_the_unshipped_overlap() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    // The holder's own local op touches `a` and stays unshipped.
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("a"),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    // A peer's op on the same name: no base (only the log can order it);
    // on another name: the last shipped position.
    let mut ask = |name: &str, req: u64| {
        let op = MutateOp::Unlink {
            parent: ROOT_INO,
            name: name.into(),
        };
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: req,
                },
                op,
                acked_through: 0,
            },
        });
        match sends(&out)[0].1 {
            PeerMsg::MutateReply { base, .. } => *base,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(ask("a", 1), None);
    assert_eq!(ask("b", 2), Some(0));
}

#[test]
fn a_non_holder_declines_a_handoff_at_once() {
    let mut h = Harness::new(1);
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseRequest { req: OpId(5) },
    });
    assert!(matches!(
        sends(&out)[0].1,
        PeerMsg::LeaseHandoff {
            req: OpId(5),
            released: false,
            ..
        }
    ));
    assert!(h.core.job().is_none());
}

#[test]
fn a_holders_handoff_queues_behind_the_round_then_flushes_and_releases() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    // The poll fires: a round starts with the upload pass.
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let poll = timers(&out, TimerKind::Poll)[0];
    let out = h.step(Event::Timer { id: poll });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the round uploads first");
    assert_eq!(h.core.job(), Some(JobKind::Round));
    // A handoff request mid-round is neither answered nor started: it
    // waits for the slot (no lock, no cancellation).
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseRequest { req: OpId(5) },
    });
    assert!(sends(&out).is_empty());
    assert_eq!(h.core.job(), Some(JobKind::Round));
    // The round runs to its end (nothing to renew, tail or ship), and
    // the queued handoff takes the slot: upload, flush, release, reply.
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    assert_eq!(h.core.stats.rounds_completed, 1);
    assert_eq!(h.core.job(), Some(JobKind::Handoff));
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the handoff uploads first");
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    assert!(
        h.core.lease().releasing,
        "new local mutations are closed from the flush to the CAS"
    );
    assert!(h
        .core
        .lease
        .new_mutation_epoch(h.now, h.core.config())
        .is_none());
    let (release, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::LeaseSwap { lease, .. } if lease.released));
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Ok(tag())),
    });
    assert!(matches!(
        sends(&out)[0].1,
        PeerMsg::LeaseHandoff {
            req: OpId(5),
            released: true,
            epoch: 1,
            ..
        }
    ));
    assert!(h.core.lease().held.is_none());
    assert_eq!(h.meta.holder_epoch(), 0);
    assert_eq!(h.core.stats.handoffs_served, 1);
    assert!(h.core.job().is_none());
}

#[test]
fn a_lost_renewal_deposes_and_the_round_recovers() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    // Something unshipped, so the deposition has work to roll back.
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("a"),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    // Past half-TTL the round renews; the CAS is lost to a takeover.
    h.advance(6_000);
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let poll = timers(&out, TimerKind::Poll)[0];
    let out = h.step(Event::Timer { id: poll });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap();
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    let (renew, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::LeaseSwap { .. }));
    let out = h.step(Event::S3 {
        op: renew,
        result: S3Result::LeasePut(Err(CasFailure::Conflict)),
    });
    let (reread, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::LeaseGet));
    let theirs = Lease {
        v: 1,
        partition: "p0".into(),
        holder: 2,
        epoch: 2,
        expires_unix_ms: h.now.plus(10_000).0,
        released: false,
        wanted_by: Vec::new(),
    };
    let out = h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((theirs, tag())))),
    });
    assert!(h.core.lease().lost);
    assert_eq!(h.core.lease().lost_floor, 2);
    assert_eq!(h.meta.holder_epoch(), 0);
    // Recovery tails to head, then strands the unshipped transaction and
    // clears the deposition: the op is queued for replay by rid.
    let (tail, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::SegmentRun { .. }));
    let _ = h.step(Event::S3 {
        op: tail,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    assert!(!h.core.lease().lost);
    assert_eq!(h.core.stats.depositions, 1);
    assert_eq!(h.core.stats.local_rolled_back, 1);
    assert_eq!(h.meta.pending_replays().unwrap().len(), 1);
    assert_eq!(
        constellation_meta::MetaStore::journal_len(&h.meta).unwrap(),
        0,
        "the stranded transaction left the journal"
    );
}

#[test]
fn overlapping_forwards_from_one_node_are_issued_in_order() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let r1 = h.rid(1);
    let r2 = h.rid(2);
    let out1 = h.step(Event::Submit {
        policy: Policy::Client,
        rid: r1,
        op: h.create("a"),
    });
    assert_eq!(sends(&out1).len(), 1);
    // The second op touches the same parent: gated, not sent.
    let out2 = h.step(Event::Submit {
        policy: Policy::Client,
        rid: r2,
        op: MutateOp::Unlink {
            parent: ROOT_INO,
            name: "a".into(),
        },
    });
    assert!(sends(&out2).is_empty());
    assert!(matches!(
        h.core.clients().find(|(r, _)| *r == r2),
        Some((_, ClientPhase::Gated))
    ));
    // The first is answered: the second goes out.
    let PeerMsg::MutateRequest { req, .. } = sends(&out1)[0].1 else {
        panic!()
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateReply {
            req: *req,
            outcome: MutateOutcome::Errno(libc::EINVAL),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
        },
    });
    assert_eq!(replies(&out).len(), 1);
    let sent = sends(&out);
    assert!(matches!(sent.as_slice(), [(2, PeerMsg::MutateRequest { rid, .. })] if *rid == r2));
}

/// Plan 30 M5 round 3: a nudge that lands while a round's forced publish
/// is still in flight must still get the journal shipped at once — the
/// snapshot manager publishes, records the snapshot, then nudges, and
/// `snapshot-mount` reads the snapshot on another node a second later.
#[test]
fn a_nudge_during_an_in_flight_publish_ships_the_journal_at_once() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("a"),
    });
    assert!(!replies(&out).is_empty());
    // A forced publish (`SyncRequest::Publish`): the round uploads, ships
    // `a`, then publishes.
    let out = h.step(Event::Control {
        op: OpId(900),
        req: Control::PublishNow,
    });
    let poll = timers(&out, TimerKind::Poll)[0];
    let out = h.step(Event::Timer { id: poll });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the round uploads first");
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    let (put, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::SegmentPut { .. }), "{req:?}");
    let out = h.step(Event::S3 {
        op: put,
        result: S3Result::SegmentPut(Ok(())),
    });
    let publish = out
        .iter()
        .find_map(|a| match a {
            Action::Publish { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the forced publish is issued");
    assert_eq!(h.core.stats.rounds_completed, 1, "the round ended");
    // The publish is still running when the snapshot record is journaled
    // and the nudge arrives.
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(2),
        op: h.create("b"),
    });
    assert!(!replies(&out).is_empty());
    let out = h.step(Event::Control {
        op: OpId(901),
        req: Control::Nudge,
    });
    let polls = timers(&out, TimerKind::Poll);
    assert_eq!(
        polls.len(),
        1,
        "the nudge re-arms the poll at once: {out:?}"
    );
    let out = h.step(Event::Timer { id: polls[0] });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the nudged round starts with the upload pass");
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    assert!(
        s3_ops(&out)
            .iter()
            .any(|(_, req)| matches!(req, S3Op::SegmentPut { .. })),
        "the nudged round ships `b` while the publish is still in flight: {out:?}"
    );
    let _ = h.step(Event::PublishDone {
        op: publish,
        ok: true,
    });
}

/// Plan 30 M5 round 3: the reply base is the last *shipped* position that
/// touched the op's keys (or the window floor), not the head. Under a
/// forward burst every requester trails the head by a segment, and a
/// head base sent every accepted reply to `AwaitingLog` — the 8×
/// slowdown of `holder-ships-under-forward-load`.
#[test]
fn a_reply_base_is_the_last_shipped_touch_not_the_head() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    // Ship two of the holder's own segments: `a` at seq 1, `b` at seq 2.
    let ship_local = |h: &mut Harness, name: &str, rid: u64| {
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid: h.rid(rid),
            op: h.create(name),
        });
        assert!(!replies(&out).is_empty());
        let out = h.step(Event::Control {
            op: OpId(700 + rid),
            req: Control::Nudge,
        });
        let poll = timers(&out, TimerKind::Poll)[0];
        let out = h.step(Event::Timer { id: poll });
        let upload = out
            .iter()
            .find_map(|a| match a {
                Action::UploadDirtyChunks { op, .. } => Some(*op),
                _ => None,
            })
            .expect("the round uploads first");
        let out = h.step(Event::UploadsDone {
            op: upload,
            result: UploadResult::Done { held: 0 },
        });
        let (put, req) = s3_ops(&out)[0];
        assert!(matches!(req, S3Op::SegmentPut { .. }), "{req:?}");
        h.step(Event::S3 {
            op: put,
            result: S3Result::SegmentPut(Ok(())),
        });
    };
    ship_local(&mut h, "a", 1);
    ship_local(&mut h, "b", 2);
    assert_eq!(h.core.ship().head_seq, 2);
    let ask = |h: &mut Harness, name: &str, req: u64| {
        let op = MutateOp::Unlink {
            parent: ROOT_INO,
            name: name.into(),
        };
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: req,
                },
                op,
                acked_through: 0,
            },
        });
        match sends(&out)[0].1 {
            PeerMsg::MutateReply { base, .. } => *base,
            other => panic!("{other:?}"),
        }
    };
    // `c` was never touched: a requester that has applied the tenure's
    // start (floor 0) may install the reply. `a` needs seq 1, `b` seq 2.
    assert_eq!(ask(&mut h, "c", 1), Some(0));
    assert_eq!(ask(&mut h, "a", 2), Some(1));
    assert_eq!(ask(&mut h, "b", 3), Some(2));
}

/// Plan 30 M5 round 3: a node whose own journal is unshipped (an
/// ephemeral clone made at mount, `snapshot-mount`) must not forward:
/// the holder has none of those records and would refuse `ENOENT`.
/// Such an op takes the lease path.
#[test]
fn an_op_from_a_node_with_an_unshipped_journal_takes_the_lease_path() {
    let mut h = Harness::new(1);
    // A local write without the lease, as `eager_clone` does at mount.
    h.meta.mkdir(ROOT_INO, "clone", 0o755, 0, 0).unwrap();
    assert!(MetaStore::journal_len(&h.meta).unwrap() > 0);
    h.core.lease.cached_holder = Some(2);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("in-clone"),
    });
    assert!(
        sends(&out).is_empty(),
        "nothing is forwarded while the journal is unshipped: {out:?}"
    );
    assert!(
        s3_ops(&out)
            .iter()
            .all(|(_, req)| !matches!(req, S3Op::InboxPut { .. })),
        "nothing goes to an inbox either: {out:?}"
    );
    assert_eq!(h.core.job(), Some(JobKind::Acquire), "the lease path");
    assert!(matches!(
        h.core.clients().next(),
        Some((_, ClientPhase::WaitingLease))
    ));
}

/// Plan 30 M5 round 4: an op that was submitted to the inbox more than
/// once (each submission its own batch) withdraws *every* batch before
/// it is forwarded over P2P — a forgotten one would be drained by the
/// next takeover after the client was answered from the forward.
#[test]
fn every_inbox_batch_of_a_rid_is_withdrawn_before_a_forward() {
    use constellation_store_s3::inbox::InboxKey;
    let mut h = Harness::new(1);
    let rid = h.rid(1);
    let keys: Vec<InboxKey> = (0..3)
        .map(|n| InboxKey {
            epoch: 1,
            node: 1,
            n,
        })
        .collect();
    // The rid's earlier attempt left three batches behind and was
    // answered in doubt; its resubmission inherits them.
    h.core.in_doubt_rids.insert(rid);
    h.core.in_doubt_batches.insert(rid, keys.clone());
    h.core.lease.cached_holder = Some(2);
    let mut out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: h.create("a"),
    });
    for key in &keys {
        assert!(
            sends(&out).is_empty(),
            "nothing is forwarded while a batch is still durable: {out:?}"
        );
        let (op, req) = s3_ops(&out)[0];
        assert_eq!(req, &S3Op::InboxDelete { key: *key }, "batches go in order");
        assert!(matches!(
            h.core.clients().next(),
            Some((_, ClientPhase::InboxWithdraw))
        ));
        out = h.step(Event::S3 {
            op,
            result: S3Result::InboxDelete(Ok(())),
        });
    }
    assert!(
        matches!(sends(&out)[0].1, PeerMsg::MutateRequest { rid: r, .. } if *r == rid),
        "forwarded once every batch is gone: {out:?}"
    );
    assert_eq!(h.core.stats.inbox_withdrawn_ops, 3);
    assert_eq!(h.core.stats.inbox_multi_batch_withdrawals, 1);
    assert!(h.core.clients.get(&rid).unwrap().inbox_keys.is_empty());
    // The holder answers; the op leaves the machine (and the key gate).
    let req = match sends(&out)[0].1 {
        PeerMsg::MutateRequest { req, .. } => *req,
        other => panic!("{other:?}"),
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Errno(libc::EIO),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
        },
    });
    assert_eq!(replies(&out).len(), 1);

    // A delete that fails leaves the batch drainable: the op takes the
    // lease path instead of forwarding past it.
    let rid2 = h.rid(2);
    h.core.in_doubt_rids.insert(rid2);
    h.core.in_doubt_batches.insert(
        rid2,
        vec![InboxKey {
            epoch: 1,
            node: 1,
            n: 7,
        }],
    );
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: rid2,
        op: h.create("b"),
    });
    let (op, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::InboxDelete { .. }));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::InboxDelete(Err(crate::event::S3Failure("500".into()))),
    });
    assert!(sends(&out).is_empty(), "{out:?}");
    assert!(matches!(
        h.core.clients().find(|(r, _)| *r == rid2),
        Some((_, ClientPhase::WaitingLease))
    ));
}

/// Plan 30 M5 round 5: `release_gated` re-enters itself through
/// `finish`; two ops gated behind one that finishes are both released
/// by the nested pass, and the outer pass must skip what is gone rather
/// than index it (the `no entry found for key` of long seed 11932).
#[test]
fn releasing_several_gated_ops_survives_the_nested_release() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let a = h.rid(1);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: a,
        op: h.create("a"),
    });
    let req = match sends(&out)[0].1 {
        PeerMsg::MutateRequest { req, .. } => *req,
        other => panic!("{other:?}"),
    };
    // Two more ops on the same directory queue behind `a` at the gate.
    for (seq, name) in [(2, "b"), (3, "c")] {
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid: h.rid(seq),
            op: h.create(name),
        });
        assert!(sends(&out).is_empty(), "{name} is gated: {out:?}");
    }
    assert_eq!(
        h.core
            .clients()
            .filter(|(_, p)| *p == ClientPhase::Gated)
            .count(),
        2
    );
    // This node becomes the holder meanwhile; `a`'s reply then releases
    // both, and each finishes locally — the second one inside the
    // release the first one's `finish` triggers.
    h.hold(1, None);
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Errno(libc::EIO),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
        },
    });
    let answered: Vec<Rid> = replies(&out).into_iter().map(|(rid, _)| rid).collect();
    assert_eq!(answered, vec![a, h.rid(2), h.rid(3)]);
    assert_eq!(h.core.clients().count(), 0);
}

// ---- plan 30 §M6: positions and the session wait ----

/// A requester (node 2) forwarding to a holder (node 1): one op through
/// the pair, returning the requester's client reply.
fn forward_through(
    holder: &mut Harness,
    requester: &mut Harness,
    rid: Rid,
    op: MutateOp,
) -> Vec<Action> {
    let out = requester.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
    });
    let sent = sends(&out);
    let [(1, request)] = sent.as_slice() else {
        panic!("expected one forward to node 1: {sent:?}")
    };
    let answer = holder.step(Event::Peer {
        from: 2,
        msg: (*request).clone(),
    });
    let reply = sends(&answer)[0].1.clone();
    requester.step(Event::Peer {
        from: 1,
        msg: reply,
    })
}

fn pair() -> (Harness, Harness) {
    let mut holder = Harness::new(1);
    holder.hold(1, None);
    let mut requester = Harness::new(2);
    requester.step(Event::Peers {
        links: [1, 3]
            .into_iter()
            .map(|node| crate::event::PeerLink {
                node,
                connected: true,
                last_seen: None,
            })
            .collect(),
    });
    requester.core.lease.cached_holder = Some(1);
    requester.meta.session().set_budget_ms(20);
    (holder, requester)
}

/// Coordinator decision 2: a forwarded op accepted and installed as a
/// shadow raises nothing, so `touch a; ls; stat .` (and `stat a`) right
/// after forwarded creates never waits — even though the holder has not
/// shipped any of them. Decision 3: the shadow's records carry the parent
/// directory's update (its mtime moves here at once).
#[test]
fn touch_ls_stat_after_forwarded_creates_take_the_fast_path() {
    use constellation_meta::{MetaStore, ReadKey, SessionWait};
    let (mut holder, mut requester) = pair();
    let before = requester.meta.getattr(ROOT_INO).unwrap().unwrap().mtime_ns;
    for (i, name) in ["a", "b", "c"].into_iter().enumerate() {
        let rid = requester.rid(i as u64 + 1);
        let op = requester.create(name);
        let out = forward_through(&mut holder, &mut requester, rid, op);
        assert!(matches!(
            replies(&out)[0].1,
            ClientReply::Outcome(MutateOutcome::Accepted { .. })
        ));
        for keys in [
            vec![ReadKey::Dentry(ROOT_INO, name.into())],
            vec![ReadKey::Dir(ROOT_INO)],
            vec![ReadKey::Ino(ROOT_INO)],
        ] {
            assert_eq!(
                requester.meta.session_wait(&keys),
                SessionWait::Fast,
                "{name}: {keys:?}"
            );
        }
    }
    assert_eq!(requester.core.stats.shadows_installed, 3);
    assert_eq!(requester.meta.session().stats().raised, 0);
    let after = requester.meta.getattr(ROOT_INO).unwrap().unwrap().mtime_ns;
    assert!(after > before, "the shadow moved the parent's mtime");
    assert!(
        MetaStore::lookup(&requester.meta, ROOT_INO, "c")
            .unwrap()
            .is_some(),
        "the entry is visible"
    );
}

/// A refusal observes the holder's unshipped state: later reads wait for
/// it (here they time out at the 20 ms test budget, degraded), except a
/// key a newer shadow covers — the parent's attributes included — until
/// the segment carrying it is applied.
#[test]
fn a_refusal_raises_observed_until_the_segment_lands() {
    use constellation_meta::{ReadKey, SessionWait};
    let (mut holder, mut requester) = pair();
    // The holder creates `a` itself; it stays unshipped.
    let out = holder.step(Event::Submit {
        policy: Policy::Client,
        rid: holder.rid(1),
        op: holder.create("a"),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    // The requester's create of `a` is refused against it: no hint (the
    // holder's unshipped journal touched the name), `observed` rises.
    let rid = requester.rid(1);
    let op = requester.create("a");
    let out = forward_through(&mut holder, &mut requester, rid, op);
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Exists { .. } | MutateOutcome::Errno(libc::EEXIST))
    ));
    assert_eq!(requester.meta.session().stats().raised, 1);
    let observed = requester.meta.session().observed();
    assert!(observed.pending.is_some(), "{observed:?}");
    assert!(matches!(
        requester
            .meta
            .session_wait(&[ReadKey::Dentry(ROOT_INO, "a".into())]),
        SessionWait::TimedOut(_)
    ));
    // A later forwarded create of `d` is installed at a newer position:
    // it covers its own name, but neither the listing nor the parent's
    // attributes (outside the per-key base, M5 round 3).
    let rid = requester.rid(2);
    let op = requester.create("d");
    forward_through(&mut holder, &mut requester, rid, op);
    assert_eq!(
        requester
            .meta
            .session_wait(&[ReadKey::Dentry(ROOT_INO, "d".into())]),
        SessionWait::Covered
    );
    for keys in [vec![ReadKey::Ino(ROOT_INO)], vec![ReadKey::Dir(ROOT_INO)]] {
        assert!(
            matches!(requester.meta.session_wait(&keys), SessionWait::TimedOut(_)),
            "{keys:?}"
        );
    }
    // The holder ships its journal; the requester applies the segment.
    let batch = constellation_meta::MetaStore::take_journal(&holder.meta, 100).unwrap();
    let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
    let through = holder.meta.journal_through_after(&seqs).unwrap();
    let records: Vec<_> = batch.into_iter().map(|(_, r)| r).collect();
    crate::replica::Replica::apply_segment(&requester.meta, 1, 1, through, &records).unwrap();
    for keys in [
        vec![ReadKey::Dentry(ROOT_INO, "a".into())],
        vec![ReadKey::Dir(ROOT_INO)],
        vec![ReadKey::Ino(ROOT_INO)],
    ] {
        assert_eq!(
            requester.meta.session_wait(&keys),
            SessionWait::Fast,
            "{keys:?}"
        );
    }
    let stats = requester.meta.session().stats();
    assert_eq!((stats.timeouts, stats.covered), (3, 1), "{stats:?}");
}

/// Read-your-writes across a stranding: while one of this node's own ops
/// is queued for replay (rolled back), reads of its keys wait even on the
/// fast path; other keys do not.
#[test]
fn a_queued_replay_blocks_reads_of_its_keys() {
    use constellation_meta::{ReadKey, SessionWait};
    let requester = Harness::new(2);
    requester.meta.session().set_budget_ms(20);
    let op = requester.create("x");
    requester.meta.queue_replay(requester.rid(1), &op).unwrap();
    assert!(matches!(
        requester
            .meta
            .session_wait(&[ReadKey::Dentry(ROOT_INO, "x".into())]),
        SessionWait::TimedOut(_)
    ));
    assert!(matches!(
        requester.meta.session_wait(&[ReadKey::Dir(ROOT_INO)]),
        SessionWait::TimedOut(_)
    ));
    assert_eq!(
        requester
            .meta
            .session_wait(&[ReadKey::Dentry(ROOT_INO, "y".into())]),
        SessionWait::Fast
    );
    let queued = requester.meta.pending_replays().unwrap();
    requester.meta.forget_replay(queued[0].queue_seq).unwrap();
    assert_eq!(
        requester
            .meta
            .session_wait(&[ReadKey::Dentry(ROOT_INO, "x".into())]),
        SessionWait::Fast
    );
    assert_eq!(requester.meta.session().stats().replay_blocked, 2);
}
