//! Pure tests of the core: events in, actions out, a real `Meta` as the
//! replica, no runtime, no clock. What the simulation cannot pin down
//! (which action a specific event produces) is pinned here.

use super::*;
use crate::action::{ClientReply, S3Op};
use crate::event::{CasFailure, PeerMsg, Policy, S3Result, UploadResult};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, MutateOutcome, Rid};
use constellation_store_s3::{Lease, LeaseMode, LeaseStore};

struct Harness {
    core: Core,
    meta: Meta,
    now: Ms,
}

impl Harness {
    fn new(node_id: NodeId) -> Self {
        Self::with(node_id, false)
    }

    /// Plan 30 §M7: `streams` turns log streams on (the other tests pin
    /// the forwarding and lease machinery's own sends).
    fn with(node_id: NodeId, streams: bool) -> Self {
        let meta = Meta::open_in_memory().unwrap();
        meta.set_node_prefix(node_id).unwrap();
        let mut cfg = Config::defaults(node_id, 1);
        cfg.log_streams = streams;
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
                        rtt_ms: None,
                        since: None,
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
            backups: Vec::new(),
            config_version: 1,
            ack_policy: constellation_store_s3::AckPolicy::Local,
            // A tenure that serves strict reads (plan 30 §M9: the flag is
            // set by one CAS before the first answer; `a_tenure_marks_
            // itself_before_its_first_strict_answer` covers that).
            granted_delegations: true,
            retired: Vec::new(),
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
    crate::replica::Replica::apply_segment(&h.meta, 1, 1, 0, &[], &[], &records).unwrap();
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
            fast_prev: None,
            backup_tail_epoch: None,
            shippable: false,
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
        deps: constellation_meta::Position::ZERO,
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
                deps: constellation_meta::Position::ZERO,
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
        backups: Vec::new(),
        config_version: 2,
        ack_policy: constellation_store_s3::AckPolicy::Local,
        granted_delegations: false,
        retired: Vec::new(),
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
            gen: 0,
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
                deps: constellation_meta::Position::ZERO,
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
            gen: 0,
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
            gen: 0,
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
                rtt_ms: Some(1),
                since: Some(Ms(0)),
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
    crate::replica::Replica::apply_segment(&requester.meta, 1, 1, through, &[], &[], &records)
        .unwrap();
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

/// Harness `deposed-reintegration` (PROGRESS, "Fix: deposed-reintegration
/// regression"): A's stranded overwrite of `same` is a size-only truncate
/// (the FUSE `O_TRUNC`) followed by the manifest commit, both queued for
/// replay by rid. B has overwritten `same` since. When A takes the lease
/// back, its takeover gate replays the queue locally: the truncate must
/// be folded into the manifest commit (whose base check refuses it, so
/// A's bytes become a conflict copy) — not executed on its own, which
/// truncated B's winner to nothing.
#[test]
fn the_takeover_gates_local_replay_folds_a_truncate_into_its_manifest_commit() {
    use constellation_fs_core::{ChunkHash, Manifest};
    let mut h = Harness::new(1);
    let file = h.meta.create(ROOT_INO, "same", 0o644, 0, 0).unwrap();
    let manifest = |bytes: &[u8]| {
        let chunks = [(0u64, ChunkHash::of(bytes))].into_iter().collect();
        Manifest::from_sparse_chunks(4096, bytes.len() as u64, chunks, 64, ChunkHash::of)
            .0
            .encode()
    };
    let set = |base: Option<Vec<u8>>, bytes: &[u8]| MutateOp::SetManifest {
        ino: file.ino,
        base_manifest: base,
        manifest: manifest(bytes),
        size: bytes.len() as u64,
    };
    let base = manifest(b"baseline");
    constellation_meta::execute_mutate(&h.meta, &set(None, b"baseline"), None).unwrap();
    // B's winner, in the log before A comes back.
    constellation_meta::execute_mutate(&h.meta, &set(Some(base.clone()), b"winner-from-b"), None)
        .unwrap();
    // A's stranded `O_TRUNC` + write, queued for replay by rid.
    let a = |seq| Rid {
        node: 2,
        incarnation: 1,
        seq,
    };
    let truncate = MutateOp::Setattr {
        ino: file.ino,
        mode: None,
        uid: None,
        gid: None,
        size: Some(0),
        atime_ns: None,
        mtime_ns: None,
    };
    h.meta.queue_replay(a(1), &truncate).unwrap();
    h.meta
        .queue_replay(a(2), &set(Some(base), b"loser-from-a"))
        .unwrap();

    h.hold(2, None);
    let mut out = Vec::new();
    h.core
        .replay_queue_locally(h.now, &h.meta, &mut out)
        .unwrap();

    let attr = h.meta.getattr(file.ino).unwrap().unwrap();
    assert_eq!(
        attr.size, 13,
        "B's winner keeps its size (was truncated to 0)"
    );
    assert_eq!(
        MetaStore::manifest(&h.meta, file.ino).unwrap(),
        Some(manifest(b"winner-from-b")),
        "B's winner keeps its content"
    );
    let queue = h.meta.pending_replays().unwrap();
    assert_eq!(queue.len(), 1, "the truncate is folded away: {queue:?}");
    assert_eq!(queue[0].rid, a(2));
    assert!(
        queue[0].refused.is_some(),
        "the manifest commit is refused on its base (its conflict copy follows)"
    );
    assert!(h.meta.completed_outcome(a(1)).unwrap().is_none());
}

/// Plan 30 §M9 meets §M3b (harness `deposed-reintegration-backup`): the
/// holder's own manifest commit (the FUSE close, `set_manifest_dirty`) has
/// no client rid; deposed, the holder queues it for replay under its
/// local replay rid. If its backup adopted the commit when it took over,
/// the log carries it — and must say so, by that rid, or the replay
/// re-evaluates the commit against whatever the successor wrote since
/// and materializes a conflict copy of a version that was never lost.
#[test]
fn a_deposed_holders_adopted_manifest_commit_is_not_replayed() {
    use constellation_fs_core::{ChunkHash, Manifest};
    let mut h = Harness::new(1);
    let file = h.meta.create(ROOT_INO, "same", 0o644, 0, 0).unwrap();
    // Everything so far is in the log.
    let rows = MetaStore::take_journal(&h.meta, usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    h.meta.ack_journal_rows_at(&seqs, 1).unwrap();

    // A holds epoch 1 and closes the file: a local manifest commit.
    h.meta.set_holder_epoch(1);
    let chunks = [(0u64, ChunkHash::of(b"loser-from-a"))]
        .into_iter()
        .collect();
    let manifest = Manifest::from_sparse_chunks(4096, 12, chunks, 64, ChunkHash::of)
        .0
        .encode();
    h.meta
        .set_manifest_dirty(file.ino, None, &manifest, 12, &[])
        .unwrap();
    let adopted: Vec<LogRecord> = MetaStore::take_journal(&h.meta, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|(_, r)| r)
        .collect();
    assert!(
        adopted.iter().any(|r| matches!(
            r,
            LogRecord::Completed { rid }
                if rid.incarnation == constellation_meta::LOCAL_REPLAY_INCARNATION
        )),
        "the manifest commit carries its replay rid's completion: {adopted:?}"
    );

    // Deposed: the unshipped commit is rolled back and queued by rid.
    h.meta.set_holder_epoch(0);
    h.meta.strand_below_epoch(2).unwrap();
    let queue = h.meta.pending_replays().unwrap();
    assert_eq!(queue.len(), 1, "{queue:?}");
    assert!(
        h.meta.completed_outcome(queue[0].rid).unwrap().is_none(),
        "the stranding forgets the rid's own completion"
    );

    // B adopted it from its backup tail and shipped it under epoch 2.
    crate::replica::Replica::apply_segment(&h.meta, 2, 2, 0, &[], &[], &adopted).unwrap();
    let mut out = Vec::new();
    h.core.on_drain_tick(h.now, &h.meta, &mut out);
    assert!(
        h.meta.pending_replays().unwrap().is_empty(),
        "the replay found its completion in the log"
    );
    assert!(
        sends(&out).is_empty() && replies(&out).is_empty(),
        "{out:?}"
    );
    assert_eq!(h.core.stats.replay_conflicts, 0);
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

// ---- an `AwaitingLog` forward when this node takes the lease over ----

/// Node 2 forwards `create name` to node 1, which accepts it under epoch
/// 1 on a base node 2 has not applied (`base: None`): the op waits for
/// the log (`AwaitingLog`). Returns the requester, the rid and the
/// holder's records (not applied anywhere yet).
fn awaiting_log_forward(name: &str) -> (Harness, Rid, Vec<LogRecord>) {
    let (holder, mut requester) = pair();
    let rid = requester.rid(1);
    let op = requester.create(name);
    let out = requester.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: op.clone(),
    });
    let sent = sends(&out);
    let [(1, PeerMsg::MutateRequest { req, .. })] = sent.as_slice() else {
        panic!("expected one forward to node 1: {sent:?}")
    };
    let req = *req;
    let records = constellation_meta::execute_mutate(&holder.meta, &op, Some(rid)).unwrap();
    let out = requester.step(Event::Peer {
        from: 1,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Accepted {
                epoch: 1,
                records: records.clone(),
            },
            base: None,
            position: constellation_meta::Position {
                seq: 1,
                pending: Some(constellation_meta::JournalPos { epoch: 1, jseq: 9 }),
                streams: Default::default(),
            },
            gen: 0,
        },
    });
    assert!(replies(&out).is_empty(), "the op waits for the log");
    assert_eq!(
        requester.core.clients().collect::<Vec<_>>(),
        vec![(rid, ClientPhase::AwaitingLog)]
    );
    (requester, rid, records)
}

fn activity(h: &mut Harness) -> Vec<Action> {
    let now = h.now;
    h.step(Event::Activity {
        last_write: now,
        acked_seqs: Vec::new(),
    })
}

/// The harness `session-forwarded-ryw` stall (PROGRESS, "Fix:
/// AwaitingLog forwards resolved on takeover"): node 2's forward waits
/// for the log, node 1 stalls, node 2 takes the lease over — and the
/// op's completion comes with the takeover (the tail to head, or node
/// 1's backup tail applied into node 2's own journal), not through a
/// segment the stream or a tail delivers later. Node 2 answers the op
/// from `completed` as soon as its view opens, without executing it
/// again. Before the fix it waited out the client deadline and answered
/// in doubt (`EIO`) for a write that had landed.
#[test]
fn an_awaiting_log_forward_is_answered_from_completed_once_this_node_holds() {
    let (mut requester, rid, records) = awaiting_log_forward("a");
    // The takeover brought the completion (here: applied from the log;
    // the backup tail lands in `completed` the same way).
    crate::replica::Replica::apply_segment(&requester.meta, 1, 1, 0, &[], &[], &records).unwrap();
    let journal_before = constellation_meta::MetaStore::journal_len(&requester.meta).unwrap();
    // The gate is still pending: nothing moves yet.
    requester.hold(
        2,
        Some(PendingGate {
            epoch: 2,
            takeover: true,
            marker_shipped: false,
            drained: false,
            fast_prev: None,
            backup_tail_epoch: None,
            shippable: false,
        }),
    );
    let out = activity(&mut requester);
    assert!(replies(&out).is_empty(), "not before the gate opens");
    // The view opens: the next event answers it.
    requester.core.lease.gate = None;
    let out = activity(&mut requester);
    let r = replies(&out);
    assert_eq!(r.len(), 1, "answered at once: {out:?}");
    assert_eq!(r[0].0, rid);
    assert!(matches!(
        r[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { records, .. }) if records.is_empty()
    ));
    assert_eq!(requester.core.stats.awaiting_log_resolved, 1);
    assert_eq!(requester.core.stats.forward_indoubt_resolved, 1);
    assert_eq!(
        constellation_meta::MetaStore::journal_len(&requester.meta).unwrap(),
        journal_before,
        "not executed a second time"
    );
    assert_eq!(requester.core.clients().count(), 0);
}

/// The same, but node 1's acceptance never landed (it stalled with the
/// row unshipped and no backup had it): under node 2's epoch the op
/// executes here by rid, once, and is answered.
#[test]
fn an_awaiting_log_forward_that_never_landed_executes_here_once_this_node_holds() {
    let (mut requester, rid, _) = awaiting_log_forward("b");
    requester.hold(2, None);
    let out = activity(&mut requester);
    let r = replies(&out);
    assert_eq!(r.len(), 1, "answered at once: {out:?}");
    assert_eq!(r[0].0, rid);
    assert!(matches!(
        r[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { records, .. }) if !records.is_empty()
    ));
    assert_eq!(requester.core.stats.awaiting_log_resolved, 1);
    assert!(
        requester.meta.completed_outcome(rid).unwrap().is_some(),
        "journaled here under the new epoch"
    );
    assert!(MetaStore::lookup(&requester.meta, ROOT_INO, "b")
        .unwrap()
        .is_some());
}

/// Not moot: the acceptance was under the epoch this node holds (a
/// delegate's stream, or a lease this node re-took at the same epoch) —
/// the log carries it; and a continuation-epoch hold (S3 away:
/// `completed` is not exact) resolves nothing.
#[test]
fn an_awaiting_log_forward_under_the_held_epoch_or_a_continuation_hold_keeps_waiting() {
    let (mut requester, rid, _) = awaiting_log_forward("c");
    requester.hold(1, None);
    let out = activity(&mut requester);
    assert!(replies(&out).is_empty());
    assert_eq!(
        requester.core.clients().collect::<Vec<_>>(),
        vec![(rid, ClientPhase::AwaitingLog)]
    );

    let (mut requester, rid, _) = awaiting_log_forward("d");
    requester.core.lease.adopt_epoch_hold(requester.now, 5);
    let out = activity(&mut requester);
    assert!(replies(&out).is_empty());
    assert_eq!(
        requester.core.clients().collect::<Vec<_>>(),
        vec![(rid, ClientPhase::AwaitingLog)]
    );
}

// ---- plan 30 §M7: log streams ----

/// A holder (node 1, streams on, holding epoch 1) and a subscriber
/// (node 2) that knows it as the holder and has subscribed; returns the
/// subscription's `req` after node 1 accepted it and node 2 applied the
/// first frame (a heartbeat).
fn stream_pair() -> (Harness, Harness, OpId) {
    let mut holder = Harness::with(1, true);
    holder.hold(1, None);
    let mut sub = Harness::with(2, true);
    sub.core.lease.cached_holder = Some(1);
    let out = sub.step(Event::Peers { links: Vec::new() });
    let sent = sends(&out);
    let [(1, PeerMsg::LogSubscribe { req, from })] = sent.as_slice() else {
        panic!("expected one subscription to node 1: {sent:?}")
    };
    assert_eq!(*from, 1, "subscribes from its next sequence");
    let req = *req;
    let served = holder.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LogSubscribe { req, from: 1 },
    });
    let frames = sends(&served);
    let [(
        2,
        PeerMsg::LogStream {
            n: 0,
            segment: None,
            ..
        },
    )] = frames.as_slice()
    else {
        panic!("expected the opening heartbeat: {frames:?}")
    };
    let first = frames[0].1.clone();
    sub.step(Event::Peer {
        from: 1,
        msg: first,
    });
    assert!(sub.core.stream_view().live);
    (holder, sub, req)
}

/// The holder executes a create and "ships" it as segment `seq`: the
/// encoded payload, as the S3 object would hold it.
fn holder_segment(holder: &mut Harness, name: &str, seq: Seq) -> Vec<u8> {
    let rid = holder.rid(seq);
    let op = holder.create(name);
    let out = holder.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    let batch = Replica::take_journal(&holder.meta, 100).unwrap();
    let records: Vec<_> = batch.iter().map(|(_, r)| r.clone()).collect();
    let seqs: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
    Replica::ack_journal(&holder.meta, &seqs, seq, None).unwrap();
    holder.core.ship.next_seq = seq + 1;
    holder.core.ship.head_seq = seq;
    crate::segment::encode(1, 1, 0, &[], &[], &records).unwrap()
}

fn stream_frames(actions: &[Action]) -> Vec<PeerMsg> {
    sends(actions)
        .into_iter()
        .filter(|(_, m)| matches!(m, PeerMsg::LogStream { .. }))
        .map(|(_, m)| m.clone())
        .collect()
}

#[test]
fn a_streamed_segment_is_applied_through_the_tail_path() {
    use constellation_meta::MetaStore;
    let (mut holder, mut sub, _) = stream_pair();
    let payload = holder_segment(&mut holder, "a", 1);
    let mut out = Vec::new();
    holder
        .core
        .stream_passed(holder.now, 1, 1, &payload, &mut out);
    let frames = stream_frames(&out);
    assert_eq!(frames.len(), 1);
    assert!(matches!(
        &frames[0],
        PeerMsg::LogStream {
            n: 1,
            head: 1,
            segment: Some((1, _)),
            ..
        }
    ));
    sub.step(Event::Peer {
        from: 1,
        msg: frames[0].clone(),
    });
    assert_eq!(Replica::applied_seq(&sub.meta).unwrap(), 1);
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "a")
        .unwrap()
        .is_some());
    assert_eq!(sub.core.stats.stream_applied, 1);
    assert_eq!(sub.core.ship().next_seq, 2);
    // A duplicate (a resubscription replaying the ring) is ignored.
    let again = PeerMsg::LogStream {
        req: match &frames[0] {
            PeerMsg::LogStream { req, .. } => *req,
            _ => unreachable!(),
        },
        n: 2,
        epoch: 1,
        head: 1,
        segment: Some((1, payload)),
    };
    sub.step(Event::Peer {
        from: 1,
        msg: again,
    });
    assert_eq!(sub.core.stats.stream_duplicates, 1);
    assert_eq!(Replica::applied_seq(&sub.meta).unwrap(), 1);
}

/// A lost frame (the numbering jumps) breaks the stream: the subscriber
/// unsubscribes, counts the gap, and a round runs at once to read the log
/// from S3.
#[test]
fn a_frame_gap_breaks_the_stream() {
    let (_holder, mut sub, req) = stream_pair();
    let out = sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LogStream {
            req,
            n: 2,
            epoch: 1,
            head: 3,
            segment: None,
        },
    });
    assert_eq!(sub.core.stats.stream_gaps, 1);
    assert!(sends(&out)
        .iter()
        .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LogUnsubscribe { req: r } if *r == req)));
    assert!(!timers(&out, TimerKind::Poll).is_empty(), "no round nudged");
    assert_eq!(sub.core.stream_view().upstream, 0);
}

/// A segment ahead of the cursor waits in the buffer (and a round is
/// nudged to read the missing one); once the missing sequence arrives,
/// both apply in order.
#[test]
fn a_segment_ahead_of_the_cursor_waits_for_it() {
    let (mut holder, mut sub, req) = stream_pair();
    let p1 = holder_segment(&mut holder, "a", 1);
    let p2 = holder_segment(&mut holder, "b", 2);
    let out = sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LogStream {
            req,
            n: 1,
            epoch: 1,
            head: 2,
            segment: Some((2, p2)),
        },
    });
    assert_eq!(Replica::applied_seq(&sub.meta).unwrap(), 0);
    assert_eq!(sub.core.stream_view().buffered, 1);
    assert!(
        !timers(&out, TimerKind::Poll).is_empty(),
        "the gap must nudge a round"
    );
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LogStream {
            req,
            n: 2,
            epoch: 1,
            head: 2,
            segment: Some((1, p1)),
        },
    });
    assert_eq!(Replica::applied_seq(&sub.meta).unwrap(), 2);
    assert_eq!(sub.core.stats.stream_applied, 2);
}

/// The round a caught-up subscriber runs skips its S3 tail; once the
/// backstop period has passed it probes S3 once, one GET wide.
#[test]
fn a_caught_up_stream_skips_the_tail_until_the_backstop() {
    let (_holder, mut sub, req) = stream_pair();
    let run_round = |sub: &mut Harness| -> Vec<Action> {
        sub.core.nudge(sub.now, &mut Vec::new());
        let mut all = Vec::new();
        let poll = sub
            .core
            .timers
            .iter()
            .find(|(_, (t, _))| matches!(t, Timer::Poll))
            .map(|(id, _)| *id)
            .expect("poll timer");
        let out = sub.step(Event::Timer { id: poll });
        let upload = out.iter().find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        });
        all.extend(out);
        if let Some(op) = upload {
            all.extend(sub.step(Event::UploadsDone {
                op,
                result: UploadResult::Done { held: 0 },
            }));
        }
        all
    };
    // The first round's tail is the backstop's first probe (no S3 tail
    // yet): one GET.
    let out = run_round(&mut sub);
    let runs: Vec<_> = s3_ops(&out)
        .into_iter()
        .filter_map(|(op, r)| match r {
            S3Op::SegmentRun { width, .. } => Some((op, *width)),
            _ => None,
        })
        .collect();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        runs[0].1, 1,
        "a caught-up stream's backstop probe is one GET"
    );
    sub.step(Event::S3 {
        op: runs[0].0,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    // Within the backstop period: no S3 tail at all.
    sub.advance(1_000);
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LogStream {
            req,
            n: 1,
            epoch: 1,
            head: 0,
            segment: None,
        },
    });
    let out = run_round(&mut sub);
    assert!(
        !s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::SegmentRun { .. })),
        "a round tailed S3 while the stream covered it: {out:?}"
    );
    assert_eq!(sub.core.stats.stream_tail_skips, 1);
}

/// A holder that lets the lease go ends every subscription; a node that
/// does not hold refuses one.
#[test]
fn serving_follows_the_lease() {
    let (mut holder, _sub, req) = stream_pair();
    holder.core.lease.released();
    let out = holder.step(Event::Peers { links: Vec::new() });
    assert!(sends(&out).iter().any(|(to, m)| *to == 2
        && matches!(m, PeerMsg::LogStreamEnd { req: r, refused: false } if *r == req)));
    let out = holder.step(Event::Peer {
        from: 3,
        msg: PeerMsg::LogSubscribe {
            req: OpId(9),
            from: 1,
        },
    });
    assert!(sends(&out)
        .iter()
        .any(|(to, m)| *to == 3 && matches!(m, PeerMsg::LogStreamEnd { refused: true, .. })));
}

/// With one node there is nobody to stream from or to.
#[test]
fn a_lone_node_never_subscribes() {
    let mut solo = Harness::with(1, true);
    solo.hold(1, None);
    let out = solo.step(Event::Peers { links: Vec::new() });
    assert!(sends(&out).is_empty());
    solo.core.lease.released();
    solo.core.lease.cached_holder = Some(1);
    let out = solo.step(Event::Peers { links: Vec::new() });
    assert!(sends(&out).is_empty(), "subscribed to itself: {out:?}");
}

// ---- plan 30 §M8: ReadIndex, read delegations, recalls ----

mod cto {
    use super::*;
    use crate::action::{ControlOk, ReadAnswer};
    use crate::event::{Control, ReadGrantMsg, ReadIndexOutcome};
    use constellation_fs_core::Ino;
    use constellation_meta::Position;

    fn setattr(ino: Ino) -> MutateOp {
        MutateOp::Setattr {
            ino,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
        }
    }

    /// A holder with a file `f` (created locally, unshipped).
    fn holder_with_file() -> (Harness, Ino) {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let op = h.create("f");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        (h, ino)
    }

    fn ask(h: &mut Harness, from: NodeId, req: u64, ino: Ino) -> ReadIndexOutcome {
        let out = h.step(Event::Peer {
            from,
            msg: PeerMsg::ReadIndex {
                req: OpId(req),
                ino,
                dir: false,
                name: None,
            },
        });
        match sends(&out).as_slice() {
            [(to, PeerMsg::ReadIndexReply { req: r, outcome })] if *to == from => {
                assert_eq!(*r, OpId(req));
                outcome.clone()
            }
            other => panic!("expected one ReadIndexReply: {other:?}"),
        }
    }

    fn grant_of(outcome: &ReadIndexOutcome) -> ReadGrantMsg {
        match outcome {
            ReadIndexOutcome::Ok { grant: Some(g), .. } => *g,
            other => panic!("expected a grant: {other:?}"),
        }
    }

    fn forward(h: &mut Harness, from: NodeId, req: u64, seq: u64, op: MutateOp) -> Vec<Action> {
        h.step(Event::Peer {
            from,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid: Rid {
                    node: from,
                    incarnation: 1,
                    seq,
                },
                op,
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
            },
        })
    }

    fn mutate_replies(out: &[Action]) -> Vec<(NodeId, OpId, MutateOutcome)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::MutateReply { req, outcome, .. } => Some((to, *req, outcome.clone())),
                _ => None,
            })
            .collect()
    }

    fn recalls(out: &[Action]) -> Vec<(NodeId, OpId, Ino, u64)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::DelegationRecall { req, ino, grant } => Some((to, *req, *ino, *grant)),
                _ => None,
            })
            .collect()
    }

    fn timer_of(out: &[Action], kind: TimerKind) -> TimerId {
        let t = timers(out, kind);
        assert_eq!(t.len(), 1, "expected one {kind:?} timer: {out:?}");
        t[0]
    }

    /// The position covers the unshipped create (the reader waits for
    /// it); the grant is capped by nothing here (10 s of lease left); a
    /// third node's forwarded write on the file is not answered until the
    /// delegate acks the recall.
    #[test]
    fn a_grant_is_recalled_before_a_forwarded_writes_reply() {
        let (mut h, ino) = holder_with_file();
        let outcome = ask(&mut h, 2, 7, ino);
        let ReadIndexOutcome::Ok { position, .. } = &outcome else {
            panic!("{outcome:?}")
        };
        assert!(
            position.pending.is_some(),
            "the unshipped create is part of it"
        );
        let g = grant_of(&outcome);
        assert_eq!(g.ttl_ms, 5_000);
        assert_eq!(h.meta.read_delegations().live_grants(), 1);
        let out = forward(&mut h, 3, 9, 1, setattr(ino));
        assert!(mutate_replies(&out).is_empty(), "the reply waits: {out:?}");
        let rs = recalls(&out);
        let [(2, recall, rino, grant)] = rs.as_slice() else {
            panic!("expected one recall to node 2: {out:?}")
        };
        assert_eq!((*rino, *grant), (ino, g.id));
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::DelegationRecalled { req: *recall },
        });
        let [(3, OpId(9), MutateOutcome::Accepted { .. })] = mutate_replies(&out).as_slice() else {
            panic!("expected the held reply: {out:?}")
        };
        assert_eq!(h.core.stats.recalls_acked, 1);
        assert_eq!(h.meta.read_delegations().live_grants(), 0);
    }

    /// The writer's own delegation is not recalled (read-your-writes
    /// covers its reads); unrelated inodes recall nothing.
    #[test]
    fn the_writers_own_grant_and_unrelated_inodes_recall_nothing() {
        let (mut h, ino) = holder_with_file();
        grant_of(&ask(&mut h, 2, 7, ino));
        let out = forward(&mut h, 2, 9, 1, setattr(ino));
        assert_eq!(mutate_replies(&out).len(), 1);
        assert!(recalls(&out).is_empty());
        let other = h.create("g");
        let out = forward(&mut h, 3, 10, 1, other);
        assert_eq!(mutate_replies(&out).len(), 1);
        assert!(recalls(&out).is_empty());
    }

    /// A delegate that never answers is outwaited: TTL + margin by the
    /// holder's clock. Meanwhile the requester's RPC is answered `Held`
    /// before it would time out, and its retry re-attaches.
    #[test]
    fn an_unanswered_recall_is_outwaited_and_a_long_wait_answers_held() {
        let (mut h, ino) = holder_with_file();
        let granted_at = h.now;
        grant_of(&ask(&mut h, 2, 7, ino));
        let out = forward(&mut h, 3, 9, 1, setattr(ino));
        let held = timer_of(&out, TimerKind::HeldReply);
        let expiry = timer_of(&out, TimerKind::GrantExpiry);
        assert_eq!(
            h.core.timer_at(expiry),
            Some(granted_at.plus(5_000 + 1_000)),
            "live until granted + ttl + margin"
        );
        h.advance(250);
        let out = h.step(Event::Timer { id: held });
        let [(3, OpId(9), MutateOutcome::Held { .. })] = mutate_replies(&out).as_slice() else {
            panic!("expected Held: {out:?}")
        };
        // The requester retries the same rid: re-attached, no answer yet.
        let out = forward(&mut h, 3, 10, 1, setattr(ino));
        assert!(mutate_replies(&out).is_empty(), "{out:?}");
        h.now = granted_at.plus(6_000);
        let out = h.step(Event::Timer { id: expiry });
        let [(3, OpId(10), MutateOutcome::Accepted { .. })] = mutate_replies(&out).as_slice()
        else {
            panic!("expected the reply to the retry: {out:?}")
        };
        assert_eq!(h.core.stats.recalls_expired, 1);
        assert_eq!(h.core.stats.held_replies, 1);
    }

    /// The sequencer's own writes (the core's local execution) recall too.
    #[test]
    fn a_local_write_recalls_before_its_reply() {
        let (mut h, ino) = holder_with_file();
        grant_of(&ask(&mut h, 2, 7, ino));
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            rid,
            op: setattr(ino),
            policy: Policy::Client,
        });
        assert!(replies(&out).is_empty(), "{out:?}");
        let rs = recalls(&out);
        let [(2, recall, _, _)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::DelegationRecalled { req: *recall },
        });
        assert_eq!(replies(&out).len(), 1);
    }

    /// The FUSE fast path's `Control::Recall` is answered once recalled.
    #[test]
    fn a_fuse_write_recall_control_waits_for_the_ack() {
        let (mut h, ino) = holder_with_file();
        grant_of(&ask(&mut h, 2, 7, ino));
        let out = h.step(Event::Control {
            op: OpId(1 << 50),
            req: Control::Recall { inos: vec![ino] },
        });
        assert!(!out.iter().any(|a| matches!(a, Action::ControlDone { .. })));
        let rs = recalls(&out);
        let [(2, recall, _, _)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::DelegationRecalled { req: *recall },
        });
        assert!(out
            .iter()
            .any(|a| matches!(a, Action::ControlDone { op, .. } if *op == OpId(1 << 50))));
    }

    /// A grant never outlives the lease backing it (minus the margin).
    #[test]
    fn a_grant_is_capped_by_the_lease() {
        let (mut h, ino) = holder_with_file();
        h.advance(7_000); // 3 s of the 10 s lease left
        let g = grant_of(&ask(&mut h, 2, 7, ino));
        assert_eq!(g.ttl_ms, 2_000);
        h.advance(2_500); // 500 ms left: under the margin, not usable
        assert!(matches!(
            ask(&mut h, 2, 8, ino),
            ReadIndexOutcome::NotHolder { .. }
        ));
    }

    /// A release (here a flush) recalls every grant first, and no new
    /// grant is made meanwhile.
    #[test]
    fn a_release_waits_for_its_grants() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        grant_of(&ask(&mut h, 2, 7, ROOT_INO));
        let out = h.step(Event::Control {
            op: OpId(1 << 50),
            req: Control::Flush,
        });
        let upload = out
            .iter()
            .find_map(|a| match a {
                Action::UploadDirtyChunks { op, .. } => Some(*op),
                _ => None,
            })
            .expect("the flush uploads first");
        let mut out = h.step(Event::UploadsDone {
            op: upload,
            result: UploadResult::Done { held: 0 },
        });
        if let Some(op) = out.iter().find_map(|a| match a {
            Action::Publish { op, .. } => Some(*op),
            _ => None,
        }) {
            out = h.step(Event::PublishDone { op, ok: true });
        }
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::LeaseSwap { .. })),
            "no release CAS before the recall: {out:?}"
        );
        let rs = recalls(&out);
        let [(2, recall, _, _)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        assert!(matches!(
            ask(&mut h, 3, 8, ROOT_INO),
            ReadIndexOutcome::Busy
        ));
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::DelegationRecalled { req: *recall },
        });
        assert!(s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::LeaseSwap { lease, .. } if lease.released)));
    }

    /// A restart inside the lease: the previous incarnation's grants are
    /// unknown, so every acknowledgement waits the persisted horizon out.
    #[test]
    fn a_restart_quarantines_acks_until_the_grant_horizon() {
        let (h0, ino) = holder_with_file();
        let now = h0.now;
        h0.meta.note_grant_horizon(now.0 + 3_000).unwrap();
        let meta = h0.meta;
        let mut core = Core::new(h0.core.cfg.clone());
        let mut out = Vec::new();
        core.start(now, &meta, &mut out);
        let quarantine = timer_of(&out, TimerKind::GrantQuarantine);
        let mut h = Harness { core, meta, now };
        h.hold(1, None);
        let out = forward(&mut h, 3, 9, 1, setattr(ino));
        assert!(mutate_replies(&out).is_empty(), "{out:?}");
        h.now = now.plus(4_000);
        let out = h.step(Event::Timer { id: quarantine });
        assert_eq!(mutate_replies(&out).len(), 1, "{out:?}");
    }

    /// A lone strict sequencer (it keeps a kernel cache TTL): the first
    /// forwarded op flips the latch and its reply waits the drain out;
    /// later ones are answered at once.
    #[test]
    fn the_first_foreign_op_drains_a_lone_strict_nodes_kernel_cache() {
        let (mut h, ino) = holder_with_file();
        h.core.cfg.kernel_cache_ttl_ms = 1_000;
        assert!(h.meta.read_delegations().is_alone());
        let flipped_at = h.now;
        let out = forward(&mut h, 3, 9, 1, setattr(ino));
        assert!(mutate_replies(&out).is_empty(), "{out:?}");
        assert!(!h.meta.read_delegations().is_alone());
        let drain = timer_of(&out, TimerKind::GrantQuarantine);
        assert_eq!(h.core.timer_at(drain), Some(flipped_at.plus(1_000)));
        h.advance(1_000);
        let out = h.step(Event::Timer { id: drain });
        assert_eq!(mutate_replies(&out).len(), 1, "{out:?}");
        let out = forward(&mut h, 3, 10, 2, setattr(ino));
        assert_eq!(mutate_replies(&out).len(), 1, "no second drain: {out:?}");
    }

    // ---- the reader ----

    fn reader() -> Harness {
        let mut r = Harness::new(2);
        r.core.lease.cached_holder = Some(1);
        r
    }

    fn read_control(r: &mut Harness, op: u64, ino: Ino) -> OpId {
        let out = r.step(Event::Control {
            op: OpId(op),
            req: Control::ReadIndex {
                ino,
                dir: false,
                name: None,
            },
        });
        match sends(&out).as_slice() {
            [(1, PeerMsg::ReadIndex { req, .. })] => *req,
            other => panic!("expected a ReadIndex to node 1: {other:?}"),
        }
    }

    fn answer(out: &[Action], op: u64) -> ReadAnswer {
        out.iter()
            .find_map(|a| match a {
                Action::ControlDone {
                    op: o,
                    result: Ok(ControlOk::ReadIndex(answer)),
                } if *o == OpId(op) => Some(*answer),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no answer for {op}: {out:?}"))
    }

    fn granted(id: u64) -> ReadIndexOutcome {
        ReadIndexOutcome::Ok {
            position: Position {
                seq: 3,
                pending: None,
                streams: Default::default(),
            },
            grant: Some(ReadGrantMsg {
                id,
                ttl_ms: 5_000,
                epoch: 1,
            }),
        }
    }

    #[test]
    fn the_holder_reads_locally() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::ReadIndex {
                ino: ROOT_INO,
                dir: true,
                name: Some("x".into()),
            },
        });
        assert_eq!(answer(&out, 5), ReadAnswer::Holder);
        assert!(sends(&out).is_empty());
    }

    /// The reader installs the grant (honoured until sent + ttl − margin),
    /// a recall removes it, and a recall that overtakes the reply carrying
    /// a grant voids that grant (the position still answers the read).
    #[test]
    fn a_strict_read_installs_its_grant_and_an_overtaking_recall_voids_it() {
        let mut r = reader();
        let sent = r.now;
        let req = read_control(&mut r, 50, 42);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::ReadIndexReply {
                req,
                outcome: granted(1),
            },
        });
        assert!(matches!(
            answer(&out, 50),
            ReadAnswer::Position {
                delegated: true,
                ..
            }
        ));
        let d = r.meta.read_delegations();
        assert!(d.valid(42, sent.0 + 3_999).is_some());
        assert!(d.valid(42, sent.0 + 4_000).is_none(), "sent + ttl − margin");
        // A fresh grant, then a recall.
        let req = read_control(&mut r, 51, 42);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::ReadIndexReply {
                req,
                outcome: granted(2),
            },
        });
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::DelegationRecall {
                req: OpId(77),
                ino: 42,
                grant: 2,
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::DelegationRecalled { req: OpId(77) })]
        ));
        assert!(r.meta.read_delegations().valid(42, r.now.0).is_none());
        // Overtaken: the recall of grant 3 arrives before its reply.
        let req = read_control(&mut r, 52, 42);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::DelegationRecall {
                req: OpId(78),
                ino: 42,
                grant: 3,
            },
        });
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::ReadIndexReply {
                req,
                outcome: granted(3),
            },
        });
        assert!(matches!(
            answer(&out, 52),
            ReadAnswer::Position {
                delegated: false,
                ..
            }
        ));
        assert!(r.meta.read_delegations().valid(42, r.now.0).is_none());
        assert_eq!(r.meta.read_delegations().stats().delegations_raced, 1);
    }

    /// No answer within the budget: degraded, not an error.
    #[test]
    fn an_unanswered_read_index_degrades() {
        let mut r = reader();
        let out = r.step(Event::Control {
            op: OpId(60),
            req: Control::ReadIndex {
                ino: 42,
                dir: false,
                name: None,
            },
        });
        let deadline = timer_of(&out, TimerKind::ReadIndexDeadline);
        r.advance(2_000);
        let out = r.step(Event::Timer { id: deadline });
        assert_eq!(answer(&out, 60), ReadAnswer::Degraded);
    }
}

/// Plan 30 §M9 + §M10: the claim rule for continuation epochs and the
/// member backup's watch.
mod epoch_rules {
    use super::*;
    use constellation_store_s3::AckPolicy;

    fn epoch(open: bool, active: bool, members: Vec<NodeId>) -> Event {
        Event::Control {
            op: OpId(900),
            req: Control::Epoch {
                open,
                active,
                frozen: false,
                flushing: false,
                base: 0,
                // Node 1's lease as `Harness::hold(1, ..)` makes it at
                // the harness's start time.
                carrier: (active && !members.is_empty()).then_some(crate::event::Carrier {
                    node: 1,
                    epoch: 1,
                    expires_unix_ms: 1_010_000,
                }),
                stale_below: 0,
                members,
            },
        }
    }

    fn holder_with(policy: AckPolicy, backups: Vec<NodeId>) -> Harness {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let lease = &mut h.core.lease.held.as_mut().expect("held").0;
        lease.ack_policy = policy;
        lease.backups = backups;
        h
    }

    /// (a) An `ack=s3` lease is never carried: the epoch activates
    /// without this node's authority, and the S3 lease stays what it
    /// was (its acknowledgements wait for S3).
    #[test]
    fn an_ack_s3_lease_is_not_carried_into_an_epoch() {
        let mut h = holder_with(AckPolicy::S3, Vec::new());
        h.step(epoch(true, true, vec![1, 2]));
        assert!(!h.core.lease.epoch_held(), "an S3-policy lease was carried");
        assert!(h.core.lease.held.is_some(), "the S3 lease itself is kept");
        assert_eq!(h.core.stats.epoch_carry_refused, 1);
        assert_eq!(
            h.core.lease.ack_policy(),
            AckPolicy::S3,
            "still acknowledging on S3"
        );
    }

    /// (a) A `Backup` lease is carried only with every listed backup a
    /// member; `Local` always.
    #[test]
    fn a_backup_lease_is_carried_only_with_every_backup_a_member() {
        let mut h = holder_with(AckPolicy::Backup, vec![3]);
        h.step(epoch(true, true, vec![1, 2]));
        assert!(!h.core.lease.epoch_held(), "backup 3 is outside the epoch");
        assert_eq!(h.core.stats.epoch_carry_refused, 1);
        h.step(epoch(false, false, Vec::new()));

        let mut h = holder_with(AckPolicy::Backup, vec![3]);
        h.step(epoch(true, true, vec![1, 2, 3]));
        assert!(h.core.lease.epoch_held(), "every backup is a member");
        assert_eq!(
            h.core.lease.ack_policy(),
            AckPolicy::Local,
            "an epoch acknowledges locally"
        );
        // Plan 30 §M10 (rebase onto M9 round 2): and nothing gates it — not
        // the held lease's backups, nor a candidate being brought up.
        h.core.ack.candidate = Some(3);
        assert!(h.core.lease.backups().is_empty());
        assert_eq!(h.core.durable_jseq(), u64::MAX);
        assert!(
            !h.core.ack_gated(),
            "the FUSE fast path is open in an epoch"
        );
        let rid = h.rid(1);
        let op = h.create("in-epoch");
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
        });
        assert!(
            matches!(
                replies(&out).as_slice(),
                [(_, ClientReply::Outcome(MutateOutcome::Accepted { .. }))]
            ),
            "acknowledged at once: {out:?}"
        );

        let mut h = holder_with(AckPolicy::Local, Vec::new());
        h.step(epoch(true, true, vec![1, 2]));
        assert!(h.core.lease.epoch_held());
    }

    /// (a) With a reconfiguration CAS in flight (its outcome unknown) the
    /// lease is not carried either.
    #[test]
    fn a_lease_with_a_reconfiguration_in_flight_is_not_carried() {
        let mut h = holder_with(AckPolicy::Backup, vec![2]);
        h.core.ack.reconfig_wanted = Some(vec![2, 3]);
        h.step(epoch(true, true, vec![1, 2, 3]));
        assert!(!h.core.lease.epoch_held());
    }

    /// (b) A member backup runs no seal watch while its epoch is open:
    /// the holder's silence past `backup_takeover_ms` neither seals nor
    /// reads the lease. Once the epoch closes the watch resumes with a
    /// fresh window, and silence then seals.
    #[test]
    fn a_member_backup_does_not_seal_while_its_epoch_is_open() {
        let mut h = Harness::new(2);
        // Node 1 streams a heartbeat: this node backs it at epoch 1.
        let out = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(7),
                epoch: 1,
                holder: 1,
                config_version: 2,
                from: 1,
                txs: Vec::new(),
                through: 0,
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::BackupAck { sealed: false, .. })]
        ));
        let watch = timers(&out, TimerKind::BackupWatch);
        assert_eq!(watch.len(), 1, "the watch is armed: {out:?}");
        // The epoch opens; the holder falls silent.
        h.step(epoch(true, true, vec![1, 2]));
        h.advance(5_000);
        let out = h.step(Event::Timer { id: watch[0] });
        assert_eq!(h.core.bk.sealed, 0, "sealed inside an open epoch");
        assert!(
            s3_ops(&out).is_empty(),
            "read the lease inside an open epoch: {out:?}"
        );
        assert!(timers(&out, TimerKind::BackupWatch).is_empty());
        // The epoch closes: the watch resumes with a full window.
        let out = h.step(epoch(false, false, Vec::new()));
        let watch = timers(&out, TimerKind::BackupWatch);
        assert_eq!(watch.len(), 1, "the watch resumes: {out:?}");
        let out = h.step(Event::Timer { id: watch[0] });
        assert_eq!(h.core.bk.sealed, 0, "sealed before a full window passed");
        let watch = timers(&out, TimerKind::BackupWatch);
        assert_eq!(watch.len(), 1);
        h.advance(2_000);
        let out = h.step(Event::Timer { id: watch[0] });
        assert_eq!(h.core.bk.sealed, 1, "silence after the close seals");
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
    }
}

/// Plan 30 §M9: a definitive refusal of a forwarded op is an outcome.
mod refusal_outcomes {
    use super::*;

    fn forward(h: &mut Harness, from: NodeId, req: u64, seq: u64, op: MutateOp) -> Vec<Action> {
        h.step(Event::Peer {
            from,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid: Rid {
                    node: from,
                    incarnation: 1,
                    seq,
                },
                op,
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
            },
        })
    }

    fn outcome_of(out: &[Action], req: u64) -> MutateOutcome {
        out.iter()
            .find_map(|a| match a {
                Action::Send {
                    msg:
                        PeerMsg::MutateReply {
                            req: r, outcome, ..
                        },
                    ..
                } if *r == OpId(req) => Some(outcome.clone()),
                _ => None,
            })
            .expect("a reply")
    }

    /// Node 2's unlink of a name that does not exist is refused ENOENT
    /// and the refusal journaled; the name is then created; a retry of
    /// the same rid (a lost reply, a replay by rid, an inbox drain)
    /// answers ENOENT again instead of unlinking the new file.
    #[test]
    fn a_refused_forward_is_journaled_and_a_retry_dedups_to_the_same_errno() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let unlink = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "f".into(),
        };
        let out = forward(&mut h, 2, 1, 1, unlink.clone());
        assert_eq!(outcome_of(&out, 1), MutateOutcome::Errno(libc::ENOENT));
        assert_eq!(h.core.stats.refusals_journaled, 1);
        assert!(
            matches!(
                h.meta.completed_outcome(Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 1,
                }),
                Ok(Some(constellation_meta::CompletedOutcome::Refused {
                    errno: libc::ENOENT
                }))
            ),
            "the refusal is a completion"
        );
        // The state changes: `f` now exists.
        let create = h.create("f");
        let out = forward(&mut h, 3, 2, 1, create);
        assert!(matches!(
            outcome_of(&out, 2),
            MutateOutcome::Accepted { .. }
        ));
        // The same rid again: the recorded outcome, not a fresh unlink.
        let out = forward(&mut h, 2, 3, 1, unlink);
        assert_eq!(outcome_of(&out, 3), MutateOutcome::Errno(libc::ENOENT));
        assert_eq!(h.core.stats.forward_dedup_hits, 1);
        assert!(
            h.meta.lookup(ROOT_INO, "f").unwrap().is_some(),
            "the retry unlinked the new file"
        );
    }
}

/// Plan 30 §M9 round 2: pipelined appends and contiguous acknowledgements.
mod pipelined_appends {
    use super::*;

    fn appends(out: &[Action]) -> Vec<(OpId, u64, usize)> {
        out.iter()
            .filter_map(|a| match a {
                Action::Send {
                    to: 2,
                    msg: PeerMsg::BackupAppend { req, from, txs, .. },
                } => Some((*req, *from, txs.len())),
                _ => None,
            })
            .collect()
    }

    /// A holder with node 2 as its candidate backup.
    fn holder_with_candidate() -> Harness {
        let mut h = Harness::new(1);
        h.hold(1, None);
        h.core.ack.candidate = Some(2);
        h.core.ack.peers.insert(
            2,
            crate::core::backup::BackupPeer {
                acked: 0,
                sent_through: 0,
                inflight: Default::default(),
                last_sent: Ms(0),
                last_progress: h.now,
                committed: false,
            },
        );
        h
    }

    fn journal(h: &Harness, name: &str, seq: u64) -> u64 {
        constellation_meta::execute_mutate(&h.meta, &h.create(name), Some(h.rid(seq))).unwrap();
        h.meta.journal_tip().unwrap()
    }

    fn journaled(h: &mut Harness, op: u64) -> Vec<Action> {
        h.step(Event::Control {
            op: OpId(op),
            req: Control::Journaled,
        })
    }

    fn ack(h: &mut Harness, req: OpId, acked: u64) -> Vec<Action> {
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::BackupAck {
                req,
                epoch: 1,
                acked,
                sealed: false,
            },
        })
    }

    /// Rows journaled while an append is in flight go out at once in a
    /// second append (up to the pipeline depth), and a cumulative ack
    /// for the second retires both.
    #[test]
    fn appends_pipeline_and_acks_are_cumulative() {
        let mut h = holder_with_candidate();
        let tip1 = journal(&h, "a", 1);
        let out = journaled(&mut h, 900);
        let first = appends(&out);
        assert_eq!(first.len(), 1, "{out:?}");
        assert_eq!(first[0].1, 1);
        let tip2 = journal(&h, "b", 2);
        let out = journaled(&mut h, 901);
        let second = appends(&out);
        assert_eq!(
            second.len(),
            1,
            "a second append while the first is in flight: {out:?}"
        );
        assert_eq!(second[0].1, tip1 + 1);
        assert_eq!(h.core.ack.peers[&2].inflight.len(), 2);
        // The second batch's ack covers both.
        ack(&mut h, second[0].0, tip2);
        let p = &h.core.ack.peers[&2];
        assert_eq!(p.acked, tip2);
        assert!(p.inflight.is_empty(), "{:?}", p.inflight);
        // The late first ack changes nothing.
        ack(&mut h, first[0].0, tip1);
        assert_eq!(h.core.ack.peers[&2].acked, tip2);
    }

    /// The oldest append answered short (its rows did not land): the
    /// holder resends from what the backup holds.
    #[test]
    fn a_short_ack_for_the_oldest_append_resends_from_the_hold() {
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        let first = appends(&journaled(&mut h, 900));
        let tip2 = journal(&h, "b", 2);
        let second = appends(&journaled(&mut h, 901));
        // The second batch arrived first at the backup: acked 0.
        ack(&mut h, second[0].0, 0);
        assert_eq!(h.core.ack.peers[&2].inflight.len(), 1);
        // The first batch was lost on the wire.
        let out = h.step(Event::PeerFailed {
            req: first[0].0,
            to: 2,
            outage: false,
        });
        let resend = appends(&out);
        assert_eq!(resend.len(), 1, "{out:?}");
        assert_eq!(resend[0].1, 1, "resent from the backup's hold");
        assert_eq!(resend[0].2, 2, "both transactions again");
        ack(&mut h, resend[0].0, tip2);
        assert_eq!(h.core.ack.peers[&2].acked, tip2);
    }

    /// Round 2: rows shipped and dropped out of order behind a held-back
    /// transaction leave holes in the journal; the holder streams them
    /// as empty transactions up to the tip, the backup's hold steps over
    /// them, and the candidate counts as caught up.
    #[test]
    fn holes_in_the_journal_stream_as_empty_transactions_up_to_the_tip() {
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        journal(&h, "b", 2);
        let tip = journal(&h, "c", 3);
        let txs = h.meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 3);
        // The second and third transactions shipped out of order (the
        // first is held back): their rows are gone, the tip stays.
        let rows: Vec<u64> = (txs[1].first..=txs[2].last).collect();
        h.meta.ack_journal_rows_at(&rows, 1).unwrap();
        assert_eq!(h.meta.journal_tip().unwrap(), tip);
        let out = journaled(&mut h, 900);
        let sent = appends(&out);
        assert_eq!(sent.len(), 1, "{out:?}");
        let batch = out
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    to: 2,
                    msg: PeerMsg::BackupAppend { txs, .. },
                } => Some(txs.clone()),
                _ => None,
            })
            .expect("the append");
        assert_eq!(batch.len(), 2, "{batch:?}");
        assert_eq!(batch[0], txs[0]);
        assert_eq!((batch[1].first, batch[1].last), (txs[1].first, tip));
        assert!(batch[1].records.is_empty(), "a hole to the tip");
        // The backup persists the hole and acknowledges through the tip.
        let mut b = Harness::new(2);
        let out = b.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(7),
                epoch: 1,
                holder: 1,
                config_version: 2,
                from: 1,
                txs: batch,
                through: 0,
            },
        });
        let acked = out
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    msg: PeerMsg::BackupAck { acked, .. },
                    ..
                } => Some(*acked),
                _ => None,
            })
            .expect("an ack");
        assert_eq!(acked, tip);
        assert_eq!(b.meta.backup_acked(1).unwrap(), tip);
        // Its ack makes the candidate caught up: the holder wants it
        // listed.
        ack(&mut h, sent[0].0, tip);
        h.step(Event::Timer {
            id: h.core.ack.tick_timer.expect("a backup tick armed"),
        });
        assert!(
            h.core.ack.reconfig.is_some() || h.core.ack.reconfig_wanted == Some(vec![2]),
            "caught up: the reconfiguration is wanted or in flight"
        );
    }

    /// Round 2: a backup answering short for the same rows over and over
    /// makes no progress; the holder resends once per short answer (not
    /// once per append in flight) and the timeout rule drops it.
    #[test]
    fn a_stuck_backup_is_resent_to_once_and_then_dropped() {
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        let first = appends(&journaled(&mut h, 900));
        journal(&h, "b", 2);
        let second = appends(&journaled(&mut h, 901));
        assert_eq!(h.core.ack.peers[&2].inflight.len(), 2);
        // The oldest comes back short: one resend, the other in-flight
        // append forgotten (its short ack would resend the same rows).
        let out = ack(&mut h, first[0].0, 0);
        let resend = appends(&out);
        assert_eq!(resend.len(), 1, "{out:?}");
        assert_eq!(resend[0].1, 1);
        assert_eq!(h.core.ack.peers[&2].inflight.len(), 1);
        assert!(appends(&ack(&mut h, second[0].0, 0)).is_empty());
        // Short answers are not progress: the candidate times out.
        let started = h.now;
        let timeout = h.core.cfg.backup_ack_timeout_ms as i64;
        let mut req = resend[0].0;
        while h.core.ack.candidate.is_some() && h.now.since(started) < 4 * timeout {
            h.advance(50);
            let out = ack(&mut h, req, 0);
            if let Some(r) = appends(&out).first() {
                req = r.0;
            }
            if let Some(id) = h.core.ack.tick_timer {
                h.step(Event::Timer { id });
            }
        }
        assert!(h.core.ack.candidate.is_none(), "dropped");
        assert!(h.core.stats.backup_ack_timeouts >= 1);
    }

    /// The backup side: a batch that overtook an earlier one is
    /// persisted but acknowledged only once the gap closes, and the ack
    /// then names the whole contiguous hold.
    #[test]
    fn a_backup_acknowledges_its_contiguous_hold() {
        let holder = Harness::new(1);
        holder.meta.set_holder_epoch(1);
        journal(&holder, "a", 1);
        journal(&holder, "b", 2);
        let txs = holder.meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 2);
        let mut b = Harness::new(2);
        let append = |b: &mut Harness, req: u64, tx: &constellation_meta::BackupTx| {
            let out = b.step(Event::Peer {
                from: 1,
                msg: PeerMsg::BackupAppend {
                    req: OpId(req),
                    epoch: 1,
                    holder: 1,
                    config_version: 2,
                    from: tx.first,
                    txs: vec![tx.clone()],
                    through: 0,
                },
            });
            out.iter()
                .find_map(|a| match a {
                    Action::Send {
                        msg:
                            PeerMsg::BackupAck {
                                req: r,
                                acked,
                                sealed,
                                ..
                            },
                        ..
                    } if *r == OpId(req) => Some((*acked, *sealed)),
                    _ => None,
                })
                .expect("an ack")
        };
        assert_eq!(
            append(&mut b, 1, &txs[1]),
            (0, false),
            "ahead of a gap: not acknowledged"
        );
        assert_eq!(
            append(&mut b, 2, &txs[0]),
            (txs[1].last, false),
            "the gap closed: both"
        );
        assert_eq!(b.meta.backup_acked(1).unwrap(), txs[1].last);
    }
}

/// Plan 30 §M10: heartbeat promises, the TTL takeover's promise check,
/// claim resolution at activation, the flush exemption, retirement.
mod m10 {
    use super::*;
    use crate::event::Carrier;
    use constellation_store_s3::heartbeat::Promise;

    fn lease_of(holder: NodeId, epoch: Epoch, expires: i64) -> Lease {
        Lease {
            v: 1,
            partition: "p0".into(),
            holder,
            epoch,
            expires_unix_ms: expires,
            released: false,
            wanted_by: Vec::new(),
            backups: Vec::new(),
            config_version: 1,
            ack_policy: constellation_store_s3::AckPolicy::Local,
            granted_delegations: false,
            retired: Vec::new(),
        }
    }

    fn flex(slack: u32) -> Harness {
        let mut h = Harness::new(1);
        h.core.cfg.epoch_slack = slack;
        h.step(Event::Roster {
            write_eligible: vec![1, 2, 3],
        });
        h
    }

    /// Acquire, and answer the lease read with `lease`.
    fn acquire_against(h: &mut Harness, lease: Lease) -> Vec<Action> {
        let out = h.step(Event::Control {
            op: OpId(700),
            req: Control::Acquire,
        });
        let (get, req) = s3_ops(&out)[0];
        assert!(matches!(req, S3Op::LeaseGet), "{out:?}");
        h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((lease, tag())))),
        })
    }

    fn heartbeat_read(out: &[Action]) -> OpId {
        s3_ops(out)
            .into_iter()
            .find(|(_, r)| matches!(r, S3Op::HeartbeatRead))
            .map(|(op, _)| op)
            .unwrap_or_else(|| panic!("no heartbeat read: {out:?}"))
    }

    fn promise_requests(out: &[Action]) -> Vec<(NodeId, OpId)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::PromiseRequest { req, .. } => Some((to, *req)),
                _ => None,
            })
            .collect()
    }

    fn tails(out: &[Action]) -> usize {
        s3_ops(out)
            .iter()
            .filter(|(_, r)| matches!(r, S3Op::SegmentRun { .. }))
            .count()
    }

    #[test]
    fn a_ttl_takeover_proceeds_once_f_others_promise_past_the_expiry() {
        let mut h = flex(1);
        let expires = h.now.0 - 1_000;
        let out = acquire_against(&mut h, lease_of(2, 3, expires));
        let hb = heartbeat_read(&out);
        let asked = promise_requests(&out);
        assert_eq!(
            asked.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![2, 3],
            "every other roster node is asked"
        );
        assert_eq!(tails(&out), 0, "no tail before the check: {out:?}");
        // Node 3 promises past the expiry; the heartbeat read must still
        // land first (a larger advertised slack would show there).
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::PromiseReply {
                req: asked[1].1,
                until: Some(h.now.0 + 15_000),
                epoch_slack: 1,
            },
        });
        assert_eq!(tails(&out), 0);
        let out = h.step(Event::S3 {
            op: hb,
            result: S3Result::Heartbeats(Ok(Vec::new())),
        });
        assert_eq!(tails(&out), 1, "the takeover proceeds: {out:?}");
        assert_eq!(h.core.stats.promise_checks, 1);
        assert_eq!(h.core.stats.takeovers_refused_promises, 0);
    }

    #[test]
    fn a_ttl_takeover_without_enough_promises_is_refused() {
        let mut h = flex(1);
        let expires = h.now.0 - 1_000;
        let out = acquire_against(&mut h, lease_of(2, 3, expires));
        let hb = heartbeat_read(&out);
        let asked = promise_requests(&out);
        // A heartbeat that does not outlast the expiry does not count.
        h.step(Event::S3 {
            op: hb,
            result: S3Result::Heartbeats(Ok(vec![(3, Promise::new(3, expires, 1, 0))])),
        });
        // Node 2 is in an open epoch (refuses); node 3 is unreachable.
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::PromiseReply {
                req: asked[0].1,
                until: None,
                epoch_slack: 1,
            },
        });
        let out = h.step(Event::PeerFailed {
            req: asked[1].1,
            to: 3,
            outage: true,
        });
        assert_eq!(tails(&out), 0, "{out:?}");
        assert_eq!(h.core.stats.takeovers_refused_promises, 1);
        assert!(h.core.job().is_none(), "the acquisition finished");
    }

    #[test]
    fn a_larger_advertised_slack_is_honoured_at_f_0() {
        let mut h = flex(0);
        let expires = h.now.0 - 1_000;
        let out = acquire_against(&mut h, lease_of(2, 3, expires));
        let hb = heartbeat_read(&out);
        assert!(
            promise_requests(&out).is_empty(),
            "f = 0 asks nobody up front"
        );
        // Node 3 still runs with f = 1 (it has not seen it lowered), and
        // its promise ran out: the taker asks now, gets nothing, refuses.
        let out = h.step(Event::S3 {
            op: hb,
            result: S3Result::Heartbeats(Ok(vec![(3, Promise::new(3, 0, 1, 0))])),
        });
        let asked = promise_requests(&out);
        assert_eq!(asked.len(), 2, "{out:?}");
        let mut last = Vec::new();
        for (to, req) in asked {
            last = h.step(Event::PeerFailed {
                req,
                to,
                outage: true,
            });
        }
        assert_eq!(tails(&last), 0);
        assert_eq!(h.core.stats.takeovers_refused_promises, 1);

        // Nobody advertises a slack: today's takeover (one LIST more).
        let mut h = flex(0);
        let out = acquire_against(&mut h, lease_of(2, 3, expires));
        let hb = heartbeat_read(&out);
        let out = h.step(Event::S3 {
            op: hb,
            result: S3Result::Heartbeats(Ok(Vec::new())),
        });
        assert_eq!(tails(&out), 1, "{out:?}");
    }

    #[test]
    fn a_promise_request_persists_then_publishes_then_answers() {
        let mut h = flex(1);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::PromiseRequest {
                req: OpId(5),
                expires_unix_ms: h.now.0 - 10,
            },
        });
        let until = h.now.0 + 15_000;
        assert_eq!(h.meta.promise_issued().unwrap(), until, "persisted first");
        assert!(
            s3_ops(&out).iter().any(|(_, r)| matches!(
                r,
                S3Op::HeartbeatPut { promise } if promise.no_epoch_until_unix_ms == until
            )),
            "{out:?}"
        );
        assert!(matches!(
            sends(&out).as_slice(),
            [(2, PeerMsg::PromiseReply { until: Some(u), epoch_slack: 1, .. })] if *u == until
        ));
        // A join holds the gate: no promise is issued, the answer refuses.
        h.advance(20_000);
        assert!(h.meta.promise_join_begin(h.now.0).unwrap());
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::PromiseRequest {
                req: OpId(6),
                expires_unix_ms: h.now.0 - 10,
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(2, PeerMsg::PromiseReply { until: None, .. })]
        ));
        assert!(!s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::HeartbeatPut { .. })));
        assert_eq!(h.meta.promise_issued().unwrap(), until);
    }

    #[test]
    fn a_member_of_an_open_epoch_promises_nothing() {
        let mut h = flex(1);
        h.step(Event::Control {
            op: OpId(900),
            req: Control::Epoch {
                open: true,
                active: true,
                frozen: false,
                flushing: false,
                base: 0,
                members: vec![1, 2],
                carrier: None,
                stale_below: 0,
            },
        });
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::PromiseRequest {
                req: OpId(5),
                expires_unix_ms: 0,
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(3, PeerMsg::PromiseReply { until: None, .. })]
        ));
        assert_eq!(h.meta.promise_issued().unwrap(), 0);
        // An observed expiry does not make it promise either.
        h.core
            .lease
            .note_object(h.now, &lease_of(2, 3, h.now.0 - 1));
        let out = h.step(Event::Peers { links: Vec::new() });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
    }

    #[test]
    fn an_observed_unrenewed_expiry_is_promised_on_once() {
        // P2P down (the harness's peers are all unconnected): nobody can
        // ask, so the node promises on what it observes.
        let mut h = flex(1);
        h.core
            .lease
            .note_object(h.now, &lease_of(2, 3, h.now.0 - 1));
        let out = h.step(Event::Peers { links: Vec::new() });
        let puts = s3_ops(&out)
            .iter()
            .filter(|(_, r)| matches!(r, S3Op::HeartbeatPut { .. }))
            .count();
        assert_eq!(puts, 1, "{out:?}");
        let out = h.step(Event::Peers { links: Vec::new() });
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::HeartbeatPut { .. })),
            "covered already"
        );
        // P2P up: takers ask; an observed expiry alone promises nothing.
        let mut h = flex(1);
        h.step(Event::Peers {
            links: vec![crate::event::PeerLink {
                node: 2,
                connected: true,
                last_seen: None,
                rtt_ms: None,
                since: None,
            }],
        });
        h.core
            .lease
            .note_object(h.now, &lease_of(2, 3, h.now.0 - 1));
        let out = h.step(Event::Peers {
            links: vec![crate::event::PeerLink {
                node: 2,
                connected: true,
                last_seen: None,
                rtt_ms: None,
                since: None,
            }],
        });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
        // f = 0: never on its own.
        let mut h = flex(0);
        h.core
            .lease
            .note_object(h.now, &lease_of(2, 3, h.now.0 - 1));
        let out = h.step(Event::Peers { links: Vec::new() });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
    }

    #[test]
    fn the_flush_reclaim_of_the_carried_lease_needs_no_promises() {
        let mut h = flex(1);
        let carried = lease_of(2, 3, h.now.0 - 1_000);
        h.core.pr.carried = Some(Carrier {
            node: 2,
            epoch: 3,
            expires_unix_ms: carried.expires_unix_ms,
        });
        let out = acquire_against(&mut h, carried.clone());
        assert_eq!(tails(&out), 1, "straight to the takeover tail: {out:?}");
        assert_eq!(h.core.stats.promise_flush_exempt, 1);
        assert_eq!(h.core.stats.promise_checks, 0);
        // A renewed object (another expiry) is not the carried one.
        let mut h = flex(1);
        h.core.pr.carried = Some(Carrier {
            node: 2,
            epoch: 3,
            expires_unix_ms: carried.expires_unix_ms - 5,
        });
        let out = acquire_against(&mut h, carried);
        assert_eq!(tails(&out), 0);
        assert_eq!(h.core.stats.promise_checks, 1);
    }

    fn activate(h: &mut Harness, carrier: Option<Carrier>, stale_below: Epoch) {
        h.step(Event::Control {
            op: OpId(901),
            req: Control::Epoch {
                open: true,
                active: true,
                frozen: false,
                flushing: false,
                base: 0,
                members: vec![1, 2],
                carrier,
                stale_below,
            },
        });
    }

    #[test]
    fn a_stale_claim_is_deposed_at_activation_and_a_foreign_carrier_not_adopted() {
        // A member knows epoch 2 exists (it sealed ours, or took over):
        // our epoch-1 claim is stale.
        let mut h = Harness::new(1);
        h.hold(1, None);
        activate(&mut h, None, 2);
        assert!(!h.core.lease.epoch_held());
        assert!(h.core.lease.lost, "deposed");
        assert_eq!(h.core.stats.epoch_stale_claims, 1);

        // The carrier is another member's lease: ours is not carried.
        let mut h = Harness::new(1);
        h.hold(1, None);
        let expires = h.core.lease.held.as_ref().unwrap().0.expires_unix_ms;
        activate(
            &mut h,
            Some(Carrier {
                node: 2,
                epoch: 1,
                expires_unix_ms: expires,
            }),
            1,
        );
        assert!(!h.core.lease.epoch_held());

        // Exactly ours: carried.
        let mut h = Harness::new(1);
        h.hold(1, None);
        let expires = h.core.lease.held.as_ref().unwrap().0.expires_unix_ms;
        activate(
            &mut h,
            Some(Carrier {
                node: 1,
                epoch: 1,
                expires_unix_ms: expires,
            }),
            1,
        );
        assert!(h.core.lease.epoch_held());
        assert!(!h.core.lease.lost);
    }

    #[test]
    fn the_claim_view_reports_the_held_lease_and_the_known_epoch() {
        let mut h = Harness::new(1);
        h.hold(4, None);
        h.core.bk.sealed = 6;
        let view = h.core.epoch_claim_view(h.now);
        assert_eq!(view.held.as_ref().map(|l| l.epoch), Some(4));
        assert_eq!(view.known, 7, "the successor of the epoch it sealed");
        assert_eq!(view.claim(&[1, 2]).map(|c| c.2), Some(true));
    }

    #[test]
    fn a_retired_node_acquires_nothing() {
        let mut h = flex(1);
        h.hold(2, None);
        h.step(Event::Control {
            op: OpId(902),
            req: Control::Retire,
        });
        assert!(h.core.lease.lost, "its tenure ends");
        let out = h.step(Event::Control {
            op: OpId(700),
            req: Control::Acquire,
        });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::PromiseRequest {
                req: OpId(5),
                expires_unix_ms: 0,
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(2, PeerMsg::PromiseReply { until: None, .. })]
        ));
    }
}

mod claim_resolution {
    use crate::core::resolve_epoch_claims;

    #[test]
    fn the_highest_claim_wins_unless_a_member_knows_later() {
        // Node 1 claims epoch 1, node 3 epoch 2 (it took over by seal).
        let (carrier, stale) =
            resolve_epoch_claims(&[(1, Some((1, 10, true)), 1), (3, Some((2, 20, true)), 2)]);
        assert_eq!(
            carrier.map(|c| (c.node, c.epoch, c.expires_unix_ms)),
            Some((3, 2, 20))
        );
        assert_eq!(stale, 2, "node 1's claim is stale");
        // Node 3 restarted without its lease, but knows epoch 2 exists:
        // node 1's claim is stale and nothing is carried.
        let (carrier, stale) = resolve_epoch_claims(&[(1, Some((1, 10, true)), 1), (3, None, 2)]);
        assert_eq!(carrier, None);
        assert_eq!(stale, 2);
        // The claim rule refuses the top claim: carried by nobody, and it
        // is not stale (its holder keeps its S3 lease).
        let (carrier, stale) = resolve_epoch_claims(&[(1, Some((4, 10, false)), 4), (2, None, 3)]);
        assert_eq!(carrier, None);
        assert_eq!(stale, 4);
        // Nobody claims.
        assert_eq!(
            resolve_epoch_claims(&[(1, None, 0), (2, None, 0)]),
            (None, 0)
        );
    }
}

// ---- plan 30 §M11 phase 2b round 2: a held forward becomes the delegate's own ----

/// A delegate's own op is forwarded to the root before the delegate's
/// table carries the grant; the root answers `Held`. The grant installs
/// meanwhile: the backoff retry must execute the op here as the
/// delegate, not re-forward it (which the root would hold again, until
/// the forward deadline: `EIO` after 40 s — harness `cross-subtree-rename`
/// / `auto-placement` after a re-delegation).
#[test]
fn a_held_forward_executes_locally_once_the_delegation_installs() {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.cfg.forwarding = true;
    h.core.lease.cached_holder = Some(2);
    // The directory exists on this replica (applied from the log).
    let dir = h.meta.allocate_ino(ROOT_INO).unwrap();
    crate::replica::Replica::apply_segment(
        &h.meta,
        1,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Mkdir {
            parent: ROOT_INO,
            name: "d1".into(),
            ino: dir,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }],
    )
    .unwrap();
    let rid = h.rid(1);
    let op = MutateOp::Create {
        parent: dir,
        name: "f".into(),
        ino: h.meta.allocate_ino(dir).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
    });
    let Some((_, PeerMsg::MutateRequest { req, .. })) = sends(&out).first().copied() else {
        panic!(
            "no forward: {out:?} clients {:?}",
            h.core.clients().collect::<Vec<_>>()
        );
    };
    let req = *req;
    // The root holds it: the delegate's table lacks the grant still.
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Held { retry_ms: 50 },
            base: None,
            position: constellation_meta::Position::ZERO,
            gen: 0,
        },
    });
    let backoff = timers(&out, TimerKind::ForwardBackoff);
    assert_eq!(backoff.len(), 1, "{out:?}");
    // The grant lands from the log and installs here.
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Delegate {
            dir,
            node: 3,
            gen: 1,
            designated: true,
        }],
    )
    .unwrap();
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    assert_eq!(
        h.core.deleg_view().mine.len(),
        1,
        "{:?}",
        h.core.deleg_view()
    );
    // The retry executes here; nothing is forwarded again.
    h.advance(100);
    let out = h.step(Event::Timer { id: backoff[0] });
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, PeerMsg::MutateRequest { .. })),
        "re-forwarded: {out:?}"
    );
    let answered: Vec<Rid> = replies(&out).into_iter().map(|(r, _)| r).collect();
    assert_eq!(answered, vec![rid], "{out:?}");
    assert_eq!(h.core.stats.deleg_retry_executed, 1);
    assert!(h.meta.child_ino(dir, "f").unwrap().is_some());
}
