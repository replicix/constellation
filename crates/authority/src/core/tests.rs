//! Pure tests of the core: events in, actions out, a real `Meta` as the
//! replica, no runtime, no clock. What the simulation cannot pin down
//! (which action a specific event produces) is pinned here.

use super::*;
use crate::action::{ClientReply, S3Op};
use crate::event::{CasFailure, PeerMsg, Policy, S3Result, UploadResult};
use constellation_fs_core::types::ROOT_INO;
use constellation_meta::{LogRecord, Meta, MetaStore, MutateOp, MutateOutcome, OwnChunks, Rid};
use constellation_store_s3::{Lease, LeaseMode, LeaseStore};

struct Harness {
    core: Core,
    meta: Meta,
    now: Ms,
    /// `step` leaves `Action::PersistLockHorizon` to the test (a slow
    /// write); otherwise it completes it at once, as the driver would.
    manual_horizon: bool,
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
        // The lock tests' timings are written for a 5 s grant (inside
        // this 10 s lease); the production default is 20 s.
        cfg.lock_ttl_ms = 5_000;
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
        Self {
            core,
            meta,
            now,
            manual_horizon: false,
        }
    }

    fn step(&mut self, event: Event) -> Vec<Action> {
        let mut out = self.core.handle(self.now, event, &self.meta);
        if self.manual_horizon {
            return out;
        }
        // The lock-grant horizon write lands at once: its answers follow
        // in the same step (`manual_horizon` keeps it pending).
        let mut i = 0;
        while i < out.len() {
            if let Action::PersistLockHorizon { until } = out[i] {
                let durable = self.meta.note_lock_grant_horizon(until).ok();
                let more = self.core.handle(
                    self.now,
                    Event::LockHorizonPersisted { until, durable },
                    &self.meta,
                );
                out.extend(more);
            }
            i += 1;
        }
        out
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

/// Answer a retention gap check in `out` with "at head" (the first empty
/// probe after a mount, and every takeover's, LISTs the log once:
/// `Core::gap_check_due`), returning what the answer produced; `out`
/// itself when there is none.
fn at_head(h: &mut Harness, out: Vec<Action>) -> Vec<Action> {
    match s3_ops(&out)
        .into_iter()
        .find(|(_, r)| matches!(r, S3Op::SegmentGap { .. }))
    {
        Some((op, _)) => h.step(Event::S3 {
            op,
            result: S3Result::SegmentGap(Ok(None)),
        }),
        None => out,
    }
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
        tag: Default::default(),
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
        tag: Default::default(),
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

/// `locks-blips-tight` with in-doubt lease PUTs (seed 413006): an
/// acquisition CAS that applied, then timed out, left the lease this
/// node's without its knowing; its peers asked it as the holder and got
/// `NotHolder`/`NotOwner` until the lease expired, and their lock
/// waiters timed out. The CAS's failure is now followed by a re-read:
/// the tenure it wrote (holder, epoch, expiry; a waiter's `wanted_by`
/// edit since included) is the acquisition won, adopted as read. Any
/// other object is not.
#[test]
fn an_acquisition_cas_in_doubt_is_reread_and_won_if_it_landed() {
    // (landed, a waiter edited `wanted_by` since)
    for (landed, wanted) in [(true, false), (true, true), (false, false)] {
        let mut h = Harness::new(1);
        let mut out = Vec::new();
        h.core.enqueue_job(
            h.now,
            super::jobs::JobReq::Acquire {
                reason: "test",
                ask_handoff: false,
            },
            &h.meta,
            &mut out,
        );
        let (get, _) = s3_ops(&out)[0];
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(None)),
        });
        let (create, sent) = match s3_ops(&out)[0] {
            (op, S3Op::LeaseCreate { lease }) => (op, lease.clone()),
            other => panic!("expected the create: {other:?}"),
        };
        let out = h.step(Event::S3 {
            op: create,
            result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
        });
        assert!(h.core.lease().held.is_none());
        let reread = match s3_ops(&out)[..] {
            [(op, S3Op::LeaseGet)] => op,
            ref other => panic!("expected a re-read: {other:?}"),
        };
        let found = if landed {
            let mut found = sent.clone();
            if wanted {
                found.wanted_by.push(2);
            }
            found
        } else {
            lease_of(2, 1, h.now.plus(10_000).0)
        };
        let out = h.step(Event::S3 {
            op: reread,
            result: S3Result::LeaseGet(Ok(Some((found, tag())))),
        });
        if !landed {
            assert!(h.core.lease().held.is_none());
            assert!(h.core.job().is_none(), "the acquisition ended: {out:?}");
            assert_eq!(h.core.stats.acquire_cas_in_doubt_landed, 0);
            continue;
        }
        assert_eq!(h.core.stats.acquire_cas_in_doubt_landed, 1);
        let (drain, req) = s3_ops(&out)[0];
        assert!(
            matches!(req, S3Op::InboxDrain { below_epoch: 1 }),
            "{req:?}"
        );
        h.step(Event::S3 {
            op: drain,
            result: S3Result::InboxDrain(Ok(Vec::new())),
        });
        assert_eq!(h.core.lease().epoch(), Some(1));
        let held = h.core.lease().held.as_ref().map(|(l, _)| l.clone());
        assert_eq!(held.map(|l| l.wanted_by.is_empty()), Some(!wanted));
    }
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
        applied: 0,
        tag: Default::default(),
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
        tag: Default::default(),
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
                applied: 0,
                tag: Default::default(),
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
        msg: PeerMsg::LeaseRequest {
            req: OpId(5),
            epoch_applied: None,
        },
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
        msg: PeerMsg::LeaseRequest {
            req: OpId(5),
            epoch_applied: None,
        },
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

/// `long_backup` seed 52088: a release CAS that fails without an answer
/// may have landed, and a released lease is anyone's at once. The holder
/// used to keep it and go on executing its own writes as the holder (two
/// holders); it now admits nothing until a re-read says which. Landed:
/// released (the handoff is served). The re-read failing too: the lease
/// is given up all the same (the handoff is declined; the requester
/// reads the object itself).
#[test]
fn a_release_in_doubt_is_reread_before_anything_is_admitted() {
    for reread_fails in [false, true] {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LeaseRequest {
                req: OpId(5),
                epoch_applied: None,
            },
        });
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
        let (release, req) = s3_ops(&out)[0];
        let S3Op::LeaseSwap {
            lease: released, ..
        } = req.clone()
        else {
            panic!("{req:?}");
        };
        assert!(released.released);
        let out = h.step(Event::S3 {
            op: release,
            result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
        });
        assert!(
            h.core
                .lease
                .new_mutation_epoch(h.now, h.core.config())
                .is_none(),
            "a release in doubt admitted a new local mutation"
        );
        assert!(
            sends(&out).is_empty(),
            "answered before the re-read: {out:?}"
        );
        let reread = match s3_ops(&out).as_slice() {
            [(op, S3Op::LeaseGet)] => *op,
            other => panic!("no lease re-read after a release in doubt: {other:?}"),
        };
        let result = if reread_fails {
            S3Result::LeaseGet(Err(crate::event::S3Failure("timed out".into())))
        } else {
            S3Result::LeaseGet(Ok(Some((released.clone(), tag()))))
        };
        let out = h.step(Event::S3 { op: reread, result });
        assert!(
            matches!(
                sends(&out)[0].1,
                PeerMsg::LeaseHandoff { req: OpId(5), released, .. } if *released == !reread_fails
            ),
            "{out:?}"
        );
        assert!(h.core.lease().held.is_none(), "the lease was kept");
        assert_eq!(h.meta.holder_epoch(), 0);
        assert!(h
            .core
            .lease
            .new_mutation_epoch(h.now, h.core.config())
            .is_none());
        assert!(h.core.job().is_none());
    }
}

/// Slow S3 (slow-s3-no-seal): a round ships until the journal is empty,
/// and with writes arriving faster than one segment PUT that is never.
/// The lease was renewed only as a round opened, so under a live,
/// writing holder it lapsed, its backups stopped hearing from it at
/// expiry and sealed it. A renewal that comes due between two segments
/// goes out before the next segment, and the ship loop resumes after it.
#[test]
fn a_renewal_due_mid_ship_goes_out_between_two_segments() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let submit = |h: &mut Harness, seq: u64, name: &str| {
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid: h.rid(seq),
            op: h.create(name),
            tag: Default::default(),
        });
        assert!(matches!(
            replies(&out)[0].1,
            ClientReply::Outcome(MutateOutcome::Accepted { .. })
        ));
    };
    submit(&mut h, 1, "a");
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
    let mut out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    // Tail (nothing new) until the round ships its first segment.
    let first = loop {
        let (op, req) = s3_ops(&out)
            .into_iter()
            .find(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
            .expect("the round issues S3 requests");
        match req {
            S3Op::SegmentPut { .. } => break op,
            S3Op::SegmentRun { .. } => {
                out = h.step(Event::S3 {
                    op,
                    result: S3Result::SegmentRun(Ok(Vec::new())),
                })
            }
            S3Op::SegmentGap { .. } => {
                out = h.step(Event::S3 {
                    op,
                    result: S3Result::SegmentGap(Ok(None)),
                })
            }
            other => panic!("unexpected {other:?}"),
        }
    };
    // While that PUT is slow, more is written and half the TTL goes by.
    submit(&mut h, 2, "b");
    h.advance(6_000);
    let out = h.step(Event::S3 {
        op: first,
        result: S3Result::SegmentPut(Ok(())),
    });
    let (renew, req) = s3_ops(&out)
        .into_iter()
        .find(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
        .expect("an S3 request after the segment");
    assert!(
        matches!(req, S3Op::LeaseSwap { lease, .. } if !lease.released),
        "the renewal goes out before the next segment: {req:?}"
    );
    let expires_before = h.core.lease().held.as_ref().unwrap().0.expires_unix_ms;
    let out = h.step(Event::S3 {
        op: renew,
        result: S3Result::LeasePut(Ok(tag())),
    });
    assert!(h.core.lease().held.as_ref().unwrap().0.expires_unix_ms > expires_before);
    assert_eq!(h.core.job(), Some(JobKind::Round), "the round goes on");
    assert!(
        s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::SegmentPut { .. })),
        "and ships what was written meanwhile: {out:?}"
    );
}

/// Fix "capture under an epoch hold": a holder another node wants the
/// lease from, whose whole backlog is a transaction deferred on a chunk
/// only an absent node has, does not run rounds back to back until that
/// node returns: a round that moved nothing re-arms the poll at its
/// cadence (flex seed 1007 spun 684 433 rounds at one simulated instant).
#[test]
fn a_backlog_that_cannot_ship_does_not_spin_the_rounds() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    submit_create(&mut h, 1, "f");
    let ino = h.meta.child_ino(ROOT_INO, "f").unwrap().unwrap();
    let away = constellation_fs_core::ChunkHash::of(b"only node 2 has it");
    // Node 2 forwarded the file's manifest with its chunk still pending
    // there; enrolled before the op executes, as the driver does.
    h.meta.enroll_remote_chunks(ino, &[away], 2).unwrap();
    let manifest = constellation_fs_core::Manifest {
        layout: constellation_fs_core::ChunkLayout::new(4096),
        file_len: 7,
        chunks: constellation_fs_core::ChunkInfo::Inline(std::collections::BTreeMap::from([(
            0u64, away,
        )])),
    }
    .encode();
    let rid = h.rid(2);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest,
            size: 7,
            mtime_ns: None,
        },
        tag: Default::default(),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    // Node 2 wants the lease, and the dwell and grace have passed.
    let long_ago = Ms(h.now.0 - 1_000_000);
    h.core.lease.wanted = vec![2];
    h.core.lease.wanted_since = Some(long_ago);
    h.core.lease.held_since = Some(long_ago);
    h.core.lease.last_write = long_ago;
    assert!(h.core.lease.wants_handoff(h.now, h.core.config()));

    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let mut poll = timers(&out, TimerKind::Poll)[0];
    let mut delayed = None;
    for round in 0..6 {
        // Drive the round to its end: the upload pass reports nothing
        // held, the S3 ops are answered, and every action is collected.
        let mut polls: Vec<(TimerId, Ms)> = Vec::new();
        let mut acts = h.step(Event::Timer { id: poll });
        loop {
            polls.extend(acts.iter().filter_map(|a| match a {
                Action::SetTimer {
                    id,
                    at,
                    kind: TimerKind::Poll,
                } => Some((*id, *at)),
                _ => None,
            }));
            if let Some(upload) = acts.iter().find_map(|a| match a {
                Action::UploadDirtyChunks { op, .. } => Some(*op),
                _ => None,
            }) {
                acts = h.step(Event::UploadsDone {
                    op: upload,
                    result: UploadResult::Done { held: 0 },
                });
                continue;
            }
            if let Some((op, req)) = s3_ops(&acts).first().map(|(o, r)| (*o, (*r).clone())) {
                let result = match req {
                    S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                    S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                    S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                    other => panic!("unexpected S3 op in the round: {other:?}"),
                };
                acts = h.step(Event::S3 { op, result });
                continue;
            }
            break;
        }
        let (id, at) = polls.last().copied().expect("the poll is re-armed");
        if at > h.now {
            delayed = Some(round);
            break;
        }
        // The first round shipped the create and may follow up at once;
        // a round that moved nothing must not.
        assert_eq!(
            round, 0,
            "round {round} moved nothing and re-armed the poll at once"
        );
        poll = id;
    }
    let held = h.meta.held_summary();
    assert_eq!(held.deferred, 1, "{held:?}");
    assert!(
        Replica::journal_len(&h.meta).unwrap() > 0,
        "the manifest waits"
    );
    assert!(
        delayed.is_some(),
        "the rounds never settled to the poll cadence"
    );
    assert!(h.core.job().is_none(), "no round follows at once");
}

/// Fix "capture under an epoch hold": a handoff whose flush cannot drain
/// the journal (a manifest deferred on a chunk only an absent node has)
/// is declined; the lease stays, and the requester keeps forwarding.
/// Released, the successor's epoch would strand the deferred rows here
/// and they would be replayed and deferred there — the lease bounced to
/// epoch 111 in flex-crash seed 3021.
#[test]
fn a_handoff_is_declined_while_the_journal_cannot_drain() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    submit_create(&mut h, 1, "f");
    let ino = h.meta.child_ino(ROOT_INO, "f").unwrap().unwrap();
    let away = constellation_fs_core::ChunkHash::of(b"only node 2 has it");
    h.meta.enroll_remote_chunks(ino, &[away], 2).unwrap();
    let manifest = constellation_fs_core::Manifest {
        layout: constellation_fs_core::ChunkLayout::new(4096),
        file_len: 7,
        chunks: constellation_fs_core::ChunkInfo::Inline(std::collections::BTreeMap::from([(
            0u64, away,
        )])),
    }
    .encode();
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(2),
        op: MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest,
            size: 7,
            mtime_ns: None,
        },
        tag: Default::default(),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    let mut out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseRequest {
            req: OpId(5),
            epoch_applied: None,
        },
    });
    assert_eq!(h.core.job(), Some(JobKind::Handoff));
    // Drive the handoff's flush: the upload pass, the create ships, the
    // manifest stays deferred.
    let mut answered = None;
    for _ in 0..8 {
        if let Some(a) = sends(&out).iter().find_map(|(to, m)| match m {
            PeerMsg::LeaseHandoff { released, .. } if *to == 2 => Some(*released),
            _ => None,
        }) {
            answered = Some(a);
            break;
        }
        if let Some(upload) = out.iter().find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        }) {
            out = h.step(Event::UploadsDone {
                op: upload,
                result: UploadResult::Done { held: 0 },
            });
            continue;
        }
        if let Some((op, req)) = s3_ops(&out).first().map(|(o, r)| (*o, (*r).clone())) {
            let result = match req {
                S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                S3Op::LeaseSwap { lease, .. } => {
                    assert!(
                        !lease.released,
                        "released the lease with a deferred journal"
                    );
                    S3Result::LeasePut(Ok(tag()))
                }
                S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                other => panic!("unexpected S3 op in the handoff: {other:?}"),
            };
            out = h.step(Event::S3 { op, result });
            continue;
        }
        break;
    }
    assert_eq!(
        answered,
        Some(false),
        "the handoff was not declined: {out:?}"
    );
    assert!(h.core.lease().held.is_some(), "the lease stays");
    assert_eq!(h.core.stats.handoffs_declined, 1);
    assert_eq!(h.meta.held_summary().deferred, 1);
    assert!(h.core.job().is_none());
}

/// A claim of the holder's placement offer asks the holder over P2P
/// before anything touches S3: a declined claim (the holder writes more
/// now, `placement::declines_claim`) costs no S3 request at all, and
/// the requester settles as a non-holder.
#[test]
fn a_claimed_offer_asks_the_holder_first_and_a_decline_costs_no_s3() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let out = h.step(Event::Control {
        op: OpId(900),
        req: Control::ClaimOffer { epoch: 1 },
    });
    assert!(
        s3_ops(&out).is_empty(),
        "no S3 before the holder answers: {out:?}"
    );
    let req = match sends(&out)[..] {
        [(2, PeerMsg::LeaseRequest { req, .. })] => *req,
        ref other => panic!("expected one LeaseRequest to the holder: {other:?}"),
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseHandoff {
            req,
            released: false,
            epoch: 1,
            head_seq: None,
        },
    });
    assert!(s3_ops(&out).is_empty(), "a decline costs no S3: {out:?}");
    assert!(h.core.lease().held.is_none());
    assert!(h.core.job().is_none(), "the acquisition ended");
}

/// `visibility-s3-latency`: a lease request queued behind a round that
/// outlasts the requester's wait (a ship loop under slow S3) is
/// declined when it reaches the slot — no flush, no release: the
/// requester gave up, and a release would leave the lease to nobody.
#[test]
fn a_handoff_request_that_waited_past_its_timeout_is_declined() {
    let mut h = Harness::new(1);
    h.hold(1, None);
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
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseRequest {
            req: OpId(5),
            epoch_applied: None,
        },
    });
    assert!(sends(&out).is_empty());
    // The round outlives the requester's wait.
    h.advance(h.core.config().handoff_request_timeout_ms + 1);
    let mut out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    // Let the round finish (it renews first: half the TTL went by).
    let mut declined = false;
    for _ in 0..10 {
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::LeaseSwap { lease, .. } if lease.released)),
            "nothing is released: {out:?}"
        );
        assert!(
            !out.iter()
                .any(|a| matches!(a, Action::UploadDirtyChunks { round: false, .. })),
            "no handoff flush: {out:?}"
        );
        if sends(&out).iter().any(|(to, m)| {
            *to == 2
                && matches!(
                    m,
                    PeerMsg::LeaseHandoff {
                        req: OpId(5),
                        released: false,
                        ..
                    }
                )
        }) {
            declined = true;
            break;
        }
        let Some((op, req)) = s3_ops(&out)
            .into_iter()
            .find(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
        else {
            break;
        };
        let result = match req {
            S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
            S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
            S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
            other => panic!("unexpected {other:?}"),
        };
        out = h.step(Event::S3 { op, result });
    }
    assert!(declined, "the stale request is declined");
    assert!(h.core.lease().held.is_some());
    assert!(!h.core.lease().releasing);
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
        tag: Default::default(),
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
    let out = h.step(Event::S3 {
        op: tail,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    let _ = at_head(&mut h, out);
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
        tag: Default::default(),
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
        tag: Default::default(),
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
            outcome: MutateOutcome::Errno(Code::Invalid),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
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
        tag: Default::default(),
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
        tag: Default::default(),
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

/// An unmount's flush that finds a cadence publish still in flight waits
/// for it and then publishes what shipped since, before it releases and
/// stops (`e2e-basic` under load: the flush skipped its publish because
/// one was running, released, and the process exit dropped the running
/// publish — a clean unmount that left no commit).
#[test]
fn a_shutdown_waits_for_an_in_flight_publish_and_publishes_after_it() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("a"),
        tag: Default::default(),
    });
    assert!(!replies(&out).is_empty());
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
    let (put, _) = s3_ops(&out)[0];
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

    // `b` lands after that publish started; then the unmount.
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(2),
        op: h.create("b"),
        tag: Default::default(),
    });
    assert!(!replies(&out).is_empty());
    let shutdown = OpId(901);
    let out = h.step(Event::Control {
        op: shutdown,
        req: Control::Shutdown,
    });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the flush uploads first");
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
    assert!(
        s3_ops(&out).is_empty()
            && !out
                .iter()
                .any(|a| matches!(a, Action::Publish { .. } | Action::ControlDone { .. })),
        "nothing released, published or answered while a publish runs: {out:?}"
    );
    assert!(!h.core.stopped());

    // The running publish ends; the flush publishes `b`'s state itself,
    // and only then releases.
    let out = h.step(Event::PublishDone {
        op: publish,
        ok: true,
    });
    let own = out
        .iter()
        .find_map(|a| match a {
            Action::Publish { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap_or_else(|| panic!("the flush publishes after the running one: {out:?}"));
    assert!(s3_ops(&out).is_empty(), "{out:?}");
    let out = h.step(Event::PublishDone { op: own, ok: true });
    let (release, req) = s3_ops(&out)[0];
    assert!(
        matches!(req, S3Op::LeaseSwap { lease, .. } if lease.released),
        "{req:?}"
    );
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Ok(tag())),
    });
    assert!(
        out.iter()
            .any(|a| matches!(a, Action::ControlDone { op, result: Ok(_) } if *op == shutdown)),
        "{out:?}"
    );
    assert!(h.core.stopped());
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
            tag: Default::default(),
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
                applied: 0,
                tag: Default::default(),
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
        tag: Default::default(),
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
        tag: Default::default(),
    });
    for key in &keys {
        assert!(
            sends(&out).is_empty(),
            "nothing is forwarded while a batch is still durable: {out:?}"
        );
        let (op, req) = s3_ops(&out)[0];
        assert!(
            matches!(req, S3Op::InboxTombstone { batch } if batch.key() == *key && batch.is_tombstone()),
            "batches go in order, each overwritten with a tombstone: {req:?}"
        );
        assert!(matches!(
            h.core.clients().next(),
            Some((_, ClientPhase::InboxWithdraw))
        ));
        out = h.step(Event::S3 {
            op,
            result: S3Result::InboxTombstone(Ok(())),
        });
    }
    assert!(
        matches!(sends(&out)[0].1, PeerMsg::MutateRequest { rid: r, .. } if *r == rid),
        "forwarded once every batch is withdrawn: {out:?}"
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
            outcome: MutateOutcome::Errno(Code::Io),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
        },
    });
    assert_eq!(replies(&out).len(), 1);

    // A withdrawal that fails leaves the batch drainable: the op takes the
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
        tag: Default::default(),
    });
    let (op, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::InboxTombstone { .. }));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::InboxTombstone(Err(crate::event::S3Failure("500".into()))),
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
        tag: Default::default(),
    });
    let req = match sends(&out)[0].1 {
        PeerMsg::MutateRequest { req, .. } => *req,
        other => panic!("{other:?}"),
    };
    // Two more ops on the same name queue behind `a` at the gate (plan
    // 30 §M12: creates of *different* names in one directory no longer
    // conflict — the parent hold is shared).
    for seq in [2, 3] {
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid: h.rid(seq),
            op: h.create("a"),
            tag: Default::default(),
        });
        assert!(sends(&out).is_empty(), "op {seq} is gated: {out:?}");
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
            outcome: MutateOutcome::Errno(Code::Io),
            base: Some(0),
            position: constellation_meta::Position::ZERO,
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
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
        tag: Default::default(),
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
        tag: Default::default(),
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
        ClientReply::Outcome(MutateOutcome::Exists { .. } | MutateOutcome::Errno(Code::Exists))
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
        mtime_ns: None,
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
    h.meta
        .queue_replay(a(1), &truncate, &Default::default())
        .unwrap();
    h.meta
        .queue_replay(a(2), &set(Some(base), b"loser-from-a"), &Default::default())
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
        .set_manifest_dirty(file.ino, None, &manifest, 12, None, &[])
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

/// `epoch-holder-retired`: node 2 held the epoch hold owner's (node 1's)
/// write as streamed speculation when node 1 died; the operator retired
/// node 1 (`leave --node-id`: its id in the fenced lease's `retired`). The
/// write was stranded here as a foreign entry and replayed by rid through
/// the successor: the retired node's unflushed write surfaced (about one
/// run in four). The drop keys on the node whose stream carried the entry
/// (its journal is what died), never on the requester: a retired node's op
/// a live holder streamed replays, and a live node's op the retired holder
/// streamed does not.
#[test]
fn a_retired_nodes_stranded_journal_is_not_replayed() {
    // (requester, streamed by, retired, dropped)
    let cases = [
        (1, 1, Some(1), true),
        (3, 1, Some(1), true),
        (1, 3, Some(1), false),
        (1, 1, None, false),
    ];
    for (requester, source, retired, dropped) in cases {
        let mut h = Harness::new(2);
        h.core.lease.cached_holder = Some(3);
        let rid = Rid {
            node: requester,
            incarnation: 1,
            seq: 4,
        };
        let ino = h.meta.allocate_ino(ROOT_INO).unwrap();
        let records = [
            LogRecord::Create {
                parent: ROOT_INO,
                name: "lost-with-a".into(),
                ino,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 3,
            },
            LogRecord::Completed { rid },
        ];
        h.meta.install_streamed(1, source, 2, 3, &records).unwrap();
        h.meta.strand_below_epoch(2).unwrap();
        let queued = h.meta.pending_replays().unwrap();
        assert_eq!(queued.len(), 1, "{queued:?}");
        assert!(queued[0].foreign);
        assert_eq!(queued[0].source, Some(source));
        let mut lease = lease_of(3, 3, h.now.plus(10_000).0);
        lease.retired = retired.into_iter().collect();
        let now = h.now;
        h.core.lease.note_object(now, &lease);
        let mut out = Vec::new();
        h.core.on_drain_tick(now, &h.meta, &mut out);
        let case = (requester, source, retired);
        if dropped {
            assert!(h.meta.pending_replays().unwrap().is_empty(), "{case:?}");
            assert!(
                sends(&out).is_empty() && s3_ops(&out).is_empty(),
                "{case:?}: {out:?}"
            );
            assert_eq!(h.core.stats.replays_of_retired_dropped, 1, "{case:?}");
        } else {
            assert_eq!(h.meta.pending_replays().unwrap().len(), 1, "{case:?}");
            assert!(
                h.core.clients().any(|(r, _)| r == rid),
                "{case:?}: the replay was not submitted: {out:?}"
            );
            assert_eq!(h.core.stats.replays_of_retired_dropped, 0, "{case:?}");
        }
    }
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
    requester
        .meta
        .queue_replay(requester.rid(1), &op, &Default::default())
        .unwrap();
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
        tag: Default::default(),
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
            own_chunks: OwnChunks::None,
            own_rows: None,
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

/// `fuse-inval-storm` on uring: node 2's rename was acknowledged by the
/// holder (node 1, epoch 1), its unlink of the new name accepted on a base
/// node 2 had not applied (`AwaitingLog`); node 1 died with both unshipped
/// and node 3 took over (epoch 2). The rename was stranded and queued for
/// replay — and its replay was gated behind the unlink (submitted
/// earlier, same directory), which waited for a log that would never
/// carry it: both waited out the 40 s client deadline, and the unlink was
/// answered in doubt (`EIO`). Now the replay goes first, and the unlink is
/// asked of the new holder once the replay is done.
#[test]
fn a_stranded_replay_is_not_gated_behind_an_op_awaiting_a_dead_epochs_log() {
    let (mut requester, rid_u, _) = awaiting_log_forward("u");
    let rid_r = requester.rid(7);
    // (Any op on the same name: the gate orders by keys.)
    let rename = MutateOp::Rename {
        parent: ROOT_INO,
        name: "r".into(),
        new_parent: ROOT_INO,
        new_name: "u".into(),
        noreplace: false,
    };
    requester
        .meta
        .queue_replay(rid_r, &rename, &Default::default())
        .unwrap();
    // Node 3's epoch 2 is in the log: node 1's epoch is over.
    crate::replica::Replica::apply_segment(&requester.meta, 1, 2, 0, &[], &[], &[]).unwrap();
    requester.core.ship.max_epoch = 2;
    requester.core.ship.next_seq = 2;
    requester.core.lease.cached_holder = Some(3);
    // The unlink is not asked of anyone before the replay.
    let out = activity(&mut requester);
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, PeerMsg::MutateRequest { .. })),
        "{out:?}"
    );
    let mut out = Vec::new();
    let now = requester.now;
    requester.core.on_drain_tick(now, &requester.meta, &mut out);
    let sent = sends(&out);
    let [(3, PeerMsg::MutateRequest { req, .. })] = sent.as_slice() else {
        panic!(
            "the replay was not forwarded to node 3: {out:?} {:?}",
            requester.core.clients().collect::<Vec<_>>()
        )
    };
    let req = *req;
    let out = requester.step(Event::Peer {
        from: 3,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Accepted {
                epoch: 2,
                records: vec![LogRecord::Completed { rid: rid_r }],
            },
            base: Some(1),
            position: constellation_meta::Position {
                seq: 1,
                pending: Some(constellation_meta::JournalPos { epoch: 2, jseq: 1 }),
                streams: Default::default(),
            },
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
        },
    });
    assert!(
        requester.meta.pending_replays().unwrap().is_empty(),
        "{out:?}"
    );
    // Now the unlink goes to node 3, by its rid (the same event).
    let sent: Vec<Rid> = sends(&out)
        .iter()
        .filter_map(|(to, m)| match m {
            PeerMsg::MutateRequest { rid, .. } if *to == 3 => Some(*rid),
            _ => None,
        })
        .collect();
    assert_eq!(sent, vec![rid_u], "{out:?}");
    assert_eq!(requester.core.stats.awaiting_log_rerouted, 1);
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

/// Chunk close-stall-metered (PROGRESS, "Fix: a metered non-owner's
/// `back` close stalled 120 s"): node 2 closes `f` under `--write-mode
/// back` with its uploads held (a metered network), so the manifest it
/// forwards names a chunk still pending on it, which node 1 enrolls as
/// node 2's (the driver does that before the op executes). Node 1's
/// answer says whether the records wait for node 2's upload
/// (`OwnChunks`); node 2 uploads at once when nothing else brings them
/// back, keeps them held when a stream does, and a safety timer covers a
/// stream that stalls.
mod own_chunks {
    use super::*;
    use constellation_fs_core::{ChunkHash, Manifest};
    use constellation_store_s3::AckPolicy;

    fn uploads_awaited(actions: &[Action]) -> Vec<u64> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::UploadAwaited { inos } => Some(inos.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }

    fn own_record_timers(h: &Harness) -> usize {
        h.core
            .timers
            .values()
            .filter(|(t, _)| matches!(t, Timer::OwnRecordWait(_)))
            .count()
    }

    /// Node 1 holds under `policy` (with `backups`); `Local` with nobody
    /// in budget is the unbacked tenure chunk 39b met on real S3.
    fn holding(policy: AckPolicy, backups: Vec<NodeId>) -> (Harness, Harness) {
        let (mut holder, requester) = pair();
        let lease = Lease {
            ack_policy: policy,
            backups,
            ..lease_of(1, 1, holder.now.plus(10_000).0)
        };
        holder.core.lease.adopt(holder.now, lease, tag(), None);
        holder.core.ack.eligible = Some(false);
        (holder, requester)
    }

    /// Node 1 creates `f` (unshipped: a forward touching it is answered
    /// on a base node 2 has not applied), and node 2's close of it names
    /// one chunk — pending on node 2, enrolled on node 1 when `pending`.
    /// Returns the close, its records as node 1 would journal them, and
    /// node 1's records of the create.
    fn back_close(
        holder: &mut Harness,
        pending: bool,
    ) -> (MutateOp, Vec<LogRecord>, Vec<LogRecord>) {
        let create = holder.create("f");
        let MutateOp::Create { ino, .. } = create else {
            unreachable!()
        };
        let created = constellation_meta::execute_mutate(&holder.meta, &create, None).unwrap();
        let chunk = ChunkHash::of(b"held on node 2");
        let manifest =
            Manifest::from_sparse_chunks(4096, 14, [(0u64, chunk)].into(), 64, ChunkHash::of)
                .0
                .encode();
        if pending {
            holder.meta.enroll_remote_chunks(ino, &[chunk], 2).unwrap();
        }
        let op = MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest,
            size: 14,
            mtime_ns: None,
        };
        let records = vec![LogRecord::WriteManifest {
            ino,
            base_manifest: None,
            manifest: match &op {
                MutateOp::SetManifest { manifest, .. } => manifest.clone(),
                _ => unreachable!(),
            },
            size: 14,
            time_ns: 0,
            mtime_ns: 0,
        }];
        (op, records, created)
    }

    /// Node 2 submits `op`; node 1 answers its forward. Returns node 1's
    /// reply and what node 2 did with it.
    fn forward(
        holder: &mut Harness,
        requester: &mut Harness,
        rid: Rid,
        op: MutateOp,
    ) -> (PeerMsg, Vec<Action>) {
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        assert!(uploads_awaited(&out).is_empty(), "not with the forward");
        let sent = sends(&out);
        let [(1, request)] = sent.as_slice() else {
            panic!("expected one forward to node 1: {sent:?}")
        };
        let answer = holder.step(Event::Peer {
            from: 2,
            msg: (*request).clone(),
        });
        let replies: Vec<PeerMsg> = sends(&answer)
            .into_iter()
            .filter(|(to, m)| *to == 2 && matches!(m, PeerMsg::MutateReply { .. }))
            .map(|(_, m)| m.clone())
            .collect();
        let [reply] = replies.as_slice() else {
            panic!("expected one reply to node 2: {answer:?}")
        };
        let out = requester.step(Event::Peer {
            from: 1,
            msg: reply.clone(),
        });
        (reply.clone(), out)
    }

    fn own_chunks_of(msg: &PeerMsg) -> (OwnChunks, &MutateOutcome) {
        match msg {
            PeerMsg::MutateReply {
                own_chunks,
                outcome,
                ..
            } => (own_chunks.clone(), outcome),
            other => panic!("not a reply: {other:?}"),
        }
    }

    /// Unbacked (`Local`, nobody in budget): no stream carries node 2's
    /// record back before its chunk is up, and the ship waits for that
    /// chunk. Node 1 says `Upload`, node 2 uploads at once. Before the
    /// fix nothing did: the close waited out the forward deadline and was
    /// answered in doubt.
    #[test]
    fn an_unbacked_holder_asks_for_the_upload_and_the_forwarder_uploads_at_once() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let (op, _, _) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = op else {
            unreachable!()
        };
        let rid = requester.rid(1);
        let (reply, out) = forward(&mut holder, &mut requester, rid, op);
        let (own, outcome) = own_chunks_of(&reply);
        assert!(
            matches!(outcome, MutateOutcome::Accepted { .. }),
            "{reply:?}"
        );
        assert_eq!(own, OwnChunks::Upload(vec![ino]));
        assert_eq!(
            requester.core.clients().collect::<Vec<_>>(),
            vec![(rid, ClientPhase::AwaitingLog)]
        );
        assert_eq!(uploads_awaited(&out), vec![ino]);
        assert_eq!(requester.core.stats.own_record_uploads, 1);
        // The safety timer repeats it while the op still waits (a pass
        // that failed).
        let [again] = timers(&out, TimerKind::OwnRecordWait)[..] else {
            panic!("one safety timer: {out:?}")
        };
        requester.advance(requester.core.cfg.own_record_wait_ms);
        let out = requester.step(Event::Timer { id: again });
        assert_eq!(uploads_awaited(&out), vec![ino]);
        assert_eq!(requester.core.stats.own_record_uploads, 2);
    }

    /// Should-fix 2: a forward whose manifest names nothing pending on
    /// node 2 (a `through` close, or its chunks already reported up) is
    /// answered `None`, and node 2 uploads nothing and arms nothing — under
    /// an `S3` lease on node 1 too, where the reply is parked for
    /// durability and answered `Held` after the hold interval, not at once
    /// as `held_for_upload` would.
    #[test]
    fn a_forward_with_nothing_pending_uploads_nothing() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let (op, _, _) = back_close(&mut holder, false);
        let (reply, out) = {
            let rid = requester.rid(1);
            forward(&mut holder, &mut requester, rid, op)
        };
        assert_eq!(own_chunks_of(&reply).0, OwnChunks::None);
        assert!(uploads_awaited(&out).is_empty(), "{out:?}");
        assert!(timers(&out, TimerKind::OwnRecordWait).is_empty());
        assert_eq!(requester.core.stats.own_record_uploads, 0);

        let (mut holder, mut requester) = holding(AckPolicy::S3, Vec::new());
        let (op, _, _) = back_close(&mut holder, false);
        let rid = requester.rid(1);
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        let sent = sends(&out);
        let [(1, request)] = sent.as_slice() else {
            panic!("expected one forward to node 1: {sent:?}")
        };
        let answer = holder.step(Event::Peer {
            from: 2,
            msg: (*request).clone(),
        });
        assert!(sends(&answer).is_empty(), "parked: {answer:?}");
        assert_eq!(holder.core.stats.held_for_upload, 0);
        let held = timers(&answer, TimerKind::HeldReply)[0];
        holder.advance(holder.core.cfg.recall_hold_ms);
        let answer = holder.step(Event::Timer { id: held });
        let [(2, reply)] = sends(&answer)[..] else {
            panic!("a held reply: {answer:?}")
        };
        let (own, outcome) = own_chunks_of(reply);
        assert!(matches!(outcome, MutateOutcome::Held { .. }), "{reply:?}");
        assert_eq!(own, OwnChunks::None);
        let out = requester.step(Event::Peer {
            from: 1,
            msg: reply.clone(),
        });
        assert!(uploads_awaited(&out).is_empty(), "{out:?}");
        assert!(timers(&out, TimerKind::OwnRecordWait).is_empty());

        // Nor does an op that names no chunk at all.
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let op = requester.create("g");
        let (reply, out) = {
            let rid = requester.rid(1);
            forward(&mut holder, &mut requester, rid, op)
        };
        assert_eq!(own_chunks_of(&reply).0, OwnChunks::None);
        assert!(uploads_awaited(&out).is_empty());
    }

    /// `ack=s3` (the holder's lease policy `S3`): the acknowledgement
    /// waits for the record's segment, which waits for node 2's chunk.
    /// Node 1 parks the reply and says so at once — a `Held` asking for
    /// the upload and a quick retry, not after the hold interval — and
    /// node 2 uploads. Before the fix it was `Held` every hold interval
    /// until the forward deadline.
    #[test]
    fn an_s3_acknowledgement_waiting_for_the_forwarders_chunk_is_held_for_its_upload_at_once() {
        let (mut holder, mut requester) = holding(AckPolicy::S3, Vec::new());
        let (op, _, _) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = op else {
            unreachable!()
        };
        let rid = requester.rid(1);
        let (reply, out) = forward(&mut holder, &mut requester, rid, op);
        let (own, outcome) = own_chunks_of(&reply);
        assert_eq!(
            outcome,
            &MutateOutcome::Held {
                retry_ms: holder.core.cfg.held_retry_ms
            }
        );
        assert_eq!(own, OwnChunks::Upload(vec![ino]));
        assert_eq!(holder.core.stats.held_for_upload, 1);
        assert_eq!(uploads_awaited(&out), vec![ino]);
        // The retry re-attaches; a later `Held` of the same wait does not
        // upload again (the safety timer repeats it if it must).
        let retry = timers(&out, TimerKind::ForwardBackoff)[0];
        requester.advance(holder.core.cfg.held_retry_ms);
        let out = requester.step(Event::Timer { id: retry });
        let sent = sends(&out);
        let [(1, request)] = sent.as_slice() else {
            panic!("expected the retry: {sent:?}")
        };
        let answer = holder.step(Event::Peer {
            from: 2,
            msg: (*request).clone(),
        });
        assert!(
            sends(&answer).is_empty(),
            "re-attached to the parked reply: {answer:?}"
        );
        let held = timers(&answer, TimerKind::HeldReply)[0];
        holder.advance(holder.core.cfg.recall_hold_ms);
        let answer = holder.step(Event::Timer { id: held });
        let [(2, reply)] = sends(&answer)[..] else {
            panic!("a held reply: {answer:?}")
        };
        assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![ino]));
        let out = requester.step(Event::Peer {
            from: 1,
            msg: reply.clone(),
        });
        assert!(uploads_awaited(&out).is_empty(), "uploaded once: {out:?}");
        assert_eq!(requester.core.stats.own_record_uploads, 1);
    }

    /// A `Held` that is not an acknowledgement waiting for node 2's
    /// chunks — a recall's, a lost dependency's, a delegate re-route's —
    /// carries `None`, and node 2 neither uploads nor arms a timer
    /// (should-fix 1: such a hold may last `recall_hold_ms`, 60 s, and the
    /// record ships without node 2's upload).
    #[test]
    fn a_recall_hold_uploads_nothing() {
        let (_holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let rid = requester.rid(1);
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: MutateOp::SetManifest {
                ino: 77,
                base_manifest: None,
                manifest: vec![1, 2, 3],
                size: 3,
                mtime_ns: None,
            },
            tag: Default::default(),
        });
        let [(1, PeerMsg::MutateRequest { req, .. })] = sends(&out)[..] else {
            panic!("expected a forward: {out:?}")
        };
        let out = requester.step(Event::Peer {
            from: 1,
            msg: PeerMsg::MutateReply {
                req: *req,
                outcome: MutateOutcome::Held { retry_ms: 50 },
                base: None,
                position: constellation_meta::Position::ZERO,
                gen: 0,
                own_chunks: OwnChunks::None,
                own_rows: None,
            },
        });
        assert!(uploads_awaited(&out).is_empty());
        assert!(timers(&out, TimerKind::OwnRecordWait).is_empty());
        assert_eq!(own_record_timers(&requester), 0);
    }

    /// The holder's side of the signal, case by case: `Streamed` only for
    /// a stream subscriber of a backed root (whose stream carries the
    /// forwarder's own transaction past its pending chunks); `Upload` for a
    /// non-subscriber, an unbacked or `S3` tenure, and a delegate's
    /// execution; `None` for records naming nothing of the requester's.
    #[test]
    fn the_holder_says_streamed_only_where_its_stream_carries_the_record() {
        let (mut holder, _sub, sub_req) = stream_pair();
        let (_, records, _) = back_close(&mut holder, true);
        let accepted = MutateOutcome::Accepted {
            epoch: 1,
            records: records.clone(),
        };
        let ino = match &records[0] {
            LogRecord::WriteManifest { ino, .. } => *ino,
            other => panic!("{other:?}"),
        };
        // Not journaled here: the records alone say what it waits for.
        let rid = Rid {
            node: 2,
            incarnation: 1,
            seq: 99,
        };
        let zero = constellation_meta::Position::ZERO;
        let ask = |h: &Harness, to: NodeId, gen: u64| {
            h.core
                .own_chunks_for(to, rid, &accepted, (gen, &zero), &h.meta)
        };
        let upload = OwnChunks::Upload(vec![ino]);
        // `Local`, no backup: nothing streams ahead.
        assert_eq!(ask(&holder, 2, 0), upload);
        let backed = |h: &mut Harness, policy: AckPolicy, backups: Vec<NodeId>| {
            let lease = Lease {
                ack_policy: policy,
                backups,
                ..lease_of(1, 1, h.now.plus(10_000).0)
            };
            h.core.lease.adopt(h.now, lease, tag(), None);
        };
        backed(&mut holder, AckPolicy::Backup, vec![3]);
        assert_eq!(ask(&holder, 2, 0), OwnChunks::Streamed(vec![ino]));
        assert_eq!(ask(&holder, 2, 5), upload, "a delegate's");
        // The records name nothing of node 4's.
        assert_eq!(ask(&holder, 4, 0), OwnChunks::None);
        // Node 2 not subscribed: no stream reaches it.
        holder.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LogUnsubscribe { req: sub_req },
        });
        assert_eq!(ask(&holder, 2, 0), upload, "not a subscriber");
        backed(&mut holder, AckPolicy::S3, Vec::new());
        assert_eq!(ask(&holder, 2, 0), upload);
        // Not accepted: nothing to say.
        assert_eq!(
            holder
                .core
                .own_chunks_for(2, rid, &MutateOutcome::Busy, (0, &zero), &holder.meta),
            OwnChunks::None
        );
        // Reported up: nothing waits for node 2 any more.
        holder
            .meta
            .ack_remote_chunks(&[ChunkHash::of(b"held on node 2")])
            .unwrap();
        assert_eq!(ask(&holder, 2, 0), OwnChunks::None);
    }

    /// Backed: node 1's stream carries node 2's record to it past its
    /// held chunk (`Streamed`), so nothing is uploaded on the metered
    /// network — only the safety timer is armed. When the record arrives
    /// the op finishes and the timer is cancelled (should-fix 3).
    #[test]
    fn a_streamed_answer_keeps_the_chunks_held_and_finishing_cancels_the_timer() {
        let (requester, rid, records) = streamed_awaiting_log();
        let mut requester = requester;
        assert_eq!(requester.core.stats.own_record_uploads, 0);
        let timer = requester
            .core
            .clients
            .get(&rid)
            .and_then(|c| c.own_record_timer)
            .expect("the safety timer");
        // The record arrives (here: the segment carrying it applied).
        crate::replica::Replica::apply_segment(&requester.meta, 1, 1, 0, &[], &[], &records)
            .unwrap();
        let mut out = Vec::new();
        let now = requester.now;
        requester
            .core
            .answer_awaiting_log(now, &requester.meta, &mut out);
        assert_eq!(requester.core.clients().count(), 0, "finished: {out:?}");
        assert_eq!(replies(&out).len(), 1);
        assert!(
            out.iter()
                .any(|a| matches!(a, Action::CancelTimer { id } if *id == timer)),
            "the safety timer is cancelled with the op: {out:?}"
        );
        assert_eq!(own_record_timers(&requester), 0);
        assert!(uploads_awaited(&out).is_empty());
    }

    /// Backed, but the stream stalls (its backup lost after the reply,
    /// say): past `own_record_wait_ms` the safety timer uploads anyway.
    #[test]
    fn a_stalled_stream_uploads_after_the_grace() {
        let (mut requester, _rid, records) = streamed_awaiting_log();
        let ino = records
            .iter()
            .find_map(|r| match r {
                LogRecord::WriteManifest { ino, .. } => Some(*ino),
                _ => None,
            })
            .unwrap_or_else(|| panic!("the close: {records:?}"));
        let timer = requester
            .core
            .timers
            .iter()
            .find(|(_, (t, _))| matches!(t, Timer::OwnRecordWait(_)))
            .map(|(id, _)| *id)
            .expect("the safety timer");
        requester.advance(requester.core.cfg.own_record_wait_ms);
        let out = requester.step(Event::Timer { id: timer });
        assert_eq!(uploads_awaited(&out), vec![ino]);
        assert_eq!(requester.core.stats.own_record_uploads, 1);
        assert_eq!(own_record_timers(&requester), 1, "repeated while it waits");
    }

    /// `CONSTELLATION_OWN_RECORD_WAIT_MS=0` turns it all off (the stall
    /// comes back: for reproducing it).
    #[test]
    fn a_zero_wait_turns_the_mechanism_off() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        requester.core.cfg.own_record_wait_ms = 0;
        let (op, _, _) = back_close(&mut holder, true);
        let (reply, out) = {
            let rid = requester.rid(1);
            forward(&mut holder, &mut requester, rid, op)
        };
        assert!(matches!(own_chunks_of(&reply).0, OwnChunks::Upload(_)));
        assert!(uploads_awaited(&out).is_empty());
        assert_eq!(own_record_timers(&requester), 0);
    }

    fn chmod(ino: u64) -> MutateOp {
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

    /// Review must-fix 1: node 2's `back` close of `f` was forwarded (its
    /// chunk still held on node 2), and node 2 then `chmod`s `f` (here
    /// from a fresh requester state: in the repro the close had already
    /// returned on an applied base). The chmod's records name no chunk,
    /// but node 1's ship plan defers it with the close (plan 30 §M4's key
    /// dependence), and its reply comes on a base node 2 has not applied:
    /// it waits for the log, which waits for node 2's chunk. Node 1 says
    /// `Upload` naming `f`, and node 2 uploads `f`'s chunks at once —
    /// under `Local` with no backup, and as a `Held` under `ack=s3`.
    /// Before, the answer was `None` and the chmod stalled 120 s, EIO.
    #[test]
    fn an_op_depending_on_a_held_close_asks_for_the_close_chunks() {
        for policy in [AckPolicy::Local, AckPolicy::S3] {
            let (mut holder, mut requester) = holding(policy, Vec::new());
            let (close, _, _) = back_close(&mut holder, true);
            let MutateOp::SetManifest { ino, .. } = close else {
                unreachable!()
            };
            let rid = requester.rid(1);
            forward(&mut holder, &mut requester, rid, close);
            let (_, mut requester) = holding(policy, Vec::new());
            let rid = requester.rid(2);
            let (reply, out) = forward(&mut holder, &mut requester, rid, chmod(ino));
            let (own, outcome) = own_chunks_of(&reply);
            assert_eq!(own, OwnChunks::Upload(vec![ino]), "{policy:?}: {reply:?}");
            match policy {
                AckPolicy::S3 => {
                    assert!(matches!(outcome, MutateOutcome::Held { .. }), "{reply:?}")
                }
                _ => {
                    assert!(
                        matches!(outcome, MutateOutcome::Accepted { .. }),
                        "{reply:?}"
                    );
                    assert_eq!(
                        requester.core.clients().collect::<Vec<_>>(),
                        vec![(rid, ClientPhase::AwaitingLog)]
                    );
                }
            }
            assert_eq!(uploads_awaited(&out), vec![ino], "{policy:?}");
            if policy == AckPolicy::S3 {
                // (Every acknowledgement waits for a segment there.)
                continue;
            }
            // An op on another file waits for nothing of node 2's.
            let other = holder.create("g");
            let (_, mut requester) = holding(policy, Vec::new());
            let rid = requester.rid(3);
            let (reply, out) = forward(&mut holder, &mut requester, rid, other);
            assert_eq!(own_chunks_of(&reply).0, OwnChunks::None, "{reply:?}");
            assert!(uploads_awaited(&out).is_empty());
        }
    }

    /// Review should-fix 1: a backed root streams node 2's transaction to
    /// it past node 2's own pending chunk, but not past one node 2 does not
    /// have — here node 1's own write-back of another file, journaled
    /// first. Then the stream stops before node 2's close and only the
    /// segment brings it, after node 2's chunk is up: `Upload`, not a
    /// `Streamed` that would cost node 2 the safety timer's 10 s first.
    #[test]
    fn a_stream_blocked_before_the_op_asks_for_the_upload() {
        for blocked in [false, true] {
            let (mut holder, _sub, _req) = stream_pair();
            let lease = Lease {
                ack_policy: AckPolicy::Backup,
                backups: vec![3],
                ..lease_of(1, 1, holder.now.plus(10_000).0)
            };
            holder.core.lease.adopt(holder.now, lease, tag(), None);
            if blocked {
                let own = ChunkHash::of(b"node 1's own write-back");
                let g = holder.meta.create(ROOT_INO, "g", 0o644, 0, 0).unwrap().ino;
                let manifest =
                    Manifest::from_sparse_chunks(4096, 9, [(0u64, own)].into(), 64, ChunkHash::of)
                        .0
                        .encode();
                holder
                    .meta
                    .set_manifest_dirty(g, None, &manifest, 9, None, &[own])
                    .unwrap();
            }
            let (close, _, _) = back_close(&mut holder, true);
            let MutateOp::SetManifest { ino, .. } = close else {
                unreachable!()
            };
            let rid = Rid {
                node: 2,
                incarnation: 1,
                seq: 1,
            };
            let records =
                constellation_meta::execute_mutate(&holder.meta, &close, Some(rid)).unwrap();
            let accepted = MutateOutcome::Accepted { epoch: 1, records };
            let own = holder.core.own_chunks_for(
                2,
                rid,
                &accepted,
                (0, &constellation_meta::Position::ZERO),
                &holder.meta,
            );
            if blocked {
                assert_eq!(own, OwnChunks::Upload(vec![ino]));
            } else {
                assert_eq!(own, OwnChunks::Streamed(vec![ino]));
            }
        }
    }

    /// Node 2's close of a file of node 1's, accepted by a backed node 1
    /// on a base node 2 has not applied, answered `Streamed`: it waits for
    /// the log (`AwaitingLog`) with the safety timer armed.
    fn streamed_awaiting_log() -> (Harness, Rid, Vec<LogRecord>) {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let (op, _, created) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = op else {
            unreachable!()
        };
        let rid = requester.rid(1);
        let closed = constellation_meta::execute_mutate(&holder.meta, &op, Some(rid)).unwrap();
        // Node 1's create, then node 2's close, as the segment that will
        // carry them.
        let records: Vec<LogRecord> = created.into_iter().chain(closed).collect();
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        let [(1, PeerMsg::MutateRequest { req, .. })] = sends(&out)[..] else {
            panic!("expected a forward: {out:?}")
        };
        let out = requester.step(Event::Peer {
            from: 1,
            msg: PeerMsg::MutateReply {
                req: *req,
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
                own_chunks: OwnChunks::Streamed(vec![ino]),
                own_rows: None,
            },
        });
        assert_eq!(
            requester.core.clients().collect::<Vec<_>>(),
            vec![(rid, ClientPhase::AwaitingLog)]
        );
        assert!(uploads_awaited(&out).is_empty(), "the stream answers");
        assert_eq!(timers(&out, TimerKind::OwnRecordWait).len(), 1);
        (requester, rid, records)
    }

    /// Chunk close-stall-followup: a requester that has applied the
    /// reply's base installs the reply at once and never reads its
    /// `own_chunks`, so the holder skips working them out; one that has
    /// not still gets them — for its own close, and for the first one,
    /// still deferred before it in the position it will observe. Node 2
    /// closes `g1` and `g2` (shipped files, so the base is the floor, 5)
    /// with a chunk pending on it each.
    #[test]
    fn a_reply_the_requester_installs_at_once_skips_the_walk() {
        let (mut holder, _requester) = holding(AckPolicy::Local, Vec::new());
        let files: Vec<u64> = ["g1", "g2"]
            .into_iter()
            .map(|name| {
                let create = holder.create(name);
                constellation_meta::execute_mutate(&holder.meta, &create, None).unwrap();
                let MutateOp::Create { ino, .. } = create else {
                    unreachable!()
                };
                ino
            })
            .collect();
        let rows: Vec<u64> = holder
            .meta
            .take_journal_grouped(usize::MAX)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b.into_iter().map(|(s, _)| s))
            .collect();
        holder.meta.ack_journal_rows_at(&rows, 5).unwrap();
        holder.core.shipped_floor = 5;
        for (i, (ino, applied, want)) in [
            (files[0], 5, OwnChunks::None),
            (files[1], 4, OwnChunks::Upload(files.clone())),
        ]
        .into_iter()
        .enumerate()
        {
            let chunk = ChunkHash::of(&ino.to_be_bytes());
            holder.meta.enroll_remote_chunks(ino, &[chunk], 2).unwrap();
            let manifest =
                Manifest::from_sparse_chunks(4096, 14, [(0u64, chunk)].into(), 64, ChunkHash::of)
                    .0
                    .encode();
            let out = holder.step(Event::Peer {
                from: 2,
                msg: PeerMsg::MutateRequest {
                    req: OpId(40 + i as u64),
                    rid: Rid {
                        node: 2,
                        incarnation: 1,
                        seq: 1 + i as u64,
                    },
                    op: MutateOp::SetManifest {
                        ino,
                        base_manifest: None,
                        manifest,
                        size: 14,
                        mtime_ns: None,
                    },
                    acked_through: 0,
                    deps: constellation_meta::Position::ZERO,
                    applied,
                    tag: Default::default(),
                },
            });
            let [(2, reply @ PeerMsg::MutateReply { base, .. })] = sends(&out)[..] else {
                panic!("one reply: {out:?}")
            };
            assert_eq!(*base, Some(5));
            assert_eq!(own_chunks_of(reply).0, want, "applied {applied}");
        }
    }

    /// Chunk close-stall-followup, found on AWS (probes of 4–10 s): node
    /// 2's close answered at once leaves its chunk held there and its
    /// record deferred here, below every later segment's `through`. A
    /// later op of node 2's that observes this journal's position — a
    /// refusal, or an acceptance that completes through the log — waited
    /// in every read after it until the session watermark's TTL, though
    /// the op itself names nothing of the close. Node 1 now names the
    /// close's inode for the position, and node 2 uploads it.
    #[test]
    fn an_observed_position_behind_a_held_close_asks_for_its_upload() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let (close, _, _) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = close else {
            unreachable!()
        };
        let held = Rid {
            node: 2,
            incarnation: 1,
            seq: 90,
        };
        constellation_meta::execute_mutate(&holder.meta, &close, Some(held)).unwrap();
        // A refusal.
        let rid = requester.rid(1);
        let unlink = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "missing".into(),
        };
        let (reply, out) = forward(&mut holder, &mut requester, rid, unlink);
        let (own, outcome) = own_chunks_of(&reply);
        assert!(matches!(outcome, MutateOutcome::Errno(_)), "{reply:?}");
        assert_eq!(own, OwnChunks::Upload(vec![ino]));
        assert_eq!(uploads_awaited(&out), vec![ino]);
        // A chmod of node 1's unshipped `x`, unrelated to the close, and
        // accepted on a base node 2 lacks (`AwaitingLog`).
        let create = holder.create("x");
        let MutateOp::Create { ino: x, .. } = create else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&holder.meta, &create, None).unwrap();
        let rid = requester.rid(2);
        let chmod = MutateOp::Setattr {
            ino: x,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
        };
        let (reply, out) = forward(&mut holder, &mut requester, rid, chmod);
        assert_eq!(own_chunks_of(&reply).0, OwnChunks::Upload(vec![ino]));
        assert!(requester
            .core
            .clients()
            .any(|(r, phase)| r == rid && phase == ClientPhase::AwaitingLog));
        assert_eq!(uploads_awaited(&out), vec![ino]);
        // Reported up: nothing more to name.
        holder
            .meta
            .ack_remote_chunks(&[ChunkHash::of(b"held on node 2")])
            .unwrap();
        let rid = requester.rid(3);
        let unlink = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "missing2".into(),
        };
        let (reply, out) = forward(&mut holder, &mut requester, rid, unlink);
        assert_eq!(own_chunks_of(&reply).0, OwnChunks::None);
        assert!(uploads_awaited(&out).is_empty());
    }

    /// Chunk close-stall-followup: an acknowledgement parked for
    /// durability keeps what its transaction waits for between hold
    /// intervals, and works it out again only every `own_record_wait_ms`.
    /// A kept answer can only be too large (node 2 reported its chunk up
    /// meanwhile): the refresh then says `None`.
    #[test]
    fn a_parked_acknowledgement_keeps_its_blockers_until_the_refresh() {
        let (mut holder, _requester) = holding(AckPolicy::S3, Vec::new());
        let (op, _, _) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = op else {
            unreachable!()
        };
        let request = PeerMsg::MutateRequest {
            req: OpId(50),
            rid: Rid {
                node: 2,
                incarnation: 1,
                seq: 1,
            },
            op,
            acked_through: 0,
            deps: constellation_meta::Position::ZERO,
            applied: 0,
            tag: Default::default(),
        };
        let tick = |holder: &mut Harness, ms: u64| {
            let out = holder.step(Event::Peer {
                from: 2,
                msg: request.clone(),
            });
            let [held] = timers(&out, TimerKind::HeldReply)[..] else {
                panic!("re-attached: {out:?}")
            };
            holder.advance(ms);
            let out = holder.step(Event::Timer { id: held });
            let [(2, reply)] = sends(&out)[..] else {
                panic!("a held reply: {out:?}")
            };
            own_chunks_of(reply).0
        };
        let out = holder.step(Event::Peer {
            from: 2,
            msg: request.clone(),
        });
        let [(2, reply)] = sends(&out)[..] else {
            panic!("held for upload: {out:?}")
        };
        assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![ino]));
        holder
            .meta
            .ack_remote_chunks(&[ChunkHash::of(b"held on node 2")])
            .unwrap();
        let hold = holder.core.cfg.recall_hold_ms;
        assert_eq!(
            tick(&mut holder, hold),
            OwnChunks::Upload(vec![ino]),
            "kept"
        );
        let wait = holder.core.cfg.own_record_wait_ms;
        assert_eq!(tick(&mut holder, wait), OwnChunks::None, "refreshed");
    }

    /// Chunk metered-own-rows: node 1 holds unbacked; `name` is created
    /// and shipped at seq 5, and node 2 applied that segment, so a close
    /// of it by node 2 is answered on a base it has. Returns the inode.
    fn shipped_file(holder: &mut Harness, requester: &Harness, name: &str) -> u64 {
        let create = holder.create(name);
        let MutateOp::Create { ino, .. } = create else {
            unreachable!()
        };
        let created = constellation_meta::execute_mutate(&holder.meta, &create, None).unwrap();
        ship_at(holder, Some(requester), 5);
        let _ = created;
        ino
    }

    /// Node 1 ships what its plan lets ship as segment `seq` (the deferred
    /// rows stay); `requester`, if given, applies the segment. Returns the
    /// rows shipped.
    fn ship_at(holder: &mut Harness, requester: Option<&Harness>, seq: u64) -> Vec<u64> {
        let batch: Vec<(u64, LogRecord)> = holder
            .meta
            .take_journal_grouped(usize::MAX)
            .unwrap()
            .into_iter()
            .flat_map(|(_, b)| b)
            .collect();
        let rows: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
        let records: Vec<LogRecord> = batch.into_iter().map(|(_, r)| r).collect();
        let through = Replica::journal_through_after(&holder.meta, &rows).unwrap();
        holder.meta.ack_journal_rows_at(&rows, seq).unwrap();
        holder.core.shipped_floor = seq;
        holder.core.ship.head_seq = seq;
        holder.core.ship.next_seq = seq + 1;
        if let Some(r) = requester {
            Replica::apply_segment(&r.meta, seq, 1, through, &rows, &[], &records).unwrap();
        }
        rows
    }

    fn own_rows_of(msg: &PeerMsg) -> Option<constellation_meta::OwnRows> {
        match msg {
            PeerMsg::MutateReply { own_rows, .. } => own_rows.clone(),
            other => panic!("not a reply: {other:?}"),
        }
    }

    /// Node 2's `back` close of `ino`, its chunk pending on node 2.
    fn held_close(holder: &Harness, ino: u64) -> MutateOp {
        let chunk = ChunkHash::of(&ino.to_be_bytes());
        holder.meta.enroll_remote_chunks(ino, &[chunk], 2).unwrap();
        MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest: Manifest::from_sparse_chunks(
                4096,
                14,
                [(0u64, chunk)].into(),
                64,
                ChunkHash::of,
            )
            .0
            .encode(),
            size: 14,
            mtime_ns: None,
        }
    }

    fn unlink_missing(name: &str) -> MutateOp {
        MutateOp::Unlink {
            parent: ROOT_INO,
            name: name.into(),
        }
    }

    /// Chunk metered-own-rows (coordinator decision): node 2's close of
    /// `g`, answered at once on a base it has, keeps its chunk held there
    /// and its rows deferred here, below every later segment's `through`.
    /// A refusal of node 2's then observes this journal's position. Node 1
    /// still names `g` for it (a requester that lacked the close would
    /// need it), but also says the rows through the position are node 2's
    /// own: the close, which node 2 carries as a shadow, and the refusal,
    /// which changes nothing. Node 2 excuses them: it uploads nothing, and
    /// its reads wait for nothing. Before, it uploaded `g` (`pending 0`
    /// on a metered network).
    ///
    /// Then another node's op interleaves below the position. One that
    /// ships on its own (node 3's create) is waited for, without an upload,
    /// until its segment arrives (shipped ahead of the close: `through`
    /// still stops below the close). One deferred behind the close (node
    /// 3's chmod of `g`) is waited for too, and only node 2's upload
    /// releases it: `g` is uploaded.
    #[test]
    fn a_position_behind_only_own_rows_is_excused() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let g = shipped_file(&mut holder, &requester, "g");
        let close = requester.rid(1);
        let op = held_close(&holder, g);
        let (reply, out) = forward(&mut holder, &mut requester, close, op);
        assert!(matches!(
            own_chunks_of(&reply).1,
            MutateOutcome::Accepted { .. }
        ));
        assert_eq!(
            own_chunks_of(&reply).0,
            OwnChunks::None,
            "installed at once"
        );
        assert!(uploads_awaited(&out).is_empty());
        assert_eq!(requester.core.clients().count(), 0, "answered: {out:?}");

        let refusal = requester.rid(2);
        let (reply, out) = forward(&mut holder, &mut requester, refusal, unlink_missing("m1"));
        assert!(
            matches!(own_chunks_of(&reply).1, MutateOutcome::Errno(_)),
            "{reply:?}"
        );
        assert_eq!(own_chunks_of(&reply).0, OwnChunks::Upload(vec![g]));
        let rows = own_rows_of(&reply).expect("worked out");
        assert!(rows.others.is_empty(), "{rows:?}");
        let txs: Vec<(Rid, bool, Vec<u64>)> = rows
            .txs
            .iter()
            .map(|t| (t.rid, t.effect, t.waits.clone()))
            .collect();
        assert_eq!(txs, vec![(close, true, vec![g]), (refusal, false, vec![])]);
        assert!(
            uploads_awaited(&out).is_empty(),
            "nothing to upload: {out:?}"
        );
        assert_eq!(requester.core.stats.own_rows_excused, 1);
        assert_eq!(requester.core.stats.own_record_uploads, 0);
        assert!(
            requester.meta.session().reaches_observed(),
            "only node 2's own rows are behind the position"
        );

        // Node 3's create: ships on its own.
        let create = holder.create("h3");
        let rid3 = |seq| Rid {
            node: 3,
            incarnation: 1,
            seq,
        };
        constellation_meta::execute_mutate(&holder.meta, &create, Some(rid3(1))).unwrap();
        let rid = requester.rid(3);
        let (reply, out) = forward(&mut holder, &mut requester, rid, unlink_missing("m2"));
        assert_eq!(own_rows_of(&reply).unwrap().others, Vec::<u64>::new());
        assert!(uploads_awaited(&out).is_empty(), "{out:?}");
        assert!(
            !requester.meta.session().reaches_observed(),
            "node 3's create is owed"
        );
        let shipped = ship_at(&mut holder, Some(&requester), 6);
        assert!(!shipped.is_empty());
        assert!(
            requester.meta.session().reaches_observed(),
            "node 3's rows arrived, shipped ahead of the deferred close"
        );

        // Node 3's chmod of `g`: deferred behind node 2's close.
        let chmod = MutateOp::Setattr {
            ino: g,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
        };
        constellation_meta::execute_mutate(&holder.meta, &chmod, Some(rid3(2))).unwrap();
        let rid = requester.rid(4);
        let (reply, out) = forward(&mut holder, &mut requester, rid, unlink_missing("m3"));
        assert_eq!(own_rows_of(&reply).unwrap().others, vec![g]);
        assert_eq!(
            uploads_awaited(&out),
            vec![g],
            "only the upload releases it"
        );
        assert!(!requester.meta.session().reaches_observed());
    }

    /// Chunk metered-own-rows (the follow-up review's dedup skip): node
    /// 2's create `x` is accepted but the reply is lost; node 1 ships it
    /// (its journal empties). Node 2's close of `g` is then answered at
    /// once and deferred here. Node 2's forward of `x` times out and is
    /// sent again: node 1 answers from its dedup, on a base node 2 has
    /// applied. Node 2 has `x` from the log already, so it installs
    /// nothing and observes the reply's position, behind the deferred
    /// close. With the base-covered skip applied to the dedup answer it
    /// carried no `own_chunks` nor `own_rows`: node 2's reads waited for
    /// the close until the watermark's TTL (10 s).
    #[test]
    fn a_dedup_answer_still_says_what_its_position_waits_for() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let g = shipped_file(&mut holder, &requester, "g");
        let x = requester.rid(1);
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid: x,
            op: requester.create("x"),
            tag: Default::default(),
        });
        let [(1, first)] = sends(&out)[..] else {
            panic!("a forward: {out:?}")
        };
        let first = first.clone();
        let timeout = timers(&out, TimerKind::ForwardTimeout)[0];
        let lost = holder.step(Event::Peer {
            from: 2,
            msg: first,
        });
        assert_eq!(replies_to(&lost, 2).len(), 1, "answered, then lost");
        ship_at(&mut holder, Some(&requester), 6);
        let close = requester.rid(2);
        let op = held_close(&holder, g);
        let (reply, _) = forward(&mut holder, &mut requester, close, op);
        assert!(matches!(
            own_chunks_of(&reply).1,
            MutateOutcome::Accepted { .. }
        ));
        assert!(requester.meta.session().reaches_observed());
        // The retry.
        requester.advance(requester.core.cfg.forward_timeout_ms);
        let mut out = requester.step(Event::Timer { id: timeout });
        for _ in 0..4 {
            if !sends(&out).is_empty() {
                break;
            }
            let backoff = timers(&out, TimerKind::ForwardBackoff);
            let [id] = backoff[..] else {
                panic!("a backoff or a retry: {out:?}")
            };
            requester.advance(requester.core.cfg.forward_backoff_ms * 4);
            out = requester.step(Event::Timer { id });
        }
        let [(1, retry)] = sends(&out)[..] else {
            panic!("the retry: {out:?}")
        };
        let retry = retry.clone();
        let hits = holder.core.stats.forward_dedup_hits;
        let answer = holder.step(Event::Peer {
            from: 2,
            msg: retry,
        });
        assert_eq!(holder.core.stats.forward_dedup_hits, hits + 1);
        let [reply] = &replies_to(&answer, 2)[..] else {
            panic!("one reply: {answer:?}")
        };
        assert!(
            own_rows_of(reply).is_some(),
            "worked out for a dedup answer: {reply:?}"
        );
        let out = requester.step(Event::Peer {
            from: 1,
            msg: reply.clone(),
        });
        assert_eq!(replies(&out).len(), 1, "x answered: {out:?}");
        assert!(uploads_awaited(&out).is_empty());
        assert!(
            requester.meta.session().reaches_observed(),
            "node 2's reads wait for nothing: the close behind the position is its own"
        );
    }

    fn replies_to(actions: &[Action], to: NodeId) -> Vec<PeerMsg> {
        sends(actions)
            .into_iter()
            .filter(|(n, m)| *n == to && matches!(m, PeerMsg::MutateReply { .. }))
            .map(|(_, m)| m.clone())
            .collect()
    }

    /// Chunk metered-own-rows (OVH run 31): after a deposition, a
    /// stranded `back` close of node 2's is replayed through the new
    /// holder, which answers it on a base node 2 lacks and says its stream
    /// carries the transaction (`Streamed`). It may have carried it
    /// already, before node 2's recovery rolled it back, and the stream
    /// does not resend: each replay waited out the 10 s safety timer
    /// before its upload, one after another. A replay uploads at once; a
    /// client's op still trusts the stream.
    #[test]
    fn a_replay_answered_streamed_uploads_at_once() {
        for replay in [true, false] {
            let (_holder, mut requester) = pair();
            let rid = requester.rid(1);
            let ino = 77;
            let op = MutateOp::SetManifest {
                ino,
                base_manifest: None,
                manifest: vec![1, 2, 3],
                size: 3,
                mtime_ns: None,
            };
            let out = if replay {
                requester
                    .meta
                    .queue_replay(rid, &op, &Default::default())
                    .unwrap();
                let cfg = requester.core.cfg.clone();
                requester.core = Core::new(cfg);
                let mut out = Vec::new();
                requester
                    .core
                    .start(requester.now, &requester.meta, &mut out);
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
                let drain = timers(&out, TimerKind::ReplayDrain)[0];
                requester.step(Event::Timer { id: drain })
            } else {
                requester.step(Event::Submit {
                    policy: Policy::Client,
                    rid,
                    op,
                    tag: Default::default(),
                })
            };
            let req = sends(&out)
                .into_iter()
                .find_map(|(to, m)| match m {
                    PeerMsg::MutateRequest { req, rid: r, .. } if to == 1 && *r == rid => {
                        Some(*req)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| panic!("forwarded: {out:?}"));
            let out = requester.step(Event::Peer {
                from: 1,
                msg: PeerMsg::MutateReply {
                    req,
                    outcome: MutateOutcome::Accepted {
                        epoch: 3,
                        records: Vec::new(),
                    },
                    base: None,
                    position: constellation_meta::Position {
                        seq: 9,
                        pending: Some(constellation_meta::JournalPos { epoch: 3, jseq: 55 }),
                        streams: Default::default(),
                    },
                    gen: 0,
                    own_chunks: OwnChunks::Streamed(vec![ino]),
                    own_rows: None,
                },
            });
            assert!(requester
                .core
                .clients()
                .any(|(r, phase)| r == rid && phase == ClientPhase::AwaitingLog));
            if replay {
                assert_eq!(uploads_awaited(&out), vec![ino], "a replay: {out:?}");
            } else {
                assert!(uploads_awaited(&out).is_empty(), "a client's op: {out:?}");
                assert_eq!(timers(&out, TimerKind::OwnRecordWait).len(), 1);
            }
        }
    }

    /// Chunk metered-own-rows (review nit): an observed position's upload
    /// is repeated every `own_record_wait_ms` while the watermark stays
    /// unreached (a pass that failed), and stops once it is reached.
    #[test]
    fn an_observed_upload_is_repeated_until_the_watermark_is_reached() {
        let (mut holder, mut requester) = holding(AckPolicy::Local, Vec::new());
        let (close, _, _) = back_close(&mut holder, true);
        let MutateOp::SetManifest { ino, .. } = close else {
            unreachable!()
        };
        let held = Rid {
            node: 2,
            incarnation: 7,
            seq: 90,
        };
        constellation_meta::execute_mutate(&holder.meta, &close, Some(held)).unwrap();
        let rid = requester.rid(1);
        let (_, out) = forward(&mut holder, &mut requester, rid, unlink_missing("m"));
        assert_eq!(
            uploads_awaited(&out),
            vec![ino],
            "an earlier incarnation's close"
        );
        let [t] = timers(&out, TimerKind::ObservedUpload)[..] else {
            panic!("the repeat timer: {out:?}")
        };
        requester.advance(requester.core.cfg.own_record_wait_ms);
        let out = requester.step(Event::Timer { id: t });
        assert_eq!(uploads_awaited(&out), vec![ino], "repeated");
        let [t] = timers(&out, TimerKind::ObservedUpload)[..] else {
            panic!("armed again: {out:?}")
        };
        requester.meta.session().advance(
            1_000,
            Some(constellation_meta::JournalPos { epoch: 9, jseq: 1 }),
        );
        requester.advance(requester.core.cfg.own_record_wait_ms);
        let out = requester.step(Event::Timer { id: t });
        assert!(uploads_awaited(&out).is_empty(), "reached: {out:?}");
        assert!(timers(&out, TimerKind::ObservedUpload).is_empty());
    }

    /// Chunk close-stall-followup: the holder's cost of `own_chunks_for`
    /// per forwarded op, measured as the whole core step that answers it.
    /// A holder with a 10,000-transaction unshipped journal (its own
    /// creates), node 2 a metered `back` forwarder with one close's chunk
    /// still pending here (deferred), node 7 with 1,000 pending marks of
    /// its own, and node 3 with none. Run in release:
    /// `cargo test --release -p constellation-authority --lib
    /// own_chunks::cost -- --ignored --nocapture`.
    mod cost {
        use super::*;
        use std::time::{Duration, Instant};

        const JOURNAL: usize = 10_000;
        const OPS: usize = 200;

        fn request(req: u64, rid: Rid, op: MutateOp) -> PeerMsg {
            PeerMsg::MutateRequest {
                req: OpId(req),
                rid,
                op,
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
                applied: 0,
                tag: Default::default(),
            }
        }

        fn rid_of(node: NodeId, seq: u64) -> Rid {
            Rid {
                node,
                incarnation: 1,
                seq,
            }
        }

        fn chmod(ino: u64, mode: u32) -> MutateOp {
            MutateOp::Setattr {
                ino,
                mode: Some(mode),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            }
        }

        fn create_in(meta: &Meta, parent: u64, name: &str) -> MutateOp {
            MutateOp::Create {
                parent,
                name: name.into(),
                ino: meta.allocate_ino(parent).unwrap(),
                mode: 0o644,
                uid: 0,
                gid: 0,
            }
        }

        /// The holder as described above. Returns it, the inode node 2's
        /// held close wrote, and a shipped directory nothing unshipped
        /// touches.
        fn loaded(policy: AckPolicy) -> (Harness, u64, u64) {
            let (mut holder, _requester) = holding(policy, Vec::new());
            let dir = MutateOp::Mkdir {
                parent: ROOT_INO,
                name: "rd".into(),
                ino: holder.meta.allocate_ino(ROOT_INO).unwrap(),
                mode: 0o755,
                uid: 0,
                gid: 0,
            };
            let MutateOp::Mkdir { ino: rd, .. } = dir else {
                unreachable!()
            };
            constellation_meta::execute_mutate(&holder.meta, &dir, None).unwrap();
            let (close, _, _) = back_close(&mut holder, true);
            let MutateOp::SetManifest { ino: f, .. } = close else {
                unreachable!()
            };
            // Ship the mkdir and f's create; the close stays deferred.
            let rows: Vec<u64> = holder
                .meta
                .take_journal_grouped(usize::MAX)
                .unwrap()
                .into_iter()
                .flat_map(|(_, b)| b.into_iter().map(|(s, _)| s))
                .collect();
            holder.meta.ack_journal_rows_at(&rows, 1).unwrap();
            constellation_meta::execute_mutate(&holder.meta, &close, Some(rid_of(2, 1))).unwrap();
            for i in 0..JOURNAL {
                let op = create_in(&holder.meta, ROOT_INO, &format!("h{i}"));
                constellation_meta::execute_mutate(&holder.meta, &op, None).unwrap();
            }
            for i in 0..1_000u64 {
                let ino = 1_000_000 + i;
                holder
                    .meta
                    .enroll_remote_chunks(ino, &[ChunkHash::of(&ino.to_be_bytes())], 7)
                    .unwrap();
            }
            (holder, f, rd)
        }

        fn report(what: &str, mut times: Vec<Duration>) {
            times.sort();
            let total: Duration = times.iter().sum();
            eprintln!(
                "COST {what}: n={} median={:?} mean={:?} p90={:?} max={:?}",
                times.len(),
                times[times.len() / 2],
                total / times.len() as u32,
                times[times.len() * 9 / 10],
                times[times.len() - 1],
            );
        }

        #[test]
        #[ignore = "benchmark; run in release with --ignored --nocapture"]
        fn per_forwarded_op_core_step() {
            let (mut holder, f, rd) = loaded(AckPolicy::Local);
            let mut req = 100;
            let mut seq = 10;
            let mut step = |holder: &mut Harness, from: NodeId, op: MutateOp| {
                req += 1;
                seq += 1;
                let msg = request(req, rid_of(from, seq), op);
                let t = Instant::now();
                let out = holder.step(Event::Peer { from, msg });
                let took = t.elapsed();
                assert!(!sends(&out).is_empty(), "answered: {out:?}");
                (took, out)
            };
            // Node 3 (nothing pending), creating in the shipped directory.
            let mut t3 = Vec::new();
            for i in 0..OPS {
                let op = create_in(&holder.meta, rd, &format!("n3-{i}"));
                t3.push(step(&mut holder, 3, op).0);
            }
            report("node 3 (nothing pending), create in rd", t3);
            // Node 2 (its close pending), creating in the shipped directory:
            // the reply's base is covered.
            let mut t2 = Vec::new();
            for i in 0..OPS {
                let op = create_in(&holder.meta, rd, &format!("n2-{i}"));
                t2.push(step(&mut holder, 2, op).0);
            }
            report("node 2 (close pending), create in rd (base covered)", t2);
            // Node 2, chmod of its held file: depends on the close.
            let mut tc = Vec::new();
            for i in 0..OPS {
                let (took, out) = step(&mut holder, 2, chmod(f, 0o600 + (i as u32 % 8)));
                let [(2, reply)] = sends(&out)[..] else {
                    panic!("{out:?}")
                };
                assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![f]));
                tc.push(took);
            }
            report("node 2 (close pending), chmod of the held file", tc);
            // Chunk metered-own-rows: node 2's refusals (an unlink of a
            // name that does not exist), which observe the position.
            let mut tr = Vec::new();
            for i in 0..OPS {
                let op = MutateOp::Unlink {
                    parent: rd,
                    name: format!("missing-{i}"),
                };
                let (took, out) = step(&mut holder, 2, op);
                let [(2, reply)] = sends(&out)[..] else {
                    panic!("{out:?}")
                };
                assert!(matches!(own_chunks_of(reply).1, MutateOutcome::Errno(_)));
                assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![f]));
                tr.push(took);
            }
            report("node 2 (close pending), refusal", tr);
            // The first answer after the plan's inputs change walks again:
            // node 7 reports one chunk up before each refusal.
            let mut tw = Vec::new();
            for i in 0..20u64 {
                let ino = 1_000_000 + i;
                holder
                    .meta
                    .ack_remote_chunks(&[ChunkHash::of(&ino.to_be_bytes())])
                    .unwrap();
                let op = MutateOp::Unlink {
                    parent: rd,
                    name: format!("cold-{i}"),
                };
                tw.push(step(&mut holder, 2, op).0);
            }
            report("node 2, refusal after a pending row changed (re-walk)", tw);
        }

        #[test]
        #[ignore = "benchmark; run in release with --ignored --nocapture"]
        fn per_parked_reply_tick() {
            let (mut holder, f, _rd) = loaded(AckPolicy::S3);
            let mut requests = Vec::new();
            for i in 0..20u64 {
                let msg = request(200 + i, rid_of(2, 20 + i), chmod(f, 0o600 + (i as u32 % 8)));
                let out = holder.step(Event::Peer {
                    from: 2,
                    msg: msg.clone(),
                });
                // Answered at once: `Held` asking for the upload.
                let [(2, reply)] = sends(&out)[..] else {
                    panic!("held for upload: {out:?}")
                };
                assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![f]));
                requests.push(msg);
            }
            let mut ticks = Vec::new();
            for _round in 0..10 {
                // Each retry re-attaches; the next tick answers it.
                let mut held = Vec::new();
                for msg in &requests {
                    let out = holder.step(Event::Peer {
                        from: 2,
                        msg: msg.clone(),
                    });
                    assert!(sends(&out).is_empty(), "re-attached: {out:?}");
                    held.extend(timers(&out, TimerKind::HeldReply));
                }
                assert_eq!(held.len(), 20, "parked for durability");
                holder.advance(holder.core.cfg.recall_hold_ms);
                for id in held {
                    let t = Instant::now();
                    let out = holder.step(Event::Timer { id });
                    ticks.push(t.elapsed());
                    let [(2, reply)] = sends(&out)[..] else {
                        panic!("a held reply: {out:?}")
                    };
                    assert_eq!(own_chunks_of(reply).0, OwnChunks::Upload(vec![f]));
                }
            }
            report("parked ack=s3 reply, one hold tick", ticks);
        }
    }
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
        tag: Default::default(),
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

/// Ship everything the holder journaled so far as segment `seq`, through
/// its last row (what a real ship's envelope says).
fn holder_ship(holder: &mut Harness, seq: Seq, max_rows: usize) -> (Vec<u8>, u64) {
    let batch = Replica::take_journal(&holder.meta, max_rows).unwrap();
    let records: Vec<_> = batch.iter().map(|(_, r)| r.clone()).collect();
    let rows: Vec<u64> = batch.iter().map(|(s, _)| *s).collect();
    let through = *rows.last().expect("something to ship");
    Replica::ack_journal(&holder.meta, &rows, seq, None).unwrap();
    holder.core.ship.next_seq = seq + 1;
    holder.core.ship.head_seq = seq;
    let payload = crate::segment::encode(1, 1, through, &rows, &[], &records).unwrap();
    (payload, through)
}

fn submit_create(holder: &mut Harness, seq: u64, name: &str) {
    let rid = holder.rid(seq);
    let op = holder.create(name);
    let out = holder.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
}

fn segment_frame(req: OpId, n: u64, seq: Seq, payload: Vec<u8>) -> PeerMsg {
    PeerMsg::LogStream {
        req,
        n,
        epoch: 1,
        head: seq,
        segment: Some((seq, payload)),
    }
}

/// The small-file fix: the holder streams a transaction ahead of S3 right
/// after shipping the segment it follows, and the batch (another QUIC
/// stream) often overtakes that segment. It waits for it and installs
/// then — it used to be dropped, and a forward whose reply waited for it
/// waited for the log instead (a non-owner's close: one S3 round trip
/// more).
#[test]
fn a_stream_ahead_batch_that_overtakes_its_segment_waits_for_it() {
    let (mut holder, mut sub, req) = stream_pair();
    submit_create(&mut holder, 1, "a");
    let (p1, through) = holder_ship(&mut holder, 1, 100);
    submit_create(&mut holder, 2, "b");
    let txs = Replica::journal_txs_from(&holder.meta, through + 1, 100);
    assert_eq!(txs.len(), 1);
    sub.core.ship.max_epoch = 1;
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 1,
            txs,
        },
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "b")
        .unwrap()
        .is_none());
    assert_eq!(sub.core.stats.streamed_dropped, 0, "kept, not dropped");
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 1, 1, p1),
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "a")
        .unwrap()
        .is_some());
    assert!(
        MetaStore::lookup(&sub.meta, ROOT_INO, "b")
            .unwrap()
            .is_some(),
        "installed once its segment was applied"
    );
    assert_eq!(sub.core.stats.streamed_installed, 1);
    assert_eq!(sub.core.stats.streamed_dropped, 0);
}

/// A segment that ships less than the stream already installed (the
/// holder shipped `b`; the stream gave us `b` and `c` already) must not
/// pull the stream's cursor back: the next batch (`d`) continues from `c`.
#[test]
fn a_segment_behind_the_stream_keeps_its_cursor() {
    let (mut holder, mut sub, req) = stream_pair();
    submit_create(&mut holder, 1, "a");
    let (p1, through) = holder_ship(&mut holder, 1, 100);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 1, 1, p1),
    });
    submit_create(&mut holder, 2, "b");
    submit_create(&mut holder, 3, "c");
    let txs = Replica::journal_txs_from(&holder.meta, through + 1, 100);
    assert_eq!(txs.len(), 2);
    let b_rows = txs[0].records.len();
    let txs_last = txs[1].last;
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 1,
            txs,
        },
    });
    assert_eq!(sub.core.stats.streamed_installed, 2);
    let after_c = txs_last;
    // The holder ships `b` alone as segment 2; then streams `d`.
    let (p2, _) = holder_ship(&mut holder, 2, b_rows);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 2, 2, p2),
    });
    submit_create(&mut holder, 4, "d");
    let txs: Vec<_> = Replica::journal_txs_from(&holder.meta, after_c + 1, 100);
    assert_eq!(txs.len(), 1);
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 2,
            txs,
        },
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "d")
        .unwrap()
        .is_some());
    assert_eq!(sub.core.stats.streamed_dropped, 0);
}

/// Fix "capture under an epoch hold": a subscriber holds `b` and `c`
/// from the stream; a restart re-derives its cursor from the log (below
/// them) and the holder streams from its start again — the replica is
/// the truth, and neither is applied a second time (flex-backup seed
/// 1230 re-applied a member's epoch journal on top of itself). A new
/// transaction after them still installs.
#[test]
fn a_restarted_subscriber_does_not_reinstall_what_it_holds() {
    let (mut holder, mut sub, req) = stream_pair();
    submit_create(&mut holder, 1, "a");
    let (p1, through) = holder_ship(&mut holder, 1, 100);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 1, 1, p1),
    });
    submit_create(&mut holder, 2, "b");
    submit_create(&mut holder, 3, "c");
    let txs = Replica::journal_txs_from(&holder.meta, through + 1, 100);
    assert_eq!(txs.len(), 2);
    let batch = PeerMsg::StreamAhead {
        epoch: 1,
        base: 1,
        txs: txs.clone(),
    };
    sub.step(Event::Peer {
        from: 1,
        msg: batch.clone(),
    });
    assert_eq!(sub.core.stats.streamed_installed, 2);
    let before = sub.meta.ns_dump().unwrap();
    // The restart: the cursor is the log's `through` again.
    sub.core.bk.ahead_next = Some((1, through + 1));
    sub.step(Event::Peer {
        from: 1,
        msg: batch,
    });
    assert_eq!(sub.core.stats.streamed_installed, 2, "not installed again");
    assert_eq!(sub.core.stats.streamed_held_already, 2);
    assert_eq!(sub.core.stats.streamed_dropped, 0);
    assert_eq!(
        sub.meta.ns_dump().unwrap(),
        before,
        "the replica is unchanged"
    );
    submit_create(&mut holder, 4, "d");
    let after_c = txs[1].last;
    let txs = Replica::journal_txs_from(&holder.meta, after_c + 1, 100);
    assert_eq!(txs.len(), 1);
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 1,
            txs,
        },
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "d")
        .unwrap()
        .is_some());
    assert_eq!(sub.core.stats.streamed_installed, 3);
}

/// EC2 campaign 6 (visibility-s3-latency): the stream carries each
/// transaction once. A subscriber that lost one (a batch that arrived
/// while a job had its cursor) used to drop every later batch too — none
/// followed its cursor — so it saw the writer's files only through S3
/// until the writer paused. The later batches wait for the segment that
/// closes the gap and are installed then; the stream is live again.
#[test]
fn stream_ahead_after_a_lost_transaction_resumes_once_the_log_closes_the_gap() {
    let (mut holder, mut sub, req) = stream_pair();
    submit_create(&mut holder, 1, "a");
    let (p1, through) = holder_ship(&mut holder, 1, 100);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 1, 1, p1),
    });
    // `b` is streamed but never reaches the subscriber.
    submit_create(&mut holder, 2, "b");
    let lost = Replica::journal_txs_from(&holder.meta, through + 1, 100);
    assert_eq!(lost.len(), 1);
    let b_rows = lost[0].records.len();
    // `c` arrives: it does not follow what the subscriber holds.
    submit_create(&mut holder, 3, "c");
    let c = Replica::journal_txs_from(&holder.meta, lost[0].last + 1, 100);
    assert_eq!(c.len(), 1);
    let c_last = c[0].last;
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 1,
            txs: c,
        },
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "c")
        .unwrap()
        .is_none());
    assert_eq!(sub.core.stats.streamed_dropped, 0, "kept, not dropped");
    // The log brings `b` (segment 2, through `b` only): `c` follows it now.
    let (p2, _) = holder_ship(&mut holder, 2, b_rows);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 2, 2, p2),
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "b")
        .unwrap()
        .is_some());
    assert!(
        MetaStore::lookup(&sub.meta, ROOT_INO, "c")
            .unwrap()
            .is_some(),
        "installed ahead of the log once the gap closed"
    );
    // And the stream continues from there: `d` installs at once.
    submit_create(&mut holder, 4, "d");
    let d = Replica::journal_txs_from(&holder.meta, c_last + 1, 100);
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::StreamAhead {
            epoch: 1,
            base: 2,
            txs: d,
        },
    });
    assert!(MetaStore::lookup(&sub.meta, ROOT_INO, "d")
        .unwrap()
        .is_some());
    assert_eq!(sub.core.stats.streamed_installed, 2);
    assert_eq!(sub.core.stats.streamed_dropped, 0);
}

/// A manifest whose chunk is still uploading on the node that forwarded
/// it (`Meta::enroll_remote_chunks`) is streamed ahead to that node — it
/// has the bytes, and its close may be waiting for the transaction — and
/// to nobody else until the chunk is reported up.
#[test]
fn stream_ahead_gives_a_pending_manifest_only_to_its_forwarder() {
    use constellation_fs_core::{ChunkHash, Manifest};
    let (mut holder, _sub2, _) = stream_pair();
    // A second subscriber, node 3.
    let served = holder.step(Event::Peer {
        from: 3,
        msg: PeerMsg::LogSubscribe {
            req: OpId(77),
            from: 1,
        },
    });
    assert!(!sends(&served).is_empty());
    let file = holder.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let chunk = ChunkHash::of(b"uploading on node 2");
    let manifest = Manifest::from_sparse_chunks(
        4096,
        19,
        [(0u64, chunk)].into_iter().collect(),
        64,
        ChunkHash::of,
    )
    .0
    .encode();
    holder
        .meta
        .enroll_remote_chunks(file.ino, &[chunk], 2)
        .unwrap();
    constellation_meta::execute_mutate(
        &holder.meta,
        &MutateOp::SetManifest {
            ino: file.ino,
            base_manifest: None,
            manifest,
            size: 19,
            mtime_ns: None,
        },
        None,
    )
    .unwrap();
    let tip = Replica::journal_tip(&holder.meta);
    // Who got the manifest (a `WriteManifest` record) in a batch.
    let manifest_to = |out: &[Action]| -> Vec<NodeId> {
        sends(out)
            .into_iter()
            .filter(|(_, m)| {
                matches!(m, PeerMsg::StreamAhead { txs, .. } if txs.iter().any(|t| t
                    .records
                    .iter()
                    .any(|r| matches!(r, LogRecord::WriteManifest { .. }))))
            })
            .map(|(to, _)| to)
            .collect()
    };
    let mut out = Vec::new();
    holder
        .core
        .stream_ahead(holder.now, 0, tip, &holder.meta, &mut out);
    assert_eq!(manifest_to(&out), vec![2], "only the forwarder: {out:?}");
    // Reported up: everyone else gets it now, the forwarder not twice.
    holder.meta.ack_remote_chunks(&[chunk]).unwrap();
    let mut out = Vec::new();
    holder
        .core
        .stream_ahead(holder.now, 0, tip, &holder.meta, &mut out);
    assert_eq!(manifest_to(&out), vec![3], "{out:?}");
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
    let out = sub.step(Event::S3 {
        op: runs[0].0,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    let _ = at_head(&mut sub, out);
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

/// `idle-cost` under host load (fix-flakes-4): a caught-up stream that
/// goes silent past the timeout leaves its rounds tailing S3, one GET of
/// the next sequence each (nothing is expected), not a full-width
/// speculative run of 404s per drop; a probe that hits is followed by a
/// full-width run, so the follower still reaches the head at once.
#[test]
fn a_dropped_caught_up_stream_tails_one_get_wide() {
    let (mut holder, mut sub, _req) = stream_pair();
    let full = sub.core.cfg.tail_width;
    assert!(full > 1, "the test needs a full width above one: {full}");
    let run_round = |sub: &mut Harness| -> Vec<(OpId, usize)> {
        sub.core.nudge(sub.now, &mut Vec::new());
        let poll = sub
            .core
            .timers
            .iter()
            .find(|(_, (t, _))| matches!(t, Timer::Poll))
            .map(|(id, _)| *id)
            .expect("poll timer");
        let mut out = sub.step(Event::Timer { id: poll });
        let upload = out.iter().find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        });
        if let Some(op) = upload {
            out.extend(sub.step(Event::UploadsDone {
                op,
                result: UploadResult::Done { held: 0 },
            }));
        }
        s3_ops(&out)
            .into_iter()
            .filter_map(|(op, r)| match r {
                S3Op::SegmentRun { width, .. } => Some((op, *width)),
                _ => None,
            })
            .collect()
    };
    let runs = run_round(&mut sub);
    assert_eq!(runs.len(), 1);
    let out = sub.step(Event::S3 {
        op: runs[0].0,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    let _ = at_head(&mut sub, out);
    // The holder falls silent (a heartbeat late under load): the
    // watchdog drops the caught-up stream.
    sub.advance(sub.core.cfg.stream_timeout_ms + 1);
    let watchdog = sub
        .core
        .timers
        .iter()
        .find(|(_, (t, _))| matches!(t, Timer::StreamWatchdog))
        .map(|(id, _)| *id)
        .expect("watchdog timer");
    sub.step(Event::Timer { id: watchdog });
    assert_eq!(sub.core.stats.stream_timeouts, 1);
    assert!(!sub.core.stream_view().live);
    // Each round in the stream's place probes one GET wide.
    for _ in 0..3 {
        sub.advance(1_000);
        let runs = run_round(&mut sub);
        assert_eq!(
            runs.iter().map(|(_, w)| *w).collect::<Vec<_>>(),
            vec![1],
            "a dropped caught-up stream's tail is one GET (full width {full})"
        );
        let out = sub.step(Event::S3 {
            op: runs[0].0,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let _ = at_head(&mut sub, out);
    }
    // The holder shipped meanwhile: the narrow probe hits, and the very
    // next step is a full-width run from past it.
    let payload = holder_segment(&mut holder, "a", 1);
    sub.advance(1_000);
    let runs = run_round(&mut sub);
    assert_eq!(runs.iter().map(|(_, w)| *w).collect::<Vec<_>>(), vec![1]);
    let out = sub.step(Event::S3 {
        op: runs[0].0,
        result: S3Result::SegmentRun(Ok(vec![(1, payload)])),
    });
    assert_eq!(sub.core.ship().next_seq, 2, "the hit was applied");
    let follow: Vec<usize> = s3_ops(&out)
        .into_iter()
        .filter_map(|(_, r)| match r {
            S3Op::SegmentRun { from: 2, width } => Some(*width),
            _ => None,
        })
        .collect();
    assert_eq!(
        follow,
        vec![full],
        "a saturated narrow probe runs full width"
    );
}

/// `visibility-after-burst`'s 17 tail GETs (fix-flakes-4): the
/// backstop probe of a caught-up stream is in flight when the stream
/// delivers the very segment it asks for (a tail leaves the cursor free,
/// so the stream applies it). The probe's answer is then a duplicate:
/// the round ends there, with no full-width follow-up run of 404s. A
/// probe that brings something new still runs full width after.
#[test]
fn a_probe_the_stream_overtook_is_not_saturated() {
    let (mut holder, mut sub, req) = stream_pair();
    let poll_round = |sub: &mut Harness| -> Vec<Action> {
        sub.core.nudge(sub.now, &mut Vec::new());
        let poll = sub
            .core
            .timers
            .iter()
            .find(|(_, (t, _))| matches!(t, Timer::Poll))
            .map(|(id, _)| *id)
            .expect("poll timer");
        let mut out = sub.step(Event::Timer { id: poll });
        let upload = out.iter().find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        });
        if let Some(op) = upload {
            out.extend(sub.step(Event::UploadsDone {
                op,
                result: UploadResult::Done { held: 0 },
            }));
        }
        out
    };
    let runs = |out: &[Action]| -> Vec<(OpId, Seq, usize)> {
        s3_ops(out)
            .into_iter()
            .filter_map(|(op, r)| match r {
                S3Op::SegmentRun { from, width } => Some((op, *from, *width)),
                _ => None,
            })
            .collect()
    };
    // The caught-up stream's backstop probe, one GET of segment 1.
    let probe = runs(&poll_round(&mut sub));
    assert_eq!(probe.len(), 1);
    assert_eq!((probe[0].1, probe[0].2), (1, 1));
    // Segment 1 lands in S3, and the stream delivers it while the GET is
    // out.
    let payload = holder_segment(&mut holder, "a", 1);
    sub.step(Event::Peer {
        from: 1,
        msg: segment_frame(req, 1, 1, payload.clone()),
    });
    assert_eq!(sub.core.stats.stream_applied, 1, "the stream applied 1");
    assert_eq!(sub.core.ship().next_seq, 2);
    // The probe answers with the segment the stream already applied.
    let out = sub.step(Event::S3 {
        op: probe[0].0,
        result: S3Result::SegmentRun(Ok(vec![(1, payload)])),
    });
    let out = at_head(&mut sub, out);
    assert_eq!(
        runs(&out),
        Vec::new(),
        "a probe the stream overtook ran again: {out:?}"
    );
    assert_eq!(sub.core.ship().next_seq, 2);
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

/// A holder that ended the stream (its epoch closed: it holds nothing
/// until its flush re-claims the lease) is not asked again until the log
/// moves — and a gossip hint of its next segment is that move, for a
/// subscriber whose own S3 tail cannot show it. Before, only the
/// subscriber's own cursor unparked it: without S3 it never followed the
/// holder again (`epoch-member-dies-with-chunk`'s B, after the close).
#[test]
fn a_parked_holder_is_asked_again_once_a_hint_shows_the_log_moved() {
    let (_holder, mut sub, req) = stream_pair();
    let subscribes = |out: &[Action]| {
        sends(out)
            .iter()
            .filter(|(to, m)| *to == 1 && matches!(m, PeerMsg::LogSubscribe { .. }))
            .count()
    };
    sub.step(Event::Peer {
        from: 1,
        msg: PeerMsg::LogStreamEnd {
            req,
            refused: false,
        },
    });
    assert_eq!(sub.core.stream_view().upstream, 0, "the stream is gone");
    // Parked: past every backoff, nothing new in the log, no resubscribe.
    sub.advance(sub.core.config().stream_retry_max_ms + 1);
    let out = sub.step(Event::Peers { links: Vec::new() });
    assert_eq!(subscribes(&out), 0, "asked a parked holder again: {out:?}");
    // The holder shipped its next segment (the flush): the hint names it.
    let next = sub.core.ship().next_seq;
    let out = sub.step(Event::Peer {
        from: 0,
        msg: PeerMsg::SegmentPublished {
            seq: next,
            epoch: 1,
        },
    });
    assert_eq!(
        subscribes(&out),
        1,
        "the log moved, but the holder is not asked again: {out:?}"
    );
    assert_eq!(sub.core.stream_view().upstream, 1);
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
                applied: 0,
                tag: Default::default(),
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

    /// M16: an unlink's records name only the parent and the entry, yet
    /// it changes the unlinked inode (its last link: the inode is gone).
    /// A read delegation on that inode must be recalled before the reply,
    /// or its holder keeps answering a strict open of the inode (a kernel
    /// dentry it still caches) from its stale replica after the unlink
    /// completed. The same for the file a rename replaces, and for a
    /// local unlink.
    #[test]
    fn an_unlink_or_a_replacing_rename_recalls_the_victims_grant() {
        let (mut h, ino) = holder_with_file();
        let g = grant_of(&ask(&mut h, 2, 7, ino));
        let out = forward(
            &mut h,
            3,
            9,
            1,
            MutateOp::Unlink {
                parent: ROOT_INO,
                name: "f".into(),
            },
        );
        assert!(mutate_replies(&out).is_empty(), "the reply waits: {out:?}");
        let rs = recalls(&out);
        assert!(
            rs.iter()
                .any(|(to, _, i, gr)| *to == 2 && *i == ino && *gr == g.id),
            "the unlinked file's grant is recalled: {out:?}"
        );
        // A rename over an existing name: the replaced file's grant.
        let (mut h, victim) = holder_with_file();
        let src = h.create("src");
        constellation_meta::execute_mutate(&h.meta, &src, None).unwrap();
        let g = grant_of(&ask(&mut h, 2, 7, victim));
        let out = forward(
            &mut h,
            3,
            9,
            1,
            MutateOp::Rename {
                parent: ROOT_INO,
                name: "src".into(),
                new_parent: ROOT_INO,
                new_name: "f".into(),
                noreplace: false,
            },
        );
        assert!(
            recalls(&out)
                .iter()
                .any(|(to, _, i, gr)| *to == 2 && *i == victim && *gr == g.id),
            "the replaced file's grant is recalled: {out:?}"
        );
        // The sequencer's own (core-executed) unlink.
        let (mut h, ino) = holder_with_file();
        grant_of(&ask(&mut h, 2, 7, ino));
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            rid,
            op: MutateOp::Unlink {
                parent: ROOT_INO,
                name: "f".into(),
            },
            policy: Policy::Client,
            tag: Default::default(),
        });
        assert!(replies(&out).is_empty(), "{out:?}");
        assert!(
            recalls(&out)
                .iter()
                .any(|(to, _, i, _)| *to == 2 && *i == ino),
            "{out:?}"
        );
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
            tag: Default::default(),
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
        let mut h = Harness {
            core,
            meta,
            now,
            manual_horizon: false,
        };
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

    /// A carrier-less activation (the claim resolution carried no lease)
    /// or a freeze of an epoch `members` are in.
    fn refusing_epoch(frozen: bool, members: Vec<NodeId>) -> Event {
        Event::Control {
            op: OpId(901),
            req: Control::Epoch {
                open: true,
                active: !frozen,
                frozen,
                flushing: false,
                base: 0,
                carrier: None,
                stale_below: 0,
                members,
            },
        }
    }

    fn refused(out: &[Action], rid: Rid) -> bool {
        replies(out).iter().any(|(r, reply)| {
            *r == rid
                && matches!(
                    reply,
                    ClientReply::Outcome(MutateOutcome::Errno(Code::ReadOnly))
                )
        })
    }

    /// Chunk close-stall-metered, review must-fix 2: a lone node without a
    /// lease, S3 cut. Its op entered the lease path (reading the lease, or
    /// waiting for an acquisition that never completes) before the
    /// carrier-less epoch activated. The engine's gate refuses new writes
    /// with `EROFS` from the activation on; the op already queued used to
    /// wait for its 2 × TTL deadline (120 s) and end in doubt (`EIO`),
    /// though it was never sent anywhere. Now it is refused `EROFS` at the
    /// activation, and an op routed afterwards is refused at once — not
    /// after a lease read on a cut S3. The same at a freeze.
    #[test]
    fn an_op_never_sent_is_refused_erofs_when_a_carrierless_epoch_activates() {
        for frozen in [false, true] {
            let mut h = Harness::new(2);
            // Reading the lease (the read never answers: S3 is cut).
            let learning = h.rid(1);
            let op = h.create("a");
            let out = h.step(Event::Submit {
                policy: Policy::Client,
                rid: learning,
                op,
                tag: Default::default(),
            });
            let (get, req) = s3_ops(&out)[0];
            assert!(matches!(req, S3Op::LeaseGet));
            // Waiting for the lease: its read said nobody holds it.
            let waiting = h.rid(2);
            let op = h.create("b");
            let out = h.step(Event::Submit {
                policy: Policy::Client,
                rid: waiting,
                op,
                tag: Default::default(),
            });
            let (get2, _) = s3_ops(&out)[0];
            h.step(Event::S3 {
                op: get2,
                result: S3Result::LeaseGet(Ok(None)),
            });
            let phases: Vec<_> = h.core.clients().collect();
            assert!(
                phases.contains(&(learning, ClientPhase::LearnHolder))
                    && phases
                        .iter()
                        .any(|(r, p)| *r == waiting && *p != ClientPhase::LearnHolder),
                "{phases:?}"
            );
            let out = h.step(refusing_epoch(frozen, vec![2]));
            assert!(refused(&out, learning), "frozen={frozen}: {out:?}");
            assert!(refused(&out, waiting), "frozen={frozen}: {out:?}");
            assert_eq!(h.core.clients().count(), 0);
            // The lease read's late answer finds no op.
            let out = h.step(Event::S3 {
                op: get,
                result: S3Result::LeaseGet(Ok(None)),
            });
            assert!(replies(&out).is_empty());
            // Routed after the activation: refused at once, no S3 read.
            let late = h.rid(3);
            let op = h.create("c");
            let out = h.step(Event::Submit {
                policy: Policy::Client,
                rid: late,
                op,
                tag: Default::default(),
            });
            assert!(refused(&out, late), "frozen={frozen}: {out:?}");
            assert!(s3_ops(&out).is_empty(), "{out:?}");
        }
    }

    /// An op that was sent to a holder before (here: forwarded, then
    /// answered `NotHolder`, so it took the lease path) is never refused
    /// `EROFS` — it may have taken effect: in doubt (`EIO`), as at a
    /// freeze.
    #[test]
    fn an_op_already_sent_stays_in_doubt_when_a_carrierless_epoch_activates() {
        let mut h = Harness::new(2);
        h.core.lease.cached_holder = Some(1);
        h.step(Event::Peers {
            links: vec![crate::event::PeerLink {
                node: 1,
                connected: true,
                last_seen: None,
                rtt_ms: Some(1),
                since: Some(Ms(0)),
            }],
        });
        let rid = h.rid(1);
        let op = h.create("a");
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        let [(1, PeerMsg::MutateRequest { req, .. })] = sends(&out)[..] else {
            panic!("expected a forward: {out:?}")
        };
        let out = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::MutateReply {
                req: *req,
                outcome: MutateOutcome::NotHolder { holder: 0 },
                base: None,
                position: constellation_meta::Position::ZERO,
                gen: 0,
                own_chunks: OwnChunks::None,
                own_rows: None,
            },
        });
        assert!(replies(&out).is_empty(), "{out:?}");
        let out = h.step(refusing_epoch(false, vec![2]));
        let r = replies(&out);
        assert!(
            matches!(r.as_slice(), [(x, ClientReply::InDoubt)] if *x == rid),
            "{out:?}"
        );
    }

    /// Chunk close-stall-followup: a stranded op's replay in an epoch
    /// that refuses writes is neither resent every drain tick (each send
    /// was answered in doubt and requeued) nor, after
    /// `replay_lease_fallback_ms`, turned into a lease request the epoch
    /// cannot grant. Once the epoch ends the drain resends it.
    #[test]
    fn a_stranded_replay_waits_out_an_epoch_that_refuses_writes() {
        for frozen in [false, true] {
            let mut h = Harness::new(2);
            h.core.lease.cached_holder = Some(1);
            h.step(Event::Peers {
                links: vec![crate::event::PeerLink {
                    node: 1,
                    connected: true,
                    last_seen: None,
                    rtt_ms: Some(1),
                    since: Some(Ms(0)),
                }],
            });
            let rid = h.rid(1);
            let op = h.create("a");
            h.meta.queue_replay(rid, &op, &Default::default()).unwrap();
            h.step(refusing_epoch(frozen, vec![2]));
            let ticks = h.core.cfg.replay_lease_fallback_ms / h.core.cfg.replay_drain_ms + 4;
            for _ in 0..ticks {
                h.advance(h.core.cfg.replay_drain_ms);
                let mut out = Vec::new();
                let now = h.now;
                h.core.on_drain_tick(now, &h.meta, &mut out);
                assert!(out.is_empty(), "frozen {frozen}: {out:?}");
                assert!(h.core.job.is_none(), "no lease asked for");
                assert!(!h.core.clients.contains_key(&rid));
            }
            assert_eq!(h.meta.pending_replays().unwrap().len(), 1, "still queued");
            h.step(Event::Control {
                op: OpId(902),
                req: Control::Epoch {
                    open: false,
                    active: false,
                    frozen: false,
                    flushing: false,
                    base: 0,
                    carrier: None,
                    stale_below: 0,
                    members: Vec::new(),
                },
            });
            h.advance(h.core.cfg.replay_drain_ms);
            let mut out = Vec::new();
            let now = h.now;
            h.core.on_drain_tick(now, &h.meta, &mut out);
            assert!(
                matches!(sends(&out)[..], [(1, PeerMsg::MutateRequest { rid: r, .. })] if *r == rid),
                "resent once the epoch is over: {out:?}"
            );
        }
    }

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

    /// `epoch-member-lost`: the activation carrying this node's lease can
    /// arrive after the lease entered its expiry margin (the claim was
    /// usable when acked). The hold is adopted anyway — before, it was
    /// not, the epoch carried a lease nobody held, and every write failed.
    #[test]
    fn a_carried_lease_is_held_though_its_activation_came_inside_the_margin() {
        let mut h = holder_with(AckPolicy::Local, vec![]);
        h.advance(9_300);
        assert!(
            !h.core.lease.usable(h.now, h.core.config()),
            "inside the margin"
        );
        h.step(epoch(true, true, vec![1, 2]));
        assert!(
            h.core.lease.epoch_held(),
            "the carried lease was not adopted"
        );
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
            tag: Default::default(),
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
                candidacy: 1,
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
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
        answer_takeover_read(&mut h, &out);
        assert_eq!(h.core.bk.sealed, 1, "silence after the close seals");
    }

    /// Plan 37 §8: a holder replaced on its own state dir asks its backup
    /// to hold the seal watch (`PeerMsg::BackupHold`). Silence inside the
    /// hold neither seals nor reads the lease; the successor's first
    /// append ends the hold (the usual window counts from it); a hold
    /// from anyone but the backed holder at its epoch is ignored; and a
    /// holder that never comes back is still sealed once the (capped)
    /// hold has passed.
    #[test]
    fn a_backup_hold_defers_the_seal_until_the_successor_or_its_end() {
        let append = |req| Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(req),
                epoch: 1,
                holder: 1,
                config_version: 2,
                candidacy: 1,
                from: 1,
                txs: Vec::new(),
                through: 0,
            },
        };
        let hold = |from, epoch, for_ms| Event::Peer {
            from,
            msg: PeerMsg::BackupHold { epoch, for_ms },
        };
        let mut h = Harness::new(2);
        let out = h.step(append(7));
        let watch = timers(&out, TimerKind::BackupWatch)[0];
        // Holds that are not the backed holder's at its epoch: ignored.
        h.step(hold(3, 1, 10_000));
        h.step(hold(1, 2, 10_000));
        assert_eq!(h.core.stats.backup_holds, 0);
        // The holder's own hold, then silence well past the budget.
        h.step(hold(1, 1, 10_000));
        assert_eq!(h.core.stats.backup_holds, 1);
        h.advance(5_000);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed inside the hold");
        assert!(s3_ops(&out).is_empty(), "read the lease: {out:?}");
        let watch = timers(&out, TimerKind::BackupWatch)[0];
        // The successor's first append ends the hold: its silence counts
        // from there, with the usual budget.
        h.advance(1_000);
        h.step(append(8));
        h.advance(1_000);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed before a full window");
        let watch = timers(&out, TimerKind::BackupWatch)[0];
        h.advance(1_000);
        let out = h.step(Event::Timer { id: watch });
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
        answer_takeover_read(&mut h, &out);
        assert_eq!(
            h.core.bk.sealed, 1,
            "silence after the successor's append seals"
        );

        // A holder that never comes back: sealed once the hold — capped
        // at `BACKUP_HOLD_MAX_MS`, whatever it asked — has passed.
        let mut h = Harness::new(2);
        let out = h.step(append(7));
        let mut watch = timers(&out, TimerKind::BackupWatch)[0];
        h.step(hold(1, 1, u64::MAX));
        let cap = super::super::backup::BACKUP_HOLD_MAX_MS as i64;
        h.advance(cap as u64);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed inside the capped hold");
        watch = timers(&out, TimerKind::BackupWatch)[0];
        h.advance(2_000);
        let out = h.step(Event::Timer { id: watch });
        answer_takeover_read(&mut h, &out);
        assert_eq!(h.core.bk.sealed, 1, "the hold outlived its cap");
    }

    /// This node backs holder 1 at epoch 1 (`config_version` 2); the
    /// watch timer it armed.
    fn backing(h: &mut Harness) -> TimerId {
        let out = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(7),
                epoch: 1,
                holder: 1,
                config_version: 2,
                candidacy: 1,
                from: 1,
                txs: Vec::new(),
                through: 0,
            },
        });
        let watch = timers(&out, TimerKind::BackupWatch);
        assert_eq!(watch.len(), 1, "the watch is armed: {out:?}");
        watch[0]
    }

    fn alive(h: &mut Harness, candidacy: u64, listed: bool, at: Ms) -> Vec<Action> {
        h.step(Event::HolderAlive {
            from: 1,
            epoch: 1,
            candidacy,
            listed,
            at,
        })
    }

    /// `stress-ng-fs-nodes`: the holder's core takes seconds over its
    /// steps, so its own heartbeat appends stop; its driver's off-core
    /// heartbeat keeps coming. The backup does not seal a live holder —
    /// also when its own core got to a heartbeat late, after the watch
    /// fired (silence counts from the arrival). Once the heartbeats stop
    /// too (a dead process, a hung core), silence seals as before.
    #[test]
    fn a_slow_holder_that_still_beats_off_its_core_is_not_sealed() {
        let mut h = Harness::new(2);
        let mut watch = backing(&mut h);
        // Five seconds without an append, a beat every 300 ms.
        for _ in 0..17 {
            h.advance(300);
            let at = h.now;
            alive(&mut h, 1, true, at);
        }
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed a holder that beats");
        assert!(s3_ops(&out).is_empty(), "read the lease: {out:?}");
        watch = timers(&out, TimerKind::BackupWatch)[0];
        // This node's own core stalled 2 s while the beats kept arriving
        // and queued up. The driver hands a timer over after what was
        // queued before it (`Driver::run`), and each beat is stamped with
        // its arrival: the window counts from the last one.
        let start = h.now;
        h.advance(2_000);
        for i in 1..=6 {
            alive(&mut h, 1, true, start.plus(300 * i));
        }
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed on this node's own stall");
        watch = timers(&out, TimerKind::BackupWatch)[0];
        // Beats from another holder or epoch are not this holder's.
        h.advance(2_000);
        let at = h.now;
        h.step(Event::HolderAlive {
            from: 3,
            epoch: 1,
            candidacy: 1,
            listed: true,
            at,
        });
        h.step(Event::HolderAlive {
            from: 1,
            epoch: 2,
            candidacy: 1,
            listed: true,
            at,
        });
        let out = h.step(Event::Timer { id: watch });
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
        answer_takeover_read(&mut h, &out);
        assert_eq!(h.core.bk.sealed, 1, "silence seals");
    }

    /// The holder dropped this backup (it made no acknowledgement
    /// progress) and says so: the backup stops watching and discards its
    /// tail, instead of sealing the live holder 1.5 s later. A dismissal
    /// of another candidacy (an earlier bring-up, overtaken by this one)
    /// is ignored.
    #[test]
    fn a_backup_its_holder_dropped_stops_watching_instead_of_sealing() {
        let mut h = Harness::new(2);
        let watch = backing(&mut h);
        let now = h.now;
        alive(&mut h, 0, false, now);
        alive(&mut h, 2, false, now);
        assert!(
            h.core.bk.role.is_some(),
            "another candidacy's dismissal applied"
        );
        alive(&mut h, 1, false, now);
        assert!(h.core.bk.role.is_none(), "still backing");
        h.advance(5_000);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed the holder that dropped it");
        assert!(s3_ops(&out).is_empty(), "read the lease: {out:?}");
        assert!(timers(&out, TimerKind::BackupWatch).is_empty());
        // Re-added later: an append makes it a backup again.
        backing(&mut h);
        assert!(h.core.bk.role.is_some());
    }

    /// Plan 37 §8's hold and the off-core heartbeat together: the
    /// replaced holder's last beats (and the successor's first ones,
    /// before its first append) never shorten a hold — silence inside it
    /// neither seals nor reads the lease — and once the hold has passed,
    /// beats keep a live successor unsealed while silence still seals.
    #[test]
    fn heartbeats_never_shorten_a_backup_hold() {
        let mut h = Harness::new(2);
        let mut watch = backing(&mut h);
        h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupHold {
                epoch: 1,
                for_ms: 10_000,
            },
        });
        assert_eq!(h.core.stats.backup_holds, 1);
        let hold_end = h.now.plus(10_000);
        // Beats for a second, then nothing for the rest of the hold.
        for _ in 0..3 {
            h.advance(300);
            let at = h.now;
            alive(&mut h, 1, true, at);
        }
        assert_eq!(h.core.bk.last_heard, hold_end, "a beat shortened the hold");
        h.advance(8_000);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed inside the hold");
        assert!(s3_ops(&out).is_empty(), "read the lease: {out:?}");
        watch = timers(&out, TimerKind::BackupWatch)[0];
        // Past the hold, the successor beats (no append yet): not sealed.
        h.advance(1_000);
        for _ in 0..5 {
            h.advance(300);
            let at = h.now;
            alive(&mut h, 1, true, at);
        }
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 0, "sealed a beating successor");
        watch = timers(&out, TimerKind::BackupWatch)[0];
        // Then silence: sealed.
        h.advance(2_000);
        let out = h.step(Event::Timer { id: watch });
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
        answer_takeover_read(&mut h, &out);
        assert_eq!(h.core.bk.sealed, 1, "silence after the hold seals");
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
                applied: 0,
                tag: Default::default(),
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
    fn a_refused_forward_is_journaled_and_a_retry_dedups_to_the_same_code() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let unlink = MutateOp::Unlink {
            parent: ROOT_INO,
            name: "f".into(),
        };
        let out = forward(&mut h, 2, 1, 1, unlink.clone());
        assert_eq!(outcome_of(&out, 1), MutateOutcome::Errno(Code::NotFound));
        assert_eq!(h.core.stats.refusals_journaled, 1);
        assert!(
            matches!(
                h.meta.completed_outcome(Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 1,
                }),
                Ok(Some(constellation_meta::CompletedOutcome::Refused {
                    code: Code::NotFound
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
        assert_eq!(outcome_of(&out, 3), MutateOutcome::Errno(Code::NotFound));
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
                candidacy: 1,
                alive_at: h.now,
                short_since_progress: false,
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
                candidacy: 1,
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

    /// overload-cascade-2: a candidate that timed out is not brought up
    /// again at once — it would start from zero and likely time out the
    /// same way (`stress-ng-fs-nodes`: the same loaded node re-selected
    /// every ~13 s for minutes). Another peer goes first; the same node
    /// comes back only after a backoff that doubles per timeout in a row.
    #[test]
    fn a_candidate_that_timed_out_is_backed_off_and_another_peer_goes_first() {
        use crate::core::backup::candidate_backoff_ms;
        let mut h = Harness::new(1);
        h.hold(1, None);
        // A lease that outlasts the two minutes (no renewal in between).
        h.core.lease.held.as_mut().unwrap().0.expires_unix_ms = h.now.plus(3_600_000).0;
        h.step(Event::Roster {
            write_eligible: vec![1, 2, 3],
        });
        let link = |node, since| crate::event::PeerLink {
            node,
            connected: true,
            last_seen: None,
            rtt_ms: Some(1),
            since: Some(Ms(since)),
        };
        // Node 2's link is the older one: it is preferred.
        h.step(Event::Peers {
            links: vec![link(2, 0), link(3, 1)],
        });
        journal(&h, "a", 1);
        journaled(&mut h, 900);
        let reconfig_min = h.core.cfg.backup_reconfig_min_ms;
        // Every candidate times out (no acknowledgement, no heartbeat
        // answer); the bring-ups over two minutes, with their times.
        let start = h.now;
        let mut brought_up: Vec<(i64, NodeId)> = Vec::new();
        let mut current = None;
        while h.now.since(start) < 120_000 {
            h.advance(50);
            if let Some(id) = h.core.ack.tick_timer {
                h.step(Event::Timer { id });
            }
            if h.now.since(start) % 1_000 == 0 {
                // The driver's directory refresh, every second.
                h.step(Event::Peers {
                    links: vec![link(2, 0), link(3, 1)],
                });
            }
            if h.core.ack.candidate != current {
                current = h.core.ack.candidate;
                if let Some(n) = current {
                    brought_up.push((h.now.since(start), n));
                }
            }
        }
        assert!(brought_up.len() >= 4, "{brought_up:?}");
        assert_eq!(brought_up[0].1, 2, "{brought_up:?}");
        assert_eq!(
            brought_up[1].1, 3,
            "the other peer goes next: {brought_up:?}"
        );
        // The same node again only after its backoff: 2×, 4×, 8×, ... the
        // rate limit (a timeout takes about a second on top).
        for n in [2, 3] {
            let times: Vec<i64> = brought_up
                .iter()
                .filter(|(_, m)| *m == n)
                .map(|(t, _)| *t)
                .collect();
            assert!(times.len() >= 2, "node {n}: {brought_up:?}");
            for (strike, w) in times.windows(2).enumerate() {
                let backoff = candidate_backoff_ms(reconfig_min, strike as u32 + 1) as i64;
                assert!(
                    w[1] - w[0] >= backoff,
                    "node {n} re-selected {} ms after its bring-up, backoff {backoff}: {brought_up:?}",
                    w[1] - w[0]
                );
            }
        }
        // Without the backoff, a node came back every rate-limit period.
        assert!(
            brought_up.len() < (120_000 / (reconfig_min as usize + 1_000)),
            "{brought_up:?}"
        );
        assert_eq!(candidate_backoff_ms(3_000, 1), 6_000);
        assert_eq!(candidate_backoff_ms(3_000, 2), 12_000);
        assert_eq!(candidate_backoff_ms(3_000, 30), 60_000);
        // Strikes decay: one more while the last backoff is recent, from 1
        // again after a quiet minute past it.
        use crate::core::backup::candidate_strikes_after;
        assert_eq!(candidate_strikes_after(None, Ms(1_000)), 1);
        assert_eq!(candidate_strikes_after(Some((3, Ms(10_000))), Ms(5_000)), 4);
        assert_eq!(
            candidate_strikes_after(Some((3, Ms(10_000))), Ms(69_999)),
            4
        );
        assert_eq!(
            candidate_strikes_after(Some((3, Ms(10_000))), Ms(70_000)),
            1
        );
        assert_eq!(candidate_strikes_after(Some((1, Ms(0))), Ms(3_600_000)), 1);
    }

    /// overload-cascade: a backup that answers the holder's heartbeat
    /// (`Event::BackupAlive`) is alive, and a loaded one acknowledges
    /// seconds late: it is not dropped at `backup_ack_timeout_ms`, only
    /// once it has made no progress for `backup_slow_max_ms`. One that
    /// stops answering the heartbeat (dead, cut off) goes at the ack
    /// timeout as before, and so does one that answers appends short
    /// (alive but stuck).
    #[test]
    fn a_slow_backup_known_alive_is_kept_until_the_slow_bound() {
        let timeout = 1_000i64;
        let slow_max = 10_000i64;
        // Steps 50 ms at a time with `alive` answers until `until` ms have
        // passed since the start or the candidate is gone; `true` if gone.
        let run = |h: &mut Harness, until: i64, alive: bool| -> bool {
            let started = h.now;
            while h.core.ack.candidate.is_some() && h.now.since(started) < until {
                h.advance(50);
                if alive {
                    let at = h.now;
                    h.step(Event::BackupAlive { from: 2, at });
                }
                if let Some(id) = h.core.ack.tick_timer {
                    h.step(Event::Timer { id });
                }
            }
            h.core.ack.candidate.is_none()
        };
        // Alive, its append unanswered: kept past the ack timeout, then
        // dropped at the slow bound.
        let mut h = holder_with_candidate();
        assert_eq!(h.core.cfg.backup_ack_timeout_ms as i64, timeout);
        assert_eq!(h.core.cfg.backup_slow_max_ms as i64, slow_max);
        journal(&h, "a", 1);
        assert_eq!(appends(&journaled(&mut h, 900)).len(), 1);
        let start = h.now;
        assert!(
            !run(&mut h, 5 * timeout, true),
            "a live, slow backup was dropped"
        );
        assert!(run(&mut h, slow_max, true), "kept past the slow bound");
        assert!(h.now.since(start) >= slow_max);
        assert!(h.now.since(start) < slow_max + 500);
        // Silent: dropped at the ack timeout, as before.
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        journaled(&mut h, 900);
        let start = h.now;
        assert!(run(&mut h, 4 * timeout, false));
        assert!(h.now.since(start) < timeout + 500, "{}", h.now.since(start));
        // Alive at first, then silent: the ack timeout counts from the
        // last answer.
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        journaled(&mut h, 900);
        let start = h.now;
        assert!(!run(&mut h, 3 * timeout, true));
        assert!(run(&mut h, 4 * timeout, false));
        assert!(h.now.since(start) < 4 * timeout + 500);
        // Alive but answering short: stuck, dropped at the ack timeout.
        let mut h = holder_with_candidate();
        journal(&h, "a", 1);
        let first = appends(&journaled(&mut h, 900));
        journal(&h, "b", 2);
        let second = appends(&journaled(&mut h, 901));
        // The later append comes back short (the first never landed).
        let out = ack(&mut h, second[0].0, 0);
        assert!(appends(&out).is_empty(), "{out:?}");
        assert!(h.core.ack.peers[&2].short_since_progress);
        let start = h.now;
        assert!(run(&mut h, 4 * timeout, true), "a stuck backup was kept");
        assert!(h.now.since(start) < timeout + 500);
        let _ = first;
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
                    candidacy: 1,
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

    /// overload-cascade review: a dismissal (`HolderAlive { listed:
    /// false }`) applies to the candidacy it names only. The holder
    /// dropped this node (candidacy 10) and brought it up again (20),
    /// counting acknowledgements from the new candidacy; the dismissal
    /// of 10, repeated for a while and still in flight, must not wipe the
    /// tail those acknowledgements stand for — the node would be promoted
    /// with an empty tail, and a takeover after the holder's crash would
    /// lose acknowledged writes. The current candidacy's dismissal does
    /// end the role, and one that comes before the next bring-up's first
    /// append is harmless (the holder counts that one from zero).
    #[test]
    fn a_stale_dismissal_never_wipes_a_readded_backups_tail() {
        let holder = Harness::new(1);
        holder.meta.set_holder_epoch(1);
        journal(&holder, "a", 1);
        journal(&holder, "b", 2);
        let txs = holder.meta.journal_txs_from(1, 1000).unwrap();
        assert_eq!(txs.len(), 2);
        let tip = txs[1].last;
        let mut b = Harness::new(2);
        let append = |b: &mut Harness,
                      req: u64,
                      candidacy: u64,
                      from: u64,
                      txs: Vec<constellation_meta::BackupTx>| {
            let out = b.step(Event::Peer {
                from: 1,
                msg: PeerMsg::BackupAppend {
                    req: OpId(req),
                    epoch: 1,
                    holder: 1,
                    config_version: 2,
                    candidacy,
                    from,
                    txs,
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
        let dismiss = |b: &mut Harness, candidacy: u64| {
            let at = b.now;
            b.step(Event::HolderAlive {
                from: 1,
                epoch: 1,
                candidacy,
                listed: false,
                at,
            })
        };
        assert_eq!(
            append(&mut b, 1, 10, 1, vec![txs[0].clone()]),
            (txs[0].last, false)
        );
        // Re-added: the new candidacy streams from the start.
        assert_eq!(append(&mut b, 2, 20, 1, txs.clone()), (tip, false));
        // The stale dismissal: nothing discarded.
        dismiss(&mut b, 10);
        assert!(
            b.core.bk.role.is_some(),
            "the stale dismissal ended the role"
        );
        assert_eq!(b.core.bk.acked, tip);
        assert_eq!(b.meta.backup_acked(1).unwrap(), tip);
        assert_eq!(
            b.meta.backup_tail(1).unwrap().len(),
            2,
            "the tail was wiped"
        );
        assert_eq!(b.core.stats.backup_dismissals, 0);
        // The next append (a heartbeat) is acknowledged through the tip.
        assert_eq!(append(&mut b, 3, 20, tip + 1, Vec::new()), (tip, false));
        // An earlier candidacy's append that arrives late does not make
        // its dismissal current again.
        assert_eq!(append(&mut b, 4, 10, tip + 1, Vec::new()), (tip, false));
        dismiss(&mut b, 10);
        assert!(b.core.bk.role.is_some());
        // The current candidacy's dismissal ends the role.
        dismiss(&mut b, 20);
        assert!(b.core.bk.role.is_none());
        assert!(b.meta.backup_tail(1).unwrap().is_empty());
        assert_eq!(b.core.stats.backup_dismissals, 1);
        // Repeated before the next bring-up's first append: nothing to
        // end; the next candidacy is acknowledged what it streams.
        dismiss(&mut b, 20);
        assert_eq!(
            append(&mut b, 5, 30, 1, vec![txs[0].clone()]),
            (txs[0].last, false)
        );
        dismiss(&mut b, 20);
        assert!(b.core.bk.role.is_some());
        assert_eq!(b.core.stats.backup_dismissals, 1);
    }

    /// EC2 follow-up 3a: under very slow S3 the holder's lease can sit
    /// inside its expiry margin while a renewal is still in flight — no
    /// new mutation is admitted, but the lease is not lost. The holder
    /// must keep its backups hearing from it (heartbeats never wait on
    /// S3): before, it dropped its backup set, went silent, and the
    /// backup sealed the live holder's epoch 1.5 s later.
    #[test]
    fn a_holder_whose_renewal_is_slow_keeps_heartbeating_its_backup() {
        let mut h = holder_with_candidate();
        let tip = journal(&h, "a", 1);
        let sent = appends(&journaled(&mut h, 900));
        ack(&mut h, sent[0].0, tip);
        // 9.3 s of a 10 s lease: inside the 1 s margin, not expired.
        h.advance(9_300);
        assert!(h.core.lease.ship_epoch(h.now, h.core.config()).is_none());
        let out = journaled(&mut h, 901);
        assert!(
            h.core.ack.peers.contains_key(&2),
            "the backup set was dropped while the lease is still held"
        );
        assert_eq!(appends(&out).len(), 1, "no heartbeat: {out:?}");
        // Past the lease's own expiry the holder stops: the backup may
        // seal and take over.
        h.advance(1_000);
        let _ = journaled(&mut h, 902);
        assert!(h.core.ack.peers.is_empty(), "an expired lease heartbeats");
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

    /// flex-crash seed 30702: an epoch hold (journal empty) is handed over
    /// P2P only to a requester that has applied the holder's whole log;
    /// one behind it could not catch up while S3 is away.
    #[test]
    fn an_epoch_hold_goes_only_to_a_requester_at_the_holders_head() {
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
        h.core.ship.head_seq = 18;
        let ask = |h: &mut Harness, req: u64, applied: Seq| {
            let out = h.step(Event::Peer {
                from: 2,
                msg: PeerMsg::LeaseRequest {
                    req: OpId(req),
                    epoch_applied: Some(applied),
                },
            });
            match &sends(&out)[0].1 {
                PeerMsg::LeaseHandoff { released, .. } => *released,
                other => panic!("{other:?}"),
            }
        };
        assert!(!ask(&mut h, 5, 13), "handed to a member behind the log");
        assert_eq!(h.core.stats.epoch_handoffs_behind, 1);
        assert!(h.core.lease.epoch_held(), "the hold stays");
        assert!(ask(&mut h, 6, 18), "a caught-up member takes the hold");
        assert!(!h.core.lease.epoch_held());
        assert_eq!(h.meta.holder_epoch(), 0, "capture ends with the hold");
    }

    /// Fix "capture under an epoch hold": the hold owner journals as a
    /// holder does (ADR-19) — `Meta::holder_epoch` is the hold's epoch,
    /// so a write under the hold is a captured `Local` row with its
    /// before-images, not an uncaptured transaction (which held the
    /// whole journal behind the first deferred one, and made a deposed
    /// owner rebuild from the head commit).
    #[test]
    fn the_hold_owner_captures_its_epoch_journal() {
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
        assert_eq!(h.core.lease.epoch_hold(), Some(1));
        assert_eq!(h.meta.holder_epoch(), 1, "capture is on under the hold");
        submit_create(&mut h, 1, "under-the-hold");
        let counts = h.meta.speculation_counts().unwrap();
        assert_eq!(counts.local, 1, "the epoch write is a captured Local row");
        assert_eq!(
            h.meta.uncaptured_tx_count().unwrap(),
            0,
            "nothing journaled under the hold is uncaptured"
        );
    }

    /// The hold a member takes over P2P journals under the epoch its
    /// flush CAS will grant (the carried lease's successor); the carrier
    /// re-adopting its own lease stays at the carried epoch. So the
    /// flush's gate strands nothing of the hold's captured journal, and
    /// a position `(epoch, jseq)` a member is answered with is reached
    /// only by the flush's segments, never by the marker alone.
    #[test]
    fn a_transferred_hold_journals_under_the_epoch_its_flush_will_claim() {
        let mut h = Harness::new(2);
        h.core.cfg.epoch_slack = 1;
        h.step(Event::Roster {
            write_eligible: vec![1, 2, 3],
        });
        h.step(Event::Peers {
            links: vec![crate::event::PeerLink {
                node: 1,
                connected: true,
                last_seen: None,
                rtt_ms: None,
                since: None,
            }],
        });
        h.core.lease.cached_holder = Some(1);
        let expires = h.now.plus(10_000).0;
        activate(
            &mut h,
            Some(Carrier {
                node: 1,
                epoch: 4,
                expires_unix_ms: expires,
            }),
            4,
        );
        assert_eq!(h.core.epoch_hold_epoch_for(4), 5, "a transferee's hold");
        let out = h.step(Event::Control {
            op: OpId(700),
            req: Control::Acquire,
        });
        let req = match sends(&out)[0] {
            (1, PeerMsg::LeaseRequest { req, epoch_applied }) => {
                assert_eq!(*epoch_applied, Some(0));
                *req
            }
            other => panic!("{other:?}"),
        };
        h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LeaseHandoff {
                req,
                released: true,
                epoch: 4,
                head_seq: Some(0),
            },
        });
        assert_eq!(h.core.lease.epoch_hold(), Some(5));
        assert_eq!(h.meta.holder_epoch(), 5, "captures under the hold's epoch");
        let now = h.now;
        let granted = h.core.lease.granted_lease(
            now,
            h.core.config(),
            Some(&Lease::granted("p0", 1, 4, 10_000)),
        );
        assert_eq!(
            granted.epoch, 5,
            "the flush CAS on the carried object grants the same epoch"
        );

        // The carrier itself, handed its hold back, stays at the carried
        // epoch: its flush re-adopts its own lease.
        let mut c = Harness::new(1);
        c.hold(4, None);
        let expires = c.core.lease.held.as_ref().unwrap().0.expires_unix_ms;
        activate(
            &mut c,
            Some(Carrier {
                node: 1,
                epoch: 4,
                expires_unix_ms: expires,
            }),
            4,
        );
        assert_eq!(c.core.epoch_hold_epoch_for(4), 4);
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
        tag: Default::default(),
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
            own_chunks: OwnChunks::None,
            own_rows: None,
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
            range: (0, 0),
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

/// A lease object for the takeover tests below.
fn lease_of(holder: NodeId, epoch: Epoch, expires_unix_ms: i64) -> Lease {
    Lease {
        v: 1,
        partition: "p0".into(),
        holder,
        epoch,
        expires_unix_ms,
        released: false,
        wanted_by: Vec::new(),
        backups: Vec::new(),
        config_version: 1,
        ack_policy: constellation_store_s3::AckPolicy::Local,
        granted_delegations: false,
        retired: Vec::new(),
    }
}

/// The one job S3 request in `out` (the periodic heartbeat read aside),
/// and its op id.
fn only_s3(out: &[Action]) -> (OpId, S3Op) {
    let ops: Vec<_> = s3_ops(out)
        .into_iter()
        .filter(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
        .collect();
    assert_eq!(ops.len(), 1, "expected one S3 request: {out:?}");
    (ops[0].0, ops[0].1.clone())
}

/// Plan 30 §M9 (the M12 coder's backup-crash seed 892): a takeover whose
/// marker slot was taken is deposed by what the retry's tail finds (a
/// newer epoch claimed the lease while this node was paused). The
/// deposition voids the gate; the marker retry then had nothing to ship
/// and issued nothing, so the acquisition never finished — and while it
/// held the job slot no round ran, the node tailed nothing and its
/// forwards waited for a log it never read. The acquisition fails now.
#[test]
fn a_takeover_deposed_while_retrying_its_marker_finishes_the_acquisition() {
    let mut h = Harness::new(1);
    // (Plan 30 §M10's promise check is its own machinery; not here.)
    h.core.cfg.takeover_promise_check = false;
    let mut out = Vec::new();
    h.core.enqueue_job(
        h.now,
        super::jobs::JobReq::Acquire {
            reason: "test",
            ask_handoff: false,
        },
        &h.meta,
        &mut out,
    );
    let (get, req) = only_s3(&out);
    assert!(matches!(req, S3Op::LeaseGet));
    // Node 3's lease at epoch 1 has expired: a takeover.
    let out = h.step(Event::S3 {
        op: get,
        result: S3Result::LeaseGet(Ok(Some((lease_of(3, 1, h.now.0 - 1), tag())))),
    });
    let (tail, req) = only_s3(&out);
    assert!(matches!(req, S3Op::SegmentRun { .. }), "{req:?}");
    let out = h.step(Event::S3 {
        op: tail,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    // A takeover confirms an empty tail with a LIST before its CAS.
    let (gap, req) = only_s3(&out);
    assert!(matches!(req, S3Op::SegmentGap { from: 1 }), "{req:?}");
    let out = h.step(Event::S3 {
        op: gap,
        result: S3Result::SegmentGap(Ok(None)),
    });
    let (cas, req) = only_s3(&out);
    assert!(matches!(req, S3Op::LeaseSwap { .. }), "{req:?}");
    let out = h.step(Event::S3 {
        op: cas,
        result: S3Result::LeasePut(Ok(tag())),
    });
    let (marker, req) = only_s3(&out);
    assert!(matches!(req, S3Op::SegmentPut { .. }), "{req:?}");
    assert!(h.core.lease().gate.is_some());
    // The marker's slot is taken; the retry tails first.
    let out = h.step(Event::S3 {
        op: marker,
        result: S3Result::SegmentPut(Err(CasFailure::Conflict)),
    });
    let (tail, req) = only_s3(&out);
    assert!(matches!(req, S3Op::SegmentRun { .. }), "{req:?}");
    // ... and finds node 2's marker at epoch 3: this node is deposed.
    let theirs = crate::segment::encode(2, 3, 0, &[], &[], &[]).unwrap();
    let _ = h.step(Event::S3 {
        op: tail,
        result: S3Result::SegmentRun(Ok(vec![(1, theirs)])),
    });
    assert!(h.core.lease().lost);
    assert_eq!(
        h.core.job(),
        None,
        "the acquisition must finish once the deposition voided its gate"
    );
}

/// The retention gap check (DESIGN.md §14 "Falling behind segment GC").
mod retention_gap {
    use super::*;

    /// Run one round on a follower: poll timer, then the upload step.
    fn round(h: &mut Harness) -> Vec<Action> {
        h.core.nudge(h.now, &mut Vec::new());
        let poll = h
            .core
            .timers
            .iter()
            .find(|(_, (t, _))| matches!(t, Timer::Poll))
            .map(|(id, _)| *id)
            .expect("poll timer");
        let mut all = h.step(Event::Timer { id: poll });
        let upload = all.iter().find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        });
        if let Some(op) = upload {
            all.extend(h.step(Event::UploadsDone {
                op,
                result: UploadResult::Done { held: 0 },
            }));
        }
        all
    }

    fn one(out: &[Action], pred: impl Fn(&S3Op) -> bool) -> OpId {
        s3_ops(out)
            .into_iter()
            .find(|(_, r)| pred(r))
            .map(|(op, _)| op)
            .unwrap_or_else(|| panic!("no matching S3 request in {out:?}"))
    }

    fn any_swap(out: &[Action]) -> bool {
        s3_ops(out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::LeaseSwap { .. } | S3Op::LeaseCreate { .. }))
    }

    /// A taker whose next slot was pruned (its GET-next found nothing, the
    /// LIST finds a later segment) never CASes the lease: it rebuilds the
    /// replica from the head commit and the acquisition fails, to be
    /// retried from the new position. Before, the empty probe read as
    /// "at head", the CAS won, and the takeover marker was created in
    /// the deleted slot — a forked log.
    #[test]
    fn a_takeover_across_a_pruned_gap_rebuilds_instead_of_claiming() {
        let mut h = Harness::new(1);
        h.core.cfg.takeover_promise_check = false;
        let mut out = Vec::new();
        h.core.enqueue_job(
            h.now,
            super::super::jobs::JobReq::Acquire {
                reason: "test",
                ask_handoff: false,
            },
            &h.meta,
            &mut out,
        );
        let get = one(&out, |r| matches!(r, S3Op::LeaseGet));
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((lease_of(3, 1, h.now.0 - 1), tag())))),
        });
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        assert!(!any_swap(&out), "no CAS before the gap check: {out:?}");
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { from: 1 }));
        // Segments 1..=5 were pruned; the log resumes at 6.
        let out = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(Some(6))),
        });
        assert!(!any_swap(&out), "{out:?}");
        let rebuild = out
            .iter()
            .find_map(|a| match a {
                Action::RebuildReplica { op } => Some(*op),
                _ => None,
            })
            .expect("a rebuild");
        assert_eq!(h.core.stats.retention_gaps, 1);
        let out = h.step(Event::RebuildDone {
            op: rebuild,
            ok: true,
        });
        assert!(!any_swap(&out), "{out:?}");
        assert_eq!(h.core.job(), None, "the acquisition ended unacquired");
        assert!(h.core.lease().held.is_none());
        // The cursor restarts from the (here: unchanged) replica.
        assert_eq!(h.core.ship.next_seq, h.meta.applied_seq().unwrap() + 1);
    }

    /// An empty takeover tail whose LIST says "at head" claims as before.
    #[test]
    fn a_takeover_at_the_head_claims_after_its_gap_check() {
        let mut h = Harness::new(1);
        h.core.cfg.takeover_promise_check = false;
        let mut out = Vec::new();
        h.core.enqueue_job(
            h.now,
            super::super::jobs::JobReq::Acquire {
                reason: "test",
                ask_handoff: false,
            },
            &h.meta,
            &mut out,
        );
        let get = one(&out, |r| matches!(r, S3Op::LeaseGet));
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((lease_of(3, 1, h.now.0 - 1), tag())))),
        });
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { .. }));
        let out = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(None)),
        });
        assert!(any_swap(&out), "{out:?}");
        assert_eq!(h.core.stats.retention_gaps, 0);
    }

    /// A takeover CAS in doubt whose re-read finds another object stays
    /// in doubt: a PUT that timed out client-side can still apply after
    /// the re-read. The takeover's predecessor is recorded either way, for
    /// a later acquisition that finds the object the CAS wrote.
    #[test]
    fn a_takeover_cas_in_doubt_keeps_its_predecessor_when_the_reread_finds_another() {
        let mut h = Harness::new(1);
        h.core.cfg.takeover_promise_check = false;
        let mut out = Vec::new();
        h.core.enqueue_job(
            h.now,
            super::super::jobs::JobReq::Acquire {
                reason: "test",
                ask_handoff: false,
            },
            &h.meta,
            &mut out,
        );
        let prev = lease_of(3, 1, h.now.0 - 1);
        let get = one(&out, |r| matches!(r, S3Op::LeaseGet));
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((prev.clone(), tag())))),
        });
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { .. }));
        let out = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(None)),
        });
        let (swap, sent) = match s3_ops(&out)
            .into_iter()
            .find(|(_, r)| matches!(r, S3Op::LeaseSwap { .. }))
        {
            Some((op, S3Op::LeaseSwap { lease, .. })) => (op, lease.clone()),
            _ => panic!("expected the takeover CAS: {out:?}"),
        };
        let out = h.step(Event::S3 {
            op: swap,
            result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
        });
        let reread = one(&out, |r| matches!(r, S3Op::LeaseGet));
        h.step(Event::S3 {
            op: reread,
            result: S3Result::LeaseGet(Ok(Some((
                lease_of(2, sent.epoch, h.now.plus(10_000).0),
                tag(),
            )))),
        });
        assert!(h.core.lease().held.is_none());
        assert_eq!(h.core.stats.acquire_cas_in_doubt_landed, 0);
        assert_eq!(h.core.lease.ambiguous_claim, Some((sent.epoch, prev)));
    }

    /// A follower: the first empty probe after a mount is confirmed by a
    /// LIST; within `gap_check_ms` and without a hint, later empty probes
    /// are not; a gossip hint at or past the cursor triggers one; a gap
    /// found rebuilds the replica and ends the round.
    #[test]
    fn a_follower_checks_once_at_start_on_hints_and_on_the_backstop() {
        let mut h = Harness::new(1);
        h.core.cfg.gap_check_ms = 300_000;
        h.core.cfg.gap_hint_check_ms = 5_000;
        // First round after the mount: probe, then one LIST.
        let out = round(&mut h);
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { from: 1 }));
        let _ = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(None)),
        });
        assert_eq!(h.core.job(), None);
        // Second round, soon after, no hint: no LIST.
        h.advance(1_000);
        let out = round(&mut h);
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::SegmentGap { .. })),
            "{out:?}"
        );
        assert_eq!(h.core.job(), None);
        // A hint names segment 10 while the cursor is at 1: the probe's
        // empty answer is checked, the gap found, the replica rebuilt.
        h.advance(6_000);
        let _ = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::SegmentPublished { seq: 10, epoch: 1 },
        });
        let out = round(&mut h);
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { from: 1 }));
        let out = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(Some(8))),
        });
        let rebuild = out
            .iter()
            .find_map(|a| match a {
                Action::RebuildReplica { op } => Some(*op),
                _ => None,
            })
            .expect("a rebuild");
        let _ = h.step(Event::RebuildDone {
            op: rebuild,
            ok: true,
        });
        assert_eq!(h.core.stats.retention_gaps, 1);
        assert_eq!(h.core.job(), None, "the round ended");
        // Much later, no hint: the backstop LIST.
        h.advance(300_000);
        let out = round(&mut h);
        let tail = one(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        // A segment that landed between the probe and the LIST is tailed.
        let gap = one(&out, |r| matches!(r, S3Op::SegmentGap { .. }));
        let out = h.step(Event::S3 {
            op: gap,
            result: S3Result::SegmentGap(Ok(Some(1))),
        });
        one(&out, |r| matches!(r, S3Op::SegmentRun { from: 1, .. }));
    }

    /// A holder never gap-checks: it is the sole appender. (Its round may
    /// skip the tail altogether, or probe its own stream once.)
    #[test]
    fn a_holder_never_gap_checks() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        h.core.ship.held_tail_at = None;
        let mut out = round(&mut h);
        if let Some((op, _)) = s3_ops(&out)
            .into_iter()
            .find(|(_, r)| matches!(r, S3Op::SegmentRun { .. }))
        {
            out = h.step(Event::S3 {
                op,
                result: S3Result::SegmentRun(Ok(Vec::new())),
            });
        }
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::SegmentGap { .. })),
            "{out:?}"
        );
        assert_eq!(h.core.stats.retention_gaps, 0);
    }
}

/// Plan 30 §M9 fixes from the backup-crash sweeps (`fix-backup-crash`).
mod backup_crash {
    use super::*;
    use constellation_store_s3::AckPolicy;

    /// The job S3 request matching `pred` in `out` (heartbeat reads and
    /// other traffic aside).
    fn find_s3(out: &[Action], pred: impl Fn(&S3Op) -> bool) -> OpId {
        s3_ops(out)
            .into_iter()
            .find(|(_, r)| pred(r))
            .map(|(op, _)| op)
            .unwrap_or_else(|| panic!("no matching S3 request in {out:?}"))
    }

    fn backup_lease(holder: NodeId, epoch: Epoch, expires: i64, backups: Vec<NodeId>) -> Lease {
        Lease {
            backups,
            ack_policy: AckPolicy::Backup,
            config_version: 2,
            ..lease_of(holder, epoch, expires)
        }
    }

    /// backup-crash seed 606255: the holder's removal CAS emptied its
    /// backup set (`Local`) while a peer in budget was eligible and no
    /// candidate was named yet; `ack_need` looked at the candidate alone
    /// and acknowledged at once, on this disk alone (the holder then died
    /// and the create came back as a conflict copy). backup-crash-slow
    /// seed 603322: the same in a tenure's first event, before its first
    /// selection pass had assessed eligibility at all.
    #[test]
    fn nothing_is_acknowledged_locally_while_a_backup_could_be_had() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        let pos = constellation_meta::Position {
            seq: 0,
            pending: Some(constellation_meta::JournalPos { epoch: 1, jseq: 5 }),
            streams: Default::default(),
        };
        assert_eq!(h.core.lease.ack_policy(), AckPolicy::Local);
        assert!(h.core.ack.candidate.is_none());
        // Not yet assessed this tenure: gated.
        assert_eq!(h.core.ack.eligible, None);
        assert_eq!(h.core.ack_need(&pos), Some(5));
        // A peer in budget, no candidate yet: gated.
        h.core.ack.eligible = Some(true);
        assert_eq!(h.core.ack_need(&pos), Some(5));
        // Nobody in budget: today's `Local`.
        h.core.ack.eligible = Some(false);
        assert_eq!(h.core.ack_need(&pos), None);
        // A configuration that never selects a backup never gates.
        h.core.ack.eligible = None;
        h.core.cfg.backup_rtt_budget_ms = 0;
        assert_eq!(h.core.ack_need(&pos), None);
    }

    /// backup-crash-slow seeds 603631 (the takeover CAS applied, then
    /// timed out) and 601692 (the successor restarted inside its gate):
    /// the sealed backup's own lease at the next epoch is its takeover
    /// having landed. Its watch must not void the tail on reading that
    /// lease (the M12 coder's backup-strict seed 1353 took the same
    /// branch after a successful CAS), and the acquisition that adopts
    /// it re-ships the tail — announced in its marker (`TailFollows`).
    #[test]
    fn a_sealed_backups_own_lease_at_the_next_epoch_reships_its_tail() {
        // Node 1's journal: one acknowledged create, backed by node 2.
        let holder = Harness::new(1);
        holder.meta.set_holder_epoch(1);
        let create = holder.create("a");
        let rid = Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        constellation_meta::execute_mutate(&holder.meta, &create, Some(rid)).unwrap();
        let txs = holder.meta.journal_txs_from(1, 1000).unwrap();
        let mut h = Harness::new(2);
        h.core.cfg.takeover_promise_check = false;
        let out = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(1),
                epoch: 1,
                holder: 1,
                config_version: 2,
                candidacy: 1,
                from: 1,
                txs,
                through: 0,
            },
        });
        let watch = timers(&out, TimerKind::BackupWatch)[0];
        // The holder falls silent: read the lease, seal, take over.
        h.advance(5_000);
        let out = h.step(Event::Timer { id: watch });
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let theirs = backup_lease(1, 1, h.now.plus(4_000).0, vec![2]);
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((theirs.clone(), tag())))),
        });
        assert_eq!(h.core.bk.sealed, 1);
        let rewatch = timers(&out, TimerKind::BackupWatch)[0];
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((theirs, tag())))),
        });
        let tail = find_s3(&out, |r| matches!(r, S3Op::SegmentRun { .. }));
        let out = h.step(Event::S3 {
            op: tail,
            result: S3Result::SegmentRun(Ok(Vec::new())),
        });
        let out = at_head(&mut h, out);
        let cas = find_s3(&out, |r| matches!(r, S3Op::LeaseSwap { .. }));
        // The CAS applies, but its reply is a timeout, and so does the
        // re-read that would have found it landed: still in doubt.
        let out = h.step(Event::S3 {
            op: cas,
            result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
        });
        let reread = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let _ = h.step(Event::S3 {
            op: reread,
            result: S3Result::LeaseGet(Err(crate::event::S3Failure("timed out".into()))),
        });
        assert!(h.core.lease.held.is_none());
        // The watch reads the lease again: it names this node now.
        h.advance(2_000);
        let out = h.step(Event::Timer { id: rewatch });
        let mine = lease_of(2, 2, h.now.plus(6_000).0);
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((mine.clone(), tag())))),
        });
        assert!(
            h.core.bk.role.is_some(),
            "the tail was voided on reading this node's own takeover"
        );
        // The acquisition adopts it.
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let mut out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((mine, tag())))),
        });
        // (Adopting its own lease may tail and renew first.)
        for _ in 0..4 {
            if s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::SegmentPut { .. }))
            {
                break;
            }
            if let Some((op, req)) = s3_ops(&out)
                .into_iter()
                .find(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
            {
                let result = match req {
                    S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                    S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                    S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                    other => panic!("unexpected {other:?}"),
                };
                out = h.step(Event::S3 { op, result });
            }
        }
        assert_eq!(
            h.core.lease.gate.and_then(|g| g.backup_tail_epoch),
            Some(1),
            "the gate re-ships the sealed tail"
        );
        assert!(
            h.core.lease.gate.is_some_and(|g| g.fast_prev.is_some()),
            "the replaced (unexpired) lease stays the predecessor: its \
             strict-read horizon is this tenure's acknowledgement floor"
        );
        let (marker, payload) = s3_ops(&out)
            .into_iter()
            .find_map(|(op, r)| match r {
                S3Op::SegmentPut { payload, .. } => Some((op, payload.clone())),
                _ => None,
            })
            .expect("the marker");
        let seg = crate::segment::decode(&payload).unwrap();
        assert_eq!(
            seg.records,
            vec![LogRecord::TailFollows { prev_epoch: 1 }],
            "the marker announces the tail"
        );
        let _ = h.step(Event::S3 {
            op: marker,
            result: S3Result::SegmentPut(Ok(())),
        });
        assert!(
            h.meta.completed_position(rid).unwrap().is_some(),
            "the acknowledged create is in this tenure's journal"
        );
        assert!(MetaStore::lookup(&h.meta, ROOT_INO, "a").unwrap().is_some());
    }

    /// backup-crash-slow seed 601692: the sealed successor crashed inside
    /// its gate (the marker had landed, the tail was not yet re-applied)
    /// and restarted with its role, seal and tail on disk. Re-adopting
    /// its own lease, the gate re-ships the tail (before: the own-lease
    /// claim named no predecessor, the tail was skipped and later voided,
    /// and an acknowledged create became a conflict copy).
    #[test]
    fn a_restarted_successor_reships_the_tail_it_sealed() {
        let holder = Harness::new(1);
        holder.meta.set_holder_epoch(1);
        let rid = Rid {
            node: 1,
            incarnation: 1,
            seq: 1,
        };
        constellation_meta::execute_mutate(&holder.meta, &holder.create("b"), Some(rid)).unwrap();
        let txs = holder.meta.journal_txs_from(1, 1000).unwrap();
        let mut h = Harness::new(2);
        let _ = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::BackupAppend {
                req: OpId(1),
                epoch: 1,
                holder: 1,
                config_version: 2,
                candidacy: 1,
                from: 1,
                txs,
                through: 0,
            },
        });
        h.meta.backup_seal(1).unwrap();
        // Restart: a fresh core over the same store.
        let mut cfg = h.core.cfg.clone();
        cfg.takeover_promise_check = false;
        h.core = Core::new(cfg);
        let mut out = Vec::new();
        h.core.start(h.now, &h.meta, &mut out);
        assert!(h.core.bk.role.is_some(), "the role survived the restart");
        let mut out = Vec::new();
        h.core.enqueue_job(
            h.now,
            super::super::jobs::JobReq::Acquire {
                reason: "test",
                ask_handoff: false,
            },
            &h.meta,
            &mut out,
        );
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let mine = lease_of(2, 2, h.now.plus(6_000).0);
        let mut out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((mine, tag())))),
        });
        for _ in 0..5 {
            if h.core.lease.gate.is_some() {
                break;
            }
            if let Some((op, req)) = s3_ops(&out)
                .into_iter()
                .find(|(_, r)| !matches!(r, S3Op::HeartbeatRead))
            {
                let result = match req {
                    S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                    S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                    S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                    other => panic!("unexpected {other:?}"),
                };
                out = h.step(Event::S3 { op, result });
            }
        }
        assert_eq!(
            h.core.lease.gate.and_then(|g| g.backup_tail_epoch),
            Some(1),
            "the restarted successor re-ships the tail it sealed"
        );
    }

    /// long-acks3 seed 50557 ("a client op was never answered"): the
    /// client heard `InDoubt` for a forward the takeover stranded; its
    /// retry by rid arrived while this node's replay of the same rid was
    /// in flight and was dropped as a duplicate. The replay's outcome now
    /// answers the client too.
    #[test]
    fn a_client_retrying_a_rid_under_replay_is_answered_by_the_replay() {
        let (holder, mut requester) = pair();
        let rid = requester.rid(1);
        let op = requester.create("x");
        requester
            .meta
            .queue_replay(rid, &op, &Default::default())
            .unwrap();
        // A fresh core over the store arms the replay drain.
        let cfg = requester.core.cfg.clone();
        requester.core = Core::new(cfg);
        let mut out = Vec::new();
        requester
            .core
            .start(requester.now, &requester.meta, &mut out);
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
        let drain = timers(&out, TimerKind::ReplayDrain)[0];
        let out = requester.step(Event::Timer { id: drain });
        let req = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::MutateRequest { req, rid: r, .. } if to == 1 && *r == rid => Some(*req),
                _ => None,
            })
            .unwrap_or_else(|| panic!("the replay forwards the rid: {out:?}"));
        // The client retries the rid meanwhile.
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: op.clone(),
            tag: Default::default(),
        });
        assert!(replies(&out).is_empty());
        let records = constellation_meta::execute_mutate(&holder.meta, &op, Some(rid)).unwrap();
        let out = requester.step(Event::Peer {
            from: 1,
            msg: PeerMsg::MutateReply {
                req,
                outcome: MutateOutcome::Accepted { epoch: 1, records },
                base: Some(0),
                position: constellation_meta::Position::ZERO,
                gen: 0,
                own_chunks: OwnChunks::None,
                own_rows: None,
            },
        });
        let r = replies(&out);
        assert_eq!(r.len(), 1, "the retrying client is answered: {out:?}");
        assert_eq!(r[0].0, rid);
        assert!(matches!(
            r[0].1,
            ClientReply::Outcome(MutateOutcome::Accepted { .. })
        ));
        assert!(requester.meta.pending_replays().unwrap().is_empty());
    }

    /// acks3 seed 700087: an op whose completion this (non-holding) node
    /// has applied from the log is answered from it at its deadline, not
    /// in doubt (the client was told `EIO` for a write the log carried).
    #[test]
    fn a_deadline_answers_from_the_applied_log() {
        let (holder, mut requester) = pair();
        let rid = requester.rid(1);
        let op = requester.create("d");
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: op.clone(),
            tag: Default::default(),
        });
        let deadline = timers(&out, TimerKind::ClientDeadline)[0];
        assert!(!sends(&out).is_empty(), "forwarded: {out:?}");
        // The holder's segment carrying it lands here (its reply is lost).
        let records = constellation_meta::execute_mutate(&holder.meta, &op, Some(rid)).unwrap();
        crate::replica::Replica::apply_segment(&requester.meta, 1, 1, 0, &[], &[], &records)
            .unwrap();
        requester.advance(60_000);
        let out = requester.step(Event::Timer { id: deadline });
        let r = replies(&out);
        assert_eq!(r.len(), 1, "{out:?}");
        assert!(
            matches!(r[0].1, ClientReply::Outcome(MutateOutcome::Accepted { .. })),
            "answered in doubt: {:?}",
            r[0].1
        );
    }

    /// long-acks3 seed 801715: an `ack=s3` holder's own write, parked for
    /// its segment, whose segment landed while the holder was paused
    /// past its lease: the acknowledgement is released (the row is in the
    /// log), not held until the client's deadline answers `EIO`.
    #[test]
    fn a_shipped_row_is_acknowledged_though_the_lease_lapsed() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        h.core.lease.held.as_mut().expect("held").0.ack_policy = AckPolicy::S3;
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: h.create("s"),
            tag: Default::default(),
        });
        assert!(replies(&out).is_empty(), "parked for the segment: {out:?}");
        // The segment lands; the holder learns it only after its lease
        // lapsed.
        let tip = h.meta.journal_tip().unwrap();
        let seqs: Vec<u64> = (1..=tip).collect();
        h.meta.ack_journal_rows_at(&seqs, 1).unwrap();
        h.advance(60_000);
        let out = activity(&mut h);
        let r = replies(&out);
        assert_eq!(r.len(), 1, "released: {out:?}");
        assert!(matches!(
            r[0].1,
            ClientReply::Outcome(MutateOutcome::Accepted { .. })
        ));
    }

    /// long-backup seed 56774: a holder's own op refused under a durable
    /// policy (its `Refused` row journaled, the answer parked), then the
    /// holder deposed before its recovery rolled the journal back. At the
    /// deadline the row is not the log's — the op will run by rid under
    /// the next holder, and may succeed there: the client hears in doubt,
    /// not the refusal (it heard `ENOENT` for a rename that then ran).
    #[test]
    fn a_deposed_holders_journaled_refusal_is_not_answered_at_the_deadline() {
        let mut h = Harness::new(1);
        h.hold(1, None);
        h.core.lease.held.as_mut().expect("held").0.ack_policy = AckPolicy::S3;
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: MutateOp::Unlink {
                parent: ROOT_INO,
                name: "missing".into(),
            },
            tag: Default::default(),
        });
        assert!(replies(&out).is_empty(), "parked for the segment: {out:?}");
        let deadline = timers(&out, TimerKind::ClientDeadline)[0];
        assert!(
            h.meta.completed_outcome(rid).unwrap().is_some(),
            "the refusal is journaled"
        );
        let mut out = Vec::new();
        h.core.deposed(h.now, 2, 2, 1, &h.meta, &mut out);
        h.advance(60_000);
        let out = h.step(Event::Timer { id: deadline });
        let r = replies(&out);
        assert_eq!(r.len(), 1, "{out:?}");
        assert!(
            matches!(r[0].1, ClientReply::InDoubt),
            "answered from the deposed journal: {:?}",
            r[0].1
        );
    }

    /// long-backup seed 802943: the holder, cut from S3, could not renew;
    /// a non-backup claimed its `Backup` lease at the expiry while the
    /// listed backup still held an acknowledged create, which came back
    /// only as the deposed holder's replay, after later ops. A node that
    /// is not a listed backup now waits `backup_claim_grace_ms` past the
    /// expiry; a listed backup may claim at once.
    #[test]
    fn a_non_backup_waits_the_grace_before_claiming_a_backup_lease() {
        let now = Ms(1_000_000);
        let lease = backup_lease(1, 1, now.0 - 1, vec![2]);
        let classify = |node: NodeId, at: Ms| {
            let h = Harness::new(node);
            let cfg = h.core.cfg.clone();
            h.core
                .lease
                .classify(at, &cfg, Some((lease.clone(), tag())), 0)
        };
        assert!(
            matches!(classify(3, now), Plan::Busy { .. }),
            "a non-backup claimed inside the grace"
        );
        assert!(
            matches!(classify(2, now), Plan::Claim { takeover: true, .. }),
            "the listed backup may claim at the expiry"
        );
        let grace = crate::core::backup_claim_grace_ms(&Config::defaults(3, 1));
        assert!(matches!(
            classify(3, Ms(lease.expires_unix_ms + grace)),
            Plan::Claim { takeover: true, .. }
        ));
        // A `Local` lease (no backups) is anyone's at the expiry.
        let local = lease_of(1, 1, now.0 - 1);
        let h = Harness::new(3);
        let cfg = h.core.cfg.clone();
        assert!(matches!(
            h.core.lease.classify(now, &cfg, Some((local, tag())), 0),
            Plan::Claim { .. }
        ));
    }

    /// EC2 follow-up 3b: a node that restarts with a persisted backup
    /// role has heard nothing because it was down. Before, it sealed its
    /// (healthy) holder's epoch 1.5 s after mounting and, still listed,
    /// took the lease over. Now silence counts from when a link to the
    /// holder is up; until then the lease is read, and only an expired
    /// one is sealed and taken over.
    #[test]
    fn a_restarted_backup_does_not_take_its_downtime_for_holder_silence() {
        for expired in [false, true] {
            let meta = Meta::open_in_memory().unwrap();
            meta.set_node_prefix(2).unwrap();
            crate::replica::Replica::set_backup_role(
                &meta,
                constellation_meta::BackupRole {
                    holder: 1,
                    epoch: 1,
                    config_version: 2,
                },
            );
            let mut cfg = Config::defaults(2, 1);
            cfg.ttl_ms = 10_000;
            let mut core = Core::new(cfg);
            let mut h = Harness {
                core: Core::new(Config::defaults(2, 1)),
                meta,
                now: Ms(1_000_000),
                manual_horizon: false,
            };
            let mut out = Vec::new();
            core.start(h.now, &h.meta, &mut out);
            h.core = core;
            let links = |connected: bool| Event::Peers {
                links: vec![crate::event::PeerLink {
                    node: 1,
                    connected,
                    last_seen: None,
                    rtt_ms: None,
                    since: None,
                }],
            };
            h.step(links(false));
            let watch = timers(&out, TimerKind::BackupWatch)[0];
            h.advance(1_600);
            let out = h.step(Event::Timer { id: watch });
            assert_eq!(h.core.stats.seals, 0, "sealed on its own downtime");
            let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
            let expires = if expired {
                h.now.0 - 1
            } else {
                h.now.0 + 5_000
            };
            let out = h.step(Event::S3 {
                op: get,
                result: S3Result::LeaseGet(Ok(Some((backup_lease(1, 1, expires, vec![2]), tag())))),
            });
            if expired {
                assert_eq!(
                    h.core.stats.seals, 1,
                    "an expired holder: seal and take over"
                );
                assert_eq!(h.meta.backup_sealed_epoch().unwrap(), 1);
                assert!(h.core.lease.takeover_permit.is_some(), "{out:?}");
                continue;
            }
            assert_eq!(h.core.stats.seals, 0, "a renewing holder was sealed");
            // The link comes up: from now on silence counts as before.
            h.step(links(true));
            let watch = timers(&out, TimerKind::BackupWatch)[0];
            h.advance(1_600);
            let out = h.step(Event::Timer { id: watch });
            assert_eq!(h.core.stats.seals, 0, "{out:?}");
            let watch = timers(&out, TimerKind::BackupWatch)[0];
            h.advance(1_600);
            let out = h.step(Event::Timer { id: watch });
            answer_takeover_read(&mut h, &out);
            assert_eq!(
                h.core.stats.seals, 1,
                "a holder silent over a live link (and still listing it) is sealed"
            );
        }
    }
}

// ---- plan 30 §M14: leased cross-node locks ----

mod locks {
    use super::*;
    use crate::action::{ControlOk, LockAnswer};
    use crate::event::{Control, LockOutcome, LockRenewEntry, LockRenewResult};
    use constellation_fs_core::Ino;
    use constellation_meta::locks::{GrantId, LocalLock, LockMode};
    use constellation_meta::Position;

    const X: LockMode = LockMode::Exclusive;
    const S: LockMode = LockMode::Shared;

    /// The rounds harness scenario (EC2 campaign 5's `index.lock`
    /// stall): a delegate's grants are capped by what is left of its
    /// delegation, which it renews at half its 5 s ttl — so it had as
    /// little as 1.5 s of authority, granted a lock of that ttl, and the
    /// holder (honouring `ttl − margin`, renewing at `ttl/2`) let it
    /// lapse before its first renewal while `git` still held the
    /// `flock`; the owner outwaited it and granted the lock to the other
    /// node. Now a delegate with less than `2 × margin` left grants
    /// nothing: the request parks and the delegation is renewed at once;
    /// once renewed, the grant is a full one.
    #[test]
    fn a_delegate_with_little_authority_left_renews_before_granting() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        let dir = h.meta.allocate_ino(ROOT_INO).unwrap();
        let f = h.meta.allocate_ino(dir).unwrap();
        crate::replica::Replica::apply_segment(
            &h.meta,
            1,
            1,
            0,
            &[],
            &[],
            &[
                LogRecord::Mkdir {
                    parent: ROOT_INO,
                    name: "d".into(),
                    ino: dir,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                    time_ns: 1,
                },
                LogRecord::Create {
                    parent: dir,
                    name: "turn.lock".into(),
                    ino: f,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    time_ns: 2,
                },
                LogRecord::Delegate {
                    dir,
                    node: 3,
                    gen: 7,
                    designated: false,
                    range: (0, 0),
                },
            ],
        )
        .unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let d = h.core.dl.mine.get_mut(&7).expect("installed");
        // 1.5 s of the delegation left: a 0.5 s lock at most.
        d.until = h.now.plus(1_500);
        d.renew = None;
        let out = request(&mut h, 2, 9, f, X, true);
        assert!(
            lock_replies(&out)
                .iter()
                .all(|(_, _, o)| !matches!(o, LockOutcome::Granted { .. })),
            "granted on 1.5 s of authority: {out:?}"
        );
        assert!(
            sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::DelegRenew { gen: 7, .. })),
            "the delegation is not renewed at once: {out:?}"
        );
        assert_eq!(h.core.stats.lock_short_authority_waits, 1);
        // Renewed: the parked request is granted at the waiter tick, for
        // long enough to be renewed.
        let d = h.core.dl.mine.get_mut(&7).expect("installed");
        d.until = h.now.plus(4_000);
        d.renew = None;
        let tick = timers(&out, TimerKind::LockWaiterTick);
        assert!(!tick.is_empty(), "no waiter tick: {out:?}");
        h.advance(100);
        let out = h.step(Event::Timer { id: tick[0] });
        let granted: Vec<u64> = pushes(&out)
            .into_iter()
            .map(|(_, _, o)| o)
            .chain(lock_replies(&out).into_iter().map(|(_, _, o)| o))
            .filter_map(|o| match o {
                LockOutcome::Granted { ttl_ms, .. } => Some(ttl_ms),
                _ => None,
            })
            .collect();
        assert_eq!(granted.len(), 1, "{out:?}");
        assert!(granted[0] >= 2_000, "a {}-ms grant", granted[0]);
    }

    /// `d/turn.lock` (`f`) in a replica whose table delegates `d` to
    /// node 3 as generation 7; returns `(dir, f)`.
    pub(super) fn delegated_file(meta: &Meta) -> (Ino, Ino) {
        let dir = meta.allocate_ino(ROOT_INO).unwrap();
        let f = meta.allocate_ino(dir).unwrap();
        crate::replica::Replica::apply_segment(
            meta,
            1,
            1,
            0,
            &[],
            &[],
            &[
                LogRecord::Mkdir {
                    parent: ROOT_INO,
                    name: "d".into(),
                    ino: dir,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                    time_ns: 1,
                },
                LogRecord::Create {
                    parent: dir,
                    name: "turn.lock".into(),
                    ino: f,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    time_ns: 2,
                },
                LogRecord::Delegate {
                    dir,
                    node: 3,
                    gen: 7,
                    designated: false,
                    range: (0, 0),
                },
            ],
        )
        .unwrap();
        (dir, f)
    }

    /// Executes two rows of generation 7 as node 3, the delegate.
    fn two_delegate_rows(h: &Harness, dir: Ino) {
        for n in 1..=2 {
            let op = MutateOp::Create {
                parent: dir,
                name: format!("r{n}"),
                ino: h.meta.allocate_ino(dir).unwrap(),
                mode: 0o644,
                uid: 0,
                gid: 0,
            };
            let rid = Rid {
                node: 3,
                incarnation: 1,
                seq: n,
            };
            let tag = constellation_meta::locks::LockTag::NONE;
            let (_, idx) = h
                .meta
                .delegate_execute(&op, Some(rid), 7, Default::default(), &tag, 0)
                .unwrap();
            assert_eq!(idx, n);
        }
    }

    /// strand-takeover-quiescence: a takeover's strand of a delegate's own
    /// rows marks the generation; a restarted core re-adopts it stopped
    /// and answers a recall at the log's index of the stream, not its
    /// counter.
    #[test]
    fn a_restarted_delegate_re_adopts_a_stranded_generation_stopped() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        let (dir, _) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        assert!(!h.core.dl.mine[&7].stopped);
        two_delegate_rows(&h, dir);
        h.meta.strand_for_takeover(2, &[7]).unwrap();
        assert_eq!(h.meta.delegate_idx(7).unwrap(), 2, "the counter stays");
        // The restart.
        h.core = Core::new(h.core.cfg.clone());
        h.core.lease.cached_holder = Some(1);
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        assert!(h.core.dl.mine[&7].stopped, "re-adopted live");
        let out = h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::DelegRecall {
                req: OpId(5),
                dir,
                gen: 7,
            },
        });
        let want = h.meta.log_stream_idx(7).unwrap();
        assert!(
            sends(&out).iter().any(|(to, m)| *to == 1
                && matches!(m, PeerMsg::DelegRecalled { gen: 7, through, .. } if *through == want)),
            "no recall answer at the log's index {want}: {out:?}"
        );
    }

    /// strand-takeover-quiescence: the takeover gate's strand of the
    /// delegate's own rows stops the generation at once.
    #[test]
    fn the_takeover_gate_stops_a_generation_whose_rows_it_stranded() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        let (dir, _) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        two_delegate_rows(&h, dir);
        h.hold(
            2,
            Some(PendingGate {
                epoch: 2,
                takeover: true,
                marker_shipped: true,
                drained: false,
                fast_prev: None,
                backup_tail_epoch: Some(1),
                shippable: true,
            }),
        );
        assert!(!h.core.dl.mine[&7].stopped);
        h.core.complete_gate(h.now, &h.meta, &mut out);
        assert!(h.meta.delegate_stranded(7).unwrap());
        assert!(h.core.dl.mine[&7].stopped, "the gate left it running");
    }

    /// overload-cascade-2 (`stress-ng-fs-nodes`): a file locked through
    /// a delegate is unlinked under its lock (`stress-ng`'s lock
    /// stressors do). No subtree contains it any more, so by location its
    /// owner is the root, and the holder renews there. The root never had
    /// the grant (the delegate minted it, or it moved there with the
    /// delegation): answered `Lost`, the holder's writes were discarded
    /// (`EIO`) while the delegate still honoured it; adopted at that
    /// renewal instead (this chunk's first round), another node routed to
    /// the root by location could be granted the inode first, release it,
    /// and then the first holder's grant was adopted — two exclusive
    /// holders, silently (the review's release-then-renew case). Now the
    /// delegate's grants on inodes that left its subtree go to the root
    /// with the batch that carries the unlink, and the root has them from
    /// the step that applies it; an unknown grant is never adopted.
    #[test]
    fn a_grant_on_a_file_unlinked_under_its_lock_moves_to_the_root_with_the_unlink() {
        let delegates_grant = GrantId { node: 3, seq: 1 };
        let moved_grant = GrantId { node: 1, seq: 77 };
        let renew_id = |h: &mut Harness, ino: Ino, grant: GrantId| {
            let out = h.step(Event::Peer {
                from: 2,
                msg: PeerMsg::LockRenew {
                    req: OpId(40),
                    entries: vec![LockRenewEntry {
                        ino,
                        grant,
                        mode: X,
                    }],
                },
            });
            let [(2, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
                panic!("expected an answer: {out:?}")
            };
            results.clone()
        };
        let renew = |h: &mut Harness, ino: Ino| renew_id(h, ino, delegates_grant);
        let root = || {
            let mut h = Harness::new(1);
            h.core.cfg.delegation = true;
            h.core.cfg.p2p = true;
            h.hold(1, None);
            let (dir, f) = delegated_file(&h.meta);
            let mut out = Vec::new();
            h.core.delegation_sync(h.now, &h.meta, &mut out);
            assert!(h.core.dl.gens.contains_key(&7), "gen 7 not learned");
            // The tenure's lock floor: it waits for the delegate's stream
            // head (its renewal) before the root grants anything.
            request(&mut h, 5, 99, ROOT_INO, X, false);
            h.step(Event::Peer {
                from: 3,
                msg: PeerMsg::DelegRenew {
                    req: OpId(98),
                    gen: 7,
                    round: 1,
                    backup: None,
                    stream_head: 0,
                    stream_head_at: 0,
                },
            });
            (h, dir, f)
        };
        let grant_of = |h: &Harness, id: GrantId, f: Ino| constellation_meta::locks::Grant {
            id,
            node: 2,
            ino: f,
            mode: X,
            until_ms: h.now.0 + 5_000,
            recalled: false,
            gen: 7,
            confirmed_ms: constellation_meta::locks::Grant::MINTED,
        };
        // The delegate's batch with the unlink row (index 1), carrying
        // `leaving`.
        let stream = |h: &mut Harness, dir: Ino, req: u64, leaving| {
            let out = h.step(Event::Peer {
                from: 3,
                msg: PeerMsg::DelegateStream {
                    req: OpId(req),
                    gen: 7,
                    round: 1,
                    txs: vec![constellation_meta::DelegateTx {
                        idx: 1,
                        rid: None,
                        records: vec![LogRecord::Unlink {
                            parent: dir,
                            name: "turn.lock".into(),
                            time_ns: 1,
                        }],
                        deps: Default::default(),
                    }],
                    leaving,
                    leaving_barriers: Vec::new(),
                },
            });
            assert!(
                sends(&out).iter().any(|(to, m)| *to == 3
                    && matches!(
                        m,
                        PeerMsg::DelegateStreamAck {
                            through: 1,
                            refused: false,
                            ..
                        }
                    )),
                "not appended: {out:?}"
            );
        };
        let granted_to_4 = |out: &[Action]| {
            lock_replies(out)
                .iter()
                .any(|(_, _, o)| matches!(o, LockOutcome::Granted { .. }))
        };
        // Still in the subtree: the delegate's, not the root's.
        let (mut h, dir, f) = root();
        assert!(
            matches!(
                renew(&mut h, f).as_slice(),
                [(_, _, LockRenewResult::NotOwner { owner: 3 })]
            ),
            "renewed by the root inside a delegated subtree"
        );
        // Unlinked: the grant came with the row, another node's request
        // waits for it, and its holder renews it here.
        let g = grant_of(&h, delegates_grant, f);
        stream(&mut h, dir, 50, vec![g]);
        let out = request(&mut h, 4, 9, f, X, false);
        assert!(!granted_to_4(&out), "granted over the moved grant: {out:?}");
        assert!(
            matches!(
                renew(&mut h, f).as_slice(),
                [(_, _, LockRenewResult::Ok { id, .. })] if *id == delegates_grant
            ),
            "the moved grant not renewed"
        );
        // Released here, then the same batch again (its acknowledgement
        // lost): the grant this root ended is not put back.
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino: f,
                grant: delegates_grant,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(h.meta.locks().get(delegates_grant).is_none());
        let g = grant_of(&h, delegates_grant, f);
        stream(&mut h, dir, 51, vec![g]);
        assert!(
            h.meta.locks().get(delegates_grant).is_none(),
            "a released grant reinstalled by a re-sent batch"
        );
        // A grant the root minted before the subtree was delegated (it
        // moved to the delegate) comes back the same way.
        let (mut h, dir, f) = root();
        let g = grant_of(&h, moved_grant, f);
        stream(&mut h, dir, 50, vec![g]);
        assert!(
            matches!(
                renew_id(&mut h, f, moved_grant).as_slice(),
                [(_, _, LockRenewResult::Ok { id, .. })] if *id == moved_grant
            ),
            "a grant that moved with the delegation not taken back"
        );
        // The review's case: a grant the root never got (here: the batch
        // carried none). Another node is granted the unlinked inode by
        // location, and releases it; the first holder's renewal is not
        // adopted — it is `Lost`, and its writes are fenced.
        let (mut h, dir, f) = root();
        stream(&mut h, dir, 50, Vec::new());
        let out = request(&mut h, 4, 9, f, X, false);
        let replies = lock_replies(&out);
        let [(4, _, LockOutcome::Granted { id: g4, .. })] = replies.as_slice() else {
            panic!("not granted by location: {out:?}")
        };
        let g4 = *g4;
        h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockReleased {
                ino: f,
                grant: g4,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(
            matches!(renew(&mut h, f).as_slice(), [(_, _, LockRenewResult::Lost)]),
            "an unknown grant adopted after another node held the lock"
        );
        // One next to a conflicting grant of this table stays out.
        let (mut h, dir, f) = root();
        h.meta.locks().install(constellation_meta::locks::Grant {
            id: GrantId { node: 1, seq: 5 },
            node: 4,
            gen: 0,
            ..grant_of(&h, moved_grant, f)
        });
        let g = grant_of(&h, delegates_grant, f);
        stream(&mut h, dir, 50, vec![g]);
        assert!(
            h.meta.locks().get(delegates_grant).is_none(),
            "installed next to a conflicting grant"
        );
        assert!(
            matches!(renew(&mut h, f).as_slice(), [(_, _, LockRenewResult::Lost)]),
            "renewed next to a conflicting grant"
        );
    }

    /// overload-cascade-2: the delegate's half of the move above. Its
    /// grant on a file unlinked under the lock leaves with the batch that
    /// carries the unlink, and leaves its table once the root
    /// acknowledged that batch (a recall must not hand it back over the
    /// root's own record).
    #[test]
    fn a_delegate_sends_its_grants_on_unlinked_files_with_the_unlink() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        let dir = delegated_to_me(&h.meta, 3);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let req = renew_req(&out, 7).expect("no renewal on install");
        h.step(super::renewed(&h, req, 7, 5_000));
        let f = h.meta.allocate_ino(dir).unwrap();
        let create = MutateOp::Create {
            parent: dir,
            name: "f".into(),
            ino: f,
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let rid = h.rid(1);
        let created = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: create,
            tag: Default::default(),
        });
        // Node 2 locks it here.
        let out = request(&mut h, 2, 9, f, X, false);
        let replies = lock_replies(&out);
        let [(2, _, LockOutcome::Granted { id, .. })] = replies.as_slice() else {
            panic!("not granted by the delegate: {out:?}")
        };
        let id = *id;
        let batch = |out: &[Action]| {
            sends(out).into_iter().find_map(|(to, m)| match m {
                PeerMsg::DelegateStream {
                    req,
                    gen: 7,
                    round,
                    txs,
                    leaving,
                    ..
                } if to == 1 => Some((
                    (*req, *round),
                    txs.last().map(|t| t.idx).unwrap_or(0),
                    leaving.clone(),
                )),
                _ => None,
            })
        };
        // The create's batch, acknowledged: the unlink goes out in a
        // batch of its own.
        if let Some(((req, round), last, leaving)) = batch(&created) {
            assert!(leaving.is_empty(), "{leaving:?}");
            h.step(Event::Peer {
                from: 1,
                msg: PeerMsg::DelegateStreamAck {
                    req,
                    gen: 7,
                    round,
                    through: last,
                    refused: false,
                },
            });
        }
        let rid = h.rid(2);
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: MutateOp::Unlink {
                parent: dir,
                name: "f".into(),
            },
            tag: Default::default(),
        });
        let ((sreq, round), last, leaving) = batch(&out).expect("the unlink not streamed");
        assert_eq!(
            leaving.iter().map(|g| (g.id, g.ino)).collect::<Vec<_>>(),
            vec![(id, f)],
            "the grant did not go with the unlink"
        );
        assert!(
            h.meta.locks().get(id).is_some(),
            "dropped before the root had it"
        );
        h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::DelegateStreamAck {
                req: sreq,
                gen: 7,
                round,
                through: last,
                refused: false,
            },
        });
        assert!(
            h.meta.locks().get(id).is_none(),
            "still in the delegate's table once the root had it"
        );
    }

    /// overload-cascade-2 review round 2 (sim
    /// `locks-unlinked-delegated-hcrash-backup` seed 41): the delegate
    /// grants a file, it is unlinked under the lock, and the batch that
    /// carries the unlink and the grant never reaches a live root. The
    /// delegate takes the root over and ends its own inherited
    /// generation: the grants on inodes that are the root's now are its
    /// own table's, not dropped — dropped, the holder's renewal was
    /// answered `Lost` at best, and the root granted the inode over it.
    #[test]
    fn a_delegate_that_takes_the_root_over_keeps_its_grants_on_unlinked_files() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        let dir = delegated_to_me(&h.meta, 3);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let req = renew_req(&out, 7).expect("no renewal on install");
        h.step(super::renewed(&h, req, 7, 5_000));
        let f = h.meta.allocate_ino(dir).unwrap();
        let rid = h.rid(1);
        h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: MutateOp::Create {
                parent: dir,
                name: "f".into(),
                ino: f,
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
            tag: Default::default(),
        });
        // Node 2 locks it here, and it is unlinked under the lock; no
        // batch is acknowledged (the root is gone).
        let out = request(&mut h, 2, 9, f, X, false);
        let replies = lock_replies(&out);
        let [(2, _, LockOutcome::Granted { id, .. })] = replies.as_slice() else {
            panic!("not granted by the delegate: {out:?}")
        };
        let id = *id;
        let rid = h.rid(2);
        h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: MutateOp::Unlink {
                parent: dir,
                name: "f".into(),
            },
            tag: Default::default(),
        });
        assert!(h.meta.locks().get(id).is_some_and(|g| g.gen == 7));
        // This node takes the root over and ends its own generation.
        h.core.lease.cached_holder = None;
        h.hold(2, None);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        assert!(
            !h.core.dl.mine.contains_key(&7),
            "the root's own generation not ended: {out:?}"
        );
        let g = h.meta.locks().get(id).expect("the grant dropped");
        assert_eq!((g.node, g.ino, g.gen), (2, f, 0));
        // Another node's request waits, the holder renews it here.
        let out = request(&mut h, 4, 10, f, X, false);
        assert!(
            !lock_replies(&out)
                .iter()
                .any(|(_, _, o)| matches!(o, LockOutcome::Granted { .. })),
            "granted over the kept grant: {out:?}"
        );
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(40),
                entries: vec![LockRenewEntry {
                    ino: f,
                    grant: id,
                    mode: X,
                }],
            },
        });
        assert!(
            matches!(
                renewed(&out, 2).as_deref(),
                Some([LockRenewResult::Ok { id: g, .. }]) if *g == id
            ),
            "the kept grant not renewed: {out:?}"
        );
    }

    /// overload-cascade-2 review round 2: a generation that ends without
    /// handing its grants back (outwaited, sealed, drained from its
    /// backup) may have granted an inode its rows then unlinked, and the
    /// grants on it never reached this root. The subtree's grace covers
    /// such an inode, which is under no directory any more; a linked
    /// inode elsewhere is not held up.
    #[test]
    fn an_outwaited_generations_grace_covers_the_inodes_it_unlinked() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        let (dir, f) = delegated_file(&h.meta);
        let other = h.meta.allocate_ino(ROOT_INO).unwrap();
        crate::replica::Replica::apply_segment(
            &h.meta,
            2,
            1,
            0,
            &[],
            &[],
            &[LogRecord::Create {
                parent: ROOT_INO,
                name: "other.lock".into(),
                ino: other,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 3,
            }],
        )
        .unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        // The tenure's floor: it waits for the delegate's renewal.
        request(&mut h, 5, 99, ROOT_INO, X, false);
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegRenew {
                req: OpId(98),
                gen: 7,
                round: 1,
                backup: None,
                stream_head: 0,
                stream_head_at: 0,
            },
        });
        // The delegate's unlink row is applied (from its backup, say),
        // without the grants it made on the file.
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegateStream {
                req: OpId(50),
                gen: 7,
                round: 1,
                txs: vec![constellation_meta::DelegateTx {
                    idx: 1,
                    rid: None,
                    records: vec![LogRecord::Unlink {
                        parent: dir,
                        name: "turn.lock".into(),
                        time_ns: 1,
                    }],
                    deps: Default::default(),
                }],
                leaving: Vec::new(),
                leaving_barriers: Vec::new(),
            },
        });
        h.core.lock_on_generation_outwaited(h.now, dir);
        // (`end_generation` marks it ended in the same step.)
        h.core.dl.gens.get_mut(&7).unwrap().ended = true;
        let granted = |out: &[Action]| {
            lock_replies(out)
                .iter()
                .any(|(_, _, o)| matches!(o, LockOutcome::Granted { .. }))
        };
        let out = request(&mut h, 4, 9, f, X, false);
        assert!(
            !granted(&out),
            "an unlinked inode granted inside the grace: {out:?}"
        );
        let out = request(&mut h, 4, 10, other, X, false);
        assert!(granted(&out), "a linked inode elsewhere held up: {out:?}");
        h.advance(h.core.lock_ttl_ms() as u64 + 2 * h.core.lock_margin_ms() as u64);
        let out = request(&mut h, 4, 11, f, X, false);
        assert!(granted(&out), "not granted once the grace passed: {out:?}");
    }

    /// Sim `locks-unlinked-delegated-dbackup-random` seed 5681: a
    /// generation is outwaited with grants this root moved to it still
    /// undelivered (its delegate never renewed), and the same step ends it
    /// and re-delegates the subtree (a parked cross-subtree op). The
    /// undelivered grants used to come back to this table only at the end
    /// of that step, restamped: a lapsed one revived, and both left in
    /// the table on inodes the new generation serves, never handed to it.
    /// Such a record of a holder later kept that holder's grant out when
    /// it came back with an unlink, and the root granted the inode over
    /// it. Now the generation's end returns them as recorded, live ones
    /// only, and the re-delegation takes them along.
    #[test]
    fn an_outwaited_generations_undelivered_grants_go_with_the_next_delegation() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        let (dir, f) = delegated_file(&h.meta);
        let lapsing = h.meta.allocate_ino(dir).unwrap();
        crate::replica::Replica::apply_segment(
            &h.meta,
            2,
            1,
            0,
            &[],
            &[],
            &[LogRecord::Create {
                parent: dir,
                name: "lapsing.lock".into(),
                ino: lapsing,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 3,
            }],
        )
        .unwrap();
        let grant =
            |seq: u64, node: NodeId, ino: Ino, until_ms: i64| constellation_meta::locks::Grant {
                id: GrantId { node: 1, seq },
                node,
                ino,
                mode: X,
                until_ms,
                recalled: false,
                gen: 0,
                confirmed_ms: constellation_meta::locks::Grant::UNCONFIRMED,
            };
        let live = grant(1, 4, f, h.now.0 + 5_000);
        h.meta.locks().install(live);
        h.meta.locks().install(grant(2, 5, lapsing, h.now.0 + 100));
        // Delegated to gen 7: both wait for its first renewal, which
        // never comes.
        h.core.lock_on_delegated(7, &h.meta);
        assert_eq!(h.meta.locks().grants_len(), 0);
        h.advance(200);
        let now = h.now;
        // One step: outwaited, ended, re-delegated as gen 8.
        h.core.lock_on_generation_outwaited(now, dir);
        h.core.lock_on_generation_ended(now, 7, &h.meta);
        crate::replica::Replica::apply_segment(
            &h.meta,
            3,
            1,
            0,
            &[],
            &[],
            &[
                LogRecord::Recall { dir, gen: 7 },
                LogRecord::Delegate {
                    dir,
                    node: 3,
                    gen: 8,
                    designated: false,
                    range: (0, 0),
                },
            ],
        )
        .unwrap();
        h.core.lock_on_delegated(8, &h.meta);
        let mut out = Vec::new();
        h.core.locks_after_event(now, &h.meta, &mut out);
        assert!(
            h.meta.locks().grants_snapshot().is_empty(),
            "left in the root's table under gen 8: {:?}",
            h.meta.locks().grants_snapshot()
        );
        let handed = h.core.lock_take_handoff(now, 8);
        assert_eq!(handed.len(), 1, "{handed:?}");
        assert_eq!(handed[0].id, live.id);
        assert_eq!(handed[0].until_ms, live.until_ms, "restamped: {handed:?}");
    }

    /// Sim `locks-unlinked-delegated-partition` seed 7455: the root
    /// recalled a generation whose delegate was paused, outwaited it and
    /// ended it. The delegate woke past its window, found the recall
    /// queued and answered it with the grants it still had, lapsed long
    /// before. The root installed them restamped with a fresh window:
    /// a dead holder's record came back to life on an inode the root now
    /// serves (there, kept a later generation's leaving grant out at an
    /// unlink, and the root upgraded the revived copy for its old holder
    /// beside the delegate's live exclusive grant). A recall answer that
    /// comes after its generation ended brings no grants back.
    #[test]
    fn a_recall_answer_after_its_generation_ended_brings_no_grants_back() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        let (dir, f) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let out = h.step(Event::Control {
            op: OpId(1 << 50),
            req: Control::Undelegate { dir },
        });
        let recall = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::DelegRecall { req, gen: 7, .. } if to == 3 => Some(*req),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no recall sent: {out:?}"));
        // The delegate stays silent: the recall is outwaited and the
        // generation ends.
        h.core.dl.gens.get_mut(&7).unwrap().until = h.now;
        let mut out = Vec::new();
        h.core.on_deleg_expiry(h.now, 7, &h.meta, &mut out);
        assert!(h.core.dl.gens[&7].ended, "not ended: {out:?}");
        // Past the outwait's grace on the subtree.
        h.advance(h.core.lock_ttl_ms() as u64 + 2 * h.core.lock_margin_ms() as u64);
        // The late answer: node 4's grant, lapsed at the delegate.
        let lapsed = constellation_meta::locks::Grant {
            id: GrantId { node: 3, seq: 5 },
            node: 4,
            ino: f,
            mode: X,
            until_ms: h.now.0 - 1_000,
            recalled: false,
            gen: 7,
            confirmed_ms: constellation_meta::locks::Grant::UNCONFIRMED,
        };
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegRecalled {
                req: recall,
                gen: 7,
                through: 0,
                locks: constellation_meta::locks::LockHandback {
                    grants: vec![lapsed],
                    floor: Position::ZERO,
                    barrier: 0,
                },
            },
        });
        assert!(
            h.meta.locks().grants_snapshot().is_empty(),
            "a lapsed grant revived: {:?}",
            h.meta.locks().grants_snapshot()
        );
        assert_eq!(h.core.stats.lock_returned_after_end, 1);
        // Another node is granted at once, and node 4 is not granted
        // over it on the strength of its old record.
        let out = request(&mut h, 5, 10, f, X, false);
        assert!(
            lock_replies(&out)
                .iter()
                .any(|(to, _, o)| *to == 5 && matches!(o, LockOutcome::Granted { .. })),
            "held up by the revived record: {out:?}"
        );
        let out = request(&mut h, 4, 11, f, X, false);
        assert!(
            !lock_replies(&out)
                .iter()
                .any(|(_, _, o)| matches!(o, LockOutcome::Granted { .. })),
            "two exclusive holders: {out:?}"
        );
    }

    /// The same late answer while the root is sealing the silent
    /// delegate's backup: the generation has not ended, and the seal
    /// pushed its `until` on, but the window was outwaited already (the
    /// grace stands). The answer's grants are not installed.
    #[test]
    fn a_recall_answer_while_the_generation_is_sealing_brings_no_grants_back() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        let (dir, f) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let out = h.step(Event::Control {
            op: OpId(1 << 50),
            req: Control::Undelegate { dir },
        });
        let recall = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::DelegRecall { req, gen: 7, .. } if to == 3 => Some(*req),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no recall sent: {out:?}"));
        let g = h.core.dl.gens.get_mut(&7).unwrap();
        g.recall = crate::core::delegate::RecallPhase::Sealing;
        g.until = h.now.plus(5_000);
        let live = constellation_meta::locks::Grant {
            id: GrantId { node: 3, seq: 5 },
            node: 4,
            ino: f,
            mode: X,
            until_ms: h.now.0 + 1_000,
            recalled: false,
            gen: 7,
            confirmed_ms: constellation_meta::locks::Grant::UNCONFIRMED,
        };
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegRecalled {
                req: recall,
                gen: 7,
                through: 0,
                locks: constellation_meta::locks::LockHandback {
                    grants: vec![live],
                    floor: Position::ZERO,
                    barrier: 0,
                },
            },
        });
        assert!(
            h.meta.locks().grants_snapshot().is_empty(),
            "a grant installed while sealing: {:?}",
            h.meta.locks().grants_snapshot()
        );
        assert_eq!(h.core.stats.lock_returned_after_end, 1);
    }

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

    fn request(
        h: &mut Harness,
        from: NodeId,
        req: u64,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
    ) -> Vec<Action> {
        request_inc(h, from, 1, req, ino, mode, blocking)
    }

    fn request_inc(
        h: &mut Harness,
        from: NodeId,
        incarnation: u32,
        req: u64,
        ino: Ino,
        mode: LockMode,
        blocking: bool,
    ) -> Vec<Action> {
        h.step(Event::Peer {
            from,
            msg: PeerMsg::LockRequest {
                req: OpId(req),
                ino,
                mode,
                blocking,
                sent: h.now,
                incarnation,
            },
        })
    }

    fn lock_replies(out: &[Action]) -> Vec<(NodeId, OpId, LockOutcome)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::LockReply { req, outcome } => Some((to, *req, outcome.clone())),
                _ => None,
            })
            .collect()
    }

    fn pushes(out: &[Action]) -> Vec<(NodeId, Ino, LockOutcome)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::LockGranted { ino, outcome, .. } => Some((to, *ino, outcome.clone())),
                _ => None,
            })
            .collect()
    }

    fn recalls(out: &[Action]) -> Vec<(NodeId, OpId, Ino, GrantId)> {
        sends(out)
            .into_iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::LockRecall { req, ino, grant } => Some((to, *req, *ino, *grant)),
                _ => None,
            })
            .collect()
    }

    fn granted(outcome: &LockOutcome) -> (GrantId, u64) {
        match outcome {
            LockOutcome::Granted { id, ttl_ms, .. } => (*id, *ttl_ms),
            other => panic!("expected a grant: {other:?}"),
        }
    }

    fn timer_of(out: &[Action], kind: TimerKind) -> TimerId {
        let t = timers(out, kind);
        assert_eq!(t.len(), 1, "expected one {kind:?} timer: {out:?}");
        t[0]
    }

    fn lock_answer(out: &[Action], op: u64) -> LockAnswer {
        out.iter()
            .find_map(|a| match a {
                Action::ControlDone {
                    op: o,
                    result: Ok(ControlOk::Lock(answer)),
                } if *o == OpId(op) => Some(*answer),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no lock answer for {op}: {out:?}"))
    }

    /// EC2 campaign 8 (a committer waiting 16–28 s while the other took
    /// turn after turn): the queue is served in the order the requests
    /// arrived, whatever the list order, and a holder that releases and
    /// asks again queues behind everyone parked meanwhile.
    #[test]
    fn waiters_are_served_in_arrival_order_and_a_re_request_queues_behind_them() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request(&mut h, 3, 8, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert_eq!(recalls(&out).len(), 1, "node 2 recalled at once: {out:?}");
        h.advance(10);
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let release = |h: &mut Harness, from: NodeId, grant: GrantId| {
            h.step(Event::Peer {
                from,
                msg: PeerMsg::LockReleased {
                    ino,
                    grant,
                    position: constellation_meta::Position::ZERO,
                },
            })
        };
        h.advance(100);
        let out = release(&mut h, 2, g2);
        let rs = lock_replies(&out);
        let [(3, OpId(8), o3)] = rs.as_slice() else {
            panic!("node 3 asked first: {out:?}")
        };
        let (g3, _) = granted(o3);
        assert_eq!(
            recalls(&out).len(),
            1,
            "node 4 still waits: node 3 recalled: {out:?}"
        );
        // Node 2 asks again right after its release: behind node 4.
        h.advance(10);
        let out = request(&mut h, 2, 17, ino, X, true);
        assert!(
            lock_replies(&out).is_empty(),
            "parked behind node 4: {out:?}"
        );
        h.advance(100);
        let out = release(&mut h, 3, g3);
        let rs = lock_replies(&out);
        let [(4, OpId(9), o4)] = rs.as_slice() else {
            panic!("node 4 asked before node 2's second request: {out:?}")
        };
        let (g4, _) = granted(o4);
        h.advance(100);
        let out = release(&mut h, 4, g4);
        let [(2, OpId(17), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("node 2's second request last: {out:?}")
        };
        assert_eq!(h.core.lock_waiters(), 0, "everyone served");
    }

    /// A grant that goes unused (the push was lost, or found no op) is
    /// outwaited; the node it was for asks again meanwhile and keeps its
    /// place in the queue — ahead of a node that asked after it — instead
    /// of parking behind it.
    #[test]
    fn a_waiter_whose_grant_went_unused_re_parks_at_its_old_position() {
        let (mut h, ino) = holder_with_file();
        let t0 = h.now;
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.now = t0.plus(100);
        let out = request(&mut h, 3, 8, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        h.now = t0.plus(200);
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        h.now = t0.plus(300);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let [(3, OpId(8), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("node 3 first: {out:?}")
        };
        // Node 4 conflicts with node 3's fresh grant: recalled at once.
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        assert_eq!(recalls(&out).len(), 1);
        // Node 3 never uses it (the reply was lost) and asks again: its
        // own grant is recalled, so it parks — at its old position. (A
        // live waiter re-sends every (ttl - margin) / 2; the last re-send
        // before the expiry is what its grant is stamped from.)
        h.now = t0.plus(5_000);
        let out = request(&mut h, 3, 18, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert_eq!(h.core.stats.lock_requeued_in_place, 1);
        // The unused grant is outwaited; node 3, first in line, is served
        // and node 4 waits for it (recalled at once).
        h.now = t0.plus(100 + 6_000);
        let out = h.step(Event::Timer { id: expiry });
        assert_eq!(h.core.stats.lock_recalls_expired, 1);
        let rs = lock_replies(&out);
        let [(3, OpId(18), LockOutcome::Granted { .. })] = rs.as_slice() else {
            panic!("node 3 keeps its place ahead of node 4: {out:?}")
        };
        assert_eq!(
            recalls(&out).len(),
            1,
            "node 4 recalls node 3's grant: {out:?}"
        );
    }

    /// Every grant in `out`, pushed or replied: `(to, id)`.
    fn grants_to(out: &[Action]) -> Vec<(NodeId, GrantId)> {
        pushes(out)
            .into_iter()
            .map(|(to, _, o)| (to, o))
            .chain(lock_replies(out).into_iter().map(|(to, _, o)| (to, o)))
            .filter_map(|(to, o)| match o {
                LockOutcome::Granted { id, .. } => Some((to, id)),
                _ => None,
            })
            .collect()
    }

    /// Fire every `LockHeldReply` timer in `out`: the parked requests
    /// are answered `Waiting`; returns their retry intervals by node.
    fn answer_held(h: &mut Harness, out: &[Action]) -> Vec<(NodeId, u64)> {
        let mut waits = Vec::new();
        for t in timers(out, TimerKind::LockHeldReply) {
            let out = h.step(Event::Timer { id: t });
            for (to, _, o) in lock_replies(&out) {
                let LockOutcome::Waiting { retry_ms } = o else {
                    panic!("held reply not Waiting: {o:?}")
                };
                waits.push((to, retry_ms));
            }
        }
        waits
    }

    /// `git-under-flock-causal` (35 minutes): node 3, the reader, came
    /// back under a new incarnation that this owner could not dial, while
    /// its own requests (and their replies) still arrived over its own
    /// connection. Each time the turn lock freed, node 3 was first in
    /// line with no request held, so the grant went out as a push and was
    /// lost; the committers' recall of it was lost too, so it was
    /// outwaited (`ttl + margin`), and node 3 — keeping its place — was
    /// pushed the next one. Now an undeliverable recall marks node 3
    /// unreachable: it loses its kept place, is passed over while it has
    /// no request held, re-sends at once, and is granted over a request
    /// of its own — never by push.
    #[test]
    fn an_unreachable_waiter_is_granted_over_its_own_requests_and_passed_over_meanwhile() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request_inc(&mut h, 3, 2, 8, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let normal = answer_held(&mut h, &out);
        assert_eq!(normal.len(), 1);
        let normal_retry = normal[0].1;
        h.advance(10);
        let out = request(&mut h, 4, 9, ino, X, true);
        answer_held(&mut h, &out);
        // Node 2 releases: node 3, first in line, is pushed its grant
        // (this owner does not know yet that it cannot reach node 3).
        h.advance(100);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let ps = pushes(&out);
        let [(3, _, LockOutcome::Granted { id: g3, .. })] = ps.as_slice() else {
            panic!("node 3 is pushed its grant: {out:?}")
        };
        let g3 = *g3;
        let rc = recalls(&out);
        let [(3, recall, _, _)] = rc.as_slice() else {
            panic!("node 4 recalls node 3's grant: {out:?}")
        };
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        // The push is lost, and the recall fails with no connection left.
        h.advance(500);
        h.step(Event::PeerFailed {
            req: *recall,
            to: 3,
            outage: true,
        });
        assert_eq!(h.core.stats.lock_peers_unreachable, 1);
        // Node 3 re-sends: its grant is recalled, so it parks — not at
        // its old place (ahead of node 4) — and is told to ask again at
        // once.
        h.advance(1_500);
        let out = request_inc(&mut h, 3, 2, 18, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let waits = answer_held(&mut h, &out);
        let [(3, retry)] = waits.as_slice() else {
            panic!("one Waiting for node 3: {waits:?}")
        };
        assert!(
            *retry < normal_retry,
            "an unreachable waiter re-sends at once ({retry} ms, normally {normal_retry} ms)"
        );
        assert_eq!(h.core.stats.lock_requeued_in_place, 0);
        // Node 4 re-sends as a live waiter does (its Waiting answered).
        h.advance(500);
        let out = request(&mut h, 4, 19, ino, X, true);
        answer_held(&mut h, &out);
        // Node 3's grant is outwaited: node 4 is served, not node 3.
        h.now = Ms(h.meta.locks().get(g3).unwrap().until_ms);
        let out = h.step(Event::Timer { id: expiry });
        assert_eq!(h.core.stats.lock_recalls_expired, 1);
        let gs = grants_to(&out);
        let [(4, g4)] = gs.as_slice() else {
            panic!("node 4 is served, not node 3: {out:?}")
        };
        let g4 = *g4;
        // Node 3 asks again (at once, as told): node 4 is recalled for it,
        // and node 4's release grants node 3 over its held request.
        h.advance(50);
        let out = request_inc(&mut h, 3, 2, 28, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert!(
            recalls(&out).iter().any(|(to, _, _, _)| *to == 4),
            "node 4 recalled for node 3: {out:?}"
        );
        h.advance(50);
        let out = h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g4,
                position: constellation_meta::Position::ZERO,
            },
        });
        let [(3, OpId(28), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("node 3 is granted over its own request: {out:?}")
        };
        assert!(pushes(&out).is_empty(), "{out:?}");
        assert!(h.core.stats.lock_unreachable_passed_over >= 1);
        // A recall node 3 acknowledges: reachable again.
        let out = request(&mut h, 2, 37, ino, X, true);
        let rc = recalls(&out);
        let [(3, recall, _, _)] = rc.as_slice() else {
            panic!("node 2 recalls node 3's grant: {out:?}")
        };
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockRecalled { req: *recall },
        });
        assert_eq!(h.core.lock_unreachable(), 0);
    }

    /// An unreachable waiter first in line, between two of its requests
    /// (its last one answered `Waiting`, the next moments away) when the
    /// lock frees: the queue waits for that request instead of serving
    /// the waiter behind it, which took the turn every time the release
    /// fell in that gap. Silent past the wait, it is passed over.
    #[test]
    fn the_queue_waits_briefly_for_an_unreachable_waiters_next_request() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        // Node 3 restarted: its new incarnation is not known reachable.
        h.advance(10);
        let out = request_inc(&mut h, 3, 1, 8, ino, X, true);
        answer_held(&mut h, &out);
        h.advance(10);
        let out = request_inc(&mut h, 3, 2, 9, ino, X, true);
        assert_eq!(h.core.lock_unreachable(), 1);
        answer_held(&mut h, &out);
        h.advance(10);
        let out = request(&mut h, 4, 10, ino, X, true);
        answer_held(&mut h, &out);
        let release = |h: &mut Harness, grant| {
            h.step(Event::Peer {
                from: 2,
                msg: PeerMsg::LockReleased {
                    ino,
                    grant,
                    position: constellation_meta::Position::ZERO,
                },
            })
        };
        // Node 2 releases while node 3 has no request held.
        h.advance(20);
        let out = release(&mut h, g2);
        assert!(grants_to(&out).is_empty(), "nobody served yet: {out:?}");
        // Node 3's next request is granted over itself.
        h.advance(30);
        let out = request_inc(&mut h, 3, 2, 11, ino, X, true);
        let rs = lock_replies(&out);
        let [(3, OpId(11), LockOutcome::Granted { id: g3, .. })] = rs.as_slice() else {
            panic!("node 3 is granted over its request: {out:?}")
        };
        let g3 = *g3;
        assert!(pushes(&out).is_empty(), "{out:?}");
        assert_eq!(h.core.stats.lock_unreachable_passed_over, 0);
        // Node 3 queues again and goes silent; node 4 holds meanwhile.
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g3,
                position: constellation_meta::Position::ZERO,
            },
        });
        let out = request(&mut h, 4, 12, ino, X, true);
        let (g4, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request_inc(&mut h, 3, 2, 13, ino, X, true);
        answer_held(&mut h, &out);
        h.advance(10);
        let out = request(&mut h, 2, 14, ino, X, true);
        answer_held(&mut h, &out);
        // Node 4 releases long after node 3's last request: node 2 is
        // served, node 3 passed over.
        h.advance(2_000);
        let out = h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g4,
                position: constellation_meta::Position::ZERO,
            },
        });
        let gs = grants_to(&out);
        let [(2, _)] = gs.as_slice() else {
            panic!("node 2 is served, node 3 passed over: {out:?}")
        };
        assert_eq!(h.core.stats.lock_unreachable_passed_over, 1);
    }

    /// The same without the transport saying so: grants pushed to a
    /// waiter that never uses them (no renewal, no release) — twice in a
    /// row — take it for unreachable, and the waiter behind it is served
    /// at the next expiry instead of it.
    #[test]
    fn a_waiter_leaving_two_pushed_grants_unused_loses_its_place() {
        let (mut h, ino) = holder_with_file();
        // Short grants: two outwaited ones fit in the harness's lease.
        h.core.cfg.lock_ttl_ms = 2_000;
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request(&mut h, 3, 8, ino, X, true);
        answer_held(&mut h, &out);
        h.advance(10);
        let out = request(&mut h, 4, 9, ino, X, true);
        answer_held(&mut h, &out);
        h.advance(100);
        let mut out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let mut req = 18;
        for round in 0..2 {
            let ps = pushes(&out);
            let [(3, _, LockOutcome::Granted { id, .. })] = ps.as_slice() else {
                panic!("round {round}: node 3 (first in line) is pushed: {out:?}")
            };
            let g3 = *id;
            let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
            // Lost; node 3 re-sends (its recall times out on a live
            // connection: no outage evidence), and re-parks in place; both
            // waiters keep re-sending until the expiry.
            let until = h.meta.locks().get(g3).unwrap().until_ms;
            for at in [h.now.0 + 500, until - 500] {
                h.now = Ms(at);
                let resend = request(&mut h, 3, req, ino, X, true);
                answer_held(&mut h, &resend);
                let resend = request(&mut h, 4, req + 1, ino, X, true);
                answer_held(&mut h, &resend);
                req += 10;
            }
            h.now = Ms(until);
            out = h.step(Event::Timer { id: expiry });
        }
        assert_eq!(
            h.core.stats.lock_requeued_in_place, 2,
            "kept its place after each unused grant"
        );
        assert_eq!(h.core.stats.lock_peers_unreachable, 1);
        // Node 3 asked 500 ms ago: the queue waits briefly for its next
        // request rather than push it a third grant. It stays silent;
        // node 4's next request is served, node 3 passed over.
        assert!(grants_to(&out).is_empty(), "{out:?}");
        // 800 ms after its request: past the wait, inside `ttl - margin`.
        h.advance(300);
        let out = request(&mut h, 4, req + 1, ino, X, true);
        let gs = grants_to(&out);
        let [(4, _)] = gs.as_slice() else {
            panic!("node 4 is served after node 3's second unused grant: {out:?}")
        };
        assert_eq!(h.core.stats.lock_unreachable_passed_over, 1);
    }

    /// A node that restarts while parked: its new incarnation's request
    /// drops what the previous one left queued (a waiter of another mode
    /// would otherwise hold the queue until its silence ran out, and its
    /// kept place would carry over), and a request still in flight from
    /// the previous incarnation is answered `Waiting` and not served.
    #[test]
    fn a_new_incarnation_drops_the_previous_ones_waiters() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request_inc(&mut h, 3, 1, 8, ino, X, true);
        answer_held(&mut h, &out);
        h.advance(10);
        let out = request(&mut h, 4, 9, ino, S, true);
        answer_held(&mut h, &out);
        assert_eq!(h.core.lock_waiters(), 2);
        // Node 3 restarts and asks for a shared lock.
        h.advance(100);
        let out = request_inc(&mut h, 3, 2, 1, ino, S, true);
        answer_held(&mut h, &out);
        assert_eq!(h.core.stats.lock_incarnation_waiters_dropped, 1);
        assert_eq!(h.core.lock_waiters(), 2, "node 4's, and node 3's new one");
        // A late request of the old incarnation: answered, not served.
        let out = request_inc(&mut h, 3, 1, 10, ino, X, true);
        let [(3, OpId(10), LockOutcome::Waiting { .. })] = lock_replies(&out).as_slice() else {
            panic!("the stale request is answered Waiting: {out:?}")
        };
        assert!(pushes(&out).is_empty(), "{out:?}");
        assert!(
            timers(&out, TimerKind::LockHeldReply).is_empty(),
            "not held: {out:?}"
        );
        assert_eq!(h.core.stats.lock_stale_incarnation_requests, 1);
        assert_eq!(h.core.lock_waiters(), 2);
        // The restarted node is not known reachable from here yet: it is
        // told to ask again at once, and granted over its own requests.
        assert_eq!(h.core.lock_unreachable(), 1);
        // Node 2 releases: node 4's shared waiter is granted; the old
        // exclusive one, had it stayed, would have gone first alone.
        // Node 3's new one, with no request held, is waited for...
        h.advance(100);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let ps = pushes(&out);
        let [(4, _, LockOutcome::Granted { mode: S, .. })] = ps.as_slice() else {
            panic!("node 4 is granted its shared lock: {out:?}")
        };
        // ... and granted alongside node 4 over its next request.
        h.advance(20);
        let out = request_inc(&mut h, 3, 2, 2, ino, S, true);
        let [(3, OpId(2), LockOutcome::Granted { mode: S, .. })] = lock_replies(&out).as_slice()
        else {
            panic!("node 3 is granted over its request: {out:?}")
        };
        assert_eq!(h.core.lock_waiters(), 0);
    }

    /// A waiter dropped for its node's new incarnation while its request
    /// is held here (`recall_hold_ms`) has that request answered: the
    /// RPC's reply is awaited on the serving side (the driver's reply
    /// table, the P2P task holding the stream), and dropped unanswered
    /// it waited there forever.
    #[test]
    fn a_held_request_of_a_restarted_nodes_dropped_waiter_is_answered() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        // Node 3's request is held, not answered yet.
        let out = request_inc(&mut h, 3, 1, 8, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "held: {out:?}");
        let held = timers(&out, TimerKind::LockHeldReply);
        assert_eq!(held.len(), 1, "{out:?}");
        // Node 3 restarts and asks again under its new incarnation.
        h.advance(10);
        let out = request_inc(&mut h, 3, 2, 1, ino, X, true);
        assert_eq!(h.core.stats.lock_incarnation_waiters_dropped, 1);
        let replies = lock_replies(&out);
        assert!(
            replies
                .iter()
                .any(|r| matches!(r, (3, OpId(8), LockOutcome::Waiting { .. }))),
            "the dropped waiter's held request is answered: {out:?}"
        );
        assert!(
            !replies.iter().any(|(_, req, _)| *req == OpId(1)),
            "the new request is held as any other: {out:?}"
        );
        assert_eq!(h.core.lock_waiters(), 1, "node 3's new one");
        // The old request's held timer no longer answers anything.
        for t in held {
            let out = h.step(Event::Timer { id: t });
            assert!(
                !lock_replies(&out).iter().any(|(_, req, _)| *req == OpId(8)),
                "answered twice: {out:?}"
            );
        }
        // The new one is granted over its held request once node 2 is
        // done.
        h.advance(10);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(
            pushes(&out).is_empty(),
            "not pushed to a restarted node: {out:?}"
        );
        let [(3, OpId(1), LockOutcome::Granted { mode: X, .. })] = lock_replies(&out).as_slice()
        else {
            panic!("node 3 is granted over its request: {out:?}")
        };
        assert_eq!(h.core.lock_waiters(), 0);
    }

    /// The owner re-affirms a grant under a new id for every answer
    /// (a re-sent request that crossed the old id's push); the node may
    /// release the old id. That release leaves the new one recalled, and
    /// the node, holding nothing on the inode and waiting for nothing
    /// there, answers the recall with its release (see
    /// `a_recall_of_a_newer_id_than_the_grant_released_is_released`): the
    /// waiter is served then, instead of after the grant is outwaited.
    #[test]
    fn a_release_naming_a_superseded_id_recalls_the_grant() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (old, _) = granted(&lock_replies(&out)[0].2);
        h.advance(10);
        let out = request(&mut h, 2, 8, ino, X, true);
        let (new, _) = granted(&lock_replies(&out)[0].2);
        assert!(new.seq > old.seq && new.node == old.node);
        assert!(h.meta.locks().get(old).is_none(), "replaced in the table");
        h.advance(10);
        let out = request(&mut h, 3, 9, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: old,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(lock_replies(&out).is_empty(), "not yet: {out:?}");
        // Recalled (already, for node 3's request), so node 2 answers it.
        assert!(h.meta.locks().get(new).is_some_and(|g| g.recalled));
        assert_eq!(h.core.stats.lock_released_superseded, 1);
        h.advance(10);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: new,
                position: constellation_meta::Position::ZERO,
            },
        });
        let [(3, OpId(9), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("the waiter is served on the new id's release: {out:?}")
        };
        assert!(h.meta.locks().get(new).is_none());
    }

    /// `locks-blips-tight-in-doubt` seed 1383: node 3 released its grant
    /// (unrecalled at the owner: re-claimed after a takeover) and asked
    /// again at once; the request overtook the release, and the owner
    /// re-affirmed the grant under a new id, which node 3 installed with
    /// a lock under it. The release of the old id ended that new grant
    /// too, and the owner granted the inode to itself beside node 3's:
    /// two exclusive holders. The new id is recalled instead, and nothing
    /// is granted over it until node 3 releases it.
    #[test]
    fn a_release_overtaken_by_the_nodes_next_request_keeps_the_new_grant() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 3, 7, ino, X, true);
        let (old, _) = granted(&lock_replies(&out)[0].2);
        // Node 3's next request (sent after its release) arrives first.
        h.advance(10);
        let out = request(&mut h, 3, 8, ino, X, true);
        let (new, _) = granted(&lock_replies(&out)[0].2);
        assert!(new.seq > old.seq);
        // Then the release of the old id.
        h.advance(10);
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: old,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(h.meta.locks().get(new).is_some_and(|g| g.recalled));
        assert!(
            recalls(&out)
                .iter()
                .any(|(to, _, i, g)| *to == 3 && *i == ino && *g == new),
            "{out:?}"
        );
        // The owner, as a lock user, waits behind node 3's live grant.
        let lock_granted = |out: &[Action]| {
            out.iter().any(|a| {
                matches!(
                    a,
                    Action::ControlDone {
                        op: OpId(5),
                        result: Ok(ControlOk::Lock(LockAnswer::Granted { .. })),
                    }
                )
            })
        };
        h.advance(10);
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::Lock {
                ino,
                mode: X,
                blocking: true,
            },
        });
        assert!(!lock_granted(&out), "granted beside node 3's: {out:?}");
        // Node 3 releases the new id once its lock is gone: served.
        h.advance(10);
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: new,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(lock_granted(&out), "{out:?}");
    }

    /// A recall of a newer id of the grant this node released last on
    /// the inode, with nothing held there since and no op waiting on it
    /// (the id that superseded the released one at its owner): nothing
    /// ran under it and nothing can install it, so it is released at
    /// once. Not so for an id it never had a release to vouch for (one
    /// it held and lost is outwaited by its owner: what ran under it may
    /// be in flight), nor with an op waiting, whose answer it may be: the
    /// reply installs it recalled.
    #[test]
    fn a_recall_of_a_newer_id_than_the_grant_released_is_released() {
        let mut r = requester();
        let recall = |r: &mut Harness, req: u64, ino: Ino, seq: u64| {
            let out = r.step(Event::Peer {
                from: 1,
                msg: PeerMsg::LockRecall {
                    req: OpId(req),
                    ino,
                    grant: GrantId { node: 1, seq },
                },
            });
            sends(&out)
                .into_iter()
                .filter_map(|(to, m)| match m {
                    PeerMsg::LockReleased { ino, grant, .. } => Some((to, *ino, *grant)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        // Never held here: kept for a reply, as before.
        assert!(recall(&mut r, 70, 44, 3).is_empty());
        // Grant 2 on inode 42, used and released.
        let req = lock_control(&mut r, 50, 42, true);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(2),
            },
        });
        let lock = LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        };
        r.meta.locks().local_set(42, lock, r.now.0);
        assert!(recall(&mut r, 71, 42, 2).is_empty(), "busy");
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        r.step(Event::Control {
            op: OpId(51),
            req: Control::LockIdle { ino: 42 },
        });
        let out = r.step(Event::LockFlushed {
            ino: 42,
            grant: GrantId { node: 1, seq: 2 },
            ok: true,
        });
        assert!(
            sends(&out)
                .iter()
                .any(|(_, m)| matches!(m, PeerMsg::LockReleased { .. })),
            "{out:?}"
        );
        // The owner had re-affirmed it as 3 meanwhile: released here.
        assert_eq!(
            recall(&mut r, 72, 42, 3),
            vec![(1, 42, GrantId { node: 1, seq: 3 })]
        );
        assert_eq!(r.core.stats.lock_recalls_unheld_released, 1);
        // An op waits on the inode now: id 4 may be its answer.
        let req = lock_control(&mut r, 52, 42, true);
        assert!(recall(&mut r, 73, 42, 4).is_empty());
        r.advance(10);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(4),
            },
        });
        assert!(r.meta.locks().held(42).is_some_and(|h| h.recalled));
    }

    /// The recall of the newer id overtook its push: released at once,
    /// it is tombstoned, so the late push — which a push installs for
    /// any later op on the inode — is refused for the next op, which asks
    /// again (sim `locks-failover` seed 5297: that op entered I/O under
    /// the released id beside the next holder's).
    #[test]
    fn a_push_behind_its_released_recall_is_refused() {
        let mut r = requester();
        // Grant 2 on inode 42, used, recalled and released.
        let req = lock_control(&mut r, 50, 42, true);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(2),
            },
        });
        let lock = LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        };
        r.meta.locks().local_set(42, lock, r.now.0);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(69),
                ino: 42,
                grant: GrantId { node: 1, seq: 2 },
            },
        });
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        r.step(Event::Control {
            op: OpId(51),
            req: Control::LockIdle { ino: 42 },
        });
        r.step(Event::LockFlushed {
            ino: 42,
            grant: GrantId { node: 1, seq: 2 },
            ok: true,
        });
        assert!(r.meta.locks().held(42).is_none());
        // The recall of 3 comes before its push: released at once.
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(70),
                ino: 42,
                grant: GrantId { node: 1, seq: 3 },
            },
        });
        assert!(
            sends(&out).iter().any(|(_, m)| matches!(
                m,
                PeerMsg::LockReleased {
                    grant: GrantId { node: 1, seq: 3 },
                    ..
                }
            )),
            "{out:?}"
        );
        assert_eq!(r.core.stats.lock_recalls_unheld_released, 1);
        // A new op on the inode; then the push of 3 lands.
        let sent = r.now;
        let _req = lock_control(&mut r, 52, 42, true);
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockGranted {
                ino: 42,
                sent,
                outcome: grant_msg(3),
            },
        });
        assert!(
            !out.iter()
                .any(|a| matches!(a, Action::ControlDone { op: OpId(52), .. })),
            "the released id answered the op: {out:?}"
        );
        assert!(r.meta.locks().held(42).is_none());
        assert_eq!(r.core.stats.lock_released_replies, 1);
        // It asks again.
        let retry = timer_of(&out, TimerKind::LockRetry);
        r.advance(1_000);
        let out = r.step(Event::Timer { id: retry });
        assert!(
            sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockRequest { .. })),
            "{out:?}"
        );
    }

    /// A push answers the op waiting for the inode even when it comes
    /// from a node other than the one the op last asked (the owner
    /// changed with a delegation, or a redirect is in flight): dropped,
    /// the grant would be outwaited by its owner.
    #[test]
    fn a_push_from_another_owner_than_the_one_asked_is_accepted() {
        let mut r = requester();
        let sent = r.now;
        let _req = lock_control(&mut r, 50, 42, true);
        r.advance(100);
        let out = r.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockGranted {
                ino: 42,
                sent,
                outcome: LockOutcome::Granted {
                    id: GrantId { node: 3, seq: 1 },
                    mode: X,
                    ttl_ms: 5_000,
                    position: Position::ZERO,
                },
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        assert_eq!(r.meta.locks().held(42).unwrap().owner, 3);
        assert_eq!(r.core.stats.lock_pushes_from_other_owner, 1);
    }

    /// The reply to a request a push already answered carries the id the
    /// owner re-affirmed the grant under: installed over the held id, the
    /// local lock and the flags kept, so the owner's recall of the new
    /// id lands here and the release names it.
    #[test]
    fn the_reply_to_a_request_a_push_answered_installs_the_owners_new_id() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(50);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockGranted {
                ino: 42,
                sent,
                outcome: grant_msg(2),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        // The application takes its lock under the pushed id.
        assert_eq!(
            r.meta.locks().local_set(
                42,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                r.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        r.advance(50);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(3),
            },
        });
        assert!(sends(&out).is_empty(), "nothing to say: {out:?}");
        let held = r.meta.locks().held(42).unwrap();
        assert_eq!(held.id, GrantId { node: 1, seq: 3 });
        assert!(!held.first_use, "the local lock continues under the new id");
        assert_eq!(r.core.stats.lock_late_replies_installed, 1);
        // The owner recalls the new id: lands here, released after the
        // unlock under that id.
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 3 },
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::LockRecalled { req: OpId(77) })]
        ));
        assert!(r.meta.locks().held(42).unwrap().recalled);
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        let out = r.step(Event::Control {
            op: OpId(51),
            req: Control::LockIdle { ino: 42 },
        });
        let flush = out
            .iter()
            .find_map(|a| match a {
                Action::LockFlush { ino: 42, grant } => Some(*grant),
                _ => None,
            })
            .expect("a flush before the release");
        assert_eq!(flush, GrantId { node: 1, seq: 3 });
    }

    /// Sim `locks-blips-tight-long-lease` seed 3270: the owner's recall of
    /// the re-affirmed id comes before the reply that carries it, and the
    /// recall adopts the id (`LockTables::recall_held`). The reply then
    /// extends the grant from its arrival, as a renewal answer would; the
    /// local lock and the recall continue. Dropped, the grant kept the
    /// push's window (counted from the first request) and lapsed under
    /// the I/O it was finishing.
    #[test]
    fn a_reply_whose_id_a_recall_already_adopted_extends_the_grant() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(300);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockGranted {
                ino: 42,
                sent,
                outcome: grant_msg(2),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let pushed_until = r.meta.locks().held(42).unwrap().until_ms;
        assert!(
            pushed_until <= sent.0 + 5_000,
            "the push's window counts from the first request"
        );
        assert_eq!(
            r.meta.locks().local_set(
                42,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                r.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 3 },
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::LockRecalled { req: OpId(77) })]
        ));
        assert_eq!(
            r.meta.locks().held(42).unwrap().id,
            GrantId { node: 1, seq: 3 }
        );
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(3),
            },
        });
        assert!(sends(&out).is_empty(), "nothing to say: {out:?}");
        let held = r.meta.locks().held(42).unwrap();
        assert_eq!(held.id, GrantId { node: 1, seq: 3 });
        assert!(
            held.until_ms >= pushed_until + 320,
            "extended from the reply: {} vs {pushed_until}",
            held.until_ms
        );
        assert!(held.recalled && !held.first_use && !held.releasing);
        assert_eq!(r.core.stats.lock_late_replies_installed, 1);
        // The I/O ends; the release names the adopted id.
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        let out = r.step(Event::Control {
            op: OpId(51),
            req: Control::LockIdle { ino: 42 },
        });
        assert!(
            out.iter().any(|a| matches!(
                a,
                Action::LockFlush {
                    ino: 42,
                    grant: GrantId { node: 1, seq: 3 }
                }
            )),
            "{out:?}"
        );
    }

    /// The holder is a lock user: its own request is answered in place,
    /// with no message; a second local request under the cached grant
    /// never reaches the core (the FUSE layer resolves it) but a
    /// conflicting peer recalls it.
    #[test]
    fn the_holder_grants_itself_locally_and_a_peer_recalls_it() {
        let (mut h, ino) = holder_with_file();
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::Lock {
                ino,
                mode: X,
                blocking: false,
            },
        });
        assert!(matches!(lock_answer(&out, 5), LockAnswer::Granted { .. }));
        assert!(sends(&out).is_empty(), "{out:?}");
        let held = h.meta.locks().held(ino).expect("installed");
        assert_eq!(held.until_ms, h.now.0 + 5_000 - 1_000);
        assert_eq!(h.meta.locks().grants_len(), 1);
        // A local lock under it, then a peer's conflicting request: the
        // grant is recalled here; nothing leaves until the local lock
        // goes (`LockIdle`), which flushes, then serves the waiter.
        assert_eq!(
            h.meta.locks().local_set(
                ino,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                h.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        let out = request(&mut h, 2, 7, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert!(recalls(&out).is_empty(), "a local recall sends nothing");
        assert!(h.meta.locks().held(ino).unwrap().recalled);
        assert!(h.meta.locks().local_unlock(ino, 9, 0, u64::MAX, h.now.0));
        let out = h.step(Event::Control {
            op: OpId(6),
            req: Control::LockIdle { ino },
        });
        let flush = out
            .iter()
            .find_map(|a| match a {
                Action::LockFlush { ino: i, grant } if *i == ino => Some(*grant),
                _ => None,
            })
            .expect("a flush before the release");
        let out = h.step(Event::LockFlushed {
            ino,
            grant: flush,
            ok: true,
        });
        let [(2, OpId(7), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("expected the waiter's grant: {out:?}")
        };
        assert!(h.meta.locks().held(ino).is_none());
        assert_eq!(h.core.stats.lock_recalls_released, 1);
    }

    /// The harness's `transport-lock-wait-budget` (a remount of a lone
    /// node): the grants a holder made itself died with its process, so
    /// they leave no restart quarantine — the restarted holder's first
    /// non-blocking lock on a fresh file is granted, not `EAGAIN` for 20 s.
    /// A grant a peer holds still quarantines the restart.
    #[test]
    fn a_holders_own_grants_do_not_quarantine_its_restart() {
        let (mut h, ino) = holder_with_file();
        let lock = |h: &mut Harness, op: u64, ino: Ino| {
            let out = h.step(Event::Control {
                op: OpId(op),
                req: Control::Lock {
                    ino,
                    mode: X,
                    blocking: false,
                },
            });
            lock_answer(&out, op)
        };
        assert!(matches!(lock(&mut h, 5, ino), LockAnswer::Granted { .. }));
        assert_eq!(h.meta.load_lock_quarantine(h.now.0), None);
        let restart = |h: Harness| {
            let mut core = Core::new(h.core.cfg.clone());
            core.start(h.now, &h.meta, &mut Vec::new());
            let mut h = Harness {
                core,
                meta: h.meta,
                now: h.now.plus(10),
                manual_horizon: false,
            };
            h.hold(2, None);
            h
        };
        let mut h = restart(h);
        let op = h.create("fresh");
        let MutateOp::Create { ino: fresh, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        assert!(
            matches!(lock(&mut h, 6, fresh), LockAnswer::Granted { .. }),
            "a fresh file after the restart"
        );
        // A peer's grant may be honoured past the restart.
        let op = h.create("peer");
        let MutateOp::Create { ino: peer, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let out = request(&mut h, 2, 7, peer, X, false);
        let [(2, OpId(7), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("{out:?}")
        };
        let mut h = restart(h);
        let op = h.create("later");
        let MutateOp::Create { ino: later, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        assert!(matches!(lock(&mut h, 8, later), LockAnswer::WouldBlock));
        assert_eq!(h.core.stats.lock_grace_refusals, 1);
    }

    /// Lock-fence-token review must-fix 1: a renewal persists the restart
    /// horizon as a grant does. The grant's own window ended long ago, its
    /// renewed one has not: a sequencer restarted inside its lease grants
    /// the lock to nobody else until the renewed window is over (it used to
    /// regrant once the *grant*'s horizon passed, while the holder still
    /// honoured its renewal).
    #[test]
    fn a_restart_waits_out_the_last_renewal_not_the_grant() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, false);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        let granted_horizon = h.meta.load_lock_quarantine(h.now.0).unwrap();
        let ttl = h.core.lock_ttl_ms();
        let mut renewed_until = 0;
        // Renew every half TTL until the grant's own horizon is long gone.
        for i in 0..6 {
            h.advance(ttl as u64 / 2);
            h.hold(1, None);
            let out = h.step(Event::Peer {
                from: 2,
                msg: PeerMsg::LockRenew {
                    req: OpId(20 + i),
                    entries: vec![LockRenewEntry {
                        ino,
                        grant: g2,
                        mode: X,
                    }],
                },
            });
            let [(2, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
                panic!("{out:?}")
            };
            assert!(
                matches!(results[0].2, LockRenewResult::Ok { .. }),
                "{results:?}"
            );
            renewed_until = h.now.0 + ttl + h.core.lock_margin_ms();
        }
        assert!(h.now.0 > granted_horizon, "the grant's own horizon passed");
        // Restart inside the lease (the holder's table is in memory).
        let mut core = Core::new(h.core.cfg.clone());
        core.start(h.now, &h.meta, &mut Vec::new());
        let mut h = Harness {
            core,
            meta: h.meta,
            now: h.now.plus(10),
            manual_horizon: false,
        };
        h.hold(2, None);
        let out = request(&mut h, 3, 30, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::WouldBlock)]
            ),
            "{out:?}"
        );
        h.now = Ms(renewed_until - 1);
        let out = request(&mut h, 3, 31, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(31), LockOutcome::WouldBlock)]
            ),
            "still inside the renewed window: {out:?}"
        );
        let horizon = h.meta.load_lock_quarantine(h.now.0).unwrap();
        assert!(horizon >= renewed_until, "{horizon} < {renewed_until}");
        h.now = Ms(horizon + 1);
        let out = request(&mut h, 3, 32, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(32), LockOutcome::Granted { .. })]
            ),
            "{out:?}"
        );
    }

    fn horizon_writes(out: &[Action]) -> Vec<i64> {
        out.iter()
            .filter_map(|a| match a {
                Action::PersistLockHorizon { until } => Some(*until),
                _ => None,
            })
            .collect()
    }

    fn renewed(out: &[Action], to: NodeId) -> Option<Vec<LockRenewResult>> {
        sends(out).into_iter().find_map(|(n, m)| match m {
            PeerMsg::LockRenewed { results, .. } if n == to => {
                Some(results.iter().map(|r| r.2).collect())
            }
            _ => None,
        })
    }

    fn renew(h: &mut Harness, from: NodeId, req: u64, ino: Ino, grant: GrantId) -> Vec<Action> {
        h.step(Event::Peer {
            from,
            msg: PeerMsg::LockRenew {
                req: OpId(req),
                entries: vec![LockRenewEntry {
                    ino,
                    grant,
                    mode: X,
                }],
            },
        })
    }

    /// Lock-fence-token fix round 4 (`git-under-flock-b2b` seed 7: one
    /// renewal blocked the core 12 s on a 7.6 s sync, and another node's
    /// grant lapsed meanwhile): the restart horizon is written off the
    /// core. A slow write holds only the answers that need it; the core
    /// keeps stepping — a refusal to another node goes out at once —
    /// and grants made while the write is in flight coalesce into the
    /// next write, which carries the highest horizon.
    #[test]
    fn a_slow_horizon_write_holds_only_the_answers_waiting_for_it() {
        let (mut h, ino) = holder_with_file();
        let other = {
            let op = h.create("g");
            let MutateOp::Create { ino, .. } = op else {
                unreachable!()
            };
            constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
            ino
        };
        h.manual_horizon = true;
        let out = request(&mut h, 2, 7, ino, X, false);
        assert!(
            lock_replies(&out).is_empty(),
            "answered before durable: {out:?}"
        );
        let first = horizon_writes(&out);
        assert_eq!(first.len(), 1, "{out:?}");
        let g2 = h
            .meta
            .locks()
            .own_grant(ino, 2, h.now.0)
            .expect("in the table");
        assert!(first[0] >= g2.until_ms);
        // The core keeps stepping: node 3's refusal needs no horizon.
        h.advance(2_000);
        let out = request(&mut h, 3, 8, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(8), LockOutcome::WouldBlock)]
            ),
            "{out:?}"
        );
        assert!(horizon_writes(&out).is_empty(), "{out:?}");
        // A grant to node 4 meanwhile: held, and no second write in flight.
        let out = request(&mut h, 4, 9, other, X, false);
        assert!(lock_replies(&out).is_empty(), "{out:?}");
        assert!(
            horizon_writes(&out).is_empty(),
            "one write at a time: {out:?}"
        );
        let g4 = h
            .meta
            .locks()
            .own_grant(other, 4, h.now.0)
            .expect("granted");
        // The first write lands: node 2's answer only (node 4's window
        // runs past it), and the next write carries node 4's.
        let durable = h.meta.note_lock_grant_horizon(first[0]).unwrap();
        assert!(durable < g4.until_ms, "the test needs a second write");
        let out = h.step(Event::LockHorizonPersisted {
            until: first[0],
            durable: Some(durable),
        });
        let rs = lock_replies(&out);
        let [(2, OpId(7), o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        assert_eq!(granted(o).0, g2.id);
        let second = horizon_writes(&out);
        assert_eq!(second.len(), 1, "{out:?}");
        assert!(second[0] >= g4.until_ms);
        let durable = h.meta.note_lock_grant_horizon(second[0]).unwrap();
        let out = h.step(Event::LockHorizonPersisted {
            until: second[0],
            durable: Some(durable),
        });
        let rs = lock_replies(&out);
        let [(4, OpId(9), o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        assert_eq!(granted(o).0, g4.id);
        assert!(horizon_writes(&out).is_empty(), "{out:?}");
        // Covered already: the next renewal inside it goes out at once.
        let out = renew(&mut h, 2, 20, ino, g2.id);
        assert!(
            matches!(
                renewed(&out, 2).as_deref(),
                Some([LockRenewResult::Ok { .. }])
            ),
            "{out:?}"
        );
        assert_eq!(h.core.stats.lock_horizon_writes, 2);
    }

    /// The safety rule the asynchronous write keeps: nothing is answered
    /// before its horizon is durable, so a restart after a renewal that
    /// was never answered waits out at least every window a peer was
    /// told of — and the renewal's own once its write landed.
    #[test]
    fn a_restart_after_an_unanswered_renewal_never_under_reports_the_horizon() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, false);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        let answered = h.meta.locks().get(g2).unwrap().until_ms;
        let ttl = h.core.lock_ttl_ms();
        h.manual_horizon = true;
        h.advance(ttl as u64 / 2);
        let out = renew(&mut h, 2, 20, ino, g2);
        assert!(
            renewed(&out, 2).is_none(),
            "answered before durable: {out:?}"
        );
        let pending = horizon_writes(&out);
        assert_eq!(pending.len(), 1, "{out:?}");
        // Crash before the write lands: the restart waits out the window
        // node 2 was told of.
        let horizon = h.meta.load_lock_quarantine(h.now.0).expect("quarantined");
        assert!(horizon >= answered, "{horizon} < {answered}");
        // The write lands: the renewal is answered, and a restart from
        // here waits out the renewed window.
        let durable = h.meta.note_lock_grant_horizon(pending[0]).unwrap();
        let out = h.step(Event::LockHorizonPersisted {
            until: pending[0],
            durable: Some(durable),
        });
        assert!(
            matches!(
                renewed(&out, 2).as_deref(),
                Some([LockRenewResult::Ok { .. }])
            ),
            "{out:?}"
        );
        let renewed_until = h.meta.locks().get(g2).unwrap().until_ms;
        assert!(renewed_until > answered);
        let horizon = h.meta.load_lock_quarantine(h.now.0).expect("quarantined");
        assert!(horizon >= renewed_until, "{horizon} < {renewed_until}");
    }

    /// A horizon write that fails refuses what it held, as the
    /// synchronous write did: the grant `Busy` (and gone from the table,
    /// so nobody waits it out), the renewal `NotOwner { 0 }`.
    #[test]
    fn a_failed_horizon_write_refuses_the_answers_it_held() {
        let (mut h, ino) = holder_with_file();
        h.manual_horizon = true;
        let out = request(&mut h, 2, 7, ino, X, false);
        let until = horizon_writes(&out)[0];
        let out = h.step(Event::LockHorizonPersisted {
            until,
            durable: None,
        });
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(2, OpId(7), LockOutcome::Busy)]
            ),
            "{out:?}"
        );
        assert!(h.meta.locks().own_grant(ino, 2, h.now.0).is_none());
        assert_eq!(h.core.stats.lock_horizon_failed, 1);
        // Asked again: a new write, and this time it lands.
        h.manual_horizon = false;
        let out = request(&mut h, 2, 8, ino, X, false);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        h.manual_horizon = true;
        // Past the rounding of that write: the renewal needs a new one.
        h.advance(2_000);
        let out = renew(&mut h, 2, 20, ino, g2);
        let until = horizon_writes(&out)[0];
        let out = h.step(Event::LockHorizonPersisted {
            until,
            durable: None,
        });
        assert!(
            matches!(
                renewed(&out, 2).as_deref(),
                Some([LockRenewResult::NotOwner { owner: 0 }])
            ),
            "{out:?}"
        );
    }

    /// EC2 campaign 4 B-1: a lock orders *every* write of its previous
    /// holder before the next holder's reads, not only the locked file's.
    /// The release carries what its node was acknowledged (here: the
    /// root's unshipped journal through 77 and a delegation stream,
    /// neither about the locked file), and every later grant of the file
    /// carries it — the next one, and one after a holder that released
    /// with nothing new.
    #[test]
    fn a_grant_carries_what_the_previous_holder_released_at() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let rs = lock_replies(&out);
        let [(2, OpId(7), o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let (g2, _) = granted(o);
        let out = request(&mut h, 3, 8, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let mut released = Position {
            seq: 3,
            pending: Some(constellation_meta::JournalPos { epoch: 1, jseq: 77 }),
            streams: constellation_meta::Streams::NONE,
        };
        assert!(released.streams.raise(5, 9));
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: released,
            },
        });
        let rs = lock_replies(&out);
        let [(
            3,
            OpId(8),
            LockOutcome::Granted {
                id: g3, position, ..
            },
        )] = rs.as_slice()
        else {
            panic!("expected node 3's grant: {out:?}")
        };
        assert!(
            position.dominates(&released),
            "the grant {position:?} does not cover the release {released:?}"
        );
        // Node 3 releases having seen nothing new; node 4's grant still
        // orders after node 2's writes.
        let g3 = *g3;
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g3,
                position: Position::ZERO,
            },
        });
        let rs = lock_replies(&out);
        let [(4, OpId(9), LockOutcome::Granted { position, .. })] = rs.as_slice() else {
            panic!("expected node 4's grant: {out:?}")
        };
        assert!(position.dominates(&released), "{position:?}");
    }

    /// The same when the previous holder is the root holder itself: its
    /// own clients' writes sit in its unshipped journal and raised no
    /// frontier, so its release covers the whole journal.
    #[test]
    fn the_holders_own_release_covers_its_unshipped_journal() {
        let (mut h, ino) = holder_with_file();
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::Lock {
                ino,
                mode: X,
                blocking: false,
            },
        });
        assert!(matches!(lock_answer(&out, 5), LockAnswer::Granted { .. }));
        let local = LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        };
        assert_eq!(
            h.meta.locks().local_set(ino, local, h.now.0),
            constellation_meta::locks::LocalOutcome::Done
        );
        // The holder's client writes another file under the lock.
        let op = h.create("refs");
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let journal = h.meta.journal_position(1).expect("unshipped");
        let out = request(&mut h, 2, 7, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert!(h.meta.locks().local_unlock(ino, 9, 0, u64::MAX, h.now.0));
        let out = h.step(Event::Control {
            op: OpId(6),
            req: Control::LockIdle { ino },
        });
        let flush = out
            .iter()
            .find_map(|a| match a {
                Action::LockFlush { ino: i, grant } if *i == ino => Some(*grant),
                _ => None,
            })
            .expect("a flush before the release");
        let out = h.step(Event::LockFlushed {
            ino,
            grant: flush,
            ok: true,
        });
        let rs = lock_replies(&out);
        let [(2, OpId(7), LockOutcome::Granted { position, .. })] = rs.as_slice() else {
            panic!("expected the waiter's grant: {out:?}")
        };
        assert!(
            position.pending >= Some(journal),
            "the grant {position:?} does not cover the holder's journal {journal:?}"
        );
    }

    /// A release position with a pending journal row and a stream.
    fn a_release() -> Position {
        let mut p = Position {
            seq: 3,
            pending: Some(constellation_meta::JournalPos { epoch: 1, jseq: 77 }),
            streams: constellation_meta::Streams::NONE,
        };
        assert!(p.streams.raise(5, 9));
        p
    }

    /// Node 2 holds `ino` exclusively and releases at `released`.
    fn grant_and_release(h: &mut Harness, ino: Ino, released: Position) {
        let out = request(h, 2, 70, ino, X, true);
        let rs = lock_replies(&out);
        let [(2, _, o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let (g2, _) = granted(o);
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: released,
            },
        });
    }

    /// Owner changes keep lock-to-unlock coherence: a fast successor
    /// installs the predecessor's mirrored floor under the whole
    /// namespace, so its first grant covers a release it never saw.
    #[test]
    fn a_fast_successor_grants_over_the_mirrored_floor() {
        let released = a_release();
        // The predecessor mirrors every floor it knows.
        let (mut h1, ino) = holder_with_file();
        grant_and_release(&mut h1, ino, released);
        {
            let lease = &mut h1.core.lease.held.as_mut().unwrap().0;
            lease.backups = vec![2];
            lease.ack_policy = constellation_store_s3::AckPolicy::Backup;
        }
        // (Answered `Busy` without fresh S3 liveness; the mirror of the
        // table as it stands goes out after the event either way.)
        h1.core.lk.mirror_dirty = true;
        let out = request(&mut h1, 3, 71, ino, S, true);
        let mirrored = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::LockMirror { floor, .. } if to == 2 => Some(*floor),
                _ => None,
            })
            .expect("a mirror to the backup");
        assert!(mirrored.dominates(&released), "{mirrored:?}");
        // The backup takes over with that mirror.
        let mut h2 = Harness::new(2);
        let op = h2.create("f");
        constellation_meta::execute_mutate(&h2.meta, &op, None).unwrap();
        let MutateOp::Create { ino: ino2, .. } = op else {
            unreachable!()
        };
        h2.core.lease.cached_holder = Some(1);
        h2.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockMirror {
                ver: 1,
                grants: Vec::new(),
                floor: mirrored,
            },
        });
        h2.hold(2, None);
        let now = h2.now;
        h2.core.lock_install_mirror(now, &h2.meta);
        let out = request(&mut h2, 3, 72, ino2, X, true);
        let rs = lock_replies(&out);
        let [(3, _, LockOutcome::Granted { position, .. })] = rs.as_slice() else {
            panic!("expected a grant: {out:?}")
        };
        assert!(
            position.dominates(&released),
            "the successor's grant {position:?} does not cover the release {released:?}"
        );
        assert!(h2.core.stats.lock_dir_floors > 0);
    }

    /// `lock-grant-dead-generation` (b's uncontended `flock` on a root
    /// file waited 451 s): a new tenure grants nothing until the
    /// delegations it *inherited* have renewed with it, but it collected
    /// them at its first grant, so a generation it had just made itself
    /// counted too, and the grant waited for that delegate's renewal (or
    /// its reclaim, when the renewal was lost). Its own generation's
    /// stream starts in this tenure: no earlier floor names it.
    #[test]
    fn a_new_tenure_does_not_wait_for_a_delegation_it_made_itself() {
        let (mut h, ino) = holder_with_file();
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        let op = h.create("d1");
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let MutateOp::Create { ino: dir, .. } = op else {
            unreachable!()
        };
        let out = h.step(Event::Control {
            op: OpId(900),
            req: Control::Delegate {
                dir,
                node: 3,
                range: (0, 0),
            },
        });
        assert!(
            out.iter().any(|a| matches!(
                a,
                Action::ControlDone {
                    op: OpId(900),
                    result: Ok(_)
                }
            )),
            "not delegated: {out:?}"
        );
        h.advance(10);
        let out = request(&mut h, 2, 74, ino, X, true);
        let rs = lock_replies(&out);
        assert!(
            matches!(rs.as_slice(), [(2, _, LockOutcome::Granted { .. })]),
            "the root file's lock waited for the new delegate's renewal: {out:?}"
        );
        assert_eq!(h.core.stats.lock_tenure_waits, 0);
    }

    /// A holder that lost its lease and holds it again keeps its floors
    /// (positions are the cluster's), and its new tenure's first grant
    /// floors everything with what it has.
    #[test]
    fn floors_survive_a_lease_gone_and_back() {
        let released = a_release();
        let (mut h, ino) = holder_with_file();
        grant_and_release(&mut h, ino, released);
        let now = h.now;
        let mut out = Vec::new();
        h.core.lock_on_lease_gone(now, &h.meta, &mut out);
        h.hold(2, None);
        let out = request(&mut h, 3, 73, ino, X, true);
        let rs = lock_replies(&out);
        let [(3, _, LockOutcome::Granted { position, .. })] = rs.as_slice() else {
            panic!("expected a grant: {out:?}")
        };
        assert!(position.dominates(&released), "{position:?}");
        assert!(h.core.stats.lock_dir_floors > 0, "no tenure floor noted");
    }

    /// An outwaited generation's subtree gets this root's own position as
    /// a floor (its holders' releases are lost with the delegate).
    #[test]
    fn an_outwaited_generation_leaves_a_floor_on_its_subtree() {
        let (mut h, ino) = holder_with_file();
        // Unshipped journal here: what the delegate's appended stream
        // would be.
        let op = h.create("appended");
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let journal = h.meta.journal_position(1).expect("unshipped");
        let now = h.now;
        h.core
            .lock_on_generation_outwaited(now, constellation_fs_core::types::ROOT_INO);
        // (The subtree grace is not what this test is about.)
        h.core.lk.grace.clear();
        // The next event notes the floor.
        let out = request(&mut h, 3, 74, ino, X, true);
        let rs = lock_replies(&out);
        let [(3, _, o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let (g3, _) = granted(o);
        assert!(h.core.stats.lock_dir_floors > 0);
        let out = request(&mut h, 4, 75, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g3,
                position: Position::ZERO,
            },
        });
        let rs = lock_replies(&out);
        let [(4, _, LockOutcome::Granted { position, .. })] = rs.as_slice() else {
            panic!("expected node 4's grant: {out:?}")
        };
        assert!(position.pending >= Some(journal), "{position:?}");
    }

    /// A delegate's renewal of generation 7 reporting `head`, sent at `at`
    /// on its clock.
    fn renew_gen7(h: &mut Harness, req: u64, head: u64, at: i64) -> Vec<Action> {
        h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegRenew {
                req: OpId(req),
                gen: 7,
                round: 0,
                backup: None,
                stream_head: head,
                stream_head_at: at,
            },
        })
    }

    /// Any grant to `node` among the replies and pushes in `out`.
    fn grant_to(out: &[Action], node: NodeId) -> Option<Position> {
        lock_replies(out)
            .into_iter()
            .map(|(to, _, o)| (to, o))
            .chain(pushes(out).into_iter().map(|(to, _, o)| (to, o)))
            .find_map(|(to, o)| match o {
                LockOutcome::Granted { position, .. } if to == node => Some(position),
                _ => None,
            })
    }

    /// stale-read-outwaited (sim `locks-unlinked-delegated-dbackup-random`
    /// seed 276): a holder partitioned from the root is outwaited; what it
    /// was acknowledged under its grant sits in a delegate's stream the
    /// root has not appended, so the next grant on the inode carried only
    /// the root's own position and its holder read the older state. Now
    /// the outwait leaves a barrier: the next grant waits for a cut as of
    /// the grant record's end — every live generation's head from a
    /// renewal its delegate sent at or after it — and carries it.
    #[test]
    fn an_outwaited_holders_successor_waits_for_every_live_streams_head() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        // The tenure's floor waits for the inherited delegate's renewal.
        request(&mut h, 5, 99, ROOT_INO, X, false);
        let now = h.now.0;
        renew_gen7(&mut h, 90, 2, now);
        let t0 = h.now;
        let out = request(&mut h, 2, 7, ino, X, true);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let out = request(&mut h, 4, 8, ino, X, true);
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        let end = t0.plus(6_000);
        assert_eq!(h.core.timer_at(expiry), Some(end));
        // Node 2 never answers: outwaited, but not granted on.
        h.now = end;
        let out = h.step(Event::Timer { id: expiry });
        assert_eq!(h.core.stats.lock_recalls_expired, 1);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past an outwait: {out:?}"
        );
        assert!(h.core.stats.lock_barrier_waits > 0);
        // A renewal sent before the record's end (it arrived late) says
        // nothing about what was executed after it.
        h.advance(100);
        renew_gen7(&mut h, 91, 4, end.0 - 1);
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted on a stale head: {out:?}"
        );
        // One sent at it settles the barrier: the grant carries its head.
        h.advance(100);
        renew_gen7(&mut h, 92, 5, end.0);
        let out = request(&mut h, 4, 10, ino, X, true);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted once every live stream was heard from: {out:?}")
        };
        assert!(
            position.streams.get(7) >= Some(5),
            "the grant does not cover the delegate's head: {position:?}"
        );
        assert_eq!(
            h.core
                .lk
                .container_sizes()
                .iter()
                .find(|(n, _)| *n == "lk_barriers")
                .map(|(_, s)| *s),
            Some(0)
        );
        // Settled once: the next grant does not wait.
        let waits = h.core.stats.lock_barrier_waits;
        let (g4, _) = granted(&LockOutcome::Granted {
            id: h.meta.locks().own_grant(ino, 4, h.now.0).unwrap().id,
            mode: X,
            ttl_ms: 0,
            position,
        });
        h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g4,
                position: Position::ZERO,
            },
        });
        let out = request(&mut h, 6, 11, ino, X, true);
        assert!(grant_to(&out, 6).is_some(), "{out:?}");
        assert_eq!(h.core.stats.lock_barrier_waits, waits);
    }

    /// A root with generation 7 (node 3's) heard from before node 2's
    /// grant on a root file was outwaited, and node 4's request parked
    /// behind the barrier: `(h, ino)`.
    fn outwaited_beside_gen7() -> (Harness, Ino) {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        let now = h.now.0;
        renew_gen7(&mut h, 90, 2, now);
        request(&mut h, 2, 7, ino, X, true);
        let out = request(&mut h, 4, 8, ino, X, true);
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        h.now = h.core.timer_at(expiry).unwrap();
        let out = h.step(Event::Timer { id: expiry });
        assert!(grant_to(&out, 4).is_none(), "{out:?}");
        (h, ino)
    }

    /// A live holder whose grant record lapsed at the owner (its renewal
    /// came late) re-asks: the barrier its own record left does not hold
    /// it — what it was acknowledged under the grant is in its own
    /// position — and the new grant ends the barrier. Another node still
    /// waits on it.
    #[test]
    fn a_lapsed_holder_does_not_wait_on_its_own_outwait_barrier() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        let now = h.now.0;
        renew_gen7(&mut h, 90, 2, now);
        let out = request(&mut h, 2, 7, ino, X, false);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let end = h.meta.locks().own_grant(ino, 2, h.now.0).unwrap().until_ms;
        h.now = Ms(end + 10);
        let out = request(&mut h, 4, 8, ino, X, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past an outwait: {out:?}"
        );
        assert_eq!(h.core.stats.lock_barrier_waits, 1);
        let out = request(&mut h, 2, 9, ino, X, false);
        assert!(
            grant_to(&out, 2).is_some(),
            "the lapsed holder waited on its own record: {out:?}"
        );
        assert_eq!(h.core.stats.lock_barrier_waits, 1);
        assert_eq!(
            h.core
                .lk
                .container_sizes()
                .iter()
                .find(|(n, _)| *n == "lk_barriers")
                .map(|(_, s)| *s),
            Some(0),
            "the new grant did not end the barrier"
        );
    }

    /// An outwaited holder whose stream ended: the generation counts no
    /// more (what the root appended of it is in the root's own position,
    /// the rest was never appended and its acknowledgements are rolled
    /// back), so the barrier settles without its renewal.
    #[test]
    fn an_ended_generation_does_not_hold_an_outwait_barrier() {
        let (mut h, ino) = outwaited_beside_gen7();
        h.core.dl.gens.get_mut(&7).unwrap().ended = true;
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(
            grant_to(&out, 4).is_some(),
            "an ended stream held the barrier: {out:?}"
        );
    }

    /// The lapsed holder granted *shared* again does not end its barrier:
    /// another node's shared grant beside it does not wait for its
    /// release, so it still waits for the cut (sim
    /// `locks-unlinked-delegated-partition` seed 390).
    #[test]
    fn a_lapsed_holders_shared_grant_keeps_its_barrier_for_others() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        let now = h.now.0;
        renew_gen7(&mut h, 90, 2, now);
        let out = request(&mut h, 2, 7, ino, X, false);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let end = h.meta.locks().own_grant(ino, 2, h.now.0).unwrap().until_ms;
        h.now = Ms(end + 10);
        let out = request(&mut h, 2, 8, ino, S, false);
        assert!(
            grant_to(&out, 2).is_some(),
            "the lapsed holder waited on its own record: {out:?}"
        );
        let out = request(&mut h, 4, 9, ino, S, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "a shared grant beside the lapsed holder skipped its barrier: {out:?}"
        );
        assert_eq!(h.core.stats.lock_barrier_waits, 1);
        h.advance(100);
        renew_gen7(&mut h, 91, 3, end);
        let out = request(&mut h, 4, 10, ino, S, false);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on a cut as of the record's end: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(3), "{position:?}");
    }

    /// An offline designation's designee writes while isolated and is
    /// never reclaimed: silent for a delegation TTL (here its renewal came
    /// a whole lock TTL before the outwait), its head joins the cut, but
    /// its silence does not hold every grant after an outwait.
    #[test]
    fn an_offline_designation_does_not_hold_an_outwait_barrier() {
        let (mut h, ino) = outwaited_beside_gen7();
        h.core.dl.gens.get_mut(&7).unwrap().kind = super::super::delegate::DelegKind::Designated;
        let out = request(&mut h, 4, 9, ino, X, true);
        let Some(position) = grant_to(&out, 4) else {
            panic!("a silent designation held the barrier: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(2), "{position:?}");
    }

    /// An online designation — renewed within a delegation TTL — counts
    /// as any generation: its head from a renewal sent before the
    /// outwaited record's end does not settle the barrier; one sent at it
    /// does.
    #[test]
    fn an_online_designation_holds_an_outwait_barrier() {
        let (mut h, ino) = outwaited_beside_gen7();
        h.core.dl.gens.get_mut(&7).unwrap().kind = super::super::delegate::DelegKind::Designated;
        let end = h.now.0;
        renew_gen7(&mut h, 91, 3, end - 1);
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(
            grant_to(&out, 4).is_none(),
            "an online designation's older head settled the barrier: {out:?}"
        );
        h.advance(100);
        renew_gen7(&mut h, 92, 4, end);
        let out = request(&mut h, 4, 10, ino, X, true);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on a cut as of the record's end: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(4), "{position:?}");
    }

    /// A restart inside the lease forgets its grant table: every grant of
    /// the previous incarnation ends unreleased, as an outwait does, and
    /// its holders may write into delegates' streams until the persisted
    /// horizon. So the first grant after the quarantine waits for a cut as
    /// of the horizon too.
    #[test]
    fn a_restart_inside_the_lease_leaves_a_barrier_at_its_horizon() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        h.meta.note_lock_grant_horizon(h.now.0 + 3_000).unwrap();
        let until = h.meta.load_lock_quarantine(h.now.0).expect("quarantined");
        let now = h.now;
        h.core.locks_start(now, &h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        // Past the quarantine, the tenure's floor is taken (the delegate's
        // renewal, sent before the horizon, arrives now).
        h.now = Ms(until + 10);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        renew_gen7(&mut h, 90, 2, until - 1);
        let out = request(&mut h, 4, 8, ino, X, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past a restart's grants: {out:?}"
        );
        assert_eq!(
            h.core.stats.lock_tenure_waits, 1,
            "held by the tenure's floor, not the barrier: {out:?}"
        );
        renew_gen7(&mut h, 91, 3, until);
        let out = request(&mut h, 4, 9, ino, X, false);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on a cut as of the horizon: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(3), "{position:?}");
    }

    /// A takeover of a released lease (a successor with no mirror of the
    /// predecessor's grants): those grants end unreleased, as an outwait
    /// does, and their holders may write into delegates' streams until
    /// the quarantine's end. The tenure's floor takes the inherited
    /// delegate's first renewal, which may be older than those writes, so
    /// the first grant past the quarantine waits for a cut as of its end.
    #[test]
    fn a_released_takeover_leaves_a_barrier_at_its_quarantines_end() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let now = h.now;
        h.core.lock_on_released_takeover(now, &h.meta);
        let until = h.meta.locks().quarantine_until();
        assert!(until > now.0);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        h.now = Ms(until + 10);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        renew_gen7(&mut h, 90, 2, until - 1);
        let out = request(&mut h, 4, 8, ino, X, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past the predecessor's grants: {out:?}"
        );
        assert_eq!(h.core.stats.lock_tenure_waits, 1, "{out:?}");
        renew_gen7(&mut h, 91, 3, until);
        let out = request(&mut h, 4, 9, ino, X, false);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on a cut as of the quarantine's end: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(3), "{position:?}");
    }

    /// The same for a node that re-claims its own released lease (no
    /// takeover): its dropped tenure's live grants are waited out, and the
    /// first grant after them waits for a cut as of their end.
    #[test]
    fn a_dropped_tenures_grants_leave_a_barrier_at_their_end() {
        let mut h = Harness::new(1);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.hold(1, None);
        delegated_file(&h.meta);
        let op = h.create("root.lock");
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        request(&mut h, 5, 99, ROOT_INO, X, false);
        let now = h.now.0;
        renew_gen7(&mut h, 90, 2, now);
        let out = request(&mut h, 2, 7, ino, X, true);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let end = h.meta.locks().own_grant(ino, 2, h.now.0).unwrap().until_ms;
        h.core.lease.released();
        let now = h.now;
        h.core.lock_on_lease_gone(now, &h.meta, &mut Vec::new());
        assert_eq!(h.meta.locks().quarantine_until(), end);
        h.advance(100);
        h.hold(2, None);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        h.now = Ms(end + 10);
        request(&mut h, 5, 98, ROOT_INO, X, false);
        renew_gen7(&mut h, 91, 3, end - 1);
        let out = request(&mut h, 4, 8, ino, X, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past the dropped tenure's grant: {out:?}"
        );
        renew_gen7(&mut h, 92, 4, end);
        let out = request(&mut h, 4, 9, ino, X, false);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on a cut as of the grants' end: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(4), "{position:?}");
    }

    /// The delegate side: a delegate that outwaits a holder has no other
    /// generations' heads; it waits for a cut its root sends with a
    /// granting renewal answer (asking for one with a renewal at once),
    /// as of the grant record's end or later.
    #[test]
    fn a_delegates_outwait_barrier_waits_for_the_roots_cut() {
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        let (_, f) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let d = h.core.dl.mine.get_mut(&7).expect("installed");
        d.until = h.now.plus(60_000);
        d.renew = None;
        let out = request(&mut h, 2, 7, f, X, true);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let out = request(&mut h, 4, 8, f, X, true);
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        let end = h.core.timer_at(expiry).unwrap();
        h.now = end;
        let out = h.step(Event::Timer { id: expiry });
        assert!(
            grant_to(&out, 4).is_none(),
            "granted past an outwait: {out:?}"
        );
        let renewal = |out: &[Action]| {
            sends(out).into_iter().find_map(|(to, m)| match m {
                PeerMsg::DelegRenew {
                    req, gen: 7, round, ..
                } if to == 1 => Some((*req, *round)),
                _ => None,
            })
        };
        let Some((req, round)) = renewal(&out) else {
            panic!("no cut asked for: {out:?}")
        };
        let answer = |req: OpId, cut_at: i64, cut: Position| Event::Peer {
            from: 1,
            msg: PeerMsg::DelegRenewed {
                req,
                gen: 7,
                round,
                ttl_ms: 20_000,
                locks: Vec::new(),
                lock_grace_ms: 0,
                lock_floor: Position::ZERO,
                lock_cut_at: cut_at,
                lock_cut: Box::new(cut),
                lock_barrier: 0,
            },
        };
        let mut cut = Position::ZERO;
        assert!(cut.streams.raise(5, 9));
        h.advance(10);
        let out = h.step(answer(req, end.0 - 1, cut));
        assert!(
            grant_to(&out, 4).is_none(),
            "granted on an older cut: {out:?}"
        );
        h.advance(10);
        let out = request(&mut h, 4, 9, f, X, true);
        assert!(grant_to(&out, 4).is_none(), "{out:?}");
        let Some((req, _)) = renewal(&out) else {
            panic!("no fresher cut asked for: {out:?}")
        };
        h.advance(10);
        let out = h.step(answer(req, end.0, cut));
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted on the root's cut: {out:?}")
        };
        assert!(position.streams.get(5) >= Some(9), "{position:?}");
    }

    /// stale-read-outwaited (sim `locks-unlinked-delegated-partition` seed
    /// 4067): a delegate outwaits a holder, then the file is unlinked; the
    /// root owns it from that row on and granted it with only its own
    /// position. The delegate's barrier on it (here still an expired,
    /// unreleased record nobody dropped) goes with the batch that carries
    /// the unlink, and the root keeps it.
    #[test]
    fn an_outwait_barrier_moves_to_the_root_with_the_unlink() {
        // Delegate side.
        let mut h = Harness::new(3);
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        let (dir, f) = delegated_file(&h.meta);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let d = h.core.dl.mine.get_mut(&7).expect("installed");
        d.until = h.now.plus(60_000);
        d.renew = None;
        let out = request(&mut h, 2, 7, f, X, true);
        assert!(grant_to(&out, 2).is_some(), "{out:?}");
        let end = h.meta.locks().own_grant(f, 2, h.now.0).unwrap().until_ms;
        h.now = Ms(end + 10);
        let unlink = LogRecord::Unlink {
            parent: dir,
            name: "turn.lock".into(),
            time_ns: 3,
        };
        crate::replica::Replica::apply_segment(&h.meta, 2, 1, 0, &[], &[], &[unlink]).unwrap();
        let now = h.now;
        let carried = h.core.deleg_leaving_barriers(now, 7, &[], &h.meta);
        assert_eq!(carried, vec![(f, end)]);
        // Root side.
        let mut r = Harness::new(1);
        r.core.cfg.delegation = true;
        r.core.cfg.p2p = true;
        r.hold(1, None);
        let (dir, f) = delegated_file(&r.meta);
        let mut out = Vec::new();
        r.core.delegation_sync(r.now, &r.meta, &mut out);
        request(&mut r, 5, 99, ROOT_INO, X, false);
        let now = r.now.0;
        renew_gen7(&mut r, 90, 0, now);
        let at = r.now.0 + 50;
        r.step(Event::Peer {
            from: 3,
            msg: PeerMsg::DelegateStream {
                req: OpId(50),
                gen: 7,
                round: 0,
                txs: vec![constellation_meta::DelegateTx {
                    idx: 1,
                    rid: None,
                    records: vec![LogRecord::Unlink {
                        parent: dir,
                        name: "turn.lock".into(),
                        time_ns: 1,
                    }],
                    deps: Default::default(),
                }],
                leaving: Vec::new(),
                leaving_barriers: vec![(f, at)],
            },
        });
        let out = request(&mut r, 4, 9, f, X, false);
        assert!(
            grant_to(&out, 4).is_none(),
            "granted over the delegate's outwait: {out:?}"
        );
        r.advance(100);
        renew_gen7(&mut r, 91, 1, at);
        let out = request(&mut r, 4, 10, f, X, false);
        let Some(position) = grant_to(&out, 4) else {
            panic!("not granted once the cut came: {out:?}")
        };
        assert!(position.streams.get(7) >= Some(1), "{position:?}");
    }

    /// Two peers: the second conflicting request recalls the first grant
    /// and parks; the release grants it. Shared grants coexist.
    #[test]
    fn a_conflicting_request_recalls_and_parks_until_the_release() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, S, true);
        let rs = lock_replies(&out);
        let [(2, OpId(7), o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        let (g2, ttl) = granted(o);
        assert_eq!(ttl, 5_000);
        let out = request(&mut h, 3, 8, ino, S, true);
        assert_eq!(lock_replies(&out).len(), 1, "shared with shared: {out:?}");
        assert!(recalls(&out).is_empty());
        let granted_at = h.now;
        let out = request(&mut h, 4, 9, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let rs = recalls(&out);
        assert_eq!(rs.len(), 2, "both shared grants recalled: {out:?}");
        let expiry = timers(&out, TimerKind::LockGrantExpiry);
        assert_eq!(expiry.len(), 2);
        assert_eq!(
            h.core.timer_at(expiry[0]),
            Some(granted_at.plus(5_000 + 1_000)),
            "live until granted + ttl + margin"
        );
        let g3 = rs.iter().find(|r| r.0 == 3).unwrap().3;
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert!(lock_replies(&out).is_empty(), "one still held: {out:?}");
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g3,
                position: constellation_meta::Position::ZERO,
            },
        });
        let [(4, OpId(9), LockOutcome::Granted { mode: X, .. })] = lock_replies(&out).as_slice()
        else {
            panic!("expected node 4's grant: {out:?}")
        };
        assert_eq!(h.core.stats.lock_recalls_released, 2);
        assert_eq!(h.meta.locks().grants_len(), 1);
    }

    /// A non-blocking request that conflicts is refused at once, but the
    /// recall still goes out (the holder gives the cache up when free).
    #[test]
    fn a_non_blocking_conflict_would_block_and_still_recalls() {
        let (mut h, ino) = holder_with_file();
        request(&mut h, 2, 7, ino, X, false);
        let out = request(&mut h, 3, 8, ino, X, false);
        let [(3, OpId(8), LockOutcome::WouldBlock)] = lock_replies(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert_eq!(recalls(&out).len(), 1);
        assert_eq!(h.core.stats.lock_would_block, 1);
    }

    /// An unanswered recall is outwaited (ttl + margin, the owner's
    /// clock); meanwhile the waiter's RPC is answered `Waiting` before it
    /// would time out, and the grant is pushed when it comes.
    #[test]
    fn an_unanswered_recall_is_outwaited_and_the_grant_is_pushed() {
        let (mut h, ino) = holder_with_file();
        let granted_at = h.now;
        request(&mut h, 2, 7, ino, X, true);
        let out = request(&mut h, 3, 8, ino, X, true);
        let held = timer_of(&out, TimerKind::LockHeldReply);
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        h.advance(250);
        let out = h.step(Event::Timer { id: held });
        let [(3, OpId(8), LockOutcome::Waiting { retry_ms: 2_000 })] =
            lock_replies(&out).as_slice()
        else {
            panic!("expected Waiting, re-sent within (ttl - margin) / 2: {out:?}")
        };
        // The waiter re-sends as told (re-attaching in place); each RPC
        // is held, then answered `Waiting` again.
        for at in [2_250, 4_500] {
            h.now = granted_at.plus(at);
            let out = request(&mut h, 3, 8 + at, ino, X, true);
            let held = timer_of(&out, TimerKind::LockHeldReply);
            h.advance(250);
            h.step(Event::Timer { id: held });
        }
        h.now = granted_at.plus(6_000);
        let out = h.step(Event::Timer { id: expiry });
        assert!(lock_replies(&out).is_empty(), "no RPC to answer: {out:?}");
        let ps = pushes(&out);
        let [(3, i, LockOutcome::Granted { .. })] = ps.as_slice() else {
            panic!("expected a push: {out:?}")
        };
        assert_eq!(*i, ino);
        assert_eq!(h.core.stats.lock_recalls_expired, 1);
        assert_eq!(h.core.stats.lock_waiting_replies, 3);
    }

    /// Park `node`'s request and answer it `Waiting` once its RPC has
    /// been held (as the held-reply timer does).
    fn park(h: &mut Harness, node: NodeId, req: u64, ino: Ino) -> TimerId {
        let out = request(h, node, req, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        timer_of(&out, TimerKind::LockHeldReply)
    }

    /// A waiter killed while parked (the EC2 lock run: node c died with
    /// its request queued) is granted from its request's *arrival*: the
    /// grant is outwaited `ttl + margin` after the last message it sent,
    /// not after however long it sat in the queue — and the waiter
    /// behind it, which keeps re-sending, is served at that point.
    #[test]
    fn a_waiter_killed_while_parked_costs_one_window_from_its_last_message() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        let t0 = h.now;
        // Node 3 parks, then dies (never re-sends); node 4 parks behind
        // it and stays alive, re-sending every (ttl - margin) / 2.
        let held3 = park(&mut h, 3, 8, ino);
        h.advance(100);
        let held4 = park(&mut h, 4, 9, ino);
        h.advance(150);
        h.step(Event::Timer { id: held3 });
        h.advance(100);
        h.step(Event::Timer { id: held4 });
        h.now = t0.plus(2_350);
        let held4 = park(&mut h, 4, 10, ino);
        h.advance(250);
        h.step(Event::Timer { id: held4 });
        // Node 2 releases 3 s after node 3's request: node 3 (first in
        // line, still inside its window) is granted — live from t0.
        h.now = t0.plus(3_000);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let ps = pushes(&out);
        let [(3, _, o)] = ps.as_slice() else {
            panic!("expected node 3's grant pushed: {out:?}")
        };
        let (g3, _) = granted(o);
        assert_eq!(
            h.meta.locks().get(g3).unwrap().until_ms,
            t0.0 + 5_000 + 1_000,
            "live from its request's arrival, not from now"
        );
        let expiry = timer_of(&out, TimerKind::LockGrantExpiry);
        assert_eq!(h.core.timer_at(expiry), Some(t0.plus(6_000)));
        h.now = t0.plus(4_600);
        let held4 = park(&mut h, 4, 11, ino);
        h.advance(250);
        h.step(Event::Timer { id: held4 });
        // Nobody answers the recall: outwaited at t0 + ttl + margin, and
        // node 4 is served then — 3 s earlier than a grant stamped at its
        // push would have allowed.
        h.now = t0.plus(6_000);
        let out = h.step(Event::Timer { id: expiry });
        let ps = pushes(&out);
        let [(4, _, LockOutcome::Granted { .. })] = ps.as_slice() else {
            panic!("expected node 4's grant pushed: {out:?}")
        };
        assert_eq!(h.core.stats.lock_recalls_expired, 1);
    }

    /// A parked remote waiter silent past its window is skipped, not
    /// granted: a live one would find such a grant lapsed on arrival
    /// (and re-sends well within it), a dead one would make everyone
    /// behind it wait `ttl + margin` — once per dead waiter. After
    /// `4 × ttl` of silence it is dropped.
    #[test]
    fn a_silent_parked_waiter_is_skipped_and_then_dropped() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        let t0 = h.now;
        // Nodes 3 and 5 park and die; node 4 parks and keeps re-sending.
        let held = [
            park(&mut h, 3, 8, ino),
            park(&mut h, 5, 9, ino),
            park(&mut h, 4, 10, ino),
        ];
        h.advance(250);
        for id in held {
            h.step(Event::Timer { id });
        }
        h.now = t0.plus(2_250);
        let held4 = park(&mut h, 4, 11, ino);
        h.advance(250);
        h.step(Event::Timer { id: held4 });
        h.now = t0.plus(4_250);
        park(&mut h, 4, 12, ino);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let rs = lock_replies(&out);
        let [(4, OpId(12), o)] = rs.as_slice() else {
            panic!("expected node 4's grant at once: {out:?}")
        };
        let (g4, _) = granted(o);
        assert!(
            pushes(&out).is_empty(),
            "nothing for the silent waiters: {out:?}"
        );
        assert!(h.meta.locks().own_grant(ino, 3, h.now.0).is_none());
        assert!(h.meta.locks().own_grant(ino, 5, h.now.0).is_none());
        assert_eq!(h.core.lock_view().waiters, 2, "skipped, still parked");
        // Past 4 × ttl of silence they are dropped at the next serve.
        h.now = t0.plus(20_000);
        h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g4,
                position: constellation_meta::Position::ZERO,
            },
        });
        assert_eq!(h.core.stats.lock_waiters_dropped, 2);
        assert_eq!(h.core.lock_view().waiters, 0);
    }

    /// A renewal extends a known grant (and reports its recalled flag);
    /// an unknown one is lost — unless a grace period admits a reclaim.
    #[test]
    fn renewals_extend_or_reclaim_or_lose() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        request(&mut h, 3, 8, ino, X, true);
        h.advance(2_000);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(20),
                entries: vec![LockRenewEntry {
                    ino,
                    grant: g2,
                    mode: X,
                }],
            },
        });
        let [(
            2,
            PeerMsg::LockRenewed {
                req: OpId(20),
                results,
            },
        )] = sends(&out).as_slice()
        else {
            panic!("{out:?}")
        };
        assert_eq!(
            results.as_slice(),
            &[(
                ino,
                g2,
                LockRenewResult::Ok {
                    ttl_ms: 5_000,
                    recalled: true,
                    id: g2,
                    mode: X,
                }
            )]
        );
        assert_eq!(
            h.meta.locks().get(g2).unwrap().until_ms,
            h.now.0 + 5_000 + 1_000
        );
        // Unknown, no grace: lost.
        let ghost = GrantId { node: 1, seq: 99 };
        let out = h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockRenew {
                req: OpId(21),
                entries: vec![LockRenewEntry {
                    ino,
                    grant: ghost,
                    mode: S,
                }],
            },
        });
        let [(4, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert_eq!(results[0].2, LockRenewResult::Lost);
        // A grace period (the restart quarantine / M9's floor) admits a
        // reclaim of an unknown grant that nothing conflicts with.
        let other = h.create("g");
        let MutateOp::Create { ino: ino2, .. } = other else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &other, None).unwrap();
        h.meta.read_delegations().set_quarantine(h.now.0 + 3_000);
        let out = h.step(Event::Peer {
            from: 4,
            msg: PeerMsg::LockRenew {
                req: OpId(22),
                entries: vec![LockRenewEntry {
                    ino: ino2,
                    grant: ghost,
                    mode: X,
                }],
            },
        });
        let [(4, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert!(
            matches!(results[0].2, LockRenewResult::Ok { .. }),
            "{results:?}"
        );
        assert_eq!(h.core.stats.lock_reclaimed, 1);
        assert_eq!(h.meta.locks().get(ghost).unwrap().node, 4);
        // And no *new* grant during the grace.
        let out = request(&mut h, 2, 9, ino2, S, false);
        let [(2, OpId(9), LockOutcome::WouldBlock)] = lock_replies(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert_eq!(h.core.stats.lock_grace_refusals, 1);
    }

    /// A node that holds an exclusive grant and asks for a shared one
    /// (a second thread's request behind the first's) keeps the
    /// exclusive grant — the same id, re-affirmed; an upgrade from shared
    /// keeps the id too, and is what the other holders are recalled for.
    #[test]
    fn a_same_node_request_never_downgrades_its_grant() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (gx, _) = granted(&lock_replies(&out)[0].2);
        let out = request(&mut h, 2, 8, ino, S, true);
        let rs = lock_replies(&out);
        let [(2, OpId(8), LockOutcome::Granted { id, mode: X, .. })] = rs.as_slice() else {
            panic!("expected the exclusive grant re-affirmed: {out:?}")
        };
        // Re-asked for: the same mode, a fresh id (never one the node may
        // have dropped), still one entry.
        assert_ne!(*id, gx);
        assert!(h.meta.locks().get(gx).is_none());
        assert_eq!(h.meta.locks().grants_len(), 1);
        let gx = *id;
        // Node 3 shared, then node 3 exclusive: an upgrade in place that
        // recalls node 2.
        let out = request(&mut h, 3, 9, ino, S, true);
        assert!(lock_replies(&out).is_empty(), "conflicts with X: {out:?}");
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: gx,
                position: constellation_meta::Position::ZERO,
            },
        });
        let rs = lock_replies(&out);
        let [(
            3,
            OpId(9),
            LockOutcome::Granted {
                id: gs, mode: S, ..
            },
        )] = rs.as_slice()
        else {
            panic!("{out:?}")
        };
        let gs = *gs;
        request(&mut h, 2, 10, ino, S, true);
        let out = request(&mut h, 3, 11, ino, X, true);
        assert!(
            lock_replies(&out).is_empty(),
            "parked behind node 2: {out:?}"
        );
        assert_eq!(recalls(&out).len(), 1);
        let g2 = h.meta.locks().own_grant(ino, 2, h.now.0).unwrap().id;
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: constellation_meta::Position::ZERO,
            },
        });
        let rs = lock_replies(&out);
        let [(3, OpId(11), LockOutcome::Granted { id, mode: X, .. })] = rs.as_slice() else {
            panic!("{out:?}")
        };
        assert_ne!(*id, gs, "an upgrade is a new grant (the old id dies)");
        assert!(h.meta.locks().get(gs).is_none());
        // The node side merges by id: the held mode is the stronger one.
        let mut r = requester();
        let req = lock_control(&mut r, 50, 42, true);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        let mut weaker = grant_msg(1);
        if let LockOutcome::Granted { mode, .. } = &mut weaker {
            *mode = S;
        }
        let req = lock_control(&mut r, 51, 42, true);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: weaker,
            },
        });
        assert_eq!(r.meta.locks().held(42).unwrap().mode, X);
    }

    /// A successor whose takeover gate is pending (the view fenced for
    /// M9's floor) still extends and reclaims; a new grant waits.
    #[test]
    fn a_fenced_successor_renews_and_reclaims_but_grants_nothing_new() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, _) = granted(&lock_replies(&out)[0].2);
        // The gate is pending: the view is fenced, and the floor is on.
        h.hold(
            2,
            Some(PendingGate {
                epoch: 2,
                takeover: true,
                marker_shipped: true,
                drained: false,
                fast_prev: Some((h.now.plus(10_000).0, true)),
                backup_tail_epoch: None,
                shippable: true,
            }),
        );
        assert!(h.core.lease.fenced());
        h.meta.read_delegations().set_quarantine(h.now.0 + 3_000);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(20),
                entries: vec![LockRenewEntry {
                    ino,
                    grant: g2,
                    mode: X,
                }],
            },
        });
        let [(2, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert!(
            matches!(results[0].2, LockRenewResult::Ok { .. }),
            "{results:?}"
        );
        let ghost = GrantId { node: 9, seq: 5 };
        let other = h.create("g");
        let MutateOp::Create { ino: ino2, .. } = other else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &other, None).unwrap();
        let out = h.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockRenew {
                req: OpId(21),
                entries: vec![LockRenewEntry {
                    ino: ino2,
                    grant: ghost,
                    mode: S,
                }],
            },
        });
        let [(3, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
            panic!("{out:?}")
        };
        assert!(
            matches!(results[0].2, LockRenewResult::Ok { .. }),
            "reclaim: {results:?}"
        );
        let out = request(&mut h, 4, 9, ino2, X, false);
        assert!(
            !matches!(
                lock_replies(&out).as_slice(),
                [(4, _, LockOutcome::Granted { .. })]
            ),
            "nothing new while fenced: {out:?}"
        );
    }

    /// A grant never outlives the lease's usable end.
    #[test]
    fn a_grant_is_capped_by_the_lease() {
        let (mut h, ino) = holder_with_file();
        h.advance(7_000); // 3 s of lease left, margin 1 s
        let out = request(&mut h, 2, 7, ino, X, true);
        let (_, ttl) = granted(&lock_replies(&out)[0].2);
        assert_eq!(ttl, 2_000);
    }

    /// The lease is gone: every grant of the tenure is void, the waiters
    /// are told to look elsewhere.
    #[test]
    fn a_lost_lease_drops_the_grants_and_answers_the_waiters() {
        let (mut h, ino) = holder_with_file();
        request(&mut h, 2, 7, ino, X, true);
        request(&mut h, 3, 8, ino, X, true);
        assert_eq!(h.meta.locks().grants_len(), 1);
        let mut out = Vec::new();
        let now = h.now;
        h.core.deposed(now, 5, 2, 1, &h.meta, &mut out);
        assert_eq!(h.meta.locks().grants_len(), 0);
        let [(3, OpId(8), LockOutcome::NotOwner { .. })] = lock_replies(&out).as_slice() else {
            panic!("{out:?}")
        };
    }

    // ---- the node side ----

    fn requester() -> Harness {
        let mut r = Harness::new(2);
        r.core.lease.cached_holder = Some(1);
        r.step(Event::Peers {
            links: (1..=4)
                .map(|node| crate::event::PeerLink {
                    node,
                    connected: true,
                    last_seen: None,
                    rtt_ms: Some(1),
                    since: None,
                })
                .collect(),
        });
        r
    }

    fn lock_control(r: &mut Harness, op: u64, ino: Ino, blocking: bool) -> OpId {
        let out = r.step(Event::Control {
            op: OpId(op),
            req: Control::Lock {
                ino,
                mode: X,
                blocking,
            },
        });
        match sends(&out).as_slice() {
            [(1, PeerMsg::LockRequest { req, .. })] => *req,
            other => panic!("expected a LockRequest to node 1: {other:?}"),
        }
    }

    fn grant_msg(seq: u64) -> LockOutcome {
        LockOutcome::Granted {
            id: GrantId { node: 1, seq },
            mode: X,
            ttl_ms: 5_000,
            position: Position {
                seq: 3,
                pending: None,
                streams: Default::default(),
            },
        }
    }

    /// A delegate's short grant (2 s: honoured for 1 s): the renewal tick
    /// comes at its renewal point, half-way through the window, not at
    /// the ttl/4 cadence (1.25 s, after the window closed: the grant
    /// lapsed under the application's `flock` and the owner handed the
    /// lock on — the rounds harness scenario).
    #[test]
    fn a_short_grant_is_renewed_inside_its_window() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: LockOutcome::Granted {
                    id: GrantId { node: 1, seq: 1 },
                    mode: X,
                    ttl_ms: 2_000,
                    position: Position::ZERO,
                },
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let held = r.meta.locks().held(42).expect("installed");
        assert_eq!(held.until_ms, sent.0 + 1_000);
        assert_eq!(held.renew_at_ms, sent.0 + 500);
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        assert_eq!(r.core.timer_at(tick), Some(sent.plus(500)));
        r.now = sent.plus(500);
        let out = r.step(Event::Timer { id: tick });
        assert!(
            sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockRenew { .. })),
            "not renewed inside the window: {out:?}"
        );
    }

    /// A renewal that finds no reachable owner reads the lease for it; the
    /// grant's renewal point stays in the past meanwhile, and the tick
    /// armed from it fired every millisecond while S3 was cut (the read
    /// outstanding all along). It now waits at least a lock request's
    /// retry cadence, and the read's answer re-ticks at once.
    #[test]
    fn a_renewal_with_no_reachable_owner_does_not_spin_while_it_relearns() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let mut tick = timer_of(&out, TimerKind::LockRenewTick);
        // The owner is forgotten (a failover, S3 cut: nothing names one).
        r.core.lease.cached_holder = None;
        r.now = sent.plus(2_100);
        let mut found = None;
        for _ in 0..8 {
            let out = r.step(Event::Timer { id: tick });
            if !s3_ops(&out).is_empty() {
                found = Some(out);
                break;
            }
            tick = timer_of(&out, TimerKind::LockRenewTick);
            r.now = r.core.timer_at(tick).unwrap().max(r.now);
        }
        let out = found.expect("no lease read for the owner");
        let [(read, S3Op::LeaseGet)] = s3_ops(&out)[..] else {
            panic!("expected the owner's lease read: {out:?}")
        };
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        let at = r.core.timer_at(tick).unwrap();
        assert!(
            at.0 >= r.now.0 + 10,
            "re-ticks {} ms later with the read outstanding",
            at.0 - r.now.0
        );
        // A tick meanwhile neither reads again nor spins.
        r.now = at;
        let out = r.step(Event::Timer { id: tick });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        assert!(r.core.timer_at(tick).unwrap().0 >= r.now.0 + 10);
        // The read answers (S3 still away): the tick comes again soon.
        let out = r.step(Event::S3 {
            op: read,
            result: S3Result::LeaseGet(Err(crate::event::S3Failure("cut".into()))),
        });
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        assert!(r.core.timer_at(tick).unwrap().0 <= r.now.0 + 1_000);
    }

    /// delegate-fenced-io (`locks-blips-tight-delegated` seed 157): a
    /// renewal whose owner this node no longer knows reads the lease, and
    /// under an S3 cut that read fails for the whole cut. With a
    /// continuation epoch active its carrier is asked instead (it serves
    /// the root's grants): before, the grant lapsed under its holder's
    /// I/O although the root renewing it was reachable over P2P.
    #[test]
    fn a_failed_owner_read_renews_with_the_epochs_carrier() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let mut tick = timer_of(&out, TimerKind::LockRenewTick);
        r.core.lease.cached_holder = None;
        r.core.epoch.active = true;
        r.core.pr.carried = Some(crate::event::Carrier {
            node: 1,
            epoch: 1,
            expires_unix_ms: 0,
        });
        r.now = sent.plus(2_100);
        let mut read = None;
        for _ in 0..8 {
            let out = r.step(Event::Timer { id: tick });
            if let [(op, S3Op::LeaseGet)] = s3_ops(&out)[..] {
                read = Some(op);
                break;
            }
            tick = timer_of(&out, TimerKind::LockRenewTick);
            r.now = r.core.timer_at(tick).unwrap().max(r.now);
        }
        let read = read.expect("no lease read for the owner");
        let out = r.step(Event::S3 {
            op: read,
            result: S3Result::LeaseGet(Err(crate::event::S3Failure("cut".into()))),
        });
        assert_eq!(r.core.lease.cached_holder, Some(1), "{out:?}");
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        r.now = r.core.timer_at(tick).unwrap();
        let out = r.step(Event::Timer { id: tick });
        assert!(
            sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockRenew { .. })),
            "not renewed with the carrier: {out:?}"
        );
        // Without an epoch, a failed read names nobody (the last lease
        // seen here is none).
        let mut r = requester();
        r.core.lease.cached_holder = None;
        assert_eq!(r.core.lock_owner_without_s3(r.now), None);
    }

    /// delegate-fenced-io (`locks-blips-tight-delegated` seed 1): a
    /// continuation epoch's recall-all takes a delegate's grants back to
    /// the root, which ends the generation in its own journal at once.
    /// Under the S3 cut the `Recall` record reaches nobody else, so every
    /// other table still names the delegate. It answered the subtree's
    /// renewals and requests `NotOwner { 0 }`, the holders found no owner
    /// for the whole cut, and their grants lapsed under their I/O. The
    /// recalled delegate now names the root that recalled it, also for a
    /// generation it never installed (an epoch was active when its table
    /// named it: seed 293), until the `Recall` record reaches its table.
    #[test]
    fn a_recalled_delegate_sends_its_subtrees_locks_to_the_root() {
        for installed in [true, false] {
            let mut h = Harness::new(3);
            h.core.cfg.delegation = true;
            h.core.cfg.p2p = true;
            h.core.lease.cached_holder = Some(1);
            let (dir, f) = delegated_file(&h.meta);
            if installed {
                let mut out = Vec::new();
                h.core.delegation_sync(h.now, &h.meta, &mut out);
                let req = renew_req(&out, 7).expect("no renewal on install");
                h.step(super::renewed(&h, req, 7, 5_000));
            }
            let renew = |h: &mut Harness| {
                let out = h.step(Event::Peer {
                    from: 2,
                    msg: PeerMsg::LockRenew {
                        req: OpId(40),
                        entries: vec![LockRenewEntry {
                            ino: f,
                            grant: GrantId { node: 3, seq: 1 },
                            mode: X,
                        }],
                    },
                });
                let [(2, PeerMsg::LockRenewed { results, .. })] = sends(&out).as_slice() else {
                    panic!("expected an answer: {out:?}")
                };
                results[0].2
            };
            if !installed {
                assert_eq!(renew(&mut h), LockRenewResult::NotOwner { owner: 0 });
            }
            h.step(Event::Peer {
                from: 1,
                msg: PeerMsg::DelegRecall {
                    req: OpId(5),
                    dir,
                    gen: 7,
                },
            });
            assert_eq!(
                renew(&mut h),
                LockRenewResult::NotOwner { owner: 1 },
                "installed {installed}"
            );
            let out = request(&mut h, 2, 9, f, X, true);
            assert!(
                matches!(
                    lock_replies(&out).as_slice(),
                    [(2, OpId(9), LockOutcome::NotOwner { owner: 1 })]
                ),
                "installed {installed}: {out:?}"
            );
            // The `Recall` record reaches this table: the generation is
            // gone, and with it what its recall handed back.
            crate::replica::Replica::apply_segment(
                &h.meta,
                2,
                1,
                0,
                &[],
                &[],
                &[LogRecord::Recall { dir, gen: 7 }],
            )
            .unwrap();
            let mut out = Vec::new();
            h.core.delegation_sync(h.now, &h.meta, &mut out);
            assert!(h.core.dl.handed_back.is_empty(), "installed {installed}");
        }
    }

    /// delegate-fenced-io: the holder's half of the above. The delegate
    /// its table names for the subtree answers a renewal `NotOwner {
    /// root }`: the next renewal goes to that root, whose table no longer
    /// delegates the subtree and renews the grant. Once the root answers
    /// `NotOwner` itself (its table delegates the subtree again), the
    /// subtree goes back to the delegate.
    #[test]
    fn a_holder_renews_where_its_recalled_delegate_points() {
        let mut r = requester();
        r.core.cfg.delegation = true;
        r.core.cfg.p2p = true;
        let (_, f) = delegated_file(&r.meta);
        let out = r.step(Event::Control {
            op: OpId(50),
            req: Control::Lock {
                ino: f,
                mode: X,
                blocking: true,
            },
        });
        let [(3, PeerMsg::LockRequest { req, .. })] = sends(&out).as_slice() else {
            panic!("expected a request to the delegate: {out:?}")
        };
        let id = GrantId { node: 3, seq: 1 };
        let out = r.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockReply {
                req: *req,
                outcome: LockOutcome::Granted {
                    id,
                    mode: X,
                    ttl_ms: 5_000,
                    position: Position::ZERO,
                },
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let mut tick = timer_of(&out, TimerKind::LockRenewTick);
        // Renews at `to`; answered `result` by `to`; returns the next tick.
        let renew_at = |r: &mut Harness, mut tick: TimerId, to: NodeId, result| {
            let mut renewal = None;
            for _ in 0..8 {
                r.now = r.core.timer_at(tick).unwrap().max(r.now);
                let out = r.step(Event::Timer { id: tick });
                renewal = sends(&out).into_iter().find_map(|(t, m)| match m {
                    PeerMsg::LockRenew { req, .. } => Some((t, *req)),
                    _ => None,
                });
                tick = timer_of(&out, TimerKind::LockRenewTick);
                if renewal.is_some() {
                    break;
                }
            }
            let Some((t, req)) = renewal else {
                panic!("no renewal")
            };
            assert_eq!(t, to, "renewed at {t}, not {to}");
            r.advance(5);
            let out = r.step(Event::Peer {
                from: to,
                msg: PeerMsg::LockRenewed {
                    req,
                    results: vec![(f, id, result)],
                },
            });
            timers(&out, TimerKind::LockRenewTick)
                .last()
                .copied()
                .unwrap_or(tick)
        };
        let ok = || LockRenewResult::Ok {
            ttl_ms: 5_000,
            recalled: false,
            id: GrantId { node: 3, seq: 1 },
            mode: X,
        };
        tick = renew_at(&mut r, tick, 3, LockRenewResult::NotOwner { owner: 1 });
        tick = renew_at(&mut r, tick, 1, ok());
        let until = r.meta.locks().held(f).expect("held").until_ms;
        assert!(until > r.now.0 + 3_000, "not renewed by the root");
        tick = renew_at(&mut r, tick, 1, LockRenewResult::NotOwner { owner: 3 });
        renew_at(&mut r, tick, 3, ok());
    }

    /// delegate-fenced-io (`locks-delegated-writes`): a waiter served from
    /// a delegate's queue near the end of its re-send interval, under a
    /// ttl the delegation capped, got a grant whose window (counted from
    /// the request's send) had 5–20 ms left. It entered its I/O and was
    /// fenced before a renewal could come back. A grant arriving with less
    /// than a quarter margin left is asked for again, like one that
    /// lapsed on arrival; one with more is installed.
    #[test]
    fn a_grant_arriving_all_but_lapsed_is_asked_for_again() {
        for (wait, installed) in [(3_700, true), (3_800, false)] {
            let mut r = requester();
            let req = lock_control(&mut r, 50, 42, true);
            // Honoured for ttl − margin = 4 s from the send.
            r.advance(wait);
            let out = r.step(Event::Peer {
                from: 1,
                msg: PeerMsg::LockReply {
                    req,
                    outcome: grant_msg(1),
                },
            });
            let answered = out.iter().any(|a| {
                matches!(
                    a,
                    Action::ControlDone {
                        op: OpId(50),
                        result: Ok(ControlOk::Lock(LockAnswer::Granted { .. })),
                    }
                )
            });
            assert_eq!(answered, installed, "after {wait} ms: {out:?}");
            assert_eq!(r.meta.locks().held(42).is_some(), installed);
            if !installed {
                let retry = timer_of(&out, TimerKind::LockRetry);
                r.now = r.core.timer_at(retry).unwrap();
                let out = r.step(Event::Timer { id: retry });
                assert!(
                    matches!(sends(&out).as_slice(), [(1, PeerMsg::LockRequest { .. })]),
                    "not asked again: {out:?}"
                );
            }
        }
    }

    /// overload-cascade-2: an owner whose requests queue behind seconds
    /// of other work answers a renewal after the requester stopped
    /// waiting (`forward_timeout_ms`). The late grant still counts —
    /// honoured from its own send, as an answer in time would be — where
    /// it used to be thrown away (every answer late: the grant lapsed
    /// under the holder's writes although the owner renewed it each
    /// time). A late `Lost`, and an answer from another node, do not.
    #[test]
    fn a_renewal_answered_after_its_timeout_still_counts() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        r.now = sent.plus(2_600);
        let renew_sent = r.now;
        let out = r.step(Event::Timer { id: tick });
        let [(1, PeerMsg::LockRenew { req, .. })] = sends(&out).as_slice() else {
            panic!("expected a renewal: {out:?}")
        };
        let req = *req;
        let timeout = timer_of(&out, TimerKind::LockRenewTimeout);
        r.advance(r.core.cfg.forward_timeout_ms);
        r.step(Event::Timer { id: timeout });
        let ok = |ttl_ms| {
            vec![(
                42,
                GrantId { node: 1, seq: 1 },
                LockRenewResult::Ok {
                    ttl_ms,
                    recalled: false,
                    id: GrantId { node: 1, seq: 1 },
                    mode: X,
                },
            )]
        };
        let before = r.meta.locks().held(42).unwrap().until_ms;
        assert_eq!(before, sent.0 + 5_000 - 1_000);
        // Another node answering that request id: not the owner asked.
        r.advance(1_000);
        r.step(Event::Peer {
            from: 3,
            msg: PeerMsg::LockRenewed {
                req,
                results: ok(5_000),
            },
        });
        assert_eq!(r.meta.locks().held(42).unwrap().until_ms, before);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRenewed {
                req,
                results: ok(5_000),
            },
        });
        assert_eq!(
            r.meta.locks().held(42).unwrap().until_ms,
            renew_sent.0 + 5_000 - 1_000,
            "the late grant did not count"
        );
        // A late `Lost` never drops the grant (the renewal sent since
        // may have been granted by a new owner).
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        r.now = sent.plus(2_600);
        let out = r.step(Event::Timer { id: tick });
        let [(1, PeerMsg::LockRenew { req, .. })] = sends(&out).as_slice() else {
            panic!("expected a renewal: {out:?}")
        };
        let req = *req;
        let timeout = timer_of(&out, TimerKind::LockRenewTimeout);
        r.advance(r.core.cfg.forward_timeout_ms);
        r.step(Event::Timer { id: timeout });
        r.advance(500);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRenewed {
                req,
                results: vec![(42, GrantId { node: 1, seq: 1 }, LockRenewResult::Lost)],
            },
        });
        assert!(r.meta.locks().held(42).is_some(), "a late Lost dropped it");
    }

    /// The grant is installed honoured until sent + ttl − margin; the
    /// renewal tick renews it at the owner from ttl/2; a recall with no
    /// local lock flushes, then releases.
    #[test]
    fn a_requester_installs_renews_and_releases_on_recall() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(30);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let held = r.meta.locks().held(42).expect("installed");
        assert_eq!(held.until_ms, sent.0 + 5_000 - 1_000);
        // Half-way through the window it is honoured for (ttl − margin).
        assert_eq!(held.renew_at_ms, sent.0 + 2_000);
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        // Too early: nothing renewed, the tick re-arms.
        r.advance(1_000);
        let out = r.step(Event::Timer { id: tick });
        assert!(sends(&out).is_empty());
        let tick = timer_of(&out, TimerKind::LockRenewTick);
        r.now = sent.plus(2_600);
        let renew_sent = r.now;
        let out = r.step(Event::Timer { id: tick });
        let [(1, PeerMsg::LockRenew { req, entries })] = sends(&out).as_slice() else {
            panic!("expected a renewal: {out:?}")
        };
        assert_eq!(entries.len(), 1);
        let req = *req;
        r.advance(20);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRenewed {
                req,
                results: vec![(
                    42,
                    GrantId { node: 1, seq: 1 },
                    LockRenewResult::Ok {
                        ttl_ms: 5_000,
                        recalled: false,
                        id: GrantId { node: 1, seq: 1 },
                        mode: X,
                    },
                )],
            },
        });
        assert!(sends(&out).is_empty());
        assert_eq!(
            r.meta.locks().held(42).unwrap().until_ms,
            renew_sent.0 + 5_000 - 1_000
        );
        // The FUSE thread takes and drops the local lock it asked for
        // (every grant pins one such attempt before a recall can release
        // it).
        assert_eq!(
            r.meta.locks().local_set(
                42,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                r.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        // A recall: acked, flushed, released.
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 1 },
            },
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::LockRecalled { req: OpId(77) })]
        ));
        assert!(out
            .iter()
            .any(|a| matches!(a, Action::LockFlush { ino: 42, .. })));
        let out = r.step(Event::LockFlushed {
            ino: 42,
            grant: GrantId { node: 1, seq: 1 },
            ok: true,
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::LockReleased { ino: 42, .. })]
        ));
        assert!(r.meta.locks().held(42).is_none());
        assert_eq!(r.core.stats.lock_released, 1);
    }

    /// `git-under-flock-b2b` (two committers in the turn lock at once): a
    /// grant recalled before its first use — the other committer asked
    /// while this node's FUSE thread still waited for the grant's floor
    /// (up to the 2 s session budget, plus 1 s for the kernel's cache) —
    /// is pinned by that first use, so it is neither released nor, as it
    /// was, renewed: the renewal went out only once the local lock was
    /// taken, past the renewal point, with a fraction of the window
    /// left, and the grant lapsed under the application's `flock` while
    /// the owner outwaited it and handed the lock on. It is renewed at
    /// its renewal point like any grant in use, and the tick does not
    /// spin meanwhile (it re-armed itself every millisecond).
    #[test]
    fn a_grant_recalled_before_its_first_use_is_renewed_in_its_window() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let mut tick = timer_of(&out, TimerKind::LockRenewTick);
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 1 },
            },
        });
        assert!(
            !out.iter().any(|a| matches!(a, Action::LockFlush { .. })),
            "released before its first use: {out:?}"
        );
        if let Some(t) = timers(&out, TimerKind::LockRenewTick).first() {
            tick = *t;
        }
        // The FUSE thread is still in its floor wait: run the ticks up
        // to the window's end.
        let mut ticks = 0;
        let mut renewed = None;
        while renewed.is_none() {
            let at = r.core.timer_at(tick).expect("the renewal tick is armed");
            assert!(
                at.0 < sent.0 + 4_000,
                "no renewal tick inside the window: next at {at:?}"
            );
            r.now = at;
            let out = r.step(Event::Timer { id: tick });
            ticks += 1;
            assert!(
                ticks <= 8,
                "the renewal tick spins: {ticks} ticks by {:?}",
                r.now
            );
            renewed = sends(&out).iter().find_map(|(to, m)| match m {
                PeerMsg::LockRenew { entries, .. } if *to == 1 => Some(entries.clone()),
                _ => None,
            });
            if renewed.is_none() {
                tick = timer_of(&out, TimerKind::LockRenewTick);
            }
        }
        assert!(
            r.now.0 <= sent.0 + 2_000 + 1,
            "renewed late, at {} ms into the window",
            r.now.0 - sent.0
        );
        assert_eq!(renewed.unwrap()[0].grant, GrantId { node: 1, seq: 1 });
    }

    /// Drive a flush-then-release job (a handoff's, `Control::Flush`'s)
    /// to its end, answering each step as S3 would; the release CAS
    /// lands. Returns the last step's actions.
    fn drive_flush_job(h: &mut Harness, mut out: Vec<Action>) -> Vec<Action> {
        for _ in 0..8 {
            if let Some(op) = out.iter().find_map(|a| match a {
                Action::UploadDirtyChunks { op, .. } => Some(*op),
                _ => None,
            }) {
                out = h.step(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                });
                continue;
            }
            if let Some(op) = out.iter().find_map(|a| match a {
                Action::Publish { op, .. } => Some(*op),
                _ => None,
            }) {
                out = h.step(Event::PublishDone { op, ok: true });
                continue;
            }
            if let Some((op, req)) = s3_ops(&out).first().map(|(o, r)| (*o, (*r).clone())) {
                let result = match req {
                    S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                    S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                    S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                    other => panic!("unexpected S3 op in the flush: {other:?}"),
                };
                out = h.step(Event::S3 { op, result });
                continue;
            }
            break;
        }
        out
    }

    /// `locks-blips-tight` seed 2723: a lease request is admitted only
    /// with no grant out, but the holder went on granting while the
    /// handoff flushed, and the release dropped two exclusive grants under
    /// their holders' I/O. Grants made meanwhile decline the handoff.
    #[test]
    fn a_handoff_is_declined_when_grants_were_made_while_it_flushed() {
        let (mut h, ino) = holder_with_file();
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LeaseRequest {
                req: OpId(5),
                epoch_applied: None,
            },
        });
        assert_eq!(h.core.job(), Some(JobKind::Handoff));
        // The handoff's upload pass is in flight: a lock is granted.
        let granted_out = request(&mut h, 3, 9, ino, X, true);
        let rs = lock_replies(&granted_out);
        let [(3, OpId(9), o)] = rs.as_slice() else {
            panic!("{granted_out:?}")
        };
        granted(o);
        let out = drive_flush_job(&mut h, out);
        let answered: Vec<bool> = sends(&out)
            .iter()
            .filter_map(|(to, m)| match m {
                PeerMsg::LeaseHandoff { released, .. } if *to == 2 => Some(*released),
                _ => None,
            })
            .collect();
        assert_eq!(answered, vec![false], "the handoff was served: {out:?}");
        assert!(
            !s3_ops(&out)
                .iter()
                .any(|(_, r)| matches!(r, S3Op::LeaseSwap { lease, .. } if lease.released)),
            "released with a live grant: {out:?}"
        );
        assert!(h.core.lease().held.is_some(), "the lease stays");
        assert!(!h.core.lease().releasing);
        assert_eq!(h.meta.locks().grants_len(), 1, "the grant stands");
        assert!(h.core.job().is_none());
    }

    /// A release that drops live grants (`Control::Flush`'s, a suspension
    /// or `leave`) leaves them honoured by their holders until they lapse.
    /// A successor's takeover of the released lease waits them out; this
    /// node re-claiming its own released lease is no takeover, and must
    /// wait them out as well.
    #[test]
    fn a_reclaim_of_an_own_released_lease_waits_out_the_dropped_grants() {
        let (mut h, ino) = holder_with_file();
        let o = request(&mut h, 2, 7, ino, X, true);
        let rs = lock_replies(&o);
        let [(2, OpId(7), g)] = rs.as_slice() else {
            panic!("{o:?}")
        };
        let (_, ttl) = granted(g);
        let live_until = h.now.0 + ttl as i64 + h.core.cfg.expiry_margin_ms as i64;
        let out = h.step(Event::Control {
            op: OpId(1 << 50),
            req: Control::Flush,
        });
        drive_flush_job(&mut h, out);
        assert!(h.core.lease().held.is_none(), "released");
        assert_eq!(h.meta.locks().grants_len(), 0, "the table went with it");
        assert!(h.meta.locks().quarantine_until() >= live_until);
        // The same node holds the lease again: node 2's grant is live.
        h.advance(100);
        h.hold(2, None);
        let out = request(&mut h, 3, 8, ino, X, false);
        let [(3, OpId(8), LockOutcome::WouldBlock)] = lock_replies(&out).as_slice() else {
            panic!("granted over a dropped live grant: {out:?}")
        };
        // Lapsed: granted.
        h.now = Ms(live_until + 1);
        let out = request(&mut h, 3, 10, ino, X, false);
        let rs = lock_replies(&out);
        let [(3, OpId(10), o)] = rs.as_slice() else {
            panic!("{out:?}")
        };
        granted(o);
    }

    /// A takeover of a released, unexpired lease waits out the grants its
    /// predecessor may still honour — and goes on waiting them out after
    /// its own tenure ends: in seed 2723 the successor's epoch flush
    /// released the lease a moment later, and its re-claim (no takeover)
    /// granted over the predecessor's live exclusive grant.
    #[test]
    fn a_released_takeovers_grace_outlives_the_successors_own_release() {
        let (mut h, ino) = holder_with_file();
        let mut out = Vec::new();
        let now = h.now;
        h.core.lock_on_released_takeover(now, &h.meta);
        assert!(h.meta.locks().quarantine_until() > now.0);
        let out1 = request(&mut h, 3, 8, ino, X, false);
        let [(3, OpId(8), LockOutcome::WouldBlock)] = lock_replies(&out1).as_slice() else {
            panic!("granted inside the grace: {out1:?}")
        };
        // The tenure ends (released, no grant of its own) and the same
        // node holds the lease again, no takeover.
        h.core.lease.released();
        h.core.lock_on_lease_gone(now, &h.meta, &mut out);
        h.advance(100);
        h.hold(2, None);
        let out2 = request(&mut h, 3, 9, ino, X, false);
        let [(3, OpId(9), LockOutcome::WouldBlock)] = lock_replies(&out2).as_slice() else {
            panic!("the grace went with the tenure: {out2:?}")
        };
        h.now = Ms(h.meta.locks().quarantine_until() + 1);
        let out3 = request(&mut h, 3, 10, ino, X, false);
        let rs = lock_replies(&out3);
        let [(3, OpId(10), o)] = rs.as_slice() else {
            panic!("{out3:?}")
        };
        granted(o);
    }

    /// A fast takeover of a tenure that granted: new lock grants wait
    /// out the predecessor's lock grants (the lock TTL), acknowledgements
    /// only its read delegations (their TTL) — a long lock TTL must not
    /// stall every write after a takeover.
    #[test]
    fn a_fast_takeover_quarantines_lock_grants_longer_than_acknowledgements() {
        let mut h = Harness::new(2);
        h.core.cfg.lock_ttl_ms = 20_000;
        h.core.cfg.read_delegation_ttl_ms = 5_000;
        let mut prev = Lease::granted("p0", 1, 1, 60_000).with_granted_delegations();
        prev.expires_unix_ms = h.now.0 + 60_000;
        h.core.note_fast_takeover(&prev);
        let mut out = Vec::new();
        let now = h.now;
        h.core.note_marker_landed(now, &h.meta, &mut out);
        let margins = 2 * h.core.cfg.expiry_margin_ms as i64 + h.core.cfg.backup_takeover_ms as i64;
        assert_eq!(
            h.meta.read_delegations().quarantine_until(),
            now.0 + 5_000 + margins,
            "acknowledgements wait for the read delegations only"
        );
        assert_eq!(
            h.meta.locks().quarantine_until(),
            now.0 + 20_000 + margins,
            "new lock grants wait for the lock grants"
        );
    }

    /// The lock-exclusion review's follow-up: a recalled grant whose
    /// first local lock never comes (the requester gave up: its answer
    /// lost, a non-blocking request that lost a race) is renewed only
    /// for the first-use budget from its install, then released — not
    /// renewed for ever while the owner's waiters wait.
    #[test]
    fn a_recalled_grant_never_used_is_released_after_the_first_use_budget() {
        let mut r = requester();
        r.meta.locks().set_first_use_budget_ms(1_500);
        let req = lock_control(&mut r, 50, 42, true);
        r.advance(10);
        let installed = r.now;
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let mut tick = timer_of(&out, TimerKind::LockRenewTick);
        r.advance(10);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 1 },
            },
        });
        assert!(
            !out.iter().any(|a| matches!(a, Action::LockFlush { .. })),
            "released before its first use could come: {out:?}"
        );
        if let Some(t) = timers(&out, TimerKind::LockRenewTick).first() {
            tick = *t;
        }
        // Nobody takes the local lock: the ticks run on.
        let mut released_at = None;
        for _ in 0..20 {
            let at = r.core.timer_at(tick).expect("the renewal tick is armed");
            r.now = at;
            let out = r.step(Event::Timer { id: tick });
            if out.iter().any(|a| matches!(a, Action::LockFlush { .. })) {
                released_at = Some(r.now);
                break;
            }
            tick = timer_of(&out, TimerKind::LockRenewTick);
        }
        let at = released_at.expect("the unused recalled grant was never released");
        assert!(
            at.0 >= installed.0 + 1_500,
            "released {} ms after the install, inside the first-use budget",
            at.0 - installed.0
        );
        assert!(
            r.meta.locks().honoured(42, at.0).is_some(),
            "released only once it had lapsed: the waiters waited a window"
        );
        assert_eq!(r.meta.locks().stats().first_use_abandoned, 1);
    }

    /// A recall that overtakes the reply carrying its grant is
    /// remembered; the reply installs the grant recalled, and — with no
    /// local lock under it — the release starts at once.
    #[test]
    fn a_recall_overtaking_its_grant_is_honoured_when_the_grant_lands() {
        let mut r = requester();
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: GrantId { node: 1, seq: 1 },
            },
        });
        assert!(
            !sends(&out)
                .iter()
                .any(|(_, m)| matches!(m, PeerMsg::LockReleased { .. })),
            "nothing to release yet: {out:?}"
        );
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(1),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let held = r.meta.locks().held(42).unwrap();
        assert!(held.recalled && held.first_use && !held.releasing);
        assert!(
            !out.iter().any(|a| matches!(a, Action::LockFlush { .. })),
            "the application gets its turn first: {out:?}"
        );
        assert_eq!(r.core.stats.lock_granted_recalled, 1);
        // The FUSE thread takes the local lock it asked for, uses it,
        // unlocks; only then is the grant released.
        assert_eq!(
            r.meta.locks().local_set(
                42,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                r.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        let out = r.step(Event::Control {
            op: OpId(51),
            req: Control::LockIdle { ino: 42 },
        });
        assert!(out
            .iter()
            .any(|a| matches!(a, Action::LockFlush { ino: 42, .. })));
    }

    /// `Waiting` re-sends after `retry_ms`; a push with the echoed send
    /// time answers the op; `NotOwner` re-routes; a non-blocking request
    /// with no reachable owner is unavailable.
    #[test]
    fn waiting_pushes_redirects_and_unavailable() {
        let mut r = requester();
        let sent = r.now;
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: LockOutcome::Waiting { retry_ms: 2_500 },
            },
        });
        let retry = timer_of(&out, TimerKind::LockRetry);
        assert_eq!(r.core.timer_at(retry), Some(sent.plus(2_500)));
        // The push comes first, echoing the send time.
        r.advance(100);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockGranted {
                ino: 42,
                sent,
                outcome: grant_msg(2),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        assert_eq!(
            r.meta.locks().held(42).unwrap().until_ms,
            sent.0 + 5_000 - 1_000
        );
        // A late retry timer is ignored.
        let out = r.step(Event::Timer { id: retry });
        assert!(sends(&out).is_empty());
        // NotOwner names the holder to ask.
        let req = lock_control(&mut r, 51, 43, false);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: LockOutcome::NotOwner { owner: 3 },
            },
        });
        assert_eq!(r.core.lease.cached_holder, Some(3));
        let retry = timer_of(&out, TimerKind::LockRetry);
        let out = r.step(Event::Timer { id: retry });
        assert!(matches!(
            sends(&out).as_slice(),
            [(3, PeerMsg::LockRequest { .. })]
        ));
        // No P2P at all: a non-blocking request is unavailable at once.
        let mut lone = Harness::new(2);
        lone.core.cfg.p2p = false;
        lone.core.lease.cached_holder = Some(1);
        let out = lone.step(Event::Control {
            op: OpId(60),
            req: Control::Lock {
                ino: 42,
                mode: X,
                blocking: false,
            },
        });
        assert_eq!(lock_answer(&out, 60), LockAnswer::Unavailable);
    }

    /// The harness's lock-failover (`contender flock: ENOLCK`): the
    /// cached holder died and the backup took the lease over before
    /// gossip told this node. A non-blocking request whose cached holder
    /// cannot be reached re-reads the lease once and asks the successor;
    /// if the lease still names the unreachable holder, it is unavailable
    /// — after one read, not a loop of them.
    #[test]
    fn an_unreachable_cached_holder_rereads_the_lease_once() {
        let mut r = requester();
        // Node 1, the cached holder, is gone.
        r.step(Event::Peers {
            links: (2..=4)
                .map(|node| crate::event::PeerLink {
                    node,
                    connected: true,
                    last_seen: None,
                    rtt_ms: Some(1),
                    since: None,
                })
                .collect(),
        });
        let lock = |r: &mut Harness, op: u64, ino: Ino| {
            r.step(Event::Control {
                op: OpId(op),
                req: Control::Lock {
                    ino,
                    mode: X,
                    blocking: false,
                },
            })
        };
        let lease_read = |out: &[Action]| {
            let ops = s3_ops(out);
            assert!(matches!(ops.as_slice(), [(_, S3Op::LeaseGet)]), "{out:?}");
            ops[0].0
        };
        let answered = |out: &[Action], op: u64| {
            out.iter()
                .any(|a| matches!(a, Action::ControlDone { op: o, .. } if *o == OpId(op)))
        };
        let out = lock(&mut r, 70, 42);
        assert!(!answered(&out, 70), "not ENOLCK at once: {out:?}");
        let get = lease_read(&out);
        // The backup, node 3, holds the lease now: asked.
        let expires = r.now.0 + 10_000;
        let out = r.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((lease_of(3, 2, expires), tag())))),
        });
        assert_eq!(r.core.lease.cached_holder, Some(3));
        assert!(matches!(
            sends(&out).as_slice(),
            [(3, PeerMsg::LockRequest { ino: 42, .. })]
        ));
        // The lease still names the dead holder (not yet expired): one
        // read, then unavailable.
        r.core.lease.cached_holder = Some(1);
        let out = lock(&mut r, 71, 43);
        let get = lease_read(&out);
        let out = r.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((lease_of(1, 1, expires), tag())))),
        });
        assert_eq!(lock_answer(&out, 71), LockAnswer::Unavailable);
        assert!(s3_ops(&out).is_empty(), "{out:?}");
    }

    /// A tenure that may not grant yet (its lease is not marked as
    /// granting: a fresh successor) still refuses a non-blocking request
    /// that a live grant conflicts with — a refusal grants nothing — and
    /// answers `Busy` only when it would have to grant.
    #[test]
    fn a_tenure_that_may_not_grant_yet_still_refuses_a_conflicting_try() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 1, ino, X, false);
        granted(&lock_replies(&out)[0].2);
        // The lease loses its granting mark (as a successor's starts).
        h.core.lease.held.as_mut().unwrap().0.granted_delegations = false;
        let out = request(&mut h, 3, 2, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(2), LockOutcome::WouldBlock)]
            ),
            "{out:?}"
        );
        // A free file: it would have to grant, so not yet.
        let other = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = request(&mut h, 3, 3, other, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(3), LockOutcome::Busy)]
            ),
            "{out:?}"
        );
    }

    /// The same failover seen earlier: the dead holder's link is not
    /// declared dead yet, so the requests to it time out; out of
    /// attempts, a non-blocking request reads the lease once before
    /// answering `ENOLCK`, and asks the successor it names.
    /// 650acc8's review, `locks-blips-tight` with in-doubt lease PUTs
    /// seed 400925: once the last holder had released, node 1 cached node
    /// 3 as the holder and node 3 cached node 1; each answered the other's
    /// requests `NotOwner` naming the requester itself, and the requester,
    /// told nothing new, asked the same node again — for 56 s, until the
    /// test's lock waiter gave up. A non-owner never names the requester;
    /// a requester told by its cached owner of no other owner reads the
    /// lease next.
    #[test]
    fn two_non_holders_caching_each_other_send_the_requester_to_the_lease() {
        // Owner side: node 2 (not holding) caches node 3 as the holder.
        let mut h = requester();
        h.core.lease.cached_holder = Some(3);
        let out = request(&mut h, 3, 7, 42, X, true);
        let [(3, OpId(7), LockOutcome::NotOwner { owner: 0 })] = lock_replies(&out).as_slice()
        else {
            panic!("named the requester as the owner: {out:?}")
        };
        // Requester side: node 2 caches node 1, which answers naming node 2.
        let mut r = requester();
        let req = lock_control(&mut r, 80, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: LockOutcome::NotOwner { owner: 2 },
            },
        });
        assert_eq!(r.core.lease.cached_holder, None, "kept the stale holder");
        let retry = timer_of(&out, TimerKind::LockRetry);
        r.advance(1_000);
        let out = r.step(Event::Timer { id: retry });
        assert!(
            !sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockRequest { .. })),
            "asked node 1 again: {out:?}"
        );
    }

    #[test]
    fn a_non_blocking_request_out_of_attempts_rereads_the_lease_once() {
        let mut r = requester();
        let mut out = r.step(Event::Control {
            op: OpId(80),
            req: Control::Lock {
                ino: 42,
                mode: X,
                blocking: false,
            },
        });
        // Every request to node 1 goes unanswered.
        for _ in 0..r.core.cfg.forward_retries {
            assert!(matches!(
                sends(&out).as_slice(),
                [(1, PeerMsg::LockRequest { .. })]
            ));
            r.advance(r.core.cfg.forward_timeout_ms);
            out = r.step(Event::Timer {
                id: timer_of(&out, TimerKind::LockRequestTimeout),
            });
            if let Some(retry) = timers(&out, TimerKind::LockRetry).first().copied() {
                r.advance(1_000);
                out = r.step(Event::Timer { id: retry });
            }
        }
        assert!(
            !out.iter()
                .any(|a| matches!(a, Action::ControlDone { op, .. } if *op == OpId(80))),
            "not ENOLCK before the lease is read: {out:?}"
        );
        let ops = s3_ops(&out);
        assert!(matches!(ops.as_slice(), [(_, S3Op::LeaseGet)]), "{out:?}");
        let expires = r.now.0 + 10_000;
        let out = r.step(Event::S3 {
            op: ops[0].0,
            result: S3Result::LeaseGet(Ok(Some((lease_of(3, 2, expires), tag())))),
        });
        assert!(matches!(
            sends(&out).as_slice(),
            [(3, PeerMsg::LockRequest { ino: 42, .. })]
        ));
        // Node 3 does not answer either: unavailable, no second read.
        let out = r.step(Event::Timer {
            id: timer_of(&out, TimerKind::LockRequestTimeout),
        });
        assert_eq!(lock_answer(&out, 80), LockAnswer::Unavailable);
        assert!(s3_ops(&out).is_empty(), "{out:?}");
    }
    /// Answers every S3 request against `object` (a lease swap or create
    /// replaces it), completes uploads, fires polls and lock retries, and
    /// reports the close the core asks for as the driver does
    /// (`flushing`), until this node holds a usable lease outside any
    /// epoch with its gate open (then a few steps more for the lock
    /// retries armed by then).
    /// Returns the lock answers seen.
    fn epoch_pump(
        h: &mut Harness,
        members: &[NodeId],
        object: &mut Lease,
        out: Vec<Action>,
    ) -> Vec<(OpId, LockAnswer)> {
        epoch_pump_with(h, members, object, out, &mut PumpFaults::default())
    }

    /// What [`epoch_pump_with`] does wrong: the next `swaps_in_doubt`
    /// lease swaps land but answer a failure (a timeout), and every S3
    /// request fails until `s3_down_until`.
    #[derive(Default)]
    struct PumpFaults {
        swaps_in_doubt: u32,
        s3_down_until: Option<Ms>,
    }

    fn epoch_pump_with(
        h: &mut Harness,
        members: &[NodeId],
        object: &mut Lease,
        mut out: Vec<Action>,
        faults: &mut PumpFaults,
    ) -> Vec<(OpId, LockAnswer)> {
        if let Some(id) = h.core.poll_timer {
            out.extend(h.step(Event::Timer { id }));
        }
        let mut answers = Vec::new();
        let mut after = 0;
        for _ in 0..400 {
            if after == 10 {
                return answers;
            }
            let last = !h.core.epoch.open
                && h.core.lease.usable(h.now, &h.core.cfg)
                && !h.core.lease.fenced();
            if last {
                after += 1;
            }
            let mut next = Vec::new();
            for action in std::mem::take(&mut out) {
                match action {
                    Action::ControlDone {
                        op,
                        result: Ok(ControlOk::Lock(answer)),
                    } => answers.push((op, answer)),
                    Action::SetTimer {
                        id,
                        kind: TimerKind::LockRetry,
                        ..
                    } => next.extend(h.step(Event::Timer { id })),
                    Action::EpochClose => next.extend(h.step(epoch_report(
                        false,
                        false,
                        true,
                        members,
                        h.core.pr.carried,
                    ))),
                    Action::UploadDirtyChunks { op, .. } => {
                        next.extend(h.step(Event::UploadsDone {
                            op,
                            result: UploadResult::Done { held: 0 },
                        }))
                    }
                    Action::S3 { op, req } => {
                        let down = faults.s3_down_until.is_some_and(|t| h.now < t);
                        let result = match req {
                            S3Op::LeaseGet if down => {
                                S3Result::LeaseGet(Err(crate::event::S3Failure("cut".into())))
                            }
                            S3Op::LeaseSwap { .. } | S3Op::LeaseCreate { .. } if down => {
                                S3Result::LeasePut(Err(CasFailure::Failed("cut".into())))
                            }
                            S3Op::SegmentRun { .. } if down => {
                                S3Result::SegmentRun(Err(crate::event::S3Failure("cut".into())))
                            }
                            S3Op::SegmentPut { .. } if down => {
                                S3Result::SegmentPut(Err(CasFailure::Failed("cut".into())))
                            }
                            _ if down => continue,
                            S3Op::LeaseGet => S3Result::LeaseGet(Ok(Some((object.clone(), tag())))),
                            S3Op::LeaseSwap { lease, .. } | S3Op::LeaseCreate { lease } => {
                                *object = lease;
                                if faults.swaps_in_doubt > 0 {
                                    faults.swaps_in_doubt -= 1;
                                    S3Result::LeasePut(Err(CasFailure::Failed("timeout".into())))
                                } else {
                                    S3Result::LeasePut(Ok(tag()))
                                }
                            }
                            S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                            S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                            S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                            S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                            S3Op::InboxDrain { .. } => S3Result::InboxDrain(Ok(Vec::new())),
                            S3Op::HeartbeatRead => S3Result::Heartbeats(Ok(Vec::new())),
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
            h.advance(10);
            out = next;
        }
        panic!(
            "never held again outside the epoch: open {} held {:?} lost {}",
            h.core.epoch.open, h.core.lease.held, h.core.lease.lost
        );
    }

    fn epoch_report(
        open: bool,
        active: bool,
        flushing: bool,
        members: &[NodeId],
        carrier: Option<crate::event::Carrier>,
    ) -> Event {
        Event::Control {
            op: OpId(1 << 40),
            req: Control::Epoch {
                open,
                active,
                frozen: false,
                flushing,
                base: 0,
                members: members.to_vec(),
                carrier,
                stale_below: 1,
            },
        }
    }

    /// The holder's own lock (a local lock under it, as an application
    /// holding its `flock`) and a peer's grant, then a continuation
    /// epoch carrying the holder's lease opens (an S3 cut).
    fn holder_with_grants_in_an_epoch() -> (Harness, Ino, Ino, GrantId, Lease) {
        let (mut h, ino) = holder_with_file();
        h.core.cfg.p2p = true;
        h.core.start(h.now, &h.meta, &mut Vec::new());
        let op = h.create("peer");
        let MutateOp::Create { ino: peer_ino, .. } = op else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::Lock {
                ino,
                mode: X,
                blocking: false,
            },
        });
        assert!(matches!(lock_answer(&out, 5), LockAnswer::Granted { .. }));
        assert_eq!(
            h.meta.locks().local_set(
                ino,
                LocalLock {
                    owner: 9,
                    pid: 1,
                    pid_start: 0,
                    write: true,
                    start: 0,
                    end: u64::MAX
                },
                h.now.0
            ),
            constellation_meta::locks::LocalOutcome::Done
        );
        let out = request(&mut h, 2, 7, peer_ino, X, false);
        let replies = lock_replies(&out);
        let [(2, OpId(7), outcome)] = replies.as_slice() else {
            panic!("{out:?}")
        };
        let (peer_grant, _) = granted(outcome);
        let object = h.core.lease.held.as_ref().unwrap().0.clone();
        let carrier = crate::event::Carrier {
            node: 1,
            epoch: object.epoch,
            expires_unix_ms: object.expires_unix_ms,
        };
        h.step(epoch_report(true, true, false, &[1, 2], Some(carrier)));
        assert!(h.core.lease.epoch_held(), "the epoch carries the lease");
        assert_eq!(h.meta.locks().grants_len(), 2);
        (h, ino, peer_ino, peer_grant, object)
    }

    /// `stress-ng-fs-faults` (bug B): every S3 blip opens a continuation
    /// epoch, and its close let the holder's lease go locally with every
    /// lock grant, though the flush re-claimed the very same lease a
    /// moment later: the holder's renewal of its own grant answered
    /// `Lost` (its lock holders fenced: `EIO`), and a peer's too. The
    /// grants now stand across a close the re-claim continues.
    #[test]
    fn lock_grants_survive_an_epoch_close_whose_reclaim_continues_the_tenure() {
        let (mut h, ino, peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        // A grant made inside the epoch, too.
        let out = request(&mut h, 3, 8, ino + 1_000, X, false);
        assert!(matches!(
            lock_replies(&out).as_slice(),
            [(3, OpId(8), LockOutcome::Granted { .. })]
        ));
        h.advance(500);
        // S3 is back: the hold owner flushes, closes and re-claims.
        epoch_pump(&mut h, &[1, 2, 3], &mut object, Vec::new());
        assert_eq!(object.holder, 1);
        assert_eq!(
            h.core.stats.releases, 0,
            "the flush keeps a lease with live grants"
        );
        assert_eq!(h.meta.locks().grants_len(), 3, "the grants stand");
        assert!(
            h.core.pr.closed_tenure.is_empty(),
            "settled by the re-claim"
        );
        // The holder's own renewal, once due, is served.
        let held = h.meta.locks().held(ino).expect("still held");
        assert!(held.until_ms > h.now.0, "lapsed during the re-claim");
        h.advance((held.renew_at_ms - h.now.0).max(0) as u64);
        let mut out = Vec::new();
        h.core.on_lock_renew_tick(h.now, &h.meta, &mut out);
        assert_eq!(h.core.stats.lock_lost, 0, "fenced: {out:?}");
        assert!(h.core.stats.lock_renewals > 0, "{out:?}");
        assert!(h
            .meta
            .locks()
            .held(ino)
            .is_some_and(|g| g.until_ms > h.now.0));
        // And the peer's.
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(70),
                entries: vec![LockRenewEntry {
                    ino: peer_ino,
                    grant: peer_grant,
                    mode: X,
                }],
            },
        });
        let renewed = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::LockRenewed { results, .. } if to == 2 => Some(results.clone()),
                _ => None,
            })
            .expect("a renewal answer");
        assert!(
            matches!(renewed.as_slice(), [(_, _, LockRenewResult::Ok { .. })]),
            "{renewed:?}"
        );
    }

    /// `locks-blips-tight` seed 99102 (three nodes): the hold owner
    /// closed its epoch in a short S3 window and was cut again before its
    /// re-claim. No next epoch could form (the other members were still
    /// in this one, waiting for that very re-claim, and a member of an
    /// open epoch joins no other), and the renewals of the grants the
    /// close kept were answered `NotOwner { 0 }`, so they lapsed under
    /// their holders' I/O. They are renewed under the closed lease now,
    /// which still stands as this node's, up to its expiry.
    #[test]
    fn kept_grants_are_renewed_in_the_reclaim_window_up_to_the_closed_leases_expiry() {
        let (mut h, ino, peer_ino, peer_grant, object) = holder_with_grants_in_an_epoch();
        // The close; S3 is cut again: nothing below answers an S3 op.
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(h.core.lease.held.is_none());
        assert!(h.core.epoch_reclaim_pending(h.now));
        let margin = h.core.cfg.expiry_margin_ms as i64;
        let renew = |h: &mut Harness, req: u64| {
            let out = h.step(Event::Peer {
                from: 2,
                msg: PeerMsg::LockRenew {
                    req: OpId(req),
                    entries: vec![LockRenewEntry {
                        ino: peer_ino,
                        grant: peer_grant,
                        mode: X,
                    }],
                },
            });
            sends(&out)
                .into_iter()
                .find_map(|(to, m)| match m {
                    PeerMsg::LockRenewed { results, .. } if to == 2 => Some(results.clone()),
                    _ => None,
                })
                .expect("a renewal answer")
        };
        // A peer's renewal: served, capped at the closed lease's expiry.
        h.advance(700);
        let renewed = renew(&mut h, 70);
        let [(_, _, LockRenewResult::Ok { ttl_ms, .. })] = renewed.as_slice() else {
            panic!("{renewed:?}")
        };
        assert!(*ttl_ms as i64 <= object.expires_unix_ms - margin - h.now.0);
        // The node's own lockers' too.
        let held = h.meta.locks().held(ino).expect("still held");
        h.advance((held.renew_at_ms - h.now.0).max(0) as u64);
        let mut out = Vec::new();
        h.core.on_lock_renew_tick(h.now, &h.meta, &mut out);
        assert_eq!(h.core.stats.lock_lost, 0, "fenced: {out:?}");
        assert!(h
            .meta
            .locks()
            .held(ino)
            .is_some_and(|g| g.until_ms > h.now.0 && g.until_ms <= object.expires_unix_ms));
        // Past the closed lease's usable end, nothing is renewed: anyone
        // may take it over from then on.
        h.now = Ms(object.expires_unix_ms - margin);
        let renewed = renew(&mut h, 71);
        assert!(
            matches!(
                renewed.as_slice(),
                [(_, _, LockRenewResult::NotOwner { owner: 0 })]
            ),
            "{renewed:?}"
        );
    }

    /// The other side of the rule: a close keeps the grants only for the
    /// lease it let go. When the lease this node acquires next is not
    /// that object (another node held it meanwhile), they are dropped.
    #[test]
    fn lock_grants_kept_at_an_epoch_close_go_if_another_tenure_intervened() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        // The close, as the driver reports it once S3 is back (flushed:
        // a flushing node would release the lease it acquires below).
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(h.core.lease.held.is_none());
        assert_eq!(h.meta.locks().grants_len(), 2, "kept for the re-claim");
        assert!(h.core.epoch_reclaim_pending(h.now));
        // Node 2 took the lease over meanwhile, and released it later:
        // this node's acquisition replaces node 2's object.
        let mut other = lease_of(2, object.epoch + 1, h.now.0 + 5_000);
        other.released = true;
        // A lock request brings the acquisition.
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = h.step(Event::Control {
            op: OpId(9),
            req: Control::Lock {
                ino: fresh,
                mode: X,
                blocking: true,
            },
        });
        epoch_pump(&mut h, &[1, 2], &mut other, out);
        assert_eq!(other.holder, 1);
        assert_eq!(h.meta.locks().grants_len(), 0, "a tenure intervened");
        assert!(h.core.pr.closed_tenure.is_empty());
    }

    /// The frozen-epoch `EROFS` of `stress-ng-fs-faults`: between an
    /// epoch's close and the lease's re-claim this node holds nothing
    /// locally, so it claimed nothing, and an S3 cut then formed an epoch
    /// carrying no lease, which refuses every write (`EROFS`) — on the
    /// node whose lease stood all along. Meanwhile it claims the lease the
    /// close let go (bounded by that lease's expiry), and the activation
    /// carrying it adopts the hold: writes go on, the grants stand, and
    /// the next close re-claims the lease as before.
    #[test]
    fn an_outage_inside_the_reclaim_window_forms_an_epoch_carrying_the_lease() {
        let (mut h, ino, _peer_ino, _peer_grant, mut object) = holder_with_grants_in_an_epoch();
        // The close as the driver reports it once S3 is back (flushed).
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(h.core.epoch_reclaim_pending(h.now));
        let view = h.core.epoch_claim_view(h.now);
        assert_eq!(view.held.as_ref(), Some(&object), "{view:?}");
        let claim = view.claim(&[1, 2]).expect("claims the lease");
        assert_eq!(claim, (object.epoch, object.expires_unix_ms, true));
        // Bounded by the lease it let go: past its expiry (less the
        // margin) it claims nothing.
        let late = Ms(object.expires_unix_ms - h.core.cfg.expiry_margin_ms as i64);
        assert!(h.core.epoch_claim_view(late).held.is_none());
        // S3 is cut again before the re-claim: the epoch carries it.
        let (carrier, stale_below) =
            crate::core::resolve_epoch_claims(&[(1, Some(claim), view.known), (2, None, 0)]);
        let carrier = carrier.expect("carried");
        assert_eq!(carrier.node, 1);
        let out = h.step(Event::Control {
            op: OpId(1 << 40),
            req: Control::Epoch {
                open: true,
                active: true,
                frozen: false,
                flushing: false,
                base: 0,
                members: vec![1, 2],
                carrier: Some(carrier),
                stale_below,
            },
        });
        assert!(h.core.lease.epoch_held(), "the hold is adopted: {out:?}");
        assert!(!h.core.epoch_refuses_writes());
        assert_eq!(h.meta.locks().grants_len(), 2, "the grants stand");
        // A grant inside the new epoch, and its own lock's renewal.
        let out = request(&mut h, 3, 8, ino + 1_000, X, false);
        assert!(matches!(
            lock_replies(&out).as_slice(),
            [(3, OpId(8), LockOutcome::Granted { .. })]
        ));
        // S3 back: the close, the flush and the re-claim of the very
        // object; the grants stand and the lease stays.
        h.advance(500);
        epoch_pump(&mut h, &[1, 2], &mut object, Vec::new());
        assert_eq!(object.holder, 1);
        assert_eq!(h.meta.locks().grants_len(), 3, "the grants stand");
        assert_eq!(h.core.stats.releases, 0);
        assert!(h.core.pr.closed_tenure.is_empty());
        assert!(h
            .meta
            .locks()
            .held(ino)
            .is_some_and(|g| g.until_ms > h.now.0));
    }

    /// A member that knows a later epoch than the lease the close let go
    /// (taken over meanwhile) makes the claim stale: the tenure is over,
    /// and its grants go.
    #[test]
    fn a_stale_claim_of_the_closed_lease_ends_its_tenure() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        let view = h.core.epoch_claim_view(h.now);
        let claim = view.claim(&[1, 2]);
        let (carrier, stale_below) = crate::core::resolve_epoch_claims(&[
            (1, claim, view.known),
            (2, None, object.epoch + 1),
        ]);
        assert!(carrier.is_none());
        h.step(Event::Control {
            op: OpId(1 << 40),
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
        assert!(!h.core.lease.epoch_held());
        assert_eq!(h.meta.locks().grants_len(), 0);
        assert!(h.core.pr.closed_tenure.is_empty());
        assert!(!h.core.epoch_reclaim_pending(h.now));
    }

    /// Chunk epoch-liveness-gap, a member's side: node 2, in an epoch
    /// carrying node 1's lease, is promised to node 1's fresh epoch (node 1
    /// closed and was cut again before its re-claim) and then activated in
    /// it, carrying the same lease. Its core never sees the epoch closed:
    /// no close, no S3 write (no acquisition, no promise) on the way, and
    /// it goes on as a member of an epoch carrying another node's lease.
    #[test]
    fn a_member_goes_from_the_open_epoch_to_its_carriers_fresh_one_without_closing() {
        let mut h = Harness::new(2);
        h.core.cfg.p2p = true;
        h.core.cfg.epoch_slack = 1;
        h.core.start(h.now, &h.meta, &mut Vec::new());
        let carrier = crate::event::Carrier {
            node: 1,
            epoch: 1,
            expires_unix_ms: h.now.0 + 5_000,
        };
        let members = [1, 2, 3];
        let mut out = h.step(epoch_report(true, true, false, &members, Some(carrier)));
        h.advance(200);
        // Promised to the new epoch (open, not active), then its activation.
        out.extend(h.step(epoch_report(true, false, false, &members, Some(carrier))));
        h.advance(20);
        out.extend(h.step(epoch_report(true, true, false, &members, Some(carrier))));
        assert!(h.core.epoch.open && h.core.epoch.active);
        assert_eq!(h.core.pr.carried, Some(carrier));
        assert!(!h.core.lease.epoch_held() && h.core.lease.held.is_none());
        assert!(
            !out.iter().any(|a| matches!(a, Action::EpochClose)),
            "{out:?}"
        );
        let writes: Vec<_> = s3_ops(&out)
            .into_iter()
            .filter(|(_, op)| {
                matches!(
                    op,
                    S3Op::LeaseCreate { .. } | S3Op::LeaseSwap { .. } | S3Op::HeartbeatPut { .. }
                )
            })
            .collect();
        assert!(writes.is_empty(), "{writes:?}");
        assert!(!h.core.epoch_reclaim_pending(h.now));
    }

    /// Review of chunk epoch-liveness-gap (should-fix 4): the member's
    /// transient report (promised to the carrier's fresh epoch: open,
    /// neither active nor frozen) is not a close. Shipping stays off, the
    /// delegates stay stopped (`dl.epoch_active`: no delegation is granted
    /// or accepted meanwhile), and no round is nudged. If the new epoch
    /// activates, nothing ran; if it is aborted instead, the close-release
    /// runs at that close.
    #[test]
    fn a_replaced_epochs_transient_report_is_not_a_close() {
        for aborted in [false, true] {
            let mut h = Harness::new(2);
            h.core.cfg.p2p = true;
            h.core.cfg.epoch_slack = 1;
            h.core.start(h.now, &h.meta, &mut Vec::new());
            let carrier = crate::event::Carrier {
                node: 1,
                epoch: 1,
                expires_unix_ms: h.now.0 + 5_000,
            };
            let members = [1, 2, 3];
            h.step(epoch_report(true, true, false, &members, Some(carrier)));
            assert!(h.core.skip_ship && h.core.dl.epoch_active);
            h.advance(200);
            // A nudge with no round in flight arms the poll now.
            let nudged = |h: &Harness, out: &[Action]| {
                h.core.nudged
                    || out
                        .iter()
                        .any(|a| matches!(a, Action::SetTimer { at, .. } if *at == h.now))
            };
            let out = h.step(epoch_report(true, false, false, &members, Some(carrier)));
            assert!(h.core.epoch_replacing);
            assert!(h.core.skip_ship, "shipping stays off");
            assert!(h.core.dl.epoch_active, "the delegates stay stopped");
            assert!(!nudged(&h, &out), "no round: {out:?}");
            h.advance(20);
            if aborted {
                let out = h.step(epoch_report(false, false, false, &members, Some(carrier)));
                assert!(!h.core.epoch_replacing);
                assert!(!h.core.skip_ship && !h.core.dl.epoch_active);
                assert!(
                    nudged(&h, &out),
                    "the close-release nudges a round: {out:?}"
                );
            } else {
                h.step(epoch_report(true, true, false, &members, Some(carrier)));
                assert!(!h.core.epoch_replacing);
                assert!(h.core.skip_ship && h.core.dl.epoch_active);
                assert!(!h.core.lease.epoch_held());
            }
        }
    }

    /// Must-fix 1 of the review: S3 is cut while the re-claim's CAS is in
    /// flight; it lands but answers a timeout. The object is then
    /// `(me, e, X')`, which the close never saw: the next acquisition
    /// replaced it and dropped every grant (and the flush then released
    /// the lease, its table empty) although nobody else had held it.
    #[test]
    fn a_reclaim_cas_in_doubt_that_landed_keeps_the_grants() {
        let (mut h, ino, _peer_ino, _peer_grant, mut object) = holder_with_grants_in_an_epoch();
        let before = object.clone();
        h.advance(500);
        let mut faults = PumpFaults {
            swaps_in_doubt: 1,
            ..Default::default()
        };
        epoch_pump_with(&mut h, &[1, 2], &mut object, Vec::new(), &mut faults);
        assert_eq!(faults.swaps_in_doubt, 0, "no CAS was in doubt");
        assert_eq!(object.holder, 1);
        assert_ne!(object.expires_unix_ms, before.expires_unix_ms);
        assert_eq!(h.meta.locks().grants_len(), 2, "the grants stand");
        assert_eq!(h.core.stats.releases, 0, "the lease stays");
        assert!(h.core.pr.closed_tenure.is_empty());
        let held = h.meta.locks().held(ino).expect("still held");
        h.advance((held.renew_at_ms - h.now.0).max(0) as u64);
        let mut out = Vec::new();
        h.core.on_lock_renew_tick(h.now, &h.meta, &mut out);
        assert_eq!(h.core.stats.lock_lost, 0, "fenced: {out:?}");
    }

    /// The other side of must-fix 1: an in-doubt CAS of the kept tenure
    /// adds its object to the tenure, nothing else does.
    #[test]
    fn only_a_kept_tenures_cas_in_doubt_joins_it() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        let it = |l: &Lease| (l.holder, l.epoch, l.expires_unix_ms);
        assert_eq!(h.core.pr.closed_tenure, vec![it(&object)]);
        let other = lease_of(2, object.epoch + 1, object.expires_unix_ms + 1);
        let mine = lease_of(1, object.epoch + 2, object.expires_unix_ms + 2);
        h.core.epoch_tenure_cas_in_doubt(&other, &mine);
        assert_eq!(
            h.core.pr.closed_tenure,
            vec![it(&object)],
            "not this tenure's"
        );
        let next = lease_of(1, object.epoch, object.expires_unix_ms + 9);
        h.core.epoch_tenure_cas_in_doubt(&object, &next);
        assert_eq!(h.core.pr.closed_tenure, vec![it(&object), it(&next)]);
    }

    /// Chunk epoch-liveness-gap (`locks-blips-tight` seed 66): the
    /// re-claim's CAS answered in doubt, S3 cut again. The closed node
    /// claims the latest object of its tenure (what the CAS wrote if it
    /// landed), carriable in an epoch with others: their members stay in
    /// it while S3 shows that object or the earlier one. An activation
    /// carrying it holds it again and keeps the grants. The claim is
    /// usable only while the earliest object is (a taker outside the
    /// epoch waits for that one's expiry).
    #[test]
    fn a_closed_claim_in_doubt_claims_the_latest_object_and_is_held_again() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        let next = lease_of(1, object.epoch, object.expires_unix_ms + 3_000);
        h.core.epoch_tenure_cas_in_doubt(&object, &next);
        let view = h.core.epoch_claim_view(h.now);
        let claimed = view.held.as_ref().expect("the closed lease is claimed");
        assert_eq!(
            (claimed.holder, claimed.epoch, claimed.expires_unix_ms),
            (1, next.epoch, next.expires_unix_ms)
        );
        assert!(!view.in_doubt);
        assert_eq!(
            view.claim(&[1, 2]),
            Some((next.epoch, next.expires_unix_ms, true))
        );
        // Past the earliest object's margin, nothing is claimed although
        // the later one is unexpired.
        let margin = h.core.cfg.expiry_margin_ms as i64;
        let at = Ms(object.expires_unix_ms - margin + 1);
        assert!(h.core.epoch_claim_view(at).held.is_none());
        let carrier = crate::event::Carrier {
            node: 1,
            epoch: next.epoch,
            expires_unix_ms: next.expires_unix_ms,
        };
        // Promised with the carrier its last activation named, then the
        // activation carrying the claim.
        let last = h.core.pr.carried;
        h.step(epoch_report(true, false, false, &[1, 2], last));
        assert!(!h.core.lease.epoch_held());
        h.step(epoch_report(true, true, false, &[1, 2], Some(carrier)));
        assert!(h.core.lease.epoch_held(), "held again");
        assert_eq!(h.core.stats.epoch_closed_leases_reheld, 1);
        assert_eq!(h.core.stats.epoch_holds_adopted_late, 0);
        assert_eq!(h.meta.locks().grants_len(), 2, "the grants stand");
    }

    /// Chunk epoch-liveness-gap (`flex-tight` seed 1913): the activation
    /// carrying the closed lease reaches its node only after that lease's
    /// expiry (a paused process). It holds it again all the same, as a
    /// held lease is (`carries_mine`): the claim was usable when acked,
    /// and nobody takes the object over while the epoch is open. Before,
    /// nobody held the epoch, and it never closed.
    #[test]
    fn a_late_activation_holds_the_closed_lease_again_past_its_expiry() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        let carrier = h.core.pr.carried;
        h.step(epoch_report(false, false, false, &[1, 2], carrier));
        assert!(h.core.epoch_claim_view(h.now).held.is_some());
        h.step(epoch_report(true, false, false, &[1, 2], carrier));
        assert!(!h.core.lease.epoch_held());
        h.advance((object.expires_unix_ms - h.now.0 + 100) as u64);
        assert!(h.core.epoch_claim_view(h.now).held.is_none(), "expired");
        h.step(epoch_report(true, true, false, &[1, 2], carrier));
        assert!(h.core.lease.epoch_held(), "the late activation holds it");
        assert_eq!(h.core.stats.epoch_closed_leases_reheld, 1);
        assert_eq!(h.meta.locks().grants_len(), 2, "the grants stand");
    }

    /// The refusing side: a CAS in doubt of the kept tenure wrote another
    /// lease epoch, so the object may be one no claim covers. Carried
    /// only by an epoch of this node alone; activated with others
    /// carrying it, the node owes the move and holds nothing.
    #[test]
    fn a_closed_claim_whose_cas_wrote_another_epoch_is_carried_alone_only() {
        let (mut h, _ino, _peer_ino, _peer_grant, object) = holder_with_grants_in_an_epoch();
        let carrier = h.core.pr.carried;
        h.step(epoch_report(false, false, false, &[1, 2], carrier));
        let other = lease_of(1, object.epoch + 1, object.expires_unix_ms + 3_000);
        h.core.epoch_tenure_cas_in_doubt(&object, &other);
        let view = h.core.epoch_claim_view(h.now);
        assert!(view.in_doubt);
        let claim = |members: &[NodeId]| view.claim(members).map(|c| c.2);
        assert_eq!(claim(&[1, 2]), Some(false));
        assert_eq!(claim(&[1]), Some(true));
        h.step(epoch_report(true, false, false, &[1, 2], carrier));
        assert!(!h.core.lease.epoch_held());
        h.step(epoch_report(true, true, false, &[1, 2], carrier));
        assert!(!h.core.lease.epoch_held(), "no hold");
        assert!(h.core.pr.owes_move.is_some(), "the move is owed");
    }

    /// Must-fix 2 of the review: a local lock asked for between the
    /// close and the re-claim, without P2P (`Route::Unknown` answered
    /// `ENOLCK` at once) — a free file waits for the re-claim and is
    /// granted; one a kept grant conflicts with would block.
    #[test]
    fn a_local_lock_in_the_reclaim_window_without_p2p_waits_for_the_reclaim() {
        let (mut h, _ino, peer_ino, _peer_grant, mut object) = holder_with_grants_in_an_epoch();
        h.core.cfg.p2p = false;
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(h.core.epoch_reclaim_pending(h.now));
        let out = h.step(Event::Control {
            op: OpId(10),
            req: Control::Lock {
                ino: peer_ino,
                mode: X,
                blocking: false,
            },
        });
        assert_eq!(lock_answer(&out, 10), LockAnswer::WouldBlock);
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = h.step(Event::Control {
            op: OpId(9),
            req: Control::Lock {
                ino: fresh,
                mode: X,
                blocking: false,
            },
        });
        assert!(
            !out.iter()
                .any(|a| matches!(a, Action::ControlDone { op: OpId(9), .. })),
            "answered at once: {out:?}"
        );
        let answers = epoch_pump(&mut h, &[1, 2], &mut object, out);
        assert!(
            matches!(answers.as_slice(), [(OpId(9), LockAnswer::Granted { .. })]),
            "{answers:?}"
        );
        assert_eq!(h.core.stats.lock_unavailable, 0);
        assert_eq!(h.meta.locks().grants_len(), 3);
    }

    /// Must-fix 2 of the review: S3 is cut again before the re-claim
    /// lands (every lease read fails): a non-blocking lock waits for the
    /// re-claim rather than spending its attempts and failing `ENOLCK`.
    #[test]
    fn a_lock_in_the_reclaim_window_waits_through_failing_lease_reads() {
        let (mut h, _ino, _peer_ino, _peer_grant, mut object) = holder_with_grants_in_an_epoch();
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = h.step(Event::Control {
            op: OpId(9),
            req: Control::Lock {
                ino: fresh,
                mode: X,
                blocking: false,
            },
        });
        let mut faults = PumpFaults {
            s3_down_until: Some(h.now.plus(1_500)),
            ..Default::default()
        };
        let answers = epoch_pump_with(&mut h, &[1, 2], &mut object, out, &mut faults);
        assert!(
            h.now.0 >= faults.s3_down_until.unwrap().0,
            "answered in the cut"
        );
        assert!(
            matches!(answers.as_slice(), [(OpId(9), LockAnswer::Granted { .. })]),
            "{answers:?}"
        );
        assert_eq!(h.core.stats.lock_unavailable, 0);
        assert_eq!(h.meta.locks().grants_len(), 3, "the kept grants stand");
    }

    /// A peer's request to the node in its re-claim window: a conflict
    /// would block, anything else is told to wait (`Waiting`, which
    /// spends no attempt), not `NotOwner` (to the lease, which names this
    /// node) nor `Busy`.
    #[test]
    fn a_resuming_owner_tells_a_peer_to_wait() {
        let (mut h, _ino, peer_ino, _peer_grant, _object) = holder_with_grants_in_an_epoch();
        h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        let out = request(&mut h, 3, 20, peer_ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(20), LockOutcome::WouldBlock)]
            ),
            "{out:?}"
        );
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = request(&mut h, 3, 21, fresh, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(21), LockOutcome::Waiting { .. })]
            ),
            "{out:?}"
        );
    }

    /// Should-fix 3 of the review: only an owner about to own the root
    /// again makes a non-blocking request wait (`Waiting`, up to
    /// `s3_less_deadline_ms`); an owner answering `Busy` for any other
    /// reason (no fresh S3 liveness, an unmarked lease) still fails it
    /// fast (`ENOLCK` after the attempts and one lease read), as before.
    #[test]
    fn busy_fails_a_non_blocking_lock_fast_and_waiting_does_not() {
        let mut r = requester();
        let start = r.now;
        let mut out = lock_control_out(&mut r, 80, 42, false);
        let mut answered = None;
        for _ in 0..20 {
            if let Some(a) = out.iter().find_map(|a| match a {
                Action::ControlDone {
                    op: OpId(80),
                    result: Ok(ControlOk::Lock(answer)),
                } => Some(*answer),
                _ => None,
            }) {
                answered = Some(a);
                break;
            }
            let mut next = Vec::new();
            for (to, m) in sends(&out) {
                if let PeerMsg::LockRequest { req, .. } = m {
                    next.extend(r.step(Event::Peer {
                        from: to,
                        msg: PeerMsg::LockReply {
                            req: *req,
                            outcome: LockOutcome::Busy,
                        },
                    }));
                }
            }
            for (op, req) in s3_ops(&out) {
                if matches!(req, S3Op::LeaseGet) {
                    let expires = r.now.0 + 10_000;
                    next.extend(r.step(Event::S3 {
                        op,
                        result: S3Result::LeaseGet(Ok(Some((lease_of(1, 2, expires), tag())))),
                    }));
                }
            }
            for t in timers(&out, TimerKind::LockRetry) {
                r.advance(300);
                next.extend(r.step(Event::Timer { id: t }));
            }
            out = next;
        }
        assert_eq!(answered, Some(LockAnswer::Unavailable));
        assert!(
            r.now.0 - start.0 < 5_000,
            "Busy waited {} ms",
            r.now.0 - start.0
        );
        // `Waiting` to a non-blocking request: retried without failing,
        // until the bound.
        let mut r = requester();
        let start = r.now;
        let mut out = lock_control_out(&mut r, 81, 42, false);
        let mut answered = None;
        for _ in 0..1_000 {
            if let Some(a) = out.iter().find_map(|a| match a {
                Action::ControlDone {
                    op: OpId(81),
                    result: Ok(ControlOk::Lock(answer)),
                } => Some(*answer),
                _ => None,
            }) {
                answered = Some(a);
                break;
            }
            let mut next = Vec::new();
            for (to, m) in sends(&out) {
                if let PeerMsg::LockRequest { req, .. } = m {
                    next.extend(r.step(Event::Peer {
                        from: to,
                        msg: PeerMsg::LockReply {
                            req: *req,
                            outcome: LockOutcome::Waiting { retry_ms: 50 },
                        },
                    }));
                }
            }
            for (op, req) in s3_ops(&out) {
                if matches!(req, S3Op::LeaseGet) {
                    let expires = r.now.0 + 60_000;
                    next.extend(r.step(Event::S3 {
                        op,
                        result: S3Result::LeaseGet(Ok(Some((lease_of(1, 2, expires), tag())))),
                    }));
                }
            }
            for t in timers(&out, TimerKind::LockRetry) {
                r.advance(100);
                next.extend(r.step(Event::Timer { id: t }));
            }
            out = next;
        }
        let waited = r.now.0 - start.0;
        assert_eq!(answered, Some(LockAnswer::Unavailable));
        assert!(
            waited >= r.core.cfg.s3_less_deadline_ms as i64,
            "gave up after {waited} ms"
        );
    }

    /// A close that keeps the grants keeps its waiters asking here:
    /// `Waiting`, not `NotOwner { owner: 0 }`, which made the peer forget
    /// this node as the holder — and with S3 cut again before the
    /// re-claim it could not learn it back, so its own grant's renewals
    /// found no owner until the grant lapsed under its I/O
    /// (`locks-blips-tight`).
    #[test]
    fn a_close_that_keeps_the_grants_tells_its_waiters_to_wait() {
        let (mut h, _ino, peer_ino, _peer_grant, _object) = holder_with_grants_in_an_epoch();
        let out = request(&mut h, 3, 30, peer_ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        let out = h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::Waiting { .. })]
            ),
            "{out:?}"
        );
        // A close that does not keep them (the lease is lost) still
        // sends its waiters to the lease.
        let (mut h, _ino, peer_ino, _peer_grant, _object) = holder_with_grants_in_an_epoch();
        let out = request(&mut h, 3, 30, peer_ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        h.core.lease.force_lost();
        let out = h.step(epoch_report(
            false,
            false,
            false,
            &[1, 2],
            h.core.pr.carried,
        ));
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::NotOwner { owner: 0 })]
            ),
            "{out:?}"
        );
        assert_eq!(h.meta.locks().grants_len(), 0);
    }

    /// A holder whose acquisition gate is pending serves renewals — its
    /// own lockers' too, as a peer's (`lock_renew_one`). Its own found no
    /// owner (`lock_route` fences the view for new grants) and spun on
    /// relearning until the grant lapsed under the I/O: a re-claim whose
    /// marker an S3 cut held back (`locks-blips-tight`).
    #[test]
    fn own_renewals_are_served_while_the_gate_is_pending() {
        let (mut h, ino) = holder_with_file();
        let out = h.step(Event::Control {
            op: OpId(5),
            req: Control::Lock {
                ino,
                mode: X,
                blocking: false,
            },
        });
        assert!(matches!(lock_answer(&out, 5), LockAnswer::Granted { .. }));
        h.hold(
            1,
            Some(PendingGate {
                epoch: 1,
                takeover: true,
                marker_shipped: false,
                drained: true,
                fast_prev: None,
                backup_tail_epoch: None,
                shippable: false,
            }),
        );
        assert!(h.core.lease.fenced());
        let held = h.meta.locks().held(ino).expect("held");
        h.advance((held.renew_at_ms - h.now.0).max(0) as u64);
        let renewals = h.core.stats.lock_renewals;
        let mut out = Vec::new();
        h.core.on_lock_renew_tick(h.now, &h.meta, &mut out);
        assert!(h.core.stats.lock_renewals > renewals, "{out:?}");
        assert!(
            s3_ops(&out).is_empty(),
            "relearned the owner from the lease: {out:?}"
        );
        let renewed = h.meta.locks().held(ino).expect("held");
        assert!(renewed.until_ms > held.until_ms);
    }

    /// Re-adopting its own lease keeps the granting mark: the tenure goes
    /// on. Dropped, a re-claim that S3 cut again could not mark the lease
    /// again, and every lock request was answered `Busy` (`ENOLCK` for a
    /// non-blocking one) until S3 returned (`locks-blips-tight`). Another
    /// node's lease is never inherited marked.
    #[test]
    fn a_re_adopted_own_lease_keeps_its_granting_mark() {
        let h = Harness::new(1);
        assert!(!h.core.cfg.strict_mounts);
        let mut own = lease_of(1, 4, h.now.0 + 5_000);
        own.granted_delegations = true;
        let next = h.core.lease.granted_lease(h.now, &h.core.cfg, Some(&own));
        assert_eq!(next.epoch, 4);
        assert!(next.granted_delegations);
        let mut other = lease_of(2, 4, h.now.0 + 5_000);
        other.granted_delegations = true;
        let next = h.core.lease.granted_lease(h.now, &h.core.cfg, Some(&other));
        assert_eq!(next.epoch, 5);
        assert!(!next.granted_delegations);
        own.released = true;
        let next = h.core.lease.granted_lease(h.now, &h.core.cfg, Some(&own));
        assert!(
            !next.granted_delegations,
            "a released lease ended its tenure"
        );
    }

    fn lock_control_out(r: &mut Harness, op: u64, ino: Ino, blocking: bool) -> Vec<Action> {
        r.step(Event::Control {
            op: OpId(op),
            req: Control::Lock {
                ino,
                mode: X,
                blocking,
            },
        })
    }

    // ---- plan 30 §M14 phase 2: the fencing token ----

    use constellation_meta::locks::{LockTag, LockToken};

    fn token(grant: GrantId, until_ms: i64) -> LockTag {
        LockTag(vec![LockToken { grant, until_ms }])
    }

    fn setattr_mtime(ino: Ino, mtime_ns: i64) -> MutateOp {
        MutateOp::Setattr {
            ino,
            mode: None,
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: Some(mtime_ns),
        }
    }

    fn forward_tagged(
        h: &mut Harness,
        from: NodeId,
        req: u64,
        rid: Rid,
        op: MutateOp,
        tag: LockTag,
    ) -> Vec<Action> {
        h.step(Event::Peer {
            from,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid,
                op,
                acked_through: 0,
                deps: Position::ZERO,
                tag,
                applied: 0,
            },
        })
    }

    /// An open epoch's rounds are its probes: the sync interval at first
    /// (an epoch carrying no lease used to probe every 10 s once idle,
    /// refusing writes with `EROFS` that long after S3 returned), backed
    /// off to four times it while nothing changes (a frozen epoch can
    /// stay open for hours with S3 reachable), and back to the sync
    /// interval once S3 answers after a failure.
    #[test]
    fn an_open_epochs_probes_back_off_and_reset_when_s3_returns() {
        let mut h = Harness::new(2);
        h.core.idle_rounds = 10;
        let base = h.core.cfg.sync_interval_ms;
        assert_eq!(h.core.next_poll_ms(h.now), h.core.cfg.idle_max_ms);
        h.step(epoch_report(true, true, false, &[1, 2], None));
        assert!(h.core.epoch_refuses_writes());
        let mut seen = Vec::new();
        let poll = |h: &mut Harness| {
            let now = h.now;
            h.core.on_poll(now, &h.meta, &mut Vec::new());
        };
        for _ in 0..5 {
            seen.push(h.core.next_poll_ms(h.now));
            poll(&mut h);
        }
        assert_eq!(seen, vec![base, base, 2 * base, 4 * base, 4 * base]);
        let down = S3Result::SegmentRun(Err(crate::event::S3Failure("cut".into())));
        h.core.note_epoch_probe(&down);
        assert_eq!(
            h.core.next_poll_ms(h.now),
            4 * base,
            "a failure changes nothing"
        );
        h.core
            .note_epoch_probe(&S3Result::SegmentRun(Ok(Vec::new())));
        assert_eq!(h.core.next_poll_ms(h.now), base, "S3 is back");
        h.core
            .note_epoch_probe(&S3Result::SegmentRun(Ok(Vec::new())));
        poll(&mut h);
        poll(&mut h);
        assert_eq!(
            h.core.next_poll_ms(h.now),
            2 * base,
            "only a return after a failure resets"
        );
        // Closed: the idle backoff again.
        h.step(epoch_report(false, false, false, &[1, 2], None));
        assert_ne!(h.core.next_poll_ms(h.now), 4 * base);
    }

    /// A held lease's poll fires when its renewal falls due (half its
    /// TTL left), not up to a quarter TTL later: the root caps every
    /// delegation, and every delegated lock grant through it, by what is
    /// left of its lease, and a renewal sent with a quarter TTL left
    /// shrank a delegated grant's renewals to about the margin
    /// (`locks-delegated` seed 1432).
    #[test]
    fn a_held_leases_poll_fires_when_its_renewal_is_due() {
        let mut h = Harness::new(1);
        h.core.idle_rounds = 10;
        let half = (h.core.cfg.ttl_ms / 2) as i64;
        let lease = lease_of(1, 1, h.now.0 + half + 300);
        h.core.lease.adopt(h.now, lease, tag(), None);
        assert_eq!(h.core.next_poll_ms(h.now), 300);
        // Due already (a renewal that failed): the quarter-TTL cap.
        let lease = lease_of(1, 1, h.now.0 + half - 10);
        h.core.lease.adopt(h.now, lease, tag(), None);
        assert_eq!(h.core.next_poll_ms(h.now), h.core.cfg.ttl_ms / 4);
        // Far from due: the quarter-TTL cap as before.
        let lease = lease_of(1, 1, h.now.0 + 2 * half);
        h.core.lease.adopt(h.now, lease, tag(), None);
        assert_eq!(h.core.next_poll_ms(h.now), h.core.cfg.ttl_ms / 4);
    }

    fn mutate_reply(out: &[Action], to: NodeId, req: u64) -> MutateOutcome {
        sends(out)
            .into_iter()
            .find_map(|(t, m)| match m {
                PeerMsg::MutateReply {
                    req: r, outcome, ..
                } if t == to && *r == OpId(req) => Some(outcome.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("no reply to node {to}'s request {req}: {out:?}"))
    }

    fn peer_rid(node: NodeId, seq: u64) -> Rid {
        Rid {
            node,
            incarnation: 1,
            seq,
        }
    }

    fn mtime(h: &Harness, ino: Ino) -> i64 {
        MetaStore::getattr(&h.meta, ino).unwrap().unwrap().mtime_ns
    }

    /// The gap phase 1 left (Kleppmann's fencing-token argument): node 2's
    /// write passes its node-local fence while its grant is honoured, then
    /// stalls (a SIGSTOPped daemon, a forward stuck in flight) past the
    /// grant; the sequencer outwaits the grant and gives the lock to node
    /// 3, which writes under it; then node 2's stalled forward arrives.
    /// It carries its token — the grant and the window node 2 honoured it
    /// for — and the sequencer refuses it: `LockLapsed`, nothing executed,
    /// nothing journaled (no `Refused` row either), and a retry of the
    /// same rid is refused the same way. Node 3's write stands.
    #[test]
    fn a_forward_stalled_past_its_grant_is_refused_after_the_next_holders_write() {
        let (mut h, ino) = holder_with_file();
        let data = h.create("data");
        let MutateOp::Create { ino: data_ino, .. } = data else {
            unreachable!()
        };
        constellation_meta::execute_mutate(&h.meta, &data, None).unwrap();
        let t0 = h.now;
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, ttl) = granted(&lock_replies(&out)[0].2);
        // What node 2 honours the grant for: `sent + ttl − margin`.
        let window2 = t0.0 + ttl as i64 - 1_000;
        // Node 2's write passed its fence at `window2 − 100`, then stalled.
        // The sequencer records the grant until `granted + ttl + margin`:
        // past that, node 3 gets the lock.
        h.now = t0.plus(ttl + 1_000 + 1);
        let out = request(&mut h, 3, 8, ino, X, true);
        let (g3, _) = granted(&lock_replies(&out)[0].2);
        let window3 = h.now.0 + ttl as i64 - 1_000;
        let out = forward_tagged(
            &mut h,
            3,
            20,
            peer_rid(3, 1),
            setattr_mtime(data_ino, 3),
            token(g3, window3),
        );
        assert!(
            matches!(mutate_reply(&out, 3, 20), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, data_ino), 3);
        // Node 2's stalled forward lands now.
        let next = crate::replica::Replica::journal_next_seq(&h.meta).unwrap();
        let rid2 = peer_rid(2, 1);
        let out = forward_tagged(
            &mut h,
            2,
            21,
            rid2,
            setattr_mtime(data_ino, 2),
            token(g2, window2),
        );
        assert_eq!(mutate_reply(&out, 2, 21), MutateOutcome::LockLapsed);
        assert_eq!(mtime(&h, data_ino), 3, "node 3's write stands");
        assert_eq!(
            crate::replica::Replica::journal_next_seq(&h.meta).unwrap(),
            next,
            "nothing journaled"
        );
        assert!(h.meta.completed_outcome(rid2).unwrap().is_none());
        // Its retry by rid (the reply was lost) is refused again.
        h.advance(200);
        let out = forward_tagged(
            &mut h,
            2,
            22,
            rid2,
            setattr_mtime(data_ino, 2),
            token(g2, window2),
        );
        assert_eq!(mutate_reply(&out, 2, 22), MutateOutcome::LockLapsed);
        assert_eq!(h.core.stats.lock_lapsed_refusals, 2);
        assert_eq!(h.meta.locks().stats().token_rejections, 2);
    }

    /// The same op, landing while the grant is still honoured, executes:
    /// the token costs a live holder nothing.
    #[test]
    fn a_forward_inside_its_grant_executes() {
        let (mut h, ino) = holder_with_file();
        let t0 = h.now;
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, ttl) = granted(&lock_replies(&out)[0].2);
        h.advance(ttl - 1_100);
        let out = forward_tagged(
            &mut h,
            2,
            21,
            peer_rid(2, 1),
            setattr_mtime(ino, 2),
            token(g2, t0.0 + ttl as i64 - 1_000),
        );
        assert!(
            matches!(mutate_reply(&out, 2, 21), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, ino), 2);
        assert_eq!(h.meta.locks().stats().token_rejections, 0);
    }

    /// Lock-fence-token review must-fix 2: the minter judges a grant it
    /// holds live exactly. Its holder kept renewing, so an op whose token
    /// window has passed (it was taken before a slow flush, or a forward
    /// was retried) still executes there; once the minter's own record
    /// has expired (outwaited) it is refused.
    #[test]
    fn the_minter_accepts_a_renewed_grants_op_past_its_token_window() {
        let (mut h, ino) = holder_with_file();
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, ttl) = granted(&lock_replies(&out)[0].2);
        let stale_window = h.now.0 + 100;
        h.advance(ttl / 2);
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(20),
                entries: vec![LockRenewEntry {
                    ino,
                    grant: g2,
                    mode: X,
                }],
            },
        });
        assert!(!sends(&out).is_empty());
        let out = forward_tagged(
            &mut h,
            2,
            21,
            peer_rid(2, 1),
            setattr_mtime(ino, 2),
            token(g2, stale_window),
        );
        assert!(
            matches!(mutate_reply(&out, 2, 21), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, ino), 2);
        // Never renewed again: the record runs out, the grant is dead.
        let record = h.meta.locks().grants_snapshot()[0].until_ms;
        h.now = Ms(record);
        h.hold(1, None);
        let out = forward_tagged(
            &mut h,
            2,
            22,
            peer_rid(2, 2),
            setattr_mtime(ino, 3),
            token(g2, record + 1_000),
        );
        assert_eq!(mutate_reply(&out, 2, 22), MutateOutcome::LockLapsed);
        assert_eq!(mtime(&h, ino), 2);
    }

    /// The minting sequencer knows releases exactly: a token naming a
    /// grant its holder released is refused at once, inside the window.
    #[test]
    fn the_minter_refuses_a_released_grants_token_inside_its_window() {
        let (mut h, ino) = holder_with_file();
        let t0 = h.now;
        let out = request(&mut h, 2, 7, ino, X, true);
        let (g2, ttl) = granted(&lock_replies(&out)[0].2);
        h.advance(100);
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockReleased {
                ino,
                grant: g2,
                position: Position::ZERO,
            },
        });
        let out = forward_tagged(
            &mut h,
            2,
            21,
            peer_rid(2, 1),
            setattr_mtime(ino, 2),
            token(g2, t0.0 + ttl as i64 - 1_000),
        );
        assert_eq!(mutate_reply(&out, 2, 21), MutateOutcome::LockLapsed);
    }

    /// A sequencer that did not mint the grant (the git case: the lock
    /// file is the root's, the objects live in a delegated subtree) judges
    /// the token by its window alone, on its own clock.
    #[test]
    fn a_sequencer_that_did_not_mint_the_grant_checks_the_window() {
        let (mut h, ino) = holder_with_file();
        let foreign = GrantId { node: 9, seq: 4 };
        let until = h.now.0 + 50;
        let out = forward_tagged(
            &mut h,
            2,
            21,
            peer_rid(2, 1),
            setattr_mtime(ino, 2),
            token(foreign, until),
        );
        assert!(
            matches!(mutate_reply(&out, 2, 21), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        h.advance(50);
        let out = forward_tagged(
            &mut h,
            2,
            22,
            peer_rid(2, 2),
            setattr_mtime(ino, 3),
            token(foreign, until),
        );
        assert_eq!(mutate_reply(&out, 2, 22), MutateOutcome::LockLapsed);
        assert_eq!(mtime(&h, ino), 2);
    }

    /// An op of the holder's own client, executed here (the core's local
    /// path), is checked the same way.
    #[test]
    fn the_holders_own_op_under_a_lapsed_grant_is_refused() {
        let (mut h, ino) = holder_with_file();
        let next = crate::replica::Replica::journal_next_seq(&h.meta).unwrap();
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            rid,
            op: setattr_mtime(ino, 7),
            policy: Policy::Client,
            tag: token(GrantId { node: 1, seq: 1 }, h.now.0 - 1),
        });
        let rs = replies(&out);
        assert!(
            matches!(
                rs.as_slice(),
                [(r, ClientReply::Outcome(MutateOutcome::LockLapsed))] if *r == rid
            ),
            "{out:?}"
        );
        assert_eq!(
            crate::replica::Replica::journal_next_seq(&h.meta).unwrap(),
            next
        );
        assert_ne!(mtime(&h, ino), 7);
    }

    /// Layer A: a stranded op replayed by rid after a holder change carries
    /// its token (persisted with its replay row). The forward goes out with
    /// it; the sequencer refuses it; the replay is settled as refused (its
    /// conflict copy keeps what it would have written aside) and is never
    /// sent again — the requester does not loop.
    #[test]
    fn a_replay_by_rid_carries_its_token_and_a_lapsed_one_is_not_resent() {
        let mut r = requester();
        let op = r.create("x");
        let rid = r.rid(1);
        let dead = token(GrantId { node: 1, seq: 5 }, r.now.0 - 10);
        r.meta.queue_replay(rid, &op, &dead).unwrap();
        let mut out = Vec::new();
        r.core.on_drain_tick(r.now, &r.meta, &mut out);
        let req = match sends(&out).as_slice() {
            [(
                1,
                PeerMsg::MutateRequest {
                    req, rid: rr, tag, ..
                },
            )] => {
                assert_eq!(*rr, rid);
                assert_eq!(*tag, dead, "the replay carries its token");
                *req
            }
            other => panic!("expected the replay's forward: {other:?}"),
        };
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::MutateReply {
                req,
                outcome: MutateOutcome::LockLapsed,
                base: None,
                position: Position::ZERO,
                gen: 0,
                own_chunks: OwnChunks::None,
                own_rows: None,
            },
        });
        assert!(
            !sends(&out)
                .iter()
                .any(|(_, m)| matches!(m, PeerMsg::MutateRequest { .. })),
            "{out:?}"
        );
        let queued = r.meta.pending_replays().unwrap();
        assert!(
            queued.iter().all(|q| q.refused.is_some()),
            "settled as refused: {queued:?}"
        );
        for _ in 0..3 {
            r.advance(500);
            let mut out = Vec::new();
            r.core.on_drain_tick(r.now, &r.meta, &mut out);
            assert!(
                !sends(&out)
                    .iter()
                    .any(|(_, m)| matches!(m, PeerMsg::MutateRequest { .. })),
                "a refused replay is never resent: {out:?}"
            );
        }
    }

    /// The takeover gate's local replay checks the token too.
    #[test]
    fn a_local_replay_under_a_lapsed_grant_is_refused() {
        let (mut h, ino) = holder_with_file();
        let rid = peer_rid(2, 9);
        h.meta
            .queue_replay(
                rid,
                &setattr_mtime(ino, 5),
                &token(GrantId { node: 1, seq: 5 }, h.now.0 - 10),
            )
            .unwrap();
        let mut out = Vec::new();
        h.core
            .replay_queue_locally(h.now, &h.meta, &mut out)
            .unwrap();
        assert_ne!(mtime(&h, ino), 5, "not executed");
        let queued = h.meta.pending_replays().unwrap();
        assert!(
            queued.iter().all(|q| q.refused.is_some()),
            "refused: {queued:?}"
        );
    }

    /// Release ordering: a recalled grant is not released while an op
    /// tagged with it is in flight, nor while one left in doubt may still
    /// execute somewhere (until its token's window is over) — released
    /// earlier, it could land after the next holder's writes at a
    /// sequencer that only checks the window.
    #[test]
    fn a_release_waits_for_the_ops_tagged_with_its_grant() {
        let mut r = requester();
        let req = lock_control(&mut r, 50, 42, true);
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(3),
            },
        });
        assert!(matches!(lock_answer(&out, 50), LockAnswer::Granted { .. }));
        let g = GrantId { node: 1, seq: 3 };
        let held = r.meta.locks().held(42).unwrap();
        let tag = token(g, held.until_ms);
        // The application locked, wrote, unlocked.
        let local = LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        };
        assert_eq!(
            r.meta.locks().local_set(42, local, r.now.0),
            constellation_meta::locks::LocalOutcome::Done
        );
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        // Two ops tagged with the grant: one in flight, one ends in doubt.
        r.meta.locks().tag_begin(&tag, r.now.0);
        r.meta.locks().tag_begin(&tag, r.now.0);
        r.meta.locks().tag_end(&tag, true, r.now.0);
        // Recalled with nothing under it: flushed, but not released.
        let out = r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: g,
            },
        });
        assert!(out
            .iter()
            .any(|a| matches!(a, Action::LockFlush { ino: 42, .. })));
        let out = r.step(Event::LockFlushed {
            ino: 42,
            grant: g,
            ok: true,
        });
        assert!(
            !sends(&out)
                .iter()
                .any(|(_, m)| matches!(m, PeerMsg::LockReleased { .. })),
            "released with a tagged op in flight: {out:?}"
        );
        let mut wait = timer_of(&out, TimerKind::LockReleaseWait);
        assert_eq!(r.meta.locks().stats().release_waits, 1);
        // The op in flight is answered; the in-doubt one still pins the
        // grant until its window is over.
        r.advance(10);
        r.meta.locks().tag_end(&tag, false, r.now.0);
        let out = r.step(Event::Timer { id: wait });
        assert!(
            !sends(&out)
                .iter()
                .any(|(_, m)| matches!(m, PeerMsg::LockReleased { .. })),
            "released while an in-doubt op may still land: {out:?}"
        );
        wait = timer_of(&out, TimerKind::LockReleaseWait);
        r.now = Ms(held.until_ms);
        let out = r.step(Event::Timer { id: wait });
        assert!(
            sends(&out)
                .iter()
                .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockReleased { .. }))
                || r.meta.locks().held(42).is_none(),
            "released (or lapsed) once the window is over: {out:?}"
        );
        assert_eq!(r.meta.locks().stats().release_waits, 1);
    }

    /// A requester holding grant `g` (seq 3) on inode 42 with nothing
    /// under it, recalled and flushed while `pin` keeps the release
    /// waiting: the release's actions and its wait timer.
    fn recalled_and_waiting(
        r: &mut Harness,
        pin: impl FnOnce(&Harness, GrantId, i64),
    ) -> (GrantId, Vec<Action>) {
        let req = lock_control(r, 50, 42, true);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockReply {
                req,
                outcome: grant_msg(3),
            },
        });
        let g = GrantId { node: 1, seq: 3 };
        let until = r.meta.locks().held(42).unwrap().until_ms;
        let local = LocalLock {
            owner: 9,
            pid: 1,
            pid_start: 0,
            write: true,
            start: 0,
            end: u64::MAX,
        };
        r.meta.locks().local_set(42, local, r.now.0);
        assert!(r.meta.locks().local_unlock(42, 9, 0, u64::MAX, r.now.0));
        pin(r, g, until);
        r.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockRecall {
                req: OpId(77),
                ino: 42,
                grant: g,
            },
        });
        let out = r.step(Event::LockFlushed {
            ino: 42,
            grant: g,
            ok: true,
        });
        assert!(!released(&out), "released while pinned: {out:?}");
        (g, out)
    }

    fn released(out: &[Action]) -> bool {
        sends(out)
            .iter()
            .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::LockReleased { .. }))
    }

    /// Review nit: the release waiting for a tagged op is woken by the
    /// op's end (`Control::LockReleaseWake`, which the FUSE thread's
    /// `tag_end` sends), not found by a 10 ms poll; the poll stays only
    /// as a slow fallback.
    #[test]
    fn the_last_tagged_op_wakes_a_waiting_release() {
        let mut r = requester();
        let (g, out) = recalled_and_waiting(&mut r, |r, g, until| {
            r.meta.locks().tag_begin(&token(g, until), r.now.0);
        });
        let wait = timer_of(&out, TimerKind::LockReleaseWait);
        let at = r.core.timers.get(&wait).map(|t| t.1).unwrap();
        assert!(at.0 >= r.now.0 + 200, "a slow fallback poll: {at:?}");
        let until = r.meta.locks().held(42).unwrap().until_ms;
        r.meta.locks().tag_end(&token(g, until), false, r.now.0);
        let out = r.step(Event::Control {
            op: OpId(90),
            req: Control::LockReleaseWake { ino: 42 },
        });
        assert!(released(&out), "{out:?}");
        assert!(
            out.iter()
                .any(|a| matches!(a, Action::CancelTimer { id } if *id == wait)),
            "the fallback poll is cancelled: {out:?}"
        );
        // A wake with no release waiting does nothing.
        let out = r.step(Event::Control {
            op: OpId(91),
            req: Control::LockReleaseWake { ino: 42 },
        });
        assert!(!released(&out));
    }

    /// Release ordering covers stranded ops too: one tagged with the
    /// grant and queued for replay by rid (after a holder change) keeps
    /// the release waiting until it is resolved — released earlier, its
    /// replay could land after the next holder's writes at an executor
    /// that only checks the window.
    #[test]
    fn a_queued_replay_tagged_with_the_grant_holds_its_release() {
        let mut r = requester();
        let rid = Rid {
            node: 2,
            incarnation: 1,
            seq: 5,
        };
        let (_, out) = recalled_and_waiting(&mut r, |r, g, until| {
            let op = MutateOp::Setattr {
                ino: 42,
                mode: Some(0o600),
                uid: None,
                gid: None,
                size: None,
                atime_ns: None,
                mtime_ns: None,
            };
            r.meta.queue_replay(rid, &op, &token(g, until)).unwrap();
        });
        let wait = timer_of(&out, TimerKind::LockReleaseWait);
        let queued = r.meta.pending_replays().unwrap();
        r.meta.forget_replay(queued[0].queue_seq).unwrap();
        r.advance(300);
        let out = r.step(Event::Timer { id: wait });
        assert!(released(&out), "resolved: released: {out:?}");
    }

    // ---- the token composed with kept tenures (650acc8) ----

    fn grant_ids(h: &Harness) -> Vec<GrantId> {
        let mut ids: Vec<GrantId> = h
            .meta
            .locks()
            .grants_snapshot()
            .iter()
            .map(|g| g.id)
            .collect();
        ids.sort();
        ids
    }

    /// The close of a continuation epoch that `holder_with_grants_in_an_epoch`
    /// opened, as the driver reports it once S3 is back, then the
    /// re-claim (brought by a blocking lock on a fresh inode) replacing
    /// `object`, the lease the close let go.
    fn close_and_reclaim(h: &mut Harness, members: &[NodeId], object: &mut Lease) {
        let carried = h.core.pr.carried;
        h.step(epoch_report(false, false, false, members, carried));
        reclaim(h, members, object);
    }

    fn reclaim(h: &mut Harness, members: &[NodeId], object: &mut Lease) {
        assert!(h.core.epoch_reclaim_pending(h.now));
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = h.step(Event::Control {
            op: OpId(1 << 30),
            req: Control::Lock {
                ino: fresh,
                mode: X,
                blocking: true,
            },
        });
        epoch_pump(h, members, object, out);
        assert_eq!(object.holder, h.core.node_id());
        assert!(
            h.core.pr.closed_tenure.is_empty(),
            "settled by the re-claim"
        );
    }

    /// Review (lock-fence-token × 650acc8): a fencing token survives a
    /// kept tenure. The grants keep their ids across a continuation
    /// epoch's close and the re-claim, so an op tagged before the close —
    /// a peer's forward, the holder's own op — still passes at the minter
    /// after the re-claim while its grant is live in the kept table, even
    /// past the window it set out with. Where another tenure intervened
    /// the table is dropped, and the same token is judged by its window.
    #[test]
    fn a_token_taken_before_an_epoch_close_passes_after_the_reclaim() {
        let (mut h, ino, peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        let own = h.meta.locks().held(ino).unwrap().id;
        let before = grant_ids(&h);
        let peer_tag = token(peer_grant, h.now.0 + 100);
        let own_tag = token(own, h.now.0 + 100);
        h.advance(500);
        close_and_reclaim(&mut h, &[1, 2], &mut object);
        let after = grant_ids(&h);
        assert!(
            before.iter().all(|id| after.contains(id)),
            "the same grant ids: {before:?} → {after:?}"
        );
        assert!(h.now.0 > peer_tag.until_ms(), "past the tokens' windows");
        let out = forward_tagged(
            &mut h,
            2,
            40,
            peer_rid(2, 1),
            setattr_mtime(peer_ino, 2),
            peer_tag.clone(),
        );
        assert!(
            matches!(mutate_reply(&out, 2, 40), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, peer_ino), 2);
        let rid = h.rid(1);
        let out = h.step(Event::Submit {
            rid,
            op: setattr_mtime(ino, 1),
            policy: Policy::Client,
            tag: own_tag,
        });
        assert!(
            !replies(&out)
                .iter()
                .any(|(_, r)| matches!(r, ClientReply::Outcome(MutateOutcome::LockLapsed))),
            "{out:?}"
        );
        assert_eq!(mtime(&h, ino), 1);
        assert_eq!(h.meta.locks().stats().token_rejections, 0);

        // Another tenure intervened (node 2 held the lease in between):
        // the kept table is dropped, and the token, past its window, is
        // refused.
        let (mut h, _ino, peer_ino, peer_grant, object) = holder_with_grants_in_an_epoch();
        let peer_tag = token(peer_grant, h.now.0 + 100);
        h.advance(500);
        let carried = h.core.pr.carried;
        h.step(epoch_report(false, false, false, &[1, 2], carried));
        let mut other = lease_of(2, object.epoch + 1, h.now.0 + 5_000);
        other.released = true;
        let fresh = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = h.step(Event::Control {
            op: OpId(9),
            req: Control::Lock {
                ino: fresh,
                mode: X,
                blocking: true,
            },
        });
        epoch_pump(&mut h, &[1, 2], &mut other, out);
        assert_eq!(h.meta.locks().grants_len(), 0, "a tenure intervened");
        let out = forward_tagged(
            &mut h,
            2,
            41,
            peer_rid(2, 1),
            setattr_mtime(peer_ino, 2),
            peer_tag,
        );
        assert_eq!(mutate_reply(&out, 2, 41), MutateOutcome::LockLapsed);
        assert_ne!(mtime(&h, peer_ino), 2);
    }

    /// Review (lock-fence-token × 650acc8): the release ordering against
    /// the one-shot `keep_grants`. The holder's own grant is recalled
    /// (node 3 waits for it) while a write tagged with it is in flight,
    /// so its release waits — through a continuation epoch's close that
    /// keeps the grant table. Once the write is answered, inside the
    /// re-claim window, the release ends the grant in the kept table (it
    /// was dropped there, and the grant outwaited before node 3 was
    /// served); the peer's grant stands through the re-claim; and the
    /// close consumed its keep: a real lease loss afterwards drops the
    /// table.
    #[test]
    fn a_release_waiting_through_a_kept_close_ends_its_grant_in_the_kept_table() {
        let (mut h, ino, _peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        let own = h.meta.locks().held(ino).unwrap();
        let own_record = h.meta.locks().get(own.id).unwrap().until_ms;
        let tag = token(own.id, own.until_ms);
        let out = request(&mut h, 3, 30, ino, X, true);
        assert!(lock_replies(&out).is_empty(), "parked: {out:?}");
        assert!(h.meta.locks().held(ino).unwrap().recalled);
        h.meta.locks().tag_begin(&tag, h.now.0);
        assert!(h.meta.locks().local_unlock(ino, 9, 0, u64::MAX, h.now.0));
        let out = h.step(Event::Control {
            op: OpId(6),
            req: Control::LockIdle { ino },
        });
        let flush = out
            .iter()
            .find_map(|a| match a {
                Action::LockFlush { ino: i, grant } if *i == ino => Some(*grant),
                _ => None,
            })
            .expect("a flush before the release");
        let out = h.step(Event::LockFlushed {
            ino,
            grant: flush,
            ok: true,
        });
        let wait = timer_of(&out, TimerKind::LockReleaseWait);
        assert!(h.meta.locks().held(ino).is_some(), "the release waits");
        // S3 is back: the close keeps the table; the waiter is told to wait.
        let carried = h.core.pr.carried;
        let out = h.step(epoch_report(false, false, false, &[1, 2, 3], carried));
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::Waiting { .. })]
            ),
            "{out:?}"
        );
        assert_eq!(h.meta.locks().grants_len(), 2, "kept for the re-claim");
        // The fallback poll in the window: the write still pins it.
        h.advance(10);
        let out = h.step(Event::Timer { id: wait });
        timer_of(&out, TimerKind::LockReleaseWait);
        assert!(h.meta.locks().held(ino).is_some());
        // The write is answered: the release ends the grant here.
        h.meta.locks().tag_end(&tag, false, h.now.0);
        h.step(Event::Control {
            op: OpId(7),
            req: Control::LockReleaseWake { ino },
        });
        assert!(h.meta.locks().held(ino).is_none());
        assert!(
            h.meta.locks().get(own.id).is_none(),
            "ended in the kept table, not left to be outwaited"
        );
        assert!(h.meta.locks().check_tag(&tag, h.now.0).is_err());
        assert_eq!(grant_ids(&h), vec![peer_grant]);
        reclaim(&mut h, &[1, 2, 3], &mut object);
        assert!(
            grant_ids(&h).contains(&peer_grant),
            "the peer's grant stands"
        );
        assert!(h.meta.locks().get(own.id).is_none());
        // Node 3 is served at once, long before the grant's record ran out.
        let out = request(&mut h, 3, 31, ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(31), LockOutcome::Granted { .. })]
            ),
            "{out:?}"
        );
        assert!(h.now.0 < own_record);
        // The keep was the close's alone: a real loss drops the table.
        h.core.lease.force_lost();
        let now = h.now;
        h.core.deleg_on_lease_gone(now, &h.meta, &mut Vec::new());
        assert_eq!(h.meta.locks().grants_len(), 0, "the keep leaked");
    }

    /// Review (lock-fence-token × 650acc8): in the re-claim window the
    /// owner answers from its kept table — a non-blocking request a kept
    /// grant conflicts with `WouldBlock`, any other `Waiting` (which
    /// grants nothing) — and a forward tagged with a kept grant is
    /// neither executed nor refused for its token there: no lease, no
    /// executor. Re-sent after the re-claim it passes (the grant is live
    /// in the kept table); once the grant's record has run out (nothing
    /// renewed it, and `Waiting` granted nothing over it) its token is
    /// refused.
    #[test]
    fn a_tagged_forward_in_the_reclaim_window_is_judged_after_the_reclaim() {
        let (mut h, _ino, peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        let record = h.meta.locks().get(peer_grant).unwrap().until_ms;
        let tag = token(peer_grant, h.now.0 + 100);
        let carried = h.core.pr.carried;
        h.step(epoch_report(false, false, false, &[1, 2, 3], carried));
        assert!(h.core.epoch_reclaim_pending(h.now));
        let out = request(&mut h, 3, 30, peer_ino, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::WouldBlock)]
            ),
            "{out:?}"
        );
        let other = h.meta.allocate_ino(ROOT_INO).unwrap();
        let out = request(&mut h, 3, 31, other, X, false);
        assert!(
            matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(31), LockOutcome::Waiting { .. })]
            ),
            "{out:?}"
        );
        let rid = peer_rid(2, 1);
        let out = forward_tagged(&mut h, 2, 40, rid, setattr_mtime(peer_ino, 2), tag.clone());
        let early = sends(&out).into_iter().find_map(|(t, m)| match m {
            PeerMsg::MutateReply { req, outcome, .. } if t == 2 && *req == OpId(40) => {
                Some(outcome.clone())
            }
            _ => None,
        });
        assert!(matches!(early, Some(MutateOutcome::Held { .. })), "{out:?}");
        assert_ne!(mtime(&h, peer_ino), 2);
        assert_eq!(h.meta.locks().stats().token_rejections, 0);
        // The forward itself brings the re-claim (`Held`: re-sent).
        h.advance(500);
        epoch_pump(&mut h, &[1, 2, 3], &mut object, out);
        assert_eq!(object.holder, 1);
        assert!(
            h.core.pr.closed_tenure.is_empty(),
            "settled by the re-claim"
        );
        let out = forward_tagged(&mut h, 2, 41, rid, setattr_mtime(peer_ino, 2), tag);
        assert!(
            matches!(mutate_reply(&out, 2, 41), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, peer_ino), 2);
        h.now = Ms(record);
        h.hold(object.epoch, None);
        let out = forward_tagged(
            &mut h,
            2,
            42,
            peer_rid(2, 2),
            setattr_mtime(peer_ino, 3),
            token(peer_grant, record + 1_000),
        );
        assert_eq!(mutate_reply(&out, 2, 42), MutateOutcome::LockLapsed);
        assert_eq!(mtime(&h, peer_ino), 2);
    }

    /// Review (lock-fence-token × 650acc8): the restart horizon persisted
    /// on renewals covers kept grants. A peer's grant kept through a
    /// close and renewed after the re-claim moves the horizon to its new
    /// record, so a restart there grants nothing over it until then —
    /// and a token naming it, which the restarted node judges by its
    /// window alone (its table is gone), can no longer be inside its
    /// window once a conflicting grant is possible.
    #[test]
    fn a_kept_grants_renewal_after_the_reclaim_moves_the_restart_horizon() {
        let (mut h, _ino, peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        h.advance(500);
        close_and_reclaim(&mut h, &[1, 2], &mut object);
        h.advance(2_000);
        let sent = h.now;
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(70),
                entries: vec![LockRenewEntry {
                    ino: peer_ino,
                    grant: peer_grant,
                    mode: X,
                }],
            },
        });
        let ttl = sends(&out)
            .into_iter()
            .find_map(|(to, m)| match m {
                PeerMsg::LockRenewed { results, .. } if to == 2 => match results.as_slice() {
                    [(_, id, LockRenewResult::Ok { ttl_ms, .. })] if *id == peer_grant => {
                        Some(*ttl_ms as i64)
                    }
                    _ => None,
                },
                _ => None,
            })
            .unwrap_or_else(|| panic!("the kept grant renewed: {out:?}"));
        let record = h.meta.locks().get(peer_grant).unwrap().until_ms;
        let window = sent.0 + ttl - h.core.lock_margin_ms();
        // The restart.
        let cfg = h.core.cfg.clone();
        h.core = Core::new(cfg);
        let now = h.now;
        h.core.start(now, &h.meta, &mut Vec::new());
        let horizon = h
            .meta
            .load_lock_quarantine(h.now.0)
            .expect("quarantined by the renewal");
        assert!(horizon >= record, "{horizon} < {record}");
        assert!(horizon >= window + 2 * h.core.lock_margin_ms());
        h.hold(object.epoch + 1, None);
        let out = request(&mut h, 3, 30, peer_ino, X, false);
        assert!(
            !matches!(
                lock_replies(&out).as_slice(),
                [(3, OpId(30), LockOutcome::Granted { .. })]
            ),
            "granted over a renewed kept grant after a restart: {out:?}"
        );
    }

    /// Review round 2, should-fix 4 (composition with `lock-release-drop`):
    /// the re-claim's lease CAS ends in doubt and the acquisition re-reads
    /// the lease (`Phase::CasReread`). A forward tagged with a kept grant
    /// arriving then is answered `Held` as in the rest of the re-claim
    /// window: not executed, not refused for its token. Once the re-read
    /// finds the CAS landed, the re-sent forward passes.
    #[test]
    fn a_tagged_forward_in_the_cas_reread_window_is_held() {
        let (mut h, _ino, peer_ino, peer_grant, mut object) = holder_with_grants_in_an_epoch();
        let tag = token(peer_grant, h.now.0 + 100);
        let carried = h.core.pr.carried;
        h.step(epoch_report(false, false, false, &[1, 2, 3], carried));
        assert!(h.core.epoch_reclaim_pending(h.now));
        let rid = peer_rid(2, 1);
        // The forward brings the re-claim.
        let mut out = forward_tagged(&mut h, 2, 40, rid, setattr_mtime(peer_ino, 2), tag.clone());
        if let Some(id) = h.core.poll_timer {
            out.extend(h.step(Event::Timer { id }));
        }
        let mut reread = None;
        for _ in 0..400 {
            let mut next = Vec::new();
            for action in std::mem::take(&mut out) {
                match action {
                    Action::S3 { op, req } => {
                        let result = match req {
                            S3Op::LeaseGet if reread.is_none() => {
                                S3Result::LeaseGet(Ok(Some((object.clone(), super::tag()))))
                            }
                            S3Op::LeaseGet => {
                                reread = Some(op);
                                continue;
                            }
                            S3Op::LeaseSwap { lease, .. } | S3Op::LeaseCreate { lease } => {
                                // Lands, but the answer is a timeout.
                                object = lease;
                                reread = Some(OpId(0));
                                S3Result::LeasePut(Err(CasFailure::Failed("timeout".into())))
                            }
                            S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                            S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                            S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                            S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                            S3Op::InboxDrain { .. } => S3Result::InboxDrain(Ok(Vec::new())),
                            S3Op::HeartbeatRead => S3Result::Heartbeats(Ok(Vec::new())),
                            _ => continue,
                        };
                        next.extend(h.step(Event::S3 { op, result }));
                    }
                    Action::SetTimer {
                        id,
                        kind: TimerKind::Poll | TimerKind::LockRetry,
                        ..
                    } => next.extend(h.step(Event::Timer { id })),
                    Action::EpochClose => next.extend(h.step(epoch_report(
                        false,
                        false,
                        true,
                        &[1, 2, 3],
                        h.core.pr.carried,
                    ))),
                    Action::UploadDirtyChunks { op, .. } => {
                        next.extend(h.step(Event::UploadsDone {
                            op,
                            result: UploadResult::Done { held: 0 },
                        }))
                    }
                    _ => {}
                }
            }
            if reread.is_some_and(|op| op != OpId(0)) {
                break;
            }
            h.advance(10);
            out = next;
        }
        let reread = reread.filter(|op| *op != OpId(0)).expect("the CAS re-read");
        assert!(h.core.acquiring());
        let out = forward_tagged(&mut h, 2, 41, rid, setattr_mtime(peer_ino, 2), tag.clone());
        let held = sends(&out).into_iter().find_map(|(t, m)| match m {
            PeerMsg::MutateReply { req, outcome, .. } if t == 2 && *req == OpId(41) => {
                Some(outcome.clone())
            }
            _ => None,
        });
        assert!(matches!(held, Some(MutateOutcome::Held { .. })), "{out:?}");
        assert_ne!(mtime(&h, peer_ino), 2);
        assert_eq!(h.meta.locks().stats().token_rejections, 0);
        assert!(
            h.meta.locks().get(peer_grant).is_some(),
            "the kept grant stands"
        );
        // The re-read finds the CAS landed: won.
        let out = h.step(Event::S3 {
            op: reread,
            result: S3Result::LeaseGet(Ok(Some((object.clone(), super::tag())))),
        });
        epoch_pump(&mut h, &[1, 2, 3], &mut object, out);
        let out = forward_tagged(&mut h, 2, 42, rid, setattr_mtime(peer_ino, 2), tag);
        assert!(
            matches!(mutate_reply(&out, 2, 42), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, peer_ino), 2);
    }

    /// Review round 2, must-fix 2: a backup's lock mirror is asynchronous,
    /// so a successor can install a grant its predecessor had already
    /// ended. Node 2 held `g2` at node 1, and its op went into the S3
    /// inbox in doubt; node 2 waited the token's window out, `g2` ended at
    /// node 1 (released), node 3 was granted and wrote, and node 1 died
    /// before a mirror without `g2` reached node 4. Node 4 takes over with
    /// the stale mirror: `g2` is live in its table, restamped. The drained
    /// inbox op past its window is refused (`LockLapsed`), not passed as
    /// a live grant's op. The holder's renewal confirms `g2` only from
    /// then on: the old op is still refused, a new one passes.
    #[test]
    fn a_stale_mirrors_grant_does_not_pass_an_op_past_its_window() {
        let mut h = Harness::new(4);
        h.core.cfg.inbox = true;
        let op = h.create("f");
        constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
        let MutateOp::Create { ino, .. } = op else {
            unreachable!()
        };
        let ttl = h.core.lock_ttl_ms();
        let g2 = GrantId { node: 1, seq: 5 };
        let window2 = h.now.0 + ttl - 1_000;
        h.core.lease.cached_holder = Some(1);
        h.step(Event::Peer {
            from: 1,
            msg: PeerMsg::LockMirror {
                ver: 1,
                grants: vec![constellation_meta::locks::Grant {
                    id: g2,
                    node: 2,
                    ino,
                    mode: X,
                    until_ms: window2 + 2_000,
                    recalled: false,
                    gen: 0,
                    confirmed_ms: constellation_meta::locks::Grant::MINTED,
                }],
                floor: Position::ZERO,
            },
        });
        // Node 1 dies; node 4 takes over after the window.
        h.now = Ms(window2 + 500);
        h.hold(2, None);
        let now = h.now;
        h.core.lock_install_mirror(now, &h.meta);
        assert!(
            h.meta.locks().get(g2).is_some(),
            "installed from the mirror"
        );
        let before = mtime(&h, ino);
        let mut batch = super::inbox_batch(2, 0, 1, &setattr_mtime(ino, 2));
        batch.ops[0].lock_tag = token(g2, window2).to_wire();
        let mut out = Vec::new();
        h.core
            .inbox_drain(h.now, vec![batch], &h.meta, &mut out)
            .unwrap();
        assert_eq!(mtime(&h, ino), before, "the stale op did not execute");
        assert_eq!(h.meta.locks().stats().token_rejections, 1);
        // The holder renews `g2` here: confirmed from now on.
        h.advance(100);
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::LockRenew {
                req: OpId(30),
                entries: vec![LockRenewEntry {
                    ino,
                    grant: g2,
                    mode: X,
                }],
            },
        });
        let confirmed = h.meta.locks().get(g2).unwrap().confirmed_ms;
        assert_eq!(confirmed, h.now.0);
        let out = forward_tagged(
            &mut h,
            2,
            31,
            peer_rid(2, 2),
            setattr_mtime(ino, 3),
            token(g2, window2),
        );
        assert_eq!(mutate_reply(&out, 2, 31), MutateOutcome::LockLapsed);
        assert_eq!(mtime(&h, ino), before);
        // An op sent under the renewed window passes even once that
        // window is over: the grant is live here and confirmed.
        let renewed = h.now.0 + ttl - 1_000;
        h.now = Ms(renewed + 10);
        let out = forward_tagged(
            &mut h,
            2,
            32,
            peer_rid(2, 3),
            setattr_mtime(ino, 4),
            token(g2, renewed),
        );
        assert!(
            matches!(mutate_reply(&out, 2, 32), MutateOutcome::Accepted { .. }),
            "{out:?}"
        );
        assert_eq!(mtime(&h, ino), 4);
    }
}

// ---- M16 fixes ----

/// A root holding the lease with a directory `d1` on its replica.
fn root_with_dir() -> (Harness, constellation_fs_core::Ino) {
    let mut h = Harness::new(1);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
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
    h.hold(2, None);
    (h, dir)
}

/// `undelegate` refuses a designation (only `online` releases one): it
/// sends no recall, and answers at once with the reason.
#[test]
fn undelegate_refuses_a_designation() {
    let (mut h, dir) = root_with_dir();
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Delegate {
            dir,
            node: 2,
            gen: 1,
            designated: true,
            range: (0, 0),
        }],
    )
    .unwrap();
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let out = h.step(Event::Control {
        op: OpId(1 << 50),
        req: Control::Undelegate { dir },
    });
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, PeerMsg::DelegRecall { .. })),
        "a designation was recalled: {out:?}"
    );
    let done: Vec<_> = out
        .iter()
        .filter_map(|a| match a {
            Action::ControlDone { op, result } if *op == OpId(1 << 50) => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert!(
        matches!(done.as_slice(), [Err(why)] if why.contains("designated")),
        "{done:?}"
    );
}

/// `startup-link-lag`: the root delegates `d1` to node 3, and node 2's
/// write into it reaches the root (node 2 had no link to 3 yet). The
/// root redirects it to the delegate only if its own links show 3
/// connected; with the peer enrolled but its link not reported yet (the
/// driver used to report links on a 1 s tick only), the root recalls the
/// fresh generation and executes the write itself.
#[test]
fn a_write_into_a_fresh_delegation_is_recalled_until_the_root_sees_the_delegate() {
    fn delegate_then_forward(c_connected: bool) -> Vec<Action> {
        let (mut h, dir) = root_with_dir();
        let out = h.step(Event::Control {
            op: OpId(900),
            req: Control::Delegate {
                dir,
                node: 3,
                range: (0, 0),
            },
        });
        assert!(
            out.iter().any(|a| matches!(
                a,
                Action::ControlDone {
                    op: OpId(900),
                    result: Ok(_)
                }
            )),
            "not delegated: {out:?}"
        );
        if c_connected {
            h.step(Event::Peers {
                links: (2..=4)
                    .map(|node| crate::event::PeerLink {
                        node,
                        connected: true,
                        last_seen: Some(h.now),
                        rtt_ms: Some(1),
                        since: Some(h.now),
                    })
                    .collect(),
            });
        }
        h.advance(10);
        let ino = h.meta.allocate_ino(ROOT_INO).unwrap();
        h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(7),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 1,
                },
                op: MutateOp::Create {
                    parent: dir,
                    name: "b-0".into(),
                    ino,
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                },
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
                applied: 0,
                tag: Default::default(),
            },
        })
    }
    let recalled = |out: &[Action]| {
        sends(out)
            .iter()
            .any(|(to, m)| *to == 3 && matches!(m, PeerMsg::DelegRecall { .. }))
    };
    let redirected = |out: &[Action]| {
        sends(out).iter().any(|(to, m)| {
            *to == 2
                && matches!(
                    m,
                    PeerMsg::MutateReply {
                        outcome: MutateOutcome::NotHolder { holder: 3 },
                        ..
                    }
                )
        })
    };
    let out = delegate_then_forward(false);
    assert!(
        recalled(&out) && !redirected(&out),
        "the root, its link to 3 not reported yet, recalls: {out:?}"
    );
    let out = delegate_then_forward(true);
    assert!(
        redirected(&out) && !recalled(&out),
        "the root, its link to 3 up, redirects to the delegate: {out:?}"
    );
}

/// `fs set epoch-slack` raising `f` from 0 on a mounted node whose
/// promise TTL exceeds lease TTL / 4 (never validated at mount, since
/// `f` was 0 then): the core clamps the TTL instead of promising for
/// longer than the rule allows.
#[test]
fn raising_the_slack_at_runtime_clamps_an_invalid_promise_ttl() {
    let mut h = Harness::new(1);
    h.core.cfg.ttl_ms = 10_000;
    h.core.cfg.promise_ttl_ms = 9_000;
    h.step(Event::Slack { epoch_slack: 0 });
    assert_eq!(h.core.cfg.promise_ttl_ms, 9_000, "unused at f = 0");
    h.step(Event::Slack { epoch_slack: 1 });
    assert_eq!(h.core.cfg.epoch_slack, 1);
    assert_eq!(h.core.cfg.promise_ttl_ms, 2_500, "clamped to lease TTL / 4");
    // A valid TTL is left alone.
    h.core.cfg.promise_ttl_ms = 2_000;
    h.step(Event::Slack { epoch_slack: 1 });
    assert_eq!(h.core.cfg.promise_ttl_ms, 2_000);
}

fn inbox_batch(
    node: NodeId,
    n: u64,
    seq: u64,
    op: &MutateOp,
) -> constellation_store_s3::inbox::InboxBatch {
    use constellation_store_s3::inbox::{InboxBatch, InboxOp, InboxRid};
    InboxBatch {
        epoch: 1,
        node,
        incarnation: 1,
        n,
        submitted_unix_ms: 0,
        ops: vec![InboxOp {
            rid: InboxRid {
                node,
                incarnation: 1,
                seq,
            },
            op: op.to_postcard().unwrap(),
            deps: Vec::new(),
            lock_tag: Vec::new(),
        }],
        wants_lease: false,
    }
}

fn inbox_deletes(out: &[Action]) -> Vec<(u64, u64, u64)> {
    s3_ops(out)
        .into_iter()
        .filter_map(|(_, r)| match r {
            S3Op::InboxDelete { key } => Some((key.epoch, key.node, key.n)),
            _ => None,
        })
        .collect()
}

/// M16: the takeover drain used to stop at the first op under a live
/// delegation, leaving every later batch — other requesters' included —
/// in older-epoch slots the new holder never polls (a crashed
/// requester's until the next takeover). Now only that requester is
/// deferred (its batches stay in order), the others drain, and the
/// deferred batches run from the holder tick once the delegation is
/// gone. `inbox_drained_batches` counts only what actually executed.
#[test]
fn the_takeover_drain_defers_only_the_delegated_requester() {
    let (mut h, dir) = root_with_dir();
    h.core.cfg.inbox = true;
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Delegate {
            dir,
            node: 2,
            gen: 1,
            designated: false,
            range: (0, 0),
        }],
    )
    .unwrap();
    // Requester 2 writes under the delegated directory (twice: its second
    // batch must wait behind its first); requester 3 in the root.
    let under = |h: &Harness, name: &str| MutateOp::Create {
        parent: dir,
        name: name.into(),
        ino: h.meta.allocate_ino(dir).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let a = under(&h, "a");
    let b = h.create("b");
    let c = h.create("c");
    let batches = vec![
        inbox_batch(2, 0, 1, &a),
        inbox_batch(2, 1, 2, &b),
        inbox_batch(3, 0, 1, &c),
    ];
    let mut out = Vec::new();
    h.core
        .inbox_drain(h.now, batches, &h.meta, &mut out)
        .unwrap();
    assert!(
        h.meta.child_ino(ROOT_INO, "c").unwrap().is_some(),
        "requester 3's batch drained"
    );
    assert!(h.meta.child_ino(dir, "a").unwrap().is_none());
    assert!(
        h.meta.child_ino(ROOT_INO, "b").unwrap().is_none(),
        "requester 2's later batch waits behind its first"
    );
    assert_eq!(inbox_deletes(&out), vec![(1, 3, 0)]);
    assert_eq!(h.core.stats.inbox_drained_batches, 1);
    assert_eq!(h.core.inbox.leftover.len(), 2);
    // The delegation ends (recalled through the log): the holder tick
    // runs requester 2's batches, in order, and deletes them.
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        2,
        0,
        &[],
        &[],
        &[LogRecord::Recall { dir, gen: 1 }],
    )
    .unwrap();
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    assert!(h.meta.child_ino(dir, "a").unwrap().is_some());
    assert!(h.meta.child_ino(ROOT_INO, "b").unwrap().is_some());
    assert_eq!(inbox_deletes(&out), vec![(1, 2, 0), (1, 2, 1)]);
    assert_eq!(h.core.stats.inbox_drained_batches, 3);
    assert!(h.core.inbox.leftover.is_empty());
}

/// M16: an inbox op carries its `deps` like a forward: one that depends
/// on a delegate's stream the holder does not have yet (a marker written
/// after data the delegate acknowledged, whose stream has not reached a
/// new root) is deferred by the takeover drain and waits after it, and
/// runs once the stream is here.
#[test]
fn an_inbox_op_waits_for_its_deps() {
    let (mut h, dir) = root_with_dir();
    h.core.cfg.inbox = true;
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Delegate {
            dir,
            node: 2,
            gen: 1,
            designated: false,
            range: (0, 0),
        }],
    )
    .unwrap();
    // Requester 2 (the delegate) wrote data under `dir` (stream index 5)
    // and then a marker in the root, through the inbox.
    let marker = h.create("marker");
    let mut batch = inbox_batch(2, 0, 7, &marker);
    batch.ops[0].deps = constellation_meta::Position::ZERO
        .with_streams_wire(&[(1, 5)])
        .to_postcard();
    let mut out = Vec::new();
    h.core
        .inbox_drain(h.now, vec![batch], &h.meta, &mut out)
        .unwrap();
    assert!(h.meta.child_ino(ROOT_INO, "marker").unwrap().is_none());
    assert_eq!(h.core.inbox.leftover.len(), 1, "deferred by the drain");
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    assert!(
        h.meta.child_ino(ROOT_INO, "marker").unwrap().is_none(),
        "still waiting for the delegate's stream"
    );
    // The delegate's stream through index 5 is here: it runs.
    crate::replica::Replica::note_stream(&h.meta, 1, 5);
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    assert!(h.meta.child_ino(ROOT_INO, "marker").unwrap().is_some());
    assert_eq!(inbox_deletes(&out), vec![(1, 2, 0)]);
}

// ---- chaos-soak-4 seed 42 (`wf293`): a delegate's reply base ----

/// A delegate's reply `base` must cover everything its replica held when
/// it evaluated the op — also what it applied *before* the grant
/// installed (a segment of the root's, applied while this node had no
/// delegation, is not in its shipped-touch window), and what it holds
/// only as speculation ahead of the log (the root's pre-S3 stream). A
/// base below either let the requester install the accepted reply as a
/// shadow on a replica that lacked the earlier write: the write then
/// landed on top of the shadow, and when the stream's copy of the
/// requester's own op adopted the shadow in place the replica kept the
/// earlier write for good (the requester served another node's
/// whole-file write while every other replica served its own).
#[test]
fn a_delegate_reply_base_covers_what_it_applied_before_the_grant_and_streamed_state() {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.cfg.forwarding = true;
    h.core.lease.cached_holder = Some(1);
    let dir = h.meta.allocate_ino(ROOT_INO).unwrap();
    let apply = |h: &Harness, seq: u64, recs: &[LogRecord]| {
        crate::replica::Replica::apply_segment(&h.meta, seq, 1, 0, &[], &[], recs).unwrap();
    };
    apply(
        &h,
        1,
        &[LogRecord::Mkdir {
            parent: ROOT_INO,
            name: "d1".into(),
            ino: dir,
            mode: 0o755,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }],
    );
    // The root creates `d1/f` for another node, before the grant.
    let f = (1u64 << 40) | 77;
    apply(
        &h,
        2,
        &[LogRecord::Create {
            parent: dir,
            name: "f".into(),
            ino: f,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 2,
        }],
    );
    apply(
        &h,
        3,
        &[LogRecord::Delegate {
            dir,
            node: 3,
            gen: 1,
            designated: true,
            range: (0, 0),
        }],
    );
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    assert_eq!(h.core.deleg_view().mine.len(), 1);
    // The root's stream carries a create of `d1/g` ahead of the log.
    let g = (1u64 << 40) | 78;
    h.meta
        .install_streamed(
            1,
            1,
            5,
            5,
            &[LogRecord::Create {
                parent: dir,
                name: "g".into(),
                ino: g,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 3,
            }],
        )
        .unwrap();
    let mut ask = |name: &str, req: u64| {
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(req),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: req,
                },
                op: MutateOp::Unlink {
                    parent: dir,
                    name: name.into(),
                },
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
                applied: 0,
                tag: Default::default(),
            },
        });
        match sends(&out).first().map(|(_, m)| *m) {
            Some(PeerMsg::MutateReply { base, outcome, .. }) => {
                assert!(
                    matches!(outcome, MutateOutcome::Accepted { .. }),
                    "{outcome:?}"
                );
                *base
            }
            other => panic!("{other:?}: {out:?}"),
        }
    };
    let base_f = ask("f", 1);
    assert!(
        base_f.is_some_and(|b| b >= 2),
        "the base {base_f:?} is below the segment that created d1/f"
    );
    assert_eq!(
        ask("g", 2),
        None,
        "d1/g is streamed speculation here: only the log can order the reply"
    );
    assert_eq!(h.core.stats.deleg_executed, 2);
}
/// EC2 finding 2: a holder whose wanter is across a P2P partition from
/// the nodes using the lease keeps it (the wanter is served through the
/// inbox); with no P2P demand of its own side — an isolated holder, or
/// M13's no-P2P shape — it hands over after the wanted grace as before.
#[test]
fn a_wanter_across_a_p2p_partition_does_not_take_the_lease_from_its_users() {
    fn links(h: &mut Harness, connected: &[NodeId]) {
        h.step(Event::Peers {
            links: (2..=4)
                .map(|node| crate::event::PeerLink {
                    node,
                    connected: connected.contains(&node),
                    last_seen: None,
                    rtt_ms: None,
                    since: None,
                })
                .collect(),
        });
    }
    /// Run one round from the poll timer; whether it released the lease.
    fn round_releases(h: &mut Harness) -> bool {
        let mut out = Vec::new();
        h.core.start(h.now, &h.meta, &mut out);
        let Some(poll) = timers(&out, TimerKind::Poll).first().copied() else {
            return false;
        };
        let mut out = h.step(Event::Timer { id: poll });
        for _ in 0..8 {
            if let Some(op) = out.iter().find_map(|a| match a {
                Action::UploadDirtyChunks { op, .. } => Some(*op),
                _ => None,
            }) {
                out = h.step(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                });
                continue;
            }
            let ops: Vec<(OpId, S3Op)> = s3_ops(&out)
                .into_iter()
                .map(|(op, req)| (op, req.clone()))
                .collect();
            match ops.as_slice() {
                [(_, S3Op::LeaseSwap { lease, .. })] if lease.released => return true,
                // The round renews first.
                [(op, S3Op::LeaseSwap { .. })] => {
                    out = h.step(Event::S3 {
                        op: *op,
                        result: S3Result::LeasePut(Ok(tag())),
                    });
                }
                _ => return false,
            }
        }
        false
    }
    for (case, connected, p2p_users, keep) in [
        // Node 4 is cut off; 2 and 3 forward to us over P2P: keep.
        ("majority holder", &[2, 3][..], &[2, 3][..], true),
        // 2+2: holder and 2 vs 3 and 4 (both want): a tie keeps it.
        ("even split", &[2][..], &[2][..], true),
        // Nobody on our side used it over P2P: hand over (M13 as ever).
        ("no p2p demand", &[2, 3][..], &[][..], false),
        // An isolated holder: nobody reaches it, the wanter takes over.
        ("isolated holder", &[][..], &[][..], false),
    ] {
        let mut h = Harness::new(1);
        h.hold(1, None);
        links(&mut h, connected);
        let wanters: Vec<NodeId> = (2..=4).filter(|n| !connected.contains(n)).collect();
        // Past the dwell; the wanters asked (inbox batches with
        // `wants_lease`), and the near side forwarded, within the window.
        h.advance(6_000);
        for w in &wanters {
            h.core.note_demand(h.now, *w, false);
            h.core.lease.note_wanted(h.now, *w);
        }
        for u in p2p_users {
            h.core.note_demand(h.now, *u, true);
        }
        h.advance(h.core.config().wanted_grace_ms + 100);
        let released = round_releases(&mut h);
        assert_eq!(!released, keep, "{case}: released {released}");
        if keep {
            assert!(h.core.stats.leases_kept_for_p2p_side > 0, "{case}");
        }
    }
}

/// EC2 follow-up (d): a continuation epoch that carries no lease (the
/// claim resolution found the holder's claim stale) and then froze (a
/// member went missing) has no hold owner. Before, only a hold owner
/// probed a frozen epoch and only a holder closed one, so once S3 came
/// back nobody ever closed it: nothing shipped, and FUSE hung for good.
/// Now every member of such an epoch probes and, S3 back, closes it.
#[test]
fn a_frozen_epoch_carrying_no_lease_is_closed_once_s3_is_back() {
    let mut h = Harness::new(2);
    h.step(Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open: true,
            active: true,
            frozen: true,
            flushing: false,
            base: 0,
            members: vec![1, 2, 3, 4],
            carrier: None,
            stale_below: 2,
        },
    });
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let poll = timers(&out, TimerKind::Poll)[0];
    let out = h.step(Event::Timer { id: poll });
    let probe = s3_ops(&out)
        .into_iter()
        .find(|(_, r)| matches!(r, S3Op::SegmentRun { .. }))
        .map(|(op, _)| op)
        .unwrap_or_else(|| panic!("a frozen epoch with no hold owner is never probed: {out:?}"));
    let out = h.step(Event::S3 {
        op: probe,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    let upload = out
        .iter()
        .find_map(|a| match a {
            Action::UploadDirtyChunks { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap_or_else(|| panic!("S3 is back but the epoch is not flushed: {out:?}"));
    let out = h.step(Event::UploadsDone {
        op: upload,
        result: UploadResult::Done { held: 0 },
    });
    assert!(
        out.iter().any(|a| matches!(a, Action::EpochClose)),
        "the epoch is not closed: {out:?}"
    );
}

/// Flex-crash seed 16755: the hold owner closes its epoch with nothing to
/// flush. Members close only once the carried lease has moved in S3
/// (`epoch_carrier_checked`), so the owner re-claims it anyway — before,
/// it left the expired carried lease standing and every member waited for
/// good. The obligation is persisted with the ended hold and settles once
/// the lease object is not the carried one.
#[test]
fn a_hold_owner_with_nothing_to_flush_reclaims_the_carried_lease_at_the_close() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = crate::event::Carrier {
        node: 1,
        epoch: carried.epoch,
        expires_unix_ms: carried.expires_unix_ms,
    };
    let state = |open: bool, active: bool, flushing: bool| Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open,
            active,
            frozen: false,
            flushing,
            base: 0,
            members: vec![1, 2],
            carrier: Some(carrier),
            stale_below: 1,
        },
    };
    h.step(state(true, true, false));
    assert!(h.core.lease.epoch_held());
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let (mut closed, mut reclaimed) = (false, None);
    for _ in 0..200 {
        if reclaimed.is_some() {
            break;
        }
        let mut next = Vec::new();
        for action in std::mem::take(&mut out) {
            match action {
                Action::EpochClose => {
                    closed = true;
                    // The driver reports the closed epoch (flushing).
                    next.extend(h.step(state(false, false, true)));
                }
                Action::UploadDirtyChunks { op, .. } => next.extend(h.step(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                })),
                Action::S3 { op, req } => {
                    let result = match req {
                        S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                        S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                        S3Op::LeaseGet => S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
                        S3Op::LeaseSwap { lease, .. } => {
                            if closed && lease.holder == 1 && !lease.released {
                                reclaimed = Some(lease.clone());
                            }
                            S3Result::LeasePut(Ok(tag()))
                        }
                        _ => continue,
                    };
                    next.extend(h.step(Event::S3 { op, result }));
                }
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => {
                    next.extend(h.step(Event::Timer { id }));
                }
                _ => {}
            }
        }
        if next.is_empty() {
            break;
        }
        h.advance(20);
        out = next;
    }
    assert!(closed, "the epoch never closed");
    let lease = reclaimed.expect("the carried lease was not re-claimed at the close");
    assert_eq!(lease.epoch, carried.epoch);
    assert_ne!(
        lease.expires_unix_ms, carried.expires_unix_ms,
        "the re-claim moves the carried lease"
    );
    assert!(!h.core.epoch_reclaim_due(), "the obligation settled");
}

/// Flex-crash seed 16364: the holder acked an epoch claim of its lease,
/// was paused, and on resuming began its idle release before the
/// activation reached its core. The release CAS may land — a released
/// lease is anyone's without promises — so no hold may stand on it; and
/// adopting one anyway, the release's in-doubt give-up (S3 cut) wiped
/// it: nobody held the epoch, its members waited for the carried lease
/// to move, the carrier waited for itself, and no promise was ever given
/// again. Now the activation adopts nothing and records that this node
/// owes the move; once S3 is back it closes its epoch and re-claims the
/// lease its release did not free.
#[test]
fn an_epoch_carrying_a_lease_being_released_is_closed_and_reclaimed_by_its_carrier() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = crate::event::Carrier {
        node: 1,
        epoch: carried.epoch,
        expires_unix_ms: carried.expires_unix_ms,
    };
    let state = |open: bool, active: bool, frozen: bool, flushing: bool| Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open,
            active,
            frozen,
            flushing,
            base: 0,
            members: vec![1, 2],
            carrier: Some(carrier),
            stale_below: 1,
        },
    };
    // The idle release is in its final section when the activation lands.
    h.core.lease.releasing = true;
    h.step(state(true, true, false, false));
    assert!(
        !h.core.lease.epoch_held(),
        "an epoch hold adopted on a lease being released"
    );
    // The release CAS fails without an answer and the re-read fails too:
    // the lease is given up locally (`ReleaseReread { in_doubt: true }`).
    h.core.lease.released();
    // A re-report (a freeze and a thaw) adopts nothing either.
    h.step(state(true, false, true, false));
    h.step(state(true, true, false, false));
    assert!(!h.core.lease.epoch_held(), "the given-up lease re-adopted");
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let (mut closed, mut reclaimed) = (false, None);
    for _ in 0..200 {
        if reclaimed.is_some() {
            break;
        }
        let mut next = Vec::new();
        for action in std::mem::take(&mut out) {
            match action {
                Action::EpochClose => {
                    closed = true;
                    next.extend(h.step(state(false, false, false, true)));
                }
                Action::UploadDirtyChunks { op, .. } => next.extend(h.step(Event::UploadsDone {
                    op,
                    result: UploadResult::Done { held: 0 },
                })),
                Action::S3 { op, req } => {
                    let result = match req {
                        S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                        S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                        // The release never landed: the carried lease stands.
                        S3Op::LeaseGet => S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
                        S3Op::LeaseSwap { lease, .. } => {
                            if closed && lease.holder == 1 && !lease.released {
                                reclaimed = Some(lease.clone());
                            }
                            S3Result::LeasePut(Ok(tag()))
                        }
                        _ => continue,
                    };
                    next.extend(h.step(Event::S3 { op, result }));
                }
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => {
                    next.extend(h.step(Event::Timer { id }));
                }
                _ => {}
            }
        }
        if next.is_empty() {
            break;
        }
        h.advance(20);
        out = next;
    }
    assert!(closed, "the carrier never closed its epoch");
    let lease = reclaimed.expect("the carried lease was not re-claimed at the close");
    assert_eq!(lease.epoch, carried.epoch);
    assert_ne!(
        lease.expires_unix_ms, carried.expires_unix_ms,
        "the re-claim moves the carried lease"
    );
}

/// The `Control::Epoch` report for an epoch of members 1 and 2 carrying
/// `carrier`.
fn epoch_report(
    carrier: crate::event::Carrier,
    open: bool,
    active: bool,
    frozen: bool,
    flushing: bool,
) -> Event {
    Event::Control {
        op: OpId(1 << 40),
        req: Control::Epoch {
            open,
            active,
            frozen,
            flushing,
            base: 0,
            members: vec![1, 2],
            carrier: Some(carrier),
            stale_below: 1,
        },
    }
}

/// The holder serves an (epoch-less) handoff request from node 3: the
/// release CAS is in flight, `releasing` up. Its op.
fn handoff_release_in_flight(h: &mut Harness) -> OpId {
    let out = h.step(Event::Peer {
        from: 3,
        msg: PeerMsg::LeaseRequest {
            req: OpId(5),
            epoch_applied: None,
        },
    });
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
    assert!(h.core.lease.releasing);
    let (release, req) = s3_ops(&out)[0];
    assert!(matches!(req, S3Op::LeaseSwap { lease, .. } if lease.released));
    release
}

/// S3 back: run the polls until the epoch closes and the carried lease
/// is re-claimed, or nothing is left to do. Lease reads answer `object`.
/// Whether it closed, and the re-claim's lease.
fn epoch_probe_until_close(
    h: &mut Harness,
    carrier: crate::event::Carrier,
    object: Lease,
) -> (bool, Option<Lease>) {
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    let (mut closed, mut reclaimed) = (false, None);
    for _ in 0..200 {
        if reclaimed.is_some() {
            break;
        }
        let mut next = Vec::new();
        for action in std::mem::take(&mut out) {
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
                        S3Op::LeaseSwap { lease, .. } => {
                            if closed && lease.holder == 1 && !lease.released {
                                reclaimed = Some(lease.clone());
                            }
                            S3Result::LeasePut(Ok(tag()))
                        }
                        _ => continue,
                    };
                    next.extend(h.step(Event::S3 { op, result }));
                }
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => {
                    next.extend(h.step(Event::Timer { id }));
                }
                _ => {}
            }
        }
        if next.is_empty() {
            break;
        }
        h.advance(20);
        out = next;
    }
    (closed, reclaimed)
}

fn carrier_of(lease: &Lease) -> crate::event::Carrier {
    crate::event::Carrier {
        node: lease.holder,
        epoch: lease.epoch,
        expires_unix_ms: lease.expires_unix_ms,
    }
}

/// Review of flex-crash seed 16364's fix (must-fix 1): the move a
/// releasing carrier owes is its own record, dropped once it holds the
/// epoch after all. The release ended with the lease kept while the
/// epoch was frozen, the thaw adopted the hold on the kept lease, and
/// node 2 then took the hold over P2P. With the obligation left standing
/// (it was `hold_ended`'s "closed" flag, which a handoff keeps), node 1
/// closed the epoch at the heal while node 2 owned its hold and journal,
/// and re-claimed the lease beside it. Now node 1 waits for node 2, as
/// any member does.
#[test]
fn an_owed_epoch_move_is_dropped_once_the_hold_is_adopted_and_handed_off() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let release = handoff_release_in_flight(&mut h);
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(!h.core.lease.epoch_held());
    assert_eq!(
        h.core.pr.owes_move,
        Some((carried.epoch, carried.expires_unix_ms))
    );
    // Frozen, then the release CAS conflicts and the re-read finds the
    // lease standing: kept.
    h.step(epoch_report(carrier, true, false, true, false));
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Err(CasFailure::Conflict)),
    });
    let (reread, _) = s3_ops(&out)[0];
    h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
    });
    assert!(!h.core.lease.releasing);
    assert!(h.core.lease.held.is_some(), "the lease was kept");
    // The thaw adopts the hold on the kept lease: the move is not owed.
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(h.core.lease.epoch_held());
    assert_eq!(h.core.pr.owes_move, None);
    assert_eq!(h.meta.epoch_owes_move(), None);
    // Node 2 takes the hold over P2P.
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::LeaseRequest {
            req: OpId(6),
            epoch_applied: Some(h.core.ship.head_seq),
        },
    });
    assert!(
        matches!(
            sends(&out)[0].1,
            PeerMsg::LeaseHandoff {
                req: OpId(6),
                released: true,
                ..
            }
        ),
        "{out:?}"
    );
    assert!(!h.core.lease.epoch_held());
    assert_eq!(
        h.core.pr.hold_ended,
        Some((carried.epoch, carried.expires_unix_ms, false))
    );
    // The heal: the carried lease stands (node 2's flush re-claims it);
    // node 1 must not close the epoch, nor re-claim.
    let (closed, reclaimed) = epoch_probe_until_close(&mut h, carrier, carried.clone());
    assert!(!closed, "a member closed the epoch its peer holds");
    assert!(reclaimed.is_none());
}

/// Should-fix 2 of that review: the release a carrier was running at the
/// activation ends with the lease kept (here the CAS conflicts and the
/// re-read finds it standing) while the epoch is active: the node is the
/// epoch's holder after all and adopts the hold. Only owing the move,
/// nobody held the epoch, and writes stopped at the lease's expiry until
/// the heal.
#[test]
fn a_carrier_whose_release_ends_with_the_lease_kept_adopts_the_epoch_hold() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let release = handoff_release_in_flight(&mut h);
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(!h.core.lease.epoch_held());
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Err(CasFailure::Conflict)),
    });
    let (reread, _) = s3_ops(&out)[0];
    h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
    });
    assert!(h.core.lease.held.is_some(), "the lease was kept");
    assert!(
        h.core.lease.epoch_held(),
        "the kept carried lease's epoch has no holder"
    );
    assert_eq!(h.core.pr.owes_move, None);
    assert_eq!(h.meta.epoch_owes_move(), None);
}

/// Should-fix 1 of that review: the release landed before the activation
/// carrying the lease (acked before the release began) was delivered.
/// `lease.held` is empty, and the late adoption ("restarted into the open
/// epoch") took the hold on a released lease any node with S3 could
/// claim without promises. Now the activation carrying exactly the lease
/// this process released owes the move, a restart keeps that, and the
/// heal closes the epoch (the lease moved: no re-claim).
#[test]
fn an_activation_carrying_a_released_lease_owes_the_move_and_adopts_nothing() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let release = handoff_release_in_flight(&mut h);
    h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Ok(tag())),
    });
    assert!(h.core.lease.held.is_none(), "released");
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(
        !h.core.lease.epoch_held(),
        "an epoch hold adopted on a released lease"
    );
    assert_eq!(
        h.core.pr.owes_move,
        Some((carried.epoch, carried.expires_unix_ms))
    );
    assert_eq!(h.meta.epoch_owes_move(), h.core.pr.owes_move);
    // A restart into the open epoch adopts nothing either.
    let cfg = h.core.config().clone();
    h.core = Core::new(cfg);
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(
        !h.core.lease.epoch_held(),
        "a restart adopted the hold on a released lease"
    );
    // The heal: the lease object is the released one, so the epoch
    // closes and nothing is re-claimed.
    let (closed, reclaimed) = epoch_probe_until_close(&mut h, carrier, carried.released());
    assert!(closed, "the epoch never closed");
    assert!(reclaimed.is_none(), "a released lease re-claimed");
    assert_eq!(h.core.pr.owes_move, None, "the close settles the move");
}

/// Review round 3, should-fix 3 (a safety hole): the record that this
/// node let a lease go was memory-only. The release CAS landed, the
/// process died before its answer, and the restarted core adopted the
/// hold of an activation carrying that lease (the late adoption) while
/// any node with S3 could claim the released object without promises:
/// two authorities. The release record is durable before the CAS goes
/// out now, and a restart owes the move instead.
#[test]
fn a_restart_after_the_release_cas_went_out_owes_the_move_instead_of_holding() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let id = Some((carried.epoch, carried.expires_unix_ms));
    let _release = handoff_release_in_flight(&mut h);
    assert_eq!(
        h.meta.lease_released(),
        id,
        "the release record is not durable before the CAS"
    );
    // The CAS lands in S3; the process dies before its answer.
    let cfg = h.core.config().clone();
    h.core = Core::new(cfg);
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(
        !h.core.lease.epoch_held(),
        "a restart adopted the hold on a lease whose release went out"
    );
    assert_eq!(h.core.pr.owes_move, id, "the move is owed instead");
    // A freeze and a thaw adopt nothing either.
    h.step(epoch_report(carrier, true, false, true, false));
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(!h.core.lease.epoch_held());
    // The heal: the object is the released lease, the epoch closes, and
    // nothing is re-claimed.
    let (closed, reclaimed) = epoch_probe_until_close(&mut h, carrier, carried.released());
    assert!(closed, "the epoch never closed");
    assert!(reclaimed.is_none(), "a released lease re-claimed");
}

/// The release record is cleared once a lease is acquired (a new expiry:
/// no activation carrying the released lease matches it), and when the
/// release ends with the lease kept.
#[test]
fn the_release_record_is_cleared_by_a_kept_release_and_by_an_acquisition() {
    // Kept: the CAS conflicts and the re-read finds the lease standing.
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let release = handoff_release_in_flight(&mut h);
    assert!(h.meta.lease_released().is_some());
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Err(CasFailure::Conflict)),
    });
    let (reread, _) = s3_ops(&out)[0];
    h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
    });
    assert!(h.core.lease.held.is_some(), "the lease was kept");
    assert_eq!(
        h.meta.lease_released(),
        None,
        "a kept lease's record stands"
    );
    assert_eq!(h.core.pr.released, None);

    // Landed, a restart, then an acquisition of the released object.
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let release = handoff_release_in_flight(&mut h);
    h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Ok(tag())),
    });
    h.advance(1_000);
    let cfg = h.core.config().clone();
    h.core = Core::new(cfg);
    let mut out = Vec::new();
    h.core.start(h.now, &h.meta, &mut out);
    assert_eq!(
        h.core.pr.released,
        Some((carried.epoch, carried.expires_unix_ms))
    );
    let out = h.step(Event::Control {
        op: OpId(700),
        req: Control::Acquire,
    });
    let (get, _) = s3_ops(&out)[0];
    let mut out = h.step(Event::S3 {
        op: get,
        result: S3Result::LeaseGet(Ok(Some((carried.released(), tag())))),
    });
    for _ in 0..10 {
        let Some((op, req)) = s3_ops(&out).first().map(|(o, r)| (*o, (*r).clone())) else {
            break;
        };
        let result = match req {
            S3Op::LeaseSwap { .. } | S3Op::LeaseCreate { .. } => S3Result::LeasePut(Ok(tag())),
            S3Op::InboxDrain { .. } => S3Result::InboxDrain(Ok(Vec::new())),
            S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
            S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
            _ => break,
        };
        out = h.step(Event::S3 { op, result });
    }
    let held = h.core.lease.held.as_ref().expect("acquired").0.clone();
    assert_ne!(held.expires_unix_ms, carried.expires_unix_ms);
    assert_eq!(
        h.meta.lease_released(),
        None,
        "the acquisition left the record"
    );
    assert_eq!(h.core.pr.released, None);
}

/// Review round 3, should-fix 1: the carrier held the epoch normally, the
/// epoch froze, a non-member's request started a handoff, and the release
/// CAS failed in doubt with the re-read failing too. The give-up wiped
/// the lease and its hold, the hold's end settled the owed move, and at
/// the heal the carrier found its lease standing, owed nothing and waited
/// for itself: nobody closed the epoch. A thaw while the release ran also
/// recorded the move while the hold still stood. Now the give-up records
/// the move after the wipe, the hold's end leaves it, writes are refused
/// (`EROFS`) at once, and the heal closes the epoch and re-claims.
#[test]
fn a_held_epochs_release_given_up_in_doubt_owes_the_move_and_refuses_writes() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let id = Some((carried.epoch, carried.expires_unix_ms));
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(h.core.lease.epoch_held(), "the carrier holds the epoch");
    h.step(epoch_report(carrier, true, false, true, false));
    let release = handoff_release_in_flight(&mut h);
    // A thaw while the release runs: the hold stands, nothing is owed.
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(h.core.lease.epoch_held());
    assert_eq!(
        h.core.pr.owes_move, None,
        "a move owed under a standing hold"
    );
    // An op waiting for the lease meanwhile.
    let waiting = h.rid(1);
    let op = h.create("a");
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
    });
    let (reread, _) = s3_ops(&out)[0];
    h.step(Event::Submit {
        policy: Policy::Client,
        rid: waiting,
        op,
        tag: Default::default(),
    });
    assert!(
        h.core.clients().any(|(r, _)| r == waiting),
        "the op does not wait for the lease"
    );
    let out = h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Err(crate::event::S3Failure("timed out".into()))),
    });
    assert!(h.core.lease.held.is_none(), "given up");
    assert!(!h.core.lease.epoch_held());
    assert_eq!(h.core.pr.owes_move, id, "the give-up's move is not owed");
    assert_eq!(h.meta.epoch_owes_move(), id);
    let refused = |out: &[Action], rid: Rid| {
        replies(out).iter().any(|(r, reply)| {
            *r == rid
                && matches!(
                    reply,
                    ClientReply::Outcome(MutateOutcome::Errno(Code::ReadOnly))
                )
        })
    };
    assert!(
        refused(&out, waiting),
        "an op waits out its deadline in an epoch nobody holds: {out:?}"
    );
    // A write routed now is refused at once, with no S3 read.
    let late = h.rid(2);
    let op = h.create("b");
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: late,
        op,
        tag: Default::default(),
    });
    assert!(refused(&out, late), "{out:?}");
    assert!(s3_ops(&out).is_empty(), "{out:?}");
    // The heal: the release did not land, the carried lease stands; the
    // carrier closes its epoch and re-claims it.
    let (closed, reclaimed) = epoch_probe_until_close(&mut h, carrier, carried.clone());
    assert!(closed, "the carrier never closed its epoch");
    let lease = reclaimed.expect("the carried lease was not re-claimed at the close");
    assert_eq!(lease.epoch, carried.epoch);
    assert_eq!(h.core.pr.owes_move, None, "the close settles the move");
}

/// Review round 3, should-fix 2: an outage that begins during a release
/// (here the handoff's release CAS is in flight) claims the lease being
/// released. Claiming nothing formed an epoch that carried no lease and
/// refused every write (`EROFS`) until the heal, though the release then
/// failed and the node held a usable lease. Now the activation owes the
/// move, the release ending kept adopts the hold, and writes are served.
#[test]
fn an_outage_during_a_release_that_ends_kept_serves_writes() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let carried = h.core.lease.held.as_ref().unwrap().0.clone();
    let carrier = carrier_of(&carried);
    let release = handoff_release_in_flight(&mut h);
    let claim = h.core.epoch_claim_view(h.now).held;
    assert_eq!(
        claim.map(|l| (l.epoch, l.expires_unix_ms)),
        Some((carried.epoch, carried.expires_unix_ms)),
        "a lease being released is not claimed"
    );
    h.step(epoch_report(carrier, true, true, false, false));
    assert!(!h.core.lease.epoch_held());
    let out = h.step(Event::S3 {
        op: release,
        result: S3Result::LeasePut(Err(CasFailure::Conflict)),
    });
    let (reread, _) = s3_ops(&out)[0];
    h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((carried.clone(), tag())))),
    });
    assert!(
        h.core.lease.epoch_held(),
        "the kept lease's epoch has no holder"
    );
    assert!(!h.core.epoch_refuses_writes());
    let rid = h.rid(1);
    let op = h.create("a");
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    assert!(
        replies(&out).iter().any(|(r, reply)| *r == rid
            && matches!(reply, ClientReply::Outcome(MutateOutcome::Accepted { .. }))),
        "{out:?}"
    );
}

/// A member of an epoch carrying another member's lease, once S3 is back:
/// it reads the lease object and closes only when the carried lease is no
/// longer there (flex-crash seed 4309: "a segment past `base`" closed a
/// member whose `base` was below the holder's pre-outage segments, and a
/// third node took over the lease the paused holder still held as the
/// epoch's authority). While the carried lease stands it uploads its own
/// pending chunks and closes nothing (`continuation-epoch`: a write it
/// forwarded left its chunks here, and the holder's flush waits for them
/// before it publishes — each used to wait for the other for 60 s).
/// The same holds while the epoch is frozen.
#[test]
fn a_member_closes_once_the_carried_lease_has_moved_and_uploads_meanwhile() {
    // Frozen too (a member is missing): a frozen member used to skip
    // its probe, so it never learned the carrier had moved and stayed in
    // the epoch until the missing member returned
    // (`epoch-member-dies-with-chunk`'s B).
    for frozen in [false, true] {
        let open = |h: &mut Harness, expires: i64| {
            h.step(Event::Control {
                op: OpId(1 << 40),
                req: Control::Epoch {
                    open: true,
                    active: true,
                    frozen,
                    flushing: false,
                    base: 0,
                    members: vec![1, 2],
                    carrier: Some(crate::event::Carrier {
                        node: 1,
                        epoch: 1,
                        expires_unix_ms: expires,
                    }),
                    stale_below: 1,
                },
            });
            assert!(!h.core.lease.epoch_held(), "node 1 carries the epoch");
            let mut out = Vec::new();
            h.core.start(h.now, &h.meta, &mut out);
            let poll = timers(&out, TimerKind::Poll)[0];
            let out = h.step(Event::Timer { id: poll });
            let probe = s3_ops(&out)
                .into_iter()
                .find(|(_, r)| matches!(r, S3Op::SegmentRun { .. }))
                .map(|(op, _)| op)
                .unwrap_or_else(|| panic!("the open epoch is not probed: {out:?}"));
            let out = h.step(Event::S3 {
                op: probe,
                result: S3Result::SegmentRun(Ok(Vec::new())),
            });
            s3_ops(&out)
                .into_iter()
                .find(|(_, r)| matches!(r, S3Op::LeaseGet))
                .map(|(op, _)| op)
                .unwrap_or_else(|| panic!("S3 is back but the lease is not read: {out:?}"))
        };
        let upload_of = |out: &[Action]| {
            out.iter()
                .find_map(|a| match a {
                    Action::UploadDirtyChunks { op, complete, .. } => Some((*op, *complete)),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("no upload pass: {out:?}"))
        };
        // The carried lease still stands: upload, close nothing.
        let mut h = Harness::new(2);
        let expires = h.now.0 + 20_000;
        let get = open(&mut h, expires);
        let object = lease_of(1, 1, expires);
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((object, tag())))),
        });
        let (upload, complete) = upload_of(&out);
        assert!(!complete, "only this node's own chunks");
        let out = h.step(Event::UploadsDone {
            op: upload,
            result: UploadResult::Done { held: 0 },
        });
        assert!(
            !out.iter().any(|a| matches!(a, Action::EpochClose)),
            "closed while the carrier may still hold the epoch: {out:?}"
        );
        assert!(h.core.epoch.open, "the epoch stays open");
        // An earlier object of the carried holder and lease epoch: the
        // carrier claimed the object its re-claim CAS in doubt may have
        // written, and that CAS did not land (chunk epoch-liveness-gap).
        // The carried lease is not reached yet: close nothing. Released,
        // or at another lease epoch, it has moved.
        for (object, closes) in [
            (lease_of(1, 1, h.now.0 + 13_000), false),
            (
                Lease {
                    released: true,
                    ..lease_of(1, 1, h.now.0 + 13_000)
                },
                true,
            ),
            (lease_of(1, 0, h.now.0 + 13_000), true),
            (lease_of(2, 1, h.now.0 + 13_000), true),
        ] {
            let mut h = Harness::new(2);
            let expires = h.now.0 + 20_000;
            let get = open(&mut h, expires);
            let out = h.step(Event::S3 {
                op: get,
                result: S3Result::LeaseGet(Ok(Some((object.clone(), tag())))),
            });
            let (upload, complete) = upload_of(&out);
            assert_eq!(complete, closes, "{object:?}");
            let out = h.step(Event::UploadsDone {
                op: upload,
                result: UploadResult::Done { held: 0 },
            });
            assert_eq!(
                out.iter().any(|a| matches!(a, Action::EpochClose)),
                closes,
                "{object:?}: {out:?}"
            );
        }
        // The holder re-claimed it (a new expiry): the epoch is over.
        let mut h = Harness::new(2);
        let expires = h.now.0 + 20_000;
        let get = open(&mut h, expires);
        let object = lease_of(1, 1, expires + 7_000);
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((object, tag())))),
        });
        let (upload, complete) = upload_of(&out);
        assert!(complete, "the closing round's complete pass");
        let out = h.step(Event::UploadsDone {
            op: upload,
            result: UploadResult::Done { held: 0 },
        });
        assert!(
            out.iter().any(|a| matches!(a, Action::EpochClose)),
            "the epoch is not closed: {out:?}"
        );
    }
}

/// EC2 follow-up (e): a candidate backup that answers `sealed` for the
/// epoch held (a seal it persisted for it before) is not asked again for
/// that epoch — before, the next housekeeping tick brought it up again,
/// every ~100 ms, forever — and does not keep acknowledgements waiting
/// for a backup that cannot be had.
#[test]
fn a_candidate_that_sealed_the_epoch_is_not_reinvited() {
    let mut h = Harness::new(1);
    h.step(Event::Roster {
        write_eligible: vec![1, 2],
    });
    h.step(Event::Peers {
        links: vec![crate::event::PeerLink {
            node: 2,
            connected: true,
            last_seen: Some(h.now),
            rtt_ms: Some(1),
            since: Some(Ms(h.now.0 - 60_000)),
        }],
    });
    h.hold(1, None);
    let mut invited = 0;
    let mut sealed_answers = 0;
    for i in 0..20u64 {
        if let Some(id) = h.core.ack.tick_timer {
            h.step(Event::Timer { id });
        } else {
            h.step(Event::Control {
                op: OpId(900 + i),
                req: Control::Journaled,
            });
        }
        if h.core.ack.candidate == Some(2) {
            invited += 1;
            // Answer every append in flight: sealed.
            let reqs: Vec<OpId> = h.core.ack.peers[&2]
                .inflight
                .iter()
                .map(|e| e.req)
                .collect();
            for req in reqs {
                sealed_answers += 1;
                h.step(Event::Peer {
                    from: 2,
                    msg: PeerMsg::BackupAck {
                        req,
                        epoch: 1,
                        acked: 0,
                        sealed: true,
                    },
                });
            }
        }
        h.advance(200);
    }
    assert!(sealed_answers >= 1, "the candidate was never streamed to");
    assert_eq!(invited, 1, "re-invited a backup that sealed the epoch");
    assert_eq!(h.core.ack.eligible, Some(false), "still waiting for it");
}

/// EC2 follow-up (the 4-node `cto-strict-root` EIO): an op that went to
/// the holder's inbox because the holder was not yet reachable over P2P
/// (a node mounting with others learned the holder from the lease before
/// its link was up) is forwarded by rid once the link is up — the holder
/// polls only unconnected requesters' inboxes, so before, the op waited
/// out the inbox deadline (2×TTL) and answered EIO.
#[test]
fn an_inbox_op_is_forwarded_once_the_holder_is_reachable() {
    let mut h = Harness::new(1);
    let rid = h.rid(1);
    h.core.lease.cached_holder = Some(2);
    let mut out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: h.create("a"),
        tag: Default::default(),
    });
    // Into the inbox (as when the forward could not be sent).
    h.core.inbox_enqueue(h.now, rid, 1, &h.meta, &mut out);
    for _ in 0..6 {
        let ops: Vec<(OpId, S3Op)> = s3_ops(&out)
            .into_iter()
            .map(|(o, r)| (o, r.clone()))
            .collect();
        let mut next = Vec::new();
        for (op, req) in ops {
            let result = match req {
                S3Op::InboxLastN { .. } => S3Result::InboxLastN(Ok(None)),
                S3Op::InboxPut { .. } => S3Result::InboxPut(Ok(())),
                _ => continue,
            };
            next.extend(h.step(Event::S3 { op, result }));
        }
        if next.is_empty() {
            break;
        }
        out = next;
    }
    assert!(
        matches!(
            h.core.clients().next(),
            Some((_, ClientPhase::InboxWaiting))
        ),
        "{:?}",
        h.core.clients().collect::<Vec<_>>()
    );
    // The link to the holder comes up; the recheck reads the lease.
    h.step(Event::Peers {
        links: vec![crate::event::PeerLink {
            node: 2,
            connected: true,
            last_seen: Some(h.now),
            rtt_ms: Some(1),
            since: Some(h.now),
        }],
    });
    let id = h.core.inbox.recheck_timer.expect("a recheck armed");
    h.advance(1_100);
    let out = h.step(Event::Timer { id });
    let get = s3_ops(&out)
        .into_iter()
        .find(|(_, r)| matches!(r, S3Op::LeaseGet))
        .map(|(op, _)| op)
        .expect("the recheck reads the lease");
    let lease = Lease {
        v: 1,
        partition: "p0".into(),
        holder: 2,
        epoch: 1,
        expires_unix_ms: h.now.0 + 30_000,
        released: false,
        wanted_by: Vec::new(),
        backups: Vec::new(),
        config_version: 1,
        ack_policy: constellation_store_s3::AckPolicy::Local,
        granted_delegations: false,
        retired: Vec::new(),
    };
    let out = h.step(Event::S3 {
        op: get,
        result: S3Result::LeaseGet(Ok(Some((lease, tag())))),
    });
    // The batch is withdrawn first, then the op is forwarded.
    let (op, req) = s3_ops(&out)
        .into_iter()
        .find(|(_, r)| matches!(r, S3Op::InboxTombstone { .. }))
        .unwrap_or_else(|| panic!("the waiting op was not re-routed: {out:?}"));
    assert!(matches!(
        req,
        S3Op::InboxTombstone { batch } if batch.node == 1 && batch.is_tombstone()
    ));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::InboxTombstone(Ok(())),
    });
    assert!(
        matches!(sends(&out).first(), Some((2, PeerMsg::MutateRequest { rid: r, .. })) if *r == rid),
        "{out:?}"
    );
    assert_eq!(h.core.stats.inbox_rerouted_to_p2p, 1);
}

/// Gate run on 7dfc05b, harness `idle-cost`: an idle holder issued 96
/// inbox GETs in two minutes (budget 60 requests per minute in all).
/// Under host load one registry-tick ping round timed out, the peer
/// directory flagged every link down until the next probe, and the
/// holder tracked each requester afresh: in the hot window (25 GETs at
/// 20 ms) and from cursor zero, and again at the next flap. Now a new
/// requester is never hot (only a hit makes it so) and is warm only on
/// recent demand, cold otherwise; one whose link comes back keeps its
/// cursor, backoff and due time for the rest of the tenure, so a flapping
/// link never polls faster than a requester that stayed down all along.
#[test]
fn a_flapping_p2p_link_never_polls_an_idle_requester_faster_than_its_backoff() {
    fn links(h: &mut Harness, up: bool) {
        h.step(Event::Peers {
            links: (2..=4)
                .map(|node| crate::event::PeerLink {
                    node,
                    connected: up,
                    last_seen: None,
                    rtt_ms: None,
                    since: None,
                })
                .collect(),
        });
    }
    /// Two minutes, every link down for 5 s out of every 10 s (a ping
    /// round that times out, then the next probe that succeeds); every
    /// poll answered empty. The poll times per requester.
    fn flap(h: &mut Harness) -> BTreeMap<NodeId, Vec<Ms>> {
        let mut polls: BTreeMap<NodeId, Vec<Ms>> = BTreeMap::new();
        let mut t = 0;
        while t <= RUN_MS {
            if t % 1_000 == 0 {
                // The lease stays held (renewals are not the subject).
                if t % 5_000 == 0 {
                    h.hold(1, None);
                }
                links(h, (t / 5_000) % 2 == 1);
            }
            let mut out = Vec::new();
            h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
            while let Some((op, node)) = s3_ops(&out).into_iter().find_map(|(op, r)| match r {
                S3Op::InboxRun {
                    node, from, width, ..
                } => {
                    assert_eq!(*from, 0, "an idle requester's cursor never moves");
                    assert_eq!(*width, 1, "an idle poll is one GET");
                    Some((op, *node))
                }
                _ => None,
            }) {
                polls.entry(node).or_default().push(h.now);
                out.retain(|a| !matches!(a, Action::S3 { op: o, .. } if *o == op));
                let more = h.step(Event::S3 {
                    op,
                    result: S3Result::InboxRun(Ok(Vec::new())),
                });
                out.extend(more);
            }
            h.advance(STEP_MS);
            t += STEP_MS;
        }
        polls
    }
    const STEP_MS: u64 = 20;
    const RUN_MS: u64 = 120_000;

    // Nobody has written: every requester is polled once when first
    // seen down, then at the cold ceiling at most.
    let mut h = Harness::new(1);
    assert!(h.core.cfg.inbox && h.core.cfg.p2p);
    let (base, cold) = (h.core.cfg.sync_interval_ms, h.core.cfg.inbox_cold_max_ms);
    assert!(h.core.cfg.inbox_hot_ms < base);
    h.hold(1, None);
    let polls = flap(&mut h);
    assert_eq!(
        polls.len(),
        3,
        "every requester is polled while down: {polls:?}"
    );
    for (node, at) in &polls {
        let gaps: Vec<i64> = at.windows(2).map(|w| w[1].since(w[0])).collect();
        assert!(
            gaps.iter().all(|g| *g >= cold as i64),
            "idle requester {node} polled faster than the cold ceiling: {gaps:?}"
        );
        assert!(at.len() as u64 <= 1 + RUN_MS / cold, "{node}: {at:?}");
    }
    let total: usize = polls.values().map(Vec::len).sum();
    eprintln!("idle: {total} inbox polls in {RUN_MS} ms of flapping");
    // 3 requesters × (1 + 120 s / 10 s) at the very most, i.e. well under
    // `idle-cost`'s budget even with every link down all the time.
    assert!(total <= 39, "{total}");

    // Requester 2 wrote over P2P just before: it starts warm (never hot),
    // and still never polls faster than one that stayed down all along.
    let mut h = Harness::new(1);
    h.hold(1, None);
    h.core.note_demand(h.now, 2, true);
    let polls = flap(&mut h);
    let continuous = {
        let mut b = constellation_store_s3::inbox::PollBackoff::two_tier(
            base,
            h.core.cfg.inbox_warm_max_ms,
            cold,
        );
        let (mut at, mut n) = (0u64, 0u64);
        while at <= RUN_MS {
            n += 1;
            at += b.delay_ms();
            b.miss();
        }
        n
    };
    let at = &polls[&2];
    let gaps: Vec<i64> = at.windows(2).map(|w| w[1].since(w[0])).collect();
    assert!(
        gaps.first().is_some_and(|g| *g < cold as i64),
        "a requester with recent demand starts warm: {gaps:?}"
    );
    assert!(
        gaps.iter().all(|g| *g >= base as i64),
        "polled faster than the base interval (hot without a hit): {gaps:?}"
    );
    assert!(
        at.len() as u64 <= continuous,
        "{} polls in {RUN_MS} ms of flapping, more than the {continuous} of a requester down \
         the whole time",
        at.len()
    );
    assert!(
        polls[&3].len() as u64 <= 1 + RUN_MS / cold,
        "3 has no demand"
    );
}

/// An idle poll is one GET (the next batch); a hit is followed at once by
/// a full-width run, and the width stays full while the requester is hot.
#[test]
fn an_inbox_poll_is_one_get_until_it_hits() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    // Node 2 wrote over P2P a moment ago, and its link is down now.
    h.core.note_demand(h.now, 2, true);
    h.step(Event::Peers {
        links: vec![crate::event::PeerLink {
            node: 2,
            connected: false,
            last_seen: None,
            rtt_ms: None,
            since: None,
        }],
    });
    let full = h.core.cfg.inbox_poll_width;
    assert!(full > 1);
    let poll = |out: &[Action]| {
        s3_ops(out).into_iter().find_map(|(op, r)| match r {
            S3Op::InboxRun {
                node: 2,
                from,
                width,
                ..
            } => Some((op, *from, *width)),
            _ => None,
        })
    };
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    let (op, from, width) = poll(&out).expect("polled at once");
    assert_eq!((from, width), (0, 1));
    let a = h.create("a");
    let out = h.step(Event::S3 {
        op,
        result: S3Result::InboxRun(Ok(vec![inbox_batch(2, 0, 1, &a)])),
    });
    assert!(h.meta.child_ino(ROOT_INO, "a").unwrap().is_some());
    // Saturated at width 1: polled again at once, at full width.
    let mut out = out;
    if poll(&out).is_none() {
        h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    }
    let (op, from, width) = poll(&out).expect("a one-GET hit is followed at once");
    assert_eq!((from, width), (1, full));
    h.step(Event::S3 {
        op,
        result: S3Result::InboxRun(Ok(Vec::new())),
    });
    // Hot: the next poll is soon and still full width.
    h.advance(h.core.cfg.inbox_hot_ms);
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    let (_, from, width) = poll(&out).expect("hot after a hit");
    assert_eq!((from, width), (1, full));
}

/// Answer the requester's inbox numbering and batch PUTs in `out` until
/// nothing more is asked; every PUT's batch, in order.
fn land_inbox_puts(
    h: &mut Harness,
    mut out: Vec<Action>,
) -> (Vec<Action>, Vec<constellation_store_s3::inbox::InboxBatch>) {
    let mut puts = Vec::new();
    let mut rest = Vec::new();
    for _ in 0..16 {
        let ops: Vec<(OpId, S3Op)> = s3_ops(&out)
            .into_iter()
            .map(|(o, r)| (o, r.clone()))
            .collect();
        let mut next = Vec::new();
        for (op, req) in ops {
            let result = match req {
                S3Op::InboxLastN { .. } => S3Result::InboxLastN(Ok(None)),
                S3Op::InboxPut { batch } => {
                    puts.push(batch);
                    S3Result::InboxPut(Ok(()))
                }
                _ => continue,
            };
            next.extend(h.step(Event::S3 { op, result }));
        }
        rest.extend(out.into_iter().filter(|a| {
            !matches!(
                a,
                Action::S3 {
                    req: S3Op::InboxLastN { .. } | S3Op::InboxPut { .. },
                    ..
                }
            )
        }));
        if next.is_empty() {
            break;
        }
        out = next;
    }
    (rest, puts)
}

/// Withdraw hole: a requester withdrawing an inbox batch before a P2P
/// forward used to DELETE it, leaving a hole at its number; a holder that
/// had not read it GET-nexted the hole forever, and every later batch of
/// that requester in the epoch waited for the in-doubt deadline and the
/// lease path. Now the batch is overwritten with a tombstone (same key,
/// no ops), and the other ops it carried go back to the front of the
/// queue, into the next batch, under their rids.
#[test]
fn a_withdrawn_inbox_batch_leaves_a_tombstone_and_its_other_ops_are_resubmitted() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let (a, b, c) = (h.rid(1), h.rid(2), h.rid(3));
    let mut out = Vec::new();
    for (rid, name) in [(a, "a"), (b, "b")] {
        out.extend(h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op: h.create(name),
            tag: Default::default(),
        }));
        // Into the inbox (as when the forward could not be sent).
        h.core.inbox_enqueue(h.now, rid, 1, &h.meta, &mut out);
    }
    let (_, puts) = land_inbox_puts(&mut h, out);
    assert_eq!(puts.len(), 1, "a and b share one batch: {puts:?}");
    assert_eq!((puts[0].n, puts[0].ops.len()), (0, 2));
    let key0 = puts[0].key();

    // a finds a P2P path (or must be held back): withdrawn first.
    let mut out = Vec::new();
    h.core.send_forward(h.now, a, 2, &h.meta, &mut out);
    let (op, req) = s3_ops(&out)[0];
    match req {
        S3Op::InboxTombstone { batch } => {
            assert_eq!(batch.key(), key0);
            assert!(batch.is_tombstone(), "{batch:?}");
        }
        other => panic!("withdrawn by {other:?}, not a tombstone"),
    }
    assert!(
        s3_ops(&out)
            .iter()
            .all(|(_, r)| !matches!(r, S3Op::InboxDelete { .. })),
        "a withdrawal never deletes (a hole stops the holder's GET-next): {out:?}"
    );
    // Meanwhile c is queued behind the PUT that will carry b again.
    let out = h.step(Event::S3 {
        op,
        result: S3Result::InboxTombstone(Ok(())),
    });
    assert!(
        matches!(sends(&out).first(), Some((2, PeerMsg::MutateRequest { rid, .. })) if *rid == a),
        "a is forwarded once its batch is a tombstone: {out:?}"
    );
    let mut out = out;
    out.extend(h.step(Event::Submit {
        policy: Policy::Client,
        rid: c,
        op: h.create("c"),
        tag: Default::default(),
    }));
    h.core.inbox_enqueue(h.now, c, 1, &h.meta, &mut out);
    let (_, puts) = land_inbox_puts(&mut h, out);
    let rids: Vec<Vec<u64>> = puts
        .iter()
        .map(|p| p.ops.iter().map(|o| o.rid.seq).collect())
        .collect();
    assert_eq!(
        puts.first().map(|p| p.n),
        Some(1),
        "the next batch number, never the withdrawn one: {rids:?}"
    );
    assert_eq!(
        rids.concat(),
        vec![2, 3],
        "b is re-submitted, ahead of c, and a is not: {rids:?}"
    );
    let cb = h.core.clients.get(&b).unwrap();
    assert!(
        !cb.inbox_keys.contains(&key0),
        "b no longer waits on the tombstone: {:?}",
        cb.inbox_keys
    );
    assert_eq!(h.core.inbox.pending.get(&b).map(|k| k.n), Some(1));
    assert_eq!(h.core.stats.inbox_withdrawn_ops, 1);
    assert_eq!(h.core.stats.inbox_resubmitted_ops, 1);
}

/// The holder side of the withdraw hole: a tombstone is read, executes
/// nothing and is stepped past, so the requester's next batch runs at
/// once instead of waiting behind a key that will never fill.
#[test]
fn a_holder_steps_past_a_withdrawn_batch_to_the_requesters_next_one() {
    use constellation_store_s3::inbox::{InboxBatch, InboxKey};
    let mut h = Harness::new(1);
    h.hold(1, None);
    h.core.note_demand(h.now, 2, true);
    h.step(Event::Peers {
        links: vec![crate::event::PeerLink {
            node: 2,
            connected: false,
            last_seen: None,
            rtt_ms: None,
            since: None,
        }],
    });
    let poll = |out: &[Action]| {
        s3_ops(out).into_iter().find_map(|(op, r)| match r {
            S3Op::InboxRun { node: 2, from, .. } => Some((op, *from)),
            _ => None,
        })
    };
    let mut out = Vec::new();
    h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    let (op, from) = poll(&out).expect("polled");
    assert_eq!(from, 0);
    let tombstone = InboxBatch::tombstone(
        InboxKey {
            epoch: 1,
            node: 2,
            n: 0,
        },
        1,
        0,
    );
    let mut out = h.step(Event::S3 {
        op,
        result: S3Result::InboxRun(Ok(vec![tombstone])),
    });
    if poll(&out).is_none() {
        h.core.inbox_holder_tick(h.now, &h.meta, &mut out);
    }
    let (op, from) = poll(&out).expect("polled again at once past the tombstone");
    assert_eq!(from, 1, "the cursor steps past the withdrawn batch");
    let a = h.create("a");
    h.step(Event::S3 {
        op,
        result: S3Result::InboxRun(Ok(vec![inbox_batch(2, 1, 1, &a)])),
    });
    assert!(h.meta.child_ino(ROOT_INO, "a").unwrap().is_some());
    assert_eq!(h.core.stats.inbox_tombstones_read, 1);
    assert_eq!(h.core.stats.inbox_executed_ops, 1);
}

// ---- EC2 campaign 8 A-1: a node whose own S3 is stalled ----

/// Drive `rid`'s forward to the cached holder through its whole retry
/// budget (every attempt times out), returning the last event's actions.
fn exhaust_forward_retries(h: &mut Harness, first: &[Action]) -> Vec<Action> {
    let mut timeout = timers(first, TimerKind::ForwardTimeout)[0];
    for attempt in 1..=h.core.cfg.forward_retries {
        h.advance(h.core.cfg.forward_timeout_ms);
        let out = h.step(Event::Timer { id: timeout });
        let backoff = timers(&out, TimerKind::ForwardBackoff)[0];
        h.advance(h.core.cfg.forward_backoff_ms * u64::from(attempt));
        let out = h.step(Event::Timer { id: backoff });
        timeout = timers(&out, TimerKind::ForwardTimeout)[0];
    }
    h.advance(h.core.cfg.forward_timeout_ms);
    h.step(Event::Timer { id: timeout })
}

fn stalled(h: &mut Harness, peers_reach_s3: Option<bool>) -> Vec<Action> {
    h.step(Event::OwnS3 {
        stalled: true,
        peers_reach_s3,
    })
}

#[test]
fn a_node_with_its_s3_stalled_keeps_forwarding_past_the_retry_budget() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    stalled(&mut h, Some(true));
    let rid = h.rid(1);
    let op = h.create("a");
    let first = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    let out = exhaust_forward_retries(&mut h, &first);
    // Not the lease path (its lease read would wait on the dead S3 path):
    // another backoff and another forward of the same rid.
    assert!(s3_ops(&out).is_empty(), "{out:?}");
    assert_ne!(h.core.job(), Some(JobKind::Acquire));
    let mut backoff = timers(&out, TimerKind::ForwardBackoff)[0];
    for _ in 0..5 {
        h.advance(h.core.cfg.s3_less_retry_ms);
        let out = h.step(Event::Timer { id: backoff });
        let sent = sends(&out);
        assert!(
            matches!(sent.as_slice(), [(2, PeerMsg::MutateRequest { rid: r, .. })] if *r == rid),
            "{out:?}"
        );
        let timeout = timers(&out, TimerKind::ForwardTimeout)[0];
        h.advance(h.core.cfg.forward_timeout_ms);
        let out = h.step(Event::Timer { id: timeout });
        assert!(s3_ops(&out).is_empty(), "{out:?}");
        backoff = timers(&out, TimerKind::ForwardBackoff)[0];
    }
    // The one that ended the ordinary budget, and five more.
    assert_eq!(h.core.stats.s3_less_retries, 6);
    assert_eq!(h.core.stats.lease_path_taken, 0);
    // The holder answers at last.
    h.advance(h.core.cfg.s3_less_retry_ms);
    let out = h.step(Event::Timer { id: backoff });
    let req = match sends(&out).as_slice() {
        [(2, PeerMsg::MutateRequest { req, .. })] => *req,
        other => panic!("{other:?}"),
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateReply {
            req,
            outcome: MutateOutcome::Errno(Code::Exists),
            base: None,
            position: constellation_meta::Position::ZERO,
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
        },
    });
    assert!(
        matches!(replies(&out).as_slice(), [(r, ClientReply::Outcome(MutateOutcome::Errno(e)))] if *r == rid && *e == Code::Exists),
        "{out:?}"
    );
}

#[test]
fn a_stall_forwards_again_an_op_already_waiting_on_the_lease_path() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let rid = h.rid(1);
    let op = h.create("a");
    let first = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    // S3 not yet known stalled: the budget ends in the lease path.
    let out = exhaust_forward_retries(&mut h, &first);
    assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));
    assert_eq!(h.core.job(), Some(JobKind::Acquire));
    // The stall becomes known while the lease read hangs: forwarded again.
    h.advance(1_000);
    let out = stalled(&mut h, None);
    assert!(
        matches!(sends(&out).as_slice(), [(2, PeerMsg::MutateRequest { rid: r, .. })] if *r == rid),
        "{out:?}"
    );
    assert_eq!(h.core.stats.s3_less_forwards, 1);
    assert!(matches!(
        h.core.clients().next(),
        Some((_, ClientPhase::Forwarded))
    ));
}

#[test]
fn a_stalled_node_fails_an_unanswered_op_at_the_bound_unless_the_bucket_is_down() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    let rid = h.rid(1);
    let op = h.create("a");
    let first = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    exhaust_forward_retries(&mut h, &first);
    h.advance(h.core.cfg.s3_less_deadline_ms);
    // Every peer answered that it cannot reach S3 either: a bucket
    // outage, left to the continuation epoch and the ordinary deadline.
    let out = stalled(&mut h, Some(false));
    assert!(replies(&out).is_empty(), "{out:?}");
    assert_eq!(h.core.stats.s3_less_deadlines, 0);
    // Only this node lost S3 (or nobody answers): in doubt now, not at
    // the 2 × TTL deadline.
    let out = stalled(&mut h, Some(true));
    assert!(
        matches!(replies(&out).as_slice(), [(r, ClientReply::InDoubt)] if *r == rid),
        "{out:?}"
    );
    assert_eq!(h.core.stats.s3_less_deadlines, 1);
    assert_eq!(h.core.clients().count(), 0);
}

#[test]
fn a_continuation_epoch_is_not_a_stall() {
    let mut h = Harness::new(1);
    h.core.lease.cached_holder = Some(2);
    h.core.epoch.open = true;
    h.core.epoch.active = true;
    stalled(&mut h, None);
    assert!(!h.core.s3_less());
}

/// A peer's forward reaching a restarted node that the lease object
/// still names: the node re-adopts its own lease and the requester is
/// told to retry, instead of `NotHolder { 0 }` sending it to a lease path
/// that, with its S3 cut, cannot run (EC2 campaign 8 A-1).
#[test]
fn a_forward_to_a_restarted_holder_readopts_its_own_lease() {
    let mut h = Harness::new(1);
    let own = lease_of(1, 3, h.now.plus(30_000).0);
    h.core.lease.note_object(h.now, &own);
    let op = h.create("a");
    let request = |req: u64| PeerMsg::MutateRequest {
        req: OpId(req),
        rid: Rid {
            node: 2,
            incarnation: 1,
            seq: 1,
        },
        op: op.clone(),
        acked_through: 0,
        deps: constellation_meta::Position::ZERO,
        applied: 0,
        tag: Default::default(),
    };
    let out = h.step(Event::Peer {
        from: 2,
        msg: request(77),
    });
    assert!(
        matches!(
            sends(&out).as_slice(),
            [(
                2,
                PeerMsg::MutateReply {
                    req: OpId(77),
                    outcome: MutateOutcome::Held { .. },
                    ..
                }
            )]
        ),
        "{out:?}"
    );
    assert_eq!(h.core.job(), Some(JobKind::Acquire));
    assert_eq!(h.core.stats.readopt_for_forward, 1);
    let (get, _) = s3_ops(&out)
        .into_iter()
        .find(|(_, r)| matches!(r, S3Op::LeaseGet))
        .expect("the acquisition reads the lease");
    // A retry meanwhile is held again, with no second acquisition.
    let out = h.step(Event::Peer {
        from: 2,
        msg: request(78),
    });
    assert!(matches!(
        sends(&out).as_slice(),
        [(
            2,
            PeerMsg::MutateReply {
                outcome: MutateOutcome::Held { .. },
                ..
            }
        )]
    ));
    assert!(s3_ops(&out)
        .iter()
        .all(|(_, r)| !matches!(r, S3Op::LeaseGet)));
    // Still ours: the re-adoption goes on (a takeover of our own lease:
    // tail to head first), no longer an idle acquisition.
    let out = h.step(Event::S3 {
        op: get,
        result: S3Result::LeaseGet(Ok(Some((own, tag())))),
    });
    assert_eq!(h.core.job(), Some(JobKind::Acquire), "{out:?}");
}

#[test]
fn a_readoption_claims_nothing_but_its_own_unreleased_lease() {
    for theirs in [
        // Released by this node (a handoff in flight): the claimer's.
        Lease {
            released: true,
            ..lease_of(1, 3, 0)
        },
        // Somebody else's by now.
        lease_of(3, 4, i64::MAX),
    ] {
        let mut h = Harness::new(1);
        h.core
            .lease
            .note_object(h.now, &lease_of(1, 3, h.now.plus(30_000).0));
        let op = h.create("a");
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(77),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 1,
                },
                op,
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
                applied: 0,
                tag: Default::default(),
            },
        });
        let (get, _) = s3_ops(&out)
            .into_iter()
            .find(|(_, r)| matches!(r, S3Op::LeaseGet))
            .expect("the acquisition reads the lease");
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((theirs.clone(), tag())))),
        });
        assert!(
            s3_ops(&out).iter().all(|(_, r)| !matches!(
                r,
                S3Op::LeaseCreate { .. } | S3Op::LeaseSwap { .. } | S3Op::SegmentRun { .. }
            )),
            "{theirs:?}: {out:?}"
        );
        assert_ne!(h.core.job(), Some(JobKind::Acquire), "{theirs:?}");
        assert!(h.core.lease.held.is_none());
        // Not re-tried on every forward: for a TTL the next one hears
        // `NotHolder`, so a requester that reaches S3 takes the lease path.
        let op = h.create("b");
        let out = h.step(Event::Peer {
            from: 2,
            msg: PeerMsg::MutateRequest {
                req: OpId(78),
                rid: Rid {
                    node: 2,
                    incarnation: 1,
                    seq: 2,
                },
                op,
                acked_through: 0,
                deps: constellation_meta::Position::ZERO,
                applied: 0,
                tag: Default::default(),
            },
        });
        assert!(
            matches!(
                sends(&out).as_slice(),
                [(
                    2,
                    PeerMsg::MutateReply {
                        outcome: MutateOutcome::NotHolder { .. },
                        ..
                    }
                )]
            ),
            "{theirs:?}: {out:?}"
        );
        assert_ne!(h.core.job(), Some(JobKind::Acquire), "{theirs:?}");
    }
}

/// A node that released the lease itself (the object read after says so)
/// answers a stale forward `NotHolder`, never re-adopting.
#[test]
fn a_released_lease_is_not_readopted_for_a_forward() {
    let mut h = Harness::new(1);
    h.core.lease.note_object(
        h.now,
        &Lease {
            released: true,
            ..lease_of(1, 3, h.now.plus(30_000).0)
        },
    );
    let op = h.create("a");
    let out = h.step(Event::Peer {
        from: 2,
        msg: PeerMsg::MutateRequest {
            req: OpId(77),
            rid: Rid {
                node: 2,
                incarnation: 1,
                seq: 1,
            },
            op,
            acked_through: 0,
            deps: constellation_meta::Position::ZERO,
            applied: 0,
            tag: Default::default(),
        },
    });
    assert!(
        matches!(
            sends(&out).as_slice(),
            [(
                2,
                PeerMsg::MutateReply {
                    outcome: MutateOutcome::NotHolder { holder: 0 },
                    ..
                }
            )]
        ),
        "{out:?}"
    );
    assert_ne!(h.core.job(), Some(JobKind::Acquire));
}

/// A-1: a node reads the lease when it starts, so a restarted holder
/// knows the lease still names it before the first forward arrives.
#[test]
fn a_node_reads_the_lease_at_start() {
    let meta = Meta::open_in_memory().unwrap();
    let mut core = Core::new(Config::defaults(1, 1));
    let mut out = Vec::new();
    core.start(Ms(0), &meta, &mut out);
    assert!(
        s3_ops(&out)
            .iter()
            .any(|(_, r)| matches!(r, S3Op::LeaseGet)),
        "{out:?}"
    );
    assert!(core.job().is_none());
}

/// Plan 31 C1: a holder's refusal is a portable [`Code`] on the wire and in
/// the journal. Two in-process nodes (real cores over real `Meta`s, like
/// [`pair`]), with every reply between them carried by the daemon's real
/// transport encoding rather than handed over as a Rust value, so what the
/// requester reads is only what the bytes say.
mod portable_codes {
    use super::*;
    use constellation_meta::{JournalPos, Position};
    use constellation_net::{Payload, Signed};
    use constellation_types::Code;

    /// One hop of the P2P transport for the holder's reply: packed the way
    /// the daemon packs it (`main.rs`'s `mutate_requested`: the outcome as
    /// postcard inside `Payload::MutateReply`), signed and framed as the
    /// endpoint sends it, then verified and unpacked the way the
    /// requester's forward task does (`authority_driver.rs`). Returns the
    /// decoded message and the outcome's wire bytes.
    fn over_the_wire(msg: &PeerMsg) -> (PeerMsg, Vec<u8>) {
        let PeerMsg::MutateReply {
            req,
            outcome,
            base,
            position,
            gen,
            own_chunks,
            own_rows,
        } = msg
        else {
            panic!("not a mutate reply: {msg:?}")
        };
        let payload = Payload::MutateReply {
            req_id: req.0,
            outcome: outcome.to_postcard().unwrap(),
            base: *base,
            position_seq: position.seq,
            position_pending: position.pending.map(|p| (p.epoch, p.jseq)),
            position_streams: position.streams_wire(),
            gen: *gen,
            own_chunks: own_chunks.to_wire().0,
            own_inos: own_chunks.to_wire().1,
            own_rows: constellation_meta::OwnRows::to_wire(own_rows.as_ref()),
        };
        let key = iroh::SecretKey::from_bytes(&[7; 32]);
        let frame = Signed::new(&key, &payload).unwrap().encode().unwrap();
        let (_, received) = Signed::decode(&frame[4..]).unwrap().verify().unwrap();
        let Payload::MutateReply {
            req_id,
            outcome,
            base,
            position_seq,
            position_pending,
            position_streams,
            gen,
            own_chunks,
            own_inos,
            own_rows,
        } = received
        else {
            panic!("the frame decoded to another payload")
        };
        let decoded = PeerMsg::MutateReply {
            req: OpId(req_id),
            outcome: MutateOutcome::from_postcard(&outcome).unwrap(),
            base,
            position: Position {
                seq: position_seq,
                pending: position_pending.map(|(epoch, jseq)| JournalPos { epoch, jseq }),
                streams: Default::default(),
            }
            .with_streams_wire(&position_streams),
            gen,
            own_chunks: OwnChunks::from_wire(own_chunks, own_inos),
            own_rows: constellation_meta::OwnRows::from_wire(&own_rows),
        };
        (decoded, outcome)
    }

    /// Forward `op` from the requester to the holder and carry the reply
    /// back over the wire: the requester's client answer, and the
    /// outcome's wire bytes.
    fn forward_over_the_wire(
        holder: &mut Harness,
        requester: &mut Harness,
        rid: Rid,
        op: MutateOp,
    ) -> (ClientReply, Vec<u8>) {
        let out = requester.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        let sent = sends(&out);
        let [(1, request)] = sent.as_slice() else {
            panic!("expected one forward to node 1: {sent:?}")
        };
        let answer = holder.step(Event::Peer {
            from: 2,
            msg: (*request).clone(),
        });
        let (reply, bytes) = over_the_wire(sends(&answer)[0].1);
        let out = requester.step(Event::Peer {
            from: 1,
            msg: reply,
        });
        let r = replies(&out);
        let [(r_rid, reply)] = r.as_slice() else {
            panic!("expected one client reply: {out:?}")
        };
        assert_eq!(*r_rid, rid);
        ((*reply).clone(), bytes)
    }

    /// Node A (the holder) refuses node B's forwarded ops; each refusal
    /// crosses the wire as `Code`'s own discriminant and reaches B's client
    /// as the same `Code`, hence the same Linux errno the FUSE boundary
    /// answers with (`constellation_frontend_fuse::reply_code`). A retry by rid is answered from
    /// the journaled refusal (the `completed` row), again the same `Code`;
    /// and the `Refused` record itself carries the portable number.
    #[test]
    fn a_holders_refusal_crosses_the_wire_as_the_same_code() {
        let (mut holder, mut requester) = pair();
        let d = holder.meta.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        holder.meta.create(d.ino, "f", 0o644, 0, 0).unwrap();
        holder.meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap();
        let cases = [
            // The golden case: ENOTEMPTY is 39 on Linux, 66 on Darwin,
            // and 3 on the wire.
            (
                MutateOp::Rmdir {
                    parent: ROOT_INO,
                    name: "d".into(),
                },
                Code::NotEmpty,
                39,
            ),
            (
                MutateOp::Unlink {
                    parent: ROOT_INO,
                    name: "missing".into(),
                },
                Code::NotFound,
                2,
            ),
            (
                MutateOp::Rmdir {
                    parent: ROOT_INO,
                    name: "file".into(),
                },
                Code::NotDir,
                20,
            ),
        ];
        for (seq, (op, code, linux)) in cases.into_iter().enumerate() {
            let rid = requester.rid(seq as u64 + 1);
            let (reply, bytes) =
                forward_over_the_wire(&mut holder, &mut requester, rid, op.clone());
            // `MutateOutcome::Errno` is variant 1; its payload is the
            // code's wire number, never the Linux errno.
            assert_eq!(bytes, vec![1, code.to_wire() as u8], "{code:?}");
            assert_ne!(
                code.to_wire() as i32,
                linux,
                "{code:?}: a wire number that is also its errno proves nothing"
            );
            let ClientReply::Outcome(MutateOutcome::Errno(got)) = reply else {
                panic!("{code:?}: expected a refusal, got {reply:?}")
            };
            assert_eq!(got, code);
            assert_eq!(got.to_linux_errno(), linux, "{code:?}");
            // The holder journaled the refusal as an outcome with the
            // portable number, and a retry by rid is answered from it.
            assert_eq!(holder.meta.refused_code(rid).unwrap(), Some(code));
            let (retry, retry_bytes) = forward_over_the_wire(&mut holder, &mut requester, rid, op);
            assert_eq!(retry_bytes, bytes, "{code:?}: the retry's answer");
            assert_eq!(retry, ClientReply::Outcome(MutateOutcome::Errno(code)));
        }
        assert_eq!(Code::NotEmpty.to_darwin_errno(), 66);
        // The journaled `Refused` records: postcard carries the wire
        // number, and a replica tailing them (on any OS) reads back the
        // same codes.
        let refused: Vec<LogRecord> = holder
            .meta
            .peek_journal_after(0)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .filter(|r| matches!(r, LogRecord::Refused { .. }))
            .collect();
        assert_eq!(refused.len(), 3, "{refused:?}");
        let follower = Meta::open_in_memory().unwrap();
        for record in &refused {
            let LogRecord::Refused { rid, code } = record else {
                unreachable!()
            };
            let bytes = record.to_postcard().unwrap();
            assert_eq!(bytes.last(), Some(&(code.to_wire() as u8)), "{record:?}");
            let decoded = LogRecord::from_postcard(&bytes).unwrap();
            assert_eq!(&decoded, record);
            follower.apply_records_journaled(&[decoded]).unwrap();
            assert_eq!(follower.refused_code(*rid).unwrap(), Some(*code));
        }
    }
}

/// Plan 32 Step 0.1 (chunk 32-m0a) records what a snapshot's drain
/// reaches inside a delegated subtree, without redesigning delegations:
/// the root holder's `Barrier` for a directory node 2 holds a write
/// delegation on is an ordinary round — upload, ship this node's journal
/// — and completes without a single message to the delegate. So what the
/// delegate has executed but not yet streamed to the root (its stream
/// tail) is not drained: a snapshot of a delegated subtree freezes the
/// root's replica as of the delegate's last streamed batch.
#[test]
fn a_barrier_inside_a_delegated_subtree_does_not_reach_the_delegate() {
    let (mut h, dir) = root_with_dir();
    crate::replica::Replica::apply_segment(
        &h.meta,
        2,
        1,
        0,
        &[],
        &[],
        &[LogRecord::Delegate {
            dir,
            node: 2,
            gen: 1,
            designated: false,
            range: (0, 0),
        }],
    )
    .unwrap();
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    assert!(
        h.core
            .deleg_view()
            .gens
            .iter()
            .any(|&(d, node, ..)| d == dir && node == 2),
        "the delegation is live at the root: {:?}",
        h.core.deleg_view()
    );
    let barrier = OpId(1 << 50);
    let mut queue: std::collections::VecDeque<Event> = [Event::Control {
        op: barrier,
        req: Control::Barrier { ino: Some(dir) },
    }]
    .into();
    let mut to_delegate = Vec::new();
    let mut done = None;
    for _ in 0..200 {
        let Some(event) = queue.pop_front() else {
            break;
        };
        let out = h.step(event);
        for (to, msg) in sends(&out) {
            if to == 2 {
                to_delegate.push(format!("{msg:?}"));
            }
        }
        for action in &out {
            match action {
                Action::ControlDone { op, result } if *op == barrier => done = Some(result.clone()),
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => queue.push_back(Event::Timer { id: *id }),
                Action::UploadDirtyChunks { op, .. } => queue.push_back(Event::UploadsDone {
                    op: *op,
                    result: UploadResult::Done { held: 0 },
                }),
                Action::S3 { op, req } => {
                    let result = match req {
                        S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                        S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                        S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                        S3Op::HeartbeatRead => S3Result::Heartbeats(Ok(Vec::new())),
                        _ => continue,
                    };
                    queue.push_back(Event::S3 { op: *op, result });
                }
                _ => {}
            }
        }
        if done.is_some() {
            break;
        }
    }
    assert_eq!(done, Some(Ok(ControlOk::Done)), "the barrier completes");
    assert!(
        to_delegate.is_empty(),
        "the barrier asked the delegate for something: {to_delegate:?}"
    );
}

/// Drive `h` (S3 answered at once, timers fired in order, uploads done)
/// until `op` is answered or `steps` events have run; `during_put` runs
/// before each segment PUT is answered — the writer of the busy-holder
/// tests. Returns the answer and how many segment PUTs it took.
fn drive_until_answered(
    h: &mut Harness,
    first: Vec<Action>,
    op: OpId,
    steps: usize,
    mut during_put: impl FnMut(&mut Harness),
) -> (Option<Result<ControlOk, String>>, usize) {
    let mut queue: std::collections::VecDeque<Event> = Default::default();
    let mut puts = 0;
    let absorb = |out: &[Action],
                  queue: &mut std::collections::VecDeque<Event>|
     -> Option<Result<ControlOk, String>> {
        let mut done = None;
        for action in out {
            match action {
                Action::ControlDone {
                    op: answered,
                    result,
                } if *answered == op => done = Some(result.clone()),
                Action::SetTimer {
                    id,
                    kind: TimerKind::Poll,
                    ..
                } => queue.push_back(Event::Timer { id: *id }),
                Action::UploadDirtyChunks { op, .. } => queue.push_back(Event::UploadsDone {
                    op: *op,
                    result: UploadResult::Done { held: 0 },
                }),
                Action::Publish { op, .. } | Action::FollowHead { op } => {
                    queue.push_back(Event::PublishDone { op: *op, ok: true })
                }
                Action::S3 { op, req } => {
                    let result = match req {
                        S3Op::SegmentPut { .. } => S3Result::SegmentPut(Ok(())),
                        S3Op::SegmentRun { .. } => S3Result::SegmentRun(Ok(Vec::new())),
                        S3Op::SegmentGap { .. } => S3Result::SegmentGap(Ok(None)),
                        S3Op::HeartbeatRead => S3Result::Heartbeats(Ok(Vec::new())),
                        S3Op::LeaseSwap { .. } => S3Result::LeasePut(Ok(tag())),
                        S3Op::InboxRun { .. } => S3Result::InboxRun(Ok(Vec::new())),
                        _ => continue,
                    };
                    queue.push_back(Event::S3 { op: *op, result });
                }
                _ => {}
            }
        }
        done
    };
    if let Some(done) = absorb(&first, &mut queue) {
        return (Some(done), puts);
    }
    for _ in 0..steps {
        let Some(event) = queue.pop_front() else {
            break;
        };
        if let Event::S3 {
            result: S3Result::SegmentPut(Ok(())),
            ..
        } = &event
        {
            puts += 1;
            during_put(h);
        }
        let out = h.step(event);
        if let Some(done) = absorb(&out, &mut queue) {
            return (Some(done), puts);
        }
    }
    (None, puts)
}

/// Fix (snap-drain-busy): a `Barrier` on a holder whose own client keeps
/// writing — a row journaled during every segment PUT, so the journal is
/// never empty — answers as soon as the segment carrying every row
/// journaled before it lands. It used to wait for a round to end with an
/// empty journal: under this writer the round ships for ever, and the
/// barrier (a snapshot's drain, an `fsync` in `--fsync-mode s3`) never
/// answered — or, when a round did end, failed "journal not shipped: no
/// lease" on the very node holding the lease.
#[test]
fn a_barrier_on_a_busy_holder_answers_once_its_own_position_ships() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    submit_create(&mut h, 1, "acknowledged-before");
    let before = Replica::journal_tip(&h.meta);
    let barrier = OpId(1 << 50);
    let first = h.step(Event::Control {
        op: barrier,
        req: Control::Barrier { ino: None },
    });
    let mut next = 2;
    let (done, puts) = drive_until_answered(&mut h, first, barrier, 2_000, |h| {
        submit_create(h, next, &format!("w{next}"));
        next += 1;
    });
    assert_eq!(done, Some(Ok(ControlOk::Done)), "after {puts} segment PUTs");
    assert!(
        puts <= 2,
        "the barrier waited {puts} segment PUTs for one row journaled before it"
    );
    assert!(
        !h.meta.journal_unshipped_through(before).unwrap(),
        "everything journaled before the barrier shipped"
    );
    assert!(
        Replica::journal_len(&h.meta).unwrap() > 0,
        "and the writer's later rows are still unshipped: the journal never emptied"
    );
}

/// The other side of the position rule: rows that cannot ship (a
/// manifest held back on a chunk only an absent node has) fail the
/// barrier after a bounded number of rounds, with a message that says
/// so — not "no lease" from the holder.
#[test]
fn a_barrier_behind_a_held_back_row_fails_after_bounded_rounds() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    submit_create(&mut h, 1, "f");
    let ino = h.meta.child_ino(ROOT_INO, "f").unwrap().unwrap();
    let away = constellation_fs_core::ChunkHash::of(b"only node 2 has it");
    h.meta.enroll_remote_chunks(ino, &[away], 2).unwrap();
    let manifest = constellation_fs_core::Manifest {
        layout: constellation_fs_core::ChunkLayout::new(4096),
        file_len: 7,
        chunks: constellation_fs_core::ChunkInfo::Inline(std::collections::BTreeMap::from([(
            0u64, away,
        )])),
    }
    .encode();
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(2),
        op: MutateOp::SetManifest {
            ino,
            base_manifest: None,
            manifest,
            size: 7,
            mtime_ns: None,
        },
        tag: Default::default(),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    let barrier = OpId(1 << 50);
    let first = h.step(Event::Control {
        op: barrier,
        req: Control::Barrier { ino: None },
    });
    let (done, _) = drive_until_answered(&mut h, first, barrier, 2_000, |_| {});
    let Some(Err(error)) = done else {
        panic!("the barrier did not fail: {done:?}");
    };
    assert!(
        error.contains("held back") && !error.contains("no lease"),
        "{error}"
    );
    assert!(h.core.barriers.is_empty(), "nothing left waiting");
}

/// A barrier on a node that does not hold the lease and has rows to ship
/// says that, instead of the old "no lease" text that the busy holder
/// also got.
#[test]
fn a_barrier_on_a_non_holder_with_a_journal_says_it_does_not_hold() {
    let mut h = Harness::new(1);
    // Journal a row directly, as an S3-only node does before it acquires.
    h.meta
        .create(ROOT_INO, "x", 0o644, 0, 0)
        .expect("journal a row locally");
    assert!(Replica::journal_len(&h.meta).unwrap() > 0);
    let barrier = OpId(1 << 50);
    let first = h.step(Event::Control {
        op: barrier,
        req: Control::Barrier { ino: None },
    });
    let (done, _) = drive_until_answered(&mut h, first, barrier, 200, |_| {});
    let Some(Err(error)) = done else {
        panic!("the barrier did not fail: {done:?}");
    };
    assert!(error.contains("does not hold the write lease"), "{error}");
}

/// Fix snap-drain-busy, review: a deposition empties the journal without
/// shipping it (the recovery round strands the unshipped rows and queues
/// them for replay by rid), so "nothing at or below my position left in
/// the journal" must not answer a barrier around one. A barrier waiting
/// as the node is deposed fails at once; one asked while deposed is
/// refused; one asked after the recovery round stranded the rows waits
/// until the replay has settled — it used to answer `Done` there, an
/// `fsync` in `--fsync-mode s3` returning before its write reached the
/// new holder.
#[test]
fn a_barrier_around_a_deposition_waits_for_the_replay_not_the_strand() {
    let mut h = Harness::new(1);
    h.hold(1, None);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: h.rid(1),
        op: h.create("a"),
        tag: Default::default(),
    });
    assert!(matches!(
        replies(&out)[0].1,
        ClientReply::Outcome(MutateOutcome::Accepted { .. })
    ));
    let answer = |out: &[Action], op: OpId| {
        out.iter().find_map(|a| match a {
            Action::ControlDone { op: o, result } if *o == op => Some(result.clone()),
            _ => None,
        })
    };
    // A barrier waits for the acknowledged create to ship.
    let waiting = OpId(1 << 50);
    let out = h.step(Event::Control {
        op: waiting,
        req: Control::Barrier { ino: None },
    });
    assert_eq!(answer(&out, waiting), None);
    // The round renews past half-TTL and loses the CAS to a takeover.
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
    let deposed = h.step(Event::S3 {
        op: reread,
        result: S3Result::LeaseGet(Ok(Some((theirs, tag())))),
    });
    assert!(h.core.lease().lost);
    let Some(Err(error)) = answer(&deposed, waiting) else {
        panic!("the waiting barrier was not failed at the deposition");
    };
    assert!(error.contains("deposed"), "{error}");
    assert!(h.core.barriers.is_empty());
    // Asked while deposed: refused at once, not queued behind the strand.
    let while_lost = OpId((1 << 50) + 1);
    let out = h.step(Event::Control {
        op: while_lost,
        req: Control::Barrier { ino: None },
    });
    let Some(Err(error)) = answer(&out, while_lost) else {
        panic!("a barrier asked while deposed was not refused");
    };
    assert!(error.contains("deposed and is recovering"), "{error}");
    assert!(h.core.barriers.is_empty());
    // The recovery round strands the create: out of the journal, queued
    // for replay to node 2, not in the log.
    let (tail, req) = s3_ops(&deposed)[0];
    assert!(matches!(req, S3Op::SegmentRun { .. }));
    let out = h.step(Event::S3 {
        op: tail,
        result: S3Result::SegmentRun(Ok(Vec::new())),
    });
    let out = at_head(&mut h, out);
    assert!(!h.core.lease().lost);
    assert_eq!(Replica::journal_len(&h.meta).unwrap(), 0);
    let queue = h.meta.pending_replays().unwrap();
    assert_eq!(queue.len(), 1);
    // Asked now (the fsync's retry): the journal is empty, but the
    // acknowledged create is only queued for replay, so no `Done`.
    let after = OpId((1 << 50) + 2);
    let mut first = out;
    first.extend(h.step(Event::Control {
        op: after,
        req: Control::Barrier { ino: None },
    }));
    let (done, _) = drive_until_answered(&mut h, first, after, 200, |_| {});
    assert!(
        !matches!(done, Some(Ok(_))),
        "the barrier answered before the stranded create was replayed: {done:?}"
    );
    if let Some(Err(error)) = &done {
        assert!(error.contains("replayed"), "{error}");
    }
    // The replay settles (the new holder acknowledged it): a barrier
    // answers again.
    h.meta.forget_replay(queue[0].queue_seq).unwrap();
    h.core.replay.in_flight = None;
    let settled = OpId((1 << 50) + 3);
    let first = h.step(Event::Control {
        op: settled,
        req: Control::Barrier { ino: None },
    });
    let (done, _) = drive_until_answered(&mut h, first, settled, 200, |_| {});
    assert_eq!(done, Some(Ok(ControlOk::Done)));
}

/// Harness `auto-placement` (2 in 11): the root's roster is read at
/// mount and every 5 s; a peer that registered after it mounted and
/// writes at once is connected, forwarding, and dominant, but not in the
/// roster. The placement skipped it as unreachable until the next read,
/// and the root lease placement handed that writer the lease first. A
/// connected candidate outside the roster has it re-read now (not every
/// tick), and the next tick delegates.
#[test]
fn placement_re_reads_the_roster_for_a_connected_writer_it_does_not_list() {
    let mut h = Harness::new(1);
    h.core.cfg.delegation = true;
    h.core.cfg.placement = true;
    h.core.cfg.placement_min_ops = 20;
    h.hold(1, None);
    let op = h.create("d1");
    constellation_meta::execute_mutate(&h.meta, &op, None).unwrap();
    let MutateOp::Create { ino: dir, .. } = op else {
        unreachable!()
    };
    h.step(Event::Roster {
        write_eligible: vec![1],
    });
    h.step(Event::Peers {
        links: vec![crate::event::PeerLink {
            node: 2,
            connected: true,
            last_seen: Some(h.now),
            rtt_ms: Some(1),
            since: Some(h.now),
        }],
    });
    for i in 0..40 {
        let bucket = super::placement::bucket_of(&format!("b-{i}"));
        h.core.place_note(2, [(dir, Some(bucket))]);
    }
    let tick = |h: &mut Harness| {
        let (now, mut out) = (h.now, Vec::new());
        h.core.on_placement_timer(now, &h.meta, &mut out);
        out
    };
    let refreshes = |out: &[Action]| {
        out.iter()
            .filter(|a| matches!(a, Action::RefreshRoster))
            .count()
    };
    let out = tick(&mut h);
    assert_eq!(refreshes(&out), 1, "the roster was not re-read: {out:?}");
    assert_eq!(h.core.stats.place_delegated, 0);
    assert_eq!(h.core.stats.place_skipped_unreachable, 1);
    // The answer is not in yet: no second read within the second.
    h.advance(400);
    let out = tick(&mut h);
    assert_eq!(refreshes(&out), 0, "re-read on every tick: {out:?}");
    // It lists the writer: the next tick delegates `d1` to it.
    h.step(Event::Roster {
        write_eligible: vec![1, 2],
    });
    h.advance(400);
    let out = tick(&mut h);
    assert_eq!(h.core.stats.place_delegated, 1, "not delegated: {out:?}");
    assert_eq!(
        h.core
            .dl
            .gens
            .values()
            .map(|g| (g.dir, g.node))
            .collect::<Vec<_>>(),
        vec![(dir, 2)]
    );
}

/// A replica of node `me` whose table delegates `/d` to `me` as
/// generation 7 (the root is node 1). Returns the directory.
fn delegated_to_me(meta: &Meta, me: NodeId) -> constellation_fs_core::Ino {
    let dir = meta.allocate_ino(ROOT_INO).unwrap();
    crate::replica::Replica::apply_segment(
        meta,
        1,
        1,
        0,
        &[],
        &[],
        &[
            LogRecord::Mkdir {
                parent: ROOT_INO,
                name: "d".into(),
                ino: dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                time_ns: 1,
            },
            LogRecord::Delegate {
                dir,
                node: me,
                gen: 7,
                designated: false,
                range: (0, 0),
            },
        ],
    )
    .unwrap();
    dir
}

fn renew_req(out: &[Action], gen: u64) -> Option<OpId> {
    sends(out).into_iter().find_map(|(_, m)| match m {
        PeerMsg::DelegRenew { req, gen: g, .. } if *g == gen => Some(*req),
        _ => None,
    })
}

/// This delegate's stream round of `gen` (every answer echoes it).
fn round_of(h: &Harness, gen: u64) -> u64 {
    h.core.dl.mine.get(&gen).map_or(0, |d| d.stream_round)
}

/// The root's (node 1's) answer to `h`'s renewal `req`.
fn renewed(h: &Harness, req: OpId, gen: u64, ttl_ms: u64) -> Event {
    Event::Peer {
        from: 1,
        msg: PeerMsg::DelegRenewed {
            req,
            gen,
            round: round_of(h, gen),
            ttl_ms,
            locks: Vec::new(),
            lock_grace_ms: 0,
            lock_floor: Default::default(),
            lock_cut_at: 0,
            lock_cut: Box::default(),
            lock_barrier: 0,
        },
    }
}

/// delegate-fenced-io (`locks-blips-tight-delegated` seed 3117): a
/// recalled delegate answers the ops parked on it `NotHolder` with no
/// generation. Taken as a root's reply, it made the delegate this node's
/// cached lease holder, and this node's lock renewals went there (and
/// that delegate's to this one) for the rest of the S3 cut, until the
/// grant lapsed under its I/O. A refusal to execute names no holder; an
/// executed reply of the root still does.
#[test]
fn a_not_holder_reply_does_not_make_its_sender_the_holder() {
    let mut h = Harness::new(2);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.cfg.forwarding = true;
    h.core.lease.cached_holder = Some(1);
    let dir = delegated_to_me(&h.meta, 3);
    let forward = |h: &mut Harness, seq: u64| {
        let rid = h.rid(seq);
        let op = MutateOp::Create {
            parent: dir,
            name: format!("f{seq}"),
            ino: h.meta.allocate_ino(dir).unwrap(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        };
        let out = h.step(Event::Submit {
            policy: Policy::Client,
            rid,
            op,
            tag: Default::default(),
        });
        let Some((3, PeerMsg::MutateRequest { req, .. })) = sends(&out).first().copied() else {
            panic!("no forward to the delegate: {out:?}");
        };
        *req
    };
    let reply = |req, outcome| Event::Peer {
        from: 3,
        msg: PeerMsg::MutateReply {
            req,
            outcome,
            base: None,
            position: constellation_meta::Position::ZERO,
            gen: 0,
            own_chunks: OwnChunks::None,
            own_rows: None,
        },
    };
    let req = forward(&mut h, 1);
    h.step(reply(req, MutateOutcome::NotHolder { holder: 0 }));
    assert_eq!(h.core.lease.cached_holder, Some(1));
    let req = forward(&mut h, 2);
    h.step(reply(req, MutateOutcome::Busy));
    assert_eq!(h.core.lease.cached_holder, Some(1));
}

/// The K5a fix round's post-handoff stall: a delegate restarted with its
/// node identity and journal (a K5 handoff, `daemon --upgrade`, a crash
/// and remount) installed nothing at start — only a `Delegate`/`Recall`
/// segment ran the table sync — so its writes under the subtree bounced
/// between the root (`NotHolder` naming it) and itself until the root
/// reclaimed the grant nobody renewed (about 4 s on kind, 7 s in the
/// harness). Now `start` re-adopts it: installed and renewed at once,
/// and the first renewal's answer lets it execute locally again.
#[test]
fn a_restarted_delegate_readopts_its_delegation_at_start() {
    let meta = Meta::open_in_memory().unwrap();
    meta.set_node_prefix(3).unwrap();
    let dir = delegated_to_me(&meta, 3);
    let mut cfg = Config::defaults(3, 1);
    cfg.delegation = true;
    cfg.p2p = true;
    let mut core = Core::new(cfg);
    core.lease.cached_holder = Some(1);
    let now = Ms(1_000_000);
    let mut out = Vec::new();
    core.start(now, &meta, &mut out);
    assert!(core.dl.mine.contains_key(&7), "not re-adopted at start");
    let req = renew_req(&out, 7).expect("no renewal sent at start");
    assert!(
        sends(&out)
            .iter()
            .any(|(to, m)| *to == 1 && matches!(m, PeerMsg::DelegRenew { gen: 7, .. })),
        "{out:?}"
    );
    let mut h = Harness {
        core,
        meta,
        now,
        manual_horizon: false,
    };
    h.advance(2);
    h.step(renewed(&h, req, 7, 5_000));
    assert!(
        h.core.dl.mine[&7].until > h.now,
        "the renewal was not honoured"
    );
    // A write under the subtree executes here, as the delegate.
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
        tag: Default::default(),
    });
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, PeerMsg::MutateRequest { .. })),
        "forwarded instead of executed: {out:?}"
    );
    assert!(
        replies(&out).iter().any(|(r, _)| *r == rid),
        "not answered: {out:?}"
    );
    assert_eq!(h.core.dl.mine[&7].executed, 1);
}

/// overload-cascade-2 (`stress-ng-fs-nodes`): a root whose requests
/// queue behind other work answers a delegate's batch and renewal after
/// the delegate stopped waiting (`deleg_request_timeout_ms`, 500 ms).
/// Thrown away, as before, every answer arrived late: the delegate
/// re-sent the same batch every half second for minutes with the root
/// holding every row (all the ops parked on it waited), and its
/// renewals never counted. The late stream acknowledgement and the late
/// grant count now; the batch is re-sent after a backoff, not at once.
#[test]
fn a_delegates_late_answers_still_count() {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.lease.cached_holder = Some(1);
    let dir = delegated_to_me(&h.meta, 3);
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let req = renew_req(&out, 7).expect("no renewal on install");
    let ttl = 5_000;
    let margin = h.core.cfg.expiry_margin_ms;
    let out = h.step(renewed(&h, req, 7, ttl));
    let renew_timer = timers(&out, TimerKind::DelegRenew);
    assert_eq!(renew_timer.len(), 1, "{out:?}");
    let batch = |out: &[Action]| {
        sends(out).into_iter().find_map(|(to, m)| match m {
            PeerMsg::DelegateStream {
                req, gen: 7, txs, ..
            } if to == 1 => Some((*req, txs.last().map(|t| t.idx).unwrap_or(0))),
            _ => None,
        })
    };
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
        tag: Default::default(),
    });
    let (sreq, last) = batch(&out).expect("no batch streamed");
    assert!(last > 0);
    let poke = |h: &mut Harness| {
        h.step(Event::Activity {
            last_write: Ms(0),
            acked_seqs: Vec::new(),
        })
    };
    // Unanswered past the timeout: given up on, re-sent only after a
    // backoff.
    h.advance(h.core.deleg_request_timeout_ms());
    let out = poke(&mut h);
    assert!(batch(&out).is_none(), "re-sent at once: {out:?}");
    assert_eq!(h.core.dl.mine[&7].streamed_through, 0);
    // The answer comes, late: the root has the rows.
    h.advance(300);
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegateStreamAck {
            req: sreq,
            gen: 7,
            round: round_of(&h, 7),
            through: last,
            refused: false,
        },
    });
    assert_eq!(
        h.core.dl.mine[&7].streamed_through, last,
        "the late acknowledgement did not count"
    );
    // A renewal answered late counts from its own send.
    let renew_sent = h.now;
    let out = h.step(Event::Timer { id: renew_timer[0] });
    let req = renew_req(&out, 7).expect("no renewal");
    h.advance(h.core.deleg_request_timeout_ms());
    let out = poke(&mut h);
    let again = renew_req(&out, 7).expect("not re-sent");
    assert_ne!(again, req);
    h.advance(700);
    h.step(renewed(&h, req, 7, ttl));
    assert_eq!(
        h.core.dl.mine[&7].until,
        renew_sent.plus(ttl - margin),
        "the late grant did not count"
    );
    // A late refusal leaves the generation to the renewal in flight.
    h.advance(h.core.deleg_request_timeout_ms());
    poke(&mut h);
    h.step(renewed(&h, again, 7, 0));
    assert!(!h.core.dl.mine[&7].stopped, "stopped on a late refusal");
}

/// overload-cascade-2 review: a late stream acknowledgement counts only
/// from the root tenure its batch went to. The root changed (R → R2) and
/// came back (→ R) within the late window: each change re-streamed from
/// what the log carries, so R's late answer for its first tenure ("I
/// have through N") says nothing about R's second tenure — credited, the
/// stream would stop short of rows the log never got (long-delegated-
/// backup seed 75504's class).
#[test]
fn a_late_stream_ack_from_an_earlier_tenure_of_the_same_root_is_not_credited() {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.lease.cached_holder = Some(1);
    let dir = delegated_to_me(&h.meta, 3);
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let req = renew_req(&out, 7).expect("no renewal on install");
    h.step(renewed(&h, req, 7, 5_000));
    let batch_to = |out: &[Action], root: NodeId| {
        sends(out).into_iter().find_map(|(to, m)| match m {
            PeerMsg::DelegateStream {
                req,
                gen: 7,
                round,
                txs,
                ..
            } if to == root => Some(((*req, *round), txs.last().map(|t| t.idx).unwrap_or(0))),
            _ => None,
        })
    };
    let rid = h.rid(1);
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op: MutateOp::Create {
            parent: dir,
            name: "f".into(),
            ino: h.meta.allocate_ino(dir).unwrap(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        tag: Default::default(),
    });
    let ((sreq, sround), last) = batch_to(&out, 1).expect("no batch streamed");
    assert!(last > 0);
    let poke = |h: &mut Harness| {
        h.step(Event::Activity {
            last_write: Ms(0),
            acked_seqs: Vec::new(),
        })
    };
    // Given up on (late), then the root changes and comes back.
    h.advance(h.core.deleg_request_timeout_ms());
    poke(&mut h);
    h.core.lease.cached_holder = Some(2);
    let out = poke(&mut h);
    assert!(
        batch_to(&out, 2).is_some(),
        "not re-streamed to R2: {out:?}"
    );
    h.advance(100);
    h.core.lease.cached_holder = Some(1);
    let out = poke(&mut h);
    let ((again, round), _) = batch_to(&out, 1).expect("not re-streamed to R");
    assert_ne!(again, sreq);
    assert_ne!(round, sround);
    assert_eq!(h.core.stats.deleg_restreams, 2);
    // R's late answer from its first tenure.
    h.advance(100);
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegateStreamAck {
            req: sreq,
            gen: 7,
            round: sround,
            through: last,
            refused: false,
        },
    });
    assert_eq!(
        h.core.dl.mine[&7].streamed_through, 0,
        "credited an acknowledgement from the root's earlier tenure"
    );
    // The answer to the batch of this tenure counts.
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegateStreamAck {
            req: again,
            gen: 7,
            round,
            through: last,
            refused: false,
        },
    });
    assert_eq!(h.core.dl.mine[&7].streamed_through, last);
}

/// delegate-stream-acks: a delegate of `/d` (generation 7, root node 1)
/// whose first renewal was granted. `stream_one` executes a create under
/// it and returns the batch that went out (`(req, round, last)`).
fn streaming_delegate() -> (Harness, constellation_fs_core::Ino) {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.lease.cached_holder = Some(1);
    let dir = delegated_to_me(&h.meta, 3);
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let req = renew_req(&out, 7).expect("no renewal on install");
    h.step(renewed(&h, req, 7, 5_000));
    (h, dir)
}

/// The batch of generation 7 in `out` to `root`: `(req, round, first,
/// last)`.
fn stream_batch(out: &[Action], root: NodeId) -> Option<(OpId, u64, u64, u64)> {
    sends(out).into_iter().find_map(|(to, m)| match m {
        PeerMsg::DelegateStream {
            req,
            gen: 7,
            round,
            txs,
            ..
        } if to == root => Some((
            *req,
            *round,
            txs.first().map(|t| t.idx).unwrap_or(0),
            txs.last().map(|t| t.idx).unwrap_or(0),
        )),
        _ => None,
    })
}

fn create_in(h: &mut Harness, dir: constellation_fs_core::Ino, seq: u64) -> Vec<Action> {
    let rid = h.rid(seq);
    let op = MutateOp::Create {
        parent: dir,
        name: format!("f{seq}"),
        ino: h.meta.allocate_ino(dir).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    })
}

fn poke(h: &mut Harness) -> Vec<Action> {
    h.step(Event::Activity {
        last_write: Ms(0),
        acked_seqs: Vec::new(),
    })
}

fn stream_ack(from: NodeId, req: OpId, round: u64, through: u64) -> Event {
    Event::Peer {
        from,
        msg: PeerMsg::DelegateStreamAck {
            req,
            gen: 7,
            round,
            through,
            refused: false,
        },
    }
}

/// Give the batch in flight up and wait out the backoff until the stream
/// re-sends (from the acknowledged cursor); the re-sent batch.
fn resend(h: &mut Harness) -> (OpId, u64, u64, u64) {
    h.advance(h.core.deleg_request_timeout_ms());
    poke(h);
    for _ in 0..20 {
        h.advance(250);
        if let Some(b) = stream_batch(&poke(h), 1) {
            return b;
        }
    }
    panic!("never re-sent");
}

/// delegate-stream-acks (`stress-ng-fs-nodes`): the root answers a batch
/// after the delegate gave up on it and re-sent it. The batch and its
/// answer are one-way messages now, so the answer is not lost to a
/// request timeout; it is cumulative, so it counts although it names the
/// batch given up on (and covers the re-send in flight), and the answer
/// to the re-send (the same cursor) and a duplicate of either change
/// nothing.
#[test]
fn a_stream_ack_after_the_resend_counts_and_duplicates_change_nothing() {
    let (mut h, dir) = streaming_delegate();
    let (first, round, _, last) = stream_batch(&create_in(&mut h, dir, 1), 1).expect("no batch");
    let (again, again_round, from, again_last) = resend(&mut h);
    assert_ne!(again, first);
    assert_eq!((again_round, from, again_last), (round, 1, last));
    assert_eq!(h.core.dl.mine[&7].streamed_through, 0);
    // The answer to the batch given up on.
    h.step(stream_ack(1, first, round, last));
    let d = &h.core.dl.mine[&7];
    assert_eq!(
        d.streamed_through, last,
        "the late acknowledgement did not count"
    );
    assert_eq!(h.core.stats.deleg_stream_acks_unmatched, 1);
    // It covers the re-send in flight: the stream is free for the next
    // row at once.
    assert!(
        d.inflight.is_none(),
        "still waiting for the re-send's answer"
    );
    // Its duplicate, the re-send's answer, and that one's duplicate.
    for (req, n) in [(first, 1), (again, 1), (again, 2)] {
        let out = h.step(stream_ack(1, req, round, last));
        assert!(
            stream_batch(&out, 1).is_none(),
            "re-sent on answer {n}: {out:?}"
        );
        assert_eq!(h.core.dl.mine[&7].streamed_through, last);
    }
    assert!(h.core.dl.mine[&7].inflight.is_none());
    assert_eq!(h.core.stats.deleg_stream_acks_unmatched, 1);
    // Nothing more is sent for rows the root has.
    h.advance(5_000);
    assert!(stream_batch(&poke(&mut h), 1).is_none());
}

/// delegate-stream-acks: the root's answers overtake each other (two
/// connections' streams, or a queued batch and its re-send handled in
/// either order): an earlier, shorter cursor arriving last never moves
/// the delegate's acknowledged cursor back, and the next batch goes out
/// from the furthest one.
#[test]
fn reordered_stream_acks_never_move_the_cursor_back() {
    let (mut h, dir) = streaming_delegate();
    let (first, round, _, one) = stream_batch(&create_in(&mut h, dir, 1), 1).expect("no batch");
    // A second row while the first batch is in flight: the re-send
    // carries both.
    create_in(&mut h, dir, 2);
    let (again, _, from, two) = resend(&mut h);
    assert_eq!((from, two), (one, one + 1));
    // The re-send's answer first, then the first batch's.
    h.step(stream_ack(1, again, round, two));
    assert_eq!(h.core.dl.mine[&7].streamed_through, two);
    h.step(stream_ack(1, first, round, one));
    assert_eq!(
        h.core.dl.mine[&7].streamed_through, two,
        "a reordered answer moved the cursor back"
    );
    // The next row streams from the furthest cursor.
    let (_, _, from, _) = stream_batch(&create_in(&mut h, dir, 3), 1).expect("no batch");
    assert_eq!(from, two + 1);
}

/// delegate-stream-acks: an answer counts only from the root this
/// generation streams to, in the round it streams in. The root changes
/// mid-stream (R → R2): R's answer, and R2's answer carrying R's round
/// (an answer to a batch that reached R2 before the delegate learned it
/// was the root), are dropped; R2's answer in its own round counts. A
/// renewal granted by the old root (with its lock handoff) is dropped
/// too.
#[test]
fn a_root_change_mid_stream_drops_the_old_rounds_answers() {
    let (mut h, dir) = streaming_delegate();
    let (first, round, _, last) = stream_batch(&create_in(&mut h, dir, 1), 1).expect("no batch");
    let until = h.core.dl.mine[&7].until;
    let renew_sent = h.now;
    let out = h.step(Event::Timer {
        id: h.core.dl.mine[&7].renew_timer.expect("no renewal timer"),
    });
    let renewal = renew_req(&out, 7).expect("no renewal");
    // R2 is the root now: re-streamed from what the log carries.
    h.advance(100);
    h.core.lease.cached_holder = Some(2);
    let (second, round2, from, _) = stream_batch(&poke(&mut h), 2).expect("not re-streamed");
    assert_eq!(from, 1);
    assert_ne!(round2, round);
    for (root, req, r) in [(1, first, round), (2, first, round), (1, second, round2)] {
        h.step(stream_ack(root, req, r, last));
        assert_eq!(
            h.core.dl.mine[&7].streamed_through, 0,
            "credited node {root}'s answer in round {r}"
        );
    }
    assert_eq!(h.core.stats.deleg_stream_acks_stale, 3);
    // R's grant of the renewal sent before the change, with a lock
    // handoff: neither counts.
    let f = h.meta.allocate_ino(dir).unwrap();
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegRenewed {
            req: renewal,
            gen: 7,
            round,
            ttl_ms: 5_000,
            locks: vec![constellation_meta::locks::Grant {
                id: constellation_meta::locks::GrantId { node: 1, seq: 9 },
                node: 4,
                ino: f,
                mode: constellation_meta::locks::LockMode::Exclusive,
                until_ms: h.now.0 + 5_000,
                recalled: false,
                gen: 0,
                confirmed_ms: constellation_meta::locks::Grant::MINTED,
            }],
            lock_grace_ms: 0,
            lock_floor: Default::default(),
            lock_cut_at: 0,
            lock_cut: Default::default(),
            lock_barrier: 0,
        },
    });
    assert_eq!(
        h.core.dl.mine[&7].until, until,
        "the old root's grant counted"
    );
    assert!(
        h.meta
            .locks()
            .get(constellation_meta::locks::GrantId { node: 1, seq: 9 })
            .is_none(),
        "the old root's lock handoff installed"
    );
    assert!(h.now > renew_sent);
    // R2's answer in its round counts.
    h.step(stream_ack(2, second, round2, last));
    assert_eq!(h.core.dl.mine[&7].streamed_through, last);
}

/// delegate-stream-acks: the root drops out of view and comes back (the
/// same node). The gap may hide another tenure, so the round changes and
/// the batch in flight, answered in the old round, is stale: the stream
/// sends again at once in the new round rather than after the batch's
/// timeout and a backoff; the old round's answer counts for nothing.
#[test]
fn a_root_lost_from_view_restarts_the_batch_in_flight_at_once() {
    let (mut h, dir) = streaming_delegate();
    let (first, round, _, last) = stream_batch(&create_in(&mut h, dir, 1), 1).expect("no batch");
    h.core.lease.cached_holder = None;
    h.core.lease.last_seen = None;
    assert!(stream_batch(&poke(&mut h), 1).is_none());
    assert!(h.core.dl.mine[&7].inflight.is_none());
    h.advance(10);
    h.core.lease.cached_holder = Some(1);
    let (again, round2, from, through) = stream_batch(&poke(&mut h), 1).expect("not re-sent");
    assert_ne!(round2, round);
    assert_eq!((from, through), (1, last));
    h.step(stream_ack(1, first, round, last));
    assert_eq!(
        h.core.dl.mine[&7].streamed_through, 0,
        "the old round counted"
    );
    h.step(stream_ack(1, again, round2, last));
    assert_eq!(h.core.dl.mine[&7].streamed_through, last);
}

/// delegate-stream-acks: a late renewal answer for a generation this
/// node no longer has (ended under the old root) counts for nothing:
/// its lock cut, from the old root, never replaces the current root's
/// newer one (the cut is replaced whenever it comes from another root).
#[test]
fn a_late_answer_for_an_ended_generation_leaves_the_cut() {
    let (mut h, _dir) = streaming_delegate();
    // R2 is the root now and its renewal carried its cut.
    h.core.lease.cached_holder = Some(2);
    poke(&mut h);
    h.core.lk.cut = Some((2, 100, Default::default()));
    // R's late answer for generation 6, which ended here.
    assert!(!h.core.dl.mine.contains_key(&6));
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegRenewed {
            req: OpId(9_999),
            gen: 6,
            round: 1,
            ttl_ms: 5_000,
            locks: Vec::new(),
            lock_grace_ms: 0,
            lock_floor: Default::default(),
            lock_cut_at: 500,
            lock_cut: Default::default(),
            lock_barrier: 0,
        },
    });
    let (root, at, _) = h.core.lk.cut.expect("the cut is gone");
    assert_eq!((root, at), (2, 100), "the old root's cut replaced R2's");
    assert_eq!(h.core.stats.deleg_renewed_stale, 1);
}

/// delegate-stream-acks: request ids start over at a restart, so an
/// answer to this node's previous incarnation could name a request of
/// this one. The round carries the incarnation: the old grant does not
/// count for the new renewal with the same id.
#[test]
fn an_answer_to_a_previous_incarnation_does_not_count() {
    let install = |incarnation: u32| {
        let mut h = Harness::new(3);
        h.core.cfg.incarnation = incarnation;
        h.core.cfg.delegation = true;
        h.core.cfg.p2p = true;
        h.core.lease.cached_holder = Some(1);
        delegated_to_me(&h.meta, 3);
        let mut out = Vec::new();
        h.core.delegation_sync(h.now, &h.meta, &mut out);
        let req = renew_req(&out, 7).expect("no renewal on install");
        (h, req)
    };
    let (old, old_req) = install(1);
    let (mut h, req) = install(2);
    assert_eq!(req, old_req, "the ids are reused (the case under test)");
    let old_round = round_of(&old, 7);
    assert_ne!(old_round, round_of(&h, 7));
    h.step(Event::Peer {
        from: 1,
        msg: PeerMsg::DelegRenewed {
            req,
            gen: 7,
            round: old_round,
            ttl_ms: 5_000,
            locks: Vec::new(),
            lock_grace_ms: 0,
            lock_floor: Default::default(),
            lock_cut_at: 0,
            lock_cut: Default::default(),
            lock_barrier: 0,
        },
    });
    assert_eq!(h.core.dl.mine[&7].until, Ms(0), "renewed by an old answer");
    h.step(renewed(&h, req, 7, 5_000));
    assert!(h.core.dl.mine[&7].until > h.now);
}

/// delegate-stream-acks (`stress-ng-fs-nodes`): the root sealed a live
/// delegate whose renewals it handled late, at the very cursor the
/// delegate was re-streaming from. A batch is a sign of life like a
/// renewal: while batches come (re-sends of rows the root has included)
/// the generation is neither sealed through its backup nor reclaimed;
/// once they and the renewals stop, it is sealed when the grant's window
/// has passed, as before.
#[test]
fn a_root_seals_a_delegate_only_on_real_silence() {
    let mut h = Harness::new(1);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.cfg.delegation_ttl_ms = 1_000;
    h.hold(1, None);
    let (dir, _) = locks::delegated_file(&h.meta);
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    assert!(h.core.dl.gens.contains_key(&7));
    h.step(Event::Peer {
        from: 3,
        msg: PeerMsg::DelegRenew {
            req: OpId(1),
            gen: 7,
            round: 1,
            backup: Some(4),
            stream_head: 0,
            stream_head_at: 0,
        },
    });
    let sub = h.meta.allocate_ino(dir).unwrap();
    let batch = |req: u64| Event::Peer {
        from: 3,
        msg: PeerMsg::DelegateStream {
            req: OpId(req),
            gen: 7,
            round: 1,
            txs: vec![constellation_meta::DelegateTx {
                idx: 1,
                rid: None,
                records: vec![LogRecord::Mkdir {
                    parent: dir,
                    name: "sub".into(),
                    ino: sub,
                    mode: 0o755,
                    uid: 0,
                    gid: 0,
                    time_ns: 5,
                }],
                deps: Default::default(),
            }],
            leaving: Vec::new(),
            leaving_barriers: Vec::new(),
        },
    };
    // Fire every delegation expiry that is due; whether one sealed.
    let fire = |h: &mut Harness| -> bool {
        let due: Vec<TimerId> = h
            .core
            .timers
            .iter()
            .filter(|(_, (t, at))| matches!(t, Timer::DelegExpiry(7)) && *at <= h.now)
            .map(|(id, _)| *id)
            .collect();
        let mut sealed = false;
        for id in due {
            let out = h.step(Event::Timer { id });
            sealed |= sends(&out)
                .iter()
                .any(|(to, m)| *to == 4 && matches!(m, PeerMsg::DelegSeal { gen: 7, .. }));
        }
        sealed
    };
    // No renewal for three windows, the same batch every 500 ms (its
    // first copy appended, the rest re-sends).
    for i in 0..12u64 {
        h.advance(500);
        let out = h.step(batch(10 + i));
        assert!(
            sends(&out).iter().any(|(to, m)| *to == 3
                && matches!(
                    m,
                    PeerMsg::DelegateStreamAck {
                        through: 1,
                        refused: false,
                        round: 1,
                        ..
                    }
                )),
            "not acknowledged: {out:?}"
        );
        assert!(!fire(&mut h), "sealed a delegate streaming to it");
        assert!(!h.core.dl.gens[&7].ended);
    }
    assert!(h.core.stats.deleg_live_by_stream > 0);
    // Silence: sealed once the window passed. (The root's lease renewed,
    // as its holder would.)
    h.hold(1, None);
    let mut sealed = false;
    for _ in 0..10 {
        h.advance(500);
        sealed |= fire(&mut h);
    }
    assert!(sealed, "a silent delegate never sealed");
}

/// A delegate of `/d` (generation 7, root node 1) whose renewal was
/// granted once and which then lost the root: its own write under the
/// subtree is parked on a renewal nobody answers. Returns the harness,
/// the directory, the parked op's rid and the lapse watch to fire.
fn delegate_parked_on_a_silent_root() -> (Harness, constellation_fs_core::Ino, Rid, TimerId) {
    let mut h = Harness::new(3);
    h.core.cfg.delegation = true;
    h.core.cfg.p2p = true;
    h.core.lease.cached_holder = Some(1);
    let dir = delegated_to_me(&h.meta, 3);
    let mut out = Vec::new();
    h.core.delegation_sync(h.now, &h.meta, &mut out);
    let req = renew_req(&out, 7).expect("no renewal on install");
    let ttl = 5_000;
    let margin = h.core.cfg.expiry_margin_ms;
    let out = h.step(renewed(&h, req, 7, ttl));
    let until = h.core.dl.mine[&7].until;
    assert_eq!(until, h.now.plus(ttl - margin));
    let lapse = timers(&out, TimerKind::DelegLapse);
    assert_eq!(
        lapse.len(),
        1,
        "no lapse watch armed by the renewal: {out:?}"
    );
    // The root goes silent: past the grant, the delegate's write parks
    // on a renewal nobody answers.
    h.advance(ttl);
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
        tag: Default::default(),
    });
    assert_eq!(h.core.stats.deleg_parked_expired, 1, "{out:?}");
    assert!(renew_req(&out, 7).is_some(), "no renewal for the parked op");
    assert!(replies(&out).is_empty());
    // Before the lapse point the watch only re-arms.
    let at = until.plus(2 * margin);
    assert!(h.now < at);
    let out = h.step(Event::Timer { id: lapse[0] });
    assert!(!h.core.dl.mine[&7].lapsed, "lapsed too early");
    let lapse = timers(&out, TimerKind::DelegLapse);
    assert_eq!(lapse.len(), 1, "the watch was not re-armed: {out:?}");
    h.now = at;
    (h, dir, rid, lapse[0])
}

/// The K5a fix round's minutes-long `fsync`: the root died while this
/// node was its delegate (and no longer its backup, so only a TTL
/// takeover can replace it). The delegate's own write parked on a
/// renewal of the lapsed grant, which a dead root never answers, so it
/// never took the forward, inbox and lease path that starts the TTL
/// takeover: it waited for good. Now a grant left unrenewed past the
/// root's earliest reclaim (`until + 2 × margin`) lapses: the parked op
/// is forwarded to the root (whose silence leads to the inbox and the
/// lease path), nothing executes or parks under the generation, and the
/// renewals go on.
#[test]
fn a_delegate_gives_up_a_grant_its_dead_root_never_renews() {
    let (mut h, dir, rid, lapse) = delegate_parked_on_a_silent_root();
    let out = h.step(Event::Timer { id: lapse });
    assert_eq!(h.core.stats.deleg_lapsed, 1);
    let d = &h.core.dl.mine[&7];
    assert!(d.lapsed && !d.stopped, "{d:?}");
    assert!(d.parked.is_empty());
    // The root is unreachable (no link): the op re-reads the lease to
    // learn where to go (then the inbox, or the lease path).
    learns_the_holder(&h, &out, rid);
    // A new write under the subtree takes the same route, not a park.
    let rid2 = h.rid(2);
    let op = MutateOp::Create {
        parent: dir,
        name: "g".into(),
        ino: h.meta.allocate_ino(dir).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid: rid2,
        op,
        tag: Default::default(),
    });
    assert!(
        h.core.dl.mine[&7].parked.is_empty(),
        "parked again: {out:?}"
    );
    learns_the_holder(&h, &out, rid2);
    // The renewals go on while the root stays silent: one per request
    // timeout, on the lapsed generation's own timer.
    for _ in 0..5 {
        let retry = timers(&out_of_renew_timer(&mut h), TimerKind::DelegRenew);
        assert_eq!(retry.len(), 1, "no renewal retry armed");
    }
    assert_eq!(h.core.dl.mine[&7].executed, 0);
}

/// `rid` left the delegate's park for the ordinary route to an
/// unreachable root: the lease read that learns the holder is issued.
fn learns_the_holder(h: &Harness, out: &[Action], rid: Rid) {
    assert!(
        matches!(h.core.clients[&rid].phase, client::Phase::LearnHolder),
        "{:?}",
        h.core.clients[&rid].phase
    );
    assert!(
        s3_ops(out).iter().any(|(_, r)| matches!(r, S3Op::LeaseGet)),
        "no lease read for the route: {out:?}"
    );
}

/// Advances to the next renewal retry of generation 7 and fires it,
/// expecting a renewal sent (the last one timed out). The actions.
fn out_of_renew_timer(h: &mut Harness) -> Vec<Action> {
    let t = h.core.dl.mine[&7].renew_timer.expect("no renewal timer");
    h.advance(h.core.deleg_request_timeout_ms());
    let out = h.step(Event::Timer { id: t });
    assert!(renew_req(&out, 7).is_some(), "no renewal sent: {out:?}");
    out
}

/// Review should-fix 4: a lapse is not the end of a delegation. A link
/// that recovers before the root reclaims answers the renewal, the grant
/// is honoured again and the next write under the subtree executes here;
/// a refusal (the root reclaimed or recalls) stops the generation.
#[test]
fn a_lapsed_delegation_renewed_before_the_reclaim_executes_again() {
    let (mut h, dir, _rid, lapse) = delegate_parked_on_a_silent_root();
    h.step(Event::Timer { id: lapse });
    assert!(h.core.dl.mine[&7].lapsed);
    let out = out_of_renew_timer(&mut h);
    let req = renew_req(&out, 7).unwrap();
    h.advance(2);
    h.step(renewed(&h, req, 7, 5_000));
    let d = &h.core.dl.mine[&7];
    assert!(!d.lapsed && !d.stopped && d.until > h.now, "{d:?}");
    let rid = h.rid(3);
    let op = MutateOp::Create {
        parent: dir,
        name: "h".into(),
        ino: h.meta.allocate_ino(dir).unwrap(),
        mode: 0o644,
        uid: 0,
        gid: 0,
    };
    let out = h.step(Event::Submit {
        policy: Policy::Client,
        rid,
        op,
        tag: Default::default(),
    });
    assert!(
        replies(&out).iter().any(|(r, _)| *r == rid),
        "not executed here: {out:?}"
    );
    assert_eq!(h.core.dl.mine[&7].executed, 1);
    // A fresh lapse watch, and a refusal after the next lapse stops it.
    let (mut h, _dir, _rid, lapse) = delegate_parked_on_a_silent_root();
    h.step(Event::Timer { id: lapse });
    let out = out_of_renew_timer(&mut h);
    let req = renew_req(&out, 7).unwrap();
    h.step(renewed(&h, req, 7, 0));
    let d = &h.core.dl.mine[&7];
    assert!(d.stopped, "{d:?}");
    assert!(d.renew_timer.is_none());
}

fn root_lease(holder: NodeId, epoch: Epoch, expires: Ms) -> Lease {
    Lease {
        v: 1,
        partition: "p0".into(),
        holder,
        epoch,
        expires_unix_ms: expires.0,
        released: false,
        wanted_by: Vec::new(),
        backups: Vec::new(),
        config_version: 1,
        ack_policy: constellation_store_s3::AckPolicy::Local,
        granted_delegations: false,
        retired: Vec::new(),
    }
}

/// The K5a fix round's root loss, seen from a third node: its op was
/// accepted (by the root, or a delegate whose stream the root appends)
/// and waits for the log, and the root dies with no backup. Nothing put
/// the node on the lease path, so the TTL takeover only came from the
/// op's retry after its client deadline (2 × TTL, `EIO` in between).
/// Now a node with such an op older than the inbox's P2P grace reads the
/// lease each round and takes the root over once it ran out; a live
/// root's lease is left alone (no acquisition, no `wanted_by`).
#[test]
fn an_op_waiting_for_a_dead_roots_append_takes_the_root_over_once_its_lease_ran_out() {
    let (mut h, _rid, _) = awaiting_log_forward("dr");
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    assert!(s3_ops(&out).is_empty(), "read within the grace: {out:?}");
    h.advance(h.core.cfg.inbox_p2p_grace_ms);
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    let read = s3_ops(&out);
    assert!(
        matches!(read.as_slice(), [(_, S3Op::LeaseGet)]),
        "no lease read: {out:?}"
    );
    let op = read[0].0;
    // One read at a time.
    let mut again = Vec::new();
    h.core.dead_root_check(h.now, &mut again);
    assert!(s3_ops(&again).is_empty(), "{again:?}");
    // A live root: nothing is asked of it.
    let live = root_lease(1, 1, h.now.plus(5_000));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::LeaseGet(Ok(Some((live, tag())))),
    });
    assert_eq!(h.core.stats.dead_root_acquires, 0);
    assert!(h.core.job.is_none(), "an acquisition against a live root");
    assert!(
        s3_ops(&out)
            .iter()
            .all(|(_, r)| !matches!(r, S3Op::LeaseSwap { .. })),
        "{out:?}"
    );
    // Seen live, the lease is not read again before the expiry it showed.
    h.advance(4_999);
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    assert!(
        s3_ops(&out).is_empty(),
        "re-read a lease seen live: {out:?}"
    );
    // A released lease is no dead root's (it shipped everything).
    h.advance(1);
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    let op = s3_ops(&out)[0].0;
    let mut released = root_lease(1, 1, Ms(h.now.0 - 1));
    released.released = true;
    h.step(Event::S3 {
        op,
        result: S3Result::LeaseGet(Ok(Some((released, tag())))),
    });
    assert_eq!(h.core.stats.dead_root_acquires, 0);
    assert!(h.core.job.is_none(), "took a released lease");
    // The root's lease ran out: the next round's read takes it over.
    h.advance(6_000);
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    let op = s3_ops(&out)[0].0;
    let expired = root_lease(1, 1, Ms(h.now.0 - 1));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::LeaseGet(Ok(Some((expired, tag())))),
    });
    assert_eq!(h.core.stats.dead_root_acquires, 1);
    assert_eq!(
        h.core.job.as_ref().map(|j| j.kind()),
        Some(super::jobs::JobKind::Acquire),
        "no acquisition started: {out:?}"
    );
}

/// Drives a dead-root check of `h` (an op waiting for the log past the
/// grace) to its acquisition: the check's read sees node 1's lease run
/// out. Returns the acquisition's actions so far.
fn dead_root_acquisition(h: &mut Harness) -> Vec<Action> {
    h.advance(h.core.cfg.inbox_p2p_grace_ms);
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    let op = s3_ops(&out)[0].0;
    let expired = root_lease(1, 1, Ms(h.now.0 - 1));
    let out = h.step(Event::S3 {
        op,
        result: S3Result::LeaseGet(Ok(Some((expired, tag())))),
    });
    assert_eq!(h.core.stats.dead_root_acquires, 1);
    out
}

/// Answers the acquisition job's own lease read with `lease`: the
/// actions that follow.
fn answer_job_read(h: &mut Harness, out: &[Action], lease: Lease) -> Vec<Action> {
    let reads: Vec<_> = s3_ops(out)
        .into_iter()
        .filter(|(_, r)| matches!(r, S3Op::LeaseGet))
        .collect();
    let [(op, _)] = reads.as_slice() else {
        panic!("expected the acquisition's lease read: {out:?}")
    };
    h.step(Event::S3 {
        op: *op,
        result: S3Result::LeaseGet(Ok(Some((lease, tag())))),
    })
}

fn asks_nothing_of_the_holder(h: &Harness, out: &[Action]) {
    assert!(
        s3_ops(out)
            .iter()
            .all(|(_, r)| !matches!(r, S3Op::LeaseSwap { .. })),
        "registered in wanted_by: {out:?}"
    );
    assert!(
        !sends(out)
            .iter()
            .any(|(_, m)| matches!(m, PeerMsg::LeaseRequest { .. })),
        "asked the holder for a handoff: {out:?}"
    );
    assert!(h.core.job.is_none(), "the acquisition did not end");
    assert!(h.core.lease.held.is_none());
}

/// Review must-fix 1: a dead-root takeover only claims a lease that ran
/// out. When the acquisition's own read finds the root live again (a
/// slow root renewed between the check's read and the job's), the plan
/// is `Busy` — and the node neither registers in `wanted_by` (which
/// would make the live root hand over) nor asks it for the lease.
#[test]
fn a_dead_root_takeover_that_finds_the_root_renewed_asks_it_nothing() {
    let (mut h, _rid, _) = awaiting_log_forward("drb");
    let out = dead_root_acquisition(&mut h);
    let live = root_lease(1, 1, h.now.plus(5_000));
    let out = answer_job_read(&mut h, &out, live);
    asks_nothing_of_the_holder(&h, &out);
}

/// Review must-fix 1, the backup case: the dead root's lease is a
/// `Backup` lease listing another node, and this node is not a listed
/// backup. Within `backup_claim_grace_ms` past the expiry the plan is
/// `Busy`, and a `wanted_by` swap would replace the lease object under
/// the listed backup's claim CAS: nothing is written.
#[test]
fn a_dead_root_takeover_leaves_a_backup_lease_in_its_claim_grace_alone() {
    let (mut h, _rid, _) = awaiting_log_forward("drg");
    let out = dead_root_acquisition(&mut h);
    let mut lease = root_lease(1, 1, Ms(h.now.0 - 1));
    lease.ack_policy = constellation_store_s3::AckPolicy::Backup;
    lease.backups = vec![3];
    let out = answer_job_read(&mut h, &out, lease);
    asks_nothing_of_the_holder(&h, &out);
}

/// Review should-fix 1: under a root that is live but slow (its ship
/// or an upload hold keeps an op waiting for the log), the dead-root
/// check reads the lease about once per renewal period, not once per
/// sync round. Here: 60 s of 500 ms rounds against a root renewing a
/// 10 s lease every 5 s — 6 reads instead of 120.
#[test]
fn the_dead_root_check_reads_a_slow_live_roots_lease_once_per_renewal() {
    let (mut h, _rid, _) = awaiting_log_forward("drs");
    h.advance(h.core.cfg.inbox_p2p_grace_ms);
    let start = h.now;
    let renewal = 5_000;
    let ttl = 10_000;
    let mut reads = 0;
    while h.now.since(start) < 60_000 {
        let mut out = Vec::new();
        h.core.dead_root_check(h.now, &mut out);
        if let [(op, S3Op::LeaseGet)] = s3_ops(&out).as_slice() {
            reads += 1;
            // The root's last renewal, on its 5 s cadence.
            let renewed = start.plus((h.now.since(start) as u64 / renewal) * renewal);
            let out = h.step(Event::S3 {
                op: *op,
                result: S3Result::LeaseGet(Ok(Some((root_lease(1, 1, renewed.plus(ttl)), tag())))),
            });
            assert!(
                h.core.job.is_none(),
                "an acquisition against a live root: {out:?}"
            );
        }
        h.advance(500);
    }
    assert_eq!(h.core.stats.dead_root_acquires, 0);
    eprintln!("dead-root lease reads over 60 s of rounds: {reads}");
    assert!(
        reads <= 7,
        "{reads} lease reads in 60 s (one per round would be 120)"
    );
    // Suspended: no read at all.
    h.advance(ttl);
    h.core.mode.suspended = true;
    let mut out = Vec::new();
    h.core.dead_root_check(h.now, &mut out);
    assert!(s3_ops(&out).is_empty(), "read while suspended: {out:?}");
}

/// Answers the silence watch's lease read in `out` with node 1's live
/// `Backup` lease at epoch 1 that lists node 2 (this backup).
fn answer_takeover_read(h: &mut Harness, out: &[Action]) -> Vec<Action> {
    let mut lease = root_lease(1, 1, h.now.plus(5_000));
    lease.ack_policy = constellation_store_s3::AckPolicy::Backup;
    lease.backups = vec![2];
    answer_job_read(h, out, lease)
}

/// Should-fix 5 of the delegate-root-loss review: a backup the live
/// holder removed (across its own restart gap — a `daemon --upgrade`, a
/// K5 handoff — or under load) hears nothing once the removal lands.
/// Before, that silence sealed the epoch 1.5 s later, the sealed node
/// refused every later append of it, and the holder ran without a
/// backup for the rest of its tenure (a TTL failover, not a seal). Now
/// the watch reads the lease first: unlisted, it gives the role up
/// without sealing, and the holder's next invitation (a candidate's
/// append at the same epoch) is acknowledged.
#[test]
fn a_backup_its_live_holder_removed_does_not_seal_and_can_be_invited_back() {
    let append = |req, candidacy| Event::Peer {
        from: 1,
        msg: PeerMsg::BackupAppend {
            req: OpId(req),
            epoch: 1,
            holder: 1,
            config_version: 2,
            from: 1,
            txs: Vec::new(),
            through: 0,
            candidacy,
        },
    };
    let mut h = Harness::new(2);
    let out = h.step(append(7, 1));
    let watch = timers(&out, TimerKind::BackupWatch)[0];
    // The holder removed this node and stopped appending.
    h.advance(2_000);
    let out = h.step(Event::Timer { id: watch });
    assert_eq!(h.core.bk.sealed, 0, "sealed before reading the lease");
    let mut unlisted = root_lease(1, 1, h.now.plus(50_000));
    unlisted.config_version = 3;
    answer_job_read(&mut h, &out, unlisted);
    assert_eq!(h.core.bk.sealed, 0, "sealed a live holder that removed it");
    assert_eq!(h.core.stats.seals, 0);
    assert!(
        h.core.bk.role.is_none(),
        "kept the role it was removed from"
    );
    assert!(h.core.job.is_none(), "took over a live holder's lease");
    // The holder invites it back as a new candidate, in the same epoch.
    h.advance(3_000);
    let out = h.step(append(8, 2));
    assert!(
        matches!(
            sends(&out).as_slice(),
            [(1, PeerMsg::BackupAck { sealed: false, .. })]
        ),
        "refused the invitation: {out:?}"
    );
}

/// The read before the seal: a holder heard while it is in flight is
/// alive, and its epoch is not sealed even though the lease (read
/// before that append) lists this node.
#[test]
fn a_holder_heard_while_the_lease_is_read_is_not_sealed() {
    let append = |req| Event::Peer {
        from: 1,
        msg: PeerMsg::BackupAppend {
            req: OpId(req),
            epoch: 1,
            holder: 1,
            config_version: 2,
            from: 1,
            txs: Vec::new(),
            through: 0,
            candidacy: 1,
        },
    };
    let mut h = Harness::new(2);
    let out = h.step(append(7));
    let watch = timers(&out, TimerKind::BackupWatch)[0];
    h.advance(2_000);
    let read = h.step(Event::Timer { id: watch });
    h.advance(100);
    h.step(append(8));
    h.advance(100);
    let out = answer_takeover_read(&mut h, &read);
    assert_eq!(h.core.bk.sealed, 0, "sealed a holder that spoke: {out:?}");
    assert!(h.core.job.is_none());
    assert!(h.core.bk.role.is_some());
}

/// A continuation epoch that opens while the lease is read: a member of
/// an open epoch never seals (rule (b)); `backup_watch_after_epoch`
/// re-arms the watch once the epoch closes.
#[test]
fn a_continuation_epoch_opened_during_the_takeover_read_blocks_the_seal() {
    let mut h = Harness::new(2);
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
            candidacy: 1,
        },
    });
    let watch = timers(&out, TimerKind::BackupWatch)[0];
    h.advance(2_000);
    let read = h.step(Event::Timer { id: watch });
    h.core.epoch.open = true;
    h.advance(100);
    let out = answer_takeover_read(&mut h, &read);
    assert_eq!(h.core.bk.sealed, 0, "sealed inside an open epoch: {out:?}");
    assert_eq!(h.core.stats.seals, 0);
    assert!(h.core.job.is_none());
}
