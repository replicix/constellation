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
        applied: 0,
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
                applied: 0,
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
        },
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
        },
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
            },
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
                    .set_manifest_dirty(g, None, &manifest, 9, &[own])
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
                    },
                    acked_through: 0,
                    deps: constellation_meta::Position::ZERO,
                    applied,
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
            };
            let out = if replay {
                requester.meta.queue_replay(rid, &op).unwrap();
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
            h.meta.queue_replay(rid, &op).unwrap();
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
        assert_eq!(
            h.core.bk.sealed, 1,
            "silence after the successor's append seals"
        );
        assert!(matches!(s3_ops(&out).as_slice(), [(_, S3Op::LeaseGet)]));

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
        h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 1, "the hold outlived its cap");
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
                from: 1,
                txs,
                through: 0,
            },
        });
        let watch = timers(&out, TimerKind::BackupWatch)[0];
        // The holder falls silent: seal, read the lease, take over.
        h.advance(5_000);
        let out = h.step(Event::Timer { id: watch });
        assert_eq!(h.core.bk.sealed, 1);
        let get = find_s3(&out, |r| matches!(r, S3Op::LeaseGet));
        let theirs = backup_lease(1, 1, h.now.plus(4_000).0, vec![2]);
        let out = h.step(Event::S3 {
            op: get,
            result: S3Result::LeaseGet(Ok(Some((theirs.clone(), tag())))),
        });
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
        // The CAS applies, but its reply is a timeout.
        let _ = h.step(Event::S3 {
            op: cas,
            result: S3Result::LeasePut(Err(CasFailure::Failed("timed out".into()))),
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
        requester.meta.queue_replay(rid, &op).unwrap();
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
            h.step(Event::Timer { id: watch });
            assert_eq!(
                h.core.stats.seals, 1,
                "a holder silent over a live link is sealed"
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
        h.step(Event::Peer {
            from,
            msg: PeerMsg::LockRequest {
                req: OpId(req),
                ino,
                mode,
                blocking,
                sent: h.now,
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

    /// The owner re-affirms a grant under a new id for every answer
    /// (a re-sent request that crossed the old id's push); the node may
    /// release the old id. That release ends the grant here too, instead
    /// of leaving it to be outwaited.
    #[test]
    fn a_release_naming_a_superseded_id_ends_the_grant() {
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
        let [(3, OpId(9), LockOutcome::Granted { .. })] = lock_replies(&out).as_slice() else {
            panic!("the waiter is served on the old id's release: {out:?}")
        };
        assert_eq!(h.core.stats.lock_released_superseded, 1);
        assert!(h.meta.locks().get(new).is_none());
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
            .lock_on_generation_outwaited(now, 9, constellation_fs_core::types::ROOT_INO);
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
        },
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
