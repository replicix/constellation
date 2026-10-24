//! Plan 41 L3/L4: the lock renewal lane's task (finding 08).
//!
//! `constellation_authority::lane` decides (sans-IO, with the argument that
//! every extension it makes is one the core could have made); this module
//! drives it: a timer on the daemon's runtime, the node's `Meta` lock tables,
//! the certificate cell the authority core publishes at the end of every
//! step, and the P2P transport for the lane's own `LockRenew` requests. The
//! core's step may be stuck in the metadata store for seconds (a priority
//! write behind a FUSE writer's slow journal write): this task does not run
//! on the core's step, so a lock grant is still renewed meanwhile.
//!
//! It never logs, never touches the store (only `meta.locks()`'s memory), and
//! tells the core what it did on the urgent lane (`SyncRequest::LaneOwn`,
//! `LaneRenewed`, `LaneUnanswered`), which the core handles when its step
//! ends.
//! `CONSTELLATION_LOCK_LANE=off` turns it off (A/B runs, the fail-before of
//! the stall scenarios); `CONSTELLATION_LOCK_LANE_MAX_STALL_MS` (default
//! 120000) bounds the remote renewals while the driver is stuck.

use crate::authority_driver::OffCore;
use crate::sync::SyncRequest;
use constellation_authority::lane::{LaneAction, LaneEvent, LaneFacts, LockLaneView};
use constellation_authority::NodeId;
use constellation_meta::Meta;
use constellation_net::{Payload, Peers};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Whether the lane runs (`CONSTELLATION_LOCK_LANE`: `off`, `0`, `false`).
pub(crate) fn lock_lane_on() -> bool {
    !matches!(
        std::env::var("CONSTELLATION_LOCK_LANE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "off" | "false"
    )
}

/// The bound on remote renewals while the driver is stuck.
pub(crate) fn lock_lane_max_stall_ms() -> i64 {
    std::env::var("CONSTELLATION_LOCK_LANE_MAX_STALL_MS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(constellation_authority::lane::DEFAULT_MAX_STALL_MS)
}

/// The lane's facts: the core's certificate and the replica's live
/// delegation flag.
struct EngineFacts {
    off_core: Arc<OffCore>,
    meta: Arc<Meta>,
}

impl LaneFacts for EngineFacts {
    fn view(&self) -> LockLaneView {
        self.off_core.lane_cell.view()
    }
    fn delegations_any(&self) -> bool {
        self.meta.delegations_any()
    }
}

/// The running lane: stops when dropped (its thread ends within a tick).
pub(crate) struct LockLane {
    stop: Arc<AtomicBool>,
}

impl Drop for LockLane {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

impl LockLane {
    /// Start the lane (`None` with the lane off, or if its thread cannot be
    /// started). It runs on a thread and a current-thread runtime of its
    /// own, like the holder's heartbeat: a core step stuck in a commit
    /// blocks a worker of the shared runtime, and tasks and timers that
    /// runtime would have run wait with it (measured: the lane's ticks did
    /// not run for the whole of a 5 s stall, `max_stuck_ms` showed one tick
    /// at its very end).
    pub(crate) fn start(
        meta: Arc<Meta>,
        peers: Peers,
        off_core: Arc<OffCore>,
        node_id: NodeId,
        forward_timeout_ms: u64,
    ) -> Option<Self> {
        if !lock_lane_on() {
            return None;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let body = move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(lane_loop(
                meta,
                peers,
                off_core,
                node_id,
                forward_timeout_ms,
                thread_stop,
            ));
        };
        std::thread::Builder::new()
            .name("cst-lock-lane".into())
            .spawn(body)
            .ok()?;
        Some(Self { stop })
    }
}

async fn lane_loop(
    meta: Arc<Meta>,
    peers: Peers,
    off_core: Arc<OffCore>,
    node_id: NodeId,
    forward_timeout_ms: u64,
    stop: Arc<AtomicBool>,
) {
    let every = Duration::from_millis((forward_timeout_ms / 4).clamp(50, 250));
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let facts = Arc::new(EngineFacts {
        off_core: off_core.clone(),
        meta: meta.clone(),
    });
    let timeout = Duration::from_millis(forward_timeout_ms.max(1));
    while !stop.load(Ordering::Relaxed) {
        tick.tick().await;
        // The common case, nothing held: one short lock.
        if meta.locks().held_count() == 0 {
            continue;
        }
        let clk = constellation_platform::clock::protocol().now();
        let stuck_ms = off_core.stuck_ms(clk.wait.0);
        let actions = off_core.lock_lane.lock().unwrap().handle(
            clk,
            LaneEvent::Tick { stuck_ms },
            meta.locks(),
            &*facts,
        );
        run(actions, &peers, &off_core, &meta, &facts, node_id, timeout);
    }
}

/// Carry out what the lane asked for: its renewals go out, what it did is
/// told to the core on the urgent lane.
fn run(
    actions: Vec<LaneAction>,
    peers: &Peers,
    off_core: &Arc<OffCore>,
    meta: &Arc<Meta>,
    facts: &Arc<EngineFacts>,
    node_id: NodeId,
    timeout: Duration,
) {
    for action in actions {
        match action {
            LaneAction::Send { to, req, entries } => {
                let (peers, off_core, meta, facts) =
                    (peers.clone(), off_core.clone(), meta.clone(), facts.clone());
                tokio::spawn(async move {
                    let results = if crate::fault::p2p_denied(to) {
                        None
                    } else {
                        // Stamped here, as it leaves (`lane_renew_payload`).
                        let payload = lane_renew_payload(node_id, req, &entries);
                        match tokio::time::timeout(
                            timeout,
                            peers.request_to_node_timeout(to, &payload, timeout),
                        )
                        .await
                        {
                            Ok(Ok(Payload::LockRenewed { req_id, results })) if req_id == req => {
                                Some(crate::locks::renew_results_of(results))
                            }
                            _ => None,
                        }
                    };
                    let event = match results {
                        Some(results) => LaneEvent::Answered { req, results },
                        None => LaneEvent::Failed { req },
                    };
                    let clk = constellation_platform::clock::protocol().now();
                    let actions = off_core.lock_lane.lock().unwrap().handle(
                        clk,
                        event,
                        meta.locks(),
                        &*facts,
                    );
                    // (An answer's own follow-up is a report to the core, never
                    // another send.)
                    report(actions, &off_core);
                });
            }
            other => report(vec![other], off_core),
        }
    }
}

/// The lane's `LockRenew` as it leaves now: `sent_ms` is this node's honour
/// reading at the send and `clock` its protocol clock's id, as the core
/// driver stamps its own renewals at their send (plan 43 C7b, row 29). An
/// owner confirms a grant it installed as a copy from that send on
/// (`Grant::confirmed`), so the stamp must be no earlier than the send:
/// never the reading of the tick that planned it, which is older by the
/// lane's queue and the spawn. (The lane's own window for the answer counts
/// from the tick's reading, the earlier one, which is the safe direction
/// there.)
fn lane_renew_payload(
    from: NodeId,
    req: u64,
    entries: &[constellation_authority::LockRenewEntry],
) -> Payload {
    let pc = constellation_platform::clock::protocol();
    Payload::LockRenew {
        from,
        req_id: req,
        entries: crate::locks::renew_entries_wire(entries),
        sent_ms: pc.honour().0,
        clock: pc.id(),
    }
}

/// Tell the core on the urgent lane.
fn report(actions: Vec<LaneAction>, off_core: &OffCore) {
    let guard = off_core.urgent.lock().unwrap();
    let Some(tx) = guard.as_ref() else {
        return;
    };
    send_reports(tx, actions);
}

fn send_reports(tx: &mpsc::UnboundedSender<SyncRequest>, actions: Vec<LaneAction>) {
    for a in actions {
        match a {
            LaneAction::OwnRenewed { renewed } => {
                let _ = tx.send(SyncRequest::LaneOwn { renewed });
            }
            LaneAction::Renewed {
                from,
                sent,
                results,
            } => {
                let _ = tx.send(SyncRequest::LaneRenewed {
                    from,
                    sent: sent.0,
                    results,
                });
            }
            LaneAction::Unanswered { to } => {
                let _ = tx.send(SyncRequest::LaneUnanswered { to });
            }
            LaneAction::Send { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Plan 43 C7b (row 29) with plan 41's lane: a lane renewal carries the
    /// holder's honour reading at its send and the clock's id, read when the
    /// payload is built for the send, not before (an owner confirms an
    /// installed copy from it; a stamp older than the send would confirm
    /// tokens whose window ended before the renewal left).
    #[test]
    fn the_lanes_renewal_is_stamped_as_it_is_sent() {
        let pc = constellation_platform::clock::protocol();
        let planned = pc.honour();
        std::thread::sleep(Duration::from_millis(30));
        let Payload::LockRenew {
            from,
            req_id,
            sent_ms,
            clock,
            ..
        } = lane_renew_payload(3, 9, &[])
        else {
            panic!("not a LockRenew");
        };
        assert_eq!((from, req_id), (3, 9));
        assert!(
            sent_ms >= planned.0 + 30,
            "stamped at the send: {sent_ms} vs {planned:?}"
        );
        assert_eq!(clock, pc.id());
        assert_ne!(clock, 0, "a clock id of 0 would confirm for every clock");
    }

    /// The lane logs nothing (a dropped line is lost evidence, and the
    /// lane's liveness must not depend on the log's queue).
    #[test]
    fn the_lane_task_never_logs() {
        let source = include_str!("lock_lane.rs");
        let code = source.split("#[cfg(test)]").next().unwrap();
        for macro_name in ["tracing::", "log::", "println!", "eprintln!"] {
            assert!(!code.contains(macro_name), "lock_lane.rs uses {macro_name}");
        }
    }

    /// The task renews this node's own grant under the certificate the core
    /// published, with the core nowhere in sight (a stuck step), and tells the
    /// core on the urgent lane; with the certificate withdrawn it does
    /// nothing. Runs on the real protocol clock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_task_renews_an_own_grant_off_the_core_and_tells_it() {
        use constellation_authority::lane::LockLaneView;
        use constellation_authority::HonourMs;
        use constellation_meta::locks::{GrantId, HeldGrant, LockMode};
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let off_core = Arc::new(OffCore::new(5_000));
        let (tx, mut rx) = mpsc::unbounded_channel();
        *off_core.urgent.lock().unwrap() = Some(tx);
        let clk = constellation_platform::clock::protocol().now();
        // This node (1) is the root and holds its own grant on inode 7,
        // due for renewal now.
        let until = clk.wait.plus(2_000);
        let id = meta.locks().grant(1, 1, 7, LockMode::Exclusive, until, 0);
        meta.locks().install_held(
            7,
            HeldGrant {
                id,
                mode: LockMode::Exclusive,
                until: HonourMs(clk.honour.0 + 5_000),
                renew_at: HonourMs(clk.honour.0 - 1),
                owner: 1,
                recalled: false,
                position: constellation_meta::Position::ZERO,
                renewing: None,
                releasing: false,
                first_use: false,
                idle_since: None,
                installed: clk.honour,
                lapsing: None,
                written: false,
                route: None,
            },
        );
        let view = LockLaneView {
            me: 1,
            root_ok: true,
            cap_until: HonourMs(clk.honour.0 + 60_000),
            fast: false,
            last_fresh: HonourMs(0),
            window_ms: 1_500,
            marked: true,
            owner: 0,
            clock: constellation_platform::clock::protocol().id(),
            lock_ttl_ms: 4_000,
            margin_ms: 500,
            forward_timeout_ms: 400,
        };
        // Withdrawn: nothing.
        off_core.lane_cell.publish(LockLaneView {
            root_ok: false,
            ..view.clone()
        });
        let _lane = LockLane::start(meta.clone(), Peers::disabled(), off_core.clone(), 1, 400)
            .expect("the lane is on by default");
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            rx.try_recv().is_err(),
            "renewed with the certificate withdrawn"
        );
        assert_eq!(meta.locks().get(id).unwrap().until, until);
        // Published: renewed, told.
        off_core.lane_cell.publish(view);
        let told = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("not told in time");
        assert!(matches!(told, Some(SyncRequest::LaneOwn { renewed: 1 })));
        assert!(meta.locks().get(id).unwrap().until > until);
        assert_eq!(meta.locks().held(7).unwrap().route, Some(1));
        assert_eq!(off_core.lock_lane.lock().unwrap().stats.own_renewals, 1);
        let _ = GrantId { node: 1, seq: 1 };
    }

    /// Fail-before (plan 41 L3): the lane runs on a runtime of its own. A
    /// core step stuck in a commit blocks a worker of the shared runtime, and
    /// the lane, as a task on it, did not tick for the whole stall (the
    /// 4-in-5 failures of `lock-stall-lone-node`). Here the shared runtime's
    /// only worker is blocked for 1.5 s and the grant is renewed meanwhile.
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn the_lane_ticks_while_the_shared_runtime_is_blocked() {
        use constellation_authority::lane::LockLaneView;
        use constellation_authority::HonourMs;
        use constellation_meta::locks::{HeldGrant, LockMode};
        let meta = Arc::new(Meta::open_in_memory().unwrap());
        let off_core = Arc::new(OffCore::new(5_000));
        let (tx, mut rx) = mpsc::unbounded_channel();
        *off_core.urgent.lock().unwrap() = Some(tx);
        let clk = constellation_platform::clock::protocol().now();
        let id = meta
            .locks()
            .grant(1, 1, 7, LockMode::Exclusive, clk.wait.plus(2_000), 0);
        meta.locks().install_held(
            7,
            HeldGrant {
                id,
                mode: LockMode::Exclusive,
                until: HonourMs(clk.honour.0 + 5_000),
                renew_at: HonourMs(clk.honour.0 + 100),
                owner: 1,
                recalled: false,
                position: constellation_meta::Position::ZERO,
                renewing: None,
                releasing: false,
                first_use: false,
                idle_since: None,
                installed: clk.honour,
                lapsing: None,
                written: false,
                route: None,
            },
        );
        off_core.lane_cell.publish(LockLaneView {
            me: 1,
            root_ok: true,
            cap_until: HonourMs(clk.honour.0 + 60_000),
            fast: false,
            last_fresh: HonourMs(0),
            window_ms: 1_500,
            marked: true,
            owner: 0,
            clock: constellation_platform::clock::protocol().id(),
            lock_ttl_ms: 4_000,
            margin_ms: 500,
            forward_timeout_ms: 400,
        });
        let _lane = LockLane::start(meta.clone(), Peers::disabled(), off_core.clone(), 1, 400)
            .expect("the lane is on by default");
        // The shared runtime's worker is in a commit.
        let blocked = tokio::spawn(async { std::thread::sleep(Duration::from_millis(1_500)) });
        let started = std::time::Instant::now();
        let told = std::thread::spawn(move || {
            let deadline = started + Duration::from_millis(1_200);
            loop {
                match rx.try_recv() {
                    Ok(r) => return Some(r),
                    Err(_) if std::time::Instant::now() > deadline => return None,
                    Err(_) => std::thread::sleep(Duration::from_millis(20)),
                }
            }
        })
        .join()
        .unwrap();
        assert!(
            matches!(told, Some(SyncRequest::LaneOwn { renewed: 1 })),
            "the lane did not tick while the shared runtime was blocked (told: {})",
            told.is_some()
        );
        blocked.await.unwrap();
    }

    #[test]
    fn what_the_lane_did_reaches_the_core_as_events_on_the_urgent_lane() {
        use constellation_authority::HonourMs;
        let (tx, mut rx) = mpsc::unbounded_channel();
        send_reports(
            &tx,
            vec![
                LaneAction::OwnRenewed { renewed: 2 },
                LaneAction::Renewed {
                    from: 3,
                    sent: HonourMs(77),
                    results: Vec::new(),
                },
                LaneAction::Unanswered { to: 4 },
            ],
        );
        assert!(matches!(
            rx.try_recv(),
            Ok(SyncRequest::LaneOwn { renewed: 2 })
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(SyncRequest::LaneRenewed {
                from: 3,
                sent: 77,
                ..
            })
        ));
        assert!(matches!(
            rx.try_recv(),
            Ok(SyncRequest::LaneUnanswered { to: 4 })
        ));
    }
}
