//! Plan 30 §M10 in the simulation: continuation epochs with flexible
//! quorums, and the single-authority check.
//!
//! The daemon's epoch coordinator (`crates/cli/src/epoch.rs`) runs its
//! propose/ack/activate exchange over P2P; the sim runs the same
//! decisions atomically, with its omniscient view of who is cut from S3
//! and who can reach whom — the protocol's messages add nothing a
//! reordering could break, since every decision is local to one member
//! (its quorum, its join gate, its claim) or the proposer's resolution
//! of the acks it collected:
//!
//! - an [`epoch_outage`] cuts the holder and some others from S3 and
//!   from everyone else over P2P, then, after a grace, tries to form an
//!   epoch among them every 100 ms: at least `N − f` members, every
//!   member alive, with a heartbeat advertising `f` landed (`f > 0`), and
//!   through its join gate (`Meta::promise_join_begin`: its own last
//!   issued promise expired in its clock); the members' claims are
//!   resolved by `resolve_epoch_claims`, and each member's core gets the
//!   activation (`Control::Epoch` with the carrier and the stale floor);
//! - a member that dies or loses a peer freezes its epoch; at the heal
//!   S3 returns and the cores close their epochs themselves
//!   (`Action::EpochClose` → the node driver);
//! - [`sample_authority`] checks, every 25 ms of simulated time, that no
//!   continuation-epoch hold coexists with another node's usable S3 lease
//!   or another hold.

use super::run::Cluster;
use constellation_authority::{resolve_epoch_claims, NodeId};
use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::Duration;

thread_local! {
    /// Authority samples taken by this thread's run.
    pub static SAMPLES: Cell<u64> = const { Cell::new(0) };
}

/// The fault: see `FaultKind::EpochOutage`.
pub async fn epoch_outage(
    cluster: Arc<Cluster>,
    at_ms: u64,
    members: usize,
    for_ms: u64,
    log: Arc<Mutex<Vec<String>>>,
) {
    let note = |s: String| log.lock().unwrap().push(s);
    let Some(lease) = super::node::read_lease(&cluster.env.bucket).await else {
        note(format!("t={at_ms} epoch-outage: no lease yet"));
        return;
    };
    let ids = cluster.ids();
    if !ids.contains(&lease.holder) {
        note(format!(
            "t={at_ms} epoch-outage: holder {} unknown",
            lease.holder
        ));
        return;
    }
    let mut cut: Vec<NodeId> = vec![lease.holder];
    cut.extend(
        ids.iter()
            .copied()
            .filter(|n| *n != lease.holder)
            .take(members.saturating_sub(1)),
    );
    cut.sort_unstable();
    let rest: Vec<NodeId> = ids.iter().copied().filter(|n| !cut.contains(n)).collect();
    note(format!(
        "t={at_ms} epoch outage: {cut:?} lose S3 and P2P to {rest:?} for {for_ms}ms (holder {})",
        lease.holder
    ));
    for n in &cut {
        cluster.env.bucket.set_cut(*n, true);
        for r in &rest {
            cluster.env.bus.set_partition(*n, *r, true);
        }
    }
    let grace = 300u64;
    let mut elapsed = 0u64;
    let mut formed = false;
    let mut frozen = false;
    while elapsed < for_ms {
        tokio::time::sleep(Duration::from_millis(100)).await;
        elapsed += 100;
        if elapsed < grace {
            continue;
        }
        if !formed {
            if let Some(members) = try_form(&cluster, &cut) {
                formed = true;
                note(format!("t={} epoch formed: {members:?}", at_ms + elapsed));
            }
            continue;
        }
        // Liveness: a member that died, or cannot reach another member,
        // freezes the epoch (`EpochMachine::note_live_members`).
        let members = cluster
            .get(cut[0])
            .shared
            .epoch
            .lock()
            .unwrap()
            .members
            .clone();
        let all_live = members.iter().all(|m| {
            cluster.get(*m).alive()
                && members
                    .iter()
                    .all(|o| o == m || cluster.env.bus.linked(*m, *o))
        });
        if all_live == frozen {
            frozen = !all_live;
            for m in &members {
                let node = cluster.get(*m);
                if !node.alive() {
                    continue;
                }
                let changed = {
                    let mut e = node.shared.epoch.lock().unwrap();
                    if e.open {
                        e.frozen = frozen;
                        e.active = !frozen;
                        true
                    } else {
                        false
                    }
                };
                if changed {
                    node.report_epoch();
                }
            }
        }
    }
    note(format!("t={} epoch outage healed", at_ms + for_ms));
    for n in &cut {
        cluster.env.bucket.set_cut(*n, false);
        for r in &rest {
            cluster.env.bus.set_partition(*n, *r, false);
        }
    }
    // A frozen epoch unfreezes once every member is back (the daemon's
    // `note_live_members`), so its non-holders can close after the flush.
    for _ in 0..600 {
        let open: Vec<NodeId> = cut
            .iter()
            .copied()
            .filter(|n| cluster.get(*n).shared.epoch.lock().unwrap().open)
            .collect();
        if open.is_empty() {
            break;
        }
        for n in open {
            let node = cluster.get(n);
            if !node.alive() {
                continue;
            }
            let changed = {
                let mut e = node.shared.epoch.lock().unwrap();
                let all_live = e.members.iter().all(|m| cluster.get(*m).alive());
                if e.frozen && all_live {
                    e.frozen = false;
                    e.active = true;
                    true
                } else {
                    false
                }
            };
            if changed {
                node.report_epoch();
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One formation attempt among the nodes cut from S3.
fn try_form(cluster: &Cluster, cut: &[NodeId]) -> Option<Vec<NodeId>> {
    let roster = cluster.ids();
    let f = cluster.slack as usize;
    let members: Vec<NodeId> = cut
        .iter()
        .copied()
        .filter(|n| cluster.get(*n).alive())
        .collect();
    let quorum = roster.len().checked_sub(f)?.max(1);
    if members.len() < quorum {
        return None;
    }
    let pairwise = members.iter().all(|a| {
        members
            .iter()
            .all(|b| a == b || cluster.env.bus.linked(*a, *b))
    });
    if !pairwise {
        return None;
    }
    let nodes: Vec<_> = members.iter().map(|m| cluster.get(*m)).collect();
    if f > 0
        && !nodes
            .iter()
            .all(|n| n.view().claim.advertised_slack == Some(f as u32))
    {
        return None;
    }
    // The join gates: every member's own promise has expired in its own
    // clock; a member that is not ready undoes the others' gates.
    let mut gated = Vec::new();
    for n in &nodes {
        if n.meta.promise_join_begin(n.clock.now().0).unwrap_or(false) {
            gated.push(n.clone());
        } else {
            for g in &gated {
                let _ = g.meta.promise_join_end();
            }
            return None;
        }
    }
    let acks: Vec<_> = nodes
        .iter()
        .map(|n| {
            let v = n.view();
            (n.id, v.claim.claim(&members), v.claim.known)
        })
        .collect();
    let (carrier, stale_below) = resolve_epoch_claims(&acks);
    for n in &nodes {
        {
            let mut e = n.shared.epoch.lock().unwrap();
            e.open = true;
            e.active = true;
            e.frozen = false;
            e.flushing = false;
            e.base = n.view().applied_seq;
            e.members = members.clone();
            e.carrier = carrier;
            e.stale_below = stale_below;
        }
        n.report_epoch();
    }
    tracing::info!(
        ?members,
        ?carrier,
        stale_below,
        "sim: continuation epoch formed"
    );
    cluster.epochs.lock().unwrap().push(members.clone());
    Some(members)
}

/// Every 25 ms: no continuation-epoch hold alongside another node's
/// usable S3 lease or another hold (plan 30 §M10's `single_authority`).
/// A node's S3 lease is usable while its own clock reads before
/// `expires − margin` (`LeaseView::usable`).
pub async fn sample_authority(cluster: Arc<Cluster>) {
    SAMPLES.with(|s| s.set(0));
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        SAMPLES.with(|s| s.set(s.get() + 1));
        let mut holds = Vec::new();
        let mut leases = Vec::new();
        for id in cluster.ids() {
            let node = cluster.get(id);
            if !node.alive() {
                continue;
            }
            let v = node.view();
            if v.epoch_held {
                holds.push(id);
            }
            if let Some((epoch, expires)) = v.s3_held {
                let margin = (cluster.env.config)(id, 1).expiry_margin_ms as i64;
                if node.clock.now().0 < expires - margin {
                    leases.push((id, epoch));
                }
            }
        }
        if !holds.is_empty() && holds.len() + leases.len() > 1 {
            let what = format!(
                "t={} epoch holds on {holds:?}, usable S3 leases {leases:?}",
                cluster.env.clock.elapsed_ms()
            );
            tracing::error!("{what}");
            cluster.split_brains.lock().unwrap().push(what);
        }
    }
}
