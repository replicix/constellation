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
    drive_epoch(&cluster, at_ms, &cut, for_ms, &note).await;
    note(format!("t={} epoch outage healed", at_ms + for_ms));
    for n in &cut {
        cluster.env.bucket.set_cut(*n, false);
        for r in &rest {
            cluster.env.bus.set_partition(*n, *r, false);
        }
    }
    unfreeze_after_heal(&cluster, &cut).await;
}

/// The fault: see `FaultKind::HolderCutEpoch`. Only the holder loses S3;
/// P2P stays up. The holder proposes an epoch to everyone it reaches,
/// and each asked node joins only by the daemon's member rule
/// (`EpochManager::handle_propose_checked`): a member that reaches S3
/// declines — this is no bucket outage, and a member of an open epoch
/// runs no S3 acquisition, so joining would leave it frozen (EROFS) if
/// the holder then died. `decline: false` is the rule's absence: every
/// live peer joins.
pub async fn holder_cut_epoch(
    cluster: Arc<Cluster>,
    at_ms: u64,
    for_ms: u64,
    decline: bool,
    log: Arc<Mutex<Vec<String>>>,
) {
    let note = |s: String| log.lock().unwrap().push(s);
    let Some(lease) = super::node::read_lease(&cluster.env.bucket).await else {
        note(format!("t={at_ms} holder-cut-epoch: no lease yet"));
        return;
    };
    if !cluster.ids().contains(&lease.holder) {
        return;
    }
    note(format!(
        "t={at_ms} holder {} alone loses S3 for {for_ms}ms (members decline with S3: {decline})",
        lease.holder
    ));
    cluster.env.bucket.set_cut(lease.holder, true);
    let candidates: Vec<NodeId> = if decline {
        // The members the rule admits: the nodes cut from S3.
        cluster
            .ids()
            .into_iter()
            .filter(|n| cluster.env.bucket.is_cut(*n))
            .collect()
    } else {
        cluster.ids()
    };
    drive_epoch(&cluster, at_ms, &candidates, for_ms, &note).await;
    note(format!("t={} holder-cut-epoch healed", at_ms + for_ms));
    cluster.env.bucket.set_cut(lease.holder, false);
    unfreeze_after_heal(&cluster, &candidates).await;
}

/// How often `drive_epoch` tries to form an epoch.
const FORM_EVERY: Duration = Duration::from_millis(100);

/// Form an epoch among `cut` (after a grace, every [`FORM_EVERY`]) and
/// keep its liveness (freeze and thaw) until `for_ms` has passed.
async fn drive_epoch(
    cluster: &Arc<Cluster>,
    at_ms: u64,
    cut: &[NodeId],
    for_ms: u64,
    note: &impl Fn(String),
) {
    let grace = 300u64;
    let mut elapsed = 0u64;
    // The member whose epoch the liveness watch follows: the one formed
    // here, or the open one it continues.
    let mut follow: Option<NodeId> = None;
    // The followed epoch is a continued one: formation is tried again
    // (every closed member keeps proposing, `maybe_propose`), and the
    // carrier's fresh epoch replaces it once the carrier has closed
    // (`try_form`).
    let mut continuing = false;
    let mut frozen = false;
    while elapsed < for_ms {
        tokio::time::sleep(FORM_EVERY).await;
        elapsed += FORM_EVERY.as_millis() as u64;
        if elapsed < grace {
            continue;
        }
        if follow.is_none() || continuing {
            match try_form(cluster, cut) {
                Formation::Formed(members, replaced) => {
                    let how = if replaced {
                        // It met an open epoch, as a continued one did.
                        if !continuing {
                            *cluster.epochs_continued.lock().unwrap() += 1;
                        }
                        *cluster.epochs_superseded.lock().unwrap() += 1;
                        " (the closed carrier's, in place of the open one)"
                    } else {
                        ""
                    };
                    follow = Some(cut[0]);
                    continuing = false;
                    frozen = false;
                    note(format!(
                        "t={} epoch formed{how}: {members:?}",
                        at_ms + elapsed
                    ));
                    continue;
                }
                Formation::Continues(node, members) => {
                    if follow.is_none() {
                        *cluster.epochs_continued.lock().unwrap() += 1;
                        note(format!(
                            "t={} epoch still open, continues: {members:?}",
                            at_ms + elapsed
                        ));
                        follow = Some(node);
                        continuing = true;
                        continue;
                    }
                }
                Formation::Refused => {}
            }
        }
        let Some(followed) = follow else {
            continue;
        };
        // Liveness: a member that died, or cannot reach another member,
        // freezes the epoch (`EpochMachine::note_live_members`).
        let members = cluster
            .get(followed)
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
}

/// After the heal: a frozen epoch unfreezes once every member is back
/// (the daemon's `note_live_members`), so its non-holders can close
/// after the flush.
async fn unfreeze_after_heal(cluster: &Arc<Cluster>, cut: &[NodeId]) {
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

/// What one formation attempt found.
enum Formation {
    /// A new epoch of these members activated; `true`: in place of the
    /// open one of its closed carrier.
    Formed(Vec<NodeId>, bool),
    /// This member's epoch (of these members) is still open: no epoch
    /// forms, and that one goes on.
    Continues(NodeId, Vec<NodeId>),
    /// No epoch formed.
    Refused,
}

/// One formation attempt among the nodes cut from S3.
///
/// A node whose own epoch is still open (promised, active or frozen: not
/// yet closed by its core's `EpochClose`) neither proposes
/// (`EpochManager::maybe_propose` returns while `is_open`) nor joins
/// another (`Machine::persist_promise` refuses a different open epoch,
/// so its ack is not accepted). Back-to-back outages (`locks-blips-tight`)
/// reach the next one before the last epoch closed: re-forming over it
/// replaced its carrier with the new resolution's, `None` (the hold's
/// owner claims nothing while it holds), and its other members, seeing
/// an epoch that carries no lease, closed on S3's return and took the
/// lease over beside the owner's hold (seeds 200 and 8398).
///
/// Except a proposal from the open epoch's carrier once it has closed
/// (`Machine::persist_promise_proposed`, chunk epoch-liveness-gap): every
/// open member that does not own the hold takes it in place of the open
/// epoch. The carrier closed as the hold's owner and was cut from S3
/// again before its re-claim landed; it claims the lease its close let
/// go, so the new epoch carries that lease again and the carrier holds
/// it. Without this, no epoch could form until the re-claim, and the
/// lease and the lock grants it kept lapsed at its expiry.
fn try_form(cluster: &Cluster, cut: &[NodeId]) -> Formation {
    let roster = cluster.ids();
    let f = cluster.slack as usize;
    // A paused node answers no ping and acks no proposal (the daemon's
    // `maybe_propose` counts the nodes whose ping answered): it is no
    // member, like a dead one (`locks-blips-tight-faults` seed 5379: an
    // epoch formed with node 1 paused for 1.9 s carried its held lease,
    // acked in its name; it resumed past that lease's expiry, adopted no
    // hold, and the epoch never closed). Except one paused since the last
    // attempt (`FORM_EVERY`): it may have acked a proposal just before it
    // stopped, and handles the activation when it resumes (flex-crash
    // seed 30908: such a member crashed and restarted into the epoch).
    let members: Vec<NodeId> = cut
        .iter()
        .copied()
        .filter(|n| {
            let n = cluster.get(*n);
            n.alive() && !n.paused_for(FORM_EVERY)
        })
        .collect();
    let open: Vec<NodeId> = members
        .iter()
        .copied()
        .filter(|n| cluster.get(*n).shared.epoch.lock().unwrap().open)
        .collect();
    if let Some(&node) = open.first() {
        // The daemon's member rule, at every open member: an activated
        // epoch (the sim's open epochs all are) whose carrier is the
        // proposer, a closed member, and whose hold is not its own.
        let takes = |n: NodeId, proposer: NodeId| {
            let node = cluster.get(n);
            let e = node.shared.epoch.lock().unwrap().clone();
            (e.active || e.frozen)
                && e.carrier.map(|c| c.node) == Some(proposer)
                && e.members.contains(&proposer)
                && !node.view().epoch_held
        };
        // The proposer is the carrier's daemon itself (`maybe_propose`),
        // so not a paused one (`locks-blips-tight-faults` seed 945: the
        // epoch replaced in the carrier's name 450 ms into a 4.8 s pause).
        let proposer = members
            .iter()
            .copied()
            .filter(|p| !open.contains(p) && !cluster.get(*p).paused())
            .find(|p| open.iter().all(|n| takes(*n, *p)));
        if proposer.is_none() {
            let members = cluster
                .get(node)
                .shared
                .epoch
                .lock()
                .unwrap()
                .members
                .clone();
            return Formation::Continues(node, members);
        }
    }
    let Some(quorum) = roster.len().checked_sub(f).map(|q| q.max(1)) else {
        return Formation::Refused;
    };
    if members.len() < quorum {
        return Formation::Refused;
    }
    let pairwise = members.iter().all(|a| {
        members
            .iter()
            .all(|b| a == b || cluster.env.bus.linked(*a, *b))
    });
    if !pairwise {
        return Formation::Refused;
    }
    let nodes: Vec<_> = members.iter().map(|m| cluster.get(*m)).collect();
    if f > 0
        && !nodes
            .iter()
            .all(|n| n.view().claim.advertised_slack == Some(f as u32))
    {
        return Formation::Refused;
    }
    // The join gates: every member's own promise has expired in its own
    // clock; a member that is not ready undoes the others' gates. An open
    // member holds its gate already, and keeps it.
    let mut gated = Vec::new();
    for n in &nodes {
        if open.contains(&n.id) {
            continue;
        }
        if n.meta.promise_join_begin(n.clock.now().0).unwrap_or(false) {
            gated.push(n.clone());
        } else {
            for g in &gated {
                let _ = g.meta.promise_join_end();
            }
            return Formation::Refused;
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
    // An open member is promised to the new epoch before its activation:
    // open, neither active nor frozen, still with the carrier it knows
    // (the daemon's `EpochManager` between `persist_promise_proposed` and
    // `handle_activate`).
    for n in nodes.iter().filter(|n| open.contains(&n.id)) {
        {
            let mut e = n.shared.epoch.lock().unwrap();
            e.active = false;
            e.frozen = false;
        }
        n.report_epoch();
    }
    for n in &nodes {
        {
            let mut e = n.shared.epoch.lock().unwrap();
            if !e.open {
                // The driver keeps an open epoch's base (`report_epoch`).
                e.base = n.view().applied_seq;
            }
            e.open = true;
            e.active = true;
            e.frozen = false;
            e.flushing = false;
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
    Formation::Formed(members, !open.is_empty())
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
