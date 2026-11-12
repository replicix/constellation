//! ADR-51: a takeover carries every kept row of the tenure it ends.
//!
//! An epoch member's `StreamAheadAck` makes its copy of the hold owner's row
//! committed: the owner answered its client on it. When another node takes
//! the tenure over (a TTL takeover by promise check, a retirement fence), the
//! member's copy would be carried only if that member claimed. So every
//! marker takeover runs one more phase, **after its promise check** and
//! before its tail to head: [`Core::begin_kept_fetch`] asks the previous
//! holder and every write-eligible, linked or heard peer for the committed
//! rows they keep of the tenure's hold streams (`SealTail { kept }`, member
//! side [`Core::seal_kept_hold`]), sealing those streams at each member, and
//! completes when
//!
//! - **R1:** every node that any answer names as a member of an epoch that
//!   carried the tenure has answered, and
//! - **R2:** the previous holder answered (it is a member of every epoch
//!   carrying its tenure, so its `members` is the whole evidence), or the
//!   answerers that the roster lists (and this node) number `f + 1`, `f`
//!   being the promise check's effective slack (an epoch carrying the tenure
//!   has at least `N - f` members: the M10 intersection argument), or every
//!   source answered.
//!
//! A member that is down makes the claim wait (the rows were acknowledged):
//! the attempt ends, the ops behind it retry, `waiting_for_group` names the
//! sources. The operator abandons the rest (`repair abandon --epoch`, the
//! claim carries what answered and its marker says so) or retires the member.
//!
//! The union is merged per epoch, by journal seq and identity, **after** the
//! claim read the log to head ([`Core::kept_install`]), written as a sealed
//! backup tail of the tenure, and re-shipped by the takeover gate verbatim,
//! before the claimant's first write. A lease that names the union
//! (`InheritedTail::kept`) is collected the same way by a later claimant,
//! from every member it names.

use super::lease::Plan;
use super::seal_tail::{FetchedTail, KeptFields, KeptSeal};
use super::Core;
use crate::action::{Action, S3Op};
use crate::event::{PeerMsg, SealRefusal};
use crate::ids::{Epoch, NodeId, Now, OpId, TimerId};
use crate::replica::Replica;
use constellation_meta::{BackupRole, BackupTx};
use replicix_objcas::Lease;
use std::collections::{BTreeMap, BTreeSet};

/// One source's answer about one stream epoch, batch by batch.
#[derive(Debug, Default, Clone)]
pub(crate) struct KeptAnswer {
    pub txs: Vec<BackupTx>,
    pub through: u64,
    pub members: Vec<Vec<NodeId>>,
    pub sealed: Vec<(Epoch, NodeId)>,
    /// The last batch is in.
    pub done: bool,
}

/// A group of rows a claim must carry: the hold streams of one tenure.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    /// The tenure `(holder, lease epoch)`.
    pub tenure: (NodeId, Epoch),
    /// The union a lease names (`InheritedTail::kept`): its members and
    /// `through`. `None`: the previous holder's own tenure.
    pub union: Option<(Vec<NodeId>, u64)>,
    /// The claimant is the tenure's holder (the carrier re-adopting its own
    /// lease after it handed its hold over): its rows of the tenure's own
    /// epoch are in its journal, only the transferee's streams one epoch up
    /// are collected.
    pub own: bool,
}

impl Target {
    /// The stream epochs collected for the tenure: its own, and the one a
    /// hold handed to a transferee journals under (`epoch_hold_epoch_for`).
    pub(crate) fn epochs(&self) -> Vec<Epoch> {
        if self.own {
            vec![self.tenure.1 + 1]
        } else {
            vec![self.tenure.1, self.tenure.1 + 1]
        }
    }

    /// The epochs a node's answer must cover: the previous holder is asked
    /// for its own journal of the tenure's epoch only.
    fn required(&self, ti: usize, node: NodeId) -> Vec<Epoch> {
        if ti == 0 && node == self.tenure.0 && !self.own {
            vec![self.tenure.1]
        } else {
            self.epochs()
        }
    }
}

/// One attempt of a claim to collect the kept rows.
#[derive(Debug)]
pub(crate) struct KeptFetch {
    pub prev: Lease,
    pub targets: Vec<Target>,
    /// Everyone asked with `kept: true` (not the previous holder, which is
    /// asked plainly; not this node, not a retired one).
    pub sources: BTreeSet<NodeId>,
    /// Requests in flight (a first request or a next batch), by id:
    /// `(target, node, epoch)`.
    pub asked: BTreeMap<OpId, (usize, NodeId, Epoch)>,
    pub answers: BTreeMap<(usize, NodeId, Epoch), KeptAnswer>,
    /// `f`, from the promise check or a heartbeat read of its own (`None`:
    /// not known).
    pub slack: Option<u32>,
    /// The operator abandoned the sources that have not answered: the fetch
    /// completes with what answered, once this node's own copy is in.
    pub abandon: bool,
    /// `--force`: two sources that kept different transactions at one seq
    /// are resolved by the longest.
    pub force: bool,
    pub timer: Option<TimerId>,
}

/// A completed fetch awaiting the log read to head and the merge.
#[derive(Debug)]
pub(crate) struct KeptUnion {
    pub prev: Lease,
    pub targets: Vec<Target>,
    pub answers: BTreeMap<(usize, NodeId, Epoch), KeptAnswer>,
    pub partial: bool,
    pub force: bool,
}

impl Core {
    /// Whether a marker takeover of `prev` runs the kept phase. Not for the
    /// claim of a lease nobody held or that was released (a release follows
    /// a flush), a node with P2P off (no epoch, hold or stream exists, and no
    /// peer message is sent), the negative control, the re-claim of the hold owner that
    /// closed the epoch, and a union already installed by an earlier pass of
    /// this claim (the promise re-check). This node's own lease (a restart's
    /// re-adoption, the carrier's re-claim) serves from its journal, which
    /// holds every row of the hold it ran itself, but not those of a hold it
    /// handed over: [`Self::carrier_handed_over`] runs the phase for the
    /// transferee's streams.
    pub(crate) fn kept_needed(&self, prev: &Lease) -> bool {
        !self.cfg.kept_fetch_skipped
            && self.cfg.p2p
            && prev.holder != 0
            && (prev.holder != self.cfg.node_id || self.carrier_handed_over(prev))
            && !prev.released
            && !self.owner_reclaims(prev)
            && self.bk.kept_ready != Some(prev.renewal_key())
    }

    /// This node holds the lease `prev` and handed the hold of the epoch
    /// that carried it to a peer (`hold_ended`'s flag is false): the peer's
    /// rows at the next epoch are on the epoch's members, not in this node's
    /// journal.
    fn carrier_handed_over(&self, prev: &Lease) -> bool {
        prev.holder == self.cfg.node_id
            && self.pr.hold_ended == Some((prev.epoch, prev.expires_unix_ms, false))
    }

    /// The targets of a claim of `prev`: the hold streams of its own tenure
    /// (a fence names the tenure it ended), and the union its lease names.
    fn kept_targets(prev: &Lease, me: NodeId) -> Vec<Target> {
        let mut out = vec![Target {
            tenure: (prev.holder, prev.retired_tenure().unwrap_or(prev.epoch)),
            union: None,
            own: prev.holder == me,
        }];
        if let Some(t) = prev.inherited_tail.as_ref().filter(|t| t.kept) {
            out.push(Target {
                tenure: (t.holder, t.epoch),
                union: Some((t.members.clone(), t.through)),
                own: false,
            });
        }
        out
    }

    /// Begin an attempt to collect the kept rows (the promise check passed,
    /// or none was needed). Ends in the claim's tail to head.
    pub(crate) fn begin_kept_fetch(
        &mut self,
        now: Now,
        prev: Lease,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.cfg.node_id;
        // The roster must be a complete read issued after this node first
        // saw `prev` at its current key: every member registered before its
        // epoch formed, and the epoch formed before the lease expired or was
        // fenced, so such a read lists every member.
        let seen = match self.lease.observed_since(&prev) {
            Some(at) => at,
            None => match self.bk.kept_seen {
                Some((e, at)) if e == prev.epoch => at,
                _ => {
                    self.bk.kept_seen = Some((prev.epoch, now.wait));
                    now.wait
                }
            },
        };
        if !self.roster.fresh_since(seen) {
            // Ask for a read and wait for it in this attempt (ending it would
            // leave the retry to the claim's own cadence, a whole backup
            // takeover window for a sealing backup): the phase resumes when a
            // read issued after `seen` is installed, or the attempt ends with
            // the window.
            out.push(Action::RefreshRoster);
            self.tail_fetch_phase();
            let round = self.op_id();
            let at = now.wait.plus(self.cfg.backup_takeover_ms.max(1));
            let timer = self.set_timer_wait(at, super::Timer::JobRequestTimeout(round), out);
            self.bk.tf_round = Some(round);
            self.bk.kept_roster_wait = Some((prev, timer));
            return;
        }
        let targets = Self::kept_targets(&prev, me);
        let mut sources: BTreeSet<NodeId> = self.roster.kept_sources(me);
        for t in &targets {
            if let Some((members, _)) = &t.union {
                sources.extend(members.iter().copied());
            }
        }
        sources.retain(|n| *n != me && *n != prev.holder && !prev.retired.contains(n));
        let abandon = self
            .bk
            .abandon
            .is_some_and(|e| targets.iter().any(|t| t.tenure.1 == e) || e == prev.epoch);
        let force = std::mem::take(&mut self.bk.kept_force);
        self.open_group_wait(now, &prev, sources.iter().copied().collect());
        let tenure_epoch = targets[0].tenure.1;
        if let Some(w) = self.bk.group_wait.as_mut() {
            w.kept = Some(tenure_epoch);
        }
        let slack = self.pr.check_slack.take();
        let mut k = KeptFetch {
            prev: prev.clone(),
            targets,
            sources,
            asked: BTreeMap::new(),
            answers: BTreeMap::new(),
            slack,
            abandon,
            force,
            timer: None,
        };
        // This node's own copy, answered in place (it seals the streams).
        let key = prev.renewal_key();
        for (ti, t) in k.targets.iter().enumerate() {
            for e in t.epochs() {
                match self.seal_kept_hold(t.tenure, e, &key, false, replica) {
                    KeptSeal::Held {
                        txs,
                        through,
                        members,
                        sealed,
                    } => {
                        k.answers.insert(
                            (ti, me, e),
                            KeptAnswer {
                                txs,
                                through,
                                members,
                                sealed,
                                done: true,
                            },
                        );
                    }
                    KeptSeal::Refused(why) => {
                        match why {
                            SealRefusal::EpochOpen => self.stats.seal_kept_refused_epoch_open += 1,
                            SealRefusal::StaleKey => self.stats.seal_kept_refused_stale_key += 1,
                        }
                        self.bk.seal_unreadable = false;
                        self.end_acquire_attempt(now, replica, out);
                        return;
                    }
                    KeptSeal::Unreadable => {
                        // (This node cannot answer yet; the attempt waits out
                        // its window and asks again.)
                        self.bk.seal_unreadable = false;
                        self.stats.seal_tail_unreadable += 1;
                    }
                }
            }
        }
        self.stats.kept_fetches += 1;
        self.bk.kept = Some(k);
        // Nothing to ask when this node's own rows already satisfy the rules
        // (no epoch ever carried the tenure and `f` is known): no round trip.
        if self.kept_complete() {
            // (The previous holder is still told: its plain seal deposes a
            // live-but-expired holder before the claim's CAS, whatever the
            // rules need of its answer, which is not waited for.)
            if prev.holder != me && !prev.retired.contains(&prev.holder) {
                let epoch = self
                    .bk
                    .kept
                    .as_ref()
                    .map_or(prev.epoch, |k| k.targets[0].tenure.1);
                self.kept_ask(0, prev.holder, epoch, false, 0, out);
            }
            self.kept_finish(now, replica, out);
            return;
        }
        self.kept_rearm(now, out);
        self.stats.tail_fetch_attempts += 1;
        self.tail_fetch_phase();
        let k = self.bk.kept.as_ref().expect("set above");
        let mut asks: Vec<(usize, NodeId, Epoch, bool)> = Vec::new();
        let holder_asked = !prev.retired.contains(&prev.holder) && prev.holder != me;
        for (ti, t) in k.targets.iter().enumerate() {
            if ti == 0 && holder_asked {
                asks.push((ti, prev.holder, t.tenure.1, false));
            }
            let nodes: BTreeSet<NodeId> = match &t.union {
                // The union's members, the previous holder (the claimant that
                // wrote it) among them.
                Some((members, _)) => members
                    .iter()
                    .copied()
                    .filter(|n| *n != me && !prev.retired.contains(n))
                    .collect(),
                None => k.sources.clone(),
            };
            for n in nodes {
                for e in t.epochs() {
                    asks.push((ti, n, e, true));
                }
            }
        }
        let needs_slack = k.slack.is_none() && !self.cfg.kept_fetch_skipped;
        for (ti, node, e, kept) in asks {
            self.kept_ask(ti, node, e, kept, 0, out);
        }
        if needs_slack {
            // No promise check ran: one heartbeat read of its own for `f`.
            self.issue_s3(S3Op::HeartbeatRead, super::S3For::KeptSlack, out);
        }
    }

    /// A roster read was installed: a claim that waits for a read issued
    /// after it first saw the lease goes on once this one is.
    pub(crate) fn kept_roster_arrived(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some((prev, _)) = self.bk.kept_roster_wait.as_ref() else {
            return;
        };
        let live = self.job.is_some()
            && matches!(&self.pending_plan, Some(Plan::Claim { prev: p, .. }) if p.renewal_key() == prev.renewal_key());
        let seen = self
            .bk
            .kept_seen
            .filter(|(e, _)| *e == prev.epoch)
            .map(|(_, at)| at);
        let ready = match (live, seen.or_else(|| self.lease.observed_since(prev))) {
            (true, Some(at)) => self.roster.fresh_since(at),
            _ => false,
        };
        if !ready {
            return;
        }
        let Some((prev, timer)) = self.bk.kept_roster_wait.take() else {
            return;
        };
        self.cancel_timer(timer, out);
        self.bk.tf_round = None;
        self.begin_kept_fetch(now, prev, replica, out);
    }

    /// One request of the fetch. `kept`: a member's (sealing); else the
    /// previous holder's plain seal.
    fn kept_ask(
        &mut self,
        target: usize,
        node: NodeId,
        epoch: Epoch,
        kept: bool,
        from: u64,
        out: &mut Vec<Action>,
    ) {
        let req = self.op_id();
        let closed_hold = kept
            && self
                .bk
                .kept
                .as_ref()
                .is_some_and(|k| self.owner_closed(&k.prev));
        let Some(k) = self.bk.kept.as_mut() else {
            return;
        };
        k.asked.insert(req, (target, node, epoch));
        let tenure = if kept {
            k.targets[target].tenure
        } else {
            (0, 0)
        };
        out.push(Action::Send {
            to: node,
            msg: PeerMsg::SealTail {
                req,
                epoch,
                config_version: k.prev.config_version,
                from,
                key: k.prev.renewal_key(),
                kept,
                tenure,
                closed_hold,
            },
        });
    }

    /// The heartbeat read of a fetch that ran no promise check: the
    /// effective slack `f`.
    pub(crate) fn on_kept_heartbeats(
        &mut self,
        now: Now,
        result: crate::S3Result,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(k) = self.bk.kept.as_mut() else {
            return;
        };
        if let crate::S3Result::Heartbeats(Ok(hb)) = &result {
            k.slack = Some(constellation_store_s3::heartbeat::effective_slack(
                self.cfg.epoch_slack,
                self.roster.write_eligible(),
                hb,
            ));
        }
        self.kept_progress(now, replica, out);
    }

    /// A member's (or the previous holder's) batch of a kept fetch. `true`:
    /// it was one.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_kept_sealed_tail(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        epoch: Epoch,
        (held, empty): (bool, bool),
        (txs, through, next): (Vec<BackupTx>, u64, Option<u64>),
        KeptFields {
            members,
            sealed,
            refused,
        }: KeptFields,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(k) = self.bk.kept.as_mut() else {
            return false;
        };
        let Some((ti, node, e)) = k.asked.remove(&req) else {
            return true;
        };
        if node != from || epoch != e {
            // (Not an answer of the asked node about the asked epoch: it
            // counts for nothing; the window ends the attempt.)
            return true;
        }
        if let Some(why) = refused {
            // A refusal ends the attempt (the member's epoch carries the
            // tenure, or a renewal it saw makes the claim moot); the ops
            // retry and the promise check refuses first while the epoch lives.
            match why {
                SealRefusal::EpochOpen => tracing::warn!(
                    node = self.cfg.node_id,
                    from,
                    "a member's open epoch carries the tenure: the claim ends and retries"
                ),
                SealRefusal::StaleKey => tracing::debug!(
                    node = self.cfg.node_id,
                    from,
                    "a member saw a newer renewal of the lease: the claim ends and retries"
                ),
            }
            self.end_tail_fetch_attempt(now, replica, out);
            return true;
        }
        // The previous holder: a node that holds nothing (`empty`) is an
        // answer of no rows, whose members still count as evidence.
        let plain_none = !held && empty && node == k.prev.holder;
        if !held && !plain_none && node == k.prev.holder {
            // The previous holder's plain refusal (a renewal landed after the
            // key this claim read: the CAS cannot land): the attempt ends at
            // once, as the members' refusals do, rather than waiting out its
            // window for a claim that cannot succeed.
            tracing::debug!(
                node = self.cfg.node_id,
                from,
                "the previous holder refused the seal (a newer renewal): the claim ends and retries"
            );
            self.end_tail_fetch_attempt(now, replica, out);
            return true;
        }
        if !held && !plain_none {
            return true;
        }
        let a = k.answers.entry((ti, from, e)).or_default();
        a.txs.extend(txs);
        a.through = a.through.max(through);
        if a.members.is_empty() {
            a.members = members;
        }
        a.sealed.extend(sealed);
        match next {
            Some(n) if !plain_none => {
                // Each batch of a member that answers gets a window of its
                // own.
                let kept = node != k.prev.holder;
                self.kept_ask(ti, from, epoch, kept, n, out);
                self.kept_rearm(now, out);
            }
            _ => {
                a.done = true;
                self.kept_rearm(now, out);
                self.kept_progress(now, replica, out);
            }
        }
        true
    }

    fn kept_rearm(&mut self, now: Now, out: &mut Vec<Action>) {
        let old = self.bk.kept.as_mut().and_then(|k| k.timer.take());
        if let Some(t) = old {
            self.cancel_timer(t, out);
        }
        let round = self.op_id();
        let at = now.wait.plus(self.cfg.backup_takeover_ms.max(1));
        let timer = self.set_timer_wait(at, super::Timer::JobRequestTimeout(round), out);
        self.bk.tf_round = Some(round);
        if let Some(k) = self.bk.kept.as_mut() {
            k.timer = Some(timer);
        }
    }

    /// The nodes whose answers are all in, for target `ti`.
    fn kept_done(k: &KeptFetch, ti: usize) -> BTreeSet<NodeId> {
        let t = &k.targets[ti];
        let mut nodes: BTreeSet<NodeId> = k.answers.keys().map(|(_, n, _)| *n).collect();
        nodes.retain(|n| {
            t.required(ti, *n)
                .iter()
                .all(|e| k.answers.get(&(ti, *n, *e)).is_some_and(|a| a.done))
        });
        nodes
    }

    /// Whether the fetch holds everything the rules ask for.
    fn kept_complete(&self) -> bool {
        let Some(k) = self.bk.kept.as_ref() else {
            return false;
        };
        let me = self.cfg.node_id;
        let prev = &k.prev;
        (0..k.targets.len()).all(|ti| {
            let t = &k.targets[ti];
            let done = Self::kept_done(k, ti);
            if !done.contains(&me) {
                return false;
            }
            if let Some((members, through)) = &t.union {
                // The union a lease names: every named member, or this
                // node's own copy through what the lease says it holds.
                let mine = k
                    .answers
                    .get(&(ti, me, t.tenure.1))
                    .map_or(0, |a| a.through);
                return members
                    .iter()
                    .filter(|n| **n != me && !prev.retired.contains(n))
                    .all(|n| done.contains(n))
                    || (*through > 0 && mine >= *through);
            }
            // R1: everyone the answers name has answered (but the tenure's
            // holder: a committed row sits on a member that acknowledged it,
            // never only on the holder; its own answer is R2's shortcut).
            let named: BTreeSet<NodeId> = k
                .answers
                .iter()
                .filter(|((i, _, _), _)| *i == ti)
                .flat_map(|(_, a)| a.members.iter().flatten().copied())
                .filter(|n| *n != me && *n != t.tenure.0 && !prev.retired.contains(n))
                .collect();
            if !named.iter().all(|n| done.contains(n)) {
                return false;
            }
            // R2: the carrier answered, or the answerers intersect every
            // possible epoch, or every source answered.
            let holder_in = !prev.retired.contains(&prev.holder) && done.contains(&prev.holder);
            let all_sources = k.sources.iter().all(|n| done.contains(n))
                && (prev.retired.contains(&prev.holder) || done.contains(&prev.holder));
            let q = done
                .iter()
                .filter(|n| self.roster.lists(**n))
                .filter(|n| !prev.retired.contains(n))
                .count();
            holder_in || all_sources || k.slack.is_some_and(|f| q > f as usize)
        })
    }

    /// Ask the members the answers name that this fetch has not asked, and
    /// finish once everything is in.
    fn kept_progress(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let me = self.cfg.node_id;
        let Some(k) = self.bk.kept.as_mut() else {
            return;
        };
        // Members the answers name that are not sources yet.
        let more: Vec<(usize, NodeId)> = k
            .answers
            .iter()
            .flat_map(|((ti, _, _), a)| a.members.iter().flatten().map(move |n| (*ti, *n)))
            .filter(|(ti, n)| {
                *n != me
                    && *n != k.prev.holder
                    && !k.prev.retired.contains(n)
                    && k.targets[*ti].union.is_none()
                    && !k.sources.contains(n)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let epochs: Vec<(usize, Vec<Epoch>)> = k
            .targets
            .iter()
            .enumerate()
            .map(|(i, t)| (i, t.epochs()))
            .collect();
        for (_, n) in &more {
            k.sources.insert(*n);
        }
        // The waiting claim names whom it still waits for.
        let waiting: Vec<NodeId> = k
            .sources
            .iter()
            .copied()
            .filter(|n| {
                !(0..k.targets.len()).all(|ti| {
                    Self::kept_done(k, ti).contains(n)
                        || k.targets[ti]
                            .union
                            .as_ref()
                            .is_some_and(|(m, _)| !m.contains(n))
                })
            })
            .collect();
        let asked_nodes: BTreeSet<NodeId> = more.iter().map(|(_, n)| *n).collect();
        for n in asked_nodes {
            for (ti, es) in &epochs {
                if self
                    .bk
                    .kept
                    .as_ref()
                    .is_some_and(|k| k.targets[*ti].union.is_none())
                {
                    for e in es {
                        self.kept_ask(*ti, n, *e, true, 0, out);
                    }
                }
            }
        }
        if let Some(w) = self.bk.group_wait.as_mut() {
            w.sources = waiting;
        }
        if self.kept_complete() {
            self.kept_finish(now, replica, out);
        }
    }

    /// Every rule holds (or the operator abandoned the rest): the claim goes
    /// on to read the log to head, and merges after it.
    fn kept_finish(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        let Some(k) = self.bk.kept.take() else {
            return;
        };
        if let Some(t) = k.timer {
            self.cancel_timer(t, out);
        }
        self.bk.tf_round = None;
        let partial = k.abandon && {
            let me_done = |ti: usize| Self::kept_done(&k, ti);
            k.sources.iter().any(|n| {
                (0..k.targets.len()).any(|ti| {
                    k.targets[ti]
                        .union
                        .as_ref()
                        .is_none_or(|(m, _)| m.contains(n))
                        && !me_done(ti).contains(n)
                })
            })
        };
        let waited = self
            .bk
            .group_wait
            .as_ref()
            .map_or(0, |w| now.wait.since(w.since).max(0));
        tracing::info!(
            node = self.cfg.node_id,
            tenure = ?k.targets[0].tenure,
            sources = k.sources.len(),
            waited_ms = waited,
            partial,
            "collected the kept rows of the tenure: the claim carries them"
        );
        let _ = replica;
        self.bk.kept_union = Some(KeptUnion {
            prev: k.prev,
            targets: k.targets,
            answers: k.answers,
            partial,
            force: k.force,
        });
        self.kept_to_tail(out);
    }

    /// The operator abandoned the sources still unanswered (`Control::Abandon`
    /// while a kept fetch waits): complete with what answered.
    pub(crate) fn kept_abandon_now(
        &mut self,
        now: Now,
        force: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.cfg.node_id;
        let Some(k) = self.bk.kept.as_mut() else {
            return;
        };
        k.abandon = true;
        k.force |= force;
        // This node's own copy must be in: it is the one source that cannot
        // be abandoned.
        let mine = (0..k.targets.len()).all(|ti| Self::kept_done(k, ti).contains(&me));
        if mine {
            self.kept_finish(now, replica, out);
        }
    }

    /// The window of the fetch ran out: an abandoned one completes with what
    /// answered; the others end the attempt (the ops behind the claim retry).
    pub(crate) fn on_kept_window_ended(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let me = self.cfg.node_id;
        let Some(k) = self.bk.kept.as_mut() else {
            return;
        };
        k.timer = None;
        let mine = (0..k.targets.len()).all(|ti| Self::kept_done(k, ti).contains(&me));
        if k.abandon && mine {
            self.kept_finish(now, replica, out);
        } else {
            self.end_tail_fetch_attempt(now, replica, out);
        }
    }

    /// No answer from this source: nothing to do until the window ends.
    pub(crate) fn on_kept_failed(&mut self, req: OpId) -> bool {
        self.bk
            .kept
            .as_mut()
            .is_some_and(|k| k.asked.remove(&req).is_some())
    }

    /// The claim read the log to head: merge the union per epoch, write it as
    /// a sealed backup tail of the tenure, and name it for the gate. `false`:
    /// the attempt ended (two sources kept different transactions at one seq;
    /// the store could not be written).
    pub(crate) fn kept_install(
        &mut self,
        now: Now,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(u) = self.bk.kept_union.take() else {
            return true;
        };
        let me = self.cfg.node_id;
        // Per epoch: every answer's transactions, by their first seq.
        let mut epochs: BTreeSet<Epoch> = BTreeSet::new();
        for t in &u.targets {
            epochs.extend(t.epochs());
        }
        // What the union an earlier claim inherited carried of its epoch: in
        // the log under the claimant's `TailOf` identity, which the log's
        // native rows do not show.
        let carried_through = |e: Epoch| -> u64 {
            u.targets
                .iter()
                .filter(|t| t.tenure.1 == e)
                .filter_map(|t| t.union.as_ref().map(|(_, through)| *through))
                .max()
                .unwrap_or(0)
        };
        let mut merged_by_epoch: BTreeMap<Epoch, Vec<BackupTx>> = BTreeMap::new();
        let mut owner_of: BTreeMap<Epoch, NodeId> = BTreeMap::new();
        let mut keepers: BTreeMap<Epoch, BTreeSet<NodeId>> = BTreeMap::new();
        let mut past_gap = 0usize;
        let mut forced = false;
        for e in &epochs {
            let mut all: BTreeMap<u64, &BackupTx> = BTreeMap::new();
            for (ti, t) in u.targets.iter().enumerate() {
                if !t.epochs().contains(e) {
                    continue;
                }
                owner_of.entry(*e).or_insert(t.tenure.0);
                for ((i, node, ae), a) in &u.answers {
                    if *i != ti || ae != e {
                        continue;
                    }
                    if !a.txs.is_empty() {
                        keepers.entry(*e).or_default().insert(*node);
                    }
                    for tx in &a.txs {
                        // ADR-59 part B (D2): a copy of a transaction a
                        // `DropHeld` of the log retired is dropped before the
                        // sources are compared (a member that missed the
                        // record serves it, a member that applied it holes
                        // it): the sources do not disagree about a row that
                        // was never to land, and the row above it is not
                        // past a gap (`present` below).
                        if !tx.records.is_empty() {
                            let ident = constellation_meta::tail_identity(&tx.records)
                                .unwrap_or((*e, tx.first));
                            if replica.row_retired(ident.0, ident.1) {
                                continue;
                            }
                        }
                        match all.get(&tx.first) {
                            Some(old)
                                if old.last != tx.last
                                    || old.origin != tx.origin
                                    || constellation_meta::tail_identity(&old.records)
                                        != constellation_meta::tail_identity(&tx.records) =>
                            {
                                if u.force {
                                    forced = true;
                                    self.stats.kept_forced += 1;
                                    if tx.last > old.last {
                                        all.insert(tx.first, tx);
                                    }
                                    continue;
                                }
                                self.stats.kept_diverged += 1;
                                tracing::error!(
                                    node = me,
                                    from = node,
                                    epoch = e,
                                    first = tx.first,
                                    "two sources kept different transactions at the same journal \
                                     seq: the claim does not complete (operator: `repair abandon \
                                     --epoch --force` takes the longest)"
                                );
                                self.bk.kept_force_wanted = Some(u.targets[0].tenure.1);
                                self.end_tail_fetch_attempt(now, replica, out);
                                return false;
                            }
                            Some(_) => {}
                            None => {
                                all.insert(tx.first, tx);
                            }
                        }
                    }
                }
            }
            // The rows past the log, from the lowest seq of the epoch the log
            // lacks, in order, up to the first seq no answer covers.
            let mut merged: Vec<BackupTx> = Vec::new();
            let mut next: Option<u64> = None;
            let mut gap = false;
            // Seq `s` is present for the gap rule: in the log, or retired by
            // a `DropHeld` (it will never be in the log, and no row above it
            // waits for it).
            // (A retired transaction covers `s` if it starts at or below it
            // and no copy starts in between: its span is not known, its
            // successors' starts are.)
            let starts: BTreeSet<u64> = all.keys().copied().collect();
            let present = |s: u64| -> bool {
                replica.row_in_log(*e, s)
                    || replica
                        .retired_at_or_below(*e, s)
                        .is_some_and(|a| starts.range(a + 1..=s).next().is_none())
            };
            for (first, tx) in all {
                if gap {
                    past_gap += 1;
                    continue;
                }
                if replica.row_in_log(*e, tx.last) {
                    next = Some(tx.last + 1);
                    continue;
                }
                match next {
                    // The first row past the log starts where the log ends:
                    // under an abandon the member that held the prefix may be
                    // the one that is down, and a copy past that gap would
                    // make the marker name a hold the log does not reach.
                    None if first == 1
                        || present(first - 1)
                        || first - 1 <= carried_through(*e) => {}
                    None => {
                        gap = true;
                        past_gap += 1;
                        continue;
                    }
                    Some(n) if first <= n => {
                        if first < n {
                            continue;
                        }
                    }
                    // (The log carries the union an earlier claim inherited
                    // through `carried_through` under the `TailOf` identity,
                    // which `row_in_log` does not show either: a seq between
                    // two copies that lies within it is covered, as a first
                    // row's predecessor is.)
                    Some(_)
                        if first > 0
                            && (present(first - 1) || first - 1 <= carried_through(*e)) => {}
                    Some(_) => {
                        gap = true;
                        past_gap += 1;
                        continue;
                    }
                }
                next = Some(tx.last + 1);
                merged.push(tx.clone());
            }
            merged_by_epoch.insert(*e, merged);
        }
        // The source of a stream with rows owns its epoch's tail (a hold
        // handed to a transferee journaled under the epoch above the
        // tenure's, as the transferee's stream); and the claim lands above
        // every epoch a hold journaled under.
        let mut floor = 0;
        for a in u.answers.values() {
            for (e, s) in &a.sealed {
                floor = floor.max(*e);
                if *e != u.targets[0].tenure.1 {
                    let o = owner_of.entry(*e).or_insert(*s);
                    *o = (*o).min(*s);
                }
            }
        }
        // The rows below are judged against the log (`row_in_log`), which
        // knows only the epoch it was last applied to. Once the tail to head
        // has brought in a segment of the epoch this claim would write (or a
        // higher one), another claimant has won the lease since this attempt
        // read it: the tenure's rows read as missing from a log that has
        // moved on, whatever they are, and this claim's CAS can only fail on
        // the lease it read. The attempt ends; the next one reads the new
        // lease. (Not for the carrier re-adopting its own lease, which lands
        // at the epoch it held.)
        let lands = u.prev.epoch.max(floor) + 1;
        if past_gap > 0
            && !u.partial
            && !u.force
            && !u.targets[0].own
            && replica.applied_epoch() >= lands
        {
            self.stats.kept_overtaken += 1;
            tracing::info!(
                node = me,
                prev_epoch = u.prev.epoch,
                lands,
                log_epoch = replica.applied_epoch(),
                rows = past_gap,
                "the log is at the epoch this claim would write: another claimant won the lease \
                 while the kept rows were collected, the attempt ends"
            );
            self.end_tail_fetch_attempt(now, replica, out);
            return false;
        }
        self.bk.kept_floor = floor;
        self.bk.gate_tails_from = u.targets.iter().map(|t| t.tenure.1).min().unwrap_or(0);
        debug_assert!(
            past_gap == 0 || u.partial || u.force,
            "a kept copy lies past a gap"
        );
        if past_gap > 0 {
            self.stats.kept_past_gap += past_gap as u64;
            tracing::warn!(
                node = me,
                rows = past_gap,
                "kept rows lie past a seq no source covers: not carried"
            );
        }
        let prev = u.prev;
        // One entry per epoch with rows; with none anywhere nothing is
        // installed (the claim goes on as one with no tail to carry: its
        // marker says so, as before the ADR).
        merged_by_epoch.retain(|_, m| !m.is_empty());
        let mut fetched: Vec<FetchedTail> = Vec::new();
        for (e, merged) in &merged_by_epoch {
            if !replica.backup_append(*e, merged) {
                self.end_acquire_attempt(now, replica, out);
                return false;
            }
            let holder = owner_of.get(e).copied().unwrap_or(prev.holder);
            let role = BackupRole {
                holder,
                epoch: *e,
                config_version: prev.config_version,
            };
            if let Err(error) = replica.set_backup_role(role) {
                tracing::error!(node = me, %error, "the kept tail's role could not be persisted: the claim ends");
                self.end_acquire_attempt(now, replica, out);
                return false;
            }
            if !merged.is_empty() {
                if !replica.backup_seal(*e) {
                    self.end_acquire_attempt(now, replica, out);
                    return false;
                }
                self.bk.sealed = self.bk.sealed.max(*e);
            }
            self.bk.role = Some(role);
            self.bk.acked = replica.backup_acked(*e).unwrap_or(0);
            // The union is held by every node that kept a row of it, and by
            // this one from here on; a later claimant asks them all.
            let mut members: Vec<NodeId> = keepers
                .get(e)
                .map(|s| s.iter().copied().collect())
                .unwrap_or_default();
            members.push(me);
            members.sort_unstable();
            members.dedup();
            fetched.push(FetchedTail {
                epoch: *e,
                config_version: prev.config_version,
                members,
                // The end of the merged prefix: the rows the log already
                // carries make the tail contiguous across them, which the
                // store's own count of it (from the oldest row held) is not.
                through: merged.last().map_or(0, |t| t.last),
                kept_owner: Some(holder),
            });
            self.stats.kept_rows_carried += merged.len() as u64;
        }
        if !fetched.is_empty() {
            self.bk.held.clear();
            // An inherited tail of an epoch the union does not cover (a T3
            // non-kept tail below the tenure's epochs) is re-shipped with it.
            let mut all = fetched;
            for old in std::mem::take(&mut self.bk.fetched) {
                if !all.iter().any(|f| f.epoch == old.epoch) {
                    all.push(old);
                }
            }
            all.sort_by_key(|f| f.epoch);
            self.bk.fetched = all;
            self.stats.tail_fetches += 1;
            if prev.inherited_tail.is_some() {
                self.stats.inherited_tail_fetches += 1;
            }
        }
        self.stats.kept_fetches_completed += 1;
        if u.partial || forced {
            let e = u.targets[0].tenure.1;
            self.stats.kept_abandoned += 1;
            self.stats.groups_abandoned += 1;
            self.bk.abandoned_claim = Some(e);
            // What the claim carries of the abandoned epoch: a copy at or
            // below it retires on carry, the ones past it replay by rid.
            self.bk.abandoned_through = self
                .bk
                .fetched
                .iter()
                .find(|f| f.epoch == e)
                .map_or(0, |f| f.through);
            tracing::warn!(
                node = me,
                epoch = e,
                "the operator abandoned the sources of a kept fetch that had not answered: \
                 the claim carries what answered"
            );
        }
        self.bk.abandon = None;
        self.bk.group_wait = None;
        self.bk.kept_ready = Some(prev.renewal_key());
        true
    }
}
