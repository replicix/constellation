//! Plan 43 C1-3: the tail fetch — a claim of a lease by TTL carries the
//! group's committed tail, or waits for it.
//!
//! # Why
//!
//! An acknowledgement under a `Backup` lease means every listed backup
//! holds the row (write-all, C1-1), so any one member of the group — the
//! holder or a listed backup — holds every row the holder acknowledged and
//! has not shipped. A seal takeover carries them (the sealer is listed and
//! re-ships its tail). A claim by TTL did not: it re-executed nothing and
//! lost the tail ("a double fault"), and what the clients had been told
//! was committed came back only as replays by rid, after the ops invoked
//! meanwhile. C1 makes the commit real, so the TTL claimant fetches the
//! tail first:
//!
//! - **`SealTail`** to every member of the old group (the holder and each
//!   listed backup; or, for a lease whose tenure re-ships a predecessor's
//!   tail and lists no backup yet, the holder and the group the lease
//!   names: `Lease::inherited_tail`). A member persists and syncs a seal of
//!   the epoch, so that its group commits nothing more of it, and answers
//!   with what it holds, in batches. The first member that answers has the
//!   tail; the claim goes on as a takeover of the epoch whose tail it
//!   carries (`TailFollows` in its marker).
//! - **No member answers**: the claim *waits* (G4, plan 43 Q1): the
//!   attempt ends, the lease path of the ops behind it retries every
//!   `backup_takeover_ms`, `status.lease.waiting_for_group` says for what.
//!   A group whose members are all gone for good is the operator's to
//!   abandon (`constellation repair abandon-group`, [`Control::AbandonGroup`]):
//!   the claim then proceeds without the tail and its marker says so
//!   (`LogRecord::GroupAbandoned`).
//!
//! The member side ([`Core::on_seal_tail`]) is where the safety lives: what
//! it answers it seals first. A *holder* that is asked deposes itself (it
//! never acknowledges the epoch again; its unshipped rows are kept as a
//! member copy before its recovery strands them) and re-claims its own
//! expired lease at the next epoch if nobody else does (D7). A *backup*
//! seals its role and answers its contiguous tail.
//!
//! The claim CAS is conditioned on the lease read that named the group, so
//! a holder that renewed (or reconfigured) meanwhile fails it and the
//! attempt starts over.

use super::{Core, Timer};
use crate::action::Action;
use crate::event::PeerMsg;
use crate::ids::{Epoch, NodeId, Now, OpId, TimerId, WaitMs};
use crate::replica::Replica;
use constellation_meta::{BackupRole, BackupTx};
use constellation_store_s3::{AckPolicy, InheritedTail, Lease, LeaseTag};
use std::collections::BTreeMap;

/// One attempt of a claim to fetch the group's sealed tail.
#[derive(Debug)]
pub(crate) struct TailFetch {
    /// The lease the claim is conditioned on, and its tag.
    pub prev: Lease,
    pub tag: LeaseTag,
    /// First requests in flight, by request id.
    pub asked: BTreeMap<OpId, (NodeId, Epoch, u64)>,
    /// The member whose batches are being pulled (the first to answer
    /// `held`).
    pub chosen: Option<Chosen>,
    /// The timer that ends this attempt.
    pub timer: Option<TimerId>,
}

/// The member a tail is being pulled from.
#[derive(Debug)]
pub(crate) struct Chosen {
    pub node: NodeId,
    pub epoch: Epoch,
    pub config_version: u64,
    /// The batch request in flight.
    pub req: OpId,
    pub txs: Vec<BackupTx>,
    pub through: u64,
}

/// A tail this node fetched (or holds as the claimant of a sealed epoch)
/// and will re-ship: the epoch it belongs to and the group that held it.
#[derive(Debug, Clone)]
pub(crate) struct FetchedTail {
    pub epoch: Epoch,
    pub config_version: u64,
    pub members: Vec<NodeId>,
    pub through: u64,
}

/// What `status.lease.waiting_for_group` reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitingForGroup {
    /// The epoch of the lease whose group's tail is awaited.
    pub epoch: Epoch,
    /// How long this claimant has waited, ms.
    pub waited_ms: u64,
    /// The members asked.
    pub sources: Vec<NodeId>,
}

/// The persistent part of a wait across attempts.
#[derive(Debug, Clone)]
pub(crate) struct GroupWait {
    pub epoch: Epoch,
    pub since: WaitMs,
    pub sources: Vec<NodeId>,
    /// The last WARN (once a minute).
    pub warned: WaitMs,
    /// The last attempt to fetch (the ops behind the claim retry it): a wait
    /// nobody retries any more (the lease was taken meanwhile, no op waits
    /// for it) is no longer reported.
    pub last_attempt: WaitMs,
    /// The lease the wait is for, as the claimant watches it: a read that
    /// shows another key (a renewal, another claim) ends the wait.
    pub key: constellation_store_s3::lease::RenewalKey,
}

/// How often a wait for a group says so at WARN.
const WARN_EVERY_MS: u64 = 60_000;

impl Core {
    // ------------------------------------------------ the member's side

    /// The rows of `epoch` this node can hand a claimant that seals it, and
    /// the last journal seq of its contiguous hold; sealing first. `None`:
    /// not a member of that epoch's group (or the seal could not be
    /// persisted: the claimant retries). A store that could not be read sets
    /// `bk.seal_unreadable`: no answer at all is sent then.
    fn seal_tail_hold(
        &mut self,
        now: Now,
        from: NodeId,
        epoch: Epoch,
        config_version: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> Option<(Vec<BackupTx>, u64)> {
        // A store that cannot be read is no answer at all: not "I hold
        // nothing", nor an empty tail (plan 43 C2e review). The claimant
        // asks again.
        if !self.backup_sealed_known(replica) {
            self.bk.seal_unreadable = true;
            return None;
        }
        // 1. The holder of `epoch` (or a node that restarted holding rows it
        // executed under it and has not shipped): it answers from its
        // journal, kept as a member copy, and never acknowledges the epoch
        // again.
        let held_epoch = self
            .lease
            .held
            .as_ref()
            .map(|(l, _)| (l.epoch, l.config_version));
        let holds_rows = match held_epoch {
            Some((e, cv)) => e == epoch && !self.lease.epoch_held() && config_version >= cv,
            None if self.lease.lost => false,
            None => match replica.journal_has_epoch_rows(epoch) {
                Some(held) => held,
                None => {
                    self.bk.seal_unreadable = true;
                    return None;
                }
            },
        };
        if holds_rows {
            if !replica.backup_seal(epoch) {
                return None;
            }
            self.bk.sealed = self.bk.sealed.max(epoch);
            let first = replica.journal_acked_seq().unwrap_or(0) + 1;
            // (With the class each row was journaled under: an abandon queues
            // only what a lone node's replay would not.)
            let txs = replica.member_copy_put_journal(epoch, config_version, first)?;
            if !self.bk.member_epochs.contains(&epoch) {
                self.bk.member_epochs.push(epoch);
            }
            self.stats.seal_tail_holder += 1;
            tracing::warn!(
                node = self.cfg.node_id,
                claimant = from,
                epoch,
                rows = txs.len(),
                "a claimant asked for this holder's tail: sealed its own epoch, acknowledging \
                 nothing of it again"
            );
            // Deposed by the claimant's seal: every parked answer aborts,
            // the recovery round strands the rows (the copy is what the
            // claimant and any later one is answered from).
            if self.lease.held.is_some() {
                self.deposed(now, from, epoch + 1, epoch, replica, out);
                self.nudge(now, out);
            }
        } else if let Some(role) = self.bk.role.filter(|r| r.epoch == epoch) {
            // 2. A backup with a role for `epoch`: seal it, answer the
            // contiguous tail (D8: any version at or above the role's).
            if config_version < role.config_version {
                return None;
            }
            if self.bk.sealed < epoch {
                if !replica.backup_seal(epoch) {
                    return None;
                }
                self.bk.sealed = epoch;
            }
            let (Some(acked), Some(tail)) =
                (replica.backup_acked(epoch), replica.backup_tail(epoch))
            else {
                self.bk.seal_unreadable = true;
                return None;
            };
            let txs: Vec<BackupTx> = tail.into_iter().filter(|t| t.last <= acked).collect();
            self.stats.seal_tail_backup += 1;
            return Some((txs, acked));
        }
        // 3. A member copy of `epoch` (a sealed holder's, or one a later
        // claimant is told again): idempotent.
        let (_, txs) = replica.member_copy(epoch)?;
        let through = txs.iter().map(|t| t.last).max().unwrap_or(0);
        Some((txs, through))
    }

    /// `SealTail`: a claimant of a lease by TTL asks for the tail of
    /// `epoch`. Answered in batches of `backup_batch_rows` from journal
    /// seq `from_seq`; the same answer every time (the seal is persisted).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_seal_tail(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        epoch: Epoch,
        config_version: u64,
        from_seq: u64,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let reply = |held: bool,
                     txs: Vec<BackupTx>,
                     through: u64,
                     next: Option<u64>,
                     out: &mut Vec<Action>| {
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::SealedTail {
                    req,
                    epoch,
                    held,
                    txs,
                    through,
                    next,
                },
            });
        };
        let Some((txs, through)) =
            self.seal_tail_hold(now, from, epoch, config_version, replica, out)
        else {
            if std::mem::take(&mut self.bk.seal_unreadable) {
                // Not "I hold nothing": nothing at all (the claimant asks
                // again, or another member answers).
                self.stats.seal_tail_unreadable += 1;
                return;
            }
            self.stats.seal_tail_refused += 1;
            reply(false, Vec::new(), 0, None, out);
            return;
        };
        let (batch, next) = batch_of(&txs, from_seq, self.cfg.backup_batch_rows.max(1));
        reply(true, batch, through, next, out);
    }

    // ------------------------------------------------ the claimant's side

    /// Begin an attempt to fetch the tail of `prev`'s group (`Plan::FetchTail`):
    /// ask every source at once.
    pub(crate) fn begin_tail_fetch(
        &mut self,
        now: Now,
        prev: Lease,
        tag: LeaseTag,
        sources: Vec<(NodeId, Epoch, u64)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let wait = self
            .bk
            .group_wait
            .get_or_insert_with(|| super::seal_tail::GroupWait {
                epoch: prev.epoch,
                since: now.wait,
                sources: Vec::new(),
                warned: now.wait,
                last_attempt: now.wait,
                key: prev.renewal_key(),
            });
        if wait.epoch != prev.epoch {
            *wait = GroupWait {
                epoch: prev.epoch,
                since: now.wait,
                sources: Vec::new(),
                warned: now.wait,
                last_attempt: now.wait,
                key: prev.renewal_key(),
            };
        }
        wait.last_attempt = now.wait;
        wait.sources = sources.iter().map(|s| s.0).collect();
        // The operator said the group is gone: no wait.
        if self.bk.abandon == Some(prev.epoch) {
            self.claim_without_tail(now, prev, tag, replica, out);
            return;
        }
        if let Some(old) = self.bk.tf.take() {
            if let Some(t) = old.timer {
                self.cancel_timer(t, out);
            }
        }
        let mut tf = TailFetch {
            prev,
            tag,
            asked: BTreeMap::new(),
            chosen: None,
            timer: None,
        };
        let round = self.op_id();
        let at = now.wait.plus(self.cfg.backup_takeover_ms.max(1));
        tf.timer = Some(self.set_timer_wait(at, Timer::JobRequestTimeout(round), out));
        self.bk.tf_round = Some(round);
        self.bk.tf = Some(tf);
        self.tail_fetch_phase();
        self.stats.tail_fetch_attempts += 1;
        for (node, epoch, config_version) in sources {
            if node == self.cfg.node_id {
                // This node's own copy (a member copy of the inherited
                // epoch, a role of it): asked in place.
                let own = self.seal_tail_hold(now, node, epoch, config_version, replica, out);
                // (Unreadable: not chosen; the other members are asked.)
                self.bk.seal_unreadable = false;
                if let Some((txs, through)) = own {
                    if let Some(tf) = self.bk.tf.as_mut() {
                        tf.asked.clear();
                        tf.chosen = Some(Chosen {
                            node,
                            epoch,
                            config_version,
                            req: OpId(0),
                            txs,
                            through,
                        });
                    }
                    self.tail_fetch_next(now, None, replica, out);
                    return;
                }
                continue;
            }
            let req = self.op_id();
            if let Some(tf) = self.bk.tf.as_mut() {
                tf.asked.insert(req, (node, epoch, config_version));
            }
            out.push(Action::Send {
                to: node,
                msg: PeerMsg::SealTail {
                    req,
                    epoch,
                    config_version,
                    from: 0,
                },
            });
        }
    }

    /// A member answered a `SealTail` of this claim.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_sealed_tail(
        &mut self,
        now: Now,
        from: NodeId,
        req: OpId,
        epoch: Epoch,
        held: bool,
        txs: Vec<BackupTx>,
        through: u64,
        next: Option<u64>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(tf) = self.bk.tf.as_mut() else {
            return;
        };
        match tf.chosen.as_mut() {
            // A batch of the member being pulled from.
            Some(ch) => {
                if ch.req != req || ch.node != from || ch.epoch != epoch {
                    // A late first answer of another member: the tail is
                    // already coming from the first.
                    return;
                }
                if !held {
                    // The member lost it between batches: the attempt ends
                    // and starts over.
                    self.end_tail_fetch_attempt(now, replica, out);
                    return;
                }
                ch.txs.extend(txs);
                ch.through = ch.through.max(through);
            }
            // A first answer.
            None => {
                let Some((node, e, config_version)) = tf.asked.remove(&req) else {
                    return;
                };
                if node != from || e != epoch || !held {
                    // Not a member that holds it: the others may; when
                    // none does the attempt ends at its timer.
                    return;
                }
                tf.asked.clear();
                tf.chosen = Some(Chosen {
                    node,
                    epoch,
                    config_version,
                    req,
                    txs,
                    through,
                });
            }
        }
        self.tail_fetch_next(now, next, replica, out);
    }

    /// Pull the next batch of the chosen member (in a window of its own), or
    /// finish.
    fn tail_fetch_next(
        &mut self,
        now: Now,
        next: Option<u64>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        match next {
            Some(from_seq) => {
                if self.bk.tf.as_ref().is_none_or(|t| t.chosen.is_none()) {
                    return;
                }
                // A member that answers is no member that is gone: each
                // batch it is asked for gets a window of its own. The
                // attempt's one window, armed when it began, ended the pull
                // of any tail longer than one window's batches and started
                // it over from the first, every attempt, with the member
                // answering all along (a wait no member's return ends).
                let old = self.bk.tf.as_mut().and_then(|t| t.timer.take());
                if let Some(t) = old {
                    self.cancel_timer(t, out);
                }
                let round = self.op_id();
                let at = now.wait.plus(self.cfg.backup_takeover_ms.max(1));
                let timer = self.set_timer_wait(at, Timer::JobRequestTimeout(round), out);
                self.bk.tf_round = Some(round);
                let req = self.op_id();
                let Some(tf) = self.bk.tf.as_mut() else {
                    return;
                };
                tf.timer = Some(timer);
                let Some(ch) = tf.chosen.as_mut() else {
                    return;
                };
                ch.req = req;
                out.push(Action::Send {
                    to: ch.node,
                    msg: PeerMsg::SealTail {
                        req,
                        epoch: ch.epoch,
                        config_version: ch.config_version,
                        from: from_seq,
                    },
                });
            }
            None => {
                let Some(ch) = self.bk.tf.as_mut().and_then(|t| t.chosen.take()) else {
                    return;
                };
                self.tail_fetch_complete(now, ch, replica, out);
            }
        }
    }

    /// No answer from this member: nothing to do until the timer (the
    /// member is simply not one that answered).
    pub(crate) fn on_tail_fetch_failed(
        &mut self,
        now: Now,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        let Some(tf) = self.bk.tf.as_mut() else {
            return false;
        };
        if tf.asked.remove(&req).is_some() {
            return true;
        }
        if tf.chosen.as_ref().is_some_and(|c| c.req == req) {
            // The member we pulled from went quiet between batches.
            self.end_tail_fetch_attempt(now, replica, out);
            return true;
        }
        false
    }

    /// The attempt's timer ran out (or its chosen member failed): end it.
    /// The ops behind the acquisition retry, and the next attempt asks
    /// again (the lease may have changed).
    pub(crate) fn on_tail_fetch_timer(
        &mut self,
        now: Now,
        req: OpId,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) -> bool {
        if self.bk.tf_round != Some(req) {
            return false;
        }
        self.bk.tf_round = None;
        if let Some(tf) = self.bk.tf.as_mut() {
            tf.timer = None;
        }
        self.end_tail_fetch_attempt(now, replica, out);
        true
    }

    fn end_tail_fetch_attempt(&mut self, now: Now, replica: &dyn Replica, out: &mut Vec<Action>) {
        if let Some(tf) = self.bk.tf.take() {
            if let Some(t) = tf.timer {
                self.cancel_timer(t, out);
            }
        }
        self.bk.tf_round = None;
        if let Some(w) = self.bk.group_wait.as_mut() {
            let waited = now.wait.since(w.since).max(0) as u64;
            if now.wait.since(w.warned) >= WARN_EVERY_MS as i64 || w.warned == w.since {
                w.warned = now.wait;
                tracing::warn!(
                    node = self.cfg.node_id,
                    epoch = w.epoch,
                    sources = ?w.sources,
                    waited_ms = waited,
                    "the claim of the lease waits for the tail of its group: no member answered \
                     (the operator may abandon the group: `repair abandon-group`)"
                );
            } else {
                tracing::debug!(
                    node = self.cfg.node_id,
                    epoch = w.epoch,
                    waited_ms = waited,
                    "still waiting for the tail of the lease's group"
                );
            }
            self.stats.tail_fetch_wait_ms_total += self.cfg.backup_takeover_ms;
        }
        // Retry no later than the next window.
        self.acquire_retry_hint = Some(now.wait.plus(self.cfg.backup_takeover_ms.max(1)));
        self.end_acquire_attempt(now, replica, out);
    }

    /// Every batch is in: write the tail as a sealed backup's, and go on
    /// with the claim as a takeover that carries it.
    fn tail_fetch_complete(
        &mut self,
        now: Now,
        ch: Chosen,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(tf) = self.bk.tf.take() else {
            return;
        };
        if let Some(t) = tf.timer {
            self.cancel_timer(t, out);
        }
        self.bk.tf_round = None;
        let prev = tf.prev;
        if !replica.backup_append(ch.epoch, &ch.txs) {
            self.end_acquire_attempt(now, replica, out);
            return;
        }
        let role = BackupRole {
            holder: prev.holder,
            epoch: ch.epoch,
            config_version: ch.config_version,
        };
        replica.set_backup_role(role);
        if !replica.backup_seal(ch.epoch) {
            self.end_acquire_attempt(now, replica, out);
            return;
        }
        self.bk.sealed = self.bk.sealed.max(ch.epoch);
        self.bk.role = Some(role);
        // (Unreadable: what it holds counts from 0, which only ever
        // acknowledges less; the holder sends again.)
        self.bk.acked = replica.backup_acked(ch.epoch).unwrap_or(0);
        self.bk.held.clear();
        // The members of the old group: who held the tail a later claimant
        // may ask; this node's copy outlives the fetch.
        let mut members: Vec<NodeId> = match &prev.inherited_tail {
            Some(t) if t.epoch == ch.epoch && prev.epoch != ch.epoch => t.members.clone(),
            // A tail of the lease's own epoch (`own_group_tail`): the group
            // it names holds the rows as well as whoever the lease lists.
            Some(t) if t.epoch == ch.epoch => std::iter::once(prev.holder)
                .chain(prev.backups.iter().copied())
                .chain(t.members.iter().copied())
                .collect(),
            _ => std::iter::once(prev.holder)
                .chain(prev.backups.iter().copied())
                .collect(),
        };
        members.push(self.cfg.node_id);
        members.sort_unstable();
        members.dedup();
        self.bk.fetched = Some(FetchedTail {
            epoch: ch.epoch,
            config_version: ch.config_version,
            members,
            through: ch.through,
        });
        self.stats.tail_fetches += 1;
        if prev.inherited_tail.is_some() {
            self.stats.inherited_tail_fetches += 1;
        }
        if let Some(w) = self.bk.group_wait.take() {
            tracing::info!(
                node = self.cfg.node_id,
                epoch = ch.epoch,
                from = ch.node,
                rows = ch.txs.len(),
                waited_ms = now.wait.since(w.since).max(0),
                "fetched the sealed tail of the lease's group: the claim carries it"
            );
        }
        self.claim_with_tail(now, prev, tf.tag, replica, out);
    }

    /// The operator abandoned the group: the claim goes on without a tail
    /// (and says so in its marker).
    fn claim_without_tail(
        &mut self,
        now: Now,
        prev: Lease,
        tag: LeaseTag,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        self.bk.abandon = None;
        self.bk.abandoned_claim = Some(prev.epoch);
        self.bk.group_wait = None;
        self.stats.groups_abandoned += 1;
        tracing::warn!(
            node = self.cfg.node_id,
            epoch = prev.epoch,
            "the operator abandoned the group of the lease: claiming it without its tail"
        );
        self.claim_with_tail(now, prev, tag, replica, out);
    }

    /// `Control::AbandonGroup`: valid while a claim waits for `epoch`'s
    /// group and none of its members has answered. With `gen != 0` it
    /// abandons a delegation's group instead (C2e,
    /// [`Core::on_control_abandon_generation`]).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_control_abandon_group(
        &mut self,
        now: Now,
        op: OpId,
        epoch: Epoch,
        gen: u64,
        force: bool,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        if gen != 0 {
            self.on_control_abandon_generation(now, op, epoch, gen, force, replica, out);
            return;
        }
        let waiting = self.bk.group_wait.as_ref().map(|w| w.epoch);
        let chosen = self.bk.tf.as_ref().is_some_and(|t| t.chosen.is_some());
        let result = if waiting != Some(epoch) || chosen {
            Err(format!("not waiting for epoch {epoch}'s group"))
        } else {
            Ok(crate::action::ControlOk::Text(format!(
                "abandoning the group of epoch {epoch}: the waiting claim goes on without its tail"
            )))
        };
        let ok = result.is_ok();
        out.push(Action::ControlDone { op, result });
        if !ok {
            return;
        }
        self.bk.abandon = Some(epoch);
        match self.bk.tf.take() {
            // An attempt is out: its claim goes on at once.
            Some(tf) => {
                if let Some(t) = tf.timer {
                    self.cancel_timer(t, out);
                }
                self.bk.tf_round = None;
                self.claim_without_tail(now, tf.prev, tf.tag, replica, out);
            }
            // Between attempts: the next one claims.
            None => self.acquire_retry_hint = Some(now.wait),
        }
    }

    // ------------------------------------------------ the claim's tail

    /// The tail this claim will re-ship, as the lease its CAS writes names
    /// it (`Lease::inherited_tail`): until the log carries the re-shipped
    /// rows, the old group's members keep their copies and a later claimant
    /// of this lease fetches from them (D2). `None`: nothing to re-ship.
    pub(crate) fn tail_to_inherit(
        &self,
        prev: &Lease,
        replica: &dyn Replica,
    ) -> Option<InheritedTail> {
        // (Unreadable: as if it carried rows, so the lease names the tail
        // and a later claimant fetches it from the members.)
        let carried = |epoch: Epoch| {
            replica
                .backup_tail(epoch)
                .is_none_or(|tail| tail.iter().any(|t| !t.records.is_empty()))
        };
        if let Some(f) = self.bk.fetched.as_ref().filter(|f| carried(f.epoch)) {
            return Some(InheritedTail {
                epoch: f.epoch,
                config_version: f.config_version,
                members: f.members.clone(),
                through: f.through,
            });
        }
        let role = self
            .bk
            .role
            .filter(|r| r.epoch == prev.epoch && r.holder == prev.holder && carried(r.epoch))?;
        let mut members: Vec<NodeId> = std::iter::once(prev.holder)
            .chain(prev.backups.iter().copied())
            .chain(std::iter::once(self.cfg.node_id))
            .collect();
        members.sort_unstable();
        members.dedup();
        Some(InheritedTail {
            epoch: role.epoch,
            config_version: prev.config_version,
            members,
            through: self.bk.acked,
        })
    }

    /// A re-adoption of this node's own `Backup` lease (a restart, or a CAS
    /// whose reply was lost): the configuration its CAS writes lists no
    /// backup (`granted_lease`), but the rows its tenure acknowledged and
    /// has not shipped are held by this disk and by every backup `prev`
    /// lists (write-all, C1-1). The lease names that group until the log
    /// carries them (D2, `clear_covered_tail`), so that a TTL claimant of
    /// the re-adopted lease fetches them (from a backup, if this node is
    /// gone) and does not claim a `Local` lease at once without them.
    pub(crate) fn own_group_tail(
        &self,
        prev: &Lease,
        replica: &dyn Replica,
    ) -> Option<InheritedTail> {
        if prev.ack_policy != AckPolicy::Backup
            || prev.backups.is_empty()
            // (Unreadable: as if it held rows, so the group's tail is
            // fetched rather than given up.)
            || !replica.journal_has_epoch_rows(prev.epoch).unwrap_or(true)
        {
            return None;
        }
        let mut members: Vec<NodeId> = std::iter::once(prev.holder)
            .chain(prev.backups.iter().copied())
            .collect();
        members.sort_unstable();
        members.dedup();
        Some(InheritedTail {
            epoch: prev.epoch,
            config_version: prev.config_version,
            members,
            through: replica.journal_tip(),
        })
    }

    /// Before a claim's CAS: a listed backup that claims at the lease's
    /// expiry seals its role first (it takes the tail it holds, and its
    /// group commits nothing more of the epoch); a self-sealed holder
    /// re-claims with its own member copy as the tail (D7). `false`: could
    /// not (the seal or the copy did not persist): the claim ends.
    pub(crate) fn prepare_tail_for_claim(&mut self, prev: &Lease, replica: &dyn Replica) -> bool {
        if !self.backup_sealed_known(replica) {
            return false;
        }
        let me = self.cfg.node_id;
        if prev.holder == me && self.bk.sealed >= prev.epoch {
            // Self-sealed: the copy of the epoch is the tail.
            if self
                .bk
                .fetched
                .as_ref()
                .is_some_and(|f| f.epoch == prev.epoch)
            {
                return true;
            }
            let Some((cv, txs)) = replica.member_copy(prev.epoch) else {
                return true;
            };
            if !txs.is_empty() {
                if !replica.backup_append(prev.epoch, &txs) {
                    return false;
                }
                let role = BackupRole {
                    holder: me,
                    epoch: prev.epoch,
                    config_version: cv,
                };
                replica.set_backup_role(role);
                self.bk.role = Some(role);
                let Some(acked) = replica.backup_acked(prev.epoch) else {
                    return false;
                };
                self.bk.acked = acked;
                let mut members: Vec<NodeId> = prev.backups.clone();
                members.push(me);
                members.sort_unstable();
                members.dedup();
                self.bk.fetched = Some(FetchedTail {
                    epoch: prev.epoch,
                    config_version: cv,
                    members,
                    through: txs.iter().map(|t| t.last).max().unwrap_or(0),
                });
            }
            return true;
        }
        // (A released lease's holder flushed everything: no group holds
        // anything to seal.)
        let listed_with_role = prev.holder != me
            && !prev.released
            && prev.ack_policy == AckPolicy::Backup
            && prev.backups.contains(&me)
            && self
                .bk
                .role
                .is_some_and(|r| r.holder == prev.holder && r.epoch == prev.epoch);
        if listed_with_role && self.bk.sealed < prev.epoch {
            if !replica.backup_seal(prev.epoch) {
                return false;
            }
            self.bk.sealed = prev.epoch;
            self.stats.claim_seals += 1;
        }
        true
    }

    /// The report of a waiting claim, for `status`.
    pub fn waiting_for_group(&self, now: Now) -> Option<WaitingForGroup> {
        let stale_after = (4 * self.cfg.backup_takeover_ms).max(10_000) as i64;
        // Waiting means somebody needs the claim: an attempt is out, or an op
        // of this node waits for the lease (or retries for it).
        let needed = self.bk.tf.is_some()
            || self.clients.values().any(|c| {
                matches!(
                    c.phase,
                    super::client::Phase::WaitingLease | super::client::Phase::AcquireRetry
                )
            });
        self.bk
            .group_wait
            .as_ref()
            .filter(|w| needed && now.wait.since(w.last_attempt) <= stale_after)
            .map(|w| WaitingForGroup {
                epoch: w.epoch,
                waited_ms: now.wait.since(w.since).max(0) as u64,
                sources: w.sources.clone(),
            })
    }

    /// A lease read: the wait for a group ends when the lease moved on
    /// (another key than the one the claim was for).
    pub(crate) fn note_group_wait_lease(&mut self, lease: &Lease) {
        if self
            .bk
            .group_wait
            .as_ref()
            .is_some_and(|w| w.key != lease.renewal_key())
        {
            self.bk.group_wait = None;
        }
    }

    /// Count what a stranding pass did to committed work (plan 43 C1-4).
    pub(crate) fn note_committed_stranded(&mut self, s: &constellation_meta::Stranded) {
        self.stats.rows_member_copied += s.member_copied as u64;
        self.stats.committed_superseded += s.superseded as u64;
        self.stats.committed_rolled_back += s.committed_rolled_back as u64;
        self.stats.replay_gated_enqueued += s.gated_queued as u64;
        self.stats.deleg_dropped += s.deleg_dropped as u64;
    }
}

/// The batch of `txs` from journal seq `from` (0: the start): whole
/// transactions up to `rows` rows, and where the next batch starts.
fn batch_of(txs: &[BackupTx], from: u64, rows: usize) -> (Vec<BackupTx>, Option<u64>) {
    let mut out = Vec::new();
    let mut count = 0usize;
    for (i, t) in txs.iter().enumerate() {
        if t.first < from {
            continue;
        }
        let n = t.records.len().max(1);
        if count > 0 && count + n > rows {
            return (out, Some(t.first));
        }
        count += n;
        out.push(t.clone());
        if i + 1 == txs.len() {
            break;
        }
    }
    (out, None)
}
