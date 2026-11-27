//! FX-5: the epoch hold's durable record and its transfer numbers.
//!
//! A continuation epoch carries one lease (the *key*: [`HoldKey`]); one
//! node at a time holds that epoch's hold. Every hold of a key has a
//! transfer number: 0 at the carrier's adoption, plus one per give. A node
//! persists exactly one [`HoldRecord`] (`local["epoch_hold_rec"]`), and
//! every transition writes the whole record durably before the act that
//! depends on it:
//!
//! - `Held { key, n, at }`: this node holds `key`'s hold, taken as transfer
//!   `n` (0: the carrier's adoption); `at` is the epoch it journals under,
//!   what a restart re-adopts at;
//! - `Given { key, n, to }`: it gave the hold as transfer `n` to `to`
//!   (persisted with the let-go, before the answer leaves);
//! - `Ended { key, n, reclaim }`: it let the hold go without a transfer
//!   (the close, a deposition, a vanish, a release in a frozen epoch). No
//!   recipient exists by construction.
//!
//! The record is the hold: in memory the node never holds without a
//! `Held` record of it, and a restart re-adopts exactly the record.
//! Uniqueness of the hold follows from the numbers (plan 46
//! §5.7): gives are totally ordered by `n`, an offer of `n` is takeable
//! only by its recipient, and only until that node has passed `n`.
//!
//! The record is read once per process (`HoldRecState`); a value that
//! cannot be read or parsed is [`HoldRecState::Unreadable`] and blocks
//! every transition (fail closed), never "no record". A write that fails
//! may still have reached the store (the value set, the sync failed): the
//! store is then rolled back to the record memory holds before any
//! transition ([`HoldRecState::RollingBack`]), so memory never lags the
//! store.

use super::Core;
use crate::action::Action;
use crate::event::{Carrier, HoldOffer, PeerMsg, XferNo};
use crate::ids::{Epoch, NodeId, Now, OpId};
use crate::replica::Replica;
use std::fmt;
use std::str::FromStr;

/// The carried lease an epoch hold stands on (node, epoch, expiry): the
/// key compared everywhere is those three fields.
pub type HoldKey = Carrier;

/// Whether a closer still owes the carried lease's re-claim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reclaim {
    /// The close made the re-claim this node's to do.
    Owed,
    /// The re-claim is done or moot, or the hold went without a close.
    Settled,
}

/// A node's one persisted record of an epoch hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldRecord {
    /// Holds `key`'s hold, taken as transfer `n`; `at` is the epoch it
    /// journals under (`Core::epoch_hold_epoch_for`).
    Held { key: HoldKey, n: XferNo, at: Epoch },
    /// Gave `key`'s hold as transfer `n` to `to`.
    Given { key: HoldKey, n: XferNo, to: NodeId },
    /// Let `key`'s hold go without a transfer; `n` is the number it held
    /// at.
    Ended {
        key: HoldKey,
        n: XferNo,
        reclaim: Reclaim,
    },
}

impl HoldRecord {
    /// The carried lease the record is about.
    pub fn key(&self) -> HoldKey {
        match self {
            HoldRecord::Held { key, .. }
            | HoldRecord::Given { key, .. }
            | HoldRecord::Ended { key, .. } => *key,
        }
    }

    /// The transfer number the record carries.
    pub fn n(&self) -> XferNo {
        match self {
            HoldRecord::Held { n, .. }
            | HoldRecord::Given { n, .. }
            | HoldRecord::Ended { n, .. } => *n,
        }
    }
}

/// Why a stored record could not be read back.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HoldRecordError {
    /// The stored value is not one of the three encodings.
    #[error("malformed epoch hold record {0:?}")]
    Malformed(String),
}

impl fmt::Display for HoldRecord {
    /// `"h:<node>:<epoch>:<expires>:<n>:<at>"`,
    /// `"g:<node>:<epoch>:<expires>:<n>:<to>"`,
    /// `"e:<node>:<epoch>:<expires>:<n>:<o|s>"`: colon-separated, every
    /// field always present.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = self.key();
        match self {
            HoldRecord::Held { n, at, .. } => write!(
                f,
                "h:{}:{}:{}:{}:{}",
                k.node, k.epoch, k.expires_unix_ms, n.0, at
            ),
            HoldRecord::Given { n, to, .. } => write!(
                f,
                "g:{}:{}:{}:{}:{}",
                k.node, k.epoch, k.expires_unix_ms, n.0, to
            ),
            HoldRecord::Ended { n, reclaim, .. } => write!(
                f,
                "e:{}:{}:{}:{}:{}",
                k.node,
                k.epoch,
                k.expires_unix_ms,
                n.0,
                match reclaim {
                    Reclaim::Owed => 'o',
                    Reclaim::Settled => 's',
                }
            ),
        }
    }
}

impl FromStr for HoldRecord {
    type Err = HoldRecordError;

    /// Anything but the three encodings is an error (never "no record":
    /// the empty string is the caller's `None`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || HoldRecordError::Malformed(s.to_string());
        let parts: Vec<&str> = s.split(':').collect();
        let [tag, node, epoch, expires, n, last] = parts.as_slice() else {
            return Err(bad());
        };
        let key = Carrier {
            node: node.parse().map_err(|_| bad())?,
            epoch: epoch.parse().map_err(|_| bad())?,
            expires_unix_ms: expires.parse().map_err(|_| bad())?,
        };
        let n = XferNo(n.parse().map_err(|_| bad())?);
        match *tag {
            "h" => Ok(HoldRecord::Held {
                key,
                n,
                at: last.parse().map_err(|_| bad())?,
            }),
            "g" => Ok(HoldRecord::Given {
                key,
                n,
                to: last.parse().map_err(|_| bad())?,
            }),
            "e" => Ok(HoldRecord::Ended {
                key,
                n,
                reclaim: match *last {
                    "o" => Reclaim::Owed,
                    "s" => Reclaim::Settled,
                    _ => return Err(bad()),
                },
            }),
            _ => Err(bad()),
        }
    }
}

/// What this process knows of the record. Fail closed: `Unreadable` and
/// `RollingBack` block every hold transition (adoption, give, re-offer,
/// take, nudge, restore) until a later read or rollback succeeds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum HoldRecState {
    /// The read failed, or the value did not parse (and the state of a
    /// process that has not read it yet).
    #[default]
    Unreadable,
    /// A write of a new record failed, and it may have reached the store
    /// all the same. Memory keeps the record that stood before it (the one
    /// inside); that record is written again before anything else (at
    /// every event until it is), so the store never stays ahead of memory.
    RollingBack(Option<HoldRecord>),
    /// What the store holds (`None`: no record).
    Known(Option<HoldRecord>),
}

impl HoldRecState {
    /// The highest transfer number of `key` this node ever took or gave:
    /// the record's own `n` when it is about `key`; 0 when there is no
    /// record of `key` (nothing taken or given); `None` when the record is
    /// unreadable (every caller declines).
    pub(crate) fn highest(&self, key: HoldKey) -> Option<XferNo> {
        match self {
            HoldRecState::Unreadable | HoldRecState::RollingBack(_) => None,
            HoldRecState::Known(Some(r)) if r.key() == key => Some(r.n()),
            HoldRecState::Known(_) => Some(XferNo(0)),
        }
    }

    /// The record, when readable.
    pub(crate) fn record(&self) -> Option<&HoldRecord> {
        match self {
            HoldRecState::Known(r) => r.as_ref(),
            HoldRecState::Unreadable | HoldRecState::RollingBack(_) => None,
        }
    }

    /// Not known: unread, unreadable, or waiting for its rollback.
    pub(crate) fn is_unknown(&self) -> bool {
        !matches!(self, HoldRecState::Known(_))
    }
}

/// How a carrier's adoption of the hold relates to the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Adopt {
    /// A first adoption: only with no record of the carried lease (at
    /// transfer 0), or a record of another lease.
    Fresh,
    /// The activation carries the lease this node's own close let go
    /// (`Core::epoch_closed_carried`), claimed while its re-claim was
    /// pending: the one edge from an `Ended` record back to `Held`, at the
    /// same number (so no offer at or below it is ever taken).
    ClosedCarried,
}

/// Which member an epoch-mode acquisition asks for the hold next (memory
/// only: reset at every activation and when the epoch ends).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct EpochAsk {
    /// The giver a nudge named, asked once, first (`on_epoch_hold_nudge`).
    pub target: Option<NodeId>,
    /// The member the last epoch-mode request that failed went to: the
    /// rotation goes on with the next one.
    pub last: Option<NodeId>,
    /// The cached holder an epoch-mode request failed against (declined, or
    /// silent): skipped while it stays the cached holder, so that a stale
    /// cache or a paused member does not absorb every attempt.
    pub skip_cached: Option<NodeId>,
    /// The epoch-mode request in flight, and whom it went to.
    pub asked: Option<(OpId, NodeId)>,
}

/// What a request for the hold finds in the record of a node that holds
/// nothing (rules R1 to R9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reoffer {
    /// The record is not a give to this asker: today's rules answer.
    NotApplicable,
    /// Declined, counted by its cause.
    Declined,
    /// The transfer again.
    Offer(HoldOffer),
}

impl Core {
    /// Whom an epoch-mode acquisition asks for the hold (FX-5, rotation).
    /// First match: the giver a nudge named (once); the cached holder, if it
    /// is a member and no request to it failed since it became the cached
    /// holder; the next member after the last one asked, in id order,
    /// wrapping. Non-members are never asked while the members are known.
    /// With no members known: the first connected link, as before.
    pub(crate) fn epoch_ask_target(&mut self) -> Option<NodeId> {
        let me = self.cfg.node_id;
        if let Some(t) = self.epoch_ask.target.take() {
            if t != me && self.pr.epoch_members.contains(&t) {
                return Some(t);
            }
        }
        if self.epoch_ask.skip_cached.is_some()
            && self.lease.cached_holder != self.epoch_ask.skip_cached
        {
            self.epoch_ask.skip_cached = None;
        }
        if let Some(h) = self
            .lease
            .cached_holder
            .filter(|h| *h != me && self.pr.epoch_members.contains(h))
            .filter(|h| Some(*h) != self.epoch_ask.skip_cached)
        {
            return Some(h);
        }
        let mut others: Vec<NodeId> = self
            .pr
            .epoch_members
            .iter()
            .copied()
            .filter(|m| *m != me)
            .collect();
        others.sort_unstable();
        others.dedup();
        if !others.is_empty() {
            let next = self
                .epoch_ask
                .last
                .and_then(|l| others.iter().copied().find(|m| *m > l))
                .unwrap_or(others[0]);
            return Some(next);
        }
        // The members are not known: today's fallback.
        self.lease.cached_holder.filter(|h| *h != me).or_else(|| {
            self.roster
                .links()
                .values()
                .find(|l| l.connected && l.node != me)
                .or_else(|| self.roster.links().values().find(|l| l.node != me))
                .map(|l| l.node)
        })
    }

    /// An epoch-mode request to `x` ended without the hold (declined, a
    /// refused offer, or no answer): the rotation moves on, and a cached
    /// holder that failed is skipped while it stays the cached holder.
    pub(crate) fn epoch_ask_advance(&mut self, x: NodeId) {
        self.epoch_ask.last = Some(x);
        if self.lease.cached_holder == Some(x) {
            self.epoch_ask.skip_cached = Some(x);
        }
        self.epoch_ask.asked = None;
    }

    /// The request `req` got no answer in time (or its peer failed).
    pub(crate) fn epoch_ask_silent(&mut self, req: OpId) {
        if let Some((r, to)) = self.epoch_ask.asked {
            if r == req {
                self.epoch_ask_advance(to);
            }
        }
    }

    /// The nudge (FX-5, rules N1 to N6), at the ordinary round: a node whose
    /// record says it gave the hold to `to`, and which holds nothing, asks
    /// `to` to ask for it again. Level-triggered and stateless: the message
    /// carries no state, the recipient filters it (`on_epoch_hold_nudge`),
    /// and the next round sends it again until the epoch ends or `to` has
    /// passed that number. No timer, no constant: the poll is the round
    /// (the sync interval, up to four times it while the epoch is open).
    pub(crate) fn nudge_hold_recipient(&mut self, out: &mut Vec<Action>) {
        let Some(HoldRecord::Given { key, n, to }) = self.hold_record().copied() else {
            return;
        };
        if !self.epoch.active
            || self.epoch.frozen
            || self.pr.carried != Some(key)
            || self.lease.epoch_held()
            || to == self.cfg.node_id
            || !self.pr.epoch_members.contains(&to)
            || self.pr.retired
        {
            return;
        }
        self.stats.epoch_hold_nudges += 1;
        out.push(Action::Send {
            to,
            msg: PeerMsg::EpochHoldNudge { key, n },
        });
    }

    /// A nudge (rules A1 to A6): `from` says it gave this node transfer `n`
    /// of the hold of `key`. If this node has not passed `n`, and is idle
    /// in the epoch, it asks `from` for it through an ordinary epoch
    /// acquisition (the one guarded path to a take). Ignored otherwise,
    /// counted by cause; the next round's nudge tries again.
    pub(crate) fn on_epoch_hold_nudge(
        &mut self,
        now: Now,
        from: NodeId,
        key: HoldKey,
        n: XferNo,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        // A1, A2, A3.
        if !self.epoch.active || self.epoch.frozen {
            self.stats.epoch_hold_nudges_ignored_state += 1;
            return;
        }
        if !self.pr.epoch_members.contains(&from) {
            self.stats.epoch_hold_nudges_ignored_not_member += 1;
            return;
        }
        if self.pr.carried != Some(key) {
            self.stats.epoch_hold_nudges_ignored_key += 1;
            return;
        }
        // A4: only a number above the highest of this key taken or given
        // here. (A node that restarted and has not read its record, or
        // cannot, ignores it: it passes nothing it cannot see.)
        if !self.pr.hold_rec.highest(key).is_some_and(|h| n > h) {
            self.stats.epoch_hold_nudges_ignored_stale += 1;
            return;
        }
        // A5: it holds nothing and is not retired (nor with an unwritten
        // end of a hold of its own).
        if self.lease.epoch_held() || self.pr.retired || self.hold_rec_dirty() {
            self.stats.epoch_hold_nudges_ignored_unfit += 1;
            return;
        }
        // A6: no acquisition running or queued (the next round's nudge
        // finds it done).
        if self.acquisition_pending() {
            self.stats.epoch_hold_nudges_ignored_busy += 1;
            return;
        }
        self.epoch_ask.target = Some(from);
        self.enqueue_job(
            now,
            super::jobs::JobReq::Acquire {
                reason: "epoch-hold-owed",
                ask_handoff: false,
            },
            replica,
            out,
        );
    }

    /// Make the record known again when it is not: read it (at start, and
    /// at every event while the last read failed; a later read resumes every
    /// transition), or write back the record a failed write left memory
    /// with. A hold the record names but memory does not have is waiting to
    /// be restored (`restore_epoch_hold`) the first time it is read. A
    /// failure is counted at every event it blocks and logged once per run
    /// of failures.
    pub(crate) fn hold_rec_load(&mut self, replica: &dyn Replica) {
        let failure = match self.pr.hold_rec {
            HoldRecState::Known(_) => return,
            HoldRecState::Unreadable => match replica.epoch_hold_record() {
                Ok(rec) => {
                    self.pr.hold_rec = HoldRecState::Known(rec);
                    self.pr.restore_pending =
                        matches!(rec, Some(HoldRecord::Held { .. })) && !self.lease.epoch_held();
                    None
                }
                Err(error) => {
                    self.stats.epoch_hold_record_unreadable += 1;
                    Some((error, "reading the epoch hold record failed"))
                }
            },
            // (`restore_pending` stays as it was: memory's view did not
            // change, only the store is put back to it.)
            HoldRecState::RollingBack(prev) => {
                match replica.persist_epoch_hold_record(prev.as_ref()) {
                    Ok(()) => {
                        self.pr.hold_rec = HoldRecState::Known(prev);
                        None
                    }
                    Err(error) => {
                        self.stats.epoch_hold_rollback_failed += 1;
                        Some((error, "rolling the epoch hold record back failed"))
                    }
                }
            }
        };
        match failure {
            None => self.pr.hold_rec_warned = false,
            Some((error, what)) => {
                if !self.pr.hold_rec_warned {
                    self.pr.hold_rec_warned = true;
                    tracing::warn!(node = self.cfg.node_id, %error, "{what}; no hold transition until it succeeds");
                }
            }
        }
    }

    /// Persist `rec` (durably) as the node's record. The in-memory record
    /// follows only a successful write. A failed write may have reached the
    /// store (set, then the sync failed): memory keeps the previous record
    /// and the store is rolled back to it before any transition
    /// (`RollingBack`), the rule that keeps the two equal.
    pub(crate) fn persist_hold_rec(&mut self, replica: &dyn Replica, rec: HoldRecord) -> bool {
        match replica.persist_epoch_hold_record(Some(&rec)) {
            Ok(()) => {
                self.pr.hold_rec = HoldRecState::Known(Some(rec));
                true
            }
            Err(error) => {
                tracing::warn!(node = self.cfg.node_id, %error, ?rec, "persisting the epoch hold record failed");
                if let HoldRecState::Known(prev) = self.pr.hold_rec {
                    self.pr.hold_rec = HoldRecState::RollingBack(prev);
                }
                false
            }
        }
    }

    /// The record, when readable and present.
    pub(crate) fn hold_record(&self) -> Option<&HoldRecord> {
        self.pr.hold_rec.record()
    }

    /// The record says this node holds, but memory holds nothing, and no
    /// restart is waiting to restore it: an involuntary end (a deposition,
    /// a vanish, a release in a frozen epoch) whose `Ended` record is not
    /// written yet. Nothing is given, re-offered, taken or nudged meanwhile,
    /// and nothing restores it in this process.
    pub(crate) fn hold_rec_dirty(&self) -> bool {
        matches!(self.hold_record(), Some(HoldRecord::Held { .. }))
            && !self.lease.epoch_held()
            && !self.pr.restore_pending
    }

    /// The record is `Ended` for exactly `key`, with `reclaim`.
    pub(crate) fn hold_ended_is(&self, key: HoldKey, reclaim: Reclaim) -> bool {
        matches!(self.hold_record(), Some(HoldRecord::Ended { key: k, reclaim: r, .. })
            if *k == key && *r == reclaim)
    }

    /// The record is about `key` (any kind).
    pub(crate) fn hold_has_record_of(&self, key: HoldKey) -> bool {
        self.hold_record().is_some_and(|r| r.key() == key)
    }

    /// The carried lease, when this node is its carrier.
    pub(crate) fn my_carried_key(&self) -> Option<HoldKey> {
        self.pr.carried.filter(|c| c.node == self.cfg.node_id)
    }

    /// The transfer number a carrier's adoption of the hold of `key` takes,
    /// or `None` when the record forbids the adoption (a record of `key`
    /// of any kind but the closed-carried re-hold of an `Ended`, one this
    /// node cannot read).
    pub(super) fn adoption_number(&self, key: HoldKey, via: Adopt) -> Option<XferNo> {
        match (&self.pr.hold_rec, via) {
            (HoldRecState::Unreadable | HoldRecState::RollingBack(_), _) => None,
            (HoldRecState::Known(None), _) => Some(XferNo(0)),
            (HoldRecState::Known(Some(r)), _) if r.key() != key => Some(XferNo(0)),
            // (An `Ended` of either kind: the close this node made, or an
            // involuntary end that a report-driven close followed; the
            // closed lease this activation carries stood in S3 all along.)
            (HoldRecState::Known(Some(HoldRecord::Ended { n, .. })), Adopt::ClosedCarried)
                if key.node == self.cfg.node_id =>
            {
                Some(*n)
            }
            (HoldRecState::Known(Some(_)), _) => None,
        }
    }

    /// The carrier takes (adopts) the hold of its carried lease. The record
    /// `Held { key, n, at }` is written durably first: no in-memory hold
    /// without a durable record of it. `false`: no hold (the record forbids
    /// it, or it could not be written); the caller's event does not adopt,
    /// and a failed write sets `adopt_retry` to `via`: the next report and
    /// round try again the same way.
    pub(crate) fn adopt_epoch_hold(
        &mut self,
        now: Now,
        epoch: Epoch,
        via: Adopt,
        replica: &dyn Replica,
    ) -> bool {
        let Some(key) = self.my_carried_key() else {
            return false;
        };
        // (A write that failed earlier is rolled back first: the retry is
        // then not one event late. A record still unknown is not a refusal
        // of the record: the adoption is tried again, the way it was.)
        self.hold_rec_load(replica);
        if self.pr.hold_rec.is_unknown() {
            self.pr.adopt_retry = Some(via);
            return false;
        }
        let Some(n) = self.adoption_number(key, via) else {
            return false;
        };
        if !self.persist_hold_rec(replica, HoldRecord::Held { key, n, at: epoch }) {
            self.stats.epoch_hold_adopt_persist_failed += 1;
            self.pr.adopt_retry = Some(via);
            return false;
        }
        self.pr.adopt_retry = None;
        if via == Adopt::ClosedCarried {
            // Counted once the `Held` write succeeded, wherever it was
            // tried (the activation, a report, a round).
            self.stats.epoch_closed_leases_reheld += 1;
        }
        self.adopt_epoch_hold_in_memory(now, epoch, replica);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> HoldKey {
        Carrier {
            node: 1,
            epoch: 4,
            expires_unix_ms: 1_700_000_123,
        }
    }

    #[test]
    fn every_record_round_trips() {
        let records = [
            HoldRecord::Held {
                key: key(),
                n: XferNo(0),
                at: 4,
            },
            HoldRecord::Held {
                key: key(),
                n: XferNo(u32::MAX),
                at: 5,
            },
            HoldRecord::Given {
                key: key(),
                n: XferNo(3),
                to: 2,
            },
            HoldRecord::Ended {
                key: key(),
                n: XferNo(2),
                reclaim: Reclaim::Owed,
            },
            HoldRecord::Ended {
                key: key(),
                n: XferNo(2),
                reclaim: Reclaim::Settled,
            },
            HoldRecord::Held {
                key: Carrier {
                    node: 3,
                    epoch: 1,
                    expires_unix_ms: -5,
                },
                n: XferNo(7),
                at: 2,
            },
        ];
        for r in records {
            let text = r.to_string();
            assert_eq!(text.parse::<HoldRecord>(), Ok(r), "{text}");
        }
        assert_eq!(
            HoldRecord::Given {
                key: key(),
                n: XferNo(3),
                to: 2
            }
            .to_string(),
            "g:1:4:1700000123:3:2"
        );
    }

    #[test]
    fn every_malformed_form_is_an_error_never_no_record() {
        for bad in [
            "",
            "0",
            "4",
            "h",
            "h:1:4:5:0",
            "h:1:4:5:0:4:9",
            "x:1:4:5:0:4",
            "H:1:4:5:0:4",
            "h:a:4:5:0:4",
            "h:1:-4:5:0:4",
            "h:1:4:x:0:4",
            "h:1:4:5:-1:4",
            "h:1:4:5:4294967296:4",
            "h:1:4:5:0:",
            "g:1:4:5:0:two",
            "e:1:4:5:0:x",
            "e:1:4:5:0:",
            "e:1:4:5:0:os",
            " h:1:4:5:0:4",
            "h:1:4:5:0:4 ",
        ] {
            assert!(bad.parse::<HoldRecord>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn highest_is_the_records_own_number_for_its_key_only() {
        let held = HoldRecord::Held {
            key: key(),
            n: XferNo(5),
            at: 4,
        };
        let known = HoldRecState::Known(Some(held));
        assert_eq!(known.highest(key()), Some(XferNo(5)));
        let other = Carrier {
            expires_unix_ms: key().expires_unix_ms + 1,
            ..key()
        };
        assert_eq!(known.highest(other), Some(XferNo(0)));
        assert_eq!(HoldRecState::Known(None).highest(key()), Some(XferNo(0)));
        assert_eq!(HoldRecState::Unreadable.highest(key()), None);
    }

    #[test]
    fn the_number_after_the_last_is_none() {
        assert_eq!(XferNo(u32::MAX).next(), None);
        assert_eq!(XferNo(1).next(), Some(XferNo(2)));
    }
}
