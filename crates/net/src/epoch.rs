//! Continuation-epoch state machine (DESIGN.md §5.3).
//!
//! An epoch is legal only when the live P2P component contains every
//! write-eligible node. Members persist a **promise** locally before
//! the epoch activates; that promise is what makes the two safety
//! rules crash-safe:
//!
//! 1. Lose contact with any other member mid-epoch → freeze (read-only).
//!    The remaining nodes cannot know whether the departed one can
//!    still reach S3.
//! 2. An open promise forbids taking epoch-held leases via S3 (the
//!    ordinary expired-lease takeover path) until holders have flushed
//!    and the epoch is closed. Both sides freeze; no conflict.
//!
//! There is no global epoch object. S3 CAS on the log is the
//! serialization point when S3 returns; the epoch only justifies who
//! could write while it was gone.
//!
//! The machine is clock-free: liveness is whatever set of member ids
//! the caller observed (typically via ping). Tests inject that set
//! directly — the "loopback transport" the plan asks for.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpochState {
    Promised,
    Active,
    Frozen,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Promise {
    pub epoch_id: String,
    pub members: Vec<u64>,
    pub base: BTreeMap<String, u64>,
    pub promised_at: i64,
    pub state: EpochState,
}

impl Promise {
    pub fn new(
        epoch_id: impl Into<String>,
        mut members: Vec<u64>,
        base: BTreeMap<String, u64>,
        promised_at: i64,
    ) -> Self {
        members.sort_unstable();
        members.dedup();
        Self {
            epoch_id: epoch_id.into(),
            members,
            base,
            promised_at,
            state: EpochState::Promised,
        }
    }
}

/// Pure epoch protocol. Persistence and P2P are the caller's job; this
/// type only decides legal transitions.
#[derive(Debug, Clone, Default)]
pub struct Machine {
    current: Option<Promise>,
}

impl Machine {
    pub fn new() -> Self {
        Self { current: None }
    }

    pub fn from_promise(p: Promise) -> Self {
        Self { current: Some(p) }
    }

    pub fn current(&self) -> Option<&Promise> {
        self.current.as_ref()
    }

    /// Persist a promise. Refusing a *different* open epoch is load-
    /// bearing: a node cannot be in two epochs at once, and a crash
    /// mid-propose must resume the one already on disk.
    pub fn persist_promise(&mut self, p: Promise) -> Result<(), &'static str> {
        match &self.current {
            None => {
                self.current = Some(p);
                Ok(())
            }
            Some(cur) if cur.epoch_id == p.epoch_id => {
                // Idempotent replay of the same propose.
                Ok(())
            }
            Some(cur) if cur.state == EpochState::Promised && p.epoch_id < cur.epoch_id => {
                // Concurrent proposers deterministically converge on the
                // lexicographically smallest id before either activates.
                self.current = Some(p);
                Ok(())
            }
            Some(cur) if cur.state == EpochState::Closed => {
                self.current = Some(p);
                Ok(())
            }
            Some(_) => Err("an open epoch promise already exists"),
        }
    }

    /// [`Self::persist_promise`] for a proposal from `proposer`, which may
    /// also replace this node's open epoch: the closed hold owner's fresh
    /// epoch (chunk epoch-liveness-gap). `carrier` is the node whose lease
    /// the open epoch carries (`None` if unknown), `holds` whether this
    /// node owns that epoch's hold.
    ///
    /// The members of an epoch carrying another node's lease stay in it
    /// until the carried lease has moved in S3 (the core's
    /// `epoch_carrier_checked`). Its hold owner closes as soon as S3 is
    /// back, and its re-claim moves the lease; if S3 is cut again before
    /// the re-claim lands, every other member is still in the last epoch,
    /// so no new one can form and the lease, with the lock grants it kept,
    /// lapses at its expiry. So an activated (active or frozen) epoch is
    /// replaced by a proposal from its carrier, when this node does not
    /// own the hold itself. That is safe because:
    ///
    /// - The proposer is in no open epoch (`maybe_propose` returns while
    ///   one is), so it closed this one, which leaves no hold of it
    ///   anywhere. A carrier that owned the hold ended it at its close. A
    ///   carrier that had handed the hold on closes only once the carried
    ///   lease has moved, and that lease moves only by the hold owner's
    ///   flush after its own close (the members promise nothing while
    ///   open, so nobody else can take it). A carrier that owed the move
    ///   (`owes_move`) never held. The carrier never abandons the epoch
    ///   for its own retirement (`abandon_if_carrier_retired`), the one
    ///   path that leaves a member closed beside a live hold.
    /// - This node goes from the open epoch straight to the proposal, but
    ///   it may still close before the carried lease moved: the proposer
    ///   aborts a proposal another member refused (`EpochAbort`), or the
    ///   new epoch carries no lease and closes on the first S3 probe.
    ///   That is safe for another reason: no hold of the old epoch
    ///   remains (above), and the carrier's remaining authority, its
    ///   closed claim, ends at the closed lease's expiry. It holds again
    ///   only through the activation of an epoch it acked while that
    ///   claim was usable, and an aborted proposal is never activated. So
    ///   this node, closed, is any closed node: a taker outside an epoch
    ///   waits out the expiry it reads, and the promises and the join
    ///   gate keep takers and epochs apart.
    /// - The new epoch is formed and resolved like any other: its carrier
    ///   is the claim at the highest epoch among its members' acks. The
    ///   closed hold owner claims the lease its close let go while the
    ///   re-claim is pending (`epoch_closed_claim`), so the new epoch
    ///   usually carries that same lease, and the members again wait for
    ///   it to move.
    ///
    /// Only an activated epoch: a merely promised one may not have been
    /// activated here, and the carrier this node knows is then an older
    /// epoch's.
    pub fn persist_promise_proposed(
        &mut self,
        p: Promise,
        proposer: u64,
        carrier: Option<u64>,
        holds: bool,
    ) -> Result<(), &'static str> {
        let refused = match self.persist_promise(p.clone()) {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let succeeds = self.current.as_ref().is_some_and(|cur| {
            matches!(cur.state, EpochState::Active | EpochState::Frozen)
                && carrier == Some(proposer)
                && !holds
                && cur.members.contains(&proposer)
                && p.members.contains(&proposer)
        });
        if !succeeds {
            return Err(refused);
        }
        self.current = Some(p);
        Ok(())
    }

    pub fn activate(&mut self, epoch_id: &str) -> Result<(), &'static str> {
        let cur = self.current.as_mut().ok_or("no promise to activate")?;
        if cur.epoch_id != epoch_id {
            return Err("activate for a different epoch");
        }
        match cur.state {
            EpochState::Promised | EpochState::Active => {
                cur.state = EpochState::Active;
                Ok(())
            }
            EpochState::Frozen => Ok(()), // activation does not unfreeze
            EpochState::Closed => Err("cannot activate a closed epoch"),
        }
    }

    /// Rule 1: any missing member while promised/active/frozen freezes
    /// the node immediately. All members visible again unfreezes to
    /// Active (the epoch resumes) unless it was already closed.
    pub fn note_live_members(&mut self, live: &[u64]) {
        let Some(cur) = self.current.as_mut() else {
            return;
        };
        if cur.state == EpochState::Closed || cur.state == EpochState::Promised {
            return;
        }
        let all_here = cur.members.iter().all(|m| live.contains(m));
        if !all_here {
            cur.state = EpochState::Frozen;
        } else if cur.state == EpochState::Frozen {
            cur.state = EpochState::Active;
        }
    }

    pub fn close(&mut self) {
        if let Some(cur) = self.current.as_mut() {
            cur.state = EpochState::Closed;
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.current.as_ref().map(|p| p.state),
            Some(EpochState::Active)
        )
    }

    pub fn is_frozen(&self) -> bool {
        matches!(
            self.current.as_ref().map(|p| p.state),
            Some(EpochState::Frozen)
        )
    }

    pub fn is_open(&self) -> bool {
        matches!(
            self.current.as_ref().map(|p| p.state),
            Some(EpochState::Promised | EpochState::Active | EpochState::Frozen)
        )
    }

    /// Rule 2: an open promise forbids S3 takeover of epoch-held leases.
    pub fn blocks_s3_takeover(&self) -> bool {
        self.is_open()
    }

    /// Writes during an epoch are allowed only while Active. Frozen is
    /// EROFS. No epoch means the ordinary lease path applies.
    pub fn epoch_writes_ok(&self) -> bool {
        self.is_active()
    }

    pub fn status(&self) -> (bool, Option<String>, Vec<u64>) {
        match &self.current {
            Some(p) if p.state != EpochState::Closed => (
                p.state == EpochState::Active,
                Some(p.epoch_id.clone()),
                p.members.clone(),
            ),
            _ => (false, None, Vec::new()),
        }
    }
}

/// Live P2P component covers the write-eligible roster (self plus every
/// connected peer that is in `roster`).
pub fn component_covers_roster(self_id: u64, connected_peer_ids: &[u64], roster: &[u64]) -> bool {
    if roster.is_empty() {
        return false;
    }
    roster
        .iter()
        .all(|&id| id == self_id || connected_peer_ids.contains(&id))
}

/// Plan 30 §M10: an [`crate::Payload::EpochActivate`] as a service sees
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Activation {
    pub epoch_id: String,
    pub members: Vec<u64>,
    pub base: Vec<(String, u64)>,
    pub carrier: Option<crate::message::EpochCarrier>,
    pub stale_below: u64,
}

/// Plan 30 §M10: the members a flexible-quorum epoch would have — every
/// roster node in the live component (self plus connected peers) — if
/// they are at least `N − epoch_slack` and include this node. With
/// `epoch_slack = 0` this is [`component_covers_roster`]. The quorum is
/// only half of the rule: each member also joins only once its own last
/// issued heartbeat promise has expired (`store_s3::heartbeat`).
pub fn component_quorum(
    self_id: u64,
    connected_peer_ids: &[u64],
    roster: &[u64],
    epoch_slack: u32,
) -> Option<Vec<u64>> {
    if !roster.contains(&self_id) {
        return None;
    }
    let mut members: Vec<u64> = roster
        .iter()
        .copied()
        .filter(|id| *id == self_id || connected_peer_ids.contains(id))
        .collect();
    members.sort_unstable();
    members.dedup();
    let quorum = roster.len().checked_sub(epoch_slack as usize)?.max(1);
    (members.len() >= quorum).then_some(members)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn promised(members: &[u64]) -> Machine {
        let mut m = Machine::new();
        m.persist_promise(Promise::new("e1", members.to_vec(), BTreeMap::new(), 1))
            .unwrap();
        m
    }

    #[test]
    fn promise_then_activate() {
        let mut m = promised(&[1, 2]);
        assert!(m.is_open());
        assert!(!m.is_active());
        m.activate("e1").unwrap();
        assert!(m.is_active());
        assert!(m.blocks_s3_takeover());
    }

    #[test]
    fn persist_is_required_before_activate() {
        let mut m = Machine::new();
        assert_eq!(m.activate("e1"), Err("no promise to activate"));
    }

    #[test]
    fn second_distinct_promise_is_refused() {
        let mut m = promised(&[1, 2]);
        assert_eq!(
            m.persist_promise(Promise::new("e2", vec![1, 2], BTreeMap::new(), 2)),
            Err("an open epoch promise already exists")
        );
    }

    fn active(members: &[u64]) -> Machine {
        let mut m = promised(members);
        m.activate("e1").unwrap();
        m
    }

    fn proposal(id: &str, members: &[u64]) -> Promise {
        Promise::new(id, members.to_vec(), BTreeMap::new(), 2)
    }

    /// Chunk epoch-liveness-gap: a member of an activated epoch carrying
    /// node 1's lease accepts node 1's fresh proposal (node 1 closed that
    /// epoch, its re-claim cut off), active or frozen.
    #[test]
    fn the_carriers_fresh_epoch_replaces_an_activated_one() {
        for frozen in [false, true] {
            let mut m = active(&[1, 2, 3]);
            if frozen {
                m.note_live_members(&[2, 3]);
                assert!(m.is_frozen());
            }
            m.persist_promise_proposed(proposal("e2", &[1, 2, 3]), 1, Some(1), false)
                .unwrap();
            let cur = m.current().unwrap();
            assert_eq!(
                (cur.epoch_id.as_str(), cur.state),
                ("e2", EpochState::Promised)
            );
            assert!(m.is_open(), "never closed in between");
            m.activate("e2").unwrap();
            assert!(m.is_active());
        }
    }

    /// The cases that keep refusing: another proposer (a competing closed
    /// node, or a member that abandoned the epoch beside a live hold), an
    /// unknown carrier, a member owning the hold (handed to it by the
    /// carrier), a merely promised epoch (its carrier is not known here),
    /// and a proposer outside the open epoch.
    #[test]
    fn only_the_carrier_replaces_an_open_epoch_and_never_at_the_hold_owner() {
        let refused = Err("an open epoch promise already exists");
        let mut m = active(&[1, 2, 3]);
        let p = proposal("e2", &[1, 2, 3]);
        assert_eq!(
            m.persist_promise_proposed(p.clone(), 3, Some(1), false),
            refused
        );
        assert_eq!(
            m.persist_promise_proposed(p.clone(), 1, None, false),
            refused
        );
        assert_eq!(
            m.persist_promise_proposed(p.clone(), 1, Some(1), true),
            refused
        );
        assert_eq!(m.current().unwrap().epoch_id, "e1");
        assert!(m.is_active());
        let mut m = promised(&[1, 2, 3]);
        assert_eq!(
            m.persist_promise_proposed(proposal("e2", &[1, 2, 3]), 1, Some(1), false),
            refused
        );
        let mut m = active(&[2, 3]);
        assert_eq!(
            m.persist_promise_proposed(proposal("e2", &[1, 2, 3]), 1, Some(1), false),
            refused
        );
        // The ordinary rules are unchanged: the same id is idempotent, and
        // a closed epoch takes anyone's proposal.
        let mut m = active(&[1, 2, 3]);
        m.persist_promise_proposed(proposal("e1", &[1, 2, 3]), 3, Some(1), false)
            .unwrap();
        assert!(m.is_active());
        m.close();
        m.persist_promise_proposed(proposal("e3", &[2, 3]), 3, Some(1), true)
            .unwrap();
        assert_eq!(m.current().unwrap().epoch_id, "e3");
    }

    /// Discipline rule 1: lose any member → freeze (read-only).
    #[test]
    fn losing_a_member_mid_epoch_freezes() {
        let mut m = promised(&[1, 2]);
        m.activate("e1").unwrap();
        m.note_live_members(&[1, 2]);
        assert!(m.is_active());
        m.note_live_members(&[1]); // 2 vanished
        assert!(m.is_frozen());
        assert!(!m.epoch_writes_ok());
        assert!(m.blocks_s3_takeover());
    }

    /// Returning member unfreezes: the epoch resumes.
    #[test]
    fn returning_member_unfreezes() {
        let mut m = promised(&[1, 2]);
        m.activate("e1").unwrap();
        m.note_live_members(&[1]);
        assert!(m.is_frozen());
        m.note_live_members(&[1, 2]);
        assert!(m.is_active());
    }

    /// Discipline rule 2: open promise forbids S3 takeover, even after
    /// freeze, until close (holders flushed).
    #[test]
    fn open_promise_blocks_s3_takeover_until_close() {
        let mut m = promised(&[1, 2]);
        assert!(m.blocks_s3_takeover(), "promised is already binding");
        m.activate("e1").unwrap();
        m.note_live_members(&[1]);
        assert!(m.blocks_s3_takeover());
        m.close();
        assert!(!m.blocks_s3_takeover());
        assert!(!m.is_open());
    }

    #[test]
    fn roster_coverage() {
        assert!(component_covers_roster(1, &[2], &[1, 2]));
        assert!(!component_covers_roster(1, &[2], &[1, 2, 3]));
        assert!(component_covers_roster(1, &[], &[1]));
        assert!(!component_covers_roster(1, &[], &[1, 2]));
    }

    #[test]
    fn flexible_quorum_takes_n_minus_f_roster_members() {
        // f = 0 is `component_covers_roster`.
        assert_eq!(
            component_quorum(1, &[2, 3], &[1, 2, 3], 0),
            Some(vec![1, 2, 3])
        );
        assert_eq!(component_quorum(1, &[2], &[1, 2, 3], 0), None);
        // f = 1: two of three.
        assert_eq!(component_quorum(1, &[2], &[1, 2, 3], 1), Some(vec![1, 2]));
        assert_eq!(component_quorum(1, &[], &[1, 2, 3], 1), None);
        // Connected peers outside the roster (read-only, retired) do not
        // count; this node must be in the roster.
        assert_eq!(component_quorum(1, &[7, 8], &[1, 2, 3], 1), None);
        assert_eq!(component_quorum(9, &[1, 2], &[1, 2, 3], 1), None);
        // Never an epoch of nobody.
        assert_eq!(component_quorum(1, &[], &[1], 5), None);
        assert_eq!(component_quorum(1, &[], &[1], 0), Some(vec![1]));
    }
}
