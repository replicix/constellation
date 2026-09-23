//! Plan 30 §M13: what the metadata store keeps for the S3 inbox.
//!
//! An inbox-submitted op has no reply, so its outcome rides the log and
//! lands here on every replica that tails it:
//!
//! - **Refusals are outcomes.** `LogRecord::Refused { rid, errno }` writes
//!   a `completed` row tagged refused ([`ROW_TAG_REFUSED`]). From then on
//!   every dedup site — the holder's own executor, a successor's takeover
//!   drain, the requester's in-doubt resolution on the lease path —
//!   answers that rid with the errno instead of re-evaluating the op.
//!   This is the one place the inbox path departs from plan 30 §M2's
//!   "refusals are not recorded": there, only the requester ever retried,
//!   and a requester that holds a refusal never retries it; here the
//!   *holder* re-reads batches (a drain) and the requester may re-submit
//!   a rid whose refusal sits in a segment it has not tailed yet. A
//!   re-evaluation of a refused `create(x)` after `x` was unlinked would
//!   execute it — the caller was told `EEXIST` and `x` appears anyway,
//!   which the Stateright model reproduces
//!   (`crates/model/tests/inbox.rs::naive_refusal_without_dedup_creates_phantoms`).
//! - **The position watermark.** `LogRecord::InboxAck { epoch, node, n,
//!   i }` advances a node-local `local` key per `(epoch, node)` to the
//!   highest batch position answered. Unlike `completed` rows, the
//!   watermark is never pruned by retention, so a takeover drain older
//!   than `CONSTELLATION_COMPLETION_RETENTION_S` still skips every
//!   position at or below it — the drain never has to trust a clock. It
//!   is a prefix because the holder executes a requester's batches in
//!   order and acks every position, executed, refused or deduplicated.
//!
//! Both records are appended by the holder in the transaction that
//! decides the outcome ([`journal::PendingInboxAck`] rides with
//! `PendingCompletion` through `append_tx`), and applied by every
//! tailing replica in `replay::apply_one`. The holder itself never
//! replays its own segments, which is why the refusal row and the
//! watermark are written at journal time as well.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::rid::Rid;
use crate::store::journal::{self, PendingInboxAck, PendingInboxAckGuard};
use crate::store::{kv_get_tx, kv_set_tx, Meta};
use fjall::{Readable, SingleWriterWriteTx};

/// The armed state of [`Meta::pending_inbox_ack`]; dropping it disarms.
pub struct InboxAckArmed(#[allow(dead_code)] PendingInboxAckGuard);

/// Outcome tag byte at offset 16 of a `completed` row: a refusal. An
/// executed row is exactly 16 bytes and carries no tag.
pub const ROW_TAG_REFUSED: u8 = 1;

/// The `(epoch, node, n, i)` position of one op in one inbox batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InboxAck {
    pub epoch: u64,
    pub node: u64,
    pub n: u64,
    pub i: u32,
}

impl InboxAck {
    pub fn record(self) -> LogRecord {
        LogRecord::InboxAck {
            epoch: self.epoch,
            node: self.node,
            n: self.n,
            i: self.i,
        }
    }

    fn key(epoch: u64, node: u64) -> String {
        format!("inbox_ack:{epoch:016x}:{node:016x}")
    }

    /// Whether `(n, i)` is at or below this watermark.
    pub fn covers(self, n: u64, i: u32) -> bool {
        (n, i) <= (self.n, self.i)
    }
}

/// What the log says about a rid: it executed at `position` (this
/// replica's journal seq for the holder's own row, `0` when tailed), or
/// it was refused with `errno`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletedOutcome {
    Executed { position: u64 },
    Refused { errno: i32 },
}

fn decode_outcome(v: &[u8]) -> Result<CompletedOutcome, MetaError> {
    let position = u64::from_be_bytes(
        v.get(0..8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| MetaError::Invalid("completed row".into()))?,
    );
    match v.get(16) {
        Some(&ROW_TAG_REFUSED) => {
            let errno = i32::from_be_bytes(
                v.get(17..21)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| MetaError::Invalid("refused completed row".into()))?,
            );
            Ok(CompletedOutcome::Refused { errno })
        }
        _ => Ok(CompletedOutcome::Executed { position }),
    }
}

/// Advance the `(epoch, node)` watermark to `ack` if it is higher; the
/// watermark only ever moves forward, so a replica tailing an old
/// segment after a newer one (a re-bootstrap) cannot lower it.
pub(crate) fn set_inbox_ack_tx(
    tx: &mut SingleWriterWriteTx,
    local: &fjall::SingleWriterTxKeyspace,
    ack: InboxAck,
) -> Result<(), MetaError> {
    let key = InboxAck::key(ack.epoch, ack.node);
    if let Some((n, i)) = get_inbox_ack_tx(&*tx, local, &key)? {
        if (n, i) >= (ack.n, ack.i) {
            return Ok(());
        }
    }
    kv_set_tx(tx, local, &key, &format!("{}:{}", ack.n, ack.i));
    Ok(())
}

fn get_inbox_ack_tx(
    r: &impl Readable,
    local: &fjall::SingleWriterTxKeyspace,
    key: &str,
) -> Result<Option<(u64, u32)>, MetaError> {
    let Some(v) = kv_get_tx(r, local, key)? else {
        return Ok(None);
    };
    let (n, i) = v
        .split_once(':')
        .ok_or_else(|| MetaError::Invalid("inbox_ack row".into()))?;
    Ok(Some((
        n.parse()
            .map_err(|_| MetaError::Invalid("inbox_ack n".into()))?,
        i.parse()
            .map_err(|_| MetaError::Invalid("inbox_ack i".into()))?,
    )))
}

impl Meta {
    /// Everything the log (or this holder's own unshipped journal) says
    /// about `rid`: executed, refused, or nothing yet.
    pub fn completed_outcome(&self, rid: Rid) -> Result<Option<CompletedOutcome>, MetaError> {
        let r = self.db.read_tx();
        match r.get(&self.completed, rid.to_key())? {
            Some(v) => Ok(Some(decode_outcome(&v)?)),
            None => Ok(None),
        }
    }

    /// The errno the log refused `rid` with, if it did.
    pub fn refused_errno(&self, rid: Rid) -> Result<Option<i32>, MetaError> {
        Ok(match self.completed_outcome(rid)? {
            Some(CompletedOutcome::Refused { errno }) => Some(errno),
            _ => None,
        })
    }

    /// The highest inbox position of `(epoch, node)` this replica knows
    /// to be answered, if any.
    pub fn inbox_ack(&self, epoch: u64, node: u64) -> Result<Option<InboxAck>, MetaError> {
        let r = self.db.read_tx();
        Ok(
            get_inbox_ack_tx(&r, &self.local, &InboxAck::key(epoch, node))?
                .map(|(n, i)| InboxAck { epoch, node, n, i }),
        )
    }

    /// Arm the thread for the inbox op about to be executed on it: the
    /// first journaled transaction (`execute`'s) also appends `ack`'s
    /// `InboxAck` and advances the watermark, atomically with the op's
    /// records and `Completed`. Dropping the guard disarms it, so an op
    /// that refuses before writing anything leaves no stray ack behind
    /// (the caller then journals the refusal with
    /// [`Meta::journal_inbox_refusal`]).
    pub fn pending_inbox_ack(&self, ack: InboxAck) -> InboxAckArmed {
        InboxAckArmed(PendingInboxAck::set(ack))
    }

    /// The holder refused an inbox op: journal `Refused { rid, errno }`
    /// and the position's `InboxAck` as one transaction with no
    /// namespace writes, write the refused `completed` row, and count it
    /// as this tenure's journaled work (`begin_local`/`finish_local`),
    /// so a deposition rolls the row back with the transaction.
    pub fn journal_inbox_refusal(
        &self,
        rid: Rid,
        errno: i32,
        ack: InboxAck,
    ) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let _pending = PendingInboxAck::set(ack);
        let position = journal::append_tx(
            &mut tx,
            &self.journal_ks,
            &self.local,
            &self.completed,
            &LogRecord::Refused { rid, errno },
        )?;
        let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
        tx.insert(
            &self.completed,
            rid.to_key(),
            Meta::encode_refused_row(position, now_ms, errno),
        );
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    /// An inbox position whose op was deduplicated (its rid already had
    /// an outcome): journal the `InboxAck` alone so the watermark still
    /// advances past it.
    pub fn journal_inbox_ack(&self, ack: InboxAck) -> Result<(), MetaError> {
        let mut tx = self.db.write_tx();
        let local = self.begin_local(&tx)?;
        let seq = journal::next_seq_tx(&mut tx, &self.local)?;
        tx.insert(
            &self.journal_ks,
            journal::seq_key(seq),
            ack.record().to_postcard()?,
        );
        set_inbox_ack_tx(&mut tx, &self.local, ack)?;
        self.finish_local(&mut tx, local)?;
        tx.commit()?;
        Ok(())
    }

    /// The journal seq the next appended row will get.
    pub fn journal_next_seq(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        journal::peek_next_seq(&r, &self.local)
    }

    /// Every journal row at or below this has shipped (plan 30 §M3b's
    /// acked watermark): what the holder's inbox GC compares against.
    pub fn journal_acked_seq(&self) -> Result<u64, MetaError> {
        let r = self.db.read_tx();
        journal::acked_watermark(&r, &self.local)
    }

    /// Plan 30 §M13 (coordinator decision): drop `recent` entries whose
    /// execution has shipped at or below journal seq `upto`. Once
    /// shipped, a rid is in `completed`, which every dedup site already
    /// answers from (plan 30 §M3a), so `recent` only needs to cover the
    /// unshipped window — and an inbox-only requester never sends the
    /// `acked_through` that used to be the only thing shrinking it.
    pub fn prune_recent_shipped(&self, upto: u64) -> Result<u64, MetaError> {
        let rids: Vec<Rid> = {
            let recent = self.recent.lock().unwrap();
            recent
                .iter()
                .flat_map(|((node, incarnation), bucket)| {
                    bucket.keys().map(move |seq| Rid {
                        node: *node,
                        incarnation: *incarnation,
                        seq: *seq,
                    })
                })
                .collect()
        };
        let mut pruned = 0u64;
        for rid in rids {
            if let Some(CompletedOutcome::Executed { position }) = self.completed_outcome(rid)? {
                if position != 0 && position <= upto {
                    self.forget_recent(rid);
                    pruned += 1;
                }
            }
        }
        Ok(pruned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutate::{execute, MutateOp};
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    /// `meta` does not depend on `libc`; these are the Linux values the
    /// `cli` crate maps to.
    const EEXIST: i32 = 17;
    const ENOENT: i32 = 2;

    fn rid(seq: u64) -> Rid {
        Rid {
            node: 7,
            incarnation: 2,
            seq,
        }
    }

    fn ack(n: u64, i: u32) -> InboxAck {
        InboxAck {
            epoch: 3,
            node: 7,
            n,
            i,
        }
    }

    fn create(name: &str) -> MutateOp {
        // A distinct ino per name: two `create` calls with different
        // names in the same `Meta` (e.g. "a" then "b") must not collide
        // on the same inode.
        let ino = name
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31) + b as u64);
        MutateOp::Create {
            parent: ROOT_INO,
            name: name.into(),
            ino: (1 << 40) | (ino & 0xff_ffff) | 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
        }
    }

    #[test]
    fn an_executed_inbox_op_journals_its_ack_with_the_completion() {
        let m = Meta::open_in_memory().unwrap();
        let before = m.journal_len().unwrap();
        {
            let _armed = m.pending_inbox_ack(ack(4, 2));
            execute(&m, &create("f"), Some(rid(1))).unwrap();
        }
        let rows = m.peek_journal_after(before).unwrap();
        let kinds: Vec<&str> = rows
            .iter()
            .map(|(_, r)| match r {
                LogRecord::Completed { .. } => "completed",
                LogRecord::InboxAck { .. } => "ack",
                LogRecord::Refused { .. } => "refused",
                _ => "op",
            })
            .collect();
        assert_eq!(kinds, vec!["op", "completed", "ack"]);
        assert!(matches!(
            m.completed_outcome(rid(1)).unwrap(),
            Some(CompletedOutcome::Executed { position }) if position > 0
        ));
        assert!(m.completed_position(rid(1)).unwrap().is_some());
        assert_eq!(m.refused_errno(rid(1)).unwrap(), None);
        assert_eq!(m.inbox_ack(3, 7).unwrap(), Some(ack(4, 2)));
        assert!(ack(4, 2).covers(4, 2) && ack(4, 2).covers(3, 9) && !ack(4, 2).covers(4, 3));
    }

    #[test]
    fn a_disarmed_guard_leaves_no_ack_behind() {
        let m = Meta::open_in_memory().unwrap();
        {
            let _armed = m.pending_inbox_ack(ack(0, 0));
            // Refused before any write: nothing journaled.
            let unlink = MutateOp::Unlink {
                parent: ROOT_INO,
                name: "nope".into(),
            };
            assert!(execute(&m, &unlink, Some(rid(1))).is_err());
        }
        assert_eq!(m.journal_len().unwrap(), 0);
        assert_eq!(m.inbox_ack(3, 7).unwrap(), None);
        // A later, unrelated op is not acked by the stale arming.
        execute(&m, &create("g"), Some(rid(2))).unwrap();
        assert_eq!(m.inbox_ack(3, 7).unwrap(), None);
    }

    #[test]
    fn a_refusal_is_a_completed_row_that_completed_position_does_not_report() {
        let m = Meta::open_in_memory().unwrap();
        m.journal_inbox_refusal(rid(5), EEXIST, ack(1, 0)).unwrap();
        assert_eq!(m.refused_errno(rid(5)).unwrap(), Some(EEXIST));
        assert_eq!(m.completed_position(rid(5)).unwrap(), None);
        assert_eq!(m.inbox_ack(3, 7).unwrap(), Some(ack(1, 0)));
        let rows = m.peek_journal_after(0).unwrap();
        assert!(
            matches!(rows[0].1, LogRecord::Refused { rid: r, errno } if r == rid(5) && errno == EEXIST)
        );
        assert!(matches!(rows[1].1, LogRecord::InboxAck { n: 1, i: 0, .. }));
        // Retention treats it like any other row (same first 16 bytes).
        let now = constellation_fs_core::types::now_ns() / 1_000_000;
        assert_eq!(m.prune_completed(now + 10, 1).unwrap(), 1);
        assert_eq!(m.refused_errno(rid(5)).unwrap(), None);
    }

    #[test]
    fn a_deduplicated_position_still_advances_the_watermark() {
        let m = Meta::open_in_memory().unwrap();
        m.journal_inbox_ack(ack(2, 5)).unwrap();
        assert_eq!(m.inbox_ack(3, 7).unwrap(), Some(ack(2, 5)));
        // Never backwards.
        m.journal_inbox_ack(ack(1, 9)).unwrap();
        assert_eq!(m.inbox_ack(3, 7).unwrap(), Some(ack(2, 5)));
        m.journal_inbox_ack(ack(2, 6)).unwrap();
        assert_eq!(m.inbox_ack(3, 7).unwrap(), Some(ack(2, 6)));
        // Another (epoch, node) is independent.
        assert_eq!(m.inbox_ack(4, 7).unwrap(), None);
    }

    /// Tailing the records (a follower) lands the same rows the holder
    /// wrote at journal time.
    #[test]
    fn tailed_outcome_records_apply_on_a_follower() {
        let follower = Meta::open_in_memory().unwrap();
        let records = vec![
            LogRecord::Refused {
                rid: rid(8),
                errno: ENOENT,
            },
            ack(6, 1).record(),
        ];
        follower.apply_records_journaled(&records).unwrap();
        assert_eq!(follower.refused_errno(rid(8)).unwrap(), Some(ENOENT));
        assert_eq!(follower.completed_position(rid(8)).unwrap(), None);
        assert_eq!(follower.inbox_ack(3, 7).unwrap(), Some(ack(6, 1)));
    }

    #[test]
    fn recent_is_pruned_once_its_rows_shipped() {
        let m = Meta::open_in_memory().unwrap();
        let recs = execute(&m, &create("a"), Some(rid(1))).unwrap();
        m.remember_outcome(rid(1), &recs);
        let recs = execute(&m, &create("b"), Some(rid(2))).unwrap();
        m.remember_outcome(rid(2), &recs);
        assert!(m.recent_outcome(rid(1)).is_some() && m.recent_outcome(rid(2)).is_some());
        let first_position = m.completed_position(rid(1)).unwrap().unwrap();
        assert_eq!(m.prune_recent_shipped(first_position).unwrap(), 1);
        assert!(m.recent_outcome(rid(1)).is_none(), "shipped: gone");
        assert!(m.recent_outcome(rid(2)).is_some(), "unshipped: kept");
        assert_eq!(m.prune_recent_shipped(u64::MAX).unwrap(), 1);
        assert!(m.recent_outcome(rid(2)).is_none());
    }
}
