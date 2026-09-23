//! The `journal` keyspace (the local write-ahead log FUSE mutations and
//! replay both append to), and the `reintegration` bookkeeping keyspace
//! that rides alongside it. (Requester shadows moved to the speculation
//! log, `store::spec`, in plan 30 §M3a.)
//!
//! `seq` is a `u64` big-endian key; SQLite's `AUTOINCREMENT` (monotonic,
//! never reused even across deletes) is replaced by an explicit counter
//! persisted in `local` (`next_journal_seq`), incremented in the same
//! write transaction as the append — the counter itself is never rolled
//! back, so a deleted seq is never reissued.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::rid::Rid;
use crate::store::{kv_set_tx, Meta, KV_NEXT_JOURNAL_SEQ};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

fn seq_key(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

/// Plan 30 §M2: every mutation now calls this at least twice per
/// transaction (the op's own record, then `Completed`), so the decimal-
/// string `kv_get_tx`/`kv_set_tx` round trip this used before (parse on
/// read, `to_string()`-allocate on write) is worth avoiding — raw
/// big-endian bytes on the same `local` key, read/written directly
/// against the transaction. `KV_NEXT_JOURNAL_SEQ` has no other reader
/// (checked: only this function touches it), so changing its on-disk
/// encoding is safe without a migration (plan 30: no backward
/// compatibility required at any level).
fn next_seq_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let key = KV_NEXT_JOURNAL_SEQ.as_bytes();
    let next: u64 = match tx.get(local, key)? {
        Some(v) => u64::from_be_bytes(
            v.as_ref()
                .try_into()
                .map_err(|_| MetaError::Invalid("next_journal_seq".into()))?,
        ),
        None => 1,
    };
    tx.insert(local, key.to_vec(), (next + 1).to_be_bytes().to_vec());
    Ok(next)
}

/// The journal counter's next value, without advancing it: where the
/// caller's transaction's first appended row will land
/// (`store::local::LocalTx`).
pub(crate) fn peek_next_seq(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    match r.get(local, KV_NEXT_JOURNAL_SEQ.as_bytes())? {
        Some(v) => {
            Ok(u64::from_be_bytes(v.as_ref().try_into().map_err(|_| {
                MetaError::Invalid("next_journal_seq".into())
            })?))
        }
        None => Ok(1),
    }
}

thread_local! {
    static PENDING_COMPLETION: std::cell::Cell<Option<Rid>> = const { std::cell::Cell::new(None) };
    static PENDING_OP: std::cell::RefCell<Option<(Option<Rid>, crate::mutate::MutateOp)>> =
        const { std::cell::RefCell::new(None) };
}

/// Plan 30 §M3b: the op (and rid, if any) `mutate::execute` is running,
/// for `store::local`'s `journal_tx` row. The same thread-local scoping
/// as [`PendingCompletion`], for the same reason: the op's own
/// transaction runs on this call stack, and nothing else can see it. A
/// replay after a deposition re-executes exactly this op by this rid.
pub(crate) struct PendingLocalOp;

impl PendingLocalOp {
    pub(crate) fn set(rid: Option<Rid>, op: &crate::mutate::MutateOp) -> PendingLocalOpGuard {
        PENDING_OP.with(|c| *c.borrow_mut() = Some((rid, op.clone())));
        PendingLocalOpGuard
    }

    /// Taken by the first journaled transaction that finishes while it is
    /// set, so a second transaction in the same call (none today) could
    /// not claim the same op.
    pub(crate) fn take() -> Option<(Option<Rid>, crate::mutate::MutateOp)> {
        PENDING_OP.with(|c| c.borrow_mut().take())
    }
}

pub(crate) struct PendingLocalOpGuard;

impl Drop for PendingLocalOpGuard {
    fn drop(&mut self) {
        PENDING_OP.with(|c| *c.borrow_mut() = None);
    }
}

/// The rid `mutate::execute` is completing, visible only to the thread
/// running it.
///
/// Every `Meta` mutating method opens, fills and commits its fjall write
/// transaction synchronously on the calling thread, so a thread-local set
/// for the duration of one `execute` call reaches exactly that op's
/// transaction and nothing else: a snapshot, bootstrap or atime write
/// appending to the journal on another thread at the same moment cannot
/// take it. The guard clears it on drop, so an error or early return never
/// leaks a rid into the next call on this thread.
pub(crate) struct PendingCompletion;

impl PendingCompletion {
    pub(crate) fn set(rid: Option<Rid>) -> PendingCompletionGuard {
        PENDING_COMPLETION.with(|c| c.set(rid));
        PendingCompletionGuard
    }

    fn take() -> Option<Rid> {
        PENDING_COMPLETION.with(|c| c.take())
    }
}

pub(crate) struct PendingCompletionGuard;

impl Drop for PendingCompletionGuard {
    fn drop(&mut self) {
        PENDING_COMPLETION.with(|c| c.set(None));
    }
}

/// Append `record` under a fresh seq, in the caller's write transaction.
///
/// Plan 30 §M2: if `execute` (`mutate.rs`) has a rid pending completion
/// on this thread (see [`PendingCompletion`]), the *first* call to `append_tx`
/// within it also appends `LogRecord::Completed { rid }` under its own
/// fresh seq and writes `rid -> position` into `completed` — all before
/// this same transaction commits, so an op's own record(s) and its
/// completion marker are atomic: either both are durable or neither is.
/// `pending` is taken (cleared) on first use, so a method that calls
/// `append_tx` more than once per transaction (e.g. `publish_file`,
/// which appends three records) only completes the op once. See
/// [`PendingCompletion`] for why a thread-local is safe here rather than
/// threading a rid through every mutating method.
#[allow(clippy::too_many_arguments)]
pub(crate) fn append_tx(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    completed: &SingleWriterTxKeyspace,
    record: &LogRecord,
) -> Result<u64, MetaError> {
    let seq = next_seq_tx(tx, local)?;
    tx.insert(journal, seq_key(seq), record.to_postcard()?);
    if let Some(rid) = PendingCompletion::take() {
        let cseq = next_seq_tx(tx, local)?;
        tx.insert(
            journal,
            seq_key(cseq),
            LogRecord::Completed { rid }.to_postcard()?,
        );
        let now_ms = constellation_fs_core::types::now_ns() / 1_000_000;
        tx.insert(
            completed,
            rid.to_key(),
            Meta::encode_completed_row(cseq, now_ms),
        );
    }
    Ok(seq)
}

fn decode_row(k: &[u8], v: &[u8]) -> Result<(u64, LogRecord), MetaError> {
    let seq = u64::from_be_bytes(
        k.try_into()
            .map_err(|_| MetaError::Invalid("journal key".into()))?,
    );
    Ok((seq, LogRecord::from_postcard(v)?))
}

/// Plan 30 §M3b: every journal row at or below this is deleted (see
/// `store::KV_JOURNAL_ACKED`). Scans start after it: the LSM keeps the
/// shipped history's tombstones until compaction, and walking them from
/// the start of the keyspace on every round cost time proportional to
/// everything ever shipped, not to the backlog.
pub(crate) fn acked_watermark(
    r: &impl Readable,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    crate::store::counter_get(r, local, crate::store::KV_JOURNAL_ACKED)
}

fn note_acked_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
    upto: u64,
) -> Result<(), MetaError> {
    // Never past the last seq handed out: an "ack everything" caller may
    // pass `u64::MAX`, and a row appended later must still be above it.
    let upto = upto.min(peek_next_seq(&*tx, local)?.saturating_sub(1));
    if upto > acked_watermark(tx, local)? {
        crate::store::counter_set_tx(tx, local, crate::store::KV_JOURNAL_ACKED, upto);
    }
    Ok(())
}

pub(crate) fn take(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    max: usize,
) -> Result<Vec<(u64, LogRecord)>, MetaError> {
    let from = acked_watermark(r, local)?.saturating_add(1);
    let mut out = Vec::new();
    for guard in r.range(journal, seq_key(from)..) {
        if out.len() >= max {
            break;
        }
        let (k, v) = guard.into_inner()?;
        out.push(decode_row(&k, &v)?);
    }
    Ok(out)
}

pub(crate) fn peek_after(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    after_seq: u64,
) -> Result<Vec<(u64, LogRecord)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.range(journal, seq_key(after_seq + 1)..) {
        let (k, v) = guard.into_inner()?;
        out.push(decode_row(&k, &v)?);
    }
    Ok(out)
}

pub(crate) fn max_seq(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    match r.last_key_value(journal) {
        Some(guard) => {
            let k = guard.key()?;
            Ok(u64::from_be_bytes(
                k.as_ref()
                    .try_into()
                    .map_err(|_| MetaError::Invalid("journal key".into()))?,
            ))
        }
        None => Ok(0),
    }
}

/// Rows still in the journal: proportional to the backlog (see
/// [`acked_watermark`]).
pub(crate) fn len(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let from = acked_watermark(r, local)?.saturating_add(1);
    let mut n = 0u64;
    for guard in r.range(journal, seq_key(from)..) {
        guard.key()?;
        n += 1;
    }
    Ok(n)
}

pub(crate) fn ack_upto(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    upto_seq: u64,
) -> Result<(), MetaError> {
    let from = acked_watermark(tx, local)?.saturating_add(1);
    let keys: Vec<Vec<u8>> = tx
        .range(journal, seq_key(from)..=seq_key(upto_seq))
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(journal, k);
    }
    note_acked_tx(tx, local, upto_seq)
}

/// Remove the shipped rows `seqs` and record the segment they shipped in
/// as applied. Plan 30 §M3b: the position advances even for an empty
/// `seqs` — a takeover's epoch-marker segment carries no journal rows but
/// is still this replica's own, applied segment.
///
/// `seqs` is usually the journal's head, so its highest seq becomes the
/// acked watermark. Plan 30 §M4: a ship may skip held-back transactions
/// (`store::held`), leaving rows below that; the watermark then stops
/// just below the oldest one still here, so every journal scan (which
/// starts past the watermark) keeps seeing it. Only then (`held_below`,
/// from `spec::retire_local_tx`'s scan) is that row looked up; the
/// ordinary ack does no extra read (plan 30 §M4 round 2).
pub(crate) fn ack_rows_at(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    seqs: &[u64],
    applied_seq: u64,
    held_below: bool,
) -> Result<(), MetaError> {
    for seq in seqs {
        tx.remove(journal, seq_key(*seq));
    }
    if let Some(&upto) = seqs.iter().max() {
        if !held_below {
            note_acked_tx(tx, local, upto)?;
            kv_set_tx(
                tx,
                local,
                crate::store::KV_APPLIED_SEQ,
                &applied_seq.to_string(),
            );
            return Ok(());
        }
        let from = acked_watermark(tx, local)?.saturating_add(1);
        let remaining = match tx.range(journal, seq_key(from)..=seq_key(upto)).next() {
            Some(guard) => {
                let (k, _) = guard.into_inner()?;
                Some(u64::from_be_bytes(
                    k.as_ref()
                        .try_into()
                        .map_err(|_| MetaError::Invalid("journal key".into()))?,
                ))
            }
            None => None,
        };
        let upto = match remaining {
            Some(held) => held.saturating_sub(1),
            None => upto,
        };
        note_acked_tx(tx, local, upto)?;
    }
    kv_set_tx(
        tx,
        local,
        crate::store::KV_APPLIED_SEQ,
        &applied_seq.to_string(),
    );
    Ok(())
}

// ------------------------------------------------------------ reintegration

pub(crate) fn unmarked(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    reintegration: &SingleWriterTxKeyspace,
) -> Result<Vec<(u64, LogRecord)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.iter(journal) {
        let (k, v) = guard.into_inner()?;
        if r.get(reintegration, k.clone())?.is_some() {
            continue;
        }
        out.push(decode_row(&k, &v)?);
    }
    Ok(out)
}

pub(crate) fn unmarked_len(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    reintegration: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    Ok(unmarked(r, journal, reintegration)?.len() as u64)
}

/// Plan 29 M0a removed namespace partitions; there is exactly one
/// implicit partition (`"p0"`), so this returns `["p0"]` iff there is
/// any unmarked row, matching the old `SELECT DISTINCT part` shape.
pub(crate) fn unmarked_parts(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    reintegration: &SingleWriterTxKeyspace,
) -> Result<Vec<String>, MetaError> {
    if unmarked_len(r, journal, reintegration)? > 0 {
        Ok(vec!["p0".to_string()])
    } else {
        Ok(Vec::new())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ReintegrationRow {
    pub disposition: String,
    pub detail: String,
}

pub(crate) fn conflict_count(
    r: &impl Readable,
    reintegration: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let mut n = 0u64;
    for guard in r.iter(reintegration) {
        let (_, v) = guard.into_inner()?;
        let row: ReintegrationRow = postcard::from_bytes(&v)?;
        if row.disposition == "conflict" {
            n += 1;
        }
    }
    Ok(n)
}
