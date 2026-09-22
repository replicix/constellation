//! The `journal` keyspace (the local write-ahead log FUSE mutations and
//! replay both append to), and the `reintegration`/`shadow` bookkeeping
//! keyspaces that ride alongside it.
//!
//! `seq` is a `u64` big-endian key; SQLite's `AUTOINCREMENT` (monotonic,
//! never reused even across deletes) is replaced by an explicit counter
//! persisted in `local` (`next_journal_seq`), incremented in the same
//! write transaction as the append — the counter itself is never rolled
//! back, so a deleted seq is never reissued.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::store::{kv_get_tx, kv_set_tx, KV_NEXT_JOURNAL_SEQ, KV_NEXT_SHADOW_ID};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

fn seq_key(seq: u64) -> Vec<u8> {
    seq.to_be_bytes().to_vec()
}

fn next_seq_tx(
    tx: &mut SingleWriterWriteTx,
    local: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let next: u64 = match kv_get_tx(tx, local, KV_NEXT_JOURNAL_SEQ)? {
        Some(s) => s
            .parse()
            .map_err(|_| MetaError::Invalid("next_journal_seq".into()))?,
        None => 1,
    };
    kv_set_tx(tx, local, KV_NEXT_JOURNAL_SEQ, &(next + 1).to_string());
    Ok(next)
}

/// Append `record` under a fresh seq, in the caller's write transaction.
pub(crate) fn append_tx(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    record: &LogRecord,
) -> Result<u64, MetaError> {
    let seq = next_seq_tx(tx, local)?;
    tx.insert(journal, seq_key(seq), record.to_postcard()?);
    Ok(seq)
}

fn decode_row(k: &[u8], v: &[u8]) -> Result<(u64, LogRecord), MetaError> {
    let seq = u64::from_be_bytes(
        k.try_into()
            .map_err(|_| MetaError::Invalid("journal key".into()))?,
    );
    Ok((seq, LogRecord::from_postcard(v)?))
}

pub(crate) fn take(
    r: &impl Readable,
    journal: &SingleWriterTxKeyspace,
    max: usize,
) -> Result<Vec<(u64, LogRecord)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.iter(journal) {
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

pub(crate) fn len(r: &impl Readable, journal: &SingleWriterTxKeyspace) -> Result<u64, MetaError> {
    let mut n = 0u64;
    for guard in r.iter(journal) {
        guard.key()?;
        n += 1;
    }
    Ok(n)
}

pub(crate) fn ack_upto(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    upto_seq: u64,
) -> Result<(), MetaError> {
    let keys: Vec<Vec<u8>> = tx
        .range(journal, ..=seq_key(upto_seq))
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(journal, k);
    }
    Ok(())
}

pub(crate) fn ack_rows_at(
    tx: &mut SingleWriterWriteTx,
    journal: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    seqs: &[u64],
    applied_seq: u64,
) -> Result<(), MetaError> {
    if seqs.is_empty() {
        return Ok(());
    }
    for seq in seqs {
        tx.remove(journal, seq_key(*seq));
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

pub(crate) fn mark_disposition_tx(
    tx: &mut SingleWriterWriteTx,
    reintegration: &SingleWriterTxKeyspace,
    seq: u64,
    disposition: &str,
    detail: &str,
) -> Result<(), MetaError> {
    let key = seq_key(seq);
    if tx.get(reintegration, key.clone())?.is_some() {
        return Ok(());
    }
    let row = ReintegrationRow {
        disposition: disposition.to_string(),
        detail: detail.to_string(),
    };
    tx.insert(reintegration, key, postcard::to_allocvec(&row)?);
    Ok(())
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

// ------------------------------------------------------------------ shadow

fn shadow_key(epoch: u64, id: u64) -> Vec<u8> {
    let mut k = epoch.to_be_bytes().to_vec();
    k.extend_from_slice(&id.to_be_bytes());
    k
}

pub(crate) fn shadow_insert_tx(
    tx: &mut SingleWriterWriteTx,
    shadow: &SingleWriterTxKeyspace,
    local: &SingleWriterTxKeyspace,
    epoch: u64,
    records: &[LogRecord],
) -> Result<(), MetaError> {
    let id: u64 = match kv_get_tx(tx, local, KV_NEXT_SHADOW_ID)? {
        Some(s) => s.parse().unwrap_or(0),
        None => 0,
    };
    kv_set_tx(tx, local, KV_NEXT_SHADOW_ID, &(id + 1).to_string());
    tx.insert(
        shadow,
        shadow_key(epoch, id),
        postcard::to_allocvec(&records.to_vec())?,
    );
    Ok(())
}

pub(crate) fn shadow_retire_matching_tx(
    tx: &mut SingleWriterWriteTx,
    shadow: &SingleWriterTxKeyspace,
    epoch: u64,
    records: &[LogRecord],
) -> Result<(), MetaError> {
    // Keys are `epoch_be ++ id_be`, so every shadow row with `epoch <=
    // given` is a prefix range from the keyspace start.
    let end = {
        let mut e = epoch.to_be_bytes().to_vec();
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e.push(0xff);
        e
    };
    let rows: Vec<(Vec<u8>, Vec<LogRecord>)> = tx
        .range(shadow, Vec::new()..=end)
        .map(|g| {
            g.into_inner().map_err(MetaError::from).and_then(|(k, v)| {
                let recs: Vec<LogRecord> = postcard::from_bytes(&v)?;
                Ok((k.to_vec(), recs))
            })
        })
        .collect::<Result<_, MetaError>>()?;
    for (key, shadow_records) in rows {
        if shadow_records.iter().all(|rec| records.contains(rec)) {
            tx.remove(shadow, key);
        }
    }
    Ok(())
}
