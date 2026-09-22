//! The `atime` (live overlay) and `atime_journal` (not-yet-shipped
//! outbox) keyspaces, and the one atime max-merge-with-skew-clamp
//! algorithm every apply path (replay, `MutateOp::AtimeBatch`, the
//! local flush) shares.
//!
//! §P6 excludes atime from the tree entirely (`constellation_mtree::record::Attrs`
//! has no atime field), so unlike the old SQLite engine's `inode.atime_ns`
//! column this is a genuinely separate, node-local keyspace that every
//! `FileAttr`-returning read overlays at the end.

use crate::error::MetaError;
use crate::store::ns;
use constellation_fs_core::types::now_ns;
use constellation_fs_core::Ino;
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

const ATIME_SKEW_TOLERANCE_S_DEFAULT: i64 = 300;

pub fn atime_skew_tolerance_ns() -> i64 {
    std::env::var("CONSTELLATION_ATIME_SKEW_TOLERANCE_S")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(ATIME_SKEW_TOLERANCE_S_DEFAULT)
        .saturating_mul(1_000_000_000)
}

pub(crate) fn get_atime(
    r: &impl Readable,
    atime: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<i64, MetaError> {
    match r.get(atime, ino.to_be_bytes())? {
        Some(v) if v.len() == 8 => Ok(i64::from_le_bytes(v.as_ref().try_into().unwrap())),
        _ => Ok(0),
    }
}

pub(crate) fn set_atime_tx(
    tx: &mut SingleWriterWriteTx,
    atime: &SingleWriterTxKeyspace,
    ino: Ino,
    atime_ns: i64,
) {
    tx.insert(
        atime,
        ino.to_be_bytes().to_vec(),
        atime_ns.to_le_bytes().to_vec(),
    );
}

pub(crate) fn remove_atime_tx(
    tx: &mut SingleWriterWriteTx,
    atime: &SingleWriterTxKeyspace,
    ino: Ino,
) {
    tx.remove(atime, ino.to_be_bytes().to_vec());
}

/// The ctime guard needs the inode's current ctime, wherever it lives
/// (`ns` for a live inode, `orphans` for one that is unlinked but still
/// open — a reader can still hold an fd to it).
fn ctime_of(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    orphans: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Option<i64>, MetaError> {
    if let Some(rec) = ns::get_inode_record(r, ns_ks, ino)? {
        return Ok(Some(rec.attrs.ctime_ns));
    }
    match r.get(orphans, ino.to_be_bytes())? {
        Some(v) => Ok(Some(
            constellation_mtree::record::InodeRecord::decode(&v)?
                .attrs
                .ctime_ns,
        )),
        None => Ok(None),
    }
}

/// Clamp `atime_ns` to `now + skew_tol_ns`, then max-merge it into the
/// `atime` overlay, guarded on `ctime_ns < time_ns` (an attribute change
/// that postdates the observed read fences off a stale bump). Never
/// writes ctime. Returns whether the bump was applied (row existed and
/// the guard passed) and whether it was skew-clamped.
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_atime_one(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    orphans: &SingleWriterTxKeyspace,
    atime: &SingleWriterTxKeyspace,
    ino: Ino,
    atime_ns: i64,
    time_ns: i64,
    skew_tol_ns: i64,
) -> Result<(bool, bool), MetaError> {
    // `clamped` reflects the claim vs. the skew ceiling alone — computed
    // (and reported) whether or not the guard below ends up applying the
    // bump, matching the old engine's single guarded `UPDATE`.
    let ceiling = now_ns().saturating_add(skew_tol_ns);
    let (claim, clamped) = if atime_ns > ceiling {
        (ceiling, true)
    } else {
        (atime_ns, false)
    };
    let Some(ctime_ns) = ctime_of(tx, ns_ks, orphans, ino)? else {
        return Ok((false, clamped));
    };
    if ctime_ns >= time_ns {
        return Ok((false, clamped));
    }
    let current = get_atime(tx, atime, ino)?;
    set_atime_tx(tx, atime, ino, current.max(claim));
    Ok((true, clamped))
}

// ---------------------------------------------------------- atime_journal

fn aj_key(ino: Ino) -> Vec<u8> {
    ino.to_be_bytes().to_vec()
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AjRow {
    atime_ns: i64,
    time_ns: i64,
}

pub(crate) fn queue_atime_tx(
    tx: &mut SingleWriterWriteTx,
    aj: &SingleWriterTxKeyspace,
    ino: Ino,
    atime_ns: i64,
    time_ns: i64,
) -> Result<(), MetaError> {
    let key = aj_key(ino);
    let merged = match tx.get(aj, key.clone())? {
        Some(v) => {
            let prev: AjRow = postcard::from_bytes(&v)?;
            AjRow {
                atime_ns: prev.atime_ns.max(atime_ns),
                time_ns: prev.time_ns.max(time_ns),
            }
        }
        None => AjRow { atime_ns, time_ns },
    };
    tx.insert(aj, key, postcard::to_allocvec(&merged)?);
    Ok(())
}

pub(crate) fn atime_backlog(
    r: &impl Readable,
    aj: &SingleWriterTxKeyspace,
) -> Result<u64, MetaError> {
    let mut n = 0u64;
    for guard in r.iter(aj) {
        guard.into_inner()?;
        n += 1;
    }
    Ok(n)
}

/// The oldest `time_ns` across every pending row, or `None` if empty.
/// Full scan, same cost class as [`atime_backlog`] (called at the same
/// cadence — once per sync round that shipped nothing else).
pub(crate) fn oldest_pending_time_ns(
    r: &impl Readable,
    aj: &SingleWriterTxKeyspace,
) -> Result<Option<i64>, MetaError> {
    let mut oldest: Option<i64> = None;
    for guard in r.iter(aj) {
        let (_, v) = guard.into_inner()?;
        let row: AjRow = postcard::from_bytes(&v)?;
        oldest = Some(oldest.map_or(row.time_ns, |o: i64| o.min(row.time_ns)));
    }
    Ok(oldest)
}

pub(crate) fn take_atime(
    r: &impl Readable,
    aj: &SingleWriterTxKeyspace,
    max: usize,
) -> Result<Vec<(Ino, i64, i64)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.iter(aj) {
        if out.len() >= max {
            break;
        }
        let (k, v) = guard.into_inner()?;
        if k.len() != 8 {
            continue;
        }
        let ino = u64::from_be_bytes(k.as_ref().try_into().unwrap());
        let row: AjRow = postcard::from_bytes(&v)?;
        out.push((ino, row.atime_ns, row.time_ns));
    }
    Ok(out)
}

pub(crate) fn clear_atime_tx(
    tx: &mut SingleWriterWriteTx,
    aj: &SingleWriterTxKeyspace,
    inos: &[Ino],
) {
    for ino in inos {
        tx.remove(aj, aj_key(*ino));
    }
}

pub(crate) fn drop_atime_all(
    tx: &mut SingleWriterWriteTx,
    aj: &SingleWriterTxKeyspace,
) -> Result<(), MetaError> {
    let keys: Vec<Vec<u8>> = tx
        .iter(aj)
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys {
        tx.remove(aj, k);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::store::Meta;
    use crate::MetaStore;
    use constellation_fs_core::types::ROOT_INO;

    /// Feeds `Shipper::ship_atime_if_stale` (plan 29 M3b): the oldest
    /// pending row's observation time, not insertion order, is what the
    /// standalone ship-max-delay ceiling checks against.
    #[test]
    fn oldest_pending_is_the_minimum_observation_time_not_insertion_order() {
        let m = Meta::open_in_memory().unwrap();
        let part = "p0";
        assert_eq!(m.atime_oldest_pending_ns(part).unwrap(), None);

        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = m.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        // Queued out of time order: b (older observation) after a.
        m.queue_atime(&[(a.ino, 5_000, 5_000)]).unwrap();
        m.queue_atime(&[(b.ino, 1_000, 1_000)]).unwrap();
        assert_eq!(m.atime_oldest_pending_ns(part).unwrap(), Some(1_000));

        // Clearing the actual oldest row moves the minimum forward.
        m.clear_atime(part, &[b.ino]).unwrap();
        assert_eq!(m.atime_oldest_pending_ns(part).unwrap(), Some(5_000));

        m.clear_atime(part, &[a.ino]).unwrap();
        assert_eq!(m.atime_oldest_pending_ns(part).unwrap(), None);
    }
}
