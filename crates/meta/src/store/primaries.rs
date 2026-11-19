//! The `primaries` keyspace: one row per inode that has a name, holding
//! the inode's primary name (ADR-42: the lowest key of
//! `keys::names_of(ino)`), so that "the first name" is a point get and a
//! seek at that name instead of a scan from the start of the range.
//!
//! The scan was the cost: an unlink is a point delete, which leaves a
//! tombstone at the name's key until compaction drops it, and a forward
//! scan from the start of `names_of(ino)` steps over every tombstone that
//! lies below the first live key (unlinking the names of a 8,192-name
//! inode in the stress tools' order made each of the later unlinks cross
//! the ones before it).
//!
//! # The row
//!
//! Key `ino BE`; value `flags u8 | tail`, where `tail = parent BE | name
//! bytes`, the part of the `0x04` key after `0x04 | ino` (a
//! [`PrimaryRow`]). Flag bit 0 is `ONE`: the inode has exactly this one
//! name. It is sound only when set (a clear bit says nothing: the row does
//! not shrink back to `ONE` when names go, so the conservative case is
//! "`ONE` clear, one name left").
//!
//! It is local derived state, like `xattr_by_name`: not published,
//! dirty-tracked, captured or journaled as log content.
//!
//! # Invariant I-P
//!
//! In every snapshot, for every `ino`, with `N(ino)` the set of its `0x04`
//! keys in `ns`:
//!
//! 1. the row exists iff `N(ino)` is not empty;
//! 2. its tail is the lowest key of `N(ino)` (the primary name);
//! 3. if `ONE` is set, `N(ino)` has exactly one key.
//!
//! # Why it holds
//!
//! The row is written only by [`note_insert`] and [`note_remove`], which
//! `ns::ns_insert` / `ns::ns_remove` call after the write of a `0x04` key
//! with a tracked [`ns::Dirty`], in the same transaction. Every write to
//! `ns` goes through that funnel, but three: bootstrap ingestion (the
//! [`crate::store::BootstrapIndexBuilder`] writes the rows in
//! `apply_bootstrap_indexes`, before the node serves), `ns_overwrite_after_ingest`
//! (the root's `0x01` only), and tests. By induction over the writes of a
//! transaction (every step sees the earlier steps of the same transaction):
//!
//! - insert `s`, row tail `p`: no row means `N` was empty, so `s` is the
//!   one name; `s < p` makes `s` the new lowest, and `N` has more than
//!   `s` (`p` is there, or it was removed already, and then the row would
//!   have moved), so `ONE` is cleared; `s == p` is a rewrite of the
//!   same key; `s > p` leaves the lowest alone and a second name ends
//!   `ONE`.
//! - remove `s`: `s != p` leaves the lowest alone (and `ONE`, which then
//!   says one name, `p`, so `s` was no key of `N`: a removal of a missing
//!   key). `s == p`: the new lowest is the first key after `s` (nothing
//!   lies between `s` and it, and nothing lies below `s`, by 2), or none,
//!   which removes the row. `ONE` set means no other key: no seek.
//!
//! A reader that opens its range at the row's key therefore loses no
//! name: no live key lies below it. And "complete" is true only if the
//! range ended, or `ONE` is set.

use crate::error::MetaError;
use crate::store::ns;
use constellation_fs_core::Ino;
use constellation_mtree::keys;
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};
use std::ops::Bound;

/// Flag bit 0: the inode has exactly the one name of the row.
const ONE: u8 = 1;

/// The length of `0x04 | ino | parent`, the shortest `0x04` key a name of
/// one byte can have is one longer.
const RDENTRY_MIN_KEY: usize = 1 + 2 * keys::INO_LEN + 1;

/// One inode's primary name as the `primaries` keyspace holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrimaryRow {
    /// The inode has exactly this name. Only meaningful when set.
    one: bool,
    /// `parent BE | name bytes`: the `0x04` key after `0x04 | ino`.
    tail: Vec<u8>,
}

impl PrimaryRow {
    /// A row for the `0x04` key tail `tail`.
    pub(crate) fn new(one: bool, tail: Vec<u8>) -> Self {
        Self { one, tail }
    }

    /// `flags | tail`.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.tail.len());
        out.push(if self.one { ONE } else { 0 });
        out.extend_from_slice(&self.tail);
        out
    }

    /// Parse a stored row; a flag bit other than `ONE`, or a tail that
    /// cannot be `parent | name` with a name, is refused.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, MetaError> {
        let Some((&flags, tail)) = bytes.split_first() else {
            return Err(MetaError::Invalid("an empty primary row".into()));
        };
        if flags & !ONE != 0 {
            return Err(MetaError::Invalid(format!(
                "a primary row with unknown flags {flags:#04x}"
            )));
        }
        if tail.len() <= keys::INO_LEN {
            return Err(MetaError::Invalid(
                "a primary row without a parent and a name".into(),
            ));
        }
        Ok(Self {
            one: flags & ONE != 0,
            tail: tail.to_vec(),
        })
    }

    /// The inode has more than this name (the bootstrap builder saw a
    /// second key).
    pub(crate) fn clear_one(&mut self) {
        self.one = false;
    }

    /// Whether the inode has exactly this name.
    pub(crate) fn is_one(&self) -> bool {
        self.one
    }

    /// The directory that holds the primary name.
    pub(crate) fn parent(&self) -> Ino {
        let mut b = [0u8; keys::INO_LEN];
        b.copy_from_slice(&self.tail[..keys::INO_LEN]);
        Ino::from_be_bytes(b)
    }

    /// The primary name's bytes.
    pub(crate) fn name(&self) -> &[u8] {
        &self.tail[keys::INO_LEN..]
    }

    /// The name as the store's `String`.
    pub(crate) fn name_lossy(&self) -> String {
        String::from_utf8_lossy(self.name()).into_owned()
    }

    /// The `0x04` key of this name for `ino`.
    pub(crate) fn key(&self, ino: Ino) -> Vec<u8> {
        rdentry_key(ino, &self.tail)
    }

    /// `parent BE | name bytes`.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn tail(&self) -> &[u8] {
        &self.tail
    }
}

/// `0x04 | ino | tail`.
fn rdentry_key(ino: Ino, tail: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + keys::INO_LEN + tail.len());
    key.push(keys::RANGE_RDENTRY);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(tail);
    key
}

/// Split a `0x04` key into its inode and its tail.
fn split_key(key: &[u8]) -> Result<(Ino, &[u8]), MetaError> {
    if key.len() < RDENTRY_MIN_KEY {
        return Err(MetaError::Invalid(format!(
            "a 0x04 key of {} bytes (the shortest is {RDENTRY_MIN_KEY})",
            key.len()
        )));
    }
    let mut b = [0u8; keys::INO_LEN];
    b.copy_from_slice(&key[1..1 + keys::INO_LEN]);
    Ok((Ino::from_be_bytes(b), &key[1 + keys::INO_LEN..]))
}

/// `ino`'s row.
pub(crate) fn primary_of(
    r: &impl Readable,
    primaries: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Option<PrimaryRow>, MetaError> {
    r.get(primaries, ino.to_be_bytes())?
        .map(|v| PrimaryRow::decode(&v))
        .transpose()
}

/// The funnel's hook after `tx.insert` of the `0x04` key `key` into `ns`:
/// keep the row of its inode the inode's lowest name (invariant I-P).
///
/// - no row: the row becomes `{ONE, s}`;
/// - `s < p`: `{not ONE, s}`;
/// - `s == p`: nothing (a rewrite of the same key);
/// - `s > p`: a row with `ONE` set becomes `{not ONE, p}`, else nothing.
pub(crate) fn note_insert(
    tx: &mut SingleWriterWriteTx,
    primaries: &SingleWriterTxKeyspace,
    key: &[u8],
) -> Result<(), MetaError> {
    let (ino, s) = split_key(key)?;
    let row = primary_of(tx, primaries, ino)?;
    let new = match row {
        None => PrimaryRow::new(true, s.to_vec()),
        Some(row) => match s.cmp(row.tail.as_slice()) {
            std::cmp::Ordering::Less => PrimaryRow::new(false, s.to_vec()),
            std::cmp::Ordering::Equal => return Ok(()),
            std::cmp::Ordering::Greater if row.one => PrimaryRow::new(false, row.tail),
            std::cmp::Ordering::Greater => return Ok(()),
        },
    };
    tx.insert(primaries, ino.to_be_bytes(), new.encode());
    Ok(())
}

/// The funnel's hook after `tx.remove` of the `0x04` key `key` from `ns`.
///
/// - no row, or `s != p`: nothing;
/// - `s == p`: the next key of the inode after `s` is the new primary
///   (`{not ONE, k}`), or there is none and the row goes. A row with `ONE`
///   set has no other key, so it goes without a seek.
///
/// # Errors
/// A read error of the seek, or a `0x04` key too short to be one.
pub(crate) fn note_remove(
    tx: &mut SingleWriterWriteTx,
    ns_ks: &SingleWriterTxKeyspace,
    primaries: &SingleWriterTxKeyspace,
    key: &[u8],
) -> Result<(), MetaError> {
    let (ino, s) = split_key(key)?;
    let Some(row) = primary_of(tx, primaries, ino)? else {
        return Ok(());
    };
    if s != row.tail.as_slice() {
        return Ok(());
    }
    if row.one {
        tx.remove(primaries, ino.to_be_bytes());
        return Ok(());
    }
    let range = keys::names_of(ino);
    let next = tx
        .range(
            ns_ks,
            (
                Bound::Excluded(key.to_vec()),
                Bound::Excluded(range.end().to_vec()),
            ),
        )
        .next()
        .map(|g| g.into_inner())
        .transpose()?
        .map(|(k, _)| k[1 + keys::INO_LEN..].to_vec());
    match next {
        Some(tail) => tx.insert(
            primaries,
            ino.to_be_bytes(),
            PrimaryRow::new(false, tail).encode(),
        ),
        None => tx.remove(primaries, ino.to_be_bytes()),
    }
    Ok(())
}

/// The scan the row replaces: the tails of the first `limit` `0x04` keys of
/// `ino`, from the start of its range.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn scan_tails(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    ino: Ino,
    limit: usize,
) -> Result<Vec<Vec<u8>>, MetaError> {
    let mut out = Vec::new();
    for guard in r.range(ns_ks, ns::key_range_bounds(&keys::names_of(ino))) {
        if out.len() >= limit {
            break;
        }
        let (k, _) = guard.into_inner()?;
        out.push(k[1 + keys::INO_LEN..].to_vec());
    }
    Ok(out)
}

/// Test-util cross-check, on `r`'s snapshot (so it cannot race a writer):
/// the row of `ino` is its first `0x04` key's tail, and absent when there is
/// none. `what` names the reader.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn assert_row_is_first_key(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    ino: Ino,
    row: Option<&PrimaryRow>,
    what: &str,
) {
    let first = scan_tails(r, ns_ks, ino, 1).expect("the scan of the cross-check");
    assert_eq!(
        row.map(PrimaryRow::tail),
        first.first().map(Vec::as_slice),
        "the primary row of {ino} disagrees with the 0x04 scan ({what})"
    );
}

/// [`Meta::check_primary_index`]'s body, over one snapshot.
#[cfg(any(test, feature = "test-util"))]
pub(crate) fn check_index(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    primaries: &SingleWriterTxKeyspace,
) -> Result<(), String> {
    use std::collections::BTreeMap;
    // ino -> (first tail, number of keys), from one pass over the 0x04 range.
    let mut seen: BTreeMap<Ino, (Vec<u8>, usize)> = BTreeMap::new();
    let range = keys::whole_range(keys::RANGE_RDENTRY);
    for guard in r.range(ns_ks, ns::key_range_bounds(&range)) {
        let (k, _) = guard.into_inner().map_err(|e| e.to_string())?;
        let (ino, tail) = split_key(&k).map_err(|e| e.to_string())?;
        seen.entry(ino)
            .and_modify(|(_, n)| *n += 1)
            .or_insert_with(|| (tail.to_vec(), 1));
    }
    for (ino, (first, n)) in &seen {
        let row = primary_of(r, primaries, *ino)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("inode {ino} has {n} 0x04 key(s) and no primary row"))?;
        if row.tail != *first {
            return Err(format!(
                "the primary row of {ino} is {:02x?}, its first 0x04 key is {first:02x?}",
                row.tail
            ));
        }
        if row.one && *n != 1 {
            return Err(format!(
                "the primary row of {ino} says ONE, it has {n} keys"
            ));
        }
    }
    for guard in r.iter(primaries) {
        let (k, v) = guard.into_inner().map_err(|e| e.to_string())?;
        let ino = <[u8; 8]>::try_from(&*k)
            .map(Ino::from_be_bytes)
            .map_err(|_| format!("a primary row key of {} bytes", k.len()))?;
        PrimaryRow::decode(&v).map_err(|e| e.to_string())?;
        if !seen.contains_key(&ino) {
            return Err(format!("a primary row of {ino}, which has no 0x04 key"));
        }
    }
    Ok(())
}

#[cfg(any(test, feature = "test-util"))]
impl crate::store::Meta {
    /// Check invariant I-P on one snapshot: every inode with `0x04` keys has
    /// a row whose tail is its first key, `ONE` only with exactly one key,
    /// and no row exists without keys.
    ///
    /// # Errors
    /// The first disagreement, as a message.
    pub fn check_primary_index(&self) -> Result<(), String> {
        let r = self.db.read_tx();
        check_index(&r, &self.ns, &self.primaries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_round_trips_and_refuses_unknown_flags() {
        let mut tail = 7u64.to_be_bytes().to_vec();
        tail.extend_from_slice("é".as_bytes());
        for one in [true, false] {
            let row = PrimaryRow::new(one, tail.clone());
            let back = PrimaryRow::decode(&row.encode()).unwrap();
            assert_eq!(back, row);
            assert_eq!(back.parent(), 7);
            assert_eq!(back.name(), "é".as_bytes());
            assert_eq!(back.key(9), keys::rdentry(9, 7, "é".as_bytes()));
        }
        let mut bad = PrimaryRow::new(true, tail.clone()).encode();
        bad[0] |= 2;
        assert!(PrimaryRow::decode(&bad).is_err());
        assert!(PrimaryRow::decode(&[]).is_err());
        assert!(PrimaryRow::decode(&[0, 1, 2]).is_err());
        assert!(
            PrimaryRow::decode(&PrimaryRow::new(true, 7u64.to_be_bytes().to_vec()).encode())
                .is_err()
        );
    }

    #[test]
    fn a_short_0x04_key_is_invalid_not_a_panic() {
        assert!(split_key(&[keys::RANGE_RDENTRY, 0, 0]).is_err());
        let no_name = keys::names_of(3).start().to_vec();
        assert!(split_key(&no_name).is_err());
        assert!(split_key(&keys::rdentry(3, 1, b"a")).is_ok());
    }
}
