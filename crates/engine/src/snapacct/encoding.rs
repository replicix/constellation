//! On-disk encodings of the accounting index.
//!
//! The chunk entry is the one record per indexed chunk, so it is encoded
//! by hand (LEB128 varints, the open sentinel stored as `0`) to keep it
//! near its information content: a one-run entry is 6–12 bytes of value.
//! Everything else is per snapshot or per chain and uses postcard.
//!
//! Keys are fixed-width big-endian so that lexicographic order is numeric
//! order and a chain's ordinals are one contiguous range:
//!
//! | keyspace | key | value |
//! |---|---|---|
//! | `snapacct_chunk` | `hash` (32) | [`ChunkEntry`] |
//! | `snapacct_birth` | `chain` (4) `first` (4) `hash` (32) | empty |
//! | `snapacct_death` | `chain` (4) `last` (4) `hash` (32) | empty, closed runs only |
//! | `snapacct_snap` | `chain` (4) `ord` (4) | [`SnapRec`] |
//! | `snapacct_meta` | `header`, `next_chain`, `fs`, `chain/` chain, `ino/` ino, `id/` id | postcard |
//! | `snapacct_tomb` | `h` hash / `t` since_ms (8) hash | `size`,`since_ms` varints / empty |
//! | `snapacct_lspill` | `s` spill / `m` member spill | empty (live spilled lists, by member) |
//!
//! `snapacct_meta` also holds `aux/<key>`: the owner's state (the
//! service's build cursor and live-refresh root), wiped with the index.

use super::{Run, SnapAcctError, OPEN};
use constellation_fs_core::ChunkHash;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Bumped whenever any encoding below changes: a mismatch on open wipes
/// the index and it is rebuilt from replicated state.
pub(super) const FORMAT: u32 = 2;

pub(super) const META_HEADER: &[u8] = b"header";
pub(super) const META_NEXT_CHAIN: &[u8] = b"next_chain";
pub(super) const META_FS: &[u8] = b"fs";

/// What one indexed chunk is: its plaintext size, whether the live tree
/// references it, and its runs (see the module docs for the invariant).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkEntry {
    pub size: u64,
    pub live: bool,
    pub runs: SmallVec<[Run; 1]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Header {
    pub format: u32,
    pub fs_uuid: String,
    pub accounted_seq: u64,
}

/// The chain registry row. `used`/`written` are Σ over the chain's
/// snapshots, kept in step with every snapshot record change.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ChainRec {
    pub dir_ino: u64,
    pub next_ord: u32,
    pub snapshots: u32,
    pub used: u64,
    pub written: u64,
}

/// One snapshot's numbers, keyed by `chain|ord`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SnapRec {
    pub id: String,
    pub root: String,
    pub used: u64,
    pub written: u64,
    pub refer: u64,
    pub lsize: u64,
}

/// The filesystem-level buckets of `snapshot space`, each `(bytes,
/// chunks)`. Every indexed chunk is in exactly one of `unique`,
/// `shared` (snapshots only, ≥2 snapshots) or `live`; tombstones are
/// not indexed chunks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct FsCounters {
    pub unique: (u64, u64),
    pub shared: (u64, u64),
    pub live: (u64, u64),
    pub tomb: (u64, u64),
}

pub(super) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(super) fn get_varint(input: &mut &[u8]) -> Result<u64, SnapAcctError> {
    let mut v: u64 = 0;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| SnapAcctError::Corrupt("truncated varint".into()))?;
        *input = rest;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(SnapAcctError::Corrupt("overlong varint".into()))
}

fn get_u32(input: &mut &[u8]) -> Result<u32, SnapAcctError> {
    u32::try_from(get_varint(input)?).map_err(|_| SnapAcctError::Corrupt("u32 out of range".into()))
}

/// `size, live, n, n × (chain, first, last + 1 | 0 = open, occ)`.
pub(super) fn encode_chunk(entry: &ChunkEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 * entry.runs.len());
    put_varint(&mut out, entry.size);
    out.push(u8::from(entry.live));
    put_varint(&mut out, entry.runs.len() as u64);
    for run in &entry.runs {
        put_varint(&mut out, u64::from(run.chain));
        put_varint(&mut out, u64::from(run.first));
        put_varint(&mut out, u64::from(run.last.wrapping_add(1)));
        put_varint(&mut out, run.occ);
    }
    out
}

pub(super) fn decode_chunk(mut input: &[u8]) -> Result<ChunkEntry, SnapAcctError> {
    let input = &mut input;
    let size = get_varint(input)?;
    let (&live, rest) = input
        .split_first()
        .ok_or_else(|| SnapAcctError::Corrupt("truncated chunk entry".into()))?;
    *input = rest;
    let n = get_varint(input)?;
    let mut runs = SmallVec::new();
    for _ in 0..n {
        let chain = get_u32(input)?;
        let first = get_u32(input)?;
        let last = get_u32(input)?.wrapping_sub(1);
        let occ = get_varint(input)?;
        runs.push(Run {
            chain,
            first,
            last,
            occ,
        });
    }
    if !input.is_empty() || live > 1 {
        return Err(SnapAcctError::Corrupt("malformed chunk entry".into()));
    }
    debug_assert!(runs
        .iter()
        .all(|r: &Run| r.last == OPEN || r.last >= r.first));
    Ok(ChunkEntry {
        size,
        live: live == 1,
        runs,
    })
}

/// A `snapacct_birth` / `snapacct_death` key.
pub(super) fn ord_key(chain: u32, ord: u32, hash: &ChunkHash) -> [u8; 40] {
    let mut key = [0u8; 40];
    key[..4].copy_from_slice(&chain.to_be_bytes());
    key[4..8].copy_from_slice(&ord.to_be_bytes());
    key[8..].copy_from_slice(&hash.0);
    key
}

pub(super) fn ord_key_hash(key: &[u8]) -> Result<ChunkHash, SnapAcctError> {
    let bytes: [u8; 32] = key
        .get(8..40)
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| SnapAcctError::Corrupt("short birth/death key".into()))?;
    Ok(ChunkHash(bytes))
}

pub(super) fn snap_key(chain: u32, ord: u32) -> [u8; 8] {
    let mut key = [0u8; 8];
    key[..4].copy_from_slice(&chain.to_be_bytes());
    key[4..].copy_from_slice(&ord.to_be_bytes());
    key
}

/// `(chain, ord)` of a `snapacct_snap` key, or the `chain|ord` prefix of
/// a birth/death key.
pub(super) fn split_chain_ord(key: &[u8]) -> Result<(u32, u32), SnapAcctError> {
    let chain = key
        .get(..4)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes);
    let ord = key
        .get(4..8)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes);
    match (chain, ord) {
        (Some(chain), Some(ord)) => Ok((chain, ord)),
        _ => Err(SnapAcctError::Corrupt("short chain|ord key".into())),
    }
}

pub(super) fn chain_key(chain: u32) -> Vec<u8> {
    let mut key = b"chain/".to_vec();
    key.extend_from_slice(&chain.to_be_bytes());
    key
}

pub(super) fn ino_key(ino: u64) -> Vec<u8> {
    let mut key = b"ino/".to_vec();
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub(super) fn id_key(id: &str) -> Vec<u8> {
    let mut key = b"id/".to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

pub(super) fn tomb_hash_key(hash: &ChunkHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    key.push(b'h');
    key.extend_from_slice(&hash.0);
    key
}

pub(super) fn tomb_time_key(since_ms: u64, hash: &ChunkHash) -> Vec<u8> {
    let mut key = Vec::with_capacity(41);
    key.push(b't');
    key.extend_from_slice(&since_ms.to_be_bytes());
    key.extend_from_slice(&hash.0);
    key
}

pub(super) fn encode_tomb(size: u64, since_ms: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    put_varint(&mut out, size);
    put_varint(&mut out, since_ms);
    out
}

pub(super) fn decode_tomb(mut input: &[u8]) -> Result<(u64, u64), SnapAcctError> {
    let input = &mut input;
    let size = get_varint(input)?;
    let since = get_varint(input)?;
    if !input.is_empty() {
        return Err(SnapAcctError::Corrupt("malformed tombstone".into()));
    }
    Ok((size, since))
}

pub(super) fn to_postcard<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("postcard encoding of an in-memory record")
}

pub(super) fn from_postcard<'a, T: Deserialize<'a>>(
    bytes: &'a [u8],
    what: &str,
) -> Result<T, SnapAcctError> {
    postcard::from_bytes(bytes).map_err(|e| SnapAcctError::Corrupt(format!("{what}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;

    #[test]
    fn chunk_entries_round_trip_and_stay_small() {
        let entry = ChunkEntry {
            size: 4 << 20,
            live: false,
            runs: smallvec![Run {
                chain: 3,
                first: 17,
                last: OPEN,
                occ: 2
            }],
        };
        let bytes = encode_chunk(&entry);
        assert!(bytes.len() <= 10, "{} bytes", bytes.len());
        assert_eq!(decode_chunk(&bytes).unwrap(), entry);
        let entry = ChunkEntry {
            size: 1,
            live: true,
            runs: smallvec![
                Run {
                    chain: 0,
                    first: 0,
                    last: 0,
                    occ: 0
                },
                Run {
                    chain: u32::MAX - 1,
                    first: 5,
                    last: u32::MAX - 1,
                    occ: 0
                },
            ],
        };
        assert_eq!(decode_chunk(&encode_chunk(&entry)).unwrap(), entry);
        assert!(decode_chunk(&[1, 2]).is_err());
        assert!(decode_chunk(&[1, 0, 0, 9]).is_err());
    }
}
