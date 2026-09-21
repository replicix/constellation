//! The on-bucket node format, and the boundary function that decides
//! where one node ends and the next begins (plan 28 §P1, §P7).
//!
//! A node is an immutable, content-addressed run of entries at one
//! level of the tree. Level 0 holds `(key, value)`; every level above
//! holds `(first_key, child_hash, subtree aggregates)`. The layout is
//! explicit little-endian with an offset table rather than a serde
//! derive, for the same reason `fs-core::Tree` is: this is persistent
//! storage, so versioning and corruption checks should be visible in
//! the layout instead of inherited from a serializer. It also means a
//! point lookup is a binary search over the encoded bytes with no
//! allocation and no full decode, which is the only reason a tree can
//! compete with SQLite's page cache on the read path (§14.2).
//!
//! ## Why keys and values are opaque here
//!
//! Nothing in this module knows what a key *means*. The §P6 key codec
//! is a separate layer (S3) precisely so that the structure and the
//! encoding can be reasoned about — and versioned — independently. The
//! one place the tree needs to know something about a value is the
//! §P7 aggregate, and that arrives as a caller-supplied projection
//! ([`crate::Config::leaf_agg`]) rather than as knowledge baked in here.
//!
//! ## The boundary function
//!
//! A node at level `L` ends after key `k` iff
//! `window(hash(k), L) < u32::MAX / TARGET`. Two properties the rest of
//! the crate leans on follow from what that predicate does *not* read:
//!
//! - it does not read the value, so a `chmod` rewrites the leaves along
//!   one root path but never reshapes the tree, and
//! - it does not read any history, so the node partition of a level is
//!   a pure function of that level's key sequence. A tree built by
//!   incremental [`crate::Tree::apply`] is therefore byte-identical to
//!   one bulk-built from the same key set, and insertion order cannot
//!   matter.
//!
//! §P1 writes the predicate as `blake3(k)[0..4]`, which is exactly what
//! this module computes at level 0. Above level 0 it takes a *different*
//! 4-byte window of the same digest, because the levels must not agree
//! on boundaries: if every boundary key of level 0 were also one at
//! level 1, every interior node would hold a single entry and the tree
//! would degenerate into a linked list. `boundary_rate_tracks_target`
//! pins both halves of that.
//!
//! ## The entry clamps
//!
//! §P1 asks for hard min/max entry clamps and §14.1 settled the worry
//! that made an earlier benchmark omit them. The worry was that a clamp
//! reintroduces history into the shape; it does not, because a node's
//! *start* is itself context-free, so "seal after MAX entries counted
//! from the start" (and symmetrically "suppress boundaries inside the
//! first MIN entries from the start") is still a pure function of the
//! key sequence. §14.1 measured the tail a keys-only boundary function
//! produces — leaf entries per node p50 81, p99 536, max 1545, so a
//! ~120 KiB worst-case leaf that a one-key lookup has to decompress in
//! full — and found `MAX_ENTRIES = 256` clips it at no measurable read
//! cost. It is free insurance and it is in the format from day one.
//!
//! The clamp is not entirely free, and the cost lands in
//! [`crate::Tree::apply`]: a node sealed by the clamp rather than by a
//! boundary key has not re-synchronized with its untouched right
//! neighbour, so the rewrite run must absorb it. That is why the clamp
//! gets its own canonicality test *under deletes*, which is the case
//! that forces the absorption.

use crate::error::MtreeError;
use crate::hash::NodeHash;

/// Entries per node, chosen so an encoded node lands near 8 KiB at the
/// census value sizes (plan 28 Appendix A, confirmed by §14.1: mean
/// 9.35 KiB at level 0, 7.49 KiB at level 1).
pub const LEAF_TARGET: u32 = 117;
pub const INTERIOR_TARGET: u32 = 115;

/// Lower clamp: boundaries inside the first `MIN_ENTRIES` of a node are
/// suppressed. The default is 1 — i.e. no suppression — because that is
/// the configuration §14 measured, and every number this crate is sized
/// against comes from there. The mechanism is implemented, canonical,
/// and tested at larger values so that raising it is a configuration
/// change rather than a format change.
pub const MIN_ENTRIES: usize = 1;

/// Upper clamp, from §14.1. At 256 the census tree became 344,308
/// leaves with p99 = max = 256 and a 22.24 MiB interior, against
/// 306,096 leaves and 19.75 MiB unclamped.
pub const MAX_ENTRIES: usize = 256;

/// `b"MTRE"`. Distinct from `fs-core`'s `CTR1`/`CTR2` snapshot-tree
/// blobs, which are a different format in a different object namespace.
pub const MAGIC: [u8; 4] = *b"MTRE";

/// Format version, written into every node and checked on every read.
///
/// This is an **on-bucket** format and ADR-5 promises not to migrate
/// it, so treat a change here as a breaking change: it changes every
/// node hash, and therefore every commit, snapshot, and clone that
/// names one. `format_is_pinned` in `tests/properties.rs` fails loudly
/// if the encoding moves by a byte.
pub const FORMAT_VERSION: u8 = 1;

/// magic(4) + version(1) + level(1) + entry count(4).
pub(crate) const HEADER_LEN: usize = 10;

/// Subtree summary carried by every interior entry (§P7).
///
/// This is a **monoid** over the key order under [`Agg::combine`] with
/// [`Agg::EMPTY`] as the identity: associative, so a parent can be
/// computed from its children alone without re-reading their subtrees,
/// which is what makes `du -sh`, `statfs`, quota admission and pin
/// admission O(depth) reads of already-resident interior nodes instead
/// of maintained counter columns. Because the aggregate is covered by
/// the node hash, a wrong total is detectable corruption rather than a
/// silently drifted counter.
///
/// It is deliberately a fixed triple plus the key count, not a generic
/// parameter: every aggregate here is either a sum or a max, and a
/// caller-defined monoid would have to be part of the on-bucket format
/// contract (two nodes agreeing on bytes but disagreeing on what the
/// third field means is not a format anyone can verify). What *is*
/// caller-defined is the projection from a leaf entry to an `Agg` —
/// see [`crate::Config::leaf_agg`] — because that needs the §P6 key
/// encoding, which this layer must not know.
///
/// Note what is **not** here: per-*directory* recursive size. A
/// directory's descendants are not contiguous in the §P6 key order, so
/// "bytes below this directory" is not a function of a contiguous key
/// range and cannot be a subtree aggregate at all. §P7 answers it with
/// a bounded range scan instead; pretending otherwise would produce an
/// aggregate that is exact per key range and wrong per directory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Agg {
    /// Sum of the byte sizes the projection attributed to each key.
    pub bytes: u64,
    /// Sum of the file counts the projection attributed to each key.
    pub files: u64,
    /// Keys in the subtree. Always the true count: the tree fills this
    /// in itself rather than trusting the projection, so it is usable
    /// as a structural check.
    pub keys: u64,
    /// Max, not sum. Nanoseconds since the epoch, signed like every
    /// other mtime in this repo.
    pub max_mtime: i64,
}

impl Agg {
    /// The monoid identity.
    ///
    /// `max_mtime: 0` rather than `i64::MIN`, which is the mathematically
    /// exact identity for a max over all of `i64`. Two reasons: an
    /// mtime of 0 already means "nothing to report" everywhere else in
    /// this repo, and `i64::MIN` varint-encodes to ten bytes in every
    /// interior entry of an otherwise-empty subtree. The cost is that a
    /// genuinely pre-1970 mtime is masked by a sibling with no mtime at
    /// all, which no POSIX filesystem this stores will produce.
    pub const EMPTY: Agg = Agg {
        bytes: 0,
        files: 0,
        keys: 0,
        max_mtime: 0,
    };

    /// Associative and commutative; `EMPTY` is the identity.
    ///
    /// Saturating rather than wrapping: a corrupt node that claims
    /// `u64::MAX` bytes should make `du` implausible, not panic a debug
    /// build or silently wrap in release.
    pub fn combine(self, other: Agg) -> Agg {
        Agg {
            bytes: self.bytes.saturating_add(other.bytes),
            files: self.files.saturating_add(other.files),
            keys: self.keys.saturating_add(other.keys),
            max_mtime: self.max_mtime.max(other.max_mtime),
        }
    }

    pub fn merge(&mut self, other: &Agg) {
        *self = self.combine(*other);
    }
}

/// What an entry carries besides its key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// Level 0: the caller's opaque value bytes.
    Leaf(Vec<u8>),
    /// Above level 0: the child node and its subtree aggregate.
    Child { hash: NodeHash, agg: Agg },
}

/// One decoded entry. `key` is the entry's key at level 0 and the
/// child's *first* key above it, which is what makes an interior key
/// both a separator and a stable identity — first keys propagate
/// upward unchanged, so a parent's next entry key is the exclusive
/// upper bound of the current child's range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Value,
}

impl Entry {
    pub fn leaf(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Entry {
        Entry {
            key: key.into(),
            value: Value::Leaf(value.into()),
        }
    }

    pub fn child(key: impl Into<Vec<u8>>, hash: NodeHash, agg: Agg) -> Entry {
        Entry {
            key: key.into(),
            value: Value::Child { hash, agg },
        }
    }

    /// The child hash, for entries known to be interior.
    pub fn child_hash(&self) -> Option<NodeHash> {
        match &self.value {
            Value::Child { hash, .. } => Some(*hash),
            Value::Leaf(_) => None,
        }
    }
}

/// True when a node at `level` ends after `key`.
pub fn is_boundary(hasher: &crate::hash::Hasher, key: &[u8], level: u8) -> bool {
    let window = (level as usize) % 8;
    hasher.key_word(key, window) < u32::MAX / target_for(level)
}

pub fn target_for(level: u8) -> u32 {
    if level == 0 {
        LEAF_TARGET
    } else {
        INTERIOR_TARGET
    }
}

// --------------------------------------------------------------- encode

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(buf: &[u8], at: &mut usize) -> Result<u64, MtreeError> {
    let mut v = 0u64;
    let mut shift = 0u32;
    loop {
        let b = *buf
            .get(*at)
            .ok_or(MtreeError::Malformed("varint runs past the entry"))?;
        *at += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift >= 64 {
            return Err(MtreeError::Malformed("varint is wider than 64 bits"));
        }
    }
}

/// Encode a run of entries as one node.
///
/// Key and value lengths are `u16`, so this rejects nothing and
/// truncates nothing: callers above this layer bound values at
/// `VALUE_SPILL` (1 KiB) and spill the rest to a blob, and a key longer
/// than 64 KiB cannot be produced by the §P6 codec. The `debug_assert`
/// is there to catch a future codec that forgets.
pub fn encode(level: u8, entries: &[Entry]) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::with_capacity(entries.len() * 80);
    let mut offsets: Vec<u32> = Vec::with_capacity(entries.len());
    for entry in entries {
        debug_assert!(entry.key.len() <= u16::MAX as usize, "key exceeds u16");
        offsets.push(body.len() as u32);
        body.extend_from_slice(&(entry.key.len() as u16).to_le_bytes());
        match &entry.value {
            Value::Leaf(value) => {
                debug_assert!(value.len() <= u16::MAX as usize, "value exceeds u16");
                body.extend_from_slice(&(value.len() as u16).to_le_bytes());
                body.extend_from_slice(&entry.key);
                body.extend_from_slice(value);
            }
            Value::Child { hash, agg } => {
                body.extend_from_slice(&entry.key);
                body.extend_from_slice(&hash.0);
                put_varint(&mut body, agg.bytes);
                put_varint(&mut body, agg.files);
                put_varint(&mut body, agg.keys);
                put_varint(&mut body, agg.max_mtime as u64);
            }
        }
    }
    let mut out = Vec::with_capacity(HEADER_LEN + entries.len() * 4 + body.len());
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.push(level);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for off in &offsets {
        out.extend_from_slice(&off.to_le_bytes());
    }
    out.extend_from_slice(&body);
    out
}

// --------------------------------------------------------------- decode

/// A borrowed view over encoded node bytes.
///
/// [`NodeRef::new`] is O(1): it checks the magic, the version, and that
/// the offset table is present, and nothing else. Every accessor is
/// then individually bounds-checked and returns
/// [`MtreeError::Malformed`] rather than panicking, so a truncated or
/// scrambled pack produces a failed read instead of a dead process, and
/// the read path keeps paying only for the bounds checks Rust slicing
/// does anyway.
///
/// [`NodeRef::validate`] is the O(entries) pass that a trust boundary
/// wants: it walks every entry extent and asserts strictly ascending
/// keys. That last part cannot be checked lazily — a node whose keys
/// are out of order decodes fine and merely makes binary search return
/// wrong answers — so it is separate, and callers that care (fsck, the
/// pack reader's first sight of a blob) call [`NodeRef::parse`].
pub struct NodeRef<'a> {
    level: u8,
    count: usize,
    buf: &'a [u8],
    /// Offset of the first entry, i.e. the end of the offset table.
    body: usize,
}

impl<'a> NodeRef<'a> {
    /// O(1) header check. Cheap enough for the hot path.
    pub fn new(buf: &'a [u8]) -> Result<NodeRef<'a>, MtreeError> {
        if buf.len() < HEADER_LEN {
            return Err(MtreeError::Malformed("shorter than a node header"));
        }
        if buf[0..4] != MAGIC {
            return Err(MtreeError::Malformed("bad magic"));
        }
        if buf[4] != FORMAT_VERSION {
            return Err(MtreeError::UnsupportedVersion {
                found: buf[4],
                expected: FORMAT_VERSION,
            });
        }
        let count = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]) as usize;
        let body = HEADER_LEN
            .checked_add(count.checked_mul(4).ok_or(MtreeError::Malformed(
                "entry count overflows the offset table",
            ))?)
            .ok_or(MtreeError::Malformed(
                "entry count overflows the offset table",
            ))?;
        if buf.len() < body {
            return Err(MtreeError::Malformed("offset table is truncated"));
        }
        Ok(NodeRef {
            level: buf[5],
            count,
            buf,
            body,
        })
    }

    /// O(1) header check plus the full structural walk.
    pub fn parse(buf: &'a [u8]) -> Result<NodeRef<'a>, MtreeError> {
        let node = NodeRef::new(buf)?;
        node.validate()?;
        Ok(node)
    }

    /// Every entry decodes inside the buffer and the keys strictly
    /// ascend. Does not verify the node's hash — that is the store's
    /// job, since only the store knows which hash it asked for.
    pub fn validate(&self) -> Result<(), MtreeError> {
        let mut previous: Option<&[u8]> = None;
        for i in 0..self.count {
            let key = self.key(i)?;
            if previous.is_some_and(|prev| prev >= key) {
                return Err(MtreeError::Malformed("entries are not in key order"));
            }
            previous = Some(key);
            if self.level == 0 {
                self.leaf_value(i)?;
            } else {
                self.child(i)?;
            }
        }
        Ok(())
    }

    pub fn level(&self) -> u8 {
        self.level
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn is_leaf(&self) -> bool {
        self.level == 0
    }

    fn entry_at(&self, i: usize) -> Result<usize, MtreeError> {
        if i >= self.count {
            return Err(MtreeError::Malformed("entry index out of range"));
        }
        let o = HEADER_LEN + i * 4;
        let off = u32::from_le_bytes([
            self.buf[o],
            self.buf[o + 1],
            self.buf[o + 2],
            self.buf[o + 3],
        ]) as usize;
        let at = self
            .body
            .checked_add(off)
            .ok_or(MtreeError::Malformed("entry offset overflows"))?;
        if at > self.buf.len() {
            return Err(MtreeError::Malformed("entry offset past the buffer"));
        }
        Ok(at)
    }

    fn u16_at(&self, at: usize) -> Result<usize, MtreeError> {
        let bytes = self
            .buf
            .get(at..at + 2)
            .ok_or(MtreeError::Malformed("entry header is truncated"))?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]) as usize)
    }

    pub fn key(&self, i: usize) -> Result<&'a [u8], MtreeError> {
        let at = self.entry_at(i)?;
        let klen = self.u16_at(at)?;
        let start = if self.level == 0 { at + 4 } else { at + 2 };
        self.buf
            .get(start..start + klen)
            .ok_or(MtreeError::Malformed("key extends past the buffer"))
    }

    pub fn leaf_value(&self, i: usize) -> Result<&'a [u8], MtreeError> {
        if self.level != 0 {
            return Err(MtreeError::Malformed("leaf value read on an interior node"));
        }
        let at = self.entry_at(i)?;
        let klen = self.u16_at(at)?;
        let vlen = self.u16_at(at + 2)?;
        let start = at + 4 + klen;
        self.buf
            .get(start..start + vlen)
            .ok_or(MtreeError::Malformed("value extends past the buffer"))
    }

    pub fn child(&self, i: usize) -> Result<(NodeHash, Agg), MtreeError> {
        if self.level == 0 {
            return Err(MtreeError::Malformed("child read on a leaf node"));
        }
        let at = self.entry_at(i)?;
        let klen = self.u16_at(at)?;
        let mut p = at + 2 + klen;
        let hash: [u8; 32] = self
            .buf
            .get(p..p + 32)
            .and_then(|bytes| bytes.try_into().ok())
            .ok_or(MtreeError::Malformed("child hash extends past the buffer"))?;
        p += 32;
        let bytes = get_varint(self.buf, &mut p)?;
        let files = get_varint(self.buf, &mut p)?;
        let keys = get_varint(self.buf, &mut p)?;
        let max_mtime = get_varint(self.buf, &mut p)? as i64;
        Ok((
            NodeHash(hash),
            Agg {
                bytes,
                files,
                keys,
                max_mtime,
            },
        ))
    }

    /// `Ok(i)` on an exact key hit, `Err(i)` with the insertion point.
    ///
    /// The `Result` of the *lookup* is the outer one; a malformed entry
    /// is reported separately so a caller cannot confuse "absent" with
    /// "unreadable".
    pub fn search(&self, key: &[u8]) -> Result<Result<usize, usize>, MtreeError> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid)?.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(Ok(mid)),
            }
        }
        Ok(Err(lo))
    }

    /// Index of the child whose key range contains `key`. Keys below
    /// the node's first key resolve to child 0, which is what lets a
    /// cursor seek to a key the tree does not hold.
    pub fn descend(&self, key: &[u8]) -> Result<usize, MtreeError> {
        Ok(match self.search(key)? {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        })
    }

    pub fn entries(&self) -> Result<Vec<Entry>, MtreeError> {
        (0..self.count)
            .map(|i| {
                let key = self.key(i)?.to_vec();
                if self.level == 0 {
                    Ok(Entry {
                        key,
                        value: Value::Leaf(self.leaf_value(i)?.to_vec()),
                    })
                } else {
                    let (hash, agg) = self.child(i)?;
                    Ok(Entry {
                        key,
                        value: Value::Child { hash, agg },
                    })
                }
            })
            .collect()
    }

    /// This node's own aggregate. Interior nodes sum their children's;
    /// leaves apply `leaf_agg` to every entry and count the keys.
    pub fn aggregate(&self, leaf_agg: crate::LeafAgg) -> Result<Agg, MtreeError> {
        let mut agg = Agg::EMPTY;
        if self.level == 0 {
            for i in 0..self.count {
                agg.merge(&leaf_agg(self.key(i)?, self.leaf_value(i)?));
            }
            agg.keys = self.count as u64;
        } else {
            for i in 0..self.count {
                agg.merge(&self.child(i)?.1);
            }
        }
        Ok(agg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::Hasher;

    fn leaf_entries(n: u32) -> Vec<Entry> {
        (0..n)
            .map(|i| Entry::leaf(i.to_be_bytes().to_vec(), vec![i as u8; 7]))
            .collect()
    }

    #[test]
    fn leaf_round_trip_and_search() {
        let entries = leaf_entries(50);
        let buf = encode(0, &entries);
        let node = NodeRef::parse(&buf).unwrap();
        assert_eq!(node.level(), 0);
        assert_eq!(node.count(), 50);
        assert_eq!(node.entries().unwrap(), entries);
        assert_eq!(node.search(&7u32.to_be_bytes()).unwrap(), Ok(7));
        assert_eq!(node.search(&99u32.to_be_bytes()).unwrap(), Err(50));
    }

    #[test]
    fn interior_round_trip_keeps_aggregates() {
        let agg = Agg {
            bytes: 1 << 40,
            files: 12_345,
            keys: 999,
            max_mtime: 1_700_000_000_000_000_000,
        };
        let entries = vec![Entry::child(b"k".to_vec(), NodeHash([9u8; 32]), agg)];
        let buf = encode(3, &entries);
        let node = NodeRef::parse(&buf).unwrap();
        assert_eq!(node.level(), 3);
        assert_eq!(node.child(0).unwrap(), (NodeHash([9u8; 32]), agg));
    }

    /// mtimes before 1970 are legal and the varint encoding is
    /// unsigned, so the cast has to survive a full round trip.
    #[test]
    fn negative_max_mtime_round_trips() {
        let agg = Agg {
            max_mtime: -1_234_567_890,
            ..Agg::default()
        };
        let buf = encode(1, &[Entry::child(b"k".to_vec(), NodeHash::ZERO, agg)]);
        assert_eq!(NodeRef::parse(&buf).unwrap().child(0).unwrap().1, agg);
    }

    #[test]
    fn header_is_magic_version_level_count() {
        let buf = encode(2, &leaf_entries(3));
        assert_eq!(&buf[0..4], b"MTRE");
        assert_eq!(buf[4], FORMAT_VERSION);
        assert_eq!(buf[5], 2);
        assert_eq!(u32::from_le_bytes(buf[6..10].try_into().unwrap()), 3);
    }

    #[test]
    fn a_future_version_is_refused_not_guessed() {
        let mut buf = encode(0, &leaf_entries(3));
        buf[4] = FORMAT_VERSION + 1;
        assert!(matches!(
            NodeRef::new(&buf),
            Err(MtreeError::UnsupportedVersion { found, expected })
                if found == FORMAT_VERSION + 1 && expected == FORMAT_VERSION
        ));
    }

    #[test]
    fn out_of_order_entries_are_rejected() {
        let entries = vec![Entry::leaf(b"b".to_vec(), b"1"), Entry::leaf(b"a", b"2")];
        let buf = encode(0, &entries);
        // Structure is fine, ordering is not, and only `validate` can
        // tell: a binary search over this node would simply lie.
        assert!(NodeRef::new(&buf).is_ok());
        assert!(NodeRef::parse(&buf).is_err());
    }

    #[test]
    fn empty_node_decodes() {
        let buf = encode(0, &[]);
        let node = NodeRef::parse(&buf).unwrap();
        assert_eq!(node.count(), 0);
        assert_eq!(node.search(b"anything").unwrap(), Err(0));
        assert!(node.entries().unwrap().is_empty());
    }

    #[test]
    fn boundary_rate_tracks_target() {
        let hasher = Hasher::Plain;
        let hits = (0u32..200_000)
            .filter(|i| is_boundary(&hasher, &i.to_be_bytes(), 0))
            .count();
        let expected = 200_000f64 / LEAF_TARGET as f64;
        assert!(
            (hits as f64) > expected * 0.8 && (hits as f64) < expected * 1.2,
            "{hits} boundaries, expected about {expected}"
        );
        // Levels must not agree on boundaries or the tree degenerates
        // into a linked list of one-entry interior nodes.
        let both = (0u32..200_000)
            .filter(|i| {
                is_boundary(&hasher, &i.to_be_bytes(), 0)
                    && is_boundary(&hasher, &i.to_be_bytes(), 1)
            })
            .count();
        assert!(both < hits / 10, "{both} shared boundaries out of {hits}");
    }

    /// §P1's predicate, verbatim, at level 0.
    #[test]
    fn level_zero_is_exactly_the_documented_predicate() {
        for i in 0u32..5_000 {
            let key = i.to_be_bytes();
            let digest = blake3::hash(&key);
            let word = u32::from_le_bytes(digest.as_bytes()[0..4].try_into().unwrap());
            assert_eq!(
                is_boundary(&Hasher::Plain, &key, 0),
                word < u32::MAX / LEAF_TARGET
            );
        }
    }

    #[test]
    fn aggregates_are_a_monoid() {
        let a = Agg {
            bytes: 3,
            files: 1,
            keys: 1,
            max_mtime: 10,
        };
        let b = Agg {
            bytes: 4,
            files: 2,
            keys: 1,
            max_mtime: 7,
        };
        let c = Agg {
            bytes: 5,
            files: 0,
            keys: 1,
            max_mtime: 99,
        };
        assert_eq!(a.combine(Agg::EMPTY), a);
        assert_eq!(Agg::EMPTY.combine(a), a);
        assert_eq!(a.combine(b), b.combine(a));
        assert_eq!(a.combine(b).combine(c), a.combine(b.combine(c)));
        assert_eq!(a.combine(b).max_mtime, 10);
        // Saturating, so a corrupt claim cannot panic a debug build.
        let huge = Agg {
            bytes: u64::MAX,
            ..Agg::default()
        };
        assert_eq!(huge.combine(huge).bytes, u64::MAX);
    }
}
