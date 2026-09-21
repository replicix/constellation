//! Node format and the boundary function (plan 28 §P1, §P7).
//!
//! A node is an immutable, content-addressed run of entries at one level
//! of the tree. Level 0 holds `(key, value)`; every level above holds
//! `(first_key, child_hash, subtree aggregates)`. The layout is explicit
//! little-endian with an offset table rather than a serde derive: point
//! lookup must be a binary search over the encoded bytes with no
//! allocation and no full decode, which is the only reason a tree can
//! compete with SQLite's page cache on the read path.
//!
//! The boundary function reads *only the key*, so chunking is
//! context-free: a node ends after key `k` at level `L` iff
//! `window(blake3(k), L) < u32::MAX / TARGET`. Two consequences that the
//! rest of the crate leans on:
//!
//! - the node partition of a level is a pure function of that level's key
//!   sequence, so a tree built incrementally is byte-identical to one bulk
//!   built from the same key set (asserted in `tree::tests`), and
//! - an edit can only reshape the nodes between the two surrounding
//!   boundary keys, which is what makes `Tree::apply` cost O(changed).
//!
//! The plan's "hard min/max entry clamps" turn out to be
//! determinism-safe, which was not obvious: because a node's *start* is
//! itself context-free, "seal after MAX entries counted from the start"
//! is still a pure function of the key sequence. `MAX_ENTRIES` (0 = off)
//! therefore clips the geometric distribution's tail without weakening
//! anything. The one place it costs something is `Tree::apply`: a node
//! sealed by the clamp rather than by a boundary key does not
//! re-synchronize with its untouched right neighbour, so the rewrite run
//! has to absorb it. §0.1 measures the tail with and without the clamp.

pub type Hash = [u8; 32];

/// Entries per node, chosen so an encoded node lands near 8 KiB at the
/// census value sizes (plan 28 Appendix A).
pub const LEAF_TARGET: u32 = 117;
pub const INTERIOR_TARGET: u32 = 115;

const MAGIC: &[u8; 4] = b"MTN1";
const HDR: usize = 9;

/// Hard ceiling on entries per node, 0 to disable. Set once at startup.
pub static MAX_ENTRIES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[inline]
pub fn max_entries() -> usize {
    MAX_ENTRIES.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn target_for(level: u8) -> u32 {
    if level == 0 {
        LEAF_TARGET
    } else {
        INTERIOR_TARGET
    }
}

/// True when a node at `level` ends after `key`.
///
/// A different 4-byte window per level decorrelates the levels; without
/// it every boundary key of level 0 would also be one at level 1 and the
/// tree would degenerate to a linked list of 1-entry interior nodes.
#[inline]
pub fn is_boundary(key: &[u8], level: u8) -> bool {
    let h = blake3::hash(key);
    let b = h.as_bytes();
    let w = (level as usize % 8) * 4;
    let v = u32::from_le_bytes([b[w], b[w + 1], b[w + 2], b[w + 3]]);
    v < u32::MAX / target_for(level)
}

/// Subtree summary carried by every interior entry (§P7).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Agg {
    pub bytes: u64,
    pub files: u64,
    pub keys: u64,
    pub max_mtime: i64,
}

impl Agg {
    pub fn merge(&mut self, other: &Agg) {
        self.bytes += other.bytes;
        self.files += other.files;
        self.keys += other.keys;
        self.max_mtime = self.max_mtime.max(other.max_mtime);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ent {
    Leaf(Vec<u8>),
    Child(Hash, Agg),
}

pub type Entry = (Vec<u8>, Ent);

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

fn get_varint(buf: &[u8], at: &mut usize) -> u64 {
    let mut v = 0u64;
    let mut shift = 0;
    loop {
        let b = buf[*at];
        *at += 1;
        v |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return v;
        }
        shift += 7;
    }
}

pub fn encode(level: u8, entries: &[Entry]) -> Vec<u8> {
    let mut body: Vec<u8> = Vec::with_capacity(entries.len() * 80);
    let mut offsets: Vec<u32> = Vec::with_capacity(entries.len());
    for (key, ent) in entries {
        offsets.push(body.len() as u32);
        body.extend_from_slice(&(key.len() as u16).to_le_bytes());
        match ent {
            Ent::Leaf(val) => {
                body.extend_from_slice(&(val.len() as u16).to_le_bytes());
                body.extend_from_slice(key);
                body.extend_from_slice(val);
            }
            Ent::Child(hash, agg) => {
                body.extend_from_slice(key);
                body.extend_from_slice(hash);
                put_varint(&mut body, agg.bytes);
                put_varint(&mut body, agg.files);
                put_varint(&mut body, agg.keys);
                put_varint(&mut body, agg.max_mtime as u64);
            }
        }
    }
    let mut out = Vec::with_capacity(HDR + entries.len() * 4 + body.len());
    out.extend_from_slice(MAGIC);
    out.push(level);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for off in &offsets {
        out.extend_from_slice(&off.to_le_bytes());
    }
    out.extend_from_slice(&body);
    out
}

pub fn hash_of(bytes: &[u8]) -> Hash {
    *blake3::hash(bytes).as_bytes()
}

/// Zero-copy reader over an encoded node.
pub struct NodeRef<'a> {
    pub level: u8,
    pub count: usize,
    buf: &'a [u8],
    ent0: usize,
}

impl<'a> NodeRef<'a> {
    pub fn new(buf: &'a [u8]) -> NodeRef<'a> {
        debug_assert_eq!(&buf[0..4], MAGIC);
        let level = buf[4];
        let count = u32::from_le_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
        NodeRef {
            level,
            count,
            buf,
            ent0: HDR + count * 4,
        }
    }

    #[inline]
    fn entry_at(&self, i: usize) -> usize {
        let o = HDR + i * 4;
        self.ent0
            + u32::from_le_bytes([
                self.buf[o],
                self.buf[o + 1],
                self.buf[o + 2],
                self.buf[o + 3],
            ]) as usize
    }

    #[inline]
    pub fn key(&self, i: usize) -> &'a [u8] {
        let at = self.entry_at(i);
        let klen = u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize;
        let start = if self.level == 0 { at + 4 } else { at + 2 };
        &self.buf[start..start + klen]
    }

    pub fn leaf_val(&self, i: usize) -> &'a [u8] {
        let at = self.entry_at(i);
        let klen = u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize;
        let vlen = u16::from_le_bytes([self.buf[at + 2], self.buf[at + 3]]) as usize;
        let start = at + 4 + klen;
        &self.buf[start..start + vlen]
    }

    pub fn child(&self, i: usize) -> (Hash, Agg) {
        let at = self.entry_at(i);
        let klen = u16::from_le_bytes([self.buf[at], self.buf[at + 1]]) as usize;
        let mut p = at + 2 + klen;
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&self.buf[p..p + 32]);
        p += 32;
        let bytes = get_varint(self.buf, &mut p);
        let files = get_varint(self.buf, &mut p);
        let keys = get_varint(self.buf, &mut p);
        let max_mtime = get_varint(self.buf, &mut p) as i64;
        (
            hash,
            Agg {
                bytes,
                files,
                keys,
                max_mtime,
            },
        )
    }

    pub fn agg(&self) -> Agg {
        let mut agg = Agg::default();
        if self.level == 0 {
            for i in 0..self.count {
                agg.merge(&crate::keys::leaf_agg(self.key(i), self.leaf_val(i)));
            }
            agg.keys = self.count as u64;
        } else {
            for i in 0..self.count {
                agg.merge(&self.child(i).1);
            }
        }
        agg
    }

    /// `Ok(i)` on an exact key hit, `Err(i)` with the insertion point.
    #[inline]
    pub fn search(&self, key: &[u8]) -> Result<usize, usize> {
        let (mut lo, mut hi) = (0usize, self.count);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.key(mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Index of the child whose key range contains `key`.
    #[inline]
    pub fn descend(&self, key: &[u8]) -> usize {
        match self.search(key) {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        }
    }

    pub fn entries(&self) -> Vec<Entry> {
        (0..self.count)
            .map(|i| {
                let k = self.key(i).to_vec();
                if self.level == 0 {
                    (k, Ent::Leaf(self.leaf_val(i).to_vec()))
                } else {
                    let (h, a) = self.child(i);
                    (k, Ent::Child(h, a))
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_round_trip_and_search() {
        let entries: Vec<Entry> = (0u32..50)
            .map(|i| (i.to_be_bytes().to_vec(), Ent::Leaf(vec![i as u8; 7])))
            .collect();
        let buf = encode(0, &entries);
        let n = NodeRef::new(&buf);
        assert_eq!(n.level, 0);
        assert_eq!(n.count, 50);
        assert_eq!(n.entries(), entries);
        assert_eq!(n.search(&7u32.to_be_bytes()), Ok(7));
        assert_eq!(n.search(&99u32.to_be_bytes()), Err(50));
    }

    #[test]
    fn interior_round_trip_keeps_aggregates() {
        let agg = Agg {
            bytes: 1 << 40,
            files: 12345,
            keys: 999,
            max_mtime: 1_700_000_000_000_000_000,
        };
        let entries = vec![(b"k".to_vec(), Ent::Child([9u8; 32], agg))];
        let buf = encode(3, &entries);
        let n = NodeRef::new(&buf);
        assert_eq!(n.level, 3);
        assert_eq!(n.child(0), ([9u8; 32], agg));
    }

    #[test]
    fn boundary_rate_tracks_target() {
        let hits = (0u32..200_000)
            .filter(|i| is_boundary(&i.to_be_bytes(), 0))
            .count();
        let expected = 200_000f64 / LEAF_TARGET as f64;
        assert!((hits as f64) > expected * 0.8 && (hits as f64) < expected * 1.2);
        // Levels must not agree on boundaries or the tree degenerates.
        let both = (0u32..200_000)
            .filter(|i| is_boundary(&i.to_be_bytes(), 0) && is_boundary(&i.to_be_bytes(), 1))
            .count();
        assert!(both < hits / 10);
    }
}
