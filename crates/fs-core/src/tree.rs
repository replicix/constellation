//! Immutable snapshot tree objects (DESIGN.md §13).
//!
//! A directory is encoded as one deterministic `CTR2` blob.  Its entries
//! carry the complete inode metadata needed by a frozen view and point to
//! either another tree object or an encoded file manifest.  Names are sorted
//! before encoding, so an unchanged directory has the same BLAKE3 identity
//! no matter which node built it or when the snapshot was named.
//!
//! The format deliberately does not use serde: this is persistent storage,
//! and an explicit little-endian layout makes versioning and corruption
//! checks visible rather than inheriting a serializer's representation.

use crate::{ChunkHash, CoreError, InodeKind};

const MAGIC_V1: &[u8; 4] = b"CTR1";
const MAGIC_V2: &[u8; 4] = b"CTR2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
    /// Attributes of the directory represented by this tree object.
    pub xattrs: Vec<(String, Vec<u8>)>,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub kind: InodeKind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub target: Option<String>,
    pub xattrs: Vec<(String, Vec<u8>)>,
    /// Child tree for a directory, encoded manifest for a file, absent for
    /// symlinks and special nodes.
    pub manifest_or_tree_hash: Option<ChunkHash>,
}

impl Tree {
    pub fn new(mut entries: Vec<TreeEntry>) -> Result<Self, CoreError> {
        for entry in &mut entries {
            entry
                .xattrs
                .sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            if entry.xattrs.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(CoreError::CorruptTree("duplicate xattr name".into()));
            }
        }
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        if entries.windows(2).any(|w| w[0].name == w[1].name) {
            return Err(CoreError::CorruptTree("duplicate entry name".into()));
        }
        Ok(Self {
            xattrs: Vec::new(),
            entries,
        })
    }

    pub fn with_xattrs(mut self, mut xattrs: Vec<(String, Vec<u8>)>) -> Result<Self, CoreError> {
        sort_xattrs(&mut xattrs)?;
        self.xattrs = xattrs;
        Ok(self)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC_V2);
        put_u32(&mut out, self.entries.len() as u32);
        put_xattrs(&mut out, &self.xattrs);
        for entry in &self.entries {
            put_bytes(&mut out, entry.name.as_bytes());
            out.push(entry.kind.as_u8());
            put_u32(&mut out, entry.mode);
            put_u32(&mut out, entry.uid);
            put_u32(&mut out, entry.gid);
            put_u64(&mut out, entry.size);
            out.extend_from_slice(&entry.mtime_ns.to_le_bytes());
            put_optional_bytes(&mut out, entry.target.as_deref().map(str::as_bytes));
            match entry.manifest_or_tree_hash {
                Some(hash) => {
                    out.push(1);
                    out.extend_from_slice(&hash.0);
                }
                None => out.push(0),
            }
            put_xattrs(&mut out, &entry.xattrs);
        }
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, CoreError> {
        let mut input = data;
        let magic = take(&mut input, 4)?;
        let has_xattrs = if magic == MAGIC_V2 {
            true
        } else if magic == MAGIC_V1 {
            false
        } else {
            return Err(CoreError::CorruptTree("bad magic".into()));
        };
        let count = read_u32(&mut input)? as usize;
        let xattrs = if has_xattrs {
            read_xattrs(&mut input)?
        } else {
            Vec::new()
        };
        // `count` is attacker/corruption-controlled (a `u32` off the bucket),
        // and each entry costs `MIN_ENTRY_BYTES`, so a valid blob of `count`
        // entries needs at least `count * MIN_ENTRY_BYTES` more bytes. Cap the
        // pre-allocation at what the remaining input could actually hold: a
        // larger `count` is guaranteed truncated and fails in the loop below,
        // but reserving for it first would let a tiny object abort the process
        // in `handle_alloc_error`. See `manifest::decode_chunk_list`.
        let mut entries = Vec::with_capacity(count.min(input.len() / MIN_ENTRY_BYTES));
        for _ in 0..count {
            let name = String::from_utf8(read_bytes(&mut input)?.to_vec())
                .map_err(|_| CoreError::CorruptTree("entry name is not UTF-8".into()))?;
            let kind = InodeKind::from_u8(take(&mut input, 1)?[0])
                .ok_or_else(|| CoreError::CorruptTree("unknown inode kind".into()))?;
            let mode = read_u32(&mut input)?;
            let uid = read_u32(&mut input)?;
            let gid = read_u32(&mut input)?;
            let size = read_u64(&mut input)?;
            let mtime_ns = i64::from_le_bytes(take(&mut input, 8)?.try_into().unwrap());
            let target = read_optional_bytes(&mut input)?
                .map(|bytes| String::from_utf8(bytes.to_vec()))
                .transpose()
                .map_err(|_| CoreError::CorruptTree("symlink target is not UTF-8".into()))?;
            let manifest_or_tree_hash = match take(&mut input, 1)?[0] {
                0 => None,
                1 => Some(ChunkHash(take(&mut input, 32)?.try_into().unwrap())),
                _ => return Err(CoreError::CorruptTree("invalid hash tag".into())),
            };
            let xattrs = if has_xattrs {
                read_xattrs(&mut input)?
            } else {
                Vec::new()
            };
            entries.push(TreeEntry {
                name,
                kind,
                mode,
                uid,
                gid,
                size,
                mtime_ns,
                target,
                manifest_or_tree_hash,
                xattrs,
            });
        }
        if !input.is_empty() {
            return Err(CoreError::CorruptTree("trailing bytes".into()));
        }
        let tree = Self::new(entries)?.with_xattrs(xattrs)?;
        if tree.entries.windows(2).any(|w| w[0].name >= w[1].name) {
            return Err(CoreError::CorruptTree("entries are not sorted".into()));
        }
        Ok(tree)
    }

    pub fn hash(&self) -> ChunkHash {
        ChunkHash::of(&self.encode())
    }
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn put_optional_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            out.push(1);
            put_bytes(out, value);
        }
        None => out.push(0),
    }
}

fn sort_xattrs(xattrs: &mut [(String, Vec<u8>)]) -> Result<(), CoreError> {
    xattrs.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if xattrs.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(CoreError::CorruptTree("duplicate xattr name".into()));
    }
    Ok(())
}

fn put_xattrs(out: &mut Vec<u8>, xattrs: &[(String, Vec<u8>)]) {
    put_u32(out, xattrs.len() as u32);
    for (name, value) in xattrs {
        put_bytes(out, name.as_bytes());
        put_bytes(out, value);
    }
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], CoreError> {
    if input.len() < len {
        return Err(CoreError::CorruptTree("truncated blob".into()));
    }
    let (value, rest) = input.split_at(len);
    *input = rest;
    Ok(value)
}

fn read_u32(input: &mut &[u8]) -> Result<u32, CoreError> {
    Ok(u32::from_le_bytes(take(input, 4)?.try_into().unwrap()))
}

fn read_u64(input: &mut &[u8]) -> Result<u64, CoreError> {
    Ok(u64::from_le_bytes(take(input, 8)?.try_into().unwrap()))
}

fn read_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], CoreError> {
    let len = read_u32(input)? as usize;
    take(input, len)
}

fn read_optional_bytes<'a>(input: &mut &'a [u8]) -> Result<Option<&'a [u8]>, CoreError> {
    match take(input, 1)?[0] {
        0 => Ok(None),
        1 => read_bytes(input).map(Some),
        _ => Err(CoreError::CorruptTree("invalid optional tag".into())),
    }
}

/// Smallest byte cost of one encoded [`TreeEntry`]: a 4-byte empty-name
/// length prefix, the 1-byte kind, `mode`/`uid`/`gid` (4 each), `size` and
/// `mtime` (8 each), and the one-byte `target`/hash tags. A `CTR2` entry
/// adds a 4-byte empty-xattr count, so 35 is a safe lower bound for both
/// formats and only ever *over*-estimates how many entries could fit.
const MIN_ENTRY_BYTES: usize = 35;
/// Smallest byte cost of one encoded xattr: a 4-byte name length prefix and
/// a 4-byte value length prefix, both empty.
const MIN_XATTR_BYTES: usize = 8;

fn read_xattrs(input: &mut &[u8]) -> Result<Vec<(String, Vec<u8>)>, CoreError> {
    let count = read_u32(input)? as usize;
    // Bounded like `Tree::decode`: never reserve for more xattrs than the
    // remaining input could encode, so a crafted count cannot force a
    // process-aborting allocation.
    let mut xattrs = Vec::with_capacity(count.min(input.len() / MIN_XATTR_BYTES));
    for _ in 0..count {
        let name = String::from_utf8(read_bytes(input)?.to_vec())
            .map_err(|_| CoreError::CorruptTree("xattr name is not UTF-8".into()))?;
        xattrs.push((name, read_bytes(input)?.to_vec()));
    }
    Ok(xattrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<TreeEntry> {
        vec![
            TreeEntry {
                name: "z-file".into(),
                kind: InodeKind::File,
                mode: 0o640,
                uid: 12,
                gid: 34,
                size: 99,
                mtime_ns: 123,
                target: None,
                manifest_or_tree_hash: Some(ChunkHash::of(b"manifest")),
                xattrs: vec![("user.foo".into(), b"bar".to_vec())],
            },
            TreeEntry {
                name: "a-link".into(),
                kind: InodeKind::Symlink,
                mode: 0o777,
                uid: 1,
                gid: 2,
                size: 6,
                mtime_ns: 456,
                target: Some("target".into()),
                manifest_or_tree_hash: None,
                xattrs: Vec::new(),
            },
        ]
    }

    #[test]
    fn round_trip_is_sorted_and_self_describing() {
        let tree = Tree::new(entries())
            .unwrap()
            .with_xattrs(vec![("user.root".into(), b"value".to_vec())])
            .unwrap();
        assert_eq!(tree.entries[0].name, "a-link");
        let encoded = tree.encode();
        assert_eq!(&encoded[..4], b"CTR2");
        assert_eq!(Tree::decode(&encoded).unwrap(), tree);
    }

    #[test]
    fn legacy_ctr1_decodes_with_empty_xattrs() {
        let all = entries();
        let entry = &all[1];
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC_V1);
        put_u32(&mut encoded, 1);
        put_bytes(&mut encoded, entry.name.as_bytes());
        encoded.push(entry.kind.as_u8());
        put_u32(&mut encoded, entry.mode);
        put_u32(&mut encoded, entry.uid);
        put_u32(&mut encoded, entry.gid);
        put_u64(&mut encoded, entry.size);
        encoded.extend_from_slice(&entry.mtime_ns.to_le_bytes());
        put_optional_bytes(&mut encoded, entry.target.as_deref().map(str::as_bytes));
        encoded.push(0);
        let tree = Tree::decode(&encoded).unwrap();
        assert!(tree.xattrs.is_empty());
        assert!(tree.entries[0].xattrs.is_empty());
    }

    #[test]
    fn unchanged_directory_hash_is_stable() {
        let a = Tree::new(entries()).unwrap();
        let mut reversed = entries();
        reversed.reverse();
        let b = Tree::new(reversed).unwrap();
        assert_eq!(a.hash(), b.hash());
        assert_eq!(a.encode(), b.encode());
    }

    #[test]
    fn corruption_is_rejected() {
        let mut encoded = Tree::new(entries()).unwrap().encode();
        encoded.pop();
        assert!(Tree::decode(&encoded).is_err());
        assert!(Tree::decode(b"wrong").is_err());
    }

    /// A tiny blob that claims billions of entries (or xattrs) must fail
    /// cleanly as truncated, never reserve a `Vec` sized from the untrusted
    /// count — which would abort the process in `handle_alloc_error`.
    #[test]
    fn a_huge_entry_count_does_not_allocate() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC_V1);
        put_u32(&mut encoded, u32::MAX); // claims ~4.29B entries in ~8 bytes
        assert!(matches!(
            Tree::decode(&encoded),
            Err(CoreError::CorruptTree(_))
        ));

        // Same for the file-level xattr count of a CTR2 blob.
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC_V2);
        put_u32(&mut encoded, 0); // entry count
        put_u32(&mut encoded, u32::MAX); // file xattr count
        assert!(matches!(
            Tree::decode(&encoded),
            Err(CoreError::CorruptTree(_))
        ));

        // And for a per-entry xattr count.
        let mut encoded = Vec::new();
        encoded.extend_from_slice(MAGIC_V2);
        put_u32(&mut encoded, 1); // one entry
        put_u32(&mut encoded, 0); // no file xattrs
        put_bytes(&mut encoded, b"f"); // name
        encoded.push(InodeKind::File.as_u8());
        put_u32(&mut encoded, 0o644);
        put_u32(&mut encoded, 0);
        put_u32(&mut encoded, 0);
        put_u64(&mut encoded, 0);
        encoded.extend_from_slice(&0i64.to_le_bytes());
        put_optional_bytes(&mut encoded, None);
        encoded.push(0); // no manifest/tree hash
        put_u32(&mut encoded, u32::MAX); // per-entry xattr count
        assert!(matches!(
            Tree::decode(&encoded),
            Err(CoreError::CorruptTree(_))
        ));
    }
}
