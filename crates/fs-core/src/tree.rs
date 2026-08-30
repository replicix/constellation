//! Immutable snapshot tree objects (DESIGN.md §13).
//!
//! A directory is encoded as one deterministic `CTR1` blob.  Its entries
//! carry the complete inode metadata needed by a frozen view and point to
//! either another tree object or an encoded file manifest.  Names are sorted
//! before encoding, so an unchanged directory has the same BLAKE3 identity
//! no matter which node built it or when the snapshot was named.
//!
//! The format deliberately does not use serde: this is persistent storage,
//! and an explicit little-endian layout makes versioning and corruption
//! checks visible rather than inheriting a serializer's representation.

use crate::{ChunkHash, CoreError, InodeKind};

const MAGIC: &[u8; 4] = b"CTR1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
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
    /// Child tree for a directory, encoded manifest for a file, absent for
    /// symlinks and special nodes.
    pub manifest_or_tree_hash: Option<ChunkHash>,
}

impl Tree {
    pub fn new(mut entries: Vec<TreeEntry>) -> Result<Self, CoreError> {
        entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
        if entries.windows(2).any(|w| w[0].name == w[1].name) {
            return Err(CoreError::CorruptTree("duplicate entry name".into()));
        }
        Ok(Self { entries })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        put_u32(&mut out, self.entries.len() as u32);
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
        }
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, CoreError> {
        let mut input = data;
        if take(&mut input, 4)? != MAGIC {
            return Err(CoreError::CorruptTree("bad magic".into()));
        }
        let count = read_u32(&mut input)? as usize;
        let mut entries = Vec::with_capacity(count);
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
            });
        }
        if !input.is_empty() {
            return Err(CoreError::CorruptTree("trailing bytes".into()));
        }
        let tree = Self::new(entries)?;
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
            },
        ]
    }

    #[test]
    fn round_trip_is_sorted_and_self_describing() {
        let tree = Tree::new(entries()).unwrap();
        assert_eq!(tree.entries[0].name, "a-link");
        let encoded = tree.encode();
        assert_eq!(&encoded[..4], b"CTR1");
        assert_eq!(Tree::decode(&encoded).unwrap(), tree);
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
}
