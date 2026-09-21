//! The plan 28 §P6 key encoding and the value records it points at.
//!
//! Big-endian throughout, so byte order is numeric order and a `readdir`
//! is a contiguous range scan. The prefixes implemented here are the ones
//! Step 0 measures:
//!
//! | prefix | key | value |
//! |---|---|---|
//! | `0x01` | `ino` | inode record (attrs, nlink, inline manifest, inline xattrs) |
//! | `0x02` | `parent_ino \| name` | `(ino, kind)` plus the READDIRPLUS attr copy |
//! | `0x03` | `ino \| xattr_name` | spilled xattr value |
//! | `0x04` | `ino \| parent_ino \| name` | `()` — reverse dentry index |
//!
//! Step S1 asks whether the attr copy earns its keep, so the `0x02` value
//! shape is a run-time choice rather than a constant. The three shapes are
//! [`Enc`]; everything downstream — corpus generation, commit batches, the
//! `ls -la` and `getattr` paths — reads the current one rather than
//! assuming §P6 as written.

use std::sync::atomic::{AtomicU8, Ordering};

use crate::node::Agg;

pub const K_INODE: u8 = 0x01;
pub const K_DENTRY: u8 = 0x02;
pub const K_XATTR: u8 = 0x03;
pub const K_RDENTRY: u8 = 0x04;

/// Inline xattr budget inside the inode record (§P6).
pub const XATTR_INLINE: usize = 256;

pub const KIND_FILE: u8 = 1;
pub const KIND_DIR: u8 = 2;
pub const KIND_LINK: u8 = 3;

/// What a `0x02` dentry value carries — the S1 question.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Enc {
    /// §P6 as written: `ino` plus a denormalized copy of the attrs
    /// READDIRPLUS returns. `ls -la` is one range scan; `setattr` writes
    /// two keys in two distant ranges.
    Copy,
    /// `ino` and `kind` only. `ls -la` becomes a `0x02` range scan plus a
    /// point read per child into `0x01`; `setattr` writes one key.
    NoCopy,
    /// §P6's own escape hatch: for `nlink == 1` the dentry *is* the
    /// authoritative record — attrs, manifest and inline xattrs live in
    /// the `0x02` value and no `0x01` key exists at all — so `setattr`
    /// writes one key and `getattr(ino)` hops through `0x04` first.
    /// Directories keep their `0x01` record.
    DentryAuth,
}

/// Process-wide because every key- and value-producing path in this
/// benchmark would otherwise have to thread an encoding parameter through
/// corpus generation, the commit batches, and the read drivers. The
/// measurement sets it once per variant and builds a fresh tree.
static ENC: AtomicU8 = AtomicU8::new(0);

pub fn enc() -> Enc {
    match ENC.load(Ordering::Relaxed) {
        1 => Enc::NoCopy,
        2 => Enc::DentryAuth,
        _ => Enc::Copy,
    }
}

pub fn set_enc(e: Enc) {
    ENC.store(
        match e {
            Enc::Copy => 0,
            Enc::NoCopy => 1,
            Enc::DentryAuth => 2,
        },
        Ordering::Relaxed,
    );
}

impl Enc {
    pub fn label(self) -> &'static str {
        match self {
            Enc::Copy => "attr copy (§P6)",
            Enc::NoCopy => "no attr copy",
            Enc::DentryAuth => "dentry-authoritative",
        }
    }

    pub fn flag(self) -> &'static str {
        match self {
            Enc::Copy => "copy",
            Enc::NoCopy => "nocopy",
            Enc::DentryAuth => "dentry-auth",
        }
    }
}

/// Whether an entry of this kind still carries a `0x01` record. Only
/// `Enc::DentryAuth` answers no, and only for the `nlink == 1` entries
/// whose dentry became authoritative.
pub fn has_inode_record(kind: u8) -> bool {
    enc() != Enc::DentryAuth || kind == KIND_DIR
}

pub fn inode_key(ino: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(K_INODE);
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

pub fn dentry_key(parent: u64, name: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + name.len());
    k.push(K_DENTRY);
    k.extend_from_slice(&parent.to_be_bytes());
    k.extend_from_slice(name);
    k
}

pub fn dentry_prefix(parent: u64) -> Vec<u8> {
    dentry_key(parent, b"")
}

pub fn xattr_key(ino: u64, name: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + name.len());
    k.push(K_XATTR);
    k.extend_from_slice(&ino.to_be_bytes());
    k.extend_from_slice(name);
    k
}

pub fn xattr_prefix(ino: u64) -> Vec<u8> {
    xattr_key(ino, b"")
}

/// The `0x04` range holding every name that points at `ino` — one entry
/// for a `nlink == 1` file, which is what makes the dentry-authoritative
/// variant's `getattr` hop possible.
pub fn rdentry_prefix(ino: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(K_RDENTRY);
    k.extend_from_slice(&ino.to_be_bytes());
    k
}

pub fn rdentry_key(ino: u64, parent: u64, name: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(17 + name.len());
    k.push(K_RDENTRY);
    k.extend_from_slice(&ino.to_be_bytes());
    k.extend_from_slice(&parent.to_be_bytes());
    k.extend_from_slice(name);
    k
}

/// Attributes shared by the inode record and the dentry's attr copy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Attrs {
    pub kind: u8,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub atime_ns: i64,
}

fn put_attrs(out: &mut Vec<u8>, a: &Attrs) {
    out.push(a.kind);
    out.extend_from_slice(&a.mode.to_le_bytes());
    out.extend_from_slice(&a.uid.to_le_bytes());
    out.extend_from_slice(&a.gid.to_le_bytes());
    out.extend_from_slice(&a.nlink.to_le_bytes());
    out.extend_from_slice(&a.size.to_le_bytes());
    out.extend_from_slice(&a.mtime_ns.to_le_bytes());
    out.extend_from_slice(&a.ctime_ns.to_le_bytes());
    out.extend_from_slice(&a.atime_ns.to_le_bytes());
}

pub const ATTRS_LEN: usize = 1 + 4 * 4 + 8 * 4;

pub fn read_attrs(buf: &[u8]) -> Attrs {
    let u32at = |o: usize| u32::from_le_bytes(buf[o..o + 4].try_into().unwrap());
    let i64at = |o: usize| i64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
    Attrs {
        kind: buf[0],
        mode: u32at(1),
        uid: u32at(5),
        gid: u32at(9),
        nlink: u32at(13),
        size: i64at(17) as u64,
        mtime_ns: i64at(25),
        ctime_ns: i64at(33),
        atime_ns: i64at(41),
    }
}

/// `0x01` value: attrs, then the inline manifest (chunk hashes up to
/// `INLINE_CHUNKS_MAX`), then inline xattrs when the whole set fits in
/// `XATTR_INLINE`.
pub fn inode_val(a: &Attrs, chunks: &[[u8; 32]], xattrs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut v = Vec::with_capacity(ATTRS_LEN + chunks.len() * 32 + 8);
    put_attrs(&mut v, a);
    v.push(chunks.len() as u8);
    for c in chunks {
        v.extend_from_slice(c);
    }
    v.push(xattrs.len() as u8);
    for (name, val) in xattrs {
        v.push(name.len() as u8);
        v.extend_from_slice(name);
        v.push(val.len() as u8);
        v.extend_from_slice(val);
    }
    v
}

/// `0x02` value in the current encoding. The manifest and xattrs are only
/// read by `Enc::DentryAuth`, where the dentry is the record; the other
/// two shapes ignore them.
pub fn dentry_val_with(
    ino: u64,
    a: &Attrs,
    chunks: &[[u8; 32]],
    xattrs: &[(Vec<u8>, Vec<u8>)],
) -> Vec<u8> {
    let mut v = Vec::with_capacity(8 + ATTRS_LEN);
    v.extend_from_slice(&ino.to_le_bytes());
    match enc() {
        Enc::Copy => put_attrs(&mut v, a),
        // `kind` is the first attr field, so every shape keeps it at the
        // same offset and `dentry_kind` needs no branch.
        Enc::NoCopy => v.push(a.kind),
        Enc::DentryAuth => v.extend_from_slice(&inode_val(a, chunks, xattrs)),
    }
    v
}

/// `0x02` value for an entry with no manifest and no xattrs.
pub fn dentry_val(ino: u64, a: &Attrs) -> Vec<u8> {
    dentry_val_with(ino, a, &[], &[])
}

pub fn dentry_ino(val: &[u8]) -> u64 {
    u64::from_le_bytes(val[0..8].try_into().unwrap())
}

/// `kind` is the first attr field and the only one `Enc::NoCopy` keeps, so
/// every shape holds it at the same offset.
#[cfg(test)]
pub fn dentry_kind(val: &[u8]) -> u8 {
    val[8]
}

#[cfg(test)]
pub fn dentry_attrs(val: &[u8]) -> Attrs {
    read_attrs(&val[8..])
}

/// Per-entry contribution to the §P7 interior aggregates, read from
/// whichever key holds the authoritative attrs in this encoding. A file's
/// bytes are counted once: under `Enc::Copy` the dentry copy is ignored,
/// under `Enc::DentryAuth` there is no inode record to ignore it for.
pub fn leaf_agg(key: &[u8], val: &[u8]) -> Agg {
    let at = match key.first() {
        Some(&K_INODE) if val.len() >= ATTRS_LEN => 0,
        Some(&K_DENTRY) if enc() == Enc::DentryAuth && val.len() >= 8 + ATTRS_LEN => 8,
        _ => return Agg::default(),
    };
    let a = read_attrs(&val[at..]);
    Agg {
        bytes: if a.kind == KIND_FILE { a.size } else { 0 },
        files: if a.kind == KIND_FILE { 1 } else { 0 },
        keys: 0,
        max_mtime: a.mtime_ns,
    }
}

/// `ENC` is process-wide, so the tests that flip it — and the tests whose
/// expectations depend on it — must not run concurrently.
#[cfg(test)]
pub static ENC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_order_is_numeric_order() {
        assert!(inode_key(1) < inode_key(2));
        assert!(inode_key(255) < inode_key(256));
        assert!(inode_key(u64::MAX - 1) < inode_key(u64::MAX));
        assert!(dentry_key(7, b"a") < dentry_key(7, b"b"));
        assert!(dentry_key(7, b"zzz") < dentry_key(8, b"a"));
        // Prefixes keep the four indexes in disjoint, ordered ranges.
        assert!(inode_key(u64::MAX) < dentry_key(0, b""));
        assert!(dentry_key(u64::MAX, b"\xff") < xattr_prefix(0));
        assert!(xattr_key(u64::MAX, b"\xff") < rdentry_key(0, 0, b""));
    }

    #[test]
    fn readdir_range_is_contiguous_and_bounded() {
        let lo = dentry_prefix(42);
        let hi = dentry_prefix(43);
        assert!(dentry_key(42, b"file") > lo && dentry_key(42, b"file") < hi);
        assert!(dentry_key(41, b"zzz") < lo);
    }

    #[test]
    fn attr_round_trip() {
        let _g = ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let a = Attrs {
            kind: KIND_FILE,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            size: 1 << 33,
            mtime_ns: -5,
            ctime_ns: 7,
            atime_ns: 9,
        };
        let v = inode_val(&a, &[[3u8; 32]], &[]);
        assert_eq!(read_attrs(&v), a);
        let d = dentry_val(77, &a);
        assert_eq!(dentry_ino(&d), 77);
        assert_eq!(dentry_attrs(&d), a);
    }

    /// The S1 variants change the `0x02` value and, for `DentryAuth`, which
    /// key is authoritative — never the key encoding itself, because the
    /// ordering properties above are what make `readdir` a range scan.
    #[test]
    fn variants_agree_on_keys_and_on_ino_and_kind() {
        let _g = ENC_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let a = Attrs {
            kind: KIND_FILE,
            mode: 0o644,
            uid: 1,
            gid: 2,
            nlink: 1,
            size: 4096,
            mtime_ns: 11,
            ctime_ns: 12,
            atime_ns: 13,
        };
        let chunks = [[7u8; 32]];
        let xattrs = vec![(b"user.a".to_vec(), b"v".to_vec())];
        let mut lens = Vec::new();
        for e in [Enc::Copy, Enc::NoCopy, Enc::DentryAuth] {
            set_enc(e);
            assert_eq!(
                dentry_key(7, b"a"),
                [&[K_DENTRY][..], &7u64.to_be_bytes(), b"a"].concat()
            );
            let d = dentry_val_with(9, &a, &chunks, &xattrs);
            assert_eq!(dentry_ino(&d), 9);
            assert_eq!(dentry_kind(&d), KIND_FILE);
            assert!(has_inode_record(KIND_DIR));
            assert_eq!(has_inode_record(KIND_FILE), e != Enc::DentryAuth);
            // Whichever keys this encoding actually writes for one file,
            // the file is counted exactly once in the §P7 aggregates.
            let mut files = leaf_agg(&dentry_key(7, b"a"), &d).files;
            if has_inode_record(KIND_FILE) {
                files += leaf_agg(&inode_key(9), &inode_val(&a, &chunks, &xattrs)).files;
            }
            assert_eq!(files, 1);
            lens.push(d.len());
        }
        set_enc(Enc::Copy);
        // NoCopy is the smallest dentry, DentryAuth the largest.
        assert!(lens[1] < lens[0] && lens[0] < lens[2]);
    }
}
