//! The values the §P6 key ranges point at, and the inline/spill rules
//! that keep them small.
//!
//! [`crate::keys`] decides what a key means; this module decides what
//! sits under it. The two are separate files and one concern, so read
//! them together: a value's shape is only defensible in terms of the key
//! that reaches it.
//!
//! ## The shapes, and S1's verdict
//!
//! - `0x01` → [`InodeRecord`]: the authoritative attrs, `nlink`, `rdev`,
//!   the inline manifest or its hash, the symlink target, and the whole
//!   xattr set when it fits in [`XATTR_INLINE`].
//! - `0x02` → [`DentryRecord`]: the target `ino` **plus a denormalized
//!   copy of the attrs READDIRPLUS returns.** This copy is the one open
//!   question §P6 left, S1 measured it (§14.10), and the verdict is that
//!   it stays: dropping it saves 10.9% of stored bytes and 1.3–1.5× on
//!   `setattr`, and charges **12,039 pack reads for a cold `ls -la`
//!   against 30.4**, because `ls -la` stops being a sequential scan and
//!   becomes a scan plus N scattered point reads into the `0x01` range.
//!   That is the only cost in plan 28 that neither cache nor cores
//!   absorb (it still ran 144× slower at 8 threads). The copy is
//!   read-locality state, not accounting state, and [`leaf_agg`] is
//!   where that distinction is enforced.
//! - `0x03` → [`Payload`]: one xattr value, spilled to a blob above
//!   [`VALUE_SPILL`].
//! - `0x04` → `()`. The reverse dentry index is a *set*; everything a
//!   caller wants from it is already in the key, and a value would be a
//!   second copy of it to keep in agreement. [`RDENTRY_VALUE`].
//! - `0x30` → [`Payload`]: a subsystem record body, same spill rule.
//!
//! ## Bounded values, mirroring what manifests already do
//!
//! §P1 requires values to be small: nodes target 8 KiB and a point
//! lookup decompresses a whole leaf, so one oversized value taxes every
//! read of its neighbours. `fs-core::manifest` already solves this for
//! chunk lists — above `INLINE_CHUNKS_MAX` the list becomes a blob and
//! the manifest carries its hash — and §P6 generalizes that rule rather
//! than inventing one: anything above [`VALUE_SPILL`] becomes a blob
//! hash. [`Payload`] is that rule, and [`plan_inode`] applies it to a
//! whole inode record in a documented order so the outcome is a function
//! of the content and not of the caller's habits.
//!
//! Xattrs get their own rule one level up, because the cost there is
//! *keys*, not bytes: the census says most files have no xattrs and the
//! common non-empty case is a single label (SELinux, or
//! `user.constellation.*`). Inlining sets below [`XATTR_INLINE`] keeps
//! those out of the keyspace entirely — without it, Appendix B's 100M
//! files × 2 labels would add 200M keys and ~13 GiB of leaves.
//!
//! ## Why these types are declared here and not imported
//!
//! `constellation-meta` already holds an authoritative field list for
//! inodes, dentries and xattrs, and this module deliberately does not
//! use it. `meta` depends on `fs-core`, and S5/S6 need `meta` — or code
//! above it — to depend on `mtree`; an `mtree → meta` edge would close
//! that into a cycle that someone would have to unpick later. So the
//! field list below was *read* from `meta::sqlite`'s schema and
//! redeclared as plain data. [`Kind`]'s discriminants match
//! `fs_core::InodeKind::as_u8` so S5's mapping is a cast rather than a
//! table, and the one field that is missing is missing on purpose:
//!
//! **There is no `atime`.** §P6 excludes it from the tree entirely, so
//! it is absent from [`Attrs`] rather than merely absent from the keys.
//! A stored atime is a stored temptation: cluster-visible atime makes a
//! `find` over 100M files into 100M scattered inode writes driven by
//! reads, which is §14.4's pathological write shape arriving through the
//! read path. It stays node-local and best-effort (today's
//! `atime_journal` with its max-merge on apply), and never enters a
//! commit or a root hash. [`ATTRS_LEN`] is pinned by a test, so adding a
//! field here fails loudly on purpose.

use crate::keys::Key;
use crate::node::Agg;
use constellation_types::Rdev;

/// A whole xattr set at or below this many encoded bytes lives in the
/// inode record; above it, every name moves to its own `0x03` key
/// (§P6). Whole-set rather than per-name, so `listxattr` is either one
/// point read or one range scan and never both.
pub const XATTR_INLINE: usize = 256;

/// A value above this many bytes is replaced by the hash of a blob
/// holding it (§P1, §P6) — the rule `fs-core::manifest` already applies
/// to chunk lists.
pub const VALUE_SPILL: usize = 1024;

/// What kind of thing an inode is. Discriminants match
/// `fs_core::InodeKind::as_u8`, deliberately: this crate must not depend
/// on `meta`, but the encodings have to agree or S5's builder would
/// silently reinterpret every inode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Kind {
    #[default]
    File = 0,
    Dir = 1,
    Symlink = 2,
    Fifo = 3,
    Socket = 4,
    BlockDev = 5,
    CharDev = 6,
}

pub const KINDS: [Kind; 7] = [
    Kind::File,
    Kind::Dir,
    Kind::Symlink,
    Kind::Fifo,
    Kind::Socket,
    Kind::BlockDev,
    Kind::CharDev,
];

impl Kind {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(byte: u8) -> Result<Kind, RecordError> {
        KINDS
            .into_iter()
            .find(|k| k.as_u8() == byte)
            .ok_or(RecordError::UnknownKind(byte))
    }

    /// Only regular files hold data bytes, which is what §P7's `bytes`
    /// and `files` aggregates count — the same rule
    /// `meta::sqlite::recursive_size_conn` applies today, so `du`,
    /// `statfs` and quota answers do not change meaning when the tree
    /// becomes their source.
    pub fn holds_data(self) -> bool {
        self == Kind::File
    }
}

/// 32 bytes naming a spilled value in the blob store.
///
/// A third hash type next to [`crate::NodeHash`] and
/// `fs-core::ChunkHash`, for the reason the other two are distinct: the
/// same algorithm over three namespaces with three lifetimes. A node
/// hash addresses tree structure, a chunk hash addresses file data, and
/// a blob hash addresses an overflowed metadata value — reachable only
/// from the tree, and so swept by §P10's mark from the roots rather than
/// by the chunk-store rules.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobHash(pub [u8; 32]);

impl std::fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobHash({})", crate::NodeHash(self.0).to_hex())
    }
}

/// Why a value failed to decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    #[error("record is truncated: {0}")]
    Truncated(&'static str),

    #[error("unknown inode kind {0}")]
    UnknownKind(u8),

    #[error("unknown payload tag {0}")]
    UnknownPayloadTag(u8),

    /// The decoders are strict about this: a record with bytes left over
    /// is a version skew or a corruption, and reading it as valid would
    /// hide whichever it was.
    #[error("record has {0} trailing bytes")]
    TrailingBytes(usize),

    /// The `0x04` range is a set; its keys carry everything and its
    /// values must be empty.
    #[error("a reverse-dentry value must be empty, got {0} bytes")]
    NonEmptyRDentryValue(usize),

    #[error("unknown subsystem record version {0}")]
    UnknownRecordVersion(u8),
}

// --------------------------------------------------------------- attrs

/// The attributes an inode record holds and a dentry copies.
///
/// Fields are the `inode` table's, minus `atime_ns` (see the module
/// docs) and minus `ino` itself, which is in the key. Little-endian,
/// unlike keys: value bytes are never compared, so the only thing
/// endianness buys here is matching the rest of the repo's encoders.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Attrs {
    pub kind: Kind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    /// Device number for block and char devices, `(0, 0)` otherwise.
    /// Portable `(major, minor)` (plan 31 §7), encoded as two
    /// little-endian `u32`s in the 8 bytes the field always had.
    pub rdev: Rdev,
}

/// kind(1) + mode/uid/gid/nlink(4×4) + size/mtime/ctime(3×8) +
/// rdev major/minor(2×4).
///
/// Pinned by `the_attr_encoding_is_pinned`, which is the guard on the
/// atime rule: adding a tenth field moves this number and fails the
/// test, so reintroducing atime cannot happen quietly.
pub const ATTRS_LEN: usize = 1 + 4 * 4 + 4 * 8;

impl Attrs {
    fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(self.kind.as_u8());
        out.extend_from_slice(&self.mode.to_le_bytes());
        out.extend_from_slice(&self.uid.to_le_bytes());
        out.extend_from_slice(&self.gid.to_le_bytes());
        out.extend_from_slice(&self.nlink.to_le_bytes());
        out.extend_from_slice(&self.size.to_le_bytes());
        out.extend_from_slice(&self.mtime_ns.to_le_bytes());
        out.extend_from_slice(&self.ctime_ns.to_le_bytes());
        out.extend_from_slice(&self.rdev.major.to_le_bytes());
        out.extend_from_slice(&self.rdev.minor.to_le_bytes());
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(ATTRS_LEN);
        self.encode_into(&mut out);
        out
    }

    /// Decode the attrs prefix of a value, returning the rest.
    pub fn decode_prefix(buf: &[u8]) -> Result<(Attrs, &[u8]), RecordError> {
        if buf.len() < ATTRS_LEN {
            return Err(RecordError::Truncated("attrs"));
        }
        let u32at = |at: usize| u32::from_le_bytes(buf[at..at + 4].try_into().expect("4 bytes"));
        let i64at = |at: usize| i64::from_le_bytes(buf[at..at + 8].try_into().expect("8 bytes"));
        Ok((
            Attrs {
                kind: Kind::from_u8(buf[0])?,
                mode: u32at(1),
                uid: u32at(5),
                gid: u32at(9),
                nlink: u32at(13),
                size: i64at(17) as u64,
                mtime_ns: i64at(25),
                ctime_ns: i64at(33),
                rdev: Rdev::new(u32at(41), u32at(45)),
            },
            &buf[ATTRS_LEN..],
        ))
    }

    pub fn decode(buf: &[u8]) -> Result<Attrs, RecordError> {
        let (attrs, rest) = Attrs::decode_prefix(buf)?;
        if !rest.is_empty() {
            return Err(RecordError::TrailingBytes(rest.len()));
        }
        Ok(attrs)
    }
}

// ------------------------------------------------------------- payloads

/// A value that is either small enough to store inline or represented by
/// the hash of a blob holding it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Inline(Vec<u8>),
    Spilled(BlobHash),
}

const PAYLOAD_INLINE: u8 = 0;
const PAYLOAD_SPILLED: u8 = 1;

/// tag(1) + length(2) for an inline payload; tag(1) + hash(32) spilled.
const PAYLOAD_INLINE_OVERHEAD: usize = 3;

impl Payload {
    /// Apply the §P6 spill rule at `limit` bytes.
    ///
    /// Returns the payload and, when it spilled, the body the caller
    /// must store under its hash — the same `(value, Option<blob>)`
    /// shape `fs-core::Manifest::from_chunks` returns, so a caller that
    /// already handles a spilled manifest handles this identically.
    ///
    /// `hash_blob` is a parameter rather than a default for the reason
    /// `manifest.rs` learned the hard way: on an E2E filesystem blob
    /// identity is a *keyed* hash, and a hardcoded plain hash would
    /// record a reference that nothing can resolve.
    pub fn place(
        bytes: Vec<u8>,
        limit: usize,
        hash_blob: impl Fn(&[u8]) -> BlobHash,
    ) -> (Payload, Option<Vec<u8>>) {
        if bytes.len() <= limit {
            return (Payload::Inline(bytes), None);
        }
        let hash = hash_blob(&bytes);
        (Payload::Spilled(hash), Some(bytes))
    }

    pub fn is_spilled(&self) -> bool {
        matches!(self, Payload::Spilled(_))
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            Payload::Inline(bytes) => PAYLOAD_INLINE_OVERHEAD + bytes.len(),
            Payload::Spilled(_) => 1 + 32,
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Payload::Inline(bytes) => {
                debug_assert!(
                    bytes.len() <= u16::MAX as usize,
                    "inline payload over 64 KiB"
                );
                out.push(PAYLOAD_INLINE);
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                out.extend_from_slice(bytes);
            }
            Payload::Spilled(hash) => {
                out.push(PAYLOAD_SPILLED);
                out.extend_from_slice(&hash.0);
            }
        }
    }

    /// Length-prefixed even when the payload is the whole value, so
    /// there is one payload encoding rather than a standalone one and an
    /// embedded one that could drift apart.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out
    }

    fn decode_prefix(buf: &[u8]) -> Result<(Payload, &[u8]), RecordError> {
        let tag = *buf.first().ok_or(RecordError::Truncated("payload tag"))?;
        match tag {
            PAYLOAD_INLINE => {
                if buf.len() < PAYLOAD_INLINE_OVERHEAD {
                    return Err(RecordError::Truncated("payload length"));
                }
                let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
                let end = PAYLOAD_INLINE_OVERHEAD + len;
                if buf.len() < end {
                    return Err(RecordError::Truncated("payload body"));
                }
                Ok((
                    Payload::Inline(buf[PAYLOAD_INLINE_OVERHEAD..end].to_vec()),
                    &buf[end..],
                ))
            }
            PAYLOAD_SPILLED => {
                if buf.len() < 33 {
                    return Err(RecordError::Truncated("payload hash"));
                }
                let hash: [u8; 32] = buf[1..33].try_into().expect("32 bytes");
                Ok((Payload::Spilled(BlobHash(hash)), &buf[33..]))
            }
            other => Err(RecordError::UnknownPayloadTag(other)),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Payload, RecordError> {
        let (payload, rest) = Payload::decode_prefix(buf)?;
        if !rest.is_empty() {
            return Err(RecordError::TrailingBytes(rest.len()));
        }
        Ok(payload)
    }
}

/// A `0x03` xattr value, or a `0x30` record body: inline, or a blob hash
/// above [`VALUE_SPILL`]. Linux caps one xattr at 64 KiB, so with this
/// rule that ceiling is bounded by the blob store rather than by node
/// size.
pub fn place_value(
    bytes: Vec<u8>,
    hash_blob: impl Fn(&[u8]) -> BlobHash,
) -> (Payload, Option<Vec<u8>>) {
    Payload::place(bytes, VALUE_SPILL, hash_blob)
}

/// The `0x04` value: empty. A `const` rather than a literal at each call
/// site so the rule is stated once.
pub const RDENTRY_VALUE: &[u8] = &[];

/// Reject a non-empty reverse-dentry value instead of ignoring it: it
/// would mean some writer believed the `0x04` range carried state.
pub fn decode_rdentry_value(buf: &[u8]) -> Result<(), RecordError> {
    if buf.is_empty() {
        Ok(())
    } else {
        Err(RecordError::NonEmptyRDentryValue(buf.len()))
    }
}

// ---------------------------------------------------------------- xattrs

/// Where an inode's xattr set lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum XattrPlacement {
    /// In the `0x01` record. No `0x03` keys exist for this inode.
    Inline,
    /// One `0x03` key per name, and none in the record.
    Spilled,
}

/// Encoded size of an inline xattr section: count, then per name a
/// length-prefixed name and a length-prefixed value.
pub fn xattr_section_len(xattrs: &[(Vec<u8>, Vec<u8>)]) -> usize {
    2 + xattrs
        .iter()
        .map(|(name, value)| 4 + name.len() + value.len())
        .sum::<usize>()
}

/// §P6's whole-set rule.
pub fn place_xattrs(xattrs: &[(Vec<u8>, Vec<u8>)]) -> XattrPlacement {
    if xattrs.is_empty() || xattr_section_len(xattrs) <= XATTR_INLINE {
        XattrPlacement::Inline
    } else {
        XattrPlacement::Spilled
    }
}

// ----------------------------------------------------------- 0x30 records

/// Version byte leading every `0x30` value.
pub const SUBSYSTEM_RECORD_VERSION: u8 = 1;

/// A `0x30` value: [`SUBSYSTEM_RECORD_VERSION`], then each field as a
/// u32-LE length and its bytes.
///
/// Deliberately untyped. Each subsystem's field list is owned by the
/// code that reads and writes that subsystem, which is above this crate
/// (snapshots and quota are daemon concepts), and the records are tiny
/// and rare — a few per filesystem, not per inode — so a self-delimiting
/// field list costs nothing and keeps adding a field to one subsystem a
/// local change. Canonicality still holds: the bytes are a function of
/// the field values alone.
pub fn encode_fields(fields: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + fields.iter().map(|field| 4 + field.len()).sum::<usize>());
    out.push(SUBSYSTEM_RECORD_VERSION);
    for field in fields {
        out.extend_from_slice(&(field.len() as u32).to_le_bytes());
        out.extend_from_slice(field);
    }
    out
}

/// Inverse of [`encode_fields`].
pub fn decode_fields(buf: &[u8]) -> Result<Vec<&[u8]>, RecordError> {
    let (&version, mut rest) = buf.split_first().ok_or(RecordError::Truncated("version"))?;
    if version != SUBSYSTEM_RECORD_VERSION {
        return Err(RecordError::UnknownRecordVersion(version));
    }
    let mut fields = Vec::new();
    while !rest.is_empty() {
        if rest.len() < 4 {
            return Err(RecordError::Truncated("field length"));
        }
        let len = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        let body = rest
            .get(4..4 + len)
            .ok_or(RecordError::Truncated("field"))?;
        fields.push(body);
        rest = &rest[4 + len..];
    }
    Ok(fields)
}

// ----------------------------------------------------------- 0x01 record

/// The `0x01` value: the authoritative record for one inode.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InodeRecord {
    pub attrs: Attrs,
    /// A file's chunk list — `fs-core`'s encoded manifest, carried
    /// opaquely so this crate needs no manifest decoder — or the hash of
    /// the blob holding it.
    pub manifest: Option<Payload>,
    /// A symlink's target. Spillable because `PATH_MAX` is 4 KiB, which
    /// is four times [`VALUE_SPILL`]; in practice targets are short and
    /// stay inline.
    pub symlink_target: Option<Payload>,
    /// The whole xattr set, present only when [`place_xattrs`] said
    /// [`XattrPlacement::Inline`]. Sorted by name by
    /// [`plan_inode`], so the record's bytes are a function of the set
    /// and not of the caller's iteration order — two replicas that
    /// disagreed here would disagree on the root hash.
    pub xattrs: Vec<(Vec<u8>, Vec<u8>)>,
    /// Whether this inode's xattr set lives in `0x03` instead
    /// (`XattrPlacement::Spilled`). Plan 29 M3a: an *empty* `xattrs`
    /// above is ambiguous on its own — it is the overwhelmingly common
    /// "no xattrs at all" case (§P6's census) but is also what a
    /// spilled set looks like at this level, since the entries then
    /// live under `0x03` and not here. Without this bit a reader has no
    /// way to tell the two apart except by probing `0x03` — cheap
    /// against a local KV store but a full tree descent per inode
    /// during bootstrap, which measured as ~900 ms of a 1.4 s, 100k-inode
    /// bootstrap (`mtree_read::load_tree`) before this field existed.
    pub xattrs_spilled: bool,
}

const FLAG_MANIFEST: u8 = 1 << 0;
const FLAG_SYMLINK: u8 = 1 << 1;
const FLAG_XATTRS: u8 = 1 << 2;
const FLAG_XATTRS_SPILLED: u8 = 1 << 3;

impl InodeRecord {
    pub fn new(attrs: Attrs) -> InodeRecord {
        InodeRecord {
            attrs,
            ..InodeRecord::default()
        }
    }

    pub fn encoded_len(&self) -> usize {
        ATTRS_LEN
            + 1
            + self.manifest.as_ref().map_or(0, Payload::encoded_len)
            + self.symlink_target.as_ref().map_or(0, Payload::encoded_len)
            + if self.xattrs.is_empty() {
                0
            } else {
                xattr_section_len(&self.xattrs)
            }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.attrs.encode_into(&mut out);
        let mut flags = 0u8;
        if self.manifest.is_some() {
            flags |= FLAG_MANIFEST;
        }
        if self.symlink_target.is_some() {
            flags |= FLAG_SYMLINK;
        }
        if !self.xattrs.is_empty() {
            flags |= FLAG_XATTRS;
        }
        if self.xattrs_spilled {
            flags |= FLAG_XATTRS_SPILLED;
        }
        out.push(flags);
        if let Some(manifest) = &self.manifest {
            manifest.encode_into(&mut out);
        }
        if let Some(target) = &self.symlink_target {
            target.encode_into(&mut out);
        }
        if !self.xattrs.is_empty() {
            debug_assert!(
                xattr_section_len(&self.xattrs) <= XATTR_INLINE,
                "inline xattr set over the §P6 budget; use plan_inode"
            );
            out.extend_from_slice(&(self.xattrs.len() as u16).to_le_bytes());
            for (name, value) in &self.xattrs {
                out.extend_from_slice(&(name.len() as u16).to_le_bytes());
                out.extend_from_slice(name);
                out.extend_from_slice(&(value.len() as u16).to_le_bytes());
                out.extend_from_slice(value);
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> Result<InodeRecord, RecordError> {
        let (attrs, rest) = Attrs::decode_prefix(buf)?;
        let flags = *rest.first().ok_or(RecordError::Truncated("flags"))?;
        let mut rest = &rest[1..];
        let mut manifest = None;
        if flags & FLAG_MANIFEST != 0 {
            let (payload, tail) = Payload::decode_prefix(rest)?;
            manifest = Some(payload);
            rest = tail;
        }
        let mut symlink_target = None;
        if flags & FLAG_SYMLINK != 0 {
            let (payload, tail) = Payload::decode_prefix(rest)?;
            symlink_target = Some(payload);
            rest = tail;
        }
        let mut xattrs = Vec::new();
        if flags & FLAG_XATTRS != 0 {
            if rest.len() < 2 {
                return Err(RecordError::Truncated("xattr count"));
            }
            let count = u16::from_le_bytes([rest[0], rest[1]]) as usize;
            rest = &rest[2..];
            xattrs.reserve(count);
            for _ in 0..count {
                let (name, tail) = take_u16_prefixed(rest, "xattr name")?;
                let (value, tail) = take_u16_prefixed(tail, "xattr value")?;
                xattrs.push((name.to_vec(), value.to_vec()));
                rest = tail;
            }
        }
        if !rest.is_empty() {
            return Err(RecordError::TrailingBytes(rest.len()));
        }
        Ok(InodeRecord {
            attrs,
            manifest,
            symlink_target,
            xattrs,
            xattrs_spilled: flags & FLAG_XATTRS_SPILLED != 0,
        })
    }
}

fn take_u16_prefixed<'a>(
    buf: &'a [u8],
    what: &'static str,
) -> Result<(&'a [u8], &'a [u8]), RecordError> {
    if buf.len() < 2 {
        return Err(RecordError::Truncated(what));
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    let end = 2 + len;
    if buf.len() < end {
        return Err(RecordError::Truncated(what));
    }
    Ok((&buf[2..end], &buf[end..]))
}

/// A planned `0x01` value: the record, where its xattrs ended up, and
/// the blob bodies the caller must store before the commit that names
/// them (§S4's ordering invariant).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodePlan {
    pub record: InodeRecord,
    pub xattrs: XattrPlacement,
    pub blobs: Vec<Vec<u8>>,
}

/// Build a `0x01` value that respects both §P6 budgets.
///
/// The spill order is part of the format, not a heuristic, because two
/// writers that spilled different fields would produce different bytes
/// for the same filesystem and therefore different root hashes. It is:
/// the xattr set by [`place_xattrs`], then — while the record is still
/// over [`VALUE_SPILL`] — the manifest, then the symlink target.
/// Manifest first because it is the field that actually grows (a large
/// file's chunk list) while a target is bounded by `PATH_MAX`, and
/// because `fs-core` already spills chunk lists, so a spilled manifest
/// is the case every reader handles anyway.
pub fn plan_inode(
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
    xattrs: &[(Vec<u8>, Vec<u8>)],
    hash_blob: impl Fn(&[u8]) -> BlobHash,
) -> InodePlan {
    let placement = place_xattrs(xattrs);
    let mut sorted = match placement {
        XattrPlacement::Inline => xattrs.to_vec(),
        XattrPlacement::Spilled => Vec::new(),
    };
    sorted.sort();
    let mut record = InodeRecord {
        attrs,
        manifest: manifest.map(Payload::Inline),
        symlink_target: symlink_target.map(Payload::Inline),
        xattrs: sorted,
        xattrs_spilled: placement == XattrPlacement::Spilled,
    };
    let mut blobs = Vec::new();
    for field in [FLAG_MANIFEST, FLAG_SYMLINK] {
        if record.encoded_len() <= VALUE_SPILL {
            break;
        }
        let slot = if field == FLAG_MANIFEST {
            &mut record.manifest
        } else {
            &mut record.symlink_target
        };
        if let Some(Payload::Inline(bytes)) = slot.take() {
            let (payload, blob) = Payload::place(bytes, 0, &hash_blob);
            *slot = Some(payload);
            blobs.extend(blob);
        }
    }
    InodePlan {
        record,
        xattrs: placement,
        blobs,
    }
}

// ----------------------------------------------------------- 0x02 record

/// The `0x02` value: the target ino plus S1's attr copy.
///
/// `kind` is inside [`Attrs`], so `readdir` (which needs only ino and
/// kind) and `readdirplus` (which needs everything) read the same bytes
/// and there is no second shape to keep in step.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DentryRecord {
    pub ino: u64,
    /// A copy, for read locality only. The `0x01` record is
    /// authoritative; this one exists so that a cold `ls -la` is a
    /// sequential scan (§14.10). Both are written in one commit under
    /// one root hash, so unlike plan 27's SQLite-vs-tree pair there is
    /// no reconciliation path along which they could drift — and
    /// [`leaf_agg`] must never count it.
    pub attrs: Attrs,
}

/// ino(8) + attrs.
pub const DENTRY_LEN: usize = 8 + ATTRS_LEN;

impl DentryRecord {
    pub fn new(ino: u64, attrs: Attrs) -> DentryRecord {
        DentryRecord { ino, attrs }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(DENTRY_LEN);
        out.extend_from_slice(&self.ino.to_le_bytes());
        self.attrs.encode_into(&mut out);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<DentryRecord, RecordError> {
        if buf.len() < 8 {
            return Err(RecordError::Truncated("dentry ino"));
        }
        let ino = u64::from_le_bytes(buf[0..8].try_into().expect("8 bytes"));
        Ok(DentryRecord {
            ino,
            attrs: Attrs::decode(&buf[8..])?,
        })
    }

    /// ino and kind alone, for plain `readdir`, without decoding the
    /// rest of the copy.
    pub fn ino_and_kind(buf: &[u8]) -> Result<(u64, Kind), RecordError> {
        if buf.len() < 9 {
            return Err(RecordError::Truncated("dentry ino and kind"));
        }
        Ok((
            u64::from_le_bytes(buf[0..8].try_into().expect("8 bytes")),
            Kind::from_u8(buf[8])?,
        ))
    }
}

// -------------------------------------------------------- the projection

/// The §P6 answer to S2's caller-supplied leaf→[`Agg`] projection
/// ([`crate::Config::leaf_agg`]).
///
/// S2 made this a function pointer instead of baking it into the tree
/// because only this codec knows which key range is *authoritative*, and
/// the question is not cosmetic: §P6 stores a file's attrs twice, once
/// in the `0x01` record and once in every `0x02` dentry that names it.
/// Counting both would inflate `du`, `statfs`, quota admission and pin
/// admission by one whole copy per link — and because the aggregate is
/// covered by the root hash, it would be inflated *consistently* on
/// every replica, which is the kind of wrong answer nothing ever
/// notices. So exactly one range counts:
///
/// - `0x01` contributes a file's `size` and its `mtime`, and one to
///   `files` when [`Kind::holds_data`].
/// - `0x02`, `0x03`, `0x04` and `0x30` contribute nothing. The dentry
///   copy exists for read locality; the reverse index is a set; xattr
///   values and subsystem records are not file bytes.
///
/// Total, and quiet about malformed input: the signature has nowhere to
/// put an error, and a leaf value that does not decode must not panic
/// the process that read it. An unreadable record contributes nothing,
/// which shows up as a `du` that is too *small* — detectable against the
/// `keys` count the tree fills in itself — rather than as a crash on the
/// `statfs` path.
pub fn leaf_agg(key: &[u8], value: &[u8]) -> Agg {
    if !matches!(Key::parse(key), Ok(Key::Inode { .. })) {
        return Agg::EMPTY;
    }
    let Ok((attrs, _)) = Attrs::decode_prefix(value) else {
        return Agg::EMPTY;
    };
    Agg {
        bytes: if attrs.kind.holds_data() {
            attrs.size
        } else {
            0
        },
        files: u64::from(attrs.kind.holds_data()),
        keys: 0,
        max_mtime: attrs.mtime_ns,
    }
}

/// The [`crate::Config`] S5 and S6 build trees with: the §P6 projection,
/// and otherwise §14's measured defaults.
pub fn config() -> crate::Config {
    crate::Config::default().with_leaf_agg(leaf_agg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys;

    fn blob_hash(bytes: &[u8]) -> BlobHash {
        BlobHash(*blake3::hash(bytes).as_bytes())
    }

    fn attrs() -> Attrs {
        Attrs {
            kind: Kind::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            size: 1 << 33,
            mtime_ns: -5,
            ctime_ns: 1_700_000_000_000_000_000,
            rdev: Rdev::default(),
        }
    }

    /// The guard on §P6's atime rule: nine fields, 49 bytes. A tenth
    /// field — and `atime_ns` is the one that would be proposed — moves
    /// this number and fails here.
    #[test]
    fn the_attr_encoding_is_pinned() {
        assert_eq!(ATTRS_LEN, 49);
        let encoded = Attrs {
            kind: Kind::Symlink,
            mode: 0x0102_0304,
            uid: 0,
            gid: 0,
            nlink: 1,
            size: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            rdev: Rdev::new(0x0a0b_0c0d, 0x1112_1314),
        }
        .encode();
        assert_eq!(encoded.len(), ATTRS_LEN);
        assert_eq!(&encoded[0..5], &[0x02, 0x04, 0x03, 0x02, 0x01]);
        // Plan 31 §7: rdev is the portable pair, major then minor, each
        // little-endian, in the 8 bytes that held Linux `makedev` before.
        assert_eq!(
            &encoded[41..49],
            &[0x0d, 0x0c, 0x0b, 0x0a, 0x14, 0x13, 0x12, 0x11]
        );
        assert_eq!(DENTRY_LEN, 57);
    }

    #[test]
    fn attrs_round_trip_including_negative_times() {
        for kind in KINDS {
            let a = Attrs {
                kind,
                mtime_ns: -1_234_567_890,
                ctime_ns: i64::MIN,
                size: u64::MAX,
                rdev: Rdev::new(u32::MAX, 0x0102_0304),
                ..attrs()
            };
            assert_eq!(Attrs::decode(&a.encode()).unwrap(), a);
        }
        assert_eq!(
            Attrs::decode(&[0u8; ATTRS_LEN - 1]),
            Err(RecordError::Truncated("attrs"))
        );
        let mut bad = Attrs::default().encode();
        bad[0] = 7;
        assert_eq!(Attrs::decode(&bad), Err(RecordError::UnknownKind(7)));
        let mut long = Attrs::default().encode();
        long.push(0);
        assert_eq!(Attrs::decode(&long), Err(RecordError::TrailingBytes(1)));
    }

    #[test]
    fn inode_records_round_trip_from_empty_to_full() {
        let empty = InodeRecord::new(Attrs::default());
        assert_eq!(InodeRecord::decode(&empty.encode()).unwrap(), empty);
        assert_eq!(empty.encode().len(), empty.encoded_len());

        let full = InodeRecord {
            attrs: attrs(),
            manifest: Some(Payload::Spilled(blob_hash(b"manifest"))),
            symlink_target: Some(Payload::Inline(b"../over/there".to_vec())),
            xattrs: vec![
                (b"security.selinux".to_vec(), b"unconfined_u".to_vec()),
                (b"user.constellation.scratch".to_vec(), Vec::new()),
            ],
            xattrs_spilled: false,
        };
        assert_eq!(InodeRecord::decode(&full.encode()).unwrap(), full);
        assert_eq!(full.encode().len(), full.encoded_len());

        // Maximal within the budgets: a spilled manifest, a PATH_MAX
        // target, and an xattr set right at XATTR_INLINE.
        let big = InodeRecord {
            attrs: attrs(),
            manifest: Some(Payload::Spilled(blob_hash(b"m"))),
            symlink_target: Some(Payload::Spilled(blob_hash(&[b'x'; 4096]))),
            xattrs: vec![(b"user.x".to_vec(), vec![0xff; XATTR_INLINE - 2 - 4 - 6])],
            xattrs_spilled: false,
        };
        assert_eq!(xattr_section_len(&big.xattrs), XATTR_INLINE);
        assert_eq!(InodeRecord::decode(&big.encode()).unwrap(), big);
        assert!(big.encoded_len() <= VALUE_SPILL);
    }

    #[test]
    fn a_truncated_record_is_an_error_not_a_panic() {
        let full = InodeRecord {
            attrs: attrs(),
            manifest: Some(Payload::Inline(vec![1, 2, 3])),
            symlink_target: Some(Payload::Inline(b"t".to_vec())),
            xattrs: vec![(b"user.a".to_vec(), b"v".to_vec())],
            xattrs_spilled: false,
        };
        let encoded = full.encode();
        for cut in 0..encoded.len() {
            assert!(
                InodeRecord::decode(&encoded[..cut]).is_err(),
                "a {cut}-byte prefix must not decode"
            );
        }
        assert!(InodeRecord::decode(&encoded).is_ok());
        assert_eq!(
            Payload::decode(&[9, 0, 0]),
            Err(RecordError::UnknownPayloadTag(9))
        );
    }

    #[test]
    fn dentry_records_round_trip_and_expose_kind_cheaply() {
        for kind in KINDS {
            let record = DentryRecord::new(7, Attrs { kind, ..attrs() });
            let encoded = record.encode();
            assert_eq!(encoded.len(), DENTRY_LEN);
            assert_eq!(DentryRecord::decode(&encoded).unwrap(), record);
            assert_eq!(DentryRecord::ino_and_kind(&encoded).unwrap(), (7, kind));
        }
        assert!(DentryRecord::decode(&[0u8; 8]).is_err());
        assert!(DentryRecord::ino_and_kind(&[0u8; 8]).is_err());
    }

    #[test]
    fn the_reverse_index_carries_no_value() {
        assert!(RDENTRY_VALUE.is_empty());
        assert_eq!(decode_rdentry_value(RDENTRY_VALUE), Ok(()));
        assert_eq!(
            decode_rdentry_value(&[0]),
            Err(RecordError::NonEmptyRDentryValue(1))
        );
    }

    /// The [`XATTR_INLINE`] boundary, crossed in both directions.
    #[test]
    fn the_xattr_set_inlines_up_to_the_budget_and_spills_above_it() {
        let fits = vec![(b"user.a".to_vec(), vec![0u8; XATTR_INLINE - 2 - 4 - 6])];
        assert_eq!(xattr_section_len(&fits), XATTR_INLINE);
        assert_eq!(place_xattrs(&fits), XattrPlacement::Inline);

        let mut over = fits.clone();
        over[0].1.push(0);
        assert_eq!(xattr_section_len(&over), XATTR_INLINE + 1);
        assert_eq!(place_xattrs(&over), XattrPlacement::Spilled);

        // And back: removing the byte restores the inline placement, so
        // the rule is a function of the set rather than sticky state.
        over[0].1.pop();
        assert_eq!(place_xattrs(&over), XattrPlacement::Inline);
        assert_eq!(place_xattrs(&[]), XattrPlacement::Inline);

        // Many small labels also spill once the *set* crosses the
        // budget, even though each name is tiny — the rule is whole-set.
        let many: Vec<(Vec<u8>, Vec<u8>)> = (0..40)
            .map(|i| (format!("user.k{i}").into_bytes(), vec![b'v'; 4]))
            .collect();
        assert_eq!(place_xattrs(&many), XattrPlacement::Spilled);
    }

    /// The [`VALUE_SPILL`] boundary, crossed in both directions.
    #[test]
    fn a_value_spills_above_the_limit_and_not_at_it() {
        let at = vec![7u8; VALUE_SPILL];
        let (payload, blob) = place_value(at.clone(), blob_hash);
        assert_eq!(payload, Payload::Inline(at.clone()));
        assert!(blob.is_none());
        assert_eq!(Payload::decode(&payload.encode()).unwrap(), payload);

        let over = vec![7u8; VALUE_SPILL + 1];
        let (payload, blob) = place_value(over.clone(), blob_hash);
        assert_eq!(payload, Payload::Spilled(blob_hash(&over)));
        assert_eq!(blob.as_deref(), Some(over.as_slice()));
        assert_eq!(Payload::decode(&payload.encode()).unwrap(), payload);
        assert_eq!(payload.encoded_len(), 33);

        // Back below the limit and the value is inline again, named by
        // content and not by history.
        let (payload, blob) = place_value(at, blob_hash);
        assert!(!payload.is_spilled() && blob.is_none());

        // The spill hash is the caller's, because an E2E filesystem
        // addresses blobs with a keyed hash.
        let keyed = |bytes: &[u8]| BlobHash(*blake3::keyed_hash(&[3u8; 32], bytes).as_bytes());
        let (payload, _) = place_value(vec![0u8; VALUE_SPILL + 1], keyed);
        assert_eq!(payload, Payload::Spilled(keyed(&[0u8; VALUE_SPILL + 1])));
    }

    #[test]
    fn plan_inode_keeps_the_record_under_the_limit() {
        // A big file's chunk list: spilled, and the body handed back.
        let manifest = vec![0xab; 4096];
        let plan = plan_inode(attrs(), Some(manifest.clone()), None, &[], blob_hash);
        assert_eq!(
            plan.record.manifest,
            Some(Payload::Spilled(blob_hash(&manifest)))
        );
        assert_eq!(plan.blobs, vec![manifest]);
        assert_eq!(plan.xattrs, XattrPlacement::Inline);
        assert!(plan.record.encoded_len() <= VALUE_SPILL);

        // A short manifest stays inline and writes no blob.
        let plan = plan_inode(attrs(), Some(vec![1, 2, 3]), None, &[], blob_hash);
        assert_eq!(plan.record.manifest, Some(Payload::Inline(vec![1, 2, 3])));
        assert!(plan.blobs.is_empty());

        // A PATH_MAX symlink target spills too.
        let target = vec![b'x'; 4095];
        let plan = plan_inode(attrs(), None, Some(target.clone()), &[], blob_hash);
        assert_eq!(plan.blobs, vec![target]);
        assert!(plan.record.symlink_target.as_ref().unwrap().is_spilled());

        // Manifest first: a record that fits once the manifest is out
        // keeps its target inline.
        let plan = plan_inode(
            attrs(),
            Some(vec![0xcd; 1_000]),
            Some(b"../t".to_vec()),
            &[],
            blob_hash,
        );
        assert!(plan.record.manifest.as_ref().unwrap().is_spilled());
        assert_eq!(
            plan.record.symlink_target,
            Some(Payload::Inline(b"../t".to_vec()))
        );

        // Spilled xattrs leave the record empty-handed; the caller
        // writes 0x03 keys.
        let many: Vec<(Vec<u8>, Vec<u8>)> = (0..40)
            .map(|i| (format!("user.k{i}").into_bytes(), vec![b'v'; 4]))
            .collect();
        let plan = plan_inode(attrs(), None, None, &many, blob_hash);
        assert_eq!(plan.xattrs, XattrPlacement::Spilled);
        assert!(plan.record.xattrs.is_empty());
    }

    /// Plan 29 M3a: `xattrs_spilled` is what lets a reader (bootstrap's
    /// `load_tree`) tell "no xattrs at all" from "xattrs live in 0x03"
    /// without probing `0x03` — both cases leave `record.xattrs` empty,
    /// so the flag has to be the source of truth, and it must round-trip
    /// through encode/decode exactly.
    #[test]
    fn xattrs_spilled_distinguishes_no_xattrs_from_spilled_and_round_trips() {
        // No xattrs at all: never spilled, `xattrs` empty.
        let plan = plan_inode(attrs(), None, None, &[], blob_hash);
        assert!(!plan.record.xattrs_spilled);
        assert!(plan.record.xattrs.is_empty());
        let decoded = InodeRecord::decode(&plan.record.encode()).unwrap();
        assert!(!decoded.xattrs_spilled);

        // A small inline set: not spilled, `xattrs` non-empty.
        let small = vec![(b"user.a".to_vec(), b"1".to_vec())];
        let plan = plan_inode(attrs(), None, None, &small, blob_hash);
        assert!(!plan.record.xattrs_spilled);
        assert!(!plan.record.xattrs.is_empty());
        let decoded = InodeRecord::decode(&plan.record.encode()).unwrap();
        assert!(!decoded.xattrs_spilled);

        // A large set: spilled, `xattrs` empty — the ambiguous case the
        // flag exists to resolve.
        let many: Vec<(Vec<u8>, Vec<u8>)> = (0..40)
            .map(|i| (format!("user.k{i}").into_bytes(), vec![b'v'; 4]))
            .collect();
        let plan = plan_inode(attrs(), None, None, &many, blob_hash);
        assert!(plan.record.xattrs_spilled);
        assert!(plan.record.xattrs.is_empty());
        let decoded = InodeRecord::decode(&plan.record.encode()).unwrap();
        assert!(decoded.xattrs_spilled);
    }

    /// Canonicality reaches into the values: the same set of xattrs in a
    /// different order has to produce the same bytes, or two replicas
    /// disagree on the root hash for content they agree on.
    #[test]
    fn inline_xattrs_are_sorted_so_the_record_is_canonical() {
        let one = vec![
            (b"user.b".to_vec(), b"2".to_vec()),
            (b"user.a".to_vec(), b"1".to_vec()),
        ];
        let other = vec![
            (b"user.a".to_vec(), b"1".to_vec()),
            (b"user.b".to_vec(), b"2".to_vec()),
        ];
        let plan = |set: &[(Vec<u8>, Vec<u8>)]| {
            plan_inode(attrs(), None, None, set, blob_hash)
                .record
                .encode()
        };
        assert_eq!(plan(&one), plan(&other));
    }

    #[test]
    fn only_the_inode_range_contributes_to_the_aggregate() {
        let a = Attrs {
            size: 4096,
            mtime_ns: 99,
            ..attrs()
        };
        let inode = leaf_agg(&keys::inode(9), &InodeRecord::new(a).encode());
        assert_eq!(
            inode,
            Agg {
                bytes: 4096,
                files: 1,
                keys: 0,
                max_mtime: 99
            }
        );
        // The dentry holds the same attrs and must contribute nothing.
        let dentry = DentryRecord::new(9, a).encode();
        assert_eq!(leaf_agg(&keys::dentry(1, b"f"), &dentry), Agg::EMPTY);
        assert_eq!(
            leaf_agg(&keys::rdentry(9, 1, b"f"), RDENTRY_VALUE),
            Agg::EMPTY
        );
        assert_eq!(
            leaf_agg(
                &keys::xattr(9, b"user.a"),
                &Payload::Inline(vec![0; 9]).encode()
            ),
            Agg::EMPTY
        );
        assert_eq!(
            leaf_agg(
                &keys::subsystem(keys::Subsystem::Snapshot, b"s"),
                &Payload::Inline(vec![0; 9]).encode()
            ),
            Agg::EMPTY
        );

        // Directories and symlinks have no data bytes and are not files,
        // matching `meta::sqlite::recursive_size_conn`.
        for kind in [Kind::Dir, Kind::Symlink, Kind::Fifo, Kind::CharDev] {
            let agg = leaf_agg(
                &keys::inode(9),
                &InodeRecord::new(Attrs { kind, ..a }).encode(),
            );
            assert_eq!(agg.bytes, 0);
            assert_eq!(agg.files, 0);
            assert_eq!(agg.max_mtime, 99, "mtime is tracked for every inode");
        }

        // Unreadable values report nothing rather than panicking.
        assert_eq!(leaf_agg(&keys::inode(9), b"short"), Agg::EMPTY);
        assert_eq!(leaf_agg(b"", b""), Agg::EMPTY);
        assert_eq!(
            leaf_agg(&[0x11, 1, 2], &InodeRecord::new(a).encode()),
            Agg::EMPTY
        );
    }

    #[test]
    fn the_shipped_config_carries_the_projection() {
        let config = config();
        assert!(config.validate().is_ok());
        assert_eq!(
            (config.leaf_agg)(&keys::inode(1), &InodeRecord::new(attrs()).encode()).files,
            1
        );
    }

    #[test]
    fn subsystem_fields_round_trip_and_refuse_garbage() {
        let encoded = encode_fields(&[b"/a", b"", &7u64.to_le_bytes()]);
        let fields = decode_fields(&encoded).unwrap();
        assert_eq!(fields, vec![&b"/a"[..], &b""[..], &7u64.to_le_bytes()[..]]);
        assert_eq!(decode_fields(&encode_fields(&[])).unwrap().len(), 0);
        assert!(decode_fields(&[]).is_err());
        assert!(decode_fields(&[2]).is_err());
        assert!(decode_fields(&encoded[..encoded.len() - 1]).is_err());
    }
}
