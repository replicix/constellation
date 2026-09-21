//! The plan 28 §P6 key encoding: which byte string names which
//! filesystem entity, and why byte order is the only order that matters.
//!
//! §P6 calls its table "the most load-bearing table in this plan", and
//! the reason is that everything else in the design is negotiable later.
//! The engine can be swapped, the pack size retuned, the compactor
//! rewritten; the key encoding is on-bucket format, ADR-5 promises no
//! migration, and getting it wrong is the one mistake in plan 28 that
//! would cost a bucket migration. So this module is small, explicit, and
//! heavily tested, and it is the *only* place in the repo that decides
//! what a tree key means.
//!
//! | range  | key                       | value                                        |
//! |--------|---------------------------|----------------------------------------------|
//! | `0x01` | `ino`                     | the authoritative inode record               |
//! | `0x02` | `parent_ino \| name`      | `(ino, kind)` **plus** the READDIRPLUS attrs |
//! | `0x03` | `ino \| xattr_name`       | one spilled-out xattr value                  |
//! | `0x04` | `ino \| parent_ino \| name` | `()` — the reverse dentry index             |
//! | `0x30` | `subsystem \| id`         | snapshots, clones, quota, designations, holds |
//!
//! `0x10`–`0x2f` are reserved and deliberately unused; see
//! [`RESERVED_RANGES`].
//!
//! ## Big-endian, and why it is not a style choice
//!
//! Every integer inside a key is big-endian, so lexicographic byte order
//! *is* numeric order and the tree's single ordering — which is over raw
//! bytes, because [`crate::node`] has no idea what a key means — answers
//! every question the filesystem asks:
//!
//! - `readdir(ino)` is a contiguous range scan over `0x02 | ino`, which
//!   is what gives §14.2's one-pack-per-directory property and makes a
//!   cold `ls -la` of a thousand files one ranged GET. A little-endian
//!   `parent_ino` would scatter a directory's dentries over the whole
//!   range and there would be no scan to do.
//! - the `readdir` cursor *is* a key, so §12's stable, exact readdir
//!   offsets are free rather than a side table.
//! - `listxattr` is the same scan over `0x03 | ino`.
//! - the ancestor walk `rename` needs for its cycle check is a point
//!   read per level over `0x04`.
//!
//! Fixed-width id fields matter for the same reason. `parent_ino` is
//! always eight bytes, so a name can never be mistaken for part of an
//! id, one directory's range can never overlap the next one's, and a key
//! can never be an ambiguous prefix of a key belonging to a different
//! entity. Names, by contrast, are variable-length and go **last** in
//! every key that has one — a name is the only field allowed to contain
//! arbitrary bytes (`0x00`, `0x2f`, `0xff`), and putting anything after
//! it would make the boundary between fields undecidable.
//!
//! ## Nothing mutable lives here
//!
//! §P6's *indexes stay out of the tree* rule: a key may be built only
//! from fields that are immutable for the lifetime of the entity it
//! describes. An index over a mutable field needs its delete keyed by
//! the field's **old** value, old values are scattered across the index
//! range, and §14.4 measured that shape at 758×–2,460× today's log
//! bytes. An earlier draft of the plan put `0x10 | name | ino`,
//! `0x11 | mtime_be | ino` and `0x20 | chunk_hash | ino` in the tree and
//! §P5 retracted them; `0x10`–`0x2f` is where they used to live and it
//! stays empty so that a future index over a *provably* immutable field
//! has somewhere to go.
//!
//! atime is the same rule applied to the one field that would otherwise
//! sneak in through the value side: with cluster-visible atime, a `find`
//! over 100M files is 100M scattered inode writes driven purely by
//! *reads*. §P6 closes the question by exclusion, so atime is node-local
//! and best-effort and appears nowhere in this module or in
//! [`crate::record`] — not in a key, and not in the attrs either.
//! [`Field`] states that mechanically instead of in a comment, and
//! `tests/keys.rs` is what makes an accidental reintroduction fail.

/// The authoritative inode record, keyed by `ino` — a point lookup,
/// which is how FUSE addresses everything.
pub const RANGE_INODE: u8 = 0x01;
/// Dentries, keyed by `parent_ino | name`.
pub const RANGE_DENTRY: u8 = 0x02;
/// Xattrs that did not fit in the inode record, keyed by
/// `ino | xattr_name`.
pub const RANGE_XATTR: u8 = 0x03;
/// The reverse dentry index, keyed by `ino | parent_ino | name`. Replaces
/// today's `dentry_by_ino`: hard links, path reconstruction, and
/// `rename`'s cycle check.
pub const RANGE_RDENTRY: u8 = 0x04;
/// Subsystem records, keyed by `subsystem | id`.
pub const RANGE_SUBSYSTEM: u8 = 0x30;

/// Reserved, and deliberately unused (ADR-11's reservation habit).
///
/// §P5 retracted the in-tree secondary indexes that used to occupy this
/// span. A future index may claim one of these bytes **only** if its key
/// is provably immutable for the lifetime of what it indexes; nothing
/// else may. `nothing_encodes_into_the_reserved_span` in `tests/keys.rs`
/// asserts the span is empty, and [`Key::parse`] refuses it by name
/// rather than by falling through to "unknown", so a stray byte reads as
/// the design decision it violates.
pub const RESERVED_RANGES: std::ops::RangeInclusive<u8> = 0x10..=0x2f;

/// Every range byte this codec emits, in order. The tests iterate it, so
/// a new range cannot be added without being round-tripped, ordered, and
/// checked against the immutability rule.
pub const RANGES: [u8; 5] = [
    RANGE_INODE,
    RANGE_DENTRY,
    RANGE_XATTR,
    RANGE_RDENTRY,
    RANGE_SUBSYSTEM,
];

/// Width of an `ino` inside a key. Not `size_of::<u64>()` spelled
/// differently: it is a format constant, and a key layout that changed
/// with the host's word size would not be a format at all.
pub const INO_LEN: usize = 8;

/// POSIX `NAME_MAX`. Recorded rather than enforced — the codec treats a
/// name as opaque bytes, because rejecting a name is the FUSE boundary's
/// job and a metadata store that cannot *represent* what the kernel
/// already accepted is a worse failure than storing it. The hard limit
/// the format imposes is [`crate::node`]'s `u16` key length.
pub const NAME_MAX: usize = 255;

/// Which subsystem a `0x30` record belongs to.
///
/// One byte between the range and the id, so each subsystem's records
/// are a contiguous scannable run ("list every snapshot" is a range
/// scan) and adding a subsystem cannot perturb another's keys. The
/// discriminants are format, not implementation detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Subsystem {
    Snapshot = 0x01,
    Clone = 0x02,
    Quota = 0x03,
    Designation = 0x04,
    Hold = 0x05,
}

/// Every subsystem, in key order.
pub const SUBSYSTEMS: [Subsystem; 5] = [
    Subsystem::Snapshot,
    Subsystem::Clone,
    Subsystem::Quota,
    Subsystem::Designation,
    Subsystem::Hold,
];

impl Subsystem {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(byte: u8) -> Option<Subsystem> {
        SUBSYSTEMS.into_iter().find(|s| s.as_u8() == byte)
    }
}

/// Why a key decode failed.
///
/// Separate from [`crate::MtreeError`] on purpose: that type is about
/// node *structure* arriving off a network, this one is about a byte
/// string that is not a §P6 key. A caller that confuses the two would
/// report bucket corruption for what is actually a codec-version
/// mismatch, or the reverse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    #[error("a key cannot be empty")]
    Empty,

    /// Not one of [`RANGES`], and not reserved either.
    #[error("key range 0x{0:02x} is not part of the §P6 encoding")]
    UnknownRange(u8),

    /// §P5's retracted indexes lived here. Refused explicitly so the
    /// error names the rule rather than shrugging.
    #[error("key range 0x{0:02x} is reserved and deliberately unused (§P6)")]
    ReservedRange(u8),

    #[error("a 0x{range:02x} key needs at least {want} bytes, got {got}")]
    Truncated { range: u8, want: usize, got: usize },

    /// A fixed-width key with bytes after its last field. Refused rather
    /// than ignored: the extra bytes mean the writer believed in a field
    /// this codec does not have, and a reader that skipped them would
    /// treat two different keys as the same entity.
    #[error("a 0x{range:02x} key is {got} bytes, expected exactly {want}")]
    Oversized { range: u8, want: usize, got: usize },

    /// A dentry, reverse-dentry or xattr key with no name. The empty
    /// name is the *prefix* of a range scan (see [`dentries_of`]) and
    /// storing it as a key would make that scan's lower bound ambiguous.
    #[error("a 0x{0:02x} key carries an empty name")]
    EmptyName(u8),

    #[error("unknown 0x30 subsystem 0x{0:02x}")]
    UnknownSubsystem(u8),
}

// ------------------------------------------------------------ encoding

fn with_ino(range: u8, ino: u64, tail: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + INO_LEN + tail.len());
    key.push(range);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(tail);
    key
}

/// `0x01 | ino`.
pub fn inode(ino: u64) -> Vec<u8> {
    with_ino(RANGE_INODE, ino, &[])
}

/// `0x02 | parent_ino | name`.
pub fn dentry(parent_ino: u64, name: &[u8]) -> Vec<u8> {
    debug_assert!(!name.is_empty(), "a dentry name cannot be empty");
    with_ino(RANGE_DENTRY, parent_ino, name)
}

/// `0x03 | ino | xattr_name`, for xattrs that exceeded the inode
/// record's inline budget (`crate::record::XATTR_INLINE`).
pub fn xattr(ino: u64, name: &[u8]) -> Vec<u8> {
    debug_assert!(!name.is_empty(), "an xattr name cannot be empty");
    with_ino(RANGE_XATTR, ino, name)
}

/// `0x04 | ino | parent_ino | name`.
///
/// The `ino` comes first because every question this index answers —
/// "which names point at this inode", "what is this inode's path", "is
/// the rename target a descendant of its source" — starts from an ino.
pub fn rdentry(ino: u64, parent_ino: u64, name: &[u8]) -> Vec<u8> {
    debug_assert!(!name.is_empty(), "a dentry name cannot be empty");
    let mut key = Vec::with_capacity(1 + 2 * INO_LEN + name.len());
    key.push(RANGE_RDENTRY);
    key.extend_from_slice(&ino.to_be_bytes());
    key.extend_from_slice(&parent_ino.to_be_bytes());
    key.extend_from_slice(name);
    key
}

/// `0x30 | subsystem | id`. An empty `id` is legal: a subsystem with one
/// global record (a filesystem-wide quota, say) needs no id to
/// distinguish it from its siblings.
pub fn subsystem(subsystem: Subsystem, id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + id.len());
    key.push(RANGE_SUBSYSTEM);
    key.push(subsystem.as_u8());
    key.extend_from_slice(id);
    key
}

// -------------------------------------------------------------- ranges

/// A half-open key range, which is what a scan actually needs.
///
/// [`KeyRange::end`] is exclusive and is derived from the prefix by
/// incrementing it, so a bounded scan provably cannot run into the next
/// entity's keys — the property that makes `readdir` of one directory
/// independent of how many directories follow it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRange {
    prefix: Vec<u8>,
    end: Vec<u8>,
}

impl KeyRange {
    /// Every key beginning with `prefix`.
    pub fn of_prefix(prefix: Vec<u8>) -> KeyRange {
        let end = successor(&prefix);
        KeyRange { prefix, end }
    }

    /// Inclusive lower bound, and the prefix every key in the range
    /// shares — [`crate::Tree::range`] takes it as both.
    pub fn start(&self) -> &[u8] {
        &self.prefix
    }

    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Exclusive upper bound. Empty only for the all-`0xff` prefix,
    /// which no §P6 range produces, and which an empty bound correctly
    /// reads as "to the end of the keyspace".
    pub fn end(&self) -> &[u8] {
        &self.end
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
    }
}

/// The shortest byte string strictly greater than every string with
/// `prefix` as a prefix: drop trailing `0xff`s, bump the last byte.
///
/// Returns empty when `prefix` is all `0xff` — there is no such string
/// then, and "no upper bound" is the right answer rather than a wrapped
/// one. `0x02 | u64::MAX` bumps to `0x03`, i.e. the start of the next
/// range, which is exactly the bound that directory's scan wants.
fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xff {
            end.push(last + 1);
            return end;
        }
    }
    Vec::new()
}

/// The whole of one range — `statfs`-style scans, and `fsck`.
pub fn whole_range(range: u8) -> KeyRange {
    KeyRange::of_prefix(vec![range])
}

/// One directory's dentries: `readdir`, `readdirplus`, and `rmdir`'s
/// emptiness probe. Contiguous by construction, which is what lands them
/// in one pack (§14.2).
pub fn dentries_of(parent_ino: u64) -> KeyRange {
    KeyRange::of_prefix(with_ino(RANGE_DENTRY, parent_ino, &[]))
}

/// One inode's spilled xattrs: `listxattr`.
pub fn xattrs_of(ino: u64) -> KeyRange {
    KeyRange::of_prefix(with_ino(RANGE_XATTR, ino, &[]))
}

/// Every name that points at `ino` — one entry unless the inode is hard
/// linked.
pub fn names_of(ino: u64) -> KeyRange {
    KeyRange::of_prefix(with_ino(RANGE_RDENTRY, ino, &[]))
}

/// Every name `parent_ino` gives to `ino`. Narrower than
/// [`names_of`], and what `unlink` needs to delete exactly one link.
pub fn names_of_in(ino: u64, parent_ino: u64) -> KeyRange {
    let mut prefix = with_ino(RANGE_RDENTRY, ino, &[]);
    prefix.extend_from_slice(&parent_ino.to_be_bytes());
    KeyRange::of_prefix(prefix)
}

/// Every record of one subsystem.
pub fn records_of(subsystem: Subsystem) -> KeyRange {
    KeyRange::of_prefix(vec![RANGE_SUBSYSTEM, subsystem.as_u8()])
}

// -------------------------------------------------------------- decode

/// A decoded key. Borrows the name from the caller's bytes, because the
/// hot path for this is a `readdir` scan decoding one key per dirent
/// straight out of a leaf node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key<'a> {
    Inode {
        ino: u64,
    },
    Dentry {
        parent_ino: u64,
        name: &'a [u8],
    },
    Xattr {
        ino: u64,
        name: &'a [u8],
    },
    RDentry {
        ino: u64,
        parent_ino: u64,
        name: &'a [u8],
    },
    Subsystem {
        subsystem: Subsystem,
        id: &'a [u8],
    },
}

fn be_u64(buf: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(buf[at..at + INO_LEN].try_into().expect("8 bytes"))
}

fn need(range: u8, buf: &[u8], want: usize) -> Result<(), KeyError> {
    if buf.len() < want {
        return Err(KeyError::Truncated {
            range,
            want,
            got: buf.len(),
        });
    }
    Ok(())
}

impl<'a> Key<'a> {
    /// Decode a key, or say precisely why it is not one.
    ///
    /// Total and non-panicking: keys come out of leaf nodes that came
    /// off a network, so "this is not a key" has to be a value a caller
    /// can handle, exactly as [`crate::NodeRef`] treats node bytes.
    pub fn parse(key: &'a [u8]) -> Result<Key<'a>, KeyError> {
        let range = *key.first().ok_or(KeyError::Empty)?;
        match range {
            RANGE_INODE => {
                need(range, key, 1 + INO_LEN)?;
                if key.len() > 1 + INO_LEN {
                    return Err(KeyError::Oversized {
                        range,
                        want: 1 + INO_LEN,
                        got: key.len(),
                    });
                }
                Ok(Key::Inode {
                    ino: be_u64(key, 1),
                })
            }
            RANGE_DENTRY => {
                need(range, key, 1 + INO_LEN + 1).map_err(|_| truncated_or_nameless(range, key))?;
                Ok(Key::Dentry {
                    parent_ino: be_u64(key, 1),
                    name: &key[1 + INO_LEN..],
                })
            }
            RANGE_XATTR => {
                need(range, key, 1 + INO_LEN + 1).map_err(|_| truncated_or_nameless(range, key))?;
                Ok(Key::Xattr {
                    ino: be_u64(key, 1),
                    name: &key[1 + INO_LEN..],
                })
            }
            RANGE_RDENTRY => {
                need(range, key, 1 + 2 * INO_LEN + 1)
                    .map_err(|_| truncated_or_nameless(range, key))?;
                Ok(Key::RDentry {
                    ino: be_u64(key, 1),
                    parent_ino: be_u64(key, 1 + INO_LEN),
                    name: &key[1 + 2 * INO_LEN..],
                })
            }
            RANGE_SUBSYSTEM => {
                need(range, key, 2)?;
                let byte = key[1];
                Ok(Key::Subsystem {
                    subsystem: Subsystem::from_u8(byte).ok_or(KeyError::UnknownSubsystem(byte))?,
                    id: &key[2..],
                })
            }
            reserved if RESERVED_RANGES.contains(&reserved) => {
                Err(KeyError::ReservedRange(reserved))
            }
            other => Err(KeyError::UnknownRange(other)),
        }
    }

    /// The range byte this key lives in.
    pub fn range(&self) -> u8 {
        match self {
            Key::Inode { .. } => RANGE_INODE,
            Key::Dentry { .. } => RANGE_DENTRY,
            Key::Xattr { .. } => RANGE_XATTR,
            Key::RDentry { .. } => RANGE_RDENTRY,
            Key::Subsystem { .. } => RANGE_SUBSYSTEM,
        }
    }

    /// Re-encode. `Key::parse(&k)?.encode() == k` for every key this
    /// codec produces, which is the round-trip the tests assert.
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            Key::Inode { ino } => inode(ino),
            Key::Dentry { parent_ino, name } => dentry(parent_ino, name),
            Key::Xattr { ino, name } => xattr(ino, name),
            Key::RDentry {
                ino,
                parent_ino,
                name,
            } => rdentry(ino, parent_ino, name),
            Key::Subsystem { subsystem, id } => self::subsystem(subsystem, id),
        }
    }

    /// The fields this key is built from, in key order.
    ///
    /// The point of stating this as data rather than as prose is §12's
    /// mechanical *no mutable field is in the tree* test: it walks every
    /// variant, asks for the fields, and asserts every one of them is
    /// [`Field::immutable`]. A codec change that mixed a mutable field
    /// into a key would have to lie here to pass, and lying here is
    /// visible in review in a way that a new `to_be_bytes` call in a key
    /// builder is not.
    pub fn fields(&self) -> &'static [Field] {
        match self {
            Key::Inode { .. } => &[Field::Range, Field::Ino],
            Key::Dentry { .. } => &[Field::Range, Field::ParentIno, Field::Name],
            Key::Xattr { .. } => &[Field::Range, Field::Ino, Field::XattrName],
            Key::RDentry { .. } => &[Field::Range, Field::Ino, Field::ParentIno, Field::Name],
            Key::Subsystem { .. } => &[Field::Range, Field::Subsystem, Field::RecordId],
        }
    }
}

/// A name-bearing key that is exactly its own prefix is nameless, not
/// truncated, and the distinction is worth keeping: one is a codec bug
/// and the other is a scan bound stored by mistake.
fn truncated_or_nameless(range: u8, key: &[u8]) -> KeyError {
    let id_bytes = if range == RANGE_RDENTRY {
        2 * INO_LEN
    } else {
        INO_LEN
    };
    if key.len() == 1 + id_bytes {
        KeyError::EmptyName(range)
    } else {
        KeyError::Truncated {
            range,
            want: 1 + id_bytes + 1,
            got: key.len(),
        }
    }
}

// ------------------------------------------------- the immutability rule

/// Every field of the §P6 model, and whether it may appear in a key.
///
/// This enum is the machine-readable form of §P6's *indexes stay out of
/// the tree* rule. Only fields that are immutable for the lifetime of
/// the entity they describe may be keyed; everything else is value-side
/// only, and [`Field::Atime`] is not even that.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Field {
    /// The range byte. A constant of the encoding.
    Range,
    /// Allocated once and never reused while the inode lives. `rename`
    /// does not change it — which is exactly why renaming a directory
    /// with 10M descendants rewrites ~6 keys instead of 20M (ADR-3).
    Ino,
    /// The containing directory's ino. Mutable *for the dentry* only in
    /// the sense that `rename` moves the dentry — and a move is a
    /// single-key delete plus insert, not an index maintenance problem,
    /// because the old key is derivable from the old name and parent
    /// that the rename already knows.
    ParentIno,
    /// A dentry's name. Same argument as `ParentIno`.
    Name,
    /// An xattr's name. An xattr's *value* changes; its name does not,
    /// and a `setxattr` that changes a name is a remove plus a set.
    XattrName,
    /// Which `0x30` subsystem a record belongs to.
    Subsystem,
    /// A subsystem record's identity — a snapshot id, a clone id. Chosen
    /// at creation and immutable by construction.
    RecordId,

    // Everything below is value-side. Listed so the test can assert the
    // two sets are disjoint, and so that a proposal to key one of them
    // has to argue with a named `immutable()` arm.
    /// Changes on `chmod`.
    Mode,
    /// Changes on `chown`.
    Uid,
    /// Changes on `chown`.
    Gid,
    /// Changes on `link`/`unlink`.
    Nlink,
    /// Changes on every `write` and `truncate`.
    Size,
    /// Changes on every write and on `utimensat`. The retracted
    /// `0x11 | mtime_be | ino` index is why this arm exists.
    Mtime,
    /// Changes on every attribute change.
    Ctime,
    /// A file's chunk list, or the hash of the spilled one. The
    /// retracted `0x20 | chunk_hash | ino` index is why this arm exists;
    /// §P10's GC works by reachability from roots and needs no such
    /// index.
    Manifest,
    /// An xattr's value.
    XattrValue,
    /// Read time. Not in a key, and — uniquely — not in a value either:
    /// §P6 excludes it from the tree entirely, so
    /// [`crate::record::Attrs`] has no field for it. With it in, a
    /// `find` over 100M files would be 100M scattered inode writes
    /// driven by reads (the §14.4 pathological shape). It is node-local,
    /// best-effort, and never part of a commit or a root hash.
    Atime,
}

impl Field {
    /// Every field, so a test can partition them exhaustively.
    pub const ALL: [Field; 17] = [
        Field::Range,
        Field::Ino,
        Field::ParentIno,
        Field::Name,
        Field::XattrName,
        Field::Subsystem,
        Field::RecordId,
        Field::Mode,
        Field::Uid,
        Field::Gid,
        Field::Nlink,
        Field::Size,
        Field::Mtime,
        Field::Ctime,
        Field::Manifest,
        Field::XattrValue,
        Field::Atime,
    ];

    /// Immutable for the lifetime of the entity the key describes, and
    /// therefore admissible in a key (§P6).
    pub fn immutable(self) -> bool {
        matches!(
            self,
            Field::Range
                | Field::Ino
                | Field::ParentIno
                | Field::Name
                | Field::XattrName
                | Field::Subsystem
                | Field::RecordId
        )
    }

    /// Stored in the tree at all. False only for [`Field::Atime`].
    pub fn in_tree(self) -> bool {
        self != Field::Atime
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_every_range() {
        let cases: Vec<Vec<u8>> = vec![
            inode(0),
            inode(u64::MAX),
            dentry(0, b"a"),
            dentry(u64::MAX, &[0xff; NAME_MAX]),
            xattr(7, b"user.constellation.scratch"),
            rdentry(1, 1, b"."),
            rdentry(u64::MAX, u64::MAX, &[0x00, 0x2f, 0xff]),
            subsystem(Subsystem::Snapshot, b"snap-1"),
            // The empty case: a subsystem with one global record.
            subsystem(Subsystem::Quota, b""),
        ];
        for key in cases {
            let parsed = Key::parse(&key).expect("a key this module built");
            assert_eq!(parsed.encode(), key);
            assert_eq!(parsed.range(), key[0]);
        }
    }

    #[test]
    fn field_widths_are_pinned() {
        assert_eq!(inode(0).len(), 9);
        assert_eq!(dentry(0, b"ab").len(), 11);
        assert_eq!(rdentry(0, 0, b"ab").len(), 19);
        assert_eq!(subsystem(Subsystem::Hold, b"").len(), 2);
        assert_eq!(
            dentry(0x0102_0304_0506_0708, b"n"),
            vec![0x02, 1, 2, 3, 4, 5, 6, 7, 8, b'n']
        );
    }

    #[test]
    fn a_nameless_key_is_refused_rather_than_decoded() {
        // These are scan *bounds*, not keys. Storing one would make the
        // lower bound of a directory's range ambiguous.
        assert_eq!(
            Key::parse(dentries_of(3).start()),
            Err(KeyError::EmptyName(RANGE_DENTRY))
        );
        assert_eq!(
            Key::parse(xattrs_of(3).start()),
            Err(KeyError::EmptyName(RANGE_XATTR))
        );
        assert_eq!(
            Key::parse(names_of_in(3, 4).start()),
            Err(KeyError::EmptyName(RANGE_RDENTRY))
        );
        assert_eq!(Key::parse(&[]), Err(KeyError::Empty));
        assert!(matches!(
            Key::parse(&[RANGE_INODE, 1, 2]),
            Err(KeyError::Truncated { .. })
        ));
        // An inode key is exactly nine bytes; a tenth is a field this
        // codec does not have.
        assert!(matches!(
            Key::parse(&[RANGE_INODE, 0, 0, 0, 0, 0, 0, 0, 1, 9]),
            Err(KeyError::Oversized { got: 10, .. })
        ));
        assert!(matches!(
            Key::parse(&[RANGE_RDENTRY, 1]),
            Err(KeyError::Truncated { .. })
        ));
        assert_eq!(
            Key::parse(&[RANGE_SUBSYSTEM, 0x7f]),
            Err(KeyError::UnknownSubsystem(0x7f))
        );
    }

    #[test]
    fn the_reserved_span_is_refused_by_name() {
        for range in RESERVED_RANGES {
            assert_eq!(
                Key::parse(&[range, 1, 2, 3]),
                Err(KeyError::ReservedRange(range)),
                "0x{range:02x}"
            );
            assert!(!RANGES.contains(&range));
        }
        // Unknown but not reserved reads differently, on purpose.
        assert_eq!(Key::parse(&[0x05, 1]), Err(KeyError::UnknownRange(0x05)));
        assert_eq!(Key::parse(&[0xff, 1]), Err(KeyError::UnknownRange(0xff)));
    }

    #[test]
    fn ranges_are_disjoint_and_ordered() {
        assert!(inode(u64::MAX) < dentries_of(0).start().to_vec());
        assert!(dentry(u64::MAX, &[0xff; 64]) < xattrs_of(0).start().to_vec());
        assert!(xattr(u64::MAX, &[0xff; 64]) < names_of(0).start().to_vec());
        assert!(rdentry(u64::MAX, u64::MAX, &[0xff; 64]) < subsystem(SUBSYSTEMS[0], b""));
        for pair in RANGES.windows(2) {
            assert!(pair[0] < pair[1]);
        }
    }

    /// `alloc_ino` builds inos as `(node_prefix << 40) | counter`
    /// (`meta::sqlite::INO_PREFIX_SHIFT`), so the interesting cases are
    /// not around 2^32 but around bit 40, where one node's counter rolls
    /// into the next node's prefix.
    #[test]
    fn inos_order_across_the_prefix_shift_and_the_u64_extremes() {
        const SHIFT: u32 = 40;
        let ino = |node: u64, counter: u64| (node << SHIFT) | counter;
        let mut inos = vec![
            0,
            1,
            ino(0, (1 << SHIFT) - 1),
            ino(1, 0),
            ino(1, 1),
            ino(1, (1 << SHIFT) - 1),
            ino(2, 0),
            ino(0xff_ffff, (1 << SHIFT) - 1),
            u64::MAX,
        ];
        inos.dedup();
        for pair in inos.windows(2) {
            assert!(pair[0] < pair[1], "test data must ascend");
            assert!(inode(pair[0]) < inode(pair[1]), "{pair:?}");
            assert!(dentry(pair[0], b"x") < dentry(pair[1], b"x"), "{pair:?}");
            assert!(xattr(pair[0], b"x") < xattr(pair[1], b"x"), "{pair:?}");
            assert!(
                rdentry(pair[0], u64::MAX, b"z") < rdentry(pair[1], 0, b"a"),
                "the ino field dominates the parent and the name: {pair:?}"
            );
        }
    }

    /// A name may hold any byte the kernel accepted, including `0x00`,
    /// `0x2f` and `0xff`. Because the ino fields are fixed-width and the
    /// name goes last, none of them can perturb the ordering of the
    /// fields ahead of it.
    #[test]
    fn hostile_names_cannot_break_the_parent_ordering() {
        let names: [&[u8]; 7] = [
            b"a",
            b"a\x00",
            b"a\x00b",
            b"a/b",
            b"a\xff",
            &[0x00],
            &[0xff; 255],
        ];
        for name in names {
            // A name never reaches past its own directory's range.
            assert!(dentries_of(42).contains(&dentry(42, name)));
            assert!(dentry(42, name) < dentries_of(43).start().to_vec());
            assert!(dentry(41, name) < dentries_of(42).start().to_vec());
            // Round-trips byte for byte, so a `0x00` is a name byte and
            // not a terminator.
            assert_eq!(
                Key::parse(&dentry(42, name)).unwrap(),
                Key::Dentry {
                    parent_ino: 42,
                    name
                }
            );
            assert_eq!(
                Key::parse(&rdentry(9, 42, name)).unwrap(),
                Key::RDentry {
                    ino: 9,
                    parent_ino: 42,
                    name
                }
            );
        }
        // Lexicographic name order inside one directory, including the
        // prefix case: "a" sorts before "a\x00", which is what stops a
        // shorter name from being confused with a longer one.
        let mut keys: Vec<Vec<u8>> = names.iter().map(|n| dentry(42, n)).collect();
        let sorted = {
            let mut copy = keys.clone();
            copy.sort();
            copy
        };
        keys.sort_by(|a, b| a[1 + INO_LEN..].cmp(&b[1 + INO_LEN..]));
        assert_eq!(keys, sorted, "key order must be name order");
    }

    #[test]
    fn a_range_cannot_run_into_the_next() {
        let dir = dentries_of(7);
        assert_eq!(dir.end(), dentries_of(8).start());
        assert!(dir.contains(&dentry(7, b"zzz")));
        assert!(!dir.contains(&dentry(8, b"aaa")));
        assert!(dentry(7, &[0xff; 300]).as_slice() < dir.end());
        assert!(dentry(8, b"a").as_slice() >= dir.end());

        // The last directory in the range: its bound is the start of the
        // next range, which is still a correct exclusive bound.
        let last = dentries_of(u64::MAX);
        assert_eq!(last.end(), &[RANGE_XATTR]);
        assert!(last.contains(&dentry(u64::MAX, b"a")));
        assert!(!last.contains(&xattr(0, b"a")));

        let x = xattrs_of(9);
        assert!(x.contains(&xattr(9, b"user.a")));
        assert!(!x.contains(&xattr(10, b"user.a")));
        assert_eq!(x.end(), xattrs_of(10).start());

        // The reverse index nests: one parent's names are a sub-range of
        // all of the inode's names.
        assert!(names_of(5).contains(names_of_in(5, 6).start()));
        assert!(names_of_in(5, 6).contains(&rdentry(5, 6, b"n")));
        assert!(!names_of_in(5, 6).contains(&rdentry(5, 7, b"n")));

        assert_eq!(records_of(Subsystem::Snapshot).end(), &[0x30, 0x02]);
        assert!(records_of(Subsystem::Snapshot).contains(&subsystem(Subsystem::Snapshot, b"id")));
        assert!(!records_of(Subsystem::Snapshot).contains(&subsystem(Subsystem::Clone, b"id")));
        assert!(whole_range(RANGE_SUBSYSTEM).contains(&subsystem(Subsystem::Hold, b"h")));
    }

    #[test]
    fn successor_of_all_ones_is_unbounded() {
        assert_eq!(successor(&[0x01, 0x02]), vec![0x01, 0x03]);
        assert_eq!(successor(&[0x01, 0xff]), vec![0x02]);
        assert_eq!(successor(&[0xff, 0xff]), Vec::<u8>::new());
        // An unbounded range still contains what its prefix says.
        let all = KeyRange::of_prefix(vec![0xff]);
        assert!(all.end().is_empty());
        assert!(all.contains(&[0xff, 0x00]));
    }

    #[test]
    fn every_keyed_field_is_immutable_and_atime_is_nowhere() {
        let keys = [
            Key::Inode { ino: 1 },
            Key::Dentry {
                parent_ino: 1,
                name: b"n",
            },
            Key::Xattr {
                ino: 1,
                name: b"user.a",
            },
            Key::RDentry {
                ino: 1,
                parent_ino: 2,
                name: b"n",
            },
            Key::Subsystem {
                subsystem: Subsystem::Snapshot,
                id: b"s",
            },
        ];
        assert_eq!(keys.len(), RANGES.len(), "every range must be covered");
        for key in keys {
            for field in key.fields() {
                assert!(field.immutable(), "{field:?} in 0x{:02x}", key.range());
                assert!(field.in_tree(), "{field:?}");
                assert_ne!(*field, Field::Atime);
            }
        }
        // The two sets partition `Field::ALL`, so a new field has to
        // choose a side.
        let immutable = Field::ALL.iter().filter(|f| f.immutable()).count();
        assert_eq!(immutable, 7);
        assert_eq!(Field::ALL.iter().filter(|f| !f.in_tree()).count(), 1);
        assert!(!Field::Atime.in_tree() && !Field::Atime.immutable());
    }
}
