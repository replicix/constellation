//! Shared read/write primitives over one keyspace holding the §P6 key
//! encoding (`ns` and `scratch` both have this shape — a scratch
//! directory's content is private and never journaled, but is stored
//! identically so a scratch → shared publish is a record copy).
//!
//! Every helper here is parameterised over the keyspace handle (and, for
//! reads, over `&impl Readable` — a snapshot or a write transaction) so
//! `ns` and `scratch` share one implementation.

use crate::error::MetaError;
use crate::store::Meta;
use constellation_fs_core::{FileAttr, Ino, InodeKind};
use constellation_mtree::keys::{self, KeyRange};
use constellation_mtree::record::{
    self, Attrs, DentryRecord, InodeRecord, Kind, Payload, XattrPlacement,
};
use fjall::{Readable, SingleWriterTxKeyspace, SingleWriterWriteTx};

pub(crate) fn kind_to_mtree(k: InodeKind) -> Kind {
    Kind::from_u8(k.as_u8()).expect("InodeKind and mtree::Kind discriminants match")
}

pub(crate) fn kind_from_mtree(k: Kind) -> InodeKind {
    InodeKind::from_u8(k.as_u8()).expect("InodeKind and mtree::Kind discriminants match")
}

pub(crate) fn attrs_to_fileattr(ino: Ino, a: &Attrs, atime_ns: i64) -> FileAttr {
    FileAttr {
        ino,
        kind: kind_from_mtree(a.kind),
        size: a.size,
        mode: a.mode,
        uid: a.uid,
        gid: a.gid,
        nlink: a.nlink,
        atime_ns,
        mtime_ns: a.mtime_ns,
        ctime_ns: a.ctime_ns,
        rdev: a.rdev,
    }
}

pub(crate) fn fileattr_to_attrs(a: &FileAttr) -> Attrs {
    Attrs {
        kind: kind_to_mtree(a.kind),
        mode: a.mode,
        uid: a.uid,
        gid: a.gid,
        nlink: a.nlink,
        size: a.size,
        mtime_ns: a.mtime_ns,
        ctime_ns: a.ctime_ns,
        rdev: a.rdev,
    }
}

/// Resolve a `Payload` to its bytes, fetching a spilled body from
/// `blobs` if needed.
pub(crate) fn resolve_payload(
    r: &impl Readable,
    blobs: &SingleWriterTxKeyspace,
    payload: &Payload,
) -> Result<Vec<u8>, MetaError> {
    match payload {
        Payload::Inline(bytes) => Ok(bytes.clone()),
        Payload::Spilled(hash) => Ok(Meta::get_blob(r, blobs, hash)?.unwrap_or_default()),
    }
}

pub(crate) fn get_inode_record(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Option<InodeRecord>, MetaError> {
    match r.get(ns, keys::inode(ino))? {
        Some(v) => Ok(Some(InodeRecord::decode(&v)?)),
        None => Ok(None),
    }
}

pub(crate) fn get_dentry_record(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
) -> Result<Option<DentryRecord>, MetaError> {
    match r.get(ns, keys::dentry(parent, name.as_bytes()))? {
        Some(v) => Ok(Some(DentryRecord::decode(&v)?)),
        None => Ok(None),
    }
}

/// The dentry pointing at `ino` (there's at most one relevant parent for
/// this purpose: an ancestor walk only needs *a* parent, and a hard
/// linked non-directory never needs one).
pub(crate) fn parent_of(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Option<Ino>, MetaError> {
    if ino == constellation_fs_core::types::ROOT_INO {
        return Ok(None);
    }
    let range = keys::names_of(ino);
    if let Some(guard) = r.range(ns_ks, key_range_bounds(&range)).next() {
        let (k, _) = guard.into_inner()?;
        if let constellation_mtree::keys::Key::RDentry { parent_ino, .. } =
            constellation_mtree::keys::Key::parse(&k)?
        {
            return Ok(Some(parent_ino));
        }
    }
    Ok(None)
}

/// `require_dir` (existence + kind check) against a keyspace holding the
/// §P6 encoding.
pub(crate) fn require_dir(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<(), MetaError> {
    match get_inode_record(r, ns_ks, ino)? {
        None => Err(MetaError::NoEnt(ino)),
        Some(rec) if rec.attrs.kind != Kind::Dir => Err(MetaError::NotDir),
        Some(_) => Ok(()),
    }
}

/// Point lookup by ino, checking `ns` first and falling back to
/// `orphans` — an unlinked-but-open inode must still answer `getattr`,
/// `manifest`, `readlink` and xattr reads.
pub(crate) fn get_inode_any(
    r: &impl Readable,
    ns_ks: &SingleWriterTxKeyspace,
    orphans: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Option<InodeRecord>, MetaError> {
    if let Some(rec) = get_inode_record(r, ns_ks, ino)? {
        return Ok(Some(rec));
    }
    match r.get(orphans, ino.to_be_bytes())? {
        Some(v) => Ok(Some(InodeRecord::decode(&v)?)),
        None => Ok(None),
    }
}

pub(crate) fn child_ino(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
) -> Result<Option<Ino>, MetaError> {
    Ok(get_dentry_record(r, ns, parent, name)?.map(|d| d.ino))
}

/// Every `(parent, name)` pointing at `ino` via the `0x04` reverse index.
pub(crate) fn links_of(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Vec<(Ino, String)>, MetaError> {
    let range = keys::names_of(ino);
    let mut out = Vec::new();
    for guard in r.range(ns, range.start().to_vec()..range.end().to_vec()) {
        let (k, _) = guard.into_inner()?;
        if let constellation_mtree::keys::Key::RDentry {
            parent_ino, name, ..
        } = constellation_mtree::keys::Key::parse(&k)?
        {
            out.push((parent_ino, String::from_utf8_lossy(name).into_owned()));
        }
    }
    Ok(out)
}

/// Xattrs spilled to `0x03` for `ino`, resolving any spilled-again
/// (`Payload::Spilled`) values from `blobs`.
pub(crate) fn spilled_xattrs(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Vec<(String, Vec<u8>)>, MetaError> {
    let range = keys::xattrs_of(ino);
    let mut out = Vec::new();
    for guard in r.range(ns, range.start().to_vec()..range.end().to_vec()) {
        let (k, v) = guard.into_inner()?;
        if let constellation_mtree::keys::Key::Xattr { name, .. } =
            constellation_mtree::keys::Key::parse(&k)?
        {
            let payload = Payload::decode(&v)?;
            let value = resolve_payload(r, blobs, &payload)?;
            out.push((String::from_utf8_lossy(name).into_owned(), value));
        }
    }
    Ok(out)
}

/// The whole xattr set for `ino`: inline (from the `0x01` record) or
/// spilled (`0x03` range scan) — never both, per `XattrPlacement`.
pub(crate) fn all_xattrs(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    rec: &InodeRecord,
    ino: Ino,
) -> Result<Vec<(String, Vec<u8>)>, MetaError> {
    if !rec.xattrs.is_empty() {
        return Ok(rec
            .xattrs
            .iter()
            .map(|(n, v)| (String::from_utf8_lossy(n).into_owned(), v.clone()))
            .collect());
    }
    spilled_xattrs(r, ns, blobs, ino)
}

/// Delete every `0x03` key for `ino` (used when replacing the whole
/// xattr set, or when an inode is removed).
pub(crate) fn clear_spilled_xattrs(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<(), MetaError> {
    let range = keys::xattrs_of(ino);
    let keys_to_remove: Vec<Vec<u8>> = tx
        .range(ns, range.start().to_vec()..range.end().to_vec())
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys_to_remove {
        tx.remove(ns, k);
    }
    Ok(())
}

/// Write `ino`'s `0x01` record (and any `0x03`/`blobs` bodies its
/// xattr set or manifest/symlink spill to), applying §P6's plan_inode
/// spill order. Returns the placement so the caller can maintain
/// `xattr_by_name`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_inode(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
    xattrs: &[(Vec<u8>, Vec<u8>)],
) -> Result<XattrPlacement, MetaError> {
    let planned = record::plan_inode(attrs, manifest, symlink_target, xattrs, Meta::hash_blob);
    for blob in planned.blobs {
        Meta::put_blob(tx, blobs, blob);
    }
    tx.insert(ns, keys::inode(ino), planned.record.encode());
    if planned.xattrs == XattrPlacement::Spilled {
        clear_spilled_xattrs(tx, ns, ino)?;
        for (name, value) in xattrs {
            let (payload, blob) = record::place_value(value.clone(), Meta::hash_blob);
            if let Some(blob) = blob {
                Meta::put_blob(tx, blobs, blob);
            }
            tx.insert(ns, keys::xattr(ino, name), payload.encode());
        }
    } else {
        clear_spilled_xattrs(tx, ns, ino)?;
    }
    Ok(planned.xattrs)
}

pub(crate) fn put_dentry(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    ino: Ino,
    attrs: Attrs,
) {
    tx.insert(
        ns,
        keys::dentry(parent, name.as_bytes()),
        DentryRecord::new(ino, attrs).encode(),
    );
    tx.insert(
        ns,
        keys::rdentry(ino, parent, name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    );
}

pub(crate) fn remove_dentry(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
    name: &str,
    ino: Ino,
) {
    tx.remove(ns, keys::dentry(parent, name.as_bytes()));
    tx.remove(ns, keys::rdentry(ino, parent, name.as_bytes()));
}

/// Whether `parent` has any dentries at all (the `rmdir` emptiness
/// probe / `scan_inos`-style range check).
pub(crate) fn has_children(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<bool, MetaError> {
    let range = keys::dentries_of(ino);
    match r
        .range(ns, range.start().to_vec()..range.end().to_vec())
        .next()
    {
        None => Ok(false),
        Some(guard) => {
            guard.into_inner()?;
            Ok(true)
        }
    }
}

pub(crate) fn readdir_entries(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
) -> Result<Vec<crate::DirEntry>, MetaError> {
    let range = keys::dentries_of(parent);
    let mut out = Vec::new();
    for guard in r.range(ns, range.start().to_vec()..range.end().to_vec()) {
        let (k, v) = guard.into_inner()?;
        let (ino, kind) = DentryRecord::ino_and_kind(&v)?;
        let name = match constellation_mtree::keys::Key::parse(&k)? {
            constellation_mtree::keys::Key::Dentry { name, .. } => {
                String::from_utf8_lossy(name).into_owned()
            }
            _ => continue,
        };
        out.push(crate::DirEntry {
            name,
            ino,
            kind: kind_from_mtree(kind),
        });
    }
    Ok(out)
}

pub(crate) fn key_range_bounds(r: &KeyRange) -> std::ops::Range<Vec<u8>> {
    r.start().to_vec()..r.end().to_vec()
}
