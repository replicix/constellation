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
use std::sync::atomic::{AtomicU64, Ordering};

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

/// The attributes a dentry stands for: its copy, or — for a name of a
/// hard-linked inode, which holds none (plan 45) — the inode's own record,
/// one point read. The flag says whether that read was made.
pub(crate) fn dentry_attrs(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    d: &DentryRecord,
) -> Result<(Attrs, bool), MetaError> {
    match d.attrs {
        Some(attrs) => Ok((attrs, false)),
        None => {
            let rec = get_inode_record(r, ns, d.ino)?.ok_or(MetaError::NoEnt(d.ino))?;
            Ok((rec.attrs, true))
        }
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

/// An orphan's xattrs (DESIGN §3: an unlinked inode a descriptor or the
/// kernel still references keeps them, as on a local filesystem). They
/// live in the node-local `orphans` keyspace beside the inode's record,
/// one key per name — `ino` (8 bytes, the record's own key) followed by
/// the name, so never 8 bytes long and never taken for a record
/// ([`Meta::orphans`] lists 8-byte keys only) — because the record's
/// inline xattr section holds [`record::XATTR_INLINE`] bytes and the
/// spilled form lives in `ns`, the replicated namespace an orphan has
/// left.
pub(crate) fn orphan_xattr_key(ino: Ino, name: &[u8]) -> Vec<u8> {
    let mut key = ino.to_be_bytes().to_vec();
    key.extend_from_slice(name);
    key
}

/// `ino`'s orphan xattrs, by name.
pub(crate) fn orphan_xattrs(
    r: &impl Readable,
    orphans: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<Vec<(String, Vec<u8>)>, MetaError> {
    let mut out = Vec::new();
    for guard in r.prefix(orphans, ino.to_be_bytes()) {
        let (k, v) = guard.into_inner()?;
        if k.len() > 8 {
            out.push((String::from_utf8_lossy(&k[8..]).into_owned(), v.to_vec()));
        }
    }
    Ok(out)
}

/// Drop every orphan xattr of `ino` (its record is reaped, or it is
/// linked again).
pub(crate) fn clear_orphan_xattrs(
    tx: &mut SingleWriterWriteTx,
    orphans: &SingleWriterTxKeyspace,
    ino: Ino,
) -> Result<(), MetaError> {
    let keys: Vec<Vec<u8>> = tx
        .prefix(orphans, ino.to_be_bytes())
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for key in keys.into_iter().filter(|k| k.len() > 8) {
        tx.remove(orphans, key);
    }
    Ok(())
}

/// Move inode `ino` (record `rec`, its last name just gone) into
/// `orphans` as `orphan_rec`, its xattrs with it: they leave `ns` (the
/// spilled `0x03` keys) and the by-name index, and are kept as orphan
/// xattrs. Returns nothing; the caller removes the `ns` record.
#[allow(clippy::too_many_arguments)]
pub(crate) fn orphan_inode(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    blobs: &SingleWriterTxKeyspace,
    xattr_by_name: &SingleWriterTxKeyspace,
    orphans: &SingleWriterTxKeyspace,
    ino: Ino,
    rec: &InodeRecord,
    mut orphan_rec: InodeRecord,
) -> Result<(), MetaError> {
    // Read before the spilled keys go: the set is inline or spilled.
    let xattrs = all_xattrs(tx, ns, blobs, rec, ino)?;
    clear_spilled_xattrs(tx, ns, dirty, ino)?;
    crate::store::misc::xattr_by_name_del_all_tx(
        tx,
        xattr_by_name,
        ino,
        xattrs.iter().map(|(n, _)| n.clone().into_bytes()),
    );
    orphan_rec.xattrs.clear();
    orphan_rec.xattrs_spilled = false;
    tx.insert(orphans, ino.to_be_bytes().to_vec(), orphan_rec.encode());
    for (name, value) in xattrs {
        tx.insert(orphans, orphan_xattr_key(ino, name.as_bytes()), value);
    }
    Ok(())
}

/// A removed (empty) directory leaves the namespace into `orphans`, as an
/// unlinked file does (DESIGN §3): every replica keeps it, attributes and
/// xattrs, for a view whose kernel still references it — a process's
/// working directory, an `O_PATH` descriptor — and reaps it as a file's
/// orphan ([`Meta::reap_orphan`]) once nothing does. Before, it was dropped
/// at once, and `chmod`, `utimes` and the xattr ops through such a
/// reference answered `ENOENT` where a local filesystem changes it.
pub(crate) fn orphan_dir(
    tx: &mut SingleWriterWriteTx,
    meta: &Meta,
    dirty: Dirty,
    ino: Ino,
    rec: &InodeRecord,
    t: i64,
) -> Result<(), MetaError> {
    let mut orphan_rec = rec.clone();
    orphan_rec.attrs.nlink = 0;
    orphan_rec.attrs.ctime_ns = t;
    orphan_inode(
        tx,
        &meta.ns,
        dirty,
        &meta.blobs,
        &meta.xattr_by_name,
        &meta.orphans,
        ino,
        rec,
        orphan_rec,
    )?;
    ns_remove(tx, &meta.ns, dirty, keys::inode(ino))
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
    dirty: Dirty,
    ino: Ino,
) -> Result<(), MetaError> {
    let range = keys::xattrs_of(ino);
    let keys_to_remove: Vec<Vec<u8>> = tx
        .range(ns, range.start().to_vec()..range.end().to_vec())
        .map(|g| g.into_inner().map(|(k, _)| k.to_vec()))
        .collect::<Result<_, _>>()?;
    for k in keys_to_remove {
        ns_remove(tx, ns, dirty, k)?;
    }
    Ok(())
}

/// Write `ino`'s encoded `0x01` record and bring the `0x02` dentries that
/// name it in line, in the same transaction. Every `0x01` write goes
/// through here.
///
/// A dentry's form is a function of the inode's record
/// ([`DentryRecord::for_inode`], plan 45): the attribute copy while the
/// inode is a directory or has one name, the remote form (`ino` + `kind`)
/// while a non-directory has several. So a write to an inode with several
/// names that keeps it that way (`chmod`, a manifest commit, a `link` or
/// `unlink` between two names and more) rewrites **only the `0x01`
/// record**: there is no copy to keep, and nothing is scanned. The names
/// are visited only when the form may change:
///
/// - the inode has one name after this write (a copy is due: its one name
///   is rewritten when stale, and the survivor of a 2 → 1 `unlink` gets
///   its copy here);
/// - it had one name before and has several now (the 1 → 2 `link`: the
///   existing name becomes remote; the new name is written by the caller
///   through [`put_dentry`], which applies the same rule);
/// - `before` is `None`: the caller knows nothing of the inode's earlier
///   state (a create or a clone, which have no names yet, or a record
///   replacing one) and every name is brought in line.
///
/// `before` is the inode's attributes as the caller read them from the
/// record this write replaces. A caller that hands over attributes of a
/// different form than the stored record's would leave a stale copy under
/// a name; a debug build checks it.
pub(crate) fn put_inode_record(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    ino: Ino,
    record: &record::InodeRecord,
    before: Option<Attrs>,
) -> Result<(), MetaError> {
    #[cfg(debug_assertions)]
    if let (Some(before), Some(stored)) = (before, get_inode_record(tx, ns, ino)?) {
        assert_eq!(
            DentryRecord::carries_copy(&before),
            DentryRecord::carries_copy(&stored.attrs),
            "put_inode_record({ino}): `before` has another dentry form than the stored record"
        );
    }
    ns_insert(tx, ns, dirty, keys::inode(ino), record.encode())?;
    let single = DentryRecord::carries_copy(&record.attrs);
    let was_multi = before.is_some_and(|before| !DentryRecord::carries_copy(&before));
    if !single && was_multi {
        return Ok(());
    }
    let want = DentryRecord::for_inode(ino, &record.attrs).encode();
    for (parent, name) in links_of(tx, ns, ino)? {
        let key = keys::dentry(parent, name.as_bytes());
        let stale = match tx.get(ns, &key)? {
            Some(held) => *held != *want,
            None => true,
        };
        if stale {
            ns_insert(tx, ns, dirty, key, want.clone())?;
        }
    }
    Ok(())
}

/// Write `ino`'s `0x01` record (and any `0x03`/`blobs` bodies its
/// xattr set or manifest/symlink spill to), applying §P6's plan_inode
/// spill order. Returns the placement so the caller can maintain
/// `xattr_by_name`. `before` is as for [`put_inode_record`]: `None` for a
/// new inode.
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_inode(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
    xattrs: &[(Vec<u8>, Vec<u8>)],
    before: Option<Attrs>,
) -> Result<XattrPlacement, MetaError> {
    let planned = record::plan_inode(attrs, manifest, symlink_target, xattrs, Meta::hash_blob);
    for blob in planned.blobs {
        Meta::put_blob(tx, blobs, blob);
    }
    put_inode_record(tx, ns, dirty, ino, &planned.record, before)?;
    if planned.xattrs == XattrPlacement::Spilled {
        clear_spilled_xattrs(tx, ns, dirty, ino)?;
        for (name, value) in xattrs {
            let (payload, blob) = record::place_value(value.clone(), Meta::hash_blob);
            if let Some(blob) = blob {
                Meta::put_blob(tx, blobs, blob);
            }
            ns_insert(tx, ns, dirty, keys::xattr(ino, name), payload.encode())?;
        }
    } else {
        clear_spilled_xattrs(tx, ns, dirty, ino)?;
    }
    Ok(planned.xattrs)
}

/// Rewrite `ino`'s `0x01` record for a change of its attrs, manifest or
/// symlink target that leaves its xattr set as `rec` stores it (a
/// manifest commit, a truncate's clip). An inline set rides in the
/// record, as [`put_inode`] writes it. A spilled set's `0x03` keys stay
/// as they are: `rec.xattrs` is empty for one, and handing that to
/// [`put_inode`] wrote the set as empty and removed the keys — every
/// xattr of a file whose set exceeds [`record::XATTR_INLINE`] was lost
/// at its next write's close or truncate. The record is what
/// [`record::plan_inode`] builds for the spilled set (as in
/// [`put_spilled_xattr`]): no inline section and the flag set, and the
/// flag takes no bytes, so the manifest and target spill as they would.
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_inode_keeping_xattrs(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
    rec: &InodeRecord,
    attrs: Attrs,
    manifest: Option<Vec<u8>>,
    symlink_target: Option<Vec<u8>>,
) -> Result<(), MetaError> {
    if !rec.xattrs_spilled {
        put_inode(
            tx,
            ns,
            dirty,
            blobs,
            ino,
            attrs,
            manifest,
            symlink_target,
            &rec.xattrs,
            Some(rec.attrs),
        )?;
        return Ok(());
    }
    let mut planned = record::plan_inode(attrs, manifest, symlink_target, &[], Meta::hash_blob);
    planned.record.xattrs_spilled = true;
    for blob in planned.blobs {
        Meta::put_blob(tx, blobs, blob);
    }
    put_inode_record(tx, ns, dirty, ino, &planned.record, Some(rec.attrs))
}

/// One xattr of a spilled set set (`Some`) or removed (`None`), when the
/// set stays spilled: only that name's `0x03` key and the `0x01` record
/// (whose `attrs` carry the new ctime) are written. Returns `false`, having
/// written nothing, when the change could bring the set back inline; the
/// caller then rewrites the whole set through [`put_inode`].
///
/// [`put_inode`] removes and re-inserts every name of a spilled set,
/// which put every name's key in the memtable once more per change (and
/// captured every before-image for speculation): each later range scan of
/// the set walked names × changes versions. A file with a few thousand
/// xattrs (stress-ng's `xattr` stressor) took seconds per `setxattr`,
/// under the store's single writer lock that every metadata write of the
/// node, the authority core's included, waits for. Whether the set stays
/// spilled only needs [`record::XATTR_INLINE`] bytes of the rest of it,
/// so the scan stops there.
#[allow(clippy::too_many_arguments)]
pub(crate) fn put_spilled_xattr(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    blobs: &SingleWriterTxKeyspace,
    ino: Ino,
    rec: &InodeRecord,
    attrs: Attrs,
    name: &str,
    value: Option<&[u8]>,
) -> Result<bool, MetaError> {
    if !rec.xattrs_spilled {
        return Ok(false);
    }
    // `record::xattr_section_len` of the set after the change, counted
    // only until it is over the inline budget.
    let mut len = 2 + value.map_or(0, |v| 4 + name.len() + v.len());
    if len <= record::XATTR_INLINE {
        let range = keys::xattrs_of(ino);
        for guard in tx.range(ns, range.start().to_vec()..range.end().to_vec()) {
            let (k, v) = guard.into_inner()?;
            let keys::Key::Xattr { name: other, .. } = keys::Key::parse(&k)? else {
                continue;
            };
            if other == name.as_bytes() {
                continue;
            }
            len += 4
                + other.len()
                + match Payload::decode(&v)? {
                    Payload::Inline(bytes) => bytes.len(),
                    // Only a value over `VALUE_SPILL` is a blob.
                    Payload::Spilled(_) => record::VALUE_SPILL + 1,
                };
            if len > record::XATTR_INLINE {
                break;
            }
        }
        if len <= record::XATTR_INLINE {
            return Ok(false);
        }
    }
    let key = keys::xattr(ino, name.as_bytes());
    match value {
        Some(value) => {
            let (payload, blob) = record::place_value(value.to_vec(), Meta::hash_blob);
            if let Some(blob) = blob {
                Meta::put_blob(tx, blobs, blob);
            }
            ns_insert(tx, ns, dirty, key, payload.encode())?;
        }
        None => ns_remove(tx, ns, dirty, key)?,
    }
    // What `record::plan_inode` would build: the attrs are fixed-size, so
    // the manifest and target placements it chose stand.
    let mut record = rec.clone();
    record.attrs = attrs;
    put_inode_record(tx, ns, dirty, ino, &record, Some(rec.attrs))?;
    Ok(true)
}

/// A new name for `ino`, whose record has the attributes `attrs`: the `0x02`
/// value in the form the inode's record decides ([`DentryRecord::for_inode`]),
/// and the `0x04` entry. `attrs` are the inode's **current** attributes —
/// after the `nlink` bump when this name is a `link`.
pub(crate) fn put_dentry(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    parent: Ino,
    name: &str,
    ino: Ino,
    attrs: Attrs,
) -> Result<(), MetaError> {
    ns_insert(
        tx,
        ns,
        dirty,
        keys::dentry(parent, name.as_bytes()),
        DentryRecord::for_inode(ino, &attrs).encode(),
    )?;
    ns_insert(
        tx,
        ns,
        dirty,
        keys::rdentry(ino, parent, name.as_bytes()),
        record::RDENTRY_VALUE.to_vec(),
    )
}

pub(crate) fn remove_dentry(
    tx: &mut SingleWriterWriteTx,
    ns: &SingleWriterTxKeyspace,
    dirty: Dirty,
    parent: Ino,
    name: &str,
    ino: Ino,
) -> Result<(), MetaError> {
    ns_remove(tx, ns, dirty, keys::dentry(parent, name.as_bytes()))?;
    ns_remove(tx, ns, dirty, keys::rdentry(ino, parent, name.as_bytes()))
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
    read: &AtomicU64,
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
    read.fetch_add(out.len() as u64, Ordering::Relaxed);
    Ok(out)
}

/// One page of `parent`'s dentries in key (name byte) order, strictly after
/// `after`. The lower bound is the smallest key greater than
/// `dentry(parent, after)`, which is that key with a zero byte appended (the
/// name `after` + `\0` itself is a valid name and is included). The fjall
/// range is lazy, so `take(limit)` bounds the dentries read to the page.
///
/// `read` counts the dentries decoded ([`Meta::dentries_read`](crate::store::Meta::dentries_read)).
///
/// `require_dir` is the caller's, for a first page only: see
/// [`crate::MetaStore::readdir_after`].
pub(crate) fn readdir_after_entries(
    r: &impl Readable,
    ns: &SingleWriterTxKeyspace,
    parent: Ino,
    after: Option<&[u8]>,
    limit: usize,
    read: &AtomicU64,
) -> Result<Vec<crate::RawDirEntry>, MetaError> {
    let range = keys::dentries_of(parent);
    let from = match after {
        Some(name) if !name.is_empty() => {
            let mut key = keys::dentry(parent, name);
            key.push(0);
            key
        }
        _ => range.start().to_vec(),
    };
    let mut out = Vec::with_capacity(limit.min(1024));
    for guard in r.range(ns, from..range.end().to_vec()).take(limit) {
        let (k, v) = guard.into_inner()?;
        let (ino, kind) = DentryRecord::ino_and_kind(&v)?;
        let name = match constellation_mtree::keys::Key::parse(&k)? {
            constellation_mtree::keys::Key::Dentry { name, .. } => name.to_vec(),
            _ => continue,
        };
        out.push(crate::RawDirEntry {
            name,
            ino,
            kind: kind_from_mtree(kind),
        });
    }
    read.fetch_add(out.len() as u64, Ordering::Relaxed);
    Ok(out)
}

pub(crate) fn key_range_bounds(r: &KeyRange) -> std::ops::Range<Vec<u8>> {
    r.start().to_vec()..r.end().to_vec()
}

// ------------------------------------------------------- dirty tracking

/// Whether a write to a §P6 keyspace also has to record the touched key
/// in `dirty` (plan 29 M2). `ns` is tracked, because it is exactly the
/// published tree's key set; `scratch` (private, never published) is
/// not, and never journaled/replicated content should not be able to
/// forget which side of that line it is on.
///
/// A tracked write may also carry a speculation capture context (plan 30
/// §M3a, [`crate::store::spec::Capture`]): every key it touches then has
/// its before-image recorded, so the write can later be rolled back.
/// Only `ns` is ever captured — the same "this is the published tree"
/// reasoning that makes only `ns` dirty-tracked makes only `ns` worth
/// undoing; derived indexes (`chunk_ref`, `xattr_by_name`, `orphans`)
/// are recomputed from the restored records instead
/// (`spec::restore_key_tx`).
#[derive(Clone, Copy)]
pub(crate) enum Dirty<'a> {
    Untracked,
    Tracked {
        dirty: &'a SingleWriterTxKeyspace,
        local: &'a SingleWriterTxKeyspace,
        capture: Option<&'a crate::store::spec::Capture>,
    },
}

impl<'a> Dirty<'a> {
    pub(crate) fn tracked(
        dirty: &'a SingleWriterTxKeyspace,
        local: &'a SingleWriterTxKeyspace,
    ) -> Self {
        Dirty::Tracked {
            dirty,
            local,
            capture: None,
        }
    }

    /// As [`Self::tracked`], additionally recording before-images into
    /// `capture`.
    pub(crate) fn capturing(
        dirty: &'a SingleWriterTxKeyspace,
        local: &'a SingleWriterTxKeyspace,
        capture: &'a crate::store::spec::Capture,
    ) -> Self {
        Dirty::Tracked {
            dirty,
            local,
            capture: Some(capture),
        }
    }

    fn mark(self, tx: &mut SingleWriterWriteTx, key: &[u8]) -> Result<(), MetaError> {
        if let Dirty::Tracked { dirty, local, .. } = self {
            let seq = crate::store::next_dirty_seq_tx(tx, local)?;
            tx.insert(dirty, key.to_vec(), seq.to_be_bytes().to_vec());
        }
        Ok(())
    }

    /// Record `key`'s current value in `ks` as its before-image, when this
    /// write carries a capture context. Must run before the write it
    /// describes.
    fn capture(
        self,
        tx: &mut SingleWriterWriteTx,
        ks: &SingleWriterTxKeyspace,
        key: &[u8],
    ) -> Result<(), MetaError> {
        if let Dirty::Tracked {
            capture: Some(capture),
            ..
        } = self
        {
            if !capture.has_seen(key) {
                let before = tx.get(ks, key)?.map(|v| v.to_vec());
                capture.note_before(key, before);
            }
        }
        Ok(())
    }
}

/// Insert into a §P6 keyspace (`ns` or `scratch`), marking `key` dirty
/// first when `dirty` says to track it. The one place every write to
/// `ns` funnels through — directly, or via [`put_inode`]/[`put_dentry`]/
/// [`remove_dentry`]/[`clear_spilled_xattrs`] below, which all call this
/// — so dirty-tracking (and, plan 30 §M3a, speculation capture) cannot
/// be forgotten on a new write path (plan 29 M2's `dirty_snapshot`/
/// `clear_dirty_upto` test exercises every mutating API to check exactly
/// that).
pub(crate) fn ns_insert(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    dirty: Dirty,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), MetaError> {
    dirty.mark(tx, &key)?;
    dirty.capture(tx, ks, &key)?;
    tx.insert(ks, key, value);
    Ok(())
}

/// The removing twin of [`ns_insert`].
pub(crate) fn ns_remove(
    tx: &mut SingleWriterWriteTx,
    ks: &SingleWriterTxKeyspace,
    dirty: Dirty,
    key: Vec<u8>,
) -> Result<(), MetaError> {
    dirty.mark(tx, &key)?;
    dirty.capture(tx, ks, &key)?;
    tx.remove(ks, key);
    Ok(())
}
