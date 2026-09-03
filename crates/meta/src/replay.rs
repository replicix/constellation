//! Replay: apply shipped log records to rebuild or advance a metadata
//! replica (fresh-node bootstrap, checkpoint catch-up, live tailing of
//! foreign segments). Replay writes use the recorded inos and
//! timestamps and are NOT journaled — they already live in the log.
//!
//! Replay is **convergent**, not strict: records apply with last-wins
//! upsert semantics so that (a) segments replayed over a checkpoint
//! that already contains their effects are idempotent, and (b) two
//! replicas that saw the same records in different interleavings agree
//! (the record later in the global log wins). Conflicts with *pending*
//! (unshipped) local records are skipped by the caller via
//! [`TouchSet`] — our own records sit later in the global log than
//! anything we are tailing, so ours win everywhere.
//!
//! ### Partition map
//!
//! `part_split` is carried on the **parent** partition's stream; the
//! child stream starts empty at seq 1 after that record is durable.
//! `part_merge` is carried on the surviving (parent) stream. Both
//! mutate the `partition` table and nothing else — no data moves.
//!
//! ### Cross-partition rename
//!
//! A `rename_xpart` is a linked two-record commit: `RenameXpartSrc` on
//! the source stream and `RenameXpartDst` on the destination stream,
//! sharing a `txid`. A replica applies the rename only when it has
//! **both** halves (parked in `xpart_pending` until the pair arrives).
//!
//! **Recovery rule.** A `RenameXpartSrc` whose partner never appears
//! (writer crashed between the two PUTs) is resolved the next time
//! *any* node writes to either stream: if the dst record is absent
//! from the dst stream at or before that stream's head, the current
//! holder of the src partition appends `RenameXpartAbort { txid }` to
//! the src stream and the file stays at its source. Only the current
//! holder of the src partition may append the abort (lease + epoch
//! fencing makes this race-free).

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::sqlite::{SqliteMeta, INO_PREFIX_SHIFT};
use constellation_fs_core::InodeKind;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashSet;

/// The namespace state a set of log records reads or writes: used to
/// detect foreign records conflicting with pending local ones.
#[derive(Default)]
pub struct TouchSet {
    pub dentries: HashSet<(u64, String)>,
    pub inos: HashSet<u64>,
}

impl TouchSet {
    pub fn from_records<'a>(records: impl Iterator<Item = &'a LogRecord>) -> Self {
        let mut set = Self::default();
        for r in records {
            set.add(r);
        }
        set
    }

    pub fn add(&mut self, rec: &LogRecord) {
        match rec {
            LogRecord::Mkdir {
                parent, name, ino, ..
            }
            | LogRecord::Create {
                parent, name, ino, ..
            }
            | LogRecord::Symlink {
                parent, name, ino, ..
            }
            | LogRecord::Mknod {
                parent, name, ino, ..
            }
            | LogRecord::Link {
                parent, name, ino, ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.inos.insert(*ino);
            }
            LogRecord::Unlink { parent, name, .. } | LogRecord::Rmdir { parent, name, .. } => {
                self.dentries.insert((*parent, name.clone()));
            }
            LogRecord::Rename {
                parent,
                name,
                new_parent,
                new_name,
                ..
            } => {
                self.dentries.insert((*parent, name.clone()));
                self.dentries.insert((*new_parent, new_name.clone()));
            }
            LogRecord::Setattr { ino, .. }
            | LogRecord::WriteManifest { ino, .. }
            | LogRecord::SetXattr { ino, .. }
            | LogRecord::RemoveXattr { ino, .. } => {
                self.inos.insert(*ino);
            }
            LogRecord::PartSplit { at_ino, .. } => {
                self.inos.insert(*at_ino);
            }
            LogRecord::PartMerge { .. } | LogRecord::RenameXpartAbort { .. } => {}
            LogRecord::SnapCreate { .. } | LogRecord::SnapDelete { .. } => {}
            LogRecord::Clone { nodes, .. } => {
                for node in nodes {
                    self.dentries.insert((node.parent, node.name.clone()));
                    self.inos.insert(node.ino);
                }
            }
            LogRecord::RenameXpartSrc {
                from_parent,
                name,
                ino,
                ..
            } => {
                self.dentries.insert((*from_parent, name.clone()));
                self.inos.insert(*ino);
            }
            LogRecord::RenameXpartDst {
                to_parent,
                new_name,
                ino,
                ..
            } => {
                self.dentries.insert((*to_parent, new_name.clone()));
                self.inos.insert(*ino);
            }
        }
    }

    pub fn conflicts(&self, rec: &LogRecord) -> bool {
        let mut single = TouchSet::default();
        single.add(rec);
        single.dentries.iter().any(|d| self.dentries.contains(d))
            || single.inos.iter().any(|i| self.inos.contains(i))
    }
}

impl SqliteMeta {
    /// Apply a batch of records in one transaction (bootstrap /
    /// checkpoint catch-up), then advance this node's ino counter past
    /// every inode seen under its own prefix.
    pub fn apply_records(&self, records: &[LogRecord]) -> Result<(), MetaError> {
        self.apply_foreign(records, &TouchSet::default())?;
        Ok(())
    }

    /// Apply records tailed from other nodes' segments, skipping any
    /// that conflict with pending (unshipped) local records. Returns
    /// the number of records skipped as conflicts.
    pub fn apply_foreign(
        &self,
        records: &[LogRecord],
        pending: &TouchSet,
    ) -> Result<usize, MetaError> {
        let mut conn = self.raw();
        let tx = conn.transaction()?;
        let mut skipped = 0usize;
        for rec in records {
            if pending.conflicts(rec) {
                tracing::warn!(?rec, "conflict: pending local op wins over foreign record");
                skipped += 1;
                continue;
            }
            match apply_one(&tx, rec) {
                Ok(Applied::Done) => {}
                Ok(Applied::Skipped(why)) => {
                    tracing::warn!(?rec, why, "skipped foreign record");
                    skipped += 1;
                }
                Err(e) => return Err(e),
            }
        }
        // Our ino counter must clear everything ever allocated under
        // our own prefix (relevant when replaying our own history).
        let prefix: u64 = tx
            .query_row("SELECT value FROM kv WHERE key = 'node_prefix'", [], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let (lo, hi) = (prefix << INO_PREFIX_SHIFT, (prefix + 1) << INO_PREFIX_SHIFT);
        let max_counter: u64 = tx.query_row(
            "SELECT COALESCE(MAX(ino), 0) FROM inode WHERE ino >= ?1 AND ino < ?2",
            params![lo, hi],
            |r| r.get::<_, i64>(0).map(|v| v as u64),
        )? & ((1 << INO_PREFIX_SHIFT) - 1);
        let next: u64 = tx
            .query_row("SELECT value FROM kv WHERE key = 'next_ino'", [], |r| {
                r.get::<_, String>(0)
            })?
            .parse()
            .map_err(|_| MetaError::Invalid("next_ino".into()))?;
        if max_counter + 1 > next {
            tx.execute(
                "UPDATE kv SET value = ?1 WHERE key = 'next_ino'",
                params![(max_counter + 1).to_string()],
            )?;
        }
        tx.commit()?;
        Ok(skipped)
    }

    /// Consistent zstd-free snapshot of the whole DB with the journal
    /// and node-local identity stripped (checkpoint payload; a restored
    /// node must not re-ship records or inherit our node id / ino
    /// counter).
    pub fn snapshot(&self) -> Result<Vec<u8>, MetaError> {
        let conn = self.raw();
        // Unique per call: a daemon checkpoints per partition, so two
        // snapshots can be in flight at once. Sharing one scratch path
        // let them delete each other's file mid-VACUUM.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let tmp = std::env::temp_dir().join(format!(
            "constellation-ckpt-{}-{}.db",
            std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&tmp);
        conn.execute("VACUUM INTO ?1", params![tmp.to_string_lossy()])?;
        drop(conn);
        {
            let c = Connection::open(&tmp)?;
            c.execute("DELETE FROM journal", [])?;
            // Reset AUTOINCREMENT so the restored node journals from 1.
            let _ = c.execute("DELETE FROM sqlite_sequence WHERE name = 'journal'", []);
            c.execute(
                "DELETE FROM kv WHERE key IN ('node_id', 'node_prefix', 'next_ino', 'applied_seq')
                 OR key LIKE 'applied_seq/%' OR key LIKE 'part_of/%'",
                [],
            )?;
            c.execute("VACUUM", [])?;
        }
        let bytes = std::fs::read(&tmp).map_err(|e| MetaError::Invalid(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp);
        Ok(bytes)
    }
}

enum Applied {
    Done,
    Skipped(&'static str),
}

fn dentry_ino(tx: &Connection, parent: u64, name: &str) -> Result<Option<u64>, MetaError> {
    Ok(tx
        .query_row(
            "SELECT ino FROM dentry WHERE parent = ?1 AND name = ?2",
            params![parent, name],
            |r| r.get(0),
        )
        .optional()?)
}

fn kind_of(tx: &Connection, ino: u64) -> Result<Option<u8>, MetaError> {
    Ok(tx
        .query_row("SELECT kind FROM inode WHERE ino = ?1", params![ino], |r| {
            r.get(0)
        })
        .optional()?)
}

fn ino_exists(tx: &Connection, ino: u64) -> Result<bool, MetaError> {
    Ok(kind_of(tx, ino)?.is_some())
}

/// Drop an existing dentry so a later log record can claim the name
/// (last-wins). Non-empty directories refuse: replacing them silently
/// would drop a subtree.
fn evict_dentry(tx: &Connection, parent: u64, name: &str, ino: u64) -> Result<Applied, MetaError> {
    if kind_of(tx, ino)? == Some(InodeKind::Dir.as_u8()) {
        let children: u64 = tx.query_row(
            "SELECT COUNT(*) FROM dentry WHERE parent = ?1",
            params![ino],
            |r| r.get(0),
        )?;
        if children > 0 {
            return Ok(Applied::Skipped("name held by non-empty directory"));
        }
        tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
        tx.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
        tx.execute(
            "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
            params![parent],
        )?;
    } else {
        // nlink drops; a 0-nlink inode is an orphan, reaped at mount.
        let (nlink, manifest): (u32, Option<Vec<u8>>) = tx.query_row(
            "SELECT nlink, manifest FROM inode WHERE ino = ?1",
            params![ino],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        tx.execute(
            "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
            params![ino],
        )?;
        if nlink == 1 {
            tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            SqliteMeta::track_manifest_transition(
                tx,
                ino,
                manifest.as_deref(),
                None,
                0,
                constellation_fs_core::types::now_ns() / 1_000_000,
            )?;
        }
    }
    tx.execute(
        "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
        params![parent, name],
    )?;
    Ok(Applied::Done)
}

#[allow(clippy::too_many_arguments)]
fn insert_node(
    tx: &Connection,
    parent: u64,
    name: &str,
    ino: u64,
    kind: InodeKind,
    mode: u32,
    uid: u32,
    gid: u32,
    rdev: u64,
    nlink: u32,
    size: u64,
    target: Option<&str>,
    t: i64,
) -> Result<Applied, MetaError> {
    if !ino_exists(tx, parent)? {
        // Cascade of a skipped conflicting mkdir: the parent never
        // materialized here. Skip the whole subtree.
        return Ok(Applied::Skipped("parent does not exist"));
    }
    if let Some(existing) = dentry_ino(tx, parent, name)? {
        if existing != ino {
            // Last-wins: this record is later in the log than whatever
            // holds the name now.
            if let Applied::Skipped(why) = evict_dentry(tx, parent, name, existing)? {
                return Ok(Applied::Skipped(why));
            }
        }
    }
    // OR REPLACE: idempotent under checkpoint/segment overlap.
    tx.execute(
        "INSERT OR REPLACE INTO inode (ino, kind, size, mode, uid, gid, nlink, atime_ns,
                            mtime_ns, ctime_ns, rdev, symlink_target)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8, ?9, ?10)",
        params![
            ino,
            kind.as_u8(),
            size as i64,
            mode,
            uid,
            gid,
            nlink,
            t,
            rdev as i64,
            target
        ],
    )?;
    let inserted = tx.execute(
        "INSERT OR IGNORE INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
        params![parent, name, ino],
    )?;
    if inserted > 0 && kind == InodeKind::Dir {
        tx.execute(
            "UPDATE inode SET nlink = nlink + 1 WHERE ino = ?1",
            params![parent],
        )?;
    }
    tx.execute(
        "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
        params![parent, t],
    )?;
    Ok(Applied::Done)
}

fn apply_one(tx: &Connection, rec: &LogRecord) -> Result<Applied, MetaError> {
    match rec {
        LogRecord::Mkdir {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            *parent,
            name,
            *ino,
            InodeKind::Dir,
            *mode,
            *uid,
            *gid,
            0,
            2,
            0,
            None,
            *time_ns,
        ),
        LogRecord::Create {
            parent,
            name,
            ino,
            mode,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            *parent,
            name,
            *ino,
            InodeKind::File,
            *mode,
            *uid,
            *gid,
            0,
            1,
            0,
            None,
            *time_ns,
        ),
        LogRecord::Symlink {
            parent,
            name,
            ino,
            target,
            uid,
            gid,
            time_ns,
        } => insert_node(
            tx,
            *parent,
            name,
            *ino,
            InodeKind::Symlink,
            0o777,
            *uid,
            *gid,
            0,
            1,
            target.len() as u64,
            Some(target),
            *time_ns,
        ),
        LogRecord::Mknod {
            parent,
            name,
            ino,
            kind,
            mode,
            uid,
            gid,
            rdev,
            time_ns,
        } => {
            let k = InodeKind::from_u8(*kind)
                .ok_or_else(|| MetaError::Invalid(format!("mknod kind {kind} in log")))?;
            insert_node(
                tx, *parent, name, *ino, k, *mode, *uid, *gid, *rdev, 1, 0, None, *time_ns,
            )
        }
        LogRecord::Link {
            ino,
            parent,
            name,
            time_ns,
        } => {
            if !ino_exists(tx, *ino)? || !ino_exists(tx, *parent)? {
                return Ok(Applied::Skipped("link target or parent missing"));
            }
            if let Some(existing) = dentry_ino(tx, *parent, name)? {
                if existing == *ino {
                    return Ok(Applied::Done); // idempotent re-apply
                }
                if let Applied::Skipped(why) = evict_dentry(tx, *parent, name, existing)? {
                    return Ok(Applied::Skipped(why));
                }
            }
            tx.execute(
                "INSERT INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
                params![parent, name, ino],
            )?;
            tx.execute(
                "UPDATE inode SET nlink = nlink + 1, ctime_ns = ?2 WHERE ino = ?1",
                params![ino, time_ns],
            )?;
            tx.execute(
                "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![parent, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Unlink {
            parent,
            name,
            time_ns,
        } => {
            let Some(ino) = dentry_ino(tx, *parent, name)? else {
                return Ok(Applied::Done); // already gone: idempotent
            };
            let (nlink, manifest): (u32, Option<Vec<u8>>) = tx.query_row(
                "SELECT nlink, manifest FROM inode WHERE ino = ?1",
                params![ino],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
            )?;
            // Orphan rows (nlink = 0) are reaped at mount startup; no
            // process holds them open on a replayed replica.
            tx.execute(
                "UPDATE inode SET nlink = nlink - 1, ctime_ns = ?2 WHERE ino = ?1",
                params![ino, time_ns],
            )?;
            if nlink == 1 {
                tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
                SqliteMeta::track_manifest_transition(
                    tx,
                    ino,
                    manifest.as_deref(),
                    None,
                    0,
                    time_ns / 1_000_000,
                )?;
            }
            tx.execute(
                "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![parent, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Rmdir {
            parent,
            name,
            time_ns,
        } => {
            let Some(ino) = dentry_ino(tx, *parent, name)? else {
                return Ok(Applied::Done); // already gone: idempotent
            };
            let children: u64 = tx.query_row(
                "SELECT COUNT(*) FROM dentry WHERE parent = ?1",
                params![ino],
                |r| r.get(0),
            )?;
            if children > 0 {
                // A skipped-conflict cascade left local children here.
                return Ok(Applied::Skipped("directory not empty locally"));
            }
            tx.execute(
                "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
            )?;
            tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            tx.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
            tx.execute(
                "UPDATE inode SET nlink = nlink - 1, mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![parent, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Rename {
            parent,
            name,
            new_parent,
            new_name,
            time_ns,
        } => {
            let Some(ino) = dentry_ino(tx, *parent, name)? else {
                return Ok(Applied::Done); // source gone: already applied
            };
            if !ino_exists(tx, *new_parent)? {
                return Ok(Applied::Skipped("rename destination parent missing"));
            }
            let src_is_dir = kind_of(tx, ino)? == Some(InodeKind::Dir.as_u8());
            if let Some(existing) = dentry_ino(tx, *new_parent, new_name)? {
                if existing == ino {
                    return Ok(Applied::Done); // hardlink pair: POSIX no-op
                }
                if let Applied::Skipped(why) = evict_dentry(tx, *new_parent, new_name, existing)? {
                    return Ok(Applied::Skipped(why));
                }
            }
            tx.execute(
                "UPDATE dentry SET parent = ?3, name = ?4 WHERE parent = ?1 AND name = ?2",
                params![parent, name, new_parent, new_name],
            )?;
            crate::sqlite::SqliteMeta::invalidate_part_cache(tx, ino)?;
            if src_is_dir && parent != new_parent {
                tx.execute(
                    "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
                    params![parent],
                )?;
                tx.execute(
                    "UPDATE inode SET nlink = nlink + 1 WHERE ino = ?1",
                    params![new_parent],
                )?;
            }
            for p in [parent, new_parent] {
                tx.execute(
                    "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                    params![p, time_ns],
                )?;
            }
            Ok(Applied::Done)
        }
        LogRecord::Setattr {
            ino,
            mode,
            uid,
            gid,
            size,
            atime_ns,
            mtime_ns,
            time_ns,
        } => {
            if !ino_exists(tx, *ino)? {
                return Ok(Applied::Done); // inode gone: attrs moot
            }
            if let Some(m) = mode {
                tx.execute(
                    "UPDATE inode SET mode = ?2 WHERE ino = ?1",
                    params![ino, m & 0o7777],
                )?;
            }
            if let Some(u) = uid {
                tx.execute("UPDATE inode SET uid = ?2 WHERE ino = ?1", params![ino, u])?;
            }
            if let Some(g) = gid {
                tx.execute("UPDATE inode SET gid = ?2 WHERE ino = ?1", params![ino, g])?;
            }
            if let Some(s) = size {
                tx.execute(
                    "UPDATE inode SET size = ?2, mtime_ns = ?3 WHERE ino = ?1",
                    params![ino, *s as i64, time_ns],
                )?;
            }
            if let Some(a) = atime_ns {
                tx.execute(
                    "UPDATE inode SET atime_ns = ?2 WHERE ino = ?1",
                    params![ino, a],
                )?;
            }
            if let Some(m) = mtime_ns {
                tx.execute(
                    "UPDATE inode SET mtime_ns = ?2 WHERE ino = ?1",
                    params![ino, m],
                )?;
            }
            tx.execute(
                "UPDATE inode SET ctime_ns = ?2 WHERE ino = ?1",
                params![ino, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::SetXattr {
            ino,
            name,
            value,
            time_ns,
        } => {
            if !ino_exists(tx, *ino)? {
                return Ok(Applied::Done);
            }
            tx.execute(
                "INSERT OR REPLACE INTO xattr (ino, name, value) VALUES (?1, ?2, ?3)",
                params![ino, name, value],
            )?;
            tx.execute(
                "UPDATE inode SET ctime_ns = ?2 WHERE ino = ?1",
                params![ino, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::RemoveXattr { ino, name, time_ns } => {
            tx.execute(
                "DELETE FROM xattr WHERE ino = ?1 AND name = ?2",
                params![ino, name],
            )?;
            tx.execute(
                "UPDATE inode SET ctime_ns = ?2 WHERE ino = ?1",
                params![ino, time_ns],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::WriteManifest {
            ino,
            base_manifest: _,
            manifest,
            size,
            time_ns,
        } => {
            if !ino_exists(tx, *ino)? {
                return Ok(Applied::Done); // inode gone: data moot
            }
            let old: Option<Vec<u8>> = tx.query_row(
                "SELECT manifest FROM inode WHERE ino = ?1",
                params![ino],
                |row| row.get(0),
            )?;
            tx.execute(
                "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4
                 WHERE ino = ?1",
                params![ino, manifest, *size as i64, time_ns],
            )?;
            SqliteMeta::track_manifest_transition(
                tx,
                *ino,
                old.as_deref(),
                Some(manifest),
                0,
                time_ns / 1_000_000,
            )?;
            Ok(Applied::Done)
        }
        LogRecord::PartSplit {
            part,
            at_ino,
            new_part,
            ..
        } => {
            let _ = part;
            tx.execute(
                "INSERT OR REPLACE INTO partition (id, root_ino) VALUES (?1, ?2)",
                params![new_part, at_ino],
            )?;
            // Any cached resolution under this subtree is now stale.
            tx.execute("DELETE FROM kv WHERE key LIKE 'part_of/%'", [])?;
            Ok(Applied::Done)
        }
        LogRecord::PartMerge {
            part, into_part, ..
        } => {
            let _ = into_part;
            tx.execute("DELETE FROM partition WHERE id = ?1", params![part])?;
            tx.execute("DELETE FROM kv WHERE key LIKE 'part_of/%'", [])?;
            Ok(Applied::Done)
        }
        LogRecord::RenameXpartSrc { txid, .. } => park_or_apply_xpart(tx, *txid, "src", rec),
        LogRecord::RenameXpartDst { txid, .. } => park_or_apply_xpart(tx, *txid, "dst", rec),
        LogRecord::RenameXpartAbort { txid } => {
            tx.execute("DELETE FROM xpart_pending WHERE txid = ?1", params![txid])?;
            Ok(Applied::Done)
        }
        LogRecord::SnapCreate {
            id,
            path,
            name,
            root_hash,
            created_unix_ms,
        } => {
            tx.execute(
                "INSERT OR REPLACE INTO snapshot
                 (id, path, name, root_hash, created_unix_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![id, path, name, root_hash, created_unix_ms],
            )?;
            if let Some(hash) = constellation_fs_core::ChunkHash::from_hex(root_hash) {
                tx.execute(
                    "DELETE FROM deref WHERE chunk_hash = ?1",
                    params![hash.0.to_vec()],
                )?;
            }
            Ok(Applied::Done)
        }
        LogRecord::SnapDelete { id, .. } => {
            let root_hash: Option<String> = tx
                .query_row(
                    "SELECT root_hash FROM snapshot WHERE id = ?1",
                    params![id],
                    |row| row.get(0),
                )
                .optional()?;
            tx.execute("DELETE FROM snapshot WHERE id = ?1", params![id])?;
            if let Some(hash) =
                root_hash.and_then(|value| constellation_fs_core::ChunkHash::from_hex(&value))
            {
                tx.execute(
                    "INSERT OR REPLACE INTO deref (chunk_hash, deref_seq, deref_unix_ms)
                     VALUES (?1, 0, ?2)",
                    params![
                        hash.0.to_vec(),
                        constellation_fs_core::types::now_ns() / 1_000_000
                    ],
                )?;
            }
            Ok(Applied::Done)
        }
        LogRecord::Clone { nodes, .. } => {
            for node in nodes {
                let Some(kind) = InodeKind::from_u8(node.kind) else {
                    return Err(MetaError::Invalid(format!(
                        "clone inode {} has invalid kind {}",
                        node.ino, node.kind
                    )));
                };
                if !ino_exists(tx, node.parent)? {
                    return Ok(Applied::Skipped("clone parent does not exist"));
                }
                tx.execute(
                    "INSERT OR REPLACE INTO inode
                     (ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns,
                      ctime_ns, rdev, manifest, symlink_target)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8, ?9, ?10, ?11)",
                    params![
                        node.ino,
                        node.kind,
                        node.size as i64,
                        node.mode,
                        node.uid,
                        node.gid,
                        if kind == InodeKind::Dir { 2 } else { 1 },
                        node.mtime_ns,
                        node.rdev as i64,
                        node.manifest,
                        node.target
                    ],
                )?;
                tx.execute(
                    "INSERT OR REPLACE INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
                    params![node.parent, node.name, node.ino],
                )?;
                tx.execute("DELETE FROM xattr WHERE ino = ?1", params![node.ino])?;
                for (name, value) in &node.xattrs {
                    tx.execute(
                        "INSERT INTO xattr (ino, name, value) VALUES (?1, ?2, ?3)",
                        params![node.ino, name, value],
                    )?;
                }
                SqliteMeta::track_manifest_transition(
                    tx,
                    node.ino,
                    None,
                    node.manifest.as_deref(),
                    0,
                    node.mtime_ns / 1_000_000,
                )?;
            }
            Ok(Applied::Done)
        }
    }
}

fn park_or_apply_xpart(
    tx: &Connection,
    txid: u64,
    half: &str,
    rec: &LogRecord,
) -> Result<Applied, MetaError> {
    let other = if half == "src" { "dst" } else { "src" };
    let partner: Option<String> = tx
        .query_row(
            "SELECT record FROM xpart_pending WHERE txid = ?1 AND half = ?2",
            params![txid, other],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(json) = partner {
        let other_rec: LogRecord = serde_json::from_str(&json)?;
        tx.execute("DELETE FROM xpart_pending WHERE txid = ?1", params![txid])?;
        let (src, dst) = if half == "src" {
            (rec, &other_rec)
        } else {
            (&other_rec, rec)
        };
        apply_xpart_pair(tx, src, dst)
    } else {
        tx.execute(
            "INSERT OR REPLACE INTO xpart_pending (txid, half, record) VALUES (?1, ?2, ?3)",
            params![txid, half, serde_json::to_string(rec)?],
        )?;
        Ok(Applied::Done)
    }
}

fn apply_xpart_pair(
    tx: &Connection,
    src: &LogRecord,
    dst: &LogRecord,
) -> Result<Applied, MetaError> {
    let LogRecord::RenameXpartSrc {
        from_parent,
        name,
        ino,
        time_ns,
        ..
    } = src
    else {
        return Ok(Applied::Skipped("malformed xpart src"));
    };
    let LogRecord::RenameXpartDst {
        to_parent,
        new_name,
        ..
    } = dst
    else {
        return Ok(Applied::Skipped("malformed xpart dst"));
    };
    // Same as an in-partition Rename, but the two halves may have
    // arrived on different streams / in either order.
    apply_one(
        tx,
        &LogRecord::Rename {
            parent: *from_parent,
            name: name.clone(),
            new_parent: *to_parent,
            new_name: new_name.clone(),
            time_ns: *time_ns,
        },
    )?;
    SqliteMeta::invalidate_part_cache(tx, *ino)?;
    Ok(Applied::Done)
}

#[cfg(test)]
mod tests {
    use crate::{LogRecord, MetaStore, SqliteMeta};

    /// Mutate one store, replay its journal into another, and compare.
    #[test]
    fn replay_reproduces_source() {
        let src = SqliteMeta::open_in_memory().unwrap();
        let d = src.mkdir(1, "dir", 0o755, 1000, 1000).unwrap();
        let f = src.create(d.ino, "file", 0o644, 1000, 1000).unwrap();
        src.set_manifest(f.ino, b"manifest-bytes", 123).unwrap();
        src.symlink(1, "ln", "dir/file", 1000, 1000).unwrap();
        let g = src.create(1, "gone", 0o644, 1000, 1000).unwrap();
        src.link(f.ino, 1, "hard").unwrap();
        src.unlink(1, "gone").unwrap();
        src.reap_orphan(g.ino).unwrap();
        src.rename(d.ino, "file", 1, "file2").unwrap();
        src.setattr(f.ino, Some(0o600), None, None, None, None, None)
            .unwrap();

        let records: Vec<LogRecord> = src
            .take_journal(1000)
            .unwrap()
            .into_iter()
            .map(|(_, r)| r)
            .collect();
        let dst = SqliteMeta::open_in_memory().unwrap();
        dst.apply_records(&records).unwrap();
        for ino in dst.orphans().unwrap() {
            dst.reap_orphan(ino).unwrap();
        }

        // Same namespace, attributes, and manifests.
        for (parent, name) in [(1u64, "dir"), (1, "ln"), (1, "file2"), (1, "hard")] {
            let a = src.lookup(parent, name).unwrap().unwrap();
            let b = dst.lookup(parent, name).unwrap().unwrap();
            assert_eq!(a, b, "{name} attrs diverge");
        }
        assert_eq!(src.manifest(f.ino).unwrap(), dst.manifest(f.ino).unwrap());
        assert!(dst.lookup(1, "gone").unwrap().is_none());
        // Ino allocation continues past everything replayed.
        let n = dst.create(1, "new", 0o644, 0, 0).unwrap();
        assert!(n.ino > f.ino.max(g.ino));
    }

    /// Concurrent snapshots in one process must not clobber each other.
    ///
    /// Regression: the scratch file was named by PID alone, so two
    /// threads checkpointing at once removed and re-created the same
    /// path; the loser's connection then failed with "database file has
    /// moved" / "attempt to write a readonly database". A daemon
    /// checkpoints per partition, so this is reachable in production and
    /// not merely a test artifact.
    #[test]
    fn concurrent_snapshots_do_not_collide() {
        let stores: Vec<std::sync::Arc<SqliteMeta>> = (0..8)
            .map(|i| {
                let m = SqliteMeta::open_in_memory().unwrap();
                m.mkdir(1, &format!("d{i}"), 0o755, 0, 0).unwrap();
                std::sync::Arc::new(m)
            })
            .collect();
        let handles: Vec<_> = stores
            .into_iter()
            .map(|m| std::thread::spawn(move || m.snapshot().map(|b| b.len())))
            .collect();
        for h in handles {
            let bytes = h.join().unwrap().expect("snapshot failed under contention");
            assert!(bytes > 0, "empty snapshot");
        }
    }

    #[test]
    fn snapshot_strips_journal() {
        let src = SqliteMeta::open_in_memory().unwrap();
        src.mkdir(1, "d", 0o755, 0, 0).unwrap();
        assert_eq!(src.journal_len().unwrap(), 1);
        let snap = src.snapshot().unwrap();

        let tmp =
            std::env::temp_dir().join(format!("constellation-test-{}.db", std::process::id()));
        std::fs::write(&tmp, &snap).unwrap();
        let dst = SqliteMeta::open(&tmp).unwrap();
        assert_eq!(dst.journal_len().unwrap(), 0);
        assert!(dst.lookup(1, "d").unwrap().is_some());
        drop(dst);
        let _ = std::fs::remove_file(&tmp);
    }
}
