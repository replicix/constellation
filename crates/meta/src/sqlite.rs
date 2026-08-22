//! SQLite implementation of the metadata engine (DECISIONS.md ADR-9).
//!
//! WAL mode, `WITHOUT ROWID` dentry table keyed by `(parent, name)`.
//! Every mutation journals its log record in the same transaction, so a
//! crash never separates a namespace change from its record.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::{DirEntry, MetaStore};
use constellation_fs_core::types::{now_ns, ROOT_INO};
use constellation_fs_core::{FileAttr, Ino, InodeKind};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS inode (
    ino       INTEGER PRIMARY KEY,
    kind      INTEGER NOT NULL,
    size      INTEGER NOT NULL DEFAULT 0,
    mode      INTEGER NOT NULL,
    uid       INTEGER NOT NULL,
    gid       INTEGER NOT NULL,
    nlink     INTEGER NOT NULL,
    atime_ns  INTEGER NOT NULL DEFAULT 0,
    mtime_ns  INTEGER NOT NULL,
    ctime_ns  INTEGER NOT NULL,
    rdev      INTEGER NOT NULL DEFAULT 0,
    manifest  BLOB,
    symlink_target TEXT
);
CREATE TABLE IF NOT EXISTS dentry (
    parent INTEGER NOT NULL,
    name   TEXT NOT NULL,
    ino    INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dentry_by_ino ON dentry (ino);
CREATE TABLE IF NOT EXISTS journal (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

/// SQLite-backed [`MetaStore`].
pub struct SqliteMeta {
    conn: Mutex<Connection>,
}

impl SqliteMeta {
    /// Open (or create) a metadata DB. Creates the root inode on first use.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        let conn = Connection::open(path)?;
        Self::init(conn)
    }

    /// In-memory store (tests).
    pub fn open_in_memory() -> Result<Self, MetaError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, MetaError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;
        // Root inode (FUSE ino 1).
        let n: u64 = conn.query_row(
            "SELECT COUNT(*) FROM inode WHERE ino = ?1",
            params![ROOT_INO],
            |r| r.get(0),
        )?;
        if n == 0 {
            let t = now_ns();
            conn.execute(
                "INSERT INTO inode (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns)
                 VALUES (?1, ?2, ?3, 0, 0, 2, ?4, ?4, ?4)",
                params![ROOT_INO, InodeKind::Dir.as_u8(), 0o755, t],
            )?;
            conn.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES ('next_ino', ?1)",
                params![(ROOT_INO + 1).to_string()],
            )?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn alloc_ino(conn: &Connection) -> Result<Ino, MetaError> {
        let next: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'next_ino'", [], |r| {
                r.get(0)
            })?;
        let ino: Ino = next
            .parse()
            .map_err(|_| MetaError::Invalid("next_ino".into()))?;
        conn.execute(
            "UPDATE kv SET value = ?1 WHERE key = 'next_ino'",
            params![(ino + 1).to_string()],
        )?;
        Ok(ino)
    }

    fn journal(conn: &Connection, record: &LogRecord) -> Result<(), MetaError> {
        conn.execute(
            "INSERT INTO journal (record) VALUES (?1)",
            params![serde_json::to_string(record)?],
        )?;
        Ok(())
    }

    fn row_to_attr(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileAttr> {
        let kind_u8: u8 = row.get(1)?;
        Ok(FileAttr {
            ino: row.get(0)?,
            kind: InodeKind::from_u8(kind_u8).unwrap_or(InodeKind::File),
            size: row.get::<_, i64>(2)? as u64,
            mode: row.get(3)?,
            uid: row.get(4)?,
            gid: row.get(5)?,
            nlink: row.get(6)?,
            atime_ns: row.get(7)?,
            mtime_ns: row.get(8)?,
            ctime_ns: row.get(9)?,
            rdev: row.get::<_, i64>(10)? as u64,
        })
    }

    fn attr_by_ino(conn: &Connection, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        Ok(conn
            .query_row(
                "SELECT ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns, rdev
                 FROM inode WHERE ino = ?1",
                params![ino],
                Self::row_to_attr,
            )
            .optional()?)
    }

    fn require_dir(conn: &Connection, ino: Ino) -> Result<(), MetaError> {
        match Self::attr_by_ino(conn, ino)? {
            None => Err(MetaError::NoEnt(ino)),
            Some(a) if a.kind != InodeKind::Dir => Err(MetaError::NotDir),
            Some(_) => Ok(()),
        }
    }

    fn dentry_ino(conn: &Connection, parent: Ino, name: &str) -> Result<Option<Ino>, MetaError> {
        Ok(conn
            .query_row(
                "SELECT ino FROM dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
                |r| r.get(0),
            )
            .optional()?)
    }

    fn insert_dentry(
        conn: &Connection,
        parent: Ino,
        name: &str,
        ino: Ino,
    ) -> Result<(), MetaError> {
        if Self::dentry_ino(conn, parent, name)?.is_some() {
            return Err(MetaError::Exists);
        }
        conn.execute(
            "INSERT INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
            params![parent, name, ino],
        )?;
        Ok(())
    }

    /// Raw connection access for same-crate extensions (replay).
    pub(crate) fn raw(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }
}

impl MetaStore for SqliteMeta {
    fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        let conn = self.conn.lock().unwrap();
        match Self::dentry_ino(&conn, parent, name)? {
            None => Ok(None),
            Some(ino) => Self::attr_by_ino(&conn, ino),
        }
    }

    fn getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        Self::attr_by_ino(&self.conn.lock().unwrap(), ino)
    }

    fn readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        let conn = self.conn.lock().unwrap();
        Self::require_dir(&conn, parent)?;
        let mut stmt = conn.prepare_cached(
            "SELECT d.name, d.ino, i.kind FROM dentry d JOIN inode i ON i.ino = d.ino
             WHERE d.parent = ?1 ORDER BY d.name",
        )?;
        let rows = stmt.query_map(params![parent], |r| {
            let kind_u8: u8 = r.get(2)?;
            Ok(DirEntry {
                name: r.get(0)?,
                ino: r.get(1)?,
                kind: InodeKind::from_u8(kind_u8).unwrap_or(InodeKind::File),
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn readlink(&self, ino: Ino) -> Result<Option<String>, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT symlink_target FROM inode WHERE ino = ?1",
                params![ino],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    fn manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT manifest FROM inode WHERE ino = ?1",
                params![ino],
                |r| r.get::<_, Option<Vec<u8>>>(0),
            )
            .optional()?
            .flatten())
    }

    fn mkdir(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        let ino = Self::alloc_ino(&tx)?;
        let t = now_ns();
        let attr = FileAttr::new_dir(ino, mode, uid, gid, t);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 2, ?6, ?6, ?6)",
            params![ino, InodeKind::Dir.as_u8(), attr.mode, uid, gid, t],
        )?;
        tx.execute(
            "UPDATE inode SET nlink = nlink + 1, mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Mkdir {
                parent,
                name: name.into(),
                ino,
                mode: attr.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(attr)
    }

    fn create(
        &self,
        parent: Ino,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        let ino = Self::alloc_ino(&tx)?;
        let t = now_ns();
        let attr = FileAttr::new_file(ino, mode, uid, gid, t);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6, ?6)",
            params![ino, InodeKind::File.as_u8(), attr.mode, uid, gid, t],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Create {
                parent,
                name: name.into(),
                ino,
                mode: attr.mode,
                uid,
                gid,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(attr)
    }

    fn symlink(
        &self,
        parent: Ino,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        let ino = Self::alloc_ino(&tx)?;
        let t = now_ns();
        let attr = FileAttr::new_symlink(ino, uid, gid, t, target.len() as u64);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode (ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns, symlink_target)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7, ?7, ?8)",
            params![ino, InodeKind::Symlink.as_u8(), attr.size, attr.mode, uid, gid, t, target],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Symlink {
                parent,
                name: name.into(),
                ino,
                target: target.into(),
                uid,
                gid,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(attr)
    }

    #[allow(clippy::too_many_arguments)]
    fn mknod(
        &self,
        parent: Ino,
        name: &str,
        kind: InodeKind,
        mode: u32,
        uid: u32,
        gid: u32,
        rdev: u64,
    ) -> Result<FileAttr, MetaError> {
        if !kind.is_special() {
            return Err(MetaError::Invalid("mknod kind".into()));
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        let ino = Self::alloc_ino(&tx)?;
        let t = now_ns();
        let attr = FileAttr::new_special(ino, kind, mode, uid, gid, rdev, t);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns, rdev)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6, ?6, ?7)",
            params![ino, kind.as_u8(), attr.mode, uid, gid, t, rdev as i64],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Mknod {
                parent,
                name: name.into(),
                ino,
                kind: kind.as_u8(),
                mode: attr.mode,
                uid,
                gid,
                rdev,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(attr)
    }

    fn link(&self, ino: Ino, parent: Ino, name: &str) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        let mut attr = Self::attr_by_ino(&tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        // POSIX: hard links to directories are forbidden.
        if attr.kind == InodeKind::Dir {
            return Err(MetaError::IsDir);
        }
        let t = now_ns();
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "UPDATE inode SET nlink = nlink + 1, ctime_ns = ?2 WHERE ino = ?1",
            params![ino, t],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Link {
                ino,
                parent,
                name: name.into(),
                time_ns: t,
            },
        )?;
        tx.commit()?;
        attr.nlink += 1;
        attr.ctime_ns = t;
        Ok(attr)
    }

    fn unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ino = Self::dentry_ino(&tx, parent, name)?.ok_or(MetaError::NoEntry)?;
        let attr = Self::attr_by_ino(&tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        if attr.kind == InodeKind::Dir {
            return Err(MetaError::IsDir);
        }
        let t = now_ns();
        tx.execute(
            "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
            params![parent, name],
        )?;
        // nlink drops; the inode row is retained at nlink=0 (orphan state,
        // DESIGN.md §3 unlink-while-open) until reap_orphan.
        tx.execute(
            "UPDATE inode SET nlink = nlink - 1, ctime_ns = ?2 WHERE ino = ?1",
            params![ino, t],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Unlink {
                parent,
                name: name.into(),
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn rmdir(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ino = Self::dentry_ino(&tx, parent, name)?.ok_or(MetaError::NoEntry)?;
        let attr = Self::attr_by_ino(&tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        if attr.kind != InodeKind::Dir {
            return Err(MetaError::NotDir);
        }
        let children: u64 = tx.query_row(
            "SELECT COUNT(*) FROM dentry WHERE parent = ?1",
            params![ino],
            |r| r.get(0),
        )?;
        if children > 0 {
            return Err(MetaError::NotEmpty);
        }
        let t = now_ns();
        tx.execute(
            "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
            params![parent, name],
        )?;
        tx.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
        tx.execute(
            "UPDATE inode SET nlink = nlink - 1, mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Rmdir {
                parent,
                name: name.into(),
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ino = Self::dentry_ino(&tx, parent, name)?.ok_or(MetaError::NoEntry)?;
        let src = Self::attr_by_ino(&tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        Self::require_dir(&tx, new_parent)?;
        // POSIX: renaming a directory into its own subtree is EINVAL.
        if src.kind == InodeKind::Dir {
            let mut cursor = new_parent;
            loop {
                if cursor == ino {
                    return Err(MetaError::Invalid("rename into own subtree".into()));
                }
                if cursor == ROOT_INO {
                    break;
                }
                cursor = tx
                    .query_row(
                        "SELECT parent FROM dentry WHERE ino = ?1 LIMIT 1",
                        params![cursor],
                        |r| r.get(0),
                    )
                    .optional()?
                    .ok_or(MetaError::NoEnt(cursor))?;
            }
        }
        let t = now_ns();
        // POSIX: an existing target is atomically replaced (kind rules:
        // dir may only replace an empty dir; non-dir may not replace a dir).
        if let Some(existing) = Self::dentry_ino(&tx, new_parent, new_name)? {
            // Same inode: rename is a no-op that succeeds.
            if existing == ino {
                return Ok(());
            }
            let ex = Self::attr_by_ino(&tx, existing)?.ok_or(MetaError::NoEnt(existing))?;
            match (src.kind == InodeKind::Dir, ex.kind == InodeKind::Dir) {
                (false, true) => return Err(MetaError::IsDir),
                (true, false) => return Err(MetaError::NotDir),
                _ => {}
            }
            if ex.kind == InodeKind::Dir {
                let children: u64 = tx.query_row(
                    "SELECT COUNT(*) FROM dentry WHERE parent = ?1",
                    params![existing],
                    |r| r.get(0),
                )?;
                if children > 0 {
                    return Err(MetaError::NotEmpty);
                }
                tx.execute("DELETE FROM inode WHERE ino = ?1", params![existing])?;
                // The removed dir's ".." reference to new_parent is gone.
                tx.execute(
                    "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
                    params![new_parent],
                )?;
            } else {
                tx.execute(
                    "UPDATE inode SET nlink = nlink - 1, ctime_ns = ?2 WHERE ino = ?1",
                    params![existing, t],
                )?;
            }
            tx.execute(
                "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
                params![new_parent, new_name],
            )?;
        }
        tx.execute(
            "UPDATE dentry SET parent = ?3, name = ?4 WHERE parent = ?1 AND name = ?2",
            params![parent, name, new_parent, new_name],
        )?;
        // A moved directory re-parents its "..": fix both parents' nlink.
        if src.kind == InodeKind::Dir && parent != new_parent {
            tx.execute(
                "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
                params![parent],
            )?;
            tx.execute(
                "UPDATE inode SET nlink = nlink + 1 WHERE ino = ?1",
                params![new_parent],
            )?;
        }
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![parent, t],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
            params![new_parent, t],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Rename {
                parent,
                name: name.into(),
                new_parent,
                new_name: new_name.into(),
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        ino: Ino,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime_ns: Option<i64>,
        mtime_ns: Option<i64>,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut attr = Self::attr_by_ino(&tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        let t = now_ns();
        if let Some(m) = mode {
            attr.mode = m & 0o7777;
        }
        if let Some(u) = uid {
            attr.uid = u;
        }
        if let Some(g) = gid {
            attr.gid = g;
        }
        if let Some(s) = size {
            attr.size = s;
            // POSIX: truncate marks mtime (and ctime) for update.
            attr.mtime_ns = t;
        }
        attr.atime_ns = atime_ns.unwrap_or(attr.atime_ns);
        attr.mtime_ns = mtime_ns.unwrap_or(attr.mtime_ns);
        attr.ctime_ns = t;
        tx.execute(
            "UPDATE inode SET mode = ?2, uid = ?3, gid = ?4, size = ?5, atime_ns = ?6,
             mtime_ns = ?7, ctime_ns = ?8 WHERE ino = ?1",
            params![
                ino,
                attr.mode,
                attr.uid,
                attr.gid,
                attr.size as i64,
                attr.atime_ns,
                attr.mtime_ns,
                t
            ],
        )?;
        Self::journal(
            &tx,
            &LogRecord::Setattr {
                ino,
                mode,
                uid,
                gid,
                size,
                atime_ns,
                mtime_ns,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(attr)
    }

    fn set_manifest(&self, ino: Ino, manifest: &[u8], size: u64) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let t = now_ns();
        let n = tx.execute(
            "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4 WHERE ino = ?1",
            params![ino, manifest, size as i64, t],
        )?;
        if n == 0 {
            return Err(MetaError::NoEnt(ino));
        }
        Self::journal(
            &tx,
            &LogRecord::WriteManifest {
                ino,
                manifest: manifest.to_vec(),
                size,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn reap_orphan(&self, ino: Ino) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM inode WHERE ino = ?1 AND nlink = 0",
            params![ino],
        )?;
        Ok(())
    }

    fn orphans(&self) -> Result<Vec<Ino>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT ino FROM inode WHERE nlink = 0")?;
        let rows = stmt.query_map([], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    fn take_journal(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT seq, record FROM journal ORDER BY seq LIMIT ?1")?;
        let rows = stmt.query_map(params![max as i64], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, String>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, json) = row?;
            out.push((seq, serde_json::from_str(&json)?));
        }
        Ok(out)
    }

    fn ack_journal(&self, upto_seq: u64) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM journal WHERE seq <= ?1", params![upto_seq])?;
        Ok(())
    }

    fn journal_len(&self) -> Result<u64, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row("SELECT COUNT(*) FROM journal", [], |r| r.get(0))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SqliteMeta {
        SqliteMeta::open_in_memory().unwrap()
    }

    #[test]
    fn root_exists() {
        let m = store();
        let root = m.getattr(ROOT_INO).unwrap().unwrap();
        assert_eq!(root.kind, InodeKind::Dir);
        assert_eq!(root.nlink, 2);
    }

    #[test]
    fn create_lookup_readdir() {
        let m = store();
        let f = m.create(ROOT_INO, "a.txt", 0o644, 1000, 1000).unwrap();
        let d = m.mkdir(ROOT_INO, "sub", 0o755, 1000, 1000).unwrap();
        assert_eq!(m.lookup(ROOT_INO, "a.txt").unwrap().unwrap().ino, f.ino);
        assert!(m.lookup(ROOT_INO, "missing").unwrap().is_none());
        let entries = m.readdir(ROOT_INO).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "a.txt");
        assert_eq!(entries[1].name, "sub");
        assert_eq!(entries[1].ino, d.ino);
        // Duplicate name refused.
        assert!(matches!(
            m.create(ROOT_INO, "a.txt", 0o644, 0, 0),
            Err(MetaError::Exists)
        ));
        // Parent nlink bumped by mkdir.
        assert_eq!(m.getattr(ROOT_INO).unwrap().unwrap().nlink, 3);
    }

    #[test]
    fn unlink_keeps_orphan_until_reap() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        m.unlink(ROOT_INO, "f").unwrap();
        assert!(m.lookup(ROOT_INO, "f").unwrap().is_none());
        // Inode retained at nlink=0 (orphan).
        let a = m.getattr(f.ino).unwrap().unwrap();
        assert_eq!(a.nlink, 0);
        assert_eq!(m.orphans().unwrap(), vec![f.ino]);
        m.reap_orphan(f.ino).unwrap();
        assert!(m.getattr(f.ino).unwrap().is_none());
        assert!(m.orphans().unwrap().is_empty());
    }

    #[test]
    fn rmdir_semantics() {
        let m = store();
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        m.create(d.ino, "x", 0o644, 0, 0).unwrap();
        assert!(matches!(m.rmdir(ROOT_INO, "d"), Err(MetaError::NotEmpty)));
        m.unlink(d.ino, "x").unwrap();
        m.rmdir(ROOT_INO, "d").unwrap();
        assert!(m.getattr(d.ino).unwrap().is_none());
    }

    #[test]
    fn rename_including_replace() {
        let m = store();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let _b = m.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        // Simple move into subdir.
        m.rename(ROOT_INO, "a", d.ino, "a2").unwrap();
        assert!(m.lookup(ROOT_INO, "a").unwrap().is_none());
        assert_eq!(m.lookup(d.ino, "a2").unwrap().unwrap().ino, a.ino);
        // Replace existing target.
        m.rename(d.ino, "a2", ROOT_INO, "b").unwrap();
        assert_eq!(m.lookup(ROOT_INO, "b").unwrap().unwrap().ino, a.ino);
    }

    #[test]
    fn symlink_roundtrip() {
        let m = store();
        let s = m.symlink(ROOT_INO, "link", "/target/path", 0, 0).unwrap();
        assert_eq!(s.kind, InodeKind::Symlink);
        assert_eq!(m.readlink(s.ino).unwrap().unwrap(), "/target/path");
        assert!(m.readlink(ROOT_INO).unwrap().is_none());
    }

    #[test]
    fn manifest_set_get() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        assert!(m.manifest(f.ino).unwrap().is_none());
        m.set_manifest(f.ino, b"MANIFEST", 12345).unwrap();
        assert_eq!(m.manifest(f.ino).unwrap().unwrap(), b"MANIFEST");
        assert_eq!(m.getattr(f.ino).unwrap().unwrap().size, 12345);
        assert!(matches!(
            m.set_manifest(9999, b"x", 1),
            Err(MetaError::NoEnt(9999))
        ));
    }

    #[test]
    fn journal_records_all_mutations() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        m.set_manifest(f.ino, b"M", 1).unwrap();
        m.setattr(f.ino, Some(0o600), None, None, None, None, None)
            .unwrap();
        m.unlink(ROOT_INO, "f").unwrap();
        assert_eq!(m.journal_len().unwrap(), 4);
        let recs = m.take_journal(10).unwrap();
        assert_eq!(recs.len(), 4);
        assert!(matches!(recs[0].1, LogRecord::Create { .. }));
        assert!(matches!(recs[1].1, LogRecord::WriteManifest { .. }));
        assert!(matches!(recs[2].1, LogRecord::Setattr { .. }));
        assert!(matches!(recs[3].1, LogRecord::Unlink { .. }));
        // Ack drains.
        m.ack_journal(recs[3].0).unwrap();
        assert_eq!(m.journal_len().unwrap(), 0);
    }

    #[test]
    fn mknod_special_nodes() {
        let m = store();
        let f = m
            .mknod(ROOT_INO, "pipe", InodeKind::Fifo, 0o644, 1, 1, 0)
            .unwrap();
        assert_eq!(f.kind, InodeKind::Fifo);
        let d = m
            .mknod(ROOT_INO, "dev", InodeKind::BlockDev, 0o600, 0, 0, 0x0102)
            .unwrap();
        assert_eq!(d.rdev, 0x0102);
        assert_eq!(m.lookup(ROOT_INO, "dev").unwrap().unwrap().rdev, 0x0102);
        assert!(matches!(
            m.mknod(ROOT_INO, "f", InodeKind::File, 0o644, 0, 0, 0),
            Err(MetaError::Invalid(_))
        ));
        m.unlink(ROOT_INO, "pipe").unwrap();
    }

    #[test]
    fn hard_links() {
        let m = store();
        let f = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let linked = m.link(f.ino, ROOT_INO, "b").unwrap();
        assert_eq!(linked.ino, f.ino);
        assert_eq!(linked.nlink, 2);
        assert_eq!(m.lookup(ROOT_INO, "b").unwrap().unwrap().ino, f.ino);
        // Unlink one name: inode survives with nlink 1.
        m.unlink(ROOT_INO, "a").unwrap();
        let attr = m.getattr(f.ino).unwrap().unwrap();
        assert_eq!(attr.nlink, 1);
        assert!(m.orphans().unwrap().is_empty());
        // Directories cannot be hard-linked.
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        assert!(matches!(
            m.link(d.ino, ROOT_INO, "d2"),
            Err(MetaError::IsDir)
        ));
    }

    #[test]
    fn rename_posix_semantics() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let d = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let sub = m.mkdir(d.ino, "sub", 0o755, 0, 0).unwrap();
        // Non-dir cannot replace a dir; dir cannot replace a non-dir.
        assert!(matches!(
            m.rename(ROOT_INO, "f", ROOT_INO, "d"),
            Err(MetaError::IsDir)
        ));
        assert!(matches!(
            m.rename(ROOT_INO, "d", ROOT_INO, "f"),
            Err(MetaError::NotDir)
        ));
        // Rename into own subtree is EINVAL.
        assert!(matches!(
            m.rename(ROOT_INO, "d", sub.ino, "loop"),
            Err(MetaError::Invalid(_))
        ));
        // Rename onto the same inode (hard link) is a successful no-op.
        m.link(f.ino, ROOT_INO, "f2").unwrap();
        m.rename(ROOT_INO, "f", ROOT_INO, "f2").unwrap();
        assert!(m.lookup(ROOT_INO, "f").unwrap().is_some());
        assert!(m.lookup(ROOT_INO, "f2").unwrap().is_some());
        // Moving a dir updates parent nlinks.
        let before = m.getattr(ROOT_INO).unwrap().unwrap().nlink;
        m.rename(d.ino, "sub", ROOT_INO, "sub").unwrap();
        assert_eq!(m.getattr(ROOT_INO).unwrap().unwrap().nlink, before + 1);
        assert_eq!(m.getattr(d.ino).unwrap().unwrap().nlink, 2);
    }

    #[test]
    fn persistence_across_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let ino = {
            let m = SqliteMeta::open(&path).unwrap();
            m.create(ROOT_INO, "persisted", 0o644, 42, 42).unwrap().ino
        };
        let m = SqliteMeta::open(&path).unwrap();
        let attr = m.lookup(ROOT_INO, "persisted").unwrap().unwrap();
        assert_eq!(attr.ino, ino);
        assert_eq!(attr.uid, 42);
        // Ino allocation continues, no reuse.
        let next = m.create(ROOT_INO, "another", 0o644, 0, 0).unwrap();
        assert!(next.ino > ino);
    }
}
