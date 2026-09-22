//! SQLite exactly as `constellation_meta::SqliteMeta` configures it
//! (`crates/meta/src/sqlite.rs`): WAL, `synchronous=NORMAL`, the real
//! `inode` / `dentry` (`WITHOUT ROWID`) / `xattr` (`WITHOUT ROWID`)
//! tables and the `dentry_by_ino` index, one writer connection behind a
//! mutex, and one read-only connection per thread
//! (`query_only=ON`, `cache_size=-524288` i.e. 512 MiB,
//! `mmap_size=256 MiB`) — `with_reader`'s exact pragmas.
//!
//! We do not drive `SqliteMeta` itself: its public API is built around
//! the 23-variant `LogRecord`/replay machinery (partitions, xpart,
//! snapshots, chunk-ref bookkeeping) that plan 28 §11b's engine swap
//! does not touch, and reproducing that machinery would spend this
//! benchmark's time budget on the log layer rather than on the storage
//! engine. Per the brief's own fallback rule, this replicates the
//! product SQL and pragmas directly, and only for the three tables the
//! FUSE op set in §P6's table touches.
//!
//! Unlike the mtree/redb/fjall engines, `dentry` carries **no** attr
//! copy — that asymmetry is real and is the whole reason §P6 adds one:
//! `readdirplus` here is a join (or, as implemented, one point lookup
//! per row) against `inode`, where the other three engines pay nothing
//! extra over plain `readdir`.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use constellation_mtree::record::{Attrs, Kind};
use rusqlite::{params, Connection, OptionalExtension};

use crate::engine::{Engine, PlusEntry};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS inode (
    ino       INTEGER PRIMARY KEY,
    kind      INTEGER NOT NULL,
    size      INTEGER NOT NULL DEFAULT 0,
    mode      INTEGER NOT NULL,
    uid       INTEGER NOT NULL,
    gid       INTEGER NOT NULL,
    nlink     INTEGER NOT NULL,
    mtime_ns  INTEGER NOT NULL,
    ctime_ns  INTEGER NOT NULL,
    rdev      INTEGER NOT NULL DEFAULT 0,
    symlink_target TEXT
);
CREATE TABLE IF NOT EXISTS dentry (
    parent INTEGER NOT NULL,
    name   TEXT NOT NULL,
    ino    INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dentry_by_ino ON dentry (ino);
CREATE TABLE IF NOT EXISTS xattr (
    ino   INTEGER NOT NULL,
    name  TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (ino, name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS xattr_by_name ON xattr (name);
";

fn kind_to_u8(k: Kind) -> i64 {
    k.as_u8() as i64
}
fn kind_from_u8(v: i64) -> Kind {
    Kind::from_u8(v as u8).unwrap_or(Kind::File)
}

pub enum SqliteTuning {
    /// `crates/meta/src/sqlite.rs`'s exact pragmas.
    Product,
    /// A larger-cache / larger-mmap variant, as its own configuration —
    /// the brief's "SQLite with a different schema/pragmas" candidate.
    Tuned { cache_kib: i64, mmap_bytes: i64 },
}

pub struct SqliteEngine {
    path: PathBuf,
    writer: Mutex<Connection>,
    tuning_cache_kib: i64,
    tuning_mmap: i64,
    bytes_written: AtomicU64,
}

thread_local! {
    static READER: RefCell<Option<(PathBuf, Connection)>> = const { RefCell::new(None) };
}

impl SqliteEngine {
    pub fn create(path: &Path, tuning: SqliteTuning) -> SqliteEngine {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
        let conn = Connection::open(path).expect("open sqlite");
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
        conn.pragma_update(None, "foreign_keys", "ON").unwrap();
        let (cache_kib, mmap) = match tuning {
            SqliteTuning::Product => (-524_288i64, 268_435_456i64),
            SqliteTuning::Tuned { cache_kib, mmap_bytes } => (cache_kib, mmap_bytes),
        };
        if let SqliteTuning::Tuned { .. } = tuning {
            conn.pragma_update(None, "page_size", 8192i64).ok();
        }
        conn.execute_batch(SCHEMA).unwrap();
        SqliteEngine {
            path: path.to_path_buf(),
            writer: Mutex::new(conn),
            tuning_cache_kib: cache_kib,
            tuning_mmap: mmap,
            bytes_written: AtomicU64::new(0),
        }
    }

    fn with_reader<T>(&self, f: impl FnOnce(&Connection) -> T) -> T {
        READER.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != &self.path,
                None => true,
            };
            if need_open {
                let conn = Connection::open(&self.path).expect("open reader");
                conn.pragma_update(None, "query_only", "ON").unwrap();
                conn.pragma_update(None, "foreign_keys", "ON").unwrap();
                conn.pragma_update(None, "cache_size", self.tuning_cache_kib).unwrap();
                conn.pragma_update(None, "mmap_size", self.tuning_mmap).unwrap();
                *slot = Some((self.path.clone(), conn));
            }
            f(&slot.as_ref().unwrap().1)
        })
    }

    fn record_write(&self, approx_bytes: u64) {
        self.bytes_written.fetch_add(approx_bytes, Ordering::Relaxed);
    }
}

fn row_attrs(row: &rusqlite::Row) -> rusqlite::Result<Attrs> {
    Ok(Attrs {
        kind: kind_from_u8(row.get("kind")?),
        mode: row.get::<_, i64>("mode")? as u32,
        uid: row.get::<_, i64>("uid")? as u32,
        gid: row.get::<_, i64>("gid")? as u32,
        nlink: row.get::<_, i64>("nlink")? as u32,
        size: row.get::<_, i64>("size")? as u64,
        mtime_ns: row.get("mtime_ns")?,
        ctime_ns: row.get("ctime_ns")?,
        rdev: row.get::<_, i64>("rdev")? as u64,
    })
}

impl Engine for SqliteEngine {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn lookup(&self, parent: u64, name: &[u8]) -> Option<(u64, Attrs)> {
        let name = String::from_utf8_lossy(name).into_owned();
        self.with_reader(|c| {
            c.query_row(
                "SELECT i.ino, i.kind, i.mode, i.uid, i.gid, i.nlink, i.size, i.mtime_ns, i.ctime_ns, i.rdev
                 FROM dentry d JOIN inode i ON i.ino = d.ino
                 WHERE d.parent = ?1 AND d.name = ?2",
                params![parent as i64, name],
                |row| Ok((row.get::<_, i64>(0)? as u64, row_attrs(row)?)),
            )
            .optional()
            .unwrap()
        })
    }

    fn getattr(&self, ino: u64) -> Option<Attrs> {
        self.with_reader(|c| {
            c.query_row(
                "SELECT kind, mode, uid, gid, nlink, size, mtime_ns, ctime_ns, rdev FROM inode WHERE ino = ?1",
                params![ino as i64],
                row_attrs,
            )
            .optional()
            .unwrap()
        })
    }

    fn readdir(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, u64, Kind)> {
        let after = start_after.map(|n| String::from_utf8_lossy(n).into_owned()).unwrap_or_default();
        self.with_reader(|c| {
            let mut stmt = c
                .prepare_cached(
                    "SELECT d.name, d.ino, i.kind FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent = ?1 AND d.name > ?2 ORDER BY d.name LIMIT ?3",
                )
                .unwrap();
            stmt.query_map(params![parent as i64, after, limit as i64], |row| {
                let name: String = row.get(0)?;
                Ok((name.into_bytes(), row.get::<_, i64>(1)? as u64, kind_from_u8(row.get(2)?)))
            })
            .unwrap()
            .filter_map(Result::ok)
            .collect()
        })
    }

    fn readdirplus(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<PlusEntry> {
        let after = start_after.map(|n| String::from_utf8_lossy(n).into_owned()).unwrap_or_default();
        self.with_reader(|c| {
            let mut stmt = c
                .prepare_cached(
                    "SELECT d.name, i.ino, i.kind, i.mode, i.uid, i.gid, i.nlink, i.size, i.mtime_ns, i.ctime_ns, i.rdev
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent = ?1 AND d.name > ?2 ORDER BY d.name LIMIT ?3",
                )
                .unwrap();
            stmt.query_map(params![parent as i64, after, limit as i64], |row| {
                let name: String = row.get(0)?;
                Ok(PlusEntry { name: name.into_bytes(), ino: row.get::<_, i64>(1)? as u64, kind: kind_from_u8(row.get(2)?), attrs: row_attrs(row)? })
            })
            .unwrap()
            .filter_map(Result::ok)
            .collect()
        })
    }

    fn getxattr(&self, ino: u64, name: &[u8]) -> Option<Vec<u8>> {
        let name = String::from_utf8_lossy(name).into_owned();
        self.with_reader(|c| {
            c.query_row("SELECT value FROM xattr WHERE ino=?1 AND name=?2", params![ino as i64, name], |r| r.get(0))
                .optional()
                .unwrap()
        })
    }

    fn listxattr(&self, ino: u64) -> Vec<Vec<u8>> {
        self.with_reader(|c| {
            let mut stmt = c.prepare_cached("SELECT name FROM xattr WHERE ino=?1").unwrap();
            stmt.query_map(params![ino as i64], |r| r.get::<_, String>(0))
                .unwrap()
                .filter_map(Result::ok)
                .map(String::into_bytes)
                .collect()
        })
    }

    fn create(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, xattrs: &[(Vec<u8>, Vec<u8>)]) {
        let name = String::from_utf8_lossy(name).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        tx.execute(
            "INSERT INTO inode (ino,kind,size,mode,uid,gid,nlink,mtime_ns,ctime_ns,rdev) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![ino as i64, kind_to_u8(attrs.kind), attrs.size as i64, attrs.mode as i64, attrs.uid as i64, attrs.gid as i64, attrs.nlink as i64, attrs.mtime_ns, attrs.ctime_ns, attrs.rdev as i64],
        ).unwrap();
        tx.execute("INSERT INTO dentry (parent,name,ino) VALUES (?1,?2,?3)", params![parent as i64, name, ino as i64]).unwrap();
        for (n, v) in xattrs {
            tx.execute("INSERT INTO xattr (ino,name,value) VALUES (?1,?2,?3)", params![ino as i64, String::from_utf8_lossy(n).into_owned(), v]).unwrap();
        }
        tx.execute("UPDATE inode SET mtime_ns=?1, ctime_ns=?1 WHERE ino=?2", params![attrs.mtime_ns, parent as i64]).unwrap();
        tx.commit().unwrap();
        drop(c);
        self.record_write(64 + name.len() as u64 + xattrs.iter().map(|(n, v)| n.len() + v.len()).sum::<usize>() as u64);
    }

    fn mkdir(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs) {
        self.create(parent, name, ino, attrs, &[]);
    }

    fn symlink(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, target: &[u8]) {
        let name_s = String::from_utf8_lossy(name).into_owned();
        let target_s = String::from_utf8_lossy(target).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        tx.execute(
            "INSERT INTO inode (ino,kind,size,mode,uid,gid,nlink,mtime_ns,ctime_ns,rdev,symlink_target) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![ino as i64, kind_to_u8(attrs.kind), attrs.size as i64, attrs.mode as i64, attrs.uid as i64, attrs.gid as i64, attrs.nlink as i64, attrs.mtime_ns, attrs.ctime_ns, attrs.rdev as i64, target_s],
        ).unwrap();
        tx.execute("INSERT INTO dentry (parent,name,ino) VALUES (?1,?2,?3)", params![parent as i64, name_s, ino as i64]).unwrap();
        tx.commit().unwrap();
        drop(c);
        self.record_write(96 + target.len() as u64);
    }

    fn unlink(&self, parent: u64, name: &[u8]) {
        let name = String::from_utf8_lossy(name).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        let ino: Option<i64> = tx.query_row("SELECT ino FROM dentry WHERE parent=?1 AND name=?2", params![parent as i64, name], |r| r.get(0)).optional().unwrap();
        if let Some(ino) = ino {
            tx.execute("DELETE FROM dentry WHERE parent=?1 AND name=?2", params![parent as i64, name]).unwrap();
            let nlink: i64 = tx.query_row("SELECT nlink FROM inode WHERE ino=?1", params![ino], |r| r.get(0)).unwrap_or(1);
            if nlink <= 1 {
                tx.execute("DELETE FROM inode WHERE ino=?1", params![ino]).unwrap();
                tx.execute("DELETE FROM xattr WHERE ino=?1", params![ino]).unwrap();
            } else {
                tx.execute("UPDATE inode SET nlink=nlink-1 WHERE ino=?1", params![ino]).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(c);
        self.record_write(32);
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> bool {
        let name = String::from_utf8_lossy(name).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        let ino: Option<i64> = tx.query_row("SELECT ino FROM dentry WHERE parent=?1 AND name=?2", params![parent as i64, name], |r| r.get(0)).optional().unwrap();
        let Some(ino) = ino else { return false };
        let has_child: Option<i64> = tx.query_row("SELECT 1 FROM dentry WHERE parent=?1 LIMIT 1", params![ino], |r| r.get(0)).optional().unwrap();
        if has_child.is_some() {
            return false;
        }
        tx.execute("DELETE FROM dentry WHERE parent=?1 AND name=?2", params![parent as i64, name]).unwrap();
        tx.execute("DELETE FROM inode WHERE ino=?1", params![ino]).unwrap();
        tx.commit().unwrap();
        drop(c);
        self.record_write(32);
        true
    }

    fn rename(&self, old_parent: u64, old_name: &[u8], new_parent: u64, new_name: &[u8]) {
        let old_name = String::from_utf8_lossy(old_name).into_owned();
        let new_name = String::from_utf8_lossy(new_name).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        let ino: Option<i64> = tx.query_row("SELECT ino FROM dentry WHERE parent=?1 AND name=?2", params![old_parent as i64, old_name], |r| r.get(0)).optional().unwrap();
        if let Some(ino) = ino {
            tx.execute("DELETE FROM dentry WHERE parent=?1 AND name=?2", params![old_parent as i64, old_name]).unwrap();
            tx.execute("INSERT OR REPLACE INTO dentry (parent,name,ino) VALUES (?1,?2,?3)", params![new_parent as i64, new_name, ino]).unwrap();
            let now: i64 = tx.query_row("SELECT mtime_ns FROM inode WHERE ino=?1", params![ino], |r| r.get(0)).unwrap_or(0);
            tx.execute("UPDATE inode SET mtime_ns=?1, ctime_ns=?1 WHERE ino=?2", params![now, old_parent as i64]).unwrap();
            if new_parent != old_parent {
                tx.execute("UPDATE inode SET mtime_ns=?1, ctime_ns=?1 WHERE ino=?2", params![now, new_parent as i64]).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(c);
        self.record_write(48);
    }

    fn link(&self, parent: u64, name: &[u8], ino: u64) {
        let name = String::from_utf8_lossy(name).into_owned();
        let mut c = self.writer.lock().unwrap();
        let tx = c.transaction().unwrap();
        tx.execute("INSERT INTO dentry (parent,name,ino) VALUES (?1,?2,?3)", params![parent as i64, name, ino as i64]).unwrap();
        tx.execute("UPDATE inode SET nlink=nlink+1 WHERE ino=?1", params![ino as i64]).unwrap();
        tx.commit().unwrap();
        drop(c);
        self.record_write(32);
    }

    fn setattr(&self, ino: u64, attrs: Attrs) {
        let c = self.writer.lock().unwrap();
        c.execute(
            "UPDATE inode SET kind=?1, size=?2, mode=?3, uid=?4, gid=?5, nlink=?6, mtime_ns=?7, ctime_ns=?8, rdev=?9 WHERE ino=?10",
            params![kind_to_u8(attrs.kind), attrs.size as i64, attrs.mode as i64, attrs.uid as i64, attrs.gid as i64, attrs.nlink as i64, attrs.mtime_ns, attrs.ctime_ns, attrs.rdev as i64, ino as i64],
        ).unwrap();
        drop(c);
        self.record_write(64);
    }

    fn setxattr(&self, ino: u64, name: &[u8], value: &[u8]) {
        let name = String::from_utf8_lossy(name).into_owned();
        let c = self.writer.lock().unwrap();
        c.execute("INSERT OR REPLACE INTO xattr (ino,name,value) VALUES (?1,?2,?3)", params![ino as i64, name, value]).unwrap();
        drop(c);
        self.record_write((name.len() + value.len()) as u64);
    }

    fn removexattr(&self, ino: u64, name: &[u8]) {
        let name = String::from_utf8_lossy(name).into_owned();
        let c = self.writer.lock().unwrap();
        c.execute("DELETE FROM xattr WHERE ino=?1 AND name=?2", params![ino as i64, name]).unwrap();
        drop(c);
        self.record_write(16);
    }

    fn init_root(&self, attrs: Attrs) {
        let c = self.writer.lock().unwrap();
        c.execute(
            "INSERT OR IGNORE INTO inode (ino,kind,size,mode,uid,gid,nlink,mtime_ns,ctime_ns,rdev) VALUES (1,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![kind_to_u8(attrs.kind), attrs.size as i64, attrs.mode as i64, attrs.uid as i64, attrs.gid as i64, attrs.nlink as i64, attrs.mtime_ns, attrs.ctime_ns, attrs.rdev as i64],
        ).unwrap();
    }

    fn flush(&self) {
        let c = self.writer.lock().unwrap();
        c.pragma_update(None, "wal_checkpoint", "PASSIVE").ok();
    }

    fn disk_bytes(&self) -> u64 {
        let base = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        let wal = std::fs::metadata(self.path.with_extension("db-wal")).map(|m| m.len()).unwrap_or(0);
        base + wal
    }

    fn bytes_written_total(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
}
