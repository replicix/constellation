//! Replay: apply shipped log records to rebuild a metadata replica
//! (fresh-node bootstrap, checkpoint catch-up). Replay writes use the
//! recorded inos and timestamps and are NOT journaled — they already
//! live in the log.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::sqlite::SqliteMeta;
use constellation_fs_core::InodeKind;
use rusqlite::{params, Connection, OptionalExtension};

impl SqliteMeta {
    /// Apply a batch of records in one transaction, then advance
    /// `next_ino` past every inode seen.
    pub fn apply_records(&self, records: &[LogRecord]) -> Result<(), MetaError> {
        let mut conn = self.raw();
        let tx = conn.transaction()?;
        for rec in records {
            apply_one(&tx, rec)?;
        }
        // next_ino must clear everything ever allocated.
        let max_ino: u64 =
            tx.query_row("SELECT COALESCE(MAX(ino), 1) FROM inode", [], |r| r.get(0))?;
        let next: u64 = tx
            .query_row("SELECT value FROM kv WHERE key = 'next_ino'", [], |r| {
                r.get::<_, String>(0)
            })?
            .parse()
            .map_err(|_| MetaError::Invalid("next_ino".into()))?;
        if max_ino + 1 > next {
            tx.execute(
                "UPDATE kv SET value = ?1 WHERE key = 'next_ino'",
                params![(max_ino + 1).to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Consistent zstd-free snapshot of the whole DB with the journal
    /// stripped (checkpoint payload; a restored node must not re-ship).
    pub fn snapshot(&self) -> Result<Vec<u8>, MetaError> {
        let conn = self.raw();
        let tmp =
            std::env::temp_dir().join(format!("constellation-ckpt-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&tmp);
        conn.execute("VACUUM INTO ?1", params![tmp.to_string_lossy()])?;
        drop(conn);
        {
            let c = Connection::open(&tmp)?;
            c.execute("DELETE FROM journal", [])?;
            // Reset AUTOINCREMENT so the restored node journals from 1.
            let _ = c.execute("DELETE FROM sqlite_sequence WHERE name = 'journal'", []);
            c.execute("VACUUM", [])?;
        }
        let bytes = std::fs::read(&tmp).map_err(|e| MetaError::Invalid(e.to_string()))?;
        let _ = std::fs::remove_file(&tmp);
        Ok(bytes)
    }
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

fn kind_of(tx: &Connection, ino: u64) -> Result<u8, MetaError> {
    tx.query_row("SELECT kind FROM inode WHERE ino = ?1", params![ino], |r| {
        r.get(0)
    })
    .optional()?
    .ok_or(MetaError::NoEnt(ino))
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
) -> Result<(), MetaError> {
    tx.execute(
        "INSERT INTO inode (ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns,
                            rdev, symlink_target)
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
    tx.execute(
        "INSERT INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
        params![parent, name, ino],
    )?;
    if kind == InodeKind::Dir {
        tx.execute(
            "UPDATE inode SET nlink = nlink + 1 WHERE ino = ?1",
            params![parent],
        )?;
    }
    tx.execute(
        "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
        params![parent, t],
    )?;
    Ok(())
}

fn apply_one(tx: &Connection, rec: &LogRecord) -> Result<(), MetaError> {
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
            Ok(())
        }
        LogRecord::Unlink {
            parent,
            name,
            time_ns,
        } => {
            let ino = dentry_ino(tx, *parent, name)?.ok_or(MetaError::NoEntry)?;
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
            tx.execute(
                "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![parent, time_ns],
            )?;
            Ok(())
        }
        LogRecord::Rmdir {
            parent,
            name,
            time_ns,
        } => {
            let ino = dentry_ino(tx, *parent, name)?.ok_or(MetaError::NoEntry)?;
            tx.execute(
                "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
            )?;
            tx.execute("DELETE FROM inode WHERE ino = ?1", params![ino])?;
            tx.execute(
                "UPDATE inode SET nlink = nlink - 1, mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![parent, time_ns],
            )?;
            Ok(())
        }
        LogRecord::Rename {
            parent,
            name,
            new_parent,
            new_name,
            time_ns,
        } => {
            let ino = dentry_ino(tx, *parent, name)?.ok_or(MetaError::NoEntry)?;
            let src_is_dir = kind_of(tx, ino)? == InodeKind::Dir.as_u8();
            if let Some(existing) = dentry_ino(tx, *new_parent, new_name)? {
                if existing == ino {
                    return Ok(());
                }
                if kind_of(tx, existing)? == InodeKind::Dir.as_u8() {
                    tx.execute("DELETE FROM inode WHERE ino = ?1", params![existing])?;
                    tx.execute(
                        "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
                        params![new_parent],
                    )?;
                } else {
                    tx.execute(
                        "UPDATE inode SET nlink = nlink - 1, ctime_ns = ?2 WHERE ino = ?1",
                        params![existing, time_ns],
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
            Ok(())
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
            Ok(())
        }
        LogRecord::WriteManifest {
            ino,
            manifest,
            size,
            time_ns,
        } => {
            tx.execute(
                "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4
                 WHERE ino = ?1",
                params![ino, manifest, *size as i64, time_ns],
            )?;
            Ok(())
        }
    }
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
