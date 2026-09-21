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
//! anything we are tailing, so ours win everywhere. That reasoning is
//! what limits the set to *unshipped* records: a record already
//! sequenced in the log (notably one a lease holder accepted for us as
//! a forwarded mutation) has no such claim, and must never suppress a
//! foreign record, or the two replicas diverge for good.
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
use crate::sqlite::{
    SqliteMeta, UsageTracker, INO_PREFIX_SHIFT, QUOTA_CREATION_KV_KEY, QUOTA_KV_KEY,
};
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
            // Atime is deliberately invisible to conflict detection: it
            // records neither a dentry nor an ino, so a pending local
            // atime bump never suppresses a foreign namespace/attr
            // record, and is never suppressed by pending local work.
            LogRecord::PartMerge { .. }
            | LogRecord::RenameXpartAbort { .. }
            | LogRecord::SetQuota { .. }
            | LogRecord::Atime { .. } => {}
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
        // Usage deltas are staged and only folded into the live counter
        // after the commit below: an aborted batch rolls the rows back,
        // and the counter has to roll back with them.
        let staged = UsageTracker::staging();
        let mut skipped = 0usize;
        for rec in records {
            if pending.conflicts(rec) {
                tracing::warn!(?rec, "conflict: pending local op wins over foreign record");
                skipped += 1;
                continue;
            }
            match apply_one(&tx, rec, &staged) {
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
        staged.drain_into(self.usage_tracker());
        Ok(skipped)
    }

    /// Consistent snapshot of the whole DB for a cluster checkpoint.
    /// Strips node-local and ephemeral state so a fresh replica does not
    /// inherit another node's identity or upload obligations:
    /// - `journal` / `atime_journal` / `scratch_*` / `shadow`
    /// - `pending_upload` (this node's not-yet-uploaded set; plan 25)
    /// - `pin` (explicitly node-local)
    /// - `epochs` / `reintegration` (local continuation / stranded-branch
    ///   bookkeeping tied to this node's journal)
    /// - identity / apply-cursor kv keys (`node_id`, `node_prefix`,
    ///   `next_ino`, `applied_seq*`, `part_of/%`) and other node-local kv
    ///   (`left`, `read_only_member`, `lease_lost`, creation-quota mirror)
    ///
    /// `xpart_pending` is kept: it is convergent replay parking for
    /// cross-partition renames that can span a checkpoint boundary, not
    /// node-local writeback state.
    ///
    /// Replicated namespace tables (`inode`, `dentry`, `xattr`, `snapshot`,
    /// `chunk_ref`, `deref`, `partition`, …) are kept.
    pub fn snapshot(&self) -> Result<Vec<u8>, MetaError> {
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

        // `VACUUM INTO` copies the whole database, which costs milliseconds
        // per MiB and so grows with the namespace. Holding the store's
        // connection across it stalls every metadata op — measured at 23ms
        // for a single getattr on a 30k-file DB. A file-backed store copies
        // through its own connection instead: WAL gives that reader a
        // consistent view while writers keep going.
        match self.db_path() {
            Some(path) => {
                let reader = Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
                )?;
                reader.busy_timeout(std::time::Duration::from_secs(30))?;
                reader.execute("VACUUM INTO ?1", params![tmp.to_string_lossy()])?;
            }
            None => {
                let conn = self.raw();
                conn.execute("VACUUM INTO ?1", params![tmp.to_string_lossy()])?;
            }
        }

        {
            let c = Connection::open(&tmp)?;
            // Rollback journaling keeps these deletes in the main file, so
            // the bytes read below are complete. Under WAL they would sit in
            // a `-wal` sidecar that the payload does not carry.
            c.pragma_update(None, "journal_mode", "DELETE")?;
            c.execute("DELETE FROM journal", [])?;
            c.execute("DELETE FROM atime_journal", [])?;
            c.execute("DELETE FROM pending_upload", [])?;
            c.execute("DELETE FROM pin", [])?;
            c.execute("DELETE FROM epochs", [])?;
            c.execute("DELETE FROM reintegration", [])?;
            c.execute_batch(
                "DROP TABLE IF EXISTS scratch_inode;
                 DROP TABLE IF EXISTS scratch_dentry;
                 DROP TABLE IF EXISTS scratch_xattr;
                 DROP TABLE IF EXISTS shadow;",
            )?;
            // Reset AUTOINCREMENT so the restored node journals from 1.
            let _ = c.execute("DELETE FROM sqlite_sequence WHERE name = 'journal'", []);
            c.execute(
                "DELETE FROM kv WHERE key IN (
                    'node_id', 'node_prefix', 'next_ino', 'applied_seq',
                    'left', 'read_only_member', 'lease_lost'
                 )
                 OR key LIKE 'applied_seq/%'
                 OR key LIKE 'part_of/%'
                 OR key = ?1",
                params![QUOTA_CREATION_KV_KEY],
            )?;
            // No second VACUUM: `VACUUM INTO` already wrote a compact file,
            // and reclaiming the pages those deletes freed would rewrite the
            // whole database again for a payload the log store compresses
            // anyway. The free pages are reused when the copy is restored.
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
fn evict_dentry(
    tx: &Connection,
    usage: &UsageTracker,
    parent: u64,
    name: &str,
    ino: u64,
) -> Result<Applied, MetaError> {
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
        let (nlink, size, kind, manifest): (u32, i64, u8, Option<Vec<u8>>) = tx.query_row(
            "SELECT nlink, size, kind, manifest FROM inode WHERE ino = ?1",
            params![ino],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
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
            if kind == InodeKind::File.as_u8() {
                usage.adjust(-size, -1);
            }
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
    usage: &UsageTracker,
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
            if let Applied::Skipped(why) = evict_dentry(tx, usage, parent, name, existing)? {
                return Ok(Applied::Skipped(why));
            }
        }
    }
    let prior_file = kind == InodeKind::File && kind_of(tx, ino)? == Some(InodeKind::File.as_u8());
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
    if kind == InodeKind::File && !prior_file {
        usage.adjust(size as i64, 1);
    }
    Ok(Applied::Done)
}

fn apply_one(tx: &Connection, rec: &LogRecord, usage: &UsageTracker) -> Result<Applied, MetaError> {
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
            usage,
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
            usage,
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
            usage,
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
                tx, usage, *parent, name, *ino, k, *mode, *uid, *gid, *rdev, 1, 0, None, *time_ns,
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
                if let Applied::Skipped(why) = evict_dentry(tx, usage, *parent, name, existing)? {
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
            let (nlink, size, kind, manifest): (u32, i64, u8, Option<Vec<u8>>) = tx.query_row(
                "SELECT nlink, size, kind, manifest FROM inode WHERE ino = ?1",
                params![ino],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
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
                if kind == InodeKind::File.as_u8() {
                    usage.adjust(-size, -1);
                }
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
                if let Applied::Skipped(why) =
                    evict_dentry(tx, usage, *new_parent, new_name, existing)?
                {
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
                let (old_size, kind): (i64, u8) = tx.query_row(
                    "SELECT size, kind FROM inode WHERE ino = ?1",
                    params![ino],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                tx.execute(
                    "UPDATE inode SET size = ?2, mtime_ns = ?3 WHERE ino = ?1",
                    params![ino, *s as i64, time_ns],
                )?;
                if kind == InodeKind::File.as_u8() {
                    usage.adjust(*s as i64 - old_size, 0);
                }
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
            let (old, old_size): (Option<Vec<u8>>, i64) = tx.query_row(
                "SELECT manifest, size FROM inode WHERE ino = ?1",
                params![ino],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            tx.execute(
                "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4
                 WHERE ino = ?1",
                params![ino, manifest, *size as i64, time_ns],
            )?;
            usage.adjust(*size as i64 - old_size, 0);
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
        LogRecord::RenameXpartSrc { txid, .. } => park_or_apply_xpart(tx, usage, *txid, "src", rec),
        LogRecord::RenameXpartDst { txid, .. } => park_or_apply_xpart(tx, usage, *txid, "dst", rec),
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
            Ok(Applied::Done)
        }
        LogRecord::SnapDelete { id, .. } => {
            tx.execute("DELETE FROM snapshot WHERE id = ?1", params![id])?;
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
                let prior: Option<(i64, u8)> = tx
                    .query_row(
                        "SELECT size, kind FROM inode WHERE ino = ?1",
                        params![node.ino],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
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
                let prior_file_bytes = prior
                    .filter(|(_, k)| *k == InodeKind::File.as_u8())
                    .map(|(s, _)| s)
                    .unwrap_or(0);
                let prior_file = prior
                    .map(|(_, k)| k == InodeKind::File.as_u8())
                    .unwrap_or(false);
                if kind == InodeKind::File {
                    usage.adjust(
                        node.size as i64 - prior_file_bytes,
                        if prior_file { 0 } else { 1 },
                    );
                } else if prior_file {
                    usage.adjust(-prior_file_bytes, -1);
                }
            }
            Ok(Applied::Done)
        }
        LogRecord::SetQuota { max_logical_bytes } => {
            let value = match max_logical_bytes {
                Some(n) => n.to_string(),
                None => String::new(),
            };
            tx.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                params![QUOTA_KV_KEY, value],
            )?;
            Ok(Applied::Done)
        }
        LogRecord::Atime {
            ino,
            atime_ns,
            time_ns,
        } => {
            // Best-effort, order-free: one shared helper (clamp + ctime
            // guard + max-merge) serves replay, the AtimeBatch handler,
            // and the emitting node's own local flush, so a value
            // applied locally can never be one a replica would reject.
            apply_atime_one(tx, *ino, *atime_ns, *time_ns, atime_skew_tolerance_ns())?;
            Ok(Applied::Done)
        }
    }
}

/// Default skew clamp: a bump's claimed atime is never accepted more
/// than this far past the applying node's clock, so one badly skewed
/// node cannot park an inode's atime in the far future.
const ATIME_SKEW_TOLERANCE_S_DEFAULT: i64 = 300;

/// Skew tolerance in nanoseconds, from `CONSTELLATION_ATIME_SKEW_TOLERANCE_S`.
/// Read here (rather than threaded through every replay call site) so
/// foreign-segment replay and bootstrap clamp identically to the local
/// flush path without plumbing cli config into the meta crate.
pub fn atime_skew_tolerance_ns() -> i64 {
    std::env::var("CONSTELLATION_ATIME_SKEW_TOLERANCE_S")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(ATIME_SKEW_TOLERANCE_S_DEFAULT)
        .saturating_mul(1_000_000_000)
}

/// Apply one atime bump to `inode`, the single definition of atime
/// merge semantics. Clamps the claimed value to `now + skew_tol_ns`,
/// then raises `atime_ns` to the max of its current value and the claim
/// — but only while `ctime_ns < time_ns`, so any explicit attr change
/// (a `touch -a`, a peer's write/chmod/truncate) that postdates the
/// read keeps its authority. Never writes ctime. A missing inode is a
/// no-op. Returns true if the claim was clamped (for `skew_clamped`).
pub fn apply_atime_one(
    tx: &Connection,
    ino: u64,
    atime_ns: i64,
    time_ns: i64,
    skew_tol_ns: i64,
) -> Result<bool, MetaError> {
    let ceiling = constellation_fs_core::types::now_ns().saturating_add(skew_tol_ns);
    let (claim, clamped) = if atime_ns > ceiling {
        (ceiling, true)
    } else {
        (atime_ns, false)
    };
    tx.execute(
        "UPDATE inode SET atime_ns = MAX(atime_ns, ?2) WHERE ino = ?1 AND ctime_ns < ?3",
        params![ino, claim, time_ns],
    )?;
    Ok(clamped)
}

fn park_or_apply_xpart(
    tx: &Connection,
    usage: &UsageTracker,
    txid: u64,
    half: &str,
    rec: &LogRecord,
) -> Result<Applied, MetaError> {
    let other = if half == "src" { "dst" } else { "src" };
    let partner: Option<Vec<u8>> = tx
        .query_row(
            "SELECT record FROM xpart_pending WHERE txid = ?1 AND half = ?2",
            params![txid, other],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(bytes) = partner {
        let other_rec = LogRecord::from_postcard(&bytes)?;
        tx.execute("DELETE FROM xpart_pending WHERE txid = ?1", params![txid])?;
        let (src, dst) = if half == "src" {
            (rec, &other_rec)
        } else {
            (&other_rec, rec)
        };
        apply_xpart_pair(tx, usage, src, dst)
    } else {
        tx.execute(
            "INSERT OR REPLACE INTO xpart_pending (txid, half, record) VALUES (?1, ?2, ?3)",
            params![txid, half, rec.to_postcard()?],
        )?;
        Ok(Applied::Done)
    }
}

fn apply_xpart_pair(
    tx: &Connection,
    usage: &UsageTracker,
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
        usage,
    )?;
    SqliteMeta::invalidate_part_cache(tx, *ino)?;
    Ok(Applied::Done)
}

#[cfg(test)]
mod tests {
    use super::{atime_skew_tolerance_ns, TouchSet};
    use crate::{LogRecord, MetaStore, SqliteMeta};

    /// A batch that fails part way rolls the rows back; the usage counter
    /// has to roll back with them, or the node mis-reports `df` and
    /// mis-enforces the quota until it is remounted.
    #[test]
    fn failed_batch_leaves_the_usage_counter_untouched() {
        let dst = SqliteMeta::open_in_memory().unwrap();
        let before = dst.usage();
        let records = vec![
            LogRecord::Create {
                parent: 1,
                name: "f".into(),
                ino: 4242,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 1,
            },
            // Undecodable kind: `apply_one` errors, aborting the batch.
            LogRecord::Mknod {
                parent: 1,
                name: "bad".into(),
                ino: 4243,
                kind: 99,
                mode: 0o644,
                uid: 0,
                gid: 0,
                rdev: 0,
                time_ns: 2,
            },
        ];
        assert!(dst.apply_records(&records).is_err());
        assert!(dst.lookup(1, "f").unwrap().is_none(), "rows rolled back");
        assert_eq!(dst.usage(), before, "counter rolled back with the rows");
        assert_eq!(dst.usage(), dst.recursive_size(1).unwrap());
    }

    /// The same batch applied cleanly does move the counter.
    #[test]
    fn applied_batch_moves_the_usage_counter() {
        let dst = SqliteMeta::open_in_memory().unwrap();
        dst.apply_records(&[LogRecord::Create {
            parent: 1,
            name: "f".into(),
            ino: 4242,
            mode: 0o644,
            uid: 0,
            gid: 0,
            time_ns: 1,
        }])
        .unwrap();
        assert_eq!(dst.usage(), (0, 1));
        dst.apply_records(&[LogRecord::WriteManifest {
            ino: 4242,
            base_manifest: None,
            manifest: b"m".to_vec(),
            size: 500,
            time_ns: 2,
        }])
        .unwrap();
        assert_eq!(dst.usage(), (500, 1));
        assert_eq!(dst.usage(), dst.recursive_size(1).unwrap());
    }

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

    /// Cluster checkpoints must not carry the writer's `pending_upload`
    /// backlog (or pins / epoch bookkeeping) onto a fresh replica
    /// (plan 25).
    #[test]
    fn snapshot_strips_pending_upload() {
        use constellation_fs_core::ChunkHash;

        let src = SqliteMeta::open_in_memory().unwrap();
        src.set_node_prefix(1).unwrap();
        let f = src.create(1, "f", 0o644, 0, 0).unwrap();
        let h = ChunkHash::of(b"writer-chunk");
        src.set_manifest_dirty(f.ino, None, b"M", 4, &[h]).unwrap();
        src.add_pin("/f", f.ino).unwrap();
        assert_eq!(src.pending_upload_count().unwrap(), 1);
        assert!(!src.pins().unwrap().is_empty());

        let snap = src.snapshot().unwrap();
        let tmp = std::env::temp_dir().join(format!(
            "constellation-test-pending-{}.db",
            std::process::id()
        ));
        std::fs::write(&tmp, &snap).unwrap();
        let dst = SqliteMeta::open(&tmp).unwrap();
        assert!(
            dst.pending_uploads().unwrap().is_empty(),
            "pending_upload must not leave the writing node via a checkpoint"
        );
        assert!(
            dst.pins().unwrap().is_empty(),
            "pins are node-local and must be stripped"
        );
        assert!(dst.kv_get("node_id").unwrap().is_none());
        assert!(dst.kv_get("node_prefix").unwrap().is_none());
        // Namespace / manifest survive.
        let kept = dst.lookup(1, "f").unwrap().unwrap();
        assert_eq!(kept.ino, f.ino);
        assert_eq!(dst.manifest(f.ino).unwrap().as_deref(), Some(b"M".as_ref()));
        drop(dst);
        let _ = std::fs::remove_file(&tmp);
    }

    // --- read-time atime (plan 20) ---

    fn atime_of(m: &SqliteMeta, ino: u64) -> i64 {
        m.getattr(ino).unwrap().unwrap().atime_ns
    }

    fn atime_rec(ino: u64, atime_ns: i64, time_ns: i64) -> LogRecord {
        LogRecord::Atime {
            ino,
            atime_ns,
            time_ns,
        }
    }

    #[test]
    fn atime_merge_is_max_idempotent_and_order_free() {
        let m = SqliteMeta::open_in_memory().unwrap();
        let f = m.create(1, "f", 0o644, 0, 0).unwrap();
        let base = f.ctime_ns; // reads always postdate creation
                               // Out-of-order + duplicated records must converge on the max.
        m.apply_records(&[
            atime_rec(f.ino, base + 50, base + 50),
            atime_rec(f.ino, base + 10, base + 10),
            atime_rec(f.ino, base + 50, base + 50), // duplicate
            atime_rec(f.ino, base + 30, base + 30),
        ])
        .unwrap();
        assert_eq!(atime_of(&m, f.ino), base + 50);
        // Replaying an older record again changes nothing (idempotent).
        m.apply_records(&[atime_rec(f.ino, base + 10, base + 10)])
            .unwrap();
        assert_eq!(atime_of(&m, f.ino), base + 50);
    }

    #[test]
    fn atime_ctime_guard_drops_pre_touch_record() {
        let m = SqliteMeta::open_in_memory().unwrap();
        let f = m.create(1, "f", 0o644, 0, 0).unwrap();
        // A read observed before an explicit attr change (ctime bump).
        let read_time = f.ctime_ns - 1;
        // Simulate the explicit change: setattr bumps ctime to "now",
        // which is >= f.ctime_ns > read_time.
        m.setattr(f.ino, None, None, None, None, Some(1234), None)
            .unwrap();
        let after = m.getattr(f.ino).unwrap().unwrap();
        assert!(after.ctime_ns >= f.ctime_ns);
        m.apply_records(&[atime_rec(f.ino, read_time + 5, read_time)])
            .unwrap();
        // The in-flight read-bump is dropped: explicit atime survives.
        assert_eq!(atime_of(&m, f.ino), 1234);
    }

    #[test]
    fn atime_for_missing_inode_is_a_noop() {
        let m = SqliteMeta::open_in_memory().unwrap();
        // No error, no row created.
        m.apply_records(&[atime_rec(999_999, 1, 1)]).unwrap();
        assert!(m.getattr(999_999).unwrap().is_none());
    }

    #[test]
    fn atime_skew_clamp_caps_a_far_future_claim() {
        let m = SqliteMeta::open_in_memory().unwrap();
        let f = m.create(1, "f", 0o644, 0, 0).unwrap();
        let now = constellation_fs_core::types::now_ns();
        // A claim a year in the future, with time_ns that passes the
        // ctime guard.
        let (applied, clamped) = m
            .apply_atime(&[(f.ino, now + 365 * 86_400_000_000_000, now + 1)])
            .unwrap();
        assert_eq!((applied, clamped), (1, 1));
        let got = atime_of(&m, f.ino);
        let ceiling = now + atime_skew_tolerance_ns();
        assert!(got <= ceiling + 1_000_000_000, "clamped near the ceiling");
        assert!(got > now, "but still moved forward");
    }

    #[test]
    fn atime_journal_coalesces_to_one_row_holding_the_max() {
        let m = SqliteMeta::open_in_memory().unwrap();
        let f = m.create(1, "f", 0o644, 0, 0).unwrap();
        let part = m.partition_of(f.ino).unwrap();
        // Many "flushes" of the same inode.
        for t in [10i64, 50, 30, 40] {
            m.queue_atime(&[(f.ino, t, t)]).unwrap();
        }
        assert_eq!(m.atime_backlog_of(&part).unwrap(), 1, "one row per inode");
        let rows = m.take_atime_of(&part, 100).unwrap();
        assert_eq!(rows, vec![(f.ino, 50, 50)], "holds the max");
        m.clear_atime(&part, &[f.ino]).unwrap();
        assert_eq!(m.atime_backlog_of(&part).unwrap(), 0);
    }

    #[test]
    fn atime_backlog_does_not_count_toward_journal_len() {
        let m = SqliteMeta::open_in_memory().unwrap();
        let f = m.create(1, "f", 0o644, 0, 0).unwrap();
        let before = m.journal_len().unwrap();
        m.queue_atime(&[(f.ino, 100, 100)]).unwrap();
        assert_eq!(
            m.journal_len().unwrap(),
            before,
            "atime is not write backlog"
        );
    }

    #[test]
    fn atime_converges_across_replicas_via_shipped_records() {
        // Two replicas holding the same file. A's reads are shipped as
        // Atime records and tailed by B (and re-applied by A); both must
        // converge on the max regardless of arrival order, and an
        // explicit backwards `touch -a` on B must survive.
        let make = || {
            let m = SqliteMeta::open_in_memory().unwrap();
            // Use a fixed ino on both replicas.
            m.apply_records(&[LogRecord::Create {
                parent: 1,
                name: "shared".into(),
                ino: (1 << 40) | 1,
                mode: 0o644,
                uid: 0,
                gid: 0,
                time_ns: 100,
            }])
            .unwrap();
            m
        };
        let a = make();
        let b = make();
        let ino = (1 << 40) | 1;

        // A records reads and publishes them as Atime records.
        let part = a.partition_of(ino).unwrap();
        a.queue_atime(&[(ino, 500, 500)]).unwrap();
        a.queue_atime(&[(ino, 300, 300)]).unwrap();
        let rows = a.take_atime_of(&part, 100).unwrap();
        let shipped: Vec<LogRecord> = rows
            .iter()
            .map(|(i, at, t)| atime_rec(*i, *at, *t))
            .collect();

        // B tails them out of order; A replays its own (idempotent).
        let mut reordered = shipped.clone();
        reordered.reverse();
        b.apply_records(&reordered).unwrap();
        a.apply_records(&shipped).unwrap();

        assert_eq!(atime_of(&a, ino), 500);
        assert_eq!(atime_of(&b, ino), 500, "replicas converge on the max");

        // B sets atime backwards with an explicit touch (bumps ctime);
        // a late-arriving read record from before it must not resurrect.
        b.setattr(ino, None, None, None, None, Some(200), None)
            .unwrap();
        b.apply_records(&[atime_rec(ino, 450, 450)]).unwrap();
        assert_eq!(atime_of(&b, ino), 200, "explicit touch -a survives");
    }

    #[test]
    fn touchset_atime_neither_conflicts_nor_is_conflicted() {
        let atime = atime_rec(42, 1, 1);
        // An Atime record touches nothing.
        let pending = TouchSet::from_records([atime.clone()].iter());
        assert!(pending.dentries.is_empty() && pending.inos.is_empty());
        // And a pending setattr on the same inode does not suppress it.
        let setattr = LogRecord::Setattr {
            ino: 42,
            mode: Some(0o600),
            uid: None,
            gid: None,
            size: None,
            atime_ns: None,
            mtime_ns: None,
            time_ns: 1,
        };
        let pending = TouchSet::from_records([setattr].iter());
        assert!(!pending.conflicts(&atime));
    }
}
