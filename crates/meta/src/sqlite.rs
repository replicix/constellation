//! SQLite implementation of the metadata engine (DECISIONS.md ADR-9).
//!
//! WAL mode, `WITHOUT ROWID` dentry table keyed by `(parent, name)`.
//! Every mutation journals its log record in the same transaction, so a
//! crash never separates a namespace change from its record.

use crate::error::MetaError;
use crate::record::LogRecord;
use crate::replay::TouchSet;
use crate::{CloneNode, CloneSpec, DirEntry, MetaStore, SetXattrMode, SnapshotNode, SnapshotRow};
use constellation_fs_core::types::{now_ns, ROOT_INO};
use constellation_fs_core::{ChunkHash, FileAttr, Ino, InodeKind};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Inos are `node_prefix << 40 | counter`: 24 bits of node id, 40 bits
/// (~1.1e12) of per-node allocations. Prefix 0 belongs to `fs create`
/// genesis (the root inode is 1).
pub const INO_PREFIX_SHIFT: u32 = 40;
pub const SCRATCH_XATTR: &str = "user.constellation.scratch";

fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

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
CREATE TABLE IF NOT EXISTS xattr (
    ino   INTEGER NOT NULL,
    name  TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (ino, name)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS journal (
    seq    INTEGER PRIMARY KEY AUTOINCREMENT,
    record BLOB NOT NULL,
    part   TEXT NOT NULL DEFAULT 'p0'
);
CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS partition (
    id       TEXT PRIMARY KEY,
    root_ino INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS partition_by_root ON partition (root_ino);
CREATE TABLE IF NOT EXISTS xpart_pending (
    txid   INTEGER PRIMARY KEY,
    half   TEXT NOT NULL,
    record BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS pin (
    path      TEXT PRIMARY KEY,
    ino       INTEGER NOT NULL,
    pinned_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS epochs (
    epoch_id    TEXT PRIMARY KEY,
    members     TEXT NOT NULL,
    base        TEXT NOT NULL,
    promised_at INTEGER NOT NULL,
    state       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reintegration (
    journal_seq  INTEGER PRIMARY KEY,
    disposition  TEXT NOT NULL,
    detail       TEXT
);
CREATE TABLE IF NOT EXISTS pending_upload (
    hash BLOB NOT NULL,
    ino  INTEGER NOT NULL,
    PRIMARY KEY (hash, ino)
);
CREATE TABLE IF NOT EXISTS snapshot (
    id              TEXT PRIMARY KEY,
    path            TEXT NOT NULL,
    name            TEXT NOT NULL,
    root_hash       TEXT NOT NULL,
    created_unix_ms INTEGER NOT NULL,
    UNIQUE(path, name)
);
CREATE TABLE IF NOT EXISTS deref (
    chunk_hash    BLOB PRIMARY KEY,
    deref_seq     INTEGER NOT NULL,
    deref_unix_ms INTEGER NOT NULL
);
-- Reverse index chunk_hash -> referencing inode. Without it, deciding
-- whether a hash is still referenced means decoding every manifest in the
-- namespace, which makes a bulk delete quadratic (see `hash_is_live`).
CREATE TABLE IF NOT EXISTS chunk_ref (
    chunk_hash BLOB NOT NULL,
    ino        INTEGER NOT NULL,
    PRIMARY KEY (chunk_hash, ino)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS chunk_ref_by_ino ON chunk_ref (ino);
CREATE TABLE IF NOT EXISTS scratch_inode (
    ino INTEGER PRIMARY KEY,
    kind INTEGER NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    mode INTEGER NOT NULL,
    uid INTEGER NOT NULL,
    gid INTEGER NOT NULL,
    nlink INTEGER NOT NULL,
    atime_ns INTEGER NOT NULL DEFAULT 0,
    mtime_ns INTEGER NOT NULL,
    ctime_ns INTEGER NOT NULL,
    rdev INTEGER NOT NULL DEFAULT 0,
    manifest BLOB,
    symlink_target TEXT
);
CREATE TABLE IF NOT EXISTS scratch_dentry (
    parent INTEGER NOT NULL,
    name TEXT NOT NULL,
    ino INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scratch_dentry_by_ino ON scratch_dentry (ino);
CREATE TABLE IF NOT EXISTS shadow (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    part TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    records BLOB NOT NULL,
    created_ms INTEGER NOT NULL
);
";

pub type JournalBatch = Vec<(u64, LogRecord)>;
pub type EpochRow = (
    String,
    Vec<u64>,
    std::collections::BTreeMap<String, u64>,
    i64,
    String,
);

/// SQLite-backed [`MetaStore`].
pub struct SqliteMeta {
    conn: Mutex<Connection>,
    /// Set for file-backed stores. A checkpoint snapshot reads through its
    /// own connection to this path so it does not hold `conn` — under WAL
    /// that reader is consistent and concurrent with writers. An in-memory
    /// store has no second handle, so it snapshots under the lock.
    path: Option<std::path::PathBuf>,
}

thread_local! {
    /// One read-only WAL connection per thread, keyed by DB path.
    static READER_CONN: RefCell<Option<(PathBuf, Connection)>> = const { RefCell::new(None) };
}

impl SqliteMeta {
    fn configure_reader(conn: &Connection) -> Result<(), MetaError> {
        conn.pragma_update(None, "query_only", "ON")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "cache_size", -524_288)?;
        conn.pragma_update(None, "mmap_size", 268_435_456i64)?;
        Ok(())
    }

    fn with_reader<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, MetaError>,
    ) -> Result<T, MetaError> {
        let Some(path) = self.path.as_ref() else {
            let conn = self.conn.lock().unwrap();
            return f(&conn);
        };
        READER_CONN.with(|slot| {
            let mut slot = slot.borrow_mut();
            let need_open = match slot.as_ref() {
                Some((p, _)) => p != path,
                None => true,
            };
            if need_open {
                let conn = Connection::open(path)?;
                Self::configure_reader(&conn)?;
                *slot = Some((path.clone(), conn));
            }
            f(&slot.as_ref().unwrap().1)
        })
    }

    fn manifest_hashes(
        bytes: Option<&[u8]>,
    ) -> Result<std::collections::HashSet<ChunkHash>, MetaError> {
        use constellation_fs_core::manifest::{ChunkInfo, Manifest};
        let Some(bytes) = bytes else {
            return Ok(std::collections::HashSet::new());
        };
        // Corrupt historical rows are reported by fsck. Deref bookkeeping
        // must not make replay of the surrounding metadata transaction fail.
        let Ok(manifest) = Manifest::decode(bytes) else {
            return Ok(std::collections::HashSet::new());
        };
        Ok(match manifest.chunks {
            ChunkInfo::Inline(hashes) => hashes.into_values().collect(),
            // The spill object is itself a bucket chunk and is the only hash
            // available without doing S3 I/O inside the SQLite transaction.
            // Its data hashes are protected by the spill while referenced and
            // become orphan-sweep candidates after the spill is collected.
            ChunkInfo::Spilled(hash) => [hash].into_iter().collect(),
        })
    }

    /// Is `hash` still reachable from a linked inode? Answered from the
    /// `chunk_ref` index: two indexed probes rather than a decode of every
    /// manifest in the namespace.
    ///
    /// The index is a *hint* in the safe direction. A stale row can only
    /// make a hash look live (delaying collection); GC never trusts this
    /// answer on its own — it re-derives the full live set from the
    /// manifests themselves before deleting anything.
    fn hash_is_live(conn: &Connection, hash: &ChunkHash) -> Result<bool, MetaError> {
        Ok(conn
            .query_row(
                "SELECT 1 FROM chunk_ref JOIN inode USING (ino)
                 WHERE chunk_ref.chunk_hash = ?1 AND inode.nlink > 0
                 LIMIT 1",
                params![hash.0.to_vec()],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Repopulate `chunk_ref` from the manifests currently in `inode`.
    /// Orphans (nlink = 0) are indexed too so that the reaping transition
    /// finds its rows to remove; `hash_is_live` filters them out via the
    /// join.
    fn rebuild_chunk_ref(conn: &Connection) -> Result<(), MetaError> {
        let mut stmt =
            conn.prepare("SELECT ino, manifest FROM inode WHERE manifest IS NOT NULL")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, Ino>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut insert =
            conn.prepare("INSERT OR IGNORE INTO chunk_ref (chunk_hash, ino) VALUES (?1, ?2)")?;
        for row in rows {
            let (ino, manifest) = row?;
            for hash in Self::manifest_hashes(Some(&manifest))? {
                insert.execute(params![hash.0.to_vec(), ino])?;
            }
        }
        Ok(())
    }

    /// One-time index build for DBs written before `chunk_ref` existed.
    /// Runs inside `init` so no code path can reach `hash_is_live` with an
    /// unpopulated index.
    fn backfill_chunk_ref_once(conn: &mut Connection) -> Result<(), MetaError> {
        let done: Option<String> = conn
            .query_row(
                "SELECT value FROM kv WHERE key = 'chunk_ref_index_v1'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if done.as_deref() == Some("1") {
            return Ok(());
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::rebuild_chunk_ref(&tx)?;
        tx.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('chunk_ref_index_v1', '1')",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Maintain the local indexed candidate set in the same transaction as
    /// a manifest/root transition. This is called by both journaled local
    /// mutations and foreign replay; bucket GC never needs to LIST chunks to
    /// discover ordinary reference garbage.
    pub(crate) fn track_manifest_transition(
        conn: &Connection,
        ino: Ino,
        old: Option<&[u8]>,
        new: Option<&[u8]>,
        seq: u64,
        unix_ms: i64,
    ) -> Result<(), MetaError> {
        let old = Self::manifest_hashes(old)?;
        let new = Self::manifest_hashes(new)?;
        for hash in new.difference(&old) {
            conn.execute(
                "INSERT OR IGNORE INTO chunk_ref (chunk_hash, ino) VALUES (?1, ?2)",
                params![hash.0.to_vec(), ino],
            )?;
            conn.execute(
                "DELETE FROM deref WHERE chunk_hash = ?1",
                params![hash.0.to_vec()],
            )?;
        }
        for hash in old.difference(&new) {
            conn.execute(
                "DELETE FROM chunk_ref WHERE chunk_hash = ?1 AND ino = ?2",
                params![hash.0.to_vec(), ino],
            )?;
            if !Self::hash_is_live(conn, hash)? {
                conn.execute(
                    "INSERT OR REPLACE INTO deref (chunk_hash, deref_seq, deref_unix_ms)
                     VALUES (?1, ?2, ?3)",
                    params![hash.0.to_vec(), seq, unix_ms],
                )?;
            }
        }
        Ok(())
    }

    fn next_deref_seq(conn: &Connection) -> Result<u64, MetaError> {
        Ok(
            conn.query_row("SELECT COALESCE(MAX(seq), 0) + 1 FROM journal", [], |row| {
                row.get(0)
            })?,
        )
    }

    /// One-time upgrade backfill. Existing live references cancel stale
    /// candidate rows; pre-upgrade abandoned uploads remain the orphan
    /// sweep's responsibility because no historical disappearance time can
    /// be reconstructed safely.
    pub fn backfill_deref_once(&self) -> Result<(), MetaError> {
        if self.kv_get("deref_backfill_v1")?.as_deref() == Some("1") {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut live = std::collections::HashSet::new();
        {
            let mut stmt =
                tx.prepare("SELECT manifest FROM inode WHERE nlink > 0 AND manifest IS NOT NULL")?;
            let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
            for row in rows {
                live.extend(Self::manifest_hashes(Some(&row?))?);
            }
        }
        for hash in live {
            tx.execute(
                "DELETE FROM deref WHERE chunk_hash = ?1",
                params![hash.0.to_vec()],
            )?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('deref_backfill_v1', '1')",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn deref_candidates(
        &self,
        older_than_unix_ms: i64,
    ) -> Result<Vec<(ChunkHash, u64, i64)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chunk_hash, deref_seq, deref_unix_ms FROM deref
             WHERE deref_unix_ms <= ?1 ORDER BY deref_unix_ms, chunk_hash",
        )?;
        let rows = stmt.query_map(params![older_than_unix_ms], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get(1)?, row.get(2)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (bytes, seq, at) = row?;
            let hash = ChunkHash(
                bytes
                    .try_into()
                    .map_err(|_| MetaError::Invalid("deref hash length".into()))?,
            );
            out.push((hash, seq, at));
        }
        Ok(out)
    }

    pub fn live_manifest_hashes(&self) -> Result<std::collections::HashSet<ChunkHash>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut out = std::collections::HashSet::new();
        let mut stmt =
            conn.prepare("SELECT manifest FROM inode WHERE nlink > 0 AND manifest IS NOT NULL")?;
        let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
        for row in rows {
            out.extend(Self::manifest_hashes(Some(&row?))?);
        }
        Ok(out)
    }

    pub fn live_manifests(&self) -> Result<Vec<Vec<u8>>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT manifest FROM inode WHERE nlink > 0 AND manifest IS NOT NULL")?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn clear_deref(&self, hash: &ChunkHash) -> Result<(), MetaError> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM deref WHERE chunk_hash = ?1",
            params![hash.0.to_vec()],
        )?;
        Ok(())
    }

    fn parse_hash_hex(value: &str) -> Result<ChunkHash, MetaError> {
        ChunkHash::from_hex(value)
            .ok_or_else(|| MetaError::Invalid(format!("invalid chunk hash {value:?}")))
    }

    /// Open (or create) a metadata DB. Creates the root inode on first use.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MetaError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path)?;
        Self::init(conn, Some(path))
    }

    /// In-memory store (tests).
    pub fn open_in_memory() -> Result<Self, MetaError> {
        Self::init(Connection::open_in_memory()?, None)
    }

    /// The backing file, if this store is not in-memory.
    pub(crate) fn db_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn init(mut conn: Connection, path: Option<std::path::PathBuf>) -> Result<Self, MetaError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;
        // Pre-partition DBs pick up the part column without a wipe.
        let _ = conn.execute(
            "ALTER TABLE journal ADD COLUMN part TEXT NOT NULL DEFAULT 'p0'",
            [],
        );
        conn.execute(
            "INSERT OR IGNORE INTO partition (id, root_ino) VALUES ('p0', ?1)",
            params![ROOT_INO],
        )?;
        conn.execute(
            "INSERT OR IGNORE INTO kv (key, value) VALUES ('next_part', '1')",
            [],
        )?;
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
        }
        // Present even on DBs restored from a checkpoint (which strips
        // node-local kv keys); `set_node_prefix` re-scopes it anyway.
        conn.execute(
            "INSERT OR IGNORE INTO kv (key, value) VALUES ('next_ino', ?1)",
            params![(ROOT_INO + 1).to_string()],
        )?;
        Self::backfill_chunk_ref_once(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            path,
        })
    }

    fn alloc_ino(conn: &Connection) -> Result<Ino, MetaError> {
        let next: String =
            conn.query_row("SELECT value FROM kv WHERE key = 'next_ino'", [], |r| {
                r.get(0)
            })?;
        let counter: u64 = next
            .parse()
            .map_err(|_| MetaError::Invalid("next_ino".into()))?;
        conn.execute(
            "UPDATE kv SET value = ?1 WHERE key = 'next_ino'",
            params![(counter + 1).to_string()],
        )?;
        Ok(Self::prefix_of(conn)? << INO_PREFIX_SHIFT | counter)
    }

    fn prefix_of(conn: &Connection) -> Result<u64, MetaError> {
        let v: Option<String> = conn
            .query_row("SELECT value FROM kv WHERE key = 'node_prefix'", [], |r| {
                r.get(0)
            })
            .optional()?;
        Ok(v.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// This node's ino prefix (0 until [`set_node_prefix`] is called).
    pub fn node_prefix(&self) -> Result<u64, MetaError> {
        Self::prefix_of(&self.conn.lock().unwrap())
    }

    /// Scope all future ino allocations to a cluster-unique node prefix
    /// (`ino = prefix << 40 | counter`), so concurrent nodes can never
    /// allocate colliding inode numbers. Changing the prefix restarts
    /// the counter: the new prefix is a fresh, empty ino namespace.
    pub fn set_node_prefix(&self, prefix: u64) -> Result<(), MetaError> {
        if prefix >= 1 << (64 - INO_PREFIX_SHIFT) {
            return Err(MetaError::Invalid(format!("node prefix {prefix}")));
        }
        let conn = self.conn.lock().unwrap();
        if Self::prefix_of(&conn)? == prefix {
            return Ok(());
        }
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('node_prefix', ?1)",
            params![prefix.to_string()],
        )?;
        conn.execute("UPDATE kv SET value = '1' WHERE key = 'next_ino'", [])?;
        Ok(())
    }

    /// The highest shared-log sequence this replica has applied or
    /// shipped (kv `applied_seq`; 0 when never synced).
    pub fn applied_seq(&self) -> Result<u64, MetaError> {
        Ok(self
            .kv_get("applied_seq")?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub fn set_applied_seq(&self, seq: u64) -> Result<(), MetaError> {
        self.kv_set("applied_seq", &seq.to_string())
    }

    /// Atomically ack journal records and record the log position that
    /// covers them: a crash can never separate the two (which would
    /// either wedge recovery or re-ship acked records).
    pub fn ack_journal_at(&self, upto_journal_seq: u64, applied_seq: u64) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM journal WHERE seq <= ?1",
            params![upto_journal_seq],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('applied_seq', ?1)",
            params![applied_seq.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Applied sequence for one partition (`applied_seq/<part>`). The
    /// legacy `applied_seq` key is p0 so pre-partition checkpoints still
    /// bootstrap.
    pub fn applied_seq_of(&self, part: &str) -> Result<u64, MetaError> {
        if let Some(v) = self.kv_get(&format!("applied_seq/{part}"))? {
            return Ok(v.parse().unwrap_or(0));
        }
        if part == "p0" {
            return self.applied_seq();
        }
        Ok(0)
    }

    pub fn set_applied_seq_of(&self, part: &str, seq: u64) -> Result<(), MetaError> {
        self.kv_set(&format!("applied_seq/{part}"), &seq.to_string())?;
        if part == "p0" {
            self.kv_set("applied_seq", &seq.to_string())?;
        }
        Ok(())
    }

    /// Atomically ack specific journal rows and record the log position
    /// that covers them for `part`.
    pub fn ack_journal_rows_at(
        &self,
        journal_seqs: &[u64],
        part: &str,
        applied_seq: u64,
    ) -> Result<(), MetaError> {
        if journal_seqs.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for seq in journal_seqs {
            tx.execute("DELETE FROM journal WHERE seq = ?1", params![seq])?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            params![format!("applied_seq/{part}"), applied_seq.to_string()],
        )?;
        if part == "p0" {
            tx.execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES ('applied_seq', ?1)",
                params![applied_seq.to_string()],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn kv_get(&self, key: &str) -> Result<Option<String>, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row("SELECT value FROM kv WHERE key = ?1", params![key], |r| {
                r.get(0)
            })
            .optional()?)
    }

    pub fn kv_set(&self, key: &str, value: &str) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    fn journal(conn: &Connection, record: &LogRecord) -> Result<(), MetaError> {
        let part = Self::part_for_record(conn, record)?;
        conn.execute(
            "INSERT INTO journal (record, part) VALUES (?1, ?2)",
            params![record.to_postcard()?, part],
        )?;
        Ok(())
    }

    /// Which stream a freshly journaled record belongs to. Computed from
    /// the mutated inode/dentry at journaling time so a later split
    /// cannot re-home an already-journaled op.
    fn part_for_record(conn: &Connection, rec: &LogRecord) -> Result<String, MetaError> {
        match rec {
            LogRecord::PartSplit { part, .. }
            | LogRecord::PartMerge {
                into_part: part, ..
            }
            | LogRecord::RenameXpartSrc { part, .. }
            | LogRecord::RenameXpartDst { part, .. } => Ok(part.clone()),
            LogRecord::RenameXpartAbort { .. }
            | LogRecord::SnapCreate { .. }
            | LogRecord::SnapDelete { .. }
            | LogRecord::Clone { .. } => Ok("p0".into()),
            LogRecord::Mkdir { parent, .. }
            | LogRecord::Create { parent, .. }
            | LogRecord::Symlink { parent, .. }
            | LogRecord::Mknod { parent, .. }
            | LogRecord::Link { parent, .. }
            | LogRecord::Unlink { parent, .. }
            | LogRecord::Rmdir { parent, .. }
            | LogRecord::Rename { parent, .. } => Self::partition_of_conn(conn, *parent),
            LogRecord::Setattr { ino, .. }
            | LogRecord::WriteManifest { ino, .. }
            | LogRecord::SetXattr { ino, .. }
            | LogRecord::RemoveXattr { ino, .. } => Self::partition_of_conn(conn, *ino),
        }
    }

    /// Journal a record onto a specific stream inside an existing
    /// transaction (split/merge/xpart halves, whose stream is not
    /// implied by a single inode).
    fn journal_on_tx(conn: &Connection, part: &str, record: &LogRecord) -> Result<(), MetaError> {
        conn.execute(
            "INSERT INTO journal (record, part) VALUES (?1, ?2)",
            params![record.to_postcard()?, part],
        )?;
        match record {
            LogRecord::PartSplit {
                at_ino, new_part, ..
            } => {
                conn.execute(
                    "INSERT OR REPLACE INTO partition (id, root_ino) VALUES (?1, ?2)",
                    params![new_part, at_ino],
                )?;
                conn.execute("DELETE FROM kv WHERE key LIKE 'part_of/%'", [])?;
            }
            LogRecord::PartMerge { part: child, .. } => {
                conn.execute("DELETE FROM partition WHERE id = ?1", params![child])?;
                conn.execute("DELETE FROM kv WHERE key LIKE 'part_of/%'", [])?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Journal a record onto a specific stream (split/merge/xpart
    /// halves, whose stream is not implied by a single inode).
    pub fn journal_on(&self, part: &str, record: &LogRecord) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        Self::journal_on_tx(&tx, part, record)?;
        tx.commit()?;
        Ok(())
    }

    /// Walk up the dentry tree to the nearest partition root. Root of
    /// `/` is always `p0`. Cached in `kv` as `part_of/<ino>` and
    /// invalidated on split/merge/rename that moves the inode.
    pub fn partition_of(&self, ino: Ino) -> Result<String, MetaError> {
        Self::partition_of_conn(&self.conn.lock().unwrap(), ino)
    }

    fn partition_of_conn(conn: &Connection, ino: Ino) -> Result<String, MetaError> {
        let cache_key = format!("part_of/{ino}");
        if let Some(cached) = conn
            .query_row(
                "SELECT value FROM kv WHERE key = ?1",
                params![cache_key],
                |r| r.get::<_, String>(0),
            )
            .optional()?
        {
            return Ok(cached);
        }
        let part = Self::resolve_partition(conn, ino)?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
            params![cache_key, &part],
        )?;
        Ok(part)
    }

    fn resolve_partition(conn: &Connection, mut ino: Ino) -> Result<String, MetaError> {
        loop {
            if let Some(id) = conn
                .query_row(
                    "SELECT id FROM partition WHERE root_ino = ?1",
                    params![ino],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
            {
                return Ok(id);
            }
            if ino == ROOT_INO {
                return Ok("p0".into());
            }
            ino = conn
                .query_row(
                    "SELECT parent FROM dentry WHERE ino = ?1 LIMIT 1",
                    params![ino],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(ROOT_INO);
        }
    }

    pub fn invalidate_part_cache(conn: &Connection, ino: Ino) -> Result<(), MetaError> {
        conn.execute(
            "DELETE FROM kv WHERE key = ?1",
            params![format!("part_of/{ino}")],
        )?;
        Ok(())
    }

    pub fn partitions(&self) -> Result<Vec<(String, Ino)>, MetaError> {
        self.with_reader(|conn| {
            let mut stmt = conn.prepare("SELECT id, root_ino FROM partition ORDER BY id")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            Ok(rows.collect::<Result<_, _>>()?)
        })
    }

    /// Path of a partition root, for the control API. `/` for p0.
    pub fn path_of(&self, ino: Ino) -> Result<String, MetaError> {
        if ino == ROOT_INO {
            return Ok("/".into());
        }
        self.with_reader(|conn| {
            let mut parts = Vec::new();
            let mut cur = ino;
            while cur != ROOT_INO {
                let row: Option<(Ino, String)> = conn
                    .query_row(
                        "SELECT parent, name FROM dentry WHERE ino = ?1 LIMIT 1",
                        params![cur],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()?;
                let Some((parent, name)) = row else {
                    break;
                };
                parts.push(name);
                cur = parent;
            }
            parts.reverse();
            Ok(format!("/{}", parts.join("/")))
        })
    }

    /// Allocate the next partition id (see [`Self::alloc_part_id`]).
    pub fn next_part_id(&self) -> Result<String, MetaError> {
        Self::alloc_part_id(&self.conn.lock().unwrap())
    }

    /// Resolve an absolute path to its inode. `/` is the root.
    pub fn resolve_path(&self, path: &str) -> Result<Option<Ino>, MetaError> {
        self.with_reader(|conn| {
            let mut cur = ROOT_INO;
            for part in path.split('/').filter(|s| !s.is_empty()) {
                match Self::dentry_ino(conn, cur, part)? {
                    Some(next) => cur = next,
                    None => return Ok(None),
                }
            }
            Ok(Some(cur))
        })
    }

    /// Fetch one directory's complete snapshot rows with one indexed join.
    pub fn snapshot_children(&self, parent: Ino) -> Result<Vec<SnapshotNode>, MetaError> {
        self.with_reader(|conn| {
            let mut nodes = {
                let mut stmt = conn.prepare(
                    "SELECT d.name, i.ino, i.kind, i.size, i.mode, i.uid, i.gid, i.nlink,
                            i.atime_ns, i.mtime_ns, i.ctime_ns, i.rdev, i.symlink_target, i.manifest
                     FROM dentry d JOIN inode i ON i.ino = d.ino
                     WHERE d.parent = ?1 ORDER BY d.name",
                )?;
                let rows = stmt.query_map(params![parent], |row| {
                    let kind: u8 = row.get(2)?;
                    Ok(SnapshotNode {
                        name: row.get(0)?,
                        attr: FileAttr {
                            ino: row.get(1)?,
                            kind: InodeKind::from_u8(kind)
                                .expect("database kind is validated on insert"),
                            size: row.get::<_, i64>(3)? as u64,
                            mode: row.get(4)?,
                            uid: row.get(5)?,
                            gid: row.get(6)?,
                            nlink: row.get(7)?,
                            atime_ns: row.get(8)?,
                            mtime_ns: row.get(9)?,
                            ctime_ns: row.get(10)?,
                            rdev: row.get::<_, i64>(11)? as u64,
                        },
                        target: row.get(12)?,
                        manifest: row.get(13)?,
                        xattrs: Vec::new(),
                    })
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            for node in &mut nodes {
                node.xattrs = Self::xattrs_by_ino(conn, node.attr.ino)?;
            }
            Ok(nodes)
        })
    }

    /// Make a snapshot visible locally and journal its immutable root.
    pub fn record_snapshot(&self, row: &SnapshotRow) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO snapshot (id, path, name, root_hash, created_unix_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                row.id,
                row.path,
                row.name,
                row.root_hash,
                row.created_unix_ms
            ],
        )?;
        tx.execute(
            "DELETE FROM deref WHERE chunk_hash = ?1",
            params![Self::parse_hash_hex(&row.root_hash)?.0.to_vec()],
        )?;
        Self::journal(
            &tx,
            &LogRecord::SnapCreate {
                id: row.id.clone(),
                path: row.path.clone(),
                name: row.name.clone(),
                root_hash: row.root_hash.clone(),
                created_unix_ms: row.created_unix_ms,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_snapshot(&self, path: &str, name: &str) -> Result<bool, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT id, root_hash FROM snapshot WHERE path = ?1 AND name = ?2",
                params![path, name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((id, root_hash)) = row else {
            return Ok(false);
        };
        tx.execute("DELETE FROM snapshot WHERE id = ?1", params![id])?;
        Self::journal(
            &tx,
            &LogRecord::SnapDelete {
                id,
                path: path.to_string(),
                name: name.to_string(),
            },
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO deref (chunk_hash, deref_seq, deref_unix_ms)
             VALUES (?1, ?2, ?3)",
            params![
                Self::parse_hash_hex(&root_hash)?.0.to_vec(),
                Self::next_deref_seq(&tx)?,
                now_unix_ms()
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn snapshots(&self, path: Option<&str>) -> Result<Vec<SnapshotRow>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let sql = if path.is_some() {
            "SELECT id, path, name, root_hash, created_unix_ms FROM snapshot
             WHERE path = ?1 ORDER BY path, name"
        } else {
            "SELECT id, path, name, root_hash, created_unix_ms FROM snapshot
             ORDER BY path, name"
        };
        let mut stmt = conn.prepare(sql)?;
        let map = |row: &rusqlite::Row<'_>| {
            Ok(SnapshotRow {
                id: row.get(0)?,
                path: row.get(1)?,
                name: row.get(2)?,
                root_hash: row.get(3)?,
                created_unix_ms: row.get(4)?,
            })
        };
        let rows = match path {
            Some(path) => stmt.query_map(params![path], map)?,
            None => stmt.query_map([], map)?,
        };
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Eager metadata clone. Data and manifest blobs stay content-addressed;
    /// only ordinary inode/dentry rows are copied.
    pub fn eager_clone(
        &self,
        source_path: &str,
        snapshot: &str,
        root_hash: &str,
        destination: &str,
        specs: &[CloneSpec],
    ) -> Result<Ino, MetaError> {
        if specs.is_empty() || specs[0].parent_index.is_some() {
            return Err(MetaError::Invalid("clone tree has no root".into()));
        }
        let mut parts: Vec<&str> = destination.split('/').filter(|p| !p.is_empty()).collect();
        let name = parts
            .pop()
            .ok_or_else(|| MetaError::Invalid("cannot clone over /".into()))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut parent = ROOT_INO;
        for part in parts {
            parent = Self::dentry_ino(&tx, parent, part)?.ok_or(MetaError::NoEntry)?;
        }
        if Self::dentry_ino(&tx, parent, name)?.is_some() {
            return Err(MetaError::Exists);
        }
        let mut inos = Vec::with_capacity(specs.len());
        let mut nodes = Vec::with_capacity(specs.len());
        for (index, spec) in specs.iter().enumerate() {
            let ino = Self::alloc_ino(&tx)?;
            let node_parent = match spec.parent_index {
                None => parent,
                Some(parent_index) if parent_index < index => inos[parent_index],
                _ => {
                    return Err(MetaError::Invalid(
                        "clone nodes are not parent-first".into(),
                    ))
                }
            };
            let node_name = if index == 0 { name } else { &spec.name };
            tx.execute(
                "INSERT INTO inode (ino, kind, size, mode, uid, gid, nlink, atime_ns,
                    mtime_ns, ctime_ns, rdev, manifest, symlink_target)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?8, 0, ?9, ?10)",
                params![
                    ino,
                    spec.kind.as_u8(),
                    spec.size as i64,
                    spec.mode,
                    spec.uid,
                    spec.gid,
                    if spec.kind == InodeKind::Dir { 2 } else { 1 },
                    spec.mtime_ns,
                    spec.manifest,
                    spec.target
                ],
            )?;
            tx.execute(
                "INSERT INTO dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
                params![node_parent, node_name, ino],
            )?;
            for (xattr_name, value) in &spec.xattrs {
                tx.execute(
                    "INSERT INTO xattr (ino, name, value) VALUES (?1, ?2, ?3)",
                    params![ino, xattr_name, value],
                )?;
            }
            Self::track_manifest_transition(
                &tx,
                ino,
                None,
                spec.manifest.as_deref(),
                Self::next_deref_seq(&tx)?,
                spec.mtime_ns / 1_000_000,
            )?;
            if spec.kind == InodeKind::Dir {
                tx.execute(
                    "UPDATE inode SET nlink = nlink + 1 WHERE ino = ?1",
                    params![node_parent],
                )?;
            }
            nodes.push(CloneNode {
                parent: node_parent,
                name: node_name.to_string(),
                ino,
                kind: spec.kind.as_u8(),
                mode: spec.mode,
                uid: spec.uid,
                gid: spec.gid,
                size: spec.size,
                mtime_ns: spec.mtime_ns,
                rdev: 0,
                target: spec.target.clone(),
                manifest: spec.manifest.clone(),
                xattrs: spec.xattrs.clone(),
            });
            inos.push(ino);
        }
        Self::journal(
            &tx,
            &LogRecord::Clone {
                source_path: source_path.to_string(),
                snapshot: snapshot.to_string(),
                root_hash: root_hash.to_string(),
                nodes,
            },
        )?;
        tx.commit()?;
        Ok(inos[0])
    }

    /// Every manifest under `ino` (inclusive), as `(ino, manifest_bytes,
    /// size)`. Used to compute a pin's footprint and to fetch its chunks.
    ///
    /// Walks the dentry tree rather than trusting a stored aggregate: the
    /// replica is the authority for what a subtree currently contains,
    /// and a stale aggregate would let a pin over-commit the cache.
    pub fn subtree_manifests(&self, ino: Ino) -> Result<Vec<(Ino, Vec<u8>, u64)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut out = Vec::new();
        let mut stack = vec![ino];
        let mut seen = std::collections::HashSet::new();
        while let Some(cur) = stack.pop() {
            if !seen.insert(cur) {
                continue; // hard links can reach one inode twice
            }
            let row: Option<(Option<Vec<u8>>, i64, u8)> = conn
                .query_row(
                    "SELECT manifest, size, kind FROM inode WHERE ino = ?1",
                    params![cur],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((manifest, size, kind)) = row {
                if let Some(m) = manifest {
                    out.push((cur, m, size as u64));
                }
                if kind == InodeKind::Dir.as_u8() {
                    let mut stmt = conn.prepare("SELECT ino FROM dentry WHERE parent = ?1")?;
                    let kids = stmt.query_map(params![cur], |r| r.get::<_, Ino>(0))?;
                    for k in kids {
                        stack.push(k?);
                    }
                }
            }
        }
        Ok(out)
    }

    /// Record a pin. Pins are **node-local**: each node decides what it
    /// keeps resident, so they are deliberately not replicated through
    /// the log.
    pub fn add_pin(&self, path: &str, ino: Ino) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO pin (path, ino, pinned_at) VALUES (?1, ?2, ?3)",
            params![path, ino, now_ns()],
        )?;
        Ok(())
    }

    /// Forget a pin. Returns whether it existed.
    pub fn remove_pin(&self, path: &str) -> Result<bool, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.execute("DELETE FROM pin WHERE path = ?1", params![path])? > 0)
    }

    pub fn pins(&self) -> Result<Vec<(String, Ino)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT path, ino FROM pin ORDER BY path")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Is `ino` inside any pinned subtree? Walks up to the root, so a
    /// file created under a pin is covered without re-pinning.
    pub fn pinned_ancestor(&self, ino: Ino) -> Result<Option<String>, MetaError> {
        let pins = self.pins()?;
        if pins.is_empty() {
            return Ok(None);
        }
        let conn = self.conn.lock().unwrap();
        let mut cur = ino;
        loop {
            if let Some((path, _)) = pins.iter().find(|(_, pino)| *pino == cur) {
                return Ok(Some(path.clone()));
            }
            if cur == ROOT_INO {
                return Ok(None);
            }
            match conn
                .query_row(
                    "SELECT parent FROM dentry WHERE ino = ?1 LIMIT 1",
                    params![cur],
                    |r| r.get::<_, Ino>(0),
                )
                .optional()?
            {
                Some(p) => cur = p,
                None => return Ok(None),
            }
        }
    }

    /// Parent of `ino` in the dentry tree, if any.
    pub fn parent_of(&self, ino: Ino) -> Result<Option<Ino>, MetaError> {
        if ino == ROOT_INO {
            return Ok(None);
        }
        let conn = self.conn.lock().unwrap();
        Ok(conn
            .query_row(
                "SELECT parent FROM dentry WHERE ino = ?1 LIMIT 1",
                params![ino],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Allocate the next partition id. Ids are **node-scoped**
    /// (`p<node>_<n>`, or plain `p<n>` on the genesis prefix 0) because
    /// the counter is local: two nodes splitting different directories at
    /// the same time must never mint the same id, or the replicated
    /// partition map would disagree per node.
    pub fn alloc_part_id(conn: &Connection) -> Result<String, MetaError> {
        let next: String = conn
            .query_row("SELECT value FROM kv WHERE key = 'next_part'", [], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or_else(|| "1".into());
        let n: u64 = next
            .parse()
            .map_err(|_| MetaError::Invalid("next_part".into()))?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('next_part', ?1)",
            params![(n + 1).to_string()],
        )?;
        let prefix = Self::prefix_of(conn)?;
        if prefix == 0 {
            Ok(format!("p{n}"))
        } else {
            Ok(format!("p{prefix}_{n}"))
        }
    }

    pub fn park_xpart(&self, txid: u64, half: &str, rec: &LogRecord) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO xpart_pending (txid, half, record) VALUES (?1, ?2, ?3)",
            params![txid, half, rec.to_postcard()?],
        )?;
        Ok(())
    }

    pub fn unpark_xpart(&self, txid: u64) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM xpart_pending WHERE txid = ?1", params![txid])?;
        Ok(())
    }

    pub fn mark_xpart_dst(&self, txid: u64) -> Result<(), MetaError> {
        self.kv_set(&format!("xpart_dst/{txid}"), "1")
    }

    pub fn xpart_dst_seen(&self, txid: u64) -> Result<bool, MetaError> {
        Ok(self.kv_get(&format!("xpart_dst/{txid}"))?.is_some())
    }

    /// Parked cross-partition rename halves: `(txid, half)` where half
    /// is `"src"` or `"dst"`.
    pub fn pending_xparts(&self) -> Result<Vec<(u64, String, LogRecord)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT txid, half, record FROM xpart_pending")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Vec<u8>>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (txid, half, bytes) = row?;
            out.push((txid, half, LogRecord::from_postcard(&bytes)?));
        }
        Ok(out)
    }

    pub fn clear_pending_xpart(&self, txid: u64) -> Result<(), MetaError> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM xpart_pending WHERE txid = ?1", params![txid])?;
        Ok(())
    }

    /// Peek the journal grouped by partition, preserving per-partition
    /// order. Does not drain.
    pub fn take_journal_grouped(
        &self,
        max_per_part: usize,
    ) -> Result<Vec<(String, JournalBatch)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT seq, record, part FROM journal ORDER BY part, seq")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, u64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?;
        let mut grouped: Vec<(String, JournalBatch)> = Vec::new();
        for row in rows {
            let (seq, bytes, part) = row?;
            let rec = LogRecord::from_postcard(&bytes)?;
            match grouped.last_mut() {
                Some((p, recs)) if p == &part => {
                    if recs.len() < max_per_part {
                        recs.push((seq, rec));
                    }
                }
                _ => grouped.push((part, vec![(seq, rec)])),
            }
        }
        Ok(grouped)
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

    fn xattrs_by_ino(conn: &Connection, ino: Ino) -> Result<Vec<(String, Vec<u8>)>, MetaError> {
        let mut stmt =
            conn.prepare_cached("SELECT name, value FROM xattr WHERE ino = ?1 ORDER BY name")?;
        let rows = stmt.query_map(params![ino], |row| Ok((row.get(0)?, row.get(1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
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

    /// Cross-partition rename: apply locally in one transaction and
    /// journal a linked pair (`RenameXpartSrc` on `src_part`,
    /// `RenameXpartDst` on `dst_part`) sharing a fresh txid. The
    /// mutating node must already hold both leases (caller acquires in
    /// canonical partition-id order).
    pub fn rename_xpart(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
        src_part: &str,
        dst_part: &str,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // A POSIX no-op (both names are the same inode) journals nothing:
        // there is no namespace change to replicate.
        let Some((ino, t)) = Self::rename_in_tx(&tx, parent, name, new_parent, new_name)? else {
            tx.commit()?;
            return Ok(());
        };
        let txid = Self::alloc_xpart_txid(&tx)?;
        Self::journal_on_tx(
            &tx,
            src_part,
            &LogRecord::RenameXpartSrc {
                txid,
                part: src_part.into(),
                from_parent: parent,
                name: name.into(),
                ino,
                time_ns: t,
            },
        )?;
        Self::journal_on_tx(
            &tx,
            dst_part,
            &LogRecord::RenameXpartDst {
                txid,
                part: dst_part.into(),
                to_parent: new_parent,
                new_name: new_name.into(),
                ino,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn alloc_xpart_txid(conn: &Connection) -> Result<u64, MetaError> {
        let n: String = conn
            .query_row("SELECT value FROM kv WHERE key = 'next_xpart'", [], |r| {
                r.get(0)
            })
            .optional()?
            .unwrap_or_else(|| "1".into());
        let id: u64 = n
            .parse()
            .map_err(|_| MetaError::Invalid("next_xpart".into()))?;
        conn.execute(
            "INSERT OR REPLACE INTO kv (key, value) VALUES ('next_xpart', ?1)",
            params![(id + 1).to_string()],
        )?;
        Ok(id)
    }

    /// The POSIX rename body, inside a caller-owned transaction. Returns
    /// `Some((ino, time_ns))` when the namespace actually changed, or
    /// `None` for the same-inode (hard link) no-op, which journals
    /// nothing. Shared by `rename` and `rename_xpart` so the two can
    /// never diverge and so the xpart pair commits atomically with the
    /// mutation it describes.
    fn rename_in_tx(
        tx: &Connection,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<Option<(Ino, i64)>, MetaError> {
        let ino = Self::dentry_ino(tx, parent, name)?.ok_or(MetaError::NoEntry)?;
        let src = Self::attr_by_ino(tx, ino)?.ok_or(MetaError::NoEnt(ino))?;
        Self::require_dir(tx, new_parent)?;
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
        if let Some(existing) = Self::dentry_ino(tx, new_parent, new_name)? {
            // Same inode: rename is a no-op that succeeds.
            if existing == ino {
                return Ok(None);
            }
            let ex = Self::attr_by_ino(tx, existing)?.ok_or(MetaError::NoEnt(existing))?;
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
                tx.execute("DELETE FROM xattr WHERE ino = ?1", params![existing])?;
                tx.execute("DELETE FROM inode WHERE ino = ?1", params![existing])?;
                // The removed dir's ".." reference to new_parent is gone.
                tx.execute(
                    "UPDATE inode SET nlink = nlink - 1 WHERE ino = ?1",
                    params![new_parent],
                )?;
            } else {
                if ex.nlink == 1 {
                    tx.execute("DELETE FROM xattr WHERE ino = ?1", params![existing])?;
                }
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
        Self::invalidate_part_cache(tx, ino)?;
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
        for p in [parent, new_parent] {
            tx.execute(
                "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?2 WHERE ino = ?1",
                params![p, t],
            )?;
        }
        Ok(Some((ino, t)))
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

    pub fn child_ino(&self, parent: Ino, name: &str) -> Result<Option<Ino>, MetaError> {
        Self::dentry_ino(&self.conn.lock().unwrap(), parent, name)
    }

    pub fn applied_vector(&self) -> Result<std::collections::BTreeMap<String, u64>, MetaError> {
        let mut out = std::collections::BTreeMap::new();
        for (id, _) in self.partitions()? {
            out.insert(id.clone(), self.applied_seq_of(&id)?);
        }
        Ok(out)
    }

    pub fn persist_epoch(
        &self,
        epoch_id: &str,
        members: &[u64],
        base: &std::collections::BTreeMap<String, u64>,
        promised_at: i64,
        state: &str,
    ) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO epochs (epoch_id, members, base, promised_at, state)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                epoch_id,
                serde_json::to_string(members)?,
                serde_json::to_string(base)?,
                promised_at,
                state,
            ],
        )?;
        Ok(())
    }

    pub fn load_open_epoch(&self) -> Result<Option<EpochRow>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(String, String, String, i64, String)> = conn
            .query_row(
                "SELECT epoch_id, members, base, promised_at, state FROM epochs
                 WHERE state != 'closed' ORDER BY promised_at DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let Some((id, members, base, at, state)) = row else {
            return Ok(None);
        };
        Ok(Some((
            id,
            serde_json::from_str(&members)?,
            serde_json::from_str(&base)?,
            at,
            state,
        )))
    }

    pub fn set_epoch_state(&self, epoch_id: &str, state: &str) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE epochs SET state = ?1 WHERE epoch_id = ?2",
            params![state, epoch_id],
        )?;
        Ok(())
    }

    /// Journal rows not yet given a reintegration disposition.
    ///
    /// This deserializes every unmarked `LogRecord`. Callers that only
    /// need a count (status, leave guards) must use
    /// [`Self::unmarked_journal_len`] instead.
    pub fn unmarked_journal(&self) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT j.seq, j.record FROM journal j
             LEFT JOIN reintegration r ON r.journal_seq = j.seq
             WHERE r.journal_seq IS NULL
             ORDER BY j.seq",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, bytes) = row?;
            out.push((seq, LogRecord::from_postcard(&bytes)?));
        }
        Ok(out)
    }

    /// Count of unmarked journal rows. Does not decode `record` postcard.
    pub fn unmarked_journal_len(&self) -> Result<u64, MetaError> {
        self.with_reader(|conn| {
            Ok(conn.query_row(
                "SELECT COUNT(*) FROM journal j
                 LEFT JOIN reintegration r ON r.journal_seq = j.seq
                 WHERE r.journal_seq IS NULL",
                [],
                |r| r.get(0),
            )?)
        })
    }

    pub fn unmarked_journal_parts(&self) -> Result<Vec<String>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT j.part
             FROM journal j
             LEFT JOIN reintegration r ON r.journal_seq = j.seq
             WHERE r.journal_seq IS NULL
             ORDER BY j.part",
        )?;
        let rows = stmt.query_map([], |row| row.get(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Same-tx: mark a stranded journal row's disposition, drop it, and
    /// optionally re-journal a replacement (clean replay). Crash-safe
    /// resume starts at the first unmarked remaining row.
    pub fn reintegrate_commit(
        &self,
        orig_seq: u64,
        disposition: &str,
        detail: &str,
        rejournal: Option<&LogRecord>,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO reintegration (journal_seq, disposition, detail)
             VALUES (?1, ?2, ?3)",
            params![orig_seq, disposition, detail],
        )?;
        if inserted == 0 {
            tx.commit()?;
            return Ok(());
        }
        tx.execute("DELETE FROM journal WHERE seq = ?1", params![orig_seq])?;
        if let Some(rec) = rejournal {
            Self::journal(&tx, rec)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn reintegration_conflict_count(&self) -> Result<u64, MetaError> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM reintegration WHERE disposition = 'conflict'",
            [],
            |r| r.get(0),
        )?)
    }

    /// Atomically replace the local namespace with a reconciled side
    /// replica, mark every stranded row, and journal the reconciled
    /// output. This is the crash-safety boundary for reintegration:
    /// after commit the namespace and disposition table cannot disagree.
    pub fn commit_reintegration_batch(
        &self,
        reconciled_db: &Path,
        dispositions: &[(u64, String, String)],
        output: &[LogRecord],
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        conn.execute(
            "ATTACH DATABASE ?1 AS reintegrated",
            params![reconciled_db.to_string_lossy().as_ref()],
        )?;
        let result: Result<(), MetaError> = (|| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute("DELETE FROM dentry", [])?;
            tx.execute("DELETE FROM inode", [])?;
            tx.execute("DELETE FROM xattr", [])?;
            tx.execute("INSERT INTO inode SELECT * FROM reintegrated.inode", [])?;
            tx.execute("INSERT INTO dentry SELECT * FROM reintegrated.dentry", [])?;
            tx.execute("INSERT INTO xattr SELECT * FROM reintegrated.xattr", [])?;
            // The namespace was replaced wholesale, so every reverse-index
            // row is about inodes that no longer exist.
            tx.execute("DELETE FROM chunk_ref", [])?;
            Self::rebuild_chunk_ref(&tx)?;
            tx.execute("DELETE FROM partition", [])?;
            tx.execute(
                "INSERT INTO partition SELECT * FROM reintegrated.partition",
                [],
            )?;
            tx.execute("DELETE FROM xpart_pending", [])?;
            tx.execute(
                "INSERT INTO xpart_pending SELECT * FROM reintegrated.xpart_pending",
                [],
            )?;
            tx.execute(
                "DELETE FROM kv WHERE key = 'applied_seq' OR key LIKE 'applied_seq/%'",
                [],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO kv
                 SELECT key, value FROM reintegrated.kv
                 WHERE key = 'applied_seq' OR key LIKE 'applied_seq/%'",
                [],
            )?;
            for (seq, disposition, detail) in dispositions {
                tx.execute(
                    "INSERT OR IGNORE INTO reintegration
                     (journal_seq, disposition, detail) VALUES (?1, ?2, ?3)",
                    params![seq, disposition, detail],
                )?;
                tx.execute("DELETE FROM journal WHERE seq = ?1", params![seq])?;
            }
            for record in output {
                Self::journal(&tx, record)?;
            }
            tx.execute("DELETE FROM kv WHERE key LIKE 'part_of/%'", [])?;
            tx.commit()?;
            Ok(())
        })();
        let _ = conn.execute("DETACH DATABASE reintegrated", []);
        result
    }

    /// Raw connection access for same-crate extensions (replay).
    pub(crate) fn raw(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap()
    }

    /// The `set_manifest` body, inside a caller-owned transaction. Shared
    /// by the plain trait method (applying a foreign/reintegrated record
    /// — that content's upload is another node's responsibility, so it
    /// never touches `pending_upload`) and [`Self::set_manifest_dirty`]
    /// (the local write path, which does).
    fn set_manifest_tx(
        tx: &Connection,
        ino: Ino,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let t = now_ns();
        let base_manifest: Option<Vec<u8>> = tx
            .query_row(
                "SELECT manifest FROM inode WHERE ino = ?1",
                params![ino],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let n = tx.execute(
            "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4 WHERE ino = ?1",
            params![ino, manifest, size as i64, t],
        )?;
        if n == 0 {
            return Err(MetaError::NoEnt(ino));
        }
        Self::track_manifest_transition(
            tx,
            ino,
            base_manifest.as_deref(),
            Some(manifest),
            Self::next_deref_seq(tx)?,
            t / 1_000_000,
        )?;
        Self::journal(
            tx,
            &LogRecord::WriteManifest {
                ino,
                base_manifest,
                manifest: manifest.to_vec(),
                size,
                time_ns: t,
            },
        )
    }

    /// Local-write variant of `set_manifest` (used only by the FUSE
    /// flush path, plan 05a step 1): durably enrolls `dirty_hashes` in
    /// `pending_upload` in the **same transaction** as the manifest
    /// commit and journal record, so a crash between "the cache has the
    /// bytes" and "S3 has the bytes" cannot silently drop the upload —
    /// this is the table `pending_uploads`/`ack_upload` drain, which
    /// replaces `DiskCache::dirty_chunks()` as `upload_dirty_chunks`'s
    /// source of truth. `hash, ino` is a pair (not just `hash`) because
    /// the same content can be dirty under two different inodes at
    /// once; acking one must not disturb the other's row.
    pub fn set_manifest_dirty(
        &self,
        ino: Ino,
        manifest: &[u8],
        size: u64,
        dirty_hashes: &[ChunkHash],
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::set_manifest_tx(&tx, ino, manifest, size)?;
        for h in dirty_hashes {
            tx.execute(
                "INSERT OR IGNORE INTO pending_upload (hash, ino) VALUES (?1, ?2)",
                params![h.0.to_vec(), ino],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn allocate_ino(&self) -> Result<Ino, MetaError> {
        Self::alloc_ino(&self.conn.lock().unwrap())
    }

    pub fn max_journal_seq(&self) -> Result<u64, MetaError> {
        Ok(self.conn.lock().unwrap().query_row(
            "SELECT COALESCE(MAX(seq), 0) FROM journal",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn peek_journal_after(&self, after_seq: u64) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT seq, record FROM journal WHERE seq > ?1 ORDER BY seq")?;
        let rows = stmt.query_map(params![after_seq], |row| {
            Ok((row.get::<_, u64>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, bytes) = row?;
            out.push((seq, LogRecord::from_postcard(&bytes)?));
        }
        Ok(out)
    }

    pub fn mkdir_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        if Self::attr_by_ino(&tx, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
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

    pub fn create_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        if Self::attr_by_ino(&tx, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
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

    pub fn symlink_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        if Self::attr_by_ino(&tx, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
        let t = now_ns();
        let attr = FileAttr::new_symlink(ino, uid, gid, t, target.len() as u64);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode (ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns, symlink_target)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7, ?7, ?8)",
            params![
                ino,
                InodeKind::Symlink.as_u8(),
                attr.size,
                attr.mode,
                uid,
                gid,
                t,
                target
            ],
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
    pub fn mknod_at(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
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
        if Self::attr_by_ino(&tx, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
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

    pub fn set_manifest_with_base(
        &self,
        ino: Ino,
        base_manifest: Option<&[u8]>,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<Vec<u8>> = tx
            .query_row(
                "SELECT manifest FROM inode WHERE ino = ?1",
                params![ino],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        // Optimistic concurrency. `manifest` is a *whole-file* image
        // composed on `base_manifest`, so installing it over a manifest
        // someone else has moved on from would drop their chunks with no
        // error — the disjoint-write lost update. Reject instead and let
        // the caller rebase onto what is current.
        //
        // Two cases are not conflicts: a caller that passes no base is
        // not making a claim about the predecessor, and a manifest that
        // already matches is a replay of a commit that landed (the rule
        // reintegration applies to the same record).
        if let (Some(base), Some(cur)) = (base_manifest, current.as_deref()) {
            if cur != base && cur != manifest {
                return Err(MetaError::Conflict);
            }
        }
        let journal_base = base_manifest.map(Vec::from).or_else(|| current.clone());
        let t = now_ns();
        let n = tx.execute(
            "UPDATE inode SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4 WHERE ino = ?1",
            params![ino, manifest, size as i64, t],
        )?;
        if n == 0 {
            return Err(MetaError::NoEnt(ino));
        }
        Self::track_manifest_transition(
            &tx,
            ino,
            current.as_deref(),
            Some(manifest),
            Self::next_deref_seq(&tx)?,
            t / 1_000_000,
        )?;
        Self::journal(
            &tx,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: journal_base,
                manifest: manifest.to_vec(),
                size,
                time_ns: t,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn publish_file(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
        mtime_ns: i64,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::require_dir(&tx, parent)?;
        if let Some(old_ino) = Self::dentry_ino(&tx, parent, name)? {
            let old_attr = Self::attr_by_ino(&tx, old_ino)?.ok_or(MetaError::NoEnt(old_ino))?;
            if old_attr.kind == InodeKind::File {
                let old_manifest: Option<Vec<u8>> = tx.query_row(
                    "SELECT manifest FROM inode WHERE ino = ?1",
                    params![old_ino],
                    |row| row.get(0),
                )?;
                if old_manifest.as_deref() == Some(manifest) {
                    return Ok(());
                }
            } else if old_attr.kind == InodeKind::Dir {
                return Err(MetaError::IsDir);
            }
            let t = now_ns();
            tx.execute(
                "DELETE FROM dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
            )?;
            tx.execute(
                "UPDATE inode SET nlink = nlink - 1, ctime_ns = ?2 WHERE ino = ?1",
                params![old_ino, t],
            )?;
            if old_attr.nlink == 1 {
                tx.execute("DELETE FROM xattr WHERE ino = ?1", params![old_ino])?;
                let old_manifest: Option<Vec<u8>> = tx.query_row(
                    "SELECT manifest FROM inode WHERE ino = ?1",
                    params![old_ino],
                    |row| row.get(0),
                )?;
                Self::track_manifest_transition(
                    &tx,
                    old_ino,
                    old_manifest.as_deref(),
                    None,
                    Self::next_deref_seq(&tx)?,
                    t / 1_000_000,
                )?;
            }
            Self::journal(
                &tx,
                &LogRecord::Unlink {
                    parent,
                    name: name.into(),
                    time_ns: t,
                },
            )?;
        }
        if Self::attr_by_ino(&tx, ino)?.is_some() {
            return Err(MetaError::Exists);
        }
        let ctime_ns = now_ns();
        let attr = FileAttr::new_file(ino, mode, uid, gid, mtime_ns);
        Self::insert_dentry(&tx, parent, name, ino)?;
        tx.execute(
            "INSERT INTO inode
             (ino, kind, size, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns, manifest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7, ?8, ?9)",
            params![
                ino,
                InodeKind::File.as_u8(),
                size as i64,
                attr.mode,
                uid,
                gid,
                mtime_ns,
                ctime_ns,
                manifest
            ],
        )?;
        tx.execute(
            "UPDATE inode SET mtime_ns = ?2, ctime_ns = ?3 WHERE ino = ?1",
            params![parent, mtime_ns, ctime_ns],
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
                time_ns: mtime_ns,
            },
        )?;
        Self::track_manifest_transition(
            &tx,
            ino,
            None,
            Some(manifest),
            Self::next_deref_seq(&tx)?,
            mtime_ns / 1_000_000,
        )?;
        Self::journal(
            &tx,
            &LogRecord::WriteManifest {
                ino,
                base_manifest: None,
                manifest: manifest.to_vec(),
                size,
                time_ns: mtime_ns,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn is_scratch_dir(&self, ino: Ino) -> Result<bool, MetaError> {
        Ok(self.get_xattr(ino, SCRATCH_XATTR)?.as_deref() == Some(b"1"))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scratch_create(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if tx
            .query_row(
                "SELECT 1 FROM scratch_inode WHERE ino = ?1",
                params![ino],
                |_| Ok(()),
            )
            .optional()?
            .is_some()
        {
            return Err(MetaError::Exists);
        }
        let t = now_ns();
        let attr = FileAttr::new_file(ino, mode, uid, gid, t);
        tx.execute(
            "INSERT INTO scratch_dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
            params![parent, name, ino],
        )
        .map_err(|error| match error {
            rusqlite::Error::SqliteFailure(ref e, _)
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                MetaError::Exists
            }
            other => other.into(),
        })?;
        tx.execute(
            "INSERT INTO scratch_inode
             (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6, ?6)",
            params![ino, InodeKind::File.as_u8(), attr.mode, uid, gid, t],
        )?;
        tx.commit()?;
        Ok(attr)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scratch_mkdir(
        &self,
        parent: Ino,
        name: &str,
        ino: Ino,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr, MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let t = now_ns();
        let attr = FileAttr::new_dir(ino, mode, uid, gid, t);
        tx.execute(
            "INSERT INTO scratch_dentry (parent, name, ino) VALUES (?1, ?2, ?3)",
            params![parent, name, ino],
        )
        .map_err(|error| match error {
            rusqlite::Error::SqliteFailure(ref e, _)
                if e.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                MetaError::Exists
            }
            other => other.into(),
        })?;
        tx.execute(
            "INSERT INTO scratch_inode
             (ino, kind, mode, uid, gid, nlink, atime_ns, mtime_ns, ctime_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, 2, ?6, ?6, ?6)",
            params![ino, InodeKind::Dir.as_u8(), attr.mode, uid, gid, t],
        )?;
        tx.commit()?;
        Ok(attr)
    }

    pub fn scratch_lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        self.with_reader(|conn| {
            Ok(conn
                .query_row(
                    "SELECT i.ino, i.kind, i.size, i.mode, i.uid, i.gid, i.nlink,
                            i.atime_ns, i.mtime_ns, i.ctime_ns, i.rdev
                     FROM scratch_dentry d JOIN scratch_inode i ON i.ino = d.ino
                     WHERE d.parent = ?1 AND d.name = ?2",
                    params![parent, name],
                    Self::row_to_attr,
                )
                .optional()?)
        })
    }

    pub fn scratch_unlink(&self, parent: Ino, name: &str) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let ino: Ino = tx
            .query_row(
                "SELECT ino FROM scratch_dentry WHERE parent = ?1 AND name = ?2",
                params![parent, name],
                |row| row.get(0),
            )
            .optional()?
            .ok_or(MetaError::NoEntry)?;
        tx.execute(
            "DELETE FROM scratch_dentry WHERE parent = ?1 AND name = ?2",
            params![parent, name],
        )?;
        tx.execute("DELETE FROM scratch_inode WHERE ino = ?1", params![ino])?;
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_rename(
        &self,
        parent: Ino,
        name: &str,
        new_parent: Ino,
        new_name: &str,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE scratch_dentry
             SET parent = ?3, name = ?4
             WHERE parent = ?1 AND name = ?2",
            params![parent, name, new_parent, new_name],
        )?;
        if changed == 0 {
            return Err(MetaError::NoEntry);
        }
        tx.commit()?;
        Ok(())
    }

    pub fn scratch_readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        self.with_reader(|conn| {
            let mut stmt = conn.prepare(
                "SELECT d.name, d.ino, i.kind
                 FROM scratch_dentry d JOIN scratch_inode i ON i.ino = d.ino
                 WHERE d.parent = ?1 ORDER BY d.name",
            )?;
            let rows = stmt.query_map(params![parent], |row| {
                let kind: u8 = row.get(2)?;
                Ok(DirEntry {
                    name: row.get(0)?,
                    ino: row.get(1)?,
                    kind: InodeKind::from_u8(kind).unwrap_or(InodeKind::File),
                })
            })?;
            Ok(rows.collect::<Result<_, _>>()?)
        })
    }

    pub fn scratch_getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        self.with_reader(|conn| {
            Ok(conn
                .query_row(
                    "SELECT ino, kind, size, mode, uid, gid, nlink,
                            atime_ns, mtime_ns, ctime_ns, rdev
                     FROM scratch_inode WHERE ino = ?1",
                    params![ino],
                    Self::row_to_attr,
                )
                .optional()?)
        })
    }

    pub fn scratch_set_manifest(
        &self,
        ino: Ino,
        manifest: &[u8],
        size: u64,
    ) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        let t = now_ns();
        let changed = conn.execute(
            "UPDATE scratch_inode
             SET manifest = ?2, size = ?3, mtime_ns = ?4, ctime_ns = ?4 WHERE ino = ?1",
            params![ino, manifest, size as i64, t],
        )?;
        if changed == 0 {
            return Err(MetaError::NoEnt(ino));
        }
        Ok(())
    }

    pub fn scratch_manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        self.with_reader(|conn| {
            Ok(conn
                .query_row(
                    "SELECT manifest FROM scratch_inode WHERE ino = ?1",
                    params![ino],
                    |row| row.get(0),
                )
                .optional()?
                .flatten())
        })
    }

    pub fn scratch_purge_all(&self) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("DELETE FROM scratch_dentry", [])?;
        tx.execute("DELETE FROM scratch_inode", [])?;
        tx.commit()?;
        Ok(())
    }

    pub fn shadow_insert(
        &self,
        part: &str,
        epoch: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO shadow (part, epoch, records, created_ms) VALUES (?1, ?2, ?3, ?4)",
            params![part, epoch, postcard::to_allocvec(&records)?, now_unix_ms()],
        )?;
        Ok(())
    }

    pub fn shadow_touch_set(&self) -> Result<TouchSet, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT records FROM shadow ORDER BY id")?;
        let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
        let mut records = Vec::new();
        for row in rows {
            records.extend(postcard::from_bytes::<Vec<LogRecord>>(&row?)?);
        }
        Ok(TouchSet::from_records(records.iter()))
    }

    pub fn shadow_retire_matching(
        &self,
        epoch: u64,
        records: &[LogRecord],
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows = {
            let mut stmt = tx.prepare("SELECT id, records FROM shadow WHERE epoch <= ?1")?;
            let rows = stmt.query_map(params![epoch], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (id, bytes) in rows {
            let shadow_records: Vec<LogRecord> = postcard::from_bytes(&bytes)?;
            if shadow_records.iter().all(|record| records.contains(record)) {
                tx.execute("DELETE FROM shadow WHERE id = ?1", params![id])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn shadow_expire(&self, older_than_ms: i64) -> Result<(), MetaError> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM shadow WHERE created_ms < ?1",
            params![older_than_ms],
        )?;
        Ok(())
    }

    /// The durable not-yet-uploaded set: every `(hash, ino)` pair a
    /// local write has journaled but this node has not yet confirmed in
    /// S3. Survives a crash — unlike `DiskCache::rescan`, which cannot
    /// tell dirty content from clean on a directory listing alone, this
    /// table is written in the same transaction as the journal record
    /// that makes the content dirty in the first place.
    pub fn pending_uploads(&self) -> Result<Vec<(ChunkHash, Ino)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT hash, ino FROM pending_upload")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Ino>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            let (bytes, ino) = row?;
            let arr: [u8; 32] = bytes
                .try_into()
                .map_err(|_| MetaError::Invalid("pending_upload hash length".into()))?;
            out.push((ChunkHash(arr), ino));
        }
        Ok(out)
    }

    /// Enrol an eagerly sealed chunk before the manifest commit. If the
    /// writer crashes before committing, uploading the resulting
    /// content-addressed orphan is harmless and later GC reclaims it.
    pub fn add_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        self.conn.lock().unwrap().execute(
            "INSERT OR IGNORE INTO pending_upload (hash, ino) VALUES (?1, ?2)",
            params![hash.0.to_vec(), ino],
        )?;
        Ok(())
    }

    pub fn pending_upload_count(&self) -> Result<u64, MetaError> {
        Ok(self.conn.lock().unwrap().query_row(
            "SELECT COUNT(*) FROM pending_upload",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn upload_pending_for_hash(&self, hash: &ChunkHash) -> Result<bool, MetaError> {
        Ok(self.conn.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_upload WHERE hash = ?1)",
            params![hash.0.to_vec()],
            |row| row.get(0),
        )?)
    }

    pub fn pending_uploads_for_inode(&self, ino: Ino) -> Result<Vec<(ChunkHash, Ino)>, MetaError> {
        Ok(self
            .pending_uploads()?
            .into_iter()
            .filter(|(_, row_ino)| *row_ino == ino)
            .collect())
    }

    /// Ack one pending upload. Deleting the row for `(hash, ino)` never
    /// disturbs a different inode still pending on the same hash (dedup
    /// across inodes is the upload side's job, not this table's).
    pub fn ack_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM pending_upload WHERE hash = ?1 AND ino = ?2",
            params![hash.0.to_vec(), ino],
        )?;
        Ok(())
    }

    /// Cancel an eager seal that was re-dirtied before its manifest
    /// committed. If a worker already completed the upload this delete
    /// is a harmless no-op; immutable orphan content is GC-safe.
    pub fn cancel_pending_upload(&self, hash: &ChunkHash, ino: Ino) -> Result<(), MetaError> {
        self.ack_upload(hash, ino)
    }
}

impl MetaStore for SqliteMeta {
    fn lookup(&self, parent: Ino, name: &str) -> Result<Option<FileAttr>, MetaError> {
        self.with_reader(|conn| match Self::dentry_ino(conn, parent, name)? {
            None => Ok(None),
            Some(ino) => Self::attr_by_ino(conn, ino),
        })
    }

    fn getattr(&self, ino: Ino) -> Result<Option<FileAttr>, MetaError> {
        self.with_reader(|conn| Self::attr_by_ino(conn, ino))
    }

    fn readdir(&self, parent: Ino) -> Result<Vec<DirEntry>, MetaError> {
        self.with_reader(|conn| {
            Self::require_dir(conn, parent)?;
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
        })
    }

    fn readlink(&self, ino: Ino) -> Result<Option<String>, MetaError> {
        self.with_reader(|conn| {
            Ok(conn
                .query_row(
                    "SELECT symlink_target FROM inode WHERE ino = ?1",
                    params![ino],
                    |r| r.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten())
        })
    }

    fn manifest(&self, ino: Ino) -> Result<Option<Vec<u8>>, MetaError> {
        self.with_reader(|conn| {
            Ok(conn
                .query_row(
                    "SELECT manifest FROM inode WHERE ino = ?1",
                    params![ino],
                    |r| r.get::<_, Option<Vec<u8>>>(0),
                )
                .optional()?
                .flatten())
        })
    }

    fn get_xattr(&self, ino: Ino, name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.with_reader(|conn| {
            if Self::attr_by_ino(conn, ino)?.is_none() {
                return Err(MetaError::NoEnt(ino));
            }
            Ok(conn
                .query_row(
                    "SELECT value FROM xattr WHERE ino = ?1 AND name = ?2",
                    params![ino, name],
                    |row| row.get(0),
                )
                .optional()?)
        })
    }

    fn list_xattrs(&self, ino: Ino) -> Result<Vec<String>, MetaError> {
        self.with_reader(|conn| {
            if Self::attr_by_ino(conn, ino)?.is_none() {
                return Err(MetaError::NoEnt(ino));
            }
            Ok(Self::xattrs_by_ino(conn, ino)?
                .into_iter()
                .map(|(name, _)| name)
                .collect())
        })
    }

    fn recursive_size(&self, ino: Ino) -> Result<(u64, u64), MetaError> {
        self.with_reader(|conn| {
            if Self::attr_by_ino(conn, ino)?.is_none() {
                return Err(MetaError::NoEnt(ino));
            }
            let file_kind = InodeKind::File.as_u8();
            let (size, count): (i64, i64) = conn.query_row(
                "WITH RECURSIVE subtree(ino) AS (
                     VALUES (?1)
                     UNION
                     SELECT d.ino FROM dentry d JOIN subtree s ON d.parent = s.ino
                 )
                 SELECT
                     COALESCE(SUM(CASE WHEN i.kind = ?2 THEN i.size ELSE 0 END), 0),
                     COALESCE(SUM(CASE WHEN i.kind = ?2 THEN 1 ELSE 0 END), 0)
                 FROM subtree s JOIN inode i ON i.ino = s.ino",
                params![ino, file_kind],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok((size as u64, count as u64))
        })
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
        if attr.nlink == 1 {
            tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
            let manifest: Option<Vec<u8>> = tx.query_row(
                "SELECT manifest FROM inode WHERE ino = ?1",
                params![ino],
                |row| row.get(0),
            )?;
            Self::track_manifest_transition(
                &tx,
                ino,
                manifest.as_deref(),
                None,
                Self::next_deref_seq(&tx)?,
                t / 1_000_000,
            )?;
        }
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
        tx.execute("DELETE FROM xattr WHERE ino = ?1", params![ino])?;
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
        if let Some((_, t)) = Self::rename_in_tx(&tx, parent, name, new_parent, new_name)? {
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
        }
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
        Self::set_manifest_tx(&tx, ino, manifest, size)?;
        tx.commit()?;
        Ok(())
    }

    fn set_xattr(
        &self,
        ino: Ino,
        name: &str,
        value: &[u8],
        mode: SetXattrMode,
    ) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if Self::attr_by_ino(&tx, ino)?.is_none() {
            return Err(MetaError::NoEnt(ino));
        }
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM xattr WHERE ino = ?1 AND name = ?2)",
            params![ino, name],
            |row| row.get(0),
        )?;
        match (mode, exists) {
            (SetXattrMode::Create, true) => return Err(MetaError::Exists),
            (SetXattrMode::Replace, false) => return Err(MetaError::NoData),
            _ => {}
        }
        let time_ns = now_ns();
        tx.execute(
            "INSERT OR REPLACE INTO xattr (ino, name, value) VALUES (?1, ?2, ?3)",
            params![ino, name, value],
        )?;
        tx.execute(
            "UPDATE inode SET ctime_ns = ?2 WHERE ino = ?1",
            params![ino, time_ns],
        )?;
        Self::journal(
            &tx,
            &LogRecord::SetXattr {
                ino,
                name: name.to_string(),
                value: value.to_vec(),
                time_ns,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn remove_xattr(&self, ino: Ino, name: &str) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if Self::attr_by_ino(&tx, ino)?.is_none() {
            return Err(MetaError::NoEnt(ino));
        }
        let time_ns = now_ns();
        if tx.execute(
            "DELETE FROM xattr WHERE ino = ?1 AND name = ?2",
            params![ino, name],
        )? == 0
        {
            return Err(MetaError::NoData);
        }
        tx.execute(
            "UPDATE inode SET ctime_ns = ?2 WHERE ino = ?1",
            params![ino, time_ns],
        )?;
        Self::journal(
            &tx,
            &LogRecord::RemoveXattr {
                ino,
                name: name.to_string(),
                time_ns,
            },
        )?;
        tx.commit()?;
        Ok(())
    }

    fn reap_orphan(&self, ino: Ino) -> Result<(), MetaError> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let manifest: Option<Vec<u8>> = tx
            .query_row(
                "SELECT manifest FROM inode WHERE ino = ?1 AND nlink = 0",
                params![ino],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        tx.execute(
            "DELETE FROM inode WHERE ino = ?1 AND nlink = 0",
            params![ino],
        )?;
        tx.execute(
            "DELETE FROM xattr WHERE ino = ?1 AND NOT EXISTS
             (SELECT 1 FROM inode WHERE inode.ino = xattr.ino)",
            params![ino],
        )?;
        Self::track_manifest_transition(
            &tx,
            ino,
            manifest.as_deref(),
            None,
            Self::next_deref_seq(&tx)?,
            now_unix_ms(),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn orphans(&self) -> Result<Vec<Ino>, MetaError> {
        self.with_reader(|conn| {
            let mut stmt = conn.prepare("SELECT ino FROM inode WHERE nlink = 0")?;
            let rows = stmt.query_map([], |r| r.get(0))?;
            Ok(rows.collect::<Result<_, _>>()?)
        })
    }

    fn take_journal(&self, max: usize) -> Result<Vec<(u64, LogRecord)>, MetaError> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT seq, record FROM journal ORDER BY seq LIMIT ?1")?;
        let rows = stmt.query_map(params![max.min(i64::MAX as usize) as i64], |r| {
            Ok((r.get::<_, u64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (seq, bytes) = row?;
            out.push((seq, LogRecord::from_postcard(&bytes)?));
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
    use constellation_fs_core::manifest::Manifest;

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

    fn one_hash_manifest(hash: ChunkHash) -> Vec<u8> {
        Manifest::from_chunks(
            constellation_fs_core::DEFAULT_CHUNK_SIZE,
            1,
            vec![hash],
            constellation_fs_core::INLINE_CHUNKS_MAX,
        )
        .0
        .encode()
    }

    #[test]
    fn deref_tracks_manifest_replace_and_rereference() {
        let meta = store();
        let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap();
        let old = ChunkHash::of(b"old");
        let new = ChunkHash::of(b"new");
        meta.set_manifest(file.ino, &one_hash_manifest(old), 1)
            .unwrap();
        meta.set_manifest(file.ino, &one_hash_manifest(new), 1)
            .unwrap();
        let rows = meta.deref_candidates(i64::MAX).unwrap();
        assert!(rows.iter().any(|(hash, _, _)| *hash == old));
        assert!(!rows.iter().any(|(hash, _, _)| *hash == new));

        meta.set_manifest(file.ino, &one_hash_manifest(old), 1)
            .unwrap();
        assert!(!meta
            .deref_candidates(i64::MAX)
            .unwrap()
            .iter()
            .any(|(hash, _, _)| *hash == old));
    }

    #[test]
    fn deref_tracks_last_unlink() {
        let meta = store();
        let file = meta.create(ROOT_INO, "file", 0o644, 0, 0).unwrap();
        let hash = ChunkHash::of(b"unlinked");
        meta.set_manifest(file.ino, &one_hash_manifest(hash), 1)
            .unwrap();
        meta.unlink(ROOT_INO, "file").unwrap();
        assert!(meta
            .deref_candidates(i64::MAX)
            .unwrap()
            .iter()
            .any(|(candidate, _, _)| *candidate == hash));
    }

    /// Dedup safety: a hash referenced by a second file must not become a
    /// collection candidate until the last reference goes away.
    #[test]
    fn deref_waits_for_the_last_reference_to_a_shared_hash() {
        let meta = store();
        let a = meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        let hash = ChunkHash::of(b"shared-by-two-files");
        meta.set_manifest(a.ino, &one_hash_manifest(hash), 1)
            .unwrap();
        meta.set_manifest(b.ino, &one_hash_manifest(hash), 1)
            .unwrap();

        let is_candidate = |meta: &SqliteMeta| {
            meta.deref_candidates(i64::MAX)
                .unwrap()
                .iter()
                .any(|(candidate, _, _)| *candidate == hash)
        };

        meta.unlink(ROOT_INO, "a").unwrap();
        meta.reap_orphan(a.ino).unwrap();
        assert!(!is_candidate(&meta), "b still references the chunk");

        meta.unlink(ROOT_INO, "b").unwrap();
        meta.reap_orphan(b.ino).unwrap();
        assert!(is_candidate(&meta), "last reference is gone");
    }

    /// Tripwire for the regression that made `rm -rf` quadratic: liveness
    /// used to be answered by decoding every manifest in the namespace, so
    /// each unlink cost a full pass over `inode`. The probe must stay
    /// index-driven.
    #[test]
    fn hash_liveness_probe_does_not_scan_the_inode_table() {
        let meta = store();
        let conn = meta.conn.lock().unwrap();
        let plan: Vec<String> = conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT 1 FROM chunk_ref JOIN inode USING (ino)
                 WHERE chunk_ref.chunk_hash = ?1 AND inode.nlink > 0
                 LIMIT 1",
            )
            .unwrap()
            .query_map(params![vec![0u8; 32]], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let plan = plan.join("; ");
        assert!(
            !plan.to_uppercase().contains("SCAN"),
            "liveness probe must stay indexed, got: {plan}"
        );
    }

    /// DBs written before `chunk_ref` existed carry no reverse index. The
    /// index must be rebuilt on open, or their live chunks would look
    /// unreferenced.
    #[test]
    fn chunk_ref_index_is_backfilled_on_open() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let hash = ChunkHash::of(b"written-before-the-index");
        let ino = {
            let meta = SqliteMeta::open(&path).unwrap();
            let file = meta.create(ROOT_INO, "legacy", 0o644, 0, 0).unwrap();
            meta.set_manifest(file.ino, &one_hash_manifest(hash), 1)
                .unwrap();
            let conn = meta.conn.lock().unwrap();
            conn.execute("DELETE FROM chunk_ref", []).unwrap();
            conn.execute("DELETE FROM kv WHERE key = 'chunk_ref_index_v1'", [])
                .unwrap();
            file.ino
        };

        let meta = SqliteMeta::open(&path).unwrap();
        let conn = meta.conn.lock().unwrap();
        let rows: u64 = conn
            .query_row(
                "SELECT COUNT(*) FROM chunk_ref WHERE chunk_hash = ?1 AND ino = ?2",
                params![hash.0.to_vec(), ino],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 1, "reopening must rebuild the reverse index");
        assert!(
            SqliteMeta::hash_is_live(&conn, &hash).unwrap(),
            "a backfilled reference must read as live"
        );
    }

    #[test]
    fn deref_tracks_snapshot_delete() {
        let meta = store();
        let hash = ChunkHash::of(b"snapshot-root");
        let row = SnapshotRow {
            id: "snap".into(),
            path: "/".into(),
            name: "before".into(),
            root_hash: hash.to_hex(),
            created_unix_ms: 1,
        };
        meta.record_snapshot(&row).unwrap();
        meta.delete_snapshot("/", "before").unwrap();
        assert!(meta
            .deref_candidates(i64::MAX)
            .unwrap()
            .iter()
            .any(|(candidate, _, _)| *candidate == hash));
    }

    #[test]
    fn epoch_promise_persists_before_activation() {
        let m = store();
        let members = vec![1, 2];
        let base = std::collections::BTreeMap::from([("p0".to_string(), 7), ("p1".to_string(), 3)]);
        m.persist_epoch("e1", &members, &base, 123, "promised")
            .unwrap();
        let loaded = m.load_open_epoch().unwrap().unwrap();
        assert_eq!(loaded.0, "e1");
        assert_eq!(loaded.1, members);
        assert_eq!(loaded.2, base);
        assert_eq!(loaded.3, 123);
        assert_eq!(loaded.4, "promised");

        m.set_epoch_state("e1", "closed").unwrap();
        assert!(m.load_open_epoch().unwrap().is_none());
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
    fn xattr_roundtrip_flags_and_unlink_cleanup() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        assert_eq!(m.get_xattr(f.ino, "user.foo").unwrap(), None);
        m.set_xattr(f.ino, "user.foo", b"one", SetXattrMode::Create)
            .unwrap();
        assert!(matches!(
            m.set_xattr(f.ino, "user.foo", b"two", SetXattrMode::Create),
            Err(MetaError::Exists)
        ));
        m.set_xattr(f.ino, "user.foo", b"two", SetXattrMode::Replace)
            .unwrap();
        assert!(matches!(
            m.set_xattr(f.ino, "user.missing", b"x", SetXattrMode::Replace),
            Err(MetaError::NoData)
        ));
        m.set_xattr(f.ino, "user.other", b"x", SetXattrMode::Set)
            .unwrap();
        assert_eq!(
            m.list_xattrs(f.ino).unwrap(),
            vec!["user.foo".to_string(), "user.other".to_string()]
        );
        m.remove_xattr(f.ino, "user.other").unwrap();
        assert!(matches!(
            m.remove_xattr(f.ino, "user.other"),
            Err(MetaError::NoData)
        ));
        m.unlink(ROOT_INO, "f").unwrap();
        assert_eq!(m.get_xattr(f.ino, "user.foo").unwrap(), None);
    }

    #[test]
    fn xattr_journal_replays() {
        let src = store();
        let file = src.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        src.set_xattr(file.ino, "user.foo", b"value", SetXattrMode::Set)
            .unwrap();
        let records: Vec<_> = src
            .take_journal(10)
            .unwrap()
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let dst = store();
        dst.apply_records(&records).unwrap();
        assert_eq!(
            dst.get_xattr(file.ino, "user.foo").unwrap(),
            Some(b"value".to_vec())
        );
        src.remove_xattr(file.ino, "user.foo").unwrap();
        let remove = src.take_journal(10).unwrap().last().unwrap().1.clone();
        dst.apply_records(&[remove]).unwrap();
        assert_eq!(dst.get_xattr(file.ino, "user.foo").unwrap(), None);
    }

    #[test]
    fn xattr_survives_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let ino = {
            let m = SqliteMeta::open(&path).unwrap();
            let file = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
            m.set_xattr(file.ino, "user.foo", b"value", SetXattrMode::Set)
                .unwrap();
            file.ino
        };
        let m = SqliteMeta::open(&path).unwrap();
        assert_eq!(
            m.get_xattr(ino, "user.foo").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn recursive_size_counts_logical_file_lengths() {
        let m = store();
        let dir = m.mkdir(ROOT_INO, "d", 0o755, 0, 0).unwrap();
        let a = m.create(dir.ino, "a", 0o644, 0, 0).unwrap();
        let nested = m.mkdir(dir.ino, "nested", 0o755, 0, 0).unwrap();
        let b = m.create(nested.ino, "b", 0o644, 0, 0).unwrap();
        m.setattr(a.ino, None, None, None, Some(7), None, None)
            .unwrap();
        m.setattr(b.ino, None, None, None, Some(1 << 30), None, None)
            .unwrap();
        assert_eq!(m.recursive_size(dir.ino).unwrap(), ((1 << 30) + 7, 2));
        assert_eq!(m.recursive_size(b.ino).unwrap(), (1 << 30, 1));
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
    fn scratch_is_node_private_and_purgeable() {
        let m = store();
        let ino = m.allocate_ino().unwrap();
        m.scratch_create(ROOT_INO, "local", ino, 0o644, 1000, 1000)
            .unwrap();

        assert_eq!(
            m.scratch_lookup(ROOT_INO, "local").unwrap().unwrap().ino,
            ino
        );
        assert!(m.lookup(ROOT_INO, "local").unwrap().is_none());
        assert_eq!(m.journal_len().unwrap(), 0);

        m.scratch_purge_all().unwrap();
        assert!(m.scratch_lookup(ROOT_INO, "local").unwrap().is_none());
        assert!(m.scratch_getattr(ino).unwrap().is_none());
    }

    #[test]
    fn publish_file_creates_shared_inode() {
        let m = store();
        let ino = m.allocate_ino().unwrap();
        m.publish_file(
            ROOT_INO,
            "published",
            ino,
            0o640,
            1000,
            1001,
            123,
            b"MANIFEST",
            42,
        )
        .unwrap();

        let attr = m.lookup(ROOT_INO, "published").unwrap().unwrap();
        assert_eq!(attr.ino, ino);
        assert_eq!(attr.size, 42);
        assert_eq!(
            m.manifest(ino).unwrap().as_deref(),
            Some(b"MANIFEST".as_slice())
        );
        let records = m.take_journal(10).unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(records[0].1, LogRecord::Create { .. }));
        assert!(matches!(records[1].1, LogRecord::WriteManifest { .. }));
    }

    #[test]
    fn publish_file_identical_manifest_dedupes() {
        let m = store();
        let existing = m.create(ROOT_INO, "published", 0o644, 1000, 1000).unwrap();
        m.set_manifest(existing.ino, b"MANIFEST", 42).unwrap();
        let journal_len = m.journal_len().unwrap();

        m.publish_file(
            ROOT_INO,
            "published",
            m.allocate_ino().unwrap(),
            0o600,
            2000,
            2000,
            456,
            b"MANIFEST",
            42,
        )
        .unwrap();

        assert_eq!(m.journal_len().unwrap(), journal_len);
        assert_eq!(
            m.lookup(ROOT_INO, "published").unwrap().unwrap().ino,
            existing.ino
        );
    }

    /// A whole-file manifest composed on a stale base must be rejected,
    /// not silently installed: it would drop every chunk another writer
    /// committed in the meantime. This is what makes concurrent disjoint
    /// `WriteAt`s from several nodes compose instead of losing all but
    /// the last one.
    #[test]
    fn set_manifest_with_base_rejects_a_stale_base() {
        let m = store();
        let f = m.create(ROOT_INO, "wd", 0o644, 0, 0).unwrap();

        // First writer commits against the empty file: no base to claim.
        m.set_manifest_with_base(f.ino, None, b"A", 1).unwrap();

        // A second writer that still believes the manifest is "A" wins.
        m.set_manifest_with_base(f.ino, Some(b"A"), b"B", 1)
            .unwrap();
        assert_eq!(m.manifest(f.ino).unwrap().as_deref(), Some(&b"B"[..]));

        // A third writer composed on the now-stale "A" must be rejected
        // rather than clobber "B".
        let err = m
            .set_manifest_with_base(f.ino, Some(b"A"), b"C", 1)
            .expect_err("stale base must not commit");
        assert!(matches!(err, MetaError::Conflict), "got {err:?}");
        assert_eq!(
            m.manifest(f.ino).unwrap().as_deref(),
            Some(&b"B"[..]),
            "the rejected commit must not have changed anything"
        );

        // Replaying a commit that already landed is not a conflict.
        m.set_manifest_with_base(f.ino, Some(b"A"), b"B", 1)
            .unwrap();
    }

    #[test]
    fn shadow_insert_and_retire() {
        let m = store();
        let record = LogRecord::Unlink {
            parent: ROOT_INO,
            name: "shadowed".into(),
            time_ns: 123,
        };
        m.shadow_insert("p0", 7, std::slice::from_ref(&record))
            .unwrap();

        let touches = m.shadow_touch_set().unwrap();
        assert!(touches
            .dentries
            .contains(&(ROOT_INO, "shadowed".to_string())));

        m.shadow_retire_matching(7, std::slice::from_ref(&record))
            .unwrap();
        let touches = m.shadow_touch_set().unwrap();
        assert!(touches.dentries.is_empty());
        assert!(touches.inos.is_empty());
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

    /// A cross-partition rename must journal exactly the linked pair and
    /// must never disturb records that other ops already journaled.
    #[test]
    fn rename_xpart_journals_only_the_pair() {
        let m = store();
        let hot = m.mkdir(ROOT_INO, "hot", 0o755, 0, 0).unwrap();
        let cold = m.mkdir(ROOT_INO, "cold", 0o755, 0, 0).unwrap();
        let f = m.create(hot.ino, "x", 0o644, 0, 0).unwrap();
        m.ack_journal(m.take_journal(100).unwrap().last().unwrap().0)
            .unwrap();
        // An unrelated, still-unshipped record must survive the rename.
        let keep = m.create(cold.ino, "keep", 0o644, 0, 0).unwrap();

        m.rename_xpart(hot.ino, "x", cold.ino, "y", "p1", "p0")
            .unwrap();

        let recs = m.take_journal(100).unwrap();
        assert!(
            recs.iter()
                .any(|(_, r)| matches!(r, LogRecord::Create { ino, .. } if *ino == keep.ino)),
            "unrelated Create was dropped: {recs:#?}"
        );
        assert!(
            !recs
                .iter()
                .any(|(_, r)| matches!(r, LogRecord::Rename { .. })),
            "plain Rename must be replaced by the xpart pair: {recs:#?}"
        );
        let src = recs
            .iter()
            .filter(|(_, r)| matches!(r, LogRecord::RenameXpartSrc { .. }))
            .count();
        let dst = recs
            .iter()
            .filter(|(_, r)| matches!(r, LogRecord::RenameXpartDst { .. }))
            .count();
        assert_eq!((src, dst), (1, 1), "exactly one pair: {recs:#?}");
        assert_eq!(m.lookup(cold.ino, "y").unwrap().unwrap().ino, f.ino);
        assert!(m.lookup(hot.ino, "x").unwrap().is_none());
    }

    /// Renaming onto a hard link of the same inode is a POSIX no-op that
    /// journals nothing; it must not delete a previously journaled row.
    #[test]
    fn rename_xpart_hardlink_noop_keeps_journal() {
        let m = store();
        let hot = m.mkdir(ROOT_INO, "hot", 0o755, 0, 0).unwrap();
        let cold = m.mkdir(ROOT_INO, "cold", 0o755, 0, 0).unwrap();
        let f = m.create(hot.ino, "x", 0o644, 0, 0).unwrap();
        m.link(f.ino, cold.ino, "same").unwrap();
        m.ack_journal(m.take_journal(100).unwrap().last().unwrap().0)
            .unwrap();
        let keep = m.create(cold.ino, "keep", 0o644, 0, 0).unwrap();

        // Same inode on both sides: POSIX no-op.
        m.rename_xpart(hot.ino, "x", cold.ino, "same", "p1", "p0")
            .unwrap();

        let recs = m.take_journal(100).unwrap();
        assert!(
            recs.iter()
                .any(|(_, r)| matches!(r, LogRecord::Create { ino, .. } if *ino == keep.ino)),
            "no-op rename ate an unrelated journal row: {recs:#?}"
        );
        // Both names still resolve to the same inode.
        assert_eq!(m.lookup(hot.ino, "x").unwrap().unwrap().ino, f.ino);
        assert_eq!(m.lookup(cold.ino, "same").unwrap().unwrap().ino, f.ino);
    }

    /// Pins are node-local state keyed by path, and `pinned_ancestor`
    /// must cover anything created underneath a pin later, so a new file
    /// in a pinned directory is kept resident without re-pinning.
    #[test]
    fn pins_cover_descendants() {
        let m = store();
        let data = m.mkdir(ROOT_INO, "data", 0o755, 0, 0).unwrap();
        let sub = m.mkdir(data.ino, "sub", 0o755, 0, 0).unwrap();
        let f = m.create(sub.ino, "deep", 0o644, 0, 0).unwrap();
        let outside = m.create(ROOT_INO, "outside", 0o644, 0, 0).unwrap();

        assert_eq!(m.resolve_path("/data/sub").unwrap(), Some(sub.ino));
        assert_eq!(m.resolve_path("/").unwrap(), Some(ROOT_INO));
        assert!(m.resolve_path("/data/nope").unwrap().is_none());

        assert!(m.pinned_ancestor(f.ino).unwrap().is_none());
        m.add_pin("/data", data.ino).unwrap();
        assert_eq!(m.pins().unwrap(), vec![("/data".to_string(), data.ino)]);
        assert_eq!(m.pinned_ancestor(f.ino).unwrap().as_deref(), Some("/data"));
        assert_eq!(
            m.pinned_ancestor(data.ino).unwrap().as_deref(),
            Some("/data"),
            "the pin root itself is covered"
        );
        assert!(
            m.pinned_ancestor(outside.ino).unwrap().is_none(),
            "a sibling of the pin must not be covered"
        );

        assert!(m.remove_pin("/data").unwrap());
        assert!(!m.remove_pin("/data").unwrap(), "second unpin is a no-op");
        assert!(m.pinned_ancestor(f.ino).unwrap().is_none());
    }

    /// A pin's footprint is the manifests under it, so admission control
    /// can refuse before reserving cache space.
    #[test]
    fn subtree_manifests_walks_the_whole_subtree() {
        let m = store();
        let data = m.mkdir(ROOT_INO, "data", 0o755, 0, 0).unwrap();
        let sub = m.mkdir(data.ino, "sub", 0o755, 0, 0).unwrap();
        let a = m.create(data.ino, "a", 0o644, 0, 0).unwrap();
        let b = m.create(sub.ino, "b", 0o644, 0, 0).unwrap();
        let outside = m.create(ROOT_INO, "outside", 0o644, 0, 0).unwrap();
        m.set_manifest(a.ino, b"MA", 10).unwrap();
        m.set_manifest(b.ino, b"MB", 20).unwrap();
        m.set_manifest(outside.ino, b"MO", 40).unwrap();

        let mut got = m.subtree_manifests(data.ino).unwrap();
        got.sort_by_key(|(ino, _, _)| *ino);
        let total: u64 = got.iter().map(|(_, _, sz)| sz).sum();
        assert_eq!(total, 30, "only the subtree counts: {got:?}");
        assert!(got.iter().all(|(ino, _, _)| *ino != outside.ino));

        // A file with no manifest (never written) contributes nothing.
        m.create(data.ino, "empty", 0o644, 0, 0).unwrap();
        let after: u64 = m
            .subtree_manifests(data.ino)
            .unwrap()
            .iter()
            .map(|(_, _, sz)| sz)
            .sum();
        assert_eq!(after, 30);

        // Whole-filesystem pin sees everything.
        let all: u64 = m
            .subtree_manifests(ROOT_INO)
            .unwrap()
            .iter()
            .map(|(_, _, sz)| sz)
            .sum();
        assert_eq!(all, 70);
    }

    #[test]
    fn partition_of_walks_to_nearest_root() {
        let m = store();
        assert_eq!(m.partition_of(ROOT_INO).unwrap(), "p0");
        let d = m.mkdir(ROOT_INO, "hot", 0o755, 0, 0).unwrap();
        let f = m.create(d.ino, "x", 0o644, 0, 0).unwrap();
        assert_eq!(m.partition_of(d.ino).unwrap(), "p0");
        assert_eq!(m.partition_of(f.ino).unwrap(), "p0");
        {
            let conn = m.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO partition (id, root_ino) VALUES ('p1', ?1)",
                params![d.ino],
            )
            .unwrap();
            SqliteMeta::invalidate_part_cache(&conn, d.ino).unwrap();
            SqliteMeta::invalidate_part_cache(&conn, f.ino).unwrap();
        }
        assert_eq!(m.partition_of(d.ino).unwrap(), "p1");
        assert_eq!(m.partition_of(f.ino).unwrap(), "p1");
        assert_eq!(m.partition_of(ROOT_INO).unwrap(), "p0");
    }

    /// `set_manifest_dirty` inserts `pending_upload` rows in the same
    /// transaction as the `WriteManifest` record: a forced failure (bad
    /// ino) must leave neither the manifest commit nor a stray pending
    /// row (plan 05a step 1).
    #[test]
    fn set_manifest_dirty_journals_pending_uploads_in_the_same_tx() {
        let m = store();
        let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let h1 = ChunkHash::of(b"chunk-a");
        let h2 = ChunkHash::of(b"chunk-b");
        m.set_manifest_dirty(f.ino, b"M1", 10, &[h1, h2]).unwrap();
        let mut pending = m.pending_uploads().unwrap();
        pending.sort();
        let mut expect = vec![(h1, f.ino), (h2, f.ino)];
        expect.sort();
        assert_eq!(pending, expect);

        let h3 = ChunkHash::of(b"chunk-c");
        assert!(matches!(
            m.set_manifest_dirty(999_999, b"M", 1, &[h3]),
            Err(MetaError::NoEnt(999_999))
        ));
        assert!(
            !m.pending_uploads().unwrap().iter().any(|(h, _)| *h == h3),
            "a failed manifest commit must not leave a stray pending row"
        );
    }

    /// Acking one inode's pending row must not disturb another inode
    /// still pending on the exact same content hash.
    #[test]
    fn ack_upload_is_per_inode() {
        let m = store();
        let a = m.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
        let b = m.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        let h = ChunkHash::of(b"shared-content");
        m.set_manifest_dirty(a.ino, b"MA", 1, &[h]).unwrap();
        m.set_manifest_dirty(b.ino, b"MB", 1, &[h]).unwrap();
        m.ack_upload(&h, a.ino).unwrap();
        assert_eq!(m.pending_uploads().unwrap(), vec![(h, b.ino)]);
        m.ack_upload(&h, b.ino).unwrap();
        assert!(m.pending_uploads().unwrap().is_empty());
    }

    /// The pending-upload table is the crash simulation: drop and
    /// reopen the same DB file and the un-uploaded hash must still be
    /// there, unlike `DiskCache::rescan`'s in-memory accounting.
    #[test]
    fn pending_uploads_survive_reopen() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("meta.db");
        let (h, ino) = {
            let m = SqliteMeta::open(&path).unwrap();
            let f = m.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
            let h = ChunkHash::of(b"crash-content");
            m.set_manifest_dirty(f.ino, b"M", 5, &[h]).unwrap();
            (h, f.ino)
        };
        let m = SqliteMeta::open(&path).unwrap();
        assert_eq!(m.pending_uploads().unwrap(), vec![(h, ino)]);
    }
}
