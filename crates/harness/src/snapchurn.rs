//! Seeded, concurrent snapshot/clone churn with a disk-backed `fjall` oracle.
//!
//! The ordinary harness model intentionally keeps payloads in memory.  Churn
//! instead records only hashes and metadata in an on-disk `fjall` database, so
//! increasing `CONSTELLATION_SNAPCHURN_*` does not make expected-state memory
//! proportional to file content.  Lifecycle operations happen only after
//! worker threads join: those quiesce points are the snapshot consistency
//! boundary.

use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{bail, Context, Result};
use fjall::Readable as _;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static REPLAY: OnceLock<(Option<PathBuf>, bool)> = OnceLock::new();

pub fn set_replay(path: Option<PathBuf>, no_sleep: bool) -> Result<()> {
    REPLAY
        .set((path, no_sleep))
        .map_err(|_| anyhow::anyhow!("snapshot-churn replay options already initialized"))
}

#[derive(Clone, Debug)]
struct Config {
    workers: usize,
    rounds: usize,
    ops: usize,
    audit_dir: Option<PathBuf>,
}

impl Config {
    fn from_env() -> Result<Self> {
        // Scale knobs are deliberately read by the harness, not inherited by
        // daemon processes:
        // CONSTELLATION_SNAPCHURN_WORKERS (default 4)
        // CONSTELLATION_SNAPCHURN_ROUNDS  (default 3)
        // CONSTELLATION_SNAPCHURN_OPS     (per worker/round, default 30)
        // CONSTELLATION_SNAPCHURN_AUDIT   (directory, default scenario tempdir)
        fn number(name: &str, default: usize) -> Result<usize> {
            let value = match std::env::var(name) {
                Ok(raw) => raw
                    .parse()
                    .with_context(|| format!("{name} must be a positive integer"))?,
                Err(_) => default,
            };
            anyhow::ensure!(value > 0, "{name} must be greater than zero");
            Ok(value)
        }
        Ok(Self {
            workers: number("CONSTELLATION_SNAPCHURN_WORKERS", 4)?,
            rounds: number("CONSTELLATION_SNAPCHURN_ROUNDS", 3)?,
            ops: number("CONSTELLATION_SNAPCHURN_OPS", 30)?,
            audit_dir: std::env::var_os("CONSTELLATION_SNAPCHURN_AUDIT").map(PathBuf::from),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Entry {
    path: String,
    kind: String,
    size: u64,
    hash: String,
    mtime_ns: i64,
    mode: u32,
}

/// Encodes an `(owner, path)` pair into a sort-friendly key for the `live`
/// and `snap_entry` keyspaces (`owner` is a churn root for `live`, a
/// snapshot id for `snap_entry`). The owner is length-prefixed so the
/// encoding stays unambiguous no matter what bytes `path` contains -- unlike
/// a plain separator byte, a length prefix cannot collide with path content.
fn owner_path_key(owner: &str, path: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + owner.len() + path.len());
    key.extend_from_slice(&(owner.len() as u32).to_be_bytes());
    key.extend_from_slice(owner.as_bytes());
    key.extend_from_slice(path.as_bytes());
    key
}

/// The prefix that selects every `owner_path_key` belonging to `owner`, and
/// only those keys: the length prefix rules out any other owner's key ever
/// sharing this byte sequence.
fn owner_prefix(owner: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + owner.len());
    key.extend_from_slice(&(owner.len() as u32).to_be_bytes());
    key.extend_from_slice(owner.as_bytes());
    key
}

/// Splits an `owner_path_key`-encoded key back into `(owner, path)`.
fn split_owner_path_key(key: &[u8]) -> Result<(String, String)> {
    anyhow::ensure!(key.len() >= 4, "oracle key too short: {} bytes", key.len());
    let len = u32::from_be_bytes(key[0..4].try_into().expect("checked above")) as usize;
    anyhow::ensure!(key.len() >= 4 + len, "oracle key truncated");
    let owner = std::str::from_utf8(&key[4..4 + len])?.to_string();
    let path = std::str::from_utf8(&key[4 + len..])?.to_string();
    Ok((owner, path))
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(postcard::to_allocvec(value)?)
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(postcard::from_bytes(bytes)?)
}

/// Collects every key/value pair under `prefix` in `ks`, as seen through
/// `read` (a [`fjall::SingleWriterWriteTx`] or a [`fjall::Snapshot`]).
fn collect_prefix<R: fjall::Readable>(
    read: &R,
    ks: &fjall::SingleWriterTxKeyspace,
    prefix: &[u8],
) -> fjall::Result<Vec<(fjall::UserKey, fjall::UserValue)>> {
    read.prefix(ks, prefix).map(|g| g.into_inner()).collect()
}

/// Test-harness bookkeeping oracle, tracking the expected file-tree state
/// (paths, kinds, sizes, hashes, mtimes, modes) across a snapshot/clone
/// churn run. Backed by an on-disk `fjall` database mirroring the four
/// logical tables the SQLite version used:
///
/// - `live`: key = `owner_path_key(root, path)`, value = postcard `Entry`.
/// - `snap`: key = `id` (raw bytes), value = postcard `(name, src_root, created_unix_ms)`.
/// - `snap_entry`: key = `owner_path_key(id, path)`, value = postcard `Entry`.
/// - `clone`: key = `dest_root` (raw bytes), value = postcard `(id, src_snap_id)`.
///
/// Cheap to clone (an `Arc`-backed database handle plus four keyspace
/// handles, themselves `Arc`-backed): unlike the SQLite/WAL engine this
/// replaces, `fjall` refuses more than one process-wide open of the
/// same directory at a time, so concurrent workers must share one
/// `Oracle` (via `clone`) rather than each opening their own by path.
#[derive(Clone)]
struct Oracle {
    db: fjall::SingleWriterTxDatabase,
    live: fjall::SingleWriterTxKeyspace,
    snap: fjall::SingleWriterTxKeyspace,
    snap_entry: fjall::SingleWriterTxKeyspace,
    clone_ks: fjall::SingleWriterTxKeyspace,
}

impl Oracle {
    fn open(path: &Path) -> Result<Self> {
        // `builder(path).open()` creates the store if it does not exist yet
        // and simply re-opens it (without wiping data) if it does, matching
        // the idempotent-reopen behavior `Connection::open` had.
        let db = fjall::SingleWriterTxDatabase::builder(path).open()?;
        let live = db.keyspace("live", fjall::KeyspaceCreateOptions::default)?;
        let snap = db.keyspace("snap", fjall::KeyspaceCreateOptions::default)?;
        let snap_entry = db.keyspace("snap_entry", fjall::KeyspaceCreateOptions::default)?;
        let clone_ks = db.keyspace("clone", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            live,
            snap,
            snap_entry,
            clone_ks,
        })
    }

    fn refresh_root(&mut self, root: &str, real: &Path) -> Result<()> {
        let entries = scan(real)?;
        let mut tx = self.db.write_tx();
        let prefix = owner_prefix(root);
        let stale = collect_prefix(&tx, &self.live, &prefix)?;
        for (key, _) in stale {
            tx.remove(&self.live, key);
        }
        for entry in entries.values() {
            let key = owner_path_key(root, &entry.path);
            let value = encode(entry)?;
            tx.insert(&self.live, key.as_slice(), value.as_slice());
        }
        tx.commit()?;
        Ok(())
    }

    fn refresh_prefix(&mut self, root: &str, prefix: &str, real: &Path) -> Result<()> {
        let entries = scan(real)?;
        let mut tx = self.db.write_tx();
        let root_prefix = owner_prefix(root);
        let candidates = collect_prefix(&tx, &self.live, &root_prefix)?;
        let nested = format!("{prefix}/");
        for (key, _) in candidates {
            let (_owner, path) = split_owner_path_key(&key)?;
            if path == prefix || path.starts_with(&nested) {
                tx.remove(&self.live, key);
            }
        }
        if real.exists() {
            let meta = std::fs::symlink_metadata(real)?;
            let dir_entry = Entry {
                path: prefix.to_string(),
                kind: "dir".to_string(),
                size: 0,
                hash: String::new(),
                mtime_ns: meta.mtime() * 1_000_000_000 + meta.mtime_nsec(),
                mode: meta.mode() & 0o7777,
            };
            let key = owner_path_key(root, &dir_entry.path);
            let value = encode(&dir_entry)?;
            tx.insert(&self.live, key.as_slice(), value.as_slice());
        }
        for entry in entries.values() {
            let mut entry = entry.clone();
            entry.path = if entry.path.is_empty() {
                prefix.to_string()
            } else {
                format!("{prefix}/{}", entry.path)
            };
            let key = owner_path_key(root, &entry.path);
            let value = encode(&entry)?;
            tx.insert(&self.live, key.as_slice(), value.as_slice());
        }
        tx.commit()?;
        Ok(())
    }

    fn freeze(&mut self, id: &str, name: &str, src_root: &str, frozen: &Path) -> Result<()> {
        let entries = scan(frozen)?;
        let mut tx = self.db.write_tx();
        let snap_value = encode(&(name.to_string(), src_root.to_string(), unix_ms()))?;
        tx.insert(&self.snap, id.as_bytes(), snap_value.as_slice());
        for entry in entries.values() {
            let key = owner_path_key(id, &entry.path);
            let value = encode(entry)?;
            tx.insert(&self.snap_entry, key.as_slice(), value.as_slice());
        }
        tx.commit()?;
        Ok(())
    }

    fn delete_snapshot(&mut self, id: &str) -> Result<()> {
        let mut tx = self.db.write_tx();
        let prefix = owner_prefix(id);
        let stale = collect_prefix(&tx, &self.snap_entry, &prefix)?;
        for (key, _) in stale {
            tx.remove(&self.snap_entry, key);
        }
        tx.remove(&self.snap, id.as_bytes());
        tx.commit()?;
        Ok(())
    }

    fn create_clone(&mut self, id: &str, dest: &str, snap_id: &str) -> Result<()> {
        let mut tx = self.db.write_tx();
        let clone_value = encode(&(id.to_string(), snap_id.to_string()))?;
        tx.insert(&self.clone_ks, dest.as_bytes(), clone_value.as_slice());

        let prefix = owner_prefix(snap_id);
        let source_entries = collect_prefix(&tx, &self.snap_entry, &prefix)?;
        for (key, value) in source_entries {
            let (_owner, path) = split_owner_path_key(&key)?;
            let dest_key = owner_path_key(dest, &path);
            tx.insert(&self.live, dest_key.as_slice(), value);
        }
        tx.commit()?;
        Ok(())
    }

    fn delete_clone(&mut self, root: &str) -> Result<()> {
        let mut tx = self.db.write_tx();
        let prefix = owner_prefix(root);
        let stale = collect_prefix(&tx, &self.live, &prefix)?;
        for (key, _) in stale {
            tx.remove(&self.live, key);
        }
        tx.remove(&self.clone_ks, root.as_bytes());
        tx.commit()?;
        Ok(())
    }

    fn expected_live(&self, root: &str) -> Result<BTreeMap<String, Entry>> {
        self.entries_for(&self.live, root)
    }

    fn expected_snapshot(&self, id: &str) -> Result<BTreeMap<String, Entry>> {
        self.entries_for(&self.snap_entry, id)
    }

    fn entries_for(
        &self,
        ks: &fjall::SingleWriterTxKeyspace,
        owner: &str,
    ) -> Result<BTreeMap<String, Entry>> {
        let read = self.db.read_tx();
        let prefix = owner_prefix(owner);
        let rows = collect_prefix(&read, ks, &prefix)?;
        let mut result = BTreeMap::new();
        for (key, value) in rows {
            let (_owner, path) = split_owner_path_key(&key)?;
            let entry: Entry = decode(&value)?;
            result.insert(path, entry);
        }
        Ok(result)
    }

    fn snapshots(&self) -> Result<Vec<(String, String, String)>> {
        let read = self.db.read_tx();
        let mut rows: Vec<(String, String, String, u64)> = Vec::new();
        for guard in read.iter(&self.snap) {
            let (key, value) = guard.into_inner()?;
            let id = std::str::from_utf8(&key)?.to_string();
            let (name, src_root, created_unix_ms): (String, String, u64) = decode(&value)?;
            rows.push((id, name, src_root, created_unix_ms));
        }
        rows.sort_by(|a, b| a.3.cmp(&b.3).then_with(|| a.0.cmp(&b.0)));
        Ok(rows
            .into_iter()
            .map(|(id, name, src_root, _)| (id, name, src_root))
            .collect())
    }

    fn roots(&self) -> Result<Vec<String>> {
        let read = self.db.read_tx();
        let mut roots = std::collections::BTreeSet::new();
        for guard in read.iter(&self.live) {
            let (key, _value) = guard.into_inner()?;
            let (root, _path) = split_owner_path_key(&key)?;
            roots.insert(root);
        }
        Ok(roots.into_iter().collect())
    }

    fn verify_all(&self, mount: &Path) -> Result<()> {
        for root in self.roots()? {
            let actual = scan(&mount_path(mount, &root))?;
            compare(
                &format!("live {root}"),
                &self.expected_live(&root)?,
                &actual,
            )?;
        }
        for (id, name, src_root) in self.snapshots()? {
            let actual = scan(&snapshot_path(mount, &src_root, &name))?;
            compare(
                &format!("snapshot {src_root}@{name}"),
                &self.expected_snapshot(&id)?,
                &actual,
            )?;
        }
        Ok(())
    }

    fn count(&self, table: &str) -> Result<u64> {
        let ks = match table {
            "live" => &self.live,
            "snap" => &self.snap,
            "snap_entry" => &self.snap_entry,
            "clone" => &self.clone_ks,
            other => bail!("unknown oracle table {other}"),
        };
        let read = self.db.read_tx();
        Ok(read.iter(ks).count() as u64)
    }
}

fn scan(root: &Path) -> Result<BTreeMap<String, Entry>> {
    let mut result = BTreeMap::new();
    if !root.exists() {
        return Ok(result);
    }
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for item in std::fs::read_dir(&dir).with_context(|| format!("walking {}", dir.display()))? {
            let item = item?;
            if item.file_name() == ".constellation" {
                continue;
            }
            let path = item.path();
            let rel = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            let meta = std::fs::symlink_metadata(&path)?;
            let (kind, size, hash) = if meta.file_type().is_symlink() {
                let target = std::fs::read_link(&path)?.to_string_lossy().into_owned();
                (
                    "symlink".to_string(),
                    target.len() as u64,
                    blake3::hash(target.as_bytes()).to_hex().to_string(),
                )
            } else if meta.is_dir() {
                dirs.push(path);
                ("dir".to_string(), 0, String::new())
            } else if meta.is_file() {
                let bytes = std::fs::read(&path)?;
                (
                    "file".to_string(),
                    meta.len(),
                    blake3::hash(&bytes).to_hex().to_string(),
                )
            } else {
                bail!("unsupported entry in churn tree: {}", path.display());
            };
            result.insert(
                rel.clone(),
                Entry {
                    path: rel,
                    kind,
                    size,
                    hash,
                    mtime_ns: meta.mtime() * 1_000_000_000 + meta.mtime_nsec(),
                    mode: meta.mode() & 0o7777,
                },
            );
        }
    }
    Ok(result)
}

fn compare(
    what: &str,
    expected: &BTreeMap<String, Entry>,
    actual: &BTreeMap<String, Entry>,
) -> Result<()> {
    anyhow::ensure!(
        expected.keys().eq(actual.keys()),
        "{what} path set differs\nexpected={:?}\nactual={:?}",
        expected.keys().collect::<Vec<_>>(),
        actual.keys().collect::<Vec<_>>()
    );
    for (path, want) in expected {
        let got = &actual[path];
        anyhow::ensure!(
            want == got,
            "{what} differs at {path}: expected {want:?}, got {got:?}"
        );
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Header {
    kind: String,
    seed: u64,
    workers: usize,
    rounds: usize,
    ops_per_worker_round: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Event {
    kind: String,
    t_ms: u64,
    worker: Option<usize>,
    op: String,
    root: Option<String>,
    path: Option<String>,
    args: Value,
}

struct Audit {
    started: Instant,
    out: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl Audit {
    fn create(path: PathBuf, header: &Header) -> Result<Self> {
        let mut out = BufWriter::new(File::create(&path)?);
        serde_json::to_writer(&mut out, header)?;
        out.write_all(b"\n")?;
        out.flush()?;
        Ok(Self {
            started: Instant::now(),
            out: Mutex::new(out),
            path,
        })
    }

    fn event(
        &self,
        worker: Option<usize>,
        op: &str,
        root: Option<&str>,
        path: Option<&str>,
        args: Value,
    ) -> Result<()> {
        let mut out = self.out.lock().unwrap();
        let event = Event {
            kind: "event".into(),
            t_ms: self.started.elapsed().as_millis() as u64,
            worker,
            op: op.into(),
            root: root.map(str::to_string),
            path: path.map(str::to_string),
            args,
        };
        serde_json::to_writer(&mut *out, &event)?;
        out.write_all(b"\n")?;
        out.flush()?;
        Ok(())
    }
}

#[derive(Default)]
struct Stats {
    snapshots_created: u64,
    snapshots_deleted: u64,
    clones_created: u64,
    clones_deleted: u64,
    verifies: u64,
    marker_isolated: bool,
    clone_of_clone: bool,
}

#[derive(Clone)]
struct Snapshot {
    id: String,
    name: String,
    root: String,
}

pub fn run(seed: u64) -> Result<()> {
    match REPLAY.get().cloned().unwrap_or((None, false)) {
        (Some(path), no_sleep) => replay(&path, no_sleep),
        (None, _) => live(seed),
    }
}

fn live(seed: u64) -> Result<()> {
    let cfg = Config::from_env()?;
    anyhow::ensure!(
        cfg.rounds >= 3,
        "CONSTELLATION_SNAPCHURN_ROUNDS must be at least 3 to exercise full lifecycle"
    );
    let env = S3Env::start().context("starting snapshot-churn S3 environment")?;
    let _proxy = env.s3_proxy()?;
    let mut temp = tempfile::Builder::new()
        .prefix("harness-snapshot-churn-")
        .tempdir()?;
    // Same convention as `scenarios::setup`: keep mount logs and state.
    if std::env::var_os("CHAOS_KEEP_TMP").is_some_and(|v| v != "0") {
        temp.disable_cleanup(true);
        eprintln!(
            "CHAOS_KEEP_TMP: artifacts kept at {}",
            temp.path().display()
        );
    }
    let prefix = format!("snapshot-churn-{}", unix_ms());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut client = Client::new(temp.path(), "churn", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_LEASE_TTL_MS", "200")
        .with_env("CONSTELLATION_GC_HORIZON_S", "0");
    client.fs_create()?;
    client.mount()?;

    let audit_dir = cfg.audit_dir.as_deref().unwrap_or(temp.path());
    std::fs::create_dir_all(audit_dir)?;
    let audit_path = audit_dir.join(format!("snapshot-churn-{seed}-{}.jsonl", unix_ms()));
    let audit = Arc::new(Audit::create(
        audit_path,
        &Header {
            kind: "header".into(),
            seed,
            workers: cfg.workers,
            rounds: cfg.rounds,
            ops_per_worker_round: cfg.ops,
        },
    )?);
    eprintln!("    snapshot-churn audit: {}", audit.path.display());

    let oracle_path = temp.path().join("snapshot-churn-oracle.db");
    let mut oracle = Oracle::open(&oracle_path)?;
    std::fs::create_dir(client.mnt.join("tree"))?;
    for worker in 0..cfg.workers {
        std::fs::create_dir(client.mnt.join(format!("tree/w{worker}")))?;
    }
    std::fs::write(client.mnt.join("tree/marker"), b"base-marker")?;
    oracle.refresh_root("/tree", &client.mnt.join("tree"))?;
    audit.event(
        None,
        "init",
        Some("/tree"),
        None,
        json!({"workers":cfg.workers}),
    )?;

    let mut stats = Stats::default();
    let mut snapshots: Vec<Snapshot> = Vec::new();
    let mut clones: Vec<String> = Vec::new();
    let mut next_clone = 1usize;

    for round in 0..cfg.rounds {
        let mut roots = vec!["/tree".to_string()];
        roots.extend(clones.iter().cloned());
        let mut joins = Vec::new();
        for worker in 0..cfg.workers {
            let root = roots[worker % roots.len()].clone();
            let mount = client.mnt.clone();
            let worker_oracle = oracle.clone();
            let audit = audit.clone();
            let ops = cfg.ops;
            joins.push(std::thread::spawn(move || {
                worker_round(
                    &mount,
                    worker_oracle,
                    &audit,
                    seed ^ ((round as u64 + 1) << 32) ^ worker as u64,
                    worker,
                    &root,
                    ops,
                )
            }));
        }
        for join in joins {
            join.join()
                .map_err(|_| anyhow::anyhow!("snapshot-churn worker panicked"))??;
        }
        audit.event(None, "quiesce", None, None, json!({"round":round}))?;

        if round == 1 && !clones.is_empty() {
            forced_marker(&client, &mut oracle, &audit, &clones[0])?;
            stats.marker_isolated = true;
            let source = snapshots
                .iter()
                .find(|snap| snap.root == "/tree")
                .context("origin source snapshot missing")?;
            let source_marker = snapshot_path(&client.mnt, "/tree", &source.name).join("marker");
            anyhow::ensure!(
                std::fs::read(source_marker)? == b"base-marker",
                "origin/clone marker divergence changed its source snapshot"
            );
        }

        let origin = create_snapshot(
            &client,
            &mut oracle,
            &audit,
            &format!("s{round}"),
            "/tree",
            &format!("s{round}"),
        )?;
        snapshots.push(origin.clone());
        stats.snapshots_created += 1;

        if round == 1 && !clones.is_empty() {
            let clone_root = clones[0].clone();
            let clone_snap = create_snapshot(
                &client,
                &mut oracle,
                &audit,
                &format!("cr{round}"),
                &clone_root,
                &format!("cr{round}"),
            )?;
            snapshots.push(clone_snap.clone());
            stats.snapshots_created += 1;
            let dest = format!("/c{next_clone}");
            next_clone += 1;
            create_clone(
                &client,
                &mut oracle,
                &audit,
                &clone_snap,
                &dest,
                &format!("clone-{dest}"),
            )?;
            clones.push(dest);
            stats.clones_created += 1;
            stats.clone_of_clone = true;
        } else if clones.len() < 3 {
            let dest = format!("/c{next_clone}");
            next_clone += 1;
            create_clone(
                &client,
                &mut oracle,
                &audit,
                &origin,
                &dest,
                &format!("clone-{dest}"),
            )?;
            clones.push(dest);
            stats.clones_created += 1;
        }

        if snapshots.len() > 2 {
            let newest_id = snapshots.last().unwrap().id.clone();
            let index = snapshots
                .iter()
                .position(|snap| snap.root == "/tree" && snap.id != newest_id)
                .context("no old origin snapshot to delete")?;
            let doomed = snapshots.remove(index);
            delete_snapshot(&client, &mut oracle, &audit, &doomed)?;
            stats.snapshots_deleted += 1;
        }

        if clones.len() > 1 {
            // First remove the parent clone after proving its clone-of-clone
            // survives both source-snapshot and source-root deletion. Later
            // rounds remove the newest throwaway clone, preserving /c2.
            let index = if round == 1 { 0 } else { clones.len() - 1 };
            let doomed = clones.remove(index);
            let mut owned = Vec::new();
            snapshots.retain(|snap| {
                if snap.root == doomed {
                    owned.push(snap.clone());
                    false
                } else {
                    true
                }
            });
            for snap in owned {
                delete_snapshot(&client, &mut oracle, &audit, &snap)?;
                stats.snapshots_deleted += 1;
            }
            delete_clone(&client, &mut oracle, &audit, &doomed)?;
            stats.clones_deleted += 1;
        }

        oracle.verify_all(&client.mnt)?;
        audit.event(None, "verify", None, None, json!({"round":round}))?;
        stats.verifies += 1;
    }

    anyhow::ensure!(!snapshots.is_empty(), "last round drained all snapshots");
    anyhow::ensure!(!clones.is_empty(), "last round drained all clones");
    anyhow::ensure!(
        oracle
            .expected_live("/tree")?
            .values()
            .any(|e| e.kind == "file"),
        "last round left no origin file"
    );
    anyhow::ensure!(
        stats.marker_isolated,
        "overlapping marker isolation did not run"
    );
    anyhow::ensure!(stats.clone_of_clone, "clone-of-clone did not run");
    oracle.verify_all(&client.mnt)?;
    audit.event(None, "verify", None, None, json!({"final":true}))?;
    stats.verifies += 1;

    for snap in std::mem::take(&mut snapshots) {
        delete_snapshot(&client, &mut oracle, &audit, &snap)?;
        stats.snapshots_deleted += 1;
    }
    anyhow::ensure!(oracle.count("snap")? == 0, "oracle snapshots remain");
    for clone in std::mem::take(&mut clones) {
        delete_clone(&client, &mut oracle, &audit, &clone)?;
        stats.clones_deleted += 1;
    }
    remove_root(&client, &mut oracle, &audit, "/tree")?;

    assert_replica_clean(&client)?;
    assert_prefix_empty(&env.direct_endpoint, &format!("{prefix}/snaps/"), "snaps/")?;
    audit.event(None, "gc", None, None, json!({}))?;
    let output = client.gc_run()?;
    anyhow::ensure!(
        output.status.success(),
        "snapshot-churn GC failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_prefix_empty(
        &env.direct_endpoint,
        &format!("{prefix}/chunks/"),
        "user chunks after cleanup GC",
    )?;
    client.unmount()?;

    eprintln!(
        "    snapshot-churn: workers={} rounds={} ops/worker/round={} snapshots={}/{} clones={}/{} verifies={} marker_isolated={} clone_of_clone={} trail={}",
        cfg.workers,
        cfg.rounds,
        cfg.ops,
        stats.snapshots_created,
        stats.snapshots_deleted,
        stats.clones_created,
        stats.clones_deleted,
        stats.verifies,
        stats.marker_isolated,
        stats.clone_of_clone,
        audit.path.display()
    );
    Ok(())
}

fn worker_round(
    mount: &Path,
    mut oracle: Oracle,
    audit: &Audit,
    seed: u64,
    worker: usize,
    root: &str,
    ops: usize,
) -> Result<()> {
    let mut rng = StdRng::seed_from_u64(seed);
    let prefix = format!("w{worker}");
    let real_prefix = mount_path(mount, root).join(&prefix);
    if !real_prefix.exists() {
        std::fs::create_dir(&real_prefix)?;
        oracle.refresh_prefix(root, &prefix, &real_prefix)?;
    }
    for sequence in 0..ops {
        one_op(
            &mut rng,
            mount,
            &mut oracle,
            audit,
            worker,
            root,
            &prefix,
            sequence,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn one_op(
    rng: &mut StdRng,
    mount: &Path,
    oracle: &mut Oracle,
    audit: &Audit,
    worker: usize,
    root: &str,
    prefix: &str,
    sequence: usize,
) -> Result<()> {
    let root_path = mount_path(mount, root);
    let prefix_path = root_path.join(prefix);
    let inventory = scan(&prefix_path)?;
    let files: Vec<_> = inventory
        .values()
        .filter(|e| e.kind == "file")
        .map(|e| e.path.clone())
        .collect();
    let links: Vec<_> = inventory
        .values()
        .filter(|e| e.kind == "symlink")
        .map(|e| e.path.clone())
        .collect();
    let mut dirs = vec![String::new()];
    dirs.extend(
        inventory
            .values()
            .filter(|e| e.kind == "dir")
            .map(|e| e.path.clone()),
    );
    let op = rng.random_range(0..100);
    let unique = format!("r{}-{sequence}-{}", unix_ms(), rng.random::<u32>());
    let (name, path, args) = match op {
        0..=24 => {
            let parent = dirs.choose(rng).unwrap();
            let rel = join_rel(parent, &format!("f-{unique}"));
            let max = if rng.random_range(0..12) == 0 {
                (2 << 20) + 8192
            } else {
                32 << 10
            };
            let data = random_bytes(rng, max);
            std::fs::write(prefix_path.join(&rel), &data)?;
            ("create", rel, json!({"data_hex":hex_encode(&data)}))
        }
        25..=39 if !files.is_empty() => {
            let rel = files.choose(rng).unwrap().clone();
            let len = std::fs::metadata(prefix_path.join(&rel))?.len();
            let offset = if len == 0 {
                0
            } else {
                rng.random_range(0..len)
            };
            let data = random_bytes(rng, 16 << 10);
            let mut file = OpenOptions::new()
                .write(true)
                .open(prefix_path.join(&rel))?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&data)?;
            (
                "overwrite",
                rel,
                json!({"offset":offset,"data_hex":hex_encode(&data)}),
            )
        }
        40..=49 if !files.is_empty() => {
            let rel = files.choose(rng).unwrap().clone();
            let data = random_bytes(rng, 16 << 10);
            let mut file = OpenOptions::new()
                .append(true)
                .open(prefix_path.join(&rel))?;
            file.write_all(&data)?;
            ("append", rel, json!({"data_hex":hex_encode(&data)}))
        }
        50..=57 if !files.is_empty() => {
            let rel = files.choose(rng).unwrap().clone();
            let old = std::fs::metadata(prefix_path.join(&rel))?.len();
            let size = rng.random_range(0..=old.saturating_add(4096));
            OpenOptions::new()
                .write(true)
                .open(prefix_path.join(&rel))?
                .set_len(size)?;
            ("truncate", rel, json!({"size":size}))
        }
        58..=68 => {
            let parent = dirs.choose(rng).unwrap();
            let rel = join_rel(parent, &format!("d-{unique}"));
            std::fs::create_dir(prefix_path.join(&rel))?;
            ("mkdir", rel, json!({}))
        }
        69..=77 if !files.is_empty() || !links.is_empty() => {
            let mut movable = files.clone();
            movable.extend(links);
            let from = movable.choose(rng).unwrap().clone();
            let parent = dirs.choose(rng).unwrap();
            let to = join_rel(parent, &format!("renamed-{unique}"));
            std::fs::rename(prefix_path.join(&from), prefix_path.join(&to))?;
            ("rename", from, json!({"to":format!("{prefix}/{to}")}))
        }
        78..=85 if !files.is_empty() => {
            let rel = files.choose(rng).unwrap().clone();
            std::fs::remove_file(prefix_path.join(&rel))?;
            ("unlink", rel, json!({}))
        }
        86..=89 => {
            let empty: Vec<_> = dirs
                .iter()
                .filter(|dir| {
                    !dir.is_empty()
                        && std::fs::read_dir(prefix_path.join(dir))
                            .is_ok_and(|mut entries| entries.next().is_none())
                })
                .cloned()
                .collect();
            if let Some(rel) = empty.choose(rng).cloned() {
                std::fs::remove_dir(prefix_path.join(&rel))?;
                ("rmdir", rel, json!({}))
            } else {
                return Ok(());
            }
        }
        90..=94 => {
            let parent = dirs.choose(rng).unwrap();
            let rel = join_rel(parent, &format!("l-{unique}"));
            let target = files
                .choose(rng)
                .cloned()
                .unwrap_or_else(|| "dangling".into());
            std::os::unix::fs::symlink(&target, prefix_path.join(&rel))?;
            ("symlink", rel, json!({"target":target}))
        }
        _ => {
            let mut candidates = files;
            candidates.extend(
                dirs.into_iter()
                    .filter(|path| !path.is_empty())
                    .collect::<Vec<_>>(),
            );
            if let Some(rel) = candidates.choose(rng).cloned() {
                let mode = if std::fs::symlink_metadata(prefix_path.join(&rel))?.is_dir() {
                    0o750
                } else if rng.random_bool(0.5) {
                    0o640
                } else {
                    0o750
                };
                std::fs::set_permissions(
                    prefix_path.join(&rel),
                    std::fs::Permissions::from_mode(mode),
                )?;
                ("chmod", rel, json!({"mode":mode}))
            } else {
                return Ok(());
            }
        }
    };
    oracle.refresh_prefix(root, prefix, &prefix_path)?;
    audit.event(
        Some(worker),
        name,
        Some(root),
        Some(&format!("{prefix}/{path}")),
        args,
    )
}

fn create_snapshot(
    client: &Client,
    oracle: &mut Oracle,
    audit: &Audit,
    id: &str,
    root: &str,
    name: &str,
) -> Result<Snapshot> {
    let selector = format!("{root}@{name}");
    client.snapshot_create(&selector)?;
    let frozen = snapshot_path(&client.mnt, root, name);
    oracle.freeze(id, name, root, &frozen)?;
    audit.event(
        None,
        "snapshot_create",
        Some(root),
        None,
        json!({"id":id,"name":name}),
    )?;
    Ok(Snapshot {
        id: id.into(),
        name: name.into(),
        root: root.into(),
    })
}

fn delete_snapshot(
    client: &Client,
    oracle: &mut Oracle,
    audit: &Audit,
    snap: &Snapshot,
) -> Result<()> {
    let frozen = snapshot_path(&client.mnt, &snap.root, &snap.name);
    client.snapshot_delete(&format!("{}@{}", snap.root, snap.name))?;
    oracle.delete_snapshot(&snap.id)?;
    std::thread::sleep(Duration::from_millis(1100));
    anyhow::ensure!(
        std::fs::symlink_metadata(&frozen).is_err(),
        "deleted snapshot {}@{} still resolves",
        snap.root,
        snap.name
    );
    audit.event(
        None,
        "snapshot_delete",
        Some(&snap.root),
        None,
        json!({"id":snap.id,"name":snap.name}),
    )
}

fn create_clone(
    client: &Client,
    oracle: &mut Oracle,
    audit: &Audit,
    snap: &Snapshot,
    dest: &str,
    id: &str,
) -> Result<()> {
    let selector = format!("{}@{}", snap.root, snap.name);
    client.clone_snapshot(&selector, dest)?;
    oracle.create_clone(id, dest, &snap.id)?;
    let actual = scan(&mount_path(&client.mnt, dest))?;
    // Frozen lookup masks write bits to enforce EROFS, while cloning restores
    // the encoded live modes. Seed from snap_entry, then record the writable
    // materialization's observable metadata.
    oracle.refresh_root(dest, &mount_path(&client.mnt, dest))?;
    compare(
        &format!("new clone {dest}"),
        &oracle.expected_live(dest)?,
        &actual,
    )?;
    audit.event(
        None,
        "clone_create",
        Some(dest),
        None,
        json!({"id":id,"selector":selector,"snap_id":snap.id}),
    )
}

fn delete_clone(client: &Client, oracle: &mut Oracle, audit: &Audit, root: &str) -> Result<()> {
    let real = mount_path(&client.mnt, root);
    make_tree_writable(&real)?;
    remove_tree(&real).with_context(|| format!("removing clone {root}"))?;
    oracle.delete_clone(root)?;
    anyhow::ensure!(
        !mount_path(&client.mnt, root).exists(),
        "deleted clone {root} remains"
    );
    audit.event(None, "clone_delete", Some(root), None, json!({}))
}

fn remove_root(client: &Client, oracle: &mut Oracle, audit: &Audit, root: &str) -> Result<()> {
    let real = mount_path(&client.mnt, root);
    make_tree_writable(&real)?;
    remove_tree(&real).with_context(|| format!("removing live root {root}"))?;
    oracle.refresh_root(root, &mount_path(&client.mnt, root))?;
    anyhow::ensure!(oracle.expected_live(root)?.is_empty());
    audit.event(None, "cleanup_root", Some(root), None, json!({}))
}

fn forced_marker(client: &Client, oracle: &mut Oracle, audit: &Audit, clone: &str) -> Result<()> {
    for (root, bytes) in [
        ("/tree", b"origin-marker-v2".as_slice()),
        (clone, b"clone-marker-v2-different".as_slice()),
    ] {
        std::fs::write(mount_path(&client.mnt, root).join("marker"), bytes)?;
        oracle.refresh_root(root, &mount_path(&client.mnt, root))?;
        audit.event(
            None,
            "overwrite",
            Some(root),
            Some("marker"),
            json!({"offset":0,"truncate":true,"data_hex":hex_encode(bytes)}),
        )?;
    }
    let origin = std::fs::read(mount_path(&client.mnt, "/tree").join("marker"))?;
    let branch = std::fs::read(mount_path(&client.mnt, clone).join("marker"))?;
    anyhow::ensure!(
        blake3::hash(&origin) != blake3::hash(&branch),
        "origin and clone marker hashes did not diverge"
    );
    Ok(())
}

fn replay(path: &Path, no_sleep: bool) -> Result<()> {
    let file =
        BufReader::new(File::open(path).with_context(|| format!("opening {}", path.display()))?);
    let mut lines = file.lines();
    let header: Header = serde_json::from_str(&lines.next().context("empty audit trail")??)?;
    anyhow::ensure!(header.kind == "header", "audit trail has no header");

    let env = S3Env::start().context("starting replay S3 environment")?;
    let _proxy = env.s3_proxy()?;
    let temp = tempfile::Builder::new()
        .prefix("harness-snapshot-churn-replay-")
        .tempdir()?;
    let prefix = format!("snapshot-churn-replay-{}", unix_ms());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut client = Client::new(temp.path(), "replay", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_LEASE_TTL_MS", "200")
        .with_env("CONSTELLATION_GC_HORIZON_S", "0");
    client.fs_create()?;
    client.mount()?;
    let mut oracle = Oracle::open(&temp.path().join("oracle.db"))?;
    let mut previous = 0;
    let mut events = 0u64;
    for line in lines {
        let event: Event = serde_json::from_str(&line?)?;
        if !no_sleep && event.t_ms > previous {
            std::thread::sleep(Duration::from_millis(event.t_ms - previous));
        }
        previous = event.t_ms;
        apply_event(&event, &client, &mut oracle, &env, &prefix)?;
        events += 1;
    }
    client.unmount()?;
    eprintln!(
        "    snapshot-churn replay: events={events} source={} timing={}",
        path.display(),
        if no_sleep { "disabled" } else { "recorded" }
    );
    Ok(())
}

fn apply_event(
    event: &Event,
    client: &Client,
    oracle: &mut Oracle,
    env: &S3Env,
    bucket_prefix: &str,
) -> Result<()> {
    let root = event.root.as_deref().unwrap_or("");
    let rel = event.path.as_deref().unwrap_or("");
    let path = mount_path(&client.mnt, root).join(rel);
    match event.op.as_str() {
        "init" => {
            std::fs::create_dir(client.mnt.join("tree"))?;
            for worker in 0..event.args["workers"].as_u64().unwrap() {
                std::fs::create_dir(client.mnt.join(format!("tree/w{worker}")))?;
            }
            std::fs::write(client.mnt.join("tree/marker"), b"base-marker")?;
            oracle.refresh_root("/tree", &client.mnt.join("tree"))?;
        }
        "create" => std::fs::write(&path, hex_decode(arg_str(event, "data_hex")?)?)?,
        "overwrite" => {
            let data = hex_decode(arg_str(event, "data_hex")?)?;
            let mut file = OpenOptions::new().write(true).open(&path)?;
            if event.args["truncate"].as_bool() == Some(true) {
                file.set_len(0)?;
            }
            file.seek(SeekFrom::Start(event.args["offset"].as_u64().unwrap()))?;
            file.write_all(&data)?;
        }
        "append" => OpenOptions::new()
            .append(true)
            .open(&path)?
            .write_all(&hex_decode(arg_str(event, "data_hex")?)?)?,
        "truncate" => OpenOptions::new()
            .write(true)
            .open(&path)?
            .set_len(event.args["size"].as_u64().unwrap())?,
        "mkdir" => std::fs::create_dir(&path)?,
        "rename" => std::fs::rename(
            &path,
            mount_path(&client.mnt, root).join(arg_str(event, "to")?),
        )?,
        "unlink" => std::fs::remove_file(&path)?,
        "rmdir" => std::fs::remove_dir(&path)?,
        "symlink" => std::os::unix::fs::symlink(arg_str(event, "target")?, &path)?,
        "chmod" => std::fs::set_permissions(
            &path,
            std::fs::Permissions::from_mode(event.args["mode"].as_u64().unwrap() as u32),
        )?,
        "snapshot_create" => {
            let id = arg_str(event, "id")?;
            let name = arg_str(event, "name")?;
            client.snapshot_create(&format!("{root}@{name}"))?;
            oracle.freeze(id, name, root, &snapshot_path(&client.mnt, root, name))?;
            return Ok(());
        }
        "snapshot_delete" => {
            let id = arg_str(event, "id")?;
            let name = arg_str(event, "name")?;
            client.snapshot_delete(&format!("{root}@{name}"))?;
            oracle.delete_snapshot(id)?;
            return Ok(());
        }
        "clone_create" => {
            client.clone_snapshot(arg_str(event, "selector")?, root)?;
            oracle.create_clone(arg_str(event, "id")?, root, arg_str(event, "snap_id")?)?;
            oracle.refresh_root(root, &mount_path(&client.mnt, root))?;
            return Ok(());
        }
        "clone_delete" => {
            let real = mount_path(&client.mnt, root);
            make_tree_writable(&real)?;
            remove_tree(&real)?;
            oracle.delete_clone(root)?;
            return Ok(());
        }
        "cleanup_root" => {
            let real = mount_path(&client.mnt, root);
            make_tree_writable(&real)?;
            remove_tree(&real)?;
            oracle.refresh_root(root, &mount_path(&client.mnt, root))?;
            return Ok(());
        }
        "verify" => {
            oracle.verify_all(&client.mnt)?;
            return Ok(());
        }
        "gc" => {
            assert_replica_clean(client)?;
            assert_prefix_empty(
                &env.direct_endpoint,
                &format!("{bucket_prefix}/snaps/"),
                "replay snaps/",
            )?;
            let output = client.gc_run()?;
            anyhow::ensure!(output.status.success(), "replay GC failed");
            assert_prefix_empty(
                &env.direct_endpoint,
                &format!("{bucket_prefix}/chunks/"),
                "replay chunks/",
            )?;
            return Ok(());
        }
        "quiesce" => return Ok(()),
        other => bail!("unknown audit operation {other}"),
    }
    let worker_prefix = rel.split('/').next().unwrap_or(rel);
    if worker_prefix.starts_with('w') {
        oracle.refresh_prefix(
            root,
            worker_prefix,
            &mount_path(&client.mnt, root).join(worker_prefix),
        )?;
    } else {
        oracle.refresh_root(root, &mount_path(&client.mnt, root))?;
    }
    Ok(())
}

/// Asks the *live* daemon (never a second process opening its metadata
/// store directly — `fjall` enforces single-process access with a lock
/// file, unlike the old SQLite/WAL engine, which tolerated a second
/// read-only connection) whether it still holds any snapshots or churn
/// roots.
fn assert_replica_clean(client: &Client) -> Result<()> {
    let snapshots = client.snapshot_count()?;
    anyhow::ensure!(snapshots == 0, "replica reports {snapshots} snapshot(s)");

    let is_churn_root = |name: &str| {
        name == "tree"
            || (name.starts_with('c')
                && name.len() > 1
                && name[1..].bytes().all(|b| b.is_ascii_digit()))
    };
    let user_roots = std::fs::read_dir(&client.mnt)
        .with_context(|| format!("reading {}", client.mnt.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().is_some_and(is_churn_root))
        .count();
    anyhow::ensure!(user_roots == 0, "replica retains {user_roots} churn roots");
    Ok(())
}

fn assert_prefix_empty(endpoint: &str, prefix: &str, what: &str) -> Result<()> {
    let body = ureq::get(&format!("{endpoint}/{BUCKET}?list-type=2&prefix={prefix}"))
        .call()?
        .into_string()?;
    anyhow::ensure!(!body.contains("<Key>"), "{what} is not empty: {body}");
    Ok(())
}

fn mount_path(mount: &Path, root: &str) -> PathBuf {
    mount.join(root.trim_start_matches('/'))
}

fn snapshot_path(mount: &Path, root: &str, name: &str) -> PathBuf {
    mount_path(mount, root)
        .join(".constellation")
        .join("snapshot")
        .join(name)
}

fn make_tree_writable(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let meta = std::fs::symlink_metadata(&dir)?;
        if meta.mode() & 0o300 != 0o300 {
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("making {} writable", dir.display()))?;
        }
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?
        {
            let path = entry?.path();
            let meta = std::fs::symlink_metadata(&path)?;
            if meta.is_dir() {
                dirs.push(path);
            } else if meta.is_file() {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("making {} writable", path.display()))?;
            }
        }
    }
    Ok(())
}

fn remove_tree(root: &Path) -> Result<()> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.is_dir() {
            remove_tree(&path)?;
        } else {
            std::fs::remove_file(&path).with_context(|| format!("unlinking {}", path.display()))?;
        }
    }
    std::fs::remove_dir(root).with_context(|| format!("removing directory {}", root.display()))?;
    Ok(())
}

fn random_bytes(rng: &mut StdRng, max: usize) -> Vec<u8> {
    let len = rng.random_range(1..=max.max(1));
    let mut bytes = vec![0; len];
    rng.fill(&mut bytes[..]);
    bytes
}

fn join_rel(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.into()
    } else {
        format!("{parent}/{child}")
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn arg_str<'a>(event: &'a Event, key: &str) -> Result<&'a str> {
    event.args[key]
        .as_str()
        .with_context(|| format!("{} event lacks string arg {key}", event.op))
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(text.len().is_multiple_of(2), "odd-length hex payload");
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => bail!("invalid hex digit"),
            };
            Ok((digit(pair[0])? << 4) | digit(pair[1])?)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_tree(root: &Path, name: &str, bytes: &[u8]) {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(root.join(name), bytes).unwrap();
    }

    #[test]
    fn freeze_is_independent_of_live_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let mut oracle = Oracle::open(&temp.path().join("o.db")).unwrap();
        let live = temp.path().join("live");
        let frozen = temp.path().join("frozen");
        file_tree(&live, "x", b"old");
        file_tree(&frozen, "x", b"old");
        oracle.refresh_root("/tree", &live).unwrap();
        oracle.freeze("s", "s", "/tree", &frozen).unwrap();
        std::fs::write(live.join("x"), b"new").unwrap();
        oracle.refresh_root("/tree", &live).unwrap();
        assert_ne!(
            oracle.expected_live("/tree").unwrap()["x"].hash,
            oracle.expected_snapshot("s").unwrap()["x"].hash
        );
    }

    #[test]
    fn clone_copy_diverges_and_survives_source_delete() {
        let temp = tempfile::tempdir().unwrap();
        let mut oracle = Oracle::open(&temp.path().join("o.db")).unwrap();
        let frozen = temp.path().join("frozen");
        file_tree(&frozen, "x", b"base");
        oracle.freeze("s", "s", "/tree", &frozen).unwrap();
        oracle.create_clone("c", "/c1", "s").unwrap();
        let clone = temp.path().join("clone");
        file_tree(&clone, "x", b"branch");
        oracle.refresh_root("/c1", &clone).unwrap();
        let branch = oracle.expected_live("/c1").unwrap();
        oracle.delete_snapshot("s").unwrap();
        assert_eq!(oracle.expected_live("/c1").unwrap(), branch);
    }

    #[test]
    fn deleting_clone_does_not_change_origin_or_other_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let mut oracle = Oracle::open(&temp.path().join("o.db")).unwrap();
        let tree = temp.path().join("tree");
        file_tree(&tree, "x", b"base");
        oracle.refresh_root("/tree", &tree).unwrap();
        oracle.freeze("s", "s", "/tree", &tree).unwrap();
        oracle.create_clone("c", "/c1", "s").unwrap();
        let live = oracle.expected_live("/tree").unwrap();
        let snap = oracle.expected_snapshot("s").unwrap();
        oracle.delete_clone("/c1").unwrap();
        assert_eq!(oracle.expected_live("/tree").unwrap(), live);
        assert_eq!(oracle.expected_snapshot("s").unwrap(), snap);
    }

    #[test]
    fn clone_of_clone_copy_is_isolated() {
        let temp = tempfile::tempdir().unwrap();
        let mut oracle = Oracle::open(&temp.path().join("o.db")).unwrap();
        let tree = temp.path().join("tree");
        file_tree(&tree, "x", b"base");
        oracle.refresh_root("/tree", &tree).unwrap();
        oracle.freeze("s0", "s0", "/tree", &tree).unwrap();
        oracle.create_clone("c1", "/c1", "s0").unwrap();
        oracle.freeze("s1", "s1", "/c1", &tree).unwrap();
        oracle.create_clone("c2", "/c2", "s1").unwrap();
        let changed = temp.path().join("changed");
        file_tree(&changed, "x", b"changed");
        oracle.refresh_root("/c2", &changed).unwrap();
        assert_ne!(
            oracle.expected_live("/c1").unwrap()["x"].hash,
            oracle.expected_live("/c2").unwrap()["x"].hash
        );
    }

    #[test]
    fn trail_fixture_with_clone_round_trips() {
        let event = Event {
            kind: "event".into(),
            t_ms: 12,
            worker: None,
            op: "clone_create".into(),
            root: Some("/c1".into()),
            path: None,
            args: json!({"id":"c1","selector":"/tree@s0","snap_id":"s0"}),
        };
        let encoded = serde_json::to_string(&event).unwrap();
        assert_eq!(serde_json::from_str::<Event>(&encoded).unwrap(), event);
    }
}
