//! redb candidate: a pure-Rust, single-file, copy-on-write B-tree with
//! MVCC. Chosen (with fjall) as one of the 2-4 "same §P6 encoding"
//! candidates: pure Rust (no C++ toolchain burden, unlike RocksDB),
//! MIT/Apache-licensed, ACID with one writer + many concurrent readers
//! (the same concurrency shape SQLite and this benchmark's mtree engine
//! have), and it already appears in `bench/dbbench`'s prior bake-off so
//! its rough shape on this codebase's data was known going in (see
//! `bench/time-redb.txt`: dbbench's synthetic workload found it by far
//! the slowest of its four engines on write-heavy phases — a signal we
//! re-test here on the real FUSE op mix rather than assume away).
//!
//! Stores the exact `constellation_mtree::keys`/`record` encoding, so
//! its `dentry` value carries the attr copy exactly as the mtree engine's
//! does: this isolates "B-tree vs Merkle prolly tree" from "encoding
//! with vs without the copy", which SQLite's comparison already covers.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use constellation_mtree::keys::{self, KeyRange};
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind, Payload};
#[allow(unused_imports)]
use redb::{Database, ReadableTable, TableDefinition};

use crate::engine::{Engine, PlusEntry};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

pub struct RedbEngine {
    db: Database,
    writer: Mutex<()>,
    bytes_written: AtomicU64,
    path: std::path::PathBuf,
}

impl RedbEngine {
    pub fn create(path: &Path, cache_bytes: usize) -> RedbEngine {
        let _ = std::fs::remove_file(path);
        let db = Database::builder().set_cache_size(cache_bytes).create(path).expect("create redb");
        {
            let txn = db.begin_write().unwrap();
            { let _ = txn.open_table(TABLE).unwrap(); }
            txn.commit().unwrap();
        }
        RedbEngine { db, writer: Mutex::new(()), bytes_written: AtomicU64::new(0), path: path.to_path_buf() }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let txn = self.db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        table.get(key).unwrap().map(|g| g.value().to_vec())
    }

    fn scan(&self, kr: &KeyRange, from: Vec<u8>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let txn = self.db.begin_read().unwrap();
        let table = txn.open_table(TABLE).unwrap();
        let end = kr.end().to_vec();
        let range: Box<dyn Iterator<Item = _>> = if end.is_empty() {
            Box::new(table.range(from.as_slice()..).unwrap())
        } else {
            Box::new(table.range(from.as_slice()..end.as_slice()).unwrap())
        };
        let mut out = Vec::with_capacity(limit.min(1024));
        for item in range {
            let (k, v) = item.unwrap();
            out.push((k.value().to_vec(), v.value().to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    fn apply(&self, edits: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let _g = self.writer.lock().unwrap();
        let mut bytes = 0u64;
        let txn = self.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(TABLE).unwrap();
            for (k, v) in &edits {
                match v {
                    Some(val) => {
                        bytes += (k.len() + val.len()) as u64;
                        table.insert(k.as_slice(), val.as_slice()).unwrap();
                    }
                    None => {
                        table.remove(k.as_slice()).unwrap();
                    }
                }
            }
        }
        txn.commit().unwrap();
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    fn dentry_locations(&self, ino: u64) -> Vec<(u64, Vec<u8>)> {
        let kr = keys::names_of(ino);
        self.scan(&kr, kr.start().to_vec(), 10_000)
            .into_iter()
            .filter_map(|(k, _)| {
                if k.len() < 17 {
                    return None;
                }
                let parent = u64::from_be_bytes(k[9..17].try_into().ok()?);
                Some((parent, k[17..].to_vec()))
            })
            .collect()
    }

    fn read_inode(&self, ino: u64) -> Option<InodeRecord> {
        self.get(&keys::inode(ino)).and_then(|v| InodeRecord::decode(&v).ok())
    }
}

fn dentry_val(ino: u64, attrs: Attrs) -> Vec<u8> {
    DentryRecord::new(ino, attrs).encode()
}

fn start_after_key(kr: &KeyRange, parent: u64, start_after: Option<&[u8]>) -> Vec<u8> {
    match start_after {
        None => kr.start().to_vec(),
        Some(name) => {
            let mut k = keys::dentry(parent, name);
            k.push(0u8);
            k
        }
    }
}

impl Engine for RedbEngine {
    fn name(&self) -> &'static str {
        "redb"
    }

    fn lookup(&self, parent: u64, name: &[u8]) -> Option<(u64, Attrs)> {
        let v = self.get(&keys::dentry(parent, name))?;
        let rec = DentryRecord::decode(&v).ok()?;
        Some((rec.ino, rec.attrs))
    }

    fn getattr(&self, ino: u64) -> Option<Attrs> {
        self.read_inode(ino).map(|r| r.attrs)
    }

    fn readdir(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<(Vec<u8>, u64, Kind)> {
        let kr = keys::dentries_of(parent);
        let from = start_after_key(&kr, parent, start_after);
        self.scan(&kr, from, limit)
            .into_iter()
            .filter_map(|(k, v)| {
                let (ino, kind) = DentryRecord::ino_and_kind(&v).ok()?;
                Some((k[9..].to_vec(), ino, kind))
            })
            .collect()
    }

    fn readdirplus(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<PlusEntry> {
        let kr = keys::dentries_of(parent);
        let from = start_after_key(&kr, parent, start_after);
        self.scan(&kr, from, limit)
            .into_iter()
            .filter_map(|(k, v)| {
                let rec = DentryRecord::decode(&v).ok()?;
                Some(PlusEntry { name: k[9..].to_vec(), ino: rec.ino, kind: rec.attrs.kind, attrs: rec.attrs })
            })
            .collect()
    }

    fn getxattr(&self, ino: u64, name: &[u8]) -> Option<Vec<u8>> {
        if let Some(rec) = self.read_inode(ino) {
            if let Some((_, v)) = rec.xattrs.iter().find(|(n, _)| n == name) {
                return Some(v.clone());
            }
        }
        self.get(&keys::xattr(ino, name))
    }

    fn listxattr(&self, ino: u64) -> Vec<Vec<u8>> {
        if let Some(rec) = self.read_inode(ino) {
            if !rec.xattrs.is_empty() {
                return rec.xattrs.into_iter().map(|(n, _)| n).collect();
            }
        }
        let kr = keys::xattrs_of(ino);
        self.scan(&kr, kr.start().to_vec(), 10_000).into_iter().map(|(k, _)| k[9..].to_vec()).collect()
    }

    fn create(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, xattrs: &[(Vec<u8>, Vec<u8>)]) {
        let mut rec = InodeRecord::new(attrs);
        let mut edits = Vec::new();
        if !xattrs.is_empty() && matches!(record::place_xattrs(xattrs), record::XattrPlacement::Spilled) {
            for (n, v) in xattrs {
                edits.push((keys::xattr(ino, n), Some(v.clone())));
            }
        } else {
            rec.xattrs = xattrs.to_vec();
        }
        edits.push((keys::inode(ino), Some(rec.encode())));
        edits.push((keys::dentry(parent, name), Some(dentry_val(ino, attrs))));
        edits.push((keys::rdentry(ino, parent, name), Some(Vec::new())));
        if let Some(mut p) = self.read_inode(parent) {
            p.attrs.mtime_ns = attrs.mtime_ns;
            p.attrs.ctime_ns = attrs.mtime_ns;
            edits.push((keys::inode(parent), Some(p.encode())));
        }
        self.apply(edits);
    }

    fn mkdir(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs) {
        self.create(parent, name, ino, attrs, &[]);
    }

    fn symlink(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, target: &[u8]) {
        let mut rec = InodeRecord::new(attrs);
        rec.symlink_target = Some(Payload::Inline(target.to_vec()));
        let edits = vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ];
        self.apply(edits);
    }

    fn unlink(&self, parent: u64, name: &[u8]) {
        let Some(v) = self.get(&keys::dentry(parent, name)) else { return };
        let Ok(rec) = DentryRecord::decode(&v) else { return };
        let ino = rec.ino;
        let mut edits = vec![(keys::dentry(parent, name), None), (keys::rdentry(ino, parent, name), None)];
        if let Some(mut inode_rec) = self.read_inode(ino) {
            if inode_rec.attrs.nlink <= 1 {
                edits.push((keys::inode(ino), None));
            } else {
                inode_rec.attrs.nlink -= 1;
                edits.push((keys::inode(ino), Some(inode_rec.encode())));
            }
        }
        self.apply(edits);
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> bool {
        let Some(v) = self.get(&keys::dentry(parent, name)) else { return false };
        let Ok(rec) = DentryRecord::decode(&v) else { return false };
        let ino = rec.ino;
        let kr = keys::dentries_of(ino);
        if !self.scan(&kr, kr.start().to_vec(), 1).is_empty() {
            return false;
        }
        self.apply(vec![(keys::dentry(parent, name), None), (keys::rdentry(ino, parent, name), None), (keys::inode(ino), None)]);
        true
    }

    fn rename(&self, old_parent: u64, old_name: &[u8], new_parent: u64, new_name: &[u8]) {
        let Some(v) = self.get(&keys::dentry(old_parent, old_name)) else { return };
        let Ok(rec) = DentryRecord::decode(&v) else { return };
        let mut edits = vec![
            (keys::dentry(old_parent, old_name), None),
            (keys::dentry(new_parent, new_name), Some(v)),
            (keys::rdentry(rec.ino, old_parent, old_name), None),
            (keys::rdentry(rec.ino, new_parent, new_name), Some(Vec::new())),
        ];
        if let Some(mut p) = self.read_inode(old_parent) {
            p.attrs.mtime_ns = rec.attrs.mtime_ns;
            edits.push((keys::inode(old_parent), Some(p.encode())));
        }
        if new_parent != old_parent {
            if let Some(mut p) = self.read_inode(new_parent) {
                p.attrs.mtime_ns = rec.attrs.mtime_ns;
                edits.push((keys::inode(new_parent), Some(p.encode())));
            }
        }
        self.apply(edits);
    }

    fn link(&self, parent: u64, name: &[u8], ino: u64) {
        let Some(mut rec) = self.read_inode(ino) else { return };
        rec.attrs.nlink += 1;
        self.apply(vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, rec.attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ]);
    }

    fn setattr(&self, ino: u64, attrs: Attrs) {
        let Some(mut rec) = self.read_inode(ino) else { return };
        rec.attrs = attrs;
        let mut edits = vec![(keys::inode(ino), Some(rec.encode()))];
        for (parent, name) in self.dentry_locations(ino) {
            edits.push((keys::dentry(parent, &name), Some(dentry_val(ino, attrs))));
        }
        self.apply(edits);
    }

    fn setxattr(&self, ino: u64, name: &[u8], value: &[u8]) {
        let Some(mut rec) = self.read_inode(ino) else { return };
        let mut set = rec.xattrs.clone();
        if let Some(slot) = set.iter_mut().find(|(n, _)| n == name) {
            slot.1 = value.to_vec();
        } else {
            set.push((name.to_vec(), value.to_vec()));
        }
        set.sort();
        match record::place_xattrs(&set) {
            record::XattrPlacement::Inline => {
                rec.xattrs = set;
                self.apply(vec![(keys::inode(ino), Some(rec.encode()))]);
            }
            record::XattrPlacement::Spilled => {
                rec.xattrs.clear();
                self.apply(vec![(keys::inode(ino), Some(rec.encode())), (keys::xattr(ino, name), Some(value.to_vec()))]);
            }
        }
    }

    fn removexattr(&self, ino: u64, name: &[u8]) {
        if let Some(mut rec) = self.read_inode(ino) {
            if rec.xattrs.iter().any(|(n, _)| n == name) {
                rec.xattrs.retain(|(n, _)| n != name);
                self.apply(vec![(keys::inode(ino), Some(rec.encode()))]);
                return;
            }
        }
        self.apply(vec![(keys::xattr(ino, name), None)]);
    }

    fn init_root(&self, attrs: Attrs) {
        let rec = InodeRecord::new(attrs);
        self.apply(vec![(keys::inode(1), Some(rec.encode()))]);
    }

    fn flush(&self) {
        // redb commits are durable per write-transaction already; there
        // is no separate checkpoint step to model here.
    }

    fn disk_bytes(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }

    fn bytes_written_total(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
}
