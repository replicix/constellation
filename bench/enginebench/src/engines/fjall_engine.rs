//! fjall candidate: a pure-Rust LSM engine (RocksDB-shaped internals,
//! no C++ toolchain, MIT-licensed). The second of the 2-4 same-encoding
//! candidates — deliberately a different mechanical family from redb's
//! copy-on-write B-tree, so the bake-off covers both shapes an LSM
//! (compaction debt, write-optimized) and a B-tree (read-optimized,
//! in-place page rewrites) can bring to this workload.
//!
//! Same `constellation_mtree::keys`/`record` encoding as the mtree and
//! redb engines (dentry carries the attr copy).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use constellation_mtree::keys::{self, KeyRange};
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind, Payload};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};

use crate::engine::{Engine, PlusEntry};

pub struct FjallEngine {
    _keyspace: Keyspace,
    part: PartitionHandle,
    bytes_written: AtomicU64,
    dir: std::path::PathBuf,
}

impl FjallEngine {
    pub fn create(dir: &Path, cache_bytes: u64) -> FjallEngine {
        let _ = std::fs::remove_dir_all(dir);
        let keyspace = Config::new(dir).cache_size(cache_bytes).open().expect("open fjall keyspace");
        let part = keyspace.open_partition("kv", PartitionCreateOptions::default()).expect("open partition");
        FjallEngine { _keyspace: keyspace, part, bytes_written: AtomicU64::new(0), dir: dir.to_path_buf() }
    }

    /// A documented, memory-capped variant: RSS was observed (this
    /// benchmark, 1M-entry corpus, aged) sitting at ~3x the configured
    /// `cache_size` — `cache_size` only bounds fjall's block cache, not
    /// the bloom-filter/index-block memory every open segment keeps
    /// resident regardless of that setting, so a fast, heavy-churn
    /// ingest that outruns background compaction accumulates many small
    /// segments and their fixed per-segment overhead. This variant
    /// bounds the memtable total (`max_write_buffer_size`, default is
    /// already a modest 64 MiB and was *not* the driver here, kept
    /// explicit anyway) and gives compaction more worker threads so it
    /// has a better chance of keeping the live segment count — and
    /// therefore the untracked index/filter memory — down.
    pub fn create_tuned(dir: &Path, cache_bytes: u64) -> FjallEngine {
        let _ = std::fs::remove_dir_all(dir);
        let keyspace = Config::new(dir)
            .cache_size(cache_bytes)
            .max_write_buffer_size(32 * 1024 * 1024)
            .compaction_workers(8)
            .flush_workers(4)
            .open()
            .expect("open fjall keyspace (tuned)");
        let part = keyspace.open_partition("kv", PartitionCreateOptions::default()).expect("open partition");
        FjallEngine { _keyspace: keyspace, part, bytes_written: AtomicU64::new(0), dir: dir.to_path_buf() }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.part.get(key).unwrap().map(|v| v.to_vec())
    }

    fn scan(&self, kr: &KeyRange, from: Vec<u8>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let end = kr.end();
        let mut out = Vec::with_capacity(limit.min(1024));
        let iter: Box<dyn Iterator<Item = _>> = if end.is_empty() {
            Box::new(self.part.range(from..))
        } else {
            Box::new(self.part.range(from..end.to_vec()))
        };
        for item in iter {
            let (k, v) = item.unwrap();
            out.push((k.to_vec(), v.to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    fn apply(&self, edits: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let mut batch = self._keyspace.batch();
        let mut bytes = 0u64;
        for (k, v) in &edits {
            match v {
                Some(val) => {
                    bytes += (k.len() + val.len()) as u64;
                    batch.insert(&self.part, k.as_slice(), val.as_slice());
                }
                None => batch.remove(&self.part, k.as_slice()),
            }
        }
        batch.commit().unwrap();
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

impl Engine for FjallEngine {
    fn name(&self) -> &'static str {
        "fjall"
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
        self.apply(vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ]);
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
        let _ = self.part.rotate_memtable_and_wait();
    }

    fn disk_bytes(&self) -> u64 {
        fn dir_size(p: &Path) -> u64 {
            let mut n = 0u64;
            if let Ok(rd) = std::fs::read_dir(p) {
                for e in rd.flatten() {
                    if let Ok(md) = e.metadata() {
                        if md.is_dir() {
                            n += dir_size(&e.path());
                        } else {
                            n += md.len();
                        }
                    }
                }
            }
            n
        }
        dir_size(&self.dir)
    }

    fn bytes_written_total(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
}
