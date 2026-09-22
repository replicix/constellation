//! The §11b candidate: `constellation_mtree::Tree` + an in-memory
//! memtable + an append-only WAL, over the `PackedNodeStore` disk-backed
//! node cache.
//!
//! Durability semantics: a write is appended to the WAL and the WAL is
//! fsync'd on a *group-commit* boundary (every [`WAL_BATCH_OPS`] ops or
//! [`WAL_BATCH_MS`] milliseconds, whichever comes first) — not on every
//! op. Data is durable in the WAL within that window, and durable in
//! the tree's pack files only once `flush()` (a "commit") applies the
//! memtable and rolls the WAL. This matches §P5's design and the
//! plan's explicit "a commit per FUSE op is NOT required" allowance.
//!
//! Writers serialize behind one lock. This is not a simplification
//! unique to the benchmark: §11b lists "optimistic commit with
//! structural rebase and read-set capture" (§P3) as future work
//! *outside* the engine swap this plan compares, and today's SQLite
//! engine already serializes on one writer connection. So "concurrent
//! writers" below measures queueing overhead on top of a serialized
//! apply for every engine in this benchmark, not true write
//! parallelism — see RESULTS.md.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use constellation_mtree::hash::NodeHash;
use constellation_mtree::keys::{self, KeyRange};
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind, Payload};
use constellation_mtree::tree::{Edit, Tree};

use crate::engine::{Engine, PlusEntry};
use super::mtree_store::{PackedNodeStore, Wal};

const WAL_BATCH_OPS: u64 = 200;
const WAL_BATCH_MS: u64 = 5;

type Mem = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

struct WalState {
    wal: Wal,
    ops_since_fsync: u64,
    last_fsync: Instant,
}

pub struct MtreeEngine {
    tree: Tree<Arc<PackedNodeStore>>,
    store: Arc<PackedNodeStore>,
    root: RwLock<NodeHash>,
    /// Shared read locks let concurrent readers proceed without
    /// contending each other — only a writer (an op, or `flush`) takes
    /// the exclusive lock, and critical sections are a BTreeMap
    /// insert/take, not I/O. Split from the WAL (below) precisely so a
    /// read never has to fight a writer's WAL append for the same lock.
    mem: RwLock<Mem>,
    wal: Mutex<WalState>,
    fsyncs_forced_by_op: AtomicU64,
    ops_written: AtomicU64,
}

impl MtreeEngine {
    pub fn create(dir: &Path, cache_budget_bytes: usize) -> MtreeEngine {
        std::fs::create_dir_all(dir).unwrap();
        let store = Arc::new(PackedNodeStore::new(dir.join("packs"), cache_budget_bytes));
        let tree = Tree::with_config(store.clone(), record::config()).expect("tree config");
        let root = tree.empty().expect("empty root");
        let wal = Wal::create(dir.join("wal.log"));
        MtreeEngine {
            tree,
            store,
            root: RwLock::new(root),
            mem: RwLock::new(BTreeMap::new()),
            wal: Mutex::new(WalState { wal, ops_since_fsync: 0, last_fsync: Instant::now() }),
            fsyncs_forced_by_op: AtomicU64::new(0),
            ops_written: AtomicU64::new(0),
        }
    }

    fn root(&self) -> NodeHash {
        *self.root.read().unwrap()
    }

    fn tree_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.tree.get(&self.root(), key).unwrap_or(None)
    }

    /// memtable -> tree ladder for one key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        {
            let m = self.mem.read().unwrap();
            if let Some(v) = m.get(key) {
                return v.clone();
            }
        }
        self.tree_get(key)
    }

    fn merged_scan(&self, kr: &KeyRange, from: Vec<u8>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let root = self.root();
        let mem_entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let m = self.mem.read().unwrap();
            let upper = if kr.end().is_empty() { Unbounded } else { Excluded(kr.end().to_vec()) };
            m.range((Included(from.clone()), upper)).map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let want = limit + mem_entries.len();
        let tree_entries = self.tree.range(&root, &from, kr.prefix(), want).unwrap_or_default();

        let mut out = Vec::with_capacity(limit);
        let (mut ti, mut mi) = (0usize, 0usize);
        while out.len() < limit {
            let t = tree_entries.get(ti);
            let m = mem_entries.get(mi);
            match (t, m) {
                (None, None) => break,
                (Some((tk, tv)), None) => {
                    out.push((tk.clone(), tv.clone()));
                    ti += 1;
                }
                (None, Some((mk, mv))) => {
                    if let Some(v) = mv {
                        out.push((mk.clone(), v.clone()));
                    }
                    mi += 1;
                }
                (Some((tk, tv)), Some((mk, mv))) => match tk.cmp(mk) {
                    std::cmp::Ordering::Less => {
                        out.push((tk.clone(), tv.clone()));
                        ti += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        if let Some(v) = mv {
                            out.push((mk.clone(), v.clone()));
                        }
                        mi += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        if let Some(v) = mv {
                            out.push((mk.clone(), v.clone()));
                        }
                        ti += 1;
                        mi += 1;
                    }
                },
            }
        }
        out
    }

    fn dentry_locations(&self, ino: u64) -> Vec<(u64, Vec<u8>)> {
        let kr = keys::names_of(ino);
        self.merged_scan(&kr, kr.start().to_vec(), 10_000)
            .into_iter()
            .filter_map(|(k, _)| {
                if k.len() < 1 + 16 {
                    return None;
                }
                let parent = u64::from_be_bytes(k[9..17].try_into().ok()?);
                let name = k[17..].to_vec();
                Some((parent, name))
            })
            .collect()
    }

    fn read_inode(&self, ino: u64) -> Option<InodeRecord> {
        self.get(&keys::inode(ino)).and_then(|v| InodeRecord::decode(&v).ok())
    }

    fn stage(&self, edits: Vec<Edit>) {
        if edits.is_empty() {
            return;
        }
        {
            let mut w = self.wal.lock().unwrap();
            for (k, v) in &edits {
                w.wal.append(k, v.as_deref());
            }
            w.ops_since_fsync += 1;
            if w.ops_since_fsync >= WAL_BATCH_OPS || w.last_fsync.elapsed() >= Duration::from_millis(WAL_BATCH_MS) {
                w.wal.fsync();
                w.ops_since_fsync = 0;
                w.last_fsync = Instant::now();
                self.fsyncs_forced_by_op.fetch_add(1, Ordering::Relaxed);
            }
        }
        {
            let mut m = self.mem.write().unwrap();
            for (k, v) in edits {
                m.insert(k, v);
            }
        }
        self.ops_written.fetch_add(1, Ordering::Relaxed);
    }

    fn touch_mtime(&self, ino: u64, mtime_ns: i64) -> Vec<Edit> {
        let Some(mut rec) = self.read_inode(ino) else { return Vec::new() };
        rec.attrs.mtime_ns = mtime_ns;
        rec.attrs.ctime_ns = mtime_ns;
        vec![(keys::inode(ino), Some(rec.encode()))]
    }
}

fn dentry_val(ino: u64, attrs: Attrs) -> Vec<u8> {
    DentryRecord::new(ino, attrs).encode()
}

impl Engine for MtreeEngine {
    fn name(&self) -> &'static str {
        "mtree"
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
        self.merged_scan(&kr, from, limit)
            .into_iter()
            .filter_map(|(k, v)| {
                let name = k[9..].to_vec();
                let (ino, kind) = DentryRecord::ino_and_kind(&v).ok()?;
                Some((name, ino, kind))
            })
            .collect()
    }

    fn readdirplus(&self, parent: u64, start_after: Option<&[u8]>, limit: usize) -> Vec<PlusEntry> {
        let kr = keys::dentries_of(parent);
        let from = start_after_key(&kr, parent, start_after);
        self.merged_scan(&kr, from, limit)
            .into_iter()
            .filter_map(|(k, v)| {
                let name = k[9..].to_vec();
                let rec = DentryRecord::decode(&v).ok()?;
                Some(PlusEntry { name, ino: rec.ino, kind: rec.attrs.kind, attrs: rec.attrs })
            })
            .collect()
    }

    fn getxattr(&self, ino: u64, name: &[u8]) -> Option<Vec<u8>> {
        if let Some(rec) = self.read_inode(ino) {
            if let Some((n, v)) = rec.xattrs.iter().find(|(n, _)| n == name) {
                let _ = n;
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
        self.merged_scan(&kr, kr.start().to_vec(), 10_000)
            .into_iter()
            .map(|(k, _)| k[9..].to_vec())
            .collect()
    }

    fn create(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, xattrs: &[(Vec<u8>, Vec<u8>)]) {
        let mut rec = InodeRecord::new(attrs);
        if !xattrs.is_empty() {
            match record::place_xattrs(xattrs) {
                record::XattrPlacement::Inline => rec.xattrs = xattrs.to_vec(),
                record::XattrPlacement::Spilled => {
                    let mut edits = vec![
                        (keys::inode(ino), Some(rec.encode())),
                        (keys::dentry(parent, name), Some(dentry_val(ino, attrs))),
                        (keys::rdentry(ino, parent, name), Some(Vec::new())),
                    ];
                    for (n, v) in xattrs {
                        edits.push((keys::xattr(ino, n), Some(v.clone())));
                    }
                    edits.extend(self.touch_mtime(parent, attrs.mtime_ns));
                    edits.sort_by(|a, b| a.0.cmp(&b.0));
                    edits.dedup_by(|a, b| a.0 == b.0);
                    self.stage(edits);
                    return;
                }
            }
        }
        let mut edits = vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ];
        edits.extend(self.touch_mtime(parent, attrs.mtime_ns));
        edits.sort_by(|a, b| a.0.cmp(&b.0));
        edits.dedup_by(|a, b| a.0 == b.0);
        self.stage(edits);
    }

    fn mkdir(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs) {
        self.create(parent, name, ino, attrs, &[]);
    }

    fn symlink(&self, parent: u64, name: &[u8], ino: u64, attrs: Attrs, target: &[u8]) {
        let mut rec = InodeRecord::new(attrs);
        rec.symlink_target = Some(Payload::Inline(target.to_vec()));
        let mut edits = vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ];
        edits.extend(self.touch_mtime(parent, attrs.mtime_ns));
        edits.sort_by(|a, b| a.0.cmp(&b.0));
        edits.dedup_by(|a, b| a.0 == b.0);
        self.stage(edits);
    }

    fn unlink(&self, parent: u64, name: &[u8]) {
        let Some(v) = self.get(&keys::dentry(parent, name)) else { return };
        let Ok(rec) = DentryRecord::decode(&v) else { return };
        let ino = rec.ino;
        let mut edits = vec![
            (keys::dentry(parent, name), None),
            (keys::rdentry(ino, parent, name), None),
        ];
        if let Some(mut inode_rec) = self.read_inode(ino) {
            if inode_rec.attrs.nlink <= 1 {
                edits.push((keys::inode(ino), None));
            } else {
                inode_rec.attrs.nlink -= 1;
                edits.push((keys::inode(ino), Some(inode_rec.encode())));
            }
        }
        edits.sort_by(|a, b| a.0.cmp(&b.0));
        edits.dedup_by(|a, b| a.0 == b.0);
        self.stage(edits);
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> bool {
        let Some(v) = self.get(&keys::dentry(parent, name)) else { return false };
        let Ok(rec) = DentryRecord::decode(&v) else { return false };
        let ino = rec.ino;
        let kr = keys::dentries_of(ino);
        if !self.merged_scan(&kr, kr.start().to_vec(), 1).is_empty() {
            return false;
        }
        let edits = vec![
            (keys::dentry(parent, name), None),
            (keys::rdentry(ino, parent, name), None),
            (keys::inode(ino), None),
        ];
        self.stage(edits);
        true
    }

    fn rename(&self, old_parent: u64, old_name: &[u8], new_parent: u64, new_name: &[u8]) {
        let Some(v) = self.get(&keys::dentry(old_parent, old_name)) else { return };
        let Ok(rec) = DentryRecord::decode(&v) else { return };
        let now = rec.attrs.mtime_ns;
        let mut edits = vec![
            (keys::dentry(old_parent, old_name), None),
            (keys::dentry(new_parent, new_name), Some(v)),
            (keys::rdentry(rec.ino, old_parent, old_name), None),
            (keys::rdentry(rec.ino, new_parent, new_name), Some(Vec::new())),
        ];
        edits.extend(self.touch_mtime(old_parent, now));
        if new_parent != old_parent {
            edits.extend(self.touch_mtime(new_parent, now));
        }
        edits.sort_by(|a, b| a.0.cmp(&b.0));
        edits.dedup_by(|a, b| a.0 == b.0);
        self.stage(edits);
    }

    fn link(&self, parent: u64, name: &[u8], ino: u64) {
        let Some(mut rec) = self.read_inode(ino) else { return };
        rec.attrs.nlink += 1;
        let edits = vec![
            (keys::inode(ino), Some(rec.encode())),
            (keys::dentry(parent, name), Some(dentry_val(ino, rec.attrs))),
            (keys::rdentry(ino, parent, name), Some(Vec::new())),
        ];
        self.stage(edits);
    }

    fn setattr(&self, ino: u64, attrs: Attrs) {
        let Some(mut rec) = self.read_inode(ino) else { return };
        rec.attrs = attrs;
        let mut edits = vec![(keys::inode(ino), Some(rec.encode()))];
        for (parent, name) in self.dentry_locations(ino) {
            edits.push((keys::dentry(parent, &name), Some(dentry_val(ino, attrs))));
        }
        edits.sort_by(|a, b| a.0.cmp(&b.0));
        edits.dedup_by(|a, b| a.0 == b.0);
        self.stage(edits);
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
                self.stage(vec![(keys::inode(ino), Some(rec.encode()))]);
            }
            record::XattrPlacement::Spilled => {
                rec.xattrs.clear();
                let mut edits = vec![(keys::inode(ino), Some(rec.encode()))];
                edits.push((keys::xattr(ino, name), Some(value.to_vec())));
                self.stage(edits);
            }
        }
    }

    fn removexattr(&self, ino: u64, name: &[u8]) {
        if let Some(mut rec) = self.read_inode(ino) {
            if rec.xattrs.iter().any(|(n, _)| n == name) {
                rec.xattrs.retain(|(n, _)| n != name);
                self.stage(vec![(keys::inode(ino), Some(rec.encode()))]);
                return;
            }
        }
        self.stage(vec![(keys::xattr(ino, name), None)]);
    }

    fn init_root(&self, attrs: Attrs) {
        let rec = InodeRecord::new(attrs);
        self.stage(vec![(keys::inode(1), Some(rec.encode()))]);
        self.flush();
    }

    fn flush(&self) {
        let mut wal = self.wal.lock().unwrap();
        wal.wal.fsync();
        let edits: Vec<Edit> = {
            let mut m = self.mem.write().unwrap();
            if m.is_empty() {
                return;
            }
            std::mem::take(&mut *m).into_iter().collect()
        };
        let root = self.root();
        let new_root = self.tree.apply(&root, &edits).expect("apply");
        *self.root.write().unwrap() = new_root;
        wal.wal.truncate();
        wal.ops_since_fsync = 0;
        wal.last_fsync = Instant::now();
        drop(wal);
        self.store.seal_current_pack();
    }

    fn disk_bytes(&self) -> u64 {
        self.store.on_disk_bytes() + self.wal.lock().unwrap().wal.bytes_since_fsync.load(Ordering::Relaxed)
    }

    fn bytes_written_total(&self) -> u64 {
        self.store.bytes_written.load(Ordering::Relaxed)
            + self.wal.lock().unwrap().wal.bytes_written_total.load(Ordering::Relaxed)
    }

    fn compact(&self) -> Option<(u64, u64)> {
        self.flush();
        let root = self.root();
        let reachable = self.tree.reachable(&[root]).ok()?;
        Some(self.store.compact(&reachable))
    }

    fn cold_leaf_reads(&self, dir: u64, want: usize) -> Option<usize> {
        Some(self.distinct_packs_touched(dir, want))
    }
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

impl MtreeEngine {
    pub fn root_hash(&self) -> NodeHash {
        self.root()
    }
    pub fn store(&self) -> &Arc<PackedNodeStore> {
        &self.store
    }
    pub fn tree(&self) -> &Tree<Arc<PackedNodeStore>> {
        &self.tree
    }
    pub fn distinct_packs_touched(&self, parent: u64, want: usize) -> usize {
        // Re-run a readdir page and count distinct packs the underlying
        // node store's *disk* reads touched, after clearing its cache —
        // a cold `ls -la`'s locality (§14.10-style measurement).
        self.store.cache_clear_for_measurement();
        let before = self.store.disk_reads.load(Ordering::Relaxed);
        let _ = self.readdir(parent, None, want);
        let after = self.store.disk_reads.load(Ordering::Relaxed);
        (after - before) as usize
    }
}
