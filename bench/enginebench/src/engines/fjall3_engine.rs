//! fjall 3.x candidate — same role as `fjall_engine.rs` (fjall 2.11.2)
//! but against the rewritten v3 engine (renamed dependency `fjall3`,
//! `package = "fjall"`, `version = "3"`, resolved to **3.1.10** per
//! `Cargo.lock`), added specifically to test whether v3's block-format
//! rewrite fixes the memory-scaling finding v2 produced in this
//! benchmark (see RESULTS.md "fjall 2 -> 3").
//!
//! v3 renames throughout: `Config`/`Keyspace` (the whole store) become
//! `Database`, `PartitionHandle`/`PartitionCreateOptions` (one column
//! family) become `Keyspace`/`KeyspaceCreateOptions`, and the
//! constructor is `Database::builder(path)` instead of `Config::new`.
//! Verified against `/tmp/fjall` (the v3 source tree) at `3.1.10`:
//! `src/db.rs`, `src/builder.rs`, `src/keyspace/{mod,options}.rs`,
//! `src/batch/mod.rs`.
//!
//! Durability semantics are unchanged from the v2 engine: every op goes
//! through a `WriteBatch` (`db.batch()` / `.insert`/`.remove` /
//! `.commit()`), flushed to OS buffers by default and fsync'd only on
//! `flush()` (`keyspace.rotate_memtable_and_wait()`, mirroring the v2
//! engine's use of the same call) — i.e. "durable within the group
//! commit window", the same allowance the other three engines get.
//!
//! Same `constellation_mtree::keys`/`record` encoding as mtree/redb/the
//! v2 fjall engine (the dentry value carries the attrs copy).
//!
//! ## Configuration
//!
//! `Fjall3Engine::create` (the primary, reported variant) sets only
//! `cache_size` — v3's own defaults are already tuned for a point-read-
//! heavy workload (default `filter_policy` is a two-tier bloom filter,
//! `data_block_size_policy` 4 KiB, leveled compaction) and the v3
//! announcement post explicitly recommends leaving levels 0/1
//! uncompressed and using `expect_point_read_hits` only when reads are
//! expected to mostly *hit* — which is exactly this workload's shape
//! (`lookup`/`getattr` on names and inos that usually exist), so that
//! knob is exercised in `Fjall3Engine::create_tuned` instead of the
//! primary variant, to keep the "give every engine a documented,
//! reasonable config" rule honest: the primary number is v3's own
//! judgment of a good default, the tuned number is this benchmark's own
//! guess at doing better for this specific access pattern.
//!
//! `create_tuned` additionally sets `expect_point_read_hits(true)`
//! (skip building a bloom filter on the last, largest level — v3's own
//! docs say this cuts filter memory ~90% and is a straight win whenever
//! most point reads hit, which describes FUSE `lookup`/`getattr` on a
//! live namespace) and a non-zero `data_block_hash_ratio_policy`
//! (v3's new hash-indexed data blocks: point reads that hit the hash
//! index skip the binary search entirely; conflicts fall back to binary
//! search automatically, so this is a pure latency win with a small,
//! bounded memory cost per block).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use constellation_mtree::keys::{self, KeyRange};
use constellation_mtree::record::{self, Attrs, DentryRecord, InodeRecord, Kind, Payload};
use fjall3::config::{HashRatioPolicy, PinningPolicy};
use fjall3::{Database, Keyspace, KeyspaceCreateOptions};

use crate::engine::{Engine, PlusEntry};

pub struct Fjall3Engine {
    db: Database,
    keyspace: Keyspace,
    bytes_written: AtomicU64,
}

impl Fjall3Engine {
    pub fn create(dir: &Path, cache_bytes: u64) -> Fjall3Engine {
        let _ = std::fs::remove_dir_all(dir);
        let db = Database::builder(dir).cache_size(cache_bytes).open().expect("open fjall3 database");
        let keyspace = db.keyspace("kv", KeyspaceCreateOptions::default).expect("open keyspace");
        Fjall3Engine { db, keyspace, bytes_written: AtomicU64::new(0) }
    }

    /// See the module doc: `expect_point_read_hits` + a non-zero
    /// hash-ratio for data blocks, both aimed at this workload's mostly-
    /// hit point-lookup shape.
    pub fn create_tuned(dir: &Path, cache_bytes: u64) -> Fjall3Engine {
        let _ = std::fs::remove_dir_all(dir);
        let db = Database::builder(dir).cache_size(cache_bytes).open().expect("open fjall3 database (tuned)");
        let keyspace = db
            .keyspace("kv", || {
                KeyspaceCreateOptions::default()
                    .expect_point_read_hits(true)
                    .data_block_hash_ratio_policy(HashRatioPolicy::all(0.5))
            })
            .expect("open keyspace (tuned)");
        Fjall3Engine { db, keyspace, bytes_written: AtomicU64::new(0) }
    }

    /// Root-cause probe for the v2->v3 aging regression (RESULTS.md
    /// "fjall 3 tuning"): `perform_write_stall`/`check_write_halt`
    /// (`src/keyspace/write_delay.rs`, `mod.rs::local_backpressure`)
    /// throttle, then halt, writers once the L0 run count reaches
    /// 20/30 — a point read also has to check every L0 run's filter, so
    /// a backed-up L0 explains both the write-throughput collapse and
    /// the post-aging point-read latency cliff. `worker_threads`
    /// defaults to `min(cores, 4)` regardless of this benchmark's
    /// 32-core host; `pin_low_levels` additionally keeps L0-L2's
    /// filter/index blocks resident (`PinningPolicy`) so a backed-up L0
    /// at least doesn't also fault its filters from the block cache.
    pub fn create_custom(
        dir: &Path,
        cache_bytes: u64,
        worker_threads: usize,
        max_memtable_mb: Option<u64>,
        pin_low_levels: bool,
        point_read_tuning: bool,
    ) -> Fjall3Engine {
        let _ = std::fs::remove_dir_all(dir);
        let db = Database::builder(dir)
            .cache_size(cache_bytes)
            .worker_threads(worker_threads)
            .open()
            .expect("open fjall3 database (custom)");
        let keyspace = db
            .keyspace("kv", || {
                let mut opts = KeyspaceCreateOptions::default();
                if let Some(mb) = max_memtable_mb {
                    opts = opts.max_memtable_size(mb * 1024 * 1024);
                }
                if pin_low_levels {
                    opts = opts
                        .filter_block_pinning_policy(PinningPolicy::new([true, true, true, false]))
                        .index_block_pinning_policy(PinningPolicy::new([true, true, true, false]));
                }
                if point_read_tuning {
                    opts = opts.expect_point_read_hits(true).data_block_hash_ratio_policy(HashRatioPolicy::all(0.5));
                }
                opts
            })
            .expect("open keyspace (custom)");
        Fjall3Engine { db, keyspace, bytes_written: AtomicU64::new(0) }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.keyspace.get(key).unwrap().map(|v| v.to_vec())
    }

    fn scan(&self, kr: &KeyRange, from: Vec<u8>, limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let end = kr.end();
        let mut out = Vec::with_capacity(limit.min(1024));
        let iter: Box<dyn Iterator<Item = _>> = if end.is_empty() {
            Box::new(self.keyspace.range(from..))
        } else {
            Box::new(self.keyspace.range(from..end.to_vec()))
        };
        for guard in iter {
            let (k, v) = guard.into_inner().unwrap();
            out.push((k.to_vec(), v.to_vec()));
            if out.len() >= limit {
                break;
            }
        }
        out
    }

    fn apply(&self, edits: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
        let mut batch = self.db.batch();
        let mut bytes = 0u64;
        for (k, v) in &edits {
            match v {
                Some(val) => {
                    bytes += (k.len() + val.len()) as u64;
                    batch.insert(&self.keyspace, k.as_slice(), val.as_slice());
                }
                None => batch.remove(&self.keyspace, k.as_slice()),
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

impl Engine for Fjall3Engine {
    fn name(&self) -> &'static str {
        "fjall3"
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
        let _ = self.keyspace.rotate_memtable_and_wait();
    }

    fn disk_bytes(&self) -> u64 {
        self.db.disk_space().unwrap_or(0)
    }

    fn bytes_written_total(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    fn compaction_debt(&self) -> Option<(usize, usize, usize, usize, f64)> {
        Some((
            self.keyspace.l0_table_count(),
            self.keyspace.table_count(),
            self.db.outstanding_flushes(),
            self.db.active_compactions(),
            self.db.time_compacting().as_secs_f64(),
        ))
    }
}
