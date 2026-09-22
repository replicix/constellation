//! A disk-backed `NodeStore` for the mtree engine: pack files on /mnt,
//! a size-bounded in-memory LRU node cache, and pack rotation at
//! `CONSTELLATION_PACK_TARGET_BYTES` (4 MiB, matching the shipped
//! default in `docs/reference/configuration.md`) so that "distinct
//! packs touched" is a meaningful locality metric (§P8/§14.10) instead
//! of an artifact of using one big file.
//!
//! This is a benchmark-only stand-in for S4's real node cache
//! (`crates/store-s3/src/node_cache.rs`, which additionally knows about
//! peers and S3). What is kept faithful: a byte-budgeted memory tier
//! over immutable, content-addressed, dedup-on-put nodes, and a
//! packed-object backing store. What is simplified, and noted in
//! RESULTS.md: eviction is a single LRU over all node bytes rather than
//! node_cache.rs's "interior admitted preferentially, budget for
//! interior only" policy — hot interior nodes stay resident here only
//! because they are re-touched by every op, which is a *conservative*
//! (pessimistic) approximation of the real cache.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use constellation_mtree::error::MtreeError;
use constellation_mtree::hash::NodeHash;
use constellation_mtree::store::NodeStore;
use lru::LruCache;

pub const PACK_TARGET_BYTES: usize = 4 * 1024 * 1024;

struct Loc {
    pack: u32,
    offset: u64,
    len: u32,
}

struct PackFile {
    file: File,
    len: u64,
}

/// The memory tier is sharded so that concurrent readers hitting
/// *different* nodes (the common case — §14.9-style scaling) do not
/// serialize on one global LRU mutex. Production's `node_cache.rs`
/// avoids this by never holding a lock across I/O and by admitting only
/// the (small, mostly-static) interior; a single unsharded `Mutex` over
/// every node here was measured to collapse read throughput above ~2
/// threads even on an all-resident tree, which would unfairly indict
/// the tree/WAL design for a benchmark-harness artifact. Sharding is
/// the standard fix (moka/quick_cache do the same) and is disclosed in
/// RESULTS.md as a benchmark-only mechanism, not part of the §11b design.
const CACHE_SHARDS: usize = 64;

pub struct PackedNodeStore {
    dir: PathBuf,
    packs: Mutex<Vec<PackFile>>,
    /// The pack currently being appended to.
    current: Mutex<(u32, Vec<u8>)>,
    index: RwLock<HashMap<NodeHash, Loc>>,
    cache: Vec<Mutex<LruCache<NodeHash, Arc<[u8]>>>>,
    cache_budget_per_shard: usize,
    cached_bytes: Vec<AtomicUsize>,

    // stats
    pub reads: AtomicU64,
    pub disk_reads: AtomicU64,
    pub writes: AtomicU64,
    pub distinct_writes: AtomicU64,
    pub bytes_written: AtomicU64,
}

fn shard_of(hash: &NodeHash) -> usize {
    hash.0[0] as usize % CACHE_SHARDS
}

impl PackedNodeStore {
    pub fn new(dir: impl AsRef<Path>, cache_budget_bytes: usize) -> PackedNodeStore {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).expect("mkdir node store dir");
        PackedNodeStore {
            dir,
            packs: Mutex::new(Vec::new()),
            current: Mutex::new((0, Vec::with_capacity(PACK_TARGET_BYTES))),
            index: RwLock::new(HashMap::new()),
            cache: (0..CACHE_SHARDS).map(|_| Mutex::new(LruCache::unbounded())).collect(),
            cache_budget_per_shard: (cache_budget_bytes / CACHE_SHARDS).max(1),
            cached_bytes: (0..CACHE_SHARDS).map(|_| AtomicUsize::new(0)).collect(),
            reads: AtomicU64::new(0),
            disk_reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            distinct_writes: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
        }
    }

    fn pack_path(&self, id: u32) -> PathBuf {
        self.dir.join(format!("pack-{id:08}.dat"))
    }

    /// Seal the in-progress pack buffer to disk if non-empty, so a
    /// `flush()` boundary always leaves a consistent set of pack files.
    pub fn seal_current_pack(&self) {
        let mut cur = self.current.lock().unwrap();
        if cur.1.is_empty() {
            return;
        }
        let next_id = cur.0 + 1;
        let (id, buf) = std::mem::replace(&mut *cur, (next_id, Vec::with_capacity(PACK_TARGET_BYTES)));
        let path = self.pack_path(id);
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path).expect("create pack");
        f.write_all(&buf).expect("write pack");
        f.sync_data().ok();
        let len = buf.len() as u64;
        let mut packs = self.packs.lock().unwrap();
        // Keep a read handle open for pread.
        let rf = File::open(&path).expect("reopen pack for read");
        while packs.len() <= id as usize {
            packs.push(PackFile { file: File::open("/dev/null").unwrap(), len: 0 });
        }
        packs[id as usize] = PackFile { file: rf, len };
    }

    fn admit(&self, hash: NodeHash, bytes: Arc<[u8]>) {
        let len = bytes.len();
        if len > self.cache_budget_per_shard {
            return;
        }
        let shard = shard_of(&hash);
        let mut cache = self.cache[shard].lock().unwrap();
        cache.put(hash, bytes);
        let mut total = self.cached_bytes[shard].fetch_add(len, Ordering::Relaxed) + len;
        while total > self.cache_budget_per_shard {
            if let Some((_, evicted)) = cache.pop_lru() {
                total -= evicted.len();
                self.cached_bytes[shard].fetch_sub(evicted.len(), Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    /// Drop the memory tier so the next reads measure genuine disk
    /// locality (a cold-cache `ls -la`), without touching the pack
    /// files or index. Benchmark-only: production never does this.
    pub fn cache_clear_for_measurement(&self) {
        for (c, b) in self.cache.iter().zip(self.cached_bytes.iter()) {
            c.lock().unwrap().clear();
            b.store(0, Ordering::Relaxed);
        }
    }

    pub fn node_count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    pub fn on_disk_bytes(&self) -> u64 {
        let packs = self.packs.lock().unwrap();
        let sealed: u64 = packs.iter().map(|p| p.len).sum();
        let cur = self.current.lock().unwrap();
        sealed + cur.1.len() as u64
    }

    /// Compact to only the reachable set: rewrite into a fresh pack
    /// sequence, dropping garbage from superseded commits (§P10 GC by
    /// reachability, modeled here as a synchronous full compaction
    /// rather than the real background reaper/compactor).
    pub fn compact(&self, reachable: &std::collections::BTreeSet<NodeHash>) -> (u64, u64) {
        self.seal_current_pack();
        let before = self.on_disk_bytes();
        let mut new_index = HashMap::with_capacity(reachable.len());
        let mut new_packs: Vec<PackFile> = Vec::new();
        let mut buf = Vec::with_capacity(PACK_TARGET_BYTES);
        let mut pack_id = 0u32;
        {
            let old_index = self.index.read().unwrap();
            let old_packs = self.packs.lock().unwrap();
            for hash in reachable {
                let Some(loc) = old_index.get(hash) else { continue };
                let mut tmp = vec![0u8; loc.len as usize];
                old_packs[loc.pack as usize].file.read_exact_at(&mut tmp, loc.offset).expect("read old pack");
                if buf.len() + tmp.len() > PACK_TARGET_BYTES && !buf.is_empty() {
                    let path = self.dir.join(format!("compact-{pack_id:08}.dat"));
                    std::fs::write(&path, &buf).expect("write compacted pack");
                    new_packs.push(PackFile { file: File::open(&path).unwrap(), len: buf.len() as u64 });
                    pack_id += 1;
                    buf.clear();
                }
                let off = buf.len() as u64;
                buf.extend_from_slice(&tmp);
                new_index.insert(*hash, Loc { pack: pack_id, offset: off, len: tmp.len() as u32 });
            }
        }
        if !buf.is_empty() {
            let path = self.dir.join(format!("compact-{pack_id:08}.dat"));
            std::fs::write(&path, &buf).expect("write compacted pack");
            new_packs.push(PackFile { file: File::open(&path).unwrap(), len: buf.len() as u64 });
        }
        // Remove old pack files from disk.
        for entry in std::fs::read_dir(&self.dir).into_iter().flatten().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("pack-") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        // Rename compact-* to pack-*.
        let mut renamed = Vec::new();
        for (i, _) in new_packs.iter().enumerate() {
            let from = self.dir.join(format!("compact-{i:08}.dat"));
            let to = self.pack_path(i as u32);
            std::fs::rename(&from, &to).expect("rename compacted pack");
            renamed.push(PackFile { file: File::open(&to).unwrap(), len: std::fs::metadata(&to).unwrap().len() });
        }
        *self.packs.lock().unwrap() = renamed;
        *self.current.lock().unwrap() = (new_packs.len() as u32, Vec::with_capacity(PACK_TARGET_BYTES));
        *self.index.write().unwrap() = new_index;
        for (c, b) in self.cache.iter().zip(self.cached_bytes.iter()) {
            c.lock().unwrap().clear();
            b.store(0, Ordering::Relaxed);
        }
        let after = self.on_disk_bytes();
        (before, after)
    }
}

impl NodeStore for PackedNodeStore {
    fn get(&self, hash: &NodeHash) -> Result<Arc<[u8]>, MtreeError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if let Some(bytes) = self.cache[shard_of(hash)].lock().unwrap().get(hash).cloned() {
            return Ok(bytes);
        }
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
        let loc = {
            let index = self.index.read().unwrap();
            match index.get(hash) {
                Some(l) => (l.pack, l.offset, l.len),
                None => return Err(MtreeError::MissingNode(*hash)),
            }
        };
        let (pack_id, offset, len) = loc;
        // The current (unsealed) pack lives in memory only.
        {
            let cur = self.current.lock().unwrap();
            if cur.0 == pack_id {
                let bytes: Arc<[u8]> = cur.1[offset as usize..offset as usize + len as usize].to_vec().into();
                drop(cur);
                self.admit(*hash, bytes.clone());
                return Ok(bytes);
            }
        }
        let mut buf = vec![0u8; len as usize];
        {
            let packs = self.packs.lock().unwrap();
            packs[pack_id as usize].file.read_exact_at(&mut buf, offset).map_err(|_| MtreeError::MissingNode(*hash))?;
        }
        let bytes: Arc<[u8]> = buf.into();
        self.admit(*hash, bytes.clone());
        Ok(bytes)
    }

    fn put(&self, hash: NodeHash, _level: u8, bytes: Vec<u8>) -> Result<(), MtreeError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        if self.index.read().unwrap().contains_key(&hash) {
            return Ok(());
        }
        {
            // Re-check under write intent to avoid a duplicate append
            // if two threads race (benchmark load is single-writer, but
            // keep this correct regardless).
            let mut index = self.index.write().unwrap();
            if index.contains_key(&hash) {
                return Ok(());
            }
            let mut cur = self.current.lock().unwrap();
            if cur.1.len() + bytes.len() > PACK_TARGET_BYTES && !cur.1.is_empty() {
                drop(cur);
                drop(index);
                self.seal_current_pack();
                index = self.index.write().unwrap();
                cur = self.current.lock().unwrap();
            }
            let offset = cur.1.len() as u64;
            let pack_id = cur.0;
            cur.1.extend_from_slice(&bytes);
            index.insert(hash, Loc { pack: pack_id, offset, len: bytes.len() as u32 });
        }
        self.distinct_writes.fetch_add(1, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let arc: Arc<[u8]> = bytes.into();
        self.admit(hash, arc);
        Ok(())
    }
}

/// A simple append-only WAL: postcard-free, fixed framing
/// (`len:u32 | key_len:u32 | key | val_len:i32(-1=delete) | val`),
/// fsync'd on a *batch* boundary rather than per record — group commit,
/// matching the plan's "a commit per FUSE op is NOT required" allowance.
pub struct Wal {
    file: File,
    pub bytes_since_fsync: AtomicU64,
    pub bytes_written_total: AtomicU64,
    pub fsyncs: AtomicU64,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Wal {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(path).expect("create wal");
        Wal { file, bytes_since_fsync: AtomicU64::new(0), bytes_written_total: AtomicU64::new(0), fsyncs: AtomicU64::new(0) }
    }

    /// Append one edit; caller decides when to fsync (`maybe_fsync`).
    pub fn append(&mut self, key: &[u8], val: Option<&[u8]>) -> u64 {
        let mut rec = Vec::with_capacity(9 + key.len() + val.map_or(0, |v| v.len()));
        rec.extend_from_slice(&(key.len() as u32).to_le_bytes());
        rec.extend_from_slice(key);
        match val {
            Some(v) => {
                rec.extend_from_slice(&(v.len() as i32).to_le_bytes());
                rec.extend_from_slice(v);
            }
            None => rec.extend_from_slice(&(-1i32).to_le_bytes()),
        }
        self.file.write_all(&rec).expect("wal append");
        let n = rec.len() as u64;
        self.bytes_since_fsync.fetch_add(n, Ordering::Relaxed);
        self.bytes_written_total.fetch_add(n, Ordering::Relaxed);
        n
    }

    pub fn fsync(&mut self) {
        self.file.sync_data().ok();
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.bytes_since_fsync.store(0, Ordering::Relaxed);
    }

    /// Truncate at a commit boundary: the memtable it protected is now
    /// durable in the tree/pack store instead.
    pub fn truncate(&mut self) {
        self.file.set_len(0).ok();
        self.file.seek(SeekFrom::Start(0)).ok();
    }
}

#[allow(dead_code)]
fn _unused(_r: impl Read) {}
