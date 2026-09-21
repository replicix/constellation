//! Node storage: the three residency tiers §0.1 asks for, plus the pack
//! writer §P8 describes.
//!
//! - `Memory`: every node resident and uncompressed — tier (a).
//! - `Packed { resident_interior: true }`: interior nodes resident, leaves
//!   in zstd'd packs on disk behind an LRU — tier (b).
//! - `Packed { resident_interior: false }` after `evict_page_cache` —
//!   tier (c), nothing cached anywhere, every read is a ranged pack read.
//!
//! Packs are the plan 26 Appendix's shape: individually compressed blobs
//! concatenated into a ~1 MiB object with an in-memory `(hash, offset,
//! len)` index, so fetching one node is one ranged read and fetching a
//! clustered run of nodes is one request.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::node::{Hash, NodeRef};

pub const ZSTD_LEVEL: i32 = 3;
pub const DEFAULT_PACK_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Debug)]
pub struct Loc {
    pub pack: u32,
    pub off: u64,
    pub clen: u32,
}

#[derive(Default)]
pub struct Counters {
    pub gets: AtomicU64,
    pub cache_hits: AtomicU64,
    pub pack_reads: AtomicU64,
    pub pack_bytes_read: AtomicU64,
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub put_zbytes: AtomicU64,
    /// Puts whose content was not already stored: the bytes a commit
    /// actually adds to the bucket, after structural sharing and dedup.
    pub new_nodes: AtomicU64,
    pub new_bytes: AtomicU64,
    pub new_zbytes: AtomicU64,
}

impl Counters {
    pub fn reset(&self) {
        for c in [
            &self.gets,
            &self.cache_hits,
            &self.pack_reads,
            &self.pack_bytes_read,
            &self.puts,
            &self.put_bytes,
            &self.put_zbytes,
            &self.new_nodes,
            &self.new_bytes,
            &self.new_zbytes,
        ] {
            c.store(0, Ordering::Relaxed);
        }
    }
}

struct Lru {
    budget: usize,
    used: usize,
    seq: u64,
    map: HashMap<Hash, (Arc<Vec<u8>>, u64)>,
    order: std::collections::BTreeMap<u64, Hash>,
}

impl Lru {
    fn new(budget: usize) -> Lru {
        Lru {
            budget,
            used: 0,
            seq: 0,
            map: HashMap::new(),
            order: std::collections::BTreeMap::new(),
        }
    }

    fn get(&mut self, h: &Hash) -> Option<Arc<Vec<u8>>> {
        let (buf, old) = self.map.get(h).cloned()?;
        self.order.remove(&old);
        self.seq += 1;
        self.order.insert(self.seq, *h);
        self.map.insert(*h, (buf.clone(), self.seq));
        Some(buf)
    }

    fn put(&mut self, h: Hash, buf: Arc<Vec<u8>>) {
        if self.budget == 0 {
            return;
        }
        self.seq += 1;
        self.used += buf.len();
        if let Some((old, oseq)) = self.map.insert(h, (buf, self.seq)) {
            self.used -= old.len();
            self.order.remove(&oseq);
        }
        self.order.insert(self.seq, h);
        while self.used > self.budget {
            let Some((&s, &victim)) = self.order.iter().next() else {
                break;
            };
            self.order.remove(&s);
            if let Some((buf, _)) = self.map.remove(&victim) {
                self.used -= buf.len();
            }
        }
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
        self.used = 0;
    }
}

struct Packs {
    dir: Option<PathBuf>,
    pack_bytes: usize,
    cur: Vec<u8>,
    cur_len: usize,
    cur_nodes: Vec<Hash>,
    files: Vec<Option<File>>,
    /// Per pack: node hashes it holds and its on-disk size. `None` marks
    /// a pack the sweep deleted, so ids stay stable for the index.
    contents: Vec<Option<Vec<Hash>>>,
    sizes: Vec<u64>,
}

pub struct Store {
    /// Resident nodes: everything in `Memory` and logical-pack modes,
    /// interior only in `Packed { resident_interior: true }`.
    mem: RwLock<HashMap<Hash, Arc<Vec<u8>>>>,
    packed: bool,
    /// Pack bookkeeping without pack files: nodes stay resident, but
    /// every node still gets compressed and assigned to a pack so the
    /// §P10b footprint and pack-death accounting are real.
    logical: bool,
    resident_interior: bool,
    index: RwLock<HashMap<Hash, Loc>>,
    packs: Option<RwLock<Packs>>,
    cache: Mutex<Lru>,
    /// Pack ids of recent misses, so a caller can ask "how many distinct
    /// ranged GETs would this operation have cost?" — Appendix A's claim
    /// about a cold `ls -la` is about packs, not nodes.
    trace: Mutex<Option<Vec<u32>>>,
    pub counters: Counters,
}

impl Store {
    pub fn memory() -> Store {
        Store {
            mem: RwLock::new(HashMap::new()),
            packed: false,
            logical: false,
            resident_interior: true,
            index: RwLock::new(HashMap::new()),
            packs: None,
            cache: Mutex::new(Lru::new(0)),
            trace: Mutex::new(None),
            counters: Counters::default(),
        }
    }

    fn with_packs(
        dir: Option<PathBuf>,
        pack_bytes: usize,
        resident_interior: bool,
        cache: usize,
    ) -> Store {
        if let Some(d) = &dir {
            std::fs::create_dir_all(d).expect("pack dir");
        }
        let logical = dir.is_none();
        Store {
            mem: RwLock::new(HashMap::new()),
            packed: true,
            logical,
            resident_interior,
            index: RwLock::new(HashMap::new()),
            packs: Some(RwLock::new(Packs {
                dir,
                pack_bytes,
                cur: Vec::with_capacity(pack_bytes + (1 << 16)),
                cur_len: 0,
                cur_nodes: Vec::new(),
                files: Vec::new(),
                contents: Vec::new(),
                sizes: Vec::new(),
            })),
            cache: Mutex::new(Lru::new(cache)),
            trace: Mutex::new(None),
            counters: Counters::default(),
        }
    }

    pub fn start_trace(&self) {
        *self.trace.lock().unwrap() = Some(Vec::new());
    }

    /// Distinct packs touched since `start_trace`.
    pub fn take_trace(&self) -> usize {
        let t = self.trace.lock().unwrap().take().unwrap_or_default();
        let mut ids: Vec<u32> = t;
        ids.sort_unstable();
        ids.dedup();
        ids.len()
    }

    pub fn packed(dir: PathBuf, pack_bytes: usize, resident_interior: bool, cache: usize) -> Store {
        Store::with_packs(Some(dir), pack_bytes, resident_interior, cache)
    }

    pub fn logical_packs(pack_bytes: usize) -> Store {
        Store::with_packs(None, pack_bytes, true, 0)
    }

    pub fn put(&self, level: u8, bytes: Vec<u8>) -> Hash {
        let h = crate::node::hash_of(&bytes);
        self.counters.puts.fetch_add(1, Ordering::Relaxed);
        self.counters
            .put_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if !self.packed {
            self.counters
                .put_zbytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            let mut mem = self.mem.write().unwrap();
            if mem.insert(h, Arc::new(bytes)).is_none() {
                self.counters.new_nodes.fetch_add(1, Ordering::Relaxed);
            }
            return h;
        }
        if self.logical || (self.resident_interior && level > 0) {
            self.mem.write().unwrap().insert(h, Arc::new(bytes.clone()));
        }
        if self.index.read().unwrap().contains_key(&h) {
            return h;
        }
        let comp = zstd::encode_all(&bytes[..], ZSTD_LEVEL).expect("zstd");
        self.counters
            .put_zbytes
            .fetch_add(comp.len() as u64, Ordering::Relaxed);
        self.counters.new_nodes.fetch_add(1, Ordering::Relaxed);
        self.counters
            .new_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.counters
            .new_zbytes
            .fetch_add(comp.len() as u64, Ordering::Relaxed);
        let mut p = self.packs.as_ref().unwrap().write().unwrap();
        let pack = p.contents.len() as u32;
        let off = p.cur_len as u64;
        p.cur_len += comp.len();
        if !self.logical {
            p.cur.extend_from_slice(&comp);
        }
        p.cur_nodes.push(h);
        self.index.write().unwrap().insert(
            h,
            Loc {
                pack,
                off,
                clen: comp.len() as u32,
            },
        );
        if p.cur_len >= p.pack_bytes {
            seal(&mut p);
        }
        h
    }

    pub fn flush(&self) {
        if let Some(packs) = &self.packs {
            let mut p = packs.write().unwrap();
            if p.cur_len > 0 {
                seal(&mut p);
            }
        }
    }

    pub fn get(&self, h: &Hash) -> Arc<Vec<u8>> {
        self.counters.gets.fetch_add(1, Ordering::Relaxed);
        if let Some(buf) = self.mem.read().unwrap().get(h) {
            self.counters.cache_hits.fetch_add(1, Ordering::Relaxed);
            return buf.clone();
        }
        if let Some(buf) = self.cache.lock().unwrap().get(h) {
            self.counters.cache_hits.fetch_add(1, Ordering::Relaxed);
            return buf;
        }
        let loc = *self.index.read().unwrap().get(h).expect("unknown node");
        let mut comp = vec![0u8; loc.clen as usize];
        {
            let p = self.packs.as_ref().unwrap().read().unwrap();
            if (loc.pack as usize) < p.files.len() {
                p.files[loc.pack as usize]
                    .as_ref()
                    .expect("pack file")
                    .read_exact_at(&mut comp, loc.off)
                    .expect("pack read");
            } else {
                // Still in the pack being filled: a writer reads its own
                // unsealed nodes out of the buffer, as it would out of the
                // spool file before the PUT.
                let at = loc.off as usize;
                comp.copy_from_slice(&p.cur[at..at + loc.clen as usize]);
            }
        }
        self.counters.pack_reads.fetch_add(1, Ordering::Relaxed);
        if let Some(t) = self.trace.lock().unwrap().as_mut() {
            t.push(loc.pack);
        }
        self.counters
            .pack_bytes_read
            .fetch_add(loc.clen as u64, Ordering::Relaxed);
        let buf = Arc::new(zstd::decode_all(&comp[..]).expect("zstd decode"));
        self.cache.lock().unwrap().put(*h, buf.clone());
        buf
    }

    /// Total resident bytes (uncompressed encoded nodes).
    pub fn resident_bytes(&self) -> u64 {
        self.mem
            .read()
            .unwrap()
            .values()
            .map(|v| v.len() as u64)
            .sum()
    }

    /// Tier (c) prep: forget the resident interior, so every read comes
    /// from a pack. Only valid on a packed store, where the nodes are
    /// also on disk.
    pub fn drop_resident(&self) {
        assert!(self.packed && !self.logical);
        self.mem.write().unwrap().clear();
    }

    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }

    pub fn set_cache_budget(&self, budget: usize) {
        let mut c = self.cache.lock().unwrap();
        c.clear();
        c.budget = budget;
    }

    /// Tier (c): tell the kernel to forget the packs so a "cold" read is
    /// a real device read rather than a page-cache hit.
    pub fn evict_page_cache(&self) {
        self.clear_cache();
        if let Some(packs) = &self.packs {
            let p = packs.read().unwrap();
            for f in p.files.iter().flatten() {
                unsafe {
                    use std::os::unix::io::AsRawFd;
                    libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
                }
            }
        }
    }

    pub fn pack_count(&self) -> usize {
        self.packs
            .as_ref()
            .map(|p| {
                p.read()
                    .unwrap()
                    .contents
                    .iter()
                    .filter(|c| c.is_some())
                    .count()
            })
            .unwrap_or(0)
    }

    /// Bytes the bucket is holding: the sealed size of every pack that
    /// has not been swept.
    pub fn pack_bytes_total(&self) -> u64 {
        self.packs
            .as_ref()
            .map(|p| {
                let p = p.read().unwrap();
                p.contents
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.is_some())
                    .map(|(i, _)| p.sizes[i])
                    .sum()
            })
            .unwrap_or(0)
    }

    /// `(whole-dead packs, partially dead packs, live bytes, dead bytes)`
    /// under a live node set — the §P10b "die whole vs need compaction"
    /// measurement.
    pub fn pack_liveness(
        &self,
        live: &std::collections::HashSet<Hash>,
    ) -> (usize, usize, u64, u64) {
        let index = self.index.read().unwrap();
        let p = self.packs.as_ref().unwrap().read().unwrap();
        let (mut whole_dead, mut partial, mut live_b, mut dead_b) = (0, 0, 0u64, 0u64);
        for nodes in p.contents.iter().flatten() {
            let (mut l, mut d) = (0u64, 0u64);
            for h in nodes {
                let clen = index.get(h).map(|x| x.clen as u64).unwrap_or(0);
                if live.contains(h) {
                    l += clen;
                } else {
                    d += clen;
                }
            }
            live_b += l;
            dead_b += d;
            if l == 0 {
                whole_dead += 1;
            } else if d > 0 {
                partial += 1;
            }
        }
        (whole_dead, partial, live_b, dead_b)
    }

    /// Rewrite the deadest packs — the other half of §P10's sweep, and the
    /// reason ADR-11 called packfiles "GC becomes compaction". Packs whose
    /// live fraction is below `threshold` have their live nodes appended
    /// to the current pack and are then deleted, up to a byte budget so
    /// the compactor can be rate-limited. Returns
    /// `(packs rewritten, live bytes rewritten, bytes freed)`.
    pub fn compact(
        &self,
        live: &std::collections::HashSet<Hash>,
        threshold: f64,
        budget_bytes: u64,
    ) -> (usize, u64, u64) {
        self.compact_par(live, threshold, budget_bytes, 1)
    }

    /// Same as `compact`, with the per-pack live-node decode parallelized
    /// across `threads`. The append of survivors stays serial (one pack
    /// writer); the expensive part is the ranged read + zstd decode.
    pub fn compact_par(
        &self,
        live: &std::collections::HashSet<Hash>,
        threshold: f64,
        budget_bytes: u64,
        threads: usize,
    ) -> (usize, u64, u64) {
        use rayon::prelude::*;
        let mut victims: Vec<(usize, f64, u64)> = Vec::new();
        {
            let index = self.index.read().unwrap();
            let p = self.packs.as_ref().unwrap().read().unwrap();
            for (i, nodes) in p.contents.iter().enumerate() {
                let Some(nodes) = nodes else { continue };
                let (mut l, mut tot) = (0u64, 0u64);
                for h in nodes {
                    let clen = index.get(h).map(|x| x.clen as u64).unwrap_or(0);
                    tot += clen;
                    if live.contains(h) {
                        l += clen;
                    }
                }
                if tot > 0 && l > 0 && (l as f64 / tot as f64) < threshold {
                    victims.push((i, l as f64 / tot as f64, l));
                }
            }
        }
        victims.sort_by(|a, b| a.1.total_cmp(&b.1));
        let (mut packs, mut rewritten, mut freed) = (0usize, 0u64, 0u64);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads.max(1))
            .build()
            .expect("rayon pool");
        for (i, _, live_bytes) in victims {
            if rewritten + live_bytes > budget_bytes {
                break;
            }
            let nodes: Vec<Hash> = {
                let p = self.packs.as_ref().unwrap().read().unwrap();
                p.contents[i].clone().unwrap_or_default()
            };
            let survivors: Vec<(Hash, Arc<Vec<u8>>)> = pool.install(|| {
                nodes
                    .par_iter()
                    .filter(|h| live.contains(*h))
                    .map(|h| (*h, self.get(h)))
                    .collect()
            });
            {
                let mut index = self.index.write().unwrap();
                let mut p = self.packs.as_ref().unwrap().write().unwrap();
                for h in &nodes {
                    index.remove(h);
                }
                freed += p.sizes[i];
                p.contents[i] = None;
                if let Some(dir) = p.dir.clone() {
                    let _ = std::fs::remove_file(dir.join(format!("{i:08}.pack")));
                }
                if i < p.files.len() {
                    p.files[i] = None;
                }
            }
            for (h, buf) in survivors {
                rewritten += buf.len() as u64;
                self.mem.write().unwrap().remove(&h);
                self.put(NodeRef::new(&buf).level, buf.as_ref().clone());
            }
            packs += 1;
        }
        (packs, rewritten, freed)
    }

    /// Delete every pack with no live node in it (zero rewrite cost) and
    /// forget the nodes they held. Returns `(packs deleted, bytes freed)`.
    pub fn sweep_packs(&self, live: &std::collections::HashSet<Hash>) -> (usize, u64) {
        let mut index = self.index.write().unwrap();
        let mut mem = self.mem.write().unwrap();
        let mut p = self.packs.as_ref().unwrap().write().unwrap();
        let (mut n, mut bytes) = (0usize, 0u64);
        for i in 0..p.contents.len() {
            let Some(nodes) = &p.contents[i] else {
                continue;
            };
            if nodes.iter().any(|h| live.contains(h)) {
                continue;
            }
            for h in nodes {
                index.remove(h);
                mem.remove(h);
            }
            bytes += p.sizes[i];
            n += 1;
            p.contents[i] = None;
            if let (Some(dir), Some(_)) = (p.dir.clone(), p.files.get(i)) {
                let _ = std::fs::remove_file(dir.join(format!("{i:08}.pack")));
            }
            if i < p.files.len() {
                p.files[i] = None;
            }
        }
        (n, bytes)
    }
}

fn seal(p: &mut Packs) {
    let id = p.contents.len();
    match &p.dir {
        Some(dir) => {
            let path = dir.join(format!("{id:08}.pack"));
            let mut f = File::create(&path).expect("create pack");
            f.write_all(&p.cur).expect("write pack");
            f.sync_all().ok();
            drop(f);
            p.files.push(Some(File::open(&path).expect("reopen pack")));
        }
        None => p.files.push(None),
    }
    p.sizes.push(p.cur_len as u64);
    p.contents.push(Some(std::mem::take(&mut p.cur_nodes)));
    p.cur.clear();
    p.cur_len = 0;
}
