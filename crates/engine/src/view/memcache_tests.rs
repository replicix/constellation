//! The read path over the chunk memory cache (`fs-core::memcache`): a
//! cached chunk is loaded and verified once, then served as shared slices;
//! a corrupt disk copy is never served (nor cached) and is refetched; a
//! removed disk entry takes its memory copy with it; E2E (keyed cache,
//! decrypt on fetch) behaves the same.
//!
//! Plus plan 38 §2.3's verify-once model: a fetch admits the bytes it
//! already holds instead of letting the first read load them back, and
//! `--cache-verify` decides whether a disk read re-hashes a chunk file
//! this process has already hashed (`always`) or trusts it (`admit`).

use super::*;
use constellation_fs_core::cache::CacheVerify;
use constellation_fs_core::types::ROOT_INO;
use constellation_store_s3::{create_keyring_block, unlock};
use object_store::memory::InMemory;
use tempfile::TempDir;

const CHUNK: u32 = 1024 * 1024;

struct Env {
    fs: View,
    cache: Arc<DiskCache>,
    store: Arc<ChunkStore>,
    meta: Arc<Meta>,
    rt: tokio::runtime::Runtime,
    dir: TempDir,
}

fn env(e2e: bool) -> Env {
    env_verify(e2e, CacheVerify::Admit)
}

fn env_verify(e2e: bool, verify: CacheVerify) -> Env {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = TempDir::new().unwrap();
    let backend = Arc::new(InMemory::new());
    let (store, cache) = if e2e {
        let block = create_keyring_block("memcache-test").unwrap();
        let keys = unlock(&block, "memcache-test").unwrap();
        let cache =
            DiskCache::open_keyed(dir.path().join("cache"), 1 << 30, *keys.addressing_key());
        (ChunkStore::new_e2e(backend, keys), cache.unwrap())
    } else {
        let cache = DiskCache::open(dir.path().join("cache"), 1 << 30);
        (ChunkStore::new(backend), cache.unwrap())
    };
    let cache = Arc::new(cache.with_memory_cache(64 << 20).with_verify(verify));
    let store = Arc::new(store);
    let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
        meta.clone(),
        store.clone(),
        CHUNK,
        1,
    ));
    let fs_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fs = View::new(
        FsDependencies {
            meta: meta.clone(),
            store: store.clone(),
            cache: cache.clone(),
            rt: fs_rt.handle().clone(),
            sync: None,
            coop: None,
            staging_dir: dir.path().join("staging"),
            staging_budget: StagingBudget::new(1 << 30),
            snapshots,
            atime: Arc::new(crate::atime::AtimeAccumulator::new(
                crate::atime::AtimeMode::Off,
                crate::atime::AtimeStats::new(),
            )),
            prune_stats: crate::prune::PruneStats::new(),
            snapsched_stats: crate::snapsched::SnapSchedStats::new(),
            inflight: crate::kernel_inval::InFlight::disabled(),
            holds: None,
            watch: OpWatch::manual("test-watch", Duration::from_secs(30)),
            caps: FrontendCaps::linux_fuse(false),
            host: constellation_platform::HostServices::native(),
        },
        CHUNK,
        CompressionSetting::RAW,
    );
    std::mem::forget(fs_rt);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Env {
        fs,
        cache,
        store,
        meta,
        rt,
        dir,
    }
}

/// 2.5 chunks of non-repeating bytes, written and flushed (its chunks are
/// in the disk cache as written, dirty), then uploaded and marked clean
/// so a dropped disk copy can be refetched. Returns the inode, the
/// content and its chunks' hashes.
fn file(e: &Env) -> (Ino, Vec<u8>, Vec<ChunkHash>) {
    let data: Vec<u8> = (0..(CHUNK as usize * 5 / 2))
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[3])
        .collect();
    let f = e.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    e.fs.do_write(f.ino, 0, &data).unwrap();
    e.fs.flush_inode(f.ino, false).unwrap();
    let manifest = e.fs.load_manifest(f.ino).unwrap();
    let hashes: Vec<ChunkHash> = e.fs.chunk_list(&manifest).unwrap().into_values().collect();
    assert_eq!(hashes.len(), 3);
    for (i, h) in hashes.iter().enumerate() {
        let start = i * CHUNK as usize;
        let chunk = &data[start..(start + CHUNK as usize).min(data.len())];
        e.rt.block_on(e.store.put_chunk(h, chunk, CompressionSetting::RAW))
            .unwrap();
        e.cache.set_state(h, ChunkState::Clean);
    }
    // Nothing is in memory yet: writes are not admitted.
    assert!(hashes.iter().all(|h| !e.cache.memory_contains(h)));
    (f.ino, data, hashes)
}

fn read(e: &Env, ino: Ino, offset: u64, len: u64) -> ReadData {
    e.fs.do_read(ino, offset, len).unwrap()
}

fn chunk_file(e: &Env, h: &ChunkHash) -> PathBuf {
    let hex = h.to_hex();
    e.dir
        .path()
        .join("cache")
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(&hex)
}

fn cached_sequential_reads_load_each_chunk_once(e2e: bool) {
    let e = env(e2e);
    let (ino, data, hashes) = file(&e);
    let before = e.cache.memory_stats().unwrap();
    // 128 KiB reads, as the kernel's readahead issues them.
    let step = 128 * 1024u64;
    let mut back = Vec::new();
    let mut offset = 0;
    while offset < data.len() as u64 {
        back.extend_from_slice(&read(&e, ino, offset, step).contiguous());
        offset += step;
    }
    assert_eq!(back, data);
    let stats = e.cache.memory_stats().unwrap();
    let reads = data.len() as u64 / step;
    assert_eq!(
        stats.misses - before.misses,
        3,
        "one verified load per chunk"
    );
    assert_eq!(
        stats.hits - before.hits,
        reads - 3,
        "every other read from memory"
    );
    assert!(hashes.iter().all(|h| e.cache.memory_contains(h)));

    // Zero-copy: two reads in one chunk are slices of one shared copy.
    let a = read(&e, ino, 0, 4096);
    let b = read(&e, ino, 8192, 4096);
    assert_eq!(a.segments().len(), 1);
    assert_eq!(
        b.segments()[0].as_ptr() as usize - a.segments()[0].as_ptr() as usize,
        8192
    );
    // A read across a chunk boundary and past EOF is still right.
    let tail = read(&e, ino, u64::from(CHUNK) * 2 - 100, u64::from(CHUNK));
    assert_eq!(&*tail.contiguous(), &data[CHUNK as usize * 2 - 100..]);

    // The memory tier writes nothing to disk: the cache directory holds
    // the chunk files and nothing else (plaintext stays where the disk
    // cache already keeps it).
    let mut files = 0;
    for l1 in std::fs::read_dir(e.dir.path().join("cache")).unwrap() {
        let l1 = l1.unwrap().path();
        if l1.file_name().unwrap() == ".spill" {
            assert_eq!(std::fs::read_dir(&l1).unwrap().count(), 0);
            continue;
        }
        for l2 in std::fs::read_dir(l1).unwrap() {
            files += std::fs::read_dir(l2.unwrap().path()).unwrap().count();
        }
    }
    assert_eq!(files, 3);
}

#[test]
fn cached_sequential_reads_load_each_chunk_once_plain() {
    cached_sequential_reads_load_each_chunk_once(false);
}

#[test]
fn cached_sequential_reads_load_each_chunk_once_e2e() {
    cached_sequential_reads_load_each_chunk_once(true);
}

fn a_corrupt_disk_copy_is_refetched_and_never_served(e2e: bool) {
    // `--cache-verify always`: this process wrote these chunks, so under
    // the default `admit` their files are trusted and not re-hashed — the
    // documented trust-model change, asserted for itself in
    // `admit_serves_a_chunk_this_process_verified_without_rehashing`
    // below. What this test is about is the refetch when a re-hash does
    // catch corruption.
    let e = env_verify(e2e, CacheVerify::Always);
    let (ino, data, hashes) = file(&e);
    // Same length, one byte off: only the hash check can tell.
    let path = chunk_file(&e, &hashes[1]);
    let mut bad = std::fs::read(&path).unwrap();
    bad[1000] ^= 0xff;
    std::fs::write(&path, &bad).unwrap();
    let got = read(&e, ino, u64::from(CHUNK), 64 * 1024);
    assert_eq!(
        &*got.contiguous(),
        &data[CHUNK as usize..CHUNK as usize + 64 * 1024],
        "the corrupt copy was served"
    );
    // Refetched from the store (decrypted, for E2E), re-inserted, and
    // the next read verifies the fresh disk copy and admits it.
    assert_eq!(
        std::fs::read(&path).unwrap()[1000],
        data[CHUNK as usize + 1000]
    );
    let _ = read(&e, ino, u64::from(CHUNK), 4096);
    assert!(e.cache.memory_contains(&hashes[1]));
    let whole = read(&e, ino, 0, data.len() as u64);
    assert_eq!(&*whole.contiguous(), &data[..]);
}

#[test]
fn a_corrupt_disk_copy_is_refetched_and_never_served_plain() {
    a_corrupt_disk_copy_is_refetched_and_never_served(false);
}

#[test]
fn a_corrupt_disk_copy_is_refetched_and_never_served_e2e() {
    a_corrupt_disk_copy_is_refetched_and_never_served(true);
}

// ---- plan 38 §2.3: verify-once ----

/// A store fetch hands its verified bytes to the memory tier itself: the
/// first read of a cold chunk is answered from RAM, and nothing reads the
/// chunk file back (nor hashes it a second time).
fn a_fetched_chunk_is_resident_without_a_second_disk_read(e2e: bool) {
    let e = env(e2e);
    let (ino, data, hashes) = file(&e);
    // Drop every local copy, disk and memory: the next read must fetch.
    for h in &hashes {
        e.cache.remove(h).unwrap();
        assert!(!e.cache.contains(h));
    }
    let before = e.cache.memory_stats().unwrap();
    let got = read(&e, ino, 0, data.len() as u64);
    assert_eq!(&*got.contiguous(), &data[..], "the fetched bytes");
    let after = e.cache.memory_stats().unwrap();
    assert!(
        hashes.iter().all(|h| e.cache.memory_contains(h)),
        "the fetch did not admit what it already held"
    );
    assert_eq!(after.admissions - before.admissions, 3, "one per chunk");
    assert_eq!(
        after.misses, before.misses,
        "a chunk was loaded back off disk after being fetched"
    );
    // And each entry is marked as hashed by this process, so no later
    // read of it re-hashes either.
    assert!(hashes.iter().all(|h| e.cache.is_verified(h) == Some(true)));
    // Every byte of the file still reads back correctly from memory.
    let whole = read(&e, ino, 0, data.len() as u64);
    assert_eq!(&*whole.contiguous(), &data[..]);
    assert_eq!(
        e.cache.memory_stats().unwrap().misses,
        before.misses,
        "the second pass touched the disk"
    );
}

#[test]
fn a_fetched_chunk_is_resident_without_a_second_disk_read_plain() {
    a_fetched_chunk_is_resident_without_a_second_disk_read(false);
}

#[test]
fn a_fetched_chunk_is_resident_without_a_second_disk_read_e2e() {
    a_fetched_chunk_is_resident_without_a_second_disk_read(true);
}

/// The `admit` half of `--cache-verify`: a chunk file this process hashed
/// is trusted afterwards. Corrupting it behind the daemon's back is
/// therefore *not* caught on the next read — the trade plan 38 §2.3 makes
/// explicit, and the reason `always` exists.
#[test]
fn admit_serves_a_chunk_this_process_verified_without_rehashing() {
    // `file` leaves its chunks on disk and nothing in the memory tier, so
    // the read below is a disk read — the one `--cache-verify` decides
    // about.
    let rot = |e: &Env, h: &ChunkHash| {
        let path = chunk_file(e, h);
        let mut bad = std::fs::read(&path).unwrap();
        bad[1000] ^= 0xff;
        std::fs::write(&path, &bad).unwrap();
        bad
    };
    let e = env(false);
    let (ino, _, hashes) = file(&e);
    assert_eq!(e.cache.is_verified(&hashes[1]), Some(true));
    let bad = rot(&e, &hashes[1]);
    let got = read(&e, ino, u64::from(CHUNK), 4096);
    assert_eq!(
        got.contiguous()[1000],
        bad[1000],
        "under `admit` the trusted disk copy is served as it is"
    );
    assert!(e.cache.contains(&hashes[1]), "the entry was dropped");

    // The same thing under `always`: caught, dropped, and refetched from
    // the store, which is the mode an operator picks when the local disk
    // is not trusted.
    let e = env_verify(false, CacheVerify::Always);
    let (ino, data, hashes) = file(&e);
    rot(&e, &hashes[1]);
    let got = read(&e, ino, u64::from(CHUNK), 4096);
    assert_eq!(
        &*got.contiguous(),
        &data[CHUNK as usize..CHUNK as usize + 4096]
    );
    assert_eq!(
        std::fs::read(chunk_file(&e, &hashes[1])).unwrap()[1000],
        data[CHUNK as usize + 1000],
        "the refetched file"
    );
}

fn evicting_a_chunk_drops_its_memory_copy(e2e: bool) {
    let e = env(e2e);
    let (ino, data, hashes) = file(&e);
    let _ = read(&e, ino, 0, data.len() as u64);
    assert!(hashes.iter().all(|h| e.cache.memory_contains(h)));
    // The conformance kit's `evict` hook: the next read must be cold.
    assert_eq!(e.fs.evict_cached(ino).unwrap(), 3);
    assert!(hashes.iter().all(|h| !e.cache.memory_contains(h)));
    assert_eq!(e.cache.memory_stats().unwrap().used_bytes, 0);
    let back = read(&e, ino, 0, data.len() as u64);
    assert_eq!(&*back.contiguous(), &data[..]);
    // A prune the same.
    let _ = read(&e, ino, 0, data.len() as u64);
    assert!(hashes.iter().any(|h| e.cache.memory_contains(h)));
    e.cache.prune_to(0).unwrap();
    assert!(hashes.iter().all(|h| !e.cache.memory_contains(h)));
}

#[test]
fn evicting_a_chunk_drops_its_memory_copy_plain() {
    evicting_a_chunk_drops_its_memory_copy(false);
}

#[test]
fn evicting_a_chunk_drops_its_memory_copy_e2e() {
    evicting_a_chunk_drops_its_memory_copy(true);
}

/// Shared chunks are never edited in place: a truncation's dead bytes are
/// zeros in the read, and the cached chunk still holds them for a reader
/// of the old content (the same chunk in another file).
#[test]
fn a_clipped_read_does_not_touch_the_shared_chunk() {
    let e = env(false);
    let (ino, data, hashes) = file(&e);
    let _ = read(&e, ino, 0, data.len() as u64); // resident
    let twin = e.meta.create(ROOT_INO, "twin", 0o644, 0, 0).unwrap();
    e.fs.do_write(twin.ino, 0, &data).unwrap();
    e.fs.flush_inode(twin.ino, false).unwrap();
    // Cut `f` inside chunk 0, then grow it back: the cut bytes read as 0.
    e.fs.truncate(ino, 1000).unwrap();
    e.meta
        .setattr(ino, None, None, None, Some(1000), None, None)
        .unwrap();
    e.fs.truncate(ino, 5000).unwrap();
    e.meta
        .setattr(ino, None, None, None, Some(5000), None, None)
        .unwrap();
    let got = read(&e, ino, 0, 5000).contiguous().into_owned();
    assert_eq!(&got[..1000], &data[..1000]);
    assert!(got[1000..].iter().all(|b| *b == 0));
    e.fs.flush_inode(ino, false).unwrap();
    let got = read(&e, ino, 0, 5000).contiguous().into_owned();
    assert_eq!(&got[..1000], &data[..1000]);
    assert!(got[1000..].iter().all(|b| *b == 0));
    // The twin (same chunk 0) still reads its full content, from memory.
    assert!(e.cache.memory_contains(&hashes[0]));
    let twin_back = read(&e, twin.ino, 0, 5000);
    assert_eq!(&*twin_back.contiguous(), &data[..5000]);
}
