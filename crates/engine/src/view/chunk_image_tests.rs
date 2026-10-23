//! A dirty chunk's content (`View::chunk_image`, finding 09): a partial
//! write stages only its own bytes, and a read, a seal and the flush all
//! lay the session's written ranges over the base — the same base for a
//! read and the commit that would follow it. And the flush composes its
//! dirty chunks in parallel, with the sequential compose's result.

use super::*;
use constellation_fs_core::types::ROOT_INO;
use object_store::memory::InMemory;
use std::os::unix::fs::MetadataExt;
use tempfile::TempDir;

const MIB: u32 = 1024 * 1024;

struct Shared {
    meta: Arc<Meta>,
    cache: Arc<DiskCache>,
    store: Arc<ChunkStore>,
    dir: TempDir,
}

fn shared() -> Shared {
    let dir = TempDir::new().unwrap();
    Shared {
        meta: Arc::new(Meta::open_in_memory().unwrap()),
        // With the memory tier on, as in production: a read's shared
        // slices of the base must never be edited.
        cache: Arc::new(
            DiskCache::open(dir.path().join("cache"), 1 << 30)
                .unwrap()
                .with_memory_cache(64 << 20),
        ),
        store: Arc::new(ChunkStore::new(Arc::new(InMemory::new()))),
        dir,
    }
}

/// A view over `s` (several views share one replica, cache and store:
/// one view's commit moves the manifest under another's session, as a
/// commit replayed from another node does). Staging under `staging`.
fn view(s: &Shared, staging: &str, chunk: u32, staging_budget: u64) -> View {
    view_on(
        s,
        staging,
        chunk,
        staging_budget,
        constellation_platform::HostServices::native().fs,
    )
}

/// [`view`], its staging on the host file primitives `fs`.
fn view_on(
    s: &Shared,
    staging: &str,
    chunk: u32,
    staging_budget: u64,
    fs: Arc<dyn constellation_platform::FsPrimitives>,
) -> View {
    // Every slot staging hands out on this thread reads as junk until
    // written: a read of anything staging was not given since shows.
    crate::staging::poison_slots(true);
    let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
        s.meta.clone(),
        s.store.clone(),
        chunk,
        1,
    ));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fs = View::new(
        FsDependencies {
            listing: crate::view::Listing::for_test(),
            meta: s.meta.clone(),
            store: s.store.clone(),
            cache: s.cache.clone(),
            rt: rt.handle().clone(),
            sync: None,
            coop: None,
            staging_dir: s.dir.path().join(staging),
            staging_budget: StagingBudget::new(staging_budget),
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
            host: constellation_platform::HostServices {
                fs,
                ..constellation_platform::HostServices::native()
            },
        },
        chunk,
        CompressionSetting::RAW,
    );
    std::mem::forget(rt);
    fs
}

fn read(fs: &View, ino: Ino, offset: u64, len: u64) -> Vec<u8> {
    fs.do_read(ino, offset, len)
        .unwrap()
        .contiguous()
        .unwrap()
        .into_owned()
}

fn read_all(fs: &View, ino: Ino) -> Vec<u8> {
    read(fs, ino, 0, 1 << 30)
}

/// Upload every pending chunk and drop every clean one from the cache
/// (memory tier included): the next read of any committed chunk fetches
/// it from the store.
fn evict_clean(s: &Shared) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(crate::upload::upload_dirty_chunks(
        &s.cache,
        &s.meta,
        &s.store,
        CompressionSetting::RAW,
        &crate::upload::UploadRuntime::for_test(true),
        None,
        None,
    ))
    .expect("every pending chunk uploads");
    s.cache.prune_to(0).unwrap();
}

/// Bytes `[0, len)` of a pattern no two chunks share (of any size up to
/// 4 GiB: no period).
fn pattern(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 56) as u8 ^ salt)
        .collect()
}

fn overwrite(model: &mut Vec<u8>, offset: u64, data: &[u8]) {
    let end = offset as usize + data.len();
    if model.len() < end {
        model.resize(end, 0);
    }
    model[offset as usize..end].copy_from_slice(data);
}

/// What the staging files under `dir` occupy on disk.
fn staged_bytes(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .map(|e| e.unwrap().metadata().unwrap().blocks() * 512)
                .sum()
        })
        .unwrap_or(0)
}

/// Finding 09: an 8 KiB write into each of three committed 1 MiB chunks
/// staged the three whole chunks (3 MiB written to the staging file, the
/// whole committed chunk copied in as a seed for reads); it stages the
/// 24 KiB written. Reads and the flush see the same bytes as before.
#[test]
fn a_partial_write_stages_only_its_own_bytes() {
    let s = shared();
    let fs = view(&s, "staging", MIB, 1 << 30);
    // Measures what staging allocates: no junk filling its slots.
    crate::staging::poison_slots(false);
    let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let mut model = pattern(4 * MIB as usize, 0);
    fs.do_write(f.ino, 0, &model).unwrap();
    fs.flush_inode(f.ino, false).unwrap();
    assert_eq!(staged_bytes(&s.dir.path().join("staging")), 0);

    for i in 0..3u64 {
        let at = i * u64::from(MIB) + 100_000;
        let data = pattern(8192, 0x40 + i as u8);
        fs.do_write(f.ino, at, &data).unwrap();
        overwrite(&mut model, at, &data);
    }
    let staged = staged_bytes(&s.dir.path().join("staging"));
    assert!(
        staged <= 3 * 64 * 1024,
        "{staged} bytes staged for 24 KiB written into three chunks"
    );
    assert_eq!(read_all(&fs, f.ino), model, "the session's reads");
    // Reads that straddle written ranges and the base, and chunk ends.
    for (at, len) in [
        (99_000, 10_000),
        (1_048_000, 200_000),
        (2 * 1_048_576 + 108_100, 50),
    ] {
        assert_eq!(
            read(&fs, f.ino, at, len),
            model[at as usize..(at + len) as usize],
            "read {at}+{len}"
        );
    }
    fs.flush_inode(f.ino, false).unwrap();
    assert_eq!(read_all(&fs, f.ino), model, "after the flush");
}

/// The manifest moves under an open session (another node's commit,
/// replayed here; a second view on the same replica stands in for it).
/// The session's flush lays its writes over the manifest that is current
/// (it rebases), so a read of a dirty chunk before the flush shows the
/// same: the session's bytes over the current base. It used to show the
/// base as it was at the session's first write into the chunk, and the
/// other writer's bytes in that chunk appeared only after the flush.
#[test]
fn a_dirty_chunk_reads_over_the_base_its_flush_commits_on() {
    const CHUNK: u32 = 64;
    for shape in ["disjoint", "overlap", "truncate"] {
        let s = shared();
        let a = view(&s, "staging-a", CHUNK, 1 << 30);
        let b = view(&s, "staging-b", CHUNK, 1 << 30);
        let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        let mut model = pattern(3 * CHUNK as usize, 1);
        a.do_write(f.ino, 0, &model).unwrap();
        a.flush_inode(f.ino, false).unwrap();

        // A's session: partial writes into chunks 0 and 1.
        let mine = [(10u64, b"aaaa".as_slice()), (70, b"AAAAAAAA".as_slice())];
        for (at, data) in mine {
            a.do_write(f.ino, at, data).unwrap();
        }
        // Another writer commits while A's session is open.
        match shape {
            "disjoint" => {
                b.do_write(f.ino, 30, b"bbbb").unwrap();
                overwrite(&mut model, 30, b"bbbb");
            }
            "overlap" => {
                b.do_write(f.ino, 8, b"bbbbbbbb").unwrap();
                overwrite(&mut model, 8, b"bbbbbbbb");
            }
            "truncate" => {
                // Cut inside chunk 1, through A's write there: A's bytes
                // survive, the base's past the cut read as zeros, and the
                // file keeps the length A's session has (the rebase keeps
                // the longer one; only a truncate of A's own shortens).
                b.truncate(f.ino, 74).unwrap();
                s.meta
                    .setattr(f.ino, None, None, None, Some(74), None, None)
                    .unwrap();
                model.truncate(74);
                model.resize(3 * CHUNK as usize, 0);
            }
            _ => unreachable!(),
        }
        b.flush_inode(f.ino, false).unwrap();
        // A's writes land last (A commits after B).
        for (at, data) in mine {
            overwrite(&mut model, at, data);
        }
        let before = read_all(&a, f.ino);
        a.flush_inode(f.ino, false).unwrap();
        let after = read_all(&a, f.ino);
        assert_eq!(after, model, "{shape}: what the flush published");
        assert_eq!(
            before, after,
            "{shape}: the session read what its flush published"
        );
    }
}

/// A tiny xorshift: the sequences are reproducible from the seed.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

/// Random writes, truncates, `fallocate` (extend, zero range, punch),
/// flushes and partial reads against a model of the file, on a committed
/// base (so partial writes land on base content), in 16-byte chunks;
/// with a large staging budget and with a small one, under which writes
/// seal dirty chunks and later writes, truncates and punches re-stage
/// sealed ones. Every read — a random range, and the whole file — must
/// equal the model, before and after each flush.
/// `CHUNK_IMAGE_FUZZ_SEEDS` for a longer run.
#[test]
fn random_sessions_over_a_base_read_back_their_model() {
    const CHUNK: u32 = 16;
    let seeds: u64 = std::env::var("CHUNK_IMAGE_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    // The eviction arm uploads and prunes the cache (a runtime and an
    // upload round per use): in the first seeds only, by default.
    let evict_seeds: u64 = std::env::var("CHUNK_IMAGE_FUZZ_EVICT_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    for budget in [1u64 << 30, 16 * u64::from(CHUNK)] {
        for seed in 1..=seeds {
            let s = shared();
            let fs = view(&s, "staging", CHUNK, budget);
            let f = s.meta.create(ROOT_INO, "fuzz", 0o644, 0, 0).unwrap();
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
            let mut model = pattern(120 + rng.below(60) as usize, seed as u8);
            // The base, committed through a view with room to stage it.
            let seeder = view(&s, "staging-seed", CHUNK, 1 << 30);
            seeder.do_write(f.ino, 0, &model).unwrap();
            seeder.flush_inode(f.ino, false).unwrap();
            let mut log = Vec::new();
            for step in 0..60 {
                let max = 200u64;
                match rng.below(14) {
                    0..=4 => {
                        let off = rng.below(max);
                        let cap = if rng.below(4) == 0 { 40 } else { 6 };
                        let len = 1 + rng.below(cap);
                        let byte = b'a' + (rng.below(26) as u8);
                        let data = vec![byte; len as usize];
                        log.push(format!("write {off}+{len}"));
                        fs.do_write(f.ino, off, &data).unwrap();
                        overwrite(&mut model, off, &data);
                    }
                    5 => {
                        let size = rng.below(max);
                        log.push(format!("truncate {size}"));
                        fs.truncate(f.ino, size).unwrap();
                        s.meta
                            .setattr(f.ino, None, None, None, Some(size), None, None)
                            .unwrap();
                        model.resize(size as usize, 0);
                    }
                    6 => {
                        let off = rng.below(max);
                        let len = 1 + rng.below(40);
                        log.push(format!("fallocate {off}+{len}"));
                        fs.do_fallocate(f.ino, off, len, FallocateMode::empty())
                            .unwrap();
                        let end = (off + len) as usize;
                        if model.len() < end {
                            model.resize(end, 0);
                        }
                    }
                    7 | 8 => {
                        let off = rng.below(max);
                        let len = 1 + rng.below(40);
                        let zero = rng.below(2) == 0;
                        let keep = !zero || rng.below(2) == 0;
                        let mode = if zero {
                            FallocateMode::ZERO_RANGE
                        } else {
                            FallocateMode::PUNCH_HOLE
                        } | if keep {
                            FallocateMode::KEEP_SIZE
                        } else {
                            FallocateMode::empty()
                        };
                        log.push(format!("{mode:?} {off}+{len}"));
                        fs.do_fallocate(f.ino, off, len, mode).unwrap();
                        let end = (off + len) as usize;
                        if !keep && model.len() < end {
                            model.resize(end, 0);
                        }
                        let stop = end.min(model.len());
                        if (off as usize) < stop {
                            model[off as usize..stop].fill(0);
                        }
                    }
                    9 => {
                        log.push("flush".into());
                        fs.flush_inode(f.ino, false).unwrap();
                    }
                    10 if seed <= evict_seeds => {
                        // Every chunk then comes from S3 again: a dirty
                        // chunk's base fetched (and staged) by a read,
                        // a sealed copy, the flush's.
                        log.push("evict".into());
                        evict_clean(&s);
                    }
                    _ => {
                        let off = rng.below(max);
                        let len = 1 + rng.below(50);
                        let got = read(&fs, f.ino, off, len);
                        let from = (off as usize).min(model.len());
                        let to = ((off + len) as usize).min(model.len());
                        assert_eq!(
                            got,
                            model[from..to],
                            "budget {budget} seed {seed} step {step} read {off}+{len}: {log:?}"
                        );
                        continue;
                    }
                }
                assert_eq!(
                    read_all(&fs, f.ino),
                    model,
                    "budget {budget} seed {seed} step {step}: {log:?}"
                );
            }
            fs.flush_inode(f.ino, false).unwrap();
            assert_eq!(
                read_all(&fs, f.ino),
                model,
                "budget {budget} seed {seed} after the last flush: {log:?}"
            );
        }
    }
}

/// A read of a dirty chunk's gap stages the base chunk it fetched
/// (`WriteState::staged_base`): later reads, and the flush, take it from
/// staging. With a cache too small to keep it, each such read used to
/// fetch the whole chunk again (a GET of 4 MiB per 8 KiB read, on a
/// cache full of chunks waiting to upload); here the base is dropped from
/// the cache and is in no store (nothing uploads), so a second fetch
/// would fail.
#[test]
fn a_dirty_chunk_s_base_is_fetched_once_per_session() {
    let s = shared();
    let fs = view(&s, "staging", MIB, 1 << 30);
    let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let mut model = pattern(4 * MIB as usize, 5);
    fs.do_write(f.ino, 0, &model).unwrap();
    fs.flush_inode(f.ino, false).unwrap();
    let at = u64::from(MIB) + 300_000;
    fs.do_write(f.ino, at, b"written").unwrap();
    overwrite(&mut model, at, b"written");
    let gap = |fs: &View| read(fs, f.ino, u64::from(MIB) + 1000, 600_000);
    let want = model[MIB as usize + 1000..MIB as usize + 601_000].to_vec();
    assert_eq!(gap(&fs), want, "the first read fetches the base");
    let manifest = fs.load_manifest(f.ino).unwrap();
    let base = fs.chunk_list(&manifest).unwrap()[&1];
    s.cache.remove(&base).unwrap();
    assert_eq!(gap(&fs), want, "a later read takes it from staging");
    fs.flush_inode(f.ino, false).unwrap();
    assert_eq!(
        read_all(&fs, f.ino),
        model,
        "the flush composes it from staging"
    );
}

/// Another node extends the file while this session's last chunk is
/// dirty: the chunk's base (re-cut by that commit) is valid past the
/// session's length, where staging holds nothing. A read — which ends at
/// the session's length — takes the staged base all the same; it used
/// to fetch and stage the chunk again on every read, as if the staged
/// copy fell short. (The base is dropped from the cache and is in no
/// store here, so a second fetch fails.) The flush, which composes the
/// chunk at the extended length, fetches it.
#[test]
fn a_base_extended_elsewhere_stays_staged_for_reads() {
    const CHUNK: u32 = 16;
    let s = shared();
    let a = view(&s, "staging-a", CHUNK, 1 << 30);
    let b = view(&s, "staging-b", CHUNK, 1 << 30);
    let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let mut model = pattern(40, 3);
    a.do_write(f.ino, 0, &model).unwrap();
    a.flush_inode(f.ino, false).unwrap();
    // A's session: chunk 2 (32..40, the file's end) dirty.
    a.do_write(f.ino, 34, b"aa").unwrap();
    // B extends the file: chunk 2 is re-cut to its full length.
    let tail = pattern(10, 9);
    b.do_write(f.ino, 60, &tail).unwrap();
    b.flush_inode(f.ino, false).unwrap();
    let manifest = a.load_manifest(f.ino).unwrap();
    let base = a.chunk_list(&manifest).unwrap()[&2];
    let mut want = model[32..40].to_vec();
    want[2..4].copy_from_slice(b"aa");
    assert_eq!(read(&a, f.ino, 32, 8), want, "fetched and staged");
    let bytes = s.cache.get_shared(&base).unwrap().expect("cached");
    s.cache.remove(&base).unwrap();
    assert_eq!(read(&a, f.ino, 32, 8), want, "from staging");
    assert_eq!(read(&a, f.ino, 33, 3), want[1..4], "from staging");
    s.cache.insert(&base, &bytes, ChunkState::Dirty).unwrap();
    a.flush_inode(f.ino, false).unwrap();
    overwrite(&mut model, 34, b"aa");
    model.resize(60, 0);
    model.extend_from_slice(&tail);
    assert_eq!(
        read_all(&a, f.ino),
        model,
        "the flush at the extended length"
    );
}

/// A truncate that fails (here: staging the sealed chunk its point falls
/// in) changes no byte: the base past the point still reads, and
/// flushes, as it was. The truncation floor used to be lowered before
/// the failing step, and the base's bytes past it read as zeros from a
/// file the truncate had not shortened.
#[test]
fn a_failed_truncate_changes_no_byte() {
    const CHUNK: u32 = 16;
    let s = shared();
    let fs = view(&s, "staging", CHUNK, 1 << 30);
    let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let mut model = pattern(32, 4);
    fs.do_write(f.ino, 0, &model).unwrap();
    fs.flush_inode(f.ino, false).unwrap();
    // Chunk 0 rewritten whole: sealed by the run crossing it.
    let sealed = pattern(CHUNK as usize, 8);
    fs.do_write(f.ino, 0, &sealed).unwrap();
    overwrite(&mut model, 0, &sealed);
    assert!(fs
        .writes
        .lock(f.ino)
        .get(&f.ino)
        .unwrap()
        .sealed
        .contains_key(&0));
    crate::staging::FAIL_WRITES.with(|w| w.set((0, 1)));
    let failed = fs.truncate(f.ino, 10);
    crate::staging::FAIL_WRITES.with(|w| w.set((0, 0)));
    assert_eq!(failed, Err(Code::Io));
    assert_eq!(read_all(&fs, f.ino), model, "the session");
    fs.flush_inode(f.ino, false).unwrap();
    assert_eq!(read_all(&fs, f.ino), model, "after the flush");
}

/// Random sessions of view A over a base another view B keeps committing
/// into between A's steps (writes, truncates down and up): A writes inside
/// chunks (never to a chunk's end, so nothing is sealed — asserted — and
/// the model is exact), punches whole chunks and flushes. A's session
/// reads A's written byte, else zero in a chunk A punched whole, else the
/// byte committed now, else zero, at A's own length; a flush publishes
/// that image at the longer of A's length and the base's, and B reads
/// what A does at the end. (Written for the review of finding 09.)
#[test]
fn sessions_over_a_moving_base_read_back_their_model() {
    const CHUNK: u32 = 16;
    let cs = CHUNK as usize;
    let seeds: u64 = std::env::var("CHUNK_IMAGE_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let expect =
        |a_written: &[Option<u8>], a_zeroed: &[bool], base: &[u8], len: usize| -> Vec<u8> {
            (0..len)
                .map(|i| {
                    if let Some(Some(byte)) = a_written.get(i) {
                        return *byte;
                    }
                    if a_zeroed.get(i / cs).copied().unwrap_or(false) {
                        return 0;
                    }
                    base.get(i).copied().unwrap_or(0)
                })
                .collect()
        };
    for seed in 1..=seeds {
        let s = shared();
        let a = view(&s, "staging-a", CHUNK, 1 << 30);
        let b = view(&s, "staging-b", CHUNK, 1 << 30);
        let f = s.meta.create(ROOT_INO, "fuzz", 0o644, 0, 0).unwrap();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut base = pattern(100 + rng.below(80) as usize, seed as u8);
        b.do_write(f.ino, 0, &base).unwrap();
        b.flush_inode(f.ino, false).unwrap();
        let mut a_written: Vec<Option<u8>> = Vec::new();
        let mut a_zeroed: Vec<bool> = Vec::new();
        let mut a_len: Option<usize> = None;
        let mut log = Vec::new();
        let max = 240u64;
        for step in 0..80 {
            match rng.below(13) {
                0..=4 => {
                    let mut off = rng.below(max);
                    if off % u64::from(CHUNK) == u64::from(CHUNK) - 1 {
                        off -= 1;
                    }
                    let room = u64::from(CHUNK) - off % u64::from(CHUNK) - 1;
                    let cap = if rng.below(4) == 0 { 14 } else { 5 };
                    let len = 1 + rng.below(room.min(cap));
                    let byte = b'a' + rng.below(26) as u8;
                    log.push(format!("A write {off}+{len}"));
                    a.do_write(f.ino, off, &vec![byte; len as usize]).unwrap();
                    let cur = a_len.get_or_insert(base.len());
                    *cur = (*cur).max((off + len) as usize);
                    if a_written.len() < *cur {
                        a_written.resize(*cur, None);
                    }
                    a_written[off as usize..(off + len) as usize].fill(Some(byte));
                }
                5 => {
                    // Chunk-aligned: a punch's boundary zeroing is a write,
                    // and one ending exactly at a chunk's end seals it.
                    let off = rng.below(max / u64::from(CHUNK)) * u64::from(CHUNK);
                    let len = (1 + rng.below(3)) * u64::from(CHUNK);
                    log.push(format!("A punch {off}+{len}"));
                    a.do_fallocate(
                        f.ino,
                        off,
                        len,
                        FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
                    )
                    .unwrap();
                    let cur = *a_len.get_or_insert(base.len());
                    let eff_end = ((off + len) as usize).min(cur);
                    let off = off as usize;
                    if off < eff_end {
                        if a_written.len() < cur {
                            a_written.resize(cur, None);
                        }
                        let full_start = off.div_ceil(cs);
                        let full_end = eff_end / cs;
                        if a_zeroed.len() < full_end {
                            a_zeroed.resize(full_end, false);
                        }
                        for (i, slot) in a_written.iter_mut().enumerate().take(eff_end).skip(off) {
                            let idx = i / cs;
                            if full_start < full_end && idx >= full_start && idx < full_end {
                                a_zeroed[idx] = true;
                                *slot = None;
                            } else {
                                *slot = Some(0);
                            }
                        }
                    }
                }
                6 | 7 => {
                    let off = rng.below(max);
                    let len = 1 + rng.below(24);
                    let byte = b'A' + rng.below(26) as u8;
                    log.push(format!("B write {off}+{len}"));
                    let data = vec![byte; len as usize];
                    b.do_write(f.ino, off, &data).unwrap();
                    b.flush_inode(f.ino, false).unwrap();
                    overwrite(&mut base, off, &data);
                }
                8 => {
                    let size = rng.below(max);
                    log.push(format!("B truncate {size}"));
                    b.truncate(f.ino, size).unwrap();
                    s.meta
                        .setattr(f.ino, None, None, None, Some(size), None, None)
                        .unwrap();
                    b.flush_inode(f.ino, false).unwrap();
                    base.resize(size as usize, 0);
                }
                9 => {
                    if let Some(len) = a_len.take() {
                        log.push("A flush".into());
                        a.flush_inode(f.ino, false).unwrap();
                        let new_len = len.max(base.len());
                        base = expect(&a_written, &a_zeroed, &base, new_len);
                        a_written.clear();
                        a_zeroed.clear();
                    }
                }
                _ => {
                    let len = a_len.unwrap_or(base.len());
                    let off = rng.below(max);
                    let n = 1 + rng.below(50);
                    let got = read(&a, f.ino, off, n);
                    let model = expect(&a_written, &a_zeroed, &base, len);
                    let from = (off as usize).min(len);
                    let to = ((off + n) as usize).min(len);
                    assert_eq!(
                        got,
                        model[from..to],
                        "seed {seed} step {step} A read {off}+{n}: {log:?}"
                    );
                    continue;
                }
            }
            {
                let shard = a.writes.lock(f.ino);
                assert!(
                    shard.get(&f.ino).is_none_or(|w| w.sealed.is_empty()),
                    "seed {seed} step {step}: the model assumes no seals: {log:?}"
                );
            }
            let len = a_len.unwrap_or(base.len());
            assert_eq!(
                read_all(&a, f.ino),
                expect(&a_written, &a_zeroed, &base, len),
                "seed {seed} step {step}: {log:?}"
            );
        }
        if let Some(len) = a_len.take() {
            a.flush_inode(f.ino, false).unwrap();
            base = expect(&a_written, &a_zeroed, &base, len.max(base.len()));
        }
        assert_eq!(read_all(&a, f.ino), base, "seed {seed} final, A: {log:?}");
        assert_eq!(read_all(&b, f.ino), base, "seed {seed} final, B: {log:?}");
    }
}

/// The session a parallel-compose test flushes: a committed 12-chunk
/// file (spilled manifest), then 8 KiB into each of ten of its chunks,
/// and two chunks rewritten whole.
fn ten_dirty_chunks(s: &Shared, fs: &View) -> (Ino, Vec<u8>) {
    let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
    let mut model = pattern(12 * MIB as usize, 3);
    fs.do_write(f.ino, 0, &model).unwrap();
    fs.flush_inode(f.ino, false).unwrap();
    for i in 0..10u64 {
        let at = i * u64::from(MIB) + 4096 * (i + 1);
        let data = pattern(8192, 0x80 + i as u8);
        fs.do_write(f.ino, at, &data).unwrap();
        overwrite(&mut model, at, &data);
    }
    for i in [10u64, 11] {
        let data = pattern(MIB as usize, 0x20 + i as u8);
        fs.do_write(f.ino, i * u64::from(MIB), &data).unwrap();
        overwrite(&mut model, i * u64::from(MIB), &data);
    }
    (f.ino, model)
}

/// The flush composes its dirty chunks in parallel (finding 09), and
/// publishes exactly what the sequential compose did: the same manifest
/// bytes, the same pending uploads, the same content.
#[test]
fn a_parallel_compose_publishes_what_a_sequential_one_does() {
    let mut published = Vec::new();
    for concurrency in [1usize, 4] {
        let s = shared();
        let fs = view(&s, "staging", MIB, 1 << 30);
        let (ino, model) = ten_dirty_chunks(&s, &fs);
        let pending_before = s.meta.pending_uploads_for_ino(ino).unwrap();
        super::flush::COMPOSE_CONCURRENCY.with(|c| c.set(Some(concurrency)));
        super::flush::COMPOSE_HOLD.with(|h| h.set(Duration::from_millis(20)));
        let flushed = fs.flush_inode(ino, false);
        super::flush::COMPOSE_CONCURRENCY.with(|c| c.set(None));
        super::flush::COMPOSE_HOLD.with(|h| h.set(Duration::ZERO));
        flushed.unwrap();
        let peak = super::flush::COMPOSE_PEAK.with(|p| p.get());
        if concurrency == 1 {
            assert_eq!(peak, 1, "one chunk at a time");
        } else {
            assert!(peak > 1, "chunks composed at once: {peak}");
        }
        assert_eq!(read_all(&fs, ino), model, "concurrency {concurrency}");
        let mut pending = s.meta.pending_uploads_for_ino(ino).unwrap();
        pending.retain(|h| !pending_before.contains(h));
        pending.sort_unstable_by_key(|h| h.0);
        published.push((s.meta.manifest(ino).unwrap().unwrap(), pending));
    }
    // Ten chunks and the spill blob; the two rewritten whole were sealed
    // (and enrolled) by their own writes, a sequential run crossing them.
    assert_eq!(published[0].1.len(), 11, "pending uploads the flush added");
    assert_eq!(
        published[0], published[1],
        "manifest bytes and pending uploads"
    );
}

/// A chunk whose base cannot be fetched fails a parallel compose as it
/// failed a sequential one: the flush answers `EIO` and commits nothing —
/// the manifest and the pending uploads are as they were.
#[test]
fn a_failed_chunk_fails_the_parallel_compose_with_nothing_committed() {
    let s = shared();
    let fs = view(&s, "staging", MIB, 1 << 30);
    let (ino, _) = ten_dirty_chunks(&s, &fs);
    // Not in S3 (nothing uploads here) and gone from the cache.
    let manifest = fs.load_manifest(ino).unwrap();
    let chunks = fs.chunk_list(&manifest).unwrap();
    s.cache.remove(&chunks[&3]).unwrap();
    let manifest_before = s.meta.manifest(ino).unwrap();
    let pending_before = s.meta.pending_uploads_for_ino(ino).unwrap();
    super::flush::COMPOSE_CONCURRENCY.with(|c| c.set(Some(4)));
    let flushed = fs.flush_inode(ino, false);
    super::flush::COMPOSE_CONCURRENCY.with(|c| c.set(None));
    assert_eq!(flushed, Err(Code::Io));
    assert_eq!(s.meta.manifest(ino).unwrap(), manifest_before);
    assert_eq!(s.meta.pending_uploads_for_ino(ino).unwrap(), pending_before);
}

/// The compose workers' memory is reserved node-wide: as many as fit,
/// released when the compose ends.
#[test]
fn compose_workers_fit_their_memory_budget() {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    let used = AtomicU64::new(0);
    let chunk = 2 * 4 * u64::from(MIB);
    {
        let a = super::flush::ComposeWorkers::reserve(&used, 64 * u64::from(MIB), 7, chunk);
        assert_eq!(a.granted, 7);
        let b = super::flush::ComposeWorkers::reserve(&used, 64 * u64::from(MIB), 7, chunk);
        assert_eq!(b.granted, 1, "what is left of the budget");
        let c = super::flush::ComposeWorkers::reserve(&used, 64 * u64::from(MIB), 7, chunk);
        assert_eq!(c.granted, 0, "a flush then composes on its own thread only");
        assert_eq!(used.load(Relaxed), 64 * u64::from(MIB));
    }
    assert_eq!(used.load(Relaxed), 0);
    // 64 MiB chunks: none beyond the flush's own thread, as before.
    let big = super::flush::ComposeWorkers::reserve(
        &used,
        64 * u64::from(MIB),
        7,
        2 * 64 * u64::from(MIB),
    );
    assert_eq!(big.granted, 0);
}

/// The host's file primitives, counting hole punches.
#[derive(Default)]
struct CountingFs {
    punches: std::sync::atomic::AtomicU64,
}

impl constellation_platform::FsPrimitives for CountingFs {
    fn punch_hole(&self, file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
        self.punches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        constellation_platform::HostServices::native()
            .fs
            .punch_hole(file, offset, len)
    }
    fn preallocate(
        &self,
        file: &std::fs::File,
        offset: u64,
        len: u64,
        keep_size: bool,
    ) -> std::io::Result<()> {
        constellation_platform::HostServices::native()
            .fs
            .preallocate(file, offset, len, keep_size)
    }
    fn full_fsync(&self, file: &std::fs::File) -> std::io::Result<()> {
        constellation_platform::HostServices::native()
            .fs
            .full_fsync(file)
    }
    fn drop_cache(&self, file: &std::fs::File, offset: u64, len: u64) -> std::io::Result<()> {
        constellation_platform::HostServices::native()
            .fs
            .drop_cache(file, offset, len)
    }
}

/// A sequential write seals each chunk it moves past (eager upload), and
/// that seal released the chunk's staged bytes by punching them out of
/// the staging file — which first writes them back to the disk and
/// waits: ~170 ms per 4 MiB chunk on btrfs with `compress=zstd`, a 23
/// MiB/s ceiling on sequential writes and `copy_file_range`, and 3–8 ms
/// on uncompressed btrfs and xfs. The sealed chunk's staging slot goes
/// to the next chunk instead: nothing is punched, the staging file and
/// the staging budget hold two chunks however long the write, and the
/// file reads back and publishes as written.
#[test]
fn a_sequential_write_punches_nothing_and_stages_two_chunks() {
    const CHUNK: u32 = 64 * 1024;
    let s = shared();
    let counting = Arc::new(CountingFs::default());
    let fs = view_on(&s, "staging", CHUNK, 1 << 30, counting.clone());
    let f = s.meta.create(ROOT_INO, "seq", 0o644, 0, 0).unwrap();
    // 40 chunks in 24 KiB writes: most writes straddle a chunk boundary.
    let model = pattern(40 * CHUNK as usize, 7);
    let staging = s.dir.path().join("staging");
    let mut peak_staged = 0;
    let mut peak_budget = 0;
    for piece in model.chunks(24 * 1024).enumerate() {
        let (i, data) = piece;
        fs.do_write(f.ino, (i * 24 * 1024) as u64, data).unwrap();
        peak_staged = peak_staged.max(staged_bytes(&staging));
        peak_budget = peak_budget.max(fs.staging_budget.used());
    }
    assert_eq!(
        counting.punches.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "hole punches on a sequential writer's path"
    );
    assert!(
        peak_staged <= 2 * u64::from(CHUNK),
        "{peak_staged} bytes staged on disk at the peak"
    );
    assert!(
        peak_budget <= 2 * u64::from(CHUNK),
        "{peak_budget} bytes of staging budget at the peak"
    );
    assert_eq!(read_all(&fs, f.ino), model, "the session's reads");
    fs.flush_inode(f.ino, false).unwrap();
    assert_eq!(fs.staging_budget.used(), 0);
    assert_eq!(staged_bytes(&staging), 0);
    assert_eq!(read_all(&fs, f.ino), model, "after the flush");
}

/// A staging write that fails for real (`EIO` from the staging file, not
/// the budget) while a sealed chunk is staged again — its copy, the write
/// itself, a whole-chunk write, a truncate's copy of the chunk the cut
/// falls in — leaves the chunk sealed: the write fails, and the bytes
/// written before it read back, before and after the flush. The chunk
/// used to be unsealed first, and then read as the base (here: zeros).
#[test]
fn a_failed_restage_of_a_sealed_chunk_keeps_its_bytes() {
    const CHUNK: u32 = 16;
    let fail = |skip: u32| crate::staging::FAIL_WRITES.with(|f| f.set((skip, 1)));
    let clear = || crate::staging::FAIL_WRITES.with(|f| f.set((0, 0)));
    for case in ["copy", "write", "whole chunk", "truncate"] {
        let s = shared();
        let fs = view(&s, "staging", CHUNK, 1 << 30);
        let f = s.meta.create(ROOT_INO, "f", 0o644, 0, 0).unwrap();
        // A whole chunk, sealed by the run that crosses it.
        let sealed = pattern(CHUNK as usize, 9);
        fs.do_write(f.ino, 0, &sealed).unwrap();
        assert!(
            fs.writes
                .lock(f.ino)
                .get(&f.ino)
                .unwrap()
                .sealed
                .contains_key(&0),
            "{case}: chunk 0 sealed"
        );
        let failed = match case {
            "copy" => {
                fail(0);
                fs.do_write(f.ino, 5, b"XY")
            }
            "write" => {
                fail(1);
                fs.do_write(f.ino, 5, b"XY")
            }
            "whole chunk" => {
                fail(0);
                fs.do_write(f.ino, 0, &[b'Z'; CHUNK as usize])
            }
            _ => {
                fail(0);
                fs.truncate(f.ino, 10).map(|()| 0u32)
            }
        };
        clear();
        assert_eq!(failed, Err(Code::Io), "{case}: the write fails");
        let keep = if case == "truncate" {
            10
        } else {
            CHUNK as usize
        };
        assert_eq!(
            read_all(&fs, f.ino)[..keep],
            sealed[..keep],
            "{case}: the session"
        );
        fs.flush_inode(f.ino, false).unwrap();
        assert_eq!(
            read_all(&fs, f.ino)[..keep],
            sealed[..keep],
            "{case}: after the flush"
        );
        // And the chunk takes a write again.
        let mut want = read_all(&fs, f.ino);
        want[5..7].copy_from_slice(b"XY");
        fs.do_write(f.ino, 5, b"XY").unwrap();
        fs.flush_inode(f.ino, false).unwrap();
        assert_eq!(read_all(&fs, f.ino), want, "{case}: a later write");
    }
}
