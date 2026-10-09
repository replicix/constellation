//! Pending-upload claims under duplicate content (the data-loss path the
//! storm-hang fix found while reading the write path): rewriting one of
//! two identical sealed chunks must not cancel the claim the other one
//! still needs.

use super::*;
use constellation_fs_core::types::ROOT_INO;
use object_store::memory::InMemory;
use tempfile::TempDir;

const CHUNK: u32 = 1024 * 1024;

struct Env {
    fs: View,
    meta: Arc<Meta>,
    cache: Arc<DiskCache>,
    store: Arc<ChunkStore>,
    rt: tokio::runtime::Runtime,
    _dir: TempDir,
}

fn env() -> Env {
    env_with(CHUNK)
}

fn env_with(chunk: u32) -> Env {
    env_with_budget(chunk, 1 << 30)
}

fn env_with_budget(chunk: u32, staging_budget: u64) -> Env {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let dir = TempDir::new().unwrap();
    // With the memory tier on: every truncate/clip shape below then also
    // checks that reads never edit (or serve stale) shared chunk bytes.
    let cache = Arc::new(
        DiskCache::open(dir.path().join("cache"), 1 << 30)
            .unwrap()
            .with_memory_cache(64 << 20),
    );
    let store = Arc::new(ChunkStore::new(Arc::new(InMemory::new())));
    let snapshots = Arc::new(crate::snapshot::SnapshotManager::new(
        meta.clone(),
        store.clone(),
        chunk,
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
            host: constellation_platform::HostServices::native(),
        },
        chunk,
        CompressionSetting::RAW,
    );
    std::mem::forget(fs_rt);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    Env {
        fs,
        meta,
        cache,
        store,
        rt,
        _dir: dir,
    }
}

fn chunk(fill: u8) -> Vec<u8> {
    vec![fill; CHUNK as usize]
}

// ---- truncate never resurrects the bytes it cut ----
//
// Every shape: a truncate (a session's, or a committed `setattr`
// the base manifest predates), then an extension past the cut — by
// a write past a gap, a truncate-up, a `fallocate` — within one write
// session or across a flush; the cut bytes read as zeros, before the
// flush (the session's view) and after it (the committed manifest).

const SMALL: u32 = 16;

fn read_all(e: &Env, ino: Ino) -> Vec<u8> {
    e.fs.do_read(ino, 0, 1 << 20)
        .unwrap()
        .contiguous()
        .unwrap()
        .into_owned()
}

/// The whole file, read a chunk at a time.
fn read_whole(e: &Env, ino: Ino) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let part =
            e.fs.do_read(ino, out.len() as u64, u64::from(CHUNK))
                .unwrap();
        let part = part.contiguous().unwrap().into_owned();
        if part.is_empty() {
            return out;
        }
        out.extend(part);
    }
}

/// The FUSE `setattr(size)` path: the session's truncate plus the
/// committed size.
fn setattr_size(e: &Env, ino: Ino, size: u64) {
    e.fs.truncate(ino, size).unwrap();
    e.meta
        .setattr(ino, None, None, None, Some(size), None, None)
        .unwrap();
}

fn file_with(e: &Env, name: &str, bytes: &[u8]) -> Ino {
    let f = e.meta.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
    e.fs.do_write(f.ino, 0, bytes).unwrap();
    e.fs.flush_inode(f.ino, false).unwrap();
    f.ino
}

fn check(e: &Env, ino: Ino, want: &[u8], what: &str) {
    same(
        &read_whole(e, ino),
        want,
        &format!("{what}: before the flush"),
    );
    e.fs.flush_inode(ino, false).unwrap();
    same(
        &read_whole(e, ino),
        want,
        &format!("{what}: after the flush"),
    );
}

/// `got == want`, reported as the first differing offset (the buffers
/// are megabytes).
fn same(got: &[u8], want: &[u8], what: &str) {
    if got != want {
        let at = got.iter().zip(want).position(|(g, w)| g != w);
        panic!(
            "{what}: {} bytes read, {} expected, first difference at {at:?}: {:?} vs {:?}",
            got.len(),
            want.len(),
            at.map(|i| &got[i..(i + 16).min(got.len())]),
            at.map(|i| &want[i..(i + 16).min(want.len())]),
        );
    }
}

#[test]
fn truncate_then_write_past_a_gap_reads_zeros_in_the_gap() {
    for chunk in [SMALL, CHUNK] {
        let e = env_with(chunk);
        // Committed base, then the truncate and the write in a new
        // session.
        let a = file_with(&e, "a", b"abcdefgh");
        setattr_size(&e, a, 2);
        e.fs.do_write(a, 5, b"X").unwrap();
        check(&e, a, b"ab\0\0\0X", "flushed base");
        // All in one session.
        let f = e.meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
        e.fs.do_write(f.ino, 0, b"abcdefgh").unwrap();
        setattr_size(&e, f.ino, 2);
        e.fs.do_write(f.ino, 5, b"X").unwrap();
        check(&e, f.ino, b"ab\0\0\0X", "one session");
    }
}

#[test]
fn truncate_then_extend_by_truncate_or_fallocate_reads_zeros() {
    let e = env_with(SMALL);
    let a = file_with(&e, "up", b"abcdefghijklmnopqrstuvwxyz");
    setattr_size(&e, a, 3);
    setattr_size(&e, a, 30);
    let mut want = b"abc".to_vec();
    want.resize(30, 0);
    check(&e, a, &want, "truncate-up");

    let f = file_with(&e, "falloc", b"abcdefghijklmnopqrstuvwxyz");
    setattr_size(&e, f, 3);
    e.fs.do_fallocate(f, 0, 30, FallocateMode::empty()).unwrap();
    check(&e, f, &want, "fallocate extend");

    let z = file_with(&e, "zero", b"abcdefghijklmnopqrstuvwxyz");
    setattr_size(&e, z, 3);
    e.fs.do_fallocate(z, 10, 20, FallocateMode::ZERO_RANGE)
        .unwrap();
    check(&e, z, &want, "fallocate zero-range extend");
}

/// `O_TRUNC` is `setattr(size = 0)` from the kernel: a partial write
/// after it must not bring back the old first chunk around it.
#[test]
fn o_trunc_then_a_partial_write_keeps_only_the_write() {
    let e = env_with(SMALL);
    let a = file_with(&e, "t", b"0123456789abcdefghijklmnopqrstuvwxyz");
    setattr_size(&e, a, 0);
    e.fs.do_write(a, 20, b"Z").unwrap();
    let mut want = vec![0u8; 20];
    want.push(b'Z');
    check(&e, a, &want, "O_TRUNC");
}

/// Whole chunks sealed into the cache by a sequential writer, cut in
/// the middle of one, then extended: the sealed tail and the chunks
/// past it are gone.
#[test]
fn truncate_inside_a_sealed_chunk_then_extend_reads_zeros() {
    let e = env_with(SMALL);
    let f = e.meta.create(ROOT_INO, "sealed", 0o644, 0, 0).unwrap();
    let data: Vec<u8> = (0..64u8).map(|i| b'a' + i % 26).collect();
    e.fs.do_write(f.ino, 0, &data).unwrap();
    setattr_size(&e, f.ino, 20);
    e.fs.do_write(f.ino, 60, b"!").unwrap();
    let mut want = data[..20].to_vec();
    want.resize(60, 0);
    want.push(b'!');
    check(&e, f.ino, &want, "sealed");
}

/// Another node's committed truncate (`setattr(size)` only: its
/// manifest commit has not landed): this node's write past the gap
/// must not read the old manifest's bytes back.
#[test]
fn a_committed_truncate_the_manifest_predates_is_honoured() {
    let e = env_with(SMALL);
    let a = file_with(&e, "remote", b"abcdefghijklmnopqrstuvwxyz");
    e.meta
        .setattr(a, None, None, None, Some(4), None, None)
        .unwrap();
    assert_eq!(read_all(&e, a), b"abcd");
    e.fs.do_write(a, 10, b"X").unwrap();
    let mut want = b"abcd".to_vec();
    want.resize(10, 0);
    want.push(b'X');
    check(&e, a, &want, "committed truncate");
    e.meta
        .setattr(a, None, None, None, Some(2), None, None)
        .unwrap();
    e.meta
        .setattr(a, None, None, None, Some(12), None, None)
        .unwrap();
    let mut want = b"ab".to_vec();
    want.resize(12, 0);
    assert_eq!(
        read_all(&e, a),
        want,
        "committed truncate then committed extend"
    );
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

/// Random write / truncate / fallocate (extend, zero range, punch) /
/// flush sequences against an in-memory model of the file; every
/// read of the whole file must equal the model, before and after
/// each flush. 16-byte chunks, files up to ~200 bytes: every shape
/// crosses chunk boundaries, and past 8 chunks the manifest spills.
/// `TRUNCATE_FUZZ_SEEDS=5000` for a longer run (5000 passed, 134 s).
#[test]
fn random_write_truncate_fallocate_sequences_match_a_model() {
    let seeds: u64 = std::env::var("TRUNCATE_FUZZ_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(150);
    for seed in 1..=seeds {
        let e = env_with(SMALL);
        let f = e.meta.create(ROOT_INO, "fuzz", 0o644, 0, 0).unwrap();
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let mut model: Vec<u8> = Vec::new();
        let mut log = Vec::new();
        for step in 0..40 {
            let max = 160u64;
            match rng.below(10) {
                0..=3 => {
                    let off = rng.below(max);
                    let len = 1 + rng.below(40);
                    let byte = b'a' + (rng.below(26) as u8);
                    let data = vec![byte; len as usize];
                    log.push(format!("write {off}+{len}"));
                    e.fs.do_write(f.ino, off, &data).unwrap();
                    let end = (off + len) as usize;
                    if model.len() < end {
                        model.resize(end, 0);
                    }
                    model[off as usize..end].copy_from_slice(&data);
                }
                4 | 5 => {
                    let size = rng.below(max);
                    log.push(format!("truncate {size}"));
                    setattr_size(&e, f.ino, size);
                    model.resize(size as usize, 0);
                }
                6 => {
                    let off = rng.below(max);
                    let len = 1 + rng.below(40);
                    log.push(format!("fallocate {off}+{len}"));
                    e.fs.do_fallocate(f.ino, off, len, FallocateMode::empty())
                        .unwrap();
                    let end = (off + len) as usize;
                    if model.len() < end {
                        model.resize(end, 0);
                    }
                }
                7 => {
                    let off = rng.below(max);
                    let len = 1 + rng.below(40);
                    let keep = rng.below(2) == 0;
                    let mode = FallocateMode::ZERO_RANGE
                        | if keep {
                            FallocateMode::KEEP_SIZE
                        } else {
                            FallocateMode::empty()
                        };
                    log.push(format!("zero-range {off}+{len} keep {keep}"));
                    e.fs.do_fallocate(f.ino, off, len, mode).unwrap();
                    let end = (off + len) as usize;
                    if !keep && model.len() < end {
                        model.resize(end, 0);
                    }
                    let stop = end.min(model.len());
                    if (off as usize) < stop {
                        model[off as usize..stop].fill(0);
                    }
                }
                8 => {
                    let off = rng.below(max);
                    let len = 1 + rng.below(40);
                    log.push(format!("punch {off}+{len}"));
                    e.fs.do_fallocate(
                        f.ino,
                        off,
                        len,
                        FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
                    )
                    .unwrap();
                    let stop = ((off + len) as usize).min(model.len());
                    if (off as usize) < stop {
                        model[off as usize..stop].fill(0);
                    }
                }
                _ => {
                    log.push("flush".into());
                    e.fs.flush_inode(f.ino, false).unwrap();
                }
            }
            assert_eq!(
                read_all(&e, f.ino),
                model,
                "seed {seed} step {step}: {log:?}"
            );
        }
        e.fs.flush_inode(f.ino, false).unwrap();
        assert_eq!(
            read_all(&e, f.ino),
            model,
            "seed {seed} after the last flush: {log:?}"
        );
    }
}

impl Env {
    fn upload_round(&self) {
        self.rt
            .block_on(crate::upload::upload_dirty_chunks(
                &self.cache,
                &self.meta,
                &self.store,
                CompressionSetting::RAW,
                &crate::upload::UploadRuntime::for_test(true),
                None,
                None,
            ))
            .expect("every pending chunk uploads");
    }

    fn in_s3(&self, hash: &ChunkHash) -> bool {
        self.rt.block_on(self.store.chunk_durable(hash)).unwrap()
    }

    /// The invariant that matters: every chunk a committed manifest
    /// names is in S3 already or still has a pending row that will
    /// put it there (and is in the cache to be uploaded from).
    fn assert_manifest_backed(&self, ino: Ino) {
        let manifest = self.fs.load_manifest(ino).unwrap();
        for (idx, hash) in self.fs.chunk_list(&manifest).unwrap() {
            let pending = self.meta.pending_upload_claims(&hash, ino).unwrap() > 0;
            assert!(
                self.in_s3(&hash) || (pending && self.cache.contains(&hash)),
                "ino {ino} chunk {idx} ({hash}) is in neither S3 nor the upload queue"
            );
        }
    }

    fn assert_manifest_in_s3(&self, ino: Ino) {
        let manifest = self.fs.load_manifest(ino).unwrap();
        for (idx, hash) in self.fs.chunk_list(&manifest).unwrap() {
            assert!(
                self.in_s3(&hash),
                "ino {ino} chunk {idx} ({hash}) never reached S3"
            );
        }
    }
}

/// One file, identical content at chunk positions 0 and 2 (both sealed
/// once the writer crosses them), then position 0 rewritten: the chunk
/// position 2 still names must stay enrolled and upload.
#[test]
fn rewriting_one_of_two_identical_sealed_chunks_keeps_the_other_enrolled() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "dup", 0o644, 0, 0).unwrap();
    let same = chunk(b'A');
    let same_hash = ChunkHash::of(&same);
    e.fs.do_write(file.ino, 0, &same).unwrap();
    e.fs.do_write(file.ino, u64::from(CHUNK), &chunk(b'B'))
        .unwrap();
    e.fs.do_write(file.ino, 2 * u64::from(CHUNK), &same)
        .unwrap();
    assert_eq!(
        e.meta.pending_upload_claims(&same_hash, file.ino).unwrap(),
        2,
        "both sealed positions enrolled a claim"
    );

    // Rewrite position 0 with different bytes: both claims stay (a
    // superseded copy uploads and becomes evictable, see `View::unseal`),
    // and the one position 2 needs is among them.
    e.fs.do_write(file.ino, 0, &chunk(b'C')).unwrap();
    assert_eq!(
        e.meta.pending_upload_claims(&same_hash, file.ino).unwrap(),
        2
    );

    e.fs.flush_inode(file.ino, false).unwrap();
    let manifest = e.fs.load_manifest(file.ino).unwrap();
    let chunks = e.fs.chunk_list(&manifest).unwrap();
    assert_eq!(chunks.get(&2), Some(&same_hash));
    e.assert_manifest_backed(file.ino);

    e.upload_round();
    assert!(e.meta.pending_uploads().unwrap().is_empty());
    e.assert_manifest_in_s3(file.ino);
    assert_eq!(e.rt.block_on(e.store.get_chunk(&same_hash)).unwrap(), same);
}

/// The same content already claimed by an earlier, not yet uploaded
/// manifest of the inode: a new write session seals it again and then
/// overwrites it, which must not cancel the earlier manifest's claim.
#[test]
fn unsealing_does_not_cancel_an_earlier_manifests_claim() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "again", 0o644, 0, 0).unwrap();
    let same = chunk(b'D');
    let same_hash = ChunkHash::of(&same);
    e.fs.do_write(file.ino, 0, &same).unwrap();
    e.fs.do_write(file.ino, u64::from(CHUNK), &same).unwrap();
    e.fs.flush_inode(file.ino, false).unwrap();
    e.assert_manifest_backed(file.ino);

    // Second session: seal the same bytes at position 0, then
    // overwrite them before the flush.
    e.fs.do_write(file.ino, 0, &same).unwrap();
    e.fs.do_write(file.ino, u64::from(CHUNK), &same).unwrap();
    e.fs.do_write(file.ino, 0, &chunk(b'E')).unwrap();
    e.fs.flush_inode(file.ino, false).unwrap();
    assert!(
        e.meta.pending_upload_claims(&same_hash, file.ino).unwrap() >= 1,
        "position 1 still names the content the first manifest enrolled"
    );
    e.assert_manifest_backed(file.ino);
    e.upload_round();
    e.assert_manifest_in_s3(file.ino);
}

/// Two files share a chunk: rewriting it in one, or deleting that
/// file outright, leaves the other's claim alone.
#[test]
fn another_inodes_rewrite_or_delete_leaves_the_shared_chunk_enrolled() {
    let e = env();
    let a = e.meta.create(ROOT_INO, "a", 0o644, 0, 0).unwrap();
    let b = e.meta.create(ROOT_INO, "b", 0o644, 0, 0).unwrap();
    let c = e.meta.create(ROOT_INO, "c", 0o644, 0, 0).unwrap();
    let shared = chunk(b'S');
    let shared_hash = ChunkHash::of(&shared);
    for ino in [a.ino, b.ino, c.ino] {
        // A full chunk plus one byte seals position 0.
        e.fs.do_write(ino, 0, &shared).unwrap();
        e.fs.do_write(ino, u64::from(CHUNK), b"x").unwrap();
    }
    for ino in [a.ino, b.ino, c.ino] {
        assert_eq!(e.meta.pending_upload_claims(&shared_hash, ino).unwrap(), 1);
    }

    // a rewrites the shared chunk; c is committed and then deleted.
    e.fs.do_write(a.ino, 0, &chunk(b'T')).unwrap();
    for ino in [a.ino, b.ino, c.ino] {
        e.fs.flush_inode(ino, false).unwrap();
    }
    e.meta.unlink(ROOT_INO, "c").unwrap();
    assert!(e.meta.pending_upload_claims(&shared_hash, b.ino).unwrap() >= 1);
    e.assert_manifest_backed(a.ino);
    e.assert_manifest_backed(b.ino);

    e.upload_round();
    e.assert_manifest_in_s3(a.ino);
    e.assert_manifest_in_s3(b.ino);
}

// ---- when a write session seals (eager upload) ----
//
// A sequential run seals the chunks it moves past; a random writer's
// chunks wait for the flush unless staging fills; a chunk written again
// after its seal is not sealed again by a run.

/// Chunk versions in the cache: one per seal (and per flushed chunk).
fn cached(e: &Env) -> usize {
    e.cache.entries().len()
}

fn claims(e: &Env, ino: Ino) -> usize {
    e.meta
        .pending_uploads()
        .unwrap()
        .iter()
        .filter(|(_, i)| *i == ino)
        .count()
}

/// A committed, uploaded file of `chunks` chunks, chunk `i` filled with
/// `b'a' + i`.
fn uploaded_file(e: &Env, name: &str, chunks: u8) -> (Ino, Vec<u8>) {
    let bytes: Vec<u8> = (0..chunks).flat_map(|i| chunk(b'a' + i)).collect();
    // A chunk per write, as FUSE delivers them (one write larger than
    // the staging budget cannot be staged).
    let ino = e.meta.create(ROOT_INO, name, 0o644, 0, 0).unwrap().ino;
    for (i, part) in bytes.chunks(CHUNK as usize).enumerate() {
        e.fs.do_write(ino, (i * CHUNK as usize) as u64, part)
            .unwrap();
    }
    e.fs.flush_inode(ino, false).unwrap();
    e.upload_round();
    assert_eq!(claims(e, ino), 0);
    (ino, bytes)
}

#[test]
fn random_overwrites_of_an_existing_file_seal_nothing_until_the_flush() {
    let e = env();
    let (ino, mut want) = uploaded_file(&e, "random", 4);
    let before = cached(&e);
    for (i, idx) in [2u64, 0, 3, 1, 2, 0].into_iter().enumerate() {
        let off = idx * u64::from(CHUNK) + 4096 * (i as u64 + 1);
        let data = vec![b'0' + i as u8; 4096];
        e.fs.do_write(ino, off, &data).unwrap();
        want[off as usize..off as usize + 4096].copy_from_slice(&data);
    }
    assert_eq!(claims(&e, ino), 0, "no chunk sealed before the flush");
    assert_eq!(
        cached(&e),
        before,
        "no chunk version cached before the flush"
    );
    check(&e, ino, &want, "random overwrites");
    assert_eq!(
        claims(&e, ino),
        4,
        "the flush enrolls each touched chunk once"
    );
    e.assert_manifest_backed(ino);
}

#[test]
fn a_sequential_overwrite_seals_behind_the_writer() {
    let e = env();
    let (ino, _) = uploaded_file(&e, "seq", 4);
    let want: Vec<u8> = (0..4).flat_map(|i| chunk(b'k' + i)).collect();
    for piece in want.chunks(64 << 10).enumerate() {
        e.fs.do_write(ino, (piece.0 * (64 << 10)) as u64, piece.1)
            .unwrap();
        let crossed = (piece.0 + 1) * (64 << 10) / CHUNK as usize;
        assert_eq!(
            claims(&e, ino),
            crossed,
            "sealed as the run crosses each chunk"
        );
    }
    check(&e, ino, &want, "sequential overwrite");
}

#[test]
fn an_append_at_an_unaligned_length_seals_the_straddling_chunk_once_crossed() {
    let e = env();
    let half = CHUNK as usize / 2;
    let mut want = chunk(b'a');
    want.extend(vec![b'b'; half]);
    let ino = file_with(&e, "append", &want);
    e.upload_round();
    let quarter = vec![b'c'; half / 2];
    e.fs.do_write(ino, want.len() as u64, &quarter).unwrap();
    want.extend(&quarter);
    assert_eq!(claims(&e, ino), 0, "chunk 1 is not crossed yet");
    e.fs.do_write(ino, want.len() as u64, &vec![b'd'; half])
        .unwrap();
    want.extend(vec![b'd'; half]);
    assert_eq!(claims(&e, ino), 1, "the run crossed chunk 1");
    check(&e, ino, &want, "append");
}

#[test]
fn a_header_patched_on_every_append_is_sealed_once() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "header", 0o644, 0, 0).unwrap();
    let mut want = Vec::new();
    let record = vec![b'r'; 64 << 10];
    for i in 0..48u32 {
        e.fs.do_write(file.ino, want.len() as u64, &record).unwrap();
        want.extend(&record);
        let count = (i + 1).to_le_bytes();
        e.fs.do_write(file.ino, 0, &count).unwrap();
        want[..4].copy_from_slice(&count);
    }
    // 3 MiB: chunks 0, 1 and 2 crossed, each sealed once: a patch inside
    // the run crosses nothing.
    assert!(cached(&e) <= 3, "{} chunk versions cached", cached(&e));
    check(&e, file.ino, &want, "header");
}

#[test]
fn two_interleaved_sequential_fronts_both_seal() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "fronts", 0o644, 0, 0).unwrap();
    let mut want = vec![0u8; 4 * CHUNK as usize];
    let piece = 64 << 10;
    for i in 0..(2 * CHUNK as usize / piece) {
        for (front, fill) in [(0usize, b'A'), (2 * CHUNK as usize, b'a')] {
            let off = front + i * piece;
            // A different byte per chunk: identical chunks share a claim.
            let data = vec![fill + (off / CHUNK as usize) as u8; piece];
            e.fs.do_write(file.ino, off as u64, &data).unwrap();
            want[off..off + piece].copy_from_slice(&data);
        }
    }
    assert_eq!(
        claims(&e, file.ino),
        4,
        "each front sealed the two chunks it crossed"
    );
    check(&e, file.ino, &want, "two fronts");
}

#[test]
fn staging_pressure_seals_a_random_writers_chunks_instead_of_refusing() {
    // Two chunks of budget: half of it is reached by one dirty chunk.
    let e = env_with_budget(CHUNK, 2 * u64::from(CHUNK));
    let (a, mut want_a) = uploaded_file(&e, "pa", 6);
    let (b, mut want_b) = uploaded_file(&e, "pb", 6);
    for round in 0..3u64 {
        for idx in [4u64, 1, 5, 0, 3, 2] {
            for (ino, want) in [(a, &mut want_a), (b, &mut want_b)] {
                let off = idx * u64::from(CHUNK) + 8192 * (round + 1);
                let data = vec![b'0' + idx as u8; 4096];
                e.fs.do_write(ino, off, &data)
                    .expect("no ENOSPC under pressure");
                want[off as usize..off as usize + 4096].copy_from_slice(&data);
            }
        }
    }
    check(&e, a, &want_a, "pressure a");
    check(&e, b, &want_b, "pressure b");
    e.assert_manifest_backed(a);
    e.assert_manifest_backed(b);
}

#[test]
fn truncate_below_a_run_on_an_existing_file_then_extend_reads_zeros() {
    let e = env();
    let (ino, bytes) = uploaded_file(&e, "trunc", 3);
    // A run starting at the end of the existing file, then cut below it.
    e.fs.do_write(ino, bytes.len() as u64, &chunk(b'z'))
        .unwrap();
    let cut = u64::from(CHUNK) + 100;
    setattr_size(&e, ino, cut);
    let mut want = bytes[..cut as usize].to_vec();
    let tail = vec![b'y'; 1000];
    e.fs.do_write(ino, cut + 5000, &tail).unwrap();
    want.resize(cut as usize + 5000, 0);
    want.extend(&tail);
    check(&e, ino, &want, "truncate below a run");
}

/// A chunk sealed and then written again (its pending-upload claim
/// withdrawn) must not stay Dirty in the cache once nothing will upload
/// it: Dirty entries are never evicted, so each one leaked cache space
/// for good (the database lane's `dirty_bytes` stuck at the 10 GiB cache
/// budget with no pending upload).
#[test]
fn a_superseded_sealed_chunk_does_not_stay_dirty() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "superseded", 0o644, 0, 0).unwrap();
    e.fs.do_write(file.ino, 0, &chunk(b'A')).unwrap();
    e.fs.do_write(file.ino, u64::from(CHUNK), b"x").unwrap(); // seals chunk 0
    e.fs.do_write(file.ino, 10, b"rewritten").unwrap(); // re-dirties it
    e.fs.flush_inode(file.ino, false).unwrap();
    e.upload_round();
    assert!(e.meta.pending_uploads().unwrap().is_empty());
    assert_eq!(
        e.cache.dirty_bytes(),
        0,
        "a Dirty entry nothing will upload"
    );
}

/// A sealed chunk, once uploaded, is a clean cache entry a full cache may
/// evict: a write into it and a read of it fetch it back (they failed
/// with EIO), and the failed write no longer lost the chunk's earlier
/// writes (it unsealed the chunk before reading the copy, so reads then
/// fell through to the base: zeros).
#[test]
fn an_evicted_sealed_copy_is_fetched_back() {
    let e = env();
    let file = e.meta.create(ROOT_INO, "evicted", 0o644, 0, 0).unwrap();
    let mut want = chunk(b'A');
    e.fs.do_write(file.ino, 0, &want).unwrap();
    e.fs.do_write(file.ino, u64::from(CHUNK), b"x").unwrap(); // seals chunk 0
    want.push(b'x');
    e.upload_round();
    let sealed = ChunkHash::of(&chunk(b'A'));
    e.cache.remove(&sealed).unwrap();
    assert_eq!(
        &read_whole(&e, file.ino)[..CHUNK as usize],
        &want[..CHUNK as usize],
        "read of the evicted copy"
    );
    e.fs.do_write(file.ino, 100, b"patched").unwrap();
    want[100..107].copy_from_slice(b"patched");
    check(&e, file.ino, &want, "patched after eviction");
}
