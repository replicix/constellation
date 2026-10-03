//! Plan 38 §3(d), Z4b: which opens are marked zero-copy, which reads are
//! answered with a range of a chunk file (`ReadData::zero_copy`) and which
//! take the ordinary path, and what a zero-copy read holds while it is in
//! flight. Driven through the `Vfs` trait as a frontend drives it — no
//! kernel: the `READ_FIXED` is the FUSE adapter's, and everything asserted
//! here holds without it.

use super::*;
use constellation_fs_core::cache::CacheVerify;
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{
    Blocking, Fh, OpCtx, OpKind, OpenOwner, Opened, ReadData, Vfs, VfsResult, WriteData,
};
use tempfile::TempDir;

/// 1 MiB, the smallest valid chunk size.
const CHUNK: u32 = 1 << 20;

struct Env {
    fs: View,
    cache: Arc<DiskCache>,
    meta: Arc<Meta>,
    caller: Caller,
    _dir: TempDir,
}

/// A view whose frontend negotiated zero-copy reads (as Linux FUSE says so
/// once its session settled on zero-copy queues), its cache in `verify`
/// mode with a memory tier of `memory` bytes.
fn env_with(verify: CacheVerify, memory: u64) -> Env {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut fs, dir, cache) = super::quota_tests::test_fs_with(meta.clone(), CHUNK, |root| {
        DiskCache::open(root, 1 << 30)
            .unwrap()
            .with_memory_cache(memory)
            .with_verify(verify)
    });
    fs.caps.zero_copy = true;
    let caps = fs.caps.clone();
    fs.frontend_negotiated(&caps);
    Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    }
}

fn env() -> Env {
    env_with(CacheVerify::Admit, 0)
}

/// [`env`], with the frontend's zero-copy threshold at `min_read` bytes
/// (the tests above run with `0`: every read).
fn env_min_read(min_read: u32) -> Env {
    let e = env();
    let caps = FrontendCaps {
        zero_copy_min_read: min_read,
        ..e.fs.caps.clone()
    };
    e.fs.frontend_negotiated(&caps);
    e
}

impl Env {
    fn cx(&self, kind: OpKind) -> OpCtx<'_> {
        OpCtx::new(kind, &self.caller)
    }

    fn open(&self, ino: Ino, flags: OpenFlags) -> Opened {
        Blocking::run(|r| {
            self.fs
                .open(&self.cx(OpKind::Open), ino, flags, OpenOwner::NONE, r)
        })
        .expect("open")
    }

    fn release(&self, ino: Ino, fh: Fh, flags: OpenFlags) {
        Blocking::run(|r| {
            self.fs
                .release(&self.cx(OpKind::Release), ino, fh, flags, None, r)
        })
        .expect("release");
    }

    fn read(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> ReadData {
        Blocking::run(|r| self.fs.read(&self.cx(OpKind::Read), ino, fh, off, len, r)).expect("read")
    }

    fn write(&self, ino: Ino, fh: Fh, off: u64, data: &[u8]) -> VfsResult<u32> {
        Blocking::run(|r| {
            self.fs.write(
                &self.cx(OpKind::Write),
                ino,
                fh,
                off,
                WriteData::Borrowed(data),
                OpenFlags::WRITE,
                r,
            )
        })
    }

    fn chunks(&self, ino: Ino) -> Vec<ChunkHash> {
        let manifest = self.fs.load_manifest(ino).unwrap();
        self.fs
            .chunk_list(&manifest)
            .unwrap()
            .into_values()
            .collect()
    }
}

/// Non-repeating bytes, so a wrong offset or a wrong chunk cannot pass.
fn bytes(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[3] ^ salt)
        .collect()
}

/// A committed file of `len` bytes whose chunks are clean and verified in
/// the disk cache, with no write session.
fn file(e: &Env, name: &str, len: usize, salt: u8) -> (Ino, Vec<u8>) {
    let data = bytes(len, salt);
    let f = e.meta.create(ROOT_INO, name, 0o644, 0, 0).unwrap();
    e.fs.do_write(f.ino, 0, &data).unwrap();
    e.fs.flush_inode(f.ino, false).unwrap();
    for h in e.chunks(f.ino) {
        e.cache.set_state(&h, ChunkState::Clean);
    }
    (f.ino, data)
}

/// The bytes a read answered, read from the file when it is zero-copy.
fn materialized(data: &ReadData) -> Vec<u8> {
    data.contiguous().unwrap().into_owned()
}

/// A read inside one chunk of a multi-chunk file is a range of that chunk's
/// file, at the offset inside the chunk, and its bytes are the file's —
/// first chunk, middle, and the short last chunk up to EOF.
#[test]
fn a_read_within_one_chunk_is_a_range_of_its_chunk_file() {
    let e = env();
    let len = 2 * CHUNK as usize + 300_000;
    let (ino, data) = file(&e, "big", len, 1);
    let o = e.open(ino, OpenFlags::READ);
    assert!(
        o.zero_copy,
        "a read-only open of a multi-chunk file is marked"
    );
    assert!(o.backing.is_none(), "passthrough was not negotiated");
    for (off, size) in [
        (0u64, 4096u32),
        (CHUNK as u64 + 12_345, 128 << 10),
        (2 * CHUNK as u64 + 4096, 1 << 20),
    ] {
        let got = e.read(ino, o.fh, off, size);
        let zc = got
            .as_zero_copy()
            .unwrap_or_else(|| panic!("a read at {off} was not zero-copy"));
        assert_eq!(zc.offset, off % CHUNK as u64, "offset inside the chunk");
        let end = (off as usize + size as usize).min(len);
        assert_eq!(zc.len, end - off as usize, "clipped at EOF");
        assert_eq!(materialized(&got), data[off as usize..end]);
    }
    e.release(ino, o.fh, OpenFlags::READ);
    assert_eq!(e.fs.zero_copy_handles(), 0, "released with the handle");
}

/// The chunk-spanning fallback (plan 38 §3(d)): a read crossing a chunk
/// boundary is never zero-copy (one `READ_FIXED` per request, no
/// whole-file object), and its bytes are the file's.
#[test]
fn a_read_crossing_a_chunk_boundary_takes_the_ordinary_path() {
    let e = env();
    let (ino, data) = file(&e, "span", 2 * CHUNK as usize, 2);
    let o = e.open(ino, OpenFlags::READ);
    assert!(o.zero_copy);
    let off = CHUNK as u64 - 4096;
    let got = e.read(ino, o.fh, off, 8192);
    assert!(
        got.as_zero_copy().is_none(),
        "a spanning read went zero-copy"
    );
    assert_eq!(materialized(&got), data[off as usize..off as usize + 8192]);
    // The same handle's next read inside one chunk is zero-copy again.
    let got = e.read(ino, o.fh, CHUNK as u64, 8192);
    assert!(got.as_zero_copy().is_some());
    e.release(ino, o.fh, OpenFlags::READ);
}

/// An open that can write, or of an inode another handle can write, is not
/// marked; and a write session on the inode (a local writer after the
/// open) sends a marked handle's reads down the ordinary path, which sees
/// the written bytes.
#[test]
fn writers_and_write_sessions_keep_reads_off_zero_copy() {
    let e = env();
    let (ino, mut data) = file(&e, "written", CHUNK as usize, 3);
    let reader = e.open(ino, OpenFlags::READ);
    assert!(reader.zero_copy);
    let writer = e.open(ino, OpenFlags::READ | OpenFlags::WRITE);
    assert!(!writer.zero_copy, "a read-write open is never marked");
    assert!(
        !e.open(ino, OpenFlags::READ).zero_copy,
        "no open is marked while a handle can write the inode"
    );
    assert_eq!(e.write(ino, writer.fh, 100, b"local").unwrap(), 5);
    data[100..105].copy_from_slice(b"local");
    let got = e.read(ino, reader.fh, 0, 4096);
    assert!(got.as_zero_copy().is_none(), "the overlay was bypassed");
    assert_eq!(materialized(&got), data[..4096]);
}

/// `--cache-verify always`: no open is marked, whatever the frontend
/// negotiated, and every read is served (and hashed) by the daemon.
#[test]
fn cache_verify_always_marks_nothing_and_serves_every_read() {
    let e = env_with(CacheVerify::Always, 0);
    let (ino, data) = file(&e, "always", CHUNK as usize, 4);
    let o = e.open(ino, OpenFlags::READ);
    assert!(!o.zero_copy);
    let got = e.read(ino, o.fh, 0, 4096);
    assert!(got.as_zero_copy().is_none());
    assert_eq!(materialized(&got), data[..4096]);
}

/// A frontend that did not negotiate zero-copy (every frontend but Linux
/// FUSE on a zero-copy session) is never offered it.
#[test]
fn a_frontend_without_zero_copy_is_never_offered_it() {
    let e = env();
    let caps = FrontendCaps {
        zero_copy: false,
        ..e.fs.caps.clone()
    };
    e.fs.frontend_negotiated(&caps);
    let (ino, _) = file(&e, "plain", 4096, 5);
    let o = e.open(ino, OpenFlags::READ);
    assert!(!o.zero_copy);
    assert!(e.read(ino, o.fh, 0, 4096).as_zero_copy().is_none());
}

/// The chunk a zero-copy read names cannot be evicted while the read is in
/// flight — not even after its handle was released — and is evictable
/// once the read is done (`zero_copy`'s module doc).
#[test]
fn an_in_flight_zero_copy_read_pins_its_chunk_past_the_release() {
    let e = env();
    let (ino, data) = file(&e, "pinned", 4096, 6);
    let hash = e.chunks(ino)[0];
    let o = e.open(ino, OpenFlags::READ);
    let in_flight = e.read(ino, o.fh, 0, 4096);
    assert!(in_flight.as_zero_copy().is_some());
    assert_eq!(e.cache.open_pin_count(&hash), 1, "one pin per cached chunk");
    // A second read of the same chunk reuses the handle's file and pin.
    let again = e.read(ino, o.fh, 0, 4096);
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    drop(again);
    e.release(ino, o.fh, OpenFlags::READ);
    assert_eq!(
        e.cache.prune_to(0).unwrap().freed_chunks,
        0,
        "evicted in flight"
    );
    assert!(e.cache.contains(&hash));
    assert_eq!(materialized(&in_flight), data);
    drop(in_flight);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.cache.prune_to(0).unwrap().freed_chunks, 1);
}

/// A handle keeps its last [`ZERO_COPY_SOURCES`] chunks open and pinned
/// (random reads over a few chunks reopen none), drops the least recently
/// read one beyond that, and the frontend's teardown drops every handle's.
#[test]
fn a_handle_holds_its_last_few_chunks_and_teardown_drops_them_all() {
    use super::zero_copy::ZERO_COPY_SOURCES;
    let e = env();
    let n = ZERO_COPY_SOURCES + 1;
    let (ino, _) = file(&e, "walk", n * CHUNK as usize, 7);
    let hashes = e.chunks(ino);
    let o = e.open(ino, OpenFlags::READ);
    let at = |i: usize| i as u64 * CHUNK as u64;
    let first = e.read(ino, o.fh, 0, 4096);
    let source = |d: &ReadData| Arc::as_ptr(&d.as_zero_copy().unwrap().source);
    for (i, hash) in hashes.iter().enumerate().take(ZERO_COPY_SOURCES) {
        drop(e.read(ino, o.fh, at(i), 4096));
        assert_eq!(e.cache.open_pin_count(hash), 1, "chunk {i}");
    }
    // Back to chunk 0: the same open file, no second pin.
    let again = e.read(ino, o.fh, 0, 4096);
    assert_eq!(source(&again), source(&first), "chunk 0 was reopened");
    drop((first, again));
    assert_eq!(e.cache.open_pin_count(&hashes[0]), 1);
    // One chunk more: the least recently read (chunk 1) goes.
    drop(e.read(ino, o.fh, at(ZERO_COPY_SOURCES), 4096));
    assert_eq!(e.cache.open_pin_count(&hashes[1]), 0, "the oldest stayed");
    assert_eq!(e.cache.open_pin_count(&hashes[0]), 1);
    assert_eq!(e.cache.open_pin_count(&hashes[ZERO_COPY_SOURCES]), 1);
    e.fs.drop_all_zero_copy();
    assert!(hashes.iter().all(|h| e.cache.open_pin_count(h) == 0));
    assert!(e.read(ino, o.fh, 0, 4096).as_zero_copy().is_none());
}

/// A chunk the memory tier holds does not stop a read from going
/// zero-copy: on a marked handle bytes are bounced into the kernel's
/// registered pages one copy more than a `READ_FIXED` costs (`zero_copy`'s
/// module doc). A read below the threshold is still answered from memory.
#[test]
fn a_chunk_held_in_memory_is_still_read_zero_copy() {
    let e = env_with(CacheVerify::Admit, 256 << 10);
    let (ino, data) = file(&e, "hot", 4096, 8);
    let hash = e.chunks(ino)[0];
    let o = e.open(ino, OpenFlags::READ);
    assert!(o.zero_copy);
    // The ordinary path admits it to memory.
    let _ = e.fs.do_read(ino, 0, 4096).unwrap();
    assert!(e.cache.in_memory(&hash));
    let hits = e.cache.memory_stats().unwrap().hits;
    let got = e.read(ino, o.fh, 0, 4096);
    assert!(
        got.as_zero_copy().is_some(),
        "a large read of memory went bytes"
    );
    assert_eq!(e.cache.memory_stats().unwrap().hits, hits);
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    assert_eq!(materialized(&got), data[..4096]);
    drop(got);

    // Below the threshold: a memory hit, nothing pinned.
    let caps = FrontendCaps {
        zero_copy_min_read: 4096,
        ..e.fs.caps.clone()
    };
    e.fs.frontend_negotiated(&caps);
    let small = e.read(ino, o.fh, 0, 4095);
    assert!(
        small.as_zero_copy().is_none(),
        "a small read went zero-copy"
    );
    assert_eq!(e.cache.memory_stats().unwrap().hits, hits + 1);
    assert_eq!(materialized(&small), data[..4095]);
}

/// Below the frontend's threshold a read is answered with bytes, at and
/// above it zero-copy — on the same handle.
#[test]
fn a_read_below_the_threshold_is_served_with_bytes() {
    let e = env_min_read(64 << 10);
    let (ino, data) = file(&e, "sized", CHUNK as usize, 10);
    let o = e.open(ino, OpenFlags::READ);
    assert!(o.zero_copy);
    let small = e.read(ino, o.fh, 0, (64 << 10) - 1);
    assert!(
        small.as_zero_copy().is_none(),
        "a small read went zero-copy"
    );
    assert_eq!(materialized(&small), data[..(64 << 10) - 1]);
    let large = e.read(ino, o.fh, 4096, 64 << 10);
    assert!(large.as_zero_copy().is_some());
    assert_eq!(materialized(&large), data[4096..4096 + (64 << 10)]);
    // A read clipped at EOF below the threshold is bytes too.
    let tail = e.read(ino, o.fh, CHUNK as u64 - 100, 64 << 10);
    assert!(tail.as_zero_copy().is_none());
    assert_eq!(materialized(&tail), data[CHUNK as usize - 100..]);
}

/// A file smaller than the threshold has no read that could qualify, so
/// its open is not marked (the kernel would hand every read over as
/// registered pages, and each byte answer is copied into them once more).
#[test]
fn a_file_smaller_than_the_threshold_is_not_marked() {
    let e = env_min_read(64 << 10);
    let (small, data) = file(&e, "small", (64 << 10) - 1, 11);
    let o = e.open(small, OpenFlags::READ);
    assert!(!o.zero_copy);
    assert_eq!(materialized(&e.read(small, o.fh, 0, 1 << 20)), data);
    let (exact, _) = file(&e, "exact", 64 << 10, 12);
    assert!(e.open(exact, OpenFlags::READ).zero_copy);
}

/// A chunk the cache dropped as corrupt (`get_verified`) and fetched again
/// is a new file at the same path; a handle that had the old one open
/// serves the new one, never the unlinked corrupt copy.
#[test]
fn a_chunk_removed_as_corrupt_is_never_served_from_the_old_file() {
    use std::os::unix::fs::MetadataExt;
    let e = env();
    let (ino, data) = file(&e, "rot", 4096, 13);
    let hash = e.chunks(ino)[0];
    let o = e.open(ino, OpenFlags::READ);
    let before = e.read(ino, o.fh, 0, 4096);
    let old_ino = before
        .as_zero_copy()
        .unwrap()
        .source
        .file()
        .metadata()
        .unwrap()
        .ino();
    // The copy on disk rots in place (same length), and a verified read
    // finds it: dropped from the cache, file unlinked.
    let path = e.cache.chunk_path(&hash);
    let mut rotten = std::fs::read(&path).unwrap();
    rotten[0] ^= 0xff;
    std::fs::write(&path, &rotten).unwrap();
    assert_eq!(e.cache.get_verified(&hash).unwrap(), None);
    assert!(!e.cache.contains(&hash));
    // Fetched again (as the ordinary path's fetch would): a new file at
    // the same path, verified by the process that wrote it.
    e.cache.insert(&hash, &data, ChunkState::Clean).unwrap();
    assert_eq!(e.cache.resident(&hash).map(|r| r.verified), Some(true));
    // Zero-copy again — from the new file.
    let after = e.read(ino, o.fh, 0, 4096);
    let zc = after.as_zero_copy().expect("the new copy is zero-copy");
    let new_ino = zc.source.file().metadata().unwrap().ino();
    assert_ne!(new_ino, old_ino, "the unlinked corrupt file was served");
    assert_eq!(new_ino, std::fs::metadata(&path).unwrap().ino());
    assert_eq!(materialized(&after), data);
    drop(before);
}

/// A file grown by `truncate` reads its new bytes as zeros whichever path
/// answers: before the growth is published only the ordinary path can (a
/// write session holds it), after it the chunk file holds them.
#[test]
fn a_file_grown_by_truncate_reads_zeros_on_either_path() {
    let e = env();
    let (ino, data) = file(&e, "grown", 4096, 9);
    Blocking::run(|r| {
        e.fs.setattr(
            &e.cx(OpKind::Setattr),
            ino,
            None,
            &constellation_vfs::SetAttr {
                size: Some(8192),
                ..Default::default()
            },
            r,
        )
    })
    .expect("truncate up");
    let mut want = data;
    want.resize(8192, 0);
    let o = e.open(ino, OpenFlags::READ);
    assert!(!o.zero_copy, "a write session holds the growth");
    assert_eq!(materialized(&e.read(ino, o.fh, 0, 8192)), want);
    e.fs.flush_inode(ino, false).unwrap();
    let o = e.open(ino, OpenFlags::READ);
    assert!(o.zero_copy);
    assert_eq!(materialized(&e.read(ino, o.fh, 0, 8192)), want);
}
