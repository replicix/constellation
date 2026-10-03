//! Plan 38 §3(c): which opens are answered with the chunk file itself
//! (`Opened::backing`), what the handle holds while it lives, and what it
//! releases. Driven through the `Vfs` trait exactly as a frontend drives
//! it — no kernel, no FUSE: the `FOPEN_PASSTHROUGH` wiring is the FUSE
//! adapter's (plan 38 Z3b, `constellation-frontend-fuse`), and everything
//! asserted here is true without it.

use super::*;
use constellation_fs_core::cache::{CacheVerify, Resident};
use constellation_fs_core::types::ROOT_INO;
use constellation_vfs::{
    Blocking, Fh, LockOwner, Name, OpCtx, OpKind, OpenOwner, Opened, SetAttr, Vfs, VfsResult,
    WriteData,
};
use std::os::unix::fs::FileExt;
use tempfile::TempDir;

/// 1 MiB: the smallest valid chunk size, so a "multi-chunk" file in these
/// tests is still cheap to write.
const CHUNK: u32 = 1 << 20;

struct Env {
    fs: View,
    cache: Arc<DiskCache>,
    meta: Arc<Meta>,
    caller: Caller,
    _dir: TempDir,
}

fn env() -> Env {
    env_with(CacheVerify::Admit, |_| {})
}

/// A view whose disk cache runs in `verify` mode, with `seed` given the
/// cache root before it is opened (so a test can put chunk files there
/// for the startup rescan to find, which is the only way an entry is
/// *unverified*).
fn env_with(verify: CacheVerify, seed: impl FnOnce(&std::path::Path)) -> Env {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (fs, dir, cache) = fs_with(meta.clone(), verify, seed);
    Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    }
}

/// The view behind [`env_with`], before it is wrapped: a frontend that
/// *consumes* `Opened::backing`. Linux FUSE only learns that it can at
/// `FUSE_INIT` and says so through `Vfs::frontend_negotiated`, and the
/// engine offers nothing to a frontend that would drop it, so every test
/// here says so explicitly, the same way.
///
/// No memory tier: a chunk held there is never offered (plan 38 Z3c,
/// [`a_chunk_held_in_memory_is_not_offered`]), and every read in these
/// tests would put it there. [`env_with_memory`] has one.
fn fs_with(
    meta: Arc<Meta>,
    verify: CacheVerify,
    seed: impl FnOnce(&std::path::Path),
) -> (View, TempDir, Arc<DiskCache>) {
    fs_with_memory(meta, verify, 0, seed)
}

fn fs_with_memory(
    meta: Arc<Meta>,
    verify: CacheVerify,
    memory: u64,
    seed: impl FnOnce(&std::path::Path),
) -> (View, TempDir, Arc<DiskCache>) {
    let (mut fs, dir, cache) = super::quota_tests::test_fs_with(meta, CHUNK, |root| {
        seed(root);
        DiskCache::open(root, 1 << 30)
            .unwrap()
            .with_memory_cache(memory)
            .with_verify(verify)
    });
    fs.caps.passthrough = true;
    let caps = fs.caps.clone();
    fs.frontend_negotiated(&caps);
    (fs, dir, cache)
}

/// A view whose memory tier is [`MEMORY`] bytes, so it admits a chunk of
/// at most a quarter of that (`MemCache::admits`): a file of up to
/// [`IN_MEMORY`] bytes goes to memory on its first read, one of
/// [`NOT_IN_MEMORY`] never does.
fn env_with_memory() -> Env {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (fs, dir, cache) = fs_with_memory(meta.clone(), CacheVerify::Admit, MEMORY, |_| {});
    Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    }
}

const MEMORY: u64 = 256 << 10;
const IN_MEMORY: usize = 4096;
const NOT_IN_MEMORY: usize = 100 << 10;

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

    fn open_ro(&self, ino: Ino) -> Opened {
        self.open(ino, OpenFlags::READ)
    }

    fn release(&self, ino: Ino) {
        self.release_with(ino, OpenFlags::READ);
    }

    /// `release` of a handle opened with `flags` (the kernel sends the
    /// handle's own flags, which is how write-intent handles are counted).
    fn release_with(&self, ino: Ino, flags: OpenFlags) {
        Blocking::run(|r| {
            self.fs
                .release(&self.cx(OpKind::Release), ino, Fh(ino), flags, None, r)
        })
        .expect("release");
    }

    fn write(&self, ino: Ino, off: u64, data: &[u8]) -> VfsResult<u32> {
        Blocking::run(|r| {
            self.fs.write(
                &self.cx(OpKind::Write),
                ino,
                Fh(ino),
                off,
                WriteData::Borrowed(data),
                OpenFlags::WRITE,
                r,
            )
        })
    }

    fn read(&self, ino: Ino, off: u64, len: u32) -> Vec<u8> {
        Blocking::run(|r| {
            self.fs.read(
                &self.cx(OpKind::Read),
                ino,
                self.fs.any_handle(ino),
                off,
                len,
                r,
            )
        })
        .expect("read")
        .contiguous()
        .unwrap()
        .into_owned()
    }

    /// The chunk list of `ino`'s committed manifest.
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

/// A file of `len` bytes, written, flushed and its chunks demoted to
/// clean (as an upload leaves them): the committed manifest is what a read
/// answers from, and nothing holds a write session.
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

/// The whole backing file, read through the descriptor the open handed out.
fn through_fd(backing: &constellation_vfs::PassthroughChunk) -> Vec<u8> {
    let mut buf = vec![0u8; backing.len as usize];
    backing.fd.read_exact_at(&mut buf, 0).unwrap();
    buf
}

#[test]
fn an_eligible_read_only_open_is_answered_with_the_chunk_file() {
    let e = env();
    let (ino, data) = file(&e, "one", 4096, 1);
    let hash = e.chunks(ino)[0];

    let opened = e.open_ro(ino);
    assert_eq!(e.fs.handle_ino(opened.fh), Some(ino));
    let backing = opened.backing.as_ref().expect("backing chunk file");
    assert_eq!(backing.len, data.len() as u64);
    assert_eq!(backing.hash, hash.0);
    // The descriptor really is that chunk, and it is the whole file.
    assert_eq!(through_fd(backing), data);

    // The chunk is pinned for exactly as long as the handle lives, and
    // the view holds the pin (not the caller's `Opened`).
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    assert_eq!(e.fs.passthrough_handles(ino), 1);
    e.release(ino);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    // The caller's own clone of the descriptor still reads: dropping the
    // pin is not closing the file.
    assert_eq!(through_fd(backing), data);
}

/// Plan 38 Z3c: a chunk the memory tier holds is a memory hit for the
/// daemon, which the kernel reading the chunk file would only be slower
/// than, so it is not offered — while a verified chunk only on disk (one
/// the tier does not hold) is, in the same view. Asking moves none of the
/// tier's counters.
#[test]
fn a_chunk_held_in_memory_is_not_offered() {
    let e = env_with_memory();
    let (hot, hot_data) = file(&e, "hot", IN_MEMORY, 30);
    let (disk, disk_data) = file(&e, "disk", NOT_IN_MEMORY, 31);
    let hot_hash = e.chunks(hot)[0];
    let disk_hash = e.chunks(disk)[0];

    // Written, so verified on disk; a write does not admit to memory, and
    // until a read does the chunk is offered.
    assert!(!e.cache.in_memory(&hot_hash));
    let opened = e.open_ro(hot);
    assert!(opened.backing.is_some(), "verified, on disk, not in memory");
    e.release(hot);

    // The first read admits the small chunk and not the large one.
    assert_eq!(e.read(hot, 0, IN_MEMORY as u32), hot_data);
    assert_eq!(e.read(disk, 0, NOT_IN_MEMORY as u32), disk_data);
    assert!(e.cache.in_memory(&hot_hash));
    assert!(!e.cache.in_memory(&disk_hash));
    assert_eq!(e.cache.resident(&disk_hash).map(|r| r.verified), Some(true));

    let stats = e.cache.memory_stats().unwrap();
    assert!(
        e.open_ro(hot).backing.is_none(),
        "a memory-resident chunk was handed to the kernel"
    );
    assert_eq!(e.cache.open_pin_count(&hot_hash), 0);
    assert_eq!(e.fs.passthrough_handles(hot), 0);
    let after = e.cache.memory_stats().unwrap();
    assert_eq!(
        (after.hits, after.misses),
        (stats.hits, stats.misses),
        "the refusal moved the memory tier's counters"
    );
    // Its reads are memory hits, as before passthrough.
    assert_eq!(e.read(hot, 0, IN_MEMORY as u32), hot_data);
    assert_eq!(e.cache.memory_stats().unwrap().hits, after.hits + 1);
    e.release(hot);

    let opened = e.open_ro(disk);
    let backing = opened.backing.as_ref().expect("verified and only on disk");
    assert_eq!(through_fd(backing), disk_data);
    assert_eq!(e.cache.open_pin_count(&disk_hash), 1);
    e.release(disk);
    assert_eq!(e.cache.open_pin_count(&disk_hash), 0);
}

/// A frontend that does not declare it consumes `Opened::backing` is
/// never offered one, and the engine takes no pin and opens no chunk file
/// on its behalf (plan 31 §6.6's capability bit): until plan 38 Z3b wires
/// the FUSE reply that consumes a backing file, every frontend in the
/// tree drops it, and a pin held for a reader that does not exist is a
/// chunk the cache cannot evict for nothing.
#[test]
fn a_frontend_that_cannot_consume_a_backing_file_is_not_offered_one() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (fs, dir, cache) = super::quota_tests::test_fs_with(meta.clone(), CHUNK, |root| {
        DiskCache::open(root, 1 << 30)
            .unwrap()
            .with_memory_cache(16 << 20)
    });
    assert!(
        !fs.caps().passthrough,
        "Linux FUSE declares passthrough only once FUSE_INIT agreed it"
    );
    let e = Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    };
    let (ino, data) = file(&e, "unconsumed", 4096, 18);
    let hash = e.chunks(ino)[0];
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    // `node.status` says so, and why (plan 38 §5).
    let status = e.fs.passthrough_status();
    assert!(!status.enabled && status.opens == 0);
    assert_eq!(status.unavailable_reason, Some("frontend"));
    // Reads go the ordinary way, exactly as before this plan.
    assert_eq!(e.read(ino, 0, 4096), data);
    e.release(ino);
}

#[test]
fn a_write_intent_open_is_not_offered_a_backing_file() {
    let e = env();
    let (ino, _) = file(&e, "rw", 4096, 2);
    let hash = e.chunks(ino)[0];
    for flags in [
        OpenFlags::READ | OpenFlags::WRITE,
        OpenFlags::WRITE,
        OpenFlags::READ | OpenFlags::TRUNC,
        OpenFlags::READ | OpenFlags::APPEND,
    ] {
        let opened = e.open(ino, flags);
        assert!(
            opened.backing.is_none(),
            "{flags:?} was offered passthrough"
        );
        assert_eq!(e.cache.open_pin_count(&hash), 0, "{flags:?} took a pin");
        assert_eq!(e.fs.passthrough_handles(ino), 0);
        e.release_with(ino, flags);
    }
    // Every write-intent handle closed: the next read-only open is eligible.
    assert!(e.open_ro(ino).backing.is_some());
    e.release(ino);
}

/// Plan 38 Z3b: a handle that can write the file keeps every read-only
/// open of it off passthrough until it closes — such a handle's writes are
/// visible to an ordinary read (the `WriteState` overlay) and would never
/// be to a passthrough one, which reads the chunk file.
#[test]
fn a_writer_keeps_read_only_opens_off_passthrough_until_it_closes() {
    let e = env();
    let (ino, _) = file(&e, "writer", 4096, 21);
    let hash = e.chunks(ino)[0];
    for writer in [OpenFlags::WRITE, OpenFlags::READ | OpenFlags::WRITE] {
        let _w = e.open(ino, writer);
        // Before it wrote a byte: there is no write session yet, only the
        // handle, and that is enough.
        assert!(
            e.open_ro(ino).backing.is_none(),
            "{writer:?} open, yet a reader was offered passthrough"
        );
        assert_eq!(e.cache.open_pin_count(&hash), 0);
        e.release(ino);
        e.release_with(ino, writer);
    }
    // Two writers: the first close leaves the second counted.
    e.open(ino, OpenFlags::WRITE);
    e.open(ino, OpenFlags::WRITE);
    e.release_with(ino, OpenFlags::WRITE);
    assert!(e.open_ro(ino).backing.is_none());
    e.release(ino);
    e.release_with(ino, OpenFlags::WRITE);
    let opened = e.open_ro(ino);
    assert_eq!(opened.backing.map(|b| b.hash), Some(hash.0));
    e.release(ino);
    // A writer that `create` opened counts the same way (as root: the
    // test's files are root's, in root's directory).
    let root = Caller::new(0, 0, None);
    let (created, _) = Blocking::run(|r| {
        e.fs.create(
            &OpCtx::new(OpKind::Create, &root),
            ROOT_INO,
            Name::new("writer"),
            0o100644,
            OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .unwrap();
    assert_eq!(created.attr.ino, ino, "create opened the existing file");
    assert!(e.open_ro(ino).backing.is_none());
    e.release(ino);
    e.release_with(ino, OpenFlags::WRITE);
    assert!(e.open_ro(ino).backing.is_some());
    e.release(ino);
}

/// What the frontend negotiated decides, not what it declared before it
/// existed (Linux FUSE learns at `FUSE_INIT`): off stops new offers and
/// leaves an open handle's pin to its own `release`.
#[test]
fn the_frontends_negotiation_turns_offers_on_and_off() {
    let e = env();
    let (ino, _) = file(&e, "negotiated", 4096, 22);
    let hash = e.chunks(ino)[0];
    let held = e.open_ro(ino);
    assert!(held.backing.is_some());
    let mut caps = e.fs.caps().clone();
    caps.passthrough = false;
    e.fs.frontend_negotiated(&caps);
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(
        e.cache.open_pin_count(&hash),
        1,
        "the held handle keeps its pin"
    );
    e.release(ino);
    e.release(ino);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    caps.passthrough = true;
    e.fs.frontend_negotiated(&caps);
    assert!(e.open_ro(ino).backing.is_some());
    e.release(ino);
}

/// Coordinator requirement for plan 38 Z3b: a chunk backing a live
/// passthrough handle is never evictable across a handover. The kernel
/// keeps serving the handed-over descriptor from the old process's
/// backing file, so the old view keeps its pin until it is gone (the
/// process `exec`s — `Engine::close_view_for_handover` leaves the pins),
/// and the resumed view re-pins the same chunk from the snapshot before
/// that. Two views of one disk cache stand in for the two images.
#[test]
fn a_chunk_backing_a_live_handle_is_never_evictable_across_a_handover() {
    let e = env();
    let (ino, data) = file(&e, "handed-over", 4096, 23);
    let hash = e.chunks(ino)[0];
    let held = e.open_ro(ino);
    let backing = held.backing.clone().expect("backing chunk file");
    // A write-intent handle on another file crosses too.
    let (other, _) = file(&e, "other", 100, 24);
    e.open(other, OpenFlags::WRITE);

    let snapshot = e.fs.export_handles();
    assert_eq!(snapshot.passthrough, vec![(ino, vec![hash.0])]);
    assert_eq!(snapshot.writers, vec![(other, 1)]);
    let json = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(
        serde_json::from_str::<HandleTableSnapshot>(&json).unwrap(),
        snapshot
    );

    // The new image's view, on the same node's cache, adopts it.
    let next = super::quota_tests::test_fs_on(
        e.meta.clone(),
        CHUNK,
        e.cache.clone(),
        e._dir.path().join("staging-next"),
    );
    let mut caps = next.caps().clone();
    caps.passthrough = true;
    next.frontend_negotiated(&caps);
    next.import_handles(&snapshot);
    assert_eq!(e.cache.open_pin_count(&hash), 2, "both images pin it");
    assert_eq!(next.passthrough_hashes(ino), vec![hash]);

    // The old image goes away (its pins with it): still pinned, by the
    // new one, through any prune.
    drop(e.fs);
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    let report = e.cache.prune_to(0).unwrap();
    assert!(
        e.cache.contains(&hash),
        "evicted under a live handle: {report:?}"
    );
    assert_eq!(through_fd(&backing), data);
    // The writer crossed: a reader of `other` is not offered passthrough.
    let cx = OpCtx::new(OpKind::Open, &e.caller);
    let opened =
        Blocking::run(|r| next.open(&cx, other, OpenFlags::READ, OpenOwner::NONE, r)).unwrap();
    assert!(opened.backing.is_none());

    // The handed-over handle's release, on the new view, lets it go.
    let rel = |ino: Ino, flags: OpenFlags| {
        let cx = OpCtx::new(OpKind::Release, &e.caller);
        Blocking::run(|r| next.release(&cx, ino, Fh(ino), flags, None, r)).unwrap();
    };
    rel(ino, OpenFlags::READ);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    e.cache.prune_to(0).unwrap();
    assert!(!e.cache.contains(&hash), "evictable once the handle closed");
    rel(other, OpenFlags::READ);
    rel(other, OpenFlags::WRITE);
    assert!(next.export_handles().writers.is_empty());
}

#[test]
fn a_multi_chunk_file_is_not_offered_a_backing_file() {
    let e = env();
    // One byte past the chunk size: two chunks, so no single file is the
    // file's content.
    let (ino, _) = file(&e, "two", CHUNK as usize + 1, 3);
    assert_eq!(e.chunks(ino).len(), 2);
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    e.release(ino);

    // Exactly the chunk size is still one chunk, and is offered.
    let (big, data) = file(&e, "exact", CHUNK as usize, 4);
    assert_eq!(e.chunks(big).len(), 1);
    let opened = e.open_ro(big);
    let backing = opened.backing.as_ref().expect("one whole chunk");
    assert_eq!(backing.len, u64::from(CHUNK));
    assert_eq!(through_fd(backing), data);
    e.release(big);
}

#[test]
fn an_empty_file_is_not_offered_a_backing_file() {
    let e = env();
    let f = e.meta.create(ROOT_INO, "empty", 0o644, 0, 0).unwrap();
    assert!(e.open_ro(f.ino).backing.is_none());
    assert_eq!(e.fs.passthrough_handles(f.ino), 0);
}

#[test]
fn a_chunk_that_is_not_in_the_disk_cache_is_not_offered() {
    let e = env();
    let (ino, _) = file(&e, "cold", 4096, 5);
    let hash = e.chunks(ino)[0];
    e.cache.remove(&hash).unwrap();
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
}

/// A chunk file a restart's directory scan found has been hashed by
/// nobody in this process (plan 38 §2.3's `verified` bit): the kernel must
/// not be handed bytes nobody checked. The first ordinary read hashes it,
/// and the open after that qualifies.
#[test]
fn an_unverified_chunk_is_hashed_by_a_read_before_it_is_offered() {
    // Build the chunk files and the metadata in one view, then stand a
    // second view up over a copy of that cache directory: its rescan
    // creates the same entries with `verified` clear.
    let first = env();
    let (ino, data) = file(&first, "scanned", 4096, 6);
    let hash = first.chunks(ino)[0];
    let seeded = first.cache.chunk_path(&hash);
    let meta = first.meta.clone();

    let hex = hash.to_hex();
    let (fs, _dir, cache) = fs_with(meta.clone(), CacheVerify::Admit, |root| {
        let dest = root.join(&hex[0..2]).join(&hex[2..4]).join(&hex);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::copy(&seeded, &dest).unwrap();
    });
    let e = Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir,
    };
    assert_eq!(
        e.cache.resident(&hash).map(|r| r.verified),
        Some(false),
        "the rescan should have found the chunk unverified"
    );
    assert!(
        e.open_ro(ino).backing.is_none(),
        "an unhashed chunk file was handed out"
    );
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    e.release(ino);

    // One ordinary read hashes it; now it is offered.
    assert_eq!(e.read(ino, 0, 4096), data);
    assert_eq!(
        e.cache.resident(&hash),
        Some(Resident {
            len: 4096,
            state: ChunkState::Clean,
            verified: true,
        })
    );
    let opened = e.open_ro(ino);
    assert_eq!(
        through_fd(opened.backing.as_ref().expect("now verified")),
        data
    );
    e.release(ino);
}

#[test]
fn a_pending_write_or_truncate_is_not_offered_a_backing_file() {
    let e = env();
    let (ino, _) = file(&e, "pending", 4096, 7);
    let hash = e.chunks(ino)[0];
    // A write session: the committed manifest is not what a read answers.
    e.write(ino, 0, b"edited").unwrap();
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    e.release(ino);

    // A pending truncate is the same session with its `floor` set
    // (`WriteState::floor`), so the same check refuses it. Truncating
    // down to a length still inside one chunk is the case that would
    // otherwise look eligible.
    let (other, _) = file(&e, "truncated", 4096, 8);
    Blocking::run(|r| {
        e.fs.setattr(
            &e.cx(OpKind::Setattr),
            other,
            None,
            &SetAttr {
                size: Some(100),
                ..SetAttr::default()
            },
            r,
        )
    })
    .expect("truncate");
    assert!(e.open_ro(other).backing.is_none());
    e.release(other);
}

/// A committed truncate lowers the file's length but leaves the chunk
/// straddling the new end with its dead bytes (a read zeroes them; the
/// kernel reading the file would not). So the chunk file being *longer*
/// than the file is its own refusal, independent of what the manifest
/// says.
#[test]
fn a_chunk_longer_than_the_file_is_not_offered() {
    let e = env();
    let (ino, _) = file(&e, "clipped", 4096, 9);
    let hash = e.chunks(ino)[0];
    e.meta
        .setattr(ino, None, None, None, Some(1000), None, None)
        .unwrap();
    assert_eq!(e.meta.getattr(ino).unwrap().unwrap().size, 1000);
    assert_eq!(e.chunks(ino), vec![hash], "the chunk itself is unchanged");
    assert_eq!(
        e.cache.resident(&hash).map(|r| r.len),
        Some(4096),
        "the chunk file still holds the bytes the truncate cut"
    );
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 0);
}

/// `--cache-verify always` promises the daemon hashes every byte it
/// serves on the read that serves it; a backing file is read by the
/// kernel, so passthrough is simply never offered (plan 38 §2.3).
#[test]
fn cache_verify_always_never_offers_a_backing_file() {
    let e = env_with(CacheVerify::Always, |_| {});
    let (ino, data) = file(&e, "always", 4096, 10);
    let hash = e.chunks(ino)[0];
    assert!(e.open_ro(ino).backing.is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    let status = e.fs.passthrough_status();
    assert!(!status.enabled);
    assert_eq!(status.unavailable_reason, Some("cache_verify_always"));
    // Reads are unaffected: they go the ordinary way, as always.
    assert_eq!(e.read(ino, 0, 4096), data);
    e.release(ino);
}

#[test]
fn the_pin_count_is_the_number_of_concurrent_opens() {
    let e = env();
    let (ino, data) = file(&e, "shared", 4096, 11);
    let hash = e.chunks(ino)[0];
    let handles: Vec<Opened> = (0..3).map(|_| e.open_ro(ino)).collect();
    assert!(handles.iter().all(|o| o.backing.is_some()));
    assert_eq!(e.cache.open_pin_count(&hash), 3);
    assert_eq!(e.fs.passthrough_handles(ino), 3);
    // What `node.status.fuse` and `constellation_fuse_passthrough_opens`
    // report for the mount.
    let status = e.fs.passthrough_status();
    assert_eq!((status.enabled, status.opens), (true, 3));
    assert_eq!(status.unavailable_reason, None);
    for expected in [2, 1, 0] {
        e.release(ino);
        assert_eq!(e.cache.open_pin_count(&hash), expected);
        assert_eq!(e.fs.passthrough_handles(ino), expected as usize);
        assert_eq!(e.fs.passthrough_status().opens, u64::from(expected));
    }
    // Every descriptor handed out is still the file's content.
    for o in &handles {
        assert_eq!(through_fd(o.backing.as_ref().unwrap()), data);
    }
}

/// `release` names the inode, not the handle (`Fh(ino)` — §6.12), so what
/// a release drops is whatever the inode holds beyond the handles still
/// open on it, never "one entry per release": an inode with both a
/// passthrough and an ordinary handle would otherwise lose a pin whose
/// handle is still being served, leaving the chunk an eviction candidate
/// under a live descriptor.
#[test]
fn releasing_an_ineligible_handle_keeps_an_eligible_handles_pin() {
    let e = env();
    let (ino, data) = file(&e, "mixed", 4096, 19);
    let hash = e.chunks(ino)[0];

    let reader = e.open_ro(ino);
    let backing = reader.backing.as_ref().expect("backing chunk file");
    // A second open with write intent: no backing file, no pin of its
    // own, and the reader's pin untouched.
    assert!(e
        .open(ino, OpenFlags::READ | OpenFlags::WRITE)
        .backing
        .is_none());
    assert_eq!(e.cache.open_pin_count(&hash), 1);

    // The writer closes first — the reader is still open.
    e.release(ino);
    assert_eq!(
        e.cache.open_pin_count(&hash),
        1,
        "the reader's pin went with another handle's release"
    );
    assert_eq!(e.fs.passthrough_hashes(ino), vec![hash]);
    // So the chunk it is served from is still not an eviction candidate,
    // and still readable through the descriptor the open handed out.
    assert_eq!(e.cache.prune_to(0).unwrap().freed_chunks, 0);
    assert!(e.cache.contains(&hash));
    assert_eq!(through_fd(backing), data);

    // The reader's own close releases it: nothing leaks either.
    e.release(ino);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    assert_eq!(e.cache.prune_to(0).unwrap().freed_chunks, 1);
}

/// While a handle holds the chunk file, eviction cannot take it — and
/// takes it the moment the handle is released (plan 38 §3(c): "pin while
/// open", so the cache's `used` keeps covering what is on the disk).
#[test]
fn an_open_chunk_survives_a_prune_and_is_evicted_after_the_release() {
    let e = env();
    let (ino, data) = file(&e, "pruned", 4096, 12);
    let hash = e.chunks(ino)[0];
    let opened = e.open_ro(ino);
    let backing = opened.backing.as_ref().expect("backing chunk file");

    let report = e.cache.prune_to(0).unwrap();
    assert_eq!(report.freed_chunks, 0, "a pinned-open chunk was evicted");
    assert!(e.cache.contains(&hash));
    assert_eq!(e.cache.usage().used, 4096);
    assert_eq!(through_fd(backing), data);

    e.release(ino);
    let report = e.cache.prune_to(0).unwrap();
    assert_eq!((report.freed_chunks, report.freed_bytes), (1, 4096));
    assert!(!e.cache.contains(&hash));
    // The handle's own descriptor outlives the unlink, as any open file
    // does; a *new* open finds nothing cached and is not offered one.
    assert_eq!(through_fd(backing), data);
    assert!(e.open_ro(ino).backing.is_none());
}

/// Close-to-open: a handle keeps reading the chunk it was opened on even
/// after a new manifest supersedes it, and the next open sees the new
/// chunk. There is no mid-open way to revoke a backing file, and plan
/// 30's `cto=strict` already promises exactly this granularity.
#[test]
fn a_new_manifest_leaves_an_open_handle_on_its_own_chunk() {
    let e = env();
    let (ino, old) = file(&e, "cto", 4096, 13);
    let old_hash = e.chunks(ino)[0];
    let opened = e.open_ro(ino);
    let backing = opened.backing.as_ref().expect("backing chunk file");
    assert_eq!(backing.hash, old_hash.0);

    // New content published under the same inode (what a remote write
    // landing a new manifest looks like from this view's read path).
    let new = bytes(4096, 99);
    e.fs.do_write(ino, 0, &new).unwrap();
    e.fs.flush_inode(ino, false).unwrap();
    let new_hash = e.chunks(ino)[0];
    assert_ne!(new_hash, old_hash);
    e.cache.set_state(&new_hash, ChunkState::Clean);

    // The open handle still reads the chunk it was opened on, and that
    // chunk is still pinned (so it is still there to read).
    assert_eq!(through_fd(backing), old);
    assert_eq!(e.cache.open_pin_count(&old_hash), 1);

    // A fresh open is on the new chunk.
    let fresh = e.open_ro(ino);
    let fresh_backing = fresh.backing.as_ref().expect("backing chunk file");
    assert_eq!(fresh_backing.hash, new_hash.0);
    assert_eq!(through_fd(fresh_backing), new);
    assert_eq!(e.cache.open_pin_count(&new_hash), 1);
    assert_eq!(e.fs.passthrough_handles(ino), 2);

    e.release(ino);
    e.release(ino);
    assert_eq!(e.cache.open_pin_count(&old_hash), 0);
    assert_eq!(e.cache.open_pin_count(&new_hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
}

/// No `release` ever arrives when the frontend serving the view is gone
/// (`Engine::close_view`, a handover, or simply the view being dropped):
/// the pins go then, not whenever the last reference happens to die.
#[test]
fn a_view_torn_down_without_releases_drops_its_pins() {
    let e = env();
    let (ino, _) = file(&e, "orphaned", 4096, 14);
    let hash = e.chunks(ino)[0];
    let _a = e.open_ro(ino);
    let _b = e.open_ro(ino);
    assert_eq!(e.cache.open_pin_count(&hash), 2);
    e.fs.drop_all_passthrough();
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    // Idempotent, and a later `release` of a handle whose state is gone
    // is not an error.
    e.fs.drop_all_passthrough();
    e.release(ino);
    e.release(ino);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
}

/// The two read-path hooks a passthrough handle's reads would never reach
/// fire from the open instead (plan 38 §3(c)). The atime bump is the
/// observable one: with `--atime relatime` on, an eligible open records a
/// bump without any read reaching the daemon.
#[test]
fn an_eligible_open_bumps_atime_and_kicks_scan_ahead() {
    let meta = Arc::new(Meta::open_in_memory().unwrap());
    let (mut fs, dir, cache) = fs_with(meta.clone(), CacheVerify::Admit, |_| {});
    let atime = Arc::new(crate::atime::AtimeAccumulator::new(
        crate::atime::AtimeMode::Relatime,
        crate::atime::AtimeStats::new(),
    ));
    fs.atime = atime.clone();
    let e = Env {
        fs,
        cache,
        meta,
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    };
    let (ino, _) = file(&e, "atimed", 4096, 15);
    // The write's own flush left no bump pending.
    atime.drain();
    let opened = e.open_ro(ino);
    assert!(opened.backing.is_some());
    let bumps = atime.drain();
    assert!(
        bumps.iter().any(|(i, _, _)| *i == ino),
        "the open recorded no atime bump: {bumps:?}"
    );
    e.release(ino);

    // An ineligible open does not: its reads still reach the daemon and
    // bump there, exactly as before this plan.
    let (rw, _) = file(&e, "atimed-rw", 4096, 16);
    atime.drain();
    assert!(e
        .open(rw, OpenFlags::READ | OpenFlags::WRITE)
        .backing
        .is_none());
    assert!(atime.drain().iter().all(|(i, _, _)| *i != rw));
    assert_eq!(e.read(rw, 0, 4096).len(), 4096);
    assert!(atime.drain().iter().any(|(i, _, _)| *i == rw));
}

/// `create` never offers a backing file: it has write intent by
/// definition, and nothing is cached for a file that does not exist yet.
#[test]
fn create_never_offers_a_backing_file() {
    let e = env();
    let (_, opened) = Blocking::run(|r| {
        e.fs.create(
            &e.cx(OpKind::Create),
            ROOT_INO,
            Name::new("fresh"),
            0o100644,
            OpenFlags::READ | OpenFlags::WRITE,
            OpenOwner::NONE,
            r,
        )
    })
    .expect("create");
    assert!(opened.backing.is_none());
    Blocking::run(|r| {
        e.fs.flush(
            &e.cx(OpKind::Flush),
            opened.fh.0,
            opened.fh,
            LockOwner(1),
            r,
        )
    })
    .expect("flush");
}

// --- Plan 38 Z3c: frozen snapshot files ---------------------------------

/// A snapshot of `/vol` holding one file per `(name, len)`, written,
/// flushed and demoted to clean as [`file`] leaves them; the snapshot's
/// tree is published through a manager that shares the view's chunk
/// store. Returns the frozen inode of each file, as
/// `/vol/.constellation/snapshot/snap/<name>` looks it up, with its bytes.
fn frozen_files(e: &mut Env, files: &[(&str, usize)]) -> (Vec<(Ino, Vec<u8>)>, TempDir) {
    let vol = e.meta.mkdir(ROOT_INO, "vol", 0o755, 0, 0).unwrap();
    let mut live = Vec::new();
    for (i, (name, len)) in files.iter().enumerate() {
        let data = bytes(*len, 40 + i as u8);
        let f = e.meta.create(vol.ino, name, 0o644, 0, 0).unwrap();
        if !data.is_empty() {
            e.fs.do_write(f.ino, 0, &data).unwrap();
            e.fs.flush_inode(f.ino, false).unwrap();
            for h in e.chunks(f.ino) {
                e.cache.set_state(&h, ChunkState::Clean);
            }
        }
        live.push((*name, data));
    }
    let (manager, nodes) = crate::snapshot::test_manager(e.meta.clone(), e.fs.store.clone(), CHUNK);
    let rows = e.meta.take_journal(usize::MAX).unwrap();
    let seqs: Vec<u64> = rows.iter().map(|(s, _)| *s).collect();
    e.meta.ack_journal_rows_at(&seqs, 1).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(manager.create("/vol", "snap"))
        .unwrap();
    e.fs.snapshots = Arc::new(manager);
    let mut dir = vol.ino;
    for name in [".constellation", "snapshot", "snap"] {
        dir = e.fs.lookup_synthetic(dir, name).unwrap().unwrap().0;
    }
    let frozen = live
        .into_iter()
        .map(|(name, data)| {
            let (ino, attr) = e.fs.lookup_synthetic(dir, name).unwrap().unwrap();
            assert!(View::is_synthetic(ino));
            assert_eq!(attr.size, data.len() as u64);
            (ino, data)
        })
        .collect();
    (frozen, nodes)
}

impl Env {
    /// `release` of exactly `fh` (a frozen file's pins are trimmed to the
    /// handles the table still lists, so the test hands back the real one).
    fn release_fh(&self, ino: Ino, fh: Fh) {
        Blocking::run(|r| {
            self.fs
                .release(&self.cx(OpKind::Release), ino, fh, OpenFlags::READ, None, r)
        })
        .expect("release");
    }

    fn read_fh(&self, ino: Ino, fh: Fh, off: u64, len: u32) -> Vec<u8> {
        Blocking::run(|r| self.fs.read(&self.cx(OpKind::Read), ino, fh, off, len, r))
            .expect("read")
            .contiguous()
            .unwrap()
            .into_owned()
    }
}

/// The snapshot view's whole point under the read-only default: a frozen
/// one-chunk file whose chunk is cached and verified is answered with the
/// chunk file, pinned until the handle's release; an open for writing is
/// still `EROFS`.
#[test]
fn a_frozen_one_chunk_file_is_answered_with_the_chunk_file() {
    let mut e = env();
    let (frozen, _nodes) = frozen_files(&mut e, &[("one", 4096)]);
    let (ino, data) = frozen[0].clone();
    let vol = e.meta.lookup(ROOT_INO, "vol").unwrap().unwrap().ino;
    let hash = e.chunks(e.meta.lookup(vol, "one").unwrap().unwrap().ino)[0];

    let opened = e.open_ro(ino);
    let backing = opened.backing.as_ref().expect("backing chunk file");
    assert_eq!(backing.len, data.len() as u64);
    assert_eq!(backing.hash, hash.0);
    assert_eq!(through_fd(backing), data);
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    assert_eq!(e.fs.passthrough_handles(ino), 1);
    // The handle still reads through the daemon (a frontend that is not
    // FUSE, or a kernel that falls back), and the same bytes.
    assert_eq!(e.read_fh(ino, opened.fh, 0, 8192), data);
    // A synthetic inode never joins `opens` (the hold writer's table).
    assert!(!e.fs.opens.lock().unwrap().contains_key(&ino));

    for flags in [
        OpenFlags::WRITE,
        OpenFlags::READ | OpenFlags::WRITE,
        OpenFlags::READ | OpenFlags::TRUNC,
    ] {
        let refused = Blocking::run(|r| {
            e.fs.open(&e.cx(OpKind::Open), ino, flags, OpenOwner::NONE, r)
        });
        assert_eq!(
            refused.err().map(|e| e.code()),
            Some(Code::ReadOnly),
            "{flags:?}"
        );
    }
    assert_eq!(
        e.cache.open_pin_count(&hash),
        1,
        "a refused open pins nothing"
    );

    e.release_fh(ino, opened.fh);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    e.cache.prune_to(0).unwrap();
    assert!(!e.cache.contains(&hash), "evictable once the handle closed");
}

/// Two handles of a frozen file pin it twice; the first release leaves
/// the other's pin, the second takes it — counted from the handle table,
/// since a synthetic inode is not in `opens`.
#[test]
fn a_frozen_files_pins_follow_its_open_handles() {
    let mut e = env();
    let (frozen, _nodes) = frozen_files(&mut e, &[("two", 1000)]);
    let (ino, _) = frozen[0].clone();
    let a = e.open_ro(ino);
    let b = e.open_ro(ino);
    let hash = ChunkHash(a.backing.as_ref().unwrap().hash);
    assert_eq!(e.cache.open_pin_count(&hash), 2);
    e.release_fh(ino, a.fh);
    assert_eq!(e.cache.open_pin_count(&hash), 1);
    e.cache.prune_to(0).unwrap();
    assert!(e.cache.contains(&hash), "evicted under a live handle");
    e.release_fh(ino, b.fh);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
}

/// What the live rule refuses for its shape or residency, the frozen one
/// refuses too: more than one chunk, nothing to hand over, a chunk that is
/// not cached, and a frontend that cannot consume a backing file.
#[test]
fn ineligible_frozen_files_are_served_by_reads() {
    let mut e = env();
    let big = CHUNK as usize + 10;
    let (frozen, _nodes) = frozen_files(
        &mut e,
        &[("big", big), ("empty", 0), ("cold", 300), ("small", 300)],
    );
    let opened = e.open_ro(frozen[0].0);
    assert!(opened.backing.is_none(), "multi-chunk");
    assert_eq!(
        e.read_fh(frozen[0].0, opened.fh, 0, big as u32),
        frozen[0].1
    );
    assert!(e.open_ro(frozen[1].0).backing.is_none(), "empty");

    let vol = e.meta.lookup(ROOT_INO, "vol").unwrap().unwrap().ino;
    let cold = e.meta.lookup(vol, "cold").unwrap().unwrap().ino;
    let cold_hash = e.chunks(cold)[0];
    // Out of the disk cache (the in-memory store keeps a copy only of
    // what was uploaded, which nothing here did — so it is not read).
    e.cache.remove(&cold_hash).unwrap();
    assert!(e.open_ro(frozen[2].0).backing.is_none(), "not resident");

    e.fs.set_passthrough_on(false);
    assert!(e.open_ro(frozen[3].0).backing.is_none(), "frontend");
    e.fs.set_passthrough_on(true);
    assert!(e.open_ro(frozen[3].0).backing.is_some());
}

/// The memory-tier refusal holds for a frozen file too (it lives in the
/// residency check both rules share): a snapshot file whose chunk is in
/// memory is served by reads, one whose verified chunk is only on disk is
/// offered.
#[test]
fn a_frozen_file_held_in_memory_is_not_offered() {
    let mut e = env_with_memory();
    let (frozen, _nodes) = frozen_files(&mut e, &[("hot", IN_MEMORY), ("disk", NOT_IN_MEMORY)]);
    let (hot, hot_data) = frozen[0].clone();
    let (disk, disk_data) = frozen[1].clone();

    // Not in memory yet: offered. Its handle's read still reaches the
    // daemon here (no kernel), which admits the chunk to memory.
    let first = e.open_ro(hot);
    let hash = ChunkHash(first.backing.as_ref().expect("not in memory yet").hash);
    assert_eq!(e.read_fh(hot, first.fh, 0, IN_MEMORY as u32 * 2), hot_data);
    e.release_fh(hot, first.fh);
    assert!(e.cache.in_memory(&hash));
    let opened = e.open_ro(hot);
    assert!(opened.backing.is_none(), "a memory-resident frozen chunk");
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.read_fh(hot, opened.fh, 0, IN_MEMORY as u32 * 2), hot_data);
    e.release_fh(hot, opened.fh);

    let opened = e.open_ro(disk);
    assert_eq!(
        through_fd(opened.backing.as_ref().expect("verified, only on disk")),
        disk_data
    );
    e.release_fh(disk, opened.fh);
}

/// A frozen chunk that is resident but unverified (found by a restart's
/// rescan, hashed by nobody in this process) is not offered; the first
/// read hashes it, and the open after that is passthrough.
#[test]
fn an_unverified_frozen_chunk_is_hashed_by_a_read_before_it_is_offered() {
    let mut first = env();
    let (frozen, nodes) = frozen_files(&mut first, &[("scanned", 4096)]);
    let data = frozen[0].1.clone();
    let vol = first.meta.lookup(ROOT_INO, "vol").unwrap().unwrap().ino;
    let hash = first.chunks(first.meta.lookup(vol, "scanned").unwrap().unwrap().ino)[0];
    let seeded = first.cache.chunk_path(&hash);

    // A second view over a copy of the cache directory, sharing the
    // metadata, the store and the snapshot manager: its rescan finds the
    // chunk unverified.
    let hex = hash.to_hex();
    let (mut fs, dir, cache) = fs_with(first.meta.clone(), CacheVerify::Admit, |root| {
        let dest = root.join(&hex[0..2]).join(&hex[2..4]).join(&hex);
        std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
        std::fs::copy(&seeded, &dest).unwrap();
    });
    fs.store = first.fs.store.clone();
    fs.snapshots = first.fs.snapshots.clone();
    let e = Env {
        fs,
        cache,
        meta: first.meta.clone(),
        caller: Caller::new(1000, 1000, None),
        _dir: dir,
    };
    let _nodes = nodes;
    // The same frozen file, interned in this view.
    let mut snap = vol;
    for name in [".constellation", "snapshot", "snap"] {
        snap = e.fs.lookup_synthetic(snap, name).unwrap().unwrap().0;
    }
    let (ino2, attr) = e.fs.lookup_synthetic(snap, "scanned").unwrap().unwrap();
    assert!(View::is_synthetic(ino2) && attr.size == data.len() as u64);
    assert_eq!(e.cache.resident(&hash).map(|r| r.verified), Some(false));

    let opened = e.open_ro(ino2);
    assert!(
        opened.backing.is_none(),
        "an unhashed frozen chunk was handed out"
    );
    assert_eq!(e.cache.open_pin_count(&hash), 0);
    assert_eq!(e.read_fh(ino2, opened.fh, 0, 8192), data);
    e.release_fh(ino2, opened.fh);
    assert_eq!(e.cache.resident(&hash).map(|r| r.verified), Some(true));

    let opened = e.open_ro(ino2);
    assert_eq!(
        through_fd(opened.backing.as_ref().expect("now verified")),
        data
    );
    e.release_fh(ino2, opened.fh);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
}

/// A read-only open with `O_APPEND` of a frozen file gets no backing file
/// (append is write intent, which a backing file cannot carry), and is
/// served by reads; the plain read-only open beside it still is offered.
#[test]
fn a_read_only_append_open_of_a_frozen_file_is_not_offered() {
    let mut e = env();
    let (frozen, _nodes) = frozen_files(&mut e, &[("appended", 4096)]);
    let (ino, data) = frozen[0].clone();
    let opened = e.open(ino, OpenFlags::READ | OpenFlags::APPEND);
    assert!(opened.backing.is_none());
    assert_eq!(e.fs.passthrough_handles(ino), 0);
    assert_eq!(e.read_fh(ino, opened.fh, 0, 8192), data);
    let plain = e.open_ro(ino);
    assert!(plain.backing.is_some());
    e.release_fh(ino, opened.fh);
    assert_eq!(
        e.fs.passthrough_handles(ino),
        1,
        "the plain handle's pin stays"
    );
    e.release_fh(ino, plain.fh);
    assert_eq!(e.fs.passthrough_handles(ino), 0);
}

/// A frozen file's manifest is loaded once: the passthrough check at
/// `open` and every `read` after it share the view's cache, whether the
/// open was passthrough or not.
#[test]
fn a_frozen_manifest_is_loaded_once_for_the_open_and_its_reads() {
    let mut e = env();
    let big = CHUNK as usize + 10;
    let (frozen, _nodes) = frozen_files(&mut e, &[("one", 4096), ("big", big)]);
    let (one, data) = frozen[0].clone();
    let (two, big_data) = frozen[1].clone();
    let (entries0, _, misses0) = e.fs.frozen_manifests.stats();

    let opened = e.open_ro(one);
    assert!(opened.backing.is_some());
    for _ in 0..3 {
        assert_eq!(e.read_fh(one, opened.fh, 0, 8192), data);
    }
    e.release_fh(one, opened.fh);
    let reopened = e.open_ro(one);
    e.release_fh(one, reopened.fh);
    let (entries, hits, misses) = e.fs.frozen_manifests.stats();
    assert_eq!(misses, misses0 + 1, "loaded once");
    assert_eq!(entries, entries0 + 1);
    assert!(hits >= 4, "three reads and a reopen: {hits}");

    // A file passthrough refuses on its size never loads it at open; its
    // reads load it once.
    let opened = e.open_ro(two);
    assert!(opened.backing.is_none());
    assert_eq!(e.fs.frozen_manifests.stats().2, misses);
    assert_eq!(e.read_fh(two, opened.fh, 0, big as u32), big_data);
    assert_eq!(e.read_fh(two, opened.fh, 0, 10), big_data[..10]);
    assert_eq!(e.fs.frozen_manifests.stats().2, misses + 1);
    e.release_fh(two, opened.fh);
}

#[test]
fn cache_verify_always_never_offers_a_frozen_file() {
    let mut e = env_with(CacheVerify::Always, |_| {});
    let (frozen, _nodes) = frozen_files(&mut e, &[("one", 4096)]);
    let (ino, data) = frozen[0].clone();
    let opened = e.open_ro(ino);
    assert!(opened.backing.is_none());
    assert_eq!(e.read_fh(ino, opened.fh, 0, 8192), data);
}

/// A frozen passthrough handle crosses a handover like a live one: the
/// new image re-pins its chunk, and its release there (the handle table
/// crossed too) lets it go.
#[test]
fn a_frozen_passthrough_handle_crosses_a_handover() {
    let mut e = env();
    let (frozen, _nodes) = frozen_files(&mut e, &[("kept", 4096)]);
    let (ino, _) = frozen[0].clone();
    let opened = e.open_ro(ino);
    let hash = ChunkHash(opened.backing.as_ref().unwrap().hash);
    let snapshot = e.fs.export_handles();
    assert_eq!(snapshot.passthrough, vec![(ino, vec![hash.0])]);

    let next = super::quota_tests::test_fs_on(
        e.meta.clone(),
        CHUNK,
        e.cache.clone(),
        e._dir.path().join("staging-next"),
    );
    next.import_handles(&snapshot);
    drop(std::mem::replace(&mut e.fs, next));
    assert_eq!(
        e.cache.open_pin_count(&hash),
        1,
        "re-pinned by the new image"
    );
    e.release_fh(ino, opened.fh);
    assert_eq!(e.cache.open_pin_count(&hash), 0);
}

/// Plan 38 Z4b: on a zero-copy frontend, a frozen file passthrough does
/// not take (more than one chunk) is marked zero-copy, and a read inside
/// one of its chunks is a range of that chunk's file, as for a live file;
/// a read across a chunk boundary takes the ordinary path. A frozen file
/// that passthrough takes is not marked: its reads never reach the daemon.
#[test]
fn a_frozen_multi_chunk_file_reads_zero_copy() {
    let mut e = env();
    let big = 2 * CHUNK as usize + 1000;
    let (frozen, _nodes) = frozen_files(&mut e, &[("big", big), ("one", 4096)]);
    let (ino, data) = frozen[0].clone();
    let caps = FrontendCaps {
        zero_copy: true,
        zero_copy_min_read: 0,
        ..e.fs.caps.clone()
    };
    e.fs.frontend_negotiated(&caps);

    let one = e.open_ro(frozen[1].0);
    assert!(one.backing.is_some() && !one.zero_copy, "passthrough wins");

    let opened = e.open_ro(ino);
    assert!(opened.backing.is_none());
    assert!(opened.zero_copy);
    let off = CHUNK as u64 + 100;
    let got = Blocking::run(|r| e.fs.read(&e.cx(OpKind::Read), ino, opened.fh, off, 8192, r))
        .expect("read");
    let zc = got
        .as_zero_copy()
        .expect("a one-chunk read of a frozen file");
    assert_eq!(zc.offset, 100);
    assert_eq!(
        &*got.contiguous().unwrap(),
        &data[off as usize..off as usize + 8192]
    );
    let span = CHUNK as u64 - 10;
    let got = Blocking::run(|r| e.fs.read(&e.cx(OpKind::Read), ino, opened.fh, span, 100, r))
        .expect("read");
    assert!(
        got.as_zero_copy().is_none(),
        "a spanning read went zero-copy"
    );
    assert_eq!(
        e.read_fh(ino, opened.fh, span, 100),
        data[span as usize..span as usize + 100]
    );
    e.release_fh(ino, opened.fh);
    assert_eq!(e.fs.zero_copy_handles(), 0);
}
