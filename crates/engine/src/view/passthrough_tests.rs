//! Plan 38 §3(c): which opens are answered with the chunk file itself
//! (`Opened::backing`), what the handle holds while it lives, and what it
//! releases. Driven through the `Vfs` trait exactly as a frontend drives
//! it — no kernel, no FUSE: the `FOPEN_PASSTHROUGH` wiring is the plan's
//! next chunk, and everything asserted here is true without it.

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
/// *consumes* `Opened::backing`. No real frontend declares that before
/// plan 38 Z3b wires the FUSE reply (`FrontendCaps::passthrough`), and
/// the engine offers nothing to a frontend that would drop it, so every
/// test here says so explicitly.
fn fs_with(
    meta: Arc<Meta>,
    verify: CacheVerify,
    seed: impl FnOnce(&std::path::Path),
) -> (View, TempDir, Arc<DiskCache>) {
    let (mut fs, dir, cache) = super::quota_tests::test_fs_with(meta, CHUNK, |root| {
        seed(root);
        DiskCache::open(root, 1 << 30)
            .unwrap()
            .with_memory_cache(16 << 20)
            .with_verify(verify)
    });
    fs.caps.passthrough = true;
    (fs, dir, cache)
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

    fn open_ro(&self, ino: Ino) -> Opened {
        self.open(ino, OpenFlags::READ)
    }

    fn release(&self, ino: Ino) {
        Blocking::run(|r| {
            self.fs.release(
                &self.cx(OpKind::Release),
                ino,
                Fh(ino),
                OpenFlags::READ,
                None,
                r,
            )
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
        "no frontend declares passthrough before plan 38 Z3b"
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
    assert!(status.unavailable_reason.unwrap().contains("frontend"));
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
        e.release(ino);
    }
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
    assert!(status
        .unavailable_reason
        .unwrap()
        .contains("--cache-verify always"));
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
