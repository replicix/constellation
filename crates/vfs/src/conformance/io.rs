//! `io`: write and read, sparse files, truncate, `setattr`, `fsync`
//! levels, `fallocate` and `SEEK_DATA`/`SEEK_HOLE` (gated on their
//! capabilities), `copy_file_range`, `statfs`.
//!
//! `OpenFlags::APPEND` on `write` is not tested: a kernel frontend computes
//! the append offset itself and sends it (the engine ignores the flag), so
//! the contract has nothing to check.

use super::{must, refused, Env, Rng, TestResult};
use crate::caps::XattrSupport;
use crate::types::{
    CopyFlags, Durability, FallocateMode, Fh, OpenFlags, SeekWhence, SetAttr, SetXattrFlags,
    TimeSet,
};
use constellation_types::Code;

pub(super) fn write_read_roundtrip(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let (e, o) = must("create", c.create(root, "f"));
    let ino = e.attr.ino;
    assert_eq!(must("write", c.write(ino, o.fh, 0, b"hello")), 5);
    assert_eq!(must("write", c.write(ino, o.fh, 5, b" world")), 6);
    assert_eq!(must("read all", c.read(ino, o.fh, 0, 64)), b"hello world");
    assert_eq!(must("read middle", c.read(ino, o.fh, 6, 3)), b"wor");
    assert_eq!(must("read short", c.read(ino, o.fh, 8, 100)), b"rld");
    must("close", c.close(ino, o.fh));
    assert_eq!(must("slurp", c.slurp(ino)), b"hello world");
    Ok(())
}

pub(super) fn read_past_the_end(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"0123456789"));
    let o = must("open", c.open(f.attr.ino, OpenFlags::READ));
    assert_eq!(must("read at end", c.read(f.attr.ino, o.fh, 10, 8)), b"");
    assert_eq!(
        must("read past end", c.read(f.attr.ino, o.fh, 1 << 20, 8)),
        b""
    );
    assert_eq!(
        must("read a zero-length range", c.read(f.attr.ino, o.fh, 0, 0)),
        b""
    );
    assert_eq!(
        must("read across the end", c.read(f.attr.ino, o.fh, 8, 8)),
        b"89"
    );
    must("release", c.release(f.attr.ino, o.fh));
    Ok(())
}

pub(super) fn pending_size_is_visible_before_close(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let (e, o) = must("create", c.create(root, "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, &[7u8; 1000]));
    // Not closed: the size a stat and a lookup report already includes the
    // write (the pending-write size overlay, beneath the trait for every
    // frontend).
    assert_eq!(must("getattr", c.getattr(ino)).size, 1000);
    assert_eq!(must("lookup", c.lookup(root, "f")).attr.size, 1000);
    assert_eq!(
        must("getattr by handle", c.getattr_fh(ino, Some(o.fh))).size,
        1000
    );
    must("close", c.close(ino, o.fh));
    assert_eq!(must("getattr", c.getattr(ino)).size, 1000);
    Ok(())
}

pub(super) fn overwrite_and_extend(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, b"aaaaaaaaaa"));
    must("overwrite the middle", c.write(ino, o.fh, 3, b"XYZ"));
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"aaaXYZaaaa");
    must("extend past the end", c.write(ino, o.fh, 8, b"1234"));
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"aaaXYZaa1234");
    assert_eq!(must("getattr", c.getattr(ino)).size, 12);
    must("close", c.close(ino, o.fh));
    assert_eq!(must("slurp", c.slurp(ino)), b"aaaXYZaa1234");
    Ok(())
}

pub(super) fn sparse_files_read_zeros(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "sparse"));
    let ino = e.attr.ino;
    let far = 3 * (1 << 20) + 17;
    must("write far out", c.write(ino, o.fh, far, b"tail"));
    assert_eq!(must("getattr", c.getattr(ino)).size, far + 4);
    // The hole reads as zeros, in one read and across the boundary.
    assert_eq!(
        must("read the hole", c.read(ino, o.fh, 1 << 20, 4096)),
        vec![0u8; 4096]
    );
    let across = must("read across", c.read(ino, o.fh, far - 3, 16));
    assert_eq!(across, b"\0\0\0tail");
    must("write at the start", c.write(ino, o.fh, 0, b"head"));
    assert_eq!(
        must("read the start", c.read(ino, o.fh, 0, 8)),
        b"head\0\0\0\0"
    );
    must("close", c.close(ino, o.fh));
    let all = must("slurp", c.slurp(ino));
    assert_eq!(all.len() as u64, far + 4);
    assert_eq!(&all[..4], b"head");
    assert!(all[4..far as usize].iter().all(|b| *b == 0));
    assert_eq!(&all[far as usize..], b"tail");
    Ok(())
}

pub(super) fn truncate_shrinks_and_growth_reads_zeros(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, b"abcdefghij"));
    assert_eq!(must("shrink", c.truncate(ino, Some(o.fh), 4)).size, 4);
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"abcd");
    // Growing again must not bring the old bytes back.
    assert_eq!(must("grow", c.truncate(ino, Some(o.fh), 10)).size, 10);
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"abcd\0\0\0\0\0\0");
    must("close", c.close(ino, o.fh));
    // The same through no handle, after close, and down to zero.
    assert_eq!(must("truncate to 0", c.truncate(ino, None, 0)).size, 0);
    assert_eq!(must("slurp", c.slurp(ino)), b"");
    assert_eq!(must("grow from empty", c.truncate(ino, None, 5)).size, 5);
    assert_eq!(must("slurp", c.slurp(ino)), vec![0u8; 5]);
    let d = must("mkdir", c.mkdir(c.root(), "d")).attr.ino;
    refused(
        "truncate of a directory",
        c.truncate(d, None, 0),
        Code::IsDir,
    );
    Ok(())
}

pub(super) fn setattr_mode_owner_and_times(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"data"));
    let ino = f.attr.ino;
    let attr = must(
        "setattr",
        c.setattr(
            ino,
            None,
            &SetAttr {
                mode: Some(0o600),
                uid: Some(42),
                gid: Some(43),
                atime: Some(TimeSet::At(5_000_000_000)),
                mtime: Some(TimeSet::At(7_000_000_000)),
                ..SetAttr::default()
            },
        ),
    );
    assert_eq!(attr.mode & 0o7777, 0o600);
    assert_eq!((attr.uid, attr.gid), (42, 43));
    assert_eq!(
        (attr.atime_ns, attr.mtime_ns),
        (5_000_000_000, 7_000_000_000)
    );
    assert_eq!(attr.size, 4, "attributes not asked for are left alone");
    let again = must("getattr", c.getattr(ino));
    assert_eq!(again.mode & 0o7777, 0o600);
    assert_eq!(
        (again.uid, again.gid, again.mtime_ns),
        (42, 43, 7_000_000_000)
    );
    // `TimeSet::Now` is the engine's clock: never before what was set.
    let now = must(
        "setattr now",
        c.setattr(
            ino,
            None,
            &SetAttr {
                mtime: Some(TimeSet::Now),
                ..SetAttr::default()
            },
        ),
    );
    assert!(
        now.mtime_ns > 7_000_000_000,
        "mtime moved to now, got {}",
        now.mtime_ns
    );
    // A write moves mtime forward.
    let before = now.mtime_ns;
    let o = must("open", c.open_rw(ino));
    must("write", c.write(ino, o.fh, 0, b"x"));
    must("close", c.close(ino, o.fh));
    assert!(must("getattr", c.getattr(ino)).mtime_ns >= before);
    Ok(())
}

/// A close changes neither ctime nor mtime: they are the last change's,
/// a write or any attribute change made before the close (xcheckfs: after
/// `write; fchmod; close` ctime moved to the time the close's commit ran,
/// ext4 leaves it at the `fchmod`). Every step is a change, so ctime
/// rises with each; an mtime a `futimens` set — even one in the future
/// (`tar x`, `cp -p`, `touch -d`) — is no change time. An attribute
/// change alone opens no write session, so it has nothing to show here.
pub(super) fn close_keeps_ctime_and_mtime(env: &Env<'_>) -> TestResult {
    const OLD: i64 = 1_000_000_000_000_000;
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Step {
        Write,
        Chmod,
        Chown,
        UtimensOld,
        UtimensFuture,
        UtimensNow,
        Setxattr,
        Removexattr,
    }
    use Step::*;
    let fx = env.fresh();
    let c = fx.client();
    let mut failures = Vec::new();
    for (i, steps) in [
        &[Write][..],
        &[Write, Chmod],
        &[Write, Chown],
        &[Write, UtimensOld],
        &[Write, UtimensFuture],
        &[Write, UtimensNow],
        &[Write, UtimensOld, Write],
        &[Write, Setxattr],
        &[Setxattr, Write],
        &[Setxattr, Write, Removexattr],
        &[Chmod, Write],
    ]
    .into_iter()
    .enumerate()
    {
        let xattr_step = steps.contains(&Setxattr) || steps.contains(&Removexattr);
        if xattr_step && fx.caps.xattrs != XattrSupport::Native {
            continue;
        }
        let (e, o) = must("create", c.create_excl(c.root(), &format!("f{i}")));
        let ino = e.attr.ino;
        let setattr = |set: SetAttr| must("setattr", c.setattr(ino, Some(o.fh), &set));
        let times = |at: Option<TimeSet>| {
            setattr(SetAttr {
                atime: at,
                mtime: at,
                ..SetAttr::default()
            })
        };
        let mut ctime = e.attr.ctime_ns;
        for step in steps {
            match step {
                Write => {
                    must("write", c.write(ino, o.fh, 0, b"data"));
                }
                Chmod => {
                    setattr(SetAttr {
                        mode: Some(0o755),
                        ..SetAttr::default()
                    });
                }
                Chown => {
                    setattr(SetAttr {
                        uid: Some(c.uid()),
                        gid: Some(c.gid()),
                        ..SetAttr::default()
                    });
                }
                UtimensOld => {
                    times(Some(TimeSet::At(OLD)));
                }
                UtimensFuture => {
                    // An hour past the engine's clock and the reference's.
                    let now = must("getattr", c.getattr(ino)).ctime_ns;
                    times(Some(TimeSet::At(now + 3_600_000_000_000)));
                }
                UtimensNow => {
                    times(Some(TimeSet::Now));
                }
                Setxattr => must(
                    "setxattr",
                    c.setxattr(ino, "user.k", b"v", SetXattrFlags::empty()),
                ),
                Removexattr => must("removexattr", c.removexattr(ino, "user.k")),
            }
            let now = must("getattr", c.getattr(ino)).ctime_ns;
            if now <= ctime {
                failures.push(format!(
                    "{steps:?}: {step:?} left ctime at {now} (was {ctime})"
                ));
            }
            ctime = now;
        }
        let open = must("getattr", c.getattr(ino));
        must("close", c.close(ino, o.fh));
        let closed = must("getattr after close", c.getattr(ino));
        let seen = |a: &crate::types::Attr| (a.ctime_ns, a.mtime_ns, a.size);
        if seen(&closed) != seen(&open) {
            failures.push(format!(
                "{steps:?}: the close changed (ctime, mtime, size) from {:?} to {:?}",
                seen(&open),
                seen(&closed)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}

pub(super) fn two_handles_see_each_others_writes(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let (e, a) = must("create", c.create(root, "f"));
    let ino = e.attr.ino;
    let b = must("open second", c.open_rw(ino));
    must("write via a", c.write(ino, a.fh, 0, b"from a"));
    assert_eq!(must("read via b", c.read(ino, b.fh, 0, 64)), b"from a");
    must("write via b", c.write(ino, b.fh, 5, b"B!"));
    assert_eq!(must("read via a", c.read(ino, a.fh, 0, 64)), b"from B!");
    assert_eq!(must("getattr", c.getattr(ino)).size, 7);
    must("close a", c.close(ino, a.fh));
    // The other handle is unaffected by the first one's close.
    assert_eq!(must("read via b", c.read(ino, b.fh, 0, 64)), b"from B!");
    must("close b", c.close(ino, b.fh));
    assert_eq!(must("slurp", c.slurp(ino)), b"from B!");
    Ok(())
}

pub(super) fn data_survives_close_and_reopen(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let mut rng = env.rng();
    let files: Vec<(String, Vec<u8>)> = (0..5)
        .map(|i| {
            let len = rng.range(0, 40_000) as usize;
            (format!("f{i}"), rng.bytes(len))
        })
        .collect();
    for (name, data) in &files {
        must("put", c.put(root, name, data));
    }
    must("sync_view", c.sync_view());
    for (name, data) in &files {
        let e = must("lookup", c.lookup(root, name));
        assert_eq!(e.attr.size, data.len() as u64, "{name}");
        assert_eq!(&must("slurp", c.slurp(e.attr.ino)), data, "{name}");
    }
    Ok(())
}

pub(super) fn large_multi_chunk_file(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let mut rng = env.rng();
    let total = 3 * (1 << 20) + 12_345;
    let data = rng.bytes(total);
    let (e, o) = must("create", c.create(c.root(), "big"));
    let ino = e.attr.ino;
    // Written in irregular pieces, read back in others.
    let mut off = 0;
    while off < total {
        let n = (rng.range(1, 300_000) as usize).min(total - off);
        assert_eq!(
            must("write", c.write(ino, o.fh, off as u64, &data[off..off + n])) as usize,
            n
        );
        off += n;
    }
    must("close", c.close(ino, o.fh));
    assert_eq!(must("getattr", c.getattr(ino)).size, total as u64);
    let o = must("open", c.open(ino, OpenFlags::READ));
    let mut got = Vec::with_capacity(total);
    while got.len() < total {
        let want = rng.range(1, 200_000) as u32;
        let chunk = must("read", c.read(ino, o.fh, got.len() as u64, want));
        assert!(
            !chunk.is_empty(),
            "a read inside the file returned nothing at {}",
            got.len()
        );
        got.extend_from_slice(&chunk);
    }
    must("release", c.release(ino, o.fh));
    assert!(
        got == data,
        "the content read back differs from what was written"
    );
    Ok(())
}

pub(super) fn fsync_levels(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, b"durable?"));
    must(
        "fsync configured",
        c.fsync(ino, o.fh, Durability::Configured),
    );
    must("fsync local", c.fsync(ino, o.fh, Durability::Local));
    // A target without a shared log may refuse the strongest level; it
    // must not lose the data either way.
    let durable = c.fsync(ino, o.fh, Durability::Durable);
    if let Err(e) = &durable {
        assert!(
            matches!(e.code(), Code::NotSupported | Code::NotImplemented),
            "fsync(durable) failed with {:?}",
            e.code()
        );
    }
    assert_eq!(must("read", c.read(ino, o.fh, 0, 64)), b"durable?");
    must("close", c.close(ino, o.fh));
    must("sync_view", c.sync_view());
    assert_eq!(must("slurp", c.slurp(ino)), b"durable?");
    Ok(())
}

pub(super) fn fallocate_modes(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    let ino = e.attr.ino;
    must("write", c.write(ino, o.fh, 0, &[1u8; 16384]));
    // Plain allocation extends the size, KEEP_SIZE does not.
    must(
        "fallocate extend",
        c.fallocate(ino, o.fh, 0, 32768, FallocateMode::empty()),
    );
    assert_eq!(must("getattr", c.getattr(ino)).size, 32768);
    must(
        "fallocate keep size",
        c.fallocate(ino, o.fh, 0, 65536, FallocateMode::KEEP_SIZE),
    );
    assert_eq!(must("getattr", c.getattr(ino)).size, 32768);
    // Allocated-but-unwritten space reads as zeros.
    assert_eq!(must("read", c.read(ino, o.fh, 20000, 8)), vec![0u8; 8]);
    assert_eq!(
        must("read", c.read(ino, o.fh, 0, 4)),
        vec![1u8; 4],
        "existing data is kept"
    );
    // Punching a hole zeroes the range and keeps the size.
    must(
        "punch",
        c.fallocate(
            ino,
            o.fh,
            4096,
            4096,
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
        ),
    );
    assert_eq!(
        must("read punched", c.read(ino, o.fh, 4096, 4096)),
        vec![0u8; 4096]
    );
    assert_eq!(
        must("read before", c.read(ino, o.fh, 4090, 6)),
        vec![1u8; 6]
    );
    assert_eq!(must("read after", c.read(ino, o.fh, 8192, 4)), vec![1u8; 4]);
    assert_eq!(must("getattr", c.getattr(ino)).size, 32768);
    // The refusals.
    refused(
        "fallocate of length zero",
        c.fallocate(ino, o.fh, 0, 0, FallocateMode::empty()),
        Code::Invalid,
    );
    refused(
        "a punch that would change the size",
        c.fallocate(ino, o.fh, 0, 4096, FallocateMode::PUNCH_HOLE),
        Code::NotSupported,
    );
    refused(
        "a mode the contract does not name",
        c.fallocate(ino, o.fh, 0, 4096, FallocateMode::UNSUPPORTED),
        Code::NotSupported,
    );
    must("close", c.close(ino, o.fh));
    Ok(())
}

/// Every successful `fallocate` is a modification (ext4, xfs, btrfs and
/// tmpfs stamp mtime and ctime for each): also the ones that change no
/// byte and no size — an allocation inside the file, any `KEEP_SIZE`
/// allocation, a zeroing or a punch past the end. The time is the
/// file's live, and the one its close publishes.
pub(super) fn fallocate_stamps_mtime_and_ctime(env: &Env<'_>) -> TestResult {
    const OLD: i64 = 1_000_000_000_000_000;
    let fx = env.fresh();
    let c = fx.client();
    let ino = must("put", c.put(c.root(), "f", &[1u8; 65536])).attr.ino;
    for (what, off, len, mode) in [
        (
            "an allocation inside the file",
            0,
            4096,
            FallocateMode::empty(),
        ),
        (
            "an allocation that extends it",
            65536,
            4096,
            FallocateMode::empty(),
        ),
        (
            "KEEP_SIZE inside the file",
            0,
            4096,
            FallocateMode::KEEP_SIZE,
        ),
        (
            "KEEP_SIZE past the end",
            131072,
            65536,
            FallocateMode::KEEP_SIZE,
        ),
        (
            "a punch inside the file",
            0,
            4096,
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
        ),
        (
            "a punch past the end",
            262144,
            4096,
            FallocateMode::PUNCH_HOLE | FallocateMode::KEEP_SIZE,
        ),
        (
            "a zeroing inside the file",
            4096,
            4096,
            FallocateMode::ZERO_RANGE,
        ),
        (
            "a KEEP_SIZE zeroing past the end",
            262144,
            4096,
            FallocateMode::ZERO_RANGE | FallocateMode::KEEP_SIZE,
        ),
    ] {
        let set = must(
            "setattr",
            c.setattr(
                ino,
                None,
                &SetAttr {
                    mtime: Some(TimeSet::At(OLD)),
                    ..SetAttr::default()
                },
            ),
        );
        assert_eq!(set.mtime_ns, OLD);
        let o = must("open", c.open_rw(ino));
        must(what, c.fallocate(ino, o.fh, off, len, mode));
        let live = must("getattr", c.getattr(ino));
        assert!(
            live.mtime_ns > OLD,
            "{what} left mtime at {}",
            live.mtime_ns
        );
        assert!(
            live.ctime_ns > set.ctime_ns && live.ctime_ns >= live.mtime_ns,
            "{what}: ctime {} (was {}, mtime {})",
            live.ctime_ns,
            set.ctime_ns,
            live.mtime_ns
        );
        must("close", c.close(ino, o.fh));
        assert_eq!(
            must("getattr after close", c.getattr(ino)).mtime_ns,
            live.mtime_ns,
            "{what}: the close publishes the allocation's mtime"
        );
    }
    Ok(())
}

pub(super) fn fallocate_absent_is_not_supported(env: &Env<'_>) -> TestResult {
    let mut caps = env.caps().clone();
    caps.fallocate = false;
    let fx = env.fresh_with(&caps);
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    // Absence is deliberate and capability-gated, not silently missing.
    refused(
        "fallocate without the capability",
        c.fallocate(e.attr.ino, o.fh, 0, 4096, FallocateMode::empty()),
        Code::NotSupported,
    );
    must("close", c.close(e.attr.ino, o.fh));
    Ok(())
}

pub(super) fn seek_data_and_hole(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    let ino = e.attr.ino;
    let far = 8 * (1 << 20);
    must("write head", c.write(ino, o.fh, 0, b"head"));
    must("write tail", c.write(ino, o.fh, far, b"tail"));
    let size = far + 4;
    assert_eq!(must("getattr", c.getattr(ino)).size, size);
    assert_eq!(
        must("data from 0", c.seek(ino, o.fh, 0, SeekWhence::Data)),
        0
    );
    // Storage granularity is the target's: the hole starts at the end of
    // the first allocation unit (at least the end of the data) and the
    // next data at or before the tail.
    let hole = must("hole from 0", c.seek(ino, o.fh, 0, SeekWhence::Hole));
    assert!((4..=far).contains(&hole), "hole at {hole}");
    let next = must(
        "data after the hole",
        c.seek(ino, o.fh, hole, SeekWhence::Data),
    );
    assert!(
        next >= hole && next <= far,
        "data at {next}, hole at {hole}"
    );
    assert_eq!(
        must(
            "data from the tail",
            c.seek(ino, o.fh, far, SeekWhence::Data)
        ),
        far
    );
    // The end of the file is an implicit hole.
    let end = must(
        "hole after the tail",
        c.seek(ino, o.fh, far, SeekWhence::Hole),
    );
    assert!((far + 4..=size).contains(&end), "hole at {end}");
    // Past the end: ENXIO. The frontend answers Set/Cur/End itself.
    refused(
        "data past the end",
        c.seek(ino, o.fh, size + 100, SeekWhence::Data),
        Code::NoDeviceOrAddress,
    );
    refused(
        "hole past the end",
        c.seek(ino, o.fh, size + 100, SeekWhence::Hole),
        Code::NoDeviceOrAddress,
    );
    refused(
        "SEEK_SET is the frontend's",
        c.seek(ino, o.fh, 0, SeekWhence::Set),
        Code::Invalid,
    );
    must("close", c.close(ino, o.fh));
    Ok(())
}

pub(super) fn seek_absent_is_not_supported(env: &Env<'_>) -> TestResult {
    let mut caps = env.caps().clone();
    caps.seek_hole = false;
    let fx = env.fresh_with(&caps);
    let c = fx.client();
    let (e, o) = must("create", c.create(c.root(), "f"));
    must("write", c.write(e.attr.ino, o.fh, 0, b"data"));
    refused(
        "SEEK_DATA without the capability",
        c.seek(e.attr.ino, o.fh, 0, SeekWhence::Data),
        Code::NotSupported,
    );
    must("close", c.close(e.attr.ino, o.fh));
    Ok(())
}

pub(super) fn statfs_is_sane(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let st = must("statfs", c.statfs(root));
    assert!(st.bsize > 0 && st.frsize > 0, "block sizes");
    assert_eq!(st.namelen, 255, "NAME_MAX");
    assert!(st.blocks >= st.bfree, "{st:?}");
    let before = st.files;
    let mut rng = Rng::new(env.seed());
    for i in 0..3 {
        let len = rng.range(1, 100) as usize;
        must("put", c.put(root, &format!("f{i}"), &rng.bytes(len)));
    }
    let after = must("statfs", c.statfs(root));
    assert!(
        after.files >= before,
        "inode count did not shrink: {before} -> {}",
        after.files
    );
    Ok(())
}

/// `copy_file_range`: the bytes land where a write of them would, the
/// destination grows (a gap before them reads as zeros), the copy stops
/// at the source's end, and a copy from at or past it copies nothing.
pub(super) fn copy_file_range_copies_and_extends(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let src = must("put src", c.put(root, "src", b"0123456789abcdef"))
        .attr
        .ino;
    let si = must("open src", c.open(src, OpenFlags::READ));
    let (e, d) = must("create dst", c.create(root, "dst"));
    let dst = e.attr.ino;
    assert_eq!(
        must("copy", c.copy_range(src, si.fh, 2, dst, d.fh, 0, 4)),
        4
    );
    assert_eq!(must("read", c.read(dst, d.fh, 0, 64)), b"2345");
    // Past the destination's end: it grows, the gap reads as zeros.
    assert_eq!(
        must(
            "copy past the end",
            c.copy_range(src, si.fh, 0, dst, d.fh, 10, 3)
        ),
        3
    );
    assert_eq!(must("getattr", c.getattr(dst)).size, 13);
    let gap = [0u8; 6];
    assert_eq!(
        must("read", c.read(dst, d.fh, 0, 64)),
        [&b"2345"[..], &gap, b"012"].concat()
    );
    // Over existing bytes, in the middle.
    assert_eq!(
        must("overwrite", c.copy_range(src, si.fh, 10, dst, d.fh, 1, 2)),
        2
    );
    assert_eq!(
        must("read", c.read(dst, d.fh, 0, 64)),
        [&b"2ab5"[..], &gap, b"012"].concat()
    );
    // Across the source's end: up to it.
    assert_eq!(
        must(
            "copy across the end",
            c.copy_range(src, si.fh, 12, dst, d.fh, 13, 100)
        ),
        4
    );
    assert_eq!(
        must("read", c.read(dst, d.fh, 0, 64)),
        [&b"2ab5"[..], &gap, b"012cdef"].concat()
    );
    // At and past the source's end, and of nothing: 0, no change.
    let before = must("getattr", c.getattr(dst));
    for (what, off, len) in [
        ("copy at the end", 16, 8),
        ("copy past the end", 1 << 20, 8),
        ("copy of nothing", 0, 0),
    ] {
        assert_eq!(
            must(what, c.copy_range(src, si.fh, off, dst, d.fh, 0, len)),
            0,
            "{what}"
        );
    }
    let after = must("getattr", c.getattr(dst));
    assert_eq!(
        (after.size, after.mtime_ns),
        (before.size, before.mtime_ns),
        "a copy of no bytes changes nothing"
    );
    // A length past what one call copies is a short copy, not an error.
    assert_eq!(
        must(
            "copy of u64::MAX bytes",
            c.copy_range(src, si.fh, 0, dst, d.fh, 0, u64::MAX)
        ),
        16
    );
    must("close dst", c.close(dst, d.fh));
    must("release src", c.release(src, si.fh));
    // The last byte is the earlier copy's `f`, past this one.
    assert_eq!(must("slurp", c.slurp(dst)), b"0123456789abcdeff");
    Ok(())
}

/// The source's bytes are the view's: its pending (unclosed) writes, as a
/// read of it would answer, and its holes as zeros — written over what
/// the destination held there.
pub(super) fn copy_file_range_reads_pending_writes_and_holes(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let src = must("put src", c.put(root, "src", &[b'o'; 8192])).attr.ino;
    let w = must("open src", c.open_rw(src));
    must("rewrite src", c.write(src, w.fh, 100, b"NEW BYTES"));
    // A hole: the file grows by a write far out.
    let far = 2 * (1 << 20) + 5;
    must("write far out", c.write(src, w.fh, far, b"tail"));
    let dst = must("put dst", c.put(root, "dst", &[b'x'; 4096])).attr.ino;
    let d = must("open dst", c.open_rw(dst));
    assert_eq!(
        must("copy", c.copy_range(src, w.fh, 96, dst, d.fh, 0, 16)),
        16
    );
    assert_eq!(
        must("read", c.read(dst, d.fh, 0, 20)),
        b"ooooNEW BYTESoooxxxx"
    );
    // The hole, then the tail: zeros where the destination had `x`.
    let n = must(
        "copy the hole",
        c.copy_range(src, w.fh, far - 1000, dst, d.fh, 0, 2000),
    );
    assert_eq!(n, 1004);
    let got = must("read", c.read(dst, d.fh, 0, 2000));
    assert_eq!(got.len(), 2000);
    assert!(
        got[..1000].iter().all(|b| *b == 0),
        "the hole reads as zeros"
    );
    assert_eq!(&got[1000..1004], b"tail");
    assert!(
        got[1004..].iter().all(|b| *b == b'x'),
        "past the copy: kept"
    );
    must("close dst", c.close(dst, d.fh));
    must("close src", c.close(src, w.fh));
    let all = must("slurp", c.slurp(dst));
    assert_eq!(&all[1000..1004], b"tail");
    Ok(())
}

/// A copy within one file, of ranges that do not overlap, is a copy; of
/// ranges that do, the VFS's `EINVAL` (counted to the source's end).
pub(super) fn copy_file_range_within_one_file(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let f = must("put f", c.put(c.root(), "f", b"abcdefgh")).attr.ino;
    let o = must("open", c.open_rw(f));
    assert_eq!(
        must("copy forward", c.copy_range(f, o.fh, 0, f, o.fh, 8, 8)),
        8
    );
    assert_eq!(must("read", c.read(f, o.fh, 0, 64)), b"abcdefghabcdefgh");
    assert_eq!(
        must("copy back", c.copy_range(f, o.fh, 12, f, o.fh, 0, 4)),
        4
    );
    assert_eq!(must("read", c.read(f, o.fh, 0, 64)), b"efghefghabcdefgh");
    for (what, off_in, off_out, len) in [
        ("overlapping forward", 0, 4, 8),
        ("overlapping backward", 4, 0, 8),
        ("the same range", 2, 2, 1),
    ] {
        refused(
            what,
            c.copy_range(f, o.fh, off_in, f, o.fh, off_out, len),
            Code::Invalid,
        );
    }
    // Clamped to the end first, as the VFS counts: `[14, 16)` to 16 does
    // not overlap.
    assert_eq!(
        must(
            "copy across the end",
            c.copy_range(f, o.fh, 14, f, o.fh, 16, 8)
        ),
        2
    );
    assert_eq!(must("getattr", c.getattr(f)).size, 18);
    must("close", c.close(f, o.fh));
    assert_eq!(must("slurp", c.slurp(f)), b"efghefghabcdefghgh");
    Ok(())
}

/// What the op refuses itself (the rest is the kernel's: the module doc of
/// `Vfs::copy_file_range`).
pub(super) fn copy_file_range_refusals(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let src = must("put src", c.put(root, "src", b"source")).attr.ino;
    let dst = must("put dst", c.put(root, "dst", b"dest")).attr.ino;
    let si = must("open src", c.open(src, OpenFlags::READ));
    let dr = must("open dst read-only", c.open(dst, OpenFlags::READ));
    let dw = must("open dst", c.open_rw(dst));
    refused(
        "a flag",
        c.copy_range_flags(src, si.fh, 0, dst, dw.fh, 0, 4, CopyFlags::UNSUPPORTED),
        Code::Invalid,
    );
    refused(
        "into a read-only handle",
        c.copy_range(src, si.fh, 0, dst, dr.fh, 0, 4),
        Code::BadFd,
    );
    refused(
        "with the destination's handle as the source's",
        c.copy_range(src, dw.fh, 0, dst, dw.fh, 0, 4),
        Code::BadFd,
    );
    refused(
        "with the source's handle as the destination's",
        c.copy_range(src, si.fh, 0, dst, si.fh, 0, 4),
        Code::BadFd,
    );
    refused(
        "with a handle never given out",
        c.copy_range(src, Fh(0xdead_beef), 0, dst, dw.fh, 0, 4),
        Code::BadFd,
    );
    // Nothing was written by any of them.
    must("close dst", c.close(dst, dw.fh));
    must("release dst", c.release(dst, dr.fh));
    must("release src", c.release(src, si.fh));
    assert_eq!(must("slurp", c.slurp(dst)), b"dest");
    let d = must("getattr", c.getattr(dst));
    assert_eq!(d.size, 4);
    Ok(())
}

/// A copy is a data change of the destination, as a write is: mtime and
/// ctime move, and its close publishes them; the source's do not.
pub(super) fn copy_file_range_stamps_the_destination(env: &Env<'_>) -> TestResult {
    const OLD: i64 = 1_000_000_000_000_000;
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let src = must("put src", c.put(root, "src", b"some bytes")).attr.ino;
    let dst = must("put dst", c.put(root, "dst", b"")).attr.ino;
    let old = SetAttr {
        mtime: Some(TimeSet::At(OLD)),
        ..SetAttr::default()
    };
    let src_set = must("setattr src", c.setattr(src, None, &old));
    let dst_set = must("setattr dst", c.setattr(dst, None, &old));
    let si = must("open src", c.open(src, OpenFlags::READ));
    let d = must("open dst", c.open_rw(dst));
    assert_eq!(
        must("copy", c.copy_range(src, si.fh, 0, dst, d.fh, 0, 64)),
        10
    );
    let live = must("getattr dst", c.getattr(dst));
    assert_eq!(live.size, 10);
    assert!(
        live.mtime_ns > OLD,
        "the copy left mtime at {}",
        live.mtime_ns
    );
    assert!(
        live.ctime_ns > dst_set.ctime_ns,
        "ctime {} (was {})",
        live.ctime_ns,
        dst_set.ctime_ns
    );
    must("close dst", c.close(dst, d.fh));
    must("release src", c.release(src, si.fh));
    let closed = must("getattr dst after close", c.getattr(dst));
    assert_eq!(
        (closed.mtime_ns, closed.size),
        (live.mtime_ns, 10),
        "the close publishes the copy's mtime"
    );
    let s = must("getattr src", c.getattr(src));
    assert_eq!(
        (s.mtime_ns, s.ctime_ns, s.size),
        (OLD, src_set.ctime_ns, 10),
        "the source is only read"
    );
    Ok(())
}

/// A copy spanning chunks, at offsets aligned to nothing, of a file that
/// is read cold or warm, and a destination that already holds data.
pub(super) fn copy_file_range_large_multi_chunk(env: &Env<'_>) -> TestResult {
    let fx = env.fresh();
    let c = fx.client();
    let root = c.root();
    let mut rng = env.rng();
    let total = 3 * (1 << 20) + 12_345;
    let data = rng.bytes(total);
    let src = must("put src", c.put(root, "src", &data)).attr.ino;
    let base = rng.bytes(1 << 20);
    let dst = must("put dst", c.put(root, "dst", &base)).attr.ino;
    let si = must("open src", c.open(src, OpenFlags::READ));
    let d = must("open dst", c.open_rw(dst));
    let (off_in, off_out) = (777u64, 333_333u64);
    let mut copied = 0u64;
    let want = total as u64 - off_in;
    while copied < want {
        let n = must(
            "copy",
            c.copy_range(
                src,
                si.fh,
                off_in + copied,
                dst,
                d.fh,
                off_out + copied,
                want - copied,
            ),
        );
        assert!(n > 0, "a copy inside the source copied nothing at {copied}");
        copied += n;
    }
    must("close dst", c.close(dst, d.fh));
    must("release src", c.release(src, si.fh));
    let mut expect = base.clone();
    expect.truncate(off_out as usize);
    expect.extend_from_slice(&data[off_in as usize..]);
    let got = must("slurp", c.slurp(dst));
    assert_eq!(got.len(), expect.len());
    assert!(got == expect, "the copy's content differs");
    Ok(())
}
