//! Plan 38 §3(d)/§6, Z4b: zero-copy reads on a real 7.3 kernel — a read
//! inside one chunk answered by one `READ_FIXED` from the chunk file
//! (buffered and `O_DIRECT`), a read crossing a chunk boundary answered
//! the ordinary way, a chunk under zero-copy reads surviving a prune, and
//! `--cache-verify always` turning zero-copy off.
//!
//! Every scenario `requires` [`suites::FUSE_URING_ZERO_COPY`] (the ring
//! here, `CAP_SYS_ADMIN`, Linux >= 7.3), so an unprivileged run or an older
//! kernel SKIPs them loudly, naming which. Where all are present the mount
//! must actually *get* zero-copy queues: a session that reports another
//! transport is a failure with the reason it logged, not a skip (the same
//! rule as the passthrough scenarios: a fast path that silently never
//! engages must be observable).
//!
//! The mounts ask for the ring explicitly (`CONSTELLATION_FUSE_TRANSPORT=uring`,
//! `--locks local`: under `auto` a mount with cluster locks stays on
//! `/dev/fuse`, plan 38 Z2c) and for zero-copy queues
//! (`CONSTELLATION_FUSE_URING_ZERO_COPY=auto`), whatever the harness's own
//! environment says. What a zero-copy read is, from outside:
//! `fuse.mounts[].zero_copy_reads` counts it, and the daemon's `read`
//! series counts every read it answered either way.
//!
//! The filesystem's chunk size is the harness's 1 MiB (`Client::fs_create`),
//! which is what the chunk-boundary case is built from.
//!
//! Which reads the engine answers zero-copy depends on the threshold,
//! which the scenarios pin rather than inherit (plan 38 Z4b):
//! `CONSTELLATION_FUSE_ZERO_COPY_MIN_READ`, [`MIN_READ`] here, so reads of
//! a few pages qualify and one page does not. The memory tier does not
//! matter: a chunk it holds is read zero-copy as well.

use super::passthrough::{
    bytes, cache_entries, cached, daemon_reads, memcache, prune_all, with_clients, Aligned,
};
use super::{eventually, setup, ts};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{bail, ensure, Context, Result};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The harness filesystem's chunk size.
const CHUNK: u64 = 1 << 20;

/// The zero-copy threshold the scenarios run with (module doc): two pages.
const MIN_READ: usize = 8192;

fn client(env: &S3Env, root: &Path, name: &str, backend: &str) -> Result<Client> {
    Ok(Client::new(root, name, &env.endpoint, backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "uring")
        .with_env("CONSTELLATION_FUSE_URING_ZERO_COPY", "auto")
        .with_env(
            "CONSTELLATION_FUSE_ZERO_COPY_MIN_READ",
            &MIN_READ.to_string(),
        ))
}

/// Mount with the ring and no cluster locks, and require the session to
/// have zero-copy queues.
fn mount_zc(c: &mut Client) -> Result<()> {
    c.mount_view(None, &["--locks", "local"])?;
    let transport = super::transport::negotiated_transport(c)?;
    if transport != "uring_zc" {
        let why: Vec<_> = c
            .log_text()
            .lines()
            .filter(|l| l.contains("zero-copy") || l.contains("io_uring"))
            .map(str::to_string)
            .collect();
        bail!(
            "this host has the ring, CAP_SYS_ADMIN and Linux >= 7.3, yet the mount negotiated \
             {transport}, not uring_zc:\n{}\n{}",
            why.join("\n"),
            c.tail_log()
        );
    }
    Ok(())
}

/// The one mount's `fuse.mounts[].zero_copy_reads`.
fn zc_reads(c: &Client) -> Result<u64> {
    let status = c.control_status()?;
    status["fuse"]["mounts"][0]["zero_copy_reads"]
        .as_u64()
        .with_context(|| format!("no fuse.mounts[0].zero_copy_reads: {}", status["fuse"]))
}

/// `(zero-copy reads, daemon reads)` now.
fn reads(c: &Client) -> Result<(u64, u64)> {
    Ok((zc_reads(c)?, daemon_reads(c)?))
}

/// Write `data` to `path` (a new file) and wait for every one of its chunks
/// (`data` cut at [`CHUNK`]) to be in the disk cache and clean: the write
/// session is published and gone, which is what lets an open be marked for
/// zero-copy. Returns the chunks as `(hash, size)`.
fn write_file(c: &Client, path: &Path, data: &[u8]) -> Result<Vec<(String, u64)>> {
    let before: HashSet<String> = cache_entries(c)?.into_iter().map(|e| e.0).collect();
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    drop(f);
    let chunks = data.chunks(CHUNK as usize).count();
    let mut out = Vec::new();
    eventually(
        "the file's chunks cached and clean",
        Duration::from_secs(120),
        || {
            let new: Vec<_> = cache_entries(c)?
                .into_iter()
                .filter(|e| !before.contains(&e.0))
                .collect();
            ensure!(
                new.len() == chunks,
                "{} new chunks, want {chunks}: {new:?}",
                new.len()
            );
            ensure!(new.iter().all(|e| e.2 == "clean"), "not all clean: {new:?}");
            out = new.into_iter().map(|e| (e.0, e.1)).collect();
            Ok(())
        },
    )?;
    Ok(out)
}

fn open_direct(path: &Path) -> Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?)
}

/// One `O_DIRECT` `pread` of `len` (a page multiple) at `offset`: one FUSE
/// read request, of exactly that range.
fn direct_read(f: &std::fs::File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut buf = Aligned::new(len);
    let n = f.read_at(buf.as_mut(), offset)?;
    Ok(buf.as_mut()[..n].to_vec())
}

fn read_all(path: &Path) -> Result<Vec<u8>> {
    let mut v = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut v)?;
    Ok(v)
}

/// `cache.open_pins` of the daemon.
fn open_pins(c: &Client) -> Result<u64> {
    c.control_status()?["cache"]["open_pins"]
        .as_u64()
        .context("no cache.open_pins")
}

pub fn single_chunk(seed: u64) -> Result<()> {
    let (env, root) = setup("zc-single")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/zc-single-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    mount_zc(&mut c)?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        let len = 3 * CHUNK as usize + 12_345;
        let data = bytes(seed, len);
        let path = c.mnt.join("three-chunks");
        write_file(c, &path, &data)?;

        // Buffered: every open starts with an empty page cache for the
        // file (no `FOPEN_KEEP_CACHE`), and the kernel's readahead
        // requests are aligned windows inside one chunk, so every read
        // that carries data is zero-copy.
        let (zc0, d0) = reads(c)?;
        ensure!(read_all(&path)? == data, "buffered: other bytes");
        let (zc1, d1) = reads(c)?;
        let (zc, daemon) = (zc1 - zc0, d1 - d0);
        eprintln!("    buffered: {daemon} daemon reads, {zc} zero-copy");
        ensure!(zc > 0, "a buffered read of the file was never zero-copy");
        ensure!(
            zc + 2 >= daemon,
            "buffered: only {zc} of {daemon} daemon reads were zero-copy"
        );

        // `O_DIRECT`: one request per `pread`, into the reader's own
        // pages, each counted exactly once — the first chunk, the middle
        // of the third, the short tail chunk.
        let f = open_direct(&path)?;
        for (off, n) in [
            (8192u64, 64 << 10),
            (2 * CHUNK + 4096, 256 << 10),
            (3 * CHUNK, 12_288),
        ] {
            let (zc0, d0) = reads(c)?;
            let got = direct_read(&f, off, n)?;
            ensure!(
                got == data[off as usize..off as usize + n],
                "O_DIRECT at {off}: other bytes"
            );
            let (zc1, d1) = reads(c)?;
            ensure!(
                (zc1 - zc0, d1 - d0) == (1, 1),
                "O_DIRECT at {off}: (zero-copy, daemon) reads moved by ({}, {}), want (1, 1)",
                zc1 - zc0,
                d1 - d0
            );
        }
        // Below the threshold: one daemon read, answered with bytes.
        let (zc0, d0) = reads(c)?;
        let off = CHUNK + 4096;
        ensure!(
            direct_read(&f, off, MIN_READ - 4096)?
                == data[off as usize..off as usize + MIN_READ - 4096],
            "O_DIRECT below the threshold: other bytes"
        );
        let (zc1, d1) = reads(c)?;
        ensure!(
            (zc1 - zc0, d1 - d0) == (0, 1),
            "O_DIRECT below the threshold: (zero-copy, daemon) reads moved by ({}, {}), \
             want (0, 1)",
            zc1 - zc0,
            d1 - d0
        );
        // Past EOF: nothing to read, nothing zero-copied.
        ensure!(direct_read(&f, 4 * CHUNK, 4096)?.is_empty());
        let status = c.control_status()?;
        ensure!(
            status["fuse"]["zero_copy_reads_total"].as_u64() >= Some(zc_reads(c)?),
            "the process total is below the mount's: {}",
            status["fuse"]
        );
        Ok(())
    })
}

pub fn chunk_spanning_fallback(seed: u64) -> Result<()> {
    let (env, root) = setup("zc-span")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/zc-span-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    mount_zc(&mut c)?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        let data = bytes(seed, 2 * CHUNK as usize);
        let path = c.mnt.join("two-chunks");
        write_file(c, &path, &data)?;
        let f = open_direct(&path)?;
        // One request of 8 KiB whose first page is the last of chunk 0
        // and whose second is the first of chunk 1: two slices, so the
        // memory-cache path answers it on the same zero-copy session.
        let off = CHUNK - 4096;
        let (zc0, d0) = reads(c)?;
        let got = direct_read(&f, off, 8192)?;
        ensure!(
            got == data[off as usize..off as usize + 8192],
            "the chunk-spanning read returned other bytes"
        );
        let (zc1, d1) = reads(c)?;
        ensure!(
            d1 - d0 == 1,
            "the spanning read was {} daemon reads, want 1",
            d1 - d0
        );
        ensure!(
            zc1 == zc0,
            "a read crossing a chunk boundary was counted zero-copy"
        );
        // The same handle's next read, inside chunk 1, is zero-copy: the
        // fallback was per request, not per open or per session.
        let got = direct_read(&f, CHUNK, 8192)?;
        ensure!(got == data[CHUNK as usize..CHUNK as usize + 8192]);
        ensure!(
            zc_reads(c)? == zc1 + 1,
            "the read inside one chunk after it was not zero-copy"
        );
        eprintln!("    spanning read: memory-cache path; next read: zero-copy");
        Ok(())
    })
}

pub fn eviction_while_inflight(seed: u64) -> Result<()> {
    let (env, root) = setup("zc-evict")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/zc-evict-{}", ts());
    let budget: u64 = 8 << 20;
    let mut c = client(&env, root.path(), "c0", &backend)?.with_cache_size(budget);
    c.fs_create()?;
    mount_zc(&mut c)?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        // A tail chunk of a size no other chunk has, to find it by.
        let len = CHUNK as usize + 12_345;
        let data = Arc::new(bytes(seed, len));
        let path = c.mnt.join("held");
        let tail = write_file(c, &path, &data)?
            .into_iter()
            .find(|(_, size)| *size == 12_345)
            .context("no tail chunk of 12345 bytes")?
            .0;
        ensure!(open_pins(c)? == 0, "a pin before any read");

        // A reader hammering the tail chunk with zero-copy reads while the
        // cache is overfilled and pruned to nothing, twice.
        let f = Arc::new(open_direct(&path)?);
        ensure!(direct_read(&f, CHUNK, 12_288)? == data[CHUNK as usize..CHUNK as usize + 12_288]);
        eventually("the handle's chunk pinned", Duration::from_secs(10), || {
            ensure!(open_pins(c)? == 1, "open pins {}", open_pins(c)?);
            Ok(())
        })?;
        let stop = Arc::new(AtomicBool::new(false));
        let served = Arc::new(AtomicU64::new(0));
        let zc0 = zc_reads(c)?;
        let reader = {
            let (f, data, stop, served) = (f.clone(), data.clone(), stop.clone(), served.clone());
            std::thread::spawn(move || -> Result<()> {
                while !stop.load(Ordering::Relaxed) {
                    let got = direct_read(&f, CHUNK, 12_288)?;
                    ensure!(
                        got == data[CHUNK as usize..CHUNK as usize + 12_288],
                        "an in-flight zero-copy read returned other bytes"
                    );
                    served.fetch_add(1, Ordering::Relaxed);
                    // Steady reads, not a busy loop: the point is a read in
                    // flight at every prune, not load on a shared host.
                    std::thread::sleep(Duration::from_millis(1));
                }
                Ok(())
            })
        };
        let load = (|| -> Result<()> {
            for round in 0..2u64 {
                for i in 0..(2 * budget / CHUNK) {
                    let filler = bytes(seed.wrapping_add(round * 100 + i + 1), CHUNK as usize);
                    let mut w = std::fs::File::create(c.mnt.join(format!("filler-{round}-{i}")))?;
                    w.write_all(&filler)?;
                    w.sync_all()?;
                }
                eventually("the fillers uploaded", Duration::from_secs(120), || {
                    let dirty: Vec<_> = cache_entries(c)?
                        .into_iter()
                        .filter(|e| e.2 != "clean")
                        .collect();
                    ensure!(dirty.is_empty(), "not clean yet: {dirty:?}");
                    Ok(())
                })?;
                prune_all(c)?;
                ensure!(
                    cached(c, &tail)?,
                    "round {round}: the chunk under zero-copy reads was evicted"
                );
                ensure!(open_pins(c)? == 1, "round {round}: open pins moved");
            }
            Ok(())
        })();
        stop.store(true, Ordering::Relaxed);
        let read_result = reader
            .join()
            .map_err(|_| anyhow::anyhow!("the reader panicked"))?;
        load?;
        read_result?;
        let n = served.load(Ordering::Relaxed);
        let zc = zc_reads(c)? - zc0;
        eprintln!("    {n} reads under two prunes, {zc} zero-copy");
        ensure!(n > 0 && zc >= n, "{n} reads, only {zc} zero-copy");

        // Closed: the pin goes, and the chunk is evictable like any other;
        // the file still reads (fetched again).
        drop(f);
        eventually(
            "the pin released at the close",
            Duration::from_secs(10),
            || {
                ensure!(open_pins(c)? == 0, "open pins {}", open_pins(c)?);
                Ok(())
            },
        )?;
        prune_all(c)?;
        ensure!(
            !cached(c, &tail)?,
            "a closed file's chunk was not evictable"
        );
        ensure!(
            read_all(&path)? == *data,
            "the file reads wrong after eviction"
        );
        Ok(())
    })
}

pub fn disabled_by_verify_always(seed: u64) -> Result<()> {
    let (env, root) = setup("zc-verify")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/zc-verify-{}", ts());
    let mut c =
        client(&env, root.path(), "c0", &backend)?.with_env("CONSTELLATION_FUSE_PASSTHROUGH", "1");
    c.fs_create()?;
    c.mount_view(None, &["--locks", "local", "--cache-verify", "always"])?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        // On a host whose mounts get zero-copy queues (the requirement),
        // `always` keeps them off: plain `uring`, and that is not a
        // transport fallback (the ring was granted).
        let status = c.control_status()?;
        let mount = &status["fuse"]["mounts"][0];
        ensure!(
            mount["transport"] == "uring" && mount["last_fallback"].is_null(),
            "--cache-verify always: want plain uring, no fallback: {mount}"
        );
        ensure!(status["cache"]["cache_verify"] == "always");
        let p = &mount["passthrough"];
        ensure!(
            p["enabled"] == false && p["unavailable_reason"] == "cache_verify_always",
            "passthrough under --cache-verify always: {p}"
        );
        let data = bytes(seed, 2 * CHUNK as usize);
        let path = c.mnt.join("verified");
        write_file(c, &path, &data)?;
        let (hits0, _) = memcache(c)?;
        let (zc0, d0) = reads(c)?;
        for _ in 0..2 {
            ensure!(read_all(&path)? == data, "buffered: other bytes");
        }
        let f = open_direct(&path)?;
        ensure!(direct_read(&f, 4096, 64 << 10)? == data[4096..4096 + (64 << 10)]);
        let (zc1, d1) = reads(c)?;
        let (hits1, _) = memcache(c)?;
        ensure!(
            zc1 == zc0 && zc1 == 0,
            "zero-copy reads under always: {zc1}"
        );
        ensure!(d1 > d0, "the reads did not reach the daemon");
        ensure!(
            hits1 > hits0,
            "the memory cache served nothing ({hits0} -> {hits1} hits)"
        );
        ensure!(
            c.control_status()?["fuse"]["zero_copy_reads_total"] == 0,
            "the process counted zero-copy reads"
        );
        eprintln!(
            "    always: {} daemon reads, {} memory hits, 0 zero-copy",
            d1 - d0,
            hits1 - hits0
        );
        Ok(())
    })
}
