//! Plan 38 §6 "Passthrough-specific" (Z3b): FUSE passthrough on a real
//! kernel — the pin-while-open guard, close-to-open across nodes and
//! within one, `O_DIRECT` on a passthrough handle, a handover with a
//! passthrough handle open, `--cache-verify always` turning it off, and
//! the default that asks for it on read-only mounts only.
//!
//! All but `passthrough-default-by-mount-mode` mount writable views with
//! the opt-in (`CONSTELLATION_FUSE_PASSTHROUGH=1`), which is what a
//! writable mount needs since review 38-z3b's must-fix 1.
//!
//! Every scenario but `passthrough-disabled-by-verify-always` `requires`
//! [`suites::CAP_SYS_ADMIN`] and [`suites::LINUX_6_9`], so an unprivileged
//! run or an old kernel SKIPs them loudly, naming which. Where both are
//! present the mount must actually *get* passthrough: a session that
//! reports `fuse.mounts[].passthrough.enabled = false` is a failure with the
//! reason it gave (a kernel without `CONFIG_FUSE_PASSTHROUGH`, say), not
//! a skip — the plan's rule that a fast path silently never engaging must
//! be observable applies to the harness first.
//!
//! What a passthrough handle is, from outside: `node.status` counts it
//! (`fuse.mounts[].passthrough.opens`), the disk cache holds a pin for
//! it (`cache.open_pins`), and its reads never reach the daemon (the
//! `read` series of `vfs_ops` does not move). The scenarios assert on
//! all three, so a handle that silently fell back to the daemon fails.
//!
//! The kernel refuses a backing file on a stacked filesystem (overlayfs:
//! `ELOOP`, since the mount asks for `max_stack_depth = 1`), so the
//! harness's work directory — where each client's cache lives — must not
//! be one; [`check_root`] says so instead of letting every open fall back.

use super::{eventually, setup, ts};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{bail, ensure, Context, Result};
use std::collections::HashSet;
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::time::Duration;

/// Non-repeating bytes from `seed` (incompressible, so the chunk file is
/// exactly the file).
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// A filesystem the kernel can back a passthrough file onto: not a
/// stacked one.
fn check_root(root: &Path) -> Result<()> {
    const OVERLAYFS_SUPER_MAGIC: i64 = 0x794c_7630;
    let c = std::ffi::CString::new(root.as_os_str().as_encoded_bytes())?;
    // SAFETY: a valid C string and a zeroed out-parameter.
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c.as_ptr(), &mut st) } != 0 {
        return Err(std::io::Error::last_os_error()).context("statfs of the harness work dir");
    }
    ensure!(
        st.f_type as i64 != OVERLAYFS_SUPER_MAGIC,
        "the harness work dir {} is on overlayfs: the kernel refuses a passthrough backing \
         file on a stacked filesystem (ELOOP); set TMPDIR to a non-stacked filesystem",
        root.display()
    );
    Ok(())
}

fn client(env: &S3Env, root: &Path, name: &str, backend: &str) -> Result<Client> {
    // The mounts must ask for passthrough whatever the harness's own
    // environment says.
    Client::new(root, name, &env.endpoint, backend)
        .map(|c| c.with_env("CONSTELLATION_FUSE_PASSTHROUGH", "1"))
}

/// The one mount's `fuse.mounts[].passthrough` section.
fn passthrough(c: &Client) -> Result<serde_json::Value> {
    let status = c.control_status()?;
    let mounts = status["fuse"]["mounts"]
        .as_array()
        .context("no fuse.mounts array")?;
    ensure!(
        mounts.len() == 1,
        "expected one FUSE mount, got {}",
        mounts.len()
    );
    Ok(mounts[0]["passthrough"].clone())
}

/// `(passthrough opens, cache open pins)`.
fn counts(c: &Client) -> Result<(u64, u64)> {
    let status = c.control_status()?;
    let opens = status["fuse"]["mounts"][0]["passthrough"]["opens"]
        .as_u64()
        .with_context(|| format!("no fuse.mounts[0].passthrough.opens: {}", status["fuse"]))?;
    let pins = status["cache"]["open_pins"]
        .as_u64()
        .context("no cache.open_pins")?;
    Ok((opens, pins))
}

fn expect_counts(c: &Client, want: (u64, u64), what: &str) -> Result<()> {
    eventually(what, Duration::from_secs(10), || {
        let got = counts(c)?;
        ensure!(
            got == want,
            "(passthrough opens, open pins) = {got:?}, want {want:?}"
        );
        Ok(())
    })
}

/// The mount got passthrough, or the reason it gave is the failure.
fn require_enabled(c: &Client) -> Result<()> {
    let p = passthrough(c)?;
    if p["enabled"].as_bool() != Some(true) {
        bail!(
            "this host has CAP_SYS_ADMIN and Linux >= 6.9, yet the mount did not get \
             passthrough: {p}\n{}",
            c.tail_log()
        );
    }
    Ok(())
}

/// Reads of the view answered by the daemon so far (every outcome).
fn daemon_reads(c: &Client) -> Result<u64> {
    let status = c.control_status()?;
    let series = status["vfs_ops"]["series"]
        .as_array()
        .context("no vfs_ops.series")?;
    Ok(series
        .iter()
        .filter(|s| s["op"] == "read")
        .flat_map(|s| s["outcomes"].as_object().into_iter().flatten())
        .filter_map(|(_, n)| n.as_u64())
        .sum())
}

/// The disk cache's entries: `hash -> (size, state)`.
fn cache_entries(c: &Client) -> Result<Vec<(String, u64, String)>> {
    let listing = c.control("cache.list", serde_json::json!({}))?;
    Ok(listing["entries"]
        .as_array()
        .context("cache.list has no entries")?
        .iter()
        .map(|e| {
            (
                e["hash"].as_str().unwrap_or_default().to_string(),
                e["size"].as_u64().unwrap_or(0),
                e["state"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect())
}

/// Write `data` to `path` (a new file) and wait for its one chunk to be
/// in the disk cache, clean (uploaded); its hash.
fn write_one_chunk(c: &Client, path: &Path, data: &[u8]) -> Result<String> {
    let before: HashSet<String> = cache_entries(c)?.into_iter().map(|e| e.0).collect();
    let mut f = std::fs::File::create(path)?;
    f.write_all(data)?;
    f.sync_all()?;
    drop(f);
    let mut hash = String::new();
    eventually(
        "the file's chunk cached and clean",
        Duration::from_secs(60),
        || {
            let new: Vec<_> = cache_entries(c)?
                .into_iter()
                .filter(|e| !before.contains(&e.0) && e.1 == data.len() as u64)
                .collect();
            ensure!(
                new.len() == 1,
                "new chunks of {} bytes: {new:?}",
                data.len()
            );
            ensure!(new[0].2 == "clean", "chunk state {}", new[0].2);
            hash = new[0].0.clone();
            Ok(())
        },
    )?;
    Ok(hash)
}

fn prune_all(c: &Client) -> Result<()> {
    c.control_call(
        "cache.prune",
        serde_json::json!({"target_bytes": 0}),
        Duration::from_secs(60),
    )?;
    Ok(())
}

fn cached(c: &Client, hash: &str) -> Result<bool> {
    Ok(cache_entries(c)?.iter().any(|e| e.0 == hash))
}

fn read_all(path: &Path) -> Result<Vec<u8>> {
    let mut v = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut v)?;
    Ok(v)
}

fn pread_all(f: &std::fs::File, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    f.read_exact_at(&mut buf, 0)?;
    Ok(buf)
}

/// Run `body` with `clients`, unmounting them on every path.
fn with_clients(clients: &mut [Client], body: impl FnOnce(&[Client]) -> Result<()>) -> Result<()> {
    let result = body(clients);
    for c in clients.iter_mut() {
        let _ = c.unmount();
    }
    result
}

/// One chunk of a 1 MiB-chunk filesystem, a size no other file has.
const LEN: usize = 200_003;

pub fn eviction_while_open(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-evict")?;
    check_root(root.path())?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-evict-{}", ts());
    let budget: u64 = 4 << 20;
    let mut c = client(&env, root.path(), "c0", &backend)?.with_cache_size(budget);
    c.fs_create()?;
    c.mount()?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        require_enabled(c)?;
        let data = bytes(seed, LEN);
        let path = c.mnt.join("held");
        let hash = write_one_chunk(c, &path, &data)?;
        expect_counts(c, (0, 0), "nothing open")?;

        let held = std::fs::File::open(&path)?;
        expect_counts(c, (1, 1), "one passthrough handle, one pin")?;
        // The guard's refcount is the open-descriptor count.
        let second = std::fs::File::open(&path)?;
        expect_counts(c, (2, 2), "two handles, two pins")?;
        drop(second);
        expect_counts(c, (1, 1), "back to one")?;

        // Fill the cache well past its budget, then prune it to nothing:
        // the held chunk is the least recently used of all and still
        // stays.
        for i in 0..(3 * budget / (1 << 20)) {
            let filler = bytes(seed.wrapping_add(i + 1), 1 << 20);
            let mut f = std::fs::File::create(c.mnt.join(format!("filler-{i}")))?;
            f.write_all(&filler)?;
            f.sync_all()?;
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
            cached(c, &hash)?,
            "the chunk of an open passthrough file was evicted"
        );
        ensure!(
            pread_all(&held, LEN)? == data,
            "the held handle reads wrong bytes"
        );
        expect_counts(c, (1, 1), "still one handle, one pin")?;

        // Closed: the pin goes, and the chunk is evictable like any other.
        drop(held);
        expect_counts(c, (0, 0), "closed")?;
        prune_all(c)?;
        ensure!(
            !cached(c, &hash)?,
            "a closed file's chunk was not evictable"
        );
        // And the file still reads (fetched again).
        ensure!(
            read_all(&path)? == data,
            "the file reads wrong after eviction"
        );
        Ok(())
    })
}

pub fn remote_write_cto(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-cto")?;
    check_root(root.path())?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-cto-{}", ts());
    let mut a = client(&env, root.path(), "a", &backend)?;
    let mut b = client(&env, root.path(), "b", &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    let mut clients = [a, b];
    with_clients(&mut clients, |cs| {
        let (a, b) = (&cs[0], &cs[1]);
        require_enabled(a)?;
        let old = bytes(seed, LEN);
        let new = bytes(seed ^ 0x5555, LEN);
        let on_a = a.mnt.join("shared");
        let on_b = b.mnt.join("shared");
        write_one_chunk(a, &on_a, &old)?;
        eventually("B sees the file", Duration::from_secs(30), || {
            ensure!(read_all(&on_b)? == old, "B reads other bytes");
            Ok(())
        })?;

        let held = std::fs::File::open(&on_a)?;
        expect_counts(a, (1, 1), "A's handle is passthrough")?;
        let reads = daemon_reads(a)?;
        ensure!(pread_all(&held, LEN)? == old);
        ensure!(
            daemon_reads(a)? == reads,
            "a passthrough handle's read reached the daemon"
        );

        // B writes new bytes in place.
        let w = std::fs::OpenOptions::new().write(true).open(&on_b)?;
        w.write_all_at(&new, 0)?;
        w.sync_all()?;
        drop(w);

        // A fresh open on A sees them as soon as A has the new manifest —
        // even with the old handle still open: it shares the inode's
        // backing in the kernel but is served by the daemon (direct I/O).
        eventually(
            "a fresh open on A sees B's write",
            Duration::from_secs(30),
            || {
                ensure!(read_all(&on_a)? == new, "A still reads the old bytes");
                Ok(())
            },
        )?;
        // The handle opened before the write keeps the chunk it was
        // opened on: close-to-open.
        ensure!(
            pread_all(&held, LEN)? == old,
            "the open passthrough handle no longer reads the bytes it was opened on"
        );
        drop(held);
        expect_counts(a, (0, 0), "closed")?;
        // After the close, a new open sees the new bytes — through a
        // passthrough handle again once the new chunk is cached (the
        // reads above cached it).
        eventually(
            "a new passthrough open on the new chunk",
            Duration::from_secs(30),
            || {
                let f = std::fs::File::open(&on_a)?;
                let got = pread_all(&f, LEN)?;
                let (opens, _) = counts(a)?;
                ensure!(got == new, "a new open reads the old bytes");
                ensure!(
                    opens == 1,
                    "the new open is not passthrough ({opens} opens)"
                );
                Ok(())
            },
        )?;
        expect_counts(a, (0, 0), "closed again")?;
        Ok(())
    })
}

pub fn local_writer(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-local")?;
    check_root(root.path())?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-local-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    c.mount()?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        require_enabled(c)?;
        let old = bytes(seed, LEN);
        let new = bytes(seed ^ 0xaaaa, LEN);
        let path = c.mnt.join("local");
        write_one_chunk(c, &path, &old)?;

        let reader = std::fs::File::open(&path)?;
        expect_counts(c, (1, 1), "the reader is passthrough")?;
        // A read-write open of a file open in passthrough mode: ETXTBSY
        // (the passthrough module doc says why).
        let rw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path);
        match rw {
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) => {}
            other => bail!("a read-write open of a passthrough-open file: {other:?}, want ETXTBSY"),
        }
        let status = passthrough(c)?;
        ensure!(status["refused_opens"].as_u64() == Some(1), "{status}");
        // A write-only open is served (by the daemon), and its write is
        // visible to every open after it ...
        let w = std::fs::OpenOptions::new().write(true).open(&path)?;
        w.write_all_at(&new, 0)?;
        ensure!(
            read_all(&path)? == new,
            "an open after a local write does not see it"
        );
        w.sync_all()?;
        drop(w);
        ensure!(read_all(&path)? == new);
        // ... but not to the passthrough handle opened before it, which
        // keeps the bytes it was opened on until it is closed: the
        // close-to-open behaviour plan 38 §3(c) documents.
        ensure!(
            pread_all(&reader, LEN)? == old,
            "the earlier passthrough handle sees the later write"
        );
        drop(reader);
        expect_counts(c, (0, 0), "closed")?;
        ensure!(read_all(&path)? == new);
        // With every handle closed, a read-write open goes through again.
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .context("a read-write open once nothing is open")?;
        Ok(())
    })
}

/// This thread's storage reads (`/proc/thread-self/io`'s `read_bytes`):
/// bytes it caused to be fetched from a block device.
fn thread_read_bytes() -> Result<u64> {
    let io = std::fs::read_to_string("/proc/thread-self/io")?;
    io.lines()
        .find_map(|l| l.strip_prefix("read_bytes:"))
        .and_then(|v| v.trim().parse().ok())
        .context("no read_bytes in /proc/thread-self/io")
}

/// A page-aligned buffer for `O_DIRECT`.
struct Aligned {
    ptr: *mut u8,
    layout: std::alloc::Layout,
}

impl Aligned {
    fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len, 4096).unwrap();
        // SAFETY: a non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Self { ptr, layout }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: `layout.size()` bytes allocated above, owned here.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

impl Drop for Aligned {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

pub fn odirect(seed: u64) -> Result<()> {
    // The cache must sit on a block device for "read from disk" to mean
    // anything (a tmpfs `O_DIRECT` read is a memory copy): the work dir
    // goes under `CONSTELLATION_HARNESS_DISK_DIR` (default `/var/tmp`).
    let disk =
        std::env::var("CONSTELLATION_HARNESS_DISK_DIR").unwrap_or_else(|_| "/var/tmp".into());
    let (env, _tmp) = setup("pt-odirect")?;
    let root = tempfile::Builder::new()
        .prefix("harness-pt-odirect-")
        .tempdir_in(&disk)
        .with_context(|| format!("a work dir under {disk}"))?;
    check_root(root.path())?;
    // SAFETY: a valid C string and a zeroed out-parameter.
    let fs_type = {
        let c = std::ffi::CString::new(root.path().as_os_str().as_encoded_bytes())?;
        let mut st: libc::statfs = unsafe { std::mem::zeroed() };
        ensure!(
            unsafe { libc::statfs(c.as_ptr(), &mut st) } == 0,
            "statfs {disk}"
        );
        st.f_type as i64
    };
    const TMPFS_MAGIC: i64 = 0x0102_1994;
    ensure!(
        fs_type != TMPFS_MAGIC,
        "{disk} is tmpfs: set CONSTELLATION_HARNESS_DISK_DIR to a block-device filesystem"
    );
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-odirect-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    c.mount()?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        require_enabled(c)?;
        // Page-multiple, so an `O_DIRECT` read of the whole file is legal.
        let len = 64 * 4096;
        let data = bytes(seed, len);
        let path = c.mnt.join("direct");
        write_one_chunk(c, &path, &data)?;

        let buffered = std::fs::File::open(&path)?;
        expect_counts(c, (1, 1), "the buffered handle is passthrough")?;
        let reads = daemon_reads(c)?;
        // Warm: read it twice through the page cache; the second read
        // fetches nothing from the device.
        ensure!(pread_all(&buffered, len)? == data);
        let before = thread_read_bytes()?;
        ensure!(pread_all(&buffered, len)? == data);
        let warm = thread_read_bytes()? - before;
        ensure!(
            warm == 0,
            "a warm buffered read fetched {warm} bytes from the device"
        );

        // `O_DIRECT` on a passthrough handle goes to the backing file's
        // device even though every page is cached (plan 38 §2.1 row 7's
        // behaviour change, asserted rather than discovered).
        let direct = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&path)?;
        expect_counts(c, (2, 2), "the O_DIRECT handle is passthrough too")?;
        let mut buf = Aligned::new(len);
        let before = thread_read_bytes()?;
        direct.read_exact_at(buf.as_mut(), 0)?;
        let cold = thread_read_bytes()? - before;
        ensure!(
            buf.as_mut() == data.as_slice(),
            "the O_DIRECT read returned other bytes"
        );
        ensure!(
            cold >= len as u64,
            "an O_DIRECT read of {len} warm bytes fetched only {cold} from the device"
        );
        ensure!(
            daemon_reads(c)? == reads,
            "a passthrough read (buffered or O_DIRECT) reached the daemon"
        );
        drop(direct);
        drop(buffered);
        expect_counts(c, (0, 0), "closed")?;
        Ok(())
    })
}

pub fn handover(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-handover")?;
    check_root(root.path())?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-handover-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    c.mount()?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        require_enabled(c)?;
        let data = bytes(seed, LEN);
        let path = c.mnt.join("across");
        let hash = write_one_chunk(c, &path, &data)?;
        let held = std::fs::File::open(&path)?;
        expect_counts(c, (1, 1), "a passthrough handle before the upgrade")?;
        let before = super::handover::generation(c)?;
        super::handover::upgrade(c)?;
        ensure!(
            super::handover::generation(c)? == before + 1,
            "no handover happened"
        );
        // The new image knows the handle (its backing id crossed) and
        // holds the pin (its chunk hash crossed).
        require_enabled(c)?;
        expect_counts(c, (1, 1), "the handle and its pin crossed the handover")?;
        prune_all(c)?;
        ensure!(
            cached(c, &hash)?,
            "evicted under a handed-over passthrough handle"
        );
        ensure!(
            pread_all(&held, LEN)? == data,
            "the handed-over handle reads wrong"
        );
        // A new open of the same inode must reuse the handed-over backing
        // id (the kernel answers anything else EIO). The new image found
        // the chunk on disk at start-up and has not hashed it, so the
        // engine offers no backing (and takes no pin) for it: the open
        // shares the inode's backing but is served by the daemon (direct
        // I/O), which is what hashes it.
        let again = std::fs::File::open(&path).context("a new open after the handover")?;
        expect_counts(c, (2, 1), "the new open shares the backing")?;
        ensure!(pread_all(&again, LEN)? == data);
        drop(again);
        drop(held);
        expect_counts(c, (0, 0), "closed in the new image")?;
        prune_all(c)?;
        ensure!(!cached(c, &hash)?, "not evictable after the close");
        ensure!(read_all(&path)? == data);
        Ok(())
    })
}

pub fn disabled_by_verify_always(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-verify")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-verify-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend)?;
    c.fs_create()?;
    c.mount_view(None, &["--cache-verify", "always"])?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        // Whatever this host can do — privileged or not — the reason is
        // the knob's: it is checked before the capability and the kernel.
        let p = passthrough(c)?;
        ensure!(
            p["enabled"] == false,
            "passthrough on under --cache-verify always: {p}"
        );
        ensure!(
            p["unavailable_reason"] == "cache_verify_always",
            "the reason is not cache_verify_always: {p}"
        );
        let status = c.control_status()?;
        ensure!(status["cache"]["cache_verify"] == "always");
        let data = bytes(seed, LEN);
        let path = c.mnt.join("verified");
        write_one_chunk(c, &path, &data)?;
        let held = std::fs::File::open(&path)?;
        let reads = daemon_reads(c)?;
        ensure!(pread_all(&held, LEN)? == data);
        expect_counts(c, (0, 0), "no passthrough handle, no pin")?;
        ensure!(
            daemon_reads(c)? > reads,
            "under --cache-verify always every read must reach the daemon"
        );
        Ok(())
    })
}

/// Review 38-z3b must-fix 1: without the opt-in only a read-only mount
/// asks for passthrough. A writable mount reports `writable_mount`, holds
/// no backing and so no `ETXTBSY` (a read-write open beside a reader is
/// ordinary); a mount of a snapshot of the same file — mounted `ro`, where
/// `open(O_RDWR)` is `EROFS` before FUSE — negotiates passthrough.
///
/// What the read-only mount does *not* show yet is a passthrough open: a
/// snapshot view serves its files as synthetic frozen nodes, which
/// `View::open` answers without consulting the passthrough eligibility
/// rule (plan 38 Z3a's, which covers the live tree only). So the scenario
/// asserts the negotiation, the bytes and `EROFS`, and leaves the count
/// alone; extending eligibility to frozen files is an engine change
/// (PROGRESS.md, plan 38 Z3, "Open").
pub fn default_by_mount_mode(seed: u64) -> Result<()> {
    let (env, root) = setup("pt-default")?;
    check_root(root.path())?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/pt-default-{}", ts());
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .without_env("CONSTELLATION_FUSE_PASSTHROUGH");
    let data = bytes(seed, LEN);
    c.fs_create()?;
    c.mount()?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        let p = passthrough(c)?;
        ensure!(
            p["enabled"] == false && p["unavailable_reason"] == "writable_mount",
            "a writable mount without the opt-in: {p}"
        );
        let path = c.mnt.join("frozen");
        write_one_chunk(c, &path, &data)?;
        let reader = std::fs::File::open(&path)?;
        ensure!(pread_all(&reader, LEN)? == data);
        expect_counts(c, (0, 0), "no passthrough handle on a writable mount")?;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .context("a read-write open beside a reader on a writable mount")?;
        drop(reader);
        c.snapshot_create("/@pt-ro")?;
        Ok(())
    })?;
    c.mount_view(Some("/@pt-ro"), &[])?;
    with_clients(std::slice::from_mut(&mut c), |cs| {
        let c = &cs[0];
        require_enabled(c)?;
        let p = passthrough(c)?;
        ensure!(
            p["unavailable_reason"].is_null(),
            "a read-only mount without the opt-in: {p}"
        );
        let path = c.mnt.join("frozen");
        let f = std::fs::File::open(&path)?;
        ensure!(
            pread_all(&f, LEN)? == data,
            "the snapshot reads other bytes"
        );
        drop(f);
        match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
        {
            Err(e) if e.raw_os_error() == Some(libc::EROFS) => Ok(()),
            other => bail!("a read-write open on the read-only mount: {other:?}, want EROFS"),
        }
    })
}
