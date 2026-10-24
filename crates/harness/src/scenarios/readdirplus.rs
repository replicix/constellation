//! Plan 40 §7 / P9: `READDIRPLUS` through a real mount. The kernel asks for
//! plus pages under the default `CONSTELLATION_FUSE_READDIRPLUS=auto` for
//! the first chunk of every listing (`ls -l`, `find`, any `getdents` loop
//! followed by `lstat`s), so these scenarios need no setting: they count
//! what `node.status` reports per mount (`readdirplus_entries`,
//! `readdirplus_name_only_why`, `kernel_refs`, `forgets_partial`,
//! `unlinked_refs`) and the node-wide `readdir` block, to show plus was
//! really used.
//!
//! - `readdirplus-attrs-match`: a second node's seeded changes (chmod,
//!   utimes, truncate, append, rename, replace, unlink, link) in a
//!   2,000-entry directory; the other node's `ls -l` equals the oracle
//!   after each batch settles.
//! - `readdirplus-local-appender`: `O_APPEND` writers, plus listers and a
//!   `chmod`er, in `--write-mode back` and `through`: every record lands
//!   in order, and no listing shows a size below what a writer had
//!   completed.
//! - `readdirplus-orphan-forget`: R2's deferred reproduction of the
//!   partial `FORGET` (plan 40 §4.4), and the `O_PATH` references that plus
//!   entries are.
//! - `readdirplus-strict`: a lone `--cto strict` sequencer answers
//!   attributes; with a second node every plus entry is name-only.
//! - `readdirplus-handover`: `daemon --upgrade` with `ls -lR` loops,
//!   `O_PATH` references and a listing paused mid-directory across it.
//!
//! The listing-exactness scenarios under pins and deletion
//! (`readdir-huge-delete`, `readdir-seekdir-after-delete`,
//! `readdir-many-open-dirs`) are in `super::readdir_pins`, which shares
//! this module's helpers. `CONSTELLATION_FUSE_TRANSPORT` is the lane's, as
//! for most scenarios (the handover one pins `/dev/fuse`, as every handover
//! scenario does).

use super::{eventually, raw_keys, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{bail, ensure, Context, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A scenario's size knob: the environment's value, else `default`.
pub(super) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &usize| *v > 0)
        .unwrap_or(default)
}

// ----------------------------------------------------------------- status

/// The first FUSE mount of `c` in `node.status`.
pub(super) fn mount_status(c: &Client) -> Result<serde_json::Value> {
    let status = c.control_status()?;
    status["fuse"]["mounts"]
        .as_array()
        .and_then(|m| m.first())
        .cloned()
        .context("no FUSE mount in node.status")
}

/// A counter of a status object (0 when absent).
pub(super) fn num(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

/// The node-wide `readdir` block of `node.status`.
pub(super) fn readdir_status(c: &Client) -> Result<serde_json::Value> {
    let status = c.control_status()?;
    ensure!(
        status["readdir"].is_object(),
        "node.status has no readdir block"
    );
    Ok(status["readdir"].clone())
}

/// The name-only reasons of a mount status, as `reason=n` pairs.
fn why(m: &serde_json::Value) -> String {
    m["readdirplus_name_only_why"].to_string()
}

// ---------------------------------------------------------------- fixture

/// A fresh filesystem with `names.len()` nodes mounted: the first creates
/// it. Every node has its own key; with more than one they wait for P2P.
pub(super) struct Fixture {
    pub env: S3Env,
    pub root: tempfile::TempDir,
    pub clients: Vec<Client>,
    /// The bucket prefix of the filesystem.
    pub prefix: String,
}

impl Fixture {
    pub(super) fn new(
        scenario: &str,
        names: &[&str],
        envs: &[(&str, &str)],
        write_mode: Option<&str>,
        args: &[&str],
    ) -> Result<Fixture> {
        let (env, root) = setup(scenario)?;
        let prefix = format!("{scenario}-{}", ts());
        let backend = format!("s3://{BUCKET}/{prefix}");
        let mut clients = Vec::new();
        for name in names {
            let mut c =
                Client::new(root.path(), name, &env.direct_endpoint, &backend)?.with_own_node_key();
            for (k, v) in envs {
                c = c.with_env(k, v);
            }
            if let Some(mode) = write_mode {
                c = c.with_write_mode(mode);
            }
            clients.push(c);
        }
        clients[0].fs_create()?;
        for c in clients.iter_mut() {
            c.mount_view(None, args)?;
        }
        if clients.len() > 1 {
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
        }
        Ok(Fixture {
            env,
            root,
            clients,
            prefix,
        })
    }

    pub(super) fn unmount_all(&mut self) {
        for c in self.clients.iter_mut().rev() {
            let _ = c.unmount();
        }
    }
}

// ------------------------------------------------------------ raw syscalls

pub(super) fn cpath(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).expect("a path without NUL")
}

/// An `O_PATH` descriptor: a kernel reference to the inode that needs no
/// name (and so survives the name's removal), which is what a plus entry's
/// lookup count must not lose.
pub(super) struct PathFd(RawFd);

impl PathFd {
    pub(super) fn open(path: &Path) -> std::io::Result<PathFd> {
        let c = cpath(path);
        // SAFETY: a NUL-terminated path.
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(PathFd(fd))
        }
    }

    /// `fstat` of the inode itself: `Ok` with `nlink 0` for an unlinked
    /// file whose reference is held, `ESTALE` if the daemon forgot it.
    pub(super) fn fstat(&self) -> std::io::Result<libc::stat> {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: an open descriptor and a stat buffer.
        if unsafe { libc::fstat(self.0, &mut st) } == 0 {
            Ok(st)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }
}

impl Drop for PathFd {
    fn drop(&mut self) {
        // SAFETY: our descriptor.
        unsafe { libc::close(self.0) };
    }
}

/// An open directory stream (`opendir`, `readdir`, `telldir`, `seekdir`):
/// the calls `fts`, `ls` and a `seekdir` user make, one `opendir` handle
/// (an `opendir`/`releasedir` pair on the wire).
pub(super) struct DirStream(*mut libc::DIR);

// SAFETY: a DIR* is used by one thread at a time.
unsafe impl Send for DirStream {}

impl DirStream {
    pub(super) fn open(path: &Path) -> std::io::Result<DirStream> {
        let c = cpath(path);
        // SAFETY: a NUL-terminated path.
        let d = unsafe { libc::opendir(c.as_ptr()) };
        if d.is_null() {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(DirStream(d))
        }
    }

    /// The next entry other than `.` and `..`, with the position after it
    /// (`telldir`); `None` at the end; an error as the kernel answered it.
    pub(super) fn next(&mut self) -> std::io::Result<Option<(String, i64)>> {
        loop {
            // SAFETY: errno is thread-local; the stream is ours.
            unsafe { *libc::__errno_location() = 0 };
            let e = unsafe { libc::readdir64(self.0) };
            if e.is_null() {
                let err = std::io::Error::last_os_error();
                return if err.raw_os_error() == Some(0) {
                    Ok(None)
                } else {
                    Err(err)
                };
            }
            // SAFETY: d_name is NUL-terminated.
            let name = unsafe { CStr::from_ptr((*e).d_name.as_ptr()) }
                .to_string_lossy()
                .into_owned();
            if name == "." || name == ".." {
                continue;
            }
            // SAFETY: the stream is ours.
            let pos = unsafe { libc::telldir(self.0) };
            return Ok(Some((name, pos as i64)));
        }
    }

    pub(super) fn seek(&mut self, pos: i64) {
        // SAFETY: the stream is ours.
        unsafe { libc::seekdir(self.0, pos as libc::c_long) };
    }

    /// Up to `n` further entries.
    pub(super) fn take(&mut self, n: usize) -> std::io::Result<Vec<(String, i64)>> {
        let mut out = Vec::new();
        while out.len() < n {
            match self.next()? {
                Some(e) => out.push(e),
                None => break,
            }
        }
        Ok(out)
    }

    /// Every remaining entry.
    pub(super) fn rest(&mut self) -> std::io::Result<Vec<(String, i64)>> {
        self.take(usize::MAX)
    }
}

impl Drop for DirStream {
    fn drop(&mut self) {
        // SAFETY: our stream.
        unsafe { libc::closedir(self.0) };
    }
}

/// A directory read with raw `getdents64`: the kernel's position is then
/// exactly the last `d_off` returned, which a buffered `readdir` hides.
pub(super) struct RawDir(std::fs::File);

impl RawDir {
    pub(super) fn open(path: &Path) -> std::io::Result<RawDir> {
        std::fs::File::open(path).map(RawDir)
    }

    /// `lseek` to a `d_off` (what `seekdir` does).
    pub(super) fn seek(&mut self, pos: i64) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        // SAFETY: an open descriptor.
        if unsafe { libc::lseek(self.0.as_raw_fd(), pos, libc::SEEK_SET) } < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// Up to `n` entries from the current position, over as many batches
    /// as it takes; an error as the kernel answered it. (glibc's
    /// `readdir` turns an `ENOENT` into the end of the directory, POSIX's
    /// reading of a directory removed under an open stream; a test that
    /// tells those apart reads with `getdents64`.)
    pub(super) fn take(&mut self, n: usize) -> std::io::Result<Vec<(String, i64)>> {
        let mut out = Vec::new();
        while out.len() < n {
            let b = self.batch(8192)?;
            if b.is_empty() {
                break;
            }
            out.extend(b);
        }
        out.truncate(n);
        Ok(out)
    }

    /// One `getdents64` of up to `bufsize` bytes: the entries other than
    /// `.` and `..` with their `d_off`; empty at the end.
    pub(super) fn batch(&mut self, bufsize: usize) -> std::io::Result<Vec<(String, i64)>> {
        use std::os::fd::AsRawFd;
        let mut buf = vec![0u8; bufsize];
        // SAFETY: an open directory descriptor and a buffer of that size.
        let n = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                self.0.as_raw_fd(),
                buf.as_mut_ptr(),
                buf.len(),
            )
        };
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut out = Vec::new();
        let mut at = 0usize;
        while at < n as usize {
            let rec = &buf[at..];
            let off = i64::from_ne_bytes(rec[8..16].try_into().unwrap());
            let len = u16::from_ne_bytes(rec[16..18].try_into().unwrap()) as usize;
            let name = CStr::from_bytes_until_nul(&rec[19..len])
                .map_err(|_| std::io::Error::other("a dirent without a NUL"))?
                .to_string_lossy()
                .into_owned();
            if name != "." && name != ".." {
                out.push((name, off));
            }
            at += len;
        }
        Ok(out)
    }
}

/// `utimensat` of the modification time (the access time untouched).
fn set_mtime(path: &Path, mtime: (i64, i64)) -> std::io::Result<()> {
    let c = cpath(path);
    let times = [
        libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        },
        libc::timespec {
            tv_sec: mtime.0,
            tv_nsec: mtime.1,
        },
    ];
    // SAFETY: a NUL-terminated path and two timespecs.
    if unsafe { libc::utimensat(libc::AT_FDCWD, c.as_ptr(), times.as_ptr(), 0) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// `syncfs` of the filesystem `mnt` is on.
pub(super) fn syncfs(mnt: &Path) -> Result<()> {
    let f = std::fs::File::open(mnt)?;
    use std::os::fd::AsRawFd;
    // SAFETY: an open descriptor.
    if unsafe { libc::syncfs(f.as_raw_fd()) } != 0 {
        bail!("syncfs: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// The descriptor limit raised to what the hard limit allows.
pub(super) fn raise_nofile(want: u64) -> Result<()> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: a rlimit buffer.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        bail!("getrlimit: {}", std::io::Error::last_os_error());
    }
    if lim.rlim_cur >= want {
        return Ok(());
    }
    ensure!(
        lim.rlim_max >= want,
        "the hard descriptor limit {} is below the {want} the scenario needs",
        lim.rlim_max
    );
    lim.rlim_cur = want;
    // SAFETY: a rlimit buffer.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) } != 0 {
        bail!("setrlimit: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// The name of the `i`th entry of a directory the scenarios fill: 23 bytes.
pub(super) fn entry_name(i: usize) -> String {
    format!(
        "entry-{i:08}-{:08x}",
        (i as u32).wrapping_mul(2_654_435_761)
    )
}

/// Creates the empty files `names(i)` for `i` in `range` under `dir`, from
/// `threads` threads.
pub(super) fn fill(
    dir: &Path,
    range: std::ops::Range<usize>,
    threads: usize,
    names: fn(usize) -> String,
) -> Result<()> {
    let next = Arc::new(AtomicU64::new(range.start as u64));
    let end = range.end as u64;
    let mut handles = Vec::new();
    for _ in 0..threads {
        let next = next.clone();
        let dir = dir.to_path_buf();
        handles.push(std::thread::spawn(move || -> Result<()> {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= end {
                    return Ok(());
                }
                let p = dir.join(names(i as usize));
                std::fs::File::create(&p).with_context(|| format!("creating {}", p.display()))?;
            }
        }));
    }
    let mut first = None;
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                next.store(end, Ordering::Relaxed);
                first.get_or_insert(e);
            }
            Err(_) => {
                next.store(end, Ordering::Relaxed);
                first.get_or_insert(anyhow::anyhow!("a filler thread panicked"));
            }
        }
    }
    first.map_or(Ok(()), Err)
}

/// A child process's output, failing on a non-zero exit except for the
/// vanished-name complaints a changing directory legitimately provokes.
fn run_tolerating_vanished(cmd: &mut Command) -> Result<()> {
    let out = cmd.output()?;
    if out.status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    for line in err.lines() {
        if !line.contains("No such file or directory") {
            bail!("{cmd:?} failed ({}): {err}", out.status);
        }
    }
    Ok(())
}

/// Runs `body` in a thread until the returned guard is stopped; the guard
/// returns the body's error, if any.
pub(super) struct Loop {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<Result<u64>>>,
}

impl Loop {
    /// `body` is called repeatedly while the flag is clear; it returns an
    /// error to end the loop, and the loop counts its calls.
    pub(super) fn start(mut body: impl FnMut() -> Result<()> + Send + 'static) -> Loop {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let thread = std::thread::spawn(move || -> Result<u64> {
            let mut n = 0;
            while !flag.load(Ordering::Relaxed) {
                body()?;
                n += 1;
            }
            Ok(n)
        });
        Loop {
            stop,
            thread: Some(thread),
        }
    }

    /// Stops the loop: how many times its body ran, or its error.
    pub(super) fn finish(mut self) -> Result<u64> {
        self.stop.store(true, Ordering::Relaxed);
        match self.thread.take().unwrap().join() {
            Ok(r) => r,
            Err(_) => bail!("a loop thread panicked"),
        }
    }
}

impl Drop for Loop {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

// ------------------------------------------------- readdirplus-attrs-match

/// What `ls -l --time-style=+%s.%N` prints for one name.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Listed {
    perm: u32,
    nlink: u32,
    size: u64,
    mtime: (i64, i64),
}

/// `ls -l --time-style=+%s.%N dir`, parsed.
fn ls_l(dir: &Path) -> Result<BTreeMap<String, Listed>> {
    let out = Command::new("ls")
        .env("LC_ALL", "C")
        .args(["-l", "--time-style=+%s.%N"])
        .arg(dir)
        .output()
        .context("running ls")?;
    ensure!(
        out.status.success(),
        "ls -l {} failed ({}): {}",
        dir.display(),
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    let mut listed = BTreeMap::new();
    for line in text.lines().filter(|l| !l.starts_with("total ")) {
        let f: Vec<&str> = line.split_whitespace().collect();
        ensure!(f.len() >= 7, "unparsable ls line {line:?}");
        let mode = f[0].as_bytes();
        ensure!(mode.len() >= 10 && mode[0] == b'-', "not a file: {line:?}");
        let mut perm = 0u32;
        for (i, c) in mode[1..10].iter().enumerate() {
            if *c != b'-' {
                perm |= 1 << (8 - i);
            }
        }
        let (sec, nsec) = f[5]
            .split_once('.')
            .with_context(|| format!("no time in {line:?}"))?;
        listed.insert(
            f[6..].join(" "),
            Listed {
                perm,
                nlink: f[1].parse()?,
                size: f[4].parse()?,
                mtime: (sec.parse()?, nsec.parse()?),
            },
        );
    }
    Ok(listed)
}

/// One inode of the oracle.
#[derive(Clone, Debug)]
struct OInode {
    perm: u32,
    size: u64,
    mtime: (i64, i64),
}

/// What the directory must list: each name's inode, and the inodes.
#[derive(Default)]
struct Oracle {
    names: BTreeMap<String, usize>,
    inodes: Vec<OInode>,
}

impl Oracle {
    fn expected(&self) -> BTreeMap<String, Listed> {
        let mut links = vec![0u32; self.inodes.len()];
        for i in self.names.values() {
            links[*i] += 1;
        }
        self.names
            .iter()
            .map(|(n, i)| {
                let ino = &self.inodes[*i];
                (
                    n.clone(),
                    Listed {
                        perm: ino.perm,
                        nlink: links[*i],
                        size: ino.size,
                        mtime: ino.mtime,
                    },
                )
            })
            .collect()
    }
}

/// The differences between what A lists and the oracle, at most `max`.
fn diffs(
    got: &BTreeMap<String, Listed>,
    want: &BTreeMap<String, Listed>,
    max: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    for (n, w) in want {
        match got.get(n) {
            None => out.push(format!("{n}: missing, want {w:?}")),
            Some(g) if g != w => out.push(format!("{n}: lists {g:?}, want {w:?}")),
            _ => {}
        }
    }
    for n in got.keys().filter(|n| !want.contains_key(*n)) {
        out.push(format!("{n}: listed but removed"));
    }
    out.truncate(max);
    out
}

/// Owner-writable modes only: the oracle's ops append and truncate.
const PERMS: [u32; 7] = [0o600, 0o640, 0o644, 0o664, 0o755, 0o700, 0o660];

/// Node B changes a 2,000-entry directory in seeded batches; node A's
/// `ls -l` equals the oracle once each batch settles. A loops `ls -l` and
/// `find -ls` meanwhile, so plus pages keep installing dentries and
/// attributes under the changes; the loops must see no error.
pub fn attrs_match(seed: u64) -> Result<()> {
    const NAME: &str = "readdirplus-attrs-match";
    let entries = env_usize("READDIRPLUS_ATTRS_ENTRIES", 2000);
    let steps = env_usize("READDIRPLUS_ATTRS_STEPS", 30);
    let per_step = env_usize("READDIRPLUS_ATTRS_OPS", 12);
    let mut fx = Fixture::new(NAME, &["a", "b"], &[], None, &[])?;
    let r = (|| -> Result<()> {
        let (a, b) = (&fx.clients[0], &fx.clients[1]);
        let mut rng = StdRng::seed_from_u64(seed);
        let bdir = b.mnt.join("d");
        let adir = a.mnt.join("d");
        std::fs::create_dir(&bdir)?;
        let mut o = Oracle::default();
        let mut clock = 0i64;
        let mut fresh = 0u64;
        let mut stamp = |rng: &mut StdRng| {
            clock += 1;
            (1_700_000_000 + clock, rng.random_range(0..1_000_000_000))
        };
        for i in 0..entries {
            let name = format!("f{i:05}");
            let p = bdir.join(&name);
            let size = rng.random_range(0..2048u64);
            std::fs::write(&p, vec![b'x'; size as usize])?;
            let perm = PERMS[rng.random_range(0..PERMS.len())];
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(perm))?;
            let mtime = stamp(&mut rng);
            set_mtime(&p, mtime)?;
            o.names.insert(name, o.inodes.len());
            o.inodes.push(OInode { perm, size, mtime });
        }
        let plus_before = num(&mount_status(a)?, "readdirplus_entries");
        // A's listers: errors are findings, vanished names are the changes.
        let listers = {
            let dir = adir.clone();
            Loop::start(move || {
                run_tolerating_vanished(Command::new("ls").arg("-l").arg(&dir))?;
                run_tolerating_vanished(Command::new("find").arg(&dir).arg("-ls"))
            })
        };
        let settle = |o: &Oracle, what: &str| -> Result<()> {
            let want = o.expected();
            eventually(what, Duration::from_secs(90), || {
                let got = ls_l(&adir)?;
                let d = diffs(&got, &want, 5);
                ensure!(d.is_empty(), "A's listing differs: {d:?}");
                Ok(())
            })
        };
        settle(&o, "A lists the populated directory")?;
        let mut ops = 0usize;
        for step in 0..steps {
            for _ in 0..per_step {
                let names: Vec<String> = o.names.keys().cloned().collect();
                if names.is_empty() {
                    break;
                }
                let name = names[rng.random_range(0..names.len())].clone();
                let p = bdir.join(&name);
                let ino = o.names[&name];
                match rng.random_range(0..9u32) {
                    0 => {
                        let perm = PERMS[rng.random_range(0..PERMS.len())];
                        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(perm))?;
                        o.inodes[ino].perm = perm;
                    }
                    1 | 2 => {
                        let mtime = stamp(&mut rng);
                        set_mtime(&p, mtime)?;
                        o.inodes[ino].mtime = mtime;
                    }
                    3 => {
                        let size = rng.random_range(0..4096u64);
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&p)?
                            .set_len(size)?;
                        let mtime = stamp(&mut rng);
                        set_mtime(&p, mtime)?;
                        o.inodes[ino].size = size;
                        o.inodes[ino].mtime = mtime;
                    }
                    4 => {
                        use std::io::Write;
                        let extra = rng.random_range(1..512u64);
                        std::fs::OpenOptions::new()
                            .append(true)
                            .open(&p)?
                            .write_all(&vec![b'y'; extra as usize])?;
                        let mtime = stamp(&mut rng);
                        set_mtime(&p, mtime)?;
                        o.inodes[ino].size += extra;
                        o.inodes[ino].mtime = mtime;
                    }
                    5 => {
                        fresh += 1;
                        let to = format!("r{fresh:05}");
                        std::fs::rename(&p, bdir.join(&to))?;
                        o.names.remove(&name);
                        o.names.insert(to, ino);
                    }
                    6 => {
                        // Replace another name's file with this one.
                        let other = names[rng.random_range(0..names.len())].clone();
                        if other != name && o.names[&other] != ino {
                            std::fs::rename(&p, bdir.join(&other))?;
                            o.names.remove(&name);
                            o.names.insert(other, ino);
                        }
                    }
                    7 => {
                        std::fs::remove_file(&p)?;
                        o.names.remove(&name);
                    }
                    _ => {
                        fresh += 1;
                        let to = format!("l{fresh:05}");
                        std::fs::hard_link(&p, bdir.join(&to))?;
                        o.names.insert(to, ino);
                    }
                }
                ops += 1;
            }
            settle(&o, &format!("A lists step {step}'s changes"))?;
        }
        let looped = listers.finish().context("A's ls/find loops")?;
        let after = mount_status(a)?;
        let plus_after = num(&after, "readdirplus_entries");
        eprintln!(
            "    {NAME}: {entries} entries, {ops} changes in {steps} steps, A's listers looped \
             {looped} times; A's plus entries {plus_before} -> {plus_after}; name-only {}",
            why(&after)
        );
        ensure!(
            plus_after > plus_before,
            "A's listings never used plus (readdirplus_entries stayed {plus_before}): {after}"
        );
        super::ensure_no_conflicts(&[&fx.clients[0], &fx.clients[1]])?;
        Ok(())
    })();
    fx.unmount_all();
    r
}

// -------------------------------------------- readdirplus-local-appender

const RECORD: usize = 12;

/// `w<writer>:<seq>\n`, `RECORD` bytes.
fn record(writer: usize, seq: u64) -> [u8; RECORD] {
    let s = format!("w{writer}:{seq:08}\n");
    let mut out = [0u8; RECORD];
    out.copy_from_slice(s.as_bytes());
    out
}

fn appender_run(mode: &str, secs: u64) -> Result<()> {
    const NAME: &str = "readdirplus-local-appender";
    const WRITERS: usize = 4;
    let mut fx = Fixture::new(&format!("{NAME}-{mode}"), &["a"], &[], Some(mode), &[])?;
    let r = (|| -> Result<()> {
        let c = &fx.clients[0];
        let dir = c.mnt.join("ap");
        std::fs::create_dir(&dir)?;
        // Names around the appended files, so the listing has pages to fill.
        for i in 0..200 {
            std::fs::write(dir.join(format!("other-{i:03}")), b"o")?;
        }
        let acked: Arc<Vec<AtomicU64>> =
            Arc::new((0..WRITERS).map(|_| AtomicU64::new(0)).collect());
        let stop = Arc::new(AtomicBool::new(false));
        let mut writers = Vec::new();
        for w in 0..WRITERS {
            let path = dir.join(format!("log-{w}"));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            let (acked, stop) = (acked.clone(), stop.clone());
            writers.push(std::thread::spawn(move || -> Result<u64> {
                use std::io::Write;
                let mut file = file;
                let mut seq = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    file.write_all(&record(w, seq))
                        .with_context(|| format!("writer {w} record {seq}"))?;
                    seq += 1;
                    acked[w].store(seq * RECORD as u64, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_micros(500));
                }
                Ok(seq)
            }));
        }
        let plus_before = num(&mount_status(c)?, "readdirplus_entries");
        // Lister 1: getdents then lstat of every name, the way `ls -l`
        // does it. A size below what a writer had completed before the
        // lstat began is a stale attribute.
        let l1 = {
            let (dir, acked) = (dir.clone(), acked.clone());
            Loop::start(move || {
                let floor: Vec<u64> = acked.iter().map(|a| a.load(Ordering::SeqCst)).collect();
                for e in std::fs::read_dir(&dir)? {
                    let e = e?;
                    let name = e.file_name().to_string_lossy().into_owned();
                    let md = std::fs::symlink_metadata(e.path())?;
                    if let Some(w) = name.strip_prefix("log-") {
                        let w: usize = w.parse()?;
                        ensure!(
                            md.len() >= floor[w],
                            "{name} lists size {} after a writer completed {}",
                            md.len(),
                            floor[w]
                        );
                    }
                }
                Ok(())
            })
        };
        // Lister 2: the real `ls -l`.
        let l2 = {
            let dir = dir.clone();
            Loop::start(move || {
                let out = Command::new("ls").arg("-l").arg(&dir).output()?;
                ensure!(out.status.success(), "ls -l failed: {:?}", out);
                Ok(())
            })
        };
        // A process that chmods the files while they are appended to.
        let chmod = {
            let dir = dir.clone();
            let mut flip = false;
            Loop::start(move || {
                flip = !flip;
                for w in 0..WRITERS {
                    let mode = if flip { 0o644 } else { 0o664 };
                    std::fs::set_permissions(
                        dir.join(format!("log-{w}")),
                        std::fs::Permissions::from_mode(mode),
                    )?;
                }
                std::thread::sleep(Duration::from_millis(5));
                Ok(())
            })
        };
        std::thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
        let mut counts = Vec::new();
        for (w, h) in writers.into_iter().enumerate() {
            counts.push(
                h.join()
                    .map_err(|_| anyhow::anyhow!("writer {w} panicked"))??,
            );
        }
        let (n1, n2, n3) = (l1.finish()?, l2.finish()?, chmod.finish()?);
        for (w, n) in counts.iter().enumerate() {
            let data = std::fs::read(dir.join(format!("log-{w}")))?;
            ensure!(
                data.len() as u64 == n * RECORD as u64,
                "log-{w}: {} bytes, {n} records of {RECORD} written",
                data.len()
            );
            for (seq, rec) in data.chunks(RECORD).enumerate() {
                ensure!(
                    rec == record(w, seq as u64),
                    "log-{w}: record {seq} is {:?}",
                    String::from_utf8_lossy(rec)
                );
            }
        }
        let after = mount_status(c)?;
        eprintln!(
            "    {NAME} ({mode}): {counts:?} records; lister loops {n1}+{n2}, chmod rounds {n3}; \
             plus entries {plus_before} -> {}",
            num(&after, "readdirplus_entries")
        );
        ensure!(
            num(&after, "readdirplus_entries") > plus_before,
            "the listers never used plus: {after}"
        );
        Ok(())
    })();
    fx.unmount_all();
    r
}

/// Four `O_APPEND` writers keep files open and append numbered records
/// while two listers loop and one process `chmod`s the files, in both
/// write modes: every file ends with every record in order (a stale plus
/// size would make the kernel position an append over acknowledged bytes).
pub fn local_appender(_seed: u64) -> Result<()> {
    let secs = env_usize("READDIRPLUS_APPENDER_SECS", 20) as u64;
    for mode in ["back", "through"] {
        appender_run(mode, secs).with_context(|| format!("--write-mode {mode}"))?;
    }
    Ok(())
}

// --------------------------------------------- readdirplus-orphan-forget

/// Waits until node `c` has applied what its peer wrote before `marker`
/// was created: `marker` exists on `c`. The poll looks up a name nothing
/// else uses, so it revalidates no other dentry.
fn applied(c: &Client, marker: &str) -> Result<()> {
    let p = c.mnt.join(marker);
    eventually(
        &format!("{} sees {marker}", c.name),
        Duration::from_secs(60),
        || {
            ensure!(std::fs::symlink_metadata(&p).is_ok(), "not yet");
            Ok(())
        },
    )
}

/// R2's deferred phase 1 (plan 40 §4.4), then the plus references.
pub fn orphan_forget(_seed: u64) -> Result<()> {
    const NAME: &str = "readdirplus-orphan-forget";
    let mut fx = Fixture::new(NAME, &["a", "b"], &[], None, &[])?;
    let r = (|| -> Result<()> {
        let (a, b) = (&fx.clients[0], &fx.clients[1]);

        // Phase 1: the revalidation's partial FORGET. A's kernel holds Y
        // under `m` (and an O_PATH descriptor of it) and X under `n`.
        std::fs::write(a.mnt.join("n"), b"X")?;
        std::fs::write(a.mnt.join("m"), b"Y")?;
        let y_ino = std::fs::metadata(a.mnt.join("m"))?.ino();
        let x_ino = std::fs::metadata(a.mnt.join("n"))?.ino();
        ensure!(x_ino != y_ino);
        let fd_m = PathFd::open(&a.mnt.join("m"))?;
        // Past the entry TTL (1 s) and then some: A's daemon writes an
        // invalidation only for a name its kernel can still hold a valid
        // dentry for, so the remote rename below leaves `n`'s dentry to be
        // revalidated by the next lookup, which is the race.
        std::thread::sleep(Duration::from_millis(2500));
        eventually("B sees m and n", Duration::from_secs(60), || {
            ensure!(
                b.mnt.join("m").exists() && b.mnt.join("n").exists(),
                "not yet"
            );
            Ok(())
        })?;
        std::fs::rename(b.mnt.join("m"), b.mnt.join("n"))?;
        std::fs::write(b.mnt.join("marker1"), b"")?;
        applied(a, "marker1")?;
        let partial0 = num(&mount_status(a)?, "forgets_partial");
        // The revalidation of n: its LOOKUP answers Y, not the dentry's X.
        let seen = std::fs::metadata(a.mnt.join("n"))?;
        ensure!(
            seen.ino() == y_ino,
            "n names inode {} on A, want Y = {y_ino} (X = {x_ino})",
            seen.ino()
        );
        // rm n: the last name of Y.
        std::fs::remove_file(a.mnt.join("n"))?;
        let st = fd_m.fstat().context(
            "fstat of the O_PATH descriptor of m after rm n (ESTALE: the orphan was reaped)",
        )?;
        ensure!(
            st.st_nlink == 0 && st.st_ino == y_ino,
            "fstat of m's descriptor: nlink {} ino {} (want nlink 0, ino {y_ino})",
            st.st_nlink,
            st.st_ino
        );
        eventually(
            "the revalidation's FORGET reaches the daemon",
            Duration::from_secs(30),
            || {
                let now = num(&mount_status(a)?, "forgets_partial");
                ensure!(now > partial0, "forgets_partial still {now}");
                Ok(())
            },
        )?;
        let partial1 = num(&mount_status(a)?, "forgets_partial");
        ensure!(
            partial1 == partial0 + 1,
            "forgets_partial rose by {} (want exactly 1)",
            partial1 - partial0
        );
        fd_m.fstat()
            .context("fstat of the descriptor after the partial FORGET")?;
        drop(fd_m);
        // `m` no longer names anything, but A's kernel still caches its
        // (expired) dentry, which holds Y: a lookup finds the name gone,
        // the dentry goes, and the kernel gives back its last reference.
        ensure!(
            std::fs::symlink_metadata(a.mnt.join("m")).is_err(),
            "m still exists on A"
        );
        eventually(
            "the orphan Y is reaped once the kernel lets go of it",
            Duration::from_secs(60),
            || {
                let n = num(&mount_status(a)?, "unlinked_refs");
                ensure!(n == 0, "unlinked_refs {n}");
                Ok(())
            },
        )?;
        eprintln!("    {NAME}: phase 1: forgets_partial {partial0} -> {partial1}, nlink 0, reaped");

        // Phase 2: references plus entries took. B creates the names, A
        // finds them through `ls -l` (so its kernel counts them from plus
        // pages), takes O_PATH descriptors, and one more thread unlinks
        // and rmdirs every name.
        let files = env_usize("READDIRPLUS_ORPHAN_FILES", 200);
        let dirs = env_usize("READDIRPLUS_ORPHAN_DIRS", 20);
        let pdir_b = b.mnt.join("p");
        std::fs::create_dir(&pdir_b)?;
        for i in 0..files {
            std::fs::write(pdir_b.join(format!("pf-{i:04}")), format!("file {i}"))?;
        }
        for i in 0..dirs {
            std::fs::create_dir(pdir_b.join(format!("pd-{i:03}")))?;
        }
        let pdir = a.mnt.join("p");
        eventually("A lists all of p", Duration::from_secs(60), || {
            let n = std::fs::read_dir(&pdir)?.count();
            ensure!(n == files + dirs, "A lists {n} of {}", files + dirs);
            Ok(())
        })?;
        // Let the plain listing's references and their dentries expire,
        // so that the references below are counted from the plus pages.
        std::thread::sleep(Duration::from_millis(1500));
        let plus0 = num(&mount_status(a)?, "readdirplus_entries");
        let refs0 = num(&mount_status(a)?, "kernel_refs");
        let out = Command::new("ls").arg("-l").arg(&pdir).output()?;
        ensure!(out.status.success(), "ls -l p failed");
        let mut held = Vec::new();
        for line in String::from_utf8_lossy(&out.stdout).lines().skip(1) {
            let name = line.split_whitespace().last().context("empty ls line")?;
            held.push((
                name.to_string(),
                PathFd::open(&pdir.join(name))
                    .with_context(|| format!("O_PATH {name} found by ls -l"))?,
            ));
        }
        ensure!(held.len() == files + dirs, "ls -l listed {}", held.len());
        let m = mount_status(a)?;
        ensure!(
            num(&m, "readdirplus_entries") >= plus0 + (files + dirs) as u64,
            "ls -l did not use plus: entries {plus0} -> {}",
            num(&m, "readdirplus_entries")
        );
        eprintln!(
            "    {NAME}: phase 2: {} descriptors from plus pages; kernel_refs {refs0} -> {}",
            held.len(),
            num(&m, "kernel_refs")
        );
        // Another thread of the mount removes every name.
        {
            let pdir = pdir.clone();
            std::thread::spawn(move || -> Result<()> {
                for i in 0..files {
                    std::fs::remove_file(pdir.join(format!("pf-{i:04}")))?;
                }
                for i in 0..dirs {
                    std::fs::remove_dir(pdir.join(format!("pd-{i:03}")))?;
                }
                Ok(())
            })
            .join()
            .map_err(|_| anyhow::anyhow!("the remover panicked"))??;
        }
        let mut bad = Vec::new();
        for (name, fd) in &held {
            match fd.fstat() {
                Ok(st) if st.st_nlink == 0 => {}
                Ok(st) => bad.push(format!("{name}: nlink {}", st.st_nlink)),
                Err(e) => bad.push(format!("{name}: {e}")),
            }
        }
        ensure!(
            bad.is_empty(),
            "{} of {} descriptors do not answer nlink 0: {:?}",
            bad.len(),
            held.len(),
            &bad[..bad.len().min(8)]
        );
        let m = mount_status(a)?;
        ensure!(
            num(&m, "unlinked_refs") > 0,
            "no unlinked reference is kept while {} descriptors are held: {m}",
            held.len()
        );
        eprintln!(
            "    {NAME}: phase 2: every fstat answered nlink 0; unlinked_refs {}",
            num(&m, "unlinked_refs")
        );
        drop(held);
        // The kernel lets go of the unlinked inodes at their last close.
        eventually(
            "unlinked_refs drains once the descriptors are closed",
            Duration::from_secs(60),
            || {
                let n = num(&mount_status(a)?, "unlinked_refs");
                ensure!(n == 0, "unlinked_refs {n}");
                Ok(())
            },
        )?;
        let holds = format!("{}/holds/", fx.prefix);
        eventually(
            "no held data is left in the bucket",
            Duration::from_secs(60),
            || {
                let keys = raw_keys(&fx.env.direct_endpoint, &holds)?;
                ensure!(keys.is_empty(), "holds: {keys:?}");
                Ok(())
            },
        )?;
        super::ensure_no_conflicts(&[&fx.clients[0], &fx.clients[1]])?;
        Ok(())
    })();
    fx.unmount_all();
    r
}

// ---------------------------------------------------- readdirplus-strict

/// The directory listing of a path as names.
fn names_of(dir: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        out.push(e?.file_name().to_string_lossy().into_owned());
    }
    out.sort();
    Ok(out)
}

/// `--cto strict`: a lone sequencer hands attributes; once a second node is
/// in the cluster every plus entry is name-only (the view cannot vouch for
/// attributes the other node may change), and the strict visibility holds
/// under `ls -l` loops; then the second node leaves and the restarted
/// sequencer is lone again.
pub fn strict(_seed: u64) -> Result<()> {
    const NAME: &str = "readdirplus-strict";
    let strict_env = [("CONSTELLATION_CTO", "strict")];
    let mut fx = Fixture::new(NAME, &["a"], &strict_env, None, &[])?;
    let mut b = Client::new(
        fx.root.path(),
        "b",
        &fx.env.direct_endpoint,
        &format!("s3://{BUCKET}/{}", fx.prefix),
    )?
    .with_own_node_key()
    .with_env("CONSTELLATION_CTO", "strict");
    let r = (|| -> Result<()> {
        let adir = fx.clients[0].mnt.join("s");
        std::fs::create_dir(&adir)?;
        for i in 0..300 {
            std::fs::write(adir.join(format!("f{i:03}")), format!("v{i}"))?;
        }
        // A is alone: a lone strict sequencer hands attributes.
        let m0 = mount_status(&fx.clients[0])?;
        let out = Command::new("ls").arg("-l").arg(&adir).output()?;
        ensure!(out.status.success(), "ls -l failed");
        let m1 = mount_status(&fx.clients[0])?;
        let de = num(&m1, "readdirplus_entries") - num(&m0, "readdirplus_entries");
        eprintln!(
            "    {NAME}: the lone sequencer: plus entries with attributes +{de}, name-only {}",
            why(&m1)
        );
        ensure!(
            de >= 300,
            "a lone strict sequencer handed {de} entries with attributes: {m1}"
        );
        // The second node joins: every plus entry is name-only from here.
        b.mount_view(None, &[])?;
        wait_for_p2p(&[&fx.clients[0], &b])?;
        let (a, b) = (&fx.clients[0], &b);
        let bdir = b.mnt.join("s");
        ensure!(names_of(&bdir)?.len() == 300, "B lists fewer than 300");
        // Let A learn that B exists (the first message from it).
        std::fs::write(bdir.join("hello"), b"b")?;
        ensure!(
            std::fs::read(adir.join("hello"))? == b"b",
            "A does not read B's write at once"
        );
        let m0 = mount_status(a)?;
        let mb0 = mount_status(b)?;
        let lister = {
            let dir = adir.clone();
            Loop::start(move || {
                let out = Command::new("ls").arg("-l").arg(&dir).output()?;
                // A name the loop below removed between the listing and
                // its `lstat` is `ENOENT` (`ls` exits 1 for it): a race
                // of the test's own, not a failure.
                let stderr = String::from_utf8_lossy(&out.stderr);
                let vanished = !stderr.trim().is_empty()
                    && stderr
                        .lines()
                        .all(|l| l.ends_with("No such file or directory"));
                ensure!(
                    out.status.success() || (out.status.code() == Some(1) && vanished),
                    "ls -l failed: {out:?}"
                );
                Ok(())
            })
        };
        // cto-strict's visibility, under the listing loop: a close on one
        // node is visible to the very next open on the other, and so are
        // a new name and a removed one.
        let rounds = env_usize("READDIRPLUS_STRICT_ROUNDS", 40);
        for i in 0..rounds {
            let (w, r) = if i % 2 == 0 { (b, a) } else { (a, b) };
            let f = w.mnt.join("s").join("f000");
            let content = format!("round-{i}");
            std::fs::write(&f, &content)?;
            let got = std::fs::read_to_string(r.mnt.join("s/f000"))?;
            if got != content {
                // What the strict reads of both nodes did, to tell a
                // degraded or tailed read from a wrong position.
                // How long the reader's replica takes to show the write.
                let started = std::time::Instant::now();
                let mut seen = Vec::new();
                let mut caught_up = None;
                while started.elapsed() < Duration::from_secs(3) {
                    let again = std::fs::read_to_string(r.mnt.join("s/f000"))?;
                    if seen.last() != Some(&again) {
                        seen.push(again.clone());
                    }
                    if again == content {
                        caught_up = Some(started.elapsed());
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
                let cto = |c: &Client| {
                    c.control_status()
                        .map(|s| format!("cto {} session {}", s["cto"], s["session"]))
                        .unwrap_or_else(|e| format!("{e:#}"))
                };
                bail!(
                    "{} read {got:?} after {content:?} (round {i}); re-reads {seen:?}, \
                     right after {caught_up:?}; reader {}; writer {}",
                    r.name,
                    cto(r),
                    cto(w)
                );
            }
            let n = w.mnt.join("s").join(format!("new-{i}"));
            std::fs::write(&n, b"n")?;
            ensure!(
                std::fs::symlink_metadata(r.mnt.join("s").join(format!("new-{i}"))).is_ok(),
                "{} does not see the new name at once",
                r.name
            );
            std::fs::remove_file(&n)?;
            ensure!(
                std::fs::symlink_metadata(r.mnt.join("s").join(format!("new-{i}"))).is_err(),
                "{} still sees the removed name",
                r.name
            );
        }
        let looped = lister.finish().context("A's ls -l loop")?;
        let m1 = mount_status(a)?;
        let mb1 = mount_status(b)?;
        let dn = num(&m1, "readdirplus_name_only") - num(&m0, "readdirplus_name_only");
        let de = num(&m1, "readdirplus_entries") - num(&m0, "readdirplus_entries");
        let dbe = num(&mb1, "readdirplus_entries") - num(&mb0, "readdirplus_entries");
        eprintln!(
            "    {NAME}: with B in the cluster A listed {looped} times: name-only +{dn}, with \
             attributes +{de} (B +{dbe}); reasons {}",
            why(&m1)
        );
        ensure!(looped > 0, "the lister never ran");
        ensure!(dn > 0, "no name-only plus entry under strict: {m1}");
        ensure!(
            de == 0,
            "{de} plus entries carried attributes under strict: {m1}"
        );
        ensure!(dbe == 0, "B's plus entries carried attributes under strict");
        ensure!(
            num(&m1["readdirplus_name_only_why"], "view") > 0,
            "strict's name-only entries are not counted as `view`: {m1}"
        );
        Ok(())
    })();
    if r.is_ok() {
        // The plan's last step: the second node leaves and the sequencer,
        // restarted, is lone again. A lone strict kernel cache also needs
        // the lease held (`reads_locally`), which a restarted node takes
        // with its first write.
        let r2 = (|| -> Result<()> {
            b.leave(None, false)?;
            fx.clients[0].unmount()?;
            fx.clients[0].mount()?;
            let a = &fx.clients[0];
            let adir = a.mnt.join("s");
            std::fs::write(adir.join("lone"), b"a")?;
            let mut last = String::new();
            let m0 = mount_status(a)?;
            let ok = eventually(
                "the restarted sequencer hands attributes again",
                Duration::from_secs(60),
                || {
                    let m = mount_status(a)?;
                    let out = Command::new("ls").arg("-l").arg(&adir).output()?;
                    ensure!(out.status.success(), "ls -l failed");
                    let m2 = mount_status(a)?;
                    let de = num(&m2, "readdirplus_entries") - num(&m, "readdirplus_entries");
                    last = format!("+{de} with attributes, name-only {}", why(&m2));
                    ensure!(de >= 300, "{last}");
                    Ok(())
                },
            );
            eprintln!(
                "    {NAME}: after the second node left and A restarted: {last} (readdirplus_entries {} before)",
                num(&m0, "readdirplus_entries")
            );
            ok
        })();
        let _ = b.unmount();
        fx.unmount_all();
        return r2;
    }
    let _ = b.unmount();
    fx.unmount_all();
    r
}

// ---------------------------------------------------- readdirplus-handover

/// `daemon --upgrade` with plus references, `ls -lR` loops and a listing
/// paused mid-directory with its stream open.
pub fn handover(_seed: u64) -> Result<()> {
    const NAME: &str = "readdirplus-handover";
    let (env, root) = setup(NAME)?;
    let _proxy = env.s3_proxy()?;
    let mut c = super::handover::dev_fuse_client(&env, root.path(), &format!("{NAME}-{}", ts()))?;
    let r = (|| -> Result<()> {
        let tree = c.mnt.join("t");
        let subdirs = env_usize("READDIRPLUS_HANDOVER_DIRS", 40);
        let per = env_usize("READDIRPLUS_HANDOVER_FILES", 25);
        for d in 0..subdirs {
            let dir = tree.join(format!("d{d:03}"));
            std::fs::create_dir_all(&dir)?;
            for f in 0..per {
                std::fs::write(dir.join(format!("f{f:03}")), format!("{d}/{f}"))?;
            }
        }
        // Part 1: references from plus pages. After a recursive listing's
        // lookups have expired, `ls -lR` hands the kernel a reference to
        // every name; hold some, and the count crosses the handover.
        std::thread::sleep(Duration::from_millis(1500));
        let out = Command::new("ls").arg("-lR").arg(&tree).output()?;
        ensure!(out.status.success(), "ls -lR failed");
        let mut held = Vec::new();
        for d in 0..subdirs {
            for f in (0..per).step_by(3) {
                held.push(PathFd::open(
                    &tree.join(format!("d{d:03}")).join(format!("f{f:03}")),
                )?);
            }
        }
        let m0 = mount_status(&c)?;
        let (refs0, plus0) = (num(&m0, "kernel_refs"), num(&m0, "readdirplus_entries"));
        ensure!(plus0 > 0, "ls -lR used no plus: {m0}");
        let took = super::handover::upgrade(&c)?;
        let m1 = mount_status(&c)?;
        let forgot = num(&m1, "forgets_final");
        eprintln!(
            "    {NAME}: quiet upgrade {took:?}: kernel_refs {refs0} -> {} (+{forgot} forgotten by the new image)",
            num(&m1, "kernel_refs")
        );
        ensure!(
            num(&m1, "kernel_refs") + forgot == refs0,
            "kernel_refs after the resume is {} with {forgot} forgets read since; before it was {refs0}",
            num(&m1, "kernel_refs")
        );
        for fd in &held {
            fd.fstat().context("a held descriptor after the upgrade")?;
        }

        // Part 2: two more upgrades under `ls -lR` loops and descriptor
        // checks; then unlink everything held: nlink 0, never ESTALE, and
        // the orphans go once the descriptors are closed.
        let listers: Vec<Loop> = (0..2)
            .map(|_| {
                let tree = tree.clone();
                Loop::start(move || {
                    let out = Command::new("ls").arg("-lR").arg(&tree).output()?;
                    ensure!(
                        out.status.success(),
                        "ls -lR failed across the handover ({}): {}",
                        out.status,
                        String::from_utf8_lossy(&out.stderr)
                    );
                    Ok(())
                })
            })
            .collect();
        for round in 0..2 {
            std::thread::sleep(Duration::from_millis(700));
            let took = super::handover::upgrade(&c)?;
            eprintln!("    {NAME}: upgrade {round} under ls -lR: {took:?}");
            for fd in &held {
                fd.fstat()
                    .with_context(|| format!("a held descriptor after upgrade {round}"))?;
            }
        }
        let mut looped = 0;
        for l in listers {
            looped += l.finish()?;
        }
        eprintln!("    {NAME}: ls -lR loops completed {looped} listings without error");
        for d in 0..subdirs {
            for f in (0..per).step_by(3) {
                std::fs::remove_file(tree.join(format!("d{d:03}")).join(format!("f{f:03}")))?;
            }
        }
        for fd in &held {
            let st = fd.fstat().context("a held descriptor after the unlinks")?;
            ensure!(st.st_nlink == 0, "nlink {} after unlink", st.st_nlink);
        }
        ensure!(
            num(&mount_status(&c)?, "unlinked_refs") > 0,
            "no orphan is kept for the held descriptors"
        );
        super::handover::upgrade(&c)?;
        for fd in &held {
            let st = fd
                .fstat()
                .context("an orphan's descriptor after an upgrade")?;
            ensure!(st.st_nlink == 0, "orphan nlink {}", st.st_nlink);
        }
        drop(held);
        eventually(
            "the orphans are reaped after their forgets",
            Duration::from_secs(60),
            || {
                let n = num(&mount_status(&c)?, "unlinked_refs");
                ensure!(n == 0, "unlinked_refs {n}");
                Ok(())
            },
        )?;

        // Part 3: a listing paused mid-directory, its stream open. The
        // kernel resumes it at the last `d_off` it was given, the cookie of
        // the last name of that page; that name is deleted before the
        // upgrade, so only the pinned page (which crosses the handover)
        // can say where to go on.
        let big = c.mnt.join("big");
        std::fs::create_dir(&big)?;
        let entries = env_usize("READDIRPLUS_HANDOVER_ENTRIES", 6000);
        fill(&big, 0..entries, 8, entry_name)?;
        let mut stream = RawDir::open(&big)?;
        let mut seen: Vec<String> = Vec::new();
        let mut last = None;
        while seen.len() < entries / 3 {
            let b = stream.batch(8192)?;
            ensure!(!b.is_empty(), "the stream ended early");
            last = b.last().map(|e| e.0.clone());
            seen.extend(b.into_iter().map(|e| e.0));
        }
        let victim = last.context("no entry read")?;
        let before = seen.len();
        std::fs::remove_file(big.join(&victim))?;
        let cold0 = num(&readdir_status(&c)?, "cold_scans");
        let took = super::handover::upgrade(&c)?;
        loop {
            let b = stream
                .batch(8192)
                .context("completing the paused listing")?;
            if b.is_empty() {
                break;
            }
            seen.extend(b.into_iter().map(|e| e.0));
        }
        let cold1 = num(&readdir_status(&c)?, "cold_scans");
        eprintln!(
            "    {NAME}: paused listing: {before} names before, {} after the upgrade ({took:?}); \
             its resume name {victim} deleted meanwhile; cold scans {cold0} -> {cold1}",
            seen.len() - before
        );
        let mut sorted = seen.clone();
        sorted.sort();
        sorted.dedup();
        ensure!(
            sorted.len() == seen.len(),
            "{} names repeated across the handover",
            seen.len() - sorted.len()
        );
        let mut want: Vec<String> = (0..entries).map(entry_name).collect();
        want.sort();
        ensure!(
            sorted == want,
            "the listing is not exact: {} names listed, {} expected",
            sorted.len(),
            want.len()
        );
        ensure!(
            cold1 == 0,
            "the resumed listing needed {cold1} cold scans: the pinned hints did not cross"
        );
        drop(stream);
        Ok(())
    })();
    let _ = c.unmount();
    r
}
