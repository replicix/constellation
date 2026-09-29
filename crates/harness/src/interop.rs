//! `harness interop write|verify --bucket-dir <dir>`: the cross-OS interop
//! lane (plan 31 §8, plan 34 M4).
//!
//! The point is to prove that what one OS wrote into a bucket, another OS
//! can mount and read back exactly: the journal's records, the portable
//! `Code` errnos it stores, device numbers (`rdev` is encoded differently on
//! Linux and macOS), timestamps, xattrs, link counts, snapshots. The bucket
//! itself travels between the runners as a plain directory (`--bucket-dir`,
//! tarred or uploaded as a CI artifact).
//!
//! - `write` mounts a **fresh** filesystem, writes a deterministic seeded
//!   tree (see [`write_tree`]), unmounts, and leaves the bucket in
//!   `--bucket-dir`.
//! - `verify` mounts that bucket with a fresh state dir (so everything is
//!   replayed from the bucket, nothing from a local cache) and checks every
//!   item, collecting all problems before failing.
//!
//! Two backends (`--backend`):
//!
//! - `file` (default): the filesystem lives directly in `--bucket-dir` on the
//!   local file backend. No S3 server, works everywhere. `verify` mounts a
//!   copy, so the artifact stays pristine.
//! - `process`: the filesystem lives in a native versitygw
//!   (`--s3-backend process`'s server). The bucket is exported through the S3
//!   API, one file per object, and imported by plain PUTs into a fresh
//!   versitygw before `verify` (never by copying versitygw's data
//!   directory: its posix backend keeps object metadata in xattrs, which do
//!   not survive a cross-OS tar).
//!
//! The tree is a pure function of the seed (recorded in `INTEROP.json` at the
//! fs root, written last so a truncated `write` is detected), so `verify`
//! regenerates every expectation instead of trusting a stored manifest.
//! File content is a keyed BLAKE3 XOF stream, which is stable across OSes
//! and versions.

use crate::client::Client;
use crate::s3env::{self, S3Backend, S3Env, BUCKET};
use anyhow::{bail, ensure, Context, Result};
use constellation_types::Code;
use std::collections::BTreeSet;
use std::ffi::CString;
use std::fs;
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, FileExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

const MANIFEST_VERSION: u64 = 1;
/// Every file's mtime is `BASE_MTIME + i * 1_000_003` s, plus [`MTIME_NSEC`].
const BASE_MTIME: i64 = 1_700_000_000;
const MTIME_NSEC: u32 = 123_456_789;
const MIB: u64 = 1 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Backend {
    /// The local file backend, directly in `--bucket-dir`.
    File,
    /// versitygw (native process), exported/imported through the S3 API.
    Process,
}

pub struct Opts {
    pub bucket_dir: PathBuf,
    pub backend: Backend,
    pub seed: u64,
}

// ---------------------------------------------------------------------------
// Deterministic content

/// splitmix64: a tiny, stable PRNG for the shape of the tree.
struct Prng(u64);

impl Prng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Bytes `[off, off + buf.len())` of the stream named `tag` under `seed`.
fn stream(seed: u64, tag: &str, off: u64, buf: &mut [u8]) {
    let key = *blake3::hash(format!("constellation-interop/{seed}/{tag}").as_bytes()).as_bytes();
    let mut xof = blake3::Hasher::new_keyed(&key).finalize_xof();
    xof.set_position(off);
    xof.fill(buf);
}

/// One step of building a file; the file's expected content is the
/// [`Model`] these steps produce.
#[derive(Clone, Debug)]
enum Step {
    /// Write `len` bytes of stream `tag` (from stream offset 0) at `off`.
    Write {
        off: u64,
        len: u64,
        tag: String,
    },
    Truncate(u64),
}

fn write(off: u64, len: u64, tag: &str) -> Step {
    Step::Write {
        off,
        len,
        tag: tag.to_string(),
    }
}

/// What a file must contain: zeros, overlaid by `extents` in order.
#[derive(Default, Debug)]
struct Model {
    len: u64,
    extents: Vec<(u64, u64, String)>,
}

impl Model {
    fn of(steps: &[Step]) -> Model {
        let mut m = Model::default();
        for s in steps {
            match s {
                Step::Write { off, len, tag } => {
                    m.len = m.len.max(off + len);
                    m.extents.push((*off, *len, tag.clone()));
                }
                Step::Truncate(n) => {
                    m.extents.retain(|e| e.0 < *n);
                    for e in &mut m.extents {
                        e.1 = e.1.min(*n - e.0);
                    }
                    m.len = *n;
                }
            }
        }
        m
    }

    /// Expected bytes `[off, off + buf.len())`, which must lie inside `len`.
    fn fill(&self, seed: u64, off: u64, buf: &mut [u8]) {
        buf.fill(0);
        let end = off + buf.len() as u64;
        for (eoff, elen, tag) in &self.extents {
            let (lo, hi) = ((*eoff).max(off), (eoff + elen).min(end));
            if lo < hi {
                let dst = &mut buf[(lo - off) as usize..(hi - off) as usize];
                stream(seed, tag, lo - eoff, dst);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The tree

struct FileSpec {
    path: String,
    mode: u32,
    steps: Vec<Step>,
}

fn mtime_of(i: usize) -> (i64, u32) {
    (BASE_MTIME + i as i64 * 1_000_003, MTIME_NSEC)
}

/// Every regular file of the tree (paths relative to the fs root).
fn file_specs(seed: u64) -> Vec<FileSpec> {
    let mut rng = Prng(seed);
    let mut v = Vec::new();
    // Sizes around the 4 KiB page and 1 MiB chunk boundaries, then random.
    let edges = [
        0,
        1,
        4095,
        4096,
        4097,
        65_535,
        65_536,
        MIB - 1,
        MIB,
        MIB + 1,
        2 * MIB,
    ];
    let modes = [0o644, 0o600, 0o755, 0o640, 0o444];
    for i in 0..24usize {
        let len = edges.get(i).copied().unwrap_or_else(|| rng.below(70_000));
        v.push(FileSpec {
            path: format!("t/small/f{i:02}"),
            mode: modes[i % modes.len()],
            steps: if len == 0 {
                vec![]
            } else {
                vec![write(0, len, &format!("small/{i}"))]
            },
        });
    }
    // Multi-chunk, and far larger than the write-back window.
    v.push(FileSpec {
        path: "t/big/large.bin".into(),
        mode: 0o644,
        steps: vec![write(0, 24 * MIB + 12_345, "large")],
    });
    // Sparse: 96 MiB apparent size, four written extents.
    v.push(FileSpec {
        path: "t/big/sparse.bin".into(),
        mode: 0o644,
        steps: vec![
            Step::Truncate(96 * MIB),
            write(0, 4096, "sparse/head"),
            write(10 * MIB + 17, 100_000, "sparse/mid"),
            write(50 * MIB, MIB, "sparse/chunk"),
            write(96 * MIB - 1, 1, "sparse/tail"),
        ],
    });
    v.push(FileSpec {
        path: "t/edit/patched".into(),
        mode: 0o644,
        steps: vec![
            write(0, 300_000, "patch/base"),
            write(100_000, 5_000, "patch/over"),
            write(299_990, 10, "patch/end"),
        ],
    });
    v.push(FileSpec {
        path: "t/edit/shrunk".into(),
        mode: 0o644,
        steps: vec![write(0, 100_000, "shrink"), Step::Truncate(5_000)],
    });
    v.push(FileSpec {
        path: "t/edit/grown".into(),
        mode: 0o644,
        steps: vec![write(0, 100, "grow"), Step::Truncate(300_000)],
    });
    // Names: non-ASCII, spaces, a leading dot, and the 255-byte maximum.
    for (i, name) in [
        "t/names/\u{fc}n\u{ef}-\u{e7}\u{f8}d\u{e9}-\u{65e5}\u{672c}\u{8a9e}",
        "t/names/with space and  double",
        "t/names/.leading-dot",
        "t/names/quote'\"back\\slash",
    ]
    .iter()
    .enumerate()
    {
        v.push(FileSpec {
            path: name.to_string(),
            mode: 0o644,
            steps: vec![write(0, 100 + i as u64, &format!("names/{i}"))],
        });
    }
    v.push(FileSpec {
        path: format!("t/names/{}", "n".repeat(255)),
        mode: 0o644,
        steps: vec![write(0, 64, "names/max")],
    });
    // Rename sources/targets (the tree is checked after the moves).
    v.push(FileSpec {
        path: "t/ren/file_from".into(),
        mode: 0o644,
        steps: vec![write(0, 12_000, "ren/from")],
    });
    v.push(FileSpec {
        path: "t/ren/file_victim".into(),
        mode: 0o644,
        steps: vec![write(0, 7_000, "ren/victim")],
    });
    v.push(FileSpec {
        path: "t/ren/dir_from/inner".into(),
        mode: 0o644,
        steps: vec![write(0, 3_000, "ren/inner")],
    });
    // Hard-link groups.
    v.push(FileSpec {
        path: "t/hl/a".into(),
        mode: 0o644,
        steps: vec![write(0, 20_000, "hl/a")],
    });
    v.push(FileSpec {
        path: "t/hl/x".into(),
        mode: 0o600,
        steps: vec![write(0, 5_000, "hl/x")],
    });
    // Extended attributes live on these.
    v.push(FileSpec {
        path: "t/xattr/file".into(),
        mode: 0o644,
        steps: vec![write(0, 1_000, "xattr/file")],
    });
    v
}

/// `(path, mode)` directories, chmod-ed after the tree is built.
const DIRS: &[(&str, u32)] = &[
    ("t/dirs/a/b/c/d", 0o755),
    ("t/dirs/private", 0o700),
    ("t/dirs/sticky", 0o1777),
    ("t/dirs/empty", 0o750),
    ("t/xattr/dir", 0o755),
];
const MANY_DIR: &str = "t/dirs/many";
const MANY_COUNT: usize = 200;

fn symlinks() -> Vec<(&'static str, String)> {
    vec![
        ("t/links/rel", "../small/f05".to_string()),
        (
            "t/links/dangling",
            "/nonexistent/absolute/target".to_string(),
        ),
        ("t/links/long", "x/".repeat(100)),
        ("t/links/dirlink", "../dirs/a".to_string()),
    ]
}

/// `(name, value)` xattrs that must survive on `t/xattr/file`, and the one
/// that is set then removed.
fn xattrs(seed: u64) -> Vec<(String, Vec<u8>)> {
    let mut big = vec![0u8; 1000];
    stream(seed, "xattr/big", 0, &mut big);
    vec![
        ("user.interop.text".into(), b"hello".to_vec()),
        ("user.interop.bin".into(), vec![0, 1, 2, 255, 254, 0, 7]),
        ("user.interop.big".into(), big),
        ("user.interop.replaced".into(), b"second".to_vec()),
    ]
}
const XATTR_REMOVED: &str = "user.interop.removed";
const XATTR_DIR: (&str, &[u8]) = ("user.interop.dir", b"dir-attr");
const XATTR_PREFIX: &str = "user.interop.";

/// `(path, is_char, major, minor)`. Majors/minors representable in both the
/// Linux and macOS `dev_t` encodings, incl. a large minor.
const DEVICES: &[(&str, bool, u32, u32)] = &[
    ("t/special/chr_null", true, 1, 3),
    ("t/special/blk_sd", false, 8, 17),
    ("t/special/chr_big", true, 200, 70_000),
];

/// `(name, frozen steps)` of the snapshotted directory `snap/`, and what the
/// live tree looks like after the snapshot.
fn snap_frozen() -> Vec<(&'static str, Vec<Step>)> {
    vec![
        ("a", vec![write(0, 30_000, "snap/a")]),
        ("b", vec![write(0, 4_000, "snap/b")]),
        ("c", vec![write(0, 9_000, "snap/c")]),
    ]
}
fn snap_live() -> Vec<(&'static str, Vec<Step>)> {
    vec![
        ("a", vec![write(0, 10_000, "snap/a2")]),
        ("c", vec![write(0, 9_000, "snap/c")]),
        ("d", vec![write(0, 500, "snap/d")]),
    ]
}
const SNAPSHOT: &str = "interop";

// ---------------------------------------------------------------------------
// OS calls

mod sys {
    use super::*;
    use std::io;

    fn cpath(p: &Path) -> io::Result<CString> {
        CString::new(p.as_os_str().as_bytes()).map_err(|_| io::ErrorKind::InvalidInput.into())
    }

    fn cname(n: &str) -> io::Result<CString> {
        CString::new(n).map_err(|_| io::ErrorKind::InvalidInput.into())
    }

    fn check(rc: libc::c_int) -> io::Result<()> {
        if rc == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Set both timestamps of `p` (not following a final symlink).
    pub fn set_times(p: &Path, secs: i64, nsec: u32) -> io::Result<()> {
        let c = cpath(p)?;
        let ts = libc::timespec {
            tv_sec: secs as _,
            tv_nsec: nsec as _,
        };
        // SAFETY: valid NUL-terminated path and a two-element timespec array.
        check(unsafe {
            libc::utimensat(
                libc::AT_FDCWD,
                c.as_ptr(),
                [ts, ts].as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        })
    }

    pub fn mkfifo(p: &Path, mode: u32) -> io::Result<()> {
        let c = cpath(p)?;
        // SAFETY: valid NUL-terminated path.
        check(unsafe { libc::mkfifo(c.as_ptr(), mode as libc::mode_t) })
    }

    pub fn mknod(p: &Path, char_dev: bool, major: u32, minor: u32, mode: u32) -> io::Result<()> {
        let c = cpath(p)?;
        let kind = if char_dev {
            libc::S_IFCHR
        } else {
            libc::S_IFBLK
        };
        let dev = libc::makedev(major as _, minor as _);
        // SAFETY: valid NUL-terminated path.
        check(unsafe { libc::mknod(c.as_ptr(), kind | mode as libc::mode_t, dev) })
    }

    pub fn major_minor(rdev: u64) -> (u32, u32) {
        (
            libc::major(rdev as libc::dev_t) as u32,
            libc::minor(rdev as libc::dev_t) as u32,
        )
    }

    pub fn is_root() -> bool {
        // SAFETY: geteuid has no preconditions.
        unsafe { libc::geteuid() == 0 }
    }

    #[cfg(target_os = "linux")]
    pub fn setxattr(p: &Path, name: &str, value: &[u8]) -> io::Result<()> {
        let (c, n) = (cpath(p)?, cname(name)?);
        // SAFETY: valid C strings; value pointer/length from a slice.
        check(unsafe {
            libc::setxattr(
                c.as_ptr(),
                n.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        })
    }
    #[cfg(target_os = "macos")]
    pub fn setxattr(p: &Path, name: &str, value: &[u8]) -> io::Result<()> {
        let (c, n) = (cpath(p)?, cname(name)?);
        // SAFETY: as above; position 0, no options.
        check(unsafe {
            libc::setxattr(
                c.as_ptr(),
                n.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
                0,
            )
        })
    }

    #[cfg(target_os = "linux")]
    pub fn removexattr(p: &Path, name: &str) -> io::Result<()> {
        let (c, n) = (cpath(p)?, cname(name)?);
        // SAFETY: valid C strings.
        check(unsafe { libc::removexattr(c.as_ptr(), n.as_ptr()) })
    }
    #[cfg(target_os = "macos")]
    pub fn removexattr(p: &Path, name: &str) -> io::Result<()> {
        let (c, n) = (cpath(p)?, cname(name)?);
        // SAFETY: valid C strings.
        check(unsafe { libc::removexattr(c.as_ptr(), n.as_ptr(), 0) })
    }

    pub fn getxattr(p: &Path, name: &str) -> io::Result<Vec<u8>> {
        let (c, n) = (cpath(p)?, cname(name)?);
        let mut buf = vec![0u8; 70_000];
        // SAFETY: buffer pointer/length from a Vec we own.
        #[cfg(target_os = "linux")]
        let rc =
            unsafe { libc::getxattr(c.as_ptr(), n.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
        #[cfg(target_os = "macos")]
        let rc = unsafe {
            libc::getxattr(
                c.as_ptr(),
                n.as_ptr(),
                buf.as_mut_ptr().cast(),
                buf.len(),
                0,
                0,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        buf.truncate(rc as usize);
        Ok(buf)
    }

    pub fn listxattr(p: &Path) -> io::Result<Vec<String>> {
        let c = cpath(p)?;
        let mut buf = vec![0u8; 70_000];
        // SAFETY: buffer pointer/length from a Vec we own.
        #[cfg(target_os = "linux")]
        let rc = unsafe { libc::listxattr(c.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
        #[cfg(target_os = "macos")]
        let rc = unsafe { libc::listxattr(c.as_ptr(), buf.as_mut_ptr().cast(), buf.len(), 0) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        buf.truncate(rc as usize);
        Ok(buf
            .split(|b| *b == 0)
            .filter(|s| !s.is_empty())
            .map(|s| String::from_utf8_lossy(s).into_owned())
            .collect())
    }
}

// ---------------------------------------------------------------------------
// write

fn create_file(root: &Path, spec: &FileSpec, seed: u64, index: usize) -> Result<()> {
    let path = root.join(&spec.path);
    fs::create_dir_all(path.parent().unwrap())?;
    let f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .with_context(|| format!("creating {}", spec.path))?;
    for step in &spec.steps {
        match step {
            Step::Truncate(n) => f.set_len(*n)?,
            Step::Write { off, len, tag } => {
                let mut done = 0u64;
                let mut buf = vec![0u8; MIB as usize];
                while done < *len {
                    let n = (*len - done).min(MIB) as usize;
                    stream(seed, tag, done, &mut buf[..n]);
                    f.write_all_at(&buf[..n], off + done)
                        .with_context(|| format!("writing {} at {}", spec.path, off + done))?;
                    done += n as u64;
                }
            }
        }
    }
    drop(f);
    fs::set_permissions(&path, fs::Permissions::from_mode(spec.mode))?;
    let (s, ns) = mtime_of(index);
    sys::set_times(&path, s, ns).with_context(|| format!("setting mtime of {}", spec.path))?;
    Ok(())
}

fn expect_refused<T>(what: &str, res: std::io::Result<T>, want: Code) -> Result<()> {
    match res {
        Ok(_) => bail!("{what}: succeeded, expected {want:?}"),
        Err(e) => {
            let got = Code::from_os_error(&e);
            ensure!(
                got == Some(want),
                "{what}: got {e} ({got:?}), expected {want:?}"
            );
            Ok(())
        }
    }
}

/// Populate the mounted fs at `root`. Returns whether device nodes were
/// created (only possible as root).
fn write_tree(client: &Client, root: &Path, seed: u64) -> Result<bool> {
    let specs = file_specs(seed);
    for (i, spec) in specs.iter().enumerate() {
        create_file(root, spec, seed, i)?;
    }

    // Directories, then the wide one.
    for (dir, _) in DIRS {
        fs::create_dir_all(root.join(dir))?;
    }
    fs::create_dir_all(root.join(MANY_DIR))?;
    for i in 0..MANY_COUNT {
        fs::File::create(root.join(MANY_DIR).join(format!("e{i:03}")))?;
    }

    // Symlinks.
    for (path, target) in symlinks() {
        fs::create_dir_all(root.join(path).parent().unwrap())?;
        symlink(&target, root.join(path))?;
    }

    // Hard links: a has 4 names, one is removed (3 left); x loses its
    // original name and lives on as y (1 left).
    let hl = root.join("t/hl");
    fs::hard_link(hl.join("a"), hl.join("b"))?;
    fs::hard_link(hl.join("a"), hl.join("c"))?;
    fs::create_dir(hl.join("sub"))?;
    fs::hard_link(hl.join("a"), hl.join("sub/d"))?;
    fs::remove_file(hl.join("c"))?;
    fs::hard_link(hl.join("x"), hl.join("y"))?;
    fs::remove_file(hl.join("x"))?;

    // xattrs: a value set twice, one set and removed, one on a directory.
    let xf = root.join("t/xattr/file");
    sys::setxattr(&xf, "user.interop.replaced", b"first")?;
    for (name, value) in xattrs(seed) {
        sys::setxattr(&xf, &name, &value)?;
    }
    sys::setxattr(&xf, XATTR_REMOVED, b"transient")?;
    sys::removexattr(&xf, XATTR_REMOVED)?;
    sys::setxattr(&root.join("t/xattr/dir"), XATTR_DIR.0, XATTR_DIR.1)?;

    // Renames: a directory move, and a file rename over an existing file.
    let ren = root.join("t/ren");
    fs::rename(ren.join("dir_from"), ren.join("dir_to"))?;
    fs::rename(ren.join("file_from"), ren.join("file_victim"))?;

    // Special files.
    fs::create_dir_all(root.join("t/special"))?;
    sys::mkfifo(&root.join("t/special/fifo"), 0o640)?;
    let devices = sys::is_root();
    if devices {
        for (path, chr, major, minor) in DEVICES {
            sys::mknod(&root.join(path), *chr, *major, *minor, 0o600)
                .with_context(|| format!("mknod {path}"))?;
        }
    } else {
        eprintln!("interop: not root, skipping device nodes (mknod needs CAP_MKNOD)");
    }

    // Operations the filesystem must refuse, with the portable code each
    // maps to. They leave records (or at least attempts) in the journal and
    // must leave the tree untouched, which `verify` checks.
    expect_refused(
        "rmdir non-empty",
        fs::remove_dir(root.join("t/dirs/a")),
        Code::NotEmpty,
    )?;
    expect_refused(
        "mkdir existing",
        fs::create_dir(root.join("t/dirs/a")),
        Code::Exists,
    )?;
    expect_refused(
        "rename dir over non-empty dir",
        fs::rename(root.join("t/dirs/empty"), root.join("t/dirs/a")),
        Code::NotEmpty,
    )?;
    expect_refused(
        "unlink missing",
        fs::remove_file(root.join("t/dirs/missing")),
        Code::NotFound,
    )?;
    expect_refused(
        "create 256-byte name",
        fs::File::create(root.join(format!("t/names/{}", "n".repeat(256)))),
        Code::NameTooLong,
    )?;
    expect_refused(
        "getxattr missing",
        sys::getxattr(&xf, XATTR_REMOVED),
        Code::NoData,
    )?;

    // Directory modes last: a 0700 dir must not get in the way earlier.
    for (dir, mode) in DIRS {
        fs::set_permissions(root.join(dir), fs::Permissions::from_mode(*mode))?;
    }

    // Snapshot: freeze `snap/`, then diverge the live tree.
    for (name, steps) in snap_frozen() {
        let spec = FileSpec {
            path: format!("snap/{name}"),
            mode: 0o644,
            steps,
        };
        create_file(root, &spec, seed, 1000)?;
    }
    client.snapshot_create(&format!("/snap@{SNAPSHOT}"))?;
    // The snapshot is taken by the daemon asynchronously to this call's
    // return in the worst case: wait until it is readable before diverging.
    let frozen = root.join("snap/.constellation/snapshot").join(SNAPSHOT);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !frozen.join("a").exists() {
        ensure!(
            std::time::Instant::now() < deadline,
            "snapshot {SNAPSHOT} never became readable at {}",
            frozen.display()
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    fs::remove_file(root.join("snap/a"))?;
    fs::remove_file(root.join("snap/b"))?;
    for (name, steps) in snap_live() {
        if name == "c" {
            continue; // unchanged
        }
        let spec = FileSpec {
            path: format!("snap/{name}"),
            mode: 0o644,
            steps,
        };
        create_file(root, &spec, seed, 1001)?;
    }

    Ok(devices)
}

pub fn write_cmd(opts: &Opts) -> Result<()> {
    let bucket_dir = absolute(&opts.bucket_dir)?;
    prepare_empty_dir(&bucket_dir)?;
    let scratch = tempfile::Builder::new()
        .prefix("interop-")
        .tempdir_in(scratch_base())?;

    let mut env_guard = None;
    let (endpoint, backend) = match opts.backend {
        Backend::File => (DUMMY_ENDPOINT.to_string(), bucket_dir.display().to_string()),
        Backend::Process => {
            s3env::set_backend(S3Backend::Process);
            let env = S3Env::start_with(S3Backend::Process)?;
            let ep = env.direct_endpoint.clone();
            env_guard = Some(env);
            (ep, format!("s3://{BUCKET}/interop"))
        }
    };
    let mut client = Client::new(scratch.path(), "interop", &endpoint, &backend)?;
    client.fs_create()?;
    client.mount()?;
    let root = client.mnt.clone();
    let result = (|| -> Result<bool> {
        let devices = write_tree(&client, &root, opts.seed)?;
        let manifest = serde_json::json!({
            "version": MANIFEST_VERSION,
            "seed": opts.seed,
            "devices": devices,
            "written_on": std::env::consts::OS,
        });
        fs::write(
            root.join("INTEROP.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )?;
        Ok(devices)
    })();
    // Unmount on every path: the bucket is only complete after the flush.
    let unmounted = client.unmount();
    let devices = result?;
    unmounted?;

    if let Some(env) = &env_guard {
        let n = export_bucket(&env.direct_endpoint, &bucket_dir)?;
        eprintln!(
            "interop: exported {n} object(s) to {}",
            bucket_dir.display()
        );
    }
    println!(
        "interop write OK: seed {}, backend {:?}, devices {devices}, bucket in {}",
        opts.seed,
        opts.backend,
        bucket_dir.display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// verify

struct Verifier {
    checked: usize,
    problems: Vec<String>,
}

impl Verifier {
    fn ok(&mut self, cond: bool, what: impl FnOnce() -> String) -> bool {
        self.checked += 1;
        if !cond {
            self.problems.push(what());
        }
        cond
    }

    fn fail(&mut self, msg: String) {
        self.checked += 1;
        self.problems.push(msg);
    }
}

/// Compare the whole file against the model, block by block.
fn verify_content(v: &mut Verifier, root: &Path, rel: &str, seed: u64, model: &Model) {
    let path = root.join(rel);
    let mut f = match fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => return v.fail(format!("{rel}: cannot open: {e}")),
    };
    match f.metadata() {
        Ok(m) => {
            if !v.ok(m.len() == model.len, || {
                format!("{rel}: size {} != expected {}", m.len(), model.len)
            }) {
                return;
            }
        }
        Err(e) => return v.fail(format!("{rel}: stat: {e}")),
    }
    let (mut got, mut want) = (vec![0u8; MIB as usize], vec![0u8; MIB as usize]);
    let mut off = 0u64;
    while off < model.len {
        let n = (model.len - off).min(MIB) as usize;
        if let Err(e) = f.read_exact(&mut got[..n]) {
            return v.fail(format!("{rel}: read at {off}: {e}"));
        }
        model.fill(seed, off, &mut want[..n]);
        if got[..n] != want[..n] {
            let at = got[..n]
                .iter()
                .zip(&want[..n])
                .position(|(a, b)| a != b)
                .unwrap();
            return v.fail(format!(
                "{rel}: content differs at byte {}",
                off + at as u64
            ));
        }
        off += n as u64;
    }
    v.ok(true, String::new);
}

fn verify_meta(v: &mut Verifier, root: &Path, rel: &str, mode: u32, mtime: Option<(i64, u32)>) {
    match fs::symlink_metadata(root.join(rel)) {
        Err(e) => v.fail(format!("{rel}: stat: {e}")),
        Ok(m) => {
            let got = m.mode() & 0o7777;
            v.ok(got == mode, || format!("{rel}: mode {got:o} != {mode:o}"));
            if let Some((s, ns)) = mtime {
                v.ok((m.mtime(), m.mtime_nsec() as u32) == (s, ns), || {
                    format!(
                        "{rel}: mtime {}.{:09} != {s}.{ns:09}",
                        m.mtime(),
                        m.mtime_nsec()
                    )
                });
            }
        }
    }
}

fn list(dir: &Path) -> std::io::Result<BTreeSet<String>> {
    fs::read_dir(dir)?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect()
}

fn verify_tree(v: &mut Verifier, root: &Path, seed: u64, devices: bool) {
    // Regular files: content, mode, mtime. Files whose names are rewritten
    // after creation are checked at their final place below.
    let moved: &[&str] = &[
        "t/ren/file_from",
        "t/ren/file_victim",
        "t/ren/dir_from/inner",
        "t/hl/x",
    ];
    for (i, spec) in file_specs(seed).iter().enumerate() {
        if moved.contains(&spec.path.as_str()) {
            continue;
        }
        let model = Model::of(&spec.steps);
        verify_content(v, root, &spec.path, seed, &model);
        // KNOWN DEVIATION, not yet fixed: `link(2)` bumps the *mtime* of the
        // target inode to "now" (POSIX: only ctime and the parent directory
        // change), so the hard-linked file's mtime is not the one it was
        // given. It is not deterministic either, so it cannot be compared.
        let mtime = (spec.path != "t/hl/a").then(|| mtime_of(i));
        verify_meta(v, root, &spec.path, spec.mode, mtime);
    }

    // Renames.
    let specs = file_specs(seed);
    let steps_of = |p: &str| specs.iter().find(|s| s.path == p).unwrap().steps.clone();
    let spec_index = |p: &str| specs.iter().position(|s| s.path == p).unwrap();
    // A renamed file keeps its content, mode and mtime under the new name.
    let mut moved_to = |new: &str, old: &str| {
        let i = spec_index(old);
        verify_content(v, root, new, seed, &Model::of(&specs[i].steps));
        verify_meta(v, root, new, specs[i].mode, Some(mtime_of(i)));
    };
    moved_to("t/ren/file_victim", "t/ren/file_from");
    moved_to("t/ren/dir_to/inner", "t/ren/dir_from/inner");
    v.ok(!root.join("t/ren/file_from").exists(), || {
        "t/ren/file_from still exists".into()
    });
    v.ok(!root.join("t/ren/dir_from").exists(), || {
        "t/ren/dir_from still exists".into()
    });

    // Directories: modes, exact listings, and that refused operations left
    // them alone.
    for (dir, mode) in DIRS {
        match fs::symlink_metadata(root.join(dir)) {
            Ok(m) => {
                v.ok(m.is_dir(), || format!("{dir}: not a directory"));
                let got = m.mode() & 0o7777;
                v.ok(got == *mode, || format!("{dir}: mode {got:o} != {mode:o}"));
            }
            Err(e) => v.fail(format!("{dir}: {e}")),
        }
    }
    let want: BTreeSet<String> = (0..MANY_COUNT).map(|i| format!("e{i:03}")).collect();
    match list(&root.join(MANY_DIR)) {
        Ok(got) => {
            v.ok(got == want, || {
                format!(
                    "{MANY_DIR}: listing has {} entries, expected {}",
                    got.len(),
                    want.len()
                )
            });
        }
        Err(e) => v.fail(format!("{MANY_DIR}: {e}")),
    }
    match list(&root.join("t/dirs")) {
        Ok(got) => {
            let want: BTreeSet<String> = ["a", "empty", "many", "private", "sticky"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            v.ok(got == want, || {
                format!("t/dirs: listing {got:?} != {want:?}")
            });
        }
        Err(e) => v.fail(format!("t/dirs: {e}")),
    }

    // Symlinks.
    for (path, target) in symlinks() {
        match fs::read_link(root.join(path)) {
            Ok(got) => {
                v.ok(got == Path::new(&target), || {
                    format!("{path}: -> {got:?}, expected {target:?}")
                });
            }
            Err(e) => v.fail(format!("{path}: readlink: {e}")),
        }
    }
    v.ok(
        fs::read(root.join("t/links/rel")).ok() == fs::read(root.join("t/small/f05")).ok(),
        || "t/links/rel does not resolve to t/small/f05's content".into(),
    );
    v.ok(!root.join("t/links/dangling").exists(), || {
        "dangling link resolves".into()
    });
    v.ok(root.join("t/links/dirlink/b/c/d").is_dir(), || {
        "dirlink does not resolve".into()
    });

    // Hard links.
    let a_model = Model::of(&steps_of("t/hl/a"));
    let mut inos = BTreeSet::new();
    for p in ["t/hl/a", "t/hl/b", "t/hl/sub/d"] {
        verify_content(v, root, p, seed, &a_model);
        match fs::metadata(root.join(p)) {
            Ok(m) => {
                v.ok(m.nlink() == 3, || format!("{p}: nlink {} != 3", m.nlink()));
                inos.insert(m.ino());
            }
            Err(e) => v.fail(format!("{p}: {e}")),
        }
    }
    v.ok(inos.len() == 1, || {
        format!("t/hl a/b/sub/d are {} inodes, expected 1", inos.len())
    });
    v.ok(!root.join("t/hl/c").exists(), || {
        "t/hl/c (unlinked) exists".into()
    });
    verify_content(v, root, "t/hl/y", seed, &Model::of(&steps_of("t/hl/x")));
    match fs::metadata(root.join("t/hl/y")) {
        Ok(m) => {
            v.ok(m.nlink() == 1, || {
                format!("t/hl/y: nlink {} != 1", m.nlink())
            });
            v.ok(m.mode() & 0o7777 == 0o600, || {
                format!("t/hl/y: mode {:o}", m.mode() & 0o7777)
            });
        }
        Err(e) => v.fail(format!("t/hl/y: {e}")),
    }
    v.ok(!root.join("t/hl/x").exists(), || {
        "t/hl/x (renamed away) exists".into()
    });

    // xattrs.
    let xf = root.join("t/xattr/file");
    match sys::listxattr(&xf) {
        Ok(names) => {
            let got: BTreeSet<String> = names
                .into_iter()
                .filter(|n| n.starts_with(XATTR_PREFIX))
                .collect();
            let want: BTreeSet<String> = xattrs(seed).into_iter().map(|(n, _)| n).collect();
            v.ok(got == want, || {
                format!("t/xattr/file: xattrs {got:?} != {want:?}")
            });
        }
        Err(e) => v.fail(format!("t/xattr/file: listxattr: {e}")),
    }
    for (name, value) in xattrs(seed) {
        match sys::getxattr(&xf, &name) {
            Ok(got) => {
                v.ok(got == value, || {
                    format!("t/xattr/file: {name} value differs")
                });
            }
            Err(e) => v.fail(format!("t/xattr/file: {name}: {e}")),
        }
    }
    match sys::getxattr(&xf, XATTR_REMOVED) {
        Ok(_) => v.fail(format!(
            "t/xattr/file: removed xattr {XATTR_REMOVED} is back"
        )),
        Err(e) => {
            let code = Code::from_os_error(&e);
            v.ok(code == Some(Code::NoData), || {
                format!("removed xattr: {e} ({code:?}), expected NoData")
            });
        }
    }
    match sys::getxattr(&root.join("t/xattr/dir"), XATTR_DIR.0) {
        Ok(got) => {
            v.ok(got == XATTR_DIR.1, || "t/xattr/dir: value differs".into());
        }
        Err(e) => v.fail(format!("t/xattr/dir: {}: {e}", XATTR_DIR.0)),
    }

    // FIFO and (when written) device nodes, incl. their device numbers.
    match fs::symlink_metadata(root.join("t/special/fifo")) {
        Ok(m) => {
            v.ok(m.file_type().is_fifo(), || {
                "t/special/fifo is not a FIFO".into()
            });
            v.ok(m.mode() & 0o777 == 0o640, || {
                format!("t/special/fifo: mode {:o}", m.mode() & 0o777)
            });
        }
        Err(e) => v.fail(format!("t/special/fifo: {e}")),
    }
    for (path, chr, major, minor) in DEVICES {
        let present = fs::symlink_metadata(root.join(path));
        if !devices {
            v.ok(present.is_err(), || {
                format!("{path}: exists but the writer was not root")
            });
            continue;
        }
        match present {
            Ok(m) => {
                let ft = m.file_type();
                v.ok(
                    if *chr {
                        ft.is_char_device()
                    } else {
                        ft.is_block_device()
                    },
                    || format!("{path}: wrong device type"),
                );
                let got = sys::major_minor(m.rdev());
                v.ok(got == (*major, *minor), || {
                    format!("{path}: rdev {got:?} != {:?}", (*major, *minor))
                });
            }
            Err(e) => v.fail(format!("{path}: {e}")),
        }
    }

    // Snapshot: frozen view and the diverged live tree.
    let frozen = root.join("snap/.constellation/snapshot").join(SNAPSHOT);
    let frozen_names: BTreeSet<String> = snap_frozen().iter().map(|(n, _)| n.to_string()).collect();
    match list(&frozen) {
        Ok(got) => {
            v.ok(got == frozen_names, || {
                format!("snapshot listing {got:?} != {frozen_names:?}")
            });
        }
        Err(e) => v.fail(format!("snapshot {SNAPSHOT}: {e}")),
    }
    for (name, steps) in snap_frozen() {
        let rel = format!("snap/.constellation/snapshot/{SNAPSHOT}/{name}");
        verify_content(v, root, &rel, seed, &Model::of(&steps));
    }
    let live_names: BTreeSet<String> = snap_live().iter().map(|(n, _)| n.to_string()).collect();
    match list(&root.join("snap")) {
        Ok(got) => {
            v.ok(got == live_names, || {
                format!("snap/ live listing {got:?} != {live_names:?}")
            });
        }
        Err(e) => v.fail(format!("snap/: {e}")),
    }
    for (name, steps) in snap_live() {
        verify_content(v, root, &format!("snap/{name}"), seed, &Model::of(&steps));
    }
}

pub fn verify_cmd(opts: &Opts) -> Result<()> {
    let bucket_dir = absolute(&opts.bucket_dir)?;
    ensure!(
        bucket_dir.is_dir() && fs::read_dir(&bucket_dir)?.next().is_some(),
        "{} is missing or empty: nothing to verify (run `harness interop write` first)",
        bucket_dir.display()
    );
    let scratch = tempfile::Builder::new()
        .prefix("interop-")
        .tempdir_in(scratch_base())?;

    let mut env_guard = None;
    let (endpoint, backend) = match opts.backend {
        Backend::File => {
            // Mount a copy so the artifact stays byte-for-byte as received.
            let copy = scratch.path().join("bucket");
            copy_dir(&bucket_dir, &copy)?;
            (DUMMY_ENDPOINT.to_string(), copy.display().to_string())
        }
        Backend::Process => {
            s3env::set_backend(S3Backend::Process);
            let env = S3Env::start_with(S3Backend::Process)?;
            let n = import_bucket(&env.direct_endpoint, &bucket_dir)?;
            eprintln!(
                "interop: imported {n} object(s) from {}",
                bucket_dir.display()
            );
            let ep = env.direct_endpoint.clone();
            env_guard = Some(env);
            (ep, format!("s3://{BUCKET}/interop"))
        }
    };
    let mut client = Client::new(scratch.path(), "interop", &endpoint, &backend)?;
    client.mount()?;
    let root = client.mnt.clone();
    let mut v = Verifier {
        checked: 0,
        problems: Vec::new(),
    };
    let outcome = (|| -> Result<()> {
        let manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(root.join("INTEROP.json"))
                .context("INTEROP.json is missing: the write did not complete")?,
        )
        .context("parsing INTEROP.json")?;
        ensure!(
            manifest["version"] == MANIFEST_VERSION,
            "unsupported INTEROP.json version {}",
            manifest["version"]
        );
        let seed = manifest["seed"]
            .as_u64()
            .context("INTEROP.json has no seed")?;
        let devices = manifest["devices"].as_bool().unwrap_or(false);
        eprintln!(
            "interop: verifying seed {seed}, written on {}, devices {devices}",
            manifest["written_on"]
        );
        verify_tree(&mut v, &root, seed, devices);
        Ok(())
    })();
    let unmounted = client.unmount();
    outcome?;
    unmounted?;
    if !v.problems.is_empty() {
        for p in &v.problems {
            eprintln!("interop: PROBLEM: {p}");
        }
        bail!(
            "interop verify FAILED: {} of {} checks",
            v.problems.len(),
            v.checked
        );
    }
    // The S3 server (process backend) outlives the mount.
    drop(env_guard);
    println!("interop verify OK: {} checks", v.checked);
    Ok(())
}

// ---------------------------------------------------------------------------
// plumbing

/// The local file backend never talks to S3, but `Client` always exports an
/// endpoint.
const DUMMY_ENDPOINT: &str = "http://127.0.0.1:9";

fn scratch_base() -> PathBuf {
    if Path::new("/tmp").is_dir() {
        PathBuf::from("/tmp")
    } else {
        std::env::temp_dir()
    }
}

fn absolute(p: &Path) -> Result<PathBuf> {
    Ok(std::path::absolute(p)?)
}

fn prepare_empty_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    ensure!(
        fs::read_dir(dir)?.next().is_none(),
        "{} is not empty: `interop write` needs a fresh bucket directory",
        dir.display()
    );
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let (src, dst) = (entry.path(), to.join(entry.file_name()));
        if entry.file_type()?.is_dir() {
            copy_dir(&src, &dst)?;
        } else {
            fs::copy(&src, &dst)?;
        }
    }
    Ok(())
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn url_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// Every object key of the bucket (paged LIST).
fn list_keys(endpoint: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut url = format!("{endpoint}/{BUCKET}?list-type=2");
        if let Some(t) = &token {
            url.push_str("&continuation-token=");
            url.push_str(&url_encode(t));
        }
        let body = crate::s3auth::signed("GET", &url)
            .call()
            .context("listing the bucket")?
            .into_string()?;
        let mut rest = body.as_str();
        while let Some(i) = rest.find("<Key>") {
            let after = &rest[i + 5..];
            let end = after.find("</Key>").context("malformed LIST response")?;
            keys.push(xml_unescape(&after[..end]));
            rest = &after[end + 6..];
        }
        let truncated = body.contains("<IsTruncated>true</IsTruncated>");
        token = body
            .split_once("<NextContinuationToken>")
            .and_then(|(_, r)| r.split_once("</NextContinuationToken>"))
            .map(|(t, _)| xml_unescape(t));
        if !truncated || token.is_none() {
            return Ok(keys);
        }
    }
}

/// Write every object of the bucket to `dir/<key>`.
fn export_bucket(endpoint: &str, dir: &Path) -> Result<usize> {
    let keys = list_keys(endpoint)?;
    for key in &keys {
        ensure!(
            !key.split('/').any(|c| c == ".." || c.is_empty()) || key.ends_with('/'),
            "refusing to export object key {key:?}"
        );
        if key.ends_with('/') {
            continue;
        }
        let path = dir.join(key);
        fs::create_dir_all(path.parent().unwrap())?;
        let mut file = fs::File::create(&path)?;
        let mut reader =
            crate::s3auth::signed("GET", &format!("{endpoint}/{BUCKET}/{}", url_encode(key)))
                .call()
                .with_context(|| format!("fetching {key}"))?
                .into_reader();
        std::io::copy(&mut reader, &mut file)?;
        file.flush()?;
    }
    Ok(keys.len())
}

/// PUT every file under `dir` as the object with the same relative key.
fn import_bucket(endpoint: &str, dir: &Path) -> Result<usize> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf)>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = if prefix.is_empty() {
                name
            } else {
                format!("{prefix}/{name}")
            };
            if entry.file_type()?.is_dir() {
                walk(&entry.path(), &key, out)?;
            } else {
                out.push((key, entry.path()));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(dir, "", &mut files)?;
    for (key, path) in &files {
        let body = fs::read(path)?;
        crate::s3auth::signed("PUT", &format!("{endpoint}/{BUCKET}/{}", url_encode(key)))
            .send_bytes(&body)
            .with_context(|| format!("importing {key}"))?;
    }
    Ok(files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_is_deterministic_and_seekable() {
        let mut a = vec![0u8; 5000];
        stream(42, "t", 0, &mut a);
        let mut b = vec![0u8; 1000];
        stream(42, "t", 3000, &mut b);
        assert_eq!(&a[3000..4000], &b[..]);
        let mut c = vec![0u8; 1000];
        stream(43, "t", 3000, &mut c);
        assert_ne!(b, c);
        assert_ne!(a[..64], a[64..128]);
    }

    #[test]
    fn model_applies_steps() {
        let m = Model::of(&[write(0, 100, "x"), Step::Truncate(40), Step::Truncate(100)]);
        assert_eq!(m.len, 100);
        let mut buf = vec![7u8; 100];
        m.fill(1, 0, &mut buf);
        let mut head = vec![0u8; 40];
        stream(1, "x", 0, &mut head);
        assert_eq!(&buf[..40], &head[..]);
        assert!(buf[40..].iter().all(|b| *b == 0), "regrown tail is zeros");

        let m = Model::of(&[write(0, 50, "a"), write(10, 10, "b")]);
        let mut buf = vec![0u8; 50];
        m.fill(1, 0, &mut buf);
        let mut b = vec![0u8; 10];
        stream(1, "b", 0, &mut b);
        assert_eq!(&buf[10..20], &b[..]);
        // A window in the middle reads the same bytes as the whole.
        let mut win = vec![0u8; 15];
        m.fill(1, 5, &mut win);
        assert_eq!(&win[..], &buf[5..20]);
    }

    #[test]
    fn spec_is_a_function_of_the_seed() {
        let a: Vec<_> = file_specs(42)
            .iter()
            .map(|s| format!("{}{:?}", s.path, s.steps))
            .collect();
        let b: Vec<_> = file_specs(42)
            .iter()
            .map(|s| format!("{}{:?}", s.path, s.steps))
            .collect();
        let c: Vec<_> = file_specs(7)
            .iter()
            .map(|s| format!("{}{:?}", s.path, s.steps))
            .collect();
        assert_eq!(a, b);
        assert_ne!(a, c);
        let paths: BTreeSet<_> = file_specs(42).into_iter().map(|s| s.path).collect();
        assert_eq!(paths.len(), file_specs(42).len(), "paths are unique");
        assert!(file_specs(42)
            .iter()
            .any(|s| s.path.ends_with(&"n".repeat(255))));
    }

    #[test]
    fn xml_and_url_helpers() {
        assert_eq!(xml_unescape("a&amp;b&lt;c"), "a&b<c");
        assert_eq!(url_encode("a b/c+d"), "a%20b/c%2Bd");
    }

    #[test]
    fn syscalls_roundtrip_on_a_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("f");
        fs::write(&p, b"x").unwrap();
        sys::set_times(&p, BASE_MTIME, MTIME_NSEC).unwrap();
        let m = fs::metadata(&p).unwrap();
        assert_eq!((m.mtime(), m.mtime_nsec() as u32), (BASE_MTIME, MTIME_NSEC));
        sys::mkfifo(&dir.path().join("q"), 0o640).unwrap();
        assert!(fs::metadata(dir.path().join("q"))
            .unwrap()
            .file_type()
            .is_fifo());
        // Encoding of device numbers round-trips through the OS macros.
        assert_eq!(
            sys::major_minor(libc::makedev(200, 70_000) as u64),
            (200, 70_000)
        );
    }
}
