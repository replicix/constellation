//! Plan 42 K1: workloads that reproduce three races in the Linux kernel's
//! FUSE client, run against a Constellation mount.
//!
//! The patterns are those of the kernel patch package's standalone
//! reproducers (`pcrace`, `cfrrace`, `nlinkrace`, `dirver`); the code is
//! this crate's own (the package's C sources are GPL-2.0, this repository
//! is MPL-2.0). Each workload returns its counts; a scenario decides what
//! a violation is (`scenarios/kernel_races.rs`).
//!
//! - **Race 1** (stale page after a write-through write): writers
//!   `pwrite` the first half of a page and `pread` it back while other
//!   threads read the pages' second halves (so a dropped page is fetched
//!   again) and something drops the file's pages — an `open`/`close`
//!   loop (every open without `FOPEN_KEEP_CACHE` drops them),
//!   `fadvise(DONTNEED)` (a drop the daemon cannot see), or another
//!   node's lock traffic (the data notification a lock grant writes). A
//!   read-back that differs from what was just written is re-read with
//!   `O_DIRECT`: when that shows the new bytes, the kernel's page cache
//!   is what is stale.
//! - **Race 2a** (cached attributes marked valid before they are
//!   stored): `mkdir`/`rmdir` of children serialized against `statx` of
//!   their directory by an application reader/writer lock; a `statx`
//!   that holds the lock shared must see `st_nlink == 2 + children`.
//! - **Race 2b** (a reply generated before a directory change cancels
//!   its invalidation): a forced `statx` of a directory whose `GETATTR`
//!   the daemon holds (a fault point), a `mkdir` in it meanwhile, then a
//!   plain `statx`: with the race, the held reply's old `st_nlink` is
//!   cached for the attribute TTL. The same for a renamed file's
//!   `ctime`.

use anyhow::{bail, ensure, Context, Result};
use rand::{Rng, SeedableRng};
use std::ffi::CString;
use std::fs::File;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Pages in `pc_race`'s file.
pub const PAGES: u64 = 64;

/// Writer threads; writer `id` owns pages `id, id + WRITERS, …`.
pub const WRITERS: u64 = 4;

/// The kernel's page size.
pub fn page_size() -> u64 {
    // SAFETY: sysconf has no preconditions.
    let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if p > 0 {
        p as u64
    } else {
        4096
    }
}

/// A page-aligned buffer, for `O_DIRECT`.
pub struct Aligned {
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
}

// SAFETY: the buffer is owned and never aliased.
unsafe impl Send for Aligned {}

impl Aligned {
    pub fn new(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len.max(1), page_size() as usize)
            .expect("an aligned layout");
        // SAFETY: a non-zero size.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "allocating an aligned buffer");
        Self { ptr, len, layout }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: `len` bytes allocated above.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for Aligned {
    fn drop(&mut self) {
        // SAFETY: allocated with this layout.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

/// What drops `pc_race`'s pages while it runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dropper {
    /// Nothing (the control: no drop, no stale read).
    None,
    /// A thread `open`s and `close`s the file in a loop.
    Opener,
    /// A thread `stat`s the file in a loop.
    Stat,
    /// A thread `posix_fadvise(POSIX_FADV_DONTNEED)`s the file in a loop:
    /// a drop the daemon cannot see. Every stale read-back is followed by
    /// a read through a *new* descriptor, which must be current
    /// (`stale_after_reopen`).
    Fadvise,
}

/// `pc_race`'s counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PcCounts {
    pub writes: u64,
    /// Read-backs that differed from what was just written.
    pub stale: u64,
    /// Of those, the ones an `O_DIRECT` read showed right on the daemon:
    /// the kernel's page cache was stale.
    pub stale_server_correct: u64,
    /// A stale read-back that a read through a new descriptor still
    /// showed stale (`Dropper::Fadvise` only).
    pub stale_after_reopen: u64,
    /// `copy_file_range` copies whose destination differed from what was
    /// written (`cfr_race` only).
    pub corrupt: u64,
    pub copies: u64,
    /// Read-backs whose `O_DIRECT` re-read was wrong too (the daemon
    /// itself answered other bytes: never expected).
    pub server_wrong: u64,
    pub drops: u64,
}

impl PcCounts {
    fn add(&mut self, o: &PcCounts) {
        self.writes += o.writes;
        self.stale += o.stale;
        self.stale_server_correct += o.stale_server_correct;
        self.stale_after_reopen += o.stale_after_reopen;
        self.corrupt += o.corrupt;
        self.copies += o.copies;
        self.server_wrong += o.server_wrong;
        self.drops += o.drops;
    }

    /// The scenario's `RESULT` fields.
    pub fn line(&self) -> String {
        format!(
            "writes={} stale={} stale_server_correct={} stale_after_reopen={} server_wrong={} \
             copies={} corrupt={} drops={}",
            self.writes,
            self.stale,
            self.stale_server_correct,
            self.stale_after_reopen,
            self.server_wrong,
            self.copies,
            self.corrupt,
            self.drops
        )
    }
}

/// What `pc_race` runs besides its writers.
#[derive(Clone, Debug)]
pub struct PcConfig {
    pub secs: u64,
    pub dropper: Dropper,
    /// Threads reading the pages' second halves.
    pub readers: usize,
    /// `Some(dst)`: the writers also `copy_file_range` what they wrote to
    /// `dst` and check it there with `O_DIRECT` (`cfr_race`).
    pub copy_to: Option<PathBuf>,
    pub seed: u64,
    /// An external stop (a scenario's handover, its fault): the run ends
    /// at `secs` or when this is set, whichever comes first.
    pub stop: Option<Arc<AtomicBool>>,
}

impl PcConfig {
    pub fn new(secs: u64, dropper: Dropper, seed: u64) -> Self {
        Self {
            secs,
            dropper,
            readers: 2,
            copy_to: None,
            seed,
            stop: None,
        }
    }
}

fn open_rw(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("opening {}", path.display()))
}

fn open_direct(path: &Path) -> Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
        .with_context(|| format!("opening {} O_DIRECT", path.display()))
}

/// The page `n` of `path` through `O_DIRECT` (the daemon's answer).
fn read_direct(f: &File, page: u64) -> Result<Vec<u8>> {
    let p = page_size();
    let mut buf = Aligned::new(p as usize);
    let n = f.read_at(buf.bytes_mut(), page * p)?;
    Ok(buf.bytes_mut()[..n].to_vec())
}

/// Create `path` as `PAGES` pages, page `n` filled with `n as u8 + 1`
/// in its second half and 0 in its first, closed and synced.
pub fn pc_prepare(path: &Path) -> Result<()> {
    let p = page_size();
    let mut data = vec![0u8; (PAGES * p) as usize];
    for page in 0..PAGES {
        let start = (page * p + p / 2) as usize;
        data[start..start + (p / 2) as usize].fill(page as u8 + 1);
    }
    std::fs::write(path, &data).with_context(|| format!("writing {}", path.display()))?;
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Race 1's workload (module doc): `WRITERS` writers, `cfg.readers`
/// readers and `cfg.dropper`, for `cfg.secs` seconds, on a file
/// [`pc_prepare`] made.
pub fn pc_race(path: &Path, cfg: &PcConfig) -> Result<PcCounts> {
    let p = page_size();
    let half = (p / 2) as usize;
    let stop = cfg
        .stop
        .clone()
        .unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let deadline = Instant::now() + Duration::from_secs(cfg.secs);
    let path = path.to_path_buf();
    let mut handles = Vec::new();
    for id in 0..WRITERS {
        let path = path.clone();
        let stop = stop.clone();
        let copy_to = cfg.copy_to.clone();
        let dropper = cfg.dropper;
        let seed = cfg.seed ^ (id + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
        handles.push(std::thread::spawn(move || -> Result<PcCounts> {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let f = open_rw(&path)?;
            let direct = open_direct(&path)?;
            let (dst, dst_direct) = match &copy_to {
                Some(d) => (Some(open_rw(d)?), Some(open_direct(d)?)),
                None => (None, None),
            };
            let mut c = PcCounts::default();
            let mut byte = (id as u8).wrapping_mul(61);
            let mut back = vec![0u8; half];
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let page = id + WRITERS * rng.random_range(0..PAGES / WRITERS);
                // A fresh byte, never 0 (the file's first halves start 0).
                byte = byte.wrapping_add(1).max(1);
                let data = vec![byte; half];
                f.write_all_at(&data, page * p)?;
                c.writes += 1;
                let n = f.read_at(&mut back, page * p)?;
                if n != half || back != data {
                    c.stale += 1;
                    let server = read_direct(&direct, page)?;
                    if server.len() >= half && server[..half] == data[..] {
                        c.stale_server_correct += 1;
                    } else {
                        c.server_wrong += 1;
                    }
                    if dropper == Dropper::Fadvise {
                        // Today's guarantee (plan 42 F1): a new open
                        // repairs a stale page.
                        let again = File::open(&path)?;
                        let mut b = vec![0u8; half];
                        let n = again.read_at(&mut b, page * p)?;
                        if n != half || b != data {
                            c.stale_after_reopen += 1;
                        }
                    }
                }
                if let (Some(dst), Some(dst_direct)) = (&dst, &dst_direct) {
                    let mut off_in = (page * p) as libc::loff_t;
                    let mut off_out = (page * p) as libc::loff_t;
                    // SAFETY: two open descriptors and in-bounds offsets.
                    let n = unsafe {
                        libc::copy_file_range(
                            f.as_raw_fd(),
                            &mut off_in,
                            dst.as_raw_fd(),
                            &mut off_out,
                            half,
                            0,
                        )
                    };
                    if n < 0 {
                        bail!("copy_file_range: {}", std::io::Error::last_os_error());
                    }
                    c.copies += 1;
                    let got = read_direct(dst_direct, page)?;
                    if got.len() < half || got[..half] != data[..] {
                        c.corrupt += 1;
                    }
                }
            }
            Ok(c)
        }));
    }
    for r in 0..cfg.readers {
        let path = path.clone();
        let stop = stop.clone();
        let seed = cfg.seed ^ (0x1000 + r as u64);
        handles.push(std::thread::spawn(move || -> Result<PcCounts> {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let f = File::open(&path)?;
            let mut b = vec![0u8; half];
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let page = rng.random_range(0..PAGES);
                f.read_at(&mut b, page * p + p / 2)?;
            }
            Ok(PcCounts::default())
        }));
    }
    if cfg.dropper != Dropper::None {
        let path = path.clone();
        let stop = stop.clone();
        let dropper = cfg.dropper;
        handles.push(std::thread::spawn(move || -> Result<PcCounts> {
            let mut c = PcCounts::default();
            let keep = File::open(&path)?;
            let cpath = CString::new(path.as_os_str().as_bytes())?;
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                match dropper {
                    Dropper::Opener => drop(File::open(&path)?),
                    Dropper::Stat => {
                        let mut st: libc::stat = unsafe { std::mem::zeroed() };
                        // SAFETY: a NUL-terminated path and a stat buffer.
                        unsafe { libc::stat(cpath.as_ptr(), &mut st) };
                    }
                    Dropper::Fadvise => {
                        // SAFETY: an open descriptor.
                        unsafe {
                            libc::posix_fadvise(keep.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED)
                        };
                        std::thread::sleep(Duration::from_micros(200));
                    }
                    Dropper::None => {}
                }
                c.drops += 1;
            }
            Ok(c)
        }));
    }
    let mut total = PcCounts::default();
    let mut first_error = None;
    for h in handles {
        match h.join() {
            Ok(Ok(c)) => total.add(&c),
            Ok(Err(e)) => {
                stop.store(true, Ordering::Relaxed);
                first_error.get_or_insert(e);
            }
            Err(_) => {
                stop.store(true, Ordering::Relaxed);
                first_error.get_or_insert(anyhow::anyhow!("a pc_race thread panicked"));
            }
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(total),
    }
}

/// One write through one descriptor, after a read of every page (so the
/// kernel holds them all, uptodate): page `k % PAGES`'s first half gets
/// byte `fill` and is read back (the kernel then holds the written page
/// even if a write-race repair dropped it), and the close publishes it —
/// one publication, which the commit fault (every second one) fails or
/// lets through. A close error (a failed publication reports `EIO`) is
/// not the workload's.
pub fn one_write(path: &Path, k: u64, fill: u8) -> Result<()> {
    let p = page_size();
    let f = open_rw(path)?;
    let mut b = vec![0u8; p as usize];
    for page in 0..PAGES {
        f.read_at(&mut b, page * p)?;
    }
    let at = (k % PAGES) * p;
    f.write_all_at(&vec![fill; (p / 2) as usize], at)?;
    f.read_at(&mut b, at)?;
    drop(f);
    Ok(())
}

/// Check every writer page's first half of `path` through a fresh
/// buffered descriptor against what an `O_DIRECT` read answers:
/// `(pages, differing)`. What a reopen after an uncommitted session
/// must agree on (plan 42 §5.1.5).
pub fn reopen_matches_view(path: &Path) -> Result<(u64, u64)> {
    let p = page_size();
    let half = (p / 2) as usize;
    let buffered = File::open(path)?;
    let direct = open_direct(path)?;
    let mut differing = 0;
    let mut b = vec![0u8; half];
    for page in 0..PAGES {
        let n = buffered.read_at(&mut b, page * p)?;
        let d = read_direct(&direct, page)?;
        if n != half || d.len() < half || b[..] != d[..half] {
            differing += 1;
        }
    }
    Ok((PAGES, differing))
}

fn statx(path: &CString, force_sync: bool) -> std::io::Result<libc::statx> {
    let mut st: libc::statx = unsafe { std::mem::zeroed() };
    let flags = if force_sync {
        libc::AT_STATX_FORCE_SYNC
    } else {
        0
    };
    // SAFETY: a NUL-terminated path and a statx buffer.
    let r = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            path.as_ptr(),
            flags,
            libc::STATX_BASIC_STATS,
            &mut st,
        )
    };
    if r == 0 {
        Ok(st)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn cpath(p: &Path) -> CString {
    CString::new(p.as_os_str().as_bytes()).expect("a path without NUL")
}

/// `nlink_race`'s counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NlinkCounts {
    pub stats: u64,
    pub changes: u64,
    pub mismatches: u64,
}

impl NlinkCounts {
    pub fn line(&self) -> String {
        format!(
            "stats={} changes={} mismatches={}",
            self.stats, self.changes, self.mismatches
        )
    }
}

/// Whether a directory's `st_nlink` under `base` counts its
/// subdirectories (`2 + subdirectories`, as ext4, xfs, tmpfs and
/// Constellation keep it); btrfs answers 1 for every directory, where
/// [`nlink_race`]'s check cannot hold. Probed by making a subdirectory in a
/// fresh directory, removed again.
pub fn counts_subdirectory_links(base: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let probe = base.join(".nlink-probe");
    std::fs::create_dir(&probe).with_context(|| format!("mkdir {}", probe.display()))?;
    let before = std::fs::metadata(&probe)?.nlink();
    let made = std::fs::create_dir(probe.join("d"));
    let after = std::fs::metadata(&probe)?.nlink();
    let _ = std::fs::remove_dir(probe.join("d"));
    std::fs::remove_dir(&probe)?;
    made?;
    Ok(after == before + 1)
}

/// Directories `nlink_race` works in.
const NLINK_DIRS: usize = 4;

/// Race 2a's workload (module doc): `threads` threads over four
/// directories under `base`, each directory behind an application
/// reader/writer lock; a thread changes a directory (a `mkdir` or
/// `rmdir` of a child) one time in four, holding its lock exclusive, and
/// otherwise `statx`es it holding the lock shared, checking `st_nlink ==
/// 2 + children`. `force_sync`: every `statx` asks the daemon
/// (`AT_STATX_FORCE_SYNC`) — the control, which the race cannot reach.
pub fn nlink_race(
    base: &Path,
    secs: u64,
    threads: usize,
    force_sync: bool,
    seed: u64,
) -> Result<NlinkCounts> {
    struct Dir {
        path: PathBuf,
        cpath: CString,
        /// The application lock, and the children the directory has.
        lock: RwLock<u32>,
    }
    let mut dirs = Vec::new();
    for d in 0..NLINK_DIRS {
        let path = base.join(format!("nl{d}"));
        std::fs::create_dir(&path).with_context(|| format!("mkdir {}", path.display()))?;
        dirs.push(Dir {
            cpath: cpath(&path),
            path,
            lock: RwLock::new(0),
        });
    }
    let dirs = Arc::new(dirs);
    let deadline = Instant::now() + Duration::from_secs(secs);
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..threads {
        let dirs = dirs.clone();
        let stop = stop.clone();
        let seed = seed ^ (t as u64 + 1).wrapping_mul(0x2545_f491_4f6c_dd1d);
        handles.push(std::thread::spawn(move || -> Result<NlinkCounts> {
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            let mut c = NlinkCounts::default();
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let d = &dirs[rng.random_range(0..NLINK_DIRS)];
                if rng.random_range(0..4) == 0 {
                    let mut children = d.lock.write().unwrap();
                    if *children > 0 && rng.random_bool(0.5) {
                        let child = d.path.join(format!("c{}", *children - 1));
                        std::fs::remove_dir(&child)
                            .with_context(|| format!("rmdir {}", child.display()))?;
                        *children -= 1;
                    } else if *children < 8 {
                        let child = d.path.join(format!("c{}", *children));
                        std::fs::create_dir(&child)
                            .with_context(|| format!("mkdir {}", child.display()))?;
                        *children += 1;
                    }
                    c.changes += 1;
                } else {
                    let children = d.lock.read().unwrap();
                    let st = statx(&d.cpath, force_sync)
                        .with_context(|| format!("statx {}", d.path.display()))?;
                    c.stats += 1;
                    if st.stx_nlink != 2 + *children {
                        c.mismatches += 1;
                    }
                }
            }
            Ok(c)
        }));
    }
    let mut total = NlinkCounts::default();
    let mut first_error = None;
    for h in handles {
        match h.join() {
            Ok(Ok(c)) => {
                total.stats += c.stats;
                total.changes += c.changes;
                total.mismatches += c.mismatches;
            }
            Ok(Err(e)) => {
                stop.store(true, Ordering::Relaxed);
                first_error.get_or_insert(e);
            }
            Err(_) => {
                stop.store(true, Ordering::Relaxed);
                first_error.get_or_insert(anyhow::anyhow!("an nlink_race thread panicked"));
            }
        }
    }
    match first_error {
        Some(e) => Err(e),
        None => Ok(total),
    }
}

/// `dir_version`'s and `rename_ctime`'s counts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RoundCounts {
    pub rounds: u64,
    pub stale: u64,
    /// Attempts whose listing did not hand its name with attributes (a
    /// held reply that carried none tests nothing), made again.
    pub redone: u64,
}

impl RoundCounts {
    pub fn line(&self) -> String {
        format!(
            "rounds={} stale={} redone={}",
            self.rounds, self.stale, self.redone
        )
    }
}

/// How long to wait after the last lookup the harness made before a
/// listing may carry the looked-up names' attributes. A `READDIRPLUS` page
/// hands a name the kernel holds from a lookup name-only (`held`, plan 40):
/// the dentry of a reply with a 1 s entry TTL is held for 3/4 of it.
pub const PAST_THE_HOLD: Duration = Duration::from_millis(1500);

/// A round whose held reply is a `READDIRPLUS` page (plan 40 P9): the
/// listing of `dir` from its start, and the mount's counters to check that
/// the page did carry the name's attributes.
pub struct Listing<'a> {
    pub dir: &'a Path,
    /// The mount's `(READDIRPLUS entries handed with attributes, entries
    /// handed name-only as held)` so far.
    pub seen: &'a dyn Fn() -> Result<(u64, u64)>,
}

impl Listing<'_> {
    /// Wait out the hold of the lookups made so far (the descriptors
    /// opened, the directory made), then check that a listing now carries
    /// the name's attributes: from the first round on the page must. The
    /// wait follows the last lookup, so it follows the opens: a path walk
    /// after it holds the names again, and a wait that is missing leaves
    /// them held, which the check fails on (no rounds are needed to see
    /// it: a round's own lookups, a load-delayed page, can make a later
    /// round's page name-only, and that one is made again).
    fn settle(&self) -> Result<()> {
        std::thread::sleep(PAST_THE_HOLD);
        let before = (self.seen)()?;
        for e in std::fs::read_dir(self.dir)? {
            e?;
        }
        let (entries, held) = (self.seen)()?;
        ensure!(
            entries > before.0 && held == before.1,
            "the listing of {} right after the wait out of the hold handed {} entries with \
             attributes and {} name-only as held: a lookup the wait does not follow holds the \
             names, and no round would test the plus page",
            self.dir.display(),
            entries.saturating_sub(before.0),
            held.saturating_sub(before.1)
        );
        Ok(())
    }

    fn start(&self) -> Result<(u64, u64)> {
        (self.seen)()
    }

    /// Whether the attempt that began at `before` listed its name with
    /// attributes and left none name-only: the race it means to hit was
    /// open. An attempt that did not is made again, once; two in a row
    /// mean the listing never carries attributes, and the row tests
    /// nothing.
    fn exercised(
        &self,
        before: (u64, u64),
        c: &mut RoundCounts,
        in_a_row: &mut u32,
    ) -> Result<bool> {
        let (entries, held) = (self.seen)()?;
        if entries > before.0 && held == before.1 {
            *in_a_row = 0;
            return Ok(true);
        }
        c.redone += 1;
        *in_a_row += 1;
        ensure!(
            *in_a_row < 2,
            "the listing of {} handed no attributes in {} attempts in a row ({} entries with \
             attributes, {} held since the attempt began): the round tests nothing",
            self.dir.display(),
            in_a_row,
            entries.saturating_sub(before.0),
            held.saturating_sub(before.1)
        );
        Ok(false)
    }
}

/// A directory or file opened for the `*at` calls below (`O_PATH`):
/// operating through it, a round walks no path, so no dentry
/// revalidation's `LOOKUP` re-installs the inode's attributes inside the
/// race's window (which would bump its attribute version and hide the
/// race in some rounds).
struct Fd(RawFd);

impl Fd {
    fn open(path: &Path) -> Result<Fd> {
        let c = cpath(path);
        // SAFETY: a NUL-terminated path.
        let fd = unsafe { libc::open(c.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            bail!(
                "open {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            );
        }
        Ok(Fd(fd))
    }

    /// `statx` of the inode itself (`AT_EMPTY_PATH`).
    fn statx(&self, force_sync: bool) -> std::io::Result<libc::statx> {
        let mut st: libc::statx = unsafe { std::mem::zeroed() };
        let flags = libc::AT_EMPTY_PATH
            | if force_sync {
                libc::AT_STATX_FORCE_SYNC
            } else {
                0
            };
        // SAFETY: an open descriptor, an empty path and a statx buffer.
        let r = unsafe {
            libc::statx(
                self.0,
                c"".as_ptr(),
                flags,
                libc::STATX_BASIC_STATS,
                &mut st,
            )
        };
        if r == 0 {
            Ok(st)
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    /// Whether `name` exists in this directory.
    fn has(&self, name: &str) -> bool {
        let n = CString::new(name).unwrap();
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: an open directory descriptor and a NUL-terminated name.
        unsafe { libc::fstatat(self.0, n.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW) == 0 }
    }

    /// Create `name` in this directory if absent, remove it if present:
    /// whether it exists now.
    fn toggle(&self, name: &str) -> Result<bool> {
        let n = CString::new(name).unwrap();
        // SAFETY: an open directory descriptor and a NUL-terminated name.
        let (r, now) = if self.has(name) {
            (
                unsafe { libc::unlinkat(self.0, n.as_ptr(), libc::AT_REMOVEDIR) },
                false,
            )
        } else {
            (unsafe { libc::mkdirat(self.0, n.as_ptr(), 0o755) }, true)
        };
        if r != 0 {
            bail!("toggling {name}: {}", std::io::Error::last_os_error());
        }
        Ok(now)
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        // SAFETY: our descriptor.
        unsafe { libc::close(self.0) };
    }
}

/// The request a round holds: a forced `statx` of `fd`, or a listing of
/// `listing` from its start (the first chunk of which the kernel asks
/// `READDIRPLUS` for under `auto`).
fn held(fd: &Arc<Fd>, listing: Option<&Listing>) -> std::thread::JoinHandle<std::io::Result<()>> {
    match listing {
        None => {
            let fd = fd.clone();
            std::thread::spawn(move || fd.statx(true).map(|_| ()))
        }
        Some(l) => {
            let parent = l.dir.to_path_buf();
            std::thread::spawn(move || {
                for e in std::fs::read_dir(parent)? {
                    e?;
                }
                Ok(())
            })
        }
    }
}

/// Race 2b's round (module doc), `rounds` times, in `dir`; the daemon
/// holds every directory `GETATTR` for `delay` after reading the
/// attributes (`CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS`). A round:
/// toggle `a`; `statx(dir)` (the kernel's attributes now valid, so the
/// `mkdir` below needs no permission `GETATTR`); a thread
/// `statx(dir, AT_STATX_FORCE_SYNC)` — its `GETATTR` held; after half of
/// the delay `mkdir`/`rmdir` `b`; join; a plain `statx(dir)` must show
/// `2 + a + b`. Everything goes through descriptors ([`Fd`]). `remote`:
/// the change is made through another mount of the same filesystem
/// (another node), and the check waits for it to be visible here first.
/// `listing`: the held reply is a `READDIRPLUS` page instead (plan 40
/// P9): a thread lists that directory (`dir`'s parent) from the start,
/// so the page carries `dir`'s attributes as of the read, and the daemon
/// holds the page for `delay`. The page must carry `dir`'s attributes
/// ([`Listing::exercised`]): the wait out of the hold of the lookups the
/// opens made comes after them, and a round whose page did not is made
/// again.
pub fn dir_version(
    dir: &Path,
    rounds: u64,
    delay: Duration,
    remote: Option<&Path>,
    listing: Option<Listing>,
) -> Result<RoundCounts> {
    let d = Arc::new(Fd::open(dir)?);
    let other = remote.map(Fd::open).transpose()?;
    if let Some(l) = &listing {
        l.settle()?;
    }
    let mut c = RoundCounts::default();
    let mut in_a_row = 0;
    while c.rounds < rounds {
        let before = listing.as_ref().map(Listing::start).transpose()?;
        let a = d.toggle("a")?;
        d.statx(false)?;
        let t = held(&d, listing.as_ref());
        std::thread::sleep(delay / 2);
        let b = other.as_ref().unwrap_or(&*d).toggle("b")?;
        t.join()
            .map_err(|_| anyhow::anyhow!("the stat thread panicked"))?
            .context("the forced statx")?;
        if remote.is_some() {
            // The other node's change reaches this one asynchronously:
            // wait until this mount's daemon answers it.
            let start = Instant::now();
            while d.has("b") != b {
                if start.elapsed() > Duration::from_secs(30) {
                    bail!("the remote change never became visible");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            // And past the hold of any GETATTR the check sent.
            std::thread::sleep(delay + Duration::from_millis(50));
        }
        let st = d.statx(false)?;
        let stale = st.stx_nlink != 2 + a as u32 + b as u32;
        c.stale += stale as u64;
        if let (Some(l), Some(before)) = (&listing, before) {
            if !l.exercised(before, &mut c, &mut in_a_row)? {
                continue;
            }
        }
        c.rounds += 1;
    }
    Ok(c)
}

/// Race 2b for a renamed file's `ctime` (plan 42 F16), `rounds` times on
/// `dir/f`, which must exist; the daemon holds the `GETATTR` of the
/// file's inode for `delay` (`CONSTELLATION_FAULT_GETATTR_DELAY_INO`). A
/// round renames the file to the other of `f` and `g`: `statx(src)` (its
/// dentry is valid now: the rename below sends no `LOOKUP` of it); a
/// thread `statx(file, AT_STATX_FORCE_SYNC)` — held; after half of the
/// delay `rename(src, dst)`; join; a plain `statx(file)` must show the
/// `ctime` a forced one answers. The file is stated through a descriptor
/// ([`Fd`]), the rename made in the directory's.
/// `listing`: as for [`dir_version`], the held reply is a listing of
/// `dir` (the file's directory) from the start. No round looks a name up
/// by path before its listing (the rename's own `ENOENT` says a name is
/// gone): a lookup would hold the name, and the page would hand it
/// name-only.
pub fn rename_ctime(
    dir: &Path,
    rounds: u64,
    delay: Duration,
    listing: Option<Listing>,
) -> Result<RoundCounts> {
    let d = Fd::open(dir)?;
    let f = Arc::new(Fd::open(&dir.join("f"))?);
    if let Some(l) = &listing {
        l.settle()?;
    }
    let mut c = RoundCounts::default();
    let mut in_a_row = 0;
    let mut attempt = 0u64;
    while c.rounds < rounds {
        let (src, dst) = if attempt.is_multiple_of(2) {
            ("f", "g")
        } else {
            ("g", "f")
        };
        attempt += 1;
        let csrc = CString::new(src).unwrap();
        let cdst = CString::new(dst).unwrap();
        let before = listing.as_ref().map(Listing::start).transpose()?;
        f.statx(false)?;
        // Past the ctime the last change stamped (the clock's granularity).
        std::thread::sleep(Duration::from_millis(5));
        let t = held(&f, listing.as_ref());
        std::thread::sleep(delay / 2);
        // SAFETY: an open directory descriptor and NUL-terminated names.
        if unsafe { libc::renameat(d.0, csrc.as_ptr(), d.0, cdst.as_ptr()) } != 0 {
            bail!("rename: {}", std::io::Error::last_os_error());
        }
        t.join()
            .map_err(|_| anyhow::anyhow!("the stat thread panicked"))?
            .context("the forced statx")?;
        let cached = f.statx(false)?;
        let truth = f.statx(true)?;
        c.stale += ((cached.stx_ctime.tv_sec, cached.stx_ctime.tv_nsec)
            != (truth.stx_ctime.tv_sec, truth.stx_ctime.tv_nsec)) as u64;
        if let (Some(l), Some(before)) = (&listing, before) {
            if !l.exercised(before, &mut c, &mut in_a_row)? {
                continue;
            }
        }
        c.rounds += 1;
    }
    Ok(c)
}

/// Take and drop an exclusive `flock` on `path` in a loop until `stop`,
/// in turns with another such loop on another mount (`turn` names whose
/// turn it is: `me` or the other), so every grant follows a recall
/// (`kernel-race-notify`). Returns the locks taken.
pub fn flock_turns(
    path: &Path,
    me: u64,
    turn: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> Result<u64> {
    let f = File::open(path)?;
    let fd: RawFd = f.as_raw_fd();
    let mut taken = 0;
    while !stop.load(Ordering::Relaxed) {
        if turn.load(Ordering::SeqCst) != me {
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        // SAFETY: an open descriptor.
        if unsafe { libc::flock(fd, libc::LOCK_EX) } != 0 {
            bail!("flock: {}", std::io::Error::last_os_error());
        }
        std::thread::sleep(Duration::from_millis(2));
        // SAFETY: as above.
        unsafe { libc::flock(fd, libc::LOCK_UN) };
        taken += 1;
        turn.store(1 - me, Ordering::SeqCst);
    }
    Ok(taken)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The workloads on a local directory: no race, so no violation —
    /// the checks themselves are right (a workload that reported stale
    /// reads on a local file system would prove nothing on FUSE).
    #[test]
    fn the_workloads_report_nothing_on_a_local_file_system() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("pc");
        pc_prepare(&f).unwrap();
        for dropper in [Dropper::None, Dropper::Opener, Dropper::Fadvise] {
            let c = pc_race(&f, &PcConfig::new(1, dropper, 7));
            // tmpfs refuses O_DIRECT; the check needs a file system that
            // takes it, which the test's temp dir may not be.
            let Ok(c) = c else { return };
            assert!(c.writes > 0);
            assert_eq!(
                (c.stale, c.stale_after_reopen),
                (0, 0),
                "{dropper:?}: {c:?}"
            );
        }
        let (pages, differing) = reopen_matches_view(&f).unwrap();
        assert_eq!((pages, differing), (PAGES, 0));
        // `nlink_race`'s check is `st_nlink == 2 + children`, which holds
        // only where a directory counts its subdirectories' `..` links: btrfs
        // answers 1 for every directory, and the check then fails on every
        // `statx` (Constellation, and the ext4 primary of plan 42 §7.3, do
        // count them).
        if !counts_subdirectory_links(dir.path()).unwrap() {
            eprintln!(
                "skipping nlink_race: {} does not count subdirectory links in st_nlink",
                dir.path().display()
            );
            return;
        }
        let n = nlink_race(dir.path(), 1, 4, false, 3).unwrap();
        assert!(n.stats > 0 && n.changes > 0);
        assert_eq!(n.mismatches, 0);
    }

    /// The probe on the test's own temp dir agrees with what a
    /// subdirectory does to its parent's link count there.
    #[test]
    fn the_link_count_probe_matches_the_file_system() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let counts = counts_subdirectory_links(dir.path()).unwrap();
        let before = std::fs::metadata(dir.path()).unwrap().nlink();
        std::fs::create_dir(dir.path().join("x")).unwrap();
        let after = std::fs::metadata(dir.path()).unwrap().nlink();
        assert_eq!(counts, after == before + 1);
        assert_eq!(
            dir.path().read_dir().unwrap().count(),
            1,
            "the probe cleans up"
        );
    }
}

/// What [`read_then_write`] saw.
#[derive(Debug, Default, Clone)]
pub struct StallCounts {
    pub rounds: u64,
    /// Read-backs that differed from what was written.
    pub stale: u64,
    /// Each write's latency, in order.
    pub write_ms: Vec<u128>,
}

impl StallCounts {
    /// `p50` and `max` of the write latencies, in ms.
    pub fn p50_max(&self) -> (u128, u128) {
        let mut v = self.write_ms.clone();
        v.sort_unstable();
        (
            v.get(v.len() / 2).copied().unwrap_or(0),
            v.last().copied().unwrap_or(0),
        )
    }

    pub fn line(&self) -> String {
        let (p50, max) = self.p50_max();
        format!(
            "rounds={} stale={} write_p50_ms={p50} write_max_ms={max}",
            self.rounds, self.stale
        )
    }
}

/// What [`read_then_write`] writes after it read two whole pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteShape {
    /// The first half of the first page: the page is the write's tail
    /// (the page the writer may hold locked, which the repair leaves out,
    /// plan 42 KF2), so no recorded page is left to repair.
    HalfPage,
    /// Both pages whole: neither is the tail, so the repair drops both —
    /// the writer's own folios, which on a kernel with fix 0001 are under
    /// writeback until the reply the repair lane holds (F10).
    TwoPages,
}

/// The write-repair stall's workload (plan 42 F10, §7.5): per round two
/// pages of `path` (a file [`pc_prepare`] made) are **read whole, then
/// written** as `shape` says, on a descriptor opened for the round — an
/// open without `FOPEN_KEEP_CACHE` arms the inode and the reads are
/// recorded, so the write's claim finds its pages among the recorded reads.
/// Rounds are serial, so nothing else of the inode is in flight and the
/// repair's clock runs.
pub fn read_then_write(path: &Path, rounds: u64, shape: WriteShape) -> Result<StallCounts> {
    let p = page_size();
    let mut c = StallCounts::default();
    for round in 0..rounds {
        let page = 2 * (round % (PAGES / 2));
        let f = open_rw(path)?;
        let mut whole = vec![0u8; 2 * p as usize];
        f.read_exact_at(&mut whole, page * p)?;
        let byte = (round as u8).wrapping_add(1).max(1);
        let len = match shape {
            WriteShape::HalfPage => (p / 2) as usize,
            WriteShape::TwoPages => 2 * p as usize,
        };
        let data = vec![byte; len];
        let t = Instant::now();
        f.write_all_at(&data, page * p)?;
        c.write_ms.push(t.elapsed().as_millis());
        let mut back = vec![0u8; len];
        f.read_exact_at(&mut back, page * p)?;
        if back != data {
            c.stale += 1;
        }
        c.rounds += 1;
    }
    Ok(c)
}
