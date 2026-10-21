//! Plan 42 §5.4: which of the three Linux FUSE client races the running
//! kernel has fixed, and so which workarounds `auto` turns on.
//!
//! Three sources, in this order of precedence: an **override** (a
//! workaround's setting `on` or `off` instead of `auto`, for a
//! distribution kernel that carries a backport under an older version
//! number, or a misdetection); a **probe** at the daemon's first mount for
//! the two fixes a deterministic test can see; and the **table** of
//! releases that carry each fix — empty until the patches are merged, and
//! the one place to edit when they are. `auto` treats `Unknown` as not
//! fixed, so every gap in the detection keeps a workaround on: a wrong
//! "fixed" reopens a race, a wrong "not fixed" costs only what the
//! workaround costs.
//!
//! # The probes
//!
//! Stable backports will not show in version numbers, and W2 forced on a
//! kernel with patch 0001 makes every repair whose pages the write covers
//! wait for its bound (plan 42 F10, F15), so the two fixes a test can see
//! are probed — on a mount of the probe's own, a tiny in-memory filesystem
//! served by this process (`ProbeVfs`): the verdict is the kernel's, not a
//! mount's, so no view's namespace is touched and a read-only or subtree
//! view is probed like any other. The probe runs once per process, from a
//! thread, after the first mount: until it answers (a few hundred ms) the
//! verdicts are the table's. A probe that cannot mount (no `/dev/fuse`, no
//! `fusermount3`), or whose test does not come out clearly, answers
//! `Unknown`.
//!
//! - **0001 `WriteThroughWriteback`**: a helper thread writes one whole
//!   page to the probe file; the probe filesystem holds the `WRITE`'s reply
//!   while the probe writes `inval_inode(file, 0, page)`. Without the fix
//!   the folio is uptodate and unlocked during the `WRITE`, and the
//!   notification returns at once; with it the folio is under writeback
//!   until the reply, and the notification waits for the reply, which the
//!   probe releases after a second. A notification still out after that
//!   second counts as having waited only if its thread is then blocked in
//!   the kernel (`D` in `/proc/self/task/<tid>/stat`): one not yet run, or
//!   descheduled after it returned, under host load, is `Unknown`, never
//!   `Fixed`. Twice, `Fixed` only if both waited.
//! - **0003 `DirChangeBumpsVersion`**: race 2b on the probe directory: a
//!   forced `statx` whose `GETATTR` the probe filesystem holds after
//!   reading the attributes, a `mkdir` in the directory meanwhile, then a
//!   plain `statx` — the old `st_nlink` (the held reply was cached) is
//!   `NotFixed`. The directory's attributes are answered with a TTL of a
//!   minute, so a cached reply cannot expire before the plain `statx` and
//!   pass for a rejected one; a `statx` later than half of it is
//!   `Unknown`. The probe mount writes no attribute notification of its
//!   own (W3b off), or it would always find the race closed.
//! - 0002 `AttrsValidAfterStore` has no deterministic test (its window is
//!   a few instructions): the table and the override decide it.

use std::sync::atomic::{AtomicU8, Ordering};

/// One of the kernel patch package's fixes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelFix {
    /// 0001 "fuse: keep folios under writeback while a write-through WRITE
    /// is in flight" (race 1; W2).
    WriteThroughWriteback,
    /// 0002 "fuse: mark cached attributes valid only after storing them"
    /// (race 2a; W3a).
    AttrsValidAfterStore,
    /// 0003 "fuse: bump attr_version when a directory is changed" (race
    /// 2b; W3b).
    DirChangeBumpsVersion,
}

impl KernelFix {
    pub const ALL: [KernelFix; 3] = [
        Self::WriteThroughWriteback,
        Self::AttrsValidAfterStore,
        Self::DirChangeBumpsVersion,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::WriteThroughWriteback => "write_through_writeback",
            Self::AttrsValidAfterStore => "attrs_valid_after_store",
            Self::DirChangeBumpsVersion => "dir_change_bumps_version",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Whether the running kernel has a fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Fixed,
    NotFixed,
    Unknown,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::NotFixed => "not_fixed",
            Self::Unknown => "unknown",
        }
    }
}

/// Where a verdict came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Table,
    Probe,
    Override,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Probe => "probe",
            Self::Override => "override",
        }
    }
}

/// A mainline tag: `(major, minor, rc)`, `rc: None` for the release.
type Mainline = (u32, u32, Option<u32>);

/// One row of [`FIXED_IN`]: the fix, its mainline tag, and its stable
/// backports `(major, minor, sublevel)`.
type FixRow = (KernelFix, Option<Mainline>, &'static [(u32, u32, u32)]);

/// The first releases carrying each fix: the mainline tag and, per stable
/// branch, the first sublevel with the backport `(major, minor, sublevel)`.
/// **Empty until the patches are merged**; filling a row in is a one-line
/// change with a unit test of its own.
const FIXED_IN: &[FixRow] = &[
    (KernelFix::WriteThroughWriteback, None, &[]),
    (KernelFix::AttrsValidAfterStore, None, &[]),
    (KernelFix::DirChangeBumpsVersion, None, &[]),
];

/// A kernel release, as `uname(2)` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Release {
    pub major: u32,
    pub minor: u32,
    pub sublevel: u32,
    /// `-rcN` / `.rcN` (a distribution's pre-release kernel).
    pub rc: Option<u32>,
}

/// Parse a `uname -r`: `7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64`
/// → 7.3.0, rc 4; `6.8.0-45-generic` → 6.8.0; `6.1.112-122.189.amzn2023`
/// → 6.1.112; `7.4-rc2` → 7.4.0 rc 2. The distribution's suffix is
/// ignored; anything that does not start with `major.minor` is `None`.
pub fn parse_release(raw: &str) -> Option<Release> {
    let raw = raw.trim();
    if !raw.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let end = raw
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(raw.len());
    let mut nums = raw[..end].split('.').filter(|s| !s.is_empty());
    let major = nums.next()?.parse().ok()?;
    let minor = nums.next()?.parse().ok()?;
    let sublevel = nums.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let rest = &raw[end..];
    let rc = rest.find("rc").and_then(|at| {
        let digits: String = rest[at + 2..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits.parse().ok()
    });
    Some(Release {
        major,
        minor,
        sublevel,
        rc,
    })
}

/// What `table` says of `fix` on `release`.
fn table_verdict(table: &[FixRow], fix: KernelFix, release: Option<Release>) -> Verdict {
    let Some(r) = release else {
        return Verdict::Unknown;
    };
    let Some((_, mainline, stable)) = table.iter().find(|(f, _, _)| *f == fix) else {
        return Verdict::NotFixed;
    };
    if let Some((major, minor, rc)) = mainline {
        // An rc sorts before its release; a release without rc after all
        // of its rcs.
        let rank = |rc: Option<u32>| rc.unwrap_or(u32::MAX);
        if (r.major, r.minor, rank(r.rc)) >= (*major, *minor, rank(*rc)) {
            return Verdict::Fixed;
        }
    }
    if stable
        .iter()
        .any(|&(ma, mi, sub)| r.major == ma && r.minor == mi && r.sublevel >= sub && r.rc.is_none())
    {
        return Verdict::Fixed;
    }
    Verdict::NotFixed
}

/// The running kernel's release.
pub fn running_release() -> Option<Release> {
    // SAFETY: `uname` fills the struct it is given.
    let mut u: libc::utsname = unsafe { std::mem::zeroed() };
    if unsafe { libc::uname(&mut u) } != 0 {
        return None;
    }
    // SAFETY: a NUL-terminated field.
    let rel = unsafe { std::ffi::CStr::from_ptr(u.release.as_ptr()) };
    parse_release(&rel.to_string_lossy())
}

/// What the table says of `fix` on the running kernel. The release is
/// read once per process: `auto`'s verdict is asked on every reply that
/// carries attributes (W3a) and every write (W2), and the running kernel
/// does not change under a process.
pub fn kernel_has(fix: KernelFix) -> Verdict {
    static RELEASE: std::sync::OnceLock<Option<Release>> = std::sync::OnceLock::new();
    table_verdict(FIXED_IN, fix, *RELEASE.get_or_init(running_release))
}

/// The probes' answers (process-wide): 0 not run, then a [`Verdict`] + 1.
static PROBED: [AtomicU8; 3] = [AtomicU8::new(0), AtomicU8::new(0), AtomicU8::new(0)];

fn encode(v: Verdict) -> u8 {
    match v {
        Verdict::Fixed => 1,
        Verdict::NotFixed => 2,
        Verdict::Unknown => 3,
    }
}

fn decode(b: u8) -> Option<Verdict> {
    match b {
        1 => Some(Verdict::Fixed),
        2 => Some(Verdict::NotFixed),
        3 => Some(Verdict::Unknown),
        _ => None,
    }
}

/// `fix`'s verdict without overrides: the probe's once it answered
/// clearly, else the table's.
pub fn verdict(fix: KernelFix) -> (Verdict, Source) {
    match decode(PROBED[fix.index()].load(Ordering::Acquire)) {
        Some(v @ (Verdict::Fixed | Verdict::NotFixed)) => (v, Source::Probe),
        _ => (kernel_has(fix), Source::Table),
    }
}

/// Whether `auto` may take `fix` as present.
pub(crate) fn fixed(fix: KernelFix) -> bool {
    verdict(fix).0 == Verdict::Fixed
}

/// Record a probe's answer.
pub(crate) fn set_probed(fix: KernelFix, v: Verdict) {
    PROBED[fix.index()].store(encode(v), Ordering::Release);
}

pub(crate) mod probe;

pub use probe::PROBE_ENV;

/// Probe the running kernel's fixes, once per process, in the background
/// (the module doc).
pub fn start_probe() {
    probe::start_once();
}

/// Wait, at most `bound`, until no probe is running; `false` if one still
/// is. Before an `exec` (a handover) or a deliberate exit: the probe holds
/// a reply of its own mount on purpose, and a thread of this process may
/// be in `D` until a timer of this process releases it (`probe`'s
/// `RUNNING`).
pub fn wait_probe_done(bound: std::time::Duration) -> bool {
    probe::wait_done(bound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn releases_parse_as_the_kernel_names_them() {
        assert_eq!(
            parse_release("7.3.0-0.rc4.260925g165768bb7026.42.fc46.x86_64"),
            Some(Release {
                major: 7,
                minor: 3,
                sublevel: 0,
                rc: Some(4)
            })
        );
        assert_eq!(
            parse_release("6.8.0-45-generic"),
            Some(Release {
                major: 6,
                minor: 8,
                sublevel: 0,
                rc: None
            })
        );
        assert_eq!(
            parse_release("6.1.112-122.189.amzn2023.x86_64"),
            Some(Release {
                major: 6,
                minor: 1,
                sublevel: 112,
                rc: None
            })
        );
        assert_eq!(
            parse_release("7.4-rc2"),
            Some(Release {
                major: 7,
                minor: 4,
                sublevel: 0,
                rc: Some(2)
            })
        );
        for garbage in ["", "linux", "7", ".3.1", "rc4"] {
            assert_eq!(parse_release(garbage), None, "{garbage:?}");
        }
    }

    /// A filled-in table: at, after and before the mainline tag; a stable
    /// branch at and below its sublevel; unparseable is `Unknown`.
    #[test]
    fn the_table_decides_by_mainline_tag_and_stable_sublevel() {
        const T: &[FixRow] = &[(
            KernelFix::WriteThroughWriteback,
            Some((7, 4, Some(3))),
            &[(6, 12, 40), (6, 6, 99)],
        )];
        let v = |s: &str| table_verdict(T, KernelFix::WriteThroughWriteback, parse_release(s));
        assert_eq!(v("7.4.0-rc3"), Verdict::Fixed, "at the rc");
        assert_eq!(v("7.4.0-rc2"), Verdict::NotFixed, "before it");
        assert_eq!(v("7.4.0"), Verdict::Fixed, "the release after its rcs");
        assert_eq!(v("7.5.1-generic"), Verdict::Fixed);
        assert_eq!(v("7.3.9"), Verdict::NotFixed);
        assert_eq!(v("6.12.40-1.el10"), Verdict::Fixed, "stable, at");
        assert_eq!(v("6.12.39"), Verdict::NotFixed, "stable, below");
        assert_eq!(v("6.6.120"), Verdict::Fixed);
        assert_eq!(v("6.1.200"), Verdict::NotFixed, "no backport listed");
        assert_eq!(
            table_verdict(T, KernelFix::DirChangeBumpsVersion, parse_release("9.9.9")),
            Verdict::NotFixed,
            "no row: not fixed"
        );
        assert_eq!(v("garbage"), Verdict::Unknown);
    }

    /// The shipped table is empty: every fix is not fixed on every
    /// release (until the patches land), and a probe's clear answer wins.
    #[test]
    fn the_shipped_table_fixes_nothing_and_a_probe_wins() {
        for fix in KernelFix::ALL {
            assert_eq!(
                table_verdict(FIXED_IN, fix, parse_release("99.0.0")),
                Verdict::NotFixed
            );
        }
        // An unclear probe leaves the table's word.
        set_probed(KernelFix::AttrsValidAfterStore, Verdict::Unknown);
        assert_eq!(verdict(KernelFix::AttrsValidAfterStore).1, Source::Table);
    }
}
