//! Leftovers of earlier harness runs, swept once when a run starts.
//!
//! A harness killed hard (`timeout -s KILL`, an OOM kill, a lost ssh
//! session) cannot tear down what it made: [`crate::spawn`] takes the
//! daemons with it, but a FUSE mount whose daemon died unclean can stay
//! behind answering `ENOTCONN`, and floci / toxiproxy containers stay up.
//! [`mounts`] and [`containers`] remove those, and only those:
//!
//! - a mount is stale when it is a FUSE mount under a `harness-*`
//!   directory of a temp root, `stat(2)` on it answers `ENOTCONN` (its
//!   daemon is gone) and no live harness owns the directory: a run
//!   [`claim`]s its scenario roots for its whole life, so a mount a live
//!   scenario has killed on purpose (`kill -9`, a transport abort) is its
//!   own to inspect and detach. A mount that answers (or hangs) has a
//!   daemon and is left alone, whoever's it is;
//! - a container is swept when it carries this run's docker prefix (the
//!   `constellation-harness-prefix` label, or, for containers made before
//!   the label existed, the `<prefix>-floci` / `<prefix>-toxiproxy` names)
//!   and the prefix is not locked by a live run. Another prefix's
//!   containers are never matched: `constellation-harness-p30-floci` is
//!   not `constellation-harness`'s, whatever its name starts with.
//!
//! Both run once per process, before the process creates anything.

use std::path::{Path, PathBuf};
use std::sync::Once;
use std::time::Duration;

pub const PREFIX_LABEL: &str = "constellation-harness-prefix";
const DEFAULT_PREFIX: &str = "constellation-harness";

/// One `docker ps -a` row: id, name, the prefix label (empty if absent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: String,
    pub name: String,
    pub prefix: String,
}

/// The rows of `rows` that belong to `prefix`.
pub fn owned_by<'a>(rows: &'a [Row], prefix: &str) -> Vec<&'a Row> {
    rows.iter()
        .filter(|r| {
            if r.prefix.is_empty() {
                r.name == format!("{prefix}-floci") || r.name == format!("{prefix}-toxiproxy")
            } else {
                r.prefix == prefix
            }
        })
        .collect()
}

fn parse_rows(text: &str) -> Vec<Row> {
    text.lines()
        .filter_map(|l| {
            let mut f = l.split('\t');
            let id = f.next()?.trim();
            let name = f.next()?.trim();
            if id.is_empty() || name.is_empty() {
                return None;
            }
            Some(Row {
                id: id.to_string(),
                name: name.to_string(),
                prefix: f.next().unwrap_or("").trim().to_string(),
            })
        })
        .collect()
}

/// Remove the containers of `prefix` and of the default prefix that no
/// live run holds. Call with `own` already locked by this run.
pub fn containers(own: &str) {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let fmt = format!("{{{{.ID}}}}\t{{{{.Names}}}}\t{{{{.Label \"{PREFIX_LABEL}\"}}}}");
        let Ok(text) = crate::docker::docker(&[
            "ps",
            "-a",
            "--filter",
            "label=constellation-harness=1",
            "--format",
            &fmt,
        ]) else {
            return;
        };
        let rows = parse_rows(&text);
        let mut prefixes = vec![own.to_string()];
        if own != DEFAULT_PREFIX && !crate::s3env::prefix_in_use(DEFAULT_PREFIX) {
            prefixes.push(DEFAULT_PREFIX.to_string());
        }
        for p in prefixes {
            for r in owned_by(&rows, &p) {
                eprintln!(
                    "=== sweep: removing leftover container {} ({})",
                    r.name, r.id
                );
                let _ = crate::docker::docker(&["rm", "-f", &r.id]);
            }
        }
    });
}

fn temp_roots() -> Vec<PathBuf> {
    let mut roots = vec![std::env::temp_dir()];
    let tmp = PathBuf::from("/tmp");
    if !roots.contains(&tmp) {
        roots.push(tmp);
    }
    roots
}

/// The marker a live run holds a shared `flock` on in each directory it
/// [`claim`]s.
const OWNER: &str = ".harness-owner";

/// Mark `dir` (a scenario root) as this run's until the process exits:
/// the startup sweep of any other run then leaves the dead mounts under
/// it alone. Claims on directories that are gone (a scenario's root,
/// removed with its `TempDir`) are dropped as new ones come.
pub fn claim(dir: &Path) {
    use std::collections::HashMap;
    use std::os::fd::AsRawFd;
    use std::sync::Mutex;
    static HELD: Mutex<Option<HashMap<PathBuf, std::fs::File>>> = Mutex::new(None);
    let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
    let held = held.get_or_insert_with(HashMap::new);
    held.retain(|d, _| d.join(OWNER).exists());
    if held.contains_key(dir) {
        return;
    }
    let Ok(file) = std::fs::File::create(dir.join(OWNER)) else {
        return;
    };
    // SAFETY: a plain `flock(2)` on a descriptor we own.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } == 0 {
        held.insert(dir.to_path_buf(), file);
    }
}

/// Whether a live run has [`claim`]ed a directory between `root` and
/// `mountpoint`.
fn claimed(mountpoint: &Path, root: &Path) -> bool {
    use std::os::fd::AsRawFd;
    mountpoint
        .ancestors()
        .skip(1)
        .take_while(|d| *d != root)
        .any(|d| {
            let Ok(file) = std::fs::File::open(d.join(OWNER)) else {
                return false;
            };
            // SAFETY: a plain `flock(2)` on a descriptor we own; the lock,
            // if taken, goes with the descriptor.
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) != 0 }
        })
}

/// Whether `path` answers `ENOTCONN` (a FUSE mount whose daemon is gone).
/// A mount that hangs is not dead: nothing is done to it, and the thread
/// that asked stays blocked on it for good (leaked; the sweep runs once).
fn dead(path: &Path) -> bool {
    let (tx, rx) = std::sync::mpsc::channel();
    let p = path.to_path_buf();
    std::thread::spawn(move || {
        let _ = tx.send(std::fs::metadata(&p));
    });
    match rx.recv_timeout(Duration::from_secs(3)) {
        Ok(Err(e)) => e.raw_os_error() == Some(libc::ENOTCONN),
        _ => false,
    }
}

/// The FUSE mounts in `mounts` under `<root>/harness-*`: (mountpoint,
/// its temp root).
pub fn candidates(
    mounts: &[constellation_platform::MountEntry],
    roots: &[PathBuf],
) -> Vec<(PathBuf, PathBuf)> {
    mounts
        .iter()
        .filter(|m| m.fstype == "fuse" || m.fstype.starts_with("fuse."))
        .filter_map(|m| {
            let root = roots.iter().find(|r| {
                m.mountpoint
                    .strip_prefix(r)
                    .ok()
                    .and_then(|rest| rest.components().next())
                    .is_some_and(|c| c.as_os_str().to_string_lossy().starts_with("harness-"))
            })?;
            Some((m.mountpoint.clone(), root.clone()))
        })
        .collect()
}

/// Lazily unmount the dead FUSE mounts under the temp roots that no live
/// run owns.
pub fn mounts() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let table = &constellation_platform::native().mounts;
        let Ok(list) = table.list() else {
            return;
        };
        for (mp, root) in candidates(&list, &temp_roots()) {
            if !claimed(&mp, &root) && dead(&mp) {
                eprintln!("=== sweep: unmounting dead mount {}", mp.display());
                let _ = table.unmount(&mp, constellation_platform::UnmountMode::Lazy);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(name: &str, prefix: &str) -> Row {
        Row {
            id: format!("id-{name}"),
            name: name.into(),
            prefix: prefix.into(),
        }
    }

    #[test]
    fn another_prefix_is_never_matched_by_its_name() {
        let rows = [
            row("constellation-harness-floci", ""),
            row("constellation-harness-toxiproxy", "constellation-harness"),
            row(
                "constellation-harness-p30-floci",
                "constellation-harness-p30",
            ),
            row("constellation-harness-p30-floci2", ""),
            row("htd-floci", ""),
        ];
        let got: Vec<_> = owned_by(&rows, "constellation-harness")
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(
            got,
            [
                "constellation-harness-floci",
                "constellation-harness-toxiproxy"
            ]
        );
        assert_eq!(owned_by(&rows, "htd").len(), 1);
    }

    #[test]
    fn only_fuse_mounts_under_harness_dirs_of_a_temp_root_are_candidates() {
        use constellation_platform::MountEntry;
        let m = |mp: &str, ty: &str| MountEntry {
            mountpoint: mp.into(),
            fstype: ty.into(),
            source: "x".into(),
            device: None,
        };
        let list = [
            m("/tmp/harness-a-1/c0/mnt", "fuse"),
            m("/tmp/harness-a-1/c1/mnt", "ext4"),
            m("/tmp/other/mnt", "fuse"),
            m("/var/tmp/htd/harness-b-2/mnt", "fuse.constellation"),
            m("/var/tmp/harness-c/mnt", "fuse"),
        ];
        let got = candidates(
            &list,
            &[PathBuf::from("/tmp"), PathBuf::from("/var/tmp/htd")],
        );
        assert_eq!(
            got,
            [
                (
                    PathBuf::from("/tmp/harness-a-1/c0/mnt"),
                    PathBuf::from("/tmp")
                ),
                (
                    PathBuf::from("/var/tmp/htd/harness-b-2/mnt"),
                    PathBuf::from("/var/tmp/htd")
                )
            ]
        );
    }

    /// A claimed scenario root is a live run's: what is mounted under it
    /// is left alone, as seen from here (a separate open file conflicts
    /// as another process's would) and from another process.
    #[test]
    fn a_claimed_root_shields_its_mounts_and_an_unclaimed_one_does_not() {
        let root = tempfile::tempdir().unwrap();
        let mine = root.path().join("harness-mine-1");
        let other = root.path().join("harness-other-1");
        for d in [&mine, &other] {
            std::fs::create_dir_all(d.join("c0/mnt")).unwrap();
        }
        // A dead run's marker: present, unlocked.
        std::fs::File::create(other.join(OWNER)).unwrap();
        claim(&mine);
        claim(&mine);
        assert!(claimed(&mine.join("c0/mnt"), root.path()));
        assert!(!claimed(&other.join("c0/mnt"), root.path()));
        let st = std::process::Command::new("flock")
            .args(["-n", "-x"])
            .arg(mine.join(OWNER))
            .arg("true")
            .status()
            .unwrap();
        assert!(!st.success(), "another process took a claimed root's lock");
    }
}
