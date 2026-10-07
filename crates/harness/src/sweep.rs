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
        for r in targets(&rows, own, DEFAULT_PREFIX, crate::s3env::prefix_in_use) {
            eprintln!(
                "=== sweep: removing leftover container {} ({})",
                r.name, r.id
            );
            let _ = crate::docker::docker(&["rm", "-f", &r.id]);
        }
    });
}

/// The rows a run of prefix `own` removes: its own, and `default`'s
/// unless `in_use` says a live run holds that prefix's lock.
pub fn targets<'a>(
    rows: &'a [Row],
    own: &str,
    default: &str,
    in_use: impl Fn(&str) -> bool,
) -> Vec<&'a Row> {
    let mut prefixes = vec![own];
    if own != default && !in_use(default) {
        prefixes.push(default);
    }
    prefixes
        .into_iter()
        .flat_map(|p| owned_by(rows, p))
        .collect()
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
    /// A live `k8s-scenario` run holds its prefix lock (another process):
    /// a sweep of another prefix, with that one as the default, leaves
    /// its containers; once the holder is gone it takes them.
    #[test]
    fn a_held_k8s_lock_keeps_the_default_sweep_off_its_container() {
        use std::io::{BufRead, Write};
        const HOLD: &str = "HARNESS_TEST_SWEEP_HOLD";
        const NAME: &str =
            "sweep::tests::a_held_k8s_lock_keeps_the_default_sweep_off_its_container";
        if std::env::var(HOLD).is_ok() {
            let _lock = crate::s3env::hold_prefix().unwrap();
            println!("held");
            let _ = std::io::stdout().flush();
            let mut line = String::new();
            let _ = std::io::stdin().lock().read_line(&mut line);
            return;
        }
        let default = format!("constellation-harness-sweeptest-{}", std::process::id());
        let rows = vec![Row {
            id: "1".into(),
            name: "kind-k8s-floci-1-2".into(),
            prefix: default.clone(),
        }];
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([NAME, "--exact", "--nocapture", "--quiet"])
            .env(HOLD, "1")
            .env("CONSTELLATION_HARNESS_DOCKER_PREFIX", &default)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut out = std::io::BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut l = String::new();
            assert!(out.read_line(&mut l).unwrap() > 0, "the holder died");
            if l.trim() == "held" {
                break;
            }
        }
        let own = "constellation-harness-sweeptest-other";
        let while_held = targets(&rows, own, &default, crate::s3env::prefix_in_use).len();
        drop(child.stdin.take());
        child.wait().unwrap();
        let after = targets(&rows, own, &default, crate::s3env::prefix_in_use).len();
        assert_eq!(while_held, 0, "swept a container of a held prefix");
        assert_eq!(after, 1, "a free prefix's container is left");
    }

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
