//! Plan 45 H4: a burst of hard links through two real mounts.
//!
//! A dentry carries a copy of its inode's attributes only while the inode is
//! a directory or has one name (plan 45, ADR-36); a non-directory with two or
//! more names has `Remote` dentries, and every write of the inode record is
//! one key whatever the number of names. Before that rule `link`, `unlink`,
//! `chmod` and a write commit on an inode of *n* names rewrote *n* dentries,
//! a burst of *n* links cost O(n²), and `stress-ng --link` left 773 MB of
//! journal behind. This scenario is the system-level check of the rule: both
//! nodes must hold the same namespace through the burst, the transitions on
//! the way up (1 → 2 names) and down (2 → 1), and a change of the inode at
//! thousands of names, and the replica must stay small.
//!
//! The burst is one file `d/base` linked to 8,192 names spread over two
//! directories, with a `chmod` and an append at four points on the way up and
//! three on the way down (the commits that used to rewrite every name), then
//! every extra name unlinked again. Node A does all of it; node B replays
//! and, meanwhile, lists the directories with `ls -lai` in a loop and walks
//! them with `du`, so READDIRPLUS pages and `lookup`s of `Remote` entries run
//! under the changes. The scenario passes when, once B has caught up after
//! each phase:
//!
//! - B's tree equals A's, name for name: kind, mode, owner, link count, size,
//!   inode number, mtime, ctime and content (every distinct inode read once),
//!   and A's own tree is what the burst should have produced (`nlink` of the
//!   file is 8,193 at the top and 1 at the bottom, with the last `chmod`'s
//!   mode and the appended size), so an equal-but-wrong pair does not pass;
//! - `ls -lai` of each directory prints the same text on B as on A;
//! - `du` (apparent and on disk) of the tree is the same on both, and counts
//!   the file once, not once per name;
//! - the replica's `meta.db` is under 64 MiB on both nodes, at the top and at
//!   the bottom;
//! - `fsck` of each node is clean (it checks the published tree's dentry
//!   forms and every inode's link count against its names), and neither node
//!   recorded a conflict;
//! - the cost of a `link` does not grow with the number of names: the last
//!   1,024 links of the burst average at most `HARDLINK_BURST_RATIO` times
//!   the first 1,024 (the quadratic code is 40 times and more).
//!
//! Sizes: `HARDLINK_BURST_LINKS` (8,192, the plan's), `HARDLINK_BURST_META_MIB`
//! (64), `HARDLINK_BURST_RATIO` (20).

use super::eventually;
use super::readdirplus::{env_usize, Fixture, Loop};
use crate::client::Client;
use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

const NAME: &str = "hardlink-burst";

/// What one name of the tree shows.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    kind: char,
    mode: u32,
    uid: u32,
    gid: u32,
    nlink: u64,
    size: u64,
    ino: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
    /// A regular file's content hash; a symlink's target hashed.
    content: u64,
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

/// Every name under `root`, relative, with what `lstat` and a read show. A
/// file's content is read once per inode.
fn walk(root: &Path) -> Result<BTreeMap<String, Entry>> {
    fn go(
        root: &Path,
        dir: &Path,
        read: &mut HashMap<u64, u64>,
        out: &mut BTreeMap<String, Entry>,
    ) -> Result<()> {
        for e in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
            let e = e?;
            let path = e.path();
            let md = std::fs::symlink_metadata(&path)
                .with_context(|| format!("lstat {}", path.display()))?;
            let ft = md.file_type();
            let (kind, content) = if ft.is_dir() {
                ('d', 0)
            } else if ft.is_symlink() {
                (
                    'l',
                    hash_bytes(
                        std::fs::read_link(&path)?
                            .to_string_lossy()
                            .into_owned()
                            .as_bytes(),
                    ),
                )
            } else if ft.is_file() {
                let c = match read.get(&md.ino()) {
                    Some(c) => *c,
                    None => {
                        let c = hash_bytes(
                            &std::fs::read(&path)
                                .with_context(|| format!("reading {}", path.display()))?,
                        );
                        read.insert(md.ino(), c);
                        c
                    }
                };
                ('f', c)
            } else if ft.is_fifo() {
                ('p', 0)
            } else {
                ('?', 0)
            };
            let rel = path.strip_prefix(root)?.to_string_lossy().into_owned();
            out.insert(
                rel,
                Entry {
                    kind,
                    mode: md.mode() & 0o7777,
                    uid: md.uid(),
                    gid: md.gid(),
                    nlink: md.nlink(),
                    size: md.len(),
                    ino: md.ino(),
                    mtime: (md.mtime(), md.mtime_nsec()),
                    ctime: (md.ctime(), md.ctime_nsec()),
                    content,
                },
            );
            if ft.is_dir() {
                go(root, &path, read, out)?;
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    go(root, root, &mut HashMap::new(), &mut out)?;
    Ok(out)
}

/// The differences between two trees, at most `max` lines.
fn tree_diff(a: &BTreeMap<String, Entry>, b: &BTreeMap<String, Entry>, max: usize) -> Vec<String> {
    let mut out = Vec::new();
    for (name, x) in a {
        match b.get(name) {
            None => out.push(format!("{name}: only on A: {x:?}")),
            Some(y) if x != y => out.push(format!("{name}: A {x:?}, B {y:?}")),
            _ => {}
        }
    }
    for (name, y) in b {
        if !a.contains_key(name) {
            out.push(format!("{name}: only on B: {y:?}"));
        }
    }
    out.truncate(max);
    out
}

/// `ls -lai` of `dir`, as printed.
fn ls_lai(dir: &Path) -> Result<String> {
    let out = Command::new("ls")
        .env("LC_ALL", "C")
        .args(["-lai", "--time-style=+%s.%N"])
        .arg(dir)
        .output()
        .context("running ls")?;
    ensure!(
        out.status.success(),
        "ls -lai {} failed ({}): {}",
        dir.display(),
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `du -s` of `dir`: apparent bytes, or blocks on disk.
fn du(dir: &Path, apparent: bool) -> Result<u64> {
    let mut cmd = Command::new("du");
    cmd.args(["-s", "--block-size=1"]);
    if apparent {
        cmd.arg("--apparent-size");
    }
    let out = cmd.arg(dir).output().context("running du")?;
    ensure!(
        out.status.success(),
        "du {} failed ({}): {}",
        dir.display(),
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .with_context(|| format!("du printed {:?}", out.stdout))
}

/// The store's bytes on disk (journals and tables).
fn replica_bytes(c: &Client) -> u64 {
    crate::bench::dir_size(&c.replica_db())
}

/// The mean of a latency window.
fn mean(window: &[Duration]) -> Duration {
    window.iter().sum::<Duration>() / window.len().max(1) as u32
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// `fsck` of a mounted node: clean, or the report.
fn fsck_clean(c: &Client, when: &str) -> Result<()> {
    let out = c.fsck(false)?;
    ensure!(
        out.status.success(),
        "{} fsck {when}: exit {:?}\n{}{}",
        c.name,
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).context("fsck's report")?;
    ensure!(
        report["clean"].as_bool() == Some(true)
            && report["issues"].as_array().is_some_and(|i| i.is_empty()),
        "{} fsck {when} is not clean: {report}",
        c.name
    );
    Ok(())
}

/// What the file must be.
struct Expect {
    mode: u32,
    size: u64,
}

/// A's tree against the plan: the base file at `names` names with the mode and
/// size the changes left, `singles` single-name files untouched, and
/// `names - 1` extra names all showing the file's own attributes.
fn check_shape(
    tree: &BTreeMap<String, Entry>,
    extras: &[String],
    names: u64,
    want: &Expect,
    singles: usize,
) -> Result<()> {
    let base = tree.get("d/base").context("d/base is missing")?;
    ensure!(
        base.nlink == names && base.mode == want.mode && base.size == want.size,
        "d/base shows nlink {}, mode {:o}, size {}; want {names}, {:o}, {}",
        base.nlink,
        base.mode,
        base.size,
        want.mode,
        want.size
    );
    for e in extras {
        let x = tree.get(e).with_context(|| format!("{e} is missing"))?;
        ensure!(
            x == base,
            "{e} shows {x:?}, d/base shows {base:?}: names of one inode differ"
        );
    }
    let n_singles = tree
        .iter()
        .filter(|(k, v)| k.starts_with("d/s") && v.nlink == 1)
        .count();
    ensure!(
        n_singles == singles,
        "{n_singles} single-name files, want {singles}"
    );
    let pair = (tree.get("d/p0"), tree.get("d/p1"));
    match pair {
        (Some(p0), Some(p1)) => ensure!(
            p0.ino == p1.ino && p0.nlink == 2 && p1.nlink == 2,
            "the pair d/p0, d/p1 shows {p0:?} and {p1:?}"
        ),
        _ => bail!("the pair d/p0, d/p1 is missing"),
    }
    Ok(())
}

pub fn burst(_seed: u64) -> Result<()> {
    let links = env_usize("HARDLINK_BURST_LINKS", 8192);
    let meta_max = env_usize("HARDLINK_BURST_META_MIB", 64) as u64 * (1 << 20);
    let ratio = env_usize("HARDLINK_BURST_RATIO", 20) as u32;
    let mut fx = Fixture::new(NAME, &["a", "b"], &[], None, &[])?;
    let r = (|| -> Result<()> {
        let (a, b) = (&fx.clients[0], &fx.clients[1]);
        let da = a.mnt.join("d");
        let singles = 64usize;
        std::fs::create_dir(&da)?;
        std::fs::create_dir(a.mnt.join("e"))?;
        let mut content = vec![0u8; 4096];
        for (i, byte) in content.iter_mut().enumerate() {
            *byte = (i * 31 % 251) as u8;
        }
        std::fs::write(da.join("base"), &content)?;
        std::fs::set_permissions(da.join("base"), std::fs::Permissions::from_mode(0o644))?;
        for i in 0..singles {
            std::fs::write(da.join(format!("s{i:03}")), format!("single {i}"))?;
        }
        std::fs::write(da.join("p0"), b"a pair")?;
        std::fs::hard_link(da.join("p0"), da.join("p1"))?;
        let mut want = Expect {
            mode: 0o644,
            size: content.len() as u64,
        };
        let sync = |what: &str, want: &BTreeMap<String, Entry>| -> Result<()> {
            eventually(what, Duration::from_secs(180), || {
                let got = walk(&b.mnt)?;
                let d = tree_diff(want, &got, 5);
                ensure!(d.is_empty(), "B's tree differs from A's: {d:?}");
                Ok(())
            })
        };
        let mut tree = walk(&a.mnt)?;
        check_shape(&tree, &[], 1, &want, singles)?;
        sync("B holds the starting tree", &tree)?;

        // B lists and walks the directories while A changes them.
        let lister = {
            let (d, e) = (b.mnt.join("d"), b.mnt.join("e"));
            // A name that vanishes under a listing is the burst's own doing.
            let vanished = |r: Result<()>| match r {
                Err(err) if !format!("{err:#}").contains("No such file") => Err(err),
                _ => Ok(()),
            };
            Loop::start(move || {
                for dir in [&d, &e] {
                    vanished(ls_lai(dir).map(|_| ()))?;
                }
                vanished(du(&d, true).map(|_| ()))
            })
        };

        // ---- up: 1 -> 2 -> ... -> links + 1 names
        let target = |i: usize| -> String {
            if i.is_multiple_of(2) {
                format!("d/l{i:05}")
            } else {
                format!("e/l{i:05}")
            }
        };
        let mut extras: Vec<String> = Vec::with_capacity(links);
        let mut took: Vec<Duration> = Vec::with_capacity(links);
        let mut flip = false;
        let t_up = Instant::now();
        for i in 0..links {
            let t = Instant::now();
            std::fs::hard_link(da.join("base"), a.mnt.join(target(i)))
                .with_context(|| format!("link {i}"))?;
            took.push(t.elapsed());
            extras.push(target(i));
            if (i + 1) % (links / 4).max(1) == 0 {
                // A change of the inode at i + 2 names.
                flip = !flip;
                want.mode = if flip { 0o640 } else { 0o600 };
                std::fs::set_permissions(
                    da.join("base"),
                    std::fs::Permissions::from_mode(want.mode),
                )?;
                use std::io::Write;
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(da.join("base"))?
                    .write_all(&[b'+'; 512])?;
                want.size += 512;
            }
        }
        let up = t_up.elapsed();
        tree = walk(&a.mnt)?;
        check_shape(&tree, &extras, links as u64 + 1, &want, singles)?;
        sync("B holds the top of the burst", &tree)?;
        let (up_a, up_b) = (replica_bytes(a), replica_bytes(b));

        let lai_a: Vec<String> = ["d", "e"]
            .iter()
            .map(|d| ls_lai(&a.mnt.join(d)))
            .collect::<Result<_>>()?;
        eventually(
            "B's ls -lai equals A's at the top",
            Duration::from_secs(60),
            || {
                for (d, want) in ["d", "e"].iter().zip(&lai_a) {
                    let got = ls_lai(&b.mnt.join(d))?;
                    ensure!(&got == want, "ls -lai {d} differs on B");
                }
                Ok(())
            },
        )?;
        let du_top = [du(&a.mnt, true)?, du(&a.mnt, false)?];
        eventually(
            "B's du equals A's at the top",
            Duration::from_secs(60),
            || {
                let got = [du(&b.mnt, true)?, du(&b.mnt, false)?];
                ensure!(got == du_top, "du on B {got:?}, on A {du_top:?}");
                Ok(())
            },
        )?;
        // One inode counted once: the apparent size of the tree is the file,
        // the single-name files, the pair and the directories, not 8,193 files.
        ensure!(
            du_top[0] < (links as u64 * want.size) / 4,
            "du counts the file once per name: {} bytes for {links} names of {} bytes",
            du_top[0],
            want.size
        );
        ensure!(
            up_a < meta_max && up_b < meta_max,
            "meta.db at the top of the burst: A {up_a} bytes, B {up_b} bytes, bound {meta_max}"
        );
        fsck_clean(a, "at the top")?;
        fsck_clean(b, "at the top")?;

        // ---- down: unlink every extra name, back to one
        let t_down = Instant::now();
        let mut flip = false;
        for (i, name) in extras.iter().enumerate() {
            std::fs::remove_file(a.mnt.join(name)).with_context(|| format!("unlink {name}"))?;
            if (i + 1) % (links / 4).max(1) == 0 && i + 1 < links {
                flip = !flip;
                want.mode = if flip { 0o664 } else { 0o600 };
                std::fs::set_permissions(
                    da.join("base"),
                    std::fs::Permissions::from_mode(want.mode),
                )?;
                // A truncate and a chmod of the inode at links - i - 1 names.
                want.size = content.len() as u64 + (i as u64 % 7);
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(da.join("base"))?
                    .set_len(want.size)?;
            }
        }
        let down = t_down.elapsed();
        let looped = lister.finish().context("B's ls/du loop")?;
        tree = walk(&a.mnt)?;
        check_shape(&tree, &[], 1, &want, singles)?;
        ensure!(
            tree.keys().all(|k| !k.contains("/l0")),
            "an extra name is still on A"
        );
        sync("B holds the bottom of the burst", &tree)?;
        let (down_a, down_b) = (replica_bytes(a), replica_bytes(b));
        let lai_a: Vec<String> = ["d", "e"]
            .iter()
            .map(|d| ls_lai(&a.mnt.join(d)))
            .collect::<Result<_>>()?;
        eventually(
            "B's ls -lai equals A's at the bottom",
            Duration::from_secs(60),
            || {
                for (d, want) in ["d", "e"].iter().zip(&lai_a) {
                    let got = ls_lai(&b.mnt.join(d))?;
                    ensure!(&got == want, "ls -lai {d} differs on B");
                }
                Ok(())
            },
        )?;
        let du_bottom = [du(&a.mnt, true)?, du(&a.mnt, false)?];
        eventually(
            "B's du equals A's at the bottom",
            Duration::from_secs(60),
            || {
                let got = [du(&b.mnt, true)?, du(&b.mnt, false)?];
                ensure!(got == du_bottom, "du on B {got:?}, on A {du_bottom:?}");
                Ok(())
            },
        )?;
        ensure!(
            down_a < meta_max && down_b < meta_max,
            "meta.db at the bottom: A {down_a} bytes, B {down_b} bytes, bound {meta_max}"
        );
        fsck_clean(a, "at the bottom")?;
        fsck_clean(b, "at the bottom")?;
        super::ensure_no_conflicts(&[a, b])?;

        // The cost of a link does not grow with the names.
        let window = 1024.min(links / 2).max(1);
        let (first, last) = (mean(&took[..window]), mean(&took[links - window..]));
        let mut sorted = took.clone();
        sorted.sort();
        eprintln!(
            "    {NAME}: {links} links in {up:?} (first {window} avg {first:?}, last {window} avg \
             {last:?}, p50 {:?}, p99 {:?}, max {:?}), {links} unlinks in {down:?}; B's ls/du loop \
             ran {looped} times; meta.db top A {up_a} B {up_b}, bottom A {down_a} B {down_b} bytes; \
             du apparent/disk top {du_top:?}, bottom {du_bottom:?}",
            percentile(&sorted, 0.5),
            percentile(&sorted, 0.99),
            sorted[sorted.len() - 1],
        );
        ensure!(
            last <= first * ratio,
            "a link got slower with the number of names: the first {window} averaged {first:?}, \
             the last {window} {last:?} (bound {ratio}x)"
        );
        Ok(())
    })();
    fx.unmount_all();
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree_of(dir: &Path) -> BTreeMap<String, Entry> {
        walk(dir).unwrap()
    }

    /// The comparison the scenario rests on sees each kind of difference two
    /// replicas could show; a pair of equal trees passes.
    #[test]
    fn the_tree_comparison_sees_a_difference() {
        let mk = || {
            let t = tempfile::tempdir().unwrap();
            std::fs::create_dir(t.path().join("d")).unwrap();
            std::fs::write(t.path().join("d/base"), b"content").unwrap();
            std::fs::hard_link(t.path().join("d/base"), t.path().join("d/l0")).unwrap();
            std::fs::hard_link(t.path().join("d/base"), t.path().join("d/l1")).unwrap();
            t
        };
        let x = mk();
        // The same tree equals itself.
        let t = tree_of(x.path());
        assert!(tree_diff(&t, &t, 10).is_empty());
        // One link fewer on the other side: a missing name and a link count.
        std::fs::remove_file(x.path().join("d/l1")).unwrap();
        let less = tree_of(x.path());
        let d = tree_diff(&t, &less, 10);
        assert!(d.iter().any(|l| l.starts_with("d/l1: only on A")), "{d:?}");
        assert!(d.iter().any(|l| l.starts_with("d/base: A")), "{d:?}");
        // A mode, a size and a content change are each seen.
        for change in 0..3 {
            let z = mk();
            let mut a = tree_of(z.path());
            let b = a.clone();
            let e = a.get_mut("d/l0").unwrap();
            match change {
                0 => e.mode ^= 0o20,
                1 => e.size += 1,
                _ => e.content ^= 1,
            }
            assert_eq!(tree_diff(&a, &b, 10).len(), 1, "change {change}");
        }
    }

    /// The shape check refuses a file whose names disagree or whose link
    /// count is not the plan's, so a pair of equal but wrong trees fails.
    #[test]
    fn the_shape_check_refuses_a_wrong_link_count() {
        let t = tempfile::tempdir().unwrap();
        std::fs::create_dir(t.path().join("d")).unwrap();
        std::fs::write(t.path().join("d/base"), b"content").unwrap();
        std::fs::set_permissions(
            t.path().join("d/base"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        std::fs::write(t.path().join("d/p0"), b"a pair").unwrap();
        std::fs::hard_link(t.path().join("d/p0"), t.path().join("d/p1")).unwrap();
        let tree = tree_of(t.path());
        let want = Expect {
            mode: 0o644,
            size: 7,
        };
        check_shape(&tree, &[], 1, &want, 0).unwrap();
        assert!(check_shape(&tree, &[], 2, &want, 0).is_err());
        let wrong = Expect {
            mode: 0o600,
            size: 7,
        };
        assert!(check_shape(&tree, &[], 1, &wrong, 0).is_err());
        assert!(check_shape(&tree, &[], 1, &want, 3).is_err());
        // An extra name that is not a name of the file.
        assert!(check_shape(&tree, &["d/p1".to_string()], 1, &want, 0).is_err());
    }
}
