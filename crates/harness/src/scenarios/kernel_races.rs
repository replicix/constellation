//! Plan 42 §7.2: three races in the Linux kernel's FUSE client, through a
//! real mount — the workloads of `crate::kernel_races`, the fault points
//! that widen their windows (`CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US`,
//! `…_VIEW_WRITE_DELAY_US`, `…_DIR_GETATTR_DELAY_MS`,
//! `…_GETATTR_DELAY_INO`, `…_COMMIT_FAIL`), and what each workaround must
//! leave of them.
//!
//! Each scenario asserts the plan's "after" column: a scenario of a race
//! the workarounds close fails on a build without them (that is its
//! fail-before; the counts it prints as one `RESULT` line are what
//! PROGRESS records), the guards pass on both. The race scenarios run
//! once with the transport ladder (`CONSTELLATION_FUSE_TRANSPORT=auto`)
//! and once with `io_uring_setup(2)` denied (`…-dev-fuse`), except those
//! that hand a session over, which `/dev/fuse` alone can do.
//!
//! The workaround counters come from `node.status`
//! (`fuse.mounts[].kernel_cache`); a counter a build does not report
//! reads as 0, so a guard that wants one at 0 passes on any build and a
//! row that needs one above 0 fails on a build without it.
//!
//! `CONSTELLATION_HARNESS_KERNEL_RACE_SECS` shortens the 60 s runs (a
//! development aid; the plan's numbers are for 60 s).

use super::{eventually, setup, ts};
use crate::client::Client;
use crate::kernel_races::{
    dir_version, flock_turns, nlink_race, one_write, pc_prepare, pc_race, rename_ctime,
    reopen_matches_view, Dropper, PcConfig, PcCounts,
};
use crate::s3env::{S3Env, BUCKET};
use anyhow::{ensure, Context, Result};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// How a scenario's mounts reach the kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Wire {
    /// The transport ladder (a ring where the host offers one).
    Auto,
    /// `/dev/fuse`, `io_uring_setup(2)` refused.
    DevFuse,
}

fn secs() -> u64 {
    std::env::var("CONSTELLATION_HARNESS_KERNEL_RACE_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|s| *s > 0)
        .unwrap_or(60)
}

fn client(env: &S3Env, root: &Path, name: &str, backend: &str, wire: Wire) -> Result<Client> {
    let c = Client::new(root, name, &env.direct_endpoint, backend)?;
    Ok(match wire {
        Wire::Auto => c.with_env("CONSTELLATION_FUSE_TRANSPORT", "auto"),
        Wire::DevFuse => c
            .with_env("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse")
            .with_io_uring_denied(),
    })
}

/// One node, `extra` in its environment, `args` on its mount.
fn one_node(
    name: &str,
    wire: Wire,
    extra: &[(&str, &str)],
    args: &[&str],
) -> Result<(S3Env, tempfile::TempDir, Client)> {
    let (env, root) = setup(name)?;
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend, wire)?;
    for (k, v) in extra {
        c = c.with_env(k, v);
    }
    c.fs_create()?;
    c.mount_view(None, args)?;
    Ok((env, root, c))
}

/// Two nodes with P2P between them (cluster locks, data notifications).
fn two_nodes(
    name: &str,
    wire: Wire,
    extra: &[(&str, &str)],
) -> Result<(S3Env, tempfile::TempDir, Vec<Client>)> {
    let (env, root) = setup(name)?;
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let mut clients = Vec::new();
    for who in ["a", "b"] {
        let mut c = client(&env, root.path(), who, &backend, wire)?.with_own_node_key();
        for (k, v) in extra {
            c = c.with_env(k, v);
        }
        clients.push(c);
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount_view(None, &["--locks", "cluster"])?;
    }
    let refs: Vec<&Client> = clients.iter().collect();
    super::wait_for_p2p(&refs)?;
    Ok((env, root, clients))
}

fn unmount_all(clients: &mut [Client]) {
    for c in clients.iter_mut() {
        let _ = c.unmount();
    }
}

/// The `fuse.mounts[]` entry of the mount at `mnt` (or the only one).
fn mount_status(c: &Client, mnt: Option<&Path>) -> Result<serde_json::Value> {
    let status = c.control_status()?;
    let mounts = status["fuse"]["mounts"]
        .as_array()
        .context("no fuse.mounts array")?
        .clone();
    match mnt {
        None => mounts.first().cloned().context("no FUSE mount"),
        Some(mnt) => mounts
            .into_iter()
            .find(|m| m["mountpoint"].as_str().map(Path::new) == Some(mnt))
            .with_context(|| format!("no FUSE mount at {}", mnt.display())),
    }
}

/// A `fuse.mounts[].kernel_cache` counter (`a.b` names a nested one); 0
/// when the build reports none (module doc).
fn counter(m: &serde_json::Value, key: &str) -> u64 {
    let mut v = &m["kernel_cache"];
    for part in key.split('.') {
        v = &v[part];
    }
    v.as_u64().unwrap_or(0)
}

/// The counters a race scenario prints and checks.
const COUNTERS: &[&str] = &[
    "keep_cache_opens",
    "dropping_opens.no_entry",
    "dropping_opens.suspect",
    "dropping_opens.stamp_changed",
    "dropping_opens.no_stamp",
    "dropping_opens.drop_in_flight",
    "repairs",
    "repair_pages",
    "repairs_late",
    "repairs_released",
    "repair_wait_ms",
    "repair_wait_max_ms",
    "ambiguous_writes",
    "dir_attr_notices",
    "dir_attr_notices_skipped",
];

/// Every dropping open, whatever its reason.
fn dropping(m: &serde_json::Value) -> u64 {
    m["kernel_cache"]["dropping_opens"]
        .as_object()
        .map_or(0, |o| o.values().filter_map(|v| v.as_u64()).sum())
}

fn counters_line(m: &serde_json::Value) -> String {
    COUNTERS
        .iter()
        .map(|k| format!("{k}={}", counter(m, k)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn result(name: &str, line: &str) {
    eprintln!("    {name}: RESULT {line}");
}

/// `pc_race` on a fresh file of `c`'s mount.
fn run_pc(c: &Client, file: &str, cfg: &PcConfig) -> Result<PcCounts> {
    let path = c.mnt.join(file);
    pc_prepare(&path)?;
    pc_race(&path, cfg)
}

fn race_open(name: &str, wire: Wire, extra: &[(&str, &str)], args: &[&str]) -> Result<()> {
    let (_env, _root, mut c) = one_node(name, wire, extra, args)?;
    let r = (|| -> Result<()> {
        let counts = run_pc(&c, "pc.dat", &PcConfig::new(secs(), Dropper::Opener, 1))?;
        let m = mount_status(&c, None)?;
        result(name, &format!("{} {}", counts.line(), counters_line(&m)));
        ensure!(counts.writes > 0, "no writes");
        ensure!(
            counts.server_wrong == 0,
            "the daemon answered other bytes: {counts:?}"
        );
        ensure!(
            counts.stale == 0,
            "{} stale read-backs of acknowledged writes ({} with the daemon right)",
            counts.stale,
            counts.stale_server_correct
        );
        let keep = extra
            .iter()
            .any(|(k, v)| *k == "CONSTELLATION_FUSE_KEEP_CACHE" && *v == "off")
            || args.contains(&"strict");
        if !keep {
            ensure!(
                counter(&m, "keep_cache_opens") > 0,
                "no open kept the file's pages"
            );
        } else {
            if args.contains(&"strict") {
                ensure!(
                    counter(&m, "keep_cache_opens") == 0,
                    "a strict open kept the file's pages"
                );
            }
            ensure!(counter(&m, "repairs") > 0, "no write was repaired");
        }
        ensure!(counter(&m, "repairs_late") == 0, "a repair was late");
        Ok(())
    })();
    let _ = c.unmount();
    r
}

macro_rules! twice {
    ($auto:ident, $dev:ident, $body:expr) => {
        pub fn $auto(_seed: u64) -> Result<()> {
            ($body)(stringify!($auto), Wire::Auto)
        }
        pub fn $dev(_seed: u64) -> Result<()> {
            ($body)(stringify!($dev), Wire::DevFuse)
        }
    };
}

twice!(open, open_dev_fuse, |n: &str, w| race_open(
    &name(n),
    w,
    &[],
    &[]
));
twice!(open_repair, open_repair_dev_fuse, |n: &str, w| race_open(
    &name(n),
    w,
    &[("CONSTELLATION_FUSE_KEEP_CACHE", "off")],
    &[]
));
twice!(
    open_repair_delay,
    open_repair_delay_dev_fuse,
    |n: &str, w| race_open(
        &name(n),
        w,
        &[
            ("CONSTELLATION_FUSE_KEEP_CACHE", "off"),
            ("CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US", "200"),
        ],
        &[]
    )
);
twice!(strict, strict_dev_fuse, |n: &str, w| race_open(
    &name(n),
    w,
    &[],
    &["--cto", "strict"]
));

/// `kernel_race_open_dev_fuse` → `kernel-race-open-dev-fuse`.
fn name(fn_name: &str) -> String {
    format!("kernel-race-{}", fn_name.replace('_', "-"))
}

fn race_notify(name: &str, wire: Wire) -> Result<()> {
    let (_env, _root, mut clients) = two_nodes(
        name,
        wire,
        &[("CONSTELLATION_FAULT_FUSE_WRITE_DELAY_US", "200")],
    )?;
    let r = (|| -> Result<()> {
        let (a, b) = (&clients[0], &clients[1]);
        let path = a.mnt.join("pc.dat");
        pc_prepare(&path)?;
        let other = b.mnt.join("pc.dat");
        eventually("the file visible on B", Duration::from_secs(30), || {
            ensure!(other.exists(), "not yet");
            Ok(())
        })?;
        let stop = Arc::new(AtomicBool::new(false));
        let turn = Arc::new(AtomicU64::new(0));
        let lockers: Vec<_> = [(path.clone(), 0u64), (other.clone(), 1u64)]
            .into_iter()
            .map(|(p, me)| {
                let (turn, stop) = (turn.clone(), stop.clone());
                std::thread::spawn(move || flock_turns(&p, me, turn, stop))
            })
            .collect();
        let counts = pc_race(&path, &PcConfig::new(secs(), Dropper::None, 2));
        stop.store(true, Ordering::Relaxed);
        let mut locks = 0;
        for l in lockers {
            locks += l
                .join()
                .map_err(|_| anyhow::anyhow!("a locker panicked"))??;
        }
        let counts = counts?;
        let m = mount_status(a, None)?;
        result(
            name,
            &format!("{} locks={locks} {}", counts.line(), counters_line(&m)),
        );
        ensure!(locks > 10, "only {locks} lock turns");
        ensure!(counts.stale == 0, "{} stale read-backs", counts.stale);
        ensure!(counter(&m, "repairs_late") == 0, "a repair was late");
        Ok(())
    })();
    unmount_all(&mut clients);
    r
}

twice!(notify, notify_dev_fuse, |n: &str, w| race_notify(
    &name(n),
    w
));

fn race_notify_foreign(name: &str, wire: Wire) -> Result<()> {
    let (_env, _root, mut clients) = two_nodes(name, wire, &[])?;
    let r = (|| -> Result<()> {
        let (a, b) = (&clients[0], &clients[1]);
        let second = a.mnt.join("second.dat");
        pc_prepare(&second)?;
        let b_second = b.mnt.join("second.dat");
        eventually(
            "the second file visible on B",
            Duration::from_secs(30),
            || {
                ensure!(b_second.exists(), "not yet");
                Ok(())
            },
        )?;
        let stop = Arc::new(AtomicBool::new(false));
        // B overwrites the second file; A reads it.
        let writer = {
            let stop = stop.clone();
            std::thread::spawn(move || -> Result<u64> {
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    std::fs::write(&b_second, vec![(n % 251) as u8 + 1; 64 << 10])?;
                    n += 1;
                    std::thread::sleep(Duration::from_millis(50));
                }
                Ok(n)
            })
        };
        let reader = {
            let stop = stop.clone();
            let second = second.clone();
            std::thread::spawn(move || -> Result<()> {
                while !stop.load(Ordering::Relaxed) {
                    let _ = std::fs::read(&second);
                }
                Ok(())
            })
        };
        // The workload's own start-up (seven descriptors of `pc.dat`
        // opened at once, each a drop the next may overlap) may cost a
        // repair or two of `pc.dat`; the foreign change of the second file
        // must cost nothing after it: `repairs` is sampled once the
        // workload is under way and must not move again.
        let (counts, settled) = std::thread::scope(|s| {
            let sample = s.spawn(|| -> Result<u64> {
                std::thread::sleep(Duration::from_secs(3.min(secs() / 4).max(1)));
                Ok(counter(&mount_status(a, None)?, "repairs"))
            });
            let counts = run_pc(a, "pc.dat", &PcConfig::new(secs(), Dropper::None, 3));
            let settled = sample
                .join()
                .map_err(|_| anyhow::anyhow!("sampler panicked"))
                .and_then(|r| r);
            (counts, settled)
        });
        let settled = settled?;
        stop.store(true, Ordering::Relaxed);
        let overwrites = writer
            .join()
            .map_err(|_| anyhow::anyhow!("writer panicked"))??;
        reader
            .join()
            .map_err(|_| anyhow::anyhow!("reader panicked"))??;
        let counts = counts?;
        let m = mount_status(a, None)?;
        result(
            name,
            &format!(
                "{} overwrites={overwrites} repairs_at_start={settled} {}",
                counts.line(),
                counters_line(&m)
            ),
        );
        ensure!(counts.stale == 0, "{} stale read-backs", counts.stale);
        ensure!(
            counter(&m, "repairs") == settled,
            "a foreign change of another file cost A {} repairs",
            counter(&m, "repairs") - settled
        );
        Ok(())
    })();
    unmount_all(&mut clients);
    r
}

twice!(notify_foreign, notify_foreign_dev_fuse, |n: &str, w| {
    race_notify_foreign(&name(n), w)
});

/// Drops the daemon cannot see: `fadvise(DONTNEED)` (which, measured on
/// Linux 7.3, never drops a folio a writer holds — `mapping_evict_folio`
/// skips one with an elevated count — so it does not reproduce race 1),
/// and the session's own unannounced data notifications
/// (`CONSTELLATION_FAULT_UNSEEN_DROP_MS`), which stand in for a drop by
/// reclaim the bookkeeping cannot see. `extra` adds the naive keep-cache
/// knob for the fail-before of plan 42 F1 (run by hand: it must fail).
fn race_unseen_drop(name: &str, wire: Wire) -> Result<()> {
    let mut extra = vec![("CONSTELLATION_FAULT_UNSEEN_DROP_MS", "2")];
    let naive = std::env::var("CONSTELLATION_HARNESS_NAIVE_KEEP_CACHE").is_ok_and(|v| v == "1");
    if naive {
        extra.push(("CONSTELLATION_FAULT_KEEP_CACHE_IGNORE_SUSPECT", "1"));
    }
    let (_env, _root, mut c) = one_node(name, wire, &extra, &[])?;
    let r = (|| -> Result<()> {
        let counts = run_pc(&c, "pc.dat", &PcConfig::new(secs(), Dropper::Fadvise, 4))?;
        let m = mount_status(&c, None)?;
        result(name, &format!("{} {}", counts.line(), counters_line(&m)));
        ensure!(counts.writes > 0, "no writes");
        // R1: a stale read after a drop the daemon cannot see is allowed,
        // a stale page that survives a reopen is not (today's bound).
        ensure!(
            counts.stale_after_reopen == 0,
            "{} stale pages survived a reopen (of {} stale reads)",
            counts.stale_after_reopen,
            counts.stale
        );
        Ok(())
    })();
    let _ = c.unmount();
    r
}

twice!(unseen_drop, unseen_drop_dev_fuse, |n: &str, w| {
    race_unseen_drop(&name(n), w)
});

fn race_cfr(name: &str, wire: Wire) -> Result<()> {
    let (_env, _root, mut c) = one_node(name, wire, &[], &[])?;
    let r = (|| -> Result<()> {
        let dst = c.mnt.join("dst.dat");
        pc_prepare(&dst)?;
        let mut cfg = PcConfig::new(secs(), Dropper::Opener, 5);
        cfg.copy_to = Some(dst);
        let counts = run_pc(&c, "pc.dat", &cfg)?;
        result(name, &counts.line());
        ensure!(counts.copies > 0, "no copies");
        ensure!(counts.corrupt == 0, "{} corrupt copies", counts.corrupt);
        Ok(())
    })();
    let _ = c.unmount();
    r
}

twice!(cfr, cfr_dev_fuse, |n: &str, w| race_cfr(&name(n), w));

/// `pc_race` with the opener while `daemon --upgrade` runs at a third and
/// two thirds of the run; after each resume the file's first open drops
/// its pages (the resumed session's table starts empty).
fn race_handover(name: &str, keep: &str) -> Result<()> {
    let (_env, _root, mut c) = one_node(
        name,
        Wire::DevFuse,
        &[("CONSTELLATION_FUSE_KEEP_CACHE", keep)],
        &[],
    )?;
    let r = (|| -> Result<()> {
        let path = c.mnt.join("pc.dat");
        pc_prepare(&path)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut cfg = PcConfig::new(secs(), Dropper::Opener, 6);
        cfg.stop = Some(stop.clone());
        let worker = {
            let path = path.clone();
            std::thread::spawn(move || pc_race(&path, &cfg))
        };
        let third = Duration::from_secs(secs() / 3);
        let mut upgrades = Vec::new();
        let mut first_drops = Vec::new();
        for _ in 0..2 {
            std::thread::sleep(third);
            match super::handover::upgrade(&c) {
                Ok(took) => upgrades.push(took),
                Err(e) => {
                    stop.store(true, Ordering::Relaxed);
                    let _ = worker.join();
                    return Err(e);
                }
            }
            // The resumed session's counters start at 0, and its table
            // knows nothing current (the resume's re-invalidation marks
            // the open file, the seeded entry has no stamp): its first
            // opens of the file drop.
            std::thread::sleep(Duration::from_millis(500));
            let m = mount_status(&c, None)?;
            first_drops.push(dropping(&m));
        }
        let counts = worker
            .join()
            .map_err(|_| anyhow::anyhow!("the workload panicked"))??;
        let m = mount_status(&c, None)?;
        result(
            name,
            &format!(
                "{} upgrades={upgrades:?} drops_after_resume={first_drops:?} {}",
                counts.line(),
                counters_line(&m)
            ),
        );
        ensure!(counts.stale == 0, "{} stale read-backs", counts.stale);
        ensure!(
            first_drops.iter().all(|n| *n >= 1),
            "a resumed session's first open did not drop: {first_drops:?}"
        );
        if keep == "auto" {
            ensure!(counter(&m, "keep_cache_opens") > 0, "no open kept");
        }
        Ok(())
    })();
    let _ = c.unmount();
    r
}

pub fn handover(_seed: u64) -> Result<()> {
    race_handover("kernel-race-handover", "off")
}

pub fn handover_keep(_seed: u64) -> Result<()> {
    race_handover("kernel-race-handover-keep", "auto")
}

/// Phases of `pc_race` without a dropper; between phases every
/// descriptor closes (the publication, every second of which the fault
/// fails) and a reopened buffered read of each page must agree with what
/// the view answers (`O_DIRECT`).
fn race_uncommitted(name: &str, fault: &str, locks: &str) -> Result<()> {
    let (_env, _root, mut c) = one_node(
        name,
        Wire::Auto,
        &[("CONSTELLATION_FAULT_COMMIT_FAIL", fault)],
        &["--locks", locks],
    )?;
    let r = (|| -> Result<()> {
        let path = c.mnt.join("pc.dat");
        pc_prepare(&path)?;
        let phases = (secs() / 2).max(4);
        let mut total = PcCounts::default();
        let mut differing = 0;
        let mut checked = 0;
        for phase in 0..phases {
            let counts = pc_race(&path, &PcConfig::new(2, Dropper::None, 100 + phase))?;
            total.writes += counts.writes;
            total.stale += counts.stale;
            // Every descriptor of the phase is closed: the publication
            // happened (or failed) by now.
            let (pages, d) = reopen_matches_view(&path)?;
            checked += pages;
            differing += d;
            // The phase's descriptors may publish more than once, and a
            // later successful publication changes the stamp, which drops
            // the pages anyway: two single-publication steps, of which one
            // is failed, each checked on its own.
            for k in 0..2 {
                one_write(&path, phase * 2 + k, (phase * 2 + k) as u8 | 0x80)?;
                let (pages, d) = reopen_matches_view(&path)?;
                checked += pages;
                differing += d;
            }
        }
        let m = mount_status(&c, None)?;
        result(
            name,
            &format!(
                "phases={phases} writes={} pages_checked={checked} differing={differing} {}",
                total.writes,
                counters_line(&m)
            ),
        );
        ensure!(
            differing == 0,
            "{differing} reopened pages differed from what the view answers"
        );
        Ok(())
    })();
    let _ = c.unmount();
    r
}

pub fn uncommitted(_seed: u64) -> Result<()> {
    race_uncommitted("kernel-race-uncommitted", "pc.dat", "cluster")
}

pub fn uncommitted_local(_seed: u64) -> Result<()> {
    race_uncommitted("kernel-race-uncommitted-local", "pc.dat", "local")
}

pub fn uncommitted_lapse(_seed: u64) -> Result<()> {
    race_uncommitted("kernel-race-uncommitted-lapse", "lapse:pc.dat", "cluster")
}

pub fn discard_local(_seed: u64) -> Result<()> {
    race_uncommitted("kernel-race-discard-local", "lapse:pc.dat", "local")
}

/// `constellation mount INNER MNT` against `c`'s state dir: attaches a
/// second view to `c`'s daemon.
fn attach(c: &Client, endpoint: &str, inner: &str, mnt: &Path) -> Result<()> {
    let out = std::process::Command::new(super::handover::bin())
        .args([
            "mount",
            inner,
            mnt.to_str().unwrap(),
            "--s3",
            &c.backend,
            "--state-dir",
            c.state_dir().to_str().unwrap(),
        ])
        .env("AWS_ACCESS_KEY_ID", "test")
        .env("AWS_SECRET_ACCESS_KEY", "test")
        .env("AWS_DEFAULT_REGION", "us-east-1")
        .env("AWS_ENDPOINT", endpoint)
        .env("AWS_ALLOW_HTTP", "true")
        .output()?;
    ensure!(
        out.status.success(),
        "attaching {inner} at {}: {}",
        mnt.display(),
        String::from_utf8_lossy(&out.stderr)
    );
    eventually("the second view up", Duration::from_secs(30), || {
        ensure!(crate::client::is_mountpoint(mnt), "not yet");
        Ok(())
    })
}

/// One node, two views of the same files: A (the whole filesystem)
/// writes and closes, B (a subtree mount) reopens and reads; every read
/// must be what A wrote, and B's next open after each of A's commits
/// drops its pages.
pub fn other_view(_seed: u64) -> Result<()> {
    const NAME: &str = "kernel-race-other-view";
    let (env, root, mut c) = one_node(NAME, Wire::Auto, &[], &[])?;
    let sub = root.path().join("sub-mnt");
    let r = (|| -> Result<()> {
        std::fs::create_dir_all(c.mnt.join("d"))?;
        std::fs::create_dir_all(&sub)?;
        attach(&c, &env.direct_endpoint, "/d", &sub)?;
        let a = c.mnt.join("d/f");
        let b = sub.join("f");
        let mut stale = 0;
        let rounds = 200;
        for i in 0..rounds {
            let data = vec![(i % 251) as u8 + 1; 16 << 10];
            std::fs::write(&a, &data)?;
            // Twice through B: the second open may keep what the first
            // read, the first must see A's commit.
            for _ in 0..2 {
                if std::fs::read(&b)? != data {
                    stale += 1;
                }
            }
        }
        let m = mount_status(&c, Some(&sub))?;
        result(
            NAME,
            &format!("rounds={rounds} stale={stale} {}", counters_line(&m)),
        );
        ensure!(
            stale == 0,
            "{stale} reads through the second view were stale"
        );
        Ok(())
    })();
    let _ = std::process::Command::new("fusermount3")
        .arg("-u")
        .arg(&sub)
        .status();
    let _ = c.unmount();
    r
}

/// Race 2a. In bounded mode W3a's default is `entry` (plan 42 Q1, option
/// B: `zero` costs a `GETATTR` per directory component of every path
/// walk), so the bounded rows run with `CONSTELLATION_FUSE_DIR_ATTR_TTL=zero`: they test what the
/// workaround does, not the default. A strict mount takes `auto`.
fn race_dir_nlink(name: &str, wire: Wire, args: &[&str]) -> Result<()> {
    let strict = args.contains(&"strict");
    let extra: &[(&str, &str)] = if strict {
        &[]
    } else {
        &[("CONSTELLATION_FUSE_DIR_ATTR_TTL", "zero")]
    };
    let (_env, _root, mut c) = one_node(name, wire, extra, args)?;
    let r = (|| -> Result<()> {
        let base = c.mnt.join("nl");
        std::fs::create_dir(&base)?;
        let control_dir = c.mnt.join("nl-sync");
        std::fs::create_dir(&control_dir)?;
        let control = nlink_race(&control_dir, (secs() / 6).max(5), 8, true, 7)?;
        let counts = nlink_race(&base, secs(), 8, false, 8)?;
        let m = mount_status(&c, None)?;
        result(
            name,
            &format!(
                "{} control_{} {}",
                counts.line(),
                control.line(),
                counters_line(&m)
            ),
        );
        ensure!(control.mismatches == 0, "the control saw {control:?}");
        ensure!(counts.stats > 1000, "only {} stats", counts.stats);
        ensure!(
            counts.mismatches == 0,
            "{} stats of {} saw a directory's st_nlink from before a completed change",
            counts.mismatches,
            counts.stats
        );
        Ok(())
    })();
    let _ = c.unmount();
    r
}

twice!(dir_nlink, dir_nlink_dev_fuse, |n: &str, w| race_dir_nlink(
    &name(n),
    w,
    &[]
));
twice!(
    dir_ttl_strict_lone,
    dir_ttl_strict_lone_dev_fuse,
    |n: &str, w| race_dir_nlink(&name(n), w, &["--cto", "strict"])
);

const HOLD: Duration = Duration::from_millis(200);

fn race_dir_version(name: &str, wire: Wire) -> Result<()> {
    let mut stale = Vec::new();
    // The parent notice alone closes 2b: once with the directory
    // attribute TTL as configured, once with the TTL part off.
    for ttl in ["auto", "entry"] {
        let (_env, _root, mut c) = one_node(
            name,
            wire,
            &[
                ("CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS", "200"),
                ("CONSTELLATION_FUSE_DIR_ATTR_TTL", ttl),
            ],
            &[],
        )?;
        let r = (|| -> Result<_> {
            let dir = c.mnt.join("dv");
            std::fs::create_dir(&dir)?;
            dir_version(&dir, 20, HOLD, None)
        })();
        let m = mount_status(&c, None);
        let _ = c.unmount();
        let counts = r?;
        result(
            name,
            &format!(
                "dir_attr_ttl={ttl} {} {}",
                counts.line(),
                m.map(|m| counters_line(&m)).unwrap_or_default()
            ),
        );
        stale.push((ttl, counts.stale));
    }
    ensure!(
        stale.iter().all(|(_, n)| *n == 0),
        "stale directory attributes after a change: {stale:?}"
    );
    Ok(())
}

twice!(dir_version_hold, dir_version_hold_dev_fuse, |n: &str, w| {
    race_dir_version(&name(n).replace("-hold", ""), w)
});

fn race_rename_ctime(name: &str, wire: Wire) -> Result<()> {
    let (env, root) = setup(name)?;
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let mut c = client(&env, root.path(), "c0", &backend, wire)?;
    c.fs_create()?;
    c.mount()?;
    // The fault holds one inode's GETATTR, named at mount: learn it first.
    let dir = c.mnt.join("rc");
    std::fs::create_dir(&dir)?;
    std::fs::write(dir.join("f"), b"x")?;
    let ino = std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(dir.join("f"))?);
    c.unmount()?;
    c.set_env("CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS", "200");
    c.set_env("CONSTELLATION_FAULT_GETATTR_DELAY_INO", &ino.to_string());
    c.mount()?;
    let r = rename_ctime(&dir, 20, HOLD);
    let m = mount_status(&c, None);
    let _ = c.unmount();
    let counts = r?;
    result(
        name,
        &format!(
            "ino={ino} {} {}",
            counts.line(),
            m.map(|m| counters_line(&m)).unwrap_or_default()
        ),
    );
    ensure!(
        counts.stale == 0,
        "{} of {} renames left the file's old ctime cached",
        counts.stale,
        counts.rounds
    );
    Ok(())
}

twice!(
    rename_ctime_hold,
    rename_ctime_hold_dev_fuse,
    |n: &str, w| { race_rename_ctime(&name(n).replace("-hold", ""), w) }
);

fn race_dir_version_remote(name: &str, wire: Wire) -> Result<()> {
    let (_env, _root, mut clients) = two_nodes(
        name,
        wire,
        &[("CONSTELLATION_FAULT_DIR_GETATTR_DELAY_MS", "200")],
    )?;
    let r = (|| -> Result<_> {
        let (a, b) = (&clients[0], &clients[1]);
        let dir = a.mnt.join("dvr");
        std::fs::create_dir(&dir)?;
        let other = b.mnt.join("dvr");
        eventually(
            "the directory visible on B",
            Duration::from_secs(30),
            || {
                ensure!(other.exists(), "not yet");
                Ok(())
            },
        )?;
        dir_version(&dir, 20, HOLD, Some(&other))
    })();
    unmount_all(&mut clients);
    let counts = r?;
    result(name, &counts.line());
    ensure!(counts.stale == 0, "{} stale rounds", counts.stale);
    Ok(())
}

twice!(
    dir_version_remote,
    dir_version_remote_dev_fuse,
    |n: &str, w| race_dir_version_remote(&name(n), w)
);
