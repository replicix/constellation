//! Reproductions of the EC2 real-S3 findings (the M14 brutal test).
//!
//! - `s3-cut-one-node`: three nodes; one non-holder loses S3 the way a
//!   firewall `DROP` takes it (a black hole: nothing refused, nothing
//!   answered) while its P2P path to the holder stays up. Its FUSE
//!   operations must only wait for what genuinely needs its own S3: a
//!   create + write + close (with and without `fsync`) under
//!   `--write-mode back` and `through` completes while the cut lasts
//!   (the holder takes the chunks over P2P and uploads them itself), the
//!   file is visible on the other nodes, and an unrelated `ls -la`, an
//!   `ls -la` of the closing file's own directory and a root `stat` on
//!   the cut node answer at once even while a close is in flight. The cut node runs with the product's S3 retry budget, not
//!   the harness's short one.
//! - `p2p-partition-one-node`: four writing nodes, one loses P2P to the
//!   other three (S3 everywhere). Its inbox demand must not move the
//!   lease off the majority (no change of hands at all), the majority's
//!   writes keep forwarding, the isolated node's go through the inbox;
//!   then the holder itself is isolated and the others' writes stall
//!   only for the detection. Everything converges.
//! - `idle-cost`: four idle nodes on the product's default intervals;
//!   S3 requests per node per minute on the wire and in `status.s3`.
//!
//! (`cto-strict-root`, finding R2-4, lives with the other `--cto`
//! scenarios in `m8.rs`.)

use super::{eventually, lease_of, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::Result;
use std::io::Write;
use std::time::{Duration, Instant};

fn node(
    root: &std::path::Path,
    name: &str,
    proxy: &CountingProxy,
    backend: &str,
) -> Result<Client> {
    Ok(Client::new(root, name, &proxy.endpoint(), backend)?
        .with_own_node_key()
        // The product's S3 retry budget (10 retries within 30 s),
        // not the harness's short one (the EC2 nodes ran with 180 s).
        .without_env("CONSTELLATION_S3_MAX_RETRIES")
        .without_env("CONSTELLATION_S3_RETRY_TIMEOUT_MS"))
}

/// Run `f` on a thread; `Some(result, elapsed)` if it finished within
/// `within`, `None` (still running; joined later by the caller through
/// the returned handle) otherwise.
fn timed<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> (std::thread::JoinHandle<(T, Duration)>, Instant) {
    let started = Instant::now();
    (
        std::thread::spawn(move || {
            let t = Instant::now();
            let r = f();
            (r, t.elapsed())
        }),
        started,
    )
}

fn wait_done<T>(h: &std::thread::JoinHandle<T>, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if h.is_finished() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    h.is_finished()
}

fn write_file(path: std::path::PathBuf, content: Vec<u8>, fsync: bool) -> std::io::Result<()> {
    let mut f = std::fs::File::create(&path)?;
    f.write_all(&content)?;
    if fsync {
        f.sync_all()?;
    }
    drop(f);
    Ok(())
}

/// `ls -la dir`: readdir plus a stat of every entry.
fn ls_la(dir: &std::path::Path) -> std::io::Result<usize> {
    let mut n = 0;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        std::fs::symlink_metadata(e.path())?;
        n += 1;
    }
    Ok(n)
}

pub fn s3_cut_one_node(_seed: u64) -> Result<()> {
    const NAME: &str = "s3-cut-one-node";
    /// How long each FUSE op on the cut node may take while the cut lasts.
    /// (The fix: ~6 s to find the node's S3 path stalled, then one
    /// peer upload; the bug: the whole outage.)
    const OP_BUDGET: Duration = Duration::from_secs(20);
    /// An unrelated metadata read on the cut node.
    const READ_BUDGET: Duration = Duration::from_secs(5);
    let (env, root) = setup(NAME)?;
    env.s3_proxy()?;
    let (pa, pb, pc) = (
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    );
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut a = node(root.path(), "a", &pa, &backend)?;
    let mut b = node(root.path(), "b", &pb, &backend)?;
    let mut c = node(root.path(), "c", &pc, &backend)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    let mut failures: Vec<String> = Vec::new();
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&a, &b, &c])?;
        // 300 files in one directory: their inode numbers cover every
        // write shard (ino mod 256), so an `ls -la` of it stats a file
        // in the shard of whatever the cut node is closing.
        std::fs::create_dir(a.mnt.join("d"))?;
        for i in 0..300 {
            std::fs::write(a.mnt.join(format!("d/f{i:03}")), format!("f{i}"))?;
        }
        eventually("A holds the lease", Duration::from_secs(20), || {
            anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
            Ok(())
        })?;
        eventually("d/ complete on C", Duration::from_secs(30), || {
            anyhow::ensure!(ls_la(&c.mnt.join("d"))? == 300, "not yet");
            Ok(())
        })?;
        anyhow::ensure!(std::fs::read(c.mnt.join("d/f000"))? == b"f0");

        for mode in ["back", "through"] {
            c.set_write_mode(mode)?;
            eprintln!("    {NAME}: [{mode}] black-holing C's S3");
            pc.blackhole();
            let cut = Instant::now();
            let mut round_fail = |what: String| {
                eprintln!("    {NAME}: [{mode}] FAIL: {what}");
                failures.push(format!("[{mode}] {what}"));
            };
            // Warm reads: already answered locally.
            let t = Instant::now();
            let n = ls_la(&c.mnt.join("d"))?;
            eprintln!(
                "    {NAME}: [{mode}] ls -la d/ ({n} entries) took {:?}",
                t.elapsed()
            );
            for (fsync, file) in [(false, "new"), (true, "synced")] {
                let name = format!("{file}-{mode}");
                let content = format!("{name}:{}", "x".repeat(5000)).into_bytes();
                let path = c.mnt.join(&name);
                let (h, _) = {
                    let (path, content) = (path.clone(), content.clone());
                    timed(move || write_file(path, content, fsync))
                };
                // While the close is in flight: unrelated reads on C.
                std::thread::sleep(Duration::from_millis(300));
                let (ls, _) = {
                    let d = c.mnt.join("d");
                    timed(move || ls_la(&d))
                };
                let (st, _) = {
                    let r = c.mnt.clone();
                    timed(move || std::fs::metadata(&r).map(|_| ()))
                };
                // The OVH run's variant: `ls -la` of the directory the
                // closing file is in (it stats that very file).
                let (same, _) = {
                    let r = c.mnt.clone();
                    timed(move || ls_la(&r))
                };
                let ls_ok = wait_done(&ls, READ_BUDGET);
                let st_ok = wait_done(&st, READ_BUDGET);
                let same_ok = wait_done(&same, READ_BUDGET);
                let done = wait_done(&h, OP_BUDGET);
                let label = if fsync {
                    "create+write+fsync+close"
                } else {
                    "create+write+close"
                };
                if done {
                    let (r, took) = h.join().expect("writer");
                    eprintln!("    {NAME}: [{mode}] {label} {name}: {r:?} in {took:?}");
                    if let Err(e) = r {
                        round_fail(format!("{label} {name} failed while C's S3 was cut: {e}"));
                    }
                } else {
                    round_fail(format!(
                        "{label} {name} still blocked after {OP_BUDGET:?} of C's S3 cut"
                    ));
                    // Joined after the heal below.
                    std::mem::forget(h);
                }
                if !ls_ok {
                    round_fail(format!(
                        "ls -la d/ on C blocked > {READ_BUDGET:?} while a close was in flight"
                    ));
                } else {
                    let (r, took) = ls.join().expect("ls");
                    eprintln!("    {NAME}: [{mode}] concurrent ls -la d/: {r:?} in {took:?}");
                }
                if !same_ok {
                    round_fail(format!(
                        "ls -la of the closing file's own directory on C blocked > {READ_BUDGET:?}"
                    ));
                } else {
                    let (r, took) = same.join().expect("ls same dir");
                    eprintln!(
                        "    {NAME}: [{mode}] concurrent ls -la / (same dir): {r:?} in {took:?}"
                    );
                }
                if !st_ok {
                    round_fail(format!(
                        "stat of the root on C blocked > {READ_BUDGET:?} while a close was in flight"
                    ));
                } else {
                    let (r, took) = st.join().expect("stat");
                    eprintln!("    {NAME}: [{mode}] concurrent stat /: {r:?} in {took:?}");
                }
                if done {
                    for x in [&a, &b] {
                        let p = x.mnt.join(&name);
                        let seen = eventually(
                            &format!("{name} visible on {} during the cut", x.name),
                            Duration::from_secs(10),
                            || {
                                anyhow::ensure!(
                                    std::fs::read(&p).ok().as_deref() == Some(&content[..]),
                                    "not yet"
                                );
                                Ok(())
                            },
                        );
                        if let Err(e) = seen {
                            round_fail(format!("{e:#}"));
                        }
                    }
                }
            }
            let wb = c.control_status()?["writeback"].clone();
            eprintln!(
                "    {NAME}: [{mode}] C during the cut ({:?}): writeback {wb}",
                cut.elapsed()
            );
            pc.heal();
            eprintln!("    {NAME}: [{mode}] healed after {:?}", cut.elapsed());
            eventually(
                "C's pending uploads drain after the heal",
                Duration::from_secs(120),
                || {
                    let p = c.control_status()?["writeback"]["pending_uploads"].as_u64();
                    anyhow::ensure!(p == Some(0), "pending {p:?}");
                    Ok(())
                },
            )?;
            for x in [&a, &b, &c] {
                for file in ["new", "synced"] {
                    let name = format!("{file}-{mode}");
                    let want = format!("{name}:{}", "x".repeat(5000)).into_bytes();
                    eventually(
                        &format!("{name} converged on {}", x.name),
                        Duration::from_secs(120),
                        || {
                            anyhow::ensure!(
                                std::fs::read(x.mnt.join(&name)).ok().as_deref() == Some(&want[..]),
                                "not yet"
                            );
                            Ok(())
                        },
                    )?;
                }
            }
        }

        // EC2 follow-up 3c: a *refused* S3 path (fails fast, so C's
        // rounds fail and it would propose a continuation epoch) for 25 s
        // under `back`: C proposes no epoch (a live member reaches S3),
        // and every close completes and is visible elsewhere meanwhile.
        c.set_write_mode("back")?;
        let proposals_before = c.control_status()?["epoch"]["proposals"]
            .as_u64()
            .unwrap_or(0);
        eprintln!("    {NAME}: [refused] cutting C's S3 (refused, not black-holed) for 25 s");
        pc.cut();
        let cut = Instant::now();
        let mut i = 0;
        let mut own_outage = false;
        while cut.elapsed() < Duration::from_secs(25) {
            let name = format!("refused-{i}");
            let content = format!("{name}:{}", "y".repeat(3000)).into_bytes();
            let (h, _) = {
                let (path, content) = (c.mnt.join(&name), content.clone());
                timed(move || write_file(path, content, false))
            };
            if !wait_done(&h, OP_BUDGET) {
                failures.push(format!(
                    "[refused] close of {name} still blocked after {OP_BUDGET:?}"
                ));
                std::mem::forget(h);
                break;
            }
            let (r, took) = h.join().expect("writer");
            eprintln!("    {NAME}: [refused] close {name}: {r:?} in {took:?}");
            if let Err(e) = r {
                failures.push(format!("[refused] close of {name} failed: {e}"));
            }
            let e = c.control_status()?["epoch"].clone();
            own_outage |= e["own_s3_outage"] == true;
            if e["active"] == true {
                failures.push(format!("[refused] C is in a continuation epoch: {e}"));
                break;
            }
            i += 1;
            std::thread::sleep(Duration::from_secs(3));
        }
        let e = c.control_status()?["epoch"].clone();
        let proposals = e["proposals"].as_u64().unwrap_or(0) - proposals_before;
        eprintln!("    {NAME}: [refused] C's epoch during the cut: {e} (proposals {proposals}, own outage seen {own_outage})");
        if proposals > 0 {
            failures.push(format!(
                "[refused] C proposed {proposals} continuation epoch(s) while its peers reached S3"
            ));
        }
        pc.heal();
        for x in [&a, &b] {
            for j in 0..i {
                let name = format!("refused-{j}");
                let want = format!("{name}:{}", "y".repeat(3000)).into_bytes();
                eventually(
                    &format!("{name} on {}", x.name),
                    Duration::from_secs(120),
                    || {
                        anyhow::ensure!(
                            std::fs::read(x.mnt.join(&name)).ok().as_deref() == Some(&want[..]),
                            "not yet"
                        );
                        Ok(())
                    },
                )?;
            }
        }
        Ok(())
    })();
    pc.heal();
    let _ = c.unmount();
    let _ = b.unmount();
    let _ = a.unmount();
    result?;
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

fn node_id(c: &Client) -> Result<u64> {
    c.control_status()?["node_id"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("{} reports no node id", c.name))
}

/// The P2P deny file of a client (`fault::p2p_denied`).
fn deny_path(c: &Client) -> std::path::PathBuf {
    c.state_dir().join("p2p-deny")
}

/// A writer thread: `name-<n>` files under `dir` every `every`, until
/// `stop`; returns every (started-at, latency, result) it saw.
type WriteLog = Vec<(Instant, Duration, std::result::Result<String, String>)>;

fn writer(
    dir: std::path::PathBuf,
    name: &'static str,
    every: Duration,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<WriteLog> {
    std::thread::spawn(move || {
        let mut log = Vec::new();
        let mut n = 0u64;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            let file = format!("{name}-{n:05}");
            let t = Instant::now();
            let r = std::fs::write(dir.join(&file), file.as_bytes())
                .map(|_| file.clone())
                .map_err(|e| format!("{file}: {e}"));
            log.push((t, t.elapsed(), r));
            n += 1;
            std::thread::sleep(every);
        }
        log
    })
}

/// EC2 finding 2: one node loses P2P to the other three (S3 reachable
/// everywhere) while all four write. The isolated node's writes go
/// through the S3 inbox (plan 30 §M13) and, sustained, escalate to
/// asking for the lease — which must not move the lease off the side of
/// the partition that is using it over P2P: the majority's writes keep
/// forwarding at LAN latency the whole time, the isolated node's still
/// complete (through the inbox), and everything converges after the
/// heal.
pub fn p2p_partition_one_node(_seed: u64) -> Result<()> {
    const NAME: &str = "p2p-partition-one-node";
    const PARTITION: Duration = Duration::from_secs(40);
    /// A majority write may take this long during the partition (the
    /// detection of the dead link on the first forward included).
    const MAJORITY_BUDGET: Duration = Duration::from_secs(12);
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut clients = Vec::new();
    for name in ["a", "b", "c", "d"] {
        let c = Client::new(root.path(), name, &env.direct_endpoint, &backend)?
            .with_own_node_key()
            // The EC2 run's shape: one sequencer, no subtree delegated (a
            // live delegation pins the root lease on its own).
            .with_env("CONSTELLATION_DELEGATION_PLACEMENT", "off");
        let deny = deny_path(&c).display().to_string();
        clients.push(c.with_env("CONSTELLATION_FAULT_P2P_DENY_FILE", &deny));
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount()?;
    }
    let result = (|| -> Result<()> {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        let (a, b, c, d) = (&clients[0], &clients[1], &clients[2], &clients[3]);
        // Everyone writes into one shared directory.
        std::fs::create_dir(a.mnt.join("w"))?;
        eventually("A holds the lease", Duration::from_secs(20), || {
            anyhow::ensure!(lease_of(a)?["held"] == true, "{}", lease_of(a)?);
            Ok(())
        })?;
        for x in [b, c, d] {
            eventually("dirs everywhere", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("w").is_dir(), "not on {}", x.name);
                Ok(())
            })?;
        }
        let ids: Vec<u64> = [a, b, c, d]
            .iter()
            .map(|x| node_id(x))
            .collect::<Result<_>>()?;
        let d_id = ids[3];
        eprintln!("    {NAME}: node ids a/b/c/d = {ids:?}");
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let every = Duration::from_millis(100);
        let wb = writer(b.mnt.join("w"), "b", every, stop.clone());
        let wc = writer(c.mnt.join("w"), "c", every, stop.clone());
        let wd = writer(d.mnt.join("w"), "d", every, stop.clone());
        std::thread::sleep(Duration::from_secs(5));
        // Cut d off from a, b and c, both ways.
        std::fs::write(
            deny_path(d),
            format!("{}\n{}\n{}\n", ids[0], ids[1], ids[2]),
        )?;
        for x in [a, b, c] {
            std::fs::write(deny_path(x), format!("{d_id}\n"))?;
        }
        let cut = Instant::now();
        eprintln!("    {NAME}: d partitioned from a, b, c (P2P only; S3 everywhere)");
        let mut holders = Vec::new();
        while cut.elapsed() < PARTITION {
            let l = lease_of(a)?;
            let holder = l["holder"].as_u64().unwrap_or(0);
            if holders.last().map(|(_, h)| *h) != Some(holder) {
                eprintln!("    {NAME}: +{:?} holder {holder} ({l})", cut.elapsed());
                holders.push((cut.elapsed(), holder));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        for x in [a, b, c, d] {
            let _ = std::fs::remove_file(deny_path(x));
        }
        let healed = Instant::now();
        eprintln!("    {NAME}: healed after {:?}", cut.elapsed());
        std::thread::sleep(Duration::from_secs(3));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let logs = [
            ("b", wb.join().expect("b")),
            ("c", wc.join().expect("c")),
            ("d", wd.join().expect("d")),
        ];
        let mut failures = Vec::new();
        let cut_at = healed - cut.elapsed();
        for (who, log) in &logs {
            let during: Vec<Duration> = log
                .iter()
                .filter(|(t, lat, _)| *t + *lat >= cut_at && *t <= healed)
                .map(|(_, lat, _)| *lat)
                .collect();
            let errors: Vec<&String> = log
                .iter()
                .filter_map(|(_, _, r)| r.as_ref().err())
                .collect();
            eprintln!(
                "    {NAME}: {who}: {} writes, {} during the partition: {}; errors {}",
                log.len(),
                during.len(),
                super::m8::dist(during.clone()),
                errors.len()
            );
            for e in errors.iter().take(3) {
                eprintln!("    {NAME}: {who}:   {e}");
            }
            if !errors.is_empty() {
                failures.push(format!("{who}: {} writes failed", errors.len()));
            }
            let max = during.iter().max().copied().unwrap_or_default();
            if *who != "d" && max > MAJORITY_BUDGET {
                failures.push(format!(
                    "{who} (majority side) had a write take {max:?} during the partition"
                ));
            }
        }
        if holders.iter().any(|(_, h)| *h == d_id) {
            failures.push(format!(
                "the lease moved to the isolated node {d_id} during the partition: {holders:?}"
            ));
        }
        // Nor may the isolated node's demand bounce it around the
        // majority (each move a release, a CAS and a new epoch's gate).
        if holders.len() > 1 {
            failures.push(format!(
                "the lease changed hands {} time(s) during the partition: {holders:?}",
                holders.len() - 1
            ));
        }
        let kept: u64 = [a, b, c]
            .iter()
            .map(|x| {
                x.control_status()
                    .ok()
                    .and_then(|s| s["inbox"]["leases_kept_for_p2p_side"].as_u64())
                    .unwrap_or(0)
            })
            .sum();
        eprintln!("    {NAME}: rounds the majority's holder kept the lease from the isolated wanter: {kept}");
        // Everything acknowledged is everywhere.
        for (who, log) in &logs {
            let files: Vec<&String> = log.iter().filter_map(|(_, _, r)| r.as_ref().ok()).collect();
            for x in [a, b, c, d] {
                eventually(
                    &format!("{who}'s {} files on {}", files.len(), x.name),
                    Duration::from_secs(60),
                    || {
                        for f in &files {
                            let got = std::fs::read(x.mnt.join("w").join(f.as_str()))
                                .map_err(|e| anyhow::anyhow!("{f}: {e}"))?;
                            anyhow::ensure!(got == f.as_bytes(), "{f} differs");
                        }
                        Ok(())
                    },
                )?;
            }
        }
        anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));

        // Phase 2: the holder itself is the isolated node (S3 still
        // reachable). The other side's writes must stall only for the
        // detection, and the lease must end up on their side.
        // Whoever holds it now (a node's `lease.holder` is its cached
        // view; `held` is the holder's own word).
        // (After the heal the lease may still be moving: wait until one
        // node has held it for a few seconds running.)
        let mut hx: Option<&Client> = None;
        let mut since = Instant::now();
        eventually(
            "one node holds the lease steadily",
            Duration::from_secs(60),
            || {
                // A write keeps someone holding (an idle lease is
                // released once the writers stopped).
                let _ = std::fs::write(b.mnt.join("w/poke"), b"poke");
                let now = [a, b, c, d]
                    .into_iter()
                    .find(|x| lease_of(x).is_ok_and(|l| l["held"] == true));
                if now.map(|x| &x.name) != hx.map(|x| &x.name) {
                    hx = now;
                    since = Instant::now();
                }
                anyhow::ensure!(
                    hx.is_some() && since.elapsed() >= Duration::from_secs(3),
                    "not yet"
                );
                Ok(())
            },
        )?;
        let hx = hx.expect("found");
        let holder = node_id(hx)?;
        let rest: Vec<&Client> = [a, b, c, d]
            .into_iter()
            .filter(|x| x.name != hx.name)
            .collect();
        let rest_ids: Vec<u64> = rest.iter().map(|x| node_id(x)).collect::<Result<_>>()?;
        eprintln!(
            "    {NAME}: phase 2: isolating the holder {} ({holder})",
            hx.name
        );
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let names: [&'static str; 3] = ["p", "q", "r"];
        let ws: Vec<_> = rest
            .iter()
            .zip(names)
            .map(|(x, n)| (n, writer(x.mnt.join("w"), n, every, stop.clone())))
            .collect();
        std::thread::sleep(Duration::from_secs(3));
        std::fs::write(
            deny_path(hx),
            rest_ids
                .iter()
                .map(|i| format!("{i}\n"))
                .collect::<String>(),
        )?;
        for x in &rest {
            std::fs::write(deny_path(x), format!("{holder}\n"))?;
        }
        let cut = Instant::now();
        let mut moved = None;
        while cut.elapsed() < Duration::from_secs(30) {
            if let Some(x) = rest
                .iter()
                .find(|x| lease_of(x).is_ok_and(|l| l["held"] == true))
            {
                moved = Some((cut.elapsed(), x.name.clone()));
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        std::thread::sleep(Duration::from_secs(5));
        let cut_at = Instant::now() - cut.elapsed();
        for x in [a, b, c, d] {
            let _ = std::fs::remove_file(deny_path(x));
        }
        std::thread::sleep(Duration::from_secs(2));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let mut failures = Vec::new();
        eprintln!("    {NAME}: phase 2: the lease moved to {moved:?} after the holder's isolation");
        // The isolated holder still has S3: it reconfigures its silent
        // backup out and keeps serving the others through the inbox
        // (plan 30 §M13) until their sustained demand moves the lease
        // (no P2P demand keeps it there). Either way the other side's
        // writes stall only for the detection.
        if moved.is_none() {
            failures.push("phase 2: the lease never left the isolated holder".to_string());
        }
        for (who, h) in ws {
            let log = h.join().expect("writer");
            let during: Vec<Duration> = log
                .iter()
                .filter(|(t, lat, _)| *t + *lat >= cut_at)
                .map(|(_, lat, _)| *lat)
                .collect();
            let errors = log.iter().filter(|(_, _, r)| r.is_err()).count();
            eprintln!(
                "    {NAME}: phase 2: {who}: {}; errors {errors}",
                super::m8::dist(during.clone())
            );
            let max = during.iter().max().copied().unwrap_or_default();
            if errors > 0 || max > MAJORITY_BUDGET {
                failures.push(format!(
                    "phase 2: {who}: {errors} errors, slowest write {max:?}"
                ));
            }
        }
        anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = std::fs::remove_file(deny_path(c));
        let _ = c.unmount();
    }
    result
}

/// EC2 finding R2-2: what an idle cluster costs in S3 requests, with the
/// product's defaults (the harness's 200 ms sync interval off). Four
/// nodes converge, then sit idle; every node's requests are counted on
/// its own relay, by kind and key area, and against the daemon's own
/// `status.s3` counters (which must agree with the wire). Prints
/// requests per node per idle minute; fails over `IDLE_BUDGET_PER_MIN`.
pub fn idle_cost(_seed: u64) -> Result<()> {
    idle_cost_run("idle-cost", false)
}

/// `idle-cost` with the lease holder's P2P links flapping: every
/// `FLAP_EVERY` its peer directory flags every peer down for `FLAP_DOWN`
/// (the fault deny file), as one late registry-tick ping round did under
/// host load in the gate run on 7dfc05b — where the holder then polled
/// each requester's inbox in its hot window from scratch (96 GETs in the
/// two minutes, the budget blown). A requester with no recent demand is
/// polled cold, and a link that comes back keeps its schedule, so the
/// budget holds however often the links flap.
pub fn idle_cost_link_flap(_seed: u64) -> Result<()> {
    idle_cost_run("idle-cost-link-flap", true)
}

fn idle_cost_run(scenario: &'static str, flap: bool) -> Result<()> {
    const IDLE: Duration = Duration::from_secs(120);
    /// Requests per node per idle minute.
    const IDLE_BUDGET_PER_MIN: f64 = 60.0;
    const FLAP_EVERY: Duration = Duration::from_secs(15);
    const FLAP_DOWN: Duration = Duration::from_secs(5);
    /// The holder's inbox GETs while flapping: three idle requesters,
    /// each polled once when first seen down and then at most once per
    /// cold ceiling (10 s).
    const FLAP_INBOX_GETS: usize = 3 * (1 + IDLE.as_secs() as usize / 10);
    let (env, root) = setup(scenario)?;
    env.s3_proxy()?;
    let proxies: Vec<CountingProxy> = (0..4)
        .map(|_| env.counting_proxy())
        .collect::<Result<_>>()?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let mut clients = Vec::new();
    for (name, p) in ["a", "b", "c", "d"].into_iter().zip(&proxies) {
        let c = node(root.path(), name, p, &backend)?.without_env("CONSTELLATION_SYNC_INTERVAL_MS");
        let deny = deny_path(&c).display().to_string();
        clients.push(if flap {
            c.with_env("CONSTELLATION_FAULT_P2P_DENY_FILE", &deny)
        } else {
            c
        });
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount()?;
    }
    let result = (|| -> Result<()> {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        std::fs::write(clients[0].mnt.join("marker"), b"idle")?;
        for x in &clients[1..] {
            eventually("marker everywhere", Duration::from_secs(60), || {
                anyhow::ensure!(std::fs::read(x.mnt.join("marker"))? == b"idle", "not yet");
                Ok(())
            })?;
        }
        // Past the write's own after-effects (the ship, the commit, the
        // lease's idle release, the backups' reconfigurations).
        std::thread::sleep(Duration::from_secs(45));
        let before: Vec<serde_json::Value> = clients
            .iter()
            .map(|c| c.control_status().map(|s| s["s3"].clone()))
            .collect::<Result<_>>()?;
        // The holder, and everyone else's node id (what its deny file
        // lists while its links are down).
        let holder = clients
            .iter()
            .position(|c| c.control_status().is_ok_and(|s| s["lease"]["held"] == true))
            .unwrap_or(0);
        let others: Vec<String> = clients
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != holder)
            .map(|(_, c)| node_id(c).map(|id| id.to_string()))
            .collect::<Result<_>>()?;
        for p in &proxies {
            p.reset();
        }
        if flap {
            let deny = deny_path(&clients[holder]);
            let started = Instant::now();
            let mut flaps = 0;
            let mut down = false;
            while started.elapsed() < IDLE {
                let phase = started.elapsed().as_millis() % FLAP_EVERY.as_millis();
                let want = phase >= (FLAP_EVERY - FLAP_DOWN).as_millis();
                if want != down {
                    if want {
                        std::fs::write(&deny, others.join("\n") + "\n")?;
                        flaps += 1;
                    } else {
                        std::fs::remove_file(&deny)?;
                    }
                    down = want;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = std::fs::remove_file(&deny);
            eprintln!(
                "    {scenario}: {}'s links flagged down {flaps} times for {FLAP_DOWN:?} in \
                     {IDLE:?}",
                clients[holder].name
            );
        } else {
            std::thread::sleep(IDLE);
        }
        let minutes = IDLE.as_secs_f64() / 60.0;
        let mut worst = 0.0f64;
        for ((c, p), b) in clients.iter().zip(&proxies).zip(&before) {
            p.ensure_sane()?;
            let reqs = p.requests();
            let t = crate::reqlog::tally(&reqs);
            let s = c.control_status()?;
            let after = &s["s3"];
            let d = |k: &str| after[k].as_u64().unwrap_or(0) - b[k].as_u64().unwrap_or(0);
            let per_min = t.total() as f64 / minutes;
            worst = worst.max(per_min);
            eprintln!(
                "    {scenario}: {} ({}): {:.1} requests/min idle: {t}\n        by area: {}\n        \
                 status.s3 delta: GET {} HEAD {} PUT {} LIST {} DELETE {}; ship rounds {} \
                 reconcile rounds {}",
                c.name,
                if s["lease"]["held"] == true { "holder" } else { "follower" },
                per_min,
                crate::reqlog::breakdown(&reqs),
                d("get"),
                d("head"),
                d("put"),
                d("list"),
                d("delete"),
                s["spool"]["ship_rounds_completed"],
                s["coop"]["reconcile_rounds"],
            );
        }
        anyhow::ensure!(
            worst <= IDLE_BUDGET_PER_MIN,
            "an idle node issued {worst:.1} S3 requests per minute (budget {IDLE_BUDGET_PER_MIN})"
        );
        if flap {
            let inbox_gets = proxies[holder]
                .requests()
                .iter()
                .filter(|r| !r.is_list() && r.area() == "inbox")
                .count();
            anyhow::ensure!(
                inbox_gets <= FLAP_INBOX_GETS,
                "the holder polled idle requesters' inboxes {inbox_gets} times while its \
                     links flapped (at most {FLAP_INBOX_GETS}: one each, then the cold ceiling)"
            );
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = std::fs::remove_file(deny_path(c));
        let _ = c.unmount();
    }
    result
}

/// The withdraw hole. A requester whose op went to the holder's S3 inbox
/// withdraws the batch before forwarding the op over a P2P path that
/// came back. It used to DELETE the batch, leaving a hole at its number:
/// a holder that had not read it yet GET-nexted the hole for the rest of
/// the epoch, and every later inbox write of that requester waited for
/// the in-doubt deadline (2 × TTL) and then the lease path. Now the
/// batch is overwritten with a tombstone the holder steps past.
///
/// Three nodes, P2P on; `b` is the requester, `a` the holder (`c` only
/// keeps it from being a pair). The holder's inbox polls are paused
/// (`CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE`) and `b` is cut from `a`
/// over P2P: `b`'s write goes into its inbox and nobody reads it. The
/// link comes back: `b` withdraws the batch and forwards the write. Cut
/// again, polls resumed: `b`'s next writes go through the inbox and must
/// each complete within `WRITE_BUDGET`, far under the deadline; the
/// holder must have read the tombstone. Then everything converges.
pub fn inbox_withdraw_hole(_seed: u64) -> Result<()> {
    const NAME: &str = "inbox-withdraw-hole";
    const TTL_MS: u64 = 30_000;
    /// One inbox write: the failed forward and the P2P grace (3 s), the
    /// holder's poll at its cold ceiling (10 s), a ship. The in-doubt
    /// deadline, which the hole made every write wait for, is 60 s.
    const WRITE_BUDGET: Duration = Duration::from_secs(20);
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut clients = Vec::new();
    for name in ["a", "b", "c"] {
        let c = Client::new(root.path(), name, &env.direct_endpoint, &backend)?
            .with_own_node_key()
            .with_env("CONSTELLATION_LEASE_TTL_MS", &TTL_MS.to_string())
            .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
            // Nothing but the inbox moves `b`'s writes: no delegation of
            // its directory, no escalation to the lease, and no backup
            // for the cut to seal.
            .with_env("CONSTELLATION_DELEGATION_PLACEMENT", "off")
            .with_env("CONSTELLATION_INBOX_ESCALATE", "off")
            .with_env("CONSTELLATION_BACKUPS", "0");
        let deny = deny_path(&c).display().to_string();
        let pause = c.state_dir().join("inbox-poll-pause").display().to_string();
        clients.push(
            c.with_env("CONSTELLATION_FAULT_P2P_DENY_FILE", &deny)
                .with_env("CONSTELLATION_FAULT_INBOX_POLL_PAUSE_FILE", &pause),
        );
    }
    clients[0].fs_create()?;
    for c in clients.iter_mut() {
        c.mount()?;
    }
    let result = (|| -> Result<()> {
        let refs: Vec<&Client> = clients.iter().collect();
        wait_for_p2p(&refs)?;
        let (a, b, c) = (&clients[0], &clients[1], &clients[2]);
        std::fs::create_dir(a.mnt.join("w"))?;
        eventually("a holds the lease", Duration::from_secs(20), || {
            anyhow::ensure!(lease_of(a)?["held"] == true, "{}", lease_of(a)?);
            Ok(())
        })?;
        for x in [b, c] {
            eventually("w everywhere", Duration::from_secs(20), || {
                anyhow::ensure!(x.mnt.join("w").is_dir(), "not on {}", x.name);
                Ok(())
            })?;
        }
        let (a_id, b_id) = (node_id(a)?, node_id(b)?);
        let inbox = |x: &Client, k: &str| -> Result<u64> {
            Ok(x.control_status()?["inbox"][k].as_u64().unwrap_or(0))
        };
        let pause = a.state_dir().join("inbox-poll-pause");
        let cut = |on: bool| -> Result<()> {
            if on {
                std::fs::write(deny_path(a), format!("{b_id}\n"))?;
                std::fs::write(deny_path(b), format!("{a_id}\n"))?;
            } else {
                let _ = std::fs::remove_file(deny_path(a));
                let _ = std::fs::remove_file(deny_path(b));
            }
            Ok(())
        };
        let write = |name: &'static str| {
            let path = b.mnt.join("w").join(name);
            std::thread::spawn(move || {
                let t = Instant::now();
                let r = std::fs::write(&path, name.as_bytes()).map_err(|e| format!("{name}: {e}"));
                (r, t.elapsed())
            })
        };

        // 1. Polls paused, b cut from a: b's write sits unread in its inbox.
        std::fs::write(&pause, b"paused")?;
        cut(true)?;
        std::thread::sleep(Duration::from_secs(2));
        let submitted = inbox(b, "submitted_batches")?;
        let first = write("f1");
        eventually("b's write is in its inbox", Duration::from_secs(30), || {
            anyhow::ensure!(
                inbox(b, "submitted_batches")? > submitted,
                "{}",
                b.control_status()?["inbox"]
            );
            Ok(())
        })?;
        let next_n = inbox(b, "next_n")?;
        eprintln!(
            "    {NAME}: b's write is batch {} of its inbox, unread",
            next_n - 1
        );

        // 2. The link comes back: b withdraws the batch and forwards.
        cut(false)?;
        let (r, took) = first.join().expect("f1");
        r.map_err(|e| anyhow::anyhow!(e))?;
        let withdrawn = inbox(b, "withdrawn_ops")?;
        eprintln!(
            "    {NAME}: f1 answered after {took:?} over P2P; b withdrew {withdrawn} batch(es)"
        );
        anyhow::ensure!(
            withdrawn >= 1,
            "b's batch was not withdrawn: {}",
            b.control_status()?["inbox"]
        );

        // 3. Cut again, polls resumed: b's next writes use the inbox,
        // behind the withdrawn batch's number.
        let unavailable = inbox(b, "unavailable")?;
        cut(true)?;
        std::fs::remove_file(&pause)?;
        std::thread::sleep(Duration::from_secs(2));
        let mut slow = Vec::new();
        for name in ["f2", "f3", "f4"] {
            let (r, took) = write(name).join().expect(name);
            r.map_err(|e| anyhow::anyhow!(e))?;
            eprintln!("    {NAME}: {name} took {took:?} through the inbox");
            if took > WRITE_BUDGET {
                slow.push(format!("{name} {took:?}"));
            }
        }
        let (tombstones, executed) = (inbox(a, "tombstones_read")?, inbox(a, "executed_ops")?);
        eprintln!(
            "    {NAME}: a read {tombstones} tombstone(s), executed {executed} inbox op(s); \
             b next batch {}, lease path {}",
            inbox(b, "next_n")?,
            inbox(b, "unavailable")? - unavailable
        );
        anyhow::ensure!(
            slow.is_empty(),
            "inbox writes behind a withdrawn batch took over {WRITE_BUDGET:?}: {slow:?}"
        );
        anyhow::ensure!(tombstones >= 1, "the holder never read the withdrawn batch");
        anyhow::ensure!(executed >= 3, "the holder executed {executed} inbox ops");
        anyhow::ensure!(
            inbox(b, "unavailable")? == unavailable,
            "an inbox write took the lease path"
        );

        // 4. Healed: every write everywhere, once.
        cut(false)?;
        for x in [a, b, c] {
            eventually(
                &format!("f1..f4 on {}", x.name),
                Duration::from_secs(60),
                || {
                    for f in ["f1", "f2", "f3", "f4"] {
                        let got = std::fs::read(x.mnt.join("w").join(f))
                            .map_err(|e| anyhow::anyhow!("{f}: {e}"))?;
                        anyhow::ensure!(got == f.as_bytes(), "{f} differs on {}", x.name);
                    }
                    Ok(())
                },
            )?;
        }
        Ok(())
    })();
    for c in clients.iter_mut().rev() {
        let _ = std::fs::remove_file(deny_path(c));
        let _ = c.unmount();
    }
    result
}

/// EC2 campaign 8 A-1. Node B's outbound HTTPS is cut the way the
/// campaign cut it (`iptables -A OUTPUT -p tcp --dport 443 ! -d
/// <cluster subnet> -j DROP`: all of S3 black-holed, the peers on the
/// cluster subnet untouched — P2P needs nothing else, no relay and no
/// discovery server). A `create()` on B then took 120 s and ended in
/// doubt: its forwards to the holder failed for a few seconds (the
/// holder had just been `kill -9`ed and restarted: B's pooled connection
/// led to the dead incarnation, and the new one did not hold the lease
/// its predecessor left behind), so B took the lease path — whose every
/// step is an S3 request B cannot make — and its next forward, 57 s
/// later, heard `NotHolder` from the restarted node and went back to it.
///
/// Three nodes (A holds; no backups, so nothing seals A's lease when it
/// dies — the campaign's lease had just moved to A); every node on the
/// product's S3 retry budget. Each phase times a `create()` on B while
/// B's S3 is black-holed:
///
/// 1. A is `kill -9`ed and remounted (with nothing of its own to ship);
///    B's S3 is cut: B's create must complete within [`CREATE_BUDGET`]
///    (A re-adopts its own lease for B's forward), and the file's
///    write + close within the S3-cut close budget (the peer upload);
/// 2. A freezes (SIGSTOP) for longer than B's forward retries last: B's
///    create must still complete within [`CREATE_BUDGET`] of the thaw
///    plus the freeze — it keeps forwarding instead of waiting on S3;
/// 3. B also loses P2P to everyone: nothing can serve its create, which
///    must fail (`EIO`) within the documented bound
///    (`CONSTELLATION_S3_LESS_OP_DEADLINE_MS`, 20 s, after the stall is
///    detected, 6 s) — not after the 120 s in-doubt deadline.
///
/// After the heal the phase-1 and phase-2 files read back everywhere.
pub fn s3_cut_create_holder_restart(_seed: u64) -> Result<()> {
    const NAME: &str = "s3-cut-create-holder-restart";
    /// A `create()` on the cut node (A-1's bound; the campaign: 120 s).
    const CREATE_BUDGET: Duration = Duration::from_secs(10);
    /// The write + close after it (the chunk handoff to a peer: ~6 s to
    /// find the S3 path stalled, then the peer's upload).
    const CLOSE_BUDGET: Duration = Duration::from_secs(20);
    /// Phase 2's freeze: longer than B's forward retries (~1.5 s).
    const FREEZE: Duration = Duration::from_secs(5);
    /// Phase 3: the S3-less bound (20 s) plus slack.
    const FAIL_BUDGET: Duration = Duration::from_secs(35);
    let (env, root) = setup(NAME)?;
    env.s3_proxy()?;
    let (pa, pb, pc) = (
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    );
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let tune = |c: Client| {
        let deny = deny_path(&c).display().to_string();
        c.with_env("CONSTELLATION_BACKUPS", "0")
            .with_env("CONSTELLATION_LEASE_PLACEMENT", "off")
            .with_env("CONSTELLATION_DELEGATION_PLACEMENT", "off")
            .with_env("CONSTELLATION_FAULT_P2P_DENY_FILE", &deny)
    };
    let mut a = tune(node(root.path(), "a", &pa, &backend)?);
    let mut b = tune(node(root.path(), "b", &pb, &backend)?);
    let mut c = tune(node(root.path(), "c", &pc, &backend)?);
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    let mut failures: Vec<String> = Vec::new();
    let mut paused = false;
    let result = (|| -> Result<()> {
        wait_for_p2p(&[&a, &b, &c])?;
        eventually("A holds the lease", Duration::from_secs(20), || {
            anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
            Ok(())
        })?;
        // B forwards to A (its pooled connection to A is up), and A ships
        // everything: restarted, A has nothing of its own to ship and so
        // does not reacquire the lease by itself.
        std::fs::write(b.mnt.join("warm"), b"warm")?;
        eventually("A's journal shipped", Duration::from_secs(30), || {
            let s = a.control_status()?;
            anyhow::ensure!(s["spool"]["journal_backlog"] == 0, "{}", s["spool"]);
            Ok(())
        })?;
        eventually("warm on C", Duration::from_secs(30), || {
            anyhow::ensure!(std::fs::read(c.mnt.join("warm"))? == b"warm");
            Ok(())
        })?;

        // ---- 1: the holder restarts; B's S3 is cut ----
        a.kill9()?;
        a.mount()?;
        eprintln!("    {NAME}: [restart] A restarted; black-holing B's S3");
        pb.blackhole();
        let create = |path: std::path::PathBuf| {
            timed(move || std::fs::File::create(&path).map(|f| (f, path)))
        };
        let (h, _) = create(b.mnt.join("after-restart"));
        if !wait_done(&h, CREATE_BUDGET) {
            failures.push(format!(
                "[restart] create on B still blocked after {CREATE_BUDGET:?} (B's S3 cut, A restarted)"
            ));
            std::mem::forget(h);
            return Ok(());
        }
        let (r, took) = h.join().expect("create");
        eprintln!(
            "    {NAME}: [restart] create on B: {:?} in {took:?}",
            r.as_ref().map(|_| ())
        );
        let (file, path) = match r {
            Ok(x) => x,
            Err(e) => {
                failures.push(format!("[restart] create on B failed: {e}"));
                return Ok(());
            }
        };
        let (h, _) = timed(move || {
            let mut file = file;
            file.write_all(b"after-restart")?;
            drop(file);
            std::io::Result::Ok(path)
        });
        if !wait_done(&h, CLOSE_BUDGET) {
            failures.push(format!(
                "[restart] write+close on B still blocked after {CLOSE_BUDGET:?}"
            ));
            std::mem::forget(h);
            return Ok(());
        }
        let (r, took) = h.join().expect("close");
        eprintln!(
            "    {NAME}: [restart] write+close on B: {:?} in {took:?}",
            r.as_ref().map(|_| ())
        );
        if let Err(e) = r {
            failures.push(format!("[restart] write+close on B failed: {e}"));
        }
        let s = b.control_status()?;
        eprintln!("    {NAME}: [restart] B own_s3 {}", s["own_s3"]);
        let s = a.control_status()?;
        eprintln!(
            "    {NAME}: [restart] A lease {} own_s3 {}",
            s["lease"], s["own_s3"]
        );

        // ---- 2: the holder freezes past B's forward retries ----
        eventually("B knows its S3 is stalled", Duration::from_secs(20), || {
            let s = b.control_status()?;
            anyhow::ensure!(s["own_s3"]["stalled"] == true, "{}", s["own_s3"]);
            Ok(())
        })?;
        eventually("A holds the lease again", Duration::from_secs(20), || {
            anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
            Ok(())
        })?;
        a.pause()?;
        paused = true;
        let frozen = Instant::now();
        let (h, _) = create(b.mnt.join("after-freeze"));
        std::thread::sleep(FREEZE);
        a.resume()?;
        paused = false;
        if !wait_done(&h, CREATE_BUDGET) {
            failures.push(format!(
                "[freeze] create on B still blocked {CREATE_BUDGET:?} after A's {FREEZE:?} freeze ended"
            ));
            std::mem::forget(h);
            return Ok(());
        }
        let (r, _) = h.join().expect("create");
        eprintln!(
            "    {NAME}: [freeze] create on B: {:?} {:?} after A froze for {FREEZE:?}",
            r.as_ref().map(|_| ()),
            frozen.elapsed()
        );
        match r {
            Ok((mut file, _)) => {
                file.write_all(b"after-freeze")?;
                drop(file);
            }
            Err(e) => failures.push(format!("[freeze] create on B failed: {e}")),
        }

        // ---- 3: B loses P2P too: a bounded failure ----
        let ids: Vec<u64> = [&a, &c].iter().map(|x| node_id(x)).collect::<Result<_>>()?;
        std::fs::write(
            deny_path(&b),
            ids.iter().map(|i| format!("{i}\n")).collect::<String>(),
        )?;
        eprintln!("    {NAME}: [isolated] B denied P2P to {ids:?}");
        let (h, started) = create(b.mnt.join("isolated"));
        if !wait_done(&h, FAIL_BUDGET) {
            failures.push(format!(
                "[isolated] create on B (no S3, no P2P) still blocked after {FAIL_BUDGET:?}; \
                 expected EIO within the S3-less bound"
            ));
            std::mem::forget(h);
        } else {
            let (r, _) = h.join().expect("create");
            eprintln!(
                "    {NAME}: [isolated] create on B: {:?} after {:?}",
                r.as_ref().map(|_| ()),
                started.elapsed()
            );
            if r.is_ok() {
                failures.push("[isolated] a create on B succeeded with neither S3 nor P2P".into());
            }
        }
        Ok(())
    })();
    if paused {
        let _ = a.resume();
    }
    let _ = std::fs::remove_file(deny_path(&b));
    pb.heal();
    let check = result.and_then(|()| {
        for x in [&a, &b, &c] {
            for (name, want) in [
                ("after-restart", &b"after-restart"[..]),
                ("after-freeze", &b"after-freeze"[..]),
            ] {
                eventually(
                    &format!("{name} on {}", x.name),
                    Duration::from_secs(60),
                    || {
                        anyhow::ensure!(std::fs::read(x.mnt.join(name))? == want, "not yet");
                        Ok(())
                    },
                )?;
            }
        }
        Ok(())
    });
    if check.is_err() || !failures.is_empty() {
        eprintln!("    {NAME}: B's log tail:\n{}", b.tail_log_n(80));
        eprintln!("    {NAME}: A's log tail:\n{}", a.tail_log_n(60));
    }
    let unmounted = [c.unmount(), b.unmount(), a.unmount()];
    anyhow::ensure!(failures.is_empty(), "{}", failures.join("\n"));
    check?;
    for u in unmounted {
        u?;
    }
    Ok(())
}
