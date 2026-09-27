//! Plan 30 §M10 scenarios: flexible-quorum continuation epochs.
//!
//! - `epoch-missing-node`: three nodes with `epoch_slack = 1`. The third
//!   unmounts; the holder and the second lose S3 (their own S3 relays are
//!   cut) and form an epoch of 2/3 and keep writing. The third remounts
//!   with S3 but no P2P path to them: its takeover of the expired lease
//!   the epoch carries must be refused (no promise outlasts the expiry —
//!   the members are silent), and nothing it writes may land meanwhile.
//!   S3 returns: the epoch flushes, the third's writes go through the
//!   holder, everyone converges with no conflict copy.
//! - `epoch-slack-zero-unchanged`: `f = 0` (the default) writes no
//!   heartbeat object at all and its per-write S3 requests are the same
//!   as before M10; then `f = 1` on a fresh pair measures the steady
//!   state (on-demand promises: no heartbeat PUT while nothing expires).
//!
//! Every node goes through its own counting S3 relay (`CountingProxy`),
//! so a cut is per node and the heartbeat traffic is counted per node.
//! Backups are off (`CONSTELLATION_BACKUP_RTT_BUDGET_MS=0`): the M9 claim
//! rule is the model's and the core tests' (`m9_*`, `epoch_rules`); here
//! the lease is `Local` so the epoch always carries it.

use super::{ensure_no_conflicts, eventually, lease_of, log_segment_seqs, setup, ts, wait_for_p2p};
use crate::client::Client;
use crate::reqlog::CountingProxy;
use crate::s3env::BUCKET;
use anyhow::{Context, Result};
use std::time::{Duration, Instant};

/// Lease TTL for these scenarios: the promise TTL is a quarter of it.
/// An epoch carries the holder's lease only while it is usable, so the
/// window between the S3 cut and the formation (the failure grace, the
/// pings, the propose round) must fit in what is left of it after the
/// last renewal (renewed at half-TTL): 20 s leaves at least 9 s.
const TTL_MS: u64 = 20_000;

fn epoch_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["epoch"].clone())
}

fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn node_id(c: &Client) -> Result<u64> {
    c.control_status()?["node_id"]
        .as_u64()
        .with_context(|| format!("{} reports no node id", c.name))
}

/// Heartbeat PUTs (and all heartbeat requests) through `p`.
fn heartbeat_traffic(p: &CountingProxy) -> (usize, usize) {
    let reqs = p.requests();
    let all = reqs.iter().filter(|r| r.touches("heartbeat")).count();
    let puts = reqs
        .iter()
        .filter(|r| r.method == "PUT" && r.touches("heartbeat"))
        .count();
    (puts, all)
}

fn node(
    root: &std::path::Path,
    name: &str,
    proxy: &CountingProxy,
    backend: &str,
    extra: &[(&str, &str)],
) -> Result<Client> {
    let mut c = Client::new(root, name, &proxy.endpoint(), backend)?
        .with_own_node_key()
        .with_env("CONSTELLATION_LEASE_TTL_MS", &TTL_MS.to_string())
        .with_env("CONSTELLATION_LEASE_IDLE_RELEASE_MS", "600000")
        .with_env("CONSTELLATION_SYNC_INTERVAL_MS", "200")
        .with_env("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")
        .with_env("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0");
    for (k, v) in extra {
        c = c.with_env(k, v);
    }
    Ok(c)
}

fn print_epoch(scenario: &str, who: &str, e: &serde_json::Value) {
    eprintln!(
        "    {scenario}: {who} epoch: active {} members {} slack {} carrier {} promise_until {} | \
         promise puts {} answered {} refused {} | checks {} takeovers refused {} flush-exempt {} \
         stale claims {}",
        e["active"],
        e["members"],
        n(e, "epoch_slack"),
        e["carrier"],
        e["promise_until_ms"],
        n(e, "promise_puts"),
        n(e, "promise_requests_answered"),
        n(e, "promise_requests_refused"),
        n(e, "promise_checks"),
        n(e, "takeovers_refused_promises"),
        n(e, "promise_flush_exempt"),
        n(e, "stale_claims"),
    );
}

pub fn epoch_missing_node(_seed: u64) -> Result<()> {
    const NAME: &str = "epoch-missing-node";
    let (env, root) = setup(NAME)?;
    // The counting relays chain through toxiproxy's `s3` route.
    env.s3_proxy()?;
    let (pa, pb, pc) = (
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    );
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut a = node(root.path(), "a", &pa, &backend, &[])?;
    let mut b = node(root.path(), "b", &pb, &backend, &[])?;
    let mut c = node(root.path(), "c", &pc, &backend, &[])?;
    a.fs_create()?;
    a.fs_set_epoch_slack(1)?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;
    std::fs::create_dir(a.mnt.join("a"))?;
    std::fs::create_dir(a.mnt.join("b"))?;
    std::fs::create_dir(a.mnt.join("c"))?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
        Ok(())
    })?;
    for x in [&b, &c] {
        eventually("dirs visible everywhere", Duration::from_secs(20), || {
            anyhow::ensure!(x.mnt.join("c").is_dir(), "not on {}", x.name);
            Ok(())
        })?;
    }
    let a_id = node_id(&a)?;
    for x in [&a, &b, &c] {
        eventually("f = 1 advertised", Duration::from_secs(20), || {
            let e = epoch_of(x)?;
            anyhow::ensure!(n(&e, "epoch_slack") == 1, "{}: {e}", x.name);
            Ok(())
        })?;
    }

    // C goes offline (its registry record stays: it is still a roster
    // member). A and B lose S3.
    c.unmount()?;
    eprintln!("    {NAME}: A's lease before the cut: {}", lease_of(&a)?);
    pa.cut();
    pb.cut();
    let cut = Instant::now();
    for x in [&a, &b] {
        eventually(
            &format!("{} is in an epoch of 2/3", x.name),
            Duration::from_secs(30),
            || {
                let e = epoch_of(x)?;
                anyhow::ensure!(e["active"] == true, "{}: {e}", x.name);
                anyhow::ensure!(
                    e["members"].as_array().is_some_and(|m| m.len() == 2),
                    "{}: {e}",
                    x.name
                );
                anyhow::ensure!(e["carrier"].as_u64() == Some(a_id), "{}: {e}", x.name);
                Ok(())
            },
        )?;
    }
    eprintln!(
        "    {NAME}: epoch of 2/3 formed {:.1}s after the cut",
        cut.elapsed().as_secs_f64()
    );
    std::fs::write(a.mnt.join("a/from-a"), b"epoch-a")?;
    std::fs::write(b.mnt.join("b/from-b"), b"epoch-b")?;
    let backlog = a.control_status()?["spool"]["journal_backlog"]
        .as_u64()
        .unwrap_or(0);
    anyhow::ensure!(backlog > 0, "A's epoch writes are not journaled");

    // C comes back with S3 but no P2P path to A and B.
    let mut c = node(
        root.path(),
        "c",
        &pc,
        &backend,
        &[("CONSTELLATION_P2P", "off")],
    )?;
    c.mount()?;
    // Past the lease's expiry (A cannot renew), C tries to write. Its
    // takeover must be refused; the write blocks (the op is in doubt)
    // and must not land while the epoch holds the lease.
    std::thread::sleep(Duration::from_millis(TTL_MS + 2_000));
    let c_mnt = c.mnt.clone();
    let writer = std::thread::spawn(move || std::fs::write(c_mnt.join("c/from-c"), b"c"));
    eventually("C's takeover is refused", Duration::from_secs(40), || {
        let e = epoch_of(&c)?;
        anyhow::ensure!(n(&e, "takeovers_refused_promises") > 0, "C: {e}");
        Ok(())
    })?;
    let c_lease = lease_of(&c)?;
    anyhow::ensure!(
        c_lease["held"] != true,
        "C holds the lease the epoch carries: {c_lease}"
    );
    print_epoch(NAME, "C (refused)", &epoch_of(&c)?);
    let a_lease = lease_of(&a)?;
    eprintln!("    {NAME}: A's lease during the epoch: {a_lease}");

    // S3 returns: the epoch flushes (A re-claims its own lease), B closes,
    // C's write goes through the holder.
    pa.heal();
    pb.heal();
    for x in [&a, &b] {
        eventually(
            &format!("{}'s epoch closes and drains", x.name),
            Duration::from_secs(60),
            || {
                let s = x.control_status()?;
                anyhow::ensure!(s["epoch"]["active"] != true, "{}: {}", x.name, s["epoch"]);
                anyhow::ensure!(
                    s["spool"]["journal_backlog"].as_u64() == Some(0),
                    "{}: {}",
                    x.name,
                    s["spool"]
                );
                Ok(())
            },
        )?;
    }
    let first = writer.join().expect("writer thread");
    eprintln!("    {NAME}: C's blocked write returned {first:?}");
    if first.is_err() {
        // In doubt (EIO past the acquire deadline): retried by the user.
        std::fs::write(c.mnt.join("c/from-c"), b"c")?;
    }
    for x in [&a, &b, &c] {
        eventually(
            &format!("{} converges", x.name),
            Duration::from_secs(60),
            || {
                for (path, want) in [
                    ("a/from-a", &b"epoch-a"[..]),
                    ("b/from-b", b"epoch-b"),
                    ("c/from-c", b"c"),
                ] {
                    let got = std::fs::read(x.mnt.join(path));
                    anyhow::ensure!(
                        got.as_deref().ok() == Some(want),
                        "{} has {path} = {got:?}",
                        x.name
                    );
                }
                Ok(())
            },
        )?;
    }
    ensure_no_conflicts(&[&a, &b, &c])?;
    for (who, x, p) in [("A", &a, &pa), ("B", &b, &pb), ("C", &c, &pc)] {
        print_epoch(NAME, who, &epoch_of(x)?);
        let (puts, all) = heartbeat_traffic(p);
        eprintln!("    {NAME}: {who} heartbeat requests: {all} ({puts} PUTs)");
    }
    Ok(())
}

pub fn epoch_slack_zero_unchanged(_seed: u64) -> Result<()> {
    const NAME: &str = "epoch-slack-zero-unchanged";
    let (env, root) = setup(NAME)?;
    env.s3_proxy()?;
    // f = 0: no heartbeat object, ever.
    let (pa, pb) = (env.counting_proxy()?, env.counting_proxy()?);
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut a = node(root.path(), "a", &pa, &backend, &[])?;
    let mut b = node(root.path(), "b", &pb, &backend, &[])?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    let start = Instant::now();
    for i in 0..60 {
        let who = if i % 2 == 0 { &a } else { &b };
        std::fs::write(who.mnt.join(format!("f{i}")), b"x")?;
    }
    std::thread::sleep(Duration::from_secs(20));
    for (who, p) in [("A", &pa), ("B", &pb)] {
        let (puts, all) = heartbeat_traffic(p);
        let t = p.tally();
        eprintln!(
            "    {NAME}: f=0 {who}: {all} heartbeat requests over {:.0}s; S3 {t:?}",
            start.elapsed().as_secs_f64()
        );
        anyhow::ensure!(
            all == 0 && puts == 0,
            "f = 0 touched heartbeat/ ({all} requests)"
        );
    }
    for x in [&a, &b] {
        let e = epoch_of(x)?;
        anyhow::ensure!(
            n(&e, "epoch_slack") == 0 && n(&e, "promise_puts") == 0,
            "{e}"
        );
    }
    a.unmount()?;
    b.unmount()?;

    // f = 1: the steady state (idle, then writing) — on-demand promises
    // publish only the one slack advertisement per mount.
    let (pc, pd) = (env.counting_proxy()?, env.counting_proxy()?);
    let backend = format!("s3://{BUCKET}/{NAME}-f1-{}", ts());
    let mut c = node(root.path(), "c", &pc, &backend, &[])?;
    let mut d = node(root.path(), "d", &pd, &backend, &[])?;
    c.fs_create()?;
    c.fs_set_epoch_slack(1)?;
    c.mount()?;
    d.mount()?;
    wait_for_p2p(&[&c, &d])?;
    // Warm-up: the mounts' slack advertisements.
    for i in 0..10 {
        let who = if i % 2 == 0 { &c } else { &d };
        std::fs::write(who.mnt.join(format!("w{i}")), b"x")?;
    }
    std::thread::sleep(Duration::from_secs(5));
    for (who, p) in [("C", &pc), ("D", &pd)] {
        let (puts, _) = heartbeat_traffic(p);
        eprintln!(
            "    {NAME}: f=1 {who}: {puts} heartbeat PUTs at mount (the slack advertisement)"
        );
        p.reset();
    }
    // Steady state: writes from both for a minute, then idle.
    let start = Instant::now();
    for i in 0..120 {
        let who = if i % 2 == 0 { &c } else { &d };
        std::fs::write(who.mnt.join(format!("f{i}")), b"x")?;
        std::thread::sleep(Duration::from_millis(250));
    }
    std::thread::sleep(Duration::from_secs(30));
    let secs = start.elapsed().as_secs_f64();
    for (who, x, p) in [("C", &c, &pc), ("D", &d, &pd)] {
        let (puts, all) = heartbeat_traffic(p);
        let per_day = puts as f64 * 86_400.0 / secs;
        print_epoch(NAME, who, &epoch_of(x)?);
        eprintln!(
            "    {NAME}: f=1 {who}: steady state {puts} heartbeat PUTs ({all} requests) over \
             {secs:.0}s: {per_day:.0}/day"
        );
        anyhow::ensure!(
            puts == 0,
            "{who} published {puts} heartbeats in steady state (on demand: expected none)"
        );
    }
    Ok(())
}

/// Plan 30 §M10: an epoch whose holder never returns, resolved by
/// `constellation leave --node-id`. Three nodes, `f = 1`: C goes offline,
/// A and B form an epoch that carries A's lease, A writes and is killed
/// (its journal is lost with it). B gets S3 back; its epoch is frozen
/// (A is missing) and B stays silent, so no TTL takeover can pass. The
/// operator retires A from B: the lease is fenced (epoch bump, A in its
/// `retired` list), B abandons the epoch and promises again; C comes back
/// and B takes the lease over with C's promise. A's state dir can never
/// mount again (the registry tombstone), and nothing else it had is
/// served.
pub fn epoch_holder_retired(_seed: u64) -> Result<()> {
    const NAME: &str = "epoch-holder-retired";
    let (env, root) = setup(NAME)?;
    env.s3_proxy()?;
    let (pa, pb, pc) = (
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    );
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut a = node(root.path(), "a", &pa, &backend, &[])?;
    let mut b = node(root.path(), "b", &pb, &backend, &[])?;
    let mut c = node(root.path(), "c", &pc, &backend, &[])?;
    a.fs_create()?;
    a.fs_set_epoch_slack(1)?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;
    std::fs::create_dir(a.mnt.join("d"))?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
        Ok(())
    })?;
    eventually("d visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("d").is_dir());
        Ok(())
    })?;
    let a_id = node_id(&a)?;
    c.unmount()?;
    pa.cut();
    pb.cut();
    for x in [&a, &b] {
        eventually(
            &format!("{} is in an epoch carrying A's lease", x.name),
            Duration::from_secs(30),
            || {
                let e = epoch_of(x)?;
                anyhow::ensure!(e["active"] == true, "{}: {e}", x.name);
                anyhow::ensure!(e["carrier"].as_u64() == Some(a_id), "{}: {e}", x.name);
                Ok(())
            },
        )?;
    }
    std::fs::write(a.mnt.join("d/lost-with-a"), b"a")?;
    a.kill9()?;
    pb.heal();
    // B: frozen (A missing), silent; C returns with S3 and cannot take the
    // lease either while B's epoch is open.
    let mut c = node(root.path(), "c", &pc, &backend, &[])?;
    c.mount()?;
    std::thread::sleep(Duration::from_millis(TTL_MS + 2_000));
    let c_mnt = c.mnt.clone();
    let blocked = std::thread::spawn(move || std::fs::write(c_mnt.join("d/from-c"), b"c"));
    eventually("C's takeover is refused", Duration::from_secs(40), || {
        let e = epoch_of(&c)?;
        anyhow::ensure!(n(&e, "takeovers_refused_promises") > 0, "C: {e}");
        Ok(())
    })?;
    // The operator retires A.
    b.leave(Some(a_id), true)?;
    eventually("B abandons the epoch", Duration::from_secs(30), || {
        let e = epoch_of(&b)?;
        anyhow::ensure!(
            e["active"] != true && e["members"] == serde_json::json!([]),
            "B: {e}"
        );
        Ok(())
    })?;
    let first = blocked.join().expect("writer thread");
    eprintln!("    {NAME}: C's write during the frozen epoch returned {first:?}");
    // Writes go through again (C or B takes the fenced lease over, with
    // the other's promise).
    std::fs::write(b.mnt.join("d/from-b"), b"b")?;
    if first.is_err() {
        std::fs::write(c.mnt.join("d/from-c"), b"c")?;
    }
    for x in [&b, &c] {
        eventually(
            &format!("{} converges", x.name),
            Duration::from_secs(60),
            || {
                for (path, want) in [("d/from-b", &b"b"[..]), ("d/from-c", b"c")] {
                    let got = std::fs::read(x.mnt.join(path));
                    anyhow::ensure!(
                        got.as_deref().ok() == Some(want),
                        "{} has {path} = {got:?}",
                        x.name
                    );
                }
                Ok(())
            },
        )?;
        print_epoch(NAME, &x.name, &epoch_of(x)?);
    }
    // A's journal died with it and was never flushed.
    anyhow::ensure!(
        !b.mnt.join("d/lost-with-a").exists(),
        "A's unflushed write surfaced"
    );
    // A can never come back under its old identity.
    pa.heal();
    let mut a = node(root.path(), "a", &pa, &backend, &[])?;
    let remount = a.mount();
    anyhow::ensure!(remount.is_err(), "the retired node remounted");
    eprintln!(
        "    {NAME}: A's remount refused: {}",
        remount
            .err()
            .map(|e| format!("{e:#}").lines().next().unwrap_or("").to_string())
            .unwrap_or_default()
    );
    Ok(())
}

/// `epoch-member-dies-with-chunk`: a member dies holding the only copy of
/// a chunk its epoch write named (plan 30 §M10 × §M9: members follow the
/// hold owner's stream, which in an epoch streams manifests whose chunks
/// are on a member only).
///
/// Three nodes, `f = 0`; all three lose S3 and form an epoch carrying A's
/// lease. C writes two files: their manifests are forwarded to A and
/// streamed to B, their chunks stay on C (nothing reaches S3 in an
/// epoch). B reads the first from C. Then C stops (`SIGSTOP`: its disk
/// holds the only copy of the second file's chunk), and:
/// - B's read of the second file fails; it never returns other bytes;
/// - S3 returns for A and B: the epoch closes, and A's ship defers the
///   two manifests, which wait for C's chunks (`held.deferred`) — and
///   only them (fix "capture under an epoch hold": the epoch journal is
///   captured, so the plan skips exactly a deferred transaction and its
///   dependents): A's own write after the close reaches S3 and B while
///   C is away, and `status.held.remote` names C's chunks for the
///   operator. A fresh node reading the bucket never sees a manifest
///   naming a chunk S3 lacks (that read would fail);
/// - B keeps both manifests as speculation (`speculation.outstanding`),
///   so nothing B publishes contains them;
/// - B converges promptly, C still away (fix "S3 client recovery after a
///   cut, and stream resubscription after an epoch"): its S3 client
///   answers right after the heal (its chunk-fetch burst met the cut),
///   its frozen epoch closes once A's flush moved the carried lease, it
///   applies A's write, and it follows A's stream again.
///
/// Then, by the seed's parity: C comes back — its chunks go up, A ships
/// the manifests, and every node (the fresh one included) reads both
/// files; or C is given up — `constellation repair drop-held <ino>
/// --remote` on A turns each write into a conflict copy with the chunk
/// as a hole, its refusal reaches the log, B's streamed copies and C's
/// own shadows (once it is back) roll back, and every node converges on
/// the two files as empty. Nothing is lost silently either way.
pub fn epoch_member_dies_with_chunk(seed: u64) -> Result<()> {
    const NAME: &str = "epoch-member-dies-with-chunk";
    let (env, root) = setup(NAME)?;
    env.s3_proxy()?;
    let (pa, pb, pc, pd) = (
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
        env.counting_proxy()?,
    );
    let prefix = format!("{NAME}-{}", ts());
    let backend = format!("s3://{BUCKET}/{prefix}");
    let log_head = || -> Result<u64> {
        Ok(log_segment_seqs(&env.direct_endpoint, &prefix, "p0")?
            .last()
            .copied()
            .unwrap_or(0))
    };
    // A's close flush waits this long for chunks a member forwarded as
    // pending before it ships what does not need them.
    let extra = [("CONSTELLATION_REMOTE_CHUNK_WAIT_S", "5")];
    let mut a = node(root.path(), "a", &pa, &backend, &extra)?;
    let mut b = node(root.path(), "b", &pb, &backend, &extra)?;
    let mut c = node(root.path(), "c", &pc, &backend, &extra)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;
    std::fs::create_dir(a.mnt.join("c"))?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        anyhow::ensure!(lease_of(&a)?["held"] == true, "{}", lease_of(&a)?);
        Ok(())
    })?;
    for x in [&b, &c] {
        eventually("c/ visible everywhere", Duration::from_secs(20), || {
            anyhow::ensure!(x.mnt.join("c").is_dir(), "not on {}", x.name);
            Ok(())
        })?;
    }
    let a_id = node_id(&a)?;
    let c_id = node_id(&c)?;

    pa.cut();
    pb.cut();
    pc.cut();
    for x in [&a, &b, &c] {
        eventually(
            &format!("{} is in the epoch of 3", x.name),
            Duration::from_secs(40),
            || {
                let e = epoch_of(x)?;
                anyhow::ensure!(e["active"] == true, "{}: {e}", x.name);
                anyhow::ensure!(e["carrier"].as_u64() == Some(a_id), "{}: {e}", x.name);
                Ok(())
            },
        )?;
    }

    // Two files of distinct bytes, one chunk each, written on C.
    let bytes = |salt: u64| -> Vec<u8> {
        let mut x = seed ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (0..192 * 1024)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    };
    let (early, only) = (bytes(1), bytes(2));
    std::fs::write(c.mnt.join("c/early"), &early).context("C's first epoch write")?;
    std::fs::write(c.mnt.join("c/only-on-c"), &only).context("C's second epoch write")?;

    // B has both manifests from A's stream, and reads the first file's
    // bytes from C (dirty there, not in S3).
    eventually("B reads C's epoch write", Duration::from_secs(30), || {
        let got = std::fs::read(b.mnt.join("c/early"))?;
        anyhow::ensure!(got == early, "B read {} bytes of c/early", got.len());
        Ok(())
    })?;
    eventually("B has the second manifest", Duration::from_secs(30), || {
        let len = std::fs::metadata(b.mnt.join("c/only-on-c"))?.len();
        anyhow::ensure!(len == only.len() as u64, "B sees c/only-on-c at {len}");
        Ok(())
    })?;
    let fetched = b.control_status()?["coop"]["epoch_member_fetches"]
        .as_u64()
        .unwrap_or(0);
    anyhow::ensure!(fetched > 0, "B's read did not come from an epoch member");

    // C dies with the only copy of c/only-on-c's chunk.
    c.pause()?;
    let read_on = |x: &Client, path: &str| -> Result<std::io::Result<Vec<u8>>> {
        let p = x.mnt.join(path);
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(std::fs::read(p));
        });
        rx.recv_timeout(Duration::from_secs(120))
            .with_context(|| format!("reading {path} on {} did not return", x.name))
    };
    match read_on(&b, "c/only-on-c")? {
        Err(e) => eprintln!("    {NAME}: B's read with C gone failed as it must: {e}"),
        Ok(got) => anyhow::bail!(
            "B read c/only-on-c with its only copy gone: {} bytes, {}",
            got.len(),
            if got == only {
                "the right ones (from where?)"
            } else {
                "WRONG ONES"
            }
        ),
    }

    // S3 returns for A and B; C stays gone.
    pa.heal();
    pb.heal();
    let healed_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;
    let healed = Instant::now();
    eventually(
        "A's epoch closes with the manifests deferred",
        Duration::from_secs(120),
        || {
            let s = a.control_status()?;
            anyhow::ensure!(s["epoch"]["active"] != true, "A: {}", s["epoch"]);
            anyhow::ensure!(
                s["held"]["deferred"].as_u64().unwrap_or(0) > 0,
                "A defers nothing: held {} spool {}",
                s["held"],
                s["spool"]
            );
            Ok(())
        },
    )?;
    // Closed, not merely frozen (a frozen member's epoch is inactive but
    // open): B learns from S3 that A's flush moved the carried lease.
    eventually("B's epoch closes", Duration::from_secs(60), || {
        let s = b.control_status()?;
        anyhow::ensure!(
            s["epoch"]["active"] != true && s["epoch"]["epoch_id"].is_null(),
            "B: {}",
            s["epoch"]
        );
        anyhow::ensure!(
            s["speculation"]["outstanding"].as_u64().unwrap_or(0) > 0,
            "B no longer holds C's manifests as speculation: {}",
            s["speculation"]
        );
        Ok(())
    })?;
    if let Ok(got) = read_on(&b, "c/only-on-c")? {
        anyhow::ensure!(got.is_empty() || got == only, "B read WRONG bytes");
        anyhow::bail!("B read c/only-on-c ({} bytes) with C gone", got.len());
    }

    // Fix "capture under an epoch hold": only C's manifests (and what
    // depends on them) wait. A's own write after the close ships while
    // C is away — the log moves, and B applies it — with the manifests
    // still deferred.
    let head_before = log_head()?;
    let after = b"written on A after the close, while C is away".to_vec();
    std::fs::write(a.mnt.join("c/after-close"), &after).context("A's write after the close")?;
    eventually(
        "A's own write reaches S3 while C's chunks are away",
        Duration::from_secs(60),
        || {
            let s = a.control_status()?;
            anyhow::ensure!(
                s["held"]["deferred"].as_u64().unwrap_or(0) >= 2,
                "A no longer defers C's manifests: {}",
                s["held"]
            );
            anyhow::ensure!(
                !s["held"]["opaque"].as_bool().unwrap_or(true),
                "the epoch journal was uncaptured: {}",
                s["held"]
            );
            let head = log_head()?;
            anyhow::ensure!(
                head > head_before,
                "the log is still at {head}: A spool {} held {}\n--- A log ---\n{}",
                s["spool"],
                s["held"],
                a.tail_log_n(40)
            );
            Ok(())
        },
    )?;
    // What the operator sees: the chunks A waits for, and whose.
    let remote = a.control_status()?["held"]["remote"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    anyhow::ensure!(
        remote.len() >= 2 && remote.iter().all(|r| r["node"].as_u64() == Some(c_id)),
        "status does not name C's chunks under held.remote: {remote:?}"
    );
    let deferred_inos: std::collections::BTreeSet<u64> =
        remote.iter().filter_map(|r| r["ino"].as_u64()).collect();
    eprintln!(
        "    {NAME}: A after the close: held {} remote {:?}",
        a.control_status()?["held"],
        remote
            .iter()
            .map(|r| format!("{}:{}@{}", r["ino"], r["path"], r["node"]))
            .collect::<Vec<_>>()
    );
    let a_status = a.control_status()?;
    eprintln!(
        "    {NAME}: A after the close: held {} spool {}",
        a_status["held"], a_status["spool"]
    );
    let mut d = node(
        root.path(),
        "d",
        &pd,
        &backend,
        &[("CONSTELLATION_P2P", "off")],
    )?;
    d.mount()?;
    eventually("D sees the pre-epoch c/", Duration::from_secs(60), || {
        anyhow::ensure!(d.mnt.join("c").is_dir(), "not yet");
        Ok(())
    })?;
    for path in ["c/early", "c/only-on-c"] {
        if !d.mnt.join(path).exists() {
            eprintln!("    {NAME}: D (S3 only) does not see {path} yet");
            continue;
        }
        let got = read_on(&d, path)?
            .with_context(|| format!("D cannot read {path}: the log names a chunk S3 lacks"))?;
        anyhow::ensure!(
            got.is_empty(),
            "D read {} bytes of {path} before C's chunks are in S3",
            got.len()
        );
    }

    eventually("D (S3 only) has A's write", Duration::from_secs(60), || {
        let got = std::fs::read(d.mnt.join("c/after-close"))?;
        anyhow::ensure!(got == after, "D has {} bytes of c/after-close", got.len());
        Ok(())
    })?;
    // Fix "S3 client recovery after a cut, and stream resubscription
    // after an epoch": B converges promptly after the heal, while C is
    // still away. Its S3 client answers again at once (its chunk-fetch
    // burst for C's file met the cut: nothing of that may linger), its
    // epoch closes once A's flush has moved the carried lease (a frozen
    // member used to skip the probe that learns it, and stayed in the
    // epoch until C returned), it applies A's write, and it follows A's
    // stream again (A ended it at the close).
    let b_view = |b: &Client| -> String {
        b.control_status()
            .map(|s| {
                format!(
                    "epoch {} spool {} log_stream {} s3 {{unanswered {}, last answered {}, \
                     last unanswered {} {}}}; its relay counted {} requests",
                    s["epoch"],
                    s["spool"],
                    s["log_stream"],
                    s["s3"]["unanswered"],
                    s["s3"]["last_answered_unix_ms"],
                    s["s3"]["last_unanswered_unix_ms"],
                    s["s3"]["last_unanswered_error"],
                    pb.tally().total()
                )
            })
            .unwrap_or_else(|e| format!("status: {e:#}"))
    };
    eventually(
        "B's S3 client answers after the heal",
        Duration::from_secs(20),
        || {
            let s = b.control_status()?;
            let answered = s["s3"]["last_answered_unix_ms"].as_i64().unwrap_or(0);
            anyhow::ensure!(answered > healed_at, "B: {}", b_view(&b));
            Ok(())
        },
    )?;
    eventually(
        "B has A's write after the close, with C away",
        Duration::from_secs(30),
        || {
            let got = std::fs::read(b.mnt.join("c/after-close"))
                .with_context(|| format!("B: {}", b_view(&b)))?;
            anyhow::ensure!(
                got == after,
                "B has {} bytes of c/after-close: {}",
                got.len(),
                b_view(&b)
            );
            Ok(())
        },
    )?;
    eventually(
        "B follows A's stream again after the close",
        Duration::from_secs(30),
        || {
            let s = b.control_status()?;
            anyhow::ensure!(
                s["log_stream"]["upstream"].as_u64() == Some(a_id)
                    && s["log_stream"]["live"] == true,
                "B: {}",
                b_view(&b)
            );
            Ok(())
        },
    )?;
    {
        let s = b.control_status()?;
        let failed_at = s["s3"]["last_unanswered_unix_ms"].as_i64().unwrap_or(0);
        // What was in flight at the heal fails within its retry budget
        // (2 s); nothing may fail after that.
        anyhow::ensure!(
            failed_at < healed_at + 10_000,
            "B's S3 requests still fail after the heal: {}",
            b_view(&b)
        );
        anyhow::ensure!(
            s["spool"]["last_ship_error"].is_null(),
            "B still reports a ship error: {}",
            b_view(&b)
        );
    }
    eprintln!(
        "    {NAME}: B converged {:.1}s after the heal: {}",
        healed.elapsed().as_secs_f64(),
        b_view(&b)
    );
    anyhow::ensure!(
        b.control_status()?["speculation"]["outstanding"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "B no longer holds C's manifests as speculation"
    );

    if seed.is_multiple_of(2) {
        // C returns: its chunks go up, A ships the manifests, all converge.
        c.resume()?;
        pc.heal();
        for x in [&a, &b, &c, &d] {
            eventually(
                &format!("{} reads C's files", x.name),
                Duration::from_secs(120),
                || {
                    for (path, want) in [("c/early", &early), ("c/only-on-c", &only)] {
                        let got = std::fs::read(x.mnt.join(path))?;
                        anyhow::ensure!(
                            &got == want,
                            "{} has {} bytes of {path}",
                            x.name,
                            got.len()
                        );
                    }
                    Ok(())
                },
            )?;
        }
        eventually("A's journal drains", Duration::from_secs(60), || {
            let s = a.control_status()?;
            anyhow::ensure!(
                s["spool"]["journal_backlog"].as_u64() == Some(0),
                "A: {}",
                s["spool"]
            );
            Ok(())
        })?;
        ensure_no_conflicts(&[&a, &b, &c, &d])?;
    } else {
        // C is given up: the operator drops what waits for its chunks.
        for ino in &deferred_inos {
            let reply =
                a.control(&serde_json::json!({ "cmd": "drop_held", "ino": ino, "remote": true }))?;
            anyhow::ensure!(reply["resp"] == "ok", "drop-held --remote failed: {reply}");
            eprintln!("    {NAME}: {}", reply["detail"]);
        }
        eventually(
            "A drains, and the conflict copies exist",
            Duration::from_secs(90),
            || {
                let s = a.control_status()?;
                anyhow::ensure!(
                    s["spool"]["journal_backlog"].as_u64() == Some(0)
                        && s["held"]["deferred"].as_u64() == Some(0)
                        && s["held"]["remote"].as_array().is_some_and(|r| r.is_empty()),
                    "A: spool {} held {}",
                    s["spool"],
                    s["held"]
                );
                // The copies live beside the files: `c/.constellation-conflict/`.
                let dir = a.mnt.join("c/.constellation-conflict");
                let names: Vec<String> = std::fs::read_dir(&dir)
                    .map(|d| {
                        d.filter_map(|e| e.ok())
                            .map(|e| e.file_name().to_string_lossy().into_owned())
                            .collect()
                    })
                    .unwrap_or_default();
                anyhow::ensure!(
                    names.iter().any(|n| n.starts_with("early@"))
                        && names.iter().any(|n| n.starts_with("only-on-c@")),
                    "conflict copies on A: {names:?} (spool {})",
                    s["spool"]
                );
                Ok(())
            },
        )?;
        // C comes back with its bytes, too late: its writes were refused
        // in the log, so its own copies roll back like B's streamed ones.
        c.resume()?;
        pc.heal();
        for x in [&a, &b, &c, &d] {
            eventually(
                &format!("{} converges on the dropped files", x.name),
                Duration::from_secs(120),
                || {
                    for path in ["c/early", "c/only-on-c"] {
                        let got = std::fs::read(x.mnt.join(path))?;
                        anyhow::ensure!(
                            got.is_empty(),
                            "{} still has {} bytes of {path}",
                            x.name,
                            got.len()
                        );
                    }
                    let got = std::fs::read(x.mnt.join("c/after-close"))?;
                    anyhow::ensure!(got == after, "{} lost c/after-close", x.name);
                    let s = x.control_status()?;
                    anyhow::ensure!(
                        s["speculation"]["outstanding"].as_u64() == Some(0)
                            && s["spool"]["journal_backlog"].as_u64() == Some(0),
                        "{}: speculation {} spool {}",
                        x.name,
                        s["speculation"],
                        s["spool"]
                    );
                    Ok(())
                },
            )?;
        }
        ensure_no_conflicts(&[&b, &c, &d])?;
    }
    for x in [&mut a, &mut b, &mut c, &mut d] {
        x.unmount()?;
    }
    Ok(())
}
