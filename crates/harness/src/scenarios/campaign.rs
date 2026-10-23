//! The host-load campaign's own scenario (phase 0): `load-probe`, a short
//! two-node workload that runs every checker of [`crate::verify`] under
//! whatever load profile is set (`CONSTELLATION_HARNESS_LOAD`), so a
//! profile is proved to apply and the checkers to see. With
//! `CONSTELLATION_HARNESS_LOAD_PROBE_BREAK=<checker>` it breaks one
//! invariant on purpose and must then FAIL naming it: `data` (a byte of
//! an acknowledged file), `ledger` (an acknowledged file removed behind
//! the ledger's back), `exclusion` (an increment of the locked counter
//! without the lock), `token` (a forged `tagged op landed` line: a write
//! that committed after a conflicting grant, finding F1's shape),
//! `split-brain` (a forged second `acquired the lease` of one epoch),
//! `log-drop` (a forged `log lines dropped` summary line in a's log: a
//! daemon's log sink did not keep up, so the log checkers could not see
//! every line, plan 44),
//! `errno` (an error outside the documented set), `stall` (a FUSE write on
//! `a` held past the hang bound by the store-stall hook: needs
//! `CONSTELLATION_HARNESS_VERIFY=1` and a short
//! `CONSTELLATION_HARNESS_HANG_S`), `log-missing` (a node's logs removed
//! before the log checks), `latency` (a forged 30 s against a 1 s bound:
//! past the hard cap, a failure under any profile) and `latency-mild` (a
//! forged 2.5 s against 1 s, no host lag: a failure, except under an
//! applied probe-blind injector, where it is inconclusive) and `enoent` (a
//! forged `ENOENT` of a rename of a name the writer certainly owns).
//! `CONSTELLATION_HARNESS_LOAD_PROBE_S` sets the workload's length
//! (default 20 s).

use super::m9::{cluster, node_id};
use super::{eventually, lease_of};
use crate::client::Client;
use crate::verify::{ErrnoLog, Evidence, FileId, Ledger, OpCtx, Ownership};
use anyhow::{bail, Context, Result};
use rand::{Rng, SeedableRng};
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

const NAME: &str = "load-probe";
pub(super) const BREAKS: &[&str] = &[
    "data",
    "ledger",
    "exclusion",
    "token",
    "split-brain",
    "log-drop",
    "errno",
    "stall",
    "log-missing",
    "latency",
    "latency-mild",
    "enoent",
];

/// A write + `fsync` this slow, with the host attentive, is a latency
/// regression: the host slack of [`write_bound`]. Without a takeover the
/// protocol has no part in it (all of it is host slack).
const WRITE_BOUND: Duration = Duration::from_secs(10);

/// The protocol part and the bound of the slowest write + `fsync` of a
/// cell. After a seal-based backup takeover (`takeovers`) the successor
/// acknowledges no mutation until its predecessor's strict-read horizon
/// has passed, the quarantine the maintainer accepted (plan 43 gate G5,
/// Q10a: `Timings::ack_floor_wait`, "acknowledging no mutation until its
/// delegation horizon has passed wait_ms=..." in the successor's log), so
/// the protocol's part is the seal window plus the longest wait any
/// daemon logged (`logged`; measured, as `delegroot` does), and the bound
/// that plus [`WRITE_BOUND`] of host time. `logged` is checked against
/// the configured quarantine (`timings.ack_floor_wait(true)`, the most it
/// can be): a longer wait is a product change this bound must not absorb.
fn write_bound(
    timings: &crate::timings::Timings,
    takeovers: u64,
    logged: Duration,
) -> Result<(Duration, Duration)> {
    anyhow::ensure!(
        logged <= timings.ack_floor_wait(true),
        "a successor's quarantine waited {logged:?}, past the configured {:?} (plan 43 G5)",
        timings.ack_floor_wait(true)
    );
    let protocol = if takeovers == 0 {
        Duration::ZERO
    } else {
        timings.backup_takeover + logged
    };
    Ok((protocol, protocol + WRITE_BOUND))
}
const INCREMENTS: u64 = 20;

fn breaking() -> Result<Option<String>> {
    match std::env::var("CONSTELLATION_HARNESS_LOAD_PROBE_BREAK") {
        Ok(b) if !b.is_empty() => {
            anyhow::ensure!(
                BREAKS.contains(&b.as_str()),
                "CONSTELLATION_HARNESS_LOAD_PROBE_BREAK={b:?}: one of {BREAKS:?}"
            );
            Ok(Some(b))
        }
        _ => Ok(None),
    }
}

fn probe_secs() -> u64 {
    std::env::var("CONSTELLATION_HARNESS_LOAD_PROBE_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(20)
}

/// One node's writer: overwrites, renames and unlinks in its own
/// directory, every acknowledged op in the ledger, every error in the
/// errno log; the slowest write + `fsync` and when it began.
///
/// `ENOENT` of a rename or an unlink is ignored only for a name this
/// writer does not have: the unlink picks a name by arithmetic, so it
/// mostly names a file never renamed into place or already unlinked. The
/// directory is the writer's own (no other node, process or load item
/// touches it), so for a name the ledger holds as acknowledged made and
/// not removed since, with no failed rename or unlink on it, `ENOENT` is
/// the filesystem refusing the application's own effect: an error
/// (`enoent-owned`, [`ErrnoLog::record_notfound`]). A name whose last op
/// failed, or that a failed rename or unlink touched (an in-doubt op may
/// still land, once, later), stays ignored.
fn writer(
    mnt: &Path,
    node: &str,
    seed: u64,
    until: Instant,
    ledger: &Ledger,
    errnos: &ErrnoLog,
) -> (Duration, Instant, u64) {
    let dir = format!("w-{node}");
    let _ = std::fs::create_dir_all(mnt.join(&dir));
    let mut rng =
        rand::rngs::StdRng::seed_from_u64(seed ^ node.len() as u64 ^ node.as_bytes()[0] as u64);
    let (mut slowest, mut slowest_at, mut seq) = (Duration::ZERO, Instant::now(), 0u64);
    while Instant::now() < until {
        seq += 1;
        let k = seq % 12;
        let rel = format!("{dir}/f{k}");
        let id = FileId {
            path: rel.clone(),
            writer: node.to_string(),
            seq,
        };
        let len = rng.random_range(64..64 * 1024);
        let t = Instant::now();
        if let Err((step, e)) = ledger.write(mnt, &rel, id, len) {
            errnos.record(node, step, &e, t, OpCtx::default());
        }
        let took = t.elapsed();
        if took > slowest {
            (slowest, slowest_at) = (took, t);
        }
        if seq % 5 == 0 {
            let to = format!("{dir}/g{k}");
            let t = Instant::now();
            match std::fs::rename(mnt.join(&rel), mnt.join(&to)) {
                Ok(()) => ledger.acked_rename(&rel, &to),
                // ENOENT: the name is not there, which is an answer only
                // when this writer does not have it (see `notfound`).
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    errnos.record_notfound(ledger, node, "rename", &rel, &e, t);
                }
                Err(e) => {
                    ledger.failed_rename(&rel, &to, &e, t.elapsed());
                    errnos.record(node, "rename", &e, t, OpCtx::default())
                }
            }
        }
        if seq % 7 == 0 {
            let gone = format!("{dir}/g{}", (k + 3) % 12);
            let t = Instant::now();
            match std::fs::remove_file(mnt.join(&gone)) {
                Ok(()) => ledger.acked_unlink(&gone),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    errnos.record_notfound(ledger, node, "unlink", &gone, &e, t);
                }
                Err(e) => {
                    ledger.failed_op(&[&gone]);
                    errnos.record(node, "unlink", &e, t, OpCtx::default())
                }
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    (slowest, slowest_at, seq)
}

/// Increments of the shared counter under `flock`: (completed, in
/// doubt: the write went out but the fsync or the unlock failed).
fn counter(mnt: &Path, node: &str, errnos: &ErrnoLog) -> (u64, u64) {
    let (mut done, mut doubt) = (0, 0);
    let t = Instant::now();
    let f = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(mnt.join("counter"))
    {
        Ok(f) => f,
        Err(e) => {
            // The counter was created and seen on both mounts: an open
            // that fails is an error the application sees.
            errnos.record(node, "open counter", &e, t, OpCtx::default());
            return (0, 0);
        }
    };
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut i = 0;
    while i < INCREMENTS && Instant::now() < deadline {
        let t = Instant::now();
        // SAFETY: flock(2) on an fd this function owns.
        if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) } != 0 {
            let e = std::io::Error::last_os_error();
            errnos.record(
                node,
                "flock",
                &e,
                t,
                OpCtx {
                    lock_request: true,
                    ..Default::default()
                },
            );
            std::thread::sleep(Duration::from_millis(200));
            continue;
        }
        let t = Instant::now();
        let mut buf = [0u8; 8];
        let step = f.read_at(&mut buf, 0).and_then(|_| {
            let cur = u64::from_be_bytes(buf);
            f.write_at(&(cur + 1).to_be_bytes(), 0)?;
            f.sync_all()
        });
        match step {
            Ok(()) => done += 1,
            Err(e) => {
                doubt += 1;
                errnos.record(
                    node,
                    "increment",
                    &e,
                    t,
                    OpCtx {
                        under_lock: true,
                        ..Default::default()
                    },
                );
            }
        }
        // SAFETY: as above.
        unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };
        i += 1;
        std::thread::sleep(Duration::from_millis(30));
    }
    (done, doubt)
}

fn read_counter(path: &Path) -> Result<u64> {
    let b = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    anyhow::ensure!(b.len() == 8, "{}: {} bytes", path.display(), b.len());
    Ok(u64::from_be_bytes(b[..8].try_into().unwrap()))
}

pub(super) fn load_probe(seed: u64) -> Result<()> {
    load_probe_with(seed, &["a", "b"], false)
}

/// `load-probe-takeover`: the same workload with the root on `b`, so a
/// load profile aimed at `b` (`pause:...@b`; a `step-pause:...@b` may)
/// deposes the root and `a`, its backup and the delegate of `w-a`, seals
/// and takes over under load. The workload starts once `b` lists `a` as
/// its backup, so the first pause meets a seal, not a lease expiry.
pub(super) fn load_probe_takeover(seed: u64) -> Result<()> {
    load_probe_with(seed, &["b", "a"], true)
}

fn load_probe_with(seed: u64, names: &[&str], takeover: bool) -> Result<()> {
    let brk = breaking()?;
    let secs = probe_secs();
    let stall_file =
        std::env::temp_dir().join(format!("harness-load-probe-stall-{}", std::process::id()));
    let mut extra = vec![
        ("CONSTELLATION_LOCK_TTL_MS", "5000".to_string()),
        (
            "RUST_LOG",
            format!("info,{}", crate::verify::LOG_DIRECTIVES),
        ),
    ];
    if brk.as_deref() == Some("stall") {
        extra.push((
            "CONSTELLATION_FAULT_STORE_STALL_FILE",
            stall_file.display().to_string(),
        ));
    }
    let extra_refs: Vec<(&str, &str)> = extra.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let (env, _root, mut clients, _) = cluster(NAME, names, &extra_refs, 0)?;
    let result = (|| -> Result<()> {
        let ledger = Ledger::with_client_deadline(super::m9::client_deadline());
        let errnos = ErrnoLog::new();
        let epoch0 = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: {} holds epoch {epoch0}; workload {secs} s under load {:?}",
            clients[0].name,
            crate::load::label().unwrap_or_else(|| "none".into())
        );
        // The takeover cell's first pause must find the backup enrolled:
        // before that, the lease expires and the other node acquires it
        // without sealing anything (no backup takeover, no stranded rows).
        if takeover {
            super::m9::wait_for_backup(&clients[0], crate::load::scaled(Duration::from_secs(30)))?;
        }
        // ---- data and acknowledged writes.
        let until = Instant::now() + Duration::from_secs(secs);
        let slowest = std::thread::scope(|s| {
            let hs: Vec<_> = clients
                .iter()
                .map(|c| {
                    let (l, e) = (&ledger, &errnos);
                    s.spawn(move || writer(&c.mnt, &c.name, seed, until, l, e))
                })
                .collect();
            hs.into_iter()
                .map(|h| h.join().expect("writer"))
                .collect::<Vec<_>>()
        });
        for (c, (took, _, ops)) in clients.iter().zip(&slowest) {
            eprintln!(
                "    {NAME}: {} wrote {ops} payloads, slowest write+fsync {took:?}",
                c.name
            );
        }
        // ---- lock exclusion.
        std::fs::write(clients[0].mnt.join("counter"), 0u64.to_be_bytes())?;
        eventually("the counter on b", Duration::from_secs(60), || {
            anyhow::ensure!(read_counter(&clients[1].mnt.join("counter"))? == 0);
            Ok(())
        })?;
        let runs = std::thread::scope(|s| {
            let hs: Vec<_> = clients
                .iter()
                .map(|c| {
                    let e = &errnos;
                    s.spawn(move || counter(&c.mnt, &c.name, e))
                })
                .collect();
            hs.into_iter()
                .map(|h| h.join().expect("counter"))
                .collect::<Vec<_>>()
        });
        let done: u64 = runs.iter().map(|r| r.0).sum();
        let doubt: u64 = runs.iter().map(|r| r.1).sum();
        if brk.as_deref() == Some("exclusion") {
            let p = clients[1].mnt.join("counter");
            let cur = read_counter(&p)?;
            std::fs::write(&p, (cur + 1).to_be_bytes())?;
        }
        // ---- the deliberate breaks of the tree.
        match brk.as_deref() {
            Some("data") => {
                let (rel, _) = ledger
                    .expected()
                    .into_iter()
                    .find(|(_, id)| id.is_some())
                    .context("nothing to corrupt")?;
                let p = clients[0].mnt.join(&rel);
                let mut b = std::fs::read(&p)?;
                let mid = b.len() / 2;
                b[mid] ^= 0x5a;
                std::fs::write(&p, b)?;
                eprintln!("    {NAME}: BREAK data: flipped byte {mid} of {rel}");
            }
            Some("ledger") => {
                let (rel, _) = ledger
                    .expected()
                    .into_iter()
                    .find(|(_, id)| id.is_some())
                    .context("nothing to remove")?;
                std::fs::remove_file(clients[0].mnt.join(&rel))?;
                eprintln!("    {NAME}: BREAK ledger: removed {rel} behind the ledger");
            }
            Some("enoent") => {
                let (rel, _) = ledger
                    .expected()
                    .into_iter()
                    .find(|(rel, id)| {
                        id.is_some() && matches!(ledger.ownership(rel), Ownership::Owned { .. })
                    })
                    .context("no name the writer certainly owns")?;
                let forged = std::io::Error::from_raw_os_error(libc::ENOENT);
                anyhow::ensure!(
                    errnos.record_notfound(&ledger, "a", "rename", &rel, &forged, Instant::now()),
                    "the forged ENOENT of {rel} was not recorded"
                );
                eprintln!("    {NAME}: BREAK enoent: a forged ENOENT of a rename of {rel}");
            }
            Some("errno") => {
                errnos.record(
                    "a",
                    "forged",
                    &std::io::Error::from_raw_os_error(libc::EBADF),
                    Instant::now(),
                    OpCtx::default(),
                );
            }
            Some(b @ ("latency" | "latency-mild")) => {
                // Bounds no host lag explains: in the future, so no probe
                // sample falls in their window.
                let forged = if b == "latency" {
                    Duration::from_secs(30)
                } else {
                    Duration::from_millis(2_500)
                };
                crate::hostlag::latency_bound(
                    &format!("a forged {forged:?} write"),
                    Instant::now() + Duration::from_secs(3_600),
                    forged,
                    Duration::ZERO,
                    Duration::from_secs(1),
                )?;
            }
            Some("stall") => {
                let hang = crate::verify::hang_bound();
                anyhow::ensure!(
                    crate::verify::enabled(),
                    "break=stall needs CONSTELLATION_HARNESS_VERIFY=1 (the hang watch)"
                );
                // On a's daemon only (both share the path), whole.
                let pid = clients[0].pid().context("a is not mounted")?;
                let tmp = stall_file.with_extension("tmp");
                std::fs::write(
                    &tmp,
                    format!(
                        "{} once pid={pid}",
                        (hang + Duration::from_secs(20)).as_millis()
                    ),
                )?;
                std::fs::rename(&tmp, &stall_file)?;
                let p = clients[0].mnt.join("stalled");
                let t = Instant::now();
                let r = std::thread::scope(|s| {
                    s.spawn(|| {
                        std::fs::write(&p, b"stalled")
                            .and_then(|_| std::fs::File::open(&p)?.sync_all())
                    })
                    .join()
                    .expect("stalled writer")
                });
                eprintln!(
                    "    {NAME}: BREAK stall: the held write returned {r:?} after {:?}",
                    t.elapsed()
                );
                let _ = std::fs::remove_file(&stall_file);
                // The hang watch reports it (the scenario ends here: the
                // mount was aborted).
                return Ok(());
            }
            _ => {}
        }
        // ---- quiescence (no op answered in doubt can still land), then
        // convergence, then every check.
        crate::verify::wait_quiescent(
            &clients.iter().collect::<Vec<_>>(),
            crate::load::scaled(Duration::from_secs(120)),
        )?;
        let deadline = Instant::now() + Duration::from_secs(90);
        let reports = loop {
            let reps: Vec<_> = clients.iter().map(|c| ledger.check(&c.mnt)).collect();
            let bad: Vec<String> = reps
                .iter()
                .zip(&clients)
                .flat_map(|(r, c)| {
                    r.violations(0)
                        .into_iter()
                        .map(move |v| format!("{}: {v}", c.name))
                })
                .collect();
            if bad.is_empty() || Instant::now() >= deadline {
                break reps;
            }
            std::thread::sleep(Duration::from_millis(500));
        };
        let refused: u64 = clients
            .iter()
            .map(|c| {
                crate::verify::counters(c)["replay_conflicts"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum();
        for (c, r) in clients.iter().zip(&reports) {
            eprintln!(
                "    {NAME}: ledger on {}: {} ops, {} files, {} intact, {} in conflict copies, \
                 {} undone unlinks, {} late landings of ops in doubt; counters {}",
                c.name,
                r.acked_ops,
                r.files,
                r.intact,
                r.in_conflict_copy.len(),
                r.resurrected.len(),
                r.late_landed.len(),
                crate::verify::counters(c)
            );
        }
        let bad: Vec<String> = reports
            .iter()
            .zip(&clients)
            .flat_map(|(r, c)| {
                r.violations(refused)
                    .into_iter()
                    .map(move |v| format!("{}: {v}", c.name))
            })
            .collect();
        anyhow::ensure!(bad.is_empty(), "acknowledged writes: {}", bad.join("; "));
        // A profile that pauses the node mounted first (the root) must
        // have deposed it: the other node sealed its epoch and took the
        // root over through its backup. Without a takeover the cell
        // proved nothing about one (`load-probe` mounts `a` first, so
        // `pause:...@b` never touches the root there).
        if crate::load::pauses_node(&clients[0].name) {
            let takeovers: u64 = clients[1..]
                .iter()
                .map(|c| {
                    crate::verify::counters(c)["backup_takeovers"]
                        .as_u64()
                        .unwrap_or(0)
                })
                .sum();
            anyhow::ensure!(
                takeovers >= 1,
                "the pause did not depose the root: {} held the root, no other node took it \
                 over through its backup (backup_takeovers {takeovers}): the cell proved \
                 nothing",
                clients[0].name
            );
            eprintln!("    {NAME}: the root was taken over ({takeovers} backup takeover(s))");
        }
        let got = read_counter(&clients[0].mnt.join("counter"))?;
        eprintln!("    {NAME}: counter {got}: {done} increments acknowledged, {doubt} in doubt");
        anyhow::ensure!(
            got >= done && got <= done + doubt,
            "lock exclusion: the counter is {got}, {done} increments were acknowledged \
             and {doubt} were in doubt"
        );
        if doubt == 0 {
            crate::verify::counter_exact("the flock'ed counter", done, got)?;
        }
        // ---- after a remount, and fsck.
        clients[0].unmount()?;
        clients[0].mount()?;
        let again = ledger.check(&clients[0].mnt);
        anyhow::ensure!(
            again.violations(refused).is_empty(),
            "acknowledged writes after a's remount: {}",
            again.violations(refused).join("; ")
        );
        let fsck = clients[0].fsck(false)?;
        anyhow::ensure!(
            fsck.status.success(),
            "fsck found problems ({:?}): {}{}",
            fsck.status.code(),
            String::from_utf8_lossy(&fsck.stdout),
            String::from_utf8_lossy(&fsck.stderr)
        );
        // ---- errors the workload saw.
        let refs: Vec<&Client> = clients.iter().collect();
        let ev = Evidence::from_clients(&refs, super::m9::client_deadline());
        let rep = errnos.judge(&ev);
        for a in &rep.allowed {
            eprintln!("    {NAME}: allowed error: {a}");
        }
        eprintln!("    {NAME}: errors by class {:?}", rep.counts);
        anyhow::ensure!(
            rep.bugs.is_empty(),
            "errors outside the documented set: {}",
            rep.bugs.join("; ")
        );
        // ---- latency, judged against the host.
        let takeovers: u64 = clients
            .iter()
            .map(|c| {
                crate::verify::counters(c)["backup_takeovers"]
                    .as_u64()
                    .unwrap_or(0)
            })
            .sum();
        let logged = clients
            .iter()
            .flat_map(super::delegroot::floor_waits_logged)
            .max()
            .map_or(Duration::ZERO, Duration::from_millis);
        let timings = crate::timings::Timings::from_pairs(&{
            let mut env = vec![("CONSTELLATION_LEASE_TTL_MS", super::m9::TTL_MS.to_string())];
            env.extend(extra.iter().cloned());
            env
        });
        let (protocol, bound) = write_bound(&timings, takeovers, logged)?;
        eprintln!(
            "    {NAME}: write+fsync bound {bound:?} (protocol {protocol:?}: {takeovers} backup \
             takeover(s), quarantine logged {logged:?} of at most {:?})",
            timings.ack_floor_wait(true)
        );
        for (c, (took, at, _)) in clients.iter().zip(&slowest) {
            crate::hostlag::latency_bound(
                &format!("{}'s slowest write+fsync", c.name),
                *at,
                *took,
                protocol,
                bound,
            )?;
        }
        Ok(())
    })();
    let ids: Vec<u64> = clients.iter().filter_map(|c| node_id(c).ok()).collect();
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result?;
    if brk.as_deref() == Some("stall") {
        return Ok(());
    }
    // ---- logs (the daemons have exited) and the bucket.
    match brk.as_deref() {
        Some("token") => {
            let texts: Vec<_> = clients
                .iter()
                .flat_map(|c| c.log_files().into_iter().map(move |p| (c.name.clone(), p)))
                .filter_map(|(n, p)| crate::verify::read_log(&n, &p).ok())
                .collect();
            let (early, late) = crate::verify::conflicting_grants(&texts)
                .context("no conflicting grants in the logs to forge a token violation with")?;
            // A write admitted under the earlier grant that committed
            // after the later one: what F1 would leave in the logs.
            let line = format!(
                "\n2026-01-01T00:00:00.000000Z DEBUG constellation::token_exec: tagged op landed minter={} seq={} host_us={}\n",
                early.minter,
                early.seq,
                late.at_us + 500_000
            );
            append(&clients[0], &line)?;
            eprintln!(
                "    {NAME}: BREAK token: forged a landed write of grant {}/{} after {}/{}",
                early.minter, early.seq, late.minter, late.seq
            );
        }
        Some("split-brain") => {
            let other = ids
                .iter()
                .copied()
                .find(|id| Some(*id) != ids.first().copied())
                .unwrap_or(99);
            let line = format!(
                "\n2026-01-01T00:00:00.000000Z  INFO constellation_authority::core::jobs: acquired the lease node={other} epoch={epoch0} takeover=true marker=true\n",
                epoch0 = epoch_of_logs(&clients[0])?
            );
            append(&clients[1], &line)?;
            eprintln!("    {NAME}: BREAK split-brain: forged node {other} acquiring a's epoch");
        }
        Some("log-drop") => {
            // What a daemon's log writer leaves in its log when its sink
            // stalled longer than its queue held.
            let line =
                "\n2026-01-01T00:00:00.000000Z  WARN constellation::log: log lines dropped: \
                        the log sink did not keep up dropped_lines=3 dropped_bytes=600 \
                        first_dropped_unix_ms=1767225600000 queue_limit_bytes=16777216\n";
            append(&clients[0], line)?;
            eprintln!("    {NAME}: BREAK log-drop: forged a log lines dropped line in a's log");
        }
        Some("log-missing") => {
            for p in clients[1].log_files() {
                std::fs::remove_file(&p)?;
            }
            eprintln!("    {NAME}: BREAK log-missing: removed b's logs");
        }
        _ => {}
    }
    let refs: Vec<&Client> = clients.iter().collect();
    let counts = crate::verify::check_logs(&refs)?;
    // The counter phase guarantees lock grants, tagged ops (checked and
    // landed) and a lease acquisition: none parsed is a blind checker,
    // not a clean run.
    anyhow::ensure!(
        counts.grants > 0 && counts.tagged > 0 && counts.landed > 0 && counts.acquired > 0,
        "the log checkers read nothing they must have seen: {counts:?}"
    );
    let prefix = clients[0]
        .backend
        .strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET))
        .context("backend outside the harness bucket")?
        .to_string();
    let segments = crate::verify::s3_log_check(&env.direct_endpoint, &prefix)?;
    anyhow::ensure!(
        segments.decoded > 0,
        "the bucket log checker read nothing: {segments:?}"
    );
    eprintln!(
        "    {NAME}: bucket log: {} segment(s), epochs monotone, one node per epoch, rids once",
        segments.decoded
    );
    Ok(())
}

/// The epoch a's log says it acquired first.
fn epoch_of_logs(c: &Client) -> Result<u64> {
    for p in c.log_files() {
        let text = std::fs::read_to_string(&p).unwrap_or_default();
        for l in text.lines().filter(|l| l.contains("acquired the lease")) {
            if let Some(e) = crate::verify::field(&crate::verify::strip_ansi(l), "epoch") {
                return e.parse().context("epoch");
            }
        }
    }
    bail!("{} never acquired the lease", c.name)
}

fn append(c: &Client, line: &str) -> Result<()> {
    use std::io::Write;
    let p = c.log_files().pop().context("no log")?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&p)?
        .write_all(line.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timings::Timings;

    /// The cell's daemons: the M9 lease TTL and `CONSTELLATION_LOCK_TTL_MS`
    /// 5000 (so the delegation TTL is 5 s too).
    fn cell() -> Timings {
        Timings::from_pairs(&[
            ("CONSTELLATION_LEASE_TTL_MS", "20000"),
            ("CONSTELLATION_LOCK_TTL_MS", "5000"),
        ])
    }

    /// F2 of the 2026-10-24 validation: a takeover's accepted 14.5 s
    /// quarantine (plan 43 G5) made a 16.5 s and an 18.6 s write fail a
    /// flat 10 s bound.
    #[test]
    fn a_takeover_cell_bound_includes_the_accepted_quarantine() {
        let t = cell();
        assert_eq!(t.ack_floor_wait(true), Duration::from_millis(14_500));
        let (protocol, bound) = write_bound(&t, 1, Duration::from_millis(14_500)).unwrap();
        assert_eq!(protocol, Duration::from_millis(16_000));
        assert_eq!(bound, Duration::from_millis(26_000));
        assert!(Duration::from_millis(18_588) <= bound);
        assert!(Duration::from_millis(16_462) <= bound);
        // A quarantine the cap shortened is only what was logged.
        let (protocol, bound) = write_bound(&t, 1, Duration::from_millis(8_500)).unwrap();
        assert_eq!(protocol, Duration::from_millis(10_000));
        assert_eq!(bound, Duration::from_millis(20_000));
    }

    /// Without a takeover the protocol has no part, as before.
    #[test]
    fn a_cell_without_a_takeover_keeps_the_flat_bound() {
        let (protocol, bound) = write_bound(&cell(), 0, Duration::ZERO).unwrap();
        assert_eq!(protocol, Duration::ZERO);
        assert_eq!(bound, WRITE_BOUND);
    }

    /// A wait past the configured quarantine is not absorbed.
    #[test]
    fn a_quarantine_past_the_configured_one_is_an_error() {
        assert!(write_bound(&cell(), 1, Duration::from_millis(14_501)).is_err());
    }
}
