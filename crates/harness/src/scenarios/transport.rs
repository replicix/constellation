//! Plan 38 §6 / §3(e): the FUSE transport the mount negotiated, and what
//! that means for `daemon --upgrade`.
//!
//! `transport-detach-refused` is **one scenario with two expected
//! outcomes, keyed on the transport the mount actually negotiated** — not
//! two scenarios, and not a scenario that skips off a ring kernel. It
//! always asks for the ladder — `CONSTELLATION_FUSE_TRANSPORT=auto`, which
//! since the 2026-10-05 decision puts the harness's cluster-lock mounts
//! (P2P is on) on the ring too — and then reads `node.status` to learn
//! what it got:
//!
//! - **`uring`** (a 6.14+ kernel with `fuse.enable_uring=Y`, an
//!   `io-uring`-feature build, and a sandbox that permits
//!   `io_uring_setup(2)`): `daemon --upgrade` must be **refused**, with an
//!   error naming the transport, and refused *without cost* — the load
//!   loses no request, the mount keeps serving, the daemon keeps its pid
//!   and generation, and `node.status` still reports `uring`. Plan 38 Z0a
//!   established why: a connection whose ring queues became ready can
//!   never be served over `/dev/fuse` again, and re-registering orphans
//!   every in-flight request (its caller unkillable until a fusectl
//!   abort). Tearing the mount down and remounting is the only upgrade
//!   path for such a mount.
//! - **`dev_fuse`** (everything else — and the dev host, where
//!   `fuse.enable_uring=N`, is exactly that): the ladder fell back, so
//!   the session is an ordinary `/dev/fuse` one and the handover must
//!   **succeed**, pid and all. That half is the fallback's own gate: a
//!   mount that asked for `auto` and got `dev_fuse` must be
//!   indistinguishable from one that asked for `dev-fuse`, upgradability
//!   included.
//!
//! Either way the load — a writer appending through a held descriptor, a
//! creator, and a reader re-reading through a held descriptor, the same
//! three `upgrade-under-load` uses — must see zero errors and every byte
//! it wrote must be there afterwards, before and after a remount.
//!
//! With `CONSTELLATION_FUSE_EXPECT_URING=1` (set by the transport matrix
//! lane on a host whose kernel grants the ring) the `dev_fuse` outcome is
//! a failure instead: there the ladder must not have fallen back.

use super::handover::{generation, start_load, try_upgrade, verify_load, Watcher};
use super::{setup, ts};
use crate::client::Client;
use crate::s3env::{S3Env, BUCKET};
use anyhow::{ensure, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

/// What the ring scenarios ask for: plan 38 §2.4's ladder for every plain
/// mount, cluster-lock ones included (`auto`, the shipped default; since
/// the 2026-10-05 decision it no longer keeps the harness's cluster-lock
/// mounts on `/dev/fuse`). Set on the client rather than inherited, so a
/// scenario asks the same question on every leg of the transport matrix
/// lane.
pub(crate) const RING_POLICY: &str = "auto";

/// A client whose mount asks for the ladder ([`RING_POLICY`]).
fn auto_client(env: &S3Env, root: &Path, prefix: &str) -> Result<Client> {
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root, "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", RING_POLICY);
    c.fs_create()?;
    c.mount()?;
    Ok(c)
}

/// The fallback reasons `node.status` may name (plan 38 §2.4, Z2b).
pub(crate) const FALLBACK_REASONS: &[&str] = &[
    "no_io_uring_feature",
    "kernel_not_offered",
    "handover_capable",
    "ring_setup_failed",
];

/// The Z2b review's gap, closed on a real daemon: a mount that asked for
/// the ring and got `dev_fuse` reports **why** in
/// `node.status.fuse.mounts[0].last_fallback` — one of
/// [`FALLBACK_REASONS`], `expected` when given — and the process-wide
/// `transport_fallbacks` counts exactly that one fallback (the daemon has
/// one mount). Returns the reason.
pub(crate) fn fallback_reported(c: &Client, expected: Option<&str>) -> Result<String> {
    let status = c.control_status()?;
    let fuse = &status["fuse"];
    let mount = &fuse["mounts"][0];
    ensure!(
        mount["transport"] == "dev_fuse",
        "fuse.mounts[0] is not a fallback: {mount}"
    );
    let reason = mount["last_fallback"]["reason"]
        .as_str()
        .with_context(|| {
            format!("a dev_fuse mount that asked for the ring has no last_fallback: {mount}")
        })?
        .to_string();
    ensure!(
        FALLBACK_REASONS.contains(&reason.as_str()),
        "unknown fallback reason {reason:?}: {mount}"
    );
    if let Some(expected) = expected {
        ensure!(
            reason == expected,
            "the fallback says {reason}, expected {expected}: {mount}"
        );
    }
    let counted: Vec<(String, u64)> = fuse["transport_fallbacks"]
        .as_array()
        .context("node.status.fuse has no transport_fallbacks")?
        .iter()
        .map(|f| {
            (
                f["reason"].as_str().unwrap_or_default().to_string(),
                f["count"].as_u64().unwrap_or(0),
            )
        })
        .collect();
    let total: u64 = counted.iter().map(|(_, n)| n).sum();
    ensure!(
        total == 1 && counted.iter().any(|(r, n)| *r == reason && *n == 1),
        "transport_fallbacks must count exactly this one {reason} fallback: {counted:?}"
    );
    Ok(reason)
}

/// Whether `transport` (as `node.status` names it) is a ring: `uring`,
/// or `uring_zc` where the kernel offers buffer pools (7.3+) and the
/// daemon has `CAP_SYS_ADMIN` (plan 38 Z4) — the same rung of the
/// ladder for every scenario here, which asks for the ring, not for
/// zero-copy.
pub(crate) fn is_ring(transport: &str) -> bool {
    matches!(transport, "uring" | "uring_zc")
}

/// The transport `node.status` reports for the daemon's one mount
/// (`dev_fuse` / `uring` / `uring_zc`, plan 38 §5).
pub(super) fn negotiated_transport(c: &Client) -> Result<String> {
    let status = c.control_status()?;
    let mounts = status["mounts"]
        .as_array()
        .context("node.status has no mounts array")?;
    ensure!(
        mounts.len() == 1,
        "expected one mount, got {}",
        mounts.len()
    );
    mounts[0]["transport"]
        .as_str()
        .map(str::to_string)
        .with_context(|| format!("mount has no transport field: {}", mounts[0]))
}

pub fn transport_detach_refused(seed: u64) -> Result<()> {
    let (env, root) = setup("transport-detach")?;
    let _proxy = env.s3_proxy()?;
    let mut c = auto_client(&env, root.path(), &format!("transport-detach-{}", ts()))?;
    let result = (|| -> Result<()> {
        let negotiated = negotiated_transport(&c)?;
        let ring = negotiated != "dev_fuse";
        // The lane sets this on a host that grants the ring
        // (tests/transport-matrix.sh): falling back there means the build
        // or the ladder lost the ring, and the `auto` leg would silently
        // be a second `dev-fuse` leg.
        if std::env::var("CONSTELLATION_FUSE_EXPECT_URING").as_deref() == Ok("1") {
            ensure!(
                ring,
                "this host grants FUSE-over-io_uring (CONSTELLATION_FUSE_EXPECT_URING=1) but \
                 the auto mount negotiated {negotiated}: is the binary built with \
                 --features constellation-frontend-fuse/io-uring?\n{}",
                c.tail_log()
            );
        }
        eprintln!(
            "transport-detach-refused: the mount asked for {RING_POLICY} and negotiated \
             {negotiated}; expecting the handover to be {}",
            if ring { "refused" } else { "served" }
        );
        // Plan 38 Z2b review: a fallback says why, and counts once.
        let first_reason = if ring {
            None
        } else {
            let reason = fallback_reported(&c, None)?;
            eprintln!("transport-detach-refused: fell back for {reason}");
            Some(reason)
        };
        let pid = c.pid().context("no daemon pid")?;
        let before = generation(&c)?;
        let watcher = Watcher::start(&c.mnt)?;
        let load = start_load(&c.mnt, seed)?;
        std::thread::sleep(Duration::from_millis(1500));
        let ops_before = load.ops.load(Ordering::SeqCst);

        let out = try_upgrade(&c)?;
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(1500));
        if ring {
            ensure!(
                !out.status.success(),
                "daemon --upgrade must refuse a {negotiated} session, it said: {said}\n{}",
                c.tail_log()
            );
            // The refusal has to name the transport: "it did not work" is
            // not an answer an operator can act on, and the action here
            // (unmount and remount, or mount with --fuse-transport
            // dev-fuse) follows from *which* transport it is.
            // Per mount, not just the generic "io_uring" of the message.
            let names = format!("served over {negotiated}");
            ensure!(
                said.contains(&names),
                "the refusal does not name the transport ({names:?}): {said}"
            );
            ensure!(
                generation(&c)? == before,
                "the generation advanced although the upgrade was refused"
            );
            ensure!(
                negotiated_transport(&c)? == negotiated,
                "node.status no longer reports {negotiated} after the refusal"
            );
        } else {
            ensure!(
                out.status.success() && said.contains("upgraded: pid"),
                "the ladder fell back to dev_fuse, so daemon --upgrade must serve; it said: \
                 {said}\n{}",
                c.tail_log()
            );
            ensure!(
                generation(&c)? == before + 1,
                "the daemon's generation did not advance"
            );
            ensure!(
                negotiated_transport(&c)? == "dev_fuse",
                "a resumed session must be dev_fuse"
            );
            // The resumed session (a new process, pinned to /dev/fuse)
            // keeps the reason the first mount had, rather than reporting
            // the pin (the Z2b review's finding).
            let again = fallback_reported(&c, first_reason.as_deref())?;
            eprintln!("transport-detach-refused: the resumed mount still says {again}");
        }
        // Both outcomes: the mount never stopped serving and the daemon is
        // the same process (a refused upgrade must not restart anything,
        // and a served one resumes under the same pid).
        ensure!(c.pid() == Some(pid), "the daemon's pid changed");
        ensure!(
            load.ops.load(Ordering::SeqCst) > ops_before,
            "the load made no progress after the upgrade attempt"
        );

        load.stop.store(true, Ordering::SeqCst);
        let mut counts = Vec::new();
        for t in load.threads {
            counts.push(
                t.join()
                    .map_err(|_| anyhow::anyhow!("a load thread panicked"))??,
            );
        }
        let gaps = watcher.finish();
        let errors = load.errors.lock().unwrap().clone();
        eprintln!(
            "transport-detach-refused: {} ops, longest syscall {} ms; appended {} records, \
             created {} files, {} reads",
            load.ops.load(Ordering::SeqCst),
            load.max_stall_us.load(Ordering::SeqCst) / 1000,
            counts[0],
            counts[1],
            counts[2]
        );
        ensure!(errors.is_empty(), "the load saw errors: {errors:?}");
        ensure!(gaps.is_empty(), "the mount showed gaps: {gaps:?}");
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after the upgrade attempt")?;
        c.unmount()?;
        c.mount()?;
        verify_load(&c.mnt, seed, counts[0], counts[1]).context("after a remount")?;
        Ok(())
    })();
    let _ = c.unmount();
    result
}

// ------------------------------------------------------------------------
// Plan 38 Z2a: every downgrade of §2.4's ladder, injected for real.
//
// Each scenario asks for `auto`, provokes one rung's refusal, and requires
// the same three things: the mount serves (a seeded workload, read back),
// `node.status` reports `dev_fuse`, and the downgrade is logged **exactly
// once** over the daemon's whole life — a busy mount must not log a
// fallback per request. The ring-only ones (`requires: fuse-uring`) first
// mount without the fault and require `uring`, so the fallback they then
// see is the fault's and not the host's.

/// A seeded workload a serving mount answers: directories, files of mixed
/// sizes written, fsynced, read back byte for byte, renamed and partly
/// unlinked.
fn serve_check(mnt: &Path, seed: u64, round: &str) -> Result<()> {
    use std::io::Write;
    let dir = mnt.join(format!("serve-{round}"));
    std::fs::create_dir_all(dir.join("sub"))?;
    let mut x = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut files = Vec::new();
    for i in 0..24 {
        let len = (next() % (384 * 1024)) as usize + 1;
        let fill = next() as u8;
        let content: Vec<u8> = (0..len).map(|j| fill.wrapping_add(j as u8)).collect();
        let path = dir.join(format!("f{i:02}"));
        let mut f = std::fs::File::create(&path)?;
        f.write_all(&content)?;
        if i % 4 == 0 {
            f.sync_all()?;
        }
        files.push((path, content));
    }
    for (path, content) in &files {
        let got = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        ensure!(&got == content, "{} read back wrong", path.display());
    }
    std::fs::rename(&files[0].0, dir.join("sub/renamed"))?;
    ensure!(std::fs::read(dir.join("sub/renamed"))? == files[0].1);
    for (path, _) in files.iter().skip(1).step_by(3) {
        std::fs::remove_file(path)?;
    }
    let left = std::fs::read_dir(&dir)?.count();
    ensure!(
        left == 1 + 23 - 8,
        "{left} entries left in {}",
        dir.display()
    );
    Ok(())
}

/// The daemon's downgrade lines (plan 38 §2.4): fuser's "io_uring requested
/// but … using /dev/fuse" (a refused setup, reservation, size, or a kernel
/// that did not offer the ring), the frontend's "io_uring requested but the
/// kernel refused to register the rings" (a refused registration), and the
/// build rung's "this build has no io-uring feature". Not the daemon's
/// per-mount "FUSE transport (fell back)" record (plan 38 Z2b): every mount
/// logs that one, ring or not, and its `detail` may repeat a downgrade's
/// words without being a second report of it.
fn downgrades(c: &Client) -> Vec<String> {
    c.log_text()
        .lines()
        .filter(|l| {
            (l.contains("io_uring requested but")
                || l.contains("this build has no io-uring feature"))
                && !l.contains("FUSE transport (fell back)")
        })
        .map(str::to_string)
        .collect()
}

/// Exactly one downgrade line, which names `why` (when given).
fn logged_once(c: &Client, why: &[&str]) -> Result<String> {
    let mut lines = downgrades(c);
    ensure!(
        lines.len() == 1,
        "expected the downgrade logged exactly once, found {}:\n{}",
        lines.len(),
        lines.join("\n")
    );
    for w in why {
        ensure!(
            lines[0].contains(w),
            "the downgrade does not say {w:?}: {}",
            lines[0]
        );
    }
    Ok(lines.remove(0))
}

/// Nothing alarming in the daemon's log: no panic, no ERROR from the ring.
fn no_alarms(c: &Client) -> Result<()> {
    let log = c.log_text();
    for bad in ["panicked", "leaking", "FUSE session ended with an error"] {
        let hits: Vec<&str> = log.lines().filter(|l| l.contains(bad)).collect();
        ensure!(
            hits.is_empty(),
            "the daemon logged {bad:?}:\n{}",
            hits.join("\n")
        );
    }
    Ok(())
}

fn ring_client(env: &S3Env, root: &Path, name: &str) -> Result<Client> {
    let backend = format!("s3://{BUCKET}/{name}-{}", ts());
    let c = Client::new(root, "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", RING_POLICY);
    c.fs_create()?;
    Ok(c)
}

/// The control half of a ring-only scenario: without the fault, `auto`
/// gets the ring here (the scenario's `requires` said it would).
fn control_round(c: &mut Client, seed: u64) -> Result<()> {
    c.mount()?;
    let negotiated = negotiated_transport(c)?;
    ensure!(
        is_ring(&negotiated),
        "without the fault this host and binary must grant the ring, got {negotiated}\n{}",
        c.tail_log()
    );
    ensure!(downgrades(c).is_empty(), "a downgrade without a fault");
    serve_check(&c.mnt, seed, "control")?;
    c.unmount()
}

/// The faulted half: the mount serves on `dev_fuse` and says why once.
fn fallback_round(c: &mut Client, seed: u64, why: &[&str]) -> Result<()> {
    c.mount_within(Duration::from_secs(60))
        .context("a mount whose ring was refused must still come up (on /dev/fuse)")?;
    let negotiated = negotiated_transport(c)?;
    ensure!(
        negotiated == "dev_fuse",
        "the fallback negotiated {negotiated}\n{}",
        c.tail_log()
    );
    serve_check(&c.mnt, seed, "fallback")?;
    serve_check(&c.mnt, seed + 1, "fallback-again")?;
    let line = logged_once(c, why)?;
    let reason = fallback_reported(c, None)?;
    eprintln!(
        "    fell back to dev_fuse ({reason}), logged once: {}",
        line.trim()
    );
    no_alarms(c)?;
    let pid = c.pid();
    c.unmount()?;
    ensure!(pid.is_some(), "the daemon was running");
    // Still exactly once: neither the workload nor the unmount logged more.
    logged_once(c, why)?;
    Ok(())
}

/// Plan 38 §2.4/§6: the kernel refuses the rings' registration after the
/// `FUSE_INIT` reply committed the connection to them. Nothing can serve
/// such a connection, so the frontend must mount again over `/dev/fuse` —
/// not hang, not fail the mount. The refusal is the kernel's own, provoked
/// with `CONSTELLATION_FUSE_URING_FAULT=malformed-register` (every
/// REGISTER names one iovec instead of two).
pub fn transport_refused_registration(seed: u64) -> Result<()> {
    let (env, root) = setup("transport-refused-registration")?;
    let _proxy = env.s3_proxy()?;
    let mut c = ring_client(&env, root.path(), "transport-refused-registration")?;
    let result = (|| -> Result<()> {
        control_round(&mut c, seed)?;
        c.set_env("CONSTELLATION_FUSE_URING_FAULT", "malformed-register");
        fallback_round(&mut c, seed, &["refused to register the rings"])
    })();
    let _ = c.unmount();
    result
}

/// Plan 38 §2.4/§8: `io_uring_setup(2)` refused by a seccomp policy (the
/// plan 37 container case). Runs everywhere and must pass everywhere: on a
/// host and binary that would grant the ring the refusal is the filter's
/// `EPERM`, logged once; anywhere else the ladder stops at an earlier rung
/// (the kernel, the build), which must be logged once just the same.
pub fn transport_seccomp_denied(seed: u64) -> Result<()> {
    let (env, root) = setup("transport-seccomp-denied")?;
    let _proxy = env.s3_proxy()?;
    let mut c = ring_client(&env, root.path(), "transport-seccomp-denied")?.with_io_uring_denied();
    let why: &[&str] = match crate::suites::unavailable(crate::suites::FUSE_URING) {
        None => &["io_uring_setup failed", "Operation not permitted"],
        Some(reason) => {
            eprintln!("    no ring to deny here ({reason}): asserting the earlier rung's fallback");
            &[]
        }
    };
    let result = fallback_round(&mut c, seed, why);
    let _ = c.unmount();
    result
}

/// Plan 38 §2.4/§6: the ring's buffer reservation refused (`ENOMEM`). The
/// daemon's address space is limited (`RLIMIT_AS`) to what it measurably
/// needs on `/dev/fuse` plus a margin of at most half of what the session's
/// rings reserve **together** (`queues x depth x 16 MiB`, deepened to 64
/// entries per queue so that the gap is wide on any CPU count). Each ring
/// maps only its own share of the queues, which may well fit in the margin
/// on its own; but the session maps every ring's before it serves, so the
/// mapping that crosses the limit fails, the mount falls back, and nothing
/// else in the daemon notices the limit.
///
/// Zero-copy queues are off for that round (plan 38 Z4a): the arithmetic is
/// the plain ring's, 16 MiB of `max_write` per entry. A daemon that may use
/// zero-copy (root on 7.3+) lowers `max_write` to one request's pages
/// (1 MiB) and maps buffer pools besides, so the same limit would cut
/// somewhere else. That is the second round, run where the daemon does get
/// `uring_zc` (root, a 7.3+ kernel; anywhere else it says why it is
/// skipped): zero-copy `auto`, the limit placed so that the ring's own
/// entries fit and its buffer pools -- which fuser maps last for exactly
/// this reason -- do not. The rung that gives way is zero-copy, not the
/// ring: the mount comes up on plain `uring`, with no fallback recorded and
/// the reason logged once.
pub fn transport_enomem_ring(seed: u64) -> Result<()> {
    const DEPTH: u64 = 64;
    let (env, root) = setup("transport-enomem-ring")?;
    let _proxy = env.s3_proxy()?;
    let mut c = ring_client(&env, root.path(), "transport-enomem-ring")?
        .with_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH", &DEPTH.to_string())
        .with_env("CONSTELLATION_FUSE_URING_ZERO_COPY", "off");
    let result = (|| -> Result<()> {
        control_round(&mut c, seed)?;
        // What the daemon needs without a ring, under the same workload.
        c.set_env("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse");
        c.mount()?;
        serve_check(&c.mnt, seed, "probe")?;
        let peak = vm_peak(c.pid().context("no daemon pid")?)?;
        c.unmount()?;
        c.set_env("CONSTELLATION_FUSE_TRANSPORT", RING_POLICY);
        let cpus = possible_cpus()?;
        let reserved = cpus * DEPTH * (16 << 20);
        let margin = (reserved / 2).min(4 << 30);
        let limit = peak + margin;
        eprintln!(
            "    daemon peak {} MiB on /dev/fuse; the rings reserve {} MiB together \
             ({cpus} queues x {DEPTH} entries x 16 MiB, each ring mapping its own share); \
             margin {} MiB, RLIMIT_AS {} MiB",
            peak >> 20,
            reserved >> 20,
            margin >> 20,
            limit >> 20
        );
        c.set_address_space_limit(Some(limit));
        fallback_round(&mut c, seed, &["ring buffers failed", "ENOMEM"])?;
        c.set_address_space_limit(None);
        enomem_zero_copy_round(&mut c, seed, peak, DEPTH, cpus)
    })();
    c.set_address_space_limit(None);
    let _ = c.unmount();
    result
}

/// [`transport_enomem_ring`]'s second round: the address-space limit cuts
/// the buffer pools of zero-copy `auto`, and only them.
fn enomem_zero_copy_round(
    c: &mut Client,
    seed: u64,
    peak: u64,
    depth: u64,
    cpus: u64,
) -> Result<()> {
    const WHY: &str = "io_uring zero-copy unavailable";
    let zero_copy_lines = |c: &Client| {
        c.log_text()
            .lines()
            .filter(|l| l.contains(WHY))
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    c.set_env("CONSTELLATION_FUSE_URING_ZERO_COPY", "auto");
    c.mount()?;
    let negotiated = negotiated_transport(c)?;
    c.unmount()?;
    if negotiated != "uring_zc" {
        let why = zero_copy_lines(c);
        eprintln!(
            "    zero-copy round skipped: the daemon negotiated {negotiated} with zero-copy auto \
             (needs root -- CAP_SYS_ADMIN -- and a kernel with io_uring buffer pools, 7.3+){}",
            why.first()
                .map(|l| format!("; {}", l.trim()))
                .unwrap_or_default()
        );
        return Ok(());
    }
    ensure!(
        zero_copy_lines(c).is_empty(),
        "zero-copy refused without a limit"
    );
    // A pool buffer, and an entry's payload, is one request's pages
    let pages: u64 = std::fs::read_to_string("/proc/sys/fs/fuse/max_pages_limit")?
        .trim()
        .parse()?;
    let page = 4096;
    let buf = pages.min(4096) * page;
    let entries = cpus * depth * (buf + page);
    let pools = cpus * depth * buf;
    let limit = peak + entries + pools / 2;
    eprintln!(
        "    zero-copy auto: entries {} MiB, buffer pools {} MiB ({cpus} queues x {depth} x {} \
         KiB); RLIMIT_AS {} MiB",
        entries >> 20,
        pools >> 20,
        buf >> 10,
        limit >> 20
    );
    let before = downgrades(c).len();
    c.set_address_space_limit(Some(limit));
    c.mount_within(Duration::from_secs(60))
        .context("a mount whose buffer pools were refused must still come up")?;
    let negotiated = negotiated_transport(c)?;
    ensure!(
        negotiated == "uring",
        "the pools refused, the mount negotiated {negotiated} rather than plain uring\n{}",
        c.tail_log()
    );
    let mount = fuse_mount(c)?;
    ensure!(
        mount["last_fallback"].is_null(),
        "giving up zero-copy is not a transport fallback: {mount}"
    );
    serve_check(&c.mnt, seed + 2, "zero-copy-refused")?;
    ensure!(
        downgrades(c).len() == before,
        "the ring itself was refused:\n{}",
        downgrades(c).join("\n")
    );
    let lines = zero_copy_lines(c);
    ensure!(
        lines.len() == 1 && lines[0].contains("buffer pools failed") && lines[0].contains("ENOMEM"),
        "expected the pools' ENOMEM logged exactly once:\n{}",
        lines.join("\n")
    );
    eprintln!(
        "    pools refused, plain uring, logged once: {}",
        lines[0].trim()
    );
    no_alarms(c)?;
    c.unmount()?;
    c.set_address_space_limit(None);
    Ok(())
}

fn vm_peak(pid: u32) -> Result<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmPeak:"))
        .context("no VmPeak")?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()?;
    Ok(kb * 1024)
}

/// The kernel's queue count (`fuse_uring` registers one per possible CPU).
fn possible_cpus() -> Result<u64> {
    let text = std::fs::read_to_string("/sys/devices/system/cpu/possible")?;
    let mut n = 0;
    for range in text.trim().split(',') {
        let (lo, hi) = range.split_once('-').unwrap_or((range, range));
        n += hi.parse::<u64>()? - lo.parse::<u64>()? + 1;
    }
    Ok(n)
}

/// Plan 38 §6: a fusectl abort (`/sys/fs/fuse/connections/<n>/abort`) of a
/// ring session whose entries are armed in the kernel, while a reader is
/// using the mount. `FrontendCaps::abortable` (this scenario's `FuseAbort`)
/// must stay true on the ring: the reader's next call fails at once (no
/// hang), the daemon unwinds by itself — its session ends cleanly, the view
/// closes, the process exits successfully — with nothing leaked, and the
/// mountpoint takes a fresh ring mount afterwards.
pub fn transport_abort_while_armed(seed: u64) -> Result<()> {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    let (env, root) = setup("transport-abort-while-armed")?;
    let _proxy = env.s3_proxy()?;
    let mut c = ring_client(&env, root.path(), "transport-abort-while-armed")?;
    let result = (|| -> Result<()> {
        c.mount()?;
        let negotiated = negotiated_transport(&c)?;
        ensure!(
            is_ring(&negotiated),
            "auto negotiated {negotiated} on a ring host"
        );
        serve_check(&c.mnt, seed, "armed")?;
        let conn = c
            .fuse_connection()
            .context("no fusectl connection for the mount")?;
        let abort = format!("/sys/fs/fuse/connections/{conn}/abort");
        ensure!(
            Path::new(&abort).exists(),
            "{abort} is missing (is fusectl mounted?)"
        );
        // A reader hammering the mount while the abort lands. It works
        // through a descriptor of the directory opened before the abort,
        // never the mount's path: once the daemon has unwound it unmounts
        // the dead mount, and a path would then reach the directory
        // underneath (ENOENT, or worse, an answer). The descriptor keeps
        // naming the aborted connection's directory.
        let pinned = std::fs::File::open(c.mnt.join("serve-armed"))?;
        let pinned_path = |f: &std::fs::File| {
            use std::os::fd::AsRawFd;
            PathBuf::from(format!("/proc/self/fd/{}", f.as_raw_fd()))
        };
        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let (mnt, stop) = (pinned_path(&pinned), stop.clone());
            type Failed = Option<(&'static str, std::io::Error)>;
            std::thread::spawn(move || -> (u64, Failed, Duration) {
                let mut ok = 0;
                loop {
                    let t = std::time::Instant::now();
                    let r = match std::fs::read(mnt.join("f05")) {
                        Err(e) => Err(("read", e)),
                        Ok(_) => std::fs::metadata(&mnt).map_err(|e| ("stat", e)),
                    };
                    match r {
                        Ok(_) => ok += 1,
                        Err(e) => return (ok, Some(e), t.elapsed()),
                    }
                    if stop.load(Ordering::SeqCst) {
                        return (ok, None, t.elapsed());
                    }
                }
            })
        };
        std::thread::sleep(Duration::from_millis(500));
        std::fs::write(&abort, "1").with_context(|| format!("writing {abort}"))?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !reader.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        stop.store(true, Ordering::SeqCst);
        ensure!(
            reader.is_finished(),
            "the reader is still blocked 10 s after the abort"
        );
        let (reads, failed, took) = reader
            .join()
            .map_err(|_| anyhow::anyhow!("the reader panicked"))?;
        let (call, err) = failed.context("the reader saw no error after the abort")?;
        let dead = |e: &std::io::Error| {
            matches!(
                e.raw_os_error(),
                Some(libc::ENOTCONN) | Some(libc::ECONNABORTED)
            )
        };
        // The connection's own errors, or -- for a data read only -- `EIO`:
        // since kernel 6.19 FUSE buffered reads go through iomap, which
        // leaves a folio whose read request the abort ended not uptodate,
        // and `filemap_read_folio` reports that as `EIO` whatever the
        // request's error was (the transport has no say in it).
        ensure!(
            dead(&err) || (call == "read" && err.raw_os_error() == Some(libc::EIO)),
            "the reader's {call} failed with {err}, not ENOTCONN/ECONNABORTED\n{}",
            c.tail_log()
        );
        ensure!(
            took < Duration::from_secs(5),
            "the failing call took {took:?}"
        );
        // Whatever the first failure was, the mount is dead now, not flaky:
        // an open, which always asks the connection (a stat may be answered
        // from cached attributes), fails with the connection's error.
        let next = std::fs::File::open(pinned_path(&pinned).join("f05"));
        drop(pinned);
        ensure!(
            next.as_ref().err().is_some_and(dead),
            "after the abort an open answered {next:?}, not ENOTCONN/ECONNABORTED"
        );
        eprintln!("    {reads} reads, then {call}: {err} after the abort (in {took:?})");
        let status = c.wait_exit(Duration::from_secs(60))?;
        c.detach_dead_mount();
        ensure!(
            status.success(),
            "the daemon exited with {status} after the abort\n{}",
            c.tail_log()
        );
        no_alarms(&c)?;
        // The mountpoint is reusable, on the ring again.
        c.mount()?;
        ensure!(is_ring(&negotiated_transport(&c)?), "the remount fell back");
        serve_check(&c.mnt, seed + 1, "after-abort")?;
        c.unmount()
    })();
    let _ = c.unmount();
    c.detach_dead_mount();
    result
}

// ------------------------------------------------------------------------
// Plan 38 Z2c: cluster locks and the ring.

/// The daemon's per-mount transport records (`FUSE transport` /
/// `FUSE transport (fell back)`, one per mount it made).
fn transport_records(c: &Client) -> Vec<String> {
    c.log_text()
        .lines()
        .filter(|l| l.contains("FUSE transport"))
        .map(str::to_string)
        .collect()
}

/// `fuse.mounts[0]` of the daemon's `node.status`.
fn fuse_mount(c: &Client) -> Result<serde_json::Value> {
    let status = c.control_status()?;
    Ok(status["fuse"]["mounts"][0].clone())
}

/// Plan 38 Z2c as decided on 2026-10-05: under `auto` a mount with
/// cluster locks (`--locks cluster`, the default with P2P, which the
/// harness's daemons run with) takes the ladder like any plain mount, on
/// the deeper queue (`CLUSTER_LOCKS_URING_QUEUE_DEPTH`, 32), and no rung
/// named `cluster_locks` exists any more; an explicit depth wins; the same
/// daemon with `--locks local` gets the ordinary depth (8). A `daemon --upgrade` of the `auto` cluster-lock
/// mount is refused where it got the ring (a ring session cannot be
/// detached; the mount keeps serving) and served where it fell back, the
/// resumed session keeping its first rung. On a host that cannot grant the
/// ring every leg is `dev_fuse`, for a rung other than the locks.
pub fn transport_cluster_locks_auto(seed: u64) -> Result<()> {
    let (env, root) = setup("transport-cluster-locks-auto")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/transport-cluster-locks-{}", ts());
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "auto");
    c.fs_create()?;
    let ring_host = crate::suites::unavailable(crate::suites::FUSE_URING).is_none();
    // The mounted leg's transport: the ring at `depth` with no fallback
    // and no lock-wait downgrade yet on a ring host, a fallback for a rung
    // other than the locks anywhere else.
    let expect = |c: &Client, depth: u64, leg: &str| -> Result<String> {
        let negotiated = negotiated_transport(c)?;
        let mount = fuse_mount(c)?;
        if ring_host {
            ensure!(
                is_ring(&negotiated) && mount["last_fallback"].is_null(),
                "{leg} on a ring host negotiated {negotiated}: {mount}"
            );
            ensure!(
                mount["uring_queue_depth"] == depth,
                "{leg}: expected queue depth {depth}: {mount}"
            );
            ensure!(mount["lock_wait_downgrades"] == 0, "{leg}: {mount}");
            ensure!(
                downgrades(c).is_empty(),
                "{leg}: no rung refused the ring: {:?}",
                downgrades(c)
            );
        } else {
            let why = fallback_reported(c, None)?;
            ensure!(
                why != "cluster_locks",
                "{leg}: no rung holds a mount back for its locks"
            );
        }
        eprintln!(
            "    {leg}: {negotiated} depth {}",
            mount["uring_queue_depth"]
        );
        Ok(negotiated)
    };
    let result = (|| -> Result<()> {
        // 1. `auto`, cluster locks: the ring, on the deeper queue.
        c.mount()?;
        expect(&c, 32, "auto, cluster locks")?;
        serve_check(&c.mnt, seed, "cluster-auto")?;
        let records = transport_records(&c);
        ensure!(
            records.len() == 1,
            "one transport record per mount: {records:?}"
        );
        no_alarms(&c)?;
        c.unmount()?;

        // 2. `auto`, cluster locks, an explicit depth: it wins.
        c.set_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH", "16");
        c.mount()?;
        expect(&c, 16, "auto, cluster locks, depth 16")?;
        serve_check(&c.mnt, seed + 1, "cluster-auto-16")?;
        c.unmount()?;
        c.unset_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH");

        // 3. `auto`, local locks: the ordinary depth.
        c.mount_view(None, &["--locks", "local"])?;
        expect(&c, 8, "auto, local locks")?;
        serve_check(&c.mnt, seed + 2, "local-auto")?;
        c.unmount()?;

        // 4. `daemon --upgrade` of the `auto` cluster-lock mount: refused
        // on the ring, at no cost to the mount; served on a fallback, the
        // resumed session (pinned now, in a new process) still naming its
        // first rung, not the pin (the Z2b review's finding).
        c.set_env("CONSTELLATION_FUSE_TRANSPORT", "auto");
        c.mount()?;
        let negotiated = negotiated_transport(&c)?;
        let before = if is_ring(&negotiated) {
            None
        } else {
            Some(fallback_reported(&c, None)?)
        };
        let generation = super::handover::generation(&c)?;
        let out = super::handover::try_upgrade(&c)?;
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        match before {
            None => {
                ensure!(
                    !out.status.success() && said.contains(&format!("served over {negotiated}")),
                    "daemon --upgrade must refuse a {negotiated} session, naming it: {said}"
                );
                ensure!(
                    super::handover::generation(&c)? == generation,
                    "the generation advanced although the upgrade was refused"
                );
                ensure!(negotiated_transport(&c)? == negotiated);
                serve_check(&c.mnt, seed + 4, "refused")?;
                eprintln!("    auto, cluster locks, daemon --upgrade: refused on {negotiated}");
            }
            Some(before) => {
                ensure!(
                    out.status.success(),
                    "daemon --upgrade of a dev_fuse mount must be served: {said}"
                );
                ensure!(
                    super::handover::generation(&c)? == generation + 1,
                    "the generation did not advance"
                );
                let after = fallback_reported(&c, Some(&before))?;
                let records = transport_records(&c);
                ensure!(
                    records.len() == 2 && records[1].contains(&after),
                    "one record per daemon image, the resumed one naming {after}: {records:?}"
                );
                serve_check(&c.mnt, seed + 4, "resumed")?;
                eprintln!("    auto, cluster locks, after daemon --upgrade: still {after}");
            }
        }
        c.unmount()
    })();
    let _ = c.unmount();
    result
}

/// `harness lock-probe ROLE FILE [INDEX]` (see the subcommand's doc): one
/// process of [`transport_lock_wait_budget`]. Returns the exit code: 0
/// done (`waiter`: granted), 3 `ENOLCK`, 1 anything else.
pub fn lock_probe(role: &str, file: &Path, index: u64) -> i32 {
    use std::io::{BufRead, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::FileExt;
    let say = |what: &str| {
        let mut out = std::io::stdout().lock();
        let _ = writeln!(out, "{what}");
        let _ = out.flush();
    };
    let f = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
    {
        Ok(f) => f,
        Err(e) => {
            say(&format!("open: {e}"));
            return 1;
        }
    };
    let lock = |cmd: libc::c_int, typ: libc::c_int| -> std::io::Result<()> {
        // SAFETY: a zeroed `flock` is a valid argument; every field the
        // kernel reads is set below.
        let mut l: libc::flock = unsafe { std::mem::zeroed() };
        l.l_type = typ as libc::c_short;
        l.l_whence = libc::SEEK_SET as libc::c_short;
        // SAFETY: an fcntl lock call on a descriptor this process owns.
        if unsafe { libc::fcntl(f.as_raw_fd(), cmd, &l) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    };
    match role {
        "holder" => {
            if let Err(e) = lock(libc::F_SETLK, libc::F_WRLCK) {
                say(&format!("lock: {e}"));
                return 1;
            }
            say("locked");
            let mut line = String::new();
            let _ = std::io::stdin().lock().read_line(&mut line);
            // The holder's own request while every other entry of its
            // CPU's queue may be held by a waiter: the deadlock the budget
            // exists to prevent would hang exactly here.
            if let Err(e) = f.write_all_at(&[b'H'; 4096], 0) {
                say(&format!("write: {e}"));
                return 1;
            }
            if let Err(e) = lock(libc::F_SETLK, libc::F_UNLCK) {
                say(&format!("unlock: {e}"));
                return 1;
            }
            say("unlocked");
            0
        }
        "waiter" => match lock(libc::F_SETLKW, libc::F_WRLCK) {
            Ok(()) => {
                let record = format!("waiter {index:04}\n");
                let at = 4096 * (1 + index);
                let r = f.write_all_at(record.as_bytes(), at);
                let _ = lock(libc::F_SETLK, libc::F_UNLCK);
                match r {
                    Ok(()) => {
                        say("granted");
                        0
                    }
                    Err(e) => {
                        say(&format!("write: {e}"));
                        1
                    }
                }
            }
            Err(e) if e.raw_os_error() == Some(libc::ENOLCK) => {
                say("enolck");
                3
            }
            Err(e) => {
                say(&format!("lock: {e}"));
                1
            }
        },
        other => {
            say(&format!("unknown role {other}"));
            1
        }
    }
}

/// The CPU every process of [`transport_lock_wait_budget`] is pinned to:
/// the last one this process may run on (an arbitrary but fixed queue).
fn budget_cpu() -> Result<usize> {
    // SAFETY: a zeroed set is valid; the kernel fills it.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: our own affinity, into a set of the right size.
    let rc = unsafe { libc::sched_getaffinity(0, std::mem::size_of_val(&set), &mut set) };
    ensure!(
        rc == 0,
        "sched_getaffinity: {}",
        std::io::Error::last_os_error()
    );
    (0..libc::CPU_SETSIZE as usize)
        .rev()
        // SAFETY: `cpu` is below CPU_SETSIZE.
        .find(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .context("no CPU in our affinity mask")
}

/// One round of [`transport_lock_wait_budget`] on a mounted client whose
/// queue depth is `depth`: `depth + extra` waiters on one CPU, the holder
/// on the same CPU. Returns the downgrades it expects (`extra + 1`).
fn lock_wait_round(c: &Client, depth: u64, extra: u64, round: &str) -> Result<u64> {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    let mount = fuse_mount(c)?;
    ensure!(
        mount["transport"].as_str().is_some_and(is_ring),
        "the cluster-lock mount must be on the ring here: {mount}"
    );
    ensure!(
        mount["uring_queue_depth"] == depth,
        "expected queue depth {depth}: {mount}"
    );
    let file = c.mnt.join(format!("budget-{round}"));
    std::fs::write(&file, b"")?;
    let cpu = budget_cpu()?.to_string();
    let me = std::env::current_exe()?;
    let pinned = |args: &[&str]| {
        let mut cmd = Command::new("taskset");
        cmd.arg("-c")
            .arg(&cpu)
            .arg(&me)
            .arg("lock-probe")
            .args(args);
        cmd
    };
    let path = file.to_str().context("mount path")?;
    let mut holder = pinned(&["holder", path])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut said = BufReader::new(holder.stdout.take().context("holder stdout")?);
    let mut line = String::new();
    said.read_line(&mut line)?;
    ensure!(line.trim() == "locked", "the holder said {line:?}");

    let n = depth + extra;
    let mut waiters: Vec<Child> = (0..n)
        .map(|i| {
            pinned(&["waiter", path, &i.to_string()])
                .stdout(Stdio::piped())
                .spawn()
        })
        .collect::<std::io::Result<_>>()?;
    // `depth - 1` of them wait on the queue's entries; every other one is
    // served as non-blocking and, the lock being held, answered ENOLCK.
    let blocked = depth - 1;
    let refused = n - blocked;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut exits: Vec<Option<i32>> = vec![None; n as usize];
    let mut settled_since: Option<std::time::Instant> = None;
    loop {
        for (i, w) in waiters.iter_mut().enumerate() {
            if exits[i].is_none() {
                if let Some(st) = w.try_wait()? {
                    exits[i] = Some(st.code().unwrap_or(-1));
                }
            }
        }
        let done = exits.iter().filter(|e| e.is_some()).count() as u64;
        let enolck = exits.iter().filter(|e| **e == Some(3)).count() as u64;
        ensure!(
            exits.iter().flatten().all(|&code| code == 3),
            "with the lock held a waiter may only be refused (exit 3): {exits:?}"
        );
        ensure!(
            enolck <= refused,
            "more waiters refused than the budget allows ({enolck} > {refused}): {exits:?}"
        );
        if enolck == refused {
            // Stable: nobody else gives up while the lock is still held.
            let since = *settled_since.get_or_insert_with(std::time::Instant::now);
            if since.elapsed() > Duration::from_secs(2) {
                break;
            }
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "after 60 s {done} of {n} waiters had exited ({enolck} with ENOLCK), \
             expected {refused} refusals and {blocked} waiting: {exits:?}\n{}",
            c.tail_log()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    ensure!(
        exits.iter().filter(|e| e.is_none()).count() as u64 == blocked,
        "expected {blocked} waiters still waiting: {exits:?}"
    );
    // The queue's spare entry serves everything else from that CPU while
    // `depth - 1` waiters hold the rest: a stat, then the holder's write
    // and unlock.
    let t = std::time::Instant::now();
    let st = Command::new("taskset")
        .arg("-c")
        .arg(&cpu)
        .arg("stat")
        .arg(path)
        .stdout(Stdio::null())
        .status()?;
    ensure!(st.success(), "a stat on the waiters' CPU failed");
    ensure!(
        t.elapsed() < Duration::from_secs(10),
        "a stat on the waiters' CPU took {:?}",
        t.elapsed()
    );
    holder
        .stdin
        .as_mut()
        .context("holder stdin")?
        .write_all(b"go\n")?;
    line.clear();
    said.read_line(&mut line)?;
    ensure!(line.trim() == "unlocked", "the holder said {line:?}");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut running = 0;
        for (i, w) in waiters.iter_mut().enumerate() {
            if exits[i].is_none() {
                match w.try_wait()? {
                    Some(st) => exits[i] = Some(st.code().unwrap_or(-1)),
                    None => running += 1,
                }
            }
        }
        if running == 0 {
            break;
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "{running} waiters still blocked 60 s after the holder unlocked: {exits:?}\n{}",
            c.tail_log()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    ensure!(holder.wait()?.success(), "the holder failed");
    let granted = exits.iter().filter(|e| **e == Some(0)).count() as u64;
    let enolck = exits.iter().filter(|e| **e == Some(3)).count() as u64;
    ensure!(
        granted == blocked && enolck == refused,
        "expected {blocked} granted and {refused} ENOLCK: {exits:?}"
    );
    // Every write that was answered landed.
    let data = std::fs::read(&file)?;
    ensure!(
        data.len() >= 4096 && data[..4096].iter().all(|&b| b == b'H'),
        "the holder's write is missing"
    );
    for (i, e) in exits.iter().enumerate() {
        if *e == Some(0) {
            let at = 4096 * (1 + i);
            let want = format!("waiter {i:04}\n");
            ensure!(
                data.get(at..at + want.len()) == Some(want.as_bytes()),
                "waiter {i}'s record is missing"
            );
        }
    }
    eprintln!(
        "    {round}: depth {depth}, {n} waiters on CPU {cpu}: {granted} waited and were \
         granted, {enolck} answered ENOLCK; the holder's write and unlock went through"
    );
    Ok(refused)
}

/// Plan 38 Z2c item 4, on a **real kernel** (not the in-memory ring of
/// `wire_uring.rs`): a cluster-lock mount on the ring under the shipped
/// `auto` (since the 2026-10-05 decision), `depth + 3` processes pinned to
/// one CPU (`taskset`) blocking in `F_SETLKW` on a lock a process on the
/// same CPU holds. Without the budget the first `depth` waiters would take every
/// entry of that CPU's queue and the holder's `write` — and with it the
/// unlock — would never be served. With it: `depth - 1` wait, the rest are
/// answered `ENOLCK` at once, a `stat` from that CPU is served, the holder
/// writes and unlocks, every waiter that waited is granted, and
/// `node.status`'s `lock_wait_downgrades` (per mount and process-wide)
/// equals the refusals. Run at depth 4 and at the shipped cluster-lock
/// default (32).
pub fn transport_lock_wait_budget(_seed: u64) -> Result<()> {
    const EXTRA: u64 = 3;
    let (env, root) = setup("transport-lock-wait-budget")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/transport-lock-budget-{}", ts());
    let mut c = Client::new(root.path(), "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "auto")
        .with_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH", "4");
    c.fs_create()?;
    let result = (|| -> Result<()> {
        for (round, depth) in [("explicit-depth", 4u64), ("default-depth", 32)] {
            if round == "default-depth" {
                c.unset_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH");
            }
            c.mount()?;
            let expected = lock_wait_round(&c, depth, EXTRA, round)?;
            let status = c.control_status()?;
            let mount = &status["fuse"]["mounts"][0];
            ensure!(
                mount["lock_wait_downgrades"] == expected
                    && status["fuse"]["lock_wait_downgrades_total"] == expected,
                "lock_wait_downgrades must count the {expected} refusals: {}",
                status["fuse"]
            );
            no_alarms(&c)?;
            c.unmount()?;
        }
        Ok(())
    })();
    let _ = c.unmount();
    result
}
