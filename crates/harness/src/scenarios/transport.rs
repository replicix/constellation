//! Plan 38 §6 / §3(e): the FUSE transport the mount negotiated, and what
//! that means for `daemon --upgrade`.
//!
//! `transport-detach-refused` is **one scenario with two expected
//! outcomes, keyed on the transport the mount actually negotiated** — not
//! two scenarios, and not a scenario that skips off a ring kernel. It
//! always asks for the ladder (`CONSTELLATION_FUSE_TRANSPORT=auto`) and
//! then reads `node.status` to learn what it got:
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

/// A client whose mount asks for plan 38 §2.4's ladder. Set on the client
/// rather than inherited, so the scenario asks the same question on both
/// legs of the transport matrix lane (the leg's own
/// `CONSTELLATION_FUSE_TRANSPORT` would otherwise decide for it).
fn auto_client(env: &S3Env, root: &Path, prefix: &str) -> Result<Client> {
    let backend = format!("s3://{BUCKET}/{prefix}");
    let mut c = Client::new(root, "c0", &env.endpoint, &backend)?
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "auto");
    c.fs_create()?;
    c.mount()?;
    Ok(c)
}

/// The transport `node.status` reports for the daemon's one mount
/// (`dev_fuse` / `uring` / `uring_zc`, plan 38 §5).
fn negotiated_transport(c: &Client) -> Result<String> {
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
            "transport-detach-refused: the mount asked for auto and negotiated {negotiated}; \
             expecting the handover to be {}",
            if ring { "refused" } else { "served" }
        );
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
        .with_env("CONSTELLATION_FUSE_TRANSPORT", "auto");
    c.fs_create()?;
    Ok(c)
}

/// The control half of a ring-only scenario: without the fault, `auto`
/// gets the ring here (the scenario's `requires` said it would).
fn control_round(c: &mut Client, seed: u64) -> Result<()> {
    c.mount()?;
    let negotiated = negotiated_transport(c)?;
    ensure!(
        negotiated == "uring",
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
    eprintln!("    fell back to dev_fuse, logged once: {}", line.trim());
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
pub fn transport_enomem_ring(seed: u64) -> Result<()> {
    const DEPTH: u64 = 64;
    let (env, root) = setup("transport-enomem-ring")?;
    let _proxy = env.s3_proxy()?;
    let mut c = ring_client(&env, root.path(), "transport-enomem-ring")?
        .with_env("CONSTELLATION_FUSE_URING_QUEUE_DEPTH", &DEPTH.to_string());
    let result = (|| -> Result<()> {
        control_round(&mut c, seed)?;
        // What the daemon needs without a ring, under the same workload.
        c.set_env("CONSTELLATION_FUSE_TRANSPORT", "dev-fuse");
        c.mount()?;
        serve_check(&c.mnt, seed, "probe")?;
        let peak = vm_peak(c.pid().context("no daemon pid")?)?;
        c.unmount()?;
        c.set_env("CONSTELLATION_FUSE_TRANSPORT", "auto");
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
        fallback_round(&mut c, seed, &["ring buffers failed", "ENOMEM"])
    })();
    c.set_address_space_limit(None);
    let _ = c.unmount();
    result
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
            negotiated == "uring",
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
        ensure!(
            negotiated_transport(&c)? == "uring",
            "the remount fell back"
        );
        serve_check(&c.mnt, seed + 1, "after-abort")?;
        c.unmount()
    })();
    let _ = c.unmount();
    c.detach_dead_mount();
    result
}
