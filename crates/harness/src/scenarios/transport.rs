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
use std::path::Path;
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
