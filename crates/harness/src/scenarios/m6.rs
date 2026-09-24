//! Plan 30 §M6 scenarios: positions on replies, the per-node `observed`
//! watermark, and the session wait on local reads (read-your-writes and
//! monotonic reads per node; `constellation_meta::session`).
//!
//! Every scenario prints the node's `status.session` block (how many
//! reads went through the check, how each ended, and the wait histogram),
//! which is also the read-latency measurement plan 30 §M6 asks for.

use super::{
    create_new, eventually, lease_of, phantom_setup_with, setup, speculation_of, ts, wait_for_p2p,
};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{bail, Context, Result};
use std::path::Path;
use std::time::{Duration, Instant};

/// A node's `status.session` block.
fn session_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["session"].clone())
}

fn n(v: &serde_json::Value, key: &str) -> u64 {
    v[key].as_u64().unwrap_or(0)
}

fn print_session(scenario: &str, who: &str, s: &serde_json::Value) {
    eprintln!(
        "    {scenario}: {who} session: reads {} fast {} covered {} waited {} timeouts {} \
         (held {}) replay-blocked {} raised {} wait-ms total {} histogram(log2 ms) {}",
        n(s, "reads"),
        n(s, "fast"),
        n(s, "covered"),
        n(s, "waited"),
        n(s, "timeouts"),
        n(s, "degraded_held"),
        n(s, "replay_blocked"),
        n(s, "raised"),
        n(s, "wait_ms_total"),
        s["waits_ms"]
    );
}

/// Two nodes on a fresh filesystem; A (with `hold` as its sync-hold file)
/// holds the lease, `f0` exists everywhere.
fn two_nodes(
    scenario: &str,
    extra: &[(&str, &str)],
    hold: Option<&Path>,
) -> Result<(crate::s3env::S3Env, tempfile::TempDir, Client, Client)> {
    let (env, root) = setup(scenario)?;
    let backend = format!("s3://{BUCKET}/{scenario}-{}", ts());
    let mk = |name: &str, hold: Option<&Path>| -> Result<Client> {
        let mut c =
            Client::new(root.path(), name, &env.direct_endpoint, &backend)?.with_own_node_key();
        for (k, v) in extra {
            c = c.with_env(k, v);
        }
        if let Some(hold) = hold {
            c = c.with_env(
                "CONSTELLATION_FAULT_HOLD_SYNC_FILE",
                hold.to_str().context("hold path")?,
            );
        }
        Ok(c)
    };
    let mut a = mk("a", hold)?;
    let mut b = mk("b", None)?;
    a.fs_create()?;
    a.mount()?;
    b.mount()?;
    wait_for_p2p(&[&a, &b])?;
    std::fs::write(a.mnt.join("f0"), b"zero")?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&a)?;
        anyhow::ensure!(lease["held"] == true, "A does not hold: {lease}");
        Ok(())
    })?;
    eventually("f0 visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("f0").is_file(), "f0 missing on B");
        Ok(())
    })?;
    Ok((env, root, a, b))
}

/// Hold `a`'s sync rounds via its hold file and wait until a round has
/// observed it (nothing A journals ships from then on).
fn hold_sync(root: &Path, hold: &Path) -> Result<()> {
    std::fs::write(hold, b"hold")?;
    let held = root.join(format!(
        "{}.held",
        hold.file_name().and_then(|n| n.to_str()).unwrap_or("hold")
    ));
    eventually(
        "the holder's round observes the hold",
        Duration::from_secs(20),
        || {
            anyhow::ensure!(held.is_file(), "no round has observed the hold yet");
            Ok(())
        },
    )
}

/// `EEXIST` becomes an instance of the general rule. A holds and has
/// journaled `g` and then `f` without shipping (its sync is held). B's
/// create of `f` is refused against that unshipped state; B then looks up
/// `f` *and* `g`. Before M6 only `f` was point-fixed (the per-name causal
/// wait), so `stat g` right after the refusal said `ENOENT`; now the
/// refusal raised B's `observed` to A's journal position, both lookups
/// wait for it, and both find their file once A ships (the hold lifts
/// after ~0.5 s), well inside the 2 s budget.
pub fn session_exists_observed(_seed: u64) -> Result<()> {
    const NAME: &str = "session-exists-observed";
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b) = two_nodes(NAME, &[], Some(&hold))?;
    hold_sync(tmp.path(), &hold)?;
    let result = (|| -> Result<()> {
        std::fs::write(a.mnt.join("g"), b"gee")?;
        std::fs::write(a.mnt.join("f"), b"eff")?;
        std::thread::sleep(Duration::from_millis(500));
        anyhow::ensure!(
            !b.mnt.join("g").exists(),
            "B already sees A's held write of `g`: A shipped while held"
        );
        match create_new(&b.mnt, "f") {
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {}
            Err(e) => bail!("B's create of `f` returned {e}, expected EEXIST"),
            Ok(()) => bail!("B's create of `f` succeeded although A has it"),
        }
        let before = session_of(&b)?;
        // Lift the hold in half a second; the lookups started now must
        // wait for A's segment rather than answer ENOENT.
        let lift = {
            let hold = hold.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(500));
                let _ = std::fs::remove_file(&hold);
            })
        };
        let started = Instant::now();
        let g = std::fs::metadata(b.mnt.join("g"));
        let g_elapsed = started.elapsed();
        let f = std::fs::metadata(b.mnt.join("f"));
        let _ = lift.join();
        let after = session_of(&b)?;
        print_session(NAME, "B", &after);
        eprintln!(
            "    {NAME}: stat g on B returned after {g_elapsed:?}: {:?}",
            g.as_ref().map(|m| m.len())
        );
        g.context("B's `stat g` after the refusal: the refusal observed A's state including `g`")?;
        f.context("B's `stat f` after the refusal")?;
        anyhow::ensure!(
            n(&after, "raised") > n(&before, "raised") || n(&after, "raised") > 0,
            "the refusal did not raise B's observed watermark: {after}"
        );
        anyhow::ensure!(
            n(&after, "waited") > n(&before, "waited"),
            "B's reads did not wait for the observed position: {after}"
        );
        anyhow::ensure!(
            n(&after, "timeouts") == 0,
            "B's reads timed out (degraded) although A shipped within 0.5 s: {after}"
        );
        Ok(())
    })();
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    a.unmount()?;
    result
}

/// Read-your-writes on the forwarding path, and coordinator decision 2:
/// B creates files through A (accepted, installed as shadows) and at once
/// lists the directory, stats the parent and each file, and reads it —
/// every read on the fast path (`waited == 0`), even though A has shipped
/// nothing yet (its sync is held for the first half).
pub fn session_forwarded_ryw(_seed: u64) -> Result<()> {
    const NAME: &str = "session-forwarded-ryw";
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b) = two_nodes(NAME, &[], Some(&hold))?;
    let result = (|| -> Result<()> {
        let before = session_of(&b)?;
        for phase in ["held", "shipping"] {
            if phase == "held" {
                hold_sync(tmp.path(), &hold)?;
            } else {
                std::fs::remove_file(&hold)?;
            }
            for i in 0..25 {
                let name = format!("{phase}-{i}");
                let p = b.mnt.join(&name);
                // touch a; ls; stat .; stat a; cat a
                std::fs::write(&p, name.as_bytes())
                    .with_context(|| format!("B's forwarded create of {name}"))?;
                let listed = std::fs::read_dir(&b.mnt)?
                    .filter_map(|e| e.ok())
                    .any(|e| e.file_name().to_str() == Some(name.as_str()));
                anyhow::ensure!(listed, "`ls` on B right after creating {name} misses it");
                std::fs::metadata(&b.mnt).context("stat . on B")?;
                std::fs::metadata(&p).with_context(|| format!("stat {name} on B"))?;
                anyhow::ensure!(
                    std::fs::read(&p)? == name.as_bytes(),
                    "B reads back other content for {name}"
                );
            }
        }
        let after = session_of(&b)?;
        print_session(NAME, "B", &after);
        anyhow::ensure!(
            n(&after, "reads") > n(&before, "reads") + 50,
            "B's reads did not go through the session check: {after}"
        );
        anyhow::ensure!(
            n(&after, "waited") == n(&before, "waited") && n(&after, "timeouts") == 0,
            "a read after B's own forwarded create waited (decision 2: installed effects \
             raise nothing): {after}"
        );
        Ok(())
    })();
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    a.unmount()?;
    result
}

/// M5 finding 1 end to end, the stale-base half of M6: plan 30 M5's
/// `stale-base-rename-divergence` (a rename accepted on a base the
/// requester has not applied waits for the log instead of installing a
/// shadow that the holder's unshipped unlink then removes). Run as is:
/// M6 changes the reply's position, not the rule.
pub fn session_stale_base_rename(seed: u64) -> Result<()> {
    super::m5::stale_base_rename_divergence(seed)
}

/// Read-your-writes across a holder kill. C's forwarded create is acked
/// by A while A cannot reach S3, then A is killed: the create never
/// ships. B takes over; C tails B's epoch marker, which strands C's
/// shadow (rolled back, queued for replay by rid) until the replay lands
/// through B. C lists the directory and stats its own file every 20 ms
/// throughout: before M6 the stranding window left C's own acknowledged
/// create out of its listing; now every read shows it (the queued replay
/// blocks reads of its keys, the directory included).
pub fn session_ryw_after_holder_kill(_seed: u64) -> Result<()> {
    const NAME: &str = "session-ryw-after-holder-kill";
    let (_env, _root, mut a, mut b, mut c, sw) =
        phantom_setup_with(NAME, &[("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")])?;
    let result = (|| -> Result<()> {
        sw.cut();
        create_new(&c.mnt, "phantom")
            .context("C's forwarded create must be acked while A's lease is still valid")?;
        a.kill9()?;
        let rolled_before = n(&speculation_of(&c)?, "rolled_back");
        // B's write takes over once A's lease runs out.
        let takeover = {
            let mnt = b.mnt.clone();
            std::thread::spawn(move || std::fs::write(mnt.join("b-after"), b"b"))
        };
        let deadline = Instant::now() + Duration::from_secs(40);
        let mut stats = 0u64;
        let mut missing = Vec::new();
        let mut slowest = Duration::ZERO;
        loop {
            // `ls` (a readdir always reaches the daemon; a `stat` is
            // mostly answered from the kernel's 1 s entry cache) and `stat`.
            let started = Instant::now();
            let listed = std::fs::read_dir(&c.mnt)?
                .filter_map(|e| e.ok())
                .any(|e| e.file_name() == "phantom");
            if !listed {
                missing.push(format!(
                    "ls at {:?} before the deadline",
                    deadline - Instant::now()
                ));
            }
            if let Err(e) = std::fs::metadata(c.mnt.join("phantom")) {
                missing.push(format!("stat at {:?}: {e}", deadline - Instant::now()));
            }
            slowest = slowest.max(started.elapsed());
            stats += 1;
            let spec = speculation_of(&c)?;
            let settled = n(&spec, "rolled_back") > rolled_before
                && n(&spec, "pending_replay") == 0
                && takeover.is_finished();
            if settled || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        takeover
            .join()
            .map_err(|_| anyhow::anyhow!("takeover thread panicked"))?
            .context("B's write (the takeover)")?;
        let spec = speculation_of(&c)?;
        let session = session_of(&c)?;
        print_session(NAME, "C", &session);
        eprintln!(
            "    {NAME}: C stat'ed its phantom {stats} times, slowest {slowest:?}; speculation {spec}"
        );
        anyhow::ensure!(
            n(&spec, "rolled_back") > rolled_before,
            "C's shadow was never stranded (the scenario did not exercise the window): {spec}"
        );
        anyhow::ensure!(
            missing.is_empty(),
            "C's own acknowledged create was missing from its reads {} time(s): {missing:?}",
            missing.len()
        );
        anyhow::ensure!(
            n(&session, "timeouts") == 0,
            "a read of the phantom timed out (degraded): {session}"
        );
        if n(&session, "replay_blocked") == 0 {
            eprintln!(
                "    {NAME}: note: no read landed inside the stranding window this run \
                 (replay-blocked 0); the listing check above still held throughout"
            );
        }
        Ok(())
    })();
    c.unmount()?;
    b.unmount()?;
    result
}

/// Degraded, not an error. A holds with its sync held and has journaled
/// `g`; B's create of `g` is refused, raising B's `observed` past
/// anything B can apply. B's reads then wait the whole budget
/// (`CONSTELLATION_SESSION_WAIT_MS=1500`) and answer from the replica —
/// no `EIO` — the first logs one warning, the rest only count.
pub fn session_wait_degrades(_seed: u64) -> Result<()> {
    const NAME: &str = "session-wait-degrades";
    const BUDGET_MS: u64 = 1_500;
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b) = two_nodes(
        NAME,
        &[("CONSTELLATION_SESSION_WAIT_MS", "1500")],
        Some(&hold),
    )?;
    let result = (|| -> Result<()> {
        hold_sync(tmp.path(), &hold)?;
        std::fs::write(a.mnt.join("g"), b"gee")?;
        match create_new(&b.mnt, "g") {
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {}
            other => bail!("B's create of `g` returned {other:?}, expected EEXIST"),
        }
        // Names never looked up before (the kernel's 1 s entry cache would
        // answer a repeated lookup without asking the daemon).
        let mut elapsed = Vec::new();
        for i in 0..3 {
            let started = Instant::now();
            let r = std::fs::metadata(b.mnt.join(format!("absent-{i}")));
            elapsed.push(started.elapsed());
            match r {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                other => {
                    bail!("B's lookup of absent-{i} must answer ENOENT (degraded), got {other:?}")
                }
            }
        }
        let session = session_of(&b)?;
        print_session(NAME, "B", &session);
        eprintln!("    {NAME}: B's degraded stats took {elapsed:?}");
        for e in &elapsed {
            anyhow::ensure!(
                *e >= Duration::from_millis(BUDGET_MS - 100)
                    && *e < Duration::from_millis(BUDGET_MS * 3),
                "a degraded read took {e:?}, expected about the {BUDGET_MS} ms budget"
            );
        }
        anyhow::ensure!(
            n(&session, "timeouts") >= 3,
            "timeouts not counted: {session}"
        );
        let warnings = b.tail_log_n(4000).matches("session wait timed out").count();
        anyhow::ensure!(
            warnings == 1,
            "expected exactly one timeout warning in B's log, found {warnings}"
        );
        // Lift the hold: A ships, B catches up, reads are fast again.
        std::fs::remove_file(&hold)?;
        eventually("g visible on B", Duration::from_secs(20), || {
            anyhow::ensure!(b.mnt.join("g").is_file(), "g missing on B");
            Ok(())
        })?;
        let before = session_of(&b)?;
        let started = Instant::now();
        let _ = std::fs::metadata(b.mnt.join("absent-9"));
        let fast = started.elapsed();
        let after = session_of(&b)?;
        anyhow::ensure!(
            n(&after, "timeouts") == n(&before, "timeouts") && fast < Duration::from_millis(500),
            "reads did not recover once A shipped: {fast:?} {after}"
        );
        Ok(())
    })();
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    a.unmount()?;
    result
}

/// The read-latency measurement plan 30 §M6 asks for: after a write burst
/// from every node and quiescence, a read-only phase (stat, readdir and
/// read of every file on every node) never waits — `waited` and
/// `timeouts` stay flat. And a single node alone never waits at all,
/// writes and reads interleaved.
pub fn session_idle_latency(_seed: u64) -> Result<()> {
    const NAME: &str = "session-idle-latency";
    const FILES: usize = 40;
    let (env, root) = setup(NAME)?;
    let backend = format!("s3://{BUCKET}/{NAME}-{}", ts());
    let mut nodes = Vec::new();
    for name in ["a", "b", "c"] {
        nodes.push(
            Client::new(root.path(), name, &env.direct_endpoint, &backend)?.with_own_node_key(),
        );
    }
    nodes[0].fs_create()?;
    for c in nodes.iter_mut() {
        c.mount()?;
    }
    wait_for_p2p(&nodes.iter().collect::<Vec<_>>())?;
    let result = (|| -> Result<()> {
        // The burst: every node writes its files (forwarded or local).
        for (i, c) in nodes.iter().enumerate() {
            for k in 0..FILES {
                std::fs::write(c.mnt.join(format!("n{i}-{k}")), format!("{i}/{k}"))?;
            }
        }
        eventually("every file everywhere", Duration::from_secs(60), || {
            for c in &nodes {
                let count = std::fs::read_dir(&c.mnt)?.count();
                anyhow::ensure!(
                    count >= 3 * FILES,
                    "{} sees {count} entries",
                    c.mnt.display()
                );
            }
            Ok(())
        })?;
        std::thread::sleep(Duration::from_secs(2));
        let before: Vec<serde_json::Value> = nodes.iter().map(session_of).collect::<Result<_>>()?;
        let started = Instant::now();
        for c in &nodes {
            for _ in 0..3 {
                for e in std::fs::read_dir(&c.mnt)? {
                    let e = e?;
                    std::fs::metadata(e.path())?;
                    if e.file_type()?.is_file() {
                        std::fs::read(e.path())?;
                    }
                }
            }
        }
        let read_phase = started.elapsed();
        for (i, c) in nodes.iter().enumerate() {
            let after = session_of(c)?;
            print_session(NAME, &format!("node {i} (idle read phase)"), &after);
            anyhow::ensure!(
                n(&after, "reads") > n(&before[i], "reads"),
                "node {i}'s reads did not go through the session check"
            );
            anyhow::ensure!(
                n(&after, "waited") == n(&before[i], "waited")
                    && n(&after, "timeouts") == n(&before[i], "timeouts"),
                "an idle read waited on node {i}: before {} after {after}",
                before[i]
            );
        }
        eprintln!("    {NAME}: idle read phase over 3 nodes took {read_phase:?}");
        Ok(())
    })();
    for c in nodes.iter_mut().rev() {
        c.unmount()?;
    }
    result?;

    // A single node, alone on its own filesystem.
    let backend = format!("s3://{BUCKET}/{NAME}-single-{}", ts());
    let mut solo =
        Client::new(root.path(), "solo", &env.direct_endpoint, &backend)?.with_own_node_key();
    solo.fs_create()?;
    solo.mount()?;
    let result = (|| -> Result<()> {
        for k in 0..FILES {
            let p = solo.mnt.join(format!("s{k}"));
            std::fs::write(&p, b"s")?;
            std::fs::metadata(&p)?;
            std::fs::read_dir(&solo.mnt)?.count();
        }
        let s = session_of(&solo)?;
        print_session(NAME, "single node", &s);
        anyhow::ensure!(
            n(&s, "waited") == 0 && n(&s, "timeouts") == 0 && n(&s, "raised") == 0,
            "a single node waited: {s}"
        );
        Ok(())
    })();
    solo.unmount()?;
    result
}
