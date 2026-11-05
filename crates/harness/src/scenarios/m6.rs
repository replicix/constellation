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
use constellation_types::Code;
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

/// What [`backed_by_third`] hands back: the environment, the scratch root,
/// and nodes A (the holder), B (the reader) and C (A's backup).
type BackedRig = (
    crate::s3env::S3Env,
    tempfile::TempDir,
    Client,
    Client,
    Client,
);

/// Three nodes on a fresh filesystem: A (with `hold` as its sync-hold
/// file) holds the lease and is backed by C; B, mounted only once C is
/// A's backup (one backup at the default `CONSTELLATION_BACKUPS`), is
/// neither. `f0` exists everywhere.
///
/// For the scenarios whose reader must see a refusal against A's
/// unshipped state without being able to apply it. Since plan 43 C2f a
/// root answers another node only once the row is committed: a lone root
/// from the archive (which A's held sync never reaches: the refusal itself
/// would wait, `EIO` at its deadline), a backed root once its backup holds
/// the row. A backup is streamed A's journal before S3, so the reader is
/// not the backup: C is.
fn backed_by_third(scenario: &str, extra: &[(&str, &str)], hold: &Path) -> Result<BackedRig> {
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
    let mut a = mk("a", Some(hold))?;
    let mut b = mk("b", None)?;
    let mut c = mk("c", None)?;
    a.fs_create()?;
    a.mount()?;
    c.mount()?;
    wait_for_p2p(&[&a, &c])?;
    std::fs::write(a.mnt.join("f0"), b"zero")?;
    eventually("A holds the lease", Duration::from_secs(20), || {
        let lease = lease_of(&a)?;
        anyhow::ensure!(lease["held"] == true, "A does not hold: {lease}");
        Ok(())
    })?;
    let c_id = c.control_status()?["node_id"]
        .as_u64()
        .context("C reports no node id")?;
    eventually("A lists C as its backup", Duration::from_secs(30), || {
        let ack = ack_of(&a)?;
        let backups: Vec<u64> = ack["backups"]
            .as_array()
            .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
            .unwrap_or_default();
        anyhow::ensure!(
            ack["policy"] == "backup" && backups == [c_id],
            "A's backup set is not [C]: {ack}"
        );
        Ok(())
    })?;
    b.mount()?;
    wait_for_p2p(&[&a, &b, &c])?;
    eventually("f0 visible on B", Duration::from_secs(20), || {
        anyhow::ensure!(b.mnt.join("f0").is_file(), "f0 missing on B");
        Ok(())
    })?;
    Ok((env, root, a, b, c))
}

/// That A is still backed by C alone (B never became a backup: it would
/// be streamed A's held rows and need not wait for them).
fn still_backed_by_c(a: &Client, c: &Client) -> Result<()> {
    let c_id = c.control_status()?["node_id"].as_u64().unwrap_or(0);
    let ack = ack_of(a)?;
    let backups: Vec<u64> = ack["backups"]
        .as_array()
        .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
        .unwrap_or_default();
    anyhow::ensure!(
        ack["policy"] == "backup" && backups == [c_id],
        "A's backup set changed during the run (not [C]): {ack}"
    );
    Ok(())
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
///
/// Three nodes since plan 43 C2f ([`backed_by_third`]): A is backed by C,
/// so the refusal is answered once C holds its row (a lone A would answer
/// B only from the archive, which the hold keeps A from reaching; a B that
/// backs A is streamed `g` and need not wait).
pub fn session_exists_observed(_seed: u64) -> Result<()> {
    const NAME: &str = "session-exists-observed";
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b, mut c) = backed_by_third(NAME, &[], &hold)?;
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
            Err(e) if Code::from_os_error(&e) == Some(Code::Exists) => {}
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
        still_backed_by_c(&a, &c)
    })();
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    c.unmount()?;
    a.unmount()?;
    result
}

/// A node's `status.ack` block (plan 30 §M9).
fn ack_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["ack"].clone())
}

/// `ls` shows `name`; `stat .`, `stat name` succeed; `cat name` reads
/// `content` — every one of them a FUSE read through the session check.
fn ls_stat_cat(dir: &Path, name: &str, content: &[u8], who: &str) -> Result<()> {
    let p = dir.join(name);
    let listed = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_str() == Some(name));
    anyhow::ensure!(
        listed,
        "`ls` on {who} right after creating {name} misses it"
    );
    std::fs::metadata(dir).with_context(|| format!("stat . on {who}"))?;
    std::fs::metadata(&p).with_context(|| format!("stat {name} on {who}"))?;
    anyhow::ensure!(
        std::fs::read(&p)? == content,
        "{who} reads back other content for {name}"
    );
    Ok(())
}

/// Read-your-writes on the forwarding path, and coordinator decision 2:
/// B creates files through A (accepted, installed as shadows) and at once
/// lists the directory, stats the parent and each file, and reads it —
/// every read on the fast path (`waited == 0`).
///
/// Three phases, in the default configuration (plan 30 §M9: A picks B as
/// its backup, so every forwarded reply is parked until B has the row):
/// 1. *held* — A's sync is held (nothing ships): `touch a; ls; stat .;
///    stat a; cat a` on B, 25 times. Only creates: a content write's
///    manifest commit on a file whose create A has not shipped is accepted
///    on a stale base and waits for the log (plan 30 §M5's base rule), so
///    with A held it would block until A's lease lapsed (the hold stops
///    its renewals too) and B took the lease over — which is what this
///    scenario did before, measuring B's reads *as the holder* for 49 of
///    its 50 iterations (see PROGRESS, "Fix: session-forwarded-ryw
///    regression").
/// 2. *shipping* — the hold lifted: the same with content (`echo x > a`).
/// 3. *holder* — the same on A, the holder, whose acknowledgements wait
///    for B's backup ack: its own reads never find a row of its own that
///    is not durable yet (the close's manifest commit included). In a
///    subdirectory, out of reach of desktop mount watchers (see the
///    phase).
///
/// A's placement is off, and the scenario checks that A held the lease
/// under the same epoch, with B as its backup, from start to end: B's
/// writes were forwarded, not executed by B as a holder.
pub fn session_forwarded_ryw(_seed: u64) -> Result<()> {
    const NAME: &str = "session-forwarded-ryw";
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b) = two_nodes(
        NAME,
        &[("CONSTELLATION_LEASE_PLACEMENT", "off")],
        Some(&hold),
    )?;
    let result = (|| -> Result<()> {
        let a_id = a.control_status()?["node_id"]
            .as_u64()
            .context("A reports no node id")?;
        let b_id = b.control_status()?["node_id"]
            .as_u64()
            .context("B reports no node id")?;
        // The default configuration's backup (plan 30 §M9): B, the only
        // peer on this LAN.
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = ack_of(&a)?;
            let backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default();
            anyhow::ensure!(
                ack["policy"] == "backup" && backups == [b_id],
                "A's backup set is not [B]: {ack}"
            );
            Ok(())
        })?;
        let epoch = lease_of(&a)?["epoch"].as_u64().unwrap_or(0);
        let a_holds = |when: &str| -> Result<()> {
            let la = lease_of(&a)?;
            let lb = lease_of(&b)?;
            anyhow::ensure!(
                la["held"] == true
                    && la["holder"].as_u64() == Some(a_id)
                    && la["epoch"].as_u64() == Some(epoch)
                    && lb["held"] != true,
                "{when}: A no longer holds epoch {epoch} (B's writes were not forwarded): \
                 A {la}, B {lb}"
            );
            let ack = ack_of(&a)?;
            anyhow::ensure!(
                ack["policy"] == "backup",
                "{when}: A is no longer under the backup policy: {ack}"
            );
            Ok(())
        };
        let before_b = session_of(&b)?;
        let forwarded_before = b.control_status()?["forwarded_ok"].as_u64().unwrap_or(0);
        for phase in ["held", "shipping"] {
            if phase == "held" {
                hold_sync(tmp.path(), &hold)?;
            } else {
                std::fs::remove_file(&hold)?;
            }
            let started = Instant::now();
            for i in 0..25 {
                let name = format!("{phase}-{i}");
                // touch a (held) / echo x > a (shipping); ls; stat .;
                // stat a; cat a
                let content: &[u8] = if phase == "held" {
                    create_new(&b.mnt, &name)
                        .with_context(|| format!("B's forwarded create of {name}"))?;
                    b""
                } else {
                    std::fs::write(b.mnt.join(&name), name.as_bytes())
                        .with_context(|| format!("B's forwarded create of {name}"))?;
                    name.as_bytes()
                };
                ls_stat_cat(&b.mnt, &name, content, "B")?;
            }
            eprintln!(
                "    {NAME}: phase {phase}: 25 creates on B in {:?}",
                started.elapsed()
            );
            a_holds(&format!("after phase {phase}"))?;
        }
        let after_b = session_of(&b)?;
        print_session(NAME, "B", &after_b);
        let forwarded =
            b.control_status()?["forwarded_ok"].as_u64().unwrap_or(0) - forwarded_before;
        eprintln!("    {NAME}: B forwarded {forwarded} ops to A");
        anyhow::ensure!(
            n(&after_b, "reads") > n(&before_b, "reads") + 50,
            "B's reads did not go through the session check: {after_b}"
        );
        anyhow::ensure!(
            forwarded >= 50,
            "B forwarded only {forwarded} ops for its 50 creates: they did not go through A"
        );
        anyhow::ensure!(
            n(&after_b, "waited") == n(&before_b, "waited") && n(&after_b, "timeouts") == 0,
            "a read after B's own forwarded create waited (decision 2: installed effects \
             raise nothing): {after_b}"
        );

        // Phase 3: the holder's own writes under the backup policy, in a
        // directory of their own. Not the mount's top directory: a desktop
        // session's `gvfsd-trash` watches every mount's top directory for
        // a trash directory to appear and stats each entry created there —
        // a GETATTR of the file *while its close is still waiting for the
        // backup*, which rightly waits (the row is not durable yet) and
        // was counted here as this scenario's read (1 run in ~15 on a
        // GNOME host: `reads durability-blocked 0 -> 6`, every blocked
        // read from pid `gvfsd-trash`, every close returned with its row
        // durable). The assertion is about reads issued after the
        // acknowledgement, which only this process makes here.
        let dir = a.mnt.join("holder");
        std::fs::create_dir(&dir).context("A's mkdir holder")?;
        let before_a = session_of(&a)?;
        let before_ack = ack_of(&a)?;
        let started = Instant::now();
        for i in 0..25 {
            let name = format!("holder-{i}");
            std::fs::write(dir.join(&name), name.as_bytes())
                .with_context(|| format!("A's create of {name}"))?;
            ls_stat_cat(&dir, &name, name.as_bytes(), "A")?;
        }
        eprintln!(
            "    {NAME}: phase holder: 25 creates on A in {:?}",
            started.elapsed()
        );
        a_holds("after phase holder")?;
        let after_a = session_of(&a)?;
        let after_ack = ack_of(&a)?;
        print_session(NAME, "A", &after_a);
        eprintln!(
            "    {NAME}: A reads durability-blocked {} -> {}",
            n(&before_ack, "reads_durability_blocked"),
            n(&after_ack, "reads_durability_blocked")
        );
        anyhow::ensure!(
            n(&after_a, "reads") > n(&before_a, "reads") + 50,
            "A's reads did not go through the session check: {after_a}"
        );
        anyhow::ensure!(
            n(&after_a, "waited") == n(&before_a, "waited")
                && n(&after_a, "timeouts") == n(&before_a, "timeouts")
                && n(&after_ack, "reads_durability_blocked")
                    == n(&before_ack, "reads_durability_blocked"),
            "a read of the holder's own write waited for durability: an acknowledgement \
             (the close's manifest commit) returned before its row was durable: \
             session {after_a}, ack {after_ack}"
        );
        Ok(())
    })();
    // Kept at the end, so a failure in any phase has its logs.
    if std::env::var_os("HARNESS_KEEP_LOGS").is_some() {
        let dir = super::m11::kept_logs_dir();
        let _ = std::fs::create_dir_all(dir);
        for c in [&a, &b] {
            let path = dir.join(format!("{NAME}-{}.log", c.name));
            let _ = std::fs::write(&path, c.tail_log_n(400_000));
            eprintln!("    {NAME}: {}'s log kept at {}", c.name, path.display());
        }
    }
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    a.unmount()?;
    result
}

/// The shape `session-forwarded-ryw` had before its rewrite, made a
/// scenario of its own: a forwarded close that waits for the log, then
/// the requester takes the lease over. A holds with its sync held (its
/// journal unshipped) and B as its M9 backup. B creates `f` (forwarded),
/// then writes it and closes: the close's manifest commit is accepted by
/// A on a base B has not applied (A's unshipped create of `f`), so B
/// waits for the log (`AwaitingLog`). A stalls (SIGSTOP); B, its backup,
/// seals A's epoch and takes the lease over, with A's backup tail — the
/// manifest commit's completion included — in its own journal. B's close
/// must return at once and succeed. Before the fix it waited for a log
/// that would never carry the op and returned `EIO` at the 120 s client
/// deadline, for a write that had landed.
///
/// The pre-S3 stream is off (`CONSTELLATION_PRE_S3_STREAMING=0`): since
/// edd3d5d it answers exactly this wait (A streams the backup-acknowledged
/// manifest commit to B ahead of S3), so the close returned in ~6 ms and
/// the precondition — a forwarded close waiting for the log — no longer
/// held. The backup stays (B's seal-based takeover of A is what the
/// scenario checks; with no backup in budget it would be a TTL takeover).
pub fn takeover_resolves_awaiting_close(_seed: u64) -> Result<()> {
    use std::io::Write;
    use std::os::fd::IntoRawFd;
    const NAME: &str = "takeover-resolves-awaiting-close";
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b) = two_nodes(
        NAME,
        &[
            ("CONSTELLATION_LEASE_PLACEMENT", "off"),
            ("CONSTELLATION_PRE_S3_STREAMING", "0"),
        ],
        Some(&hold),
    )?;
    let mut paused = false;
    let result = (|| -> Result<()> {
        let b_id = b.control_status()?["node_id"]
            .as_u64()
            .context("B reports no node id")?;
        eventually("A lists B as its backup", Duration::from_secs(30), || {
            let ack = ack_of(&a)?;
            let backups: Vec<u64> = ack["backups"]
                .as_array()
                .map(|v| v.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default();
            anyhow::ensure!(
                ack["policy"] == "backup" && backups == [b_id],
                "A's backup set is not [B]: {ack}"
            );
            Ok(())
        })?;
        let epoch = lease_of(&a)?["epoch"].as_u64().unwrap_or(0);
        hold_sync(tmp.path(), &hold)?;
        create_new(&b.mnt, "f").context("B's forwarded create of f")?;
        let content = b"written through a holder that then stalled".to_vec();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = {
            let path = b.mnt.join("f");
            let content = content.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                let result = (|| -> std::io::Result<()> {
                    let mut f = std::fs::OpenOptions::new().write(true).open(&path)?;
                    f.write_all(&content)?;
                    // The close is where the manifest commit happens; its
                    // error must not be lost in `File`'s drop.
                    let fd = f.into_raw_fd();
                    // SAFETY: `fd` is ours (just taken out of the `File`).
                    if unsafe { libc::close(fd) } != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                })();
                let _ = done_tx.send((result, started.elapsed()));
            })
        };
        // The close waits for the log (A has not shipped the create).
        match done_rx.recv_timeout(Duration::from_millis(1500)) {
            Err(_) => {}
            Ok((r, took)) => bail!(
                "B's close returned ({r:?} after {took:?}) while A's sync was held: \
                 the manifest commit did not wait for the log, the scenario's precondition"
            ),
        }
        let stalled = Instant::now();
        a.pause().context("SIGSTOP A")?;
        paused = true;
        let (result, took) = done_rx.recv_timeout(Duration::from_secs(30)).map_err(|_| {
            anyhow::anyhow!(
                "B's close had not returned 30 s after A stalled (B lease {:?}, spec {:?})",
                lease_of(&b).ok(),
                speculation_of(&b).ok()
            )
        })?;
        let after_stall = stalled.elapsed();
        let _ = writer.join();
        let lb = lease_of(&b)?;
        eprintln!(
            "    {NAME}: B's close returned {result:?} after {took:?} ({after_stall:?} after A \
             stalled); B lease {lb}"
        );
        result.context("B's close (the manifest commit) after B took the lease over")?;
        anyhow::ensure!(
            lb["held"] == true && lb["epoch"].as_u64().unwrap_or(0) > epoch,
            "B does not hold a newer epoch than A's {epoch}: {lb}"
        );
        let read = std::fs::read(b.mnt.join("f")).context("B reads f back")?;
        anyhow::ensure!(read == content, "B reads back other content for f");
        // Latency, once the data checked out: the seal takes ~1.5 s
        // (`CONSTELLATION_BACKUP_TAKEOVER_MS`), the rest is host slack.
        crate::hostlag::latency_bound(
            "B's close after A stalled",
            stalled,
            after_stall,
            Duration::from_millis(1_500),
            Duration::from_secs(15),
        )?;
        let _ = std::fs::remove_file(&hold);
        a.resume().context("SIGCONT A")?;
        paused = false;
        eventually("A sees B's f", Duration::from_secs(30), || {
            let got = std::fs::read(a.mnt.join("f"))?;
            anyhow::ensure!(got == content, "A reads {} bytes of f", got.len());
            Ok(())
        })?;
        Ok(())
    })();
    if paused {
        let _ = a.resume();
    }
    let _ = std::fs::remove_file(&hold);
    // The scenario's own verdict first: a close still blocked keeps B's
    // mount busy, and the unmount error would hide why.
    let unmounted_b = b.unmount();
    let unmounted_a = a.unmount();
    result?;
    unmounted_b?;
    unmounted_a
}

/// M5 finding 1 end to end, the stale-base half of M6: plan 30 M5's
/// `stale-base-rename-divergence` (a rename accepted on a base the
/// requester has not applied waits for the log instead of installing a
/// shadow that the holder's unshipped unlink then removes). Run as is:
/// M6 changes the reply's position, not the rule.
pub fn session_stale_base_rename(seed: u64) -> Result<()> {
    super::m5::stale_base_rename_divergence(seed)
}

/// Read-your-writes across a holder kill. Before plan 43 C2f, C's forwarded
/// create was acknowledged by A from memory while A could not reach S3; A
/// was killed, B took over, and C's shadow was stranded until its replay
/// landed through B: C's listing had to show its own create throughout.
/// Since C2f a lone root (the rig's `CONSTELLATION_BACKUPS=0`) answers
/// another node only from the archive, so that shadow cannot arise (as in
/// `holder-crash-no-phantom-*`, [`super::unacked_create`]): C's create
/// blocks while A's S3 is cut, A is killed, B's write takes the lease over,
/// and C's create ends by its deadline or is executed by rid through B.
/// C lists the directory and stats `phantom` every 20 ms throughout: once
/// its create has returned success, every read shows it (read-your-writes);
/// the nodes agree on `phantom` as I4 has it
/// ([`super::check_create_outcome`]); C never held, stranded or replayed a
/// shadow, and no read timed out.
pub fn session_ryw_after_holder_kill(_seed: u64) -> Result<()> {
    const NAME: &str = "session-ryw-after-holder-kill";
    let (_env, _root, mut a, mut b, mut c, sw) =
        phantom_setup_with(NAME, &[("CONSTELLATION_SYNC_IDLE_MAX_MS", "1000")])?;
    let result = (|| -> Result<()> {
        let rolled_before = n(&speculation_of(&c)?, "rolled_back");
        let unsettled_before = super::unsettled_eio_of(&c)?;
        let create = super::unacked_create(&mut a, &c, &sw)?;
        // B's write takes over once A's lease runs out.
        let takeover = {
            let mnt = b.mnt.clone();
            std::thread::spawn(move || std::fs::write(mnt.join("b-after"), b"b"))
        };
        let deadline = Instant::now() + Duration::from_secs(150);
        let mut create = Some(create);
        let mut created: Option<std::io::Result<()>> = None;
        let mut created_at = None;
        let (mut reads, mut after_ack) = (0u64, 0u64);
        let mut missing = Vec::new();
        let mut slowest = Duration::ZERO;
        loop {
            if create.as_ref().is_some_and(|h| h.is_finished()) {
                let r = create
                    .take()
                    .expect("present")
                    .join()
                    .map_err(|_| anyhow::anyhow!("the create thread panicked"))?;
                eprintln!("    {NAME}: C's create returned {r:?}");
                created = Some(r);
                created_at = Some(Instant::now());
            }
            // Only reads started after the create returned success owe it.
            let acked = matches!(created, Some(Ok(())));
            // `ls` (a readdir always reaches the daemon; a `stat` is
            // mostly answered from the kernel's 1 s entry cache) and `stat`.
            let started = Instant::now();
            let listed = std::fs::read_dir(&c.mnt)?
                .filter_map(|e| e.ok())
                .any(|e| e.file_name() == "phantom");
            let stat = std::fs::metadata(c.mnt.join("phantom"));
            slowest = slowest.max(started.elapsed());
            reads += 1;
            if acked {
                after_ack += 1;
                let since = created_at.map(|t| t.elapsed()).unwrap_or_default();
                if !listed {
                    missing.push(format!("ls {since:?} after the ack"));
                }
                if let Err(e) = stat {
                    missing.push(format!("stat {since:?} after the ack: {e}"));
                }
            }
            if (created.is_some() && takeover.is_finished()) || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // Reads keep going a little after both ended: the window right
        // after the takeover's marker.
        if matches!(created, Some(Ok(()))) {
            for _ in 0..50 {
                let listed = std::fs::read_dir(&c.mnt)?
                    .filter_map(|e| e.ok())
                    .any(|e| e.file_name() == "phantom");
                if !listed {
                    missing.push("ls after the takeover".into());
                }
                if let Err(e) = std::fs::metadata(c.mnt.join("phantom")) {
                    missing.push(format!("stat after the takeover: {e}"));
                }
                after_ack += 1;
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let created = created.ok_or_else(|| {
            anyhow::anyhow!("C's create had not returned 150 s after A was killed")
        })?;
        takeover
            .join()
            .map_err(|_| anyhow::anyhow!("takeover thread panicked"))?
            .context("B's write (the takeover)")?;
        let session = session_of(&c)?;
        print_session(NAME, "C", &session);
        eprintln!(
            "    {NAME}: C listed + stat'ed {reads} times ({after_ack} after its create's \
             success), slowest {slowest:?}"
        );
        anyhow::ensure!(
            missing.is_empty(),
            "C's own acknowledged create was missing from its reads {} time(s): {missing:?}",
            missing.len()
        );
        let exists = super::phantom_agreement(&[("b", &b), ("c", &c)], Duration::from_secs(30))?;
        super::check_create_outcome(
            "c",
            &created,
            exists,
            super::unsettled_eio_of(&c)?.saturating_sub(unsettled_before),
        )?;
        super::no_shadow_ever(&c, rolled_before)?;
        anyhow::ensure!(
            n(&session_of(&c)?, "timeouts") == 0,
            "a read of the phantom timed out (degraded): {session}"
        );
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
///
/// B is not A's backup. The hold stops A's sync rounds only; a backup is
/// fed A's journal over P2P, before S3. When A took B as its backup inside
/// B's reads, B applied `g` from the stream and the rest of its reads were
/// rightly fast — the scenario failed "a degraded read took 1.3 s" (the
/// first, cut short by the stream; then µs) in 5 of 5 runs at load 50. It
/// then ran with `CONSTELLATION_BACKUPS=0`, which since plan 43 C2f makes
/// A a lone root that answers B's create only from the archive: with A's
/// sync held the refusal itself waited, and B's create ended in `EIO` at
/// its deadline. Now C backs A ([`backed_by_third`]): the refusal is
/// committed once C holds it, and B still cannot apply `g`.
pub fn session_wait_degrades(_seed: u64) -> Result<()> {
    const NAME: &str = "session-wait-degrades";
    const BUDGET_MS: u64 = 1_500;
    let tmp = tempfile::tempdir()?;
    let hold = tmp.path().join("hold-a");
    let (_env, _root, mut a, mut b, mut c) =
        backed_by_third(NAME, &[("CONSTELLATION_SESSION_WAIT_MS", "1500")], &hold)?;
    let result = (|| -> Result<()> {
        hold_sync(tmp.path(), &hold)?;
        std::fs::write(a.mnt.join("g"), b"gee")?;
        match create_new(&b.mnt, "g") {
            Err(e) if Code::from_os_error(&e) == Some(Code::Exists) => {}
            other => bail!("B's create of `g` returned {other:?}, expected EEXIST"),
        }
        // Names never looked up before (the kernel's 1 s entry cache would
        // answer a repeated lookup without asking the daemon).
        let mut elapsed = Vec::new();
        for i in 0..3 {
            let started = Instant::now();
            let r = std::fs::metadata(b.mnt.join(format!("absent-{i}")));
            elapsed.push((started.elapsed(), started));
            match r {
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                other => {
                    bail!("B's lookup of absent-{i} must answer ENOENT (degraded), got {other:?}")
                }
            }
        }
        let session = session_of(&b)?;
        print_session(NAME, "B", &session);
        eprintln!(
            "    {NAME}: B's degraded stats took {:?}",
            elapsed.iter().map(|e| e.0).collect::<Vec<_>>()
        );
        for (e, _) in &elapsed {
            // The budget is waited out (the protocol's lower bound: exact);
            // how far past it the answer came is latency (below).
            anyhow::ensure!(
                *e >= Duration::from_millis(BUDGET_MS - 100),
                "a degraded read took {e:?}, before the {BUDGET_MS} ms budget"
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
        for (e, at) in &elapsed {
            crate::hostlag::latency_bound(
                "a degraded read",
                *at,
                *e,
                Duration::from_millis(BUDGET_MS),
                Duration::from_millis(BUDGET_MS * 3),
            )?;
        }
        still_backed_by_c(&a, &c)?;
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
        // Recovered: no further session timeout (exact); fast (latency).
        anyhow::ensure!(
            n(&after, "timeouts") == n(&before, "timeouts"),
            "reads did not recover once A shipped: {fast:?} {after}"
        );
        crate::hostlag::latency_bound(
            "a read once A shipped",
            started,
            fast,
            Duration::ZERO,
            Duration::from_millis(500),
        )?;
        Ok(())
    })();
    let _ = std::fs::remove_file(&hold);
    b.unmount()?;
    c.unmount()?;
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
