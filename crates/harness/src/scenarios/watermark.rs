//! EC2 campaign 7, finding B-2: a node whose every read "hangs" after a
//! fault phase, while its daemon is healthy and every other node reads
//! the same files at once.
//!
//! What campaign 7 saw was not a lost reply but a session watermark
//! nobody reaches: a lock grant carries the lock's floor, the join of
//! every releaser's frontier since the lock was first taken, and that
//! floor keeps naming delegation stream generations long after their
//! `Recall`. A replica that applied the `Recall` while running voids the
//! generation; one that restarted since (the committers, killed and
//! remounted over and over) has no memory of it, so the grant's position
//! is never reached and every read on the node waits the whole session
//! budget (2 s) before answering degraded — 288 timeouts of 292 reads on
//! the hung node. `git add`, `git fsck` and `cat` walk hundreds of paths:
//! hours.
//!
//! `lock-grant-dead-generation` builds exactly that: `d1` delegated to
//! `c`, written into by `c` and by `b` (whose forwarded write's reply
//! puts the generation into `b`'s frontier), the turn file locked and
//! released by `b` and by `a` (the floor now names the generation), the
//! delegation ended, `b` remounted (a fresh session), then `b` takes the
//! lock. The grant's position names the dead generation and the root's
//! journal position. Every lookup on `b` must then be fast: the
//! generation is void from the persisted delegation table (seeded at
//! open, or looked up before the first wait), the journal position is
//! reached through the persisted applied position, and anything the
//! store cannot account for is dropped after
//! `CONSTELLATION_SESSION_WATERMARK_TTL_MS`. Against a build without
//! the fix, the 21 lookups took 126 s and `session.timeouts` counted 69.

use super::m11::{
    cluster, deleg_of, delegate, dump_logs_on_failure, n, undelegate, unmount_all,
    wait_for_connected_peers, wait_installed,
};
use super::m9::node_id;
use super::{eventually, wait_for_p2p};
use crate::client::Client;
use anyhow::{Context, Result};
use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};

fn flock(f: &std::fs::File, op: libc::c_int) -> std::io::Result<()> {
    if unsafe { libc::flock(f.as_raw_fd(), op) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn turn_file(c: &Client) -> Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(c.mnt.join("turn"))
        .with_context(|| format!("{} opening the turn file", c.name))
}

/// Lock and release the turn file on `c` (the release carries `c`'s
/// session frontier to the lock's floor).
fn lock_and_release(c: &Client) -> Result<Duration> {
    let f = turn_file(c)?;
    let t = Instant::now();
    flock(&f, libc::LOCK_EX).with_context(|| format!("{} LOCK_EX", c.name))?;
    let took = t.elapsed();
    flock(&f, libc::LOCK_UN).with_context(|| format!("{} LOCK_UN", c.name))?;
    Ok(took)
}

fn session_of(c: &Client) -> Result<serde_json::Value> {
    Ok(c.control_status()?["session"].clone())
}

pub fn lock_grant_dead_generation(_seed: u64) -> Result<()> {
    const NAME: &str = "lock-grant-dead-generation";
    const FILES: usize = 20;
    let (_env, _root, mut clients, _proxies) = cluster(NAME, &["a", "b", "c"], &[], 0)?;
    let result = (|| -> Result<()> {
        let c_id = node_id(&clients[2])?;
        // b's write into d1 must go to the delegate c. A node's authority
        // core learns its links on a 1 s tick, and it sends an op under
        // another node's delegation to the root while it has no link to
        // that delegate yet; the root, not linked to c yet either,
        // recalls the delegation and executes it (the designed fallback
        // for an unreachable delegate). Right after the mounts, c had
        // enrolled a second before b's write: the generation ended under
        // the scenario, whose `undelegate` then found nothing delegated.
        let linked = {
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
            wait_for_connected_peers(&refs)?;
            Instant::now()
        };
        {
            let a = &clients[0];
            std::fs::create_dir(a.mnt.join("d1"))?;
            std::fs::write(a.mnt.join("turn"), b"")?;
            for x in &clients[1..] {
                eventually(
                    &format!("d1 and the turn file reach {}", x.name),
                    Duration::from_secs(30),
                    || {
                        anyhow::ensure!(x.mnt.join("d1").is_dir() && x.mnt.join("turn").is_file());
                        Ok(())
                    },
                )?;
            }
            delegate(a, "/d1", c_id)?;
        }
        let gen = wait_installed(&clients[2], "/d1", Duration::from_secs(30))?;
        eprintln!("    {NAME}: d1 delegated to c as generation {gen}");
        // The delegate's own writes advance the generation's stream; b's
        // forwarded write's reply carries the stream position into b's
        // session frontier.
        let mut names = Vec::new();
        for i in 0..FILES {
            let name = format!("d1/c-{i}");
            std::fs::write(clients[2].mnt.join(&name), name.as_bytes())?;
            names.push(name);
        }
        for x in &clients[..2] {
            eventually(
                &format!("{} applies the delegation of /d1", x.name),
                Duration::from_secs(30),
                || {
                    let d = deleg_of(x)?;
                    let table = d["table"].as_array().cloned().unwrap_or_default();
                    anyhow::ensure!(
                        table
                            .iter()
                            .any(|e| e["path"] == "/d1" && n(e, "gen") == gen),
                        "{}'s table: {table:?}",
                        x.name
                    );
                    Ok(())
                },
            )?;
        }
        // Two link ticks since every link was up: the cores route to c.
        std::thread::sleep(Duration::from_secs(2).saturating_sub(linked.elapsed()));
        std::fs::write(clients[1].mnt.join("d1/b-0"), b"d1/b-0")?;
        names.push("d1/b-0".into());
        for x in &clients {
            eventually(
                &format!("every d1 file reaches {}", x.name),
                Duration::from_secs(60),
                || {
                    for name in &names {
                        anyhow::ensure!(x.mnt.join(name).is_file(), "{name} missing");
                    }
                    Ok(())
                },
            )?;
        }
        // The lock's floor: b's release (its frontier names the
        // generation), then the root's own.
        let took_b = lock_and_release(&clients[1])?;
        let took_a = lock_and_release(&clients[0])?;
        eprintln!(
            "    {NAME}: the turn lock taken and released by b ({took_b:?}) and a ({took_a:?})"
        );
        // The generation ends. Every running replica voids it as it
        // applies the Recall; b is about to forget that.
        undelegate(&clients[0], "/d1")?;
        for x in &clients {
            eventually(
                &format!("the recall reaches {}", x.name),
                Duration::from_secs(30),
                || {
                    let d = deleg_of(x)?;
                    let table = d["table"].as_array().cloned().unwrap_or_default();
                    anyhow::ensure!(table.is_empty(), "{}'s table: {table:?}", x.name);
                    Ok(())
                },
            )?;
        }
        // b restarts: a fresh session, no memory of generation `gen`.
        clients[1].unmount().context("unmounting b")?;
        clients[1].mount().context("remounting b")?;
        {
            let refs: Vec<&Client> = clients.iter().collect();
            wait_for_p2p(&refs)?;
            wait_for_connected_peers(&refs)?;
        }
        let before = session_of(&clients[1])?;
        // b takes the lock: the grant's position is the floor, which
        // names the dead generation.
        let f = turn_file(&clients[1])?;
        let t = Instant::now();
        flock(&f, libc::LOCK_EX).context("b LOCK_EX after its restart")?;
        let grant = t.elapsed();
        // Every lookup on b must be fast now.
        let t = Instant::now();
        let mut slowest = Duration::ZERO;
        for name in &names {
            let t1 = Instant::now();
            std::fs::metadata(clients[1].mnt.join(name))
                .with_context(|| format!("b stat {name}"))?;
            slowest = slowest.max(t1.elapsed());
        }
        let lookups = t.elapsed();
        let content = std::fs::read(clients[1].mnt.join("d1/c-0"))?;
        anyhow::ensure!(content == b"d1/c-0", "b read d1/c-0 as {content:?}");
        flock(&f, libc::LOCK_UN)?;
        let after = session_of(&clients[1])?;
        let timeouts =
            after["timeouts"].as_u64().unwrap_or(0) - before["timeouts"].as_u64().unwrap_or(0);
        eprintln!(
            "    {NAME}: b's grant after its restart took {grant:?}; {} lookups took {lookups:?} (slowest {slowest:?}), \
             {timeouts} session timeouts; watermarks raised {} dropped {}, dependencies voided from the table {}",
            names.len(),
            after["raised"],
            after["abandoned"],
            after["voided_ended"],
        );
        anyhow::ensure!(
            lookups < Duration::from_secs(5) && slowest < Duration::from_millis(1_500),
            "lookups on b after the grant were slow: {lookups:?} in all, the slowest {slowest:?} \
             (a session watermark nobody reaches? {after})"
        );
        anyhow::ensure!(
            timeouts == 0,
            "{timeouts} session waits timed out on b after the grant: {after}"
        );
        anyhow::ensure!(
            after["raised"].as_u64().unwrap_or(0) >= 1,
            "the grant raised no watermark on b (the scenario tests nothing): {after}"
        );
        // And the cluster is whole: a write on b is seen everywhere.
        std::fs::write(clients[1].mnt.join("after"), b"after")?;
        for x in &clients {
            eventually(
                &format!("b's write after the grant reaches {}", x.name),
                Duration::from_secs(30),
                || {
                    anyhow::ensure!(std::fs::read(x.mnt.join("after"))? == b"after");
                    Ok(())
                },
            )?;
        }
        Ok(())
    })();
    dump_logs_on_failure(NAME, &clients, &result);
    unmount_all(&mut clients);
    result
}
