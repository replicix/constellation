//! Plan 43 C7b (commit 9): a lease is claimed by observation, whatever the
//! claimant's clock says.
//!
//! - `lease-claimant-ahead`: three nodes with no backup, the lease at `a`,
//!   and `b` running a clock 90 s ahead (beyond the 20 s lease TTL; on its
//!   own clock `a`'s lease is long expired). `a` and `b` write into one
//!   filesystem under the ledger (`b` forwards to `a`). For 1.5 TTL `a`
//!   stays the holder at one epoch and `b` and `c` never hold. Then `a` is
//!   killed; before C7b `b` read an expiry 90 s in its past and claimed at
//!   once, now a
//!   claim follows only once a node has watched the lease key stand still
//!   for a TTL on its own wait clock (no earlier than half a TTL after the
//!   kill, the holder's last renewal being up to half a TTL before it), and
//!   within two and a half TTLs of it (the protocol's part is a TTL from the
//!   first read after the kill, which follows the kill by the forward
//!   timeout, and two read cadences; the rest is host slack, judged against
//!   the host-lag probe); the time is printed.
//! - `lease-cold-claim` (gate G6): a lone holder is killed and a node that
//!   was not running starts afterwards. A cold claimant has seen no
//!   renewal, so it claims one full TTL after its first read of the lease
//!   (the accepted cost of the observation rule), not at the dead holder's
//!   expiry: crash-to-claim and start-to-claim are printed and bounded
//!   from both sides.

use super::campaign_load::{ledger_converges, ledger_writer};
use super::m9::{cluster, TTL_MS};
use super::{eventually, lease_of};
use crate::client::Client;
use crate::verify::{ErrnoLog, Evidence, Ledger};
use anyhow::{bail, Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The longest wait between two reads of a lease an op is waiting for (the
/// retry backoff's cap).
const READ_CADENCE: Duration = Duration::from_secs(2);

/// No backup peer (no seal path: every takeover here is a TTL claim) and a
/// holder that keeps its lease while others write.
const NO_BACKUP: &[(&str, &str)] = &[
    ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "0"),
    ("CONSTELLATION_LEASE_WANTED_GRACE_MS", "600000"),
];

/// How long node `b`'s clock runs ahead of true time (ms): more than the
/// lease TTL, so on `b`'s clock any lease it reads has long expired.
const AHEAD_MS: i64 = 90_000;

fn ttl() -> Duration {
    Duration::from_millis(TTL_MS)
}

/// The first of `clients` (by index) that holds the lease with an epoch above
/// `after`, polling every 100 ms until `within`: its index and the instant.
fn first_holder_above(
    clients: &[Client],
    candidates: &[usize],
    after: u64,
    within: Duration,
) -> Result<(usize, Instant)> {
    let until = Instant::now() + within;
    loop {
        for &i in candidates {
            let lease = lease_of(&clients[i])?;
            if lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > after {
                return Ok((i, Instant::now()));
            }
        }
        if Instant::now() >= until {
            let seen: Vec<String> = candidates
                .iter()
                .map(|&i| {
                    format!(
                        "{}: {}",
                        clients[i].name,
                        lease_of(&clients[i]).unwrap_or_default()
                    )
                })
                .collect();
            bail!(
                "no node claimed the lease within {within:?}: {}",
                seen.join("; ")
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Raises the flags when dropped.
struct StopOnDrop(Vec<Arc<AtomicBool>>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        for f in &self.0 {
            f.store(true, Ordering::Relaxed);
        }
    }
}

/// See the module docs.
pub(super) fn lease_claimant_ahead(_seed: u64) -> Result<()> {
    const NAME: &str = "lease-claimant-ahead";
    crate::load::set_scenario_clock_offset("b", AHEAD_MS);
    let (env, _root, mut clients, _) = cluster(NAME, &["a", "b", "c"], NO_BACKUP, 0)?;
    let result = (|| -> Result<()> {
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        anyhow::ensure!(
            lease_of(&clients[0])?["held"] == true,
            "a does not hold the lease: {}",
            lease_of(&clients[0])?
        );
        let (ledger, errnos) = (
            Ledger::with_client_deadline(super::m9::client_deadline()),
            ErrnoLog::new(),
        );
        let stop_a = Arc::new(AtomicBool::new(false));
        let stop_b = Arc::new(AtomicBool::new(false));
        // The writers stop on every way out, or the mounts stay busy.
        let _stop = StopOnDrop(vec![stop_a.clone(), stop_b.clone()]);
        let writer = |c: &Client, stop: &Arc<AtomicBool>| {
            let (mnt, name, l, e, stop) = (
                c.mnt.clone(),
                c.name.clone(),
                ledger.clone(),
                errnos.clone(),
                stop.clone(),
            );
            std::thread::spawn(move || {
                ledger_writer(&mnt, &format!("w-{name}"), &name, &l, &e, &stop)
            })
        };
        let wa = writer(&clients[0], &stop_a);
        let wb = writer(&clients[1], &stop_b);
        std::thread::sleep(Duration::from_secs(3));

        // `b` forwards its writes to `a` (P2P is up, so it does not need the
        // lease), `a` renews every half TTL, and on `b`'s clock any lease it
        // reads has long expired: for 1.5 TTL `a` keeps its lease at one epoch
        // and `b` and `c` never hold it.
        let live = ttl() * 3 / 2;
        let t0 = Instant::now();
        eprintln!(
            "    {NAME}: a holds epoch {epoch} for {live:?}; b's clock is {AHEAD_MS} ms ahead"
        );
        while t0.elapsed() < live {
            for c in &clients {
                let lease = lease_of(c)?;
                if c.name == "a" {
                    anyhow::ensure!(
                        lease["held"] == true
                            && lease["lost"] == false
                            && lease["epoch"].as_u64() == Some(epoch),
                        "a lost or left its lease at {:?}, while it renewed: {lease}",
                        t0.elapsed()
                    );
                } else {
                    anyhow::ensure!(
                        lease["held"] == false,
                        "{} claimed a live lease {:?} in: {lease}",
                        c.name,
                        t0.elapsed()
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }

        // `a` is killed with `b` and `c` as they are: `b` was reading the
        // lease; whoever claims has to watch it stand still for a TTL.
        stop_a.store(true, Ordering::Relaxed);
        wa.join()
            .map_err(|_| anyhow::anyhow!("a's writer panicked"))?;
        let killed = Instant::now();
        clients[0].kill9()?;
        let (who, at) = first_holder_above(&clients, &[1, 2], epoch, ttl() * 3)?;
        let took = at - killed;
        eprintln!(
            "    {NAME}: a killed; {} claimed the lease {took:?} later (TTL {:?}, read cadence \
             {READ_CADENCE:?})",
            clients[who].name,
            ttl()
        );
        // The lease key last moved up to half a TTL before the kill (a
        // renews every half TTL) and a claimant has to see it stand still
        // for a TTL: not before half a TTL after the kill (a claim by the
        // 90 s-ahead clock would have been at once).
        anyhow::ensure!(
            took + Duration::from_secs(1) >= ttl() / 2,
            "{} claimed {took:?} after the holder was killed, before half a TTL ({:?}): a claim by \
             the claimant's own clock, not by its observation",
            clients[who].name,
            ttl() / 2
        );
        crate::hostlag::latency_bound(
            "a claim after the holder's death",
            killed,
            took,
            ttl() + 2 * READ_CADENCE,
            ttl() * 5 / 2,
        )?;

        stop_b.store(true, Ordering::Relaxed);
        let (slowest, _, ops) = wb
            .join()
            .map_err(|_| anyhow::anyhow!("b's writer panicked"))?;
        eprintln!("    {NAME}: b wrote {ops} payloads, slowest {slowest:?}");
        ledger_converges(NAME, &clients[1..], &ledger)?;
        let refs: Vec<&Client> = clients[1..].iter().collect();
        let rep = errnos.judge(&Evidence::from_clients(&refs, super::m9::client_deadline()));
        anyhow::ensure!(
            rep.bugs.is_empty(),
            "errors outside the documented set: {}",
            rep.bugs.join("; ")
        );
        let prefix = clients[1]
            .backend
            .strip_prefix(&format!("s3://{}/", crate::s3env::BUCKET))
            .context("backend outside the harness bucket")?
            .to_string();
        crate::verify::s3_log_check(&env.direct_endpoint, &prefix).map(|_| ())
    })();
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}

/// See the module docs.
pub(super) fn lease_cold_claim(_seed: u64) -> Result<()> {
    const NAME: &str = "lease-cold-claim";
    let (_env, _root, mut clients, _) = cluster(NAME, &["a", "b"], NO_BACKUP, 0)?;
    let result = (|| -> Result<()> {
        let epoch = lease_of(&clients[0])?["epoch"].as_u64().unwrap_or(0);
        anyhow::ensure!(lease_of(&clients[0])?["held"] == true, "a holds no lease");
        // `b` stops (it was only there to be a node that is not running),
        // the lone holder dies, and `b` starts cold afterwards.
        clients[1].unmount()?;
        std::thread::sleep(Duration::from_secs(1));
        let killed = Instant::now();
        clients[0].kill9()?;
        std::thread::sleep(Duration::from_secs(2));
        let started = Instant::now();
        clients[1].mount()?;
        let mounted = Instant::now();
        let mnt = clients[1].mnt.clone();
        let write = std::thread::spawn(move || {
            std::fs::write(mnt.join("after-the-crash"), b"cold").map(|_| Instant::now())
        });
        let (who, claimed) = first_holder_above(&clients, &[1], epoch, ttl() * 3)?;
        let wrote = write
            .join()
            .map_err(|_| anyhow::anyhow!("the write panicked"))?
            .context("the write on the cold node")?;
        let (crash_to_claim, start_to_claim, mount_to_claim) =
            (claimed - killed, claimed - started, claimed - mounted);
        eprintln!(
            "    {NAME}: {} claimed {crash_to_claim:?} after the crash, {start_to_claim:?} after \
             its start ({mount_to_claim:?} after its mount; its write returned {:?} later): TTL \
             {:?}",
            clients[who].name,
            wrote.saturating_duration_since(claimed),
            ttl()
        );
        // A full TTL from its first read, which cannot be before its start;
        // by the dead holder's expiry it would have claimed up to half a
        // TTL sooner than that.
        anyhow::ensure!(
            start_to_claim + Duration::from_secs(1) >= ttl(),
            "the cold claimant took the lease {start_to_claim:?} after it started, before a full \
             TTL ({:?}): it claimed on the dead holder's expiry, not on its own observation",
            ttl()
        );
        crate::hostlag::latency_bound(
            "a cold claim after the holder's death",
            mounted,
            mount_to_claim,
            ttl() + 2 * READ_CADENCE,
            ttl() * 2,
        )?;
        anyhow::ensure!(
            std::fs::read(clients[1].mnt.join("after-the-crash"))? == b"cold",
            "the cold node's write is not there"
        );
        anyhow::ensure!(
            std::fs::read(clients[1].mnt.join("f"))? == b"initial",
            "the dead holder's file is gone"
        );
        eventually("b is the holder", Duration::from_secs(10), || {
            let lease = lease_of(&clients[1])?;
            anyhow::ensure!(
                lease["held"] == true && lease["epoch"].as_u64().unwrap_or(0) > epoch,
                "{lease}"
            );
            Ok(())
        })
    })();
    super::m11::dump_logs_on_failure(NAME, &clients, &result);
    for c in clients.iter_mut().rev() {
        let _ = c.unmount();
    }
    result
}
