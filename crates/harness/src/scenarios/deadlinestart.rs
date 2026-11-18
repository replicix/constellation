//! The protocol deadlines a daemon checks as it starts
//! (`engine::authority_driver::enforce_deadlines`): the defaults start as
//! configured, a read-delegation TTL past D11b is cut to fit and the daemon
//! starts, and a `Refuse`-class violation (D1: a read-delegation TTL not
//! above the expiry margin) stops the start with the invariant named.

use super::{setup, ts};
use crate::client::Client;
use crate::s3env::BUCKET;
use anyhow::{ensure, Result};
use std::time::Duration;

const MOUNT_WITHIN: Duration = Duration::from_secs(60);

pub(super) fn deadline_check_at_start(_seed: u64) -> Result<()> {
    let (env, root) = setup("deadline-check-at-start")?;
    let _proxy = env.s3_proxy()?;
    let backend = format!("s3://{BUCKET}/deadline-start-{}", ts());
    let mk = |name: &str| -> Result<Client> {
        Ok(Client::new(root.path(), name, &env.endpoint, &backend)?.with_own_node_key())
    };

    // The defaults: checked, nothing cut.
    let mut a = mk("dl-defaults")?;
    a.fs_create()?;
    a.mount_within(MOUNT_WITHIN)?;
    let log = a.log_text();
    a.unmount()?;
    ensure!(
        log.contains("protocol deadlines checked"),
        "defaults: no deadline check in the log:\n{}",
        a.tail_log_n(40)
    );
    ensure!(
        !log.contains("cut to fit"),
        "defaults: the read-delegation TTL was cut:\n{}",
        a.tail_log_n(40)
    );

    // R = 100 s: cut to 40.25 s (cap 84500 ms, 2R + 4000), and it starts.
    let mut b = mk("dl-long-read-ttl")?.with_env("CONSTELLATION_READ_DELEGATION_TTL_MS", "100000");
    b.mount_within(MOUNT_WITHIN)?;
    let log = b.log_text();
    b.unmount()?;
    ensure!(
        log.contains("running with the read-delegation TTL cut to fit") && log.contains("40250"),
        "long TTL: no cut to 40250 ms in the log:\n{}",
        b.tail_log_n(40)
    );

    // R = 1 s, not above the 1 s margin: refused, D1 named.
    let mut c = mk("dl-refused")?.with_env("CONSTELLATION_READ_DELEGATION_TTL_MS", "1000");
    match c.mount_within(MOUNT_WITHIN) {
        Ok(()) => {
            let tail = c.tail_log_n(40);
            c.unmount()?;
            anyhow::bail!("R = 1000 ms (D1) mounted:\n{tail}");
        }
        Err(e) => {
            let text = format!("{e:#}\n{}", c.log_text());
            ensure!(
                text.contains("the protocol deadlines refuse this configuration")
                    && text.contains("D1:"),
                "R = 1000 ms failed for another reason: {text}"
            );
        }
    }
    Ok(())
}
