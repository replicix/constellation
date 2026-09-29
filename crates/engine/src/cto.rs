//! Plan 30 §M8: `--cto bounded|strict` and the read-delegation knobs.
//!
//! The mechanism lives in `constellation_authority::core` (ReadIndex,
//! grants, recalls — see its `readindex` module doc), in
//! `constellation_meta::readdeleg` (the tables FUSE threads and the core
//! share) and in `view::View::strict_read` (the open, lookup
//! and readdir paths). This module only reads the configuration.
//!
//! - `--cto bounded` (default): an open reads the local replica, which
//!   follows the log within the visibility bound (M6's session guarantees
//!   still hold per node).
//! - `--cto strict`: an open, lookup or listing on a node that is not the
//!   sequencer sees every close another node completed before it began.
//!   The kernel's attribute and entry caches are off (TTL 0), so every
//!   open and path step reaches the daemon.
//! - `CONSTELLATION_CTO` supplies the default when the flag is absent.
//! - `CONSTELLATION_READ_DELEGATION_TTL_MS` (default 5000): how long a
//!   read delegation lasts; renewed in the background while in use.
//! - `CONSTELLATION_READ_DELEGATIONS=0`: this node, as sequencer, grants
//!   none (every strict read then costs a round trip).
//! - `CONSTELLATION_READ_INDEX_BUDGET_MS` (default 2000, like M6's
//!   session wait): how long a strict read waits for the sequencer's
//!   answer before it reads the replica anyway (degraded, counted).

use anyhow::{bail, Result};

/// `--cto` (or `CONSTELLATION_CTO`): whether this mount is strict.
pub fn strict_from(flag: Option<&str>) -> Result<bool> {
    let env = std::env::var("CONSTELLATION_CTO").ok();
    let raw = flag.or(env.as_deref()).unwrap_or("bounded");
    match raw.trim().to_ascii_lowercase().as_str() {
        "bounded" | "" => Ok(false),
        "strict" => Ok(true),
        other => bail!("invalid --cto {other:?} (expected bounded or strict)"),
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// `CONSTELLATION_READ_DELEGATION_TTL_MS` (default 5000).
pub fn read_delegation_ttl_ms() -> u64 {
    env_u64("CONSTELLATION_READ_DELEGATION_TTL_MS", 5_000).max(1)
}

/// The kernel attribute/entry TTL a *lone* strict sequencer answers with
/// (`view::View::ttl`): half the lease's drift margin, at
/// most 1 s. While no other node has shown itself, nothing but this
/// node's own FUSE writes can change what it serves, so the kernel may
/// cache — strict mode then costs a single node exactly what bounded
/// does. The first sign of another node (`ReadDelegations::leave_alone`)
/// turns the TTL to 0, and every acknowledgement of a mutation, and every
/// release, waits [`lone_kernel_drain_ms`] once, so no entry cached
/// before can outlive the switch into another node's close. Half the
/// margin: a lone reply is only given while the lease is usable, so the
/// last one expires before a taker could claim the lease (the same
/// argument as the lease's own margin, with slack for a FUSE thread
/// preempted between its check and its reply).
pub fn lone_kernel_ttl() -> std::time::Duration {
    std::time::Duration::from_millis((crate::lease::expiry_margin_ms() as u64 / 2).clamp(1, 1_000))
}

/// How long acknowledgements wait after a lone strict node learns it is
/// not alone: the lone TTL plus slack for a reply computed just before
/// the switch and delivered just after it.
pub fn lone_kernel_drain_ms() -> u64 {
    lone_kernel_ttl().as_millis() as u64 + 500
}

/// `CONSTELLATION_READ_INDEX_BUDGET_MS` (default 2000).
pub fn read_index_budget_ms() -> u64 {
    env_u64("CONSTELLATION_READ_INDEX_BUDGET_MS", 2_000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse() {
        assert!(!strict_from(Some("bounded")).unwrap());
        assert!(strict_from(Some("Strict")).unwrap());
        assert!(strict_from(Some("close-to-open")).is_err());
    }
}
