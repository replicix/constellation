//! The protocol timings a daemon runs with, derived from the
//! `CONSTELLATION_*` knobs the harness gives it, with the engine's own
//! defaults and formulas (`engine::authority_driver::load_config`,
//! `engine::lease::expiry_margin_ms`, `authority::core::Config::defaults`,
//! `core::backup::note_marker_landed`). Scenarios derive their bounds from
//! these instead of restating constants that drift (finding F4), and the
//! load injectors pause daemons for durations chosen around them
//! ([`Timings::boundaries`]).

use std::time::Duration;

/// One daemon's timings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timings {
    pub lease_ttl: Duration,
    /// `min(1 s, lease_ttl / 4)`.
    pub margin: Duration,
    pub lock_ttl: Duration,
    /// Write delegations' grant TTL (default: the lock TTL, at least 5 s).
    pub delegation_ttl: Duration,
    pub read_delegation_ttl: Duration,
    /// A listed backup's seal window.
    pub backup_takeover: Duration,
    /// The holder's "driver made no progress" bound (`holder_alive_task`).
    pub holder_stall: Duration,
}

fn ms(v: u64) -> Duration {
    Duration::from_millis(v)
}

impl Timings {
    /// From a lookup of the daemon's environment (`None`: unset).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Timings {
        // `env_ms`: a positive integer, else the default.
        let knob = |name: &str, default: u64| {
            get(name)
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(default)
        };
        let lease_ttl = knob("REPLICIX_LEASE_TTL_MS", 60_000);
        let margin = 1_000u64.min(lease_ttl / 4);
        // Cut to the lease TTL less two margins (deadline D8).
        let lock_ttl = knob("CONSTELLATION_LOCK_TTL_MS", 20_000)
            .min(lease_ttl.saturating_sub(2 * margin))
            .max(1);
        let delegation_ttl = knob("CONSTELLATION_DELEGATION_TTL_MS", lock_ttl.max(5_000));
        let backup_takeover = knob("CONSTELLATION_BACKUP_TAKEOVER_MS", 1_500);
        let configured_read_ttl = get("CONSTELLATION_READ_DELEGATION_TTL_MS")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5_000)
            .max(1);
        // Cut to what D11/D11b allow (`enforce_deadlines`, ADR-48): the TTL
        // the daemon runs with, which the scenario's guards must use.
        let strict =
            get("CONSTELLATION_CTO").is_some_and(|v| v.trim().eq_ignore_ascii_case("strict"));
        let mut cfg = constellation_authority::core::Config::defaults(1, 1);
        cfg.ttl_ms = lease_ttl;
        cfg.expiry_margin_ms = margin;
        cfg.lock_ttl_ms = lock_ttl;
        cfg.delegation_ttl_ms = delegation_ttl;
        cfg.forward_timeout_ms = knob("CONSTELLATION_FORWARD_TIMEOUT_MS", 500);
        cfg.backup_takeover_ms = backup_takeover;
        // The kernel drain of a strict mount (`cto::lone_kernel_drain_ms`).
        cfg.kernel_cache_ttl_ms = if strict {
            (margin / 2).clamp(1, 1_000) + 500
        } else {
            0
        };
        cfg.read_delegation_ttl_ms = configured_read_ttl;
        constellation_authority::deadlines::normalize(&mut cfg);
        let read_delegation_ttl =
            constellation_authority::deadlines::read_ttl_fitting_settle_cap(&cfg)
                .unwrap_or(configured_read_ttl);
        let holder_stall = knob("CONSTELLATION_HOLDER_STALL_MS", 15_000);
        Timings {
            lease_ttl: ms(lease_ttl),
            margin: ms(margin),
            lock_ttl: ms(lock_ttl),
            delegation_ttl: ms(delegation_ttl),
            read_delegation_ttl: ms(read_delegation_ttl),
            backup_takeover: ms(backup_takeover),
            holder_stall: ms(holder_stall),
        }
    }

    /// From `(name, value)` pairs, later pairs winning (a client's env as
    /// a scenario builds it).
    pub fn from_pairs<K: AsRef<str>, V: AsRef<str>>(pairs: &[(K, V)]) -> Timings {
        Self::from_lookup(|name| {
            pairs
                .iter()
                .rev()
                .find(|(k, _)| k.as_ref() == name)
                .map(|(_, v)| v.as_ref().to_string())
        })
    }

    /// The client op deadline (`Deadlines::client_deadline`, twice
    /// the lease TTL): an op still in doubt then answers `EIO`.
    pub fn client_deadline(&self) -> Duration {
        2 * self.lease_ttl
    }

    /// The root's reclaim horizon of a write delegation
    /// (`delegation_ttl + margin`).
    pub fn reclaim_horizon(&self) -> Duration {
        self.delegation_ttl + self.margin
    }

    /// How long a successor that took an unexpired lease over from a
    /// tenure marked granting waits, from its marker, before it
    /// acknowledges a mutation (`note_marker_landed`): the takeover
    /// window, the read-delegation TTL, the reclaim horizon when write
    /// delegations were live, and two margins — exactly, whatever the
    /// predecessor's lease had left (the floor no longer takes the
    /// minimum with its expiry, a stamp of another node's clock).
    pub fn ack_floor_wait(&self, delegations_live: bool) -> Duration {
        self.backup_takeover
            + self.read_delegation_ttl
            + if delegations_live {
                self.reclaim_horizon()
            } else {
                Duration::ZERO
            }
            + 2 * self.margin
    }

    /// The durations a pause should straddle (load campaign §5.1 item 2),
    /// by name: `takeover`, `lock-half` (`(lock_ttl - margin) / 2`, a
    /// holder's renewal point), `lock-lapse` (`lock_ttl - margin`, where a
    /// holder stops honouring its grant), `lock-outwait` (`lock_ttl +
    /// margin`, where the owner outwaits it), the same for delegations,
    /// `holder-stall`, `lease-half` and `lease-ttl`.
    pub fn boundaries(&self) -> Vec<(&'static str, Duration)> {
        let lock_lapse = self.lock_ttl.saturating_sub(self.margin);
        let deleg_lapse = self.delegation_ttl.saturating_sub(self.margin);
        vec![
            ("takeover", self.backup_takeover),
            ("lock-half", lock_lapse / 2),
            ("lock-lapse", lock_lapse),
            ("lock-outwait", self.lock_ttl + self.margin),
            ("deleg-half", deleg_lapse / 2),
            ("deleg-lapse", deleg_lapse),
            ("deleg-outwait", self.delegation_ttl + self.margin),
            ("holder-stall", self.holder_stall),
            ("lease-half", self.lease_ttl / 2),
            ("lease-ttl", self.lease_ttl),
        ]
    }

    /// A boundary by name.
    pub fn boundary(&self, name: &str) -> Option<Duration> {
        self.boundaries()
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, d)| d)
    }

    /// `<boundary>[+|-<ms>]` or `<ms>`: a pause length.
    pub fn duration_of(&self, spec: &str) -> Option<Duration> {
        if let Ok(v) = spec.trim().parse::<u64>() {
            return Some(ms(v));
        }
        let (name, delta) = match spec.find(['+', '-']) {
            // A boundary name has `-` in it: split at the last sign.
            Some(_) => {
                let i = spec.rfind(['+', '-'])?;
                match spec[i + 1..].parse::<u64>() {
                    Ok(d) => {
                        let d = d as i64;
                        (&spec[..i], if &spec[i..i + 1] == "-" { -d } else { d })
                    }
                    Err(_) => (spec, 0),
                }
            }
            None => (spec, 0),
        };
        let base = self.boundary(name)?.as_millis() as i64;
        Some(ms((base + delta).max(0) as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_the_engine() {
        let t = Timings::from_pairs::<&str, &str>(&[]);
        assert_eq!(t.lease_ttl, ms(60_000));
        assert_eq!(t.margin, ms(1_000));
        assert_eq!(t.client_deadline(), ms(120_000));
        assert_eq!(t.lock_ttl, ms(20_000));
        // The lock TTL, at least the core's 5 s.
        assert_eq!(t.delegation_ttl, ms(20_000));
        assert_eq!(t.read_delegation_ttl, ms(5_000));
        assert_eq!(t.backup_takeover, ms(1_500));
        let short = Timings::from_pairs(&[("REPLICIX_LEASE_TTL_MS", "2000")]);
        assert_eq!(short.margin, ms(500));
    }

    /// Finding F4: the delegated handoff failover's 14.5 s.
    #[test]
    fn the_ack_floor_of_the_delegroot_scenarios() {
        let t = Timings::from_pairs(&[
            ("REPLICIX_LEASE_TTL_MS", "60000"),
            ("CONSTELLATION_DELEGATION_TTL_MS", "5000"),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50"),
        ]);
        assert_eq!(t.reclaim_horizon(), ms(6_000));
        assert_eq!(t.ack_floor_wait(true), ms(14_500));
        assert_eq!(t.ack_floor_wait(false), ms(8_500));
    }

    /// The read-delegation TTL is cut as the daemon cuts it
    /// (`enforce_deadlines`): the defaults pass, a long one is cut to 40.25 s,
    /// the 1 s lease cuts the default, and the m11 strict cluster (lease 20 s,
    /// write-delegation TTL 3 s) cuts 30 s to 11.75 s.
    #[test]
    fn the_read_ttl_is_cut_to_fit_the_settle_cap() {
        let t = Timings::from_pairs::<&str, &str>(&[]);
        assert_eq!(t.read_delegation_ttl, ms(5_000));
        let t = Timings::from_pairs(&[("CONSTELLATION_READ_DELEGATION_TTL_MS", "100000")]);
        assert_eq!(t.read_delegation_ttl, ms(40_250));
        let t = Timings::from_pairs(&[("REPLICIX_LEASE_TTL_MS", "1000")]);
        assert_eq!(t.read_delegation_ttl, ms(3_625));
        let m11 = [
            ("REPLICIX_LEASE_TTL_MS", "20000"),
            ("CONSTELLATION_DELEGATION_TTL_MS", "3000"),
            ("CONSTELLATION_CTO", "strict"),
            ("CONSTELLATION_READ_DELEGATION_TTL_MS", "30000"),
        ];
        assert_eq!(Timings::from_pairs(&m11).read_delegation_ttl, ms(11_750));
        // Write-delegation TTL 39.5 s: 30 s fits.
        let mut fits = m11.to_vec();
        fits[1].1 = "39500";
        assert_eq!(Timings::from_pairs(&fits).read_delegation_ttl, ms(30_000));
    }

    #[test]
    fn the_lock_ttl_is_cut_to_fit_the_lease() {
        let t = Timings::from_pairs(&[("REPLICIX_LEASE_TTL_MS", "20000")]);
        assert_eq!(t.lock_ttl, ms(18_000));
        assert_eq!(t.delegation_ttl, ms(18_000));
        let t = Timings::from_pairs(&[("REPLICIX_LEASE_TTL_MS", "22000")]);
        assert_eq!(t.lock_ttl, ms(20_000));
    }

    #[test]
    fn boundaries_and_durations() {
        let t = Timings::from_pairs(&[("CONSTELLATION_LOCK_TTL_MS", "3000")]);
        assert_eq!(t.boundary("lock-lapse"), Some(ms(2_000)));
        assert_eq!(t.boundary("lock-half"), Some(ms(1_000)));
        assert_eq!(t.boundary("lock-outwait"), Some(ms(4_000)));
        assert_eq!(t.duration_of("lock-lapse+500"), Some(ms(2_500)));
        assert_eq!(t.duration_of("lock-lapse-500"), Some(ms(1_500)));
        assert_eq!(t.duration_of("takeover"), Some(ms(1_500)));
        assert_eq!(t.duration_of("lease-ttl+1000"), Some(ms(61_000)));
        assert_eq!(t.duration_of("750"), Some(ms(750)));
        assert_eq!(t.duration_of("nonsense"), None);
    }
}
