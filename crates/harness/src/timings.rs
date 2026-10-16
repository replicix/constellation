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
        let lease_ttl = knob("CONSTELLATION_LEASE_TTL_MS", 60_000);
        let margin = 1_000u64.min(lease_ttl / 4);
        let lock_ttl = knob("CONSTELLATION_LOCK_TTL_MS", 20_000);
        let delegation_ttl = knob("CONSTELLATION_DELEGATION_TTL_MS", lock_ttl.max(5_000));
        let read_delegation_ttl = get("CONSTELLATION_READ_DELEGATION_TTL_MS")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(5_000)
            .max(1);
        let backup_takeover = knob("CONSTELLATION_BACKUP_TAKEOVER_MS", 1_500);
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

    /// The client op deadline (`acquire_deadline_ms`, twice the lease
    /// TTL): an op still in doubt then answers `EIO`.
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
    /// delegations were live, and two margins — capped by the
    /// predecessor's lease expiry, which this does not know (an upper
    /// bound, then).
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
        let short = Timings::from_pairs(&[("CONSTELLATION_LEASE_TTL_MS", "2000")]);
        assert_eq!(short.margin, ms(500));
    }

    /// Finding F4: the delegated handoff failover's 14.5 s.
    #[test]
    fn the_ack_floor_of_the_delegroot_scenarios() {
        let t = Timings::from_pairs(&[
            ("CONSTELLATION_LEASE_TTL_MS", "60000"),
            ("CONSTELLATION_DELEGATION_TTL_MS", "5000"),
            ("CONSTELLATION_BACKUP_RTT_BUDGET_MS", "50"),
        ]);
        assert_eq!(t.reclaim_horizon(), ms(6_000));
        assert_eq!(t.ack_floor_wait(true), ms(14_500));
        assert_eq!(t.ack_floor_wait(false), ms(8_500));
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
