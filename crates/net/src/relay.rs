//! P2P relay policy (iroh `RelayMode`).
//!
//! Default is **disabled**: peers dial only addresses published in the
//! S3 node registry (typically LAN/VPN reachability). Opt into n0's
//! public relays or a self-hosted map via `CONSTELLATION_P2P_RELAY`.

use anyhow::{bail, Context, Result};
use iroh::{RelayMap, RelayMode, RelayUrl};

/// How this node uses iroh relays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayPolicy {
    /// No relays — registry direct addresses only (historical default).
    Disabled,
    /// n0 production public relays (`RelayMode::Default`).
    Default,
    /// Explicit relay URL list (self-hosted or a custom subset).
    Custom {
        urls: Vec<String>,
        /// Optional shared bearer token for `access.shared_token` relays.
        auth_token: Option<String>,
    },
}

impl RelayPolicy {
    /// Parse from the process environment.
    ///
    /// * `CONSTELLATION_P2P_RELAY` unset / empty / `off` / `disabled` → [`Self::Disabled`]
    /// * `default` / `public` / `n0` → [`Self::Default`]
    /// * one or more comma-separated URLs → [`Self::Custom`]
    /// * `CONSTELLATION_P2P_RELAY_TOKEN` → optional auth for custom maps
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("CONSTELLATION_P2P_RELAY").unwrap_or_default();
        let token = std::env::var("CONSTELLATION_P2P_RELAY_TOKEN")
            .ok()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        Self::parse(&raw, token)
    }

    pub fn parse(raw: &str, auth_token: Option<String>) -> Result<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty()
            || matches!(
                trimmed.to_ascii_lowercase().as_str(),
                "off" | "disabled" | "none" | "0" | "false"
            )
        {
            if auth_token.is_some() {
                tracing::warn!(
                    "CONSTELLATION_P2P_RELAY_TOKEN is set but relays are disabled; ignoring token"
                );
            }
            return Ok(Self::Disabled);
        }
        if matches!(
            trimmed.to_ascii_lowercase().as_str(),
            "default" | "public" | "n0"
        ) {
            if auth_token.is_some() {
                tracing::warn!(
                    "CONSTELLATION_P2P_RELAY_TOKEN applies only to custom relay URLs; ignoring"
                );
            }
            return Ok(Self::Default);
        }
        let urls: Vec<String> = trimmed
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if urls.is_empty() {
            bail!("CONSTELLATION_P2P_RELAY has no usable URLs");
        }
        for u in &urls {
            let _ = u
                .parse::<RelayUrl>()
                .with_context(|| format!("invalid relay URL {u:?}"))?;
        }
        Ok(Self::Custom { urls, auth_token })
    }

    /// Short label for status / logs.
    pub fn label(&self) -> String {
        match self {
            Self::Disabled => "disabled".into(),
            Self::Default => "default".into(),
            Self::Custom { urls, .. } => {
                if urls.len() == 1 {
                    urls[0].clone()
                } else {
                    format!("custom({} urls)", urls.len())
                }
            }
        }
    }

    pub fn to_iroh(&self) -> Result<RelayMode> {
        Ok(match self {
            Self::Disabled => RelayMode::Disabled,
            Self::Default => RelayMode::Default,
            Self::Custom { urls, auth_token } => {
                let relay_urls: Vec<RelayUrl> = urls
                    .iter()
                    .map(|u| {
                        u.parse::<RelayUrl>()
                            .with_context(|| format!("invalid relay URL {u:?}"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                match auth_token {
                    Some(token) => {
                        let map = RelayMap::from_iter(relay_urls).with_auth_token(token.clone());
                        RelayMode::Custom(map)
                    }
                    None => RelayMode::custom(relay_urls),
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_and_off_are_disabled() {
        assert_eq!(RelayPolicy::parse("", None).unwrap(), RelayPolicy::Disabled);
        assert_eq!(
            RelayPolicy::parse("off", None).unwrap(),
            RelayPolicy::Disabled
        );
        assert_eq!(
            RelayPolicy::parse("DISABLED", None).unwrap(),
            RelayPolicy::Disabled
        );
    }

    #[test]
    fn public_aliases_select_default() {
        for v in ["default", "public", "n0", "N0"] {
            assert_eq!(
                RelayPolicy::parse(v, None).unwrap(),
                RelayPolicy::Default,
                "{v}"
            );
        }
    }

    #[test]
    fn comma_separated_urls_are_custom() {
        let p = RelayPolicy::parse(
            "https://relay.example.com.,http://127.0.0.1:3340",
            Some("sekret".into()),
        )
        .unwrap();
        match &p {
            RelayPolicy::Custom { urls, auth_token } => {
                assert_eq!(urls.len(), 2);
                assert_eq!(auth_token.as_deref(), Some("sekret"));
            }
            other => panic!("expected custom, got {other:?}"),
        }
        assert!(p.to_iroh().is_ok());
    }

    #[test]
    fn garbage_url_is_rejected() {
        assert!(RelayPolicy::parse("not a url", None).is_err());
    }
}
