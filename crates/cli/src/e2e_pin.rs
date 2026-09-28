//! Local trust-on-first-use pin of a filesystem's E2E state.
//!
//! `meta.json` is plain, unauthenticated JSON on the backend, and E2E
//! exists to protect data from whoever controls that backend — who can
//! also rewrite `meta.json`: clear `e2e` so the next mount writes
//! plaintext, or swap the keyring block. So the E2E state is not taken
//! from `meta.json` alone. This machine remembers, per filesystem (keyed
//! like the registry's endpoint record: by name, or by backend URL for an
//! unregistered target), whether it was E2E and a fingerprint of its
//! master key. Every later load is held against that pin in two steps:
//!
//! * before any prompt or key derivation, `meta.json`'s `e2e` must not
//!   have gone from `true` to `false` ([`check`]);
//! * after the passphrase has opened the keyring, the master key it
//!   opened to must be the pinned one ([`PinCheck::confirm`]).
//!
//! The fingerprint is of the *master key*, not of the keyring block:
//! `fs passwd` rewraps the same master under a new passphrase and must
//! not read as tampering on every other machine, while a keyring block
//! spliced in from another filesystem (same passphrase reused) opens to
//! a different master and is refused before any key is used. A swapped
//! block the passphrase does not open fails the unlock itself.
//!
//! Either violation is refused unless the operator accepts it with
//! [`ACCEPT_ENV`], which re-pins. Switching a pinned plaintext filesystem
//! to E2E is an upgrade and passes.
//!
//! The pins live in their own file next to `registry.toml` (the registry
//! rewrites its rows from its own schema, so it cannot carry them), with
//! the same lock-then-rename discipline.

use anyhow::{bail, Context, Result};
use constellation_store_s3::{E2eKeys, FsMeta};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::registry::Registry;
use crate::target::Target;

/// Set to `1` to accept an E2E downgrade or master-key change and re-pin.
pub const ACCEPT_ENV: &str = "CONSTELLATION_ACCEPT_E2E_CHANGE";

/// What this machine last trusted about one filesystem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct E2ePin {
    /// The backend URL the pin was taken against. A name repointed at a
    /// different URL is a different filesystem and starts unpinned, as
    /// the registry's endpoint record does.
    pub s3: String,
    pub e2e: bool,
    /// [`E2eKeys::pin_fingerprint`] of the master key; `None` unless `e2e`,
    /// and `None` until the first unlock after `fs create`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub master_fingerprint: Option<String>,
}

impl E2ePin {
    /// The pin `meta` (read from `s3`) produces, with the master key
    /// fingerprint once `keys` has opened it.
    pub fn observe(s3: &str, meta: &FsMeta, keys: Option<&E2eKeys>) -> Self {
        E2ePin {
            s3: s3.to_string(),
            e2e: meta.e2e,
            master_fingerprint: keys
                .filter(|_| meta.e2e)
                .map(|keys| keys.pin_fingerprint(&meta.uuid.to_string())),
        }
    }
}

/// How a freshly read `meta.json` relates to the pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Nothing pinned for this filesystem (or pinned at another URL).
    Unpinned,
    Same,
    /// Pinned plaintext, now E2E.
    Upgrade,
    /// Pinned E2E, now plaintext.
    Downgrade,
    /// Pinned E2E, still E2E, but the keyring opened to another master.
    MasterChanged,
}

/// The pre-unlock comparison: E2E flags only.
pub fn compare(pinned: Option<&E2ePin>, observed: &E2ePin) -> Verdict {
    let Some(pinned) = pinned.filter(|pin| pin.s3 == observed.s3) else {
        return Verdict::Unpinned;
    };
    match (pinned.e2e, observed.e2e) {
        (true, false) => Verdict::Downgrade,
        (false, true) => Verdict::Upgrade,
        _ => Verdict::Same,
    }
}

/// The post-unlock comparison: the pinned master, if any, against the one
/// the passphrase opened.
pub fn compare_master(pinned: Option<&E2ePin>, observed: &E2ePin) -> Verdict {
    match compare(pinned, observed) {
        Verdict::Same if observed.e2e => {
            let pinned = pinned.and_then(|pin| pin.master_fingerprint.as_deref());
            match (pinned, observed.master_fingerprint.as_deref()) {
                (Some(a), Some(b)) if a != b => Verdict::MasterChanged,
                _ => Verdict::Same,
            }
        }
        other => other,
    }
}

/// Which pin a command reads and writes.
#[derive(Debug, Clone)]
pub struct PinTarget {
    key: String,
    s3: String,
}

impl PinTarget {
    pub fn named(name: &str, s3: &str) -> Self {
        PinTarget {
            key: name.to_string(),
            s3: s3.to_string(),
        }
    }

    /// An unregistered target is keyed by its URL. Names never contain
    /// `/`, and a backend URL always does, so the two cannot collide.
    pub fn unnamed(s3: &str) -> Self {
        Self::named(s3, s3)
    }

    pub fn for_target(target: &Target, s3: &str) -> Self {
        match target {
            Target::Named { name, .. } => Self::named(name, s3),
            Target::Raw(_) => Self::unnamed(s3),
        }
    }
}

/// `registry.toml`'s sibling `registry.e2e.toml`.
pub fn pins_path() -> Result<PathBuf> {
    Ok(Registry::path()?.with_extension("e2e.toml"))
}

fn accept_change() -> bool {
    std::env::var(ACCEPT_ENV).is_ok_and(|value| value == "1")
}

/// The outcome of [`check`], to [`confirm`](PinCheck::confirm) once the
/// passphrase has opened the keyring (or at once for plaintext).
#[derive(Debug)]
pub struct PinCheck {
    path: Option<PathBuf>,
    target: PinTarget,
    pinned: Option<E2ePin>,
    observed: E2ePin,
    uuid: String,
    accept: bool,
}

/// Compare `meta`'s E2E flag with the pin. Call before deciding whether to
/// prompt for a passphrase or deriving any key; an error here is the
/// refusal of a downgrade.
pub fn check(target: &PinTarget, meta: &FsMeta) -> Result<PinCheck> {
    match pins_path() {
        Ok(path) => check_at(Some(path), target, meta, accept_change()),
        Err(error) => {
            tracing::warn!(
                %error,
                "cannot locate the E2E pin store; meta.json is not checked against a pin"
            );
            check_at(None, target, meta, false)
        }
    }
}

fn check_at(
    path: Option<PathBuf>,
    target: &PinTarget,
    meta: &FsMeta,
    accept: bool,
) -> Result<PinCheck> {
    let observed = E2ePin::observe(&target.s3, meta, None);
    let pinned = match &path {
        Some(path) => read(path)?.remove(&target.key),
        None => None,
    };
    let check = PinCheck {
        path,
        target: target.clone(),
        pinned,
        observed,
        uuid: meta.uuid.to_string(),
        accept,
    };
    if compare(check.pinned.as_ref(), &check.observed) == Verdict::Downgrade {
        check.refuse_or_warn(format!(
            "meta.json at {} says {} is NOT end-to-end encrypted, but it was end-to-end \
             encrypted when this machine last used it",
            target.s3, target.key
        ))?;
    }
    Ok(check)
}

fn short(fingerprint: Option<&str>) -> &str {
    fingerprint.map_or("none", |fp| &fp[..fp.len().min(16)])
}

impl PinCheck {
    fn refuse_or_warn(&self, what: String) -> Result<()> {
        if !self.accept {
            let path = self.path.as_deref().unwrap_or(Path::new("?"));
            bail!(
                "{what}. meta.json is not authenticated, so whoever controls the storage may \
                 have tampered with it; refusing to continue. If the change is yours (the \
                 filesystem was recreated at this URL), rerun with {ACCEPT_ENV}=1 to accept it \
                 and re-pin (pins: {})",
                path.display()
            );
        }
        tracing::warn!("{what}; accepted by {ACCEPT_ENV}=1, re-pinning");
        Ok(())
    }

    /// Hold the master key `keys` opened against the pin (`None` for a
    /// plaintext filesystem), then record what was checked. An error is
    /// the refusal of a changed master: the keys must not be used. A
    /// keyring the passphrase does not open never gets here, so a swapped
    /// block is never pinned. Recording is best-effort: a failure leaves
    /// the old pin in place.
    pub fn confirm(&self, keys: Option<&E2eKeys>) -> Result<()> {
        let mut observed = self.observed.clone();
        observed.master_fingerprint = keys
            .filter(|_| observed.e2e)
            .map(|keys| keys.pin_fingerprint(&self.uuid));
        let verdict = compare_master(self.pinned.as_ref(), &observed);
        if verdict == Verdict::MasterChanged {
            self.refuse_or_warn(format!(
                "the E2E keyring in meta.json at {} opened to a master key (fingerprint {}) \
                 other than the one this machine pinned for {} (fingerprint {})",
                self.target.s3,
                short(observed.master_fingerprint.as_deref()),
                self.target.key,
                short(
                    self.pinned
                        .as_ref()
                        .and_then(|pin| pin.master_fingerprint.as_deref())
                ),
            ))?;
        }
        let changed = match verdict {
            Verdict::Unpinned => observed.e2e,
            Verdict::Same => {
                // A pin from `fs create` has no fingerprint yet.
                observed.master_fingerprint.is_some()
                    && self
                        .pinned
                        .as_ref()
                        .is_some_and(|pin| pin.master_fingerprint.is_none())
            }
            Verdict::Upgrade | Verdict::Downgrade | Verdict::MasterChanged => true,
        };
        if changed {
            if let Some(path) = &self.path {
                if let Err(error) = write(path, &self.target.key, &observed) {
                    tracing::warn!(key = %self.target.key, %error, "recording the E2E pin failed");
                }
            }
        }
        Ok(())
    }
}

/// Pin `meta` unconditionally: `fs create`, which just wrote it (`keys`
/// `None`: the first unlock adds the fingerprint), and `fs passwd`, which
/// just rewrapped and reopened it. Best-effort.
pub fn record(target: &PinTarget, meta: &FsMeta, keys: Option<&E2eKeys>) {
    let pin = E2ePin::observe(&target.s3, meta, keys);
    let saved = pins_path().and_then(|path| write(&path, &target.key, &pin));
    if let Err(error) = saved {
        tracing::warn!(key = %target.key, %error, "recording the E2E pin failed");
    }
}

fn read(path: &Path) -> Result<BTreeMap<String, E2ePin>> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text).with_context(|| format!("parsing {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Read-modify-write under an exclusive lock, then write-then-rename, as
/// the registry does.
fn write(path: &Path, key: &str, pin: &E2ePin) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path.with_extension("toml.lock"))
        .context("opening E2E pin lock file")?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: `lock_file` owns this open fd for the duration of the
        // call; `flock` does not touch memory.
        let rc = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error()).context("locking E2E pin file");
        }
    }
    let mut pins = read(path)?;
    pins.insert(key.to_string(), pin.clone());
    let text = toml::to_string_pretty(&pins).context("serializing E2E pins")?;
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming into place: {}", path.display()))?;
    drop(lock_file);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use constellation_store_s3::e2e::rewrap_master;
    use constellation_store_s3::{create_keyring_block, unlock, SharedE2eKeys};

    const S3: &str = "s3://bucket/prefix";

    /// An E2E `meta.json` with a real keyring, and the keys that open it.
    fn e2e_meta(passphrase: &str) -> (FsMeta, SharedE2eKeys) {
        let mut meta = FsMeta::new(1024 * 1024, "raw");
        meta.e2e = true;
        let block = create_keyring_block(passphrase).unwrap();
        let keys = unlock(&block, passphrase).unwrap();
        meta.keyring = Some(block);
        (meta, keys)
    }

    fn pin(e2e: bool, fingerprint: Option<&str>) -> E2ePin {
        E2ePin {
            s3: S3.into(),
            e2e,
            master_fingerprint: fingerprint.map(str::to_string),
        }
    }

    fn pins_file(dir: &Path) -> PathBuf {
        dir.join("registry.e2e.toml")
    }

    #[test]
    fn compare_covers_every_transition() {
        let e2e = pin(true, Some("aa"));
        let plain = pin(false, None);
        assert_eq!(compare(None, &e2e), Verdict::Unpinned);
        assert_eq!(compare(None, &plain), Verdict::Unpinned);
        assert_eq!(compare(Some(&e2e), &e2e), Verdict::Same);
        assert_eq!(compare(Some(&plain), &plain), Verdict::Same);
        assert_eq!(compare(Some(&e2e), &plain), Verdict::Downgrade);
        assert_eq!(compare(Some(&plain), &e2e), Verdict::Upgrade);
        // The flag check alone never looks at the master.
        assert_eq!(compare(Some(&e2e), &pin(true, Some("bb"))), Verdict::Same);
        assert_eq!(
            compare_master(Some(&e2e), &pin(true, Some("bb"))),
            Verdict::MasterChanged
        );
        assert_eq!(compare_master(Some(&e2e), &e2e), Verdict::Same);
        // No fingerprint on either side (pinned by `fs create`, or not
        // yet unlocked) is not a change.
        assert_eq!(compare_master(Some(&pin(true, None)), &e2e), Verdict::Same);
        assert_eq!(compare_master(Some(&e2e), &pin(true, None)), Verdict::Same);
        assert_eq!(compare_master(Some(&e2e), &plain), Verdict::Downgrade);
    }

    #[test]
    fn compare_ignores_a_pin_taken_at_another_url() {
        let mut elsewhere = pin(true, Some("aa"));
        elsewhere.s3 = "s3://other/prefix".into();
        assert_eq!(
            compare(Some(&elsewhere), &pin(false, None)),
            Verdict::Unpinned
        );
    }

    /// The fingerprint follows the master key: a rewrap under a new
    /// passphrase keeps it, another keyring (same passphrase) changes it,
    /// and so does the uuid it is bound to.
    #[test]
    fn fingerprint_follows_the_master_not_the_passphrase() {
        let (meta, keys) = e2e_meta("old");
        let base = keys.pin_fingerprint(&meta.uuid.to_string());
        assert_eq!(base.len(), 64);
        let rewrapped = rewrap_master(meta.keyring.as_ref().unwrap(), "old", "new").unwrap();
        let reopened = unlock(&rewrapped, "new").unwrap();
        assert_eq!(reopened.pin_fingerprint(&meta.uuid.to_string()), base);
        let (_, other) = e2e_meta("old");
        assert_ne!(other.pin_fingerprint(&meta.uuid.to_string()), base);
        assert_ne!(keys.pin_fingerprint("another-uuid"), base);
        assert_eq!(
            E2ePin::observe(S3, &meta, Some(&keys))
                .master_fingerprint
                .as_deref(),
            Some(base.as_str())
        );
        let mut plain = meta.clone();
        plain.e2e = false;
        assert_eq!(E2ePin::observe(S3, &plain, Some(&keys)), pin(false, None));
    }

    #[test]
    fn first_unlock_pins_and_a_downgrade_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = pins_file(dir.path());
        let target = PinTarget::named("myfs", S3);
        let (meta, keys) = e2e_meta("pw");

        let first = check_at(Some(path.clone()), &target, &meta, false).unwrap();
        assert!(read(&path).unwrap().is_empty(), "pinned before the unlock");
        first.confirm(Some(&keys)).unwrap();
        assert_eq!(
            read(&path).unwrap()["myfs"],
            E2ePin::observe(S3, &meta, Some(&keys))
        );

        let again = check_at(Some(path.clone()), &target, &meta, false).unwrap();
        again.confirm(Some(&keys)).unwrap();

        let mut stripped = meta.clone();
        stripped.e2e = false;
        stripped.keyring = None;
        let err = check_at(Some(path.clone()), &target, &stripped, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("NOT end-to-end encrypted"), "{msg}");
        assert!(msg.contains(ACCEPT_ENV), "{msg}");

        // Another name is pinned separately.
        let other = PinTarget::named("otherfs", S3);
        let unpinned = check_at(Some(path.clone()), &other, &stripped, false).unwrap();
        unpinned.confirm(None).unwrap();
        assert!(!read(&path).unwrap().contains_key("otherfs"));
    }

    /// `fs passwd` elsewhere passes; a keyring from another filesystem
    /// that the same passphrase opens is refused after the unlock, before
    /// its keys can be used.
    #[test]
    fn a_rewrap_passes_and_a_spliced_keyring_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = pins_file(dir.path());
        let target = PinTarget::named("myfs", S3);
        let (meta, keys) = e2e_meta("pw");
        check_at(Some(path.clone()), &target, &meta, false)
            .unwrap()
            .confirm(Some(&keys))
            .unwrap();

        let mut rewrapped = meta.clone();
        rewrapped.keyring =
            Some(rewrap_master(meta.keyring.as_ref().unwrap(), "pw", "new").unwrap());
        let reopened = unlock(rewrapped.keyring.as_ref().unwrap(), "new").unwrap();
        check_at(Some(path.clone()), &target, &rewrapped, false)
            .unwrap()
            .confirm(Some(&reopened))
            .unwrap();

        let (foreign, foreign_keys) = e2e_meta("pw");
        let mut spliced = meta.clone();
        spliced.keyring = foreign.keyring;
        let check = check_at(Some(path.clone()), &target, &spliced, false).unwrap();
        let err = check.confirm(Some(&foreign_keys)).unwrap_err();
        assert!(format!("{err:#}").contains("master key"));
        assert_eq!(
            read(&path).unwrap()["myfs"].master_fingerprint.as_deref(),
            Some(keys.pin_fingerprint(&meta.uuid.to_string()).as_str()),
            "the refused master must not have been pinned"
        );
    }

    #[test]
    fn accepted_change_re_pins() {
        let dir = tempfile::tempdir().unwrap();
        let path = pins_file(dir.path());
        let target = PinTarget::named("myfs", S3);
        let (meta, keys) = e2e_meta("pw");
        write(&path, "myfs", &E2ePin::observe(S3, &meta, Some(&keys))).unwrap();

        let mut stripped = meta.clone();
        stripped.e2e = false;
        stripped.keyring = None;
        let accepted = check_at(Some(path.clone()), &target, &stripped, true).unwrap();
        accepted.confirm(None).unwrap();
        assert_eq!(read(&path).unwrap()["myfs"], pin(false, None));

        // Back to E2E is an upgrade: allowed, and pinned once unlocked.
        let upgrade = check_at(Some(path.clone()), &target, &meta, false).unwrap();
        upgrade.confirm(Some(&keys)).unwrap();
        assert_eq!(
            read(&path).unwrap()["myfs"],
            E2ePin::observe(S3, &meta, Some(&keys))
        );

        // A pin from `fs create` carries no fingerprint; the first unlock
        // adds it.
        write(&path, "myfs", &pin(true, None)).unwrap();
        check_at(Some(path.clone()), &target, &meta, false)
            .unwrap()
            .confirm(Some(&keys))
            .unwrap();
        assert!(read(&path).unwrap()["myfs"].master_fingerprint.is_some());
    }

    #[test]
    fn unnamed_targets_are_keyed_by_url() {
        let target = PinTarget::for_target(&Target::Raw("/mnt/x".into()), S3);
        assert_eq!(target.key, S3);
        let target = PinTarget::for_target(
            &Target::Named {
                name: "myfs".into(),
                path: None,
                entry: Default::default(),
            },
            S3,
        );
        assert_eq!(target.key, "myfs");
    }

    #[test]
    fn pins_round_trip_through_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = pins_file(dir.path());
        write(&path, "a", &pin(true, Some("aa"))).unwrap();
        write(&path, S3, &pin(false, None)).unwrap();
        let pins = read(&path).unwrap();
        assert_eq!(pins["a"], pin(true, Some("aa")));
        assert_eq!(pins[S3], pin(false, None));
    }
}
