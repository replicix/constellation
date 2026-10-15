//! [`Manager::passwd`]: `fs passwd` and `fs.passwd`.

use super::{failed, store_err, store_failed, Manager};
use crate::e2e_pin::{self, PinTarget};
use constellation_control::proto::ControlError;
use constellation_platform::CredentialSource;
use constellation_store_s3::ChunkStore;
use std::sync::Arc;
use zeroize::Zeroizing;

/// Whose passphrase to change.
#[derive(Clone)]
pub struct PasswdRequest {
    pub url: String,
    /// The E2E pin it is held against: the name's, or the URL's for an
    /// unregistered filesystem.
    pub pin: PinTarget,
    /// What to sign with (`None`: the environment's AWS chain).
    pub credentials: Option<Arc<CredentialSource>>,
}

/// What a passphrase change came to.
#[derive(Debug, Clone, Default)]
pub struct PasswdOutcome {
    /// Set when the change landed but `meta.json` read back afterwards
    /// did not open with the new passphrase (the pin was then left as it
    /// was).
    pub warning: Option<String>,
}

impl Manager {
    /// Change an E2E filesystem's passphrase: the same master key,
    /// rewrapped, so no data is re-encrypted and mounted nodes need no
    /// remount. `secrets` supplies `(current, new)` once the filesystem is
    /// known to be E2E (the CLI prompts there).
    ///
    /// `meta.json` is unauthenticated, so the keyring is held against this
    /// host's pin (`e2e_pin`) twice: its E2E flag before anything is
    /// asked, and the master key the current passphrase opens before the
    /// rewrap — a keyring spliced in from another filesystem under a
    /// reused passphrase is refused rather than rewrapped and re-pinned.
    /// `CONSTELLATION_ACCEPT_E2E_CHANGE=1` accepts either change, as it does
    /// for a mount.
    pub async fn passwd(
        &self,
        req: PasswdRequest,
        secrets: &mut (dyn FnMut() -> Result<(Zeroizing<String>, Zeroizing<String>), ControlError>
                  + Send),
    ) -> Result<PasswdOutcome, ControlError> {
        let (store, _) = self.open(&req.url, req.credentials.as_ref()).await?;
        self.passwd_in(&store, &req.pin, secrets).await
    }

    /// [`Manager::passwd`] on an open store.
    pub(crate) async fn passwd_in(
        &self,
        store: &ChunkStore,
        pin: &PinTarget,
        secrets: &mut (dyn FnMut() -> Result<(Zeroizing<String>, Zeroizing<String>), ControlError>
                  + Send),
    ) -> Result<PasswdOutcome, ControlError> {
        let meta = self.load_meta(store).await.map_err(store_err)?;
        let check = e2e_pin::check_with(self.pins(), pin, &meta).map_err(failed)?;
        if !meta.e2e {
            return Err(ControlError::invalid("filesystem is not in E2E mode"));
        }
        let (old, new) = secrets()?;
        if new.is_empty() {
            return Err(ControlError::invalid("passphrase must not be empty"));
        }
        let keys = meta
            .unlock(&old)
            .map_err(|e| failed(format!("changing E2E passphrase: {e}")))?;
        check.confirm(Some(&keys)).map_err(failed)?;
        drop(keys);
        // The block just checked is the one rewrapped: the update is
        // conditional on it (a keyring swapped in since is a conflict).
        let checked = meta.keyring.as_ref().ok_or_else(|| {
            ControlError::failed("changing E2E passphrase: E2E filesystem has no keyring block")
        })?;
        store
            .change_passphrase(checked, &old, &new)
            .await
            .map_err(|e| store_failed(e, Some("changing E2E passphrase")))?;
        // The rewrap kept the master, so the pin's fingerprint stands;
        // confirm it once the new passphrase has opened the block (a pin
        // from `fs create` has none yet, and gets it here).
        let rewrapped = self.load_meta(store).await.map_err(store_err)?;
        let warning = match rewrapped.unlock(&new) {
            Ok(keys) => e2e_pin::check_with(self.pins(), pin, &rewrapped)
                .and_then(|check| check.confirm(Some(&keys)))
                .err()
                .map(|e| format!("{e:#}")),
            Err(e) => Some(format!(
                "meta.json read back after the change does not open with the new passphrase \
                 ({e}); the local E2E pin was not updated"
            )),
        };
        Ok(PasswdOutcome { warning })
    }
}
