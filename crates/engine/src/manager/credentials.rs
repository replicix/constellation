//! What `fs.unlock` supplies (plan 31 §9.8), read once for every holder of
//! it: the running daemon's `fs.unlock` (`engine::control`), and
//! `constellation serve --await-unlock`'s credential gate, which answers
//! the first `fs.unlock` before the engine exists.
//!
//! An unlock carries an S3 key pair (both halves, or neither), an optional
//! session token, and an optional E2E passphrase, which must not be empty
//! (an empty one can neither open a keyring a daemon could start from nor
//! protect a new one). They are held in an `EphemeralSecretStore` — memory
//! only — under the [`CredentialSource`] names and [`E2E_PASSPHRASE`].

use constellation_control::proto::types::UnlockCredentials;
use constellation_control::proto::ControlError;
use constellation_platform::{CredentialSource, EphemeralSecretStore, SecretStore};
use zeroize::Zeroizing;

/// The secret name an unlocked E2E passphrase is held under.
pub const E2E_PASSPHRASE: &str = "e2e_passphrase";

/// Whether `c` carries a key pair, after checking it is a usable unlock:
/// both halves of a pair or neither, something to unlock at all, and a
/// non-empty passphrase.
pub fn validate(c: &UnlockCredentials) -> Result<bool, ControlError> {
    let has_keys = c.access_key_id.is_some() || c.secret_access_key.is_some();
    if has_keys && (c.access_key_id.is_none() || c.secret_access_key.is_none()) {
        return Err(ControlError::invalid(
            "access_key_id and secret_access_key go together",
        ));
    }
    if !has_keys && c.e2e_passphrase.is_none() {
        return Err(ControlError::invalid("no credentials to unlock"));
    }
    if c.e2e_passphrase
        .as_ref()
        .is_some_and(|p| p.expose().is_empty())
    {
        return Err(ControlError::invalid(
            "the e2e passphrase must not be empty",
        ));
    }
    Ok(has_keys)
}

/// The key pair of `c` as store entries (the session token cleared when
/// absent), or nothing when it carries none.
pub fn key_entries(c: &UnlockCredentials) -> Vec<(&'static str, Option<&[u8]>)> {
    match (&c.access_key_id, &c.secret_access_key) {
        (Some(id), Some(secret)) => vec![
            (
                CredentialSource::ACCESS_KEY_ID,
                Some(id.expose().as_bytes()),
            ),
            (
                CredentialSource::SECRET_ACCESS_KEY,
                Some(secret.expose().as_bytes()),
            ),
            (
                CredentialSource::SESSION_TOKEN,
                c.session_token.as_ref().map(|t| t.expose().as_bytes()),
            ),
        ],
        _ => Vec::new(),
    }
}

/// Everything `c` carries as store entries, for one `replace` (one
/// generation, so an S3 client never sees half a key pair): the key pair
/// with the expiry cleared — a rotation replaces expiring keys with what
/// it brings — and the passphrase.
pub fn entries(c: &UnlockCredentials) -> Vec<(&'static str, Option<&[u8]>)> {
    let mut entries = key_entries(c);
    if !entries.is_empty() {
        entries.push((CredentialSource::EXPIRY, None));
    }
    if let Some(passphrase) = &c.e2e_passphrase {
        entries.push((E2E_PASSPHRASE, Some(passphrase.expose().as_bytes())));
    }
    entries
}

/// The E2E passphrase an unlock left in `store`, if any.
pub fn passphrase(store: &EphemeralSecretStore) -> Result<Option<Zeroizing<String>>, ControlError> {
    let Some(secret) = store
        .get(E2E_PASSPHRASE)
        .map_err(|e| ControlError::failed(format!("reading the unlocked passphrase: {e}")))?
    else {
        return Ok(None);
    };
    let text = secret
        .expose_str()
        .ok_or_else(|| ControlError::invalid("the e2e passphrase is not UTF-8"))?;
    if text.is_empty() {
        return Err(ControlError::invalid(
            "the e2e passphrase must not be empty",
        ));
    }
    Ok(Some(Zeroizing::new(text.to_string())))
}

/// The E2E passphrase held by `source`, when it is a static source an
/// unlock filled.
pub fn source_passphrase(
    source: &CredentialSource,
) -> Result<Option<Zeroizing<String>>, ControlError> {
    match source {
        CredentialSource::Static(store) => passphrase(store),
        _ => Ok(None),
    }
}

/// Whether `source` can sign S3 requests: not a static source an unlock
/// filled with a passphrase only. Such an unlock leaves signing to the
/// environment's AWS chain (IRSA, an instance role), as the `serve
/// --await-unlock` gate always did; the running daemon's `fs.*` signed
/// with the key-less store and failed ("the static credential source has
/// no aws_access_key_id").
pub fn signs(source: &CredentialSource) -> bool {
    match source {
        CredentialSource::Static(store) => store
            .get(CredentialSource::ACCESS_KEY_ID)
            .is_ok_and(|key| key.is_some()),
        _ => true,
    }
}
