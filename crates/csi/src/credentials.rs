//! Engine-pod credentials (plan 37 §9, K6a): what the plugins send an
//! engine pod with `fs.unlock`, and the Secret watch behind
//! `credentialSource: refreshing`.
//!
//! **Only over the control socket.** An engine pod's S3 keys and E2E
//! passphrase are never in its pod spec, its environment, its image or a
//! hostPath file: the pod starts with `constellation serve --await-unlock`
//! and an empty `EphemeralSecretStore`, its control socket answers nothing
//! but `node.ping` and `fs.unlock` until the first unlock arrives
//! ([`constellation_control::proto::AWAITING_UNLOCK`] to anything else),
//! and every later `fs.unlock` rotates the store in place — the S3 clients
//! sign their next request with the new keys, with no remount. The bytes
//! come from the request (`req.secrets`: the class's `*-secret-name`,
//! resolved by the sidecars or kubelet) or, for a `refreshing` class, from
//! the Secret this module watches; the plugins hold them in memory only,
//! to unlock a replacement engine pod (a crash, a container restart) whose
//! restage carries no secret, and the node plugin forgets a unit's once no
//! volume of it is staged on the node. A plugin restart forgets them: a
//! `static-ephemeral` engine pod that then dies cannot be restaged until a
//! request brings the secret again — a `NodePublishVolume` of a class that
//! names `csi.storage.k8s.io/node-publish-secret-name`, else kubelet's next
//! `NodeStageVolume`, after the pod using the volume is recreated; a
//! `refreshing` one re-reads its Secret. Both plugins run with core dumps
//! off (`replicix_platform_lite::forbid_core_dumps`).
//!
//! **The watch** (`replicix_csi_common::credentials::Refresher`): one per Secret per process, a list with a
//! `metadata.name` field selector (so RBAC can scope it to that one name,
//! `resourceNames`) followed by a watch from its resourceVersion, re-listed
//! with backoff (1 s doubling to 30 s) whenever the watch ends. Each change
//! of the Secret's *data* (not of its metadata) is published to the
//! subscribers, which push it to their engine pods ([`follow_rotations`]:
//! attributed to the Secret, retried while the engine is starting or cannot
//! ask S3, given up when the engine refuses the pair — it tries the pair
//! against the bucket first and keeps the old one). A deleted Secret is
//! logged and the last bytes kept: the engine keeps the credentials it has.
//! Nothing here ever logs a value; errors name the Secret, never its data.

use constellation_control::proto::types::{FsUnlockParams, UnlockCredentials};
use constellation_control::proto::{ControlError, ErrorKind, Secret};
use replicix_csi_common::credentials::{
    Latest, Secrets, ACCESS_KEY_ID, E2E_PASSPHRASE, SECRET_ACCESS_KEY, SESSION_TOKEN,
};
use replicix_csi_common::params::SecretRef;
use std::sync::Arc;
use std::time::Duration;

/// `fs.unlock` naming `fs` with what `secrets` holds: the key pair (and
/// token), the passphrase. A half key pair is dropped (the class's
/// business, not a reason to refuse the passphrase). `None` when there is
/// nothing to send.
pub fn unlock_params(fs: &str, secrets: &Secrets) -> Option<FsUnlockParams> {
    let get = |k: &str| secrets.get(k).filter(|v| !v.is_empty()).map(Secret::new);
    let (id, key) = (get(ACCESS_KEY_ID), get(SECRET_ACCESS_KEY));
    let keys = id.is_some() && key.is_some();
    let credentials = UnlockCredentials {
        access_key_id: id.filter(|_| keys),
        secret_access_key: key.filter(|_| keys),
        session_token: get(SESSION_TOKEN).filter(|_| keys),
        e2e_passphrase: get(E2E_PASSPHRASE),
    };
    (keys || credentials.e2e_passphrase.is_some()).then(|| FsUnlockParams {
        fs: fs.to_string(),
        credentials,
    })
}

/// Whether `e` is an engine saying it waits for `fs.unlock` (module docs).
pub fn is_awaiting_unlock(e: &ControlError) -> bool {
    e.kind == ErrorKind::Unavailable
        && e.message
            .contains(constellation_control::proto::AWAITING_UNLOCK)
}

/// The `on_behalf_of` of a rotation pushed from a watch: the Secret it
/// came from (`secret:<namespace>/<name>`), so the engine's audit line
/// says what the `fs.unlock` was for.
pub fn rotation_attribution(secret: &SecretRef) -> String {
    crate::controller::attribution_of(&format!("secret:{}/{}", secret.namespace, secret.name))
}

/// Whether a refused push is worth repeating unchanged: the engine was
/// starting ("connect again"), unreachable, or could not ask S3. A pair S3
/// refused (`Denied`) is not — the engine keeps the one it has until the
/// Secret changes again.
pub fn retry_push(e: &ControlError) -> bool {
    matches!(
        e.kind,
        ErrorKind::Unavailable | ErrorKind::Timeout | ErrorKind::Cancelled
    )
}

/// Push every new value of a watched Secret (`latest`) with `push`
/// (`Ok(true)`: an engine took it; `Ok(false)`: none is up to take it, its
/// next start gets it), attributed to the Secret
/// ([`rotation_attribution`]). A push that failed transiently
/// ([`retry_push`]) is repeated with backoff (1 s doubling to 30 s) until
/// it lands or the Secret changes again; `landed` is told each value an
/// engine took or none needed. Ends when the watch does.
pub async fn follow_rotations<P, Fut>(
    mut latest: Latest,
    secret: SecretRef,
    what: String,
    push: P,
    landed: impl Fn(&Secrets),
) where
    P: Fn(Arc<Secrets>) -> Fut,
    Fut: std::future::Future<Output = Result<bool, ControlError>>,
{
    let who = rotation_attribution(&secret);
    while latest.changed().await.is_ok() {
        let Some(mut data) = latest.borrow_and_update().clone() else {
            continue;
        };
        let mut backoff = Duration::from_secs(1);
        loop {
            match constellation_control::client::on_behalf_of(who.clone(), push(data.clone())).await
            {
                Ok(pushed) => {
                    if pushed {
                        tracing::info!(%secret, what, "pushed rotated credentials to the engine pod");
                    }
                    landed(&data);
                    break;
                }
                Err(e) if retry_push(&e) => {
                    tracing::warn!(%secret, what, error = %e.message, retry_in = ?backoff,
                        "pushing rotated credentials to the engine pod");
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        changed = latest.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            if let Some(newer) = latest.borrow_and_update().clone() {
                                data = newer;
                            }
                        }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
                Err(e) => {
                    tracing::warn!(%secret, what, error = %e.message,
                        "the engine pod refused the rotated credentials and keeps the ones it \
                         has until the Secret changes again");
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicix_csi_common::credentials::fingerprint;
    use std::sync::Mutex;
    use tokio::sync::watch;

    fn secrets(kv: &[(&str, &str)]) -> Secrets {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn unlock_params_carry_the_pair_and_the_passphrase_only() {
        let full = secrets(&[
            (ACCESS_KEY_ID, "id"),
            (SECRET_ACCESS_KEY, "key"),
            (SESSION_TOKEN, "tok"),
            (E2E_PASSPHRASE, "pw"),
            ("unrelated", "x"),
        ]);
        let p = unlock_params("s3://b/p", &full).unwrap();
        assert_eq!(p.fs, "s3://b/p");
        let c = &p.credentials;
        assert_eq!(c.access_key_id.as_ref().unwrap().expose(), "id");
        assert_eq!(c.secret_access_key.as_ref().unwrap().expose(), "key");
        assert_eq!(c.session_token.as_ref().unwrap().expose(), "tok");
        assert_eq!(c.e2e_passphrase.as_ref().unwrap().expose(), "pw");
        // Half a pair: the passphrase still goes, the half does not.
        let half = unlock_params(
            "f",
            &secrets(&[(ACCESS_KEY_ID, "id"), (E2E_PASSPHRASE, "pw")]),
        )
        .unwrap();
        assert!(half.credentials.access_key_id.is_none());
        assert!(half.credentials.e2e_passphrase.is_some());
        // Nothing usable: nothing to send.
        assert!(unlock_params("f", &secrets(&[(ACCESS_KEY_ID, "id")])).is_none());
        assert!(unlock_params("f", &secrets(&[(SECRET_ACCESS_KEY, "")])).is_none());
        assert!(unlock_params("f", &Secrets::new()).is_none());
        // The fingerprint follows the credentials, not unrelated keys.
        let mut other = full.clone();
        other.insert("unrelated".into(), "y".into());
        assert_eq!(fingerprint(&full), fingerprint(&other));
        other.insert(SECRET_ACCESS_KEY.into(), "rotated".into());
        assert_ne!(fingerprint(&full), fingerprint(&other));
    }

    #[test]
    fn the_awaiting_unlock_refusal_is_recognised() {
        let waiting = ControlError::unavailable(constellation_control::proto::AWAITING_UNLOCK);
        assert!(is_awaiting_unlock(&waiting));
        assert!(!is_awaiting_unlock(&ControlError::unavailable("down")));
        assert!(!is_awaiting_unlock(&ControlError::failed(
            constellation_control::proto::AWAITING_UNLOCK
        )));
    }

    /// A push the engine refused transiently (it was starting: "connect
    /// again") is repeated until it lands, attributed to the Secret; one it
    /// refused for the pair itself (`Denied`) waits for the next change.
    #[tokio::test(start_paused = true)]
    async fn a_rotation_push_is_retried_until_it_lands() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let (tx, latest) = watch::channel(None::<Arc<Secrets>>);
        let attempts = Arc::new(AtomicU32::new(0));
        let landed = Arc::new(Mutex::new(Vec::new()));
        let who = Arc::new(Mutex::new(Vec::new()));
        let r = SecretRef {
            namespace: "ns".into(),
            name: "creds".into(),
        };
        let task = tokio::spawn(follow_rotations(
            latest,
            r.clone(),
            "unit".into(),
            {
                let (attempts, who) = (attempts.clone(), who.clone());
                move |data: Arc<Secrets>| {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    let who = who.clone();
                    async move {
                        who.lock()
                            .unwrap()
                            .push(constellation_control::client::current_on_behalf_of());
                        match (data[ACCESS_KEY_ID].as_str(), n) {
                            ("A", 0 | 1) => Err(ControlError::unavailable("connect again")),
                            ("BAD", _) => Err(ControlError::new(ErrorKind::Denied, "refused")),
                            _ => Ok(true),
                        }
                    }
                }
            },
            {
                let landed = landed.clone();
                move |data: &Secrets| landed.lock().unwrap().push(data[ACCESS_KEY_ID].clone())
            },
        ));
        let pair = |id: &str| Arc::new(secrets(&[(ACCESS_KEY_ID, id), (SECRET_ACCESS_KEY, "x")]));
        tx.send(Some(pair("A"))).unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "two refusals, then it landed"
        );
        assert_eq!(*landed.lock().unwrap(), ["A"]);
        tx.send(Some(pair("BAD"))).unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "a refused pair is not retried"
        );
        assert_eq!(*landed.lock().unwrap(), ["A"]);
        assert!(who
            .lock()
            .unwrap()
            .iter()
            .all(|w| w.as_deref() == Some("secret:ns/creds")));
        task.abort();
    }
}
