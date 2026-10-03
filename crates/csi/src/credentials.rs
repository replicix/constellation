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
//! off (`constellation_platform::forbid_core_dumps`).
//!
//! **The watch** ([`Refresher`]): one per Secret per process, a list with a
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

use crate::params::SecretRef;
use async_trait::async_trait;
use constellation_control::proto::types::{FsUnlockParams, UnlockCredentials};
use constellation_control::proto::{ControlError, ErrorKind, Secret};
use futures::stream::BoxStream;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

/// A Secret's data, as the CSI request (`req.secrets`) or the API carries
/// it: plan 37 §6's keys.
pub type Secrets = BTreeMap<String, String>;

pub const ACCESS_KEY_ID: &str = "aws_access_key_id";
pub const SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
pub const SESSION_TOKEN: &str = "aws_session_token";
pub const E2E_PASSPHRASE: &str = "e2e_passphrase";

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

/// A fingerprint of what [`unlock_params`] sends, to tell whether an
/// engine already has it. Kept in memory only, never logged.
pub fn fingerprint(secrets: &Secrets) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    for key in [
        ACCESS_KEY_ID,
        SECRET_ACCESS_KEY,
        SESSION_TOKEN,
        E2E_PASSPHRASE,
    ] {
        h.update(key.as_bytes());
        h.update(&[0]);
        h.update(secrets.get(key).map(String::as_bytes).unwrap_or_default());
        h.update(&[0]);
    }
    *h.finalize().as_bytes()
}

/// Whether `e` is an engine saying it waits for `fs.unlock` (module docs).
pub fn is_awaiting_unlock(e: &ControlError) -> bool {
    e.kind == ErrorKind::Unavailable
        && e.message
            .contains(constellation_control::proto::AWAITING_UNLOCK)
}

/// What a watch reports.
pub enum SecretEvent {
    /// The Secret's data now.
    Changed(Secrets),
    Deleted,
}

/// Where Secrets are read and watched: the API server ([`KubeSecrets`]),
/// or a test double.
#[async_trait]
pub trait SecretSource: Send + Sync + 'static {
    /// The Secret's data (`None`: it does not exist) and the
    /// resourceVersion to watch from.
    async fn read(&self, secret: &SecretRef) -> Result<(Option<Secrets>, String), String>;
    /// Its changes after `version`, until the stream ends.
    async fn watch(
        &self,
        secret: &SecretRef,
        version: &str,
    ) -> Result<BoxStream<'static, Result<SecretEvent, String>>, String>;
}

/// [`SecretSource`] over the Kubernetes API, scoped to one name per
/// request (`fieldSelector=metadata.name=<name>`), which is what lets the
/// chart grant `get`/`list`/`watch` on exactly the watched Secrets.
pub struct KubeSecrets {
    client: kube::Client,
}

impl KubeSecrets {
    pub fn new(client: kube::Client) -> KubeSecrets {
        KubeSecrets { client }
    }

    fn api(&self, secret: &SecretRef) -> kube::Api<k8s_openapi::api::core::v1::Secret> {
        kube::Api::namespaced(self.client.clone(), &secret.namespace)
    }
}

fn secret_data(s: &k8s_openapi::api::core::v1::Secret) -> Secrets {
    s.data
        .iter()
        .flatten()
        .map(|(k, v)| (k.clone(), String::from_utf8_lossy(&v.0).into_owned()))
        .collect()
}

#[async_trait]
impl SecretSource for KubeSecrets {
    async fn read(&self, secret: &SecretRef) -> Result<(Option<Secrets>, String), String> {
        let list = self
            .api(secret)
            .list(
                &kube::api::ListParams::default().fields(&format!("metadata.name={}", secret.name)),
            )
            .await
            .map_err(|e| e.to_string())?;
        let version = list.metadata.resource_version.clone().unwrap_or_default();
        Ok((list.items.first().map(secret_data), version))
    }

    async fn watch(
        &self,
        secret: &SecretRef,
        version: &str,
    ) -> Result<BoxStream<'static, Result<SecretEvent, String>>, String> {
        use kube::api::WatchEvent;
        let params = kube::api::WatchParams::default()
            .fields(&format!("metadata.name={}", secret.name))
            .timeout(290);
        let stream = self
            .api(secret)
            .watch(&params, version)
            .await
            .map_err(|e| e.to_string())?;
        Ok(stream
            .filter_map(|event| async move {
                match event {
                    Ok(WatchEvent::Added(s)) | Ok(WatchEvent::Modified(s)) => {
                        Some(Ok(SecretEvent::Changed(secret_data(&s))))
                    }
                    Ok(WatchEvent::Deleted(_)) => Some(Ok(SecretEvent::Deleted)),
                    Ok(WatchEvent::Bookmark(_)) => None,
                    Ok(WatchEvent::Error(status)) => Some(Err(status.to_string())),
                    Err(e) => Some(Err(e.to_string())),
                }
            })
            .boxed())
    }
}

/// The latest data of a watched Secret.
pub type Latest = watch::Receiver<Option<Arc<Secrets>>>;

/// One running watch and the units subscribed to it.
struct Watch {
    latest: Latest,
    subscribers: usize,
    task: tokio::task::AbortHandle,
}

/// One watch per Secret, shared by every subscriber in the process
/// (module docs); the last [`Refresher::release`] ends it and drops the
/// bytes it held.
pub struct Refresher {
    source: Arc<dyn SecretSource>,
    watches: Mutex<HashMap<SecretRef, Watch>>,
    /// The first re-list delay after a watch ends; tests shorten it.
    backoff: Duration,
}

impl Refresher {
    pub fn new(source: Arc<dyn SecretSource>) -> Arc<Refresher> {
        Arc::new(Refresher {
            source,
            watches: Mutex::default(),
            backoff: Duration::from_secs(1),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_backoff(source: Arc<dyn SecretSource>, backoff: Duration) -> Arc<Refresher> {
        Arc::new(Refresher {
            source,
            watches: Mutex::default(),
            backoff,
        })
    }

    /// The latest data of `secret`, watched from the first call on; counts
    /// one subscriber, to be given back with [`Self::release`].
    pub fn subscribe(&self, secret: &SecretRef) -> Latest {
        let mut watches = self.watches.lock().unwrap();
        if let Some(watch) = watches.get_mut(secret) {
            watch.subscribers += 1;
            return watch.latest.clone();
        }
        let (tx, rx) = watch::channel(None);
        let task = tokio::spawn(run_watch(
            self.source.clone(),
            secret.clone(),
            tx,
            self.backoff,
        ));
        watches.insert(
            secret.clone(),
            Watch {
                latest: rx.clone(),
                subscribers: 1,
                task: task.abort_handle(),
            },
        );
        rx
    }

    /// Give back one [`Self::subscribe`]. The last one ends the watch and
    /// drops the Secret's bytes with it.
    pub fn release(&self, secret: &SecretRef) {
        let mut watches = self.watches.lock().unwrap();
        let Some(watch) = watches.get_mut(secret) else {
            return;
        };
        watch.subscribers = watch.subscribers.saturating_sub(1);
        if watch.subscribers == 0 {
            if let Some(watch) = watches.remove(secret) {
                watch.task.abort();
            }
        }
    }

    /// [`Self::subscribe`], waiting up to `wait` for the first read; the
    /// subscription is given back on return, so a watch only this call
    /// wanted ends with it (callers that mean to keep one subscribe
    /// first).
    pub async fn current(&self, secret: &SecretRef, wait: Duration) -> Option<Arc<Secrets>> {
        let mut latest = self.subscribe(secret);
        let _ = tokio::time::timeout(wait, latest.wait_for(Option::is_some)).await;
        let current = latest.borrow().clone();
        drop(latest);
        self.release(secret);
        current
    }
}

/// Publish `data` unless its credentials are what subscribers have.
fn publish(tx: &watch::Sender<Option<Arc<Secrets>>>, data: Secrets) {
    tx.send_if_modified(|have| {
        let same = have
            .as_ref()
            .is_some_and(|h| fingerprint(h) == fingerprint(&data));
        if !same {
            *have = Some(Arc::new(data));
        }
        !same
    });
}

async fn run_watch(
    source: Arc<dyn SecretSource>,
    secret: SecretRef,
    tx: watch::Sender<Option<Arc<Secrets>>>,
    first_backoff: Duration,
) {
    let mut backoff = first_backoff;
    loop {
        match source.read(&secret).await {
            Ok((data, version)) => {
                match data {
                    Some(data) => publish(&tx, data),
                    None => tracing::warn!(%secret,
                        "the watched credentials Secret does not exist; engines keep what they have"),
                }
                match source.watch(&secret, &version).await {
                    Ok(mut events) => {
                        backoff = first_backoff;
                        while let Some(event) = events.next().await {
                            match event {
                                Ok(SecretEvent::Changed(data)) => publish(&tx, data),
                                Ok(SecretEvent::Deleted) => tracing::warn!(%secret,
                                    "the watched credentials Secret was deleted; engines keep what they have"),
                                Err(e) => {
                                    tracing::debug!(%secret, error = %e, "the Secret watch ended");
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => tracing::warn!(%secret, error = %e, "watching a credentials Secret"),
                }
            }
            Err(e) => tracing::warn!(%secret, error = %e, "reading a credentials Secret"),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
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

/// A [`SecretSource`] double for this crate's tests.
#[cfg(test)]
pub(crate) mod fake {
    use super::*;

    /// A Secret in memory: `set` changes it and wakes the watch; `end`
    /// ends the current watch stream (as the API server does).
    #[derive(Default)]
    pub(crate) struct FakeSecrets {
        pub(crate) state: Mutex<(Option<Secrets>, u64)>,
        pub(crate) events:
            Mutex<Vec<futures::channel::mpsc::UnboundedSender<Result<SecretEvent, String>>>>,
        pub(crate) reads: std::sync::atomic::AtomicU64,
    }

    impl FakeSecrets {
        pub(crate) fn set(&self, data: Option<Secrets>) {
            let mut st = self.state.lock().unwrap();
            st.0 = data.clone();
            st.1 += 1;
            for tx in self.events.lock().unwrap().iter() {
                let _ = tx.unbounded_send(Ok(match &data {
                    Some(d) => SecretEvent::Changed(d.clone()),
                    None => SecretEvent::Deleted,
                }));
            }
        }

        pub(crate) fn end(&self) {
            for tx in self.events.lock().unwrap().drain(..) {
                let _ = tx.unbounded_send(Err("watch closed".into()));
            }
        }
    }

    #[async_trait]
    impl SecretSource for Arc<FakeSecrets> {
        async fn read(&self, _: &SecretRef) -> Result<(Option<Secrets>, String), String> {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let st = self.state.lock().unwrap();
            Ok((st.0.clone(), st.1.to_string()))
        }

        async fn watch(
            &self,
            _: &SecretRef,
            _: &str,
        ) -> Result<BoxStream<'static, Result<SecretEvent, String>>, String> {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            self.events.lock().unwrap().push(tx);
            Ok(rx.boxed())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeSecrets;
    use super::*;

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

    async fn next_value(latest: &mut Latest) -> Arc<Secrets> {
        tokio::time::timeout(Duration::from_secs(10), latest.changed())
            .await
            .expect("a change within 10 s")
            .unwrap();
        latest.borrow().clone().unwrap()
    }

    /// The `Refreshing(callback)` source: every change of the Secret's
    /// credentials reaches a subscriber, once; a re-list after the watch
    /// ends republishes nothing unchanged; a deletion keeps the last bytes;
    /// one watch serves every subscriber of one Secret.
    #[tokio::test]
    async fn a_watched_secret_publishes_each_rotation_once() {
        let fake = Arc::new(FakeSecrets::default());
        fake.set(Some(secrets(&[
            (ACCESS_KEY_ID, "A"),
            (SECRET_ACCESS_KEY, "a"),
        ])));
        let refresher = Refresher::with_backoff(Arc::new(fake.clone()), Duration::from_millis(10));
        let r = SecretRef {
            namespace: "ns".into(),
            name: "creds".into(),
        };
        let mut latest = refresher.subscribe(&r);
        let first = refresher
            .current(&r, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(first[ACCESS_KEY_ID], "A");
        latest.borrow_and_update();

        fake.set(Some(secrets(&[
            (ACCESS_KEY_ID, "B"),
            (SECRET_ACCESS_KEY, "b"),
        ])));
        assert_eq!(next_value(&mut latest).await[ACCESS_KEY_ID], "B");

        // The watch ends; the re-list finds nothing new and publishes
        // nothing, and the new watch carries the next rotation.
        let reads = fake.reads.load(std::sync::atomic::Ordering::SeqCst);
        fake.end();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while fake.reads.load(std::sync::atomic::Ordering::SeqCst) == reads {
            assert!(std::time::Instant::now() < deadline, "no re-list");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        while fake.events.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !latest.has_changed().unwrap(),
            "an unchanged re-list republished"
        );
        fake.set(Some(secrets(&[
            (ACCESS_KEY_ID, "C"),
            (SECRET_ACCESS_KEY, "c"),
        ])));
        assert_eq!(next_value(&mut latest).await[ACCESS_KEY_ID], "C");

        // Deleted: the last bytes stay.
        fake.set(None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!latest.has_changed().unwrap());
        assert_eq!(latest.borrow().as_ref().unwrap()[ACCESS_KEY_ID], "C");

        // A second subscriber shares the one watch.
        let other = refresher.subscribe(&r);
        assert_eq!(other.borrow().as_ref().unwrap()[ACCESS_KEY_ID], "C");
        assert_eq!(fake.events.lock().unwrap().len(), 1);
    }

    /// The last subscriber to give a Secret back ends its watch: the API
    /// stream is dropped and the bytes are gone; earlier ones leave it
    /// running; a later subscriber starts afresh.
    #[tokio::test]
    async fn the_last_release_ends_the_watch_and_drops_the_bytes() {
        let fake = Arc::new(FakeSecrets::default());
        fake.set(Some(secrets(&[
            (ACCESS_KEY_ID, "A"),
            (SECRET_ACCESS_KEY, "a"),
        ])));
        let refresher = Refresher::with_backoff(Arc::new(fake.clone()), Duration::from_millis(10));
        let r = SecretRef {
            namespace: "ns".into(),
            name: "creds".into(),
        };
        let one = refresher.subscribe(&r);
        let mut two = refresher.subscribe(&r);
        assert!(refresher
            .current(&r, Duration::from_secs(10))
            .await
            .is_some());
        // `current` gave its own subscription back: the watch still runs.
        assert_eq!(fake.events.lock().unwrap().len(), 1);
        two.wait_for(Option::is_some).await.unwrap();
        two.borrow_and_update();
        drop(one);
        refresher.release(&r);
        fake.set(Some(secrets(&[
            (ACCESS_KEY_ID, "B"),
            (SECRET_ACCESS_KEY, "b"),
        ])));
        assert_eq!(next_value(&mut two).await[ACCESS_KEY_ID], "B");

        refresher.release(&r);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !fake.events.lock().unwrap().iter().all(|tx| tx.is_closed()) {
            assert!(
                std::time::Instant::now() < deadline,
                "the stream stayed open"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(refresher.watches.lock().unwrap().is_empty());
        assert!(
            two.changed().await.is_err(),
            "the sender, and the bytes it held, are gone"
        );
        // A new subscriber starts a new watch from nothing.
        let _fresh = refresher.subscribe(&r);
        assert_eq!(refresher.watches.lock().unwrap().len(), 1);
        refresher.release(&r);
        assert!(refresher.watches.lock().unwrap().is_empty());
    }
}
