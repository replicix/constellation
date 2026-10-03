//! Secrets: where the host keeps small named secrets, and where the
//! engine's S3 credentials come from (plan 31 §9.8).
//!
//! Two [`SecretStore`]s:
//!
//! - [`FileSecretStore`]: one file per secret under a directory, `0600`,
//!   written to a temporary name and renamed into place so a reader never
//!   sees a half-written or briefly world-readable secret. The desktop and
//!   server default, holding the host's P2P `node.key` and the E2E pins
//!   (`registry.e2e.toml`) in the config dir, with the same file names and
//!   the same lock-file discipline as before plan 31. (A Keychain store
//!   replaces it on macOS in plan 34 M2.)
//! - [`EphemeralSecretStore`]: memory only, never touches a disk, wiped
//!   when dropped. Plan 37's engine pods receive S3 credentials and E2E
//!   passphrases per request from the CSI plugin and must never write them
//!   to a hostPath or a container filesystem.
//!
//! [`CredentialSource`] is where an engine's S3 credentials come from
//! (`EngineConfig::credentials`). The `fs.unlock` control method fills a
//! `Static` one at runtime and rotates it in place; plan 37's engine pods
//! (`constellation serve --await-unlock`) start from one that is empty
//! until the CSI plugin's first `fs.unlock`.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use zeroize::{Zeroize, Zeroizing};

use crate::dirs::Dirs;
use crate::lock::FileLock;

/// A secret value: the bytes are wiped when it is dropped, and `Debug`
/// never prints them.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(Zeroizing<Vec<u8>>);

impl Secret {
    pub fn new(bytes: Vec<u8>) -> Secret {
        Secret(Zeroizing::new(bytes))
    }

    pub fn expose(&self) -> &[u8] {
        &self.0
    }

    /// The bytes as UTF-8, if they are.
    pub fn expose_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }
}

impl From<Vec<u8>> for Secret {
    fn from(bytes: Vec<u8>) -> Secret {
        Secret::new(bytes)
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Secret {
        Secret::new(s.as_bytes().to_vec())
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Secret({} bytes)", self.0.len())
    }
}

/// The read-modify-write step of [`SecretStore::update`]: given the
/// current value (`None`: absent), the new one to store, or `None` to
/// leave the store unchanged.
pub type Update<'a> = &'a mut dyn FnMut(Option<&[u8]>) -> io::Result<Option<Vec<u8>>>;

/// Named secrets. Names are plain file names (no `/`, not `.`/`..`).
pub trait SecretStore: Send + Sync {
    /// The secret called `name`, or `None` when there is none.
    fn get(&self, name: &str) -> io::Result<Option<Secret>>;

    /// Store `value` as `name`, replacing any previous value atomically.
    fn put(&self, name: &str, value: &[u8]) -> io::Result<()>;

    /// Remove `name`; removing an absent secret succeeds.
    fn delete(&self, name: &str) -> io::Result<()>;

    /// Read-modify-write `name` atomically against every other `update`
    /// of it, across processes where the store is shared by them.
    fn update(&self, name: &str, f: Update<'_>) -> io::Result<()>;

    /// Where `name` lives, for messages (a path for the file store).
    fn describe(&self, name: &str) -> String;
}

fn check_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', '\0']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name:?} is not a valid secret name"),
        ));
    }
    Ok(())
}

enum Root {
    Fixed(PathBuf),
    /// The config dir, resolved at each use: a missing `HOME` is an error
    /// of that use, not of building the host's services.
    Config(Arc<dyn Dirs>),
}

/// One `0600` file per secret in a directory. See the module docs.
pub struct FileSecretStore {
    root: Root,
    lock: Arc<dyn FileLock>,
}

impl FileSecretStore {
    /// Secrets as files directly under `root`.
    pub fn new(root: impl Into<PathBuf>) -> FileSecretStore {
        FileSecretStore {
            root: Root::Fixed(root.into()),
            lock: crate::sys::file_lock(),
        }
    }

    /// The store for the file at `path`, and the secret name it is in it:
    /// `(FileSecretStore::new(<dir>), <file name>)`. For locations an
    /// operator overrides with a whole path (`CONSTELLATION_NODE_KEY`,
    /// `CONSTELLATION_REGISTRY`).
    pub fn for_path(path: &Path) -> io::Result<(FileSecretStore, String)> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{} does not name a file", path.display()),
                )
            })?
            .to_string();
        let dir = path.parent().unwrap_or(Path::new("."));
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        Ok((FileSecretStore::new(dir), name))
    }

    /// Secrets in `dirs`' config dir, with `lock` for [`SecretStore::update`].
    pub fn in_config_dir(dirs: Arc<dyn Dirs>, lock: Arc<dyn FileLock>) -> FileSecretStore {
        FileSecretStore {
            root: Root::Config(dirs),
            lock,
        }
    }

    fn root(&self) -> io::Result<PathBuf> {
        match &self.root {
            Root::Fixed(p) => Ok(p.clone()),
            Root::Config(dirs) => dirs.config_dir(),
        }
    }

    /// `<root>/<name>`, after checking the name.
    pub fn path(&self, name: &str) -> io::Result<PathBuf> {
        check_name(name)?;
        Ok(self.root()?.join(name))
    }

    /// Write `value` to `<name>.tmp` (owner-only from creation) and rename
    /// it over `<name>`. Two unlocked `put`s of one name racing each
    /// other: one fails (`AlreadyExists` on the temp file) rather than
    /// both writing into it; [`SecretStore::update`] is the serialised
    /// path.
    fn write_file(path: &Path, value: &[u8]) -> io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = sibling(path, "tmp");
        // A leftover temp file from a crash may have another mode; start
        // over so `mode(0o600)` applies.
        let _ = std::fs::remove_file(&tmp);
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp)?;
        #[cfg(unix)]
        {
            // `mode` is filtered by the umask; a umask cannot widen 0600,
            // but make the owner-only intent explicit anyway.
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(value)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp, path)
    }

    fn read_file(path: &Path) -> io::Result<Option<Secret>> {
        match std::fs::read(path) {
            Ok(bytes) => Ok(Some(Secret::new(bytes))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// `<path>.<ext>`: `node.key` -> `node.key.tmp`,
/// `registry.e2e.toml` -> `registry.e2e.toml.lock` (the pre-plan-31 names).
fn sibling(path: &Path, ext: &str) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".");
    s.push(ext);
    PathBuf::from(s)
}

impl SecretStore for FileSecretStore {
    fn get(&self, name: &str) -> io::Result<Option<Secret>> {
        FileSecretStore::read_file(&self.path(name)?)
    }

    fn put(&self, name: &str, value: &[u8]) -> io::Result<()> {
        FileSecretStore::write_file(&self.path(name)?, value)
    }

    fn delete(&self, name: &str) -> io::Result<()> {
        match std::fs::remove_file(self.path(name)?) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    fn update(&self, name: &str, f: Update<'_>) -> io::Result<()> {
        let path = self.path(name)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let _guard = self
            .lock
            .lock(crate::lock::open_lock_file(&sibling(&path, "lock"))?)?;
        let current = FileSecretStore::read_file(&path)?;
        let next = f(current.as_ref().map(Secret::expose))?;
        if let Some(mut next) = next {
            let written = FileSecretStore::write_file(&path, &next);
            next.zeroize();
            written?;
        }
        Ok(())
    }

    fn describe(&self, name: &str) -> String {
        match self.path(name) {
            Ok(p) => p.display().to_string(),
            Err(e) => format!("{name} ({e})"),
        }
    }
}

/// Keep this process's memory out of core dumps (plan 37 K6a): a process
/// whose credentials live only in memory — an engine pod waiting for
/// `fs.unlock`, the CSI plugins holding the Secrets they pass on — must not
/// write them wherever the node's `core_pattern` points when it crashes.
/// `RLIMIT_CORE` 0 stops a core file (and handlers like systemd-coredump
/// that honour it); on Linux the process is also made non-dumpable
/// (`PR_SET_DUMPABLE` 0), which stops every core handler and keeps other
/// processes of the same uid from reading its memory through `/proc` or
/// `ptrace`. A no-op where neither exists.
pub fn forbid_core_dumps() -> io::Result<()> {
    #[cfg(unix)]
    {
        let none = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: a valid rlimit for this process.
        if unsafe { libc::setrlimit(libc::RLIMIT_CORE, &none) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // SAFETY: PR_SET_DUMPABLE takes one integer argument.
        if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Whether [`forbid_core_dumps`] is in effect for this process.
pub fn core_dumps_forbidden() -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // SAFETY: PR_GET_DUMPABLE takes no argument.
        if unsafe { libc::prctl(libc::PR_GET_DUMPABLE, 0, 0, 0, 0) } != 0 {
            return false;
        }
    }
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 1,
            rlim_max: 1,
        };
        // SAFETY: a valid out-pointer.
        if unsafe { libc::getrlimit(libc::RLIMIT_CORE, &mut limit) } != 0 || limit.rlim_cur != 0 {
            return false;
        }
        true
    }
    #[cfg(not(unix))]
    false
}

/// Secrets held in this process's memory only. Cloning shares the same
/// secrets (a handle); the values are wiped when the last handle drops.
///
/// Every change bumps a **generation**, read with the values in one
/// critical section ([`Self::snapshot`]), so a reader that caches what it
/// derived from the store (the engine's S3 credential provider) can tell
/// that it is stale without comparing secrets. [`Self::replace`] changes
/// several names under one generation: a rotation's key id and secret
/// never appear half-swapped. The generation the reader last *used* is
/// recorded back ([`Self::note_in_use`]), which is how a rotation shows
/// that the running S3 clients picked it up (plan 37 K6a).
#[derive(Clone, Default)]
pub struct EphemeralSecretStore {
    inner: Arc<EphemeralInner>,
}

#[derive(Default)]
struct EphemeralInner {
    secrets: Mutex<(HashMap<String, Secret>, u64)>,
    in_use: std::sync::atomic::AtomicU64,
}

impl EphemeralSecretStore {
    pub fn new() -> EphemeralSecretStore {
        EphemeralSecretStore::default()
    }

    fn map(&self) -> std::sync::MutexGuard<'_, (HashMap<String, Secret>, u64)> {
        self.inner.secrets.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// How many secrets it holds.
    pub fn len(&self) -> usize {
        self.map().0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map().0.is_empty()
    }

    /// How many changes the store has seen (0: never written).
    pub fn generation(&self) -> u64 {
        self.map().1
    }

    /// Every secret and the generation they belong to, read at once.
    pub fn snapshot(&self) -> (u64, HashMap<String, Secret>) {
        let map = self.map();
        (map.1, map.0.clone())
    }

    /// Set (`Some`) or remove (`None`) several names as one change.
    pub fn replace(&self, entries: &[(&str, Option<&[u8]>)]) -> io::Result<()> {
        for (name, _) in entries {
            check_name(name)?;
        }
        let mut map = self.map();
        for (name, value) in entries {
            match value {
                Some(v) => {
                    map.0.insert(name.to_string(), Secret::new(v.to_vec()));
                }
                None => {
                    map.0.remove(*name);
                }
            }
        }
        map.1 += 1;
        Ok(())
    }

    /// Record that generation `generation` is what a reader now uses.
    pub fn note_in_use(&self, generation: u64) {
        self.inner
            .in_use
            .fetch_max(generation, std::sync::atomic::Ordering::SeqCst);
    }

    /// The newest generation a reader said it uses (0: none yet).
    pub fn in_use_generation(&self) -> u64 {
        self.inner.in_use.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl std::fmt::Debug for EphemeralSecretStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (generation, names) = {
            let map = self.map();
            let mut names: Vec<String> = map.0.keys().cloned().collect();
            names.sort();
            (map.1, names)
        };
        f.debug_struct("EphemeralSecretStore")
            .field("names", &names)
            .field("generation", &generation)
            .finish()
    }
}

impl SecretStore for EphemeralSecretStore {
    fn get(&self, name: &str) -> io::Result<Option<Secret>> {
        check_name(name)?;
        Ok(self.map().0.get(name).cloned())
    }

    fn put(&self, name: &str, value: &[u8]) -> io::Result<()> {
        self.replace(&[(name, Some(value))])
    }

    fn delete(&self, name: &str) -> io::Result<()> {
        self.replace(&[(name, None)])
    }

    fn update(&self, name: &str, f: Update<'_>) -> io::Result<()> {
        check_name(name)?;
        let mut map = self.map();
        let next = f(map.0.get(name).map(Secret::expose))?;
        if let Some(next) = next {
            map.0.insert(name.to_string(), Secret::new(next));
            map.1 += 1;
        }
        Ok(())
    }

    fn describe(&self, name: &str) -> String {
        format!("{name} (in memory)")
    }
}

/// One set of S3 credentials. The secret parts are wiped on drop and
/// never printed by `Debug`.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub access_key_id: String,
    pub secret_access_key: Secret,
    pub session_token: Option<Secret>,
    /// When the credentials stop working; `None` for long-lived keys.
    pub expiry: Option<SystemTime>,
}

impl Credential {
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<Secret>,
    ) -> Credential {
        Credential {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            expiry: None,
        }
    }

    /// Whether the credentials have expired at `now`.
    pub fn is_expired(&self, now: SystemTime) -> bool {
        self.expiry.is_some_and(|expiry| expiry <= now)
    }
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expiry", &self.expiry)
            .finish()
    }
}

/// Where an engine's S3 credentials come from (plan 31 §9.8).
pub enum CredentialSource {
    /// The AWS SDK's own chain (environment, profiles, SSO, IMDS, IRSA,
    /// EKS Pod Identity): nothing to supply, the SDK finds them.
    AwsDefaultChain,
    /// Supplied once and held in memory, under the names
    /// [`CredentialSource::ACCESS_KEY_ID`] and friends.
    Static(EphemeralSecretStore),
    /// Asked for again whenever the engine needs fresh credentials
    /// (rotation): the supplier is called on every [`resolve`].
    ///
    /// [`resolve`]: CredentialSource::resolve
    Refreshing(Box<dyn Fn() -> Credential + Send + Sync>),
}

impl CredentialSource {
    pub const ACCESS_KEY_ID: &'static str = "aws_access_key_id";
    pub const SECRET_ACCESS_KEY: &'static str = "aws_secret_access_key";
    pub const SESSION_TOKEN: &'static str = "aws_session_token";
    /// Seconds since the Unix epoch, as decimal text.
    pub const EXPIRY: &'static str = "aws_expiry_unix";

    /// A [`CredentialSource::Static`] holding `credential`.
    pub fn from_static(credential: &Credential) -> CredentialSource {
        let store = EphemeralSecretStore::new();
        // Infallible: the names are constants and valid.
        let _ = store.put(Self::ACCESS_KEY_ID, credential.access_key_id.as_bytes());
        let _ = store.put(
            Self::SECRET_ACCESS_KEY,
            credential.secret_access_key.expose(),
        );
        if let Some(token) = &credential.session_token {
            let _ = store.put(Self::SESSION_TOKEN, token.expose());
        }
        if let Some(expiry) = credential.expiry {
            let secs = expiry
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let _ = store.put(Self::EXPIRY, secs.to_string().as_bytes());
        }
        CredentialSource::Static(store)
    }

    /// The credentials to use now: `None` for
    /// [`CredentialSource::AwsDefaultChain`] (the SDK resolves them), an
    /// error when a static source lacks the key id or secret (not
    /// unlocked yet).
    pub fn resolve(&self) -> io::Result<Option<Credential>> {
        self.resolve_generation().map(|(_, c)| c)
    }

    /// The generation of what [`Self::resolve`] would give: a static
    /// source's store generation (it changes with every rotation), `0`
    /// for the other kinds (the chain renews itself, a refreshing source
    /// is asked every time).
    pub fn generation(&self) -> u64 {
        match self {
            CredentialSource::Static(store) => store.generation(),
            _ => 0,
        }
    }

    /// [`Self::resolve`], with the generation the answer belongs to (read
    /// in one critical section with it, so a rotation in between cannot
    /// pair a new key id with an old generation).
    pub fn resolve_generation(&self) -> io::Result<(u64, Option<Credential>)> {
        match self {
            CredentialSource::AwsDefaultChain => Ok((0, None)),
            CredentialSource::Refreshing(supply) => Ok((0, Some(supply()))),
            CredentialSource::Static(store) => {
                let (generation, secrets) = store.snapshot();
                let missing = |what: &str| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("the static credential source has no {what}"),
                    )
                };
                let id = secrets
                    .get(Self::ACCESS_KEY_ID)
                    .ok_or_else(|| missing(Self::ACCESS_KEY_ID))?;
                let access_key_id = id
                    .expose_str()
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "access key id is not UTF-8")
                    })?
                    .to_string();
                let secret_access_key = secrets
                    .get(Self::SECRET_ACCESS_KEY)
                    .cloned()
                    .ok_or_else(|| missing(Self::SECRET_ACCESS_KEY))?;
                let expiry = secrets
                    .get(Self::EXPIRY)
                    .and_then(|s| s.expose_str().and_then(|t| t.parse::<u64>().ok()))
                    .map(|secs| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(secs));
                Ok((
                    generation,
                    Some(Credential {
                        access_key_id,
                        secret_access_key,
                        session_token: secrets.get(Self::SESSION_TOKEN).cloned(),
                        expiry,
                    }),
                ))
            }
        }
    }

    /// Record that the S3 clients now sign with generation `generation`
    /// (a static source only; see [`EphemeralSecretStore::note_in_use`]).
    pub fn note_in_use(&self, generation: u64) {
        if let CredentialSource::Static(store) = self {
            store.note_in_use(generation);
        }
    }
}

impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CredentialSource::AwsDefaultChain => f.write_str("AwsDefaultChain"),
            CredentialSource::Static(store) => f.debug_tuple("Static").field(store).finish(),
            CredentialSource::Refreshing(_) => f.write_str("Refreshing(..)"),
        }
    }
}

/// Every store and source can be shared across threads.
const _: () = {
    const fn assert<T: Send + Sync>() {}
    assert::<FileSecretStore>();
    assert::<EphemeralSecretStore>();
    assert::<CredentialSource>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn files_under(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(files_under(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    #[test]
    fn file_store_round_trips_replaces_and_deletes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSecretStore::new(dir.path().join("sub"));
        assert_eq!(store.get("node.key").unwrap(), None);
        store.put("node.key", b"one").unwrap();
        assert_eq!(store.get("node.key").unwrap().unwrap().expose(), b"one");
        store.put("node.key", b"two").unwrap();
        assert_eq!(store.get("node.key").unwrap().unwrap().expose(), b"two");
        // Only the secret itself is left: no temp file, no lock file (a
        // plain put takes no lock).
        assert_eq!(
            files_under(dir.path()),
            vec![dir.path().join("sub").join("node.key")]
        );
        store.delete("node.key").unwrap();
        store.delete("node.key").unwrap();
        assert_eq!(store.get("node.key").unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn file_store_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let store = FileSecretStore::new(dir.path());
        // A stale, world-readable temp file from a crash does not leak its
        // mode into the secret.
        std::fs::write(dir.path().join("node.key.tmp"), "stale").unwrap();
        std::fs::set_permissions(
            dir.path().join("node.key.tmp"),
            std::fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        store.put("node.key", b"k").unwrap();
        store
            .update("registry.e2e.toml", &mut |_| Ok(Some(b"x".to_vec())))
            .unwrap();
        for name in ["node.key", "registry.e2e.toml"] {
            let mode = std::fs::metadata(dir.path().join(name))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "{name}");
        }
    }

    #[test]
    fn file_store_update_is_a_locked_read_modify_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(FileSecretStore::new(dir.path()));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || {
                    for _ in 0..25 {
                        store
                            .update("counter", &mut |old| {
                                let n: u64 = old
                                    .map(|b| std::str::from_utf8(b).unwrap().parse().unwrap())
                                    .unwrap_or(0);
                                Ok(Some((n + 1).to_string().into_bytes()))
                            })
                            .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(store.get("counter").unwrap().unwrap().expose(), b"200");
        // The lock file keeps the pre-plan-31 name.
        assert!(dir.path().join("counter.lock").exists());
        // `None` leaves the value alone.
        store.update("counter", &mut |_| Ok(None)).unwrap();
        assert_eq!(store.get("counter").unwrap().unwrap().expose(), b"200");
    }

    #[test]
    fn names_that_escape_the_store_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let file = FileSecretStore::new(dir.path());
        let mem = EphemeralSecretStore::new();
        for bad in ["", ".", "..", "a/b", "../x", "a\\b"] {
            assert_eq!(
                file.put(bad, b"x").unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
                "{bad:?}"
            );
            assert_eq!(
                mem.put(bad, b"x").unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn for_path_splits_directory_and_name() {
        let (store, name) = FileSecretStore::for_path(Path::new("/etc/c/node.key")).unwrap();
        assert_eq!(name, "node.key");
        assert_eq!(store.path(&name).unwrap(), PathBuf::from("/etc/c/node.key"));
        let (store, name) = FileSecretStore::for_path(Path::new("node.key")).unwrap();
        assert_eq!(store.path(&name).unwrap(), PathBuf::from("./node.key"));
        assert!(FileSecretStore::for_path(Path::new("/")).is_err());
    }

    /// The ephemeral store has no path to write to at all; check that a
    /// full workout leaves the working directory (the crate dir, which no
    /// other test writes to) exactly as it was.
    #[test]
    fn ephemeral_store_never_touches_disk() {
        let watched = std::env::current_dir().unwrap();
        let listing = |dir: &Path| {
            let mut names: Vec<_> = std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .map(|e| e.file_name())
                .collect();
            names.sort();
            names
        };
        let before = listing(&watched);
        let store = EphemeralSecretStore::new();
        store.put("node.key", b"k").unwrap();
        store
            .update("pins", &mut |old| {
                assert!(old.is_none());
                Ok(Some(b"p".to_vec()))
            })
            .unwrap();
        let handle = store.clone();
        assert_eq!(handle.get("node.key").unwrap().unwrap().expose(), b"k");
        assert_eq!(store.len(), 2);
        assert_eq!(store.describe("pins"), "pins (in memory)");
        store.delete("node.key").unwrap();
        assert_eq!(handle.get("node.key").unwrap(), None);
        drop(store);
        drop(handle);
        assert_eq!(listing(&watched), before);
        // Debug shows names, never values.
        let store = EphemeralSecretStore::new();
        store.put("s", b"hunter2").unwrap();
        let shown = format!("{store:?} {:?}", store.get("s").unwrap().unwrap());
        assert!(!shown.contains("hunter2"), "{shown}");
    }

    #[test]
    fn a_static_source_resolves_what_it_was_given() {
        let expiry = SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000);
        let mut credential = Credential::new("AKIA123", "s3cr3t");
        credential.session_token = Some(Secret::from("tok"));
        credential.expiry = Some(expiry);
        let source = CredentialSource::from_static(&credential);
        assert_eq!(source.resolve().unwrap(), Some(credential.clone()));
        assert!(!credential.is_expired(SystemTime::UNIX_EPOCH));
        assert!(credential.is_expired(expiry));
        let shown = format!("{source:?} {credential:?}");
        assert!(
            !shown.contains("s3cr3t") && !shown.contains("tok\""),
            "{shown}"
        );

        // Long-lived keys: no token, no expiry.
        let plain = Credential::new("AKIA", "k");
        assert_eq!(
            CredentialSource::from_static(&plain).resolve().unwrap(),
            Some(plain)
        );

        // Not unlocked yet: an error, not a guess.
        let empty = CredentialSource::Static(EphemeralSecretStore::new());
        assert_eq!(empty.resolve().unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn a_rotation_is_one_generation_and_never_half_applied() {
        let store = EphemeralSecretStore::new();
        assert_eq!(store.generation(), 0);
        let source = CredentialSource::Static(store.clone());
        store
            .replace(&[
                (CredentialSource::ACCESS_KEY_ID, Some(b"OLD")),
                (CredentialSource::SECRET_ACCESS_KEY, Some(b"old-secret")),
                (CredentialSource::SESSION_TOKEN, Some(b"old-token")),
            ])
            .unwrap();
        let (g1, c1) = source.resolve_generation().unwrap();
        assert_eq!((g1, c1.unwrap().access_key_id.as_str()), (1, "OLD"));
        // A rotation drops the token and swaps the pair under one bump.
        store
            .replace(&[
                (CredentialSource::ACCESS_KEY_ID, Some(b"NEW")),
                (CredentialSource::SECRET_ACCESS_KEY, Some(b"new-secret")),
                (CredentialSource::SESSION_TOKEN, None),
            ])
            .unwrap();
        let (g2, c2) = source.resolve_generation().unwrap();
        let c2 = c2.unwrap();
        assert_eq!(g2, 2);
        assert_eq!(c2.access_key_id, "NEW");
        assert_eq!(c2.secret_access_key.expose(), b"new-secret");
        assert!(c2.session_token.is_none());
        assert_eq!(source.generation(), 2);
        // What a reader uses is recorded, and never goes backwards.
        assert_eq!(store.in_use_generation(), 0);
        source.note_in_use(2);
        source.note_in_use(1);
        assert_eq!(store.in_use_generation(), 2);
        // put/delete/update are changes too; a no-op update is not.
        store.put("x", b"1").unwrap();
        store.delete("x").unwrap();
        store.update("x", &mut |_| Ok(None)).unwrap();
        assert_eq!(store.generation(), 4);
        assert!(!format!("{store:?}").contains("secret\""));
    }

    #[test]
    fn a_refreshing_source_asks_every_time_and_the_chain_defers() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = calls.clone();
        let source = CredentialSource::Refreshing(Box::new(move || {
            let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Credential::new(format!("AKIA{n}"), "k")
        }));
        assert_eq!(source.resolve().unwrap().unwrap().access_key_id, "AKIA0");
        assert_eq!(source.resolve().unwrap().unwrap().access_key_id, "AKIA1");
        assert_eq!(format!("{source:?}"), "Refreshing(..)");
        assert_eq!(CredentialSource::AwsDefaultChain.resolve().unwrap(), None);
    }
}
