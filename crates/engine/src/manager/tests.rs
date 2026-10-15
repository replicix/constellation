//! The filesystem manager, one operation at a time, against a private
//! registry, private host dirs and local-directory backends: no process
//! environment is read or changed (`Manager::at`, [`TestDirs`]).

use super::*;
use crate::e2e_pin::{self, PinTarget};
use crate::registry::{FsOverrides, MountEntry};
use constellation_control::proto::types::UnlockCredentials;
use constellation_control::proto::{ErrorKind, Secret};
use constellation_store_s3::{ChunkStore, FsMeta, PreflightCheck};
use object_store::ObjectStoreExt;
use std::path::Path;
use std::sync::Mutex;
use zeroize::Zeroizing;

/// Host dirs under one temp dir.
struct TestDirs(PathBuf);

impl constellation_platform::Dirs for TestDirs {
    fn config_dir(&self) -> std::io::Result<PathBuf> {
        Ok(self.0.join("config"))
    }
    fn data_dir(&self) -> std::io::Result<PathBuf> {
        Ok(self.0.join("data"))
    }
    fn runtime_dir(&self) -> std::io::Result<PathBuf> {
        Ok(self.0.join("run"))
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    manager: Manager,
    rt: tokio::runtime::Runtime,
}

/// [`TestDirs`] whose data dir cannot be located (no `HOME`, say).
struct NoDataDir(PathBuf);

impl constellation_platform::Dirs for NoDataDir {
    fn config_dir(&self) -> std::io::Result<PathBuf> {
        Ok(self.0.join("config"))
    }
    fn data_dir(&self) -> std::io::Result<PathBuf> {
        Err(std::io::Error::new(std::io::ErrorKind::NotFound, "no HOME"))
    }
    fn runtime_dir(&self) -> std::io::Result<PathBuf> {
        Ok(self.0.join("run"))
    }
}

impl Fixture {
    fn new() -> Fixture {
        Fixture::with_dirs(|root| Arc::new(TestDirs(root)))
    }

    fn with_dirs(dirs: impl FnOnce(PathBuf) -> Arc<dyn constellation_platform::Dirs>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let host = HostServices {
            dirs: dirs(dir.path().to_path_buf()),
            ..HostServices::native()
        };
        let manager = Manager::at(dir.path().join("config/registry.toml"), host)
            .with_meta_wait(Duration::from_secs(2));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        Fixture { dir, manager, rt }
    }

    /// A local backend URL (an existing, empty directory).
    fn backend(&self, name: &str) -> String {
        let path = self.dir.path().join("s3").join(name);
        std::fs::create_dir_all(&path).unwrap();
        path.display().to_string()
    }

    fn meta_json(&self, url: &str) -> PathBuf {
        Path::new(url).join("meta.json")
    }

    fn create(&self, req: CreateRequest, hooks: &mut Hooks) -> Result<Created, ControlError> {
        self.rt.block_on(self.manager.create(req, hooks))
    }

    fn named(&self, name: &str, url: &str, existing: Existing) -> CreateRequest {
        CreateRequest {
            name: Some(name.to_string()),
            ..CreateRequest::new(url, existing)
        }
    }

    fn pin(&self, key: &str) -> Option<e2e_pin::E2ePin> {
        e2e_pin::pinned(&self.manager.pins().unwrap(), key).unwrap()
    }

    fn passwd(&self, name: &str, old: &str, new: &str) -> Result<PasswdOutcome, ControlError> {
        let url = self.manager.entry(name).unwrap().unwrap().s3;
        let req = PasswdRequest {
            pin: PinTarget::named(name, &url),
            url,
            credentials: None,
        };
        let (old, new) = (old.to_string(), new.to_string());
        let mut secrets = move || Ok((Zeroizing::new(old.clone()), Zeroizing::new(new.clone())));
        self.rt.block_on(self.manager.passwd(req, &mut secrets))
    }

    fn export(
        &self,
        name: &str,
        force: bool,
        daemons: &FakeDaemons,
    ) -> Result<Vec<ExportNote>, ControlError> {
        let notes = Mutex::new(Vec::new());
        let mut note = |n| notes.lock().unwrap().push(n);
        self.rt
            .block_on(self.manager.export(name, force, daemons, &mut note))?;
        Ok(notes.into_inner().unwrap())
    }

    /// A registered filesystem whose state dir exists (a daemon log in
    /// it) but holds no replica.
    fn registered(&self, name: &str) -> PathBuf {
        let url = self.backend(name);
        self.create(
            self.named(name, &url, Existing::Refuse),
            &mut Hooks::default(),
        )
        .unwrap();
        let dir = self.manager.entry(name).unwrap().unwrap().state_dir;
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("daemon.log"), b"x").unwrap();
        dir
    }
}

/// What a create asked for, and the passphrase it is given.
#[derive(Default)]
struct Hooks {
    passphrase: Option<String>,
    asked: usize,
    preflights: usize,
}

impl Hooks {
    fn with(passphrase: &str) -> Hooks {
        Hooks {
            passphrase: Some(passphrase.to_string()),
            ..Hooks::default()
        }
    }
}

impl CreateHooks for Hooks {
    fn preflight(&mut self, checks: &[PreflightCheck]) {
        assert!(!checks.is_empty());
        self.preflights += 1;
    }
    fn passphrase(&mut self) -> Result<Zeroizing<String>, ControlError> {
        self.asked += 1;
        self.passphrase
            .clone()
            .map(Zeroizing::new)
            .ok_or_else(|| ControlError::invalid("aborted at the prompt"))
    }
}

fn read_meta(path: &Path) -> FsMeta {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[test]
fn create_registers_pins_and_keeps_the_gossip_secret_of_an_e2e_filesystem_private() {
    let f = Fixture::new();
    let plain = f.backend("plain");
    let mut hooks = Hooks::default();
    let created = f
        .create(f.named("plain", &plain, Existing::Refuse), &mut hooks)
        .unwrap();
    assert!(created.created && !created.raced);
    assert_eq!((hooks.preflights, hooks.asked), (1, 0));
    assert!(read_meta(&f.meta_json(&plain)).gossip_secret.is_some());
    let entry = f.manager.entry("plain").unwrap().unwrap();
    assert_eq!(entry.s3, plain);
    // The default state dir is the manager's host's.
    assert_eq!(entry.state_dir, f.dir.path().join("data/plain"));
    assert!(!f.pin("plain").unwrap().e2e);

    let secret = f.backend("secret");
    let mut hooks = Hooks::with("pw");
    let req = CreateRequest {
        e2e: true,
        ..f.named("secret", &secret, Existing::Refuse)
    };
    let created = f.create(req, &mut hooks).unwrap();
    assert_eq!(hooks.asked, 1);
    let meta = read_meta(&f.meta_json(&secret));
    assert!(meta.e2e && meta.keyring.is_some());
    assert_eq!(
        meta.gossip_secret, None,
        "an E2E gossip secret in meta.json"
    );
    assert_eq!(meta.uuid, created.meta.uuid);
    meta.unlock("pw").unwrap();
    let pin = f.pin("secret").unwrap();
    assert!(pin.e2e && pin.master_fingerprint.is_none());
}

#[test]
fn an_aborted_or_empty_passphrase_writes_nothing() {
    let f = Fixture::new();
    let url = f.backend("e2e");
    for passphrase in [None, Some("")] {
        let mut hooks = Hooks {
            passphrase: passphrase.map(str::to_string),
            ..Hooks::default()
        };
        let req = CreateRequest {
            e2e: true,
            ..f.named("e2e", &url, Existing::Compare)
        };
        let e = f.create(req, &mut hooks).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Invalid, "{e:?}");
        assert!(
            !f.meta_json(&url).exists(),
            "{passphrase:?} left a meta.json"
        );
        assert!(f.manager.entry("e2e").unwrap().is_none());
    }
}

#[test]
fn an_existing_filesystem_is_refused_compared_or_accepted() {
    let f = Fixture::new();
    let url = f.backend("fs");
    let first = f
        .create(f.named("fs", &url, Existing::Refuse), &mut Hooks::default())
        .unwrap();

    // `fs create`: refused before any preflight or prompt.
    let mut hooks = Hooks::with("pw");
    let e = f
        .create(f.named("fs", &url, Existing::Refuse), &mut hooks)
        .unwrap_err();
    assert_eq!(e.code, Some(Code::Exists));
    assert!(e.message.contains("already exists"), "{}", e.message);
    assert_eq!((hooks.preflights, hooks.asked), (0, 0));

    // `fs.create`: the same parameters answer the same uuid ...
    let again = f
        .create(f.named("fs", &url, Existing::Compare), &mut hooks)
        .unwrap();
    assert!(!again.created);
    assert_eq!(again.meta.uuid, first.meta.uuid);
    // ... other ones name what is there.
    let req = CreateRequest {
        chunk_size: Some(2 * 1024 * 1024),
        e2e: true,
        ..f.named("fs", &url, Existing::Compare)
    };
    let e = f.create(req.clone(), &mut hooks).unwrap_err();
    assert_eq!(e.code, Some(Code::Exists));
    let details = e.details.unwrap().0;
    let existing = &details["existing"];
    assert!(existing.get("chunk_size").is_some() && existing.get("e2e").is_some());
    assert_eq!((hooks.preflights, hooks.asked), (0, 0));

    // `serve --create`: whatever is there.
    let accepted = f
        .create(
            CreateRequest {
                existing: Existing::Accept,
                ..req
            },
            &mut hooks,
        )
        .unwrap();
    assert_eq!(accepted.meta.uuid, first.meta.uuid);
}

#[test]
fn a_name_is_repointed_only_while_its_state_dir_is_not_in_use() {
    let f = Fixture::new();
    let (a, b, c) = (f.backend("a"), f.backend("b"), f.backend("c"));
    f.create(f.named("fs", &a, Existing::Refuse), &mut Hooks::default())
        .unwrap();
    // Never mounted: the test suites' fixed names are reused this way.
    f.create(f.named("fs", &b, Existing::Refuse), &mut Hooks::default())
        .unwrap();
    let entry = f.manager.entry("fs").unwrap().unwrap();
    assert_eq!(entry.s3, b);

    // A replica in its state dir: refused, before anything is made at
    // the new URL.
    std::fs::create_dir_all(entry.state_dir.join("meta.db")).unwrap();
    for existing in [Existing::Refuse, Existing::Compare] {
        let e = f
            .create(f.named("fs", &c, existing), &mut Hooks::default())
            .unwrap_err();
        assert_eq!(e.code, Some(Code::Exists), "{e:?}");
        assert!(e.message.contains("is in use"), "{}", e.message);
        assert_eq!(e.details.unwrap().0["s3"], serde_json::json!(b));
        assert!(!f.meta_json(&c).exists());
        assert_eq!(f.manager.entry("fs").unwrap().unwrap().s3, b);
    }
    // A daemon still starting (its lock held, no replica yet): `mount
    // --s3` refuses too.
    std::fs::remove_dir_all(entry.state_dir.join("meta.db")).unwrap();
    let lock =
        constellation_platform::lock::open_lock_file(&entry.state_dir.join("daemon.lock")).unwrap();
    let held = constellation_platform::native()
        .file_lock
        .try_lock(lock)
        .unwrap()
        .unwrap();
    let mut reg = f.manager.registry_locked().unwrap();
    let e = f
        .manager
        .save_mount(&mut reg, "fs", Some(&c), None, FsOverrides::default())
        .unwrap_err();
    assert!(e.message.contains("is in use"), "{}", e.message);
    drop(held);
    let entry = f
        .manager
        .save_mount(&mut reg, "fs", Some(&c), None, FsOverrides::default())
        .unwrap();
    assert_eq!(entry.s3, c);
}

/// A name that comes into use while a create of it elsewhere is under way
/// (here, during the passphrase prompt) makes nothing at the new URL. It
/// used to: the second check ran after the conditional PUT, which left an
/// unregistered filesystem there that a retry called "already exists".
#[test]
fn a_name_that_comes_into_use_during_a_create_makes_nothing() {
    struct Mounted(PathBuf);
    impl CreateHooks for Mounted {
        fn passphrase(&mut self) -> Result<Zeroizing<String>, ControlError> {
            std::fs::create_dir_all(self.0.join("meta.db")).unwrap();
            Ok(Zeroizing::new("pw".into()))
        }
    }
    let f = Fixture::new();
    let (a, b) = (f.backend("a"), f.backend("b"));
    f.create(f.named("fs", &a, Existing::Refuse), &mut Hooks::default())
        .unwrap();
    let state_dir = f.manager.entry("fs").unwrap().unwrap().state_dir;
    let req = CreateRequest {
        e2e: true,
        ..f.named("fs", &b, Existing::Refuse)
    };
    let e =
        f.rt.block_on(f.manager.create(req, &mut Mounted(state_dir)))
            .unwrap_err();
    assert_eq!(e.code, Some(Code::Exists), "{e:?}");
    assert!(!f.meta_json(&b).exists(), "made at the new URL");
    assert_eq!(f.manager.entry("fs").unwrap().unwrap().s3, a);
}

#[test]
fn a_name_is_one_path_component() {
    let f = Fixture::new();
    let url = f.backend("x");
    for name in ["", ".", "..", "a/b", "../x", "a:b", "a\nb"] {
        let e = f
            .create(f.named(name, &url, Existing::Refuse), &mut Hooks::default())
            .unwrap_err();
        assert_eq!(e.kind, ErrorKind::Invalid, "{name:?}: {e:?}");
        assert!(!f.meta_json(&url).exists(), "{name:?}");
        let doc = format!(
            "name = {name:?}\ns3 = {url:?}\nstate_dir = \"\"\nread_only_member = false\n\
             web_ui = 0\nmounts = []\n"
        );
        assert_eq!(
            f.manager.import(&doc, None).unwrap_err().kind,
            ErrorKind::Invalid,
            "{name:?}"
        );
        let mut reg = f.manager.registry_locked().unwrap();
        let e = f
            .manager
            .save_mount(&mut reg, name, Some(&url), None, FsOverrides::default())
            .unwrap_err();
        assert_eq!(e.kind, ErrorKind::Invalid, "{name:?}");
    }
    assert!(f.manager.list().unwrap().is_empty());
}

#[test]
fn a_document_exported_on_one_host_imports_on_another() {
    let f = Fixture::new();
    let url = f.backend("fs");
    f.create(f.named("fs", &url, Existing::Refuse), &mut Hooks::default())
        .unwrap();
    let mut reg = f.manager.registry_locked().unwrap();
    f.manager
        .save_mount(
            &mut reg,
            "fs",
            None,
            None,
            FsOverrides {
                cache_size: Some("2G".into()),
                mount: Some(MountEntry {
                    subtree: "/data".into(),
                    mountpoint: "/mnt/data".into(),
                    fs_name: "fs".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .unwrap();
    drop(reg);
    let doc = f.manager.export_document("fs").unwrap();
    assert_eq!(
        f.manager.export_document("nope").unwrap_err().kind,
        ErrorKind::NotFound
    );

    let other = Fixture::new();
    let (name, entry) = other.manager.import(&doc, Some("copy".into())).unwrap();
    assert_eq!(name, "copy");
    assert_eq!(entry.s3, url);
    assert_eq!(entry.cache_size, "2G");
    assert_eq!(entry.mounts.len(), 1);
    // The state dir is the importing host's, never the document's.
    assert_eq!(entry.state_dir, other.dir.path().join("data/copy"));
    // Again: the same row. Another URL under that name: refused.
    other.manager.import(&doc, Some("copy".into())).unwrap();
    let elsewhere = doc.replace(&url, "/elsewhere");
    let e = other
        .manager
        .import(&elsewhere, Some("copy".into()))
        .unwrap_err();
    assert_eq!(e.code, Some(Code::Exists));
    assert_eq!(other.manager.entry("copy").unwrap().unwrap().s3, url);
}

/// An E2E filesystem in memory (a local directory cannot take
/// `meta.json`'s conditional update), pinned as `fs create` pins it.
fn in_memory_e2e(f: &Fixture, passphrase: &str, pin: &PinTarget) -> ChunkStore {
    let store = ChunkStore::new(Arc::new(object_store::memory::InMemory::new()));
    let mut meta = FsMeta::new(1024 * 1024, "raw");
    meta.e2e = true;
    meta.gossip_secret = None;
    meta.keyring = Some(constellation_store_s3::create_keyring_block(passphrase).unwrap());
    f.rt.block_on(store.create_fs(&meta)).unwrap();
    e2e_pin::record_with(f.manager.pins(), pin, &meta, None);
    store
}

impl Fixture {
    fn passwd_in(
        &self,
        store: &ChunkStore,
        pin: &PinTarget,
        old: &str,
        new: &str,
    ) -> Result<PasswdOutcome, ControlError> {
        let (old, new) = (old.to_string(), new.to_string());
        let mut secrets = move || Ok((Zeroizing::new(old.clone()), Zeroizing::new(new.clone())));
        self.rt
            .block_on(self.manager.passwd_in(store, pin, &mut secrets))
    }
}

#[test]
fn passwd_rewraps_the_master_and_refuses_a_spliced_keyring() {
    let f = Fixture::new();
    let pin = PinTarget::named("fs", "mem://fs");
    let store = in_memory_e2e(&f, "one", &pin);
    let meta = || f.rt.block_on(store.load_fs()).unwrap();
    let wrong = f.passwd_in(&store, &pin, "nope", "two").unwrap_err();
    assert!(
        wrong.message.contains("changing E2E passphrase"),
        "{wrong:?}"
    );
    assert_eq!(
        f.passwd_in(&store, &pin, "one", "").unwrap_err().kind,
        ErrorKind::Invalid
    );
    let outcome = f.passwd_in(&store, &pin, "one", "two").unwrap();
    assert!(outcome.warning.is_none(), "{outcome:?}");
    meta().unlock("two").unwrap();
    assert!(meta().unlock("one").is_err());
    // The first unlock pinned the master key.
    assert!(f.pin("fs").unwrap().master_fingerprint.is_some());

    // Another filesystem's keyring under the same passphrase, spliced
    // into this one's meta.json: refused before anything is rewrapped
    // (it used to be rewrapped and its master pinned).
    let other = in_memory_e2e(&f, "two", &PinTarget::named("other", "mem://other"));
    let mut spliced = meta();
    spliced.keyring = f.rt.block_on(other.load_fs()).unwrap().keyring;
    let path = object_store::path::Path::from("meta.json");
    f.rt.block_on(
        store
            .inner()
            .put(&path, serde_json::to_vec_pretty(&spliced).unwrap().into()),
    )
    .unwrap();
    let e = f.passwd_in(&store, &pin, "two", "three").unwrap_err();
    assert!(
        e.message.contains("other than the one this machine pinned"),
        "{e:?}"
    );
    meta().unlock("two").unwrap();
    assert!(meta().unlock("three").is_err());

    // Plaintext: nothing to change (on a local directory, by URL).
    let plain = f.backend("plain");
    f.create(
        f.named("plain", &plain, Existing::Refuse),
        &mut Hooks::default(),
    )
    .unwrap();
    let e = f.passwd("plain", "a", "b").unwrap_err();
    assert_eq!(e.message, "filesystem is not in E2E mode");
}

/// The keyring rewrapped is the keyring checked: one swapped into
/// `meta.json` after the check (here, while the passphrases are asked
/// for) is not rewrapped. It was: the change re-read `meta.json` and
/// rewrapped whatever block it found there.
#[test]
fn passwd_rewraps_only_the_keyring_it_checked() {
    let f = Fixture::new();
    let pin = PinTarget::named("fs", "mem://fs");
    let store = in_memory_e2e(&f, "pw", &pin);
    let other = in_memory_e2e(&f, "pw", &PinTarget::named("other", "mem://other"));
    let mut spliced = f.rt.block_on(store.load_fs()).unwrap();
    spliced.keyring = f.rt.block_on(other.load_fs()).unwrap().keyring;
    let spliced = serde_json::to_vec_pretty(&spliced).unwrap();
    let inner = store.inner().clone();
    let mut secrets = move || {
        futures::executor::block_on(inner.put(
            &object_store::path::Path::from("meta.json"),
            spliced.clone().into(),
        ))
        .unwrap();
        Ok((Zeroizing::new("pw".into()), Zeroizing::new("pw2".into())))
    };
    let e =
        f.rt.block_on(f.manager.passwd_in(&store, &pin, &mut secrets))
            .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Unavailable, "{e:?}");
    let now = f.rt.block_on(store.load_fs()).unwrap();
    now.unlock("pw").unwrap();
    assert!(
        now.unlock("pw2").is_err(),
        "the swapped keyring was rewrapped"
    );
}

#[test]
fn doctor_reports_each_step() {
    let f = Fixture::new();
    let url = f.backend("fs");
    let report = f.rt.block_on(f.manager.doctor(&url, None)).unwrap();
    assert!(report.usable(), "{report:?}");
    match &report.filesystem {
        Some(FsState::Missing(_)) => {}
        other => panic!("{other:?}"),
    }
    let created = f
        .create(
            CreateRequest::new(&url, Existing::Refuse),
            &mut Hooks::default(),
        )
        .unwrap();
    let report = f.rt.block_on(f.manager.doctor(&url, None)).unwrap();
    match &report.filesystem {
        Some(FsState::Present { uuid, .. }) => assert_eq!(*uuid, created.meta.uuid.to_string()),
        other => panic!("{other:?}"),
    }
    assert!(report.cas.unwrap().unwrap().probes.len() > 1);
}

/// A daemon the export talks to, scripted.
struct FakeDaemons {
    answers: bool,
    leave: Mutex<Option<Result<Result<String, ControlError>, ControlError>>>,
    left: Mutex<Vec<bool>>,
}

impl FakeDaemons {
    fn new(answers: bool, leave: Result<Result<String, ControlError>, ControlError>) -> Self {
        FakeDaemons {
            answers,
            leave: Mutex::new(Some(leave)),
            left: Mutex::new(Vec::new()),
        }
    }
}

impl Daemons for FakeDaemons {
    fn answers<'a>(&'a self, _: &'a Path) -> futures::future::BoxFuture<'a, bool> {
        Box::pin(async move { self.answers })
    }
    fn leave<'a>(
        &'a self,
        _: &'a Path,
        force: bool,
    ) -> futures::future::BoxFuture<'a, Result<Result<String, ControlError>, ControlError>> {
        self.left.lock().unwrap().push(force);
        let answer = self.leave.lock().unwrap().take().expect("one leave");
        Box::pin(async move { answer })
    }
    fn socket_exists(&self, _: &Path) -> bool {
        false
    }
}

#[test]
fn export_of_a_never_mounted_name_drops_the_row_and_the_dir() {
    let f = Fixture::new();
    let dir = f.registered("fs");
    let daemons = FakeDaemons::new(false, Ok(Ok(String::new())));
    assert_eq!(f.export("fs", false, &daemons).unwrap(), vec![]);
    assert!(
        daemons.left.lock().unwrap().is_empty(),
        "left without a node id"
    );
    assert!(!dir.exists());
    assert!(f.manager.entry("fs").unwrap().is_none());
    let e = f.export("fs", false, &daemons).unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotFound);
}

#[test]
fn export_leaves_through_a_live_daemon_and_stops_at_its_refusal() {
    let f = Fixture::new();
    let dir = f.registered("fs");
    let refusing = FakeDaemons::new(true, Ok(Err(ControlError::failed("busy"))));
    let e = f.export("fs", false, &refusing).unwrap_err();
    assert_eq!(e.message, "leave failed: busy");
    assert!(dir.exists() && f.manager.entry("fs").unwrap().is_some());

    let refusing = FakeDaemons::new(true, Ok(Err(ControlError::failed("busy"))));
    let notes = f.export("fs", true, &refusing).unwrap();
    assert_eq!(notes, vec![ExportNote::LeaveRefused("busy".into())]);
    assert_eq!(*refusing.left.lock().unwrap(), vec![true]);
    assert!(!dir.exists() && f.manager.entry("fs").unwrap().is_none());

    let dir = f.registered("fs2");
    let leaving = FakeDaemons::new(true, Ok(Ok("left".into())));
    assert_eq!(
        f.export("fs2", false, &leaving).unwrap(),
        vec![ExportNote::Left("left".into())]
    );
    assert!(!dir.exists());
}

#[test]
fn export_does_not_retire_a_starting_daemon_unless_forced() {
    let f = Fixture::new();
    let dir = f.registered("fs");
    // A node id on record, and a daemon that holds the lock but answers
    // nothing.
    let meta = constellation_meta::Meta::open(dir.join("meta.db")).unwrap();
    meta.kv_set_durable("node_id", "42").unwrap();
    drop(meta);
    let lock = constellation_platform::lock::open_lock_file(&dir.join("daemon.lock")).unwrap();
    let held = constellation_platform::native()
        .file_lock
        .try_lock(lock)
        .unwrap()
        .unwrap();
    let unreachable = FakeDaemons::new(false, Err(ControlError::unavailable("no socket")));
    let e = f.export("fs", false, &unreachable).unwrap_err();
    assert!(e.message.contains("is held by a live process"), "{e:?}");
    assert!(dir.join("meta.db").exists());
    assert!(f.manager.entry("fs").unwrap().is_some());
    drop(held);
}

#[test]
fn export_never_deletes_a_dir_that_holds_another_names() {
    let f = Fixture::new();
    let inner = f.registered("inner");
    // A row written before names were checked: `mount ..` made the data
    // dir's parent a state dir.
    let path = f.manager.registry_path().unwrap();
    let mut text = std::fs::read_to_string(&path).unwrap();
    text.push_str(&format!(
        "\n[outer]\ns3 = \"/nowhere\"\nstate_dir = {:?}\nread_only_member = false\nweb_ui = 0\n\
         mounts = []\n",
        f.dir.path().join("data/..").display().to_string()
    ));
    std::fs::write(&path, text).unwrap();
    let daemons = FakeDaemons::new(false, Ok(Ok(String::new())));
    let e = f.export("outer", false, &daemons).unwrap_err();
    assert_eq!(e.kind, ErrorKind::Denied);
    assert!(e.message.contains("refusing to delete it"), "{e:?}");
    assert!(inner.exists() && path.exists());
    assert!(f.manager.entry("outer").unwrap().is_some());
}

/// The runtime dir, another name's cache dir, and a host dir that cannot
/// be located also stop the deletion (they did not).
#[test]
fn export_never_deletes_the_runtime_dir_or_a_cache_dir_and_needs_the_host_dirs() {
    let f = Fixture::new();
    let outer = f.registered("outer");
    let daemons = || FakeDaemons::new(false, Ok(Ok(String::new())));
    // The name's state dir is the runtime dir.
    let path = f.manager.registry_path().unwrap();
    let rows = std::fs::read_to_string(&path).unwrap();
    let run = f.dir.path().join("run");
    std::fs::create_dir_all(&run).unwrap();
    let stored = format!("state_dir = {:?}", outer.display().to_string());
    let moved = format!("state_dir = {:?}", run.display().to_string());
    std::fs::write(&path, rows.replace(&stored, &moved)).unwrap();
    let e = f.export("outer", false, &daemons()).unwrap_err();
    assert!(e.message.contains("holds the runtime dir"), "{e:?}");
    assert!(run.exists());
    std::fs::write(&path, &rows).unwrap();

    // Another name's cache dir inside it.
    let inner = f.backend("inner");
    f.create(
        f.named("inner", &inner, Existing::Refuse),
        &mut Hooks::default(),
    )
    .unwrap();
    let mut reg = f.manager.registry_locked().unwrap();
    f.manager
        .save_mount(
            &mut reg,
            "inner",
            None,
            None,
            FsOverrides {
                cache_dir: Some(outer.join("cache-of-inner").display().to_string()),
                ..Default::default()
            },
        )
        .unwrap();
    drop(reg);
    let e = f.export("outer", false, &daemons()).unwrap_err();
    assert!(e.message.contains("the cache dir of inner"), "{e:?}");
    assert!(outer.exists());

    // A host whose data dir cannot be located deletes nothing.
    let g = Fixture::with_dirs(|root| Arc::new(NoDataDir(root)));
    let url = g.backend("fs");
    let mut reg = g.manager.registry_locked().unwrap();
    let state_dir = g.dir.path().join("state");
    g.manager
        .save_mount(
            &mut reg,
            "fs",
            Some(&url),
            Some(&state_dir),
            FsOverrides::default(),
        )
        .unwrap();
    drop(reg);
    std::fs::create_dir_all(&state_dir).unwrap();
    let e = g.export("fs", false, &daemons()).unwrap_err();
    assert!(e.message.contains("cannot locate the data dir"), "{e:?}");
    assert!(state_dir.exists());
}

#[test]
fn list_and_resolve_read_the_managers_registry() {
    let f = Fixture::new();
    let url = f.backend("fs");
    f.create(f.named("fs", &url, Existing::Refuse), &mut Hooks::default())
        .unwrap();
    let names: Vec<_> = f
        .manager
        .list()
        .unwrap()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, vec!["fs".to_string()]);
    match f.manager.resolve("fs:/data").unwrap() {
        crate::target::Target::Named { name, path, entry } => {
            assert_eq!((name.as_str(), path.as_deref()), ("fs", Some("/data")));
            assert_eq!(entry.s3, url);
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        f.manager.resolve("other").unwrap(),
        crate::target::Target::Raw(_)
    ));
}

#[test]
fn an_unlock_carries_a_pair_or_a_passphrase_and_never_an_empty_one() {
    let secret = |s: &str| Some(Secret::new(s));
    let half = UnlockCredentials {
        access_key_id: secret("id"),
        ..Default::default()
    };
    assert!(credentials::validate(&half)
        .unwrap_err()
        .message
        .contains("go together"));
    assert!(credentials::validate(&UnlockCredentials::default()).is_err());
    let empty = UnlockCredentials {
        e2e_passphrase: secret(""),
        ..Default::default()
    };
    assert_eq!(
        credentials::validate(&empty).unwrap_err().kind,
        ErrorKind::Invalid
    );
    let full = UnlockCredentials {
        access_key_id: secret("id"),
        secret_access_key: secret("key"),
        e2e_passphrase: secret("pw"),
        ..Default::default()
    };
    assert!(credentials::validate(&full).unwrap());
    let store = constellation_platform::EphemeralSecretStore::new();
    store.replace(&credentials::entries(&full)).unwrap();
    assert_eq!(
        credentials::passphrase(&store).unwrap().unwrap().as_str(),
        "pw"
    );
    let source = CredentialSource::Static(store);
    assert!(credentials::signs(&source));
    // A passphrase alone signs with the environment's chain.
    let passphrase_only = constellation_platform::EphemeralSecretStore::new();
    passphrase_only
        .replace(&credentials::entries(&UnlockCredentials {
            e2e_passphrase: secret("pw"),
            ..Default::default()
        }))
        .unwrap();
    assert!(!credentials::signs(&CredentialSource::Static(
        passphrase_only
    )));
    assert!(credentials::signs(&CredentialSource::AwsDefaultChain));
    assert_eq!(
        credentials::source_passphrase(&source)
            .unwrap()
            .unwrap()
            .as_str(),
        "pw"
    );
}
