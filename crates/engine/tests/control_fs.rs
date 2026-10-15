//! `fs.*` over the control service, against a real engine: the behaviour
//! the control side took from the filesystem manager when the CLI's and
//! the control service's implementations became one (plan 31 §4, "Plan 31
//! closing: one Manager for the CLI and the control service"). Each test
//! names what the control service did before.
//!
//! The service reads the process's registry (`CONSTELLATION_REGISTRY`) and
//! host dirs (`XDG_*`), so this is a test binary of its own: they are set
//! once, before the engine starts, and the tests share one engine and run
//! one at a time.

use constellation_control::fd::OwnedFd;
use constellation_control::proto::types::{
    HandoffParams, HandoffReport, HandoverStatus, ViewInfo, ViewMountParams,
};
use constellation_control::proto::{ControlError, ErrorKind};
use constellation_control::{Principal, Router};
use constellation_engine::control::{ControlHost, EngineControl, HostView};
use constellation_engine::{Engine, EngineConfig, EngineProfile, P2pMode};
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

/// A host with no views.
struct NoViews;

impl ControlHost for NoViews {
    fn views(&self) -> Vec<HostView> {
        Vec::new()
    }
    fn mount(&self, _: &ViewMountParams, _: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
        Err(ControlError::unsupported("no views here"))
    }
    fn unmount(&self, _: &Path) -> Result<String, ControlError> {
        Err(ControlError::unsupported("no views here"))
    }
    fn detach_all(&self) {}
    fn handover_status(&self) -> HandoverStatus {
        HandoverStatus::default()
    }
    fn handoff(
        &self,
        _: &HandoffParams,
        _: Option<OwnedFd>,
        _: Option<&constellation_control::authz::ServiceMatch>,
    ) -> Result<HandoffReport, ControlError> {
        Err(ControlError::unsupported("no handoff here"))
    }
}

struct Fixture {
    root: PathBuf,
    backend: String,
    rt: tokio::runtime::Runtime,
    router: Router,
    _svc: Arc<EngineControl>,
}

/// The one engine, and the lock that runs the tests one at a time.
fn fixture() -> (MutexGuard<'static, ()>, &'static Fixture) {
    static SERIAL: Mutex<()> = Mutex::new(());
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let f = FIXTURE.get_or_init(|| {
        let root = tempfile::tempdir().unwrap().keep();
        // SAFETY: set before the engine (or any thread of this binary that
        // reads them) starts; never changed afterwards.
        unsafe {
            std::env::set_var("CONSTELLATION_REGISTRY", root.join("config/registry.toml"));
            std::env::set_var("XDG_CONFIG_HOME", root.join("config"));
            std::env::set_var("XDG_DATA_HOME", root.join("data"));
            std::env::set_var("XDG_RUNTIME_DIR", root.join("run"));
        }
        let backend = root.join("own").display().to_string();
        std::fs::create_dir_all(&backend).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let store = ChunkStore::new(
            rt.block_on(constellation_engine::backend::open_backend(&backend))
                .unwrap(),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .unwrap();
        let engine = Arc::new(
            Engine::start(
                EngineConfig {
                    state_dir: Some(root.join("state")),
                    cache_size: 64 * 1024 * 1024,
                    runtime: Some(rt.handle().clone()),
                    ..EngineConfig::new(&backend)
                },
                HostServices::native(),
                EngineProfile {
                    p2p: P2pMode::Off,
                    ..EngineProfile::desktop()
                },
            )
            .unwrap(),
        );
        let svc = EngineControl::new(
            engine,
            Arc::new(NoViews),
            constellation_engine::log_buffer::LogBuffer::default(),
            Default::default(),
            "control-fs",
        );
        Fixture {
            router: constellation_engine::control::router(&svc),
            _svc: svc,
            root,
            backend,
            rt,
        }
    });
    (guard, f)
}

impl Fixture {
    fn call(&self, method: &str, params: Value) -> Result<Value, ControlError> {
        self.rt.block_on(constellation_control::dispatch_in_process(
            &self.router,
            &Principal::InProcess,
            method,
            params,
        ))
    }

    /// An empty directory to make a filesystem in.
    fn dir(&self, name: &str) -> String {
        let path = self.root.join("s3").join(name);
        std::fs::create_dir_all(&path).unwrap();
        path.display().to_string()
    }

    /// Register `name` at `url` without making a filesystem there (so a
    /// later `fs.unlock` can name it).
    fn import(&self, name: &str, url: &str) -> Result<Value, ControlError> {
        let doc = format!(
            "name = {name:?}\ns3 = {url:?}\nstate_dir = \"\"\nread_only_member = false\n\
             web_ui = 0\nmounts = []\n"
        );
        self.call("fs.import", json!({ "document": doc }))
    }

    /// `name`'s registry row, as `fs.export` writes it.
    fn row(&self, name: &str) -> toml::Value {
        let doc = self.call("fs.export", json!({ "fs": name })).unwrap();
        toml::from_str(doc["document"].as_str().unwrap()).unwrap()
    }

    fn pins(&self) -> String {
        std::fs::read_to_string(self.root.join("config/registry.e2e.toml")).unwrap_or_default()
    }

    /// An E2E filesystem made over the control service: registered,
    /// unlocked with `passphrase`, created.
    fn e2e(&self, name: &str, passphrase: &str) -> String {
        let url = self.dir(name);
        self.import(name, &url).unwrap();
        self.call(
            "fs.unlock",
            json!({ "fs": name, "credentials": { "e2e_passphrase": passphrase } }),
        )
        .unwrap();
        self.call(
            "fs.create",
            json!({ "name": name, "bucket": url, "e2e": true }),
        )
        .unwrap();
        url
    }
}

fn meta(url: &str) -> FsMeta {
    serde_json::from_slice(&std::fs::read(Path::new(url).join("meta.json")).unwrap()).unwrap()
}

/// Was: accepted and held, so a later named `fs.create{e2e}` made a
/// keyring under the empty passphrase, which `serve` then refused to
/// start from.
#[test]
fn an_empty_passphrase_is_not_unlocked() {
    let (_serial, f) = fixture();
    let e = f
        .call(
            "fs.unlock",
            json!({ "fs": f.backend, "credentials": { "e2e_passphrase": "" } }),
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Invalid, "{e:?}");
}

/// Was: a name already registered elsewhere was repointed at the new
/// filesystem whenever that one was new, though its state dir held the
/// old filesystem's replica (and refused only when it already existed).
#[test]
fn a_name_in_use_is_not_repointed() {
    let (_serial, f) = fixture();
    let (a, b) = (f.dir("busy-a"), f.dir("busy-b"));
    f.call("fs.create", json!({ "name": "busy", "bucket": a }))
        .unwrap();
    let state_dir = PathBuf::from(f.row("busy")["state_dir"].as_str().unwrap());
    std::fs::create_dir_all(state_dir.join("meta.db")).unwrap();
    let e = f
        .call("fs.create", json!({ "name": "busy", "bucket": b }))
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Conflict, "{e:?}");
    assert!(!Path::new(&b).join("meta.json").exists(), "made at {b}");
    assert_eq!(f.row("busy")["s3"].as_str(), Some(a.as_str()));
}

/// Was: only the CLI's `fs create` recorded the E2E pin.
#[test]
fn a_named_create_pins_its_e2e_state() {
    let (_serial, f) = fixture();
    let url = f.dir("pinned");
    f.call("fs.create", json!({ "name": "pinned", "bucket": url }))
        .unwrap();
    let pins: toml::Value = toml::from_str(&f.pins()).unwrap();
    assert_eq!(pins["pinned"]["e2e"].as_bool(), Some(false), "{pins}");
    assert_eq!(pins["pinned"]["s3"].as_str(), Some(url.as_str()));
}

/// Was: the keyring was rewrapped without being held against this host's
/// pin, so another filesystem's keyring spliced in under a reused
/// passphrase went unnoticed.
#[test]
fn passwd_holds_the_keyring_against_the_pin() {
    let (_serial, f) = fixture();
    let url = f.e2e("sealed", "pw");
    assert!(meta(&url).gossip_secret.is_none());
    // The first change pins the master key the passphrase opens (a local
    // directory then cannot take the conditional update itself).
    let _ = f.call(
        "fs.passwd",
        json!({ "fs": "sealed", "old_passphrase": "pw", "new_passphrase": "pw2" }),
    );
    let pins: toml::Value = toml::from_str(&f.pins()).unwrap();
    assert!(pins["sealed"].get("master_fingerprint").is_some(), "{pins}");
    let other = f.e2e("intruder", "pw");
    let mut spliced = meta(&url);
    spliced.keyring = meta(&other).keyring;
    std::fs::write(
        Path::new(&url).join("meta.json"),
        serde_json::to_vec_pretty(&spliced).unwrap(),
    )
    .unwrap();
    let e = f
        .call(
            "fs.passwd",
            json!({ "fs": "sealed", "old_passphrase": "pw", "new_passphrase": "pw3" }),
        )
        .unwrap_err();
    assert!(
        e.message.contains("other than the one this machine pinned"),
        "{e:?}"
    );
}

/// Was: `backend`, `meta.json`, and only then `conditional_writes`; a
/// prefix with no filesystem got no capability check at all, and no CAS
/// semantics were probed.
#[test]
fn doctor_checks_the_backend_before_the_filesystem() {
    let (_serial, f) = fixture();
    let url = f.dir("bare");
    f.import("bare", &url).unwrap();
    let report = f.call("fs.doctor", json!({ "fs": "bare" })).unwrap();
    let checks: Vec<(String, bool)> = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["check"].as_str().unwrap().to_string(),
                c["ok"].as_bool().unwrap(),
            )
        })
        .collect();
    let names: Vec<&str> = checks.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "backend",
            "conditional_writes",
            "cas_semantics",
            "versioning",
            "meta.json"
        ],
        "{report}"
    );
    assert!(checks[..4].iter().all(|(_, ok)| *ok), "{report}");
    assert!(!checks[4].1, "{report}");
}

/// Was: any name was registered, `..` included — whose default state dir
/// is the data dir's parent, which `export` deletes.
#[test]
fn import_refuses_a_name_that_is_not_one_path_component() {
    let (_serial, f) = fixture();
    let url = f.dir("dots");
    for name in ["..", "a/b", "a:b"] {
        let e = f.import(name, &url).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Invalid, "{name:?}: {e:?}");
    }
    let list = f.call("fs.list", json!({})).unwrap();
    assert!(
        !list["filesystems"]
            .as_array()
            .unwrap()
            .iter()
            .any(|fs| fs["name"] == json!("..")),
        "{list}"
    );
}
