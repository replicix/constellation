//! Plan 32 M2 end to end: binding a snapshot policy to a directory
//! through a real engine's control service, in process.
//!
//! One `Engine` (local backend, P2P off) behind an `EngineControl` with
//! no kernel views; every call goes through the method table as an
//! in-process admin, so `browse.xattr` and `snapshot.policy.set/remove/
//! pause` reach the service's own View exactly as they do from the CLI
//! and the web UI. Asserted: the setxattr gate (directories only, never
//! scratch, parseable or `EINVAL` with the reason in `node.status`), the
//! verbatim storage of a plain setxattr, `set`'s server-side expiry guard
//! and dry run, the pause round trip, `remove` orphaning (never deleting)
//! the root's auto snapshots, and `node.status`'s `snapsched` section.

use super::*;
use crate::{EngineConfig, EngineProfile, P2pMode};
use constellation_control::proto::{ByteBuf, ErrorKind};
use constellation_meta::snapsched::SNAPSHOT_POLICY_XATTR;
use constellation_meta::SnapshotRow;
use constellation_platform::HostServices;
use constellation_store_s3::{ChunkStore, FsMeta};
use constellation_types::Code;
use serde_json::{json, Value};

/// A host with no views (the control service opens its own).
struct NoViews;

impl ControlHost for NoViews {
    fn views(&self) -> Vec<HostView> {
        Vec::new()
    }
    fn mount(&self, _: &ViewMountParams, _: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
        Err(ControlError::unsupported("no mounts here"))
    }
    fn unmount(&self, _: &Path) -> Result<String, ControlError> {
        Err(ControlError::unsupported("no mounts here"))
    }
    fn detach_all(&self) {}
    fn handover_status(&self) -> HandoverStatus {
        HandoverStatus::default()
    }
    fn handoff(
        &self,
        _: &HandoffParams,
        _: Option<OwnedFd>,
    ) -> Result<HandoffReport, ControlError> {
        Err(ControlError::unsupported("no handoff here"))
    }
}

struct Fixture {
    rt: tokio::runtime::Runtime,
    engine: Arc<Engine>,
    svc: Arc<EngineControl>,
    router: Router,
    _dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let backend = format!("file://{}", dir.path().join("backend").display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let store = ChunkStore::new(
            rt.block_on(crate::backend::open_backend(&backend))
                .expect("open backend"),
        );
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .expect("create_fs");
        let engine = Arc::new(
            Engine::start(
                EngineConfig {
                    state_dir: Some(dir.path().join("state")),
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
            .expect("Engine::start"),
        );
        let svc = EngineControl::new(
            engine.clone(),
            Arc::new(NoViews),
            crate::log_buffer::LogBuffer::default(),
            Arc::new(crate::prefetch::PrefetchStats::default()),
            "snapsched-tests",
        );
        Fixture {
            router: router(&svc),
            svc,
            rt,
            engine,
            _dir: dir,
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, ControlError> {
        self.rt.block_on(constellation_control::dispatch_in_process(
            &self.router,
            &Principal::InProcess,
            method,
            params,
        ))
    }

    fn ok(&self, method: &str, params: Value) -> Value {
        self.call(method, params.clone())
            .unwrap_or_else(|e| panic!("{method} {params}: {e:?}"))
    }

    fn mkdir(&self, path: &str) -> u64 {
        self.ok("browse.mkdir", json!({"path": path}))["ino"]
            .as_u64()
            .unwrap()
    }

    fn setxattr(&self, path: &str, name: &str, value: &str) -> Result<Value, ControlError> {
        let value = serde_json::to_value(ByteBuf::from(value.as_bytes().to_vec())).unwrap();
        self.call(
            "browse.xattr",
            json!({"path": path, "op": {"Set": {"name": name, "value": value}}}),
        )
    }

    /// The policy xattr as the View reads it back.
    fn policy(&self, path: &str) -> Option<String> {
        match self.call(
            "browse.xattr",
            json!({"path": path, "op": {"Get": {"name": SNAPSHOT_POLICY_XATTR}}}),
        ) {
            Ok(v) => {
                let bytes: ByteBuf = serde_json::from_value(v["value"].clone()).unwrap();
                Some(String::from_utf8(bytes.0.to_vec()).unwrap())
            }
            Err(e) if e.kind == ErrorKind::NotFound => None,
            Err(e) => panic!("get {path}: {e:?}"),
        }
    }

    fn last_parse_error(&self) -> Value {
        self.ok("node.status", json!({}))["snapsched"]["last_parse_error"].clone()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.engine.shutdown();
    }
}

fn invalid(r: Result<Value, ControlError>) -> ControlError {
    let e = r.expect_err("refused");
    assert_eq!(
        (e.kind, e.code),
        (ErrorKind::Invalid, Some(Code::Invalid)),
        "{e:?}"
    );
    e
}

#[test]
fn the_setxattr_gate_admits_only_parseable_policies_on_shared_directories() {
    let f = Fixture::new();
    f.mkdir("/proj");
    f.ok(
        "browse.write",
        json!({"path": "/proj/file", "data": "eA==", "create": true}),
    );

    // Before anything is refused, the section is there and empty.
    let status = f.ok("node.status", json!({}));
    let section = status["snapsched"].as_object().expect("snapsched section");
    for key in [
        "ticks",
        "leader",
        "roots",
        "paused_roots",
        "unparseable_roots",
        "capped_roots",
        "orphaned_snapshots",
        "created",
        "skipped_empty",
        "create_failed",
        "expired",
        "skipped_reverify",
        "skipped_grace",
        "budget_expired",
        "budget_stale",
        "refused_lag",
        "refused_state",
        "last_create_unix_ms",
        "last_error",
        "last_parse_error",
    ] {
        assert!(section.contains_key(key), "node.status.snapsched.{key}");
    }
    assert_eq!(section.len(), 20, "{section:?}");
    assert_eq!(section["ticks"], json!(0));
    assert_eq!(section["last_parse_error"], Value::Null);

    // A file cannot carry a policy.
    invalid(f.setxattr("/proj/file", SNAPSHOT_POLICY_XATTR, "1h:1d"));
    assert!(f.last_parse_error()[2]
        .as_str()
        .unwrap()
        .contains("directory"));

    // An unparseable expression: EINVAL, the reason and offset in status.
    invalid(f.setxattr("/proj", SNAPSHOT_POLICY_XATTR, "1h:1d 7m:1d"));
    let last = f.last_parse_error();
    assert_eq!(last[0], json!("1h:1d 7m:1d"));
    assert_eq!(last[1], json!(6));
    assert!(last[2].as_str().unwrap().contains("7m"), "{last}");
    assert_eq!(f.policy("/proj"), None);

    // A good one is stored verbatim (what a setxattr wrote reads back).
    f.setxattr("/proj", SNAPSHOT_POLICY_XATTR, "1d:7d   1h:1d")
        .unwrap();
    assert_eq!(f.policy("/proj").as_deref(), Some("1d:7d   1h:1d"));
    let listed = f.ok("snapshot.policy.list", json!({}));
    assert_eq!(listed["roots"][0]["path"], json!("/proj"));
    assert_eq!(listed["roots"][0]["expr"], json!("1d:7d   1h:1d"));
    assert_eq!(listed["roots"][0]["canonical"], json!("1h:1d 1d:7d"));

    // Scratch: neither a scratch root nor anything inside one.
    f.mkdir("/tmp");
    f.setxattr("/tmp", constellation_meta::SCRATCH_XATTR, "1")
        .unwrap();
    f.mkdir("/tmp/sub");
    invalid(f.setxattr("/tmp", SNAPSHOT_POLICY_XATTR, "1h:1d"));
    assert!(f.last_parse_error()[2]
        .as_str()
        .unwrap()
        .contains("scratch"));
    invalid(f.setxattr("/tmp/sub", SNAPSHOT_POLICY_XATTR, "1h:1d"));
    // ... and a policy root cannot become a scratch root.
    invalid(f.setxattr("/proj", constellation_meta::SCRATCH_XATTR, "1"));

    // `snapshot.policy.set` meets the same gate, with the reason in the
    // message rather than only in status.
    let e = f
        .call(
            "snapshot.policy.set",
            json!({"path": "/tmp", "expr": "1h:1d"}),
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Invalid, "{e:?}");
    assert!(e.message.contains("scratch"), "{e:?}");
    let e = f
        .call(
            "snapshot.policy.set",
            json!({"path": "/proj", "expr": "1h:1d 7m:1d"}),
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Invalid, "{e:?}");
    assert_eq!(e.details.unwrap().0["offset"], json!(6));
    assert_eq!(f.policy("/proj").as_deref(), Some("1d:7d   1h:1d"));
}

#[test]
fn set_guards_expiry_pause_round_trips_and_remove_orphans() {
    let f = Fixture::new();
    let proj = f.mkdir("/proj");
    const HOUR: i64 = 3_600_000;
    let now = constellation_store_s3::lease::now_unix_ms();
    // 48 hourly auto snapshots of /proj, and a manual one.
    let meta = f.engine.meta();
    for h in 0..48 {
        let id = format!("a{h:02}");
        meta.record_snapshot(&SnapshotRow {
            origin: 1,
            policy_ino: proj,
            ..SnapshotRow::new(&id, "/proj", &id, "mtree:0:00:1", now - (48 - h) * HOUR)
        })
        .unwrap();
    }
    meta.record_snapshot(&SnapshotRow::new(
        "manual",
        "/proj",
        "manual",
        "mtree:0:00:1",
        now - 47 * HOUR,
    ))
    .unwrap();
    let set = |expr: &str, extra: Value| {
        let mut p = json!({"path": "/proj", "expr": expr});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        f.call("snapshot.policy.set", p)
    };

    // Unbound: auto snapshots of a directory with no policy are orphaned.
    let listed = f.ok("snapshot.policy.list", json!({}));
    assert_eq!(listed["roots"].as_array().unwrap().len(), 1);
    assert_eq!(listed["roots"][0]["ino"], json!(proj));
    assert_eq!(listed["roots"][0]["orphaned"], json!(true));
    assert_eq!(listed["roots"][0]["auto_snapshots"], json!(48));

    // A dry run reports the delta and writes nothing.
    let delta = set("1d:2d 1h:1d", json!({"dry_run": true})).unwrap();
    assert_eq!(delta["written"], json!(false));
    assert_eq!(delta["canonical"], json!("1h:1d 1d:2d"));
    assert_eq!(delta["creates_every"], json!("1h"));
    assert_eq!(delta["previous"], Value::Null);
    assert_eq!(f.policy("/proj"), None);

    // `1h:1d` keeps the newest day of hourlies and expires the older 24.
    let delta = set("1h:1d", json!({"dry_run": true})).unwrap();
    assert_eq!(delta["would_expire"], json!(24), "{delta}");
    let ids = delta["would_expire_ids"].as_array().unwrap();
    assert_eq!(ids.len(), 24);
    assert_eq!(ids[0], json!("a00"));
    assert!(!ids.contains(&json!("manual")));
    assert!(delta["grace_note"].as_str().unwrap().contains("not active"));

    // Unconfirmed, or confirmed for another count: refused, nothing written.
    for extra in [json!({}), json!({"confirm_expiring": 23})] {
        let e = set("1h:1d", extra.clone()).unwrap_err();
        assert_eq!(e.kind, ErrorKind::Conflict, "{extra}: {e:?}");
        assert_eq!(e.details.as_ref().unwrap().0["would_expire"], json!(24));
        assert_eq!(e.details.unwrap().0["written"], json!(false));
        assert_eq!(f.policy("/proj"), None, "{extra}");
    }
    // Confirmed for exactly the delta: written, canonical.
    let delta = set("1h:1d;  last=1", json!({"confirm_expiring": 24})).unwrap();
    assert_eq!(delta["written"], json!(true));
    assert_eq!(delta["would_expire"], json!(24));
    assert_eq!(f.policy("/proj").as_deref(), Some("1h:1d"));
    assert_eq!(
        meta.snapshots(None).unwrap().len(),
        49,
        "set deletes nothing"
    );
    // A change expiring nothing needs no confirmation, and reports what
    // it replaces.
    let delta = set("1h:3d", json!({})).unwrap();
    assert_eq!(
        (delta["written"].clone(), delta["previous"].clone()),
        (json!(true), json!("1h:1d"))
    );

    // Show: the root and its verdicts.
    let shown = f.ok("snapshot.policy.show", json!({"path": "/proj"}));
    assert_eq!(shown["root"]["canonical"], json!("1h:3d"));
    assert_eq!(shown["root"]["orphaned"], json!(false));
    assert_eq!(shown["verdicts"]["snapshots"], json!(49));
    assert_eq!(shown["verdicts"]["would_expire"], json!(0));

    // Pause and resume: the canonical expression with/without `paused`.
    let paused = f.ok(
        "snapshot.policy.pause",
        json!({"path": "/proj", "paused": true}),
    );
    assert_eq!(paused["paused"], json!(true));
    assert_eq!(f.policy("/proj").as_deref(), Some("1h:3d; paused"));
    let listed = f.ok("snapshot.policy.list", json!({}));
    assert_eq!(listed["roots"][0]["paused"], json!(true));
    let resumed = f.ok(
        "snapshot.policy.pause",
        json!({"path": "/proj", "paused": false}),
    );
    assert_eq!(resumed["paused"], json!(false));
    assert_eq!(f.policy("/proj").as_deref(), Some("1h:3d"));

    // A rename keeps the root: its identity is the inode.
    f.ok("browse.rename", json!({"from": "/proj", "to": "/projects"}));
    let listed = f.ok("snapshot.policy.list", json!({}));
    assert_eq!(listed["roots"].as_array().unwrap().len(), 1);
    assert_eq!(listed["roots"][0]["ino"], json!(proj));
    assert_eq!(listed["roots"][0]["path"], json!("/projects"));
    assert_eq!(listed["roots"][0]["auto_snapshots"], json!(48));

    // `remove {expire}` waits for M4; plain `remove` orphans and keeps.
    let e = f
        .call(
            "snapshot.policy.remove",
            json!({"path": "/projects", "expire": true, "confirm_expiring": 48}),
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::Unsupported, "{e:?}");
    assert_eq!(f.policy("/projects").as_deref(), Some("1h:3d"));
    let removed = f.ok("snapshot.policy.remove", json!({"path": "/projects"}));
    assert_eq!(removed["orphaned"], json!(true));
    assert_eq!(removed["expr"], json!(""));
    assert_eq!(f.policy("/projects"), None);
    assert_eq!(meta.snapshots(None).unwrap().len(), 49, "nothing deleted");
    let listed = f.ok("snapshot.policy.list", json!({}));
    assert_eq!(listed["roots"][0]["orphaned"], json!(true));
    let e = f
        .call("snapshot.policy.remove", json!({"path": "/projects"}))
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotFound, "{e:?}");
    let e = f
        .call(
            "snapshot.policy.pause",
            json!({"path": "/projects", "paused": true}),
        )
        .unwrap_err();
    assert_eq!(e.kind, ErrorKind::NotFound, "{e:?}");
    // Show still answers for the orphaned stream, without verdicts.
    let shown = f.ok("snapshot.policy.show", json!({"path": "/projects"}));
    assert_eq!(shown["verdicts"], Value::Null);
    assert_eq!(shown["root"]["orphaned"], json!(true));
}

#[test]
fn no_directory_at_or_above_a_policy_root_may_become_scratch() {
    let f = Fixture::new();
    f.mkdir("/a");
    f.mkdir("/a/b");
    f.mkdir("/a/b/c");
    f.mkdir("/other");
    f.setxattr("/a/b", SNAPSHOT_POLICY_XATTR, "1h:1d").unwrap();

    // The root itself, and every ancestor of it, are refused, with a
    // reason that says it was the scratch marking that failed.
    let marking = format!("{}=1", constellation_meta::SCRATCH_XATTR);
    for path in ["/a/b", "/a"] {
        invalid(f.setxattr(path, constellation_meta::SCRATCH_XATTR, "1"));
        let last = f.last_parse_error();
        assert_eq!(last[0], json!(marking), "{path}: {last}");
        let msg = last[2].as_str().unwrap();
        assert!(msg.starts_with("scratch refused"), "{path}: {msg}");
        assert!(msg.contains("1h:1d"), "{path}: {msg}");
    }
    // Below a root, or beside one, scratch is still allowed.
    f.setxattr("/a/b/c", constellation_meta::SCRATCH_XATTR, "1")
        .unwrap();
    f.setxattr("/other", constellation_meta::SCRATCH_XATTR, "1")
        .unwrap();
    // Once the policy is gone, so is the objection.
    f.ok("snapshot.policy.remove", json!({"path": "/a/b"}));
    f.setxattr("/a", constellation_meta::SCRATCH_XATTR, "1")
        .unwrap();
}

#[test]
fn set_reports_the_gates_own_reason_not_the_shared_status_slot() {
    let f = Fixture::new();
    f.mkdir("/tmp");
    f.setxattr("/tmp", constellation_meta::SCRATCH_XATTR, "1")
        .unwrap();
    // Some other refusal (a concurrent setfattr, say) left its reason in
    // the node-wide slot.
    f.engine
        .snapsched_stats()
        .record_parse_error("x", 0, "SOMEONE ELSE'S REASON");
    let e = invalid(f.call(
        "snapshot.policy.set",
        json!({"path": "/tmp", "expr": "1h:1d"}),
    ));
    assert!(e.message.contains("scratch"), "{e:?}");
    assert!(!e.message.contains("SOMEONE ELSE"), "{e:?}");
    // ... and `set`'s pre-check does not overwrite the slot either.
    assert_eq!(f.last_parse_error()[2], json!("SOMEONE ELSE'S REASON"));
}

#[test]
fn a_write_to_a_directory_renamed_since_evaluation_is_a_conflict() {
    let f = Fixture::new();
    let old = f.mkdir("/proj");
    f.ok("browse.rename", json!({"from": "/proj", "to": "/moved"}));
    let new = f.mkdir("/proj");
    assert_ne!(old, new);
    // The caller evaluated inode `old` at /proj; /proj now names `new`.
    let vfs = f.svc.browser(&Principal::InProcess).unwrap();
    for value in [Some("1h:1d"), None] {
        let e = vfs
            .snapshot_policy("/proj", old, value)
            .expect_err("conflict");
        assert_eq!(e.kind, ErrorKind::Conflict, "{e:?}");
    }
    assert_eq!(f.policy("/proj"), None);
    assert_eq!(f.policy("/moved"), None);
    // Addressed to the inode it evaluated, the write lands.
    vfs.snapshot_policy("/proj", new, Some("1h:1d")).unwrap();
    assert_eq!(f.policy("/proj").as_deref(), Some("1h:1d"));
}
