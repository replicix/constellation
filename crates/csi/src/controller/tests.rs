use super::*;
use crate::control_client::{InMemoryControl, InMemoryEngines};
use crate::proto::csi::v1::volume_capability::{AccessMode, BlockVolume, MountVolume};
use constellation_control::proto::types::{
    CloneParams, FsCreateParams, SnapshotCreateParams, SnapshotHoldParams,
};
use tonic::Code as GrpcCode;

const GIB: i64 = 1 << 30;

struct Fixture {
    engines: Arc<InMemoryEngines>,
    service: ControllerService,
}

fn fixture_with(config: ControllerConfig) -> Fixture {
    let engines = Arc::new(InMemoryEngines::default());
    let service = ControllerService::new(Some(engines.clone() as Arc<dyn Engines>), config);
    Fixture { engines, service }
}

/// Fast retries, so the exhaustion tests do not sleep for seconds.
fn fixture() -> Fixture {
    fixture_with(ControllerConfig {
        quota_retry: QuotaRetry {
            attempts: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        },
        ..ControllerConfig::default()
    })
}

fn mount_cap(mode: Mode) -> VolumeCapability {
    VolumeCapability {
        access_type: Some(AccessType::Mount(MountVolume::default())),
        access_mode: Some(AccessMode { mode: mode as i32 }),
    }
}

fn class(kv: &[(&str, &str)]) -> HashMap<String, String> {
    kv.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn create_req(name: &str, required: i64) -> CreateVolumeRequest {
    CreateVolumeRequest {
        name: name.to_string(),
        capacity_range: Some(CapacityRange {
            required_bytes: required,
            limit_bytes: 0,
        }),
        volume_capabilities: vec![mount_cap(Mode::MultiNodeMultiWriter)],
        parameters: class(&[
            ("bucket", "b"),
            ("prefix", "pool"),
            (PVC_NAME_KEY, "data"),
            (PVC_NAMESPACE_KEY, "team"),
        ]),
        ..Default::default()
    }
}

impl Fixture {
    async fn create(&self, req: CreateVolumeRequest) -> Result<Volume, Status> {
        self.service
            .create_volume(Request::new(req))
            .await
            .map(|r| r.into_inner().volume.unwrap())
    }

    async fn delete(&self, volume_id: &str) -> Result<(), Status> {
        self.service
            .delete_volume(Request::new(DeleteVolumeRequest {
                volume_id: volume_id.to_string(),
                secrets: HashMap::new(),
            }))
            .await
            .map(drop)
    }

    async fn expand(&self, volume_id: &str, required: i64) -> Result<i64, Status> {
        self.service
            .controller_expand_volume(Request::new(ControllerExpandVolumeRequest {
                volume_id: volume_id.to_string(),
                capacity_range: Some(CapacityRange {
                    required_bytes: required,
                    limit_bytes: 0,
                }),
                ..Default::default()
            }))
            .await
            .map(|r| r.into_inner().capacity_bytes)
    }

    /// The concrete per-filesystem fake behind a volume id.
    async fn fs_of(&self, volume_id: &str) -> Arc<InMemoryControl> {
        let id = VolumeId::parse(volume_id).unwrap();
        self.engines
            .filesystem(id.fs_uuid(), &Default::default())
            .await
            .unwrap();
        self.engines.filesystem_client(id.fs_uuid()).unwrap()
    }
}

async fn record(fs: &InMemoryControl, path: &str) -> BTreeMap<String, String> {
    read_record(fs, path).await.unwrap()
}

#[tokio::test]
async fn create_writes_the_record_and_the_quota() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert_eq!(v.capacity_bytes, GIB);
    let id = VolumeId::parse(&v.volume_id).unwrap();
    let VolumeId::Pool { shard, name, .. } = &id else {
        panic!("{id:?}")
    };
    assert_eq!((*shard, name.as_str()), (0, "pvc-1"));
    // The context locates the pool for NodeStageVolume (K3): the class's
    // own keys, none of the sidecars'.
    assert_eq!(
        v.volume_context,
        class(&[("bucket", "b"), ("prefix", "pool")]),
        "{:?}",
        v.volume_context
    );
    let again = f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert_eq!(
        again.volume_context, v.volume_context,
        "idempotent, context too"
    );

    // The pool is the class's (bucket, prefix) filesystem.
    let pool = f
        .engines
        .registry_client()
        .fs_create(FsCreateParams {
            bucket: "b".into(),
            prefix: "pool".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!pool.created);
    assert_eq!(pool.uuid, id.fs_uuid());

    let fs = f.fs_of(&v.volume_id).await;
    let rec = record(&fs, "/volumes/pvc-1").await;
    assert_eq!(rec[X_PV], "pvc-1");
    assert_eq!(rec[X_PVC], "data");
    assert_eq!(rec[X_NAMESPACE], "team");
    assert_eq!(rec[X_CAPACITY], GIB.to_string());
    assert_eq!(rec[X_SOURCE], "");
    assert!(rec[X_CREATED].parse::<u64>().unwrap() > 0);
    assert_eq!(
        fs.quota_get("/volumes/pvc-1").await.unwrap().max_bytes,
        Some(GIB as u64)
    );
    // The pool's own root carries no cap: the quota is per volume.
    assert_eq!(fs.quota_get("/").await.unwrap().max_bytes, None);
}

/// Plan 37 K6a: every call a CSI RPC makes to an engine carries the PV
/// it is about (`on_behalf_of`), so the engine's audit line for it is
/// attributable; calls outside an RPC carry none.
#[tokio::test]
async fn every_engine_call_of_an_rpc_names_its_volume() {
    let f = fixture();
    let v = f.create(create_req("pvc-attr", GIB)).await.unwrap();
    f.expand(&v.volume_id, 2 * GIB).await.unwrap();
    f.delete(&v.volume_id).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    let calls = fs.calls();
    let made_by_rpcs: Vec<_> = calls.iter().filter(|(m, _)| *m != "fs.create").collect();
    assert!(made_by_rpcs.len() >= 6, "{calls:?}");
    for (method, who) in &made_by_rpcs {
        // `fs_of`'s own lookup happens outside any RPC.
        if who.is_none() {
            continue;
        }
        assert_eq!(who.as_deref(), Some("pvc-attr"), "{method}");
    }
    for m in ["browse.mkdir", "quota.set", "browse.rename"] {
        assert!(
            calls
                .iter()
                .any(|(c, who)| *c == m && who.as_deref() == Some("pvc-attr")),
            "{m} not attributed: {calls:?}"
        );
    }
    assert_eq!(attribution_of("pvc-1 with\nnewline"), "pvc-1_with_newline");
    assert_eq!(
        attribution(&v.volume_id),
        "pvc-attr",
        "a pool volume's id names its PV"
    );
}

#[tokio::test]
async fn no_capacity_means_no_cap() {
    let f = fixture();
    let mut req = create_req("pvc-free", 0);
    req.capacity_range = None;
    let v = f.create(req).await.unwrap();
    assert_eq!(v.capacity_bytes, 0);
    let fs = f.fs_of(&v.volume_id).await;
    assert_eq!(
        fs.quota_get("/volumes/pvc-free").await.unwrap().max_bytes,
        None
    );

    // A limit alone is the size to use (the largest the caller allows).
    let mut req = create_req("pvc-limit", 0);
    req.capacity_range = Some(CapacityRange {
        required_bytes: 0,
        limit_bytes: 5 * GIB,
    });
    assert_eq!(f.create(req).await.unwrap().capacity_bytes, 5 * GIB);
}

#[tokio::test]
async fn shards_route_by_name_to_their_own_filesystems() {
    let f = fixture();
    let mut uuids = BTreeMap::new();
    for i in 0..16 {
        let mut req = create_req(&format!("pvc-{i}"), GIB);
        req.parameters.insert("shards".into(), "4".into());
        let v = f.create(req).await.unwrap();
        let VolumeId::Pool { shard, fs_uuid, .. } = VolumeId::parse(&v.volume_id).unwrap() else {
            unreachable!()
        };
        // One filesystem per shard, at <prefix>/shard-<k>.
        let expected = f
            .engines
            .registry_client()
            .fs_create(FsCreateParams {
                bucket: "b".into(),
                prefix: format!("pool/shard-{shard}"),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(expected.uuid, fs_uuid);
        uuids.insert(shard, fs_uuid);
    }
    assert!(uuids.len() > 1, "16 names all hashed to one shard");
}

#[tokio::test]
async fn create_is_idempotent_and_refuses_incompatible_repeats() {
    let f = fixture();
    let first = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let again = f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert_eq!(again, first);

    // A range the stored size satisfies is the same volume.
    let mut range = create_req("pvc-1", GIB / 2);
    range.capacity_range.as_mut().unwrap().limit_bytes = 2 * GIB;
    assert_eq!(f.create(range).await.unwrap().capacity_bytes, GIB);

    for (required, limit) in [(2 * GIB, 0), (GIB / 4, GIB / 2)] {
        let mut req = create_req("pvc-1", required);
        req.capacity_range.as_mut().unwrap().limit_bytes = limit;
        let e = f.create(req).await.unwrap_err();
        assert_eq!(e.code(), GrpcCode::AlreadyExists, "{required}/{limit}: {e}");
    }
    // An uncapped volume does not satisfy a sized request, nor vice versa.
    let mut free = create_req("pvc-free", 0);
    free.capacity_range = None;
    f.create(free).await.unwrap();
    let e = f.create(create_req("pvc-free", GIB)).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::AlreadyExists);
}

#[tokio::test]
async fn an_interrupted_create_is_completed_in_place() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    // Simulate an attempt that died before its commit mark (and with a
    // different size than the retry asks for — it never returned success).
    fs.browse_xattr(XattrParams {
        path: "/volumes/pvc-1".into(),
        op: XattrOp::Remove {
            name: X_CREATED.into(),
        },
    })
    .await
    .unwrap();
    let again = f.create(create_req("pvc-1", 3 * GIB)).await.unwrap();
    assert_eq!(again.capacity_bytes, 3 * GIB);
    assert_eq!(again.volume_id, v.volume_id);
    let rec = record(&fs, "/volumes/pvc-1").await;
    assert!(rec.contains_key(X_CREATED));
    assert_eq!(rec[X_CAPACITY], (3 * GIB).to_string());
    assert_eq!(
        fs.quota_get("/volumes/pvc-1").await.unwrap().max_bytes,
        Some(3 * GIB as u64)
    );
}

#[tokio::test]
async fn a_directory_owned_by_another_volume_is_never_adopted() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.browse_mkdir(MkdirParams {
        path: "/volumes/pvc-2".into(),
        mode: None,
        parents: false,
    })
    .await
    .unwrap();
    set_xattr(fs.as_ref(), "/volumes/pvc-2", X_PV, "someone-else")
        .await
        .unwrap();
    let e = f.create(create_req("pvc-2", GIB)).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::AlreadyExists);
}

#[tokio::test]
async fn transient_quota_failures_are_retried() {
    let f = fixture();
    // Create the pool first so the fault can be injected into its client.
    let v = f.create(create_req("pvc-0", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    let before = fs.quota_set_calls();
    fs.fail_next_quota_sets(3);
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert_eq!(fs.quota_set_calls() - before, 4);
    assert!(record(&fs, "/volumes/pvc-1").await.contains_key(X_CREATED));
    assert_eq!(v.capacity_bytes, GIB);
}

#[tokio::test]
async fn exhausted_quota_retries_are_aborted_then_resumable() {
    let f = fixture();
    let v = f.create(create_req("pvc-0", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    let before = fs.quota_set_calls();
    fs.fail_next_quota_sets(4);
    let e = f.create(create_req("pvc-1", GIB)).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::Aborted, "{e}");
    assert_eq!(fs.quota_set_calls() - before, 4, "bounded by attempts");
    // Everything before quota.set is committed; the commit mark is not.
    let rec = record(&fs, "/volumes/pvc-1").await;
    assert_eq!(rec[X_PV], "pvc-1");
    assert!(!rec.contains_key(X_CREATED));
    // The sidecar's retry finishes the job.
    f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert!(record(&fs, "/volumes/pvc-1").await.contains_key(X_CREATED));
}

#[tokio::test]
async fn bad_requests_are_invalid_argument() {
    let f = fixture();
    let mut cases: Vec<(&str, CreateVolumeRequest)> = Vec::new();
    cases.push(("empty name", create_req("", GIB)));
    cases.push(("slash in name", create_req("a/b", GIB)));
    let mut r = create_req("pvc", GIB);
    r.volume_capabilities.clear();
    cases.push(("no capabilities", r));
    let mut r = create_req("pvc", GIB);
    r.volume_capabilities = vec![VolumeCapability {
        access_type: Some(AccessType::Block(BlockVolume {})),
        access_mode: Some(AccessMode {
            mode: Mode::SingleNodeWriter as i32,
        }),
    }];
    cases.push(("block", r));
    let mut r = create_req("pvc", GIB);
    r.volume_capabilities = vec![mount_cap(Mode::Unknown)];
    cases.push(("unknown mode", r));
    let mut r = create_req("pvc", -1);
    r.capacity_range.as_mut().unwrap().limit_bytes = 0;
    cases.push(("negative", r));
    let mut r = create_req("pvc", 2 * GIB);
    r.capacity_range.as_mut().unwrap().limit_bytes = GIB;
    cases.push(("inverted range", r));
    let mut r = create_req("pvc", GIB);
    r.parameters.remove("bucket");
    cases.push(("missing bucket", r));
    let mut r = create_req("pvc", GIB);
    r.parameters.insert("shards".into(), "0".into());
    cases.push(("bad shards", r));
    let mut r = create_req("pvc", GIB);
    r.parameters.insert("layout".into(), "zfs".into());
    cases.push(("bad layout", r));
    for (what, req) in cases {
        let e = f.create(req).await.unwrap_err();
        assert_eq!(e.code(), GrpcCode::InvalidArgument, "{what}: {e}");
    }
}

fn dedicated_req(name: &str, required: i64) -> CreateVolumeRequest {
    let mut r = create_req(name, required);
    r.parameters.insert("layout".into(), "dedicated".into());
    r
}

/// Should-fix 4 of the 37-k3a review: `layout: dedicated` creates a whole
/// filesystem per volume (§2.3); this driver does not delete filesystems
/// (the purge worker empties pool trash only, 37-k6b).
#[tokio::test]
async fn a_dedicated_volume_is_a_filesystem_of_its_own() {
    let f = fixture();
    let v = f.create(dedicated_req("pvc-d", GIB)).await.unwrap();
    let id = VolumeId::parse(&v.volume_id).unwrap();
    assert!(matches!(id, VolumeId::Dedicated { .. }), "{id:?}");
    assert_eq!(id.subtree(), "/");
    // The context locates the volume's own filesystem for the node.
    assert_eq!(
        v.volume_context,
        class(&[
            ("bucket", "b"),
            ("prefix", "pool/pvc-d"),
            ("layout", "dedicated")
        ])
    );
    let own = f
        .engines
        .registry_client()
        .fs_create(FsCreateParams {
            bucket: "b".into(),
            prefix: "pool/pvc-d".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!own.created);
    assert_eq!(own.uuid, id.fs_uuid());
    let fs = f.fs_of(&v.volume_id).await;
    let rec = record(&fs, "/").await;
    assert_eq!(rec[X_PV], "pvc-d");
    assert_eq!(rec[X_PVC], "data");
    assert!(rec.contains_key(X_CREATED));
    assert_eq!(fs.quota_get("/").await.unwrap().max_bytes, Some(GIB as u64));

    // Idempotent; an incompatible repeat is refused.
    let again = f.create(dedicated_req("pvc-d", GIB)).await.unwrap();
    assert_eq!(again.volume_id, v.volume_id);
    assert_eq!(again.volume_context, v.volume_context);
    assert_eq!(
        f.create(dedicated_req("pvc-d", 4 * GIB))
            .await
            .unwrap_err()
            .code(),
        GrpcCode::AlreadyExists
    );
    // Another volume, another filesystem.
    let other = f.create(dedicated_req("pvc-e", 0)).await.unwrap();
    assert_ne!(
        VolumeId::parse(&other.volume_id).unwrap().fs_uuid(),
        id.fs_uuid()
    );
    // Expansion is the filesystem-wide cap.
    assert_eq!(f.expand(&v.volume_id, 2 * GIB).await.unwrap(), 2 * GIB);
    assert_eq!(
        fs.quota_get("/").await.unwrap().max_bytes,
        Some(2 * GIB as u64)
    );
    // Filesystems are never deleted: refused, clearly, and nothing is
    // touched.
    let e = f.delete(&v.volume_id).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::FailedPrecondition, "{e}");
    assert!(e.message().contains("does not delete filesystems"), "{e}");
    assert_eq!(record(&fs, "/").await[X_PV], "pvc-d");
}

#[tokio::test]
async fn an_empty_content_source_is_invalid() {
    let f = fixture();
    let mut r = create_req("pvc", GIB);
    r.volume_content_source = Some(VolumeContentSource { r#type: None });
    assert_eq!(
        f.create(r).await.unwrap_err().code(),
        GrpcCode::InvalidArgument
    );
}

#[tokio::test]
async fn without_a_backend_volume_rpcs_are_unavailable() {
    let service = ControllerService::new(None, ControllerConfig::default());
    let e = service
        .create_volume(Request::new(create_req("pvc", GIB)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), GrpcCode::Unavailable);
    // Static answers still work.
    service
        .controller_get_capabilities(Request::new(ControllerGetCapabilitiesRequest {}))
        .await
        .unwrap();
}

#[tokio::test]
async fn a_second_call_for_the_same_volume_is_aborted() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let held = f.service.locks.try_lock("pvc-1".into()).unwrap();
    let e = f.create(create_req("pvc-1", GIB)).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::Aborted);
    assert!(e.message().contains("already in progress"), "{e}");
    // Delete and expand of that volume share the name's key.
    assert_eq!(
        f.delete(&v.volume_id).await.unwrap_err().code(),
        GrpcCode::Aborted
    );
    assert_eq!(
        f.expand(&v.volume_id, 2 * GIB).await.unwrap_err().code(),
        GrpcCode::Aborted
    );
    // Other volumes are unaffected; releasing the key frees this one.
    f.create(create_req("pvc-2", GIB)).await.unwrap();
    drop(held);
    f.create(create_req("pvc-1", GIB)).await.unwrap();
}

#[tokio::test]
async fn creates_queue_behind_the_per_pool_bound() {
    let f = fixture_with(ControllerConfig {
        pool_create_concurrency: 1,
        ..ControllerConfig::default()
    });
    let gate = f.service.pool_gate("b", "pool");
    let permit = gate.clone().acquire_owned().await.unwrap();
    let queued = tokio::time::timeout(
        Duration::from_millis(50),
        f.create(create_req("pvc-1", GIB)),
    )
    .await;
    assert!(queued.is_err(), "a create ran past a full pool gate");
    // A different pool is not held up by this one.
    let mut other = create_req("pvc-2", GIB);
    other.parameters.insert("prefix".into(), "other".into());
    f.create(other).await.unwrap();
    drop(permit);
    f.create(create_req("pvc-1", GIB)).await.unwrap();
}

#[tokio::test]
async fn delete_trashes_and_is_idempotent() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"));
    let trashed = fs.children("/.trash");
    assert_eq!(trashed.len(), 1, "{trashed:?}");
    assert!(trashed[0].starts_with("/.trash/pvc-1-"), "{trashed:?}");
    // The quota was released before the rename and travelled with it.
    assert_eq!(fs.quota_get(&trashed[0]).await.unwrap().max_bytes, Some(0));
    // The record travels too: a purge worker can tell whose it was.
    assert_eq!(record(&fs, &trashed[0]).await[X_PV], "pvc-1");

    // Already gone: OK, and nothing more is trashed.
    f.delete(&v.volume_id).await.unwrap();
    assert_eq!(fs.children("/.trash").len(), 1);
    // The name is free again.
    let again = f.create(create_req("pvc-1", GIB)).await.unwrap();
    assert_eq!(again.volume_id, v.volume_id);
}

/// The 37-K2a review's stuck-PV case: once the volume is in the trash, a
/// retried `DeleteVolume` must answer `OK` without asking `quota.set` at
/// all — whatever error kind the engine would give it (the fake fails it
/// `Failed`, which `set_quota` would retry to exhaustion and `ABORTED`).
#[tokio::test]
async fn a_retried_delete_does_not_depend_on_quota_set() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    f.delete(&v.volume_id).await.unwrap();
    let calls = fs.quota_set_calls();
    fs.fail_next_quota_sets(1000);
    f.delete(&v.volume_id).await.unwrap();
    assert_eq!(
        fs.quota_set_calls(),
        calls,
        "no quota.set for a gone volume"
    );
    // And with the engine's own answer for a missing subtree (`NotFound`,
    // what `quota.set{subtree}` returns since K2; `crates/cli/tests/serve.rs`
    // checks the real daemon): still OK.
    fs.fail_next_quota_sets(0);
    assert_eq!(
        fs.quota_set(SubtreeQuotaParams {
            subtree: "/volumes/pvc-1".into(),
            max_bytes: Some(0),
        })
        .await
        .unwrap_err()
        .kind,
        ErrorKind::NotFound
    );
    f.delete(&v.volume_id).await.unwrap();
}

/// Must-fix 1 of the 37-K2b review: no RPC on an existing volume walks it
/// (the real engine's subtree usage is O(entries)). Create, expand,
/// validate and delete — first and retried — ask for no usage at all.
#[tokio::test]
async fn no_rpc_walks_a_capped_volume() {
    let f = fixture();
    let v = f.create(create_req("pvc-big", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.set_used_bytes("/volumes/pvc-big", GIB as u64 / 2);
    assert_eq!(f.expand(&v.volume_id, 2 * GIB).await.unwrap(), 2 * GIB);
    f.service
        .validate_volume_capabilities(Request::new(ValidateVolumeCapabilitiesRequest {
            volume_id: v.volume_id.clone(),
            volume_capabilities: create_req("x", GIB).volume_capabilities,
            ..Default::default()
        }))
        .await
        .unwrap();
    f.delete(&v.volume_id).await.unwrap();
    f.delete(&v.volume_id).await.unwrap();
    assert_eq!(fs.usage_walks(), 0, "a walk on the RPC path");
}

/// A directory with no volume record is adopted only while it is empty
/// (an attempt that died between `mkdir` and its first xattr); one holding
/// data is somebody else's.
#[tokio::test]
async fn a_recordless_directory_is_adopted_only_when_empty() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    for name in ["pvc-empty", "pvc-full"] {
        fs.browse_mkdir(MkdirParams {
            path: format!("/volumes/{name}"),
            mode: None,
            parents: false,
        })
        .await
        .unwrap();
    }
    fs.set_used_bytes("/volumes/pvc-full", 4096);
    let adopted = f.create(create_req("pvc-empty", GIB)).await.unwrap();
    assert_eq!(record(&fs, "/volumes/pvc-empty").await[X_PV], "pvc-empty");
    assert_eq!(adopted.capacity_bytes, GIB);
    let e = f.create(create_req("pvc-full", GIB)).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::AlreadyExists, "{e:?}");
    assert!(record(&fs, "/volumes/pvc-full").await.is_empty());
}

#[tokio::test]
async fn delete_edge_cases() {
    let f = fixture();
    assert_eq!(
        f.delete("").await.unwrap_err().code(),
        GrpcCode::InvalidArgument
    );
    // Never ours / unknown pool: nothing to delete.
    f.delete("reallyfakevolumeid").await.unwrap();
    f.delete("v1/pool/0/no-such-fs/volumes/pvc-1")
        .await
        .unwrap();
    // A static handle is never deleted by this driver.
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    assert_eq!(
        f.delete(&format!("{uuid}/volumes/pvc-1"))
            .await
            .unwrap_err()
            .code(),
        GrpcCode::FailedPrecondition
    );
    assert!(f.fs_of(&v.volume_id).await.exists("/volumes/pvc-1"));
    // Transient quota failures are retried on delete too.
    let fs = f.fs_of(&v.volume_id).await;
    fs.fail_next_quota_sets(2);
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"));
}

#[tokio::test]
async fn expand_grows_only() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    assert_eq!(f.expand(&v.volume_id, 2 * GIB).await.unwrap(), 2 * GIB);
    assert_eq!(
        fs.quota_get("/volumes/pvc-1").await.unwrap().max_bytes,
        Some(2 * GIB as u64)
    );
    assert_eq!(
        record(&fs, "/volumes/pvc-1").await[X_CAPACITY],
        (2 * GIB).to_string()
    );
    // At or below the current size: already satisfied, no shrink.
    assert_eq!(f.expand(&v.volume_id, GIB).await.unwrap(), 2 * GIB);
    // So a repeated create now sees the grown size.
    assert_eq!(
        f.create(create_req("pvc-1", 2 * GIB))
            .await
            .unwrap()
            .capacity_bytes,
        2 * GIB
    );
}

#[tokio::test]
async fn expand_edge_cases() {
    let f = fixture();
    let mut free = create_req("pvc-free", 0);
    free.capacity_range = None;
    let v = f.create(free).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.set_used_bytes("/volumes/pvc-free", 100);
    // Capping an uncapped volume below what it holds.
    let e = f.expand(&v.volume_id, 50).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::OutOfRange);
    assert_eq!(f.expand(&v.volume_id, 200).await.unwrap(), 200);

    let missing = "v1/pool/0/no-such-fs/volumes/pvc-x";
    assert_eq!(
        f.expand(missing, GIB).await.unwrap_err().code(),
        GrpcCode::NotFound
    );
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let gone = format!("v1/pool/0/{uuid}/volumes/pvc-gone");
    assert_eq!(
        f.expand(&gone, GIB).await.unwrap_err().code(),
        GrpcCode::NotFound
    );
    assert_eq!(
        f.expand("garbage", GIB).await.unwrap_err().code(),
        GrpcCode::NotFound
    );
    let e = f
        .service
        .controller_expand_volume(Request::new(ControllerExpandVolumeRequest {
            volume_id: v.volume_id.clone(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(e.code(), GrpcCode::InvalidArgument);
}

#[tokio::test]
async fn validate_volume_capabilities() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let validate = |volume_id: String, caps: Vec<VolumeCapability>| {
        f.service
            .validate_volume_capabilities(Request::new(ValidateVolumeCapabilitiesRequest {
                volume_id,
                volume_capabilities: caps,
                ..Default::default()
            }))
    };
    let all_modes = [
        Mode::SingleNodeWriter,
        Mode::SingleNodeReaderOnly,
        Mode::MultiNodeReaderOnly,
        Mode::MultiNodeSingleWriter,
        Mode::MultiNodeMultiWriter,
        Mode::SingleNodeSingleWriter,
        Mode::SingleNodeMultiWriter,
    ];
    let ok = validate(
        v.volume_id.clone(),
        all_modes.into_iter().map(mount_cap).collect(),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(ok.confirmed.is_some(), "{}", ok.message);

    let block = VolumeCapability {
        access_type: Some(AccessType::Block(BlockVolume {})),
        access_mode: Some(AccessMode {
            mode: Mode::SingleNodeWriter as i32,
        }),
    };
    let refused = validate(v.volume_id.clone(), vec![block])
        .await
        .unwrap()
        .into_inner();
    assert!(refused.confirmed.is_none());
    assert!(refused.message.contains("block"), "{}", refused.message);

    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    for missing in [
        format!("v1/pool/0/{uuid}/volumes/pvc-nope"),
        "v1/pool/0/no-such-fs/volumes/pvc-1".to_string(),
        "garbage".to_string(),
    ] {
        let e = validate(missing.clone(), vec![mount_cap(Mode::SingleNodeWriter)])
            .await
            .unwrap_err();
        assert_eq!(e.code(), GrpcCode::NotFound, "{missing}");
    }
    let e = validate(v.volume_id.clone(), vec![]).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::InvalidArgument);
}

#[tokio::test]
async fn advertises_exactly_what_works() {
    let f = fixture();
    let caps = f
        .service
        .controller_get_capabilities(Request::new(ControllerGetCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner()
        .capabilities;
    let types: Vec<RpcType> = caps
        .iter()
        .map(|c| match &c.r#type {
            Some(CapabilityType::Rpc(rpc)) => RpcType::try_from(rpc.r#type).unwrap(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        types,
        [
            RpcType::CreateDeleteVolume,
            RpcType::ExpandVolume,
            RpcType::CreateDeleteSnapshot,
            RpcType::ListSnapshots,
            RpcType::CloneVolume,
            RpcType::GetSnapshot,
            RpcType::SingleNodeMultiWriter
        ]
    );
}

#[test]
fn control_errors_map_to_grpc_codes() {
    for (kind, code) in [
        (ErrorKind::NotFound, GrpcCode::NotFound),
        (ErrorKind::Denied, GrpcCode::PermissionDenied),
        (ErrorKind::Invalid, GrpcCode::InvalidArgument),
        (ErrorKind::Unsupported, GrpcCode::Unimplemented),
        (ErrorKind::Failed, GrpcCode::Internal),
        (ErrorKind::Cancelled, GrpcCode::Cancelled),
        (ErrorKind::Unavailable, GrpcCode::Unavailable),
        (ErrorKind::Conflict, GrpcCode::FailedPrecondition),
        (ErrorKind::Timeout, GrpcCode::DeadlineExceeded),
    ] {
        assert_eq!(status("op", ControlError::new(kind, "x")).code(), code);
    }
    let full = ControlError::from(Code::NoSpace);
    assert_eq!(status("op", full).code(), GrpcCode::ResourceExhausted);
    // quota.set: transient after retries is retryable for the sidecar.
    assert_eq!(
        quota_status("/v", ControlError::failed("journal not shipped: no lease")).code(),
        GrpcCode::Aborted
    );
    assert_eq!(
        quota_status("/v", ControlError::invalid("x")).code(),
        GrpcCode::InvalidArgument
    );
}

#[test]
fn config_reads_its_env_knobs() {
    // One test touches these variables, so no cross-test races.
    std::env::set_var("CONSTELLATION_CSI_POOL_CREATE_CONCURRENCY", "2");
    std::env::set_var("CONSTELLATION_CSI_QUOTA_SET_ATTEMPTS", "3");
    let config = ControllerConfig::from_env().unwrap();
    assert_eq!(config.pool_create_concurrency, 2);
    assert_eq!(config.quota_retry.attempts, 3);
    std::env::set_var("CONSTELLATION_CSI_QUOTA_SET_ATTEMPTS", "0");
    assert!(ControllerConfig::from_env().is_err());
    std::env::remove_var("CONSTELLATION_CSI_POOL_CREATE_CONCURRENCY");
    std::env::remove_var("CONSTELLATION_CSI_QUOTA_SET_ATTEMPTS");
    assert_eq!(
        ControllerConfig::from_env().unwrap(),
        ControllerConfig::default()
    );
}

// ---- K4: snapshots, clones, and deletes that start nothing --------------

impl Fixture {
    async fn snapshot(&self, name: &str, source: &str) -> Result<Snapshot, Status> {
        self.service
            .create_snapshot(Request::new(CreateSnapshotRequest {
                name: name.to_string(),
                source_volume_id: source.to_string(),
                ..Default::default()
            }))
            .await
            .map(|r| r.into_inner().snapshot.unwrap())
    }

    async fn delete_snapshot(&self, id: &str) -> Result<(), Status> {
        self.service
            .delete_snapshot(Request::new(DeleteSnapshotRequest {
                snapshot_id: id.to_string(),
                secrets: HashMap::new(),
            }))
            .await
            .map(drop)
    }

    async fn list_snapshots(
        &self,
        req: ListSnapshotsRequest,
    ) -> Result<(Vec<String>, String), Status> {
        let r = self
            .service
            .list_snapshots(Request::new(req))
            .await?
            .into_inner();
        Ok((
            r.entries
                .into_iter()
                .map(|e| e.snapshot.unwrap().snapshot_id)
                .collect(),
            r.next_token,
        ))
    }
}

fn from_snapshot(name: &str, required: i64, snapshot_id: &str) -> CreateVolumeRequest {
    let mut r = create_req(name, required);
    r.volume_content_source = Some(VolumeContentSource {
        r#type: Some(volume_content_source::Type::Snapshot(
            volume_content_source::SnapshotSource {
                snapshot_id: snapshot_id.to_string(),
            },
        )),
    });
    r
}

fn from_volume(name: &str, required: i64, volume_id: &str) -> CreateVolumeRequest {
    let mut r = create_req(name, required);
    r.volume_content_source = Some(VolumeContentSource {
        r#type: Some(volume_content_source::Type::Volume(
            volume_content_source::VolumeSource {
                volume_id: volume_id.to_string(),
            },
        )),
    });
    r
}

#[tokio::test]
async fn a_snapshot_is_a_held_engine_snapshot_named_by_its_id() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.set_used_bytes("/volumes/pvc-src", 12345);
    let s = f.snapshot("snapshot-1", &v.volume_id).await.unwrap();
    assert_eq!(s.snapshot_id, format!("{}@snapshot-1", v.volume_id));
    assert_eq!(s.source_volume_id, v.volume_id);
    assert_eq!(s.size_bytes, 12345, "REFER");
    assert!(s.ready_to_use);
    assert!(s.creation_time.unwrap().seconds > 0);
    let rows = fs.snapshots();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (rows[0].path.as_str(), rows[0].name.as_str()),
        ("/volumes/pvc-src", "snapshot-1")
    );
    assert!(rows[0].held);
    assert_eq!(
        rows[0].held_by.as_deref(),
        Some(format!("csi:{}", s.snapshot_id).as_str())
    );

    // Idempotent: the same name and source is the same snapshot.
    let again = f.snapshot("snapshot-1", &v.volume_id).await.unwrap();
    assert_eq!(again, s);
    assert_eq!(fs.snapshots().len(), 1);
    // The same name of another volume is ALREADY_EXISTS.
    let other = f.create(create_req("pvc-other", GIB)).await.unwrap();
    let e = f
        .snapshot("snapshot-1", &other.volume_id)
        .await
        .unwrap_err();
    assert_eq!(e.code(), GrpcCode::AlreadyExists, "{e}");
    // Bad requests.
    for (name, source, code) in [
        ("", v.volume_id.as_str(), GrpcCode::InvalidArgument),
        ("s", "", GrpcCode::InvalidArgument),
        ("a@b", v.volume_id.as_str(), GrpcCode::InvalidArgument),
        ("a%b", v.volume_id.as_str(), GrpcCode::InvalidArgument),
        ("s", "not-a-volume-id", GrpcCode::NotFound),
    ] {
        let e = f.snapshot(name, source).await.unwrap_err();
        assert_eq!(e.code(), code, "{name:?} of {source:?}: {e}");
    }
    // A volume that is gone.
    f.delete(&other.volume_id).await.unwrap();
    let e = f
        .snapshot("snapshot-2", &other.volume_id)
        .await
        .unwrap_err();
    assert_eq!(e.code(), GrpcCode::NotFound, "{e}");
}

#[tokio::test]
async fn delete_snapshot_releases_its_hold_and_leaves_a_human_hold_alone() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    let a = f.snapshot("snap-a", &v.volume_id).await.unwrap();
    let b = f.snapshot("snap-b", &v.volume_id).await.unwrap();
    f.delete_snapshot(&a.snapshot_id).await.unwrap();
    assert_eq!(fs.snapshots().len(), 1);
    // Idempotent, and ids this driver never minted are OK.
    f.delete_snapshot(&a.snapshot_id).await.unwrap();
    f.delete_snapshot("reallyfakesnapshotid").await.unwrap();
    assert_eq!(
        f.delete_snapshot("").await.unwrap_err().code(),
        GrpcCode::InvalidArgument
    );
    // A human took the hold over: the CSI object goes, the snapshot stays.
    fs.snapshot_hold(SnapshotHoldParams {
        id: "/volumes/pvc-src@snap-b".into(),
        held: true,
        by: Some("user:alice".into()),
        force: true,
    })
    .await
    .unwrap();
    f.delete_snapshot(&b.snapshot_id).await.unwrap();
    let rows = fs.snapshots();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].held_by.as_deref(), Some("user:alice"));
    // Deleting the source volume leaves its snapshots usable.
    let c = f.snapshot("snap-c", &v.volume_id).await.unwrap();
    f.delete(&v.volume_id).await.unwrap();
    let restored = f
        .create(from_snapshot("pvc-restored", GIB, &c.snapshot_id))
        .await
        .unwrap();
    assert!(fs.exists(&VolumeId::parse(&restored.volume_id).unwrap().subtree()));
    f.delete_snapshot(&c.snapshot_id).await.unwrap();
    assert_eq!(fs.snapshots().len(), 1, "only the human's is left");
}

#[tokio::test]
async fn list_snapshots_filters_and_pages() {
    let f = fixture();
    let mut ids = Vec::new();
    let mut volumes = Vec::new();
    for i in 0..5 {
        let v = f
            .create(create_req(&format!("pvc-{i}"), GIB))
            .await
            .unwrap();
        ids.push(
            f.snapshot(&format!("snap-{i}"), &v.volume_id)
                .await
                .unwrap()
                .snapshot_id,
        );
        volumes.push(v.volume_id);
    }
    ids.sort();
    // A snapshot this driver does not hold is not listed.
    let fs = f.fs_of(&volumes[0]).await;
    fs.snapshot_create(SnapshotCreateParams {
        selector: "/volumes/pvc-0@human".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let all = |r| f.list_snapshots(r);
    let (got, token) = all(ListSnapshotsRequest::default()).await.unwrap();
    assert_eq!((got.clone(), token.as_str()), (ids.clone(), ""));

    // Pages of two, in id order, ending without a token.
    let mut paged = Vec::new();
    let mut token = String::new();
    loop {
        let (page, next) = all(ListSnapshotsRequest {
            max_entries: 2,
            starting_token: token.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(page.len() <= 2);
        paged.extend(page);
        if next.is_empty() {
            break;
        }
        token = next;
    }
    assert_eq!(paged, ids);
    // A token, then everything after it (csi-sanity's shape); a snapshot
    // deleted meanwhile shifts nothing.
    let (first, token) = all(ListSnapshotsRequest {
        max_entries: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(first, ids[..2]);
    f.delete_snapshot(&ids[0]).await.unwrap();
    let (rest, next) = all(ListSnapshotsRequest {
        starting_token: token,
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!((rest, next.as_str()), (ids[2..].to_vec(), ""));
    let e = all(ListSnapshotsRequest {
        starting_token: "invalid-token".into(),
        ..Default::default()
    })
    .await
    .unwrap_err();
    assert_eq!(e.code(), GrpcCode::Aborted);

    // By id, and by source volume.
    let (one, _) = all(ListSnapshotsRequest {
        snapshot_id: ids[3].clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(one, [ids[3].clone()]);
    let (by_source, _) = all(ListSnapshotsRequest {
        source_volume_id: volumes[2].clone(),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(by_source, [format!("{}@snap-2", volumes[2])]);
    // Unknown ids and sources list nothing (no error).
    for req in [
        ListSnapshotsRequest {
            snapshot_id: "none-exist-id".into(),
            ..Default::default()
        },
        ListSnapshotsRequest {
            snapshot_id: format!("{}@nope", volumes[1]),
            ..Default::default()
        },
        ListSnapshotsRequest {
            source_volume_id: "v1/pool/0/fake-fs-ffff/volumes/x".into(),
            ..Default::default()
        },
    ] {
        assert_eq!(all(req).await.unwrap().0, Vec::<String>::new());
    }
    // A pre-provisioned content may name any snapshot by id, held or not.
    let (human, _) = all(ListSnapshotsRequest {
        snapshot_id: format!("{}@human", volumes[0]),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(human, [format!("{}@human", volumes[0])]);
}

#[tokio::test]
async fn restore_is_a_metadata_clone_inside_the_pool() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.plant_dir("/volumes/pvc-src/data/sub");
    fs.set_used_bytes("/volumes/pvc-src/data/sub", 1000);
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    // Changed after the snapshot: the restore does not see it.
    fs.plant_dir("/volumes/pvc-src/later");

    let walks = fs.usage_walks();
    let r = f
        .create(from_snapshot("pvc-restored", 2 * GIB, &s.snapshot_id))
        .await
        .unwrap();
    assert_eq!(fs.clones(), 1, "one clone.create: metadata only");
    assert_eq!(
        fs.usage_walks(),
        walks,
        "no walk of the source or the clone"
    );
    let id = VolumeId::parse(&r.volume_id).unwrap();
    assert_eq!(
        id.fs_uuid(),
        VolumeId::parse(&v.volume_id).unwrap().fs_uuid()
    );
    assert!(r.content_source.is_some());
    assert_eq!(r.capacity_bytes, 2 * GIB);
    assert!(fs.exists("/volumes/pvc-restored/data/sub"));
    assert!(!fs.exists("/volumes/pvc-restored/later"));
    // The record is the new volume's, not the source's copy.
    let rec = record(&fs, "/volumes/pvc-restored").await;
    assert_eq!(rec[X_PV], "pvc-restored");
    assert_eq!(rec[X_CAPACITY], (2 * GIB).to_string());
    assert_eq!(rec[X_SOURCE], format!("snapshot:{}", s.snapshot_id));
    assert!(rec.contains_key(X_CREATED));
    assert_eq!(
        fs.quota_get("/volumes/pvc-restored")
            .await
            .unwrap()
            .max_bytes,
        Some(2 * GIB as u64)
    );
    // Idempotent; the same name from another source is ALREADY_EXISTS.
    let again = f
        .create(from_snapshot("pvc-restored", 2 * GIB, &s.snapshot_id))
        .await
        .unwrap();
    assert_eq!(again.volume_id, r.volume_id);
    assert_eq!(fs.clones(), 1);
    let e = f
        .create(from_volume("pvc-restored", 2 * GIB, &v.volume_id))
        .await
        .unwrap_err();
    assert_eq!(e.code(), GrpcCode::AlreadyExists, "{e}");
    // Missing sources.
    for req in [
        from_snapshot("pvc-x", GIB, "non-existing-snapshot-id"),
        from_snapshot("pvc-x", GIB, &format!("{}@nope", v.volume_id)),
        from_volume("pvc-x", GIB, "fake-vol-id"),
        from_volume("pvc-x", GIB, &format!("{}-gone", v.volume_id)),
    ] {
        let e = f.create(req).await.unwrap_err();
        assert_eq!(e.code(), GrpcCode::NotFound, "{e}");
    }
    assert!(!fs.exists("/volumes/pvc-x"));
}

#[tokio::test]
async fn a_volume_clone_goes_through_a_transient_snapshot_in_the_sources_shard() {
    let f = fixture();
    let sharded = |mut r: CreateVolumeRequest| {
        r.parameters.insert("shards".into(), "4".into());
        r
    };
    let v = f.create(sharded(create_req("pvc-src", GIB))).await.unwrap();
    let VolumeId::Pool { shard, .. } = VolumeId::parse(&v.volume_id).unwrap() else {
        unreachable!()
    };
    let fs = f.fs_of(&v.volume_id).await;
    fs.plant_dir("/volumes/pvc-src/tree");
    // Names that hash to every shard: each clone still lands in the
    // source's.
    for i in 0..8 {
        let name = format!("pvc-clone-{i}");
        let c = f
            .create(sharded(from_volume(&name, GIB, &v.volume_id)))
            .await
            .unwrap();
        let VolumeId::Pool {
            shard: s, fs_uuid, ..
        } = VolumeId::parse(&c.volume_id).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(s, shard);
        assert_eq!(fs_uuid, VolumeId::parse(&v.volume_id).unwrap().fs_uuid());
        assert!(fs.exists(&format!("/volumes/{name}/tree")));
        let rec = record(&fs, &format!("/volumes/{name}")).await;
        assert_eq!(rec[X_PV], name);
        assert_eq!(rec[X_SOURCE], format!("volume:{}", v.volume_id));
    }
    // No transient snapshot is left behind.
    assert!(fs.snapshots().is_empty());
}

/// A clone whose `clone.create` fails after its transient snapshot was made
/// (and is then abandoned with its PVC) leaves no snapshot behind. The
/// post-clone failure paths share the same cleanup (one `drop_transient`).
#[tokio::test]
async fn a_failed_volume_clone_leaves_no_transient_snapshot() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    fs.plant_dir("/volumes/pvc-src/tree");
    fs.fail_next_clones(1);
    let e = f
        .create(from_volume("pvc-clone", GIB, &v.volume_id))
        .await
        .unwrap_err();
    assert!(e.message().contains("clone.create"), "{e}");
    assert!(fs.snapshots().is_empty(), "transient snapshot removed");
    assert!(!fs.exists("/volumes/pvc-clone"));
    // The retry works and leaves none either.
    f.create(from_volume("pvc-clone", GIB, &v.volume_id))
        .await
        .unwrap();
    assert!(fs.snapshots().is_empty());
}

/// A delete of a volume this process created starts the engine even when
/// the CO names no PV (the provisioner cleaning up a PV it failed to save).
#[tokio::test]
async fn a_delete_of_a_volume_this_process_created_trashes_it_without_a_pv() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    f.engines.set_named(Some(Default::default()));
    f.engines.stop_all();
    let starts = f.engines.starts();
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"), "the volume is trashed");
    assert_eq!(f.engines.starts(), starts + 1);
    // Repeated: nothing more is started.
    f.delete(&v.volume_id).await.unwrap();
    assert_eq!(f.engines.starts(), starts + 1);
}

#[tokio::test]
async fn an_interrupted_clone_is_completed_in_place() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let fs = f.fs_of(&v.volume_id).await;
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    // The raw copy a crash right after clone.create leaves: the source's
    // whole record, its commit mark included.
    fs.clone_create(CloneParams {
        selector: "/volumes/pvc-src@snap".into(),
        destination: "/volumes/pvc-new".into(),
    })
    .await
    .unwrap();
    assert_eq!(record(&fs, "/volumes/pvc-new").await[X_PV], "pvc-src");
    let r = f
        .create(from_snapshot("pvc-new", 3 * GIB, &s.snapshot_id))
        .await
        .unwrap();
    assert_eq!(r.capacity_bytes, 3 * GIB);
    let rec = record(&fs, "/volumes/pvc-new").await;
    assert_eq!(rec[X_PV], "pvc-new");
    assert_eq!(rec[X_CAPACITY], (3 * GIB).to_string());
    assert_eq!(fs.clones(), 1, "the copy was adopted, not redone");
}

#[tokio::test]
async fn cross_pool_and_cross_shard_sources_are_refused() {
    let f = fixture();
    let v = f.create(create_req("pvc-src", GIB)).await.unwrap();
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    let src_uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let other_pool = |mut r: CreateVolumeRequest| {
        r.parameters.insert("prefix".into(), "other-pool".into());
        r
    };
    for req in [
        other_pool(from_snapshot("pvc-a", GIB, &s.snapshot_id)),
        other_pool(from_volume("pvc-b", GIB, &v.volume_id)),
    ] {
        let e = f.create(req).await.unwrap_err();
        assert_eq!(e.code(), GrpcCode::InvalidArgument, "{e}");
        assert!(e.message().contains(&src_uuid), "names the source: {e}");
        assert!(
            e.message().contains("other-pool"),
            "names the destination: {e}"
        );
    }
    // A dedicated destination is a filesystem of its own.
    let mut dedicated = from_snapshot("pvc-c", GIB, &s.snapshot_id);
    dedicated
        .parameters
        .insert("layout".into(), "dedicated".into());
    assert_eq!(
        f.create(dedicated).await.unwrap_err().code(),
        GrpcCode::InvalidArgument
    );
    // Shard 3 of a 4-shard pool has no counterpart in a 2-shard class
    // naming the same prefix.
    let mut src4 = create_req("pvc-s4", GIB);
    src4.parameters.insert("shards".into(), "4".into());
    let name = (0..)
        .map(|i| format!("pvc-s4-{i}"))
        .find(|n| {
            crate::params::ClassParams::parse(&src4.parameters)
                .unwrap()
                .shard_for(n)
                == 3
        })
        .unwrap();
    src4.name = name;
    let v4 = f.create(src4).await.unwrap();
    let mut two = from_volume("pvc-d", GIB, &v4.volume_id);
    two.parameters.insert("shards".into(), "2".into());
    let e = f.create(two).await.unwrap_err();
    assert_eq!(e.code(), GrpcCode::InvalidArgument, "{e}");
    assert!(e.message().contains("shard 3"), "{e}");
    // Nothing was made in the source's pool for any of them.
    let fs = f.fs_of(&v.volume_id).await;
    for n in ["pvc-a", "pvc-b", "pvc-c"] {
        assert!(!fs.exists(&format!("/volumes/{n}")));
    }
}

/// The 37-k3b review's product bug: external-provisioner repeated
/// `DeleteVolume` after the PV was gone and the controller recreated the
/// pool's engine pod to answer it.
#[tokio::test]
async fn a_repeated_delete_after_the_pv_is_gone_starts_no_engine() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    f.engines.set_named(Some(Default::default()));
    f.delete_snapshot(&s.snapshot_id).await.unwrap();
    f.delete(&v.volume_id).await.unwrap();
    // A teardown deletes the pool's engine pods; the sidecars repeat their
    // deletes.
    f.engines.stop_all();
    let starts = f.engines.starts();
    for _ in 0..3 {
        f.delete(&v.volume_id).await.unwrap();
        f.delete_snapshot(&s.snapshot_id).await.unwrap();
    }
    assert_eq!(f.engines.starts(), starts, "no engine pod was started");
    assert!(!f.engines.is_running(&uuid));
}

/// A delete that does need the engine (the PV still exists, no pod is up)
/// starts it, and stops it again afterwards — unless it moved a volume
/// into the trash and the purge worker runs: that pod stays for the worker
/// (37-k6b), which reaps it once the pool is empty.
#[tokio::test]
async fn a_delete_that_needs_the_engine_starts_it_and_stops_it() {
    let f = fixture_with(ControllerConfig {
        purge_worker: true,
        ..ControllerConfig::default()
    });
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let fs = f.fs_of(&v.volume_id).await;
    f.engines.set_named(Some(
        [v.volume_id.clone(), s.snapshot_id.clone()]
            .into_iter()
            .collect(),
    ));
    f.engines.stop_all();
    let starts = f.engines.starts();
    f.delete_snapshot(&s.snapshot_id).await.unwrap();
    assert!(fs.snapshots().is_empty(), "the snapshot is deleted");
    assert_eq!(
        f.engines.retires(),
        1,
        "the snapshot's delete stopped its pod"
    );
    assert!(!f.engines.is_running(&uuid));
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"), "the volume is trashed");
    assert_eq!(f.engines.starts(), starts + 2);
    assert_eq!(f.engines.retires(), 1, "a pod with trash to purge stays");
    assert!(f.engines.is_running(&uuid));
    // A delete that finds the volume gone already stops what it started.
    f.engines.stop_all();
    f.delete(&v.volume_id).await.unwrap();
    assert_eq!(f.engines.starts(), starts + 3);
    assert_eq!(f.engines.retires(), 2);
    assert!(!f.engines.is_running(&uuid));
    // With a pod up, a delete uses it and leaves it up.
    let w = f.create(create_req("pvc-2", GIB)).await.unwrap();
    f.engines.set_named(None);
    let retires = f.engines.retires();
    f.delete(&w.volume_id).await.unwrap();
    assert_eq!(f.engines.retires(), retires);
    assert!(f.engines.is_running(&uuid));
}

#[tokio::test]
async fn get_snapshot_is_list_by_id_with_not_found() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let s = f.snapshot("snap", &v.volume_id).await.unwrap();
    let get = |id: String| {
        f.service.get_snapshot(Request::new(GetSnapshotRequest {
            snapshot_id: id,
            secrets: HashMap::new(),
        }))
    };
    let got = get(s.snapshot_id.clone()).await.unwrap().into_inner();
    assert_eq!(got.snapshot.unwrap(), s);
    for (id, code) in [
        (String::new(), GrpcCode::InvalidArgument),
        ("none-exist-id".to_string(), GrpcCode::NotFound),
        (format!("{}@other", v.volume_id), GrpcCode::NotFound),
    ] {
        assert_eq!(get(id).await.unwrap_err().code(), code);
    }
}

/// With the purge worker off, nothing would ever purge through or reap a
/// pod a delete started: it is stopped even when the volume went into the
/// trash (the 37-k6b review).
#[tokio::test]
async fn without_the_purge_worker_a_trashing_delete_stops_its_pod() {
    let f = fixture();
    assert!(!ControllerConfig::default().purge_worker);
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let fs = f.fs_of(&v.volume_id).await;
    f.engines
        .set_named(Some([v.volume_id.clone()].into_iter().collect()));
    f.engines.stop_all();
    let starts = f.engines.starts();
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"), "the volume is trashed");
    assert_eq!(f.engines.starts(), starts + 1);
    assert_eq!(f.engines.retires(), 1, "stopped: no worker would reap it");
    assert!(!f.engines.is_running(&uuid));
}

/// A pool goes on record before a delete trashes into it (the purge
/// worker's way back to a pool whose pod and PVs are gone); a delete that
/// cannot record it trashes nothing and fails retryably.
#[tokio::test]
async fn a_delete_records_the_pool_before_it_trashes() {
    let f = fixture();
    let v = f.create(create_req("pvc-1", GIB)).await.unwrap();
    let uuid = VolumeId::parse(&v.volume_id).unwrap().fs_uuid().to_string();
    let fs = f.fs_of(&v.volume_id).await;
    f.engines.fail_next_records(1);
    let err = f.delete(&v.volume_id).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert!(fs.exists("/volumes/pvc-1"), "nothing trashed unrecorded");
    assert!(fs.children("/.trash").is_empty());
    f.delete(&v.volume_id).await.unwrap();
    assert!(!fs.exists("/volumes/pvc-1"));
    assert_eq!(fs.children("/.trash").len(), 1);
    assert_eq!(f.engines.recorded(), vec![uuid]);
}
