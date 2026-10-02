use super::*;
use crate::control_client::{InMemoryControl, InMemoryEngines};
use crate::proto::csi::v1::volume_capability::{AccessMode, BlockVolume, MountVolume};
use constellation_control::proto::types::FsCreateParams;
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
        self.engines.filesystem(id.fs_uuid()).await.unwrap();
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

#[tokio::test]
async fn deferred_shapes_are_unimplemented() {
    let f = fixture();
    let mut r = create_req("pvc", GIB);
    r.parameters.insert("layout".into(), "dedicated".into());
    assert_eq!(
        f.create(r).await.unwrap_err().code(),
        GrpcCode::Unimplemented
    );
    let mut r = create_req("pvc", GIB);
    r.volume_content_source = Some(VolumeContentSource { r#type: None });
    assert_eq!(
        f.create(r).await.unwrap_err().code(),
        GrpcCode::Unimplemented
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
