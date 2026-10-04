//! The Node service's state machine against [`FakeMounter`] and
//! [`InMemoryNodeEngines`]: what it mounts, what it sends the engine, what
//! it records — and what a crashed engine pod, a restarted plugin and a
//! volume edited behind the driver's back do to it.

use super::*;
use crate::control_client::{ControlClient, InMemoryControl, SubtreeQuotaParams};
use crate::proto::csi::v1::volume_capability::{AccessMode, MountVolume};
use constellation_control::proto::types::{XattrOp, XattrParams};

const UUID: &str = "4f9c1e2a-0b1c-4d5e-8f90-1a2b3c4d5e6f";

struct Rig {
    dir: tempfile::TempDir,
    mounter: Arc<FakeMounter>,
    engines: Arc<InMemoryNodeEngines>,
    node: NodeService,
}

impl Rig {
    fn new() -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let mounter = Arc::new(FakeMounter::default());
        let engines = Arc::new(InMemoryNodeEngines::new("node-a"));
        let node = Rig::service(&dir, &mounter, &engines);
        Rig {
            dir,
            mounter,
            engines,
            node,
        }
    }

    /// A node plugin process over this rig's host: its records, mounts and
    /// engine pods (a restart is a second one).
    fn service(
        dir: &tempfile::TempDir,
        mounter: &Arc<FakeMounter>,
        engines: &Arc<InMemoryNodeEngines>,
    ) -> NodeService {
        NodeService::new(
            "node-a".into(),
            Some(engines.clone()),
            mounter.clone(),
            StateStore::open(&dir.path().join("host/volumes")).unwrap(),
        )
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.dir.path().join(rel)
    }

    fn staging(&self) -> PathBuf {
        self.path("kubelet/staging/pvc-a/globalmount")
    }

    fn target(&self, n: u32) -> PathBuf {
        self.path(&format!("kubelet/pods/p{n}/volumes/pvc-a/mount"))
    }

    /// The pool's engine on this node, with volume `pvc-a` planted in it
    /// carrying a controller-shaped record.
    async fn plant(&self) -> Arc<InMemoryControl> {
        self.plant_in(&pool(1)).await
    }

    /// [`Self::plant`] for a pool of another class (the same location).
    async fn plant_in(&self, pool: &PoolRef) -> Arc<InMemoryControl> {
        let control = self.engines.plant(pool, UUID);
        // The engine waits for its credentials (a static-ephemeral class),
        // so the volume is planted behind its back.
        control.plant_dir("/volumes/pvc-a");
        for (k, v) in [
            (X_PV, "pvc-a"),
            (X_PVC, "data"),
            (X_NAMESPACE, "tenant"),
            ("user.constellation.csi.created", "1"),
        ] {
            control.plant_xattr("/volumes/pvc-a", k, v.as_bytes());
        }
        control
    }

    fn unit(&self) -> String {
        self.engines.names(&pool(1)).0
    }

    async fn stage(&self, staging: &Path) -> Result<(), Status> {
        self.node
            .node_stage_volume(Request::new(stage_req(staging)))
            .await
            .map(drop)
    }

    async fn publish(&self, target: &Path, mode: Mode) -> Result<(), Status> {
        self.node
            .node_publish_volume(Request::new(NodePublishVolumeRequest {
                volume_id: volume_id(),
                staging_target_path: self.staging().display().to_string(),
                target_path: target.display().to_string(),
                volume_capability: Some(capability(mode)),
                volume_context: context(),
                ..Default::default()
            }))
            .await
            .map(drop)
    }

    async fn unpublish(&self, target: &Path) -> Result<(), Status> {
        self.node
            .node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
                volume_id: volume_id(),
                target_path: target.display().to_string(),
            }))
            .await
            .map(drop)
    }

    async fn unstage(&self) -> Result<(), Status> {
        self.node
            .node_unstage_volume(Request::new(NodeUnstageVolumeRequest {
                volume_id: volume_id(),
                staging_target_path: self.staging().display().to_string(),
            }))
            .await
            .map(drop)
    }

    async fn health(&self) -> Vec<(VolumeHealthErrorType, String)> {
        let rsp = self
            .node
            .node_get_volume_health(Request::new(NodeGetVolumeHealthRequest {
                volume_id: volume_id(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner()
            .volume_health
            .unwrap();
        assert_eq!(rsp.volume_id, volume_id());
        rsp.health_statuses
            .into_iter()
            .map(|e| (VolumeHealthErrorType::try_from(e.status).unwrap(), e.reason))
            .collect()
    }
}

async fn set_xattr(control: &InMemoryControl, path: &str, name: &str, value: &str) {
    control
        .browse_xattr(XattrParams {
            path: path.into(),
            op: XattrOp::Set {
                name: name.into(),
                value: value.as_bytes().to_vec().into(),
            },
        })
        .await
        .unwrap();
}

fn context() -> HashMap<String, String> {
    [
        ("bucket", "b"),
        ("prefix", "team/pool"),
        ("shards", "2"),
        ("endpoint", "http://s3:4566"),
        // What external-provisioner adds to every volume it provisions.
        (
            "storage.kubernetes.io/csiProvisionerIdentity",
            "1790930000000-1234-csi.constellation.dev",
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

fn pool(shard: u32) -> PoolRef {
    let mut class = context();
    class.retain(|k, _| !k.contains('/'));
    PoolRef {
        class: ClassParams::parse(&class).unwrap(),
        shard,
        secrets: Default::default(),
    }
}

fn volume_id() -> String {
    VolumeId::Pool {
        shard: 1,
        fs_uuid: UUID.into(),
        name: "pvc-a".into(),
    }
    .to_string()
}

fn capability(mode: Mode) -> VolumeCapability {
    VolumeCapability {
        access_type: Some(AccessType::Mount(MountVolume::default())),
        access_mode: Some(AccessMode { mode: mode as i32 }),
    }
}

fn stage_req(staging: &Path) -> NodeStageVolumeRequest {
    NodeStageVolumeRequest {
        volume_id: volume_id(),
        staging_target_path: staging.display().to_string(),
        volume_capability: Some(capability(Mode::MultiNodeMultiWriter)),
        volume_context: context(),
        secrets: [
            ("aws_access_key_id", "id"),
            ("aws_secret_access_key", "key"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect(),
        ..Default::default()
    }
}

fn code<T>(r: Result<T, Status>) -> tonic::Code {
    match r {
        Ok(_) => tonic::Code::Ok,
        Err(s) => s.code(),
    }
}

fn dev(mounter: &FakeMounter, path: &Path) -> u64 {
    match mounter.state(path) {
        MountState::Alive { dev, .. } => dev,
        other => panic!("{} is {other:?}", path.display()),
    }
}

#[tokio::test]
async fn stage_publish_unpublish_unstage() {
    let rig = Rig::new();
    let control = rig.plant().await;
    let staging = rig.staging();

    rig.stage(&staging).await.unwrap();
    let staged_calls = control.calls();
    // The FUSE mount is the plugin's; its descriptor went to the engine
    // with a view named by the staging path.
    assert!(rig.mounter.mount_at(&staging).unwrap().source.is_none());
    assert_eq!(control.fds_received(), 1);
    let views = control.view_list(Default::default()).await.unwrap().views;
    assert_eq!(views.len(), 1);
    assert_eq!(Path::new(&views[0].mountpoint), staging);
    assert_eq!(views[0].subtree, "/volumes/pvc-a");
    let labels = &views[0].labels;
    assert_eq!(labels["pv"], "pvc-a");
    assert_eq!(labels["pvc"], "data");
    assert_eq!(labels["namespace"], "tenant");
    assert_eq!(labels["shard"], "1");
    assert_eq!(labels[LABEL_VOLUME_ID], volume_id());
    assert_eq!(
        control.unlocks(),
        1,
        "the request's credentials reach the pod"
    );
    assert_eq!(control.last_unlock_key().as_deref(), Some("id"));
    assert!(!control.is_gated());
    // Plan 37 K6a: the engine's audit line for each call names the PV.
    assert!(!staged_calls.is_empty());
    for (method, who) in staged_calls {
        assert_eq!(who.as_deref(), Some("pvc-a"), "{method}");
    }
    assert_eq!(rig.engines.view_count(&rig.unit()), Some(1));
    let record = rig.node.state.get(&volume_id()).unwrap();
    assert_eq!(record.fs_uuid, UUID);
    assert_eq!(record.xattrs[X_PV], "pvc-a");
    assert!(
        record.context.contains_key("bucket") && !record.context.contains_key("aws_access_key_id")
    );

    // Idempotent: the same path again is OK and mounts nothing new.
    rig.stage(&staging).await.unwrap();
    assert_eq!(control.fds_received(), 1);
    assert_eq!(rig.engines.started(), 1);

    // Publish: a bind of the staging mount, read-only for a reader mode.
    let (t1, t2) = (rig.target(1), rig.target(2));
    rig.publish(&t1, Mode::MultiNodeMultiWriter).await.unwrap();
    rig.publish(&t1, Mode::MultiNodeMultiWriter).await.unwrap();
    rig.publish(&t2, Mode::MultiNodeReaderOnly).await.unwrap();
    let (m1, m2) = (
        rig.mounter.mount_at(&t1).unwrap(),
        rig.mounter.mount_at(&t2).unwrap(),
    );
    assert_eq!(m1.source.as_deref(), Some(staging.as_path()));
    assert!(!m1.read_only && m2.read_only);
    assert_eq!(dev(&rig.mounter, &t1), dev(&rig.mounter, &staging));
    assert_eq!(rig.mounter.mounted().len(), 3);
    assert_eq!(rig.node.state.get(&volume_id()).unwrap().published.len(), 2);
    // An incompatible republish (read-write onto the read-only bind, and
    // the reverse) is ALREADY_EXISTS, and leaves the binds as they were.
    assert_eq!(
        code(rig.publish(&t2, Mode::MultiNodeMultiWriter).await),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        code(rig.publish(&t1, Mode::MultiNodeReaderOnly).await),
        tonic::Code::AlreadyExists
    );
    assert!(rig.mounter.mount_at(&t2).unwrap().read_only);
    assert!(!rig.mounter.mount_at(&t1).unwrap().read_only);

    // Unpublish removes the bind and the directory; twice is fine.
    rig.unpublish(&t1).await.unwrap();
    rig.unpublish(&t1).await.unwrap();
    assert!(rig.mounter.mount_at(&t1).is_none());
    assert!(!t1.exists());
    rig.unpublish(&t2).await.unwrap();
    assert!(rig
        .node
        .state
        .get(&volume_id())
        .unwrap()
        .published
        .is_empty());

    // Unstage: the mount, the view and the record go; twice is fine.
    rig.unstage().await.unwrap();
    assert!(rig.mounter.mount_at(&staging).is_none());
    assert!(control
        .view_list(Default::default())
        .await
        .unwrap()
        .views
        .is_empty());
    assert!(rig.node.state.get(&volume_id()).is_none());
    assert_eq!(rig.engines.view_count(&rig.unit()), Some(0));
    rig.unstage().await.unwrap();
}

#[tokio::test]
async fn requests_are_validated_and_unknown_volumes_are_not_found() {
    let rig = Rig::new();
    let staging = rig.staging();
    let mut no_id = stage_req(&staging);
    no_id.volume_id.clear();
    let mut no_path = stage_req(&staging);
    no_path.staging_target_path.clear();
    let mut no_cap = stage_req(&staging);
    no_cap.volume_capability = None;
    let mut block = stage_req(&staging);
    block.volume_capability = Some(VolumeCapability {
        access_type: Some(AccessType::Block(Default::default())),
        access_mode: Some(AccessMode {
            mode: Mode::SingleNodeWriter as i32,
        }),
    });
    let mut garbage = stage_req(&staging);
    garbage.volume_id = "fake-volume-id".into();
    let mut no_bucket = stage_req(&staging);
    no_bucket.volume_context.remove("bucket");
    for (req, want) in [
        (no_id, tonic::Code::InvalidArgument),
        (no_path, tonic::Code::InvalidArgument),
        (no_cap, tonic::Code::InvalidArgument),
        (block, tonic::Code::InvalidArgument),
        (garbage, tonic::Code::NotFound),
        (no_bucket, tonic::Code::InvalidArgument),
        // The pool exists, the volume directory does not.
        (stage_req(&staging), tonic::Code::NotFound),
    ] {
        let got = code(rig.node.node_stage_volume(Request::new(req.clone())).await);
        assert_eq!(got, want, "{req:?}");
    }
    assert!(
        rig.mounter.mounted().is_empty(),
        "nothing is mounted for a refused stage"
    );

    // An engine serving another filesystem than the id names.
    let other = Rig::new();
    let control = other.engines.plant(&pool(1), "another-uuid");
    control.plant_dir("/volumes/pvc-a");
    assert_eq!(
        code(other.stage(&other.staging()).await),
        tonic::Code::NotFound
    );

    let publish = |target: &str, cap: Option<VolumeCapability>| NodePublishVolumeRequest {
        volume_id: volume_id(),
        staging_target_path: staging.display().to_string(),
        target_path: target.into(),
        volume_capability: cap,
        ..Default::default()
    };
    let cap = Some(capability(Mode::SingleNodeWriter));
    for (req, want) in [
        (publish("", cap.clone()), tonic::Code::InvalidArgument),
        (publish("/t", None), tonic::Code::InvalidArgument),
        // Never staged here.
        (publish("/t", cap.clone()), tonic::Code::FailedPrecondition),
    ] {
        assert_eq!(
            code(rig.node.node_publish_volume(Request::new(req)).await),
            want
        );
    }
    for (id, path, want) in [
        ("", "/p", tonic::Code::InvalidArgument),
        ("x", "", tonic::Code::InvalidArgument),
        ("fake-volume-id", "some/path", tonic::Code::NotFound),
    ] {
        let got = rig
            .node
            .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
                volume_id: id.into(),
                volume_path: path.into(),
                ..Default::default()
            }))
            .await;
        assert_eq!(code(got), want, "{id:?} {path:?}");
    }
    // Unpublishing and unstaging what is not there is OK (idempotent).
    rig.unpublish(&rig.target(9)).await.unwrap();
    rig.unstage().await.unwrap();
}

#[tokio::test]
async fn a_volume_staged_elsewhere_is_already_exists() {
    let rig = Rig::new();
    rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let elsewhere = rig.path("kubelet/staging/other/globalmount");
    assert_eq!(
        code(rig.stage(&elsewhere).await),
        tonic::Code::AlreadyExists
    );
}

#[tokio::test]
async fn single_writer_refuses_a_second_target() {
    let rig = Rig::new();
    rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let mode = Mode::SingleNodeSingleWriter;
    rig.publish(&rig.target(1), mode).await.unwrap();
    rig.publish(&rig.target(1), mode).await.unwrap();
    assert_eq!(
        code(rig.publish(&rig.target(2), mode).await),
        tonic::Code::FailedPrecondition
    );
    // A multi-writer mode is not held to it.
    rig.publish(&rig.target(2), Mode::SingleNodeMultiWriter)
        .await
        .unwrap();
}

/// Settled decision 12: an engine pod crash leaves the staging mount
/// `ENOTCONN`; the next publish restages against the pod's next
/// incarnation and binds the target again.
#[tokio::test]
async fn a_dead_staging_mount_is_restaged_on_the_next_publish() {
    let rig = Rig::new();
    let control = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let target = rig.target(1);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    let before = dev(&rig.mounter, &staging);

    rig.engines.crash(&rig.unit());
    rig.mounter.kill(&staging);
    assert_eq!(rig.mounter.state(&target), MountState::Dead);
    assert!(rig
        .health()
        .await
        .contains(&(VolumeHealthErrorType::Inaccessible, "EngineGone".into())));

    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    let after = dev(&rig.mounter, &staging);
    assert_ne!(before, after, "a new FUSE mount");
    assert_eq!(
        dev(&rig.mounter, &target),
        after,
        "the target binds the new mount"
    );
    assert_eq!(control.fds_received(), 2);
    assert_eq!(
        control
            .view_list(Default::default())
            .await
            .unwrap()
            .views
            .len(),
        1
    );
    // The publish carries no secret: the new incarnation (an empty
    // credential store) gets what the plugin last sent the unit.
    assert_eq!(control.unlocks(), 2);
    assert_eq!(control.last_unlock_key().as_deref(), Some("id"));
    let record = rig.node.state.get(&volume_id()).unwrap();
    assert!(record.published.contains(&target));
    assert!(rig.health().await.is_empty());

    // A stage after a crash restages too (and unlocks the new pod).
    rig.engines.crash(&rig.unit());
    rig.mounter.kill(&staging);
    rig.stage(&staging).await.unwrap();
    assert_eq!(control.fds_received(), 3);
    assert_eq!(control.unlocks(), 3);
    assert!(matches!(
        rig.mounter.state(&staging),
        MountState::Alive { .. }
    ));
}

#[tokio::test]
async fn stats_report_the_quota_and_the_subtree() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let target = rig.target(1);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    control
        .quota_set(SubtreeQuotaParams {
            subtree: "/volumes/pvc-a".into(),
            max_bytes: Some(10 << 20),
        })
        .await
        .unwrap();
    control.plant_dir("/volumes/pvc-a/sub");
    control.set_used_bytes("/volumes/pvc-a/sub", 3 << 20);
    let stats = |path: &Path| NodeGetVolumeStatsRequest {
        volume_id: volume_id(),
        volume_path: path.display().to_string(),
        ..Default::default()
    };
    let usage = rig
        .node
        .node_get_volume_stats(Request::new(stats(&target)))
        .await
        .unwrap()
        .into_inner()
        .usage;
    let bytes = usage.iter().find(|u| u.unit == Unit::Bytes as i32).unwrap();
    assert_eq!(
        (bytes.total, bytes.used, bytes.available),
        (10 << 20, 3 << 20, 7 << 20)
    );
    let inodes = usage
        .iter()
        .find(|u| u.unit == Unit::Inodes as i32)
        .unwrap();
    assert_eq!(inodes.used, 2, "the volume directory and its child");
    assert_eq!(inodes.available, inodes.total - inodes.used);
    // The staging path answers too; a path the volume is not mounted at
    // does not.
    rig.node
        .node_get_volume_stats(Request::new(stats(&rig.staging())))
        .await
        .unwrap();
    assert_eq!(
        code(
            rig.node
                .node_get_volume_stats(Request::new(stats(Path::new("some/path"))))
                .await
        ),
        tonic::Code::NotFound
    );
}

/// Plan 37 §11 / settled decision 18: a volume edited outside Kubernetes
/// is reported, not refused.
#[tokio::test]
async fn health_reports_a_volume_changed_behind_the_drivers_back() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    assert!(rig.health().await.is_empty());

    set_xattr(&control, "/volumes/pvc-a", X_PV, "pvc-someone-else").await;
    assert_eq!(
        rig.health().await,
        [(
            VolumeHealthErrorType::Degraded,
            "VolumeRecordChanged".to_string()
        )]
    );
    control.remove_tree("/volumes/pvc-a");
    assert_eq!(
        rig.health().await,
        [(VolumeHealthErrorType::DataLoss, "VolumeRemoved".to_string())]
    );
    // Stats still answer: a condition is reported, never enforced.
    rig.node
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: volume_id(),
            volume_path: rig.staging().display().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        code(
            rig.node
                .node_get_volume_health(Request::new(NodeGetVolumeHealthRequest {
                    volume_id: "v1/dedicated/nope/".into(),
                    ..Default::default()
                }))
                .await
        ),
        tonic::Code::NotFound
    );
}

/// A plugin restart loses nothing: its records are on the hostPath, the
/// mounts in the host's namespace, the views in the engine pod.
#[tokio::test]
async fn a_restarted_plugin_carries_on_from_its_records() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    rig.publish(&rig.target(1), Mode::MultiNodeMultiWriter)
        .await
        .unwrap();

    let restarted = Rig {
        node: Rig::service(&rig.dir, &rig.mounter, &rig.engines),
        ..rig
    };
    restarted.stage(&restarted.staging()).await.unwrap();
    assert_eq!(
        control.fds_received(),
        1,
        "still staged: nothing mounted anew"
    );
    restarted
        .node
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: volume_id(),
            volume_path: restarted.target(1).display().to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    restarted.unpublish(&restarted.target(1)).await.unwrap();
    restarted.unstage().await.unwrap();
    assert!(control
        .view_list(Default::default())
        .await
        .unwrap()
        .views
        .is_empty());
}

/// A record lost (or never written: the plugin died between `view.mount`
/// and the record) leaves a served view under the staging path; the next
/// stage adopts it instead of mounting a second session.
#[tokio::test]
async fn a_lost_record_adopts_the_view_the_engine_still_serves() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    std::fs::remove_dir_all(rig.path("host/volumes")).unwrap();

    let restarted = Rig {
        node: Rig::service(&rig.dir, &rig.mounter, &rig.engines),
        ..rig
    };
    assert!(restarted.node.state.get(&volume_id()).is_none());
    restarted.stage(&restarted.staging()).await.unwrap();
    assert_eq!(control.fds_received(), 1, "adopted, not mounted again");
    assert!(restarted.node.state.get(&volume_id()).is_some());

    // A view at the path that is not this volume's is replaced.
    let restarted = Rig {
        node: Rig::service(&restarted.dir, &restarted.mounter, &restarted.engines),
        ..restarted
    };
    std::fs::remove_dir_all(restarted.path("host/volumes")).unwrap();
    let restarted = Rig {
        node: Rig::service(&restarted.dir, &restarted.mounter, &restarted.engines),
        ..restarted
    };
    restarted.mounter.kill(&restarted.staging());
    restarted.stage(&restarted.staging()).await.unwrap();
    assert_eq!(control.fds_received(), 2, "a dead mount is not adopted");
    assert_eq!(
        control
            .view_list(Default::default())
            .await
            .unwrap()
            .views
            .len(),
        1
    );
}

#[tokio::test]
async fn static_and_dedicated_ids_stage_their_own_subtree() {
    let rig = Rig::new();
    // A static handle into the same pool's shard 1, located by its
    // volumeAttributes (the class keys and `shard`).
    let control = rig.engines.plant(&pool(1), UUID);
    control.plant_dir("/datasets/imagenet");
    let mut req = stage_req(&rig.staging());
    req.volume_id = format!("{UUID}/datasets/imagenet");
    req.volume_context.insert("shard".into(), "1".into());
    rig.node
        .node_stage_volume(Request::new(req.clone()))
        .await
        .unwrap();
    let views = control.view_list(Default::default()).await.unwrap().views;
    assert_eq!(views[0].subtree, "/datasets/imagenet");
    // A shard the pool does not have.
    req.volume_id = format!("{UUID}/datasets/other");
    req.volume_context.insert("shard".into(), "7".into());
    req.staging_target_path = rig.path("s2").display().to_string();
    assert_eq!(
        code(rig.node.node_stage_volume(Request::new(req)).await),
        tonic::Code::InvalidArgument
    );

    // A dedicated volume is its filesystem's root.
    let mut ctx = context();
    ctx.retain(|k, _| !k.contains('/'));
    ctx.insert("layout".into(), "dedicated".into());
    ctx.remove("shards");
    let dedicated = PoolRef {
        class: ClassParams::parse(&ctx).unwrap(),
        shard: 0,
        secrets: Default::default(),
    };
    let control = rig.engines.plant(&dedicated, "dedicated-uuid");
    let mut req = stage_req(&rig.path("s3"));
    req.volume_id = "v1/dedicated/dedicated-uuid/".into();
    req.volume_context = ctx;
    rig.node.node_stage_volume(Request::new(req)).await.unwrap();
    let views = control.view_list(Default::default()).await.unwrap().views;
    assert_eq!(views[0].subtree, "/");
}

#[tokio::test]
async fn capabilities_and_info() {
    let rig = Rig::new();
    let caps = rig
        .node
        .node_get_capabilities(Request::new(NodeGetCapabilitiesRequest {}))
        .await
        .unwrap()
        .into_inner();
    let types: Vec<RpcType> = caps
        .capabilities
        .iter()
        .filter_map(|c| match c.r#type {
            Some(CapabilityType::Rpc(Rpc { r#type })) => RpcType::try_from(r#type).ok(),
            None => None,
        })
        .collect();
    assert_eq!(
        types,
        [
            RpcType::StageUnstageVolume,
            RpcType::GetVolumeStats,
            RpcType::SingleNodeMultiWriter,
            RpcType::GetVolumeHealth
        ]
    );
    assert!(
        !types.contains(&RpcType::ExpandVolume),
        "settled decision 6"
    );
    let info = rig
        .node
        .node_get_info(Request::new(NodeGetInfoRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.node_id, "node-a");
    assert!(info.accessible_topology.is_none());
}

#[tokio::test]
async fn without_an_engine_backend_staging_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let node = NodeService::new(
        "n".into(),
        None,
        Arc::new(FakeMounter::default()),
        StateStore::open(dir.path()).unwrap(),
    );
    let got = node
        .node_stage_volume(Request::new(stage_req(&dir.path().join("s"))))
        .await;
    assert_eq!(code(got), tonic::Code::Unavailable);
    // csi-sanity's Controller-group cleanup still works.
    node.node_unpublish_volume(Request::new(NodeUnpublishVolumeRequest {
        volume_id: "v".into(),
        target_path: dir.path().join("t").display().to_string(),
    }))
    .await
    .unwrap();
}

/// Plan 37 §9: the remembered credentials live in the plugin's memory
/// only. A restarted plugin whose engine pod then dies cannot restage a
/// `static-ephemeral` volume from a publish (no secret in it): the publish
/// fails `UNAVAILABLE`, saying why, instead of serving a dead mount — and a
/// publish that brings the class's node-publish secret repairs it, as a
/// stage that brings the secret would.
#[tokio::test]
async fn a_restarted_plugin_cannot_unlock_a_crashed_engine_without_a_secret() {
    let rig = Rig::new();
    let control = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let target = rig.target(1);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    let restarted = Rig::service(&rig.dir, &rig.mounter, &rig.engines);
    rig.engines.crash(&rig.unit());
    rig.mounter.kill(&staging);
    assert!(control.is_gated());
    let refused = restarted
        .node_publish_volume(Request::new(NodePublishVolumeRequest {
            volume_id: volume_id(),
            staging_target_path: staging.display().to_string(),
            target_path: target.display().to_string(),
            volume_capability: Some(capability(Mode::MultiNodeMultiWriter)),
            volume_context: context(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert!(
        refused.message().contains("waits for its credentials"),
        "{refused:?}"
    );
    assert_eq!(control.unlocks(), 1);
    restarted
        .node_publish_volume(Request::new(NodePublishVolumeRequest {
            volume_id: volume_id(),
            staging_target_path: staging.display().to_string(),
            target_path: target.display().to_string(),
            volume_capability: Some(capability(Mode::MultiNodeMultiWriter)),
            volume_context: context(),
            secrets: stage_req(&staging).secrets,
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(control.unlocks(), 2);
    assert!(!control.is_gated());
    assert!(matches!(
        rig.mounter.state(&staging),
        MountState::Alive { .. }
    ));
}

/// Plan 37 §9 `credentialSource: aws-default-chain`: no secret, no
/// unlock — the engine pod never waits (it signs with its own
/// ServiceAccount's chain).
#[tokio::test]
async fn an_aws_default_chain_engine_is_never_unlocked() {
    let rig = Rig::new();
    let mut ctx = context();
    ctx.insert("credentialSource".into(), "aws-default-chain".into());
    let mut class = ctx.clone();
    class.retain(|k, _| !k.contains('/'));
    let chain = PoolRef {
        class: ClassParams::parse(&class).unwrap(),
        shard: 1,
        secrets: Default::default(),
    };
    let control = rig.plant_in(&chain).await;
    assert!(!control.is_gated());
    let mut req = stage_req(&rig.staging());
    req.volume_context = ctx;
    req.secrets.clear();
    rig.node.node_stage_volume(Request::new(req)).await.unwrap();
    assert_eq!(control.unlocks(), 0);
    assert_eq!(control.fds_received(), 1);
}

/// Plan 37 §9 `Refreshing(callback)`: a `refreshing` class's engine pod
/// is unlocked from the watched Secret (here with no node-stage secret in
/// the request at all), and every rotation of that Secret is pushed to the
/// running pod at once — the same FUSE mount, no restage — and is what a
/// crashed pod's replacement gets.
#[tokio::test]
async fn a_refreshing_class_pushes_each_rotation_to_the_running_engine() {
    use crate::credentials::fake::FakeSecrets;
    let secret = |id: &str| -> Secrets {
        [("aws_access_key_id", id), ("aws_secret_access_key", "k")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let fake = Arc::new(FakeSecrets::default());
    fake.set(Some(secret("WATCHED-1")));
    let dir = tempfile::tempdir().unwrap();
    let mounter = Arc::new(FakeMounter::default());
    let engines = Arc::new(InMemoryNodeEngines::new("node-a"));
    let refresher = Refresher::with_backoff(Arc::new(fake.clone()), Duration::from_millis(10));
    let rig = Rig {
        node: Rig::service(&dir, &mounter, &engines).with_refresher(refresher),
        dir,
        mounter,
        engines,
    };
    let mut ctx = context();
    for (k, v) in [
        ("credentialSource", "refreshing"),
        ("credentialSecretName", "s3-creds"),
        ("credentialSecretNamespace", "tenant"),
    ] {
        ctx.insert(k.into(), v.into());
    }
    let mut class = ctx.clone();
    class.retain(|k, _| !k.contains('/'));
    let refreshing = PoolRef {
        class: ClassParams::parse(&class).unwrap(),
        shard: 1,
        secrets: Default::default(),
    };
    let control = rig.plant_in(&refreshing).await;
    let staging = rig.staging();
    let mut req = stage_req(&staging);
    req.volume_context = ctx;
    req.secrets.clear();
    rig.node.node_stage_volume(Request::new(req)).await.unwrap();
    assert_eq!(control.last_unlock_key().as_deref(), Some("WATCHED-1"));
    let mount = dev(&rig.mounter, &staging);

    let unit = rig.engines.names(&refreshing).0;
    for n in 2..=3 {
        fake.set(Some(secret(&format!("WATCHED-{n}"))));
        let want = format!("WATCHED-{n}");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while control.last_unlock_key().as_deref() != Some(want.as_str()) {
            assert!(
                std::time::Instant::now() < deadline,
                "rotation {n} not pushed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    assert_eq!(control.unlocks(), 3, "one unlock per distinct credential");
    assert_eq!(dev(&rig.mounter, &staging), mount, "no remount");
    assert_eq!(control.fds_received(), 1);
    // A rotation to the same bytes (a resync) sends nothing.
    fake.set(Some(secret("WATCHED-3")));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(control.unlocks(), 3);
    // A crash's replacement gets the latest rotation.
    rig.engines.crash(&unit);
    rig.mounter.kill(&staging);
    assert!(control.is_gated());
    rig.node
        .node_stage_volume(Request::new({
            let mut r = stage_req(&staging);
            r.volume_context = rig
                .node
                .state
                .get(&volume_id())
                .unwrap()
                .context
                .into_iter()
                .collect();
            r.secrets.clear();
            r
        }))
        .await
        .unwrap();
    assert!(!control.is_gated());
    assert_eq!(control.last_unlock_key().as_deref(), Some("WATCHED-3"));
    // Each push names the Secret it came from in the engine's audit line.
    let pushes: Vec<_> = control
        .calls()
        .into_iter()
        .filter(|(m, _)| *m == "fs.unlock")
        .collect();
    assert!(
        pushes
            .iter()
            .filter(|(_, who)| who.as_deref() == Some("secret:tenant/s3-creds"))
            .count()
            >= 2,
        "{pushes:?}"
    );

    // The last volume of the unit unstaged: the plugin forgets the unit's
    // credentials and stops pushing to it.
    rig.unstage().await.unwrap();
    assert!(rig.node.remembered.lock().unwrap().get(&unit).is_none());
    assert!(rig.node.rotating.lock().unwrap().is_empty());
    let unlocks = control.unlocks();
    fake.set(Some(secret("WATCHED-4")));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        control.unlocks(),
        unlocks,
        "pushed to a unit with no volume"
    );
}

/// Plan 37 K6a review: the plugin keeps a unit's credentials only while
/// one of its volumes is staged here.
#[tokio::test]
async fn an_unstaged_units_credentials_are_forgotten() {
    let rig = Rig::new();
    rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    assert!(rig
        .node
        .remembered
        .lock()
        .unwrap()
        .contains_key(&rig.unit()));
    rig.unstage().await.unwrap();
    assert!(!rig
        .node
        .remembered
        .lock()
        .unwrap()
        .contains_key(&rig.unit()));
}

/// Plan 37 K6a review: an unstage's "no volume left" check waits for a
/// concurrent stage of another volume of the same unit to finish recording
/// it and keeping its credentials, then sees the volume and forgets nothing.
#[test]
fn forgetting_waits_for_a_concurrent_stage_of_the_same_unit() {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(NodeService::new(
        "node-a".into(),
        None,
        Arc::new(FakeMounter::default()),
        StateStore::open(&dir.path().join("volumes")).unwrap(),
    ));
    // The stage is between recording its volume and keeping credentials.
    let lock = node.unit_lock("unit");
    let held = lock.lock().unwrap();
    let forgetting = {
        let node = node.clone();
        std::thread::spawn(move || node.forget_credentials("unit"))
    };
    std::thread::sleep(Duration::from_millis(200));
    assert!(!forgetting.is_finished(), "forgot without the unit's lock");
    let mut r = VolumeRecord::new("vol-2", &dir.path().join("s2"));
    r.unit = "unit".into();
    r.pod = "pod".into();
    node.state.put(r).unwrap();
    node.remembered
        .lock()
        .unwrap()
        .insert("unit".into(), Secrets::new());
    drop(held);
    forgetting.join().unwrap();
    assert!(node.remembered.lock().unwrap().contains_key("unit"));
}

/// [`InMemoryNodeEngines`] whose `last-view-count` patches take longer
/// the lower the count, as API-server round trips sometimes do: unordered,
/// a stage that counted 1 lands after one that counted 2.
struct SlowAnnotations(Arc<InMemoryNodeEngines>);

#[async_trait::async_trait]
impl NodeEngines for SlowAnnotations {
    fn names(&self, pool: &PoolRef) -> (String, String) {
        self.0.names(pool)
    }
    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        unlock: Option<&Secrets>,
    ) -> Result<engines::NodeEngine, constellation_control::proto::ControlError> {
        self.0.engine(pool, fs_uuid, unlock).await
    }
    async fn rotate(
        &self,
        unit: &str,
        secrets: &Secrets,
    ) -> Result<bool, constellation_control::proto::ControlError> {
        self.0.rotate(unit, secrets).await
    }
    async fn existing(
        &self,
        unit: &str,
    ) -> Result<Option<engines::NodeEngine>, constellation_control::proto::ControlError> {
        self.0.existing(unit).await
    }
    async fn set_view_count(
        &self,
        unit: &str,
        views: usize,
    ) -> Result<(), constellation_control::proto::ControlError> {
        tokio::time::sleep(Duration::from_millis(100 / views.max(1) as u64)).await;
        self.0.set_view_count(unit, views).await
    }
}

#[tokio::test]
async fn concurrent_stages_leave_the_last_view_count_not_a_stale_one() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemoryNodeEngines::new("node-a"));
    let slow = SlowAnnotations(inner.clone());
    let node = NodeService::new(
        "node-a".into(),
        None,
        Arc::new(FakeMounter::default()),
        StateStore::open(&dir.path().join("volumes")).unwrap(),
    );
    let record = |n: u32| {
        let mut r = VolumeRecord::new(&format!("vol-{n}"), &dir.path().join(format!("s{n}")));
        r.unit = "unit".into();
        r.pod = "pod".into();
        r
    };
    node.state.put(record(1)).unwrap();
    // The first stage counts 1 and is slow to record it; the second
    // stage's volume lands meanwhile and it counts 2.
    let first = node.record_views(&slow, "unit", "pod");
    let second = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        node.state.put(record(2)).unwrap();
        node.record_views(&slow, "unit", "pod").await
    };
    tokio::join!(first, second);
    assert_eq!(inner.view_count("unit"), Some(2));
}

/// [`InMemoryNodeEngines`] whose `last-view-count` patches of unit
/// `hung` never come back, as against a hung API server.
struct HungAnnotations(Arc<InMemoryNodeEngines>);

#[async_trait::async_trait]
impl NodeEngines for HungAnnotations {
    fn names(&self, pool: &PoolRef) -> (String, String) {
        self.0.names(pool)
    }
    async fn engine(
        &self,
        pool: &PoolRef,
        fs_uuid: &str,
        unlock: Option<&Secrets>,
    ) -> Result<engines::NodeEngine, constellation_control::proto::ControlError> {
        self.0.engine(pool, fs_uuid, unlock).await
    }
    async fn rotate(
        &self,
        unit: &str,
        secrets: &Secrets,
    ) -> Result<bool, constellation_control::proto::ControlError> {
        self.0.rotate(unit, secrets).await
    }
    async fn existing(
        &self,
        unit: &str,
    ) -> Result<Option<engines::NodeEngine>, constellation_control::proto::ControlError> {
        self.0.existing(unit).await
    }
    async fn set_view_count(
        &self,
        unit: &str,
        views: usize,
    ) -> Result<(), constellation_control::proto::ControlError> {
        if unit == "hung" {
            std::future::pending::<()>().await;
        }
        self.0.set_view_count(unit, views).await
    }
}

#[tokio::test]
async fn a_hung_view_count_patch_holds_up_neither_other_units_nor_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(InMemoryNodeEngines::new("node-a"));
    let hung = HungAnnotations(inner.clone());
    let mut node = NodeService::new(
        "node-a".into(),
        None,
        Arc::new(FakeMounter::default()),
        StateStore::open(&dir.path().join("volumes")).unwrap(),
    );
    node.annotation_timeout = Duration::from_millis(500);
    for (n, unit) in [(1, "hung"), (2, "ok")] {
        let mut r = VolumeRecord::new(&format!("vol-{n}"), &dir.path().join(format!("s{n}")));
        r.unit = unit.into();
        r.pod = format!("pod-{unit}");
        node.state.put(r).unwrap();
    }
    let t0 = std::time::Instant::now();
    let stuck = node.record_views(&hung, "hung", "pod-hung");
    let other = async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        node.record_views(&hung, "ok", "pod-ok").await;
        t0.elapsed()
    };
    let ((), other_done) = tokio::join!(stuck, other);
    // The other unit's patch did not queue behind the hung one.
    assert!(
        other_done < Duration::from_millis(400),
        "unit ok recorded after {other_done:?}"
    );
    assert_eq!(inner.view_count("ok"), Some(1));
    // The hung patch gave up at the timeout, not never.
    let all = t0.elapsed();
    assert!(
        all >= Duration::from_millis(500) && all < Duration::from_secs(5),
        "{all:?}"
    );
    assert_eq!(inner.view_count("hung"), None);
}

// ---- plan 37 §8: rollouts by session handoff ----

use super::handoff::{Outcome, Step};
use super::rollout::Rolled;
use constellation_control::proto::types::HandoffPhase;

#[tokio::test]
async fn a_drifted_engine_pod_hands_its_views_to_a_replacement() {
    let rig = Rig::new();
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let target = rig.target(1);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    let mount = dev(&rig.mounter, &staging);

    // Nothing drifted: nothing to do.
    assert!(rig.node.rollout_once().await.is_empty());

    rig.engines.set_desired("spec-2");
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(&rolled[..], [(unit, Rolled::HandedOff(Outcome::Succeeded { views: 1, .. }))] if *unit == rig.unit()),
        "{rolled:?}"
    );
    // The old engine is gone; the new one serves the very same mount (the
    // kernel connection was handed over, nothing was remounted).
    assert!(old.handed_off());
    assert_eq!(rig.engines.spec_of(&rig.unit()).as_deref(), Some("spec-2"));
    let new = rig.engines.control(&rig.unit()).unwrap();
    assert!(!Arc::ptr_eq(&new, &old));
    // 37-k6a: it waited for its credentials, and got the old pod's.
    assert!(!new.is_gated());
    assert_eq!(new.last_unlock_key(), old.last_unlock_key());
    assert_eq!(new.view_mountpoints(), [staging.display().to_string()]);
    assert_eq!(dev(&rig.mounter, &staging), mount, "the same mount");
    assert!(matches!(
        rig.mounter.state(&target),
        MountState::Alive { .. }
    ));
    let record = rig.node.state.get(&volume_id()).unwrap();
    assert_eq!(record.unit, rig.unit());
    assert_eq!(rig.engines.view_count(&rig.unit()), Some(1));
    assert!(rig.health().await.is_empty());
    // Converged: the next pass has nothing to do, and the volume unstages
    // from the new engine.
    assert!(rig.node.rollout_once().await.is_empty());
    rig.unpublish(&target).await.unwrap();
    rig.unstage().await.unwrap();
    assert!(new.view_mountpoints().is_empty());
    let metrics = rig.node.handoff_metrics().render();
    assert!(metrics.contains("constellation_csi_handoff_total{outcome=\"succeeded\"} 1"));
}

/// 37-k6a with K5a: a `static-ephemeral` engine pod is handed over by a
/// node plugin that restarted since it staged the volume — so holds no
/// secret for it (the chart upgrade that rolls the pods restarts it) — and
/// the replacement serves on the very credentials the old pod held,
/// handed over by the old pod itself.
#[tokio::test]
async fn a_restarted_plugin_hands_over_an_engine_it_holds_no_credentials_for() {
    let rig = Rig::new();
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    assert_eq!(old.last_unlock_key().as_deref(), Some("id"));
    let mount = dev(&rig.mounter, &staging);
    let rig = Rig {
        node: Rig::service(&rig.dir, &rig.mounter, &rig.engines),
        ..rig
    };
    assert!(rig.node.remembered.lock().unwrap().is_empty());

    rig.engines.set_desired("spec-2");
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(
            &rolled[..],
            [(_, Rolled::HandedOff(Outcome::Succeeded { views: 1, .. }))]
        ),
        "{rolled:?}"
    );
    let new = rig.engines.control(&rig.unit()).unwrap();
    assert!(!Arc::ptr_eq(&new, &old));
    assert!(!new.is_gated(), "unlocked by the handoff");
    assert_eq!(new.last_unlock_key().as_deref(), Some("id"));
    assert_eq!(new.unlocks(), 0, "no fs.unlock from this plugin");
    assert_eq!(dev(&rig.mounter, &staging), mount, "the same mount");
}

/// The lock order (37-k6a with K5a): a `refreshing` class's rotation push
/// holds the unit's gate shared, so one that comes while a handoff holds
/// the gate waits for it, and lands on the pod that serves afterwards
/// rather than on the one being retired.
#[tokio::test]
async fn a_rotation_waits_for_a_running_handoff() {
    use crate::credentials::fake::FakeSecrets;
    let secret = |id: &str| -> Secrets {
        [("aws_access_key_id", id), ("aws_secret_access_key", "k")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    };
    let fake = Arc::new(FakeSecrets::default());
    fake.set(Some(secret("WATCHED-1")));
    let dir = tempfile::tempdir().unwrap();
    let mounter = Arc::new(FakeMounter::default());
    let engines = Arc::new(InMemoryNodeEngines::new("node-a"));
    let refresher = Refresher::with_backoff(Arc::new(fake.clone()), Duration::from_millis(10));
    let rig = Rig {
        node: Rig::service(&dir, &mounter, &engines).with_refresher(refresher),
        dir,
        mounter,
        engines,
    };
    let mut ctx = context();
    for (k, v) in [
        ("credentialSource", "refreshing"),
        ("credentialSecretName", "s3-creds"),
        ("credentialSecretNamespace", "tenant"),
    ] {
        ctx.insert(k.into(), v.into());
    }
    let mut class = ctx.clone();
    class.retain(|k, _| !k.contains('/'));
    let refreshing = PoolRef {
        class: ClassParams::parse(&class).unwrap(),
        shard: 1,
        secrets: Default::default(),
    };
    let control = rig.plant_in(&refreshing).await;
    let mut req = stage_req(&rig.staging());
    req.volume_context = ctx;
    req.secrets.clear();
    rig.node.node_stage_volume(Request::new(req)).await.unwrap();
    assert_eq!(control.last_unlock_key().as_deref(), Some("WATCHED-1"));
    let unit = rig.engines.names(&refreshing).0;

    // A handoff holds the gate; the Secret rotates meanwhile.
    let held = rig.node.unit_gate(&unit).write_owned().await;
    fake.set(Some(secret("WATCHED-2")));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        control.last_unlock_key().as_deref(),
        Some("WATCHED-1"),
        "not pushed into the handoff"
    );
    drop(held);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while control.last_unlock_key().as_deref() != Some("WATCHED-2") {
        assert!(std::time::Instant::now() < deadline, "rotation not pushed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn an_idle_drifted_engine_pod_is_retired_not_handed_over() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.engines.set_desired("spec-2");
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::Retired)]), "{rolled:?}");
    assert!(control.handoff_calls().is_empty());
    assert!(rig.engines.drifted().await.unwrap().is_empty());
}

/// §8 "Failure handling": a handoff that fails rolls back (the old pod
/// serves on), is retried at the next pass, and after `max_attempts` the
/// rollout gives up on the unit and says so.
#[tokio::test]
async fn failed_handoffs_roll_back_then_fall_back_after_max_attempts() {
    let rig = Rig::new();
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    rig.engines.set_desired("spec-2");

    // First a replacement that never starts, then two prepares that fail.
    rig.engines.fail_replacement_starts(1);
    old.fail_handoff(HandoffPhase::Prepare);
    old.fail_handoff(HandoffPhase::Prepare);
    let first = rig.node.rollout_once().await;
    assert!(
        matches!(&first[..], [(_, Rolled::NoReplacement(_))]),
        "{first:?}"
    );
    for _ in 0..2 {
        let rolled = rig.node.rollout_once().await;
        assert!(
            matches!(
                &rolled[..],
                [(
                    _,
                    Rolled::HandedOff(Outcome::RolledBack {
                        step: Step::Prepare,
                        restored: true,
                        ..
                    })
                )]
            ),
            "{rolled:?}"
        );
        // The old pod still serves, and the replacement went.
        assert_eq!(old.view_mountpoints(), [staging.display().to_string()]);
        assert!(rig.engines.standby(&rig.unit()).is_none());
    }
    assert_eq!(rig.engines.fallbacks().len(), 1, "reported once");
    assert!(rig.engines.fallbacks()[0].1.contains("giving up"));
    // Given up for this desired spec: no more attempts.
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::GaveUp)]), "{rolled:?}");
    assert_eq!(
        old.handoff_calls()
            .iter()
            .filter(|p| **p == HandoffPhase::Prepare)
            .count(),
        2
    );
    let metrics = rig.node.handoff_metrics().render();
    assert!(metrics.contains("constellation_csi_handoff_fallback_total 1"));
    // A further spec change starts the count again — and succeeds.
    rig.engines.set_desired("spec-3");
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(
            &rolled[..],
            [(_, Rolled::HandedOff(Outcome::Succeeded { .. }))]
        ),
        "{rolled:?}"
    );
}

/// A handoff lost after its commit cannot be undone: both pods go, and
/// the next publish restages the volume onto a fresh one (§8's
/// `requiresRepublish` fallback).
#[tokio::test]
async fn a_handoff_lost_after_its_commit_is_restaged_by_the_republish() {
    let rig = Rig::new();
    rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let target = rig.target(1);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    rig.engines.set_desired("spec-2");
    let unit = rig.unit();
    // The replacement the rollout starts will not resume.
    rig.engines
        .fail_resume_of_next_standby("its engine did not start");
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(
            &rolled[..],
            [(
                _,
                Rolled::HandedOff(Outcome::Lost {
                    step: Step::Resume,
                    ..
                })
            )]
        ),
        "{rolled:?}"
    );
    // The mount died with the old engine; kubelet's republish restages it.
    rig.mounter.kill(&staging);
    rig.publish(&target, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    let control = rig.engines.control(&unit).unwrap();
    assert_eq!(control.view_mountpoints(), [staging.display().to_string()]);
    assert_eq!(rig.engines.spec_of(&unit).as_deref(), Some("spec-2"));
}

/// An engine pod that lost its views (it restarted: their mounts are dead)
/// has nothing to hand over: the rollout deletes it rather than failing
/// prepares until it gives up, and the republish restages onto the next.
#[tokio::test]
async fn a_drifted_pod_that_lost_its_views_is_retired() {
    let rig = Rig::new();
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    old.drop_views();
    rig.engines.set_desired("spec-2");
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::Retired)]), "{rolled:?}");
    assert!(old.handoff_calls().is_empty());
    assert!(rig.engines.fallbacks().is_empty());
}

/// Should-fix 3 of 37-k5a's review: the replacement served, but adopting it
/// failed (its readiness, the API server): it is never retired for that —
/// it holds the sessions — but adopted at a later pass.
#[tokio::test]
async fn an_adoption_error_after_the_cutover_never_retires_the_serving_replacement() {
    let rig = Rig::new();
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    rig.engines.set_desired("spec-2");
    rig.engines.fail_adoptions(4);
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(
            &rolled[..],
            [(_, Rolled::HandedOff(Outcome::Succeeded { .. }))]
        ),
        "{rolled:?}"
    );
    assert!(old.handed_off());
    // Not adopted yet, and nothing deleted: the replacement serves the view.
    let standby = rig.engines.standby(&rig.unit()).expect("still there");
    assert_eq!(standby.view_mountpoints(), [staging.display().to_string()]);
    // The next pass adopts it (one more failure, then a retry).
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::Adopted)]), "{rolled:?}");
    let new = rig.engines.control(&rig.unit()).unwrap();
    assert!(Arc::ptr_eq(&new, &standby));
    assert_eq!(rig.engines.spec_of(&rig.unit()).as_deref(), Some("spec-2"));
    assert!(rig.node.rollout_once().await.is_empty(), "converged");
    rig.unstage().await.unwrap();
    assert!(new.view_mountpoints().is_empty());
}

/// Must-fix 1 of 37-k5a's review, at the rollout: a replacement that
/// neither serves nor fails within the resume budget is left pending —
/// never deleted on a timeout — and adopted once it serves.
#[tokio::test]
async fn an_unresolved_handoff_stays_pending_until_its_replacement_serves() {
    let dir = tempfile::tempdir().unwrap();
    let mounter = Arc::new(FakeMounter::default());
    let engines = Arc::new(InMemoryNodeEngines::new("node-a"));
    let rig = Rig {
        node: Rig::service(&dir, &mounter, &engines).with_handoff(handoff::HandoffConfig {
            drain: Duration::from_millis(500),
            total: Duration::from_secs(5),
            resume: Duration::from_millis(300),
            max_attempts: 3,
        }),
        dir,
        mounter,
        engines,
    };
    let old = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    rig.engines.set_desired("spec-2");
    rig.engines
        .resume_next_standby_after(Duration::from_millis(1500));
    let rolled = rig.node.rollout_once().await;
    assert!(
        matches!(
            &rolled[..],
            [(_, Rolled::HandedOff(Outcome::Unresolved { .. }))]
        ),
        "{rolled:?}"
    );
    assert!(old.handed_off());
    let standby = rig.engines.standby(&rig.unit()).expect("left alone");
    assert!(!standby.handoff_calls().contains(&HandoffPhase::Abort));
    // Still not serving: pending, and no new handoff is started for it.
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::Pending)]), "{rolled:?}");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let rolled = rig.node.rollout_once().await;
    assert!(matches!(&rolled[..], [(_, Rolled::Adopted)]), "{rolled:?}");
    assert_eq!(standby.view_mountpoints(), [staging.display().to_string()]);
    let metrics = rig.node.handoff_metrics().render();
    assert!(metrics.contains("constellation_csi_handoff_total{outcome=\"unresolved\"} 1"));
}

/// Should-fix 4 of 37-k5a's review: publish, unpublish, stats and health
/// meet a staging mount whose session may be changing hands — they wait
/// for the unit's handoff (its gate) rather than stall on the mount.
#[tokio::test]
async fn publish_unpublish_and_stats_wait_for_a_running_handoff() {
    let rig = Rig::new();
    rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let first = rig.target(1);
    rig.publish(&first, Mode::MultiNodeMultiWriter)
        .await
        .unwrap();
    // One at a time: a second RPC on the volume would meet the first's
    // per-volume lock (`ABORTED`), not the gate.
    let pause = Duration::from_millis(300);
    let gate = || rig.node.unit_gate(&rig.unit()).write_owned();
    let second = rig.target(2);

    let held = gate().await;
    let publish = rig.publish(&second, Mode::MultiNodeMultiWriter);
    tokio::pin!(publish);
    assert!(
        tokio::time::timeout(pause, &mut publish).await.is_err(),
        "publish waits"
    );
    drop(held);
    publish.await.unwrap();

    let held = gate().await;
    let unpublish = rig.unpublish(&first);
    tokio::pin!(unpublish);
    assert!(
        tokio::time::timeout(pause, &mut unpublish).await.is_err(),
        "unpublish waits"
    );
    drop(held);
    unpublish.await.unwrap();

    let held = gate().await;
    let stats = rig
        .node
        .node_get_volume_stats(Request::new(NodeGetVolumeStatsRequest {
            volume_id: volume_id(),
            volume_path: rig.staging().display().to_string(),
            ..Default::default()
        }));
    tokio::pin!(stats);
    assert!(
        tokio::time::timeout(pause, &mut stats).await.is_err(),
        "stats wait"
    );
    drop(held);
    stats.await.unwrap();

    let held = gate().await;
    let health = rig.health();
    tokio::pin!(health);
    assert!(
        tokio::time::timeout(pause, &mut health).await.is_err(),
        "health waits"
    );
    drop(held);
    assert!(health.await.is_empty());
}

// ---- plan 37 §7: idle GC and drain (37-k6b, `gc`) ----

fn gc_cfg() -> gc::GcConfig {
    gc::GcConfig {
        interval: Duration::from_secs(1),
        idle_ttl: Some(Duration::from_secs(600)),
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// §7: a pod that has served nothing for longer than the TTL leaves the
/// registry and goes; before the TTL, or while a volume is staged, it
/// stays. Its next volume starts a new pod (a new node).
#[tokio::test]
async fn an_idle_engine_pod_leaves_the_registry_after_the_ttl() {
    let rig = Rig::new();
    let control = rig.plant().await;
    let staging = rig.staging();
    rig.stage(&staging).await.unwrap();
    let unit = rig.unit();
    // Staged: never a candidate, however late.
    let done = rig.node.gc_once(&gc_cfg(), now() + 100_000).await;
    assert!(done.is_empty(), "{done:?}");
    rig.unstage().await.unwrap();
    assert_eq!(rig.engines.view_count(&unit), Some(0));
    // Idle, but not for long enough.
    assert!(rig.node.gc_once(&gc_cfg(), now() + 10).await.is_empty());
    assert!(control.leaves().is_empty());
    // Past the TTL: it leaves (self-leave), then the unit is forgotten.
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert_eq!(done, vec![(unit.clone(), gc::Collected::Left)]);
    assert_eq!(control.leaves(), vec![None]);
    assert_eq!(rig.engines.forgotten(), vec![unit.clone()]);
    assert!(!rig.engines.is_running(&unit));
    // Nothing left to collect; the next stage starts a new pod.
    assert!(rig.node.gc_once(&gc_cfg(), now() + 10_000).await.is_empty());
    let started = rig.engines.started();
    rig.stage(&staging).await.unwrap();
    assert_eq!(rig.engines.started(), started + 1);
    // TTL 0: idle pods stay.
    rig.unstage().await.unwrap();
    let off = gc::GcConfig {
        idle_ttl: None,
        ..gc_cfg()
    };
    assert!(rig.node.gc_once(&off, now() + 100_000).await.is_empty());
}

/// The 37-k3b review: the annotation is never trusted alone. A pod whose
/// count says 0 but which serves a view (a lost patch) is kept.
#[tokio::test]
async fn an_idle_annotation_is_checked_against_view_list() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let unit = rig.unit();
    // The view count's patch was lost, and the record too (a restarted
    // plugin, say): only the engine knows it serves a view.
    rig.engines.set_view_count(&unit, 0).await.unwrap();
    rig.node.state.remove(&volume_id()).unwrap();
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert!(
        matches!(&done[..], [(u, gc::Collected::Kept(why))] if *u == unit && why.contains("view")),
        "{done:?}"
    );
    assert!(control.leaves().is_empty());
    assert!(rig.engines.is_running(&unit));
}

/// §7 "Drain": on a draining node an engine pod with no view left leaves
/// at once, whatever the TTL; one still serving a volume blocks until it is
/// unstaged.
#[tokio::test]
async fn a_draining_node_collects_its_engines_once_their_views_are_gone() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let unit = rig.unit();
    rig.engines.set_draining(true);
    assert!(rig.node.gc_once(&gc_cfg(), now()).await.is_empty());
    assert!(control.leaves().is_empty());
    // Merely cordoned, the plugin stopping is a rollout of it: its preStop
    // does not wait for the volume, which stays staged.
    gc::pre_stop(rig.engines.as_ref(), Duration::ZERO)
        .await
        .unwrap();
    // Going for good, the pre-stop hook waits while the pod is there.
    rig.engines.set_drain(Drain::Condemned);
    let waited = tokio::time::timeout(
        Duration::from_millis(300),
        gc::pre_stop(rig.engines.as_ref(), Duration::from_secs(60)),
    )
    .await;
    assert!(waited.is_err(), "preStop must wait for the engine pod");
    rig.unstage().await.unwrap();
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert_eq!(done, vec![(unit.clone(), gc::Collected::Left)]);
    assert_eq!(control.leaves(), vec![None]);
    gc::pre_stop(rig.engines.as_ref(), Duration::from_secs(1))
        .await
        .unwrap();
    // Not draining: preStop returns at once even with pods around.
    rig.engines.set_draining(false);
    rig.plant().await;
    gc::pre_stop(rig.engines.as_ref(), Duration::ZERO)
        .await
        .unwrap();
}

/// The identity moves aside before the leave, and back when the leave is
/// refused (the 37-k6b review: a stop between the leave and the cleanup
/// must not leave a retired identity in place).
#[tokio::test]
async fn the_identity_is_aside_through_the_leave_and_back_if_refused() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    rig.unstage().await.unwrap();
    let unit = rig.unit();
    control.refuse_next_leaves(1);
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert!(
        matches!(&done[..], [(u, gc::Collected::Kept(why))] if *u == unit && why.contains("node.leave")),
        "{done:?}"
    );
    assert!(rig.engines.aside().is_empty(), "restored after the refusal");
    assert!(rig.engines.is_running(&unit));
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert_eq!(done, vec![(unit.clone(), gc::Collected::Left)]);
    assert!(rig.engines.aside().is_empty());
    assert_eq!(rig.engines.forgotten(), vec![unit]);
}

/// A pass that ended with the identity aside is settled by the next: an
/// engine that never left gets it back; one that left is forgotten; with
/// no engine to ask, the unit is forgotten (a fresh node next).
#[tokio::test]
async fn an_identity_left_aside_is_settled_by_the_next_pass() {
    // Stopped between moving it aside and the leave.
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let unit = rig.unit();
    rig.engines.set_identity_aside(&unit).await.unwrap();
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert!(
        matches!(&done[..], [(u, gc::Collected::Kept(_))] if *u == unit),
        "{done:?}"
    );
    assert!(rig.engines.aside().is_empty(), "the identity is back");
    assert!(rig.engines.forgotten().is_empty());
    assert!(control.leaves().is_empty());

    // Stopped between the leave and the cleanup.
    rig.unstage().await.unwrap();
    rig.engines.set_identity_aside(&unit).await.unwrap();
    control
        .node_leave(constellation_control::proto::types::LeaveParams {
            node_id: None,
            force: false,
        })
        .await
        .unwrap();
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert_eq!(done, vec![(unit.clone(), gc::Collected::Left)]);
    assert_eq!(rig.engines.forgotten(), vec![unit.clone()]);
    assert!(rig.engines.aside().is_empty());

    // Aside with no engine at all to ask: forgotten.
    rig.engines.set_identity_aside("gone-unit").await.unwrap();
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert_eq!(done, vec![("gone-unit".to_string(), gc::Collected::Left)]);
    assert!(rig.engines.aside().is_empty());
}

/// An identity aside next to a fresh one (the plugin and the pod died
/// mid-collect, and a stage started a pod on a new identity before the
/// next pass) is an older incarnation's: retired once, the fresh one and
/// its engine untouched, and no pass "restores" it again.
#[tokio::test]
async fn an_identity_aside_superseded_by_a_fresh_one_is_retired() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    let unit = rig.unit();
    rig.engines.set_identity_aside(&unit).await.unwrap();
    rig.engines.make_fresh_identity(&unit);
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert!(
        matches!(&done[..], [(u, gc::Collected::Kept(why))] if *u == unit && why.contains("superseded")),
        "{done:?}"
    );
    assert!(rig.engines.aside().is_empty());
    assert_eq!(rig.engines.discarded(), vec![unit.clone()]);
    assert!(
        rig.engines.forgotten().is_empty(),
        "the fresh identity stays"
    );
    assert!(rig.engines.is_running(&unit));
    assert!(control.leaves().is_empty());
    // Settled for good: the next pass has nothing aside to look at.
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert!(done.is_empty(), "{done:?}");
    assert_eq!(rig.engines.discarded(), vec![unit]);
}

/// A pod still waiting for its credentials never joined the registry: it
/// is deleted without a leave, and its identity kept.
#[tokio::test]
async fn an_engine_waiting_for_credentials_is_deleted_without_a_leave() {
    let rig = Rig::new();
    let control = rig.plant().await;
    assert!(control.is_gated());
    let unit = rig.unit();
    rig.engines.set_draining(true);
    let done = rig.node.gc_once(&gc_cfg(), now()).await;
    assert_eq!(done, vec![(unit, gc::Collected::Deleted)]);
    assert!(control.leaves().is_empty());
    assert!(rig.engines.forgotten().is_empty());
}

/// A unit busy with a stage (its gate held) is not collected in that pass.
#[tokio::test]
async fn a_busy_unit_is_left_for_the_next_pass() {
    let rig = Rig::new();
    let control = rig.plant().await;
    rig.stage(&rig.staging()).await.unwrap();
    rig.unstage().await.unwrap();
    let unit = rig.unit();
    let held = rig.node.shared_gate(&unit).await;
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert!(
        matches!(&done[..], [(_, gc::Collected::Kept(why))] if why.contains("busy")),
        "{done:?}"
    );
    drop(held);
    let done = rig.node.gc_once(&gc_cfg(), now() + 601).await;
    assert_eq!(done, vec![(unit, gc::Collected::Left)]);
    assert_eq!(control.leaves(), vec![None]);
}

#[test]
fn gc_settings_come_from_the_environment() {
    let cfg = gc::GcConfig::from_vars(|k| match k {
        "CONSTELLATION_CSI_ENGINE_IDLE_TTL" => Some("45s".into()),
        "CONSTELLATION_CSI_IDLE_GC_INTERVAL_S" => Some("5".into()),
        _ => None,
    })
    .unwrap();
    assert_eq!(cfg.idle_ttl, Some(Duration::from_secs(45)));
    assert_eq!(cfg.interval, Duration::from_secs(5));
    let off =
        gc::GcConfig::from_vars(|k| (k == "CONSTELLATION_CSI_ENGINE_IDLE_TTL").then(|| "0".into()))
            .unwrap();
    assert_eq!(off.idle_ttl, None);
    assert_eq!(
        gc::GcConfig::from_vars(|_| None).unwrap(),
        gc::GcConfig::default()
    );
}
