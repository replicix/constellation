//! `node.ops` (plan 31 §6.10): the request watchdog's registry
//! (`constellation_vfs::OpWatch`), attributed to views; and the unified op
//! metrics as `status` carries them (`vfs_ops`, which `/metrics` renders as
//! `constellation_vfs_ops_total` and `constellation_vfs_op_seconds`).
//!
//! Every view registers its ops through a handle tagged with its id and
//! labels (`OpWatch::for_view`, set in `View::set_id`/`apply_spec`), so an
//! entry knows its view even though the registry is one per engine. The
//! report lists the operations in flight, oldest first (capped at
//! [`api::OPS_LIST_MAX`]; the counts never are), and summarises them per
//! view with the view's labels; every open view has a summary, idle ones
//! included, so an operator sees the view table and not only the busy
//! rows. `view` narrows both to one view (an unknown id is `NotFound`);
//! `min_age_s` keeps operations at least that old.

use super::*;
use api::{OpEntry, OpsParams, OpsReport, VfsOpSeries, VfsOpsStatus, ViewOps, OPS_LIST_MAX};
use constellation_vfs::watch::InFlightOp;
use std::time::Duration;

impl EngineControl {
    /// `node.ops`.
    pub(crate) fn node_ops(&self, p: &OpsParams) -> Result<OpsReport, ControlError> {
        let views = self.engine.views();
        if let Some(id) = p.view {
            if !views.iter().any(|v| v.id == id) {
                return Err(ControlError::not_found("no such view"));
            }
        }
        let watch = self.engine.op_watch();
        Ok(ops_report(&views, watch.ops(), watch.threshold(), p))
    }
}

/// The report for `ops` (the registry's, oldest first) with `views` open.
fn ops_report(
    views: &[crate::node::ViewInfo],
    ops: Vec<InFlightOp>,
    threshold: Duration,
    p: &OpsParams,
) -> OpsReport {
    let min_age = Duration::from_secs(p.min_age_s.unwrap_or(0));
    let ops: Vec<InFlightOp> = ops
        .into_iter()
        .filter(|op| op.age >= min_age)
        .filter(|op| {
            p.view
                .is_none_or(|id| op.view.as_ref().is_some_and(|v| v.id == id))
        })
        .collect();
    let mut report = OpsReport {
        stall_threshold_s: threshold.as_secs(),
        ..OpsReport::default()
    };
    // One summary per open view, then the ops of no view (an untagged
    // handle), if any.
    let mut summaries: Vec<ViewOps> = views
        .iter()
        .filter(|v| p.view.is_none_or(|id| id == v.id))
        .map(|v| ViewOps {
            view: Some(v.id),
            labels: v.labels.clone(),
            ..ViewOps::default()
        })
        .collect();
    for op in &ops {
        let id = op.view.as_ref().map(|v| v.id);
        let at = match summaries.iter().position(|s| s.view == id) {
            Some(at) => at,
            // A view that closed with an op still finishing, or an op of
            // no view: summarised under what its tag says.
            None => {
                summaries.push(ViewOps {
                    view: id,
                    labels: op
                        .view
                        .as_ref()
                        .map(|v| v.labels.clone())
                        .unwrap_or_default(),
                    ..ViewOps::default()
                });
                summaries.len() - 1
            }
        };
        let summary = &mut summaries[at];
        let age_s = op.age.as_secs();
        report.in_flight += 1;
        summary.in_flight += 1;
        if op.stalled {
            report.stalled += 1;
            summary.stalled += 1;
        }
        if !op.blocking {
            report.oldest_s = report.oldest_s.max(age_s);
            summary.oldest_s = summary.oldest_s.max(age_s);
        }
    }
    report.truncated = ops.len() > OPS_LIST_MAX;
    report.ops = ops
        .iter()
        .take(OPS_LIST_MAX)
        .map(|op| OpEntry {
            op: op.op.to_string(),
            ino: op.ino,
            age_s: op.age.as_secs(),
            stage: op.stage.to_string(),
            tid: op.tid as u32,
            blocking: op.blocking,
            stalled: op.stalled,
            view: op.view.as_ref().map(|v| v.id),
        })
        .collect();
    report.views = summaries;
    report
}

/// The unified op metrics as `status` reports them
/// (`constellation_vfs::metrics::snapshot`).
pub(crate) fn vfs_ops_status() -> VfsOpsStatus {
    use constellation_vfs::metrics::{snapshot, BUCKET_BOUNDS_NS};
    VfsOpsStatus {
        bucket_bounds_s: BUCKET_BOUNDS_NS.iter().map(|ns| *ns as f64 / 1e9).collect(),
        series: snapshot()
            .into_iter()
            .map(|s| VfsOpSeries {
                frontend: s.frontend.to_string(),
                view: s.view,
                op: s.op.to_string(),
                outcomes: s
                    .outcomes
                    .into_iter()
                    .map(|(outcome, n)| (outcome.to_string(), n))
                    .collect(),
                buckets: s.buckets,
                sum_ns: s.sum_ns,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::ViewInfo as EngineViewInfo;
    use crate::{DeferredEvents, EngineConfig, EngineProfile, P2pMode, ViewSpec};
    use constellation_control::{dispatch_in_process, web};
    use constellation_platform::HostServices;
    use constellation_store_s3::{ChunkStore, FsMeta};
    use constellation_types::Code;
    use constellation_vfs::watch::ViewTag;
    use constellation_vfs::{Blocking, Caller, Name, Observer, OpKind, Vfs, ROOT_INO};
    use serde_json::json;
    use tower::ServiceExt;

    /// A host whose views are the ones the test hands it.
    struct Host(Mutex<Vec<HostView>>);

    impl ControlHost for Host {
        fn views(&self) -> Vec<HostView> {
            self.0.lock().unwrap().clone()
        }
        fn mount(&self, _: &ViewMountParams, _: Option<OwnedFd>) -> Result<ViewInfo, ControlError> {
            Err(ControlError::unsupported("the fixture mounts nothing"))
        }
        fn unmount(&self, _: &Path) -> Result<String, ControlError> {
            Err(ControlError::unsupported("the fixture mounts nothing"))
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
            Err(ControlError::unsupported("the fixture cannot hand over"))
        }
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        rt: tokio::runtime::Runtime,
        svc: Arc<EngineControl>,
        views: Vec<Arc<View>>,
    }

    /// An offline engine with one view per label set.
    fn fixture(labels: &[&[(&str, &str)]]) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let backend = format!("file://{}", dir.path().join("backend").display());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let store = ChunkStore::new(rt.block_on(crate::backend::open_backend(&backend)).unwrap());
        rt.block_on(store.create_fs(&FsMeta::new(1024 * 1024, "raw")))
            .unwrap();
        let engine = Arc::new(
            Engine::start(
                EngineConfig {
                    state_dir: Some(dir.path().join("state")),
                    cache_size: 16 * 1024 * 1024,
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
        let mut hosted = Vec::new();
        let mut views = Vec::new();
        for (n, set) in labels.iter().enumerate() {
            let labels: BTreeMap<String, String> = set
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let view = engine
                .open_view(
                    ViewSpec {
                        labels: labels.clone(),
                        ..ViewSpec::new("/")
                    },
                    constellation_vfs::FrontendCaps::linux_fuse(false),
                    DeferredEvents::new(),
                )
                .unwrap();
            hosted.push(HostView {
                id: view.id(),
                subtree: "/".into(),
                mountpoint: format!("/ops-test/{n}").into(),
                since: Instant::now(),
                labels,
                qos: Default::default(),
                confine_links: false,
                // No FUSE session in this fixture.
                transport: None,
                view: Some(view.clone()),
            });
            views.push(view);
        }
        let prefetch = views[0].prefetch_stats();
        let svc = EngineControl::new(
            engine,
            Arc::new(Host(Mutex::new(hosted))),
            crate::log_buffer::LogBuffer::default(),
            prefetch,
            "ops-test",
        );
        Fixture {
            _dir: dir,
            rt,
            svc,
            views,
        }
    }

    #[test]
    fn node_ops_lists_the_ops_in_flight_per_view_with_labels() {
        let f = fixture(&[&[("pv", "pv-a"), ("namespace", "team-a")], &[]]);
        let (a, b) = (&f.views[0], &f.views[1]);
        let _read = a.op_watch().enter("read", 7);
        let _lock = b.op_watch().enter_blocking("setlk", 9);
        _read.stage("chunk fetch: S3");

        let all = f.svc.node_ops(&OpsParams::default()).unwrap();
        assert_eq!((all.in_flight, all.stalled, all.ops.len()), (2, 0, 2));
        assert!(!all.truncated);
        assert_eq!(all.views.len(), 2, "one summary per open view");
        let of = |report: &OpsReport, id: u64| {
            report
                .views
                .iter()
                .find(|v| v.view == Some(id))
                .unwrap()
                .clone()
        };
        let sa = of(&all, a.id());
        assert_eq!((sa.in_flight, sa.stalled), (1, 0));
        assert_eq!(sa.labels.get("pv").map(String::as_str), Some("pv-a"));
        assert_eq!(
            sa.labels.get("namespace").map(String::as_str),
            Some("team-a")
        );
        assert_eq!(of(&all, b.id()).in_flight, 1);
        let read = all.ops.iter().find(|o| o.op == "read").unwrap();
        assert_eq!(
            (read.ino, read.stage.as_str(), read.view, read.blocking),
            (7, "chunk fetch: S3", Some(a.id()), false)
        );
        assert!(all.ops.iter().find(|o| o.op == "setlk").unwrap().blocking);

        // The view filter: that view's ops and summary only.
        let only_b = f
            .svc
            .node_ops(&OpsParams {
                view: Some(b.id()),
                ..OpsParams::default()
            })
            .unwrap();
        assert_eq!(
            (only_b.in_flight, only_b.ops.len(), only_b.views.len()),
            (1, 1, 1)
        );
        assert_eq!(only_b.ops[0].op, "setlk");
        assert_eq!(only_b.views[0].view, Some(b.id()));
        // An idle view still has its (empty) row.
        drop(_read);
        let idle_a = f
            .svc
            .node_ops(&OpsParams {
                view: Some(a.id()),
                ..OpsParams::default()
            })
            .unwrap();
        assert_eq!(
            (idle_a.in_flight, idle_a.ops.len(), idle_a.views.len()),
            (0, 0, 1)
        );
        // An age no op has reached, and a view that does not exist.
        let none = f
            .svc
            .node_ops(&OpsParams {
                min_age_s: Some(3600),
                ..OpsParams::default()
            })
            .unwrap();
        assert_eq!((none.in_flight, none.ops.len()), (0, 0));
        let unknown = f
            .svc
            .node_ops(&OpsParams {
                view: Some(u64::MAX),
                ..OpsParams::default()
            })
            .unwrap_err();
        assert_eq!(
            unknown.kind,
            constellation_control::proto::ErrorKind::NotFound
        );

        // Over the control protocol, as an operator's client calls it.
        let router = router(&f.svc);
        let value =
            f.rt.block_on(dispatch_in_process(
                &router,
                &Principal::InProcess,
                "node.ops",
                json!({"view": a.id()}),
            ))
            .unwrap();
        assert_eq!(value["in_flight"], 0);
        assert_eq!(value["views"][0]["view"], a.id());
        assert_eq!(value["views"][0]["labels"]["pv"], "pv-a");
        assert_eq!(value["stall_threshold_s"], 30);
        let refused =
            f.rt.block_on(dispatch_in_process(
                &router,
                &Principal::InProcess,
                "node.ops",
                json!({"view": 424242}),
            ))
            .unwrap_err();
        assert_eq!(
            refused.kind,
            constellation_control::proto::ErrorKind::NotFound
        );
    }

    #[test]
    fn stalled_ops_are_counted_per_view_and_the_listing_is_capped() {
        let tag = |id| {
            Some(Arc::new(ViewTag {
                id,
                labels: [("pv".to_string(), format!("pv-{id}"))].into(),
            }))
        };
        let op = |name: &'static str, age: u64, stalled, view| InFlightOp {
            op: name,
            ino: 1,
            age: Duration::from_secs(age),
            stage: "meta read",
            tid: 1,
            blocking: false,
            stalled,
            view,
        };
        let views = [EngineViewInfo {
            id: 1,
            root: "/".into(),
            labels: [("pv".to_string(), "pv-1".to_string())].into(),
            since: Instant::now(),
        }];
        let ops = vec![
            op("read", 90, true, tag(1)),
            op("lookup", 40, true, tag(1)),
            op("getattr", 2, false, tag(1)),
            // A view that is gone, and an op of no view.
            op("write", 5, false, tag(9)),
            op("statfs", 1, false, None),
        ];
        let r = ops_report(
            &views,
            ops.clone(),
            Duration::from_secs(30),
            &OpsParams::default(),
        );
        assert_eq!(
            (r.in_flight, r.stalled, r.oldest_s, r.stall_threshold_s),
            (5, 2, 90, 30)
        );
        let by_view = |id| r.views.iter().find(|v| v.view == id).unwrap().clone();
        let one = by_view(Some(1));
        assert_eq!((one.in_flight, one.stalled, one.oldest_s), (3, 2, 90));
        assert_eq!(one.labels["pv"], "pv-1");
        assert_eq!(
            by_view(Some(9)).labels["pv"],
            "pv-9",
            "a closed view keeps its tag's labels"
        );
        assert_eq!(by_view(None).in_flight, 1);
        let old = ops_report(
            &views,
            ops.clone(),
            Duration::from_secs(30),
            &OpsParams {
                min_age_s: Some(30),
                ..OpsParams::default()
            },
        );
        assert_eq!(
            old.ops.iter().map(|o| o.op.as_str()).collect::<Vec<_>>(),
            ["read", "lookup"]
        );
        assert!(old.ops.iter().all(|o| o.stalled));
        let many: Vec<InFlightOp> = (0..OPS_LIST_MAX + 5)
            .map(|_| op("read", 1, false, tag(1)))
            .collect();
        let capped = ops_report(&views, many, Duration::from_secs(30), &OpsParams::default());
        assert_eq!(
            capped.in_flight as usize,
            OPS_LIST_MAX + 5,
            "counts are never capped"
        );
        assert_eq!((capped.ops.len(), capped.truncated), (OPS_LIST_MAX, true));
    }

    /// The DoD of plan 31 C7: the metrics named in §6.10 are emitted per
    /// op and scraped from `/metrics` with the right labels and counts —
    /// the view label is the allowlisted `pv`, never the label map.
    #[test]
    fn metrics_are_scraped_with_their_labels_and_counts() {
        let f = fixture(&[&[
            ("pv", "pv-9"),
            ("namespace", "team-a"),
            ("pvc", "claim-7f3a"),
        ]]);
        let view = &f.views[0];
        let identity = view.identity();
        assert_eq!(identity.metric_view.as_deref(), Some("pv-9"));
        // A frontend of this test's own, so the process-wide registry other
        // tests share cannot add to these counts.
        let obs = Observer::new("scrape-test", &identity);
        let caller = Caller::root();
        for _ in 0..3 {
            let op = obs.begin(OpKind::Getattr, ROOT_INO);
            let _in = op.enter();
            let attr =
                Blocking::run(|r| view.getattr(&op.ctx(&caller), ROOT_INO, None, op.responder(r)));
            assert!(attr.is_ok());
        }
        for _ in 0..2 {
            let op = obs.begin(OpKind::Lookup, ROOT_INO);
            let _in = op.enter();
            let entry = Blocking::run(|r| {
                view.lookup(
                    &op.ctx(&caller),
                    ROOT_INO,
                    Name::new(b"absent"),
                    op.responder(r),
                )
            });
            assert_eq!(entry.unwrap_err().code(), Code::NotFound);
        }

        let app = web::app(Arc::new(router(&f.svc)));
        let body = f.rt.block_on(async {
            let request = axum::http::Request::builder()
                .uri("/metrics")
                .header("host", "127.0.0.1")
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), 200);
            assert!(response.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/plain; version=0.0.4"));
            let bytes = axum::body::to_bytes(response.into_body(), 64 << 20)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        });
        let labels = r#"frontend="scrape-test",view="pv-9""#;
        for line in [
            "# TYPE constellation_vfs_ops_total counter".to_string(),
            "# TYPE constellation_vfs_op_seconds histogram".to_string(),
            format!(r#"constellation_vfs_ops_total{{{labels},op="getattr",outcome="ok"}} 3"#),
            format!(r#"constellation_vfs_ops_total{{{labels},op="lookup",outcome="NotFound"}} 2"#),
            format!(r#"constellation_vfs_op_seconds_count{{{labels},op="getattr"}} 3"#),
            format!(r#"constellation_vfs_op_seconds_bucket{{{labels},op="getattr",le="+Inf"}} 3"#),
            format!(r#"constellation_vfs_op_seconds_count{{{labels},op="lookup"}} 2"#),
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
        // Cumulative, monotone buckets ending at the count.
        let cumulative: Vec<u64> = body
            .lines()
            .filter(|l| {
                l.starts_with("constellation_vfs_op_seconds_bucket{")
                    && l.contains(labels)
                    && l.contains(r#"op="getattr""#)
            })
            .map(|l| l.rsplit(' ').next().unwrap().parse().unwrap())
            .collect();
        assert_eq!(cumulative.len(), 22, "21 bounds and +Inf");
        assert!(cumulative.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(*cumulative.last().unwrap(), 3);
        // Bounded cardinality: the other labels never reach a series.
        assert!(!body.contains("team-a") && !body.contains("claim-7f3a"));
        // The same numbers ride `stats.subscribe`'s samples, as totals.
        let sample = streams::sample_of(&f.svc.status());
        assert!(sample.counters["constellation_vfs_ops_total"] >= 5);
        assert!(sample.counters["constellation_vfs_ops_refused_total"] >= 2);
    }

    /// The chunk memory cache reports in `node.status`'s cache section,
    /// on `/metrics`, and in `stats.subscribe`'s samples.
    #[test]
    fn the_chunk_memory_cache_is_reported() {
        use constellation_fs_core::cache::ChunkState;
        use constellation_fs_core::ChunkHash;
        let f = fixture(&[&[]]);
        let data = vec![0x5a; 64 * 1024];
        let hash = ChunkHash::of(&data);
        f.svc.cache.insert(&hash, &data, ChunkState::Clean).unwrap();
        for _ in 0..3 {
            let got = f.svc.cache.get_shared(&hash).unwrap().unwrap();
            assert_eq!(got.len(), data.len());
        }
        let cache = f.svc.status().cache;
        // The engine's 16 MiB disk cache bounds the default budget.
        assert_eq!(cache.memory_budget_bytes, 16 * 1024 * 1024);
        assert_eq!(
            (cache.memory_hits, cache.memory_misses, cache.memory_chunks),
            (2, 1, 1)
        );
        assert_eq!(cache.memory_used_bytes, 64 * 1024);

        let app = web::app(Arc::new(router(&f.svc)));
        let body = f.rt.block_on(async {
            let request = axum::http::Request::builder()
                .uri("/metrics")
                .header("host", "127.0.0.1")
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), 64 << 20)
                .await
                .unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        });
        for line in [
            "constellation_cache_memory_budget_bytes 16777216",
            "constellation_cache_memory_used_bytes 65536",
            "constellation_cache_memory_chunks 1",
            "constellation_cache_memory_hits_total 2",
            "constellation_cache_memory_misses_total 1",
            "constellation_cache_memory_evictions_total 0",
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
        let sample = streams::sample_of(&f.svc.status());
        assert_eq!(sample.counters["constellation_cache_memory_hits_total"], 2);
        assert_eq!(
            sample.gauges["constellation_cache_memory_used_bytes"],
            65536.0
        );
    }
}
