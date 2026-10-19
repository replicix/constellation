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
                transport: s.transport.to_string(),
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
pub(super) mod tests {
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

    /// A host whose views are the ones the test hands it, and whose
    /// process-wide FUSE counters are the test's too.
    struct Host(Mutex<Vec<HostView>>, Mutex<api::FuseStatus>);

    impl ControlHost for Host {
        fn views(&self) -> Vec<HostView> {
            self.0.lock().unwrap().clone()
        }
        fn fuse_counters(&self) -> api::FuseStatus {
            self.1.lock().unwrap().clone()
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
            _: Option<&constellation_control::authz::ServiceMatch>,
        ) -> Result<HandoffReport, ControlError> {
            Err(ControlError::unsupported("the fixture cannot hand over"))
        }
    }

    /// Also the fixture of `super::snapshot_tests`.
    pub(in crate::control) struct Fixture {
        _dir: tempfile::TempDir,
        pub(in crate::control) rt: tokio::runtime::Runtime,
        pub(in crate::control) svc: Arc<EngineControl>,
        host: Arc<Host>,
        views: Vec<Arc<View>>,
    }

    /// An offline engine with one view per label set.
    pub(in crate::control) fn fixture(labels: &[&[(&str, &str)]]) -> Fixture {
        fixture_with(labels, None)
    }

    /// [`fixture`] with the space-accounting service's knobs (`None`: the
    /// environment's).
    pub(in crate::control) fn fixture_with(
        labels: &[&[(&str, &str)]],
        snapacct: Option<crate::snapacct::SnapAcctConfig>,
    ) -> Fixture {
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
                    snapacct,
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
                fuse: None,
                view: Some(view.clone()),
            });
            views.push(view);
        }
        let prefetch = views[0].prefetch_stats();
        let host = Arc::new(Host(Mutex::new(hosted), Mutex::default()));
        let svc = EngineControl::new(
            engine,
            host.clone(),
            crate::log_buffer::LogBuffer::default(),
            prefetch,
            "ops-test",
        );
        Fixture {
            _dir: dir,
            rt,
            svc,
            host,
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
        let obs = Observer::new("scrape-test", &identity, "dev_fuse");
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
            format!(
                r#"constellation_vfs_ops_total{{{labels},op="getattr",transport="dev_fuse",outcome="ok"}} 3"#
            ),
            format!(
                r#"constellation_vfs_ops_total{{{labels},op="lookup",transport="dev_fuse",outcome="NotFound"}} 2"#
            ),
            format!(
                r#"constellation_vfs_op_seconds_count{{{labels},op="getattr",transport="dev_fuse"}} 3"#
            ),
            format!(
                r#"constellation_vfs_op_seconds_bucket{{{labels},op="getattr",transport="dev_fuse",le="+Inf"}} 3"#
            ),
            format!(
                r#"constellation_vfs_op_seconds_count{{{labels},op="lookup",transport="dev_fuse"}} 2"#
            ),
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
        // Plan 44: the log writer's counters.
        for line in [
            "# TYPE constellation_log_dropped_lines_total counter",
            "constellation_log_dropped_lines_total 0",
            "# TYPE constellation_log_queued_bytes gauge",
            "constellation_log_queued_bytes 0",
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

    /// Plan 44: `status` carries the log writer's counters, and
    /// `node.logs.flush`'s barrier waits for the lines logged before it.
    #[test]
    fn status_reports_the_log_writer_and_flush_waits_for_it() {
        use crate::log_buffer::QueueConfig;
        use std::io::Write;
        use tracing_subscriber::fmt::MakeWriter;
        struct Gate(
            Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
            Arc<std::sync::Mutex<Vec<u8>>>,
        );
        impl Write for Gate {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                // Bounded: a failing test must not hang the test binary.
                let (open, timeout) = self
                    .0
                     .1
                    .wait_timeout_while(
                        self.0 .0.lock().unwrap(),
                        std::time::Duration::from_secs(30),
                        |open| !*open,
                    )
                    .unwrap();
                if timeout.timed_out() && !*open {
                    return Err(std::io::ErrorKind::TimedOut.into());
                }
                drop(open);
                self.1.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let f = fixture(&[&[]]);
        assert_eq!(f.svc.status().log.mode, "sync");
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let got = Arc::new(std::sync::Mutex::new(Vec::new()));
        f.svc
            .log_buffer
            .start(
                QueueConfig::with_limit(1 << 20),
                Box::new(Gate(gate.clone(), got.clone())),
            )
            .unwrap();
        for i in 0..3 {
            f.svc
                .log_buffer
                .make_writer()
                .write_all(format!("line {i}\n").as_bytes())
                .unwrap();
        }
        // The sink is stalled: status shows the queue, the flush gives up
        // at its bound and says how much is left.
        let log = f.svc.status().log;
        assert_eq!((log.mode.as_str(), log.limit_bytes), ("queue", 1 << 20));
        assert_eq!(
            (log.queued_lines, log.written_lines, log.dropped_lines),
            (3, 0, 0)
        );
        assert!(log.queued_bytes > 0);
        let flush = f.svc.logs_flush(50);
        assert!(!flush.complete);
        assert_eq!(flush.unwritten_lines, 3);
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        let flush = f.svc.logs_flush(5_000);
        assert!(flush.complete, "{flush:?}");
        assert_eq!(f.svc.status().log.written_lines, 3);
        assert_eq!(got.lock().unwrap().as_slice(), b"line 0\nline 1\nline 2\n");
        // Lines the previous image lost at its exec are dropped lines here.
        f.svc.log_buffer.note_exec_loss(5);
        let log = f.svc.status().log;
        assert_eq!(
            (
                log.dropped_lines,
                log.drop_episodes,
                log.unwritten_at_previous_exec
            ),
            (5, 1, 5)
        );
        // A bound above the cap is capped, not an error.
        assert!(f.svc.logs_flush(u64::MAX).complete);
    }

    /// Plan 31 C7a: the control protocol's file browser is a frontend of
    /// the view like the FUSE session, so its ops are counted and timed
    /// the same way — `frontend="control"` in `/metrics` and in
    /// `node.status`'s `vfs_ops` — not only seen by the watchdog. Each
    /// browse call's ops are the ones the view ran for it, by outcome.
    #[test]
    fn browse_ops_are_counted_like_a_mounts() {
        use crate::control::browse::{ControlVfs, FRONTEND};
        let f = fixture(&[&[("pv", "pv-browse")]]);
        let view = f.views[0].clone();
        let browser = ControlVfs::new(view, Caller::root());
        browser.mkdir("/d", None, false).unwrap();
        browser.stat("/d").unwrap();
        assert_eq!(
            browser.stat("/absent").unwrap_err().code,
            Some(Code::NotFound)
        );
        browser
            .write("/d/f", 0, b"hello", true, None, false)
            .unwrap();
        let mut got = Vec::new();
        browser
            .read("/d/f", 0, None, |bytes| {
                got.extend_from_slice(&bytes);
                true
            })
            .unwrap();
        assert_eq!(got, b"hello");

        // Per call: `mkdir` looks `/d` up (absent); `stat /d` looks it up
        // and gets its attributes; `stat /absent` misses; `write` looks
        // up `/d` and `f` (absent), creates, writes, fsyncs, flushes and
        // releases, then gets the attributes; `read` looks up both,
        // opens, reads the data and the end of the file, and releases.
        let status = f.svc.status();
        let series: std::collections::BTreeMap<String, std::collections::BTreeMap<String, u64>> =
            status
                .vfs_ops
                .series
                .iter()
                .filter(|s| s.frontend == FRONTEND && s.view.as_deref() == Some("pv-browse"))
                .map(|s| (s.op.clone(), s.outcomes.clone()))
                .collect();
        let count = |op: &str, outcome: &str| {
            series
                .get(op)
                .and_then(|o| o.get(outcome))
                .copied()
                .unwrap_or(0)
        };
        assert_eq!(count("mkdir", "ok"), 1, "{series:?}");
        assert_eq!(count("lookup", "NotFound"), 3, "{series:?}");
        assert_eq!(count("lookup", "ok"), 4, "{series:?}");
        assert_eq!(count("create", "ok"), 1, "{series:?}");
        assert_eq!(count("open", "ok"), 1, "{series:?}");
        assert_eq!(count("getattr", "ok"), 2, "{series:?}");
        assert_eq!(count("flush", "ok"), 1, "{series:?}");
        assert_eq!(count("write", "ok"), 1, "{series:?}");
        assert_eq!(count("fsync", "ok"), 1, "{series:?}");
        assert_eq!(
            count("read", "ok"),
            2,
            "the data, then end of file: {series:?}"
        );
        assert_eq!(count("release", "ok"), 2, "{series:?}");

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
        let labels = r#"frontend="control",view="pv-browse""#;
        for line in [
            format!(
                r#"constellation_vfs_ops_total{{{labels},op="mkdir",transport="n/a",outcome="ok"}} 1"#
            ),
            format!(
                r#"constellation_vfs_ops_total{{{labels},op="lookup",transport="n/a",outcome="NotFound"}} 3"#
            ),
            format!(
                r#"constellation_vfs_op_seconds_count{{{labels},op="lookup",transport="n/a"}} 7"#
            ),
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
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

    /// Plan 38 §5: the host's per-session FUSE state reaches
    /// `node.status.fuse` per mount — its passthrough section included
    /// (plan 38 Z3b: the session's is the authoritative one), the view's
    /// own filled in where the host has none; the host's process-wide fallback
    /// counter reaches `/metrics` and `stats.subscribe`; and the mount's
    /// `MountInfo::transport` is the same value.
    #[test]
    fn the_fuse_section_reports_each_mount_and_the_fallbacks() {
        let f = fixture(&[&[], &[]]);
        let fallback = api::FuseFallback {
            from: "uring".into(),
            to: "dev_fuse".into(),
            reason: "kernel_not_offered".into(),
            detail: "fuse.enable_uring is N".into(),
            at_unix_ms: 1_700_000_000_000,
        };
        let host = &f.host;
        {
            let mut views = host.0.lock().unwrap();
            // The first view has a FUSE session that fell back; the second
            // has none (a control-only view) and is not in the section.
            views[0].fuse = Some(api::FuseMountStatus {
                transport: "dev_fuse".into(),
                last_fallback: Some(fallback.clone()),
                // The id is overwritten by the engine; the session's
                // passthrough section is kept.
                id: 999,
                passthrough: api::FusePassthroughStatus {
                    enabled: true,
                    opens: 42,
                    unavailable_reason: None,
                    refused_opens: 3,
                    opens_total: 9,
                },
                ..Default::default()
            });
        }
        *host.1.lock().unwrap() = api::FuseStatus {
            transport_fallbacks: vec![api::FuseFallbackCount {
                from: "uring".into(),
                to: "dev_fuse".into(),
                reason: "kernel_not_offered".into(),
                count: 1,
            }],
            ..Default::default()
        };

        let status = f.svc.status();
        let id = f.views[0].id();
        assert_eq!(status.fuse.mounts.len(), 1, "{:?}", status.fuse);
        let mount = &status.fuse.mounts[0];
        assert_eq!((mount.id, mount.mountpoint.as_str()), (id, "/ops-test/0"));
        assert_eq!(mount.last_fallback.as_ref(), Some(&fallback));
        assert_eq!(
            mount.passthrough,
            api::FusePassthroughStatus {
                enabled: true,
                opens: 42,
                unavailable_reason: None,
                refused_opens: 3,
                opens_total: 9,
            }
        );
        let info = status.mounts.iter().find(|m| m.id == id).unwrap();
        assert_eq!(info.transport.as_deref(), Some("dev_fuse"));
        // Round-trips as the schema describes it.
        let json = serde_json::to_value(&status.fuse).unwrap();
        assert_eq!(
            json["mounts"][0]["last_fallback"]["reason"],
            "kernel_not_offered"
        );
        let back: api::FuseStatus = serde_json::from_value(json).unwrap();
        assert_eq!(back, status.fuse);

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
            r#"constellation_fuse_transport_fallbacks_total{from="uring",to="dev_fuse",reason="kernel_not_offered"} 1"#,
            "constellation_fuse_zero_copy_reads_total 0",
            r#"constellation_fuse_passthrough_opens{mountpoint="/ops-test/0"} 42"#,
            r#"constellation_fuse_uring_queue_depth{mountpoint="/ops-test/0",transport="dev_fuse"} 0"#,
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
        let sample = streams::sample_of(&status);
        assert_eq!(
            sample.counters["constellation_fuse_transport_fallbacks_total"],
            1
        );
        assert_eq!(
            sample.counters["constellation_fuse_zero_copy_reads_total"],
            0
        );
        assert_eq!(sample.gauges["constellation_fuse_passthrough_opens"], 42.0);
        assert_eq!(sample.gauges["constellation_fuse_uring_queue_depth"], 0.0);

        // A host with no passthrough answer of its own gets the view's:
        // `linux_fuse` declares none until `FUSE_INIT` agrees it.
        host.0.lock().unwrap()[0].fuse.as_mut().unwrap().passthrough = Default::default();
        let mount = f.svc.status().fuse.mounts[0].passthrough.clone();
        assert!(!mount.enabled);
        assert_eq!(mount.opens, 0);
        assert_eq!(mount.unavailable_reason.as_deref(), Some("frontend"));
    }

    /// The writeback status's `dirty_bytes` counts what write sessions
    /// stage (`StagingBudget::active`), not the released slots they keep
    /// for reuse, which are free space; `cache.staging_bytes` (disk use)
    /// counts those too.
    #[test]
    fn writeback_dirty_bytes_leave_kept_staging_slots_out() {
        let f = fixture(&[&[]]);
        let budget = f.svc.staging_budget.clone();
        let dir = tempfile::tempdir().unwrap();
        let mut staging = crate::staging::Staging::create(
            dir.path(),
            5,
            0,
            4096,
            budget.clone(),
            HostServices::native().fs,
        )
        .unwrap();
        staging.write_at(0, &[1u8; 8192]).unwrap();
        staging.mark_dirty(0);
        staging.release_chunk(0);
        assert_eq!((budget.used(), budget.active()), (8192, 4096));
        let status = f.svc.status();
        assert_eq!(status.cache.staging_bytes, 8192);
        assert_eq!(
            status.writeback.dirty_bytes,
            f.svc.cache.dirty_bytes() + 4096,
            "a kept slot is not dirty"
        );
        staging.discard();
    }
}
