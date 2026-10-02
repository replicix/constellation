//! What a frontend does around every op to make it observable (plan 31
//! §6.10): an [`OpId`], a `vfs.op` tracing span entered for the engine
//! call, and the op's count and latency ([`crate::metrics`]) recorded
//! when its responder completes.
//!
//! ```ignore
//! let op = observer.begin(OpKind::Getattr, ino);   // id, span, clock
//! let _in_span = op.enter();                        // engine events nest under it
//! vfs.getattr(&op.ctx(&caller), ino, fh, op.responder(reply));
//! ```
//!
//! An [`Observer`] is built once per frontend session from the [`Vfs`]'s
//! [`ViewIdentity`] (`Vfs::identity`); the frontend's name is a constant
//! of that session. Everything per op is allocation-free:
//!
//! - the id is one relaxed atomic increment;
//! - the span is a `tracing` **debug**-level span. With no subscriber, or
//!   one that does not enable debug for this module, `debug_span!` is a
//!   static level check that returns the empty span without evaluating
//!   its fields; with one that does, the subscriber pays for what it
//!   records, which is why it is not `info` (an `fmt` subscriber at
//!   `info` would format eight fields for every FUSE request). Enable it
//!   with `RUST_LOG=constellation_vfs::observe=debug`;
//! - the responder wrapper is an `Arc` clone and an `Instant`.
//!
//! **Labels.** The span carries the view's numeric id and its `view`
//! label, the allowlisted metric label (`ViewSpec::metric_labels`, plan
//! 31 §9.10) — the same value the metrics carry — and the session's
//! `transport` (plan 38 §5: `dev_fuse`/`uring`/`uring_zc` for FUSE,
//! [`NO_TRANSPORT`](crate::metrics::NO_TRANSPORT) elsewhere), so a trace shows which kernel path served
//! a read without cross-referencing the metrics. The view's full label
//! map is logged once, when the view opens, and is visible in
//! `view.list` and `node.ops`; it is not repeated on every op.

use crate::ctx::{Caller, OpCtx, OpId, OpKind};
use crate::metrics::{OpMetrics, Timed};
use crate::types::Ino;
use std::sync::Arc;
use std::time::Instant;

/// Who a [`crate::Vfs`] is, for metrics and tracing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewIdentity {
    /// The engine's number for the view (0: none).
    pub id: u64,
    /// The view's allowlisted metric label, rendered (`pv-1` for
    /// `{pv: pv-1}`); `None` for a view with no allowlisted label.
    /// Never the full label map (plan 31 §9.10).
    pub metric_view: Option<String>,
}

/// One frontend session's observability, built once (module doc).
#[derive(Clone)]
pub struct Observer {
    frontend: &'static str,
    transport: &'static str,
    view_id: u64,
    view: Option<Arc<str>>,
    metrics: Arc<OpMetrics>,
}

impl Observer {
    /// The observer of `frontend` serving `identity` over `transport`
    /// ([`NO_TRANSPORT`](crate::metrics::NO_TRANSPORT) for a frontend without one), counting into the
    /// process-wide metrics a scrape sees ([`crate::metrics::snapshot`]).
    pub fn new(frontend: &'static str, identity: &ViewIdentity, transport: &'static str) -> Self {
        Self::with_metrics(
            frontend,
            identity,
            OpMetrics::for_view(frontend, identity.metric_view.as_deref(), transport),
        )
    }

    /// As [`Self::new`], counting into `metrics` (a benchmark's own); the
    /// span's `transport` is the one `metrics` is labelled with.
    pub fn with_metrics(
        frontend: &'static str,
        identity: &ViewIdentity,
        metrics: Arc<OpMetrics>,
    ) -> Self {
        Self {
            frontend,
            transport: metrics.transport(),
            view_id: identity.id,
            view: identity.metric_view.as_deref().map(Arc::from),
            metrics,
        }
    }

    pub fn metrics(&self) -> &Arc<OpMetrics> {
        &self.metrics
    }

    /// Begin an op of `kind` addressed to inode `ino`.
    #[inline]
    pub fn begin(&self, kind: OpKind, ino: Ino) -> ObservedOp<'_> {
        let started = Instant::now();
        let id = OpId::next();
        let span = tracing::debug_span!(
            "vfs.op",
            op = kind.name(),
            ino,
            op_id = id.0,
            frontend = self.frontend,
            transport = self.transport,
            view_id = self.view_id,
            view = self.view.as_deref(),
        );
        ObservedOp {
            observer: self,
            id,
            kind,
            span,
            started,
        }
    }
}

/// One op begun by an [`Observer`].
pub struct ObservedOp<'o> {
    observer: &'o Observer,
    id: OpId,
    kind: OpKind,
    span: tracing::Span,
    started: Instant,
}

impl<'o> ObservedOp<'o> {
    pub fn id(&self) -> OpId {
        self.id
    }

    pub fn span(&self) -> &tracing::Span {
        &self.span
    }

    /// The op's context: its id, kind and span, and who asks.
    #[inline]
    pub fn ctx<'a>(&'a self, caller: &'a Caller) -> OpCtx<'a> {
        OpCtx {
            op: self.id,
            kind: self.kind,
            caller,
            lock_owner: None,
            deadline: None,
            cancel: None,
            span: &self.span,
        }
    }

    /// Enter the op's span on this thread for the engine call: events the
    /// engine (and its S3/P2P calls) log on it nest under `vfs.op`.
    #[inline]
    pub fn enter(&self) -> tracing::span::Entered<'_> {
        self.span.enter()
    }

    /// `responder`, wrapped to count this op when it completes.
    #[inline]
    pub fn responder<R>(&self, responder: R) -> Timed<R> {
        self.observer
            .metrics
            .timed(self.kind, self.started, responder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::responder::{Blocking, Responder};
    use std::sync::Mutex;
    use tracing_subscriber::layer::SubscriberExt;

    /// Records the spans created and the events inside them.
    #[derive(Clone, Default)]
    struct Recorder(Arc<Mutex<Vec<String>>>);

    struct Fields(String);

    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!(" {}={:?}", field.name(), value));
        }
    }

    impl<S: tracing::Subscriber + for<'l> tracing_subscriber::registry::LookupSpan<'l>>
        tracing_subscriber::Layer<S> for Recorder
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut fields = Fields(String::new());
            attrs.record(&mut fields);
            self.0
                .lock()
                .unwrap()
                .push(format!("span {}{}", attrs.metadata().name(), fields.0));
        }

        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let parent = ctx.lookup_current().map(|s| s.name().to_owned());
            self.0.lock().unwrap().push(format!("event in {parent:?}"));
        }
    }

    #[test]
    fn the_span_names_the_op_and_the_engine_events_nest_under_it() {
        let recorder = Recorder::default();
        let subscriber = tracing_subscriber::registry().with(recorder.clone());
        let identity = ViewIdentity {
            id: 7,
            metric_view: Some("pv-1".into()),
        };
        let observer = Observer::with_metrics(
            "unit",
            &identity,
            OpMetrics::detached("unit", Some("pv-1"), "uring"),
        );
        let caller = Caller::root();
        let id = tracing::subscriber::with_default(subscriber, || {
            let op = observer.begin(OpKind::Lookup, 42);
            let _in = op.enter();
            let cx = op.ctx(&caller);
            assert_eq!(cx.op, op.id());
            assert_eq!(cx.kind, OpKind::Lookup);
            tracing::debug!("an engine event, synchronous under the op");
            op.id()
        });
        let log = recorder.0.lock().unwrap().clone();
        let span = &log[0];
        for want in [
            "span vfs.op",
            "op=\"lookup\"",
            "ino=42",
            &format!("op_id={}", id.0),
            "frontend=\"unit\"",
            "transport=\"uring\"",
            "view_id=7",
            "view=\"pv-1\"",
        ] {
            assert!(span.contains(want), "{want} in {span}");
        }
        assert_eq!(log[1], "event in Some(\"vfs.op\")");
    }

    #[test]
    fn an_op_has_its_id_and_its_count_whatever_the_subscriber_does() {
        let observer = Observer::with_metrics(
            "unit",
            &ViewIdentity::default(),
            OpMetrics::detached("unit", None, crate::metrics::NO_TRANSPORT),
        );
        // (Whether the span is the empty one depends on tracing's
        // process-wide callsite cache, which another test's subscriber
        // changes; the `vfs-bench` binary, with none, counts the
        // allocations of that path.)
        let op = observer.begin(OpKind::Getattr, 1);
        let caller = Caller::root();
        assert_ne!(op.ctx(&caller).op, OpCtx::new(OpKind::Getattr, &caller).op);
        let _ = Blocking::run(|r: Blocking<u8>| op.responder(r).done(Ok(1)));
        assert_eq!(observer.metrics().view(), None);
    }
}
