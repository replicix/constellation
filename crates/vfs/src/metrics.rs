//! Unified op metrics (plan 31 §6.10): `constellation_vfs_ops_total
//! {frontend,op,outcome}` and `constellation_vfs_op_seconds{frontend,op}`,
//! recorded once per op, when its responder completes.
//!
//! # Why the responder
//!
//! Every op ends in exactly one [`Responder::done`] (or its drop
//! fail-safe, [`crate::responder`]), on whichever thread completes it — a
//! blocking lock's `lock-wait` thread as much as the frontend worker. A
//! [`Timed`] wraps the frontend's responder, so the recording point is
//! that completion and nothing else: a deferred op counts when it is
//! answered, an op dropped unanswered counts as `Io` (which is what its
//! caller sees), and no op counts twice.
//!
//! # No allocation, no lookup on the hot path
//!
//! An [`OpMetrics`] is built once per (frontend, view) and holds one
//! [`Series`] per [`OpKind`]: a fixed array of atomic counters indexed by
//! outcome, a fixed array of atomic histogram buckets, and an atomic
//! nanosecond sum. Recording an op is three relaxed `fetch_add`s at
//! indices computed from two enums and one comparison run over
//! [`BUCKET_BOUNDS_NS`]: no string is formatted, no map is consulted, no
//! heap is touched. Labels are text only when *scraped* ([`snapshot`]).
//! The frontend name is a `&'static str` fixed at construction; the
//! `view` label is fixed at construction too, and only ever from the
//! allowlisted `ViewSpec::metric_labels` (the engine's; plan 31 §9.10,
//! bounded cardinality) — it arrives here as one already-rendered string.
//!
//! # Outcome
//!
//! `ok`, or the [`Code`]'s name ([`Code::name`]): a bounded set of at
//! most [`OUTCOME_SLOTS`] values. A code numbered past the table folds
//! into `Io` (a unit test fails when a new code would need a wider one).
//!
//! # Lifetime
//!
//! [`OpMetrics::for_view`] shares one series set between every
//! frontend session of the same (frontend, view label) — a handover's
//! resumed session, two mounts of one volume — so a scrape never sees a
//! counter step backwards when one of them ends. The registry holds them
//! weakly: when the last session of a label is gone its series are too
//! (an ordinary counter reset for a scraper).

use crate::ctx::OpKind;
use crate::error::VfsResult;
use crate::responder::{DirSink, Responder};
use crate::types::{FileKind, Ino};
use constellation_types::Code;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// Outcome slots per op: slot 0 is `ok`, slot `n` is the [`Code`] with
/// wire number `n`.
pub const OUTCOME_SLOTS: usize = 80;

/// Histogram bucket upper bounds (inclusive), nanoseconds: 10 µs to 60 s,
/// a 1-2.5-5 progression (with a 30 s step before the last). The implicit
/// last bucket is `+Inf`.
pub const BUCKET_BOUNDS_NS: [u64; 21] = [
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
];

/// Buckets per histogram: the bounds and `+Inf`.
pub const BUCKETS: usize = BUCKET_BOUNDS_NS.len() + 1;

/// The slot an outcome is counted in.
const fn outcome_slot(code: Option<Code>) -> usize {
    match code {
        None => 0,
        Some(code) => {
            let wire = code.to_wire() as usize;
            if wire < OUTCOME_SLOTS {
                wire
            } else {
                Code::Io.to_wire() as usize
            }
        }
    }
}

/// The bucket a latency falls in (the first bound at or above it).
fn bucket_of(ns: u64) -> usize {
    BUCKET_BOUNDS_NS.partition_point(|&bound| bound < ns)
}

/// One op's counters within an [`OpMetrics`].
struct Series {
    outcomes: [AtomicU64; OUTCOME_SLOTS],
    buckets: [AtomicU64; BUCKETS],
    sum_ns: AtomicU64,
}

impl Series {
    fn new() -> Self {
        Self {
            outcomes: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
        }
    }
}

/// The metrics of one (frontend, view): see the module doc.
pub struct OpMetrics {
    frontend: &'static str,
    view: Option<Box<str>>,
    series: Box<[Series]>,
}

static REGISTRY: Mutex<Vec<Weak<OpMetrics>>> = Mutex::new(Vec::new());

impl OpMetrics {
    fn build(frontend: &'static str, view: Option<&str>) -> Arc<Self> {
        Arc::new(Self {
            frontend,
            view: view.map(Box::from),
            series: OpKind::ALL.iter().map(|_| Series::new()).collect(),
        })
    }

    /// The process-wide metrics of `frontend` serving the view labelled
    /// `view` (the allowlisted label value, already rendered; `None` for
    /// a view with none), created on first use and shared afterwards.
    /// Scraped by [`snapshot`].
    pub fn for_view(frontend: &'static str, view: Option<&str>) -> Arc<Self> {
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        registry.retain(|m| m.strong_count() > 0);
        let existing = registry
            .iter()
            .filter_map(Weak::upgrade)
            .find(|m| m.frontend == frontend && m.view.as_deref() == view);
        if let Some(existing) = existing {
            return existing;
        }
        let metrics = Self::build(frontend, view);
        registry.push(Arc::downgrade(&metrics));
        metrics
    }

    /// Metrics no scrape sees (a benchmark's own, a test's).
    pub fn detached(frontend: &'static str, view: Option<&str>) -> Arc<Self> {
        Self::build(frontend, view)
    }

    pub fn frontend(&self) -> &'static str {
        self.frontend
    }

    pub fn view(&self) -> Option<&str> {
        self.view.as_deref()
    }

    /// Count one op: `None` is `ok`, `Some(code)` a refusal.
    #[inline]
    pub fn record(&self, kind: OpKind, outcome: Option<Code>, elapsed: Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let series = &self.series[kind as usize];
        series.outcomes[outcome_slot(outcome)].fetch_add(1, Relaxed);
        series.buckets[bucket_of(ns)].fetch_add(1, Relaxed);
        series.sum_ns.fetch_add(ns, Relaxed);
    }

    /// `responder`, wrapped to count the op when it completes; `started`
    /// is when the op began.
    #[inline]
    pub fn timed<R>(self: &Arc<Self>, kind: OpKind, started: Instant, responder: R) -> Timed<R> {
        Timed {
            inner: Some(responder),
            metrics: self.clone(),
            kind,
            started,
        }
    }
}

/// A [`Responder`] that counts its op when it completes (module doc). It
/// is also a [`DirSink`] when what it wraps is, so `readdir` is
/// instrumented like the rest.
pub struct Timed<R> {
    inner: Option<R>,
    metrics: Arc<OpMetrics>,
    kind: OpKind,
    started: Instant,
}

impl<T, R: Responder<T>> Responder<T> for Timed<R> {
    fn done(mut self, result: VfsResult<T>) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        // Counted before the reply is written: the op is over, and the
        // count must not depend on the reply path returning.
        self.metrics.record(
            self.kind,
            result.as_ref().err().map(|e| e.code()),
            self.started.elapsed(),
        );
        inner.done(result);
    }
}

impl<R> Drop for Timed<R> {
    fn drop(&mut self) {
        // Dropped without `done`: the wrapped responder's own fail-safe
        // answers `Io` as it drops here, so that is what is counted.
        if let Some(inner) = self.inner.take() {
            self.metrics
                .record(self.kind, Some(Code::Io), self.started.elapsed());
            drop(inner);
        }
    }
}

impl<R: DirSink> DirSink for Timed<R> {
    fn add(&mut self, ino: Ino, next: u64, kind: FileKind, name: &[u8]) -> bool {
        match self.inner.as_mut() {
            Some(sink) => sink.add(ino, next, kind, name),
            None => true,
        }
    }
}

/// One (frontend, view, op) as scraped: the counters of every
/// [`OpMetrics`] sharing it, summed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpSeries {
    pub frontend: &'static str,
    pub view: Option<String>,
    pub op: &'static str,
    /// Ops per outcome (`ok` or a [`Code::name`]), non-zero ones only.
    pub outcomes: Vec<(&'static str, u64)>,
    /// Ops per latency bucket, not cumulative; `BUCKETS` long, the last
    /// being `+Inf`.
    pub buckets: Vec<u64>,
    /// The latencies' sum.
    pub sum_ns: u64,
}

impl OpSeries {
    /// Ops counted.
    pub fn count(&self) -> u64 {
        self.outcomes.iter().map(|(_, n)| n).sum()
    }
}

/// The name of an outcome slot.
fn outcome_name(slot: usize) -> Option<&'static str> {
    if slot == 0 {
        return Some("ok");
    }
    Code::ALL
        .iter()
        .find(|c| c.to_wire() as usize == slot)
        .map(|c| c.name())
}

/// Every op counted so far by a live [`OpMetrics`], one entry per
/// (frontend, view, op name) with anything counted. `setlk`'s two kinds
/// (acquire and release) are one series, as their name is.
pub fn snapshot() -> Vec<OpSeries> {
    let live: Vec<Arc<OpMetrics>> = {
        let mut registry = REGISTRY.lock().unwrap_or_else(|e| e.into_inner());
        registry.retain(|m| m.strong_count() > 0);
        registry.iter().filter_map(Weak::upgrade).collect()
    };
    let mut out: Vec<OpSeries> = Vec::new();
    for metrics in &live {
        for (kind, series) in OpKind::ALL.iter().zip(metrics.series.iter()) {
            let outcomes: Vec<(&'static str, u64)> = (0..OUTCOME_SLOTS)
                .filter_map(|slot| {
                    let n = series.outcomes[slot].load(Relaxed);
                    if n == 0 {
                        return None;
                    }
                    outcome_name(slot).map(|name| (name, n))
                })
                .collect();
            if outcomes.is_empty() {
                continue;
            }
            let buckets: Vec<u64> = series.buckets.iter().map(|b| b.load(Relaxed)).collect();
            let sum_ns = series.sum_ns.load(Relaxed);
            let view = metrics.view.as_deref();
            let existing = out.iter_mut().find(|s| {
                s.frontend == metrics.frontend && s.view.as_deref() == view && s.op == kind.name()
            });
            match existing {
                Some(s) => {
                    for (name, n) in outcomes {
                        match s.outcomes.iter_mut().find(|(k, _)| *k == name) {
                            Some((_, total)) => *total += n,
                            None => s.outcomes.push((name, n)),
                        }
                    }
                    for (total, n) in s.buckets.iter_mut().zip(buckets) {
                        *total += n;
                    }
                    s.sum_ns += sum_ns;
                }
                None => out.push(OpSeries {
                    frontend: metrics.frontend,
                    view: view.map(str::to_owned),
                    op: kind.name(),
                    outcomes,
                    buckets,
                    sum_ns,
                }),
            }
        }
    }
    out.sort_by(|a, b| (a.frontend, &a.view, a.op).cmp(&(b.frontend, &b.view, b.op)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::responder::Blocking;
    use crate::VfsError;

    #[test]
    fn every_code_has_its_own_outcome_slot() {
        let mut seen = std::collections::HashSet::new();
        for code in Code::ALL {
            let slot = outcome_slot(Some(*code));
            assert!(
                slot > 0 && slot < OUTCOME_SLOTS,
                "{code:?} needs a wider table"
            );
            assert!(seen.insert(slot), "{code:?} shares a slot");
            assert_eq!(outcome_name(slot), Some(code.name()));
        }
        assert_eq!(outcome_slot(None), 0);
        assert_eq!(outcome_name(0), Some("ok"));
    }

    #[test]
    fn latencies_land_in_the_first_bucket_at_or_above_them() {
        assert_eq!(bucket_of(0), 0);
        assert_eq!(bucket_of(10_000), 0);
        assert_eq!(bucket_of(10_001), 1);
        assert_eq!(bucket_of(60_000_000_000), BUCKET_BOUNDS_NS.len() - 1);
        assert_eq!(bucket_of(60_000_000_001), BUCKETS - 1);
        assert_eq!(bucket_of(u64::MAX), BUCKETS - 1);
        assert!(BUCKET_BOUNDS_NS.windows(2).all(|w| w[0] < w[1]));
    }

    fn series_of(view: &str, op: &str) -> Option<OpSeries> {
        snapshot()
            .into_iter()
            .find(|s| s.view.as_deref() == Some(view) && s.op == op)
    }

    #[test]
    fn a_completed_op_is_counted_once_by_outcome_and_latency() {
        let m = OpMetrics::for_view("unit", Some("v-counted"));
        let started = Instant::now();
        let ok = Blocking::run(|r: Blocking<u8>| m.timed(OpKind::Getattr, started, r).done(Ok(1)));
        assert_eq!(ok, Ok(1));
        for _ in 0..2 {
            let refused = Blocking::run(|r: Blocking<u8>| {
                m.timed(OpKind::Getattr, started, r)
                    .done(Err(VfsError::new(Code::NotFound)))
            });
            assert_eq!(refused, Err(Code::NotFound.into()));
        }
        let s = series_of("v-counted", "getattr").expect("a getattr series");
        assert_eq!((s.frontend, s.count()), ("unit", 3));
        assert_eq!(s.outcomes, vec![("ok", 1), ("NotFound", 2)]);
        assert_eq!(s.buckets.iter().sum::<u64>(), 3);
        assert_eq!(s.buckets.len(), BUCKETS);
        assert!(
            series_of("v-counted", "lookup").is_none(),
            "untouched ops are absent"
        );
    }

    #[test]
    fn a_responder_dropped_unanswered_counts_as_io_and_never_twice() {
        let m = OpMetrics::for_view("unit", Some("v-dropped"));
        let got = Blocking::<u8>::run(|r| drop(m.timed(OpKind::Read, Instant::now(), r)));
        assert_eq!(got, Err(Code::Io.into()));
        // Completed on another thread, as a deferred op is.
        let got = Blocking::<u8>::run(|r| {
            let r = m.timed(OpKind::Read, Instant::now(), r);
            std::thread::spawn(move || r.done(Ok(1)));
        });
        assert_eq!(got, Ok(1));
        let s = series_of("v-dropped", "read").unwrap();
        assert_eq!(s.outcomes, vec![("ok", 1), ("Io", 1)]);
    }

    #[test]
    fn views_of_one_label_share_series_and_lock_ops_share_a_name() {
        let a = OpMetrics::for_view("unit", Some("v-shared"));
        let b = OpMetrics::for_view("unit", Some("v-shared"));
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(
            &a,
            &OpMetrics::for_view("unit", Some("v-other"))
        ));
        assert!(!Arc::ptr_eq(&a, &OpMetrics::for_view("unit", None)));
        a.record(OpKind::LockAcquire, None, Duration::from_micros(20));
        b.record(OpKind::LockRelease, None, Duration::from_micros(20));
        let s = series_of("v-shared", "setlk").unwrap();
        assert_eq!(s.count(), 2, "acquire and release are both `setlk`");
        assert_eq!(s.sum_ns, 40_000);
        // Nothing keeps a series once its last user is gone.
        drop((a, b));
        // (A concurrent test's scrape may hold a strong reference for a
        // moment while it reads.)
        let gone = (0..200).any(|_| {
            std::thread::sleep(Duration::from_millis(5));
            series_of("v-shared", "setlk").is_none()
        });
        assert!(gone, "the series outlived its last user");
    }

    #[test]
    fn a_readdir_sink_wrapped_still_collects() {
        use crate::responder::CollectDir;
        let m = OpMetrics::detached("unit", None);
        let (sink, wait) = CollectDir::pair(8);
        let mut sink = m.timed(OpKind::Readdir, Instant::now(), sink);
        assert!(!sink.add(2, 1, FileKind::File, b"f"));
        sink.done(Ok(()));
        assert_eq!(wait.wait().unwrap().len(), 1);
    }
}
