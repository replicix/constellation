//! The streaming methods: `node.logs.tail` and `browse.read` (chunks),
//! `stats.subscribe` and `events.subscribe` (events).
//!
//! **Events** come from an [`EventBus`] (a broadcast channel). Hosts may
//! publish their own; the service's watcher — started with the first
//! subscriber, stopped when the service is dropped — observes the rest once
//! a second: views mounted and unmounted (whoever mounted them: the CLI's
//! first mount never passes through `view.mount`), the write lease acquired
//! and lost, peers connected and disconnected. A subscriber that falls more
//! than the channel's capacity behind skips the lost events (and is told so
//! by an `events.lagged` event) rather than stalling the others.
//!
//! **Stats** are samples of the same numbers `/metrics` exports, taken
//! from `node.status` every `interval_ms` (at least 100 ms).

use super::{blocking, EngineControl};
use bytes::Bytes;
use constellation_control::methods::{BrowseRead, EventsSubscribe, NodeLogsTail, StatsSubscribe};
use constellation_control::proto::types::{ControlEvent, StatsSample, StatusReport};
use constellation_control::proto::{ControlError, JsonValue};
use constellation_control::Router;
use futures::stream::{self, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::broadcast;

/// Events buffered per subscriber before it starts skipping.
const EVENT_CAPACITY: usize = 256;
/// How often a `node.logs.tail --follow` looks for new lines.
const FOLLOW_POLL: Duration = Duration::from_millis(250);
/// How often the watcher looks for transitions.
const WATCH_PERIOD: Duration = Duration::from_secs(1);

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Where daemon events are published.
pub(crate) struct EventBus {
    tx: broadcast::Sender<ControlEvent>,
    watching: AtomicBool,
}

impl EventBus {
    pub(crate) fn new() -> EventBus {
        EventBus {
            tx: broadcast::channel(EVENT_CAPACITY).0,
            watching: AtomicBool::new(false),
        }
    }

    pub(crate) fn publish(&self, topic: &str, data: serde_json::Value) {
        let _ = self.tx.send(ControlEvent {
            unix_ms: now_ms(),
            topic: topic.to_string(),
            data: JsonValue(data),
        });
    }
}

/// What the watcher compares from one look to the next.
#[derive(Default, PartialEq)]
struct Observed {
    views: BTreeMap<u64, String>,
    lease_held: bool,
    peers: BTreeSet<u64>,
}

fn observe(svc: &EngineControl) -> Observed {
    Observed {
        views: svc
            .host
            .views()
            .into_iter()
            .map(|v| (v.id, v.mountpoint.display().to_string()))
            .collect(),
        lease_held: svc.lease.status().held,
        peers: svc
            .peers
            .snapshot()
            .into_iter()
            .filter(|p| p.connected)
            .map(|p| p.node_id)
            .collect(),
    }
}

fn publish_changes(svc: &EngineControl, before: &Observed, now: &Observed) {
    for (id, mountpoint) in &now.views {
        if !before.views.contains_key(id) {
            svc.publish(
                "view.mounted",
                serde_json::json!({"id": id, "mountpoint": mountpoint}),
            );
        }
    }
    for (id, mountpoint) in &before.views {
        if !now.views.contains_key(id) {
            svc.publish(
                "view.unmounted",
                serde_json::json!({"id": id, "mountpoint": mountpoint}),
            );
        }
    }
    if now.lease_held != before.lease_held {
        let topic = if now.lease_held {
            "lease.acquired"
        } else {
            "lease.lost"
        };
        svc.publish(topic, serde_json::json!({"node_id": svc.node_id}));
    }
    for id in now.peers.difference(&before.peers) {
        svc.publish("peer.connected", serde_json::json!({"node_id": id}));
    }
    for id in before.peers.difference(&now.peers) {
        svc.publish("peer.disconnected", serde_json::json!({"node_id": id}));
    }
}

/// Start the watcher once (see the module docs).
fn ensure_watcher(svc: &Arc<EngineControl>) {
    if svc.events.watching.swap(true, Ordering::SeqCst) {
        return;
    }
    let weak: Weak<EngineControl> = Arc::downgrade(svc);
    svc.rt.spawn(async move {
        let mut before: Option<Observed> = None;
        loop {
            let Some(svc) = weak.upgrade() else { return };
            let now = match tokio::task::spawn_blocking(move || {
                let now = observe(&svc);
                (svc, now)
            })
            .await
            {
                Ok((svc, now)) => {
                    if let Some(before) = &before {
                        publish_changes(&svc, before, &now);
                    }
                    now
                }
                Err(_) => return,
            };
            before = Some(now);
            tokio::time::sleep(WATCH_PERIOD).await;
        }
    });
}

fn wanted(topics: &[String], topic: &str) -> bool {
    topics.is_empty()
        || topics.iter().any(|t| {
            topic == t
                || topic
                    .strip_prefix(t.as_str())
                    .is_some_and(|rest| rest.starts_with('.'))
        })
}

/// The gauges and counters of a status report, by the `/metrics` names.
pub(crate) fn sample_of(status: &StatusReport) -> StatsSample {
    let mut counters = BTreeMap::new();
    let mut gauges = BTreeMap::new();
    let mut c = |name: &str, v: u64| {
        counters.insert(format!("constellation_{name}"), v);
    };
    c("coop_peer_hits_total", status.coop.peer_hits);
    c("coop_s3_fetches_total", status.coop.s3_fetches);
    c("prune_runs_total", status.prune.runs);
    c("prune_deleted_total", status.prune.deleted);
    c("forwarded_ok_total", status.forwarded_ok);
    c("forwarded_err_total", status.forwarded_err);
    c("s3_get_total", status.s3.get);
    c("s3_put_total", status.s3.put);
    c("s3_list_total", status.s3.list);
    c("s3_delete_total", status.s3.delete);
    c(
        "fuse_requests_stalled_total",
        status.fuse_requests.stalled_total,
    );
    // The op metrics by name are labelled series (`/metrics`); a sample
    // carries their stable totals.
    let (ops, refused) = status
        .vfs_ops
        .series
        .iter()
        .fold((0, 0), |(all, bad), series| {
            let total: u64 = series.outcomes.values().sum();
            let ok = series.outcomes.get("ok").copied().unwrap_or(0);
            (all + total, bad + total - ok)
        });
    c("vfs_ops_total", ops);
    c("vfs_ops_refused_total", refused);
    let mut g = |name: &str, v: f64| {
        gauges.insert(format!("constellation_{name}"), v);
    };
    g("spool_backlog", status.spool.journal_backlog as f64);
    g("spool_head_sequence", status.spool.head_seq as f64);
    g("cache_used_bytes", status.cache.used_bytes as f64);
    g("cache_budget_bytes", status.cache.budget_bytes as f64);
    g("cache_chunks", status.cache.chunks as f64);
    g("cache_pinned_bytes", status.cache.pinned_bytes as f64);
    g("lease_held", u8::from(status.lease.held) as f64);
    g("lease_epoch", status.lease.epoch as f64);
    g("writeback_dirty_bytes", status.writeback.dirty_bytes as f64);
    g(
        "writeback_pending_uploads",
        status.writeback.pending_uploads as f64,
    );
    g("prefetch_inflight", status.prefetch.inflight as f64);
    g(
        "fuse_requests_in_flight",
        status.fuse_requests.in_flight as f64,
    );
    g("fuse_requests_stalled", status.fuse_requests.stalled as f64);
    g("views", status.mounts.len() as f64);
    StatsSample {
        unix_ms: now_ms(),
        counters,
        gauges,
    }
}

pub(super) fn register_subscriptions(r: &mut Router, svc: &Arc<EngineControl>) {
    {
        let svc = svc.clone();
        r.register_events::<StatsSubscribe, _, _, _>(move |_ctx, p| {
            let svc = svc.clone();
            async move {
                let period = Duration::from_millis(p.interval_ms.max(100));
                Ok(stream::unfold(
                    (svc, true),
                    move |(svc, first)| async move {
                        if !first {
                            tokio::time::sleep(period).await;
                        }
                        let s = svc.clone();
                        let sample = blocking(move || Ok(sample_of(&s.status()))).await;
                        Some((sample, (svc, false)))
                    },
                ))
            }
        });
    }
    let svc = svc.clone();
    r.register_events::<EventsSubscribe, _, _, _>(move |_ctx, p| {
        let svc = svc.clone();
        async move {
            let rx = svc.events.tx.subscribe();
            ensure_watcher(&svc);
            let topics = p.topics;
            Ok(stream::unfold(
                (rx, topics),
                |(mut rx, topics)| async move {
                    loop {
                        match rx.recv().await {
                            Ok(event) if wanted(&topics, &event.topic) => {
                                return Some((Ok(event), (rx, topics)));
                            }
                            Ok(_) => continue,
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                let event = ControlEvent {
                                    unix_ms: now_ms(),
                                    topic: "events.lagged".into(),
                                    data: JsonValue(serde_json::json!({"skipped": skipped})),
                                };
                                return Some((Ok(event), (rx, topics)));
                            }
                            Err(broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                },
            ))
        }
    });
}

pub(super) fn register_logs_tail(r: &mut Router, svc: &Arc<EngineControl>) {
    let svc = svc.clone();
    r.register_chunks::<NodeLogsTail, _, _, _>(move |_ctx, p| {
        let logs = svc.log_buffer.clone();
        async move {
            // The old `LogTail` capped the request at 10 000 lines.
            let (pos, lines) = logs.tail_at(p.lines.min(10_000));
            let text = |lines: Vec<String>| -> Bytes {
                let mut out = String::new();
                for line in lines {
                    out.push_str(&line);
                    out.push('\n');
                }
                Bytes::from(out)
            };
            let first = text(lines);
            let follow = p.follow;
            Ok(stream::unfold(
                (logs, pos, Some(first)),
                move |(logs, pos, first)| async move {
                    if let Some(first) = first {
                        return Some((Ok(first), (logs, pos, None)));
                    }
                    if !follow {
                        return None;
                    }
                    loop {
                        tokio::time::sleep(FOLLOW_POLL).await;
                        let (next, lines) = logs.since(pos);
                        if !lines.is_empty() {
                            return Some((Ok(text(lines)), (logs, next, None)));
                        }
                    }
                },
            )
            .filter(|item: &Result<Bytes, ControlError>| {
                std::future::ready(!matches!(item, Ok(b) if b.is_empty()))
            }))
        }
    });
}

pub(super) fn register_browse_read(r: &mut Router, svc: &Arc<EngineControl>) {
    let svc = svc.clone();
    r.register_chunks::<BrowseRead, _, _, _>(move |ctx, p| {
        let svc = svc.clone();
        let principal = ctx.principal.clone();
        async move {
            let browser = svc.browser(&principal)?;
            // A bounded hand-off: the reading thread waits for the consumer,
            // so at most a few slices are in memory whatever the file size.
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, ControlError>>(4);
            tokio::task::spawn_blocking(move || {
                let sent = browser.read(&p.path, p.offset, p.length, |bytes| {
                    tx.blocking_send(Ok(Bytes::from(bytes))).is_ok()
                });
                if let Err(e) = sent {
                    let _ = tx.blocking_send(Err(e));
                }
            });
            Ok(stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            }))
        }
    });
}
