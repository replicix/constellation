//! Plan 30 §M7: direct log streams.
//!
//! The holder serves `LogSubscribe { from }` and streams, in order, every
//! segment its log cursor passes over while it holds — the segments it
//! ships, its takeover marker, and anything it reads back from S3 (its
//! own unacked segments, a fenced one) — as exactly the bytes of the S3
//! object at that sequence. A subscriber feeds each streamed segment to
//! [`Core::apply_incoming`], the tail path, fencing included: a streamed
//! apply is the same state transition as tailing that sequence from S3,
//! only earlier. Streams change latency and S3 traffic, never what is
//! applied.
//!
//! # Never skipping a sequence
//!
//! A subscriber applies only `next_seq`. Segments that arrive ahead of it
//! wait in a bounded reorder buffer; a missing sequence is read from S3 by
//! the ordinary round (the stream never *replaces* the tail, it lets a
//! round skip it). Three things make a missing sequence visible:
//! - frames are numbered per subscription (`n`); a frame out of order —
//!   one was lost, or two were reordered — breaks the stream at once
//!   (a QUIC stream never does either; the simulation's bus does);
//! - every frame carries the holder's `head`: after draining, a
//!   subscriber whose cursor is at or below it is missing something the
//!   holder will not send (it passed before the subscription, or the
//!   holder's ring no longer had it) and tails S3;
//! - a heartbeat frame every `stream_heartbeat_ms` keeps `head` current
//!   when nothing ships, and a stream silent for `stream_timeout_ms` is
//!   dead. A round skips the S3 tail only while the stream is live, the
//!   buffer is empty and the cursor is past the last reported head — and
//!   even then probes S3 once (width 1) every `stream_backstop_ms`.
//!
//! None of this is a safety input (plan 30 §2.5): a false "dead" costs a
//! resubscription and an S3 GET; a missed death costs staleness bounded
//! by the backstop, never a wrong apply.
//!
//! # Epoch changes
//!
//! A node serves only while it holds the lease or a continuation epoch's
//! hold (not deposed). Losing, releasing or handing it off ends every
//! subscription (`LogStreamEnd`); a subscriber that learns of a different
//! holder (a newer segment, a lease read, a redirect) or takes the lease
//! itself drops its stream. A new holder's first streamed segment is its
//! takeover marker; a stale holder that still streams after being
//! deposed (it has not yet noticed) only ever streams segments that are
//! in S3 at those sequences, which the subscriber's own fencing handles
//! exactly as a tail would.
//!
//! # Continuation epochs
//!
//! An epoch is exactly when S3 cannot deliver, so the stream is the only
//! path by which a member learns the hold owner's log. The hold owner
//! keeps serving (its ring: the segments it shipped before the outage, so
//! a member that missed some catches up — flex-crash seed 30702's
//! partitioned member), and it streams its epoch journal ahead
//! (`StreamAhead`, M9's machinery: installed as `Streamed` speculation,
//! retired by the segments its flush ships at the close, stranded if a
//! later epoch's segments come first). Members keep following it, so a
//! forward reply whose `base` names the holder's unshipped journal is
//! answered once the stream reaches it — never before — instead of
//! waiting for a log that cannot arrive until S3 returns.
//!
//! # Holder side, bounded
//!
//! The holder keeps the last `stream_ring_segments` segments (at most
//! `stream_ring_bytes`) so a subscriber that subscribes a little behind
//! (it tailed to head a moment ago) is served without S3. Everything else
//! about a slow subscriber is the driver's: each subscriber has a bounded
//! send buffer, and one that overflows is dropped back to S3 tailing
//! (`Event::SubscriberGone`) — a slow WAN subscriber never slows the
//! holder or its other subscribers.
//!
//! # Why the holder serves everyone (no relay tree)
//!
//! See the plan 30 M7 section of `docs/plans/v1/PROGRESS.md`: one stream
//! per subscriber from the holder, with per-subscriber buffers, costs the
//! holder one copy of each segment per subscriber. The protocol is
//! relay-ready (a relay would be a subscriber that also serves
//! `LogSubscribe` from its buffer), but nothing needs one yet.

use super::{Core, Timer};
use crate::action::Action;
use crate::event::PeerMsg;
use crate::ids::{Epoch, Ms, NodeId, OpId, Seq, TimerId};
use crate::replica::Replica;
use std::collections::{BTreeMap, VecDeque};

/// One subscriber the holder serves.
#[derive(Debug, Clone)]
struct Served {
    req: OpId,
    /// The next frame number.
    n: u64,
    last_sent: Ms,
}

/// This node's subscription to a holder.
#[derive(Debug, Clone)]
struct Subscription {
    holder: NodeId,
    req: OpId,
    /// The frame number expected next.
    next_n: u64,
    /// The holder's highest reported applied-or-shipped sequence.
    head: Seq,
    /// At least one frame arrived.
    live: bool,
    since: Ms,
    last_frame: Ms,
    /// Segments ahead of the cursor, waiting for it.
    buf: BTreeMap<Seq, Vec<u8>>,
    buf_bytes: usize,
}

/// Both sides' state.
impl StreamState {
    pub(crate) fn container_sizes(&self) -> Vec<(&'static str, usize)> {
        vec![
            ("stream_served", self.served.len()),
            ("stream_ring", self.ring.len()),
            ("stream_ring_bytes", self.ring_bytes),
            (
                "stream_sub_buf",
                self.sub.as_ref().map(|s| s.buf.len()).unwrap_or(0),
            ),
        ]
    }
}

#[derive(Debug, Default)]
pub(crate) struct StreamState {
    // ---- holder ----
    served: BTreeMap<NodeId, Served>,
    ring: VecDeque<(Seq, Epoch, Vec<u8>)>,
    ring_bytes: usize,
    heartbeat_timer: Option<TimerId>,
    // ---- subscriber ----
    sub: Option<Subscription>,
    retry_at: Ms,
    retry_ms: u64,
    watchdog_timer: Option<TimerId>,
    last_s3_tail: Ms,
    /// Whether the peer directory last said the known holder is
    /// reachable: a subscriber that backed off while the holder was not
    /// (a fresh mount, before the registry enrolled both keys) retries at
    /// once when it becomes reachable, not a whole backoff later.
    holder_link_up: bool,
    /// A holder that refused or ended a subscription, and the log head
    /// then: it is not asked again until the log has moved past that head
    /// (someone shipped — it again, or a new holder the tail will name),
    /// as this node's cursor or a gossip hint (`hinted`) shows.
    /// Keeps an idle cluster, whose holder let the lease go, from asking
    /// it every backoff period for ever.
    parked: Option<(NodeId, Seq)>,
    /// The highest sequence a `SegmentPublished` hint named: a round
    /// tailing because of hints probes just past it rather than a full
    /// speculative run (gossip hints carry no payload since M7).
    hinted: Seq,
    /// The last subscription was caught up when it went (live, nothing
    /// buffered, the cursor past the holder's last reported head): S3
    /// is expected to hold nothing new, so the rounds that tail in its
    /// place probe one GET wide until a stream is live again. On a
    /// loaded host a heartbeat late past `stream_timeout_ms` drops an
    /// idle cluster's streams every few minutes, and each drop cost a
    /// full-width speculative run of 404s (`tail_width`, 16 GETs):
    /// `idle-cost`'s follower at 61 requests/min, 75 `GET log` in two
    /// minutes against its peers' 11. A narrow probe that hits is
    /// followed by a full-width run, so it never stops short of the
    /// head.
    dropped_caught_up: bool,
}

/// What a subscriber's stream looks like, for `status`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamView {
    /// The holder this node is subscribed to (0: none).
    pub upstream: NodeId,
    /// At least one frame arrived on the current subscription.
    pub live: bool,
    /// Subscribers this node serves as the holder.
    pub serving: u64,
    /// Segments waiting in the reorder buffer.
    pub buffered: u64,
}

impl Core {
    /// The stream's observable state, for `status`.
    pub fn stream_view(&self) -> StreamView {
        let s = &self.stream;
        StreamView {
            upstream: s.sub.as_ref().map(|x| x.holder).unwrap_or(0),
            live: s.sub.as_ref().is_some_and(|x| x.live),
            serving: s.served.len() as u64,
            buffered: s.sub.as_ref().map(|x| x.buf.len() as u64).unwrap_or(0),
        }
    }

    /// This node may serve its log: it holds the lease through S3, or a
    /// continuation epoch's hold.
    fn stream_serving(&self) -> bool {
        self.cfg.log_streams
            && self.cfg.p2p
            && (self.lease.held.is_some() || self.lease.epoch_held())
            && !self.lease.lost
    }

    /// Plan 30 §M9: the subscribers this holder serves (pre-S3 streaming
    /// goes to them).
    pub(crate) fn stream_subscribers(&self) -> Vec<NodeId> {
        if !self.stream_serving() {
            return Vec::new();
        }
        self.stream.served.keys().copied().collect()
    }

    /// Plan 30 §M9: whether a live stream from `holder` is up (its
    /// heartbeats are P2P liveness evidence).
    pub(crate) fn stream_live_from(&self, holder: NodeId) -> bool {
        self.stream
            .sub
            .as_ref()
            .is_some_and(|s| s.holder == holder && s.live)
    }

    /// Plan 30 §M9: when the last frame (a heartbeat included) arrived
    /// on this node's subscription to `holder`, if it has one.
    pub(crate) fn stream_last_frame_from(&self, holder: NodeId) -> Option<Ms> {
        self.stream
            .sub
            .as_ref()
            .filter(|s| s.holder == holder && s.live)
            .map(|s| s.last_frame)
    }

    /// The holder's heartbeat interval: `stream_heartbeat_ms`, or a
    /// third of `backup_takeover_ms` under `ack=s3`, whose followers
    /// read the holder's silence off this stream (plan 30 §M9).
    fn stream_heartbeat_every(&self) -> u64 {
        let every = self.cfg.stream_heartbeat_ms.max(1);
        if self.lease.ack_policy() == constellation_store_s3::AckPolicy::S3 {
            every.min((self.cfg.backup_takeover_ms / 3).max(1))
        } else {
            every
        }
    }

    // ---- holder side ----

    /// The holder's cursor passed `seq` (shipped it, landed its marker,
    /// or read it back from S3): remember it and stream it.
    pub(crate) fn stream_passed(
        &mut self,
        now: Ms,
        seq: Seq,
        epoch: Epoch,
        payload: &[u8],
        out: &mut Vec<Action>,
    ) {
        if !self.stream_serving() {
            return;
        }
        self.stream.ring.push_back((seq, epoch, payload.to_vec()));
        self.stream.ring_bytes += payload.len();
        while self.stream.ring.len() > self.cfg.stream_ring_segments.max(1)
            || (self.stream.ring_bytes > self.cfg.stream_ring_bytes && self.stream.ring.len() > 1)
        {
            if let Some((_, _, p)) = self.stream.ring.pop_front() {
                self.stream.ring_bytes -= p.len();
            }
        }
        let nodes: Vec<NodeId> = self.stream.served.keys().copied().collect();
        for node in nodes {
            self.stream_frame(now, node, Some((seq, payload.to_vec())), out);
        }
    }

    fn stream_frame(
        &mut self,
        now: Ms,
        node: NodeId,
        segment: Option<(Seq, Vec<u8>)>,
        out: &mut Vec<Action>,
    ) {
        let epoch = self.lease.epoch().unwrap_or(0);
        let head = self.ship.head_seq;
        let Some(s) = self.stream.served.get_mut(&node) else {
            return;
        };
        let n = s.n;
        s.n += 1;
        s.last_sent = now;
        self.stats.stream_frames_sent += 1;
        if let Some((seq, _)) = &segment {
            tracing::debug!(
                target: "constellation_authority::stream",
                node = self.cfg.node_id,
                to = node,
                seq,
                n,
                head,
                "stream send"
            );
        }
        out.push(Action::Send {
            to: node,
            msg: PeerMsg::LogStream {
                req: s.req,
                n,
                epoch,
                head,
                segment,
            },
        });
    }

    pub(crate) fn on_log_subscribe(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        from_seq: Seq,
        out: &mut Vec<Action>,
    ) {
        if !self.stream_serving() || from == self.cfg.node_id {
            self.stats.stream_declined += 1;
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LogStreamEnd { req, refused: true },
            });
            return;
        }
        self.stats.stream_served += 1;
        tracing::debug!(
            target: "constellation_authority::stream",
            node = self.cfg.node_id,
            subscriber = from,
            from_seq,
            head = self.ship.head_seq,
            ring = self.stream.ring.len(),
            "serving a log stream"
        );
        self.stream.served.insert(
            from,
            Served {
                req,
                n: 0,
                last_sent: now,
            },
        );
        if self.epoch_streams_ahead() {
            // A (re)subscribing member may have missed stream-ahead
            // batches (nothing ships during an epoch to carry them
            // later): stream the epoch journal again from its start;
            // what a subscriber holds already it skips.
            self.restream_ahead();
        }
        let backlog: Vec<(Seq, Vec<u8>)> = self
            .stream
            .ring
            .iter()
            .filter(|(seq, _, _)| *seq >= from_seq)
            .map(|(seq, _, p)| (*seq, p.clone()))
            .collect();
        for segment in backlog {
            self.stream_frame(now, from, Some(segment), out);
        }
        // Always one frame, so the subscriber learns the head (and that
        // the stream is up) even when nothing is pending.
        self.stream_frame(now, from, None, out);
        if self.stream.heartbeat_timer.is_none() {
            let id = self.set_timer(
                now.plus(self.stream_heartbeat_every()),
                Timer::StreamHeartbeat,
                out,
            );
            self.stream.heartbeat_timer = Some(id);
        }
    }

    pub(crate) fn on_log_unsubscribe(&mut self, from: NodeId, req: OpId, out: &mut Vec<Action>) {
        if self.stream.served.get(&from).is_some_and(|s| s.req == req) {
            self.stream.served.remove(&from);
            // Closes the driver's side of the stream too (the subscriber
            // ignores an end for a subscription it let go).
            out.push(Action::Send {
                to: from,
                msg: PeerMsg::LogStreamEnd {
                    req,
                    refused: false,
                },
            });
        }
    }

    /// `Event::SubscriberGone`: the driver dropped this subscriber.
    pub(crate) fn on_subscriber_gone(&mut self, node: NodeId, req: OpId) {
        if self.stream.served.get(&node).is_some_and(|s| s.req == req) {
            self.stream.served.remove(&node);
            self.stats.stream_subscribers_dropped += 1;
            tracing::debug!(
                target: "constellation_authority::stream",
                node = self.cfg.node_id,
                subscriber = node,
                "dropped a log-stream subscriber"
            );
        }
    }

    pub(crate) fn on_stream_heartbeat(&mut self, now: Ms, out: &mut Vec<Action>) {
        self.stream.heartbeat_timer = None;
        if self.stream.served.is_empty() {
            return;
        }
        let every = self.stream_heartbeat_every();
        let idle: Vec<NodeId> = self
            .stream
            .served
            .iter()
            .filter(|(_, s)| now.since(s.last_sent) >= every as i64)
            .map(|(n, _)| *n)
            .collect();
        for node in idle {
            self.stream_frame(now, node, None, out);
        }
        let id = self.set_timer(now.plus(every), Timer::StreamHeartbeat, out);
        self.stream.heartbeat_timer = Some(id);
    }

    fn end_served(&mut self, out: &mut Vec<Action>) {
        for (node, s) in std::mem::take(&mut self.stream.served) {
            out.push(Action::Send {
                to: node,
                msg: PeerMsg::LogStreamEnd {
                    req: s.req,
                    refused: false,
                },
            });
        }
        self.stream.ring.clear();
        self.stream.ring_bytes = 0;
        if let Some(id) = self.stream.heartbeat_timer.take() {
            self.cancel_timer(id, out);
        }
    }

    // ---- subscriber side ----

    /// The holder this node should stream from right now, if any.
    fn stream_upstream(&self) -> Option<NodeId> {
        if !self.cfg.log_streams || !self.cfg.p2p || self.stopped {
            return None;
        }
        if self.lease.held.is_some() || self.lease.epoch_held() || self.lease.lost {
            return None;
        }
        // (A member of an open continuation epoch keeps following the
        // hold owner: see the module comment.)
        let holder = self.lease.cached_holder?;
        (holder != 0 && holder != self.cfg.node_id).then_some(holder)
    }

    fn subscribe(&mut self, now: Ms, holder: NodeId, out: &mut Vec<Action>) {
        let req = self.op_id();
        let from = self.ship.next_seq;
        self.stream.sub = Some(Subscription {
            holder,
            req,
            next_n: 0,
            head: 0,
            live: false,
            since: now,
            last_frame: now,
            buf: BTreeMap::new(),
            buf_bytes: 0,
        });
        self.stats.stream_subscribes += 1;
        tracing::debug!(
            target: "constellation_authority::stream",
            node = self.cfg.node_id,
            holder,
            from,
            "subscribing to the holder's log stream"
        );
        out.push(Action::Send {
            to: holder,
            msg: PeerMsg::LogSubscribe { req, from },
        });
        if self.stream.watchdog_timer.is_none() {
            let id = self.set_timer(
                now.plus(self.stream_watchdog_every()),
                Timer::StreamWatchdog,
                out,
            );
            self.stream.watchdog_timer = Some(id);
        }
    }

    fn stream_watchdog_every(&self) -> u64 {
        (self.cfg.stream_timeout_ms / 2).max(1)
    }

    /// Let the subscription go: tell the holder when `unsubscribe`, back
    /// off before the next one when `backoff`, and nudge a round so S3
    /// covers whatever the stream would have delivered.
    fn drop_subscription(
        &mut self,
        now: Ms,
        why: &'static str,
        unsubscribe: bool,
        backoff: bool,
        out: &mut Vec<Action>,
    ) {
        let Some(sub) = self.stream.sub.take() else {
            return;
        };
        self.stream.dropped_caught_up =
            sub.live && sub.buf.is_empty() && self.ship.next_seq > sub.head;
        tracing::debug!(
            target: "constellation_authority::stream",
            node = self.cfg.node_id,
            holder = sub.holder,
            why,
            live = sub.live,
            buffered = sub.buf.len(),
            "log stream dropped; tailing S3"
        );
        if unsubscribe {
            out.push(Action::Send {
                to: sub.holder,
                msg: PeerMsg::LogUnsubscribe { req: sub.req },
            });
        }
        if backoff {
            let min = self.cfg.stream_retry_min_ms.max(1);
            let wait = self
                .stream
                .retry_ms
                .clamp(min, self.cfg.stream_retry_max_ms.max(min));
            self.stream.retry_at = now.plus(wait);
            self.stream.retry_ms = wait.saturating_mul(2);
        }
        self.nudge(now, out);
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_log_stream(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        n: u64,
        epoch: Epoch,
        head: Seq,
        segment: Option<(Seq, Vec<u8>)>,
        replica: &dyn Replica,
        out: &mut Vec<Action>,
    ) {
        let Some(sub) = self.stream.sub.as_mut() else {
            return;
        };
        if sub.req != req || sub.holder != from {
            return;
        }
        if n != sub.next_n {
            self.stats.stream_gaps += 1;
            tracing::warn!(
                target: "constellation_authority::stream",
                node = self.cfg.node_id,
                holder = from,
                expected = sub.next_n,
                got = n,
                "log stream frame out of order; falling back to S3"
            );
            self.drop_subscription(now, "frame gap", true, true, out);
            return;
        }
        sub.next_n += 1;
        sub.last_frame = now;
        sub.live = true;
        self.stream.dropped_caught_up = false;
        sub.head = sub.head.max(head);
        self.stream.retry_ms = self.cfg.stream_retry_min_ms;
        if epoch >= self.ship.max_epoch {
            self.lease.cached_holder = Some(from);
        }
        if let Some((seq, payload)) = segment {
            tracing::debug!(
                target: "constellation_authority::stream",
                node = self.cfg.node_id,
                holder = from,
                seq,
                n,
                next = self.ship.next_seq,
                "stream receive"
            );
            if seq < self.ship.next_seq {
                self.stats.stream_duplicates += 1;
            } else if !sub.buf.contains_key(&seq) {
                sub.buf_bytes += payload.len();
                sub.buf.insert(seq, payload);
                if sub.buf.len() > self.cfg.stream_buffer_segments.max(1)
                    || sub.buf_bytes > self.cfg.stream_buffer_bytes
                {
                    self.stats.stream_overflows += 1;
                    self.drop_subscription(now, "reorder buffer full", true, true, out);
                    return;
                }
            }
        }
        self.stream_drain(now, replica, out);
    }

    pub(crate) fn on_log_stream_end(
        &mut self,
        now: Ms,
        from: NodeId,
        req: OpId,
        refused: bool,
        out: &mut Vec<Action>,
    ) {
        if !self
            .stream
            .sub
            .as_ref()
            .is_some_and(|s| s.req == req && s.holder == from)
        {
            return;
        }
        if refused {
            self.stats.stream_refused += 1;
        } else {
            self.stats.stream_ended += 1;
        }
        self.stream.parked = Some((from, self.ship.head_seq));
        self.drop_subscription(
            now,
            if refused {
                "refused"
            } else {
                "holder ended it"
            },
            false,
            true,
            out,
        );
    }

    /// `Event::PeerFailed` for the subscription: it could not be opened,
    /// or broke. `true` when `req` was the subscription.
    pub(crate) fn on_stream_failed(&mut self, now: Ms, req: OpId, out: &mut Vec<Action>) -> bool {
        if !self.stream.sub.as_ref().is_some_and(|s| s.req == req) {
            return false;
        }
        self.stats.stream_lost += 1;
        self.drop_subscription(now, "transport", false, true, out);
        true
    }

    pub(crate) fn on_stream_watchdog(&mut self, now: Ms, out: &mut Vec<Action>) {
        self.stream.watchdog_timer = None;
        let Some(sub) = self.stream.sub.as_ref() else {
            return;
        };
        let silent = now.since(if sub.live { sub.last_frame } else { sub.since });
        if silent >= self.cfg.stream_timeout_ms as i64 {
            self.stats.stream_timeouts += 1;
            self.drop_subscription(now, "silent", true, true, out);
            return;
        }
        let id = self.set_timer(
            now.plus(self.stream_watchdog_every()),
            Timer::StreamWatchdog,
            out,
        );
        self.stream.watchdog_timer = Some(id);
    }

    /// Apply buffered segments from the cursor on, while no job owns it.
    pub(crate) fn stream_drain(&mut self, now: Ms, replica: &dyn Replica, out: &mut Vec<Action>) {
        if !self.cursor_free() {
            return;
        }
        let mut applied = 0u64;
        loop {
            let next = self.ship.next_seq;
            let Some(sub) = self.stream.sub.as_mut() else {
                break;
            };
            let Some((&seq, _)) = sub.buf.first_key_value() else {
                break;
            };
            if seq > next {
                break;
            }
            let payload = sub.buf.remove(&seq).expect("first key");
            sub.buf_bytes -= payload.len();
            if seq < next {
                self.stats.stream_duplicates += 1;
                continue;
            }
            match self.apply_incoming(now, seq, &payload, replica, out) {
                Ok(()) => {
                    applied += 1;
                    tracing::debug!(
                        target: "constellation_authority::stream",
                        node = self.cfg.node_id,
                        seq,
                        "stream apply"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        target: "constellation_authority::stream",
                        node = self.cfg.node_id,
                        seq,
                        %error,
                        "streamed segment not applied; falling back to S3"
                    );
                    self.drop_subscription(now, "apply failed", true, true, out);
                    break;
                }
            }
        }
        if applied > 0 {
            self.stats.segments_applied += applied;
            self.stats.stream_applied += applied;
            self.answer_awaiting_log(now, replica, out);
        }
        // Missing a sequence the stream will not deliver: S3 has it. (Not
        // during an epoch: S3 is what the epoch is without, and its probe
        // rounds keep their own cadence. Flex-crash seed 1072: a frozen
        // member, whose round ends at once, was nudged again by every
        // round's drain — a zero-delay poll loop.)
        let next = self.ship.next_seq;
        if self.epoch.open {
            return;
        }
        if let Some(sub) = self.stream.sub.as_ref() {
            let gap = sub.buf.first_key_value().is_some_and(|(s, _)| *s > next);
            if sub.live && (gap || next <= sub.head) {
                self.nudge(now, out);
            }
        }
    }

    /// A round may skip its S3 tail: the stream is live and has delivered
    /// everything up to the holder's reported head, and the backstop
    /// probe is not due.
    pub(crate) fn stream_covers_tail(&self, now: Ms) -> bool {
        self.stream_caught_up(now)
            && now.since(self.stream.last_s3_tail) < self.cfg.stream_backstop_ms as i64
    }

    fn stream_caught_up(&self, now: Ms) -> bool {
        self.stream.sub.as_ref().is_some_and(|s| {
            s.live
                && s.buf.is_empty()
                && self.ship.next_seq > s.head
                && now.since(s.last_frame) < self.cfg.stream_timeout_ms as i64
        })
    }

    /// The width of a round's S3 tail probe: 1 for the backstop probe of
    /// a caught-up stream (nothing is expected); one past the highest
    /// hinted sequence when hints say how far the log went (a hint costs
    /// a GET per new segment plus one, not a full speculative run); 1
    /// again once a caught-up stream went and nothing hints past the
    /// cursor (`dropped_caught_up`); the configured width otherwise. A
    /// saturated probe is followed by a full-width one, so a narrow probe
    /// never stops short of the head.
    pub(crate) fn stream_tail_width(&mut self, now: Ms) -> usize {
        self.stream.last_s3_tail = now;
        let full = self.cfg.tail_width.max(1);
        if self.stream_caught_up(now) {
            1
        } else if self.stream.hinted >= self.ship.next_seq {
            ((self.stream.hinted - self.ship.next_seq + 2) as usize).min(full)
        } else if self.stream.dropped_caught_up {
            1
        } else {
            full
        }
    }

    /// The holder's highest reported sequence on a live stream, if any.
    pub(crate) fn stream_head(&self) -> Option<Seq> {
        self.stream.sub.as_ref().filter(|s| s.live).map(|s| s.head)
    }

    /// The highest sequence a gossip hint named.
    pub(crate) fn stream_hinted(&self) -> Seq {
        self.stream.hinted
    }

    /// A gossip hint named `seq`.
    pub(crate) fn stream_note_hint(&mut self, seq: Seq) {
        self.stream.hinted = self.stream.hinted.max(seq);
    }

    /// A `SegmentPublished` hint from the holder the stream follows (or
    /// from an unnamed author, `from == 0`, while a stream is live): the
    /// stream delivers the segment (or its head reveals a gap), so no
    /// round is needed.
    pub(crate) fn stream_delivers_from(&self, from: NodeId) -> bool {
        self.stream
            .sub
            .as_ref()
            .is_some_and(|s| s.live && (from == 0 || s.holder == from))
    }

    /// `Event::Peers`: the directory's view of the peers changed.
    pub(crate) fn stream_on_peers(&mut self, now: Ms) {
        let up = self
            .lease
            .cached_holder
            .and_then(|h| self.links.get(&h))
            .is_some_and(|l| l.connected);
        if up && !self.stream.holder_link_up && self.stream.sub.is_none() {
            self.stream.retry_at = now;
            self.stream.retry_ms = self.cfg.stream_retry_min_ms;
        }
        self.stream.holder_link_up = up;
    }

    /// After every event: serve only while holding; subscribe, switch or
    /// drop as the known holder changes.
    pub(crate) fn stream_after_event(&mut self, now: Ms, out: &mut Vec<Action>) {
        if (!self.stream.served.is_empty() || !self.stream.ring.is_empty())
            && !self.stream_serving()
        {
            self.end_served(out);
        }
        let mut want = self.stream_upstream();
        if let (Some(w), Some((parked, head))) = (want, self.stream.parked) {
            // The log moved past the parking head: this node applied past
            // it, or a gossip hint names a segment past it. The hint is
            // all a node without a working S3 path has: a hold owner ends
            // its streams when its epoch closes (it holds nothing until
            // its flush re-claims the lease), and a member that could not
            // tail S3 never asked it again — it applied nothing new until
            // the epoch's missing member returned
            // (`epoch-member-dies-with-chunk`'s B).
            if w == parked && self.ship.head_seq.max(self.stream.hinted) <= head {
                want = None;
            } else {
                self.stream.parked = None;
            }
        }
        let current = self.stream.sub.as_ref().map(|s| s.holder);
        match (current, want) {
            (Some(cur), Some(want)) if cur == want => {}
            (Some(_), want) => {
                let why = if want.is_some() {
                    "holder changed"
                } else {
                    "no longer following a holder"
                };
                self.drop_subscription(now, why, true, false, out);
                if let Some(want) = want {
                    if now >= self.stream.retry_at {
                        self.subscribe(now, want, out);
                    }
                }
            }
            (None, Some(want)) if now >= self.stream.retry_at => self.subscribe(now, want, out),
            _ => {}
        }
    }
}
