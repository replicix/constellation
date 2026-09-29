//! Host lifecycle events (plan 31 §10): foreground/background, suspend and
//! resume, network reachability and metering, low power.
//!
//! A mobile host produces these from the OS (plan 36's `platform::android`);
//! a desktop or server has no such signals, so Linux and macOS provide a
//! [`ManualLifecycle`] that something else pushes into — tests today, and
//! the `node.lifecycle` control method in C8, so the harness can exercise
//! the engine's suspend/resume path on Linux ahead of any mobile port. What
//! the engine does on each event (sync every view, release leases,
//! quiesce P2P on `Suspending`) is C8's; this module only delivers them.

use std::sync::mpsc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleEvent {
    /// The hosting app came to the foreground.
    Foreground,
    /// The hosting app went to the background (may be suspended soon).
    Background,
    /// The host will suspend this process at `deadline`: whatever must be
    /// durable (the journal shipped, leases released) must be by then.
    Suspending { deadline: Instant },
    /// Running again after a suspension.
    Resumed,
    /// Network reachability or metering changed.
    NetworkChanged { reachable: bool, metered: bool },
    /// The host asked for reduced background work (battery saver).
    LowPower,
}

/// A stream of events from one [`LifecycleSource::subscribe`] call.
/// Dropping it unsubscribes. Events pushed before the subscription are not
/// replayed.
#[derive(Debug)]
pub struct Subscription {
    rx: mpsc::Receiver<LifecycleEvent>,
}

impl Subscription {
    /// Wait for the next event; `None` once the source is gone.
    pub fn recv(&self) -> Option<LifecycleEvent> {
        self.rx.recv().ok()
    }

    /// The next event, if one is waiting.
    pub fn try_recv(&self) -> Option<LifecycleEvent> {
        self.rx.try_recv().ok()
    }

    /// Wait up to `timeout` for the next event.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<LifecycleEvent> {
        self.rx.recv_timeout(timeout).ok()
    }
}

pub trait LifecycleSource: Send + Sync {
    /// Every event from now on, in order, to this subscriber. Delivery is
    /// a queue per subscriber: a slow consumer never blocks the source or
    /// another subscriber.
    fn subscribe(&self) -> Subscription;
}

/// A [`LifecycleSource`] whose events are whatever [`ManualLifecycle::push`]
/// is given.
#[derive(Debug, Default)]
pub struct ManualLifecycle {
    subscribers: Mutex<Vec<mpsc::Sender<LifecycleEvent>>>,
}

impl ManualLifecycle {
    pub fn new() -> ManualLifecycle {
        ManualLifecycle::default()
    }

    /// Deliver `event` to every current subscriber (and forget the ones
    /// that dropped their [`Subscription`]). Returns how many received it.
    pub fn push(&self, event: LifecycleEvent) -> usize {
        let mut subscribers = self.subscribers.lock().unwrap_or_else(|p| p.into_inner());
        subscribers.retain(|tx| tx.send(event).is_ok());
        subscribers.len()
    }
}

impl LifecycleSource for ManualLifecycle {
    fn subscribe(&self) -> Subscription {
        let (tx, rx) = mpsc::channel();
        self.subscribers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(tx);
        Subscription { rx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_subscriber_sees_every_later_event_in_order() {
        let source = ManualLifecycle::new();
        assert_eq!(source.push(LifecycleEvent::LowPower), 0);
        let a = source.subscribe();
        let b = source.subscribe();
        let deadline = Instant::now() + Duration::from_secs(5);
        let events = [
            LifecycleEvent::Background,
            LifecycleEvent::Suspending { deadline },
            LifecycleEvent::Resumed,
            LifecycleEvent::NetworkChanged {
                reachable: true,
                metered: true,
            },
            LifecycleEvent::Foreground,
        ];
        for event in events {
            assert_eq!(source.push(event), 2);
        }
        for sub in [&a, &b] {
            for event in events {
                assert_eq!(sub.recv_timeout(Duration::from_secs(1)), Some(event));
            }
            // The event pushed before subscribing is not replayed.
            assert_eq!(sub.try_recv(), None);
        }
    }

    #[test]
    fn a_dropped_subscription_is_forgotten() {
        let source = ManualLifecycle::new();
        let kept = source.subscribe();
        drop(source.subscribe());
        assert_eq!(source.push(LifecycleEvent::Resumed), 1);
        assert_eq!(kept.try_recv(), Some(LifecycleEvent::Resumed));
    }

    #[test]
    fn a_subscription_ends_with_its_source() {
        let source = ManualLifecycle::new();
        let sub = source.subscribe();
        drop(source);
        assert_eq!(sub.recv(), None);
    }

    #[test]
    fn events_cross_threads() {
        let source = std::sync::Arc::new(ManualLifecycle::new());
        let sub = source.subscribe();
        let pusher = source.clone();
        std::thread::spawn(move || {
            pusher.push(LifecycleEvent::LowPower);
        })
        .join()
        .unwrap();
        assert_eq!(
            sub.recv_timeout(Duration::from_secs(5)),
            Some(LifecycleEvent::LowPower)
        );
    }
}
